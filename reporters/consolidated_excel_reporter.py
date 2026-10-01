"""
reporters/consolidated_excel_reporter.py
────────────────────────────────────────
The consolidated Excel workbook produced for one validation phase.

Sheet order, sheet names and column order are declared in the `reporting`
YAML block and are reproduced here verbatim; this module never invents,
renames or reorders a sheet or a column. The same code path serves every
phase, so nothing below is specific to smoke test, historical backfill,
incremental or full load.

Cell values follow one rule throughout: a numeric ``0`` is written only when
the check that produces it actually executed and its real result is zero.
Anything that was not executed, or does not apply, reports the configured
``not_applicable`` marker.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from core.models import RunSummary, TableValidationResult


logger = logging.getLogger(__name__)


# Excel stores at most 32,767 characters in one cell.  Execution Details is a
# diagnostic convenience column, so keep its rendering comfortably inside
# that boundary without materializing an arbitrarily large JSON document.
EXCEL_TEXT_LIMIT = 32_767
DETAILS_TEXT_BUDGET = 24_000
DETAILS_NODE_BUDGET = 500
DETAILS_MAX_DEPTH = 6
DETAILS_TRUNCATED = "<truncated for Excel>"

# Backward-compatible public name. Incremental update evidence is now written
# only to each table-level workbook by PhaseExcelReportWriter.
INCREMENTAL_UPDATES_SHEET = "Incremental Updates"


#: Sheet keys this writer knows how to build, in contract order.
SHEET_KEYS: Tuple[str, ...] = (
    "summary",
    "table_results",
    "failed_checks",
    "all_check_detail",
    "final_reconciliation",
)

class ConsolidatedReportConfigError(ValueError):
    """Raised when the `reporting` YAML cannot describe a consolidated report."""


def _require_mapping(parent: Dict[str, Any], name: str, path: str) -> Dict[str, Any]:
    value = parent.get(name)
    if not isinstance(value, dict) or not value:
        raise ConsolidatedReportConfigError(f"{path}.{name} must be a non-empty mapping")
    return value


def _require_text(parent: Dict[str, Any], name: str, path: str) -> str:
    value = parent.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ConsolidatedReportConfigError(f"{path}.{name} must be a non-empty string")
    return value


class ConsolidatedExcelReportWriter:
    """Write one consolidated workbook for one phase from a `RunSummary`."""

    CONFIG_PATH = "reporting.consolidated_excel"

    def __init__(self, reporting_settings: Dict[str, Any]):
        if not isinstance(reporting_settings, dict):
            raise ConsolidatedReportConfigError("reporting configuration must be a mapping")
        self.config = _require_mapping(
            reporting_settings, "consolidated_excel", "reporting"
        )
        self.na = _require_text(self.config, "not_applicable", self.CONFIG_PATH)
        self.datetime_format = _require_text(
            self.config, "datetime_format", self.CONFIG_PATH
        )
        self.duration_template = _require_text(
            self.config, "duration_template", self.CONFIG_PATH
        )
        self.percent_template = _require_text(
            self.config, "percent_template", self.CONFIG_PATH
        )
        self.comparison_check = _require_text(
            self.config, "record_comparison_check", self.CONFIG_PATH
        )
        self.row_count_check = _require_text(
            self.config, "row_count_check", self.CONFIG_PATH
        )
        self.formatting = _require_mapping(
            self.config, "formatting", self.CONFIG_PATH
        )
        self.status_colors = _require_mapping(
            self.formatting, "status_colors", f"{self.CONFIG_PATH}.formatting"
        )
        if "DEFAULT" not in self.status_colors:
            raise ConsolidatedReportConfigError(
                f"{self.CONFIG_PATH}.formatting.status_colors.DEFAULT is required"
            )
        self.failed_statuses = self._status_list("failed_check_statuses")
        self.thresholds = _require_mapping(
            self.config, "pass_rate_thresholds", self.CONFIG_PATH
        )
        self.summary_metrics = self._summary_metric_specs()
        self.sheets = self._sheet_specs()

    # ── configuration ───────────────────────────────────────────────────

    def _status_list(self, name: str) -> frozenset:
        values = self.config.get(name)
        if not isinstance(values, list) or not values or any(
            not isinstance(item, str) or not item.strip() for item in values
        ):
            raise ConsolidatedReportConfigError(
                f"{self.CONFIG_PATH}.{name} must be a non-empty list of statuses"
            )
        return frozenset(str(item).upper() for item in values)

    def _summary_metric_specs(self) -> List[Dict[str, Any]]:
        raw = self.config.get("summary_metrics")
        if not isinstance(raw, list) or not raw:
            raise ConsolidatedReportConfigError(
                f"{self.CONFIG_PATH}.summary_metrics must be a non-empty list"
            )
        specs = []
        for entry in raw:
            if not isinstance(entry, dict):
                raise ConsolidatedReportConfigError(
                    f"{self.CONFIG_PATH}.summary_metrics entries must be mappings"
                )
            metric = _require_text(entry, "metric", f"{self.CONFIG_PATH}.summary_metrics")
            source = _require_text(entry, "source", f"{self.CONFIG_PATH}.summary_metrics")
            if source not in _SUMMARY_SOURCES:
                raise ConsolidatedReportConfigError(
                    f"{self.CONFIG_PATH}.summary_metrics '{metric}' uses unknown "
                    f"source '{source}'"
                )
            specs.append({"metric": metric, "source": source, **entry})
        return specs

    def _sheet_specs(self) -> List[Dict[str, Any]]:
        raw = self.config.get("sheets")
        if not isinstance(raw, list) or not raw:
            raise ConsolidatedReportConfigError(
                f"{self.CONFIG_PATH}.sheets must be a non-empty list"
            )
        specs: List[Dict[str, Any]] = []
        seen_names = set()
        for entry in raw:
            if not isinstance(entry, dict):
                raise ConsolidatedReportConfigError(
                    f"{self.CONFIG_PATH}.sheets entries must be mappings"
                )
            key = _require_text(entry, "key", f"{self.CONFIG_PATH}.sheets")
            name = _require_text(entry, "name", f"{self.CONFIG_PATH}.sheets")
            if key not in SHEET_KEYS:
                raise ConsolidatedReportConfigError(
                    f"{self.CONFIG_PATH}.sheets declares unsupported sheet '{key}'"
                )
            if len(name) > 31:
                raise ConsolidatedReportConfigError(
                    f"Excel sheet name '{name}' exceeds 31 characters"
                )
            if name in seen_names:
                raise ConsolidatedReportConfigError(
                    f"{self.CONFIG_PATH}.sheets repeats sheet name '{name}'"
                )
            seen_names.add(name)
            columns = entry.get("columns")
            if not isinstance(columns, list) or not columns or any(
                not isinstance(item, str) or not item.strip() for item in columns
            ):
                raise ConsolidatedReportConfigError(
                    f"{self.CONFIG_PATH}.sheets '{key}'.columns must be a "
                    "non-empty list of column names"
                )
            if len(set(columns)) != len(columns):
                raise ConsolidatedReportConfigError(
                    f"{self.CONFIG_PATH}.sheets '{key}'.columns repeats a column"
                )
            unknown = [
                column for column in columns
                if column not in _COLUMN_BUILDERS.get(key, {})
            ]
            if unknown:
                raise ConsolidatedReportConfigError(
                    f"{self.CONFIG_PATH}.sheets '{key}' declares columns with no "
                    "reporting source: " + ", ".join(unknown)
                )
            specs.append({"key": key, "name": name, "columns": list(columns)})
        return specs

    @property
    def sheet_names(self) -> List[str]:
        return [spec["name"] for spec in self.sheets]

    def columns_for(self, key: str) -> List[str]:
        for spec in self.sheets:
            if spec["key"] == key:
                return list(spec["columns"])
        raise KeyError(key)

    # ── workbook ────────────────────────────────────────────────────────

    def write(
        self,
        summary: RunSummary,
        phase: str,
        path: Path,
        evidence_limiter: Optional[Callable[[List[Any], str], List[Any]]] = None,
    ) -> Path:
        """Write the consolidated workbook for `phase` and return its path.

        `evidence_limiter` lets the caller that owns the record-level export
        limits report the same exported counts the table-level reports show,
        so the two never disagree. Without it, the exported counts are the
        evidence the run actually captured.
        """
        from openpyxl import Workbook
        from openpyxl.styles import Alignment, Font, PatternFill
        from openpyxl.utils import get_column_letter

        tables = list(summary.table_results)
        workbook = Workbook()
        workbook.remove(workbook.active)
        header_color = str(self.formatting.get("header_color", "1D4ED8"))
        header_font_color = str(self.formatting.get("header_font_color", "FFFFFF"))
        freeze_panes = self.formatting.get("freeze_panes") or None
        max_width = int(self.formatting.get("max_column_width", 60))

        for spec in self.sheets:
            columns = spec["columns"]
            worksheet = workbook.create_sheet(spec["name"])
            worksheet.append(columns)
            for cell in worksheet[1]:
                cell.font = Font(bold=True, color=header_font_color)
                cell.fill = PatternFill("solid", fgColor=header_color)
                cell.alignment = Alignment(wrap_text=True, vertical="center")
            for row in self._rows(
                spec["key"], summary, tables, phase, evidence_limiter
            ):
                worksheet.append([row[column] for column in columns])
            if freeze_panes:
                worksheet.freeze_panes = freeze_panes
            worksheet.auto_filter.ref = worksheet.dimensions
            if "Status" in columns:
                index = columns.index("Status") + 1
                for row_number in range(2, worksheet.max_row + 1):
                    cell = worksheet.cell(row_number, index)
                    color = self.status_colors.get(
                        str(cell.value), self.status_colors["DEFAULT"]
                    )
                    cell.fill = PatternFill("solid", fgColor=color)
            for column_cells in worksheet.columns:
                width = max(
                    (len(str(cell.value or "")) for cell in column_cells), default=0
                )
                worksheet.column_dimensions[
                    get_column_letter(column_cells[0].column)
                ].width = min(width + 2, max_width)

        # The standard report tree is deep, so the workbook is opened through
        # the shared helper that keeps long Windows paths openable.
        from .report_paths import filesystem_path

        filesystem_path(path.parent).mkdir(parents=True, exist_ok=True)
        workbook.save(str(filesystem_path(path)))
        return path

    def _rows(
        self,
        key: str,
        summary: RunSummary,
        tables: List[TableValidationResult],
        phase: str,
        evidence_limiter: Optional[Callable[[List[Any], str], List[Any]]] = None,
    ) -> List[Dict[str, Any]]:
        if key == "summary":
            return self._summary_rows(summary, tables)
        if key == "table_results":
            return [self._table_row(table, phase) for table in tables]
        if key == "failed_checks":
            return self._failed_check_rows(tables, phase)
        if key == "all_check_detail":
            return self._all_check_rows(tables, phase)
        if key == "final_reconciliation":
            return [
                self._reconciliation_row(table, evidence_limiter)
                for table in tables
            ]
        raise KeyError(key)

    # ── Summary ─────────────────────────────────────────────────────────

    def _summary_rows(
        self, summary: RunSummary, tables: List[TableValidationResult]
    ) -> List[Dict[str, Any]]:
        values, pass_rate = _summary_values(summary, tables, self)
        rows = []
        for spec in self.summary_metrics:
            value = values[spec["source"]]
            rows.append({
                "Metric": spec["metric"],
                "Value": value,
                "Status": self._summary_status(spec, value, pass_rate),
            })
        return rows

    def _summary_status(
        self, spec: Dict[str, Any], value: Any, pass_rate: Any
    ) -> str:
        declared = spec.get("status")
        if declared == "threshold":
            return self._pass_rate_status(pass_rate)
        if isinstance(declared, str) and declared.strip():
            return declared
        zero_status = spec.get("status_when_zero")
        non_zero_status = spec.get("status_when_non_zero")
        if zero_status is None and non_zero_status is None:
            return self.na
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            return self.na
        chosen = zero_status if not value else non_zero_status
        return str(chosen) if chosen else self.na

    def _pass_rate_status(self, pass_rate: Any) -> str:
        """Grade the phase pass rate. No executable table means nothing to grade."""
        if pass_rate is None:
            return self.na
        if pass_rate >= float(self.thresholds["pass"]):
            return "PASS"
        if pass_rate >= float(self.thresholds["warning"]):
            return "WARNING"
        return "FAIL"

    # ── Table Results ───────────────────────────────────────────────────

    def _table_row(self, table: TableValidationResult, phase: str) -> Dict[str, Any]:
        context = {"table": table, "phase": phase, "writer": self}
        return {
            column: builder(context)
            for column, builder in _COLUMN_BUILDERS["table_results"].items()
        }

    # ── Failed Checks ───────────────────────────────────────────────────

    def _failed_check_rows(
        self, tables: List[TableValidationResult], phase: str
    ) -> List[Dict[str, Any]]:
        rows = []
        for table in tables:
            for check in table.checks:
                if _status_value(check).upper() not in self.failed_statuses:
                    continue
                context = {
                    "table": table, "check": check, "phase": phase, "writer": self,
                }
                rows.append({
                    column: builder(context)
                    for column, builder in _COLUMN_BUILDERS["failed_checks"].items()
                })
        return rows

    # ── All Check Detail ────────────────────────────────────────────────

    def _all_check_rows(
        self, tables: List[TableValidationResult], phase: str
    ) -> List[Dict[str, Any]]:
        rows = []
        for table in tables:
            for check in table.checks:
                context = {
                    "table": table, "check": check, "phase": phase, "writer": self,
                }
                rows.append({
                    column: builder(context)
                    for column, builder in _COLUMN_BUILDERS["all_check_detail"].items()
                })
        return rows

    # ── Final Reconciliation ────────────────────────────────────────────

    def _reconciliation_row(
        self,
        table: TableValidationResult,
        evidence_limiter: Optional[Callable[[List[Any], str], List[Any]]] = None,
    ) -> Dict[str, Any]:
        context = {"table": table, "writer": self, "limiter": evidence_limiter}
        return {
            column: builder(context)
            for column, builder in _COLUMN_BUILDERS["final_reconciliation"].items()
        }

    # ── value helpers ───────────────────────────────────────────────────

    def cell(self, value: Any) -> Any:
        """Render one value, reporting `not_applicable` when nothing executed."""
        if value is None:
            return self.na
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, (datetime, date)):
            return value.strftime(self.datetime_format)
        if isinstance(value, (dict, list, tuple, set)):
            if not value:
                return self.na
            return json.dumps(
                sorted(value) if isinstance(value, set) else value, default=str
            )
        text = str(value)
        return text if text.strip() else self.na

    def details_cell(self, value: Any) -> Any:
        """Render bounded diagnostic JSON without allowing evidence to abort Excel."""
        if not value:
            return self.na
        try:
            budget = {"nodes": DETAILS_NODE_BUDGET, "text": DETAILS_TEXT_BUDGET}
            bounded = self._bounded_detail(value, budget, 0, set())
            rendered = json.dumps(bounded, default=str)
        except (TypeError, ValueError, OverflowError, RecursionError) as exc:
            # Execution Details is diagnostic-only. A connector or file
            # validator may attach a circular/custom runtime object; that must
            # not prevent every Excel/JSON/HTML report from being written.
            logger.warning(
                "Excel Execution Details could not be serialized; writing a "
                "safe diagnostic marker | value_type=%s | error_type=%s",
                type(value).__name__, type(exc).__name__,
            )
            rendered = json.dumps({
                "reporting_warning": "Execution details were not JSON serializable",
                "detail_type": type(value).__name__,
                "error_type": type(exc).__name__,
            })
        if len(rendered) > EXCEL_TEXT_LIMIT:
            return rendered[: EXCEL_TEXT_LIMIT - len(DETAILS_TRUNCATED)] + DETAILS_TRUNCATED
        return rendered

    def _bounded_detail(
        self, value: Any, budget: Dict[str, int], depth: int, active: set
    ) -> Any:
        """Copy only the portion of a details value that one Excel cell can hold."""
        if budget["nodes"] <= 0 or budget["text"] <= 0:
            return DETAILS_TRUNCATED
        budget["nodes"] -= 1
        if depth >= DETAILS_MAX_DEPTH:
            return DETAILS_TRUNCATED
        if value is None or isinstance(value, (bool, int, float)):
            return value
        if isinstance(value, (datetime, date)):
            return value.strftime(self.datetime_format)
        if isinstance(value, str):
            retained = min(len(value), budget["text"])
            budget["text"] -= retained
            return value if retained == len(value) else value[:retained] + DETAILS_TRUNCATED

        identity = id(value)
        if isinstance(value, dict):
            if identity in active:
                return "<recursive reference>"
            active.add(identity)
            result: Dict[str, Any] = {}
            try:
                for key, item in value.items():
                    if budget["nodes"] <= 0 or budget["text"] <= 0:
                        result[DETAILS_TRUNCATED] = True
                        break
                    result[str(key)] = self._bounded_detail(
                        item, budget, depth + 1, active
                    )
            finally:
                active.remove(identity)
            return result
        if isinstance(value, (list, tuple, set)):
            if identity in active:
                return ["<recursive reference>"]
            active.add(identity)
            result = []
            try:
                for item in value:
                    if budget["nodes"] <= 0 or budget["text"] <= 0:
                        result.append(DETAILS_TRUNCATED)
                        break
                    result.append(self._bounded_detail(
                        item, budget, depth + 1, active
                    ))
            finally:
                active.remove(identity)
            return result

        text = str(value)
        retained = min(len(text), budget["text"])
        budget["text"] -= retained
        return text if retained == len(text) else text[:retained] + DETAILS_TRUNCATED

    def number(self, value: Any) -> Any:
        """Render a count, keeping a real zero and marking an absent one."""
        if value is None:
            return self.na
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return value
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return self.cell(value)

    def percent(self, value: Any) -> Any:
        if value is None:
            return self.na
        return self.percent_template.format(value=value)

    def comparison_details(self, table: TableValidationResult) -> Dict[str, Any]:
        """Details of the record comparison check, or `{}` when it never ran."""
        check = next(
            (item for item in table.checks
             if item.check_name == self.comparison_check),
            None,
        )
        return dict(check.details) if check is not None else {}

    def row_count(self, table: TableValidationResult, side: str) -> Any:
        metric = table.execution_metrics.get(f"{side}_row_count")
        if metric is not None:
            return metric
        check = next(
            (item for item in table.checks
             if item.check_name == self.row_count_check),
            None,
        )
        return getattr(check, f"{side}_value", None) if check is not None else None

    def metric(self, table: TableValidationResult, name: str) -> Any:
        return self.cell(table.execution_metrics.get(name))

    def metric_number(self, table: TableValidationResult, name: str) -> Any:
        return self.number(table.execution_metrics.get(name))

    def detail(self, details: Dict[str, Any], *names: str) -> Any:
        for name in names:
            if name in details and details[name] is not None:
                return details[name]
        return None

    def error_count(self, table: TableValidationResult) -> int:
        return table.framework_error_count + table.config_error_count


# ─────────────────────────────────────────────────────────────────────────
# Summary source values
# ─────────────────────────────────────────────────────────────────────────

_SUMMARY_SOURCES = (
    "run_id",
    "project_name",
    "started_at",
    "duration",
    "total_tables",
    "passed_tables",
    "failed_tables",
    "warning_tables",
    "error_tables",
    "pass_rate",
)


def _summary_values(
    summary: RunSummary,
    tables: List[TableValidationResult],
    writer: "ConsolidatedExcelReportWriter",
) -> Tuple[Dict[str, Any], Any]:
    """Every Summary metric, derived from the phase results actually executed.

    Returns the metric values plus the resolved pass rate, which is ``None``
    when the phase executed no gradable table.
    """
    statuses = [_status_value(table) for table in tables]
    executable = [status for status in statuses if status != "SKIP"]
    passed = sum(1 for status in statuses if status == "PASS")
    failed = sum(1 for status in statuses if status == "FAIL")
    warned = sum(1 for status in statuses if status in {"WARN", "WARNING"})
    errored = sum(
        1 for status in statuses
        if status in {"ERROR", "FRAMEWORK_ERROR", "CONFIG_ERROR"}
    )
    pass_rate = round(passed / len(executable) * 100, 2) if executable else None
    values = {
        "run_id": writer.cell(summary.run_id),
        "project_name": writer.cell(summary.project_name),
        "started_at": writer.cell(summary.started_at),
        "duration": writer.duration_template.format(
            seconds=float(summary.duration_seconds or 0.0)
        ),
        "total_tables": len(tables),
        "passed_tables": passed,
        "failed_tables": failed,
        "warning_tables": warned,
        "error_tables": errored,
        "pass_rate": (
            writer.percent(pass_rate) if pass_rate is not None else writer.na
        ),
    }
    return values, pass_rate


def _status_value(item: Any) -> str:
    status = getattr(item, "overall_status", None)
    if status is None:
        status = getattr(item, "status", None)
    value = getattr(status, "value", status)
    return str(value).upper() if value is not None else ""


def _execution_time(check) -> Any:
    for name in ("execution_time", "execution_time_seconds"):
        value = check.details.get(name)
        if value is not None:
            return value
    return None


def _validation_category(check) -> str:
    explicit = (check.details or {}).get("validation_category")
    if explicit is not None and str(explicit).strip():
        return explicit
    name = str(check.check_name or "").strip()
    for prefixes, category in (
        (("row_count",), "row_count"),
        (("key_validation",), "key_validation"),
        (("null_", "mandatory_null_", "unexpected_null_"), "null_validation"),
        (("duplicate", "near_duplicate", "post_transform_duplicate"), "duplicate_validation"),
        (("aggregate",), "aggregate_validation"),
        (("record_comparison",), "record_comparison"),
        (("rule_", "business_rule"), "business_rules"),
        (("referential",), "referential_integrity"),
        (("scd",), "scd_validation"),
        (("pii",), "pii_scan"),
        (("audit",), "audit_validation"),
    ):
        if name.lower().startswith(prefixes):
            return category
    return (check.details or {}).get("validation_group") or name or "Validation"


def _failure_reason(check) -> Any:
    for name in ("failure_reason", "error_reason"):
        value = (check.details or {}).get(name)
        if value is not None and str(value).strip():
            return value
    if _status_value(check) in {"FAIL", "ERROR", "FRAMEWORK_ERROR", "CONFIG_ERROR"}:
        if check.message and str(check.message).strip():
            return check.message
        return f"{check.check_name}: {_status_value(check)} (no failure details recorded)"
    return None


# ─────────────────────────────────────────────────────────────────────────
# Column builders
#
# One entry per declared column. The YAML declares which columns appear and
# in which order; these builders say where each column's value comes from.
# A column named in YAML with no builder is a configuration error, so the
# workbook can never contain an unexplained column.
# ─────────────────────────────────────────────────────────────────────────

def _table_builders() -> Dict[str, Callable[[Dict[str, Any]], Any]]:
    def value(getter):
        return lambda ctx: getter(ctx["table"], ctx["writer"], ctx)

    return {
        "Table ID": value(lambda t, w, c: w.cell(t.table_id)),
        "Source System": value(lambda t, w, c: w.cell(t.source_system)),
        "Source Table": value(lambda t, w, c: w.cell(t.source_fqn)),
        "Target Table": value(lambda t, w, c: w.cell(t.target_fqn)),
        "Layer": value(lambda t, w, c: w.cell(t.target_layer)),
        "Phase": value(lambda t, w, c: w.cell(t.migration_phase or c["phase"])),
        "Tags": value(lambda t, w, c: w.cell(", ".join(t.tags))),
        "Checks Pass": value(lambda t, w, c: t.pass_count),
        "Checks Fail": value(lambda t, w, c: t.fail_count),
        "Checks Warning": value(lambda t, w, c: t.warn_count),
        "Checks Error": value(lambda t, w, c: w.error_count(t)),
        "Checks Skip": value(lambda t, w, c: t.skip_count),
        "Overall Status": value(lambda t, w, c: t.overall_status.value),
        "Duration (s)": value(lambda t, w, c: round(t.duration_seconds, 6)),
        "Error": value(lambda t, w, c: w.cell(t.error_message)),
        "Execution Mode": value(lambda t, w, c: w.metric(t, "execution_mode")),
        "Parallel": value(lambda t, w, c: w.metric(t, "parallel_execution_status")),
        "Batch Enabled": value(lambda t, w, c: w.metric(t, "batch_execution_status")),
        "Batch Size": value(lambda t, w, c: w.metric_number(t, "batch_size")),
        "Worker Count": value(lambda t, w, c: w.metric_number(t, "worker_count")),
        "Source Rows": value(lambda t, w, c: w.number(w.row_count(t, "source"))),
        "Target Rows": value(lambda t, w, c: w.number(w.row_count(t, "target"))),
        "Processed Rows": value(
            lambda t, w, c: w.metric_number(t, "processed_row_count")
        ),
        "Total Batches": value(lambda t, w, c: w.metric_number(t, "total_batches")),
        "Successful Batches": value(
            lambda t, w, c: w.metric_number(t, "successful_batches")
        ),
        "Failed Batches": value(lambda t, w, c: w.metric_number(t, "failed_batches")),
        "Retry Count": value(lambda t, w, c: w.metric_number(t, "retry_count")),
        "Throughput Rows/s": value(
            lambda t, w, c: w.metric_number(t, "throughput_rows_per_second")
        ),
        "Validation Parallel": value(
            lambda t, w, c: w.metric(t, "validation_parallelism_status")
        ),
        "Validation Workers": value(
            lambda t, w, c: w.metric_number(t, "validation_worker_count")
        ),
        "Table Wall Clock (s)": value(
            lambda t, w, c: w.metric_number(t, "table_execution_time")
        ),
        "Sequential Validation Sum (s)": value(
            lambda t, w, c: w.metric_number(t, "sequential_validation_time")
        ),
        "Slowest Validation": value(lambda t, w, c: w.metric(t, "slowest_validation")),
        "Slowest Validation (s)": value(
            lambda t, w, c: w.metric_number(t, "slowest_validation_seconds")
        ),
    }


def _failed_check_builders() -> Dict[str, Callable[[Dict[str, Any]], Any]]:
    def value(getter):
        return lambda ctx: getter(ctx["table"], ctx["check"], ctx["writer"], ctx)

    return {
        "Table ID": value(lambda t, k, w, c: w.cell(t.table_id)),
        "Source": value(lambda t, k, w, c: w.cell(t.source_fqn)),
        "Target": value(lambda t, k, w, c: w.cell(t.target_fqn)),
        "Layer": value(lambda t, k, w, c: w.cell(t.target_layer)),
        "Phase": value(lambda t, k, w, c: w.cell(t.migration_phase or c["phase"])),
        "Validation Category": value(
            lambda t, k, w, c: w.cell(_validation_category(k))
        ),
        "Check Name": value(lambda t, k, w, c: w.cell(k.check_name)),
        "Status": value(lambda t, k, w, c: k.status.value),
        "Source Value": value(lambda t, k, w, c: w.cell(k.source_value)),
        "Target Value": value(lambda t, k, w, c: w.cell(k.target_value)),
        "Difference": value(lambda t, k, w, c: w.cell(k.delta)),
        "Difference Percentage": value(lambda t, k, w, c: w.number(k.delta_pct)),
        "Failure Reason": value(lambda t, k, w, c: w.cell(_failure_reason(k))),
        "Message": value(lambda t, k, w, c: w.cell(k.message)),
        "Execution Time (s)": value(
            lambda t, k, w, c: w.number(_execution_time(k))
        ),
    }


def _all_check_builders() -> Dict[str, Callable[[Dict[str, Any]], Any]]:
    def value(getter):
        return lambda ctx: getter(ctx["table"], ctx["check"], ctx["writer"], ctx)

    return {
        "Table ID": value(lambda t, k, w, c: w.cell(t.table_id)),
        "Source FQN": value(lambda t, k, w, c: w.cell(t.source_fqn)),
        "Target FQN": value(lambda t, k, w, c: w.cell(t.target_fqn)),
        "Layer": value(lambda t, k, w, c: w.cell(t.target_layer)),
        "Phase": value(lambda t, k, w, c: w.cell(t.migration_phase or c["phase"])),
        "Check Name": value(lambda t, k, w, c: w.cell(k.check_name)),
        "Status": value(lambda t, k, w, c: k.status.value),
        "Source Value": value(lambda t, k, w, c: w.cell(k.source_value)),
        "Target Value": value(lambda t, k, w, c: w.cell(k.target_value)),
        "Difference": value(lambda t, k, w, c: w.cell(k.delta)),
        "Difference Percentage": value(lambda t, k, w, c: w.number(k.delta_pct)),
        "Message": value(lambda t, k, w, c: w.cell(k.message)),
        "Comparison Mode": value(
            lambda t, k, w, c: w.cell(k.details.get("comparison_mode"))
        ),
        "Actual Comparison Strategy": value(
            lambda t, k, w, c: w.cell(
                w.detail(k.details, "actual_comparison_strategy",
                         "comparison_strategy")
            )
        ),
        "Sampling Strategy": value(
            lambda t, k, w, c: w.cell(k.details.get("sampling_strategy"))
        ),
        "Key Columns Used": value(
            lambda t, k, w, c: w.cell(
                w.detail(k.details, "key_columns_used", "key_columns")
            )
        ),
        "Records Checked": value(
            lambda t, k, w, c: w.number(k.details.get("records_checked"))
        ),
        "Matched Records": value(
            lambda t, k, w, c: w.number(k.details.get("matched_records"))
        ),
        "Mismatch Records": value(
            lambda t, k, w, c: w.number(k.details.get("mismatch_records"))
        ),
        "Execution Time (s)": value(
            lambda t, k, w, c: w.number(_execution_time(k))
        ),
        "Validation Profile": value(
            lambda t, k, w, c: w.cell(
                k.details.get("validation_profile", t.migration_phase)
            )
        ),
        "Key Mapping Source": value(
            lambda t, k, w, c: w.cell(k.details.get("key_mapping_source"))
        ),
        "Records Compared": value(
            lambda t, k, w, c: w.number(
                w.detail(k.details, "records_compared", "records_checked")
            )
        ),
        "Validation Category": value(
            lambda t, k, w, c: w.cell(_validation_category(k))
        ),
        "Source Null Count": value(
            lambda t, k, w, c: w.number(k.details.get("source_null_count"))
        ),
        "Target Null Count": value(
            lambda t, k, w, c: w.number(k.details.get("target_null_count"))
        ),
        "Mismatch Count": value(
            lambda t, k, w, c: w.number(k.details.get("mismatch_count"))
        ),
        "Source Record Count": value(
            lambda t, k, w, c: w.number(k.details.get("source_record_count"))
        ),
        "Target Record Count": value(
            lambda t, k, w, c: w.number(k.details.get("target_record_count"))
        ),
        "Audit Columns Checked": value(
            lambda t, k, w, c: w.cell(k.details.get("audit_columns_checked"))
        ),
        "Failure Reason": value(lambda t, k, w, c: w.cell(_failure_reason(k))),
        "Validation Group": value(
            lambda t, k, w, c: w.cell(k.details.get("validation_group"))
        ),
        "Start Time": value(lambda t, k, w, c: w.cell(k.details.get("start_time"))),
        "End Time": value(lambda t, k, w, c: w.cell(k.details.get("end_time"))),
        "Wall Clock Execution Time (s)": value(
            lambda t, k, w, c: w.number(k.details.get("execution_time_seconds"))
        ),
        "Execution Details": value(
            lambda t, k, w, c: w.details_cell(k.details)
        ),
    }


def _reconciliation_builders() -> Dict[str, Callable[[Dict[str, Any]], Any]]:
    def value(getter):
        def build(ctx):
            table = ctx["table"]
            writer = ctx["writer"]
            details = ctx.setdefault(
                "_comparison", writer.comparison_details(table)
            )
            return getter(table, details, writer, ctx.get("limiter"))
        return build

    def total(details, writer, *names):
        """A reconciliation total: only reported when the comparison executed."""
        if not details:
            return writer.na
        return writer.number(writer.detail(details, *names))

    def exported(details, writer, limiter, records, category):
        """How many evidence rows were exported for an executed comparison."""
        if not details:
            return writer.na
        return len(limiter(records, category) if limiter else records)

    def record_match_pct(details, writer):
        if not details:
            return writer.na
        checked = writer.detail(details, "records_checked")
        matched = writer.detail(details, "matched_records")
        if not isinstance(checked, (int, float)) or not isinstance(
            matched, (int, float)
        ):
            return writer.na
        if not checked:
            return writer.percent(0.0)
        return writer.percent(round(matched / checked * 100, 2))

    def check_pass_pct(table, writer):
        if not table.checks:
            return writer.na
        return writer.percent(table.pass_rate_pct)

    return {
        "Table ID": value(lambda t, d, w, l: w.cell(t.table_id)),
        "Table Name": value(lambda t, d, w, l: w.cell(t.target_fqn)),
        "Source Records": value(
            lambda t, d, w, l: w.number(w.row_count(t, "source"))
        ),
        "Target Records": value(
            lambda t, d, w, l: w.number(w.row_count(t, "target"))
        ),
        "Comparison Mode": value(
            lambda t, d, w, l: w.cell(w.detail(d, "comparison_mode"))
        ),
        "Actual Comparison Strategy": value(
            lambda t, d, w, l: w.cell(
                w.detail(d, "actual_comparison_strategy", "comparison_strategy")
            )
        ),
        "Sampling Strategy": value(
            lambda t, d, w, l: w.cell(w.detail(d, "sampling_strategy"))
        ),
        "Key Columns Used": value(
            lambda t, d, w, l: w.cell(w.detail(d, "key_columns_used", "key_columns"))
        ),
        "Records Checked": value(lambda t, d, w, l: total(d, w, "records_checked")),
        "Total Matched": value(lambda t, d, w, l: total(d, w, "matched_records")),
        "Exported Matched": value(
            lambda t, d, w, l: exported(d, w, l, t.matched_records, "matched_records")
        ),
        "Total Mismatched": value(
            lambda t, d, w, l: total(d, w, "mismatch_records", "mismatched_records")
        ),
        "Exported Mismatched": value(
            lambda t, d, w, l: exported(
                d, w, l, t.mismatched_records, "mismatched_records"
            )
        ),
        "Missing In Source": value(
            lambda t, d, w, l: total(d, w, "missing_in_source")
        ),
        "Missing In Target": value(
            lambda t, d, w, l: total(d, w, "missing_in_target")
        ),
        "Total Excess In Source": value(
            lambda t, d, w, l: total(d, w, "excess_in_source", "missing_in_target")
        ),
        "Exported Excess In Source": value(
            lambda t, d, w, l: exported(
                d, w, l, t.missing_in_target, "excess_in_source"
            )
        ),
        "Total Excess In Target": value(
            lambda t, d, w, l: total(d, w, "excess_in_target", "missing_in_source")
        ),
        "Exported Excess In Target": value(
            lambda t, d, w, l: exported(
                d, w, l, t.missing_in_source, "excess_in_target"
            )
        ),
        "Record Match %": value(lambda t, d, w, l: record_match_pct(d, w)),
        "Validation Check Pass %": value(lambda t, d, w, l: check_pass_pct(t, w)),
        "Status": value(lambda t, d, w, l: t.overall_status.value),
    }


#: The Summary sheet is built from `summary_metrics` rather than per-column
#: accessors, so its entry only declares which column names are supported.
_SUMMARY_COLUMNS: Dict[str, Callable[[Dict[str, Any]], Any]] = {
    "Metric": lambda ctx: ctx["row"]["Metric"],
    "Value": lambda ctx: ctx["row"]["Value"],
    "Status": lambda ctx: ctx["row"]["Status"],
}


_COLUMN_BUILDERS: Dict[str, Dict[str, Callable[[Dict[str, Any]], Any]]] = {
    "summary": _SUMMARY_COLUMNS,
    "table_results": _table_builders(),
    "failed_checks": _failed_check_builders(),
    "all_check_detail": _all_check_builders(),
    "final_reconciliation": _reconciliation_builders(),
}

