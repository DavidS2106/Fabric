from __future__ import annotations

import json
import os
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

from core.models import (
    INCREMENTAL_UPDATE_EVIDENCE_LIMIT, SERVER_2_MATCH_EVIDENCE_LIMIT,
    RunSummary, TableValidationResult,
)

from .consolidated_excel_reporter import ConsolidatedExcelReportWriter
from .report_paths import filesystem_path
from .table_identity import primary_key_label, table_name


class PhaseExcelReportWriter:
    """Build the sheets of the consolidated and per-table phase workbooks.

    The per-table sheet list, sheet names, status vocabulary, formatting and
    evidence limits are described by
    ``comparison_settings.full_data_comparison.excel_reports``. The
    consolidated workbook is delegated to
    :class:`~reporters.consolidated_excel_reporter.ConsolidatedExcelReportWriter`
    so that its sheet and column contract lives in one place, in the
    ``reporting`` YAML block, for every phase alike.

    Where a workbook is written is not this class's business: callers pass the
    path resolved from ``report_output``, so no output folder or file name is
    described twice.
    """

    REQUIRED_SHEETS = frozenset({
        "summary", "table_summary", "validation_checks",
        "matched_records", "mismatched_records", "excess_in_source",
        "excess_in_target", "reconciliation",
    })
    INCREMENTAL_UPDATES_SHEET = "Incremental Updates"
    INCREMENTAL_UPDATE_COLUMNS = (
        "Table ID", "Source Table", "Target Table", "Scenario / Strategy",
        "Record Identifier", "PK / Discriminator", "Classification", "Reason",
        "Changed Columns", "Previous Target Values", "Current Values",
        "Previous Audit Boundary", "Current Audit / Watermark Boundary",
    )
    SERVER_2_MATCHES_SHEET = "Server 2 Matches"
    SERVER_2_MATCH_COLUMNS = (
        "Table ID", "Source Table", "Target Table", "Server 2",
        "Match Method", "Record Identifier", "PK / Discriminator",
        "Occurrences", "Status", "Source Values", "Target Values",
    )

    def __init__(
        self,
        validations_config: Dict[str, Any] | None = None,
        reporting_settings: Dict[str, Any] | None = None,
    ):
        settings = (validations_config or {}).get("comparison_settings", {})
        self.full_comparison = settings.get("full_data_comparison")
        self.config = (
            self.full_comparison.get("excel_reports")
            if isinstance(self.full_comparison, dict) else None
        )
        self.evidence = (
            self.full_comparison.get("evidence")
            if isinstance(self.full_comparison, dict) else None
        )
        self.reporting_settings = reporting_settings
        self._consolidated_writer = (
            ConsolidatedExcelReportWriter(reporting_settings)
            if isinstance(reporting_settings, dict) and reporting_settings
            else None
        )
        if self.config is not None:
            self._validate_config()

    @property
    def row_hash_column(self) -> str:
        """The row-hash column name carried into table-level record sheets.

        An empty configured value switches the columns off entirely, so a
        deployment that does not stage a row hash sees exactly the sheets it
        saw before.
        """
        configured = (self.evidence or {}).get("row_hash_column")
        return str(configured).strip() if configured else ""

    @property
    def enabled(self) -> bool:
        return bool(self.config and self.config["enabled"])

    def write_consolidated_workbook(
        self,
        summary: RunSummary,
        tables: List[TableValidationResult],
        phase: str,
        path: Path,
    ) -> Path:
        """Write the consolidated workbook for the tables executed in `phase`."""
        if self._consolidated_writer is None:
            raise ValueError(
                "Consolidated Excel reporting requires the `reporting` "
                "configuration block; pass reporting_settings to "
                "PhaseExcelReportWriter"
            )
        return self._consolidated_writer.write(
            replace(summary, table_results=list(tables)), phase, path,
            evidence_limiter=self.exported_records,
        )

    def write_table_workbook(
        self, summary: RunSummary, table: TableValidationResult, path: Path
    ) -> Path:
        """Write the per-table workbook exactly as configured for one table."""
        self._write_workbook(
            summary, [table], self._filesystem_path(path),
            self.config["per_table"]["sheets"],
        )
        return path

    @property
    def per_table_enabled(self) -> bool:
        """Whether the per-table sheet contract is available at all."""
        return self.enabled

    def exported_records(self, values: List[Any], category: str) -> List[Any]:
        """The evidence rows the table-level sheets actually export.

        Exposed so the consolidated report can quote the same exported counts
        the table-level sheets show instead of a second, disagreeing number.
        """
        return self._limited(values, category)

    def _write_workbook(
        self,
        summary: RunSummary,
        tables: List[TableValidationResult],
        path: Path,
        sheet_keys: Iterable[str],
    ) -> None:
        from openpyxl import Workbook
        from openpyxl.styles import Alignment, Font, PatternFill
        from openpyxl.utils import get_column_letter
        from openpyxl.utils.cell import coordinate_from_string

        workbook = Workbook()
        workbook.remove(workbook.active)
        sheet_names = self.config["sheet_names"]
        status_colors = self.config["formatting"]["status_colors"]
        header_color = self.config["formatting"]["header_color"]
        header_font_color = self.config["formatting"]["header_font_color"]
        max_width = self.config["formatting"]["max_column_width"]

        def add_sheet(
            key: str, headers: List[str], rows: Iterable[List[Any]],
            sheet_name: str | None = None,
        ):
            worksheet = workbook.create_sheet(sheet_name or sheet_names[key])
            worksheet.append(["Table Name", table_name(tables[0])])
            worksheet["A1"].font = Font(bold=True)
            worksheet["B1"].font = Font(bold=True)
            worksheet.append(headers)
            for cell in worksheet[2]:
                cell.font = Font(bold=True, color=header_font_color)
                cell.fill = PatternFill("solid", fgColor=header_color)
                cell.alignment = Alignment(wrap_text=True, vertical="center")
            for row in rows:
                worksheet.append(row)
            freeze_panes = self.config["formatting"]["freeze_panes"]
            if freeze_panes:
                column, row = coordinate_from_string(freeze_panes)
                worksheet.freeze_panes = f"{column}{row + 1}"
            worksheet.auto_filter.ref = (
                f"A2:{get_column_letter(len(headers))}{worksheet.max_row}"
            )
            status_column = next(
                (index for index, value in enumerate(headers, 1) if value == "Status"),
                None,
            )
            if status_column:
                for row_number in range(3, worksheet.max_row + 1):
                    cell = worksheet.cell(row_number, status_column)
                    color = status_colors.get(str(cell.value), status_colors["DEFAULT"])
                    cell.fill = PatternFill("solid", fgColor=color)
            for column in worksheet.columns:
                width = max((len(str(cell.value or "")) for cell in column), default=0)
                worksheet.column_dimensions[get_column_letter(column[0].column)].width = min(
                    width + 2, max_width
                )

        factories = {
            "summary": lambda: self._summary_rows(summary, tables),
            "table_summary": lambda: self._table_rows(tables),
            "validation_checks": lambda: self._check_rows(tables),
            "matched_records": lambda: self._matched_rows(tables),
            "mismatched_records": lambda: self._mismatched_rows(tables),
            "excess_in_source": lambda: self._missing_rows(tables, "excess_in_source"),
            "excess_in_target": lambda: self._missing_rows(tables, "excess_in_target"),
            "reconciliation": lambda: self._reconciliation_rows(tables),
        }
        for key in sheet_keys:
            headers, rows = factories[key]()
            add_sheet(key, headers, rows)
        # Valid UPDATE evidence belongs to the individual table workbook. It
        # remains private runtime evidence, so JSON/HTML and the consolidated
        # workbook do not gain a second table-level record feed.
        update_rows = self._incremental_update_rows(tables)
        if any(
            str(table.migration_phase or "").strip().casefold() == "incremental"
            for table in tables
        ):
            add_sheet(
                "incremental_updates",
                list(self.INCREMENTAL_UPDATE_COLUMNS),
                ([row[column] for column in self.INCREMENTAL_UPDATE_COLUMNS]
                 for row in update_rows),
                self.INCREMENTAL_UPDATES_SHEET,
            )
            server_2_rows = self._server_2_match_rows(tables)
            add_sheet(
                "server_2_matches",
                list(self.SERVER_2_MATCH_COLUMNS),
                ([row[column] for column in self.SERVER_2_MATCH_COLUMNS]
                 for row in server_2_rows),
                self.SERVER_2_MATCHES_SHEET,
            )
        filesystem_path(path.parent).mkdir(parents=True, exist_ok=True)
        save_target = str(path.resolve())
        if os.name == "nt" and not save_target.startswith("\\\\?\\"):
            save_target = "\\\\?\\" + save_target
        workbook.save(save_target)

    def _incremental_update_rows(self, tables):
        rows = []
        for table in tables:
            if str(table.migration_phase or "").strip().casefold() != "incremental":
                continue
            for payload in getattr(
                table, "_incremental_update_evidence", ()
            )[:INCREMENTAL_UPDATE_EVIDENCE_LIMIT]:
                changed = payload.get("mismatched_columns") or []
                identity = payload.get("primary_key") or payload.get("row_hash")
                previous = {item.get("column"): item.get("previous") for item in changed}
                current = {item.get("column"): item.get("current") for item in changed}
                rows.append({
                    "Table ID": self._cell(payload.get("table_id", table.table_id)),
                    "Source Table": self._cell(payload.get("source_table", table.source_fqn)),
                    "Target Table": self._cell(payload.get("target_table", table.target_fqn)),
                    "Scenario / Strategy": self._cell(payload.get("resolved_incremental_strategy")),
                    "Record Identifier": self._cell(primary_key_label(table, identity)),
                    "PK / Discriminator": self._cell(primary_key_label(table, identity)),
                    "Classification": "UPDATE / VALID UPDATE",
                    "Reason": self._cell(payload.get("failure_reason")),
                    "Changed Columns": self._cell("; ".join(
                        f"{item.get('column')}: {item.get('previous')!r} -> {item.get('current')!r}"
                        for item in changed
                    )),
                    "Previous Target Values": self._cell(previous),
                    "Current Values": self._cell(current),
                    "Previous Audit Boundary": self._cell(
                        payload.get("historical_audit_value")
                        or payload.get("historical_watermark_value")
                    ),
                    "Current Audit / Watermark Boundary": self._cell(
                        payload.get("target_audit_value")
                        or payload.get("target_watermark_value")
                        or payload.get("source_watermark_value")
                    ),
                })
        return rows

    def _server_2_match_rows(self, tables):
        rows = []
        for table in tables:
            if str(table.migration_phase or "").strip().casefold() != "incremental":
                continue
            evidence = list(getattr(table, "server_2_match_evidence", ()))
            if not evidence:
                evidence = list(getattr(
                    table, "_server_2_match_evidence", ()
                ))
            if not evidence:
                # Backup handoff for report trees that reconstruct the outer
                # TableValidationResult but preserve its completed checks.
                for check in table.checks:
                    if check.check_name == "record_comparison":
                        evidence = list(getattr(
                            check, "_server_2_match_evidence", ()
                        ))
                        if not evidence:
                            evidence = list(
                                (check.details or {}).get(
                                    "server_2_match_evidence", ()
                                )
                            )
                        if evidence:
                            break
            for payload in evidence[:SERVER_2_MATCH_EVIDENCE_LIMIT]:
                identity = payload.get("record_identifier")
                rows.append({
                    "Table ID": self._cell(payload.get("table_id", table.table_id)),
                    "Source Table": self._cell(payload.get("source_table", table.source_fqn)),
                    "Target Table": self._cell(payload.get("target_table", table.target_fqn)),
                    "Server 2": self._cell(payload.get("server_2")),
                    "Match Method": self._cell(payload.get("match_method")),
                    "Record Identifier": self._cell(primary_key_label(table, identity)),
                    "PK / Discriminator": self._cell(primary_key_label(table, identity)),
                    "Occurrences": int(payload.get("occurrences", 1) or 1),
                    "Status": self._cell(payload.get("status", "MATCHED IN SERVER_2")),
                    "Source Values": self._cell(payload.get("source_values")),
                    "Target Values": self._cell(payload.get("target_values")),
                })
        return rows

    def _summary_rows(self, summary, tables):
        headers = ["Metric", "Value", "Status"]
        statuses = self.config["overall_statuses"]
        counts = {status: 0 for status in statuses}
        for table in tables:
            counts[self._display_status(table.overall_status.value)] += 1
        rows = [
            ["Run ID", summary.run_id, ""],
            ["Project", summary.project_name, ""],
            ["Executed Tables", len(tables), ""],
            ["Source Total", sum(self._row_count(item, "source") or 0 for item in tables), ""],
            ["Target Total", sum(self._row_count(item, "target") or 0 for item in tables), ""],
            ["Matched Count", sum(self._evidence_totals(item)["matched_records"] for item in tables), ""],
            ["Mismatched Count", sum(self._evidence_totals(item)["mismatched_records"] for item in tables), ""],
            ["Excess In Source Count", sum(self._evidence_totals(item)["excess_in_source"] for item in tables), ""],
            ["Excess In Target Count", sum(self._evidence_totals(item)["excess_in_target"] for item in tables), ""],
            ["Selected Comparison Mode", self._detail_values(tables, "comparison_mode"), ""],
            ["Actual Comparison Strategy", self._detail_values(tables, "actual_comparison_strategy"), ""],
            ["Validation Scope", self._detail_values(tables, "validation_scope"), ""],
            ["Warnings", self._detail_values(tables, "warning_code"), "WARNING" if any(item.warn_count for item in tables) else ""],
            ["Final Status", self._summary_status(tables), self._summary_status(tables)],
        ]
        rows.extend([[f"{status} Tables", counts[status], status] for status in statuses])
        if len(tables) == 1:
            table = tables[0]
            rows[2:2] = [
                ["Table ID", table.table_id, ""],
                ["Table Name", table_name(table), ""],
                ["Source Table", table.source_fqn, ""],
                ["Target Table", table.target_fqn, ""],
            ]
        return headers, rows

    #: The resolved incremental window, reported per table. Each entry is the
    #: column heading and the ``execution_metrics`` key the orchestrator wrote
    #: from the resolved scope, so the report never restates the resolution.
    INCREMENTAL_COLUMNS = (
        ("Incremental Enabled", "incremental_enabled"),
        ("Incremental Strategy", "incremental_strategy"),
        ("Watermark Mode", "watermark_mode"),
        ("Source Watermark Column", "source_watermark_column"),
        ("Target Watermark Column", "target_watermark_column"),
        ("Previous Watermark", "previous_watermark"),
        ("Current Watermark", "current_watermark"),
        ("Previous Watermark Source", "previous_watermark_source"),
        ("Current Watermark Source", "current_watermark_source"),
        ("Key Checkpoint", "key_checkpoint"),
        ("Scope Type", "scope_type"),
        ("Full Table Strategy Used", "full_table_strategy_used"),
        ("Full Table Strategy Reason", "full_table_strategy_reason"),
    )

    def _table_rows(self, tables):
        headers = [
            "Table ID", "Table Name", "Source", "Target", "Phase", "Source Rows",
            "Target Rows", "PASS", "FAIL", "WARNING", "ERROR", "SKIP",
            "Status", "Duration (s)", "Execution Details", "Total Matched",
            "Exported Matched", "Total Mismatched", "Exported Mismatched",
            "Total Excess In Source", "Exported Excess In Source",
            "Total Excess In Target", "Exported Excess In Target",
        ]
        headers += [heading for heading, _key in self.INCREMENTAL_COLUMNS]
        rows = []
        for table in tables:
            counts = self._check_status_counts(table)
            totals = self._evidence_totals(table)
            rows.append([
                table.table_id, table_name(table), table.source_fqn, table.target_fqn,
                table.migration_phase, self._row_count(table, "source"),
                self._row_count(table, "target"), counts["PASS"], counts["FAIL"],
                counts["WARNING"], counts["ERROR"], counts["SKIP"],
                self._display_status(table.overall_status.value),
                round(table.duration_seconds, 6),
                json.dumps(table.execution_metrics, default=str),
                totals["matched_records"], len(self._limited(table.matched_records, "matched_records")),
                totals["mismatched_records"], len(self._limited(table.mismatched_records, "mismatched_records")),
                totals["excess_in_source"], len(self._limited(table.missing_in_target, "excess_in_source")),
                totals["excess_in_target"], len(self._limited(table.missing_in_source, "excess_in_target")),
            ] + [
                self._cell(table.execution_metrics.get(key))
                for _heading, key in self.INCREMENTAL_COLUMNS
            ])
        return headers, rows

    def _check_rows(self, tables):
        headers = [
            "Table ID", "Validation", "Status", "Reason", "Source Value",
            "Target Value", "Delta", "Duration (s)",
        ]
        rows = []
        for table in tables:
            for check in table.checks:
                rows.append([
                    table.table_id, check.check_name,
                    self._display_status(check.status.value),
                    check.details.get("error_reason")
                    or check.details.get("failure_reason") or check.message,
                    self._cell(check.source_value), self._cell(check.target_value),
                    self._cell(check.delta),
                    check.details.get("execution_time_seconds", check.details.get("execution_time")),
                ])
        return headers, rows

    def _matched_rows(self, tables):
        headers = ["Table ID", "Configured Key", "Business Key", "Source Hash", "Target Hash", "Status", "Timestamp"]
        headers += self._row_hash_headers("source", "target")
        rows = []
        for table in tables:
            rows.extend([
                [table.table_id, primary_key_label(table, item.primary_key), item.business_key, item.source_hash,
                 item.target_hash, item.match_status, item.timestamp.isoformat()]
                + self._row_hash_values(
                    item.source_row_hash_value, item.target_row_hash_value
                )
                for item in self._limited(table.matched_records, "matched_records")
            ])
        return headers, rows

    def _mismatched_rows(self, tables):
        headers = ["Table ID", "Configured Key", "Column", "Source Value", "Target Value", "Difference", "Timestamp"]
        headers += self._row_hash_headers("source", "target")
        rows = []
        for table in tables:
            rows.extend([
                [table.table_id, primary_key_label(table, item.primary_key), item.column_name,
                 self._cell(item.source_value), self._cell(item.target_value),
                 item.difference_type, item.timestamp.isoformat()]
                + self._row_hash_values(
                    item.source_row_hash_value, item.target_row_hash_value
                )
                for item in self._limited(table.mismatched_records, "mismatched_records")
            ])
        return headers, rows

    def _missing_rows(self, tables, category):
        headers = ["Table ID", "Configured Key", "Record Data", "Location", "Timestamp"]
        # A record that exists on one side only carries that side's own row
        # hash. The opposite side has no such record, so it has no value to
        # report and none is ever borrowed from the side that does.
        side = "source" if category == "excess_in_source" else "target"
        headers += self._row_hash_headers(side)
        rows = []
        for table in tables:
            items = (
                table.missing_in_target
                if category == "excess_in_source" else table.missing_in_source
            )
            rows.extend([
                [table.table_id, primary_key_label(table, item.primary_key, side), item.record_data,
                 item.location, item.timestamp.isoformat()]
                + self._row_hash_values(item.row_hash_value)
                for item in self._limited(items, category)
            ])
        return headers, rows

    def _row_hash_headers(self, *sides: str) -> List[str]:
        """Row-hash headers for the given sides, or none when unconfigured."""
        column = self.row_hash_column
        if not column:
            return []
        return [f"{side.title()} {column}" for side in sides]

    def _row_hash_values(self, *values: Any) -> List[Any]:
        """Row-hash cells, blank wherever the table does not expose the column."""
        if not self.row_hash_column:
            return []
        return [
            "" if value is None or str(value) == "" else str(value)
            for value in values
        ]

    def _reconciliation_rows(self, tables):
        headers = [
            "Table ID", "Source Rows", "Target Rows", "Records Checked",
            "Total Matched", "Exported Matched", "Total Mismatched",
            "Exported Mismatched", "Total Excess In Source",
            "Exported Excess In Source", "Total Excess In Target",
            "Exported Excess In Target", "Status",
        ]
        rows = []
        for table in tables:
            totals = self._evidence_totals(table)
            comparison = self._comparison_details(table)
            rows.append([
                table.table_id, self._row_count(table, "source"),
                self._row_count(table, "target"), comparison.get("records_checked", 0),
                totals["matched_records"], len(self._limited(table.matched_records, "matched_records")),
                totals["mismatched_records"], len(self._limited(table.mismatched_records, "mismatched_records")),
                totals["excess_in_source"], len(self._limited(table.missing_in_target, "excess_in_source")),
                totals["excess_in_target"], len(self._limited(table.missing_in_source, "excess_in_target")),
                self._display_status(table.overall_status.value),
            ])
        return headers, rows

    def _evidence_totals(self, table):
        details = self._comparison_details(table)
        names = self.full_comparison["count_names"]
        return {
            "matched_records": int(details.get(names["matched"], details.get("matched_records", len(table.matched_records))) or 0),
            "mismatched_records": int(details.get(names["mismatched"], details.get("mismatch_records", len(table.mismatched_records))) or 0),
            "excess_in_source": int(details.get(names["excess_in_source"], details.get("excess_in_source", details.get("missing_in_target", len(table.missing_in_target)))) or 0),
            "excess_in_target": int(details.get(names["excess_in_target"], details.get("excess_in_target", details.get("missing_in_source", len(table.missing_in_source)))) or 0),
        }

    @staticmethod
    def _comparison_details(table):
        check = next((item for item in table.checks if item.check_name == "record_comparison"), None)
        return check.details if check else {}

    @staticmethod
    def _row_count(table, side):
        metric = table.execution_metrics.get(f"{side}_row_count")
        if metric is not None:
            return metric
        check = next((item for item in table.checks if item.check_name == "row_count"), None)
        return getattr(check, f"{side}_value", None) if check else None

    def _check_status_counts(self, table):
        counts = {status: 0 for status in self.config["overall_statuses"]}
        for check in table.checks:
            counts[self._display_status(check.status.value)] += 1
        return counts

    def _display_status(self, status):
        return self.config["status_display"][str(status)]

    def _limited(self, values, category):
        export_all_field = {
            "excess_in_source": "export_all_excess_source",
            "excess_in_target": "export_all_excess_target",
        }.get(category)
        if export_all_field and self.evidence[export_all_field]:
            return values
        limit_field = {
            "matched_records": "matched_records_limit",
            "mismatched_records": "mismatch_sample_limit",
            "excess_in_source": "excess_source_limit",
            "excess_in_target": "excess_target_limit",
        }[category]
        return values[: self.evidence[limit_field]]

    def _detail_values(self, tables, field):
        values = {
            str(self._comparison_details(item).get(field))
            for item in tables if self._comparison_details(item).get(field) is not None
        }
        return ", ".join(sorted(values))

    def _summary_status(self, tables):
        if not tables:
            return self._display_status("SKIP")
        displayed = [self._display_status(item.overall_status.value) for item in tables]
        return next(
            status for status in self.config["summary_severity_order"]
            if status in displayed
        )

    @staticmethod
    def _cell(value):
        if value is None or isinstance(value, (str, int, float, bool, datetime)):
            return value
        return json.dumps(value, default=str)

    #: A standard report tree is deep, so every workbook is opened through the
    #: one helper that keeps long Windows paths openable.
    _filesystem_path = staticmethod(filesystem_path)

    def _validate_config(self):
        config = self.config
        if not isinstance(config, dict) or not isinstance(config.get("enabled"), bool):
            raise ValueError("comparison_settings.full_data_comparison.excel_reports.enabled must be boolean")
        if not config["enabled"]:
            return
        # Output paths belong to report_output alone. A path declared here
        # would be a second, competing output tree.
        declared_paths = sorted({
            "base_path", "timestamp_format", "phase_folders", "run_folder",
            "excel_folder", "consolidated",
        }.intersection(config))
        if declared_paths:
            raise ValueError(
                "comparison_settings.full_data_comparison.excel_reports "
                "must not declare report output paths; move them to "
                "report_output in the execution configuration: "
                + ", ".join(declared_paths)
            )
        names = config.get("sheet_names")
        if not isinstance(names, dict) or set(names) != self.REQUIRED_SHEETS:
            raise ValueError("comparison_settings.full_data_comparison.excel_reports.sheet_names must define every supported sheet")
        if len(set(names.values())) != len(names) or any(
            not isinstance(value, str) or not value or len(value) > 31
            for value in names.values()
        ):
            raise ValueError("Excel sheet names must be unique, non-empty, and at most 31 characters")
        if not isinstance(self.evidence, dict):
            raise ValueError("comparison_settings.full_data_comparison.evidence is required")
        # The consolidated workbook's sheet and column contract belongs to the
        # `reporting` YAML block so that every phase shares one definition.
        # Here, only the per-table sheet list is described.
        per_table = config.get("per_table")
        section = (
            "comparison_settings.full_data_comparison.excel_reports.per_table"
        )
        if not isinstance(per_table, dict) or "sheets" not in per_table:
            raise ValueError(f"{section}.sheets is required")
        if not isinstance(per_table["sheets"], list) or not per_table["sheets"] or any(
            item not in self.REQUIRED_SHEETS for item in per_table["sheets"]
        ):
            raise ValueError(f"{section}.sheets is invalid")
        statuses = config.get("overall_statuses")
        severity = config.get("summary_severity_order")
        mapping = config.get("status_display")
        if (
            not isinstance(statuses, list) or not statuses
            or not isinstance(severity, list) or set(severity) != set(statuses)
            or not isinstance(mapping, dict)
        ):
            raise ValueError("Excel status reporting configuration is incomplete")
        if not set(mapping.values()).issubset(set(statuses)):
            raise ValueError("Excel status display values must use configured overall_statuses")
        formatting = config.get("formatting")
        if not isinstance(formatting, dict) or not isinstance(
            formatting.get("max_column_width"), int
        ) or formatting["max_column_width"] <= 0:
            raise ValueError("Excel formatting configuration is incomplete")
        colors = formatting.get("status_colors")
        if not isinstance(colors, dict) or "DEFAULT" not in colors:
            raise ValueError("Excel status_colors.DEFAULT is required")
