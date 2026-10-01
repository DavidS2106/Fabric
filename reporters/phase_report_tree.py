"""
reporters/phase_report_tree.py
──────────────────────────────
Writes the Excel, JSON and HTML reports of one validation phase into the run
folder that :class:`~reporters.report_paths.ReportPathResolver` already
resolved::

    <run root>/Excel/Consolidated/<consolidated workbook>
    <run root>/Excel/Tables/<TABLE_ID>/<table workbook>
    <run root>/JSON/<consolidated payload>
    <run root>/JSON/Tables/<TABLE_ID>/<table payload>
    <run root>/HTML/<consolidated document>

This module owns report *content* only. It never builds a phase folder, a run
folder or a file name: those come from `report_output` through the resolver,
so the same tree serves the packager and every other writer. A table folder is
created for a table only when that table actually executed in the current run,
so the tree never advertises work that did not happen. Which formats are
written, and whether table-wise reports exist for a format at all, is the
resolved layout's decision.
"""

from __future__ import annotations

import json
import logging
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.config_loader import ReportingConfig
from core.models import RunSummary, TableValidationResult

from .all_reporters import HTMLReporter, JSONReporter
from .consolidated_excel_reporter import ConsolidatedExcelReportWriter
from .phase_excel_reporter import PhaseExcelReportWriter
from .table_identity import primary_key_label, table_name
from .report_paths import ReportPathError, ResolvedReportPaths, filesystem_path


logger = logging.getLogger(__name__)


class PhaseReportTreeError(ReportPathError):
    """Raised when a phase's reports cannot be written as configured."""


class PhaseReportTreeWriter:
    """Write the consolidated and table-wise reports of one run."""

    def __init__(
        self,
        reporting_settings: Dict[str, Any] | None = None,
        validations_config: Dict[str, Any] | None = None,
    ):
        self.reporting_settings = reporting_settings or {}
        self.validations_config = validations_config or {}
        self._excel_writer = PhaseExcelReportWriter(self.validations_config)
        self._consolidated_writer = (
            ConsolidatedExcelReportWriter(self.reporting_settings)
            if self.reporting_settings else None
        )

    # ── the run root this writer will use ───────────────────────────────

    def target_directories(self, paths: ResolvedReportPaths) -> List[Path]:
        """Every directory this writer may create, for the shared-root check."""
        wanted: List[Path] = []
        for layout in paths.formats:
            if not layout.enabled:
                continue
            wanted.append(layout.root)
            if layout.consolidated is not None:
                wanted.append(layout.consolidated.parent)
        return wanted

    # ── validation phases ───────────────────────────────────────────────

    def write(
        self, summary: RunSummary, paths: ResolvedReportPaths
    ) -> Dict[str, Path]:
        """Write every enabled report of `paths.phase` into its run folder."""
        phase = paths.phase
        # Only the tables this phase actually executed are reported.
        phase_summary = replace(
            summary,
            table_results=[
                item for item in summary.table_results
                if item.migration_phase == phase
            ],
        )
        paths.create_directories()
        written: Dict[str, Path] = {}
        written.update(self._write_consolidated(phase_summary, paths))
        written.update(self._write_tables(phase_summary, paths))
        return written

    def _write_consolidated(
        self, phase_summary: RunSummary, paths: ResolvedReportPaths
    ) -> Dict[str, Path]:
        written: Dict[str, Path] = {}
        for layout in paths.formats:
            path = layout.consolidated
            if not layout.enabled or path is None:
                continue
            filesystem_path(path.parent).mkdir(parents=True, exist_ok=True)
            if layout.name == "excel":
                self._write_consolidated_excel(phase_summary, paths, path)
            else:
                reporter = JSONReporter if layout.name == "json" else HTMLReporter
                reporter(
                    ReportingConfig(
                        formats=[layout.name],
                        output_dir=str(filesystem_path(path.parent)),
                    ),
                    phase_summary,
                    filename_stem=path.stem,
                ).generate()
            written[f"{paths.phase}_consolidated_{layout.name}"] = path
            self._log(paths, "CONSOLIDATED", layout.name, path)
        return written

    def _write_consolidated_excel(
        self, phase_summary: RunSummary, paths: ResolvedReportPaths, path: Path
    ) -> None:
        if self._consolidated_writer is None:
            raise PhaseReportTreeError(
                "The consolidated workbook needs the `reporting` "
                "configuration block that declares its sheets and columns"
            )
        # The consolidated exported counts must agree with the table-level
        # evidence sheets, so both honour the same configured export limits.
        self._consolidated_writer.write(
            phase_summary, paths.phase, path,
            evidence_limiter=(
                self._excel_writer.exported_records
                if self._excel_writer.enabled else None
            ),
        )

    def _write_tables(
        self, phase_summary: RunSummary, paths: ResolvedReportPaths
    ) -> Dict[str, Path]:
        written: Dict[str, Path] = {}
        layouts = [
            layout for layout in paths.formats
            if layout.enabled and layout.per_table_enabled
        ]
        if not layouts:
            return written
        if any(layout.name == "excel" for layout in layouts) and (
            not self._excel_writer.per_table_enabled
        ):
            raise PhaseReportTreeError(
                "Table-wise Excel reports need comparison_settings."
                "full_data_comparison.excel_reports to be enabled so the "
                "per-table sheets stay described by one configuration"
            )
        for table in phase_summary.table_results:
            for layout in layouts:
                path = paths.table_path(layout.name, table.table_id)
                filesystem_path(path.parent).mkdir(parents=True, exist_ok=True)
                if layout.name == "excel":
                    self._excel_writer.write_table_workbook(
                        phase_summary, table, path
                    )
                elif layout.name == "json":
                    filesystem_path(path).write_text(
                        json.dumps(
                            self.table_payload(
                                phase_summary, table, paths.phase
                            ),
                            indent=2,
                            default=str,
                        ),
                        encoding="utf-8",
                    )
                else:
                    raise PhaseReportTreeError(
                        f"Table-wise {layout.name} reports are not supported; "
                        f"disable {layout.name}.per_table"
                    )
                written[
                    f"{paths.phase}_table_{layout.name}:{table.table_id}"
                ] = path
                self._log(paths, "TABLE", layout.name, path, table.table_id)
        return written

    @staticmethod
    def _log(
        paths: ResolvedReportPaths,
        scope: str,
        report_format: str,
        path: Path,
        table_id: Optional[str] = None,
    ) -> None:
        logger.info(
            "run_id=%s phase=%s table_id=%s report_scope=%s report_format=%s "
            "report_path=%s generation_status=SUCCESS",
            paths.run_id, paths.phase, table_id or "-", scope,
            report_format.upper(), path,
        )

    # ── table-wise JSON payload ─────────────────────────────────────────

    #: The resolved incremental window, as the orchestrator recorded it on the
    #: table's execution metrics. Reported verbatim so the report describes the
    #: window that was actually read rather than re-deriving one.
    INCREMENTAL_FIELDS = (
        "incremental_enabled",
        "incremental_strategy",
        "watermark_mode",
        "source_watermark_column",
        "target_watermark_column",
        "previous_watermark",
        "current_watermark",
        "previous_watermark_source",
        "current_watermark_source",
        "key_checkpoint",
        "scope_type",
        "full_table_strategy_used",
        "full_table_strategy_reason",
        "primary_key_limitation",
    )

    @staticmethod
    def incremental_payload(table: TableValidationResult) -> Dict[str, Any]:
        """The resolved incremental execution detail for one table."""
        metrics = table.execution_metrics or {}
        return {
            name: metrics.get(name)
            for name in PhaseReportTreeWriter.INCREMENTAL_FIELDS
            if name in metrics
        }

    @staticmethod
    def table_payload(
        summary: RunSummary, table: TableValidationResult, phase: str
    ) -> Dict[str, Any]:
        """Complete validation detail for one table, including its evidence."""
        return {
            "run_id": summary.run_id,
            "project_name": summary.project_name,
            "phase": phase,
            "started_at": summary.started_at.isoformat(),
            "completed_at": (
                summary.completed_at.isoformat() if summary.completed_at else None
            ),
            "table": {
                "table_id": table.table_id,
                "table_name": table_name(table),
                "source_system": table.source_system,
                "source_fqn": table.source_fqn,
                "target_fqn": table.target_fqn,
                "target_layer": table.target_layer,
                "migration_phase": table.migration_phase,
                "tags": list(table.tags),
                "overall_status": table.overall_status.value,
                "started_at": table.started_at.isoformat(),
                "completed_at": (
                    table.completed_at.isoformat() if table.completed_at else None
                ),
                "duration_seconds": table.duration_seconds,
                "error_message": table.error_message,
                "execution_metrics": table.execution_metrics,
                "incremental": PhaseReportTreeWriter.incremental_payload(table),
            },
            "check_counts": {
                "pass": table.pass_count,
                "fail": table.fail_count,
                "warning": table.warn_count,
                "framework_error": table.framework_error_count,
                "config_error": table.config_error_count,
                "skip": table.skip_count,
                "total": table.total_checks,
                "pass_rate_pct": table.pass_rate_pct,
            },
            "checks": [
                {
                    "check_name": check.check_name,
                    "status": check.status.value,
                    "source_value": check.source_value,
                    "target_value": check.target_value,
                    "delta": check.delta,
                    "delta_pct": check.delta_pct,
                    "message": check.message,
                    "details": check.details,
                }
                for check in table.checks
            ],
            "record_evidence": {
                "matched_records": [
                    {
                        "primary_key": primary_key_label(table, item.primary_key),
                        "business_key": item.business_key,
                        "source_hash": item.source_hash,
                        "target_hash": item.target_hash,
                        "match_status": item.match_status,
                        "timestamp": item.timestamp.isoformat(),
                    }
                    for item in table.matched_records
                ],
                "mismatched_records": [
                    {
                        "primary_key": primary_key_label(table, item.primary_key),
                        "column_name": item.column_name,
                        "source_value": item.source_value,
                        "target_value": item.target_value,
                        "difference_type": item.difference_type,
                        "timestamp": item.timestamp.isoformat(),
                    }
                    for item in table.mismatched_records
                ],
                "missing_in_source": [
                    {
                        "primary_key": primary_key_label(table, item.primary_key, "target"),
                        "record_data": item.record_data,
                        "location": item.location,
                        "timestamp": item.timestamp.isoformat(),
                    }
                    for item in table.missing_in_source
                ],
                "missing_in_target": [
                    {
                        "primary_key": primary_key_label(table, item.primary_key),
                        "record_data": item.record_data,
                        "location": item.location,
                        "timestamp": item.timestamp.isoformat(),
                    }
                    for item in table.missing_in_target
                ],
            },
        }
