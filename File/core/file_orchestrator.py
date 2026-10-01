"""The File validation flow, from discovery to the existing report tree.

What this module does:

    resolve the File configuration
        -> resolve the phase's source folder
        -> discover (and expand) the source files
        -> resolve headers, delimiters and formats
        -> file_to_file : match and stage each pair separately
           files_to_table: consolidate logically into one staged dataset
        -> resolve the target (Lakehouse file, or the project's Fabric SQL)
        -> hand the prepared datasets to the EXISTING validation engine
        -> write the EXISTING reports, into File/output
        -> record file-level reconciliation

What this module never does: validate. Row count, key validation, null
validation, duplicate detection, record comparison, sampling, hashing, audit
validation, evidence and reporting are the existing framework's, unchanged,
and which of them run for a phase stays owned by ``config/validations``.
"""

from __future__ import annotations

import logging
import shutil
import time
import uuid
from fnmatch import fnmatchcase
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from core.config_loader import MigrationConfig, TableMapping, TargetConfig
from core.models import (
    CheckResult,
    RunSummary,
    TableValidationResult,
    ValidationStatus,
)
from core.orchestrator import ValidationOrchestrator
from reporters.phase_package_reporter import PhasePackageReporter
from reporters.phase_report_tree import PhaseReportTreeWriter
from reporters.report_paths import (
    ReportPathResolver,
    assert_writers_share_run_root,
    filesystem_path,
)

from .archive_handler import ArchiveHandler
from .fabric_file_reader import FabricFileAccessError, FabricFileReader
from .file_config_loader import (
    FileConfigError,
    FileConfigLoader,
    FileFrameworkSettings,
    FileTableConfig,
)
from .file_consolidator import FileConsolidator, SchemaCompatibilityError
from .file_discovery import DiscoveredFile, DiscoveryResult, FileDiscovery
from .file_matcher import FileMatcher, MatchResult
from .file_reader import FileReader, FileReadError
from .file_reconciliation import (
    KIND_FILE,
    KIND_LOGICAL_TABLE,
    PROCESSING_CONSOLIDATED,
    PROCESSING_NOT_EXECUTED,
    PROCESSING_REJECTED,
    PROCESSING_SKIPPED,
    PROCESSING_VALIDATED,
    FileReconciliationLedger,
    FileReconciliationRecord,
)
from .header_resolver import HeaderResolutionError, HeaderResolver
from .target_resolver import (
    LakehouseFileTarget,
    SqlTableTarget,
    TargetResolver,
)
from .validation_adapter import (
    FileStagingError,
    FileStagingStore,
    FileStagingResourceError,
    TargetBinding,
    build_table_mapping,
    business_column_mappings,
    staging_connection,
    staging_target_connection,
)

logger = logging.getLogger(__name__)

#: Ingestion outcomes reported through the existing result model, so an
#: unmatched or unreadable file appears in the phase report like any other
#: finding instead of vanishing into a log line.
INGESTION_CHECK = "file_ingestion"
TARGET_RESOLUTION_CHECK = "file_target_resolution"

#: Configured actions -> the status the ingestion result carries.
ACTION_STATUS = {
    "fail": ValidationStatus.FAIL,
    "skip": ValidationStatus.SKIP,
    "warn": ValidationStatus.WARNING,
}


@dataclass
class FilePhaseOutcome:
    """Everything one phase of File validation produced."""

    phase: str
    summary: RunSummary
    reports: Dict[str, Path] = field(default_factory=dict)
    packages: Dict[str, Path] = field(default_factory=dict)
    reconciliation: Dict[str, Path] = field(default_factory=dict)
    published: Dict[str, List[Path]] = field(default_factory=dict)

    @property
    def status(self) -> ValidationStatus:
        return self.summary.overall_status


@dataclass
class _PhasePlan:
    """The datasets one phase will validate, grouped by their target."""

    sources: Dict[str, Any] = field(default_factory=dict)
    groups: Dict[str, Tuple[TargetConfig, List[TableMapping]]] = field(
        default_factory=dict
    )
    ingestion_results: List[TableValidationResult] = field(default_factory=list)
    stores: List[FileStagingStore] = field(default_factory=list)


class FileValidationRunner:
    """Run File -> Fabric validation through the existing framework."""

    def __init__(
        self,
        settings: FileFrameworkSettings,
        connections: Mapping[str, Mapping[str, Any]],
        tables: Sequence[FileTableConfig],
        config: MigrationConfig,
        execution_config_path: Path,
        project_root: Path,
        spark: Any = None,
        output_root: Optional[Path] = None,
        staging_root: Optional[Path] = None,
    ):
        self.settings = settings
        self.connections = connections
        self.tables = list(tables)
        self.config = config
        self.execution_config_path = Path(execution_config_path)
        self.project_root = Path(project_root)
        self.spark = spark
        self.output_root = (
            Path(output_root) if output_root
            else settings.output.reports_path(self.project_root)
        )

        self.staging_root = (
            Path(staging_root).expanduser().resolve()
            if staging_root is not None
            else _resolve(project_root, settings.staging.directory)
        )
        self.archive_root = _resolve(
            project_root, settings.archive.extract_directory
        )
        self.archives = ArchiveHandler(settings, self.archive_root)
        self.discovery = FileDiscovery(settings, self.archives)
        self.reader = FileReader(settings)
        self.headers = HeaderResolver(settings)
        self.consolidator = FileConsolidator(settings, self.reader, self.headers)
        self.targets = TargetResolver(
            settings, connections, config, self.execution_config_path,
            self.staging_root / "target_cache",
        )

    # ── construction ──────────────────────────────────────────────────

    @classmethod
    def from_project(
        cls,
        execution_config_path: str | Path,
        config: MigrationConfig,
        *,
        spark: Any = None,
        output_root: Optional[str | Path] = None,
        staging_root: Optional[str | Path] = None,
        project_root: Optional[str | Path] = None,
    ) -> Optional["FileValidationRunner"]:
        """Build the runner, or ``None`` when File validation is not in use."""
        root = Path(project_root) if project_root else Path(__file__).resolve().parents[2]
        loader = FileConfigLoader.for_project(root)
        if not loader.available():
            return None
        loader.load_environment()
        settings = loader.load_settings()
        if not settings.enabled:
            logger.info("File validation is disabled in config_setting.yml")
            return None
        tables = loader.load_tables()
        if not tables:
            logger.info("File validation is enabled but no File table is configured")
            return None
        return cls(
            settings=settings,
            connections=loader.load_connections(),
            tables=tables,
            config=config,
            execution_config_path=Path(execution_config_path),
            project_root=root,
            spark=spark,
            output_root=Path(output_root) if output_root else None,
            staging_root=Path(staging_root) if staging_root else None,
        )

    # ── entry point ───────────────────────────────────────────────────

    def run(
        self,
        phases: Sequence[str],
        *,
        filter_table_ids: Optional[Sequence[str]] = None,
        filter_tags: Optional[Sequence[str]] = None,
        filter_comparison_modes: Optional[Sequence[str]] = None,
        only_checks: Optional[Sequence[str]] = None,
        skip_checks: Optional[Sequence[str]] = None,
    ) -> List[FilePhaseOutcome]:
        selected = [
            table for table in self.tables
            if _selected(
                table, filter_table_ids, filter_tags, filter_comparison_modes
            )
        ]
        if not selected:
            logger.info("No File table matches the run's filters")
            return []

        resolver = ReportPathResolver.from_execution_config(
            self.execution_config_path, root_folder=self.output_root
        )
        report_tree = PhaseReportTreeWriter(
            getattr(getattr(self.config, "reporting", None), "settings", None),
            getattr(self.config, "validations", {}),
        )
        packager = PhasePackageReporter()

        # A killed or disk-full run cannot reach the normal finally cleanup.
        # Never let its partial SQLite databases consume the next run's disk
        # budget when staging retention is disabled.
        if not self.settings.staging.retain_after_run:
            shutil.rmtree(self.staging_root, ignore_errors=True)
        self.archives.prepare()
        outcomes: List[FilePhaseOutcome] = []
        try:
            for phase in phases:
                outcome = self._run_phase(
                    phase, selected, resolver, report_tree, packager,
                    only_checks, skip_checks,
                )
                if outcome is not None:
                    outcomes.append(outcome)
        finally:
            self._cleanup()
        return outcomes

    # ── one phase ─────────────────────────────────────────────────────

    def _run_phase(
        self,
        phase: str,
        tables: Sequence[FileTableConfig],
        resolver: ReportPathResolver,
        report_tree: PhaseReportTreeWriter,
        packager: PhasePackageReporter,
        only_checks: Optional[Sequence[str]],
        skip_checks: Optional[Sequence[str]],
    ) -> Optional[FilePhaseOutcome]:
        eligible = [table for table in tables if table.source.path_for(phase)]
        if not eligible:
            logger.info(
                "No File table configures a source path for phase '%s'", phase
            )
            return None

        run_id = f"{self.config.run_id_prefix}-FILE-{uuid.uuid4().hex[:8].upper()}"
        ledger = FileReconciliationLedger(run_id=run_id, phase=phase)

        print(f"\n{'=' * 65}")
        print(f"  File -> Fabric validation — phase '{phase}' — run {run_id}")
        print(f"  File tables in scope: {len(eligible)}")
        print(f"{'=' * 65}\n", flush=True)

        plan = self._prepare_phase(phase, eligible, ledger)
        if not plan.groups and not plan.ingestion_results:
            logger.info("Phase '%s' produced no File dataset to validate", phase)
            return None

        started = time.perf_counter()
        results: List[TableValidationResult] = list(plan.ingestion_results)
        try:
            for key, (target_config, table_maps) in plan.groups.items():
                if not table_maps:
                    continue
                summary = self._validate_group(
                    phase, key, target_config, table_maps, plan.sources,
                    only_checks, skip_checks,
                )
                results.extend(summary.table_results)
        finally:
            # Validation is complete (or could not start), so reports only
            # need the in-memory result objects. Release SQLite handles first.
            for store in plan.stores:
                store.close()

        if not self.settings.staging.retain_after_run:
            # Do this before report generation, not only in run()'s outer
            # finally. A disk-full staging failure may have consumed every
            # remaining byte with a partial SQLite database and target cache;
            # OpenPyXL then cannot create even its small ZIP members. Each
            # later phase recreates its stores/cache normally.
            shutil.rmtree(self.staging_root, ignore_errors=True)
            logger.info(
                "Released File staging storage before report generation | "
                "phase=%s | path=%s",
                phase, self.staging_root,
            )

        summary = RunSummary(
            project_name=self.config.project_name,
            source_systems=sorted(plan.sources),
        )
        summary.run_id = run_id
        summary.table_results = results
        summary.completed_at = datetime.utcnow()
        summary.execution_metrics.update({
            "file_validation": True,
            "file_phase": phase,
            "file_tables_validated": len(results),
            "file_reconciliation_counts": ledger.counts(),
            "file_phase_duration_seconds": round(time.perf_counter() - started, 6),
        })

        ledger.apply_validation_results(results)
        outcome = FilePhaseOutcome(phase=phase, summary=summary)
        self._write_reports(outcome, resolver, report_tree, packager)
        outcome.reconciliation = ledger.write(
            self.settings.output.reconciliation_path(self.project_root),
            self.settings.output.reconciliation_formats,
        )
        for name, path in outcome.reconciliation.items():
            print(f"  File reconciliation ({name}): {path}", flush=True)
        return outcome

    def _validate_group(
        self,
        phase: str,
        target_key: str,
        target_config: TargetConfig,
        table_maps: Sequence[TableMapping],
        sources: Mapping[str, Any],
        only_checks: Optional[Sequence[str]],
        skip_checks: Optional[Sequence[str]],
    ) -> RunSummary:
        """Run the *existing* orchestrator over one target's File tables."""
        control = replace(
            self.config.execution_control,
            full_run=True,
            phases=[phase],
            table_ids=[],
            table_names=[],
            layers=[],
            servers=[],
            databases=[],
            schemas=[],
        )
        group_config = replace(
            self.config,
            tables=list(table_maps),
            sources=dict(sources),
            target=target_config,
            target_ref=target_key,
            source_ref=next(iter(sources)),
            execution_control=control,
        )
        orchestrator = ValidationOrchestrator(config=group_config, spark=self.spark)
        return orchestrator.run(
            filter_phases=[phase],
            only_checks=list(only_checks) if only_checks else None,
            skip_checks=list(skip_checks) if skip_checks else None,
        )

    # ── preparation ───────────────────────────────────────────────────

    def _prepare_phase(
        self,
        phase: str,
        tables: Sequence[FileTableConfig],
        ledger: FileReconciliationLedger,
    ) -> _PhasePlan:
        plan = _PhasePlan()
        staging_directory = self.staging_root / _token(phase)
        source_store = FileStagingStore(
            staging_directory, "source", "file_source"
        )
        target_store = FileStagingStore(
            staging_directory, "target", "fabric_file"
        )
        plan.stores.extend((source_store, target_store))

        source_name = "file_source"
        plan.sources[source_name] = staging_connection(source_name, source_store)
        staged_target_name = "fabric_lakehouse_file"
        staged_target = staging_target_connection(
            staged_target_name, target_store
        )

        for table in tables:
            try:
                self._prepare_table(
                    table, phase, plan, ledger,
                    source_store, target_store,
                    source_name, staged_target_name, staged_target,
                )
            except FileReadError as exc:
                # The configuration successfully selected a supported reader;
                # the physical file content could not be parsed.  That is a
                # data-validation failure (bad delimiter/quoting/truncated or
                # corrupt content), not a configuration error.
                logger.error(
                    "File content validation failed | table=%s | phase=%s | %s",
                    table.table_id, phase, exc,
                )
                plan.ingestion_results.append(_ingestion_result(
                    table, phase, INGESTION_CHECK, ValidationStatus.FAIL,
                    str(exc),
                    details={
                        "validation_category": "file_data_validation",
                        "failure_reason": str(exc),
                        "exception_category": "FILE_CONTENT_INVALID",
                    },
                ))
                ledger.add(FileReconciliationRecord(
                    table_id=table.table_id,
                    logical_table_name=table.logical_table_name,
                    comparison_mode=table.comparison_mode,
                    validation_table_id=table.table_id,
                    record_kind=KIND_LOGICAL_TABLE,
                    source_path=table.source.path_for(phase),
                    processing_status=PROCESSING_NOT_EXECUTED,
                    rejected_status="data_validation_failed",
                    message=str(exc),
                ))
            except FileStagingResourceError as exc:
                logger.error(
                    "File staging resource failure | table=%s | phase=%s | %s",
                    table.table_id, phase, exc,
                )
                plan.ingestion_results.append(_ingestion_result(
                    table, phase, INGESTION_CHECK, ValidationStatus.ERROR,
                    str(exc),
                    details={
                        "validation_category": "file_staging",
                        "failure_reason": str(exc),
                        "exception_category": "DISK_FULL",
                    },
                ))
                ledger.add(FileReconciliationRecord(
                    table_id=table.table_id,
                    logical_table_name=table.logical_table_name,
                    comparison_mode=table.comparison_mode,
                    validation_table_id=table.table_id,
                    record_kind=KIND_LOGICAL_TABLE,
                    source_path=table.source.path_for(phase),
                    processing_status=PROCESSING_NOT_EXECUTED,
                    rejected_status="resource_error",
                    message=str(exc),
                ))
            except (
                FileConfigError,
                FileStagingError,
                HeaderResolutionError,
                SchemaCompatibilityError,
                FabricFileAccessError,
            ) as exc:
                logger.error(
                    "File ingestion failed | table=%s | phase=%s | %s",
                    table.table_id, phase, exc,
                )
                plan.ingestion_results.append(_ingestion_result(
                    table, phase, INGESTION_CHECK, ValidationStatus.CONFIG_ERROR,
                    str(exc),
                ))
                ledger.add(FileReconciliationRecord(
                    table_id=table.table_id,
                    logical_table_name=table.logical_table_name,
                    comparison_mode=table.comparison_mode,
                    validation_table_id=table.table_id,
                    record_kind=KIND_LOGICAL_TABLE,
                    source_path=table.source.path_for(phase),
                    processing_status=PROCESSING_NOT_EXECUTED,
                    rejected_status="ingestion_error",
                    message=str(exc),
                ))
        return plan

    def _prepare_table(
        self,
        table: FileTableConfig,
        phase: str,
        plan: _PhasePlan,
        ledger: FileReconciliationLedger,
        source_store: FileStagingStore,
        target_store: FileStagingStore,
        source_name: str,
        staged_target_name: str,
        staged_target: TargetConfig,
    ) -> None:
        relative = table.source.path_for(phase)
        discovered = self._discover_source(table, relative)
        logger.info(
            "File discovery | table=%s | phase=%s | folder=%s | files=%d | "
            "rejected=%d",
            table.table_id, phase, discovered.scanned_path,
            len(discovered.files), len(discovered.rejected),
        )
        for display, reason in discovered.rejected:
            ledger.add(FileReconciliationRecord(
                table_id=table.table_id,
                logical_table_name=table.logical_table_name,
                comparison_mode=table.comparison_mode,
                source_filename=Path(display).name,
                source_path=str(discovered.scanned_path / display),
                processing_status=PROCESSING_REJECTED,
                rejected_status="unreadable",
                message=reason,
            ))

        target = self.targets.resolve(table)
        if table.comparison_mode == "file_to_file":
            self._prepare_file_to_file(
                table, phase, plan, ledger, discovered.files, target,
                source_store, target_store, source_name,
                staged_target_name, staged_target,
            )
        else:
            self._prepare_files_to_table(
                table, phase, plan, ledger, discovered.files, target,
                source_store, source_name,
            )

    def _discover_source(
        self, table: FileTableConfig, relative: str,
    ) -> DiscoveryResult:
        """Discover local files or fetch a configured OneLake source.

        OneLake files are downloaded into the existing bounded staging area,
        then handed to the unchanged local discovery/read/stage pipeline.
        """
        connection = self.targets.source_connection(table.source.connection)
        if not self._is_onelake_source(table, connection):
            return self.discovery.discover(
                self._source_root(table), relative,
                table.source.pattern, table.source.format,
            )

        remote_relative = "" if str(relative).strip() in {"", "."} else relative
        cache_root = self._materialize_onelake_source(
            table, connection, remote_relative
        )
        return self.discovery.discover(
            cache_root, remote_relative,
            table.source.pattern, table.source.format,
        )

    @staticmethod
    def _is_onelake_source(table: FileTableConfig, connection: Any) -> bool:
        return bool(
            str(getattr(connection, "type", "") or "").strip().lower()
            == "fabric_lakehouse"
            or str(table.source.connection).strip().lower()
            == "fabric_lakehouse_source"
        )

    def _materialize_onelake_source(
        self, table: FileTableConfig, connection: Any, relative: str,
    ) -> Path:
        """Download the configured OneLake cohort into existing staging."""
        if str(getattr(connection, "type", "") or "").strip().lower() != "fabric_lakehouse":
            connection = self.targets.file_connection(
                "target", "fabric_lakehouse_file"
            )
        cache_root = self.staging_root / "source_cache" / _token(table.table_id)
        (cache_root / relative).mkdir(parents=True, exist_ok=True)
        reader = FabricFileReader(connection, self.settings, cache_root)
        for item in reader.list_files(relative):
            if (
                fnmatchcase(item.name, table.source.pattern)
                or fnmatchcase(item.relative_path, table.source.pattern)
            ):
                reader.fetch(item, relative)
        return cache_root

    # ── mode A: file -> file ──────────────────────────────────────────

    def _prepare_file_to_file(
        self,
        table: FileTableConfig,
        phase: str,
        plan: _PhasePlan,
        ledger: FileReconciliationLedger,
        files: Sequence[DiscoveredFile],
        target: LakehouseFileTarget,
        source_store: FileStagingStore,
        target_store: FileStagingStore,
        source_name: str,
        staged_target_name: str,
        staged_target: TargetConfig,
    ) -> None:
        available = target.reader.list_files(target.relative_path)
        matcher = FileMatcher(table.target.matching)
        matches = matcher.match_all(files, available)
        folder = target.reader.describe(target.relative_path)

        for index, match in enumerate(matches):
            if not match.matched:
                self._record_unmatched(table, phase, plan, ledger, match, folder)
                continue
            self._prepare_pair(
                table, phase, plan, ledger, match, target, folder, index,
                source_store, target_store, source_name,
                staged_target_name, staged_target,
            )

    def _prepare_pair(
        self,
        table: FileTableConfig,
        phase: str,
        plan: _PhasePlan,
        ledger: FileReconciliationLedger,
        match: MatchResult,
        target: LakehouseFileTarget,
        folder: str,
        index: int,
        source_store: FileStagingStore,
        target_store: FileStagingStore,
        source_name: str,
        staged_target_name: str,
        staged_target: TargetConfig,
    ) -> None:
        source_file = match.source
        options = table.source.options_for(source_file.extension)
        decision = self.headers.resolve(source_file, options)
        source_plan = self.reader.plan(source_file, options, decision)
        source_columns = self.reader.columns(source_plan)
        staged_source = source_store.stage(
            f"{table.logical_table_name}__{source_file.stem}",
            self.reader.read_chunks(source_plan, self.settings.staging.chunk_rows),
            expected_columns=source_columns,
            contribution_key=source_file.display_path,
        )

        local_target = target.reader.fetch(match.target, target.relative_path)
        target_file = DiscoveredFile(
            path=Path(local_target),
            display_path=match.target.relative_path,
            name=match.target.name,
            stem=match.target.stem,
            extension=match.target.extension,
            reader=_reader_for(match.target.extension),
            size_bytes=match.target.size_bytes,
        )
        target_options = table.target.options_for(target_file.extension)
        # A file-to-file target represents the same logical file as its
        # matched source.  Older table-config objects (and configurations
        # written before target.file.options was introduced) may not carry a
        # target header option even though the source states it explicitly.
        # Excel headers cannot be sniffed reliably, so preserve the resolved
        # source decision instead of falling back to ``auto`` and rejecting
        # an otherwise valid .xls/.xlsx pair.  An explicit target setting
        # still wins.
        target_options.setdefault("header", source_plan.header.has_header)
        target_decision = self.headers.resolve(target_file, target_options)
        target_plan = self.reader.plan(target_file, target_options, target_decision)
        target_columns = self.reader.columns(target_plan)
        staged_target_table = target_store.stage(
            f"{table.logical_table_name}__{match.target.stem}",
            self.reader.read_chunks(target_plan, self.settings.staging.chunk_rows),
            expected_columns=target_columns,
            contribution_key=match.target.relative_path,
        )

        mappings = list(table.compare_column_mappings) or business_column_mappings(
            source_columns, target_columns, self._audit_columns()
        )
        validation_table_id = f"{table.table_id}__{_token(source_file.stem)}"
        table_map = build_table_mapping(
            table_id=validation_table_id,
            phase=phase,
            layer=table.layer,
            tags=table.tags,
            source_system=source_name,
            source_table=staged_source.name,
            source_location=str(source_file.path),
            target=TargetBinding(
                system=staged_target_name,
                schema="main",
                table=staged_target_table.name,
                database=staged_target.database,
                server=folder,
                location=f"{folder}/{match.target.relative_path}",
            ),
            key_mappings=table.key_mappings,
            compare_column_mappings=mappings,
            incremental=table.incremental,
        )
        _group(plan, staged_target_name, staged_target).append(table_map)

        ledger.add(FileReconciliationRecord(
            table_id=table.table_id,
            logical_table_name=table.logical_table_name,
            comparison_mode=table.comparison_mode,
            validation_table_id=validation_table_id,
            record_kind=KIND_FILE,
            source_filename=source_file.name,
            source_path=str(source_file.path),
            archive_name=source_file.archive_name,
            file_format=source_file.extension,
            delimiter=source_plan.delimiter,
            delimiter_source=source_plan.delimiter_source,
            header_status=decision.status,
            target_kind=target.kind,
            target_filename=match.target.name,
            target_path=f"{folder}/{match.target.relative_path}",
            source_row_count=staged_source.row_count,
            target_row_count=staged_target_table.row_count,
            staged_source_table=staged_source.name,
            staged_target_table=staged_target_table.name,
            processing_status=PROCESSING_VALIDATED,
            message=match.message,
        ))

    def _record_unmatched(
        self,
        table: FileTableConfig,
        phase: str,
        plan: _PhasePlan,
        ledger: FileReconciliationLedger,
        match: MatchResult,
        folder: str,
    ) -> None:
        status = ACTION_STATUS.get(match.action, ValidationStatus.FAIL)
        validation_table_id = f"{table.table_id}__{_token(match.source.stem)}"
        plan.ingestion_results.append(_ingestion_result(
            table, phase, TARGET_RESOLUTION_CHECK, status, match.message,
            table_id=validation_table_id,
            source_fqn=match.source.display_path,
            target_fqn=folder,
            details={
                "match_status": match.status,
                "configured_action": match.action,
                "candidates": list(match.candidates),
            },
        ))
        ledger.add(FileReconciliationRecord(
            table_id=table.table_id,
            logical_table_name=table.logical_table_name,
            comparison_mode=table.comparison_mode,
            validation_table_id=validation_table_id,
            record_kind=KIND_FILE,
            source_filename=match.source.name,
            source_path=str(match.source.path),
            archive_name=match.source.archive_name,
            file_format=match.source.extension,
            target_kind="fabric_lakehouse_file",
            target_path=folder,
            processing_status=(
                PROCESSING_SKIPPED if match.action == "skip"
                else PROCESSING_REJECTED
            ),
            skipped_status=match.status if match.action == "skip" else None,
            rejected_status=match.status if match.action != "skip" else None,
            message=match.message,
        ))

    # ── mode B: files -> table ────────────────────────────────────────

    def _prepare_files_to_table(
        self,
        table: FileTableConfig,
        phase: str,
        plan: _PhasePlan,
        ledger: FileReconciliationLedger,
        files: Sequence[DiscoveredFile],
        target: SqlTableTarget,
        source_store: FileStagingStore,
        source_name: str,
    ) -> None:
        if not files:
            raise FileConfigError(
                f"{table.table_id}: no readable source file was discovered for "
                f"phase '{phase}'"
            )
        planned, skipped = self.consolidator.plan_files(
            files, lambda item: table.source.options_for(item.extension)
        )
        for display, reason in skipped:
            ledger.add(FileReconciliationRecord(
                table_id=table.table_id,
                logical_table_name=table.logical_table_name,
                comparison_mode=table.comparison_mode,
                source_filename=Path(display).name,
                source_path=display,
                processing_status=PROCESSING_SKIPPED,
                skipped_status="schema_unresolved",
                message=reason,
            ))
        if not planned:
            raise SchemaCompatibilityError(
                f"{table.table_id}: every discovered file was skipped, so no "
                "logical dataset could be formed"
            )

        compatibility = str(
            table.source.consolidation.get("compatibility", "strict")
        )
        # Compatibility is proved before a single row is staged. A table that
        # switches the check off takes the first file's column order as the
        # dataset's, and a later file that cannot supply it still fails.
        reference = (
            self.consolidator.reference_columns(planned, compatibility)
            if table.source.consolidation.get("validate_schema_compatibility", True)
            else planned[0].columns
        )
        result = self.consolidator.consolidate(
            table.logical_table_name, planned, source_store, compatibility,
            self.settings.staging.chunk_rows, reference=reference,
        )
        logger.info(
            "File consolidation | table=%s | phase=%s | files=%d | rows=%d",
            table.table_id, phase, len(result.planned), result.staged.row_count,
        )

        mappings = list(table.compare_column_mappings) or [
            {"source_column": column, "target_column": column}
            for column in result.reference_columns
            if column.casefold() not in {
                item.casefold() for item in self._audit_columns()
            }
        ]
        table_map = build_table_mapping(
            table_id=table.table_id,
            phase=phase,
            layer=table.layer,
            tags=table.tags,
            source_system=source_name,
            source_table=result.staged.name,
            source_location=str(
                self._source_root(table) / (table.source.path_for(phase) or "")
            ),
            target=TargetBinding(
                system=target.reference,
                schema=target.schema,
                table=target.table,
                database=target.database,
                server=getattr(target.connection, "host", None),
                location=target.location,
            ),
            key_mappings=table.key_mappings,
            compare_column_mappings=mappings,
            incremental=table.incremental,
        )
        _group(plan, target.reference, target.connection).append(table_map)

        contributions = dict(result.staged.contributions)
        for item in result.planned:
            ledger.add(FileReconciliationRecord(
                table_id=table.table_id,
                logical_table_name=table.logical_table_name,
                comparison_mode=table.comparison_mode,
                validation_table_id=table.table_id,
                record_kind=KIND_FILE,
                source_filename=item.discovered.name,
                source_path=str(item.discovered.path),
                archive_name=item.discovered.archive_name,
                file_format=item.discovered.extension,
                delimiter=item.plan.delimiter,
                delimiter_source=item.plan.delimiter_source,
                header_status=item.plan.header.status,
                target_kind=target.kind,
                target_table=target.location,
                source_row_count=contributions.get(item.discovered.display_path),
                staged_source_table=result.staged.name,
                processing_status=PROCESSING_CONSOLIDATED,
            ))
        ledger.add(FileReconciliationRecord(
            table_id=table.table_id,
            logical_table_name=table.logical_table_name,
            comparison_mode=table.comparison_mode,
            validation_table_id=table.table_id,
            record_kind=KIND_LOGICAL_TABLE,
            source_filename=f"{len(result.planned)} file(s)",
            source_path=str(
                self._source_root(table) / (table.source.path_for(phase) or "")
            ),
            file_format=table.source.format,
            target_kind=target.kind,
            target_table=target.location,
            source_row_count=result.staged.row_count,
            staged_source_table=result.staged.name,
            processing_status=PROCESSING_VALIDATED,
            message=(
                f"{len(result.planned)} file(s) consolidated logically under "
                f"'{compatibility}' schema compatibility"
            ),
        ))

    # ── reporting ─────────────────────────────────────────────────────

    def _write_reports(
        self,
        outcome: FilePhaseOutcome,
        resolver: ReportPathResolver,
        report_tree: PhaseReportTreeWriter,
        packager: PhasePackageReporter,
    ) -> None:
        paths = resolver.resolve(outcome.phase, outcome.summary.run_id)
        paths.log_resolution()
        assert_writers_share_run_root(paths, (report_tree, packager))
        outcome.reports = report_tree.write(outcome.summary, paths)
        for name, path in outcome.reports.items():
            print(f"  {name}: {path}", flush=True)
        outcome.packages = packager.generate(paths)
        for name, path in outcome.packages.items():
            print(f"  {name}: {path}", flush=True)
        outcome.published = self._publish(outcome, paths)

    def _publish(
        self, outcome: FilePhaseOutcome, paths: Any
    ) -> Dict[str, List[Path]]:
        """Copy the generated reports into the File output folders.

        The reports themselves are the existing framework's; this only files
        copies where ``file_validation.output`` says File results live.
        """
        reports_root = self.settings.output.reports_path(self.project_root)
        evidence_root = self.settings.output.evidence_path(self.project_root)
        published: Dict[str, List[Path]] = {}
        for name, path in outcome.reports.items():
            # The report tree is deep by design, so both ends of the copy are
            # opened through the framework's own long-path helper.
            source = filesystem_path(Path(path))
            if not source.is_file():
                logger.warning(
                    "File output not published; report is missing | report=%s",
                    name,
                )
                continue
            table_id = name.split(":", 1)[1] if ":" in name else None
            fmt = name.split(":", 1)[0].rsplit("_", 1)[-1]
            destination_root = (
                evidence_root / _token(table_id) if table_id
                else reports_root / fmt
            )
            filesystem_path(destination_root).mkdir(parents=True, exist_ok=True)
            destination = filesystem_path(destination_root / Path(path).name)
            try:
                shutil.copy2(source, destination)
            except OSError as exc:
                logger.warning(
                    "File output publication failed | report=%s | error=%s",
                    name, exc,
                )
                continue
            published.setdefault(
                "evidence" if table_id else fmt, []
            ).append(destination)
        for kind, items in published.items():
            print(f"  File output ({kind}): {len(items)} file(s)", flush=True)
        return published

    # ── helpers ───────────────────────────────────────────────────────

    def _source_root(self, table: FileTableConfig) -> Path:
        connection = self.targets.source_connection(table.source.connection)
        if self._is_onelake_source(table, connection):
            # Compatibility guard for older _prepare_table implementations
            # that still ask for a source root before discovery. A OneLake
            # source is materialized into staging and never requires
            # params.root_path.
            return self._materialize_onelake_source(table, connection, "")
        configured = connection.param("root_path", required=True)
        return _resolve(self.project_root, configured)

    def _audit_columns(self) -> Sequence[str]:
        """The framework's own configured target audit columns."""
        validations = getattr(self.config, "validations", {}) or {}
        audit = validations.get("audit_validation") or {}
        columns = audit.get("audit_columns") or []
        return [str(item) for item in columns]

    def _cleanup(self) -> None:
        self.archives.cleanup()
        if self.settings.staging.retain_after_run:
            return
        shutil.rmtree(self.staging_root, ignore_errors=True)


# ── module helpers ────────────────────────────────────────────────────


def _group(
    plan: _PhasePlan, key: str, target_config: TargetConfig
) -> List[TableMapping]:
    entry = plan.groups.get(key)
    if entry is None:
        entry = (target_config, [])
        plan.groups[key] = entry
    return entry[1]


def _ingestion_result(
    table: FileTableConfig,
    phase: str,
    check_name: str,
    status: ValidationStatus,
    message: str,
    *,
    table_id: Optional[str] = None,
    source_fqn: Optional[str] = None,
    target_fqn: Optional[str] = None,
    details: Optional[Mapping[str, Any]] = None,
) -> TableValidationResult:
    """Report an ingestion outcome through the existing result model."""
    result = TableValidationResult(
        table_id=table_id or table.table_id,
        source_system=table.source.connection,
        source_fqn=source_fqn or table.logical_table_name,
        target_fqn=target_fqn or (table.target.relative_path or table.target.table or ""),
        target_layer=table.layer,
        migration_phase=phase,
        tags=list(table.tags),
    )
    result.checks.append(CheckResult(
        check_name=check_name,
        status=status,
        message=message,
        details={
            "comparison_mode": table.comparison_mode,
            "configuration_document": table.document.name,
            "final_status": status.value,
            **dict(details or {}),
        },
    ))
    result.completed_at = datetime.utcnow()
    return result


def _reader_for(extension: str) -> str:
    from .file_discovery import FORMAT_BY_EXTENSION

    reader = FORMAT_BY_EXTENSION.get(str(extension).lower().lstrip("."))
    if reader is None:
        raise FileConfigError(
            f"Fabric target files with extension '{extension}' are not "
            "supported; add it to file_validation.supported_files and use a "
            "readable format"
        )
    return reader


def _selected(
    table: FileTableConfig,
    table_ids: Optional[Sequence[str]],
    tags: Optional[Sequence[str]],
    comparison_modes: Optional[Sequence[str]] = None,
) -> bool:
    if table_ids and table.table_id not in table_ids:
        return False
    if tags and not set(tags) & set(table.tags):
        return False
    if comparison_modes and table.comparison_mode not in comparison_modes:
        return False
    return True


def _resolve(project_root: Path, configured: str) -> Path:
    path = Path(str(configured)).expanduser()
    return path if path.is_absolute() else Path(project_root) / path


def _token(value: Any) -> str:
    return "".join(
        character if character.isalnum() or character in "-_" else "_"
        for character in str(value)
    ) or "file"
