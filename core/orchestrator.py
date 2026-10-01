"""
fabric_migration_validator/core/orchestrator.py
────────────────────────────────────────────────
Orchestrates validation using dynamic execution resolution.
"""

from __future__ import annotations
import logging
import contextlib
import gc
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from .object_dependency import (
    ObjectDependenciesUnavailable, precheck_object, cache_object_failure,
    dependency_results, dependency_failures_for_exception, object_identity,
    DependencyAwareConnector,
)
from datetime import datetime
from typing import Callable, Dict, Optional

from core.capabilities import capability_report, resolve_capabilities
from core.config_loader import MigrationConfig
from core.connection_manager import ConnectionManager, sanitize_error_text
from core.checkpoint import CheckpointStore
from core.models import CheckResult, RunSummary, TableValidationResult, ValidationStatus
from core.execution_engine import ExecutionConfig, ExecutionEngineResolver
from core.instrumentation import ScanLedger
from core.resource_monitor import cleanup_framework_temp_files, resource_snapshot
from core.performance import (
    ConcurrencyGovernor,
    ExecutionResultCache,
    LargeTableScheduler,
    PerformanceLedger,
)
# FIX 4.1: Import QuerySignatureLogger for signature-only query logging
from validators.performance_utilities import QuerySignatureLogger
from core.scan_planner import (
    ROW_COUNT_KEY,
    SharedStatistics,
    SharedStatisticsBuilder,
    resolve_max_expressions,
    statistics_pushdown_available,
)
from core.strategy import StrategyResolver, estimate_row_volume
from core.validation_policy import (
    ValidationPolicies,
    ZeroRecordPolicy,
    exception_category,
    invalid_column_names,
)
from core.validation_scope import IncrementalStrategyResolver
from core.validation_registry import ValidationRegistry

from validators import (
    AggregateValidator, RowCountValidator,
    BusinessRuleValidator, DuplicateDetectionValidator,
    KeyValidationValidator, PIIScanner, ReferentialIntegrityValidator, SCDValidator,
    NullValidationValidator, RecordComparisonValidator, DataValueValidator,
    EdgeCaseValidator,
    AuditValidator,
)
from validators.validator_utils import _fqn
from validators.base_validator import BaseValidator

logger = logging.getLogger(__name__)


# Map internal check names to their section in the merged config/validations/
VALIDATION_MAP = {
    "edge_case_validation": "edge_case_validation",
    "row_count": "row_count",
    "key_validation": "key_validation",
    "null_validation": "null_validation",
    "aggregate": "aggregate_validation",
    "referential": "referential_integrity",
    "scd": "scd_validation",
    "business_rules": "business_rules",
    "pii": "pii_scan",
    "duplicate": "duplicate_check",
    "data_value_validation": "data_value_validation",
    "record_comparison": "record_level_comparison",
    "audit_validation": "audit_validation",
}

#: Numeric detail fields a configured zero-record result reports as 0, so a
#: validation that did not run still carries the count keys its reports read.
#: The configured comparison count aliases are added on top.
ZERO_RECORD_ZERO_COUNTS = {
    "record_comparison": (
        "records_checked",
        "records_compared",
        "matched_records",
        "mismatch_records",
        "value_mismatch_records",
        "missing_in_source",
        "missing_in_target",
    ),
}

# Validations that compute statistics are resolved at runtime from the registry.
def _get_statistic_checks():
    """Get list of statistic checks from the registry."""
    return ValidationRegistry._STATISTIC_CHECKS

VALIDATION_CATEGORIES = {
    "migration_validation": {
        "row_count",
        "aggregate",
        "record_comparison",
    },
    "data_quality_validation": {
        "key_validation",
        "null_validation",
        "referential",
        "scd",
        "pii",
        "duplicate",
    },
    "business_rule_validation": {
        "business_rules",
    },
    "data_value_validation": {
        "data_value_validation",
    },
    "edge_case_validation": {
        "edge_case_validation",
    },
    "audit_validation": {
        "audit_validation",
    },
}

class ValidationOrchestrator:
    def __init__(self, config: MigrationConfig, spark=None):
        self.config = config
        self.validation_policies = ValidationPolicies.from_config(
            config.validations
        )
        # What an empty validation scope does to the remaining validations,
        # including the result a named validation reports instead of running
        # (validations.edge_case_validation.zero_record_policy).
        self._zero_record_policy = ZeroRecordPolicy.from_config(config.validations)
        table_parallelism = config.performance.parallel_execution.table_parallelism
        self.max_workers = table_parallelism.max_workers
        self.executor_type = table_parallelism.executor_type
        self.fail_fast = table_parallelism.fail_fast

        resolver = ExecutionEngineResolver(config.performance)
        self.context = resolver.resolve(spark_session=spark)

        self.concurrency_governor = ConcurrencyGovernor(
            config.performance.parallel_execution.resolved_global_max_concurrency()
        )
        # FIX #8: Validate semaphore sizing won't bottleneck parallelism
        expected_concurrent = (
            config.performance.parallel_execution.validation_parallelism.max_workers_per_table
            * config.performance.parallel_execution.table_parallelism.max_workers
        )
        max_concurrent = config.performance.parallel_execution.max_concurrent_validations
        if max_concurrent < expected_concurrent:
            logger.warning(
                "max_concurrent_validations=%d may limit parallelism; "
                "expected_concurrent=%d (check_workers=%d × table_workers=%d)",
                max_concurrent,
                expected_concurrent,
                config.performance.parallel_execution.validation_parallelism.max_workers_per_table,
                config.performance.parallel_execution.table_parallelism.max_workers,
            )
        self.performance_ledger = PerformanceLedger(
            enabled=config.performance.performance_monitoring.enabled
        )
        self.strategy_resolver = StrategyResolver(config.performance)
        self._runtime_phase_enforcement_enabled = True

        self.conn_manager = ConnectionManager(
            config,
            context=self.context,
            concurrency_governor=self.concurrency_governor,
            performance_ledger=self.performance_ledger,
        )
        validation_parallelism = (
            config.performance.parallel_execution.validation_parallelism
        )
        self.check_workers = validation_parallelism.max_workers_per_table
        self.validation_parallel_enabled = bool(
            config.performance.parallel_execution.enabled
            and validation_parallelism.enabled
            and validation_parallelism.max_workers_per_table > 1
        )
        self._validation_slots = threading.BoundedSemaphore(
            config.performance.parallel_execution.max_concurrent_validations
        )
        # Scheduling-only ceilings for heavy scans on large tables. Every
        # validation still executes with unchanged results.
        self._large_table_scheduler = LargeTableScheduler(
            getattr(config.performance, "large_table_concurrency", None),
            config.performance.large_table,
        )
        self.checkpoint_store = CheckpointStore(config.performance.checkpoint)

        # Store validator overrides. By default, use the registry.
        self._validators = {}
        # Cache run-level metrics once — config doesn't change between tables.
        self._cached_run_metrics = None

    def _new_validator(self, name):
        """Create a new validator instance from the registry.

        Supports test overrides via _validators dict.
        """
        override = getattr(self, "_validators", {}).get(name)
        if override is not None:
            return override
        return ValidationRegistry.new_validator(name)

    def run(self, filter_tags=None, filter_layers=None, filter_phases=None,
            filter_table_ids=None, filter_table_names=None,
            only_checks=None, skip_checks=None, parallel=True,
            eligible_table_ids=None,
            phase_completed_callback: Optional[
                Callable[[str, RunSummary], None]
            ] = None) -> RunSummary:

        # Resolve execution control filters
        ctrl = self.config.execution_control
        mode = "FULL_RUN" if ctrl.full_run else "FILTERED"
        
        # Merge config filters with CLI filters
        effective_layers = filter_layers or (ctrl.layers if not ctrl.full_run else [])
        effective_ids = filter_table_ids or (ctrl.table_ids if not ctrl.full_run else [])
        effective_names = filter_table_names or (
            getattr(ctrl, "table_names", []) if not ctrl.full_run else []
        )
        effective_servers = getattr(ctrl, "servers", []) if not ctrl.full_run else []
        effective_databases = getattr(ctrl, "databases", []) if not ctrl.full_run else []
        effective_schemas = getattr(ctrl, "schemas", []) if not ctrl.full_run else []
        effective_phases = filter_phases or ctrl.phases

        summary = RunSummary(
            project_name=self.config.project_name,
            source_systems=[self.config.source_ref],
            execution_metrics=self._run_execution_metrics(),
        )
        summary.run_id = f"{self.config.run_id_prefix}-{summary.run_id}"
        
        # Apply filters
        if effective_phases:
            missing_profiles = sorted(
                set(effective_phases) - set(self.config.validation_profiles)
            )
            if missing_profiles:
                raise ValueError(
                    "Unknown validation profile phase(s): " + ", ".join(missing_profiles)
                )
            effective_phases = self._ordered_phases(effective_phases)
        tables = self._filter_tables(
            filter_tags, effective_layers, effective_phases, effective_ids,
            servers=effective_servers,
            databases=effective_databases,
            schemas=effective_schemas,
            table_names=effective_names,
            eligible_ids=eligible_table_ids,
        )

        # NEW: Log phase selection approach
        if effective_phases:
            logger.info(
                f"Phase selection: requested_phases={effective_phases}, "
                f"mode=profile_driven (not table-dependent)"
            )
            logger.info(
                f"Table discovery: total_loaded={len(self.config.tables)}, "
                f"tables_for_phases={len(tables)}"
            )

        print(f"\n{'='*65}")
        print(f"  Fabric Migration Validator — Run {summary.run_id}")
        print(f"  Execution Mode: {mode}")
        if not ctrl.full_run:
            if effective_ids: print(f"  Selected IDs  : {', '.join(effective_ids)}")
            if effective_names: print(f"  Selected Tables: {', '.join(effective_names)}")
            if effective_layers: print(f"  Selected Layers: {', '.join(effective_layers)}")
            if effective_servers: print(f"  Selected Servers: {', '.join(effective_servers)}")
            if effective_databases: print(f"  Selected Databases: {', '.join(effective_databases)}")
            if effective_schemas: print(f"  Selected Schemas: {', '.join(effective_schemas)}")
        
        print(f"  Execution Engine: {self.context.engine_type.value.upper()}")
        print(f"  Project  : {self.config.project_name}")
        print(f"  Tables   : {len(tables)} in scope")
        print(f"  Started  : {summary.started_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        
        if not tables:
             raise ValueError(f"No tables found matching the criteria. (Mode: {mode})")
        
        print(f"{'='*65}\n")

        run_args = dict(
            only_checks=only_checks,
            skip_checks=skip_checks,
            run_id=summary.run_id,
            validation_parallel=parallel,
            # _filter_tables expands each physical table into the exact
            # configured phase executions.  Never force the first selected
            # profile onto every table.
            selected_phase=None,
        )

        try:
            table_parallel_cfg = (
                self.config.performance.parallel_execution.table_parallelism
            )
            # Tables are deliberately serialized. Validation parallelism is
            # still available inside each table, where the shared statistics
            # cache can prevent duplicate scans.
            use_table_parallelism = False

            total_tables = len(tables)
            completed_tables = 0
            progress_lock = threading.Lock()
            results = []
            phase_remaining: Dict[str, int] = {}
            for table in tables:
                phase_remaining[table.migration_phase] = (
                    phase_remaining.get(table.migration_phase, 0) + 1
                )

            def validate_with_progress(table_map):
                nonlocal completed_tables
                result = self._validate_table(table_map, **run_args)
                completed_phase = None
                with progress_lock:
                    results.append(result)
                    summary.table_results = list(results)
                    completed_tables += 1
                    pct = round(100 * completed_tables / total_tables)
                    progress = (
                        f"[{completed_tables}/{total_tables}] {pct}% | "
                        f"phase={table_map.migration_phase} table={table_map.id}"
                    )
                    print(f"  {progress}", flush=True)
                    logger.info("[TABLE_PROGRESS] %s", progress)
                    phase_remaining[table_map.migration_phase] -= 1
                    if phase_remaining[table_map.migration_phase] == 0:
                        completed_phase = table_map.migration_phase
                        summary.completed_at = datetime.utcnow()
                        summary.execution_metrics.update(self._performance_summary())
                if completed_phase is not None:
                    logger.info(
                        "[PHASE_COMPLETE] phase=%s completed_tables=%d",
                        completed_phase,
                        sum(
                            1 for item in summary.table_results
                            if item.migration_phase == completed_phase
                        ),
                    )
                    if phase_completed_callback is not None:
                        phase_completed_callback(completed_phase, summary)
                return result

            if use_table_parallelism:
                worker_count = min(self.max_workers, len(tables))
                logger.info(
                    "Table parallelism enabled: %d tables across %d workers",
                    len(tables),
                    worker_count,
                )
                with ThreadPoolExecutor(
                    max_workers=worker_count,
                    thread_name_prefix="table-validation",
                ) as executor:
                    futures = [
                        executor.submit(validate_with_progress, t)
                        for t in tables
                    ]
                    for future in futures:
                        future.result()
            else:
                for table in tables:
                    validate_with_progress(table)

            summary.table_results = list(results)
            summary.completed_at = datetime.utcnow()
            summary.execution_metrics.update(self._performance_summary())

            self._print_console_summary(summary)
            return summary
        finally:
            self.conn_manager.close_all()

    def _validate_table(
        self, table_map, only_checks=None, skip_checks=None, parallel=False,
        run_id=None, validation_parallel=None, selected_phase=None,
    ):
        # The table clock: monotonic, started before anything the table does
        # (scope resolution, connections, shared statistics, every validator,
        # cleanup) and read once at the very end. Reporting the timestamp
        # difference before ``completed_at`` was set produced "0.000s" tables.
        table_started = time.perf_counter()
        table_timing: Dict[str, float] = {}
        result = TableValidationResult(
            table_id=table_map.id,
            source_system=table_map.source_system,
            source_fqn=f"{table_map.source_schema}.{table_map.source_table}",
            target_fqn=f"{table_map.target_schema}.{table_map.target_table}",
            target_layer=table_map.target_layer,
            migration_phase=table_map.migration_phase,
            tags=table_map.tags,
            execution_metrics=self._table_execution_metrics(),
            status_policy=dict(
                (self.config.comparison_settings or {}).get("status_policy", {})
            ),
        )

        src_conn = None
        tgt_conn = None
        execution_cache = None
        shared_statistics = None
        temporary_storage = getattr(
            self.config.performance, "temporary_storage", None
        )
        temporary_directory = getattr(
            temporary_storage, "directory", "output/temp"
        )
        result.execution_metrics["resources_before_validation"] = resource_snapshot(
            temporary_directory
        )
        # These values are also consumed by post-error bookkeeping. Initialize
        # them before connection/scope work so a database failure cannot mask
        # the reported table error and prevent end-of-run report generation.
        phase_to_use = selected_phase or table_map.migration_phase
        chk = {}
        try:
            profile = self.config.validation_profiles.get(phase_to_use)
            if profile is None:
                raise ValueError(
                    "Validation profile not found: "
                    f"{phase_to_use}"
                )
            checkpoint_store = getattr(self, "checkpoint_store", None)
            def read_audit_boundaries(audit_table, column):
                nonlocal tgt_conn
                if tgt_conn is None:
                    tgt_conn = self.conn_manager.get_target(fresh=False, table_context=audit_table)
                helper = RecordComparisonValidator()
                dialect = helper._dialect(tgt_conn)
                fqn = _fqn(audit_table.target_schema, audit_table.target_table, dialect)
                quoted = dialect.quote(column)
                sql = dialect.select(
                    quoted, fqn, f" WHERE {quoted} IS NOT NULL",
                    limit=2, order_by=f"{quoted} DESC", distinct=True,
                )
                frame = tgt_conn.execute_query(sql)
                return frame.iloc[:, 0].tolist()

            scope = IncrementalStrategyResolver(
                RecordComparisonValidator._filter_literal, read_audit_boundaries
            ).resolve(
                table_map,
                profile.validation_scope,
                (self.config.validations or {}).get("incremental_strategy", {}),
                (
                    checkpoint_store.load_incremental(table_map.id)
                    if checkpoint_store is not None else None
                ),
            )
            table_map = replace(table_map, validation_scope=scope.to_dict())
            result.execution_metrics.update(scope.as_details())
            if scope.blocks_validation:
                result.checks.append(CheckResult(
                    check_name="incremental_scope_resolution",
                    status=ValidationStatus(scope.policy_status or "CONFIG_ERROR"),
                    message=scope.reason or "Incremental scope could not be resolved",
                    details={**scope.as_details(), "final_status": scope.policy_status},
                ))
                result.completed_at = datetime.utcnow()
                result.measured_duration_seconds = round(
                    time.perf_counter() - table_started, 6
                )
                result.execution_metrics["validation_duration"] = result.duration_seconds
                result.execution_metrics["table_duration_seconds"] = result.duration_seconds
                return result
            connection_started = time.perf_counter()
            with self.performance_ledger.scope(
                table_id=table_map.id,
                phase=phase_to_use,
                validation_name="connection_acquisition",
            ):
                src_conn = self.conn_manager.get_source(
                    table_map.source_system, fresh=False, table_context=table_map
                )
                source_connection_seconds = time.perf_counter() - connection_started
                target_started = time.perf_counter()
                if tgt_conn is None:
                    tgt_conn = self.conn_manager.get_target(
                        fresh=False, table_context=table_map
                    )
                target_connection_seconds = time.perf_counter() - target_started
            table_timing["connection_acquisition_seconds"] = round(
                time.perf_counter() - connection_started, 6
            )
            result.execution_metrics.update(
                source_connection_acquisition_seconds=round(
                    source_connection_seconds, 6
                ),
                target_connection_acquisition_seconds=round(
                    target_connection_seconds, 6
                ),
                connection_acquisition_seconds=table_timing[
                    "connection_acquisition_seconds"
                ],
            )
            src_type = self.config.sources[table_map.source_system].type
            thresholds = self.config.thresholds
            result.execution_metrics.update(
                source_capabilities=capability_report(resolve_capabilities(src_conn)),
                target_capabilities=capability_report(resolve_capabilities(tgt_conn)),
            )
            chk = self._resolve_checks(profile, table_map, only_checks, skip_checks)
            logger.info(
                "Selected Phase: %s | Loaded Profile: %s | Enabled Validations: %s | "
                "Skipped Validations: %s",
                profile.name,
                profile.name,
                ", ".join(name for name, enabled in chk.items() if enabled) or "none",
                ", ".join(name for name, enabled in chk.items() if not enabled) or "none",
            )

            # Verify smoke_test doesn't have record_comparison enabled
            if phase_to_use == "smoke_test" and chk.get("record_comparison"):
                logger.warning(
                    "[PROFILE] smoke_test profile has record_comparison=true; "
                    "this will significantly slow down validation. "
                    "Record comparison should remain disabled for smoke testing."
                )

            val_args = (src_conn, tgt_conn, table_map, thresholds, self.context, src_type)
            execution_cache = ExecutionResultCache()
            execution_cache.validation_policies = self.validation_policies
            execution_cache.validations_config = self.config.validations
            execution_cache.scan_ledger = ScanLedger(enabled=True)

            src_conn = DependencyAwareConnector(src_conn, "source", execution_cache,
                object_identity(src_conn, table_map.source_schema, table_map.source_table))
            tgt_conn = DependencyAwareConnector(tgt_conn, "target", execution_cache,
                object_identity(tgt_conn, table_map.target_schema, table_map.target_table))
            val_args = (src_conn, tgt_conn, table_map, thresholds, self.context, src_type)

            if any(chk.values()):
                object_failures = []
                for role, connection in (("source", src_conn), ("target", tgt_conn)):
                    execution_cache.put(("object_identity", role), object_identity(
                        connection, getattr(table_map, f"{role}_schema"),
                        getattr(table_map, f"{role}_table"),
                    ))
                    failure = precheck_object(
                        connection, role, getattr(table_map, f"{role}_schema"),
                        getattr(table_map, f"{role}_table"),
                    )
                    if failure is not None:
                        cache_object_failure(execution_cache, failure)
                        object_failures.append(failure)
                if object_failures:
                    raise ObjectDependenciesUnavailable(object_failures)

            shared_statistics = self._prepare_shared_statistics(
                chk, table_map, src_conn, tgt_conn, execution_cache
            )
            if shared_statistics is not None:
                # Materialize the per-table statistics before independent
                # validators start. Each side is still allowed to run in
                # parallel, but no validator can race to issue a duplicate scan.
                prefetch_started = time.perf_counter()
                with self.performance_ledger.scope(
                    table_id=table_map.id,
                    phase=profile.name,
                    validation_name="shared_statistics",
                ):
                    with execution_cache.scan_ledger.scope(
                        table_id=table_map.id,
                        phase=profile.name,
                        validation_name="shared_statistics",
                    ):
                        execution_cache._scope_token = shared_statistics.scope_token
                        shared_statistics.prefetch()
                prefetch_elapsed = round(time.perf_counter() - prefetch_started, 6)
                table_timing["shared_statistics_seconds"] = prefetch_elapsed
                stats_details = shared_statistics.details()
                stats_details["prefetch_seconds"] = prefetch_elapsed
                stats_details.update(
                    execution_cache.scan_ledger.details_for(
                        table_map.id, profile.name, "shared_statistics"
                    )
                )
                result.execution_metrics.update(stats_details)
                logger.info(
                    "[TIMING] table=%s phase=%s operation=shared_statistics duration=%.6fs "
                    "source_stmts=%d target_stmts=%d",
                    table_map.id,
                    profile.name,
                    prefetch_elapsed,
                    stats_details.get("shared_statistics_source_statements", 0),
                    stats_details.get("shared_statistics_target_statements", 0),
                )
                logger.debug(
                    "Shared statistics prefetch completed for %s in %.3fs "
                    "(source_statements=%d, target_statements=%d)",
                    table_map.id,
                    prefetch_elapsed,
                    stats_details.get("shared_statistics_source_statements", 0),
                    stats_details.get("shared_statistics_target_statements", 0),
                )

            object_failures = [
                failure for role in ("source", "target")
                if (failure := execution_cache.peek(("object_dependency", role))) is not None
            ]
            if object_failures:
                raise ObjectDependenciesUnavailable(object_failures)

            # FIX #13: Edge case validation included in parallel task queue
            # A zero-sided dataset has a complete result here and must not produce
            # contradictory failures from validators that require records.
            continue_normal_validation = True
            edge_results_from_cache = None

            comparison_config = dict(profile.record_comparison or {})

            # Define mapping of check names to validator instances and their specific arguments
            # Note: sample and record_comparison have different signatures
            
            check_tasks = []
            edge_task = None

            # Edge-case validation is a barrier: its zero-record decision must
            # be known before validators that require records are scheduled.
            if chk.get("edge_case_validation"):
                validator = self._new_validator("edge_case_validation")
                if validator:
                    args = (
                        src_conn, tgt_conn, table_map, self.config.validations,
                        src_type, profile.name, self.config.performance,
                        execution_cache,
                    )
                    edge_task = ("edge_case_validation", validator.validate, args)

            for check_internal in VALIDATION_MAP:
                if check_internal == "edge_case_validation":
                    continue  # Already added above
                if not chk.get(check_internal):
                    continue

                validator = self._new_validator(check_internal)
                if not validator:
                    continue

                if check_internal == "record_comparison":
                    # The alternate source is deliberately lazy: it is opened
                    # only if a target-only incremental row remains unresolved
                    # after the normal source and historical-target lookups.
                    validator._server_2_connection_provider = None
                    if getattr(table_map, "server_2", None):
                        validator._server_2_connection_provider = (
                            lambda tm=table_map: self.conn_manager.get_source(
                                tm.source_system,
                                fresh=False,
                                table_context=tm,
                                server_override=tm.server_2,
                            )
                        )
                    args = (
                        src_conn, tgt_conn, table_map, self.config.validations,
                        result, src_type, comparison_config,
                        self.config.performance,
                        getattr(self, "checkpoint_store", None),
                        execution_cache,
                    )
                elif check_internal == "data_value_validation":
                    args = (
                        src_conn, tgt_conn, table_map, self.config.validations,
                        src_type, profile.name, self.config.performance,
                        execution_cache,
                    )
                elif ValidationRegistry.is_statistic_check(check_internal):
                    # Statistic validations receive the runtime configuration and
                    # the run-local cache so they can push down and share scans.
                    args = (*val_args, self.config.performance, execution_cache)
                elif check_internal == "audit_validation":
                    args = (
                        src_conn, tgt_conn, table_map, self.config.validations,
                        src_type, profile.name, self.config.performance,
                        execution_cache,
                    )
                else:
                    args = val_args
                check_tasks.append((check_internal, validator.validate, args))

            def execute_task(task, isolated=False):
                name, func, args = task
                lifecycle_started = None
                lifecycle_results = None
                lifecycle_status = None
                try:
                    failures = [failure for role in ("source", "target")
                                if (failure := execution_cache.peek(("object_dependency", role))) is not None]
                    if failures:
                        return dependency_results({name: True}, failures, include_roots=False)
                    lifecycle_started = time.perf_counter()
                    logger.info(
                        "[VALIDATOR_START] phase=%s table_id=%s validator_name=%s",
                        profile.name, table_map.id, name,
                    )
                    check_results = self._execute_timed_check(
                        name, func, args, table_map, profile.name,
                        run_id, isolated, src_conn, tgt_conn, execution_cache,
                    )
                    lifecycle_results = check_results
                    if check_results:
                        return check_results
                except MemoryError as exc:
                    lifecycle_status = ValidationStatus.ERROR.value
                    # Rule 13: Separate MemoryError handler
                    logger.error(
                        "Validator OUT_OF_MEMORY | table=%s | validator=%s | error_type=%s (Rule 13)",
                        table_map.id, name, type(exc).__name__,
                        exc_info=False  # Rule 9: Don't log full traceback
                    )
                    error_result = (
                        self._validator_exception_result(name, table_map, exc)
                    )
                    self._raise_if_fatal(exc)
                    return [error_result]
                except Exception as e:
                    lifecycle_status = ValidationStatus.ERROR.value
                    # A connection that stayed unavailable after the centralized
                    # retry budget is a dependency the validation could not
                    # execute against, not an application defect: it reports
                    # NOT_EXECUTED with its structured root category, exactly as
                    # when the failure surfaced through the shared cache.
                    dependency_failures = dependency_failures_for_exception(
                        e, execution_cache, consumer=name
                    )
                    if dependency_failures is not None:
                        logger.warning(
                            "Validator not executed | table=%s | validator=%s | "
                            "dependency=%s | error_type=%s",
                            table_map.id, name,
                            ",".join(failure.category for failure in dependency_failures),
                            type(e).__name__,
                        )
                        self._raise_if_fatal(e)
                        include_roots = any(
                            execution_cache.peek(("object_dependency", failure.role)) is not failure
                            for failure in dependency_failures
                        )
                        return dependency_results(
                            {name: True}, dependency_failures, include_roots=include_roots
                        )
                    # Rule 9-11: Safe logging - no SQL, params, or full traceback
                    logger.error(
                        "Validator failed | table=%s | validator=%s | error_type=%s (Rule 9-11)",
                        table_map.id, name, type(e).__name__,
                        exc_info=False  # Rule 11: Don't log full traceback
                    )
                    error_result = (
                        self._validator_exception_result(name, table_map, e)
                    )
                    self._raise_if_fatal(e)
                    return [error_result]
                finally:
                    if lifecycle_started is not None:
                        duration = time.perf_counter() - lifecycle_started
                        statuses = {
                            check.status.value
                            for check in (lifecycle_results or [])
                            if check is not None
                        }
                        status = lifecycle_status or (
                            next(iter(statuses)) if len(statuses) == 1 else (
                                "NO_RESULT" if not statuses else "MIXED"
                            )
                        )
                        logger.info(
                            "[VALIDATOR_FINISH] phase=%s table_id=%s "
                            "validator_name=%s status=%s duration=%.6fs",
                            profile.name, table_map.id, name, status, duration,
                        )
                return []

            if edge_task is not None:
                result.checks.extend(execute_task(edge_task))
                zero_record_check = None
                for check in result.checks:
                    if check.check_name == "zero_record_dataset_check":
                        if check.source_value is not None:
                            execution_cache.put(("row_count", "source"), check.source_value)
                        if check.target_value is not None:
                            execution_cache.put(("row_count", "target"), check.target_value)
                        continue_normal_validation = check.details.get(
                            "continue_normal_validation", False
                        )
                        zero_record_check = check
                if not continue_normal_validation:
                    # The configured zero-record policy stops downstream
                    # validation. A validation that declares a result for the
                    # observed condition reports that result instead of being
                    # dropped; it still never runs, so no sample, lookup, hash,
                    # batch or temporary resource is created for it. Every other
                    # validation is dropped exactly as before.
                    check_tasks = self._zero_record_result_tasks(
                        check_tasks, zero_record_check, table_map, profile.name
                    )

            requested_validation_parallel = (
                self.validation_parallel_enabled
                if validation_parallel is None
                else bool(validation_parallel)
            )
            run_checks_in_parallel = bool(
                requested_validation_parallel
                and self.validation_parallel_enabled
                and len(check_tasks) > 1
                # Target-driven incremental checks do not share a source
                # predicate. Running their independent source/target reads at
                # the same time can exhaust the connector pool: one worker may
                # log VALIDATOR_FINISH while the orchestrator remains blocked
                # on a different future, so table finalization and reporting
                # never start. Preserve every profile-enabled check, but run
                # this scope deterministically through the sequential path.
                and scope.source_scope_available
            )
            if (
                len(check_tasks) > 1
                and requested_validation_parallel
                and self.validation_parallel_enabled
                and not scope.source_scope_available
            ):
                logger.info(
                    "[VALIDATION_PARALLELISM] phase=%s table_id=%s enabled=false "
                    "reason=target_driven_source_scope_unavailable checks=%d",
                    profile.name, table_map.id, len(check_tasks),
                )
            checks_started = time.perf_counter()
            if run_checks_in_parallel:
                # futures are consumed in task order so report/check ordering is
                # identical to sequential execution even though independent
                # checks overlap.
                # FIX #7: Use fixed worker count, let executor queue excess tasks
                with ThreadPoolExecutor(
                    max_workers=self.check_workers,
                    thread_name_prefix=f"validation-{table_map.id}",
                ) as executor:
                    # DB-API/SQLAlchemy connection and cursor objects must not
                    # be shared by concurrently executing validators. The
                    # isolated path gives each worker its own source/target
                    # connection and closes both when that validator returns.
                    # Sharing the table-level connections here could leave one
                    # future blocked in a driver call after another validator
                    # had already logged VALIDATOR_FINISH, preventing table
                    # finalization and progression to the next table/phase.
                    futures = [
                        executor.submit(execute_task, task, True)
                        for task in check_tasks
                    ]
                    for task, future in zip(check_tasks, futures):
                        logger.info(
                            "[VALIDATOR_BARRIER_WAIT] phase=%s table_id=%s "
                            "validator_name=%s",
                            profile.name, table_map.id, task[0],
                        )
                        result.checks.extend(future.result())
                        logger.info(
                            "[VALIDATOR_BARRIER_RELEASE] phase=%s table_id=%s "
                            "validator_name=%s",
                            profile.name, table_map.id, task[0],
                        )
            else:
                for task in check_tasks:
                    result.checks.extend(execute_task(task))
            table_timing["validators_seconds"] = round(
                time.perf_counter() - checks_started, 6
            )

            for role in ("source", "target"):
                failure = execution_cache.peek(("object_dependency", role))
                if failure is not None:
                    result.checks.append(failure.check())

            self._reconcile_target_driven_row_count(scope, result.checks)

            # Post-execution: Process edge case results to update cache and control flow
            for check in result.checks:
                if check.check_name == "zero_record_dataset_check":
                    if check.source_value is not None:
                        execution_cache.put(("row_count", "source"), check.source_value)
                    if check.target_value is not None:
                        execution_cache.put(("row_count", "target"), check.target_value)
                    # Update continue_normal_validation based on edge case result
                    continue_normal_validation = check.details.get(
                        "continue_normal_validation", False
                    )

        except ObjectDependenciesUnavailable as exc:
            blocked_results = dependency_results(chk, exc.failures)
            for check in blocked_results:
                check.details.update(table_id=table_map.id, phase=profile.name)
            result.checks.extend(blocked_results)
        except MemoryError as exc:
            # PHASE 2 FIX: Rules 13-15 - Separate MemoryError handler with cleanup
            result.error_message = "OUT_OF_MEMORY"
            result.status = ValidationStatus.ERROR

            # Rule 9-11: Safe logging (type only, not full traceback or memory details)
            try:
                logger.error(
                    "Table %s OUT_OF_MEMORY | phase=%s | error_type=%s | rule=13-15",
                    table_map.id,
                    getattr(table_map, "migration_phase", "unknown"),
                    type(exc).__name__,
                    exc_info=False  # Rule 9: Never log full traceback
                )
            except Exception:
                pass  # Rule 12: Logging failure doesn't hide original exception

            result.checks.append(
                self._validator_exception_result("orchestrator", table_map, exc)
            )

            # Rule 15: Guaranteed cleanup on MemoryError
            try:
                if execution_cache is not None:
                    execution_cache.clear()
                gc.collect()
            except Exception:
                pass  # Rule 12: Cleanup failure doesn't hide original exception

            # Rule 14: Continue to next table after MemoryError (don't re-raise)

        except Exception as e:
            # Rule 9-11: Safe exception handling - type only, no full traceback
            result.error_message = type(e).__name__  # Error type, not full message

            # Rule 9-11: Safe logging
            logger.error(
                "Table %s validation failed | phase=%s | error_type=%s | rule=9-11",
                table_map.id,
                getattr(table_map, "migration_phase", "unknown"),
                type(e).__name__,
                exc_info=False  # Rule 11: Limited logging (no full traceback)
            )
            result.checks.append(
                self._validator_exception_result("orchestrator", table_map, e)
            )
            self._raise_if_fatal(e)  # Re-raise fatal errors only
        finally:
            logger.info(
                "[TABLE_FINALIZE_START] phase=%s table_id=%s checks=%d",
                phase_to_use, table_map.id, len(result.checks),
            )
            # A validator that reports FRAMEWORK_ERROR/ERROR through a check
            # never raises, so neither exception handler above runs and the
            # table-level error field stayed null while overall_status was
            # still derived as FRAMEWORK_ERROR from the checks. Populate the
            # existing field from the first failing check. Set only when an
            # exception has not already supplied a reason.
            if not result.error_message:
                result.error_message = self._table_error_message(result.checks)
            try:
                result.execution_metrics["resources_after_validation_before_cleanup"] = (
                    resource_snapshot(temporary_directory)
                )
                logger.info(
                    "[TABLE_FINALIZE_STAGE] phase=%s table_id=%s stage=resource_snapshot status=FINISH",
                    phase_to_use, table_map.id,
                )
            except Exception as exc:
                logger.warning(
                    "[TABLE_FINALIZE_STAGE] phase=%s table_id=%s "
                    "stage=resource_snapshot status=SKIPPED error_type=%s",
                    phase_to_use, table_map.id, type(exc).__name__,
                )
            cleanup_started = time.perf_counter()

            cache_cleanup_started = time.perf_counter()
            released = {"cache_entries_released": 0, "cache_locks_released": 0}
            try:
                if execution_cache is not None:
                    released = execution_cache.clear()
                logger.info(
                    "[TABLE_FINALIZE_STAGE] phase=%s table_id=%s stage=cache_cleanup "
                    "status=FINISH entries=%d locks=%d",
                    phase_to_use, table_map.id,
                    released.get("cache_entries_released", 0),
                    released.get("cache_locks_released", 0),
                )
            except Exception as exc:
                logger.warning(
                    "[TABLE_FINALIZE_STAGE] phase=%s table_id=%s "
                    "stage=cache_cleanup status=SKIPPED error_type=%s",
                    phase_to_use, table_map.id, type(exc).__name__,
                )
            cache_cleanup_seconds = round(
                time.perf_counter() - cache_cleanup_started, 6
            )
            result.execution_metrics["cache_cleanup_seconds"] = cache_cleanup_seconds
            logger.debug(
                "Cache cleanup completed for %s in %.3fs (entries=%d, locks=%d)",
                table_map.id,
                cache_cleanup_seconds,
                released.get("cache_entries_released", 0),
                released.get("cache_locks_released", 0),
            )

            shared_stats = None
            execution_cache = None

            # Successful validation must not stop between its final validator
            # log and table/report finalization to enumerate the entire Python
            # heap or run a synchronous full collection.  Large pandas samples
            # can leave millions of tracked objects, making gc.get_objects()
            # and gc.collect() appear to hang here.  Reference release above,
            # normal generational GC, and process teardown reclaim them.  The
            # explicit full collection in the MemoryError path remains because
            # that is recovery rather than successful-run bookkeeping.
            result.execution_metrics["garbage_collection_seconds"] = 0.0
            result.execution_metrics["gc_objects_before"] = None
            result.execution_metrics["gc_objects_after"] = None
            result.execution_metrics["gc_objects_freed"] = 0
            result.execution_metrics["gc_deferred"] = True
            logger.debug(
                "Full garbage collection deferred for %s; table finalization remains non-blocking",
                table_map.id,
            )

            temp_cleanup_started = time.perf_counter()
            temp_cleanup = {
                "temporary_files_removed": 0,
                "temporary_bytes_removed": 0,
                "temporary_cleanup_failures": [],
            }
            try:
                temp_cleanup = cleanup_framework_temp_files(temporary_directory)
                logger.info(
                    "[TABLE_FINALIZE_STAGE] phase=%s table_id=%s "
                    "stage=temporary_cleanup status=FINISH removed=%d",
                    phase_to_use, table_map.id,
                    temp_cleanup.get("temporary_files_removed", 0),
                )
            except Exception as exc:
                logger.warning(
                    "[TABLE_FINALIZE_STAGE] phase=%s table_id=%s "
                    "stage=temporary_cleanup status=SKIPPED error_type=%s",
                    phase_to_use, table_map.id, type(exc).__name__,
                )
            temp_cleanup_seconds = round(
                time.perf_counter() - temp_cleanup_started, 6
            )
            result.execution_metrics.update(temp_cleanup)
            result.execution_metrics["temporary_cleanup_seconds"] = temp_cleanup_seconds
            logger.debug(
                "Temporary file cleanup completed for %s in %.3fs (deleted=%d files)",
                table_map.id,
                temp_cleanup_seconds,
                temp_cleanup.get("temp_files_deleted", 0),
            )

            result.execution_metrics.update(released)
            result.execution_metrics["cleanup_seconds"] = round(
                time.perf_counter() - cleanup_started, 6
            )

            try:
                result.execution_metrics["resources_after_cleanup"] = resource_snapshot(
                    temporary_directory
                )
            except Exception as exc:
                logger.warning(
                    "[TABLE_FINALIZE_STAGE] phase=%s table_id=%s "
                    "stage=post_cleanup_snapshot status=SKIPPED error_type=%s",
                    phase_to_use, table_map.id, type(exc).__name__,
                )

            # Close the table clock here, after cleanup, and before anything
            # reports the duration. Everything below is bookkeeping on the
            # finished result.
            result.completed_at = datetime.utcnow()
            result.measured_duration_seconds = round(
                time.perf_counter() - table_started, 6
            )
            table_timing["cleanup_seconds"] = result.execution_metrics["cleanup_seconds"]
            table_timing["total_table_seconds"] = result.measured_duration_seconds
            result.execution_metrics["table_duration_seconds"] = (
                result.measured_duration_seconds
            )
            result.execution_metrics["table_timing_breakdown"] = dict(table_timing)

            logger.info(
                "Table %s validation completed: duration=%.3fs, cleanup=%.3fs, "
                "checks=%d, pass_rate=%.0f%%",
                table_map.id,
                result.duration_seconds,
                result.execution_metrics["cleanup_seconds"],
                len(result.checks),
                result.pass_rate_pct if result.pass_rate_pct is not None else 0,
            )
            logger.info(
                "[TIMING] table=%s phase=%s operation=table_total duration=%.6fs "
                "connection=%.3fs shared_statistics=%.3fs validators=%.3fs cleanup=%.3fs",
                table_map.id,
                getattr(table_map, "migration_phase", None),
                table_timing["total_table_seconds"],
                table_timing.get("connection_acquisition_seconds", 0.0),
                table_timing.get("shared_statistics_seconds", 0.0),
                table_timing.get("validators_seconds", 0.0),
                table_timing.get("cleanup_seconds", 0.0),
            )

        if result.completed_at is None:  # pragma: no cover - defensive
            result.completed_at = datetime.utcnow()
            result.measured_duration_seconds = round(
                time.perf_counter() - table_started, 6
            )

        if self._runtime_phase_enforcement_enabled:
            self._verify_phase_execution(result, chk, phase_to_use)

        scope = RecordComparisonValidator._resolved_scope(table_map)
        if scope is not None:
            scope_details = scope.as_details()
            for check in result.checks:
                check.details = {**scope_details, **dict(check.details or {})}
            # Final safety pass after resolved scope details have been attached.
            # This also covers result paths where a connector/report integration
            # reconstructed the scope without preserving source_scope_available.
            self._reconcile_target_driven_row_count(scope, result.checks)
            phase_to_check = selected_phase or table_map.migration_phase
            if (
                phase_to_check == "incremental"
                and result.overall_status == ValidationStatus.PASS
            ):
                self._commit_incremental_checkpoint(table_map, scope, result)
        result.execution_metrics["validation_duration"] = result.duration_seconds
        timed_checks = [
            check for check in result.checks
            if check.details.get("execution_time_seconds") is not None
        ]
        if timed_checks:
            slowest = max(
                timed_checks,
                key=lambda item: item.details["execution_time_seconds"],
            )
            result.execution_metrics.update(
                table_execution_time=result.duration_seconds,
                sequential_validation_time=round(sum(
                    item.details["execution_time_seconds"] for item in timed_checks
                ), 6),
                slowest_validation=slowest.check_name,
                slowest_validation_seconds=slowest.details[
                    "execution_time_seconds"
                ],
            )

            # Log detailed timing breakdown by validator
            by_validator = {}
            for check in timed_checks:
                validator_name = check.details.get("validation_name", "unknown")
                elapsed = check.details.get("execution_time_seconds", 0.0)
                if validator_name not in by_validator:
                    by_validator[validator_name] = {"count": 0, "total": 0.0}
                by_validator[validator_name]["count"] += 1
                by_validator[validator_name]["total"] += elapsed

            for validator_name, stats in sorted(
                by_validator.items(),
                key=lambda x: x[1]["total"],
                reverse=True
            ):
                logger.info(
                    "[TIMING_BREAKDOWN] table=%s validator=%s checks=%d total_time=%.6fs avg_time=%.6fs",
                    table_map.id,
                    validator_name,
                    stats["count"],
                    stats["total"],
                    stats["total"] / stats["count"] if stats["count"] > 0 else 0.0,
                )
        result.execution_metrics["retry_count"] = sum(
            getattr(connection, "retry_count", 0)
            for connection in (src_conn, tgt_conn)
            if connection is not None
        )
        key_consistency = next(
            (
                check for check in result.checks
                if check.check_name == "key_validation_consistency"
                and check.details.get("full_scan") is True
            ),
            None,
        )
        if key_consistency is not None:
            result.execution_metrics.update(
                key_validation_full_scan=True,
                key_validation_source_rows_scanned=key_consistency.details.get(
                    "source_rows_scanned", 0
                ),
                key_validation_target_rows_scanned=key_consistency.details.get(
                    "target_rows_scanned", 0
                ),
                key_validation_rows_scanned=key_consistency.details.get(
                    "rows_scanned", 0
                ),
                key_validation_distinct_keys_scanned=(
                    int(key_consistency.details.get("source_distinct_keys_scanned", 0) or 0)
                    + int(key_consistency.details.get("target_distinct_keys_scanned", 0) or 0)
                ),
            )
        record_checks = [
            check for check in result.checks if check.check_name == "record_comparison"
        ]
        row_count_checks = [
            check for check in result.checks if check.check_name == "row_count"
        ]
        if row_count_checks:
            result.execution_metrics.update(
                source_row_count=row_count_checks[-1].source_value,
                target_row_count=row_count_checks[-1].target_value,
            )
        if record_checks:
            details = record_checks[-1].details
            processed = int(details.get("records_checked", 0))
            result.execution_metrics.update(
                processed_row_count=processed,
                total_batches=int(details.get("source_batches", 0)),
                successful_batches=int(details.get("source_batches", 0)),
                failed_batches=(
                    1 if record_checks[-1].status in {
                        ValidationStatus.ERROR,
                        ValidationStatus.FRAMEWORK_ERROR,
                        ValidationStatus.CONFIG_ERROR,
                    } else 0
                ),
                throughput_rows_per_second=(
                    round(processed / result.duration_seconds, 2)
                    if result.duration_seconds > 0 else 0.0
                ),
                retry_count=(
                    result.execution_metrics["retry_count"]
                    + int(details.get("retry_count", 0))
                ),
            )
        return result

    def _verify_phase_execution(self, result, enabled_checks, phase_name):
        """Verify that only configured validators executed.

        Records phase execution compliance in metrics. Validators that ran but
        weren't configured for this phase are recorded as violations.
        """
        executed_checks = {check.check_name for check in result.checks
                          if check.check_name in VALIDATION_MAP}
        enabled_for_phase = {name for name, enabled in enabled_checks.items()
                            if enabled}

        unexpected = executed_checks - enabled_for_phase
        if unexpected:
            logger.warning(
                "Phase execution violation: %s phase for %s ran checks not in profile: %s",
                phase_name, result.table_id, ", ".join(sorted(unexpected))
            )
            result.execution_metrics["phase_execution_violations"] = sorted(unexpected)
        else:
            result.execution_metrics["phase_execution_violations"] = []

    def _zero_record_result_tasks(self, tasks, zero_record_check, table_map, phase):
        """Replace stopped validations with their configured zero-record result.

        The configured zero-record policy has stopped downstream validation.
        A validation that declares a result for the observed condition in
        ``validations.edge_case_validation.zero_record_policy`` is scheduled as
        a task that returns exactly that configured result, so it flows through
        the normal timing, status and reporting path without reading a single
        record. Validations that declare nothing are dropped, which is the
        existing behavior.
        """
        policy = getattr(self, "_zero_record_policy", None)
        if policy is None or zero_record_check is None:
            return []
        source_rows = zero_record_check.source_value
        target_rows = zero_record_check.target_value
        if not (
            isinstance(source_rows, int) and not isinstance(source_rows, bool)
            and isinstance(target_rows, int) and not isinstance(target_rows, bool)
        ):
            # The counts are unknown (the check itself failed), so no condition
            # is established and nothing is reported in place of a validation.
            return []
        condition = policy.condition_for(source_rows, target_rows)
        replacements = []
        for name, _func, _args in tasks:
            override = policy.validation_override(condition, name)
            if override is None:
                continue
            built = [self._zero_record_override_result(
                name, override, condition, source_rows, target_rows,
                table_map, phase,
            )]
            replacements.append((name, lambda results=built: results, ()))
        return replacements

    def _zero_record_override_result(
        self, name, override, condition, source_rows, target_rows, table_map, phase,
    ):
        """Build the configured result for one validation over an empty scope."""
        status = ValidationStatus(override["status"])
        details = {
            "validation_profile": phase,
            "validation_name": name,
            "table_id": getattr(table_map, "id", None),
            "execution_strategy": "zero_record_policy",
            "comparison_strategy": "zero_record_policy",
            "source_record_count": source_rows,
            "target_record_count": target_rows,
            "rows_processed": 0,
            "execution_time": 0.0,
            "zero_record_condition": condition,
            "zero_record_policy_source": (
                "validations.edge_case_validation.zero_record_policy"
            ),
            **{
                field_name: 0
                for field_name in ZERO_RECORD_ZERO_COUNTS.get(name, ())
            },
            **{alias: 0 for alias in self._comparison_count_aliases(name)},
        }
        logger.info(
            "%s | table=%s | status=%s | source_rows=%s | target_rows=%s | reason=%s",
            name.replace("_", " ").capitalize(),
            getattr(table_map, "id", None),
            status.value,
            source_rows,
            target_rows,
            condition,
        )
        return CheckResult(
            check_name=name,
            status=status,
            source_value=source_rows,
            target_value=target_rows,
            delta=0,
            message=override["message"],
            details=details,
        )

    def _comparison_count_aliases(self, name):
        """The configured record-comparison count names, for a zeroed result."""
        if name != "record_comparison":
            return ()
        configured = self.validation_policies.full_comparison.get("count_names")
        if not isinstance(configured, dict):
            return ()
        return tuple(
            str(value) for value in configured.values()
            if isinstance(value, str) and value.strip()
        )

    def _commit_incremental_checkpoint(self, table_map, scope, result) -> None:
        """Advance only the durable successful boundary for this table."""
        if not result.checks or result.overall_status != ValidationStatus.PASS:
            return
        if scope.incremental_policy.get("classification_enabled"):
            for check in result.checks:
                details = check.details or {}
                if details.get("operation_classification_performed") is not False:
                    continue
                sample_classification_skip = (
                    check.check_name == "record_comparison"
                    and details.get("classification_status") == "skipped"
                    and details.get("classification_reason") == "sample_mode"
                    and (
                        str(details.get("comparison_mode") or "").startswith("sample")
                        or details.get("execution_mode") == "SAMPLE_COMPARISON"
                    )
                )
                if not sample_classification_skip:
                    return
        checkpoint_store = getattr(self, "checkpoint_store", None)
        if checkpoint_store is None:
            return
        strategy = scope.details.get("incremental_strategy")
        boundary_strategy = (
            scope.details.get("cdc_base_strategy") if strategy == "cdc" else strategy
        )
        if boundary_strategy not in {"watermark", "primary_key", "target_audit"}:
            # An explicit full-hash strategy has no incremental boundary to
            # advance; the whole table was read and nothing is carried forward.
            return
        state = {
            "strategy": strategy,
            "boundary_strategy": boundary_strategy,
            "bootstrap_used": bool(scope.details.get("bootstrap_used", False)),
        }
        if boundary_strategy == "watermark":
            # The committed current_watermark is exactly the value the resolver
            # reads back as the next run's previous_watermark (checkpoint chain).
            state["previous_watermark"] = scope.lower_bound
            state["current_watermark"] = scope.upper_bound
        elif boundary_strategy == "target_audit":
            state["previous_load_boundary"] = scope.lower_bound
            state["current_load_boundary"] = scope.upper_bound
        if boundary_strategy == "primary_key":
            comparison = next(
                (item for item in reversed(result.checks)
                 if item.check_name == "record_comparison"),
                None,
            )
            last_key = (
                comparison.details.get("last_processed_key")
                if comparison is not None else None
            )
            if last_key is None:
                return
            state["last_processed_key"] = last_key
        checkpoint_store.commit_incremental(table_map.id, state)

    @staticmethod
    def _table_error_message(checks) -> Optional[str]:
        """Sanitized table-level reason for the first failing check, if any.

        Composed only from the failing check's name, the stage it reported and
        its error class, all of which are bounded tokens the validators set
        deliberately. Statement text, row values and driver messages are never
        used, so this cannot leak data into a report.
        """
        failing = {ValidationStatus.FRAMEWORK_ERROR, ValidationStatus.ERROR}
        for check in checks or ():
            if check is None or getattr(check, "status", None) not in failing:
                continue
            details = check.details if isinstance(check.details, dict) else {}
            name = (check.check_name or "validation").replace("_", " ").strip()
            name = name[:1].upper() + name[1:] if name else "Validation"
            stage = str(details.get("failed_stage") or "").strip()
            error_class = str(
                details.get("error_class")
                or details.get("exception_category")
                or "unknown_error"
            ).strip()
            if stage:
                return f"{name} failed during {stage}: {error_class}"
            return f"{name} failed: {error_class}"
        return None

    def _validator_exception_result(self, name, table_map, exc):
        from core.connection_manager import DatabaseQueryError

        category = exception_category(exc)
        status = self.validation_policies.configured_status(
            "configuration_error_status"
            if category in {
                "CONFIGURATION_POLICY", "DATATYPE_OR_CONFIGURATION",
                "INVALID_CONFIGURED_COLUMN",
            }
            else "framework_exception_status"
        )
        # Driver exceptions can embed the statement (and therefore row values)
        # in their text; the check message keeps the driver's message only.
        safe_message = sanitize_error_text(exc)
        internal_error = isinstance(exc, (AttributeError, IndexError, KeyError, AssertionError, NameError)) or type(exc) is RuntimeError
        if internal_error:
            category = "FRAMEWORK_ERROR"
            status = ValidationStatus.ERROR
        invalid_columns = list(invalid_column_names(exc))
        return CheckResult(
            check_name=name,
            status=status,
            message=safe_message,
            details={
                "table_id": getattr(table_map, "id", None),
                "validation_name": name,
                "exception_category": category,
                "error_type": (
                    "RESOURCE_ERROR" if category == "MEMORY_LIMIT_EXCEEDED"
                    else "CONFIGURATION_ERROR"
                    if category == "INVALID_CONFIGURED_COLUMN"
                    else "TIMEOUT_ERROR" if "TIMEOUT" in category
                    else "CONNECTION_ERROR" if "CONNECTION" in category
                    else "DATABASE_ERROR" if isinstance(exc, DatabaseQueryError)
                    else "FRAMEWORK_ERROR"
                ),
                "side": "UNKNOWN",
                "invalid_column": (
                    invalid_columns[0] if len(invalid_columns) == 1 else None
                ),
                "invalid_columns": invalid_columns,
                "framework_error": internal_error,
                "source_column": None,
                "target_column": None,
                "source_datatype": None,
                "target_datatype": None,
                "source_precision": None,
                "target_precision": None,
                "comparison_strategy": None,
                "fallback_used": None,
                "error_reason": safe_message,
                "final_status": status.value,
            },
        )

    def _raise_if_fatal(self, exc):
        category = exception_category(exc)
        if category in self.validation_policies.errors["fatal_exception_categories"]:
            raise exc

    @staticmethod
    def _dependency_reclassified(name, results, failures):
        """Report what a dependency failure stopped as NOT_EXECUTED, nothing more.

        A validator that swallowed the unavailable connection reports ERROR for
        the checks it could not compute; those become NOT_EXECUTED dependency
        results with their structured root category. A check the validator did
        compute from data (PASS, FAIL, WARN) keeps its own result: a connection
        another validator lost concurrently never rewrites an executed check.
        """
        if not results:
            return dependency_results({name: True}, failures, include_roots=False)
        not_executed = {ValidationStatus.ERROR, ValidationStatus.FRAMEWORK_ERROR}
        reclassified = []
        for check in results:
            if check is None or check.status not in not_executed:
                reclassified.append(check)
                continue
            replacement = dependency_results(
                {check.check_name: True}, failures, include_roots=False
            )[0]
            replacement.details = {**dict(check.details or {}), **replacement.details}
            reclassified.append(replacement)
        return reclassified

    def _shared_failure_reclassified(self, name, results, execution_cache):
        """Classify a failed shared scan without duplicating its DB error."""
        provider = getattr(execution_cache, "shared_statistics", None)
        contexts = provider.failure_contexts() if provider is not None else []
        invalid = [
            context for context in contexts
            if context.get("category") == "INVALID_CONFIGURED_COLUMN"
        ]
        if not invalid or not results:
            return results

        aliases = {"duplicate": "duplicate_check"}
        requester_name = aliases.get(name, name)
        owners = [
            context for context in invalid
            if requester_name in context.get("requested_by", ())
        ]
        context = (owners or invalid)[0]
        role = str(context.get("role") or "unknown").upper()
        columns = list(context.get("invalid_columns") or ())
        affected = {ValidationStatus.ERROR, ValidationStatus.FRAMEWORK_ERROR}
        reclassified = []
        for check in results:
            if check is None or check.status not in affected:
                reclassified.append(check)
                continue
            details = dict(check.details or {})
            details.update({
                "dependency": "shared_statistics",
                "root_cause": "INVALID_CONFIGURED_COLUMN",
                "source_or_target": role,
                "side": role,
            })
            if owners:
                status = self.validation_policies.configured_status(
                    "configuration_error_status"
                )
                details.update({
                    "exception_category": "INVALID_CONFIGURED_COLUMN",
                    "error_type": "CONFIGURATION_ERROR",
                    "invalid_column": columns[0] if len(columns) == 1 else None,
                    "invalid_columns": columns,
                    "final_status": status.value,
                })
                reclassified.append(replace(
                    check,
                    status=status,
                    message=(
                        "A configured validation column does not exist in the "
                        "runtime table schema."
                    ),
                    details=details,
                ))
            else:
                details.update({
                    "exception_category": "DEPENDENCY_FAILED",
                    "error_type": "DEPENDENCY_FAILED",
                    "final_status": ValidationStatus.SKIP.value,
                })
                reclassified.append(replace(
                    check,
                    status=ValidationStatus.SKIP,
                    message=(
                        "Not executed: dependency failed | "
                        "dependency=shared_statistics | "
                        "root_cause=INVALID_CONFIGURED_COLUMN"
                    ),
                    details=details,
                ))
        return reclassified

    @staticmethod
    def _validation_group(name):
        if name in {"row_count", "null_validation", "aggregate", "audit_validation"}:
            return "statistics"
        if name in {"key_validation", "duplicate"}:
            return "keys"
        if name == "record_comparison":
            return "record_comparison"
        return "data_value"

    def _execute_timed_check(
        self, name, func, args, table_map, phase, run_id, isolated,
        source_conn=None, target_conn=None, execution_cache=None,
    ):
        started_at = datetime.utcnow()
        started = time.perf_counter()
        decision = self._resolve_strategy(
            name, table_map, source_conn, target_conn, execution_cache
        )
        validation_slots = getattr(self, "_validation_slots", None)
        if validation_slots is None:
            validation_slots = threading.BoundedSemaphore(
                self.config.performance.parallel_execution.max_concurrent_validations
            )
        large_table_scheduler = getattr(self, "_large_table_scheduler", None)
        with validation_slots:
            with contextlib.nullcontext() if large_table_scheduler is None else (
                large_table_scheduler.slot(name, table_map, execution_cache)
            ):
                ledger = getattr(self, "performance_ledger", None)
                operation_scope = (
                    ledger.scope(
                        table_id=table_map.id,
                        phase=phase,
                        validation_name=name,
                    )
                    if ledger is not None
                    else contextlib.nullcontext()
                )
                with operation_scope:
                    scan_ledger = getattr(execution_cache, "scan_ledger", None)
                    scan_scope = (
                        scan_ledger.scope(
                            table_id=table_map.id,
                            phase=phase,
                            validation_name=name,
                        )
                        if scan_ledger is not None else contextlib.nullcontext()
                    )
                    with scan_scope:
                        results = (
                            self._execute_isolated_check(func, args, table_map)
                            if isolated else func(*args)
                        )
        if execution_cache is not None:
            failures = [failure for role in ("source", "target")
                        if (failure := execution_cache.peek(("object_dependency", role))) is not None]
            if failures:
                results = self._dependency_reclassified(name, results, failures)
            results = self._shared_failure_reclassified(
                name, results, execution_cache
            )
        completed_at = datetime.utcnow()
        elapsed = round(time.perf_counter() - started, 6)
        strategy_details = decision.as_details() if decision is not None else {}
        for check in results or []:
            if check.status == ValidationStatus.ERROR:
                check.status = self.validation_policies.configured_status(
                    "framework_exception_status"
                )
            elif check.status == ValidationStatus.WARN:
                check.status = self.validation_policies.configured_status(
                    "fallback_status"
                )
            # A validator that resolved its own strategy keeps it; the planned
            # strategy is recorded either way.
            planned = dict(strategy_details)
            planned["planned_execution_strategy"] = planned.pop(
                "execution_strategy", None
            )
            check.details.update({
                key: value
                for key, value in planned.items()
                if key not in check.details
            })
            check.details.setdefault("exception_category", None)
            check.details.setdefault("source_column", None)
            check.details.setdefault("target_column", None)
            check.details.setdefault("source_datatype", None)
            check.details.setdefault("target_datatype", None)
            check.details.setdefault("source_precision", None)
            check.details.setdefault("target_precision", None)
            check.details.setdefault("comparison_strategy", check.details.get("execution_strategy"))
            check.details.setdefault("fallback_used", None)
            check.details.setdefault("error_reason", check.details.get("failure_reason"))
            check.details["final_status"] = check.status.value
            check.details.setdefault(
                "execution_strategy", planned.get("planned_execution_strategy")
            )
            check.details["execution_time_seconds"] = elapsed
            check.details.update({
                "run_id": run_id,
                "phase": phase,
                "table_id": table_map.id,
                "validation_name": check.check_name,
                "validation_group": self._validation_group(name),
            })
            check.details.setdefault("source_or_target", "BOTH")
            timing = self.config.performance.timing
            if timing.enabled and timing.individual_check_timing:
                measured = {
                    "start_time": started_at.isoformat(),
                    "end_time": completed_at.isoformat(),
                    "execution_time_seconds": elapsed,
                }
                if timing.allow_orchestrator_override:
                    check.details.update(measured)
                elif timing.preserve_validator_timing:
                    for key, value in measured.items():
                        check.details.setdefault(key, value)
                else:
                    check.details.update(measured)
        self._record_validation_performance(
            name, results, table_map, phase, elapsed, decision
        )
        monitoring = self.config.performance.performance_monitoring

        # Log timing details for this validator
        check_count = len(results or [])
        logger.info(
            "[TIMING] validator=%s table=%s phase=%s duration=%.6fs checks=%d",
            name,
            table_map.id,
            phase,
            elapsed,
            check_count,
        )

        if (
            monitoring.enabled
            and elapsed >= monitoring.slow_validation_threshold_seconds
        ):
            logger.warning(
                "[SLOW] %s validation=%s strategy=%s elapsed=%.6fs phase=%s checks=%d",
                table_map.id,
                name,
                (decision.strategy.value if decision is not None else "unresolved"),
                elapsed,
                phase,
                check_count,
            )
        return results

    def _resolve_strategy(
        self, name, table_map, source_conn, target_conn, execution_cache
    ):
        """Select the best supported strategy for one validation, or None."""
        resolver = getattr(self, "strategy_resolver", None)
        if resolver is None or source_conn is None or target_conn is None:
            return None
        try:
            return resolver.resolve(
                name,
                source_connector=source_conn,
                target_connector=target_conn,
                estimated_rows=estimate_row_volume(table_map, execution_cache),
                table_map=table_map,
                validation_profile=table_map.migration_phase,
            )
        except Exception:
            logger.debug("Strategy resolution failed for %s", name, exc_info=True)
            return None

    def _prepare_shared_statistics(
        self, checks, table_map, source_conn, target_conn, execution_cache
    ):
        """Build one phase-scoped statistics provider for an enabled table.

        The same path is used by smoke_test, historical_backfill, incremental
        and full_load. Preflight intentionally has no data-statistics checks.
        Unsupported connectors retain the validators' existing query paths.
        """
        if BaseValidator._source_scope_unavailable(table_map):
            return None
        optimization = getattr(self.config.performance, "scan_optimization", None)
        if (
            optimization is None
            or not optimization.statistics_sharing_enabled()
        ):
            return None

        connections = {"source": source_conn, "target": target_conn}
        if not statistics_pushdown_available(connections, self.config.performance):
            return None

        builder = SharedStatisticsBuilder()
        builder.key_columns(
            [item.source_column for item in table_map.key_mappings],
            [item.target_column for item in table_map.key_mappings],
        )

        for name, enabled in checks.items():
            if not enabled:
                continue
            validator = self._new_validator(name)
            request_factory = getattr(validator, "statistic_requests", None)
            if not callable(request_factory):
                continue
            if name == "audit_validation":
                requests = request_factory(table_map, self.config.validations)
            elif name == "aggregate":
                aggregations = validator.configured_aggregations(
                    self.config.validations
                )
                requests = request_factory(table_map, aggregations)
            else:
                requests = request_factory(table_map)
            builder.extend(requests or [])

        plan = builder.build(resolve_max_expressions(
            connections, self.config.performance
        ))
        if plan.is_empty:
            return None

        helper = BaseValidator()
        source_dialect = helper._dialect(source_conn)
        target_dialect = helper._dialect(target_conn)
        provider = SharedStatistics(
            plan=plan,
            execution_cache=execution_cache,
            connections=connections,
            table_sources={
                "source": _fqn(
                    table_map.source_schema, table_map.source_table, source_dialect
                ),
                "target": _fqn(
                    table_map.target_schema, table_map.target_table, target_dialect
                ),
            },
            where_clause={
                "source": helper._build_where_clause(
                    table_map, source_dialect, "source"
                ),
                "target": helper._build_where_clause(
                    table_map, target_dialect, "target"
                ),
            },
            scope_token=helper._scope_token(table_map),
            run_both_sides=lambda source_call, target_call: helper._run_both_sides(
                self.config.performance, source_call, target_call
            ),
            scan_ledger=getattr(execution_cache, "scan_ledger", None),
            compatibility_writers=(
                lambda role, values: execution_cache.put(
                    ("row_count", role), int(values[ROW_COUNT_KEY])
                ) if values.get(ROW_COUNT_KEY) is not None else None,
            ),
            # A shared scan that timed out or failed in the database already
            # spent the row count's statement time; record the failure so no
            # later validator issues COUNT_BIG(*) again for this table/side.
            failure_writers=(
                lambda role, exc: execution_cache.put_failure(
                    ("row_count", role), exc, consumer="shared_statistics"
                ),
            ),
        )
        execution_cache.shared_statistics = provider
        return provider

    @staticmethod
    def _skip_unsafe_target_driven_checks(scope, checks, result) -> None:
        """Compatibility hook; profile-enabled checks are no longer overridden.

        Scope safety is resolved by each validator and its configured strategy.
        The orchestrator must not silently change an explicit validation
        profile after it has been resolved.
        """
        del scope, checks, result

    @staticmethod
    def _reconcile_target_driven_row_count(scope, checks) -> None:
        """Remove incremental INSERT/UPDATE versions from the target count."""
        row_count = next(
            (item for item in checks if item.check_name == "row_count"), None
        )
        comparison = next(
            (item for item in checks if item.check_name == "record_comparison"),
            None,
        )
        if row_count is None or comparison is None:
            return
        if (row_count.details or {}).get("row_count_reconciled"):
            return
        comparison_details = comparison.details or {}
        operations = comparison_details.get("incremental_operations") or {}
        incremental_classified = bool(
            comparison_details.get("operation_classification_performed")
            or operations
        )
        target_driven = (
            scope is not None and not scope.source_scope_available
        ) or comparison_details.get("comparison_direction") == "target_to_source"
        if not target_driven and not incremental_classified:
            return
        updates = int(
            comparison_details.get("validated_update_matches", 0) or 0
        )
        if updates <= 0:
            # Some compatibility/report paths retain the finalized operation
            # summary but omit the newer validated_update_matches scalar. A
            # target-driven PASS with no remaining comparison mismatches has
            # already proved those UPDATEs against target history, so the
            # operation count is an equivalent bounded reconciliation input.
            comparison_clean = (
                comparison.status == ValidationStatus.PASS
                and int(comparison_details.get("mismatch_records", 0) or 0) == 0
                and int(comparison_details.get("missing_in_source", 0) or 0) == 0
                and int(comparison_details.get("missing_in_target", 0) or 0) == 0
            )
            if comparison_clean:
                updates = int(operations.get("update", 0) or 0)
        # Only INSERTs proved through the alternate Server 2 source are
        # target rows absent from the primary (Server 1) source count.
        # Ordinary Server 1 INSERT classifications are already represented
        # in the measured source count and must not be deducted.
        server_2_inserts = int(
            comparison_details.get("server_2_matches", 0) or 0
        )
        if row_count.source_value is None or row_count.target_value is None:
            return
        measured_source = int(row_count.source_value)
        measured_target = int(row_count.target_value)
        adjustment_executed = bool(
            comparison_details.get("operation_classification_performed")
            and (updates > 0 or server_2_inserts > 0)
        )
        comparable_target = (
            max(0, measured_target - updates - server_2_inserts)
            if adjustment_executed else measured_source
        )
        delta = abs(measured_source - comparable_target)
        delta_pct = (
            delta / max(abs(measured_source), abs(comparable_target)) * 100
            if measured_source or comparable_target else 0.0
        )
        tolerance = float((row_count.details or {}).get("tolerance_pct", 0) or 0)
        row_count.details = {
            **dict(row_count.details or {}),
            "measured_source_count": measured_source,
            "measured_target_count": measured_target,
            "historically_validated_updates": updates,
            "server_2_insert_count": server_2_inserts,
            "incremental_row_count_adjustment_executed": adjustment_executed,
            "reconciled_target_count": comparable_target,
            "row_count_reconciled": True,
            "raw_status": row_count.status.value,
        }
        row_count.target_value = comparable_target
        row_count.delta = delta
        row_count.delta_pct = round(delta_pct, 4)
        row_count.status = (
            ValidationStatus.PASS
            if delta_pct <= tolerance else ValidationStatus.FAIL
        )
        row_count.message = (
            "Row count after incremental target reconciliation: "
            f"measured_source={measured_source:,}, "
            f"measured_target={measured_target:,}, "
            f"validated_updates={updates:,}, "
            f"server_2_inserts={server_2_inserts:,}, "
            f"adjustment_executed={str(adjustment_executed).lower()}, "
            f"reconciled_target={comparable_target:,}, "
            f"delta={delta:,} ({delta_pct:.4f}%), tolerance={tolerance:.4f}%"
        )

    def _record_validation_performance(
        self, name, results, table_map, phase, elapsed, decision
    ):
        """Record the configured performance fields for one validation."""
        ledger = getattr(self, "performance_ledger", None)
        if ledger is None or not ledger.enabled:
            return
        details = (results or [{}]) and [
            check.details for check in (results or []) if getattr(check, "details", None)
        ]
        merged = {}
        for item in details or []:
            merged.update(item)
        strategy = merged.get("execution_strategy") or (
            decision.strategy.value if decision is not None else None
        )
        parallel = self.config.performance.parallel_execution
        ledger.record_validation(
            phase=phase,
            table_id=table_map.id,
            source_system=table_map.source_system,
            target_system=getattr(table_map, "target_system", None),
            validation_name=name,
            execution_strategy=strategy,
            source_execution_time=merged.get("source_execution_time"),
            target_execution_time=merged.get("target_execution_time"),
            comparison_time=merged.get("comparison_time"),
            total_execution_time=elapsed,
            rows_processed=(
                merged.get("records_checked")
                or merged.get("source_records_scanned")
                or merged.get("source_rows_scanned")
                or merged.get("rows_scanned")
                or merged.get("source_records_selected")
            ),
            batch_count=(
                merged.get("batch_count")
                or merged.get("source_batches")
                or 0
            ),
            batch_size=merged.get("batch_size"),
            worker_count=parallel.validation_parallelism.max_workers_per_table,
            batch_strategy=merged.get("batch_strategy"),
            estimated_rows=(decision.estimated_rows if decision is not None else None),
        )

    def _execute_isolated_check(self, func, args, table_map):
        """Run one check with connector objects owned only by this worker."""
        source = self.conn_manager.get_source(
            table_map.source_system, fresh=True, table_context=table_map
        )
        target = self.conn_manager.get_target(fresh=True, table_context=table_map)
        try:
            isolated_args = (source, target, *args[2:])
            return func(*isolated_args)
        finally:
            source.close()
            target.close()

    def _run_execution_metrics(self):
        execution = getattr(self.config, "execution", ExecutionConfig())
        table_parallel = execution.parallel_execution.table_parallelism
        return {
            "execution_mode": self.context.engine_type.value,
            "parallel_execution_status": bool(
                execution.parallel_execution.enabled and table_parallel.enabled
            ),
            "batch_execution_status": execution.batch_processing.enabled,
            "batch_size": execution.batch_processing.batch_size,
            "worker_count": table_parallel.max_workers,
            "executor_type": table_parallel.executor_type,
            "validation_parallelism_status": getattr(
                self,
                "validation_parallel_enabled",
                bool(
                    execution.parallel_execution.enabled
                    and execution.parallel_execution.validation_parallelism.enabled
                ),
            ),
            "validation_worker_count": getattr(
                self, "check_workers", execution.check_workers
            ),
            "max_concurrent_validations": (
                execution.parallel_execution.max_concurrent_validations
            ),
            "global_max_concurrency": (
                execution.parallel_execution.resolved_global_max_concurrency()
            ),
            "batch_strategy": execution.batch_processing.strategy,
            "strategy_engine": execution.strategies.engine,
            "pushdown_enabled": execution.strategies.pushdown.enabled,
            "pandas_max_rows_in_memory": execution.strategies.pandas.max_rows_in_memory,
            "spark_strategy_enabled": execution.strategies.spark.enabled,
            "spark_threshold_rows": execution.strategies.spark.threshold_rows,
        }

    def _performance_summary(self):
        """Run-level performance findings for the reports and the log."""
        ledger = getattr(self, "performance_ledger", None)
        governor = getattr(self, "concurrency_governor", None)
        metrics = {}
        if governor is not None:
            metrics.update(governor.metrics())
        if ledger is None or not ledger.enabled:
            return metrics
        summary = ledger.summary()
        metrics["performance_summary"] = summary
        metrics["validation_performance"] = ledger.validations
        for label, key in (
            ("Slowest validation", "slowest_validation"),
            ("Slowest table", "slowest_table"),
            ("Slowest connector operation", "slowest_connector_operation"),
            ("Highest batch count", "highest_batch_count"),
        ):
            if key in summary:
                logger.info("[PERFORMANCE] %s: %s", label, summary[key])
        return metrics

    def _table_execution_metrics(self):
        if self._cached_run_metrics is None:
            self._cached_run_metrics = self._run_execution_metrics()
        metrics = dict(self._cached_run_metrics)
        metrics.update(
            source_row_count=None,
            target_row_count=None,
            processed_row_count=0,
            total_batches=0,
            successful_batches=0,
            failed_batches=0,
            retry_count=0,
            throughput_rows_per_second=0.0,
            validation_duration=0.0,
        )
        return metrics

    def _filter_tables(
        self, tags, layers, phases, ids, *, servers=None, databases=None,
        schemas=None, table_names=None, eligible_ids=None,
    ):
        tables = self.config.tables
        if tags: tables = [t for t in tables if any(g in t.tags for g in tags)]
        if layers: tables = [t for t in tables if t.target_layer in layers]
        if servers: tables = [t for t in tables if t.source_server in servers]
        if databases: tables = [t for t in tables if t.source_database in databases]
        if schemas: tables = [t for t in tables if t.source_schema in schemas]
        if ids or table_names:
            tables = [
                t for t in tables
                if (ids and t.id in ids)
                or (table_names and t.source_table in table_names)
            ]
        if eligible_ids is not None:
            eligible_id_set = set(eligible_ids)
            tables = [t for t in tables if t.id in eligible_id_set]
        if not phases:
            return tables

        # FIXED: Do NOT filter tables by validation_phases
        # Phase selection is now driven by central validation_profiles only
        # All tables pass through; _resolve_checks() determines what validations run
        expanded = []
        for phase in phases:
            for table in tables:
                expanded.append(replace(table, migration_phase=phase))
        return expanded

    def _ordered_phases(self, phases):
        unique = list(dict.fromkeys(phases))
        configured = [
            phase
            for phase in self.config.validation_profiles
            if phase in unique
        ]
        remaining = [
            phase
            for phase in unique
            if phase not in configured
        ]
        return configured + remaining

    def _resolve_checks(self, profile, table_map, cli_only, cli_skip) -> Dict[str, bool]:
        checks = {name: bool(getattr(profile, attr, False)) for name, attr in VALIDATION_MAP.items()}
        if getattr(profile, "name", None) == "preflight":
            # Global run defaults and CLI data-check switches do not apply to
            # the metadata-only preflight profile.
            return {name: False for name in checks}
        rd = self.config.run_defaults
        if rd.active_checks:
            checks = {n: n in rd.active_checks for n in checks}
        for c in (rd.always_run or []):
            if c in checks: checks[c] = True
        for c in (rd.always_skip or []):
            if c in checks: checks[c] = False
        if getattr(table_map, "override_checks", None):
            checks = {n: n in table_map.override_checks for n in checks}
        for c in (getattr(table_map, "skip_checks", None) or []):
            if c in checks: checks[c] = False
        if cli_only:
            checks = {n: n in cli_only for n in checks}
        for c in (cli_skip or []):
            if c in checks: checks[c] = False
        configured_categories = getattr(
            self.config.performance, "validation_categories", None
        )
        if isinstance(configured_categories, (list, tuple, set)):
            category_checks = set().union(*(
                VALIDATION_CATEGORIES[category]
                for category in configured_categories
            ))
            checks = {
                name: enabled and name in category_checks
                for name, enabled in checks.items()
            }
        return checks

    def _print_console_summary(self, summary: RunSummary):
        print(f"\n{'='*65}")
        print(f"  VALIDATION COMPLETE — {summary.run_id}")
        print(f"{'='*65}")
        print(f"  Total tables : {summary.total_tables}")
        print(f"  PASS         : {summary.passed_tables}")
        print(f"  FAIL         : {summary.failed_tables}")
        print(f"  Pass rate    : {summary.pass_rate_pct}%")
        print(f"  Duration     : {summary.duration_seconds:.1f}s")
        print(f"{'='*65}\n")
