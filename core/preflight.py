"""Configuration-driven source and target connection preflight validation."""

from __future__ import annotations

import glob
import json
import logging
import os
import re
import socket
import uuid
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

from core.config_loader import (
    ConfigLoader, SourceConfig, TargetConfig, _expand_env_vars,
    load_execution_document, load_yaml_document, normalize_connection_config,
    normalize_connection_document,
)
from core.connection_manager import (
    ConnectorCache, build_connector, merge_connection_context, normalize_connection_type,
)
from core.performance import (
    ConcurrencyGovernor, ExecutionResultCache, PerformanceLedger,
)
from core.masking import MaskingFilter, collect_sensitive_values, mask_data, mask_text
from core.models import ValidationStatus
from core.sql_dialect import resolve_dialect
from core.validation_registry import PreflightCheckLoader, ValidationRegistry
from validators import (
    AggregateValidator, DuplicateDetectionValidator, KeyValidationValidator,
    NullValidationValidator, RowCountValidator,
)

logger = logging.getLogger(__name__)
_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

_PREFLIGHT_STATUS_FIELDS = {
    "connection_validation": (
        "source_connection_validation_status",
        "target_connection_validation_status",
    ),
    "database_availability": (
        "source_database_availability_status",
        "target_database_availability_status",
    ),
    "schema_availability": (
        "source_schema_availability_status",
        "target_schema_availability_status",
    ),
    "table_availability": (
        "source_table_availability_status",
        "target_table_availability_status",
    ),
    "column_availability": (
        "source_column_availability_status",
        "target_column_availability_status",
    ),
    "key_column_availability": (
        "source_key_column_availability_status",
        "target_key_column_availability_status",
    ),
    "object_validation": (
        "source_object_validation_status",
        "target_object_validation_status",
    ),
}


def evaluate_overall_preflight_status(
    validation_results: Dict[str, Any],
    enabled_checks: Dict[str, bool],
) -> tuple[str, List[str], Optional[str]]:
    """Evaluate only enabled, mandatory source/target preflight checks."""
    required_fields = [
        field_name
        for check_name, field_names in _PREFLIGHT_STATUS_FIELDS.items()
        if enabled_checks.get(check_name, False)
        for field_name in field_names
    ]
    failed_fields = [
        field_name
        for field_name in required_fields
        if str(validation_results.get(field_name) or "").strip().upper() != "PASSED"
    ]
    if failed_fields:
        labels = [field_name.removesuffix("_status").replace("_", " ") for field_name in failed_fields]
        return (
            "FAILED",
            failed_fields,
            "; ".join(f"{label.title()} did not pass" for label in labels) + ".",
        )
    return "PASSED", [], None


@dataclass
class ConnectionSpec:
    config_file: str
    reference: str
    classification: str
    raw: Dict[str, Any]
    required: bool = True


@dataclass
class PreflightResult:
    configuration_file_name: str
    connection_name: str
    connection_reference: str
    connection_type: str
    classification: str
    connector_class_used: Optional[str] = None
    authentication_type: Optional[str] = None
    environment_variable_resolution_status: str = "NOT_STARTED"
    connection_attempt_status: str = "NOT_ATTEMPTED"
    connection_status: str = "NOT_CONNECTED"
    validation_status: str = "FAILED"
    executed_validation_checks: List[str] = field(default_factory=list)
    message: str = ""
    failure_stage: Optional[str] = None
    failure_reason: Optional[str] = None
    exception_category: Optional[str] = None
    validation_duration_seconds: float = 0.0
    required: bool = True


@dataclass
class TablePreflightResult:
    layer_name: str
    configuration_file_name: str
    table_configuration_id: str
    classification: str
    connection_reference: str
    connection_type: str
    masked_server_name: Optional[str]
    database_name: Optional[str]
    schema_name: str
    table_name: str
    configured_key_columns: List[str] = field(default_factory=list)
    configured_columns: List[str] = field(default_factory=list)
    missing_columns: List[str] = field(default_factory=list)
    additional_columns: List[str] = field(default_factory=list)
    connection_validation_status: str = "SKIPPED"
    database_availability_status: str = "SKIPPED"
    schema_availability_status: str = "SKIPPED"
    table_availability_status: str = "SKIPPED"
    column_availability_status: str = "SKIPPED"
    key_column_availability_status: str = "SKIPPED"
    source_row_count: Optional[int] = None
    target_row_count: Optional[int] = None
    row_count_validation_status: str = "SKIPPED"
    key_validation_status: str = "SKIPPED"
    null_validation_status: str = "SKIPPED"
    duplicate_validation_status: str = "SKIPPED"
    aggregate_validation_status: str = "SKIPPED"
    validation_details: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    overall_validation_status: str = "SKIPPED"
    validation_status: str = "SKIPPED"
    message: str = ""
    failure_reason: Optional[str] = None
    validation_start_time: Optional[str] = None
    validation_completion_time: Optional[str] = None
    validation_duration_seconds: float = 0.0
    required: bool = True


@dataclass
class IndependentTableSmokeResult:
    layer_name: str
    configuration_file_name: str
    table_configuration_id: str
    source_connection_reference: str
    target_connection_reference: str
    source_connection_type: str
    target_connection_type: str
    source_server_name: Optional[str]
    target_server_name: Optional[str]
    source_database_name: Optional[str]
    target_database_name: Optional[str]
    source_schema_name: str
    target_schema_name: str
    source_table_name: str
    target_table_name: str
    source_configured_key_columns: List[str] = field(default_factory=list)
    target_configured_key_columns: List[str] = field(default_factory=list)
    source_configured_columns: List[str] = field(default_factory=list)
    target_configured_columns: List[str] = field(default_factory=list)
    enabled_checks: List[str] = field(default_factory=list)
    source_connection_validation_status: str = "NOT_CONFIGURED"
    target_connection_validation_status: str = "NOT_CONFIGURED"
    source_database_availability_status: str = "NOT_CONFIGURED"
    target_database_availability_status: str = "NOT_CONFIGURED"
    source_schema_availability_status: str = "NOT_CONFIGURED"
    target_schema_availability_status: str = "NOT_CONFIGURED"
    source_table_availability_status: str = "NOT_CONFIGURED"
    target_table_availability_status: str = "NOT_CONFIGURED"
    source_column_availability_status: str = "NOT_CONFIGURED"
    target_column_availability_status: str = "NOT_CONFIGURED"
    source_key_column_availability_status: str = "NOT_CONFIGURED"
    target_key_column_availability_status: str = "NOT_CONFIGURED"
    source_object_validation_status: str = "FAILED"
    target_object_validation_status: str = "FAILED"
    source_row_count: Optional[int] = None
    target_row_count: Optional[int] = None
    source_row_count_status: str = "NOT_CONFIGURED"
    target_row_count_status: str = "NOT_CONFIGURED"
    row_count_comparison_status: str = "NOT_CONFIGURED"
    source_key_validation_status: str = "NOT_CONFIGURED"
    target_key_validation_status: str = "NOT_CONFIGURED"
    key_comparison_status: str = "NOT_CONFIGURED"
    source_null_validation_status: str = "NOT_CONFIGURED"
    target_null_validation_status: str = "NOT_CONFIGURED"
    null_comparison_status: str = "NOT_CONFIGURED"
    source_duplicate_validation_status: str = "NOT_CONFIGURED"
    target_duplicate_validation_status: str = "NOT_CONFIGURED"
    duplicate_comparison_status: str = "NOT_CONFIGURED"
    source_aggregate_validation_status: str = "NOT_CONFIGURED"
    target_aggregate_validation_status: str = "NOT_CONFIGURED"
    aggregate_comparison_status: str = "NOT_CONFIGURED"
    source_validation_details: Dict[str, Any] = field(default_factory=dict)
    target_validation_details: Dict[str, Any] = field(default_factory=dict)
    comparison_validation_details: Dict[str, Any] = field(default_factory=dict)
    overall_preflight_validation_status: str = "FAILED"
    failed_preflight_validation_checks: List[str] = field(default_factory=list)
    preflight_failure_reason: Optional[str] = None
    preflight_message: str = ""
    overall_validation_status: str = "FAILED"
    validation_status: str = "FAILED"
    message: str = ""
    failure_reason: Optional[str] = None
    validation_start_time: Optional[str] = None
    validation_completion_time: Optional[str] = None
    validation_duration_seconds: float = 0.0
    required: bool = True


@dataclass
class PreflightReport:
    validation_run_id: str
    validation_timestamp: str
    overall_preflight_status: str
    connections: List[Any]
    #: Connection establishment and reuse counters for the whole run.
    connection_metrics: Dict[str, Any] = field(default_factory=dict)
    wall_clock_duration: float = 0.0
    cumulative_worker_duration: float = 0.0
    operation_timings: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return mask_data(asdict(self))


def _read_yaml(path: Path) -> Dict[str, Any]:
    return load_yaml_document(path)


def _exception_category(exc: Exception) -> str:
    text = str(exc).lower()
    if isinstance(exc, ModuleNotFoundError):
        return "MISSING_DRIVER_OR_DEPENDENCY"
    if isinstance(exc, (socket.gaierror,)):
        return "DNS_OR_HOST_RESOLUTION_FAILURE"
    if isinstance(exc, TimeoutError) or "timeout" in text:
        return "NETWORK_TIMEOUT"
    if any(
        marker in text
        for marker in (
            "login",
            "logon",
            "authentication",
            "credential",
            "unauthorized",
            "cannot generate sspi context",
        )
    ):
        return "AUTHENTICATION_FAILURE"
    if "connection string" in text or "invalid connection string attribute" in text:
        return "INVALID_CONNECTION_STRING"
    # Most pyodbc errors include the active driver's name, so the word "driver"
    # alone does not mean that the driver is missing.
    if any(
        marker in text
        for marker in (
            "configured odbc driver is not installed",
            "driver not found",
            "driver is not installed",
            "can't open lib",
            "cannot open lib",
            "data source name not found",
            "no default driver specified",
        )
    ):
        return "MISSING_ODBC_DRIVER"
    if "query" in text or "sql" in text:
        return "QUERY_EXECUTION_FAILURE"
    if isinstance(exc, ValueError):
        return "CONFIGURATION_ERROR"
    return type(exc).__name__.upper()


class PreflightValidator:
    def __init__(self, execution_config: str, context: Any):
        self.master_path = Path(execution_config)
        self.context = context
        # The complete execution document: the master plus the sections it
        # names under `config_files`. Preflight reads `config_paths`, `source`,
        # `target` and `preflight` from it exactly as before.
        self.master = load_execution_document(self.master_path)
        self._load_environment()
        self.preflight_checks = self._load_preflight_checks()
        # One cache for the whole preflight run. Connections are established per
        # unique connection identity and disposed only when the run finishes.
        self.connector_cache: Optional[ConnectorCache] = None
        self.concurrency_governor: Optional[ConcurrencyGovernor] = None
        self.metadata_cache = ExecutionResultCache()
        self.performance_ledger = PerformanceLedger(enabled=True)

    def _open_connector_cache(self, execution_config: Any = None) -> ConnectorCache:
        """Create the run-scoped connection cache from existing configuration."""
        parallel = getattr(execution_config, "parallel_execution", None)
        if parallel is not None:
            self.concurrency_governor = ConcurrencyGovernor(
                parallel.resolved_global_max_concurrency()
            )
        self.connector_cache = ConnectorCache(
            self.context,
            runtime_config=execution_config,
            concurrency_governor=self.concurrency_governor,
            performance_ledger=self.performance_ledger,
        )
        return self.connector_cache

    def _close_connector_cache(self) -> Dict[str, Any]:
        """Dispose every cached connection and pool after the complete run."""
        if self.connector_cache is None:
            return {}
        metrics = self.connector_cache.metrics()
        self.connector_cache.close_all()
        self.connector_cache = None
        if self.concurrency_governor is not None:
            metrics.update(self.concurrency_governor.metrics())
            self.concurrency_governor = None
        logger.info(
            "Preflight connection reuse: established=%s reused=%s",
            metrics.get("connections_established"),
            metrics.get("connection_reuse_count"),
        )
        metrics["metadata_timing_summary"] = self.performance_ledger.summary()
        return metrics

    def _project_root(self) -> Path:
        return (
            self.master_path.parent.parent
            if self.master_path.parent.name.lower() == "config"
            else self.master_path.parent
        )

    def _glob_config(self, pattern: str) -> List[Path]:
        candidates = [Path(item) for item in glob.glob(pattern, recursive=True)]
        if not candidates:
            candidates = [
                Path(item)
                for item in glob.glob(
                    str(self._project_root() / pattern), recursive=True
                )
            ]
        return sorted(candidates)

    def _load_preflight_checks(self) -> Optional[Dict[str, bool]]:
        """Load preflight schema validations from configuration.

        Reads ONLY the preflight/schema_validation section from configuration.
        Supports both new nested format and legacy flat smoke_test format.
        Data checks (row_count, key_validation, etc.) are always disabled.
        """
        pattern = self.master.get("config_paths", {}).get("validations")
        if not pattern:
            return None

        # Collect configuration from all validation files
        schema_config: Dict[str, Any] = {}
        preflight_profile_config: Dict[str, Any] = {}
        legacy_smoke_config: Dict[str, Any] = {}

        for path in self._glob_config(str(pattern)):
            document = _read_yaml(path)

            # Support legacy flat smoke_test format (backward compatibility)
            if "smoke_test" in document:
                smoke = document.get("smoke_test") or {}
                if isinstance(smoke, dict):
                    legacy_smoke_config.update(smoke)

            # New format: validations.schema_validation (global defaults)
            validations_section = document.get("validations", {}) or {}
            if isinstance(validations_section, dict):
                schema_section = validations_section.get("schema_validation", {})
                if isinstance(schema_section, dict):
                    schema_config.update(schema_section)

            # New format: validation_profiles.preflight.schema_validation
            profiles = document.get("validation_profiles", {}) or {}
            if isinstance(profiles, dict):
                preflight_profile = profiles.get("preflight")
                if preflight_profile and isinstance(preflight_profile, dict):
                    profile_schema = preflight_profile.get("schema_validation", {})
                    if isinstance(profile_schema, dict):
                        preflight_profile_config.update(profile_schema)

        # Merge: profile > schema_config > legacy format
        # (profile overrides schema_config, both override legacy)
        final_config = dict(legacy_smoke_config)
        final_config.update(schema_config)
        final_config.update(preflight_profile_config)

        if not final_config:
            return None

        # Use PreflightCheckLoader to validate and normalize
        loader = PreflightCheckLoader(final_config)
        return loader.enabled_checks()

    def _load_smoke_test(self) -> Optional[Dict[str, bool]]:
        """Compatibility alias for older callers; returns preflight checks."""
        return self._load_preflight_checks()

    def _load_environment(self) -> None:
        # Use one canonical .env in the configured connections directory.
        project_root = (
            self.master_path.parent.parent
            if self.master_path.parent.name.lower() == "config"
            else self.master_path.parent
        )
        connection_pattern = self.master.get("config_paths", {}).get("connections", "")
        connection_prefix = Path(str(connection_pattern).split("*", 1)[0])
        connection_dir = (
            connection_prefix
            if str(connection_pattern).split("*", 1)[0].endswith(("/", "\\"))
            else connection_prefix.parent
        )
        if not connection_dir.is_absolute():
            connection_dir = project_root / connection_dir
        candidates = [connection_dir / ".env"]
        for path in candidates:
            if path.is_file():
                load_dotenv(path, override=False)

    def discover(self) -> List[ConnectionSpec]:
        pattern = self.master.get("config_paths", {}).get("connections")
        if not pattern:
            raise ValueError("Missing config_paths.connections in execution configuration")
        paths = [Path(p) for p in glob.glob(str(pattern), recursive=True)]
        if not paths:
            alternative = self.master_path.parent.parent / str(pattern)
            paths = [Path(p) for p in glob.glob(str(alternative), recursive=True)]
        if not paths:
            raise FileNotFoundError("No connection YAML configuration files were found")

        by_reference: Dict[str, ConnectionSpec] = {}
        for path in sorted(paths):
            raw_file = normalize_connection_document(_read_yaml(path))
            for section, classification in (("sources", "SOURCE"), ("targets", "TARGET")):
                connections = raw_file.get(section, {})
                if connections is None:
                    continue
                if not isinstance(connections, dict):
                    raise ValueError(f"'{section}' in '{path.name}' must be a mapping")
                for reference, raw in connections.items():
                    if not isinstance(raw, dict):
                        raise ValueError(f"Connection '{reference}' in '{path.name}' must be a mapping")
                    identity = str(reference)
                    required = self._is_required(identity, classification, raw)
                    existing = by_reference.get(identity)
                    if existing and existing.classification != classification:
                        existing.classification = "BOTH"
                        existing.required = existing.required or required
                    else:
                        by_reference[identity] = ConnectionSpec(
                            config_file=path.name,
                            reference=identity,
                            classification=classification,
                            raw=dict(raw),
                            required=required,
                        )
        specs = list(by_reference.values())
        if not specs:
            raise ValueError("No source or target connection definitions were found")
        return specs

    def _is_required(
        self,
        reference: str,
        classification: str,
        raw: Dict[str, Any],
    ) -> bool:
        if "required" in raw:
            return bool(raw["required"])

        selected_source = self.master.get("source")
        selected_target = self.master.get("target")
        if selected_source is None and selected_target is None:
            return True

        return (
            classification == "SOURCE"
            and reference == selected_source
        ) or (
            classification == "TARGET"
            and reference == selected_target
        )

    def run(self) -> PreflightReport:
        if self.preflight_checks is not None:
            return self._run_table_smoke_test()
        run_started = perf_counter()
        run_started_at = datetime.now(timezone.utc).isoformat()
        self._open_connector_cache()
        try:
            results = [self._validate(spec) for spec in self.discover()]
        finally:
            connection_metrics = self._close_connector_cache()
        required_failures = any(r.required and r.validation_status == "FAILED" for r in results)
        optional_failures = any(not r.required and r.validation_status == "FAILED" for r in results)
        if required_failures:
            overall = "FAILED"
        elif optional_failures:
            overall = "PARTIALLY_PASSED"
        elif results and all(r.validation_status == "SKIPPED" for r in results):
            overall = "SKIPPED"
        else:
            overall = "PASSED"
        logger.info(
            "Preflight validation summary: overall_preflight_status=%s "
            "connection_count=%d failed_connection_count=%d",
            overall,
            len(results),
            sum(
                item.validation_status == "FAILED"
                for item in results
            ),
        )
        settings = self.master.get("preflight", {})
        prefix = str(settings.get("run_id_prefix", "PREFLIGHT"))
        return PreflightReport(
            validation_run_id=f"{prefix}-{uuid.uuid4().hex[:8].upper()}",
            validation_timestamp=run_started_at,
            overall_preflight_status=overall,
            connections=results,
            connection_metrics=connection_metrics,
            wall_clock_duration=round(perf_counter() - run_started, 6),
            cumulative_worker_duration=round(sum(
                float(item.validation_duration_seconds or 0) for item in results
            ), 6),
            operation_timings=self.performance_ledger.operations,
        )

    def _table_file_names(self) -> Dict[str, str]:
        pattern = self.master.get("config_paths", {}).get("tables")
        if not pattern:
            raise ValueError("Missing config_paths.tables in execution configuration")
        names: Dict[str, str] = {}
        for path in self._glob_config(str(pattern).replace("{layer}", "*")):
            document = _read_yaml(path)
            configured_tables = document.get("tables")
            if configured_tables is None and document.get("id"):
                configured_tables = [document]
            for table in configured_tables or []:
                if isinstance(table, dict) and table.get("enabled", True) and table.get("id"):
                    names[str(table["id"])] = path.name
        return names

    def _extract_unique_connection_combinations(
        self, config: Any
    ) -> Dict[str, Tuple[ConnectionSpec, Dict[str, Any]]]:
        """Extract unique (connection_reference, database, server) combinations from tables.

        Returns a dict keyed by identity (e.g., "source_ref:DBNAME") mapping to:
        (ConnectionSpec, connection_context) tuples used for establishing that combination.
        """
        combinations: Dict[str, Tuple[ConnectionSpec, Dict[str, Any]]] = {}

        # Iterate tables and collect unique connection combinations
        if hasattr(config, "tables"):
            for table in config.tables:
                # Source side
                source_ref = getattr(table, "source_system", None)
                if source_ref and source_ref in (config.sources or {}):
                    source_config = config.sources[source_ref]
                    source_db = getattr(table, "source_database", None)
                    source_schema = getattr(table, "source_schema", None)
                    if source_db:
                        key = f"source:{source_ref}:{source_db}"
                        if key not in combinations:
                            raw = vars(source_config) if hasattr(source_config, "__dict__") else dict(source_config)
                            spec = ConnectionSpec(
                                config_file="<dynamic>",
                                reference=source_ref,
                                classification="SOURCE",
                                raw=raw,
                                required=True,
                            )
                            context = {
                                "database": source_db,
                                "schema": source_schema,
                                "table": getattr(table, "source_table", None),
                            }
                            combinations[key] = (spec, context)

                # Target side
                target_ref = config.target_ref
                if target_ref and target_ref in {config.target_ref}:
                    target_config = config.target
                    target_db = getattr(table, "target_database", None)
                    target_schema = getattr(table, "target_schema", None)
                    if target_db:
                        key = f"target:{target_ref}:{target_db}"
                        if key not in combinations:
                            raw = vars(target_config) if hasattr(target_config, "__dict__") else dict(target_config)
                            spec = ConnectionSpec(
                                config_file="<dynamic>",
                                reference=target_ref,
                                classification="TARGET",
                                raw=raw,
                                required=True,
                            )
                            context = {
                                "database": target_db,
                                "schema": target_schema,
                                "table": getattr(table, "target_table", None),
                            }
                            combinations[key] = (spec, context)

        return combinations

    def _display_connection_summary(
        self,
        result: PreflightResult,
        spec: Optional[ConnectionSpec] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Display non-sensitive connection details to console for user confirmation."""
        def get_value(cfg: Any, key: str, default: Any = None) -> Any:
            """Get value from config (handles dict and SimpleNamespace)."""
            if isinstance(cfg, dict):
                return cfg.get(key, default)
            return getattr(cfg, key, default)

        print(f"\n{result.classification} CONNECTION")
        print("-" * 48)
        print(f"Connection Name : {result.connection_name}")
        print(f"Connection Type : {result.connection_type}")

        # Resolve environment variables in the raw config for display
        if spec:
            raw_config = spec.raw
            resolved_config = normalize_connection_config(_expand_env_vars(raw_config))

            # Merge context values (e.g., resolved database from table)
            if context:
                resolved_config = merge_connection_context(resolved_config, context)

            # Extract and display connection details based on type
            if result.connection_type.upper() == "FABRIC":
                # Fabric-specific details
                print(f"Workspace       : {get_value(resolved_config, 'workspace_name', 'N/A')}")
                print(f"SQL Endpoint    : {get_value(resolved_config, 'sql_endpoint', 'N/A')}")
                print(f"Lakehouse       : {get_value(resolved_config, 'lakehouse_name', 'N/A')}")
                print(f"Initial Database: {get_value(resolved_config, 'initial_database', 'master')}")
                print(f"Authentication  : {result.authentication_type or get_value(resolved_config, 'authentication', 'N/A')}")
                print(f"Driver          : {get_value(resolved_config, 'driver', 'ODBC Driver 18 for SQL Server')}")
                timeout = get_value(resolved_config, 'connection_timeout', 30)
                print(f"Timeout         : {timeout}s")
            elif result.connection_type.upper() == "SQLSERVER":
                # SQL Server-specific details
                host = get_value(resolved_config, 'host') or get_value(resolved_config, 'server', 'N/A')
                print(f"Server/Host     : {host}")
                print(f"Database        : {get_value(resolved_config, 'database', 'N/A')}")
                print(f"Authentication  : {result.authentication_type or 'Windows'}")
                print(f"Driver          : {get_value(resolved_config, 'driver', 'ODBC Driver 17 for SQL Server')}")

        print(f"Status          : {result.connection_status}")
        if result.failure_reason:
            print(f"Failure Reason  : {result.failure_reason}")

    def _validate_upfront_connections(
        self, config: Any
    ) -> List[PreflightResult]:
        """Establish and validate source and target connections before table validation.

        Follows the required flow:
        1. Extract unique source/target connection combinations from all tables
        2. Resolve environment variables for each combination
        3. Display non-sensitive connection details
        4. Establish and test each unique combination (stored in run-scoped cache)
        5. Stop preflight if required connections fail

        Each unique (connection_reference, database) combination is tested once
        and shared by all tables that reference the same combination, avoiding
        duplicate connection establishment.
        """
        combinations = self._extract_unique_connection_combinations(config)

        if not combinations:
            logger.warning("No source or target connections found in configuration")
            return []

        results: List[PreflightResult] = []
        seen_identities: Dict[str, PreflightResult] = {}

        for combo_key, (spec, context) in combinations.items():
            # Create a unique display key (reference:database) to avoid duplicate output
            display_key = f"{spec.reference}:{context.get('database', 'unknown')}"

            if display_key not in seen_identities:
                # Establish connection with context so it can be reused by tables
                result = self._validate_connection_with_context(spec, context)
                results.append(result)
                self._display_connection_summary(result, spec, context)
                seen_identities[display_key] = result

        # Evaluate connection results
        required_failures = sum(
            1 for r in results if r.required and r.validation_status == "FAILED"
        )
        required_count = sum(1 for r in results if r.required)
        passed_count = required_count - required_failures

        logger.info(
            "Preflight connection validation: total=%d required=%d passed=%d failed=%d",
            len(results),
            required_count,
            passed_count,
            required_failures,
        )

        # Print summary
        if required_failures == 0:
            print("\nConnection preflight PASSED.")
            print("Loading table configurations...\n")
        else:
            print(f"\nConnection preflight FAILED: {required_failures} required connection(s) failed.")
            print("Table validation will not proceed.\n")

        return results

    def _validate_connection_with_context(
        self, spec: ConnectionSpec, context: Dict[str, Any]
    ) -> PreflightResult:
        """Test a connection with resolved context, establishing it once in the run-scoped cache.

        Unlike _validate(), this method:
        - Applies context (database/schema/table placeholders) before establishing the connection
        - Stores the connection in the run-scoped cache for reuse by tables
        - Returns a PreflightResult with validation status
        """
        started = perf_counter()
        raw = spec.raw
        result = PreflightResult(
            configuration_file_name=spec.config_file,
            connection_name=str(raw.get("name", spec.reference)),
            connection_reference=spec.reference,
            connection_type=str(raw.get("type", "")),
            classification=spec.classification,
            authentication_type=None,
            required=spec.required,
        )
        connector = None
        masking_filter = None
        filtered_loggers = []
        try:
            if not raw.get("enabled", True):
                result.environment_variable_resolution_status = "NOT_REQUIRED"
                result.validation_status = "SKIPPED"
                result.connection_status = "SKIPPED"
                result.message = "Connection is disabled by configuration"
                return result

            referenced_variables = set(_ENV_PATTERN.findall(json.dumps(raw)))
            missing = sorted({
                name for name in referenced_variables
                if os.getenv(name) is None
                or re.fullmatch(r"\s*<[^<>]+>\s*", os.getenv(name, ""))
            })
            if missing:
                result.environment_variable_resolution_status = "FAILED"
                result.failure_stage = "ENVIRONMENT_RESOLUTION"
                result.failure_reason = f"Missing required environment variable(s): {', '.join(missing)}"
                result.exception_category = "MISSING_ENVIRONMENT_VARIABLE"
                result.message = result.failure_reason
                return result
            result.environment_variable_resolution_status = "RESOLVED"

            resolved = normalize_connection_config(_expand_env_vars(raw))
            result.connection_type = normalize_connection_type(resolved.get("type"))
            result.authentication_type = resolved.get("authentication_type")
            masking_filter = MaskingFilter(collect_sensitive_values(resolved))
            filtered_loggers = [
                logging.getLogger(),
                logging.getLogger("core.connection_manager"),
                logger,
            ]
            for filtered_logger in filtered_loggers:
                filtered_logger.addFilter(masking_filter)
            config_class = TargetConfig if spec.classification in {"TARGET", "BOTH"} else SourceConfig
            allowed = {item.name for item in fields(config_class)}
            values = {key: value for key, value in resolved.items() if key in allowed and key != "name"}
            if not values.get("type"):
                raise ValueError("Missing connection type")
            config = config_class(name=str(resolved.get("name", spec.reference)), **values)

            # Establish connection with context applied
            connector = self._connector_for(
                config, connection_context=context, role=spec.classification.casefold()
            )
            result.connector_class_used = connector.__class__.__name__
            result.connection_attempt_status = "ATTEMPTED"
            result.executed_validation_checks.append("connector.execute_connectivity_query")
            # Test connectivity using the same cached check as table validation
            # This ensures consistent metadata call tracking and reuse metrics
            self._cached_metadata_operation(
                ("connection", id(connector)),
                connector.execute_connectivity_query,
            )
            result.connection_status = "CONNECTED"
            result.validation_status = "PASSED"
            result.message = "Connection validation completed successfully"
        except Exception as exc:
            result.connection_status = "FAILED"
            result.validation_status = "FAILED"
            result.failure_stage = result.failure_stage or (
                "CONNECTOR_BUILD" if connector is None else "CONNECTIVITY_TEST"
            )
            result.exception_category = result.exception_category or _exception_category(exc)
            result.failure_reason = mask_text(exc)
            result.message = f"Connection validation failed during {result.failure_stage.lower()}"
            logger.error("%s (%s)", result.message, result.exception_category)
        finally:
            if connector is not None:
                result.executed_validation_checks.append("connector.cached_for_reuse")
            if masking_filter is not None:
                for filtered_logger in filtered_loggers:
                    filtered_logger.removeFilter(masking_filter)
            result.validation_duration_seconds = round(perf_counter() - started, 6)
        return result

    def _run_table_smoke_test(self) -> PreflightReport:
        run_started = perf_counter()
        run_started_at = datetime.now(timezone.utc).isoformat()
        config = ConfigLoader(str(self.master_path)).load()
        file_names = self._table_file_names()
        self._open_connector_cache(getattr(config, "execution", None))
        try:
            # Establish source and target connections first, before table validation
            connection_results = self._validate_upfront_connections(config)

            # Pre-build metadata for all configured tables grouped by connection/database/schema
            # This optimization reduces redundant database calls by fetching metadata once per group
            self._prebuild_table_metadata(config)

            # Proceed with table validation using pre-built metadata
            # Tables will report connection-related failures if connections are invalid
            results = self._validate_configured_tables(config, file_names)
        finally:
            connection_metrics = self._close_connector_cache()

        # Evaluate overall status based on result type
        if not results:
            overall = "SKIPPED"
        elif all(isinstance(item, PreflightResult) for item in results):
            # All results are connection results
            if any(item.validation_status == "FAILED" for item in results):
                overall = "FAILED"
            else:
                overall = "PASSED"
        elif all(isinstance(item, IndependentTableSmokeResult) for item in results):
            # All results are table results
            if any(item.overall_preflight_validation_status != "PASSED" for item in results):
                overall = "FAILED"
            else:
                overall = "PASSED"
        else:
            # Mixed result types (shouldn't normally happen, but handle it)
            overall = "FAILED"

        failed_count = sum(
            1 for item in results
            if (
                isinstance(item, PreflightResult) and item.validation_status == "FAILED"
            ) or (
                isinstance(item, IndependentTableSmokeResult)
                and item.overall_preflight_validation_status != "PASSED"
            )
        )

        logger.info(
            "Preflight validation summary: overall_preflight_status=%s "
            "result_count=%d failed_count=%d",
            overall,
            len(results),
            failed_count,
        )
        settings = self.master.get("preflight", {})
        prefix = str(settings.get("run_id_prefix", "PREFLIGHT"))
        return PreflightReport(
            validation_run_id=f"{prefix}-{uuid.uuid4().hex[:8].upper()}",
            validation_timestamp=run_started_at,
            overall_preflight_status=overall,
            connections=results,
            connection_metrics=connection_metrics,
            wall_clock_duration=round(perf_counter() - run_started, 6),
            cumulative_worker_duration=round(sum(
                float(item.validation_duration_seconds or 0)
                for item in results
            ), 6),
            operation_timings=self.performance_ledger.operations,
        )

    def _prebuild_table_metadata(self, config: Any) -> None:
        """Pre-build lightweight TABLE_NAME+COLUMN_NAME metadata for preflight.

        This optimization:
        1. Groups tables by (side, connection_ref, database, schema)
        2. Fetches metadata only for configured tables (not whole schema)
        3. Fetches only TABLE_NAME and COLUMN_NAME (no datatypes)
        4. Caches lightweight {table_name: {column_names}} structure
        """
        if not hasattr(config, "tables"):
            return

        # Group tables by (side, connection_ref, database, schema, table_names)
        metadata_groups: Dict[tuple, tuple[Any, list]] = {}
        for table in config.tables:
            # Source side
            source_ref = getattr(table, "source_system", None)
            source_db = getattr(table, "source_database", None)
            source_schema = getattr(table, "source_schema", None)
            source_table = getattr(table, "source_table", None)
            if source_ref and source_db and source_schema and source_table:
                key = ("source", source_ref, source_db, source_schema)
                if key not in metadata_groups:
                    conn_config = config.sources.get(source_ref)
                    metadata_groups[key] = (conn_config, [])
                metadata_groups[key][1].append(source_table)

            # Target side
            target_ref = config.target_ref
            target_db = getattr(table, "target_database", None)
            target_schema = getattr(table, "target_schema", None)
            target_table = getattr(table, "target_table", None)
            if target_ref and target_db and target_schema and target_table:
                key = ("target", target_ref, target_db, target_schema)
                if key not in metadata_groups:
                    metadata_groups[key] = (config.target, [])
                metadata_groups[key][1].append(target_table)

        # For each group, fetch lightweight metadata once for configured tables
        for (side, conn_ref, db, schema), (conn_config, table_names) in metadata_groups.items():
            try:
                if not conn_config:
                    continue

                connector = self._connector_for(
                    conn_config,
                    connection_context={"database": db, "schema": schema},
                    role=side,
                )

                start_time = perf_counter()
                # OPTIMIZED: Fetch only TABLE_NAME and COLUMN_NAME for configured tables
                table_columns = connector.get_preflight_table_columns(schema, list(set(table_names)))
                duration = round(perf_counter() - start_time, 6)

                # Cache lightweight structure: {table_name: {column_names}}
                database_key = str(db or "").strip().casefold()
                schema_key = str(schema).strip().casefold()
                for table_name, column_names in table_columns.items():
                    table_key = str(table_name).strip().casefold()
                    cache_key = ("preflight_columns", id(connector), database_key, schema_key, table_key)
                    self.metadata_cache.get_or_compute(
                        cache_key,
                        lambda cols=column_names: (True, cols)
                    )

                self.performance_ledger.record_operation(
                    f"metadata_preflight_{side}_{conn_ref}_{db}_{schema}",
                    duration,
                )
                logger.info(
                    "Pre-built preflight metadata for %s.%s (%d tables): %.3fs",
                    db, schema, len(table_names), duration
                )
            except Exception as exc:
                logger.warning(
                    "Failed to pre-build preflight metadata for %s.%s: %s",
                    db, schema, mask_text(exc)
                )

    def _validate_configured_tables(
        self, config: Any, file_names: Dict[str, str]
    ) -> List[Any]:
        """Validate every configured table sequentially in configuration order."""
        tables = list(config.tables)
        results: List[Any] = []
        for table in tables:
            results.extend(
                self._validate_configured_table(
                    table, config, file_names.get(table.id, "")
                )
            )
        return results

    @staticmethod
    def _configured_columns(table: Any, side: str) -> List[str]:
        key = f"{side}_column"
        values: List[str] = []
        for attribute in (
            "key_mappings",
            "compare_column_mappings",
            "numeric_columns",
            "null_columns",
            "mandatory_columns",
        ):
            for item in getattr(table, attribute, []) or []:
                if hasattr(item, key):
                    value = getattr(item, key)
                elif isinstance(item, dict):
                    value = item.get(key) or item.get("column")
                else:
                    value = item
                if value:
                    values.append(str(value))
        if side == "target":
            values.extend(
                str(item)
                for item in (getattr(table, "post_transform_duplicate_columns", []) or [])
                if isinstance(item, str)
            )
        values.extend(
            str(item)
            for item in (getattr(table, "duplicate_columns", []) or [])
            if isinstance(item, str)
        )
        return list(dict.fromkeys(values))

    @staticmethod
    def _status_from_checks(checks: List[Any]) -> str:
        statuses = {getattr(item, "status", None) for item in checks}
        if ValidationStatus.ERROR in statuses:
            return "ERROR"
        if ValidationStatus.FAIL in statuses:
            return "FAILED"
        if statuses and statuses <= {ValidationStatus.SKIP}:
            return "SKIPPED"
        return "PASSED" if ValidationStatus.PASS in statuses else "SKIPPED"

    def _new_table_result(
        self, table: Any, file_name: str, side: str, connection: Any
    ) -> TablePreflightResult:
        server = self._table_server(table, side, connection)
        database = getattr(table, f"{side}_database", None)
        schema = getattr(table, f"{side}_schema")
        table_name = getattr(table, f"{side}_table")
        reference = (
            table.source_system if side == "source" else table.target_system
        )
        key_columns = [
            getattr(mapping, f"{side}_column")
            for mapping in table.key_mappings
        ]
        return TablePreflightResult(
            layer_name=table.target_layer,
            configuration_file_name=file_name,
            table_configuration_id=table.id,
            classification=side.upper(),
            connection_reference=reference,
            connection_type=normalize_connection_type(connection.type),
            # The centralized masking policy decides which report metadata is
            # visible. Preserve the real server value until that policy runs.
            masked_server_name=server,
            database_name=database,
            schema_name=schema,
            table_name=table_name,
            configured_key_columns=key_columns,
            configured_columns=self._configured_columns(table, side),
            validation_start_time=datetime.now(timezone.utc).isoformat(),
        )

    @staticmethod
    def _table_server(table: Any, side: str, connection_config: Any) -> Any:
        """Resolve table metadata first, then the selected connection endpoint."""
        return (
            getattr(table, f"{side}_server", None)
            or getattr(connection_config, "sql_endpoint", None)
            or getattr(connection_config, "host", None)
        )

    def _validate_configured_table(
        self, table: Any, config: Any, file_name: str
    ) -> List[IndependentTableSmokeResult]:
        source_config = config.sources.get(table.source_system)
        target_config = config.target if table.target_system == config.target_ref else None
        if source_config is None:
            raise ValueError(
                f"Table '{table.id}' references unknown source connection "
                f"'{table.source_system}'"
            )
        if target_config is None:
            raise ValueError(
                f"Table '{table.id}' references unknown target connection "
                f"'{table.target_system}'"
            )
        source_result = self._new_table_result(
            table, file_name, "source", source_config
        )
        target_result = self._new_table_result(
            table, file_name, "target", target_config
        )
        source_connector = None
        target_connector = None
        started = perf_counter()
        try:
            source_connector = self._validate_table_object(
                table, "source", source_config, source_result
            )
            target_connector = self._validate_table_object(
                table, "target", target_config, target_result
            )
        finally:
            # Connectors stay open: they are shared with every other table that
            # uses the same connection details and are disposed once, after the
            # complete preflight run finishes.
            completed = datetime.now(timezone.utc).isoformat()
            duration = round(perf_counter() - started, 6)
            for result in (source_result, target_result):
                result.validation_completion_time = completed
                result.validation_duration_seconds = duration
                statuses = [
                    value
                    for key, value in asdict(result).items()
                    if key.endswith("_status") and key not in {
                        "overall_validation_status", "validation_status"
                    }
                    and value != "SKIPPED"
                ]
                if "ERROR" in statuses:
                    overall = "ERROR"
                elif "FAILED" in statuses:
                    overall = "FAILED"
                elif statuses:
                    overall = "PASSED"
                else:
                    overall = "SKIPPED"
                result.overall_validation_status = overall
                result.validation_status = overall
                if not result.message:
                    result.message = (
                        "Configured table smoke tests completed"
                        if overall == "PASSED"
                        else "Configured table smoke tests were skipped"
                    )
        return [
            self._combine_independent_results(
                table,
                file_name,
                source_result,
                target_result,
                self._side_object_valid(source_connector, source_result),
                self._side_object_valid(target_connector, target_result),
            )
        ]

    def _connector_for(
        self,
        connection_config: Any,
        connection_context: Optional[Dict[str, Any]] = None,
        role: Optional[str] = None,
    ) -> Any:
        """Return the shared connector for this connection's identity.

        Falls back to a direct build only when no run-scoped cache is open, so
        the method stays usable outside a full preflight run.
        """
        if self.connector_cache is not None:
            return self.connector_cache.get(
                connection_config, connection_context, role
            )
        return build_connector(connection_config, self.context, connection_context)

    @staticmethod
    def _is_configured_value(value: Any) -> bool:
        text = str(value or "").strip()
        return bool(text) and not (
            text.startswith("${") and text.endswith("}")
        ) and not (
            text.startswith("{") and text.endswith("}")
        ) and not re.fullmatch(r"<[^<>]+>", text)

    def _cached_metadata_operation(self, key: Any, operation: Any) -> Any:
        """Single-flight one metadata operation with timing, including failed outcomes."""
        def capture() -> tuple[bool, Any]:
            start_time = perf_counter()
            try:
                result = operation()
                duration = round(perf_counter() - start_time, 6)
                # Record timing for this operation
                op_name = str(key[0]) if isinstance(key, tuple) else str(key)
                self.performance_ledger.record_operation(f"remote_metadata_{op_name}", duration)
                return True, result
            except Exception as exc:  # cache failures to avoid retry storms
                duration = round(perf_counter() - start_time, 6)
                op_name = str(key[0]) if isinstance(key, tuple) else str(key)
                self.performance_ledger.record_operation(f"remote_metadata_failed_{op_name}", duration)
                return False, exc

        succeeded, value = self.metadata_cache.get_or_compute(key, capture)
        if succeeded:
            return value
        raise value

    def _table_columns(
        self,
        connector: Any,
        database: Any,
        schema: str,
        table_name: str,
        preflight_mode: bool = False,
    ) -> Any:
        """Return cached columns, using pre-built metadata when available.

        In preflight mode: returns Set[str] (column names only, lightweight)
        In normal mode: returns Dict[str, str] (column names mapped to datatypes)
        """
        connector_key = id(connector)
        database_key = str(database or "").strip().casefold()
        schema_key = str(schema).strip().casefold()
        table_key = str(table_name).strip().casefold()

        if preflight_mode:
            # Try preflight lightweight metadata first
            prebuilt_key = ("preflight_columns", connector_key, database_key, schema_key, table_key)
            cached_result = self.metadata_cache.get_or_compute(
                prebuilt_key, lambda: (False, None)
            )
            if cached_result[0] and isinstance(cached_result[1], set):
                return cached_result[1]  # Return column name set
            # If preflight metadata not found, return empty set (metadata may not have been pre-built)
            return set()

        # Normal mode: try pre-built metadata first (built during _prebuild_table_metadata)
        prebuilt_key = (f"source_table_columns", connector_key, database_key, schema_key, table_key)
        cached_result = self.metadata_cache.get_or_compute(
            prebuilt_key, lambda: (False, None)  # Dummy to check if key exists
        )
        if cached_result[0]:  # If pre-built metadata exists
            return dict(cached_result[1])

        # Fallback: fetch on-demand (for tables not pre-built or if pre-build failed)
        start_time = perf_counter()
        if bool(getattr(connector, "BULK_SCHEMA_METADATA", False)):
            tables = self._cached_metadata_operation(
                ("schema_columns", connector_key, database_key, schema_key),
                lambda: connector.get_schema_table_columns(str(schema)),
            )
            requested = str(table_name)
            if requested in tables:
                duration = round(perf_counter() - start_time, 6)
                self.performance_ledger.record_operation(
                    f"metadata_fallback_bulk_{database_key}_{schema_key}",
                    duration,
                )
                return dict(tables[requested])
            requested_key = requested.casefold()
            for available_table, columns in tables.items():
                if str(available_table).casefold() == requested_key:
                    duration = round(perf_counter() - start_time, 6)
                    self.performance_ledger.record_operation(
                        f"metadata_fallback_bulk_{database_key}_{schema_key}",
                        duration,
                    )
                    return dict(columns)
            return {}

        result = dict(self._cached_metadata_operation(
            (
                "table_columns", connector_key, database_key, schema_key, table_key,
            ),
            lambda: connector.get_table_columns(str(schema), str(table_name)),
        ))
        duration = round(perf_counter() - start_time, 6)
        self.performance_ledger.record_operation(
            f"metadata_fallback_single_{database_key}_{schema_key}_{table_key}",
            duration,
        )
        return result

    def _validate_table_object(
        self,
        table: Any,
        side: str,
        connection_config: Any,
        result: TablePreflightResult,
    ) -> Optional[Any]:
        settings = self.preflight_checks or {}
        server = self._table_server(table, side, connection_config)
        database = getattr(table, f"{side}_database", None)
        schema = getattr(table, f"{side}_schema", None)
        table_name = getattr(table, f"{side}_table", None)
        connector = None
        current_check = "connection_validation_status"
        try:
            required_values = {
                f"{side}_server": server,
                f"{side}_database": database,
                f"{side}_schema": schema,
                f"{side}_table": table_name,
            }
            missing = [
                name
                for name, value in required_values.items()
                if not self._is_configured_value(value)
            ]
            if missing:
                raise ValueError(
                    f"Table '{table.id}' is missing configured value(s): "
                    + ", ".join(missing)
                )

            configured_server = str(
                getattr(connection_config, "sql_endpoint", None)
                or getattr(connection_config, "host", None)
                or ""
            ).strip()

            # A table-level server name can legitimately differ from the connection
            # endpoint (for example short host name vs FQDN, alias, or SQL instance).
            # Treat only this specific mismatch as a warning and continue with the
            # selected connection configuration. All real connection/database/schema/
            # table/column validation exceptions still fail through the outer handler.
            try:
                if (
                    self._is_configured_value(configured_server)
                    and self._is_configured_value(server)
                    and configured_server.casefold() != str(server).strip().casefold()
                ):
                    raise ValueError(
                        f"Configured {side} server does not match connection "
                        f"'{result.connection_reference}'"
                    )
            except ValueError as server_exc:
                warning_message = mask_text(
                    server_exc,
                    (server, configured_server),
                )
                logger.warning(
                    "Server validation warning for table '%s' [%s]: %s. "
                    "Continuing with configured connection endpoint.",
                    table.id,
                    side,
                    warning_message,
                )
                result.validation_details.setdefault("warnings", []).append(
                    {
                        "check": "server_validation",
                        "side": side,
                        "status": "WARNING",
                        "message": warning_message,
                        "action": "Continued with configured connection endpoint",
                    }
                )

                # Use the connection configuration as the runtime source of truth.
                server = configured_server or server

            configured_database = str(
                getattr(connection_config, "database", None) or ""
            ).strip()
            if (
                self._is_configured_value(configured_database)
                and configured_database.casefold() != str(database).strip().casefold()
            ):
                raise ValueError(
                    f"Configured {side} database does not match connection "
                    f"'{result.connection_reference}'"
                )

            # The connection is established once per unique connection identity
            # and shared by every table that resolves to the same details.
            connector = self._connector_for(
                connection_config,
                connection_context={
                    "database": database,
                    "schema": schema,
                    "table": table_name,
                },
                role=side,
            )
            if settings["connection_validation"]:
                # The connectivity query is itself the proof that the configured
                # endpoint is reachable, so no separate probe is issued.
                self._cached_metadata_operation(
                    ("connection", id(connector)),
                    connector.execute_connectivity_query,
                )
                result.connection_validation_status = "PASSED"
            elif any(settings.values()):
                # Connectivity is not being reported, but later checks still need
                # a proven connection: borrow one and return it immediately.
                self._cached_metadata_operation(
                    ("connection", id(connector)), connector.ensure_connection
                )

            if settings["database_availability"]:
                current_check = "database_availability_status"
                current_database = self._cached_metadata_operation(
                    (
                        "database", id(connector),
                        str(database).strip().casefold(),
                    ),
                    connector.get_current_database,
                )
                if not current_database or (
                    current_database.casefold() != str(database).strip().casefold()
                ):
                    result.database_availability_status = "FAILED"
                    raise RuntimeError(
                        f"Connected database does not match configured {side} database"
                    )
                result.database_availability_status = "PASSED"

            if settings["schema_availability"]:
                current_check = "schema_availability_status"
                schema_exists = self._cached_metadata_operation(
                    (
                        "schema", id(connector),
                        str(database).strip().casefold(),
                        str(schema).strip().casefold(),
                    ),
                    lambda: connector.schema_exists(str(schema)),
                )
                if not schema_exists:
                    result.schema_availability_status = "FAILED"
                    raise RuntimeError(
                        f"Configured {side} schema does not exist or is inaccessible"
                    )
                result.schema_availability_status = "PASSED"

            need_columns = any(
                settings[name]
                for name in (
                    "table_availability",
                    "column_availability",
                    "key_column_availability",
                )
            )
            actual_columns = (
                self._table_columns(
                    connector, database, str(schema), str(table_name),
                    preflight_mode=True
                )
                if need_columns
                else set()
            )
            if settings["table_availability"]:
                current_check = "table_availability_status"
                result.table_availability_status = (
                    "PASSED" if actual_columns else "FAILED"
                )
                if not actual_columns:
                    raise RuntimeError(
                        f"Configured {side} table does not exist or is inaccessible"
                    )

            actual_names = set(actual_columns)
            expected_names = set(result.configured_columns)
            if settings["column_availability"]:
                current_check = "column_availability_status"
                result.missing_columns = sorted(expected_names - actual_names)
                result.additional_columns = sorted(actual_names - expected_names)
                result.column_availability_status = (
                    "FAILED" if result.missing_columns else "PASSED"
                )

            if settings["key_column_availability"]:
                current_check = "key_column_availability_status"
                missing_keys = sorted(
                    set(result.configured_key_columns) - actual_names
                )
                result.key_column_availability_status = (
                    "FAILED" if missing_keys else "PASSED"
                )
            object_failures = []
            if result.missing_columns:
                object_failures.append(
                    f"missing columns: {', '.join(result.missing_columns)}"
                )
            if settings["key_column_availability"]:
                missing_keys = sorted(
                    set(result.configured_key_columns) - actual_names
                )
                if missing_keys:
                    object_failures.append(
                        f"missing key columns: {', '.join(missing_keys)}"
                    )
            result.validation_details["object_metadata"] = {
                "server": result.masked_server_name,
                "database": database,
                "schema": schema,
                "table": table_name,
                "configured_columns": result.configured_columns,
                "available_columns": sorted(actual_columns) if isinstance(actual_columns, set) else list(actual_columns),
                "missing_columns": result.missing_columns,
                "additional_columns": result.additional_columns,
                "configured_key_columns": result.configured_key_columns,
            }
            if object_failures:
                result.failure_reason = "; ".join(object_failures)
                result.message = f"{side.title()} object validation failed"
            return connector
        except Exception as exc:
            if getattr(result, current_check) not in {"FAILED", "PASSED"}:
                setattr(result, current_check, "FAILED")
            result.failure_reason = mask_text(exc, (server, configured_server if 'configured_server' in locals() else None))
            result.message = f"{side.title()} object validation failed"
            logger.error("%s: %s", result.message, result.failure_reason)
            # The connector is shared with every other table using the same
            # connection details, so it is never closed here.
            return None

    def _run_configured_data_checks(
        self,
        table: Any,
        config: Any,
        source_connector: Any,
        target_connector: Any,
        source_result: TablePreflightResult,
        target_result: TablePreflightResult,
    ) -> None:
        settings = self.preflight_checks or {}
        source_valid = self._side_object_valid(source_connector, source_result)
        target_valid = self._side_object_valid(target_connector, target_result)

        source_values = (
            self._run_side_smoke_checks(
                table, "source", source_connector, source_result
            )
            if source_valid
            else {}
        )
        target_values = (
            self._run_side_smoke_checks(
                table, "target", target_connector, target_result
            )
            if target_valid
            else {}
        )

        if source_valid and target_valid:
            self._run_cached_comparisons(
                table,
                config,
                source_values,
                target_values,
                source_result,
                target_result,
            )

    @staticmethod
    def _side_object_valid(
        connector: Any, result: TablePreflightResult
    ) -> bool:
        statuses = {
            result.connection_validation_status,
            result.database_availability_status,
            result.schema_availability_status,
            result.table_availability_status,
            result.column_availability_status,
            result.key_column_availability_status,
        }
        return connector is not None and not statuses & {"FAILED", "ERROR"}

    @staticmethod
    def _mapping_column(item: Any, side: str) -> Optional[str]:
        key = f"{side}_column"
        if hasattr(item, key):
            return getattr(item, key)
        if isinstance(item, dict):
            return item.get(key) or item.get("column")
        return str(item) if isinstance(item, str) else None

    def _run_side_smoke_checks(
        self,
        table: Any,
        side: str,
        connector: Any,
        result: TablePreflightResult,
    ) -> Dict[str, Any]:
        settings = self.preflight_checks or {}
        schema = getattr(table, f"{side}_schema")
        table_name = getattr(table, f"{side}_table")
        dialect = resolve_dialect(connector)
        fqn = dialect.qualify(schema, table_name)
        values: Dict[str, Any] = {}
        details = result.validation_details

        row_count = None
        if settings["row_count"] or settings["null_validation"]:
            try:
                sql = dialect.select(
                    "COUNT(*) AS row_count", fqn
                )
                row_count = int(connector.execute_query(sql).iloc[0, 0])
                values["row_count"] = row_count
                if side == "source":
                    result.source_row_count = row_count
                else:
                    result.target_row_count = row_count
                if settings["row_count"]:
                    status = (
                        "PASSED"
                        if row_count > 0 or bool(getattr(table, "allow_zero_rows", False))
                        else "FAILED"
                    )
                    result.row_count_validation_status = status
                    details["row_count"] = {
                        "status": status,
                        "row_count": row_count,
                        "query": sql,
                    }
            except Exception as exc:
                if settings["row_count"]:
                    result.row_count_validation_status = "FAILED"
                details["row_count"] = {
                    "status": "FAILED",
                    "message": mask_text(exc),
                }

        key_columns = [
            self._mapping_column(item, side)
            for item in (getattr(table, "key_mappings", []) or [])
        ]
        key_columns = [item for item in key_columns if item]
        if settings["key_validation"]:
            if not key_columns:
                result.key_validation_status = "NOT_CONFIGURED"
            else:
                try:
                    key_list = ", ".join(
                        dialect.quote(column) for column in key_columns
                    )
                    null_condition = " OR ".join(
                        f"{dialect.quote(column)} IS NULL"
                        for column in key_columns
                    )
                    null_sql = (
                        f"SELECT COUNT(*) FROM {fqn} WHERE {null_condition}"
                    )
                    null_count = int(
                        connector.execute_query(null_sql).iloc[0, 0]
                    )
                    duplicate_sql = (
                        "SELECT COUNT(*) AS duplicate_group_count, "
                        "COALESCE(SUM(group_size), 0) AS duplicate_row_count "
                        "FROM ("
                        f"SELECT COUNT(*) AS group_size FROM {fqn} "
                        f"GROUP BY {key_list} HAVING COUNT(*) > 1"
                        ") duplicate_groups"
                    )
                    duplicate_row = connector.execute_query(duplicate_sql).iloc[0]
                    duplicate_groups = int(duplicate_row.iloc[0] or 0)
                    duplicate_rows = int(duplicate_row.iloc[1] or 0)
                    signature_sql = (
                        "SELECT COUNT(*) AS distinct_key_count, "
                        "COALESCE(CHECKSUM_AGG(BINARY_CHECKSUM("
                        f"{key_list})), 0) AS key_checksum "
                        "FROM ("
                        f"SELECT {key_list} FROM {fqn} GROUP BY {key_list}"
                        ") AS distinct_keys"
                    )
                    signature_row = connector.execute_query(signature_sql).iloc[0]
                    distinct_key_count = int(signature_row.iloc[0] or 0)
                    key_checksum = int(signature_row.iloc[1] or 0)
                    values["key_signature"] = (
                        distinct_key_count,
                        key_checksum,
                    )
                    values["key_duplicate_details"] = {
                        "columns": key_columns,
                        "duplicate_group_count": duplicate_groups,
                        "duplicate_row_count": duplicate_rows,
                        "query": duplicate_sql,
                    }
                    status = (
                        "PASSED"
                        if null_count == 0
                        and (
                            duplicate_groups == 0
                            or bool(getattr(table, "allow_duplicates", False))
                        )
                        else "FAILED"
                    )
                    result.key_validation_status = status
                    details["key_validation"] = {
                        "status": status,
                        "configured_key_columns": key_columns,
                        "null_key_row_count": null_count,
                        "duplicate_key_group_count": duplicate_groups,
                        "duplicate_key_row_count": duplicate_rows,
                        "distinct_key_count": distinct_key_count,
                        "key_checksum": key_checksum,
                        "comparison_scope": "sql_scalar_signature",
                        "queries": [null_sql, duplicate_sql, signature_sql],
                    }
                except Exception as exc:
                    result.key_validation_status = "FAILED"
                    details["key_validation"] = {
                        "status": "FAILED",
                        "message": mask_text(exc),
                    }

        if settings["null_validation"]:
            null_mappings = (
                getattr(table, "null_columns", []) or
                getattr(table, "compare_column_mappings", []) or
                getattr(table, "numeric_columns", []) or []
            )
            mandatory = {
                self._mapping_column(item, side)
                for item in (getattr(table, "mandatory_columns", []) or [])
            }
            null_details = []
            failed = False
            for item in null_mappings:
                column = self._mapping_column(item, side)
                if not column:
                    continue
                try:
                    sql = (
                        "SELECT SUM(CASE WHEN "
                        f"{dialect.quote(column)} IS NULL THEN 1 ELSE 0 END) FROM {fqn}"
                    )
                    null_count = int(
                        connector.execute_query(sql).iloc[0, 0] or 0
                    )
                    total = row_count or 0
                    percentage = (null_count / total * 100) if total else 0.0
                    threshold = (
                        item.get("max_null_percentage")
                        if isinstance(item, dict)
                        else None
                    )
                    if column in mandatory:
                        threshold = 0.0
                    item_status = (
                        "PASSED"
                        if threshold is None or percentage <= float(threshold)
                        else "FAILED"
                    )
                    failed = failed or item_status == "FAILED"
                    null_details.append({
                        "column": column,
                        "null_count": null_count,
                        "total_row_count": total,
                        "null_percentage": round(percentage, 6),
                        "configured_threshold": threshold,
                        "status": item_status,
                        "query": sql,
                    })
                except Exception as exc:
                    failed = True
                    null_details.append({
                        "column": column,
                        "status": "FAILED",
                        "message": mask_text(exc),
                    })
            result.null_validation_status = (
                "NOT_CONFIGURED" if not null_details
                else "FAILED" if failed else "PASSED"
            )
            details["null_validation"] = null_details
            values["nulls"] = {
                item["column"]: item
                for item in null_details
                if "null_percentage" in item
            }

        if settings["duplicate_check"]:
            duplicate_columns = key_columns or [
                self._mapping_column(item, side)
                for item in (getattr(table, "duplicate_columns", []) or [])
            ]
            duplicate_columns = [item for item in duplicate_columns if item]
            if not duplicate_columns:
                result.duplicate_validation_status = "NOT_CONFIGURED"
            else:
                try:
                    cached = values.get("key_duplicate_details")
                    if cached and cached["columns"] == duplicate_columns:
                        sql = cached["query"]
                        group_count = cached["duplicate_group_count"]
                        duplicate_rows = cached["duplicate_row_count"]
                    else:
                        columns = ", ".join(
                            dialect.quote(column)
                            for column in duplicate_columns
                        )
                        sql = (
                            "SELECT COUNT(*) AS duplicate_group_count, "
                            "COALESCE(SUM(group_size), 0) AS duplicate_row_count "
                            "FROM ("
                            f"SELECT COUNT(*) AS group_size FROM {fqn} "
                            f"GROUP BY {columns} HAVING COUNT(*) > 1"
                            ") duplicate_groups"
                        )
                        row = connector.execute_query(sql).iloc[0]
                        group_count = int(row.iloc[0] or 0)
                        duplicate_rows = int(row.iloc[1] or 0)
                    values["duplicates"] = group_count
                    status = (
                        "PASSED"
                        if group_count == 0
                        or bool(getattr(table, "allow_duplicates", False))
                        else "FAILED"
                    )
                    result.duplicate_validation_status = status
                    details["duplicate_validation"] = {
                        "status": status,
                        "columns": duplicate_columns,
                        "duplicate_group_count": group_count,
                        "duplicate_row_count": duplicate_rows,
                        "query": sql,
                    }
                except Exception as exc:
                    result.duplicate_validation_status = "FAILED"
                    details["duplicate_validation"] = {
                        "status": "FAILED",
                        "message": mask_text(exc),
                    }

        if settings["aggregate_validation"]:
            aggregate_details: Dict[str, Any] = {}
            failed = False
            for item in (getattr(table, "numeric_columns", []) or []):
                column = self._mapping_column(item, side)
                if not column:
                    continue
                quoted_column = dialect.quote(column)
                sql = (
                    f"SELECT {dialect.count_column(quoted_column)}, "
                    f"{dialect.count_distinct(quoted_column)}, "
                    f"SUM({quoted_column}), MIN({quoted_column}), "
                    f"MAX({quoted_column}), AVG({quoted_column}) "
                    f"FROM {fqn}"
                )
                try:
                    row = connector.execute_query(sql).iloc[0]
                    aggregate_details[column] = {
                        "count": row.iloc[0],
                        "count_distinct": row.iloc[1],
                        "sum": row.iloc[2],
                        "min": row.iloc[3],
                        "max": row.iloc[4],
                        "avg": row.iloc[5],
                        "status": "PASSED",
                        "query": sql,
                    }
                except Exception as exc:
                    failed = True
                    aggregate_details[column] = {
                        "status": "FAILED",
                        "message": mask_text(exc),
                    }
            result.aggregate_validation_status = (
                "NOT_CONFIGURED" if not aggregate_details
                else "FAILED" if failed else "PASSED"
            )
            details["aggregate_validation"] = aggregate_details
            values["aggregates"] = aggregate_details
        return values

    @staticmethod
    def _merge_validation_status(side_status: str, comparison_status: str) -> str:
        if "FAILED" in {side_status, comparison_status}:
            return "FAILED"
        if side_status == "PASSED" or comparison_status == "PASSED":
            return "PASSED"
        if "SKIPPED" in {side_status, comparison_status}:
            return "SKIPPED"
        return "NOT_CONFIGURED"

    def _run_cached_comparisons(
        self,
        table: Any,
        config: Any,
        source_values: Dict[str, Any],
        target_values: Dict[str, Any],
        source_result: TablePreflightResult,
        target_result: TablePreflightResult,
    ) -> None:
        settings = self.preflight_checks or {}
        statuses: Dict[str, str] = {}
        details: Dict[str, Any] = {}

        if settings["row_count"]:
            if "row_count" in source_values and "row_count" in target_values:
                source_count = source_values["row_count"]
                target_count = target_values["row_count"]
                delta = abs(source_count - target_count)
                delta_pct = (
                    0.0
                    if source_count == target_count == 0
                    else 100.0
                    if source_count == 0
                    else delta / abs(source_count) * 100
                )
                tolerance = config.thresholds.row_count_tolerance_pct
                statuses["row_count"] = (
                    "PASSED" if delta_pct <= tolerance else "FAILED"
                )
                details["row_count"] = {
                    "status": statuses["row_count"],
                    "source_row_count": source_count,
                    "target_row_count": target_count,
                    "absolute_difference": delta,
                    "difference_percentage": round(delta_pct, 6),
                    "configured_tolerance_percentage": tolerance,
                }
            else:
                statuses["row_count"] = "SKIPPED"

        if settings["key_validation"]:
            source_signature = source_values.get("key_signature")
            target_signature = target_values.get("key_signature")
            if source_signature is not None and target_signature is not None:
                statuses["key"] = (
                    "PASSED"
                    if source_signature == target_signature
                    else "FAILED"
                )
                details["key"] = {
                    "status": statuses["key"],
                    "comparison_scope": "sql_scalar_signature",
                    "source_distinct_key_count": source_signature[0],
                    "target_distinct_key_count": target_signature[0],
                    "source_key_checksum": source_signature[1],
                    "target_key_checksum": target_signature[1],
                    "exact_record_reconciliation": False,
                }
            else:
                statuses["key"] = "SKIPPED"

        if settings["null_validation"]:
            source_nulls = source_values.get("nulls", {})
            target_nulls = target_values.get("nulls", {})
            comparisons = []
            for source_item, target_item in zip(
                source_nulls.values(), target_nulls.values()
            ):
                delta = (
                    target_item["null_percentage"]
                    - source_item["null_percentage"]
                )
                comparisons.append({
                    "source_column": source_item["column"],
                    "target_column": target_item["column"],
                    "source_null_count": source_item["null_count"],
                    "target_null_count": target_item["null_count"],
                    "source_null_percentage": source_item["null_percentage"],
                    "target_null_percentage": target_item["null_percentage"],
                    "null_percentage_difference": round(delta, 6),
                    "status": "PASSED" if delta == 0 else "FAILED",
                })
            statuses["null"] = (
                "NOT_CONFIGURED" if not comparisons
                else "FAILED"
                if any(item["status"] == "FAILED" for item in comparisons)
                else "PASSED"
            )
            details["null"] = comparisons

        if settings["duplicate_check"]:
            if "duplicates" in source_values and "duplicates" in target_values:
                statuses["duplicate"] = (
                    "PASSED"
                    if source_values["duplicates"] == target_values["duplicates"]
                    else "FAILED"
                )
                details["duplicate"] = {
                    "status": statuses["duplicate"],
                    "source_duplicate_group_count": source_values["duplicates"],
                    "target_duplicate_group_count": target_values["duplicates"],
                }
            else:
                statuses["duplicate"] = "NOT_CONFIGURED"

        if settings["aggregate_validation"]:
            source_aggregates = source_values.get("aggregates", {})
            target_aggregates = target_values.get("aggregates", {})
            comparisons = []
            tolerance = config.thresholds.aggregate_tolerance_pct
            for source_item, target_item in zip(
                source_aggregates.values(), target_aggregates.values()
            ):
                if (
                    source_item.get("status") != "PASSED"
                    or target_item.get("status") != "PASSED"
                ):
                    continue
                for aggregate in (
                    "count", "count_distinct", "sum", "min", "max", "avg"
                ):
                    source_value = source_item.get(aggregate)
                    target_value = target_item.get(aggregate)
                    if source_value is None and target_value is None:
                        continue
                    try:
                        delta = abs(float(source_value or 0) - float(target_value or 0))
                        delta_pct = (
                            0.0
                            if float(source_value or 0) == float(target_value or 0)
                            else 100.0
                            if float(source_value or 0) == 0
                            else delta / abs(float(source_value)) * 100
                        )
                        item_status = (
                            "PASSED" if delta_pct <= tolerance else "FAILED"
                        )
                    except (TypeError, ValueError):
                        delta = None
                        delta_pct = None
                        item_status = (
                            "PASSED" if source_value == target_value else "FAILED"
                        )
                    comparisons.append({
                        "aggregate": aggregate,
                        "source_value": source_value,
                        "target_value": target_value,
                        "absolute_difference": delta,
                        "difference_percentage": delta_pct,
                        "status": item_status,
                    })
            statuses["aggregate"] = (
                "NOT_CONFIGURED" if not comparisons
                else "FAILED"
                if any(item["status"] == "FAILED" for item in comparisons)
                else "PASSED"
            )
            details["aggregate"] = comparisons

        comparison_payload = {
            "statuses": statuses,
            "details": details,
        }
        source_result.validation_details["comparison"] = comparison_payload
        target_result.validation_details["comparison"] = comparison_payload

    def _combine_independent_results(
        self,
        table: Any,
        file_name: str,
        source: TablePreflightResult,
        target: TablePreflightResult,
        source_valid: bool,
        target_valid: bool,
    ) -> IndependentTableSmokeResult:
        settings = self.preflight_checks or {}
        comparison = source.validation_details.get("comparison", {})
        comparison_statuses = comparison.get("statuses", {})

        def configured(setting: str, value: str) -> str:
            return value if settings[setting] else "NOT_CONFIGURED"

        def data_status(
            setting: str, side_valid: bool, value: str
        ) -> str:
            if not settings[setting]:
                return "NOT_CONFIGURED"
            return value if side_valid else "SKIPPED"

        source_object_status = "PASSED" if source_valid else "FAILED"
        target_object_status = "PASSED" if target_valid else "FAILED"
        source_failure = source.failure_reason
        target_failure = target.failure_reason

        if source_valid and target_valid:
            message = (
                "Source-side, target-side, and configured comparison "
                "validations completed successfully."
            )
        elif source_valid:
            message = (
                "Source-side validation completed successfully. Target object "
                "validation failed. Source-to-target comparison validations were skipped."
            )
        elif target_valid:
            message = (
                "Target-side validation completed successfully. Source object "
                "validation failed. Source-to-target comparison validations were skipped."
            )
        else:
            message = (
                "Source and target object validations failed. "
                "Data validations were not executed."
            )

        comparison_default = (
            "SKIPPED" if source_valid != target_valid or not (source_valid or target_valid)
            else "NOT_CONFIGURED"
        )
        row_comparison = (
            comparison_statuses.get("row_count", comparison_default)
            if settings["row_count"] else "NOT_CONFIGURED"
        )
        key_comparison = (
            comparison_statuses.get("key", comparison_default)
            if settings["key_validation"] else "NOT_CONFIGURED"
        )
        null_comparison = (
            comparison_statuses.get("null", comparison_default)
            if settings["null_validation"] else "NOT_CONFIGURED"
        )
        duplicate_comparison = (
            comparison_statuses.get("duplicate", comparison_default)
            if settings["duplicate_check"] else "NOT_CONFIGURED"
        )
        aggregate_comparison = (
            comparison_statuses.get("aggregate", comparison_default)
            if settings["aggregate_validation"] else "NOT_CONFIGURED"
        )

        required_statuses = [
            source_object_status,
            target_object_status,
            data_status("row_count", source_valid, source.row_count_validation_status),
            data_status("row_count", target_valid, target.row_count_validation_status),
            data_status("key_validation", source_valid, source.key_validation_status),
            data_status("key_validation", target_valid, target.key_validation_status),
            data_status("null_validation", source_valid, source.null_validation_status),
            data_status("null_validation", target_valid, target.null_validation_status),
            data_status("duplicate_check", source_valid, source.duplicate_validation_status),
            data_status("duplicate_check", target_valid, target.duplicate_validation_status),
            data_status("aggregate_validation", source_valid, source.aggregate_validation_status),
            data_status("aggregate_validation", target_valid, target.aggregate_validation_status),
            row_comparison,
            key_comparison,
            null_comparison,
            duplicate_comparison,
            aggregate_comparison,
        ]
        if source_valid != target_valid:
            overall = "PARTIALLY_PASSED"
            validation_status = "FAILED"
        elif not source_valid and not target_valid:
            overall = "FAILED"
            validation_status = "FAILED"
        elif "FAILED" in required_statuses:
            overall = "FAILED"
            validation_status = "FAILED"
            message = (
                "Source-side, target-side, and configured comparison validations "
                "completed with failures."
            )
        else:
            overall = "PASSED"
            validation_status = "PASSED"

        failure_reason = "; ".join(
            item for item in (source_failure, target_failure) if item
        ) or None
        preflight_values = {
            "source_connection_validation_status": configured(
                "connection_validation", source.connection_validation_status
            ),
            "target_connection_validation_status": configured(
                "connection_validation", target.connection_validation_status
            ),
            "source_database_availability_status": configured(
                "database_availability", source.database_availability_status
            ),
            "target_database_availability_status": configured(
                "database_availability", target.database_availability_status
            ),
            "source_schema_availability_status": configured(
                "schema_availability", source.schema_availability_status
            ),
            "target_schema_availability_status": configured(
                "schema_availability", target.schema_availability_status
            ),
            "source_table_availability_status": configured(
                "table_availability", source.table_availability_status
            ),
            "target_table_availability_status": configured(
                "table_availability", target.table_availability_status
            ),
            "source_column_availability_status": configured(
                "column_availability", source.column_availability_status
            ),
            "target_column_availability_status": configured(
                "column_availability", target.column_availability_status
            ),
            "source_key_column_availability_status": configured(
                "key_column_availability", source.key_column_availability_status
            ),
            "target_key_column_availability_status": configured(
                "key_column_availability", target.key_column_availability_status
            ),
            "source_object_validation_status": configured(
                "object_validation", source_object_status
            ),
            "target_object_validation_status": configured(
                "object_validation", target_object_status
            ),
        }
        preflight_status, failed_preflight_checks, preflight_reason = (
            evaluate_overall_preflight_status(preflight_values, settings)
        )
        if preflight_reason and failure_reason:
            preflight_reason = f"{preflight_reason} {failure_reason}"
        preflight_message = (
            "All enabled mandatory preflight validations passed."
            if preflight_status == "PASSED"
            else "One or more enabled mandatory preflight validations failed."
        )
        enabled_checks = sorted(
            key for key, enabled in settings.items() if enabled
        )
        return IndependentTableSmokeResult(
            layer_name=table.target_layer,
            configuration_file_name=file_name,
            table_configuration_id=table.id,
            source_connection_reference=table.source_system,
            target_connection_reference=table.target_system,
            source_connection_type=source.connection_type,
            target_connection_type=target.connection_type,
            source_server_name=source.masked_server_name,
            target_server_name=target.masked_server_name,
            source_database_name=source.database_name,
            target_database_name=target.database_name,
            source_schema_name=source.schema_name,
            target_schema_name=target.schema_name,
            source_table_name=source.table_name,
            target_table_name=target.table_name,
            source_configured_key_columns=source.configured_key_columns,
            target_configured_key_columns=target.configured_key_columns,
            source_configured_columns=source.configured_columns,
            target_configured_columns=target.configured_columns,
            enabled_checks=enabled_checks,
            **preflight_values,
            source_row_count=source.source_row_count,
            target_row_count=target.target_row_count,
            source_row_count_status=data_status(
                "row_count", source_valid, source.row_count_validation_status
            ),
            target_row_count_status=data_status(
                "row_count", target_valid, target.row_count_validation_status
            ),
            row_count_comparison_status=row_comparison,
            source_key_validation_status=data_status(
                "key_validation", source_valid, source.key_validation_status
            ),
            target_key_validation_status=data_status(
                "key_validation", target_valid, target.key_validation_status
            ),
            key_comparison_status=key_comparison,
            source_null_validation_status=data_status(
                "null_validation", source_valid, source.null_validation_status
            ),
            target_null_validation_status=data_status(
                "null_validation", target_valid, target.null_validation_status
            ),
            null_comparison_status=null_comparison,
            source_duplicate_validation_status=data_status(
                "duplicate_check", source_valid, source.duplicate_validation_status
            ),
            target_duplicate_validation_status=data_status(
                "duplicate_check", target_valid, target.duplicate_validation_status
            ),
            duplicate_comparison_status=duplicate_comparison,
            source_aggregate_validation_status=data_status(
                "aggregate_validation", source_valid, source.aggregate_validation_status
            ),
            target_aggregate_validation_status=data_status(
                "aggregate_validation", target_valid, target.aggregate_validation_status
            ),
            aggregate_comparison_status=aggregate_comparison,
            source_validation_details=source.validation_details,
            target_validation_details=target.validation_details,
            comparison_validation_details=comparison.get("details", {}),
            overall_preflight_validation_status=preflight_status,
            failed_preflight_validation_checks=failed_preflight_checks,
            preflight_failure_reason=preflight_reason,
            preflight_message=preflight_message,
            overall_validation_status=overall,
            validation_status=validation_status,
            message=message,
            failure_reason=failure_reason,
            validation_start_time=source.validation_start_time,
            validation_completion_time=source.validation_completion_time,
            validation_duration_seconds=source.validation_duration_seconds,
        )

    def _validate(self, spec: ConnectionSpec) -> PreflightResult:
        started = perf_counter()
        raw = spec.raw
        result = PreflightResult(
            configuration_file_name=spec.config_file,
            connection_name=str(raw.get("name", spec.reference)),
            connection_reference=spec.reference,
            connection_type=str(raw.get("type", "")),
            classification=spec.classification,
            authentication_type=None,
            required=spec.required,
        )
        connector = None
        masking_filter = None
        filtered_loggers = []
        try:
            if not raw.get("enabled", True):
                result.environment_variable_resolution_status = "NOT_REQUIRED"
                result.validation_status = "SKIPPED"
                result.connection_status = "SKIPPED"
                result.message = "Connection is disabled by configuration"
                return result

            referenced_variables = set(_ENV_PATTERN.findall(json.dumps(raw)))
            missing = sorted({
                name for name in referenced_variables
                if os.getenv(name) is None
                or re.fullmatch(r"\s*<[^<>]+>\s*", os.getenv(name, ""))
            })
            if missing:
                result.environment_variable_resolution_status = "FAILED"
                result.failure_stage = "ENVIRONMENT_RESOLUTION"
                result.failure_reason = f"Missing required environment variable(s): {', '.join(missing)}"
                result.exception_category = "MISSING_ENVIRONMENT_VARIABLE"
                result.message = result.failure_reason
                return result
            result.environment_variable_resolution_status = "RESOLVED"

            resolved = normalize_connection_config(_expand_env_vars(raw))
            result.connection_type = normalize_connection_type(resolved.get("type"))
            result.authentication_type = resolved.get("authentication_type")
            masking_filter = MaskingFilter(collect_sensitive_values(resolved))
            filtered_loggers = [
                logging.getLogger(),
                logging.getLogger("core.connection_manager"),
                logger,
            ]
            for filtered_logger in filtered_loggers:
                filtered_logger.addFilter(masking_filter)
            config_class = TargetConfig if spec.classification in {"TARGET", "BOTH"} else SourceConfig
            allowed = {item.name for item in fields(config_class)}
            values = {key: value for key, value in resolved.items() if key in allowed and key != "name"}
            if not values.get("type"):
                raise ValueError("Missing connection type")
            config = config_class(name=str(resolved.get("name", spec.reference)), **values)
            # Established once per unique connection identity and kept open for
            # the whole run, so repeated references reuse the same connection.
            connector = self._connector_for(
                config, role=spec.classification.casefold()
            )
            result.connector_class_used = connector.__class__.__name__
            result.connection_attempt_status = "ATTEMPTED"
            result.executed_validation_checks.append("connector.test_connectivity")
            connected = connector.test_connectivity()
            if not connected:
                connector_error = getattr(connector, "last_error", None)
                if connector_error is not None:
                    raise ConnectionError(str(connector_error)) from connector_error
                raise ConnectionError("Connector connectivity check returned an unsuccessful result")
            result.connection_status = "CONNECTED"
            result.validation_status = "PASSED"
            result.message = "Connection validation completed successfully"
        except Exception as exc:
            result.connection_status = "FAILED"
            result.validation_status = "FAILED"
            result.failure_stage = result.failure_stage or (
                "CONNECTOR_BUILD" if connector is None else "CONNECTIVITY_TEST"
            )
            result.exception_category = result.exception_category or _exception_category(exc)
            result.failure_reason = mask_text(exc)
            result.message = f"Connection validation failed during {result.failure_stage.lower()}"
            logger.error("%s (%s)", result.message, result.exception_category)
        finally:
            if connector is not None:
                # The connection stays in the run-scoped cache; it is disposed
                # once, after the complete preflight run finishes.
                result.executed_validation_checks.append("connector.cached_for_reuse")
            if masking_filter is not None:
                for filtered_logger in filtered_loggers:
                    filtered_logger.removeFilter(masking_filter)
            result.validation_duration_seconds = round(perf_counter() - started, 6)
        return result


class PreflightReportWriter:
    """Write the preflight reports into the resolved `preflight` run folder.

    The writer owns no path of its own. It asks the shared
    :class:`~reporters.report_paths.ReportPathResolver` for the preflight
    phase's run folder, so the preflight reports sit in the same standard
    structure as every validation phase and never in a Smoke folder.
    """

    def __init__(self, execution_config: str, resolver: Optional[Any] = None):
        # `report_output` and `reporting` may live in their own documents, so
        # the complete execution document is resolved rather than just read.
        self.master = load_execution_document(Path(execution_config))
        self.resolver = resolver if resolver is not None else self._resolver()

    def _resolver(self):
        from reporters.report_paths import ReportPathResolver

        return ReportPathResolver.from_execution_document(self.master)

    def generate_all(self, report: PreflightReport) -> Dict[str, Path]:
        """Write, verify and package the preflight reports of this run."""
        from reporters.phase_package_reporter import PhasePackageReporter
        from reporters.preflight_package_reporter import (
            SeparateValidationPackageWriter,
        )
        from reporters.report_paths import (
            PREFLIGHT_PHASE,
            assert_writers_share_run_root,
        )

        report_writer = SeparateValidationPackageWriter(
            self.master.get("reporting") or {}
        )
        packager = PhasePackageReporter()
        paths = self.resolver.resolve(PREFLIGHT_PHASE, report.validation_run_id)
        paths.log_resolution()
        assert_writers_share_run_root(paths, (report_writer, packager))
        generated: Dict[str, Path] = dict(
            report_writer.generate(report.to_dict(), paths)
        )
        packaged = packager.package(paths)
        if packaged is not None:
            generated["zip"] = packaged
        return generated

