"""
fabric_migration_validator/core/config_loader.py
─────────────────────────────────────────────────
Loads and validates modular configuration from the config/ directory.
"""

from __future__ import annotations
import os
import re
import yaml
import glob
import copy
from decimal import Decimal, InvalidOperation
from dotenv import load_dotenv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from core.execution_engine import (
    AdaptiveBatchingConfig,
    BatchProcessingConfig,
    KeyValidationExecutionConfig,
    BatchParallelismConfig,
    BatchStrategyConfig,
    CheckpointConfig,
    CohortProcessingConfig,
    ConnectionPoolConfig,
    DatabaseProtectionConfig,
    DiagnosticsTrackingConfig,
    ENGINE_UNAVAILABLE_ACTIONS,
    EngineSelectionConfig,
    ExecutionConfig,
    EngineType,
    HashSpillConfig,
    IncrementalExecutionConfig,
    LargeTableConcurrencyConfig,
    LargeTableConfig,
    PandasStrategyConfig,
    ParallelExecutionConfig,
    PerformanceMonitoringConfig,
    PredicateOrConfig,
    PushdownStrategyConfig,
    RetryConfig,
    ScanOptimizationConfig,
    SELECTABLE_ENGINE_NAMES,
    SparkAvailabilityCheckConfig,
    SparkEngineSelectionConfig,
    SparkStrategyConfig,
    StrategySelectionConfig,
    TableParallelismConfig,
    TemporaryStorageConfig,
    TimingConfig,
    ValidationParallelismConfig,
    ValuesJoinConfig,
    SourceTargetParallelismConfig,
)
from core.sql_dialect import SqlDialectRegistry
from core.strategy import BatchStrategy
from core.validation_scope import (
    SCOPE_MODES,
    UNRESOLVED_ACTIONS,
    ScopePolicy,
    ValidationScopeError,
)


VALIDATION_PROFILE_ORDER = (
    "smoke_test",
    "historical_backfill",
    "incremental",
    "full_load",
)


class UniqueKeySafeLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects duplicate mapping keys."""


def _construct_unique_mapping(loader, node, deep=False):
    loader.flatten_mapping(node)
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError as exc:
            raise ValueError(
                f"YAML mapping key at line {key_node.start_mark.line + 1} is not hashable"
            ) from exc
        if duplicate:
            raise ValueError(
                f"Duplicate YAML key '{key}' at line {key_node.start_mark.line + 1}"
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def load_yaml_document(path: Path) -> Dict[str, Any]:
    """Load one configuration document with strict mapping validation."""
    try:
        with Path(path).open(encoding="utf-8") as stream:
            document = yaml.load(stream, Loader=UniqueKeySafeLoader) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in configuration file '{Path(path).name}'") from exc
    except ValueError as exc:
        raise ValueError(
            f"Invalid YAML in configuration file '{Path(path).name}': {exc}"
        ) from exc
    if not isinstance(document, dict):
        raise ValueError(
            f"Configuration file '{Path(path).name}' must contain a YAML mapping"
        )
    return document


def merge_configuration_documents(paths: Iterable[Path]) -> Dict[str, Any]:
    """Merge several configuration documents into one, in the given order.

    The validation configuration is split across one document per
    responsibility (schema, data, data values, edge cases, audit, advanced,
    incremental strategy, comparison settings, profiles, thresholds). At
    startup :meth:`ConfigLoader.load` globs them through
    ``config_paths.validations`` and deep-merges them into the same single
    configuration the framework has always consumed.

    This helper is that same merge, for callers that need the merged document
    without loading the whole configuration. It delegates to
    :meth:`ConfigLoader._deep_merge` so there is exactly one merge rule.
    """
    loader = ConfigLoader.__new__(ConfigLoader)
    merged: Dict[str, Any] = {}
    for path in paths:
        loader._deep_merge(merged, load_yaml_document(Path(path)))
    return merged


#: Key in the master execution document that names its split sections.
EXECUTION_INCLUDE_KEY = "config_files"


def execution_include_paths(path: Path) -> Dict[str, Path]:
    """The file named by each ``config_files`` entry, resolved to a real path."""
    master_path = Path(path)
    includes = load_yaml_document(master_path).get(EXECUTION_INCLUDE_KEY) or {}
    if not isinstance(includes, dict):
        raise ValueError(
            f"{EXECUTION_INCLUDE_KEY} must be a YAML mapping of name to file path"
        )
    project_root = ConfigLoader._project_root(master_path)
    resolved: Dict[str, Path] = {}
    for name, relative in includes.items():
        included = Path(relative)
        if not included.is_absolute():
            included = project_root / included
        if not included.is_file():
            raise ValueError(
                f"{EXECUTION_INCLUDE_KEY}.{name} does not exist: {relative}"
            )
        resolved[str(name)] = included
    return resolved


def execution_section_owners(path: Path) -> Dict[str, Path]:
    """Map each execution section to the document that declares it.

    Callers that need to change one setting can find its single home without
    knowing how the execution configuration happens to be split.
    """
    master_path = Path(path)
    owners: Dict[str, Path] = {
        key: master_path
        for key in load_yaml_document(master_path)
        if key != EXECUTION_INCLUDE_KEY
    }
    for included in execution_include_paths(master_path).values():
        for key in load_yaml_document(included):
            owners[key] = included
    return owners


def load_execution_document(path: Path) -> Dict[str, Any]:
    """The master execution document, with its ``config_files`` sections merged.

    The execution configuration is one document per responsibility. The master
    document keeps the run's identity (project, source, target, categories) and
    names the rest under ``config_files``::

        config_files:
          execution_control: config/execution/execution_control.yml
          config_paths:      config/execution/config_paths.yml
          report_output:     config/reporting/report_output.yml
          ...

    Those sections are merged in before anything reads ``config_paths``, so
    discovery, the connection ``.env`` and preflight all see the same complete
    document they saw when it was one file.

    A master document without ``config_files`` is returned unchanged, so a
    single-file ``execution.yml`` keeps working through this same code path.

    Raises:
        ValueError: if a named file is missing, or if a section is declared in
            more than one document. One setting must have exactly one home.
    """
    master_path = Path(path)
    document = load_yaml_document(master_path)
    if not document.get(EXECUTION_INCLUDE_KEY):
        return document
    loader = ConfigLoader.__new__(ConfigLoader)
    merged = dict(document)
    for name, included in execution_include_paths(master_path).items():
        section = load_yaml_document(included)
        duplicated = sorted(
            set(section) & (set(merged) - {EXECUTION_INCLUDE_KEY})
        )
        if duplicated:
            raise ValueError(
                f"{', '.join(duplicated)} is declared in more than one execution "
                f"configuration document; '{included.name}' must be its only home"
            )
        loader._deep_merge(merged, section)
    return merged


def load_validation_documents(directory: Path) -> Dict[str, Any]:
    """The merged validation configuration held in one directory.

    Documents are merged in sorted file-name order, matching the order
    :meth:`ConfigLoader.load` glob-discovers them, so this returns exactly the
    configuration the framework runs with.
    """
    return merge_configuration_documents(
        sorted(Path(directory).glob("*.y*ml"))
    )


# ── Data Models ──────────────────────────────────────────────

@dataclass
class SourceConfig:
    name: str
    type: str
    enabled: bool = True
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    jdbc_url: Optional[str] = None
    jdbc_driver: Optional[str] = None
    odbc_driver: Optional[str] = None
    trusted_connection: Optional[str] = None
    # Deprecated connection-local compatibility input. Runtime behavior is
    # controlled by performance.connection_management.connection_timeout_seconds.
    connection_timeout_seconds: Optional[int] = None
    authentication_type: Optional[str] = None
    workspace_name: Optional[str] = None
    lakehouse_name: Optional[str] = None
    sql_endpoint: Optional[str] = None
    database_required_for_validation: bool = False
    # Optional per-connection capability overrides. Lets a deployment describe a
    # new Source/Target platform without changing the validation engine.
    capabilities: Optional[Dict[str, Any]] = None
    # Optional SQL dialect selection: a profile name from the dialect
    # configuration, or an inline mapping overriding that profile's fields.
    sql_dialect: Optional[Any] = None

@dataclass
class TargetConfig(SourceConfig):
    pass

@dataclass
class ValidationProfile:
    name: str
    row_count: bool = True
    aggregate_validation: bool = True
    null_validation: bool = True
    key_validation: bool = True
    referential_integrity: bool = False
    scd_validation: bool = False
    business_rules: bool = False
    pii_scan: bool = False
    duplicate_check: bool = False
    data_value_validation: bool = False
    edge_case_validation: bool = False
    audit_validation: bool = False
    record_level_comparison: bool = False
    record_comparison: Dict[str, Any] = field(default_factory=dict)
    schema_validation: Dict[str, bool] = field(default_factory=dict)
    data_validation: Dict[str, bool] = field(default_factory=dict)
    advanced_validation: Dict[str, bool] = field(default_factory=dict)
    #: Resolved data window this profile is allowed to read.
    validation_scope: Optional[ScopePolicy] = None

@dataclass
class RunDefaults:
    active_checks: Optional[List[str]] = None
    always_run: List[str] = field(default_factory=list)
    always_skip: List[str] = field(default_factory=list)
    report_overrides: bool = True


@dataclass
class PreflightSchemaValidationConfig:
    """Which profile's schema toggles preflight applies."""

    profile_source: str = "selected_phase"
    fixed_profile: str = "smoke_test"


@dataclass
class PreflightDataCheckConfig:
    """Explicit opt-in for data checks during preflight."""

    enabled: bool = False
    checks: List[str] = field(default_factory=list)
    apply_validation_scope: bool = True


@dataclass
class PreflightConfig:
    """Preflight behavior, entirely from execution configuration."""

    run_id_prefix: str = "PREFLIGHT"
    report_name: str = "preflight_validation"
    timestamp_format: str = "%Y%m%d_%H%M%S"
    schema_validation: PreflightSchemaValidationConfig = field(
        default_factory=PreflightSchemaValidationConfig
    )
    data_checks: PreflightDataCheckConfig = field(
        default_factory=PreflightDataCheckConfig
    )



@dataclass
class KeyMapping:
    source_column: str
    target_column: str

@dataclass
class TableMapping:
    id: str
    source_system: str
    source_schema: str
    source_table: str
    target_layer: str
    target_schema: str
    target_table: str
    migration_phase: str
    validation_phases: List[str] = field(default_factory=list)
    target_system: Optional[str] = None
    source_server: Optional[str] = None
    # Optional alternate source host used only to verify otherwise-unresolved
    # target-only INSERT candidates during target-driven incremental checks.
    server_2: Optional[str] = None
    target_server: Optional[str] = None
    allow_zero_rows: bool = False
    allow_duplicates: bool = False
    primary_keys: List[str] = field(default_factory=list)
    numeric_columns: List[Any] = field(default_factory=list)
    key_mappings: List[KeyMapping] = field(default_factory=list)
    compare_column_mappings: List[Dict] = field(default_factory=list)
    null_columns: List[Any] = field(default_factory=list)
    mandatory_columns: List[Any] = field(default_factory=list)
    duplicate_columns: List[Any] = field(default_factory=list)
    near_duplicate_columns: List[Any] = field(default_factory=list)
    post_transform_duplicate_columns: List[Any] = field(default_factory=list)
    date_filter_column: Optional[str] = None
    date_range: Optional[Dict[str, str]] = None
    enabled: bool = True
    tags: List[str] = field(default_factory=list)
    override_checks: Optional[List[str]] = None
    skip_checks: List[str] = field(default_factory=list)
    referential_checks: List[Dict] = field(default_factory=list)
    business_rules: List[Dict] = field(default_factory=list)
    scd_config: Optional[Dict] = None
    incremental_column: Optional[str] = None
    source_watermark_column: Optional[str] = None
    target_watermark_column: Optional[str] = None
    incremental_enabled: Optional[bool] = None
    incremental_strategy: Optional[str] = None
    incremental_watermark_mode: Optional[str] = None
    incremental_pk_enabled: Optional[bool] = None
    incremental_pk_compatible: Optional[bool] = None
    incremental_pk_monotonic: Optional[bool] = None
    incremental_fallback: Optional[str] = None
    # Optional per-table bootstrap policy overrides. This is intentionally
    # narrow metadata; absent entries retain the global strategy policy.
    incremental_bootstrap: Optional[Dict[str, str]] = None
    incremental_value: Optional[Any] = None
    # Watermark window applied identically to every incremental-capable check:
    # previous_watermark < incremental_column <= current_watermark.
    incremental_previous_value: Optional[Any] = None
    incremental_current_value: Optional[Any] = None
    incremental_lower_bound_inclusive: bool = False
    incremental_upper_bound_inclusive: bool = True
    # Optional runtime metadata used only to size execution decisions.
    estimated_row_count: Optional[int] = None
    partition_column: Optional[str] = None
    source_database: Optional[str] = None
    target_database: Optional[str] = None
    # Optional runtime watermark metadata, used only when the profile's
    # watermark_sources include runtime_metadata.
    incremental_metadata: Optional[Dict[str, Any]] = None
    # Table-specific change-data-capture metadata (`incremental.cdc`): the
    # operation column, the configured operation values and optional
    # sequence metadata. CDC exists for a table only when this is configured.
    incremental_cdc: Optional[Dict[str, Any]] = None
    # Table-specific target audit/load metadata (`incremental.target_audit`):
    # the target load-timestamp column and optional configured boundaries.
    incremental_target_audit: Optional[Dict[str, Any]] = None
    # Window resolved for the phase being validated. Populated by the
    # orchestrator, carried here so every validator and the optional preflight
    # data checks read the table through exactly one window.
    validation_scope: Optional[Dict[str, Any]] = None

@dataclass
class Thresholds:
    row_count_tolerance_pct: float = 0.0
    aggregate_tolerance_pct: float = 0.001

@dataclass
class ReportingConfig:
    formats: List[str] = field(default_factory=lambda: ["html", "excel", "console", "json"])
    output_dir: str = "./reports"
    #: The whole `reporting` block, verbatim. The phase report tree and the
    #: consolidated workbook structure are read from here, so no phase name,
    #: folder, format or column order is ever spelled out in Python.
    settings: Dict[str, Any] = field(default_factory=dict)

#: How selected phases combine with each table's configured migration_phase.
PHASE_EXECUTION_MODES = ("per_table_configured", "all_selected_phases")


@dataclass
class ExecutionControl:
    full_run: bool = True
    environment: Optional[str] = None
    table_ids: List[str] = field(default_factory=list)
    table_names: List[str] = field(default_factory=list)
    layers: List[str] = field(default_factory=list)
    servers: List[str] = field(default_factory=list)
    databases: List[str] = field(default_factory=list)
    schemas: List[str] = field(default_factory=list)
    phases: List[str] = field(default_factory=list)
    phase_execution_mode: str = "per_table_configured"

@dataclass
class MigrationConfig:
    project_name: str
    run_id_prefix: str
    sources: Dict[str, SourceConfig]
    target: TargetConfig
    validation_profiles: Dict[str, ValidationProfile]
    tables: List[TableMapping]
    thresholds: Thresholds
    reporting: ReportingConfig
    validations: Dict[str, Any] = field(default_factory=dict)
    comparison_settings: Dict[str, Any] = field(default_factory=dict)
    performance: ExecutionConfig = field(default_factory=ExecutionConfig)
    execution_control: ExecutionControl = field(default_factory=ExecutionControl)
    run_defaults: RunDefaults = field(default_factory=RunDefaults)
    source_ref: Optional[str] = None
    target_ref: Optional[str] = None
    version: str = "1.0"
    framework: Dict[str, Any] = field(default_factory=dict)
    preflight: PreflightConfig = field(default_factory=PreflightConfig)
    sql_dialects: Optional[SqlDialectRegistry] = None

    @property
    def execution(self) -> ExecutionConfig:
        """Compatibility alias; performance.yaml is the sole backing source."""
        return self.performance

    @property
    def dialect_registry(self) -> SqlDialectRegistry:
        """The run's dialect profiles, falling back to the shipped set."""
        return self.sql_dialects or SqlDialectRegistry.builtin()

# ── Environment Variable Expansion ───────────────────────────

def _expand_env_vars(value: Any) -> Any:
    if isinstance(value, str):
        pattern = re.compile(r"\$\{(\w+)\}")
        def replacer(match):
            var = match.group(1)
            resolved = os.environ.get(var)
            if resolved is None:
                return f"${{{var}}}" # Keep placeholder if not found
            return resolved
        return pattern.sub(replacer, value)
    elif isinstance(value, dict):
        return {k: _expand_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_expand_env_vars(i) for i in value]
    return value


def normalize_connection_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten the supported `params` shape and normalize public aliases."""
    normalized = dict(config)
    params = normalized.pop("params", {}) or {}
    if not isinstance(params, dict):
        raise ValueError("Connection 'params' must be a YAML mapping")
    normalized.update(params)
    aliases = {
        "server": "host",
        "driver": "odbc_driver",
        "authentication": "authentication_type",
        "workspace_id": "workspace_name",
        "lakehouse_id": "lakehouse_name",
    }
    for source_key, target_key in aliases.items():
        if source_key in normalized and target_key not in normalized:
            normalized[target_key] = normalized.pop(source_key)
    if normalized.get("sql_endpoint") and not normalized.get("host"):
        normalized["host"] = normalized["sql_endpoint"]
    return normalized


def normalize_connection_document(document: Dict[str, Any]) -> Dict[str, Any]:
    """Support role-aware `connections:` files and legacy sources/targets files."""
    normalized = dict(document)
    if "connections" not in normalized:
        return normalized
    role = str(normalized.pop("connection_role", "") or "").strip().lower()
    connections = normalized.pop("connections")
    if role not in {"source", "target"}:
        raise ValueError(
            "A connection YAML using 'connections' must define "
            "connection_role as 'source' or 'target'"
        )
    section = "sources" if role == "source" else "targets"
    normalized[section] = connections
    return normalized


_PROFILE_SECTIONS = {
    "data_validation": (
        "row_count",
        "key_validation",
        "null_validation",
        "duplicate_check",
        "aggregate_validation",
    ),
    "advanced_validation": (
        "referential_integrity",
        "scd_validation",
        "business_rules",
        "pii_scan",
    ),
}
_FORBIDDEN_EXECUTION_KEY_FIELDS = {
    "key_columns",
    "key_mappings",
    "primary_key",
    "primary_keys",
    "primary_key_based_comparison",
}
_FORBIDDEN_TABLE_KEY_FIELDS = {
    "key_columns",
    "primary_key",
    "primary_keys",
    "primary_key_based_comparison",
}
_EXECUTION_ONLY_FIELDS = {
    "batch_size",
    "max_rows_in_memory",
    "max_workers",
    "executor_type",
    "fail_fast",
    "retry_failed_batch",
    "max_retries",
    "threshold_rows",
    "force_batch_processing",
    "allow_full_dataframe_load",
    "pool_size",
    "max_overflow",
    "pool_pre_ping",
    "pool_recycle_seconds",
    "timeout_seconds",
    "max_attempts",
    "delay_seconds",
    "resume_failed_run",
    "full_scan",
    "partition_size",
    "reuse_connections",
}


def _find_execution_fields(value: Any, path: str) -> List[str]:
    found = []
    if isinstance(value, dict):
        for key, item in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            if key in _EXECUTION_ONLY_FIELDS:
                found.append(child_path)
            found.extend(_find_execution_fields(item, child_path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            found.extend(_find_execution_fields(item, f"{path}[{index}]"))
    return found


def _parse_strategy_selection(
    raw: Any,
    batch_config: BatchProcessingConfig,
    boolean,
    nonnegative_int,
) -> StrategySelectionConfig:
    """Build strategy configuration from the centralized performance document."""
    if not isinstance(raw, dict):
        raise ValueError("execution.strategies must be a YAML mapping")

    supported_sections = {"pushdown", "pandas", "batch", "spark"}
    unknown_sections = sorted(set(raw) - supported_sections - {"engine"})
    if unknown_sections:
        raise ValueError(
            "Unsupported execution.strategies section(s): "
            + ", ".join(unknown_sections)
        )

    def section(name: str) -> Dict[str, Any]:
        if name not in raw:
            raise ValueError(f"performance.execution.strategies.{name} is required")
        value = raw[name]
        if not isinstance(value, dict):
            raise ValueError(
                f"performance.execution.strategies.{name} must be a YAML mapping"
            )
        return value

    def required(section_raw: Dict[str, Any], field_name: str, path: str) -> Any:
        if field_name not in section_raw:
            raise ValueError(f"{path}.{field_name} is required")
        return section_raw[field_name]

    engine = required(raw, "engine", "performance.execution.strategies")
    if not isinstance(engine, str) or not engine.strip():
        raise ValueError("execution.strategies.engine must be a non-empty string")

    pushdown_raw = section("pushdown")
    pandas_raw = section("pandas")
    batch_raw = section("batch")
    spark_raw = section("spark")

    batch_strategy = required(
        batch_raw, "strategy", "performance.execution.strategies.batch"
    )
    supported_batch_strategies = tuple(item.value for item in BatchStrategy)
    if batch_strategy not in supported_batch_strategies:
        raise ValueError(
            "execution.strategies.batch.strategy must be one of: "
            + ", ".join(supported_batch_strategies)
        )

    return StrategySelectionConfig(
        engine=engine.strip(),
        pushdown=PushdownStrategyConfig(
            enabled=boolean(
                required(pushdown_raw, "enabled", "performance.execution.strategies.pushdown"),
                "execution.strategies.pushdown.enabled",
            ),
            combine_statistics=boolean(
                required(pushdown_raw, "combine_statistics", "performance.execution.strategies.pushdown"),
                "execution.strategies.pushdown.combine_statistics",
            ),
            max_statistics_per_query=nonnegative_int(
                required(pushdown_raw, "max_statistics_per_query", "performance.execution.strategies.pushdown"),
                "execution.strategies.pushdown.max_statistics_per_query",
            ),
        ),
        pandas=PandasStrategyConfig(
            enabled=boolean(
                required(pandas_raw, "enabled", "performance.execution.strategies.pandas"),
                "execution.strategies.pandas.enabled",
            ),
            max_rows_in_memory=nonnegative_int(
                required(pandas_raw, "max_rows_in_memory", "performance.execution.strategies.pandas"),
                "execution.strategies.pandas.max_rows_in_memory",
            ),
        ),
        batch=BatchStrategyConfig(
            enabled=boolean(
                required(batch_raw, "enabled", "performance.execution.strategies.batch"),
                "execution.strategies.batch.enabled",
            ),
            strategy=batch_strategy,
            batch_size=nonnegative_int(
                batch_config.batch_size,
                "execution.strategies.batch.batch_size",
            ),
        ),
        spark=SparkStrategyConfig(
            enabled=boolean(
                required(spark_raw, "enabled", "performance.execution.strategies.spark"),
                "execution.strategies.spark.enabled",
            ),
            threshold_rows=nonnegative_int(
                required(spark_raw, "threshold_rows", "performance.execution.strategies.spark"),
                "execution.strategies.spark.threshold_rows",
            ),
            max_pandas_rows=nonnegative_int(
                required(spark_raw, "max_pandas_rows", "performance.execution.strategies.spark"),
                "execution.strategies.spark.max_pandas_rows",
            ),
        ),
    )


def _ordered_profile_names(names) -> List[str]:
    unique_names = list(dict.fromkeys(names))
    required = [name for name in VALIDATION_PROFILE_ORDER if name in unique_names]
    custom = [name for name in unique_names if name not in VALIDATION_PROFILE_ORDER]
    return required + custom


def _toggle_value(value: Any, path: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, dict) and isinstance(value.get("enabled"), bool):
        return value["enabled"]
    raise ValueError(f"{path} must be a boolean or contain an 'enabled' boolean")


def _global_profile_defaults(validations: Dict[str, Any]) -> Dict[str, bool]:
    defaults: Dict[str, bool] = {}
    for section_name, field_names in _PROFILE_SECTIONS.items():
        section = validations.get(section_name, {}) or {}
        if not isinstance(section, dict):
            raise ValueError(f"validations.{section_name} must be a YAML mapping")
        for field_name in field_names:
            if field_name in section:
                defaults[field_name] = _toggle_value(
                    section[field_name],
                    f"validations.{section_name}.{field_name}",
                )
    return defaults


def _global_scope_policy(validations: Dict[str, Any]) -> ScopePolicy:
    """The default data window every profile inherits."""
    raw = validations.get("validation_scope")
    if raw is None:
        # No global block: the framework's own baseline is an unrestricted
        # window with an explicit error when a narrower one is requested but
        # cannot be resolved. Both values are still surfaced in the report.
        return ScopePolicy(
            mode="full",
            on_unresolved="error",
            watermark_sources=("table_yaml", "runtime_metadata"),
            require_both_bounds=False,
            status_on_warning="WARNING",
            status_on_error="CONFIG_ERROR",
            status_on_skip="SKIP",
        )
    return ScopePolicy.from_config(raw, "validations.validation_scope")


def _normalize_profile_config(
    name: str,
    config: Dict[str, Any],
    validations: Dict[str, Any],
) -> Dict[str, Any]:
    if not isinstance(config, dict):
        raise ValueError(f"validation_profiles.{name} must be a YAML mapping")

    normalized = dict(config)
    normalized["validation_scope"] = ScopePolicy.from_config(
        normalized.pop("validation_scope", None),
        f"validation_profiles.{name}.validation_scope",
        defaults=_global_scope_policy(validations),
    )
    for field_name, default in _global_profile_defaults(validations).items():
        normalized.setdefault(field_name, default)
    for section_name, field_names in _PROFILE_SECTIONS.items():
        for field_name in field_names:
            if field_name in normalized:
                normalized[field_name] = _toggle_value(
                    normalized[field_name],
                    f"validation_profiles.{name}.{field_name}",
                )

    schema_config = normalized.pop("schema_validation", {}) or {}
    if not isinstance(schema_config, dict):
        raise ValueError(
            f"validation_profiles.{name}.schema_validation must be a YAML mapping"
        )
    global_schema = validations.get("schema_validation", {}) or {}
    if not isinstance(global_schema, dict):
        raise ValueError("validations.schema_validation must be a YAML mapping")
    normalized_schema = {
        check_name: _toggle_value(
            value,
            f"validations.schema_validation.{check_name}",
        )
        for check_name, value in global_schema.items()
    }
    normalized_schema.update({
        check_name: _toggle_value(
            value,
            f"validation_profiles.{name}.schema_validation.{check_name}",
        )
        for check_name, value in schema_config.items()
    })

    normalized_sections: Dict[str, Dict[str, bool]] = {}
    for section_name, allowed_fields in _PROFILE_SECTIONS.items():
        section = normalized.pop(section_name, {}) or {}
        if not isinstance(section, dict):
            raise ValueError(
                f"validation_profiles.{name}.{section_name} must be a YAML mapping"
            )
        normalized_section = {}
        for field_name, value in section.items():
            if field_name in config:
                raise ValueError(
                    f"validation_profiles.{name}.{field_name} is configured both "
                    f"at profile level and under {section_name}"
                )
            enabled = _toggle_value(
                value,
                f"validation_profiles.{name}.{section_name}.{field_name}",
            )
            if field_name in allowed_fields:
                normalized[field_name] = enabled
            normalized_section[field_name] = enabled
        for field_name in allowed_fields:
            if field_name in normalized:
                normalized_section.setdefault(field_name, bool(normalized[field_name]))
        normalized_sections[section_name] = normalized_section

    canonical_comparison = normalized.pop("record_level_comparison", None)
    legacy_comparison = normalized.pop("record_comparison", None)
    if canonical_comparison is not None and legacy_comparison is not None:
        raise ValueError(
            f"validation_profiles.{name} must not define both "
            "record_level_comparison and record_comparison"
        )
    profile_comparison = (
        canonical_comparison
        if canonical_comparison is not None
        else legacy_comparison
    )
    if isinstance(profile_comparison, bool):
        profile_comparison = {"enabled": profile_comparison}
    if profile_comparison is not None and not isinstance(profile_comparison, dict):
        raise ValueError(
            f"validation_profiles.{name}.record_level_comparison must be a YAML mapping"
        )

    global_comparison = validations.get("record_level_comparison", {}) or {}
    if isinstance(global_comparison, bool):
        global_comparison = {"enabled": global_comparison}
    if not isinstance(global_comparison, dict):
        raise ValueError("validations.record_level_comparison must be a YAML mapping")
    comparison = {**global_comparison, **(profile_comparison or {})}
    if (
        profile_comparison is not None
        and "enabled" not in profile_comparison
        and "enabled" not in global_comparison
    ):
        comparison["enabled"] = True
    # The global block defines how record comparison behaves; the selected
    # profile alone decides whether it executes.  An omitted profile section
    # must not inherit global ``enabled: true`` and silently add a validator.
    comparison_enabled = (
        False
        if profile_comparison is None
        else profile_comparison.get("enabled", comparison.get("enabled", False))
    )
    if not isinstance(comparison_enabled, bool):
        raise ValueError(
            f"validation_profiles.{name}.record_level_comparison.enabled must be boolean"
        )
    comparison["enabled"] = comparison_enabled
    # Execution/report metadata is derived from the profile being normalized;
    # callers cannot accidentally label results as another profile.
    comparison["validation_profile"] = name

    normalized["record_comparison"] = comparison
    normalized["record_level_comparison"] = bool(comparison.get("enabled", False))

    profile_data_values = normalized.pop("data_value_validation", None)
    global_data_values = validations.get("data_value_validation", {}) or {}
    if isinstance(global_data_values, bool):
        global_data_values = {"enabled": global_data_values}
    if not isinstance(global_data_values, dict):
        raise ValueError("validations.data_value_validation must be a YAML mapping")
    global_data_values_enabled = global_data_values.get("enabled", False)
    if not isinstance(global_data_values_enabled, bool):
        raise ValueError(
            "validations.data_value_validation.enabled must be boolean"
        )
    default_data_values_enabled = (
        False if name == "smoke_test" else global_data_values_enabled
    )
    if profile_data_values is None:
        data_values_enabled = default_data_values_enabled
    elif isinstance(profile_data_values, bool):
        data_values_enabled = profile_data_values
    elif isinstance(profile_data_values, dict):
        unknown_data_value_fields = set(profile_data_values) - {"enabled"}
        if unknown_data_value_fields:
            raise ValueError(
                f"validation_profiles.{name}.data_value_validation contains "
                "unsupported field(s): "
                + ", ".join(sorted(unknown_data_value_fields))
            )
        data_values_enabled = profile_data_values.get(
            "enabled", default_data_values_enabled
        )
    else:
        raise ValueError(
            f"validation_profiles.{name}.data_value_validation must be boolean "
            "or a YAML mapping"
        )
    if not isinstance(data_values_enabled, bool):
        raise ValueError(
            f"validation_profiles.{name}.data_value_validation.enabled must be boolean"
        )
    normalized["data_value_validation"] = data_values_enabled

    profile_edge_cases = normalized.pop("edge_case_validation", None)
    global_edge_cases = validations.get("edge_case_validation", {}) or {}
    if isinstance(global_edge_cases, bool):
        global_edge_cases = {"enabled": global_edge_cases}
    if not isinstance(global_edge_cases, dict):
        raise ValueError("validations.edge_case_validation must be a YAML mapping")
    global_edge_cases_enabled = global_edge_cases.get("enabled", False)
    if not isinstance(global_edge_cases_enabled, bool):
        raise ValueError(
            "validations.edge_case_validation.enabled must be boolean"
        )
    if profile_edge_cases is None:
        edge_cases_enabled = global_edge_cases_enabled
    elif isinstance(profile_edge_cases, bool):
        edge_cases_enabled = profile_edge_cases
    elif isinstance(profile_edge_cases, dict):
        unknown_edge_fields = set(profile_edge_cases) - {"enabled"}
        if unknown_edge_fields:
            raise ValueError(
                f"validation_profiles.{name}.edge_case_validation contains "
                "unsupported field(s): "
                + ", ".join(sorted(unknown_edge_fields))
            )
        edge_cases_enabled = profile_edge_cases.get(
            "enabled", global_edge_cases_enabled
        )
    else:
        raise ValueError(
            f"validation_profiles.{name}.edge_case_validation must be boolean "
            "or a YAML mapping"
        )
    if not isinstance(edge_cases_enabled, bool):
        raise ValueError(
            f"validation_profiles.{name}.edge_case_validation.enabled must be boolean"
        )
    normalized["edge_case_validation"] = edge_cases_enabled

    profile_audit = normalized.pop("audit_validation", None)
    global_audit = validations.get("audit_validation", {}) or {}
    if not isinstance(global_audit, dict):
        raise ValueError("validations.audit_validation must be a YAML mapping")
    global_audit_enabled = global_audit.get("enabled", False)
    if not isinstance(global_audit_enabled, bool):
        raise ValueError("validations.audit_validation.enabled must be boolean")
    default_audit_enabled = False if name == "smoke_test" else global_audit_enabled
    if profile_audit is None:
        audit_enabled = default_audit_enabled
    elif isinstance(profile_audit, bool):
        audit_enabled = profile_audit
    elif isinstance(profile_audit, dict):
        unknown_audit_fields = set(profile_audit) - {"enabled"}
        if unknown_audit_fields:
            raise ValueError(
                f"validation_profiles.{name}.audit_validation contains unsupported "
                "field(s): " + ", ".join(sorted(unknown_audit_fields))
            )
        audit_enabled = profile_audit.get("enabled", default_audit_enabled)
    else:
        raise ValueError(
            f"validation_profiles.{name}.audit_validation must be boolean or a YAML mapping"
        )
    if not isinstance(audit_enabled, bool):
        raise ValueError(
            f"validation_profiles.{name}.audit_validation.enabled must be boolean"
        )
    normalized["audit_validation"] = audit_enabled
    normalized["schema_validation"] = normalized_schema
    normalized["data_validation"] = normalized_sections["data_validation"]
    normalized["advanced_validation"] = normalized_sections["advanced_validation"]
    if name == "preflight":
        # Preflight is an object-readiness profile.  Global validator defaults
        # describe data phases and must never leak into this metadata-only path.
        for field_name in set().union(*_PROFILE_SECTIONS.values()):
            normalized[field_name] = False
        normalized["data_value_validation"] = False
        normalized["edge_case_validation"] = False
        normalized["audit_validation"] = False
        normalized["record_level_comparison"] = False
        normalized["record_comparison"]["enabled"] = False
    return normalized


def _find_forbidden_execution_key_fields(value: Any, path: str) -> List[str]:
    found: List[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            item_path = f"{path}.{key}" if path else str(key)
            if (
                key in _FORBIDDEN_EXECUTION_KEY_FIELDS
                and item_path not in {
                    # Strategy policy blocks named after the primary_key
                    # strategy, not key column configuration.
                    "validations.incremental_strategy.primary_key",
                    "validations.incremental_strategy.bootstrap.primary_key",
                }
            ):
                found.append(item_path)
            found.extend(_find_forbidden_execution_key_fields(item, item_path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            found.extend(_find_forbidden_execution_key_fields(item, f"{path}[{index}]"))
    return found


def _normalize_table_config(table: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(table, dict):
        raise ValueError("Each tables entry must be a YAML mapping")
    normalized = dict(table)
    table_id = normalized.get("id", "<missing>")

    server_2 = normalized.get("server_2")
    if server_2 is not None:
        if not isinstance(server_2, str) or not server_2.strip():
            raise ValueError(
                f"Table '{table_id}' server_2 must be a non-empty string"
            )
        normalized["server_2"] = server_2.strip()

    for role in ("source", "target"):
        endpoint_name = f"{role}_table"
        endpoint = normalized.get(endpoint_name)
        if not isinstance(endpoint, dict):
            continue
        for nested_name, flat_name in (
            ("database", f"{role}_database"),
            ("schema", f"{role}_schema"),
            ("table", endpoint_name),
        ):
            value = endpoint.get(nested_name)
            existing = normalized.get(flat_name)
            if flat_name == endpoint_name:
                existing = None
            if existing is not None and value is not None and existing != value:
                raise ValueError(
                    f"Table '{table_id}' defines conflicting {flat_name} values"
                )
            if value is not None:
                normalized[flat_name] = value
        if not isinstance(normalized.get(endpoint_name), str):
            raise ValueError(
                f"Table '{table_id}' {endpoint_name}.table must be a non-empty string"
            )

    forbidden_table_fields = [
        field_name
        for field_name in _FORBIDDEN_TABLE_KEY_FIELDS
        if field_name in normalized
    ]
    if forbidden_table_fields:
        raise ValueError(
            f"Table '{table_id}' must use key_mappings instead of "
            + ", ".join(sorted(forbidden_table_fields))
        )

    raw_mappings = normalized.get("key_mappings", []) or []
    if not isinstance(raw_mappings, list):
        raise ValueError(f"Table '{table_id}' key_mappings must be a YAML list")
    key_mappings: List[KeyMapping] = []
    seen_pairs = set()
    seen_source_keys = set()
    seen_target_keys = set()
    for index, mapping in enumerate(raw_mappings):
        if not isinstance(mapping, dict):
            raise ValueError(
                f"Table '{table_id}' key_mappings[{index}] must be a YAML mapping"
            )
        source_column = mapping.get("source_column")
        target_column = mapping.get("target_column")
        if not all(
            isinstance(column, str) and column.strip()
            for column in (source_column, target_column)
        ):
            raise ValueError(
                f"Table '{table_id}' key_mappings[{index}] requires non-empty "
                "source_column and target_column"
            )
        pair = (source_column.strip(), target_column.strip())
        if pair in seen_pairs:
            raise ValueError(
                f"Table '{table_id}' contains duplicate key mapping {pair}"
            )
        if pair[0] in seen_source_keys or pair[1] in seen_target_keys:
            raise ValueError(
                f"Table '{table_id}' key_mappings must map each source and target "
                "column only once"
            )
        seen_pairs.add(pair)
        seen_source_keys.add(pair[0])
        seen_target_keys.add(pair[1])
        key_mappings.append(KeyMapping(*pair))
    normalized["key_mappings"] = key_mappings
    # Compatibility view only; key_mappings remains the authoritative source.
    normalized["primary_keys"] = [item.source_column for item in key_mappings]

    compare_mappings = normalized.get("compare_column_mappings", []) or []
    if not isinstance(compare_mappings, list):
        raise ValueError(
            f"Table '{table_id}' compare_column_mappings must be a YAML list"
        )
    seen_compare_pairs = set()
    for index, mapping in enumerate(compare_mappings):
        if not isinstance(mapping, dict):
            raise ValueError(
                f"Table '{table_id}' compare_column_mappings[{index}] must be a YAML mapping"
            )
        source_column = mapping.get("source_column")
        target_column = mapping.get("target_column")
        if not all(
            isinstance(column, str) and column.strip()
            for column in (source_column, target_column)
        ):
            raise ValueError(
                f"Table '{table_id}' compare_column_mappings[{index}] requires "
                "non-empty source_column and target_column"
            )
        pair = (source_column.strip(), target_column.strip())
        if pair in seen_compare_pairs:
            raise ValueError(
                f"Table '{table_id}' contains duplicate compare column mapping {pair}"
            )
        seen_compare_pairs.add(pair)

    _normalize_incremental_scope(normalized, table_id)

    estimated = normalized.get("estimated_row_count")
    if estimated is not None and (
        isinstance(estimated, bool) or not isinstance(estimated, int) or estimated < 0
    ):
        raise ValueError(
            f"Table '{table_id}' estimated_row_count must be a non-negative integer"
        )
    return normalized


def _normalize_incremental_scope(normalized: Dict[str, Any], table_id: str) -> None:
    """Validate the configured incremental watermark window.

    The column and both watermark values always come from table configuration or
    runtime metadata. The framework never infers a column name.
    """
    nested = normalized.pop("incremental", None)
    if nested is not None:
        if not isinstance(nested, dict):
            raise ValueError(
                f"Table '{table_id}' incremental must be a YAML mapping"
            )
        aliases = {
            "enabled": "incremental_enabled",
            "column": "incremental_column",
            "source_watermark_column": "source_watermark_column",
            "target_watermark_column": "target_watermark_column",
            "value": "incremental_value",
            "previous_watermark": "incremental_previous_value",
            "current_watermark": "incremental_current_value",
            "lower_bound_inclusive": "incremental_lower_bound_inclusive",
            "upper_bound_inclusive": "incremental_upper_bound_inclusive",
            "strategy": "incremental_strategy",
            "watermark_mode": "incremental_watermark_mode",
            "pk_enabled": "incremental_pk_enabled",
            "pk_incremental_compatible": "incremental_pk_compatible",
            "pk_monotonic": "incremental_pk_monotonic",
            "fallback": "incremental_fallback",
            "bootstrap": "incremental_bootstrap",
            "cdc": "incremental_cdc",
            "target_audit": "incremental_target_audit",
        }
        unknown = sorted(set(nested) - set(aliases))
        if unknown:
            raise ValueError(
                f"Table '{table_id}' incremental has unsupported field(s): "
                + ", ".join(unknown)
            )
        for nested_name, flat_name in aliases.items():
            if nested_name not in nested:
                continue
            value = nested[nested_name]
            existing = normalized.get(flat_name)
            if existing is not None and existing != value:
                raise ValueError(
                    f"Table '{table_id}' defines conflicting {flat_name} values"
                )
            normalized[flat_name] = value

    for flag in (
        "incremental_enabled",
        "incremental_pk_enabled",
        "incremental_pk_compatible",
        "incremental_pk_monotonic",
        "incremental_lower_bound_inclusive",
        "incremental_upper_bound_inclusive",
    ):
        if flag in normalized and not isinstance(normalized[flag], bool):
            raise ValueError(f"Table '{table_id}' {flag} must be boolean")

    # A watermark column is either absent or a non-empty name. An empty list,
    # an empty string or any other placeholder is an invalid configuration,
    # never "no watermark": treating `[]` as a column name (or as no column)
    # would silently change which strategy runs.
    for column_field in (
        "incremental_column", "source_watermark_column", "target_watermark_column",
    ):
        value = normalized.get(column_field)
        if value is None:
            continue
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"Table '{table_id}' {column_field} must be a non-empty column "
                f"name or omitted; got {value!r}"
            )
        normalized[column_field] = value.strip()

    watermark_fields = (
        "incremental_value",
        "incremental_previous_value",
        "incremental_current_value",
    )
    has_watermark = any(
        normalized.get(field_name) is not None for field_name in watermark_fields
    )
    source_column = normalized.get("source_watermark_column")
    target_column = normalized.get("target_watermark_column")
    legacy_column = normalized.get("incremental_column")
    if legacy_column:
        normalized.setdefault("source_watermark_column", legacy_column)
        normalized.setdefault("target_watermark_column", legacy_column)
    elif source_column and target_column and source_column == target_column:
        normalized["incremental_column"] = source_column
    if has_watermark and not (
        str(normalized.get("source_watermark_column") or "").strip()
        and str(normalized.get("target_watermark_column") or "").strip()
    ):
        raise ValueError(
            f"Table '{table_id}' configures an incremental watermark value "
            "without incremental_column or both source_watermark_column and "
            "target_watermark_column"
        )
    strategy = normalized.get("incremental_strategy")
    if strategy is not None and strategy not in INCREMENTAL_TABLE_STRATEGIES:
        raise ValueError(
            f"Table '{table_id}' incremental strategy must be one of: "
            + ", ".join(INCREMENTAL_TABLE_STRATEGIES)
        )
    bootstrap = normalized.get("incremental_bootstrap")
    if bootstrap is not None:
        if not isinstance(bootstrap, dict):
            raise ValueError(
                f"Table '{table_id}' incremental.bootstrap must be a YAML mapping"
            )
        unknown = sorted(set(bootstrap) - {
            "watermark", "primary_key", "target_audit", "cdc",
        })
        if unknown:
            raise ValueError(
                f"Table '{table_id}' incremental.bootstrap has unsupported "
                "field(s): " + ", ".join(unknown)
            )
        for name, value in bootstrap.items():
            if value not in INCREMENTAL_BOOTSTRAP_MODES:
                raise ValueError(
                    f"Table '{table_id}' incremental.bootstrap.{name} must be "
                    "one of: " + ", ".join(INCREMENTAL_BOOTSTRAP_MODES)
                )
        normalized["incremental_bootstrap"] = dict(bootstrap)
    mode = normalized.get("incremental_watermark_mode")
    if mode is not None and mode not in {"manual", "auto", "hybrid"}:
        raise ValueError(
            f"Table '{table_id}' incremental watermark_mode must be one of: "
            "manual, auto, hybrid"
        )
    _normalize_table_cdc(normalized, table_id)
    _normalize_table_target_audit(normalized, table_id)


#: Strategies a table may request. ``auto`` defers to the configured priority.
INCREMENTAL_TABLE_STRATEGIES = (
    "auto", "cdc", "watermark", "primary_key", "target_audit", "full_hash",
)
#: Strategies the generic priority list may contain.
INCREMENTAL_STRATEGY_NAMES = (
    "cdc", "watermark", "primary_key", "target_audit", "full_hash",
)
#: Operations an incremental record can be classified as.
INCREMENTAL_OPERATIONS = (
    "insert", "update", "delete", "replay", "unchanged", "unknown",
)
#: Final statuses an operation policy may assign.
INCREMENTAL_POLICY_STATUSES = {"pass": "pass", "fail": "fail", "warning": "warning", "warn": "warning"}
INCREMENTAL_TARGET_BEHAVIORS = ("append_only", "versioned_history")
INCREMENTAL_BOOTSTRAP_MODES = (
    "require_previous_bound", "require_checkpoint", "open_lower_bound",
)
CDC_OPERATION_KINDS = ("insert", "update", "delete")


def _required_name(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{path} must be a non-empty column name")
    return value.strip()


def _normalize_table_cdc(normalized: Dict[str, Any], table_id: str) -> None:
    """Validate a table's explicit CDC metadata; nothing is ever inferred."""
    cdc = normalized.get("incremental_cdc")
    if cdc is None:
        return
    path = f"Table '{table_id}' incremental.cdc"
    if not isinstance(cdc, dict):
        raise ValueError(f"{path} must be a YAML mapping")
    # Only fields this framework version actually honours are accepted.
    # `sequence_column` orders the versions of one logical record; the CDC
    # window itself is resolved by the watermark / key-checkpoint / target
    # audit boundary, so a CDC-sequence or LSN window is not supported yet and
    # is reported rather than accepted and silently ignored.
    allowed = {"enabled", "operation_column", "operation_values", "sequence_column"}
    not_implemented = {
        "lsn_column": "a CDC log-sequence window is not implemented",
        "version_column": "a CDC version window is not implemented",
        "previous_sequence": "a CDC sequence window is not implemented",
        "current_sequence": "a CDC sequence window is not implemented",
    }
    declared = sorted(set(cdc) & set(not_implemented))
    if declared:
        raise ValueError(
            f"{path} declares field(s) this framework version does not support: "
            + ", ".join(f"{name} ({not_implemented[name]})" for name in declared)
            + "; resolve the incremental window with a watermark, a key "
            "checkpoint or incremental.target_audit instead"
        )
    unknown = sorted(set(cdc) - allowed)
    if unknown:
        raise ValueError(f"{path} has unsupported field(s): " + ", ".join(unknown))
    resolved = dict(cdc)
    enabled = resolved.get("enabled", True)
    if not isinstance(enabled, bool):
        raise ValueError(f"{path}.enabled must be boolean")
    resolved["enabled"] = enabled
    if not enabled:
        normalized["incremental_cdc"] = resolved
        return
    resolved["operation_column"] = _required_name(
        resolved.get("operation_column"), f"{path}.operation_column"
    )
    values = resolved.get("operation_values")
    if not isinstance(values, dict) or not values:
        raise ValueError(
            f"{path}.operation_values must map insert/update/delete to the "
            "configured operation values"
        )
    unknown_kinds = sorted(set(values) - set(CDC_OPERATION_KINDS))
    if unknown_kinds:
        raise ValueError(
            f"{path}.operation_values has unsupported operation(s): "
            + ", ".join(unknown_kinds)
        )
    normalized_values: Dict[str, List[str]] = {}
    seen: Dict[str, str] = {}
    for kind, configured in values.items():
        if isinstance(configured, (str, int)):
            configured = [configured]
        if not isinstance(configured, list) or not configured:
            raise ValueError(f"{path}.operation_values.{kind} must be a non-empty list")
        rendered = []
        for item in configured:
            if item is None or (isinstance(item, str) and not item.strip()):
                raise ValueError(f"{path}.operation_values.{kind} contains an empty value")
            text = str(item).strip()
            if text in seen and seen[text] != kind:
                raise ValueError(
                    f"{path}.operation_values maps '{text}' to both {seen[text]} and {kind}"
                )
            seen[text] = kind
            rendered.append(text)
        normalized_values[kind] = rendered
    resolved["operation_values"] = normalized_values
    if resolved.get("sequence_column") is not None:
        resolved["sequence_column"] = _required_name(
            resolved["sequence_column"], f"{path}.sequence_column"
        )
    normalized["incremental_cdc"] = resolved


def _normalize_table_target_audit(normalized: Dict[str, Any], table_id: str) -> None:
    """Validate a table's target audit/load-window metadata."""
    audit = normalized.get("incremental_target_audit")
    if audit is None:
        return
    path = f"Table '{table_id}' incremental.target_audit"
    if not isinstance(audit, dict):
        raise ValueError(f"{path} must be a YAML mapping")
    raise ValueError(
        f"{path} is no longer a table setting; configure target_audit only in "
        "validations.incremental_strategy and supply boundaries in incremental.metadata"
    )


def _normalize_incremental_strategy_config(raw: Any) -> Dict[str, Any]:
    """Validate and fill the common, connector-independent strategy policy."""
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError("validations.incremental_strategy must be a YAML mapping")
    configured = dict(raw)
    strategy = dict(configured.get("strategy") or {})
    watermark = dict(configured.get("watermark") or {})
    primary_key = dict(configured.get("primary_key") or {})
    fallback = dict(configured.get("fallback") or {})
    safety = dict(configured.get("safety") or {})
    mode = strategy.setdefault("mode", "auto")
    if mode not in INCREMENTAL_TABLE_STRATEGIES:
        raise ValueError("validations.incremental_strategy.strategy.mode is invalid")
    priority = strategy.setdefault("priority", ["watermark", "primary_key", "full_hash"])
    if not isinstance(priority, list) or not priority or any(
        item not in INCREMENTAL_STRATEGY_NAMES for item in priority
    ):
        raise ValueError(
            "validations.incremental_strategy.strategy.priority must contain only "
            + ", ".join(INCREMENTAL_STRATEGY_NAMES)
        )
    watermark_mode = watermark.setdefault("mode", "hybrid")
    if watermark_mode not in {"manual", "auto", "hybrid"}:
        raise ValueError("validations.incremental_strategy.watermark.mode is invalid")
    sources = watermark.setdefault(
        "resolution_sources", ["table_yaml", "runtime_metadata", "checkpoint"]
    )
    allowed_sources = {"table_yaml", "runtime_metadata", "checkpoint", "common_validation_config"}
    if not isinstance(sources, list) or any(item not in allowed_sources for item in sources):
        raise ValueError(
            "validations.incremental_strategy.watermark.resolution_sources is invalid"
        )
    filter_config = dict(watermark.get("filter") or {})
    filter_config.setdefault("lower_operator", ">")
    filter_config.setdefault("upper_operator", "<=")
    filter_config.setdefault("parameterized", True)
    if filter_config["lower_operator"] not in {">", ">="}:
        raise ValueError("incremental watermark lower_operator must be > or >=")
    if filter_config["upper_operator"] not in {"<", "<="}:
        raise ValueError("incremental watermark upper_operator must be < or <=")
    watermark["filter"] = filter_config
    primary_key.setdefault("enabled", True)
    primary_key.setdefault("require_incremental_compatible_key", True)
    primary_key.setdefault("require_monotonic_key", True)
    primary_key.setdefault("checkpoint_enabled", True)
    primary_key.setdefault("unsuitable_key_action", "full_hash")
    no_selector = fallback.setdefault("no_watermark_no_pk", "full_hash")
    if no_selector not in {"full_hash", "target_audit", "error", "skip_table"}:
        raise ValueError(
            "validations.incremental_strategy.fallback.no_watermark_no_pk must be "
            "one of: full_hash, target_audit, error, skip_table"
        )
    full_hash = dict(fallback.get("full_hash") or {})
    full_hash.setdefault("enabled", True)
    full_hash.setdefault("use_existing_hash_comparison", True)
    preserve_keys = full_hash.setdefault("preserve_key_mappings", True)
    if not isinstance(preserve_keys, bool):
        raise ValueError(
            "validations.incremental_strategy.fallback.full_hash.preserve_key_mappings "
            "must be boolean"
        )
    fallback["full_hash"] = full_hash
    safety.setdefault("allow_implicit_full_table_fallback", False)
    safety.setdefault("allow_explicit_full_hash_strategy", True)
    safety.setdefault("unresolved_watermark_action", "error")
    safety.setdefault("missing_incremental_configuration_action", "error")
    configured.setdefault("enabled", True)
    configured["strategy"] = strategy
    configured["watermark"] = watermark
    configured["primary_key"] = primary_key
    configured["fallback"] = fallback
    configured["safety"] = safety
    configured.setdefault(
        "precedence", ["table_yaml", "common_validation_config", "framework_default"]
    )
    _normalize_incremental_operation_policy(configured)
    return configured


def _normalize_incremental_operation_policy(configured: Dict[str, Any]) -> None:
    """Validate the generic operation/target-behavior/CDC/bootstrap policy.

    No operation outcome is defaulted in Python: when ``operation_policy`` is
    absent, operation classification is simply not enabled and the incremental
    phase keeps its existing comparison-only behavior. When it is present every
    operation must be assigned explicitly.
    """
    prefix = "validations.incremental_strategy"

    def mapping(name: str) -> Dict[str, Any]:
        raw = configured.get(name)
        if raw is None:
            return {}
        if not isinstance(raw, dict):
            raise ValueError(f"{prefix}.{name} must be a YAML mapping")
        return dict(raw)

    policy = mapping("operation_policy")
    classification = mapping("classification")
    target_behavior = mapping("target_behavior")
    cdc = mapping("cdc")
    target_audit = mapping("target_audit")
    bootstrap = mapping("bootstrap")
    reporting = mapping("reporting")

    enabled = classification.get("enabled", bool(policy))
    if not isinstance(enabled, bool):
        raise ValueError(f"{prefix}.classification.enabled must be boolean")
    classification["enabled"] = enabled
    for name in (
        "preserve_operation_and_status_separately", "historical_check",
        "historical_absence_proves_insert",
    ):
        value = classification.setdefault(name, True)
        if not isinstance(value, bool):
            raise ValueError(f"{prefix}.classification.{name} must be boolean")

    if policy:
        unknown = sorted(set(policy) - set(INCREMENTAL_OPERATIONS))
        if unknown:
            raise ValueError(
                f"{prefix}.operation_policy has unsupported operation(s): "
                + ", ".join(unknown)
            )
        missing = [name for name in INCREMENTAL_OPERATIONS if name not in policy]
        if missing:
            raise ValueError(
                f"{prefix}.operation_policy must assign every operation; missing: "
                + ", ".join(missing)
            )
        for name in INCREMENTAL_OPERATIONS:
            value = str(policy[name]).strip().lower()
            if value not in INCREMENTAL_POLICY_STATUSES:
                raise ValueError(
                    f"{prefix}.operation_policy.{name} must be one of: pass, fail, warning"
                )
            policy[name] = INCREMENTAL_POLICY_STATUSES[value]
    elif enabled:
        raise ValueError(
            f"{prefix}.operation_policy must be configured when "
            f"{prefix}.classification.enabled is true"
        )

    if target_behavior:
        mode = target_behavior.get("mode")
        if mode not in INCREMENTAL_TARGET_BEHAVIORS:
            raise ValueError(
                f"{prefix}.target_behavior.mode must be one of: "
                + ", ".join(INCREMENTAL_TARGET_BEHAVIORS)
            )
    elif enabled:
        raise ValueError(
            f"{prefix}.target_behavior.mode must be configured when operation "
            "classification is enabled"
        )

    behavior = cdc.get("delete_behavior")
    if behavior is not None and (not isinstance(behavior, str) or not behavior.strip()):
        raise ValueError(f"{prefix}.cdc.delete_behavior must be a non-empty string")
    if behavior is not None:
        cdc["delete_behavior"] = behavior.strip().lower()
    column = cdc.get("soft_delete_column")
    if column is not None and (not isinstance(column, str) or not column.strip()):
        raise ValueError(f"{prefix}.cdc.soft_delete_column must be a column name or null")
    values = cdc.get("soft_delete_values")
    if values is None:
        cdc["soft_delete_values"] = []
    elif not isinstance(values, list):
        raise ValueError(f"{prefix}.cdc.soft_delete_values must be a list")
    else:
        cdc["soft_delete_values"] = [str(item).strip() for item in values]

    sources = target_audit.setdefault(
        "resolution_sources", ["runtime_metadata", "checkpoint"]
    )
    if not isinstance(sources, list) or not sources or any(
        item not in {"runtime_metadata", "checkpoint", "audit_values"} for item in sources
    ):
        raise ValueError(
            f"{prefix}.target_audit.resolution_sources must list runtime_metadata, "
            "checkpoint and/or audit_values"
        )
    if not isinstance(target_audit.get("enabled", False), bool):
        raise ValueError(f"{prefix}.target_audit.enabled must be boolean")
    if target_audit.get("enabled"):
        target_audit["load_timestamp_column"] = _required_name(
            target_audit.get("load_timestamp_column"), f"{prefix}.target_audit.load_timestamp_column"
        )
    semantics = target_audit.get("semantics")
    if semantics is not None and semantics not in {"batch_value", "row_value"}:
        raise ValueError(
            f"{prefix}.target_audit.semantics must be batch_value or row_value"
        )
    batch = target_audit.get("batch_window_resolution") or {}
    if not isinstance(batch, dict) or not isinstance(batch.get("enabled", False), bool):
        raise ValueError(f"{prefix}.target_audit.batch_window_resolution must contain boolean enabled")
    if batch.get("enabled") and batch.get("strategy") != "latest_two_distinct_values":
        raise ValueError(f"{prefix}.target_audit.batch_window_resolution.strategy is unsupported")
    if batch.get("enabled") and semantics != "batch_value":
        raise ValueError(
            f"{prefix}.target_audit batch_window_resolution requires "
            "target_audit.semantics=batch_value"
        )
    # Enabling batch discovery is the explicit authorization to read the
    # target audit column. Keep configured metadata/checkpoint precedence and
    # add database discovery as the final fallback; requiring users to also
    # repeat ``audit_values`` made an enabled strategy silently do nothing.
    if batch.get("enabled") and "audit_values" not in sources:
        sources.append("audit_values")
    if "audit_values" in sources and not batch.get("enabled"):
        raise ValueError(f"{prefix}.target_audit audit_values requires batch_window_resolution.enabled")

    defaults = {
        "watermark": "require_previous_bound",
        "primary_key": "require_checkpoint",
        # A target-audit window can safely bootstrap from its resolved current
        # batch boundary when no prior checkpoint exists. The target-driven
        # comparison remains bounded by that current boundary and commits it
        # for the next run.
        "target_audit": "open_lower_bound",
        "cdc": "require_checkpoint",
    }
    unknown = sorted(set(bootstrap) - set(defaults))
    if unknown:
        raise ValueError(f"{prefix}.bootstrap has unsupported field(s): " + ", ".join(unknown))
    for name, default in defaults.items():
        value = bootstrap.setdefault(name, default)
        if value not in INCREMENTAL_BOOTSTRAP_MODES:
            raise ValueError(
                f"{prefix}.bootstrap.{name} must be one of: "
                + ", ".join(INCREMENTAL_BOOTSTRAP_MODES)
            )

    capture = reporting.setdefault("capture_mismatch_evidence", True)
    if not isinstance(capture, bool):
        raise ValueError(f"{prefix}.reporting.capture_mismatch_evidence must be boolean")

    configured["operation_policy"] = policy
    configured["classification"] = classification
    configured["target_behavior"] = target_behavior
    configured["cdc"] = cdc
    configured["target_audit"] = target_audit
    configured["bootstrap"] = bootstrap
    configured["reporting"] = reporting

#: Behaviors the character-length check supports, from configuration only.
MAX_LENGTH_MODES = ("exact", "truncation_only", "tolerance")

#: Actions the zero-record policy supports for downstream validations.
ZERO_RECORD_ACTIONS = ("continue", "warn_and_continue", "stop_downstream")

#: Which sides the zero-record policy can describe.
ZERO_RECORD_SIDES = ("both_empty", "source_empty", "target_empty")

#: Key-cohort strategies selectable from configuration.
KEY_COHORT_STRATEGIES = (
    "auto", "values_join", "key_range", "predicate_or", "staged_keys",
)

#: Reverse-pass policies for the key-based full comparison.
REVERSE_PASS_MODES = ("always", "when_counts_reconcile")

#: Hash spill strategies.
SPILL_STRATEGIES = ("sqlite", "memory")

#: Row-payload retention policies for hash reconciliation evidence.
PAYLOAD_POLICIES = ("bounded", "all", "none")

_SUPPORTED_STATUS_NAMES = {
    "PASS", "FAIL", "WARN", "WARNING", "SKIP", "ERROR",
    "FRAMEWORK_ERROR", "CONFIG_ERROR",
}


def _validate_data_value_check_options(check: Dict[str, Any], index: int) -> None:
    """Validate the per-check options a data value check may declare."""
    path = f"validations.data_value_validation.checks[{index}]"
    name = str(check.get("name", "")).strip()
    if name == "max_character_length_check":
        mode = check.get("mode", "exact")
        if mode not in MAX_LENGTH_MODES:
            raise ValueError(
                f"{path}.mode must be one of: " + ", ".join(MAX_LENGTH_MODES)
            )
        tolerance = check.get("length_tolerance", 0)
        if (
            isinstance(tolerance, bool)
            or not isinstance(tolerance, int)
            or tolerance < 0
        ):
            raise ValueError(
                f"{path}.length_tolerance must be a non-negative integer"
            )
        if mode == "tolerance" and tolerance <= 0:
            raise ValueError(
                f"{path}.length_tolerance must be positive when mode is tolerance"
            )


def _validate_zero_record_policy(raw: Any) -> None:
    """Validate the configured behavior for an empty validation scope."""
    if raw is None:
        return
    if not isinstance(raw, dict):
        raise ValueError(
            "validations.edge_case_validation.zero_record_policy must be a "
            "YAML mapping"
        )
    override_fields = {f"{side}_validation_overrides" for side in ZERO_RECORD_SIDES}
    unknown = sorted(
        set(raw) - set(ZERO_RECORD_SIDES) - override_fields - {
            "downstream_allowed_when_stopped", "status_when_stopped",
            "stopped_message",
        }
    )
    if unknown:
        raise ValueError(
            "validations.edge_case_validation.zero_record_policy has "
            "unsupported field(s): " + ", ".join(unknown)
        )
    _validate_zero_record_overrides(raw)
    for side in ZERO_RECORD_SIDES:
        if side in raw and raw[side] not in ZERO_RECORD_ACTIONS:
            raise ValueError(
                f"validations.edge_case_validation.zero_record_policy.{side} "
                "must be one of: " + ", ".join(ZERO_RECORD_ACTIONS)
            )
    allowed = raw.get("downstream_allowed_when_stopped", [])
    if not isinstance(allowed, list) or not all(
        isinstance(item, str) and item.strip() for item in allowed
    ):
        raise ValueError(
            "validations.edge_case_validation.zero_record_policy."
            "downstream_allowed_when_stopped must be a list of validation names"
        )
    status = raw.get("status_when_stopped", "SKIP")
    if status not in _SUPPORTED_STATUS_NAMES:
        raise ValueError(
            "validations.edge_case_validation.zero_record_policy."
            "status_when_stopped has an unsupported status"
        )
    message = raw.get("stopped_message")
    if message is not None and not (
        isinstance(message, str) and message.strip()
    ):
        raise ValueError(
            "validations.edge_case_validation.zero_record_policy."
            "stopped_message must be a non-empty string"
        )


def _validate_zero_record_overrides(raw: Dict[str, Any]) -> None:
    """Validate the per-validation results configured for an empty scope.

    ``<side>_validation_overrides`` declares the result a named validation
    reports instead of running when that side condition is observed, for
    example a PASS for record comparison when both sides hold zero records.
    The validation name is not checked against the enabled validations: a name
    that never runs simply never matches.
    """
    for side in ZERO_RECORD_SIDES:
        field_name = f"{side}_validation_overrides"
        overrides = raw.get(field_name)
        if overrides is None:
            continue
        base = (
            "validations.edge_case_validation.zero_record_policy."
            f"{field_name}"
        )
        if not isinstance(overrides, dict):
            raise ValueError(f"{base} must be a YAML mapping")
        for validation_name, override in overrides.items():
            path = f"{base}.{validation_name}"
            if not isinstance(override, dict):
                raise ValueError(f"{path} must be a YAML mapping")
            unsupported = sorted(set(override) - {"status", "message"})
            if unsupported:
                raise ValueError(
                    f"{path} has unsupported field(s): " + ", ".join(unsupported)
                )
            if override.get("status") not in _SUPPORTED_STATUS_NAMES:
                raise ValueError(f"{path}.status has an unsupported status")
            message = override.get("message")
            if not (isinstance(message, str) and message.strip()):
                raise ValueError(f"{path}.message must be a non-empty string")


def _validate_scan_optimization(raw: Any) -> None:
    """Validate the physical scan-sharing configuration."""
    if raw is None:
        return
    if not isinstance(raw, dict):
        raise ValueError("validations.scan_optimization must be a YAML mapping")
    unknown = sorted(
        set(raw) - {"shared_statistics", "shared_record_read", "instrumentation"}
    )
    if unknown:
        raise ValueError(
            "validations.scan_optimization has unsupported section(s): "
            + ", ".join(unknown)
        )
    booleans = {
        "shared_statistics": (
            "enabled", "combine_key_statistics", "reuse_across_validations",
            "parallel_sides",
        ),
        "shared_record_read": ("enabled",),
        "instrumentation": ("enabled", "record_statements"),
    }
    for section_name, field_names in booleans.items():
        section = raw.get(section_name)
        if section is None:
            continue
        if not isinstance(section, dict):
            raise ValueError(
                f"validations.scan_optimization.{section_name} must be a "
                "YAML mapping"
            )
        for field_name in field_names:
            if field_name in section and not isinstance(section[field_name], bool):
                raise ValueError(
                    f"validations.scan_optimization.{section_name}.{field_name} "
                    "must be boolean"
                )
    record_read = raw.get("shared_record_read") or {}
    consumers = record_read.get("consumers", [])
    if not isinstance(consumers, list) or not all(
        isinstance(item, str) and item.strip() for item in consumers
    ):
        raise ValueError(
            "validations.scan_optimization.shared_record_read.consumers must be "
            "a list of validation names"
        )


def _build_scan_optimization(raw: Any) -> ScanOptimizationConfig:
    """Build the physical scan-sharing configuration from performance.yaml."""
    defaults = ScanOptimizationConfig()
    if raw is None:
        return defaults
    if not isinstance(raw, dict):
        raise ValueError("performance.scan_optimization must be a YAML mapping")
    supported = {
        "enabled", "shared_statistics_scan", "shared_record_scan",
        "reuse_key_statistics", "reuse_row_counts", "reuse_null_statistics",
        "reuse_duplicate_statistics", "reuse_audit_statistics",
        "duplicate_query_detection",
    }
    unknown = sorted(set(raw) - supported)
    if unknown:
        raise ValueError(
            "performance.scan_optimization has unsupported field(s): "
            + ", ".join(unknown)
        )

    def nested_flag(name: str, default: bool) -> bool:
        value = raw.get(name, default)
        if isinstance(value, dict):
            value = value.get("enabled", default)
        if not isinstance(value, bool):
            raise ValueError(
                f"performance.scan_optimization.{name} must be boolean or a "
                "mapping containing an 'enabled' boolean"
            )
        return value

    return ScanOptimizationConfig(
        enabled=nested_flag("enabled", defaults.enabled),
        shared_statistics_scan=nested_flag(
            "shared_statistics_scan", defaults.shared_statistics_scan
        ),
        shared_record_scan=nested_flag(
            "shared_record_scan", defaults.shared_record_scan
        ),
        reuse_key_statistics=nested_flag(
            "reuse_key_statistics", defaults.reuse_key_statistics
        ),
        reuse_row_counts=nested_flag("reuse_row_counts", defaults.reuse_row_counts),
        reuse_null_statistics=nested_flag(
            "reuse_null_statistics", defaults.reuse_null_statistics
        ),
        reuse_duplicate_statistics=nested_flag(
            "reuse_duplicate_statistics", defaults.reuse_duplicate_statistics
        ),
        reuse_audit_statistics=nested_flag(
            "reuse_audit_statistics", defaults.reuse_audit_statistics
        ),
        duplicate_query_detection=nested_flag(
            "duplicate_query_detection", defaults.duplicate_query_detection
        ),
    )


def _build_cohort_processing(raw: Any) -> CohortProcessingConfig:
    """Build the key-cohort strategy configuration from performance.yaml."""
    defaults = CohortProcessingConfig()
    if raw is None:
        return defaults
    if not isinstance(raw, dict):
        raise ValueError("performance.cohort_processing must be a YAML mapping")
    unknown = sorted(
        set(raw) - {
            "strategy", "allowed_strategies", "values_join", "predicate_or"
        }
    )
    if unknown:
        raise ValueError(
            "performance.cohort_processing has unsupported field(s): "
            + ", ".join(unknown)
        )
    strategy = raw.get("strategy", defaults.strategy)
    if strategy not in KEY_COHORT_STRATEGIES:
        raise ValueError(
            "performance.cohort_processing.strategy must be one of: "
            + ", ".join(KEY_COHORT_STRATEGIES)
        )
    allowed = raw.get("allowed_strategies", list(defaults.allowed_strategies))
    if not isinstance(allowed, list) or not allowed:
        raise ValueError(
            "performance.cohort_processing.allowed_strategies must be a "
            "non-empty list"
        )
    concrete_strategies = set(KEY_COHORT_STRATEGIES) - {"auto"}
    unsupported = sorted(set(allowed) - concrete_strategies)
    if unsupported:
        raise ValueError(
            "performance.cohort_processing.allowed_strategies contains "
            "unsupported strategy(ies): " + ", ".join(unsupported)
        )
    if strategy != "auto" and strategy not in allowed:
        raise ValueError(
            f"performance.cohort_processing.strategy '{strategy}' is not listed "
            "in allowed_strategies"
        )
    values_join_raw = raw.get("values_join")
    if "values_join" in allowed and values_join_raw is None:
        raise ValueError(
            "performance.cohort_processing.values_join must configure max_keys"
        )
    values_join_raw = values_join_raw or {}
    if not isinstance(values_join_raw, dict):
        raise ValueError(
            "performance.cohort_processing.values_join must be a YAML mapping"
        )
    values_join_max_keys = values_join_raw.get("max_keys")
    if "values_join" in allowed and (
        isinstance(values_join_max_keys, bool)
        or not isinstance(values_join_max_keys, int)
        or values_join_max_keys <= 0
    ):
        raise ValueError(
            "performance.cohort_processing.values_join.max_keys must be a "
            "positive integer"
        )
    predicate_raw = raw.get("predicate_or") or {}
    if not isinstance(predicate_raw, dict):
        raise ValueError(
            "performance.cohort_processing.predicate_or must be a YAML mapping"
        )
    predicate_enabled = predicate_raw.get(
        "enabled", defaults.predicate_or.enabled
    )
    if not isinstance(predicate_enabled, bool):
        raise ValueError(
            "performance.cohort_processing.predicate_or.enabled must be boolean"
        )
    max_keys = predicate_raw.get("max_keys", defaults.predicate_or.max_keys)
    if isinstance(max_keys, bool) or not isinstance(max_keys, int) or max_keys <= 0:
        raise ValueError(
            "performance.cohort_processing.predicate_or.max_keys must be a "
            "positive integer"
        )
    return CohortProcessingConfig(
        strategy=strategy,
        allowed_strategies=tuple(allowed),
        values_join=ValuesJoinConfig(max_keys=values_join_max_keys),
        predicate_or=PredicateOrConfig(
            enabled=predicate_enabled, max_keys=max_keys
        ),
    )


def _build_incremental_execution(raw: Any) -> IncrementalExecutionConfig:
    """Build the global incremental-scope safety ceiling."""
    defaults = IncrementalExecutionConfig()
    if raw is None:
        return defaults
    if not isinstance(raw, dict):
        raise ValueError("performance.incremental_execution must be a YAML mapping")
    unknown = sorted(
        set(raw) - {
            "require_scope", "missing_scope_action", "allow_full_table_fallback",
        }
    )
    if unknown:
        raise ValueError(
            "performance.incremental_execution has unsupported field(s): "
            + ", ".join(unknown)
        )
    require_scope = raw.get("require_scope", defaults.require_scope)
    allow_fallback = raw.get(
        "allow_full_table_fallback", defaults.allow_full_table_fallback
    )
    for name, value in (
        ("require_scope", require_scope),
        ("allow_full_table_fallback", allow_fallback),
    ):
        if not isinstance(value, bool):
            raise ValueError(
                f"performance.incremental_execution.{name} must be boolean"
            )
    action = raw.get("missing_scope_action", defaults.missing_scope_action)
    if action not in UNRESOLVED_ACTIONS:
        raise ValueError(
            "performance.incremental_execution.missing_scope_action must be one "
            "of: " + ", ".join(UNRESOLVED_ACTIONS)
        )
    if action == "full_scan" and not allow_fallback:
        raise ValueError(
            "performance.incremental_execution.missing_scope_action cannot be "
            "full_scan while allow_full_table_fallback is false"
        )
    return IncrementalExecutionConfig(
        require_scope=require_scope,
        missing_scope_action=action,
        allow_full_table_fallback=allow_fallback,
    )


def _config_bool(raw: dict, name: str, default: bool, path: str) -> bool:
    value = raw.get(name, default)
    if not isinstance(value, bool):
        raise ValueError(f"{path}.{name} must be boolean")
    return value


def _config_positive_int(raw: dict, name: str, default: int, path: str) -> int:
    value = raw.get(name, default)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{path}.{name} must be a positive integer")
    return value


def _config_nonnegative_number(raw: dict, name: str, default, path: str):
    value = raw.get(name, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError(f"{path}.{name} must be a non-negative number")
    return value


def _reject_unknown(raw: dict, supported: set, path: str) -> None:
    unknown = sorted(set(raw) - supported)
    if unknown:
        raise ValueError(
            f"{path} has unsupported field(s): " + ", ".join(unknown)
        )


def _build_engine_selection(raw: Any) -> EngineSelectionConfig:
    """Build the run-level engine availability and fallback policy."""
    defaults = EngineSelectionConfig()
    if raw is None:
        return defaults
    if not isinstance(raw, dict):
        raise ValueError("performance.engine_selection must be a YAML mapping")
    path = "performance.engine_selection"
    _reject_unknown(
        raw,
        {"preferred_engine", "availability_check", "priority", "spark", "fallback"},
        path,
    )

    preferred = raw.get("preferred_engine", defaults.preferred_engine)
    if preferred not in ("auto",) + SELECTABLE_ENGINE_NAMES:
        raise ValueError(
            f"{path}.preferred_engine must be one of: auto, "
            + ", ".join(SELECTABLE_ENGINE_NAMES)
        )

    check_raw = raw.get("availability_check") or {}
    if not isinstance(check_raw, dict):
        raise ValueError(f"{path}.availability_check must be a YAML mapping")
    _reject_unknown(check_raw, {"enabled", "cache_for_run"}, f"{path}.availability_check")
    check_enabled = _config_bool(
        check_raw, "enabled", defaults.availability_check_enabled,
        f"{path}.availability_check",
    )
    cache_for_run = _config_bool(
        check_raw, "cache_for_run", defaults.cache_for_run,
        f"{path}.availability_check",
    )

    priority = raw.get("priority", list(defaults.priority))
    if not isinstance(priority, list) or not priority:
        raise ValueError(f"{path}.priority must be a non-empty list")
    unsupported = sorted(set(priority) - set(SELECTABLE_ENGINE_NAMES))
    if unsupported:
        raise ValueError(
            f"{path}.priority contains unsupported engine(s): "
            + ", ".join(unsupported)
        )

    spark_raw = raw.get("spark") or {}
    if not isinstance(spark_raw, dict):
        raise ValueError(f"{path}.spark must be a YAML mapping")
    _reject_unknown(
        spark_raw,
        {
            "enabled", "required", "require_java", "availability",
            "on_java_missing", "on_pyspark_missing",
            "on_initialization_failure", "unavailable_action",
        },
        f"{path}.spark",
    )
    spark_defaults = defaults.spark
    availability_raw = spark_raw.get("availability") or {}
    if not isinstance(availability_raw, dict):
        raise ValueError(f"{path}.spark.availability must be a YAML mapping")
    _reject_unknown(
        availability_raw,
        {"check_java", "check_java_home", "check_pyspark"},
        f"{path}.spark.availability",
    )
    availability = SparkAvailabilityCheckConfig(
        check_java=_config_bool(
            availability_raw, "check_java",
            spark_defaults.availability.check_java, f"{path}.spark.availability",
        ),
        check_java_home=_config_bool(
            availability_raw, "check_java_home",
            spark_defaults.availability.check_java_home,
            f"{path}.spark.availability",
        ),
        check_pyspark=_config_bool(
            availability_raw, "check_pyspark",
            spark_defaults.availability.check_pyspark,
            f"{path}.spark.availability",
        ),
    )
    # ``unavailable_action`` is shorthand for all three specific actions.
    shorthand = spark_raw.get("unavailable_action")
    actions = {}
    for name in ("on_java_missing", "on_pyspark_missing", "on_initialization_failure"):
        value = spark_raw.get(name, shorthand or getattr(spark_defaults, name))
        if value not in ENGINE_UNAVAILABLE_ACTIONS:
            raise ValueError(
                f"{path}.spark.{name} must be one of: "
                + ", ".join(ENGINE_UNAVAILABLE_ACTIONS)
            )
        actions[name] = value
    spark = SparkEngineSelectionConfig(
        enabled=_config_bool(
            spark_raw, "enabled", spark_defaults.enabled, f"{path}.spark"
        ),
        required=_config_bool(
            spark_raw, "required", spark_defaults.required, f"{path}.spark"
        ),
        require_java=_config_bool(
            spark_raw, "require_java", spark_defaults.require_java, f"{path}.spark"
        ),
        availability=availability,
        **actions,
    )

    fallback_raw = raw.get("fallback") or {}
    if not isinstance(fallback_raw, dict):
        raise ValueError(f"{path}.fallback must be a YAML mapping")
    _reject_unknown(
        fallback_raw, {"enabled", "when_spark_unavailable"}, f"{path}.fallback"
    )
    fallback_enabled = _config_bool(
        fallback_raw, "enabled", defaults.fallback_enabled, f"{path}.fallback"
    )
    when_unavailable = fallback_raw.get(
        "when_spark_unavailable", list(defaults.when_spark_unavailable)
    )
    if not isinstance(when_unavailable, list) or not when_unavailable:
        raise ValueError(
            f"{path}.fallback.when_spark_unavailable must be a non-empty list"
        )
    unsupported = sorted(
        set(when_unavailable) - (set(SELECTABLE_ENGINE_NAMES) - {"spark"})
    )
    if unsupported:
        raise ValueError(
            f"{path}.fallback.when_spark_unavailable contains unsupported "
            "engine(s): " + ", ".join(unsupported)
        )

    return EngineSelectionConfig(
        preferred_engine=preferred,
        availability_check_enabled=check_enabled,
        cache_for_run=cache_for_run,
        priority=tuple(priority),
        spark=spark,
        fallback_enabled=fallback_enabled,
        when_spark_unavailable=tuple(when_unavailable),
    )


def _build_adaptive_batching(raw: Any) -> AdaptiveBatchingConfig:
    """Build the bounded physical fetch-size tuning configuration."""
    defaults = AdaptiveBatchingConfig()
    if raw is None:
        return defaults
    if not isinstance(raw, dict):
        raise ValueError(
            "performance.batch_processing.adaptive_batching must be a YAML mapping"
        )
    path = "performance.batch_processing.adaptive_batching"
    _reject_unknown(
        raw,
        {
            "enabled", "initial_batch_size", "minimum_batch_size",
            "maximum_batch_size", "increase_factor", "decrease_factor",
            "target_batch_seconds",
        },
        path,
    )
    enabled = _config_bool(raw, "enabled", defaults.enabled, path)
    initial = raw.get("initial_batch_size", defaults.initial_batch_size)
    maximum = raw.get("maximum_batch_size", defaults.maximum_batch_size)
    for name, value in (("initial_batch_size", initial), ("maximum_batch_size", maximum)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{path}.{name} must be a non-negative integer")
    minimum = _config_positive_int(
        raw, "minimum_batch_size", defaults.minimum_batch_size, path
    )
    increase = _config_nonnegative_number(
        raw, "increase_factor", defaults.increase_factor, path
    )
    decrease = _config_nonnegative_number(
        raw, "decrease_factor", defaults.decrease_factor, path
    )
    if increase < 1:
        raise ValueError(f"{path}.increase_factor must be at least 1")
    if not 0 < decrease <= 1:
        raise ValueError(f"{path}.decrease_factor must be between 0 and 1")
    target = _config_nonnegative_number(
        raw, "target_batch_seconds", defaults.target_batch_seconds, path
    )
    if maximum and maximum < minimum:
        raise ValueError(
            f"{path}.maximum_batch_size must be at least minimum_batch_size"
        )
    return AdaptiveBatchingConfig(
        enabled=enabled,
        initial_batch_size=initial,
        minimum_batch_size=minimum,
        maximum_batch_size=maximum,
        increase_factor=float(increase),
        decrease_factor=float(decrease),
        target_batch_seconds=float(target),
    )


def _build_hash_spill(raw: Any) -> HashSpillConfig:
    """Build the local hash-count spill store configuration."""
    defaults = HashSpillConfig()
    if raw is None:
        return defaults
    if not isinstance(raw, dict):
        raise ValueError("performance.hash_spill must be a YAML mapping")
    path = "performance.hash_spill"
    _reject_unknown(raw, {"engine", "priority", "commit_interval_batches"}, path)
    supported_engines = ("auto", "duckdb", "sqlite", "memory")
    engine = raw.get("engine", defaults.engine)
    if engine not in supported_engines:
        raise ValueError(
            f"{path}.engine must be one of: " + ", ".join(supported_engines)
        )
    priority = raw.get("priority", list(defaults.priority))
    if not isinstance(priority, list) or not priority:
        raise ValueError(f"{path}.priority must be a non-empty list")
    unsupported = sorted(set(priority) - {"duckdb", "sqlite", "memory"})
    if unsupported:
        raise ValueError(
            f"{path}.priority contains unsupported engine(s): "
            + ", ".join(unsupported)
        )
    commit_interval = raw.get(
        "commit_interval_batches", defaults.commit_interval_batches
    )
    if not (
        isinstance(commit_interval, int)
        and not isinstance(commit_interval, bool)
        and commit_interval > 0
    ):
        raise ValueError(f"{path}.commit_interval_batches must be positive")
    return HashSpillConfig(
        engine=engine,
        priority=tuple(priority),
        commit_interval_batches=commit_interval,
    )


def _build_temporary_storage(raw: Any) -> TemporaryStorageConfig:
    """Build the temporary-storage safety configuration."""
    defaults = TemporaryStorageConfig()
    if raw is None:
        return defaults
    if not isinstance(raw, dict):
        raise ValueError("performance.temporary_storage must be a YAML mapping")
    path = "performance.temporary_storage"
    _reject_unknown(
        raw,
        {
            "directory", "cleanup_on_success", "cleanup_on_failure",
            "disk_space_check", "minimum_free_space_mb",
        },
        path,
    )
    directory = raw.get("directory", defaults.directory)
    if not isinstance(directory, str) or not directory.strip():
        raise ValueError(f"{path}.directory must be a non-empty string")
    check_raw = raw.get("disk_space_check") or {}
    if not isinstance(check_raw, dict):
        raise ValueError(f"{path}.disk_space_check must be a YAML mapping")
    _reject_unknown(check_raw, {"enabled"}, f"{path}.disk_space_check")
    return TemporaryStorageConfig(
        directory=directory.strip(),
        cleanup_on_success=_config_bool(
            raw, "cleanup_on_success", defaults.cleanup_on_success, path
        ),
        cleanup_on_failure=_config_bool(
            raw, "cleanup_on_failure", defaults.cleanup_on_failure, path
        ),
        disk_space_check_enabled=_config_bool(
            check_raw, "enabled", defaults.disk_space_check_enabled,
            f"{path}.disk_space_check",
        ),
        minimum_free_space_mb=_config_positive_int(
            raw, "minimum_free_space_mb", defaults.minimum_free_space_mb, path
        ),
    )


def _build_large_table_concurrency(raw: Any) -> LargeTableConcurrencyConfig:
    """Build the large-table heavy-scan scheduling ceilings."""
    defaults = LargeTableConcurrencyConfig()
    if raw is None:
        return defaults
    if not isinstance(raw, dict):
        raise ValueError(
            "performance.large_table_concurrency must be a YAML mapping"
        )
    path = "performance.large_table_concurrency"
    _reject_unknown(
        raw,
        {
            "enabled", "max_heavy_validations_per_table",
            "max_large_tables_per_source_endpoint",
            "max_large_tables_per_target_endpoint",
        },
        path,
    )
    return LargeTableConcurrencyConfig(
        enabled=_config_bool(raw, "enabled", defaults.enabled, path),
        max_heavy_validations_per_table=_config_positive_int(
            raw, "max_heavy_validations_per_table",
            defaults.max_heavy_validations_per_table, path,
        ),
        max_large_tables_per_source_endpoint=_config_positive_int(
            raw, "max_large_tables_per_source_endpoint",
            defaults.max_large_tables_per_source_endpoint, path,
        ),
        max_large_tables_per_target_endpoint=_config_positive_int(
            raw, "max_large_tables_per_target_endpoint",
            defaults.max_large_tables_per_target_endpoint, path,
        ),
    )


def _build_diagnostics_tracking(raw: Any) -> DiagnosticsTrackingConfig:
    """Build the execution-counter tracking configuration."""
    defaults = DiagnosticsTrackingConfig()
    if not isinstance(raw, dict):
        return defaults
    values = {}
    for item in ("track_database_queries", "track_full_scans", "track_batch_queries",
                 "track_cache_hits", "track_cache_misses", "track_connection_reuse",
                 "track_shared_scans"):
        value = raw.get(item, getattr(defaults, item))
        if not isinstance(value, bool):
            raise ValueError(f"performance.diagnostics.{item} must be boolean")
        values[item] = value
    return DiagnosticsTrackingConfig(**values)


def _build_preflight_config(raw: Any) -> PreflightConfig:
    defaults = PreflightConfig()
    if raw is None:
        return defaults
    if not isinstance(raw, dict):
        raise ValueError("preflight must be a YAML mapping")
    unknown = sorted(
        set(raw) - {
            "run_id_prefix", "report_name", "timestamp_format",
            "schema_validation", "data_checks",
        }
    )
    if unknown:
        raise ValueError("preflight has unsupported field(s): " + ", ".join(unknown))

    schema_raw = raw.get("schema_validation") or {}
    if not isinstance(schema_raw, dict):
        raise ValueError("preflight.schema_validation must be a YAML mapping")
    profile_source = schema_raw.get(
        "profile_source", defaults.schema_validation.profile_source
    )
    if profile_source not in {"selected_phase", "fixed"}:
        raise ValueError(
            "preflight.schema_validation.profile_source must be selected_phase or fixed"
        )
    fixed_profile = schema_raw.get(
        "fixed_profile", defaults.schema_validation.fixed_profile
    )
    if not isinstance(fixed_profile, str) or not fixed_profile.strip():
        raise ValueError(
            "preflight.schema_validation.fixed_profile must be a non-empty string"
        )

    data_raw = raw.get("data_checks") or {}
    if not isinstance(data_raw, dict):
        raise ValueError("preflight.data_checks must be a YAML mapping")
    enabled = data_raw.get("enabled", defaults.data_checks.enabled)
    if not isinstance(enabled, bool):
        raise ValueError("preflight.data_checks.enabled must be boolean")
    checks = data_raw.get("checks", [])
    if not isinstance(checks, list) or not all(
        isinstance(item, str) and item.strip() for item in checks
    ):
        raise ValueError(
            "preflight.data_checks.checks must be a list of check names"
        )
    supported_preflight_checks = {
        "row_count", "key_validation", "null_validation",
        "duplicate_check", "aggregate_validation",
    }
    unsupported = sorted(set(checks) - supported_preflight_checks)
    if unsupported:
        raise ValueError(
            "preflight.data_checks.checks contains unsupported check(s): "
            + ", ".join(unsupported)
        )
    if enabled and not checks:
        raise ValueError(
            "preflight.data_checks.checks must list at least one check when "
            "preflight data checks are enabled"
        )
    apply_scope = data_raw.get(
        "apply_validation_scope", defaults.data_checks.apply_validation_scope
    )
    if not isinstance(apply_scope, bool):
        raise ValueError(
            "preflight.data_checks.apply_validation_scope must be boolean"
        )
    return PreflightConfig(
        run_id_prefix=str(raw.get("run_id_prefix", defaults.run_id_prefix)),
        report_name=str(raw.get("report_name", defaults.report_name)),
        timestamp_format=str(
            raw.get("timestamp_format", defaults.timestamp_format)
        ),
        schema_validation=PreflightSchemaValidationConfig(
            profile_source=profile_source,
            fixed_profile=fixed_profile.strip(),
        ),
        data_checks=PreflightDataCheckConfig(
            enabled=enabled,
            checks=list(checks),
            apply_validation_scope=apply_scope,
        ),
    )


# ── Dynamic Connection Resolution ────────────────────────────

def _resolve_connection_for_table(
    table_conn_ref: Optional[str],
    execution_ref: Optional[str],
    available_connections: Dict[str, "SourceConfig"],
    connection_role: str = "source"
) -> str:
    """
    Resolve which connection to use for a table with fallback logic.

    Resolution order:
    1. If table explicitly references a connection, validate and use it
    2. If execution specifies a default, validate and use it
    3. If exactly one connection is enabled, auto-select it
    4. Otherwise raise a clear error
    """
    enabled_conns = {
        name: cfg for name, cfg in available_connections.items()
        if cfg.enabled
    }

    # Case 1: Table explicitly references a connection
    if table_conn_ref:
        if table_conn_ref not in available_connections:
            raise ValueError(
                f"Table references unknown {connection_role} connection '{table_conn_ref}'. "
                f"Available: {', '.join(available_connections.keys())}"
            )
        if not available_connections[table_conn_ref].enabled:
            raise ValueError(
                f"Table references disabled {connection_role} connection '{table_conn_ref}'. "
                f"Enable it in configuration or update table settings."
            )
        return table_conn_ref

    # Case 2: Execution specifies a default
    if execution_ref:
        if execution_ref not in available_connections:
            raise ValueError(
                f"Execution configuration specifies unknown {connection_role} '{execution_ref}'. "
                f"Available: {', '.join(available_connections.keys())}"
            )
        if not available_connections[execution_ref].enabled:
            raise ValueError(
                f"Execution {connection_role} '{execution_ref}' is disabled. "
                f"Enable it or update execution configuration. "
                f"Enabled {connection_role}s: {', '.join(enabled_conns.keys()) if enabled_conns else 'none'}"
            )
        return execution_ref

    # Case 3: Auto-select if exactly one is enabled
    if len(enabled_conns) == 1:
        return list(enabled_conns.keys())[0]

    # Case 4: Error - ambiguous or none available
    if not enabled_conns:
        raise ValueError(
            f"No {connection_role} connections are enabled. "
            f"Enable at least one in configuration."
        )
    raise ValueError(
        f"Multiple {connection_role} connections are enabled but none specified. "
        f"Set table.{connection_role}_system or execution.{connection_role} to resolve ambiguity. "
        f"Enabled: {', '.join(enabled_conns.keys())}"
    )


def _resolve_source_target_pair(
    raw: dict,
    sources: Dict[str, "SourceConfig"],
    targets: Dict[str, "TargetConfig"]
) -> tuple[str, str]:
    """Resolve execution-level source and target references with validation."""
    execution_source_ref = raw.get("source")
    execution_target_ref = raw.get("target")

    # Validate that at least one source and target are configured
    if not sources:
        raise ValueError("No source connections configured")
    if not targets:
        raise ValueError("No target connections configured")

    # Resolve source: prefer execution reference, else auto-select if exactly one enabled
    if execution_source_ref:
        execution_source_ref = str(execution_source_ref).strip()
        if execution_source_ref not in sources:
            raise ValueError(
                f"Execution 'source' references unknown connection '{execution_source_ref}'. "
                f"Available: {', '.join(sources.keys())}"
            )

    # Resolve target: same logic
    if execution_target_ref:
        execution_target_ref = str(execution_target_ref).strip()
        if execution_target_ref not in targets:
            raise ValueError(
                f"Execution 'target' references unknown connection '{execution_target_ref}'. "
                f"Available: {', '.join(targets.keys())}"
            )

    return execution_source_ref, execution_target_ref


# ── Config Loader ─────────────────────────────────────────────

class ConfigLoader:
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)

    def load(self) -> MigrationConfig:
        merged_raw = {}
        
        # 1. Determine master configuration path
        if self.config_dir.is_file():
            master_path = self.config_dir
        else:
            # Try common names if directory is provided
            candidates = ["execution.yml", "execution.yaml", "migration_config.yml", "master_config.yml"]
            master_path = None
            for candidate in candidates:
                p = self.config_dir / candidate
                if p.exists():
                    master_path = p
                    break
            
            if not master_path:
                raise FileNotFoundError(f"No master configuration file found in {self.config_dir}")

        print(f"  [INFO] Loading master configuration: {master_path}")
        # The master document plus the sections it names under `config_files`.
        # Merged here, before anything reads `config_paths`, so discovery and
        # the connection environment see one complete execution document.
        merged_raw = load_execution_document(master_path)
        self._load_connection_environment(master_path, merged_raw)
        merged_raw = _expand_env_vars(merged_raw)

        # 2. Extract config paths from the master config
        config_paths = merged_raw.get("config_paths", {})
        
        if not config_paths:
            print(f"  [WARNING] No 'config_paths' defined in {master_path.name}. Falling back to legacy loading.")
            # Legacy fallback: load everything in the directory
            self._legacy_load(merged_raw, master_path)
        else:
            # 3. Load modular configs based on paths/globs defined in execution.yml
            project_root = self._project_root(master_path)
            for key, pattern in config_paths.items():
                # Support {layer} placeholder by replacing it with a wildcard for discovery
                discovery_pattern = pattern.replace("{layer}", "*")
                pattern_path = Path(discovery_pattern)
                if not pattern_path.is_absolute():
                    pattern_path = project_root / pattern_path
                matched_files = sorted(glob.glob(str(pattern_path), recursive=True))
                
                if not matched_files:
                    print(f"  [DEBUG] No files found for {key} pattern: {pattern}")
                    continue
                    
                for file_path in matched_files:
                    p = Path(file_path)
                    # Avoid reloading the master config
                    if p.absolute() == master_path.absolute():
                        continue
                        
                    raw = self._normalize_modular_document(
                        key,
                        normalize_connection_document(self._load_yaml(p)),
                        p,
                    )
                    raw = _expand_env_vars(raw)
                    self._deep_merge(merged_raw, raw)

        if not merged_raw:
            raise ValueError(f"No configuration data found")

        self._resolve_performance_profile(merged_raw)
        return self._parse(merged_raw)

    def _resolve_performance_profile(self, raw: dict) -> None:
        performance = raw.get("performance")
        if not isinstance(performance, dict) or not performance:
            return
        profiles = performance.get("profiles", {}) or {}
        if not isinstance(profiles, dict):
            raise ValueError("performance.profiles must be a YAML mapping")
        environment = (raw.get("execution_control", {}) or {}).get("environment")
        if profiles and not environment:
            raise ValueError(
                "execution_control.environment is required when performance profiles exist"
            )
        effective = copy.deepcopy({
            key: value for key, value in performance.items() if key != "profiles"
        })
        if environment and profiles:
            if environment not in profiles:
                raise ValueError(
                    f"No performance profile is configured for environment '{environment}'"
                )
            override = profiles[environment] or {}
            if not isinstance(override, dict):
                raise ValueError(
                    f"performance.profiles.{environment} must be a YAML mapping"
                )
            self._deep_merge(effective, copy.deepcopy(override))
        raw["performance"] = effective

    @staticmethod
    def _project_root(master_path: Path) -> Path:
        return (
            master_path.parent.parent
            if master_path.parent.name.lower() == "config"
            else master_path.parent
        )

    @staticmethod
    def _load_connection_environment(master_path: Path, master_raw: dict) -> None:
        """Load the canonical connection .env for every application entry point."""
        pattern = master_raw.get("config_paths", {}).get("connections")
        if not pattern:
            return
        project_root = ConfigLoader._project_root(master_path)
        prefix_text = str(pattern).split("*", 1)[0]
        prefix = Path(prefix_text)
        connection_dir = (
            prefix if prefix_text.endswith(("/", "\\")) else prefix.parent
        )
        if not connection_dir.is_absolute():
            connection_dir = project_root / connection_dir
        env_path = connection_dir / ".env"
        if env_path.is_file():
            # Explicit process/CI variables take precedence over local .env values.
            load_dotenv(env_path, override=False)

    def _legacy_load(self, merged_raw: dict, master_path: Path):
        config_root = master_path.parent if master_path.is_file() else self.config_dir
        for root, _, files in os.walk(config_root):
            for file in sorted(files):
                if file.endswith((".yml", ".yaml")):
                    file_path = Path(root) / file
                    if file_path.absolute() == master_path.absolute():
                        continue
                    raw = normalize_connection_document(self._load_yaml(file_path))
                    if "tables" in raw or raw.get("id"):
                        raw = self._normalize_modular_document(
                            "tables", raw, file_path
                        )
                    raw = _expand_env_vars(raw)
                    self._deep_merge(merged_raw, raw)

    def _load_yaml(self, path: Path) -> dict:
        return load_yaml_document(path)

    @staticmethod
    def _normalize_modular_document(
        section: str,
        raw: dict,
        source_path: Optional[Path] = None,
    ) -> dict:
        if section == "tables":
            if "tables" not in raw and raw.get("id"):
                raw = {"tables": [raw]}
        return raw

    def _deep_merge(self, base: dict, extra: dict):
        for k, v in extra.items():
            if k == "tables" and k in base:
                base[k].extend(v)
            elif isinstance(v, dict) and k in base and isinstance(base[k], dict):
                self._deep_merge(base[k], v)
            else:
                base[k] = v

    def _parse(self, raw: dict) -> MigrationConfig:
        if isinstance(raw.get("performance"), dict) and "profiles" in raw["performance"]:
            self._resolve_performance_profile(raw)
        proj = raw.get("project", {})
        from dataclasses import fields as dc_fields

        forbidden_paths = []
        for scope in ("validations", "comparison_settings", "validation_profiles"):
            forbidden_paths.extend(
                _find_forbidden_execution_key_fields(raw.get(scope, {}), scope)
            )
        if forbidden_paths:
            raise ValueError(
                "Key columns must be configured only with table key_mappings; "
                "remove " + ", ".join(forbidden_paths)
            )
        misplaced_execution = []
        for scope in ("validations", "validation_profiles", "tables"):
            misplaced_execution.extend(
                _find_execution_fields(raw.get(scope, {}), scope)
            )
        if misplaced_execution:
            raise ValueError(
                "Performance settings are allowed only in config/performance/performance.yaml; remove "
                + ", ".join(misplaced_execution)
            )
        
        # Parse all available source connections (without immediate validation)
        sources = {}
        sc_fields = {f.name for f in dc_fields(SourceConfig)}
        for name, cfg in raw.get("sources", {}).items():
            cfg = normalize_connection_config(cfg)
            source_data = {k: v for k, v in cfg.items() if k in sc_fields and k != "name"}
            source_name = cfg.get("name", name)
            sources[name] = SourceConfig(name=source_name, **source_data)

        # Parse all available target connections (without immediate validation)
        targets_dict = raw.get("targets", {})
        targets_dict = {
            name: normalize_connection_config(cfg)
            for name, cfg in targets_dict.items()
        }

        tc_fields = {f.name for f in dc_fields(TargetConfig)}
        targets = {}
        for target_ref_name, t in targets_dict.items():
            target_data = {k: v for k, v in t.items() if k in tc_fields and k != "name"}
            target_name = t.get("name", t.get("type", "Target"))
            targets[target_ref_name] = TargetConfig(name=target_name, **target_data)

        # Resolve execution-level source and target with validation (allows None if auto-selectable)
        execution_source_ref, execution_target_ref = _resolve_source_target_pair(raw, sources, targets)

        # We'll use execution_source_ref and execution_target_ref as defaults for tables
        # but won't use the single resolved TargetConfig yet (it's resolved per-table)

        validations_raw = raw.get("validations", {}) or {}
        if not isinstance(validations_raw, dict):
            raise ValueError("validations must be a YAML mapping")
        validations_raw["incremental_strategy"] = (
            _normalize_incremental_strategy_config(
                validations_raw.get("incremental_strategy")
            )
        )
        data_value_config = validations_raw.get("data_value_validation", {}) or {}
        if isinstance(data_value_config, bool):
            data_value_config = {"enabled": data_value_config, "checks": []}
        if not isinstance(data_value_config, dict):
            raise ValueError(
                "validations.data_value_validation must be a YAML mapping"
            )
        if not isinstance(data_value_config.get("enabled", False), bool):
            raise ValueError(
                "validations.data_value_validation.enabled must be boolean"
            )
        configured_data_value_checks = data_value_config.get("checks", []) or []
        if not isinstance(configured_data_value_checks, list):
            raise ValueError(
                "validations.data_value_validation.checks must be a YAML list"
            )
        seen_data_value_checks = set()
        for index, check in enumerate(configured_data_value_checks):
            if not isinstance(check, dict):
                raise ValueError(
                    "validations.data_value_validation.checks"
                    f"[{index}] must be a YAML mapping"
                )
            check_name = check.get("name")
            if not isinstance(check_name, str) or not check_name.strip():
                raise ValueError(
                    "validations.data_value_validation.checks"
                    f"[{index}].name must be a non-empty string"
                )
            check_name = check_name.strip()
            if check_name in seen_data_value_checks:
                raise ValueError(
                    "Duplicate data value validation check "
                    f"'{check_name}'"
                )
            seen_data_value_checks.add(check_name)
            if not isinstance(check.get("enabled", True), bool):
                raise ValueError(
                    "validations.data_value_validation.checks"
                    f"[{index}].enabled must be boolean"
                )
            _validate_data_value_check_options(check, index)
        edge_case_config = validations_raw.get("edge_case_validation", {}) or {}
        if isinstance(edge_case_config, bool):
            edge_case_config = {"enabled": edge_case_config, "checks": []}
        if not isinstance(edge_case_config, dict):
            raise ValueError(
                "validations.edge_case_validation must be a YAML mapping"
            )
        if not isinstance(edge_case_config.get("enabled", False), bool):
            raise ValueError(
                "validations.edge_case_validation.enabled must be boolean"
            )
        configured_edge_checks = edge_case_config.get("checks", []) or []
        if not isinstance(configured_edge_checks, list):
            raise ValueError(
                "validations.edge_case_validation.checks must be a YAML list"
            )
        seen_edge_checks = set()
        for index, check in enumerate(configured_edge_checks):
            path = f"validations.edge_case_validation.checks[{index}]"
            if not isinstance(check, dict):
                raise ValueError(f"{path} must be a YAML mapping")
            check_name = check.get("name")
            if not isinstance(check_name, str) or not check_name.strip():
                raise ValueError(f"{path}.name must be a non-empty string")
            check_name = check_name.strip()
            if check_name in seen_edge_checks:
                raise ValueError(
                    f"Duplicate edge case validation check '{check_name}'"
                )
            seen_edge_checks.add(check_name)
            if not isinstance(check.get("enabled", True), bool):
                raise ValueError(f"{path}.enabled must be boolean")
        _validate_zero_record_policy(edge_case_config.get("zero_record_policy"))
        _validate_scan_optimization(validations_raw.get("scan_optimization"))
        audit_config = validations_raw.get("audit_validation", {}) or {}
        if not isinstance(audit_config, dict):
            raise ValueError("validations.audit_validation must be a YAML mapping")
        if not isinstance(audit_config.get("enabled", False), bool):
            raise ValueError("validations.audit_validation.enabled must be boolean")
        audit_scope = audit_config.get("scope", {}) or {}
        if not isinstance(audit_scope, dict):
            raise ValueError("validations.audit_validation.scope must be a YAML mapping")
        if audit_scope.get("audit_columns", "target_only") != "target_only":
            raise ValueError(
                "validations.audit_validation.scope.audit_columns must be target_only"
            )
        if audit_scope.get("count_reconciliation", "source_to_target") != "source_to_target":
            raise ValueError(
                "validations.audit_validation.scope.count_reconciliation must be source_to_target"
            )
        audit_columns = audit_config.get("audit_columns", []) or []
        if not isinstance(audit_columns, list) or not all(
            isinstance(column, str) and column.strip() for column in audit_columns
        ):
            raise ValueError(
                "validations.audit_validation.audit_columns must be a YAML list of non-empty strings"
            )
        if len({column.strip() for column in audit_columns}) != len(audit_columns):
            raise ValueError(
                "validations.audit_validation.audit_columns must not contain duplicates"
            )
        audit_checks = audit_config.get("checks", []) or []
        if not isinstance(audit_checks, list):
            raise ValueError("validations.audit_validation.checks must be a YAML list")
        seen_audit_checks = set()
        for index, check in enumerate(audit_checks):
            path = f"validations.audit_validation.checks[{index}]"
            if not isinstance(check, dict):
                raise ValueError(f"{path} must be a YAML mapping")
            check_name = check.get("name")
            if not isinstance(check_name, str) or not check_name.strip():
                raise ValueError(f"{path}.name must be a non-empty string")
            check_name = check_name.strip()
            if check_name in seen_audit_checks:
                raise ValueError(f"Duplicate audit validation check '{check_name}'")
            seen_audit_checks.add(check_name)
            if not isinstance(check.get("enabled", True), bool):
                raise ValueError(f"{path}.enabled must be boolean")
        comparison_settings_raw = raw.get("comparison_settings", {}) or {}
        if not isinstance(comparison_settings_raw, dict):
            raise ValueError("comparison_settings must be a YAML mapping")
        key_mapping_source = comparison_settings_raw.get(
            "key_mapping_source", {"type": "table_yaml"}
        ) or {}
        if (
            not isinstance(key_mapping_source, dict)
            or key_mapping_source.get("type", "table_yaml") != "table_yaml"
        ):
            raise ValueError(
                "comparison_settings.key_mapping_source.type must be table_yaml"
            )
        if "path" in key_mapping_source and not (
            isinstance(key_mapping_source["path"], str)
            and key_mapping_source["path"].strip()
        ):
            raise ValueError(
                "comparison_settings.key_mapping_source.path must be a non-empty string"
            )
        sampling_defaults = comparison_settings_raw.get("sampling", {}) or {}
        if not isinstance(sampling_defaults, dict):
            raise ValueError("comparison_settings.sampling must be a YAML mapping")
        default_sample_size = sampling_defaults.get("default_sample_size")
        if comparison_settings_raw and not isinstance(
            sampling_defaults.get("enabled"), bool
        ):
            raise ValueError(
                "comparison_settings.sampling.enabled must be boolean"
            )
        if sampling_defaults.get("enabled") and not (
            isinstance(default_sample_size, int)
            and not isinstance(default_sample_size, bool)
            and default_sample_size > 0
        ):
            raise ValueError(
                "comparison_settings.sampling.default_sample_size must be a "
                "positive integer"
            )
        large_table_sampling = comparison_settings_raw.get(
            "large_table_sampling"
        )
        if comparison_settings_raw:
            if not isinstance(large_table_sampling, dict):
                raise ValueError(
                    "comparison_settings.large_table_sampling must be a YAML mapping"
                )
            if not isinstance(large_table_sampling.get("enabled"), bool):
                raise ValueError(
                    "comparison_settings.large_table_sampling.enabled must be boolean"
                )
            for field_name in ("row_count_threshold", "sample_size"):
                value = large_table_sampling.get(field_name)
                if not (
                    isinstance(value, int)
                    and not isinstance(value, bool)
                    and value > 0
                ):
                    raise ValueError(
                        "comparison_settings.large_table_sampling."
                        f"{field_name} must be a positive integer"
                    )
        missing_key_behavior = comparison_settings_raw.get("missing_key_behavior")
        if missing_key_behavior is not None:
            if not isinstance(missing_key_behavior, dict):
                raise ValueError(
                    "comparison_settings.missing_key_behavior must be a YAML mapping"
                )
            allowed_missing_key_actions = {
                "hash_comparison", "hash_full_comparison", "random_sampling",
                "full_scan", "skip", "error"
            }
            missing_key_fields = ["full_data_comparison"]
            if sampling_defaults.get("enabled"):
                missing_key_fields.append("deterministic_sampling")
            for field_name in missing_key_fields:
                action = missing_key_behavior.get(field_name)
                if action not in allowed_missing_key_actions:
                    raise ValueError(
                        f"comparison_settings.missing_key_behavior.{field_name} must be "
                        + ", ".join(sorted(allowed_missing_key_actions))
                    )
            allowed_missing_key_statuses = {
                "fallback_status": {"PASS", "WARNING", "WARN"},
                "skip_status": {"SKIP"},
                "error_status": {"ERROR", "FRAMEWORK_ERROR", "CONFIG_ERROR"},
            }
            for field_name, allowed_statuses in allowed_missing_key_statuses.items():
                if missing_key_behavior.get(field_name) not in allowed_statuses:
                    raise ValueError(
                        f"comparison_settings.missing_key_behavior.{field_name} has an unsupported status"
                    )
        null_handling = comparison_settings_raw.get("null_handling")
        if null_handling is not None:
            if not isinstance(null_handling, dict):
                raise ValueError("comparison_settings.null_handling must be a YAML mapping")
            if not isinstance(null_handling.get("blank_is_null"), bool):
                raise ValueError(
                    "comparison_settings.null_handling.blank_is_null must be boolean"
                )
            if null_handling.get("both_null") not in {"match", "mismatch"}:
                raise ValueError(
                    "comparison_settings.null_handling.both_null must be match or mismatch"
                )
            if null_handling.get("one_null") not in {"match", "mismatch"}:
                raise ValueError(
                    "comparison_settings.null_handling.one_null must be match or mismatch"
                )
        timestamp_handling = comparison_settings_raw.get("timestamp_handling")
        if timestamp_handling is not None:
            if not isinstance(timestamp_handling, dict):
                raise ValueError(
                    "comparison_settings.timestamp_handling must be a YAML mapping"
                )
            for field_name in ("parse_iso_strings", "preserve_precision"):
                if not isinstance(timestamp_handling.get(field_name), bool):
                    raise ValueError(
                        f"comparison_settings.timestamp_handling.{field_name} must be boolean"
                    )
            if timestamp_handling.get("out_of_range_strategy") not in {
                "native_or_string", "error"
            }:
                raise ValueError(
                    "comparison_settings.timestamp_handling.out_of_range_strategy must be native_or_string or error"
                )
            if timestamp_handling.get("timezone_comparison") not in {
                "preserve", "ignore"
            }:
                raise ValueError(
                    "comparison_settings.timestamp_handling.timezone_comparison must be preserve or ignore"
                )
            if timestamp_handling.get("precision_loss_behavior") not in {
                "mismatch", "tolerance"
            }:
                raise ValueError(
                    "comparison_settings.timestamp_handling.precision_loss_behavior must be mismatch or tolerance"
                )
            try:
                fractional_tolerance = Decimal(
                    str(timestamp_handling.get("fractional_second_tolerance"))
                )
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise ValueError(
                    "comparison_settings.timestamp_handling.fractional_second_tolerance must be a non-negative decimal"
                ) from exc
            if fractional_tolerance < 0:
                raise ValueError(
                    "comparison_settings.timestamp_handling.fractional_second_tolerance must be a non-negative decimal"
                )
        hash_comparison = comparison_settings_raw.get("hash_comparison")
        if hash_comparison is not None:
            if not isinstance(hash_comparison, dict):
                raise ValueError(
                    "comparison_settings.hash_comparison must be a YAML mapping"
                )
            if hash_comparison.get("algorithm") not in {
                "sha256", "sha384", "sha512", "blake2b"
            }:
                raise ValueError(
                    "comparison_settings.hash_comparison.algorithm is unsupported"
                )
            if hash_comparison.get("comparison_scope") != "configured_columns":
                raise ValueError(
                    "comparison_settings.hash_comparison.comparison_scope must be configured_columns"
                )
            if hash_comparison.get("duplicate_handling") != "multiset":
                raise ValueError(
                    "comparison_settings.hash_comparison.duplicate_handling must be multiset"
                )
            exclusions = hash_comparison.get("column_exclusions")
            if not isinstance(exclusions, list) or not all(
                isinstance(item, str) and item.strip() for item in exclusions
            ):
                raise ValueError(
                    "comparison_settings.hash_comparison.column_exclusions must be a list of column names"
                )
            hash_batch_size = hash_comparison.get("batch_size")
            if not (
                isinstance(hash_batch_size, int)
                and not isinstance(hash_batch_size, bool)
                and hash_batch_size > 0
            ):
                raise ValueError(
                    "comparison_settings.hash_comparison.batch_size must be a positive integer"
                )
            if not isinstance(hash_comparison.get("parallel_ingest", False), bool):
                raise ValueError(
                    "comparison_settings.hash_comparison.parallel_ingest must be boolean"
                )
            for field_name in ("ingest_workers", "progress_interval_percent"):
                value = hash_comparison.get(field_name, 2 if field_name == "ingest_workers" else 25)
                if not (
                    isinstance(value, int)
                    and not isinstance(value, bool)
                    and value > 0
                ):
                    raise ValueError(
                        f"comparison_settings.hash_comparison.{field_name} must be positive"
                    )
            if int(hash_comparison.get("ingest_workers", 2)) > 2:
                raise ValueError(
                    "comparison_settings.hash_comparison.ingest_workers cannot exceed 2"
                )
            adaptive_hash = hash_comparison.get("adaptive_batching") or {}
            if not isinstance(adaptive_hash, dict):
                raise ValueError(
                    "comparison_settings.hash_comparison.adaptive_batching must be a YAML mapping"
                )
            if adaptive_hash and not isinstance(adaptive_hash.get("enabled"), bool):
                raise ValueError(
                    "comparison_settings.hash_comparison.adaptive_batching.enabled must be boolean"
                )
            for tier in adaptive_hash.get("tiers", []):
                if not isinstance(tier, dict) or not all(
                    isinstance(tier.get(name), int)
                    and not isinstance(tier.get(name), bool)
                    and tier[name] > 0
                    for name in ("max_rows", "batch_size")
                ):
                    raise ValueError(
                        "comparison_settings.hash_comparison.adaptive_batching.tiers "
                        "must contain positive max_rows and batch_size values"
                    )
            large_hash_batch = adaptive_hash.get("large_table_batch_size")
            if large_hash_batch is not None and not (
                isinstance(large_hash_batch, int)
                and not isinstance(large_hash_batch, bool)
                and large_hash_batch > 0
            ):
                raise ValueError(
                    "comparison_settings.hash_comparison.adaptive_batching."
                    "large_table_batch_size must be positive"
                )
            normalization = hash_comparison.get("normalization")
            if not isinstance(normalization, dict):
                raise ValueError(
                    "comparison_settings.hash_comparison.normalization must be a YAML mapping"
                )
            if not isinstance(normalization.get("null_representation"), str):
                raise ValueError(
                    "comparison_settings.hash_comparison.normalization.null_representation must be a string"
                )
            for field_name in (
                "trim_strings", "case_sensitive", "normalize_datatypes"
            ):
                if not isinstance(normalization.get(field_name), bool):
                    raise ValueError(
                        f"comparison_settings.hash_comparison.normalization.{field_name} must be boolean"
                    )
            allowed_normalization = {
                "blank_handling": {"preserve", "null"},
                "unicode_normalization": {"none", "NFC", "NFD", "NFKC", "NFKD"},
                "numeric_normalization": {"canonical_decimal", "preserve"},
                "date_normalization": {"iso8601"},
                "timestamp_normalization": {"precision_aware"},
                "timezone_normalization": {"preserve", "ignore"},
                "precision_normalization": {"preserve", "trim_trailing_zeros"},
            }
            for field_name, allowed in allowed_normalization.items():
                if normalization.get(field_name) not in allowed:
                    raise ValueError(
                        f"comparison_settings.hash_comparison.normalization.{field_name} is unsupported"
                    )
            spill = hash_comparison.get("spill")
            if spill is not None:
                if not isinstance(spill, dict):
                    raise ValueError(
                        "comparison_settings.hash_comparison.spill must be a "
                        "YAML mapping"
                    )
                unknown_spill = sorted(
                    set(spill) - {
                        "strategy", "directory", "store_row_payloads",
                        "commit_interval_batches",
                    }
                )
                if unknown_spill:
                    raise ValueError(
                        "comparison_settings.hash_comparison.spill has "
                        "unsupported field(s): " + ", ".join(unknown_spill)
                    )
                if spill.get("strategy", "sqlite") not in SPILL_STRATEGIES:
                    raise ValueError(
                        "comparison_settings.hash_comparison.spill.strategy must "
                        "be one of: " + ", ".join(SPILL_STRATEGIES)
                    )
                if spill.get("store_row_payloads", "bounded") not in PAYLOAD_POLICIES:
                    raise ValueError(
                        "comparison_settings.hash_comparison.spill."
                        "store_row_payloads must be one of: "
                        + ", ".join(PAYLOAD_POLICIES)
                    )
                commit_interval = spill.get("commit_interval_batches", 20)
                if not (
                    isinstance(commit_interval, int)
                    and not isinstance(commit_interval, bool)
                    and commit_interval > 0
                ):
                    raise ValueError(
                        "comparison_settings.hash_comparison.spill."
                        "commit_interval_batches must be positive"
                    )
                directory = spill.get("directory")
                if directory is not None and not (
                    isinstance(directory, str) and directory.strip()
                ):
                    raise ValueError(
                        "comparison_settings.hash_comparison.spill.directory must "
                        "be a non-empty string"
                    )
        full_comparison = comparison_settings_raw.get("full_data_comparison")
        if full_comparison is not None:
            if not isinstance(full_comparison, dict) or not isinstance(
                full_comparison.get("enabled"), bool
            ):
                raise ValueError(
                    "comparison_settings.full_data_comparison must configure enabled"
                )
            for field_name in (
                "mode_name", "validation_scope_source", "consistency_requirement",
            ):
                if not isinstance(full_comparison.get(field_name), str) or not full_comparison[field_name].strip():
                    raise ValueError(
                        f"comparison_settings.full_data_comparison.{field_name} must be a non-empty string"
                    )
            for section in (
                "key_based", "hash_fallback", "batching", "evidence",
                "count_names", "excel_reports",
            ):
                if not isinstance(full_comparison.get(section), dict):
                    raise ValueError(
                        f"comparison_settings.full_data_comparison.{section} must be a YAML mapping"
                    )
            fallback = full_comparison["hash_fallback"]
            if not isinstance(fallback.get("enabled"), bool):
                raise ValueError(
                    "comparison_settings.full_data_comparison.hash_fallback.enabled must be boolean"
                )
            if fallback.get("enabled") and missing_key_behavior.get(
                "full_data_comparison"
            ) != fallback.get("strategy"):
                raise ValueError(
                    "Enabled hash fallback must be the configured full_data_comparison missing-key action"
                )
            for field_name in (
                "strategy", "strategy_name", "warning_status", "warning_code",
                "warning_message",
            ):
                if not isinstance(fallback.get(field_name), str) or not fallback[field_name].strip():
                    raise ValueError(
                        "comparison_settings.full_data_comparison.hash_fallback."
                        f"{field_name} must be a non-empty string"
                    )
            if fallback["warning_status"] not in {
                "PASS", "FAIL", "WARN", "WARNING", "SKIP", "ERROR",
                "FRAMEWORK_ERROR", "CONFIG_ERROR",
            }:
                raise ValueError(
                    "comparison_settings.full_data_comparison.hash_fallback.warning_status is unsupported"
                )
            if not isinstance(fallback.get("trigger_conditions"), list) or not fallback["trigger_conditions"]:
                raise ValueError(
                    "comparison_settings.full_data_comparison.hash_fallback.trigger_conditions must be a non-empty list"
                )
            required_fallback_triggers = {
                "missing", "empty", "invalid", "nulls", "duplicates", "non_unique",
            }
            if not required_fallback_triggers.issubset(
                set(fallback["trigger_conditions"])
            ):
                raise ValueError(
                    "comparison_settings.full_data_comparison.hash_fallback."
                    "trigger_conditions must cover missing, empty, invalid, nulls, "
                    "duplicates, and non_unique keys"
                )
            key_based = full_comparison["key_based"]
            for field_name in (
                "enabled", "validate_nulls", "validate_duplicates",
                "require_unique_keys",
            ):
                if not isinstance(key_based.get(field_name), bool):
                    raise ValueError(
                        "comparison_settings.full_data_comparison.key_based."
                        f"{field_name} must be boolean"
                    )
            if not isinstance(key_based.get("strategy_name"), str) or not key_based["strategy_name"].strip():
                raise ValueError(
                    "comparison_settings.full_data_comparison.key_based.strategy_name must be a non-empty string"
                )
            batching = full_comparison["batching"]
            if not (
                isinstance(batching.get("batch_size"), int)
                and not isinstance(batching.get("batch_size"), bool)
                and batching["batch_size"] > 0
            ):
                raise ValueError(
                    "comparison_settings.full_data_comparison.batching.batch_size must be positive"
                )
            for field_name in (
                "streaming_enabled", "database_pushdown_enabled",
                "memory_safe_processing", "retry_failed_batch",
            ):
                if not isinstance(batching.get(field_name), bool):
                    raise ValueError(
                        f"comparison_settings.full_data_comparison.batching.{field_name} must be boolean"
                    )
            parallel = batching.get("parallel_execution")
            if not isinstance(parallel, dict) or not isinstance(
                parallel.get("enabled"), bool
            ):
                raise ValueError(
                    "comparison_settings.full_data_comparison.batching."
                    "parallel_execution.enabled must be boolean"
                )
            workers = parallel.get("worker_count")
            if not isinstance(workers, int) or isinstance(workers, bool) or workers < 1:
                raise ValueError(
                    "comparison_settings.full_data_comparison.batching."
                    "parallel_execution.worker_count must be positive"
                )
            retries = batching.get("max_retries")
            if not isinstance(retries, int) or isinstance(retries, bool) or retries < 0:
                raise ValueError(
                    "comparison_settings.full_data_comparison.batching.max_retries must be non-negative"
                )
            # FIX: Make spill_directory optional (defaults to output/temp if not configured)
            spill_dir = batching.get("spill_directory")
            if spill_dir is not None:
                if not isinstance(spill_dir, str) or not spill_dir.strip():
                    raise ValueError(
                        "comparison_settings.full_data_comparison.batching.spill_directory "
                        "must be a non-empty string or null (defaults to output/temp)"
                    )
            cohort_strategy = batching.get("key_cohort_strategy", "auto")
            if cohort_strategy not in KEY_COHORT_STRATEGIES:
                raise ValueError(
                    "comparison_settings.full_data_comparison.batching."
                    "key_cohort_strategy must be one of: "
                    + ", ".join(KEY_COHORT_STRATEGIES)
                )
            cohort_chunk = batching.get("cohort_chunk_size", batching["batch_size"])
            if (
                isinstance(cohort_chunk, bool)
                or not isinstance(cohort_chunk, int)
                or cohort_chunk <= 0
            ):
                raise ValueError(
                    "comparison_settings.full_data_comparison.batching."
                    "cohort_chunk_size must be a positive integer"
                )
            if "reuse_connections" in batching and not isinstance(
                batching["reuse_connections"], bool
            ):
                raise ValueError(
                    "comparison_settings.full_data_comparison.batching."
                    "reuse_connections must be boolean"
                )
            reverse_pass = full_comparison.get("reverse_pass")
            if reverse_pass is not None:
                if not isinstance(reverse_pass, dict):
                    raise ValueError(
                        "comparison_settings.full_data_comparison.reverse_pass "
                        "must be a YAML mapping"
                    )
                unknown_reverse = sorted(
                    set(reverse_pass) - {"mode", "require_shared_statistics"}
                )
                if unknown_reverse:
                    raise ValueError(
                        "comparison_settings.full_data_comparison.reverse_pass has "
                        "unsupported field(s): " + ", ".join(unknown_reverse)
                    )
                if reverse_pass.get("mode", "always") not in REVERSE_PASS_MODES:
                    raise ValueError(
                        "comparison_settings.full_data_comparison.reverse_pass.mode "
                        "must be one of: " + ", ".join(REVERSE_PASS_MODES)
                    )
                if not isinstance(
                    reverse_pass.get("require_shared_statistics", True), bool
                ):
                    raise ValueError(
                        "comparison_settings.full_data_comparison.reverse_pass."
                        "require_shared_statistics must be boolean"
                    )
            evidence = full_comparison["evidence"]
            for field_name in (
                "export_matched_records", "export_all_excess_source",
                "export_all_excess_target",
            ):
                if not isinstance(evidence.get(field_name), bool):
                    raise ValueError(
                        "comparison_settings.full_data_comparison.evidence."
                        f"{field_name} must be boolean"
                    )
            for field_name in (
                "matched_records_limit", "mismatch_sample_limit",
                "excess_source_limit", "excess_target_limit",
            ):
                value = evidence.get(field_name)
                if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                    raise ValueError(
                        f"comparison_settings.full_data_comparison.evidence.{field_name} must be non-negative"
                    )
            for field_name in (
                "matched", "mismatched", "excess_in_source", "excess_in_target",
            ):
                value = full_comparison["count_names"].get(field_name)
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(
                        "comparison_settings.full_data_comparison.count_names."
                        f"{field_name} must be a non-empty string"
                    )
        error_policy = comparison_settings_raw.get("error_policy")
        if error_policy is not None:
            if not isinstance(error_policy, dict):
                raise ValueError("comparison_settings.error_policy must be a YAML mapping")
            if not isinstance(error_policy.get("continue_after_check_error"), bool):
                raise ValueError(
                    "comparison_settings.error_policy.continue_after_check_error must be boolean"
                )
            if not isinstance(error_policy.get("fatal_exception_categories"), list):
                raise ValueError(
                    "comparison_settings.error_policy.fatal_exception_categories must be a list"
                )
            allowed_error_statuses = {"ERROR", "FRAMEWORK_ERROR", "CONFIG_ERROR"}
            for field_name in (
                "framework_exception_status", "configuration_error_status"
            ):
                if error_policy.get(field_name) not in allowed_error_statuses:
                    raise ValueError(
                        f"comparison_settings.error_policy.{field_name} has an unsupported status"
                    )
        status_policy = comparison_settings_raw.get("status_policy")
        if status_policy is not None:
            if not isinstance(status_policy, dict):
                raise ValueError("comparison_settings.status_policy must be a YAML mapping")
            severity_order = status_policy.get("severity_order")
            table_impact = status_policy.get("table_impact")
            supported_statuses = {
                "PASS", "FAIL", "WARN", "WARNING", "SKIP", "ERROR",
                "FRAMEWORK_ERROR", "CONFIG_ERROR",
            }
            if (
                not isinstance(severity_order, list)
                or not severity_order
                or len(set(severity_order)) != len(severity_order)
                or not set(severity_order) <= supported_statuses
            ):
                raise ValueError(
                    "comparison_settings.status_policy.severity_order must be a unique status list"
                )
            if (
                not isinstance(table_impact, dict)
                or not set(table_impact) <= supported_statuses
                or not set(table_impact.values()) <= supported_statuses
            ):
                raise ValueError(
                    "comparison_settings.status_policy.table_impact must map supported statuses"
                )

        vp_fields = {f.name for f in dc_fields(ValidationProfile)}
        profiles_raw = raw.get("validation_profiles")
        if profiles_raw is None:
            legacy_validations = raw.get("validations", {}) or {}
            profiles_raw = {
                profile_name: legacy_validations[profile_name]
                for profile_name in VALIDATION_PROFILE_ORDER
                if profile_name in legacy_validations
            }
        if not isinstance(profiles_raw, dict):
            raise ValueError("validation_profiles must be a YAML mapping")
        profiles = {}
        for name in _ordered_profile_names(profiles_raw):
            profile_cfg = _normalize_profile_config(
                name,
                profiles_raw[name],
                validations_raw,
            )
            profiles[name] = ValidationProfile(
                name=name,
                **{k: v for k, v in profile_cfg.items() if k in vp_fields}
            )

        tm_fields = {f.name for f in dc_fields(TableMapping)}
        tables = []
        raw_tables = raw.get("tables", []) or []
        if not isinstance(raw_tables, list):
            raise ValueError("tables must be a YAML list")
        seen_table_ids = set()
        for tbl in raw_tables:
            tbl = _normalize_table_config(tbl)
            if not tbl.get("enabled", True):
                continue
            table_id = tbl.get("id")
            if not isinstance(table_id, str) or not table_id.strip():
                raise ValueError("Every enabled table configuration requires a non-empty id")
            if table_id in seen_table_ids:
                raise ValueError(f"Duplicate table configuration id '{table_id}'")
            seen_table_ids.add(table_id)

            # Resolve source connection with fallback: table.source_system → execution.source → auto-select
            table_source_ref = tbl.get("source_system")
            resolved_source_ref = _resolve_connection_for_table(
                table_source_ref,
                execution_source_ref,
                sources,
                connection_role="source"
            )
            tbl["source_system"] = resolved_source_ref

            # Resolve target connection with fallback: table.target_system → execution.target → auto-select
            table_target_ref = tbl.get("target_system")
            resolved_target_ref = _resolve_connection_for_table(
                table_target_ref,
                execution_target_ref,
                targets,
                connection_role="target"
            )
            tbl["target_system"] = resolved_target_ref

            if "layer" in tbl:
                if "target_layer" in tbl and tbl["target_layer"] != tbl["layer"]:
                    raise ValueError(
                        f"Table '{tbl.get('id', '<missing>')}' defines conflicting "
                        "layer and target_layer values"
                    )
                tbl["target_layer"] = tbl.pop("layer")
            if not isinstance(tbl.get("target_layer"), str) or not tbl["target_layer"].strip():
                raise ValueError(
                    f"Table '{tbl.get('id', '<missing>')}' requires a non-empty layer"
                )
            tbl.setdefault("migration_phase", "full_load")
            validation_phases = tbl.get("validation_phases")
            if validation_phases is None:
                # Backward compatibility for existing table documents: their
                # configured migration_phase remains their sole eligible phase.
                validation_phases = [tbl["migration_phase"]]
            if not isinstance(validation_phases, list) or not validation_phases or not all(
                isinstance(phase, str) and phase.strip()
                for phase in validation_phases
            ):
                raise ValueError(
                    f"Table '{table_id}' validation_phases must be a list of "
                    "non-empty phase names"
                )
            tbl["validation_phases"] = list(dict.fromkeys(
                phase.strip() for phase in validation_phases
            ))

            # Backward-compatible generic table context. It is applied only
            # to the side(s) using SQL Server; explicit role-specific fields win.
            generic_database = tbl.get("database")
            generic_schema = tbl.get("schema")
            generic_table = tbl.get("table_name", tbl.get("table"))
            source_is_sql = re.sub(
                r"[\s_-]+", "", sources[resolved_source_ref].type.lower()
            ) in {"sql", "sqlserver", "mssql"}
            target_is_sql = re.sub(
                r"[\s_-]+", "", targets[resolved_target_ref].type.lower()
            ) in {"sql", "sqlserver", "mssql"}
            if source_is_sql:
                if generic_database is not None and "source_database" not in tbl:
                    tbl["source_database"] = generic_database
                if generic_schema and not tbl.get("source_schema"):
                    tbl["source_schema"] = generic_schema
                if generic_table and not tbl.get("source_table"):
                    tbl["source_table"] = generic_table
            if target_is_sql:
                if generic_database is not None and "target_database" not in tbl:
                    tbl["target_database"] = generic_database
                if generic_schema and not tbl.get("target_schema"):
                    tbl["target_schema"] = generic_schema
                if generic_table and not tbl.get("target_table"):
                    tbl["target_table"] = generic_table

            tables.append(TableMapping(**{k: v for k, v in tbl.items() if k in tm_fields}))

        for table in tables:
            if table.source_system not in sources:
                raise ValueError(
                    f"Table '{table.id}' references unknown source connection "
                    f"'{table.source_system}'"
                )
            if not sources[table.source_system].enabled:
                raise ValueError(
                    f"Table '{table.id}' references disabled source connection "
                    f"'{table.source_system}'"
                )
            if profiles and table.migration_phase not in profiles:
                raise ValueError(
                    f"Table '{table.id}' references unknown validation profile "
                    f"'{table.migration_phase}'"
                )
            unknown_eligible_phases = sorted(
                set(table.validation_phases) - set(profiles)
            )
            if profiles and unknown_eligible_phases:
                raise ValueError(
                    f"Table '{table.id}' validation_phases references unknown "
                    "validation profile(s): " + ", ".join(unknown_eligible_phases)
                )

        thr = raw.get("thresholds", {})
        thresholds = Thresholds(
            row_count_tolerance_pct=thr.get("row_count_tolerance", {}).get("tolerance_pct", thr.get("row_count_tolerance_pct", 0.0)),
            aggregate_tolerance_pct=thr.get("aggregate_tolerance", {}).get("tolerance_pct", thr.get("aggregate_tolerance_pct", 0.001)),
        )

        rep = raw.get("reporting", {}) or {}
        # `project.output_dir` is deliberately not read: every report path is
        # resolved from `report_output` by reporters.report_paths, so a second
        # output-folder configuration would create a second output tree.
        reporting = ReportingConfig(
            formats=rep.get("formats", ["html", "excel", "console", "json"]),
            settings=copy.deepcopy(rep) if isinstance(rep, dict) else {},
        )

        rd = raw.get("run_defaults", {})
        if not isinstance(rd, dict):
            raise ValueError("run_defaults must be a YAML mapping")
        if not isinstance(rd.get("report_overrides", True), bool):
            raise ValueError("run_defaults.report_overrides must be boolean")
        run_defaults = RunDefaults(
            active_checks=rd.get("active_checks"),
            always_run=rd.get("always_run", []),
            always_skip=rd.get("always_skip", []),
            report_overrides=bool(rd.get("report_overrides", True)),
        )

        exe = raw.get("execution", {}) or {}
        migrated_top_level = {
            "parallel_execution", "performance_monitoring", "batch_processing",
            "key_validation", "large_table", "connection_pool",
            "database_protection", "scalable_execution_required",
        }
        detected_old = sorted(migrated_top_level.intersection(raw))
        detected_execution = sorted(set(exe) - {"validation_categories"})
        if detected_old or detected_execution:
            paths = detected_old + [f"execution.{name}" for name in detected_execution]
            raise ValueError(
                "Performance settings have moved to config/performance/performance.yaml; remove "
                + ", ".join(paths)
            )
        performance = raw.get("performance")
        if not isinstance(performance, dict) or not performance:
            raise ValueError(
                "Missing required performance configuration. Set "
                "config_paths.performance to config/performance/performance.yaml"
            )
        performance_execution = performance.get("execution")
        if not isinstance(performance_execution, dict):
            raise ValueError("performance.execution must be a YAML mapping")
        supported_categories = {
            "migration_validation",
            "data_quality_validation",
            "business_rule_validation",
            "data_value_validation",
            "edge_case_validation",
            "audit_validation",
        }
        validation_categories = exe.get(
            "validation_categories", sorted(supported_categories)
        )
        if not isinstance(validation_categories, list) or not validation_categories:
            raise ValueError(
                "execution.validation_categories must be a non-empty list"
            )
        unknown_categories = sorted(
            set(validation_categories) - supported_categories
        )
        if unknown_categories:
            raise ValueError(
                "Unsupported execution.validation_categories: "
                + ", ".join(unknown_categories)
            )
        scalable_section_names = (
            "parallel_execution",
            "batch_processing",
            "key_validation",
            "table_strategy",
            "connection_management",
            "query_execution",
            "timing",
            "diagnostics",
        )
        scalable_mode = True
        if scalable_mode:
            missing_sections = [
                name for name in scalable_section_names if name not in performance
            ]
            if missing_sections:
                raise ValueError(
                    "Unsafe execution configuration: missing required section(s) in "
                    "performance.yaml: " + ", ".join(missing_sections)
                )

        def required(section, field_name):
            if field_name not in section:
                raise ValueError(
                    "Unsafe execution configuration: missing required value "
                    f"'{field_name}' in performance.yaml"
                )
            return section[field_name]

        def positive_int(value, path):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{path} must be a positive integer")
            return value

        def nonnegative_int(value, path):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{path} must be a non-negative integer")
            return value

        def boolean(value, path):
            if not isinstance(value, bool):
                raise ValueError(f"{path} must be boolean")
            return value

        if scalable_mode:
            for field_name in ("engine", "fallback_enabled", "allowed_engines", "strategies"):
                required(performance_execution, field_name)
            parallel_raw = performance["parallel_execution"]
            table_parallel_raw = required(parallel_raw, "table_parallelism")
            validation_parallel_raw = required(parallel_raw, "validation_parallelism")
            source_target_parallel_raw = required(parallel_raw, "source_target_parallelism")
            batch_parallel_raw = required(parallel_raw, "batch_parallelism")
            batch_raw = performance["batch_processing"]
            key_validation_raw = performance["key_validation"]
            table_strategy_raw = performance["table_strategy"]
            large_raw = required(table_strategy_raw, "large_table")
            connection_management_raw = performance["connection_management"]
            pool_raw = required(connection_management_raw, "pool")
            protection_raw = performance["query_execution"]
            retry_raw = required(protection_raw, "retry")
            checkpoint_raw = raw["checkpoint"]
            for name, value in (
                ("parallel_execution", parallel_raw),
                ("parallel_execution.table_parallelism", table_parallel_raw),
                ("parallel_execution.validation_parallelism", validation_parallel_raw),
                ("parallel_execution.source_target_parallelism", source_target_parallel_raw),
                ("parallel_execution.batch_parallelism", batch_parallel_raw),
                ("batch_processing", batch_raw),
                ("key_validation", key_validation_raw),
                ("table_strategy", table_strategy_raw),
                ("large_table", large_raw),
                ("connection_management", connection_management_raw),
                ("connection_pool", pool_raw),
                ("database_protection", protection_raw),
                ("database_protection.retry", retry_raw),
                ("checkpoint", checkpoint_raw),
            ):
                if not isinstance(value, dict):
                    raise ValueError(f"{name} must be a YAML mapping")
            executor_type = required(table_parallel_raw, "executor_type")
            if executor_type not in {"thread", "process"}:
                raise ValueError(
                    "parallel_execution.table_parallelism.executor_type must be "
                    "'thread' or 'process'"
                )
            strategy = required(batch_raw, "strategy")
            supported_batch_strategies = tuple(item.value for item in BatchStrategy)
            if strategy not in supported_batch_strategies:
                raise ValueError(
                    "batch_processing.strategy must be one of: "
                    + ", ".join(supported_batch_strategies)
                )
            parallel_config = ParallelExecutionConfig(
                enabled=boolean(
                    required(parallel_raw, "enabled"), "parallel_execution.enabled"
                ),
                table_parallelism=TableParallelismConfig(
                    enabled=boolean(
                        required(table_parallel_raw, "enabled"),
                        "parallel_execution.table_parallelism.enabled",
                    ),
                    max_workers=positive_int(
                        required(table_parallel_raw, "max_workers"),
                        "parallel_execution.table_parallelism.max_workers",
                    ),
                    executor_type=executor_type,
                    fail_fast=boolean(
                        required(table_parallel_raw, "fail_fast"),
                        "parallel_execution.table_parallelism.fail_fast",
                    ),
                ),
                validation_parallelism=ValidationParallelismConfig(
                    enabled=boolean(
                        required(validation_parallel_raw, "enabled"),
                        "parallel_execution.validation_parallelism.enabled",
                    ),
                    max_workers_per_table=positive_int(
                        required(validation_parallel_raw, "max_workers_per_table"),
                        "parallel_execution.validation_parallelism.max_workers_per_table",
                    ),
                ),
                source_target_parallelism=SourceTargetParallelismConfig(
                    enabled=boolean(
                        required(source_target_parallel_raw, "enabled"),
                        "parallel_execution.source_target_parallelism.enabled",
                    ),
                    max_workers_per_validation=positive_int(
                        required(source_target_parallel_raw, "max_workers_per_validation"),
                        "parallel_execution.source_target_parallelism.max_workers_per_validation",
                    ),
                ),
                batch_parallelism=BatchParallelismConfig(
                    enabled=boolean(
                        required(batch_parallel_raw, "enabled"),
                        "parallel_execution.batch_parallelism.enabled",
                    ),
                    max_workers_per_table=positive_int(
                        required(batch_parallel_raw, "max_workers_per_table"),
                        "parallel_execution.batch_parallelism.max_workers_per_table",
                    ),
                ),
                max_concurrent_validations=positive_int(
                    required(parallel_raw, "max_concurrent_validations"),
                    "parallel_execution.max_concurrent_validations",
                ),
                global_max_concurrency=nonnegative_int(
                    required(parallel_raw, "global_max_concurrency"),
                    "parallel_execution.global_max_concurrency",
                ),
            )
            batch_config = BatchProcessingConfig(
                enabled=boolean(
                    required(batch_raw, "enabled"), "batch_processing.enabled"
                ),
                batch_size=positive_int(
                    required(batch_raw, "batch_size"),
                    "batch_processing.batch_size",
                ),
                max_rows_in_memory=positive_int(
                    required(batch_raw, "max_rows_in_memory"),
                    "batch_processing.max_rows_in_memory",
                ),
                strategy=strategy,
                retry_failed_batch=boolean(
                    required(batch_raw, "retry_failed_batch"),
                    "batch_processing.retry_failed_batch",
                ),
                max_retries=nonnegative_int(
                    required(batch_raw, "max_retries"),
                    "batch_processing.max_retries",
                ),
                adaptive_batching=_build_adaptive_batching(
                    batch_raw.get("adaptive_batching")
                ),
            )
            if batch_config.batch_size * 2 > batch_config.max_rows_in_memory:
                raise ValueError(
                    "batch_processing.max_rows_in_memory must hold one source "
                    "and one target batch (at least 2 * batch_size)"
                )
            adaptive = batch_config.adaptive_batching
            if adaptive.enabled:
                largest = max(
                    adaptive.maximum_batch_size,
                    adaptive.initial_batch_size,
                    adaptive.minimum_batch_size,
                )
                if largest * 2 > batch_config.max_rows_in_memory:
                    raise ValueError(
                        "batch_processing.adaptive_batching sizes must respect "
                        "max_rows_in_memory (at most max_rows_in_memory / 2)"
                    )
            key_validation_config = KeyValidationExecutionConfig(
                full_scan=boolean(
                    required(key_validation_raw, "full_scan"),
                    "key_validation.full_scan",
                ),
                partition_size=positive_int(
                    required(key_validation_raw, "partition_size"),
                    "key_validation.partition_size",
                ),
                max_workers=positive_int(
                    required(key_validation_raw, "max_workers"),
                    "key_validation.max_workers",
                ),
                parallel_execution=boolean(
                    required(key_validation_raw, "parallel_execution"),
                    "key_validation.parallel_execution",
                ),
                reuse_connections=boolean(
                    required(key_validation_raw, "reuse_connections"),
                    "key_validation.reuse_connections",
                ),
                key_cohort_strategy=key_validation_raw.get(
                    "key_cohort_strategy", "auto"
                ),
            )
            if key_validation_config.key_cohort_strategy not in KEY_COHORT_STRATEGIES:
                raise ValueError(
                    "key_validation.key_cohort_strategy must be one of: "
                    + ", ".join(KEY_COHORT_STRATEGIES)
                )
            if not key_validation_config.full_scan:
                raise ValueError(
                    "key_validation.full_scan must be true; key validation does not support sampling"
                )
            if boolean(
                required(key_validation_raw, "sampling_enabled"),
                "key_validation.sampling_enabled",
            ):
                raise ValueError(
                    "key_validation.sampling_enabled must be false; key validation is full scan"
                )
            large_config = LargeTableConfig(
                enabled=boolean(required(large_raw, "enabled"), "large_table.enabled"),
                threshold_rows=positive_int(
                    required(large_raw, "threshold_rows"),
                    "large_table.threshold_rows",
                ),
                force_batch_processing=boolean(
                    required(large_raw, "force_batch_processing"),
                    "large_table.force_batch_processing",
                ),
                allow_full_dataframe_load=boolean(
                    required(large_raw, "allow_full_dataframe_load"),
                    "large_table.allow_full_dataframe_load",
                ),
            )
            pool_config = ConnectionPoolConfig(
                enabled=boolean(required(pool_raw, "enabled"), "connection_pool.enabled"),
                pool_size=positive_int(
                    required(pool_raw, "pool_size"), "connection_pool.pool_size"
                ),
                max_overflow=nonnegative_int(
                    required(pool_raw, "max_overflow"),
                    "connection_pool.max_overflow",
                ),
                pool_pre_ping=boolean(
                    required(pool_raw, "pool_pre_ping"),
                    "connection_pool.pool_pre_ping",
                ),
                pool_recycle_seconds=positive_int(
                    required(pool_raw, "pool_recycle_seconds"),
                    "connection_pool.pool_recycle_seconds",
                ),
                connection_timeout_seconds=positive_int(
                    required(connection_management_raw, "connection_timeout_seconds"),
                    "performance.connection_management.connection_timeout_seconds",
                ),
                reuse_connections=boolean(
                    pool_raw.get("reuse_connections", True),
                    "connection_pool.reuse_connections",
                ),
            )
            retry_config = RetryConfig(
                enabled=boolean(
                    required(retry_raw, "enabled"),
                    "database_protection.retry.enabled",
                ),
                max_attempts=positive_int(
                    required(retry_raw, "max_attempts"),
                    "database_protection.retry.max_attempts",
                ),
                delay_seconds=required(retry_raw, "delay_seconds"),
                # Optional: a deterministic statement timeout is not retried
                # unless a deployment explicitly opts in.
                retry_on_timeout=boolean(
                    retry_raw.get("retry_on_timeout", False),
                    "query_execution.retry.retry_on_timeout",
                ),
                timeout_max_attempts=positive_int(
                    retry_raw.get("timeout_max_attempts", 1),
                    "query_execution.retry.timeout_max_attempts",
                ),
            )
            if (
                isinstance(retry_config.delay_seconds, bool)
                or not isinstance(retry_config.delay_seconds, (int, float))
                or retry_config.delay_seconds < 0
            ):
                raise ValueError(
                    "database_protection.retry.delay_seconds must be non-negative"
                )
            protection_config = DatabaseProtectionConfig(
                retry=retry_config,
                timeout_seconds=positive_int(
                    required(protection_raw, "timeout_seconds"),
                    "database_protection.timeout_seconds",
                ),
            )
            checkpoint_config = CheckpointConfig(
                enabled=boolean(
                    required(checkpoint_raw, "enabled"), "checkpoint.enabled"
                ),
                directory=required(checkpoint_raw, "directory"),
                resume_failed_run=boolean(
                    required(checkpoint_raw, "resume_failed_run"),
                    "checkpoint.resume_failed_run",
                ),
            )
            if checkpoint_config.enabled and not str(checkpoint_config.directory).strip():
                raise ValueError("checkpoint.directory must be non-empty when enabled")
        else:
            parallel_config = ParallelExecutionConfig()
            batch_config = BatchProcessingConfig()
            key_validation_config = KeyValidationExecutionConfig()
            large_config = LargeTableConfig()
            pool_config = ConnectionPoolConfig()
            protection_config = DatabaseProtectionConfig()
            checkpoint_config = CheckpointConfig()

        monitoring_raw = performance["diagnostics"]
        if not isinstance(monitoring_raw, dict):
            raise ValueError("performance_monitoring must be a YAML mapping")
        slow_threshold = required(monitoring_raw, "slow_validation_threshold_seconds")
        if (
            isinstance(slow_threshold, bool)
            or not isinstance(slow_threshold, (int, float))
            or slow_threshold < 0
        ):
            raise ValueError(
                "performance_monitoring.slow_validation_threshold_seconds must be non-negative"
            )
        monitoring_config = PerformanceMonitoringConfig(
            enabled=boolean(
                required(monitoring_raw, "enabled"),
                "performance_monitoring.enabled",
            ),
            slow_validation_threshold_seconds=float(slow_threshold),
        )

        timing_raw = performance["timing"]
        timing_config = TimingConfig(
            enabled=boolean(required(timing_raw, "enabled"), "performance.timing.enabled"),
            individual_check_timing=boolean(
                required(timing_raw, "individual_check_timing"),
                "performance.timing.individual_check_timing",
            ),
            group_timing=boolean(
                required(timing_raw, "group_timing"), "performance.timing.group_timing"
            ),
            preserve_validator_timing=boolean(
                required(timing_raw, "preserve_validator_timing"),
                "performance.timing.preserve_validator_timing",
            ),
            allow_orchestrator_override=boolean(
                required(timing_raw, "allow_orchestrator_override"),
                "performance.timing.allow_orchestrator_override",
            ),
        )

        strategies_config = _parse_strategy_selection(
            performance_execution["strategies"],
            batch_config,
            boolean=boolean,
            nonnegative_int=nonnegative_int,
        )

        # Physical execution controls. Every one of these changes how much
        # database work runs, never what is validated.
        scan_optimization_config = _build_scan_optimization(
            performance.get("scan_optimization")
        )
        cohort_processing_config = _build_cohort_processing(
            performance.get("cohort_processing")
        )
        incremental_execution_config = _build_incremental_execution(
            performance.get("incremental_execution")
        )
        diagnostics_tracking_config = _build_diagnostics_tracking(monitoring_raw)
        engine_selection_config = _build_engine_selection(
            performance.get("engine_selection")
        )
        hash_spill_config = _build_hash_spill(performance.get("hash_spill"))
        temporary_storage_config = _build_temporary_storage(
            performance.get("temporary_storage")
        )
        large_table_concurrency_config = _build_large_table_concurrency(
            performance.get("large_table_concurrency")
        )

        execution = ExecutionConfig(
            engine=EngineType(performance_execution["engine"]),
            fallback_enabled=boolean(
                performance_execution["fallback_enabled"],
                "performance.execution.fallback_enabled",
            ),
            allowed_engines=[EngineType(e) for e in performance_execution["allowed_engines"]],
            check_workers=parallel_config.validation_parallelism.max_workers_per_table,
            validation_categories=validation_categories,
            parallel_execution=parallel_config,
            batch_processing=batch_config,
            key_validation=key_validation_config,
            large_table=large_config,
            connection_pool=pool_config,
            database_protection=protection_config,
            checkpoint=checkpoint_config,
            performance_monitoring=monitoring_config,
            timing=timing_config,
            strategies=strategies_config,
            scan_optimization=scan_optimization_config,
            cohort_processing=cohort_processing_config,
            incremental_execution=incremental_execution_config,
            diagnostics_tracking=diagnostics_tracking_config,
            engine_selection=engine_selection_config,
            hash_spill=hash_spill_config,
            temporary_storage=temporary_storage_config,
            large_table_concurrency=large_table_concurrency_config,
        )

        dialect_raw = raw.get("sql_dialects")
        try:
            sql_dialects = (
                SqlDialectRegistry.from_config(dialect_raw)
                if isinstance(dialect_raw, dict) and dialect_raw
                else SqlDialectRegistry.builtin()
            )
        except Exception as exc:
            raise ValueError(f"Invalid SQL dialect configuration: {exc}") from exc
        for name, connection in list(sources.items()) + list(targets.items()):
            configured_dialect = getattr(connection, "sql_dialect", None)
            if isinstance(configured_dialect, str) and configured_dialect.strip():
                # Fail at load time, not at first query.
                sql_dialects.get(configured_dialect)
            elif configured_dialect is not None and not isinstance(
                configured_dialect, dict
            ):
                raise ValueError(
                    f"Connection '{name}' sql_dialect must be a dialect name or "
                    "a YAML mapping"
                )

        preflight_config = _build_preflight_config(raw.get("preflight"))

        ctrl = raw.get("execution_control", {})
        configured_phases = ctrl.get("phases", [])
        if not isinstance(configured_phases, list) or not all(
            isinstance(phase, str) and phase.strip() for phase in configured_phases
        ):
            raise ValueError("execution_control.phases must be a list of non-empty phase names")
        configured_phases = _ordered_profile_names(
            phase.strip() for phase in configured_phases
        )
        if not configured_phases and "smoke_test" in profiles:
            configured_phases = ["smoke_test"]
        unknown_phases = sorted(set(configured_phases) - set(profiles))
        if unknown_phases:
                raise ValueError(
                    "Invalid validation phase: " + ", ".join(unknown_phases)
                    + ". Supported phases: " + ", ".join(profiles)
                )
        phase_execution = ctrl.get("phase_execution") or {}
        if not isinstance(phase_execution, dict):
            raise ValueError(
                "execution_control.phase_execution must be a YAML mapping"
            )
        unknown_phase_fields = sorted(set(phase_execution) - {"mode"})
        if unknown_phase_fields:
            raise ValueError(
                "execution_control.phase_execution has unsupported field(s): "
                + ", ".join(unknown_phase_fields)
            )
        phase_execution_mode = phase_execution.get(
            "mode", PHASE_EXECUTION_MODES[0]
        )
        if phase_execution_mode not in PHASE_EXECUTION_MODES:
            raise ValueError(
                "execution_control.phase_execution.mode must be one of: "
                + ", ".join(PHASE_EXECUTION_MODES)
            )
        filter_fields = (
            "table_ids", "table_names", "layers", "servers", "databases", "schemas"
        )
        normalized_filters = {}
        for field_name in filter_fields:
            values = ctrl.get(field_name, [])
            if not isinstance(values, list) or not all(
                isinstance(value, str) and value.strip() for value in values
            ):
                raise ValueError(
                    f"execution_control.{field_name} must be a list of non-empty strings"
                )
            normalized_filters[field_name] = list(dict.fromkeys(
                value.strip() for value in values
            ))

        execution_control = ExecutionControl(
            full_run=ctrl.get("full_run", True),
            environment=ctrl.get("environment"),
            table_ids=normalized_filters["table_ids"],
            table_names=normalized_filters["table_names"],
            layers=normalized_filters["layers"],
            servers=normalized_filters["servers"],
            databases=normalized_filters["databases"],
            schemas=normalized_filters["schemas"],
            phases=configured_phases,
            phase_execution_mode=phase_execution_mode,
        )

        validations = dict(validations_raw)
        if raw.get("comparison_settings") is not None:
            # Two documents may describe comparison settings: the nested
            # ``validations.comparison_settings`` block (comparison_settings.yml)
            # and a top-level ``comparison_settings`` block from a legacy
            # document in the same folder. Replacing the nested block with the
            # top-level one silently dropped every key only the nested block
            # declares (target lookup batching, query safety ceilings, batch
            # parallelism). They are merged instead: a key declared in both
            # keeps the top-level value, so existing behaviour is unchanged,
            # while keys declared only in the nested block become effective.
            nested_settings = validations_raw.get("comparison_settings")
            if isinstance(nested_settings, dict) and nested_settings:
                merged_settings = copy.deepcopy(nested_settings)
                self._deep_merge(merged_settings, copy.deepcopy(comparison_settings_raw))
                comparison_settings_raw = merged_settings
            validations["comparison_settings"] = comparison_settings_raw

        comparison_settings = comparison_settings_raw

        return MigrationConfig(
            project_name=proj.get("name", "Migration"),
            run_id_prefix=proj.get("run_id_prefix", "MIG"),
            sources=sources,
            target=targets[execution_target_ref] if execution_target_ref else list(targets.values())[0],
            validation_profiles=profiles,
            tables=tables,
            thresholds=thresholds,
            reporting=reporting,
            validations=validations,
            comparison_settings=comparison_settings,
            performance=execution,
            execution_control=execution_control,
            run_defaults=run_defaults,
            source_ref=execution_source_ref,
            target_ref=execution_target_ref,
            version=str(raw.get("version", "1.0")),
            framework=dict(raw.get("framework", {}) or {}),
            preflight=preflight_config,
            sql_dialects=sql_dialects,
        )
