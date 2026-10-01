"""
fabric_migration_validator/core/execution_engine.py
────────────────────────────────────────────────────
Enterprise-grade execution engine resolution and abstraction.
Supports Spark, SQL/ODBC, and Pandas with dynamic fallback.
"""

import logging
import enum
from typing import Any, Optional, List
from dataclasses import dataclass, field, fields

logger = logging.getLogger(__name__)

class EngineType(str, enum.Enum):
    AUTO = "auto"
    SPARK = "spark"
    ODBC = "odbc"
    PANDAS = "pandas"


@dataclass(frozen=True)
class TableParallelismConfig:
    enabled: bool = False
    max_workers: int = 1
    executor_type: str = "thread"
    fail_fast: bool = False


@dataclass(frozen=True)
class ValidationParallelismConfig:
    enabled: bool = False
    max_workers_per_table: int = 1


@dataclass(frozen=True)
class SourceTargetParallelismConfig:
    enabled: bool = False
    max_workers_per_validation: int = 1


@dataclass(frozen=True)
class BatchParallelismConfig:
    enabled: bool = False
    max_workers_per_table: int = 1


@dataclass(frozen=True)
class ParallelExecutionConfig:
    enabled: bool = False
    table_parallelism: TableParallelismConfig = field(
        default_factory=TableParallelismConfig
    )
    validation_parallelism: ValidationParallelismConfig = field(
        default_factory=ValidationParallelismConfig
    )
    source_target_parallelism: SourceTargetParallelismConfig = field(
        default_factory=SourceTargetParallelismConfig
    )
    batch_parallelism: BatchParallelismConfig = field(
        default_factory=BatchParallelismConfig
    )
    max_concurrent_validations: int = 1
    # Single ceiling shared by every parallelism dimension. 0 derives it from
    # the configured worker counts instead of multiplying them.
    global_max_concurrency: int = 0

    def resolved_global_max_concurrency(self) -> int:
        """Return the effective global ceiling on concurrent remote operations."""
        if self.global_max_concurrency > 0:
            return self.global_max_concurrency
        candidates = [
            self.max_concurrent_validations,
            self.table_parallelism.max_workers,
            self.validation_parallelism.max_workers_per_table,
            self.source_target_parallelism.max_workers_per_validation,
            self.batch_parallelism.max_workers_per_table,
        ]
        return max([value for value in candidates if value and value > 0] or [1])


@dataclass(frozen=True)
class PerformanceMonitoringConfig:
    enabled: bool = False
    slow_validation_threshold_seconds: float = 0.0


@dataclass(frozen=True)
class TimingConfig:
    enabled: bool = True
    individual_check_timing: bool = True
    group_timing: bool = True
    preserve_validator_timing: bool = True
    allow_orchestrator_override: bool = False


@dataclass(frozen=True)
class AdaptiveBatchingConfig:
    """Physical fetch-size tuning between bounded limits.

    Adjusts only how many records one physical query requests. Coverage,
    ordering, key boundaries and every validation result stay identical:
    the keyset cursor still advances through the same records exactly once.
    """

    enabled: bool = False
    #: 0 means "start from the configured base batch size".
    initial_batch_size: int = 0
    minimum_batch_size: int = 1
    #: 0 means "no ceiling above the initial size".
    maximum_batch_size: int = 0
    increase_factor: float = 1.5
    decrease_factor: float = 0.5
    target_batch_seconds: float = 10.0

    def clamp(self, size: int) -> int:
        size = max(size, self.minimum_batch_size, 1)
        if self.maximum_batch_size > 0:
            size = min(size, self.maximum_batch_size)
        return size

    def first_batch_size(self, base_batch_size: int) -> int:
        return self.clamp(self.initial_batch_size or base_batch_size)

    def next_batch_size(self, current: int, elapsed_seconds: float) -> int:
        if self.target_batch_seconds <= 0:
            return self.clamp(current)
        if elapsed_seconds > self.target_batch_seconds:
            return self.clamp(int(current * self.decrease_factor))
        # Grow only when there is clear headroom, so sizes do not oscillate.
        if elapsed_seconds < self.target_batch_seconds / 2:
            return self.clamp(int(current * self.increase_factor))
        return self.clamp(current)


@dataclass(frozen=True)
class BatchProcessingConfig:
    enabled: bool = False
    batch_size: int = 1
    max_rows_in_memory: int = 1
    #: auto | keyset | partition | range | hash | watermark | file_partition.
    #: ``auto`` resolves the most efficient strategy both connectors support.
    strategy: str = "keyset"
    retry_failed_batch: bool = False
    max_retries: int = 0
    adaptive_batching: AdaptiveBatchingConfig = field(
        default_factory=AdaptiveBatchingConfig
    )


@dataclass(frozen=True)
class KeyValidationExecutionConfig:
    """Safe, full-scan execution controls for key validation."""

    # This switch is deliberately validated as true by ConfigLoader. It is
    # explicit in YAML so a deployment cannot silently turn exact validation
    # into sampling.
    full_scan: bool = True
    partition_size: int = 10_000
    max_workers: int = 1
    parallel_execution: bool = False
    reuse_connections: bool = True
    #: How a partition of keys is handed to the other database. ``auto`` lets
    #: the connector's dialect preference decide.
    key_cohort_strategy: str = "auto"


@dataclass(frozen=True)
class PushdownStrategyConfig:
    """Server-side computation of statistics on the configured engines."""

    enabled: bool = True
    #: Produce many statistics per scan instead of one scan per statistic.
    combine_statistics: bool = True
    #: 0 means "no framework-imposed ceiling"; connectors may still narrow it.
    max_statistics_per_query: int = 0


@dataclass(frozen=True)
class PandasStrategyConfig:
    """In-memory execution for small tables, samples, batches and reports."""

    enabled: bool = True
    max_rows_in_memory: int = 0


@dataclass(frozen=True)
class BatchStrategyConfig:
    """Bounded batching for large record-level work."""

    enabled: bool = True
    strategy: str = "auto"
    batch_size: int = 0


@dataclass(frozen=True)
class SparkStrategyConfig:
    """Distributed execution, used only when both sides truly support it."""

    enabled: bool = False
    threshold_rows: int = 0
    #: Cap on rows a distributed frame may materialize into Pandas.
    max_pandas_rows: int = 0


@dataclass(frozen=True)
class StrategySelectionConfig:
    """Configuration consumed by :class:`core.strategy.StrategyResolver`."""

    engine: str = "auto"
    pushdown: PushdownStrategyConfig = field(default_factory=PushdownStrategyConfig)
    pandas: PandasStrategyConfig = field(default_factory=PandasStrategyConfig)
    batch: BatchStrategyConfig = field(default_factory=BatchStrategyConfig)
    spark: SparkStrategyConfig = field(default_factory=SparkStrategyConfig)


@dataclass(frozen=True)
class LargeTableConfig:
    enabled: bool = False
    threshold_rows: int = 1
    force_batch_processing: bool = False
    allow_full_dataframe_load: bool = False


@dataclass(frozen=True)
class ConnectionPoolConfig:
    enabled: bool = False
    pool_size: int = 1
    max_overflow: int = 0
    pool_pre_ping: bool = True
    pool_recycle_seconds: int = 1
    connection_timeout_seconds: int = 30
    #: Share one pool between every worker that resolves to the same configured
    #: connection identity, instead of building a pool per table.
    reuse_connections: bool = True


@dataclass(frozen=True)
class ScanOptimizationConfig:
    """Physical scan sharing. Never changes what is validated.

    Every flag here removes duplicate database work. Turning any of them off
    restores the independent per-validation queries and must produce identical
    validation results.
    """

    enabled: bool = True
    shared_statistics_scan: bool = True
    shared_record_scan: bool = True
    reuse_key_statistics: bool = True
    reuse_row_counts: bool = True
    reuse_null_statistics: bool = True
    reuse_duplicate_statistics: bool = True
    reuse_audit_statistics: bool = True
    duplicate_query_detection: bool = True

    def statistics_sharing_enabled(self) -> bool:
        return bool(self.enabled and self.shared_statistics_scan)

    def record_sharing_enabled(self) -> bool:
        return bool(self.enabled and self.shared_record_scan)


@dataclass(frozen=True)
class PredicateOrConfig:
    """The last-resort per-key predicate strategy and its safety ceiling."""

    enabled: bool = True
    max_keys: int = 100


@dataclass(frozen=True)
class ValuesJoinConfig:
    """Safety ceiling for one inline VALUES key cohort statement."""

    max_keys: Optional[int] = None


@dataclass(frozen=True)
class CohortProcessingConfig:
    """How a batch of keys is handed to the other database."""

    strategy: str = "auto"
    allowed_strategies: tuple = ("values_join", "key_range", "staged_keys", "predicate_or")
    values_join: ValuesJoinConfig = field(default_factory=ValuesJoinConfig)
    predicate_or: PredicateOrConfig = field(default_factory=PredicateOrConfig)

    def permits(self, strategy: str) -> bool:
        if strategy == "predicate_or" and not self.predicate_or.enabled:
            return False
        return strategy in self.allowed_strategies


@dataclass(frozen=True)
class IncrementalExecutionConfig:
    """Global safety ceiling for incremental validation scope.

    A validation profile declares what window it wants. This block decides
    whether an unresolvable window may ever be widened to the whole table, so a
    profile cannot quietly opt into a full historical scan.
    """

    require_scope: bool = True
    missing_scope_action: str = "error"
    allow_full_table_fallback: bool = False


@dataclass(frozen=True)
class DiagnosticsTrackingConfig:
    """Which execution counters the report carries."""

    track_database_queries: bool = True
    track_full_scans: bool = True
    track_batch_queries: bool = True
    track_cache_hits: bool = True
    track_cache_misses: bool = True
    track_connection_reuse: bool = True
    track_shared_scans: bool = True

    def any_enabled(self) -> bool:
        return any(
            getattr(self, item.name) for item in fields(self)
        )


#: Actions supported when an optional engine dependency is unavailable.
ENGINE_UNAVAILABLE_ACTIONS = ("fallback", "error")

#: Engine names accepted in engine-selection priority lists.
SELECTABLE_ENGINE_NAMES = (
    "spark", "sql_pushdown", "odbc", "polars", "duckdb", "pandas",
)


@dataclass(frozen=True)
class SparkAvailabilityCheckConfig:
    """Which environment facts the one-per-run Spark capability check probes."""

    check_java: bool = True
    check_java_home: bool = True
    check_pyspark: bool = True


@dataclass(frozen=True)
class SparkEngineSelectionConfig:
    """Whether Spark may be used and what happens when it cannot be."""

    enabled: bool = True
    #: When true, an unavailable Spark stops the run instead of falling back.
    required: bool = False
    require_java: bool = True
    availability: SparkAvailabilityCheckConfig = field(
        default_factory=SparkAvailabilityCheckConfig
    )
    on_java_missing: str = "fallback"
    on_pyspark_missing: str = "fallback"
    on_initialization_failure: str = "fallback"

    def action_for(self, reason: str) -> str:
        """The configured action for one unavailability reason."""
        if self.required:
            return "error"
        return {
            "java_missing": self.on_java_missing,
            "pyspark_missing": self.on_pyspark_missing,
            "initialization_failure": self.on_initialization_failure,
        }.get(reason, "fallback")


@dataclass(frozen=True)
class EngineSelectionConfig:
    """Run-level execution-engine availability and fallback policy.

    Selecting a different engine never changes what a validation computes;
    it changes only where equivalent work physically runs.
    """

    preferred_engine: str = "auto"
    availability_check_enabled: bool = True
    cache_for_run: bool = True
    priority: tuple = ("spark", "sql_pushdown", "polars", "duckdb", "pandas")
    spark: SparkEngineSelectionConfig = field(
        default_factory=SparkEngineSelectionConfig
    )
    fallback_enabled: bool = True
    when_spark_unavailable: tuple = ("sql_pushdown", "polars", "duckdb", "pandas")


@dataclass(frozen=True)
class HashSpillConfig:
    """Which local store holds streamed hash counts during full comparison.

    Every engine stores the identical multiset of row hashes and produces the
    identical matched/excess arithmetic; only the storage medium differs.
    """

    engine: str = "auto"
    priority: tuple = ("memory", "sqlite", "duckdb")
    commit_interval_batches: int = 20


@dataclass(frozen=True)
class TemporaryStorageConfig:
    """Safety limits for framework-owned temporary files."""

    directory: str = "output/temp"
    cleanup_on_success: bool = True
    cleanup_on_failure: bool = True
    disk_space_check_enabled: bool = True
    minimum_free_space_mb: int = 2048


@dataclass(frozen=True)
class LargeTableConcurrencyConfig:
    """Scheduling ceilings for heavy scans on large tables.

    Gating changes only when a validation starts, never whether it runs or
    what it produces.
    """

    enabled: bool = False
    max_heavy_validations_per_table: int = 1
    max_large_tables_per_source_endpoint: int = 2
    max_large_tables_per_target_endpoint: int = 2


@dataclass(frozen=True)
class RetryConfig:
    enabled: bool = False
    max_attempts: int = 1
    delay_seconds: float = 0.0
    #: A statement timeout is deterministic for an unchanged statement, so it
    #: is not retried by default; transient connection failures still are.
    retry_on_timeout: bool = False
    #: Attempts allowed for a timed-out statement when ``retry_on_timeout`` is
    #: enabled (bounded independently of ``max_attempts``).
    timeout_max_attempts: int = 1


@dataclass(frozen=True)
class DatabaseProtectionConfig:
    retry: RetryConfig = field(default_factory=RetryConfig)
    timeout_seconds: int = 30


@dataclass(frozen=True)
class CheckpointConfig:
    enabled: bool = False
    directory: str = ""
    resume_failed_run: bool = False

@dataclass
class ExecutionConfig:
    engine: EngineType = EngineType.AUTO
    fallback_enabled: bool = True
    allowed_engines: List[EngineType] = field(default_factory=lambda: [
        EngineType.SPARK, EngineType.ODBC, EngineType.PANDAS
    ])
    check_workers: int = 4  # Default parallel threads for individual checks per table
    validation_categories: List[str] = field(default_factory=lambda: [
        "migration_validation",
        "data_quality_validation",
        "business_rule_validation",
    ])
    parallel_execution: ParallelExecutionConfig = field(
        default_factory=ParallelExecutionConfig
    )
    batch_processing: BatchProcessingConfig = field(
        default_factory=BatchProcessingConfig
    )
    key_validation: KeyValidationExecutionConfig = field(
        default_factory=KeyValidationExecutionConfig
    )
    large_table: LargeTableConfig = field(default_factory=LargeTableConfig)
    connection_pool: ConnectionPoolConfig = field(
        default_factory=ConnectionPoolConfig
    )
    database_protection: DatabaseProtectionConfig = field(
        default_factory=DatabaseProtectionConfig
    )
    checkpoint: CheckpointConfig = field(default_factory=CheckpointConfig)
    performance_monitoring: PerformanceMonitoringConfig = field(
        default_factory=PerformanceMonitoringConfig
    )
    timing: TimingConfig = field(default_factory=TimingConfig)
    strategies: StrategySelectionConfig = field(
        default_factory=StrategySelectionConfig
    )
    scan_optimization: ScanOptimizationConfig = field(
        default_factory=ScanOptimizationConfig
    )
    cohort_processing: CohortProcessingConfig = field(
        default_factory=CohortProcessingConfig
    )
    incremental_execution: IncrementalExecutionConfig = field(
        default_factory=IncrementalExecutionConfig
    )
    diagnostics_tracking: DiagnosticsTrackingConfig = field(
        default_factory=DiagnosticsTrackingConfig
    )
    engine_selection: EngineSelectionConfig = field(
        default_factory=EngineSelectionConfig
    )
    hash_spill: HashSpillConfig = field(default_factory=HashSpillConfig)
    temporary_storage: TemporaryStorageConfig = field(
        default_factory=TemporaryStorageConfig
    )
    large_table_concurrency: LargeTableConcurrencyConfig = field(
        default_factory=LargeTableConcurrencyConfig
    )

@dataclass
class ExecutionContext:
    engine_type: EngineType
    spark_session: Optional[Any] = None

class ExecutionEngineResolver:
    """
    Dynamically determines the best execution engine based on environment and config.
    """
    def __init__(self, config: ExecutionConfig):
        self.config = config

    def resolve(self, spark_session: Optional[Any] = None) -> ExecutionContext:
        requested = self.config.engine
        
        # 1. Force Spark if explicitly requested and available
        if requested == EngineType.SPARK:
            if spark_session:
                logger.info("Selected execution engine: SPARK (explicitly requested)")
                return ExecutionContext(EngineType.SPARK, spark_session=spark_session)
            elif self.config.fallback_enabled:
                logger.warning("Spark requested but not available. Falling back...")
            else:
                raise RuntimeError("Spark engine requested but SparkSession is missing.")

        # 2. Auto-detect
        if requested == EngineType.AUTO or self.config.fallback_enabled:
            # Check for Spark
            if spark_session and EngineType.SPARK in self.config.allowed_engines:
                logger.info("Selected execution engine: SPARK (auto-detected)")
                return ExecutionContext(EngineType.SPARK, spark_session=spark_session)
            
            # Check for ODBC (requires pyodbc)
            if EngineType.ODBC in self.config.allowed_engines:
                try:
                    import pyodbc
                    logger.info("Selected execution engine: ODBC (auto-detected fallback)")
                    return ExecutionContext(EngineType.ODBC)
                except ImportError:
                    pass

            # Final fallback: Pandas
            if EngineType.PANDAS in self.config.allowed_engines:
                logger.info("Selected execution engine: PANDAS (final fallback)")
                return ExecutionContext(EngineType.PANDAS)

        raise RuntimeError(f"Could not resolve execution engine for config: {self.config}")
