"""
fabric_migration_validator/core/strategy.py
───────────────────────────────────────────
Generic, capability-driven execution strategy resolution.

    Validation Request
            |
            v
    Read Source Connector Capabilities
            |
            v
    Read Target Connector Capabilities
            |
            v
    Read Validation Type
            |
            v
    Read Estimated Data Volume
            |
            v
    Read Validation Profile
            |
            v
    Select Best Supported Strategy

Nothing in this module knows a platform, a host, a driver or a table. It reads
the connector capability records, the execution configuration and the runtime
volume estimate, and returns the best strategy both sides actually support.
"""

from __future__ import annotations

import enum
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Sequence, Tuple

from core.capabilities import ConnectorCapabilities, resolve_capabilities

logger = logging.getLogger(__name__)


class ExecutionStrategy(str, enum.Enum):
    """How one validation will be executed."""

    METADATA = "metadata"
    PUSHDOWN = "pushdown"
    PANDAS = "pandas"
    KEYSET_BATCH = "keyset_batch"
    HASH_BATCH = "hash_batch"
    SPARK_DISTRIBUTED = "spark_distributed"
    FILE_BATCH = "file_batch"


class BatchStrategy(str, enum.Enum):
    """How a large record-level comparison is split into bounded units."""

    AUTO = "auto"
    KEYSET = "keyset"
    PARTITION = "partition"
    RANGE = "range"
    HASH = "hash"
    WATERMARK = "watermark"
    FILE_PARTITION = "file_partition"


#: Capability required to run each batching strategy on both sides.
_BATCH_STRATEGY_CAPABILITY = {
    BatchStrategy.KEYSET: "keyset_pagination",
    BatchStrategy.PARTITION: "partition_read",
    BatchStrategy.RANGE: "range_pagination",
    BatchStrategy.HASH: "hash_comparison",
    BatchStrategy.WATERMARK: "watermark_filter",
    BatchStrategy.FILE_PARTITION: "file_partition_read",
}

#: Preference order used when ``batch_processing.strategy: auto`` is configured.
_AUTO_BATCH_PREFERENCE = (
    BatchStrategy.KEYSET,
    BatchStrategy.PARTITION,
    BatchStrategy.RANGE,
    BatchStrategy.WATERMARK,
    BatchStrategy.HASH,
    BatchStrategy.FILE_PARTITION,
)


class ValidationKind(str, enum.Enum):
    """What a validation fundamentally needs from the two systems."""

    #: Answered from connector metadata operations only.
    METADATA = "metadata"
    #: A count / aggregate / statistic that an engine can compute server-side.
    AGGREGATE = "aggregate"
    #: Needs individual records aligned across Source and Target.
    RECORD = "record"
    #: Needs individual values of aligned records.
    VALUE = "value"


#: Validation name → kind. Keys are the framework's own validation names, which
#: come from the validation configuration, never from a platform.
VALIDATION_KINDS: Dict[str, ValidationKind] = {
    "row_count": ValidationKind.AGGREGATE,
    "null_validation": ValidationKind.AGGREGATE,
    "aggregate": ValidationKind.AGGREGATE,
    "duplicate": ValidationKind.AGGREGATE,
    "audit_validation": ValidationKind.AGGREGATE,
    "edge_case_validation": ValidationKind.AGGREGATE,
    "referential": ValidationKind.AGGREGATE,
    "scd": ValidationKind.AGGREGATE,
    "business_rules": ValidationKind.AGGREGATE,
    "pii": ValidationKind.METADATA,
    "key_validation": ValidationKind.RECORD,
    "record_comparison": ValidationKind.RECORD,
    "data_value_validation": ValidationKind.VALUE,
}


@dataclass(frozen=True)
class StrategyDecision:
    """The selected strategy plus everything needed to explain and log it."""

    validation_name: str
    kind: ValidationKind
    strategy: ExecutionStrategy
    reason: str
    batch_strategy: Optional[BatchStrategy] = None
    batch_size: int = 0
    max_rows_in_memory: int = 0
    estimated_rows: Optional[int] = None
    considered: Tuple[str, ...] = field(default_factory=tuple)
    rejected: Tuple[str, ...] = field(default_factory=tuple)

    @property
    def is_pushdown(self) -> bool:
        return self.strategy in (
            ExecutionStrategy.PUSHDOWN,
            ExecutionStrategy.METADATA,
        )

    @property
    def is_batched(self) -> bool:
        return self.strategy in (
            ExecutionStrategy.KEYSET_BATCH,
            ExecutionStrategy.HASH_BATCH,
            ExecutionStrategy.FILE_BATCH,
        )

    def as_details(self) -> Dict[str, Any]:
        """Report-friendly mapping merged into ``CheckResult.details``."""
        details = {
            "execution_strategy": self.strategy.value,
            "execution_strategy_reason": self.reason,
            "validation_kind": self.kind.value,
            "estimated_rows": self.estimated_rows,
        }
        if self.batch_strategy is not None:
            details["batch_strategy"] = self.batch_strategy.value
            details["batch_size"] = self.batch_size
        if self.considered:
            details["strategies_considered"] = list(self.considered)
        if self.rejected:
            details["strategies_rejected"] = list(self.rejected)
        return details


class StrategyResolver:
    """Select the best supported execution strategy for one validation."""

    def __init__(self, execution_config: Any = None):
        self.execution_config = execution_config
        self.strategies = getattr(execution_config, "strategies", None)
        self.large_table = getattr(execution_config, "large_table", None)
        self.batch_processing = getattr(execution_config, "batch_processing", None)

    # ── Configuration accessors ───────────────────────────────
    def _strategy_section(self, name: str) -> Any:
        return getattr(self.strategies, name, None)

    def _enabled(self, name: str, default: bool = True) -> bool:
        section = self._strategy_section(name)
        if section is None:
            return default
        return bool(getattr(section, "enabled", default))

    @property
    def max_rows_in_memory(self) -> int:
        pandas_section = self._strategy_section("pandas")
        configured = int(getattr(pandas_section, "max_rows_in_memory", 0) or 0)
        if configured:
            return configured
        return int(getattr(self.batch_processing, "max_rows_in_memory", 0) or 0)

    @property
    def batch_size(self) -> int:
        batch_section = self._strategy_section("batch")
        configured = int(getattr(batch_section, "batch_size", 0) or 0)
        if configured:
            return configured
        return int(getattr(self.batch_processing, "batch_size", 0) or 0)

    @property
    def spark_threshold_rows(self) -> int:
        spark_section = self._strategy_section("spark")
        return int(getattr(spark_section, "threshold_rows", 0) or 0)

    @property
    def large_table_threshold_rows(self) -> int:
        if self.large_table is not None and getattr(self.large_table, "enabled", False):
            return int(getattr(self.large_table, "threshold_rows", 0) or 0)
        return 0

    @property
    def allows_full_dataframe_load(self) -> bool:
        if self.large_table is None:
            return True
        if not getattr(self.large_table, "enabled", False):
            return True
        return bool(getattr(self.large_table, "allow_full_dataframe_load", False))

    # ── Batch strategy selection ──────────────────────────────
    def resolve_batch_strategy(
        self,
        source: ConnectorCapabilities,
        target: ConnectorCapabilities,
        table_map: Any = None,
        configured: Optional[str] = None,
    ) -> Optional[BatchStrategy]:
        """Return the most efficient batching strategy both sides support."""
        requested = configured
        if requested is None:
            batch_section = self._strategy_section("batch")
            requested = getattr(batch_section, "strategy", None) or getattr(
                self.batch_processing, "strategy", None
            )
        requested = str(requested or BatchStrategy.AUTO.value).strip().lower()

        if requested != BatchStrategy.AUTO.value:
            try:
                explicit = BatchStrategy(requested)
            except ValueError:
                raise ValueError(
                    f"Unsupported batch strategy '{requested}'. Supported: "
                    + ", ".join(item.value for item in BatchStrategy)
                )
            if self._batch_strategy_available(explicit, source, target, table_map):
                return explicit
            logger.info(
                "Configured batch strategy %s is unsupported by the configured "
                "connectors; resolving the best supported alternative",
                explicit.value,
            )

        for candidate in _AUTO_BATCH_PREFERENCE:
            if self._batch_strategy_available(candidate, source, target, table_map):
                return candidate
        return None

    def _batch_strategy_available(
        self,
        candidate: BatchStrategy,
        source: ConnectorCapabilities,
        target: ConnectorCapabilities,
        table_map: Any,
    ) -> bool:
        capability = _BATCH_STRATEGY_CAPABILITY.get(candidate)
        if capability is None:
            return False
        if not (source.supports(capability) and target.supports(capability)):
            return False
        if candidate in (BatchStrategy.KEYSET, BatchStrategy.RANGE):
            return bool(_configured_keys(table_map))
        if candidate is BatchStrategy.WATERMARK:
            return bool(getattr(table_map, "incremental_column", None))
        if candidate is BatchStrategy.PARTITION:
            return bool(
                _configured_keys(table_map)
                or getattr(table_map, "incremental_column", None)
            )
        return True

    # ── Main resolution ───────────────────────────────────────
    def resolve(
        self,
        validation_name: str,
        source_connector: Any = None,
        target_connector: Any = None,
        estimated_rows: Optional[int] = None,
        table_map: Any = None,
        validation_profile: Optional[str] = None,
        kind: Optional[ValidationKind] = None,
        source_capabilities: Optional[ConnectorCapabilities] = None,
        target_capabilities: Optional[ConnectorCapabilities] = None,
    ) -> StrategyDecision:
        source = source_capabilities or resolve_capabilities(source_connector)
        target = target_capabilities or resolve_capabilities(target_connector)
        resolved_kind = kind or VALIDATION_KINDS.get(
            validation_name, ValidationKind.RECORD
        )
        considered: list = []
        rejected: list = []

        def reject(strategy: ExecutionStrategy, why: str) -> None:
            rejected.append(f"{strategy.value}:{why}")

        # 1. Metadata-only work never touches data.
        if resolved_kind is ValidationKind.METADATA:
            considered.append(ExecutionStrategy.METADATA.value)
            return self._decision(
                validation_name,
                resolved_kind,
                ExecutionStrategy.METADATA,
                "Metadata validation uses connector metadata operations only",
                estimated_rows,
                considered,
                rejected,
            )

        # 2. Aggregates/statistics: prefer server-side computation.
        if resolved_kind is ValidationKind.AGGREGATE:
            considered.append(ExecutionStrategy.PUSHDOWN.value)
            if not self._enabled("pushdown"):
                reject(ExecutionStrategy.PUSHDOWN, "disabled_by_configuration")
            elif not source.supports("aggregation_pushdown"):
                reject(ExecutionStrategy.PUSHDOWN, "source_lacks_aggregation_pushdown")
            elif not target.supports("aggregation_pushdown"):
                reject(ExecutionStrategy.PUSHDOWN, "target_lacks_aggregation_pushdown")
            else:
                return self._decision(
                    validation_name,
                    resolved_kind,
                    ExecutionStrategy.PUSHDOWN,
                    "Both connectors compute the statistic on their own engine",
                    estimated_rows,
                    considered,
                    rejected,
                )

        # 3. Record/value work, and aggregates that could not be pushed down.
        spark_decision = self._try_spark(
            source, target, estimated_rows, considered, reject
        )
        if spark_decision is not None:
            return self._decision(
                validation_name,
                resolved_kind,
                ExecutionStrategy.SPARK_DISTRIBUTED,
                spark_decision,
                estimated_rows,
                considered,
                rejected,
            )

        pandas_reason = self._try_pandas(
            estimated_rows, considered, reject
        )
        if pandas_reason is not None:
            return self._decision(
                validation_name,
                resolved_kind,
                ExecutionStrategy.PANDAS,
                pandas_reason,
                estimated_rows,
                considered,
                rejected,
            )

        batch_strategy = None
        considered.append(ExecutionStrategy.KEYSET_BATCH.value)
        if not self._enabled("batch"):
            reject(ExecutionStrategy.KEYSET_BATCH, "disabled_by_configuration")
        else:
            batch_strategy = self.resolve_batch_strategy(source, target, table_map)
            if batch_strategy is None:
                reject(
                    ExecutionStrategy.KEYSET_BATCH,
                    "no_batching_capability_supported_by_both_connectors",
                )
        if batch_strategy is not None:
            strategy = _BATCH_STRATEGY_EXECUTION.get(
                batch_strategy, ExecutionStrategy.KEYSET_BATCH
            )
            return self._decision(
                validation_name,
                resolved_kind,
                strategy,
                (
                    f"Bounded {batch_strategy.value} batching is the best strategy "
                    "supported by both connectors"
                ),
                estimated_rows,
                considered,
                rejected,
                batch_strategy=batch_strategy,
            )

        # 4. Last resort: Pandas, only when a full load is explicitly allowed.
        considered.append(ExecutionStrategy.PANDAS.value)
        if self._enabled("pandas") and self.allows_full_dataframe_load:
            return self._decision(
                validation_name,
                resolved_kind,
                ExecutionStrategy.PANDAS,
                (
                    "No pushdown, distributed or batching capability is available; "
                    "configuration allows a full in-memory load"
                ),
                estimated_rows,
                considered,
                rejected,
            )
        reject(ExecutionStrategy.PANDAS, "full_dataframe_load_not_allowed")
        return self._decision(
            validation_name,
            resolved_kind,
            ExecutionStrategy.PUSHDOWN,
            (
                "No in-memory or batching strategy is permitted; only server-side "
                "computation may run for this validation"
            ),
            estimated_rows,
            considered,
            rejected,
        )

    # ── Candidate helpers ─────────────────────────────────────
    def _try_spark(
        self,
        source: ConnectorCapabilities,
        target: ConnectorCapabilities,
        estimated_rows: Optional[int],
        considered: list,
        reject,
    ) -> Optional[str]:
        considered.append(ExecutionStrategy.SPARK_DISTRIBUTED.value)
        if not self._enabled("spark", default=False):
            reject(ExecutionStrategy.SPARK_DISTRIBUTED, "disabled_by_configuration")
            return None
        if not source.supports("spark_read"):
            reject(ExecutionStrategy.SPARK_DISTRIBUTED, "source_not_readable_by_spark")
            return None
        if not target.supports("spark_read"):
            reject(ExecutionStrategy.SPARK_DISTRIBUTED, "target_not_readable_by_spark")
            return None
        threshold = self.spark_threshold_rows
        if estimated_rows is None:
            reject(ExecutionStrategy.SPARK_DISTRIBUTED, "volume_unknown")
            return None
        if threshold and estimated_rows < threshold:
            reject(
                ExecutionStrategy.SPARK_DISTRIBUTED,
                f"volume_below_threshold_{threshold}",
            )
            return None
        return (
            "Both connectors are readable by the distributed engine and the "
            f"estimated volume ({estimated_rows}) justifies distributed processing"
        )

    def _try_pandas(
        self,
        estimated_rows: Optional[int],
        considered: list,
        reject,
    ) -> Optional[str]:
        considered.append(ExecutionStrategy.PANDAS.value)
        if not self._enabled("pandas"):
            reject(ExecutionStrategy.PANDAS, "disabled_by_configuration")
            return None
        limit = self.max_rows_in_memory
        if not limit:
            reject(ExecutionStrategy.PANDAS, "max_rows_in_memory_not_configured")
            return None
        if estimated_rows is None:
            reject(ExecutionStrategy.PANDAS, "volume_unknown")
            return None
        if estimated_rows > limit:
            reject(
                ExecutionStrategy.PANDAS,
                f"volume_above_max_rows_in_memory_{limit}",
            )
            return None
        large_threshold = self.large_table_threshold_rows
        if (
            large_threshold
            and estimated_rows >= large_threshold
            and getattr(self.large_table, "force_batch_processing", False)
        ):
            reject(ExecutionStrategy.PANDAS, "large_table_forces_batch_processing")
            return None
        return (
            f"Estimated volume ({estimated_rows}) fits the configured "
            f"in-memory limit ({limit})"
        )

    def _decision(
        self,
        validation_name: str,
        kind: ValidationKind,
        strategy: ExecutionStrategy,
        reason: str,
        estimated_rows: Optional[int],
        considered: Sequence[str],
        rejected: Sequence[str],
        batch_strategy: Optional[BatchStrategy] = None,
    ) -> StrategyDecision:
        return StrategyDecision(
            validation_name=validation_name,
            kind=kind,
            strategy=strategy,
            reason=reason,
            batch_strategy=batch_strategy,
            batch_size=self.batch_size if batch_strategy is not None else 0,
            max_rows_in_memory=self.max_rows_in_memory,
            estimated_rows=estimated_rows,
            considered=tuple(dict.fromkeys(considered)),
            rejected=tuple(dict.fromkeys(rejected)),
        )


#: Which execution strategy a batching strategy runs under.
_BATCH_STRATEGY_EXECUTION = {
    BatchStrategy.KEYSET: ExecutionStrategy.KEYSET_BATCH,
    BatchStrategy.RANGE: ExecutionStrategy.KEYSET_BATCH,
    BatchStrategy.PARTITION: ExecutionStrategy.KEYSET_BATCH,
    BatchStrategy.WATERMARK: ExecutionStrategy.KEYSET_BATCH,
    BatchStrategy.HASH: ExecutionStrategy.HASH_BATCH,
    BatchStrategy.FILE_PARTITION: ExecutionStrategy.FILE_BATCH,
}


def _configured_keys(table_map: Any) -> Sequence[Any]:
    return getattr(table_map, "key_mappings", None) or []


def estimate_row_volume(
    table_map: Any = None,
    execution_cache: Any = None,
    default: Optional[int] = None,
) -> Optional[int]:
    """Return the best already-known row estimate without querying anything.

    The estimate comes from runtime metadata a previous validation already
    computed (the run-local cache) or from optional table configuration. It
    never issues a query of its own, so strategy selection stays free.
    """
    values = []
    for role in ("source", "target"):
        cached = _cached_value(execution_cache, ("row_count", role))
        if isinstance(cached, int) and not isinstance(cached, bool):
            values.append(cached)
    configured = getattr(table_map, "estimated_row_count", None)
    if isinstance(configured, int) and not isinstance(configured, bool):
        values.append(configured)
    if values:
        return max(values)
    return default


def _cached_value(execution_cache: Any, key: Any) -> Any:
    peek = getattr(execution_cache, "peek", None)
    if not callable(peek):
        return None
    try:
        return peek(key)
    except Exception:  # pragma: no cover - defensive against test doubles
        return None
