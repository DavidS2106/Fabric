"""Thread-safe, run-local performance primitives."""

from __future__ import annotations

import contextlib
import threading
import time
from typing import Any, Callable, Dict, Hashable, List, Optional


class CachedFailure(RuntimeError):
    """A computation already failed for this key during the current table run.

    Raised instead of re-running the computation, so a statement that timed out
    (or failed deterministically) for one consumer is not issued again by the
    next one. ``error_class`` is one of ``statement_timeout``,
    ``transient_connection`` or ``database_error``; ``original`` is the first
    failure and ``first_consumer`` a label of who issued it.
    """

    def __init__(self, key: Hashable, original: BaseException, error_class: str,
                 first_consumer: Optional[str] = None) -> None:
        label = f" by {first_consumer}" if first_consumer else ""
        super().__init__(
            f"{_describe_key(key)} unavailable: cached {error_class} from the first "
            f"attempt{label}; the statement was not executed again "
            f"({type(original).__name__}: {original})"
        )
        self.key = key
        self.original = original
        self.error_class = error_class
        self.first_consumer = first_consumer


def _describe_key(key: Hashable) -> str:
    if isinstance(key, tuple) and key and key[0] == "row_count":
        side = key[1] if len(key) > 1 else "?"
        return f"{side} row count"
    return f"cached result {key!r}"


def classify_failure(exc: BaseException) -> str:
    """Map a database failure to the class the count cache and logs report."""
    if getattr(exc, "query_timeout", False):
        return "statement_timeout"
    if getattr(exc, "transient", False):
        return "transient_connection"
    message = str(getattr(exc, "orig", None) or exc)
    lowered = message.lower()
    if "hyt00" in lowered or "query timeout expired" in lowered:
        return "statement_timeout"
    if isinstance(exc, (ConnectionError, TimeoutError)):
        return "transient_connection"
    return "database_error"


class ExecutionResultCache:
    """Single-flight cache: one database computation per reusable result key.

    Supports hierarchical caching with metadata versioning to enable cache
    invalidation when table schemas or configurations change.

    A computation may also record its *failure* (``cache_failures=True`` or
    :meth:`put_failure`). A later request for the same key then raises
    :class:`CachedFailure` immediately instead of repeating the statement: a
    statement timeout is not a cache miss.
    """

    def __init__(self) -> None:
        self._values: Dict[Hashable, Any] = {}
        self._failures: Dict[Hashable, CachedFailure] = {}
        self._locks: Dict[Hashable, threading.Lock] = {}
        self._guard = threading.Lock()
        self._metadata_version: Dict[str, int] = {}

    def get_or_compute(
        self,
        key: Hashable,
        compute: Callable[[], Any],
        *,
        cache_failures: bool = False,
        consumer: Optional[str] = None,
    ) -> Any:
        with self._guard:
            lock = self._locks.setdefault(key, threading.Lock())
        with lock:
            with self._guard:
                if key in self._values:
                    return self._values[key]
                failure = self._failures.get(key)
            if failure is not None:
                raise failure
            try:
                value = compute()
            except Exception as exc:
                if cache_failures and not isinstance(exc, CachedFailure):
                    self.put_failure(key, exc, consumer=consumer)
                raise
            with self._guard:
                self._values[key] = value
            return value

    def put(self, key: Hashable, value: Any) -> None:
        with self._guard:
            self._values[key] = value
            self._failures.pop(key, None)

    def put_failure(
        self, key: Hashable, exc: BaseException, consumer: Optional[str] = None
    ) -> None:
        """Record that computing ``key`` failed, unless a value already exists."""
        if isinstance(exc, CachedFailure):
            exc = exc.original
        with self._guard:
            if key in self._values or key in self._failures:
                return
            self._failures[key] = CachedFailure(
                key, exc, classify_failure(exc), first_consumer=consumer
            )

    def peek_failure(self, key: Hashable) -> Optional[CachedFailure]:
        """The cached failure for ``key``, when its computation already failed."""
        with self._guard:
            return self._failures.get(key)

    def peek(self, key: Hashable, default: Any = None) -> Any:
        """Return an already-computed value without triggering computation."""
        with self._guard:
            return self._values.get(key, default)

    def get_many(self, keys: List[Hashable], default: Any = None) -> Dict[Hashable, Any]:
        """Retrieve multiple cached values in a single guard acquisition."""
        with self._guard:
            return {key: self._values.get(key, default) for key in keys}

    def put_many(self, items: Dict[Hashable, Any]) -> None:
        """Store multiple values in a single guard acquisition."""
        with self._guard:
            self._values.update(items)

    def invalidate_namespace(self, namespace_prefix: str) -> int:
        """Invalidate all cache entries matching a namespace prefix."""
        with self._guard:
            to_remove = [
                key for key in self._values.keys()
                if isinstance(key, tuple) and len(key) > 0
                and str(key[0]).startswith(namespace_prefix)
            ]
            count = len(to_remove)
            for key in to_remove:
                self._values.pop(key, None)
                self._failures.pop(key, None)
                self._locks.pop(key, None)
            return count

    def clear(self) -> Dict[str, int]:
        """Release every table-local cached value and single-flight lock."""
        with self._guard:
            released = len(self._values)
            locks = len(self._locks)
            self._values.clear()
            self._failures.clear()
            self._locks.clear()
            self._metadata_version.clear()
            for name in (
                "shared_statistics",
                "validation_policies",
                "validations_config",
                "scan_ledger",
            ):
                if hasattr(self, name):
                    setattr(self, name, None)
            return {"cache_entries_released": released, "cache_locks_released": locks}


class ConcurrencyGovernor:
    """One global ceiling on concurrent remote operations for a run.

    Table, validation, source/target and batch parallelism all draw from this
    single pool, so raising one worker count can never multiply into a load the
    configured Source, Target, network or driver cannot absorb.
    """

    def __init__(self, limit: int) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("Concurrency limit must be a positive integer")
        self._limit = limit
        self._semaphore = threading.BoundedSemaphore(limit)
        self._guard = threading.Lock()
        self._active = 0
        self._peak = 0
        self._acquisitions = 0
        self._wait_seconds = 0.0

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def peak_concurrency(self) -> int:
        with self._guard:
            return self._peak

    @property
    def total_acquisitions(self) -> int:
        with self._guard:
            return self._acquisitions

    @property
    def total_wait_seconds(self) -> float:
        with self._guard:
            return round(self._wait_seconds, 6)

    @contextlib.contextmanager
    def slot(self):
        """Hold one global concurrency slot for the duration of the block."""
        waiting_from = time.perf_counter()
        self._semaphore.acquire()
        waited = time.perf_counter() - waiting_from
        with self._guard:
            self._active += 1
            self._acquisitions += 1
            self._wait_seconds += waited
            if self._active > self._peak:
                self._peak = self._active
        try:
            yield self
        finally:
            with self._guard:
                self._active -= 1
            self._semaphore.release()

    def metrics(self) -> Dict[str, Any]:
        with self._guard:
            return {
                "global_concurrency_limit": self._limit,
                "peak_global_concurrency": self._peak,
                "total_remote_operations": self._acquisitions,
                "total_concurrency_wait_seconds": round(self._wait_seconds, 6),
            }


class LargeTableScheduler:
    """Scheduling ceilings for heavy scans on large tables.

    Gating changes only WHEN a heavy validation starts, never whether it runs
    or what it produces. A table counts as large only when a row count already
    observed this run meets the configured large-table threshold; unknown
    sizes are never delayed.
    """

    HEAVY_CHECKS = frozenset({"record_comparison", "data_value_validation"})

    def __init__(self, concurrency_config, large_table_config) -> None:
        self._config = concurrency_config
        self._threshold = int(
            getattr(large_table_config, "threshold_rows", 0) or 0
        ) if getattr(large_table_config, "enabled", False) else 0
        self._guard = threading.Lock()
        self._table_gates: Dict[Any, threading.BoundedSemaphore] = {}
        self._endpoint_gates: Dict[Any, threading.BoundedSemaphore] = {}

    def _gate(self, gates, key, limit) -> threading.BoundedSemaphore:
        with self._guard:
            gate = gates.get(key)
            if gate is None:
                gate = gates[key] = threading.BoundedSemaphore(limit)
            return gate

    def _is_large(self, execution_cache) -> bool:
        if self._threshold <= 0 or execution_cache is None:
            return False
        peek = getattr(execution_cache, "peek", None)
        if not callable(peek):
            return False
        for role in ("source", "target"):
            try:
                count = peek(("row_count", role))
            except Exception:
                continue
            if isinstance(count, (int, float)) and count >= self._threshold:
                return True
        return False

    @contextlib.contextmanager
    def slot(self, check_name: str, table_map: Any, execution_cache: Any):
        """Hold the configured scheduling gates around one heavy validation."""
        if (
            not getattr(self._config, "enabled", False)
            or check_name not in self.HEAVY_CHECKS
        ):
            yield
            return
        # Fixed acquisition order (table, source endpoint, target endpoint)
        # keeps gate waits deadlock-free.
        held = []
        gates = [self._gate(
            self._table_gates,
            getattr(table_map, "id", None),
            self._config.max_heavy_validations_per_table,
        )]
        if self._is_large(execution_cache):
            gates.append(self._gate(
                self._endpoint_gates,
                ("source", getattr(table_map, "source_system", None)),
                self._config.max_large_tables_per_source_endpoint,
            ))
            gates.append(self._gate(
                self._endpoint_gates,
                ("target", getattr(table_map, "target_layer", None)),
                self._config.max_large_tables_per_target_endpoint,
            ))
        try:
            for gate in gates:
                gate.acquire()
                held.append(gate)
            yield
        finally:
            for gate in reversed(held):
                gate.release()


@contextlib.contextmanager
def optional_slot(governor: Optional[ConcurrencyGovernor]):
    """Acquire a governor slot when one is configured, otherwise pass through."""
    if governor is None:
        yield None
        return
    with governor.slot() as held:
        yield held


class PerformanceLedger:
    """Collects per-validation and per-connector-operation timings for a run."""

    #: Fields captured for every validation execution.
    VALIDATION_FIELDS = (
        "phase",
        "table_id",
        "source_system",
        "target_system",
        "validation_name",
        "execution_strategy",
        "source_execution_time",
        "target_execution_time",
        "comparison_time",
        "total_execution_time",
        "rows_processed",
        "batch_count",
        "batch_size",
        "worker_count",
    )

    def __init__(self, enabled: bool = True) -> None:
        self.enabled = bool(enabled)
        self._guard = threading.Lock()
        self._validations: List[Dict[str, Any]] = []
        self._operations: List[Dict[str, Any]] = []
        self._scope = threading.local()

    @contextlib.contextmanager
    def scope(self, **fields: Any):
        """Attribute connector operations to the current table/validator."""
        previous = getattr(self._scope, "fields", None)
        merged = dict(previous or {})
        merged.update({key: value for key, value in fields.items() if value is not None})
        self._scope.fields = merged
        try:
            yield
        finally:
            self._scope.fields = previous

    # ── Recording ─────────────────────────────────────────────
    def record_validation(self, **fields: Any) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None
        record = {name: fields.get(name) for name in self.VALIDATION_FIELDS}
        extra = {
            name: value
            for name, value in fields.items()
            if name not in self.VALIDATION_FIELDS
        }
        record.update(extra)
        with self._guard:
            self._validations.append(record)
        return record

    def record_operation(
        self,
        operation: str,
        seconds: float,
        system: Any = None,
        role: Any = None,
        table_id: Any = None,
        rows: Any = None,
        statement: Any = None,
        phase: Any = None,
        validation_name: Any = None,
    ) -> None:
        if not self.enabled:
            return
        scope = dict(getattr(self._scope, "fields", None) or {})
        table_id = table_id if table_id is not None else scope.get("table_id")
        phase = phase if phase is not None else scope.get("phase")
        validation_name = (
            validation_name
            if validation_name is not None
            else scope.get("validation_name")
        )
        query_kind = _query_kind(operation, statement)
        with self._guard:
            self._operations.append(
                {
                    "operation": operation,
                    "role": role,
                    "system": system,
                    "table_id": table_id,
                    "phase": phase,
                    "validation_name": validation_name,
                    "query_kind": query_kind,
                    "rows": rows,
                    "execution_time_seconds": round(float(seconds), 6),
                    "statement": statement,
                }
            )

    # ── Reading ───────────────────────────────────────────────
    @property
    def validations(self) -> List[Dict[str, Any]]:
        with self._guard:
            return [dict(item) for item in self._validations]

    @property
    def operations(self) -> List[Dict[str, Any]]:
        with self._guard:
            return [dict(item) for item in self._operations]

    def merge(self, other: "PerformanceLedger") -> None:
        """Absorb another ledger, e.g. one produced inside a worker process."""
        if other is None or not self.enabled:
            return
        with self._guard:
            self._validations.extend(other.validations)
            self._operations.extend(other.operations)

    # ── Summarizing ───────────────────────────────────────────
    def summary(self) -> Dict[str, Any]:
        """Return the slowest validation, table, connector operation and batching."""
        validations = self.validations
        operations = self.operations
        summary: Dict[str, Any] = {
            "validations_recorded": len(validations),
            "connector_operations_recorded": len(operations),
        }
        if not self.enabled:
            return summary

        slowest_validation = _max_by(validations, "total_execution_time")
        if slowest_validation:
            summary["slowest_validation"] = {
                "validation_name": slowest_validation.get("validation_name"),
                "table_id": slowest_validation.get("table_id"),
                "phase": slowest_validation.get("phase"),
                "execution_strategy": slowest_validation.get("execution_strategy"),
                "total_execution_time": slowest_validation.get("total_execution_time"),
            }

        by_table: Dict[Any, float] = {}
        for item in validations:
            elapsed = _number(item.get("total_execution_time"))
            if elapsed is None:
                continue
            by_table[item.get("table_id")] = by_table.get(item.get("table_id"), 0.0) + elapsed
        if by_table:
            table_id, total = max(by_table.items(), key=lambda pair: pair[1])
            summary["slowest_table"] = {
                "table_id": table_id,
                "total_execution_time": round(total, 6),
            }

        slowest_operation = _max_by(operations, "execution_time_seconds")
        if slowest_operation:
            summary["slowest_connector_operation"] = {
                "operation": slowest_operation.get("operation"),
                "role": slowest_operation.get("role"),
                "system": slowest_operation.get("system"),
                "table_id": slowest_operation.get("table_id"),
                "execution_time_seconds": slowest_operation.get(
                    "execution_time_seconds"
                ),
                "validation_name": slowest_operation.get("validation_name"),
                "query_kind": slowest_operation.get("query_kind"),
                "statement": slowest_operation.get("statement"),
            }

        highest_batches = _max_by(validations, "batch_count")
        if highest_batches and _number(highest_batches.get("batch_count")):
            summary["highest_batch_count"] = {
                "validation_name": highest_batches.get("validation_name"),
                "table_id": highest_batches.get("table_id"),
                "batch_count": highest_batches.get("batch_count"),
                "batch_size": highest_batches.get("batch_size"),
            }

        strategies: Dict[str, int] = {}
        for item in validations:
            strategy = item.get("execution_strategy")
            if strategy is None:
                continue
            strategies[str(strategy)] = strategies.get(str(strategy), 0) + 1
        if strategies:
            summary["execution_strategy_counts"] = strategies
        duplicate_groups: Dict[Any, Dict[str, Any]] = {}
        for item in operations:
            statement = item.get("statement")
            if not statement:
                continue
            signature = _query_signature(statement)
            if not signature:
                continue
            key = (
                item.get("table_id"), item.get("role"), signature,
            )
            group = duplicate_groups.setdefault(key, {
                "table_id": item.get("table_id"),
                "role": item.get("role"),
                "validation_names": set(),
                "statement": statement,
                "statement_hash": hash(signature) & 0x7FFFFFFF,
                "execution_count": 0,
                "first_execution_time_seconds": float(item.get("execution_time_seconds") or 0),
                "last_execution_time_seconds": float(item.get("execution_time_seconds") or 0),
                "total_execution_time_seconds": 0.0,
                "query_kind": item.get("query_kind", "unknown"),
            })
            if item.get("validation_name") is not None:
                group["validation_names"].add(item.get("validation_name"))
            group["execution_count"] += 1
            group["total_execution_time_seconds"] += float(
                item.get("execution_time_seconds") or 0
            )
            group["last_execution_time_seconds"] = float(
                item.get("execution_time_seconds") or 0
            )
        duplicates = []
        for group in duplicate_groups.values():
            if group["execution_count"] > 1:
                group["validation_names"] = sorted(group["validation_names"])
                group["total_execution_time_seconds"] = round(
                    group["total_execution_time_seconds"], 6
                )
                group["first_execution_time_seconds"] = round(
                    group["first_execution_time_seconds"], 6
                )
                group["last_execution_time_seconds"] = round(
                    group["last_execution_time_seconds"], 6
                )
                group["average_execution_time_seconds"] = round(
                    group["total_execution_time_seconds"] / group["execution_count"], 6
                )
                duplicates.append(group)
        summary["duplicate_query_groups"] = sorted(
            duplicates,
            key=lambda item: item["total_execution_time_seconds"],
            reverse=True,
        )[:50]
        summary["total_duplicate_query_groups"] = len(duplicates)
        summary["total_duplicate_executions"] = sum(
            item["execution_count"] - 1 for item in duplicates
        )
        return summary


def _number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _query_signature(statement: Any) -> str:
    """Normalize SQL to a canonical signature for duplicate detection."""
    if not statement:
        return ""
    sql = str(statement or "").strip()
    normalized = " ".join(sql.split()).casefold()
    return normalized


def _query_kind(operation: Any, statement: Any) -> str:
    operation_name = str(operation or "").casefold()
    sql = " ".join(str(statement or "").split()).casefold()
    if operation_name == "metadata_query" or any(
        token in sql for token in ("information_schema", "sys.tables", "sys.columns")
    ):
        return "metadata_query"
    if operation_name == "streamed_read_sql":
        return "streaming_query"
    if any(token in sql for token in ("count(", "sum(", "min(", "max(", "avg(")):
        return "aggregate_query"
    if " order by " in sql and any(
        token in sql for token in (" where ", " limit ", " fetch next ", " top ")
    ):
        return "indexed_or_key_query"
    return "full_table_query"


def _max_by(records: List[Dict[str, Any]], field: str) -> Optional[Dict[str, Any]]:
    candidates = [
        record for record in records if _number(record.get(field)) is not None
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda record: _number(record.get(field)) or 0.0)
