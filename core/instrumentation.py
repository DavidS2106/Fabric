"""
fabric_migration_validator/core/instrumentation.py
──────────────────────────────────────────────────
Per-table, per-validation execution accounting.

Every remote statement a connector runs is attributed to the validation that
asked for it, so a report can state exactly how many database queries, full
scans, metadata queries and batch queries one validation cost, how many rows it
read, and how often it reused a statistic another validation had already
computed.

Attribution uses a thread-local scope, because validations for one table run in
a thread pool and every connector read happens inside the validation that
requested it.
"""

from __future__ import annotations

import contextlib
import hashlib
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

#: How one statement is classified in the report.
QUERY_KINDS = ("full_scan", "batch_query", "metadata_query")

#: Connector operation name → default statement classification. Anything not
#: listed inherits the classification of the enclosing validation scope.
_OPERATION_KINDS = {
    "metadata_query": "metadata_query",
}


def _query_signature(statement: str) -> str:
    normalized = " ".join(str(statement).strip().upper().split())
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


@dataclass
class QueryExecution:
    """Timing and details for one executed SQL statement."""

    statement: str
    role: str
    elapsed_seconds: float
    rows_returned: Optional[int] = None
    timestamp_started: Optional[str] = None
    timestamp_completed: Optional[str] = None


@dataclass
class ScanCounters:
    """Accounting for one (table, phase, validation) execution."""

    table_id: Optional[str] = None
    phase: Optional[str] = None
    validation_name: Optional[str] = None
    planned_scan_group: Optional[str] = None
    actual_scan_group: Optional[str] = None
    shared_scan_used: bool = False
    database_queries: int = 0
    full_scans: int = 0
    batch_queries: int = 0
    metadata_queries: int = 0
    rows_read: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    reused_statistics: int = 0
    connections_established: int = 0
    connections_reused: int = 0
    statements: List[str] = field(default_factory=list)
    query_executions: List[QueryExecution] = field(default_factory=list)

    def as_details(self) -> Dict[str, Any]:
        """Report fields merged into ``CheckResult.details``."""
        signature_counts: Dict[str, int] = {}
        query_timings = []
        for query in self.query_executions:
            signature = _query_signature(query.statement)
            signature_counts[signature] = signature_counts.get(signature, 0) + 1
            query_timings.append({
                "signature": signature,
                "role": query.role,
                "elapsed_seconds": query.elapsed_seconds,
                "rows_returned": query.rows_returned,
            })
        return {
            "planned_scan_group": self.planned_scan_group,
            "actual_scan_group": self.actual_scan_group,
            "shared_scan_used": self.shared_scan_used,
            "database_queries": self.database_queries,
            "full_scans": self.full_scans,
            "batch_queries": self.batch_queries,
            "metadata_queries": self.metadata_queries,
            "rows_read": self.rows_read,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_reuse": self.reused_statistics,
            "reused_statistics": self.reused_statistics,
            "connections_established": self.connections_established,
            "connections_reused": self.connections_reused,
            "query_timings": query_timings,
            "query_signature_execution_counts": signature_counts,
        }

    def merge(self, other: "ScanCounters") -> None:
        for name in (
            "database_queries", "full_scans", "batch_queries", "metadata_queries",
            "rows_read", "cache_hits", "cache_misses", "reused_statistics",
            "connections_established", "connections_reused",
        ):
            setattr(self, name, getattr(self, name) + getattr(other, name))
        self.shared_scan_used = self.shared_scan_used or other.shared_scan_used
        if len(self.query_executions) < 100:
            self.query_executions.extend(
                other.query_executions[:100 - len(self.query_executions)]
            )


class ScanLedger:
    """Thread-safe collector of statement and cache accounting for one run."""

    def __init__(self, enabled: bool = True) -> None:
        self.enabled = bool(enabled)
        self._guard = threading.Lock()
        self._records: Dict[Tuple[Any, Any, Any], ScanCounters] = {}
        self._scope = threading.local()

    # ── Scope management ──────────────────────────────────────
    @contextlib.contextmanager
    def scope(
        self,
        table_id: Optional[str] = None,
        phase: Optional[str] = None,
        validation_name: Optional[str] = None,
        default_kind: str = "full_scan",
        planned_scan_group: Optional[str] = None,
    ):
        """Attribute every statement inside the block to one validation."""
        if not self.enabled:
            yield None
            return
        if default_kind not in QUERY_KINDS:
            raise ValueError(
                f"Unsupported query kind '{default_kind}'. Supported: "
                + ", ".join(QUERY_KINDS)
            )
        counters = self._counters(table_id, phase, validation_name)
        if planned_scan_group is not None:
            counters.planned_scan_group = planned_scan_group
        previous = getattr(self._scope, "current", None)
        self._scope.current = (counters, default_kind)
        try:
            yield counters
        finally:
            self._scope.current = previous

    @contextlib.contextmanager
    def statement_kind(self, kind: str):
        """Reclassify statements inside the block without changing attribution."""
        if not self.enabled:
            yield
            return
        current = getattr(self._scope, "current", None)
        if current is None:
            yield
            return
        counters, _ = current
        self._scope.current = (counters, kind)
        try:
            yield
        finally:
            self._scope.current = current

    def current(self) -> Optional[ScanCounters]:
        current = getattr(self._scope, "current", None)
        return current[0] if current else None

    # ── Recording ─────────────────────────────────────────────
    def record_statement(
        self,
        operation: Optional[str] = None,
        rows: Optional[int] = None,
        statement: Optional[str] = None,
    ) -> None:
        """Record one executed statement against the enclosing scope."""
        if not self.enabled:
            return
        current = getattr(self._scope, "current", None)
        if current is None:
            return
        counters, default_kind = current
        kind = _OPERATION_KINDS.get(str(operation or ""), default_kind)
        with self._guard:
            counters.database_queries += 1
            if kind == "full_scan":
                counters.full_scans += 1
            elif kind == "batch_query":
                counters.batch_queries += 1
            else:
                counters.metadata_queries += 1
            if isinstance(rows, int) and not isinstance(rows, bool):
                counters.rows_read += rows
            if statement is not None and len(counters.statements) < 25:
                counters.statements.append(str(statement)[:500])

    def record_cache(self, hit: bool, reused_statistics: int = 0) -> None:
        """Record one execution-cache lookup against the enclosing scope."""
        if not self.enabled:
            return
        counters = self.current()
        if counters is None:
            return
        with self._guard:
            if hit:
                counters.cache_hits += 1
                counters.reused_statistics += max(int(reused_statistics or 0), 1)
            else:
                counters.cache_misses += 1

    def record_connection(self, established: bool) -> None:
        if not self.enabled:
            return
        counters = self.current()
        if counters is None:
            return
        with self._guard:
            if established:
                counters.connections_established += 1
            else:
                counters.connections_reused += 1

    def record_query_execution(
        self,
        statement: str,
        role: str,
        elapsed_seconds: float,
        rows_returned: Optional[int] = None,
        timestamp_started: Optional[str] = None,
        timestamp_completed: Optional[str] = None,
    ) -> None:
        """Record timing and metadata for one SQL statement execution.

        Note: counters are incremented by record_statement(), not here.
        This method captures detailed timing for diagnostics.
        """
        if not self.enabled:
            return
        counters = self.current()
        if counters is None:
            return
        execution = QueryExecution(
            statement=statement,
            role=role,
            elapsed_seconds=round(float(elapsed_seconds), 6),
            rows_returned=rows_returned,
            timestamp_started=timestamp_started,
            timestamp_completed=timestamp_completed,
        )
        with self._guard:
            if len(counters.query_executions) < 100:
                counters.query_executions.append(execution)

    def record_scan_group(
        self,
        table_id: Optional[str],
        phase: Optional[str],
        validation_name: Optional[str],
        planned: Optional[str] = None,
        actual: Optional[str] = None,
        shared: Optional[bool] = None,
    ) -> None:
        if not self.enabled:
            return
        counters = self._counters(table_id, phase, validation_name)
        with self._guard:
            if planned is not None:
                counters.planned_scan_group = planned
            if actual is not None:
                counters.actual_scan_group = actual
            if shared is not None:
                counters.shared_scan_used = bool(shared)

    # ── Reading ───────────────────────────────────────────────
    def counters(
        self,
        table_id: Optional[str],
        phase: Optional[str],
        validation_name: Optional[str],
    ) -> ScanCounters:
        return self._counters(table_id, phase, validation_name)

    def details_for(
        self,
        table_id: Optional[str],
        phase: Optional[str],
        validation_name: Optional[str],
    ) -> Dict[str, Any]:
        if not self.enabled:
            return {}
        with self._guard:
            record = self._records.get((table_id, phase, validation_name))
            return record.as_details() if record is not None else {}

    def table_totals(
        self, table_id: Optional[str], phase: Optional[str] = None
    ) -> Dict[str, Any]:
        """Aggregate every validation recorded for one table."""
        if not self.enabled:
            return {}
        total = ScanCounters(table_id=table_id, phase=phase)
        with self._guard:
            records = [
                record for key, record in self._records.items()
                if key[0] == table_id and (phase is None or key[1] == phase)
            ]
        for record in records:
            total.merge(record)
        details = total.as_details()
        details.pop("planned_scan_group", None)
        details.pop("actual_scan_group", None)
        return details

    def records(self) -> List[Dict[str, Any]]:
        with self._guard:
            return [
                {
                    "table_id": record.table_id,
                    "phase": record.phase,
                    "validation_name": record.validation_name,
                    **record.as_details(),
                }
                for record in self._records.values()
            ]

    def merge(self, other: Optional["ScanLedger"]) -> None:
        """Absorb another ledger, e.g. one produced inside a worker process."""
        if other is None or not self.enabled:
            return
        for record in other.records():
            counters = self._counters(
                record.get("table_id"),
                record.get("phase"),
                record.get("validation_name"),
            )
            with self._guard:
                counters.database_queries += int(record.get("database_queries", 0))
                counters.full_scans += int(record.get("full_scans", 0))
                counters.batch_queries += int(record.get("batch_queries", 0))
                counters.metadata_queries += int(record.get("metadata_queries", 0))
                counters.rows_read += int(record.get("rows_read", 0))
                counters.cache_hits += int(record.get("cache_hits", 0))
                counters.cache_misses += int(record.get("cache_misses", 0))
                counters.reused_statistics += int(record.get("cache_reuse", 0))

    def summary(self) -> Dict[str, Any]:
        """Run-level scan accounting for the run report."""
        if not self.enabled:
            return {}
        total = ScanCounters()
        with self._guard:
            records = list(self._records.values())
        for record in records:
            total.merge(record)
        details = total.as_details()
        details.pop("planned_scan_group", None)
        details.pop("actual_scan_group", None)
        details["validations_instrumented"] = len(records)
        return details

    # ── Internals ─────────────────────────────────────────────
    def _counters(
        self,
        table_id: Optional[str],
        phase: Optional[str],
        validation_name: Optional[str],
    ) -> ScanCounters:
        key = (table_id, phase, validation_name)
        with self._guard:
            record = self._records.get(key)
            if record is None:
                record = ScanCounters(
                    table_id=table_id,
                    phase=phase,
                    validation_name=validation_name,
                )
                self._records[key] = record
            return record


def record_statement(ledger: Optional[ScanLedger], **fields: Any) -> None:
    """Record a statement when a ledger is present, otherwise do nothing."""
    if ledger is not None:
        ledger.record_statement(**fields)


@contextlib.contextmanager
def optional_scope(ledger: Optional[ScanLedger], **fields: Any):
    """Enter a ledger scope when instrumentation is configured."""
    if ledger is None:
        yield None
        return
    with ledger.scope(**fields) as counters:
        yield counters
