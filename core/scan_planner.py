"""
fabric_migration_validator/core/scan_planner.py
───────────────────────────────────────────────
The shared statistics scan layer.

Row counts, zero-record detection, per-column NULL counts, key NULL counts,
duplicate-key statistics, distinct-key counts, numeric aggregates and
audit-column population are all *statistics over one table under one filter*.
Computing them independently costs one scan each. This module collects the
statistics that the checks enabled for the selected profile actually need, and
computes them with as few scans as the configured connectors allow — normally
one statement per side.

The plan is built only from enabled checks, so a disabled check contributes no
expression and costs nothing. Results land in the run-local
:class:`~core.performance.ExecutionResultCache`, keyed by role, scope and
statistic, so any validator can consume a statistic another validator already
paid for.

Nothing here knows a platform: every expression is rendered through the
connection's configured :class:`~core.sql_dialect.SqlDialect`.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from core.capabilities import resolve_capabilities
from core.instrumentation import ScanLedger
from core.sql_dialect import SqlDialect, resolve_dialect

logger = logging.getLogger(__name__)

#: Statistics derived from a per-key grouped scan.
KEY_GROUPED_KINDS = frozenset({
    "key_null_rows",
    "key_distinct_count",
    "key_duplicate_groups",
    "key_duplicate_rows",
})

#: Statistics computable from either a flat or a key-grouped scan.
COMPOSABLE_KINDS = frozenset({
    "row_count",
    "null_count",
    "min",
    "max",
    "sum",
    "count",
})

#: Statistics that cannot be derived from a key-grouped scan.
FLAT_ONLY_KINDS = frozenset({"distinct_count"})

SUPPORTED_KINDS = KEY_GROUPED_KINDS | COMPOSABLE_KINDS | FLAT_ONLY_KINDS

#: Result aliases the key-grouped statement always produces. These names are
#: part of the framework's own SQL contract, not a platform's.
TOTAL_ROWS_ALIAS = "total_row_count"
NULL_KEY_ALIAS = "null_key_count"
DISTINCT_KEY_ALIAS = "distinct_key_count"
DUPLICATE_GROUP_ALIAS = "duplicate_group_count"
DUPLICATE_ROW_ALIAS = "duplicate_row_count"

#: Canonical statistic keys. Every check that needs one of these asks for it by
#: the same name, which is what lets one scan serve all of them.
ROW_COUNT_KEY = "row_count"
KEY_NULL_ROWS_KEY = NULL_KEY_ALIAS
KEY_DISTINCT_COUNT_KEY = DISTINCT_KEY_ALIAS
KEY_DUPLICATE_GROUPS_KEY = DUPLICATE_GROUP_ALIAS
KEY_DUPLICATE_ROWS_KEY = DUPLICATE_ROW_ALIAS


def null_count_key(name: str) -> str:
    """Canonical key for one configured column's NULL count."""
    return f"null_count:{name}"


def aggregate_key(kind: str, name: str) -> str:
    """Canonical key for one configured column's aggregate."""
    return f"{kind}:{name}"


def audit_null_count_key(column: str) -> str:
    """Canonical key for one configured audit column's NULL count."""
    return f"audit_null_count:{column}"


_GROUP_SIZE_ALIAS = "__mv_group_rows"
_NULL_KEY_FLAG_ALIAS = "__mv_null_key"
_INNER_PREFIX = "__mv_inner"
_OUTER_PREFIX = "__mv_stat"
_INNER_TABLE_ALIAS = "__mv_key_groups"


class ScanPlanError(ValueError):
    """Raised when a statistics request cannot be planned."""


#: Roles a statistic may be computed for.
ROLES = ("source", "target")


@dataclass(frozen=True)
class StatisticRequest:
    """One statistic a check needs, expressed without any SQL.

    ``roles`` lets a statistic apply to one side only. Target audit columns, for
    example, do not exist on the source, so requesting them for both sides would
    make the source scan fail and take every other statistic down with it.
    """

    key: str
    kind: str
    source_column: Optional[str] = None
    target_column: Optional[str] = None
    requested_by: Tuple[str, ...] = ()
    roles: Tuple[str, ...] = ROLES

    def column_for(self, role: str) -> Optional[str]:
        return self.source_column if role == "source" else self.target_column

    def applies_to(self, role: str) -> bool:
        return role in self.roles

    def __post_init__(self) -> None:
        if self.kind not in SUPPORTED_KINDS:
            raise ScanPlanError(
                f"Unsupported statistic kind '{self.kind}'. Supported: "
                + ", ".join(sorted(SUPPORTED_KINDS))
            )
        unknown_roles = sorted(set(self.roles) - set(ROLES))
        if unknown_roles or not self.roles:
            raise ScanPlanError(
                f"Statistic '{self.key}' must apply to at least one of: "
                + ", ".join(ROLES)
            )
        needs_column = self.kind in (
            COMPOSABLE_KINDS - {"row_count"}
        ) | FLAT_ONLY_KINDS
        if needs_column and any(
            not self.column_for(role) for role in self.roles
        ):
            raise ScanPlanError(
                f"Statistic '{self.key}' of kind '{self.kind}' requires a "
                "column for every role it applies to"
            )


def statistic_cache_key(role: str, scope_token: str, statistic_key: str) -> Tuple:
    """Run-local cache key for one computed statistic."""
    return ("statistic", role, scope_token, statistic_key)


def statistics_scan_cache_key(role: str, scope_token: str, signature: str) -> Tuple:
    """Run-local cache key for one executed statistics scan."""
    return ("statistics_scan", role, scope_token, signature)


@dataclass
class ScanExecution:
    """What one side's statistics scan actually did, for the report."""

    role: str
    statements: List[str] = field(default_factory=list)
    values: Dict[str, Any] = field(default_factory=dict)
    elapsed: float = 0.0
    grouped: bool = False
    error: Optional[Exception] = None

    @property
    def statement_count(self) -> int:
        return len(self.statements)


class StatisticsScanPlan:
    """The statements one side needs to satisfy a set of statistic requests."""

    def __init__(
        self,
        requests: Sequence[StatisticRequest],
        key_columns: Dict[str, List[str]],
        max_expressions: int = 0,
    ) -> None:
        self._requests = list(dict.fromkeys(requests))
        self._by_key = {request.key: request for request in self._requests}
        self._key_columns = {
            role: list(columns or []) for role, columns in key_columns.items()
        }
        self._max_expressions = max(int(max_expressions or 0), 0)

    # ── Introspection ─────────────────────────────────────────
    @property
    def requests(self) -> Tuple[StatisticRequest, ...]:
        return tuple(self._requests)

    @property
    def keys(self) -> Tuple[str, ...]:
        return tuple(self._by_key)

    def request(self, key: str) -> Optional[StatisticRequest]:
        return self._by_key.get(key)

    def key_columns(self, role: str) -> List[str]:
        return list(self._key_columns.get(role, []))

    def requests_for(self, role: str) -> List[StatisticRequest]:
        """Only the statistics this side is asked to compute."""
        return [request for request in self._requests if request.applies_to(role)]

    def provides(self, key: str, role: Optional[str] = None) -> bool:
        request = self._by_key.get(key)
        if request is None:
            return False
        return role is None or request.applies_to(role)

    @property
    def is_empty(self) -> bool:
        return not self._requests

    def needs_grouped_scan(self, role: str) -> bool:
        """Whether a per-key grouped scan is required for this side."""
        if not self._key_columns.get(role):
            return False
        return any(
            request.kind in KEY_GROUPED_KINDS
            for request in self.requests_for(role)
        )

    def signature(self, role: str) -> str:
        """Stable identity of the statements this plan renders for one side."""
        parts = [
            f"{request.kind}:{request.column_for(role) or ''}:{request.key}"
            for request in self.requests_for(role)
        ]
        parts.append("keys=" + ",".join(self._key_columns.get(role, [])))
        parts.append(f"max={self._max_expressions}")
        return "|".join(sorted(parts))

    # ── SQL rendering ─────────────────────────────────────────
    def statements(
        self, role: str, dialect: SqlDialect, fqn: str, where_clause: str
    ) -> List[Tuple[str, List[Tuple[str, str]]]]:
        """Render every statement, each paired with its (alias, key) mapping."""
        grouped = self.needs_grouped_scan(role)
        applicable = self.requests_for(role)
        composable = [
            request for request in applicable
            if request.kind in COMPOSABLE_KINDS
        ]
        flat_only = [
            request for request in applicable if request.kind in FLAT_ONLY_KINDS
        ]
        rendered: List[Tuple[str, List[Tuple[str, str]]]] = []

        if grouped:
            rendered.extend(
                self._grouped_statements(
                    role, dialect, fqn, where_clause, composable
                )
            )
        elif composable:
            rendered.extend(
                self._flat_statements(role, dialect, fqn, where_clause, composable)
            )

        if flat_only:
            rendered.extend(
                self._flat_statements(role, dialect, fqn, where_clause, flat_only)
            )
        return rendered

    # ── Internals ─────────────────────────────────────────────
    def _chunks(self, requests: Sequence[StatisticRequest], reserved: int = 0):
        limit = self._max_expressions
        if limit <= 0:
            yield list(requests)
            return
        available = max(limit - reserved, 1)
        if available >= len(requests):
            yield list(requests)
            return
        for index in range(0, len(requests), available):
            yield list(requests[index:index + available])

    def _flat_statements(
        self,
        role: str,
        dialect: SqlDialect,
        fqn: str,
        where_clause: Any,
        requests: Sequence[StatisticRequest],
    ) -> List[Tuple[str, List[Tuple[str, str]]]]:
        rendered = []
        for chunk in self._chunks(requests):
            if not chunk:
                continue
            expressions = []
            mapping = []
            for index, request in enumerate(chunk):
                alias = f"{_OUTER_PREFIX}_{index}"
                expressions.append(
                    f"{self._flat_expression(request, role, dialect)} AS {alias}"
                )
                mapping.append((alias, request.key))
            rendered.append((
                dialect.select(", ".join(expressions), fqn, where_clause),
                mapping,
            ))
        return rendered

    def _grouped_statements(
        self,
        role: str,
        dialect: SqlDialect,
        fqn: str,
        where_clause: str,
        composable: Sequence[StatisticRequest],
    ) -> List[Tuple[str, List[Tuple[str, str]]]]:
        keys = [dialect.quote(key) for key in self._key_columns[role]]
        key_list = ", ".join(keys)
        null_condition = " OR ".join(f"{key} IS NULL" for key in keys)

        rendered = []
        # The first statement always carries the key statistics; five outer
        # expressions are reserved for them.
        for position, chunk in enumerate(self._chunks(composable, reserved=5)):
            inner_expressions = [
                f"{key_list}",
                f"{dialect.count_all()} AS {_GROUP_SIZE_ALIAS}",
            ]
            outer_expressions: List[str] = []
            mapping: List[Tuple[str, str]] = []
            if position == 0:
                inner_expressions.append(
                    f"MAX(CASE WHEN {null_condition} THEN 1 ELSE 0 END) "
                    f"AS {_NULL_KEY_FLAG_ALIAS}"
                )
                outer_expressions.extend([
                    f"COALESCE(SUM({_GROUP_SIZE_ALIAS}), 0) AS {TOTAL_ROWS_ALIAS}",
                    f"COALESCE(SUM(CASE WHEN {_NULL_KEY_FLAG_ALIAS} = 1 THEN "
                    f"{_GROUP_SIZE_ALIAS} ELSE 0 END), 0) AS {NULL_KEY_ALIAS}",
                    f"{dialect.count_column('*')} AS {DISTINCT_KEY_ALIAS}",
                    f"COALESCE(SUM(CASE WHEN {_GROUP_SIZE_ALIAS} > 1 THEN 1 "
                    f"ELSE 0 END), 0) AS {DUPLICATE_GROUP_ALIAS}",
                    f"COALESCE(SUM(CASE WHEN {_GROUP_SIZE_ALIAS} > 1 THEN "
                    f"{_GROUP_SIZE_ALIAS} ELSE 0 END), 0) AS {DUPLICATE_ROW_ALIAS}",
                ])
                mapping.extend([
                    (TOTAL_ROWS_ALIAS, TOTAL_ROWS_ALIAS),
                    (NULL_KEY_ALIAS, NULL_KEY_ALIAS),
                    (DISTINCT_KEY_ALIAS, DISTINCT_KEY_ALIAS),
                    (DUPLICATE_GROUP_ALIAS, DUPLICATE_GROUP_ALIAS),
                    (DUPLICATE_ROW_ALIAS, DUPLICATE_ROW_ALIAS),
                ])

            for index, request in enumerate(chunk):
                inner_alias = f"{_INNER_PREFIX}_{index}"
                outer_alias = f"{_OUTER_PREFIX}_{index}"
                inner, outer = self._grouped_expression(
                    request, role, dialect, inner_alias
                )
                inner_expressions.append(f"{inner} AS {inner_alias}")
                outer_expressions.append(f"{outer} AS {outer_alias}")
                mapping.append((outer_alias, request.key))

            if not outer_expressions:
                continue
            inner = dialect.select(
                ", ".join(inner_expressions),
                fqn,
                where_clause,
            ) + f" GROUP BY {key_list}"
            source = dialect.derived_table(inner, _INNER_TABLE_ALIAS)
            rendered.append((
                dialect.select(", ".join(outer_expressions), source),
                mapping,
            ))
        return rendered

    def _flat_expression(
        self, request: StatisticRequest, role: str, dialect: SqlDialect
    ) -> str:
        column = request.column_for(role)
        quoted_column = dialect.quote(column) if column is not None else None
        kind = request.kind
        if kind == "row_count":
            return dialect.count_all()
        if kind == "null_count":
            return f"SUM(CASE WHEN {quoted_column} IS NULL THEN 1 ELSE 0 END)"
        if kind == "distinct_count":
            return dialect.count_distinct(quoted_column)
        if kind == "count":
            return dialect.count_column(quoted_column)
        if kind in {"min", "max", "sum"}:
            return f"{kind.upper()}({quoted_column})"
        raise ScanPlanError(
            f"Statistic kind '{kind}' cannot be computed by a flat scan"
        )

    def _grouped_expression(
        self,
        request: StatisticRequest,
        role: str,
        dialect: SqlDialect,
        inner_alias: str,
    ) -> Tuple[str, str]:
        """Return the inner (per-group) and outer (roll-up) expressions.

        Every composable statistic is decomposable over a partition of the
        rows, which is what makes one grouped scan able to answer both the key
        statistics and the column statistics exactly.
        """
        column = request.column_for(role)
        quoted_column = dialect.quote(column) if column is not None else None
        kind = request.kind
        if kind == "row_count":
            return dialect.count_all(), f"COALESCE(SUM({inner_alias}), 0)"
        if kind == "null_count":
            return (
                f"SUM(CASE WHEN {quoted_column} IS NULL THEN 1 ELSE 0 END)",
                f"COALESCE(SUM({inner_alias}), 0)",
            )
        if kind == "count":
            return dialect.count_column(quoted_column), f"COALESCE(SUM({inner_alias}), 0)"
        if kind == "sum":
            return f"SUM({quoted_column})", f"SUM({inner_alias})"
        if kind == "min":
            return f"MIN({quoted_column})", f"MIN({inner_alias})"
        if kind == "max":
            return f"MAX({quoted_column})", f"MAX({inner_alias})"
        raise ScanPlanError(
            f"Statistic kind '{kind}' cannot be derived from a grouped scan"
        )


class SharedStatistics:
    """Single-flight access to one table's statistics for every validator.

    The first validator that needs any statistic triggers the scan for that
    side. Every later request — from any validation, on any thread — is served
    from the run-local cache and recorded as reuse.
    """

    def __init__(
        self,
        plan: StatisticsScanPlan,
        execution_cache: Any,
        connections: Dict[str, Any],
        table_sources: Dict[str, str],
        where_clause: str,
        scope_token: str,
        run_both_sides: Optional[Callable] = None,
        scan_ledger: Optional[ScanLedger] = None,
        compatibility_writers: Sequence[Callable[[str, Dict[str, Any]], None]] = (),
        failure_writers: Sequence[Callable[[str, BaseException], None]] = (),
    ) -> None:
        self.plan = plan
        self._cache = execution_cache
        self._connections = dict(connections)
        self._sources = dict(table_sources)
        # Full-load phases normally supply one empty clause. Incremental phases
        # may use different source/target column names (and dialect quoting), so
        # they supply a role-keyed mapping instead.
        self._where = where_clause
        self._scope_token = scope_token
        self._run_both_sides = run_both_sides
        self._ledger = scan_ledger
        self._compatibility_writers = list(compatibility_writers)
        #: Told when a side's scan failed in the database (not a plan error),
        #: so the statistics it would have produced (the row count above all)
        #: are recorded as failed and never re-issued by another validator.
        self._failure_writers = list(failure_writers)
        self._executions: Dict[str, ScanExecution] = {}
        self._guard = threading.Lock()

    # ── Public access ─────────────────────────────────────────
    @property
    def scope_token(self) -> str:
        return self._scope_token

    @property
    def where_clause(self) -> Any:
        return self._where

    def where_clause_for(self, role: str) -> Any:
        """Return the phase scope rendered for one endpoint."""
        if isinstance(self._where, Mapping):
            return self._where.get(role, "")
        return self._where

    def provides(self, statistic_key: str, role: Optional[str] = None) -> bool:
        """Whether the shared scan produces one statistic, optionally per side."""
        if statistic_key in {
            TOTAL_ROWS_ALIAS, NULL_KEY_ALIAS, DISTINCT_KEY_ALIAS,
            DUPLICATE_GROUP_ALIAS, DUPLICATE_ROW_ALIAS,
        }:
            return True
        return self.plan.provides(statistic_key, role)

    def value(self, role: str, statistic_key: str, default: Any = None) -> Any:
        """Return one statistic, executing the shared scan at most once."""
        execution = self.execution(role)
        # A later statement may fail after an earlier statement published this
        # statistic. The known value remains authoritative and reusable.
        if statistic_key in execution.values:
            return execution.values[statistic_key]
        if execution.error is not None:
            raise execution.error
        return default

    def failure_contexts(self) -> List[Dict[str, Any]]:
        """Describe failed sides and the configured requests implicated."""
        from core.validation_policy import exception_category, invalid_column_names

        contexts = []
        for role in ROLES:
            execution = self._executions.get(role)
            if execution is None or execution.error is None:
                continue
            columns = invalid_column_names(execution.error)
            column_keys = {column.casefold() for column in columns}
            requesters = []
            if column_keys:
                for request in self.plan.requests_for(role):
                    column = request.column_for(role)
                    if column and column.casefold() in column_keys:
                        requesters.extend(request.requested_by)
            contexts.append({
                "dependency": "shared_statistics",
                "role": role,
                "category": exception_category(execution.error),
                "invalid_columns": columns,
                "requested_by": tuple(dict.fromkeys(requesters)),
            })
        return contexts

    def integer(self, role: str, statistic_key: str, default: int = 0) -> int:
        value = self.value(role, statistic_key, default)
        if value is None:
            return default
        return int(value)

    def statements(self, role: str) -> List[str]:
        return list(self.execution(role).statements)

    def statement(self, role: str) -> Any:
        statements = self.statements(role)
        if not statements:
            return None
        return statements[0] if len(statements) == 1 else statements

    def execution(self, role: str) -> ScanExecution:
        # FIX #10: Double-checked locking to reduce lock contention
        # Fast path: check without lock (most executions hit cache)
        existing = self._executions.get(role)
        if existing is not None:
            self._record_reuse(existing)
            return existing

        # Slow path: compute with lock held
        with self._guard:
            # Re-check after acquiring lock (another thread may have computed)
            existing = self._executions.get(role)
            if existing is not None:
                self._record_reuse(existing)
                return existing
            # Compute and cache
            execution = self._compute(role)
            self._executions[role] = execution
            return execution

    def prefetch(self) -> None:
        """Run both sides' statistics scans, concurrently when configured."""
        if self.plan.is_empty:
            return
        source_call = lambda: self.execution("source")
        target_call = lambda: self.execution("target")
        if self._run_both_sides is not None:
            self._run_both_sides(source_call, target_call)
            return
        source_call()
        target_call()

    def details(self) -> Dict[str, Any]:
        """Report fields describing the shared scan itself."""
        source = self._executions.get("source")
        target = self._executions.get("target")
        return {
            "shared_statistics_source_statements": (
                source.statement_count if source else 0
            ),
            "shared_statistics_target_statements": (
                target.statement_count if target else 0
            ),
            "shared_statistics_grouped": bool(
                (source and source.grouped) or (target and target.grouped)
            ),
            "shared_statistics_source_seconds": round(
                source.elapsed if source else 0.0, 6
            ),
            "shared_statistics_target_seconds": round(
                target.elapsed if target else 0.0, 6
            ),
            "shared_statistics_keys": list(self.plan.keys),
        }

    # ── Internals ─────────────────────────────────────────────
    def _record_reuse(self, execution: ScanExecution) -> None:
        if self._ledger is not None:
            self._ledger.record_cache(
                hit=True, reused_statistics=len(execution.values)
            )

    def _compute(self, role: str) -> ScanExecution:
        connection = self._connections.get(role)
        fqn = self._sources.get(role)
        if connection is None or not fqn:
            return ScanExecution(
                role=role,
                error=ScanPlanError(
                    f"No {role} connection or table source was supplied for the "
                    "shared statistics scan"
                ),
            )
        signature = self.plan.signature(role)
        cache_key = statistics_scan_cache_key(role, self._scope_token, signature)
        computed = self._cached(
            cache_key, lambda: self._execute(role, connection, fqn)
        )
        if isinstance(computed, ScanExecution):
            # Publish whatever the side computed *before* recording the
            # failure. A statement that timed out does not invalidate the
            # statements that already returned: a timeout on a later, far more
            # expensive statistic (distinct_count over millions of rows) used
            # to discard a COUNT_BIG(*) that had completed in seconds and then
            # mark the row count itself as unavailable, so every check needing
            # the count failed with FRAMEWORK_ERROR.
            #
            # Ordering carries the rest: ExecutionResultCache.put_failure is a
            # no-op for a key that already holds a value, so publishing first
            # means only genuinely uncomputed statistics are recorded as
            # failed. A failed scan is still cached as a failed scan, so no
            # statement is ever issued a second time.
            if computed.error is None or computed.values:
                self._publish(role, computed)
            if computed.error is not None and not isinstance(
                computed.error, ScanPlanError
            ):
                if computed.values:
                    # Statistic key names only: never a statement or a value.
                    logger.warning(
                        "[SHARED_STATISTICS] role=%s scan failed after computing "
                        "%d statistic(s) | computed=%s | error_type=%s",
                        role, len(computed.values),
                        ",".join(sorted(str(key) for key in computed.values)),
                        type(computed.error).__name__,
                    )
                for writer in self._failure_writers:
                    try:
                        writer(role, computed.error)
                    except Exception:  # pragma: no cover - bookkeeping only
                        logger.debug("Statistics failure writer failed", exc_info=True)
        return computed

    def _cached(self, key: Any, compute: Callable[[], Any]) -> Any:
        if self._cache is None:
            if self._ledger is not None:
                self._ledger.record_cache(hit=False)
            return compute()
        existing = self._cache.peek(key)
        if existing is not None:
            if self._ledger is not None:
                self._ledger.record_cache(hit=True)
            return existing
        if self._ledger is not None:
            self._ledger.record_cache(hit=False)
        return self._cache.get_or_compute(key, compute)

    def _execute(self, role: str, connection: Any, fqn: str) -> ScanExecution:
        dialect = resolve_dialect(connection)
        started = time.perf_counter()
        execution = ScanExecution(
            role=role, grouped=self.plan.needs_grouped_scan(role)
        )
        try:
            for sql, mapping in self.plan.statements(
                role, dialect, fqn, self.where_clause_for(role)
            ):
                execution.statements.append(sql)
                stmt_started = time.perf_counter()
                frame = connection.execute_query(sql)
                stmt_elapsed = round(time.perf_counter() - stmt_started, 6)

                if self._ledger is not None:
                    self._ledger.record_query_execution(
                        statement=sql,
                        role=role,
                        elapsed_seconds=stmt_elapsed,
                        rows_returned=len(frame) if hasattr(frame, '__len__') else None,
                    )

                if not isinstance(frame, pd.DataFrame):
                    raise ScanPlanError(
                        "Connector execute_query must return a pandas DataFrame"
                    )
                if frame.empty:
                    raise ScanPlanError(
                        "Shared statistics scan returned no result row"
                    )
                columns = {
                    str(column).casefold(): column for column in frame.columns
                }
                for position, (alias, statistic_key) in enumerate(mapping):
                    column = columns.get(alias.casefold())
                    if column is None:
                        if position < len(frame.columns):
                            column = frame.columns[position]
                        else:
                            raise ScanPlanError(
                                "Shared statistics scan did not return "
                                f"'{statistic_key}'"
                            )
                    value = frame.iloc[0][column]
                    execution.values[statistic_key] = (
                        None if pd.isna(value) else value
                    )
        except Exception as exc:  # recorded, not raised: callers decide
            from .object_dependency import invalid_object_error, missing_object, object_identity, cache_object_failure
            if invalid_object_error(exc):
                identity = (
                    self._cache.peek(("object_identity", role))
                    if self._cache is not None else None
                ) or object_identity(connection, table=fqn)
                exc = missing_object(role, identity)
                if self._cache is not None:
                    cache_object_failure(self._cache, exc, consumer="shared_statistics")
            execution.error = exc
            from .object_dependency import connection_dependency
            identity = (self._cache.peek(("object_identity", role)) if self._cache is not None else None) or object_identity(connection, table=fqn)
            dependency = connection_dependency(exc, role, identity)
            if dependency is not None:
                execution.error = dependency
                if self._cache is not None:
                    cache_object_failure(self._cache, dependency, consumer="shared_statistics")
            logger.debug("Shared statistics scan failed for %s", role, exc_info=True)
        finally:
            # One side renders several statements: the composable statistic
            # statement carrying the row count, then the flat-only ones such
            # as distinct_count. A later statement failing does not
            # invalidate the results the earlier ones already returned, so
            # the row-count aliasing is applied to whatever was collected
            # rather than only to a scan that completed in full.
            self._normalize(execution.values)
        execution.elapsed = round(time.perf_counter() - started, 6)
        return execution

    @staticmethod
    def _normalize(values: Dict[str, Any]) -> None:
        """Make the row count reachable under both of its canonical names.

        A key-grouped scan reports the row count as ``total_row_count`` and a
        flat scan as ``row_count``; every consumer may use either name.
        """
        if ROW_COUNT_KEY in values and TOTAL_ROWS_ALIAS not in values:
            values[TOTAL_ROWS_ALIAS] = values[ROW_COUNT_KEY]
        elif TOTAL_ROWS_ALIAS in values and ROW_COUNT_KEY not in values:
            values[ROW_COUNT_KEY] = values[TOTAL_ROWS_ALIAS]

    def _publish(self, role: str, execution: ScanExecution) -> None:
        """Expose every computed statistic through the run-local cache."""
        if self._cache is None:
            return
        for statistic_key, value in execution.values.items():
            self._cache.put(
                statistic_cache_key(role, self._scope_token, statistic_key), value
            )
        for writer in self._compatibility_writers:
            try:
                writer(role, dict(execution.values))
            except Exception:  # pragma: no cover - compatibility only
                logger.debug(
                    "Statistics compatibility writer failed", exc_info=True
                )


class SharedStatisticsBuilder:
    """Collect statistic requests from the checks enabled for one profile."""

    def __init__(self) -> None:
        self._requests: Dict[str, StatisticRequest] = {}
        self._key_columns: Dict[str, List[str]] = {"source": [], "target": []}

    def key_columns(self, source: Sequence[str], target: Sequence[str]) -> None:
        self._key_columns = {
            "source": list(source or []),
            "target": list(target or []),
        }

    def add(self, request: StatisticRequest) -> None:
        existing = self._requests.get(request.key)
        if existing is None:
            self._requests[request.key] = request
            return
        if (
            existing.kind != request.kind
            or existing.source_column != request.source_column
            or existing.target_column != request.target_column
        ):
            raise ScanPlanError(
                f"Conflicting statistic requests for key '{request.key}'"
            )
        merged = tuple(dict.fromkeys(existing.requested_by + request.requested_by))
        self._requests[request.key] = StatisticRequest(
            key=existing.key,
            kind=existing.kind,
            source_column=existing.source_column,
            target_column=existing.target_column,
            requested_by=merged,
        )

    def extend(self, requests: Sequence[StatisticRequest]) -> None:
        for request in requests:
            self.add(request)

    def build(self, max_expressions: int = 0) -> StatisticsScanPlan:
        return StatisticsScanPlan(
            list(self._requests.values()), self._key_columns, max_expressions
        )


def resolve_max_expressions(
    connections: Dict[str, Any], execution_config: Any
) -> int:
    """Lowest configured/declared ceiling on expressions in one statement."""
    pushdown = getattr(
        getattr(execution_config, "strategies", None), "pushdown", None
    )
    configured = int(getattr(pushdown, "max_statistics_per_query", 0) or 0)
    ceilings = [configured] if configured else []
    for connection in connections.values():
        if connection is None:
            continue
        declared = int(
            getattr(resolve_capabilities(connection), "max_pushdown_expressions", 0)
            or 0
        )
        if declared:
            ceilings.append(declared)
    return min(ceilings) if ceilings else 0


def statistics_pushdown_available(
    connections: Dict[str, Any], execution_config: Any
) -> bool:
    """Whether both sides can compute many statistics in one scan."""
    pushdown = getattr(
        getattr(execution_config, "strategies", None), "pushdown", None
    )
    if pushdown is not None and not (
        getattr(pushdown, "enabled", True)
        and getattr(pushdown, "combine_statistics", True)
    ):
        return False
    for connection in connections.values():
        capabilities = resolve_capabilities(connection)
        if not capabilities.supports_all(
            "aggregation_pushdown", "multi_statistic_pushdown"
        ):
            return False
    return True
