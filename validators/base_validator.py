"""
fabric_migration_validator/validators/base_validator.py
──────────────────────────────────────────────────────
Shared, platform-independent helpers for every validator.

Validators ask connectors what they support and read every column, key,
watermark and threshold from configuration. Nothing here names a platform.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

from core.capabilities import ConnectorCapabilities, resolve_capabilities
from core.sql_dialect import BoundSql, SqlDialect, resolve_dialect
from core.validation_scope import ValidationScope


class BaseValidator:
    # ── Capability access ─────────────────────────────────────
    @staticmethod
    def _capabilities(connection: Any) -> ConnectorCapabilities:
        """Return one connector's effective capabilities."""
        return resolve_capabilities(connection)

    # ── Dialect access ────────────────────────────────────────
    @staticmethod
    def _dialect(connection: Any) -> SqlDialect:
        """Return the SQL syntax configured for one connection."""
        return resolve_dialect(connection)

    # ── Shared execution services ─────────────────────────────
    @staticmethod
    def _shared_statistics(execution_cache: Any):
        """Return the run's shared statistics provider, when one is attached.

        The provider travels on the run-local execution cache, so a validator
        called directly (without an orchestrator) keeps its own self-sufficient
        query path and behaves exactly as before.
        """
        return getattr(execution_cache, "shared_statistics", None)

    def _log_fallback(
        self,
        validator_name: str,
        table_id: str,
        role: str,
        statistic: str,
        reason: str,
    ) -> None:
        """Log when a validator must fall back from shared statistics to individual query.

        This log entry helps diagnose performance regressions caused by cache misses.
        """
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(
            "[FALLBACK] validator=%s table=%s role=%s statistic=%s reason=%s",
            validator_name,
            table_id,
            role,
            statistic,
            reason,
        )

    @staticmethod
    def _scan_ledger(execution_cache: Any):
        """Return the run's scan ledger, when instrumentation is configured."""
        return getattr(execution_cache, "scan_ledger", None)

    @staticmethod
    def _configured_policies(execution_cache: Any):
        """Return the run's resolved comparison policies, when available.

        Validators whose signature predates the policy object read it from the
        run-local cache, so normalization, NULL and timestamp rules stay
        configuration-driven without changing every call site.
        """
        return getattr(execution_cache, "validation_policies", None)

    def _configured_normalization(self, execution_cache: Any) -> Dict[str, Any]:
        """The configured value-normalization rules, or an empty mapping."""
        policies = self._configured_policies(execution_cache)
        if policies is None:
            return {}
        try:
            return dict(policies.hashing["normalization"])
        except (AttributeError, KeyError, TypeError):
            return {}

    def _both_support(self, source_conn: Any, target_conn: Any, *capabilities: str) -> bool:
        """Return whether Source and Target both support every capability."""
        source = self._capabilities(source_conn)
        target = self._capabilities(target_conn)
        return source.supports_all(*capabilities) and target.supports_all(*capabilities)

    def _can_combine_statistics(
        self, source_conn: Any, target_conn: Any, execution_config: Any = None
    ) -> bool:
        """Return whether many statistics may share one scan on both sides."""
        if not self._both_support(
            source_conn, target_conn, "aggregation_pushdown", "multi_statistic_pushdown"
        ):
            return False
        pushdown = getattr(
            getattr(execution_config, "strategies", None), "pushdown", None
        )
        if pushdown is None:
            return True
        return bool(
            getattr(pushdown, "enabled", True)
            and getattr(pushdown, "combine_statistics", True)
        )

    @staticmethod
    def _max_statistics_per_query(
        capabilities: ConnectorCapabilities,
        requested: int,
        execution_config: Any = None,
    ) -> int:
        """Clamp combined statistics to the configured and connector ceilings."""
        pushdown = getattr(
            getattr(execution_config, "strategies", None), "pushdown", None
        )
        configured = int(getattr(pushdown, "max_statistics_per_query", 0) or 0)
        allowed = capabilities.statistic_batch_size(requested)
        if configured:
            allowed = min(allowed, configured)
        return max(allowed, 1) if requested else 0

    # ── Run-local reuse ───────────────────────────────────────
    @staticmethod
    def _cached(execution_cache: Any, key: Any, compute) -> Any:
        """Compute once per run key, so validations never repeat a scan."""
        if execution_cache is None:
            return compute()
        return execution_cache.get_or_compute(key, compute)

    @staticmethod
    def _row_count_once(
        execution_cache: Any, role: str, compute, consumer: Optional[str] = None
    ) -> int:
        """The side's exact row count, physically executed at most once per table.

        Every consumer of a row count (row count, edge case, audit, null
        validation, record comparison strategy selection) goes through the
        same single-flight key. Success is cached; so is failure: when the
        first ``COUNT_BIG(*)`` timed out or failed, later consumers receive the
        cached failure immediately instead of issuing the identical statement
        for another timeout, and follow their own configured error handling.
        """
        if execution_cache is None:
            return int(compute())
        get_or_compute = getattr(execution_cache, "get_or_compute", None)
        if not callable(get_or_compute):
            return int(compute())
        try:
            return int(get_or_compute(
                ("row_count", role), compute, cache_failures=True, consumer=consumer,
            ))
        except TypeError as exc:
            # A minimal cache without failure support (test doubles).
            if "cache_failures" not in str(exc) and "consumer" not in str(exc):
                raise
            return int(get_or_compute(("row_count", role), compute))

    def _get_cached_statistic(
        self, execution_cache: Any, role: str, statistic_key: str,
        default: Any = None
    ) -> Any:
        """Retrieve a cached statistic without triggering computation.

        Checks the execution cache first, then the shared statistics provider.
        Returns the default if neither has the value.
        """
        if execution_cache is None:
            return default

        from core.scan_planner import statistic_cache_key
        scope_token = getattr(execution_cache, "_scope_token", "")
        cache_key = statistic_cache_key(role, scope_token, statistic_key)
        value = execution_cache.peek(cache_key, default)
        if value is not None:
            return value

        provider = self._shared_statistics(execution_cache)
        if provider is not None:
            try:
                return provider.value(role, statistic_key, default)
            except Exception:
                return default

        return default

    def _run_both_sides(self, execution_config: Any, source_call, target_call):
        """Run independent Source/Target reads with deterministic return order."""
        parallel = getattr(execution_config, "parallel_execution", None)
        configured = getattr(parallel, "source_target_parallelism", None)
        if (
            getattr(parallel, "enabled", False)
            and getattr(configured, "enabled", False)
            and getattr(configured, "max_workers_per_validation", 1) > 1
        ):
            with ThreadPoolExecutor(
                max_workers=2, thread_name_prefix="validation-side"
            ) as executor:
                source_future = executor.submit(source_call)
                target_future = executor.submit(target_call)
                return source_future.result(), target_future.result()
        return source_call(), target_call()

    # ── Filtering ─────────────────────────────────────────────
    @staticmethod
    def _resolved_scope(table_map) -> Optional[ValidationScope]:
        """The window the orchestrator resolved for this table and phase."""
        return ValidationScope.from_dict(
            getattr(table_map, "validation_scope", None)
        )

    @classmethod
    def _source_scope_unavailable(cls, table_map) -> bool:
        scope = cls._resolved_scope(table_map)
        return bool(
            scope is not None
            and scope.mode == "incremental"
            and not scope.source_scope_available
        )

    def _scope_token(self, table_map) -> str:
        """Stable identity of the window this table is read through."""
        scope = self._resolved_scope(table_map)
        if scope is not None:
            return scope.token()
        predicates = self._filter_predicates(table_map)
        return "|".join(predicates) if predicates else "unfiltered"

    def _build_where_clause(
        self, table_map, dialect: Optional[SqlDialect] = None,
        role: Optional[str] = None,
    ) -> str:
        """Build the configured filter applied identically to every check.

        When the orchestrator has resolved a window for the phase being
        validated, that window is authoritative: every statistics scan, key
        read, value read, audit query and record read renders the same filter.

        Otherwise two independent, fully configuration-driven filters are
        derived from the table itself:

        * a date range on a configured filter column, and
        * an incremental watermark window on a configured incremental column,
          evaluated as ``previous_watermark < column <= current_watermark``.

        Bound inclusivity comes from configuration. When only a single legacy
        ``incremental_value`` is configured the original open lower bound is kept.
        """
        scope = self._resolved_scope(table_map)
        if scope is not None:
            filters = list(scope.predicates_for(role))
            if dialect is not None:
                filters = self._quote_filter_identifiers(
                    filters, table_map, dialect, role
                )
            if not filters:
                return ""
            statement = " WHERE " + " AND ".join(filters)
            parameters = scope.parameters_for(role)
            return BoundSql(statement, parameters) if parameters else statement
        filters = self._filter_predicates(table_map, dialect)
        if not filters:
            return ""
        return " WHERE " + " AND ".join(filters)

    def _filter_predicates(
        self, table_map, dialect: Optional[SqlDialect] = None,
        role: Optional[str] = None,
    ) -> List[str]:
        scope = self._resolved_scope(table_map)
        if scope is not None:
            filters = list(scope.predicates_for(role))
            return (
                self._quote_filter_identifiers(filters, table_map, dialect, role)
                if dialect is not None else filters
            )
        filters: List[str] = []

        date_column = getattr(table_map, "date_filter_column", None)
        date_range = getattr(table_map, "date_range", None) or {}
        if date_column and isinstance(date_range, dict):
            start = date_range.get("start")
            end = date_range.get("end")
            if start is not None and end is not None:
                filters.append(
                    f"{dialect.quote(date_column) if dialect else date_column} BETWEEN "
                    f"{self._filter_literal(start)} AND {self._filter_literal(end)}"
                )

        filters.extend(self._incremental_predicates(table_map, dialect))
        return filters

    def _incremental_predicates(
        self, table_map, dialect: Optional[SqlDialect] = None
    ) -> List[str]:
        """Return the configured incremental watermark predicates."""
        column = getattr(table_map, "incremental_column", None)
        if not column:
            return []

        previous = getattr(table_map, "incremental_previous_value", None)
        current = getattr(table_map, "incremental_current_value", None)
        legacy = getattr(table_map, "incremental_value", None)

        if previous is None and current is None:
            if legacy is None:
                return []
            # Backward-compatible single-bound incremental filter.
            rendered = dialect.quote(column) if dialect else column
            return [f"{rendered} > {self._filter_literal(legacy)}"]

        lower_inclusive = bool(
            getattr(table_map, "incremental_lower_bound_inclusive", False)
        )
        upper_inclusive = bool(
            getattr(table_map, "incremental_upper_bound_inclusive", True)
        )
        predicates = []
        rendered = dialect.quote(column) if dialect else column
        lower = previous if previous is not None else legacy
        if lower is not None:
            operator = ">=" if lower_inclusive else ">"
            predicates.append(f"{rendered} {operator} {self._filter_literal(lower)}")
        if current is not None:
            operator = "<=" if upper_inclusive else "<"
            predicates.append(f"{rendered} {operator} {self._filter_literal(current)}")
        return predicates

    def _historical_target_where(self, table_map, dialect: Optional[SqlDialect] = None):
        """The target rows loaded before the current incremental window.

        Rendered only for the target role from the predicates the incremental
        strategy resolver derived (the exact complement of the window's lower
        bound), with the same bound-parameter mechanism as every other filter.
        Empty when the scope has no history (a bootstrap run) or is not
        incremental.
        """
        scope = self._resolved_scope(table_map)
        if scope is None:
            return ""
        filters = list(scope.historical_target_predicates())
        if not filters:
            return ""
        if dialect is not None:
            filters = self._quote_filter_identifiers(filters, table_map, dialect, "target")
        statement = " WHERE " + " AND ".join(filters)
        parameters = scope.historical_target_parameters()
        return BoundSql(statement, parameters) if parameters else statement

    @staticmethod
    def _quote_filter_identifiers(
        predicates: List[str], table_map, dialect: SqlDialect,
        role: Optional[str] = None,
    ) -> List[str]:
        """Quote framework-owned identifiers in resolved filter predicates.

        Arbitrary configured SQL remains untouched. Only columns separately
        declared as date, watermark, key-checkpoint or target-audit identifiers
        are replaced, so literals and SQL parameters are never changed by this
        helper.
        """
        import re

        rendered = list(predicates)
        audit = getattr(table_map, "incremental_target_audit", None)
        audit_column = (
            audit.get("load_timestamp_column") if isinstance(audit, dict) and role == "target"
            else None
        )
        if role == "target":
            scope = BaseValidator._resolved_scope(table_map)
            if scope is not None and scope.mode == "incremental":
                audit_column = scope.details.get("target_audit_column") or audit_column
        columns = {
            str(value)
            for value in (
                getattr(table_map, "date_filter_column", None),
                getattr(table_map, "incremental_column", None),
                (
                    getattr(table_map, "source_watermark_column", None)
                    if role == "source" else
                    getattr(table_map, "target_watermark_column", None)
                    if role == "target" else None
                ),
                audit_column,
            )
            if value
        }
        for column in sorted(columns, key=len, reverse=True):
            quoted = dialect.quote(column)
            if quoted == column:
                continue
            pattern = re.compile(
                rf"(?<![\w{re.escape(dialect.identifier_quote_close)}])"
                rf"{re.escape(column)}"
                rf"(?![\w{re.escape(dialect.identifier_quote_open)}])"
            )
            rendered = [pattern.sub(quoted, item) for item in rendered]
        return rendered

    def _incremental_scope(self, table_map) -> Dict[str, Any]:
        """Report-friendly description of the scope actually applied."""
        scope = self._resolved_scope(table_map)
        if scope is not None:
            return scope.as_details()
        column = getattr(table_map, "incremental_column", None)
        if not column:
            return {"incremental_scope_applied": False}
        return {
            "incremental_scope_applied": bool(self._incremental_predicates(table_map)),
            "incremental_column": column,
            "incremental_previous_value": getattr(
                table_map, "incremental_previous_value", None
            ),
            "incremental_current_value": getattr(
                table_map, "incremental_current_value", None
            ),
            "incremental_value": getattr(table_map, "incremental_value", None),
        }

    @staticmethod
    def _filter_literal(value: Any) -> str:
        """Quote a configured filter value without assuming a value type."""
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)):
            return str(value)
        return "'" + str(value).replace("'", "''") + "'"

    # ── Identifier helpers ────────────────────────────────────
    @staticmethod
    def _alias(prefix: str, index: int) -> str:
        """Return a collision-free result alias for a combined statistic query."""
        return f"__mv_{prefix}_{index}"

    @staticmethod
    def _scalar(frame: Any, column: Optional[str] = None, default: Any = None) -> Any:
        """Read one scalar from a single-row result frame."""
        if frame is None or getattr(frame, "empty", True):
            return default
        if column is not None and column in getattr(frame, "columns", []):
            value = frame.iloc[0][column]
        else:
            value = frame.iloc[0, 0]
        return default if value is None else value

    @classmethod
    def _scalar_at(
        cls,
        frame: Any,
        index: int,
        alias: Optional[str] = None,
        default: Any = None,
    ) -> Any:
        """Read one scalar of a combined-statistic row by alias, else by position.

        Connectors are free to fold or change result-column case, so the alias is
        preferred and the ordinal position is the portable fallback.
        """
        if frame is None or getattr(frame, "empty", True):
            return default
        columns = list(getattr(frame, "columns", []))
        if alias:
            by_casefold = {str(column).casefold(): column for column in columns}
            match = by_casefold.get(alias.casefold())
            if match is not None:
                value = frame.iloc[0][match]
                return default if value is None else value
        if index < len(columns):
            value = frame.iloc[0, index]
            return default if value is None else value
        return default
