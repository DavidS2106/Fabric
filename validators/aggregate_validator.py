from __future__ import annotations

from decimal import Decimal, DivisionByZero, InvalidOperation
from typing import Any, Dict, List, Optional, Sequence, Tuple

from core.models import CheckResult, ValidationStatus
from core.execution_engine import ExecutionContext
from core.scan_planner import (
    DISTINCT_KEY_ALIAS,
    StatisticRequest,
    aggregate_key,
)
from .base_validator import BaseValidator
from .validator_utils import _safe_pct_delta, _fqn


#: Aggregates the framework can compute. Which of them actually run comes from
#: ``validations.data_validation.aggregate_validation.aggregations``.
SUPPORTED_AGGREGATIONS = ("sum", "avg", "min", "max", "count")

#: Aggregates that a shared grouped scan can produce directly. ``avg`` is
#: derived from ``sum`` and ``count`` because an average is not decomposable
#: over a partition of the rows.
_DIRECT_SHARED_AGGREGATIONS = ("sum", "min", "max", "count")


class AggregateValidator(BaseValidator):
    """Compare configured aggregates, pushing them down when both sides can.

    When the run's shared statistics scan already produced the configured
    aggregates, this validation issues no query of its own. Otherwise, when the
    configured connectors expose aggregation and multi-statistic pushdown, every
    aggregate for every configured column plus every distinct key count is
    computed in one scan per side instead of one scan per statistic. Connectors
    that do not declare those capabilities keep the per-statistic path.
    """

    #: Retained for compatibility with direct callers. The configured list is
    #: what actually runs.
    AGGREGATIONS = ("sum", "avg", "min", "max")

    @classmethod
    def configured_aggregations(cls, validations_config=None) -> Tuple[str, ...]:
        """Which aggregates to compare, from configuration."""
        section = ((validations_config or {}).get("data_validation") or {}).get(
            "aggregate_validation"
        )
        if not isinstance(section, dict):
            return cls.AGGREGATIONS
        configured = section.get("aggregations")
        if not configured:
            return cls.AGGREGATIONS
        unsupported = sorted(set(configured) - set(SUPPORTED_AGGREGATIONS))
        if unsupported:
            raise ValueError(
                "validations.data_validation.aggregate_validation.aggregations "
                "contains unsupported aggregate(s): " + ", ".join(unsupported)
            )
        return tuple(configured)

    @classmethod
    def statistic_requests(
        cls, table_map, aggregations: Optional[Sequence[str]] = None
    ) -> List[StatisticRequest]:
        """The statistics this check needs from the shared scan."""
        validator = cls()
        mappings = validator._normalize_numeric_mappings(table_map.numeric_columns)
        selected = tuple(aggregations or cls.AGGREGATIONS)
        requests: List[StatisticRequest] = []
        for mapping in mappings:
            kinds = set()
            for aggregation in selected:
                if aggregation == "avg":
                    kinds.update({"sum", "count"})
                else:
                    kinds.add(aggregation)
            for kind in sorted(kinds):
                requests.append(StatisticRequest(
                    key=aggregate_key(kind, mapping["name"]),
                    kind=kind,
                    source_column=mapping["source_column"],
                    target_column=mapping["target_column"],
                    requested_by=("aggregate",),
                ))
        key_mappings = list(getattr(table_map, "key_mappings", None) or [])
        if len(key_mappings) == 1:
            # A single-column key's distinct count is exactly the shared scan's
            # distinct-key count. Composite keys need a per-column count, which
            # a grouped scan cannot derive, so those stay on their own path.
            requests.append(StatisticRequest(
                key=DISTINCT_KEY_ALIAS,
                kind="key_distinct_count",
                requested_by=("aggregate",),
            ))
        return requests

    def validate(
        self,
        source_conn,
        target_conn,
        table_map,
        thresholds,
        context: ExecutionContext,
        source_type: str = "generic",
        execution_config=None,
        execution_cache=None,
    ) -> List[CheckResult]:
        source_dialect = self._dialect(source_conn)
        target_dialect = self._dialect(target_conn)
        src_fqn = _fqn(
            table_map.source_schema, table_map.source_table, source_dialect
        )
        tgt_fqn = _fqn(
            table_map.target_schema, table_map.target_table, target_dialect
        )
        tolerance = thresholds.aggregate_tolerance_pct
        source_where = self._build_where_clause(table_map, source_dialect, "source")
        target_where = self._build_where_clause(table_map, target_dialect, "target")
        incremental_scope = self._incremental_scope(table_map)

        aggregations = self.configured_aggregations(
            getattr(execution_cache, "validations_config", None)
        )
        numeric_mappings = self._normalize_numeric_mappings(table_map.numeric_columns)
        requests = self._statistic_requests(
            numeric_mappings, table_map, aggregations
        )
        if not requests:
            return [CheckResult(
                check_name="aggregate",
                status=ValidationStatus.SKIP,
                message=(
                    "No numeric columns or key mappings configured for aggregate "
                    "validation"
                ),
                details=incremental_scope,
            )]

        shared = self._validate_from_shared_statistics(
            execution_cache, requests, tolerance, incremental_scope
        )
        if shared is not None:
            return shared

        combined = self._can_combine_statistics(
            source_conn, target_conn, execution_config
        )
        if combined:
            return self._validate_combined(
                source_conn,
                target_conn,
                src_fqn,
                tgt_fqn,
                source_where,
                target_where,
                requests,
                tolerance,
                execution_config,
                incremental_scope,
            )
        return self._validate_per_statistic(
            source_conn,
            target_conn,
            src_fqn,
            tgt_fqn,
            table_map,
            requests,
            tolerance,
            incremental_scope,
        )

    # ── Shared statistics scan ────────────────────────────────
    def _validate_from_shared_statistics(
        self,
        execution_cache,
        requests: List[Dict[str, Any]],
        tolerance: float,
        incremental_scope: Dict[str, Any],
    ) -> Optional[List[CheckResult]]:
        """Build every aggregate result from the run's shared statistics scan."""
        provider = self._shared_statistics(execution_cache)
        if provider is None:
            return None
        plans = [self._shared_plan(request) for request in requests]
        if any(plan is None for plan in plans):
            return None
        needed = {key for plan in plans for key in plan["keys"]}
        if not all(provider.provides(key) for key in needed):
            return None
        try:
            values = {
                role: {key: provider.value(role, key) for key in needed}
                for role in ("source", "target")
            }
        except Exception:
            return None

        shared_details = {
            **incremental_scope,
            "execution_strategy": "shared_statistics_scan",
            "shared_scan_used": True,
            "pushdown_scans_per_side": len(provider.statements("source")),
            "statistics_per_scan": len(needed),
            "source_sql": provider.statement("source"),
            "target_sql": provider.statement("target"),
            "tolerance_pct": tolerance,
        }
        results = []
        for request, plan in zip(requests, plans):
            source_value = plan["derive"](values["source"])
            target_value = plan["derive"](values["target"])
            results.append(self._result(
                request, source_value, target_value, tolerance,
                dict(shared_details),
            ))
        return results

    @staticmethod
    def _shared_plan(request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """How one aggregate is obtained from the shared scan's statistics."""
        kind = request["kind"]
        if kind == "distinct_count":
            key = DISTINCT_KEY_ALIAS
            return {"keys": (key,), "derive": lambda values: values[key]}
        name = request.get("mapping_name")
        if not name:
            return None
        if kind in _DIRECT_SHARED_AGGREGATIONS:
            key = aggregate_key(kind, name)
            return {"keys": (key,), "derive": lambda values: values[key]}
        if kind == "avg":
            total_key = aggregate_key("sum", name)
            count_key = aggregate_key("count", name)

            def derive(values, total_key=total_key, count_key=count_key):
                total = values.get(total_key)
                count = values.get(count_key)
                if total is None or not count:
                    return None
                try:
                    return Decimal(str(total)) / Decimal(str(count))
                except (InvalidOperation, DivisionByZero, TypeError, ValueError):
                    return None

            return {"keys": (total_key, count_key), "derive": derive}
        return None

    # ── Request construction ──────────────────────────────────
    def _statistic_requests(
        self,
        numeric_mappings: List[Dict[str, str]],
        table_map,
        aggregations: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Describe every statistic this validation needs, from configuration."""
        requests: List[Dict[str, Any]] = []
        selected = tuple(aggregations or self.AGGREGATIONS)
        for mapping in numeric_mappings:
            for aggregation in selected:
                requests.append({
                    "kind": aggregation,
                    "check_name": f"aggregate_{aggregation}_{mapping['name']}",
                    "source_column": mapping["source_column"],
                    "target_column": mapping["target_column"],
                    "mapping_name": mapping["name"],
                    "numeric": True,
                })
        for mapping in table_map.key_mappings:
            requests.append({
                "kind": "distinct_count",
                "check_name": f"distinct_count_{mapping.source_column}",
                "source_column": mapping.source_column,
                "target_column": mapping.target_column,
                "numeric": False,
            })
        return requests

    @staticmethod
    def _expression(
        request: Dict[str, Any], column: str, dialect: Any = None
    ) -> str:
        column = dialect.quote(column) if dialect is not None else column
        if request["kind"] == "distinct_count":
            return (
                dialect.count_distinct(column) if dialect is not None
                else f"COUNT(DISTINCT {column})"
            )
        if request["kind"] == "count":
            return (
                dialect.count_column(column) if dialect is not None
                else f"COUNT({column})"
            )
        return f"{request['kind'].upper()}({column})"

    # ── Combined single-scan pushdown ─────────────────────────
    def _validate_combined(
        self,
        source_conn,
        target_conn,
        src_fqn: str,
        tgt_fqn: str,
        source_where: str,
        target_where: str,
        requests: List[Dict[str, Any]],
        tolerance: float,
        execution_config,
        incremental_scope: Dict[str, Any],
    ) -> List[CheckResult]:
        source_chunks = self._chunk(
            requests,
            self._max_statistics_per_query(
                self._capabilities(source_conn), len(requests), execution_config
            ),
        )
        target_chunks = self._chunk(
            requests,
            self._max_statistics_per_query(
                self._capabilities(target_conn), len(requests), execution_config
            ),
        )
        source_queries = [
            self._combined_sql(
                src_fqn, source_where, chunk, "source_column",
                self._dialect(source_conn),
            )
            for chunk in source_chunks
        ]
        target_queries = [
            self._combined_sql(
                tgt_fqn, target_where, chunk, "target_column",
                self._dialect(target_conn),
            )
            for chunk in target_chunks
        ]

        def read(connection, queries, chunks):
            values: Dict[str, Any] = {}
            for sql, chunk in zip(queries, chunks):
                frame = connection.execute_query(sql)
                for index, request in enumerate(chunk):
                    values[request["check_name"]] = self._scalar_at(
                        frame, index, self._alias("stat", index)
                    )
            return values

        try:
            source_values, target_values = self._run_both_sides(
                execution_config,
                lambda: read(source_conn, source_queries, source_chunks),
                lambda: read(target_conn, target_queries, target_chunks),
            )
        except Exception as exc:
            return [
                CheckResult(
                    check_name=request["check_name"],
                    status=ValidationStatus.ERROR,
                    message=(
                        f"{request['kind'].upper()} failed for "
                        f"{request['source_column']} -> {request['target_column']}: {exc}"
                    ),
                    details={
                        **incremental_scope,
                        "execution_strategy": "pushdown_combined",
                    },
                )
                for request in requests
            ]

        shared_details = {
            **incremental_scope,
            "execution_strategy": "pushdown_combined",
            "pushdown_scans_per_side": len(source_queries),
            "statistics_per_scan": len(requests) if len(source_queries) == 1 else None,
            "source_sql": source_queries[0] if len(source_queries) == 1 else source_queries,
            "target_sql": target_queries[0] if len(target_queries) == 1 else target_queries,
            "tolerance_pct": tolerance,
        }
        return [
            self._result(
                request,
                source_values.get(request["check_name"]),
                target_values.get(request["check_name"]),
                tolerance,
                dict(shared_details),
            )
            for request in requests
        ]

    def _combined_sql(
        self,
        fqn: str,
        where_clause: str,
        requests: Sequence[Dict[str, Any]],
        column_key: str,
        dialect: Any = None,
    ) -> str:
        expressions = ", ".join(
            f"{self._expression(request, request[column_key], dialect)} AS "
            f"{self._alias('stat', index)}"
            for index, request in enumerate(requests)
        )
        if dialect is None:
            return f"SELECT {expressions} FROM {fqn}{where_clause}"
        return dialect.select(expressions, fqn, where_clause)

    @staticmethod
    def _chunk(requests: List[Dict[str, Any]], size: int) -> List[List[Dict[str, Any]]]:
        if size <= 0 or size >= len(requests):
            return [list(requests)]
        return [
            list(requests[index:index + size])
            for index in range(0, len(requests), size)
        ]

    # ── Per-statistic fallback ────────────────────────────────
    def _validate_per_statistic(
        self,
        source_conn,
        target_conn,
        src_fqn: str,
        tgt_fqn: str,
        table_map,
        requests: List[Dict[str, Any]],
        tolerance: float,
        incremental_scope: Dict[str, Any],
    ) -> List[CheckResult]:
        results = []
        for request in requests:
            try:
                source_value, source_sql = self._single_statistic(
                    source_conn, src_fqn, request, "source_column", table_map
                )
                target_value, target_sql = self._single_statistic(
                    target_conn, tgt_fqn, request, "target_column", table_map
                )
            except Exception as exc:
                results.append(CheckResult(
                    check_name=request["check_name"],
                    status=ValidationStatus.ERROR,
                    message=(
                        f"{request['kind'].upper()} failed for "
                        f"{request['source_column']} -> {request['target_column']}: {exc}"
                    ),
                    details={
                        **incremental_scope,
                        "execution_strategy": "pushdown_per_statistic",
                    },
                ))
                continue
            results.append(self._result(
                request,
                source_value,
                target_value,
                tolerance,
                {
                    **incremental_scope,
                    "execution_strategy": "pushdown_per_statistic",
                    "source_sql": source_sql,
                    "target_sql": target_sql,
                    "tolerance_pct": tolerance,
                },
            ))
        return results

    def _single_statistic(
        self, connection, fqn: str, request: Dict[str, Any], column_key: str, table_map
    ) -> Tuple[Any, str]:
        dialect = self._dialect(connection)
        role = "source" if column_key == "source_column" else "target"
        where = self._build_where_clause(table_map, dialect, role)
        expression = self._expression(request, request[column_key], dialect)
        alias = "d" if request["kind"] == "distinct_count" else None
        projection = f"{expression} AS {alias}" if alias else expression
        sql = dialect.select(projection, fqn, where)
        return self._scalar(connection.execute_query(sql)), sql

    # ── Result construction ───────────────────────────────────
    def _result(
        self,
        request: Dict[str, Any],
        source_value: Any,
        target_value: Any,
        tolerance: float,
        details: Dict[str, Any],
    ) -> CheckResult:
        details.update({
            "source_column": request["source_column"],
            "target_column": request["target_column"],
        })
        if request["kind"] == "distinct_count":
            return self._distinct_result(request, source_value, target_value, details)
        return self._numeric_result(
            request, source_value, target_value, tolerance, details
        )

    def _numeric_result(
        self,
        request: Dict[str, Any],
        source_value: Any,
        target_value: Any,
        tolerance: float,
        details: Dict[str, Any],
    ) -> CheckResult:
        try:
            source_number = float(source_value) if source_value is not None else 0.0
            target_number = float(target_value) if target_value is not None else 0.0
        except (TypeError, ValueError) as exc:
            return CheckResult(
                check_name=request["check_name"],
                status=ValidationStatus.ERROR,
                message=(
                    f"{request['kind'].upper()} returned a non-numeric value for "
                    f"{request['source_column']} -> {request['target_column']}: {exc}"
                ),
                details=details,
            )
        delta = abs(source_number - target_number)
        delta_pct = _safe_pct_delta(source_number, target_number)
        status = (
            ValidationStatus.PASS if delta_pct <= tolerance else ValidationStatus.FAIL
        )
        label = request["kind"].upper()
        return CheckResult(
            check_name=request["check_name"],
            status=status,
            source_value=round(source_number, 4),
            target_value=round(target_number, 4),
            delta=round(delta, 4),
            delta_pct=round(delta_pct, 6),
            message=(
                f"{label} matched for "
                f"{request['source_column']} -> {request['target_column']}"
                if status == ValidationStatus.PASS
                else (
                    f"{label} mismatch for "
                    f"{request['source_column']} -> {request['target_column']}: "
                    f"source={source_number}, target={target_number}, "
                    f"delta_pct={delta_pct:.6f}%, tolerance={tolerance:.6f}%"
                )
            ),
            details=details,
        )

    def _distinct_result(
        self,
        request: Dict[str, Any],
        source_value: Any,
        target_value: Any,
        details: Dict[str, Any],
    ) -> CheckResult:
        try:
            source_count = int(source_value or 0)
            target_count = int(target_value or 0)
        except (TypeError, ValueError) as exc:
            return CheckResult(
                check_name=request["check_name"],
                status=ValidationStatus.ERROR,
                message=(
                    "Distinct count returned a non-numeric value for "
                    f"{request['source_column']} -> {request['target_column']}: {exc}"
                ),
                details=details,
            )
        delta = abs(source_count - target_count)
        status = (
            ValidationStatus.PASS
            if source_count == target_count
            else ValidationStatus.FAIL
        )
        return CheckResult(
            check_name=request["check_name"],
            status=status,
            source_value=source_count,
            target_value=target_count,
            delta=delta,
            message=(
                "DISTINCT_COUNT matched for "
                f"{request['source_column']} -> {request['target_column']}"
                if status == ValidationStatus.PASS
                else (
                    "Distinct count mismatch for "
                    f"{request['source_column']} -> {request['target_column']}: "
                    f"source={source_count}, target={target_count}, delta={delta}"
                )
            ),
            details=details,
        )

    # ── Configuration normalization ───────────────────────────
    def _normalize_numeric_mappings(
        self, numeric_columns: List[Any]
    ) -> List[Dict[str, str]]:
        mappings = []
        for item in numeric_columns or []:
            if isinstance(item, str):
                mappings.append({
                    "source_column": item,
                    "target_column": item,
                    "name": item,
                })
            elif isinstance(item, dict):
                source_col = item.get("source_column")
                target_col = item.get("target_column", source_col)
                if not source_col or not target_col:
                    continue
                mappings.append({
                    "source_column": source_col,
                    "target_column": target_col,
                    "name": item.get("name", f"{source_col}_to_{target_col}"),
                })
        return mappings

    # ── Backward-compatible helpers ───────────────────────────
    def _get_agg(
        self,
        conn,
        fqn: str,
        col: str,
        agg: str,
        table_map,
    ) -> Tuple[Optional[float], str]:
        value, sql = self._single_statistic(
            conn,
            fqn,
            {"kind": agg, "source_column": col, "target_column": col},
            "source_column",
            table_map,
        )
        return (float(value) if value is not None else 0.0), sql

    def _get_distinct(self, conn, fqn: str, col: str, table_map) -> Tuple[int, str]:
        value, sql = self._single_statistic(
            conn,
            fqn,
            {"kind": "distinct_count", "source_column": col, "target_column": col},
            "source_column",
            table_map,
        )
        return int(value or 0), sql
