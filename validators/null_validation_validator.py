from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from core.execution_engine import ExecutionContext
from core.models import CheckResult, ValidationStatus
from core.scan_planner import (
    ROW_COUNT_KEY,
    StatisticRequest,
    null_count_key,
)
from .base_validator import BaseValidator
from .validator_utils import _fqn

logger = logging.getLogger(__name__)


def null_statistic_key(source_column: str, target_column: str) -> str:
    """Canonical shared-scan key for one mapped column's NULL count."""
    return null_count_key(f"{source_column}->{target_column}")


class NullValidationValidator(BaseValidator):
    """Compare configured null counts using as few scans as the connectors allow.

    When the run's shared statistics scan already computed the configured NULL
    counts and the record count, this validation issues no query at all. Where
    it runs on its own and both connectors support multi-statistic pushdown,
    every configured column's null count and the total record count are
    produced by one scan per side. Otherwise the per-column path is unchanged.
    """

    @classmethod
    def statistic_requests(cls, table_map) -> List[StatisticRequest]:
        """The statistics this check needs from the shared scan."""
        validator = cls()
        mappings = validator._resolve_null_mappings(table_map)
        mandatory = validator._normalize_mappings(
            getattr(table_map, "mandatory_columns", []) or []
        )
        if not mappings and not mandatory:
            return []
        requests = [
            StatisticRequest(
                key=ROW_COUNT_KEY,
                kind="row_count",
                requested_by=("null_validation",),
            )
        ]
        for mapping in list(mappings) + list(mandatory):
            requests.append(StatisticRequest(
                key=null_statistic_key(
                    mapping["source_column"], mapping["target_column"]
                ),
                kind="null_count",
                source_column=mapping["source_column"],
                target_column=mapping["target_column"],
                requested_by=("null_validation",),
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
        mappings = self._resolve_null_mappings(table_map)
        mandatory_mappings = self._normalize_mappings(
            getattr(table_map, "mandatory_columns", []) or []
        )

        if not mappings and not mandatory_mappings:
            return [
                CheckResult(
                    check_name="null_validation",
                    status=ValidationStatus.SKIP,
                    message=(
                        "Skipped null validation: configure null_columns, mandatory_columns, "
                        "compare_column_mappings, or numeric_columns"
                    ),
                )
            ]

        source_dialect = self._dialect(source_conn)
        target_dialect = self._dialect(target_conn)
        src_fqn = _fqn(
            table_map.source_schema, table_map.source_table, source_dialect
        )
        tgt_fqn = _fqn(
            table_map.target_schema, table_map.target_table, target_dialect
        )

        shared = self._validate_from_shared_statistics(
            execution_cache, table_map, mappings, mandatory_mappings
        )
        if shared is not None:
            return shared

        if self._can_combine_statistics(source_conn, target_conn, execution_config):
            return self._validate_combined(
                source_conn,
                target_conn,
                src_fqn,
                tgt_fqn,
                table_map,
                mappings,
                mandatory_mappings,
                execution_config,
                execution_cache,
            )

        results = []
        if mappings:
            results.extend(
                self._compare_null_counts(
                    source_conn, target_conn, src_fqn, tgt_fqn, mappings, table_map,
                    execution_cache=execution_cache,
                )
            )
            results.extend(
                self._detect_unexpected_nulls(
                    source_conn, target_conn, src_fqn, tgt_fqn, mappings, table_map
                )
            )

        if mandatory_mappings:
            results.extend(
                self._check_mandatory_columns(
                    source_conn, target_conn, src_fqn, tgt_fqn, mandatory_mappings, table_map
                )
            )

        return results

    # ── Shared statistics scan ────────────────────────────────
    def _validate_from_shared_statistics(
        self,
        execution_cache,
        table_map,
        mappings: List[Dict[str, str]],
        mandatory_mappings: List[Dict[str, str]],
    ) -> Optional[List[CheckResult]]:
        """Build every result from the run's shared statistics scan.

        Returns ``None`` when no shared scan is available or it did not produce
        the required statistics, triggering fallback to combined or per-column queries.
        every statistic this validation needs, so the validator falls back to
        its own queries and its results never depend on the optimization.
        """
        provider = self._shared_statistics(execution_cache)
        if provider is None:
            self._log_fallback(
                validator_name="null_validation",
                table_id=getattr(table_map, "id", "unknown"),
                role="both",
                statistic="all_null_counts",
                reason="no_shared_statistics_provider",
            )
            return None
        needed = [ROW_COUNT_KEY] + [
            null_statistic_key(item["source_column"], item["target_column"])
            for item in list(mappings) + list(mandatory_mappings)
        ]
        missing_statistics = [key for key in needed if not provider.provides(key)]
        if missing_statistics:
            self._log_fallback(
                validator_name="null_validation",
                table_id=getattr(table_map, "id", "unknown"),
                role="both",
                statistic=",".join(missing_statistics[:3]) + ("..." if len(missing_statistics) > 3 else ""),
                reason=f"shared_statistics_missing_{len(missing_statistics)}_of_{len(needed)}_needed",
            )
            return None
        try:
            source_total = provider.integer("source", ROW_COUNT_KEY)
            target_total = provider.integer("target", ROW_COUNT_KEY)
            source_counts = {
                key: provider.integer("source", key) for key in needed
            }
            target_counts = {
                key: provider.integer("target", key) for key in needed
            }
        except Exception as exc:
            self._log_fallback(
                validator_name="null_validation",
                table_id=getattr(table_map, "id", "unknown"),
                role="both",
                statistic="null_counts",
                reason=f"provider_error_{type(exc).__name__}",
            )
            return None

        shared_details = {
            **self._incremental_scope(table_map),
            "execution_strategy": "shared_statistics_scan",
            "shared_scan_used": True,
            "pushdown_scans_per_side": len(provider.statements("source")),
            "statistics_per_scan": len(needed),
            "source_sql": provider.statement("source"),
            "target_sql": provider.statement("target"),
        }
        results: List[CheckResult] = []
        for mapping in mappings:
            key = null_statistic_key(
                mapping["source_column"], mapping["target_column"]
            )
            results.append(self._null_count_result(
                mapping,
                source_counts[key],
                target_counts[key],
                source_total,
                target_total,
                dict(shared_details),
            ))
        if mappings:
            results.extend(self._unexpected_null_placeholder(table_map))
        for mapping in mandatory_mappings:
            key = null_statistic_key(
                mapping["source_column"], mapping["target_column"]
            )
            for side, counts, column in (
                ("source", source_counts, mapping["source_column"]),
                ("target", target_counts, mapping["target_column"]),
            ):
                results.append(self._mandatory_result(
                    mapping,
                    side,
                    column,
                    counts[key],
                    {
                        **shared_details,
                        "sql": shared_details[f"{side}_sql"],
                        "column": column,
                    },
                ))
        return results

    # ── Combined single-scan pushdown ─────────────────────────
    def _validate_combined(
        self,
        source_conn,
        target_conn,
        src_fqn: str,
        tgt_fqn: str,
        table_map,
        mappings: List[Dict[str, str]],
        mandatory_mappings: List[Dict[str, str]],
        execution_config,
        execution_cache,
    ) -> List[CheckResult]:
        incremental_scope = self._incremental_scope(table_map)
        # One request list per side keeps aliases aligned across both queries.
        source_columns = [item["source_column"] for item in mappings] + [
            item["source_column"] for item in mandatory_mappings
        ]
        target_columns = [item["target_column"] for item in mappings] + [
            item["target_column"] for item in mandatory_mappings
        ]

        def read(connection, fqn, columns, role):
            where_clause = self._build_where_clause(
                table_map, self._dialect(connection), role
            )
            counts, sql = self._combined_null_counts(
                connection,
                fqn,
                where_clause,
                columns,
                execution_config,
                include_row_count=True,
            )
            row_count = counts.pop("__row_count__")
            if execution_cache is not None and row_count is not None:
                execution_cache.put(("row_count", role), int(row_count))
            return counts, row_count, sql

        try:
            (
                (source_counts, source_total, source_sql),
                (target_counts, target_total, target_sql),
            ) = self._run_both_sides(
                execution_config,
                lambda: read(source_conn, src_fqn, source_columns, "source"),
                lambda: read(target_conn, tgt_fqn, target_columns, "target"),
            )
        except Exception as exc:
            failed = [f"null_count_{item['name']}" for item in mappings] + [
                f"mandatory_null_check_{side}_{item['name']}"
                for item in mandatory_mappings
                for side in ("source", "target")
            ]
            return [
                CheckResult(
                    check_name=name,
                    status=ValidationStatus.ERROR,
                    message=f"Null validation pushdown failed: {exc}",
                    details={
                        **incremental_scope,
                        "execution_strategy": "pushdown_combined",
                    },
                )
                for name in failed
            ]

        shared = {
            **incremental_scope,
            "execution_strategy": "pushdown_combined",
            "pushdown_scans_per_side": 1,
            "statistics_per_scan": len(source_columns) + 1,
        }
        results: List[CheckResult] = []
        for mapping in mappings:
            results.append(self._null_count_result(
                mapping,
                int(source_counts.get(mapping["source_column"], 0) or 0),
                int(target_counts.get(mapping["target_column"], 0) or 0),
                int(source_total or 0),
                int(target_total or 0),
                {**shared, "source_sql": source_sql, "target_sql": target_sql},
            ))
        if mappings:
            results.extend(
                self._detect_unexpected_nulls(
                    source_conn, target_conn, src_fqn, tgt_fqn, mappings, table_map
                )
            )
        for mapping in mandatory_mappings:
            for side, counts, column in (
                ("source", source_counts, mapping["source_column"]),
                ("target", target_counts, mapping["target_column"]),
            ):
                results.append(self._mandatory_result(
                    mapping,
                    side,
                    column,
                    int(counts.get(column, 0) or 0),
                    {
                        **shared,
                        "sql": source_sql if side == "source" else target_sql,
                        "column": column,
                    },
                ))
        return results

    def _combined_null_counts(
        self,
        connection,
        fqn: str,
        where_clause: str,
        columns: Sequence[str],
        execution_config,
        include_row_count: bool,
    ) -> Tuple[Dict[str, Any], str]:
        """Return every requested null count, and the row count, in one scan."""
        unique_columns = list(dict.fromkeys(columns))
        capabilities = self._capabilities(connection)
        dialect = self._dialect(connection)
        requested = len(unique_columns) + (1 if include_row_count else 0)

        # Get the maximum expressions supported by the connector
        allowed = self._max_statistics_per_query(
            capabilities, requested, execution_config
        )

        # Log the final query plan before executing
        chunk_count = 1 if requested <= allowed else (
            (requested - 1) // (allowed - 1) if include_row_count else
            (requested + allowed - 1) // allowed
        )
        logger.info(
            "[QUERY_PLAN] null_validation final plan: %d columns in %d queries "
            "(connector_max_expressions=%d, requested=%d)",
            len(unique_columns),
            chunk_count,
            allowed,
            requested,
        )

        counts: Dict[str, Any] = {}
        statements: List[str] = []
        # Reserve one expression for the shared record count in the first scan.
        chunks = self._chunk_columns(
            unique_columns, allowed - 1 if include_row_count and allowed > 1 else allowed
        )

        for position, chunk in enumerate(chunks):
            expressions = [
                f"SUM(CASE WHEN {dialect.quote(column)} IS NULL THEN 1 ELSE 0 END) AS "
                f"{self._alias('null', index)}"
                for index, column in enumerate(chunk)
            ]
            include_here = include_row_count and position == 0
            if include_here:
                expressions.append(
                    f"{dialect.count_all()} AS {self._alias('null', len(chunk))}"
                )
            sql = dialect.select(", ".join(expressions), fqn, where_clause)
            statements.append(sql)

            started = time.perf_counter()
            frame = connection.execute_query(sql)
            elapsed = round(time.perf_counter() - started, 6)

            logger.debug(
                "[QUERY_TIMING] null_validation chunk %d/%d duration=%.6fs expressions=%d",
                position + 1,
                len(chunks),
                elapsed,
                len(expressions),
            )

            for index, column in enumerate(chunk):
                counts[column] = self._scalar_at(
                    frame, index, self._alias("null", index), default=0
                )
            if include_here:
                counts["__row_count__"] = self._scalar_at(
                    frame, len(chunk), self._alias("null", len(chunk)), default=0
                )
        if include_row_count and "__row_count__" not in counts:
            counts["__row_count__"] = 0
        return counts, statements[0] if len(statements) == 1 else statements

    @staticmethod
    def _chunk_columns(columns: List[str], size: int) -> List[List[str]]:
        if size <= 0 or size >= len(columns):
            return [list(columns)] if columns else [[]]
        return [
            list(columns[index:index + size])
            for index in range(0, len(columns), size)
        ]

    # ── Result construction ───────────────────────────────────
    def _null_count_result(
        self,
        mapping: Dict[str, str],
        source_count: int,
        target_count: int,
        source_total: int,
        target_total: int,
        details: Dict[str, Any],
    ) -> CheckResult:
        source_col = mapping["source_column"]
        target_col = mapping["target_column"]
        src_pct = (source_count / source_total * 100) if source_total else 0.0
        tgt_pct = (target_count / target_total * 100) if target_total else 0.0
        delta = target_count - source_count
        delta_pct = tgt_pct - src_pct
        # Null-count reconciliation is based on the absolute counts. Percentage
        # differences are retained below as reporting information only because
        # source and target row totals can differ even when their null counts
        # are identical.
        status = (
            ValidationStatus.PASS
            if source_count == target_count
            else ValidationStatus.FAIL
        )
        return CheckResult(
            check_name=f"null_count_{mapping['name']}",
            status=status,
            source_value=source_count,
            target_value=target_count,
            delta=delta,
            delta_pct=round(delta_pct, 6),
            message=(
                f"Null counts match for {source_col} -> {target_col}: {source_count}"
                if status == ValidationStatus.PASS
                else (
                    f"Null count mismatch for {source_col} -> {target_col}: "
                    f"source={source_count}, target={target_count}, delta={delta}"
                )
            ),
            details={
                **details,
                "source_column": source_col,
                "target_column": target_col,
                "source_total_row_count": source_total,
                "target_total_row_count": target_total,
                "source_null_percentage": round(src_pct, 6),
                "target_null_percentage": round(tgt_pct, 6),
                "null_count_difference": delta,
                "null_percentage_difference": round(delta_pct, 6),
            },
        )

    @staticmethod
    def _mandatory_result(
        mapping: Dict[str, str],
        side: str,
        column: str,
        null_count: int,
        details: Dict[str, Any],
    ) -> CheckResult:
        status = ValidationStatus.PASS if null_count == 0 else ValidationStatus.FAIL
        return CheckResult(
            check_name=f"mandatory_null_check_{side}_{mapping['name']}",
            status=status,
            source_value=null_count if side == "source" else None,
            target_value=null_count if side == "target" else None,
            delta=null_count,
            message=(
                f"Mandatory column {column} has no nulls in {side}"
                if status == ValidationStatus.PASS
                else f"Mandatory column {column} has {null_count} nulls in {side}"
            ),
            details=details,
        )

    # ── Per-column fallback ───────────────────────────────────
    def _compare_null_counts(
        self,
        source_conn,
        target_conn,
        src_fqn: str,
        tgt_fqn: str,
        mappings: List[Dict[str, str]],
        table_map,
        execution_cache=None,
    ) -> List[CheckResult]:
        results = []
        # The record count does not depend on the column, so it is read once per
        # side and reused for every configured column (and, through the run
        # cache, by every other validator of this table).
        totals: Dict[str, Tuple[int, str]] = {}
        for mapping in mappings:
            source_col = mapping["source_column"]
            target_col = mapping["target_column"]
            display_name = mapping["name"]
            try:
                src_count, src_sql = self._null_count(
                    source_conn, src_fqn, source_col, table_map, "source"
                )
                tgt_count, tgt_sql = self._null_count(
                    target_conn, tgt_fqn, target_col, table_map, "target"
                )
                if "source" not in totals:
                    totals["source"] = self._row_count(
                        source_conn, src_fqn, table_map, "source", execution_cache
                    )
                if "target" not in totals:
                    totals["target"] = self._row_count(
                        target_conn, tgt_fqn, table_map, "target", execution_cache
                    )
                src_total, src_total_sql = totals["source"]
                tgt_total, tgt_total_sql = totals["target"]
                result = self._null_count_result(
                    mapping,
                    src_count,
                    tgt_count,
                    src_total,
                    tgt_total,
                    {
                        "execution_strategy": "pushdown_per_column",
                        "source_sql": src_sql,
                        "target_sql": tgt_sql,
                        "source_total_sql": src_total_sql,
                        "target_total_sql": tgt_total_sql,
                    },
                )
                results.append(result)
            except Exception as e:
                results.append(
                    CheckResult(
                        check_name=f"null_count_{display_name}",
                        status=ValidationStatus.ERROR,
                        message=(
                            f"Null count comparison failed for {source_col} -> "
                            f"{target_col}: {e}"
                        ),
                    )
                )
        return results

    def _row_count(
        self, conn, fqn: str, table_map, role=None, execution_cache=None
    ) -> Tuple[int, str]:
        """The side's exact row count, reused from the run-local cache when known.

        The count is executed at most once per table / side: a value already
        produced by the shared statistics scan, the row-count check or the
        edge-case check is reused, and a value computed here is published for
        every later consumer under the same single-flight key.
        """
        dialect = self._dialect(conn)
        sql = dialect.select(
            f"{dialect.count_all()} AS row_count",
            fqn,
            self._build_where_clause(table_map, dialect, role),
        )
        if execution_cache is None or role is None:
            df = conn.execute_query(sql)
            return int(df.iloc[0, 0]), sql
        compute = lambda: int(conn.execute_query(sql).iloc[0, 0])
        return self._row_count_once(
            execution_cache, role, compute, consumer="null_validation"
        ), sql

    def _detect_unexpected_nulls(
        self,
        source_conn,
        target_conn,
        src_fqn: str,
        tgt_fqn: str,
        mappings: List[Dict[str, str]],
        table_map,
    ) -> List[CheckResult]:
        return self._unexpected_null_placeholder(table_map)

    @staticmethod
    def _unexpected_null_placeholder(table_map) -> List[CheckResult]:
        """Report where record-level null localization is performed instead."""
        if not table_map.key_mappings:
            return [
                CheckResult(
                    check_name="unexpected_null_introduction",
                    status=ValidationStatus.SKIP,
                    message="Skipped unexpected null detection: no key_mappings configured",
                )
            ]

        source_keys = [m.source_column for m in table_map.key_mappings]
        target_keys = [m.target_column for m in table_map.key_mappings]

        # Cross-system row joins cannot be pushed down to either database. The
        # aggregate null comparisons above remain authoritative and bounded;
        # record-level null localization belongs to the keyset comparison.
        return [
            CheckResult(
                check_name="unexpected_null_introduction",
                status=ValidationStatus.SKIP,
                message=(
                    "Skipped cross-system null localization to prevent an "
                    "unbounded dataframe load; use keyset record comparison"
                ),
                details={
                    "source_keys": source_keys,
                    "target_keys": target_keys,
                    "execution_strategy": "sql_pushdown_only",
                },
            )
        ]

    def _check_mandatory_columns(
        self,
        source_conn,
        target_conn,
        src_fqn: str,
        tgt_fqn: str,
        mappings: List[Dict[str, str]],
        table_map,
    ) -> List[CheckResult]:
        results = []
        for mapping in mappings:
            for side, conn, fqn, col in (
                ("source", source_conn, src_fqn, mapping["source_column"]),
                ("target", target_conn, tgt_fqn, mapping["target_column"]),
            ):
                try:
                    null_count, sql = self._null_count(
                        conn, fqn, col, table_map, side
                    )
                    results.append(self._mandatory_result(
                        mapping,
                        side,
                        col,
                        null_count,
                        {
                            "execution_strategy": "pushdown_per_column",
                            "sql": sql,
                            "column": col,
                        },
                    ))
                except Exception as e:
                    results.append(
                        CheckResult(
                            check_name=f"mandatory_null_check_{side}_{mapping['name']}",
                            status=ValidationStatus.ERROR,
                            message=(
                                "Mandatory column null check failed for "
                                f"{side}.{col}: {e}"
                            ),
                        )
                    )
        return results

    def _null_count(
        self, conn, fqn: str, column: str, table_map, role=None
    ) -> Tuple[int, str]:
        dialect = self._dialect(conn)
        sql = dialect.select(
            f"SUM(CASE WHEN {dialect.quote(column)} IS NULL THEN 1 ELSE 0 END) AS null_count",
            fqn,
            self._build_where_clause(table_map, dialect, role),
        )
        df = conn.execute_query(sql)
        value = df.iloc[0, 0]
        return int(value or 0), sql

    # ── Configuration normalization ───────────────────────────
    def _resolve_null_mappings(self, table_map) -> List[Dict[str, str]]:
        configured = getattr(table_map, "null_columns", None) or []
        if configured:
            return self._normalize_mappings(configured)

        compare_mappings = getattr(table_map, "compare_column_mappings", None) or []
        if compare_mappings:
            return self._normalize_mappings(compare_mappings)

        return self._normalize_mappings(getattr(table_map, "numeric_columns", []) or [])

    def _normalize_mappings(self, columns: List[Any]) -> List[Dict[str, str]]:
        mappings = []
        for item in columns or []:
            if isinstance(item, str):
                mappings.append(
                    {
                        "source_column": item,
                        "target_column": item,
                        "name": item,
                    }
                )
            elif isinstance(item, dict):
                source_col = item.get("source_column") or item.get("column")
                target_col = item.get("target_column", source_col)
                if not source_col or not target_col:
                    continue
                mappings.append(
                    {
                        "source_column": source_col,
                        "target_column": target_col,
                        "name": item.get("name", f"{source_col}_to_{target_col}"),
                    }
                )
        return mappings

    def _build_merge_key(self, df: pd.DataFrame, key_columns: List[str]) -> pd.Series:
        return df[key_columns].astype(str).agg("||".join, axis=1)

    def _merged_column_name(
        self,
        column: str,
        owning_df: pd.DataFrame,
        other_df: pd.DataFrame,
        suffix: str,
    ) -> str:
        if column in owning_df.columns and column in other_df.columns:
            return f"{column}{suffix}"
        return column

    def _dedupe(self, values: List[str]) -> List[str]:
        seen = set()
        result = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result
