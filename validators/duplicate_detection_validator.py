from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from core.execution_engine import ExecutionContext
from core.models import CheckResult, ValidationStatus
from core.scan_planner import DUPLICATE_GROUP_ALIAS, StatisticRequest
from .base_validator import BaseValidator
from .key_validation_validator import (
    KeyValidationValidator,
    duplicate_group_cache_key,
    key_statistic_requests,
    key_statistics_cache_key,
)
from .validator_utils import _fqn

logger = logging.getLogger(__name__)


class DuplicateDetectionValidator(BaseValidator):
    """Detect duplicates entirely on the configured engines.

    Every check is a grouped count pushed down to the Source or Target. The
    duplicate-key statistic comes from the run's shared statistics scan, so the
    same group-by never runs twice for one table — and never runs a third time
    for record comparison or data value validation either.
    """

    @staticmethod
    def statistic_requests(table_map) -> List[StatisticRequest]:
        """The statistics this check needs from the shared scan."""
        requests = key_statistic_requests(table_map)
        return [
            StatisticRequest(
                key=item.key,
                kind=item.kind,
                source_column=item.source_column,
                target_column=item.target_column,
                requested_by=("duplicate_check",),
            )
            for item in requests
        ]

    def validate(
        self,
        source_conn,
        target_conn,
        table_map,
        thresholds,
        context: ExecutionContext = None,
        source_type: str = "generic",
        execution_config=None,
        execution_cache=None,
    ) -> List[CheckResult]:
        src_fqn = _fqn(
            table_map.source_schema,
            table_map.source_table,
            self._dialect(source_conn),
        )
        tgt_fqn = _fqn(
            table_map.target_schema,
            table_map.target_table,
            self._dialect(target_conn),
        )

        results = []
        results.extend(self._validate_key_duplicates(
            source_conn, target_conn, src_fqn, tgt_fqn, table_map,
            execution_config, execution_cache,
        ))
        results.extend(self._validate_full_row_duplicates(
            source_conn, target_conn, src_fqn, tgt_fqn, table_map, execution_config,
        ))
        results.extend(self._validate_near_duplicates(
            source_conn, target_conn, src_fqn, tgt_fqn, table_map, execution_config,
            self._configured_normalization(execution_cache),
        ))
        results.extend(self._validate_post_transform_duplicates(target_conn, tgt_fqn, table_map))

        if not results:
            return [
                CheckResult(
                    check_name="duplicate_check",
                    status=ValidationStatus.SKIP,
                    message="Skipped duplicate check: no duplicate validation inputs configured",
                )
            ]

        return results

    def _validate_key_duplicates(
        self,
        source_conn,
        target_conn,
        src_fqn: str,
        tgt_fqn: str,
        table_map,
        execution_config=None,
        execution_cache=None,
    ) -> List[CheckResult]:
        source_keys = [m.source_column for m in table_map.key_mappings]
        target_keys = [m.target_column for m in table_map.key_mappings]

        if not source_keys or not target_keys:
            return [
                CheckResult(
                    check_name="duplicate_key_check",
                    status=ValidationStatus.SKIP,
                    message="Skipped key duplicate check: no key_mappings configured",
                )
            ]

        try:
            (
                (src_duplicate_groups, src_sql),
                (tgt_duplicate_groups, tgt_sql),
            ) = self._run_both_sides(
                execution_config,
                lambda: self._cached(
                    execution_cache,
                    duplicate_group_cache_key("source", source_keys),
                    lambda: self._shared_duplicate_group_count(
                        execution_cache, "source", source_conn, src_fqn,
                        source_keys, table_map,
                    ),
                ),
                lambda: self._cached(
                    execution_cache,
                    duplicate_group_cache_key("target", target_keys),
                    lambda: self._shared_duplicate_group_count(
                        execution_cache, "target", target_conn, tgt_fqn,
                        target_keys, table_map,
                    ),
                ),
            )

            delta = tgt_duplicate_groups - src_duplicate_groups
            duplicates_allowed = bool(getattr(table_map, "allow_duplicates", False))
            status = (
                ValidationStatus.PASS
                if duplicates_allowed or (src_duplicate_groups == 0 and tgt_duplicate_groups == 0)
                else ValidationStatus.FAIL
            )

            return [
                CheckResult(
                    check_name="duplicate_key_check",
                    status=status,
                    source_value=src_duplicate_groups,
                    target_value=tgt_duplicate_groups,
                    delta=delta,
                    message=(
                        "Duplicate key groups are not increased in target"
                        if status == ValidationStatus.PASS
                        else (
                            "Unexpected duplicate key groups found in target: "
                            f"source={src_duplicate_groups}, target={tgt_duplicate_groups}, increase={delta}"
                        )
                    ),
                    details={
                        "source_keys": source_keys,
                        "target_keys": target_keys,
                        "source_sql": src_sql,
                        "target_sql": tgt_sql,
                        "execution_strategy": "pushdown",
                        **self._incremental_scope(table_map),
                    },
                )
            ]
        except Exception as e:
            return [
                CheckResult(
                    check_name="duplicate_key_check",
                    status=ValidationStatus.ERROR,
                    message=f"Key duplicate check failed: {e}",
                )
            ]

    def _validate_full_row_duplicates(
        self,
        source_conn,
        target_conn,
        src_fqn: str,
        tgt_fqn: str,
        table_map,
        execution_config=None,
    ) -> List[CheckResult]:
        configured_columns = self._normalize_columns(getattr(table_map, "duplicate_columns", []) or [])

        if not configured_columns:
            return [
                CheckResult(
                    check_name="duplicate_full_row_check",
                    status=ValidationStatus.SKIP,
                    message=(
                        "Skipped full-row duplicate validation: duplicate_columns "
                        "must be configured for SQL pushdown"
                    ),
                )
            ]

        try:
            (
                (src_count, src_sql, src_samples),
                (tgt_count, tgt_sql, tgt_samples),
            ) = self._run_both_sides(
                execution_config,
                lambda: self._duplicate_rows_sql(
                    source_conn, src_fqn, configured_columns, table_map, "source"
                ),
                lambda: self._duplicate_rows_sql(
                    target_conn, tgt_fqn, configured_columns, table_map, "target"
                ),
            )

            status = (
                ValidationStatus.PASS
                if bool(getattr(table_map, "allow_duplicates", False))
                or (src_count == 0 and tgt_count == 0)
                else ValidationStatus.FAIL
            )
            return [
                CheckResult(
                    check_name="duplicate_full_row_check",
                    status=status,
                    source_value=src_count,
                    target_value=tgt_count,
                    delta=tgt_count - src_count,
                    message=(
                        "No exact duplicate rows found in source or target"
                        if status == ValidationStatus.PASS
                        else f"Exact duplicate rows found: source={src_count}, target={tgt_count}"
                    ),
                    details={
                        "columns": configured_columns or "ALL_COLUMNS",
                        "source_sql": src_sql,
                        "target_sql": tgt_sql,
                        "sample_source_duplicates": src_samples,
                        "sample_target_duplicates": tgt_samples,
                    },
                )
            ]
        except Exception as e:
            return [
                CheckResult(
                    check_name="duplicate_full_row_check",
                    status=ValidationStatus.ERROR,
                    message=f"Full-row duplicate detection failed: {e}",
                )
            ]

    def _validate_near_duplicates(
        self,
        source_conn,
        target_conn,
        src_fqn: str,
        tgt_fqn: str,
        table_map,
        execution_config=None,
        normalization: Optional[Dict[str, Any]] = None,
    ) -> List[CheckResult]:
        columns = self._normalize_columns(getattr(table_map, "near_duplicate_columns", []) or [])
        if not columns:
            return [
                CheckResult(
                    check_name="near_duplicate_check",
                    status=ValidationStatus.SKIP,
                    message="Skipped near-duplicate check: no near_duplicate_columns configured",
                )
            ]

        try:
            (
                (src_count, src_sql, src_samples),
                (tgt_count, tgt_sql, tgt_samples),
            ) = self._run_both_sides(
                execution_config,
                lambda: self._normalized_duplicate_groups(
                    source_conn, src_fqn, columns, table_map, normalization, "source"
                ),
                lambda: self._normalized_duplicate_groups(
                    target_conn, tgt_fqn, columns, table_map, normalization, "target"
                ),
            )
            status = ValidationStatus.PASS if src_count == 0 and tgt_count == 0 else ValidationStatus.FAIL
            return [
                CheckResult(
                    check_name="near_duplicate_check",
                    status=status,
                    source_value=src_count,
                    target_value=tgt_count,
                    delta=tgt_count - src_count,
                    message=(
                        "No near-duplicate groups found"
                        if status == ValidationStatus.PASS
                        else f"Near-duplicate groups found: source={src_count}, target={tgt_count}"
                    ),
                    details={
                        "columns": columns,
                        "normalization": {
                            "source": "comparison_settings.hash_comparison.normalization",
                            "trim_strings": (normalization or {}).get(
                                "trim_strings", True
                            ),
                            "case_sensitive": (normalization or {}).get(
                                "case_sensitive", False
                            ),
                        },
                        "source_sql": src_sql,
                        "target_sql": tgt_sql,
                        "sample_source_near_duplicates": src_samples,
                        "sample_target_near_duplicates": tgt_samples,
                    },
                )
            ]
        except Exception as e:
            return [
                CheckResult(
                    check_name="near_duplicate_check",
                    status=ValidationStatus.ERROR,
                    message=f"Near-duplicate detection failed: {e}",
                )
            ]

    def _validate_post_transform_duplicates(self, target_conn, tgt_fqn: str, table_map) -> List[CheckResult]:
        columns = self._normalize_columns(getattr(table_map, "post_transform_duplicate_columns", []) or [])
        if not columns:
            return [
                CheckResult(
                    check_name="post_transform_duplicate_check",
                    status=ValidationStatus.SKIP,
                    message="Skipped post-transformation duplicate check: no post_transform_duplicate_columns configured",
                )
            ]

        try:
            duplicate_groups, sql = self._get_duplicate_group_count(
                target_conn, tgt_fqn, columns, table_map, "target"
            )
            status = ValidationStatus.PASS if duplicate_groups == 0 else ValidationStatus.FAIL
            return [
                CheckResult(
                    check_name="post_transform_duplicate_check",
                    status=status,
                    target_value=duplicate_groups,
                    delta=duplicate_groups,
                    message=(
                        "No post-transformation duplicate groups found in target"
                        if status == ValidationStatus.PASS
                        else f"Post-transformation duplicate groups found in target: {duplicate_groups}"
                    ),
                    details={
                        "target_columns": columns,
                        "target_sql": sql,
                    },
                )
            ]
        except Exception as e:
            return [
                CheckResult(
                    check_name="post_transform_duplicate_check",
                    status=ValidationStatus.ERROR,
                    message=f"Post-transformation duplicate check failed: {e}",
                )
            ]

    def _get_duplicate_group_count(
        self,
        conn,
        fqn: str,
        keys: List[str],
        table_map,
        role=None,
    ) -> Tuple[int, str]:
        dialect = self._dialect(conn)
        keys_str = ", ".join(dialect.quote(key) for key in keys)
        where = self._build_where_clause(table_map, dialect, role)
        inner = dialect.select(keys_str, fqn, where) + (
            f" GROUP BY {keys_str} HAVING {dialect.count_all()} > 1"
        )
        sql = dialect.select(
            f"{dialect.count_column('*')} AS duplicate_groups",
            dialect.derived_table(inner, "__mv_duplicate_groups"),
        )
        df = conn.execute_query(sql)
        return int(df.iloc[0, 0]), sql

    def _shared_duplicate_group_count(
        self, execution_cache, role: str, conn, fqn: str,
        keys: List[str], table_map,
    ) -> Tuple[int, str]:
        """Reuse the one grouped scan every key-based check shares."""
        table_id = getattr(table_map, "id", "unknown")
        provider = self._shared_statistics(execution_cache)

        if provider is None:
            self._log_fallback(
                validator_name="duplicate_check",
                table_id=table_id,
                role=role,
                statistic=DUPLICATE_GROUP_ALIAS,
                reason="no_shared_statistics_provider",
            )
        elif not provider.provides(DUPLICATE_GROUP_ALIAS):
            self._log_fallback(
                validator_name="duplicate_check",
                table_id=table_id,
                role=role,
                statistic=DUPLICATE_GROUP_ALIAS,
                reason="statistic_not_in_shared_scan",
            )
        else:
            try:
                statement = provider.statement(role)
                return (
                    provider.integer(role, DUPLICATE_GROUP_ALIAS),
                    statement if isinstance(statement, str) else str(statement),
                )
            except Exception as e:
                # Shared scan failed; log reason for fallback
                self._log_fallback(
                    validator_name="duplicate_check",
                    table_id=table_id,
                    role=role,
                    statistic=DUPLICATE_GROUP_ALIAS,
                    reason=f"provider_error_{type(e).__name__}",
                )

        # Fall back to independent execution
        if execution_cache is None:
            return self._get_duplicate_group_count(conn, fqn, keys, table_map, role)
        validator = KeyValidationValidator()
        statistics = execution_cache.get_or_compute(
            key_statistics_cache_key(role, keys, table_map),
            lambda: validator._execute_statistics(role, conn, fqn, keys, table_map),
        )
        if statistics.error is not None:
            raise statistics.error
        return statistics.duplicate_groups, statistics.sql

    def _duplicate_rows_sql(
        self,
        conn,
        fqn: str,
        columns: List[str],
        table_map,
        role=None,
    ) -> Tuple[int, str, List[Dict[str, Any]]]:
        if not columns:
            raise ValueError("duplicate_columns are required for SQL pushdown")
        dialect = self._dialect(conn)
        group_columns = ", ".join(dialect.quote(column) for column in columns)
        inner = dialect.select(
            group_columns, fqn, self._build_where_clause(table_map, dialect, role)
        ) + f" GROUP BY {group_columns} HAVING {dialect.count_all()} > 1"
        sql = dialect.select(
            f"{dialect.count_all()} AS duplicate_groups",
            dialect.derived_table(inner, "duplicate_groups"),
        )
        df = conn.execute_query(sql)
        return int(df.iloc[0, 0]), sql, []

    def _normalized_duplicate_groups(
        self,
        conn,
        fqn: str,
        columns: List[str],
        table_map,
        normalization: Optional[Dict[str, Any]] = None,
        role=None,
    ) -> Tuple[int, str, List[Dict[str, Any]]]:
        """Group on the configured normalization of each near-duplicate column.

        The normalization steps come from the configured hash normalization
        policy, so "near" means the same thing here as it does everywhere else
        in the framework.
        """
        dialect = self._dialect(conn)
        policy = normalization or {}
        applied: List[str] = []
        normalized_columns = []
        for column in columns:
            expression = dialect.cast_to_text(dialect.quote(column))
            expression = f"COALESCE({expression}, '')"
            if policy.get("trim_strings", True):
                expression = f"LTRIM(RTRIM({expression}))"
                applied.append("trim")
            if not policy.get("case_sensitive", False):
                expression = f"LOWER({expression})"
                applied.append("lower")
            normalized_columns.append(expression)
        group_columns = ", ".join(normalized_columns)
        inner = dialect.select(
            group_columns, fqn, self._build_where_clause(table_map, dialect, role)
        ) + f" GROUP BY {group_columns} HAVING {dialect.count_all()} > 1"
        sql = dialect.select(
            f"{dialect.count_all()} AS duplicate_groups",
            dialect.derived_table(inner, "normalized_duplicate_groups"),
        )
        df = conn.execute_query(sql)
        if df.empty:
            return 0, sql, []
        return int(df.iloc[0, 0]), sql, []

    def _normalize_columns(self, columns: List[Any]) -> List[str]:
        normalized = []
        for item in columns or []:
            if isinstance(item, str):
                normalized.append(item)
            elif isinstance(item, dict):
                col = item.get("target_column") or item.get("source_column") or item.get("column")
                if col:
                    normalized.append(col)
        return normalized
