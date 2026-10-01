from __future__ import annotations

import contextlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from core.execution_engine import ExecutionContext, KeyValidationExecutionConfig
from core.models import CheckResult, ValidationStatus
from core.scan_planner import (
    DISTINCT_KEY_ALIAS,
    DUPLICATE_GROUP_ALIAS,
    NULL_KEY_ALIAS,
    ROW_COUNT_KEY,
    StatisticRequest,
    statistic_cache_key,
)
from core.strategy import ExecutionStrategy
from .base_validator import BaseValidator
from .key_cohort import KeyCohortBuilder, combine_where
from .record_comparison_validator import RecordComparisonValidator
from .validator_utils import _fqn

logger = logging.getLogger(__name__)


def duplicate_group_cache_key(role: str, columns: Sequence[str]) -> Tuple[Any, ...]:
    """Backward-compatible cache key consumed by duplicate detection."""
    return ("duplicate_group_count", role, tuple(columns))


def key_statistics_cache_key(
    role: str, columns: Sequence[str], table_map=None
) -> Tuple[Any, ...]:
    """Run-local key for the single combined key-statistics scan."""
    if table_map is not None:
        scope = BaseValidator()._scope_token(table_map)
        scope_identity = json.dumps(
            [
                getattr(table_map, "id", None),
                getattr(table_map, "migration_phase", None),
                scope,
            ],
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )
        return (
            "key_statistics", getattr(table_map, "id", None), role,
            getattr(table_map, "migration_phase", None), scope_identity,
            tuple(columns),
        )
    return ("key_statistics", role, tuple(columns))


def key_statistic_requests(table_map) -> List[StatisticRequest]:
    """The key statistics the shared scan must produce for one table."""
    if not getattr(table_map, "key_mappings", None):
        return []
    return [
        StatisticRequest(
            key=ROW_COUNT_KEY, kind="row_count",
            requested_by=("key_validation",),
        ),
        StatisticRequest(
            key=NULL_KEY_ALIAS, kind="key_null_rows",
            requested_by=("key_validation",),
        ),
        StatisticRequest(
            key=DISTINCT_KEY_ALIAS, kind="key_distinct_count",
            requested_by=("key_validation",),
        ),
        StatisticRequest(
            key=DUPLICATE_GROUP_ALIAS, kind="key_duplicate_groups",
            requested_by=("key_validation",),
        ),
    ]


@dataclass(frozen=True)
class _SideStatistics:
    role: str
    sql: str
    total_rows: int = 0
    null_rows: int = 0
    distinct_keys: int = 0
    duplicate_groups: int = 0
    elapsed: float = 0.0
    started_at: str = ""
    completed_at: str = ""
    error: Optional[Exception] = None


@dataclass(frozen=True)
class _PartitionResult:
    total_keys: int
    missing_keys: int
    samples: Tuple[Tuple[str, ...], ...]
    partitions: int
    elapsed: float
    worker_count: int


class KeyValidationValidator(BaseValidator):
    """Exact, full-scan key validation with bounded cross-system comparison."""

    #: The statistics this check needs from the shared scan.
    statistic_requests = staticmethod(key_statistic_requests)

    def __init__(self) -> None:
        self._comparison = RecordComparisonValidator()

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
        if not table_map.key_mappings:
            return [CheckResult(
                check_name="key_validation",
                status=ValidationStatus.SKIP,
                message="Skipped key validation: no key_mappings configured",
            )]

        key_config = self._key_config(execution_config)
        if not key_config.full_scan:
            return [CheckResult(
                check_name="key_validation",
                status=ValidationStatus.ERROR,
                message="Key validation requires full_scan=true; sampling is unsupported",
            )]

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
        source_keys = [mapping.source_column for mapping in table_map.key_mappings]
        target_keys = [mapping.target_column for mapping in table_map.key_mappings]

        source_stats, target_stats = self._collect_both_statistics(
            source_conn, target_conn, src_fqn, tgt_fqn, source_keys, target_keys,
            table_map, execution_config, execution_cache, key_config,
        )
        results = self._statistics_results(
            source_stats, target_stats, source_keys, target_keys, table_map
        )
        results.append(self._validate_key_set_consistency(
            source_conn, target_conn, src_fqn, tgt_fqn, source_keys, target_keys,
            table_map, execution_config, key_config, source_stats, target_stats,
        ))
        return results

    # -- One grouped statistics scan per side ---------------------------------
    def _collect_both_statistics(
        self, source_conn, target_conn, src_fqn: str, tgt_fqn: str,
        source_keys: List[str], target_keys: List[str], table_map,
        execution_config, execution_cache,
        key_config: KeyValidationExecutionConfig,
    ) -> Tuple[_SideStatistics, _SideStatistics]:
        source_call = lambda: self._cached_statistics(
            execution_cache, "source", source_conn, src_fqn, source_keys, table_map
        )
        target_call = lambda: self._cached_statistics(
            execution_cache, "target", target_conn, tgt_fqn, target_keys, table_map
        )
        if key_config.parallel_execution:
            return self._run_both_sides(execution_config, source_call, target_call)
        return source_call(), target_call()

    def _cached_statistics(
        self, execution_cache, role: str, conn, fqn: str,
        keys: List[str], table_map,
    ) -> _SideStatistics:
        shared = self._statistics_from_shared_scan(
            execution_cache, role, conn, fqn, keys, table_map
        )
        if shared is not None:
            result = shared
        else:
            result = self._cached(
                execution_cache,
                key_statistics_cache_key(role, keys, table_map),
                lambda: self._execute_statistics(role, conn, fqn, keys, table_map),
            )
        if execution_cache is not None and result.error is None:
            scoped_identity = key_statistics_cache_key(
                role, keys, table_map
            )[4]
            execution_cache.put(
                statistic_cache_key(role, scoped_identity, ROW_COUNT_KEY),
                result.total_rows,
            )
            execution_cache.put(
                statistic_cache_key(role, scoped_identity, NULL_KEY_ALIAS),
                result.null_rows,
            )
            execution_cache.put(
                statistic_cache_key(
                    role, scoped_identity, DUPLICATE_GROUP_ALIAS
                ),
                result.duplicate_groups,
            )
            execution_cache.put(
                duplicate_group_cache_key(role, keys),
                (result.duplicate_groups, result.sql),
            )
            execution_cache.put(("row_count", role), result.total_rows)
        return result

    def _statistics_from_shared_scan(
        self, execution_cache, role: str, conn, fqn: str,
        keys: List[str], table_map,
    ) -> Optional[_SideStatistics]:
        """Build this side's statistics from the run's shared scan, if present.

        The shared scan computes exactly the same four values from exactly the
        same grouped read, so the results are identical; the difference is that
        one scan now serves key validation, duplicate detection, row count,
        null validation and the audit reconciliation together.
        """
        provider = self._shared_statistics(execution_cache)
        if provider is None:
            return None
        needed = (
            ROW_COUNT_KEY, NULL_KEY_ALIAS, DISTINCT_KEY_ALIAS,
            DUPLICATE_GROUP_ALIAS,
        )
        if not all(provider.provides(key) for key in needed):
            return None
        started_at = datetime.now(timezone.utc).isoformat()
        try:
            values = {
                "total_rows": provider.integer(role, ROW_COUNT_KEY),
                "null_rows": provider.integer(role, NULL_KEY_ALIAS),
                "distinct_keys": provider.integer(role, DISTINCT_KEY_ALIAS),
                "duplicate_groups": provider.integer(role, DUPLICATE_GROUP_ALIAS),
            }
        except Exception:
            return None
        execution = provider.execution(role)
        statement = provider.statement(role)
        return _SideStatistics(
            role=role,
            sql=statement if isinstance(statement, str) else str(statement),
            elapsed=execution.elapsed,
            started_at=started_at,
            completed_at=datetime.now(timezone.utc).isoformat(),
            **values,
        )

    def _execute_statistics(
        self, role: str, conn, fqn: str, keys: List[str], table_map,
    ) -> _SideStatistics:
        sql = self._combined_statistics_sql(fqn, keys, table_map, conn, role)
        started_at = datetime.now(timezone.utc).isoformat()
        started = time.perf_counter()
        try:
            frame = conn.execute_query(sql)
            elapsed = round(time.perf_counter() - started, 6)
            values = {
                "total_rows": self._required_nonnegative_scalar(frame, "total_row_count"),
                "null_rows": self._required_nonnegative_scalar(frame, "null_key_count"),
                "distinct_keys": self._required_nonnegative_scalar(frame, "distinct_key_count"),
                "duplicate_groups": self._required_nonnegative_scalar(
                    frame, "duplicate_group_count"
                ),
            }
            logger.debug(
                "[QUERY_TIMING] key_validation query role=%s duration=%.6fs "
                "total_rows=%d duplicate_groups=%d",
                role,
                elapsed,
                values["total_rows"],
                values["duplicate_groups"],
            )
            return _SideStatistics(
                role=role, sql=sql,
                elapsed=elapsed,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc).isoformat(),
                **values,
            )
        except Exception as exc:
            elapsed = round(time.perf_counter() - started, 6)
            logger.debug(
                "[QUERY_TIMING] key_validation query role=%s failed after %.6fs: %s",
                role,
                elapsed,
                type(exc).__name__,
            )
            return _SideStatistics(
                role=role, sql=sql,
                elapsed=elapsed,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc).isoformat(),
                error=exc,
            )

    def _combined_statistics_sql(
        self, fqn: str, keys: Sequence[str], table_map, connection=None,
        role=None,
    ) -> str:
        dialect = self._dialect(connection)
        quoted_keys = [dialect.quote(key) for key in keys]
        key_list = ", ".join(quoted_keys)
        null_condition = " OR ".join(f"{key} IS NULL" for key in quoted_keys)
        where = self._build_where_clause(table_map, dialect, role)
        inner = dialect.select(
            f"{key_list}, {dialect.count_all()} AS __mv_group_rows", fqn, where
        ) + f" GROUP BY {key_list}"
        outer = (
            "COALESCE(SUM(__mv_group_rows), 0) AS total_row_count, "
            "COALESCE(SUM(CASE WHEN "
            f"{null_condition} THEN __mv_group_rows ELSE 0 END), 0) AS null_key_count, "
            f"{dialect.count_column('*')} AS distinct_key_count, "
            "COALESCE(SUM(CASE WHEN __mv_group_rows > 1 THEN 1 ELSE 0 END), 0) "
            "AS duplicate_group_count"
        )
        return dialect.select(
            outer, dialect.derived_table(inner, "__mv_key_groups")
        )

    @staticmethod
    def _required_nonnegative_scalar(frame: pd.DataFrame, name: str) -> int:
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            raise ValueError(f"Combined key statistics did not return {name}")
        columns = {str(column).casefold(): column for column in frame.columns}
        column = columns.get(name.casefold())
        if column is None:
            raise ValueError(f"Combined key statistics did not return {name}")
        value = frame.iloc[0][column]
        count = int(value or 0)
        if count < 0 or value != count:
            raise ValueError(f"Combined key statistics returned invalid {name}")
        return count

    def _statistics_results(
        self, source: _SideStatistics, target: _SideStatistics,
        source_keys: List[str], target_keys: List[str], table_map,
    ) -> List[CheckResult]:
        results = [
            self._null_result(source, source_keys, table_map),
            self._null_result(target, target_keys, table_map),
        ]
        elapsed = max(source.elapsed, target.elapsed)
        timing = self._timing_details(
            min(source.started_at, target.started_at),
            max(source.completed_at, target.completed_at),
            elapsed,
        )
        if source.error or target.error:
            errors = "; ".join(
                f"{item.role}: {item.error}"
                for item in (source, target) if item.error is not None
            )
            results.append(CheckResult(
                check_name="key_validation_duplicates",
                status=ValidationStatus.ERROR,
                message=f"Duplicate key validation failed: {errors}",
                details=timing,
            ))
            return results

        status = (
            ValidationStatus.PASS
            if source.duplicate_groups == 0 and target.duplicate_groups == 0
            else ValidationStatus.FAIL
        )
        results.append(CheckResult(
            check_name="key_validation_duplicates",
            status=status,
            source_value=source.duplicate_groups,
            target_value=target.duplicate_groups,
            delta=target.duplicate_groups - source.duplicate_groups,
            message=(
                "No duplicate key groups found in source or target"
                if status is ValidationStatus.PASS
                else "Duplicate key groups found: "
                f"source={source.duplicate_groups}, target={target.duplicate_groups}"
            ),
            details={
                **timing,
                "source_sql": source.sql,
                "target_sql": target.sql,
                "source_keys": source_keys,
                "target_keys": target_keys,
                "source_execution_time": source.elapsed,
                "target_execution_time": target.elapsed,
                "source_rows_scanned": source.total_rows,
                "target_rows_scanned": target.total_rows,
                "rows_scanned": source.total_rows + target.total_rows,
                "execution_strategy": "combined_statistics_pushdown",
                "full_scan": True,
                **self._incremental_scope(table_map),
            },
        ))
        return results

    def _null_result(
        self, stats: _SideStatistics, keys: List[str], table_map,
    ) -> CheckResult:
        details = {
            **self._timing_details(stats.started_at, stats.completed_at, stats.elapsed),
            "sql": stats.sql,
            "key_columns": keys,
            "execution_strategy": "combined_statistics_pushdown",
            "full_scan": True,
            f"{stats.role}_rows_scanned": stats.total_rows,
            "rows_scanned": stats.total_rows,
            **self._incremental_scope(table_map),
        }
        if stats.error is not None:
            return CheckResult(
                check_name=f"key_validation_nulls_{stats.role}",
                status=ValidationStatus.ERROR,
                message=f"Null key validation failed for {stats.role}: {stats.error}",
                details=details,
            )
        status = ValidationStatus.PASS if stats.null_rows == 0 else ValidationStatus.FAIL
        return CheckResult(
            check_name=f"key_validation_nulls_{stats.role}",
            status=status,
            source_value=stats.null_rows if stats.role == "source" else None,
            target_value=stats.null_rows if stats.role == "target" else None,
            delta=stats.null_rows,
            message=(
                f"No null key values found in {stats.role}"
                if status is ValidationStatus.PASS
                else f"Found {stats.null_rows} rows with null key values in {stats.role}"
            ),
            details=details,
        )

    # -- Exact bounded consistency --------------------------------------------
    def _validate_key_set_consistency(
        self, source_conn, target_conn, src_fqn: str, tgt_fqn: str,
        source_keys: List[str], target_keys: List[str], table_map,
        execution_config, key_config: KeyValidationExecutionConfig,
        source_stats: _SideStatistics, target_stats: _SideStatistics,
    ) -> CheckResult:
        started_at = datetime.now(timezone.utc).isoformat()
        started = time.perf_counter()
        if source_stats.error or target_stats.error:
            return self._consistency_error(
                "Combined key statistics are unavailable", started_at, started
            )
        if not self._both_support(source_conn, target_conn, "keyset_pagination"):
            return self._consistency_error(
                "Exact cross-system key consistency requires keyset_pagination on both connectors",
                started_at, started,
            )

        try:
            source_result, target_result = self._run_consistency_directions(
                source_conn, target_conn, src_fqn, tgt_fqn, source_keys, target_keys,
                table_map, execution_config, key_config,
            )
            elapsed = round(time.perf_counter() - started, 6)
            matched = source_result.total_keys - source_result.missing_keys
            status = (
                ValidationStatus.PASS
                if not source_result.missing_keys and not target_result.missing_keys
                else ValidationStatus.FAIL
            )
            return CheckResult(
                check_name="key_validation_consistency",
                status=status,
                source_value=source_result.total_keys,
                target_value=target_result.total_keys,
                delta=abs(source_result.total_keys - target_result.total_keys),
                message=(
                    f"Key sets match: distinct_keys={source_result.total_keys}"
                    if status is ValidationStatus.PASS
                    else "Key set mismatch: "
                    f"missing_in_target={source_result.missing_keys}, "
                    f"missing_in_source={target_result.missing_keys}"
                ),
                details={
                    **self._timing_details(
                        started_at, datetime.now(timezone.utc).isoformat(), elapsed
                    ),
                    "execution_strategy": ExecutionStrategy.KEYSET_BATCH.value,
                    "comparison_scope": "partitioned_full_key_set",
                    "full_scan": True,
                    "sampling_applied": False,
                    "partition_size": key_config.partition_size,
                    "configured_worker_count": key_config.max_workers,
                    "source_worker_count": source_result.worker_count,
                    "target_worker_count": target_result.worker_count,
                    "parallel_execution": key_config.parallel_execution,
                    "connection_reuse": key_config.reuse_connections,
                    "source_keys": source_keys,
                    "target_keys": target_keys,
                    "source_distinct_keys": source_stats.distinct_keys,
                    "target_distinct_keys": target_stats.distinct_keys,
                    "source_rows_scanned": source_stats.total_rows,
                    "target_rows_scanned": target_stats.total_rows,
                    "rows_scanned": source_stats.total_rows + target_stats.total_rows,
                    "source_distinct_keys_scanned": source_result.total_keys,
                    "target_distinct_keys_scanned": target_result.total_keys,
                    "missing_in_target_count": source_result.missing_keys,
                    "missing_in_source_count": target_result.missing_keys,
                    "matched_key_count": matched,
                    "additional_in_target_count": target_result.missing_keys,
                    "sample_missing_in_target": list(source_result.samples),
                    "sample_missing_in_source": list(target_result.samples),
                    "source_partitions": source_result.partitions,
                    "target_partitions": target_result.partitions,
                    "source_execution_time": source_result.elapsed,
                    "target_execution_time": target_result.elapsed,
                    **self._incremental_scope(table_map),
                },
            )
        except Exception as exc:
            return self._consistency_error(str(exc), started_at, started)

    def _run_consistency_directions(
        self, source_conn, target_conn, src_fqn: str, tgt_fqn: str,
        source_keys: List[str], target_keys: List[str], table_map,
        execution_config, key_config: KeyValidationExecutionConfig,
    ) -> Tuple[_PartitionResult, _PartitionResult]:
        source_call = lambda: self._probe_missing_keys_partitioned(
            source_conn, target_conn, src_fqn, tgt_fqn, source_keys, target_keys,
            table_map, execution_config, key_config, "source", "target",
        )
        target_call = lambda: self._probe_missing_keys_partitioned(
            target_conn, source_conn, tgt_fqn, src_fqn, target_keys, source_keys,
            table_map, execution_config, key_config, "target", "source",
        )
        if key_config.parallel_execution:
            return self._run_both_sides(execution_config, source_call, target_call)
        return source_call(), target_call()

    def _probe_missing_keys_partitioned(
        self, reading_conn, probing_conn, reading_fqn: str, probing_fqn: str,
        reading_keys: List[str], probing_keys: List[str], table_map,
        execution_config, key_config: KeyValidationExecutionConfig,
        reading_role=None, probing_role=None,
    ) -> _PartitionResult:
        started = time.perf_counter()
        reading_dialect = self._dialect(reading_conn)
        probing_dialect = self._dialect(probing_conn)
        where_clause = self._build_where_clause(
            table_map, reading_dialect, reading_role
        )
        probing_where_clause = self._build_where_clause(
            table_map, probing_dialect, probing_role
        )
        partition_size = key_config.partition_size
        total = missing = partitions = 0
        samples: List[Tuple[str, ...]] = []
        last_key: Optional[Tuple[Any, ...]] = None
        last_token: Optional[Tuple[str, ...]] = None
        comparison = self._comparison
        comparison._filter_batch_size = partition_size
        comparison._batch_config = getattr(execution_config, "batch_processing", None)
        comparison._batch_retry_count = 0
        # The probe cohort is rendered through the configured strategy, so a
        # partition becomes one seekable read instead of one predicate per key.
        reading_cohort = KeyCohortBuilder.resolve(
            self._dialect(reading_conn),
            self._capabilities(reading_conn),
            self._configured_cohort_strategy(execution_config),
            partition_size,
            comparison._sql_literal,
            cohort_config=getattr(execution_config, "cohort_processing", None),
        )
        probing_cohort = KeyCohortBuilder.resolve(
            self._dialect(probing_conn),
            self._capabilities(probing_conn),
            self._configured_cohort_strategy(execution_config),
            partition_size,
            comparison._sql_literal,
            cohort_config=getattr(execution_config, "cohort_processing", None),
        )
        query_session = getattr(reading_conn, "query_session", None)
        session_factory = (
            query_session
            if key_config.reuse_connections and callable(query_session)
            else lambda: contextlib.nullcontext(reading_conn)
        )

        probe_session = getattr(probing_conn, "query_session", None)
        probe_session_factory = (
            probe_session
            if key_config.reuse_connections and callable(probe_session)
            else lambda: contextlib.nullcontext(probing_conn)
        )

        with session_factory() as reader, probe_session_factory() as prober:
            while True:
                sql = self._distinct_key_page_sql(
                    reading_fqn, reading_keys, where_clause,
                    partition_size, last_key, reading_conn, reading_cohort,
                )
                page = comparison._execute_batch_query(reader, sql)
                if not isinstance(page, pd.DataFrame):
                    raise TypeError(
                        "Connector execute_query must return a pandas DataFrame"
                    )
                if page.empty:
                    break
                partitions += 1
                fetched = len(page)
                total += fetched
                key_values = comparison._extract_values(page, reading_keys)
                tokens = comparison._key_tokens(page, reading_keys)
                count, found_samples = self._probe_partition(
                    prober, probing_fqn, probing_keys, probing_where_clause,
                    key_values, tokens, comparison, probing_cohort, last_key,
                )
                missing += count
                if len(samples) < 20:
                    samples.extend(found_samples[: 20 - len(samples)])

                next_key = key_values[-1]
                next_token = tuple(
                    comparison._canonical_value(value) for value in next_key
                )
                if next_token == last_token:
                    raise RuntimeError(
                        "Key validation partition pagination did not advance"
                    )
                last_key = next_key
                last_token = next_token
                if fetched < partition_size:
                    break

        return _PartitionResult(
            total_keys=total,
            missing_keys=missing,
            samples=tuple(samples),
            partitions=partitions,
            elapsed=round(time.perf_counter() - started, 6),
            worker_count=1,
        )

    def _probe_partition(
        self, probing_conn, probing_fqn: str, probing_keys: List[str],
        where_clause: str, key_values: List[Tuple[Any, ...]],
        tokens: List[Tuple[str, ...]], comparison: RecordComparisonValidator,
        cohort: KeyCohortBuilder, lower_bound: Optional[Sequence[Any]] = None,
    ) -> Tuple[int, List[Tuple[str, ...]]]:
        plan = cohort.plan(
            probing_fqn,
            list(probing_keys),
            where_clause,
            probing_keys,
            key_values,
            lower_bound=lower_bound,
            upper_bound=key_values[-1] if key_values else None,
        )
        # Only membership of the probed keys matters, so stream the cohort
        # queries rather than materialising one combined frame.
        found = comparison._collect_key_tokens(
            probing_conn, list(plan.queries), probing_keys, with_retry=True
        )
        missing = [token for token in tokens if token not in found]
        return len(missing), missing[:20]

    @staticmethod
    def _configured_cohort_strategy(execution_config) -> Optional[str]:
        """The cohort strategy configured for key validation, if any."""
        return getattr(
            getattr(execution_config, "key_validation", None),
            "key_cohort_strategy",
            None,
        )

    def _distinct_key_page_sql(
        self, fqn: str, keys: Sequence[str], where_clause: str,
        partition_size: int, last_values: Optional[Sequence[Any]],
        connection=None, cohort: Optional[KeyCohortBuilder] = None,
    ) -> str:
        effective_where = where_clause
        if last_values is not None:
            predicate = (
                cohort.greater_than(keys, last_values) if cohort is not None
                else self._comparison._keyset_predicate(keys, last_values)
            )
            effective_where = combine_where(where_clause, predicate)
        dialect = self._dialect(connection)
        key_list = ", ".join(dialect.quote(key) for key in keys)
        # ORDER BY is required for correct keyset pagination. No other sorting
        # is performed by key validation.
        return dialect.select(
            key_list,
            fqn,
            effective_where,
            order_by=key_list,
            limit=partition_size,
            distinct=True,
        )

    @staticmethod
    def _effective_partition_workers(
        execution_config, key_config: KeyValidationExecutionConfig,
    ) -> int:
        configured = key_config.max_workers if key_config.parallel_execution else 1
        memory_limit = int(
            getattr(getattr(execution_config, "batch_processing", None),
                    "max_rows_in_memory", 0) or 0
        )
        if memory_limit:
            partitions_per_direction = memory_limit // (2 * key_config.partition_size)
            # Each worker owns an input partition and a bounded probe result;
            # one additional partition may wait in the producer queue.
            memory_workers = max((partitions_per_direction - 1) // 2, 1)
            configured = min(configured, memory_workers)
        return max(configured, 1)

    def _consistency_error(
        self, message: str, started_at: str, started: float,
    ) -> CheckResult:
        elapsed = round(time.perf_counter() - started, 6)
        return CheckResult(
            check_name="key_validation_consistency",
            status=ValidationStatus.ERROR,
            message=f"Key set consistency validation failed: {message}",
            details={
                **self._timing_details(
                    started_at, datetime.now(timezone.utc).isoformat(), elapsed
                ),
                "full_scan": True,
                "sampling_applied": False,
            },
        )

    @staticmethod
    def _timing_details(
        started_at: str, completed_at: str, elapsed: float,
    ) -> Dict[str, Any]:
        return {
            "start_time": started_at,
            "end_time": completed_at,
            "execution_time_seconds": elapsed,
        }

    @staticmethod
    def _key_config(execution_config) -> KeyValidationExecutionConfig:
        configured = getattr(execution_config, "key_validation", None)
        if isinstance(configured, KeyValidationExecutionConfig):
            return configured
        raise ValueError(
            "Missing performance.key_validation configuration; key validation "
            "does not use hardcoded runtime defaults"
        )
