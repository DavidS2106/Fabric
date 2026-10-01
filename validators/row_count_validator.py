from __future__ import annotations
import time
from typing import List

from core.models import CheckResult, ValidationStatus
from core.execution_engine import ExecutionContext
from core.scan_planner import ROW_COUNT_KEY
from .base_validator import BaseValidator
from .validator_utils import _safe_pct_delta, _fqn


class RowCountValidator(BaseValidator):
    """Compare record counts, pushed down to both configured engines.

    The count is a statistic like any other, so when the run's shared
    statistics scan already produced it this validation issues no query of its
    own. Its result, tolerance and status are unchanged either way.
    """

    @staticmethod
    def statistic_requests(table_map=None):
        """The statistics this check needs from the shared scan."""
        from core.scan_planner import StatisticRequest

        return [
            StatisticRequest(
                key=ROW_COUNT_KEY,
                kind="row_count",
                requested_by=("row_count",),
            )
        ]

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
        source_filter_clause = self._build_where_clause(
            table_map, source_dialect, "source"
        )
        target_filter_clause = self._build_where_clause(
            table_map, target_dialect, "target"
        )

        src_sql = source_dialect.select(
            f"{source_dialect.count_all()} AS cnt", src_fqn, source_filter_clause
        )
        tgt_sql = target_dialect.select(
            f"{target_dialect.count_all()} AS cnt", tgt_fqn, target_filter_clause
        )
        tolerance = thresholds.row_count_tolerance_pct

        details = {
            "source_sql": src_sql,
            "target_sql": tgt_sql,
            "source_filter_clause": source_filter_clause,
            "target_filter_clause": target_filter_clause,
            "filter_clause": source_filter_clause,
            "tolerance_pct": tolerance,
            "execution_strategy": "pushdown",
            **self._incremental_scope(table_map),
        }
        side_times = {}

        try:
            shared = self._shared_counts(execution_cache, details)
            if shared is not None:
                src_count, tgt_count = shared
            else:
                def count(role, connection, sql):
                    def compute():
                        started = time.perf_counter()
                        value = int(connection.execute_query(sql).iloc[0, 0])
                        side_times[role] = round(time.perf_counter() - started, 6)
                        return value

                    return self._row_count_once(
                        execution_cache, role, compute, consumer="row_count"
                    )

                src_count, tgt_count = self._run_both_sides(
                    execution_config,
                    lambda: count("source", source_conn, src_sql),
                    lambda: count("target", target_conn, tgt_sql),
                )

            details.update(
                source_execution_time=side_times.get("source"),
                target_execution_time=side_times.get("target"),
                rows_processed=max(src_count, tgt_count),
            )
            delta = abs(src_count - tgt_count)
            delta_pct = _safe_pct_delta(src_count, tgt_count)

            if src_count == 0 or tgt_count == 0:
                status = (
                    ValidationStatus.PASS
                    if src_count == tgt_count
                    else ValidationStatus.FAIL
                )
            else:
                status = (
                    ValidationStatus.PASS
                    if delta_pct <= tolerance
                    else ValidationStatus.FAIL
                )

            if status == ValidationStatus.PASS:
                message = (
                    f"Row counts are within tolerance: "
                    f"source={src_count:,}, target={tgt_count:,}, "
                    f"delta={delta:,} ({delta_pct:.4f}%), "
                    f"tolerance={tolerance:.4f}%"
                )
            else:
                message = (
                    f"Row count mismatch: "
                    f"source={src_count:,}, target={tgt_count:,}, "
                    f"delta={delta:,} ({delta_pct:.4f}%), "
                    f"tolerance={tolerance:.4f}%"
                )

            return [
                CheckResult(
                    check_name="row_count",
                    status=status,
                    source_value=src_count,
                    target_value=tgt_count,
                    delta=delta,
                    delta_pct=round(delta_pct, 4),
                    message=message,
                    details=details,
                )
            ]

        except Exception as e:
            return [
                CheckResult(
                    check_name="row_count",
                    status=ValidationStatus.ERROR,
                    message=f"Row count check failed: {e}",
                    details=details,
                )
            ]

    def _shared_counts(self, execution_cache, details):
        """Read both counts from the shared statistics scan, when available."""
        provider = self._shared_statistics(execution_cache)
        if provider is None or not provider.provides(ROW_COUNT_KEY):
            return None
        try:
            source = provider.integer("source", ROW_COUNT_KEY)
            target = provider.integer("target", ROW_COUNT_KEY)
        except Exception:
            # A failed shared scan must not hide this check: fall back to the
            # validation's own query.
            return None
        details.update(
            execution_strategy="shared_statistics_scan",
            shared_scan_used=True,
            source_sql=provider.statement("source"),
            target_sql=provider.statement("target"),
            source_execution_time=provider.execution("source").elapsed,
            target_execution_time=provider.execution("target").elapsed,
        )
        return source, target
