from __future__ import annotations

from typing import List, Tuple

from core.models import CheckResult, ValidationStatus
from core.execution_engine import ExecutionContext
from .base_validator import BaseValidator
from .validator_utils import _fqn


class SCDValidator(BaseValidator):
    def validate(
        self,
        source_conn,
        target_conn,
        table_map,
        thresholds,
        context: ExecutionContext = None,
        source_type: str = "generic",
    ) -> List[CheckResult]:
        results = []
        scd = getattr(table_map, "scd_config", None)

        if not scd:
            return [
                CheckResult(
                    check_name="scd_validation",
                    status=ValidationStatus.SKIP,
                    message="Skipped SCD validation: no scd_config configured",
                )
            ]

        natural_keys = scd.get("natural_key", [])
        effective_col = scd.get("effective_date_col", "effective_date")
        end_col = scd.get("end_date_col", "end_date")
        open_end_sentinel = scd.get("open_end_sentinel")
        require_current_record = scd.get("require_current_record", True)

        if not natural_keys:
            return [
                CheckResult(
                    check_name="scd_validation",
                    status=ValidationStatus.ERROR,
                    message="Invalid SCD config: natural_key is required",
                    details={"config": scd},
                )
            ]

        tgt_fqn = _fqn(
            table_map.target_schema,
            table_map.target_table,
            self._dialect(target_conn),
        )

        checks = [
            self._check_duplicate_open_records(
                target_conn,
                tgt_fqn,
                natural_keys,
                end_col,
                open_end_sentinel,
                table_map,
            ),
            self._check_invalid_date_ranges(
                target_conn,
                tgt_fqn,
                effective_col,
                end_col,
                open_end_sentinel,
                table_map,
            ),
            self._check_overlapping_ranges(
                target_conn,
                tgt_fqn,
                natural_keys,
                effective_col,
                end_col,
                open_end_sentinel,
                table_map,
            ),
        ]

        if require_current_record:
            checks.append(
                self._check_missing_current_records(
                    target_conn,
                    tgt_fqn,
                    natural_keys,
                    end_col,
                    open_end_sentinel,
                    table_map,
                )
            )

        for check_name, violation_count, sql, message_base in checks:
            status = (
                ValidationStatus.PASS
                if violation_count == 0
                else ValidationStatus.FAIL
            )

            results.append(
                CheckResult(
                    check_name=check_name,
                    status=status,
                    target_value=violation_count,
                    delta=violation_count,
                    message=(
                        f"{message_base}: no violations found"
                        if status == ValidationStatus.PASS
                        else f"{message_base}: violations={violation_count}"
                    ),
                    details={
                        "target_table": tgt_fqn,
                        "natural_keys": natural_keys,
                        "effective_date_col": effective_col,
                        "end_date_col": end_col,
                        "open_end_sentinel": open_end_sentinel,
                        "sql": sql,
                    },
                )
            )

        return results

    def _open_record_condition(
        self,
        end_col: str,
        open_end_sentinel,
        dialect,
    ) -> str:
        end_col = dialect.quote(end_col)
        if open_end_sentinel:
            return f"{end_col} = '{open_end_sentinel}'"

        return f"{end_col} IS NULL"

    def _closed_end_expression(
        self,
        end_col: str,
        open_end_sentinel,
        dialect,
    ) -> str:
        end_col = dialect.quote(end_col)
        if open_end_sentinel:
            return f"NULLIF({end_col}, '{open_end_sentinel}')"

        return end_col

    def _scoped_where(
        self,
        table_map,
        dialect,
        alias: str = "",
    ) -> str:
        base_where = self._build_where_clause(table_map, dialect)

        if not base_where:
            return ""

        condition = base_where.replace(" WHERE ", "", 1)

        if alias:
            # Basic aliasing for simple column filters.
            condition = condition.replace(" BETWEEN ", " BETWEEN ")
            parts = condition.split(" AND ")
            aliased_parts = []
            for part in parts:
                stripped = part.strip()
                if "." not in stripped:
                    first_token = stripped.split()[0]
                    stripped = stripped.replace(first_token, f"{alias}.{first_token}", 1)
                aliased_parts.append(stripped)
            condition = " AND ".join(aliased_parts)

        return condition

    def _check_duplicate_open_records(
        self,
        target_conn,
        tgt_fqn: str,
        natural_keys: List[str],
        end_col: str,
        open_end_sentinel,
        table_map,
    ) -> Tuple[str, int, str, str]:
        dialect = self._dialect(target_conn)
        key_list = ", ".join(dialect.quote(key) for key in natural_keys)
        open_condition = self._open_record_condition(
            end_col, open_end_sentinel, dialect
        )

        filters = [open_condition]
        scoped_filter = self._scoped_where(table_map, dialect)

        if scoped_filter:
            filters.append(scoped_filter)

        where_clause = " AND ".join(filters)

        sql = (
            f"SELECT COUNT(*) FROM ("
            f"SELECT {key_list} "
            f"FROM {tgt_fqn} "
            f"WHERE {where_clause} "
            f"GROUP BY {key_list} "
            f"HAVING COUNT(*) > 1"
            f") d"
        )

        df = target_conn.execute_query(sql)
        return (
            "scd_duplicate_open_records",
            int(df.iloc[0, 0]),
            sql,
            "Duplicate open/current SCD records",
        )

    def _check_invalid_date_ranges(
        self,
        target_conn,
        tgt_fqn: str,
        effective_col: str,
        end_col: str,
        open_end_sentinel,
        table_map,
    ) -> Tuple[str, int, str, str]:
        dialect = self._dialect(target_conn)
        closed_end_expr = self._closed_end_expression(
            end_col, open_end_sentinel, dialect
        )
        effective_col = dialect.quote(effective_col)

        filters = [
            f"{closed_end_expr} IS NOT NULL",
            f"{effective_col} > {closed_end_expr}",
        ]

        scoped_filter = self._scoped_where(table_map, dialect)
        if scoped_filter:
            filters.append(scoped_filter)

        sql = (
            f"SELECT COUNT(*) "
            f"FROM {tgt_fqn} "
            f"WHERE {' AND '.join(filters)}"
        )

        df = target_conn.execute_query(sql)
        return (
            "scd_invalid_date_ranges",
            int(df.iloc[0, 0]),
            sql,
            "Invalid SCD date ranges",
        )

    def _check_overlapping_ranges(
        self,
        target_conn,
        tgt_fqn: str,
        natural_keys: List[str],
        effective_col: str,
        end_col: str,
        open_end_sentinel,
        table_map,
    ) -> Tuple[str, int, str, str]:
        dialect = self._dialect(target_conn)
        partition_by = ", ".join(dialect.quote(key) for key in natural_keys)
        key_join = " AND ".join(
            f"{dialect.column(key, 'a')} = {dialect.column(key, 'b')}"
            for key in natural_keys
        )
        a_effective = dialect.column(effective_col, "a")
        b_effective = dialect.column(effective_col, "b")
        quoted_effective = dialect.quote(effective_col)
        quoted_end = dialect.quote(end_col)

        a_end = (
            f"COALESCE(NULLIF({dialect.column(end_col, 'a')}, '{open_end_sentinel}'), '9999-12-31')"
            if open_end_sentinel
            else f"COALESCE({dialect.column(end_col, 'a')}, '9999-12-31')"
        )
        b_end = (
            f"COALESCE(NULLIF({dialect.column(end_col, 'b')}, '{open_end_sentinel}'), '9999-12-31')"
            if open_end_sentinel
            else f"COALESCE({dialect.column(end_col, 'b')}, '9999-12-31')"
        )

        filters = [
            key_join,
            "a._scd_row_num < b._scd_row_num",
            f"{a_effective} <= {b_end}",
            f"{b_effective} <= {a_end}",
        ]

        scoped_filter_a = self._scoped_where(table_map, dialect, alias="a")
        if scoped_filter_a:
            filters.append(scoped_filter_a)

        sql = (
            f"WITH scoped_scd AS ("
            f"SELECT *, ROW_NUMBER() OVER ("
            f"PARTITION BY {partition_by} "
            f"ORDER BY {quoted_effective}, {quoted_end}"
            f") AS _scd_row_num "
            f"FROM {tgt_fqn}"
            f") "
            f"SELECT COUNT(*) FROM ("
            f"SELECT DISTINCT {', '.join(dialect.column(k, 'a') for k in natural_keys)} "
            f"FROM scoped_scd a "
            f"JOIN scoped_scd b "
            f"ON {' AND '.join(filters)}"
            f") d"
        )

        df = target_conn.execute_query(sql)
        return (
            "scd_overlapping_date_ranges",
            int(df.iloc[0, 0]),
            sql,
            "Overlapping SCD date ranges",
        )

    def _check_missing_current_records(
        self,
        target_conn,
        tgt_fqn: str,
        natural_keys: List[str],
        end_col: str,
        open_end_sentinel,
        table_map,
    ) -> Tuple[str, int, str, str]:
        dialect = self._dialect(target_conn)
        key_list = ", ".join(dialect.quote(key) for key in natural_keys)
        open_condition = self._open_record_condition(
            end_col, open_end_sentinel, dialect
        )

        scoped_filter = self._scoped_where(table_map, dialect)
        scoped_where = f"WHERE {scoped_filter}" if scoped_filter else ""

        sql = (
            f"SELECT COUNT(*) FROM ("
            f"SELECT {key_list} "
            f"FROM {tgt_fqn} "
            f"{scoped_where} "
            f"GROUP BY {key_list} "
            f"HAVING SUM(CASE WHEN {open_condition} THEN 1 ELSE 0 END) = 0"
            f") d"
        )

        df = target_conn.execute_query(sql)
        return (
            "scd_missing_current_records",
            int(df.iloc[0, 0]),
            sql,
            "Missing open/current SCD records",
        )
