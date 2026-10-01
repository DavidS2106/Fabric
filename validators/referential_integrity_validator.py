from __future__ import annotations

from typing import List, Tuple

from core.models import CheckResult, ValidationStatus
from core.execution_engine import ExecutionContext
from .base_validator import BaseValidator
from .validator_utils import _fqn


class ReferentialIntegrityValidator(BaseValidator):
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
        checks = getattr(table_map, "referential_checks", []) or []

        if not checks:
            return [
                CheckResult(
                    check_name="referential_integrity",
                    status=ValidationStatus.SKIP,
                    message="Skipped referential integrity: no referential_checks configured",
                )
            ]

        dialect = self._dialect(target_conn)
        child_fqn = _fqn(
            table_map.target_schema, table_map.target_table, dialect
        )

        for ref in checks:
            child_col = ref.get("child_column")
            parent_table = ref.get("parent_table")
            parent_col = ref.get("parent_column", child_col)
            allow_nulls = ref.get("allow_nulls", True)
            sample_limit = ref.get("sample_limit", 20)

            check_name = self._build_check_name(child_col, parent_table)

            if not child_col or not parent_table or not parent_col:
                results.append(
                    CheckResult(
                        check_name=check_name,
                        status=ValidationStatus.ERROR,
                        message=(
                            "Invalid referential check config. "
                            "Required fields: child_column, parent_table, parent_column"
                        ),
                        details={"config": ref},
                    )
                )
                continue

            try:
                orphan_count, count_sql = self._get_orphan_count(
                    target_conn=target_conn,
                    child_fqn=child_fqn,
                    child_col=child_col,
                    parent_table=parent_table,
                    parent_col=parent_col,
                    allow_nulls=allow_nulls,
                    table_map=table_map,
                )

                sample_keys, sample_sql = [], ""
                if orphan_count:
                    sample_keys, sample_sql = self._get_sample_orphans(
                        target_conn=target_conn,
                        child_fqn=child_fqn,
                        child_col=child_col,
                        parent_table=parent_table,
                        parent_col=parent_col,
                        allow_nulls=allow_nulls,
                        table_map=table_map,
                        sample_limit=sample_limit,
                    )

                status = (
                    ValidationStatus.PASS
                    if orphan_count == 0
                    else ValidationStatus.FAIL
                )

                results.append(
                    CheckResult(
                        check_name=check_name,
                        status=status,
                        source_value=parent_table,
                        target_value=orphan_count,
                        delta=orphan_count,
                        message=(
                            f"No orphan records found for {child_col} -> {parent_table}.{parent_col}"
                            if status == ValidationStatus.PASS
                            else (
                                f"Found {orphan_count} orphan records for "
                                f"{child_col} -> {parent_table}.{parent_col}"
                            )
                        ),
                        details={
                            "child_table": child_fqn,
                            "child_column": child_col,
                            "parent_table": parent_table,
                            "parent_column": parent_col,
                            "allow_nulls": allow_nulls,
                            "sample_orphan_keys": sample_keys,
                            "count_sql": count_sql,
                            "sample_sql": sample_sql,
                        },
                    )
                )

            except Exception as e:
                results.append(
                    CheckResult(
                        check_name=check_name,
                        status=ValidationStatus.ERROR,
                        message=f"Referential integrity check failed: {e}",
                        details={"config": ref},
                    )
                )

        return results

    def _build_check_name(self, child_col: str, parent_table: str) -> str:
        if not child_col or not parent_table:
            return "referential_integrity"

        safe_parent = parent_table.replace(".", "_").replace("[", "").replace("]", "")
        return f"referential_{child_col}_to_{safe_parent}"

    def _get_orphan_count(
        self,
        target_conn,
        child_fqn: str,
        child_col: str,
        parent_table: str,
        parent_col: str,
        allow_nulls: bool,
        table_map,
    ) -> Tuple[int, str]:
        dialect = self._dialect(target_conn)
        child_filter = self._build_child_filter(
            child_col, allow_nulls, table_map, dialect
        )
        child_ref = dialect.column(child_col, "c")
        parent_ref = dialect.column(parent_col, "p")
        parent_fqn = dialect.qualify_path(parent_table)

        sql = (
            f"SELECT COUNT(*) AS orphan_count "
            f"FROM {child_fqn} c "
            f"WHERE {child_filter} "
            f"AND NOT EXISTS ("
            f"SELECT 1 FROM {parent_fqn} p "
            f"WHERE {parent_ref} = {child_ref}"
            f")"
        )

        df = target_conn.execute_query(sql)
        return int(df.iloc[0, 0]), sql

    
    def _get_sample_orphans(
        self,
        target_conn,
        child_fqn: str,
        child_col: str,
        parent_table: str,
        parent_col: str,
        allow_nulls: bool,
        table_map,
        sample_limit: int,
    ) -> Tuple[List, str]:
        dialect = self._dialect(target_conn)
        child_filter = self._build_child_filter(
            child_col, allow_nulls, table_map, dialect
        )
        child_ref = dialect.column(child_col, "c")
        parent_ref = dialect.column(parent_col, "p")
        parent_fqn = dialect.qualify_path(parent_table)

        limit = sample_limit if sample_limit and sample_limit > 0 else None
        # The sample is bounded by the connector's own row-limit clause when it
        # has one, so a widely violated constraint never returns every orphan.
        top_clause = (
            f"TOP {limit} "
            if limit and self._capabilities(target_conn).supports("row_limit_clause")
            else ""
        )
        sql = (
            f"SELECT DISTINCT {top_clause}{child_ref} "
            f"FROM {child_fqn} c "
            f"WHERE {child_filter} "
            f"AND NOT EXISTS ("
            f"SELECT 1 FROM {parent_fqn} p "
            f"WHERE {parent_ref} = {child_ref}"
            f")"
        )

        df = target_conn.execute_query(sql)
        values = df.iloc[:, 0].tolist()
        return (values[:limit] if limit else values), sql

    def _build_child_filter(
        self,
        child_col: str,
        allow_nulls: bool,
        table_map,
        dialect,
    ) -> str:
        filters = []

        if allow_nulls:
            filters.append(f"{dialect.column(child_col, 'c')} IS NOT NULL")
        else:
            child_ref = dialect.column(child_col, "c")
            filters.append(f"{child_ref} IS NULL OR {child_ref} IS NOT NULL")

        base_where = self._build_where_clause(table_map, dialect)

        if base_where:
            child_scoped_where = base_where.replace(" WHERE ", "", 1)
            for column in (
                getattr(table_map, "date_filter_column", None),
                getattr(table_map, "incremental_column", None),
            ):
                if column:
                    quoted = dialect.quote(column)
                    child_scoped_where = child_scoped_where.replace(
                        quoted, f"c.{quoted}"
                    )
            filters.append(child_scoped_where)

        return " AND ".join(filters)
