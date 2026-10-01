from __future__ import annotations

from typing import Dict, List, Tuple

from core.models import CheckResult, ValidationStatus
from core.execution_engine import ExecutionContext
from .base_validator import BaseValidator
from .validator_utils import _fqn


class BusinessRuleValidator(BaseValidator):
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
        rules = getattr(table_map, "business_rules", []) or []

        if not rules:
            return [
                CheckResult(
                    check_name="business_rules",
                    status=ValidationStatus.SKIP,
                    message="Skipped business rules: no business_rules configured",
                )
            ]

        dialect = self._dialect(target_conn)
        tgt_fqn = _fqn(
            table_map.target_schema, table_map.target_table, dialect
        )
        target_keys = [m.target_column for m in table_map.key_mappings]

        for rule in rules:
            name = rule.get("name")
            assertion = rule.get("assertion")
            description = rule.get("description", "")
            severity = str(rule.get("severity", "fail")).strip().lower()
            sample_limit = rule.get("sample_limit", 20)
            check_name = f"rule_{name}" if name else "business_rule"

            if not name or not assertion:
                results.append(
                    CheckResult(
                        check_name=check_name,
                        status=ValidationStatus.ERROR,
                        message="Invalid business rule config. Required fields: name, assertion",
                        details={"config": rule},
                    )
                )
                continue

            try:
                violation_count, count_sql = self._get_violation_count(
                    target_conn=target_conn,
                    target_fqn=tgt_fqn,
                    assertion=assertion,
                    table_map=table_map,
                )

                sample_keys, sample_sql = [], ""
                if violation_count:
                    sample_keys, sample_sql = self._get_sample_violations(
                        target_conn=target_conn,
                        target_fqn=tgt_fqn,
                        assertion=assertion,
                        target_keys=target_keys,
                        table_map=table_map,
                        sample_limit=sample_limit,
                    )

                if violation_count == 0:
                    status = ValidationStatus.PASS
                elif severity in {"warn", "warning"}:
                    status = ValidationStatus.WARN
                else:
                    status = ValidationStatus.FAIL

                results.append(
                    CheckResult(
                        check_name=check_name,
                        status=status,
                        target_value=violation_count,
                        delta=violation_count,
                        message=(
                            f"Business rule passed: {name}"
                            if status == ValidationStatus.PASS
                            else f"Business rule failed: {name}. Violations={violation_count}"
                        ),
                        details={
                            "rule_name": name,
                            "description": description,
                            "severity": severity,
                            "assertion": assertion,
                            "target_table": tgt_fqn,
                            "target_keys": target_keys,
                            "sample_violating_keys": sample_keys,
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
                        message=f"Business rule check failed for {name}: {e}",
                        details={"config": rule},
                    )
                )

        return results

    def _get_violation_count(
        self,
        target_conn,
        target_fqn: str,
        assertion: str,
        table_map,
    ) -> Tuple[int, str]:
        dialect = self._dialect(target_conn)
        where_clause = self._build_rule_where_clause(
            assertion, table_map, dialect
        )
        sql = dialect.select(
            f"{dialect.count_all()} AS violation_count",
            target_fqn,
            where_clause,
        )
        df = target_conn.execute_query(sql)
        return int(df.iloc[0, 0]), sql

    def _get_sample_violations(
        self,
        target_conn,
        target_fqn: str,
        assertion: str,
        target_keys: List[str],
        table_map,
        sample_limit: int,
    ) -> Tuple[List[Dict], str]:
        if not target_keys:
            return [], ""

        dialect = self._dialect(target_conn)
        key_list = ", ".join(dialect.quote(key) for key in target_keys)
        where_clause = self._build_rule_where_clause(
            assertion, table_map, dialect
        )
        # The sample limit is pushed down so a rule violated by millions of rows
        # never materializes more than the configured sample.
        limit = sample_limit if isinstance(sample_limit, int) and sample_limit > 0 else None
        sql = dialect.select(
            key_list,
            target_fqn,
            where_clause,
            limit=limit,
            distinct=True,
        )
        df = target_conn.execute_query(sql)

        rows = df.head(limit).to_dict(orient="records") if limit else df.to_dict(orient="records")
        return rows, sql

    def _build_rule_where_clause(self, assertion: str, table_map, dialect) -> str:
        conditions = []

        base_where = self._build_where_clause(table_map, dialect)
        if base_where:
            conditions.append(base_where.replace(" WHERE ", "", 1))

        conditions.append(f"NOT ({assertion})")

        return " WHERE " + " AND ".join(conditions)
