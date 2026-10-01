from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from core.models import CheckResult, ValidationStatus
from core.scan_planner import (
    ROW_COUNT_KEY,
    StatisticRequest,
    audit_null_count_key,
)
from .base_validator import BaseValidator
from .validator_utils import _fqn


class AuditValidator(BaseValidator):
    """Validate configurable target audit columns and count reconciliation."""

    SUPPORTED_CHECKS = frozenset({
        "audit_column_population_check",
        "audit_count_reconciliation_check",
    })

    @staticmethod
    def statistic_requests(
        table_map=None, validations_config: Optional[Dict[str, Any]] = None
    ) -> List[StatisticRequest]:
        """The statistics this check needs from the shared scan.

        The reconciliation counts are the same record counts every other check
        uses, so they join the shared scan on both sides. Audit columns are
        target-only by configured scope, so their NULL counts are requested for
        the target side alone; asking the source for a column it does not have
        would fail that side's whole scan.
        """
        config = (validations_config or {}).get("audit_validation") or {}
        if not isinstance(config, dict):
            return []
        # Same gate ``validate`` applies: a disabled audit block runs nothing,
        # so it must not add audit-only statistics to the shared scan either.
        if not config.get("enabled", False):
            return []
        requests = [
            StatisticRequest(
                key=ROW_COUNT_KEY,
                kind="row_count",
                requested_by=("audit_validation",),
            )
        ]
        scope = config.get("scope") or {}
        if not isinstance(scope, dict):
            return requests
        if scope.get("audit_columns", "target_only") != "target_only":
            return requests
        for column in config.get("audit_columns") or []:
            if not isinstance(column, str) or not column.strip():
                continue
            name = column.strip()
            requests.append(StatisticRequest(
                key=audit_null_count_key(name),
                kind="null_count",
                target_column=name,
                requested_by=("audit_validation",),
                roles=("target",),
            ))
        return requests

    def validate(
        self,
        source_conn,
        target_conn,
        table_map,
        validations_config: Dict[str, Any],
        source_type: str = "generic",
        validation_profile: Optional[str] = None,
        execution_config=None,
        execution_cache=None,
    ) -> List[CheckResult]:
        started = time.perf_counter()
        config = (validations_config or {}).get("audit_validation", {}) or {}
        if not isinstance(config, dict):
            return [self._error("audit_validation must be a mapping", started)]
        if not config.get("enabled", False):
            return []

        checks = config.get("checks", []) or []
        if not isinstance(checks, list):
            return [self._error("audit_validation.checks must be a list", started)]
        enabled = []
        seen = set()
        for index, item in enumerate(checks):
            if not isinstance(item, dict):
                return [self._error(
                    f"audit_validation.checks[{index}] must be a mapping", started
                )]
            name = item.get("name")
            if not isinstance(name, str) or not name.strip():
                return [self._error(
                    f"audit_validation.checks[{index}].name is required", started
                )]
            name = name.strip()
            if name in seen:
                return [self._error(f"Duplicate audit check '{name}'", started, name)]
            seen.add(name)
            flag = item.get("enabled", True)
            if not isinstance(flag, bool):
                return [self._error(
                    f"Audit check '{name}' enabled must be boolean", started, name
                )]
            if not flag:
                continue
            if name not in self.SUPPORTED_CHECKS:
                return [self._error(f"Unsupported audit check '{name}'", started, name)]
            enabled.append(name)
        if not enabled:
            return []

        audit_columns = config.get("audit_columns", []) or []
        if not isinstance(audit_columns, list) or not all(
            isinstance(column, str) and column.strip() for column in audit_columns
        ):
            return [self._error(
                "audit_validation.audit_columns must be a list of non-empty strings",
                started,
            )]
        audit_columns = [column.strip() for column in audit_columns]
        if len(set(audit_columns)) != len(audit_columns):
            return [self._error(
                "audit_validation.audit_columns must not contain duplicates", started
            )]

        scope = config.get("scope", {}) or {}
        if not isinstance(scope, dict):
            return [self._error("audit_validation.scope must be a mapping", started)]
        if scope.get("audit_columns", "target_only") != "target_only":
            return [self._error(
                "audit_validation.scope.audit_columns must be target_only", started
            )]
        if scope.get("count_reconciliation", "source_to_target") != "source_to_target":
            return [self._error(
                "audit_validation.scope.count_reconciliation must be source_to_target",
                started,
            )]

        source_dialect = self._dialect(source_conn)
        target_dialect = self._dialect(target_conn)
        source_fqn = _fqn(
            table_map.source_schema, table_map.source_table, source_dialect
        )
        target_fqn = _fqn(
            table_map.target_schema, table_map.target_table, target_dialect
        )
        source_where = self._build_where_clause(table_map, source_dialect, "source")
        target_where = self._build_where_clause(table_map, target_dialect, "target")
        source_count_sql = source_dialect.select(
            f"{source_dialect.count_all()} AS record_count", source_fqn, source_where
        )
        target_count_sql = target_dialect.select(
            f"{target_dialect.count_all()} AS record_count", target_fqn, target_where
        )
        provider = self._shared_statistics(execution_cache)
        try:
            shared = self._shared_counts(provider)
            if shared is not None:
                source_count, target_count = shared
                source_count_sql = provider.statement("source") or source_count_sql
                target_count_sql = provider.statement("target") or target_count_sql
            else:
                def count(role, connection, sql):
                    compute = lambda: int(connection.execute_query(sql).iloc[0, 0])
                    return self._row_count_once(
                        execution_cache, role, compute, consumer="audit_validation"
                    )

                source_count = count("source", source_conn, source_count_sql)
                target_count = count("target", target_conn, target_count_sql)
        except Exception as exc:
            return [
                self._error(f"Audit count query failed: {exc}", started, name)
                for name in enabled
            ]

        results = []
        for name in enabled:
            if name == "audit_column_population_check":
                results.append(self._population_result(
                    target_conn,
                    target_fqn,
                    target_where,
                    audit_columns,
                    source_count,
                    target_count,
                    table_map,
                    validation_profile,
                    started,
                    provider,
                ))
            else:
                matched = source_count == target_count
                reason = "" if matched else (
                    "Source and target record counts do not reconcile"
                )
                results.append(self._result(
                    name,
                    ValidationStatus.PASS if matched else ValidationStatus.FAIL,
                    audit_columns,
                    source_count,
                    target_count,
                    reason,
                    validation_profile,
                    started,
                    "Source and target record counts reconcile" if matched else reason,
                ))
        return results

    def _population_result(
        self, target_conn, target_fqn, where_clause, audit_columns,
        source_count, target_count, table_map, validation_profile, started,
        provider=None,
    ) -> CheckResult:
        name = "audit_column_population_check"
        dialect = self._dialect(target_conn)
        try:
            actual_by_casefold = self._target_columns(
                target_conn,
                table_map.target_schema,
                table_map.target_table,
                target_fqn,
                dialect,
            )
            missing = [
                column for column in audit_columns
                if column.casefold() not in actual_by_casefold
            ]
            null_counts = {}
            shared_used = False
            if not missing and audit_columns:
                shared = self._shared_null_counts(provider, audit_columns)
                if shared is not None:
                    null_counts = shared
                    shared_used = True
                else:
                    expressions = [
                        f"SUM(CASE WHEN {dialect.quote(column)} IS NULL THEN 1 "
                        f"ELSE 0 END) AS null_{index}"
                        for index, column in enumerate(audit_columns)
                    ]
                    null_frame = target_conn.execute_query(
                        dialect.select(
                            ", ".join(expressions), target_fqn, where_clause
                        )
                    )
                    null_counts = {
                        column: int(null_frame.iloc[0, index] or 0)
                        for index, column in enumerate(audit_columns)
                    }
            null_columns = [
                column for column, count in null_counts.items() if count > 0
            ]
            passed = not missing and not null_columns
            reasons = []
            if missing:
                reasons.append("Missing target audit columns: " + ", ".join(missing))
            if null_columns:
                reasons.append("NULL values found in target audit columns: " + ", ".join(null_columns))
            reason = "; ".join(reasons)
            return self._result(
                name,
                ValidationStatus.PASS if passed else ValidationStatus.FAIL,
                audit_columns,
                source_count,
                target_count,
                reason,
                validation_profile,
                started,
                "All configured target audit columns exist and are populated" if passed else reason,
                {
                    "missing_audit_columns": missing,
                    "audit_column_null_counts": null_counts,
                    "shared_scan_used": shared_used,
                },
            )
        except Exception as exc:
            return self._error(
                f"Audit column population check failed: {exc}", started, name,
                audit_columns, source_count, target_count, validation_profile,
            )

    # ── Shared statistics scan ────────────────────────────────
    @staticmethod
    def _shared_counts(provider) -> Optional[Tuple[int, int]]:
        """Read both record counts from the shared statistics scan."""
        if provider is None or not provider.provides(ROW_COUNT_KEY):
            return None
        try:
            return (
                provider.integer("source", ROW_COUNT_KEY),
                provider.integer("target", ROW_COUNT_KEY),
            )
        except Exception:
            return None

    @staticmethod
    def _shared_null_counts(
        provider, audit_columns: List[str]
    ) -> Optional[Dict[str, int]]:
        """Read every audit column's NULL count from the shared scan."""
        if provider is None:
            return None
        keys = {
            column: audit_null_count_key(column) for column in audit_columns
        }
        if not all(
            provider.provides(key, "target") for key in keys.values()
        ):
            return None
        try:
            return {
                column: provider.integer("target", key)
                for column, key in keys.items()
            }
        except Exception:
            return None

    def _target_columns(
        self, target_conn, schema, table, target_fqn, dialect
    ) -> Dict[str, str]:
        """The target object's actual columns, from metadata where available."""
        describe = getattr(target_conn, "get_table_columns", None)
        if callable(describe) and schema and table:
            try:
                columns = describe(schema, table)
                by_casefold = {
                    str(column).casefold(): str(column) for column in columns
                } if columns is not None else {}
            except Exception:
                by_casefold = {}
            # Metadata that names no columns is inconclusive (unsupported
            # connector, insufficient metadata privileges), never proof the
            # columns are absent: the zero-row read stays authoritative.
            if by_casefold:
                return by_casefold
        # No usable metadata operation: read an empty result set instead.
        if dialect.row_limit_style == "top":
            probe_sql = f"SELECT {dialect.row_limit_keyword} 0 * FROM {target_fqn}"
        else:
            probe_sql = dialect.select("*", target_fqn, " WHERE 1 = 0")
        schema_frame = target_conn.execute_query(probe_sql)
        if not isinstance(schema_frame, pd.DataFrame):
            raise TypeError("Connector execute_query must return a pandas DataFrame")
        return {
            str(column).casefold(): str(column) for column in schema_frame.columns
        }

    @staticmethod
    def _result(
        name, status, columns, source_count, target_count, reason,
        profile, started, message, extra=None,
    ) -> CheckResult:
        details = {
            "validation_category": "audit_validation",
            "validation_profile": profile,
            "check_name": name,
            "audit_columns_checked": list(columns),
            "source_record_count": source_count,
            "target_record_count": target_count,
            "status": status.value,
            "failure_reason": reason,
            "execution_time": round(time.perf_counter() - started, 3),
        }
        details.update(extra or {})
        return CheckResult(
            check_name=name,
            status=status,
            source_value=source_count,
            target_value=target_count,
            delta=abs(source_count - target_count),
            message=message,
            details=details,
        )

    @staticmethod
    def _error(
        message, started, check_name="audit_validation", columns=None,
        source_count=None, target_count=None, profile=None,
    ) -> CheckResult:
        return AuditValidator._result(
            check_name, ValidationStatus.ERROR, columns or [], source_count,
            target_count, message, profile, started, message,
        )
