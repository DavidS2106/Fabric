from __future__ import annotations

from typing import Any, Dict, List, Optional

from core.models import CheckResult, ValidationStatus
from core.scan_planner import ROW_COUNT_KEY, StatisticRequest
from core.validation_policy import ZeroRecordPolicy
from .base_validator import BaseValidator
from .validator_utils import _fqn


class EdgeCaseValidator(BaseValidator):
    """Execute YAML-selected dataset edge checks before normal validation."""

    SUPPORTED_CHECKS = frozenset({"zero_record_dataset_check"})

    @staticmethod
    def statistic_requests(table_map=None):
        """The statistics this check needs from the shared scan."""
        return [
            StatisticRequest(
                key=ROW_COUNT_KEY,
                kind="row_count",
                requested_by=("edge_case_validation",),
            )
        ]

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
        config = (validations_config or {}).get("edge_case_validation", {}) or {}
        if isinstance(config, bool):
            config = {"enabled": config, "checks": []}
        if not isinstance(config, dict):
            return [self._error("edge_case_validation must be a mapping")]
        if not config.get("enabled", False):
            return []

        checks = config.get("checks", []) or []
        if not isinstance(checks, list):
            return [self._error("edge_case_validation.checks must be a list")]

        enabled_names = []
        seen = set()
        for index, check in enumerate(checks):
            if not isinstance(check, dict):
                return [self._error(
                    f"edge_case_validation.checks[{index}] must be a mapping"
                )]
            name = check.get("name")
            if not isinstance(name, str) or not name.strip():
                return [self._error(
                    f"edge_case_validation.checks[{index}].name is required"
                )]
            name = name.strip()
            if name in seen:
                return [self._error(f"Duplicate edge case check '{name}'", name)]
            seen.add(name)
            enabled = check.get("enabled", True)
            if not isinstance(enabled, bool):
                return [self._error(
                    f"Edge case check '{name}' enabled must be boolean", name
                )]
            if not enabled:
                continue
            if name not in self.SUPPORTED_CHECKS:
                return [self._error(f"Unsupported edge case check '{name}'", name)]
            enabled_names.append(name)

        if "zero_record_dataset_check" not in enabled_names:
            return []

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
        source_sql = source_dialect.select(
            f"{source_dialect.count_all()} AS record_count", source_fqn, source_where
        )
        target_sql = target_dialect.select(
            f"{target_dialect.count_all()} AS record_count", target_fqn, target_where
        )
        strategy = "pushdown"
        shared_used = False
        try:
            shared = self._shared_counts(execution_cache)
            if shared is not None:
                source_count, target_count = shared
                strategy = "shared_statistics_scan"
                shared_used = True
                provider = self._shared_statistics(execution_cache)
                source_sql = provider.statement("source") or source_sql
                target_sql = provider.statement("target") or target_sql
            else:
                # Both counts are pushed down, and the run-local cache lets the
                # row-count and audit validations reuse them.
                def count(role, connection, sql):
                    compute = lambda: int(connection.execute_query(sql).iloc[0, 0])
                    return self._row_count_once(
                        execution_cache, role, compute, consumer="edge_case_validation"
                    )

                source_count, target_count = self._run_both_sides(
                    execution_config,
                    lambda: count("source", source_conn, source_sql),
                    lambda: count("target", target_conn, target_sql),
                )
        except Exception as exc:
            return [self._error(
                f"Zero record dataset check failed: {exc}",
                "zero_record_dataset_check",
            )]

        # What happens to the remaining validations is a configured decision.
        policy = ZeroRecordPolicy.from_config(validations_config)
        action = policy.action_for(source_count, target_count)
        continue_normal = policy.continues_downstream(action)

        if source_count == 0 and target_count == 0:
            status = ValidationStatus.PASS
            failure_reason = ""
            message = "Source and target datasets both contain zero records"
        elif source_count == 0:
            status = ValidationStatus.FAIL
            failure_reason = "Source dataset is empty while target contains records"
            message = failure_reason
        elif target_count == 0:
            status = ValidationStatus.FAIL
            failure_reason = "Target dataset is empty while source contains records"
            message = failure_reason
        else:
            status = ValidationStatus.PASS
            failure_reason = ""
            message = "Both datasets contain records; continuing normal validation"
        if action == "warn_and_continue" and status is ValidationStatus.PASS and (
            source_count == 0 or target_count == 0
        ):
            status = ValidationStatus.WARNING

        return [CheckResult(
            check_name="zero_record_dataset_check",
            status=status,
            source_value=source_count,
            target_value=target_count,
            delta=abs(source_count - target_count),
            message=message,
            details={
                "validation_category": "edge_case_validation",
                "validation_profile": validation_profile,
                "check_name": "zero_record_dataset_check",
                "source_record_count": source_count,
                "target_record_count": target_count,
                "status": status.value,
                "failure_reason": failure_reason,
                "continue_normal_validation": continue_normal,
                "source_sql": source_sql,
                "target_sql": target_sql,
                "execution_strategy": strategy,
                "shared_scan_used": shared_used,
                "rows_processed": max(source_count, target_count),
                **policy.as_details(action),
                **self._incremental_scope(table_map),
            },
        )]

    def _shared_counts(self, execution_cache):
        """Read both record counts from the shared statistics scan, if present."""
        provider = self._shared_statistics(execution_cache)
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
    def _error(message: str, check_name: str = "edge_case_validation") -> CheckResult:
        return CheckResult(
            check_name=check_name,
            status=ValidationStatus.ERROR,
            message=message,
            details={
                "validation_category": "edge_case_validation",
                "check_name": check_name,
                "status": ValidationStatus.ERROR.value,
                "source_record_count": None,
                "target_record_count": None,
                "failure_reason": message,
                "continue_normal_validation": False,
            },
        )
