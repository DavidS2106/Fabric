from __future__ import annotations

from datetime import datetime, time as time_value
from decimal import Decimal, InvalidOperation
import numbers
import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from core.capabilities import resolve_capabilities
from core.models import CheckResult, ValidationStatus
from core.scan_planner import DUPLICATE_ROW_ALIAS
from core.strategy import BatchStrategy, StrategyResolver
from core.validation_policy import (
    ValidationPolicies, ValidationPolicyError, exception_category,
)
from core.value_normalization import (
    is_null_scalar, temporal_token, temporal_values_match,
)
from .base_validator import BaseValidator
from .record_comparison_validator import RecordComparisonValidator
from .validator_utils import _fqn


CheckEvaluation = Tuple[bool, bool, str]


class DataValueValidator(BaseValidator):
    """Run YAML-selected value-preservation checks in one aligned table scan."""

    DEFAULT_LARGE_NUMERIC_THRESHOLD = Decimal("2147483648")

    #: Checks that also report how many NULLs each side held.
    NULL_TRACKING_CHECKS = frozenset({
        "null_preservation_check",
        "null_vs_blank_check",
        "default_value_replacement_check",
    })

    def __init__(self) -> None:
        self._comparison = RecordComparisonValidator()
        self._checks: Dict[str, Callable[[Any, Any, Dict[str, Any]], CheckEvaluation]] = {
            "empty_string_preservation_check": self._empty_string,
            "special_character_check": self._special_character,
            "unicode_character_check": self._unicode_character,
            "max_character_length_check": self._max_character_length,
            "decimal_precision_value_check": self._decimal_precision,
            "negative_value_check": self._negative_value,
            "zero_value_check": self._zero_value,
            "large_numeric_value_check": self._large_numeric_value,
            "time_component_check": self._time_component,
            "timestamp_precision_check": self._timestamp_precision,
            "timezone_preservation_check": self._timezone_preservation,
            "leading_zero_check": self._leading_zero,
            "case_preservation_check": self._case_preservation,
            "null_preservation_check": self._null_preservation,
            "null_vs_blank_check": self._null_vs_blank,
            "default_value_replacement_check": self._default_value_replacement,
        }

    @property
    def supported_checks(self) -> frozenset[str]:
        return frozenset(self._checks)

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
        try:
            self._policies = ValidationPolicies.from_config(validations_config)
            self._comparison._policies = self._policies
        except ValidationPolicyError as exc:
            return [self._configuration_error(
                str(exc), validation_profile, started,
                status=ValidationStatus.CONFIG_ERROR,
                exception_category=exception_category(exc),
                table_id=getattr(table_map, "id", None),
            )]
        category_config = (validations_config or {}).get(
            "data_value_validation", {}
        ) or {}
        if isinstance(category_config, bool):
            category_config = {"enabled": category_config, "checks": []}
        if not isinstance(category_config, dict):
            return [
                self._configuration_error(
                    "data_value_validation must be a mapping",
                    validation_profile,
                    started,
                )
            ]
        if not category_config.get("enabled", False):
            return []

        configured_checks = category_config.get("checks", []) or []
        if not isinstance(configured_checks, list):
            return [
                self._configuration_error(
                    "data_value_validation.checks must be a list",
                    validation_profile,
                    started,
                )
            ]

        enabled: List[Tuple[str, Dict[str, Any]]] = []
        seen = set()
        configuration_errors: List[CheckResult] = []
        for index, item in enumerate(configured_checks):
            if not isinstance(item, dict):
                configuration_errors.append(
                    self._configuration_error(
                        f"data_value_validation.checks[{index}] must be a mapping",
                        validation_profile,
                        started,
                    )
                )
                continue
            name = item.get("name")
            if not isinstance(name, str) or not name.strip():
                configuration_errors.append(
                    self._configuration_error(
                        f"data_value_validation.checks[{index}].name is required",
                        validation_profile,
                        started,
                    )
                )
                continue
            name = name.strip()
            if name in seen:
                configuration_errors.append(
                    self._configuration_error(
                        f"Duplicate data value check '{name}'",
                        validation_profile,
                        started,
                        check_name=name,
                    )
                )
                continue
            seen.add(name)
            if not isinstance(item.get("enabled", True), bool):
                configuration_errors.append(
                    self._configuration_error(
                        f"Data value check '{name}' enabled must be boolean",
                        validation_profile,
                        started,
                        check_name=name,
                    )
                )
                continue
            if not item.get("enabled", True):
                continue
            if name not in self._checks:
                configuration_errors.append(
                    self._configuration_error(
                        f"Unsupported data value check '{name}'",
                        validation_profile,
                        started,
                        check_name=name,
                    )
                )
                continue
            check_config_error = self._check_config_error(name, item)
            if check_config_error:
                configuration_errors.append(
                    self._configuration_error(
                        check_config_error,
                        validation_profile,
                        started,
                        check_name=name,
                    )
                )
                continue
            enabled.append((name, dict(item)))

        if not enabled:
            return configuration_errors

        key_mappings = list(getattr(table_map, "key_mappings", None) or [])
        column_mappings = list(
            getattr(table_map, "compare_column_mappings", None) or []
        )
        if not key_mappings:
            action = self._policies.missing_keys["full_data_comparison"]
            status_field = "error_status" if action == "error" else (
                "skip_status" if action == "skip" else "fallback_status"
            )
            status = self._policies.configured_status(status_field)
            return configuration_errors + self._all_errors(
                enabled,
                f"Data value validation cannot align rows without key_mappings; configured action={action}",
                validation_profile,
                started,
                status=status,
                exception_category="MISSING_KEY_MAPPING",
                table_id=getattr(table_map, "id", None),
            )
        if not column_mappings:
            return configuration_errors + self._all_errors(
                enabled,
                "Data value validation requires compare_column_mappings",
                validation_profile,
                started,
            )

        if execution_config is None:
            # Backward-compatible direct validator calls still resolve runtime
            # sizing from the canonical execution document.
            from core.config_loader import ConfigLoader
            execution_config = ConfigLoader("config").load().performance
        configured_batch = getattr(execution_config, "batch_processing", None)
        batch_size = getattr(configured_batch, "batch_size", None)
        if not (
            isinstance(batch_size, int) and not isinstance(batch_size, bool) and batch_size > 0
        ):
            return configuration_errors + self._all_errors(
                enabled,
                "data_value_validation.batch_size must be a positive integer",
                validation_profile,
                started,
            )

        source_keys = [item.source_column for item in key_mappings]
        target_keys = [item.target_column for item in key_mappings]
        try:
            normalized_mappings = self._normalize_column_mappings(column_mappings)
        except ValueError as exc:
            return configuration_errors + self._all_errors(
                enabled,
                str(exc),
                validation_profile,
                started,
            )
        source_columns = [item["source_column"] for item in normalized_mappings]
        target_columns = [item["target_column"] for item in normalized_mappings]
        source_dialect = self._dialect(source_conn)
        target_dialect = self._dialect(target_conn)
        src_fqn = _fqn(
            table_map.source_schema, table_map.source_table, source_dialect
        )
        tgt_fqn = _fqn(
            table_map.target_schema, table_map.target_table, target_dialect
        )
        source_where = self._build_where_clause(table_map, source_dialect, "source")
        target_where = self._build_where_clause(table_map, target_dialect, "target")
        try:
            sampling_config = self._comparison._large_table_sampling_config(
                (validations_config or {}).get("comparison_settings", {}).get(
                    "large_table_sampling"
                )
            )
            source_row_count = target_row_count = None
            comparison_mode = "full"
            configured_sample_size = sampling_config["sample_size"]
            record_limit = None
            if sampling_config["enabled"]:
                source_row_count, target_row_count = (
                    self._comparison._cached_row_counts(execution_cache)
                )
                if self._comparison._large_table_sampling_applies(
                    source_row_count, target_row_count, sampling_config
                ):
                    comparison_mode = "sample"
                    record_limit = min(configured_sample_size, source_row_count)
        except (AttributeError, ValueError) as exc:
            return configuration_errors + self._all_errors(
                enabled,
                str(exc),
                validation_profile,
                started,
                status=ValidationStatus.CONFIG_ERROR,
                exception_category="CONFIGURATION_POLICY",
                table_id=getattr(table_map, "id", None),
            )
        # Value checks always need aligned records, so the bounded batching
        # strategy the two connectors share is what actually runs.
        batch_strategy_name = self._resolved_batch_strategy_name(
            source_conn, target_conn, table_map, execution_config
        )

        states = {
            name: {
                "evaluated": 0,
                "matched": 0,
                "mismatched": 0,
                "first_source": None,
                "first_target": None,
                "failure_reason": "",
                "column_mapping": None,
                "primary_key": None,
                "source_null_count": 0,
                "target_null_count": 0,
                "error": None,
            }
            for name, _ in enabled
        }

        try:
            source_duplicate_sql = self._comparison._duplicate_record_count_sql(
                src_fqn, source_where, source_keys, source_dialect
            )
            target_duplicate_sql = self._comparison._duplicate_record_count_sql(
                tgt_fqn, target_where, target_keys, target_dialect
            )
            provider = self._shared_statistics(execution_cache)

            def duplicate_count(role, connection, sql):
                if provider is not None and provider.provides(
                    DUPLICATE_ROW_ALIAS, role
                ):
                    try:
                        return provider.integer(role, DUPLICATE_ROW_ALIAS)
                    except Exception:
                        pass
                return self._comparison._duplicate_record_count(
                    connection, sql, role
                )

            source_duplicates, target_duplicates = self._run_both_sides(
                execution_config,
                lambda: duplicate_count(
                    "source", source_conn, source_duplicate_sql
                ),
                lambda: duplicate_count(
                    "target", target_conn, target_duplicate_sql
                ),
            )
            if source_duplicates or target_duplicates:
                raise ValueError(
                    "Data value validation requires unique table key_mappings; "
                    f"source duplicate records={source_duplicates}, "
                    f"target duplicate records={target_duplicates}"
                )

            source_projection = self._comparison._projection(
                source_keys + source_columns, source_dialect
            )
            target_projection = self._comparison._projection(
                target_keys + target_columns, target_dialect
            )
            last_source_key = None
            last_source_token = None
            source_records_scanned = 0
            target_records_matched = 0
            batch_count = 0

            while True:
                if record_limit is not None:
                    remaining = record_limit - source_records_scanned
                    if remaining <= 0:
                        break
                    page_size = min(batch_size, remaining)
                else:
                    page_size = batch_size
                source_sql = self._comparison._keyset_select_sql(
                    src_fqn,
                    source_projection,
                    source_where,
                    source_keys,
                    page_size,
                    last_source_key,
                    source_dialect,
                )
                source_page = source_conn.execute_query(source_sql)
                if not isinstance(source_page, pd.DataFrame):
                    raise TypeError(
                        "Connector execute_query must return a pandas DataFrame"
                    )
                if source_page.empty:
                    break
                self._comparison._require_columns(
                    source_page, source_keys + source_columns, "source data value"
                )
                batch_count += 1
                source_records_scanned += len(source_page)
                source_key_values = self._comparison._extract_values(
                    source_page, source_keys
                )
                self._comparison._filter_batch_size = batch_size
                target_queries = self._comparison._build_filter_queries(
                    tgt_fqn,
                    target_projection,
                    target_where,
                    target_keys,
                    source_key_values,
                    target_dialect,
                )
                target_page = self._comparison._execute_queries(
                    target_conn, target_queries
                )
                self._comparison._require_columns(
                    target_page, target_keys + target_columns, "target data value"
                )
                target_records_matched += len(target_page)
                target_page = target_page.copy()
                target_page["_merge_key"] = self._comparison._key_tokens(
                    target_page, target_keys
                )
                if target_page["_merge_key"].duplicated().any():
                    raise ValueError(
                        "Target key_mappings are not unique in the aligned cohort"
                    )
                source_page = source_page.copy()
                source_page["_merge_key"] = self._comparison._key_tokens(
                    source_page, source_keys
                )
                self._evaluate_batch(
                    source_page,
                    target_page,
                    normalized_mappings,
                    enabled,
                    states,
                    source_keys,
                    target_keys,
                    getattr(table_map, "id", None),
                )

                next_source_key = source_key_values[-1]
                next_source_token = tuple(
                    self._comparison._canonical_value(value)
                    for value in next_source_key
                )
                if next_source_token == last_source_token:
                    raise RuntimeError(
                        "Data value keyset pagination did not advance"
                    )
                last_source_key = next_source_key
                last_source_token = next_source_token
                if len(source_page) < page_size:
                    break

            duration = round(time.perf_counter() - started, 3)
            results = configuration_errors
            for name, _ in enabled:
                state = states[name]
                if state["error"] is not None:
                    status = self._policies.configured_status(
                        "framework_exception_status"
                    )
                else:
                    status = (
                        ValidationStatus.PASS
                        if state["mismatched"] == 0
                        else ValidationStatus.FAIL
                    )
                results.append(
                    CheckResult(
                        check_name=name,
                        status=status,
                        source_value=state["first_source"],
                        target_value=state["first_target"],
                        delta=state["mismatched"],
                        message=(
                            f"Evaluated {state['evaluated']} applicable values; "
                            f"mismatches={state['mismatched']}"
                        ),
                        details={
                            "validation_category": "data_value_validation",
                            "validation_profile": validation_profile,
                            "check_name": name,
                            "status": status.value,
                            "failure_reason": state["failure_reason"],
                            "execution_time": duration,
                            "values_evaluated": state["evaluated"],
                            "matched_values": state["matched"],
                            "mismatch_values": state["mismatched"],
                            "source_null_count": state["source_null_count"],
                            "target_null_count": state["target_null_count"],
                            "mismatch_count": state["mismatched"],
                            "source_records_scanned": source_records_scanned,
                            "target_records_matched": target_records_matched,
                            "batch_count": batch_count,
                            "batch_size": batch_size,
                            "comparison_mode": comparison_mode,
                            "configured_sample_size": configured_sample_size,
                            "actual_sample_size": (
                                source_records_scanned
                                if comparison_mode == "sample" else None
                            ),
                            "source_row_count": source_row_count,
                            "target_row_count": target_row_count,
                            "pagination_strategy": "keyset",
                            "execution_strategy": batch_strategy_name,
                            "batch_strategy": batch_strategy_name,
                            **self._incremental_scope(table_map),
                            "key_mapping_source": "table_yaml",
                            "key_columns_used": [
                                {
                                    "source_column": item.source_column,
                                    "target_column": item.target_column,
                                }
                                for item in key_mappings
                            ],
                            "column_mapping": state["column_mapping"],
                            "primary_key": state["primary_key"],
                            **(state["error"] or {}),
                        },
                    )
                )
            return results
        except Exception as exc:
            return configuration_errors + self._all_errors(
                enabled,
                f"Data value validation failed: {exc}",
                validation_profile,
                started,
                status=self._policies.configured_status(
                    "framework_exception_status"
                ),
                exception_category=exception_category(exc),
                table_id=getattr(table_map, "id", None),
            )

    def _resolved_batch_strategy_name(
        self, source_conn, target_conn, table_map, execution_config
    ) -> str:
        """Name the batching strategy both connectors support, for reporting."""
        try:
            resolved = StrategyResolver(execution_config).resolve_batch_strategy(
                resolve_capabilities(source_conn),
                resolve_capabilities(target_conn),
                table_map,
            )
        except Exception:
            return BatchStrategy.KEYSET.value
        return (resolved or BatchStrategy.KEYSET).value

    def _evaluate_batch(
        self,
        source_page: pd.DataFrame,
        target_page: pd.DataFrame,
        mappings: List[Dict[str, Any]],
        enabled: List[Tuple[str, Dict[str, Any]]],
        states: Dict[str, Dict[str, Any]],
        source_keys: List[str],
        target_keys: List[str],
        table_id: Optional[str],
    ) -> None:
        """Evaluate every enabled check over one aligned batch, column at a time.

        The batch is aligned once with a positional index instead of a per-record
        frame lookup, each mapped column is read once as a plain sequence, and
        every check's evaluation context is built once per column rather than
        once per value. Only the value-semantics decision itself stays per value,
        because that is what these checks genuinely require.
        """
        comparison = self._comparison
        target_positions: Dict[Tuple[str, ...], int] = {}
        if not target_page.empty:
            target_positions = {
                token: position
                for position, token in enumerate(
                    comparison._key_tokens(target_page, target_keys)
                )
            }
        source_tokens = source_page["_merge_key"].tolist()
        aligned_positions = [target_positions.get(token) for token in source_tokens]
        has_target = [position is not None for position in aligned_positions]
        record_count = len(source_tokens)
        display_keys: Dict[int, str] = {}

        # Earliest failure per check, ordered by (record, mapping) so the
        # reported sample matches record order.
        first_failure: Dict[str, Tuple[Tuple[int, int], Dict[str, Any]]] = {}

        for mapping_index, mapping in enumerate(mappings):
            source_values = source_page[mapping["source_column"]].tolist()
            source_datatype = str(source_page[mapping["source_column"]].dtype)
            target_datatype = (
                str(target_page[mapping["target_column"]].dtype)
                if mapping["target_column"] in target_page.columns
                else None
            )
            if target_page.empty:
                aligned_targets: List[Any] = [None] * record_count
            else:
                target_values = target_page[mapping["target_column"]].tolist()
                aligned_targets = [
                    target_values[position] if position is not None else None
                    for position in aligned_positions
                ]
            blank_is_null = self._policies.nulls["blank_is_null"]
            source_null = [
                is_null_scalar(value, blank_is_null=blank_is_null)
                for value in source_values
            ]
            target_null = [
                is_null_scalar(value, blank_is_null=blank_is_null)
                for value in aligned_targets
            ]

            for name, check_config in enabled:
                state = states[name]
                evaluate = self._checks[name]
                evaluation_config = dict(check_config)
                evaluation_config["column_mapping"] = mapping
                evaluation_config["null_handling"] = self._policies.nulls
                evaluation_config["timestamp_handling"] = self._policies.timestamps
                tracks_nulls = name in self.NULL_TRACKING_CHECKS
                for record in range(record_count):
                    source_value = source_values[record]
                    target_value = aligned_targets[record]
                    record_has_target = has_target[record]
                    if tracks_nulls:
                        if source_null[record]:
                            state["source_null_count"] += 1
                        if record_has_target and target_null[record]:
                            state["target_null_count"] += 1
                    try:
                        applicable, preserved, reason = evaluate(
                            source_value, target_value, evaluation_config
                        )
                    except Exception as exc:
                        if state["error"] is None:
                            source_token = temporal_token(
                                source_value,
                                parse_strings=self._policies.timestamps["parse_iso_strings"],
                            )
                            target_token = temporal_token(
                                target_value,
                                parse_strings=self._policies.timestamps["parse_iso_strings"],
                            )
                            state["error"] = {
                                "table_id": table_id,
                                "source_column": mapping["source_column"],
                                "target_column": mapping["target_column"],
                                "validation_name": name,
                                "exception_category": exception_category(exc),
                                "source_datatype": source_datatype,
                                "target_datatype": target_datatype,
                                "source_precision": source_token.precision if source_token else None,
                                "target_precision": target_token.precision if target_token else None,
                                "comparison_strategy": "precision_aware_native",
                                "fallback_used": None,
                                "error_reason": str(exc),
                            }
                        if not self._policies.errors["continue_after_check_error"]:
                            raise
                        continue
                    if not applicable:
                        continue
                    state["evaluated"] += 1
                    if record_has_target and preserved:
                        state["matched"] += 1
                        continue
                    state["mismatched"] += 1
                    order = (record, mapping_index)
                    existing = first_failure.get(name)
                    if existing is not None and existing[0] <= order:
                        continue
                    if record not in display_keys:
                        display_keys[record] = comparison._display_key(
                            source_tokens[record]
                        )
                    first_failure[name] = (
                        order,
                        {
                            "first_source": source_value,
                            "first_target": target_value,
                            "failure_reason": (
                                "Matching target record was not found"
                                if not record_has_target
                                else reason
                            ),
                            "column_mapping": mapping,
                            "primary_key": display_keys[record],
                        },
                    )

        for name, (_, payload) in first_failure.items():
            state = states[name]
            if state["failure_reason"]:
                # An earlier batch already reported this check's first failure.
                continue
            state.update(payload)

    @staticmethod
    def _normalize_column_mappings(configured: List[Any]) -> List[Dict[str, Any]]:
        normalized = []
        seen = set()
        for index, item in enumerate(configured):
            if not isinstance(item, dict):
                raise ValueError(
                    f"compare_column_mappings[{index}] must be a mapping"
                )
            source_column = item.get("source_column")
            target_column = item.get("target_column")
            if not all(
                isinstance(column, str) and column.strip()
                for column in (source_column, target_column)
            ):
                raise ValueError(
                    f"compare_column_mappings[{index}] requires source_column "
                    "and target_column"
                )
            pair = (source_column.strip(), target_column.strip())
            if pair in seen:
                raise ValueError(f"Duplicate compare column mapping {pair}")
            allow_replacement = item.get("allow_null_default_replacement", False)
            if not isinstance(allow_replacement, bool):
                raise ValueError(
                    f"compare_column_mappings[{index}]."
                    "allow_null_default_replacement must be boolean"
                )
            if allow_replacement and "null_default_value" not in item:
                raise ValueError(
                    f"compare_column_mappings[{index}] must define "
                    "null_default_value when NULL default replacement is allowed"
                )
            seen.add(pair)
            normalized_item = dict(item)
            normalized_item["source_column"] = pair[0]
            normalized_item["target_column"] = pair[1]
            normalized.append(normalized_item)
        return normalized

    def _check_config_error(
        self, name: str, config: Dict[str, Any]
    ) -> Optional[str]:
        if name != "large_numeric_value_check":
            return None
        threshold = self._as_decimal(
            config.get(
                "minimum_absolute_value", self.DEFAULT_LARGE_NUMERIC_THRESHOLD
            )
        )
        if threshold is None or threshold < 0:
            return (
                "large_numeric_value_check.minimum_absolute_value must be "
                "a non-negative number"
            )
        return None

    def _all_errors(
        self,
        enabled: List[Tuple[str, Dict[str, Any]]],
        message: str,
        validation_profile: Optional[str],
        started: float,
        status: ValidationStatus = ValidationStatus.CONFIG_ERROR,
        exception_category: str = "CONFIGURATION",
        table_id: Optional[str] = None,
    ) -> List[CheckResult]:
        return [
            self._configuration_error(
                message,
                validation_profile,
                started,
                check_name=name,
                status=status,
                exception_category=exception_category,
                table_id=table_id,
            )
            for name, _ in enabled
        ]

    @staticmethod
    def _configuration_error(
        message: str,
        validation_profile: Optional[str],
        started: float,
        check_name: str = "data_value_validation",
        status: ValidationStatus = ValidationStatus.CONFIG_ERROR,
        exception_category: str = "CONFIGURATION",
        table_id: Optional[str] = None,
    ) -> CheckResult:
        return CheckResult(
            check_name=check_name,
            status=status,
            message=message,
            details={
                "validation_category": "data_value_validation",
                "validation_profile": validation_profile,
                "check_name": check_name,
                "status": status.value,
                "table_id": table_id,
                "validation_name": check_name,
                "exception_category": exception_category,
                "error_reason": message,
                "source_null_count": 0,
                "target_null_count": 0,
                "mismatch_count": 0,
                "failure_reason": message,
                "execution_time": round(time.perf_counter() - started, 3),
            },
        )

    @staticmethod
    def _equal(source: Any, target: Any) -> bool:
        if pd.isna(source) and pd.isna(target):
            return True
        if pd.isna(source) or pd.isna(target):
            return False
        try:
            return bool(source == target)
        except (TypeError, ValueError):
            return str(source) == str(target)

    @staticmethod
    def _is_number(value: Any) -> bool:
        return (
            isinstance(value, numbers.Number)
            and not isinstance(value, (bool, complex))
            and not bool(pd.isna(value))
        )

    @staticmethod
    def _as_decimal(value: Any) -> Optional[Decimal]:
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return None

    @staticmethod
    def _fractional_digits(value: Any) -> int:
        decimal_value = DataValueValidator._as_decimal(value)
        if decimal_value is None:
            return -1
        return max(-decimal_value.as_tuple().exponent, 0)

    @staticmethod
    def _timestamp(value: Any, config: Dict[str, Any]):
        timestamp_config = config["timestamp_handling"]
        null_config = config["null_handling"]
        if is_null_scalar(value, blank_is_null=null_config["blank_is_null"]):
            return None
        token = temporal_token(
            value, parse_strings=timestamp_config["parse_iso_strings"]
        )
        if token is None and timestamp_config["out_of_range_strategy"] == "error":
            raise ValueError("Temporal value cannot be represented by configured strategy")
        return token

    def _empty_string(self, source, target, _config) -> CheckEvaluation:
        applies = isinstance(source, str) and source == ""
        return applies, applies and target == "", "Empty string was not preserved"

    def _special_character(self, source, target, _config) -> CheckEvaluation:
        applies = isinstance(source, str) and bool(re.search(r"[^\w\s]", source))
        return applies, applies and self._equal(source, target), "Special characters changed"

    def _unicode_character(self, source, target, _config) -> CheckEvaluation:
        applies = isinstance(source, str) and any(ord(char) > 127 for char in source)
        return applies, applies and self._equal(source, target), "Unicode characters changed"

    def _max_character_length(self, source, target, _config) -> CheckEvaluation:
        applies = isinstance(source, str)
        preserved = applies and isinstance(target, str) and len(target) >= len(source)
        return applies, preserved, "Target character value was truncated"

    def _decimal_precision(self, source, target, _config) -> CheckEvaluation:
        applies = (
            isinstance(source, Decimal)
            or (self._is_number(source) and self._fractional_digits(source) > 0)
        )
        source_decimal = self._as_decimal(source)
        target_decimal = self._as_decimal(target)
        preserved = (
            applies
            and source_decimal is not None
            and target_decimal == source_decimal
            and self._fractional_digits(target) >= self._fractional_digits(source)
        )
        return applies, preserved, "Decimal value or fractional precision changed"

    def _negative_value(self, source, target, _config) -> CheckEvaluation:
        applies = self._is_number(source) and source < 0
        return applies, applies and self._equal(source, target), "Negative value changed"

    def _zero_value(self, source, target, _config) -> CheckEvaluation:
        applies = self._is_number(source) and source == 0
        return applies, applies and self._equal(source, target), "Zero value changed"

    def _large_numeric_value(self, source, target, config) -> CheckEvaluation:
        threshold = self._as_decimal(
            config.get(
                "minimum_absolute_value", self.DEFAULT_LARGE_NUMERIC_THRESHOLD
            )
        )
        # Configuration is validated before the shared table scan.
        assert threshold is not None
        source_decimal = self._as_decimal(source) if self._is_number(source) else None
        applies = source_decimal is not None and abs(source_decimal) >= threshold
        return applies, applies and self._equal(source, target), "Large numeric value changed"

    def _time_component(self, source, target, config) -> CheckEvaluation:
        null_config = config["null_handling"]
        if is_null_scalar(source, blank_is_null=null_config["blank_is_null"]):
            return False, True, "Source temporal value is NULL"
        if isinstance(source, time_value):
            applies = True
            source_token = self._timestamp(source, config)
            target_token = self._timestamp(target, config)
        else:
            source_token = self._timestamp(source, config)
            target_token = self._timestamp(target, config)
            applies = source_token is not None
        preserved = (
            applies
            and target_token is not None
            and source_token.time_component == target_token.time_component
            and source_token.fraction == target_token.fraction
        )
        return applies, preserved, "Time component changed"

    def _timestamp_precision(self, source, target, config) -> CheckEvaluation:
        source_timestamp = self._timestamp(source, config)
        target_timestamp = self._timestamp(target, config)
        applies = source_timestamp is not None
        timestamp_config = config["timestamp_handling"]
        preserved = (
            applies
            and target_timestamp is not None
            and temporal_values_match(
                source_timestamp,
                target_timestamp,
                preserve_precision=timestamp_config["preserve_precision"],
                timezone_comparison=timestamp_config["timezone_comparison"],
                precision_loss_behavior=timestamp_config["precision_loss_behavior"],
                fractional_second_tolerance=timestamp_config[
                    "fractional_second_tolerance"
                ],
            )
        )
        return applies, preserved, "Timestamp value or precision changed"

    def _timezone_preservation(self, source, target, config) -> CheckEvaluation:
        source_timestamp = self._timestamp(source, config)
        target_timestamp = self._timestamp(target, config)
        applies = source_timestamp is not None and bool(source_timestamp.timezone)
        preserved = (
            applies
            and target_timestamp is not None
            and source_timestamp.timezone == target_timestamp.timezone
            and source_timestamp.canonical == target_timestamp.canonical
        )
        return applies, preserved, "Timezone information changed"

    def _leading_zero(self, source, target, _config) -> CheckEvaluation:
        applies = isinstance(source, str) and bool(re.fullmatch(r"0+\d+", source))
        return applies, applies and self._equal(source, target), "Leading zeros changed"

    def _case_preservation(self, source, target, _config) -> CheckEvaluation:
        applies = isinstance(source, str) and any(char.isalpha() for char in source)
        return applies, applies and self._equal(source, target), "Character case changed"

    def _null_preservation(self, source, target, config) -> CheckEvaluation:
        policy = config["null_handling"]
        applies = is_null_scalar(source, blank_is_null=policy["blank_is_null"])
        target_null = is_null_scalar(
            target, blank_is_null=policy["blank_is_null"]
        )
        preserved = applies and (
            (target_null and policy["both_null"] == "match")
            or (not target_null and policy["one_null"] == "match")
        )
        return (
            applies,
            preserved,
            "Source NULL was not preserved as target NULL",
        )

    def _null_vs_blank(self, source, target, config) -> CheckEvaluation:
        policy = config["null_handling"]
        source_is_null = is_null_scalar(source, blank_is_null=False)
        source_is_blank = isinstance(source, str) and not source.strip()
        applies = source_is_null or source_is_blank
        preserved = (
            source_is_null and is_null_scalar(target, blank_is_null=False)
        ) or (
            source_is_blank and isinstance(target, str) and not target.strip()
        )
        if policy["blank_is_null"]:
            preserved = is_null_scalar(source, blank_is_null=True) and is_null_scalar(
                target, blank_is_null=True
            )
        return (
            applies,
            applies and preserved,
            "NULL and blank values were not preserved distinctly",
        )

    def _default_value_replacement(self, source, target, config) -> CheckEvaluation:
        policy = config["null_handling"]
        applies = is_null_scalar(source, blank_is_null=policy["blank_is_null"])
        if not applies:
            return False, False, "Source value is not NULL"
        if is_null_scalar(target, blank_is_null=policy["blank_is_null"]):
            return True, policy["both_null"] == "match", ""

        mapping = config.get("column_mapping", {}) or {}
        replacement_allowed = mapping.get(
            "allow_null_default_replacement", False
        )
        has_configured_default = "null_default_value" in mapping
        preserved = bool(
            replacement_allowed
            and has_configured_default
            and self._equal(mapping["null_default_value"], target)
        )
        return (
            True,
            preserved,
            "Source NULL was replaced with an unconfigured default value",
        )
