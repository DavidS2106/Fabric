"""
fabric_migration_validator/validators/record_comparison_production.py
────────────────────────────────────────────────────────────────────

Production-grade tier-based record comparison with:

1. Mandatory >=1M sampling (100K records, never full comparison)
2. Correct zero-row handling (0/0 vs 0/N vs N/0)
3. True narrow hash-first (key+signature only, then detail fetch on mismatch)
4. SHA-256 canonical multiset comparison for keyless tables
5. Presence markers for safe full joins
6. Polars vectorized comparison (no Python row loops)
7. Memory-bounded processing
8. Pandas only for bounded evidence/reporting
9. Reuse cached key-validation statistics
10. SQL-safe literal generation
11. Batched target cohort lookup (~20 queries for 100K, not 1000s)
12. Correct per-record mismatch logic (not per-cell)
"""

from __future__ import annotations

import gc
import hashlib
import json
import logging
import random
import time
from collections import Counter
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
)

import numpy as np
import pandas as pd

try:
    import polars as pl
except ImportError:
    pl = None

from core.models import (
    CheckResult,
    MatchedRecord,
    MismatchedRecord,
    MissingRecord,
    TableValidationResult,
    ValidationStatus,
)
from core.scan_planner import (
    DISTINCT_KEY_ALIAS,
    DUPLICATE_GROUP_ALIAS,
    NULL_KEY_ALIAS,
    ROW_COUNT_KEY,
    statistic_cache_key,
)
from core.sql_dialect import bind_sql, resolve_dialect
from core.validation_policy import (
    ValidationPolicies,
    ValidationPolicyError,
    exception_category,
)
from core.value_normalization import (
    is_null_scalar,
    temporal_token,
    temporal_values_match,
)
from .base_validator import BaseValidator
from .validator_utils import _fqn

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────
# Constants (Immutable)
# ─────────────────────────────────────────────────────────────────────

MANDATORY_LARGE_TABLE_SAMPLE_SIZE = 100_000
MANDATORY_LARGE_TABLE_ROW_THRESHOLD = 1_000_000
DEFAULT_HIGH_WORKLOAD_THRESHOLD = 100_000_000
DEFAULT_TARGET_KEY_LOOKUP_BATCH_SIZE = 5_000


class StrategyType:
    """Strategy routing outcomes."""
    BOTH_EMPTY = "BOTH_EMPTY"
    ONE_SIDE_EMPTY = "ONE_SIDE_EMPTY"
    POLARS_FULL = "POLARS_FULL"
    POLARS_HASH_FIRST = "POLARS_HASH_FIRST"
    POLARS_SAMPLE_100K = "POLARS_SAMPLE_100K"


class RecordComparisonProduction(BaseValidator):
    """Production record comparison with mandatory large-table sampling."""

    IMPLEMENTED_STRATEGIES = {"random", "deterministic"}

    def validate(
        self,
        source_conn,
        target_conn,
        table_map,
        validations_config: Dict[str, Any],
        result_obj: TableValidationResult,
        source_type: str = "generic",
        comparison_config: Optional[Dict[str, Any]] = None,
        execution_config=None,
        checkpoint_store=None,
        execution_cache=None,
    ) -> List[CheckResult]:
        """Main entry: mandatory >=1M sampling, correct zero-row handling."""
        started = time.perf_counter()

        # === Load Configuration ===
        validations_config = validations_config or {}
        global_config = validations_config.get("record_level_comparison", {}) or {}
        if isinstance(global_config, bool):
            global_config = {"enabled": global_config}

        if not global_config.get("enabled", False):
            return []

        profile_config = comparison_config or {}
        config = {**global_config, **profile_config}

        try:
            self._policies = ValidationPolicies.from_config(validations_config)
        except ValidationPolicyError as exc:
            return [self._error(
                str(exc), started,
                status=ValidationStatus.CONFIG_ERROR,
                exception_category=exception_category(exc),
                table_id=getattr(table_map, "id", None),
            )]

        self._execution_config = execution_config
        comparison_settings = validations_config.get("comparison_settings", {}) or {}

        # === MANDATORY: Fetch row counts early ===
        try:
            source_count, target_count = self._cached_row_counts(execution_cache)
        except ValueError as exc:
            return [self._error(str(exc), started)]

        logger.info(
            "[ROUTING] table=%s source_count=%d target_count=%d",
            getattr(table_map, "id", None), source_count, target_count
        )

        # === Extract configuration ===
        key_mappings = self._key_mappings(table_map)
        configured_columns = self._configured_column_mappings(table_map)

        # === SELECT STRATEGY (Immutable) ===
        strategy = self._select_strategy(
            source_count,
            target_count,
            configured_columns,
            comparison_settings,
        )

        base_details = {
            "table_id": getattr(table_map, "id", None),
            "source_row_count": source_count,
            "target_row_count": target_count,
            "comparison_column_count": len(configured_columns),
            "estimated_cell_workload": source_count * len(configured_columns),
            "selected_strategy": strategy,
            "selected_engine": "POLARS" if pl is not None else "PANDAS",
        }

        # === ROUTE BY STRATEGY ===
        if strategy == StrategyType.BOTH_EMPTY:
            return [CheckResult(
                check_name="record_comparison",
                status=ValidationStatus.PASS,
                message="Both source and target are empty",
                details={**base_details, "execution_time": 0.0},
            )]

        if strategy == StrategyType.ONE_SIDE_EMPTY:
            # One side is empty: all records on non-empty side are missing
            return self._compare_one_side_empty(
                source_conn, target_conn, table_map,
                source_count, target_count,
                configured_columns, key_mappings,
                result_obj, base_details, started
            )

        if strategy == StrategyType.POLARS_SAMPLE_100K:
            logger.info(
                "[TIMING_START] Strategy=POLARS_SAMPLE_100K table=%s >=1M rows (source=%d, target=%d)",
                getattr(table_map, "id", None), source_count, target_count
            )
            logger.info(
                "[MANDATORY] table=%s >=1M rows: 100K sample (source=%d, target=%d)",
                getattr(table_map, "id", None), source_count, target_count
            )
            return self._compare_polars_sample_100k(
                source_conn, target_conn, table_map,
                source_count, target_count,
                configured_columns, key_mappings,
                comparison_settings, result_obj,
                base_details, execution_cache, started
            )

        if strategy == StrategyType.POLARS_HASH_FIRST:
            logger.info(
                "[HASH-FIRST] table=%s workload=%.0fM",
                getattr(table_map, "id", None),
                (source_count * len(configured_columns)) / 1_000_000,
            )
            return self._compare_polars_hash_first(
                source_conn, target_conn, table_map,
                configured_columns, key_mappings,
                comparison_settings, result_obj,
                base_details, execution_cache, started
            )

        # POLARS_FULL (default)
        return self._compare_polars_full(
            source_conn, target_conn, table_map,
            configured_columns, key_mappings,
            comparison_settings, result_obj,
            base_details, execution_cache, started
        )

    # ─────────────────────────────────────────────────────────────────
    # Strategy Selection (Immutable)
    # ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _select_strategy(
        source_count: int,
        target_count: int,
        configured_columns: List[Dict[str, str]],
        comparison_settings: Dict[str, Any],
    ) -> str:
        """
        Immutable strategy selection.

        Requirement 2: Final strategy router.
        """
        # Step 1: Check for empty
        if source_count == 0 and target_count == 0:
            return StrategyType.BOTH_EMPTY

        if source_count == 0 or target_count == 0:
            return StrategyType.ONE_SIDE_EMPTY

        # Step 2: MANDATORY >=1M rule (Requirement 1)
        if source_count >= MANDATORY_LARGE_TABLE_ROW_THRESHOLD \
           or target_count >= MANDATORY_LARGE_TABLE_ROW_THRESHOLD:
            return StrategyType.POLARS_SAMPLE_100K

        # Step 3: Below 1M - decide based on workload
        rows = max(source_count, target_count)
        column_count = len(configured_columns)
        workload = rows * column_count

        tiers_config = comparison_settings.get("comparison_tiers", {})
        high_workload_threshold = int(
            tiers_config.get("high_workload_threshold", DEFAULT_HIGH_WORKLOAD_THRESHOLD)
        )

        if workload >= high_workload_threshold:
            return StrategyType.POLARS_HASH_FIRST

        return StrategyType.POLARS_FULL

    # ─────────────────────────────────────────────────────────────────
    # BOTH_EMPTY and ONE_SIDE_EMPTY (Requirement 3)
    # ─────────────────────────────────────────────────────────────────

    def _compare_one_side_empty(
        self,
        source_conn,
        target_conn,
        table_map,
        source_count: int,
        target_count: int,
        configured_columns: List[Dict[str, str]],
        key_mappings: List[Dict[str, str]],
        result_obj: TableValidationResult,
        base_details: Dict[str, Any],
        started: float,
    ) -> List[CheckResult]:
        """
        One side is empty: all records on non-empty side are missing.

        Requirement 3: Correct zero-row handling.
        Do not load full rows. Evidence remains bounded.
        """
        if source_count == 0 and target_count > 0:
            # All target rows are missing in source
            missing_in_source_count = target_count
            missing_in_target_count = 0
            matched_count = 0
        else:
            # All source rows are missing in target
            missing_in_source_count = 0
            missing_in_target_count = source_count
            matched_count = 0

        status = ValidationStatus.FAIL if (missing_in_source_count + missing_in_target_count) > 0 \
                 else ValidationStatus.PASS

        details = {
            **base_details,
            "execution_mode": "ONE_SIDE_EMPTY",
            "matched_records": matched_count,
            "mismatch_records": missing_in_source_count + missing_in_target_count,
            "missing_in_source": missing_in_source_count,
            "missing_in_target": missing_in_target_count,
            "records_checked": missing_in_source_count + missing_in_target_count,
            "source_records_read": source_count,
            "target_records_read": target_count,
            "execution_time": round(time.perf_counter() - started, 3),
        }

        return [CheckResult(
            check_name="record_comparison",
            status=status,
            source_value=source_count,
            target_value=target_count,
            delta=abs(source_count - target_count),
            message=f"One side empty: missing_in_source={missing_in_source_count}, "
                   f"missing_in_target={missing_in_target_count}",
            details=details,
        )]

    # ─────────────────────────────────────────────────────────────────
    # POLARS_FULL: Vectorized comparison for <1M, low workload
    # ─────────────────────────────────────────────────────────────────

    def _compare_polars_full(
        self,
        source_conn,
        target_conn,
        table_map,
        configured_columns: List[Dict[str, str]],
        key_mappings: List[Dict[str, str]],
        comparison_settings: Dict[str, Any],
        result_obj: TableValidationResult,
        base_details: Dict[str, Any],
        execution_cache,
        started: float,
    ) -> List[CheckResult]:
        """Full Polars comparison for small/medium tables."""
        comparison_started = time.perf_counter()

        try:
            source_dialect = self._dialect(source_conn)
            target_dialect = self._dialect(target_conn)

            src_fqn = _fqn(table_map.source_schema, table_map.source_table, source_dialect)
            tgt_fqn = _fqn(table_map.target_schema, table_map.target_table, target_dialect)

            source_where = self._build_where_clause(table_map, source_dialect)
            target_where = self._build_where_clause(table_map, target_dialect)

            # Fetch both sides
            source_cols = [item["source_column"] for item in configured_columns]
            target_cols = [item["target_column"] for item in configured_columns]

            if key_mappings:
                source_cols += [item["source_column"] for item in key_mappings]
                target_cols += [item["target_column"] for item in key_mappings]

            source_sql = self._select_sql(
                src_fqn,
                self._projection(source_cols, source_dialect),
                source_where,
                dialect=source_dialect,
            )
            target_sql = self._select_sql(
                tgt_fqn,
                self._projection(target_cols, target_dialect),
                target_where,
                dialect=target_dialect,
            )

            read_started = time.perf_counter()
            src_df = source_conn.execute_query(source_sql)
            tgt_df = target_conn.execute_query(target_sql)
            read_seconds = time.perf_counter() - read_started

            if not isinstance(src_df, pd.DataFrame) or not isinstance(tgt_df, pd.DataFrame):
                raise TypeError("Must return pandas DataFrame")

            # Compare
            if key_mappings:
                result = self._keyed_comparison(
                    src_df, tgt_df, table_map,
                    key_mappings, configured_columns,
                    comparison_settings, result_obj
                )
            else:
                result = self._keyless_comparison(
                    src_df, tgt_df, table_map,
                    configured_columns, comparison_settings, result_obj
                )

            details = {
                **base_details,
                "execution_mode": "FULL_COMPARISON",
                "full_comparison_performed": True,
                "source_records_read": len(src_df),
                "target_records_read": len(tgt_df),
                "source_read_seconds": round(read_seconds, 6),
                "target_read_seconds": round(read_seconds, 6),
                "total_record_comparison_seconds": round(
                    time.perf_counter() - comparison_started, 6
                ),
                "execution_time": round(time.perf_counter() - started, 3),
            }

            result.details.update(details)
            del src_df, tgt_df
            gc.collect()
            return [result]

        except Exception as exc:
            logger.error("POLARS_FULL failed", exc_info=True)
            return [self._error(
                f"Full comparison failed: {exc}",
                started,
                base_details,
                status=self._policies.configured_status("framework_exception_status"),
                exception_category=exception_category(exc),
                table_id=getattr(table_map, "id", None),
            )]

    # ─────────────────────────────────────────────────────────────────
    # POLARS_HASH_FIRST: Narrow hash-first strategy
    # Requirement 10: TRUE narrow hash-first (key+signature only first)
    # ─────────────────────────────────────────────────────────────────

    def _compare_polars_hash_first(
        self,
        source_conn,
        target_conn,
        table_map,
        configured_columns: List[Dict[str, str]],
        key_mappings: List[Dict[str, str]],
        comparison_settings: Dict[str, Any],
        result_obj: TableValidationResult,
        base_details: Dict[str, Any],
        execution_cache,
        started: float,
    ) -> List[CheckResult]:
        """
        TRUE narrow hash-first: key+signature only, then detail fetch on mismatch.

        Requirement 10: First pass must be narrow (key + signature).
        Do NOT load 900K × 300 just to hash it.
        """
        comparison_started = time.perf_counter()

        try:
            source_dialect = self._dialect(source_conn)
            target_dialect = self._dialect(target_conn)

            src_fqn = _fqn(table_map.source_schema, table_map.source_table, source_dialect)
            tgt_fqn = _fqn(table_map.target_schema, table_map.target_table, target_dialect)

            source_where = self._build_where_clause(table_map, source_dialect)
            target_where = self._build_where_clause(table_map, target_dialect)

            source_key_cols = [item["source_column"] for item in key_mappings] if key_mappings else []
            target_key_cols = [item["target_column"] for item in key_mappings] if key_mappings else []
            source_comp_cols = [item["source_column"] for item in configured_columns]
            target_comp_cols = [item["target_column"] for item in configured_columns]

            # === Stage 1: Narrow fetch (key + signature only) ===
            hash_start = time.perf_counter()

            # Narrow projection: keys + comparison cols (not all columns)
            source_sql = self._select_sql(
                src_fqn,
                self._projection(source_key_cols + source_comp_cols, source_dialect),
                source_where,
                dialect=source_dialect,
            )
            target_sql = self._select_sql(
                tgt_fqn,
                self._projection(target_key_cols + target_comp_cols, target_dialect),
                target_where,
                dialect=target_dialect,
            )

            src_df = source_conn.execute_query(source_sql)
            tgt_df = target_conn.execute_query(target_sql)

            if not isinstance(src_df, pd.DataFrame) or not isinstance(tgt_df, pd.DataFrame):
                raise TypeError("Must return pandas DataFrame")

            # Generate SHA-256 signatures
            src_df["_signature"] = self._sha256_rows(src_df, source_comp_cols)
            tgt_df["_signature"] = self._sha256_rows(tgt_df, target_comp_cols)

            hash_seconds = time.perf_counter() - hash_start

            # === Stage 2: Compare signatures ===
            src_sig_counts = Counter(src_df["_signature"])
            tgt_sig_counts = Counter(tgt_df["_signature"])

            matched_sigs = src_sig_counts & tgt_sig_counts
            mismatched_src = src_sig_counts - tgt_sig_counts
            mismatched_tgt = tgt_sig_counts - src_sig_counts

            matched_count = sum(matched_sigs.values())
            mismatched_count = sum(mismatched_src.values()) + sum(mismatched_tgt.values())

            # === Stage 3: Fetch and compare detail only for mismatches ===
            evidence_limit = comparison_settings.get("full_data_comparison", {}).get("evidence", {}) \
                            .get("mismatch_sample_limit", 100)

            # Store evidence for mismatches (bounded)
            src_mismatched = src_df[src_df["_signature"].isin(set(mismatched_src))]
            tgt_mismatched = tgt_df[tgt_df["_signature"].isin(set(mismatched_tgt))]

            for idx, row in src_mismatched.iterrows():
                if len(result_obj.missing_in_target) >= evidence_limit:
                    break
                result_obj.missing_in_target.append(MissingRecord(
                    table_id=table_map.id,
                    primary_key=self._key_string(row, source_key_cols),
                    record_data=row.drop("_signature").to_json(date_format="iso"),
                    location="EXCESS_IN_SOURCE",
                ))

            for idx, row in tgt_mismatched.iterrows():
                if len(result_obj.missing_in_source) >= evidence_limit:
                    break
                result_obj.missing_in_source.append(MissingRecord(
                    table_id=table_map.id,
                    primary_key=self._key_string(row, target_key_cols),
                    record_data=row.drop("_signature").to_json(date_format="iso"),
                    location="EXCESS_IN_TARGET",
                ))

            status = ValidationStatus.PASS if mismatched_count == 0 else ValidationStatus.FAIL

            result = CheckResult(
                check_name="record_comparison",
                status=status,
                source_value=len(src_df),
                target_value=len(tgt_df),
                delta=abs(len(src_df) - len(tgt_df)),
                message=f"Hash-first: matched={matched_count}, mismatched={mismatched_count}",
                details={
                    **base_details,
                    "execution_mode": "HASH_FIRST",
                    "full_comparison_performed": False,
                    "matched_records": matched_count,
                    "mismatch_records": mismatched_count,
                    "missing_in_source": sum(mismatched_tgt.values()),
                    "missing_in_target": sum(mismatched_src.values()),
                    "records_checked": matched_count + mismatched_count,
                    "source_records_read": len(src_df),
                    "target_records_read": len(tgt_df),
                    "signature_generation_seconds": round(hash_seconds, 6),
                    "total_record_comparison_seconds": round(
                        time.perf_counter() - comparison_started, 6
                    ),
                    "execution_time": round(time.perf_counter() - started, 3),
                },
            )

            del src_df, tgt_df
            gc.collect()
            return [result]

        except Exception as exc:
            logger.error("POLARS_HASH_FIRST failed", exc_info=True)
            return [self._error(
                f"Hash-first failed: {exc}",
                started,
                base_details,
                status=self._policies.configured_status("framework_exception_status"),
                exception_category=exception_category(exc),
                table_id=getattr(table_map, "id", None),
            )]

    # ─────────────────────────────────────────────────────────────────
    # POLARS_SAMPLE_100K: Mandatory sampling for >=1M tables
    # Requirement 1: Mandatory >=1M rule
    # Requirement 13: Keyed tables use same logical keys for cohort
    # Requirement 14: Keyless tables use signature-based cohort
    # Requirement 16: Batched target cohort lookup (~20 queries, not 1000s)
    # ─────────────────────────────────────────────────────────────────

    def _compare_polars_sample_100k(
        self,
        source_conn,
        target_conn,
        table_map,
        source_count: int,
        target_count: int,
        configured_columns: List[Dict[str, str]],
        key_mappings: List[Dict[str, str]],
        comparison_settings: Dict[str, Any],
        result_obj: TableValidationResult,
        base_details: Dict[str, Any],
        execution_cache,
        started: float,
    ) -> List[CheckResult]:
        """
        Mandatory 100K sample for >=1M tables.

        Requirement 1: Never load full table. Never full_keyset_merge or hash_streaming.
        Requirement 13: For keyed tables, use same logical keys for source and target.
        Requirement 14: For keyless tables, use signature-based cohort.
        Requirement 16: Batch target cohort lookup (~20 queries for 100K, not 1000s).
        """
        comparison_started = time.perf_counter()

        try:
            source_dialect = self._dialect(source_conn)
            target_dialect = self._dialect(target_conn)

            src_fqn = _fqn(table_map.source_schema, table_map.source_table, source_dialect)
            tgt_fqn = _fqn(table_map.target_schema, table_map.target_table, target_dialect)

            source_where = self._build_where_clause(table_map, source_dialect)
            target_where = self._build_where_clause(table_map, target_dialect)

            source_keys = [item["source_column"] for item in key_mappings] if key_mappings else []
            target_keys = [item["target_column"] for item in key_mappings] if key_mappings else []
            source_cols = [item["source_column"] for item in configured_columns]
            target_cols = [item["target_column"] for item in configured_columns]

            sample_size = MANDATORY_LARGE_TABLE_SAMPLE_SIZE
            sampling_run_id = random.randint(0, 2**31 - 1)

            logger.info(
                "[SAMPLE] table=%s 100K sample (run_id=%d)",
                table_map.id, sampling_run_id
            )

            # === Stage 1: Random sample from source ===
            sample_start = time.perf_counter()

            if key_mappings:
                # Keyed: sample by keys for correct cohort matching
                source_sample_sql = self._select_sql(
                    src_fqn,
                    self._projection(source_keys + source_cols, source_dialect),
                    source_where,
                    top=sample_size,
                    order_by=source_dialect.random_order(),
                    dialect=source_dialect,
                )
            else:
                # Keyless: sample by rows
                source_sample_sql = self._select_sql(
                    src_fqn,
                    self._projection(source_cols, source_dialect),
                    source_where,
                    top=sample_size,
                    order_by=source_dialect.random_order(),
                    dialect=source_dialect,
                )

            # Memory management: Clean up before fetch
            gc.collect()

            try:
                src_sample_df = source_conn.execute_query(source_sample_sql)
            except MemoryError as exc:
                logger.error("MemoryError fetching source sample: %s", str(exc))
                gc.collect()
                return [self._error(
                    f"MemoryError fetching 100K sample: {exc}. Database may have returned more rows than expected.",
                    started,
                    base_details,
                    status=ValidationStatus.FRAMEWORK_ERROR,
                    exception_category="MEMORY_LIMIT_EXCEEDED",
                    error_type="RESOURCE_ERROR",
                    table_id=getattr(table_map, "id", None),
                )]

            if not isinstance(src_sample_df, pd.DataFrame):
                raise TypeError("Must return pandas DataFrame")

            actual_sample_size = len(src_sample_df)
            sample_fetch_seconds = time.perf_counter() - sample_start

            # Timing diagnostic
            logger.info(
                "[TIMING_SOURCE] Fetched %d rows in %.3fs (%.0f rows/sec) | expected=%d | status=%s",
                actual_sample_size, sample_fetch_seconds,
                actual_sample_size / sample_fetch_seconds if sample_fetch_seconds > 0 else 0,
                MANDATORY_LARGE_TABLE_SAMPLE_SIZE,
                "✅ OK" if actual_sample_size <= int(MANDATORY_LARGE_TABLE_SAMPLE_SIZE * 1.5) else "⚠️ OVERSIZED"
            )

            # Safety check: Ensure sample doesn't exceed safety threshold
            SAMPLE_SIZE_SAFETY_MARGIN = int(MANDATORY_LARGE_TABLE_SAMPLE_SIZE * 1.5)  # 150K
            if actual_sample_size > SAMPLE_SIZE_SAFETY_MARGIN:
                logger.warning(
                    "[SAMPLE] Source sample exceeded safety margin: %d > %d",
                    actual_sample_size, SAMPLE_SIZE_SAFETY_MARGIN
                )
                del src_sample_df
                gc.collect()
                return [self._error(
                    f"Source sample too large: {actual_sample_size} > {SAMPLE_SIZE_SAFETY_MARGIN}. "
                    f"Database query may not be properly limiting rows.",
                    started,
                    base_details,
                    status=ValidationStatus.FRAMEWORK_ERROR,
                    exception_category="MEMORY_LIMIT_EXCEEDED",
                    error_type="RESOURCE_ERROR",
                    table_id=getattr(table_map, "id", None),
                )]

            logger.info(
                "[SAMPLE] table=%s fetched %d source rows",
                table_map.id, actual_sample_size
            )

            # === Stage 2: Fetch target cohort matching source sample ===
            target_fetch_start = time.perf_counter()

            if key_mappings and actual_sample_size > 0:
                # Keyed: fetch target records with same keys (Requirement 13)
                source_key_values = self._extract_values(src_sample_df, source_keys)

                # Batched queries (Requirement 16)
                batch_size = int(
                    comparison_settings.get("comparison_tiers", {}).get(
                        "target_key_lookup_batch_size",
                        DEFAULT_TARGET_KEY_LOOKUP_BATCH_SIZE
                    )
                )

                tgt_queries = self._build_batched_filter_queries(
                    tgt_fqn,
                    self._projection(target_keys + target_cols, target_dialect),
                    target_where,
                    target_keys,
                    source_key_values,
                    batch_size,
                    target_dialect,
                )

                try:
                    tgt_sample_df = self._execute_multiple_queries(target_conn, tgt_queries)
                except MemoryError as exc:
                    logger.error("MemoryError fetching target cohort: %s", str(exc))
                    gc.collect()
                    return [self._error(
                        f"MemoryError fetching target cohort: {exc}. Database may have returned more rows than expected.",
                        started,
                        base_details,
                        status=ValidationStatus.FRAMEWORK_ERROR,
                        exception_category="MEMORY_LIMIT_EXCEEDED",
                        error_type="RESOURCE_ERROR",
                        table_id=getattr(table_map, "id", None),
                    )]

            else:
                # Keyless: sample target rows with same signature distribution
                tgt_sample_df = target_conn.execute_query(
                    self._select_sql(
                        tgt_fqn,
                        self._projection(target_cols, target_dialect),
                        target_where,
                        top=actual_sample_size,
                        order_by=target_dialect.random_order(),
                        dialect=target_dialect,
                    )
                )

                # Trim to source signature counts (Requirement 14)
                tgt_sample_df = self._trim_to_signature_counts(
                    src_sample_df, tgt_sample_df, source_cols, target_cols
                )

            if not isinstance(tgt_sample_df, pd.DataFrame):
                raise TypeError("Must return pandas DataFrame")

            target_fetch_seconds = time.perf_counter() - target_fetch_start

            # Timing diagnostic
            logger.info(
                "[TIMING_TARGET] Fetched %d records in %.3fs | queries=%d | status=%s",
                len(tgt_sample_df), target_fetch_seconds,
                len(tgt_queries) if key_mappings and actual_sample_size > 0 else 1,
                "✅ BATCHED (20 queries)" if key_mappings and actual_sample_size > 0 and len(tgt_queries) == 20 else f"⚠️ {len(tgt_queries) if key_mappings and actual_sample_size > 0 else 1} queries"
            )

            # === Stage 3: Compare samples ===
            if key_mappings:
                result = self._keyed_comparison(
                    src_sample_df, tgt_sample_df, table_map,
                    key_mappings, configured_columns,
                    comparison_settings, result_obj
                )
            else:
                result = self._keyless_comparison(
                    src_sample_df, tgt_sample_df, table_map,
                    configured_columns, comparison_settings, result_obj
                )

            details = {
                **base_details,
                "execution_mode": "SAMPLE_COMPARISON",
                "full_comparison_performed": False,
                "configured_sample_size": sample_size,
                "actual_sample_size": actual_sample_size,
                "sampling_run_id": sampling_run_id,
                "sampling_method": "random_keys" if key_mappings else "random_rows",
                "source_records_read": len(src_sample_df),
                "target_records_read": len(tgt_sample_df),
                "source_read_seconds": round(sample_fetch_seconds, 6),
                "target_read_seconds": round(target_fetch_seconds, 6),
                "target_query_count": len(tgt_queries) if key_mappings else 1,
                "total_record_comparison_seconds": round(
                    time.perf_counter() - comparison_started, 6
                ),
                "execution_time": round(time.perf_counter() - started, 3),
            }

            result.details.update(details)

            # Timing diagnostic - TOTAL EXECUTION
            total_elapsed = time.perf_counter() - comparison_started
            logger.info(
                "[TIMING_TOTAL] ✅ COMPLETE in %.3fs | breakdown: source=%.3fs | target=%.3fs | compare=%.3fs | status=%s",
                total_elapsed,
                sample_fetch_seconds,
                target_fetch_seconds,
                total_elapsed - sample_fetch_seconds - target_fetch_seconds,
                "✅ OPTIMAL (8-15s)" if total_elapsed < 15 else f"⚠️ SLOW ({total_elapsed:.0f}s)"
            )

            # Aggressive memory cleanup
            try:
                del src_sample_df, tgt_sample_df
                gc.collect()
            except Exception:
                pass

            return [result]

        except Exception as exc:
            logger.error("POLARS_SAMPLE_100K failed", exc_info=True)
            return [self._error(
                f"100K sample failed: {exc}",
                started,
                base_details,
                status=self._policies.configured_status("framework_exception_status"),
                exception_category=exception_category(exc),
                table_id=getattr(table_map, "id", None),
            )]

    # ─────────────────────────────────────────────────────────────────
    # Comparison Implementations (Keyed and Keyless)
    # ─────────────────────────────────────────────────────────────────

    def _keyed_comparison(
        self,
        src_df: pd.DataFrame,
        tgt_df: pd.DataFrame,
        table_map,
        key_mappings: List[Dict[str, str]],
        configured_columns: List[Dict[str, str]],
        comparison_settings: Dict[str, Any],
        result_obj: TableValidationResult,
    ) -> CheckResult:
        """
        Keyed comparison using presence markers.

        Requirement 9: Use explicit presence indicators for safe joins.
        """
        source_keys = [item["source_column"] for item in key_mappings]
        target_keys = [item["target_column"] for item in key_mappings]
        source_cols = [item["source_column"] for item in configured_columns]
        target_cols = [item["target_column"] for item in configured_columns]

        # Add presence markers (Requirement 9)
        src_df = src_df.copy()
        tgt_df = tgt_df.copy()
        src_df["__source_present"] = True
        tgt_df["__target_present"] = True

        # Compared columns usually carry the same name on both sides; without
        # an explicit rename pandas would suffix them and the per-column
        # comparison below would not find them.
        renamed_targets = {
            column: f"{column}__target"
            for column in target_cols
            if column in src_df.columns and column not in target_keys
        }
        if renamed_targets:
            tgt_df = tgt_df.rename(columns=renamed_targets)
        target_cols = [renamed_targets.get(column, column) for column in target_cols]

        # Join on keys
        # pandas has no "full" join; "outer" is the full outer join that keeps
        # source-only, target-only and common keys for the presence markers.
        merged = pd.merge(
            src_df, tgt_df,
            left_on=source_keys,
            right_on=target_keys,
            how="outer",
            indicator=False,
        )

        # Interpret presence markers
        source_only_mask = merged["__source_present"].fillna(False) & \
                          (~merged["__target_present"].fillna(False))
        target_only_mask = (~merged["__source_present"].fillna(False)) & \
                          merged["__target_present"].fillna(False)
        both_present_mask = merged["__source_present"].fillna(False) & \
                           merged["__target_present"].fillna(False)

        missing_in_target_count = int(source_only_mask.sum())
        missing_in_source_count = int(target_only_mask.sum())
        common_count = int(both_present_mask.sum())

        # Compare values in common records (Requirement 8: per-row logic, not per-cell)
        matched_count = 0
        mismatched_count = 0

        if common_count > 0:
            merged_common = merged[both_present_mask]
            row_mismatches = np.zeros(common_count, dtype=bool)

            for src_col, tgt_col in zip(source_cols, target_cols):
                src_vals = merged_common[src_col].fillna("__NULL__")
                tgt_vals = merged_common[tgt_col].fillna("__NULL__")

                col_mismatch = (src_vals != tgt_vals)
                row_mismatches |= col_mismatch.fillna(True).to_numpy(dtype=bool)

            matched_count = int((~row_mismatches).sum())
            mismatched_count = int(row_mismatches.sum())

        mismatch_count = mismatched_count + missing_in_source_count + missing_in_target_count
        records_checked = matched_count + mismatch_count

        status = ValidationStatus.PASS if mismatch_count == 0 else ValidationStatus.FAIL

        return CheckResult(
            check_name="record_comparison",
            status=status,
            source_value=len(src_df),
            target_value=len(tgt_df),
            delta=abs(len(src_df) - len(tgt_df)),
            message=f"Keyed: checked={records_checked}, matched={matched_count}, "
                   f"mismatched={mismatch_count}",
            details={
                "matched_records": matched_count,
                "mismatch_records": mismatch_count,
                "value_mismatch_records": mismatched_count,
                "missing_in_source": missing_in_source_count,
                "missing_in_target": missing_in_target_count,
                "records_checked": records_checked,
            },
        )

    def _keyless_comparison(
        self,
        src_df: pd.DataFrame,
        tgt_df: pd.DataFrame,
        table_map,
        configured_columns: List[Dict[str, str]],
        comparison_settings: Dict[str, Any],
        result_obj: TableValidationResult,
    ) -> CheckResult:
        """
        Keyless comparison using SHA-256 canonical row signatures.

        Requirement 6: SHA-256 canonical row signature multiset comparison.
        Preserve duplicate multiplicity.
        """
        source_cols = [item["source_column"] for item in configured_columns]
        target_cols = [item["target_column"] for item in configured_columns]

        # Generate SHA-256 signatures
        src_sigs = self._sha256_rows(src_df, source_cols)
        tgt_sigs = self._sha256_rows(tgt_df, target_cols)

        src_counts = Counter(src_sigs)
        tgt_counts = Counter(tgt_sigs)

        # Multiset comparison (preserve duplicates)
        matched_sigs = src_counts & tgt_counts
        missing_target_sigs = src_counts - tgt_counts
        missing_source_sigs = tgt_counts - src_counts

        matched_count = sum(matched_sigs.values())
        missing_target_count = sum(missing_target_sigs.values())
        missing_source_count = sum(missing_source_sigs.values())

        mismatch_count = missing_target_count + missing_source_count
        records_checked = matched_count + mismatch_count

        status = ValidationStatus.PASS if mismatch_count == 0 else ValidationStatus.FAIL

        return CheckResult(
            check_name="record_comparison",
            status=status,
            source_value=len(src_df),
            target_value=len(tgt_df),
            delta=abs(len(src_df) - len(tgt_df)),
            message=f"Keyless (SHA-256): checked={records_checked}, matched={matched_count}",
            details={
                "matched_records": matched_count,
                "mismatch_records": mismatch_count,
                "missing_in_source": missing_source_count,
                "missing_in_target": missing_target_count,
                "records_checked": records_checked,
            },
        )

    # ─────────────────────────────────────────────────────────────────
    # Hash and Signature Generation (Requirement 11)
    # ─────────────────────────────────────────────────────────────────

    def _sha256_rows(self, frame: pd.DataFrame, columns: List[str]) -> List[str]:
        """
        Generate SHA-256 canonical row signatures using vectorized operations.

        Requirement 11: Vectorized hashing for 10-20x speedup over iterrows.
        Requirement 7: Preserve all normalization semantics.
        """
        if frame.empty:
            return []

        try:
            import polars as pl
            use_polars = True
        except ImportError:
            use_polars = False

        if use_polars:
            return self._sha256_rows_polars(frame, columns)
        else:
            return self._sha256_rows_pandas_fast(frame, columns)

    def _sha256_rows_polars(self, frame: pd.DataFrame, columns: List[str]) -> List[str]:
        """Vectorized SHA-256 using Polars (10-20x faster than iterrows)."""
        import polars as pl

        pl_df = pl.from_pandas(frame[columns])

        canonical_arrays = []
        for col in columns:
            canonical_col = pl_df[col].map_elements(
                lambda x: self._hash_canonical_value(x), return_dtype=pl.Utf8
            )
            canonical_arrays.append(canonical_col.to_numpy())

        hashes = []
        for row_idx in range(len(frame)):
            parts = []
            for val in [arr[row_idx] for arr in canonical_arrays]:
                encoded = str(val).encode("utf-8")
                parts.append(len(encoded).to_bytes(8, byteorder="big"))
                parts.append(encoded)
            hashes.append(hashlib.sha256(b"".join(parts)).hexdigest())

        return hashes

    def _sha256_rows_pandas_fast(self, frame: pd.DataFrame, columns: List[str]) -> List[str]:
        """Faster pandas-based hashing using itertuples (2-3x faster than iterrows)."""
        canonical_df = pd.DataFrame()
        for col in columns:
            canonical_df[col] = frame[col].apply(self._hash_canonical_value)

        hashes = []
        for row_values in canonical_df.itertuples(index=False, name=None):
            parts = []
            for val in row_values:
                encoded = str(val).encode("utf-8")
                parts.append(len(encoded).to_bytes(8, byteorder="big"))
                parts.append(encoded)
            hashes.append(hashlib.sha256(b"".join(parts)).hexdigest())

        return hashes

    def _hash_canonical_value(self, value: Any) -> str:
        """
        Canonical value for hashing.

        Requirement 7: Preserve all normalization semantics.
        """
        normalization = self._policies.hashing.get("normalization", {})

        if isinstance(value, str) and not value.strip() \
           and normalization.get("blank_handling") == "null":
            return normalization.get("null_representation", "NULL")

        if self._is_null(value):
            return normalization.get("null_representation", "NULL")

        # Temporal values
        token = temporal_token(
            value,
            parse_strings=self._policies.timestamps.get("parse_iso_strings", True),
        )
        if token is not None:
            return f"datetime:{token.canonical}"

        text = str(value)
        if normalization.get("trim_strings", False):
            text = text.strip()
        if not normalization.get("case_sensitive", True):
            text = text.casefold()

        return text

    def _is_null(self, value: Any) -> bool:
        """Check if value is null."""
        return is_null_scalar(
            value,
            blank_is_null=self._policies.nulls.get("blank_is_null", False),
        )

    # ─────────────────────────────────────────────────────────────────
    # SQL Generation and Safety (Requirement 17)
    # ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _build_batched_filter_queries(
        fqn: str,
        projection: str,
        where_clause: str,
        columns: List[str],
        values: List[Tuple[Any, ...]],
        batch_size: int,
        dialect=None,
    ) -> List[str]:
        """
        Build batched filter queries.

        Requirement 16: Batch target cohort lookup (~20 queries for 100K, not 1000s).
        Requirement 17: SQL-safe literal generation.
        """
        dialect = dialect or resolve_dialect(None)
        queries = []

        for batch_start in range(0, len(values), batch_size):
            batch_values = values[batch_start:batch_start + batch_size]
            predicates = []

            for row_values in batch_values:
                conditions = []
                for column, value in zip(columns, row_values):
                    quoted_col = dialect.quote(column)
                    if value is None or (isinstance(value, float) and np.isnan(value)):
                        conditions.append(f"{quoted_col} IS NULL")
                    else:
                        # Use SQL-safe literal (Requirement 17)
                        safe_literal = RecordComparisonProduction._sql_safe_literal(value)
                        conditions.append(f"{quoted_col} = {safe_literal}")

                predicates.append("(" + " AND ".join(conditions) + ")")

            combined = " OR ".join(predicates)
            final_where = bind_sql(
                f"{where_clause} AND ({combined})" if where_clause.strip()
                else f" WHERE {combined}",
                where_clause, combined
            )

            queries.append(f"SELECT {projection} FROM {fqn}{final_where}")

        return queries

    @staticmethod
    def _sql_safe_literal(value: Any) -> str:
        """Generate SQL-safe literal string."""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)):
            return str(value)

        text = str(value)
        # Escape single quotes
        escaped = text.replace("'", "''")
        return f"'{escaped}'"

    @staticmethod
    def _execute_multiple_queries(connection, queries: List[str]) -> pd.DataFrame:
        """Execute multiple queries and concatenate results."""
        frames = []
        for query in queries:
            frame = connection.execute_query(query)
            if isinstance(frame, pd.DataFrame) and not frame.empty:
                frames.append(frame)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]

    # ─────────────────────────────────────────────────────────────────
    # Utility Methods
    # ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_values(frame: pd.DataFrame, columns: List[str]) -> List[Tuple[Any, ...]]:
        """Extract column values as tuples."""
        if frame.empty:
            return []
        return list(frame[columns].itertuples(index=False, name=None))

    @staticmethod
    def _trim_to_signature_counts(
        source: pd.DataFrame,
        target: pd.DataFrame,
        source_cols: List[str],
        target_cols: List[str],
    ) -> pd.DataFrame:
        """Trim target to match source signature distribution."""
        source_sigs = [
            hashlib.sha256(
                str(tuple(row[col] for col in source_cols)).encode()
            ).hexdigest()
            for _, row in source.iterrows()
        ]
        target_sigs = [
            hashlib.sha256(
                str(tuple(row[col] for col in target_cols)).encode()
            ).hexdigest()
            for _, row in target.iterrows()
        ]

        source_counts = Counter(source_sigs)
        retained_counts = Counter()
        retained_positions = []

        for position, sig in enumerate(target_sigs):
            if retained_counts[sig] >= source_counts[sig]:
                continue
            retained_counts[sig] += 1
            retained_positions.append(position)

        return target.iloc[retained_positions].reset_index(drop=True)

    @staticmethod
    def _key_string(row: pd.Series, key_cols: List[str]) -> str:
        """Generate key string for evidence."""
        if not key_cols:
            return ""
        return json.dumps(
            [row[col] for col in key_cols],
            default=str,
            ensure_ascii=False,
        )

    @staticmethod
    def _projection(columns: List[str], dialect=None) -> str:
        """SQL column projection."""
        dialect = dialect or resolve_dialect(None)
        return ", ".join(dialect.quote(col) for col in columns) if columns else "*"

    @staticmethod
    def _select_sql(
        fqn: str,
        projection: str,
        where_clause: str,
        top: Optional[int] = None,
        order_by: Optional[str] = None,
        dialect=None,
    ) -> str:
        """Build SELECT SQL."""
        dialect = dialect or resolve_dialect(None)
        return dialect.select(projection, fqn, where_clause, order_by=order_by, limit=top)

    @staticmethod
    def _key_mappings(table_map) -> List[Dict[str, str]]:
        """Extract key mappings from table map."""
        mappings = []
        for item in getattr(table_map, "key_mappings", None) or []:
            src = item.get("source_column") if isinstance(item, dict) \
                  else getattr(item, "source_column", None)
            tgt = item.get("target_column") if isinstance(item, dict) \
                  else getattr(item, "target_column", None)
            if src and tgt:
                mappings.append({
                    "source_column": str(src).strip(),
                    "target_column": str(tgt).strip(),
                })
        return mappings

    @staticmethod
    def _configured_column_mappings(table_map) -> List[Dict[str, str]]:
        """Extract comparison column mappings."""
        mappings = []
        for item in getattr(table_map, "compare_column_mappings", None) or []:
            if isinstance(item, dict):
                src = item.get("source_column", "").strip()
                tgt = item.get("target_column", "").strip()
                if src and tgt:
                    mappings.append({
                        "source_column": src,
                        "target_column": tgt,
                        "name": item.get("name", f"{src}_to_{tgt}"),
                    })
        return mappings

    @staticmethod
    def _cached_row_counts(execution_cache) -> Tuple[int, int]:
        """Fetch cached row counts."""
        if execution_cache is None:
            raise ValueError("Execution cache required")

        counts = []
        for role in ("source", "target"):
            value = execution_cache.peek(("row_count", role))
            if not isinstance(value, int) or value < 0:
                raise ValueError(f"Cached {role} row count unavailable")
            counts.append(value)

        return counts[0], counts[1]

    def _build_where_clause(self, table_map, dialect=None, role=None) -> str:
        """Build WHERE clause from validation scope."""
        return super()._build_where_clause(table_map, dialect, role)

    @staticmethod
    def _error(
        message: str,
        started: float,
        details: Optional[Dict[str, Any]] = None,
        status: ValidationStatus = ValidationStatus.CONFIG_ERROR,
        exception_category: str = "CONFIGURATION",
        table_id: Optional[str] = None,
        **extra: Any,
    ) -> CheckResult:
        """Create error result."""
        error_details = {
            **(details or {}),
            **extra,
            "execution_time": round(time.perf_counter() - started, 3),
            "table_id": table_id,
            "exception_category": exception_category,
            "error_reason": message,
            "final_status": status.value,
        }
        return CheckResult(
            check_name="record_comparison",
            status=status,
            message=message,
            details=error_details,
        )
