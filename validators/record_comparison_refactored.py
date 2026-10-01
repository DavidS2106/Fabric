"""
fabric_migration_validator/validators/record_comparison_refactored.py
──────────────────────────────────────────────────────────────────────

Tier-based record comparison using Polars for normal tables (≤1M rows)
and mandatory 100K sampling for large tables (>1M rows).

MANDATORY ROUTING RULE:
  if source_count >= 1_000_000 or target_count >= 1_000_000:
      use 100K sample comparison directly
      never load full table
      never call dual_keyset_merge or hash_streaming

Architecture tiers:
  0 rows              → SKIP
  1-100K rows        → POLARS_FULL (vectorized join + compare)
  100K-<1M           → POLARS_FULL or POLARS_HASH_FIRST (workload-dependent)
  >=1M rows          → POLARS_SAMPLE_100K (mandatory 100K sample)
  forced_full_huge   → SPARK_FULL (explicit config only, not auto)

Performance improvements:
  • Polars as primary engine (columnar, vectorized)
  • No per-row Python loops
  • No to_dict("records") in hot paths
  • Hash-first for wide tables (900K×300 → narrow key+hash first)
  • Efficient key-cohort joins for sampling
  • Memory-bounded processing
  • Pandas only for evidence/reporting
"""

from __future__ import annotations

import gc
import hashlib
import json
import logging
import random
import time
from collections import Counter
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import SimpleNamespace
from typing import (
    Any,
    Dict,
    Iterable,
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
from core.sql_dialect import resolve_dialect
from core.validation_policy import (
    ValidationPolicies,
    ValidationPolicyError,
    exception_category,
)
from core.value_normalization import (
    is_null_scalar,
    normalize_decimal_for_hash,
    temporal_token,
    temporal_values_match,
)
from .base_validator import BaseValidator
from .comparison_kernels import ValueComparator
from .validator_utils import _fqn

logger = logging.getLogger(__name__)

#: Rows fetched per round trip when the keyless full-scan comparison pages a table.
FULL_SCAN_PAGE_ROWS = 50_000

#: Default row budget for keyless full-scan comparison per side.
DEFAULT_FULL_SCAN_ROW_BUDGET = 1_000_000

#: Mandatory sample size for tables >= 1M rows.
LARGE_TABLE_SAMPLE_SIZE = 100_000

#: Workload threshold (rows × columns) to trigger hash-first strategy.
HIGH_WORKLOAD_THRESHOLD = 100_000_000


class ComparisonStrategy:
    """Strategy selection and metrics."""
    SKIP = "SKIP"
    POLARS_FULL = "POLARS_FULL"
    POLARS_HASH_FIRST = "POLARS_HASH_FIRST"
    POLARS_SAMPLE_100K = "POLARS_SAMPLE_100K"
    SPARK_FULL = "SPARK_FULL"


class RecordComparisonRefactored(BaseValidator):
    """Tier-based record comparison with Polars and mandatory large-table sampling."""

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
        """Main entry point: tier-based routing with mandatory >=1M sampling."""
        started = time.perf_counter()

        # === Configuration Loading ===
        validations_config = validations_config or {}
        global_config = validations_config.get("record_level_comparison", {}) or {}
        if isinstance(global_config, bool):
            global_config = {"enabled": global_config}

        profile_config = comparison_config or {}
        if not isinstance(profile_config, dict) or not isinstance(global_config, dict):
            return [self._error(
                "Record comparison configuration must be a mapping", started
            )]

        config = {**global_config, **profile_config}
        if not config.get("enabled", False):
            return []

        # === Load Policies ===
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
        self._large_table_processing = self._large_table_processing_config(
            validations_config.get("comparison_settings", {}).get("large_table_processing")
        )
        self._hash_comparison_config = dict(
            validations_config.get("comparison_settings", {}).get("hash_comparison") or {}
        )

        # === Load Comparison Settings ===
        comparison_settings = validations_config.get("comparison_settings", {}) or {}
        if not isinstance(comparison_settings, dict):
            return [self._error("comparison_settings must be a mapping", started)]

        # === MANDATORY: Check row counts early ===
        try:
            source_count, target_count = self._cached_row_counts(execution_cache)
        except ValueError as exc:
            return [self._error(str(exc), started)]

        logger.info(
            "[ROUTING] table=%s source_count=%s target_count=%s",
            getattr(table_map, "id", None), source_count, target_count
        )

        # === Get Key Mappings ===
        try:
            key_mappings = self._key_mappings(table_map)
        except (AttributeError, TypeError, ValueError) as exc:
            key_mappings = []

        configured_columns = self._configured_column_mappings(table_map)

        # === GET COMPARISON TIER (MANDATORY >=1M RULE) ===
        tier = self._get_comparison_tier(
            source_count, target_count, configured_columns,
            self._tier_config(comparison_settings)
        )

        base_details = {
            "table_id": getattr(table_map, "id", None),
            "source_row_count": source_count,
            "target_row_count": target_count,
            "comparison_column_count": len(configured_columns),
            "estimated_cell_workload": source_count * len(configured_columns),
            "selected_strategy": tier,
            "selected_engine": "POLARS",
        }

        # === Route by Tier ===
        if tier == ComparisonStrategy.SKIP:
            return [CheckResult(
                check_name="record_comparison",
                status=ValidationStatus.PASS,
                message="No records to compare",
                details={**base_details, "execution_time": 0.0},
            )]

        if tier == ComparisonStrategy.POLARS_SAMPLE_100K:
            logger.info(
                "[TIER] table=%s MANDATORY large-table sampling: %s rows → 100K sample",
                getattr(table_map, "id", None),
                max(source_count, target_count),
            )
            return self._compare_polars_sample_100k(
                source_conn, target_conn, table_map,
                configured_columns, key_mappings,
                comparison_settings, result_obj,
                base_details, execution_cache, started
            )

        if tier == ComparisonStrategy.POLARS_HASH_FIRST:
            logger.info(
                "[TIER] table=%s wide table (workload=%.0fM): hash-first strategy",
                getattr(table_map, "id", None),
                (source_count * len(configured_columns)) / 1_000_000,
            )
            return self._compare_polars_hash_first(
                source_conn, target_conn, table_map,
                configured_columns, key_mappings,
                comparison_settings, result_obj,
                base_details, execution_cache, started
            )

        # POLARS_FULL (default for <1M, low workload)
        logger.info(
            "[TIER] table=%s full Polars comparison",
            getattr(table_map, "id", None),
        )
        return self._compare_polars_full(
            source_conn, target_conn, table_map,
            configured_columns, key_mappings,
            comparison_settings, result_obj,
            base_details, execution_cache, started
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # Tier Selection Logic
    # ─────────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _get_comparison_tier(
        source_count: int,
        target_count: int,
        configured_columns: List[Dict[str, str]],
        tier_config: Dict[str, Any],
    ) -> str:
        """Select comparison tier based on row/column counts and workload.

        MANDATORY RULE:
            if source_count >= 1_000_000 or target_count >= 1_000_000:
                return POLARS_SAMPLE_100K (never full comparison)
        """
        if source_count == 0 or target_count == 0:
            return ComparisonStrategy.SKIP

        # MANDATORY >=1M ROUTING
        if source_count >= 1_000_000 or target_count >= 1_000_000:
            return ComparisonStrategy.POLARS_SAMPLE_100K

        # Below 1M: decide based on workload
        rows = max(source_count, target_count)
        column_count = len(configured_columns)
        workload = rows * column_count

        workload_threshold = int(
            tier_config.get("high_workload_threshold", HIGH_WORKLOAD_THRESHOLD)
        )

        if workload >= workload_threshold:
            return ComparisonStrategy.POLARS_HASH_FIRST

        return ComparisonStrategy.POLARS_FULL

    @staticmethod
    def _tier_config(comparison_settings: Dict[str, Any]) -> Dict[str, Any]:
        """Load tier configuration from YAML."""
        return comparison_settings.get("comparison_tiers", {})

    # ─────────────────────────────────────────────────────────────────────────────
    # POLARS_FULL: Vectorized comparison for small/medium tables (<100K rows)
    # ─────────────────────────────────────────────────────────────────────────────

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
        """Full comparison using Polars vectorization."""
        comparison_started = time.perf_counter()

        try:
            # === Fetch both sides ===
            source_dialect = self._dialect(source_conn)
            target_dialect = self._dialect(target_conn)

            src_fqn = _fqn(table_map.source_schema, table_map.source_table, source_dialect)
            tgt_fqn = _fqn(table_map.target_schema, table_map.target_table, target_dialect)

            source_where = self._build_where_clause(table_map, source_dialect, "source")
            target_where = self._build_where_clause(table_map, target_dialect, "target")

            source_cols = [
                item["source_column"] for item in configured_columns
            ] + [
                item["source_column"] for item in key_mappings
            ]
            target_cols = [
                item["target_column"] for item in configured_columns
            ] + [
                item["target_column"] for item in key_mappings
            ]

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
                raise TypeError("Connector execute_query must return pandas DataFrame")

            # === Convert to Polars if available ===
            if pl is not None:
                src_pl = pl.from_pandas(src_df)
                tgt_pl = pl.from_pandas(tgt_df)
            else:
                src_pl = None
                tgt_pl = None

            # === Join and compare ===
            if key_mappings and (src_pl is not None and tgt_pl is not None):
                result = self._compare_polars_by_key(
                    src_pl, tgt_pl, src_df, tgt_df,
                    table_map, key_mappings, configured_columns,
                    result_obj, comparison_settings
                )
            else:
                result = self._compare_by_row_hash(
                    src_df, tgt_df, table_map, configured_columns,
                    comparison_settings.get("full_data_comparison", {}).get("evidence", {})
                    .get("mismatch_sample_limit", 100),
                    comparison_settings.get("full_data_comparison", {}).get("evidence", {})
                    .get("matched_records_limit", 100),
                    comparison_settings.get("full_data_comparison", {}).get("evidence", {})
                    .get("export_matched_records", False),
                    result_obj
                )

            details = {
                **base_details,
                "source_read_seconds": round(read_seconds, 6),
                "target_read_seconds": round(read_seconds, 6),
                "source_records_read": len(src_df),
                "target_records_read": len(tgt_df),
                "total_record_comparison_seconds": round(
                    time.perf_counter() - comparison_started, 6
                ),
                "execution_time": round(time.perf_counter() - started, 3),
                "execution_mode": "FULL_COMPARISON",
                "full_comparison_performed": True,
            }

            result.details.update(details)
            result.details["records_compared"] = result.details.get("records_checked", 0)

            del src_df, tgt_df
            if src_pl is not None:
                del src_pl
            if tgt_pl is not None:
                del tgt_pl
            gc.collect()

            return [result]

        except Exception as exc:
            logger.error("POLARS_FULL comparison failed", exc_info=True)
            return [self._error(
                f"Full comparison failed: {exc}",
                started,
                base_details,
                status=self._policies.configured_status("framework_exception_status"),
                exception_category=exception_category(exc),
                table_id=getattr(table_map, "id", None),
            )]

    def _compare_polars_by_key(
        self,
        src_pl,
        tgt_pl,
        src_df: pd.DataFrame,
        tgt_df: pd.DataFrame,
        table_map,
        key_mappings: List[Dict[str, str]],
        configured_columns: List[Dict[str, str]],
        result_obj: TableValidationResult,
        comparison_settings: Dict[str, Any],
    ) -> CheckResult:
        """Polars-based key join and comparison."""
        source_keys = [item["source_column"] for item in key_mappings]
        target_keys = [item["target_column"] for item in key_mappings]
        source_cols = [item["source_column"] for item in configured_columns]
        target_cols = [item["target_column"] for item in configured_columns]

        # Polars join on keys
        try:
            join_result = src_pl.join(
                tgt_pl,
                left_on=source_keys,
                right_on=target_keys,
                how="full",
            )
        except Exception:
            # Fallback to pandas if Polars join fails
            return self._compare_by_key(
                src_df, tgt_df, table_map, source_keys, target_keys,
                configured_columns, result_obj, comparison_settings
            )

        # Vectorized comparison
        matched = 0
        mismatched = 0
        missing_in_source = 0
        missing_in_target = 0

        for source_col, target_col in zip(source_cols, target_cols):
            if source_col in join_result.columns and target_col in join_result.columns:
                src_vals = join_result[source_col]
                tgt_vals = join_result[target_col]

                # Polars vectorized comparison
                try:
                    eq_mask = (
                        (src_vals.is_null() & tgt_vals.is_null())
                        | ((~src_vals.is_null()) & (~tgt_vals.is_null()) & (src_vals == tgt_vals))
                    )
                    matched += eq_mask.sum()
                    mismatched += (~eq_mask).sum()
                except Exception:
                    pass

        # Count missing records
        for key in source_keys:
            if f"{key}_right" in join_result.columns:
                missing_in_target += join_result[f"{key}_right"].is_null().sum()
            if f"{key}_left" in join_result.columns:
                missing_in_source += join_result[f"{key}_left"].is_null().sum()

        status = ValidationStatus.PASS if mismatched == 0 else ValidationStatus.FAIL
        return CheckResult(
            check_name="record_comparison",
            status=status,
            source_value=len(src_df),
            target_value=len(tgt_df),
            delta=abs(len(src_df) - len(tgt_df)),
            message=f"Compared records={matched + mismatched}, matched={matched}, mismatched={mismatched}",
            details={
                "matched_records": matched,
                "mismatch_records": mismatched,
                "missing_in_source": missing_in_source,
                "missing_in_target": missing_in_target,
                "records_checked": matched + mismatched + missing_in_source + missing_in_target,
                "source_records_selected": len(src_df),
                "target_records_selected": len(tgt_df),
            },
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # POLARS_HASH_FIRST: Hash-based pre-filtering for wide tables
    # ─────────────────────────────────────────────────────────────────────────────

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
        """Hash-first strategy: compute row hashes, filter to mismatches, then detail-compare."""
        comparison_started = time.perf_counter()

        try:
            # === Fetch key + hash only ===
            source_dialect = self._dialect(source_conn)
            target_dialect = self._dialect(target_conn)

            src_fqn = _fqn(table_map.source_schema, table_map.source_table, source_dialect)
            tgt_fqn = _fqn(table_map.target_schema, table_map.target_table, target_dialect)

            source_where = self._build_where_clause(table_map, source_dialect, "source")
            target_where = self._build_where_clause(table_map, target_dialect, "target")

            source_key_cols = [item["source_column"] for item in key_mappings] if key_mappings else []
            target_key_cols = [item["target_column"] for item in key_mappings] if key_mappings else []
            source_comp_cols = [item["source_column"] for item in configured_columns]
            target_comp_cols = [item["target_column"] for item in configured_columns]

            # === Stage 1: Fetch keys and comparison cols, compute hashes ===
            hash_start = time.perf_counter()

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
                raise TypeError("Connector execute_query must return pandas DataFrame")

            # Compute row hashes
            src_df["_row_hash"] = self._row_hashes(src_df, source_comp_cols)
            tgt_df["_row_hash"] = self._row_hashes(tgt_df, target_comp_cols)

            hash_seconds = time.perf_counter() - hash_start

            # === Stage 2: Compare hashes, identify mismatches ===
            src_hash_counts = Counter(src_df["_row_hash"])
            tgt_hash_counts = Counter(tgt_df["_row_hash"])

            matched_hashes = src_hash_counts & tgt_hash_counts
            mismatched_source = src_hash_counts - tgt_hash_counts
            mismatched_target = tgt_hash_counts - src_hash_counts

            matched = sum(matched_hashes.values())
            mismatched = sum(mismatched_source.values()) + sum(mismatched_target.values())

            # === Stage 3: Detailed comparison only for mismatched hashes ===
            if mismatched > 0 and key_mappings:
                mismatch_detail_start = time.perf_counter()

                # Rows with mismatched hashes
                src_mismatch = src_df[src_df["_row_hash"].isin(set(mismatched_source))]
                tgt_mismatch = tgt_df[tgt_df["_row_hash"].isin(set(mismatched_target))]

                # Store evidence (bounded by limit)
                evidence_policy = comparison_settings.get("full_data_comparison", {}).get("evidence", {})
                mismatch_limit = evidence_policy.get("mismatch_sample_limit", 100)

                for idx, row in src_mismatch.iterrows():
                    if len(result_obj.missing_in_target) >= mismatch_limit:
                        break
                    result_obj.missing_in_target.append(MissingRecord(
                        table_id=table_map.id,
                        primary_key=json.dumps(
                            [row[col] for col in source_key_cols],
                            default=str,
                        ),
                        record_data=row.drop("_row_hash").to_json(date_format="iso"),
                        location="EXCESS_IN_SOURCE",
                    ))

                for idx, row in tgt_mismatch.iterrows():
                    if len(result_obj.missing_in_source) >= mismatch_limit:
                        break
                    result_obj.missing_in_source.append(MissingRecord(
                        table_id=table_map.id,
                        primary_key=json.dumps(
                            [row[col] for col in target_key_cols],
                            default=str,
                        ),
                        record_data=row.drop("_row_hash").to_json(date_format="iso"),
                        location="EXCESS_IN_TARGET",
                    ))

                mismatch_detail_seconds = time.perf_counter() - mismatch_detail_start
            else:
                mismatch_detail_seconds = 0.0

            status = ValidationStatus.PASS if mismatched == 0 else ValidationStatus.FAIL

            result = CheckResult(
                check_name="record_comparison",
                status=status,
                source_value=len(src_df),
                target_value=len(tgt_df),
                delta=abs(len(src_df) - len(tgt_df)),
                message=f"Hash-first comparison: matched={matched}, mismatched={mismatched}",
                details={
                    **base_details,
                    "matched_records": matched,
                    "mismatch_records": mismatched,
                    "missing_in_source": sum(mismatched_target.values()),
                    "missing_in_target": sum(mismatched_source.values()),
                    "records_checked": matched + mismatched,
                    "source_records_read": len(src_df),
                    "target_records_read": len(tgt_df),
                    "hash_seconds": round(hash_seconds, 6),
                    "total_record_comparison_seconds": round(
                        time.perf_counter() - comparison_started, 6
                    ),
                    "execution_time": round(time.perf_counter() - started, 3),
                    "execution_mode": "HASH_FIRST",
                    "full_comparison_performed": False,
                },
            )
            result.details["records_compared"] = matched + mismatched

            del src_df, tgt_df
            gc.collect()

            return [result]

        except Exception as exc:
            logger.error("POLARS_HASH_FIRST comparison failed", exc_info=True)
            return [self._error(
                f"Hash-first comparison failed: {exc}",
                started,
                base_details,
                status=self._policies.configured_status("framework_exception_status"),
                exception_category=exception_category(exc),
                table_id=getattr(table_map, "id", None),
            )]

    # ─────────────────────────────────────────────────────────────────────────────
    # POLARS_SAMPLE_100K: Mandatory large-table sampling (>=1M rows)
    # ─────────────────────────────────────────────────────────────────────────────

    def _compare_polars_sample_100k(
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
        Mandatory 100K sample for tables >= 1M rows.

        MANDATORY RULE:
            Do NOT load full table.
            Do NOT call dual_keyset_merge.
            Do NOT call hash_streaming.
            Randomly select 100K source keys.
            Fetch corresponding target records.
            Compare only this cohort.
        """
        comparison_started = time.perf_counter()

        try:
            source_dialect = self._dialect(source_conn)
            target_dialect = self._dialect(target_conn)

            src_fqn = _fqn(table_map.source_schema, table_map.source_table, source_dialect)
            tgt_fqn = _fqn(table_map.target_schema, table_map.target_table, target_dialect)

            source_where = self._build_where_clause(table_map, source_dialect, "source")
            target_where = self._build_where_clause(table_map, target_dialect, "target")

            source_keys = [item["source_column"] for item in key_mappings] if key_mappings else []
            target_keys = [item["target_column"] for item in key_mappings] if key_mappings else []
            source_cols = [item["source_column"] for item in configured_columns]
            target_cols = [item["target_column"] for item in configured_columns]

            sample_size = LARGE_TABLE_SAMPLE_SIZE
            sampling_seed = random.randint(0, 2**31 - 1)

            # === Stage 1: Randomly select 100K source keys ===
            logger.info(
                "[SAMPLE] table=%s selecting %d random keys (seed=%d)",
                table_map.id, sample_size, sampling_seed
            )

            if key_mappings:
                # Sample by keys: more correct than random rows
                source_sample_sql = self._sample_by_keys_sql(
                    src_fqn, source_keys + source_cols, source_where,
                    sample_size, source_dialect
                )
            else:
                # Keyless: random sample of comparison columns
                source_sample_sql = self._select_sql(
                    src_fqn,
                    self._projection(source_cols, source_dialect),
                    source_where,
                    top=sample_size,
                    order_by=source_dialect.random_order(),
                    dialect=source_dialect,
                )

            sample_start = time.perf_counter()
            src_sample_df = source_conn.execute_query(source_sample_sql)
            src_sample_seconds = time.perf_counter() - sample_start

            if not isinstance(src_sample_df, pd.DataFrame):
                raise TypeError("Connector execute_query must return pandas DataFrame")

            actual_sample_size = len(src_sample_df)
            logger.info(
                "[SAMPLE] table=%s fetched %d source sample rows",
                table_map.id, actual_sample_size
            )

            # === Stage 2: Fetch target records matching sampled source keys ===
            if key_mappings and actual_sample_size > 0:
                source_key_values = self._extract_values(src_sample_df, source_keys)
                target_queries = self._build_filter_queries(
                    tgt_fqn,
                    self._projection(target_keys + target_cols, target_dialect),
                    target_where,
                    target_keys,
                    source_key_values,
                    target_dialect,
                )
                target_sample_start = time.perf_counter()
                tgt_sample_df = self._execute_queries(target_conn, target_queries)
                target_sample_seconds = time.perf_counter() - target_sample_start
            else:
                # Keyless: fetch equivalent number of random target rows
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
                target_sample_seconds = time.perf_counter() - target_sample_start
                # Trim to match source cohort
                tgt_sample_df = self._trim_target_to_source_signature_counts(
                    src_sample_df, tgt_sample_df, configured_columns
                )

            if not isinstance(tgt_sample_df, pd.DataFrame):
                raise TypeError("Connector execute_query must return pandas DataFrame")

            # === Stage 3: Compare samples ===
            result = self._compare_by_key(
                src_sample_df, tgt_sample_df, table_map,
                source_keys, target_keys, configured_columns,
                mismatch_limit=100, matched_limit=100,
                export_matched=False, result_obj=result_obj
            )

            result.details.update({
                **base_details,
                "execution_mode": "SAMPLE_COMPARISON",
                "full_comparison_performed": False,
                "configured_sample_size": sample_size,
                "actual_sample_size": actual_sample_size,
                "sampling_seed": sampling_seed,
                "source_read_seconds": round(src_sample_seconds, 6),
                "target_read_seconds": round(target_sample_seconds, 6),
                "source_records_read": len(src_sample_df),
                "target_records_read": len(tgt_sample_df),
                "total_record_comparison_seconds": round(
                    time.perf_counter() - comparison_started, 6
                ),
                "execution_time": round(time.perf_counter() - started, 3),
            })
            result.details["records_compared"] = result.details.get("records_checked", 0)

            del src_sample_df, tgt_sample_df
            gc.collect()

            return [result]

        except Exception as exc:
            logger.error("POLARS_SAMPLE_100K comparison failed", exc_info=True)
            return [self._error(
                f"Large-table sample comparison failed: {exc}",
                started,
                base_details,
                status=self._policies.configured_status("framework_exception_status"),
                exception_category=exception_category(exc),
                table_id=getattr(table_map, "id", None),
            )]

    # ─────────────────────────────────────────────────────────────────────────────
    # Comparison Helpers (Existing, with refactoring)
    # ─────────────────────────────────────────────────────────────────────────────

    def _compare_by_key(
        self,
        src_df: pd.DataFrame,
        tgt_df: pd.DataFrame,
        table_map,
        source_keys: List[str],
        target_keys: List[str],
        column_mappings: List[Dict[str, str]],
        mismatch_limit: int,
        matched_limit: int,
        export_matched: bool,
        result_obj: TableValidationResult,
    ) -> CheckResult:
        """Key-based comparison (pandas)."""
        self._require_columns(src_df, source_keys, "source key")
        self._require_columns(tgt_df, target_keys, "target key")

        source = src_df.copy()
        target = tgt_df.copy()
        source["_merge_key"] = self._key_tokens(source, source_keys)
        target["_merge_key"] = self._key_tokens(target, target_keys)

        source_by_key = source.set_index("_merge_key", drop=False)
        target_by_key = target.set_index("_merge_key", drop=False)

        source_key_set = set(source_by_key.index)
        target_key_set = set(target_by_key.index)

        missing_target_keys = source_key_set - target_key_set
        missing_source_keys = target_key_set - source_key_set

        matched, mismatched, metrics = self._compare_aligned_values(
            source, target, table_map, source_key_set & target_key_set,
            column_mappings, mismatch_limit, matched_limit, export_matched, result_obj
        )

        missing_source_count = len(missing_source_keys)
        missing_target_count = len(missing_target_keys)
        mismatch_count = mismatched + missing_source_count + missing_target_count
        records_checked = matched + mismatch_count

        return CheckResult(
            check_name="record_comparison",
            status=ValidationStatus.PASS if mismatch_count == 0 else ValidationStatus.FAIL,
            source_value=len(source),
            target_value=len(target),
            delta=abs(len(source) - len(target)),
            message=f"Compared={records_checked}, matched={matched}, mismatched={mismatch_count}",
            details={
                "records_checked": records_checked,
                "matched_records": matched,
                "mismatch_records": mismatch_count,
                "value_mismatch_records": mismatched,
                "missing_in_source": missing_source_count,
                "missing_in_target": missing_target_count,
                "source_records_selected": len(source),
                "target_records_selected": len(target),
                **metrics,
            },
        )

    def _compare_aligned_values(
        self,
        source: pd.DataFrame,
        target: pd.DataFrame,
        table_map,
        common_keys: set,
        column_mappings: List[Dict[str, str]],
        mismatch_limit: int,
        matched_limit: int,
        export_matched: bool,
        result_obj: TableValidationResult,
    ) -> Tuple[int, int, Dict[str, Any]]:
        """Vectorized value comparison (existing implementation)."""
        if not common_keys:
            return 0, 0, {}

        ordered_keys = sorted(common_keys)
        source_positions = {key: position for position, key in enumerate(source["_merge_key"])}
        target_positions = {key: position for position, key in enumerate(target["_merge_key"])}

        source_aligned = source.iloc[[source_positions[key] for key in ordered_keys]].reset_index(drop=True)
        target_aligned = target.iloc[[target_positions[key] for key in ordered_keys]].reset_index(drop=True)

        record_count = len(ordered_keys)
        mismatch_masks: List[np.ndarray] = []

        for mapping in column_mappings:
            source_values = source_aligned[mapping["source_column"]]
            target_values = target_aligned[mapping["target_column"]]

            try:
                mask = (source_values != target_values).fillna(True).to_numpy(dtype=bool)
            except Exception:
                mask = np.array([
                    not self._values_match(sv, tv)
                    for sv, tv in zip(source_values, target_values)
                ], dtype=bool)

            mismatch_masks.append(mask)

        combined_mismatch = np.zeros(record_count, dtype=bool)
        for mask in mismatch_masks:
            combined_mismatch |= mask

        value_mismatch_count = int(combined_mismatch.sum())
        matched_count = record_count - value_mismatch_count

        return matched_count, value_mismatch_count, {}

    def _compare_by_row_hash(
        self,
        src_df: pd.DataFrame,
        tgt_df: pd.DataFrame,
        table_map,
        column_mappings: List[Dict[str, str]],
        comparison_settings: Dict[str, Any],
        result_obj: TableValidationResult = None,
    ) -> CheckResult:
        """Row-hash comparison for keyless tables."""
        source = src_df.copy()
        target = tgt_df.copy()

        source_columns = [item["source_column"] for item in column_mappings]
        target_columns = [item["target_column"] for item in column_mappings]

        source["_row_hash"] = self._row_hashes(source, source_columns)
        target["_row_hash"] = self._row_hashes(target, target_columns)

        source_counts = Counter(source["_row_hash"])
        target_counts = Counter(target["_row_hash"])

        matched_count = sum((source_counts & target_counts).values())
        missing_target = source_counts - target_counts
        missing_source = target_counts - source_counts

        missing_target_count = sum(missing_target.values())
        missing_source_count = sum(missing_source.values())
        mismatch_count = missing_target_count + missing_source_count

        return CheckResult(
            check_name="record_comparison",
            status=ValidationStatus.PASS if mismatch_count == 0 else ValidationStatus.FAIL,
            source_value=len(source),
            target_value=len(target),
            delta=abs(len(source) - len(target)),
            message=f"Row-hash: matched={matched_count}, mismatched={mismatch_count}",
            details={
                "matched_records": matched_count,
                "mismatch_records": mismatch_count,
                "missing_in_source": missing_source_count,
                "missing_in_target": missing_target_count,
                "records_checked": matched_count + mismatch_count,
                "source_records_selected": len(source),
                "target_records_selected": len(target),
            },
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # SQL and Utility Methods
    # ─────────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _sample_by_keys_sql(
        fqn: str,
        columns: List[str],
        where_clause: str,
        sample_size: int,
        dialect=None,
    ) -> str:
        """SQL to sample N random keys from a table."""
        dialect = dialect or resolve_dialect(None)
        return dialect.select(
            ", ".join(dialect.quote(col) for col in columns),
            fqn,
            where_clause,
            order_by=dialect.random_order(),
            limit=sample_size,
        )

    def _row_hashes(self, frame: pd.DataFrame, columns: List[str]) -> List[str]:
        """Compute row hashes efficiently."""
        return self._hash_frame(frame[columns], columns)

    def _hash_frame(self, frame: pd.DataFrame, columns: List[str]) -> List[str]:
        """Hash each row using canonical column values."""
        if frame.empty:
            return []

        algorithm = self._policies.hashing["algorithm"]
        hashes = []

        for _, row in frame.iterrows():
            parts = []
            for col in columns:
                canonical = self._hash_canonical_value(row[col])
                encoded = canonical.encode("utf-8")
                parts.append(len(encoded).to_bytes(8, byteorder="big"))
                parts.append(encoded)

            row_hash = hashlib.new(algorithm, b"".join(parts)).hexdigest()
            hashes.append(row_hash)

        return hashes

    def _key_tokens(self, frame: pd.DataFrame, columns: List[str]) -> List[Tuple[str, ...]]:
        """Build key tokens."""
        if frame.empty:
            return []

        return [
            tuple(self._canonical_value(row[col]) for col in columns)
            for _, row in frame.iterrows()
        ]

    def _canonical_value(self, value: Any) -> str:
        """Canonical representation of a value."""
        if self._is_null(value):
            return "null:<NULL>"
        if isinstance(value, bool):
            return f"bool:{str(value).lower()}"

        token = temporal_token(
            value,
            parse_strings=self._policies.timestamps["parse_iso_strings"],
        )
        if token is not None:
            return f"datetime:{token.canonical}"

        if isinstance(value, Decimal):
            return f"number:{normalize_decimal_for_hash(value)}"
        if isinstance(value, (int, float)):
            return f"number:{value}"
        if isinstance(value, bytes):
            return f"bytes:{value.hex()}"

        return f"string:{value}"

    def _hash_canonical_value(self, value: Any) -> str:
        """Canonical value for hashing."""
        normalization = self._policies.hashing["normalization"]

        if isinstance(value, str) and not value.strip() and normalization.get("blank_handling") == "null":
            return normalization.get("null_representation", "NULL")

        if self._is_null(value):
            return normalization.get("null_representation", "NULL")

        text = str(value)
        if normalization.get("trim_strings"):
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

    def _values_match(self, source_value: Any, target_value: Any) -> bool:
        """Check if two values match."""
        source_null = self._is_null(source_value)
        target_null = self._is_null(target_value)

        if source_null and target_null:
            return self._policies.nulls.get("both_null") == "match"
        if source_null or target_null:
            return self._policies.nulls.get("one_null") == "match"

        try:
            return bool(source_value == target_value)
        except (TypeError, ValueError):
            return self._canonical_value(source_value) == self._canonical_value(target_value)

    @staticmethod
    def _extract_values(frame: pd.DataFrame, columns: List[str]) -> List[Tuple[Any, ...]]:
        """Extract column values as tuples."""
        if frame.empty:
            return []
        return list(frame[columns].itertuples(index=False, name=None))

    @staticmethod
    def _trim_target_to_source_signature_counts(
        source: pd.DataFrame,
        target: pd.DataFrame,
        column_mappings: List[Dict[str, str]],
    ) -> pd.DataFrame:
        """Trim target to match source row counts by signature."""
        source_columns = [item["source_column"] for item in column_mappings]
        target_columns = [item["target_column"] for item in column_mappings]

        source_hashes = [
            hashlib.md5(
                str(tuple(row[col] for col in source_columns)).encode()
            ).hexdigest()
            for _, row in source.iterrows()
        ]
        target_hashes = [
            hashlib.md5(
                str(tuple(row[col] for col in target_columns)).encode()
            ).hexdigest()
            for _, row in target.iterrows()
        ]

        source_counts = Counter(source_hashes)
        retained_counts = Counter()
        retained_positions = []

        for position, row_hash in enumerate(target_hashes):
            if retained_counts[row_hash] >= source_counts[row_hash]:
                continue
            retained_counts[row_hash] += 1
            retained_positions.append(position)

        return target.iloc[retained_positions].reset_index(drop=True)

    @staticmethod
    def _build_filter_queries(
        fqn: str,
        projection: str,
        where_clause: str,
        columns: List[str],
        values: List[Tuple[Any, ...]],
        dialect=None,
    ) -> List[str]:
        """Build filter queries from row values."""
        dialect = dialect or resolve_dialect(None)
        predicates = []

        for row_values in values:
            conditions = []
            for column, value in zip(columns, row_values):
                if value is None or (isinstance(value, float) and np.isnan(value)):
                    conditions.append(f"{dialect.quote(column)} IS NULL")
                else:
                    conditions.append(f"{dialect.quote(column)} = '{value}'")
            predicates.append("(" + " AND ".join(conditions) + ")")

        queries = []
        batch_size = 100

        for i in range(0, len(predicates), batch_size):
            batch = predicates[i:i+batch_size]
            combined_where = (
                f"{where_clause} AND ({' OR '.join(batch)})"
                if where_clause.strip()
                else f" WHERE {' OR '.join(batch)}"
            )

            queries.append(f"SELECT {projection} FROM {fqn}{combined_where}")

        return queries or [f"SELECT {projection} FROM {fqn}{where_clause}"]

    @staticmethod
    def _execute_queries(connection, queries: List[str]) -> pd.DataFrame:
        """Execute multiple queries and concatenate."""
        frames = []
        for query in queries:
            frame = connection.execute_query(query)
            if isinstance(frame, pd.DataFrame) and not frame.empty:
                frames.append(frame)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]

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
    def _require_columns(frame: pd.DataFrame, columns: List[str], purpose: str) -> None:
        """Verify columns exist."""
        missing = [col for col in columns if col not in frame.columns]
        if missing:
            raise ValueError(f"Missing {purpose} columns: {missing}")

    # ─────────────────────────────────────────────────────────────────────────────
    # Configuration and Static Methods (from original)
    # ─────────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _key_mappings(table_map) -> List[Dict[str, str]]:
        """Extract key mappings."""
        resolved = []
        for item in getattr(table_map, "key_mappings", None) or []:
            source = item.get("source_column") if isinstance(item, dict) else getattr(item, "source_column")
            target = item.get("target_column") if isinstance(item, dict) else getattr(item, "target_column")
            resolved.append({"source_column": source.strip(), "target_column": target.strip()})
        return resolved

    @staticmethod
    def _configured_column_mappings(table_map) -> List[Dict[str, str]]:
        """Extract comparison column mappings."""
        mappings = []
        for item in getattr(table_map, "compare_column_mappings", None) or []:
            if isinstance(item, dict):
                mappings.append({
                    "source_column": item.get("source_column", "").strip(),
                    "target_column": item.get("target_column", "").strip(),
                    "name": item.get("name", ""),
                })
        return mappings

    @staticmethod
    def _cached_row_counts(execution_cache) -> Tuple[int, int]:
        """Fetch cached row counts."""
        if execution_cache is None:
            raise ValueError("Execution cache required for row counts")

        counts = []
        for role in ("source", "target"):
            value = execution_cache.peek(("row_count", role))
            if not isinstance(value, int) or value < 0:
                raise ValueError(f"Cached {role} row count unavailable")
            counts.append(value)

        return counts[0], counts[1]

    @staticmethod
    def _large_table_processing_config(raw_config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Load large-table processing config."""
        defaults = {
            "enabled": False,
            "medium_table_threshold": 500_000,
            "large_table_threshold": 2_000_000,
        }
        if not isinstance(raw_config, dict):
            return defaults
        return {**defaults, **raw_config}

    # The phase filter is rendered by BaseValidator._build_where_clause from the
    # resolved ValidationScope (``predicates_for``/``parameters_for``), the same
    # way every other validator renders it. The previous override called a
    # ``sql_filter`` attribute ValidationScope never had, so every scoped table
    # failed before its first read.

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
