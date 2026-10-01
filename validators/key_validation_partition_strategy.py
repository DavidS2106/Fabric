"""Partition-hash-first strategy for exact >=1M key validation.

This module implements optimized key consistency checking for large tables using
partition signatures instead of materializing all keys.

Key concepts:
- Partition signature: deterministic SHA-256 hash of distinct keys + count
- Only mismatching partitions perform exact key reconciliation
- Final result remains 100% exact, not sampled
- Fallback to keyset_batch if optimization cannot safely execute
"""

import hashlib
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PartitionSignature:
    """Summary of one table partition for fast cross-system comparison."""
    partition_id: int
    distinct_key_count: int
    key_hash: str  # SHA-256 of canonical sorted key values
    role: str


@dataclass(frozen=True)
class PartitionComparisonResult:
    """Result of comparing source and target partition signatures."""
    total_partitions: int
    matching_partitions: int
    mismatching_partitions: int
    partition_ids_to_reconcile: List[int]
    elapsed: float


@dataclass(frozen=True)
class ExactPartitionResult:
    """Exact key reconciliation result for one mismatching partition."""
    partition_id: int
    missing_in_target: int
    missing_in_source: int
    matched_keys: int
    samples_missing_in_target: List[Tuple[str, ...]]
    samples_missing_in_source: List[Tuple[str, ...]]
    elapsed: float


class PartitionHashFirstStrategy:
    """Optimize >=1M key validation using partition signatures."""

    def __init__(self, large_table_threshold: int = 1_000_000):
        self.large_table_threshold = large_table_threshold

    def should_use_hash_first(
        self, source_distinct_keys: int, target_distinct_keys: int
    ) -> bool:
        """Route to partition-hash-first if either side >= threshold."""
        max_distinct = max(source_distinct_keys, target_distinct_keys)
        return max_distinct >= self.large_table_threshold

    def generate_partition_sql(
        self,
        fqn: str,
        keys: Sequence[str],
        where_clause: str,
        partition_count: int,
        dialect,
        role: str = None,
    ) -> str:
        """Generate SQL to compute partition signatures.

        Returns partitions with:
        - partition_id (0 to partition_count-1)
        - distinct_key_count
        - key_hash (SHA-256 of canonical key values)

        Uses database-side computation for scalability.
        """
        quoted_keys = [dialect.quote(key) for key in keys]
        key_list = ", ".join(quoted_keys)

        # Deterministic partition ID: hash each key, mod partition_count
        # Use database function to avoid materializing all keys
        if len(keys) == 1:
            partition_expr = self._single_key_partition_expr(
                quoted_keys[0], partition_count, dialect
            )
        else:
            partition_expr = self._composite_key_partition_expr(
                quoted_keys, partition_count, dialect
            )

        # Build canonical row-by-row key hash (deterministic across systems)
        key_concat = self._key_concatenation_expr(quoted_keys, dialect)
        row_hash_expr = self._sha256_expr(key_concat, dialect)

        # Inner query: compute row-level partition and hash
        inner = dialect.select(
            f"{partition_expr} AS part_id, "
            f"{row_hash_expr} AS row_hash",
            fqn,
            where_clause,
        )

        # Outer query: aggregate partition signatures
        outer_select = (
            "part_id AS partition_id, "
            f"{dialect.count_column('DISTINCT row_hash')} AS distinct_key_count, "
            f"{self._aggregate_hash_expr(dialect)} AS key_hash"
        )

        return dialect.select(
            outer_select,
            dialect.derived_table(inner, "__partition_sigs"),
            group_by="part_id",
            order_by="part_id",
        )

    def _single_key_partition_expr(
        self, quoted_key: str, partition_count: int, dialect
    ) -> str:
        """Partition single-column key by hashing to 0..partition_count-1."""
        # Convert any type to string, then hash to integer mod partition_count
        return (
            f"CAST("
            f"ABS(HASHBYTES('SHA2_256', CAST({quoted_key} AS NVARCHAR(MAX)))) "
            f"% {partition_count} AS INT"
            ")"
        )

    def _composite_key_partition_expr(
        self, quoted_keys: List[str], partition_count: int, dialect
    ) -> str:
        """Partition composite key by concatenating and hashing."""
        key_concat = self._key_concatenation_expr(quoted_keys, dialect)
        return (
            f"CAST("
            f"ABS(HASHBYTES('SHA2_256', {key_concat})) "
            f"% {partition_count} AS INT"
            ")"
        )

    @staticmethod
    def _key_concatenation_expr(quoted_keys: List[str], dialect) -> str:
        """Concatenate keys with length prefixes for deterministic comparison."""
        parts = []
        for key in quoted_keys:
            # Length-prefixed: concat(length_as_hex, ':', value)
            # Ensures key1='a'+key2='b' != key1='ab'+key2=''
            parts.append(f"LEN(CAST({key} AS NVARCHAR(MAX))) + ':' + CAST({key} AS NVARCHAR(MAX))")
        return f"CONCAT({', '.join(parts)})"

    @staticmethod
    def _sha256_expr(expr: str, dialect) -> str:
        """Express SHA-256 hash of expr using dialect-appropriate function."""
        # Assumes SQL Server HASHBYTES('SHA2_256', ...)
        # For Fabric/other dialects, override or use dialect.hash_function()
        return f"CAST(HASHBYTES('SHA2_256', {expr}) AS NVARCHAR(64))"

    @staticmethod
    def _aggregate_hash_expr(dialect) -> str:
        """Hash aggregated row hashes to create partition signature.

        For each row hash in partition, concat them sorted, then hash the result.
        """
        # STRING_AGG sorts hashes then hashes the result
        return (
            "CAST(HASHBYTES('SHA2_256', "
            "STRING_AGG(CAST(row_hash AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY row_hash)) "
            "AS NVARCHAR(64))"
        )

    def compare_partition_signatures(
        self,
        source_sigs: pd.DataFrame,
        target_sigs: pd.DataFrame,
    ) -> PartitionComparisonResult:
        """Compare source and target partition signatures.

        Returns partition IDs where signatures differ (need exact reconciliation).
        """
        started = time.perf_counter()

        # Create lookup dictionaries: partition_id -> signature
        source_sig_map = {}
        target_sig_map = {}

        for _, row in source_sigs.iterrows():
            part_id = int(row["partition_id"])
            source_sig_map[part_id] = {
                "distinct_count": int(row["distinct_key_count"]),
                "key_hash": str(row["key_hash"]).strip(),
            }

        for _, row in target_sigs.iterrows():
            part_id = int(row["partition_id"])
            target_sig_map[part_id] = {
                "distinct_count": int(row["distinct_key_count"]),
                "key_hash": str(row["key_hash"]).strip(),
            }

        # Find partitions that differ
        all_partition_ids = sorted(set(source_sig_map.keys()) | set(target_sig_map.keys()))
        mismatching = []

        for part_id in all_partition_ids:
            source_sig = source_sig_map.get(part_id)
            target_sig = target_sig_map.get(part_id)

            # Partitions differ if:
            # - one side missing
            # - distinct counts differ
            # - key hashes differ
            if (source_sig is None or target_sig is None or
                source_sig["distinct_count"] != target_sig["distinct_count"] or
                source_sig["key_hash"] != target_sig["key_hash"]):
                mismatching.append(part_id)

        elapsed = time.perf_counter() - started

        return PartitionComparisonResult(
            total_partitions=len(all_partition_ids),
            matching_partitions=len(all_partition_ids) - len(mismatching),
            mismatching_partitions=len(mismatching),
            partition_ids_to_reconcile=mismatching,
            elapsed=round(elapsed, 6),
        )

    @staticmethod
    def canonical_key_value(value: Any) -> str:
        """Convert any key value to canonical string for hashing."""
        if value is None:
            return "<NULL>"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)):
            return str(value)
        return str(value).strip()
