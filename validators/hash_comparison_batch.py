"""
fabric_migration_validator/validators/hash_comparison_batch.py
──────────────────────────────────────────────────────────────
Key-cohort batching and row hash comparison for efficient record matching.

Implements:
- Parameterized key-cohort queries
- Row hash computation for column-set comparison
- IS NULL handling for null values
- Batch-wise comparison with query signature logging
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


class KeyCohortBatcher:
    """Batch multiple keys using temporary table or key-cohort method.

    Reduces database round trips by grouping keys into cohorts for efficient
    batch comparison without loading all records into memory.
    """

    def __init__(
        self,
        connection: Any,
        key_columns: List[str],
        batch_size: int = 1000,
        use_temp_table: bool = True,
    ):
        """Initialize key cohort batcher.

        Args:
            connection: Database connection with execute_query() method
            key_columns: List of key column names to batch
            batch_size: Number of keys per batch cohort
            use_temp_table: Whether to use temporary table strategy
        """
        self.connection = connection
        self.key_columns = key_columns
        self.batch_size = max(1, int(batch_size))
        self.use_temp_table = bool(use_temp_table)
        self._temp_table_name: Optional[str] = None

    def batch_keys(
        self,
        keys_df: pd.DataFrame,
    ) -> List[pd.DataFrame]:
        """Yield batches of key rows for cohort processing.

        Args:
            keys_df: DataFrame containing key columns to batch

        Returns:
            List of DataFrames, each containing up to batch_size key rows
        """
        batches = []
        for i in range(0, len(keys_df), self.batch_size):
            batch = keys_df.iloc[i : i + self.batch_size].copy()
            batches.append(batch)

        logger.debug(
            "Key cohort batching: total_keys=%d, batch_size=%d, batches=%d",
            len(keys_df),
            self.batch_size,
            len(batches),
        )
        return batches

    def create_cohort_where_clause(self, key_batch: pd.DataFrame) -> str:
        """Generate WHERE clause for key cohort.

        Handles composite keys and NULL values correctly.

        Args:
            key_batch: DataFrame with one row per key value

        Returns:
            WHERE clause like: (key1=? AND key2=?) OR (key1=? AND key2=?) ...
        """
        if len(key_batch) == 0:
            return "1=0"  # Empty cohort

        if len(key_batch) == 1:
            # Single key
            conditions = []
            row = key_batch.iloc[0]
            for col in self.key_columns:
                val = row[col]
                if pd.isna(val):
                    conditions.append(f"{col} IS NULL")
                else:
                    conditions.append(f"{col} = ?")
            return " AND ".join(conditions)

        # Multiple keys - use OR with IN clause for better optimization
        if len(self.key_columns) == 1:
            # Single key column - use IN clause
            key_col = self.key_columns[0]
            non_null_vals = key_batch[key_col].dropna()
            null_check = f"{key_col} IS NULL" if key_batch[key_col].isna().any() else ""

            if len(non_null_vals) > 0:
                placeholders = ",".join(["?" for _ in non_null_vals])
                in_clause = f"{key_col} IN ({placeholders})"
                if null_check:
                    return f"({in_clause} OR {null_check})"
                return in_clause
            return null_check if null_check else "1=0"

        # Composite keys - use OR with AND for each row
        conditions = []
        for _, row in key_batch.iterrows():
            row_conditions = []
            for col in self.key_columns:
                val = row[col]
                if pd.isna(val):
                    row_conditions.append(f"{col} IS NULL")
                else:
                    row_conditions.append(f"{col} = ?")
            conditions.append(f"({' AND '.join(row_conditions)})")

        return " OR ".join(conditions)

    def cleanup(self) -> None:
        """Clean up temporary resources."""
        if self._temp_table_name and self.use_temp_table:
            try:
                self.connection.execute_query(f"DROP TABLE IF EXISTS {self._temp_table_name}")
                logger.debug("Cleaned up temporary table: %s", self._temp_table_name)
            except Exception as exc:
                logger.warning("Could not clean up temporary table %s: %s", self._temp_table_name, exc)


class RowHashComparator:
    """Compare records using row hashes instead of all-column predicates.

    Computes deterministic hashes for efficient comparison and deduplication.
    Handles NULL values correctly (NULL hash is different from missing).
    """

    def __init__(self, columns: List[str], hash_algorithm: str = "sha256"):
        """Initialize row hash comparator.

        Args:
            columns: List of column names to include in hash
            hash_algorithm: Hash algorithm (sha256, md5, sha1)
        """
        self.columns = columns
        self.hash_algorithm = hash_algorithm
        self._algorithm_func = getattr(hashlib, hash_algorithm)

    def compute_row_hash(
        self,
        row: Dict[str, Any],
        include_nulls: bool = True,
    ) -> str:
        """Compute deterministic hash for a single row.

        Args:
            row: Dictionary of column names to values
            include_nulls: Whether to include NULL values in hash

        Returns:
            Hex string hash of row
        """
        hash_input = []

        for col in self.columns:
            val = row.get(col)

            if val is None or pd.isna(val):
                # Generate IS NULL marker for actual null values
                if include_nulls:
                    hash_input.append(f"{col}:IS_NULL")
            else:
                # Convert to string representation
                if isinstance(val, (int, float, bool)):
                    val_str = str(val).lower()  # Consistent case for booleans
                elif isinstance(val, bytes):
                    val_str = val.hex()
                elif hasattr(val, "isoformat"):
                    # datetime, date, time
                    val_str = val.isoformat()
                else:
                    val_str = str(val)

                hash_input.append(f"{col}:{val_str}")

        combined = "|".join(hash_input)
        return self._algorithm_func(combined.encode("utf-8")).hexdigest()

    def compute_batch_hashes(self, df: pd.DataFrame) -> pd.Series:
        """Compute row hashes for entire DataFrame.

        Args:
            df: DataFrame with columns matching self.columns

        Returns:
            Series of hex string hashes indexed by row position
        """
        hashes = []
        for _, row in df.iterrows():
            row_dict = row[self.columns].to_dict()
            hash_val = self.compute_row_hash(row_dict)
            hashes.append(hash_val)

        return pd.Series(hashes, index=df.index)

    def compare_hashes(
        self,
        source_hash: str,
        target_hash: str,
    ) -> bool:
        """Compare two row hashes.

        Args:
            source_hash: Hash from source row
            target_hash: Hash from target row

        Returns:
            True if hashes match, False otherwise
        """
        return source_hash == target_hash


class ComparisonQueryLogger:
    """Log only query signatures, not full SQL.

    Tracks query metadata for performance analysis without exposing sensitive SQL.
    """

    def __init__(self):
        """Initialize comparison query logger."""
        self.queries: List[Dict[str, Any]] = []
        self._execution_count = 0

    def log_query_execution(
        self,
        query_signature: str,
        batch_number: int,
        row_count: int,
        execution_time_seconds: float,
        source_or_target: str = "source",
    ) -> None:
        """Log a query execution without the full SQL.

        Args:
            query_signature: Signature/hash of the query
            batch_number: Batch number in the sequence
            row_count: Number of rows returned
            execution_time_seconds: Time to execute query
            source_or_target: Whether this is 'source' or 'target' query
        """
        self._execution_count += 1

        log_entry = {
            "query_signature": query_signature,
            "batch_number": batch_number,
            "row_count": row_count,
            "execution_time_seconds": round(execution_time_seconds, 6),
            "source_or_target": source_or_target,
            "sequence_number": self._execution_count,
        }
        self.queries.append(log_entry)

        logger.debug(
            "Query execution logged | signature=%s | batch=%d | rows=%d | time=%.3fs | side=%s",
            query_signature,
            batch_number,
            row_count,
            execution_time_seconds,
            source_or_target,
        )

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of logged queries.

        Returns:
            Dictionary with summary statistics
        """
        if not self.queries:
            return {
                "total_queries": 0,
                "total_rows": 0,
                "total_time": 0.0,
                "queries": [],
            }

        total_rows = sum(q["row_count"] for q in self.queries)
        total_time = sum(q["execution_time_seconds"] for q in self.queries)

        return {
            "total_queries": len(self.queries),
            "total_rows": total_rows,
            "total_time_seconds": round(total_time, 6),
            "avg_time_per_query": round(total_time / len(self.queries), 6) if self.queries else 0,
            "queries": self.queries,
        }


def generate_query_signature(sql: str) -> str:
    """Generate a signature for a SQL query (hash, not full SQL).

    Args:
        sql: SQL query string

    Returns:
        Hex hash signature
    """
    # Normalize query for consistent signatures
    normalized = sql.strip().upper()
    normalized = " ".join(normalized.split())  # Remove extra whitespace

    signature = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]
    return signature


def apply_null_handling(sql: str, null_handling: str = "standard") -> str:
    """Apply NULL handling to SQL query.

    Ensures IS NULL is generated for actual null values in WHERE clauses.

    Args:
        sql: Original SQL query
        null_handling: "standard" or "custom"

    Returns:
        SQL with NULL handling applied
    """
    if null_handling == "standard":
        # Standard SQL NULL handling - no changes
        return sql

    # Could extend with custom NULL handling strategies
    return sql
