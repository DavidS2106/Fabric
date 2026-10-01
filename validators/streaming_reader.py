"""
fabric_migration_validator/validators/streaming_reader.py
──────────────────────────────────────────────────────────
Memory-efficient streaming batch reader for large table scans.
"""

from __future__ import annotations

import gc
import logging
import time
from typing import Any, Generator, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


class StreamingBatchReader:
    """Read large tables in configurable batches to control memory usage.

    Yields DataFrames of up to batch_size rows, ensuring that only one batch
    resides in memory at any given time. Automatically cleans up previous batches
    and tracks progress.
    """

    def __init__(
        self,
        connection: Any,
        sql: str,
        batch_size: int = 50000,
        enable_gc: bool = True,
    ):
        """Initialize the streaming reader.

        Args:
            connection: Database connection with execute_query() method
            sql: SQL query to execute
            batch_size: Maximum rows per batch (default 50K)
            enable_gc: Whether to trigger garbage collection between batches
        """
        self.connection = connection
        self.sql = sql
        self.batch_size = max(1, int(batch_size))
        self.enable_gc = bool(enable_gc)

        self._total_rows_read = 0
        self._batch_count = 0
        self._current_batch: Optional[pd.DataFrame] = None
        self._current_batch_index = 0

    def read_batches(self) -> Generator[pd.DataFrame, None, None]:
        """Yield successive batches from the result set.

        Each yielded DataFrame contains up to batch_size rows. The previous
        batch is automatically cleaned up before yielding the next.

        Yields:
            pd.DataFrame: Up to batch_size rows from the result set
        """
        started = time.perf_counter()
        batch_start_offset = 0

        while True:
            batch_started = time.perf_counter()

            # Build OFFSET/LIMIT clause for this batch
            sql_with_limit = self._apply_limit_clause(
                self.sql, self.batch_size, batch_start_offset
            )

            try:
                frame = self.connection.execute_query(sql_with_limit)
            except Exception as exc:
                logger.error(
                    "Batch query failed at offset=%d: %s",
                    batch_start_offset,
                    exc,
                )
                raise

            if not isinstance(frame, pd.DataFrame):
                raise TypeError(
                    "Connector execute_query() must return a pandas DataFrame"
                )

            batch_elapsed = round(time.perf_counter() - batch_started, 6)
            rows_in_batch = len(frame)

            if rows_in_batch == 0:
                logger.debug(
                    "Batch read complete: total_batches=%d, total_rows=%d, "
                    "total_time=%.3fs",
                    self._batch_count,
                    self._total_rows_read,
                    time.perf_counter() - started,
                )
                break

            self._batch_count += 1
            self._total_rows_read += rows_in_batch
            self._current_batch_index += 1

            logger.debug(
                "Batch %d fetched: offset=%d, rows=%d, elapsed=%.3fs, "
                "total_rows=%d",
                self._batch_count,
                batch_start_offset,
                rows_in_batch,
                batch_elapsed,
                self._total_rows_read,
            )

            yield frame

            # Cleanup current batch immediately after yielding (FIX 3.2)
            del frame
            self._cleanup_batch()

            # Stop if we got fewer rows than the batch size (last batch)
            if rows_in_batch < self.batch_size:
                logger.debug(
                    "End of result set detected (partial batch: %d rows)",
                    rows_in_batch,
                )
                break

            batch_start_offset += self.batch_size

    def progress(self) -> Tuple[int, int]:
        """Return (rows_read, batches_read) progress tracking."""
        return self._total_rows_read, self._batch_count

    def _cleanup_batch(self) -> None:
        """Clean up the previous batch and trigger garbage collection."""
        self._current_batch = None
        if self.enable_gc:
            gc.collect()

    @staticmethod
    def _apply_limit_clause(sql: str, batch_size: int, offset: int) -> str:
        """Append LIMIT/OFFSET clause to SQL for batch fetching.

        Handles both ANSI SQL and SQL Server syntax.
        CRITICAL: SQL MUST contain ORDER BY for deterministic batch ordering.
        """
        sql_trimmed = sql.rstrip()
        if sql_trimmed.endswith(";"):
            sql_trimmed = sql_trimmed[:-1]

        sql_upper = sql_trimmed.upper()

        # Validate prerequisites
        if " OFFSET " in sql_upper or " LIMIT " in sql_upper:
            raise ValueError(
                "SQL already contains LIMIT/OFFSET clause; cannot apply batching"
            )

        if ";" in sql_trimmed:
            raise ValueError("Cannot apply LIMIT/OFFSET to multi-statement SQL")

        # CRITICAL: Verify deterministic ORDER BY (FIX A)
        if " ORDER BY " not in sql_upper:
            raise ValueError(
                "OFFSET/FETCH batching requires deterministic ORDER BY clause. "
                "Rows may be duplicated or skipped between batches without ORDER BY. "
                "Add ORDER BY to ensure consistent row ordering. "
                "Example: SELECT * FROM table ORDER BY id"
            )

        # Handle FETCH NEXT
        if " FETCH NEXT " in sql_upper:
            sql_trimmed = sql_trimmed.split(" FETCH NEXT ")[0]

        if sql_trimmed.upper().startswith("WITH "):
            batched = f"{sql_trimmed} OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
        elif "(" in sql_trimmed and sql_trimmed.strip().endswith(")"):
            batched = (
                f"SELECT * FROM ({sql_trimmed}) __batch_source "
                f"OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
            )
        else:
            batched = (
                f"{sql_trimmed} OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
            )

        return batched


class KeyCohortStreamingReader:
    """Stream results for a specific key cohort with optional parameterized batching."""

    def __init__(
        self,
        connection: Any,
        base_sql: str,
        key_cohort_where: str,
        batch_size: int = 50000,
        enable_gc: bool = True,
    ):
        """Initialize key cohort streaming reader.

        Args:
            connection: Database connection with execute_query() method
            base_sql: SQL query template (without WHERE clause)
            key_cohort_where: WHERE clause for this cohort
            batch_size: Maximum rows per batch
            enable_gc: Whether to trigger garbage collection between batches
        """
        self.connection = connection
        self.base_sql = base_sql
        self.key_cohort_where = key_cohort_where
        self.batch_size = max(1, int(batch_size))
        self.enable_gc = bool(enable_gc)

        self._total_rows_read = 0
        self._batch_count = 0

        # FIX 3.1: Validate ORDER BY for deterministic batching
        self._validate_batch_safety()

    def _validate_batch_safety(self) -> None:
        """Validate base SQL has ORDER BY for deterministic batching.

        FIX 3.1: Enforce ORDER BY to prevent row duplication/skipping.
        """
        sql_upper = self.base_sql.strip().upper()

        if " ORDER BY " not in sql_upper:
            raise ValueError(
                "OFFSET/FETCH batching requires deterministic ORDER BY clause. "
                "Rows may be duplicated or skipped between batches without ORDER BY. "
                "Add ORDER BY to base_sql to ensure consistent row ordering. "
                "Example: SELECT * FROM table ORDER BY id"
            )

    def read_cohort_batches(self) -> Generator[pd.DataFrame, None, None]:
        """Yield successive batches for this key cohort.

        Each yielded DataFrame contains up to batch_size rows from the cohort.

        Yields:
            pd.DataFrame: Up to batch_size rows from the cohort
        """
        started = time.perf_counter()
        batch_start_offset = 0

        while True:
            batch_started = time.perf_counter()

            # Build SQL with WHERE clause and OFFSET/LIMIT for this batch
            sql_with_cohort = f"{self.base_sql} WHERE {self.key_cohort_where}"
            sql_with_limit = (
                f"{sql_with_cohort} OFFSET {batch_start_offset} ROWS "
                f"FETCH NEXT {self.batch_size} ROWS ONLY"
            )

            try:
                frame = self.connection.execute_query(sql_with_limit)
            except Exception as exc:
                logger.error(
                    "Cohort batch query failed at offset=%d: %s",
                    batch_start_offset,
                    exc,
                )
                raise

            if not isinstance(frame, pd.DataFrame):
                raise TypeError(
                    "Connector execute_query() must return a pandas DataFrame"
                )

            batch_elapsed = round(time.perf_counter() - batch_started, 6)
            rows_in_batch = len(frame)

            if rows_in_batch == 0:
                logger.debug(
                    "Cohort batch read complete: total_batches=%d, total_rows=%d, "
                    "total_time=%.3fs",
                    self._batch_count,
                    self._total_rows_read,
                    time.perf_counter() - started,
                )
                break

            self._batch_count += 1
            self._total_rows_read += rows_in_batch

            logger.debug(
                "Cohort batch %d fetched: offset=%d, rows=%d, elapsed=%.3fs, "
                "total_rows=%d",
                self._batch_count,
                batch_start_offset,
                rows_in_batch,
                batch_elapsed,
                self._total_rows_read,
            )

            yield frame

            # FIX 3.2: Clean up batch immediately after yielding to free memory
            del frame
            if self.enable_gc:
                gc.collect()

            if rows_in_batch < self.batch_size:
                logger.debug(
                    "End of cohort result set detected (partial batch: %d rows)",
                    rows_in_batch,
                )
                break

            batch_start_offset += self.batch_size


class StreamingAggregator:
    """Compute aggregates over batched data without loading all rows at once."""

    def __init__(self):
        self._count = 0
        self._sum_values = {}
        self._min_values = {}
        self._max_values = {}
        self._null_counts = {}

    def process_batch(
        self,
        frame: pd.DataFrame,
        numeric_columns: Optional[list] = None,
        nullable_columns: Optional[list] = None,
    ) -> None:
        """Process one batch to update running aggregates."""
        self._count += len(frame)

        if numeric_columns:
            for col in numeric_columns:
                if col not in frame.columns:
                    continue
                values = pd.to_numeric(frame[col], errors="coerce")
                if col not in self._sum_values:
                    self._sum_values[col] = 0
                    self._min_values[col] = float("inf")
                    self._max_values[col] = float("-inf")

                valid = values.dropna()
                if len(valid) > 0:
                    self._sum_values[col] += valid.sum()
                    self._min_values[col] = min(
                        self._min_values[col], valid.min()
                    )
                    self._max_values[col] = max(
                        self._max_values[col], valid.max()
                    )

        if nullable_columns:
            for col in nullable_columns:
                if col not in frame.columns:
                    continue
                if col not in self._null_counts:
                    self._null_counts[col] = 0
                self._null_counts[col] += frame[col].isna().sum()

    def result(self) -> dict:
        """Return computed aggregates."""
        return {
            "count": self._count,
            "sums": self._sum_values,
            "mins": self._min_values,
            "maxs": self._max_values,
            "null_counts": self._null_counts,
        }
