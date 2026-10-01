"""
fabric_migration_validator/validators/performance_utilities.py
───────────────────────────────────────────────────────────────
Non-invasive performance utilities for existing comparison algorithms.

These utilities enhance performance without changing comparison logic:
- Parameterized SQL where applicable (wraps existing queries)
- Correct IS NULL predicate generation (filter utility)
- Query signature logging (wrap existing execution)
- Safe deterministic batching (utility for WHERE clauses)
- Spill directory safety verification (pre/post checks)
"""

from __future__ import annotations

import hashlib
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


class SQLParameterBuilder:
    """Build safe SQL predicates with parameter binding.

    Wraps existing query building to add IS NULL handling and
    parameterization without changing the comparison algorithm.
    """

    @staticmethod
    def build_in_predicate(
        column: str,
        values: List[Any],
        include_null: bool = False,
    ) -> Tuple[str, List[Any]]:
        """Build safe IN predicate with NULL handling.

        Returns:
            Tuple of (predicate_string, parameters)

        Example:
            >>> predicate, params = SQLParameterBuilder.build_in_predicate(
            ...     "id", [1, 2, None, 3]
            ... )
            >>> predicate
            "id IN (?, ?, ?) OR id IS NULL"
            >>> params
            [1, 2, 3]
        """
        non_null_values = [v for v in values if v is not None and not pd.isna(v)]
        has_null = any(v is None or pd.isna(v) for v in values)

        if not non_null_values and not has_null:
            return "1=0", []  # Empty set

        predicates = []
        params = []

        if non_null_values:
            placeholders = ", ".join(["?" for _ in non_null_values])
            predicates.append(f"{column} IN ({placeholders})")
            params.extend(non_null_values)

        if has_null:
            predicates.append(f"{column} IS NULL")

        return " OR ".join(predicates) if predicates else "1=0", params

    @staticmethod
    def build_equality_predicate(
        column: str,
        value: Any,
    ) -> Tuple[str, List[Any]]:
        """Build safe equality predicate with NULL handling.

        Example:
            >>> predicate, params = SQLParameterBuilder.build_equality_predicate("name", "John")
            >>> predicate, params
            ("name = ?", ["John"])

            >>> predicate, params = SQLParameterBuilder.build_equality_predicate("name", None)
            >>> predicate, params
            ("name IS NULL", [])
        """
        if value is None or pd.isna(value):
            return f"{column} IS NULL", []
        return f"{column} = ?", [value]

    @staticmethod
    def build_composite_key_predicate(
        columns: List[str],
        row_values: List[Dict[str, Any]],
    ) -> Tuple[str, List[Any]]:
        """Build composite key predicate for multiple rows with IS NULL handling.

        Example:
            >>> rows = [
            ...     {"id": 1, "date": "2026-01-01"},
            ...     {"id": 2, "date": None},
            ...     {"id": None, "date": "2026-01-03"},
            ... ]
            >>> predicate, params = SQLParameterBuilder.build_composite_key_predicate(
            ...     ["id", "date"], rows
            ... )
            >>> # Result preserves NULL semantics
        """
        if not row_values:
            return "1=0", []

        row_predicates = []
        params = []

        for row in row_values:
            col_predicates = []
            for col in columns:
                val = row.get(col)
                if val is None or pd.isna(val):
                    col_predicates.append(f"{col} IS NULL")
                else:
                    col_predicates.append(f"{col} = ?")
                    params.append(val)

            row_predicates.append(f"({' AND '.join(col_predicates)})")

        return " OR ".join(row_predicates), params


class QuerySignatureLogger:
    """Log query signatures without exposing SQL text.

    Wraps query execution to log only fingerprint, metrics, and timing.
    Does not change query or comparison logic.
    """

    def __init__(self, phase: str = "validation"):
        """Initialize query logger.

        Args:
            phase: Validation phase name (preflight, smoke_test, etc.)
        """
        self.phase = phase
        self.queries: List[Dict[str, Any]] = []
        self._start_time = time.perf_counter()

    def log_query(
        self,
        sql: str,
        row_count: int,
        execution_time_seconds: float,
        side: str = "source",
    ) -> None:
        """Log a query execution by signature only.

        Args:
            sql: Query SQL (not stored in log)
            row_count: Rows returned by query
            execution_time_seconds: Query execution time
            side: "source" or "target"
        """
        signature = self._compute_signature(sql)

        entry = {
            "signature": signature,
            "side": side,
            "rows": row_count,
            "time_seconds": round(execution_time_seconds, 6),
            "sequence": len(self.queries) + 1,
        }
        self.queries.append(entry)

        logger.debug(
            "Query logged | sig=%s | side=%s | rows=%d | time=%.3fs",
            signature[:8],
            side,
            row_count,
            execution_time_seconds,
        )

    def get_summary(self) -> Dict[str, Any]:
        """Get query execution summary (metrics only, no SQL).

        Returns:
            Dictionary with aggregated query statistics
        """
        if not self.queries:
            return {
                "phase": self.phase,
                "total_queries": 0,
                "total_rows": 0,
                "total_time_seconds": 0,
            }

        return {
            "phase": self.phase,
            "total_queries": len(self.queries),
            "total_rows": sum(q["rows"] for q in self.queries),
            "total_time_seconds": round(
                sum(q["time_seconds"] for q in self.queries), 6
            ),
            "avg_time_per_query": round(
                sum(q["time_seconds"] for q in self.queries) / len(self.queries), 6
            ),
            "slowest_query_time": round(max(q["time_seconds"] for q in self.queries), 6),
            "fastest_query_time": round(min(q["time_seconds"] for q in self.queries), 6),
            "queries_by_side": {
                "source": len([q for q in self.queries if q["side"] == "source"]),
                "target": len([q for q in self.queries if q["side"] == "target"]),
            },
        }

    @staticmethod
    def _compute_signature(sql: str) -> str:
        """Compute query signature (hash, not SQL).

        Args:
            sql: Query SQL

        Returns:
            16-character hex signature
        """
        normalized = sql.strip().upper()
        normalized = " ".join(normalized.split())  # Collapse whitespace
        return hashlib.sha256(normalized.encode()).hexdigest()[:16]


class DeterministicBatchBuilder:
    """Build safe batches with deterministic ordering.

    Ensures batching is safe by verifying ORDER BY requirement
    without changing the comparison algorithm.
    """

    @staticmethod
    def validate_batch_safety(sql: str) -> Tuple[bool, Optional[str]]:
        """Verify SQL is safe for OFFSET/FETCH batching.

        Returns:
            Tuple of (is_safe, error_message)
        """
        sql_upper = sql.strip().upper()

        # Must have ORDER BY for deterministic batching
        if " ORDER BY " not in sql_upper:
            return False, (
                "Batching with OFFSET/FETCH requires deterministic ORDER BY clause. "
                "Database result sets are unordered without ORDER BY, causing rows to "
                "be duplicated or skipped between batches."
            )

        return True, None

    @staticmethod
    def add_offset_fetch(sql: str, offset: int, limit: int) -> str:
        """Append OFFSET/FETCH to SQL safely.

        Args:
            sql: SQL query with ORDER BY
            offset: Row offset
            limit: Row limit

        Returns:
            SQL with OFFSET/FETCH appended
        """
        is_safe, error = DeterministicBatchBuilder.validate_batch_safety(sql)
        if not is_safe:
            raise ValueError(error)

        sql_trimmed = sql.rstrip()
        if sql_trimmed.endswith(";"):
            sql_trimmed = sql_trimmed[:-1]

        return f"{sql_trimmed} OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"


class SpillDirectorySafetyManager:
    """Verify spill directory safety before/after use.

    Wraps existing spill directory logic with verification checks.
    Does not change spill strategy, only adds pre/post verification.
    """

    def __init__(self, directory: str):
        """Initialize safety manager.

        Args:
            directory: Spill directory path
        """
        self.directory = Path(directory)
        self.files_created: List[Path] = []

    def verify_writable(self) -> Tuple[bool, Optional[str]]:
        """Verify directory is writable before use.

        Returns:
            Tuple of (is_writable, error_message)
        """
        try:
            # Try to create test file
            self.directory.mkdir(parents=True, exist_ok=True)
            test_file = self.directory / ".write_test"
            test_file.write_text("test", encoding="utf-8")
            test_file.unlink()
            return True, None
        except Exception as exc:
            return False, f"Spill directory not writable: {exc}"

    def track_file(self, file_path: Path) -> None:
        """Track file created in spill directory.

        Args:
            file_path: Path to file created in spill directory
        """
        self.files_created.append(file_path)

    def cleanup_created_files(self) -> Tuple[int, Optional[str]]:
        """Remove only files created for this operation.

        Returns:
            Tuple of (files_removed, error_message)
        """
        removed_count = 0
        errors = []

        for file_path in self.files_created:
            try:
                if file_path.exists():
                    file_path.unlink()
                    removed_count += 1
            except Exception as exc:
                errors.append(f"Could not remove {file_path}: {exc}")

        error_msg = "; ".join(errors) if errors else None
        return removed_count, error_msg

    def verify_cleanup(self) -> Tuple[bool, Optional[str]]:
        """Verify created files were actually removed.

        Returns:
            Tuple of (all_removed, error_message)
        """
        remaining = [f for f in self.files_created if f.exists()]
        if remaining:
            return False, f"Cleanup incomplete, files remain: {remaining}"
        return True, None


class ExceptionSafetyHandler:
    """Safely handle exceptions while preserving outer flow.

    Wraps existing error handling to ensure one-table failure
    doesn't terminate the complete validation run.
    """

    @staticmethod
    def safe_validate_table(
        validate_func,
        table_name: str,
        *args,
        **kwargs,
    ) -> Tuple[Optional[Any], Optional[Exception]]:
        """Execute validation with exception safety.

        Returns:
            Tuple of (result, exception)
            - If success: (result_object, None)
            - If error: (None, exception_object)
        """
        try:
            result = validate_func(*args, **kwargs)
            return result, None
        except Exception as exc:
            logger.exception(
                "Table validation failed (continuing with next table) | table=%s | error=%s",
                table_name,
                exc,
            )
            return None, exc


class PerformanceMetricsCollector:
    """Collect performance metrics without changing algorithm.

    Tracks timing across validation phases and operations.
    """

    def __init__(self):
        """Initialize metrics collector."""
        self.timings: Dict[str, List[float]] = {}
        self.counts: Dict[str, int] = {}

    def record_timing(self, operation: str, seconds: float) -> None:
        """Record timing for operation.

        Args:
            operation: Operation name (e.g., "query_execution", "comparison")
            seconds: Duration in seconds
        """
        if operation not in self.timings:
            self.timings[operation] = []
        self.timings[operation].append(seconds)

    def record_count(self, metric: str, value: int) -> None:
        """Record count metric.

        Args:
            metric: Metric name (e.g., "queries_executed", "rows_compared")
            value: Count value
        """
        self.counts[metric] = self.counts.get(metric, 0) + value

    def get_summary(self) -> Dict[str, Any]:
        """Get performance summary.

        Returns:
            Dictionary with timing and count metrics
        """
        timing_summary = {}
        for op, times in self.timings.items():
            timing_summary[op] = {
                "total_seconds": round(sum(times), 6),
                "avg_seconds": round(sum(times) / len(times), 6) if times else 0,
                "count": len(times),
            }

        return {
            "timings": timing_summary,
            "counts": self.counts,
        }
