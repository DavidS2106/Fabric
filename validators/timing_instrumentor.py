"""Detailed timing instrumentation for validators.

Captures timing for each validator phase and operation without changing
validation logic or result semantics.
"""

from __future__ import annotations

import contextlib
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class TimingOperation:
    """One timed operation within a validator."""
    name: str
    started_at: float
    completed_at: Optional[float] = None
    duration_seconds: Optional[float] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def complete(self) -> None:
        """Mark operation as complete and calculate duration."""
        self.completed_at = time.perf_counter()
        self.duration_seconds = round(self.completed_at - self.started_at, 6)


class ValidatorTimingContext:
    """Collects timing data for one validator execution."""

    def __init__(self, validator_name: str, table_id: str):
        """Initialize timing context.

        Args:
            validator_name: Name of the validator (null_validation, key_validation, etc.)
            table_id: Table being validated
        """
        self.validator_name = validator_name
        self.table_id = table_id
        self.operations: List[TimingOperation] = []
        self._stack: List[str] = []

    @contextlib.contextmanager
    def operation(self, name: str, details: Optional[Dict[str, Any]] = None):
        """Time an operation within the validator.

        Args:
            name: Operation name (e.g., 'source_query', 'target_query', 'comparison')
            details: Optional metadata to attach to the operation

        Yields:
            The TimingOperation instance
        """
        started = time.perf_counter()
        operation = TimingOperation(
            name=name,
            started_at=started,
            details=details or {}
        )
        self.operations.append(operation)

        try:
            yield operation
        finally:
            operation.complete()
            duration = operation.duration_seconds or 0.0
            logger.debug(
                f"[TIMING] validator={self.validator_name} table={self.table_id} "
                f"operation={name} duration={duration:.6f}s"
            )

    def summary(self) -> Dict[str, Any]:
        """Get summary of all operations."""
        if not self.operations:
            return {}

        total_duration = sum(op.duration_seconds or 0.0 for op in self.operations)

        return {
            "validator": self.validator_name,
            "table_id": self.table_id,
            "operation_count": len(self.operations),
            "total_duration": round(total_duration, 6),
            "operations": [
                {
                    "name": op.name,
                    "duration": op.duration_seconds,
                    "details": op.details
                }
                for op in self.operations
            ]
        }

    def log_summary(self) -> None:
        """Log the complete timing summary."""
        summary = self.summary()
        if not summary:
            return

        logger.info(
            f"[VALIDATOR_TIMING] validator={self.validator_name} "
            f"table={self.table_id} total_duration={summary['total_duration']:.6f}s "
            f"operations={summary['operation_count']}"
        )

        for op in self.operations:
            if op.duration_seconds and op.duration_seconds > 0.001:
                logger.debug(
                    f"  - {op.name}: {op.duration_seconds:.6f}s"
                )


class QueryTimingWrapper:
    """Wraps database queries to capture timing without modifying queries."""

    def __init__(self, timing_context: ValidatorTimingContext):
        """Initialize query timing wrapper.

        Args:
            timing_context: ValidatorTimingContext to record timing in
        """
        self.timing_context = timing_context

    def time_query_execution(
        self,
        query_name: str,
        execute_func: Callable[[], Any],
        side: str = "both",
        row_count: Optional[int] = None,
    ) -> Any:
        """Execute a query with timing capture.

        Args:
            query_name: Name of the query (for logging)
            execute_func: Callable that executes the query
            side: "source", "target", or "both"
            row_count: Optional row count returned by query

        Returns:
            Result from execute_func
        """
        details = {"side": side}
        if row_count is not None:
            details["rows"] = row_count

        with self.timing_context.operation(
            name=f"query_{query_name}_{side}",
            details=details
        ) as op:
            result = execute_func()
            if isinstance(result, tuple) and len(result) >= 1:
                first = result[0]
                if hasattr(first, '__len__') and not isinstance(first, str):
                    op.details["rows"] = len(first)
            return result

    def time_comparison(
        self,
        comparison_name: str,
        compare_func: Callable[[], Any],
    ) -> Any:
        """Execute a comparison with timing capture.

        Args:
            comparison_name: Name of the comparison
            compare_func: Callable that performs the comparison

        Returns:
            Result from compare_func
        """
        with self.timing_context.operation(
            name=f"comparison_{comparison_name}",
            details={"phase": "comparison"}
        ):
            return compare_func()


def capture_validator_timing(
    validator_name: str,
    table_id: str,
    validate_func: Callable[[], List[Any]],
) -> tuple[List[Any], Dict[str, Any]]:
    """Wrap a validator call to capture timing.

    Args:
        validator_name: Name of the validator
        table_id: Table ID being validated
        validate_func: The validator's validate method

    Returns:
        Tuple of (results, timing_summary)
    """
    timing_context = ValidatorTimingContext(validator_name, table_id)

    # Time the entire validator execution
    with timing_context.operation(f"validate_{validator_name}"):
        results = validate_func()

    timing_context.log_summary()
    return results, timing_context.summary()
