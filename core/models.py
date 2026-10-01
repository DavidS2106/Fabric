"""
fabric_migration_validator/core/models.py
──────────────────────────────────────────
Typed result models used across all validators and reporters.
"""

from __future__ import annotations

import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


INCREMENTAL_UPDATE_EVIDENCE_LIMIT = 100
SERVER_2_MATCH_EVIDENCE_LIMIT = 100

# ─────────────────────────────────────────────────────────────
# Validation Status
# ─────────────────────────────────────────────────────────────

class ValidationStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"
    SKIP = "SKIP"
    ERROR = "ERROR"
    FRAMEWORK_ERROR = "FRAMEWORK_ERROR"
    CONFIG_ERROR = "CONFIG_ERROR"
    WARNING = "WARNING"


# ─────────────────────────────────────────────────────────────
# Migration Phase
# ─────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────
# Atomic Check Result
# ─────────────────────────────────────────────────────────────

@dataclass
class CheckResult:
    """
    Single atomic validation result.
    Example:
        - row_count
        - duplicate_check
        - referential_integrity
        - record_comparison
    """

    check_name: str
    status: ValidationStatus

    source_value: Any = None
    target_value: Any = None

    delta: Any = None
    delta_pct: float = 0.0

    message: str = ""

    details: Dict[str, Any] = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────
# Record-Level Results
# ─────────────────────────────────────────────────────────────

#: Record-level evidence carries the row-hash column that the source or the
#: target table already stores, when that table exposes it. The value is read
#: from the record it belongs to and is never computed, derived, or copied
#: across sides: `source_row_hash_value` only ever holds a source table's own
#: value and `target_row_hash_value` only ever holds a target table's own.
#: A table without the column leaves the field `None`.

@dataclass
class MatchedRecord:
    table_id: str
    primary_key: str
    business_key: str
    source_hash: str
    target_hash: str
    match_status: str = "MATCH"
    timestamp: datetime = field(default_factory=datetime.utcnow)
    source_row_hash_value: Optional[str] = None
    target_row_hash_value: Optional[str] = None


@dataclass
class MismatchedRecord:
    table_id: str
    primary_key: str
    column_name: str
    source_value: Any
    target_value: Any
    difference_type: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    source_row_hash_value: Optional[str] = None
    target_row_hash_value: Optional[str] = None


@dataclass
class MissingRecord:
    table_id: str
    primary_key: str
    record_data: str
    location: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    #: The row hash of the single table this record actually exists in.
    row_hash_value: Optional[str] = None


# ─────────────────────────────────────────────────────────────
# Table Validation Result
# ─────────────────────────────────────────────────────────────

@dataclass
class TableValidationResult:
    """
    All validation results for a single source→target table.
    """

    table_id: str
    source_system: str

    source_fqn: str
    target_fqn: str

    target_layer: str
    migration_phase: str

    tags: List[str]

    checks: List[CheckResult] = field(default_factory=list)

    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    #: Monotonic (``time.perf_counter``) wall time of the complete table run,
    #: set by the orchestrator; ``duration_seconds`` prefers it over the
    #: timestamp difference.
    measured_duration_seconds: Optional[float] = None

    error_message: Optional[str] = None

    # Configuration-derived execution and scalability metrics.
    execution_metrics: Dict[str, Any] = field(default_factory=dict)

    # Loaded from comparison_settings.status_policy for repository runs.
    status_policy: Dict[str, Any] = field(default_factory=dict)

    # Record-level validation details
    matched_records: List[MatchedRecord] = field(default_factory=list)
    mismatched_records: List[MismatchedRecord] = field(default_factory=list)
    missing_in_source: List[MissingRecord] = field(default_factory=list)
    missing_in_target: List[MissingRecord] = field(default_factory=list)

    # Bounded successful alternate-source matches used only by the per-table
    # Excel writer. Declaring the field makes the evidence survive table
    # result projection/copying performed between validation and reporting.
    server_2_match_evidence: List[Dict[str, Any]] = field(
        default_factory=list, repr=False
    )

    # ─────────────────────────────────────────────────────────
    # Enterprise Overall Status Aggregation
    # ─────────────────────────────────────────────────────────

    @property
    def overall_status(self) -> ValidationStatus:
        """
        Enterprise-grade validation aggregation logic.

        Priority:
            ERROR/FAIL > PASS > WARN > SKIP

        Rules:
        - Any FAIL or ERROR => FAIL
        - Any PASS and no FAIL => PASS
        - WARN only if no PASS/FAIL
        - All SKIP => SKIP
        """

        # No validations executed
        if not self.checks:
            return (
                ValidationStatus.FRAMEWORK_ERROR
                if self.error_message else ValidationStatus.SKIP
            )

        statuses = []

        for chk in self.checks:

            if chk is None:
                continue

            if chk.status is None:
                continue

            try:
                status = chk.status.value.upper()
            except Exception:
                status = str(chk.status).upper()

            statuses.append(status)

        # Empty after filtering
        if not statuses:
            return ValidationStatus.SKIP

        counter = Counter(statuses)

        impact = self.status_policy.get("table_impact", {})
        severity = self.status_policy.get("severity_order", [])
        if impact and severity:
            impacted = {impact.get(status, status) for status in statuses}
            for status_name in severity:
                if status_name in impacted:
                    return ValidationStatus(status_name)
            return ValidationStatus.SKIP

        # Compatibility for programmatic callers that do not load YAML policy.
        if counter["CONFIG_ERROR"]:
            return ValidationStatus.CONFIG_ERROR
        if counter["FRAMEWORK_ERROR"] or counter["ERROR"]:
            return ValidationStatus.FRAMEWORK_ERROR
        if counter["FAIL"]:
            return ValidationStatus.FAIL
        if counter["WARNING"] or counter["WARN"]:
            return ValidationStatus.WARNING
        if counter["PASS"]:
            return ValidationStatus.PASS
        return ValidationStatus.SKIP

    # ─────────────────────────────────────────────────────────
    # Derived Metrics
    # ─────────────────────────────────────────────────────────

    @property
    def pass_count(self) -> int:
        return sum(
            1
            for c in self.checks
            if c.status == ValidationStatus.PASS
        )

    @property
    def fail_count(self) -> int:
        return sum(
            1
            for c in self.checks
            if c.status == ValidationStatus.FAIL
        )

    @property
    def framework_error_count(self) -> int:
        return sum(
            1 for c in self.checks
            if c.status in (ValidationStatus.ERROR, ValidationStatus.FRAMEWORK_ERROR)
        )

    @property
    def config_error_count(self) -> int:
        return sum(1 for c in self.checks if c.status == ValidationStatus.CONFIG_ERROR)

    @property
    def warn_count(self) -> int:
        return sum(
            1
            for c in self.checks
            if c.status in (ValidationStatus.WARN, ValidationStatus.WARNING)
        )

    @property
    def skip_count(self) -> int:
        return sum(
            1
            for c in self.checks
            if c.status == ValidationStatus.SKIP
        )

    @property
    def total_checks(self) -> int:
        return len(self.checks)

    @property
    def pass_rate_pct(self) -> float:

        executable_checks = [
            c for c in self.checks
            if c.status != ValidationStatus.SKIP
        ]

        if not executable_checks:
            return 0.0

        passed = sum(
            1
            for c in executable_checks
            if c.status == ValidationStatus.PASS
        )

        return round(
            (passed / len(executable_checks)) * 100,
            2
        )

    @property
    def duration_seconds(self) -> float:
        """Wall time of the complete table run.

        The orchestrator measures the run with a monotonic clock around
        everything the table does (shared statistics, every validator,
        cleanup) and stores it in ``measured_duration_seconds``; that value is
        authoritative. The timestamp difference is the fallback for results
        built outside the orchestrator.
        """
        measured = self.measured_duration_seconds
        if isinstance(measured, (int, float)) and not isinstance(measured, bool):
            return float(measured)
        if self.completed_at:
            return (
                self.completed_at - self.started_at
            ).total_seconds()

        return 0.0


# ─────────────────────────────────────────────────────────────
# Run Summary
# ─────────────────────────────────────────────────────────────

@dataclass
class RunSummary:
    """
    Top-level execution summary.
    """

    run_id: str = field(
        default_factory=lambda: str(uuid.uuid4())[:8].upper()
    )

    project_name: str = ""

    started_at: datetime = field(default_factory=datetime.utcnow)

    completed_at: Optional[datetime] = None

    table_results: List[TableValidationResult] = field(default_factory=list)

    source_systems: List[str] = field(default_factory=list)

    execution_metrics: Dict[str, Any] = field(default_factory=dict)

    # ─────────────────────────────────────────────────────────
    # Summary Metrics
    # ─────────────────────────────────────────────────────────

    @property
    def total_tables(self) -> int:
        return len(self.table_results)

    @property
    def passed_tables(self) -> int:
        return sum(
            1
            for r in self.table_results
            if r.overall_status == ValidationStatus.PASS
        )

    @property
    def failed_tables(self) -> int:
        return sum(
            1
            for r in self.table_results
            if r.overall_status == ValidationStatus.FAIL
        )

    @property
    def error_tables(self) -> int:
        return sum(
            1
            for r in self.table_results
            if r.overall_status in (
                ValidationStatus.ERROR, ValidationStatus.FRAMEWORK_ERROR
            )
        )

    @property
    def config_error_tables(self) -> int:
        return sum(
            1 for r in self.table_results
            if r.overall_status == ValidationStatus.CONFIG_ERROR
        )

    @property
    def warn_tables(self) -> int:
        return sum(
            1
            for r in self.table_results
            if r.overall_status in (ValidationStatus.WARN, ValidationStatus.WARNING)
        )

    @property
    def skipped_tables(self) -> int:
        return sum(
            1
            for r in self.table_results
            if r.overall_status == ValidationStatus.SKIP
        )

    @property
    def pass_rate_pct(self) -> float:

        executable_tables = [
            r for r in self.table_results
            if r.overall_status != ValidationStatus.SKIP
        ]

        if not executable_tables:
            return 0.0

        passed = sum(
            1
            for r in executable_tables
            if r.overall_status == ValidationStatus.PASS
        )

        return round(
            (passed / len(executable_tables)) * 100,
            2
        )

    @property
    def overall_status(self) -> ValidationStatus:
        """Priority: FAIL/ERROR > PASS > WARN > SKIP"""
        if self.config_error_tables > 0:
            return ValidationStatus.CONFIG_ERROR
        if self.error_tables > 0:
            return ValidationStatus.FRAMEWORK_ERROR
        if self.failed_tables > 0:
            return ValidationStatus.FAIL
        
        executable = [r for r in self.table_results if r.overall_status != ValidationStatus.SKIP]
        if not executable:
            return ValidationStatus.SKIP
            
        if self.passed_tables == len(executable):
            return ValidationStatus.PASS
            
        if self.warn_tables > 0:
            return ValidationStatus.WARNING
            
        return ValidationStatus.SKIP

    @property
    def duration_seconds(self) -> float:

        if self.completed_at:
            return (
                self.completed_at - self.started_at
            ).total_seconds()

        return 0.0

    # ─────────────────────────────────────────────────────────
    # Breakdown Utilities
    # ─────────────────────────────────────────────────────────

    def by_layer(self) -> Dict[str, List[TableValidationResult]]:

        result = {}

        for r in self.table_results:
            result.setdefault(
                r.target_layer,
                []
            ).append(r)

        return result

    def by_status(self) -> Dict[str, List[TableValidationResult]]:

        result = {}

        for r in self.table_results:
            result.setdefault(
                r.overall_status.value,
                []
            ).append(r)

        return result

    # ─────────────────────────────────────────────────────────
    # Flat Failure Export
    # ─────────────────────────────────────────────────────────

    def failed_checks_flat(self) -> List[Dict]:

        rows = []

        for tr in self.table_results:

            for chk in tr.checks:

                if chk.status == ValidationStatus.FAIL:

                    rows.append({
                        "table_id": tr.table_id,
                        "source": tr.source_fqn,
                        "target": tr.target_fqn,
                        "layer": tr.target_layer,
                        "check": chk.check_name,
                        "status": chk.status.value,
                        "source_value": chk.source_value,
                        "target_value": chk.target_value,
                        "delta": chk.delta,
                        "delta_pct": chk.delta_pct,
                        "message": chk.message,
                    })

        return rows
