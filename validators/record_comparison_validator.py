from __future__ import annotations

from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import hashlib
import base64
import gc
import json
import logging
import math
import numbers
import os
import pickle
from pathlib import Path
import threading
import time
from types import SimpleNamespace
import unicodedata
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import numpy as np
import pandas as pd

from core.connection_manager import (
    _is_query_timeout_error,
    _is_retryable_database_error,
    _is_transient_connection_error,
    error_class_of,
    normalize_connection_type,
    sanitize_error_text,
)
from core.models import (
    CheckResult,
    INCREMENTAL_UPDATE_EVIDENCE_LIMIT,
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
from core.sql_dialect import SqlDialectError, bind_sql, resolve_dialect
from core.validation_policy import (
    ValidationPolicies, ValidationPolicyError, exception_category,
)
from core.value_normalization import (
    is_null_scalar, normalize_decimal_for_hash, temporal_token, temporal_values_match,
)
from .base_validator import BaseValidator
from .comparison_kernels import ValueComparator
from .hash_spill import open_hash_spill_store
from .incremental_classification import (
    HashOperationClassifier,
    IncrementalPolicy,
    OperationClassifier,
    combine_statuses,
    not_performed_details,
    policy_violation_category,
)
from .key_cohort import KeyCohortBuilder
from .streaming_reader import StreamingBatchReader
from .validator_utils import _fqn


logger = logging.getLogger(__name__)

#: Rows fetched per round trip when the keyless full-scan comparison pages a
#: table. Independent of the total row budget: this only sizes what is in
#: flight, so it can stay modest however large the budget is.
FULL_SCAN_PAGE_ROWS = 50_000

#: Rows the keyless full-scan comparison may hold per side when
#: ``failure_fallback.sample_record_limit`` is absent or unusable.
DEFAULT_FULL_SCAN_ROW_BUDGET = 1_000_000

#: Maximum rows whose canonical byte representations are materialized at once
#: while hashing a streamed database batch.  The database fetch size can be
#: much larger; hashing is subdivided so wide/text-heavy tables do not multiply
#: one whole batch into an ``encoded_columns`` object in memory.
HASH_CANONICAL_CHUNK_ROWS = 1_000

# ── Sampled-comparison target lookup defaults ─────────────────────────
# Every value below is only the fallback for an absent configuration key.
# The sample size (how many source rows are examined) and the lookup batch
# size (how many of those rows one target statement asks about) are separate
# settings on purpose: the former is a coverage decision, the latter a SQL
# size decision. Deriving one from the other is what produced a single
# statement carrying the entire 100K sample.

#: Keys per target statement for keyed cohorts
#: (``comparison_settings.comparison_tiers.target_key_lookup_batch_size``).
DEFAULT_KEYED_LOOKUP_BATCH_SIZE = 5_000
#: Sampled rows per candidate lookup for keyless cohorts
#: (``comparison_settings.comparison_tiers.target_keyless_lookup_batch_size``).
DEFAULT_KEYLESS_LOOKUP_BATCH_SIZE = 5_000
#: Compared columns used as the database-side candidate discriminator for a
#: keyless cohort (``comparison_tiers.keyless_discriminator_columns``).
DEFAULT_KEYLESS_DISCRIMINATOR_COLUMNS = 3
#: Widest discriminator a cohort may progressively grow to when its candidates
#: exceed the budget (``comparison_tiers.keyless_discriminator_max_columns``).
#: The widening stays a set-based VALUES join on a few columns; the wide
#: per-row predicate form is never generated.
DEFAULT_KEYLESS_DISCRIMINATOR_MAX_COLUMNS = 12
#: Rows one streamed candidate frame carries when a cohort runs without a
#: candidate budget (widest discriminator reached; retention stays bounded).
UNBOUNDED_CANDIDATE_PAGE_ROWS = 20_000
#: Candidate rows one lookup batch may return per sampled row before the exact
#: bounded fallback is used (``comparison_tiers.keyless_candidate_row_multiplier``).
DEFAULT_KEYLESS_CANDIDATE_ROW_MULTIPLIER = 4
#: Generated-statement ceilings (``comparison_settings.query_safety``).
DEFAULT_MAX_SQL_LENGTH_BYTES = 5_000_000
DEFAULT_MAX_PREDICATES_PER_QUERY = 5_000
#: Exception category of the FAIL result reported when a configured
#: ``key_mappings`` or ``compare_column_mappings`` column does not exist in the
#: actual source or target metadata. The comparison is then not executed: no
#: statement naming the column is generated, retried or repaired.
CONFIGURED_COLUMN_NOT_FOUND = "CONFIGURED_COLUMN_NOT_FOUND"
#: Sampled comparisons that may run at once across all tables
#: (``comparison_settings.large_table_sampling.max_concurrent_comparisons``).
DEFAULT_MAX_CONCURRENT_LARGE_COMPARISONS = 1
#: Used only when no ``performance.cohort_processing`` block reaches the
#: validator (direct invocation without an orchestrator).
DEFAULT_VALUES_BLOCK_ROWS = 500
DEFAULT_PREDICATE_OR_MAX_KEYS = 100
#: Statements echoed into the ``target_sql`` detail when a lookup ran many.
STATEMENT_DETAIL_LIMIT = 3
#: Target lookup batches in flight at once inside one sampled comparison when
#: neither ``comparison_tiers.target_lookup_parallelism`` nor an enabled
#: ``performance.parallel_execution.batch_parallelism`` block is configured.
#: 1 keeps the historical sequential behaviour for unconfigured deployments.
DEFAULT_TARGET_LOOKUP_PARALLELISM = 1
#: Connections a SQLAlchemy engine holds when the configured pool is disabled
#: (SQLAlchemy's own QueuePool default); used only to cap batch parallelism.
SQLALCHEMY_DEFAULT_POOL_CAPACITY = 5
#: Adaptive parallelism defaults (``comparison_tiers.adaptive_parallelism``).
DEFAULT_ADAPTIVE_PARALLELISM = {"enabled": True, "min_workers": 1, "reduce_factor": 0.5}


#: Exact value types whose canonical hash token is a pure function of
#: ``(type, value)``; memoized per column while hashing a frame.
_HASH_MEMO_TYPES = frozenset({str, int, bool, bytes})


class _CandidateBudgetExceeded(Exception):
    """A keyless candidate lookup returned more rows than its batch budget."""


@dataclass
class _LookupBatch:
    """One logical target lookup batch, identified by its position."""

    index: int
    payload: Any
    rows: int


@dataclass
class _LookupBatchResult:
    """What one executed lookup batch hands back to the ordered consumer.

    Everything a worker measured is carried here and merged into the shared
    instrumentation by the consuming thread, so no worker ever writes to the
    shared ``timings`` dictionary.
    """

    index: int
    frames: List[pd.DataFrame] = field(default_factory=list)
    #: Keyless only: the canonical row hash of every row in ``frames`` (one
    #: list per frame, same order), computed once on the worker.
    hashes: List[List[str]] = field(default_factory=list)
    #: Empty frame carrying the result columns (for an all-empty cohort).
    template: Optional[pd.DataFrame] = None
    statements: List[str] = field(default_factory=list)
    strategy: Optional[str] = None
    rows: int = 0
    candidate_rows: int = 0
    #: Keyless only: exact all-column matches before the comparison retention
    #: cap. This measures the looked-up cohort without changing its semantics.
    matching_rows: int = 0
    matching_counts: Counter = field(default_factory=Counter)
    used_fallback: bool = False
    elapsed_seconds: float = 0.0
    sql_generation_seconds: float = 0.0
    target_execute_seconds: float = 0.0
    target_extract_seconds: float = 0.0
    connection_wait_seconds: float = 0.0
    db_execution_seconds: float = 0.0
    hash_seconds: float = 0.0
    peak_frame_rows: int = 0
    max_sql_bytes: int = 0
    safety_splits: int = 0
    attempts: int = 1
    #: Keyless only: how many times the discriminator was widened for this
    #: cohort, the tier it started from, the final width, and whether the
    #: widest tier ran unbudgeted.
    widened: int = 0
    start_tier: int = 0
    discriminator_width: int = 0
    unbounded: bool = False


class _TierFloor:
    """Lowest discriminator tier the remaining keyless cohorts still attempt.

    A discriminator width that already proved too weak for one cohort of a
    table is not rediscovered by every later cohort: the floor rises to the
    tier that cohort had to widen to, and subsequent cohorts start there.

    This is a scheduling optimization with no effect on results. Tiers are
    nested prefixes, so starting at a wider tier filters target candidates on
    a superset of the base equality predicates; a genuine match is identical
    on every compared column and therefore satisfies any subset of them, so
    no true candidate can be excluded. Retention is decided afterwards by the
    canonical all-column row hash, which is unchanged. The floor only ever
    rises, so it cannot oscillate between cohorts.
    """

    __slots__ = ("_index", "_lock")

    def __init__(self) -> None:
        self._index = 0
        self._lock = threading.Lock()

    @property
    def index(self) -> int:
        with self._lock:
            return self._index

    def raise_to(self, index: int) -> None:
        with self._lock:
            if index > self._index:
                self._index = index


class _LookupBatchFailed(RuntimeError):
    """A lookup batch failed; carries safe metadata for logging and decisions."""

    def __init__(self, message: str, *, index: int, rows: int, sql_bytes: int,
                 elapsed_seconds: float, error_type: str, cause: Exception) -> None:
        super().__init__(message)
        self.index = index
        self.rows = rows
        self.sql_bytes = sql_bytes
        self.elapsed_seconds = elapsed_seconds
        self.error_type = error_type
        self.cause = cause


class RecordComparisonValidator(BaseValidator):
    """Execute profile-driven sampled or complete source-to-target comparison."""

    IMPLEMENTED_STRATEGIES = {"random", "deterministic"}

    #: Process-wide gates bounding how many sampled (large-table) comparisons
    #: hold their frames at the same time, keyed by the configured limit.
    _LARGE_COMPARISON_GATES: Dict[int, threading.BoundedSemaphore] = {}
    _LARGE_COMPARISON_GATE_LOCK = threading.Lock()

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
        started = time.perf_counter()
        # Incremental operation classification (INSERT / UPDATE / DELETE /
        # REPLAY / UNCHANGED / UNKNOWN) runs inside the comparison engines when
        # the resolved incremental policy enables it. Each comparison result is
        # combined with the configured operation policy exactly once, by
        # ``_finalize_incremental_result``, so the same fact reaches Excel and
        # JSON through one result object.
        self._incremental_policy = None
        self._incremental_classifier = None
        self._incremental_not_performed_reason = None
        self._incremental_cdc_counts = None
        # Physical execution controls (spill engine, temp-storage safety,
        # adaptive fetch sizing). They never change what is compared.
        self._execution_config = execution_config
        validations_config = validations_config or {}
        global_config = validations_config.get("record_level_comparison", {}) or {}
        if isinstance(global_config, bool):
            global_config = {"enabled": global_config}
        profile_config = comparison_config or {}
        if not isinstance(profile_config, dict) or not isinstance(global_config, dict):
            return [self._error("Record comparison configuration must be a mapping", started)]
        config = {**global_config, **profile_config}

        if not config.get("enabled", False):
            return []

        comparison_settings = validations_config.get("comparison_settings", {}) or {}
        if not isinstance(comparison_settings, dict):
            return [self._error("comparison_settings must be a mapping", started)]
        update_evidence_limit = comparison_settings.get(
            "incremental_update_evidence_limit", INCREMENTAL_UPDATE_EVIDENCE_LIMIT
        )
        if not self._is_nonnegative_int(update_evidence_limit):
            return [self._error(
                "comparison_settings.incremental_update_evidence_limit must be a "
                "non-negative integer", started,
            )]
        self._incremental_update_evidence_limit = int(update_evidence_limit)
        self._large_table_processing = self._large_table_processing_config(
            comparison_settings.get("large_table_processing")
        )
        self._hash_comparison_config = dict(
            comparison_settings.get("hash_comparison") or {}
        )
        try:
            self._policies = ValidationPolicies.from_config(validations_config)
        except ValidationPolicyError as exc:
            return [self._error(
                str(exc), started,
                status=ValidationStatus.CONFIG_ERROR,
                exception_category=exception_category(exc),
                table_id=getattr(table_map, "id", None),
            )]
        configured_key_source = comparison_settings.get(
            "key_mapping_source", {"type": "table_yaml"}
        ) or {}
        if (
            not isinstance(configured_key_source, dict)
            or configured_key_source.get("type", "table_yaml") != "table_yaml"
        ):
            return [
                self._error(
                    "comparison_settings.key_mapping_source.type must be table_yaml",
                    started,
                )
            ]

        full_policy = self._policies.full_comparison
        full_mode = full_policy["mode_name"]
        # Per-statement retry policy for every cohort read this table issues
        # (the keyed full path re-applies the same block).
        batching_policy = full_policy.get("batching")
        self._batch_config = batching_policy if isinstance(batching_policy, dict) else None
        self._batch_retry_count = 0
        mode = config.get("mode", comparison_settings.get("default_mode"))
        if mode is None:
            return [self._error(
                "Comparison mode must be configured in validations.yml",
                started,
                status=self._policies.configured_status("configuration_error_status"),
            )]
        sampling_settings = comparison_settings.get("sampling")
        if not isinstance(sampling_settings, dict) or not isinstance(
            sampling_settings.get("enabled"), bool
        ):
            return [self._error(
                "comparison_settings.sampling.enabled must be configured",
                started,
                status=self._policies.configured_status("configuration_error_status"),
                exception_category="CONFIGURATION_POLICY",
                table_id=getattr(table_map, "id", None),
            )]
        if not sampling_settings["enabled"]:
            mode = full_mode
        supported_modes = comparison_settings.get("supported_modes")
        key_mapping_issue = None
        try:
            key_mappings = self._key_mappings(table_map)
        except (AttributeError, TypeError, ValueError) as exc:
            key_mappings = []
            key_mapping_issue = f"Invalid key_mappings: {exc}"
        # The key mappings exactly as the table YAML configures them. The
        # metadata precheck validates these, whichever strategy runs later.
        configured_key_mappings = list(key_mappings)
        resolved_scope = self._resolved_scope(table_map)
        self._incremental_policy = IncrementalPolicy.from_scope(resolved_scope)
        if (
            resolved_scope is not None
            and resolved_scope.details.get("incremental_strategy") == "full_hash"
            and not resolved_scope.details.get("preserve_key_mappings", True)
        ):
            # Only when the configured fallback explicitly asks for keyless
            # processing are valid key mappings set aside; otherwise the
            # snapshot fallback keeps the keyed comparison the table configured.
            key_mappings = []
            key_mapping_issue = "Explicit incremental full-hash strategy"
        strategy: Optional[str] = None
        base_details = self._metric_details(
            mode,
            strategy,
            key_mappings,
            validation_profile=config.get("validation_profile"),
        )
        base_details["table_id"] = getattr(table_map, "id", None)
        self._current_table_id = getattr(table_map, "id", None)
        self._sample_timings = None
        self._sample_hash_cache = None
        self._lookup_matching_counts = Counter()
        self._held_gate = None
        try:
            large_table_sampling = self._large_table_sampling_config(
                comparison_settings.get("large_table_sampling")
            )
            # Target lookup batching, statement safety ceilings and the
            # large-comparison concurrency gate. Resolved once here so the
            # normal sample path and the failure fallback share one setting.
            self._sample_lookup = self._resolve_sample_lookup_config(
                comparison_settings
            )
        except ValueError as exc:
            return [self._error(str(exc), started, base_details)]
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
        # Every configured key and comparison column must exist in the actual
        # source and target metadata before any comparison statement is built.
        precheck_failure = self._configured_column_precheck(
            source_conn, target_conn, table_map, src_fqn, tgt_fqn,
            source_dialect, target_dialect, configured_key_mappings,
            base_details, started,
            extra_columns=self._incremental_projection_columns(),
        )
        if precheck_failure is not None:
            return [precheck_failure]
        source_count = target_count = None
        automatic_sample_size = None
        if large_table_sampling["enabled"]:
            try:
                if resolved_scope is not None and resolved_scope.mode == "incremental":
                    source_count, target_count = self._incremental_row_counts(
                        execution_cache, source_conn, target_conn, src_fqn, tgt_fqn,
                        source_where, target_where, source_dialect, target_dialect,
                    )
                else:
                    source_count, target_count = self._cached_row_counts(execution_cache)
            except ValueError as exc:
                return [self._error(str(exc), started, base_details)]
            except Exception as exc:
                return [self._error(
                    f"Scoped count query failed ({type(exc).__name__})", started, base_details,
                    status=ValidationStatus.ERROR, exception_category="QUERY_EXECUTION",
                )]
            configured_sample_size = large_table_sampling["sample_size"]
            base_details["configured_sample_size"] = configured_sample_size
            base_details["source_row_count"] = source_count
            base_details["target_row_count"] = target_count
            if self._large_table_sampling_applies(
                source_count, target_count, large_table_sampling
            ):
                mode = "sample_comparison"
                driver_count = (
                    target_count
                    if self._incremental_policy is not None
                    and self._incremental_policy.target_driven
                    else source_count
                )
                automatic_sample_size = min(configured_sample_size, driver_count)
                base_details["execution_mode"] = "SAMPLE_COMPARISON"
                base_details["full_comparison_performed"] = False
                base_details["total_source_rows"] = source_count
                base_details["total_target_rows"] = target_count
                base_details["configured_sample_size"] = configured_sample_size
                base_details["actual_sample_size"] = automatic_sample_size
                base_details["comparison_mode"] = "sample_comparison"
                base_details["record_comparison_mode"] = "sample_comparison"
            else:
                base_details["execution_mode"] = "FULL_COMPARISON"
                base_details["full_comparison_performed"] = True
                base_details["total_source_rows"] = source_count
                base_details["total_target_rows"] = target_count
        if (mode != "sample_comparison"
                and self._incremental_policy is not None
                and self._incremental_policy.enabled
                and not self._incremental_policy.target_driven
                and self._incremental_policy.cdc):
            try:
                self._incremental_cdc_counts = self._count_incremental_cdc(
                    source_conn, src_fqn, source_where, source_dialect
                )
            except Exception as exc:
                return [self._error(
                    f"Incremental CDC operation summary failed ({type(exc).__name__})",
                    started, base_details, status=ValidationStatus.ERROR,
                    exception_category="QUERY_EXECUTION",
                )]
        missing_key_override: Optional[str] = None

        if (
            not isinstance(supported_modes, (list, tuple, set))
            or not all(isinstance(item, str) for item in supported_modes)
        ):
            return [
                self._error(
                    "comparison_settings.supported_modes must be a list",
                    started,
                    base_details,
                )
            ]
        supported_mode_set = set(supported_modes)
        implemented_modes = {"sample_comparison", full_mode}
        if mode not in supported_mode_set or mode not in implemented_modes:
            return [
                self._error(
                    f"Unsupported comparison mode: {mode}",
                    started,
                    base_details,
                    valid_modes=sorted(implemented_modes & supported_mode_set),
                    requested_mode=mode,
                )
            ]

        sampling_config: Dict[str, Any] = {}
        sample_size: Optional[int] = None
        if mode == "sample_comparison":
            default_sampling = comparison_settings.get("default_sampling_strategy") or {}
            configured_sampling = config.get("sampling_strategy", {}) or {}
            if not isinstance(default_sampling, dict) or not isinstance(configured_sampling, dict):
                return [self._error("sampling_strategy must be a mapping", started, base_details)]
            forbidden = {
                "key_columns", "key_mappings", "primary_key", "primary_keys"
            }.intersection(configured_sampling)
            if forbidden:
                return [
                    self._error(
                        "Sampling key columns must come only from table key_mappings",
                        started,
                        base_details,
                        forbidden_fields=sorted(forbidden),
                    )
                ]
            sampling_config = {**default_sampling, **configured_sampling}
            configured_sample_key_source = sampling_config.get(
                "key_source", {"type": "table_yaml"}
            ) or {}
            if (
                not isinstance(configured_sample_key_source, dict)
                or configured_sample_key_source.get("type", "table_yaml")
                != "table_yaml"
            ):
                return [
                    self._error(
                        "sampling_strategy.key_source.type must be table_yaml",
                        started,
                        base_details,
                    )
                ]
            strategy = sampling_config.get("type")
            base_details["sampling_strategy"] = strategy
            supported_strategies = comparison_settings.get(
                "supported_sampling_strategies"
            )
            if (
                not isinstance(supported_strategies, (list, tuple, set))
                or not all(isinstance(item, str) for item in supported_strategies)
            ):
                return [
                    self._error(
                        "comparison_settings.supported_sampling_strategies must be a list",
                        started,
                        base_details,
                    )
                ]
            supported_strategy_set = set(supported_strategies)
            if (
                strategy not in supported_strategy_set
                or strategy not in self.IMPLEMENTED_STRATEGIES
            ):
                return [
                    self._error(
                        f"Unsupported sampling strategy: {strategy}",
                        started,
                        base_details,
                        valid_sampling_strategies=sorted(
                            self.IMPLEMENTED_STRATEGIES & supported_strategy_set
                        ),
                    )
                ]
            legacy_sample = config.get("sample", {}) or {}
            if not isinstance(legacy_sample, dict):
                return [self._error("sample must be a mapping", started, base_details)]
            sampling_defaults = comparison_settings.get("sampling", {}) or {}
            if not isinstance(sampling_defaults, dict):
                return [
                    self._error(
                        "comparison_settings.sampling must be a mapping",
                        started,
                        base_details,
                    )
                ]
            sample_size = sampling_config.get(
                "sample_size",
                legacy_sample.get(
                    "limit", sampling_defaults.get("default_sample_size")
                ),
            )
            if automatic_sample_size is not None:
                sample_size = automatic_sample_size
            if not self._is_positive_int(sample_size):
                return [
                    self._error(
                        "sampling_strategy.sample_size must be a positive integer",
                        started,
                        base_details,
                    )
                ]
            # The sample size is deliberately NOT the target lookup batch size.
            # The lookup batch comes from ``self._sample_lookup`` (configured
            # independently), so a 100K sample is looked up in bounded pages
            # instead of one statement carrying 100K predicates.
            if strategy == "deterministic" and not key_mappings:
                fallback = self._policies.missing_keys["deterministic_sampling"]
                base_details.update(
                    fallback_used=fallback,
                    fallback_reason="Deterministic sampling requested without key_mappings",
                    exception_category="MISSING_KEY_MAPPING",
                )
                if fallback in {"random_sampling", "hash_comparison"}:
                    strategy = "random"
                    base_details["sampling_strategy"] = strategy
                    base_details["comparison_strategy"] = fallback
                elif fallback == "full_scan":
                    mode = full_mode
                    missing_key_override = "full_scan"
                    base_details["comparison_mode"] = mode
                    base_details["record_comparison_mode"] = mode
                elif fallback == "skip":
                    return [self._policy_result(
                        "Record comparison skipped: deterministic sampling has no key_mappings",
                        started, base_details, "skip_status",
                    )]
                elif fallback == "error":
                    return [self._policy_result(
                        "Deterministic sampling requires table key_mappings",
                        started, base_details, "error_status",
                    )]

        evidence_policy = full_policy["evidence"]
        mismatch_limit = evidence_policy["mismatch_sample_limit"]
        matched_limit = evidence_policy["matched_records_limit"]
        if not self._is_nonnegative_int(mismatch_limit):
            return [self._error("mismatch_sample_limit must be a non-negative integer", started, base_details)]
        if not self._is_nonnegative_int(matched_limit):
            return [self._error("matched_records_limit must be a non-negative integer", started, base_details)]
        export_matched = evidence_policy["export_matched_records"]
        if not isinstance(export_matched, bool):
            return [self._error("export_matched_records must be boolean", started, base_details)]

        source_keys = [item["source_column"] for item in key_mappings]
        target_keys = [item["target_column"] for item in key_mappings]

        large_table_cfg = self._large_table_processing_config(
            config.get("large_table_processing")
        )
        large_table_mode = "FULL_COMPARISON"
        if large_table_cfg.get("enabled"):
            large_limit = int(large_table_cfg.get("large_table_threshold", 2000000) or 2000000)
            medium_limit = int(large_table_cfg.get("medium_table_threshold", 500000) or 500000)
            try:
                if source_count is None or target_count is None:
                    source_count, target_count = self._cached_row_counts(execution_cache)
                estimated_rows = max(source_count, target_count)
                if estimated_rows > large_limit:
                    large_table_mode = "FULL_COMPARISON"
                    config["_large_table_rows"] = estimated_rows
                    config["_large_table_detected"] = True
                elif estimated_rows > medium_limit:
                    large_table_mode = "FULL_COMPARISON"
                    config["_large_table_rows"] = estimated_rows
                    config["_large_table_detected"] = True
            except Exception:
                pass

        try:
            configured_columns = self._configured_column_mappings(table_map)
            excluded_columns = set(self._policies.hashing["column_exclusions"])
            configured_columns = [
                item for item in configured_columns
                if item["source_column"] not in excluded_columns
                and item["target_column"] not in excluded_columns
            ]
            # Build projections with keys and business columns
            source_cols = source_keys + [item["source_column"] for item in configured_columns]
            target_cols = target_keys + [item["target_column"] for item in configured_columns]
            # Incremental classification reads a few configured, non-compared
            # columns from the same rows: the CDC operation column, the
            # watermark and target audit values quoted in evidence, and the
            # soft-delete marker. They are projected, never compared or hashed.
            incremental_columns = self._incremental_projection_columns()
            source_cols = source_cols + [
                column for column in incremental_columns["source"] if column not in source_cols
            ]
            target_cols = target_cols + [
                column for column in incremental_columns["target"] if column not in target_cols
            ]

            # _RowHashValue is evidence-only. Resolve each side independently.
            # If the column is absent on one side, that side remains blank/None
            # and record comparison continues using the configured business columns.
            configured_audit = self._row_hash_column
            source_audit_resolved = None
            target_audit_resolved = None

            if configured_audit:
                source_audit_resolved = self._resolve_row_hash_column_for_extraction(
                    source_conn, src_fqn, ""
                )
                if source_audit_resolved:
                    source_cols = source_cols + [source_audit_resolved]

                target_audit_resolved = self._resolve_row_hash_column_for_extraction(
                    target_conn, tgt_fqn, ""
                )
                if target_audit_resolved:
                    target_cols = target_cols + [target_audit_resolved]

            source_projection = self._projection(source_cols, source_dialect)
            target_projection = self._projection(target_cols, target_dialect)
            # The column list behind ``target_projection``; the bounded sample
            # lookup renders qualified projections from names, not SQL text.
            target_projection_columns: List[str] = list(target_cols)

            if mode == full_mode:
                if not full_policy["enabled"]:
                    raise ValueError(
                        "full_data_comparison requires "
                        "comparison_settings.full_data_comparison.enabled=true"
                    )
                consistency_requirement = full_policy["consistency_requirement"]
                if consistency_requirement != "stable_validation_window":
                    raise ValueError(
                        "comparison_settings.full_data_comparison."
                        "consistency_requirement must be "
                        "stable_validation_window"
                    )
                base_details["consistency_requirement"] = consistency_requirement
                if not full_policy["batching"]["database_pushdown_enabled"]:
                    source_projection = "*"
                    target_projection = "*"
                    target_projection_columns = []
                if key_mappings:
                    self._shared_statistics_used = False
                    fallback_reason = (
                        None
                        if self._incremental_policy is not None
                        and self._incremental_policy.target_driven
                        else self._key_fallback_reason(
                            source_conn, target_conn, src_fqn, tgt_fqn,
                            source_keys, target_keys, source_where, target_where,
                            table_map, execution_cache,
                        )
                    )
                    if fallback_reason:
                        fallback = full_policy["hash_fallback"]
                        if not fallback["enabled"]:
                            raise ValidationPolicyError(
                                f"Key mapping is unusable ({fallback_reason}) and hash fallback is disabled"
                            )
                        key_mappings = []
                        source_keys = []
                        target_keys = []
                        if full_policy["batching"]["database_pushdown_enabled"]:
                            fallback_source_cols = [item["source_column"] for item in configured_columns] + [
                                column for column in incremental_columns["source"]
                                if column not in {item["source_column"] for item in configured_columns}
                            ]
                            fallback_target_cols = [item["target_column"] for item in configured_columns] + [
                                column for column in incremental_columns["target"]
                                if column not in {item["target_column"] for item in configured_columns}
                            ]
                            # Carry _RowHashValue only on the side where the
                            # physical column actually exists. Missing side stays blank.
                            if source_audit_resolved:
                                fallback_source_cols = (
                                    fallback_source_cols + [source_audit_resolved]
                                )
                            if target_audit_resolved:
                                fallback_target_cols = (
                                    fallback_target_cols + [target_audit_resolved]
                                )
                            source_projection = self._projection(
                                fallback_source_cols,
                                source_dialect,
                            )
                            target_projection = self._projection(
                                fallback_target_cols,
                                target_dialect,
                            )
                            target_projection_columns = list(fallback_target_cols)
                        missing_key_override = fallback["strategy"]
                        base_details.update(
                            fallback_used=fallback["strategy"],
                            fallback_reason=fallback_reason,
                            execution_strategy=fallback["strategy"],
                            warning_code=fallback["warning_code"],
                            warning_message=fallback["warning_message"],
                        )
                if key_mappings:
                    # Batch size for comparison operations (50K rows per flush in dual_keyset_merge)
                    # comes from performance config, not validation config.
                    batch_size = self._get_configured_batch_size()
                    if not self._is_positive_int(batch_size):
                        raise ValueError(
                            "batch_size must be a positive integer from performance config"
                        )
                    self._filter_batch_size = batch_size
                    self._batch_config = full_policy["batching"]
                    self._batch_retry_count = 0
                    logger.debug(
                        "Record comparison batching configured: batch_size=%d (from performance config)",
                        batch_size
                    )
                    base_details["strategy_selection_seconds"] = round(
                        time.perf_counter() - started, 6
                    )
                    self._start_incremental_classifier(
                        "keyed", table_map, target_conn, tgt_fqn, target_dialect,
                        source_keys, target_keys, configured_columns, result_obj,
                        batch_size,
                    )
                    result = self._compare_full_by_key_merge(
                        source_conn,
                        target_conn,
                        src_fqn,
                        tgt_fqn,
                        source_where,
                        target_where,
                        source_projection,
                        target_projection,
                        configured_columns,
                        source_keys,
                        target_keys,
                        batch_size,
                        table_map,
                        mismatch_limit,
                        matched_limit,
                        export_matched,
                        result_obj,
                        base_details,
                        checkpoint_store,
                        execution_cache,
                    )
                    result.details["execution_time"] = round(
                        time.perf_counter() - started, 3
                    )
                    result.details["records_compared"] = result.details.get(
                        "records_checked", 0
                    )
                    return [self._finalize_incremental_result(result, table_map)]
                missing_key_action = missing_key_override or self._policies.missing_keys[
                    "full_data_comparison"
                ]
                fallback = full_policy["hash_fallback"]
                if fallback["enabled"]:
                    missing_key_action = fallback["strategy"]
                base_details.update(
                    fallback_used=missing_key_action,
                    fallback_reason=(
                        base_details.get("fallback_reason")
                        or key_mapping_issue
                        or "Full comparison requested without valid key_mappings"
                    ),
                    exception_category="MISSING_KEY_MAPPING",
                    comparison_strategy=missing_key_action,
                )
                if missing_key_action == "skip":
                    return [self._policy_result(
                        "Record comparison skipped: full comparison has no key_mappings",
                        started, base_details, "skip_status",
                    )]
                if missing_key_action == "error":
                    return [self._policy_result(
                        "Full comparison requires key_mappings under configured error policy",
                        started, base_details, "error_status",
                    )]
                if missing_key_action == full_policy["hash_fallback"]["strategy"]:
                    base_details.update(
                        fallback_used=fallback["strategy"],
                        warning_code=fallback["warning_code"],
                        warning_message=fallback["warning_message"],
                        comparison_strategy=fallback["strategy_name"],
                        execution_strategy=fallback["strategy"],
                    )
                if missing_key_action == "random_sampling":
                    strategy = "random"
                    sample_size = comparison_settings["sampling"]["default_sample_size"]
                    (
                        src_df, tgt_df, source_queries, target_queries,
                    ) = self._sample_source_and_target(
                        source_conn, target_conn, src_fqn, tgt_fqn,
                        source_where, target_where,
                        source_projection, target_projection, configured_columns,
                        source_keys, target_keys, strategy, sample_size,
                        target_columns=target_projection_columns,
                        source_schema=table_map.source_schema,
                        source_table=table_map.source_table,
                    )
                    mode = "sample_comparison"
                    base_details["comparison_mode"] = mode
                    base_details["record_comparison_mode"] = mode
                elif missing_key_action not in {
                    "hash_comparison", full_policy["hash_fallback"]["strategy"],
                    "full_scan",
                }:
                    raise ValidationPolicyError(
                        f"Unsupported missing-key action: {missing_key_action}"
                    )
                else:
                    source_queries = [
                        self._select_sql(
                            src_fqn, source_projection, source_where,
                            dialect=source_dialect,
                        )
                    ]
                    target_queries = [
                        self._select_sql(
                            tgt_fqn, target_projection, target_where,
                            dialect=target_dialect,
                        )
                    ]
                if missing_key_action == full_policy["hash_fallback"]["strategy"]:
                    base_details["strategy_selection_seconds"] = round(
                        time.perf_counter() - started, 6
                    )
                    self._start_incremental_classifier(
                        "hash", table_map, target_conn, tgt_fqn, target_dialect,
                        [], [], configured_columns, result_obj,
                        self._get_configured_batch_size(),
                    )
                    result = self._compare_full_by_hash_streaming(
                        source_conn, target_conn, source_queries[0], target_queries[0],
                        table_map, configured_columns, mismatch_limit, matched_limit,
                        export_matched, result_obj, base_details, execution_cache,
                    )
                    result.details["execution_time"] = round(
                        time.perf_counter() - started, 3
                    )
                    result.details["records_compared"] = result.details.get(
                        "records_checked", 0
                    )
                    return [self._finalize_incremental_result(result, table_map)]
                if missing_key_action in {"hash_comparison", "full_scan"}:
                    # Read both sides in pages under an explicit row budget.
                    #
                    # The pages are joined once at the end because the comparison
                    # below needs whole frames. What bounds memory is therefore
                    # the budget, not the paging: paging a table and then keeping
                    # every page would still peak at full-table size. The budget
                    # is enforced while reading, and recorded, because a
                    # comparison over a truncated read must not be reported as a
                    # complete one.
                    #
                    # Page size and budget are deliberately separate. The page
                    # size is how much is in flight per round trip; the budget is
                    # how many rows the comparison may hold in total. Deriving
                    # one from the other (budget = page * 2) tied a memory
                    # ceiling to a round-trip size, so configuring either one
                    # moved both.
                    row_budget = self._full_scan_row_budget()
                    page_rows = min(FULL_SCAN_PAGE_ROWS, row_budget)
                    src_df = None
                    tgt_df = None

                    # Paging with OFFSET/FETCH needs a deterministic ORDER BY,
                    # or rows may be skipped and repeated between pages;
                    # StreamingBatchReader refuses the read without one. The
                    # projection is the sort key: for a keyless comparison it
                    # orders by every compared column, which is deterministic up
                    # to identical rows, and identical rows are interchangeable
                    # here. Reported below, so the report names the SQL that ran.
                    source_queries = [
                        self._select_sql(
                            src_fqn, source_projection, source_where,
                            order_by=source_projection, dialect=source_dialect,
                        )
                    ]
                    target_queries = [
                        self._select_sql(
                            tgt_fqn, target_projection, target_where,
                            order_by=target_projection, dialect=target_dialect,
                        )
                    ]

                    try:
                        src_df, src_rows, src_truncated = (
                            self._read_within_row_budget(
                                source_conn, source_queries[0],
                                page_rows, row_budget, "Source",
                            )
                        )
                        tgt_df, tgt_rows, tgt_truncated = (
                            self._read_within_row_budget(
                                target_conn, target_queries[0],
                                page_rows, row_budget, "Target",
                            )
                        )
                        gc.collect()
                        base_details.update(
                            full_scan_row_budget=row_budget,
                            full_scan_page_rows=page_rows,
                            source_rows_examined=src_rows,
                            target_rows_examined=tgt_rows,
                            full_scan_truncated=bool(
                                src_truncated or tgt_truncated
                            ),
                        )
                    except MemoryError:
                        # Do not retry by loading the whole table: that is
                        # precisely what exhausted memory. Let this reach the
                        # handler below, which retries through the bounded
                        # sample path.
                        logger.error(
                            "OUT_OF_MEMORY | table=%s | phase=%s | "
                            "error_type=MemoryError",
                            getattr(table_map, "id", "unknown"),
                            getattr(table_map, "migration_phase", "unknown"),
                        )
                        src_df = None
                        tgt_df = None
                        gc.collect()
                        raise
            else:
                if key_mappings:
                    self._shared_statistics_used = False
                    fallback_reason = (
                        None
                        if self._incremental_policy is not None
                        and self._incremental_policy.target_driven
                        else self._key_fallback_reason(
                            source_conn, target_conn, src_fqn, tgt_fqn,
                            source_keys, target_keys, source_where, target_where,
                            table_map, execution_cache,
                        )
                    )
                    if fallback_reason:
                        fallback = full_policy["hash_fallback"]
                        if not fallback["enabled"]:
                            raise ValidationPolicyError(
                                "Cached key validation does not permit keyset comparison "
                                "and hash fallback is disabled"
                            )
                        key_mappings = []
                        source_keys = []
                        target_keys = []
                        source_projection = self._projection(
                            [item["source_column"] for item in configured_columns]
                            + ([source_audit_resolved] if source_audit_resolved else []),
                            source_dialect,
                        )
                        target_projection_columns = (
                            [item["target_column"] for item in configured_columns]
                            + ([target_audit_resolved] if target_audit_resolved else [])
                        )
                        target_projection = self._projection(
                            target_projection_columns,
                            target_dialect,
                        )
                        configured_fallback = self._policies.missing_keys[
                            "deterministic_sampling"
                        ]
                        if configured_fallback not in {
                            "random_sampling", "hash_comparison"
                        }:
                            raise ValidationPolicyError(
                                "Sampled hash fallback requires the configured "
                                "deterministic_sampling action to be random_sampling "
                                "or hash_comparison"
                            )
                        strategy = "random"
                        base_details.update(
                            fallback_used=fallback["strategy"],
                            fallback_reason=fallback_reason,
                            comparison_strategy=fallback["strategy_name"],
                            execution_strategy=fallback["strategy"],
                            warning_code=fallback["warning_code"],
                            warning_message=fallback["warning_message"],
                        )
                (
                    src_df,
                    tgt_df,
                    source_queries,
                    target_queries,
                ) = self._sample_source_and_target(
                    source_conn,
                    target_conn,
                    src_fqn,
                    tgt_fqn,
                    source_where,
                    target_where,
                    source_projection,
                    target_projection,
                    configured_columns,
                    source_keys,
                    target_keys,
                    strategy,
                    sample_size,
                    target_columns=target_projection_columns,
                    source_row_count=source_count,
                    target_row_count=target_count,
                    source_schema=table_map.source_schema,
                    source_table=table_map.source_table,
                )

            if not isinstance(src_df, pd.DataFrame) or not isinstance(tgt_df, pd.DataFrame):
                raise TypeError("Connector execute_query must return a pandas DataFrame")

            column_mappings = self._resolve_column_mappings(
                configured_columns,
                src_df,
                tgt_df,
                source_keys,
                target_keys,
            )
            common_details = {
                **base_details,
                "sampling_strategy": strategy,
                "sample_size": sample_size,
                "source_sql": self._statement_details(source_queries),
                "target_sql": self._statement_details(target_queries),
                "source_keys": source_keys,
                "target_keys": target_keys,
                "key_columns": source_keys,
                "compared_columns": column_mappings,
                "source_records_selected": len(src_df),
                "target_records_selected": len(tgt_df),
                "source_datatypes": {
                    item["source_column"]: str(src_df[item["source_column"]].dtype)
                    for item in column_mappings
                },
                "target_datatypes": {
                    item["target_column"]: str(tgt_df[item["target_column"]].dtype)
                    for item in column_mappings
                },
                "source_precision": self._column_precisions(
                    src_df, [item["source_column"] for item in column_mappings]
                ),
                "target_precision": self._column_precisions(
                    tgt_df, [item["target_column"] for item in column_mappings]
                ),
            }
            if mode == "sample_comparison" and strategy == "random":
                common_details["sampling_semantics"] = "source_cohort"

            compare_started = time.perf_counter()
            if key_mappings:
                result = self._compare_by_key(
                    src_df,
                    tgt_df,
                    table_map,
                    source_keys,
                    target_keys,
                    column_mappings,
                    mismatch_limit,
                    matched_limit,
                    export_matched,
                    result_obj,
                    common_details,
                )
            else:
                result = self._compare_by_row_hash(
                    src_df,
                    tgt_df,
                    table_map,
                    column_mappings,
                    mismatch_limit,
                    matched_limit,
                    export_matched,
                    result_obj,
                    common_details,
                )
            self._attach_sample_performance(
                result, time.perf_counter() - compare_started
            )
            result.details["execution_time"] = round(time.perf_counter() - started, 3)
            result.details["records_compared"] = result.details.get(
                "records_checked", 0
            )
            if automatic_sample_size is not None:
                result.details["actual_sample_size"] = len(src_df)
            if (
                mode == "sample_comparison"
                and self._incremental_policy is not None
                and self._incremental_policy.target_driven
            ):
                # These are measured sample/cohort rows, not an estimate of the
                # complete source population. The source total stays None/N/A.
                result.details.update(
                    source_row_count=None,
                    target_row_count=target_count,
                    total_source_rows=None,
                    total_target_rows=target_count,
                    sample_source_rows=len(src_df),
                    sample_target_rows=len(tgt_df),
                )
            if result.details.get("fallback_used") and result.status == ValidationStatus.PASS:
                result.status = self._policies.configured_status("fallback_status")
                result.details["final_status"] = result.status.value
                result.message += "; configured missing-key fallback was applied"
            return [self._finalize_incremental_result(result, table_map)]
        except Exception as exc:
            fallback_cfg = self._large_table_processing_config(
                config.get("large_table_processing")
            ).get("failure_fallback", {})
            if self._failure_fallback_allowed(exc, fallback_cfg):
                logger.warning(
                    "Record comparison failed under resource pressure; retrying sample fallback | "
                    "table=%s | error_type=%s | error=%s | limit=%s",
                    getattr(table_map, "id", None),
                    type(exc).__name__,
                    sanitize_error_text(exc),
                    fallback_cfg.get("sample_record_limit", 1000000),
                )
                logger.debug("Resource pressure failure detail", exc_info=True)
                sample_size = min(int(fallback_cfg.get("sample_record_limit", 1000000) or 1000000), 1000000)
                strategy = "random"
                (
                    src_df, tgt_df, source_queries, target_queries,
                ) = self._sample_source_and_target(
                    source_conn,
                    target_conn,
                    src_fqn,
                    tgt_fqn,
                    source_where,
                    target_where,
                    source_projection,
                    target_projection,
                    configured_columns,
                    source_keys,
                    target_keys,
                    strategy,
                    sample_size,
                    target_columns=target_projection_columns,
                    source_row_count=source_count,
                    source_schema=table_map.source_schema,
                    source_table=table_map.source_table,
                )
                if not isinstance(src_df, pd.DataFrame) or not isinstance(tgt_df, pd.DataFrame):
                    raise
                column_mappings = self._resolve_column_mappings(
                    configured_columns,
                    src_df,
                    tgt_df,
                    source_keys,
                    target_keys,
                )
                common_details = {
                    **base_details,
                    "sampling_strategy": strategy,
                    "sample_size": sample_size,
                    "source_sql": self._statement_details(source_queries),
                    "target_sql": self._statement_details(target_queries),
                    "source_keys": source_keys,
                    "target_keys": target_keys,
                    "key_columns": source_keys,
                    "compared_columns": column_mappings,
                    "source_records_selected": len(src_df),
                    "target_records_selected": len(tgt_df),
                    "comparison_mode": "sample_comparison",
                    "record_comparison_mode": "sample_comparison",
                    "fallback_used": "sample_comparison",
                    "fallback_trigger": type(exc).__name__,
                    "fallback_sample_size": sample_size,
                    "execution_mode": "SAMPLE_FALLBACK",
                }
                compare_started = time.perf_counter()
                if key_mappings:
                    result = self._compare_by_key(
                        src_df,
                        tgt_df,
                        table_map,
                        source_keys,
                        target_keys,
                        column_mappings,
                        mismatch_limit,
                        matched_limit,
                        export_matched,
                        result_obj,
                        common_details,
                    )
                else:
                    result = self._compare_by_row_hash(
                        src_df,
                        tgt_df,
                        table_map,
                        column_mappings,
                        mismatch_limit,
                        matched_limit,
                        export_matched,
                        result_obj,
                        common_details,
                    )
                self._attach_sample_performance(
                    result, time.perf_counter() - compare_started
                )
                result.details["execution_mode"] = "SAMPLE_FALLBACK"
                result.details["fallback_trigger"] = type(exc).__name__
                result.details["fallback_sample_size"] = sample_size
                result.details["final_status"] = result.status.value
                result.message = (
                    f"Full comparison failed due to {type(exc).__name__}; used sample comparison fallback "
                    f"(sample_size={sample_size}). " + result.message
                )
                # The full comparison that would have classified operations did
                # not complete; the sampled fallback reports why, never a guess.
                self._incremental_classifier = None
                self._incremental_not_performed_reason = (
                    "sampled comparison fallback after "
                    f"{type(exc).__name__}: operations are classified only by the "
                    "full incremental comparison"
                )
                return [self._finalize_incremental_result(result, table_map)]
            # Safe failure record: no statement text, no row values and no
            # traceback at ERROR level (the traceback is available at DEBUG).
            logger.error(
                "Record comparison failed | table=%s | validation=record_comparison | "
                "mode=%s | strategy=%s | error_type=%s | error=%s",
                getattr(table_map, "id", None), mode,
                base_details.get("comparison_strategy") or strategy,
                type(exc).__name__, sanitize_error_text(exc),
            )
            logger.debug("Record comparison failure detail", exc_info=True)
            if checkpoint_store and key_mappings:
                existing = checkpoint_store.load(table_map.id) or {}
                checkpoint_store.save(
                    table_map.id,
                    int(existing.get("batch_number", 0)),
                    existing.get("last_processed_key"),
                    int(existing.get("processed_rows", 0)),
                    "failed",
                    metrics=existing.get("metrics", {}),
                )
            return [
                self._error(
                    f"Record comparison failed: {sanitize_error_text(exc)}",
                    started,
                    base_details,
                    status=self._policies.configured_status(
                        "configuration_error_status"
                        if isinstance(exc, ValidationPolicyError)
                        else "framework_exception_status"
                    ),
                    exception_category=exception_category(exc),
                    error_type=(
                        "RESOURCE_ERROR" if isinstance(exc, MemoryError)
                        else error_class_of(exc)
                    ),
                    table_id=getattr(table_map, "id", None),
                    # Stage and error class only: two bounded, sanitized
                    # tokens the table-level error field is composed from, so
                    # a FRAMEWORK_ERROR table no longer reports a null reason.
                    # Neither carries statement text or row values.
                    failed_stage=getattr(exc, "validation_stage", "") or "",
                    error_class=error_class_of(exc),
                )
            ]
        finally:
            # Whatever happened above, this table's large-comparison slot and
            # its large intermediates are released here, not left to garbage
            # collection while the next table starts.
            self._release_large_comparison_slot()
            self._release_sample_resources()

    @staticmethod
    def _failure_fallback_allowed(exc, fallback_cfg) -> bool:
        """Whether the configured resource fallback applies to this failure.

        A real ``MemoryError`` is reported as resource exhaustion. It is never
        converted into a different comparison strategy.
        """
        if isinstance(exc, MemoryError):
            return False
        text = str(exc).lower()
        return bool(
            fallback_cfg.get("enabled")
            and fallback_cfg.get("mode") == "sample_comparison"
            and (
                "memoryerror" in text
                or "resource" in text
                or "exhaust" in text
            )
        )

    def _key_fallback_reason(
        self, source_conn, target_conn, src_fqn, tgt_fqn,
        source_keys, target_keys, source_where, target_where,
        table_map, execution_cache,
    ) -> Optional[str]:
        policy = self._policies.full_comparison["key_based"]
        table_id = getattr(table_map, "id", "UNKNOWN")

        if not policy["enabled"]:
            logger.info(
                "Table %s: key_based strategy disabled by policy",
                table_id,
            )
            return "key_based_disabled"
        try:
            quality_calls = (
                (
                    "source", source_conn, src_fqn, source_keys,
                    source_where, table_map, execution_cache,
                ),
                (
                    "target", target_conn, tgt_fqn, target_keys,
                    target_where, table_map, execution_cache,
                ),
            )
            source, target = self._run_both_sides(
                getattr(self, "_execution_config", None),
                lambda: self._cached_key_quality(*quality_calls[0]),
                lambda: self._cached_key_quality(*quality_calls[1]),
            )
        except Exception as exc:
            # Infrastructure/query failures do NOT prove that the configured
            # business key is unusable. Falling back to a full hash scan here
            # can turn a temporary Fabric/ODBC issue into a very expensive
            # comparison and can hide the real failure. Let the validator fail
            # after the connection layer has exhausted its configured retries.
            logger.error(
                "Table %s: key quality check failed; not falling back to hash | "
                "error_type=%s | error=%s",
                table_id, type(exc).__name__, sanitize_error_text(exc),
            )
            logger.debug("Key quality failure detail", exc_info=True)
            raise RuntimeError(
                f"Key quality query failed for table={table_id}; "
                f"cannot safely choose comparison strategy: "
                f"{type(exc).__name__}: {exc}"
            ) from exc

        # These are keyset algorithm preconditions, not additional validation
        # rules. Reuse key_validation evidence even when optional checks in
        # the comparison policy are disabled.
        if source[0] or target[0]:
            reason = f"null_keys:source={source[0]},target={target[0]}"
            logger.info(
                "Table %s: null keys detected, fallback to hash: %s",
                table_id, reason,
            )
            return reason

        if source[1] or target[1]:
            reason = f"duplicate_keys:source={source[1]},target={target[1]}"
            logger.info(
                "Table %s: duplicate keys detected, fallback to hash: %s",
                table_id, reason,
            )
            return reason

        if any(total is None or distinct is None or total != distinct
               for _nulls, _duplicates, total, distinct in (source, target)):
            reason = "non_unique_keys: cached row and distinct-key counts do not prove uniqueness"
            logger.info("Table %s: %s; using configured hash fallback", table_id, reason)
            return reason

        logger.info(
            "Table %s: key validation passed, using dual_keyset_merge strategy",
            table_id,
        )
        return None

    def _statistics_identity(self, table_map, role: str) -> Tuple[Any, ...]:
        scope_token = self._scope_token(table_map)
        scoped_identity = json.dumps(
            [
                getattr(table_map, "id", None),
                getattr(table_map, "migration_phase", None),
                scope_token,
            ],
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )
        return (
            getattr(table_map, "id", None),
            role,
            getattr(table_map, "migration_phase", None),
            scoped_identity,
        )

    def _cached_key_quality(
        self, role, connection, fqn, keys, where_clause,
        table_map, execution_cache,
    ) -> Tuple[int, int, Optional[int], Optional[int]]:
        identity = self._statistics_identity(table_map, role)
        cache_key = (
            "key_statistics", identity[0], role, identity[2],
            identity[3], tuple(keys),
        )
        if execution_cache is not None:
            prior = execution_cache.peek(cache_key)
            if prior is not None:
                if getattr(prior, "error", None) is not None:
                    raise ValueError(
                        f"Cached key_validation statistics failed for {role}"
                    ) from prior.error
                # Consume the existing result without replacing it with a
                # comparison-owned statistics object or scanning the keys again.
                result = (
                    int(prior.null_rows), int(prior.duplicate_groups),
                    int(prior.total_rows), int(prior.distinct_keys),
                )
                self._shared_statistics_used = True
                return result
        provider = self._shared_statistics(execution_cache)
        if provider is not None and all(
            provider.provides(name)
            for name in (
                ROW_COUNT_KEY, NULL_KEY_ALIAS, DISTINCT_KEY_ALIAS,
                DUPLICATE_GROUP_ALIAS,
            )
        ):
            try:
                result = (
                    provider.integer(role, NULL_KEY_ALIAS),
                    provider.integer(role, DUPLICATE_GROUP_ALIAS),
                    provider.integer(role, ROW_COUNT_KEY),
                    provider.integer(role, DISTINCT_KEY_ALIAS),
                )
                self._shared_statistics_used = True
                self._publish_key_quality(
                    execution_cache, identity, keys, result, provider.statement(role)
                )
                return result
            except Exception:
                pass

        # ``--checks record_comparison`` intentionally excludes the standalone
        # key validator and its shared-statistics prefetch.  Key quality is an
        # execution precondition of the keyed comparison, not a mandatory
        # dependency on that separately reportable check, so compute the same
        # bounded aggregate here when no reusable result exists.  This keeps
        # record comparison independently runnable while preserving the cache-
        # first path (and the exact null/duplicate/uniqueness safeguards).
        statistics = self._quality_statistics(
            role, connection, fqn, keys, where_clause
        )
        result = (
            int(statistics.null_rows),
            int(statistics.duplicate_groups),
            int(statistics.total_rows),
            int(statistics.distinct_keys),
        )
        self._publish_key_quality(
            execution_cache, identity, keys, result, statistics.sql
        )
        logger.info(
            "Key quality statistics computed by record comparison | "
            "table=%s role=%s keys=%d",
            identity[0], role, len(keys),
        )
        return result

    def _quality_statistics(
        self, role, connection, fqn, keys, where_clause,
    ) -> SimpleNamespace:
        sql = self._key_quality_sql(connection, fqn, keys, where_clause)
        nulls, duplicates, total, distinct = self._key_quality(
            connection, fqn, keys, where_clause, sql=sql
        )
        return SimpleNamespace(
            role=role,
            sql=sql,
            total_rows=total,
            null_rows=nulls,
            distinct_keys=distinct,
            duplicate_groups=duplicates,
            error=None,
        )

    @staticmethod
    def _publish_key_quality(
        execution_cache, identity, keys, result, statement,
    ) -> None:
        if execution_cache is None:
            return
        role = identity[1]
        scope_token = identity[3]
        nulls, duplicates, row_count, distinct_keys = result
        execution_cache.put(
            statistic_cache_key(role, scope_token, NULL_KEY_ALIAS), nulls
        )
        execution_cache.put(
            statistic_cache_key(role, scope_token, DUPLICATE_GROUP_ALIAS),
            duplicates,
        )
        if distinct_keys is not None:
            execution_cache.put(
                statistic_cache_key(role, scope_token, DISTINCT_KEY_ALIAS),
                distinct_keys,
            )
        execution_cache.put(
            ("duplicate_group_count", role, tuple(keys)),
            (duplicates, statement),
        )
        if row_count is not None:
            execution_cache.put(
                statistic_cache_key(role, scope_token, ROW_COUNT_KEY), row_count
            )
            execution_cache.put(("row_count", role), row_count)
        statistics = SimpleNamespace(
            role=role,
            sql=statement,
            total_rows=row_count,
            null_rows=nulls,
            distinct_keys=distinct_keys,
            duplicate_groups=duplicates,
            error=None,
        )
        execution_cache.put(
            ("key_statistics", identity[0], role, identity[2], scope_token, tuple(keys)),
            statistics,
        )
        execution_cache.put(
            ("key_statistics", role, tuple(keys)), statistics
        )

    def _key_quality_sql(self, connection, fqn, keys, where_clause) -> str:
        if not keys:
            raise ValueError("Key mapping is empty")
        dialect = self._dialect(connection)
        quoted_keys = [dialect.quote(column) for column in keys]
        null_condition = " OR ".join(
            f"{column} IS NULL" for column in quoted_keys
        )
        key_list = ", ".join(quoted_keys)
        return bind_sql((
            "SELECT "
            "COALESCE(SUM(__mv_rows), 0) AS total_row_count, "
            "COUNT(*) AS distinct_key_count, "
            f"COALESCE(SUM(CASE WHEN {null_condition} THEN __mv_rows ELSE 0 END), 0) AS null_key_records, "
            "COALESCE(SUM(CASE WHEN __mv_rows > 1 THEN 1 ELSE 0 END), 0) AS duplicate_records "
            f"FROM (SELECT {key_list}, COUNT(*) AS __mv_rows FROM {fqn}{where_clause} "
            f"GROUP BY {key_list}) AS __mv_key_quality"
        ), where_clause)

    def _key_quality(
        self, connection, fqn, keys, where_clause, sql=None,
    ):
        sql = sql or self._key_quality_sql(
            connection, fqn, keys, where_clause
        )
        frame = connection.execute_query(sql)
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            raise ValueError("Key quality query returned no result")
        columns = {str(column).casefold(): column for column in frame.columns}
        try:
            nulls = int(frame.iloc[0][columns["null_key_records"]] or 0)
            duplicates = int(frame.iloc[0][columns["duplicate_records"]] or 0)
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise ValueError("Key quality query returned invalid counts") from exc
        if nulls < 0 or duplicates < 0:
            raise ValueError("Key quality counts cannot be negative")
        total_column = columns.get("total_row_count")
        total = (
            int(frame.iloc[0][total_column] or 0)
            if total_column is not None else None
        )
        if total is not None and total < 0:
            raise ValueError("Key quality counts cannot be negative")
        distinct_column = columns.get("distinct_key_count")
        distinct = (
            int(frame.iloc[0][distinct_column] or 0)
            if distinct_column is not None else total
        )
        if distinct is not None and distinct < 0:
            raise ValueError("Key quality counts cannot be negative")
        return nulls, duplicates, total, distinct

    def _compare_target_driven_full_frames(
        self, source_conn, target_conn, table_map, column_mappings,
        source_keys, target_keys, mismatch_limit, matched_limit,
        export_matched, result_obj, base_details, execution_cache=None,
    ) -> CheckResult:
        """Run the target-owned full window with batch-bounded frames."""
        if "_sample_source_and_target" in self.__dict__:
            # Existing focused tests/integrations may inject the materialized
            # cohort itself; preserve that explicit seam.
            return self._compare_target_driven_full_frames_legacy(
                source_conn, target_conn, table_map, column_mappings,
                source_keys, target_keys, mismatch_limit, matched_limit,
                export_matched, result_obj, base_details, execution_cache,
            )
        if source_keys:
            return self._compare_target_driven_key_batches(
                source_conn, target_conn, table_map, column_mappings,
                source_keys, target_keys, mismatch_limit, matched_limit,
                export_matched, result_obj, base_details,
            )
        return self._compare_target_driven_hash_batches(
            source_conn, target_conn, table_map, column_mappings,
            mismatch_limit, matched_limit, export_matched, result_obj,
            base_details,
        )

    def _target_driven_context(self, source_conn, target_conn, table_map,
                               column_mappings, source_keys, target_keys):
        source_dialect, target_dialect = self._dialect(source_conn), self._dialect(target_conn)
        source_columns = list(dict.fromkeys(
            list(source_keys) + [x["source_column"] for x in column_mappings]
            + self._incremental_projection_columns()["source"]
        ))
        target_columns = list(dict.fromkeys(
            list(target_keys) + [x["target_column"] for x in column_mappings]
            + self._incremental_projection_columns()["target"]
        ))
        source_fqn = _fqn(table_map.source_schema, table_map.source_table, source_dialect)
        target_fqn = _fqn(table_map.target_schema, table_map.target_table, target_dialect)
        source_audit = self._resolve_row_hash_column_for_extraction(
            source_conn, source_fqn, ""
        )
        target_audit = self._resolve_row_hash_column_for_extraction(
            target_conn, target_fqn, ""
        )
        if source_audit and source_audit not in source_columns:
            source_columns.append(source_audit)
        if target_audit and target_audit not in target_columns:
            target_columns.append(target_audit)
        return SimpleNamespace(
            sd=source_dialect, td=target_dialect,
            sf=source_fqn, tf=target_fqn, sa=source_audit, ta=target_audit,
            sw=self._build_where_clause(table_map, source_dialect, "source"),
            tw=self._build_where_clause(table_map, target_dialect, "target"),
            sc=source_columns, tc=target_columns,
            sp=self._projection(source_columns, source_dialect),
            tp=self._projection(target_columns, target_dialect),
        )

    def _target_driven_details(self, table_map, mappings, source_keys,
                               base_details, source_count, target_count,
                               batches, strategy):
        names = self._policies.full_comparison["count_names"]
        matched, value_mismatch, missing_source, missing_target = batches
        mismatch = value_mismatch + missing_source + missing_target
        details = {
            **base_details, "sampling_strategy": None, "sample_size": None,
            "source_keys": list(source_keys), "target_keys": [],
            "key_columns": list(source_keys), "compared_columns": mappings,
            "source_records_selected": source_count,
            "target_records_selected": target_count,
            "source_total": source_count, "target_total": target_count,
            "source_row_count": source_count, "target_row_count": target_count,
            "total_source_rows": source_count, "total_target_rows": target_count,
            "source_rows_processed": source_count,
            "target_rows_processed": target_count,
            "records_checked": matched + mismatch,
            "matched_records": matched, "mismatch_records": mismatch,
            "value_mismatch_records": value_mismatch,
            "missing_in_source": missing_source,
            "missing_in_target": missing_target,
            "excess_in_source": missing_target,
            "excess_in_target": missing_source,
            names["matched"]: matched, names["mismatched"]: mismatch,
            names["excess_in_source"]: missing_target,
            names["excess_in_target"]: missing_source,
            "comparison_mode": self._policies.full_comparison["mode_name"],
            "record_comparison_mode": self._policies.full_comparison["mode_name"],
            "execution_mode": "FULL_COMPARISON", "full_comparison_performed": True,
            "comparison_direction": "target_to_source",
            "execution_strategy": strategy,
            "memory_safe_processing": True,
            "batch_size": self._get_configured_batch_size(),
            "validation_scope": {
                "phase": table_map.migration_phase,
                "source_filter": "",
                "target_filter": str(self._build_where_clause(table_map, None, "target")).strip(),
                "scope_source": "target_audit",
            },
        }
        return details, mismatch

    def _compare_target_driven_key_batches(
        self, source_conn, target_conn, table_map, mappings, source_keys,
        target_keys, mismatch_limit, matched_limit, export_matched, result_obj,
        base_details,
    ):
        ctx = self._target_driven_context(
            source_conn, target_conn, table_map, mappings, source_keys, target_keys
        )
        batch_size = self._get_configured_batch_size()
        if not hasattr(self, "_batch_config"):
            self._batch_config = self._policies.full_comparison["batching"]
        lookup = self._sample_lookup_settings()
        totals = [0, 0, 0, 0]
        source_total = target_total = target_batches = source_batches = 0
        last_key = None
        classifier = getattr(self, "_incremental_classifier", None)
        while True:
            sql = self._keyset_select_sql(
                ctx.tf, ctx.tp, ctx.tw, target_keys, batch_size, last_key, ctx.td
            )
            target = self._execute_batch_query(target_conn, sql)
            if target.empty:
                break
            target_batches += 1
            target_total += len(target)
            values = self._extract_values(target, target_keys)
            coerced = self._coerce_lookup_keys_to_source_types(
                source_conn, table_map.source_schema, table_map.source_table,
                source_keys, values,
            )
            timings = self._new_sample_timings(len(target), True, lookup, len(mappings))
            source, statements = self._keyed_sample_cohort(
                source_conn, ctx.sd, ctx.sf, ctx.sc, ctx.sw, source_keys,
                coerced, lookup, timings, lookup_side="source",
            )
            source_batches += len(statements)
            source_total += len(source)
            mismatched_tokens = set()
            block = self._compare_by_key(
                source, target, table_map, source_keys, target_keys, mappings,
                mismatch_limit, matched_limit, export_matched, result_obj, {},
                mismatched_tokens=mismatched_tokens,
            )
            totals[0] += block.details.get("matched_records", 0)
            totals[1] += block.details.get("value_mismatch_records", 0)
            totals[2] += block.details.get("missing_in_source", 0)
            totals[3] += block.details.get("missing_in_target", 0)
            if isinstance(classifier, OperationClassifier):
                st, tt = self._key_tokens(source, source_keys), self._key_tokens(target, target_keys)
                si, ti = {v:i for i,v in enumerate(st)}, {v:i for i,v in enumerate(tt)}
                common = sorted(set(si) & set(ti))
                if common:
                    classifier.observe_aligned(
                        source.iloc[[si[x] for x in common]].reset_index(drop=True),
                        target.iloc[[ti[x] for x in common]].reset_index(drop=True),
                        common, mismatched_tokens,
                    )
                records = target.iloc[[ti[x] for x in set(ti)-set(si)]].to_dict("records")
                for token, record in zip(set(ti)-set(si), records):
                    classifier.observe_target_only(token, record)
            last_key = values[-1]
            del source, target
            if len(values) < batch_size:
                break
        if isinstance(classifier, OperationClassifier):
            classifier.flush()
        details, mismatch = self._target_driven_details(
            table_map, mappings, source_keys, base_details,
            source_total, target_total, totals, "target_driven_key_cohort",
        )
        details.update(source_batches=source_batches, target_batches=target_batches,
                       peak_frame_rows=batch_size, pagination_strategy="keyset")
        return CheckResult(
            check_name="record_comparison",
            status=ValidationStatus.FAIL if mismatch else ValidationStatus.PASS,
            source_value=source_total, target_value=target_total,
            delta=abs(source_total-target_total),
            message=f"Full comparison checked={details['records_checked']}, matched={totals[0]}, mismatched={mismatch}, missing_in_source={totals[2]}, missing_in_target={totals[3]}",
            details=details,
        )

    def _compare_target_driven_hash_batches(
        self, source_conn, target_conn, table_map, mappings, mismatch_limit,
        matched_limit, export_matched, result_obj, base_details,
    ):
        """Spill exact C/D hash occurrences while frames stay batch-bounded."""
        if not callable(getattr(target_conn, "execute_query_batches", None)):
            # Lightweight/direct connectors cannot promise bounded extraction;
            # retain their established behavior. Production memory-safe
            # connectors expose the streaming contract used below.
            return self._compare_target_driven_full_frames_legacy(
                source_conn, target_conn, table_map, mappings, [], [],
                mismatch_limit, matched_limit, export_matched, result_obj,
                base_details,
            )
        ctx = self._target_driven_context(
            source_conn, target_conn, table_map, mappings, [], []
        )
        batch_size = self._get_configured_batch_size()
        batching = dict(self._policies.full_comparison["batching"])
        batching["batch_size"] = batch_size
        target_compare = [x["target_column"] for x in mappings]
        source_compare = [x["source_column"] for x in mappings]
        target_sql = self._select_sql(ctx.tf, ctx.tp, ctx.tw, dialect=ctx.td)
        execution = getattr(self, "_execution_config", None)
        temporary = getattr(execution, "temporary_storage", None)
        spill_cfg = getattr(execution, "hash_spill", None)
        spill_root = Path(
            getattr(temporary, "directory", None) or
            (getattr(self, "_hash_comparison_config", {}).get("spill") or {}).get("directory") or
            "output/temp"
        )
        spill_root.mkdir(parents=True, exist_ok=True)
        store = open_hash_spill_store(
            spill_root, spill_cfg, temporary, force_disk_spill=True
        )
        target_total = target_batches = source_batches = 0
        classifier = getattr(self, "_incremental_classifier", None)
        try:
            for frame in self._stream_query_batches(target_conn, target_sql, batching):
                self._require_columns(frame, target_compare, "target hash")
                hashes = self._hash_frame(frame[target_compare], target_compare)
                payloads = []
                for pos in range(len(frame)):
                    native = {column: frame.iloc[pos][column] for column in ctx.tc}
                    evidence_row = dict(native)
                    if ctx.ta:
                        evidence_row[f"_audit_{ctx.ta}"] = native.get(ctx.ta)
                    payloads.append(json.dumps({
                        "evidence": json.dumps(
                            evidence_row, default=str, ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                        "native": base64.b64encode(
                            pickle.dumps(native, protocol=pickle.HIGHEST_PROTOCOL)
                        ).decode("ascii"),
                    }, separators=(",", ":")))
                store.upsert_rows("target", list(zip(hashes, payloads)))
                if isinstance(classifier, HashOperationClassifier):
                    classifier.observe_target_batch(frame, hashes)
                target_total += len(frame)
                target_batches += 1
                del frame, hashes, payloads
            store.finalize_batch()

            lookup = self._sample_lookup_settings()
            while True:
                representatives = store.unresolved_target_rows(batch_size)
                if not representatives:
                    break
                hashes = [row[0] for row in representatives]
                target_rows = [
                    pickle.loads(base64.b64decode(json.loads(row[2])["native"]))
                    for row in representatives
                ]
                driver = pd.DataFrame(target_rows, columns=ctx.tc)
                timings = self._new_sample_timings(
                    len(driver), False, lookup, len(mappings)
                )
                self._lookup_matching_counts = Counter()
                source, statements = self._keyless_sample_cohort(
                    source_conn, ctx.sd, ctx.sf, ctx.sc, ctx.sp, ctx.sw,
                    [{**x, "source_column": x["target_column"],
                      "target_column": x["source_column"]} for x in mappings],
                    driver, lookup, timings, lookup_side="source",
                )
                counts = Counter(self._lookup_matching_counts)
                source_hashes = self._hash_frame(source[source_compare], source_compare) if len(source) else []
                source_payloads = {}
                for pos, row_hash in enumerate(source_hashes):
                    evidence_row = source.iloc[pos].to_dict()
                    if ctx.sa:
                        evidence_row[f"_audit_{ctx.sa}"] = evidence_row.get(ctx.sa)
                    source_payloads.setdefault(row_hash, json.dumps(
                        evidence_row, default=str, ensure_ascii=False,
                        separators=(",", ":"),
                    ))
                store.upsert_counts("source", counts, source_payloads)
                store.mark_source_resolved(hashes)
                source_batches += len(statements)
                del driver, source, target_rows, source_hashes, source_payloads
            store.finalize_batch()
            matched, excess_source, excess_target = store.totals()
            source_total = matched + excess_source
            if isinstance(classifier, HashOperationClassifier):
                resolved_updates = classifier.finalize(
                    store, matched, excess_source, excess_target
                )
                excess_target = max(0, excess_target - resolved_updates)
                matched += resolved_updates
            policy = self._policies.full_comparison["evidence"]
            if export_matched and matched_limit:
                remaining = matched_limit - len(result_obj.matched_records)
                for row_hash, count, source_payload, target_payload in store.iter_matched():
                    if remaining <= 0:
                        break
                    target_evidence = target_payload
                    if target_payload:
                        target_evidence = json.loads(target_payload).get("evidence")
                    retained = min(int(count), remaining)
                    for _ in range(retained):
                        result_obj.matched_records.append(MatchedRecord(
                            table_id=table_map.id,
                            primary_key=f"row_hash:{row_hash}", business_key="",
                            source_hash=row_hash, target_hash=row_hash,
                            source_row_hash_value=self._row_hash_from_payload(
                                source_payload, f"_audit_{ctx.sa}" if ctx.sa else None
                            ),
                            target_row_hash_value=self._row_hash_from_payload(
                                target_evidence, f"_audit_{ctx.ta}" if ctx.ta else None
                            ),
                        ))
                    remaining -= retained
            self._store_hash_excess(store, result_obj, table_map.id, "source",
                                    excess_source, policy)
            self._store_hash_excess(
                store, result_obj, table_map.id, "target", excess_target,
                policy,
                excluded_counts=getattr(
                    classifier, "server_2_match_counts", None
                ),
            )
            totals = [matched, 0, excess_target, excess_source]
            details, mismatch = self._target_driven_details(
                table_map, mappings, [], base_details, source_total,
                target_total, totals, "target_driven_hash_cohort",
            )
            details.update(
                source_batches=source_batches, target_batches=target_batches,
                peak_frame_rows=batch_size, hash_spill_engine=store.engine_name,
                hash_algorithm=self._policies.hashing["algorithm"],
                hash_duplicate_handling=self._policies.hashing["duplicate_handling"],
            )
            return CheckResult(
                check_name="record_comparison",
                status=ValidationStatus.FAIL if mismatch else ValidationStatus.PASS,
                source_value=source_total, target_value=target_total,
                delta=abs(source_total-target_total),
                message=f"Row-hash comparison checked={matched+mismatch}, matched={matched}, missing_in_source={excess_target}, missing_in_target={excess_source}",
                details=details,
            )
        finally:
            store.close()
            store.cleanup()

    def _compare_target_driven_full_frames_legacy(
        self, source_conn, target_conn, table_map, column_mappings,
        source_keys, target_keys, mismatch_limit, matched_limit,
        export_matched, result_obj, base_details, execution_cache=None,
    ) -> CheckResult:
        """Compare the complete target-owned increment to source counterparts.

        Target-audit scopes have no valid source predicate.  Materialize only
        the already-counted (< threshold) target window, then reuse the bounded
        PK/signature cohort lookup used by sampling to obtain source rows.  The
        audit predicate is never rendered against the source.
        """
        source_dialect = self._dialect(source_conn)
        target_dialect = self._dialect(target_conn)
        src_fqn = _fqn(table_map.source_schema, table_map.source_table, source_dialect)
        tgt_fqn = _fqn(table_map.target_schema, table_map.target_table, target_dialect)
        source_where = self._build_where_clause(table_map, source_dialect, "source")
        target_where = self._build_where_clause(table_map, target_dialect, "target")
        source_columns = list(dict.fromkeys(
            list(source_keys)
            + [item["source_column"] for item in column_mappings]
            + self._incremental_projection_columns()["source"]
        ))
        target_columns = list(dict.fromkeys(
            list(target_keys)
            + [item["target_column"] for item in column_mappings]
            + self._incremental_projection_columns()["target"]
        ))
        source_projection = self._projection(source_columns, source_dialect)
        target_projection = self._projection(target_columns, target_dialect)
        try:
            _source_count, target_count = self._cached_row_counts(execution_cache)
        except ValueError:
            count_sql = self._select_sql(
                tgt_fqn, target_dialect.count_all(), target_where,
                dialect=target_dialect,
            )
            target_count = int(target_conn.execute_query(count_sql).iloc[0, 0])

        src_df, tgt_df, source_queries, target_queries = self._sample_source_and_target(
            source_conn, target_conn, src_fqn, tgt_fqn,
            source_where, target_where, source_projection, target_projection,
            column_mappings, list(source_keys), list(target_keys),
            "deterministic" if source_keys else "random", max(1, target_count),
            target_columns=target_columns, source_row_count=target_count,
            target_row_count=target_count,
            source_schema=table_map.source_schema,
            source_table=table_map.source_table,
        )
        details = {
            **base_details,
            "sampling_strategy": None,
            "sample_size": None,
            "source_keys": list(source_keys),
            "target_keys": list(target_keys),
            "key_columns": list(source_keys),
            "compared_columns": column_mappings,
            "source_records_selected": len(src_df),
            "target_records_selected": len(tgt_df),
            "source_sql": self._statement_details(source_queries),
            "target_sql": self._statement_details(target_queries),
            "comparison_mode": self._policies.full_comparison["mode_name"],
            "record_comparison_mode": self._policies.full_comparison["mode_name"],
            "execution_strategy": (
                "target_driven_key_cohort" if source_keys
                else "target_driven_hash_cohort"
            ),
            "validation_scope": {
                "phase": table_map.migration_phase,
                "source_filter": str(source_where).strip(),
                "target_filter": str(target_where).strip(),
                "scope_source": "target_audit",
            },
        }
        if source_keys:
            # The engine's own verdict for the aligned records is captured here
            # instead of being recomputed per row below: same comparison, same
            # columns, one pass.
            mismatched: set = set()
            result = self._compare_by_key(
                src_df, tgt_df, table_map, list(source_keys), list(target_keys),
                column_mappings, mismatch_limit, matched_limit, export_matched,
                result_obj, details, mismatched_tokens=mismatched,
            )
            classifier = getattr(self, "_incremental_classifier", None)
            if (
                isinstance(classifier, OperationClassifier)
                and not result.details.get("source_duplicate_records")
                and not result.details.get("target_duplicate_records")
            ):
                source_tokens = self._key_tokens(src_df, source_keys)
                target_tokens = self._key_tokens(tgt_df, target_keys)
                source_positions = {
                    token: position for position, token in enumerate(source_tokens)
                }
                target_positions = {
                    token: position for position, token in enumerate(target_tokens)
                }
                common = set(source_positions) & set(target_positions)
                if common:
                    ordered = sorted(common)
                    classifier.observe_aligned(
                        src_df.iloc[
                            [source_positions[token] for token in ordered]
                        ].reset_index(drop=True),
                        tgt_df.iloc[
                            [target_positions[token] for token in ordered]
                        ].reset_index(drop=True),
                        ordered, mismatched,
                    )
                # A target row whose configured key is absent from the source
                # cohort is not skipped and is not a framework error: it is
                # TARGET_ONLY, and the existing historical/CDC classification
                # decides what it means. No source-vs-target column comparison
                # is attempted for it, because there is no source row.
                target_only = list(set(target_positions) - set(source_positions))
                classifier_batch = max(
                    1, int(getattr(classifier, "_batch_size", 50_000) or 50_000)
                )
                for offset in range(0, len(target_only), classifier_batch):
                    tokens = target_only[offset:offset + classifier_batch]
                    positions = [target_positions[token] for token in tokens]
                    # Pandas converts one frame block to records far faster than
                    # hundreds of thousands of individual iloc(...).to_dict()
                    # calls. The classifier still sees exactly the same rows.
                    records = tgt_df.iloc[positions].to_dict("records")
                    for token, record in zip(tokens, records):
                        classifier.observe_target_only(token, record)
                    logger.info(
                        "[TARGET_CLASSIFICATION] table=%s target_only_rows=%d/%d "
                        "source_lookup_rows=%d target_scope_rows=%d",
                        table_map.id, min(offset + len(tokens), len(target_only)),
                        len(target_only), len(src_df), len(tgt_df),
                    )
                classifier.flush()
        else:
            result = self._compare_by_row_hash(
                src_df, tgt_df, table_map, column_mappings, mismatch_limit,
                matched_limit, export_matched, result_obj, details,
            )
            classifier = getattr(self, "_incremental_classifier", None)
            target_hashes = self._hash_frame(
                tgt_df[[item["target_column"] for item in column_mappings]],
                [item["target_column"] for item in column_mappings],
            )
            target_counts = Counter(target_hashes)
            source_counts = Counter(
                getattr(self, "_lookup_matching_counts", None) or self._hash_frame(
                    src_df[[item["source_column"] for item in column_mappings]],
                    [item["source_column"] for item in column_mappings],
                )
            )
            matched_counts = {
                value: min(source_counts[value], target_counts[value])
                for value in source_counts.keys() & target_counts.keys()
            }
            excess_source = sum((source_counts - target_counts).values())
            excess_target = sum((target_counts - source_counts).values())
            if excess_source:
                result.status = ValidationStatus.FAIL
                result.source_value = sum(source_counts.values())
                result.target_value = sum(target_counts.values())
                result.delta = abs(result.source_value - result.target_value)
                result.details["missing_in_target"] = (
                    int(result.details.get("missing_in_target", 0)) + excess_source
                )
                result.details["excess_in_source"] = result.details["missing_in_target"]
                result.details["mismatch_records"] = (
                    int(result.details.get("mismatch_records", 0)) + excess_source
                )
                result.details["records_checked"] = (
                    int(result.details.get("records_checked", 0)) + excess_source
                )
                result.message = (
                    f"Row-hash comparison checked={result.details['records_checked']}, "
                    f"matched={result.details.get('matched_records', 0)}, "
                    f"missing_in_source={result.details.get('missing_in_source', 0)}, "
                    f"missing_in_target={result.details['missing_in_target']}"
                )
            if isinstance(classifier, HashOperationClassifier):
                classifier.observe_target_batch(tgt_df, target_hashes)
                target_excess_counts = target_counts - source_counts

                class _MatchedHashes:
                    def iter_matched(self_nonlocal):
                        for value, count in matched_counts.items():
                            yield value, count, None, None
                    def iter_excess(self_nonlocal, side):
                        counts = (
                            target_excess_counts if side == "target"
                            else source_counts - target_counts
                        )
                        rows = tgt_df if side == "target" else src_df
                        columns = (
                            [item["target_column"] for item in column_mappings]
                            if side == "target" else
                            [item["source_column"] for item in column_mappings]
                        )
                        hashes = target_hashes if side == "target" else self._hash_frame(
                            rows[columns], columns
                        )
                        samples = {}
                        for position, row_hash in enumerate(hashes):
                            samples.setdefault(
                                row_hash,
                                json.dumps({
                                    "native": base64.b64encode(pickle.dumps(
                                        rows.iloc[position].to_dict(),
                                        protocol=pickle.HIGHEST_PROTOCOL,
                                    )).decode("ascii")
                                }),
                            )
                        for value, count in counts.items():
                            yield value, count, samples.get(value)
                matched = sum(matched_counts.values())
                resolved = classifier.finalize(
                    _MatchedHashes(), matched, excess_source, excess_target
                )
                if resolved:
                    resolved = min(resolved, excess_target)
                    result.details["missing_in_source"] = max(
                        0, int(result.details.get("missing_in_source", 0)) - resolved
                    )
                    result.details["excess_in_target"] = result.details["missing_in_source"]
                    result.details["mismatch_records"] = max(
                        0, int(result.details.get("mismatch_records", 0)) - resolved
                    )
                    result.details["matched_records"] = int(
                        result.details.get("matched_records", 0)
                    ) + resolved
                    resolved_hashes = set(classifier.server_2_match_counts)
                    if resolved_hashes:
                        result_obj.missing_in_source[:] = [
                            row for row in result_obj.missing_in_source
                            if not (
                                row.primary_key.startswith("row_hash:")
                                and row.primary_key.split(":", 1)[1] in resolved_hashes
                            )
                        ]
                    if result.details["mismatch_records"] == 0:
                        result.status = ValidationStatus.PASS
        result.details.update(
            source_total=len(src_df), target_total=len(tgt_df),
            source_rows_processed=len(src_df), target_rows_processed=len(tgt_df),
            execution_mode="FULL_COMPARISON",
            full_comparison_performed=True,
            comparison_direction="target_to_source",
            source_batches=len(source_queries),
            target_batches=len(target_queries),
            source_scan_count=len(source_queries),
            target_scan_count=1 if target_queries else 0,
            actual_comparison_strategy=(
                self._policies.full_comparison["key_based"]["strategy_name"]
                if source_keys else
                self._policies.full_comparison["hash_fallback"]["strategy_name"]
            ),
        )
        actual_source_count = (
            len(src_df)
            if source_keys
            else sum(source_counts.values())
        )
        result.details.update(
            source_row_count=actual_source_count,
            target_row_count=len(tgt_df),
            total_source_rows=actual_source_count,
            total_target_rows=len(tgt_df),
        )
        count_names = self._policies.full_comparison["count_names"]
        result.details.update({
            count_names["matched"]: result.details.get("matched_records", 0),
            count_names["mismatched"]: result.details.get("mismatch_records", 0),
            count_names["excess_in_source"]: result.details.get("missing_in_target", 0),
            count_names["excess_in_target"]: result.details.get("missing_in_source", 0),
        })
        return result

    def _compare_full_by_hash_streaming(
        self, source_conn, target_conn, source_sql, target_sql, table_map,
        column_mappings, mismatch_limit, matched_limit, export_matched,
        result_obj, base_details, execution_cache=None,
    ) -> CheckResult:
        """Compare complete source/target datasets using streamed row hashes.

        Only compare_column_mappings participate in the comparison hash.
        _RowHashValue remains evidence-only. Source and target ingestion can run
        concurrently when configured. Spill writes are serialized with a lock so
        a single SQLite/DuckDB spill-store object is not written concurrently.

        If configured parallel ingestion fails, the partial spill store is
        discarded and the exact same comparison is retried once sequentially
        from a clean spill store. This changes only physical execution, never the
        comparison result semantics.
        """
        started = time.perf_counter()

        if bool(getattr(getattr(self, "_incremental_policy", None), "target_driven", False)):
            return self._compare_target_driven_full_frames(
                source_conn, target_conn, table_map, column_mappings,
                [], [], mismatch_limit, matched_limit, export_matched,
                result_obj, base_details, execution_cache,
            )

        if not column_mappings:
            raise ValidationPolicyError(
                "Hash full comparison requires compare_column_mappings"
            )

        policy = self._policies.full_comparison
        batching = dict(policy["batching"])
        fallback = policy["hash_fallback"]

        # IMPORTANT: keys are not included here. Hash comparison uses only the
        # explicitly configured business comparison columns.
        source_columns = [item["source_column"] for item in column_mappings]
        target_columns = [item["target_column"] for item in column_mappings]

        source_audit_fqn = _fqn(
            table_map.source_schema,
            table_map.source_table,
            self._dialect(source_conn),
        )
        target_audit_fqn = _fqn(
            table_map.target_schema,
            table_map.target_table,
            self._dialect(target_conn),
        )

        source_audit_column = self._resolve_row_hash_column_for_extraction(
            source_conn, source_audit_fqn, source_sql
        )
        target_audit_column = self._resolve_row_hash_column_for_extraction(
            target_conn, target_audit_fqn, target_sql
        )

        # Audit row hash is evidence-only and is never part of the business hash.
        source_evidence_columns = source_columns + (
            [source_audit_column] if source_audit_column else []
        )
        target_evidence_columns = target_columns + (
            [target_audit_column] if target_audit_column else []
        )

        # FIX: Handle missing spill_directory safely with fallback to temp directory
        spill_directory = (
            (getattr(self, "_hash_comparison_config", {}).get("spill") or {}).get("directory")
            or batching.get("spill_directory")
            or getattr(getattr(self, "_execution_config", None), "temporary_storage", None)
            and getattr(getattr(self, "_execution_config", None).temporary_storage, "directory", None)
            or "output/temp"
        )
        spill_root = Path(spill_directory)
        try:
            spill_root.mkdir(parents=True, exist_ok=True)

            # FIX B: Verify directory is writable (safety check)
            test_file = spill_root / ".write_test"
            try:
                test_file.write_text("test", encoding="utf-8")
                test_file.unlink()
                logger.debug("Verified spill directory is writable: %s", spill_root)
            except Exception as write_error:
                raise IOError(f"Spill directory {spill_root} is not writable: {write_error}")

        except Exception as e:
            logger.warning(
                "Could not create spill directory %s, using fallback: %s",
                spill_directory, e
            )
            spill_root = Path("output/temp")
            spill_root.mkdir(parents=True, exist_ok=True)

            # FIX B: Verify fallback is writable too
            test_file = spill_root / ".write_test"
            try:
                test_file.write_text("test", encoding="utf-8")
                test_file.unlink()
                logger.debug("Verified fallback spill directory is writable: %s", spill_root)
            except Exception as fallback_error:
                logger.error("Emergency fallback spill directory not writable: %s", fallback_error)
                raise IOError(f"Fallback spill directory {spill_root} is not writable: {fallback_error}")
        execution_config = getattr(self, "_execution_config", None)
        temporary_storage = getattr(execution_config, "temporary_storage", None)
        hash_spill_config = getattr(execution_config, "hash_spill", None)
        table_id = getattr(table_map, "id", None)

        parallel_config = batching.get("parallel_execution", {}) or {}
        hash_config = getattr(self, "_hash_comparison_config", {}) or {}
        execution_parallel = getattr(
            getattr(self, "_execution_config", None),
            "parallel_execution",
            None,
        )
        source_target_parallel = getattr(
            execution_parallel,
            "source_target_parallelism",
            None,
        ) if execution_parallel is not None else None
        configured_workers = hash_config.get(
            "ingest_workers", parallel_config.get("worker_count", 2)
        )
        try:
            configured_workers = int(configured_workers)
        except (TypeError, ValueError):
            configured_workers = 2
        execution_worker_count = 1
        if source_target_parallel is not None:
            try:
                execution_worker_count = int(
                    getattr(source_target_parallel, "max_workers_per_validation", 1) or 1
                )
            except (TypeError, ValueError):
                execution_worker_count = 1
        actual_parallel_workers = min(2, max(1, configured_workers))
        if source_target_parallel is not None and getattr(source_target_parallel, "enabled", False):
            actual_parallel_workers = min(2, max(1, execution_worker_count))
        # An explicit hash-comparison setting owns this decision. In
        # particular, ``parallel_ingest: false`` must not be overridden by the
        # global source/target parallelism switch: two wide frames being
        # fetched and canonicalized together can double peak memory.
        if "parallel_ingest" in hash_config:
            parallel_enabled = bool(hash_config["parallel_ingest"])
        else:
            parallel_enabled = bool(
                parallel_config.get("enabled", False)
                or (
                    source_target_parallel is not None
                    and getattr(source_target_parallel, "enabled", False)
                    and execution_worker_count > 1
                )
            )
        parallel_enabled = parallel_enabled and actual_parallel_workers > 1

        source_total = target_total = source_batches = target_batches = 0
        parallel_attempted = False
        parallel_failed = False
        sequential_fallback_used = False
        parallel_failure = None
        database = None
        large_table_config = getattr(self, "_large_table_processing", None) or self._large_table_processing_config({})
        try:
            cached_source_count, cached_target_count = self._cached_row_counts(
                execution_cache
            )
        except ValueError:
            cached_source_count = cached_target_count = 0
        estimated_rows = max(cached_source_count, cached_target_count)
        target_batch_size = self._hash_batch_size(
            estimated_rows, hash_config, large_table_config
        )
        batching["batch_size"] = target_batch_size
        parallel_enabled = parallel_enabled and all(
            self._parallel_ingest_safe(connection)
            for connection in (source_conn, target_conn)
        )
        commit_interval_batches = int(
            (hash_config.get("spill") or {}).get("commit_interval_batches", 20)
        )
        progress_interval = int(hash_config.get("progress_interval_percent", 25))
        completed = False
        spill_engine_name = None
        timing_totals = {
            "connection_acquisition": 0.0,
            "statistics_query": 0.0,
            "source_extraction": 0.0,
            "target_extraction": 0.0,
            "batching": 0.0,
            "hash_generation": 0.0,
            "comparison": 0.0,
            "spill_read": 0.0,
            "spill_write": 0.0,
            "spill_commit": 0.0,
            "evidence_generation": 0.0,
            "cleanup": 0.0,
        }

        # The same spill-store instance is shared by both ingestion workers.
        # Serialize writes to avoid SQLite/DuckDB connection/thread hazards while
        # still overlapping source/target DB fetch and Python hash preparation.
        import threading
        spill_write_lock = threading.Lock()
        hash_classifier = getattr(self, "_incremental_classifier", None)
        if hash_classifier is not None and not isinstance(hash_classifier, HashOperationClassifier):
            hash_classifier = None
        out_of_window_source_rows = 0
        progress_lock = threading.Lock()
        progress_state = {"processed": 0, "next": progress_interval}
        expected_ingest_rows = cached_source_count + cached_target_count

        def report_progress(side: str, rows: int) -> None:
            if expected_ingest_rows <= 0 or progress_interval <= 0:
                return
            with progress_lock:
                progress_state["processed"] += rows
                percent = min(
                    100,
                    int(progress_state["processed"] * 100 / expected_ingest_rows),
                )
                if percent < progress_state["next"]:
                    return
                logger.info(
                    "[%s] Record comparison: %s / %s (%s%%) | side=%s",
                    table_id,
                    min(progress_state["processed"], expected_ingest_rows),
                    expected_ingest_rows,
                    percent,
                    side,
                )
                while progress_state["next"] <= percent:
                    progress_state["next"] += progress_interval

        def open_store(preferred_engine: Optional[str] = None, force_disk_spill: bool = False):
            return open_hash_spill_store(
                spill_root,
                hash_spill_config,
                temporary_storage,
                preferred_engine=preferred_engine,
                force_disk_spill=force_disk_spill,
                commit_interval_batches=commit_interval_batches,
            )

        def dispose_store(store, *, force_cleanup: bool = False, success: bool = False):
            if store is None:
                return
            cleanup_started = time.perf_counter()
            try:
                store.close()
            finally:
                # Spill databases are per-table scratch data and are never
                # retained, including on exceptions and interruption.
                store.cleanup()
                timing_totals["cleanup"] += time.perf_counter() - cleanup_started

        def ingest(
            side,
            connection,
            sql,
            comparison_columns,
            evidence_columns,
            audit_col,
        ):
            nonlocal database, sequential_fallback_used, spill_engine_name
            # Keep evidence_columns explicit in the signature for symmetry.
            del evidence_columns

            total = 0
            batches = 0
            next_batch_number = 1
            side_timings = {
                "source_extraction": 0.0,
                "target_extraction": 0.0,
                "batching": 0.0,
                "hash_generation": 0.0,
                "spill_read": 0.0,
            }
            stream = iter(self._stream_query_batches(connection, sql, batching))

            while True:
                fetch_started = time.perf_counter()
                try:
                    frame = next(stream)
                except StopIteration:
                    break
                except Exception as exc:
                    logger.error(
                        "Record comparison ingest failed | table=%s | side=%s | "
                        "batch=%s | operation=execute_query_batches | "
                        "error_type=%s | error=%s | sql_bytes=%d",
                        table_id, side, next_batch_number, type(exc).__name__,
                        sanitize_error_text(exc), len(str(sql)),
                    )
                    logger.debug("Ingest failure detail", exc_info=True)
                    raise RuntimeError(
                        f"Hash-streaming ingest failed: table={table_id}, "
                        f"side={side}, batch={next_batch_number}, "
                        f"operation=execute_query_batches, "
                        f"error_type={type(exc).__name__}, error={exc}"
                    ) from exc

                side_timings[f"{side}_extraction"] += (
                    time.perf_counter() - fetch_started
                )

                batch_number = next_batch_number
                next_batch_number += 1

                if not isinstance(frame, pd.DataFrame):
                    exc = TypeError(
                        "Connector execute_query_batches must yield pandas DataFrame objects"
                    )
                    logger.error(
                        "Record comparison ingest failed | table=%s | side=%s | "
                        "batch=%s | operation=validate_batch_type | "
                        "error_type=%s | error=%s",
                        table_id, side, batch_number, type(exc).__name__, exc,
                    )
                    raise RuntimeError(
                        f"Hash-streaming ingest failed: table={table_id}, "
                        f"side={side}, batch={batch_number}, "
                        f"operation=validate_batch_type, "
                        f"error_type={type(exc).__name__}, error={exc}"
                    ) from exc

                try:
                    self._require_columns(
                        frame, comparison_columns, f"{side} hash"
                    )
                except Exception as exc:
                    logger.error(
                        "Record comparison ingest failed | table=%s | side=%s | "
                        "batch=%s | operation=require_columns | "
                        "error_type=%s | error=%s",
                        table_id, side, batch_number, type(exc).__name__,
                        sanitize_error_text(exc),
                    )
                    raise RuntimeError(
                        f"Hash-streaming ingest failed: table={table_id}, "
                        f"side={side}, batch={batch_number}, "
                        f"operation=require_columns, "
                        f"error_type={type(exc).__name__}, error={exc}"
                    ) from exc

                # Hash only configured comparison columns. Keep batching so DB
                # transfer and spill writes stay memory bounded.
                batch_started = time.perf_counter()
                selected = frame[comparison_columns]
                side_timings["batching"] += time.perf_counter() - batch_started
                hash_started = time.perf_counter()
                try:
                    rows = self._hash_frame(selected, comparison_columns)
                except MemoryError:
                    # Preserve the resource exception.  Wrapping it as a
                    # RuntimeError prevents both accurate reporting and the
                    # caller's memory-specific handling from recognizing it.
                    logger.error(
                        "Record comparison ingest exhausted memory | table=%s | "
                        "side=%s | batch=%s | operation=hash_batch | rows=%s",
                        table_id, side, batch_number, len(selected),
                    )
                    raise
                except Exception as exc:
                    logger.error(
                        "Record comparison ingest failed | table=%s | side=%s | "
                        "batch=%s | operation=hash_batch | error_type=%s | error=%s",
                        table_id, side, batch_number, type(exc).__name__,
                        sanitize_error_text(exc),
                    )
                    raise RuntimeError(
                        f"Hash-streaming ingest failed: table={table_id}, "
                        f"side={side}, batch={batch_number}, operation=hash_batch, "
                        f"error_type={type(exc).__name__}, error={exc}"
                    ) from exc
                side_timings["hash_generation"] += (
                    time.perf_counter() - hash_started
                )
                if side == "target" and hash_classifier is not None and rows:
                    # Same batch, same hashes: is the identical business row
                    # already present in the historical target scope?
                    hash_classifier.observe_target_batch(frame, rows)

                try:
                    if rows:
                        # Protect a shared spill-store connection from concurrent
                        # writes. Fetch/hash work remains concurrent.
                        with spill_write_lock:
                            spill_rows = []
                            for position, row_hash in enumerate(rows):
                                native = frame.iloc[position].to_dict()
                                payload = json.dumps({
                                    "native": base64.b64encode(
                                        pickle.dumps(native, protocol=pickle.HIGHEST_PROTOCOL)
                                    ).decode("ascii")
                                }, separators=(",", ":"))
                                spill_rows.append((row_hash, payload))
                            database.upsert_rows(side, spill_rows)
                except Exception as exc:
                    logger.error(
                        "Record comparison ingest failed | table=%s | side=%s | "
                        "batch=%s | operation=spill_upsert | rows=%s | "
                        "error_type=%s | error=%s",
                        table_id, side, batch_number, len(rows),
                        type(exc).__name__, sanitize_error_text(exc),
                    )
                    raise RuntimeError(
                        f"Hash-streaming ingest failed: table={table_id}, "
                        f"side={side}, batch={batch_number}, "
                        f"operation=spill_upsert, rows={len(rows)}, "
                        f"error_type={type(exc).__name__}, error={exc}"
                    ) from exc

                total += len(frame)
                batches += 1
                report_progress(side, len(frame))

            return total, batches, side_timings

        def fetch_evidence(side, connection, sql, comparison_columns, audit_col, hashes):
            """Second-pass lookup for only the bounded hashes selected as evidence."""
            needed = set(hashes)
            payloads = {}
            audit_values = {}
            if not needed:
                return payloads, audit_values
            for frame in self._stream_query_batches(connection, sql, batching):
                self._require_columns(frame, comparison_columns, f"{side} evidence")
                frame_hashes = self._hash_frame(
                    frame[comparison_columns], comparison_columns
                )
                for position, row_hash in enumerate(frame_hashes):
                    if row_hash not in needed:
                        continue
                    row_values = tuple(
                        frame.iloc[position][column]
                        for column in comparison_columns
                    )
                    payload = dict(zip(comparison_columns, row_values))
                    audit_key = f"_audit_{audit_col}" if audit_col else None
                    if audit_col and audit_col in frame.columns:
                        audit_value = frame.iloc[position][audit_col]
                        if audit_value is not None:
                            payload[audit_key] = self._row_hash_value(audit_value)
                    payloads[row_hash] = json.dumps(
                        payload,
                        default=str,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    audit_values[row_hash] = (
                        self._row_hash_from_payload(payloads[row_hash], audit_key)
                        if audit_key else None
                    )
                    needed.remove(row_hash)
                    if not needed:
                        return payloads, audit_values
            return payloads, audit_values

        ingestion = (
            (
                "source", source_conn, source_sql, source_columns,
                source_evidence_columns, source_audit_column,
            ),
            (
                "target", target_conn, target_sql, target_columns,
                target_evidence_columns, target_audit_column,
            ),
        )

        try:
            engine_priority = None
            force_disk_spill = bool(
                large_table_config.get("enabled")
                and large_table_config.get("large_table", {}).get("force_disk_spill")
            )
            database = open_store(force_disk_spill=force_disk_spill)
            spill_engine_name = database.engine_name

            if parallel_enabled:
                parallel_attempted = True
                try:
                    from concurrent.futures import ThreadPoolExecutor, as_completed

                    ingest_results = {}
                    with ThreadPoolExecutor(
                        max_workers=actual_parallel_workers
                    ) as executor:
                        futures = {
                            executor.submit(ingest, *ingestion[0]): "source",
                            executor.submit(ingest, *ingestion[1]): "target",
                        }
                        for future in as_completed(futures):
                            side_name = futures[future]
                            total, batches, side_timings = future.result()
                            ingest_results[side_name] = (total, batches, side_timings)

                    source_total, source_batches, source_side_timings = ingest_results["source"]
                    target_total, target_batches, target_side_timings = ingest_results["target"]
                    for key in ("source_extraction", "target_extraction", "batching", "hash_generation"):
                        timing_totals[key] += source_side_timings.get(key, 0.0) + target_side_timings.get(key, 0.0)
                except Exception as exc:
                    parallel_failed = True
                    parallel_failure = exc
                    logger.warning(
                        "Parallel hash ingest failed; retrying sequentially from "
                        "a clean spill store | table=%s | error_type=%s | error=%s",
                        table_id, type(exc).__name__, sanitize_error_text(exc),
                    )
                    logger.debug("Parallel ingest failure detail", exc_info=True)

                    # Discard all partial parallel results before retrying.
                    dispose_store(database, force_cleanup=True, success=False)
                    database = open_store()
                    spill_engine_name = database.engine_name
                    sequential_fallback_used = True

                    source_total, source_batches, source_side_timings = ingest(*ingestion[0])
                    target_total, target_batches, target_side_timings = ingest(*ingestion[1])
                    for key in ("source_extraction", "target_extraction", "batching", "hash_generation"):
                        timing_totals[key] += source_side_timings.get(key, 0.0) + target_side_timings.get(key, 0.0)
            else:
                source_total, source_batches, source_side_timings = ingest(*ingestion[0])
                target_total, target_batches, target_side_timings = ingest(*ingestion[1])
                for key in ("source_extraction", "target_extraction", "batching", "hash_generation"):
                    timing_totals[key] += source_side_timings.get(key, 0.0) + target_side_timings.get(key, 0.0)

            database.finalize_batch()
            timing_totals["spill_write"] = database.write_seconds
            timing_totals["spill_commit"] = database.commit_seconds
            spill_file_peak_size_mb = database.peak_size_mb

            comparison_started = time.perf_counter()
            matched, excess_source, excess_target = database.totals()
            timing_totals["comparison"] += time.perf_counter() - comparison_started
            if hash_classifier is not None and hash_classifier.target_driven:
                # No source selector: source rows outside the current target
                # load are not part of the window, so they are not excess.
                out_of_window_source_rows, excess_source = excess_source, 0
                hash_classifier.out_of_window_source_rows = out_of_window_source_rows
            if hash_classifier is not None:
                resolved_updates = hash_classifier.finalize(
                    database, matched, excess_source, excess_target
                )
                excess_target = max(0, excess_target - resolved_updates)
                matched += resolved_updates
            mismatched = excess_source + excess_target

            source_audit_key = (
                f"_audit_{source_audit_column}" if source_audit_column else None
            )
            target_audit_key = (
                f"_audit_{target_audit_column}" if target_audit_column else None
            )

            spill_read_started = time.perf_counter()
            matched_requirements = []
            if export_matched and matched_limit:
                remaining = matched_limit
                for row_hash, count, _, _ in database.iter_matched():
                    retained = min(int(count), remaining)
                    matched_requirements.append((row_hash, retained))
                    remaining -= retained
                    if remaining <= 0:
                        break
            excess_requirements = {}
            for side, total in (
                ("source", excess_source), ("target", excess_target)
            ):
                evidence = policy["evidence"]
                export_all = evidence[
                    "export_all_excess_source"
                    if side == "source" else "export_all_excess_target"
                ]
                limit = total if export_all else evidence[
                    "excess_source_limit"
                    if side == "source" else "excess_target_limit"
                ]
                retained_rows = []
                remaining = int(limit)
                if remaining > 0:
                    excluded = (
                        getattr(hash_classifier, "server_2_match_counts", Counter())
                        if side == "target" else Counter()
                    )
                    for row_hash, count, _ in database.iter_excess(side):
                        reportable = max(
                            0, int(count) - int(excluded.get(row_hash, 0))
                        )
                        retained = min(reportable, remaining)
                        if retained <= 0:
                            continue
                        retained_rows.append((row_hash, retained))
                        remaining -= retained
                        if remaining <= 0:
                            break
                excess_requirements[side] = retained_rows
            timing_totals["spill_read"] += time.perf_counter() - spill_read_started

            evidence_started = time.perf_counter()
            source_needed = {
                row_hash for row_hash, _ in matched_requirements
            } | {
                row_hash for row_hash, _ in excess_requirements["source"]
            }
            target_needed = {
                row_hash for row_hash, _ in matched_requirements
            } | {
                row_hash for row_hash, _ in excess_requirements["target"]
            }
            source_payloads, source_audits = fetch_evidence(
                "source", source_conn, source_sql, source_columns,
                source_audit_column, source_needed,
            )
            target_payloads, target_audits = fetch_evidence(
                "target", target_conn, target_sql, target_columns,
                target_audit_column, target_needed,
            )
            for row_hash, count in matched_requirements:
                for _ in range(count):
                    result_obj.matched_records.append(MatchedRecord(
                        table_id=table_map.id,
                        primary_key=f"row_hash:{row_hash}",
                        business_key="",
                        source_hash=row_hash,
                        target_hash=row_hash,
                        source_row_hash_value=source_audits.get(row_hash),
                        target_row_hash_value=target_audits.get(row_hash),
                    ))
            for side, location, destination, payloads, audits in (
                (
                    "source", "EXCESS_IN_SOURCE", result_obj.missing_in_target,
                    source_payloads, source_audits,
                ),
                (
                    "target", "EXCESS_IN_TARGET", result_obj.missing_in_source,
                    target_payloads, target_audits,
                ),
            ):
                for row_hash, count in excess_requirements[side]:
                    for _ in range(count):
                        destination.append(MissingRecord(
                            table_id=table_map.id,
                            primary_key=f"row_hash:{row_hash}",
                            record_data=payloads.get(row_hash),
                            location=location,
                            row_hash_value=audits.get(row_hash),
                        ))
            timing_totals["evidence_generation"] += (
                time.perf_counter() - evidence_started
            )
            completed = True
        except BaseException:
            if database is not None:
                database.rollback()
            raise
        finally:
            if database is not None:
                dispose_store(database, success=completed)
            gc.collect()

        count_names = policy["count_names"]
        status = (
            ValidationStatus.FAIL
            if mismatched else ValidationStatus(fallback["warning_status"])
        )
        # Rendered per role: source and target may filter on different
        # columns, so a role-less rendering would be the wrong predicate.
        scope = {
            "phase": table_map.migration_phase,
            "source_filter": str(self._build_where_clause(table_map, None, "source")).strip(),
            "target_filter": str(self._build_where_clause(table_map, None, "target")).strip(),
            "scope_source": policy["validation_scope_source"],
        }
        scope["filter"] = scope["source_filter"]

        total_execution = time.perf_counter() - started
        logger.info(
            "Record comparison performance | table=%s | "
            "source_records=%s | target_records=%s | matched=%s | mismatched=%s | "
            "source_batches=%s | target_batches=%s | total_time=%.2fs | "
            "strategy=%s | spill_engine=%s | parallel_attempted=%s | "
            "parallel_failed=%s | sequential_fallback=%s | "
            "timings={connection_acquisition=%.3fs, statistics_query=%.3fs, "
            "source_extraction=%.3fs, target_extraction=%.3fs, batching=%.3fs, "
            "hash_generation=%.3fs, spill_write=%.3fs, spill_commit=%.3fs, "
            "comparison=%.3fs, spill_read=%.3fs, evidence=%.3fs, cleanup=%.3fs}",
            table_id, source_total, target_total, matched, mismatched,
            source_batches, target_batches, total_execution,
            fallback["strategy_name"], spill_engine_name,
            parallel_attempted, parallel_failed, sequential_fallback_used,
            timing_totals["connection_acquisition"],
            timing_totals["statistics_query"],
            timing_totals["source_extraction"],
            timing_totals["target_extraction"],
            timing_totals["batching"],
            timing_totals["hash_generation"],
            timing_totals["spill_write"],
            timing_totals["spill_commit"],
            timing_totals["comparison"],
            timing_totals["spill_read"],
            timing_totals["evidence_generation"],
            timing_totals["cleanup"],
        )

        details = {
            **base_details,
            count_names["matched"]: matched,
            count_names["mismatched"]: mismatched,
            count_names["excess_in_source"]: excess_source,
            count_names["excess_in_target"]: excess_target,
            "matched_records": matched,
            "mismatch_records": mismatched,
            "excess_in_source": excess_source,
            "excess_in_target": excess_target,
            "missing_in_target": excess_source,
            "missing_in_source": excess_target,
            "records_checked": matched + mismatched,
            "source_records_selected": source_total,
            "target_records_selected": target_total,
            "source_total": source_total,
            "target_total": target_total,
            "comparison_mode": policy["mode_name"],
            "actual_comparison_strategy": fallback["strategy_name"],
            "comparison_strategy": fallback["strategy_name"],
            "validation_scope": scope,
            "warning_code": fallback["warning_code"],
            "warning_message": fallback["warning_message"],
            "batch_size": batching["batch_size"],
            "selected_batch_size": batching["batch_size"],
            "source_batches": source_batches,
            "target_batches": target_batches,
            "streaming_enabled": batching["streaming_enabled"],
            "database_pushdown_enabled": batching["database_pushdown_enabled"],
            "memory_safe_processing": batching["memory_safe_processing"],
            "parallel_execution": parallel_enabled,
            "parallel_ingest_used": bool(
                parallel_attempted and not parallel_failed
            ),
            "parallel_worker_count": (
                actual_parallel_workers if parallel_enabled else 1
            ),
            "parallel_attempted": parallel_attempted,
            "parallel_failed": parallel_failed,
            "sequential_fallback_used": sequential_fallback_used,
            "parallel_failure": (
                str(parallel_failure) if parallel_failure else None
            ),
            "hash_algorithm": self._policies.hashing["algorithm"],
            "hash_duplicate_handling": self._policies.hashing["duplicate_handling"],
            "hash_spill_engine": spill_engine_name,
            "strategy_selection_seconds": base_details.get(
                "strategy_selection_seconds", 0.0
            ),
            "source_read_seconds": round(
                source_side_timings.get("source_extraction", 0.0), 6
            ),
            "target_read_seconds": round(
                target_side_timings.get("target_extraction", 0.0), 6
            ),
            "source_hash_seconds": round(
                source_side_timings.get("hash_generation", 0.0), 6
            ),
            "target_hash_seconds": round(
                target_side_timings.get("hash_generation", 0.0), 6
            ),
            "spill_write_seconds": round(timing_totals["spill_write"], 6),
            "spill_read_seconds": round(timing_totals["spill_read"], 6),
            "spill_commit_seconds": round(timing_totals["spill_commit"], 6),
            "key_alignment_seconds": 0.0,
            "vectorized_compare_seconds": 0.0,
            "evidence_generation_seconds": round(
                timing_totals["evidence_generation"], 6
            ),
            "cleanup_seconds": round(timing_totals["cleanup"], 6),
            "total_record_comparison_seconds": round(total_execution, 6),
            "source_rows_processed": source_total,
            "target_rows_processed": target_total,
            "spill_file_peak_size_mb": round(spill_file_peak_size_mb, 3),
            "execution_strategy": fallback["strategy"],
            "final_status": status.value,
            "out_of_window_source_rows": out_of_window_source_rows,
        }

        # FIX B: Clean up spill files after comparison completes
        # FIX B: Verify cleanup actually succeeded (safety verification)
        try:
            import shutil
            if spill_root.exists():
                shutil.rmtree(spill_root, ignore_errors=True)

                # Verify cleanup was successful
                if spill_root.exists():
                    logger.warning(
                        "Spill directory cleanup incomplete: %s still exists after rmtree",
                        spill_root
                    )
                else:
                    logger.debug("Cleaned up spill directory: %s", spill_root)
        except Exception as e:
            logger.warning("Could not clean up spill directory %s: %s", spill_root, e)

        return CheckResult(
            check_name="record_comparison",
            status=status,
            source_value=source_total,
            target_value=target_total,
            delta=abs(source_total - target_total),
            message=(
                f"{fallback['warning_message']} matched={matched}, "
                f"mismatched={mismatched}, excess_in_source={excess_source}, "
                f"excess_in_target={excess_target}"
            ),
            details=details,
        )

    def _stream_query_batches(self, connection, sql, batching):
        method = getattr(type(connection), "execute_query_batches", None)
        if batching["streaming_enabled"] and callable(method):
            base_batch_size = int(batching.get("batch_size") or 10000)
            retry_cfg = (self._large_table_processing or self._large_table_processing_config({}))
            retry_policy = retry_cfg.get("memory_error_retry") or {}
            enabled = bool(retry_policy.get("enabled", True))
            retry_sizes = []
            if enabled:
                for value in retry_policy.get("retry_batch_sizes") or []:
                    try:
                        candidate = int(value)
                    except (TypeError, ValueError):
                        continue
                    if candidate > 0 and candidate < base_batch_size:
                        retry_sizes.append(candidate)
            candidate_sizes = [base_batch_size] + [
                size for size in retry_sizes if size != base_batch_size
            ]
            seen = set()
            for index, batch_size in enumerate(candidate_sizes):
                if batch_size <= 0 or batch_size in seen:
                    continue
                seen.add(batch_size)
                try:
                    yield from connection.execute_query_batches(sql, batch_size)
                    return
                except MemoryError as exc:
                    next_batch_size = None
                    if index + 1 < len(candidate_sizes):
                        next_batch_size = candidate_sizes[index + 1]
                    logger.warning(
                        "MemoryError while streaming query batches; retrying with smaller batch size | "
                        "current_batch_size=%s | next_batch_size=%s | endpoint=%s | sql_bytes=%d",
                        batch_size,
                        next_batch_size,
                        getattr(getattr(connection, "cfg", None), "name", None),
                        len(str(sql)),
                    )
                    if next_batch_size is None:
                        raise
                    continue
            return
        if batching["memory_safe_processing"]:
            raise ValidationPolicyError(
                "Configured memory-safe hash comparison requires connector streaming support"
            )
        yield connection.execute_query(sql)

    @classmethod
    def _store_hash_excess(
        cls, database, result_obj, table_id, side, total, evidence,
        row_hash_key=None, excluded_counts=None,
    ) -> None:
        export_all = evidence[
            "export_all_excess_source" if side == "source"
            else "export_all_excess_target"
        ]
        limit = (
            total if export_all else evidence[
                "excess_source_limit" if side == "source"
                else "excess_target_limit"
            ]
        )
        if limit <= 0:
            return
        destination = (
            result_obj.missing_in_target if side == "source"
            else result_obj.missing_in_source
        )
        location = "EXCESS_IN_SOURCE" if side == "source" else "EXCESS_IN_TARGET"
        excluded_counts = excluded_counts or Counter()
        for row_hash, count, payload in database.iter_excess(side):
            count = max(0, int(count) - int(excluded_counts.get(row_hash, 0)))
            if count <= 0:
                continue
            if side == "target" and payload:
                try:
                    wrapped = json.loads(payload)
                    if isinstance(wrapped, dict) and "evidence" in wrapped:
                        payload = wrapped["evidence"]
                except (TypeError, ValueError, json.JSONDecodeError):
                    pass
            for _ in range(min(int(count), limit - len(destination))):
                destination.append(MissingRecord(
                    table_id=table_id,
                    primary_key=f"row_hash:{row_hash}",
                    record_data=payload,
                    location=location,
                    row_hash_value=cls._row_hash_from_payload(
                        payload, row_hash_key
                    ),
                ))
            if len(destination) >= limit:
                break

    def _compare_full_by_key_merge(
        self,
        source_conn,
        target_conn,
        src_fqn: str,
        tgt_fqn: str,
        source_where: str,
        target_where: str,
        source_projection: str,
        target_projection: str,
        configured_columns: List[Dict[str, str]],
        source_keys: List[str],
        target_keys: List[str],
        batch_size: int,
        table_map,
        mismatch_limit: int,
        matched_limit: int,
        export_matched: bool,
        result_obj: TableValidationResult,
        base_details: Dict[str, Any],
        checkpoint_store=None,
        execution_cache=None,
    ) -> CheckResult:
        """Merge two independently paged, key-ordered streams exactly once."""
        comparison_total_started = time.perf_counter()
        if bool(getattr(getattr(self, "_incremental_policy", None), "target_driven", False)):
            return self._compare_target_driven_full_frames(
                source_conn, target_conn, table_map, configured_columns,
                source_keys, target_keys, mismatch_limit, matched_limit,
                export_matched, result_obj, base_details, execution_cache,
            )
        source_dialect = self._dialect(source_conn)
        target_dialect = self._dialect(target_conn)
        checkpoint = checkpoint_store.load(table_map.id) if checkpoint_store else None
        checkpoint_metrics = (checkpoint or {}).get("metrics", {})
        if checkpoint and checkpoint_metrics.get("comparison_strategy") != "dual_keyset_merge":
            checkpoint = None
            checkpoint_metrics = {}

        matched_count = int(checkpoint_metrics.get("matched_records", 0))
        value_mismatch_count = int(
            checkpoint_metrics.get("value_mismatch_records", 0)
        )
        missing_target_count = int(checkpoint_metrics.get("missing_in_target", 0))
        missing_source_count = int(checkpoint_metrics.get("missing_in_source", 0))
        source_count = int((checkpoint or {}).get("processed_rows", 0))
        target_count = int(checkpoint_metrics.get("target_processed_rows", 0))
        last_source_key = (
            tuple(checkpoint["last_processed_key"])
            if checkpoint and checkpoint.get("last_processed_key") is not None
            else None
        )
        last_target_key = (
            tuple(checkpoint_metrics["target_last_processed_key"])
            if checkpoint_metrics.get("target_last_processed_key") is not None
            else None
        )

        adaptive = getattr(
            getattr(getattr(self, "_execution_config", None), "batch_processing", None),
            "adaptive_batching",
            None,
        )
        adaptive_enabled = bool(adaptive is not None and adaptive.enabled)
        initial_batch_size = (
            adaptive.first_batch_size(batch_size) if adaptive_enabled else batch_size
        )
        source_state = {
            "batches": int(checkpoint_metrics.get("source_batches", 0)),
            "requested_rows": initial_batch_size,
            "read_seconds": 0.0,
        }
        target_state = {
            "batches": int(checkpoint_metrics.get("target_batches", 0)),
            "requested_rows": initial_batch_size,
            "read_seconds": 0.0,
        }

        source_stream = self._ordered_keyset_stream(
            source_conn, src_fqn, source_projection, source_where, source_keys,
            source_dialect, last_source_key, source_state, adaptive,
        )
        target_stream = self._ordered_keyset_stream(
            target_conn, tgt_fqn, target_projection, target_where, target_keys,
            target_dialect, last_target_key, target_state, adaptive,
        )
        source_item = next(source_stream, None)
        target_item = next(target_stream, None)
        compared_columns = configured_columns
        operations_since_checkpoint = 0
        try:
            expected_source_rows, expected_target_rows = self._cached_row_counts(
                execution_cache
            )
        except ValueError:
            expected_source_rows = expected_target_rows = 0
        expected_merge_rows = expected_source_rows + expected_target_rows
        progress_interval = int(
            getattr(self, "_hash_comparison_config", {}).get(
                "progress_interval_percent", 25
            )
        )
        next_progress = progress_interval
        # Use dictionaries to accumulate column data instead of row-by-row lists
        # This allows DataFrame construction from dict-of-arrays which is much faster
        aligned_source_dict = {}
        aligned_target_dict = {}
        aligned_tokens = []
        key_alignment_seconds = 0.0
        vectorized_compare_seconds = 0.0
        vectorization_totals = {
            "vectorized_columns": 0,
            "scalar_columns": 0,
            "vectorized_value_comparisons": 0,
            "scalar_value_comparisons": 0,
        }
        batch_flushes = 0
        dict_rows_accumulated = 0
        # Incremental operation classification observes the same aligned,
        # source-only and target-only rows this merge produces; it never reads
        # the two windows a second time.
        classifier = getattr(self, "_incremental_classifier", None)
        if classifier is not None and not isinstance(classifier, OperationClassifier):
            classifier = None
        target_driven = bool(classifier is not None and classifier.target_driven)

        def record_missing_in_target(row, token) -> None:
            nonlocal missing_target_count
            missing_target_count += 1
            self._store_stream_missing(
                result_obj, table_map.id, row, token,
                "MISSING_IN_TARGET", self._configured_excess_limit("source"),
            )

        def flush_aligned() -> None:
            nonlocal matched_count, value_mismatch_count
            nonlocal vectorized_compare_seconds, batch_flushes, dict_rows_accumulated
            if not aligned_tokens:
                return
            batch_flushes += 1
            dict_rows_accumulated += len(aligned_tokens)
            compare_started = time.perf_counter()
            # Construct DataFrames from dict-of-arrays - O(N) instead of O(N log N)
            source_frame = pd.DataFrame(aligned_source_dict)
            target_frame = pd.DataFrame(aligned_target_dict)
            source_frame["_merge_key"] = aligned_tokens
            target_frame["_merge_key"] = aligned_tokens
            logger.debug(
                "[PERF] Batch %d: Flushed %d aligned rows, DataFrame construction started",
                batch_flushes, len(aligned_tokens)
            )
            mismatched_tokens: set = set()
            block_matched, block_mismatched, metrics = self._compare_aligned_values(
                source_frame,
                target_frame,
                table_map,
                set(aligned_tokens),
                compared_columns,
                mismatch_limit,
                matched_limit,
                export_matched,
                result_obj,
                mismatched_tokens=mismatched_tokens if classifier is not None else None,
            )
            matched_count += block_matched
            value_mismatch_count += block_mismatched
            for name in vectorization_totals:
                vectorization_totals[name] += int(metrics.get(name, 0) or 0)
            vectorized_compare_seconds += time.perf_counter() - compare_started
            if classifier is not None:
                classifier.observe_aligned(
                    source_frame, target_frame, list(aligned_tokens), mismatched_tokens
                )
            aligned_source_dict.clear()
            aligned_target_dict.clear()
            aligned_tokens.clear()

        def save_checkpoint() -> None:
            if not checkpoint_store:
                return
            checkpoint_store.save(
                table_map.id,
                source_state["batches"],
                list(last_source_key) if last_source_key is not None else None,
                source_count,
                "in_progress",
                metrics={
                    "comparison_strategy": "dual_keyset_merge",
                    "matched_records": matched_count,
                    "value_mismatch_records": value_mismatch_count,
                    "missing_in_target": missing_target_count,
                    "missing_in_source": missing_source_count,
                    "target_processed_rows": target_count,
                    "target_last_processed_key": (
                        list(last_target_key)
                        if last_target_key is not None else None
                    ),
                    "source_batches": source_state["batches"],
                    "target_batches": target_state["batches"],
                },
            )

        while source_item is not None or target_item is not None:
            advance_source = False
            advance_target = False
            if source_item is None:
                relation = 1
            elif target_item is None:
                relation = -1
            elif source_item[1] == target_item[1]:
                relation = 0
            else:
                relation = (
                    -1 if source_item[2] < target_item[2] else 1
                )

            if relation == 0:
                source_values, source_token, _, source_row = source_item
                target_values, _, _, target_row = target_item
                alignment_started = time.perf_counter()
                # Accumulate row data in dict-of-arrays (more efficient)
                for key, value in source_row.items():
                    if key not in aligned_source_dict:
                        aligned_source_dict[key] = []
                    aligned_source_dict[key].append(value)
                for key, value in target_row.items():
                    if key not in aligned_target_dict:
                        aligned_target_dict[key] = []
                    aligned_target_dict[key].append(value)
                aligned_tokens.append(source_token)
                key_alignment_seconds += time.perf_counter() - alignment_started
                last_source_key = source_values
                last_target_key = target_values
                source_count += 1
                target_count += 1
                advance_source = True
                advance_target = True
                if len(aligned_tokens) >= batch_size:
                    flush_aligned()
            elif relation < 0:
                flush_aligned()
                source_values, source_token, _, source_row = source_item
                if target_driven:
                    # No source selector exists for this table: a source row
                    # outside the current target load is not part of the
                    # window and is neither missing nor classified.
                    classifier.out_of_window_source_rows += 1
                elif classifier is not None:
                    # Missing or UNCHANGED / applied DELETE is decided after the
                    # batched historical lookup; the count and evidence follow.
                    classifier.observe_source_only(
                        source_token, source_row, record_missing_in_target
                    )
                else:
                    record_missing_in_target(source_row, source_token)
                last_source_key = source_values
                source_count += 1
                advance_source = True
            else:
                flush_aligned()
                target_values, target_token, _, target_row = target_item
                missing_source_count += 1
                self._store_stream_missing(
                    result_obj, table_map.id, target_row, target_token,
                    "MISSING_IN_SOURCE", self._configured_excess_limit("target"),
                )
                if classifier is not None:
                    classifier.observe_target_only(target_token, target_row)
                last_target_key = target_values
                target_count += 1
                advance_target = True

            operations_since_checkpoint += 1
            if expected_merge_rows > 0 and progress_interval > 0:
                percent = min(
                    100,
                    int((source_count + target_count) * 100 / expected_merge_rows),
                )
                if percent >= next_progress:
                    logger.info(
                        "[%s] Record comparison: %s / %s (%s%%) | "
                        "strategy=dual_keyset_merge",
                        table_map.id,
                        min(source_count + target_count, expected_merge_rows),
                        expected_merge_rows,
                        percent,
                    )
                    while next_progress <= percent:
                        next_progress += progress_interval
            if operations_since_checkpoint >= batch_size:
                flush_aligned()
                save_checkpoint()
                operations_since_checkpoint = 0
            if advance_source:
                source_item = next(source_stream, None)
            if advance_target:
                target_item = next(target_stream, None)

        flush_aligned()
        if classifier is not None:
            # Deferred source-only decisions land in missing_target_count here.
            classifier.flush()
        mismatch_count = (
            value_mismatch_count + missing_target_count + missing_source_count
        )
        records_checked = matched_count + mismatch_count

        # Log detailed timing breakdown with phase analysis
        source_total_time = source_state.get("read_seconds", 0.0)
        target_total_time = target_state.get("read_seconds", 0.0)
        source_extract_time = source_state.get("extract_seconds", 0.0)
        target_extract_time = target_state.get("extract_seconds", 0.0)
        source_order_time = source_state.get("order_seconds", 0.0)
        target_order_time = target_state.get("order_seconds", 0.0)
        source_record_time = source_state.get("record_seconds", 0.0)
        target_record_time = target_state.get("record_seconds", 0.0)

        comparison_elapsed = time.perf_counter() - comparison_total_started

        # Calculate throughput and efficiency metrics
        source_throughput = (dict_rows_accumulated / source_total_time) if source_total_time > 0 else 0
        target_throughput = (dict_rows_accumulated / target_total_time) if target_total_time > 0 else 0
        total_query_time = source_total_time + target_total_time
        merge_and_compare_time = key_alignment_seconds + vectorized_compare_seconds
        other_time = comparison_elapsed - total_query_time - merge_and_compare_time

        logger.info(
            "[PERF] %s record_comparison dual_keyset_merge detailed timing | "
            "total=%.2f sec | "
            "query_phase=%.2f sec (%.0f%%) | "
            "├─ source_query=%.2f sec (batches=%d) | target_query=%.2f sec | "
            "merge_compare_phase=%.2f sec (%.0f%%) | "
            "├─ alignment=%.2f sec | vectorized_compare=%.2f sec | "
            "other=%.2f sec (%.0f%%) | "
            "throughput: source=%.0f r/s | target=%.0f r/s | "
            "batch_flush_count=%d | aligned_rows=%d",
            table_map.id,
            comparison_elapsed,
            total_query_time, (total_query_time / comparison_elapsed * 100) if comparison_elapsed > 0 else 0,
            source_total_time, source_state.get("batches", 0), target_total_time,
            merge_and_compare_time, (merge_and_compare_time / comparison_elapsed * 100) if comparison_elapsed > 0 else 0,
            key_alignment_seconds, vectorized_compare_seconds,
            other_time, (other_time / comparison_elapsed * 100) if comparison_elapsed > 0 else 0,
            source_throughput, target_throughput,
            batch_flushes, dict_rows_accumulated,
        )

        # Log SQL performance metrics separately for easier identification of bottlenecks
        if source_total_time > target_total_time * 1.2:
            logger.warning(
                "[PERF-WARN] %s source queries significantly slower than target | "
                "source=%.2f sec | target=%.2f sec | "
                "source_batches=%d | ratio=%.2fx",
                table_map.id, source_total_time, target_total_time,
                source_state.get("batches", 0), source_total_time / target_total_time if target_total_time > 0 else 1
            )

        vectorized_total = (
            vectorization_totals["vectorized_value_comparisons"]
            + vectorization_totals["scalar_value_comparisons"]
        )
        vectorization_totals["vectorized_comparison_ratio"] = (
            round(
                vectorization_totals["vectorized_value_comparisons"]
                / vectorized_total,
                6,
            )
            if vectorized_total else None
        )
        count_names = self._policies.full_comparison["count_names"]
        details = {
            **base_details,
            "sampling_strategy": None,
            "sample_size": None,
            "source_keys": source_keys,
            "target_keys": target_keys,
            "key_columns": source_keys,
            "compared_columns": compared_columns,
            "source_records_selected": source_count,
            "target_records_selected": target_count,
            "records_checked": records_checked,
            "matched_records": matched_count,
            "mismatch_records": mismatch_count,
            "value_mismatch_records": value_mismatch_count,
            "missing_in_source": missing_source_count,
            "missing_in_target": missing_target_count,
            "excess_in_source": missing_target_count,
            "excess_in_target": missing_source_count,
            count_names["matched"]: matched_count,
            count_names["mismatched"]: mismatch_count,
            count_names["excess_in_source"]: missing_target_count,
            count_names["excess_in_target"]: missing_source_count,
            "source_total": source_count,
            "target_total": target_count,
            "comparison_mode": self._policies.full_comparison["mode_name"],
            "actual_comparison_strategy": self._policies.full_comparison[
                "key_based"
            ]["strategy_name"],
            "comparison_strategy": self._policies.full_comparison[
                "key_based"
            ]["strategy_name"],
            "validation_scope": {
                "phase": table_map.migration_phase,
                "source_filter": source_where.strip(),
                "target_filter": target_where.strip(),
                "scope_source": self._policies.full_comparison[
                    "validation_scope_source"
                ],
            },
            "execution_strategy": "dual_keyset_merge",
            "strategy_selection_seconds": base_details.get(
                "strategy_selection_seconds", 0.0
            ),
            "source_read_seconds": round(source_state["read_seconds"], 6),
            "target_read_seconds": round(target_state["read_seconds"], 6),
            "source_hash_seconds": 0.0,
            "target_hash_seconds": 0.0,
            "spill_write_seconds": 0.0,
            "spill_read_seconds": 0.0,
            "spill_commit_seconds": 0.0,
            "key_alignment_seconds": round(key_alignment_seconds, 6),
            "vectorized_compare_seconds": round(
                vectorized_compare_seconds, 6
            ),
            "evidence_generation_seconds": round(
                vectorized_compare_seconds, 6
            ),
            "cleanup_seconds": 0.0,
            "total_record_comparison_seconds": round(
                time.perf_counter() - comparison_total_started, 6
            ),
            "source_rows_processed": source_count,
            "target_rows_processed": target_count,
            "spill_file_peak_size_mb": 0.0,
            "selected_batch_size": batch_size,
            "parallel_ingest_used": False,
            **vectorization_totals,
            "pagination_strategy": "keyset",
            "batch_size": batch_size,
            "source_batches": source_state["batches"],
            "target_batches": target_state["batches"],
            "source_scan_count": 1,
            "target_scan_count": 1,
            "shared_statistics_used": bool(
                getattr(self, "_shared_statistics_used", False)
            ),
            "reverse_scan_used": False,
            "retry_count": self._batch_retry_count,
            "batch_parallelism_status": False,
            "batch_worker_count": 1,
            "source_sql": self._keyset_select_sql(
                src_fqn, source_projection, source_where, source_keys,
                initial_batch_size, None, source_dialect,
            ),
            "target_sql": self._keyset_select_sql(
                tgt_fqn, target_projection, target_where, target_keys,
                initial_batch_size, None, target_dialect,
            ),
            "last_processed_key": (
                list(last_source_key) if last_source_key is not None else None
            ),
        }
        if adaptive_enabled:
            details.update(
                adaptive_batching_enabled=True,
                initial_batch_size=initial_batch_size,
                final_source_batch_size=source_state["requested_rows"],
                final_target_batch_size=target_state["requested_rows"],
            )
        self._publish_comparison_counts(
            execution_cache, table_map, source_count, target_count
        )
        if checkpoint_store:
            checkpoint_store.complete(table_map.id, source_state["batches"], source_count)
        return CheckResult(
            check_name="record_comparison",
            status=(
                ValidationStatus.PASS
                if mismatch_count == 0 else ValidationStatus.FAIL
            ),
            source_value=source_count,
            target_value=target_count,
            delta=abs(source_count - target_count),
            message=(
                f"Full comparison checked={records_checked}, matched={matched_count}, "
                f"mismatched={mismatch_count}, missing_in_source={missing_source_count}, "
                f"missing_in_target={missing_target_count}"
            ),
            details=details,
        )

    def _ordered_keyset_stream(
        self, connection, fqn, projection, where_clause, keys, dialect,
        last_key, state, adaptive,
    ):
        last_token = (
            tuple(self._canonical_value(value) for value in last_key)
            if last_key is not None else None
        )
        query_count = 0
        while True:
            requested_rows = state["requested_rows"]
            sql = self._keyset_select_sql(
                fqn, projection, where_clause, keys, requested_rows,
                last_key, dialect,
            )
            query_count += 1
            started = time.perf_counter()
            page = self._execute_batch_query(connection, sql)
            query_elapsed = time.perf_counter() - started
            state["read_seconds"] = state.get("read_seconds", 0.0) + query_elapsed

            # Log SQL query performance with throughput analysis
            rows_returned = len(page) if hasattr(page, '__len__') else 0
            query_throughput = (rows_returned / query_elapsed) if query_elapsed > 0 else 0
            logger.debug(
                "[PERF SQL] batch=%d | rows_requested=%d | rows_returned=%d | "
                "query_seconds=%.3f | throughput=%.0f rows/sec | fqn=%s",
                query_count, requested_rows, rows_returned,
                query_elapsed, query_throughput, fqn
            )
            # Warn if individual batch query is slow (>2 sec for 50K rows)
            if query_elapsed > 2.0 and rows_returned > 10000:
                logger.warning(
                    "[PERF-WARN] Slow SQL batch query | batch=%d | "
                    "rows_returned=%d | query_seconds=%.2f | throughput=%.0f r/s | fqn=%s",
                    query_count, rows_returned, query_elapsed, query_throughput, fqn
                )

            if not isinstance(page, pd.DataFrame):
                raise TypeError("Connector execute_query must return a pandas DataFrame")
            if page.empty:
                return
            self._require_columns(page, keys, "keyset stream")
            state["batches"] += 1

            # Extract and convert values
            extract_started = time.perf_counter()
            key_values = self._extract_values(page, keys)
            tokens = self._key_tokens(page, keys)
            extract_elapsed = time.perf_counter() - extract_started
            state["extract_seconds"] = state.get("extract_seconds", 0.0) + extract_elapsed

            # Build and validate order tokens
            order_started = time.perf_counter()
            order_tokens = [
                tuple(self._sortable_key_part(value) for value in values)
                for values in key_values
            ]
            if any(
                order_tokens[index] <= order_tokens[index - 1]
                for index in range(1, len(order_tokens))
            ):
                raise RuntimeError("Keyset stream is not strictly ordered by unique keys")
            order_elapsed = time.perf_counter() - order_started
            state["order_seconds"] = state.get("order_seconds", 0.0) + order_elapsed

            next_token = tokens[-1]
            if next_token == last_token:
                raise RuntimeError("Keyset pagination did not advance")

            # Build row records efficiently using dict-of-arrays approach
            record_started = time.perf_counter()
            labels = list(page.columns)

            # Convert DataFrame rows to dicts without itertuples overhead
            # Use to_dict('records') which is faster than itertuples
            try:
                row_dicts = page.to_dict('records')
            except Exception:
                # Fallback to manual approach if to_dict fails
                row_dicts = [
                    dict(zip(labels, values))
                    for values in page.itertuples(index=False, name=None)
                ]

            record_elapsed = time.perf_counter() - record_started
            state["record_seconds"] = state.get("record_seconds", 0.0) + record_elapsed

            # Yield results
            for position in range(len(key_values)):
                yield (
                    key_values[position], tokens[position],
                    order_tokens[position], row_dicts[position],
                )

            last_key = key_values[-1]
            last_token = next_token
            if len(page) < requested_rows:
                return
            if adaptive is not None and adaptive.enabled:
                state["requested_rows"] = adaptive.next_batch_size(
                    requested_rows, query_elapsed
                )

    def _sortable_key_part(self, value: Any) -> Tuple[int, Any]:
        canonical = self._canonical_value(value)
        kind, _, payload = canonical.partition(":")
        if kind == "number":
            return 2, Decimal(payload)
        ranks = {
            "null": 0, "bool": 1, "datetime": 3,
            "bytes": 4, "string": 5,
        }
        return ranks.get(kind, 6), payload

    def _row_hash_column_in_labels(self, labels: Iterable[Any]) -> Optional[str]:
        configured = self._row_hash_column
        if not configured:
            return None
        folded = configured.casefold()
        return next(
            (str(label) for label in labels if str(label).casefold() == folded),
            None,
        )

    def _store_stream_missing(
        self, result_obj, table_id, row, token, location, limit,
    ) -> None:
        destination = (
            result_obj.missing_in_target
            if location == "MISSING_IN_TARGET"
            else result_obj.missing_in_source
        )
        if limit is not None and len(destination) >= limit:
            return
        labels = row.index if hasattr(row, "index") else row.keys()
        record_data = (
            row.to_json(date_format="iso")
            if hasattr(row, "to_json")
            else json.dumps(
                row, default=str, ensure_ascii=False, separators=(",", ":")
            )
        )
        destination.append(MissingRecord(
            table_id=table_id,
            primary_key=self._display_key(token),
            record_data=record_data,
            location=location,
            row_hash_value=self._row_hash_from_row(
                row, self._row_hash_column_in_labels(labels)
            ),
        ))

    def _publish_comparison_counts(
        self, execution_cache, table_map, source_count, target_count,
    ) -> None:
        if execution_cache is None:
            return
        for role, count in (("source", source_count), ("target", target_count)):
            identity = self._statistics_identity(table_map, role)
            execution_cache.put(
                statistic_cache_key(role, identity[3], ROW_COUNT_KEY), count
            )
            execution_cache.put(("row_count", role), count)

    def _compare_full_by_key_batches(
        self,
        source_conn,
        target_conn,
        src_fqn: str,
        tgt_fqn: str,
        source_where: str,
        target_where: str,
        source_projection: str,
        target_projection: str,
        configured_columns: List[Dict[str, str]],
        source_keys: List[str],
        target_keys: List[str],
        batch_size: int,
        table_map,
        mismatch_limit: int,
        matched_limit: int,
        export_matched: bool,
        result_obj: TableValidationResult,
        base_details: Dict[str, Any],
        checkpoint_store=None,
    ) -> CheckResult:
        source_dialect = self._dialect(source_conn)
        target_dialect = self._dialect(target_conn)
        checkpoint = checkpoint_store.load(table_map.id) if checkpoint_store else None
        checkpoint_metrics = (checkpoint or {}).get("metrics", {})
        matched_count = int(checkpoint_metrics.get("matched_records", 0))
        value_mismatch_count = int(checkpoint_metrics.get("value_mismatch_records", 0))
        missing_target_count = int(checkpoint_metrics.get("missing_in_target", 0))
        source_count = int((checkpoint or {}).get("processed_rows", 0))
        target_count = 0
        source_batches = int((checkpoint or {}).get("batch_number", 0))
        target_batches = 0
        compared_columns: Optional[List[Dict[str, str]]] = None

        last_source_key: Optional[Tuple[Any, ...]] = (
            tuple(checkpoint["last_processed_key"])
            if checkpoint and checkpoint.get("last_processed_key") is not None
            else None
        )
        last_source_token: Optional[Tuple[str, ...]] = None
        # Adaptive batching tunes only the physical fetch size between the
        # configured bounds. The keyset cursor still walks the same records
        # exactly once, so coverage and results are identical.
        adaptive = getattr(
            getattr(getattr(self, "_execution_config", None), "batch_processing", None),
            "adaptive_batching",
            None,
        )
        adaptive_enabled = bool(adaptive is not None and adaptive.enabled)
        initial_batch_size = (
            adaptive.first_batch_size(batch_size) if adaptive_enabled else batch_size
        )
        requested_rows = initial_batch_size
        while True:
            source_sql = self._keyset_select_sql(
                src_fqn,
                source_projection,
                source_where,
                source_keys,
                requested_rows,
                last_source_key,
                source_dialect,
            )
            fetch_started = time.perf_counter()
            source_page = self._execute_batch_query(source_conn, source_sql)
            fetch_seconds = time.perf_counter() - fetch_started
            if not isinstance(source_page, pd.DataFrame):
                raise TypeError("Connector execute_query must return a pandas DataFrame")
            if source_page.empty:
                break
            source_batches += 1
            source_count += len(source_page)
            key_values = self._extract_values(source_page, source_keys)
            target_queries = self._build_filter_queries(
                tgt_fqn,
                target_projection,
                target_where,
                target_keys,
                key_values,
                target_dialect,
            )
            target_page = self._execute_batch_queries(target_conn, target_queries)
            if compared_columns is None:
                compared_columns = self._resolve_column_mappings(
                    configured_columns,
                    source_page,
                    target_page,
                    source_keys,
                    target_keys,
                )
            batch_result = self._compare_by_key(
                source_page,
                target_page,
                table_map,
                source_keys,
                target_keys,
                compared_columns,
                mismatch_limit,
                matched_limit,
                export_matched,
                result_obj,
                dict(base_details),
                excess_source_limit=self._configured_excess_limit("source"),
                excess_target_limit=self._configured_excess_limit("target"),
                full_evidence_policy=True,
            )
            if (
                batch_result.details.get("source_duplicate_records")
                or batch_result.details.get("target_duplicate_records")
            ):
                batch_result.details.update(
                    execution_strategy="key_batched",
                    batch_size=batch_size,
                    source_batches=source_batches,
                    target_batches=target_batches,
                )
                return batch_result
            matched_count += batch_result.details["matched_records"]
            value_mismatch_count += batch_result.details.get(
                "value_mismatch_records", 0
            )
            missing_target_count += batch_result.details.get("missing_in_target", 0)
            next_source_key = key_values[-1]
            next_source_token = tuple(
                self._canonical_value(value) for value in next_source_key
            )
            if next_source_token == last_source_token:
                raise RuntimeError("Source keyset pagination did not advance")
            last_source_key = next_source_key
            last_source_token = next_source_token
            if checkpoint_store:
                checkpoint_store.save(
                    table_map.id,
                    source_batches,
                    list(next_source_key),
                    source_count,
                    "in_progress",
                    metrics={
                        "matched_records": matched_count,
                        "value_mismatch_records": value_mismatch_count,
                        "missing_in_target": missing_target_count,
                    },
                )
            if len(source_page) < requested_rows:
                break
            if adaptive_enabled:
                requested_rows = adaptive.next_batch_size(
                    requested_rows, fetch_seconds
                )
        final_source_batch_size = requested_rows

        missing_source_count = 0
        last_target_key: Optional[Tuple[Any, ...]] = None
        last_target_token: Optional[Tuple[str, ...]] = None
        requested_rows = initial_batch_size
        while True:
            target_sql = self._keyset_select_sql(
                tgt_fqn,
                target_projection,
                target_where,
                target_keys,
                requested_rows,
                last_target_key,
                target_dialect,
            )
            fetch_started = time.perf_counter()
            target_page = self._execute_batch_query(target_conn, target_sql)
            fetch_seconds = time.perf_counter() - fetch_started
            if not isinstance(target_page, pd.DataFrame):
                raise TypeError("Connector execute_query must return a pandas DataFrame")
            if target_page.empty:
                break
            target_batches += 1
            target_count += len(target_page)
            self._require_columns(target_page, target_keys, "target key")
            target_page = target_page.copy()
            target_page["_merge_key"] = self._key_tokens(target_page, target_keys)
            target_by_key = target_page.set_index("_merge_key", drop=False)
            target_key_set = set(target_by_key.index)

            source_queries = self._build_filter_queries(
                src_fqn,
                self._projection(source_keys, source_dialect),
                source_where,
                source_keys,
                self._extract_values(target_page, target_keys),
                source_dialect,
            )
            # Only the key set is needed, so stream the cohort queries instead
            # of concatenating every result frame first.
            source_key_set = self._collect_key_tokens(
                source_conn,
                source_queries,
                source_keys,
                "source key",
                with_retry=True,
            )
            missing_keys = target_key_set - source_key_set
            missing_source_count += len(missing_keys)
            self._store_missing_records(
                result_obj,
                table_map.id,
                target_by_key,
                missing_keys,
                "MISSING_IN_SOURCE",
                self._configured_excess_limit("target"),
            )
            next_target_key = self._extract_values(target_page, target_keys)[-1]
            next_target_token = tuple(
                self._canonical_value(value) for value in next_target_key
            )
            if next_target_token == last_target_token:
                raise RuntimeError("Target keyset pagination did not advance")
            last_target_key = next_target_key
            last_target_token = next_target_token
            if len(target_page) < requested_rows:
                break
            if adaptive_enabled:
                requested_rows = adaptive.next_batch_size(
                    requested_rows, fetch_seconds
                )

        mismatch_count = (
            value_mismatch_count + missing_target_count + missing_source_count
        )
        records_checked = matched_count + mismatch_count
        count_names = self._policies.full_comparison["count_names"]
        details = {
            **base_details,
            "sampling_strategy": None,
            "sample_size": None,
            "source_keys": source_keys,
            "target_keys": target_keys,
            "key_columns": source_keys,
            "compared_columns": compared_columns or configured_columns,
            "source_records_selected": source_count,
            "target_records_selected": target_count,
            "records_checked": records_checked,
            "matched_records": matched_count,
            "mismatch_records": mismatch_count,
            "value_mismatch_records": value_mismatch_count,
            "missing_in_source": missing_source_count,
            "missing_in_target": missing_target_count,
            "excess_in_source": missing_target_count,
            "excess_in_target": missing_source_count,
            count_names["matched"]: matched_count,
            count_names["mismatched"]: mismatch_count,
            count_names["excess_in_source"]: missing_target_count,
            count_names["excess_in_target"]: missing_source_count,
            "source_total": source_count,
            "target_total": target_count,
            "comparison_mode": self._policies.full_comparison["mode_name"],
            "actual_comparison_strategy": self._policies.full_comparison[
                "key_based"
            ]["strategy_name"],
            "comparison_strategy": self._policies.full_comparison[
                "key_based"
            ]["strategy_name"],
            "validation_scope": {
                "phase": table_map.migration_phase,
                "source_filter": source_where.strip(),
                "target_filter": target_where.strip(),
                "scope_source": self._policies.full_comparison[
                    "validation_scope_source"
                ],
            },
            "execution_strategy": "key_batched",
            "pagination_strategy": "keyset",
            "batch_size": batch_size,
            "source_batches": source_batches,
            "target_batches": target_batches,
            "retry_count": self._batch_retry_count,
            "batch_parallelism_status": self._batch_config[
                "parallel_execution"
            ]["enabled"],
            "batch_worker_count": self._batch_config[
                "parallel_execution"
            ]["worker_count"],
            "source_sql": self._keyset_select_sql(
                src_fqn,
                source_projection,
                source_where,
                source_keys,
                initial_batch_size,
                None,
                source_dialect,
            ),
            "target_sql": self._keyset_select_sql(
                tgt_fqn,
                target_projection,
                target_where,
                target_keys,
                initial_batch_size,
                None,
                target_dialect,
            ),
        }
        if adaptive_enabled:
            details.update(
                adaptive_batching_enabled=True,
                initial_batch_size=initial_batch_size,
                final_source_batch_size=final_source_batch_size,
                final_target_batch_size=requested_rows,
            )
        if checkpoint_store:
            checkpoint_store.complete(table_map.id, source_batches, source_count)
        return CheckResult(
            check_name="record_comparison",
            status=(ValidationStatus.PASS if mismatch_count == 0 else ValidationStatus.FAIL),
            source_value=source_count,
            target_value=target_count,
            delta=abs(source_count - target_count),
            message=(
                f"Full comparison checked={records_checked}, matched={matched_count}, "
                f"mismatched={mismatch_count}, missing_in_source={missing_source_count}, "
                f"missing_in_target={missing_target_count}"
            ),
            details=details,
        )

    def _sample_source_and_target(
        self,
        source_conn,
        target_conn,
        src_fqn: str,
        tgt_fqn: str,
        source_where: str,
        target_where: str,
        source_projection: str,
        target_projection: str,
        column_mappings: List[Dict[str, str]],
        source_keys: List[str],
        target_keys: List[str],
        strategy: str,
        sample_size: int,
        target_columns: Optional[Sequence[str]] = None,
        source_row_count: Optional[int] = None,
        target_row_count: Optional[int] = None,
        source_schema: Optional[str] = None,
        source_table: Optional[str] = None,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], List[str]]:
        """Read one bounded source sample and the matching target cohort.

        The sample is one ``TOP n`` read. The target cohort is never one
        statement built from the whole sample: the sampled rows are looked up
        in pages of the configured lookup batch size, every generated statement
        is checked against the configured SQL-size ceilings before it runs, and
        the rows kept per page are bounded (see :meth:`_keyed_sample_cohort`
        and :meth:`_keyless_sample_cohort`). The result is the same source
        cohort the comparison always used; only how it is fetched changed.
        ``target_columns`` is the column list behind ``target_projection`` (an
        empty list means ``*``); when omitted it is derived from the keys and
        the column mappings.
        """
        lookup = self._sample_lookup_settings()
        timings = self._new_sample_timings(
            sample_size, bool(source_keys), lookup, len(column_mappings)
        )
        self._acquire_large_comparison_slot(lookup, timings)
        source_dialect = self._dialect(source_conn)
        target_dialect = self._dialect(target_conn)
        target_driven = bool(
            getattr(getattr(self, "_incremental_policy", None), "target_driven", False)
        )
        if target_driven:
            full_random_population = bool(
                strategy == "random"
                and target_row_count is not None
                and sample_size >= target_row_count
            )
            target_order = None if full_random_population else (
                target_dialect.random_order()
                if strategy == "random"
                else ", ".join(target_dialect.quote(key) for key in target_keys)
            )
            target_sql = self._select_sql(
                tgt_fqn, target_projection, target_where,
                top=None if full_random_population else sample_size,
                order_by=target_order, dialect=target_dialect,
            )
            read_started = time.perf_counter()
            tgt_df = target_conn.execute_query(target_sql)
            timings["target_query_seconds"] = time.perf_counter() - read_started
            if not isinstance(tgt_df, pd.DataFrame):
                raise TypeError("Connector execute_query must return a pandas DataFrame")
            timings["target_rows"] = len(tgt_df)
            # The driving side here is the target scope; the keys are looked up
            # in the source. Named explicitly so the diagnostics cannot be read
            # as "the target returned no rows".
            timings["target_scope_rows"] = len(tgt_df)
            timings["lookup_side"] = "source"
            self._note_peak_rows(timings, tgt_df)

            source_columns = list(dict.fromkeys(
                list(source_keys)
                + [item["source_column"] for item in column_mappings]
                + self._incremental_projection_columns()["source"]
            ))
            source_audit = self._resolve_row_hash_column_for_extraction(
                source_conn, src_fqn, ""
            )
            if source_audit and source_audit not in source_columns:
                source_columns.append(source_audit)
            if target_keys:
                target_values = self._extract_values(tgt_df, target_keys)
                target_values = self._coerce_lookup_keys_to_source_types(
                    source_conn, source_schema, source_table,
                    source_keys, target_values,
                )
                if target_values:
                    src_df, source_queries = self._keyed_sample_cohort(
                        source_conn, source_dialect, src_fqn, source_columns,
                        source_where, source_keys, target_values, lookup, timings,
                        lookup_side="source",
                    )
                else:
                    src_df = pd.DataFrame(columns=source_columns)
                    source_queries = []
            else:
                if strategy != "random":
                    raise ValueError("Deterministic sampling requires table key_mappings")
                reversed_mappings = [
                    {
                        **item,
                        "source_column": item["target_column"],
                        "target_column": item["source_column"],
                    }
                    for item in column_mappings
                ]
                if len(tgt_df):
                    src_df, source_queries = self._keyless_sample_cohort(
                        source_conn, source_dialect, src_fqn, source_columns,
                        source_projection, source_where, reversed_mappings,
                        tgt_df, lookup, timings, lookup_side="source",
                    )
                    # The keyless helper's cache is oriented driver->lookup;
                    # the final comparison remains source->target.
                    self._sample_hash_cache = None
                else:
                    src_df = pd.DataFrame(columns=source_columns)
                    source_queries = []
            timings["source_rows"] = len(src_df)
            # Rows the source lookup actually returned for the target keys.
            # Zero is a valid result (no source row exists for any key of the
            # window) and is reported as such, not as a failed lookup.
            timings["source_lookup_rows"] = len(src_df)
            timings["source_statements"] = len(source_queries)
            self._note_peak_rows(timings, src_df)
            self._log_sample_lookup_summary(timings)
            return src_df, tgt_df, source_queries, [target_sql]
        sample_predicate = ""
        if strategy == "random" and not source_keys:
            # Keyless: select the sample deterministically instead of ordering
            # the whole table by a random value.
            sample_predicate, order_by = self._keyless_sample_selection(
                source_dialect,
                [item["source_column"] for item in column_mappings],
                sample_size,
                source_row_count,
                timings,
            )
        else:
            order_by = (
                source_dialect.random_order()
                if strategy == "random"
                else ", ".join(source_dialect.quote(key) for key in source_keys)
            )
        source_sql = self._select_sql(
            src_fqn,
            source_projection,
            self._combine_where(source_where, sample_predicate),
            top=sample_size,
            order_by=order_by,
            dialect=source_dialect,
        )
        read_started = time.perf_counter()
        src_df = source_conn.execute_query(source_sql)
        timings["source_query_seconds"] = time.perf_counter() - read_started
        if not isinstance(src_df, pd.DataFrame):
            raise TypeError("Connector execute_query must return a pandas DataFrame")
        timings["source_rows"] = len(src_df)
        # Source-driven sampling: the source sample drives, the target is the
        # side being looked up (the mirror image of the target-driven path).
        timings["source_scope_rows"] = len(src_df)
        timings["lookup_side"] = "target"
        self._note_peak_rows(timings, src_df)

        if target_columns is None:
            target_columns = list(target_keys) + [
                item["target_column"] for item in column_mappings
            ]
        target_columns = [column for column in target_columns if column]

        if source_keys:
            key_values = self._extract_values(src_df, source_keys)
            if key_values:
                tgt_df, target_queries = self._keyed_sample_cohort(
                    target_conn, target_dialect, tgt_fqn, target_columns,
                    target_where, target_keys, key_values, lookup, timings,
                )
            else:
                target_queries = [
                    self._select_sql(
                        tgt_fqn,
                        target_projection,
                        target_where,
                        top=sample_size,
                        order_by=(
                            target_dialect.random_order()
                            if strategy == "random"
                            else ", ".join(
                                target_dialect.quote(key) for key in target_keys
                            )
                        ),
                        dialect=target_dialect,
                    )
                ]
                tgt_df = self._timed_statement_frame(
                    target_conn, target_queries[0], timings
                )
        else:
            if strategy != "random":
                raise ValueError("Deterministic sampling requires table key_mappings")
            if not column_mappings:
                raise ValueError(
                    "Keyless random sampling requires compare_column_mappings"
                )
            if len(src_df):
                try:
                    tgt_df, target_queries = self._keyless_sample_cohort(
                        target_conn, target_dialect, tgt_fqn, target_columns,
                        target_projection, target_where, column_mappings, src_df,
                        lookup, timings,
                    )
                except Exception as exc:
                    self._note_failed_stage(exc, "keyless target lookup")
                    raise
            else:
                # An empty source sample owns an empty cohort, so no target
                # statement is issued at all. The target is never sampled
                # independently on the keyless path: what used to run here was
                # a second ``TOP <n> ORDER BY <random>`` read of the target,
                # immediately trimmed to the source signature counts. With no
                # source rows that trim discarded every fetched row, so the
                # statement only ever cost a full randomized scan of the target
                # and could not affect the comparison. The empty frame keeps
                # the configured target columns so column resolution and the
                # detail payloads behave exactly as before.
                target_queries = []
                tgt_df = pd.DataFrame(columns=list(target_columns))
        if not isinstance(tgt_df, pd.DataFrame):
            raise TypeError("Connector execute_query must return a pandas DataFrame")
        timings["target_rows"] = len(tgt_df)
        timings["target_lookup_rows"] = len(tgt_df)
        timings["target_statements"] = len(target_queries)
        self._note_peak_rows(timings, tgt_df)
        self._log_sample_lookup_summary(timings)
        return src_df, tgt_df, [source_sql], target_queries

    # ── Sampled comparison: configuration ─────────────────────
    @classmethod
    def _resolve_sample_lookup_config(
        cls, comparison_settings: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Resolve target lookup batching, statement ceilings and the gate.

        ``comparison_settings.comparison_tiers.target_key_lookup_batch_size``
        (an existing setting) sizes keyed lookups; the remaining keys default
        to the module constants when absent. Every value must be a positive
        integer; a bad value is a configuration error, never a silent default.
        """
        settings = comparison_settings if isinstance(comparison_settings, dict) else {}

        def section(name: str) -> Dict[str, Any]:
            raw = settings.get(name)
            if raw is None:
                return {}
            if not isinstance(raw, dict):
                raise ValueError(f"comparison_settings.{name} must be a YAML mapping")
            return raw

        tiers = section("comparison_tiers")
        safety = section("query_safety")
        sampling = section("large_table_sampling")

        def positive(raw: Dict[str, Any], key: str, default: int, path: str) -> int:
            value = raw.get(key, default)
            if not cls._is_positive_int(value):
                raise ValueError(f"comparison_settings.{path}.{key} must be a positive integer")
            return int(value)

        return {
            "keyed_batch_size": positive(
                tiers, "target_key_lookup_batch_size",
                DEFAULT_KEYED_LOOKUP_BATCH_SIZE, "comparison_tiers",
            ),
            "keyless_batch_size": positive(
                tiers, "target_keyless_lookup_batch_size",
                DEFAULT_KEYLESS_LOOKUP_BATCH_SIZE, "comparison_tiers",
            ),
            "discriminator_columns": positive(
                tiers, "keyless_discriminator_columns",
                DEFAULT_KEYLESS_DISCRIMINATOR_COLUMNS, "comparison_tiers",
            ),
            "discriminator_max_columns": positive(
                tiers, "keyless_discriminator_max_columns",
                DEFAULT_KEYLESS_DISCRIMINATOR_MAX_COLUMNS, "comparison_tiers",
            ),
            "candidate_row_multiplier": positive(
                tiers, "keyless_candidate_row_multiplier",
                DEFAULT_KEYLESS_CANDIDATE_ROW_MULTIPLIER, "comparison_tiers",
            ),
            "max_sql_length_bytes": positive(
                safety, "max_sql_length_bytes",
                DEFAULT_MAX_SQL_LENGTH_BYTES, "query_safety",
            ),
            "max_predicates_per_query": positive(
                safety, "max_predicates_per_query",
                DEFAULT_MAX_PREDICATES_PER_QUERY, "query_safety",
            ),
            "max_concurrent_comparisons": positive(
                sampling, "max_concurrent_comparisons",
                DEFAULT_MAX_CONCURRENT_LARGE_COMPARISONS, "large_table_sampling",
            ),
            # Batch-level concurrency inside one sampled comparison. ``None``
            # means "not configured here": the runtime then consults the
            # existing performance.parallel_execution.batch_parallelism block.
            "target_lookup_parallelism": (
                positive(tiers, "target_lookup_parallelism", 1, "comparison_tiers")
                if tiers.get("target_lookup_parallelism") is not None else None
            ),
            "target_lookup_parallelism_by_connector": cls._parallelism_by_connector(
                tiers.get("target_lookup_parallelism_by_connector")
            ),
            "adaptive_parallelism": cls._adaptive_parallelism_config(
                tiers.get("adaptive_parallelism")
            ),
        }

    @classmethod
    def _parallelism_by_connector(cls, raw: Any) -> Dict[str, int]:
        if raw is None:
            return {}
        if not isinstance(raw, dict):
            raise ValueError(
                "comparison_settings.comparison_tiers."
                "target_lookup_parallelism_by_connector must be a YAML mapping"
            )
        resolved: Dict[str, int] = {}
        for connector, value in raw.items():
            if not cls._is_positive_int(value):
                raise ValueError(
                    "comparison_settings.comparison_tiers."
                    f"target_lookup_parallelism_by_connector.{connector} must be a "
                    "positive integer"
                )
            resolved[normalize_connection_type(connector)] = int(value)
        return resolved

    @classmethod
    def _adaptive_parallelism_config(cls, raw: Any) -> Dict[str, Any]:
        config = dict(DEFAULT_ADAPTIVE_PARALLELISM)
        if raw is None:
            return config
        if not isinstance(raw, dict):
            raise ValueError(
                "comparison_settings.comparison_tiers.adaptive_parallelism must be a YAML mapping"
            )
        config.update(raw)
        path = "comparison_settings.comparison_tiers.adaptive_parallelism"
        if not isinstance(config.get("enabled"), bool):
            raise ValueError(f"{path}.enabled must be boolean")
        if not cls._is_positive_int(config.get("min_workers")):
            raise ValueError(f"{path}.min_workers must be a positive integer")
        factor = config.get("reduce_factor")
        if (
            isinstance(factor, bool) or not isinstance(factor, (int, float))
            or not 0 < float(factor) < 1
        ):
            raise ValueError(f"{path}.reduce_factor must be a number between 0 and 1")
        config["min_workers"] = int(config["min_workers"])
        config["reduce_factor"] = float(factor)
        return config

    # ── Sampled comparison: batch-level parallelism ───────────
    def _target_lookup_parallelism(
        self, connection, lookup: Dict[str, Any], batch_count: int
    ) -> Tuple[int, Dict[str, Any]]:
        """Resolve how many lookup batches may be in flight for this target.

        Resolution order (all configuration, nothing hard-coded):

        1. ``comparison_tiers.target_lookup_parallelism_by_connector`` for the
           target connector's type, else ``comparison_tiers.target_lookup_parallelism``;
        2. otherwise ``performance.parallel_execution.batch_parallelism.max_workers_per_table``
           when that block is enabled; otherwise 1 (sequential).

        The result is capped by the target connection pool capacity, by the
        run's global concurrency ceiling and by the number of batches, so no
        configuration can put more statements in flight than the pool or the
        governor can serve. Every cap is reported in the returned details.
        """
        details: Dict[str, Any] = {"parallelism_caps": []}
        connector_type = self._connector_type(connection)
        details["target_connector_type"] = connector_type
        configured = None
        source = None
        by_connector = lookup.get("target_lookup_parallelism_by_connector") or {}
        if connector_type and connector_type in by_connector:
            configured = int(by_connector[connector_type])
            source = f"comparison_tiers.target_lookup_parallelism_by_connector.{connector_type}"
        elif lookup.get("target_lookup_parallelism") is not None:
            configured = int(lookup["target_lookup_parallelism"])
            source = "comparison_tiers.target_lookup_parallelism"
        else:
            parallel = getattr(getattr(self, "_execution_config", None), "parallel_execution", None)
            batch_parallelism = getattr(parallel, "batch_parallelism", None)
            if (
                getattr(parallel, "enabled", False)
                and getattr(batch_parallelism, "enabled", False)
                and self._is_positive_int(getattr(batch_parallelism, "max_workers_per_table", None))
            ):
                configured = int(batch_parallelism.max_workers_per_table)
                source = "performance.parallel_execution.batch_parallelism.max_workers_per_table"
            else:
                configured = DEFAULT_TARGET_LOOKUP_PARALLELISM
                source = "default (no batch parallelism configured)"
        details["configured_parallelism"] = configured
        details["parallelism_source"] = source
        effective = max(1, configured)

        pool_capacity = self._pool_capacity()
        details["target_pool_capacity"] = pool_capacity
        if pool_capacity is not None and effective > pool_capacity:
            details["parallelism_caps"].append(
                f"connection_pool_capacity={pool_capacity}"
            )
            effective = pool_capacity

        governor_limit = self._global_concurrency_limit(connection)
        details["global_concurrency_limit"] = governor_limit
        if governor_limit is not None and effective > governor_limit:
            details["parallelism_caps"].append(
                f"global_max_concurrency={governor_limit}"
            )
            effective = governor_limit

        if batch_count and effective > batch_count:
            details["parallelism_caps"].append(f"batch_count={batch_count}")
            effective = batch_count
        effective = max(1, int(effective))
        details["parallel_workers"] = effective
        if details["parallelism_caps"]:
            logger.info(
                "[SAMPLE_PARALLELISM] table=%s configured=%d effective=%d source=%s "
                "capped_by=%s",
                getattr(self, "_current_table_id", None), configured, effective,
                source, ",".join(details["parallelism_caps"]),
            )
        return effective, details

    @staticmethod
    def _connector_type(connection) -> Optional[str]:
        cfg = getattr(connection, "cfg", None)
        raw = getattr(cfg, "type", None) if cfg is not None else None
        if raw is None and isinstance(cfg, dict):
            raw = cfg.get("type")
        if raw:
            return normalize_connection_type(raw)
        name = type(connection).__name__.lower()
        for token in ("fabric", "sqlserver"):
            if token in name.replace("_", ""):
                return token
        return None

    def _pool_capacity(self) -> Optional[int]:
        """Connections one target engine can hand out at the same time."""
        pool = getattr(getattr(self, "_execution_config", None), "connection_pool", None)
        if pool is None:
            return None
        if not getattr(pool, "enabled", False):
            return SQLALCHEMY_DEFAULT_POOL_CAPACITY
        size = getattr(pool, "pool_size", None)
        overflow = getattr(pool, "max_overflow", 0) or 0
        if not self._is_positive_int(size):
            return None
        return int(size) + int(overflow)

    @staticmethod
    def _global_concurrency_limit(connection) -> Optional[int]:
        governor = getattr(connection, "concurrency_governor", None)
        limit = getattr(governor, "limit", None)
        if isinstance(limit, int) and not isinstance(limit, bool) and limit > 0:
            return limit
        return None

    def _run_lookup_batches(
        self,
        batches: Sequence[_LookupBatch],
        run_batch: Callable[[_LookupBatch], _LookupBatchResult],
        process: Callable[[_LookupBatchResult], None],
        workers: int,
        lookup: Dict[str, Any],
        timings: Dict[str, Any],
        strategy_label: str,
    ) -> None:
        """Execute lookup batches with bounded concurrency, consume them in order.

        * At most ``workers`` batches are in flight; a new batch is submitted
          only when an earlier one has been consumed, so ``max_inflight_batches``
          never exceeds the configured worker count and at most ``workers``
          result frames exist before they are processed.
        * Results are consumed strictly in batch order whatever the completion
          order, so matched / missing counts, duplicate handling and evidence
          are exactly what sequential execution produces.
        * A failed batch stops the comparison: pending batches are cancelled,
          running ones are awaited, and the error propagates. Partial counts
          are never reported.
        * Adaptive reduction: a statement timeout or resource-pressure failure
          shrinks the in-flight window (``adaptive_parallelism``) and retries
          that one batch once at the reduced level. Sample and batches are
          unchanged; every reduction is logged.
        """
        total = len(batches)
        timings["batch_count"] = total
        timings["parallel_workers"] = workers
        adaptive = lookup.get("adaptive_parallelism") or DEFAULT_ADAPTIVE_PARALLELISM
        wall_started = time.perf_counter()
        inflight = {"now": 0, "peak": 0}
        guard = threading.Lock()

        def tracked(batch: _LookupBatch) -> _LookupBatchResult:
            with guard:
                inflight["now"] += 1
                inflight["peak"] = max(inflight["peak"], inflight["now"])
            started = time.perf_counter()
            try:
                result = run_batch(batch)
                result.elapsed_seconds = time.perf_counter() - started
                return result
            except Exception as exc:
                elapsed = time.perf_counter() - started
                sql_bytes = getattr(exc, "sql_bytes", 0) or 0
                raise _LookupBatchFailed(
                    sanitize_error_text(exc),
                    index=batch.index, rows=batch.rows, sql_bytes=sql_bytes,
                    elapsed_seconds=elapsed, error_type=type(exc).__name__,
                    cause=exc,
                ) from exc
            finally:
                with guard:
                    inflight["now"] -= 1

        def absorb(result: _LookupBatchResult) -> None:
            timings["batch_elapsed_sum_seconds"] += result.elapsed_seconds
            timings["sql_generation_seconds"] += result.sql_generation_seconds
            timings["target_execute_seconds"] += result.target_execute_seconds
            timings["target_extract_seconds"] += result.target_extract_seconds
            timings["connection_wait_seconds"] += result.connection_wait_seconds
            timings["db_execution_seconds"] += result.db_execution_seconds
            timings["hash_seconds"] += result.hash_seconds
            timings["safety_splits"] += result.safety_splits
            timings["max_sql_bytes"] = max(timings["max_sql_bytes"], result.max_sql_bytes)
            if result.peak_frame_rows > timings.get("peak_frame_rows", 0):
                timings["peak_frame_rows"] = result.peak_frame_rows
            timings["lookup_batches"] += 1
            timings["target_statements"] = timings.get("target_statements", 0) + len(
                result.statements
            )
            self._note_peak_memory(timings)
            process(result)

        def fail(failure: _LookupBatchFailed, workers_now: int) -> None:
            logger.error(
                "Target lookup failed | table=%s | validation=record_comparison | "
                "strategy=%s | batch=%d/%d | rows=%d | sql_bytes=%d | elapsed=%.1fs | "
                "parallel_workers=%d | error_type=%s | error_class=%s | error=%s",
                timings.get("table_id"), strategy_label, failure.index + 1, total,
                failure.rows, failure.sql_bytes, failure.elapsed_seconds, workers_now,
                failure.error_type, error_class_of(failure.cause), str(failure),
            )

        def adaptive_retry_allowed(failure: _LookupBatchFailed, window: int) -> bool:
            cause = failure.cause
            pressure = _is_query_timeout_error(cause) or any(
                token in str(cause).lower()
                for token in ("resource", "exhaust", "memory", "busy", "throttl")
            )
            return bool(
                adaptive.get("enabled")
                and pressure
                and window > int(adaptive.get("min_workers", 1))
            )

        try:
            if workers <= 1:
                # Sequential path: identical processing order, no thread pool.
                for batch in batches:
                    try:
                        result = tracked(batch)
                    except _LookupBatchFailed as failure:
                        fail(failure, 1)
                        raise failure.cause
                    absorb(result)
                    del result
                return

            window = workers
            pending: deque = deque()
            next_index = 0
            executor = ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix=f"target-lookup-{timings.get('table_id')}",
            )
            try:
                while next_index < total or pending:
                    while len(pending) < window and next_index < total:
                        batch = batches[next_index]
                        pending.append((batch, executor.submit(tracked, batch), False))
                        next_index += 1
                    batch, future, retried = pending.popleft()
                    try:
                        result = future.result()
                    except _LookupBatchFailed as failure:
                        fail(failure, window)
                        if not retried and adaptive_retry_allowed(failure, window):
                            reduced = max(
                                int(adaptive.get("min_workers", 1)),
                                int(math.floor(window * float(adaptive.get("reduce_factor", 0.5)))),
                            )
                            timings["adaptive_parallelism_events"].append({
                                "batch": failure.index + 1,
                                "from_workers": window,
                                "to_workers": reduced,
                                "error_type": failure.error_type,
                            })
                            logger.warning(
                                "[ADAPTIVE_PARALLELISM] table=%s strategy=%s batch=%d/%d "
                                "reducing in-flight lookups %d -> %d after %s; retrying "
                                "that batch once at the reduced level",
                                timings.get("table_id"), strategy_label,
                                failure.index + 1, total, window, reduced,
                                failure.error_type,
                            )
                            window = reduced
                            pending.appendleft((batch, executor.submit(tracked, batch), True))
                            continue
                        raise failure.cause
                    absorb(result)
                    del result
            finally:
                # Cancel what has not started; wait for what has, so no worker
                # touches a connection after the comparison has moved on.
                executor.shutdown(wait=True, cancel_futures=True)
        finally:
            timings["max_inflight_batches"] = inflight["peak"]
            timings["parallel_batch_elapsed_seconds"] += time.perf_counter() - wall_started

    @staticmethod
    def _connector_timing(connection) -> Dict[str, float]:
        """Connection-wait / execution split of this thread's last statement."""
        timing = getattr(connection, "last_query_timing", None)
        if callable(timing):  # pragma: no cover - property expected
            timing = timing()
        if not isinstance(timing, dict):
            return {"connection_wait_seconds": 0.0, "db_execution_seconds": 0.0}
        return {
            "connection_wait_seconds": float(timing.get("connection_wait_seconds") or 0.0),
            "db_execution_seconds": float(timing.get("db_execution_seconds") or 0.0),
        }

    def _sample_lookup_settings(self) -> Dict[str, Any]:
        settings = getattr(self, "_sample_lookup", None)
        if not isinstance(settings, dict):
            settings = self._resolve_sample_lookup_config({})
            self._sample_lookup = settings
        return settings

    def _predicate_ceiling(self) -> int:
        """Rows one per-row predicate statement may carry (the last-resort form)."""
        cohort = getattr(getattr(self, "_execution_config", None), "cohort_processing", None)
        configured = getattr(getattr(cohort, "predicate_or", None), "max_keys", None)
        ceiling = int(configured) if self._is_positive_int(configured) else DEFAULT_PREDICATE_OR_MAX_KEYS
        return min(ceiling, self._sample_lookup_settings()["max_predicates_per_query"])

    def _cohort_builder(self, connection, dialect, chunk_size: int) -> KeyCohortBuilder:
        """The configured set-based lookup strategy for one connection.

        ``performance.cohort_processing`` decides the strategy (``values_join``
        preferred, ``predicate_or`` as the bounded last resort) and the row
        ceilings. ``chunk_size`` keys are rendered per statement; a VALUES
        cohort larger than its per-constructor ceiling is split into several
        UNION ALL blocks inside that one statement.
        """
        execution_config = getattr(self, "_execution_config", None)
        cohort_config = getattr(execution_config, "cohort_processing", None)
        if cohort_config is None:
            cohort_config = SimpleNamespace(
                strategy="auto",
                allowed_strategies=None,
                values_join=SimpleNamespace(max_keys=DEFAULT_VALUES_BLOCK_ROWS),
                predicate_or=SimpleNamespace(
                    enabled=True, max_keys=DEFAULT_PREDICATE_OR_MAX_KEYS
                ),
            )
        try:
            return KeyCohortBuilder.resolve(
                dialect,
                self._capabilities(connection),
                getattr(cohort_config, "strategy", None),
                chunk_size,
                self._sql_literal,
                cohort_config=cohort_config,
                union_values_blocks=True,
            )
        except SqlDialectError as exc:
            logger.warning(
                "Set-based cohort lookup unavailable; using bounded predicate "
                "statements | reason=%s",
                exc,
            )
            return KeyCohortBuilder(
                dialect, "predicate_or",
                min(chunk_size, self._predicate_ceiling()), self._sql_literal,
            )

    # ── Sampled comparison: statement safety ──────────────────
    def _statement_violation(
        self, statements: Sequence[str], key_count: int, lookup: Dict[str, Any]
    ) -> Optional[str]:
        """Why a rendered lookup may not run, or ``None`` when it is within limits."""
        if not statements:
            return None
        keys_per_statement = int(math.ceil(key_count / len(statements)))
        if keys_per_statement > lookup["max_predicates_per_query"]:
            return (
                f"predicates={keys_per_statement} > "
                f"max_predicates_per_query={lookup['max_predicates_per_query']}"
            )
        for sql in statements:
            size = len(sql) if sql.isascii() else len(sql.encode("utf-8"))
            if size > lookup["max_sql_length_bytes"]:
                return (
                    f"sql_bytes={size} > "
                    f"max_sql_length_bytes={lookup['max_sql_length_bytes']}"
                )
        return None

    def _bounded_statement_batches(
        self,
        values: Sequence[Any],
        batch_size: int,
        build,
        lookup: Dict[str, Any],
        timings: Dict[str, Any],
    ) -> Iterator[Tuple[List[Any], List[str], str]]:
        """Yield ``(batch, statements, strategy)`` with every statement within limits.

        ``build(batch)`` renders the statements for one batch of lookup values.
        A batch whose rendering exceeds a configured ceiling is split in half
        and re-rendered, never executed; a single value that still exceeds the
        ceilings is a hard error, because no smaller statement exists.
        """

        def emit(batch: List[Any]):
            generation_started = time.perf_counter()
            statements, strategy_name = build(batch)
            timings["sql_generation_seconds"] += time.perf_counter() - generation_started
            violation = self._statement_violation(statements, len(batch), lookup)
            if violation is None:
                for sql in statements:
                    size = len(sql) if sql.isascii() else len(sql.encode("utf-8"))
                    if size > timings["max_sql_bytes"]:
                        timings["max_sql_bytes"] = size
                yield batch, list(statements), strategy_name
                return
            if len(batch) <= 1:
                raise ValueError(
                    "A single-row target lookup statement exceeds the configured "
                    f"query safety limits ({violation}); raise "
                    "comparison_settings.query_safety or reduce the compared columns"
                )
            timings["safety_splits"] += 1
            logger.warning(
                "[QUERY_SAFETY] table=%s generated statement exceeded limits; "
                "splitting lookup batch | rows=%d | %s",
                timings.get("table_id"), len(batch), violation,
            )
            half = len(batch) // 2
            yield from emit(batch[:half])
            yield from emit(batch[half:])

        for batch in self._batches(values, batch_size):
            yield from emit(list(batch))

    # ── Sampled comparison: execution helpers ─────────────────
    def _timed_statement_frame(
        self, connection, sql: str, timings: Dict[str, Any]
    ) -> pd.DataFrame:
        """Run one statement with the configured retry policy, timing it."""
        started = time.perf_counter()
        try:
            return self._execute_batch_query(connection, sql)
        finally:
            timings["target_execute_seconds"] += time.perf_counter() - started
            connector = self._connector_timing(connection)
            timings["connection_wait_seconds"] = (
                timings.get("connection_wait_seconds", 0.0)
                + connector["connection_wait_seconds"]
            )
            timings["db_execution_seconds"] = (
                timings.get("db_execution_seconds", 0.0)
                + connector["db_execution_seconds"]
            )

    def _iter_statement_frames(
        self, connection, sql: str, chunk_rows: int, timings: Dict[str, Any]
    ) -> Iterator[pd.DataFrame]:
        """Yield one statement's result in bounded frames, one frame in memory.

        Connectors exposing ``execute_query_batches`` stream the result so a
        candidate lookup can be abandoned as soon as it exceeds its budget;
        the time to the first frame is recorded as database execution, later
        frames as extraction. Other connectors return the single frame.
        """
        batches = getattr(connection, "execute_query_batches", None)
        started = time.perf_counter()
        if not callable(batches):
            frame = self._execute_batch_query(connection, sql)
            timings["target_execute_seconds"] += time.perf_counter() - started
            yield frame
            return
        iterator = batches(sql, chunk_rows)
        first = True
        try:
            for frame in iterator:
                elapsed = time.perf_counter() - started
                if first:
                    timings["target_execute_seconds"] += elapsed
                    first = False
                else:
                    timings["target_extract_seconds"] += elapsed
                yield frame
                started = time.perf_counter()
        finally:
            close = getattr(iterator, "close", None)
            if callable(close):
                close()

    @staticmethod
    def _note_peak_rows(timings: Dict[str, Any], frame: Any) -> None:
        rows = len(frame) if isinstance(frame, pd.DataFrame) else 0
        if rows > timings.get("peak_frame_rows", 0):
            timings["peak_frame_rows"] = rows

    def _note_peak_memory(self, timings: Dict[str, Any]) -> None:
        """Track the highest process RSS observed while batches are consumed."""
        current = self._process_memory_mb()
        if current is None:
            return
        previous = timings.get("peak_memory_mb")
        if previous is None or current > previous:
            timings["peak_memory_mb"] = current

    @staticmethod
    def _concat_bounded(frames: List[pd.DataFrame], template: Optional[pd.DataFrame],
                        columns: Sequence[str]) -> pd.DataFrame:
        """Join bounded frames exactly once; an empty result keeps its columns."""
        populated = [frame for frame in frames if not frame.empty]
        if not populated:
            # A lookup that matched nothing still has to carry the requested
            # columns. Connectors are not required to describe the columns of a
            # zero-row result, and without them the comparison would read "no
            # rows found" as missing metadata and fail as a framework error
            # instead of reporting the rows as absent on that side.
            requested = [column for column in columns if column]
            described = (
                set() if template is None
                else {str(column) for column in template.columns}
            )
            if template is not None and not set(requested) - described:
                return template.iloc[0:0].reset_index(drop=True)
            return pd.DataFrame(
                {column: pd.Series(dtype=object) for column in requested}
            )
        if len(populated) == 1:
            return populated[0].reset_index(drop=True)
        return pd.concat(populated, ignore_index=True)

    # ── Sampled comparison: keyed cohort ──────────────────────
    def _keyed_sample_cohort(
        self,
        connection,
        dialect,
        fqn: str,
        columns: Sequence[str],
        where_clause: str,
        key_columns: Sequence[str],
        key_values: Sequence[Tuple[Any, ...]],
        lookup: Dict[str, Any],
        timings: Dict[str, Any],
        lookup_side: str = "target",
    ) -> Tuple[pd.DataFrame, List[str]]:
        """Fetch the rows of the looked-up side for the driving side's keys.

        Each page of ``keyed_batch_size`` distinct keys becomes one statement
        (a VALUES join by default, bounded predicates as the configured last
        resort), checked against the SQL-size ceilings before it runs. The
        rows kept are bounded by ``keys × candidate_row_multiplier``: a keyed
        cohort whose looked-up side returns more than that is not unique and is
        refused rather than materialized without limit. Frames are joined once.

        ``lookup_side`` names the side actually being queried, because a
        target-driven incremental comparison drives from the target and looks
        the keys up in the *source*. It only labels the diagnostics; the rows
        fetched and returned are the same either way.
        """
        unique_keys = list(dict.fromkeys(key_values))
        builder = self._cohort_builder(connection, dialect, lookup["keyed_batch_size"])
        timings["lookup_strategy"] = builder.strategy
        timings["lookup_batch_size"] = builder.keys_per_statement
        row_budget = max(len(unique_keys), 1) * lookup["candidate_row_multiplier"]
        frames: List[pd.DataFrame] = []
        template: Optional[pd.DataFrame] = None
        statements: List[str] = []
        rows = 0

        def build(batch):
            plan = builder.plan(fqn, columns, where_clause, key_columns, batch)
            return list(plan.queries), plan.strategy

        timings["lookup_side"] = lookup_side
        # Every page is rendered and size-checked up front on this thread, so
        # each batch is a fixed unit of work with a stable index. The exact
        # keys of the driving side are what the other side is asked for: the
        # looked-up side never samples on its own.
        batches = [
            _LookupBatch(index=index, payload=(page, queries, strategy_name), rows=len(page))
            for index, (page, queries, strategy_name) in enumerate(
                self._bounded_statement_batches(
                    unique_keys, builder.keys_per_statement, build, lookup, timings
                )
            )
        ]
        workers, parallel_details = self._target_lookup_parallelism(
            connection, lookup, len(batches)
        )
        timings.update(parallel_details)

        def run_batch(batch: _LookupBatch) -> _LookupBatchResult:
            page, queries, strategy_name = batch.payload
            result = _LookupBatchResult(index=batch.index, strategy=strategy_name)
            for sql in queries:
                started = time.perf_counter()
                try:
                    frame = self._execute_batch_query(connection, sql)
                except Exception as exc:
                    self._note_failed_statement(exc, sql)
                    raise
                result.target_execute_seconds += time.perf_counter() - started
                connector = self._connector_timing(connection)
                result.connection_wait_seconds += connector["connection_wait_seconds"]
                result.db_execution_seconds += connector["db_execution_seconds"]
                result.statements.append(sql)
                result.max_sql_bytes = max(result.max_sql_bytes, len(sql))
                if not isinstance(frame, pd.DataFrame):
                    raise TypeError("Connector execute_query must return a pandas DataFrame")
                result.rows += len(frame)
                if result.rows > row_budget:
                    raise ValueError(
                        f"Keyed sample cohort returned {result.rows} target rows for "
                        f"{len(unique_keys)} sampled keys, above the safety budget "
                        f"of {row_budget}; the target key is not unique for this "
                        "cohort"
                    )
                result.frames.append(frame)
            return result

        def process(result: _LookupBatchResult) -> None:
            nonlocal template, rows
            page, queries, _strategy = batches[result.index].payload
            timings["lookup_strategy"] = result.strategy
            batch_rows = 0
            for frame in result.frames:
                if template is None:
                    template = frame.iloc[0:0]
                if frame.empty:
                    continue
                self._note_peak_rows(timings, frame)
                rows += len(frame)
                batch_rows += len(frame)
                if rows > row_budget:
                    raise ValueError(
                        f"Keyed sample cohort returned {rows} {lookup_side} rows "
                        f"for {len(unique_keys)} looked-up keys, above the safety "
                        f"budget of {row_budget}; the {lookup_side} key is not "
                        "unique for this cohort"
                    )
                frames.append(frame)
            statements.extend(result.statements)
            logger.info(
                "[SAMPLE_LOOKUP] table=%s validation=record_comparison "
                "strategy=keyed_cohort batch=%d/%d keys=%d workers=%d "
                "statements=%d lookup=%s lookup_side=%s sql_bytes=%d "
                "%s_lookup_rows=%d elapsed=%.3fs "
                "connection_wait=%.3fs db_execution=%.3fs",
                timings.get("table_id"), result.index + 1, len(batches), len(page),
                workers, len(result.statements), result.strategy, lookup_side,
                result.max_sql_bytes, lookup_side,
                batch_rows, result.elapsed_seconds, result.connection_wait_seconds,
                result.db_execution_seconds,
            )
            result.frames = []

        try:
            self._run_lookup_batches(
                batches, run_batch, process, workers, lookup, timings, "keyed_cohort"
            )
            timings["candidate_rows"] = rows
            return self._concat_bounded(frames, template, columns), statements
        finally:
            frames.clear()

    # ── Sampled comparison: keyless cohort ────────────────────
    def _keyless_sample_cohort(
        self,
        connection,
        dialect,
        fqn: str,
        columns: Sequence[str],
        projection: str,
        where_clause: str,
        column_mappings: Sequence[Dict[str, str]],
        src_df: pd.DataFrame,
        lookup: Dict[str, Any],
        timings: Dict[str, Any],
        lookup_side: str = "target",
    ) -> Tuple[pd.DataFrame, List[str]]:
        """Fetch the looked-up rows identical to the driving side's rows, bounded.

        ``lookup_side`` names the side actually queried (a target-driven
        incremental comparison drives from the target and looks rows up in the
        source). It labels the diagnostics only.

        Semantics are unchanged from the original signature cohort: for every
        sampled source row, the target rows with identical compared values are
        kept up to the source multiplicity, and the multiset comparison then
        decides matched / missing. What changed is the fetch:

        * The source sample is hashed once (the existing canonical row hash).
        * A small set of *discriminator* columns (``keyless_discriminator_columns``
          of the compared columns, chosen from the sample for selectivity and
          absence of NULLs/floats) is looked up in the target with the same
          set-based cohort statement a keyed table uses, in batches of
          ``keyless_batch_size`` sampled rows. This is a database-side
          candidate filter only; every candidate row is then hashed in Python
          and kept only if it is identical on *all* compared columns and the
          source multiplicity is not yet satisfied.
        * If a cohort's candidates exceed ``rows × keyless_candidate_row_multiplier``
          (a weak discriminator), that cohort alone is re-fetched with a wider
          discriminator (next tier: twice the columns, still one compact
          VALUES join, up to ``keyless_discriminator_max_columns``); the widest
          tier streams every candidate without a budget. No all-column
          per-row predicate is ever generated, and nothing is truncated or
          skipped.

        Peak memory is the source sample, the retained rows (at most the
        sample size) and one streamed candidate frame per worker.
        """
        source_columns = [item["source_column"] for item in column_mappings]
        target_compare = [item["target_column"] for item in column_mappings]
        hash_started = time.perf_counter()
        source_hashes = self._row_hashes(src_df, source_columns)
        source_counts = Counter(source_hashes)
        timings["hash_seconds"] += time.perf_counter() - hash_started
        # Hashes computed here are reused by the final multiset comparison
        # (identical function, identical columns), so no side is hashed twice.
        target_hashes: List[str] = []
        self._sample_hash_cache = {
            "source_columns": list(source_columns),
            "source_hashes": source_hashes,
            "target_columns": list(target_compare),
            "target_hashes": target_hashes,
        }

        disc_source, disc_target, selectivity = self._select_discriminator_columns(
            src_df, column_mappings, lookup["discriminator_columns"]
        )
        # Progressive widening tiers: every tier is a superset of the base
        # discriminator (nested prefixes), so cohort ownership by base tuple
        # keeps every physical target row fetched by exactly one cohort.
        tiers = self._discriminator_tiers(
            src_df, column_mappings, disc_source, disc_target,
            lookup["discriminator_max_columns"],
        )
        timings["discriminator_columns"] = list(disc_target)
        timings["discriminator_selectivity"] = selectivity
        timings["discriminator_tiers"] = [len(tier[0]) for tier in tiers]
        builder = self._cohort_builder(connection, dialect, lookup["keyless_batch_size"])
        timings["lookup_strategy"] = builder.strategy
        timings["lookup_batch_size"] = lookup["keyless_batch_size"]
        timings["lookup_side"] = lookup_side
        multiplier = lookup["candidate_row_multiplier"]

        retained_counts: Counter = Counter()
        retained_frames: List[pd.DataFrame] = []
        template: Optional[pd.DataFrame] = None
        statements: List[str] = []
        total = len(src_df)
        batch_size = lookup["keyless_batch_size"]

        # The sample is cut into signature cohorts of about ``batch_size``
        # sampled rows, in source order, with one invariant: a discriminator
        # tuple belongs to exactly one cohort, and that cohort owns every
        # sampled row carrying the tuple. A target row therefore reaches the
        # comparison through exactly one lookup, so a source row duplicated
        # across the sample (multiset semantics: ``(row_hash, occurrences)``)
        # is never satisfied twice by the same physical target row, and the
        # exact fallback of a cohort carries the row's full multiplicity.
        #
        # Two-stage retention keeps the result identical to sequential
        # processing while workers run concurrently:
        #
        # * On the worker, every streamed candidate frame is hashed at once and
        #   pre-filtered against the *fixed* source multiset: for each hash at
        #   most ``source_counts[hash]`` rows of this cohort are kept, in
        #   stream order. This depends only on the cohort itself, so it is the
        #   same whatever order cohorts complete in, and the frame is released
        #   immediately afterwards (one streamed frame alive per worker).
        # * On this thread, cohorts are consumed strictly in index order and
        #   the cross-cohort cap ``retained_counts`` is applied to the kept
        #   rows, exactly as sequential processing would.
        batches = self._keyless_cohort_batches(src_df, disc_source, batch_size)
        workers, parallel_details = self._target_lookup_parallelism(
            connection, lookup, len(batches)
        )
        timings.update(parallel_details)
        # Shared across this table's cohorts (workers included) so a width
        # already measured as too weak is not re-attempted by every cohort.
        tier_floor = _TierFloor()

        def run_batch(batch: _LookupBatch) -> _LookupBatchResult:
            tuples, positions = batch.payload
            batch_frame = src_df.iloc[positions]
            budget = len(batch_frame) * multiplier
            local = self._local_batch_timings(timings)
            result = _LookupBatchResult(index=batch.index, strategy=builder.strategy)
            pre_counts: Counter = Counter()
            kept_frames: List[pd.DataFrame] = []
            kept_hashes: List[List[str]] = []

            def on_frame(frame) -> None:
                # Worker-side: hash, pre-filter, release. Never touches shared
                # state.
                if not isinstance(frame, pd.DataFrame):
                    raise TypeError("Connector execute_query must return a pandas DataFrame")
                if result.template is None:
                    result.template = frame.iloc[0:0]
                if frame.empty:
                    return
                if len(frame) > local["peak_frame_rows"]:
                    local["peak_frame_rows"] = len(frame)
                started = time.perf_counter()
                frame_hashes = self._row_hashes(frame, target_compare)
                result.matching_counts.update(
                    row_hash for row_hash in frame_hashes if row_hash in source_counts
                )
                result.matching_rows = sum(result.matching_counts.values())
                kept = self._prefilter_matching_rows(
                    frame, target_compare, source_counts, pre_counts,
                    row_hashes=frame_hashes,
                )
                local["hash_seconds"] += time.perf_counter() - started
                if kept is not None:
                    kept_frames.append(kept[0])
                    kept_hashes.append(kept[1])

            # Tier 0 is the cohort's own (base) tuples. When a tier's candidates
            # exceed the budget the discriminator is widened to the next tier
            # (still one compact VALUES join on a few columns) and the cohort is
            # re-fetched; the widest tier runs without a budget so nothing is
            # ever truncated or skipped. Retention stays bounded whatever the
            # candidate volume: one streamed frame per worker, kept rows capped
            # by the source multiplicity.
            stmts: List[str] = []
            # Start at the narrowest tier no cohort of this table has already
            # disproved. Tiers below the floor were measured as too weak for
            # this data, so re-running them would spend a full candidate
            # budget to reach the same conclusion.
            start_tier = min(tier_floor.index, len(tiers) - 1)
            result.start_tier = start_tier
            for tier_index, (tier_source, tier_target) in enumerate(tiers):
                if tier_index < start_tier:
                    continue
                final_tier = tier_index == len(tiers) - 1
                if tier_index == 0:
                    values = list(tuples)
                else:
                    values = self._normalized_tuples(batch_frame, tier_source)
                tier_budget = None if final_tier else budget

                def build_tier(page, _columns=tier_target):
                    plan = builder.plan(fqn, columns, where_clause, _columns, page)
                    return list(plan.queries), plan.strategy

                pre_counts.clear()
                kept_frames.clear()
                kept_hashes.clear()
                result.matching_rows = 0
                result.matching_counts.clear()
                result.template = None
                result.discriminator_width = len(tier_target)
                try:
                    stmts = self._collect_candidate_frames(
                        connection, values, builder.keys_per_statement,
                        build_tier, tier_budget, lookup, local,
                        lambda rows, _budget=budget: _CandidateBudgetExceeded(
                            f"{rows} candidate rows > budget {_budget}"
                        ),
                        on_frame,
                        page_rows=(
                            budget + 1 if tier_budget is not None
                            else UNBOUNDED_CANDIDATE_PAGE_ROWS
                        ),
                    )
                except _CandidateBudgetExceeded as exc:
                    # Weak discriminator at this width: discard the partial
                    # candidates and widen. Never an all-column predicate.
                    result.widened += 1
                    result.used_fallback = True
                    # This width is now known too weak for this table, so the
                    # remaining cohorts skip it (results are unaffected).
                    tier_floor.raise_to(tier_index + 1)
                    next_width = len(tiers[tier_index + 1][1])
                    logger.warning(
                        "[SAMPLE_LOOKUP] table=%s strategy=keyless_signature "
                        "batch=%d/%d rows=%d discriminator_columns=%d candidates "
                        "exceeded budget (%s); widening discriminator to %d columns"
                        "%s",
                        timings.get("table_id"), batch.index + 1, len(batches),
                        len(batch_frame), len(tier_target), exc, next_width,
                        " (final tier, unbudgeted streaming)"
                        if tier_index + 1 == len(tiers) - 1 else "",
                    )
                    continue
                result.unbounded = final_tier and tier_index > 0
                break
            result.frames = kept_frames
            result.hashes = kept_hashes
            result.statements = stmts
            result.candidate_rows = int(local["candidate_rows"])
            result.rows = sum(len(frame) for frame in kept_frames)
            self._merge_local_batch_timings(result, local)
            return result

        def process(result: _LookupBatchResult) -> None:
            nonlocal template
            batch_rows = batches[result.index].rows
            if template is None and result.template is not None:
                template = result.template
            batch_counts: Counter = Counter()
            batch_frames: List[pd.DataFrame] = []
            for frame, hashes in zip(result.frames, result.hashes):
                kept = self._apply_retention_cap(
                    frame, hashes, source_counts, retained_counts, batch_counts
                )
                if kept is not None:
                    batch_frames.append(kept[0])
                    target_hashes.extend(kept[1])
            retained_counts.update(batch_counts)
            retained_frames.extend(batch_frames)
            statements.extend(result.statements)
            timings["candidate_rows"] += result.candidate_rows
            timings["lookup_matching_rows"] += result.matching_rows
            self._lookup_matching_counts.update(result.matching_counts)
            if result.used_fallback:
                timings["fallback_batches"] += 1
                timings["widened_batches"] = timings.get("widened_batches", 0) + 1
            if result.unbounded:
                timings["unbounded_batches"] = timings.get("unbounded_batches", 0) + 1
            if result.discriminator_width > timings.get("max_discriminator_columns", 0):
                timings["max_discriminator_columns"] = result.discriminator_width
            logger.info(
                "[SAMPLE_LOOKUP] table=%s validation=record_comparison "
                "strategy=keyless_signature batch=%d/%d rows=%d workers=%d "
                "statements=%d lookup=%s lookup_side=%s discriminator_columns=%d "
                "start_tier=%d widened=%d "
                "sql_bytes=%d candidate_rows=%d retained_rows=%d elapsed=%.3fs "
                "connection_wait=%.3fs db_execution=%.3fs",
                timings.get("table_id"), result.index + 1, len(batches), batch_rows,
                workers, len(result.statements), result.strategy, lookup_side,
                result.discriminator_width, result.start_tier, result.widened,
                result.max_sql_bytes,
                result.candidate_rows, sum(len(frame) for frame in batch_frames),
                result.elapsed_seconds, result.connection_wait_seconds,
                result.db_execution_seconds,
            )
            result.frames = []
            result.hashes = []

        try:
            self._run_lookup_batches(
                batches, run_batch, process, workers, lookup, timings,
                "keyless_signature",
            )
            return (
                self._concat_bounded(retained_frames, template, columns or target_compare),
                statements,
            )
        finally:
            retained_frames.clear()

    def _keyless_cohort_batches(
        self, src_df: pd.DataFrame, disc_source: Sequence[str], batch_size: int
    ) -> List[_LookupBatch]:
        """Cut the sample into cohorts that each own a disjoint set of tuples.

        Tuples are taken in first-seen source order and a cohort closes once
        the rows it owns reach ``batch_size`` (a single tuple owning more rows
        than that forms a cohort on its own). NULLs are normalized so two NULL
        tuples are the same tuple; the lookup renders them as ``NULL`` with the
        builder's null-safe join. The payload is ``(tuples, positions)``.
        """
        owners: Dict[Tuple[Any, ...], List[int]] = {}
        for position, row in enumerate(self._extract_values(src_df, disc_source)):
            key = tuple(
                None if is_null_scalar(value, blank_is_null=False) else value
                for value in row
            )
            owners.setdefault(key, []).append(position)
        batches: List[_LookupBatch] = []
        tuples: List[Tuple[Any, ...]] = []
        positions: List[int] = []
        for key, owned in owners.items():
            tuples.append(key)
            positions.extend(owned)
            if len(positions) >= batch_size:
                batches.append(_LookupBatch(
                    index=len(batches), payload=(tuples, positions), rows=len(positions),
                ))
                tuples, positions = [], []
        if tuples:
            batches.append(_LookupBatch(
                index=len(batches), payload=(tuples, positions), rows=len(positions),
            ))
        return batches

    def _collect_candidate_frames(
        self,
        connection,
        values: Sequence[Any],
        batch_size: int,
        build,
        budget: Optional[int],
        lookup: Dict[str, Any],
        local: Dict[str, Any],
        exceeded: Callable[[int], Exception],
        on_frame: Callable[[pd.DataFrame], None],
        page_rows: Optional[int] = None,
    ) -> List[str]:
        """Stream one cohort's candidate frames through ``on_frame``.

        Runs on a worker thread: all timing goes to ``local`` (merged into the
        shared instrumentation by the consuming thread). Each streamed frame
        is handed to ``on_frame`` and released before the next one is fetched;
        the cohort is abandoned with ``exceeded(rows)`` as soon as more than
        ``budget`` candidate rows have been seen. ``budget=None`` streams every
        candidate (the caller guarantees bounded retention).
        """
        statements: List[str] = []
        candidate_rows = 0
        if page_rows is None:
            page_rows = (budget + 1) if budget is not None else UNBOUNDED_CANDIDATE_PAGE_ROWS
        for _, queries, _strategy in self._bounded_statement_batches(
            values, batch_size, build, lookup, local
        ):
            for sql in queries:
                statements.append(sql)
                try:
                    for frame in self._iter_statement_frames(connection, sql, page_rows, local):
                        candidate_rows += len(frame) if isinstance(frame, pd.DataFrame) else 0
                        if budget is not None and candidate_rows > budget:
                            raise exceeded(candidate_rows)
                        on_frame(frame)
                        del frame
                except _CandidateBudgetExceeded:
                    raise
                except Exception as exc:
                    self._note_failed_statement(exc, sql)
                    raise
                connector = self._connector_timing(connection)
                local["connection_wait_seconds"] += connector["connection_wait_seconds"]
                local["db_execution_seconds"] += connector["db_execution_seconds"]
        local["candidate_rows"] = candidate_rows
        return statements

    @staticmethod
    def _note_failed_statement(exc: Exception, sql: Any) -> None:
        """Attach the failed statement's size (never its text) to the error."""
        try:
            if not getattr(exc, "sql_bytes", 0):
                exc.sql_bytes = len(str(sql))
        except Exception:  # pragma: no cover - exceptions without __dict__
            pass

    @staticmethod
    def _note_failed_stage(exc: Exception, stage: str) -> None:
        """Attach the failing stage (never data or statement text) to the error.

        The stage travels with the exception so the table-level error field can
        name where the comparison died without the orchestrator having to
        re-inspect driver messages. The first stage set wins: the innermost
        stage is the specific one.
        """
        try:
            if not getattr(exc, "validation_stage", ""):
                exc.validation_stage = stage
        except Exception:  # pragma: no cover - exceptions without __dict__
            pass

    def _prefilter_matching_rows(
        self,
        frame: pd.DataFrame,
        columns: Sequence[str],
        source_counts: Counter,
        pre_counts: Counter,
        row_hashes: Optional[Sequence[str]] = None,
    ) -> Optional[Tuple[pd.DataFrame, List[str]]]:
        """Worker-side retention against the fixed source multiset.

        Keeps, in stream order, at most ``source_counts[hash]`` rows per hash
        for the current cohort. This is a superset of the final cohort (the
        cross-cohort cap is applied later, in order) and depends on nothing
        shared, so it is safe on a worker thread. Returns the kept rows and
        their hashes so nothing is hashed twice.
        """
        self._require_columns(frame, columns, "target signature")
        keep: List[int] = []
        kept_hashes: List[str] = []
        hashes = row_hashes if row_hashes is not None else self._row_hashes(frame, columns)
        for position, row_hash in enumerate(hashes):
            allowed = source_counts.get(row_hash, 0)
            if allowed and pre_counts[row_hash] < allowed:
                pre_counts[row_hash] += 1
                keep.append(position)
                kept_hashes.append(row_hash)
        if not keep:
            return None
        return frame.iloc[keep], kept_hashes

    @staticmethod
    def _apply_retention_cap(
        frame: pd.DataFrame,
        hashes: Sequence[str],
        source_counts: Counter,
        retained_counts: Counter,
        batch_counts: Counter,
    ) -> Optional[Tuple[pd.DataFrame, List[str]]]:
        """Consumer-side cap: keep rows while the source multiplicity allows.

        ``retained_counts`` holds committed earlier cohorts, ``batch_counts``
        the current one; together they are the sequential
        :meth:`_retain_matching_rows` rule applied in cohort order. Returns
        the kept rows with their hashes (same order) so the final comparison
        can reuse them.
        """
        keep: List[int] = []
        kept_hashes: List[str] = []
        for position, row_hash in enumerate(hashes):
            allowed = source_counts.get(row_hash, 0)
            if allowed and retained_counts[row_hash] + batch_counts[row_hash] < allowed:
                batch_counts[row_hash] += 1
                keep.append(position)
                kept_hashes.append(row_hash)
        if not keep:
            return None
        if len(keep) == len(frame):
            return frame, kept_hashes
        return frame.iloc[keep], kept_hashes

    def _normalized_tuples(
        self, frame: pd.DataFrame, columns: Sequence[str]
    ) -> List[Tuple[Any, ...]]:
        """Distinct value tuples of ``columns`` with NULLs normalized to ``None``."""
        seen: Dict[Tuple[Any, ...], None] = {}
        for row in self._extract_values(frame, columns):
            seen.setdefault(tuple(
                None if is_null_scalar(value, blank_is_null=False) else value
                for value in row
            ), None)
        return list(seen)

    def _discriminator_tiers(
        self,
        src_df: pd.DataFrame,
        column_mappings: Sequence[Dict[str, str]],
        base_source: Sequence[str],
        base_target: Sequence[str],
        max_columns: int,
    ) -> List[Tuple[List[str], List[str]]]:
        """Nested discriminator widths for progressive candidate narrowing.

        Tier 0 is the configured base discriminator. Each later tier doubles
        the width by appending the next best-ranked compared columns (no
        NULLs first, non-floating-point, highest distinct count; floating-point
        and very long text columns are never appended because a literal for
        them may not round-trip exactly and would risk dropping a true
        candidate) up to ``max_columns`` or the number of eligible columns.
        Every tier is a prefix-superset of the previous one.
        """
        base = list(zip(base_source, base_target))
        base_names = {source for source, _ in base}
        extra: List[Tuple[str, str]] = []
        for mapping in self._rank_discriminator_candidates(src_df, column_mappings):
            source = mapping["source_column"]
            if source in base_names:
                continue
            extra.append((source, mapping["target_column"]))
        ceiling = max(len(base), min(int(max_columns), len(base) + len(extra)))
        tiers = [base]
        width = len(base)
        while width < ceiling:
            width = min(ceiling, max(width * 2, width + 1))
            tier = base + extra[: width - len(base)]
            tiers.append(tier)
        return [
            ([source for source, _ in tier], [target for _, target in tier])
            for tier in tiers
        ]

    def _rank_discriminator_candidates(
        self, src_df: pd.DataFrame, column_mappings: Sequence[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """Compared columns safe to use as database-side filters, best first."""
        ranked = []
        total = len(src_df)
        for index, mapping in enumerate(column_mappings):
            column = mapping["source_column"]
            if column not in src_df.columns:
                continue
            series = src_df[column]
            null_count = int(series.isna().sum())
            if null_count >= total:
                continue
            non_null = series.dropna()
            sample_values = non_null.head(50).tolist()
            if pd.api.types.is_float_dtype(series.dtype) or any(
                isinstance(value, float) for value in sample_values
            ):
                continue
            if any(isinstance(value, str) and len(value) > 200 for value in sample_values):
                continue
            try:
                distinct = int(non_null.nunique())
            except TypeError:
                continue
            if distinct <= 1 and total > 1:
                continue
            ranked.append((null_count > 0, -distinct, index, mapping))
        ranked.sort(key=lambda item: item[:3])
        return [item[3] for item in ranked]

    @staticmethod
    def _local_batch_timings(timings: Dict[str, Any]) -> Dict[str, Any]:
        """A worker-private timing dictionary with the keys the helpers update."""
        return {
            "table_id": timings.get("table_id"),
            "sql_generation_seconds": 0.0,
            "target_execute_seconds": 0.0,
            "target_extract_seconds": 0.0,
            "connection_wait_seconds": 0.0,
            "db_execution_seconds": 0.0,
            "hash_seconds": 0.0,
            "peak_frame_rows": 0,
            "candidate_rows": 0,
            "lookup_matching_rows": 0,
            "max_sql_bytes": 0,
            "safety_splits": 0,
        }

    @staticmethod
    def _merge_local_batch_timings(result: _LookupBatchResult, local: Dict[str, Any]) -> None:
        result.sql_generation_seconds = local["sql_generation_seconds"]
        result.target_execute_seconds = local["target_execute_seconds"]
        result.target_extract_seconds = local["target_extract_seconds"]
        result.connection_wait_seconds = local["connection_wait_seconds"]
        result.db_execution_seconds = local["db_execution_seconds"]
        result.hash_seconds = local["hash_seconds"]
        result.peak_frame_rows = local["peak_frame_rows"]
        result.max_sql_bytes = local["max_sql_bytes"]
        result.safety_splits = local["safety_splits"]

    def _retain_matching_rows(
        self,
        frame: pd.DataFrame,
        columns: Sequence[str],
        source_counts: Counter,
        retained_counts: Counter,
        batch_counts: Counter,
    ) -> Optional[pd.DataFrame]:
        """Keep the candidate rows identical to a sampled row, up to its multiplicity.

        Incremental form of :meth:`_trim_target_to_source_signature_counts`:
        a row is kept while the rows already retained for its hash (committed
        batches plus the current one) are fewer than the source multiplicity.
        """
        self._require_columns(frame, columns, "target signature")
        keep = []
        for position, row_hash in enumerate(self._row_hashes(frame, columns)):
            allowed = source_counts.get(row_hash, 0)
            if allowed and retained_counts[row_hash] + batch_counts[row_hash] < allowed:
                batch_counts[row_hash] += 1
                keep.append(position)
        if not keep:
            return None
        return frame.iloc[keep]

    def _select_discriminator_columns(
        self,
        src_df: pd.DataFrame,
        column_mappings: Sequence[Dict[str, str]],
        count: int,
    ) -> Tuple[List[str], List[str], Optional[float]]:
        """Choose the compared columns used as the database-side candidate filter.

        Ranking, from the sample only: columns without NULLs first (a NULL key
        cannot seek and an all-NULL row constructor has no type), then
        non-floating-point columns (floating literals round-trip
        imprecisely), then the highest distinct count. The choice only affects
        how many candidate rows come back, never which rows are finally kept.
        """
        ranked = []
        total = len(src_df)
        for index, mapping in enumerate(column_mappings):
            column = mapping["source_column"]
            if column not in src_df.columns:
                continue
            series = src_df[column]
            null_count = int(series.isna().sum())
            if null_count >= total:
                continue
            non_null = series.dropna()
            sample_values = non_null.head(50).tolist()
            is_float = pd.api.types.is_float_dtype(series.dtype) or any(
                isinstance(value, float) for value in sample_values
            )
            long_text = any(
                isinstance(value, str) and len(value) > 200 for value in sample_values
            )
            try:
                distinct = int(non_null.nunique())
            except TypeError:
                continue  # unhashable values cannot be rendered as a key set
            if distinct <= 1 and total > 1:
                continue  # a constant column filters nothing
            ranked.append((null_count > 0, is_float, long_text, -distinct, index, mapping))
        ranked.sort(key=lambda item: item[:5])
        chosen = [item[5] for item in ranked[:count]] or list(column_mappings[:count])
        disc_source = [item["source_column"] for item in chosen]
        disc_target = [item["target_column"] for item in chosen]
        selectivity = None
        if total and disc_source:
            try:
                distinct_tuples = len(set(self._extract_values(src_df, disc_source)))
                selectivity = round(distinct_tuples / total, 4)
            except TypeError:
                selectivity = None
        return disc_source, disc_target, selectivity

    # ── Sampled comparison: concurrency gate ──────────────────
    @classmethod
    def _large_comparison_gate(cls, limit: int) -> threading.BoundedSemaphore:
        with cls._LARGE_COMPARISON_GATE_LOCK:
            gate = cls._LARGE_COMPARISON_GATES.get(limit)
            if gate is None:
                gate = cls._LARGE_COMPARISON_GATES[limit] = threading.BoundedSemaphore(limit)
            return gate

    def _acquire_large_comparison_slot(
        self, lookup: Dict[str, Any], timings: Dict[str, Any]
    ) -> None:
        """Hold one of the configured sampled-comparison slots until the table ends.

        Bounded work per table (the lookup batching) is not enough on its own
        when several large tables run in parallel threads; this caps how many
        of them hold a sample and its cohort at the same time. Small tables
        never take this path. Released in ``validate``'s ``finally``.
        """
        if getattr(self, "_held_gate", None) is not None:
            return
        gate = self._large_comparison_gate(lookup["max_concurrent_comparisons"])
        waiting_from = time.perf_counter()
        gate.acquire()
        waited = time.perf_counter() - waiting_from
        timings["gate_wait_seconds"] += waited
        self._held_gate = gate
        logger.info(
            "[SAMPLE_GATE] table=%s acquired large-comparison slot | limit=%d | waited=%.3fs",
            timings.get("table_id"), lookup["max_concurrent_comparisons"], waited,
        )

    def _release_large_comparison_slot(self) -> None:
        gate = getattr(self, "_held_gate", None)
        if gate is not None:
            self._held_gate = None
            try:
                gate.release()
            except ValueError:  # pragma: no cover - released twice
                pass

    def _release_sample_resources(self) -> None:
        """Drop table-specific large references once the table is finished.

        The sample and cohort frames are locals of ``validate`` and die with
        it; the instrumentation dict and the cached lookup settings are the
        only table-scoped state kept on the instance. Collecting here returns
        the frames' memory before the next table starts instead of when the
        allocator happens to run.
        """
        self._sample_timings = None
        self._sample_hash_cache = None
        gc.collect()

    # ── Sampled comparison: instrumentation ───────────────────
    @staticmethod
    def _process_memory_mb() -> Optional[float]:
        try:
            import psutil  # optional dependency
        except ImportError:
            return None
        try:
            return round(psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024), 1)
        except Exception:  # pragma: no cover - platform specific
            return None

    # ── Sampled comparison: keyless sample selection ──────────
    def _keyless_sample_selection(
        self,
        dialect,
        signature_columns: Sequence[str],
        sample_size: int,
        source_row_count: Optional[int],
        timings: Dict[str, Any],
    ) -> Tuple[str, Optional[str]]:
        """Return ``(where_predicate, order_by)`` for a keyless source sample.

        A keyless sample used to be ``ORDER BY <random>``. That makes the
        engine read every row of the table, generate a value per row and sort
        the entire projection before returning the first page: the cost is the
        table, not the sample. On a large table it exhausts the configured
        statement timeout before a single row is compared.

        The sample is now selected by a deterministic signature over the
        configured compare columns:

        * ``signature % stride = 0``, with ``stride`` the ratio of the table's
          row count to the sample size, keeps an even spread of about
          ``sample_size`` rows and needs no ORDER BY at all, so the engine
          streams rows and stops once the row limit is met. The signature is a
          function of the compared values only, so the same rows are selected
          on every run over unchanged data.
        * When the engine declares no signature expression, the sample orders
          by the compared columns themselves. That keeps the sort but is still
          deterministic and still never random.

        Neither form samples the target. The target cohort is looked up from
        the rows this sample returned, so the source stays the only sampling
        authority (see :meth:`_keyless_sample_cohort`).
        """
        columns = [column for column in signature_columns if column]
        if not columns:
            raise ValueError(
                "Keyless sampling requires compare_column_mappings to select rows"
            )
        stride = 1
        if (
            isinstance(source_row_count, int)
            and source_row_count > 0
            and sample_size > 0
            and source_row_count > sample_size
        ):
            stride = max(1, int(source_row_count // sample_size))
        timings["sample_selection_columns"] = len(columns)
        timings["sample_selection_stride"] = stride

        if dialect.supports_row_signature:
            timings["sample_selection"] = "signature_modulus"
            if stride <= 1:
                # The table is no larger than the sample: every row qualifies,
                # so the row limit alone bounds the read and no predicate or
                # sort is needed.
                return "", None
            signature = dialect.row_signature(columns)
            return f"{signature} % {stride} = 0", None

        # Deterministic, engine-independent fallback: a total order over the
        # compared columns. No randomness, so the sample is still repeatable.
        timings["sample_selection"] = "ordered_compare_columns"
        return "", ", ".join(dialect.quote(column) for column in columns)

    def _new_sample_timings(
        self, sample_size: int, keyed: bool, lookup: Dict[str, Any], compare_columns: int
    ) -> Dict[str, Any]:
        timings = {
            "_started": time.perf_counter(),
            "table_id": getattr(self, "_current_table_id", None),
            "comparison_mode": "sample_comparison",
            "sample_size": sample_size,
            "keyed": keyed,
            "compare_columns": compare_columns,
            "lookup_strategy": None,
            "lookup_batch_size": lookup["keyed_batch_size"] if keyed else lookup["keyless_batch_size"],
            # Which side drives and which side is looked up. A target-driven
            # incremental comparison drives from the target scope and looks the
            # keys up in the source, so "rows returned by the lookup" and "rows
            # in the driving scope" are different measurements and are reported
            # under names that say which is which. (Rows read from the
            # historical target scope are reported by the classifier, under
            # ``historical_rows_read`` and the [HISTORICAL_LOOKUP] log line.)
            "lookup_side": "target",
            "source_scope_rows": None,
            "target_scope_rows": None,
            "source_lookup_rows": None,
            "target_lookup_rows": None,
            "lookup_batches": 0,
            "target_statements": 0,
            "safety_splits": 0,
            "fallback_batches": 0,
            "max_sql_bytes": 0,
            "sql_generation_seconds": 0.0,
            "source_query_seconds": 0.0,
            "target_execute_seconds": 0.0,
            "target_extract_seconds": 0.0,
            "hash_seconds": 0.0,
            "gate_wait_seconds": 0.0,
            "source_rows": 0,
            "target_rows": 0,
            "candidate_rows": 0,
            # Keyless cohort processing accumulates the number of candidate
            # rows that matched the driving-side hashes. This must exist for
            # every sample/full-hash fallback timing dictionary before the
            # first cohort result is processed.
            "lookup_matching_rows": 0,
            "peak_frame_rows": 0,
            "memory_before_mb": self._process_memory_mb(),
            # Batch-level parallel execution statistics. Every value below is
            # measured, never estimated: ``batch_elapsed_sum_seconds`` is the
            # sum of the measured per-batch durations (what strictly sequential
            # execution of the same batches would have added up to) and
            # ``parallel_batch_elapsed_seconds`` the wall time they took.
            "batch_count": 0,
            "parallel_workers": 1,
            "configured_parallelism": None,
            "parallelism_source": None,
            "parallelism_caps": [],
            "max_inflight_batches": 0,
            "parallel_batch_elapsed_seconds": 0.0,
            "batch_elapsed_sum_seconds": 0.0,
            "connection_wait_seconds": 0.0,
            "db_execution_seconds": 0.0,
            "adaptive_parallelism_events": [],
            "peak_memory_mb": self._process_memory_mb(),
            # Keyless discriminator widening (compact set-based fallback).
            "widened_batches": 0,
            "unbounded_batches": 0,
            "max_discriminator_columns": 0,
            "discriminator_tiers": [],
        }
        self._sample_timings = timings
        self._lookup_matching_counts = Counter()
        return timings

    def _log_sample_lookup_summary(self, timings: Dict[str, Any]) -> None:
        logger.info(
            "[SAMPLE_FETCH] table=%s keyed=%s sample_size=%s lookup_side=%s "
            "source_scope_rows=%s target_scope_rows=%s source_lookup_rows=%s "
            "target_lookup_rows=%s source_rows=%d "
            "target_rows=%d compare_columns=%d strategy=%s lookup_batch_size=%s "
            "lookup_batches=%d statements=%d max_sql_bytes=%d safety_splits=%d "
            "fallback_batches=%d | sql_generation=%.3fs source_query=%.3fs "
            "target_execute=%.3fs target_extract=%.3fs hash=%.3fs gate_wait=%.3fs "
            "| candidate_rows=%d peak_frame_rows=%d memory_before_mb=%s",
            timings.get("table_id"), timings["keyed"], timings["sample_size"],
            timings.get("lookup_side"), timings.get("source_scope_rows"),
            timings.get("target_scope_rows"), timings.get("source_lookup_rows"),
            timings.get("target_lookup_rows"),
            timings["source_rows"], timings["target_rows"], timings["compare_columns"],
            timings["lookup_strategy"], timings["lookup_batch_size"],
            timings["lookup_batches"], timings["target_statements"],
            timings["max_sql_bytes"], timings["safety_splits"], timings["fallback_batches"],
            timings["sql_generation_seconds"], timings["source_query_seconds"],
            timings["target_execute_seconds"], timings["target_extract_seconds"],
            timings["hash_seconds"], timings["gate_wait_seconds"],
            timings["candidate_rows"], timings["peak_frame_rows"],
            timings["memory_before_mb"],
        )
        logger.info(
            "[SAMPLE_PARALLEL] table=%s batch_count=%d parallel_workers=%d "
            "configured_parallelism=%s source=%s caps=%s max_inflight_batches=%d "
            "target_batch_wall_seconds=%.3f batch_elapsed_sum_seconds=%.3f "
            "connection_wait_seconds=%.3f db_execution_seconds=%.3f "
            "adaptive_events=%d peak_memory_mb=%s",
            timings.get("table_id"), timings.get("batch_count", 0),
            timings.get("parallel_workers", 1), timings.get("configured_parallelism"),
            timings.get("parallelism_source"),
            ",".join(timings.get("parallelism_caps") or []) or "none",
            timings.get("max_inflight_batches", 0),
            timings.get("parallel_batch_elapsed_seconds", 0.0),
            timings.get("batch_elapsed_sum_seconds", 0.0),
            timings.get("connection_wait_seconds", 0.0),
            timings.get("db_execution_seconds", 0.0),
            len(timings.get("adaptive_parallelism_events") or []),
            timings.get("peak_memory_mb"),
        )

    def _attach_sample_performance(self, result: CheckResult, compare_seconds: float) -> None:
        """Record the sampled comparison's timing breakdown on the result and log."""
        timings = getattr(self, "_sample_timings", None)
        if not isinstance(timings, dict):
            return
        timings["comparison_seconds"] = round(compare_seconds, 6)
        timings["memory_after_mb"] = self._process_memory_mb()
        self._note_peak_memory(timings)
        timings["total_seconds"] = round(time.perf_counter() - timings["_started"], 6)
        # Names requested by the performance evidence: the target batch wall
        # time and the total record comparison time under explicit keys.
        timings["target_batch_wall_seconds"] = timings.get(
            "parallel_batch_elapsed_seconds", 0.0
        )
        timings["source_fetch_seconds"] = timings.get("source_query_seconds", 0.0)
        timings["total_record_comparison_seconds"] = timings["total_seconds"]
        performance = {
            key: (round(value, 6) if isinstance(value, float) else value)
            for key, value in timings.items()
            if not key.startswith("_")
        }
        result.details["sampling_performance"] = performance
        logger.info(
            "[SAMPLE_PERF] table=%s total=%.3fs | source_query=%.3fs sql_generation=%.3fs "
            "target_execute=%.3fs target_extract=%.3fs hash=%.3fs comparison=%.3fs "
            "gate_wait=%.3fs | statements=%d max_sql_bytes=%d peak_frame_rows=%d "
            "memory_before_mb=%s memory_after_mb=%s peak_memory_mb=%s | "
            "batch_count=%d parallel_workers=%d max_inflight=%d "
            "target_batch_wall=%.3fs batch_elapsed_sum=%.3fs",
            performance.get("table_id"), performance["total_seconds"],
            performance["source_query_seconds"], performance["sql_generation_seconds"],
            performance["target_execute_seconds"], performance["target_extract_seconds"],
            performance["hash_seconds"], performance["comparison_seconds"],
            performance["gate_wait_seconds"], performance["target_statements"],
            performance["max_sql_bytes"], performance["peak_frame_rows"],
            performance["memory_before_mb"], performance["memory_after_mb"],
            performance.get("peak_memory_mb"),
            performance.get("batch_count", 0), performance.get("parallel_workers", 1),
            performance.get("max_inflight_batches", 0),
            performance.get("target_batch_wall_seconds", 0.0),
            performance.get("batch_elapsed_sum_seconds", 0.0),
        )

    @staticmethod
    def _statement_details(statements: Sequence[str]) -> Any:
        """The SQL echoed into the result details, bounded for many statements."""
        if not statements:
            return []
        if len(statements) == 1:
            return statements[0]
        shown: List[str] = [str(sql) for sql in statements[:STATEMENT_DETAIL_LIMIT]]
        if len(statements) > STATEMENT_DETAIL_LIMIT:
            shown.append(
                f"... {len(statements) - STATEMENT_DETAIL_LIMIT} more statement(s) "
                f"omitted ({len(statements)} total)"
            )
        return shown

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
        details: Dict[str, Any],
        excess_source_limit: Optional[int] = None,
        excess_target_limit: Optional[int] = None,
        full_evidence_policy: bool = False,
        mismatched_tokens: Optional[set] = None,
    ) -> CheckResult:
        """Compare two aligned cohorts by key.

        ``mismatched_tokens``, when a set is supplied, receives the key token of
        every aligned record with at least one differing column, so a caller
        that has to classify those records further reuses this engine's own
        verdict instead of comparing the same values a second time.
        """
        self._require_columns(src_df, source_keys, "source key")
        self._require_columns(tgt_df, target_keys, "target key")
        source = src_df.copy()
        target = tgt_df.copy()
        source["_merge_key"] = self._key_tokens(source, source_keys)
        target["_merge_key"] = self._key_tokens(target, target_keys)

        source_duplicate_count = int(source["_merge_key"].duplicated(keep=False).sum())
        target_duplicate_count = int(target["_merge_key"].duplicated(keep=False).sum())
        if source_duplicate_count or target_duplicate_count:
            duplicate_count = source_duplicate_count + target_duplicate_count
            details.update(
                records_checked=duplicate_count,
                matched_records=0,
                mismatch_records=duplicate_count,
                source_duplicate_records=source_duplicate_count,
                target_duplicate_records=target_duplicate_count,
            )
            return CheckResult(
                check_name="record_comparison",
                status=ValidationStatus.FAIL,
                source_value=len(source),
                target_value=len(target),
                message=(
                    "Record comparison requires unique key_mappings; "
                    f"source duplicate records={source_duplicate_count}, "
                    f"target duplicate records={target_duplicate_count}"
                ),
                details=details,
            )

        source_by_key = source.set_index("_merge_key", drop=False)
        target_by_key = target.set_index("_merge_key", drop=False)
        source_key_set = set(source_by_key.index)
        target_key_set = set(target_by_key.index)
        missing_target_keys = source_key_set - target_key_set
        missing_source_keys = target_key_set - source_key_set

        remaining = (
            excess_source_limit
            if full_evidence_policy
            else self._remaining_mismatch_limit(result_obj, mismatch_limit)
        )
        self._store_missing_records(
            result_obj,
            table_map.id,
            source_by_key,
            missing_target_keys,
            "MISSING_IN_TARGET",
            remaining,
        )
        remaining = (
            excess_target_limit
            if full_evidence_policy
            else self._remaining_mismatch_limit(result_obj, mismatch_limit)
        )
        self._store_missing_records(
            result_obj,
            table_map.id,
            target_by_key,
            missing_source_keys,
            "MISSING_IN_SOURCE",
            remaining,
        )

        result = self._compare_aligned_values(
            source,
            target,
            table_map,
            source_key_set & target_key_set,
            column_mappings,
            mismatch_limit,
            matched_limit,
            export_matched,
            result_obj,
            mismatched_tokens=mismatched_tokens,
        )
        matched_count, value_mismatch_count = result[0], result[1]
        vectorization_metrics = result[2] if len(result) > 2 else {}

        missing_source_count = len(missing_source_keys)
        missing_target_count = len(missing_target_keys)
        mismatch_count = (
            value_mismatch_count + missing_source_count + missing_target_count
        )
        records_checked = matched_count + mismatch_count
        status = ValidationStatus.PASS if mismatch_count == 0 else ValidationStatus.FAIL
        details.update(
            records_checked=records_checked,
            matched_records=matched_count,
            mismatch_records=mismatch_count,
            value_mismatch_records=value_mismatch_count,
            missing_in_source=missing_source_count,
            missing_in_target=missing_target_count,
            **vectorization_metrics,
        )
        return CheckResult(
            check_name="record_comparison",
            status=status,
            source_value=len(source),
            target_value=len(target),
            delta=abs(len(source) - len(target)),
            message=(
                f"Compared records={records_checked}, matched={matched_count}, "
                f"mismatched={mismatch_count}, missing_in_source={missing_source_count}, "
                f"missing_in_target={missing_target_count}"
            ),
            details=details,
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
        mismatched_tokens: Optional[set] = None,
    ) -> Tuple[int, int, Dict[str, Any]]:
        """Compare every mapped column of the aligned records column at a time.

        Both cohorts are aligned once with a single positional take, then each
        mapped column is compared as a whole array. Only records that actually
        differ, and only up to the configured export limit, are ever visited row
        by row in Python.

        ``mismatched_tokens``, when a set is supplied, receives the key token of
        every record with at least one differing column (all of them, not only
        the exported evidence), for callers that classify records further.

        Optimizations:
        - Uses ValueComparator for vectorized column comparison
        - Lazy evidence construction: matched records only built when export_matched=True
        - Vectorized mismatch detection across all columns at once
        """
        if not common_keys:
            return 0, 0, {}

        ordered_keys = sorted(common_keys)
        source_positions = self._key_positions(source)
        target_positions = self._key_positions(target)
        source_aligned = source.iloc[
            [source_positions[key] for key in ordered_keys]
        ].reset_index(drop=True)
        target_aligned = target.iloc[
            [target_positions[key] for key in ordered_keys]
        ].reset_index(drop=True)

        # Each side reports only its own row-hash column, when it has one.
        source_hash_column = self._resolve_row_hash_column(source_aligned)
        target_hash_column = self._resolve_row_hash_column(target_aligned)

        record_count = len(ordered_keys)

        vectorized_enabled = bool(
            self._policies.full_comparison.get("performance", {}).get(
                "vectorized_comparison", True
            )
        )
        comparator = ValueComparator.from_policies(
            self._policies, per_value=self._values_match
        )

        # Compute mismatch masks for all columns using vectorized operations
        mismatch_masks: List[np.ndarray] = []
        for mapping in column_mappings:
            source_values = source_aligned[mapping["source_column"]]
            target_values = target_aligned[mapping["target_column"]]
            if vectorized_enabled:
                mask = comparator.mismatch_mask(source_values, target_values)
            else:
                mask = self._column_mismatch_mask(source_values, target_values)
                comparator.scalar_columns += 1
                comparator.scalar_rows += record_count
            mismatch_masks.append(np.array(mask, dtype=bool))

        # Detect records with any column mismatch using vectorized operations
        combined_mismatch = np.zeros(record_count, dtype=bool)
        for mask in mismatch_masks:
            combined_mismatch |= mask

        value_mismatch_count = int(combined_mismatch.sum())
        matched_count = record_count - value_mismatch_count
        if mismatched_tokens is not None and value_mismatch_count:
            mismatched_tokens.update(
                ordered_keys[int(position)] for position in np.flatnonzero(combined_mismatch)
            )

        # Lazy evidence construction: only build matched records when needed
        if export_matched and matched_count > 0:
            matched_positions = np.flatnonzero(~combined_mismatch)
            for idx, position in enumerate(matched_positions):
                if len(result_obj.matched_records) >= matched_limit:
                    break
                position_int = int(position)
                result_obj.matched_records.append(
                    MatchedRecord(
                        table_id=table_map.id,
                        primary_key=self._display_key(ordered_keys[position_int]),
                        business_key="",
                        source_hash="",
                        target_hash="",
                        source_row_hash_value=self._row_hash_at(
                            source_aligned, source_hash_column, position_int
                        ),
                        target_row_hash_value=self._row_hash_at(
                            target_aligned, target_hash_column, position_int
                        ),
                    )
                )

        # Build mismatch evidence only for mismatched records
        mismatch_positions = np.flatnonzero(combined_mismatch)
        for position in mismatch_positions:
            if self._remaining_mismatch_limit(result_obj, mismatch_limit) <= 0:
                break
            position_int = int(position)
            key = ordered_keys[position_int]
            for mapping, mask in zip(column_mappings, mismatch_masks):
                if not mask[position_int]:
                    continue
                if self._remaining_mismatch_limit(result_obj, mismatch_limit) <= 0:
                    break
                source_value = source_aligned[mapping["source_column"]].iloc[position_int]
                target_value = target_aligned[mapping["target_column"]].iloc[position_int]
                result_obj.mismatched_records.append(
                    MismatchedRecord(
                        table_id=table_map.id,
                        primary_key=self._display_key(key),
                        column_name=mapping["name"],
                        source_value=source_value,
                        target_value=target_value,
                        difference_type=self._difference_type(
                            source_value, target_value
                        ),
                        source_row_hash_value=self._row_hash_at(
                            source_aligned, source_hash_column, position_int
                        ),
                        target_row_hash_value=self._row_hash_at(
                            target_aligned, target_hash_column, position_int
                        ),
                    )
                )

        vectorization_metrics = comparator.metrics()
        return matched_count, value_mismatch_count, vectorization_metrics

    @staticmethod
    def _key_positions(frame: pd.DataFrame) -> Dict[Tuple[str, ...], int]:
        """Map each unique key token to its positional row index."""
        return {
            key: position
            for position, key in enumerate(frame["_merge_key"].tolist())
        }

    def _column_mismatch_mask(
        self, source_values: pd.Series, target_values: pd.Series
    ) -> List[bool]:
        """Return one mismatch flag per aligned record for a single column.

        Equality is evaluated for the whole column at once. Columns holding
        values that cannot be compared as an array fall back to a per-value
        comparison, so semantics never depend on the dtype.
        """
        return [
            not self._values_match(source_value, target_value)
            for source_value, target_value in zip(
                source_values.tolist(), target_values.tolist()
            )
        ]

    def _null_mask(self, values: pd.Series) -> List[bool]:
        try:
            return [bool(item) for item in pd.isna(values).to_numpy()]
        except (TypeError, ValueError):
            return [self._is_null(value) for value in values.tolist()]

    def _compare_by_row_hash(
        self,
        src_df: pd.DataFrame,
        tgt_df: pd.DataFrame,
        table_map,
        column_mappings: List[Dict[str, str]],
        mismatch_limit: int,
        matched_limit: int,
        export_matched: bool,
        result_obj: TableValidationResult,
        details: Dict[str, Any],
    ) -> CheckResult:
        if not column_mappings:
            raise ValueError(
                "Keyless row comparison requires at least one compare_column_mapping"
            )
        source = src_df.copy()
        target = tgt_df.copy()
        source_columns = [item["source_column"] for item in column_mappings]
        target_columns = [item["target_column"] for item in column_mappings]
        # The sampled keyless cohort already hashed both sides with this very
        # function over these very columns; reuse those hashes when they line
        # up with the frames instead of hashing every cell a second time.
        cached = getattr(self, "_sample_hash_cache", None) or {}
        source_hashes = cached.get("source_hashes")
        target_hashes = cached.get("target_hashes")
        if not (
            cached.get("source_columns") == source_columns
            and isinstance(source_hashes, list) and len(source_hashes) == len(source)
        ):
            source_hashes = self._row_hashes(source, source_columns)
        if not (
            cached.get("target_columns") == target_columns
            and isinstance(target_hashes, list) and len(target_hashes) == len(target)
        ):
            target_hashes = self._row_hashes(target, target_columns)
        source["_row_hash"] = list(source_hashes)
        target["_row_hash"] = list(target_hashes)
        source_counts = Counter(source["_row_hash"])
        target_counts = Counter(target["_row_hash"])
        missing_target = source_counts - target_counts
        missing_source = target_counts - source_counts
        matched_count = sum((source_counts & target_counts).values())

        remaining = self._remaining_mismatch_limit(result_obj, mismatch_limit)
        self._store_hash_missing_records(
            result_obj,
            table_map.id,
            source,
            missing_target,
            "MISSING_IN_TARGET",
            remaining,
        )
        remaining = self._remaining_mismatch_limit(result_obj, mismatch_limit)
        self._store_hash_missing_records(
            result_obj,
            table_map.id,
            target,
            missing_source,
            "MISSING_IN_SOURCE",
            remaining,
        )

        if export_matched:
            # Each side is looked up in its own frame, so a matched record
            # reports the source table's stored hash and the target table's
            # stored hash independently of one another.
            source_stored = self._row_hash_lookup(
                source, self._resolve_row_hash_column(source)
            )
            target_stored = self._row_hash_lookup(
                target, self._resolve_row_hash_column(target)
            )
            for row_hash, count in (source_counts & target_counts).items():
                available = matched_limit - len(result_obj.matched_records)
                for _ in range(min(count, max(available, 0))):
                    result_obj.matched_records.append(
                        MatchedRecord(
                            table_id=table_map.id,
                            primary_key=f"row_hash:{row_hash}",
                            business_key="",
                            source_hash=row_hash,
                            target_hash=row_hash,
                            source_row_hash_value=source_stored.get(row_hash),
                            target_row_hash_value=target_stored.get(row_hash),
                        )
                    )
                if len(result_obj.matched_records) >= matched_limit:
                    break

        missing_target_count = sum(missing_target.values())
        missing_source_count = sum(missing_source.values())
        mismatch_count = missing_target_count + missing_source_count
        records_checked = matched_count + mismatch_count
        details.update(
            hash_algorithm=self._policies.hashing["algorithm"],
            hash_comparison_scope=self._policies.hashing["comparison_scope"],
            hash_duplicate_handling=self._policies.hashing["duplicate_handling"],
            hash_batch_size=self._policies.hashing.get("batch_size"),
            comparison_strategy="hash_comparison",
            records_checked=records_checked,
            matched_records=matched_count,
            mismatch_records=mismatch_count,
            missing_in_source=missing_source_count,
            missing_in_target=missing_target_count,
        )
        return CheckResult(
            check_name="record_comparison",
            status=(ValidationStatus.PASS if mismatch_count == 0 else ValidationStatus.FAIL),
            source_value=len(source),
            target_value=len(target),
            delta=abs(len(source) - len(target)),
            message=(
                f"Row-hash comparison checked={records_checked}, matched={matched_count}, "
                f"missing_in_source={missing_source_count}, "
                f"missing_in_target={missing_target_count}"
            ),
            details=details,
        )

    @staticmethod
    def _key_mappings(table_map) -> List[Dict[str, str]]:
        resolved = []
        seen = set()
        for index, mapping in enumerate(
            getattr(table_map, "key_mappings", None) or []
        ):
            source = (
                mapping.get("source_column") if isinstance(mapping, dict)
                else getattr(mapping, "source_column")
            )
            target = (
                mapping.get("target_column") if isinstance(mapping, dict)
                else getattr(mapping, "target_column")
            )
            if not all(
                isinstance(column, str) and column.strip()
                for column in (source, target)
            ):
                raise ValueError(
                    f"key_mappings[{index}] requires non-empty source and target columns"
                )
            pair = (source.strip(), target.strip())
            if pair in seen:
                raise ValueError(f"duplicate key mapping {pair}")
            seen.add(pair)
            resolved.append({
                "source_column": pair[0], "target_column": pair[1],
            })
        return resolved

    @staticmethod
    def _configured_column_mappings(table_map) -> List[Dict[str, str]]:
        configured = getattr(table_map, "compare_column_mappings", None) or []
        mappings: List[Dict[str, str]] = []
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
                    f"compare_column_mappings[{index}] requires non-empty "
                    "source_column and target_column"
                )
            pair = (source_column.strip(), target_column.strip())
            if pair in seen:
                raise ValueError(f"Duplicate compare column mapping {pair}")
            seen.add(pair)
            mappings.append(
                {
                    "source_column": pair[0],
                    "target_column": pair[1],
                    "name": item.get("name", f"{pair[0]}_to_{pair[1]}"),
                }
            )
        return mappings

    # ── Configured-column metadata precheck ───────────────────────────
    def _configured_column_precheck(
        self,
        source_conn,
        target_conn,
        table_map,
        src_fqn: str,
        tgt_fqn: str,
        source_dialect,
        target_dialect,
        configured_key_mappings: List[Dict[str, str]],
        base_details: Dict[str, Any],
        started: float,
        extra_columns: Optional[Dict[str, List[str]]] = None,
    ) -> Optional[CheckResult]:
        """Validate configured columns against the actual table metadata.

        Runs before any record-comparison statement is generated. Every column
        named in the table's ``key_mappings`` and ``compare_column_mappings``
        must exist on its own side, as must the configured incremental columns
        (``extra_columns``: CDC operation column, watermark and target audit
        columns, soft-delete marker). A missing column produces a FAIL result
        with exception category ``CONFIGURED_COLUMN_NOT_FOUND``; the comparison
        is not executed, nothing is removed, substituted or retried, and the
        configured mapping is echoed unchanged. A side whose metadata cannot
        be described blocks comparison as an unavailable metadata dependency;
        it is not evidence that a configured column is missing.
        """
        try:
            configured_columns = self._configured_column_mappings(table_map)
        except (AttributeError, TypeError, ValueError):
            # A malformed compare mapping is reported by the existing path.
            configured_columns = []
        extra_columns = extra_columns or {}
        expected = {
            "source": {
                "key_mappings": [
                    item["source_column"] for item in configured_key_mappings
                ],
                "compare_column_mappings": [
                    item["source_column"] for item in configured_columns
                ],
                "incremental_columns": list(extra_columns.get("source") or []),
            },
            "target": {
                "key_mappings": [
                    item["target_column"] for item in configured_key_mappings
                ],
                "compare_column_mappings": [
                    item["target_column"] for item in configured_columns
                ],
                "incremental_columns": list(extra_columns.get("target") or []),
            },
        }
        if not any(names for side in expected.values() for names in side.values()):
            return None
        table_id = getattr(table_map, "id", None)
        sides = {
            "source": (
                source_conn,
                getattr(table_map, "source_schema", None),
                getattr(table_map, "source_table", None),
                src_fqn,
                source_dialect,
            ),
            "target": (
                target_conn,
                getattr(table_map, "target_schema", None),
                getattr(table_map, "target_table", None),
                tgt_fqn,
                target_dialect,
            ),
        }
        missing: Dict[str, Dict[str, List[str]]] = {"source": {}, "target": {}}
        metadata_source: Dict[str, str] = {}
        inconclusive: Dict[str, str] = {}
        for role, (connection, schema, table, fqn, dialect) in sides.items():
            actual, origin = self._actual_columns(
                connection, schema, table, fqn, dialect
            )
            if actual is None:
                inconclusive[role] = origin
                continue
            metadata_source[role] = origin
            for mapping_name, names in expected[role].items():
                absent = [name for name in names if name.casefold() not in actual]
                if absent:
                    missing[role][mapping_name] = absent

        if not any(missing.values()):
            outcome = {
                "status": "INCONCLUSIVE" if inconclusive else "PASSED",
                "metadata_source": metadata_source,
            }
            if inconclusive:
                outcome["inconclusive"] = inconclusive
                logger.warning(
                    "Configured-column precheck inconclusive | table=%s | %s | "
                    "record comparison not executed",
                    table_id,
                    "; ".join(f"{role}: {why}" for role, why in inconclusive.items()),
                )
            base_details["configured_column_precheck"] = outcome
            if inconclusive:
                return self._error(
                    "Record comparison not executed: actual column metadata "
                    "unavailable | " + "; ".join(
                        f"{role}: {why}" for role, why in inconclusive.items()
                    ),
                    started,
                    base_details,
                    exception_category="METADATA_UNAVAILABLE",
                    table_id=table_id,
                    execution_status="NOT_EXECUTED",
                    record_comparison_executed=False,
                    execution_strategy="configured_column_precheck",
                )
            return None

        parts = [
            f"{role} {mapping_name}: {', '.join(names)}"
            for role in ("source", "target")
            for mapping_name, names in missing[role].items()
        ]
        message = (
            "Record comparison not executed: configured columns not found in "
            "actual metadata | " + " | ".join(parts)
        )
        logger.error(
            "Record comparison not executed | table=%s | category=%s | %s | "
            "metadata_source=%s",
            table_id, CONFIGURED_COLUMN_NOT_FOUND, " | ".join(parts), metadata_source,
        )
        return self._error(
            message,
            started,
            base_details,
            status=ValidationStatus.FAIL,
            exception_category=CONFIGURED_COLUMN_NOT_FOUND,
            table_id=table_id,
            framework_error=False,
            execution_status="NOT_EXECUTED",
            record_comparison_executed=False,
            execution_strategy="configured_column_precheck",
            missing_source_key_columns=missing["source"].get("key_mappings", []),
            missing_target_key_columns=missing["target"].get("key_mappings", []),
            missing_source_compare_columns=missing["source"].get(
                "compare_column_mappings", []
            ),
            missing_target_compare_columns=missing["target"].get(
                "compare_column_mappings", []
            ),
            configured_key_mappings=deepcopy(
                getattr(table_map, "key_mappings", None) or []
            ),
            configured_compare_column_mappings=deepcopy(
                getattr(table_map, "compare_column_mappings", None) or []
            ),
            metadata_source=metadata_source,
            configured_column_precheck={
                "status": "FAILED",
                "metadata_source": metadata_source,
                **({"inconclusive": inconclusive} if inconclusive else {}),
            },
        )

    @staticmethod
    def _actual_columns(
        connection, schema, table, fqn: str, dialect
    ) -> Tuple[Optional[Dict[str, str]], str]:
        """One side's actual column names, casefolded to their stored spelling.

        The connector's metadata operation (``get_table_columns``, the existing
        connector abstraction) is authoritative when it names columns. Metadata
        naming no column is inconclusive (catalog lookups are case-sensitive on
        some engines, privileges may be insufficient, a connector may not
        implement it) and never proof of absence, so the zero-row probe this
        validator already uses for the row-hash column is consulted instead.
        Returns ``(None, reason)`` when neither can describe the table.
        """
        reasons: List[str] = []
        describe = getattr(connection, "get_table_columns", None)
        if callable(describe) and schema and table:
            try:
                names = RecordComparisonValidator._column_names(describe(schema, table))
                if names:
                    return {name.casefold(): name for name in names}, "connector_metadata"
                reasons.append("connector metadata named no columns")
            except Exception as exc:
                reasons.append(
                    f"connector metadata unavailable ({type(exc).__name__})"
                )
        safe_fqn = str(fqn or "").strip()
        if not safe_fqn:
            reasons.append("table name unavailable for the schema probe")
            return None, "; ".join(reasons)
        try:
            if getattr(dialect, "row_limit_style", None) == "top":
                probe_sql = f"SELECT {dialect.row_limit_keyword} 0 * FROM {safe_fqn}"
            else:
                probe_sql = dialect.select("*", safe_fqn, " WHERE 1 = 0")
            names = RecordComparisonValidator._column_names(
                connection.execute_query(probe_sql)
            )
            if names:
                return {name.casefold(): name for name in names}, "schema_probe"
            reasons.append("schema probe named no columns")
        except Exception as exc:
            reasons.append(f"schema probe unavailable ({type(exc).__name__})")
        return None, "; ".join(reasons)

    @staticmethod
    def _column_names(described: Any) -> List[str]:
        """Column names from a metadata mapping, a frame, or a plain sequence."""
        if described is None:
            return []
        columns = getattr(described, "columns", None)
        if columns is not None and not isinstance(described, dict):
            return [str(column) for column in columns]
        if isinstance(described, dict):
            return [str(name) for name in described.keys()]
        return [str(name) for name in described]

    def _resolve_column_mappings(
        self,
        configured: List[Dict[str, str]],
        source: pd.DataFrame,
        target: pd.DataFrame,
        source_keys: List[str],
        target_keys: List[str],
    ) -> List[Dict[str, str]]:
        if configured:
            missing_source = [
                item["source_column"]
                for item in configured
                if item["source_column"] not in source.columns
            ]
            missing_target = [
                item["target_column"]
                for item in configured
                if item["target_column"] not in target.columns
            ]
            if missing_source or missing_target:
                raise ValueError(
                    "Configured comparison columns are missing; "
                    f"source={missing_source}, target={missing_target}"
                )
            return configured

        common = sorted(
            set(source.columns).intersection(target.columns)
            - set(source_keys)
            - set(target_keys)
        )
        return [
            {"source_column": column, "target_column": column, "name": column}
            for column in common
        ]

    @staticmethod
    def _projection(columns: Sequence[str], dialect=None) -> str:
        dialect = dialect or resolve_dialect(None)
        unique = list(dict.fromkeys(column for column in columns if column))
        return ", ".join(dialect.quote(column) for column in unique) if unique else "*"

    @staticmethod
    def _select_sql(
        fqn: str,
        projection: str,
        where_clause: str,
        top: Optional[int] = None,
        order_by: Optional[str] = None,
        dialect=None,
    ) -> str:
        dialect = dialect or resolve_dialect(None)
        return dialect.select(
            projection,
            fqn,
            where_clause,
            order_by=order_by,
            limit=top,
        )

    @staticmethod
    def _duplicate_record_count_sql(
        fqn: str,
        where_clause: str,
        key_columns: Sequence[str],
        dialect=None,
    ) -> str:
        if not key_columns:
            raise ValueError("Duplicate-key prerequisite requires table key_mappings")
        dialect = dialect or resolve_dialect(None)
        key_list = ", ".join(dialect.quote(column) for column in key_columns)
        return (
            "SELECT COALESCE(SUM(duplicate_group_size), 0) AS duplicate_records "
            "FROM ("
            "SELECT COUNT(*) AS duplicate_group_size "
            f"FROM {fqn}{where_clause} "
            f"GROUP BY {key_list} HAVING COUNT(*) > 1"
            ") AS duplicate_key_groups"
        )

    def _duplicate_record_count(
        self,
        connection,
        sql: str,
        side: str,
    ) -> int:
        frame = connection.execute_query(sql)
        if not isinstance(frame, pd.DataFrame):
            raise TypeError("Connector execute_query must return a pandas DataFrame")
        if frame.empty:
            return 0
        columns = {str(column).lower(): column for column in frame.columns}
        column = columns.get("duplicate_records")
        if column is None:
            if len(frame.columns) != 1:
                raise ValueError(
                    f"{side} duplicate-key prerequisite did not return duplicate_records"
                )
            column = frame.columns[0]
        value = frame.iloc[0][column]
        if self._is_null(value):
            return 0
        if isinstance(value, bool):
            raise ValueError(
                f"{side} duplicate-key prerequisite returned an invalid count"
            )
        try:
            count = int(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                f"{side} duplicate-key prerequisite returned an invalid count"
            ) from exc
        if count < 0 or value != count:
            raise ValueError(
                f"{side} duplicate-key prerequisite returned an invalid count"
            )
        return count

    def _keyset_select_sql(
        self,
        fqn: str,
        projection: str,
        where_clause: str,
        order_columns: Sequence[str],
        batch_size: int,
        last_values: Optional[Sequence[Any]] = None,
        dialect=None,
    ) -> str:
        if not order_columns:
            raise ValueError("Keyset full comparison requires table key_mappings")
        effective_where = where_clause
        if last_values is not None:
            effective_where = self._combine_where(
                where_clause,
                self._keyset_predicate(order_columns, last_values, dialect),
            )
        return self._select_sql(
            fqn,
            projection,
            effective_where,
            top=batch_size,
            order_by=", ".join(
                (dialect or resolve_dialect(None)).quote(column)
                for column in order_columns
            ),
            dialect=dialect,
        )

    def _keyset_predicate(
        self,
        columns: Sequence[str],
        values: Sequence[Any],
        dialect=None,
    ) -> str:
        if len(columns) != len(values):
            raise ValueError("Keyset columns and values must have the same length")
        dialect = dialect or resolve_dialect(None)
        alternatives = []
        for index, (column, value) in enumerate(zip(columns, values)):
            column = dialect.quote(column)
            prefix = []
            for prefix_column, prefix_value in zip(
                columns[:index], values[:index]
            ):
                prefix_column = dialect.quote(prefix_column)
                if is_null_scalar(prefix_value, blank_is_null=False):
                    prefix.append(f"{prefix_column} IS NULL")
                else:
                    prefix.append(
                        f"{prefix_column} = {self._sql_literal(prefix_value)}"
                    )
            greater = (
                f"{column} IS NOT NULL"
                if is_null_scalar(value, blank_is_null=False)
                else f"{column} > {self._sql_literal(value)}"
            )
            alternatives.append("(" + " AND ".join(prefix + [greater]) + ")")
        return "(" + " OR ".join(alternatives) + ")"

    def _build_filter_queries(
        self,
        fqn: str,
        projection: str,
        where_clause: str,
        columns: Sequence[str],
        values: Sequence[Tuple[Any, ...]],
        dialect=None,
    ) -> List[str]:
        predicates = list(
            dict.fromkeys(
                self._row_predicate(columns, row_values, dialect)
                for row_values in values
            )
        )
        return [
            self._select_sql(
                fqn,
                projection,
                self._combine_where(
                    where_clause,
                    "(" + " OR ".join(batch) + ")",
                ),
                dialect=dialect,
            )
            for batch in self._batches(predicates, self._predicate_batch_size())
        ]

    def _predicate_batch_size(self, requested: Optional[int] = None) -> int:
        """Rows per per-row predicate statement, always within the safety ceiling.

        ``_filter_batch_size`` is what a caller paging a keyed table asked for;
        it is capped by ``comparison_settings.query_safety.max_predicates_per_query``
        so no caller can turn a page size into an unbounded OR list.
        """
        lookup = self._sample_lookup_settings()
        batch = requested if requested is not None else getattr(self, "_filter_batch_size", None)
        if not self._is_positive_int(batch):
            batch = lookup["keyed_batch_size"]
        return max(1, min(int(batch), lookup["max_predicates_per_query"]))

    def _build_signature_cohort_queries(
        self,
        fqn: str,
        projection: str,
        where_clause: str,
        columns: Sequence[str],
        values: Sequence[Tuple[Any, ...]],
        dialect=None,
        batch_size: Optional[int] = None,
    ) -> List[str]:
        """Fetch at most the sampled multiplicity for each keyless signature.

        Rows per statement come from ``batch_size`` (capped by the query safety
        ceiling), never from the sample size. This is the exact, last-resort
        form used only for a batch whose discriminator candidates were too many;
        see :meth:`_keyless_sample_cohort`.
        """
        if not columns:
            raise ValueError("Signature cohort sampling requires mapped columns")
        predicate_counts = Counter(
            self._row_predicate(columns, row_values, dialect) for row_values in values
        )
        rank_column = "__mv_signature_rank"
        queries = []
        for batch in self._batches(
            list(predicate_counts.items()), self._predicate_batch_size(batch_size)
        ):
            cohort_predicate = "(" + " OR ".join(
                predicate for predicate, _ in batch
            ) + ")"
            ranked_source = (
                f"SELECT {projection}, ROW_NUMBER() OVER ("
                f"PARTITION BY {', '.join((dialect or resolve_dialect(None)).quote(column) for column in columns)} ORDER BY (SELECT NULL)"
                f") AS {rank_column} FROM {fqn}"
                f"{self._combine_where(where_clause, cohort_predicate)}"
            )
            bounded_predicate = " OR ".join(
                f"({predicate} AND {rank_column} <= {count})"
                for predicate, count in batch
            )
            # bind_sql keeps the parameters a bound scope filter carries; a bare
            # f-string would silently drop them.
            queries.append(bind_sql(
                f"SELECT {projection} FROM ({ranked_source}) AS sampled_target "
                f"WHERE {bounded_predicate}",
                ranked_source,
            ))
        return queries

    def _row_predicate(
        self, columns: Sequence[str], values: Sequence[Any], dialect=None
    ) -> str:
        dialect = dialect or resolve_dialect(None)
        conditions = []
        for column, value in zip(columns, values):
            column = dialect.quote(column)
            if self._is_null(value):
                conditions.append(f"{column} IS NULL")
            else:
                conditions.append(f"{column} = {self._sql_literal(value)}")
        return "(" + " AND ".join(conditions) + ")"

    @staticmethod
    def _combine_where(where_clause: str, predicate: str) -> str:
        if not predicate:
            return where_clause
        if where_clause.strip():
            return bind_sql(
                f"{where_clause} AND ({predicate})", where_clause, predicate
            )
        return bind_sql(f" WHERE {predicate}", predicate)

    def _full_scan_row_budget(self) -> int:
        """Total rows the keyless full-scan comparison may hold per side.

        Read from ``large_table_processing.failure_fallback.sample_record_limit``,
        the same key the resource-pressure fallback uses, so one setting
        governs how many rows a degraded comparison examines.
        """
        config = self._large_table_processing
        if not isinstance(config, dict):
            return DEFAULT_FULL_SCAN_ROW_BUDGET
        limit = (
            config.get("failure_fallback", {}).get("sample_record_limit")
            if isinstance(config.get("failure_fallback"), dict)
            else None
        )
        try:
            budget = int(limit)
        except (TypeError, ValueError):
            return DEFAULT_FULL_SCAN_ROW_BUDGET
        return budget if budget > 0 else DEFAULT_FULL_SCAN_ROW_BUDGET

    def _read_within_row_budget(
        self,
        connection,
        sql: str,
        batch_size: int,
        row_budget: int,
        role: str,
    ) -> Tuple[pd.DataFrame, int, bool]:
        """Read ``sql`` in pages, stopping once ``row_budget`` rows are held.

        Returns the frame, the number of rows read, and whether the read was
        cut short by the budget. Truncation is returned rather than logged and
        forgotten: the caller records it, because a comparison over a
        truncated read must not be reported as a complete one.
        """
        reader = StreamingBatchReader(connection, sql, batch_size=batch_size)
        pages: List[pd.DataFrame] = []
        rows = 0
        truncated = False
        for page in reader.read_batches():
            pages.append(page)
            rows += len(page)
            if rows > row_budget:
                truncated = True
                logger.warning(
                    "%s read stopped at %d rows by the row budget (%d); the "
                    "comparison covers only these rows",
                    role,
                    rows,
                    row_budget,
                )
                break
        if not pages:
            return pd.DataFrame(), 0, False
        frame = (
            pages[0] if len(pages) == 1
            else pd.concat(pages, ignore_index=True)
        )
        pages.clear()
        return frame, rows, truncated

    def _iter_query_frames(
        self, connection, queries: Sequence[str], *, with_retry: bool = False
    ) -> Iterator[pd.DataFrame]:
        """Yield one result frame per query, holding a single frame at a time.

        This is the memory-bounded primitive. Callers that only need a
        reduction over the results (a key set, a count) should consume this
        rather than ``_execute_queries``/``_execute_batch_queries``, so peak
        memory stays bounded by the largest single query result instead of by
        the combined result of every query.
        """
        for query in queries:
            if with_retry:
                yield self._execute_batch_query(connection, query)
            else:
                yield connection.execute_query(query)

    def _collect_query_frames(
        self, connection, queries: Sequence[str], *, with_retry: bool = False
    ) -> pd.DataFrame:
        """Materialise every query result as one concatenated frame.

        A single ``pd.concat`` over all frames is deliberate: it allocates the
        combined frame exactly once. Concatenating incrementally
        (``acc = concat([acc, batch])`` per batch) copies the whole accumulator
        on every step, which is O(n^2) in time and lowers peak memory not at
        all -- the returned frame necessarily holds every row either way.
        Callers that cannot afford that must use ``_iter_query_frames``.
        """
        frames = list(self._iter_query_frames(connection, queries, with_retry=with_retry))
        if not frames:
            return pd.DataFrame()
        if len(frames) == 1:
            return frames[0]
        return pd.concat(frames, ignore_index=True)

    def _collect_key_tokens(
        self,
        connection,
        queries: Sequence[str],
        columns: Sequence[str],
        purpose: Optional[str] = None,
        *,
        with_retry: bool = False,
    ) -> Set[Tuple[str, ...]]:
        """Union of key tokens across queries, without a combined frame.

        Peak memory is bounded by one query result plus the token set, rather
        than by every result row. Empty frames are skipped: they contribute no
        tokens, and a connector may return them without column metadata.
        """
        tokens: Set[Tuple[str, ...]] = set()
        for frame in self._iter_query_frames(connection, queries, with_retry=with_retry):
            if frame.empty:
                continue
            if purpose is not None:
                self._require_columns(frame, columns, purpose)
            tokens.update(self._key_tokens(frame, columns))
        return tokens

    def _execute_queries(self, connection, queries: List[str]) -> pd.DataFrame:
        """Execute multiple queries and return the concatenated result."""
        return self._collect_query_frames(connection, queries)

    #: Guards ``_batch_retry_count`` when lookup batches run on worker threads.
    _RETRY_COUNT_LOCK = threading.Lock()

    def _execute_batch_query(self, connection, sql: str) -> pd.DataFrame:
        """Run one statement under the configured batch retry policy.

        Only transient connection failures are retried. A statement timeout is
        deterministic for an unchanged statement and a MemoryError only gets
        worse on repetition, so neither is re-issued here; the connector layer
        applies its own configured policy underneath.
        """
        config = self._batch_config
        if config is None:
            attempts = 1
        else:
            retry_failed = (
                config.get("retry_failed_batch", True)
                if isinstance(config, dict) else getattr(config, "retry_failed_batch", True)
            )
            max_retries = (
                config.get("max_retries", 3)
                if isinstance(config, dict) else getattr(config, "max_retries", 3)
            )
            attempts = 1 + (max_retries if retry_failed else 0)
        for attempt in range(1, attempts + 1):
            try:
                return connection.execute_query(sql)
            except Exception as exc:
                if attempt >= attempts or not self._batch_retry_allowed(exc):
                    raise
                with self._RETRY_COUNT_LOCK:
                    self._batch_retry_count += 1
                logger.warning(
                    "Lookup statement failed; retrying | table=%s | attempt=%d/%d | "
                    "sql_bytes=%d | error_type=%s | error_class=transient_connection | "
                    "error=%s",
                    getattr(self, "_current_table_id", None), attempt + 1, attempts,
                    len(str(sql)), type(exc).__name__, sanitize_error_text(exc),
                )

    @staticmethod
    def _batch_retry_allowed(exc: Exception) -> bool:
        """Whether re-issuing the identical statement can reasonably succeed.

        Only a transient transport/availability failure qualifies. A statement
        timeout, a MemoryError, a framework error and any other deterministic
        database error would fail identically on repetition; the connector
        layer has already applied its own configured retry policy underneath.
        """
        if isinstance(exc, MemoryError) or _is_query_timeout_error(exc):
            return False
        return _is_transient_connection_error(exc)

    def _execute_batch_queries(self, connection, queries: List[str]) -> pd.DataFrame:
        """Execute multiple queries with retry logic, concatenating the results."""
        return self._collect_query_frames(connection, queries, with_retry=True)

    @staticmethod
    def _extract_values(
        frame: pd.DataFrame, columns: Sequence[str]
    ) -> List[Tuple[Any, ...]]:
        if frame.empty:
            return []
        RecordComparisonValidator._require_columns(frame, columns, "sampling")
        return list(frame[list(columns)].itertuples(index=False, name=None))

    def _trim_target_to_source_signature_counts(
        self,
        source: pd.DataFrame,
        target: pd.DataFrame,
        column_mappings: Sequence[Dict[str, str]],
    ) -> pd.DataFrame:
        source_columns = [item["source_column"] for item in column_mappings]
        target_columns = [item["target_column"] for item in column_mappings]
        self._require_columns(source, source_columns, "source signature")
        self._require_columns(target, target_columns, "target signature")
        source_counts = Counter(self._row_hashes(source, source_columns))
        retained_counts: Counter = Counter()
        retained_positions = []
        for position, row_hash in enumerate(self._row_hashes(target, target_columns)):
            if retained_counts[row_hash] >= source_counts[row_hash]:
                continue
            retained_counts[row_hash] += 1
            retained_positions.append(position)
        return target.iloc[retained_positions].reset_index(drop=True)

    @staticmethod
    def _require_columns(
        frame: pd.DataFrame, columns: Iterable[str], purpose: str
    ) -> None:
        missing = [column for column in columns if column not in frame.columns]
        if missing:
            raise ValueError(f"Missing {purpose} columns: {missing}")

    def _sql_literal(self, value: Any) -> str:
        if is_null_scalar(value, blank_is_null=False):
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, numbers.Number) and not isinstance(value, complex):
            return str(value)
        token = temporal_token(
            value,
            parse_strings=(
                self._policies.timestamps["parse_iso_strings"]
                if hasattr(self, "_policies") else True
            ),
        )
        text = token.canonical if token is not None else str(value)
        literal = "'" + text.replace("'", "''") + "'"
        # A non-ASCII value must travel as a national-character literal, or the
        # server re-encodes it in its default code page and the lookup misses.
        return literal if text.isascii() else "N" + literal

    @staticmethod
    def _coerce_lookup_keys_to_source_types(
        connection,
        schema: Optional[str],
        table: Optional[str],
        key_columns: Sequence[str],
        key_values: Sequence[Tuple[Any, ...]],
    ) -> List[Tuple[Any, ...]]:
        """Convert target-derived lookup literals to source key datatypes.

        This keeps the source column unwrapped and index-seekable while avoiding
        an empty cohort solely because migration changed a key's physical type.
        """
        describe = getattr(connection, "get_table_columns", None)
        if not callable(describe) or not schema or not table or not key_values:
            return list(key_values)
        try:
            metadata = describe(schema, table)
        except Exception as exc:
            logger.warning(
                "Source key datatype metadata unavailable; using native lookup "
                "values | schema=%s table=%s error_type=%s",
                schema, table, type(exc).__name__,
            )
            return list(key_values)
        if not isinstance(metadata, dict):
            return list(key_values)
        folded = {
            str(name).casefold(): str(kind).casefold()
            for name, kind in metadata.items()
        }
        types = [folded.get(str(column).casefold()) for column in key_columns]
        integer_types = {"bigint", "int", "integer", "smallint", "tinyint"}
        decimal_types = {"decimal", "numeric", "money", "smallmoney"}
        float_types = {"float", "real", "double", "double precision"}
        text_types = {
            "char", "nchar", "varchar", "nvarchar", "text", "ntext",
            "string", "uniqueidentifier", "uuid",
        }

        def coerce(value: Any, sql_type: Optional[str]) -> Any:
            if is_null_scalar(value, blank_is_null=False) or not sql_type:
                return value
            try:
                if sql_type in integer_types:
                    return int(Decimal(str(value).strip()))
                if sql_type in decimal_types:
                    return Decimal(str(value).strip())
                if sql_type in float_types:
                    return float(value)
                if sql_type in text_types:
                    return str(value)
            except (InvalidOperation, TypeError, ValueError, OverflowError):
                # Never invent or discard a key when metadata conversion fails.
                return value
            return value

        return [
            tuple(
                coerce(value, types[index] if index < len(types) else None)
                for index, value in enumerate(row)
            )
            for row in key_values
        ]

    def _key_tokens(
        self, frame: pd.DataFrame, columns: Sequence[str]
    ) -> List[Tuple[str, ...]]:
        """Build key tokens efficiently by vectorizing canonical value conversion."""
        if frame.empty:
            return []

        # Convert each column to canonical values once
        canonical_cols = {}
        for col in columns:
            canonical_cols[col] = frame[col].apply(self._canonical_value).tolist()

        # Zip into tuples efficiently
        return [
            tuple(canonical_cols[col][i] for col in columns)
            for i in range(len(frame))
        ]

    def _count_rows(self, connection, fqn: str, where_clause: str, dialect: Any) -> int:
        sql = self._count_sql(fqn, where_clause, dialect)
        frame = connection.execute_query(sql)
        if frame.empty:
            return 0
        return int(frame.iloc[0, 0])

    def _row_hashes(
        self, frame: pd.DataFrame, columns: Sequence[str]
    ) -> List[str]:
        return self._hash_frame(frame[list(columns)], columns)

    def _hash_frame(
        self, frame: pd.DataFrame, columns: Sequence[str]
    ) -> List[str]:
        """Build canonical hash inputs column-at-a-time, then hash each row once.

        Canonicalization remains the existing authoritative implementation. The
        optimization removes the per-cell digest updates and the nested Python
        row/column loop without changing the length-prefixed byte stream.
        """
        if frame.empty:
            return []
        # A fetched batch remains bounded by the connector configuration, but
        # canonicalization temporarily expands every value into Python bytes.
        # Process smaller hash-only windows and retain only their fixed-size
        # digests.  On genuine allocation pressure, halve the failing window
        # without changing row coverage, order, or hash semantics.
        hashes: List[str] = []
        for start in range(0, len(frame), HASH_CANONICAL_CHUNK_ROWS):
            chunk = frame.iloc[start:start + HASH_CANONICAL_CHUNK_ROWS]
            hashes.extend(self._hash_frame_chunk_adaptive(chunk, columns))
        return hashes

    def _hash_frame_chunk_adaptive(
        self, frame: pd.DataFrame, columns: Sequence[str]
    ) -> List[str]:
        """Hash one canonical window, splitting it after allocation failure."""
        try:
            return self._hash_frame_chunk(frame, columns)
        except MemoryError:
            if len(frame) <= 1:
                raise
            midpoint = len(frame) // 2
            gc.collect()
            return (
                self._hash_frame_chunk_adaptive(frame.iloc[:midpoint], columns)
                + self._hash_frame_chunk_adaptive(frame.iloc[midpoint:], columns)
            )

    def _hash_frame_chunk(
        self, frame: pd.DataFrame, columns: Sequence[str]
    ) -> List[str]:
        """Hash a small frame using the authoritative canonical byte format."""
        encoded_columns = []
        canonicalize = self._hash_canonical_value
        for column in columns:
            # The canonical token of a value depends only on its exact type and
            # value for str / int / bool / bytes, so within one column each
            # distinct such value is canonicalized and encoded once. Floats,
            # decimals, temporals and everything else go through the function
            # unchanged. The byte stream is identical to the per-cell form.
            memo: Dict[Tuple[type, Any], bytes] = {}
            encoded_column = []
            for value in frame[column].tolist():
                value_type = type(value)
                if value_type in _HASH_MEMO_TYPES:
                    key = (value_type, value)
                    part = memo.get(key)
                    if part is None:
                        encoded = canonicalize(value).encode("utf-8")
                        part = len(encoded).to_bytes(8, byteorder="big") + encoded
                        memo[key] = part
                else:
                    encoded = canonicalize(value).encode("utf-8")
                    part = len(encoded).to_bytes(8, byteorder="big") + encoded
                encoded_column.append(part)
            encoded_columns.append(encoded_column)
        algorithm = self._policies.hashing["algorithm"]
        return [
            hashlib.new(algorithm, b"".join(parts)).hexdigest()
            for parts in zip(*encoded_columns)
        ]

    def _column_precisions(
        self, frame: pd.DataFrame, columns: Sequence[str]
    ) -> Dict[str, List[int]]:
        result: Dict[str, List[int]] = {}
        for column in columns:
            precisions = {
                token.precision
                for token in (
                    temporal_token(
                        value,
                        parse_strings=self._policies.timestamps["parse_iso_strings"],
                    )
                    for value in frame[column].tolist()
                )
                if token is not None
            }
            result[column] = sorted(precisions)
        return result

    def _batch_serialize_rows(
        self, rows: List[Tuple[str, dict]], batch_size: int = 1000
    ) -> List[Tuple[str, str]]:
        """Batch serialize multiple row payloads to JSON.

        Reduces per-row JSON overhead by using minimal intermediate representations.
        Returns list of (row_hash, json_payload) tuples.
        """
        results = []
        for row_hash, payload_dict in rows:
            try:
                payload = json.dumps(
                    payload_dict,
                    default=str,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                results.append((row_hash, payload))
            except Exception:
                # Fallback: return minimal JSON if serialization fails
                results.append((row_hash, "{}"))
        return results

    @staticmethod
    def _fallback_spill_priority(hash_spill_config: Any, current_engine: Optional[str]) -> Tuple[str, ...]:
        priority = tuple(
            getattr(hash_spill_config, "priority", None) or ("memory", "sqlite", "duckdb")
        )
        if current_engine is None:
            return priority
        current = str(current_engine)
        try:
            index = priority.index(current)
        except ValueError:
            return tuple()
        return priority[index + 1 :]

    @staticmethod
    def _large_table_sampling_config(
        raw_config: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        path = "comparison_settings.large_table_sampling"
        if not isinstance(raw_config, dict):
            raise ValueError(f"{path} must be configured as a YAML mapping")
        enabled = raw_config.get("enabled")
        if not isinstance(enabled, bool):
            raise ValueError(f"{path}.enabled must be boolean")
        for field_name in ("row_count_threshold", "sample_size"):
            value = raw_config.get(field_name)
            if not RecordComparisonValidator._is_positive_int(value):
                raise ValueError(f"{path}.{field_name} must be a positive integer")
        return dict(raw_config)

    @staticmethod
    def _large_table_sampling_applies(
        source_count: Optional[int], target_count: Optional[int], config: Dict[str, Any]
    ) -> bool:
        measured = [count for count in (source_count, target_count) if count is not None]
        return bool(
            config.get("enabled", False)
            and measured
            and max(int(count) for count in measured)
            >= int(config["row_count_threshold"])
        )

    @staticmethod
    def _cached_row_counts(execution_cache) -> Tuple[int, int]:
        if execution_cache is None or not callable(
            getattr(execution_cache, "peek", None)
        ):
            raise ValueError(
                "Cached row counts are required for large-table sampling; "
                "run row_count or key_validation before comparison"
            )
        counts = []
        peek_failure = getattr(execution_cache, "peek_failure", None)
        for role in ("source", "target"):
            value = execution_cache.peek(("row_count", role))
            if not (
                isinstance(value, numbers.Integral)
                and not isinstance(value, bool)
                and value >= 0
            ):
                failure = peek_failure(("row_count", role)) if callable(peek_failure) else None
                if failure is not None:
                    # The count was attempted once this table run and failed;
                    # it is not attempted again here (a timeout is not a miss).
                    raise ValueError(
                        f"Cached {role} row count is unavailable for large-table "
                        f"sampling: {failure}"
                    )
                raise ValueError(
                    f"Cached {role} row count is unavailable for large-table "
                    "sampling; run row_count or key_validation before comparison"
                )
            counts.append(int(value))
        return counts[0], counts[1]

    @staticmethod
    def _large_table_fallback_config(raw_config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        defaults = {
            "enabled": False,
            "full_comparison_max_rows": 1000000,
            "fallback_mode": "sample_comparison",
            "sample_record_limit": 900000,
        }
        if not isinstance(raw_config, dict):
            return defaults
        config = {**defaults, **raw_config}
        try:
            config["full_comparison_max_rows"] = int(
                config.get("full_comparison_max_rows", defaults["full_comparison_max_rows"]) or defaults["full_comparison_max_rows"]
            )
        except (TypeError, ValueError):
            config["full_comparison_max_rows"] = defaults["full_comparison_max_rows"]
        try:
            config["sample_record_limit"] = int(
                config.get("sample_record_limit", defaults["sample_record_limit"]) or defaults["sample_record_limit"]
            )
        except (TypeError, ValueError):
            config["sample_record_limit"] = defaults["sample_record_limit"]
        return config

    @staticmethod
    def _large_table_processing_config(raw_config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        defaults = {
            "enabled": False,
            "medium_table_threshold": 500000,
            "large_table_threshold": 2000000,
            "batch_size": {"small": 5000, "medium": 2000, "large": 1000},
            "large_table": {"force_disk_spill": True, "parallel_ingest": False},
            "memory_error_retry": {"enabled": True, "retry_batch_sizes": [2500, 1000, 500, 200]},
            "failure_fallback": {
                "enabled": True,
                "mode": "sample_comparison",
                "sample_record_limit": 1000000,
                "trigger_on": ["MemoryError", "resource_exhaustion"],
                "use_existing_sample_logic": True,
            },
        }
        if not isinstance(raw_config, dict):
            return defaults
        config = {**defaults, **raw_config}
        batch_size = {**defaults["batch_size"], **(raw_config.get("batch_size") or {})}
        config["batch_size"] = batch_size
        if isinstance(raw_config.get("large_table"), dict):
            config["large_table"] = {**defaults["large_table"], **raw_config["large_table"]}
        if isinstance(raw_config.get("memory_error_retry"), dict):
            config["memory_error_retry"] = {
                **defaults["memory_error_retry"],
                **raw_config["memory_error_retry"],
            }
        if isinstance(raw_config.get("failure_fallback"), dict):
            config["failure_fallback"] = {
                **defaults["failure_fallback"],
                **raw_config["failure_fallback"],
            }
        return config

    def _get_configured_batch_size(self) -> int:
        """Get batch size from performance config, with intelligent fallback."""
        # Try to get from performance config's batch_processing section
        execution_config = getattr(self, "_execution_config", None)
        if execution_config is not None:
            batch_processing = getattr(execution_config, "batch_processing", None)
            if batch_processing is not None:
                batch_size = getattr(batch_processing, "batch_size", None)
                if self._is_positive_int(batch_size):
                    return int(batch_size)
        # Fallback to reasonable default (50K for modern hardware)
        return 50000

    def _log_connection_statistics(self, connection, role: str) -> None:
        """Log connection pool reuse statistics if available."""
        pool = getattr(connection, "connection_pool", None)
        if pool is None:
            return
        pool_size = getattr(pool, "pool", None)
        if pool_size is None:
            return
        checked_in = len(pool_size) if hasattr(pool_size, '__len__') else "unknown"
        overflow = getattr(pool, "overflow", "unknown")
        checkedout_count = getattr(pool, "checkedout", "unknown")
        logger.debug(
            "Connection pool reuse | role=%s | pool_size=%s | overflow=%s | "
            "checked_out=%s",
            role, checked_in, overflow, checkedout_count
        )

    def _large_table_batch_size(self, row_count: Optional[int], large_table_config: Dict[str, Any]) -> int:
        if not large_table_config.get("enabled", False):
            return 10000
        medium = int(large_table_config.get("medium_table_threshold", 500000) or 500000)
        large = int(large_table_config.get("large_table_threshold", 2000000) or 2000000)
        if row_count is None:
            return int(large_table_config["batch_size"].get("large", 2500))
        if row_count <= medium:
            return int(large_table_config["batch_size"].get("small", 10000))
        if row_count <= large:
            return int(large_table_config["batch_size"].get("medium", 5000))
        return int(large_table_config["batch_size"].get("large", 2500))

    def _hash_batch_size(
        self,
        row_count: int,
        hash_config: Dict[str, Any],
        large_table_config: Dict[str, Any],
    ) -> int:
        adaptive = hash_config.get("adaptive_batching") or {}
        if adaptive.get("enabled", False) and row_count > 0:
            for tier in adaptive.get("tiers") or []:
                if not isinstance(tier, dict):
                    continue
                maximum = tier.get("max_rows")
                batch_size = tier.get("batch_size")
                if self._is_positive_int(maximum) and self._is_positive_int(
                    batch_size
                ) and row_count <= int(maximum):
                    return min(int(batch_size), HASH_CANONICAL_CHUNK_ROWS)
            configured_large = adaptive.get("large_table_batch_size")
            if self._is_positive_int(configured_large):
                return min(
                    int(configured_large), HASH_CANONICAL_CHUNK_ROWS
                )
        configured = hash_config.get("batch_size")
        if self._is_positive_int(configured):
            return min(int(configured), HASH_CANONICAL_CHUNK_ROWS)
        return min(
            self._large_table_batch_size(row_count or None, large_table_config),
            HASH_CANONICAL_CHUNK_ROWS,
        )

    @staticmethod
    def _parallel_ingest_safe(connection: Any) -> bool:
        """Honor an explicit connector safety declaration when one is present."""
        for name in (
            "thread_safe", "is_thread_safe", "supports_parallel_reads",
            "parallel_ingest_safe",
        ):
            value = getattr(connection, name, None)
            if callable(value):
                try:
                    value = value()
                except Exception:
                    return False
            if value is False:
                return False
        return True

    def _hash_values(self, values: Sequence[Any]) -> str:
        parts = []
        for value in values:
            encoded = self._hash_canonical_value(value).encode("utf-8")
            parts.append(len(encoded).to_bytes(8, byteorder="big"))
            parts.append(encoded)
        return hashlib.new(
            self._policies.hashing["algorithm"], b"".join(parts)
        ).hexdigest()

    def _canonical_value(self, value: Any) -> str:
        if self._is_null(value):
            return "null:<NULL>"
        if isinstance(value, bool):
            return f"bool:{str(value).lower()}"
        token = temporal_token(
            value,
            parse_strings=(
                self._policies.timestamps["parse_iso_strings"]
                if hasattr(self, "_policies") else True
            ),
        )
        if token is not None:
            return f"datetime:{token.canonical}"
        if isinstance(value, Decimal):
            return f"number:{normalize_decimal_for_hash(value)}"
        if isinstance(value, numbers.Integral):
            return f"number:{int(value)}"
        if isinstance(value, numbers.Real):
            return f"number:{normalize_decimal_for_hash(Decimal(str(value)))}"
        if isinstance(value, bytes):
            return f"bytes:{value.hex()}"
        return f"string:{value}"

    def _hash_canonical_value(self, value: Any) -> str:
        normalization = self._policies.hashing["normalization"]
        if (
            isinstance(value, str) and not value.strip()
            and normalization["blank_handling"] == "null"
        ):
            return normalization["null_representation"]
        if self._is_null(value):
            return normalization["null_representation"]
        token = temporal_token(
            value,
            parse_strings=self._policies.timestamps["parse_iso_strings"],
        )
        if token is not None:
            fraction = token.fraction
            if normalization["precision_normalization"] == "trim_trailing_zeros":
                fraction = fraction.rstrip("0")
            timezone = (
                token.timezone
                if normalization["timezone_normalization"] == "preserve" else ""
            )
            fractional = f".{fraction}" if fraction else ""
            if token.kind == "date":
                canonical = token.date_component
            elif token.kind == "time":
                canonical = f"{token.time_component}{fractional}{timezone}"
            else:
                canonical = (
                    f"{token.date_component}T{token.time_component}"
                    f"{fractional}{timezone}"
                )
            return (
                f"datetime:{canonical}"
                if normalization["normalize_datatypes"]
                else canonical
            )
        text = str(value)
        if normalization["trim_strings"]:
            text = text.strip()
        if not normalization["case_sensitive"]:
            text = text.casefold()
        if normalization["unicode_normalization"] != "none":
            text = unicodedata.normalize(
                normalization["unicode_normalization"], text
            )
        if normalization["normalize_datatypes"]:
            if (
                isinstance(value, str)
                and normalization["numeric_normalization"] == "canonical_decimal"
            ):
                try:
                    decimal = Decimal(text)
                except (InvalidOperation, ValueError):
                    pass
                else:
                    return f"number:{normalize_decimal_for_hash(decimal)}"
            if isinstance(value, bool):
                return f"bool:{text.lower()}"
            if isinstance(value, numbers.Number) and not isinstance(value, complex):
                if normalization["numeric_normalization"] == "canonical_decimal":
                    decimal = Decimal(text)
                    return f"number:{normalize_decimal_for_hash(decimal)}"
                return f"number:{text}"
            return f"string:{text}"
        return text

    def _is_null(self, value: Any) -> bool:
        return is_null_scalar(
            value,
            blank_is_null=(
                self._policies.nulls["blank_is_null"]
                if hasattr(self, "_policies") else False
            ),
        )

    def _values_match(self, source_value: Any, target_value: Any) -> bool:
        source_null = self._is_null(source_value)
        target_null = self._is_null(target_value)
        if source_null and target_null:
            return self._policies.nulls["both_null"] == "match"
        if source_null or target_null:
            return self._policies.nulls["one_null"] == "match"
        source_token = temporal_token(
            source_value,
            parse_strings=self._policies.timestamps["parse_iso_strings"],
        )
        target_token = temporal_token(
            target_value,
            parse_strings=self._policies.timestamps["parse_iso_strings"],
        )
        if source_token is not None or target_token is not None:
            return bool(
                source_token is not None
                and target_token is not None
                and temporal_values_match(
                    source_token,
                    target_token,
                    preserve_precision=self._policies.timestamps["preserve_precision"],
                    timezone_comparison=self._policies.timestamps["timezone_comparison"],
                    precision_loss_behavior=self._policies.timestamps[
                        "precision_loss_behavior"
                    ],
                    fractional_second_tolerance=self._policies.timestamps[
                        "fractional_second_tolerance"
                    ],
                )
            )
        try:
            return bool(source_value == target_value)
        except (TypeError, ValueError):
            return self._canonical_value(source_value) == self._canonical_value(target_value)

    def _difference_type(self, source_value: Any, target_value: Any) -> str:
        if self._is_null(source_value) or self._is_null(target_value):
            return "NULL_MISMATCH"
        if str(source_value).strip() == str(target_value).strip():
            return "WHITESPACE_DIFFERENCE"
        if str(source_value).lower() == str(target_value).lower():
            return "CASE_DIFFERENCE"
        return "VALUE_MISMATCH"

    # ─────────────────────────────────────────────────────────────────────
    # Row-hash evidence
    #
    # The configured row-hash column is read from whichever table the record
    # actually came from, and only when that table exposes it. It is never
    # computed, never rebuilt, and never carried across sides: a source
    # record can only ever report a source value and a target record can only
    # ever report a target value.
    # ─────────────────────────────────────────────────────────────────────

    @property
    def _row_hash_column(self) -> str:
        configured = (
            self._policies.full_comparison.get("evidence", {}) or {}
        ).get("row_hash_column")
        return str(configured).strip() if configured else ""

    def _resolve_row_hash_column(self, frame: Any) -> Optional[str]:
        """This frame's own row-hash column, or None when it has none."""
        configured = self._row_hash_column
        if not configured or not isinstance(frame, pd.DataFrame):
            return None
        if configured in frame.columns:
            return configured
        folded = configured.casefold()
        return next(
            (
                str(column) for column in frame.columns
                if str(column).casefold() == folded
            ),
            None,
        )

    def _resolve_row_hash_column_for_extraction(
        self, connection: Any, fqn: str, sql: str
    ) -> Optional[str]:
        """Return the physical _RowHashValue column name when it exists.

        _RowHashValue is evidence-only:
        - source has it / target does not -> target value remains None/blank
        - target has it / source does not -> source value remains None/blank
        - neither side has it -> both remain None/blank

        Missing _RowHashValue must never fail record comparison and must never
        change matched/mismatched/missing/excess counts.

        A TOP 0 '*' metadata probe is used so absence of the audit column does
        not intentionally generate a database error or trigger retry logic.
        """
        configured = self._row_hash_column
        if not configured:
            return None

        safe_fqn = str(fqn or "").strip()
        if not safe_fqn:
            logger.debug(
                "_RowHashValue probe skipped because table FQN is empty"
            )
            return None

        try:
            # TOP 0 returns the table's column metadata without fetching data.
            probe_sql = f"SELECT TOP 0 * FROM {safe_fqn}"
            probe_frame = connection.execute_query(probe_sql)

            if probe_frame is None or not hasattr(probe_frame, "columns"):
                return None

            configured_folded = str(configured).casefold()
            for column in probe_frame.columns:
                if str(column).casefold() == configured_folded:
                    return str(column)

            # Column does not exist on this side. This is valid and is represented
            # as None so reporting writes a blank audit value.
            return None

        except Exception as exc:
            # Audit evidence must not block the actual record comparison.
            # Keep this at DEBUG because table/query failures are handled by the
            # main validation path; this helper simply treats audit evidence as absent.
            logger.debug(
                "_RowHashValue evidence probe unavailable for %s: %s",
                safe_fqn,
                exc,
            )
            return None

    def _row_hash_payload_key(self, columns: Iterable[str]) -> Optional[str]:
        """The row-hash key inside a projected payload, when it was projected."""
        configured = self._row_hash_column
        if not configured:
            return None
        folded = configured.casefold()
        return next(
            (
                str(column) for column in columns
                if str(column).casefold() == folded
            ),
            None,
        )

    @staticmethod
    def _row_hash_value(value: Any) -> Optional[str]:
        if value is None:
            return None
        try:
            if bool(pd.isna(value)):
                return None
        except (TypeError, ValueError):
            pass
        text = str(value)
        return text if text != "" else None

    @classmethod
    def _row_hash_at(
        cls, frame: pd.DataFrame, column: Optional[str], position: int
    ) -> Optional[str]:
        if column is None:
            return None
        return cls._row_hash_value(frame[column].iloc[position])

    @classmethod
    def _row_hash_from_row(cls, row: Any, column: Optional[str]) -> Optional[str]:
        if column is None:
            return None
        try:
            return cls._row_hash_value(row[column])
        except (KeyError, IndexError, TypeError):
            return None

    @classmethod
    def _row_hash_from_payload(
        cls, payload: Any, key: Optional[str]
    ) -> Optional[str]:
        if key is None or not payload:
            return None
        try:
            record = json.loads(payload)
        except (TypeError, ValueError):
            return None
        if not isinstance(record, dict):
            return None
        return cls._row_hash_value(record.get(key))

    @classmethod
    def _row_hash_lookup(
        cls, frame: pd.DataFrame, column: Optional[str]
    ) -> Dict[str, Optional[str]]:
        """Map each computed row hash to that table's own stored row hash."""
        if column is None:
            return {}
        lookup: Dict[str, Optional[str]] = {}
        for computed, stored in zip(frame["_row_hash"], frame[column]):
            if computed not in lookup:
                lookup[computed] = cls._row_hash_value(stored)
        return lookup

    def _store_missing_records(
        self,
        result_obj: TableValidationResult,
        table_id: str,
        indexed: pd.DataFrame,
        missing_keys: Iterable[Tuple[str, ...]],
        location: str,
        limit: Optional[int],
    ) -> None:
        target_list = (
            result_obj.missing_in_target
            if location == "MISSING_IN_TARGET"
            else result_obj.missing_in_source
        )
        remaining = None if limit is None else max(limit - len(target_list), 0)
        if remaining == 0:
            return
        ordered_keys = sorted(missing_keys)
        if remaining is not None:
            ordered_keys = ordered_keys[:remaining]
        # `indexed` is the one table these records exist in, so its own
        # row-hash column is the only source for their row hash.
        hash_column = self._resolve_row_hash_column(indexed)
        for key in ordered_keys:
            row = self._indexed_row(indexed, key)
            target_list.append(
                MissingRecord(
                    table_id=table_id,
                    primary_key=self._display_key(key),
                    record_data=row.drop(labels=["_merge_key"], errors="ignore").to_json(
                        date_format="iso"
                    ),
                    location=location,
                    row_hash_value=self._row_hash_from_row(row, hash_column),
                )
            )

    def _configured_excess_limit(self, side: str) -> Optional[int]:
        evidence = self._policies.full_comparison["evidence"]
        if evidence[
            "export_all_excess_source"
            if side == "source" else "export_all_excess_target"
        ]:
            return None
        return evidence[
            "excess_source_limit" if side == "source" else "excess_target_limit"
        ]

    def _store_hash_missing_records(
        self,
        result_obj: TableValidationResult,
        table_id: str,
        frame: pd.DataFrame,
        missing_hashes: Counter,
        location: str,
        limit: int,
    ) -> None:
        target_list = (
            result_obj.missing_in_target
            if location == "MISSING_IN_TARGET"
            else result_obj.missing_in_source
        )
        if limit <= 0 or not missing_hashes:
            return
        # Only rows whose hash is actually missing are visited.
        frame = frame[frame["_row_hash"].isin(set(missing_hashes))]
        # `frame` is the one table these records exist in.
        hash_column = self._resolve_row_hash_column(frame)
        stored = Counter()
        for _, row in frame.iterrows():
            row_hash = row["_row_hash"]
            if stored[row_hash] >= missing_hashes[row_hash]:
                continue
            target_list.append(
                MissingRecord(
                    table_id=table_id,
                    primary_key=f"row_hash:{row_hash}",
                    record_data=row.drop(labels=["_row_hash"]).to_json(
                        date_format="iso"
                    ),
                    location=location,
                    row_hash_value=self._row_hash_from_row(row, hash_column),
                )
            )
            stored[row_hash] += 1
            if sum(stored.values()) >= limit:
                break

    @staticmethod
    def _remaining_mismatch_limit(
        result_obj: TableValidationResult, limit: int
    ) -> int:
        used = (
            len(result_obj.mismatched_records)
            + len(result_obj.missing_in_source)
            + len(result_obj.missing_in_target)
        )
        return max(limit - used, 0)

    @staticmethod
    def _display_key(key: Tuple[str, ...]) -> str:
        return json.dumps(list(key), ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _indexed_row(frame: pd.DataFrame, key: Tuple[str, ...]) -> pd.Series:
        return frame.loc[[key]].iloc[0]

    @staticmethod
    def _batches(values: Sequence[Any], size: int):
        for index in range(0, len(values), size):
            yield values[index:index + size]

    @staticmethod
    def _is_positive_int(value: Any) -> bool:
        return isinstance(value, int) and not isinstance(value, bool) and value > 0

    @staticmethod
    def _is_nonnegative_int(value: Any) -> bool:
        return isinstance(value, int) and not isinstance(value, bool) and value >= 0

    # ── Incremental operation classification ──────────────────
    def _incremental_row_counts(self, cache, source_conn, target_conn,
                                src_fqn, tgt_fqn, source_where, target_where,
                                source_dialect, target_dialect):
        """Collect scope statistics without enabling row_count validation."""
        policy = getattr(self, "_incremental_policy", None)
        if policy is not None and policy.target_driven:
            # The target audit window is the authoritative cohort. Counting an
            # unfiltered source here both scans history unnecessarily and can
            # incorrectly force sample mode. With unique configured keys, its
            # corresponding source cohort cannot contain more logical records
            # than the target-owned cohort; duplicate-key safety is checked by
            # the selected comparison engine.
            sql = self._select_sql(
                tgt_fqn, target_dialect.count_all(), target_where,
                dialect=target_dialect,
            )
            target_count = self._row_count_once(
                cache, "target",
                lambda: int(target_conn.execute_query(sql).iloc[0, 0]),
                consumer="record_comparison",
            )
            # No source count has been measured yet. The real source cohort is
            # known only after the existing PK/signature lookup runs; copying
            # the target count here made the report claim a measurement that
            # never happened.
            return None, target_count
        counts = []
        for role, connection, fqn, where, dialect in (
            ("source", source_conn, src_fqn, source_where, source_dialect),
            ("target", target_conn, tgt_fqn, target_where, target_dialect),
        ):
            sql = self._select_sql(fqn, dialect.count_all(), where, dialect=dialect)
            counts.append(self._row_count_once(
                cache, role, lambda: int(connection.execute_query(sql).iloc[0, 0]),
                consumer="record_comparison",
            ))
        return tuple(counts)

    def _count_incremental_cdc(self, connection, fqn, where, dialect):
        """Aggregate event evidence without extracting change-level rows."""
        policy = self._incremental_policy
        if policy.target_driven:
            raise ValidationPolicyError("CDC source event scope is unavailable for target-only loads")
        cdc = policy.cdc
        column = dialect.quote(cdc["operation_column"])
        expressions, recognized = [], []
        operations = list(cdc.get("operation_values") or {})
        for operation in operations:
            values = ", ".join(self._filter_literal(value) for value in cdc["operation_values"][operation])
            predicate = f"{column} IN ({values})"
            recognized.append(predicate)
            expressions.append(f"COALESCE(SUM(CASE WHEN {predicate} THEN 1 ELSE 0 END), 0)")
        expressions.append(
            "COALESCE(SUM(CASE WHEN " + " OR ".join(recognized) + " THEN 0 ELSE 1 END), 0)"
        )
        frame = connection.execute_query(self._select_sql(fqn, ", ".join(expressions), where, dialect=dialect))
        return dict(zip(operations + ["unknown"], map(int, frame.iloc[0].tolist())))

    def _incremental_projection_columns(self) -> Dict[str, List[str]]:
        """Configured, non-compared columns the incremental classification reads.

        Source: the CDC operation column (and sequence column) and the source
        watermark column. Target: the target watermark column, the target
        audit/load column and the CDC soft-delete marker. All come from the
        resolved policy, i.e. from configuration; none is compared or hashed.
        """
        policy = getattr(self, "_incremental_policy", None)
        if policy is None or not policy.enabled:
            return {"source": [], "target": []}
        # CDC columns are source-event metadata. Target-audit comparisons are
        # driven by target rows and prove operations from target history, so a
        # source CDC projection would be unscoped and directionally invalid.
        cdc = {} if policy.target_driven else (policy.cdc or {})
        source = [
            name for name in (
                cdc.get("operation_column"), cdc.get("sequence_column"),
                policy.source_watermark_column,
            ) if name
        ]
        target = [
            name for name in (
                policy.target_watermark_column, policy.target_audit_column,
                cdc.get("soft_delete_column") if cdc.get("delete_behavior") == "soft_delete" else None,
            ) if name
        ]
        return {
            "source": list(dict.fromkeys(source)),
            "target": list(dict.fromkeys(target)),
        }

    def _start_incremental_classifier(
        self, kind: str, table_map, target_conn, tgt_fqn, target_dialect,
        source_keys, target_keys, configured_columns, result_obj, batch_size,
    ) -> None:
        """Attach the operation classifier the selected engine will feed."""
        self._incremental_classifier = None
        policy = getattr(self, "_incremental_policy", None)
        if policy is None:
            return
        if not policy.enabled:
            self._incremental_not_performed_reason = policy.unavailable_reason
            return
        historical_where = self._historical_target_where(table_map, target_dialect)
        cdc = policy.cdc or {}
        if kind == "keyed":
            self._incremental_classifier = OperationClassifier(
                self, policy, table_map, target_conn, tgt_fqn, target_dialect,
                historical_where, source_keys, target_keys, configured_columns,
                result_obj, batch_size,
                source_cdc_column=cdc.get("operation_column"),
                target_soft_delete_column=(
                    cdc.get("soft_delete_column")
                    if cdc.get("delete_behavior") == "soft_delete" else None
                ),
            )
            return
        self._incremental_classifier = HashOperationClassifier(
            self, policy, table_map, target_conn, tgt_fqn, target_dialect,
            historical_where, configured_columns, result_obj, batch_size,
        )

    @staticmethod
    def _source_fqn_for_table(table_map, dialect) -> str:
        """Build the configured source object name for an alternate host."""
        return _fqn(table_map.source_schema, table_map.source_table, dialect)

    def _finalize_incremental_result(self, check: CheckResult, table_map) -> CheckResult:
        """Combine the comparison result with the classified operations, once.

        Operation classification, the source/target data comparison and the
        configured operation policy stay three separate facts in the details;
        the final status is the worst of the data status and every violated
        policy status. A comparison that produced ERROR / CONFIG_ERROR / SKIP
        is returned unchanged.
        """
        policy = getattr(self, "_incremental_policy", None)
        classifier = getattr(self, "_incremental_classifier", None)
        if policy is None or check is None or check.check_name != "record_comparison":
            return check
        details = dict(check.details or {})
        if classifier is None:
            sample_mode = (
                str(details.get("comparison_mode") or "").startswith("sample")
                or details.get("execution_mode") == "SAMPLE_COMPARISON"
            )
            reason = (
                getattr(self, "_incremental_not_performed_reason", None)
                or policy.unavailable_reason
                or (
                    "sampled comparison: operations are classified only by the "
                    "full incremental comparison"
                    if sample_mode
                    else "the selected comparison engine does not classify operations"
                )
            )
            details.update(not_performed_details(policy, reason))
            if sample_mode:
                # Sampling validates the already-resolved incremental window.
                # Per-record operation classification is intentionally absent,
                # not failed, so a successful table may advance its boundary.
                details.update(
                    classification_status="skipped",
                    classification_reason="sample_mode",
                )
            counts = getattr(self, "_incremental_cdc_counts", None)
            if counts:
                statuses = [policy.status_for(operation) for operation, count in counts.items() if count]
                check.status = combine_statuses(check.status, statuses)
                details["incremental_operations"].update(counts)
                details["cdc_operation_policy_violations"] = {
                    policy_violation_category(operation, "cdc"): count for operation, count in counts.items()
                    if count and policy.status_for(operation) != "pass"
                }
                details["operation_policy_violations"] = dict(details["cdc_operation_policy_violations"])
                if details["cdc_operation_policy_violations"]:
                    check.message += " | CDC operation policy violations in the resolved source window"
                details["final_status"] = check.status.value
                details["incremental_failure_samples"] = [
                    "CDC operation counts cover the source window; change-level comparison was not performed."
                ]
            check.details = details
            return check
        summary = classifier.summary()
        details.update(summary)
        validated_updates = int(summary.get("validated_update_matches", 0) or 0)
        server_2_matches = int(summary.get("server_2_matches", 0) or 0)
        # Keep the bounded, Excel-only evidence reachable from the completed
        # check as well as the table result.  Some orchestration/report paths
        # retain the CheckResult while rebuilding the surrounding table
        # summary; dynamic attributes on that outer object are then absent.
        # A private CheckResult attribute survives that path without adding
        # the evidence feed to dataclass/JSON serialization.
        server_2_evidence = list(getattr(
            getattr(classifier, "_server_2_evidence", None), "rows", ()
        ))
        if not server_2_evidence:
            evidence_result = getattr(
                getattr(classifier, "_server_2_evidence", None), "_result", None
            )
            server_2_evidence = list(getattr(
                evidence_result, "server_2_match_evidence",
                getattr(evidence_result, "_server_2_match_evidence", ())
            ))
        if server_2_evidence:
            check._server_2_match_evidence = server_2_evidence
            # Store the bounded payload in the finalized comparison details as
            # the durable reporting handoff. Unlike a dynamic attribute on the
            # outer table object, details survive every report-tree summary
            # projection used by the orchestrator.
            details["server_2_match_evidence"] = server_2_evidence
        details["server_2_evidence_exported"] = len(server_2_evidence)
        if server_2_matches:
            details["fallback_used"] = "server_2"
        resolved_target_only = validated_updates + server_2_matches
        if resolved_target_only and isinstance(classifier, OperationClassifier):
            # Historical UPDATE validation applies only to target-only rows
            # (missing_in_source / excess_in_target). Value mismatches remain
            # ordinary comparison failures and never enter history lookup.
            prior_missing_source = int(details.get("missing_in_source", 0) or 0)
            resolved = min(resolved_target_only, prior_missing_source)
            details["missing_in_source"] = prior_missing_source - resolved
            details["excess_in_target"] = details["missing_in_source"]
            details["mismatch_records"] = max(
                0, int(details.get("mismatch_records", 0) or 0) - resolved
            )
            details["matched_records"] = int(
                details.get("matched_records", 0) or 0
            ) + resolved
            count_names = self._policies.full_comparison["count_names"]
            details[count_names["matched"]] = details["matched_records"]
            details[count_names["mismatched"]] = details["mismatch_records"]
            details[count_names["excess_in_target"]] = details["missing_in_source"]
            keys = set(getattr(classifier, "validated_update_keys", set()))
            keys.update(getattr(classifier, "server_2_matched_keys", set()))
            if keys:
                table_result = getattr(classifier, "_evidence", None)
                result_holder = getattr(table_result, "_result", None)
                if result_holder is not None:
                    result_holder.missing_in_source[:] = [
                        row for row in result_holder.missing_in_source
                        if row.primary_key not in keys
                    ]
            if details["mismatch_records"] == 0 and check.status == ValidationStatus.FAIL:
                check.status = ValidationStatus.PASS
        violations = summary.get("operation_policy_violations") or {}
        statuses = summary.get("operation_policy_statuses") or {}
        data_failures = summary.get("incremental_data_failures") or {}
        final = combine_statuses(check.status, list(statuses.values()))
        if isinstance(classifier, HashOperationClassifier):
            counts = getattr(self, "_incremental_cdc_counts", None) or {}
            final = combine_statuses(final, [policy.status_for(op) for op, count in counts.items() if count])
            details["cdc_operation_policy_violations"] = {
                policy_violation_category(op, "cdc"): count for op, count in counts.items()
                if count and policy.status_for(op) != "pass"
            }
            for operation, count in counts.items():
                if count and policy.status_for(operation) != "pass":
                    category = policy_violation_category(operation, "cdc")
                    violations[category] = max(violations.get(category, 0), count)
                    statuses[category] = policy.status_for(operation)
            details["operation_policy_violations"] = violations
            details["operation_policy_statuses"] = statuses
        if any(int(count or 0) > 0 for count in data_failures.values()):
            # Incremental data failures are independent of operation policy.
            # A policy-allowed INSERT is still invalid when its current target
            # record is absent from the source comparison scope.
            final = combine_statuses(final, ["fail"])
        categories = sorted(set(violations) | set(data_failures))
        if details.get("cdc_operation_policy_violations"):
            # A CDC-detected rejection is reported under both names: the
            # operation it rejected and the CDC attribution.
            categories = sorted(set(categories) | {"CDC_OPERATION_NOT_ALLOWED"})
        if final != check.status:
            details["data_comparison_status"] = check.status.value
        details["final_status"] = final.value
        details["failure_categories"] = categories
        operations = summary.get("incremental_operations") or {}
        operation_text = ", ".join(
            f"{name}={count}" for name, count in operations.items() if count
        ) or "none"
        message = f"{check.message} | incremental operations: {operation_text}"
        if violations:
            message += " | operation policy violations: " + ", ".join(
                f"{category}={count}" for category, count in sorted(violations.items())
            )
            reasons = summary.get("incremental_failure_samples") or []
            details["failure_reason"] = " ".join([
                "; ".join(
                    f"{category}: {count} record(s) ({statuses.get(category, 'fail').upper()})"
                    for category, count in sorted(violations.items())
                ) + ".",
                *(reasons[:1]),
            ])
        elif final != ValidationStatus.PASS and data_failures:
            details.setdefault(
                "failure_reason",
                "; ".join(
                    f"{category}: {count} record(s)"
                    for category, count in sorted(data_failures.items())
                ),
            )
        if resolved_target_only and isinstance(classifier, OperationClassifier):
            # Reconciliation changed the public counts; the message must not
            # retain the engine's pre-reconciliation mismatch totals.
            message = (
                f"Full comparison checked={int(details.get('records_checked', 0) or 0)}, "
                f"matched={int(details.get('matched_records', 0) or 0)}, "
                f"mismatched={int(details.get('mismatch_records', 0) or 0)}, "
                f"missing_in_source={int(details.get('missing_in_source', 0) or 0)}, "
                f"missing_in_target={int(details.get('missing_in_target', 0) or 0)}"
                f" | incremental operations: {operation_text}"
            )
        check.message = message
        check.status = final
        check.details = details
        if policy.target_driven:
            evidence_writer = getattr(classifier, "_evidence", None)
            result_holder = getattr(evidence_writer, "_result", None)
            self._reconcile_completed_row_count(result_holder, check)
        return check

    @staticmethod
    def _reconcile_completed_row_count(result_obj, comparison: CheckResult) -> None:
        """Reconcile the already-completed row-count check before returning.

        Row count runs earlier in the configured validation order. Performing
        this mutation here makes the finalized comparison the synchronization
        point, independent of which orchestrator/reporting route invoked it.
        """
        checks = getattr(result_obj, "checks", None)
        if not isinstance(checks, list):
            return
        row_count = next(
            (item for item in checks if item.check_name == "row_count"), None
        )
        if row_count is None or (row_count.details or {}).get("row_count_reconciled"):
            return
        details = comparison.details or {}
        operations = details.get("incremental_operations") or {}
        updates = int(details.get("validated_update_matches", 0) or 0)
        if updates <= 0:
            clean = (
                comparison.status == ValidationStatus.PASS
                and int(details.get("mismatch_records", 0) or 0) == 0
                and int(details.get("missing_in_source", 0) or 0) == 0
                and int(details.get("missing_in_target", 0) or 0) == 0
            )
            if clean:
                updates = int(
                    operations.get("update", 0) or 0
                )
        server_2_inserts = int(details.get("server_2_matches", 0) or 0)
        if row_count.source_value is None or row_count.target_value is None:
            return
        measured_source = int(row_count.source_value)
        measured_target = int(row_count.target_value)
        adjustment_executed = bool(
            details.get("operation_classification_performed")
            and (updates > 0 or server_2_inserts > 0)
        )
        reconciled_target = (
            max(0, measured_target - updates - server_2_inserts)
            if adjustment_executed else measured_source
        )
        delta = abs(measured_source - reconciled_target)
        delta_pct = (
            delta / max(abs(measured_source), abs(reconciled_target)) * 100
            if measured_source or reconciled_target else 0.0
        )
        tolerance = float((row_count.details or {}).get("tolerance_pct", 0) or 0)
        row_count.details = {
            **dict(row_count.details or {}),
            "measured_source_count": measured_source,
            "measured_target_count": measured_target,
            "historically_validated_updates": updates,
            "server_2_insert_count": server_2_inserts,
            "incremental_row_count_adjustment_executed": adjustment_executed,
            "reconciled_target_count": reconciled_target,
            "row_count_reconciled": True,
            "raw_status": row_count.status.value,
        }
        row_count.target_value = reconciled_target
        row_count.delta = delta
        row_count.delta_pct = round(delta_pct, 4)
        row_count.status = (
            ValidationStatus.PASS
            if delta_pct <= tolerance else ValidationStatus.FAIL
        )
        row_count.message = (
            "Row count after incremental target reconciliation: "
            f"measured_source={measured_source:,}, "
            f"measured_target={measured_target:,}, "
            f"validated_updates={updates:,}, server_2_inserts={server_2_inserts:,}, "
            f"adjustment_executed={str(adjustment_executed).lower()}, "
            f"reconciled_target={reconciled_target:,}, "
            f"delta={delta:,} ({delta_pct:.4f}%), tolerance={tolerance:.4f}%"
        )

    @staticmethod
    def _metric_details(
        mode: Any,
        strategy: Optional[str],
        key_mappings: List[Dict[str, str]],
        validation_profile: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {
            "validation_profile": validation_profile,
            "comparison_mode": mode,
            "record_comparison_mode": mode,
            "sampling_strategy": strategy,
            "key_mapping_source": "table_yaml",
            "key_columns_used": key_mappings,
            "records_checked": 0,
            "records_compared": 0,
            "matched_records": 0,
            "mismatch_records": 0,
            "execution_time": 0.0,
        }

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
        error_details = {
            "validation_profile": None,
            "comparison_mode": None,
            "sampling_strategy": None,
            "key_mapping_source": "table_yaml",
            "key_columns_used": [],
            "records_checked": 0,
            "records_compared": 0,
            "matched_records": 0,
            "mismatch_records": 0,
            **(details or {}),
            **extra,
            "execution_time": round(time.perf_counter() - started, 3),
            "table_id": table_id,
            "validation_name": "record_comparison",
            "exception_category": exception_category,
            "error_reason": message,
            "final_status": status.value,
        }
        error_details["records_compared"] = error_details.get(
            "records_checked", 0
        )
        return CheckResult(
            check_name="record_comparison",
            status=status,
            message=message,
            details=error_details,
        )

    def _policy_result(
        self,
        message: str,
        started: float,
        details: Dict[str, Any],
        status_field: str,
    ) -> CheckResult:
        status = self._policies.configured_status(status_field)
        return self._error(
            message,
            started,
            details,
            status=status,
            exception_category="MISSING_KEY_MAPPING",
            table_id=details.get("table_id"),
        )
