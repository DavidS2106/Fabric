"""Incremental operation classification for the record comparison validator.

The incremental phase asks three separate questions of every record in the
current load, and answers them in this order:

1. *What operation happened?* — INSERT, UPDATE, DELETE, REPLAY, UNCHANGED or
   UNKNOWN, decided from the configured CDC operation column when the table
   declares one, otherwise from a bounded lookup of the same logical record in
   the historical target scope (the rows loaded before the current window).
2. *Do source and current target agree?* — the existing comparison engine's
   result for the record (match, value mismatch, missing, unexpected).
3. *What does configuration say about that operation?* — the
   ``operation_policy`` of ``validations.incremental_strategy``.

Nothing here decides PASS or FAIL on its own: the classifier counts operations,
records policy violations with specific reasons and bounded evidence, and the
validator combines them with the data comparison result at the end.

Everything reused comes from the validator instance it is given: the key
cohort lookup (``_cohort_builder``), the canonical value comparison
(``_values_match``), the row hash (``_hash_frame``), the per-row predicate
statements (``_build_filter_queries``), the evidence limits
(``_configured_excess_limit``) and the existing evidence record types. No
second comparison, hash or evidence model is introduced.
"""

from __future__ import annotations

import json
import logging
import base64
import pickle
from collections import Counter
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from core.models import (
    INCREMENTAL_UPDATE_EVIDENCE_LIMIT, SERVER_2_MATCH_EVIDENCE_LIMIT,
    MissingRecord, ValidationStatus,
)

logger = logging.getLogger(__name__)

#: Operations a current-load record can be classified as.
OPERATIONS = ("insert", "update", "delete", "replay", "unchanged", "unknown")
class _UpdateEvidenceWriter:
    """Keep Excel-only successful UPDATE evidence bounded in memory.

    The evidence is attached as a private runtime attribute so dataclass/JSON
    serialization remains byte-for-byte compatible with the existing public
    result model.  The consolidated Excel writer is the sole consumer.
    """

    def __init__(self, validator, result_obj) -> None:
        raw = getattr(validator, "_incremental_update_evidence_limit", None)
        self.limit = (
            int(raw) if isinstance(raw, int) and not isinstance(raw, bool) and raw >= 0
            else INCREMENTAL_UPDATE_EVIDENCE_LIMIT
        )
        self._result = result_obj
        if not hasattr(result_obj, "_incremental_update_evidence"):
            result_obj._incremental_update_evidence = []

    def write(self, payload: Dict[str, Any]) -> None:
        rows = self._result._incremental_update_evidence
        if len(rows) < self.limit:
            rows.append(payload)


class _Server2EvidenceWriter:
    """Keep table-Excel evidence for successful alternate-source matches."""

    def __init__(self, result_obj) -> None:
        self.limit = SERVER_2_MATCH_EVIDENCE_LIMIT
        self._result = result_obj
        if not hasattr(result_obj, "server_2_match_evidence"):
            result_obj.server_2_match_evidence = []
        if not hasattr(result_obj, "_server_2_match_evidence"):
            # Compatibility alias for callers built against the earlier
            # private runtime evidence implementation.
            result_obj._server_2_match_evidence = (
                result_obj.server_2_match_evidence
            )

    def write(self, payload: Dict[str, Any]) -> None:
        rows = self._result.server_2_match_evidence
        if len(rows) < self.limit:
            rows.append(payload)

#: Validation/data failure categories (never framework errors).
TARGET_RECORD_MISSING = "TARGET_RECORD_MISSING"
UNEXPECTED_TARGET_RECORD = "UNEXPECTED_TARGET_RECORD"
SOURCE_TARGET_VALUE_MISMATCH = "SOURCE_TARGET_VALUE_MISMATCH"
HASH_OCCURRENCE_MISMATCH = "HASH_OCCURRENCE_MISMATCH"
DELETE_NOT_APPLIED = "DELETE_NOT_APPLIED"
CDC_OPERATION_NOT_ALLOWED = "CDC_OPERATION_NOT_ALLOWED"

_POLICY_RANK = {"pass": 0, "warning": 1, "fail": 2}
_POLICY_STATUS = {
    "pass": ValidationStatus.PASS,
    "warning": ValidationStatus.WARNING,
    "fail": ValidationStatus.FAIL,
}
_STATUS_SEVERITY = {
    ValidationStatus.PASS: 0,
    ValidationStatus.WARN: 1,
    ValidationStatus.WARNING: 1,
    ValidationStatus.FAIL: 2,
}
#: Changed columns quoted in one failure reason before the list is elided.
_REASON_COLUMN_LIMIT = 3


def policy_violation_category(operation: str, detection: str) -> str:
    """The failure category of an operation the configured policy rejects.

    Always named after the operation (``UPDATE_NOT_ALLOWED``,
    ``DELETE_NOT_ALLOWED``, ``REPLAY_NOT_ALLOWED``, ...), whichever way the
    operation was detected. ``CDC_OPERATION_NOT_ALLOWED`` is reported
    alongside it for a CDC-detected operation (see
    :func:`cdc_policy_violation`), so a CDC-driven rejection and a
    historically-derived one stay distinguishable without either losing the
    operation it refers to.
    """
    del detection  # naming depends on the operation only
    return f"{operation.upper()}_NOT_ALLOWED"


def cdc_policy_violation(operation: str, detection: str) -> bool:
    """Whether a rejected operation was detected from the configured CDC column."""
    return detection == "cdc" and operation in ("insert", "update", "delete")


def combine_statuses(data_status: ValidationStatus, policy_statuses: Sequence[str]) -> ValidationStatus:
    """Worst of the data comparison status and every violated policy status.

    Only PASS / WARNING / FAIL participate; an ERROR, CONFIG_ERROR or SKIP from
    the comparison itself is returned unchanged, because a comparison that did
    not execute has nothing to combine.
    """
    if data_status not in _STATUS_SEVERITY:
        return data_status
    worst = data_status
    for policy in policy_statuses:
        candidate = _POLICY_STATUS.get(str(policy).lower())
        if candidate is None:
            continue
        if _STATUS_SEVERITY[candidate] > _STATUS_SEVERITY[worst]:
            worst = candidate
    return worst


@dataclass(frozen=True)
class IncrementalPolicy:
    """The one effective incremental execution policy resolved for a table."""

    enabled: bool
    unavailable_reason: Optional[str]
    operation_policy: Dict[str, str]
    target_behavior: Optional[str]
    historical_check: bool
    historical_absence_proves_insert: bool
    preserve_separately: bool
    capture_evidence: bool
    cdc: Optional[Dict[str, Any]]
    target_audit_column: Optional[str]
    target_watermark_column: Optional[str]
    source_watermark_column: Optional[str]
    comparison_direction: str
    historical_predicates: Tuple[str, ...]
    historical_parameters: Tuple[Any, ...]
    capabilities: Dict[str, Any]
    bootstrap_used: bool
    strategy: Optional[str]

    @classmethod
    def from_scope(cls, scope: Any) -> Optional["IncrementalPolicy"]:
        if scope is None:
            return None
        raw = getattr(scope, "incremental_policy", None)
        if not isinstance(raw, dict) or not raw:
            return None
        details = getattr(scope, "details", {}) or {}
        cdc = raw.get("cdc")
        return cls(
            enabled=bool(raw.get("classification_enabled")),
            unavailable_reason=raw.get("classification_unavailable_reason"),
            operation_policy={
                str(name): str(value).lower()
                for name, value in (raw.get("operation_policy") or {}).items()
            },
            target_behavior=raw.get("target_behavior"),
            historical_check=bool(raw.get("historical_check", True)),
            historical_absence_proves_insert=bool(
                raw.get("historical_absence_proves_insert", False)
            ),
            preserve_separately=bool(
                raw.get("preserve_operation_and_status_separately", True)
            ),
            capture_evidence=bool(raw.get("capture_mismatch_evidence", True)),
            cdc=dict(cdc) if isinstance(cdc, dict) and cdc.get("enabled") else None,
            target_audit_column=raw.get("target_audit_column"),
            target_watermark_column=raw.get("target_watermark_column"),
            source_watermark_column=raw.get("source_watermark_column"),
            comparison_direction=str(raw.get("comparison_direction") or "bidirectional"),
            historical_predicates=tuple(raw.get("historical_target_predicates") or ()),
            historical_parameters=tuple(raw.get("historical_target_parameters") or ()),
            capabilities=dict(raw.get("capabilities") or {}),
            bootstrap_used=bool(raw.get("bootstrap_used")),
            strategy=(
                details.get("resolved_incremental_strategy")
                or details.get("incremental_strategy")
            ),
        )

    @property
    def target_driven(self) -> bool:
        """Current target rows drive the comparison; source rows outside the
        current load are not part of the window (no source selector exists)."""
        return self.comparison_direction == "target_to_source"

    def status_for(self, operation: str) -> str:
        """The configured final status of one operation (pass|warning|fail)."""
        configured = self.operation_policy.get(operation)
        if configured is None:
            # Reachable only for a policy bypassing the loader's completeness
            # check; report the gap rather than silently accepting the record.
            logger.warning(
                "operation_policy has no entry for '%s'; treating it as fail", operation
            )
            return "fail"
        return configured

    def cdc_operation(self, raw_value: Any) -> Optional[str]:
        """Map a configured CDC operation value to insert/update/delete.

        Returns ``None`` when the table has no CDC, ``"unknown"`` when CDC is
        configured but the value is NULL or not among the configured values.
        """
        if not self.cdc:
            return None
        if raw_value is None or (isinstance(raw_value, float) and pd.isna(raw_value)):
            return "unknown"
        text = str(raw_value).strip()
        for kind, values in (self.cdc.get("operation_values") or {}).items():
            if text in {str(item).strip() for item in values}:
                return kind
        return "unknown"

    def as_details(self) -> Dict[str, Any]:
        return {
            "operation_policy": dict(self.operation_policy),
            "target_behavior": self.target_behavior,
            "resolved_incremental_strategy": self.strategy,
            "comparison_direction": self.comparison_direction,
            "cdc_enabled": bool(self.cdc),
            "cdc_operation_column": (self.cdc or {}).get("operation_column"),
            "cdc_delete_behavior": (self.cdc or {}).get("delete_behavior"),
            "target_audit_column": self.target_audit_column,
            "historical_check": self.historical_check,
            "historical_absence_proves_insert": self.historical_absence_proves_insert,
            "bootstrap_used": self.bootstrap_used,
        }


class _EvidenceWriter:
    """Bounded evidence into the existing record lists.

    Excel and JSON already read ``missing_in_source`` (EXCESS_IN_TARGET) and
    ``missing_in_target`` (EXCESS_IN_SOURCE); an operation-policy violation is
    written where its physical evidence row lives: the current target row for
    an UPDATE / REPLAY / UNKNOWN / disallowed INSERT, the source-side change
    for a DELETE the target no longer holds. The existing excess limits bound
    every list.
    """

    def __init__(self, validator, result_obj, table_id: Any) -> None:
        self._validator = validator
        self._result = result_obj
        self._table_id = table_id
        self.exported = 0
        self.limit_reached = False

    def write(self, side: str, primary_key: str, location: str, payload: Dict[str, Any]) -> bool:
        destination = (
            self._result.missing_in_source if side == "target"
            else self._result.missing_in_target
        )
        limit = self._validator._configured_excess_limit(side)
        if limit is not None and len(destination) >= limit:
            self.limit_reached = True
            return False
        destination.append(MissingRecord(
            table_id=self._table_id,
            primary_key=primary_key,
            record_data=json.dumps(
                payload, default=str, ensure_ascii=False, separators=(",", ":")
            ),
            location=location,
            row_hash_value=None,
        ))
        self.exported += 1
        return True


class OperationClassifier:
    """Per-record operation classification for keyed comparisons.

    Fed by the dual keyset merge as it aligns rows: matched pairs (with the
    engine's own mismatch verdict), current target rows with no source row,
    and source rows with no current target row. Rows are buffered per batch and
    the historical target scope is read for a whole batch of keys through the
    configured key cohort strategy, so memory and statement count stay bounded
    by the comparison batch size.
    """

    def __init__(
        self,
        validator,
        policy: IncrementalPolicy,
        table_map,
        target_conn,
        target_fqn: str,
        target_dialect,
        historical_where: str,
        source_keys: Sequence[str],
        target_keys: Sequence[str],
        configured_columns: Sequence[Dict[str, str]],
        result_obj,
        batch_size: int,
        source_cdc_column: Optional[str] = None,
        target_soft_delete_column: Optional[str] = None,
    ) -> None:
        self._validator = validator
        self.policy = policy
        self._table_map = table_map
        self._table_id = getattr(table_map, "id", None)
        self._target_conn = target_conn
        self._target_fqn = target_fqn
        self._target_dialect = target_dialect
        self._historical_where = historical_where
        self._source_keys = list(source_keys)
        self._target_keys = list(target_keys)
        self._columns = [dict(item) for item in configured_columns]
        self._target_compare_columns = [item["target_column"] for item in self._columns]
        self._source_to_target = {
            item["source_column"]: item["target_column"] for item in self._columns
        }
        self._batch_size = max(1, int(batch_size or 1))
        self._source_cdc_column = source_cdc_column
        self._target_soft_delete_column = target_soft_delete_column
        self._evidence = _EvidenceWriter(validator, result_obj, self._table_id)
        self._update_evidence = _UpdateEvidenceWriter(validator, result_obj)
        self._server_2_evidence = _Server2EvidenceWriter(result_obj)
        self._pending: List[Dict[str, Any]] = []
        self._pending_source_only: List[Dict[str, Any]] = []
        self.operations: Counter = Counter()
        self.violations: Counter = Counter()
        self.cdc_violations: Counter = Counter()
        self.violation_statuses: Dict[str, str] = {}
        self.data_failures: Counter = Counter()
        self.confidence: Counter = Counter()
        self.detection: Counter = Counter()
        self.out_of_window_source_rows = 0
        self.historical_lookups = 0
        self.historical_rows_read = 0
        self._historical_scope_nonempty: Optional[bool] = None
        self.records_classified = 0
        self._reasons: List[str] = []
        self.validated_updates = 0
        self.validated_update_keys: set = set()
        self.server_2_rows_checked = 0
        self.server_2_matches = 0
        self.server_2_match_counts: Counter = Counter()
        self.server_2_matched_keys: set = set()

    # ── Observation API (called by the comparison engine) ──────
    @property
    def target_driven(self) -> bool:
        return self.policy.target_driven

    def observe_aligned(
        self, source_frame: pd.DataFrame, target_frame: pd.DataFrame,
        tokens: Sequence[Tuple[str, ...]], mismatched_tokens: set,
    ) -> None:
        source_rows = source_frame.drop(columns=["_merge_key"], errors="ignore").to_dict("records")
        target_rows = target_frame.drop(columns=["_merge_key"], errors="ignore").to_dict("records")
        # Aligned rows (both matches and value mismatches) are final comparison
        # outcomes. Historical UPDATE proof is reserved for target-only rows.
        del source_rows, target_rows, tokens, mismatched_tokens

    def observe_target_only(self, token: Tuple[str, ...], target_row: Dict[str, Any]) -> None:
        self._pending.append({
            "token": token, "target": dict(target_row), "source": None, "data": "unexpected",
        })
        if len(self._pending) >= self._batch_size:
            self._flush_current()

    def observe_source_only(
        self, token: Tuple[str, ...], source_row: Dict[str, Any],
        record_missing: Callable[[Dict[str, Any], Tuple[str, ...]], None],
    ) -> None:
        """A source row of the window with no current target row.

        The engine defers its MISSING_IN_TARGET count and evidence to
        ``record_missing``; the classifier calls it back for every row that is
        genuinely missing, and not for a row whose historical target record is
        identical (UNCHANGED) or a CDC DELETE the target applied as configured.
        """
        self._pending_source_only.append({
            "token": token, "source": dict(source_row), "record_missing": record_missing,
        })
        if len(self._pending_source_only) >= self._batch_size:
            self._flush_source_only()

    def flush(self) -> None:
        self._flush_current()
        self._flush_source_only()

    # ── Historical target lookup ──────────────────────────────
    def _historical_scope_has_rows(self) -> bool:
        """Probe the configured historical scope once and cache the result.

        An empty historical window used to execute one cohort statement for
        every page of current keys even though every answer was necessarily
        empty.  A single bounded existence query proves that condition for the
        whole classifier.  Non-empty scopes continue through the existing
        keyed, bounded and parallel cohort path.
        """
        if self._historical_scope_nonempty is not None:
            return self._historical_scope_nonempty
        projection = self._validator._projection(
            [self._target_keys[0]], self._target_dialect
        )
        sql = self._validator._select_sql(
            self._target_fqn, projection, self._historical_where,
            top=1, dialect=self._target_dialect,
        )
        frame = self._validator._execute_batch_query(self._target_conn, sql)
        if not isinstance(frame, pd.DataFrame):
            raise TypeError("Connector execute_query must return a pandas DataFrame")
        self.historical_lookups += 1
        self.historical_rows_read += len(frame)
        self._historical_scope_nonempty = not frame.empty
        logger.info(
            "[HISTORICAL_SCOPE] table=%s historical_scope_nonempty=%s probe_rows=%d",
            self._table_id, self._historical_scope_nonempty, len(frame),
        )
        return self._historical_scope_nonempty

    def _historical_rows(
        self, keys_by_token: Dict[Tuple[str, ...], Tuple[Any, ...]]
    ) -> Dict[Tuple[str, ...], List[Dict[str, Any]]]:
        if not keys_by_token or not self.policy.historical_check:
            return {}
        if not str(self._historical_where or "").strip():
            # A bootstrap run has no rows loaded before the current window.
            return {}
        if not self._historical_scope_has_rows():
            logger.info(
                "[HISTORICAL_LOOKUP] table=%s keys=%d statements=0 "
                "historical_target_rows=0 keys_with_history=0 "
                "reason=empty_historical_scope",
                self._table_id, len(keys_by_token),
            )
            return {}
        columns = list(dict.fromkeys(
            self._target_keys + self._target_compare_columns
            + [name for name in (
                self.policy.target_audit_column, self.policy.target_watermark_column,
                self._target_soft_delete_column,
            ) if name]
        ))
        # One classification batch can carry far more keys than a single
        # statement should, and the whole batch used to be rendered as one
        # cohort statement. The configured target lookup page size
        # (``comparison_settings.comparison_tiers.target_key_lookup_batch_size``,
        # the same setting the cohort lookup already uses) decides how many
        # keys one statement carries, and every rendered statement is checked
        # against ``comparison_settings.query_safety`` before it runs. The
        # whole batch is still read in one pass, so this remains a batched
        # lookup and never becomes one query per row.
        lookup = self._validator._sample_lookup_settings()
        page_keys = max(1, min(self._batch_size, int(lookup["keyed_batch_size"])))
        builder = self._validator._cohort_builder(
            self._target_conn, self._target_dialect, page_keys
        )
        keys = list(dict.fromkeys(keys_by_token.values()))

        def build(page):
            plan = builder.plan(
                self._target_fqn, columns, self._historical_where,
                self._target_keys, page,
            )
            return list(plan.queries), plan.strategy

        current_sample_timings = getattr(self._validator, "_sample_timings", None)
        timings = self._validator._new_sample_timings(len(keys), True, lookup, len(columns))
        # Historical classification has its own instrumentation and must not
        # replace the current source/target fetch timings reported by the
        # comparison engine.
        self._validator._sample_timings = current_sample_timings
        timings["table_id"] = self._table_id
        rows_by_token: Dict[Tuple[str, ...], List[Dict[str, Any]]] = {}
        statements = 0
        historical_rows = 0
        rendered = list(self._validator._bounded_statement_batches(
            keys, builder.keys_per_statement, build, lookup, timings
        ))
        batches = [
            SimpleNamespace(index=index, payload=(page, queries, strategy), rows=len(page))
            for index, (page, queries, strategy) in enumerate(rendered)
        ]
        workers, parallel_details = self._validator._target_lookup_parallelism(
            self._target_conn, lookup, len(batches)
        )
        timings.update(parallel_details)

        def run_batch(batch):
            _page, queries, strategy = batch.payload
            result = SimpleNamespace(
                index=batch.index, frames=[], statements=[], strategy=strategy,
                sql_generation_seconds=0.0, target_execute_seconds=0.0,
                target_extract_seconds=0.0, connection_wait_seconds=0.0,
                db_execution_seconds=0.0, hash_seconds=0.0, safety_splits=0,
                max_sql_bytes=0, peak_frame_rows=0,
            )
            for query in queries:
                frame = self._validator._execute_batch_query(self._target_conn, query)
                result.frames.append(frame)
                result.statements.append(query)
                result.max_sql_bytes = max(result.max_sql_bytes, len(query))
                result.peak_frame_rows = max(result.peak_frame_rows, len(frame))
            return result

        def process(result):
            nonlocal statements, historical_rows
            statements += len(result.statements)
            self.historical_lookups += len(result.statements)
            for frame in result.frames:
                if frame is None or frame.empty:
                    continue
                self._validator._require_columns(
                    frame, self._target_keys, "historical target lookup"
                )
                tokens = self._validator._key_tokens(frame, self._target_keys)
                records = frame.to_dict("records")
                historical_rows += len(records)
                self.historical_rows_read += len(records)
                for token, record in zip(tokens, records):
                    rows_by_token.setdefault(token, []).append(record)
            result.frames = []

        self._validator._run_lookup_batches(
            batches, run_batch, process, workers, lookup, timings,
            "incremental_historical_cohort",
        )
        logger.info(
            "[HISTORICAL_LOOKUP] table=%s keys=%d statements=%d "
            "keys_per_statement=%d parallel_workers=%d "
            "historical_target_rows=%d keys_with_history=%d",
            self._table_id, len(keys), statements, builder.keys_per_statement,
            workers, historical_rows, len(rows_by_token),
        )
        return rows_by_token

    def _latest(self, rows: Optional[List[Dict[str, Any]]]) -> Optional[Dict[str, Any]]:
        """The current version of one logical record in the historical scope.

        Ordered by a configured *target* column only: the target audit/load
        column, else the target watermark column. A CDC sequence column
        describes the source change feed and cannot order target rows, so it is
        never used here. Multiple versions without a configured ordering
        column are inconclusive and stop classification.
        """
        if not rows:
            return None
        if len(rows) == 1:
            return rows[0]
        for order_column in (
            self.policy.target_audit_column,
            self.policy.target_watermark_column,
        ):
            if order_column and all(order_column in row for row in rows):
                return max(
                    rows,
                    key=lambda row: self._validator._sortable_key_part(row.get(order_column)),
                )
        raise ValueError("Historical target has multiple versions without configured version ordering")

    def _changed_columns(
        self, previous: Dict[str, Any], current: Dict[str, Any],
        current_is_source: bool = False,
    ) -> List[Dict[str, Any]]:
        changed = []
        for item in self._columns:
            target_column = item["target_column"]
            current_column = item["source_column"] if current_is_source else target_column
            before = previous.get(target_column)
            after = current.get(current_column)
            if not self._validator._values_match(before, after):
                changed.append({
                    "column": target_column,
                    "mapping": item.get("name") or target_column,
                    "previous": before, "current": after,
                })
        return changed

    # ── Classification ────────────────────────────────────────
    def _flush_current(self) -> None:
        items, self._pending = self._pending, []
        if not items:
            return
        keys_by_token = {
            item["token"]: tuple(item["target"].get(key) for key in self._target_keys)
            for item in items
        }
        historical = self._historical_rows(keys_by_token)
        server_2_tokens = self._server_2_keyed_tokens(
            [item for item in items if not historical.get(item["token"])]
        )
        for item in items:
            self._classify_current(
                item, self._latest(historical.get(item["token"])),
                server_2_found=item["token"] in server_2_tokens,
            )

    def _server_2_keyed_tokens(self, items: Sequence[Dict[str, Any]]) -> set:
        """Verify unresolved target keys against the optional alternate source."""
        provider = getattr(self._validator, "_server_2_connection_provider", None)
        if not items or not callable(provider):
            return set()
        logger.info(
            "[SERVER_2_FALLBACK] table=%s enabled=true strategy=keyed "
            "unresolved_records=%d",
            self._table_id, len(items),
        )
        connection = provider()
        dialect = self._validator._dialect(connection)
        fqn = self._validator._source_fqn_for_table(self._table_map, dialect)
        columns = list(dict.fromkeys(
            self._source_keys + [item["source_column"] for item in self._columns]
        ))
        key_values = [
            tuple(item["target"].get(key) for key in self._target_keys)
            for item in items
        ]
        key_values = self._validator._coerce_lookup_keys_to_source_types(
            connection,
            self._table_map.source_schema,
            self._table_map.source_table,
            self._source_keys,
            key_values,
        )
        lookup = self._validator._sample_lookup_settings()
        timings = self._validator._new_sample_timings(
            len(key_values), True, lookup, len(columns)
        )
        frame, _ = self._validator._keyed_sample_cohort(
            connection, dialect, fqn, columns, "", self._source_keys,
            key_values, lookup, timings, lookup_side="server_2",
        )
        self.server_2_rows_checked += len(items)
        if frame.empty:
            return set()
        self._validator._require_columns(frame, self._source_keys, "server_2 key")
        found = set(self._validator._key_tokens(frame, self._source_keys))
        self.server_2_matches += len(found)
        self.server_2_matched_keys.update(
            self._validator._display_key(token) for token in found
        )
        target_by_token = {item["token"]: item["target"] for item in items}
        for token, source_row in zip(
            self._validator._key_tokens(frame, self._source_keys),
            frame.to_dict("records"),
        ):
            if token not in found:
                continue
            identifier = {
                source: source_row.get(source)
                for source in self._source_keys
            }
            self._server_2_evidence.write({
                "table_id": self._table_id,
                "source_table": (
                    f"{getattr(self._table_map, 'source_schema', '')}."
                    f"{getattr(self._table_map, 'source_table', '')}"
                ),
                "target_table": (
                    f"{getattr(self._table_map, 'target_schema', '')}."
                    f"{getattr(self._table_map, 'target_table', '')}"
                ),
                "server_2": getattr(self._table_map, "server_2", None),
                "match_method": "KEY",
                "record_identifier": identifier,
                "occurrences": 1,
                "status": "MATCHED IN SERVER_2",
                "source_values": {
                    item["source_column"]: source_row.get(item["source_column"])
                    for item in self._columns
                },
                "target_values": {
                    item["target_column"]: target_by_token.get(token, {}).get(
                        item["target_column"]
                    )
                    for item in self._columns
                },
            })
        logger.info(
            "[SERVER_2_FALLBACK] table=%s strategy=keyed checked=%d "
            "found=%d unresolved=%d",
            self._table_id, len(items), len(found), len(items) - len(found),
        )
        return found

    def _classify_current(
        self, item: Dict[str, Any], latest: Optional[Dict[str, Any]],
        server_2_found: bool = False,
    ) -> None:
        self.records_classified += 1
        source_row, target_row = item["source"], item["target"]
        changed = self._changed_columns(latest, target_row) if latest is not None else []
        cdc_raw = (
            source_row.get(self._source_cdc_column)
            if self.policy.cdc and source_row is not None else None
        )
        cdc_operation = self.policy.cdc_operation(cdc_raw) if self.policy.cdc else None
        # UPDATE is target-history evidence only.  The current source row is
        # used by the independent source/target comparison, never as the
        # before-image for operation classification.  A configured CDC DELETE
        # remains authoritative because its application semantics are checked
        # separately below; CDC INSERT remains usable only when target history
        # confirms that no earlier target identity exists.
        if cdc_operation == "delete":
            operation, detection, confidence = "delete", "cdc", "high"
        elif latest is not None:
            operation = "update" if changed else "replay"
            detection, confidence = "historical_lookup", "key_based"
        elif self.policy.cdc and source_row is None:
            operation, detection, confidence = "unknown", "cdc", "no_cdc_event"
        elif cdc_operation == "insert":
            operation, detection, confidence = "insert", "cdc", "high"
        elif cdc_operation == "update":
            operation, detection, confidence = (
                "unknown", "historical_lookup", "target_history_required_for_update"
            )
        elif cdc_operation == "unknown":
            operation, detection, confidence = "unknown", "cdc", "cdc_value_unmapped"
        elif not self.policy.historical_check or not self._historical_where:
            operation, detection, confidence = "unknown", "historical_check_disabled", "none"
        elif latest is None and self.policy.historical_absence_proves_insert:
            # In a target-driven window the current target row is the record
            # being classified and source_row is intentionally absent for a
            # missing-in-source outcome. A complete historical target lookup
            # therefore proves that this target identity is a new INSERT even
            # though the independent source/target comparison must still fail
            # it as UNEXPECTED_TARGET_RECORD.
            operation, detection, confidence = "insert", "historical_lookup", "high"
        elif latest is None:
            operation, detection, confidence = "unknown", "historical_lookup", "history_sufficiency_unproven"
        else:  # defensive: a non-empty latest row is handled above
            operation, detection, confidence = "unknown", "historical_lookup", "none"
        if item["data"] == "unexpected" and operation != "update" and not server_2_found:
            self.data_failures[UNEXPECTED_TARGET_RECORD] += 1
        elif item["data"] == "mismatch" and operation != "update":
            self.data_failures[SOURCE_TARGET_VALUE_MISMATCH] += 1
        self._record(
            operation, detection, confidence,
            token=item["token"], source_row=source_row, target_row=target_row,
            historical=latest, changed=changed, data=item["data"], cdc_raw=cdc_raw,
        )
        if operation == "update" and item["data"] == "unexpected":
            self.validated_updates += 1
            self.validated_update_keys.add(self._validator._display_key(item["token"]))
            reason = self._reason(
                operation, detection, item["token"], latest, changed,
                item["data"], cdc_raw, "pass",
            )
            self._update_evidence.write(self._payload(
                operation, detection, confidence, item["token"], source_row,
                target_row, latest, changed, data=item["data"], cdc_raw=cdc_raw,
                reason=reason, status="VALID UPDATE", category=None,
            ))
        if operation == "delete" and detection == "cdc":
            self._check_delete_applied(item["token"], source_row, target_row, latest, cdc_raw)

    def _flush_source_only(self) -> None:
        items, self._pending_source_only = self._pending_source_only, []
        if not items:
            return
        keys_by_token = {
            item["token"]: tuple(item["source"].get(key) for key in self._source_keys)
            for item in items
        }
        historical = self._historical_rows(keys_by_token)
        for item in items:
            self._classify_source_only(item, self._latest(historical.get(item["token"])))

    def _classify_source_only(self, item: Dict[str, Any], latest: Optional[Dict[str, Any]]) -> None:
        self.records_classified += 1
        source_row = item["source"]
        record_missing = item["record_missing"]
        changed = (
            self._changed_columns(latest, source_row, current_is_source=True)
            if latest is not None else []
        )
        cdc_raw = source_row.get(self._source_cdc_column) if self.policy.cdc else None
        cdc_operation = self.policy.cdc_operation(cdc_raw) if self.policy.cdc else None
        missing = True
        if cdc_operation == "delete":
            operation, detection, confidence = "delete", "cdc", "high"
            behavior = (self.policy.cdc or {}).get("delete_behavior")
            if behavior == "hard_delete":
                # The configured target behavior is exactly "no current row".
                missing = False
            else:
                self.data_failures[DELETE_NOT_APPLIED] += 1
                self._write_evidence(
                    "source", item["token"], DELETE_NOT_APPLIED,
                    self._payload(
                        "delete", detection, confidence, item["token"], source_row,
                        None, latest, [], data="missing", cdc_raw=cdc_raw,
                        reason=(
                            f"CDC operation DELETE was detected for PK {self._key_text(item['token'])}, "
                            f"but no current target row carries the configured soft-delete marker "
                            f"(configured delete behavior: {behavior})."
                        ),
                        status="FAIL", category=DELETE_NOT_APPLIED,
                    ),
                )
        elif cdc_operation in ("insert", "update"):
            operation, detection, confidence = cdc_operation, "cdc", "high"
        elif cdc_operation == "unknown":
            operation, detection, confidence = "unknown", "cdc", "cdc_value_unmapped"
        elif not self.policy.historical_check or not self._historical_where:
            operation, detection, confidence = "unknown", "historical_check_disabled", "none"
        elif latest is not None and not changed:
            # The target already holds this exact logical record: nothing new
            # was expected to be loaded for it.
            operation, detection, confidence = "unchanged", "historical_lookup", "key_based"
            # A source row selected by the current window is expected in the
            # current target scope. An older copy does not satisfy that load.
        elif latest is None:
            operation, detection, confidence = "unknown", "historical_lookup", "history_sufficiency_unproven"
        else:
            operation, detection, confidence = "update", "historical_lookup", "key_based"
        if missing:
            self.data_failures[TARGET_RECORD_MISSING] += 1
            record_missing(source_row, item["token"])
        self._record(
            operation, detection, confidence,
            token=item["token"], source_row=source_row, target_row=None,
            historical=latest, changed=changed,
            data="missing" if missing else "not_loaded_as_configured", cdc_raw=cdc_raw,
        )

    def _check_delete_applied(self, token, source_row, target_row, latest, cdc_raw) -> None:
        behavior = (self.policy.cdc or {}).get("delete_behavior")
        applied = False
        reason = None
        if behavior == "hard_delete":
            reason = (
                f"CDC operation DELETE was detected for PK {self._key_text(token)}, but the "
                "current target load still holds a row for the record (configured delete "
                "behavior: hard_delete)."
            )
        elif behavior == "soft_delete":
            column = (self.policy.cdc or {}).get("soft_delete_column")
            values = {str(item).strip() for item in (self.policy.cdc or {}).get("soft_delete_values") or []}
            marker = target_row.get(column) if column else None
            applied = marker is not None and str(marker).strip() in values
            if not applied:
                reason = (
                    f"CDC operation DELETE was detected for PK {self._key_text(token)}, but the "
                    f"current target row's '{column}' value {marker!r} is not a configured "
                    "soft-delete marker."
                )
        if applied:
            return
        self.data_failures[DELETE_NOT_APPLIED] += 1
        self._write_evidence(
            "target", token, DELETE_NOT_APPLIED,
            self._payload(
                "delete", "cdc", "high", token, source_row, target_row, latest, [],
                data="delete_not_applied", cdc_raw=cdc_raw, reason=reason,
                status="FAIL", category=DELETE_NOT_APPLIED,
            ),
        )

    # ── Policy + evidence ─────────────────────────────────────
    def _record(
        self, operation: str, detection: str, confidence: str, *, token, source_row,
        target_row, historical, changed, data, cdc_raw,
    ) -> None:
        self.operations[operation] += 1
        self.detection[detection] += 1
        self.confidence[confidence] += 1
        status = self.policy.status_for(operation)
        if status == "pass":
            return
        category = policy_violation_category(operation, detection)
        self.violations[category] += 1
        self.violation_statuses[category] = status
        if cdc_policy_violation(operation, detection):
            # The same rejection, additionally attributed to the configured CDC
            # operation column; counted separately so the two are never confused.
            self.cdc_violations[category] += 1
            self.violation_statuses[CDC_OPERATION_NOT_ALLOWED] = status
        reason = self._reason(operation, detection, token, historical, changed, data, cdc_raw, status)
        if len(self._reasons) < _REASON_COLUMN_LIMIT:
            self._reasons.append(reason)
        side = "target" if target_row is not None else "source"
        self._write_evidence(
            side, token, category,
            self._payload(
                operation, detection, confidence, token, source_row, target_row,
                historical, changed, data=data, cdc_raw=cdc_raw, reason=reason,
                status=status.upper(), category=category,
            ),
        )

    def _write_evidence(self, side: str, token, category: str, payload: Dict[str, Any]) -> None:
        if not self.policy.capture_evidence:
            return
        self._evidence.write(side, self._validator._display_key(token), category, payload)

    def _key_text(self, token) -> str:
        return self._validator._display_key(token)

    def _reason(self, operation, detection, token, historical, changed, data, cdc_raw, status) -> str:
        key = self._key_text(token)
        policy_text = f"Configured {operation.upper()} policy is {status.upper()}."
        if detection == "cdc" and operation in ("insert", "update", "delete"):
            return (
                f"CDC operation {operation.upper()} (value {cdc_raw!r}) was detected for PK {key}. "
                f"{policy_text}"
            )
        if operation == "update":
            columns = ", ".join(
                f"{item['column']}: historical={item['previous']!r}, current={item['current']!r}"
                for item in changed[:_REASON_COLUMN_LIMIT]
            )
            if len(changed) > _REASON_COLUMN_LIMIT:
                columns += f" (+{len(changed) - _REASON_COLUMN_LIMIT} more)"
            return (
                f"PK {key} existed before the current load and its business values changed "
                f"({columns}). Operation classified as UPDATE. {policy_text}"
            )
        if operation == "replay":
            return (
                f"PK {key} existed before the current load with identical business values and "
                f"was loaded again in the current load. Operation classified as REPLAY. {policy_text}"
            )
        if operation == "insert":
            return (
                f"PK {key} did not exist before the current load. Operation classified as "
                f"INSERT. {policy_text}"
            )
        if operation == "unchanged":
            return (
                f"PK {key} changed in the incremental source scope but the target already holds "
                f"identical business values and loaded no new row. Operation classified as "
                f"UNCHANGED. {policy_text}"
            )
        if data == "unexpected":
            return (
                f"Unexpected target-only record for PK {key}: source record not found. "
                f"Operation classified as {operation.upper()}. {policy_text}"
            )
        if detection == "cdc":
            return (
                f"PK {key} has no configured CDC operation value ({cdc_raw!r}); the operation "
                f"cannot be proven. Operation classified as UNKNOWN. {policy_text}"
            )
        return (
            f"PK {key} is in the current target load but its origin cannot be proven "
            f"({data}). Operation classified as UNKNOWN. {policy_text}"
        )

    def _payload(
        self, operation, detection, confidence, token, source_row, target_row, historical,
        changed, *, data, cdc_raw, reason, status, category,
    ) -> Dict[str, Any]:
        key_values = {
            column: (target_row or {}).get(column) if target_row is not None else None
            for column in self._target_keys
        }
        if target_row is None and source_row is not None:
            key_values = {
                target: source_row.get(source)
                for source, target in zip(self._source_keys, self._target_keys)
            }
        source_values = (
            {item["source_column"]: source_row.get(item["source_column"]) for item in self._columns}
            if source_row is not None else None
        )
        target_values = (
            {item["target_column"]: target_row.get(item["target_column"]) for item in self._columns}
            if target_row is not None else None
        )
        historical_values = (
            {item["target_column"]: historical.get(item["target_column"]) for item in self._columns}
            if historical is not None else None
        )
        audit = self.policy.target_audit_column
        watermark_target = self.policy.target_watermark_column
        watermark_source = self.policy.source_watermark_column
        return {
            "table_id": self._table_id,
            "source_table": f"{getattr(self._table_map, 'source_schema', '')}.{getattr(self._table_map, 'source_table', '')}",
            "target_table": f"{getattr(self._table_map, 'target_schema', '')}.{getattr(self._table_map, 'target_table', '')}",
            "validation_name": "record_comparison",
            "resolved_incremental_strategy": self.policy.strategy,
            "target_behavior": self.policy.target_behavior,
            "operation": operation.upper(),
            "status": status,
            "failure_category": category,
            "failure_reason": reason,
            "detection_method": detection,
            "classification_confidence": confidence,
            "primary_key": key_values,
            "source_target_comparison": data,
            "cdc_operation": cdc_raw,
            "cdc_sequence_value": (
                source_row.get((self.policy.cdc or {}).get("sequence_column"))
                if source_row is not None and (self.policy.cdc or {}).get("sequence_column")
                else None
            ),
            "source_watermark_value": (
                source_row.get(watermark_source) if source_row is not None and watermark_source else None
            ),
            "target_watermark_value": (
                target_row.get(watermark_target) if target_row is not None and watermark_target else None
            ),
            "target_audit_value": target_row.get(audit) if target_row is not None and audit else None,
            "historical_audit_value": historical.get(audit) if historical is not None and audit else None,
            "historical_watermark_value": (
                historical.get(watermark_target) if historical is not None and watermark_target else None
            ),
            "historical_record_found": historical is not None,
            "current_target_record_found": target_row is not None,
            "mismatched_columns": changed,
            "source_values": source_values,
            "target_values": target_values,
            "historical_values": historical_values,
        }

    # ── Summary ───────────────────────────────────────────────
    def summary(self) -> Dict[str, Any]:
        return {
            "operation_classification_performed": True,
            "classification_method": "keyed",
            "records_classified": self.records_classified,
            "incremental_operations": {name: int(self.operations.get(name, 0)) for name in OPERATIONS},
            "operation_policy_violations": dict(self.violations),
            "operation_policy_statuses": dict(self.violation_statuses),
            "cdc_operation_policy_violations": dict(self.cdc_violations),
            "incremental_data_failures": dict(self.data_failures),
            "classification_confidence": _dominant(self.confidence),
            "classification_confidence_counts": dict(self.confidence),
            "detection_method": _dominant(self.detection),
            "detection_method_counts": dict(self.detection),
            "historical_lookups": self.historical_lookups,
            "historical_rows_read": self.historical_rows_read,
            "out_of_window_source_rows": self.out_of_window_source_rows,
            "incremental_evidence_exported": self._evidence.exported,
            "incremental_evidence_limit_reached": self._evidence.limit_reached,
            "incremental_failure_samples": list(self._reasons),
            "validated_update_matches": self.validated_updates,
            "server_2_rows_checked": self.server_2_rows_checked,
            "server_2_matches": self.server_2_matches,
            **self.policy.as_details(),
        }


class HashOperationClassifier:
    """Operation classification for keyless (row-hash + occurrence) comparisons.

    A row hash is not a key: without a stable identifier the framework can
    prove REPLAY (the identical business row already existed in the historical
    target scope), can call a matched new row an INSERT candidate, and must
    report every current target occurrence it cannot trace to a source
    occurrence as UNKNOWN. UPDATE and DELETE are never guessed here.
    """

    def __init__(
        self, validator, policy: IncrementalPolicy, table_map, target_conn,
        target_fqn: str, target_dialect, historical_where: str,
        column_mappings: Sequence[Any], result_obj, batch_size: int,
    ) -> None:
        self._validator = validator
        self.policy = policy
        self._table_map = table_map
        self._table_id = getattr(table_map, "id", None)
        self._target_conn = target_conn
        self._target_fqn = target_fqn
        self._target_dialect = target_dialect
        self._historical_where = historical_where
        if column_mappings and isinstance(column_mappings[0], dict):
            self._mappings = [dict(item) for item in column_mappings]
        else:
            self._mappings = [
                {"source_column": str(column), "target_column": str(column)}
                for column in column_mappings
            ]
        self._columns = [item["target_column"] for item in self._mappings]
        self._batch_size = max(1, int(batch_size or 1))
        self._evidence = _EvidenceWriter(validator, result_obj, self._table_id)
        self._update_evidence = _UpdateEvidenceWriter(validator, result_obj)
        self._server_2_evidence = _Server2EvidenceWriter(result_obj)
        self.replay_counts: Counter = Counter()
        self._replay_samples: Dict[str, Dict[str, Any]] = {}
        self._replay_sample_limit = validator._configured_excess_limit("target")
        self.operations: Counter = Counter()
        self.violations: Counter = Counter()
        self.violation_statuses: Dict[str, str] = {}
        self.data_failures: Counter = Counter()
        self.historical_lookups = 0
        self.historical_rows_read = 0
        self.target_rows_observed = 0
        self.out_of_window_source_rows = 0
        self._reasons: List[str] = []
        self.validated_updates = 0
        self.server_2_rows_checked = 0
        self.server_2_matches = 0
        self.server_2_match_counts: Counter = Counter()

    @staticmethod
    def _payload_record(payload: Any) -> Dict[str, Any]:
        if not payload:
            return {}
        try:
            value = json.loads(payload) if isinstance(payload, str) else payload
            if isinstance(value, dict) and "native" in value:
                return pickle.loads(base64.b64decode(value["native"]))
            if isinstance(value, dict) and "evidence" in value:
                value = json.loads(value["evidence"])
            return value if isinstance(value, dict) else {}
        except Exception:
            return {}

    def _validate_update_excess(self, database, side: str) -> int:
        """Prove keyless mismatches from bounded discriminator history reads."""
        if (database is None or not hasattr(database, "iter_excess")
                or not self.policy.historical_check or not self._historical_where):
            return 0
        lookup = self._validator._sample_lookup_settings()
        pending = []
        resolved = 0

        def flush(items):
            nonlocal resolved
            if not items:
                return
            current_rows = []
            for row_hash, count, payload in items:
                native = self._payload_record(payload)
                if side == "source":
                    native = {
                        mapping["target_column"]: native.get(mapping["source_column"])
                        for mapping in self._mappings
                    }
                current_rows.append((row_hash, int(count), native))
            frame = pd.DataFrame([row for _, _, row in current_rows], columns=self._columns)
            if frame.empty:
                return
            same_side = [
                {"source_column": name, "target_column": name} for name in self._columns
            ]
            disc_source, disc_target, _ = self._validator._select_discriminator_columns(
                frame, same_side, lookup["discriminator_columns"]
            )
            values = self._validator._normalized_tuples(frame, disc_source)
            builder = self._validator._cohort_builder(
                self._target_conn, self._target_dialect, lookup["keyless_batch_size"]
            )
            projection = list(dict.fromkeys(
                self._columns + [name for name in (
                    self.policy.target_audit_column,
                    self.policy.target_watermark_column,
                ) if name]
            ))
            plan = builder.plan(
                self._target_fqn, projection, self._historical_where,
                disc_target, values,
            )
            candidates = []
            for candidate_frame in self._validator._iter_query_frames(
                self._target_conn, plan.queries, with_retry=True
            ):
                self.historical_lookups += 1
                self.historical_rows_read += len(candidate_frame)
                candidates.extend(candidate_frame.to_dict("records"))
            history_absent = []
            for row_hash, count, current in current_rows:
                matches = [
                    prior for prior in candidates
                    if all(self._validator._values_match(prior.get(c), current.get(c))
                           for c in disc_target)
                ]
                if not matches:
                    history_absent.append((row_hash, count, current))
                changed_candidates = []
                for prior in matches:
                    changed = [
                        {"column": column, "previous": prior.get(column),
                         "current": current.get(column)}
                        for column in self._columns
                        if not self._validator._values_match(
                            prior.get(column), current.get(column)
                        )
                    ]
                    if changed:
                        changed_candidates.append((prior, changed))
                if not changed_candidates:
                    identical = next((prior for prior in matches if all(
                        self._validator._values_match(
                            prior.get(column), current.get(column)
                        ) for column in self._columns
                    )), None)
                    if identical is not None:
                        self.replay_counts[row_hash] += count
                        if (
                            self._replay_sample_limit is None
                            or len(self._replay_samples) < self._replay_sample_limit
                        ):
                            sample = dict(current)
                            audit = self.policy.target_audit_column
                            sample["_historical_audit_value"] = (
                                identical.get(audit) if audit else None
                            )
                            self._replay_samples.setdefault(row_hash, sample)
                    continue
                prior, changed = changed_candidates[-1]
                resolved += count
                self.validated_updates += count
                reason_changes = "; ".join(
                    f"{item['column']} changed from {item['previous']!r} to {item['current']!r}"
                    for item in changed
                )
                reason = (
                    "Existing target record identified by configured discriminator; "
                    f"{reason_changes}."
                )
                self._update_evidence.write({
                    "table_id": self._table_id,
                    "source_table": f"{getattr(self._table_map, 'source_schema', '')}.{getattr(self._table_map, 'source_table', '')}",
                    "target_table": f"{getattr(self._table_map, 'target_schema', '')}.{getattr(self._table_map, 'target_table', '')}",
                    "resolved_incremental_strategy": self.policy.strategy,
                    "operation": "UPDATE", "status": "VALID UPDATE",
                    "failure_reason": reason, "row_hash": row_hash,
                    "primary_key": {c: current.get(c) for c in disc_target},
                    "mismatched_columns": changed,
                    "historical_audit_value": prior.get(self.policy.target_audit_column),
                    "target_audit_value": current.get(self.policy.target_audit_column),
                    "source_watermark_value": current.get(self.policy.source_watermark_column),
                })
                self.operations["update"] += count
            resolved += self._server_2_keyless_matches(history_absent)
        for item in database.iter_excess(side):
            pending.append(item)
            if len(pending) >= self._batch_size:
                flush(pending)
                pending = []
        flush(pending)
        return resolved

    def _server_2_keyless_matches(self, items) -> int:
        """Return occurrence matches from a bounded alternate-source cohort."""
        provider = getattr(self._validator, "_server_2_connection_provider", None)
        if not items or not callable(provider):
            return 0
        checked = sum(int(count) for _, count, _ in items)
        logger.info(
            "[SERVER_2_FALLBACK] table=%s enabled=true strategy=keyless "
            "unresolved_records=%d",
            self._table_id, checked,
        )
        connection = provider()
        dialect = self._validator._dialect(connection)
        fqn = self._validator._source_fqn_for_table(self._table_map, dialect)
        driver = pd.DataFrame([row for _, _, row in items], columns=self._columns)
        lookup = self._validator._sample_lookup_settings()
        timings = self._validator._new_sample_timings(
            len(driver), False, lookup, len(self._mappings)
        )
        reverse = [
            {**item, "source_column": item["target_column"],
             "target_column": item["source_column"]}
            for item in self._mappings
        ]
        source_columns = [item["source_column"] for item in self._mappings]
        self._validator._lookup_matching_counts = Counter()
        self._validator._keyless_sample_cohort(
            connection, dialect, fqn, source_columns,
            self._validator._projection(source_columns, dialect), "", reverse,
            driver, lookup, timings, lookup_side="server_2",
        )
        available = Counter(self._validator._lookup_matching_counts)
        found = 0
        for row_hash, count, _ in items:
            matched = min(int(count), int(available.get(row_hash, 0)))
            if matched:
                self.server_2_match_counts[row_hash] += matched
                found += matched
                self._server_2_evidence.write({
                    "table_id": self._table_id,
                    "source_table": (
                        f"{getattr(self._table_map, 'source_schema', '')}."
                        f"{getattr(self._table_map, 'source_table', '')}"
                    ),
                    "target_table": (
                        f"{getattr(self._table_map, 'target_schema', '')}."
                        f"{getattr(self._table_map, 'target_table', '')}"
                    ),
                    "server_2": getattr(self._table_map, "server_2", None),
                    "match_method": "ROW_HASH_DISCRIMINATOR",
                    "record_identifier": f"row_hash:{row_hash}",
                    "occurrences": matched,
                    "status": "MATCHED IN SERVER_2",
                    "source_values": None,
                    "target_values": dict(_),
                })
        self.server_2_rows_checked += checked
        self.server_2_matches += found
        logger.info(
            "[SERVER_2_FALLBACK] table=%s strategy=keyless checked=%d "
            "found=%d unresolved=%d",
            self._table_id, checked, found, checked - found,
        )
        return found

    @property
    def target_driven(self) -> bool:
        return self.policy.target_driven

    def observe_target_batch(self, frame: pd.DataFrame, hashes: Sequence[str]) -> None:
        """Count the current target batch; history is queried after mismatch resolution."""
        self.target_rows_observed += len(hashes)

    def finalize(self, database, matched: int, excess_source: int, excess_target: int) -> int:
        """Turn the multiset totals into operation counts and policy outcomes."""
        # Only current target rows absent from the source scope are eligible
        # for second-level UPDATE proof.
        mismatch_side = "target"
        resolved_updates = self._validate_update_excess(database, mismatch_side)
        excess_target = max(0, excess_target - resolved_updates)
        replay_total = int(sum(self.replay_counts.values()))
        matched_replayed = 0
        if self.replay_counts and database is not None:
            for row_hash, count, _, _ in database.iter_matched():
                if row_hash in self.replay_counts:
                    matched_replayed += min(int(count), int(self.replay_counts[row_hash]))
        insert = max(int(matched) - matched_replayed, 0)
        unknown = int(excess_target)
        # Absence from the historical hash lookup is not proof that retained
        # history is complete. This classifier has no configured retention or
        # CDC identity evidence with which to confirm an INSERT candidate.
        absence_proven = bool(
            self.policy.historical_absence_proves_insert
            and self.policy.historical_check
            and str(self._historical_where or "").strip()
        )
        if not absence_proven:
            unknown += insert
            insert = 0
        self.operations.update({"insert": insert, "replay": replay_total, "unknown": unknown})
        if excess_source:
            self.data_failures[HASH_OCCURRENCE_MISMATCH] += int(excess_source)
        if excess_target:
            self.data_failures[UNEXPECTED_TARGET_RECORD] += int(excess_target)
        for operation, count in (("insert", insert), ("replay", replay_total), ("unknown", unknown)):
            if count <= 0:
                continue
            status = self.policy.status_for(operation)
            if status == "pass":
                continue
            category = policy_violation_category(operation, "hash_lookup")
            self.violations[category] += count
            self.violation_statuses[category] = status
        if not self.policy.capture_evidence:
            return resolved_updates
        replay_status = self.policy.status_for("replay")
        if replay_status != "pass":
            for row_hash, count in self.replay_counts.items():
                sample = self._replay_samples.get(row_hash, {})
                reason = (
                    f"Row hash {row_hash} already existed in historical target data and was "
                    f"appended again in the current load ({count} occurrence(s)). Operation "
                    f"classified as REPLAY. Configured REPLAY policy is {replay_status.upper()}."
                )
                if len(self._reasons) < _REASON_COLUMN_LIMIT:
                    self._reasons.append(reason)
                written = self._evidence.write(
                    "target", f"row_hash:{row_hash}", "REPLAY_NOT_ALLOWED",
                    self._payload(
                        "REPLAY", "hash_lookup", row_hash, count, None, sample, reason,
                        replay_status.upper(), "REPLAY_NOT_ALLOWED", historical_found=True,
                    ),
                )
                if not written:
                    break
        unknown_status = self.policy.status_for("unknown")
        if unknown and unknown_status != "pass" and database is not None:
            for row_hash, excess, _ in database.iter_excess("target"):
                reason = (
                    f"Business-row hash {row_hash} has {int(excess)} occurrence(s) in the current "
                    "target load that cannot be traced to a source record in the incremental "
                    "scope. Operation classified as UNKNOWN (identity cannot be proven without a "
                    f"key or CDC). Configured UNKNOWN policy is {unknown_status.upper()}."
                )
                if len(self._reasons) < _REASON_COLUMN_LIMIT:
                    self._reasons.append(reason)
                written = self._evidence.write(
                    "target", f"row_hash:{row_hash}", "UNKNOWN_NOT_ALLOWED",
                    self._payload(
                        "UNKNOWN", "hash_lookup", row_hash, int(excess), 0, None, reason,
                        unknown_status.upper(), "UNKNOWN_NOT_ALLOWED",
                        historical_found=row_hash in self.replay_counts,
                    ),
                )
                if not written:
                    break
        return resolved_updates

    def _payload(
        self, operation, detection, row_hash, target_occurrences, source_occurrences,
        sample, reason, status, category, *, historical_found,
    ) -> Dict[str, Any]:
        sample = dict(sample or {})
        audit = self.policy.target_audit_column
        return {
            "table_id": self._table_id,
            "source_table": f"{getattr(self._table_map, 'source_schema', '')}.{getattr(self._table_map, 'source_table', '')}",
            "target_table": f"{getattr(self._table_map, 'target_schema', '')}.{getattr(self._table_map, 'target_table', '')}",
            "validation_name": "record_comparison",
            "resolved_incremental_strategy": self.policy.strategy,
            "target_behavior": self.policy.target_behavior,
            "operation": operation,
            "status": status,
            "failure_category": category,
            "failure_reason": reason,
            "detection_method": detection,
            "classification_confidence": "hash_based",
            "row_hash": row_hash,
            "target_occurrences": target_occurrences,
            "source_occurrences": source_occurrences,
            "historical_record_found": historical_found,
            "current_target_record_found": True,
            "historical_audit_value": sample.pop("_historical_audit_value", None),
            "target_audit_value": sample.get(audit) if audit else None,
            "target_values": {column: sample.get(column) for column in self._columns} if sample else None,
        }

    def summary(self) -> Dict[str, Any]:
        return {
            "operation_classification_performed": True,
            "classification_method": "row_hash_occurrence",
            "records_classified": self.target_rows_observed,
            "incremental_operations": {name: int(self.operations.get(name, 0)) for name in OPERATIONS},
            "operation_policy_violations": dict(self.violations),
            "operation_policy_statuses": dict(self.violation_statuses),
            # A keyless hash comparison has no per-record CDC event to attribute.
            "cdc_operation_policy_violations": {},
            "incremental_data_failures": dict(self.data_failures),
            "classification_confidence": "hash_based",
            "detection_method": "hash_lookup",
            "historical_lookups": self.historical_lookups,
            "historical_rows_read": self.historical_rows_read,
            "out_of_window_source_rows": self.out_of_window_source_rows,
            "incremental_evidence_exported": self._evidence.exported,
            "incremental_evidence_limit_reached": self._evidence.limit_reached,
            "incremental_failure_samples": list(self._reasons),
            "validated_update_matches": self.validated_updates,
            "server_2_rows_checked": self.server_2_rows_checked,
            "server_2_matches": self.server_2_matches,
            **self.policy.as_details(),
        }


def _dominant(counter: Counter) -> str:
    if not counter:
        return "none"
    if len(counter) == 1:
        return next(iter(counter))
    return "mixed"


def not_performed_details(policy: Optional[IncrementalPolicy], reason: str) -> Dict[str, Any]:
    """Details for a comparison whose operations were not classified."""
    details = {
        "operation_classification_performed": False,
        "classification_unavailable_reason": reason,
        "classification_confidence": "none",
        "incremental_operations": {name: 0 for name in OPERATIONS},
        "operation_policy_violations": {},
    }
    if policy is not None:
        details.update(policy.as_details())
    return details
