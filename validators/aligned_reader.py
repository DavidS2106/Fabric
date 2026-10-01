"""
fabric_migration_validator/validators/aligned_reader.py
──────────────────────────────────────────────────────
One bounded, key-ordered read that serves every record-level check.

Full record comparison and the data-value checks both need the same thing: a
page of source rows in key order, plus the target rows carrying those keys.
Reading that twice doubles the cost of the heaviest part of a validation, so
this reader produces it once and hands each batch to every consumer.

Consumers stay completely independent: each keeps its own counters and produces
its own :class:`~core.models.CheckResult`. Sharing is physical only.

The target cohort is read through :mod:`validators.key_cohort`, so the cohort
strategy comes from configuration and connector capability rather than from a
hardcoded predicate shape.
"""

from __future__ import annotations

import contextlib
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple

import pandas as pd

from core.capabilities import resolve_capabilities
from core.instrumentation import ScanLedger
from core.sql_dialect import resolve_dialect
from .key_cohort import CohortPlan, KeyCohortBuilder, projection

logger = logging.getLogger(__name__)

#: Column added to every page so consumers align on identical key tokens.
MERGE_KEY = "_merge_key"


@dataclass
class AlignedBatch:
    """One page of source rows and the target rows carrying the same keys."""

    number: int
    source_page: pd.DataFrame
    target_page: pd.DataFrame
    source_tokens: List[Tuple[str, ...]]
    key_values: List[Tuple[Any, ...]]
    last_key: Tuple[Any, ...]
    cohort: Optional[CohortPlan] = None
    is_final: bool = False

    @property
    def source_rows(self) -> int:
        return len(self.source_page)

    @property
    def target_rows(self) -> int:
        return len(self.target_page)


@dataclass
class ReaderMetrics:
    """What the shared read cost, for the report."""

    source_batches: int = 0
    target_batches: int = 0
    source_rows: int = 0
    target_rows: int = 0
    batch_queries: int = 0
    retry_count: int = 0
    cohort_strategy: Optional[str] = None
    consumers: Tuple[str, ...] = ()
    shared: bool = False

    def as_details(self) -> Dict[str, Any]:
        return {
            "source_batches": self.source_batches,
            "target_batches": self.target_batches,
            "batch_queries": self.batch_queries,
            "key_cohort_strategy": self.cohort_strategy,
            "aligned_read_consumers": list(self.consumers),
            "shared_scan_used": self.shared,
        }


class AlignedBatchReader:
    """Key-ordered paged reader shared by every record-level consumer."""

    def __init__(
        self,
        source_conn: Any,
        target_conn: Any,
        source_fqn: str,
        target_fqn: str,
        where_clause: str,
        source_keys: Sequence[str],
        target_keys: Sequence[str],
        source_columns: Sequence[str],
        target_columns: Sequence[str],
        batch_size: int,
        key_tokens: Callable[[pd.DataFrame, Sequence[str]], List[Tuple[str, ...]]],
        extract_values: Callable[[pd.DataFrame, Sequence[str]], List[Tuple[Any, ...]]],
        canonical_value: Callable[[Any], str],
        literal: Callable[[Any], str],
        execute_query: Optional[Callable[[Any, str], pd.DataFrame]] = None,
        execute_queries: Optional[Callable[[Any, List[str]], pd.DataFrame]] = None,
        cohort_strategy: Optional[str] = None,
        cohort_chunk_size: Optional[int] = None,
        cohort_config: Any = None,
        scan_ledger: Optional[ScanLedger] = None,
        reuse_connections: bool = False,
    ) -> None:
        if not source_keys or not target_keys:
            raise ValueError("An aligned read requires configured key columns")
        if not (
            isinstance(batch_size, int)
            and not isinstance(batch_size, bool)
            and batch_size > 0
        ):
            raise ValueError("Aligned read batch size must be a positive integer")
        self._source_conn = source_conn
        self._target_conn = target_conn
        self._source_fqn = source_fqn
        self._target_fqn = target_fqn
        self._where = where_clause
        self._source_keys = list(source_keys)
        self._target_keys = list(target_keys)
        self._source_columns = list(source_columns)
        self._target_columns = list(target_columns)
        self._batch_size = batch_size
        self._key_tokens = key_tokens
        self._extract_values = extract_values
        self._canonical_value = canonical_value
        self._execute_query = execute_query or (
            lambda connection, sql: connection.execute_query(sql)
        )
        self._execute_queries = execute_queries
        self._ledger = scan_ledger
        self._reuse_connections = bool(reuse_connections)

        self._source_dialect = resolve_dialect(source_conn)
        self._target_dialect = resolve_dialect(target_conn)
        self.source_cohort = KeyCohortBuilder.resolve(
            self._source_dialect,
            resolve_capabilities(source_conn),
            cohort_strategy,
            cohort_chunk_size or batch_size,
            literal,
            cohort_config=cohort_config,
        )
        self.target_cohort = KeyCohortBuilder.resolve(
            self._target_dialect,
            resolve_capabilities(target_conn),
            cohort_strategy,
            cohort_chunk_size or batch_size,
            literal,
            cohort_config=cohort_config,
        )
        self.metrics = ReaderMetrics(cohort_strategy=self.target_cohort.strategy)

    # ── Statements, for the report ─────────────────────────────
    def source_page_sql(self, last_key: Optional[Sequence[Any]] = None) -> str:
        return self._page_sql(
            self._source_dialect,
            self.source_cohort,
            self._source_fqn,
            self._source_keys,
            self._source_columns,
            last_key,
        )

    def target_page_sql(self, last_key: Optional[Sequence[Any]] = None) -> str:
        return self._page_sql(
            self._target_dialect,
            self.target_cohort,
            self._target_fqn,
            self._target_keys,
            self._target_columns,
            last_key,
        )

    def _page_sql(
        self,
        dialect,
        cohort: KeyCohortBuilder,
        fqn: str,
        keys: Sequence[str],
        columns: Sequence[str],
        last_key: Optional[Sequence[Any]],
    ) -> str:
        where_clause = self._where
        if last_key is not None:
            from .key_cohort import combine_where

            where_clause = combine_where(
                where_clause, cohort.greater_than(keys, last_key)
            )
        return dialect.select(
            projection(list(keys) + list(columns), dialect),
            fqn,
            where_clause,
            order_by=", ".join(dialect.quote(key) for key in keys),
            limit=self._batch_size,
        )

    # ── Paging ────────────────────────────────────────────────
    def batches(self) -> Iterator[AlignedBatch]:
        """Yield every aligned batch covering the whole configured scope."""
        last_key: Optional[Tuple[Any, ...]] = None
        last_token: Optional[Tuple[str, ...]] = None
        number = 0
        with self._reader_session(self._source_conn) as reader:
            while True:
                sql = self.source_page_sql(last_key)
                source_page = self._read(reader, sql)
                if source_page.empty:
                    break
                number += 1
                self.metrics.source_batches = number
                self.metrics.source_rows += len(source_page)
                self._require_columns(
                    source_page,
                    self._source_keys + self._source_columns,
                    "source aligned",
                )
                key_values = self._extract_values(source_page, self._source_keys)
                source_page = source_page.copy()
                source_page[MERGE_KEY] = self._key_tokens(
                    source_page, self._source_keys
                )
                target_page, cohort = self._read_cohort(key_values, last_key)
                if not target_page.empty:
                    self._require_columns(
                        target_page,
                        self._target_keys + self._target_columns,
                        "target aligned",
                    )
                    target_page = target_page.copy()
                    target_page[MERGE_KEY] = self._key_tokens(
                        target_page, self._target_keys
                    )
                else:
                    target_page = target_page.copy()
                    target_page[MERGE_KEY] = pd.Series(dtype=object)
                self.metrics.target_rows += len(target_page)
                self.metrics.target_batches += 1

                next_key = key_values[-1]
                next_token = tuple(
                    self._canonical_value(value) for value in next_key
                )
                if next_token == last_token:
                    raise RuntimeError(
                        "Aligned keyset pagination did not advance"
                    )
                is_final = len(source_page) < self._batch_size
                yield AlignedBatch(
                    number=number,
                    source_page=source_page,
                    target_page=target_page,
                    source_tokens=list(source_page[MERGE_KEY]),
                    key_values=key_values,
                    last_key=next_key,
                    cohort=cohort,
                    is_final=is_final,
                )
                last_key = next_key
                last_token = next_token
                if is_final:
                    break

    def target_only_keys(
        self, matched_tokens: Any, last_source_key: Optional[Sequence[Any]]
    ) -> Iterator[AlignedBatch]:
        """Page the target and read the source keys carrying the same keys.

        Used to prove that no target record exists without a source record when
        the configured reverse-pass policy requires the explicit check.
        """
        last_key: Optional[Tuple[Any, ...]] = None
        last_token: Optional[Tuple[str, ...]] = None
        number = 0
        with self._reader_session(self._target_conn) as reader:
            while True:
                sql = self.target_page_sql(last_key)
                target_page = self._read(reader, sql)
                if target_page.empty:
                    break
                number += 1
                self._require_columns(target_page, self._target_keys, "target key")
                key_values = self._extract_values(target_page, self._target_keys)
                target_page = target_page.copy()
                target_page[MERGE_KEY] = self._key_tokens(
                    target_page, self._target_keys
                )
                source_page, cohort = self._read_reverse_cohort(key_values, last_key)
                if not source_page.empty:
                    source_page = source_page.copy()
                    source_page[MERGE_KEY] = self._key_tokens(
                        source_page, self._source_keys
                    )
                else:
                    source_page = source_page.copy()
                    source_page[MERGE_KEY] = pd.Series(dtype=object)

                next_key = key_values[-1]
                next_token = tuple(
                    self._canonical_value(value) for value in next_key
                )
                if next_token == last_token:
                    raise RuntimeError(
                        "Aligned reverse keyset pagination did not advance"
                    )
                is_final = len(target_page) < self._batch_size
                yield AlignedBatch(
                    number=number,
                    source_page=source_page,
                    target_page=target_page,
                    source_tokens=list(target_page[MERGE_KEY]),
                    key_values=key_values,
                    last_key=next_key,
                    cohort=cohort,
                    is_final=is_final,
                )
                last_key = next_key
                last_token = next_token
                if is_final:
                    break

    # ── Internals ─────────────────────────────────────────────
    @contextlib.contextmanager
    def _reader_session(self, connection: Any):
        """Lease one connection for the sequential page reader when allowed."""
        session = getattr(connection, "query_session", None)
        if self._reuse_connections and callable(session):
            with session() as leased:
                yield leased
            return
        yield connection

    def _read_cohort(
        self,
        key_values: List[Tuple[Any, ...]],
        lower_bound: Optional[Sequence[Any]],
    ) -> Tuple[pd.DataFrame, CohortPlan]:
        plan = self.target_cohort.plan(
            self._target_fqn,
            list(self._target_keys) + list(self._target_columns),
            self._where,
            self._target_keys,
            key_values,
            lower_bound=lower_bound,
            upper_bound=key_values[-1] if key_values else None,
        )
        return self._read_plan(self._target_conn, plan), plan

    def _read_reverse_cohort(
        self,
        key_values: List[Tuple[Any, ...]],
        lower_bound: Optional[Sequence[Any]],
    ) -> Tuple[pd.DataFrame, CohortPlan]:
        plan = self.source_cohort.plan(
            self._source_fqn,
            list(self._source_keys),
            self._where,
            self._source_keys,
            key_values,
            lower_bound=lower_bound,
            upper_bound=key_values[-1] if key_values else None,
        )
        return self._read_plan(self._source_conn, plan), plan

    def _read_plan(self, connection: Any, plan: CohortPlan) -> pd.DataFrame:
        if not plan.queries:
            return pd.DataFrame()
        if len(plan.queries) > 1 and self._execute_queries is not None:
            self.metrics.batch_queries += len(plan.queries)
            return self._execute_queries(connection, list(plan.queries))
        frames = [self._read(connection, sql) for sql in plan.queries]
        if len(frames) == 1:
            return frames[0]
        return pd.concat(frames, ignore_index=True)

    def _read(self, connection: Any, sql: str) -> pd.DataFrame:
        self.metrics.batch_queries += 1
        frame = self._execute_query(connection, sql)
        if not isinstance(frame, pd.DataFrame):
            raise TypeError("Connector execute_query must return a pandas DataFrame")
        return frame

    @staticmethod
    def _require_columns(
        frame: pd.DataFrame, columns: Sequence[str], purpose: str
    ) -> None:
        missing = [column for column in columns if column not in frame.columns]
        if missing:
            raise ValueError(f"Missing {purpose} columns: {missing}")


class AlignedReadCoordinator:
    """Drive one shared aligned read across several independent consumers."""

    def __init__(self, reader: AlignedBatchReader) -> None:
        self._reader = reader
        self._consumers: List[Tuple[str, Any]] = []

    def register(self, name: str, consumer: Any) -> None:
        """Register one consumer exposing ``consume(batch)``."""
        self._consumers.append((name, consumer))

    @property
    def consumer_names(self) -> Tuple[str, ...]:
        return tuple(name for name, _ in self._consumers)

    def run(self) -> ReaderMetrics:
        """Read every batch once and offer it to every registered consumer."""
        if not self._consumers:
            return self._reader.metrics
        self._reader.metrics.consumers = self.consumer_names
        self._reader.metrics.shared = len(self._consumers) > 1
        for batch in self._reader.batches():
            for name, consumer in self._consumers:
                consumer.consume(batch)
                if getattr(consumer, "stop_requested", False):
                    logger.debug("Consumer %s requested an early stop", name)
                    return self._reader.metrics
        return self._reader.metrics
