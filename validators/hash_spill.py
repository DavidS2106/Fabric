"""Local spill stores for streamed hash-count comparison.

Every store here holds the identical multiset of row hashes and computes the
identical matched/excess arithmetic:

    matched        = SUM(min(source_count, target_count))
    excess_source  = SUM(max(source_count - target_count, 0))
    excess_target  = SUM(max(target_count - source_count, 0))

Only the storage medium differs (SQLite file, in-memory SQLite, or DuckDB
file), so switching engines is a pure resource/performance decision.
"""

from __future__ import annotations

import importlib.util
import logging
import shutil
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

_BYTES_PER_MB = 1024 * 1024
_SQLITE_SIDECAR_SUFFIXES = ("-journal", "-wal", "-shm")

_CREATE_TABLE = (
    "CREATE TABLE row_counts (row_hash {text_type} PRIMARY KEY, "
    "source_count {int_type} NOT NULL DEFAULT 0, "
    "target_count {int_type} NOT NULL DEFAULT 0, "
    "source_payload {text_type}, target_payload {text_type}, "
    "source_resolved {int_type} NOT NULL DEFAULT 0)"
)


class SpillStoreUnavailableError(RuntimeError):
    """No configured spill engine can run in this environment."""


class _BaseHashSpillStore:
    """Shared upsert and portable-SQL behavior for every spill engine."""

    engine_name = "base"
    #: Two-argument scalar min/max, the only non-portable SQL used.
    _min_two = "MIN({a},{b})"
    _max_two = "MAX({a},{b})"

    def __init__(self, commit_interval_batches: int = 20) -> None:
        self._lock = threading.Lock()
        self._connection: Any = None
        self._commit_interval_batches = max(1, int(commit_interval_batches))
        self._pending_batches = 0
        self.write_seconds = 0.0
        self.commit_seconds = 0.0
        self.peak_size_mb = 0.0

    def _create_schema(self, text_type: str, int_type: str) -> None:
        self._connection.execute(
            _CREATE_TABLE.format(text_type=text_type, int_type=int_type)
        )

    def upsert_rows(
        self, side: str, rows: Sequence[Any]
    ) -> None:
        """Count a hash batch without retaining full record payloads."""
        count_column = f"{side}_count"
        payload_column = f"{side}_payload"
        statement = (
            f"INSERT INTO row_counts(row_hash,{count_column},{payload_column}) "
            "VALUES (?,?,?) ON CONFLICT(row_hash) DO UPDATE SET "
            f"{count_column}={count_column}+excluded.{count_column}, "
            f"{payload_column}=COALESCE({payload_column},excluded.{payload_column})"
        )
        # Collapse duplicate hashes within the incoming frame. Besides reducing
        # SQL calls this keeps the spill proportional to distinct rows, not rows.
        counts = {}
        for item in rows:
            row_hash = item[0] if isinstance(item, (tuple, list)) else item
            payload = item[1] if isinstance(item, (tuple, list)) and len(item) > 1 else None
            count, prior_payload = counts.get(row_hash, (0, None))
            counts[row_hash] = (count + 1, prior_payload or payload)
        if not counts:
            return
        started = time.perf_counter()
        with self._lock:
            try:
                self._connection.executemany(
                    statement,
                    ((row_hash, count, payload)
                     for row_hash, (count, payload) in counts.items()),
                )
                self._pending_batches += 1
                if self._pending_batches >= self._commit_interval_batches:
                    self._commit()
            except Exception:
                self._rollback()
                raise
            finally:
                self.write_seconds += time.perf_counter() - started
                self._update_peak_size()

    def finalize_batch(self) -> None:
        """Explicitly finalize pending writes (for batch mode).

        Commit the current transaction. Normal ingestion calls this once after
        all batches; the configured interval bounds transaction size.
        """
        with self._lock:
            self._commit()

    def upsert_counts(self, side: str, counts, payloads=None) -> None:
        """Add pre-aggregated hash counts without expanding occurrences."""
        payloads = payloads or {}
        count_column = f"{side}_count"
        payload_column = f"{side}_payload"
        statement = (
            f"INSERT INTO row_counts(row_hash,{count_column},{payload_column}) "
            "VALUES (?,?,?) ON CONFLICT(row_hash) DO UPDATE SET "
            f"{count_column}={count_column}+excluded.{count_column}, "
            f"{payload_column}=COALESCE({payload_column},excluded.{payload_column})"
        )
        values = [
            (row_hash, int(count), payloads.get(row_hash))
            for row_hash, count in counts.items() if int(count) > 0
        ]
        if not values:
            return
        with self._lock:
            self._connection.executemany(statement, values)
            self._pending_batches += 1
            if self._pending_batches >= self._commit_interval_batches:
                self._commit()

    def unresolved_target_rows(self, limit: int):
        """Yield bounded target representatives not yet looked up in source."""
        return list(self._iter_result(
            "SELECT row_hash,target_count,target_payload FROM row_counts "
            f"WHERE target_count > 0 AND source_resolved = 0 LIMIT {int(limit)}"
        ))

    def mark_source_resolved(self, hashes) -> None:
        values = [(row_hash,) for row_hash in hashes]
        if values:
            self._connection.executemany(
                "UPDATE row_counts SET source_resolved=1 WHERE row_hash=?", values
            )

    def _commit(self) -> None:
        if not self._pending_batches:
            return
        started = time.perf_counter()
        self._connection.commit()
        self.commit_seconds += time.perf_counter() - started
        self._pending_batches = 0

    def _rollback(self) -> None:
        try:
            self._connection.rollback()
        finally:
            self._pending_batches = 0

    def rollback(self) -> None:
        with self._lock:
            self._rollback()

    def _update_peak_size(self) -> None:
        """Track file growth for diagnostics; memory stores remain zero."""
        spill_file = getattr(self, "_spill_file", None)
        if spill_file is None:
            return
        try:
            size = Path(spill_file).stat().st_size / _BYTES_PER_MB
        except OSError:
            return
        self.peak_size_mb = max(self.peak_size_mb, size)

    def totals(self) -> Tuple[int, int, int]:
        """Return (matched, excess_source, excess_target) multiset counts."""
        min_counts = self._min_two.format(a="source_count", b="target_count")
        max_source = self._max_two.format(a="source_count-target_count", b="0")
        max_target = self._max_two.format(a="target_count-source_count", b="0")
        row = self._fetch_one(
            f"SELECT COALESCE(SUM({min_counts}),0), "
            f"COALESCE(SUM({max_source}),0), "
            f"COALESCE(SUM({max_target}),0) FROM row_counts"
        )
        return int(row[0]), int(row[1]), int(row[2])

    def iter_matched(self) -> Iterable[Tuple[str, int, Any, Any]]:
        """Yield (row_hash, matched_count, None, None) for compatibility."""
        min_counts = self._min_two.format(a="source_count", b="target_count")
        return self._iter_result(
            f"SELECT row_hash, {min_counts}, source_payload, target_payload "
            "FROM row_counts WHERE source_count > 0 AND target_count > 0"
        )

    def iter_excess(self, side: str) -> Iterable[Tuple[str, int, Any]]:
        """Yield (row_hash, excess_count, payload) rows for one side."""
        count_expression = (
            "source_count-target_count" if side == "source"
            else "target_count-source_count"
        )
        return self._iter_result(
            f"SELECT row_hash, {count_expression}, {side}_payload "
            f"FROM row_counts WHERE {count_expression} > 0"
        )

    def _execute(self, sql: str):
        return self._connection.execute(sql)

    def _iter_result(self, sql: str):
        result = self._execute(sql)
        try:
            yield from result
        finally:
            close = getattr(result, "close", None)
            if callable(close) and result is not self._connection:
                close()

    def _fetch_one(self, sql: str):
        result = self._connection.execute(sql)
        try:
            return result.fetchone()
        finally:
            close = getattr(result, "close", None)
            if callable(close) and result is not self._connection:
                close()

    def close(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is None:
            return
        try:
            connection.close()
        except Exception:  # pragma: no cover - close is best effort
            logger.debug("Spill store close failed", exc_info=True)

    def cleanup(self) -> None:
        """Remove any on-disk artifacts. No-op for memory stores."""


class SqliteHashSpillStore(_BaseHashSpillStore):
    """The existing SQLite spill file, tuned for scratch usage.

    Journal and fsync are disabled: this database never needs to survive a
    crash (it is deleted afterward), so durability work only costs time and
    doubles temporary disk usage.
    """

    engine_name = "sqlite"

    def __init__(
        self, spill_file: Optional[Path], commit_interval_batches: int = 20
    ) -> None:
        super().__init__(commit_interval_batches)
        self._spill_file = spill_file
        location = str(spill_file) if spill_file is not None else ":memory:"
        self._connection = sqlite3.connect(location, check_same_thread=False)
        try:
            if spill_file is not None:
                self._connection.execute("PRAGMA journal_mode=OFF")
                self._connection.execute("PRAGMA synchronous=OFF")
            self._create_schema("TEXT", "INTEGER")
        except Exception:
            self.close()
            raise

    def cleanup(self) -> None:
        if self._spill_file is not None:
            _remove_spill_artifacts(self._spill_file, include_sqlite_sidecars=True)


class MemoryHashSpillStore(SqliteHashSpillStore):
    """Pure in-memory variant with byte-identical SQL semantics."""

    engine_name = "memory"

    def __init__(self, commit_interval_batches: int = 20) -> None:
        super().__init__(
            spill_file=None, commit_interval_batches=commit_interval_batches
        )


class DuckDbHashSpillStore(_BaseHashSpillStore):
    """DuckDB spill file. Requires the optional ``duckdb`` package.

    LEAST/GREATEST are DuckDB's two-argument scalar min/max and return the
    identical integer results as SQLite's MIN/MAX on the stored counts.
    """

    engine_name = "duckdb"
    _min_two = "LEAST({a},{b})"
    _max_two = "GREATEST({a},{b})"

    def __init__(self, spill_file: Path, commit_interval_batches: int = 20) -> None:
        super().__init__(commit_interval_batches)
        import duckdb

        self._spill_file = spill_file
        self._connection = duckdb.connect(str(spill_file))
        try:
            self._create_schema("VARCHAR", "BIGINT")
        except Exception:
            self.close()
            raise

    def _execute(self, sql: str):
        # DuckDB results are not directly iterable like sqlite3 cursors;
        # stream them in bounded chunks so evidence limits stay bounded.
        result = self._connection.execute(sql)
        while True:
            rows = result.fetchmany(10_000)
            if not rows:
                return
            yield from rows

    def cleanup(self) -> None:
        _remove_spill_artifacts(self._spill_file)


def duckdb_available() -> bool:
    try:
        return importlib.util.find_spec("duckdb") is not None
    except (ImportError, ValueError, AttributeError):
        return False


def _free_space_mb(directory: Path) -> Optional[float]:
    try:
        return shutil.disk_usage(str(directory)).free / _BYTES_PER_MB
    except OSError:
        return None


def _remove_spill_artifacts(
    spill_file: Path, *, include_sqlite_sidecars: bool = False
) -> None:
    paths = [spill_file]
    if include_sqlite_sidecars:
        paths.extend(Path(f"{spill_file}{suffix}") for suffix in _SQLITE_SIDECAR_SUFFIXES)
    for path in paths:
        try:
            path.unlink(missing_ok=True)
        except OSError:  # pragma: no cover - best-effort cleanup during failure handling
            logger.debug("Could not remove spill artifact %s", path, exc_info=True)


def open_hash_spill_store(
    spill_directory: Path,
    hash_spill_config: Any = None,
    temporary_storage_config: Any = None,
    preferred_engine: Optional[str] = None,
    force_disk_spill: bool = False,
    commit_interval_batches: Optional[int] = None,
) -> _BaseHashSpillStore:
    """Open the first configured spill engine this environment can run.

    File-based engines additionally require the configured minimum free disk
    space; when the disk is too full they are skipped so a configured memory
    engine can still complete the identical comparison instead of failing
    mid-run with "database or disk is full".
    """
    engine = str(getattr(hash_spill_config, "engine", "auto") or "auto")
    commit_interval = int(
        commit_interval_batches
        if commit_interval_batches is not None
        else getattr(hash_spill_config, "commit_interval_batches", 20) or 20
    )
    priority: List[str] = list(
        getattr(hash_spill_config, "priority", None) or ("sqlite", "memory")
    )
    if force_disk_spill:
        disk_priority = [name for name in priority if name != "memory"]
        if "memory" in priority:
            disk_priority.append("memory")
        priority = disk_priority
    if preferred_engine is not None:
        preferred = str(preferred_engine)
        priority = [preferred] + [name for name in priority if name != preferred]
    elif engine != "auto":
        priority = [engine] + [name for name in priority if name != engine]

    check_enabled = bool(
        getattr(temporary_storage_config, "disk_space_check_enabled", False)
    )
    minimum_mb = int(
        getattr(temporary_storage_config, "minimum_free_space_mb", 0) or 0
    )
    disk_ok = True
    free_mb = None
    if check_enabled and minimum_mb > 0:
        free_mb = _free_space_mb(spill_directory)
        disk_ok = free_mb is None or free_mb >= minimum_mb

    skipped = []
    for name in priority:
        if name == "memory":
            return MemoryHashSpillStore(commit_interval)
        if not disk_ok:
            skipped.append(f"{name} (insufficient free disk: {free_mb:.0f} MB)")
            continue
        spill_file = spill_directory / f"record_hash_{uuid.uuid4().hex}"
        if name == "sqlite":
            sqlite_file = spill_file.with_suffix(".sqlite3")
            try:
                return SqliteHashSpillStore(sqlite_file, commit_interval)
            except Exception:
                _remove_spill_artifacts(sqlite_file, include_sqlite_sidecars=True)
                raise
        if name == "duckdb":
            if not duckdb_available():
                skipped.append("duckdb (package not installed)")
                continue
            duckdb_file = spill_file.with_suffix(".duckdb")
            try:
                return DuckDbHashSpillStore(duckdb_file, commit_interval)
            except Exception:
                _remove_spill_artifacts(duckdb_file)
                raise
        skipped.append(f"{name} (unsupported engine)")
    raise SpillStoreUnavailableError(
        "No configured hash spill engine is usable: " + "; ".join(skipped)
    )
