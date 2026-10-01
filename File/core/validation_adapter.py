"""Hand prepared File datasets to the existing validation framework.

This module is the whole bridge, and it contains no validation of its own.

* :class:`FileStagingStore` streams a dataset — one file, or many files
  logically consolidated — into a local relational store, in bounded chunks.
  Nothing is ever concatenated in memory.
* :class:`FileDatasetConnector` is an ordinary
  :class:`core.connection_manager.BaseConnector`. It answers the same
  ``execute_query`` the existing validators already issue, rendered through the
  ``sqlite`` profile in ``config/dialects/dialects.yml``. Row count, key
  validation, null validation, duplicate detection, record comparison,
  sampling, hashing, audit validation, evidence and reporting therefore run
  completely unchanged.
* :func:`build_table_mapping` produces a normal ``TableMapping`` through the
  existing ``core.config_loader`` normalizer, so a File table is validated by
  exactly the code path a database table is.
"""

from __future__ import annotations

import logging
import re
import shutil
import threading
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
from sqlalchemy import create_engine, text

from core.capabilities import RELATIONAL_QUERY_ENGINE_CAPABILITIES
from core.config_loader import (
    SourceConfig,
    TableMapping,
    TargetConfig,
    _normalize_table_config,
)
from core.connection_manager import BaseConnector, register_connector

logger = logging.getLogger(__name__)

#: Connection type the staging connector registers under.
FILE_DATASET_CONNECTION_TYPE = "file_dataset"
#: SQLite's own name for the database a connection opens directly. Used as the
#: schema of every staged dataset so a staged table has a real qualified name.
STAGING_SCHEMA = "main"
#: Dialect profile the staged datasets are read through.
STAGING_DIALECT = "sqlite"

_IDENTIFIER = re.compile(r"[^0-9A-Za-z_]")


class FileStagingError(RuntimeError):
    """Raised when a dataset cannot be staged for validation."""


class FileStagingResourceError(FileStagingError):
    """Raised when local storage cannot hold the staged dataset."""


# ── staging ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class StagedTable:
    """One dataset materialized for the validation engine to read."""

    name: str
    row_count: int
    columns: Tuple[str, ...]
    #: Per-source-file counts, preserved even when files were consolidated.
    contributions: Tuple[Tuple[str, int], ...] = ()


class FileStagingStore:
    """A local relational store holding one role's staged datasets."""

    def __init__(self, directory: Path, role: str, database_name: str):
        self.directory = Path(directory)
        self.role = role
        self.database_name = database_name
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / f"{role}.sqlite"
        self._engine = None
        self._lock = threading.Lock()
        self._names: Dict[str, int] = {}

    # ── lifecycle ─────────────────────────────────────────────────────

    @property
    def engine(self):
        if self._engine is None:
            with self._lock:
                if self._engine is None:
                    self._engine = create_engine(
                        f"sqlite:///{self.path.as_posix()}",
                        connect_args={"check_same_thread": False},
                        hide_parameters=True,
                    )
        return self._engine

    def close(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    def dispose(self) -> None:
        """Close and delete the whole store."""
        self.close()
        shutil.rmtree(self.directory, ignore_errors=True)

    # ── writing ───────────────────────────────────────────────────────

    @staticmethod
    def _is_disk_full_error(exc: BaseException) -> bool:
        """Recognize SQLite disk exhaustion through pandas/SQLAlchemy wrappers."""
        current: Optional[BaseException] = exc
        seen = set()
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            message = str(getattr(current, "orig", current)).casefold()
            if (
                "database or disk is full" in message
                or "disk full" in message
                or "sqlite_full" in message
            ):
                return True
            current = current.__cause__ or current.__context__
        return False

    def _write_frame(self, frame, table_name, connection, if_exists) -> None:
        try:
            frame.to_sql(
                table_name, connection, if_exists=if_exists, index=False
            )
        except Exception as exc:
            # pandas may raise pandas.errors.DatabaseError while SQLAlchemy
            # raises DBAPIError for the same sqlite3.OperationalError. Inspect
            # the wrapper chain instead of depending on one library's type.
            if self._is_disk_full_error(exc):
                raise FileStagingResourceError(
                    "Local staging disk is full while writing "
                    f"{self.path}. Remove stale data from {self.directory} or "
                    "set file_validation.staging.directory to a drive with "
                    "several GB of free space. Reports are not affected by "
                    "the staging directory."
                ) from exc
            raise

    def stage(
        self,
        preferred_name: str,
        frames: Iterable[pd.DataFrame],
        *,
        expected_columns: Optional[Sequence[str]] = None,
        contribution_key: Optional[str] = None,
    ) -> StagedTable:
        """Stream *frames* into one table and report what was written."""
        name = self._unique_name(preferred_name)
        columns: Tuple[str, ...] = tuple(expected_columns or ())
        row_count = 0
        contributions: Dict[str, int] = {}
        written = False
        with self.engine.begin() as connection:
            for frame in frames:
                if frame is None or frame.empty:
                    continue
                frame_columns = tuple(str(item) for item in frame.columns)
                if len(set(frame_columns)) != len(frame_columns):
                    raise FileStagingError(
                        f"{preferred_name}: a chunk repeats a column name; "
                        "duplicate columns cannot be validated"
                    )
                if not written:
                    columns = frame_columns
                elif frame_columns != columns:
                    raise FileStagingError(
                        f"{preferred_name}: chunk columns changed mid-stream "
                        f"({list(columns)} then {list(frame_columns)})"
                    )
                self._write_frame(
                    frame, name, connection,
                    "append" if written else "replace",
                )
                written = True
                row_count += len(frame)
                if contribution_key is not None:
                    contributions[contribution_key] = (
                        contributions.get(contribution_key, 0) + len(frame)
                    )
            if not written:
                if not columns:
                    raise FileStagingError(
                        f"{preferred_name} produced no rows and no column "
                        "order, so an empty dataset cannot be staged"
                    )
                self._write_frame(
                    pd.DataFrame(columns=list(columns)), name, connection,
                    "replace",
                )
        return StagedTable(
            name=name,
            row_count=row_count,
            columns=columns,
            contributions=tuple(contributions.items()),
        )

    def append(
        self, table: StagedTable, frames: Iterable[pd.DataFrame],
        contribution_key: str,
    ) -> StagedTable:
        """Add another file's rows to an existing staged table."""
        added = 0
        with self.engine.begin() as connection:
            for frame in frames:
                if frame is None or frame.empty:
                    continue
                frame_columns = tuple(str(item) for item in frame.columns)
                if frame_columns != table.columns:
                    raise FileStagingError(
                        f"{contribution_key}: columns {list(frame_columns)} do "
                        f"not match the dataset's {list(table.columns)}"
                    )
                self._write_frame(frame, table.name, connection, "append")
                added += len(frame)
        return replace(
            table,
            row_count=table.row_count + added,
            contributions=table.contributions + ((contribution_key, added),),
        )

    def row_count(self, name: str) -> int:
        with self.engine.connect() as connection:
            return int(
                connection.execute(text(f'SELECT COUNT(*) FROM "{name}"')).scalar_one()
            )

    # ── naming ────────────────────────────────────────────────────────

    def _unique_name(self, preferred: str) -> str:
        base = _IDENTIFIER.sub("_", str(preferred).strip()) or "dataset"
        if base[0].isdigit():
            base = f"t_{base}"
        base = base[:120]
        with self._lock:
            seen = self._names.get(base, 0)
            self._names[base] = seen + 1
        return base if seen == 0 else f"{base}__{seen}"


# ── connector ─────────────────────────────────────────────────────────


class FileDatasetConnector(BaseConnector):
    """Read a staged File dataset with the framework's ordinary SQL path."""

    SQL_DIALECT = STAGING_DIALECT
    BULK_SCHEMA_METADATA = False
    CAPABILITIES = RELATIONAL_QUERY_ENGINE_CAPABILITIES

    # ── connection ────────────────────────────────────────────────────

    def _database_path(self) -> Path:
        configured = getattr(self.cfg, "host", None)
        if not configured:
            raise ValueError(
                "A file dataset connection requires the staged store path"
            )
        return Path(str(configured))

    def _get_sqlalchemy_engine(self):
        if self._engine is None:
            with self._lock:
                if self._engine is None:
                    self._engine = create_engine(
                        f"sqlite:///{self._database_path().as_posix()}",
                        connect_args={"check_same_thread": False},
                        hide_parameters=True,
                    )
        return self._engine

    def _get_odbc_conn(self):  # pragma: no cover - the engine is always poolable
        return self._get_sqlalchemy_engine().raw_connection()

    def _apply_query_timeout(self, dbapi_connection: Any) -> Optional[int]:
        """A local store has no server-side statement timeout to apply."""
        return None

    def execute_query(self, sql: str) -> pd.DataFrame:
        parameters = tuple(getattr(sql, "params", ()) or ())
        statement = self._ensure_read_only_sql(sql)
        return self._read_sql_dataframe(statement, params=parameters or None)

    def test_connection(self) -> bool:
        try:
            self.last_error = None
            self.execute_connectivity_query()
            return True
        except Exception as exc:
            self.last_error = exc
            logger.error(
                "Staged file dataset unavailable: %s",
                self._masked_connection_error(exc),
            )
            return False

    # ── metadata ──────────────────────────────────────────────────────

    def get_current_database(self) -> Optional[str]:
        return getattr(self.cfg, "database", None)

    def schema_exists(self, schema: str) -> bool:
        return str(schema or STAGING_SCHEMA) == STAGING_SCHEMA

    def get_table_columns(self, schema: str, table: str) -> Dict[str, str]:
        rows = self._fetch_all(f'PRAGMA table_info("{_identifier(table)}")')
        return {str(row[1]): str(row[2] or "TEXT") for row in rows}

    def get_schema_table_columns(self, schema: str) -> Dict[str, Dict[str, str]]:
        names = [
            str(row[0]) for row in self._fetch_all(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        ]
        return {name: self.get_table_columns(schema, name) for name in names}

    def get_preflight_table_columns(
        self, schema: str, table_names: list
    ) -> Dict[str, set]:
        return {
            name: set(self.get_table_columns(schema, name))
            for name in table_names or []
        }


register_connector(FileDatasetConnector, FILE_DATASET_CONNECTION_TYPE)


def staging_connection(
    name: str, store: FileStagingStore, *, database_name: Optional[str] = None
) -> SourceConfig:
    """The connection configuration one staged store is reached through."""
    return SourceConfig(**_staging_fields(name, store, database_name))


def staging_target_connection(
    name: str, store: FileStagingStore, *, database_name: Optional[str] = None
) -> TargetConfig:
    """The same staged store, in the target role."""
    return TargetConfig(**_staging_fields(name, store, database_name))


def _staging_fields(
    name: str, store: FileStagingStore, database_name: Optional[str]
) -> Dict[str, Any]:
    return {
        "name": name,
        "type": FILE_DATASET_CONNECTION_TYPE,
        "enabled": True,
        # The store's path is this connection's endpoint, so two staged stores
        # never share one cached connector.
        "host": str(store.path),
        "database": database_name or store.database_name,
        "sql_dialect": STAGING_DIALECT,
    }


# ── table mapping ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class TargetBinding:
    """Where a File dataset's target lives, whichever kind it is."""

    system: str
    schema: str
    table: str
    database: Optional[str] = None
    server: Optional[str] = None
    #: Reporting-only description of the real target (file path or table).
    location: str = ""


def build_table_mapping(
    *,
    table_id: str,
    phase: str,
    layer: str,
    tags: Sequence[str],
    source_system: str,
    source_table: str,
    source_location: str,
    target: TargetBinding,
    key_mappings: Sequence[Mapping[str, str]],
    compare_column_mappings: Sequence[Mapping[str, str]],
    incremental: Mapping[str, Any],
) -> TableMapping:
    """Build one ``TableMapping`` through the existing normalizer."""
    raw: Dict[str, Any] = {
        "id": table_id,
        "source_system": source_system,
        "target_system": target.system,
        # `target_layer` is the field name the mapping carries; the loader
        # renames a table document's `layer` to it before this point.
        "target_layer": layer,
        "tags": list(tags),
        "source_server": source_location,
        "source_schema": STAGING_SCHEMA,
        "source_table": source_table,
        "target_server": target.server or target.location,
        "target_schema": target.schema,
        "target_table": target.table,
        "migration_phase": phase,
        "validation_phases": [phase],
        "key_mappings": [dict(item) for item in key_mappings],
        "compare_column_mappings": [dict(item) for item in compare_column_mappings],
    }
    if target.database:
        raw["target_database"] = target.database
    if incremental:
        raw["incremental"] = dict(incremental)
    normalized = _normalize_table_config(raw)
    fields = {item.name for item in TableMapping.__dataclass_fields__.values()}
    return TableMapping(**{
        key: value for key, value in normalized.items() if key in fields
    })


def business_column_mappings(
    source_columns: Sequence[str],
    target_columns: Sequence[str],
    audit_columns: Sequence[str],
) -> List[Dict[str, str]]:
    """Pair the columns a file and its target share, minus audit columns.

    ``audit_columns`` is the framework's own
    ``validations.audit_validation.audit_columns``; no audit column name is
    written here, and the audit validation itself remains untouched.
    """
    excluded = {str(item).casefold() for item in audit_columns}
    target_index = {
        str(column).casefold(): str(column) for column in target_columns
    }
    mappings: List[Dict[str, str]] = []
    for column in source_columns:
        key = str(column).casefold()
        if key in excluded or key not in target_index:
            continue
        mappings.append({
            "source_column": str(column),
            "target_column": target_index[key],
        })
    return mappings


def _identifier(value: str) -> str:
    return str(value).replace('"', '""')
