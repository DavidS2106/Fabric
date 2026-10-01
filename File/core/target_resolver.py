"""Route a File table to its target: a Lakehouse file, or a Fabric SQL table.

``fabric_lakehouse_file`` targets are read through :mod:`fabric_file_reader`
and staged like the source side, so the existing validators compare them with
the same SQL they use everywhere else.

``fabric_sql_table`` targets reuse the project's own Fabric connection. The
File configuration only *references* it by name; the server, endpoint, driver,
credentials and authentication are never repeated in ``File/config``.
"""

from __future__ import annotations

import glob as globlib
import logging
from dataclasses import dataclass, fields as dataclass_fields
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from core.config_loader import (
    ConfigLoader,
    MigrationConfig,
    SourceConfig,
    TargetConfig,
    _expand_env_vars,
    load_execution_document,
    load_yaml_document,
    normalize_connection_config,
    normalize_connection_document,
)

from .fabric_file_reader import FabricFileReader
from .file_config_loader import (
    FileConfigError,
    FileConnection,
    FileFrameworkSettings,
    FileTableConfig,
)

logger = logging.getLogger(__name__)

TARGET_LAKEHOUSE_FILE = "fabric_lakehouse_file"
TARGET_SQL_TABLE = "fabric_sql_table"

#: What a ``fabric_lakehouse`` connection must resolve before it can serve any
#: target. Everything else it may declare — ``local_mount_path``, ``database``,
#: ``driver``, ``file_storage.base_path`` — is optional.
REQUIRED_LAKEHOUSE_PARAMS = (
    "sql_endpoint",
    "workspace",
    "lakehouse",
    "authentication",
    "onelake_account_url",
)


@dataclass(frozen=True)
class LakehouseFileTarget:
    """A Fabric Lakehouse *file* target, compared file by file."""

    connection: FileConnection
    reader: FabricFileReader
    relative_path: str

    @property
    def kind(self) -> str:
        return TARGET_LAKEHOUSE_FILE


@dataclass(frozen=True)
class SqlTableTarget:
    """A Fabric SQL table target served by the project's own connection."""

    reference: str
    connection: TargetConfig
    database: str
    schema: str
    table: str

    @property
    def kind(self) -> str:
        return TARGET_SQL_TABLE

    @property
    def location(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"


class TargetResolver:
    """Resolve one File table's configured target into something readable."""

    def __init__(
        self,
        settings: FileFrameworkSettings,
        connections: Mapping[str, Mapping[str, FileConnection]],
        project_config: MigrationConfig,
        execution_config_path: Path,
        cache_directory: Path,
    ):
        self.settings = settings
        self.connections = connections
        self.project_config = project_config
        self.execution_config_path = Path(execution_config_path)
        self.cache_directory = Path(cache_directory)
        self._project_connections: Optional[Dict[str, Dict[str, Any]]] = None

    # ── entry point ───────────────────────────────────────────────────

    def resolve(self, table: FileTableConfig):
        connection = self._file_connection("target", table.target.connection)
        if connection.type and connection.type.strip() == "fabric_lakehouse":
            _require_lakehouse_params(connection, table)
        if table.target.kind == TARGET_LAKEHOUSE_FILE:
            if connection.connection_reference:
                raise FileConfigError(
                    f"{table.document.name}: a {TARGET_LAKEHOUSE_FILE} target "
                    f"cannot use connection '{connection.name}', which only "
                    "references a database connection"
                )
            return LakehouseFileTarget(
                connection=connection,
                reader=FabricFileReader(
                    connection, self.settings, self.cache_directory
                ),
                relative_path=str(table.target.relative_path or "").strip("/"),
            )

        reference = connection.connection_reference
        if reference:
            resolved = self.project_connection(reference)
        elif connection.param("sql_endpoint"):
            # The connection describes its own SQL endpoint — a lakehouse
            # serving both its OneLake files and its SQL analytics endpoint.
            # Nothing is duplicated: this is the one connection definition.
            reference = str(connection.name)
            resolved = _build_target(connection.name, {
                "type": connection.type, "params": dict(connection.params),
            })
        else:
            raise FileConfigError(
                f"{table.document.name}: a {TARGET_SQL_TABLE} target needs a "
                f"connection that either sets 'connection_reference' or "
                f"declares its own params.sql_endpoint; '{connection.name}' "
                "does neither"
            )
        return SqlTableTarget(
            reference=reference,
            connection=resolved,
            database=str(table.target.database),
            schema=str(table.target.schema),
            table=str(table.target.table),
        )

    # ── connections ───────────────────────────────────────────────────

    def _file_connection(self, role: str, name: str) -> FileConnection:
        available = self.connections.get(role, {})
        connection = available.get(name)
        if connection is None:
            raise FileConfigError(
                f"File table references unknown {role} connection '{name}'. "
                f"Available: {', '.join(sorted(available)) or 'none'}"
            )
        if not connection.enabled:
            raise FileConfigError(
                f"File table references disabled {role} connection '{name}'"
            )
        return connection

    def source_connection(self, name: str) -> FileConnection:
        return self._file_connection("source", name)

    def file_connection(self, role: str, name: str) -> FileConnection:
        """Return an enabled File connection without changing its role.

        This public accessor lets source ingestion reuse the existing OneLake
        connection while target resolution remains otherwise unchanged.
        """
        return self._file_connection(role, name)

    def project_connection(self, reference: str) -> TargetConfig:
        """The project's own connection named *reference*, never re-declared.

        The execution document's ``config_paths.connections`` glob is the same
        one :class:`core.config_loader.ConfigLoader` uses, so the File
        capability reads exactly the connections the rest of the framework
        does — from ``config/connections/*.yml`` and its ``.env``.
        """
        if reference == getattr(self.project_config, "target_ref", None):
            return self.project_config.target
        source = (self.project_config.sources or {}).get(reference)
        if source is not None:
            return _as_target(source)
        entry = self._discover_project_connections().get(reference)
        if entry is None:
            raise FileConfigError(
                f"File target connection_reference '{reference}' does not name "
                "a connection in the project's config/connections"
            )
        return _build_target(reference, entry)

    def _discover_project_connections(self) -> Dict[str, Dict[str, Any]]:
        if self._project_connections is not None:
            return self._project_connections
        document = load_execution_document(self.execution_config_path)
        pattern = (document.get("config_paths") or {}).get("connections")
        if not pattern:
            raise FileConfigError(
                "The execution configuration declares no "
                "config_paths.connections, so a File target "
                "connection_reference cannot be resolved"
            )
        project_root = ConfigLoader._project_root(self.execution_config_path)
        resolved = Path(pattern)
        if not resolved.is_absolute():
            resolved = project_root / resolved
        found: Dict[str, Dict[str, Any]] = {}
        for path in sorted(globlib.glob(str(resolved), recursive=True)):
            raw = _expand_env_vars(
                normalize_connection_document(load_yaml_document(Path(path)))
            )
            for section in ("sources", "targets"):
                for name, entry in (raw.get(section) or {}).items():
                    found[str(name)] = entry
        self._project_connections = found
        return found


def _require_lakehouse_params(
    connection: FileConnection, table: FileTableConfig
) -> None:
    """Fail with every missing required lakehouse param, not just the first."""
    missing = [
        name for name in REQUIRED_LAKEHOUSE_PARAMS
        if not connection.param(name)
    ]
    if missing:
        raise FileConfigError(
            f"{table.document.name}: File target connection "
            f"'{connection.name}' is missing required param(s) "
            + ", ".join(missing)
            + ". Each is unset, empty, or its environment variable is not "
            "defined."
        )


def _build_target(name: str, raw: Mapping[str, Any]) -> TargetConfig:
    normalized = normalize_connection_config(dict(raw))
    known = {item.name for item in dataclass_fields(TargetConfig)}
    values = {
        key: value for key, value in normalized.items()
        if key in known and key != "name"
    }
    return TargetConfig(name=normalized.get("name", name), **values)


def _as_target(source: SourceConfig) -> TargetConfig:
    known = {item.name for item in dataclass_fields(TargetConfig)}
    values = {
        key: getattr(source, key) for key in known if key != "name"
    }
    return TargetConfig(name=source.name, **values)
