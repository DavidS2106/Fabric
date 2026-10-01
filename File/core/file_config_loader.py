"""Read every File-capability configuration document.

Three documents, and nothing else:

* ``File/config/config_setting.yml``   global File-framework behaviour,
* ``File/config/connection/*.yml``     source and target connections,
* ``File/config/table/*.yml``          one document per logical table.

Nothing here decides which validations run. That stays in
``config/validations/*.yml``, which the existing framework already owns.

YAML loading, duplicate-key rejection and ``${ENV}`` expansion are the
existing loader's, so a File document is parsed by exactly the same rules as
every other configuration document in the project.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from dotenv import load_dotenv

from core.config_loader import (
    _expand_env_vars,
    load_yaml_document,
    normalize_connection_document,
)


class FileConfigError(ValueError):
    """Raised for any invalid or missing File-capability configuration."""


#: Folder layout of the File capability, relative to its module root.
SETTINGS_DOCUMENT = "config_setting.yml"
CONNECTION_FOLDER = "connection"
TABLE_FOLDER = "table"
ENVIRONMENT_FILE = ".env"

COMPARISON_MODES = ("file_to_file", "files_to_table")
TARGET_KINDS = ("fabric_lakehouse_file", "fabric_sql_table")
MATCH_STRATEGIES = ("same_file_stem", "same_file_name", "relative_path")
MISSING_TARGET_ACTIONS = ("fail", "skip", "warn")
MULTIPLE_TARGET_ACTIONS = ("fail", "skip", "warn")
COLUMN_TYPE_POLICIES = ("as_read", "text")
COMPATIBILITY_MODES = ("strict", "ordered", "subset")


# ── settings ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class DelimiterPolicy:
    """How a delimited file's separator is decided. Never assumed."""

    auto_detect: bool
    sample_bytes: int
    sample_lines: int
    on_ambiguous: str
    on_undetected: str
    candidates: Tuple[Tuple[str, str], ...]

    @property
    def values(self) -> Tuple[str, ...]:
        return tuple(value for _, value in self.candidates)

    def name_of(self, delimiter: str) -> str:
        for name, value in self.candidates:
            if value == delimiter:
                return name
        return "configured"


@dataclass(frozen=True)
class DiscoveryPolicy:
    recursive: bool
    ignore_hidden: bool
    follow_symlinks: bool
    on_no_files: str


@dataclass(frozen=True)
class ArchivePolicy:
    enabled: bool
    nested_zip: bool
    max_depth: int
    max_members: int
    max_uncompressed_bytes: int
    extract_directory: str
    retain: bool


@dataclass(frozen=True)
class HeaderPolicy:
    configurable: bool
    allow_headerless_files: bool
    reuse_schema_from_first_headed_file: bool
    on_missing_schema: str


@dataclass(frozen=True)
class StagingPolicy:
    directory: str
    chunk_rows: int
    retain_after_run: bool
    column_types: str


@dataclass(frozen=True)
class OutputPolicy:
    base_path: str
    reports: str
    evidence: str
    reconciliation: str
    reconciliation_formats: Tuple[str, ...]

    def reports_path(self, project_root: Path) -> Path:
        return _under(project_root, self.base_path) / self.reports

    def evidence_path(self, project_root: Path) -> Path:
        return _under(project_root, self.base_path) / self.evidence

    def reconciliation_path(self, project_root: Path) -> Path:
        return _under(project_root, self.base_path) / self.reconciliation


@dataclass(frozen=True)
class FileFrameworkSettings:
    """``File/config/config_setting.yml``, validated."""

    enabled: bool
    supported_files: Tuple[str, ...]
    delimiters: DelimiterPolicy
    discovery: DiscoveryPolicy
    archive: ArchivePolicy
    header: HeaderPolicy
    comparison_modes: Tuple[str, ...]
    phases: Tuple[str, ...]
    staging: StagingPolicy
    output: OutputPolicy
    existing_framework: Mapping[str, Any]

    def supports(self, extension: str) -> bool:
        return extension.lower().lstrip(".") in self.supported_files


# ── connections ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class FileConnection:
    """One entry of ``File/config/connection/{source,target}.yml``."""

    name: str
    role: str
    enabled: bool
    type: Optional[str] = None
    params: Mapping[str, Any] = field(default_factory=dict)
    file_storage: Mapping[str, Any] = field(default_factory=dict)
    connection_reference: Optional[str] = None

    def param(self, name: str, *, required: bool = False) -> Optional[str]:
        value = self.params.get(name)
        text = None if value is None else str(value).strip()
        if not text or text.startswith("${"):
            if required:
                raise FileConfigError(
                    f"File {self.role} connection '{self.name}' requires "
                    f"params.{name}; it is unset or its environment variable "
                    "is not defined"
                )
            return None
        return text


# ── tables ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class FileSourceSpec:
    connection: str
    paths: Mapping[str, str]
    pattern: str
    format: str
    options: Mapping[str, Any]
    format_options: Mapping[str, Mapping[str, Any]]
    consolidation: Mapping[str, Any]

    def path_for(self, phase: str) -> Optional[str]:
        return self.paths.get(phase)

    def options_for(self, resolved_format: str) -> Dict[str, Any]:
        merged = dict(self.options)
        merged.update(self.format_options.get(resolved_format, {}) or {})
        return merged


@dataclass(frozen=True)
class FileTargetSpec:
    kind: str
    connection: str
    relative_path: Optional[str] = None
    matching: Mapping[str, Any] = field(default_factory=dict)
    format: Optional[str] = None
    options: Mapping[str, Any] = field(default_factory=dict)
    format_options: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    database: Optional[str] = None
    schema: Optional[str] = None
    table: Optional[str] = None

    def options_for(self, resolved_format: str) -> Dict[str, Any]:
        merged = dict(self.options)
        key = str(resolved_format).lower().lstrip(".")
        merged.update(self.format_options.get(key, {}) or {})
        return merged


@dataclass(frozen=True)
class FileTableConfig:
    """One ``File/config/table/*.yml`` document, validated."""

    table_id: str
    logical_table_name: str
    comparison_mode: str
    layer: str
    tags: Tuple[str, ...]
    source: FileSourceSpec
    target: FileTargetSpec
    key_mappings: Tuple[Mapping[str, str], ...]
    compare_column_mappings: Tuple[Mapping[str, str], ...]
    incremental: Mapping[str, Any]
    document: Path

    @property
    def phases(self) -> Tuple[str, ...]:
        return tuple(self.source.paths)


# ── loader ────────────────────────────────────────────────────────────


class FileConfigLoader:
    """Load and validate every File-capability configuration document."""

    def __init__(self, module_root: Path, project_root: Path):
        self.module_root = Path(module_root)
        self.project_root = Path(project_root)
        self.config_root = self.module_root / "config"

    # ── entry points ──────────────────────────────────────────────────

    @classmethod
    def for_project(cls, project_root: Path) -> "FileConfigLoader":
        """The loader for the ``File`` module shipped inside *project_root*."""
        return cls(Path(project_root) / "File", Path(project_root))

    def available(self) -> bool:
        """Whether a File configuration exists at all."""
        return (self.config_root / SETTINGS_DOCUMENT).is_file()

    def load_environment(self) -> None:
        """Load ``File/config/connection/.env``, never overriding the process."""
        env_path = self.config_root / CONNECTION_FOLDER / ENVIRONMENT_FILE
        if env_path.is_file():
            load_dotenv(env_path, override=False)

    def load_settings(self) -> FileFrameworkSettings:
        document = self._document(self.config_root / SETTINGS_DOCUMENT)
        section = document.get("file_validation")
        if not isinstance(section, dict):
            raise FileConfigError(
                f"{SETTINGS_DOCUMENT} must declare a file_validation mapping"
            )
        return _build_settings(section)

    def load_connections(self) -> Dict[str, Dict[str, FileConnection]]:
        """Source and target connections, keyed by role then by name."""
        folder = self.config_root / CONNECTION_FOLDER
        if not folder.is_dir():
            raise FileConfigError(f"Missing File connection folder: {folder}")
        resolved: Dict[str, Dict[str, FileConnection]] = {
            "source": {}, "target": {}
        }
        documents = sorted(folder.glob("*.y*ml"))
        if not documents:
            raise FileConfigError(f"No connection documents found in {folder}")
        for path in documents:
            raw = normalize_connection_document(self._document(path))
            for role, section in (("source", "sources"), ("target", "targets")):
                for name, entry in (raw.get(section) or {}).items():
                    if name in resolved[role]:
                        raise FileConfigError(
                            f"Duplicate File {role} connection '{name}'"
                        )
                    resolved[role][str(name)] = _build_connection(
                        str(name), role, entry, path
                    )
        return resolved

    def load_tables(self) -> List[FileTableConfig]:
        folder = self.config_root / TABLE_FOLDER
        if not folder.is_dir():
            return []
        tables: List[FileTableConfig] = []
        seen: Dict[str, Path] = {}
        for path in sorted(folder.glob("*.y*ml")):
            document = self._document(path)
            section = document.get("table")
            if not isinstance(section, dict):
                raise FileConfigError(
                    f"{path.name} must declare a 'table' mapping"
                )
            table = _build_table(section, path)
            if table.table_id in seen:
                raise FileConfigError(
                    f"Duplicate File table_id '{table.table_id}' in "
                    f"{path.name} and {seen[table.table_id].name}"
                )
            seen[table.table_id] = path
            tables.append(table)
        return tables

    # ── internals ─────────────────────────────────────────────────────

    def _document(self, path: Path) -> Dict[str, Any]:
        if not path.is_file():
            raise FileConfigError(f"Missing File configuration document: {path}")
        return _expand_env_vars(load_yaml_document(path))


# ── builders ──────────────────────────────────────────────────────────


def _build_settings(raw: Mapping[str, Any]) -> FileFrameworkSettings:
    delimiters = _require_mapping(raw, "delimiters", "file_validation")
    candidates = delimiters.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        raise FileConfigError(
            "file_validation.delimiters.candidates must be a non-empty list"
        )
    resolved: List[Tuple[str, str]] = []
    seen_values = set()
    for index, candidate in enumerate(candidates):
        where = f"file_validation.delimiters.candidates[{index}]"
        if not isinstance(candidate, dict):
            raise FileConfigError(f"{where} must be a YAML mapping")
        name = _require_text(candidate, "name", where)
        value = candidate.get("value")
        if not isinstance(value, str) or len(value) != 1:
            raise FileConfigError(
                f"{where}.value must be a single character"
            )
        if value in seen_values:
            raise FileConfigError(f"{where}.value duplicates an earlier candidate")
        seen_values.add(value)
        resolved.append((name, value))

    supported = raw.get("supported_files")
    if not isinstance(supported, list) or not supported:
        raise FileConfigError(
            "file_validation.supported_files must be a non-empty list"
        )

    modes = tuple(str(item) for item in (raw.get("comparison_modes") or ()))
    unknown_modes = sorted(set(modes) - set(COMPARISON_MODES))
    if unknown_modes:
        raise FileConfigError(
            "file_validation.comparison_modes contains unsupported mode(s): "
            + ", ".join(unknown_modes)
        )

    discovery = _require_mapping(raw, "discovery", "file_validation")
    archive = _require_mapping(raw, "archive", "file_validation")
    header = _require_mapping(raw, "header", "file_validation")
    staging = _require_mapping(raw, "staging", "file_validation")
    output = _require_mapping(raw, "output", "file_validation")

    column_types = str(staging.get("column_types", "as_read"))
    if column_types not in COLUMN_TYPE_POLICIES:
        raise FileConfigError(
            "file_validation.staging.column_types must be one of: "
            + ", ".join(COLUMN_TYPE_POLICIES)
        )

    return FileFrameworkSettings(
        enabled=_require_bool(raw, "enabled", "file_validation"),
        supported_files=tuple(
            str(item).lower().lstrip(".") for item in supported
        ),
        delimiters=DelimiterPolicy(
            auto_detect=_require_bool(
                delimiters, "auto_detect", "file_validation.delimiters"
            ),
            sample_bytes=_positive_int(
                delimiters, "sample_bytes", "file_validation.delimiters"
            ),
            sample_lines=_positive_int(
                delimiters, "sample_lines", "file_validation.delimiters"
            ),
            on_ambiguous=_choice(
                delimiters, "on_ambiguous", ("error", "first_match"),
                "file_validation.delimiters",
            ),
            on_undetected=_choice(
                delimiters, "on_undetected", ("error", "first_match"),
                "file_validation.delimiters",
            ),
            candidates=tuple(resolved),
        ),
        discovery=DiscoveryPolicy(
            recursive=_require_bool(
                discovery, "recursive", "file_validation.discovery"
            ),
            ignore_hidden=_require_bool(
                discovery, "ignore_hidden", "file_validation.discovery"
            ),
            follow_symlinks=_require_bool(
                discovery, "follow_symlinks", "file_validation.discovery"
            ),
            on_no_files=_choice(
                discovery, "on_no_files", ("fail", "skip"),
                "file_validation.discovery",
            ),
        ),
        archive=ArchivePolicy(
            enabled=_require_bool(archive, "enabled", "file_validation.archive"),
            nested_zip=_require_bool(
                archive, "nested_zip", "file_validation.archive"
            ),
            max_depth=_positive_int(archive, "max_depth", "file_validation.archive"),
            max_members=_positive_int(
                archive, "max_members", "file_validation.archive"
            ),
            max_uncompressed_bytes=_positive_int(
                archive, "max_uncompressed_bytes", "file_validation.archive"
            ),
            extract_directory=_require_text(
                archive, "extract_directory", "file_validation.archive"
            ),
            retain=_require_bool(archive, "retain", "file_validation.archive"),
        ),
        header=HeaderPolicy(
            configurable=_require_bool(
                header, "configurable", "file_validation.header"
            ),
            allow_headerless_files=_require_bool(
                header, "allow_headerless_files", "file_validation.header"
            ),
            reuse_schema_from_first_headed_file=_require_bool(
                header, "reuse_schema_from_first_headed_file",
                "file_validation.header",
            ),
            on_missing_schema=_choice(
                header, "on_missing_schema", ("fail", "skip"),
                "file_validation.header",
            ),
        ),
        comparison_modes=modes or COMPARISON_MODES,
        phases=tuple(str(item) for item in (raw.get("phases") or ())),
        staging=StagingPolicy(
            directory=_require_text(
                staging, "directory", "file_validation.staging"
            ),
            chunk_rows=_positive_int(
                staging, "chunk_rows", "file_validation.staging"
            ),
            retain_after_run=_require_bool(
                staging, "retain_after_run", "file_validation.staging"
            ),
            column_types=column_types,
        ),
        output=OutputPolicy(
            base_path=_require_text(output, "base_path", "file_validation.output"),
            reports=_require_text(output, "reports", "file_validation.output"),
            evidence=_require_text(output, "evidence", "file_validation.output"),
            reconciliation=_require_text(
                output, "reconciliation", "file_validation.output"
            ),
            reconciliation_formats=tuple(
                str(item).lower()
                for item in (output.get("reconciliation_formats") or ("json",))
            ),
        ),
        existing_framework=dict(raw.get("existing_framework") or {}),
    )


def _build_connection(
    name: str, role: str, raw: Any, document: Path
) -> FileConnection:
    if not isinstance(raw, dict):
        raise FileConfigError(
            f"{document.name}: connection '{name}' must be a YAML mapping"
        )
    reference = raw.get("connection_reference")
    connection_type = raw.get("type")
    if reference is None and not isinstance(connection_type, str):
        raise FileConfigError(
            f"{document.name}: connection '{name}' requires either 'type' or "
            "'connection_reference'"
        )
    if reference is not None and connection_type is not None:
        raise FileConfigError(
            f"{document.name}: connection '{name}' must declare either 'type' "
            "or 'connection_reference', never both"
        )
    return FileConnection(
        name=name,
        role=role,
        enabled=bool(raw.get("enabled", True)),
        type=str(connection_type) if connection_type else None,
        params=dict(raw.get("params") or {}),
        file_storage=dict(raw.get("file_storage") or {}),
        connection_reference=(
            str(reference).strip() if reference is not None else None
        ),
    )


def _build_table(raw: Mapping[str, Any], document: Path) -> FileTableConfig:
    where = document.name
    table_id = _require_text(raw, "table_id", where)
    logical_name = _require_text(raw, "logical_table_name", where)
    mode = _choice(raw, "comparison_mode", COMPARISON_MODES, where)

    source_raw = _require_mapping(raw, "source", where)
    target_raw = _require_mapping(raw, "target", where)

    paths = source_raw.get("paths")
    if not isinstance(paths, dict) or not paths:
        raise FileConfigError(
            f"{where}: source.paths must map at least one phase to a folder"
        )
    for phase, relative in paths.items():
        if not isinstance(relative, str) or not relative.strip():
            raise FileConfigError(
                f"{where}: source.paths.{phase} must be a non-empty folder"
            )
        if Path(relative).is_absolute():
            raise FileConfigError(
                f"{where}: source.paths.{phase} must be relative to the "
                "configured source root"
            )

    file_raw = _require_mapping(source_raw, "file", f"{where}.source")
    consolidation = dict(source_raw.get("consolidation") or {})
    if mode == "file_to_file" and consolidation.get("enabled"):
        raise FileConfigError(
            f"{where}: consolidation is not permitted in file_to_file mode; "
            "each source file is validated on its own"
        )
    compatibility = str(consolidation.get("compatibility", "strict"))
    if compatibility not in COMPATIBILITY_MODES:
        raise FileConfigError(
            f"{where}: source.consolidation.compatibility must be one of: "
            + ", ".join(COMPATIBILITY_MODES)
        )
    consolidation["compatibility"] = compatibility

    source = FileSourceSpec(
        connection=_require_text(source_raw, "connection", f"{where}.source"),
        paths={str(k): str(v).strip() for k, v in paths.items()},
        pattern=_require_text(file_raw, "pattern", f"{where}.source.file"),
        format=str(file_raw.get("format", "auto")).lower(),
        options=dict(file_raw.get("options") or {}),
        format_options={
            str(k).lower(): dict(v or {})
            for k, v in (file_raw.get("format_options") or {}).items()
        },
        consolidation=consolidation,
    )

    kind = _choice(target_raw, "type", TARGET_KINDS, f"{where}.target")
    target_file = dict(target_raw.get("file") or {})
    matching = dict(target_raw.get("matching") or {})
    if kind == "fabric_lakehouse_file":
        strategy = str(matching.get("strategy", "same_file_stem"))
        if strategy not in MATCH_STRATEGIES:
            raise FileConfigError(
                f"{where}: target.matching.strategy must be one of: "
                + ", ".join(MATCH_STRATEGIES)
            )
        matching["strategy"] = strategy
        matching["missing_target"] = _choice(
            matching, "missing_target", MISSING_TARGET_ACTIONS,
            f"{where}.target.matching", default="fail",
        )
        matching["multiple_targets"] = _choice(
            matching, "multiple_targets", MULTIPLE_TARGET_ACTIONS,
            f"{where}.target.matching", default="fail",
        )
        if not str(target_raw.get("relative_path") or "").strip():
            raise FileConfigError(
                f"{where}: target.relative_path is required for a "
                "fabric_lakehouse_file target"
            )
    else:
        for required in ("database", "schema", "table"):
            _require_text(target_raw, required, f"{where}.target")

    target = FileTargetSpec(
        kind=kind,
        connection=_require_text(target_raw, "connection", f"{where}.target"),
        relative_path=(
            str(target_raw["relative_path"]).strip()
            if target_raw.get("relative_path") else None
        ),
        matching=matching,
        format=(
            str(target_file.get("format")).lower()
            if target_file.get("format")
            else str(matching.get("target_extension") or "").lower() or None
        ),
        options=dict(target_file.get("options") or {}),
        format_options={
            str(k).lower().lstrip("."): dict(v or {})
            for k, v in (target_file.get("format_options") or {}).items()
        },
        database=(
            str(target_raw["database"]) if target_raw.get("database") else None
        ),
        schema=str(target_raw["schema"]) if target_raw.get("schema") else None,
        table=str(target_raw["table"]) if target_raw.get("table") else None,
    )

    return FileTableConfig(
        table_id=table_id,
        logical_table_name=logical_name,
        comparison_mode=mode,
        layer=str(raw.get("layer") or "file_to_bronze"),
        tags=tuple(str(item) for item in (raw.get("tags") or ())),
        source=source,
        target=target,
        key_mappings=tuple(
            dict(item) for item in (raw.get("key_mappings") or ())
        ),
        compare_column_mappings=tuple(
            dict(item) for item in (raw.get("compare_column_mappings") or ())
        ),
        incremental=dict(raw.get("incremental") or {}),
        document=document,
    )


# ── small validated readers ───────────────────────────────────────────


def _require_mapping(raw: Mapping[str, Any], name: str, where: str) -> Dict[str, Any]:
    value = raw.get(name)
    if not isinstance(value, dict):
        raise FileConfigError(f"{where}.{name} must be a YAML mapping")
    return value


def _require_text(raw: Mapping[str, Any], name: str, where: str) -> str:
    value = raw.get(name)
    if not isinstance(value, str) or not value.strip():
        raise FileConfigError(f"{where}.{name} must be a non-empty string")
    return value.strip()


def _require_bool(raw: Mapping[str, Any], name: str, where: str) -> bool:
    value = raw.get(name)
    if not isinstance(value, bool):
        raise FileConfigError(f"{where}.{name} must be boolean")
    return value


def _positive_int(raw: Mapping[str, Any], name: str, where: str) -> int:
    value = raw.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FileConfigError(f"{where}.{name} must be a positive integer")
    return value


def _choice(
    raw: Mapping[str, Any], name: str, allowed: Tuple[str, ...], where: str,
    default: Optional[str] = None,
) -> str:
    value = raw.get(name, default)
    if value not in allowed:
        raise FileConfigError(
            f"{where}.{name} must be one of: " + ", ".join(allowed)
        )
    return str(value)


def _under(project_root: Path, configured: str) -> Path:
    """Resolve a configured path against the project root when relative."""
    path = Path(os.path.expandvars(str(configured))).expanduser()
    return path if path.is_absolute() else Path(project_root) / path
