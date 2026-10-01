"""
reporters/report_paths.py
─────────────────────────
The one component that turns `report_output` in the execution document into
concrete report paths::

    execution.yml
          |
    ReportPathResolver
          |
    one resolved run root
          |
    Excel / JSON / HTML / ZIP writers

Every writer receives an already resolved :class:`ResolvedReportPaths` and
writes only inside it. No writer builds a phase folder, a run folder or a
file name of its own, so a phase has exactly one physical output tree::

    <root_folder>/<phase folder>/<run folder>/
        Excel/Consolidated/<consolidated workbook>
        Excel/Tables/<TABLE_ID>/<table workbook>
        JSON/
        HTML/
        <zip name>

The resolver refuses to invent anything. An unconfigured phase, a phase
folder that collides with another one when compared case-insensitively, or a
table filename without `{table_id}` are configuration errors raised before a
single byte is written. Excel filenames may be compact because their phase,
run and table identity is already encoded in the enclosing directory tree.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


logger = logging.getLogger(__name__)

#: Path segment characters a rendered token may contribute. A token value is
#: never allowed to introduce a separator, so no template can escape the run
#: folder however a run is identified.
_UNSAFE_TOKEN = re.compile(r"[^A-Za-z0-9._-]+")

#: The framework's logical validation phases. Which folder each one is
#: written to, and whether it is enabled at all, is configuration; the
#: vocabulary itself is what makes an unknown phase reportable as an error
#: instead of silently becoming a new directory.
LOGICAL_PHASES: Tuple[str, ...] = (
    "preflight",
    "smoke_test",
    "historical_backfill",
    "incremental",
    "full_load",
)

#: The logical phase the preflight reports belong to.
PREFLIGHT_PHASE = "preflight"

CONFIG_PATH = "report_output"


class ReportPathError(ValueError):
    """Raised when `report_output` cannot describe the report tree."""


# ── template helpers ────────────────────────────────────────────────────────

def safe_token(value: Any) -> str:
    """A token value reduced to one safe path segment."""
    return _UNSAFE_TOKEN.sub("_", str(value)).strip("._") or "unnamed"


def filesystem_path(path: Path) -> Path:
    """`path` in the form the platform can actually open.

    A standard report tree is deep by design — phase, run, format, table — so
    it reaches the Windows MAX_PATH limit on ordinary output roots. Every
    writer opens its destination through here so the configured structure is
    never quietly truncated to fit.
    """
    absolute = str(Path(path).resolve())
    if (
        os.name == "nt"
        and len(absolute) >= 248
        and not absolute.startswith("\\\\?\\")
    ):
        return Path("\\\\?\\" + absolute)
    return Path(absolute)


def walk_root(path: Path) -> Path:
    """`path` in the form a recursive walk can traverse to any depth.

    An ordinary Windows directory stops being enumerable as soon as its
    children pass MAX_PATH — the deep entries simply do not appear, with no
    error. A package built from such a walk would silently omit reports, so a
    walk always starts from the extended-length form even when the root itself
    is short.
    """
    absolute = str(Path(path).resolve())
    if os.name == "nt" and not absolute.startswith("\\\\?\\"):
        return Path("\\\\?\\" + absolute)
    return Path(absolute)


def _render(template: str, tokens: Mapping[str, str], where: str) -> str:
    try:
        rendered = str(template).format_map(dict(tokens))
    except KeyError as exc:
        raise ReportPathError(
            f"{where} uses an unknown token: {{{exc.args[0]}}}"
        ) from exc
    candidate = Path(rendered)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ReportPathError(f"{where} must stay relative to the run folder")
    return rendered


def _require(parent: Mapping[str, Any], name: str, where: str) -> Any:
    if not isinstance(parent, Mapping) or name not in parent:
        raise ReportPathError(f"{where}.{name} is required")
    return parent[name]


def _require_text(parent: Mapping[str, Any], name: str, where: str) -> str:
    value = _require(parent, name, where)
    if not isinstance(value, str) or not value.strip():
        raise ReportPathError(f"{where}.{name} must be a non-empty string")
    return value


def _require_bool(parent: Mapping[str, Any], name: str, where: str) -> bool:
    value = _require(parent, name, where)
    if not isinstance(value, bool):
        raise ReportPathError(f"{where}.{name} must be true or false")
    return value


def _require_mapping(
    parent: Mapping[str, Any], name: str, where: str
) -> Mapping[str, Any]:
    value = _require(parent, name, where)
    if not isinstance(value, Mapping) or not value:
        raise ReportPathError(f"{where}.{name} must be a non-empty mapping")
    return value


def _require_tokens(
    template: str, tokens: Sequence[str], where: str
) -> None:
    missing = [name for name in tokens if f"{{{name}}}" not in template]
    if missing:
        raise ReportPathError(
            f"{where} must contain " + ", ".join(f"{{{name}}}" for name in missing)
        )


def _require_segment(value: str, where: str) -> str:
    """A single directory name — never a path, never a traversal."""
    if value != value.strip() or Path(value).name != value or value in {".", ".."}:
        raise ReportPathError(
            f"{where} must be one folder name, not a path: {value!r}"
        )
    return value


# ── the resolved tree of one phase of one run ───────────────────────────────

@dataclass(frozen=True)
class FormatLayout:
    """Where one report format writes, and whether it writes at all."""

    name: str
    enabled: bool
    root: Path
    consolidated: Optional[Path]
    per_table_enabled: bool
    #: Rendered lazily per table so a table folder only ever exists for a
    #: table the run actually executed.
    _table_folder: Optional[str] = None
    _table_filename: Optional[str] = None

    def table_path(self, run_root: Path, tokens: Mapping[str, str]) -> Path:
        if not (self.enabled and self.per_table_enabled):
            raise ReportPathError(
                f"{CONFIG_PATH}.{self.name}.per_table is not enabled; no "
                "table-wise report path exists for this format"
            )
        where = f"{CONFIG_PATH}.{self.name}.per_table"
        return (
            run_root
            / _render(str(self._table_folder), tokens, f"{where}.folder")
            / _render(str(self._table_filename), tokens, f"{where}.filename")
        )


@dataclass(frozen=True)
class ResolvedReportPaths:
    """Every path one phase of one run may write, and nothing else."""

    phase: str
    phase_folder: str
    run_id: str
    timestamp: str
    root: Path
    phase_root: Path
    run_root: Path
    zip_path: Optional[Path]
    excel: FormatLayout
    json: FormatLayout
    html: FormatLayout
    create_disabled_format_folders: bool
    _tokens: Mapping[str, str] = field(repr=False, compare=False,
                                       default_factory=dict)

    # ── format access ───────────────────────────────────────────────────

    @property
    def formats(self) -> Tuple[FormatLayout, ...]:
        return (self.excel, self.json, self.html)

    def layout(self, name: str) -> FormatLayout:
        for item in self.formats:
            if item.name == name:
                return item
        raise ReportPathError(f"Unknown report format: {name}")

    def table_path(self, name: str, table_id: str) -> Path:
        """The table-wise report path of `table_id` in the given format."""
        tokens = {**self._tokens, "table_id": safe_token(table_id)}
        return self.layout(name).table_path(self.run_root, tokens)

    # ── directories ─────────────────────────────────────────────────────

    def directories(self) -> Tuple[Path, ...]:
        """The directories this run is allowed to create, run root first."""
        wanted = [self.run_root]
        for item in self.formats:
            if item.enabled or self.create_disabled_format_folders:
                wanted.append(item.root)
        return tuple(dict.fromkeys(wanted))

    def create_directories(self) -> Path:
        for directory in self.directories():
            filesystem_path(directory).mkdir(parents=True, exist_ok=True)
        return self.run_root

    def contains(self, path: Path) -> bool:
        """Whether `path` belongs to this run folder."""
        run_root = self.run_root.resolve()
        candidate = Path(path).resolve()
        return candidate == run_root or run_root in candidate.parents

    # ── logging ─────────────────────────────────────────────────────────

    def log_fields(self) -> Dict[str, str]:
        """The resolved paths, reported once at the start of every phase."""
        return {
            "phase": self.phase,
            "phase_folder": self.phase_folder,
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "run_root": str(self.run_root),
            "excel_path": str(self.excel.root) if self.excel.enabled else "DISABLED",
            "json_path": str(self.json.root) if self.json.enabled else "DISABLED",
            "html_path": str(self.html.root) if self.html.enabled else "DISABLED",
            "zip_path": str(self.zip_path) if self.zip_path else "DISABLED",
        }

    def log_resolution(self) -> None:
        logger.info(
            "report_paths_resolved " + " ".join(
                f"{key}=%s" for key in self.log_fields()
            ),
            *self.log_fields().values(),
        )


# ── the resolver ────────────────────────────────────────────────────────────

class ReportPathResolver:
    """Resolve `report_output` into one run root per phase, once per run.

    One resolver instance serves one execution: it owns the run timestamp, so
    every phase and every writer of that run agrees on the run folder without
    passing timestamps around.
    """

    def __init__(
        self,
        report_output: Mapping[str, Any] | None,
        timestamp: Optional[str] = None,
        root_folder: Optional[str | Path] = None,
    ):
        if not isinstance(report_output, Mapping) or not report_output:
            raise ReportPathError(f"{CONFIG_PATH} is required")
        self.config: Mapping[str, Any] = report_output
        self._validate()
        self.timestamp = safe_token(
            timestamp
            if timestamp is not None
            else datetime.now().strftime(self.config["timestamp_format"])
        )
        # Relocating the whole tree is the only override a caller gets: the
        # structure below the root stays entirely configuration-driven.
        self._root = Path(
            str(root_folder) if root_folder else str(self.config["root_folder"])
        )
        self._resolved: Dict[Tuple[str, str], ResolvedReportPaths] = {}

    # ── construction ────────────────────────────────────────────────────

    @classmethod
    def from_execution_document(
        cls,
        document: Mapping[str, Any],
        timestamp: Optional[str] = None,
        root_folder: Optional[str | Path] = None,
    ) -> "ReportPathResolver":
        if not isinstance(document, Mapping) or CONFIG_PATH not in document:
            raise ReportPathError(
                f"The execution configuration must declare `{CONFIG_PATH}`"
            )
        return cls(document[CONFIG_PATH], timestamp, root_folder)

    @classmethod
    def from_execution_config(
        cls,
        config_path: str | Path,
        timestamp: Optional[str] = None,
        root_folder: Optional[str | Path] = None,
    ) -> "ReportPathResolver":
        # The complete execution document: `report_output` may live in its own
        # document named under the master's `config_files`.
        from core.config_loader import load_execution_document

        return cls.from_execution_document(
            load_execution_document(Path(config_path)), timestamp, root_folder
        )

    # ── configuration contract ──────────────────────────────────────────

    def _validate(self) -> None:
        config = self.config
        _require_text(config, "root_folder", CONFIG_PATH)
        _require_text(config, "timestamp_format", CONFIG_PATH)
        for flag in ("generate_excel", "generate_json", "generate_html",
                     "create_disabled_format_folders"):
            _require_bool(config, flag, CONFIG_PATH)
        run_folder = _require_mapping(config, "run_folder", CONFIG_PATH)
        template = _require_text(run_folder, "format", f"{CONFIG_PATH}.run_folder")
        _require_tokens(
            template, ("run_id", "timestamp"), f"{CONFIG_PATH}.run_folder.format"
        )
        _require_bool(
            _require_mapping(config, "zip", CONFIG_PATH), "enabled",
            f"{CONFIG_PATH}.zip",
        )
        self._validate_phases()
        for name in ("excel", "json", "html"):
            self._validate_format(name)

    def _validate_phases(self) -> None:
        where = f"{CONFIG_PATH}.phase_reports"
        phases = _require_mapping(self.config, "phase_reports", CONFIG_PATH)
        unknown = sorted(set(phases) - set(LOGICAL_PHASES))
        if unknown:
            raise ReportPathError(
                f"{where} declares phase(s) that are not validation phases: "
                + ", ".join(unknown)
            )
        missing = [name for name in LOGICAL_PHASES if name not in phases]
        if missing:
            raise ReportPathError(
                f"{where} is missing: " + ", ".join(missing)
            )
        seen: Dict[str, str] = {}
        for phase in LOGICAL_PHASES:
            spec = phases[phase]
            if not isinstance(spec, Mapping):
                raise ReportPathError(f"{where}.{phase} must be a mapping")
            folder = _require_segment(
                _require_text(spec, "folder", f"{where}.{phase}"),
                f"{where}.{phase}.folder",
            )
            # Two phases whose folders differ only by case are one folder on
            # Windows and two on Linux. Neither is an output structure, so the
            # configuration is rejected instead of behaving per platform.
            collision = seen.get(folder.casefold())
            if collision:
                raise ReportPathError(
                    f"{where}.{phase}.folder '{folder}' collides with "
                    f"'{seen[folder.casefold()]}' when compared "
                    "case-insensitively; phase folders must differ on "
                    "case-sensitive and case-insensitive filesystems alike"
                )
            seen[folder.casefold()] = folder
            zip_name = _require_text(spec, "zip_name", f"{where}.{phase}")
            _require_tokens(
                zip_name, ("timestamp",), f"{where}.{phase}.zip_name"
            )
            _require_segment(
                _render(
                    zip_name,
                    {"phase": phase, "run_id": "run", "timestamp": "ts"},
                    f"{where}.{phase}.zip_name",
                ),
                f"{where}.{phase}.zip_name",
            )

    def _validate_format(self, name: str) -> None:
        where = f"{CONFIG_PATH}.{name}"
        section = _require_mapping(self.config, name, CONFIG_PATH)
        folder = _require_segment(
            _require_text(section, "folder", where), f"{where}.folder"
        )
        consolidated = _require_mapping(section, "consolidated", where)
        if _require_bool(consolidated, "enabled", f"{where}.consolidated"):
            self._validate_target(
                consolidated, f"{where}.consolidated", folder,
                () if name == "excel" else ("run_id", "timestamp"),
            )
        per_table = _require_mapping(section, "per_table", where)
        if _require_bool(per_table, "enabled", f"{where}.per_table"):
            self._validate_target(
                per_table, f"{where}.per_table", folder,
                (("table_id",) if name == "excel" else
                 ("table_id", "phase", "run_id", "timestamp")),
            )

    def _validate_target(
        self,
        section: Mapping[str, Any],
        where: str,
        format_folder: str,
        required_tokens: Sequence[str],
    ) -> None:
        folder = _require_text(section, "folder", where)
        # Keeping every report of a format under that format's own folder is
        # what makes `Excel/Consolidated` and `Excel/Tables` the only two
        # places an Excel report can appear.
        if Path(folder).parts[:1] != (format_folder,):
            raise ReportPathError(
                f"{where}.folder must start with {format_folder!r} so the "
                "format keeps one folder inside the run"
            )
        filename = _require_text(section, "filename", where)
        _require_tokens(filename, required_tokens, f"{where}.filename")
        if Path(filename).name != filename:
            raise ReportPathError(f"{where}.filename must not contain a path")

    # ── resolution ──────────────────────────────────────────────────────

    @property
    def root(self) -> Path:
        return self._root

    @property
    def phases(self) -> Tuple[str, ...]:
        """The logical phases this configuration can report, in order."""
        return tuple(
            phase for phase in LOGICAL_PHASES if phase in self.config["phase_reports"]
        )

    def phase_folder(self, phase: str) -> str:
        """The configured physical folder of a logical phase."""
        configured = self.config["phase_reports"]
        if phase not in configured:
            raise ReportPathError(
                f"Unknown validation phase '{phase}'. Configure "
                f"{CONFIG_PATH}.phase_reports.{phase}.folder to report it; "
                "phases are never created implicitly. Configured phases: "
                + ", ".join(self.phases)
            )
        return str(configured[phase]["folder"])

    def resolve(self, phase: str, run_id: str) -> ResolvedReportPaths:
        """The single resolved tree every writer of `phase` must use."""
        phase_folder = self.phase_folder(phase)
        key = (phase, str(run_id))
        cached = self._resolved.get(key)
        if cached is not None:
            return cached

        tokens = {
            "phase": safe_token(phase),
            "run_id": safe_token(run_id),
            "timestamp": self.timestamp,
        }
        phase_root = self.root / phase_folder
        run_root = phase_root / _render(
            self.config["run_folder"]["format"], tokens,
            f"{CONFIG_PATH}.run_folder.format",
        )
        spec = self.config["phase_reports"][phase]
        zip_path = (
            run_root / _render(
                str(spec["zip_name"]), tokens,
                f"{CONFIG_PATH}.phase_reports.{phase}.zip_name",
            )
            if self.config["zip"]["enabled"] else None
        )
        resolved = ResolvedReportPaths(
            phase=phase,
            phase_folder=phase_folder,
            run_id=str(run_id),
            timestamp=self.timestamp,
            root=self.root,
            phase_root=phase_root,
            run_root=run_root,
            zip_path=zip_path,
            excel=self._layout("excel", run_root, tokens),
            json=self._layout("json", run_root, tokens),
            html=self._layout("html", run_root, tokens),
            create_disabled_format_folders=bool(
                self.config["create_disabled_format_folders"]
            ),
            _tokens=tokens,
        )
        self._resolved[key] = resolved
        return resolved

    def _layout(
        self, name: str, run_root: Path, tokens: Mapping[str, str]
    ) -> FormatLayout:
        section = self.config[name]
        enabled = bool(self.config[f"generate_{name}"])
        consolidated_spec = section["consolidated"]
        consolidated = None
        if enabled and consolidated_spec["enabled"]:
            where = f"{CONFIG_PATH}.{name}.consolidated"
            consolidated = (
                run_root
                / _render(str(consolidated_spec["folder"]), tokens, f"{where}.folder")
                / _render(
                    str(consolidated_spec["filename"]), tokens, f"{where}.filename"
                )
            )
        per_table = section["per_table"]
        return FormatLayout(
            name=name,
            enabled=enabled,
            root=run_root / str(section["folder"]),
            consolidated=consolidated,
            per_table_enabled=bool(enabled and per_table["enabled"]),
            _table_folder=per_table.get("folder"),
            _table_filename=per_table.get("filename"),
        )


# ── the pre-generation agreement check ──────────────────────────────────────

def assert_writers_share_run_root(
    paths: ResolvedReportPaths, writers: Iterable[Any]
) -> Path:
    """Verify every writer targets the resolved run root, before writing.

    A writer declares the directories it intends to write through
    ``target_directories(paths)``. Any directory outside the run root means a
    writer is still building a tree of its own, which is exactly the failure
    this module exists to prevent, so nothing is generated at all.
    """
    offenders: List[str] = []
    checked = 0
    for writer in writers:
        declare = getattr(writer, "target_directories", None)
        if declare is None:
            raise ReportPathError(
                f"{type(writer).__name__} does not declare its target "
                "directories, so its run root cannot be verified"
            )
        for directory in declare(paths):
            checked += 1
            if not paths.contains(Path(directory)):
                offenders.append(f"{type(writer).__name__} -> {directory}")
    if offenders:
        raise ReportPathError(
            f"Writers must stay inside the resolved run root {paths.run_root}: "
            + "; ".join(offenders)
        )
    logger.info(
        "report_writers_agree phase=%s run_root=%s verified_directories=%d",
        paths.phase, paths.run_root, checked,
    )
    return paths.run_root
