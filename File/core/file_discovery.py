"""Discover the source files one table reads for one phase.

The folder comes from ``source.paths.<phase>``, the wildcard from
``source.file.pattern`` and recursion from ``discovery.recursive``. Nothing is
inferred from a folder or file name, and an archive's members join the same
result set as loose files.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

from .archive_handler import ArchiveError, ArchiveHandler
from .file_config_loader import FileConfigError, FileFrameworkSettings

logger = logging.getLogger(__name__)

#: Formats the reader can route. Extensions map onto them, never the reverse.
FORMAT_BY_EXTENSION = {
    "csv": "delimited",
    "txt": "delimited",
    "dat": "delimited",
    "json": "json",
    "xlsx": "excel",
    "xls": "excel",
    "sas7bdat": "sas",
    "parquet": "parquet",
}
#: Names a table may configure in ``source.file.format``.
CONFIGURABLE_FORMATS = ("auto", "csv", "txt", "dat", "json", "xlsx", "xls",
                        "sas7bdat", "parquet")


@dataclass(frozen=True)
class DiscoveredFile:
    """One readable source file, loose or recovered from an archive."""

    path: Path
    #: Human-readable identity used in reports and reconciliation.
    display_path: str
    name: str
    stem: str
    extension: str
    #: Reader family: delimited | json | excel | sas | parquet.
    reader: str
    size_bytes: int
    archive_name: Optional[str] = None
    member_name: Optional[str] = None

    @property
    def from_archive(self) -> bool:
        return self.archive_name is not None


@dataclass(frozen=True)
class DiscoveryResult:
    files: Tuple[DiscoveredFile, ...]
    #: (display path, reason) for everything discovered but not readable.
    rejected: Tuple[Tuple[str, str], ...]
    #: The folder actually scanned, for reporting.
    scanned_path: Path


class FileDiscovery:
    """Find, and where required expand, the files a table reads."""

    def __init__(self, settings: FileFrameworkSettings, archives: ArchiveHandler):
        self.settings = settings
        self.archives = archives

    def discover(
        self,
        source_root: Path,
        relative_path: str,
        pattern: str,
        configured_format: str,
    ) -> DiscoveryResult:
        if configured_format not in CONFIGURABLE_FORMATS:
            raise FileConfigError(
                "source.file.format must be one of: "
                + ", ".join(CONFIGURABLE_FORMATS)
            )
        folder = (Path(source_root) / relative_path).resolve()
        if not folder.is_dir():
            raise FileConfigError(
                f"Configured source path does not exist: {folder}"
            )

        # Windows can enumerate a deeply nested entry while ``stat`` (and
        # therefore Path.is_file()) fails once the legacy MAX_PATH boundary is
        # crossed.  Keep the configured/discovered path for reporting, but use
        # an extended-length path for every filesystem operation.
        matched = sorted(
            folder.rglob(pattern)
            if self.settings.discovery.recursive
            else folder.glob(pattern)
        )

        files: List[DiscoveredFile] = []
        rejected: List[Tuple[str, str]] = []

        for candidate in matched:
            io_candidate = _extended_path(candidate)
            if not io_candidate.is_file():
                # Patterns such as "*" also enumerate directories. They are
                # containers, not rejected input files.
                continue
            if self._hidden(candidate, folder):
                continue
            if (
                io_candidate.is_symlink()
                and not self.settings.discovery.follow_symlinks
            ):
                rejected.append((
                    self._display(candidate, folder),
                    "symlink skipped (discovery.follow_symlinks is false)",
                ))
                continue
            extension = candidate.suffix.lower().lstrip(".")
            if not self.settings.supports(extension):
                rejected.append((
                    self._display(candidate, folder),
                    f"extension '{extension or 'none'}' is not in "
                    "file_validation.supported_files",
                ))
                continue
            if self.archives.is_archive(candidate):
                expanded, archive_rejects = self._expand(
                    io_candidate, _extended_path(folder), configured_format
                )
                files.extend(expanded)
                rejected.extend(archive_rejects)
                continue
            resolved = self._reader_for(extension, configured_format)
            if resolved is None:
                rejected.append((
                    self._display(candidate, folder),
                    f"extension '{extension}' cannot serve the configured "
                    f"format '{configured_format}'",
                ))
                continue
            files.append(DiscoveredFile(
                path=io_candidate,
                display_path=self._display(candidate, folder),
                name=candidate.name,
                stem=candidate.stem,
                extension=extension,
                reader=resolved,
                size_bytes=io_candidate.stat().st_size,
            ))

        if not files and self.settings.discovery.on_no_files == "fail":
            raise FileConfigError(
                f"No files matched '{pattern}' under {folder}"
            )

        files.sort(key=lambda item: item.display_path)
        return DiscoveryResult(
            files=tuple(files),
            rejected=tuple(rejected),
            scanned_path=folder,
        )

    # ── internals ─────────────────────────────────────────────────────

    def _expand(
        self, archive: Path, folder: Path, configured_format: str
    ) -> Tuple[List[DiscoveredFile], List[Tuple[str, str]]]:
        display = self._display(archive, folder)
        try:
            members = self.archives.expand(archive)
        except ArchiveError as exc:
            return [], [(display, str(exc))]
        files: List[DiscoveredFile] = []
        rejected: List[Tuple[str, str]] = []
        for member in members:
            extension = member.path.suffix.lower().lstrip(".")
            resolved = self._reader_for(extension, configured_format)
            if resolved is None:
                rejected.append((
                    f"{display}!{member.member_name}",
                    f"extension '{extension}' cannot serve the configured "
                    f"format '{configured_format}'",
                ))
                continue
            files.append(DiscoveredFile(
                path=member.path,
                display_path=f"{display}!{member.member_name}",
                name=member.path.name,
                stem=member.path.stem,
                extension=extension,
                reader=resolved,
                size_bytes=member.path.stat().st_size,
                archive_name=archive.name,
                member_name=member.member_name,
            ))
        if not files and not rejected:
            rejected.append((display, "archive contains no supported files"))
        return files, rejected

    def _reader_for(self, extension: str, configured_format: str) -> Optional[str]:
        """The reader family for one file under the table's format setting."""
        if configured_format != "auto" and configured_format != extension:
            # A table pinned to one format only reads that format. This keeps
            # `pattern: "*.*"` honest instead of silently widening it.
            return None
        return FORMAT_BY_EXTENSION.get(extension)

    def _hidden(self, candidate: Path, folder: Path) -> bool:
        if not self.settings.discovery.ignore_hidden:
            return False
        relative = candidate.relative_to(folder)
        return any(part.startswith(".") for part in relative.parts)

    @staticmethod
    def _display(candidate: Path, folder: Path) -> str:
        try:
            return candidate.relative_to(folder).as_posix()
        except ValueError:
            return candidate.as_posix()


def _extended_path(path: Path) -> Path:
    """Return a Windows path that is not constrained by legacy MAX_PATH."""
    resolved = str(Path(path).resolve())
    if os.name != "nt" or resolved.startswith("\\\\?\\"):
        return Path(resolved)
    if resolved.startswith("\\\\"):
        return Path("\\\\?\\UNC\\" + resolved.lstrip("\\"))
    return Path("\\\\?\\" + resolved)
