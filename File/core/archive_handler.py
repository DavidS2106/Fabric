"""Safe ZIP (and nested ZIP) expansion for File discovery.

An archive member participates in exactly the same discovery, reading and
comparison flow as a loose file: this module's only job is to turn archive
members into real paths on disk, safely and within configured limits.

Safety, all configuration-driven:

* a member whose resolved path escapes the extraction root is rejected
  (``zip slip``), as is an absolute member path or a symlink member,
* nesting is followed only when ``archive.nested_zip`` is on, and never
  deeper than ``archive.max_depth``,
* ``archive.max_members`` and ``archive.max_uncompressed_bytes`` bound what
  one archive is allowed to expand to.
"""

from __future__ import annotations

import hashlib
import logging
import shutil
import stat
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import List, Optional

from .file_config_loader import ArchivePolicy, FileFrameworkSettings

logger = logging.getLogger(__name__)

ARCHIVE_EXTENSION = "zip"


class ArchiveError(RuntimeError):
    """Raised when an archive cannot be expanded safely or within limits."""


@dataclass(frozen=True)
class ExtractedMember:
    """One supported file recovered from an archive."""

    path: Path
    #: Archive-relative member name, nested members joined with ``!``.
    member_name: str
    #: The outermost archive this member came from.
    archive_path: Path
    depth: int


@dataclass
class _Budget:
    members: int
    uncompressed_bytes: int

    def spend(self, policy: ArchivePolicy, size: int, where: str) -> None:
        self.members += 1
        self.uncompressed_bytes += max(0, int(size))
        if self.members > policy.max_members:
            raise ArchiveError(
                f"{where} expands to more than the configured "
                f"archive.max_members ({policy.max_members})"
            )
        if self.uncompressed_bytes > policy.max_uncompressed_bytes:
            raise ArchiveError(
                f"{where} expands to more than the configured "
                f"archive.max_uncompressed_bytes "
                f"({policy.max_uncompressed_bytes})"
            )


class ArchiveHandler:
    """Expand archives into a configured extraction directory."""

    def __init__(self, settings: FileFrameworkSettings, extract_root: Path):
        self.settings = settings
        self.policy = settings.archive
        self.extract_root = Path(extract_root)

    # ── lifecycle ─────────────────────────────────────────────────────

    def prepare(self) -> None:
        """Create (and, unless retained, clear) the extraction directory."""
        if self.extract_root.exists() and not self.policy.retain:
            shutil.rmtree(self.extract_root, ignore_errors=True)
        self.extract_root.mkdir(parents=True, exist_ok=True)

    def cleanup(self) -> None:
        if self.policy.retain:
            return
        shutil.rmtree(self.extract_root, ignore_errors=True)

    # ── expansion ─────────────────────────────────────────────────────

    def is_archive(self, path: Path) -> bool:
        return Path(path).suffix.lower().lstrip(".") == ARCHIVE_EXTENSION

    def expand(self, archive_path: Path) -> List[ExtractedMember]:
        """Every supported file inside *archive_path*, recursively."""
        if not self.policy.enabled:
            raise ArchiveError(
                f"{Path(archive_path).name} is an archive but "
                "file_validation.archive.enabled is false"
            )
        self.extract_root.mkdir(parents=True, exist_ok=True)
        destination = self.extract_root / _safe_stem(Path(archive_path))
        destination.mkdir(parents=True, exist_ok=True)
        budget = _Budget(members=0, uncompressed_bytes=0)
        return self._expand_into(
            Path(archive_path), destination, Path(archive_path), "", 1, budget
        )

    # ── internals ─────────────────────────────────────────────────────

    def _expand_into(
        self,
        archive_path: Path,
        destination: Path,
        outermost: Path,
        prefix: str,
        depth: int,
        budget: _Budget,
    ) -> List[ExtractedMember]:
        if depth > self.policy.max_depth:
            raise ArchiveError(
                f"{outermost.name} nests deeper than the configured "
                f"archive.max_depth ({self.policy.max_depth})"
            )
        recovered: List[ExtractedMember] = []
        try:
            archive = zipfile.ZipFile(archive_path)
        except zipfile.BadZipFile as exc:
            raise ArchiveError(f"{archive_path.name} is not a readable ZIP") from exc
        with archive:
            for info in archive.infolist():
                if info.is_dir():
                    continue
                member = self._safe_member_path(info, destination, outermost)
                if member is None:
                    continue
                extension = member.suffix.lower().lstrip(".")
                nested = extension == ARCHIVE_EXTENSION
                if not nested and not self.settings.supports(extension):
                    logger.info(
                        "Archive member skipped: unsupported extension | "
                        "archive=%s | member=%s",
                        outermost.name, info.filename,
                    )
                    continue
                budget.spend(self.policy, info.file_size, outermost.name)
                member.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(info) as source, member.open("wb") as sink:
                    shutil.copyfileobj(source, sink, length=1024 * 1024)
                member_name = (
                    f"{prefix}!{info.filename}" if prefix else info.filename
                )
                if nested:
                    if not self.policy.nested_zip:
                        logger.info(
                            "Nested archive skipped: archive.nested_zip is off "
                            "| archive=%s | member=%s",
                            outermost.name, info.filename,
                        )
                        continue
                    recovered.extend(
                        self._expand_into(
                            member,
                            member.with_suffix("") if member.suffix else member,
                            outermost,
                            member_name,
                            depth + 1,
                            budget,
                        )
                    )
                    continue
                recovered.append(
                    ExtractedMember(
                        path=member,
                        member_name=member_name,
                        archive_path=outermost,
                        depth=depth,
                    )
                )
        return recovered

    def _safe_member_path(
        self, info: zipfile.ZipInfo, destination: Path, outermost: Path
    ) -> Optional[Path]:
        """Resolve one member below *destination*, or reject it."""
        if stat.S_ISLNK(info.external_attr >> 16):
            logger.warning(
                "Archive member rejected: symlink | archive=%s | member=%s",
                outermost.name, info.filename,
            )
            return None
        member = PurePosixPath(info.filename.replace("\\", "/"))
        if member.is_absolute() or any(part == ".." for part in member.parts):
            raise ArchiveError(
                f"{outermost.name} contains a member that escapes the "
                f"extraction directory: {info.filename}"
            )
        root = destination.resolve()
        candidate = (destination / Path(*member.parts)).resolve()
        if candidate != root and root not in candidate.parents:
            raise ArchiveError(
                f"{outermost.name} contains a member that escapes the "
                f"extraction directory: {info.filename}"
            )
        return candidate


def _safe_stem(path: Path) -> str:
    """A filesystem-safe, collision-free folder name for one archive.

    Two archives with the same name in different source folders must not
    expand into each other, so the archive's own location is part of the name.
    """
    stem = "".join(
        character if character.isalnum() or character in "-_." else "_"
        for character in path.stem
    ) or "archive"
    digest = hashlib.sha256(
        str(path.resolve()).casefold().encode("utf-8")
    ).hexdigest()[:8]
    return f"{stem}_{digest}"
