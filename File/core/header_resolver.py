"""Decide whether one file carries a header, and which columns it has.

Three rules, in this order, none of which ever invents a column:

1. ``source.file.options.column_names`` — an explicitly configured column
   order always wins and is used verbatim.
2. ``source.file.options.header`` — ``true`` reads the first record as the
   header, ``false`` declares the file headerless, ``auto`` sniffs it.
3. A headerless file may borrow the column order of the first file in the
   same set that did carry one, but only when
   ``header.reuse_schema_from_first_headed_file`` allows it. Without a
   reference the file is failed or skipped per ``header.on_missing_schema``.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from typing import Any, List, Mapping, Optional, Sequence

from .file_config_loader import FileFrameworkSettings
from .file_discovery import DiscoveredFile

logger = logging.getLogger(__name__)

#: Reader families whose header is a property of the file itself.
HEADER_BEARING_READERS = ("delimited", "excel")

HEADER_STATUS_PRESENT = "header_present"
HEADER_STATUS_CONFIGURED = "columns_configured"
HEADER_STATUS_REUSED = "headerless_schema_reused"
HEADER_STATUS_SELF_DESCRIBING = "self_describing"


class HeaderResolutionError(ValueError):
    """Raised when a file's columns cannot be established from configuration."""


@dataclass(frozen=True)
class HeaderDecision:
    """What the reader must do about the first record of one file."""

    has_header: bool
    #: Column order to apply when the file carries no header. ``None`` when
    #: the file describes its own columns.
    column_names: Optional[List[str]]
    status: str

    @property
    def headerless(self) -> bool:
        return not self.has_header


class HeaderResolver:
    def __init__(self, settings: FileFrameworkSettings):
        self.settings = settings
        self.policy = settings.header

    def resolve(
        self,
        discovered: DiscoveredFile,
        options: Mapping[str, Any],
        reference_columns: Optional[Sequence[str]] = None,
    ) -> HeaderDecision:
        configured_names = options.get("column_names")
        if configured_names is not None:
            names = _column_list(configured_names, discovered)
            return HeaderDecision(
                has_header=bool(options.get("header", False)),
                column_names=names,
                status=HEADER_STATUS_CONFIGURED,
            )

        if discovered.reader not in HEADER_BEARING_READERS:
            # JSON and Parquet carry their own field names.
            return HeaderDecision(
                has_header=True,
                column_names=None,
                status=HEADER_STATUS_SELF_DESCRIBING,
            )

        configured = options.get("header", "auto")
        if configured == "auto":
            if not self.policy.configurable:
                raise HeaderResolutionError(
                    f"{discovered.display_path}: header must be configured "
                    "because file_validation.header.configurable is false"
                )
            has_header = self._sniff(discovered, options)
        elif isinstance(configured, bool):
            has_header = configured
        else:
            raise HeaderResolutionError(
                f"{discovered.display_path}: source.file.options.header must "
                "be true, false or 'auto'"
            )

        if has_header:
            return HeaderDecision(
                has_header=True,
                column_names=None,
                status=HEADER_STATUS_PRESENT,
            )

        if not self.policy.allow_headerless_files:
            raise HeaderResolutionError(
                f"{discovered.display_path} carries no header and "
                "file_validation.header.allow_headerless_files is false"
            )
        if reference_columns and self.policy.reuse_schema_from_first_headed_file:
            return HeaderDecision(
                has_header=False,
                column_names=list(reference_columns),
                status=HEADER_STATUS_REUSED,
            )
        raise HeaderResolutionError(
            f"{discovered.display_path} carries no header and no column order "
            "is available: configure source.file.options.column_names, or "
            "provide a file in the same set that does carry a header"
        )

    def skip_on_missing_schema(self) -> bool:
        """Whether an unresolvable header skips the file instead of failing."""
        return self.policy.on_missing_schema == "skip"

    # ── internals ─────────────────────────────────────────────────────

    def _sniff(
        self, discovered: DiscoveredFile, options: Mapping[str, Any]
    ) -> bool:
        if discovered.reader == "excel":
            # A spreadsheet has no reliable header signal; ask for it.
            raise HeaderResolutionError(
                f"{discovered.display_path}: source.file.options.header must "
                "be stated explicitly for a spreadsheet"
            )
        encoding = str(options.get("encoding") or "utf-8")
        sample = _read_sample(
            discovered, encoding, self.settings.delimiters.sample_bytes
        )
        if not sample.strip():
            raise HeaderResolutionError(
                f"{discovered.display_path} is empty; its header cannot be "
                "determined"
            )
        try:
            return csv.Sniffer().has_header(sample)
        except csv.Error as exc:
            raise HeaderResolutionError(
                f"{discovered.display_path}: header could not be determined "
                f"({exc}); state source.file.options.header explicitly"
            ) from exc


def _column_list(configured: Any, discovered: DiscoveredFile) -> List[str]:
    if not isinstance(configured, (list, tuple)) or not configured:
        raise HeaderResolutionError(
            f"{discovered.display_path}: source.file.options.column_names must "
            "be a non-empty list of column names"
        )
    names = [str(item).strip() for item in configured]
    if any(not name for name in names):
        raise HeaderResolutionError(
            f"{discovered.display_path}: source.file.options.column_names must "
            "not contain an empty name"
        )
    if len(set(names)) != len(names):
        raise HeaderResolutionError(
            f"{discovered.display_path}: source.file.options.column_names must "
            "not repeat a column name"
        )
    return names


def _read_sample(discovered: DiscoveredFile, encoding: str, size: int) -> str:
    with discovered.path.open("r", encoding=encoding, errors="replace") as stream:
        return stream.read(size)
