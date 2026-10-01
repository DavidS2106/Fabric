"""Logically consolidate many source files into one dataset.

Used only by ``comparison_mode: files_to_table``. ``file_to_file`` never
reaches this module: there, each file is validated on its own.

"Logical" is literal. The files are never concatenated in memory: each one is
read in bounded chunks and streamed into the staged dataset, so a set of files
of any total size costs the memory of one chunk.

Schema compatibility is checked *before* anything is staged, under the mode
``source.consolidation.compatibility`` selects:

``strict``   every file exposes the same column set; column order is aligned
             to the first file's order.
``ordered``  every file exposes the same columns in the same order.
``subset``   the dataset is the intersection of every file's columns, and
             each file is projected onto it. No column is ever invented.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterator, List, Optional, Sequence, Tuple

import pandas as pd

from .file_config_loader import FileFrameworkSettings
from .file_discovery import DiscoveredFile
from .file_reader import FileReader, ReadPlan
from .header_resolver import HeaderResolutionError, HeaderResolver
from .validation_adapter import FileStagingError, FileStagingStore, StagedTable

logger = logging.getLogger(__name__)


class SchemaCompatibilityError(ValueError):
    """Raised when the discovered files cannot form one logical dataset."""


@dataclass(frozen=True)
class PlannedFile:
    """One file's resolved read plan and column order."""

    discovered: DiscoveredFile
    plan: ReadPlan
    columns: Tuple[str, ...]


@dataclass(frozen=True)
class ConsolidationResult:
    staged: StagedTable
    planned: Tuple[PlannedFile, ...]
    reference_columns: Tuple[str, ...]
    compatibility: str


class FileConsolidator:
    """Turn many compatible files into one logically consolidated dataset."""

    def __init__(
        self,
        settings: FileFrameworkSettings,
        reader: FileReader,
        headers: HeaderResolver,
    ):
        self.settings = settings
        self.reader = reader
        self.headers = headers

    # ── planning ──────────────────────────────────────────────────────

    def plan_files(
        self,
        files: Sequence[DiscoveredFile],
        options_for: Any,
    ) -> Tuple[List[PlannedFile], List[Tuple[str, str]]]:
        """Resolve every file's header and column order without loading it.

        Files are planned in discovery order so that the first file carrying a
        header can supply the column order a later headerless file borrows.
        """
        planned: List[PlannedFile] = []
        skipped: List[Tuple[str, str]] = []
        reference: Optional[List[str]] = None
        for discovered in files:
            options = options_for(discovered)
            try:
                decision = self.headers.resolve(discovered, options, reference)
                plan = self.reader.plan(discovered, options, decision)
                columns = self.reader.columns(plan)
            except HeaderResolutionError as exc:
                if self.headers.skip_on_missing_schema():
                    skipped.append((discovered.display_path, str(exc)))
                    continue
                raise
            if reference is None and decision.has_header:
                reference = list(columns)
            planned.append(PlannedFile(
                discovered=discovered, plan=plan, columns=tuple(columns)
            ))
        return planned, skipped

    # ── compatibility ─────────────────────────────────────────────────

    def reference_columns(
        self, planned: Sequence[PlannedFile], compatibility: str
    ) -> Tuple[str, ...]:
        if not planned:
            raise SchemaCompatibilityError(
                "No readable file remains to consolidate"
            )
        first = planned[0]
        if compatibility == "subset":
            shared = [
                column for column in first.columns
                if all(column in item.columns for item in planned)
            ]
            if not shared:
                raise SchemaCompatibilityError(
                    "The discovered files share no column, so they cannot form "
                    "one logical dataset"
                )
            return tuple(shared)

        for item in planned[1:]:
            if compatibility == "ordered":
                if item.columns != first.columns:
                    raise SchemaCompatibilityError(
                        f"{item.discovered.display_path} has column order "
                        f"{list(item.columns)}; "
                        f"{first.discovered.display_path} has "
                        f"{list(first.columns)}"
                    )
            elif set(item.columns) != set(first.columns):
                missing = sorted(set(first.columns) - set(item.columns))
                extra = sorted(set(item.columns) - set(first.columns))
                raise SchemaCompatibilityError(
                    f"{item.discovered.display_path} is not compatible with "
                    f"{first.discovered.display_path}: "
                    f"missing {missing or 'nothing'}, "
                    f"unexpected {extra or 'nothing'}"
                )
        return first.columns

    # ── staging ───────────────────────────────────────────────────────

    def consolidate(
        self,
        dataset_name: str,
        planned: Sequence[PlannedFile],
        store: FileStagingStore,
        compatibility: str,
        chunk_rows: int,
        reference: Optional[Sequence[str]] = None,
    ) -> ConsolidationResult:
        """Stream every planned file into one staged dataset.

        *reference* is the dataset's column order. Passing it lets the caller
        decide whether compatibility was checked first; omitting it checks now.
        """
        reference = (
            tuple(reference) if reference
            else self.reference_columns(planned, compatibility)
        )
        first, *rest = planned
        staged = store.stage(
            dataset_name,
            self._aligned(first, reference, chunk_rows),
            expected_columns=reference,
            contribution_key=first.discovered.display_path,
        )
        for item in rest:
            staged = store.append(
                staged,
                self._aligned(item, reference, chunk_rows),
                contribution_key=item.discovered.display_path,
            )
        return ConsolidationResult(
            staged=staged,
            planned=tuple(planned),
            reference_columns=reference,
            compatibility=compatibility,
        )

    def _aligned(
        self, item: PlannedFile, reference: Sequence[str], chunk_rows: int
    ) -> Iterator[pd.DataFrame]:
        """Project one file's chunks onto the dataset's column order."""
        wanted = list(reference)
        for frame in self.reader.read_chunks(item.plan, chunk_rows):
            missing = [column for column in wanted if column not in frame.columns]
            if missing:
                raise FileStagingError(
                    f"{item.discovered.display_path} is missing column(s) "
                    + ", ".join(missing)
                )
            yield frame[wanted]
