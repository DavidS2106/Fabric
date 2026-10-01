"""File-level reconciliation metadata.

Every file the capability touched is recorded here, whether it was validated,
consolidated into a logical dataset, skipped or rejected — and, for a
consolidated dataset, both the individual files *and* the logical result are
kept. This is bookkeeping about files; it never decides a PASS or a FAIL. The
validation status a record carries is copied from the result the existing
validation engine produced.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from reporters.report_paths import filesystem_path

logger = logging.getLogger(__name__)

#: Column order of every reconciliation export.
FIELD_ORDER = (
    "run_id",
    "phase",
    "table_id",
    "logical_table_name",
    "comparison_mode",
    "validation_table_id",
    "record_kind",
    "source_filename",
    "source_path",
    "archive_name",
    "file_format",
    "delimiter",
    "delimiter_source",
    "header_status",
    "target_kind",
    "target_filename",
    "target_path",
    "target_table",
    "source_row_count",
    "target_row_count",
    "staged_source_table",
    "staged_target_table",
    "processing_status",
    "skipped_status",
    "rejected_status",
    "validation_status",
    "message",
    "recorded_at",
)

#: What one record describes.
KIND_FILE = "source_file"
KIND_LOGICAL_TABLE = "logical_table"

PROCESSING_VALIDATED = "validated"
PROCESSING_CONSOLIDATED = "consolidated"
PROCESSING_SKIPPED = "skipped"
PROCESSING_REJECTED = "rejected"
PROCESSING_NOT_EXECUTED = "not_executed"


@dataclass
class FileReconciliationRecord:
    """One row of the File reconciliation export."""

    run_id: str = ""
    phase: str = ""
    table_id: str = ""
    logical_table_name: str = ""
    comparison_mode: str = ""
    validation_table_id: Optional[str] = None
    record_kind: str = KIND_FILE
    source_filename: Optional[str] = None
    source_path: Optional[str] = None
    archive_name: Optional[str] = None
    file_format: Optional[str] = None
    delimiter: Optional[str] = None
    delimiter_source: Optional[str] = None
    header_status: Optional[str] = None
    target_kind: Optional[str] = None
    target_filename: Optional[str] = None
    target_path: Optional[str] = None
    target_table: Optional[str] = None
    source_row_count: Optional[int] = None
    target_row_count: Optional[int] = None
    staged_source_table: Optional[str] = None
    staged_target_table: Optional[str] = None
    processing_status: str = PROCESSING_NOT_EXECUTED
    skipped_status: Optional[str] = None
    rejected_status: Optional[str] = None
    validation_status: Optional[str] = None
    message: Optional[str] = None
    recorded_at: str = field(
        default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds")
    )

    def as_row(self) -> Dict[str, Any]:
        values = asdict(self)
        return {name: values.get(name) for name in FIELD_ORDER}


class FileReconciliationLedger:
    """Collect, update and export the run's file-level metadata."""

    def __init__(self, run_id: str, phase: str):
        self.run_id = run_id
        self.phase = phase
        self._records: List[FileReconciliationRecord] = []

    # ── collection ────────────────────────────────────────────────────

    def add(self, record: FileReconciliationRecord) -> FileReconciliationRecord:
        record.run_id = record.run_id or self.run_id
        record.phase = record.phase or self.phase
        self._records.append(record)
        return record

    def extend(self, records: Iterable[FileReconciliationRecord]) -> None:
        for record in records:
            self.add(record)

    @property
    def records(self) -> Sequence[FileReconciliationRecord]:
        return tuple(self._records)

    def apply_validation_results(self, results: Iterable[Any]) -> None:
        """Copy each table's overall status onto the records it belongs to."""
        by_table: Dict[str, Any] = {}
        for result in results:
            status = getattr(result, "overall_status", None)
            by_table[str(getattr(result, "table_id", ""))] = (
                getattr(status, "value", status), getattr(result, "error_message", None)
            )
        for record in self._records:
            entry = by_table.get(str(record.validation_table_id or ""))
            if entry is None:
                continue
            status, error = entry
            record.validation_status = status
            if error and not record.message:
                record.message = str(error)

    # ── export ────────────────────────────────────────────────────────

    def write(
        self, directory: Path, formats: Sequence[str]
    ) -> Dict[str, Path]:
        """Write the ledger in every configured format. Returns the paths."""
        if not self._records:
            return {}
        directory = Path(directory)
        filesystem_path(directory).mkdir(parents=True, exist_ok=True)
        stem = (
            f"file_reconciliation_{_token(self.phase)}_{_token(self.run_id)}_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        rows = [record.as_row() for record in self._records]
        written: Dict[str, Path] = {}
        for fmt in formats:
            name = str(fmt).lower()
            try:
                if name == "json":
                    path = directory / f"{stem}.json"
                    filesystem_path(path).write_text(
                        json.dumps(
                            {
                                "run_id": self.run_id,
                                "phase": self.phase,
                                "record_count": len(rows),
                                "records": rows,
                            },
                            indent=2, default=str,
                        ),
                        encoding="utf-8",
                    )
                elif name == "csv":
                    import pandas as pd

                    path = directory / f"{stem}.csv"
                    pd.DataFrame(rows, columns=list(FIELD_ORDER)).to_csv(
                        filesystem_path(path), index=False
                    )
                elif name in ("excel", "xlsx"):
                    import pandas as pd

                    path = directory / f"{stem}.xlsx"
                    pd.DataFrame(rows, columns=list(FIELD_ORDER)).to_excel(
                        filesystem_path(path),
                        index=False,
                        sheet_name="File Reconciliation",
                    )
                else:
                    logger.warning(
                        "Unsupported File reconciliation format '%s'; skipped",
                        fmt,
                    )
                    continue
            except Exception as exc:
                logger.error(
                    "File reconciliation export failed | format=%s | error=%s",
                    name, exc,
                )
                continue
            # Return the same long-path-safe form used for the write. Callers
            # may immediately reopen the CSV/XLSX, which otherwise fails on
            # deeply nested Windows project directories.
            written[name] = filesystem_path(path)
        return written

    # ── summary ───────────────────────────────────────────────────────

    def counts(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for record in self._records:
            counts[record.processing_status] = (
                counts.get(record.processing_status, 0) + 1
            )
        return counts


def _token(value: str) -> str:
    return "".join(
        character if character.isalnum() or character in "-_" else "_"
        for character in str(value)
    ) or "run"
