"""Route one discovered file to its reader and stream it in bounded chunks.

Every behaviour is configuration: the delimiter, the encoding, the quote and
escape characters, the header, the spreadsheet sheet, the JSON shape, the
compression, the date parsing and the column types. Nothing is assumed. When a
delimited file states no delimiter, one is detected from the universal
candidate list in ``file_validation.delimiters`` — it is never guessed at
outside that list.

Readers yield ``pandas`` frames of at most ``staging.chunk_rows`` rows, so a
file of any size is read with bounded memory and is never concatenated whole.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Mapping, Optional

import pandas as pd

from .file_config_loader import DelimiterPolicy, FileConfigError, FileFrameworkSettings
from .file_discovery import DiscoveredFile
from .header_resolver import HeaderDecision

logger = logging.getLogger(__name__)

#: Reader options a table may configure, per reader family. Anything else is
#: rejected rather than silently forwarded to pandas.
SUPPORTED_OPTIONS: Dict[str, frozenset] = {
    "delimited": frozenset({
        "header", "column_names", "delimiter", "encoding", "quotechar",
        "escapechar", "doublequote", "na_values", "keep_default_na",
        "skiprows", "skipfooter", "comment", "dtype", "parse_dates",
        "thousands", "decimal", "compression", "on_bad_lines",
        "lineterminator", "true_values", "false_values", "skipinitialspace",
        "usecols", "engine",
    }),
    "json": frozenset({
        "lines", "orient", "encoding", "dtype", "compression",
        "convert_dates", "keep_default_dates", "column_names", "header",
    }),
    "excel": frozenset({
        "header", "column_names", "sheet_name", "skiprows", "skipfooter",
        "dtype", "usecols", "engine", "na_values", "keep_default_na",
    }),
    "sas": frozenset({
        "header", "column_names", "encoding", "compression", "index",
    }),
    "parquet": frozenset({"columns", "column_names", "header"}),
}

#: Options consumed by the File module itself, never forwarded to pandas.
MODULE_OPTIONS = frozenset({"header", "column_names"})

#: ``dtype`` values the File module understands as a whole-frame policy.
DTYPE_POLICIES = {"infer": None, "string": "string", "text": "string"}


class FileReadError(RuntimeError):
    """Raised when a discovered file cannot be read as configured."""


@dataclass(frozen=True)
class ReadPlan:
    """Everything decided about one file before a single row is read."""

    discovered: DiscoveredFile
    reader: str
    options: Mapping[str, Any]
    header: HeaderDecision
    delimiter: Optional[str] = None
    delimiter_source: Optional[str] = None

    def as_details(self) -> Dict[str, Any]:
        details: Dict[str, Any] = {
            "reader": self.reader,
            "header_status": self.header.status,
        }
        if self.delimiter is not None:
            details["delimiter"] = self.delimiter
            details["delimiter_source"] = self.delimiter_source
        return details


class FileReader:
    """Turn a discovered file into a bounded stream of frames."""

    def __init__(self, settings: FileFrameworkSettings):
        self.settings = settings

    # ── planning ──────────────────────────────────────────────────────

    def plan(
        self,
        discovered: DiscoveredFile,
        options: Mapping[str, Any],
        header: HeaderDecision,
    ) -> ReadPlan:
        self._validate_options(discovered, options)
        delimiter = None
        source = None
        if discovered.reader == "delimited":
            configured = options.get("delimiter")
            if configured is not None:
                if not isinstance(configured, str) or len(configured) != 1:
                    raise FileConfigError(
                        f"{discovered.display_path}: "
                        "source.file.options.delimiter must be a single "
                        "character"
                    )
                delimiter, source = configured, "configured"
            else:
                delimiter, source = self._detect_delimiter(discovered, options)
        return ReadPlan(
            discovered=discovered,
            reader=discovered.reader,
            options=dict(options),
            header=header,
            delimiter=delimiter,
            delimiter_source=source,
        )

    def columns(self, plan: ReadPlan) -> List[str]:
        """The column order of one file, read without loading its rows.

        A file that holds a header and no records still has a schema, so the
        column order is taken from the header rather than from a first row.
        """
        if plan.header.column_names and not plan.header.has_header:
            return list(plan.header.column_names)
        peeked = self._peek_columns(plan)
        if peeked:
            return peeked
        for frame in self.read_chunks(plan, chunk_rows=1, limit_chunks=1):
            return [str(column) for column in frame.columns]
        if plan.header.column_names:
            return list(plan.header.column_names)
        raise FileReadError(
            f"{plan.discovered.display_path} yielded no records, so its "
            "columns cannot be established"
        )

    def _peek_columns(self, plan: ReadPlan) -> List[str]:
        """Column names read from the file's own schema, when it has one."""
        try:
            if plan.reader == "delimited":
                options = self._pandas_options(plan)
                options["sep"] = plan.delimiter
                options["header"] = 0 if plan.header.has_header else None
                if plan.header.column_names:
                    options["names"] = list(plan.header.column_names)
                options.pop("skipfooter", None)
                frame = pd.read_csv(plan.discovered.path, nrows=0, **options)
            elif plan.reader == "excel":
                options = self._pandas_options(plan)
                options.setdefault("sheet_name", 0)
                options["header"] = 0 if plan.header.has_header else None
                if plan.header.column_names:
                    options["names"] = list(plan.header.column_names)
                frame = pd.read_excel(plan.discovered.path, nrows=0, **options)
            elif plan.reader == "parquet":
                import pyarrow.parquet as pq

                return [
                    str(name)
                    for name in pq.ParquetFile(plan.discovered.path).schema_arrow.names
                ]
            else:
                return []
        except Exception:
            logger.debug(
                "Column peek fell back to reading a chunk | file=%s",
                plan.discovered.display_path, exc_info=True,
            )
            return []
        return [str(column).strip() for column in frame.columns]

    # ── reading ───────────────────────────────────────────────────────

    def read_chunks(
        self, plan: ReadPlan, chunk_rows: int, limit_chunks: Optional[int] = None
    ) -> Iterator[pd.DataFrame]:
        """Yield frames of at most *chunk_rows* rows, in file order."""
        if chunk_rows <= 0:
            raise FileConfigError("staging.chunk_rows must be a positive integer")
        reader = {
            "delimited": self._read_delimited,
            "json": self._read_json,
            "excel": self._read_excel,
            "sas": self._read_sas,
            "parquet": self._read_parquet,
        }.get(plan.reader)
        if reader is None:  # pragma: no cover - discovery rejects these first
            raise FileReadError(
                f"{plan.discovered.display_path}: no reader for '{plan.reader}'"
            )
        produced = 0
        try:
            for frame in reader(plan, chunk_rows):
                if frame is None or frame.empty:
                    continue
                yield self._finalize(plan, frame)
                produced += 1
                if limit_chunks is not None and produced >= limit_chunks:
                    return
        except FileReadError:
            raise
        except Exception as exc:
            raise FileReadError(
                f"{plan.discovered.display_path} could not be read: {exc}"
            ) from exc

    # ── reader families ───────────────────────────────────────────────

    def _read_delimited(
        self, plan: ReadPlan, chunk_rows: int
    ) -> Iterator[pd.DataFrame]:
        options = self._pandas_options(plan)
        options["sep"] = plan.delimiter
        options["engine"] = options.get("engine", "python" if _needs_python_engine(options) else "c")
        if plan.header.has_header and plan.header.column_names is None:
            options["header"] = 0
        else:
            options["header"] = 0 if plan.header.has_header else None
            if plan.header.column_names:
                options["names"] = list(plan.header.column_names)
        with pd.read_csv(
            plan.discovered.path, chunksize=chunk_rows, **options
        ) as frames:
            for frame in frames:
                yield frame

    def _read_json(self, plan: ReadPlan, chunk_rows: int) -> Iterator[pd.DataFrame]:
        options = self._pandas_options(plan)
        # `dtype` as a whole-frame policy is applied by `_finalize`; read_json
        # only accepts a bool or a per-column mapping.
        if isinstance(options.get("dtype"), str):
            options.pop("dtype")
        lines = bool(options.pop("lines", True))
        if lines:
            with pd.read_json(
                plan.discovered.path, lines=True, chunksize=chunk_rows, **options
            ) as frames:
                for frame in frames:
                    yield frame
            return
        frame = pd.read_json(plan.discovered.path, lines=False, **options)
        yield from _slice(frame, chunk_rows)

    def _read_excel(self, plan: ReadPlan, chunk_rows: int) -> Iterator[pd.DataFrame]:
        options = self._pandas_options(plan)
        options.setdefault("sheet_name", 0)
        if isinstance(options["sheet_name"], (list, type(None))):
            raise FileConfigError(
                f"{plan.discovered.display_path}: "
                "source.file.options.sheet_name must name exactly one sheet"
            )
        options["header"] = 0 if plan.header.has_header else None
        if plan.header.column_names and not plan.header.has_header:
            options["names"] = list(plan.header.column_names)
        # A spreadsheet has to be parsed as a whole by its engine; it is then
        # handed on in the same bounded chunks as every other reader.
        frame = pd.read_excel(plan.discovered.path, **options)
        yield from _slice(frame, chunk_rows)

    def _read_parquet(
        self, plan: ReadPlan, chunk_rows: int
    ) -> Iterator[pd.DataFrame]:
        import pyarrow.parquet as pq

        options = self._pandas_options(plan)
        options.pop("dtype", None)
        parquet_file = pq.ParquetFile(plan.discovered.path)
        for batch in parquet_file.iter_batches(
            batch_size=chunk_rows, columns=options.get("columns")
        ):
            yield batch.to_pandas()

    def _read_sas(
        self, plan: ReadPlan, chunk_rows: int
    ) -> Iterator[pd.DataFrame]:
        """Stream a self-describing SAS7BDAT dataset in bounded chunks."""
        options = self._pandas_options(plan)
        # Discovery fixes the physical SAS format from the extension.
        reader = pd.read_sas(
            plan.discovered.path,
            format="sas7bdat",
            iterator=True,
            chunksize=chunk_rows,
            **options,
        )
        try:
            for frame in reader:
                yield frame
        finally:
            close = getattr(reader, "close", None)
            if callable(close):
                close()

    # ── shared ────────────────────────────────────────────────────────

    def _finalize(self, plan: ReadPlan, frame: pd.DataFrame) -> pd.DataFrame:
        frame = frame.copy()
        frame.columns = [str(column).strip() for column in frame.columns]
        policy = plan.options.get("dtype")
        if self.settings.staging.column_types == "text" or policy in (
            "string", "text"
        ):
            frame = frame.astype("string")
        return frame

    def _validate_options(
        self, discovered: DiscoveredFile, options: Mapping[str, Any]
    ) -> None:
        supported = SUPPORTED_OPTIONS[discovered.reader]
        unknown = sorted(set(options) - supported)
        if unknown:
            raise FileConfigError(
                f"{discovered.display_path}: source.file.options has "
                f"option(s) the {discovered.reader} reader does not support: "
                + ", ".join(unknown)
            )

    def _pandas_options(self, plan: ReadPlan) -> Dict[str, Any]:
        options = {
            key: value
            for key, value in plan.options.items()
            if key not in MODULE_OPTIONS and key != "delimiter"
        }
        dtype = options.get("dtype")
        if isinstance(dtype, str):
            if dtype not in DTYPE_POLICIES:
                raise FileConfigError(
                    f"{plan.discovered.display_path}: "
                    "source.file.options.dtype must be a column mapping, or "
                    "one of: " + ", ".join(sorted(DTYPE_POLICIES))
                )
            resolved = DTYPE_POLICIES[dtype]
            if resolved is None:
                options.pop("dtype")
            else:
                options["dtype"] = resolved
        return options

    def _detect_delimiter(
        self, discovered: DiscoveredFile, options: Mapping[str, Any]
    ) -> tuple:
        policy: DelimiterPolicy = self.settings.delimiters
        if not policy.auto_detect:
            raise FileConfigError(
                f"{discovered.display_path}: no delimiter is configured and "
                "file_validation.delimiters.auto_detect is false"
            )
        encoding = str(options.get("encoding") or "utf-8")
        with discovered.path.open(
            "r", encoding=encoding, errors="replace", newline=""
        ) as stream:
            sample = stream.read(policy.sample_bytes)
        if not sample.strip():
            raise FileReadError(
                f"{discovered.display_path} is empty; its delimiter cannot be "
                "detected"
            )
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters="".join(policy.values))
            return dialect.delimiter, f"detected:{policy.name_of(dialect.delimiter)}"
        except csv.Error:
            logger.debug(
                "Delimiter sniffing fell back to counting | file=%s",
                discovered.display_path,
            )
        return self._count_delimiter(discovered, sample, policy)

    @staticmethod
    def _count_delimiter(
        discovered: DiscoveredFile, sample: str, policy: DelimiterPolicy
    ) -> tuple:
        lines = [
            line for line in sample.splitlines()[: policy.sample_lines]
            if line.strip()
        ]
        consistent: Dict[str, int] = {}
        present: Dict[str, int] = {}
        for name, value in policy.candidates:
            counts = [line.count(value) for line in lines]
            if not counts or min(counts) < 1:
                continue
            present[value] = sum(counts)
            if len(set(counts)) == 1:
                consistent[value] = counts[0]
        pool = consistent or present
        if not pool:
            if policy.on_undetected == "first_match":
                name, value = policy.candidates[0]
                return value, f"undetected:first_match:{name}"
            raise FileReadError(
                f"{discovered.display_path}: no configured delimiter candidate "
                "separates this file; state source.file.options.delimiter"
            )
        best = max(pool.values())
        winners = [value for value, score in pool.items() if score == best]
        if len(winners) > 1 and policy.on_ambiguous == "error":
            raise FileReadError(
                f"{discovered.display_path}: delimiter is ambiguous between "
                + ", ".join(repr(item) for item in winners)
                + "; state source.file.options.delimiter"
            )
        chosen = winners[0]
        label = "counted" if consistent else "counted_relaxed"
        return chosen, f"{label}:{policy.name_of(chosen)}"


def _needs_python_engine(options: Mapping[str, Any]) -> bool:
    """The C parser rejects a few options; select the engine accordingly."""
    return bool(options.get("skipfooter"))


def _slice(frame: pd.DataFrame, chunk_rows: int) -> Iterator[pd.DataFrame]:
    for start in range(0, len(frame), chunk_rows):
        yield frame.iloc[start:start + chunk_rows]
