"""Lightweight per-table process-memory and framework-temp accounting."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable

_SQLITE_SUFFIXES = (".db", ".sqlite", ".sqlite3", ".db-wal", ".db-shm", ".db-journal")
_SPILL_SUFFIXES = (".csv", ".parquet", ".duckdb")


def _rss_bytes() -> int | None:
    if sys.platform == "win32":
        try:
            import ctypes
            from ctypes import wintypes

            class ProcessMemoryCounters(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            counters = ProcessMemoryCounters()
            counters.cb = ctypes.sizeof(counters)
            handle = ctypes.windll.kernel32.GetCurrentProcess()
            if ctypes.windll.psapi.GetProcessMemoryInfo(
                handle, ctypes.byref(counters), counters.cb
            ):
                return int(counters.WorkingSetSize)
        except (AttributeError, OSError, TypeError, ValueError):
            return None
    try:
        import resource

        value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        return value if sys.platform == "darwin" else value * 1024
    except (ImportError, OSError, ValueError):
        return None


def _files(root: Path) -> Iterable[Path]:
    if not root.exists():
        return ()
    try:
        return tuple(path for path in root.rglob("*") if path.is_file())
    except OSError:
        return ()


def resource_snapshot(directory: Any) -> Dict[str, Any]:
    """Capture current RSS and files owned by the configured temp directory."""
    root = Path(str(directory or "output/temp")).resolve()
    files = list(_files(root))
    sizes = []
    for path in files:
        try:
            sizes.append((path, path.stat().st_size))
        except OSError:
            continue
    sqlite = [item for item in sizes if item[0].name.casefold().endswith(_SQLITE_SUFFIXES)]
    spill = [
        item for item in sizes
        if item[0].name.casefold().endswith(_SPILL_SUFFIXES)
        or item[0].name.casefold().startswith("record_hash_")
    ]
    rss = _rss_bytes()
    return {
        "process_rss_bytes": rss,
        "process_rss_mb": round(rss / (1024 * 1024), 3) if rss is not None else None,
        "temporary_directory": os.fspath(root),
        "temporary_file_count": len(sizes),
        "temporary_directory_bytes": sum(size for _, size in sizes),
        "sqlite_temp_file_count": len(sqlite),
        "sqlite_temp_bytes": sum(size for _, size in sqlite),
        "spill_file_count": len(spill),
        "spill_bytes": sum(size for _, size in spill),
        "remaining_temporary_files": [
            os.fspath(path.relative_to(root)) for path, _ in sizes[:50]
        ],
    }


def cleanup_framework_temp_files(directory: Any) -> Dict[str, Any]:
    """Remove framework-owned SQLite/spill artifacts from the temp root.

    The configured directory is dedicated to validation scratch data.  Only
    recognized spill/database names are removed; report and unrelated files
    are never candidates.
    """
    root = Path(str(directory or "output/temp")).resolve()
    removed_files = 0
    removed_bytes = 0
    failures = []
    for path in _files(root):
        folded = path.name.casefold()
        recognized = (
            folded.endswith(_SQLITE_SUFFIXES)
            or folded.endswith(_SPILL_SUFFIXES)
            or folded.startswith("record_hash_")
        )
        if not recognized:
            continue
        try:
            size = path.stat().st_size
            path.unlink(missing_ok=True)
            removed_files += 1
            removed_bytes += size
        except OSError as exc:
            failures.append(f"{path.name}: {exc}")
    return {
        "temporary_files_removed": removed_files,
        "temporary_bytes_removed": removed_bytes,
        "temporary_cleanup_failures": failures,
    }
