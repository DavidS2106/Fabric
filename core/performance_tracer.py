"""Detailed performance instrumentation with hierarchical timing breakdown."""

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


@dataclass
class TimingEntry:
    """One timed operation."""
    name: str
    start_time: float
    end_time: Optional[float] = None
    duration_seconds: Optional[float] = None
    parent: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def complete(self, end_time: float) -> None:
        self.end_time = end_time
        self.duration_seconds = round(end_time - self.start_time, 6)


class PerformanceTracer:
    """Hierarchical timing with automatic parent/child relationships."""

    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self.entries: List[TimingEntry] = []
        self._stack: List[str] = []
        self._lock = __import__("threading").Lock()

    @contextmanager
    def span(self, name: str, details: Optional[Dict[str, Any]] = None):
        """Time a block of code with automatic nesting."""
        if not self.enabled:
            yield
            return

        start = time.perf_counter()
        with self._lock:
            parent = self._stack[-1] if self._stack else None
            self._stack.append(name)
            entry = TimingEntry(
                name=name,
                start_time=start,
                parent=parent,
                details=details or {}
            )
            self._entries_idx = len(self.entries)
            self.entries.append(entry)

        try:
            yield entry
        finally:
            with self._lock:
                self._stack.pop()
                entry.complete(time.perf_counter())
                logger.debug(
                    f"[PERF] {name}: {entry.duration_seconds:.3f} sec"
                )

    def summary(self) -> Dict[str, Any]:
        """Generate a hierarchical timing summary."""
        if not self.entries:
            return {}

        by_parent: Dict[Optional[str], List[TimingEntry]] = {}
        for entry in self.entries:
            key = entry.parent
            if key not in by_parent:
                by_parent[key] = []
            by_parent[key].append(entry)

        result = {}
        for entry in by_parent.get(None, []):
            result[entry.name] = self._entry_dict(entry, by_parent)

        return result

    def _entry_dict(self, entry: TimingEntry, by_parent: Dict) -> Dict[str, Any]:
        return {
            "duration_seconds": entry.duration_seconds,
            "start_time": entry.start_time,
            "details": entry.details,
            "children": [
                self._entry_dict(child, by_parent)
                for child in by_parent.get(entry.name, [])
            ],
        }

    def flat_summary(self) -> List[Dict[str, Any]]:
        """Flat list of all timings sorted by duration (slowest first)."""
        timings = [
            {
                "name": e.name,
                "duration_seconds": e.duration_seconds,
                "parent": e.parent,
                "details": e.details,
            }
            for e in self.entries
            if e.duration_seconds is not None
        ]
        return sorted(timings, key=lambda x: x["duration_seconds"], reverse=True)
