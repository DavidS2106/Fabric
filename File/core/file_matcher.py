"""Resolve one local source file to exactly one Fabric Lakehouse target file.

Used only by ``comparison_mode: file_to_file``. Every source file gets its own
match and its own validation; nothing is ever consolidated here.

The strategy, the target extension and what happens when a match is missing or
ambiguous all come from ``target.matching`` in the table document, so no
filename, extension or naming convention is written in Python.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from .fabric_file_reader import FabricFile
from .file_config_loader import FileConfigError
from .file_discovery import DiscoveredFile

logger = logging.getLogger(__name__)

STATUS_MATCHED = "matched"
STATUS_MISSING = "missing_target"
STATUS_MULTIPLE = "multiple_targets"


@dataclass(frozen=True)
class MatchResult:
    source: DiscoveredFile
    target: Optional[FabricFile]
    status: str
    #: The configured action for this status: fail | skip | warn.
    action: str
    candidates: Tuple[str, ...]
    message: str

    @property
    def matched(self) -> bool:
        return self.status == STATUS_MATCHED


class FileMatcher:
    """Pair source files with target files under the configured strategy."""

    def __init__(self, matching: Mapping[str, object]):
        self.strategy = str(matching.get("strategy", "same_file_stem"))
        extension = matching.get("target_extension")
        self.target_extension = (
            str(extension).lower().lstrip(".") if extension else None
        )
        self.missing_action = str(matching.get("missing_target", "fail"))
        self.multiple_action = str(matching.get("multiple_targets", "fail"))
        self.case_sensitive = bool(matching.get("case_sensitive", True))

    def match_all(
        self,
        sources: Sequence[DiscoveredFile],
        targets: Sequence[FabricFile],
    ) -> List[MatchResult]:
        index = self._index(targets)
        return [self._match_one(source, index) for source in sources]

    # ── internals ─────────────────────────────────────────────────────

    def _index(
        self, targets: Sequence[FabricFile]
    ) -> Dict[str, List[FabricFile]]:
        index: Dict[str, List[FabricFile]] = {}
        for target in targets:
            if (
                self.target_extension
                and target.extension != self.target_extension
            ):
                continue
            index.setdefault(self._target_key(target), []).append(target)
        return index

    def _match_one(
        self, source: DiscoveredFile, index: Mapping[str, List[FabricFile]]
    ) -> MatchResult:
        key = self._source_key(source)
        candidates = index.get(key, [])
        if not candidates:
            return MatchResult(
                source=source, target=None, status=STATUS_MISSING,
                action=self.missing_action, candidates=(),
                message=(
                    f"No Fabric target file matches '{source.display_path}' "
                    f"under strategy '{self.strategy}'"
                    + (
                        f" with extension '{self.target_extension}'"
                        if self.target_extension else ""
                    )
                ),
            )
        if len(candidates) > 1:
            return MatchResult(
                source=source, target=None, status=STATUS_MULTIPLE,
                action=self.multiple_action,
                candidates=tuple(item.relative_path for item in candidates),
                message=(
                    f"{len(candidates)} Fabric target files match "
                    f"'{source.display_path}': "
                    + ", ".join(item.relative_path for item in candidates)
                ),
            )
        target = candidates[0]
        return MatchResult(
            source=source, target=target, status=STATUS_MATCHED,
            action="proceed", candidates=(target.relative_path,),
            message=(
                f"'{source.display_path}' resolved to '{target.relative_path}'"
            ),
        )

    def _source_key(self, source: DiscoveredFile) -> str:
        if self.strategy == "same_file_stem":
            key = source.stem
        elif self.strategy == "same_file_name":
            key = source.name
        elif self.strategy == "relative_path":
            path = PurePosixPath(source.display_path.split("!")[-1])
            key = path.with_suffix("").as_posix()
        else:  # pragma: no cover - the loader validates the strategy
            raise FileConfigError(
                f"Unsupported target.matching.strategy '{self.strategy}'"
            )
        return key if self.case_sensitive else key.casefold()

    def _target_key(self, target: FabricFile) -> str:
        if self.strategy == "same_file_stem":
            key = target.stem
        elif self.strategy == "same_file_name":
            key = target.name
        else:
            key = PurePosixPath(target.relative_path).with_suffix("").as_posix()
        return key if self.case_sensitive else key.casefold()
