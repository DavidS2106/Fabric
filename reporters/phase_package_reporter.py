"""
reporters/phase_package_reporter.py
───────────────────────────────────
Packages the reports of one phase run into the ZIP that `report_output`
names, inside the very run folder those reports were written to::

    <run root>/<configured zip name>

The packager never renders a report and never builds a path. It archives the
resolved run folder that the report writers just filled, which is what makes
it impossible for a package to carry a file from an earlier run: an earlier
run has a different run folder.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional
from zipfile import ZIP_DEFLATED, ZipFile

from .report_paths import (
    ReportPathError,
    ResolvedReportPaths,
    filesystem_path,
    walk_root,
)


logger = logging.getLogger(__name__)


class PhasePackageReporter:
    """Archive one phase's resolved run folder, exactly as configured."""

    def target_directories(self, paths: ResolvedReportPaths) -> List[Path]:
        """The only directory this writer touches is the run folder itself."""
        return [] if paths.zip_path is None else [paths.zip_path.parent]

    def package(self, paths: ResolvedReportPaths) -> Optional[Path]:
        """Zip every report of this run, or nothing when zipping is off."""
        zip_path = paths.zip_path
        if zip_path is None:
            return None
        if not paths.contains(zip_path):
            raise ReportPathError(
                f"The package {zip_path} would be written outside the run "
                f"folder {paths.run_root}"
            )
        scan_root = walk_root(paths.run_root)
        members = self._members(paths, scan_root)
        if not members:
            raise ReportPathError(
                f"No reports were written under {paths.run_root}, so there is "
                "nothing to package; check the enabled report formats"
            )
        filesystem_path(zip_path.parent).mkdir(parents=True, exist_ok=True)
        try:
            with ZipFile(
                filesystem_path(zip_path), "w", compression=ZIP_DEFLATED
            ) as archive:
                for member in members:
                    archive.write(scan_root / member, member.as_posix())
        except Exception as exc:
            if zip_path.exists():
                zip_path.unlink()
            logger.error(
                "run_id=%s phase=%s zip_path=%s generation_status=NOT_CREATED "
                "failure_reason=%s",
                paths.run_id, paths.phase, zip_path, exc,
            )
            raise
        logger.info(
            "run_id=%s phase=%s zip_path=%s member_count=%d "
            "generation_status=SUCCESS",
            paths.run_id, paths.phase, zip_path, len(members),
        )
        return zip_path

    def generate(self, paths: ResolvedReportPaths) -> Dict[str, Path]:
        """The packaged archive of one phase, keyed for the run report."""
        zip_path = self.package(paths)
        return {} if zip_path is None else {f"{paths.phase}_zip": zip_path}

    @staticmethod
    def _members(paths: ResolvedReportPaths, scan_root: Path) -> List[Path]:
        """Every report file of this run, relative to the run folder.

        The run folder is walked through `scan_root` so that a report whose
        path is longer than the platform's ordinary limit is archived rather
        than silently left out of the package.
        """
        if not scan_root.is_dir():
            return []
        archive_name = paths.zip_path.name if paths.zip_path else None
        members = []
        for item in scan_root.rglob("*"):
            if not item.is_file():
                continue
            relative = item.relative_to(scan_root)
            if relative.as_posix() == archive_name:
                continue
            members.append(relative)
        return sorted(members)
