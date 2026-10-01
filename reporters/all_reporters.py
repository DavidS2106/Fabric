"""
fabric_migration_validator/reporters/all_reporters.py
──────────────────────────────────────────────────────
Report *content* renderers:

  1. HTMLReporter    — full dashboard with summary + drilldown
  2. ExcelReporter   — multi-sheet: Summary | By Layer | Failures | All Checks
  3. JSONReporter    — machine-readable for CI/CD gates

These classes render one report into a directory they are handed. Where a
report lands is decided exclusively by `report_output` in execution.yml,
resolved through :class:`reporters.report_paths.ReportPathResolver` and driven
by :class:`reporters.phase_report_tree.PhaseReportTreeWriter` — no reporter
here builds a phase folder, run folder or output tree of its own.
"""

from __future__ import annotations

import json
import html
import hashlib
import re
from datetime import datetime
from pathlib import Path
from typing import Dict

from core.config_loader import VALIDATION_PROFILE_ORDER
from core.models import RunSummary, ValidationStatus
from .table_identity import primary_key_label


_COMPARISON_DETAIL_FIELDS = (
    "validation_profile",
    "comparison_mode",
    "sampling_strategy",
    "key_mapping_source",
    "key_columns_used",
    "records_compared",
    "records_checked",
    "matched_records",
    "mismatch_records",
    "execution_time",
    "exception_category",
    "source_datatypes",
    "target_datatypes",
    "source_precision",
    "target_precision",
    "comparison_strategy",
    "fallback_used",
    "error_reason",
    "final_status",
)

_DATA_VALUE_DETAIL_FIELDS = (
    "validation_category",
    "validation_profile",
    "check_name",
    "status",
    "source_value",
    "target_value",
    "source_null_count",
    "target_null_count",
    "mismatch_count",
    "failure_reason",
    "execution_time",
)

_EDGE_CASE_DETAIL_FIELDS = (
    "validation_category",
    "validation_profile",
    "check_name",
    "status",
    "source_record_count",
    "target_record_count",
    "failure_reason",
)

_AUDIT_DETAIL_FIELDS = (
    "validation_category",
    "validation_profile",
    "check_name",
    "audit_columns_checked",
    "source_record_count",
    "target_record_count",
    "status",
    "failure_reason",
    "execution_time",
)

_UNSAFE_DOM_ID = re.compile(r"[^A-Za-z0-9_-]+")
_PROFILE_ORDER_INDEX = {
    profile: index for index, profile in enumerate(VALIDATION_PROFILE_ORDER)
}


def _safe_dom_id(value: object) -> str:
    """Return a readable, collision-resistant HTML identifier."""
    raw = str(value)
    slug = _UNSAFE_DOM_ID.sub("-", raw).strip("-_").lower() or "item"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:10]
    return f"{slug}-{digest}"


def _profile_result_sort_key(result) -> tuple:
    phase = str(result.migration_phase)
    return (
        _PROFILE_ORDER_INDEX.get(phase, len(_PROFILE_ORDER_INDEX)),
        phase,
        str(result.table_id),
    )


# ─────────────────────────────────────────────────────────────────────────────
# SVG icon library (inline, no external deps)
# ─────────────────────────────────────────────────────────────────────────────
_ICONS: Dict[str, str] = {
    "home": (
        '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">'
        '<path d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10'
        ' a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6"/>'
        "</svg>"
    ),
    "file": (
        '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">'
        '<rect x="4" y="2" width="16" height="20" rx="2"/>'
        '<line x1="8" y1="6" x2="16" y2="6"/>'
        '<line x1="8" y1="10" x2="16" y2="10"/>'
        '<line x1="8" y1="14" x2="12" y2="14"/>'
        "</svg>"
    ),
    "chevron_left": (
        '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">'
        '<polyline points="15 18 9 12 15 6"/>'
        "</svg>"
    ),
    "chevron_right": (
        '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">'
        '<polyline points="9 18 15 12 9 6"/>'
        "</svg>"
    ),
    "double_chevron_left": (
        '<svg id="sidebar-collapse-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" '
        'stroke="currentColor" stroke-width="2.5" style="transition:transform 0.25s ease;">'
        '<polyline points="15 18 9 12 15 6"/>'
        '<polyline points="9 18 3 12 9 6"/>'
        "</svg>"
    ),
    "chevron_down": (
        '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">'
        '<polyline points="6 9 12 15 18 9"/>'
        "</svg>"
    ),
    "activity": (
        '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">'
        '<polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>'
        "</svg>"
    ),
}

def _icon(name: str) -> str:
    return _ICONS.get(name, "")

class ReportTemplateBuilder:
    MASTECH_LOGO = "https://i.ibb.co/HDXXnh72/image.jpg"

    def __init__(self, logo_url: str = "", primary_color: str = "#153D64", sidebar_width: int = 320, header_height: int = 72):
        self.logo_url = logo_url or self.MASTECH_LOGO
        self.primary_color = primary_color
        self.sidebar_width = sidebar_width
        self.header_height = header_height

    def get_styles(self, extra_css: str = "") -> str:
        sw, hh, pc = self.sidebar_width, self.header_height, self.primary_color
        return f"""<style>
        :root {{ --header-height: {hh}px; --sidebar-width: {sw}px; --primary: {pc}; --primary-dark: #0d2b4a; 
                --pass: #22c55e; --fail: #ef4444; --warn: #f59e0b; --error: #dc2626; --skip: #94a3b8; 
                --bg: #f8fafc; --card: #ffffff; --border: #e2e8f0; --text: #1e293b; --muted: #64748b; }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, system-ui, sans-serif; background: var(--bg); color: var(--text); line-height: 1.6; display: flex; flex-direction: column; height: 100%; }}
        .top-header {{ position: fixed; top: 0; left: 0; right: 0; height: var(--header-height); background: linear-gradient(135deg, var(--primary) 0%, var(--primary-dark) 100%); color: white; display: flex; align-items: center; justify-content: space-between; padding: 0 32px; z-index: 1100; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
        .top-header-left {{ display: flex; align-items: center; gap: 16px; }}
        .top-header-left img {{ height: 32px; }}
        .top-header-divider {{ width: 1px; height: 32px; background: rgba(255,255,255,0.2); }}
        .top-header-title {{ display: flex; flex-direction: column; }}
        .title-main {{ font-size: 1rem; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; }}
        .title-sub {{ font-size: 0.7rem; opacity: 0.8; }}
        .layout-container {{ display: flex; flex: 1; padding-top: var(--header-height); }}
        .sidebar {{ width: var(--sidebar-width); background: white; border-right: 1px solid var(--border); position: fixed; left: 0; top: var(--header-height); bottom: 0; overflow-y: auto; transition: transform 0.3s ease; z-index: 1000; }}
        .sidebar.collapsed {{ transform: translateX(-100%); }}
        .sidebar-home-link {{ display: flex; align-items: center; gap: 10px; padding: 16px 20px; font-weight: 600; border-bottom: 1px solid var(--border); cursor: pointer; }}
        .nav-item {{ display: flex; align-items: center; gap: 10px; padding: 10px 24px; cursor: pointer; font-size: 0.85rem; border-left: 3px solid transparent; }}
        .nav-item:hover {{ background: #f1f5f9; }}
        .nav-item.active {{ background: #e2e8f0; border-left-color: var(--primary); }}
        .main-content {{ flex: 1; margin-left: var(--sidebar-width); transition: margin-left 0.3s ease; min-height: calc(100vh - var(--header-height)); display: flex; flex-direction: column; }}
        .main-content.expanded {{ margin-left: 0; }}
        .page-wrapper {{ padding: 32px; flex: 1; display: none; flex-direction: column; }}
        .page-wrapper.active {{ display: flex; }}
        .page-footer {{ padding: 12px; border-top: 1px solid var(--border); text-align: center; font-size: 0.75rem; background: white; color: var(--muted); }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 16px; background: white; border-radius: 8px; overflow: hidden; }}
        th {{ background: #f8fafc; padding: 12px 16px; text-align: left; font-size: 0.75rem; text-transform: uppercase; color: var(--muted); border-bottom: 1px solid var(--border); }}
        td {{ padding: 12px 16px; border-bottom: 1px solid var(--border); font-size: 0.85rem; }}
        .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.7rem; font-weight: 700; text-transform: uppercase; }}
        .badge-pass {{ background: #dcfce7; color: #166534; }}
        .badge-fail {{ background: #fee2e2; color: #991b1b; }}
        .badge-warn {{ background: #fef9c3; color: #854d0e; }}
        .badge-error {{ background: #fecaca; color: #991b1b; }}
        .badge-framework_error {{ background: #fecaca; color: #991b1b; }}
        .badge-config_error {{ background: #fed7aa; color: #9a3412; }}
        .badge-warning {{ background: #fef9c3; color: #854d0e; }}
        .badge-skip {{ background: #f1f5f9; color: #475569; }}
        .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 16px; margin-bottom: 24px; }}
        .kpi {{ background: white; border: 1px solid var(--border); border-radius: 8px; padding: 16px; text-align: center; }}
        .kpi-value {{ font-size: 1.5rem; font-weight: 700; color: var(--primary); }}
        .kpi-label {{ font-size: 0.65rem; color: var(--muted); text-transform: uppercase; margin-top: 4px; }}
        #rt-sidebar-reopen-btn {{ display: none; position: fixed; left: 12px; top: calc(var(--header-height) + 12px); z-index: 1100; background: var(--primary); color: white; border: none; width: 32px; height: 32px; border-radius: 6px; cursor: pointer; align-items: center; justify-content: center; }}
        {extra_css}
        </style>"""

    def get_scripts(self) -> str:
        return """<script>
        function toggleSidebar() {
            document.querySelector('.sidebar').classList.toggle('collapsed');
            document.querySelector('.main-content').classList.toggle('expanded');
            let btn = document.getElementById('rt-sidebar-reopen-btn');
            btn.style.display = document.querySelector('.sidebar').classList.contains('collapsed') ? 'flex' : 'none';
        }
        function showPage(pageId) {
            document.querySelectorAll('.page-wrapper').forEach(p => p.classList.remove('active'));
            document.getElementById(pageId).classList.add('active');
            document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
            document.querySelector(`[data-item-id="${pageId}"]`).classList.add('active');
        }
        </script>"""

    def wrap_multi_page(self, title, report_title, pages, run_id, project):
        hh = self.header_height
        nav_html = ""
        content_html = ""
        for i, p in enumerate(pages):
            active = "active" if i == 0 else ""
            nav_html += f'<div class="nav-item {active}" onclick="showPage(\'{p["id"]}\')" data-item-id="{p["id"]}">{_icon(p.get("icon", "file"))} {p["label"]}</div>'
            content_html += f'<div class="page-wrapper {active}" id="{p["id"]}">{p["content"]}</div>'

        return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>{title}</title>{self.get_styles()}</head><body>
        <header class="top-header">
            <div class="top-header-left"><img src="{self.logo_url}"><div class="top-header-divider"></div>
            <div class="top-header-title"><span class="title-main">{report_title}</span><span class="title-sub">Fabric Migration Validator</span></div></div>
            <div style="font-size:0.8rem; opacity:0.8">Run ID: {run_id} | {datetime.now().strftime('%Y-%m-%d %H:%M')}</div>
        </header>
        <div class="layout-container">
            <button id="rt-sidebar-reopen-btn" onclick="toggleSidebar()">{_icon("chevron_right")}</button>
            <aside class="sidebar">
                <div class="sidebar-home-link" onclick="toggleSidebar()">{_icon("home")} <span>Collapse Sidebar</span></div>
                <div style="padding: 16px 20px 8px; font-size: 0.65rem; color: #94a3b8; font-weight: 700; text-transform: uppercase;">Reports</div>
                {nav_html}
            </aside>
            <main class="main-content">{content_html}
                <footer class="page-footer"><strong>Fabric Migration Validator</strong> | Enterprise Data Governance Platform | Version 1.0</footer>
            </main>
        </div>
        {self.get_scripts()}</body></html>"""

# ── Base ──────────────────────────────────────────────────────

class BaseReporter:
    def __init__(
        self,
        reporting_config,
        summary: RunSummary,
        filename_stem: str | None = None,
    ):
        self.cfg = reporting_config
        self.summary = summary

        # Always convert to Path
        self.output_dir = Path(getattr(reporting_config, "output_dir", "output"))
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # ✅ Timestamp (MANDATORY)
        self.ts = summary.started_at.strftime("%Y%m%d_%H%M%S")

        # Callers that own the output tree supply the file name themselves so
        # the report lands directly where the configured layout expects it.
        self.filename_stem = filename_stem

    def _filepath(self, ext: str) -> Path:
        stem = (
            self.filename_stem
            or f"migration_report_{self.summary.run_id}_{self.ts}"
        )
        # The destination is opened through the same helper every other writer
        # uses. A directory can be well under MAX_PATH while the document
        # inside it is not, so the check has to be made on the file itself.
        from .report_paths import filesystem_path

        return filesystem_path(self.output_dir / f"{stem}.{ext}")


# ── HTML Reporter ─────────────────────────────────────────────

class HTMLReporter(BaseReporter):
    @staticmethod
    def _table_page_id(result) -> str:
        identity = f"{result.table_id}\x1f{result.migration_phase}"
        return f"table-{_safe_dom_id(identity)}"

    def generate(self) -> Path:
        s = self.summary
        try:
            path = self._filepath("html")
        except Exception as e:
            raise Exception(f"Failed to build HTML file path: {e}")

        builder = ReportTemplateBuilder()
        val_stats = self._get_validation_stats()

        pages = []
        
        # 1. Home Page (Summary)
        home_content = f"""
        <div class="report-header">
            <h1>Executive Summary</h1>
            <div class="report-meta">Project: {s.project_name} | Duration: {s.duration_seconds:.1f}s</div>
        </div>
        {self._build_kpi_section(val_stats)}
        <div class="section-title">{_icon("activity")} Table-wise Summary</div>
        {self._build_table_summary()}
        """
        pages.append({"id": "home", "label": "Executive Summary", "icon": "home", "content": home_content})

        # 2. Table-wise detail pages
        for r in sorted(
            self.summary.table_results,
            key=_profile_result_sort_key,
        ):
            table_content = self._build_single_table_detail(r)
            pages.append({
                "id": self._table_page_id(r),
                "label": (
                    f"Details: {html.escape(str(r.table_id))} "
                    f"({html.escape(str(r.migration_phase))})"
                ),
                "icon": "file",
                "content": table_content
            })

        html_document = builder.wrap_multi_page(
            title=f"Migration Report - {s.run_id}",
            report_title="Migration Validator",
            pages=pages,
            run_id=s.run_id,
            project=s.project_name
        )

        path.write_text(html_document, encoding="utf-8")
        print(f"  HTML report: {path}")
        return path

    def _build_single_table_detail(self, r) -> str:
        rows = ""
        for c in sorted(r.checks, key=lambda x: x.check_name):
            status = c.status.value
            badge_cls = f"badge-{status.lower()}"
            msg = html.escape(str(c.message if c.message is not None else ""))
            comparison_details = ""
            if c.check_name == "record_comparison":
                detail_rows = "".join(
                    "<div><strong>"
                    + html.escape(field.replace("_", " ").title())
                    + ":</strong> "
                    + html.escape(
                        json.dumps(c.details.get(field), default=str)
                        if isinstance(c.details.get(field), (dict, list))
                        else str(c.details.get(field))
                    )
                    + "</div>"
                    for field in _COMPARISON_DETAIL_FIELDS
                )
                comparison_details = (
                    '<div style="font-size:0.75rem;line-height:1.5">'
                    + detail_rows
                    + "</div>"
                )
            elif c.details.get("validation_category") == "data_value_validation":
                data_value_fields = {
                    **c.details,
                    "check_name": c.check_name,
                    "status": c.status.value,
                    "source_value": c.source_value,
                    "target_value": c.target_value,
                }
                detail_rows = "".join(
                    "<div><strong>"
                    + html.escape(field.replace("_", " ").title())
                    + ":</strong> "
                    + html.escape(str(data_value_fields.get(field)))
                    + "</div>"
                    for field in _DATA_VALUE_DETAIL_FIELDS
                )
                comparison_details = (
                    '<div style="font-size:0.75rem;line-height:1.5">'
                    + detail_rows
                    + "</div>"
                )
            elif c.details.get("validation_category") == "edge_case_validation":
                edge_fields = {
                    **c.details,
                    "check_name": c.check_name,
                    "status": c.status.value,
                }
                detail_rows = "".join(
                    "<div><strong>"
                    + html.escape(field.replace("_", " ").title())
                    + ":</strong> "
                    + html.escape(str(edge_fields.get(field)))
                    + "</div>"
                    for field in _EDGE_CASE_DETAIL_FIELDS
                )
                comparison_details = (
                    '<div style="font-size:0.75rem;line-height:1.5">'
                    + detail_rows
                    + "</div>"
                )
            elif c.details.get("validation_category") == "audit_validation":
                audit_fields = {
                    **c.details,
                    "check_name": c.check_name,
                    "status": c.status.value,
                }
                detail_rows = "".join(
                    "<div><strong>"
                    + html.escape(field.replace("_", " ").title())
                    + ":</strong> "
                    + html.escape(
                        json.dumps(audit_fields.get(field), default=str)
                        if isinstance(audit_fields.get(field), (dict, list))
                        else str(audit_fields.get(field))
                    )
                    + "</div>"
                    for field in _AUDIT_DETAIL_FIELDS
                )
                comparison_details = (
                    '<div style="font-size:0.75rem;line-height:1.5">'
                    + detail_rows
                    + "</div>"
                )
            rows += f"""
            <tr>
              <td><strong>{html.escape(c.check_name)}</strong></td>
              <td class="text-center"><span class="badge {badge_cls}">{status}</span></td>
              <td style="color: var(--muted); font-size: 0.8rem;">{msg}</td>
              <td>{comparison_details}</td>
            </tr>"""

        return f"""
        <div class="report-header">
            <h1>Validation Details — {html.escape(str(r.table_id))}</h1>
            <div class="report-meta">Phase: {html.escape(str(r.migration_phase))} | Layer: {html.escape(str(r.target_layer))}</div>
        </div>
        
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 24px;">
            <div class="kpi">
                <div class="kpi-label">Source Table</div>
                <div style="font-size: 0.9rem; font-weight: 600; margin-top: 4px;">{html.escape(str(r.source_fqn))}</div>
            </div>
            <div class="kpi">
                <div class="kpi-label">Target Table</div>
                <div style="font-size: 0.9rem; font-weight: 600; margin-top: 4px;">{html.escape(str(r.target_fqn))}</div>
            </div>
        </div>

        <table>
            <thead>
                <tr>
                    <th style="width: 250px;">Check Name</th>
                    <th class="text-center" style="width: 120px;">Status</th>
                    <th>Message</th>
                    <th>Validation Details</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>
        """

    def _get_validation_stats(self) -> dict:
        counts = {name: 0 for name in (
            "total", "passed", "data_failed", "framework_errors",
            "config_errors", "warnings", "skipped",
        )}
        for tr in self.summary.table_results:
            for c in tr.checks:
                counts["total"] += 1
                status = c.status.value
                if status == "PASS": counts["passed"] += 1
                elif status == "FAIL": counts["data_failed"] += 1
                elif status in {"ERROR", "FRAMEWORK_ERROR"}: counts["framework_errors"] += 1
                elif status == "CONFIG_ERROR": counts["config_errors"] += 1
                elif status in {"WARN", "WARNING"}: counts["warnings"] += 1
                elif status == "SKIP": counts["skipped"] += 1
        return counts

    def _build_kpi_section(self, stats: dict) -> str:
        s = self.summary
        return f"""
        <div class="kpi-grid">
            <div class="kpi"><div class="kpi-value">{s.total_tables}</div><div class="kpi-label">Total Tables</div></div>
            <div class="kpi"><div class="kpi-value" style="color:var(--pass)">{s.passed_tables}</div><div class="kpi-label">Passed</div></div>
            <div class="kpi"><div class="kpi-value" style="color:var(--fail)">{s.failed_tables}</div><div class="kpi-label">Data Failed</div></div>
            <div class="kpi"><div class="kpi-value" style="color:var(--error)">{s.error_tables}</div><div class="kpi-label">Framework Error</div></div>
            <div class="kpi"><div class="kpi-value" style="color:var(--error)">{s.config_error_tables}</div><div class="kpi-label">Config Error</div></div>
            <div class="kpi"><div class="kpi-value" style="color:var(--warn)">{s.warn_tables}</div><div class="kpi-label">Warning</div></div>
            <div class="kpi"><div class="kpi-value">{s.skipped_tables}</div><div class="kpi-label">Skipped</div></div>
            <div class="kpi"><div class="kpi-value">{stats['total']}</div><div class="kpi-label">Total Checks</div></div>
            <div class="kpi"><div class="kpi-value">{s.pass_rate_pct}%</div><div class="kpi-label">Pass Rate</div></div>
        </div>"""

    def _build_table_summary(self) -> str:
        rows = ""
        for r in sorted(
            self.summary.table_results,
            key=lambda result: (
                result.overall_status.value != "FAIL",
                *_profile_result_sort_key(result),
            ),
        ):
            st = r.overall_status.value
            page_id = self._table_page_id(r)
            rows += f"""
            <tr onclick="showPage('{page_id}')" style="cursor:pointer">
                <td><strong>{html.escape(str(r.table_id))}</strong></td>
                <td>{html.escape(str(r.source_fqn))}</td>
                <td>{html.escape(str(r.target_fqn))}</td>
                <td class="text-center">{html.escape(str(r.target_layer))}</td>
                <td class="text-center">{html.escape(str(r.migration_phase))}</td>
                <td class="text-center"><span class="badge badge-{st.lower()}">{st}</span></td>
                <td class="text-right">{r.duration_seconds:.1f}s</td>
            </tr>"""
        return f"""
        <table>
            <thead>
                <tr>
                    <th>Table ID</th><th>Source</th><th>Target</th>
                    <th class="text-center">Layer</th><th class="text-center">Phase</th>
                    <th class="text-center">Status</th>
                    <th class="text-right">Duration</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>"""



# ── Excel Reporter ────────────────────────────────────────────

class ExcelReporter(BaseReporter):
    def generate(self) -> Path:
        try:
            import openpyxl
            from openpyxl.styles import Font, PatternFill, Alignment
            from openpyxl.utils import get_column_letter
        except ImportError:
            print("  openpyxl not installed — skipping Excel report. "
                  "Install with: pip install openpyxl")
            return None

        s = self.summary
        path = self._filepath("xlsx")
        wb = openpyxl.Workbook()

        # Colour palette
        GREEN = "FF22C55E"; RED = "FFEF4444"; AMBER = "FFF59E0B"
        HEADER = "FF1D4ED8"
        WHITE = "FFE2E8F0"

        def header_style(cell, bg=HEADER):
            cell.font = Font(bold=True, color=WHITE, size=10)
            cell.fill = PatternFill("solid", fgColor=bg)
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

        def status_fill(status: str) -> PatternFill:
            color = {"PASS": GREEN, "FAIL": RED, "WARN": AMBER,
                     "WARNING": AMBER, "ERROR": "FFDC2626",
                     "FRAMEWORK_ERROR": "FFDC2626", "CONFIG_ERROR": "FFF97316",
                     "SKIP": "FF475569"}.get(status, "FFFFFFFF")
            return PatternFill("solid", fgColor=color)

        def auto_width(ws):
            for col in ws.columns:
                max_len = max((len(str(cell.value or "")) for cell in col), default=10)
                ws.column_dimensions[get_column_letter(col[0].column)].width = min(max_len + 4, 40)

        # ── Sheet 1: Executive Summary ──
        ws1 = wb.active
        ws1.title = "Executive Summary"
        ws1.append(["Migration Validation Report", "", "", ""])
        ws1.append([f"Run ID: {s.run_id}", "", f"Project: {s.project_name}", ""])
        ws1.append([f"Started: {s.started_at.strftime('%Y-%m-%d %H:%M UTC')}", "",
                    f"Duration: {s.duration_seconds:.1f}s", ""])
        ws1.append([])
        ws1.append(["Metric", "Value", "Status", ""])
        for cell in ws1[5]:
            header_style(cell)

        kpis = [
            ("Total Tables", s.total_tables, ""),
            ("Passed Tables", s.passed_tables, "PASS"),
            ("Data Failed Tables", s.failed_tables, "FAIL" if s.failed_tables else "PASS"),
            ("Warning Tables", s.warn_tables, "WARNING" if s.warn_tables else "PASS"),
            ("Framework Error Tables", s.error_tables, "FRAMEWORK_ERROR" if s.error_tables else "PASS"),
            ("Config Error Tables", s.config_error_tables, "CONFIG_ERROR" if s.config_error_tables else "PASS"),
            ("Skipped Tables", s.skipped_tables, "SKIP" if s.skipped_tables else "PASS"),
            ("Pass Rate", f"{s.pass_rate_pct}%",
             "PASS" if s.pass_rate_pct >= 95 else "WARN" if s.pass_rate_pct >= 80 else "FAIL"),
        ]
        for metric, val, status in kpis:
            row = ws1.max_row + 1
            ws1.cell(row, 1, metric)
            ws1.cell(row, 2, val)
            if status:
                cell = ws1.cell(row, 3, status)
                cell.fill = status_fill(status)
                cell.font = Font(bold=True, color="FFFFFFFF")
        auto_width(ws1)

        # ── Sheet 2: Table Results ──
        ws2 = wb.create_sheet("Table Results")
        headers = ["Table ID", "Source System", "Source Table", "Target Table",
                   "Layer", "Phase", "Tags", "Checks Pass", "Data Checks Fail",
                   "Framework Errors", "Config Errors", "Warnings", "Skipped Checks",
                   "Overall Status", "Duration (s)", "Error", "Execution Mode",
                   "Parallel", "Batch Enabled", "Batch Size", "Worker Count",
                   "Source Rows", "Target Rows", "Processed Rows", "Total Batches",
                   "Successful Batches", "Failed Batches", "Retry Count",
                   "Throughput Rows/s", "Validation Parallel", "Validation Workers",
                   "Table Wall Clock (s)", "Sequential Validation Sum (s)",
                   "Slowest Validation", "Slowest Validation (s)"]
        ws2.append(headers)
        for cell in ws2[1]:
            header_style(cell)
        for r in sorted(s.table_results,
                        key=lambda x: (x.overall_status != ValidationStatus.FAIL, x.table_id)):
            st = r.overall_status.value
            row_data = [
                r.table_id, r.source_system, r.source_fqn, r.target_fqn,
                r.target_layer, r.migration_phase, ", ".join(r.tags),
                r.pass_count, r.fail_count, r.framework_error_count,
                r.config_error_count, r.warn_count, r.skip_count, st,
                round(r.duration_seconds, 2), r.error_message or "",
                r.execution_metrics.get("execution_mode"),
                r.execution_metrics.get("parallel_execution_status"),
                r.execution_metrics.get("batch_execution_status"),
                r.execution_metrics.get("batch_size"),
                r.execution_metrics.get("worker_count"),
                r.execution_metrics.get("source_row_count"),
                r.execution_metrics.get("target_row_count"),
                r.execution_metrics.get("processed_row_count"),
                r.execution_metrics.get("total_batches"),
                r.execution_metrics.get("successful_batches"),
                r.execution_metrics.get("failed_batches"),
                r.execution_metrics.get("retry_count"),
                r.execution_metrics.get("throughput_rows_per_second"),
                r.execution_metrics.get("validation_parallelism_status"),
                r.execution_metrics.get("validation_worker_count"),
                r.execution_metrics.get("table_execution_time"),
                r.execution_metrics.get("sequential_validation_time"),
                r.execution_metrics.get("slowest_validation"),
                r.execution_metrics.get("slowest_validation_seconds"),
            ]
            row_num = ws2.max_row + 1
            ws2.append(row_data)
            status_cell = ws2.cell(row_num, 14)
            status_cell.fill = status_fill(st)
            status_cell.font = Font(bold=True, color="FFFFFFFF")
        auto_width(ws2)

        # ── Sheet 3: Failed Checks (Drilldown) ──
        ws3 = wb.create_sheet("Failed Checks")
        f_headers = ["Table ID", "Source", "Target", "Layer", "Check Name",
                     "Source Value", "Target Value", "Delta", "Delta %", "Message"]
        ws3.append(f_headers)
        for cell in ws3[1]:
            header_style(cell, bg="FF991B1B")
        for fc in s.failed_checks_flat():
            ws3.append([
                fc["table_id"], fc["source"], fc["target"], fc["layer"], fc["check"],
                str(fc["source_value"]), str(fc["target_value"]),
                str(fc["delta"]), fc["delta_pct"], fc["message"]
            ])
        auto_width(ws3)

        # ── Sheet 4: All Checks Detail ──
        ws4 = wb.create_sheet("All Checks Detail")
        detail_headers = ["Table ID", "Source FQN", "Target FQN", "Layer",
                          "Check Name", "Status", "Source Value",
                          "Target Value", "Delta %", "Message",
                          "Comparison Mode", "Sampling Strategy", "Key Columns Used",
                          "Records Checked", "Matched Records", "Mismatch Records",
                          "Execution Time (s)", "Validation Profile",
                          "Key Mapping Source", "Records Compared",
                          "Validation Category", "Source Null Count",
                          "Target Null Count", "Mismatch Count",
                          "Source Record Count", "Target Record Count",
                          "Audit Columns Checked",
                          "Failure Reason", "Validation Group", "Start Time",
                          "End Time", "Wall Clock Execution Time (s)",
                          "Source Column", "Target Column", "Exception Category",
                          "Source Datatype", "Target Datatype", "Source Precision",
                          "Target Precision", "Comparison Strategy", "Fallback Used",
                          "Error Reason", "Final Status"]
        ws4.append(detail_headers)
        for cell in ws4[1]:
            header_style(cell)
        for r in s.table_results:
            for chk in r.checks:
                row_num = ws4.max_row + 1
                ws4.append([
                    r.table_id, r.source_fqn, r.target_fqn, r.target_layer,
                    chk.check_name, chk.status.value,
                    str(chk.source_value), str(chk.target_value),
                    chk.delta_pct, chk.message,
                    chk.details.get("comparison_mode"),
                    chk.details.get("sampling_strategy"),
                    json.dumps(chk.details.get("key_columns_used", []), default=str),
                    chk.details.get("records_checked"),
                    chk.details.get("matched_records"),
                    chk.details.get("mismatch_records"),
                    chk.details.get(
                        "execution_time", chk.details.get("execution_time_seconds")
                    ),
                    chk.details.get("validation_profile", r.migration_phase),
                    chk.details.get("key_mapping_source"),
                    chk.details.get("records_compared", chk.details.get("records_checked")),
                    chk.details.get("validation_category"),
                    chk.details.get("source_null_count"),
                    chk.details.get("target_null_count"),
                    chk.details.get("mismatch_count"),
                    chk.details.get("source_record_count"),
                    chk.details.get("target_record_count"),
                    json.dumps(chk.details.get("audit_columns_checked", []), default=str),
                    chk.details.get("failure_reason"),
                    chk.details.get("validation_group"),
                    chk.details.get("start_time"),
                    chk.details.get("end_time"),
                    chk.details.get("execution_time_seconds"),
                    chk.details.get("source_column"),
                    chk.details.get("target_column"),
                    chk.details.get("exception_category"),
                    json.dumps(chk.details.get("source_datatype", chk.details.get("source_datatypes")), default=str),
                    json.dumps(chk.details.get("target_datatype", chk.details.get("target_datatypes")), default=str),
                    json.dumps(chk.details.get("source_precision"), default=str),
                    json.dumps(chk.details.get("target_precision"), default=str),
                    chk.details.get("comparison_strategy"),
                    chk.details.get("fallback_used"),
                    chk.details.get("error_reason"),
                    chk.details.get("final_status", chk.status.value),
                ])
                ws4.cell(row_num, 6).fill = status_fill(chk.status.value)
                ws4.cell(row_num, 6).font = Font(bold=True, color="FFFFFFFF")
        auto_width(ws4)

        # ── Sheet 5: MATCHED_RECORDS ──
        ws5 = wb.create_sheet("MATCHED_RECORDS")
        ws5.append(["Table Name", "Primary Key", "Business Key", "Source Hash", "Target Hash", "Match Status", "Validation Timestamp"])
        for cell in ws5[1]: header_style(cell, bg="FF166534")
        for r in s.table_results:
            for rec in r.matched_records:
                ws5.append([r.table_id, primary_key_label(r, rec.primary_key), rec.business_key, rec.source_hash, rec.target_hash, rec.match_status, rec.timestamp.strftime("%Y-%m-%d %H:%M:%S")])
        auto_width(ws5)

        # ── Sheet 6: MISMATCHED_RECORDS ──
        ws6 = wb.create_sheet("MISMATCHED_RECORDS")
        ws6.append(["Table Name", "Primary Key", "Column Name", "Source Value", "Target Value", "Difference Type", "Validation Timestamp"])
        for cell in ws6[1]: header_style(cell, bg="FF991B1B")
        for r in s.table_results:
            for rec in r.mismatched_records:
                ws6.append([r.table_id, primary_key_label(r, rec.primary_key), rec.column_name, str(rec.source_value), str(rec.target_value), rec.difference_type, rec.timestamp.strftime("%Y-%m-%d %H:%M:%S")])
        auto_width(ws6)

        # ── Sheet 7: MISSING_IN_SOURCE ──
        ws7 = wb.create_sheet("MISSING_IN_SOURCE")
        ws7.append(["Table Name", "Primary Key", "Target Record Data", "Detection Timestamp"])
        for cell in ws7[1]: header_style(cell, bg="FF7C2D12")
        for r in s.table_results:
            for rec in r.missing_in_source:
                ws7.append([r.table_id, primary_key_label(r, rec.primary_key, "target"), rec.record_data, rec.timestamp.strftime("%Y-%m-%d %H:%M:%S")])
        auto_width(ws7)

        # ── Sheet 8: MISSING_IN_TARGET ──
        ws8 = wb.create_sheet("MISSING_IN_TARGET")
        ws8.append(["Table Name", "Primary Key", "Source Record Data", "Detection Timestamp"])
        for cell in ws8[1]: header_style(cell, bg="FF7C2D12")
        for r in s.table_results:
            for rec in r.missing_in_target:
                ws8.append([r.table_id, primary_key_label(r, rec.primary_key), rec.record_data, rec.timestamp.strftime("%Y-%m-%d %H:%M:%S")])
        auto_width(ws8)

        # ── Sheet 9: Final Reconciliation ──
        ws9 = wb.create_sheet("Final Reconciliation")
        ws9.append(["Table Name", "Source Records", "Target Records",
                    "Comparison Mode", "Sampling Strategy", "Key Columns Used",
                    "Records Checked", "Matched Records", "Mismatched Records",
                    "Missing in Source", "Missing in Target", "Pass %", "Status",
                    "PASS", "DATA_FAIL", "FRAMEWORK_ERROR", "CONFIG_ERROR",
                    "WARNING", "SKIP"])
        for cell in ws9[1]: header_style(cell)
        for r in s.table_results:
            src_count = sum(c.source_value for c in r.checks if c.check_name == "row_count" and isinstance(c.source_value, (int, float))) or 0
            tgt_count = sum(c.target_value for c in r.checks if c.check_name == "row_count" and isinstance(c.target_value, (int, float))) or 0
            
            comp_res = next((c for c in r.checks if c.check_name == "record_comparison"), None)
            if comp_res:
                comp_details = comp_res.details
                mode = comp_details.get("comparison_mode")
                strategy = comp_details.get("sampling_strategy")
                keys_used = json.dumps(comp_details.get("key_columns_used", []), default=str)
                checked = comp_details.get("records_checked", 0)
                matched = comp_details.get("matched_records", 0)
                mismatched = comp_details.get("mismatch_records", 0)
                missing_src = comp_details.get("missing_in_source", 0)
                missing_tgt = comp_details.get("missing_in_target", 0)
            else:
                mode = strategy = keys_used = checked = "N/A"
                matched = mismatched = missing_src = missing_tgt = "N/A"
                
            total_checks = (r.pass_count + r.fail_count)
            pass_rate = f"{r.pass_count/total_checks*100:.1f}%" if total_checks > 0 else "0%"
            
            ws9.append([
                r.table_id, src_count, tgt_count, mode, strategy, keys_used, checked,
                matched, mismatched, missing_src, missing_tgt,
                pass_rate, r.overall_status.value, r.pass_count, r.fail_count,
                r.framework_error_count, r.config_error_count, r.warn_count,
                r.skip_count,
            ])
        auto_width(ws9)

        # ── Sheet 10: Execution Performance ──
        ws10 = wb.create_sheet("Execution Performance")
        performance_headers = [
            "Phase", "Table ID", "Source System", "Target System",
            "Validation Name", "Execution Strategy", "Batch Strategy",
            "Source Execution Time (s)", "Target Execution Time (s)",
            "Comparison Time (s)", "Total Execution Time (s)",
            "Rows Processed", "Batch Count", "Batch Size", "Worker Count",
        ]
        ws10.append(performance_headers)
        for cell in ws10[1]:
            header_style(cell)
        performance_records = s.execution_metrics.get("validation_performance", []) or []
        for record in sorted(
            performance_records,
            key=lambda item: item.get("total_execution_time") or 0,
            reverse=True,
        ):
            ws10.append([
                record.get("phase"),
                record.get("table_id"),
                record.get("source_system"),
                record.get("target_system"),
                record.get("validation_name"),
                record.get("execution_strategy"),
                record.get("batch_strategy"),
                record.get("source_execution_time"),
                record.get("target_execution_time"),
                record.get("comparison_time"),
                record.get("total_execution_time"),
                record.get("rows_processed"),
                record.get("batch_count"),
                record.get("batch_size"),
                record.get("worker_count"),
            ])
        performance_summary = s.execution_metrics.get("performance_summary", {}) or {}
        if performance_summary:
            ws10.append([])
            ws10.append(["Performance Finding", "Detail"])
            for cell in ws10[ws10.max_row]:
                header_style(cell)
            for label, key in (
                ("Slowest validation", "slowest_validation"),
                ("Slowest table", "slowest_table"),
                ("Slowest connector operation", "slowest_connector_operation"),
                ("Highest batch count", "highest_batch_count"),
                ("Execution strategy counts", "execution_strategy_counts"),
            ):
                if key in performance_summary:
                    ws10.append([
                        label,
                        json.dumps(performance_summary[key], default=str),
                    ])
        auto_width(ws10)

        wb.save(path)
        print(f"  Excel report: {path}")
        return path


# ── JSON Reporter ─────────────────────────────────────────────

class JSONReporter(BaseReporter):
    """Machine-readable output for CI/CD pipeline gates."""

    def generate(self) -> Path:
        s = self.summary
        try:
          path = self._filepath("json")
        except Exception as e:
          raise Exception(f"Failed to build JSON file path: {e}")

        data = {
            "run_id": s.run_id,
            "project_name": s.project_name,
            "started_at": s.started_at.isoformat(),
            "completed_at": s.completed_at.isoformat() if s.completed_at else None,
            "duration_seconds": s.duration_seconds,
            "execution_metrics": s.execution_metrics,
            "summary": {
                "total_tables": s.total_tables,
                "passed": s.passed_tables,
                "failed": s.failed_tables,
                "warned": s.warn_tables,
                "errors": s.error_tables,
                "data_fail": s.failed_tables,
                "framework_error": s.error_tables,
                "config_error": s.config_error_tables,
                "warning": s.warn_tables,
                "skip": s.skipped_tables,
                "pass_rate_pct": s.pass_rate_pct,
                "overall_status": s.overall_status.value,
            },
            "tables": [
                {
                    "table_id": r.table_id,
                    "source_system": r.source_system,
                    "source_fqn": r.source_fqn,
                    "target_fqn": r.target_fqn,
                    "target_layer": r.target_layer,
                    "migration_phase": r.migration_phase,
                    "tags": r.tags,
                    "status": r.overall_status.value,
                    "duration_seconds": r.duration_seconds,
                    "execution_metrics": r.execution_metrics,
                    "checks": [
                        {
                            "check_name": c.check_name,
                            "status": c.status.value,
                            "source_value": c.source_value,
                            "target_value": c.target_value,
                            "delta_pct": c.delta_pct,
                            "message": c.message,
                            "details": c.details,
                            "validation_category": c.details.get(
                                "validation_category"
                            ),
                            "source_null_count": c.details.get("source_null_count"),
                            "target_null_count": c.details.get("target_null_count"),
                            "mismatch_count": c.details.get("mismatch_count"),
                            "source_record_count": c.details.get(
                                "source_record_count"
                            ),
                            "target_record_count": c.details.get(
                                "target_record_count"
                            ),
                            "audit_columns_checked": c.details.get(
                                "audit_columns_checked"
                            ),
                            "failure_reason": c.details.get("failure_reason"),
                            "validation_profile": c.details.get(
                                "validation_profile", r.migration_phase
                            ),
                            "comparison_mode": c.details.get("comparison_mode"),
                            "sampling_strategy": c.details.get("sampling_strategy"),
                            "key_mapping_source": c.details.get("key_mapping_source"),
                            "key_columns_used": c.details.get("key_columns_used"),
                            "records_compared": c.details.get(
                                "records_compared", c.details.get("records_checked")
                            ),
                            "records_checked": c.details.get("records_checked"),
                            "matched_records": c.details.get("matched_records"),
                            "mismatch_records": c.details.get("mismatch_records"),
                            "execution_time": c.details.get("execution_time"),
                            "exception_category": c.details.get("exception_category"),
                            "source_column": c.details.get("source_column"),
                            "target_column": c.details.get("target_column"),
                            "source_datatype": c.details.get(
                                "source_datatype", c.details.get("source_datatypes")
                            ),
                            "target_datatype": c.details.get(
                                "target_datatype", c.details.get("target_datatypes")
                            ),
                            "source_precision": c.details.get("source_precision"),
                            "target_precision": c.details.get("target_precision"),
                            "comparison_strategy": c.details.get("comparison_strategy"),
                            "fallback_used": c.details.get("fallback_used"),
                            "error_reason": c.details.get("error_reason"),
                            "final_status": c.details.get("final_status", c.status.value),
                        }
                        for c in r.checks
                    ]
                }
                for r in s.table_results
            ]
        }

        path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        print(f"  JSON report:  {path}")
        return path
