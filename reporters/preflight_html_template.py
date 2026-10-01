"""Standalone HTML template for the preflight validation report.

This module deliberately has no effect on the JSON or Excel renderers.  It
adapts the Mastech report chrome supplied for this project while keeping the
generated report self-contained and safe to open without network access.
"""

from __future__ import annotations

import html
import json
from typing import Any, Dict, Iterable, Mapping, Sequence


def _text(value: Any) -> str:
    """Return a display value suitable for escaping into an HTML cell."""
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return str(value)


def _status_class(value: Any) -> str:
    normalized = _text(value).upper()
    if normalized in {"PASS", "PASSED"}:
        return "pass"
    if normalized in {"FAIL", "FAILED"}:
        return "fail"
    if normalized in {"WARN", "WARNING", "PARTIALLY_PASSED"}:
        return "warning"
    return ""


def _metric(
    label: str,
    value: Any,
    *,
    css_class: str = "",
    detail: str = "",
) -> str:
    class_name = f" metric-card {css_class}" if css_class else "metric-card"
    detail_html = (
        f'<span class="metric-detail">{html.escape(detail)}</span>'
        if detail else ""
    )
    return (
        f'<article class="{class_name.strip()}">'
        f'<span class="metric-label">{html.escape(label)}</span>'
        f'<strong class="metric-value">{html.escape(_text(value))}</strong>'
        f"{detail_html}</article>"
    )


def render_preflight_html(
    *,
    title: str,
    run_id: Any,
    timestamp: Any,
    validation_type: str,
    overall_status: Any,
    summary: Mapping[str, Any],
    connection_metrics: Mapping[str, Any],
    columns: Sequence[str],
    rows: Iterable[Mapping[str, Any]],
) -> str:
    """Render the complete, self-contained preflight HTML report."""
    escaped_title = html.escape(_text(title))
    escaped_run_id = html.escape(_text(run_id))
    escaped_timestamp = html.escape(_text(timestamp))
    escaped_validation_type = html.escape(_text(validation_type))
    status_text = _text(overall_status or "UNKNOWN").upper()
    status_css = _status_class(status_text) or "neutral"

    table_header = "".join(
        '<th scope="col" tabindex="0" '
        'onclick="sortTable(this.cellIndex)" '
        'onkeydown="sortOnKey(event,this.cellIndex)">'
        f"{html.escape(column)}<span class=\"sort-mark\" aria-hidden=\"true\">↕</span></th>"
        for column in columns
    )
    table_rows = []
    for row in rows:
        cells = []
        for column in columns:
            text = _text(row.get(column))
            css_class = _status_class(text)
            indicator = (
                '<span class="indicator" aria-hidden="true"></span>'
                if css_class else ""
            )
            cells.append(
                f'<td class="{css_class}">{indicator}{html.escape(text)}</td>'
            )
        table_rows.append("<tr>" + "".join(cells) + "</tr>")
    table_body = "".join(table_rows)
    if not table_body:
        table_body = (
            f'<tr class="empty-row"><td colspan="{len(columns)}">'
            "No preflight validation results were produced.</td></tr>"
        )

    overview_cards = "".join((
        _metric("Tables checked", summary.get("total_tables", 0), detail="Preflight scope"),
        _metric("Passed", summary.get("passed_count", 0), css_class="success", detail="Ready to proceed"),
        _metric("Failed", summary.get("failed_count", 0), css_class="danger", detail="Action required"),
        _metric("Warnings", summary.get("warning_count", 0), css_class="caution", detail="Review advised"),
        _metric(
            "Wall-clock duration",
            f'{summary.get("wall_clock_duration", 0)}s',
            detail="Actual elapsed time",
        ),
        _metric(
            "Cumulative worker duration",
            f'{summary.get("cumulative_worker_duration", 0)}s',
            detail="Sum across parallel table workers",
        ),
    ))

    established = connection_metrics.get("connections_established", 0)
    reused = connection_metrics.get("connection_reuse_count", 0)
    cached = connection_metrics.get("cached_connection_count")
    connection_cards = "".join((
        _metric("Connections established", established, detail="Physical connections opened"),
        _metric("Reused by later validations", reused, css_class="brand", detail="Avoided reconnections"),
        _metric("Cached connections", cached if cached is not None else "—", detail="Available for this run"),
    ))

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escaped_title}</title>
<style>
:root {{
  --navy: #153d64;
  --navy-deep: #0d2b4a;
  --blue: #0875c1;
  --blue-pale: #eaf4fb;
  --ink: #172033;
  --muted: #5d6b7c;
  --line: #dce4ec;
  --canvas: #f3f6f9;
  --white: #fff;
  --green: #147a42;
  --green-bg: #e9f7ef;
  --red: #b42318;
  --red-bg: #fff0ee;
  --amber: #9a6700;
  --amber-bg: #fff7dd;
  --header-height: 76px;
  --sidebar-width: 248px;
}}
* {{ box-sizing: border-box; }}
html {{ scroll-behavior: smooth; }}
body {{
  margin: 0;
  color: var(--ink);
  background: var(--canvas);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  font-size: 14px;
  line-height: 1.5;
}}
button, input, select {{ font: inherit; }}
.top-header {{
  position: fixed;
  inset: 0 0 auto 0;
  z-index: 30;
  height: var(--header-height);
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 24px;
  padding: 0 28px;
  color: var(--white);
  background: linear-gradient(120deg, var(--navy), var(--navy-deep));
  box-shadow: 0 3px 15px rgba(10, 35, 58, .24);
}}
.brand {{ display: flex; align-items: center; min-width: 0; gap: 14px; }}
.brand-mark {{
  width: 40px;
  height: 40px;
  flex: 0 0 auto;
  display: grid;
  place-items: center;
  border: 1px solid rgba(255,255,255,.45);
  background: rgba(255,255,255,.12);
  font-size: 14px;
  font-weight: 800;
  letter-spacing: .08em;
}}
.brand-copy {{ min-width: 0; padding-left: 14px; border-left: 1px solid rgba(255,255,255,.3); }}
.brand-title {{ display: block; font-size: 16px; font-weight: 800; letter-spacing: .06em; text-transform: uppercase; }}
.brand-subtitle {{ display: block; margin-top: 2px; color: #c9d9e8; font-size: 12px; }}
.run-meta {{ text-align: right; color: #d9e6f1; font-size: 12px; white-space: nowrap; }}
.run-meta strong {{ display: block; color: var(--white); font-size: 13px; }}
.sidebar {{
  position: fixed;
  inset: var(--header-height) auto 0 0;
  z-index: 20;
  width: var(--sidebar-width);
  background: var(--white);
  border-right: 1px solid var(--line);
  box-shadow: 3px 0 14px rgba(18, 47, 72, .06);
  transition: transform .22s ease;
}}
.sidebar.collapsed {{ transform: translateX(-100%); }}
.sidebar-heading {{ display: flex; align-items: center; justify-content: space-between; padding: 18px; border-bottom: 1px solid var(--line); }}
.sidebar-heading strong {{ color: var(--navy); font-size: 12px; letter-spacing: .1em; text-transform: uppercase; }}
.icon-button {{
  display: inline-grid;
  place-items: center;
  width: 32px;
  height: 32px;
  border: 1px solid #cbd6e1;
  border-radius: 6px;
  color: var(--navy);
  background: var(--white);
  cursor: pointer;
}}
.icon-button:hover, .icon-button:focus-visible {{ border-color: var(--blue); color: var(--blue); outline: 2px solid #b8dcf5; outline-offset: 1px; }}
.nav {{ padding: 14px 10px; }}
.nav a {{
  display: flex;
  align-items: center;
  gap: 11px;
  padding: 11px 13px;
  border-left: 3px solid transparent;
  color: #34465a;
  text-decoration: none;
  font-weight: 600;
}}
.nav a:hover, .nav a:focus-visible {{ color: var(--navy); background: #f4f8fb; border-left-color: var(--blue); outline: none; }}
.nav-icon {{ width: 22px; color: var(--blue); font-weight: 800; text-align: center; }}
.sidebar-note {{ margin: 10px 18px; padding-top: 16px; border-top: 1px solid var(--line); color: var(--muted); font-size: 12px; }}
.sidebar-open {{
  position: fixed;
  z-index: 25;
  top: calc(var(--header-height) + 14px);
  left: 14px;
  display: none;
  color: var(--white);
  border-color: var(--blue);
  background: var(--blue);
}}
.sidebar-open.visible {{ display: inline-grid; }}
.main-content {{ margin-left: var(--sidebar-width); padding-top: var(--header-height); transition: margin-left .22s ease; }}
.main-content.expanded {{ margin-left: 0; }}
.page-shell {{ max-width: 1800px; margin: 0 auto; padding: 32px 34px 18px; }}
.report-heading {{ display: flex; align-items: flex-end; justify-content: space-between; gap: 24px; margin-bottom: 20px; }}
.eyebrow {{ margin: 0 0 5px; color: var(--blue); font-size: 11px; font-weight: 800; letter-spacing: .13em; text-transform: uppercase; }}
h1, h2 {{ margin: 0; color: var(--navy-deep); }}
h1 {{ font-size: clamp(24px, 3vw, 34px); line-height: 1.15; }}
h2 {{ font-size: 19px; }}
.lead {{ margin: 8px 0 0; color: var(--muted); }}
.status-banner {{
  min-width: 180px;
  padding: 13px 18px;
  border: 1px solid #cbd6e1;
  border-left-width: 5px;
  background: var(--white);
}}
.status-banner span {{ display: block; color: var(--muted); font-size: 10px; font-weight: 800; letter-spacing: .1em; text-transform: uppercase; }}
.status-banner strong {{ display: block; margin-top: 2px; font-size: 20px; }}
.status-banner.pass {{ border-left-color: var(--green); }}
.status-banner.pass strong {{ color: var(--green); }}
.status-banner.fail {{ border-left-color: var(--red); }}
.status-banner.fail strong {{ color: var(--red); }}
.status-banner.warning {{ border-left-color: var(--amber); }}
.status-banner.warning strong {{ color: var(--amber); }}
.status-banner.neutral {{ border-left-color: var(--muted); }}
.panel {{ margin-bottom: 22px; border: 1px solid var(--line); background: var(--white); box-shadow: 0 8px 24px rgba(22, 57, 86, .05); }}
.panel-heading {{ display: flex; align-items: center; justify-content: space-between; gap: 18px; padding: 17px 20px; border-bottom: 1px solid var(--line); }}
.panel-heading p {{ margin: 2px 0 0; color: var(--muted); font-size: 12px; }}
.metrics-grid {{ display: grid; grid-template-columns: repeat(5, minmax(140px, 1fr)); }}
.connection-grid {{ grid-template-columns: repeat(3, minmax(170px, 1fr)); }}
.metric-card {{ min-height: 114px; padding: 19px 20px; border-right: 1px solid var(--line); background: linear-gradient(180deg, #fff, #fafcfd); }}
.metric-card:last-child {{ border-right: 0; }}
.metric-label {{ display: block; color: var(--muted); font-size: 11px; font-weight: 800; letter-spacing: .06em; text-transform: uppercase; }}
.metric-value {{ display: block; margin: 7px 0 2px; color: var(--navy); font-size: 27px; line-height: 1; }}
.metric-detail {{ color: var(--muted); font-size: 11px; }}
.metric-card.success .metric-value {{ color: var(--green); }}
.metric-card.danger .metric-value {{ color: var(--red); }}
.metric-card.caution .metric-value {{ color: var(--amber); }}
.metric-card.brand .metric-value {{ color: var(--blue); }}
.toolbar {{ display: flex; flex-wrap: wrap; align-items: end; gap: 12px; padding: 16px 20px; background: #f8fafc; border-bottom: 1px solid var(--line); }}
.control {{ display: grid; gap: 5px; }}
.control:first-child {{ min-width: min(420px, 100%); flex: 1; }}
.control label {{ color: #42556b; font-size: 11px; font-weight: 800; letter-spacing: .06em; text-transform: uppercase; }}
.control input, .control select {{ height: 38px; padding: 0 11px; border: 1px solid #bdcad7; border-radius: 5px; color: var(--ink); background: var(--white); }}
.control input:focus, .control select:focus {{ border-color: var(--blue); outline: 3px solid #d8eefc; }}
.result-count {{ margin-left: auto; padding-bottom: 9px; color: var(--muted); font-size: 12px; }}
.table-scroll {{ max-height: 66vh; overflow: auto; }}
table {{ width: 100%; border-collapse: separate; border-spacing: 0; font-size: 12px; }}
th, td {{ padding: 9px 10px; border-right: 1px solid var(--line); border-bottom: 1px solid var(--line); text-align: left; white-space: nowrap; }}
th {{ position: sticky; top: 0; z-index: 2; color: var(--white); background: var(--navy); cursor: pointer; font-size: 11px; letter-spacing: .02em; }}
th:hover, th:focus-visible {{ background: #1d527f; outline: 2px solid #7dc4f1; outline-offset: -2px; }}
tbody tr:nth-child(even) {{ background: #f8fafc; }}
tbody tr:hover {{ background: var(--blue-pale); }}
td.pass, td.fail, td.warning {{ font-weight: 700; }}
td.pass {{ color: var(--green); background: var(--green-bg); }}
td.fail {{ color: var(--red); background: var(--red-bg); }}
td.warning {{ color: var(--amber); background: var(--amber-bg); }}
.indicator {{ display: inline-block; width: 7px; height: 7px; margin-right: 6px; border-radius: 50%; background: currentColor; }}
.sort-mark {{ margin-left: 7px; color: #bcd4e6; }}
.empty-row td {{ padding: 42px; color: var(--muted); text-align: center; }}
.page-footer {{ margin-top: 22px; padding: 17px; border-top: 2px solid var(--blue); color: #526174; background: var(--white); text-align: center; font-size: 12px; }}
.footer-sep {{ padding: 0 8px; color: #9ca9b5; }}
@media (max-width: 980px) {{
  :root {{ --sidebar-width: 0px; }}
  .sidebar {{ width: 260px; transform: translateX(-100%); }}
  .sidebar.mobile-open {{ transform: translateX(0); }}
  .sidebar-open {{ display: inline-grid; }}
  .main-content {{ margin-left: 0; }}
  .metrics-grid {{ grid-template-columns: repeat(2, minmax(140px, 1fr)); }}
  .metric-card {{ border-bottom: 1px solid var(--line); }}
}}
@media (max-width: 640px) {{
  .top-header {{ height: auto; min-height: var(--header-height); padding: 12px 16px; }}
  .brand-subtitle, .run-meta span {{ display: none; }}
  .brand-title {{ font-size: 13px; }}
  .page-shell {{ padding: 24px 14px 12px; }}
  .report-heading {{ align-items: stretch; flex-direction: column; }}
  .status-banner {{ min-width: 0; }}
  .metrics-grid, .connection-grid {{ grid-template-columns: 1fr; }}
  .metric-card {{ min-height: 0; border-right: 0; }}
  .result-count {{ width: 100%; margin-left: 0; }}
}}
@media print {{
  body {{ background: var(--white); }}
  .top-header, .sidebar, .sidebar-open, .toolbar {{ display: none !important; }}
  .main-content {{ margin: 0; padding: 0; }}
  .page-shell {{ max-width: none; padding: 0; }}
  .panel {{ box-shadow: none; break-inside: avoid; }}
  .table-scroll {{ max-height: none; overflow: visible; }}
  th {{ position: static; }}
}}
</style>
</head>
<body>
<header class="top-header">
  <div class="brand">
    <span class="brand-mark" aria-label="Mastech Digital">MD</span>
    <div class="brand-copy">
      <span class="brand-title">{escaped_title}</span>
      <span class="brand-subtitle">Enterprise Data Governance Platform</span>
    </div>
  </div>
  <div class="run-meta"><strong>Run ID: {escaped_run_id}</strong><span>{escaped_timestamp}</span></div>
</header>
<button class="icon-button sidebar-open" id="sidebarOpen" onclick="toggleSidebar()" aria-label="Open navigation" title="Open navigation">☰</button>
<aside class="sidebar" id="sidebar" aria-label="Report navigation">
  <div class="sidebar-heading"><strong>Report sections</strong><button class="icon-button" onclick="toggleSidebar()" aria-label="Collapse navigation" title="Collapse navigation">«</button></div>
  <nav class="nav">
    <a href="#overview"><span class="nav-icon">01</span>Overview</a>
    <a href="#validation-results"><span class="nav-icon">02</span>Validation Results</a>
  </nav>
  <p class="sidebar-note">Validation type<br><strong>{escaped_validation_type}</strong></p>
</aside>
<main class="main-content" id="mainContent">
  <div class="page-shell">
    <section id="overview" aria-labelledby="report-title">
      <div class="report-heading">
        <div><p class="eyebrow">Migration readiness</p><h1 id="report-title">{escaped_title}</h1><p class="lead">Connection and object checks required before validation begins.</p></div>
        <div class="status-banner {status_css}"><span>Overall run status</span><strong>{html.escape(status_text)}</strong></div>
      </div>
      <div class="panel" aria-labelledby="readiness-heading">
        <div class="panel-heading"><div><h2 id="readiness-heading">Readiness overview</h2><p>Immediate outcome across the selected table configurations.</p></div></div>
        <div class="metrics-grid">{overview_cards}</div>
      </div>
      <div class="panel" aria-labelledby="connection-heading">
        <div class="panel-heading"><div><h2 id="connection-heading">Connection reuse</h2><p>Connection activity recorded during this preflight run.</p></div></div>
        <div class="metrics-grid connection-grid">{connection_cards}</div>
      </div>
    </section>
    <section class="panel" id="validation-results" aria-labelledby="results-heading">
      <div class="panel-heading"><div><h2 id="results-heading">Validation Results</h2><p>Search, filter, or select any column heading to sort.</p></div></div>
      <div class="toolbar">
        <div class="control"><label for="search">Search results</label><input id="search" type="search" placeholder="Table, server, database, status…" oninput="filterRows()"></div>
        <div class="control"><label for="status">Overall status</label><select id="status" onchange="filterRows()"><option value="">All statuses</option><option value="PASSED">PASSED</option><option value="FAILED">FAILED</option><option value="PARTIALLY_PASSED">PARTIALLY_PASSED</option></select></div>
        <span class="result-count" id="resultCount" aria-live="polite"></span>
      </div>
      <div class="table-scroll"><table id="results"><thead><tr>{table_header}</tr></thead><tbody>{table_body}</tbody></table></div>
    </section>
    <footer class="page-footer"><strong>Preflight Validation</strong><span class="footer-sep">|</span>Enterprise Data Governance Platform<span class="footer-sep">|</span>Mastech Digital<span class="footer-sep">|</span>Version 1.0</footer>
  </div>
</main>
<script>
function toggleSidebar() {{
  var sidebar = document.getElementById('sidebar');
  var main = document.getElementById('mainContent');
  var openButton = document.getElementById('sidebarOpen');
  if (window.matchMedia('(max-width: 980px)').matches) {{
    sidebar.classList.toggle('mobile-open');
  }} else {{
    sidebar.classList.toggle('collapsed');
    main.classList.toggle('expanded');
    openButton.classList.toggle('visible');
  }}
}}
function filterRows() {{
  var query = document.getElementById('search').value.toLowerCase();
  var status = document.getElementById('status').value;
  var rows = document.querySelectorAll('#results tbody tr:not(.empty-row)');
  var visible = 0;
  rows.forEach(function(row) {{
    var text = row.innerText;
    var show = text.toLowerCase().includes(query) && (!status || text.includes(status));
    row.style.display = show ? '' : 'none';
    if (show) visible += 1;
  }});
  document.getElementById('resultCount').textContent = visible + ' of ' + rows.length + ' results';
}}
function sortTable(columnIndex) {{
  var body = document.querySelector('#results tbody');
  var rows = Array.from(body.querySelectorAll('tr:not(.empty-row)'));
  var nextAscending = body.dataset.sortColumn != columnIndex || body.dataset.sortDirection != 'asc';
  rows.sort(function(left, right) {{
    return left.cells[columnIndex].innerText.localeCompare(
      right.cells[columnIndex].innerText, undefined, {{numeric: true, sensitivity: 'base'}}
    ) * (nextAscending ? 1 : -1);
  }});
  rows.forEach(function(row) {{ body.appendChild(row); }});
  body.dataset.sortColumn = columnIndex;
  body.dataset.sortDirection = nextAscending ? 'asc' : 'desc';
}}
function sortOnKey(event, columnIndex) {{
  if (event.key === 'Enter' || event.key === ' ') {{ event.preventDefault(); sortTable(columnIndex); }}
}}
document.addEventListener('DOMContentLoaded', filterRows);
</script>
</body>
</html>"""
