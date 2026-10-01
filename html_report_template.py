"""Shared HTML report template builder for all agents.

Provides a standardized header, collapsible sidebar, and footer
that matches the design language used by the Data Quality and
Data Profiler toolkits. Any agent tool can use this to wrap its
own HTML content inside the standard chrome.

Usage (quick):
    from toolkit.html_report_template import ReportTemplateBuilder

    builder = ReportTemplateBuilder()
    nav_items = [
        {'id': 'page_one', 'label': 'Dataset A', 'icon': 'file'},
        {'id': 'page_two', 'label': 'Dataset B', 'icon': 'file'},
    ]
    html = builder.wrap_content(
        title='My Agent Report',
        report_title='My Agent',
        subtitle='Enterprise Data Governance Platform',
        right_content='Report ID: AGT-0001',
        agent_name='My Agent',
        nav_items=nav_items,
        main_content='<h2>Hello World</h2><p>Agent content goes here.</p>',
    )

Usage (granular):
    header  = builder.get_header_html('My Agent', 'Sub-title', 'Report ID: AGT-0001')
    sidebar = builder.get_sidebar_html(nav_items, home_label='Home', home_onclick='showHome()')
    footer  = builder.get_footer_html('My Agent', version='0.1')
    page    = builder.build_page(
        title='My Agent Report',
        header_html=header,
        sidebar_html=sidebar,
        main_content_html='<p>Content</p>',
        footer_html=footer,
    )
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# SVG icon library  (inline, no external deps)
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
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
    "dollar-sign": (
        '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">'
        '<line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/>'
        "</svg>"
    ),
    "activity": (
        '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">'
        '<polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>'
        "</svg>"
    ),
    "shield": (
        '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">'
        '<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>'
        "</svg>"
    ),
    "check-circle": (
        '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">'
        '<path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/>'
        "</svg>"
    ),
    "alert-circle": (
        '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">'
        '<circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/>'
        "</svg>"
    ),
    "x-circle": (
        '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">'
        '<circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/>'
        "</svg>"
    ),
}


def _icon(name: str) -> str:
    return _ICONS.get(name, "")


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# ReportTemplateBuilder
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

class ReportTemplateBuilder:
    """
    Builds standardised HTML report pages with a fixed header, collapsible
    sidebar, and branded footer â matching the Data Quality and Data Profiler
    report style.

    All methods return raw HTML strings so they can be composed freely or used
    through the high-level ``wrap_content`` / ``build_page`` helpers.

    Attributes:
        logo_url:        URL of the company logo shown in the header.
        primary_color:   CSS hex for the dark brand colour (header gradient start).
        sidebar_width:   Width of the open sidebar in pixels.
        header_height:   Height of the fixed header bar in pixels.
    """

    MASTECH_LOGO = "https://i.ibb.co/HDXXnh72/image.jpg"

    def __init__(
        self,
        logo_url: str = "",
        primary_color: str = "#153D64",
        sidebar_width: int = 320,
        header_height: int = 72,
    ) -> None:
        self.logo_url = logo_url or self.MASTECH_LOGO
        self.primary_color = primary_color
        self.sidebar_width = sidebar_width
        self.header_height = header_height

    # ââ public: granular builders ââââââââââââââââââââââââââââââââââââââââââââ

    def get_styles(self, extra_css: str = "") -> str:
        """Return the full shared <style> block (header + sidebar + footer + layout).

        Args:
            extra_css: Additional CSS rules to append inside the <style> tag.

        Returns:
            Complete ``<style>â¦</style>`` HTML string.
        """
        sw = self.sidebar_width
        hh = self.header_height
        pc = self.primary_color

        return f"""<style>
        :root {{
            --header-height: {hh}px;
            --sidebar-width: {sw}px;
            --primary: {pc};
            --primary-light: #217abc;
            --primary-dark: #0d2b4a;
        }}

        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        html, body {{
            height: 100%;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto',
                         'Helvetica Neue', Arial, sans-serif;
            background: #ffffff;
            color: #2c3e50;
            line-height: 1.6;
            overflow-x: hidden;
            display: flex;
            flex-direction: column;
            zoom: 0.90;
        }}

        /* ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
           FIXED TOP HEADER
        ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ */
        .top-header {{
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            height: var(--header-height);
            background: linear-gradient(135deg, var(--primary) 0%, var(--primary-dark) 100%);
            color: #ffffff;
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0 32px;
            z-index: 1100;
            box-shadow: 0 2px 12px rgba(0,0,0,0.15);
        }}

        .top-header-left {{
            display: flex;
            align-items: center;
            gap: 16px;
        }}

        .top-header-left img {{
            height: 36px;
            width: auto;
        }}

        .top-header-divider {{
            width: 1px;
            height: 36px;
            background: rgba(255,255,255,0.25);
        }}

        .top-header-title {{
            display: flex;
            flex-direction: column;
            gap: 2px;
        }}

        .top-header-title .title-main {{
            font-size: 1.1rem;
            font-weight: 800;
            letter-spacing: 0.8px;
            line-height: 1.2;
            text-transform: uppercase;
        }}

        .top-header-title .title-sub {{
            font-size: 0.75rem;
            font-weight: 400;
            opacity: 0.8;
            letter-spacing: 0.2px;
        }}

        .top-header-right {{
            display: flex;
            align-items: center;
            gap: 16px;
            font-size: 0.85rem;
            opacity: 0.9;
        }}

        /* ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
           LAYOUT CONTAINER
        ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ */
        .layout-container {{
            display: flex;
            flex: 1;
            padding-top: var(--header-height);
            background: #ffffff;
        }}

        /* ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
           SIDEBAR
        ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ */
        .sidebar {{
            width: var(--sidebar-width);
            background: #ffffff;
            border-right: 1px solid #e1e8ed;
            position: fixed;
            left: 0;
            top: var(--header-height);
            bottom: 0;
            overflow-y: auto;
            box-shadow: 2px 0 10px rgba(0,0,0,0.05);
            transition: transform 0.3s cubic-bezier(0.4,0,0.2,1), opacity 0.3s ease;
            z-index: 1000;
        }}

        .sidebar.collapsed {{
            transform: translateX(-100%);
            opacity: 0;
        }}

        /* Home row âââââââââââââââââââââââââââââââââââââââ */
        .sidebar-home-row {{
            display: flex;
            align-items: center;
            border-bottom: 1px solid #e1e8ed;
        }}

        .sidebar-home-link {{
            flex: 1;
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 18px 20px;
            font-size: 15px;
            font-weight: 500;
            color: #1a1a1a;
            cursor: pointer;
            transition: background 0.2s ease;
        }}

        .sidebar-home-link:hover {{
            background: #f8f9fa;
        }}

        .sidebar-home-link .nav-icon {{
            display: flex;
            align-items: center;
            color: #6c757d;
        }}

        /* Collapse button inside sidebar ââââââââââââââââ */
        .sidebar-collapse-btn {{
            background: none;
            border: 1px solid #d1d5db;
            border-radius: 6px;
            cursor: pointer;
            padding: 5px 7px;
            margin-right: 12px;
            display: flex;
            align-items: center;
            color: #6c757d;
            transition: all 0.2s ease;
        }}

        .sidebar-collapse-btn:hover {{
            background: #f0f4f8;
        }}

        /* Navigation section âââââââââââââââââââââââââââââââ */
        .sidebar-navigation {{
            padding: 0;
        }}

        .nav-section-title {{
            padding: 16px 20px 8px 20px;
            font-size: 11px;
            font-weight: 700;
            color: #6c757d;
            text-transform: uppercase;
            letter-spacing: 1px;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: space-between;
            transition: color 0.2s ease;
        }}

        .nav-section-title:hover {{
            color: #495057;
        }}

        .nav-section-title svg {{
            transition: transform 0.3s ease;
        }}

        .nav-section-title.collapsed svg {{
            transform: rotate(-90deg);
        }}

        .file-list {{
            max-height: 2000px;
            overflow: hidden;
            transition: max-height 0.3s ease;
        }}

        .file-list.collapsed {{
            max-height: 0;
        }}

        /* Nav items ââââââââââââââââââââââââââââââââââââââââ */
        .nav-item {{
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 12px 20px 12px 28px;
            cursor: pointer;
            transition: all 0.2s ease;
            border-left: 3px solid transparent;
            font-size: 14px;
            color: #333;
        }}

        .nav-item:hover {{
            background: #f8f9fa;
            border-left-color: #0066cc;
        }}

        .nav-item.active {{
            background: #e3f2fd;
            border-left-color: #0066cc;
        }}

        .nav-item .nav-icon {{
            flex-shrink: 0;
            display: flex;
            align-items: center;
            color: #6c757d;
        }}

        .nav-item .nav-label {{
            font-weight: 400;
            color: #333;
        }}

        /* Sidebar reopen button (shown when collapsed) ââââ */
        #rt-sidebar-reopen-btn {{
            display: none;
            position: fixed;
            left: 12px;
            top: calc(var(--header-height) + 12px);
            z-index: 1100;
            background: #0066cc;
            color: white;
            border: none;
            width: 36px;
            height: 36px;
            border-radius: 8px;
            cursor: pointer;
            align-items: center;
            justify-content: center;
            box-shadow: 0 2px 8px rgba(0,102,204,0.35);
            transition: background 0.2s;
        }}

        #rt-sidebar-reopen-btn:hover {{
            background: #0052a3;
        }}

        /* ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
           MAIN CONTENT
        ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ */
        .main-content {{
            flex: 1;
            margin-left: var(--sidebar-width);
            padding: 0;
            background: #ffffff;
            min-height: calc(100vh - var(--header-height));
            display: flex;
            flex-direction: column;
            overflow-x: hidden;
            transition: margin-left 0.3s cubic-bezier(0.4,0,0.2,1);
        }}

        .main-content.expanded {{
            margin-left: 0;
        }}

        /* Page wrapper âââââââââââââââââââââââââââââââââââââ */
        .page-wrapper {{
            background: white;
            padding: 32px 40px;
            flex: 1;
            display: flex;
            flex-direction: column;
        }}

        .page-wrapper:not(.active) {{
            display: none;
        }}

        .page-wrapper.active {{
            display: flex;
        }}

        /* ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
           PREFLIGHT VALIDATION SECTION
        ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ */
        .preflight-validation {{
            background: #f8fafb;
            border: 1px solid #e1e8ed;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 24px;
            transition: all 0.3s ease;
        }}

        .preflight-validation.with-warnings {{
            background: #fffbf0;
            border-color: #ffd966;
        }}

        .preflight-validation.with-errors {{
            background: #fff5f5;
            border-color: #f5a623;
        }}

        .preflight-validation.complete {{
            background: #f0fdf4;
            border-color: #27ae60;
        }}

        .preflight-header {{
            display: flex;
            align-items: center;
            gap: 12px;
            margin-bottom: 16px;
        }}

        .preflight-title {{
            font-size: 16px;
            font-weight: 600;
            color: #1a1a1a;
            flex: 1;
        }}

        .preflight-status {{
            display: flex;
            align-items: center;
            gap: 6px;
            padding: 4px 12px;
            border-radius: 6px;
            font-size: 13px;
            font-weight: 600;
            white-space: nowrap;
        }}

        .preflight-status.pending {{
            background: #e3f2fd;
            color: #1976d2;
        }}

        .preflight-status.passed {{
            background: #e8f5e9;
            color: #27ae60;
        }}

        .preflight-status.warning {{
            background: #fff3cd;
            color: #f39c12;
        }}

        .preflight-status.failed {{
            background: #ffebee;
            color: #e74c3c;
        }}

        .preflight-checks {{
            display: flex;
            flex-direction: column;
            gap: 10px;
        }}

        .preflight-check {{
            display: flex;
            align-items: flex-start;
            gap: 12px;
            padding: 10px 12px;
            background: white;
            border-radius: 6px;
            border-left: 3px solid #e1e8ed;
            transition: all 0.2s ease;
        }}

        .preflight-check.pending {{
            border-left-color: #1976d2;
            background: #f5f7fa;
        }}

        .preflight-check.passed {{
            border-left-color: #27ae60;
            background: #f6fbf8;
        }}

        .preflight-check.warning {{
            border-left-color: #f39c12;
            background: #fffaf2;
        }}

        .preflight-check.failed {{
            border-left-color: #e74c3c;
            background: #fff8f8;
        }}

        .preflight-icon {{
            flex-shrink: 0;
            display: flex;
            align-items: center;
            width: 20px;
            height: 20px;
            color: #6c757d;
        }}

        .preflight-check.passed .preflight-icon {{
            color: #27ae60;
        }}

        .preflight-check.warning .preflight-icon {{
            color: #f39c12;
        }}

        .preflight-check.failed .preflight-icon {{
            color: #e74c3c;
        }}

        .preflight-check.pending .preflight-icon {{
            color: #1976d2;
        }}

        .preflight-content {{
            flex: 1;
            display: flex;
            flex-direction: column;
            gap: 4px;
        }}

        .preflight-label {{
            font-size: 14px;
            font-weight: 500;
            color: #1a1a1a;
        }}

        .preflight-message {{
            font-size: 13px;
            color: #6c757d;
            line-height: 1.4;
        }}

        .preflight-message.warning {{
            color: #f39c12;
        }}

        .preflight-message.error {{
            color: #e74c3c;
        }}

        .preflight-message.success {{
            color: #27ae60;
        }}

        .preflight-progress {{
            margin-top: 12px;
            background: #e1e8ed;
            height: 6px;
            border-radius: 3px;
            overflow: hidden;
        }}

        .preflight-progress-bar {{
            height: 100%;
            background: #27ae60;
            width: 0%;
            transition: width 0.4s ease;
            border-radius: 3px;
        }}

        .preflight-summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
            gap: 12px;
            margin-top: 16px;
            padding-top: 12px;
            border-top: 1px solid #e1e8ed;
        }}

        .summary-stat {{
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 4px;
        }}

        .summary-stat-value {{
            font-size: 18px;
            font-weight: 700;
            color: #1a1a1a;
        }}

        .summary-stat-label {{
            font-size: 11px;
            color: #6c757d;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        /* ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
           FOOTER
        ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ */
        .page-footer {{
            padding: 16px 0;
            border-top: 2px solid #0066cc;
            text-align: center;
            font-size: 13px;
            color: #374151;
            background: #f9fafb;
            margin-top: auto;
        }}

        .page-footer .footer-line strong {{
            font-weight: 700;
            color: #1a1a1a;
        }}

        .page-footer .footer-sep {{
            color: #9ca3af;
            padding: 0 6px;
        }}

        .page-footer .footer-link {{
            color: #0066cc;
            text-decoration: none;
        }}

        /* ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
           UTILITY
        ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ */
        .report-header {{
            border-bottom: 2px solid #e1e8ed;
            padding-bottom: 16px;
            margin-bottom: 20px;
        }}

        .report-header h1 {{
            font-size: 26px;
            font-weight: 600;
            color: #1a1a1a;
            margin-bottom: 4px;
        }}

        .report-meta {{
            font-size: 13px;
            color: #6c757d;
        }}

        /* summary cards */
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }}

        .summary-card {{
            background: linear-gradient(135deg, #f8fafb 0%, #ffffff 100%);
            border: 1px solid #e1e8ed;
            border-radius: 12px;
            padding: 16px;
            text-align: center;
            transition: all 0.3s ease;
            cursor: default;
        }}

        .summary-card:hover {{
            transform: translateY(-4px) scale(1.02);
            box-shadow: 0 8px 24px rgba(0,102,204,0.12);
            border-color: #0066cc;
        }}

        .summary-card h3 {{
            font-size: 12px;
            color: #6c757d;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 10px;
        }}

        .summary-card .value {{
            font-size: 32px;
            font-weight: 700;
            color: #0066cc;
        }}

        .summary-card.success .value  {{ color: #27AE60; }}
        .summary-card.warning .value  {{ color: #f39c12; }}
        .summary-card.danger  .value  {{ color: #e74c3c; }}

        /* section title */
        .section-title {{
            font-size: 18px;
            font-weight: 600;
            color: #1a1a1a;
            margin: 24px 0 12px;
            padding-bottom: 10px;
            border-bottom: 1px solid #e1e8ed;
            display: flex;
            align-items: center;
            gap: 8px;
        }}

        /* ââ Responsive ââ */
        @media (max-width: 768px) {{
            :root {{
                --sidebar-width: 0px;
            }}
            .sidebar {{
                width: 280px;
                transform: translateX(-100%);
                opacity: 0;
            }}
            .sidebar.open {{
                transform: translateX(0);
                opacity: 1;
            }}
            .main-content {{
                margin-left: 0;
            }}
        }}

        {extra_css}
        </style>"""

    def get_scripts(self, extra_js: str = "") -> str:
        """Return the shared sidebar ``<script>`` block.

        Includes ``toggleSidebar()``, ``Ctrl+B`` keyboard shortcut, and an
        optional file-list collapse helper.

        Args:
            extra_js: Additional JavaScript to append inside the ``<script>`` tag.

        Returns:
            Complete ``<script>â¦</script>`` HTML string.
        """
        return f"""<script>
        // ââ Sidebar toggle ââââââââââââââââââââââââââââââââââââââââââââââââââ
        var _rtSidebarCollapsed = false;

        function toggleSidebar() {{
            var sidebar    = document.querySelector('.sidebar');
            var mainContent = document.querySelector('.main-content');
            var collapseIcon = document.getElementById('sidebar-collapse-icon');
            var reopenBtn  = document.getElementById('rt-sidebar-reopen-btn');

            _rtSidebarCollapsed = !_rtSidebarCollapsed;

            if (_rtSidebarCollapsed) {{
                sidebar.classList.add('collapsed');
                mainContent.classList.add('expanded');
                if (collapseIcon) collapseIcon.style.transform = 'rotate(180deg)';
                if (reopenBtn)   reopenBtn.style.display = 'flex';
            }} else {{
                sidebar.classList.remove('collapsed');
                mainContent.classList.remove('expanded');
                if (collapseIcon) collapseIcon.style.transform = 'rotate(0deg)';
                if (reopenBtn)   reopenBtn.style.display = 'none';
            }}
        }}

        // Ctrl+B keyboard shortcut
        document.addEventListener('keydown', function(e) {{
            if (e.ctrlKey && e.key === 'b') {{
                e.preventDefault();
                toggleSidebar();
            }}
        }});

        // ââ File-list section collapse helper ââââââââââââââââââââââââââââââ
        function toggleNavSection(sectionId) {{
            var title = document.querySelector('[data-section="' + sectionId + '"]');
            var list  = document.getElementById(sectionId);
            if (title && list) {{
                title.classList.toggle('collapsed');
                list.classList.toggle('collapsed');
            }}
        }}

        // ââ Active nav item ââââââââââââââââââââââââââââââââââââââââââââââââ
        function setActiveNavItem(itemId) {{
            document.querySelectorAll('.nav-item').forEach(function(el) {{
                el.classList.remove('active');
            }});
            var el = document.querySelector('.nav-item[data-item-id="' + itemId + '"]');
            if (el) el.classList.add('active');
        }}

        // ââ Page switching helpers âââââââââââââââââââââââââââââââââââââââââ
        /**
         * showPage(pageId)
         *   Hides all .page-wrapper elements and shows the one with id=pageId.
         *   Call setActiveNavItem(pageId) yourself if you need the sidebar highlight.
         */
        function showPage(pageId) {{
            document.querySelectorAll('.page-wrapper').forEach(function(el) {{
                el.classList.remove('active');
            }});
            var target = document.getElementById(pageId);
            if (target) {{
                target.classList.add('active');
                requestAnimationFrame(function() {{
                    window.scrollTo({{ top: 0, left: 0, behavior: 'instant' }});
                }});
            }}
            setActiveNavItem(pageId);
        }}

        // ââ Preflight validation helpers ââââââââââââââââââââââââââââââââââ
        /**
         * updatePreflightCheck(checkId, status, message)
         *   Updates a single preflight check item.
         *   status: 'pending' | 'passed' | 'warning' | 'failed'
         */
        function updatePreflightCheck(checkId, status, message) {{
            var checkEl = document.getElementById(checkId);
            if (checkEl) {{
                checkEl.className = 'preflight-check ' + status;
                var msgEl = checkEl.querySelector('.preflight-message');
                if (msgEl) msgEl.textContent = message;
            }}
        }}

        /**
         * updatePreflightProgress(percent)
         *   Updates the progress bar width (0-100).
         */
        function updatePreflightProgress(percent) {{
            var progressBar = document.querySelector('.preflight-progress-bar');
            if (progressBar) {{
                progressBar.style.width = percent + '%';
            }}
        }}

        /**
         * updatePreflightStatus(overallStatus, counts)
         *   Updates the overall validation status.
         *   overallStatus: 'pending' | 'passed' | 'warning' | 'failed'
         *   counts: object with passed, warning, failed, total properties
         */
        function updatePreflightStatus(overallStatus, counts) {{
            var statusEl = document.querySelector('.preflight-status');
            if (statusEl) {{
                statusEl.className = 'preflight-status ' + overallStatus;
                switch(overallStatus) {{
                    case 'passed':
                        statusEl.innerHTML = '{_icon("check-circle")} All Checks Passed';
                        break;
                    case 'warning':
                        statusEl.innerHTML = '{_icon("alert-circle")} Warnings Found';
                        break;
                    case 'failed':
                        statusEl.innerHTML = '{_icon("x-circle")} Validation Failed';
                        break;
                    case 'pending':
                        statusEl.innerHTML = 'Validating...';
                        break;
                }}
            }}

            if (counts) {{
                document.querySelectorAll('.summary-stat-value').forEach(function(el, i) {{
                    if (i === 0 && counts.passed !== undefined) el.textContent = counts.passed;
                    else if (i === 1 && counts.warning !== undefined) el.textContent = counts.warning;
                    else if (i === 2 && counts.failed !== undefined) el.textContent = counts.failed;
                    else if (i === 3 && counts.total !== undefined) el.textContent = counts.total;
                }});
            }}
        }}

        {extra_js}
        </script>"""

    def get_header_html(
        self,
        report_title: str,
        subtitle: str = "Enterprise Data Governance Platform",
        right_content: Optional[str] = None,
    ) -> str:
        """Return the fixed ``<header class="top-header">`` block.

        Args:
            report_title:  Bold title shown next to the logo divider.
            subtitle:      Smaller text below the title (default: platform name).
            right_content: HTML placed on the right side of the header bar.
                           If None, defaults to "Generated on YYYY-MM-DD HH:MM:SS".

        Returns:
            HTML string for the header element.
        """
        if right_content is None:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            right_content = f"Generated on {ts}"

        right_html = (
            f'<span class="top-header-right">{right_content}</span>'
            if right_content
            else ""
        )
        return f"""<header class="top-header">
    <div class="top-header-left">
        <img src="{self.logo_url}" alt="Mastech Digital">
        <div class="top-header-divider"></div>
        <div class="top-header-title">
            <span class="title-main">{report_title}</span>
            <span class="title-sub">{subtitle}</span>
        </div>
    </div>
    {right_html}
</header>"""

    def get_sidebar_html(
        self,
        nav_items: List[Dict[str, str]],
        *,
        home_label: str = "Home",
        home_onclick: str = "showPage('homePage')",
        section_label: str = "FILES",
        section_id: str = "rt-file-list",
    ) -> str:
        """Return the collapsible ``<aside class="sidebar">`` block.

        Also emits the floating sidebar-reopen button that appears when the
        sidebar is collapsed.

        Args:
            nav_items:     List of dicts, each with keys:
                             - ``id``       (str) â used as ``data-item-id`` and passed to showPage()
                             - ``label``    (str) â display text
                             - ``icon``     (str, optional) â icon key from _ICONS (default: "file")
                             - ``onclick``  (str, optional) â JS call override

            home_label:    Label for the Home link at the top of the sidebar.
            home_onclick:  JS expression called when Home is clicked.
            section_label: ALL-CAPS label above the nav-item list.
            section_id:    HTML id of the collapsible file-list ``<div>``.

        Returns:
            HTML string for the reopen button + sidebar ``<aside>`` element.
        """
        items_html = ""
        for item in nav_items:
            item_id  = item.get("id", "")
            label    = item.get("label", item_id)
            icon_key = item.get("icon", "file")
            onclick  = item.get("onclick") or f"showPage('{item_id}')"
            icon_svg = _icon(icon_key)
            items_html += f"""
            <div class="nav-item" onclick="{onclick}" data-item-id="{item_id}">
                <span class="nav-icon">{icon_svg}</span>
                <span class="nav-label">{label}</span>
            </div>"""

        reopen_btn = f"""<!-- Sidebar reopen button (shown when sidebar collapsed) -->
<button id="rt-sidebar-reopen-btn"
        onclick="toggleSidebar()"
        title="Open Sidebar">
    {_icon("chevron_right")}
</button>"""

        return f"""{reopen_btn}
<aside class="sidebar" id="sidebar">
    <!-- Home row + collapse button -->
    <div class="sidebar-home-row">
        <div class="sidebar-home-link" onclick="{home_onclick}">
            <span class="nav-icon">{_icon("home")}</span>
            <span class="nav-label">{home_label}</span>
        </div>
        <button class="sidebar-collapse-btn"
                onclick="toggleSidebar()"
                title="Collapse sidebar">
            {_icon("double_chevron_left")}
        </button>
    </div>

    <!-- Navigation -->
    <div class="sidebar-navigation">
        <div class="nav-section-title"
             data-section="{section_id}"
             onclick="toggleNavSection('{section_id}')">
            <span>{section_label}</span>
            {_icon("chevron_down")}
        </div>
        <div class="file-list" id="{section_id}">
            {items_html}
        </div>
    </div>
</aside>"""

    def get_footer_html(
        self,
        agent_name: str = "Agent",
        version: str = "0.1",
        extra_parts: Optional[List[str]] = None,
    ) -> str:
        """Return the branded ``<footer class="page-footer">`` block.

        Args:
            agent_name:   Name of the agent that appears bold on the left.
            version:      Version string shown on the right.
            extra_parts:  Additional ``<span>`` strings to include between the
                          separators (e.g. ``['<a href="#">Docs</a>']``).

        Returns:
            HTML string for the footer element.
        """
        middle = '<span class="footer-sep">|</span><span>Enterprise Data Governance Platform</span>'
        if extra_parts:
            for part in extra_parts:
                middle += f'<span class="footer-sep">|</span><span>{part}</span>'

        return f"""<footer class="page-footer">
    <div class="footer-line">
        <strong>{agent_name}</strong>
        {middle}
        <span class="footer-sep">|</span>
        <span>Version {version}</span>
    </div>
</footer>"""

    def get_preflight_validation_html(
        self,
        validation_items: List[Dict[str, str]],
        *,
        title: str = "Preflight Validation",
        overall_status: str = "pending",
    ) -> str:
        """Return a preflight validation ``<section class="preflight-validation">`` block.

        Args:
            validation_items: List of dicts, each with keys:
                               - ``id``       (str) â HTML id for the check item
                               - ``label``    (str) â display name of the check
                               - ``status``   (str) â 'pending'|'passed'|'warning'|'failed'
                               - ``message``  (str) â status message/description
            title:           Title of the validation section.
            overall_status:  Overall validation state: 'pending'|'passed'|'warning'|'failed'

        Returns:
            HTML string for the preflight validation section.

        Example::

            builder = ReportTemplateBuilder()
            validation_html = builder.get_preflight_validation_html(
                validation_items=[
                    {
                        'id': 'check_db_connection',
                        'label': 'Database Connection',
                        'status': 'passed',
                        'message': 'Successfully connected to production database'
                    },
                    {
                        'id': 'check_schema_version',
                        'label': 'Schema Version',
                        'status': 'warning',
                        'message': 'Schema version mismatch: expected 5.2, found 5.1'
                    },
                ],
                overall_status='warning'
            )
        """
        checks_html = ""
        for item in validation_items:
            item_id = item.get("id", "")
            label = item.get("label", "")
            status = item.get("status", "pending")
            message = item.get("message", "")
            icon_key = {
                "passed": "check-circle",
                "warning": "alert-circle",
                "failed": "x-circle",
                "pending": "activity",
            }.get(status, "activity")

            checks_html += f"""
            <div class="preflight-check {status}" id="{item_id}">
                <span class="preflight-icon">{_icon(icon_key)}</span>
                <div class="preflight-content">
                    <div class="preflight-label">{label}</div>
                    <div class="preflight-message {status}">{message}</div>
                </div>
            </div>"""

        status_html = f'<span class="preflight-status {overall_status}">Validating...</span>'
        if overall_status == "passed":
            status_html = f'<span class="preflight-status {overall_status}">{_icon("check-circle")} All Checks Passed</span>'
        elif overall_status == "warning":
            status_html = f'<span class="preflight-status {overall_status}">{_icon("alert-circle")} Warnings Found</span>'
        elif overall_status == "failed":
            status_html = f'<span class="preflight-status {overall_status}">{_icon("x-circle")} Validation Failed</span>'

        return f"""<section class="preflight-validation {overall_status}">
    <div class="preflight-header">
        <span class="preflight-title">{title}</span>
        {status_html}
    </div>
    <div class="preflight-checks">
        {checks_html}
    </div>
    <div class="preflight-progress">
        <div class="preflight-progress-bar"></div>
    </div>
    <div class="preflight-summary">
        <div class="summary-stat">
            <div class="summary-stat-value">0</div>
            <div class="summary-stat-label">Passed</div>
        </div>
        <div class="summary-stat">
            <div class="summary-stat-value">0</div>
            <div class="summary-stat-label">Warnings</div>
        </div>
        <div class="summary-stat">
            <div class="summary-stat-value">0</div>
            <div class="summary-stat-label">Failed</div>
        </div>
        <div class="summary-stat">
            <div class="summary-stat-value">{len(validation_items)}</div>
            <div class="summary-stat-label">Total</div>
        </div>
    </div>
</section>"""

    # ââ public: assembly helpers âââââââââââââââââââââââââââââââââââââââââââââ

    def build_page(
        self,
        title: str,
        header_html: str,
        sidebar_html: str,
        main_content_html: str,
        footer_html: str = "",
        extra_styles: str = "",
        extra_scripts: str = "",
    ) -> str:
        """Assemble a complete ``<!DOCTYPE html>`` document.

        Args:
            title:             Browser tab title (``<title>``).
            header_html:       Full header HTML (from ``get_header_html``).
            sidebar_html:      Full sidebar HTML (from ``get_sidebar_html``).
            main_content_html: The body of the report (agent-specific HTML).
            footer_html:       Optional footer HTML (from ``get_footer_html``).
            extra_styles:      Additional CSS inserted into the shared ``<style>`` block.
            extra_scripts:     Additional JS inserted into the shared ``<script>`` block.

        Returns:
            Complete HTML string ready to be uploaded or written to a file.
        """
        styles  = self.get_styles(extra_css=extra_styles)
        scripts = self.get_scripts(extra_js=extra_scripts)

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    {styles}
</head>
<body>
    {header_html}

    <div class="layout-container">
        {sidebar_html}

        <main class="main-content">
            {main_content_html}
        </main>
    </div>

    {scripts}
</body>
</html>"""

    def wrap_content(
        self,
        title: str,
        report_title: str,
        main_content: str,
        *,
        subtitle: str = "Enterprise Data Governance Platform",
        right_content: str = "",
        agent_name: str = "Agent",
        version: str = "0.1",
        nav_items: Optional[List[Dict[str, str]]] = None,
        home_label: str = "Home",
        home_onclick: str = "showPage('homePage')",
        section_label: str = "FILES",
        extra_styles: str = "",
        extra_scripts: str = "",
        include_footer_in_content: bool = True,
    ) -> str:
        """One-call convenience wrapper â most agents only need this method.

        Assembles a fully-templated page from minimal inputs.

        Args:
            title:          Browser tab title.
            report_title:   Bold heading shown in the header bar.
            main_content:   Agent-specific HTML for the ``<main>`` area.
                            Should contain one or more ``.page-wrapper`` divs.
                            If you pass a plain string it is wrapped automatically.
            subtitle:       Sub-text below the report title in the header.
            right_content:  HTML placed on the right of the header (e.g. Report ID).
            agent_name:     Agent name shown in the footer.
            version:        Version shown in the footer.
            nav_items:      List of sidebar nav item dicts
                            (``{'id', 'label', 'icon'?, 'onclick'?}``).
            home_label:     Label for the sidebar Home link.
            home_onclick:   JS expression for the sidebar Home onclick.
            section_label:  Section heading above nav items in the sidebar.
            extra_styles:   Additional CSS.
            extra_scripts:  Additional JavaScript.
            include_footer_in_content:
                            If True (default), the footer is appended inside
                            ``main_content``.  Set to False if you have already
                            included a footer in ``main_content``.

        Returns:
            Complete HTML string.

        Example::

            builder = ReportTemplateBuilder()
            html = builder.wrap_content(
                title='My Report',
                report_title='My Agent',
                right_content='Report ID: AGT-001',
                agent_name='My Agent',
                nav_items=[
                    {'id': 'homePage', 'label': 'Overview', 'icon': 'home'},
                    {'id': 'page_dataset1', 'label': 'Dataset 1'},
                ],
                main_content='''
                    <div class="page-wrapper active" id="homePage">
                        <h2>Overview</h2>
                        <p>Your content here.</p>
                    </div>
                    <div class="page-wrapper" id="page_dataset1">
                        <h2>Dataset 1</h2>
                        <p>Dataset-specific content.</p>
                    </div>
                ''',
            )
        """
        header  = self.get_header_html(report_title, subtitle, right_content)
        sidebar = self.get_sidebar_html(
            nav_items or [],
            home_label=home_label,
            home_onclick=home_onclick,
            section_label=section_label,
        )
        footer = self.get_footer_html(agent_name=agent_name, version=version)

        body = main_content
        if include_footer_in_content:
            body = f"{main_content}\n{footer}"

        return self.build_page(
            title=title,
            header_html=header,
            sidebar_html=sidebar,
            main_content_html=body,
            footer_html="",
            extra_styles=extra_styles,
            extra_scripts=extra_scripts,
        )

    def build_multi_page_report(
        self,
        title: str,
        report_title: str,
        pages: List[Dict[str, Any]],
        *,
        subtitle: str = "Enterprise Data Governance Platform",
        right_content: Optional[str] = None,
        agent_name: str = "Agent",
        version: str = "0.1",
        home_label: str = "Home",
        section_label: str = "REPORTS",
        extra_styles: str = "",
        extra_scripts: str = "",
    ) -> str:
        """Helper to build a complete multi-page report with standard sidebar-to-content mapping.

        Args:
            title:         Browser tab title.
            report_title:  Bold heading in header.
            pages:         List of dicts: ``{'id': str, 'label': str, 'content': str, 'icon'?: str}``.
                           The 'content' should be the inner HTML for that page.
                           The first page in the list is marked 'active' by default.
            subtitle:      Sub-text in header.
            right_content: Header metadata (defaults to timestamp).
            agent_name:    Footer agent name.
            version:       Footer version.
            home_label:    Sidebar home link label.
            section_label: Sidebar section title.

        Returns:
            Complete HTML string.
        """
        nav_items = []
        main_html_parts = []

        for i, p in enumerate(pages):
            pid = p["id"]
            label = p["label"]
            content = p["content"]
            icon = p.get("icon", "file")

            # Nav Item
            nav_items.append({"id": pid, "label": label, "icon": icon})

            # Content Wrapper
            active_class = " active" if i == 0 else ""
            main_html_parts.append(
                f'<div class="page-wrapper{active_class}" id="{pid}">{content}</div>'
            )

        # Ensure home_onclick shows the first page if it exists
        home_onclick = f"showPage('{pages[0]['id']}')" if pages else ""

        return self.wrap_content(
            title=title,
            report_title=report_title,
            subtitle=subtitle,
            right_content=right_content,
            agent_name=agent_name,
            version=version,
            nav_items=nav_items,
            home_label=home_label,
            home_onclick=home_onclick,
            section_label=section_label,
            main_content="\n".join(main_html_parts),
            extra_styles=extra_styles,
            extra_scripts=extra_scripts,
        )
