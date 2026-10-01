"""
fabric_migration_validator/trackers/wave_tracker.py
────────────────────────────────────────────────────
Wave Migration Tracker — strategic observability layer.

Tracks migration progress across waves and over time:
  - % tables validated per wave
  - Pass rate trending per wave
  - Go-live readiness score per wave
  - Persistent run history (JSON file or Fabric Delta table)
  - HTML wave dashboard for leadership

Usage:
    from trackers.wave_tracker import WaveTracker
    tracker = WaveTracker(config, history_path="./reports/wave_history.json")
    tracker.record_run(summary)
    tracker.generate_wave_dashboard("./reports/wave_dashboard.html")

In Fabric Notebook — write history to Delta:
    tracker = WaveTracker(config, spark=spark,
                          delta_table="validation.run_history")
    tracker.record_run(summary)
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from core.models import RunSummary, ValidationStatus


# ── Wave config model ─────────────────────────────────────────

@dataclass
class WaveConfig:
    """
    Defines a migration wave. Add to execution.yml under waves:.

    waves:
      - id: wave_1
        name: "CRM & Customer Data"
        target_go_live: "2025-06-01"
        table_ids: [CUST_001, CUST_002, CUST_003]
        readiness_threshold_pct: 95.0

      - id: wave_2
        name: "Finance Migration"
        target_go_live: "2025-09-01"
        table_ids: [SALES_001, SALES_002]
        readiness_threshold_pct: 100.0
    """
    id: str
    name: str
    target_go_live: Optional[str] = None
    table_ids: List[str] = field(default_factory=list)
    readiness_threshold_pct: float = 95.0


@dataclass
class WaveRunRecord:
    """Snapshot of a single validation run for a wave."""
    run_id: str
    wave_id: str
    timestamp: str
    total_tables: int
    passed_tables: int
    failed_tables: int
    error_tables: int
    warn_tables: int
    pass_rate_pct: float
    readiness_threshold_pct: float
    is_go_live_ready: bool
    duration_seconds: float
    check_breakdown: Dict[str, int] = field(default_factory=dict)


# ── Wave Tracker ──────────────────────────────────────────────

class WaveTracker:
    """
    Records run history and generates wave-level progress reports.

    Persistence options:
      A. JSON file (default) — local/CI usage
      B. Fabric Delta table  — production, enables Power BI on top

    History format:
      {
        "runs": [WaveRunRecord, ...],
        "waves": [WaveConfig, ...]
      }
    """

    def __init__(
        self,
        waves: List[WaveConfig],
        history_path: str = "./reports/wave_history.json",
        spark=None,
        delta_table: Optional[str] = None,
    ):
        self.waves = {w.id: w for w in waves}
        self.history_path = Path(history_path)
        self.spark = spark
        self.delta_table = delta_table
        self._history: List[WaveRunRecord] = self._load_history()

    # ── Record a run ──────────────────────────────────────────

    def record_run(self, summary: RunSummary) -> List[WaveRunRecord]:
        """
        Extracts per-wave statistics from a RunSummary and persists them.
        Returns the list of WaveRunRecords created for this run.
        """
        records = []
        table_result_map = {r.table_id: r for r in summary.table_results}

        for wave in self.waves.values():
            wave_results = [
                table_result_map[tid]
                for tid in wave.table_ids
                if tid in table_result_map
            ]
            if not wave_results:
                continue

            total      = len(wave_results)
            passed     = sum(1 for r in wave_results if r.overall_status == ValidationStatus.PASS)
            failed     = sum(1 for r in wave_results if r.overall_status == ValidationStatus.FAIL)
            errors     = sum(1 for r in wave_results if r.overall_status == ValidationStatus.ERROR)
            warns      = sum(1 for r in wave_results if r.overall_status == ValidationStatus.WARN)
            pass_rate  = round(passed / total * 100, 2) if total > 0 else 0.0

            # Check breakdown: how many times each check type passed/failed across this wave
            check_breakdown: Dict[str, int] = {}
            for r in wave_results:
                for chk in r.checks:
                    if chk.status == ValidationStatus.FAIL:
                        check_breakdown[chk.check_name] = check_breakdown.get(chk.check_name, 0) + 1

            record = WaveRunRecord(
                run_id=summary.run_id,
                wave_id=wave.id,
                timestamp=summary.started_at.isoformat(),
                total_tables=total,
                passed_tables=passed,
                failed_tables=failed,
                error_tables=errors,
                warn_tables=warns,
                pass_rate_pct=pass_rate,
                readiness_threshold_pct=wave.readiness_threshold_pct,
                is_go_live_ready=(pass_rate >= wave.readiness_threshold_pct and errors == 0),
                duration_seconds=summary.duration_seconds,
                check_breakdown=check_breakdown,
            )
            records.append(record)
            self._history.append(record)

        self._persist_history()
        if self.delta_table and self.spark:
            self._write_to_delta(records)

        return records

    # ── Readiness report ──────────────────────────────────────

    def readiness_report(self) -> Dict:
        """Returns current readiness status per wave based on last run."""
        latest_by_wave: Dict[str, WaveRunRecord] = {}
        for rec in self._history:
            if rec.wave_id not in latest_by_wave or rec.timestamp > latest_by_wave[rec.wave_id].timestamp:
                latest_by_wave[rec.wave_id] = rec

        report = {}
        for wave_id, wave in self.waves.items():
            latest = latest_by_wave.get(wave_id)
            report[wave_id] = {
                "wave_name": wave.name,
                "target_go_live": wave.target_go_live,
                "total_tables": len(wave.table_ids),
                "validated_tables": latest.total_tables if latest else 0,
                "passed_tables": latest.passed_tables if latest else 0,
                "pass_rate_pct": latest.pass_rate_pct if latest else 0.0,
                "threshold_pct": wave.readiness_threshold_pct,
                "is_go_live_ready": latest.is_go_live_ready if latest else False,
                "last_run_id": latest.run_id if latest else None,
                "last_run_timestamp": latest.timestamp if latest else None,
                "top_failures": sorted(
                    (latest.check_breakdown or {}).items(),
                    key=lambda x: x[1], reverse=True
                )[:5] if latest else [],
            }
        return report

    # ── HTML Wave Dashboard ───────────────────────────────────

    def generate_wave_dashboard(self, output_path: str) -> Path:
        """Generates an HTML leadership dashboard showing wave progress."""
        report = self.readiness_report()
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        waves_html = ""
        for wave_id, data in report.items():
            pct = data["pass_rate_pct"]
            ready = data["is_go_live_ready"]
            threshold = data["threshold_pct"]
            bar_color = "#22c55e" if ready else "#f59e0b" if pct >= 70 else "#ef4444"
            status_badge = (
                '<span style="background:#dcfce7;color:#166534;padding:3px 12px;border-radius:99px;font-size:12px;font-weight:600">GO-LIVE READY</span>'
                if ready else
                '<span style="background:#fef9c3;color:#854d0e;padding:3px 12px;border-radius:99px;font-size:12px;font-weight:600">IN PROGRESS</span>'
            )
            failures_html = ""
            for check_name, count in data["top_failures"]:
                failures_html += f'<div style="font-size:12px;color:#ef4444;padding:2px 0">{check_name}: {count} table(s)</div>'

            # Build trend sparkline from history
            wave_history = [r for r in self._history if r.wave_id == wave_id][-10:]
            sparkline = ""
            if len(wave_history) > 1:
                pts = [r.pass_rate_pct for r in wave_history]
                max_p, min_p = max(pts), min(pts)
                rng = max_p - min_p if max_p != min_p else 1
                coords = " ".join(
                    f"{int(i * 14)},{int(40 - (p - min_p) / rng * 35)}"
                    for i, p in enumerate(pts)
                )
                sparkline = f'''
                <svg width="{len(pts) * 14}" height="44" style="overflow:visible">
                  <polyline fill="none" stroke="{bar_color}" stroke-width="2"
                    points="{coords}"/>
                </svg>'''

            waves_html += f"""
            <div style="background:#1e293b;border:1px solid #334155;border-radius:12px;padding:20px;margin-bottom:16px">
              <div style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:12px">
                <div>
                  <div style="font-size:16px;font-weight:600;color:#e2e8f0">{data['wave_name']}</div>
                  <div style="font-size:12px;color:#94a3b8;margin-top:2px">
                    Target go-live: {data['target_go_live'] or 'TBD'}
                    &nbsp;|&nbsp; Last run: {(data['last_run_timestamp'] or 'Never')[:16]}
                  </div>
                </div>
                {status_badge}
              </div>
              <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:14px">
                <div style="background:#0f172a;border-radius:8px;padding:12px;text-align:center">
                  <div style="font-size:22px;font-weight:700;color:#e2e8f0">{data['validated_tables']}/{data['total_tables']}</div>
                  <div style="font-size:11px;color:#94a3b8;margin-top:2px">Tables validated</div>
                </div>
                <div style="background:#0f172a;border-radius:8px;padding:12px;text-align:center">
                  <div style="font-size:22px;font-weight:700;color:{bar_color}">{pct}%</div>
                  <div style="font-size:11px;color:#94a3b8;margin-top:2px">Pass rate (need {threshold}%)</div>
                </div>
                <div style="background:#0f172a;border-radius:8px;padding:12px;text-align:center">
                  <div style="font-size:22px;font-weight:700;color:#22c55e">{data['passed_tables']}</div>
                  <div style="font-size:11px;color:#94a3b8;margin-top:2px">Tables passing</div>
                </div>
                <div style="background:#0f172a;border-radius:8px;padding:12px;text-align:center">
                  <div style="font-size:22px;font-weight:700;color:#ef4444">{data['passed_tables'] and data['total_tables'] - data['passed_tables']}</div>
                  <div style="font-size:11px;color:#94a3b8;margin-top:2px">Tables failing</div>
                </div>
              </div>
              <div style="background:#0f172a;border-radius:6px;height:8px;margin-bottom:10px">
                <div style="background:{bar_color};height:8px;border-radius:6px;width:{min(pct,100):.1f}%;transition:width 0.5s"></div>
              </div>
              <div style="display:flex;justify-content:space-between;align-items:flex-end">
                <div>
                  {'<div style="font-size:12px;font-weight:500;color:#94a3b8;margin-bottom:4px">Top failing checks:</div>' + failures_html if data['top_failures'] else '<div style="font-size:12px;color:#22c55e">No failures in last run</div>'}
                </div>
                <div style="text-align:right">
                  <div style="font-size:11px;color:#64748b;margin-bottom:2px">Pass rate trend</div>
                  {sparkline or '<div style="font-size:11px;color:#64748b">No history yet</div>'}
                </div>
              </div>
            </div>"""

        # Overall stats
        total_tables_all = sum(d["total_tables"] for d in report.values())
        validated_all    = sum(d["validated_tables"] for d in report.values())
        ready_count      = sum(1 for d in report.values() if d["is_go_live_ready"])
        overall_pass     = round(
            sum(d["pass_rate_pct"] for d in report.values()) / len(report), 1
        ) if report else 0

        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Wave Migration Tracker</title>
<style>
  * {{ box-sizing:border-box; margin:0; padding:0; }}
  body {{ font-family:'Segoe UI',system-ui,sans-serif; background:#0f172a; color:#e2e8f0; padding:24px; }}
  h1 {{ font-size:1.5rem; font-weight:700; margin-bottom:4px; }}
  .meta {{ color:#94a3b8; font-size:13px; margin-bottom:24px; }}
  .kpi-grid {{ display:grid; grid-template-columns:repeat(4,1fr); gap:16px; margin-bottom:28px; }}
  .kpi {{ background:#1e293b; border:1px solid #334155; border-radius:10px; padding:16px; text-align:center; }}
  .kpi-value {{ font-size:2rem; font-weight:700; }}
  .kpi-label {{ font-size:11px; color:#94a3b8; margin-top:4px; text-transform:uppercase; }}
</style>
</head>
<body>
<h1>Wave Migration Tracker</h1>
<div class="meta">Generated: {now}</div>
<div class="kpi-grid">
  <div class="kpi"><div class="kpi-value">{len(self.waves)}</div><div class="kpi-label">Total waves</div></div>
  <div class="kpi"><div class="kpi-value" style="color:#22c55e">{ready_count}</div><div class="kpi-label">Go-live ready</div></div>
  <div class="kpi"><div class="kpi-value">{validated_all}/{total_tables_all}</div><div class="kpi-label">Tables validated</div></div>
  <div class="kpi"><div class="kpi-value" style="color:{'#22c55e' if overall_pass >= 95 else '#f59e0b' if overall_pass >= 80 else '#ef4444'}">{overall_pass}%</div><div class="kpi-label">Overall pass rate</div></div>
</div>
{waves_html}
<div style="color:#475569;font-size:12px;margin-top:16px;text-align:center">
  Fabric Migration Validator — Wave Tracker &nbsp;|&nbsp; {now}
</div>
</body></html>"""

        path.write_text(html, encoding="utf-8")
        print(f"  🌊 Wave dashboard: {path}")
        return path

    # ── Persistence ───────────────────────────────────────────

    def _load_history(self) -> List[WaveRunRecord]:
        if not self.history_path.exists():
            return []
        try:
            with open(self.history_path) as f:
                data = json.load(f)
            return [WaveRunRecord(**r) for r in data.get("runs", [])]
        except Exception:
            return []

    def _persist_history(self):
        self.history_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "waves": [{"id": w.id, "name": w.name, "target_go_live": w.target_go_live,
                       "table_ids": w.table_ids, "readiness_threshold_pct": w.readiness_threshold_pct}
                      for w in self.waves.values()],
            "runs": [
                {k: v for k, v in vars(r).items()} for r in self._history
            ]
        }
        with open(self.history_path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    def _write_to_delta(self, records: List[WaveRunRecord]):
        """Write run records to a Fabric Delta table for Power BI consumption."""
        import pandas as pd
        rows = [{
            "run_id": r.run_id,
            "wave_id": r.wave_id,
            "wave_name": self.waves[r.wave_id].name if r.wave_id in self.waves else r.wave_id,
            "timestamp": r.timestamp,
            "total_tables": r.total_tables,
            "passed_tables": r.passed_tables,
            "failed_tables": r.failed_tables,
            "error_tables": r.error_tables,
            "pass_rate_pct": r.pass_rate_pct,
            "is_go_live_ready": r.is_go_live_ready,
            "duration_seconds": r.duration_seconds,
        } for r in records]
        if not rows:
            return
        df = self.spark.createDataFrame(pd.DataFrame(rows))
        df.write.format("delta").mode("append").saveAsTable(self.delta_table)
        print(f"  📊 Wave run records written to Delta: {self.delta_table}")

