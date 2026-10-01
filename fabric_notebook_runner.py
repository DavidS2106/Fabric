# ============================================================
# fabric_migration_validator — Fabric Notebook Entry Point
# ============================================================
# Copy this cell block into a Microsoft Fabric Notebook.
# Run cell-by-cell or run all.
# ============================================================

# ── Cell 1: Install dependencies ────────────────────────────
# %pip install pyyaml pandas openpyxl pyodbc SQLAlchemy

# ── Cell 2: Set config path & parameters ────────────────────
CONFIG_PATH = "/lakehouse/default/Files/migration_validator/config/execution.yml"

# Optional filters — set to None to run everything
FILTER_TAGS    = None        # e.g. ["critical", "finance"]
FILTER_LAYERS  = None        # e.g. ["gold"]
FILTER_PHASES  = None        # e.g. ["full_load"]
FILTER_IDS     = None        # e.g. ["CUST_001", "SALES_001"]

OUTPUT_DIR     = "/lakehouse/default/Files/migration_validator/reports"

# ── Cell 3: Bootstrap sys.path ───────────────────────────────
import sys
UTIL_ROOT = "/lakehouse/default/Files/migration_validator"
if UTIL_ROOT not in sys.path:
    sys.path.insert(0, UTIL_ROOT)

# ── Cell 4: Load config ──────────────────────────────────────
from core.config_loader import ConfigLoader
config = ConfigLoader(CONFIG_PATH).load()
print(f"✅ Config loaded: {len(config.tables)} tables across {len(config.sources)} sources")

# ── Cell 5: Run validation ────────────────────────────────────
from core.orchestrator import ValidationOrchestrator

selected_phases = list(
    FILTER_PHASES
    or getattr(getattr(config, "execution_control", None), "phases", None)
    or ["smoke_test"]
)
print(f"Selected validation phase(s): {', '.join(selected_phases)}")

# SparkSession is already available as `spark` in Fabric Notebooks
orchestrator = ValidationOrchestrator(config=config, spark=spark)

summary = orchestrator.run(
    filter_tags=FILTER_TAGS,
    filter_layers=FILTER_LAYERS,
    filter_phases=selected_phases,
    filter_table_ids=FILTER_IDS,
)

# ── Cell 6: Generate reports ─────────────────────────────────
from reporters.phase_package_reporter import PhasePackageReporter
from reporters.phase_report_tree import PhaseReportTreeWriter
from reporters.report_paths import (
    ReportPathResolver,
    assert_writers_share_run_root,
)

# One resolver serves the whole run: every phase and every writer resolves
# the identical run folder from `report_output` in execution.yml. OUTPUT_DIR
# only relocates the root — the tree below it stays configuration-driven.
path_resolver = ReportPathResolver.from_execution_config(
    CONFIG_PATH, root_folder=OUTPUT_DIR
)
report_tree = PhaseReportTreeWriter(
    getattr(getattr(config, "reporting", None), "settings", None),
    getattr(config, "validations", {}),
)
packager = PhasePackageReporter()

report_paths = {}
for phase in selected_phases:
    phase_paths = path_resolver.resolve(phase, summary.run_id)
    phase_paths.log_resolution()
    assert_writers_share_run_root(phase_paths, (report_tree, packager))
    report_paths.update(report_tree.write(summary, phase_paths))
    report_paths.update(packager.generate(phase_paths))

print("\n📂 Reports saved under:", path_resolver.root)
for name, path in report_paths.items():
    print(f"   {name}: {path}")

# ── Cell 7: Quick DataFrame view (optional) ──────────────────
import pandas as pd

results_df = pd.DataFrame([{
    "table_id": r.table_id,
    "source": r.source_fqn,
    "target": r.target_fqn,
    "layer": r.target_layer,
    "phase": r.migration_phase,
    "status": r.overall_status.value,
    "pass": r.pass_count,
    "fail": r.fail_count,
    "duration_s": round(r.duration_seconds, 2),
} for r in summary.table_results])

display(results_df.sort_values(["status", "table_id"]))

# ── Cell 8: CI gate (optional — raise if failures) ───────────
if summary.failed_tables > 0 or summary.error_tables > 0:
    raise Exception(
        f"❌ Migration validation FAILED: "
        f"{summary.failed_tables} table(s) failed, "
        f"{summary.error_tables} error(s). "
        f"Pass rate: {summary.pass_rate_pct}%"
    )
else:
    print(f"✅ ALL {summary.total_tables} tables PASSED — pass rate {summary.pass_rate_pct}%")
