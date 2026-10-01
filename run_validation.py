"""
fabric_migration_validator/run_validation.py
---------------------------------------------
CLI entry point for the migration validation utility.

Usage examples:

  # Full run — all tables (loads everything from execution.yml)
  python run_validation.py --config config/execution.yml

  # Only critical tables
  python run_validation.py --config config/execution.yml --tags critical

  # Only Gold layer, initial load phase
  python run_validation.py --config config/execution.yml \
      --layers gold --phases full_load

  # Specific table IDs
  python run_validation.py --config config/execution.yml \
      --table-ids CUST_001 SALES_001

  # Fail fast mode (exit code 1 on any failure — for CI/CD gates)

  # Sequential (no parallelism) — for debugging
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

# Load connection variables from the canonical connection configuration folder.
connection_env_path = Path(__file__).parent / "config" / "connections" / ".env"
load_dotenv(dotenv_path=connection_env_path)

from core.config_loader import ConfigLoader, load_execution_document
from core.models import ValidationStatus
from core.orchestrator import ValidationOrchestrator
from core.preflight import PreflightReportWriter, PreflightValidator
from core.runtime_identity import RUNTIME_IDENTITY_FIELDS, log_runtime_identity
from reporters.phase_package_reporter import PhasePackageReporter
from reporters.phase_report_tree import PhaseReportTreeWriter
from reporters.report_paths import (
    ReportPathResolver,
    assert_writers_share_run_root,
)


def _report_phase_paths(paths) -> None:
    """Announce the resolved output tree before a phase writes anything."""
    fields = paths.log_fields()
    paths.log_resolution()
    print(f"\n  Report paths for phase '{fields['phase']}':")
    for label, key in (
        ("Phase folder", "phase_folder"),
        ("Run ID", "run_id"),
        ("Timestamp", "timestamp"),
        ("Run root", "run_root"),
        ("Excel", "excel_path"),
        ("JSON", "json_path"),
        ("HTML", "html_path"),
        ("ZIP", "zip_path"),
    ):
        print(f"    {label:<13}: {fields[key]}")


def run_file_validation(args, config, spark, selected_phases):
    """Run File -> Fabric only when ``--comparison-mode`` selects it.

    The File module only ingests, discovers and routes: the datasets it
    prepares are validated by the orchestrator, the profiles and the reporters
    this file already uses. A deployment without ``File/config`` — or with
    ``file_validation.enabled: false`` — is untouched by this call.
    """
    from File.core.file_orchestrator import FileValidationRunner

    modes = getattr(args, "comparison_modes", None)
    # File validation is opt-in. Its global `enabled` setting means the
    # capability is available; it must not make File tables part of every
    # SQL Server -> Fabric or Fabric -> Fabric database run.
    if not modes:
        logging.getLogger(__name__).info(
            "File validation not selected; use --comparison-mode "
            "file_to_file and/or files_to_table to run File tables"
        )
        return []
    runner = FileValidationRunner.from_project(
        args.config, config, spark=spark,
        project_root=Path(__file__).resolve().parent,
        staging_root=getattr(args, "file_staging_dir", None),
    )
    if runner is None:
        if modes:
            raise ValueError(
                "--comparison-mode selects File tables, but File validation is "
                "not configured or is disabled in "
                "File/config/config_setting.yml"
            )
        return []
    selected_tables = [
        table for table in runner.tables
        if (not getattr(args, "table_ids", None)
            or table.table_id in args.table_ids)
        and (not getattr(args, "tags", None)
             or set(args.tags) & set(table.tags))
        and table.comparison_mode in modes
    ]
    route_labels = {
        "file_to_file": "File -> File",
        "files_to_table": "Files -> DB table",
    }
    print("\n  Execution selection:")
    print("    Route type       : " + ", ".join(route_labels[mode] for mode in modes))
    print(f"    Configuration    : {runner.project_root / 'File' / 'config' / 'table' / '*.yml'}")
    print(f"    Comparison mode  : {', '.join(modes)}")
    print(f"    Staging directory: {runner.staging_root}")
    print(f"    Selected tables  : {len(selected_tables)}")
    for table in selected_tables:
        print(
            f"      {table.table_id}: {table.source.connection} -> "
            f"{table.target.connection} [{table.document}]"
        )
    return runner.run(
        selected_phases,
        filter_table_ids=args.table_ids,
        filter_tags=args.tags,
        filter_comparison_modes=modes,
        only_checks=args.checks,
        skip_checks=args.skip_checks,
    )


def configure_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S"
    )


def _active_database_table_ids(config):
    """IDs belonging to the source/target pair selected by execution.yml."""
    source_ref = getattr(config, "source_ref", None)
    target_ref = getattr(config, "target_ref", None)
    return [
        table.id for table in config.tables
        if getattr(table, "enabled", True)
        and table.source_system == source_ref
        and table.target_system == target_ref
    ]


def _database_table_config_pattern(config_path):
    """Return the table YAML glob actually used by the DB configuration loader."""
    document = load_execution_document(Path(config_path))
    return document.get("config_paths", {}).get(
        "tables", "config/tables/**/*.y*ml"
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fabric Migration Validation Utility",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("--config", default="config/execution.yml",
                        help="Path to master execution YAML (default: config/execution.yml)")
    parser.add_argument("--tags", nargs="*",
                        help="Filter: only validate tables with these tags")
    parser.add_argument("--layers", nargs="*",
                        help="Filter: only validate specific Medallion layers")
    parser.add_argument("--phases", nargs="*",
                        help="Filter: only validate specific migration phases")
    parser.add_argument(
        "--phase",
        action="append",
        help="Validation phase to execute; repeat for multiple phases",
    )
    parser.add_argument("--table-ids", nargs="*",
                        help="Filter: only validate specific table IDs")
    parser.add_argument("--table-names", nargs="*",
                        help="Filter: only validate specific source table names")
    parser.add_argument(
        "--comparison-mode", nargs="+", dest="comparison_modes",
        choices=["file_to_file", "files_to_table"],
        help=(
            "Validate File tables of these comparison modes only. Database "
            "tables are not part of a File-mode run."
        ),
    )
    parser.add_argument(
        "--checks", nargs="*",
        choices=["row_count","key_validation","null_validation","aggregate","referential","scd",
                 "business_rules","pii","duplicate","data_value_validation",
                 "record_comparison"],
        help="Run ONLY these checks (overrides all profiles and table overrides)"
    )
    parser.add_argument(
        "--skip-checks", nargs="*", dest="skip_checks",
        choices=["row_count","key_validation","null_validation","aggregate","referential","scd",
                 "business_rules","pii","duplicate","data_value_validation",
                 "record_comparison"],
        help="Skip these specific checks (applied after --checks resolution)"
    )
    parser.add_argument("--output-dir", default=None,
                        help="Override output directory for reports")
    parser.add_argument(
        "--file-staging-dir",
        default=None,
        help=(
            "Override temporary SQLite staging for File comparisons only "
            "(for example D:/MigrationValidator/File/.staging)"
        ),
    )
    parser.add_argument("--verbose", action="store_true",
                        help="Enable debug logging")
    parser.add_argument("--preflight-only", action="store_true",
                        help="Run only preflight validation and its reports")
    parser.add_argument(
        "--preflight",
        action="store_true",
        help="Run Preflight validation only",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    configure_logging(args.verbose)

    print("\n" + "="*60)
    print("  Fabric Migration Validator")
    print("="*60)

    # Which copy of the project is actually executing. Several copies of this
    # tree commonly coexist on one machine and a report cannot identify the
    # one that produced it, so the identity is logged (never reported) before
    # any validation starts.
    identity = log_runtime_identity()
    print()
    for field in RUNTIME_IDENTITY_FIELDS:
        print(f"  {field}={identity[field]}")

    # Load config
    print(f"\n  Loading config from: {args.config}")
    loader = ConfigLoader(args.config)
    config = loader.load()

    print(f"  Config loaded: {len(config.tables)} tables, "
          f"{len(config.sources)} sources")

    preflight_only = args.preflight_only or getattr(args, "preflight", False)
    if preflight_only:
        selected_phases = []
        print("  Execution mode: preflight only")
    else:
        cli_phases = getattr(args, "phase", None) or args.phases
        configured_phases = getattr(
            getattr(config, "execution_control", None), "phases", None
        )
        selected_phases = list(
            cli_phases or configured_phases or ["smoke_test"]
        )
        print(f"  Selected validation phase(s): {', '.join(selected_phases)}")

    # One capability check per run decides whether Spark may initialize.
    # A missing JDK/PySpark selects the configured fallback engine; the
    # identical validations run either way.
    from core.engine_capabilities import (
        detect_engine_availability,
        reset_run_cache,
        resolve_spark_session,
    )

    if getattr(args, "comparison_modes", None):
        # File datasets are exposed to the validation framework through the
        # local SQLite connector. Starting a JVM here adds no capability to a
        # File-only run and Spark's gateway startup can block indefinitely on
        # locked-down Windows desktops. Keep the availability snapshot for the
        # normal fallback resolver, but deliberately do not create a session.
        reset_run_cache()
        selection = getattr(config.performance, "engine_selection", None)
        engine_availability = detect_engine_availability(selection)
        spark = None
        print(
            "  File comparison mode: SparkSession initialization skipped; "
            "using the file/SQLite execution path",
            flush=True,
        )
    else:
        spark, engine_availability = resolve_spark_session(config.performance)
        if spark is not None:
            print("  SparkSession detected")
        else:
            print(
                "  No SparkSession available"
                + (
                    f" ({engine_availability.spark_unavailable_reason}); "
                    f"fallback engine: {engine_availability.selected_fallback}"
                    if engine_availability.spark_unavailable_reason else ""
                )
            )

    # One resolver for the whole execution: it owns the run timestamp, so
    # every phase and every writer resolves the identical run folder.
    path_resolver = ReportPathResolver.from_execution_config(
        args.config, root_folder=args.output_dir
    )
    print(f"  Report root: {path_resolver.root} (run timestamp "
          f"{path_resolver.timestamp})")

    if preflight_only:
        # Preflight is an explicit, self-contained execution mode. Phase runs do
        # not pass through this path and are never gated or filtered by it.
        from core.execution_engine import ExecutionEngineResolver

        preflight_context = ExecutionEngineResolver(config.performance).resolve(spark)
        preflight_report = PreflightValidator(args.config, preflight_context).run()
        preflight_paths = PreflightReportWriter(
            args.config, resolver=path_resolver
        ).generate_all(preflight_report)
        print(f"\n  Preflight status: {preflight_report.overall_preflight_status}")
        for fmt, path in preflight_paths.items():
            print(f"  {fmt.upper()} preflight report: {path}")
        return 0 if preflight_report.overall_preflight_status in {
            "PASSED", "SKIPPED"
        } else 1

    reporting_settings = getattr(
        getattr(config, "reporting", None), "settings", None
    )

    # One resolved run folder per phase, written by the report tree and then
    # archived by the packager. Both are checked against that run root before
    # anything is generated, so no writer can build a tree of its own.
    report_tree = PhaseReportTreeWriter(
        reporting_settings, getattr(config, "validations", {})
    )
    packager = PhasePackageReporter()

    def write_completed_phase(phase, phase_summary):
        report_timings = phase_summary.execution_metrics.setdefault(
            "report_generation_timings", []
        )
        phase_paths = path_resolver.resolve(phase, phase_summary.run_id)
        _report_phase_paths(phase_paths)
        assert_writers_share_run_root(phase_paths, (report_tree, packager))
        report_started = time.perf_counter()
        generated_reports = report_tree.write(phase_summary, phase_paths)
        report_seconds = round(time.perf_counter() - report_started, 6)
        report_timings.append({
            "phase": phase,
            "step": "phase_report_generation",
            "execution_time_seconds": report_seconds,
        })
        print(f"  Phase report generation time: {report_seconds:.6f}s", flush=True)
        for name, path in generated_reports.items():
            print(f"  {name}: {path}", flush=True)
        package_started = time.perf_counter()
        generated_packages = packager.generate(phase_paths)
        package_seconds = round(time.perf_counter() - package_started, 6)
        report_timings.append({
            "phase": phase,
            "step": "phase_report_packaging",
            "execution_time_seconds": package_seconds,
        })
        print(f"  Phase report packaging time: {package_seconds:.6f}s", flush=True)
        for name, path in generated_packages.items():
            print(f"  {name}: {path}", flush=True)

    # File -> Fabric validation is opt-in through --comparison-mode. A normal
    # database run must not acquire File tables merely because File/config is
    # enabled.
    file_outcomes = run_file_validation(args, config, spark, selected_phases)

    # Phase validation is independent of preflight. Table discovery and profile
    # resolution remain owned by the existing orchestrator and YAML configuration.
    # The callback writes a phase as soon as its last selected table completes;
    # reporting errors propagate without rewriting any validation result.
    if getattr(args, "comparison_modes", None):
        # A comparison mode is a property of a File table only, so selecting
        # one selects File tables and nothing else.
        print(
            "\n  Comparison mode selected "
            f"({', '.join(args.comparison_modes)}); database routes are not "
            "part of this run."
        )
        summary = None
    else:
        active_database_table_ids = _active_database_table_ids(config)
        if not active_database_table_ids:
            raise ValueError(
                "No database tables are configured for the active route "
                f"'{config.source_ref}' -> '{config.target_ref}'. Update "
                "table source_system/target_system values or select the "
                "correct source and target in config/execution.yml."
            )
        table_pattern = _database_table_config_pattern(args.config)
        print("\n  Execution selection:")
        print("    Route type       : DB -> DB")
        print(f"    Source connection: {config.source_ref}")
        print(f"    Target connection: {config.target_ref}")
        print(f"    Configuration    : {table_pattern}")
        print(f"    Selected tables  : {len(active_database_table_ids)}")
        orchestrator = ValidationOrchestrator(config=config, spark=spark)
        try:
            summary = orchestrator.run(
                filter_tags=args.tags,
                filter_layers=args.layers,
                filter_phases=selected_phases,
                filter_table_ids=args.table_ids,
                filter_table_names=getattr(args, "table_names", None),
                eligible_table_ids=active_database_table_ids,
                only_checks=args.checks,
                skip_checks=args.skip_checks,
                phase_completed_callback=write_completed_phase,
            )
        except ValueError as exc:
            # A deployment that validates files only has no database table in
            # scope. Its File results have already been produced and reported,
            # so the run reports on them instead of failing on an empty
            # selection. Every other configuration error still stops the run.
            if not file_outcomes or not str(exc).startswith("No tables found"):
                raise
            print("\n  No database table in scope; reporting File results only.")
            summary = None

    has_failures = bool(
        summary is not None and summary.overall_status != ValidationStatus.PASS
    ) or any(
        outcome.status != ValidationStatus.PASS for outcome in file_outcomes
    )

    print(f"\n  {'ALL VALIDATIONS PASSED' if not has_failures else 'SOME VALIDATIONS FAILED'}")
    if summary is not None:
        print(f"  Pass rate: {summary.pass_rate_pct}% ({summary.passed_tables}/{summary.total_tables})")
    for outcome in file_outcomes:
        print(
            f"  File phase '{outcome.phase}': {outcome.status.value} "
            f"({outcome.summary.passed_tables}/{outcome.summary.total_tables} "
            f"datasets passed)"
        )
    print()

    return 1 if has_failures else 0


if __name__ == "__main__":
    sys.exit(main())
