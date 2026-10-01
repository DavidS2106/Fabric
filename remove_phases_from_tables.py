#!/usr/bin/env python3
"""
Remove phase properties from table YAML files.
Phase will come ONLY from CLI, not table configuration.
"""

import yaml
from pathlib import Path


def update_table_yaml(file_path: Path) -> bool:
    """Remove migration_phase and validation_phases from table YAML."""
    try:
        with open(file_path, 'r') as f:
            content = yaml.safe_load(f)
    except Exception as e:
        print(f"  ✗ Failed to read {file_path.name}: {e}")
        return False

    if not content or 'tables' not in content:
        return False

    modified = False
    for table in content.get('tables', []):
        # Remove migration_phase
        if 'migration_phase' in table:
            del table['migration_phase']
            modified = True
            print(f"  - Removed migration_phase from {table.get('id')}")

        # Remove validation_phases
        if 'validation_phases' in table:
            del table['validation_phases']
            modified = True
            print(f"  - Removed validation_phases from {table.get('id')}")

    if modified:
        try:
            with open(file_path, 'w') as f:
                yaml.dump(content, f, default_flow_style=False, sort_keys=False)
            print(f"  ✓ Updated {file_path.name}")
            return True
        except Exception as e:
            print(f"  ✗ Failed to write {file_path.name}: {e}")
            return False

    return False


def main():
    """Remove phase properties from all table YAML files."""
    table_dir = Path(__file__).parent / 'config' / 'tables'

    if not table_dir.exists():
        print(f"✗ Table directory not found: {table_dir}")
        return

    print("=" * 80)
    print("REMOVING PHASE PROPERTIES FROM TABLE YAML FILES")
    print("=" * 80)
    print("\nObjective:")
    print("  - Remove migration_phase from table YAMLs")
    print("  - Remove validation_phases from table YAMLs")
    print("  - Phase will come ONLY from CLI --phase option")
    print("  - All tables will be discovered regardless of phase")
    print("\n" + "-" * 80 + "\n")

    yaml_files = sorted(table_dir.glob('**/*.yaml'))
    print(f"Processing {len(yaml_files)} table files...\n")

    modified_count = 0
    for yaml_file in yaml_files:
        if update_table_yaml(yaml_file):
            modified_count += 1

    print("\n" + "=" * 80)
    print(f"✓ Updated {modified_count}/{len(yaml_files)} files")
    print("=" * 80)

    # Verify
    print("\nVerifying configuration...")
    try:
        from core.config_loader import ConfigLoader
        loader = ConfigLoader('config/execution.yml')
        config = loader.load()

        print(f"✓ Config loaded: {len(config.tables)} tables")

        if config.tables:
            t = config.tables[0]
            print(f"\nSample table (phase properties removed):")
            print(f"  ID: {t.id}")
            print(f"  Migration phase: {t.migration_phase if hasattr(t, 'migration_phase') and t.migration_phase else '(none)'}")
            print(f"  Has validation_phases: {hasattr(t, 'validation_phases') and t.validation_phases is not None}")

    except Exception as e:
        print(f"⚠ Verification failed: {e}")

    print("\n" + "=" * 80)
    print("NEXT STEPS:")
    print("=" * 80)
    print("1. Modify orchestrator._filter_tables() to NOT filter by phase")
    print("2. Modify orchestrator.run() to map phase → validation profile")
    print("3. Update run_validation.py CLI to use --phase for profile selection")
    print("4. Test: python run_validation.py --phase full_load")
    print("5. Expected: All 83 tables, validated with full_load profile")
    print("=" * 80)


if __name__ == '__main__':
    main()
