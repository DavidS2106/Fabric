#!/usr/bin/env python3
"""
Add migration_phase and validation_phases to all table YAML files.
Ensures all config is driven by YAML, no hardcoding.
"""

import yaml
from pathlib import Path
from typing import Dict, Any

def update_table_yaml(file_path: Path) -> None:
    """Add migration_phase and validation_phases if missing."""
    try:
        with open(file_path, 'r') as f:
            content = yaml.safe_load(f)
    except Exception as e:
        print(f"  ✗ Failed to read {file_path.name}: {e}")
        return

    if not content or 'tables' not in content:
        return

    modified = False
    for table in content.get('tables', []):
        # Add migration_phase if missing
        if 'migration_phase' not in table:
            table['migration_phase'] = 'full_load'
            modified = True
            print(f"  + Added migration_phase='full_load' to {table.get('id')}")

        # Add validation_phases if missing
        if 'validation_phases' not in table:
            phase = table.get('migration_phase', 'full_load')
            table['validation_phases'] = [phase]
            modified = True
            print(f"  + Added validation_phases=['{phase}'] to {table.get('id')}")

    if modified:
        try:
            with open(file_path, 'w') as f:
                yaml.dump(content, f, default_flow_style=False, sort_keys=False)
            print(f"  ✓ Updated {file_path.name}")
        except Exception as e:
            print(f"  ✗ Failed to write {file_path.name}: {e}")


def main():
    """Update all table YAML files in the project."""
    table_dir = Path(__file__).parent / 'config' / 'tables'

    if not table_dir.exists():
        print(f"✗ Table directory not found: {table_dir}")
        return

    print(f"Scanning table configurations in: {table_dir}")
    print("=" * 65)

    yaml_files = sorted(table_dir.glob('**/*.yaml'))
    print(f"Found {len(yaml_files)} table configuration files\n")

    for yaml_file in yaml_files:
        update_table_yaml(yaml_file)

    print("\n" + "=" * 65)
    print("✓ Table phase configuration complete")
    print("\nVerifying with config loader...")

    # Verify by loading config
    try:
        from core.config_loader import ConfigLoader
        loader = ConfigLoader('config/execution.yml')
        config = loader.load()
        print(f"✓ Config loaded: {len(config.tables)} tables")
        print(f"✓ Source ref: {config.source_ref}")
        print(f"✓ Target ref: {config.target_ref}")

        # Sample first table
        if config.tables:
            t = config.tables[0]
            print(f"\nSample table:")
            print(f"  ID: {t.id}")
            print(f"  Migration phase: {t.migration_phase}")
            print(f"  Validation phases: {t.validation_phases}")
            print(f"  Source system: {t.source_system}")
            print(f"  Target system: {t.target_system}")
    except Exception as e:
        print(f"✗ Config verification failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
