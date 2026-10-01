"""
fabric_migration_validator/validators/validator_utils.py
"""

def _safe_pct_delta(source_val: float, target_val: float) -> float:
    if source_val == 0:
        return 0.0 if target_val == 0 else 100.0
    return abs(source_val - target_val) / abs(source_val) * 100

def _fqn(schema: str, table: str, dialect) -> str:
    """Render a schema-qualified table name through one SQL dialect."""
    return dialect.qualify(schema, table)
