"""Display names from the YAML-derived identity carried by table results."""

import json

from core.models import TableValidationResult


def table_name(table: TableValidationResult) -> str:
    """Remove the schema prefix added to the configured source table name."""
    source = table.source_fqn or ""
    return source.partition(".")[2] if "." in source else source


def primary_key_label(table: TableValidationResult, value, side: str = "source"):
    """Render canonical key tokens with the YAML-derived column labels.

    Unknown formats and row hashes are preserved rather than guessing which
    column a value belongs to. This never changes the stored evidence.
    """
    if not isinstance(value, str) or value.startswith("row_hash:"):
        return value
    mappings = next((
        (check.details or {}).get("key_columns_used")
        for check in table.checks
        if check.check_name == "record_comparison"
        and (check.details or {}).get("key_columns_used")
    ), None)
    if not isinstance(mappings, list) or not mappings:
        return value
    columns = [
        item.get(f"{side}_column") if isinstance(item, dict) else item
        for item in mappings
    ]
    if not all(isinstance(column, str) and column for column in columns):
        return value
    try:
        tokens = json.loads(value)
    except (ValueError, TypeError):
        tokens = [value]
    if not isinstance(tokens, list) or len(tokens) != len(columns):
        return value
    parts = []
    for column, token in zip(columns, tokens):
        if not isinstance(token, str):
            return value
        kind, separator, raw = token.partition(":")
        if not separator or kind not in {"number", "string", "bool", "datetime", "bytes", "null"}:
            return value
        parts.append(f"{column}: {raw}")
    return "; ".join(parts)
