"""Atomic, table-scoped checkpoints for resumable keyset validation."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any, Dict, Optional


def parse_optional_json(
    value: Any,
    default: Any = None,
    *,
    config_key: str = "checkpoint",
    table_id: Optional[str] = None,
) -> Any:
    """Parse one optional JSON value without changing blank/default semantics."""
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return default
        try:
            return json.loads(stripped)
        except json.JSONDecodeError as exc:
            context = f"configuration key '{config_key}'"
            if table_id:
                context += f" for table '{table_id}'"
            raise ValueError(f"Invalid JSON in {context}: {exc}") from exc
    return value


class CheckpointStore:
    def __init__(self, config):
        self.enabled = bool(config.enabled)
        self.resume_failed_run = bool(config.resume_failed_run)
        self.directory = Path(config.directory) if self.enabled else None

    def _path(self, table_id: str, namespace: Optional[str] = None) -> Path:
        safe_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", table_id).strip("._")
        if not safe_id:
            raise ValueError("Checkpoint table ID cannot be empty")
        suffix = f".{namespace}" if namespace else ""
        return self.directory / f"{safe_id}{suffix}.json"

    def load_incremental(self, table_id: str) -> Optional[Dict[str, Any]]:
        """Return the last successfully committed incremental boundary."""
        if not self.enabled:
            return None
        path = self._path(table_id, "incremental")
        if not path.is_file():
            return None
        value = parse_optional_json(
            path.read_text(encoding="utf-8"), default=None,
            config_key="performance.checkpoint.incremental", table_id=table_id,
        )
        if value is not None and not isinstance(value, dict):
            raise ValueError(
                f"Invalid incremental checkpoint for table '{table_id}': "
                "expected a JSON object"
            )
        return value

    def commit_incremental(self, table_id: str, state: Dict[str, Any]) -> None:
        """Atomically advance a table's boundary after successful validation."""
        if not self.enabled:
            return
        self.directory.mkdir(parents=True, exist_ok=True)
        path = self._path(table_id, "incremental")
        temp_path = path.with_suffix(path.suffix + ".tmp")
        payload = {
            "table_id": table_id,
            "status": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **dict(state),
        }
        with temp_path.open("w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, default=str, indent=2)
        temp_path.replace(path)

    def load(self, table_id: str) -> Optional[Dict[str, Any]]:
        if not self.enabled or not self.resume_failed_run:
            return None
        path = self._path(table_id)
        if not path.is_file():
            return None
        value = parse_optional_json(
            path.read_text(encoding="utf-8"),
            default=None,
            config_key="performance.checkpoint",
            table_id=table_id,
        )
        if value is None:
            return None
        if not isinstance(value, dict):
            raise ValueError(
                "Invalid JSON in configuration key 'performance.checkpoint' "
                f"for table '{table_id}': expected a JSON object"
            )
        return value if value.get("status") in {"failed", "in_progress"} else None

    def save(
        self,
        table_id: str,
        batch_number: int,
        last_processed_key: Any,
        processed_rows: int,
        status: str,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self.enabled:
            return
        self.directory.mkdir(parents=True, exist_ok=True)
        path = self._path(table_id)
        temp_path = path.with_suffix(".tmp")
        payload = {
            "table_id": table_id,
            "batch_number": batch_number,
            "last_processed_key": last_processed_key,
            "processed_rows": processed_rows,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "metrics": metrics or {},
        }
        with temp_path.open("w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, default=str, indent=2)
        temp_path.replace(path)

    def complete(self, table_id: str, batch_number: int, processed_rows: int) -> None:
        self.save(table_id, batch_number, None, processed_rows, "completed")
