"""
fabric_migration_validator/core/capabilities.py
───────────────────────────────────────────────
Connector capability declaration and resolution.

Validation code must never branch on a platform name, a connection name or a
driver. It asks the connector what it can do, and the connector answers with
this capability record. A connector declares its capabilities in code
(``CAPABILITIES`` class attribute) and any deployment may override or extend
them from the connection YAML, so a brand new Source/Target platform becomes
usable without touching the validation engine.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, fields, replace
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConnectorCapabilities:
    """What one configured connector is able to do on the remote engine.

    Every flag defaults to ``False``: a connector that declares nothing is
    treated as the least capable option, so the engine degrades safely instead
    of issuing an operation the platform cannot serve.
    """

    # Server-side / query-engine computation.
    query_pushdown: bool = False
    aggregation_pushdown: bool = False
    distinct_count_pushdown: bool = False
    # One scan producing many statistics instead of one scan per statistic.
    multi_statistic_pushdown: bool = False

    # Bounded reading strategies.
    keyset_pagination: bool = False
    range_pagination: bool = False
    partition_read: bool = False
    file_partition_read: bool = False
    row_limit_clause: bool = False
    # Writing a temporary key set the other side can join against. Off by
    # default: a validation connection is read-only unless a deployment says
    # otherwise in its connection configuration.
    staged_keys_write: bool = False

    # Filtering and comparison.
    watermark_filter: bool = False
    hash_comparison: bool = False

    # Distributed and concurrent reading.
    spark_read: bool = False
    parallel_read: bool = False

    # 0 means "no configured ceiling on combined expressions per query".
    max_pushdown_expressions: int = 0

    # ── Introspection ─────────────────────────────────────────
    @classmethod
    def flag_names(cls) -> tuple:
        return tuple(
            item.name for item in fields(cls) if item.type == "bool"
        )

    def supports(self, capability: str) -> bool:
        """Return whether a capability, named with or without ``supports_``."""
        name = normalize_capability_name(capability)
        value = getattr(self, name, False)
        return bool(value) if isinstance(value, bool) else False

    def supports_all(self, *capabilities: str) -> bool:
        return all(self.supports(name) for name in capabilities)

    def as_dict(self) -> Dict[str, Any]:
        return {item.name: getattr(self, item.name) for item in fields(self)}

    def statistic_batch_size(self, requested: int) -> int:
        """Clamp a combined-statistic count to the connector's ceiling."""
        ceiling = int(self.max_pushdown_expressions or 0)
        if requested <= 0:
            return 0
        if ceiling <= 0:
            return requested
        return min(requested, ceiling)

    # ── Derivation ────────────────────────────────────────────
    def merged_with(self, overrides: Optional[Mapping[str, Any]]) -> "ConnectorCapabilities":
        """Return a copy with configuration-supplied overrides applied."""
        if not overrides:
            return self
        valid = {item.name for item in fields(self)}
        applied: Dict[str, Any] = {}
        for key, value in dict(overrides).items():
            name = normalize_capability_name(key)
            if name not in valid:
                raise ValueError(
                    f"Unsupported connector capability '{key}'. Supported: "
                    + ", ".join(sorted(valid))
                )
            if name == "max_pushdown_expressions":
                applied[name] = _coerce_count(value, key)
            else:
                applied[name] = _coerce_bool(value, key)
        return replace(self, **applied)

    @classmethod
    def from_mapping(cls, mapping: Optional[Mapping[str, Any]]) -> "ConnectorCapabilities":
        return cls().merged_with(mapping)


#: A connector that only promises to answer ``execute_query``.
MINIMAL_CAPABILITIES = ConnectorCapabilities(query_pushdown=True)

#: Full-featured relational query engine reachable over a SQL connection.
#: Individual connectors narrow this down; nothing here names a platform.
RELATIONAL_QUERY_ENGINE_CAPABILITIES = ConnectorCapabilities(
    query_pushdown=True,
    aggregation_pushdown=True,
    distinct_count_pushdown=True,
    multi_statistic_pushdown=True,
    keyset_pagination=True,
    range_pagination=True,
    row_limit_clause=True,
    watermark_filter=True,
    hash_comparison=True,
    parallel_read=True,
)


def normalize_capability_name(capability: Any) -> str:
    """Accept ``supports_query_pushdown``, ``query-pushdown`` or ``QueryPushdown``."""
    text = re.sub(r"[\s\-]+", "_", str(capability or "").strip())
    text = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", text).lower()
    if text.startswith("supports_"):
        text = text[len("supports_"):]
    return text


def _coerce_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "yes", "1", "y", "on", "enabled"}:
        return True
    if normalized in {"false", "no", "0", "n", "off", "disabled"}:
        return False
    raise ValueError(f"Connector capability '{field_name}' must be boolean")


def _coerce_count(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(
            f"Connector capability '{field_name}' must be a non-negative integer"
        )
    return value


def declared_capabilities(connector: Any) -> ConnectorCapabilities:
    """Return the capabilities a connector class declares in code."""
    declared = getattr(connector, "CAPABILITIES", None)
    if isinstance(declared, ConnectorCapabilities):
        return declared
    if isinstance(declared, Mapping):
        return ConnectorCapabilities.from_mapping(declared)
    return MINIMAL_CAPABILITIES


def configured_capability_overrides(connector: Any) -> Dict[str, Any]:
    """Return the ``capabilities:`` mapping from the connection configuration."""
    overrides = getattr(getattr(connector, "cfg", None), "capabilities", None)
    if overrides is None or overrides == "":
        return {}
    if isinstance(overrides, Mapping):
        return dict(overrides)
    if isinstance(overrides, (list, tuple, set, str, int, float, bool)):
        raise ValueError("Connection 'capabilities' must be a YAML mapping")
    # Anything else cannot come from configuration (for example a test double).
    logger.debug("Ignoring non-configuration capability override on connector")
    return {}


def resolve_capabilities(connector: Any) -> ConnectorCapabilities:
    """Resolve one connector's effective capabilities.

    Precedence, lowest to highest:
      1. the connector class declaration,
      2. the ``capabilities:`` mapping in the connection configuration,
      3. runtime reality (a distributed read needs a live session).

    The result is cached on the connector instance so repeated validations do
    not re-resolve, and callers may pass anything (including a test double).
    """
    if connector is None:
        return ConnectorCapabilities()

    cached = _instance_attribute(connector, "_resolved_capabilities")
    if isinstance(cached, ConnectorCapabilities):
        return cached

    # An instance-level ``capabilities`` value wins outright. Read it from the
    # instance dictionary so the ``BaseConnector.capabilities`` property (which
    # delegates back here) cannot recurse.
    explicit = _instance_attribute(connector, "capabilities")
    if isinstance(explicit, ConnectorCapabilities):
        resolved = explicit
    elif isinstance(explicit, Mapping):
        resolved = ConnectorCapabilities.from_mapping(explicit)
    else:
        resolved = declared_capabilities(connector).merged_with(
            configured_capability_overrides(connector)
        )

    if resolved.spark_read and not _has_distributed_session(connector):
        # Declared but not available in this run: fall back automatically.
        resolved = replace(resolved, spark_read=False)

    try:
        setattr(connector, "_resolved_capabilities", resolved)
    except Exception:  # pragma: no cover - read-only doubles
        logger.debug("Connector capabilities could not be cached", exc_info=True)
    return resolved


def _instance_attribute(connector: Any, name: str) -> Any:
    """Read an instance attribute without triggering class descriptors."""
    try:
        return vars(connector).get(name)
    except TypeError:  # objects without __dict__
        return None


def _has_distributed_session(connector: Any) -> bool:
    context = getattr(connector, "context", None)
    return getattr(context, "spark_session", None) is not None


def capability_report(capabilities: ConnectorCapabilities) -> Dict[str, Any]:
    """Return a report-friendly, ``supports_``-prefixed capability mapping."""
    return {
        (f"supports_{name}" if isinstance(value, bool) else name): value
        for name, value in capabilities.as_dict().items()
    }
