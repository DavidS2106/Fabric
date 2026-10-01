"""
fabric_migration_validator/core/sql_dialect.py
──────────────────────────────────────────────
Configuration-driven SQL rendering.

Validators never write platform SQL. They describe *what* they need
(a projection, a filter, a row limit, a cohort of keys) and one
:class:`SqlDialect` renders it for the configured database engine.

Every dialect profile lives in ``config/dialects/dialects.yml``, so a new
database platform is supported by adding configuration, not code.

Resolution order for one connection, highest precedence first:

1. ``sql_dialect:`` in the connection YAML (name, or an inline mapping of
   profile fields that overrides the named profile),
2. the ``SQL_DIALECT`` name the connector class declares,
3. ``sql_dialects.default_dialect`` from the dialect configuration.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

logger = logging.getLogger(__name__)

#: Dialect profiles shipped with the framework. Deployments may point
#: ``config_paths.dialects`` at their own file instead.
BUILTIN_DIALECT_PATH = (
    Path(__file__).resolve().parent.parent / "config" / "dialects" / "dialects.yml"
)

#: Row-limit rendering styles a profile may declare.
ROW_LIMIT_STYLES = frozenset({"top", "limit", "fetch_first"})

#: NULL ordering a profile may declare for keyset pagination correctness.
NULL_SORT_ORDERS = frozenset({"nulls_first", "nulls_last"})

#: Key-cohort strategies the engine can render.
COHORT_STRATEGIES = ("key_range", "values_join", "predicate_or", "staged_keys")


class SqlDialectError(ValueError):
    """Raised when dialect configuration is absent, unknown or invalid."""


class BoundSql(str):
    """A SQL string carrying positional DB-API parameters without interpolation."""

    def __new__(cls, value: str, params: Sequence[Any] = ()):
        instance = super().__new__(cls, value)
        instance.params = tuple(params)
        return instance

    def __add__(self, other):
        return BoundSql(super().__add__(str(other)), self.params + sql_params(other))

    def __radd__(self, other):
        return BoundSql(str(other) + str(self), sql_params(other) + self.params)


def sql_params(value: Any) -> tuple:
    return tuple(getattr(value, "params", ()) or ())


def bind_sql(value: str, *parts: Any) -> str:
    params = tuple(item for part in parts for item in sql_params(part))
    return BoundSql(value, params) if params else value


@dataclass(frozen=True)
class SqlDialect:
    """One database engine's statement syntax, entirely from configuration."""

    name: str
    row_limit_style: str
    row_limit_keyword: str
    count_all_function: str
    count_column_function: str
    identifier_quote_open: str
    identifier_quote_close: str
    identifier_quote_escape: str
    text_cast_type: str
    null_sort_order: str
    supports_row_number: bool
    supports_values_constructor: bool
    values_row_alias: str
    derived_table_requires_alias: bool
    cohort_strategy_preference: tuple
    random_order_expression: Optional[str] = None
    #: Template rendering a deterministic integer signature over a row's
    #: values, with ``{columns}`` receiving the quoted column list. Lets a
    #: sample be selected by a stable predicate instead of by ordering the
    #: whole table. ``None`` when the engine offers no such expression.
    row_signature_expression: Optional[str] = None

    # ── Identifiers ───────────────────────────────────────────
    def quote(self, identifier: str) -> str:
        """Quote one database identifier using the configured delimiters.

        The operation is idempotent for identifiers already rendered by this
        dialect. Callers pass qualified names to :meth:`qualify`, which quotes
        and escapes every component independently.
        """
        text = str(identifier)
        if self._is_quoted(text):
            return text
        escaped = text.replace(
            self.identifier_quote_close, self.identifier_quote_escape
        )
        return f"{self.identifier_quote_open}{escaped}{self.identifier_quote_close}"

    def qualify(self, *identifiers: Optional[str]) -> str:
        """Quote and join the non-empty components of an object name."""
        parts = [
            self.quote(item)
            for item in identifiers
            if item is not None and str(item)
        ]
        if not parts:
            raise SqlDialectError(
                "A qualified identifier requires at least one component"
            )
        return ".".join(parts)

    def qualify_path(self, identifier_path: str) -> str:
        """Quote a configured dotted object path component by component."""
        return self.qualify(*(part.strip() for part in str(identifier_path).split(".")))

    def column(self, identifier: str, qualifier: Optional[str] = None) -> str:
        """Render a quoted column, optionally prefixed by a SQL alias."""
        quoted = self.quote(identifier)
        return f"{qualifier}.{quoted}" if qualifier else quoted

    def _is_quoted(self, text: str) -> bool:
        """Whether *text* is one complete identifier quoted by this dialect."""
        if not (
            text.startswith(self.identifier_quote_open)
            and text.endswith(self.identifier_quote_close)
        ):
            return False
        body = text[
            len(self.identifier_quote_open):-len(self.identifier_quote_close)
        ]
        remainder = body.replace(self.identifier_quote_escape, "")
        return self.identifier_quote_close not in remainder

    def alias(self, name: str) -> str:
        """Render a result-set alias. Aliases are framework-generated."""
        return str(name)

    # ── Expressions ───────────────────────────────────────────
    def count_all(self) -> str:
        """Row-count expression wide enough for large tables."""
        return f"{self.count_all_function}(*)"

    def count_column(self, expression: str) -> str:
        return f"{self.count_column_function}({expression})"

    def count_distinct(self, expression: str) -> str:
        return f"{self.count_column_function}(DISTINCT {expression})"

    def cast_to_text(self, expression: str) -> str:
        return f"CAST({expression} AS {self.text_cast_type})"

    def random_order(self) -> str:
        if not self.random_order_expression:
            raise SqlDialectError(
                f"Dialect '{self.name}' declares no random_order_expression; "
                "randomized ordering is unavailable for this platform"
            )
        return self.random_order_expression

    @property
    def supports_row_signature(self) -> bool:
        """Whether this engine can render a deterministic row signature."""
        return bool(self.row_signature_expression)

    def row_signature(self, columns: Sequence[str]) -> str:
        """Deterministic integer signature over one row's *columns*.

        Used to select a stable sample with a predicate rather than by
        ordering a table: the same rows are chosen on every run for unchanged
        data, and the statement needs no sort. The expression itself is
        configuration, so no engine syntax is hard-coded here.
        """
        if not self.row_signature_expression:
            raise SqlDialectError(
                f"Dialect '{self.name}' declares no row_signature_expression; "
                "deterministic signature sampling is unavailable for this platform"
            )
        if not columns:
            raise SqlDialectError("A row signature requires at least one column")
        rendered = ", ".join(self.quote(column) for column in columns)
        return self.row_signature_expression.format(columns=rendered)

    def row_number_over(
        self, partition_columns: Sequence[str], order_expression: str
    ) -> str:
        if not self.supports_row_number:
            raise SqlDialectError(
                f"Dialect '{self.name}' does not support ROW_NUMBER()"
            )
        partition = ", ".join(partition_columns)
        return (
            f"ROW_NUMBER() OVER (PARTITION BY {partition} "
            f"ORDER BY {order_expression})"
        )

    # ── Statements ────────────────────────────────────────────
    def select(
        self,
        projection: str,
        source: str,
        where_clause: str = "",
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        distinct: bool = False,
    ) -> str:
        """Render one bounded SELECT for the configured engine."""
        if limit is not None and not (
            isinstance(limit, int) and not isinstance(limit, bool) and limit > 0
        ):
            raise SqlDialectError("Row limit must be a positive integer")
        distinct_sql = "DISTINCT " if distinct else ""
        head = ""
        tail = ""
        if limit is not None:
            if self.row_limit_style == "top":
                head = f"{self.row_limit_keyword} {limit} "
            elif self.row_limit_style == "limit":
                tail = f" {self.row_limit_keyword} {limit}"
            else:  # fetch_first
                tail = f" {self.row_limit_keyword} {limit} ROWS ONLY"
        order_sql = f" ORDER BY {order_by}" if order_by else ""
        statement = (
            f"SELECT {distinct_sql}{head}{projection} FROM {source}"
            f"{where_clause}{order_sql}{tail}"
        )
        return bind_sql(statement, projection, source, where_clause, order_by)

    def derived_table(self, statement: str, alias: str) -> str:
        """Wrap a subquery so it is usable as a table source."""
        if self.derived_table_requires_alias:
            return bind_sql(f"({statement}) AS {self.alias(alias)}", statement)
        return bind_sql(f"({statement})", statement)

    def values_source(
        self, rows: Sequence[str], columns: Sequence[str], alias: str
    ) -> str:
        """Render an inline row constructor usable as a join source."""
        if not self.supports_values_constructor:
            raise SqlDialectError(
                f"Dialect '{self.name}' does not support a VALUES constructor"
            )
        if not rows or not columns:
            raise SqlDialectError("A VALUES source requires rows and columns")
        column_list = ", ".join(self.alias(column) for column in columns)
        return (
            f"(VALUES {', '.join(rows)}) AS {self.alias(alias)} ({column_list})"
        )

    # ── Ordering semantics ────────────────────────────────────
    @property
    def nulls_sort_first(self) -> bool:
        """Whether ``ORDER BY col`` places NULLs before non-NULL values."""
        return self.null_sort_order == "nulls_first"

    def as_details(self) -> Dict[str, Any]:
        return {
            "sql_dialect": self.name,
            "row_limit_style": self.row_limit_style,
            "null_sort_order": self.null_sort_order,
        }


class SqlDialectRegistry:
    """Every configured dialect profile, plus the configured default."""

    _builtin_lock = threading.Lock()
    _builtin: Optional["SqlDialectRegistry"] = None

    def __init__(self, dialects: Mapping[str, SqlDialect], default_dialect: str):
        if not dialects:
            raise SqlDialectError("No SQL dialect profiles are configured")
        if default_dialect not in dialects:
            raise SqlDialectError(
                f"sql_dialects.default_dialect '{default_dialect}' is not a "
                "configured dialect profile"
            )
        self._dialects = dict(dialects)
        self._default_name = default_dialect

    # ── Construction ──────────────────────────────────────────
    @classmethod
    def from_config(cls, raw: Mapping[str, Any]) -> "SqlDialectRegistry":
        """Build the registry from a ``sql_dialects`` configuration mapping."""
        if not isinstance(raw, Mapping):
            raise SqlDialectError("sql_dialects must be a YAML mapping")
        default_dialect = raw.get("default_dialect")
        if not isinstance(default_dialect, str) or not default_dialect.strip():
            raise SqlDialectError(
                "sql_dialects.default_dialect must be a non-empty string"
            )
        profiles = raw.get("dialects")
        if not isinstance(profiles, Mapping) or not profiles:
            raise SqlDialectError("sql_dialects.dialects must be a YAML mapping")
        resolved = {
            str(name): _build_dialect(str(name), profile)
            for name, profile in profiles.items()
        }
        return cls(resolved, default_dialect.strip())

    @classmethod
    def builtin(cls) -> "SqlDialectRegistry":
        """The profiles shipped with the framework, loaded once."""
        if cls._builtin is None:
            with cls._builtin_lock:
                if cls._builtin is None:
                    cls._builtin = cls.from_config(_load_builtin_document())
        return cls._builtin

    # ── Lookup ────────────────────────────────────────────────
    @property
    def default_name(self) -> str:
        return self._default_name

    @property
    def names(self) -> tuple:
        return tuple(sorted(self._dialects))

    def get(self, name: Optional[str]) -> SqlDialect:
        if name is None or not str(name).strip():
            return self._dialects[self._default_name]
        key = str(name).strip()
        if key not in self._dialects:
            raise SqlDialectError(
                f"Unknown SQL dialect '{key}'. Configured dialects: "
                + ", ".join(self.names)
            )
        return self._dialects[key]

    def resolve(self, connector: Any) -> SqlDialect:
        """Resolve one connector's effective dialect and cache it on it."""
        if connector is None:
            return self.get(None)
        cached = _instance_attribute(connector, "_resolved_sql_dialect")
        if isinstance(cached, SqlDialect):
            return cached
        configured = getattr(getattr(connector, "cfg", None), "sql_dialect", None)
        declared = getattr(connector, "SQL_DIALECT", None)
        # Only a real configured name counts. Anything else (including a test
        # double's auto-created attribute) falls back to the configured default.
        if not (isinstance(declared, str) and declared.strip()):
            declared = None
        if isinstance(configured, Mapping):
            base = self.get(configured.get("name") or declared)
            dialect = _apply_overrides(base, configured)
        elif isinstance(configured, str) and configured.strip():
            dialect = self.get(configured)
        else:
            dialect = self.get(declared)
        try:
            setattr(connector, "_resolved_sql_dialect", dialect)
        except Exception:  # pragma: no cover - read-only doubles
            logger.debug("SQL dialect could not be cached on connector", exc_info=True)
        return dialect


def resolve_dialect(connector: Any) -> SqlDialect:
    """Return one connector's dialect, using whatever registry it carries.

    Connectors prepared by :class:`~core.connection_manager.ConnectionManager`
    carry the run's registry. Anything else (including test doubles) resolves
    against the profiles shipped with the framework.
    """
    if connector is not None:
        attached = _instance_attribute(connector, "_resolved_sql_dialect")
        if isinstance(attached, SqlDialect):
            return attached
        registry = getattr(connector, "sql_dialect_registry", None)
        if isinstance(registry, SqlDialectRegistry):
            return registry.resolve(connector)
    return SqlDialectRegistry.builtin().resolve(connector)


def resolve_cohort_strategy(
    dialect: SqlDialect,
    capabilities: Any,
    configured: Optional[str] = None,
    allowed_strategies: Optional[Sequence[str]] = None,
) -> str:
    """Choose how a batch of keys is handed to the other database.

    ``configured`` wins when the deployment permits it and the dialect and
    connector can serve it. Otherwise the dialect's own preference order
    decides, so no platform name and no strategy is hardcoded in a validator.
    ``allowed_strategies`` is the deployment's whitelist; a strategy outside it
    is never selected, even if the dialect prefers it.
    """
    permitted = (
        {str(item).strip().lower() for item in allowed_strategies}
        if allowed_strategies else None
    )
    candidates = []
    if configured and str(configured).strip().lower() != "auto":
        candidates.append(str(configured).strip().lower())
    candidates.extend(dialect.cohort_strategy_preference)
    fallback = None
    for candidate in candidates:
        if candidate not in COHORT_STRATEGIES:
            raise SqlDialectError(
                f"Unsupported key cohort strategy '{candidate}'. Supported: "
                + ", ".join(COHORT_STRATEGIES)
            )
        if permitted is not None and candidate not in permitted:
            continue
        if _cohort_available(candidate, dialect, capabilities):
            return candidate
        fallback = fallback or candidate
    if permitted is not None and "predicate_or" not in permitted:
        raise SqlDialectError(
            "No permitted key cohort strategy is available for dialect "
            f"'{dialect.name}'; permitted: " + ", ".join(sorted(permitted))
        )
    return "predicate_or"


def _cohort_available(strategy: str, dialect: SqlDialect, capabilities: Any) -> bool:
    if strategy == "key_range":
        return bool(_supports(capabilities, "keyset_pagination"))
    if strategy == "values_join":
        return bool(
            dialect.supports_values_constructor
            and _supports(capabilities, "query_pushdown")
        )
    if strategy == "staged_keys":
        return bool(_supports(capabilities, "staged_keys_write"))
    return strategy == "predicate_or"


def _supports(capabilities: Any, capability: str) -> bool:
    supports = getattr(capabilities, "supports", None)
    if callable(supports):
        try:
            return bool(supports(capability))
        except Exception:  # pragma: no cover - defensive for doubles
            return False
    return False


# ── Internals ─────────────────────────────────────────────────

def _load_builtin_document() -> Mapping[str, Any]:
    import yaml

    if not BUILTIN_DIALECT_PATH.is_file():
        raise SqlDialectError(
            "SQL dialect configuration is missing; expected "
            f"{BUILTIN_DIALECT_PATH}"
        )
    with BUILTIN_DIALECT_PATH.open(encoding="utf-8") as stream:
        document = yaml.safe_load(stream) or {}
    section = document.get("sql_dialects")
    if not isinstance(section, Mapping):
        raise SqlDialectError(
            f"{BUILTIN_DIALECT_PATH.name} must define a sql_dialects mapping"
        )
    return section


_REQUIRED_FIELDS = tuple(
    item.name for item in fields(SqlDialect)
    if item.name not in {
        "name", "random_order_expression", "row_signature_expression",
    }
)


def _build_dialect(name: str, profile: Any) -> SqlDialect:
    if not isinstance(profile, Mapping):
        raise SqlDialectError(f"sql_dialects.dialects.{name} must be a YAML mapping")
    known = {item.name for item in fields(SqlDialect)} - {"name"}
    unknown = sorted(set(profile) - known)
    if unknown:
        raise SqlDialectError(
            f"sql_dialects.dialects.{name} has unsupported field(s): "
            + ", ".join(unknown)
        )
    missing = [field_name for field_name in _REQUIRED_FIELDS if field_name not in profile]
    if missing:
        raise SqlDialectError(
            f"sql_dialects.dialects.{name} is missing required field(s): "
            + ", ".join(missing)
        )
    values = dict(profile)
    _require_choice(name, "row_limit_style", values["row_limit_style"], ROW_LIMIT_STYLES)
    _require_choice(name, "null_sort_order", values["null_sort_order"], NULL_SORT_ORDERS)
    for field_name in (
        "row_limit_keyword", "count_all_function", "count_column_function",
        "identifier_quote_open", "identifier_quote_close",
        "identifier_quote_escape", "text_cast_type", "values_row_alias",
    ):
        if not isinstance(values[field_name], str) or not values[field_name]:
            raise SqlDialectError(
                f"sql_dialects.dialects.{name}.{field_name} must be a non-empty string"
            )
    for field_name in (
        "supports_row_number", "supports_values_constructor",
        "derived_table_requires_alias",
    ):
        if not isinstance(values[field_name], bool):
            raise SqlDialectError(
                f"sql_dialects.dialects.{name}.{field_name} must be boolean"
            )
    preference = values.get("cohort_strategy_preference")
    if not isinstance(preference, (list, tuple)) or not preference:
        raise SqlDialectError(
            f"sql_dialects.dialects.{name}.cohort_strategy_preference must be a "
            "non-empty list"
        )
    unsupported = sorted(set(preference) - set(COHORT_STRATEGIES))
    if unsupported:
        raise SqlDialectError(
            f"sql_dialects.dialects.{name}.cohort_strategy_preference contains "
            "unsupported strategy(ies): " + ", ".join(unsupported)
        )
    values["cohort_strategy_preference"] = tuple(preference)
    random_order = values.get("random_order_expression")
    if random_order is not None and (
        not isinstance(random_order, str) or not random_order.strip()
    ):
        raise SqlDialectError(
            f"sql_dialects.dialects.{name}.random_order_expression must be a "
            "non-empty string or null"
        )
    signature = values.get("row_signature_expression")
    if signature is not None and (
        not isinstance(signature, str)
        or not signature.strip()
        or "{columns}" not in signature
    ):
        raise SqlDialectError(
            f"sql_dialects.dialects.{name}.row_signature_expression must be a "
            "non-empty string containing '{columns}', or null"
        )
    return SqlDialect(name=name, **values)


def _require_choice(name: str, field_name: str, value: Any, allowed) -> None:
    if value not in allowed:
        raise SqlDialectError(
            f"sql_dialects.dialects.{name}.{field_name} must be one of: "
            + ", ".join(sorted(allowed))
        )


def _apply_overrides(base: SqlDialect, overrides: Mapping[str, Any]) -> SqlDialect:
    known = {item.name for item in fields(SqlDialect)} - {"name"}
    applied = {
        key: value for key, value in overrides.items()
        if key in known
    }
    unknown = sorted(set(overrides) - known - {"name"})
    if unknown:
        raise SqlDialectError(
            "Connection sql_dialect has unsupported field(s): " + ", ".join(unknown)
        )
    if "cohort_strategy_preference" in applied:
        preference = applied["cohort_strategy_preference"]
        if not isinstance(preference, (list, tuple)) or not preference:
            raise SqlDialectError(
                "Connection sql_dialect.cohort_strategy_preference must be a "
                "non-empty list"
            )
        applied["cohort_strategy_preference"] = tuple(preference)
    if "row_limit_style" in applied:
        _require_choice(base.name, "row_limit_style", applied["row_limit_style"], ROW_LIMIT_STYLES)
    if "null_sort_order" in applied:
        _require_choice(base.name, "null_sort_order", applied["null_sort_order"], NULL_SORT_ORDERS)
    return replace(base, **applied) if applied else base


def _instance_attribute(connector: Any, name: str) -> Any:
    try:
        return vars(connector).get(name)
    except TypeError:  # objects without __dict__
        return None
