"""Connection management for every configured Source and Target connector.

Each connector declares what it can do through ``CAPABILITIES``; validators read
those capabilities instead of branching on a platform, connection or driver
name. New connectors join by registering a class with its own capability
record, or by declaring ``capabilities:`` in the connection configuration.
"""

from __future__ import annotations

import contextlib
import logging
import re
import threading
import time
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass, is_dataclass, replace
from types import SimpleNamespace
from typing import Any, Dict, Iterable, Iterator, Optional, Tuple, Type

import pandas as pd
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import URL

from core.capabilities import (
    ConnectorCapabilities,
    MINIMAL_CAPABILITIES,
    RELATIONAL_QUERY_ENGINE_CAPABILITIES,
    resolve_capabilities,
)
from core.execution_engine import EngineType, ExecutionContext
from core.masking import MASK, mask_text
from core.performance import ConcurrencyGovernor, optional_slot

logger = logging.getLogger(__name__)


class DistributedMaterializationLimitExceeded(RuntimeError):
    """Raised when a distributed frame is larger than the configured Pandas cap.

    Callers treat this as a signal to fall back to a bounded strategy instead of
    pulling an unbounded dataset into one Python process.
    """

_TRUE_VALUES = {"true", "yes", "1", "y", "on"}
_FALSE_VALUES = {"false", "no", "0", "n", "off"}
_MUTATING_SQL = re.compile(
    r"\b(insert|update|delete|truncate|drop|alter|create|merge|execute|exec|grant|revoke)\b",
    re.IGNORECASE,
)


#: Start of any token that suspends the read-only lexical scan: a line
#: comment, a block comment, or a quoted literal / identifier.
_SQL_NON_CODE_START = re.compile(r"--|/\*|['\"\[`]")
_SQL_LINE_END = re.compile(r"[\r\n]")
_SQL_CLOSING_DELIMITER = {"'": "'", '"': '"', "[": "]", "`": "`"}
_SQL_LEADING_SELECT = re.compile(r"select\b", re.IGNORECASE)
_SQL_NON_WHITESPACE = re.compile(r"\S")


def _iter_sql_code_spans(sql: str) -> Iterator[Tuple[int, int]]:
    """Yield ``(start, end)`` index pairs of the code segments of ``sql``.

    Everything between the yielded spans is a comment, a string literal or a
    quoted identifier. The scan is index based and jumps between delimiters
    with C-level ``find``/``search`` calls, so it holds no per-character copy
    of the statement: a generated statement of hundreds of megabytes costs no
    additional memory to validate. (The previous implementation expanded the
    statement into one Python object per character, roughly eight bytes per
    input byte, which is what exhausted memory on very large cohort queries.)
    """
    length = len(sql)
    position = 0
    while position < length:
        match = _SQL_NON_CODE_START.search(sql, position)
        if match is None:
            yield position, length
            return
        start = match.start()
        if start > position:
            yield position, start
        token = match.group()
        if token == "--":
            line_end = _SQL_LINE_END.search(sql, start + 2)
            position = length if line_end is None else line_end.start()
            continue
        if token == "/*":
            end = sql.find("*/", start + 2)
            position = length if end < 0 else end + 2
            continue
        closing = _SQL_CLOSING_DELIMITER[token]
        index = start + 1
        while True:
            end = sql.find(closing, index)
            if end < 0:
                # Unterminated literal: the remainder is not code.
                position = length
                break
            if end + 1 < length and sql[end + 1] == closing:
                # Doubled delimiter escapes itself inside the literal.
                index = end + 2
                continue
            position = end + 1
            break


def _sql_code_only(sql: str) -> str:
    """Mask literals, comments, and quoted identifiers for SQL safety checks.

    The returned text is used only for lexical validation; callers still send
    the original statement and parameters to the database unchanged. The mask
    is assembled from bounded slices (never from a per-character list), so its
    cost is one copy of the statement.
    """
    pieces = []
    previous_end = 0
    for start, end in _iter_sql_code_spans(sql):
        if start > previous_end:
            pieces.append(" " * (start - previous_end))
        pieces.append(sql[start:end])
        previous_end = end
    if previous_end < len(sql):
        pieces.append(" " * (len(sql) - previous_end))
    return "".join(pieces)


def _sql_is_single_read_only_select(sql: str) -> bool:
    """Lexically verify ``sql`` is one read-only SELECT, without copying it.

    Comments, literals and quoted identifiers are skipped by index; the
    leading-keyword, statement-separator and mutating-keyword checks run on
    the code spans in place through ``pos``/``endpos`` so no masked copy of
    the statement is ever materialized.
    """
    leading_checked = False
    for start, end in _iter_sql_code_spans(sql):
        if not leading_checked:
            first_token = _SQL_NON_WHITESPACE.search(sql, start, end)
            if first_token is None:
                # A span made only of whitespace: keep looking for the first token.
                continue
            if _SQL_LEADING_SELECT.match(sql, first_token.start(), end) is None:
                return False
            leading_checked = True
        if sql.find(";", start, end) != -1:
            return False
        if _MUTATING_SQL.search(sql, start, end) is not None:
            return False
    return leading_checked


def _error_chain(exc):
    pending, seen = [exc], set()
    while pending:
        error = pending.pop()
        if error is None or id(error) in seen:
            continue
        seen.add(id(error))
        yield error
        pending.extend(getattr(error, name, None) for name in ("orig", "original", "__cause__", "__context__"))


def _native_states(exc):
    states = set()
    for error in _error_chain(exc):
        for name in ("sqlstate", "pgcode"):
            value = getattr(error, name, None)
            if isinstance(value, str):
                states.add(value.upper())
        for argument in getattr(error, "args", ()):
            if isinstance(argument, str):
                states.update(re.findall(r"\b(?:08[A-Z0-9]{3}|42[A-Z0-9]{3}|HYT0[01]|28000|3D000|3F000)\b", argument.upper()))
    return states


def _deterministic_query_error(exc):
    from .object_dependency import invalid_object_error
    return invalid_object_error(exc) or any(
        state.startswith(("42", "28", "3D", "3F")) for state in _native_states(exc)
    ) or any(token in sanitize_error_text(exc).lower() for token in (
        "invalid column", "invalid schema", "invalid sql", "syntax error",
        "invalid configuration", "invalid mapping",
    ))


def _is_query_timeout_error(exc: Exception) -> bool:
    """Whether a driver error reports the configured query timeout expiring.

    A statement timeout is deterministic for an unchanged statement: the same
    query against the same data will expire again, so it is never retried
    unchanged (see :func:`_is_retryable_database_error`).
    """
    if exc is None:
        return False
    if getattr(exc, "connection_failure", False) or "login timeout" in sanitize_error_text(exc).lower():
        return False
    if getattr(exc, "query_timeout", False):
        return True
    message = str(getattr(exc, "orig", None) or exc)
    return "HYT00" in _native_states(exc) or "query timeout expired" in message.lower()


#: Driver/SQLSTATE markers of failures that come from the transport, the login
#: handshake or a transiently unavailable server rather than from the statement.
_TRANSIENT_ERROR_MARKERS = (
    "08s01", "08001", "08007", "08003", "hyt01",          # SQLSTATE link/login
    "communication link failure", "connection reset", "connection was forcibly",
    "tcp provider", "login timeout expired", "semaphore timeout",
    "transport-level error", "broken pipe", "connection is closed",
    "closed connection", "not currently available", "service is busy",
    "transient",                                            # Azure SQL wording
    "40613", "40197", "40501", "49918", "49919", "49920", "10053", "10054",
    "10060", "1205",                                        # deadlock victim
    "tcp timeout", "transport failure", "temporary network failure", "login timeout",
)


def _is_transient_connection_error(exc: Exception) -> bool:
    """Whether a failure is a transport/availability problem worth one more try."""
    if exc is None or _is_query_timeout_error(exc):
        return False
    if getattr(exc, "retry_exhausted", False):
        return False
    if _deterministic_query_error(exc):
        return False
    if getattr(exc, "connection_failure", False):
        return True
    if getattr(exc, "transient", False):
        return True
    if isinstance(exc, (ConnectionError, TimeoutError)):
        return True
    if any(state.startswith("08") or state == "HYT01" for state in _native_states(exc)):
        return True
    if any(getattr(error, "connection_invalidated", False) is True for error in _error_chain(exc)):
        return True
    return any(marker in sanitize_error_text(exc).lower() for marker in _TRANSIENT_ERROR_MARKERS)


_SQL_ECHO_PATTERN = re.compile(r"\[SQL:.*", re.IGNORECASE | re.DOTALL)
_PARAMETERS_ECHO_PATTERN = re.compile(r"\[parameters:.*", re.IGNORECASE | re.DOTALL)
_BACKGROUND_LINK_PATTERN = re.compile(r"\(Background on this error.*", re.DOTALL)
#: pandas wraps a driver failure as ``Execution failed on sql '<statement>': <error>``.
_PANDAS_SQL_ECHO_PATTERN = re.compile(
    r"Execution failed on sql\s+'.*'(?=\s*:\s)", re.IGNORECASE | re.DOTALL
)
_PANDAS_SQL_ECHO_PREFIX = re.compile(r"^\s*Execution failed on sql\b", re.IGNORECASE)
#: Any surviving statement fragment: a SELECT list or a VALUES constructor.
_STATEMENT_FRAGMENT_PATTERN = re.compile(
    r"(\bSELECT\s+(?:TOP\b|DISTINCT\b|\*|\[|\w+\s*,)|\bVALUES\s*\(|\bINSERT\s+INTO\b).*",
    re.IGNORECASE | re.DOTALL,
)
#: Longest error text a log line or check message may carry.
SAFE_ERROR_TEXT_LIMIT = 500


def sanitize_error_text(exc: Any, limit: int = SAFE_ERROR_TEXT_LIMIT) -> str:
    """Return an exception's message without the statement it was executing.

    SQLAlchemy ``StatementError`` (and its DB-API subclasses) append the whole
    statement to ``str(exc)``, and pandas wraps driver failures as
    ``Execution failed on sql '<statement>': <error>``; a generated cohort
    lookup carries sampled key and column values, so echoing it into a log or
    a check message exposes row data and can be megabytes long. The driver's
    own message is used when it can be reached (``exc.orig`` or the chained
    cause of a pandas wrapper); any remaining ``[SQL: ...]`` /
    ``[parameters: ...]`` echo and any leftover statement fragment are cut,
    connection secrets are masked and the text is bounded.
    """
    if exc is None:
        return ""
    original = getattr(exc, "orig", None)
    if original is None and isinstance(exc, BaseException):
        text = str(exc)
        cause = exc.__cause__ if exc.__cause__ is not None else exc.__context__
        if _PANDAS_SQL_ECHO_PREFIX.match(text) and cause is not None:
            original = cause
    text = str(original) if original is not None else str(exc)
    nested = getattr(original, "orig", None) if original is not None else None
    if nested is not None:
        text = str(nested)
    text = _PANDAS_SQL_ECHO_PATTERN.sub("Execution failed on sql <statement omitted>", text)
    text = _SQL_ECHO_PATTERN.sub("", text)
    text = _PARAMETERS_ECHO_PATTERN.sub("", text)
    text = _BACKGROUND_LINK_PATTERN.sub("", text)
    text = _STATEMENT_FRAGMENT_PATTERN.sub("<statement omitted>", text)
    text = " ".join(text.split())
    text = mask_text(text)
    if limit and len(text) > limit:
        text = text[: max(limit - 3, 0)] + "..."
    return text


def error_class_of(exc: Any) -> str:
    """``statement_timeout`` | ``transient_connection`` | ``database_error``."""
    if exc is None:
        return "database_error"
    if _is_query_timeout_error(exc):
        return "statement_timeout"
    if getattr(exc, "transient", False) or _is_transient_connection_error(exc):
        return "transient_connection"
    return "database_error"


class DatabaseQueryError(RuntimeError):
    """A database read failed; the message never carries the statement text.

    ``query_timeout`` marks a deterministic statement timeout, ``transient`` a
    transport/availability failure. Both are read by retry and adaptive
    parallelism decisions instead of re-parsing driver messages.
    """

    def __init__(
        self,
        message: str,
        *,
        error_type: str,
        query_timeout: bool = False,
        transient: bool = False,
        sql_bytes: int = 0,
        elapsed_seconds: float = 0.0,
        configured_timeout_seconds: Optional[int] = None,
    ) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.query_timeout = query_timeout
        self.transient = transient
        self.sql_bytes = sql_bytes
        self.elapsed_seconds = elapsed_seconds
        self.configured_timeout_seconds = configured_timeout_seconds


class ConnectionUnavailableError(DatabaseQueryError):
    """The centralized configured retry budget is exhausted."""
    retry_exhausted = True

    def __init__(self, role, original, attempts, *, query_timeout=False):
        super().__init__(sanitize_error_text(original), error_type=type(original).__name__,
                         query_timeout=query_timeout, transient=not query_timeout)
        self.role = role
        self.attempts = attempts
        self.category = f"{role.upper()}_CONNECTION_UNAVAILABLE" if role in ("source", "target") else "DEPENDENCY_UNAVAILABLE"
        self.root_category = "QUERY_TIMEOUT" if query_timeout else "TRANSIENT_CONNECTION_ERROR"


def wrap_database_error(
    exc: Exception,
    *,
    sql: Any = None,
    elapsed_seconds: float = 0.0,
    configured_timeout_seconds: Optional[int] = None,
    secrets: Iterable[Any] = (),
) -> Exception:
    """Return an exception safe to log and report, chained to the original.

    ``MemoryError`` and already-safe framework errors are returned unchanged
    so callers that dispatch on them keep working.
    """
    if isinstance(exc, (MemoryError, DatabaseQueryError, KeyboardInterrupt)):
        return exc
    message = sanitize_error_text(exc)
    message = mask_text(message, secrets)
    original_text = str(exc)
    if (
        message == original_text
        and not isinstance(exc, _statement_error_types())
        and not _is_query_timeout_error(exc)
    ):
        return exc
    wrapped = DatabaseQueryError(
        message,
        error_type=type(exc).__name__,
        query_timeout=_is_query_timeout_error(exc),
        transient=_is_transient_connection_error(exc),
        sql_bytes=len(str(sql)) if sql is not None else 0,
        elapsed_seconds=elapsed_seconds,
        configured_timeout_seconds=configured_timeout_seconds,
    )
    wrapped.__cause__ = exc
    return wrapped


def _statement_error_types() -> tuple:
    try:
        from sqlalchemy.exc import StatementError
    except Exception:  # pragma: no cover - sqlalchemy is a hard dependency
        return ()
    return (StatementError,)


def normalize_connection_type(connection_type: Any) -> str:
    """Normalize public aliases to one canonical registry key."""
    normalized = re.sub(r"[\s_-]+", "", str(connection_type or "").strip().lower())
    aliases = {
        "sql": "sqlserver",
        "sqlserver": "sqlserver",
        "mssql": "sqlserver",
        "fabric": "fabric",
        "fabriclakehouse": "fabric",
    }
    return aliases.get(normalized, normalized)


def _parse_boolean(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(
        f"{field_name} must be one of: "
        f"{', '.join(sorted(_TRUE_VALUES | _FALSE_VALUES))}"
    )


def _resolve_context_value(value: Any, connection_context: Optional[Dict[str, Any]]) -> Any:
    if not isinstance(value, str) or not connection_context:
        return value
    match = re.fullmatch(r"\{(database|schema|table|table_name)\}", value.strip())
    if not match:
        return value
    key = "table" if match.group(1) == "table_name" else match.group(1)
    resolved = connection_context.get(key)
    return resolved if resolved not in (None, "") else value


def merge_connection_context(config: Any, connection_context: Optional[Dict[str, Any]]) -> Any:
    """Return a config copy with database/schema/table placeholders resolved."""
    if not connection_context:
        return config
    if isinstance(config, dict):
        values = dict(config)
    else:
        values = dict(vars(config))
    database_uses_context = str(values.get("database", "")).strip() == "{database}"
    values = {
        key: _resolve_context_value(value, connection_context)
        for key, value in values.items()
    }
    if database_uses_context:
        values["database_required_for_validation"] = True
    if is_dataclass(config):
        valid = {field.name for field in config.__dataclass_fields__.values()}
        return replace(config, **{key: value for key, value in values.items() if key in valid})
    return SimpleNamespace(**values)


_NAMED_PARAMETER = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)")


def _ordered_parameters(
    statement: str, parameters: Optional[Dict[str, Any]]
) -> Tuple[Any, ...]:
    """Return named parameters in the order the statement references them."""
    if not parameters:
        return ()
    return tuple(
        parameters[name]
        for name in _NAMED_PARAMETER.findall(statement)
        if name in parameters
    )


def _positional_statement(statement: str) -> str:
    """Rewrite named placeholders as positional ones for driver-level cursors."""
    return _NAMED_PARAMETER.sub("?", statement)


def _is_retryable_database_error(
    exc: Exception, retry_on_timeout: bool = False
) -> bool:
    """Return whether a database failure is transient enough to justify retry.

    A statement timeout (``HYT00`` / "query timeout expired") is deterministic:
    re-running the identical statement re-runs the identical scan and expires
    again after the identical wait. It is therefore not retried unless
    ``performance.query_execution.retry.retry_on_timeout`` explicitly asks for
    it. Transport and availability failures remain retryable.
    """
    if exc is None:
        return False
    if getattr(exc, "retry_exhausted", False) or _deterministic_query_error(exc):
        return False
    if _is_query_timeout_error(exc):
        return bool(retry_on_timeout)
    return _is_transient_connection_error(exc)



@dataclass(frozen=True)
class ConnectionIdentity:
    """What makes one established connection reusable by another validation.

    A connection is established once per unique ``connection_type + server +
    database``. Two further components complete the key so that reuse can never
    silently change how a connection is made:

    * ``principal`` — the authenticating context (user, authentication type,
      trusted flag). Two configurations for the same endpoint under different
      credentials must never share one session.
    * ``transport`` — the remaining connection-affecting settings (driver,
      timeout, endpoint fields). Two references that differ only by driver or
      timeout get their own connection; references that are genuinely identical
      still share one, which is what keeps reuse high.

    A connection *reference name* is deliberately not part of the key: two
    references describing the same endpoint the same way should share one
    connection rather than each establishing its own.

    Every component is read from the connection configuration. Nothing here
    knows a platform, a host, a database or a credential value.
    """

    connection_type: str
    server: str
    database: str
    principal: str
    transport: str = ""

    @property
    def key(self) -> Tuple[str, str, str, str, str]:
        return (
            self.connection_type,
            self.server,
            self.database,
            self.principal,
            self.transport,
        )

    @property
    def endpoint_key(self) -> Tuple[str, str, str]:
        """The endpoint identity alone: connection type, server and database."""
        return (self.connection_type, self.server, self.database)

    def describe(self) -> str:
        """Return a log-safe description with the endpoint and principal masked."""
        return (
            f"type={self.connection_type or 'unknown'} "
            f"server={MASK if self.server else 'unset'} "
            f"database={self.database or 'unset'} "
            f"principal={MASK if self.principal else 'default'}"
        )


def _configured_endpoint(config: Any) -> str:
    """Return the configured endpoint, whatever field the connection uses."""
    for attribute in ("sql_endpoint", "host", "jdbc_url"):
        value = getattr(config, attribute, None)
        if value not in (None, ""):
            text_value = str(value).strip()
            if text_value:
                return text_value
    return ""


def connection_identity(config: Any) -> ConnectionIdentity:
    """Derive the reuse identity of one configured connection."""
    port = getattr(config, "port", None)
    server = _configured_endpoint(config)
    if port not in (None, "") and str(port) not in server:
        server = f"{server},{port}"
    principal_parts = [
        str(getattr(config, "username", None) or ""),
        str(getattr(config, "authentication_type", None) or ""),
        str(getattr(config, "trusted_connection", None) or ""),
    ]
    # Settings that change how the connection itself is opened. Reuse must not
    # silently apply one reference's driver or timeout to another's connection.
    transport_parts = [
        str(getattr(config, "odbc_driver", None) or ""),
        str(getattr(config, "sql_endpoint", None) or ""),
    ]
    return ConnectionIdentity(
        connection_type=normalize_connection_type(getattr(config, "type", "")),
        server=server.casefold(),
        database=str(getattr(config, "database", None) or "").strip().casefold(),
        principal="|".join(principal_parts).casefold(),
        transport="|".join(transport_parts).casefold(),
    )


def get_best_sql_driver(configured_driver: Optional[str] = None) -> str:
    """Return the explicitly configured SQL Server ODBC driver."""
    driver = str(configured_driver or "").strip()
    if not driver:
        raise ValueError("Connection configuration requires an ODBC driver")
    return driver


def validate_odbc_driver(driver: str) -> None:
    """Fail early with a clear category when a configured driver is unavailable."""
    import pyodbc

    installed = pyodbc.drivers()
    if installed and driver not in installed:
        raise RuntimeError("Configured ODBC driver is not installed")


class BaseConnector(ABC):
    #: What this connector can do. Subclasses narrow or widen this; deployments
    #: may override any flag from the connection configuration.
    CAPABILITIES: ConnectorCapabilities = MINIMAL_CAPABILITIES
    #: Production relational connectors opt in when they can read one schema's
    #: complete column catalogue efficiently. Custom connectors keep the
    #: existing per-table metadata API unless they explicitly support this.
    BULK_SCHEMA_METADATA = False

    def __init__(self, config: Any, context: ExecutionContext):
        self.cfg = config
        self.context = context
        self._conn = None
        self._engine = None
        self._lock = threading.Lock()
        self.last_error: Optional[Exception] = None
        self.runtime_config = None
        self.retry_count = 0
        #: Shared, run-wide ceiling on concurrent remote operations.
        self.concurrency_governor: Optional[ConcurrencyGovernor] = None
        #: Optional performance ledger recording every remote operation.
        self.performance_ledger = None
        #: Reporting label for the connection this connector serves.
        self.system_name = str(
            getattr(config, "name", None) or getattr(config, "type", "") or "unknown"
        )
        self.role: Optional[str] = None

    @property
    def capabilities(self) -> ConnectorCapabilities:
        """Return this connector's effective, runtime-checked capabilities."""
        return resolve_capabilities(self)

    def supports(self, capability: str) -> bool:
        return self.capabilities.supports(capability)

    def _connection_timeout_seconds(self) -> int:
        """Return centralized connect timeout, with explicit legacy support."""
        pool = getattr(self.runtime_config, "connection_pool", None)
        configured = getattr(pool, "connection_timeout_seconds", None)
        if configured is not None:
            return int(configured)
        legacy = getattr(self.cfg, "connection_timeout_seconds", None)
        if legacy is not None:
            warnings.warn(
                "connection_timeout_seconds in a connection definition is deprecated; "
                "use performance.connection_management.connection_timeout_seconds",
                DeprecationWarning,
                stacklevel=2,
            )
            return int(legacy)
        raise ValueError(
            "Missing performance.connection_management.connection_timeout_seconds"
        )

    @abstractmethod
    def execute_query(self, sql: str) -> pd.DataFrame:
        pass

    def execute_query_batches(self, sql: str, batch_size: int):
        yield from self._resilient_batches(sql, batch_size)

    def _resilient_batches(self, sql, batch_size, connection=None):
        def start():
            iterator = self._execute_query_batches_once(sql, batch_size, connection)
            try:
                return next(iterator), iterator
            except StopIteration:
                return None
        started = self._retry_operation(start)
        if started is None:
            return
        first, iterator = started
        try:
            yield first
            yield from iterator
        except Exception as exc:
            # Replaying an already-delivered stream would duplicate records.
            # Fail its dependency instead; no matching or sampling rule changes.
            if _is_transient_connection_error(exc) or _is_query_timeout_error(exc):
                raise ConnectionUnavailableError(self.role, exc, 1,
                    query_timeout=_is_query_timeout_error(exc)) from exc
            raise
        finally:
            iterator.close()

    def _execute_query_batches_once(self, sql: str, batch_size: int, leased_connection=None):
        """Stream one read-only query as bounded DataFrame batches."""
        parameters = tuple(getattr(sql, "params", ()) or ())
        statement = self._ensure_read_only_sql(sql)
        started = time.perf_counter()
        timing = {
            "connection_wait_seconds": 0.0,
            "db_execution_seconds": 0.0,
            "configured_timeout_seconds": self._query_timeout_seconds(),
            "effective_statement_timeout_seconds": None,
            "attempts": 1,
        }
        self._query_timing.last = timing
        try:
            with optional_slot(self.concurrency_governor):
                checkout_started = time.perf_counter()
                with self._checked_connection(leased_connection) as connection:
                    timing["connection_wait_seconds"] = (
                        time.perf_counter() - checkout_started
                    )
                    timing["effective_statement_timeout_seconds"] = (
                        self._effective_statement_timeout(connection)
                    )
                    options = {"chunksize": batch_size}
                    if parameters:
                        options["params"] = parameters
                    execute_started = time.perf_counter()
                    frames = iter(pd.read_sql(statement, connection, **options))
                    while True:
                        fetch_started = time.perf_counter()
                        try:
                            frame = next(frames)
                        except StopIteration:
                            break
                        timing["db_execution_seconds"] = (
                            time.perf_counter() - execute_started
                        )
                        self._record_operation(
                            "streamed_read_sql", fetch_started, statement, frame
                        )
                        yield frame
        except Exception as exc:
            elapsed = time.perf_counter() - started
            if _is_query_timeout_error(exc):
                logger.error(
                    "QUERY_TIMEOUT | configured_timeout_seconds=%s | "
                    "effective_statement_timeout_seconds=%s | endpoint=%s | "
                    "streamed_read=true | elapsed=%.3fs | sql_bytes=%d | "
                    "timeout_kind=statement",
                    timing["configured_timeout_seconds"],
                    timing["effective_statement_timeout_seconds"],
                    self.system_name, elapsed, len(statement),
                )
            if not isinstance(exc, (MemoryError, GeneratorExit)):
                logger.error(
                    "Streamed database read failed | endpoint=%s | elapsed=%.3fs | "
                    "sql_bytes=%d | error_type=%s | error=%s",
                    self.system_name, elapsed, len(statement),
                    type(exc).__name__,
                    mask_text(
                        sanitize_error_text(exc),
                        (getattr(self.cfg, "username", None),
                         getattr(self.cfg, "password", None)),
                    ),
                )
                logger.debug("Streamed read failure detail", exc_info=True)
                wrapped = wrap_database_error(
                    exc,
                    sql=statement,
                    elapsed_seconds=elapsed,
                    configured_timeout_seconds=timing["configured_timeout_seconds"],
                    secrets=(
                        getattr(self.cfg, "username", None),
                        getattr(self.cfg, "password", None),
                    ),
                )
                if wrapped is not exc:
                    raise wrapped from exc
            raise

    @abstractmethod
    def test_connection(self) -> bool:
        pass

    def test_connectivity(self) -> bool:
        """Compatibility name used by the standalone smoke-check flow."""
        return self.test_connection()

    @staticmethod
    def _ensure_read_only_sql(sql: str) -> str:
        """Reject anything that is not one read-only SELECT.

        Validation is lexical and index based (see
        :func:`_sql_is_single_read_only_select`), so a very large generated
        statement is checked without building a masked or per-character copy.
        """
        statement = str(sql or "").strip()
        without_trailing_terminator = statement[:-1].rstrip() if statement.endswith(";") else statement
        if not _sql_is_single_read_only_select(without_trailing_terminator):
            raise ValueError("Smoke and validation queries must be a single read-only SELECT")
        return statement

    # ── Query timeout ─────────────────────────────────────────
    def _query_timeout_seconds(self) -> Optional[int]:
        """Configured statement timeout (``performance.query_execution.timeout_seconds``).

        Returns ``None`` when no positive value is configured, in which case the
        driver default (no statement timeout) is left untouched.
        """
        protection = getattr(self.runtime_config, "database_protection", None)
        value = getattr(protection, "timeout_seconds", None)
        try:
            seconds = int(value)
        except (TypeError, ValueError):
            return None
        return seconds if seconds > 0 else None

    def _apply_query_timeout(self, dbapi_connection: Any) -> Optional[int]:
        """Apply the configured statement timeout to one DB-API connection.

        For pyodbc the ``timeout`` attribute is the *query* timeout
        (``SQL_ATTR_QUERY_TIMEOUT``) inherited by every cursor of the connection;
        it is distinct from the login timeout passed to ``connect``. Returns the
        seconds applied, or ``None`` when nothing was configured or the driver
        does not expose the attribute.
        """
        seconds = self._query_timeout_seconds()
        if seconds is None or dbapi_connection is None:
            return None
        try:
            dbapi_connection.timeout = seconds
        except Exception as exc:  # pragma: no cover - driver without the attribute
            logger.warning(
                "Query timeout could not be applied | endpoint=%s | "
                "timeout_seconds=%s | error=%s",
                self.system_name, seconds, exc,
            )
            return None
        logger.debug(
            "Query timeout applied | endpoint=%s | timeout_seconds=%s",
            self.system_name, seconds,
        )
        return seconds

    def _query_timeout_listener(self):
        """SQLAlchemy ``connect`` listener applying the query timeout per connection."""
        listener = getattr(self, "_query_timeout_event_listener", None)
        if listener is None:
            def _on_connect(dbapi_connection, connection_record):
                self._apply_query_timeout(dbapi_connection)

            listener = _on_connect
            self._query_timeout_event_listener = listener
        return listener

    # ── Pooled connection lifecycle ───────────────────────────
    def _retry_delay(self, retry, attempt):
        import random
        # Full jitter over exponential backoff; the base delay and number of
        # attempts come from the existing retry configuration.
        time.sleep(random.uniform(0, retry.delay_seconds * (2 ** (attempt - 1))))

    def _retry_operation(self, operation):
        retry = getattr(getattr(self.runtime_config, "database_protection", None), "retry", None)
        attempt = 0
        while True:
            attempt += 1
            try:
                return operation()
            except Exception as exc:
                timeout = _is_query_timeout_error(exc)
                allowed = bool(retry and retry.enabled)
                limit = (retry.timeout_max_attempts if timeout else retry.max_attempts) if allowed else 1
                if allowed and attempt < limit and _is_retryable_database_error(exc, retry.retry_on_timeout):
                    self.retry_count += 1
                    self._retry_delay(retry, attempt)
                    continue
                if not getattr(exc, "retry_exhausted", False) and (
                    _is_transient_connection_error(exc) or timeout
                ) and not _deterministic_query_error(exc):
                    raise ConnectionUnavailableError(self.role, exc, attempt, query_timeout=timeout) from exc
                raise

    @staticmethod
    def _discard_connection(connection, exc):
        invalidate = getattr(connection, "invalidate", None)
        if callable(invalidate):
            invalidate(exc)
        else:
            close = getattr(connection, "close", None)
            if callable(close):
                close()

    @contextlib.contextmanager
    def _checked_connection(self, connection=None):
        """Invalidate a broken lease before its context returns it to the pool."""
        if connection is not None and (
            getattr(connection, "invalidated", False) is True
            or getattr(connection, "closed", False) is True
        ):
            connection = None
        with contextlib.ExitStack() as stack:
            try:
                lease = self._get_sqlalchemy_engine().connect() if connection is None else contextlib.nullcontext(connection)
                active = stack.enter_context(lease)
            except Exception as exc:
                if _is_query_timeout_error(exc):
                    wrapped = DatabaseQueryError(sanitize_error_text(exc), error_type=type(exc).__name__, transient=True)
                    wrapped.connection_failure = True
                    raise wrapped from exc
                raise
            try:
                yield active
            except Exception as exc:
                if _is_transient_connection_error(exc):
                    self._discard_connection(active, exc)
                raise

    @contextlib.contextmanager
    def acquire_connection(self) -> Iterator[Any]:
        """Borrow one connection for the block and return it on exit.

        Metadata operations run on a connection borrowed from this connector's
        pool, so many workers can validate concurrently through one shared,
        cached connector instead of sharing a single raw driver handle.
        """
        engine = self._poolable_engine()
        if engine is None:
            # A connector without a poolable connection string manages its own
            # handle; there is nothing to borrow or return.
            direct = self._get_odbc_conn()
            try:
                yield direct
            except Exception as exc:
                if _is_transient_connection_error(exc):
                    self._discard_connection(direct, exc)
                    if self._conn is direct:
                        self._conn = None
                raise
            return
        with optional_slot(self.concurrency_governor):
            # Leaving the block returns the connection to the pool; the pool,
            # not this call, decides whether to keep the underlying handle open.
            with self._checked_connection() as connection:
                yield connection

    @contextlib.contextmanager
    def query_session(self) -> Iterator[Any]:
        """Yield an executor that reuses one checked-out connection.

        Long-running bounded validators use this for their sequential page
        reader. Probe workers continue to use ``execute_query`` and therefore
        receive independent pooled connections; a DBAPI connection is never
        used concurrently by multiple threads.
        """
        engine = self._poolable_engine()
        if engine is None:
            yield self
            return
        # Retry initial checkout, then retain the lease for sequential queries.
        stack = contextlib.ExitStack()
        try:
            connection = self._retry_operation(lambda: stack.enter_context(self._checked_connection()))
            yield _ConnectionBoundQueryExecutor(self, connection)
        finally:
            stack.close()

    def _poolable_engine(self) -> Optional[Any]:
        """Return this connector's pooled engine, or None when it has none."""
        try:
            return self._get_sqlalchemy_engine()
        except Exception:
            logger.debug(
                "Connector has no poolable connection; using its direct handle",
                exc_info=True,
            )
            return None

    def ensure_connection(self) -> None:
        """Establish the configured connection by borrowing one from the pool."""
        def connect():
            with self.acquire_connection():
                pass
        self._retry_operation(connect)

    def _fetch_all(
        self, statement: str, parameters: Optional[Dict[str, Any]] = None
    ) -> list:
        return self._retry_operation(lambda: self._fetch_all_once(statement, parameters))

    def _fetch_all_once(self, statement, parameters):
        """Run one read-only metadata statement on a borrowed connection."""
        started = time.perf_counter()
        with self.acquire_connection() as connection:
            if hasattr(connection, "execute"):
                rows = list(connection.execute(text(statement), parameters or {}))
            else:  # a connector managing its own driver handle
                cursor = connection.cursor()
                try:
                    ordered = _ordered_parameters(statement, parameters)
                    cursor.execute(_positional_statement(statement), *ordered)
                    rows = cursor.fetchall()
                finally:
                    cursor.close()
        self._record_operation("metadata_query", started, statement, None)
        return rows

    def execute_connectivity_query(self) -> None:
        rows = self._fetch_all("SELECT 1")
        if not rows or int(rows[0][0]) != 1:
            raise RuntimeError("Connectivity query did not return the expected result")

    def get_current_database(self) -> Optional[str]:
        rows = self._fetch_all("SELECT DB_NAME()")
        if not rows or rows[0][0] is None:
            return None
        return str(rows[0][0])

    def schema_exists(self, schema: str) -> bool:
        rows = self._fetch_all(
            "SELECT TOP 1 1 FROM INFORMATION_SCHEMA.SCHEMATA "
            "WHERE SCHEMA_NAME = :schema_name",
            {"schema_name": schema},
        )
        return bool(rows)

    def get_table_columns(self, schema: str, table: str) -> Dict[str, str]:
        rows = self._fetch_all(
            "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = :schema_name AND TABLE_NAME = :table_name "
            "ORDER BY ORDINAL_POSITION",
            {"schema_name": schema, "table_name": table},
        )
        return {str(row[0]): str(row[1]) for row in rows}

    def get_schema_table_columns(
        self, schema: str
    ) -> Dict[str, Dict[str, str]]:
        """Return all table/column metadata for one schema in one query."""
        rows = self._fetch_all(
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = :schema_name "
            "ORDER BY TABLE_NAME, ORDINAL_POSITION",
            {"schema_name": schema},
        )
        tables: Dict[str, Dict[str, str]] = {}
        for table_name, column_name, data_type in rows:
            tables.setdefault(str(table_name), {})[str(column_name)] = str(data_type)
        return tables

    def get_preflight_table_columns(
        self, schema: str, table_names: list
    ) -> Dict[str, set]:
        """Fetch TABLE_NAME and COLUMN_NAME only for configured tables (preflight-optimized).

        Returns lightweight structure without datatypes:
        {table_name: {column_name1, column_name2, ...}, ...}
        """
        if not table_names:
            return {}

        placeholders = ",".join(f":{i}" for i in range(len(table_names)))
        params = {str(i): name for i, name in enumerate(table_names)}
        params["schema_name"] = schema

        query = f"""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :schema_name
              AND TABLE_NAME IN ({placeholders})
            ORDER BY TABLE_NAME, ORDINAL_POSITION
        """

        rows = self._fetch_all(query, params)
        result: Dict[str, set] = {}
        for table_name, column_name in rows:
            table_key = str(table_name)
            if table_key not in result:
                result[table_key] = set()
            result[table_key].add(str(column_name))
        return result

    def _masked_connection_error(self, exc: Exception) -> str:
        """Return an error message with configured credentials redacted."""
        return mask_text(
            exc,
            (
                getattr(self.cfg, "username", None),
                getattr(self.cfg, "password", None),
            ),
        )

    def _get_sqlalchemy_engine(self):
        """Return the connector's reusable SQLAlchemy engine."""
        if self._engine is None:
            with self._lock:
                if self._engine is None:
                    connection_url = URL.create(
                        "mssql+pyodbc",
                        query={"odbc_connect": self._get_conn_str()},
                    )
                    runtime = self.runtime_config
                    pool = getattr(runtime, "connection_pool", None)
                    timeout = self._connection_timeout_seconds()
                    engine_options = {
                        "pool_pre_ping": (
                            pool.pool_pre_ping if pool and pool.enabled else True
                        ),
                        "hide_parameters": True,
                        "connect_args": {"timeout": timeout},
                    }
                    if pool and pool.enabled:
                        engine_options.update(
                            pool_size=pool.pool_size,
                            max_overflow=pool.max_overflow,
                            pool_recycle=pool.pool_recycle_seconds,
                        )
                    engine = create_engine(connection_url, **engine_options)
                    query_timeout = self._query_timeout_seconds()
                    if query_timeout is not None:
                        # Every pooled DB-API connection receives the configured
                        # statement timeout; the connect_args timeout above is
                        # only the login timeout.
                        event.listen(engine, "connect", self._query_timeout_listener())
                        logger.info(
                            "Query timeout configured | endpoint=%s | timeout_seconds=%s",
                            self.system_name, query_timeout,
                        )
                    self._engine = engine
        return self._engine

    def _read_sql_dataframe(
        self, sql: str, connection: Optional[Any] = None,
        params: Optional[Any] = None,
        *,
        table: Optional[str] = None,
        validation_name: Optional[str] = None,
        endpoint: Optional[str] = None,
        side: Optional[str] = None,
        query_type: str = "SELECT",
    ) -> pd.DataFrame:
        """Execute a Pandas query through a supported SQLAlchemy connection.

        Every remote read passes through the run's global concurrency governor,
        so no combination of table, validation, source/target and batch workers
        can exceed the single configured ceiling.
        """
        retry = getattr(
            getattr(self.runtime_config, "database_protection", None),
            "retry",
            None,
        )
        attempts = retry.max_attempts if retry and retry.enabled else 1
        retry_on_timeout = bool(getattr(retry, "retry_on_timeout", False)) if retry else False
        timeout_attempts = (
            max(1, int(getattr(retry, "timeout_max_attempts", 1) or 1))
            if retry and retry.enabled and retry_on_timeout else 1
        )
        endpoint_name = endpoint or self.system_name or self.role or "unknown"
        query_label = query_type or "SELECT"
        configured_timeout = self._query_timeout_seconds()
        sql_bytes = len(str(sql))
        timeout_failures = 0
        attempt = 0
        while True:
            attempt += 1
            started = time.perf_counter()
            timing = {
                "connection_wait_seconds": 0.0,
                "db_execution_seconds": 0.0,
                "configured_timeout_seconds": configured_timeout,
                "effective_statement_timeout_seconds": None,
                "attempts": attempt,
            }
            self._query_timing.last = timing
            try:
                with optional_slot(self.concurrency_governor):
                    # Time spent obtaining a connection (governor slot already
                    # held, so this is the pool checkout / login) is kept apart
                    # from the statement's own execution time.
                    checkout_started = time.perf_counter()
                    with self._checked_connection(connection) as checked_out:
                        timing["connection_wait_seconds"] = time.perf_counter() - checkout_started
                        timing["effective_statement_timeout_seconds"] = self._effective_statement_timeout(checked_out)
                        execute_started = time.perf_counter()
                        frame = (pd.read_sql(sql, checked_out, params=params)
                                 if params else pd.read_sql(sql, checked_out))
                        timing["db_execution_seconds"] = time.perf_counter() - execute_started
                # Whatever is left of the wall time is the wait for the global
                # concurrency slot; it counts as connection wait, not execution.
                governor_wait = (
                    (time.perf_counter() - started)
                    - timing["connection_wait_seconds"]
                    - timing["db_execution_seconds"]
                )
                timing["connection_wait_seconds"] += max(0.0, governor_wait)
                self._record_operation("read_sql", started, sql, frame)
                return frame
            except Exception as exc:
                elapsed = time.perf_counter() - started
                timing["db_execution_seconds"] = elapsed - timing["connection_wait_seconds"]
                is_timeout = _is_query_timeout_error(exc)
                is_transient = _is_transient_connection_error(exc)
                safe_error = sanitize_error_text(
                    exc
                ) if not isinstance(exc, MemoryError) else "MemoryError"
                safe_error = mask_text(
                    safe_error,
                    (getattr(self.cfg, "username", None), getattr(self.cfg, "password", None)),
                )
                if is_timeout:
                    timeout_failures += 1
                    logger.error(
                        "QUERY_TIMEOUT | configured_timeout_seconds=%s | "
                        "effective_statement_timeout_seconds=%s | table=%s | "
                        "validation=%s | endpoint=%s | side=%s | elapsed=%.3fs | "
                        "sql_bytes=%d | timeout_kind=statement",
                        configured_timeout,
                        timing.get("effective_statement_timeout_seconds"),
                        table,
                        validation_name,
                        endpoint_name,
                        side,
                        elapsed,
                        sql_bytes,
                    )
                # Rule: no statement text, no row values and no traceback at
                # ERROR level. The traceback is available at DEBUG.
                logger.error(
                    "Database query failed | table=%s | validation=%s | "
                    "endpoint=%s | side=%s | query_type=%s | attempt=%s/%s | "
                    "elapsed=%.3fs | connection_wait=%.3fs | sql_bytes=%d | "
                    "error_type=%s | error_class=%s | error=%s",
                    table,
                    validation_name,
                    endpoint_name,
                    side,
                    query_label,
                    attempt,
                    timeout_attempts if is_timeout else attempts,
                    elapsed,
                    timing["connection_wait_seconds"],
                    sql_bytes,
                    type(exc).__name__,
                    (
                        "statement_timeout" if is_timeout
                        else "transient_connection" if is_transient
                        else "database"
                    ),
                    safe_error,
                )
                logger.debug("Database query failure detail", exc_info=True)
                if is_timeout:
                    retryable = (
                        retry_on_timeout and timeout_failures < timeout_attempts
                    )
                else:
                    retryable = attempt < attempts and _is_retryable_database_error(
                        exc, retry_on_timeout
                    )
                if getattr(exc, "retry_exhausted", False) or _deterministic_query_error(exc):
                    retryable = False
                if retryable:
                    self.retry_count += 1
                    logger.warning(
                        "Database query failed; retrying | table=%s | "
                        "validation=%s | endpoint=%s | side=%s | query_type=%s | "
                        "next_attempt=%s | error_type=%s | error_class=%s",
                        table,
                        validation_name,
                        endpoint_name,
                        side,
                        query_label,
                        attempt + 1,
                        type(exc).__name__,
                        "statement_timeout" if is_timeout else "transient_connection",
                    )
                    if is_transient:
                        connection = None
                    self._retry_delay(retry, attempt)
                    continue
                if is_timeout and not retry_on_timeout:
                    logger.info(
                        "Statement timeout is deterministic; not retried | "
                        "table=%s | validation=%s | endpoint=%s | side=%s | "
                        "configured_timeout_seconds=%s "
                        "(performance.query_execution.retry.retry_on_timeout=false)",
                        table, validation_name, endpoint_name, side, configured_timeout,
                    )
                self._record_operation("read_sql_failed", started, sql, None)
                if (is_transient or is_timeout) and not _deterministic_query_error(exc):
                    if getattr(exc, "retry_exhausted", False):
                        raise
                    raise ConnectionUnavailableError(side or self.role, exc, attempt, query_timeout=is_timeout) from exc
                wrapped = wrap_database_error(
                    exc,
                    sql=sql,
                    elapsed_seconds=elapsed,
                    configured_timeout_seconds=configured_timeout,
                    secrets=(
                        getattr(self.cfg, "username", None),
                        getattr(self.cfg, "password", None),
                    ),
                )
                if wrapped is exc:
                    raise
                raise wrapped from exc

    # ── Per-thread query timing ───────────────────────────────
    @property
    def _query_timing(self) -> threading.local:
        store = self.__dict__.get("_query_timing_store")
        if store is None:
            store = threading.local()
            self.__dict__["_query_timing_store"] = store
        return store

    @property
    def last_query_timing(self) -> Optional[Dict[str, Any]]:
        """Timing of the last statement this thread ran through the connector.

        ``connection_wait_seconds`` is the pool checkout / login wait and
        ``db_execution_seconds`` the statement's own execution; workers running
        lookup batches in parallel read their own thread's value.
        """
        timing = getattr(self._query_timing, "last", None)
        return dict(timing) if isinstance(timing, dict) else None

    @staticmethod
    def _effective_statement_timeout(connection: Any) -> Optional[int]:
        """The statement timeout the driver connection actually carries."""
        try:
            raw = getattr(connection, "connection", None)
            dbapi = getattr(raw, "dbapi_connection", raw) if raw is not None else None
            if dbapi is None:
                dbapi = getattr(connection, "dbapi_connection", None)
            value = getattr(dbapi, "timeout", None)
            return int(value) if value is not None else None
        except Exception:  # pragma: no cover - driver specific
            return None


    def _record_operation(
        self, operation: str, started: float, sql: Any, frame: Any
    ) -> None:
        """Record one connector operation in the run's performance ledger."""
        ledger = self.performance_ledger
        if ledger is None:
            return
        try:
            rows = len(frame) if frame is not None and hasattr(frame, "__len__") else None
            ledger.record_operation(
                operation=operation,
                seconds=time.perf_counter() - started,
                system=self.system_name,
                role=self.role,
                rows=rows,
                # Keep the complete read-only statement so the performance
                # report can identify the exact slow batch/query.  Truncating
                # here made different keyset pages indistinguishable.
                statement=str(sql),
            )
        except Exception:  # pragma: no cover - logging must never fail a run
            logger.debug("Performance ledger rejected an operation", exc_info=True)

    def _distributed_pandas_row_cap(self) -> int:
        """Return the configured cap on rows a distributed frame may materialize."""
        spark = getattr(
            getattr(self.runtime_config, "strategies", None), "spark", None
        )
        cap = int(getattr(spark, "max_pandas_rows", 0) or 0)
        if cap:
            return cap
        pandas_strategy = getattr(
            getattr(self.runtime_config, "strategies", None), "pandas", None
        )
        return int(getattr(pandas_strategy, "max_rows_in_memory", 0) or 0)

    def _bounded_to_pandas(self, frame: Any) -> pd.DataFrame:
        """Materialize a distributed frame only while it stays within the cap.

        A large distributed dataset is never collected into one process: the read
        raises so the caller can fall back to a bounded strategy.
        """
        cap = self._distributed_pandas_row_cap()
        if not cap or not hasattr(frame, "limit"):
            return frame.toPandas()
        materialized = frame.limit(cap + 1).toPandas()
        if len(materialized) > cap:
            raise DistributedMaterializationLimitExceeded(
                "Distributed result exceeds the configured Pandas materialization "
                f"limit of {cap} rows; use a bounded execution strategy"
            )
        return materialized

    def read_distributed_frame(self, sql: str) -> Any:
        """Return a native distributed frame when the connector supports it.

        Connectors that cannot be read by the distributed engine raise, which
        lets the strategy resolver fall back to pushdown or batching.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support distributed reads"
        )

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception as exc:
                logger.warning(
                    "Connection close failed: %s",
                    self._masked_connection_error(exc),
                )
            self._conn = None
        if self._engine is not None:
            try:
                self._engine.dispose()
            except Exception as exc:
                logger.warning(
                    "SQLAlchemy engine disposal failed: %s",
                    self._masked_connection_error(exc),
                )
            finally:
                self._engine = None


class _ConnectionBoundQueryExecutor:
    """Connector-compatible executor backed by one leased connection."""

    def __init__(self, owner: BaseConnector, connection: Any) -> None:
        self._owner = owner
        self._connection = connection

    @property
    def capabilities(self):
        return self._owner.capabilities

    def execute_query(self, sql: str) -> pd.DataFrame:
        parameters = tuple(getattr(sql, "params", ()) or ())
        statement = self._owner._ensure_read_only_sql(sql)
        return self._owner._read_sql_dataframe(
            statement, self._connection, params=parameters or None
        )

    def execute_query_batches(self, sql: str, batch_size: int):
        yield from self._owner._resilient_batches(sql, batch_size, self._connection)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._owner, name)


class SQLServerConnector(BaseConnector):
    BULK_SCHEMA_METADATA = True
    # A relational query engine that also exposes a distributed JDBC reader.
    CAPABILITIES = replace(
        RELATIONAL_QUERY_ENGINE_CAPABILITIES,
        spark_read=True,
        partition_read=True,
    )

    def _uses_trusted_connection(self) -> bool:
        configured = getattr(self.cfg, "trusted_connection", None)
        if configured is None or (isinstance(configured, str) and not configured.strip()):
            raise ValueError(
                "SQL Server configuration requires trusted_connection"
            )
        return _parse_boolean(configured, "trusted_connection")

    def _configured_database(self) -> Optional[str]:
        database = self.cfg.database
        if database is None:
            return None
        database = str(database).strip()
        if (
            not database
            or (database.startswith("${") and database.endswith("}"))
            or (database.startswith("{") and database.endswith("}"))
        ):
            return None
        return database

    def _get_conn_str(self) -> str:
        host = str(self.cfg.host or "").strip()
        if not host:
            raise ValueError("SQL Server configuration requires server/host")
        if self.cfg.port and str(self.cfg.port) not in host:
            host = f"{host},{self.cfg.port}"
        driver = get_best_sql_driver(self.cfg.odbc_driver)
        if not driver:
            raise ValueError("SQL Server configuration requires an ODBC driver")
        parts = [f"DRIVER={{{driver}}};", f"SERVER={host};"]
        database = self._configured_database()
        if (
            getattr(self.cfg, "database_required_for_validation", False)
            and not database
        ):
            raise ValueError(
                "SQL Server database is required for table validation context"
            )
        if database:
            parts.append(f"DATABASE={database};")
        if self._uses_trusted_connection():
            parts.append("Trusted_Connection=Yes;")
        else:
            if not self.cfg.username or not self.cfg.password:
                raise ValueError(
                    "SQL Server username and password are required when "
                    "trusted_connection is disabled"
                )
            parts.extend([f"UID={self.cfg.username};", f"PWD={self.cfg.password};"])
        return "".join(parts)

    def _get_jdbc_url(self) -> str:
        jdbc_url = self.cfg.jdbc_url
        database = self._configured_database()
        if database and "databaseName=" not in jdbc_url:
            separator = "" if jdbc_url.endswith(";") else ";"
            return f"{jdbc_url}{separator}databaseName={database}"
        return jdbc_url

    def _get_odbc_conn(self):
        if self._conn is None:
            import pyodbc
            self._conn = pyodbc.connect(
                self._get_conn_str(),
                timeout=self._connection_timeout_seconds(),
            )
            self._apply_query_timeout(self._conn)
        return self._conn

    def read_distributed_frame(self, sql: str) -> Any:
        """Return the distributed JDBC frame for this query without collecting it."""
        sql = self._ensure_read_only_sql(sql)
        session = getattr(self.context, "spark_session", None)
        if session is None:
            raise NotImplementedError(
                "Distributed reads require an active distributed session"
            )
        if self._uses_trusted_connection():
            raise NotImplementedError(
                "Distributed reads require credential-based authentication"
            )
        return (
            session.read.format("jdbc")
            .option("url", self._get_jdbc_url())
            .option("query", sql)
            .option("user", self.cfg.username)
            .option("password", self.cfg.password)
            .option("driver", self.cfg.jdbc_driver)
            .load()
        )

    def execute_query(self, sql: str) -> pd.DataFrame:
        parameters = tuple(getattr(sql, "params", ()) or ())
        sql = self._ensure_read_only_sql(sql)
        if parameters:
            return self._read_sql_dataframe(sql, params=parameters)
        if self.context.engine_type == EngineType.SPARK and self.context.spark_session:
            if self._uses_trusted_connection():
                return self._read_sql_dataframe(sql, params=parameters or None)
            started = time.perf_counter()
            with optional_slot(self.concurrency_governor):
                # Large distributed results are never collected whole: the
                # configured cap keeps this materialization bounded.
                frame = self._bounded_to_pandas(self.read_distributed_frame(sql))
            self._record_operation("distributed_read", started, sql, frame)
            return frame
        return self._read_sql_dataframe(sql, params=parameters or None)

    def test_connection(self) -> bool:
        return self.test_connectivity()

    def test_connectivity(self) -> bool:
        try:
            self.last_error = None
            import pyodbc
            validate_odbc_driver(get_best_sql_driver(self.cfg.odbc_driver))
            connection = pyodbc.connect(
                self._get_conn_str(),
                timeout=self._connection_timeout_seconds(),
            )
            connection.close()
            return True
        except Exception as exc:
            safe_message = self._masked_connection_error(exc)
            self.last_error = RuntimeError(safe_message) if safe_message != str(exc) else exc
            logger.error("SQL Server connection failed: %s", safe_message)
            return False

SqlServerConnector = SQLServerConnector


class FabricConnector(BaseConnector):
    BULK_SCHEMA_METADATA = True
    # A relational query engine reached over its SQL endpoint. No distributed
    # reader is implemented here, so ``spark_read`` stays false and the strategy
    # resolver falls back to pushdown or bounded batching automatically.
    CAPABILITIES = RELATIONAL_QUERY_ENGINE_CAPABILITIES

    def _get_conn_str(self) -> str:
        endpoint = str(
            self.cfg.sql_endpoint or self.cfg.host or self.cfg.jdbc_url or ""
        ).strip()
        if not endpoint:
            raise ValueError("Fabric configuration requires sql_endpoint")
        if not self.cfg.database:
            raise ValueError("Fabric configuration requires database")
        driver = get_best_sql_driver(self.cfg.odbc_driver)
        if not driver:
            raise ValueError("Fabric configuration requires an ODBC driver")
        auth_type = (self.cfg.authentication_type or "").strip().lower()
        parts = [
            f"DRIVER={{{driver}}};",
            f"SERVER={endpoint};",
            f"DATABASE={self.cfg.database};",
            "Encrypt=Yes;",
            "TrustServerCertificate=No;",
        ]
        if auth_type in {"service_principal", "active_directory_service_principal"}:
            if not self.cfg.username or not self.cfg.password:
                raise ValueError("Fabric service principal authentication requires credentials")
            parts.extend([
                f"UID={self.cfg.username};",
                f"PWD={self.cfg.password};",
                "Authentication=ActiveDirectoryServicePrincipal;",
            ])
        elif auth_type in {
            "activedirectorypassword", "active_directory_password", "azure_ad_password"
        }:
            if not self.cfg.username or not self.cfg.password:
                raise ValueError("ActiveDirectoryPassword requires username and password")
            parts.extend([
                f"UID={self.cfg.username};",
                f"PWD={self.cfg.password};",
                "Authentication=ActiveDirectoryPassword;",
            ])
        elif auth_type in {"activedirectoryinteractive", "active_directory_interactive"}:
            parts.append("Authentication=ActiveDirectoryInteractive;")
        else:
            raise ValueError(
                "Unsupported Fabric authentication_type. Use "
                "'ActiveDirectoryInteractive' or 'ActiveDirectoryPassword'."
            )
        return "".join(parts)

    def _get_odbc_conn(self):
        if self._conn is None:
            import pyodbc
            self._conn = pyodbc.connect(
                self._get_conn_str(),
                timeout=self._connection_timeout_seconds(),
            )
            self._apply_query_timeout(self._conn)
        return self._conn

    def execute_query(self, sql: str) -> pd.DataFrame:
        parameters = tuple(getattr(sql, "params", ()) or ())
        sql = self._ensure_read_only_sql(sql)
        # SQLAlchemy checks out one pooled connection per call. No cursor or
        # transaction is shared between validation workers.
        return self._read_sql_dataframe(sql, params=parameters or None)

    def test_connection(self) -> bool:
        return self.test_connectivity()

    def test_connectivity(self) -> bool:
        try:
            self.last_error = None
            import pyodbc
            validate_odbc_driver(get_best_sql_driver(self.cfg.odbc_driver))
            connection = pyodbc.connect(
                self._get_conn_str(),
                timeout=self._connection_timeout_seconds(),
            )
            connection.close()
            return True
        except Exception as exc:
            safe_message = self._masked_connection_error(exc)
            self.last_error = RuntimeError(safe_message) if safe_message != str(exc) else exc
            logger.error("Fabric connection failed: %s", safe_message)
            return False

CONNECTOR_REGISTRY: Dict[str, Type[BaseConnector]] = {}


def register_connector(connector_class: Type[BaseConnector], *connection_types: str) -> None:
    for connection_type in connection_types:
        CONNECTOR_REGISTRY[normalize_connection_type(connection_type)] = connector_class


def resolve_connection_config(
    config: Any,
    connection_context: Optional[Dict[str, Any]] = None,
) -> Any:
    """Normalize a connection configuration and resolve its context placeholders."""
    if isinstance(config, dict):
        from core.config_loader import normalize_connection_config
        config = SimpleNamespace(**normalize_connection_config(config))
    return merge_connection_context(config, connection_context)


def build_connector_from_resolved(
    config: Any, context: ExecutionContext
) -> BaseConnector:
    """Instantiate the registered connector for an already-resolved config."""
    connection_type = normalize_connection_type(getattr(config, "type", ""))
    connector_class = CONNECTOR_REGISTRY.get(connection_type)
    if connector_class is None:
        raise ValueError(f"Unsupported connector type '{connection_type or 'missing'}'")
    return connector_class(config, context=context)


def build_connector(
    config: Any,
    context: ExecutionContext,
    connection_context: Optional[Dict[str, Any]] = None,
) -> BaseConnector:
    return build_connector_from_resolved(
        resolve_connection_config(config, connection_context), context
    )


register_connector(SqlServerConnector, "sqlserver", "sql", "sql_server", "sql-server", "sql server")
register_connector(FabricConnector, "fabric_lakehouse", "fabric")


class ConnectorCache:
    """One established connection per unique connection identity, for a whole run.

    This is the single place the framework establishes connections. Two tables
    that resolve to the same ``connection_type + server + database`` share one
    connector and therefore one pool; a table whose server or database differs
    gets its own. Nothing is disposed until :meth:`close_all` runs at the end of
    the run.
    """

    def __init__(
        self,
        context: ExecutionContext,
        runtime_config: Any = None,
        concurrency_governor: Optional[ConcurrencyGovernor] = None,
        performance_ledger: Any = None,
    ):
        self.context = context
        self.runtime_config = runtime_config
        self.concurrency_governor = concurrency_governor
        self.performance_ledger = performance_ledger
        self._connectors: Dict[Tuple[str, ...], BaseConnector] = {}
        self._identities: Dict[Tuple[str, ...], ConnectionIdentity] = {}
        self._key_locks: Dict[Tuple[str, ...], threading.Lock] = {}
        self._guard = threading.Lock()
        self._established = 0
        self._reused = 0

    def get(
        self,
        config: Any,
        connection_context: Optional[Dict[str, Any]] = None,
        role: Optional[str] = None,
    ) -> BaseConnector:
        """Return the shared connector for this configuration's identity."""
        resolved = resolve_connection_config(config, connection_context)
        identity = connection_identity(resolved)
        key = identity.key
        with self._guard:
            lock = self._key_locks.setdefault(key, threading.Lock())
        # Single-flight: concurrent workers wanting the same identity establish
        # exactly one connection between them.
        with lock:
            existing = self._connectors.get(key)
            if existing is not None:
                with self._guard:
                    self._reused += 1
                return existing
            connector = self._prepare(build_connector_from_resolved(resolved, self.context), role)
            with self._guard:
                self._connectors[key] = connector
                self._identities[key] = identity
                self._established += 1
            logger.info(
                "Established preflight/validation connection (%s); "
                "reused for every table with the same details",
                identity.describe(),
            )
            return connector

    def _prepare(self, connector: BaseConnector, role: Optional[str]) -> BaseConnector:
        """Attach the run's shared runtime services to one connector."""
        connector.runtime_config = self.runtime_config
        connector.concurrency_governor = self.concurrency_governor
        connector.performance_ledger = self.performance_ledger
        if role is not None:
            connector.role = role
        return connector

    @property
    def connectors(self) -> Tuple[BaseConnector, ...]:
        with self._guard:
            return tuple(self._connectors.values())

    def metrics(self) -> Dict[str, Any]:
        """Connection reuse counters for the run report."""
        with self._guard:
            return {
                "connections_established": self._established,
                "connection_reuse_count": self._reused,
                "cached_connection_count": len(self._connectors),
                "cached_connection_identities": [
                    identity.describe() for identity in self._identities.values()
                ],
            }

    def close_all(self) -> None:
        """Dispose every cached connection and pool, once, at end of run."""
        with self._guard:
            connectors = list(self._connectors.values())
            self._connectors.clear()
            self._key_locks.clear()
        for connector in connectors:
            try:
                connector.close()
            except Exception as exc:
                logger.warning("Connector close failed: %s", mask_text(exc))


class ConnectionManager:
    def __init__(
        self,
        config,
        context: ExecutionContext,
        concurrency_governor: Optional[ConcurrencyGovernor] = None,
        performance_ledger: Any = None,
    ):
        self.config = config
        self.context = context
        self.concurrency_governor = concurrency_governor
        self.performance_ledger = performance_ledger
        # One centralized cache keyed on connection identity backs both roles.
        self._cache = ConnectorCache(
            context,
            runtime_config=getattr(config, "performance", None),
            concurrency_governor=concurrency_governor,
            performance_ledger=performance_ledger,
        )

    @property
    def connector_cache(self) -> ConnectorCache:
        return self._cache

    def _prepare(self, connector: BaseConnector, role: str) -> BaseConnector:
        """Attach the run's shared runtime services to one connector."""
        connector.runtime_config = getattr(self.config, "performance", None)
        connector.concurrency_governor = self.concurrency_governor
        connector.performance_ledger = self.performance_ledger
        connector.role = role
        return connector

    @staticmethod
    def _table_connection_context(table_context: Any, role: str) -> Dict[str, Any]:
        if table_context is None:
            return {}
        prefix = "source" if role == "source" else "target"
        return {
            "database": getattr(table_context, f"{prefix}_database", None),
            "schema": getattr(table_context, f"{prefix}_schema", None),
            "table": getattr(table_context, f"{prefix}_table", None),
        }

    def get_source(
        self, source_name: str, fresh: bool = False, table_context: Any = None,
        server_override: Optional[str] = None,
    ) -> BaseConnector:
        source_config = self.config.sources[source_name]
        if server_override is not None:
            host = str(server_override).strip()
            if not host:
                raise ValueError("Source server override must be a non-empty string")
            source_config = (
                replace(source_config, host=host)
                if is_dataclass(source_config)
                else SimpleNamespace(**{**vars(source_config), "host": host})
            )
        connection_context = self._table_connection_context(table_context, "source")
        if fresh:
            return self._prepare(
                build_connector(source_config, self.context, connection_context),
                "source",
            )
        return self._cache.get(source_config, connection_context, "source")

    def get_target(self, fresh: bool = False, table_context: Any = None) -> BaseConnector:
        connection_context = self._table_connection_context(table_context, "target")
        if fresh:
            return self._prepare(
                build_connector(self.config.target, self.context, connection_context),
                "target",
            )
        return self._cache.get(self.config.target, connection_context, "target")

    def metrics(self) -> Dict[str, Any]:
        return self._cache.metrics()

    def close_all(self) -> None:
        self._cache.close_all()
