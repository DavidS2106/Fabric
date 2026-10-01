"""Configured-object metadata dependency failures; never substitute an identity."""
import re
import logging

from .models import CheckResult, ValidationStatus

logger = logging.getLogger(__name__)


def _safe_metadata_error(connection, exc):
    """Expose a useful, credential-masked driver reason in precheck reports."""
    masker = getattr(connection, "_masked_connection_error", None)
    if not callable(masker):
        return type(exc).__name__
    try:
        detail = str(masker(exc)).replace("\r", " ").replace("\n", " ").strip()
    except Exception:
        return type(exc).__name__
    if not detail:
        return type(exc).__name__
    return f"{type(exc).__name__}: {detail[:500]}"


class ObjectDependenciesUnavailable(RuntimeError):
    """Stop dependent scheduling while retaining normal table finalization."""
    def __init__(self, failures):
        super().__init__("Configured object dependencies unavailable")
        self.failures = tuple(failures)


class ObjectDependencyFailure(RuntimeError):
    def __init__(self, role, identity, category, message, *, missing=False):
        super().__init__(message)
        self.role = role
        self.identity = dict(identity)
        self.category = category
        self.missing = missing

    def check(self):
        status = ValidationStatus.FAIL if self.missing else ValidationStatus.CONFIG_ERROR
        return CheckResult(
            check_name=f"{self.role}_table_precheck", status=status, message=str(self),
            details={"exception_category": self.category, "framework_error": False,
                     "configured_object": self.identity, "final_status": status.value},
        )


def object_identity(connection, schema=None, table=None):
    cfg = getattr(connection, "cfg", None)
    return {"server": getattr(cfg, "host", None),
            "database": getattr(cfg, "database", None), "schema": schema, "table": table}


def missing_object(role, identity):
    return ObjectDependencyFailure(
        role, identity, f"{role.upper()}_TABLE_NOT_FOUND",
        f"Configured {role} object does not exist or is inaccessible: "
        f"{identity.get('schema')}.{identity.get('table')}", missing=True,
    )


class ConnectionDependencyFailure(ObjectDependencyFailure):
    """A side's connection stayed unavailable after the centralized retry budget.

    ``root_category`` is the classification of the underlying failure
    (``TRANSIENT_CONNECTION_ERROR`` or ``QUERY_TIMEOUT``) and ``attempts`` the
    number of attempts the configured retry policy allowed before giving up.

    ``query_timeout`` / ``transient`` mirror the same classification under the
    attribute names :func:`core.performance.classify_failure` reads, so a
    statement timeout cached for one consumer is still reported as a statement
    timeout to the next one instead of a generic database error.
    """
    root_category = None
    attempts = None
    query_timeout = False
    transient = False

    def check(self):
        result = super().check()
        result.status = ValidationStatus.SKIP
        result.check_name = f"{self.role}_connection_precheck"
        result.details.update(
            final_status="SKIP", execution_status="NOT_EXECUTED",
            root_category=self.root_category, retry_attempts=self.attempts,
        )
        return result


def connection_dependency(exc, role, identity):
    from .connection_manager import _error_chain, ConnectionUnavailableError, _is_transient_connection_error, _is_query_timeout_error
    for error in _error_chain(exc):
        if isinstance(error, ConnectionDependencyFailure):
            return error
        if isinstance(error, ConnectionUnavailableError) or _is_transient_connection_error(error) or _is_query_timeout_error(error):
            category = f"{role.upper()}_CONNECTION_UNAVAILABLE" if role in ("source", "target") else "DEPENDENCY_UNAVAILABLE"
            failure = ConnectionDependencyFailure(role, identity, category,
                f"Required {role} connection unavailable after centralized query handling")
            failure.__cause__ = exc
            if isinstance(error, ConnectionUnavailableError):
                failure.root_category = error.root_category
                failure.attempts = error.attempts
            else:
                failure.root_category = (
                    "QUERY_TIMEOUT" if _is_query_timeout_error(error) else "TRANSIENT_CONNECTION_ERROR"
                )
            failure.query_timeout = failure.root_category == "QUERY_TIMEOUT"
            failure.transient = not failure.query_timeout
            return failure
    return None


def dependency_failures_for_exception(exc, cache, consumer="validation"):
    """Explain a validator's raised exception through table-local dependency state.

    A validator that lets a post-retry connection failure escape must report the
    same NOT_EXECUTED dependency result as one that consulted the shared cache.
    Failures already recorded for this table (by the dependency-aware connector,
    the shared statistics scan or the object precheck) are authoritative; when
    none is recorded yet and the exception chain itself is a connection failure,
    it is recorded now so the remaining validations of the table reuse it.
    Returns ``None`` for any other exception, which keeps its existing
    framework-error handling.
    """
    recorded = [
        failure for role in ("source", "target")
        if cache is not None and (failure := cache.peek(("object_dependency", role))) is not None
    ]
    if recorded:
        return recorded
    from .connection_manager import _error_chain, ConnectionUnavailableError
    role = None
    for error in _error_chain(exc):
        if isinstance(error, (ObjectDependencyFailure, ConnectionUnavailableError)):
            role = getattr(error, "role", None)
            break
    identity = (cache.peek(("object_identity", role)) if cache is not None and role else None) or {}
    failure = connection_dependency(exc, role if role in ("source", "target") else "dependency", identity)
    if failure is None:
        return None
    if cache is not None and failure.role in ("source", "target"):
        cache_object_failure(cache, failure, consumer=consumer)
    return [failure]


def invalid_object_error(exc):
    """Recognize driver object-not-found errors through existing wrappers."""
    seen = set()
    pending = [exc]
    while pending:
        error = pending.pop()
        if error is None or id(error) in seen:
            continue
        seen.add(id(error))
        if isinstance(error, ObjectDependencyFailure):
            return error.missing
        if re.search(r"invalid object name|\b42S02\b|\b42P01\b", str(error), re.I):
            return True
        pending.extend(getattr(error, attr, None) for attr in ("orig", "original", "__cause__", "__context__"))
    return False


def precheck_object(connection, role, schema, table):
    """Use the same metadata APIs as preflight, on the configured connection."""
    describe = getattr(connection, "get_table_columns", None)
    identity = object_identity(connection, schema, table)
    if not callable(describe):
        return ObjectDependencyFailure(role, identity, "METADATA_DEPENDENCY_UNAVAILABLE",
            f"Configured {role} connector cannot describe the object")
    try:
        connect = getattr(connection, "ensure_connection", None)
        if callable(connect):
            connect()
        current_database = getattr(connection, "get_current_database", None)
        if callable(current_database) and identity["database"] is not None:
            if current_database() != identity["database"]:
                return ObjectDependencyFailure(role, identity, "DATABASE_DEPENDENCY_UNAVAILABLE",
                    f"Connected {role} database does not match the configured database")
        schema_exists = getattr(connection, "schema_exists", None)
        if callable(schema_exists) and not schema_exists(schema):
            return missing_object(role, identity)
        if not describe(schema, table):
            return missing_object(role, identity)
    except MemoryError:
        raise
    except Exception as exc:
        failure = connection_dependency(exc, role, identity)
        if failure is not None:
            return failure
        if invalid_object_error(exc):
            return missing_object(role, identity)
        if isinstance(exc, (AttributeError, IndexError, KeyError, AssertionError, RuntimeError)):
            from .connection_manager import DatabaseQueryError
            if not isinstance(exc, DatabaseQueryError):
                raise
        safe_error = _safe_metadata_error(connection, exc)
        logger.warning(
            "Configured object metadata unavailable | side=%s | error=%s",
            role, safe_error,
        )
        return ObjectDependencyFailure(
            role, identity, "METADATA_DEPENDENCY_UNAVAILABLE",
            f"Configured {role} object metadata unavailable ({safe_error})",
        )
    return None


def cache_object_failure(cache, failure, consumer="table_precheck"):
    cache.put(("object_dependency", failure.role), failure)
    cache.put_failure(("row_count", failure.role), failure, consumer=consumer)
    logger.warning("Object dependency unavailable | side=%s | category=%s | dependent checks not executed",
                   failure.role, failure.category)


def dependency_results(checks, failures, include_roots=True):
    results = [failure.check() for failure in failures] if include_roots else []
    categories = [failure.category for failure in failures]
    reason = "; ".join(str(failure) for failure in failures)
    for name, enabled in checks.items():
        if not enabled:
            continue
        check_name = "zero_record_dataset_check" if name == "edge_case_validation" else name
        results.append(CheckResult(
            check_name=check_name, status=ValidationStatus.SKIP,
            message=f"Not executed: required object dependency unavailable | {reason}",
            details={"execution_status": "NOT_EXECUTED", "exception_category": "DEPENDENCY_UNAVAILABLE",
                     "root_failure_categories": categories, "framework_error": False,
                     "final_status": "SKIP"},
        ))
    return results


class DependencyAwareConnector:
    """Table-local failure propagation around a shared, reusable connector."""
    def __init__(self, connector, role, cache, identity):
        self._connector, self._role, self._cache, self._identity = connector, role, cache, identity

    def __getattr__(self, name):
        if name == "query_session" and callable(getattr(self._connector, name, None)):
            return self._query_session
        return getattr(self._connector, name)

    def _check_dependency(self):
        for role in ("source", "target"):
            failure = self._cache.peek(("object_dependency", role))
            if failure is not None:
                raise failure

    def _record_failure(self, exc):
        failure = connection_dependency(exc, self._role, self._identity)
        if failure is not None:
            cache_object_failure(self._cache, failure, consumer="query_execution")

    def execute_query(self, *args, **kwargs):
        self._check_dependency()
        try:
            return self._connector.execute_query(*args, **kwargs)
        except Exception as exc:
            self._record_failure(exc)
            raise

    def execute_query_batches(self, *args, **kwargs):
        self._check_dependency()
        try:
            yield from self._connector.execute_query_batches(*args, **kwargs)
        except Exception as exc:
            self._record_failure(exc)
            raise

    def _query_session(self):
        import contextlib
        @contextlib.contextmanager
        def session():
            self._check_dependency()
            try:
                with self._connector.query_session() as executor:
                    yield DependencyAwareConnector(executor, self._role, self._cache, self._identity)
            except Exception as exc:
                self._record_failure(exc)
                raise
        return session()
