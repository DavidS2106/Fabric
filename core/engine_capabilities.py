"""Run-cached execution-engine availability detection and Spark resolution.

The framework must start and validate without JDK, Java, or PySpark being
installed. This module performs ONE capability probe per run (Java, JAVA_HOME,
PySpark import, Spark initialization permission, Polars, DuckDB), caches the
result, and resolves a Spark session only when the configured policy allows
it. Selecting a fallback engine never changes validation semantics — the same
logical work runs on the configured alternative.
"""

from __future__ import annotations

import logging
import os
import shutil
import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from core.execution_engine import (
    EngineSelectionConfig,
    ENGINE_UNAVAILABLE_ACTIONS,
)

logger = logging.getLogger(__name__)


class SparkUnavailableError(RuntimeError):
    """Raised only when configuration explicitly requires Spark."""


@dataclass(frozen=True)
class EngineAvailability:
    """One immutable snapshot of what this environment can run."""

    java_available: bool = False
    java_home_set: bool = False
    java_executable: Optional[str] = None
    pyspark_available: bool = False
    pyspark_error: Optional[str] = None
    spark_available: bool = False
    spark_unavailable_reason: Optional[str] = None
    polars_available: bool = False
    duckdb_available: bool = False
    selected_fallback: Optional[str] = None

    def as_details(self) -> Dict[str, Any]:
        return {
            "java_available": self.java_available,
            "java_home_set": self.java_home_set,
            "java_executable": self.java_executable,
            "pyspark_available": self.pyspark_available,
            "pyspark_error": self.pyspark_error,
            "spark_available": self.spark_available,
            "spark_unavailable_reason": self.spark_unavailable_reason,
            "polars_available": self.polars_available,
            "duckdb_available": self.duckdb_available,
            "selected_fallback": self.selected_fallback,
        }


def _package_importable(name: str) -> Tuple[bool, Optional[str]]:
    """Whether a package actually imports.

    Resolving alone is not enough: a package may be installed but broken for
    the running interpreter (e.g. an old PySpark on a new Python), in which
    case importing it raises and the engine must count as unavailable.
    """
    try:
        __import__(name)
        return True, None
    except Exception as exc:  # a broken install must never stop the run
        return False, f"{type(exc).__name__}: {exc}"


def _probe_environment(selection: EngineSelectionConfig) -> EngineAvailability:
    """Perform the actual environment checks. Called at most once per run."""
    spark_policy = selection.spark
    checks = spark_policy.availability

    java_home = os.environ.get("JAVA_HOME", "").strip()
    java_home_set = bool(java_home) if checks.check_java_home else False
    java_executable = None
    if checks.check_java:
        java_executable = shutil.which("java")
        if java_executable is None and java_home:
            candidate = os.path.join(java_home, "bin", "java")
            for suffix in ("", ".exe"):
                if os.path.isfile(candidate + suffix):
                    java_executable = candidate + suffix
                    break
    java_available = java_executable is not None

    pyspark_available = False
    pyspark_error = None
    if checks.check_pyspark and spark_policy.enabled:
        pyspark_available, pyspark_error = _package_importable("pyspark")

    spark_available = False
    reason = None
    if not spark_policy.enabled:
        reason = "disabled_by_configuration"
    elif checks.check_pyspark and not pyspark_available:
        reason = "pyspark_missing"
    elif spark_policy.require_java and checks.check_java and not java_available:
        reason = "java_missing"
    else:
        # Initialization itself is attempted lazily by resolve_spark_session;
        # here Spark counts as available for selection purposes.
        spark_available = True

    polars_available, _ = _package_importable("polars")
    duckdb_available, _ = _package_importable("duckdb")

    snapshot = EngineAvailability(
        java_available=java_available,
        java_home_set=java_home_set,
        java_executable=java_executable,
        pyspark_available=pyspark_available,
        pyspark_error=pyspark_error,
        spark_available=spark_available,
        spark_unavailable_reason=reason,
        polars_available=polars_available,
        duckdb_available=duckdb_available,
    )
    fallback = select_fallback_engine(snapshot, selection)
    return EngineAvailability(
        **{**snapshot.as_details(), "selected_fallback": fallback}
    )


class _RunCache:
    """Holds the single per-run availability snapshot."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._snapshot: Optional[EngineAvailability] = None
        self._logged = False

    def get(self, selection: EngineSelectionConfig) -> EngineAvailability:
        if not selection.cache_for_run:
            return _probe_environment(selection)
        with self._lock:
            if self._snapshot is None:
                self._snapshot = _probe_environment(selection)
            return self._snapshot

    def log_once(self, snapshot: EngineAvailability) -> None:
        with self._lock:
            if self._logged:
                return
            self._logged = True
        if snapshot.spark_available:
            logger.info(
                "Engine availability: Spark eligible (java=%s, pyspark=%s); "
                "polars=%s, duckdb=%s",
                snapshot.java_available, snapshot.pyspark_available,
                snapshot.polars_available, snapshot.duckdb_available,
            )
        else:
            logger.info(
                "Spark unavailable: %s. Using configured fallback engine: %s. "
                "Validation semantics remain unchanged.",
                snapshot.spark_unavailable_reason,
                snapshot.selected_fallback,
            )

    def reset(self) -> None:
        with self._lock:
            self._snapshot = None
            self._logged = False


_RUN_CACHE = _RunCache()


def reset_run_cache() -> None:
    """Forget the cached probe. Intended for tests and fresh runs."""
    _RUN_CACHE.reset()


def detect_engine_availability(
    selection: Optional[EngineSelectionConfig] = None,
) -> EngineAvailability:
    """The run's cached engine availability snapshot."""
    selection = selection or EngineSelectionConfig()
    if not selection.availability_check_enabled:
        # Checks disabled: report nothing available beyond the always-present
        # engines, so callers keep the configured non-Spark path.
        return EngineAvailability(
            spark_unavailable_reason="availability_check_disabled",
            selected_fallback=select_fallback_engine(
                EngineAvailability(), selection
            ),
        )
    snapshot = _RUN_CACHE.get(selection)
    _RUN_CACHE.log_once(snapshot)
    return snapshot


def select_fallback_engine(
    availability: EngineAvailability,
    selection: EngineSelectionConfig,
) -> Optional[str]:
    """The first configured non-Spark engine this environment can actually run.

    This selects WHERE work runs; it never selects a different comparison
    logic. ``sql_pushdown``/``odbc`` and ``pandas`` are always runnable;
    ``polars`` and ``duckdb`` require their optional packages.
    """
    if not selection.fallback_enabled:
        return None
    candidates = tuple(selection.when_spark_unavailable) or tuple(
        name for name in selection.priority if name != "spark"
    )
    for name in candidates:
        if name in ("sql_pushdown", "odbc", "pandas"):
            return name
        if name == "polars" and availability.polars_available:
            return name
        if name == "duckdb" and availability.duckdb_available:
            return name
    return None


def resolve_spark_session(
    performance_config: Any,
) -> Tuple[Optional[Any], EngineAvailability]:
    """Resolve one Spark session under the configured availability policy.

    Returns ``(session_or_None, availability)``. Never raises unless the
    configuration explicitly declares Spark required (or an unavailability
    action of ``error``); a plain missing JDK/PySpark simply yields ``None``
    so the configured non-Spark engines carry the identical validations.
    """
    selection = getattr(
        performance_config, "engine_selection", None
    ) or EngineSelectionConfig()
    # This is the run entry point: probe the environment fresh once, then
    # every later lookup in the run reuses the cached snapshot.
    _RUN_CACHE.reset()
    availability = detect_engine_availability(selection)

    if not availability.spark_available:
        reason = availability.spark_unavailable_reason or "unavailable"
        action = selection.spark.action_for(reason)
        if action not in ENGINE_UNAVAILABLE_ACTIONS:
            action = "fallback"
        if action == "error":
            raise SparkUnavailableError(
                f"Spark is required by configuration but unavailable: {reason}"
            )
        return None, availability

    try:
        from pyspark.sql import SparkSession

        session = SparkSession.builder.getOrCreate()
        logger.info("SparkSession initialized")
        return session, availability
    except Exception as exc:
        action = selection.spark.action_for("initialization_failure")
        degraded = EngineAvailability(**{
            **availability.as_details(),
            "spark_available": False,
            "spark_unavailable_reason": f"initialization_failure: {exc}",
            "selected_fallback": select_fallback_engine(availability, selection),
        })
        if action == "error":
            raise SparkUnavailableError(
                f"Spark initialization failed and configuration requires Spark: {exc}"
            ) from exc
        logger.info(
            "Spark unavailable: initialization failed (%s). Using configured "
            "fallback engine: %s. Validation semantics remain unchanged.",
            exc, degraded.selected_fallback,
        )
        return None, degraded
