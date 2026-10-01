"""Startup identity of the running framework copy.

Several copies of this project commonly sit side by side on one machine
(``Migration - v1 - Copy``, ``Migration - Copy (2)``, ...). A report cannot
say which copy produced it, so a run against a pre-fix folder is
indistinguishable from a run against a fixed one. This module resolves the
project root actually on ``sys.path`` and the file backing the active record
comparison validator, so a run can be tied to the code that produced it.

Log-only by design: nothing here is written into a report, because the
resolved paths are user-specific filesystem locations.
"""

from __future__ import annotations

import inspect
import logging
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)

#: Emitted at startup, in this order.
RUNTIME_IDENTITY_FIELDS = (
    "RUNTIME_PROJECT_ROOT",
    "RECORD_COMPARISON_VALIDATOR",
)

_UNRESOLVED = "<unresolved>"


def runtime_identity() -> Dict[str, str]:
    """Resolve the executing project root and the active validator file.

    Never raises: an unimportable validator is reported as unresolved rather
    than taking the run down, because this is a diagnostic.
    """
    identity = {
        "RUNTIME_PROJECT_ROOT": str(Path(__file__).resolve().parent.parent),
        "RECORD_COMPARISON_VALIDATOR": _UNRESOLVED,
    }
    try:
        from validators import RecordComparisonValidator

        identity["RECORD_COMPARISON_VALIDATOR"] = str(
            Path(inspect.getfile(RecordComparisonValidator)).resolve()
        )
    except Exception as exc:  # pragma: no cover - import failure is diagnostic
        identity["RECORD_COMPARISON_VALIDATOR"] = (
            f"{_UNRESOLVED}: {type(exc).__name__}"
        )
    return identity


def log_runtime_identity() -> Dict[str, str]:
    """Log the runtime identity and return it for the caller to print."""
    identity = runtime_identity()
    for field in RUNTIME_IDENTITY_FIELDS:
        logger.info("%s=%s", field, identity[field])
    return identity
