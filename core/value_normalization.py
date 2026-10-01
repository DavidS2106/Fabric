"""Null-safe, range-safe scalar normalization used by validation comparisons."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation, MAX_EMAX, Overflow, localcontext
import re
from typing import Any, Optional

import pandas as pd


def normalize_decimal_for_hash(value: Decimal) -> str:
    """Keep existing decimal formatting, retrying only exponent overflow.

    Retain the caller's precision and rounding so successful hashes do not
    change. The wider exponent limit is local to the failed normalization.
    """
    try:
        normalized = value.normalize()
    except Overflow:
        with localcontext() as context:
            context.Emax = MAX_EMAX
            normalized = value.normalize()
    return format(normalized, "f")


_ISO_TEMPORAL = re.compile(
    r"^\s*(?P<year>[+-]?\d{4,})-(?P<month>\d{2})-(?P<day>\d{2})"
    r"(?:[ T](?P<hour>\d{2}):(?P<minute>\d{2})"
    r"(?::(?P<second>\d{2})(?:\.(?P<fraction>\d+))?)?"
    r"(?P<timezone>Z|[+-]\d{2}:?\d{2})?)?\s*$"
)
_ISO_TIME = re.compile(
    r"^\s*(?P<hour>\d{2}):(?P<minute>\d{2})"
    r"(?::(?P<second>\d{2})(?:\.(?P<fraction>\d+))?)?"
    r"(?P<timezone>Z|[+-]\d{2}:?\d{2})?\s*$"
)


def is_null_scalar(value: Any, *, blank_is_null: bool = False) -> bool:
    """Recognize scalar NULL forms without invoking operations on NaT."""
    if value is None:
        return True
    if blank_is_null and isinstance(value, str) and not value.strip():
        return True
    try:
        result = pd.isna(value)
        return bool(result) if isinstance(result, bool) else bool(result.item())
    except (AttributeError, TypeError, ValueError):
        return False


@dataclass(frozen=True)
class TemporalToken:
    kind: str
    date_component: str
    time_component: str
    fraction: str
    timezone: str

    @property
    def precision(self) -> int:
        return len(self.fraction)

    @property
    def canonical(self) -> str:
        fractional = f".{self.fraction}" if self.fraction else ""
        if self.kind == "date":
            return self.date_component
        if self.kind == "time":
            return f"{self.time_component}{fractional}{self.timezone}"
        return (
            f"{self.date_component}T{self.time_component}"
            f"{fractional}{self.timezone}"
        )


def _token_from_text(value: str) -> Optional[TemporalToken]:
    match = _ISO_TEMPORAL.fullmatch(value)
    kind = "datetime"
    if match is None:
        match = _ISO_TIME.fullmatch(value)
        kind = "time"
    if match is None:
        return None
    parts = match.groupdict()
    timezone = parts.get("timezone") or ""
    if timezone and timezone != "Z" and ":" not in timezone:
        timezone = f"{timezone[:3]}:{timezone[3:]}"
    if kind == "time":
        date_component = ""
    else:
        date_component = (
            f"{parts['year']}-{parts['month']}-{parts['day']}"
        )
        if parts.get("hour") is None:
            return TemporalToken("date", date_component, "", "", "")
    second = parts.get("second") or "00"
    time_component = f"{parts['hour']}:{parts['minute']}:{second}"
    return TemporalToken(
        kind,
        date_component,
        time_component,
        parts.get("fraction") or "",
        timezone,
    )


def temporal_token(value: Any, *, parse_strings: bool = True) -> Optional[TemporalToken]:
    """Return an exact temporal token without datetime64[ns] coercion."""
    if is_null_scalar(value):
        return None
    if isinstance(value, pd.Timestamp):
        return _token_from_text(value.isoformat())
    if isinstance(value, datetime):
        return _token_from_text(value.isoformat())
    if isinstance(value, time):
        return _token_from_text(value.isoformat())
    if isinstance(value, date):
        return _token_from_text(value.isoformat())
    # numpy.datetime64 and driver-specific temporal scalars generally expose an
    # ISO representation through str(), which avoids nanosecond coercion.
    module = type(value).__module__.split(".", 1)[0]
    if module == "numpy" and "datetime64" in type(value).__name__:
        return _token_from_text(str(value))
    if parse_strings and isinstance(value, str):
        return _token_from_text(value)
    return None


def temporal_values_match(
    source: TemporalToken,
    target: TemporalToken,
    *,
    preserve_precision: bool,
    timezone_comparison: str,
    precision_loss_behavior: str = "mismatch",
    fractional_second_tolerance: str = "0",
) -> bool:
    if source.kind != target.kind:
        return False
    if source.date_component != target.date_component:
        return False
    if source.time_component != target.time_component:
        return False
    if preserve_precision:
        if source.fraction != target.fraction:
            return False
        if source.precision != target.precision:
            return False
    elif source.fraction != target.fraction:
        try:
            source_fraction = Decimal(f"0.{source.fraction or '0'}")
            target_fraction = Decimal(f"0.{target.fraction or '0'}")
            tolerance = Decimal(str(fractional_second_tolerance))
        except InvalidOperation:
            return False
        if source_fraction != target_fraction and not (
            precision_loss_behavior == "tolerance"
            and abs(source_fraction - target_fraction) <= tolerance
        ):
            return False
    if timezone_comparison == "preserve" and source.timezone != target.timezone:
        return False
    return True
