"""Validated access to comparison behavior configured in validations.yml."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from core.models import ValidationStatus

#: The zero-record conditions a policy can describe, plus ``populated`` for a
#: scope that holds rows on both sides.
ZERO_RECORD_CONDITIONS = ("both_empty", "source_empty", "target_empty")
POPULATED_CONDITION = "populated"


class ValidationPolicyError(ValueError):
    """Raised when a required comparison policy is absent or invalid."""


def _mapping(parent: Dict[str, Any], name: str) -> Dict[str, Any]:
    value = parent.get(name)
    if not isinstance(value, dict):
        raise ValidationPolicyError(
            f"comparison_settings.{name} must be configured in validations.yml"
        )
    return value


@dataclass(frozen=True)
class ValidationPolicies:
    missing_keys: Dict[str, Any]
    nulls: Dict[str, Any]
    timestamps: Dict[str, Any]
    hashing: Dict[str, Any]
    full_comparison: Dict[str, Any]
    errors: Dict[str, Any]
    statuses: Dict[str, Any]

    @classmethod
    def from_config(cls, validations_config: Dict[str, Any]) -> "ValidationPolicies":
        settings = (validations_config or {}).get("comparison_settings")
        if not isinstance(settings, dict):
            raise ValidationPolicyError(
                "comparison_settings must be configured in validations.yml"
            )
        return cls(
            missing_keys=_mapping(settings, "missing_key_behavior"),
            nulls=_mapping(settings, "null_handling"),
            timestamps=_mapping(settings, "timestamp_handling"),
            hashing=_mapping(settings, "hash_comparison"),
            full_comparison=_mapping(settings, "full_data_comparison"),
            errors=_mapping(settings, "error_policy"),
            statuses=_mapping(settings, "status_policy"),
        )

    def configured_status(self, field_name: str) -> ValidationStatus:
        value = self.errors.get(field_name)
        if value is None:
            value = self.missing_keys.get(field_name)
        try:
            return ValidationStatus(str(value))
        except ValueError as exc:
            raise ValidationPolicyError(
                f"Configured status '{value}' for {field_name} is unsupported"
            ) from exc


@dataclass(frozen=True)
class ZeroRecordPolicy:
    """What happens to the remaining validations when a scope holds no rows.

    Every part of this decision — including which validations may still run —
    comes from ``validations.edge_case_validation.zero_record_policy``. When no
    policy is configured, downstream validation stops and *no* validation is
    exempted, so an exemption is always an explicit configuration statement.
    """

    both_empty: str = "stop_downstream"
    source_empty: str = "stop_downstream"
    target_empty: str = "stop_downstream"
    allowed_when_stopped: frozenset = frozenset()
    status_when_stopped: str = "SKIP"
    stopped_message: str = (
        "Skipped by the configured zero-record policy for an empty dataset."
    )
    #: ``{condition: {validation_name: {"status": ..., "message": ...}}}`` from
    #: ``<condition>_validation_overrides``. A validation named here reports the
    #: configured result for that condition instead of running, so an empty
    #: scope that is a valid outcome (both sides empty) is reported as such
    #: rather than being dropped or reported as a failure.
    validation_overrides: Dict[str, Dict[str, Dict[str, str]]] = field(
        default_factory=dict
    )

    @classmethod
    def from_config(cls, validations_config: Dict[str, Any]) -> "ZeroRecordPolicy":
        section = (validations_config or {}).get("edge_case_validation") or {}
        if not isinstance(section, dict):
            return cls()
        raw = section.get("zero_record_policy")
        if not isinstance(raw, dict):
            return cls()
        defaults = cls()
        return cls(
            both_empty=str(raw.get("both_empty", defaults.both_empty)),
            source_empty=str(raw.get("source_empty", defaults.source_empty)),
            target_empty=str(raw.get("target_empty", defaults.target_empty)),
            allowed_when_stopped=frozenset(
                str(item) for item in
                (raw.get("downstream_allowed_when_stopped") or [])
            ),
            status_when_stopped=str(
                raw.get("status_when_stopped", defaults.status_when_stopped)
            ),
            stopped_message=str(
                raw.get("stopped_message", defaults.stopped_message)
            ).strip(),
            validation_overrides=cls._parse_validation_overrides(raw),
        )

    @staticmethod
    def _parse_validation_overrides(
        raw: Dict[str, Any]
    ) -> Dict[str, Dict[str, Dict[str, str]]]:
        """Read ``<condition>_validation_overrides`` from the policy section."""
        overrides: Dict[str, Dict[str, Dict[str, str]]] = {}
        for condition in ZERO_RECORD_CONDITIONS:
            configured = raw.get(f"{condition}_validation_overrides")
            if not isinstance(configured, dict):
                continue
            for validation_name, override in configured.items():
                if not isinstance(override, dict):
                    continue
                status = override.get("status")
                message = override.get("message")
                if not (isinstance(status, str) and status.strip()):
                    continue
                if not (isinstance(message, str) and message.strip()):
                    continue
                overrides.setdefault(condition, {})[str(validation_name)] = {
                    "status": status.strip(),
                    # A folded YAML scalar keeps its line joins and trailing
                    # newline; the reported message is the collapsed sentence.
                    "message": " ".join(message.split()),
                }
        return overrides

    @staticmethod
    def condition_for(source_count: int, target_count: int) -> str:
        """Which zero-record condition one observed pair of counts describes."""
        if source_count == 0 and target_count == 0:
            return "both_empty"
        if source_count == 0:
            return "source_empty"
        if target_count == 0:
            return "target_empty"
        return POPULATED_CONDITION

    def action_for(self, source_count: int, target_count: int) -> str:
        """The configured action for one observed pair of record counts."""
        condition = self.condition_for(source_count, target_count)
        if condition == POPULATED_CONDITION:
            return "continue"
        return getattr(self, condition)

    def validation_override(
        self, condition: Optional[str], validation_name: str
    ) -> Optional[Dict[str, str]]:
        """The configured result for one validation under a zero-record condition.

        ``None`` means the condition declares nothing for that validation, so
        the existing behavior applies unchanged.
        """
        if not condition or condition == POPULATED_CONDITION:
            return None
        override = (self.validation_overrides.get(condition) or {}).get(
            validation_name
        )
        return dict(override) if isinstance(override, dict) else None

    def continues_downstream(self, action: str) -> bool:
        return action in {"continue", "warn_and_continue"}

    def permits(self, action: str, validation_name: str) -> bool:
        """Whether one validation may still run under a resolved action."""
        if self.continues_downstream(action):
            return True
        return validation_name in self.allowed_when_stopped

    def stopped_status(self) -> ValidationStatus:
        try:
            return ValidationStatus(self.status_when_stopped)
        except ValueError as exc:
            raise ValidationPolicyError(
                "Configured zero-record status "
                f"'{self.status_when_stopped}' is unsupported"
            ) from exc

    def as_details(self, action: str) -> Dict[str, Any]:
        return {
            "zero_record_action": action,
            "zero_record_policy_source": "validations.edge_case_validation."
                                         "zero_record_policy",
            "zero_record_allowed_validations": sorted(self.allowed_when_stopped),
        }


def invalid_column_names(exc: Exception) -> tuple[str, ...]:
    """Return column names explicitly identified by a database exception."""
    names = []
    seen_errors = set()
    pending = [exc]
    while pending:
        error = pending.pop()
        if error is None or id(error) in seen_errors:
            continue
        seen_errors.add(id(error))
        text = str(error)
        for name in re.findall(
            r"invalid\s+column\s+name\s+['\"]([^'\"]+)['\"]", text, re.I
        ):
            if name not in names:
                names.append(name)
        pending.extend(
            getattr(error, attr, None)
            for attr in ("orig", "original", "__cause__", "__context__")
        )
    return tuple(names)


def is_invalid_column_error(exc: Exception) -> bool:
    """Recognize generic SQL invalid-column errors without naming columns."""
    if invalid_column_names(exc):
        return True
    text = str(exc)
    return bool(
        re.search(r"\b42S22\b", text, re.I)
        or re.search(r"invalid\s+column\s+name", text, re.I)
    )


def exception_category(exc: Exception) -> str:
    explicit = getattr(exc, "category", None)
    if explicit:
        return str(explicit)
    if isinstance(exc, MemoryError):
        return "MEMORY_LIMIT_EXCEEDED"
    if is_invalid_column_error(exc):
        return "INVALID_CONFIGURED_COLUMN"
    if isinstance(exc, ValidationPolicyError):
        return "CONFIGURATION_POLICY"
    if isinstance(exc, OverflowError):
        return "TIMESTAMP_OVERFLOW"
    if isinstance(exc, (TypeError, ValueError)):
        return "DATATYPE_OR_CONFIGURATION"
    return type(exc).__name__.upper()
