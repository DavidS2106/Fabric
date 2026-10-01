"""Configuration-driven validation registry and factory.

Maps validation configuration keys to validator implementations.
Supports both phase-based (smoke_test, full_load, etc.) and preflight execution.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from validators import (
    AggregateValidator,
    AuditValidator,
    BusinessRuleValidator,
    DataValueValidator,
    DuplicateDetectionValidator,
    EdgeCaseValidator,
    KeyValidationValidator,
    PIIScanner,
    NullValidationValidator,
    RecordComparisonValidator,
    ReferentialIntegrityValidator,
    RowCountValidator,
    SCDValidator,
)


class ValidationRegistry:
    """Maps configuration keys to validator classes."""

    _VALIDATORS = {
        "row_count": RowCountValidator,
        "key_validation": KeyValidationValidator,
        "null_validation": NullValidationValidator,
        "aggregate": AggregateValidator,
        "record_comparison": RecordComparisonValidator,
        "referential": ReferentialIntegrityValidator,
        "scd": SCDValidator,
        "business_rules": BusinessRuleValidator,
        "pii": PIIScanner,
        "duplicate": DuplicateDetectionValidator,
        "data_value_validation": DataValueValidator,
        "edge_case_validation": EdgeCaseValidator,
        "audit_validation": AuditValidator,
    }

    # Validation keys that produce statistics (used for pushdown/caching)
    _STATISTIC_CHECKS = frozenset({
        "row_count",
        "null_validation",
        "aggregate",
        "key_validation",
        "duplicate",
        "pii",
    })

    @classmethod
    def get_validator_class(cls, check_name: str) -> Optional[type]:
        """Return the validator class for a check name, or None if not found."""
        return cls._VALIDATORS.get(check_name)

    @classmethod
    def new_validator(cls, check_name: str) -> Optional[Any]:
        """Create a new validator instance, or None if check_name is unknown."""
        validator_class = cls.get_validator_class(check_name)
        return validator_class() if validator_class else None

    @classmethod
    def is_statistic_check(cls, check_name: str) -> bool:
        """Return whether this check produces statistics."""
        return check_name in cls._STATISTIC_CHECKS

    @classmethod
    def registered_checks(cls) -> List[str]:
        """Return list of all registered check names."""
        return sorted(cls._VALIDATORS.keys())


class ValidationProfileLoader:
    """Loads enabled validations from a configuration profile."""

    def __init__(self, profile: Dict[str, Any]):
        """Initialize from a profile dict (e.g., validation_profiles.smoke_test)."""
        self.profile = profile or {}

    def enabled_checks(self) -> Dict[str, bool]:
        """Extract enabled checks from the profile.

        Returns a dict of {check_name: True/False} for every known check.
        Checks not mentioned in the profile default to False.
        """
        result = {}
        for check_name in ValidationRegistry.registered_checks():
            result[check_name] = self._check_enabled(check_name)
        return result

    def _check_enabled(self, check_name: str) -> bool:
        """Resolve whether a single check is enabled in this profile."""
        # Try direct presence in profile (flat structure)
        if check_name in self.profile:
            value = self.profile[check_name]
            return self._to_bool(value, f"{check_name}")

        # Try mapping to a section name (nested structure)
        section_name = self._section_name(check_name)
        if section_name and section_name in self.profile:
            section = self.profile[section_name]
            if isinstance(section, dict) and check_name in section:
                # Section exists and has this specific check
                value = section[check_name]
                return self._to_bool(value, f"{section_name}.{check_name}")
            # Section exists but doesn't have this check key: default to False
            return False

        return False

    @staticmethod
    def _section_name(check_name: str) -> Optional[str]:
        """Map a check name to its section name in the profile."""
        mapping = {
            "row_count": "data_validation",
            "key_validation": "data_validation",
            "null_validation": "data_validation",
            "duplicate": "data_validation",
            "aggregate": "aggregate_validation",
            "record_comparison": "record_level_comparison",
            "referential": "referential_integrity",
            "scd": "scd_validation",
            "pii": "pii_scan",
            "business_rules": "business_rules",
            "data_value_validation": "data_value_validation",
            "edge_case_validation": "edge_case_validation",
            "audit_validation": "audit_validation",
        }
        return mapping.get(check_name)

    @staticmethod
    def _to_bool(value: Any, path: str) -> bool:
        """Convert a value to boolean, matching the profile schema."""
        if isinstance(value, bool):
            return value
        if isinstance(value, dict) and "enabled" in value:
            enabled = value["enabled"]
            if isinstance(enabled, bool):
                return enabled
            raise ValueError(
                f"{path}.enabled must be a boolean, got {type(enabled).__name__}"
            )
        raise ValueError(
            f"{path} must be a boolean or a dict with an 'enabled' key, "
            f"got {type(value).__name__}: {value!r}"
        )


class PreflightCheckLoader:
    """Loads Preflight-specific schema checks from configuration.

    Preflight has a fixed set of schema/readiness checks:
    - connection_validation
    - database_availability
    - schema_availability
    - table_availability
    - column_availability
    - key_column_availability
    - object_validation

    Data checks (row_count, key_validation, null_validation, duplicate_check,
    aggregate_validation) are never enabled in preflight.
    """

    # Fixed preflight checks (no data scans)
    PREFLIGHT_CHECKS = frozenset({
        "connection_validation",
        "database_availability",
        "schema_availability",
        "table_availability",
        "column_availability",
        "key_column_availability",
        "object_validation",
    })

    # Data checks that are always disabled in preflight
    DATA_CHECKS = frozenset({
        "row_count",
        "key_validation",
        "null_validation",
        "duplicate_check",
        "aggregate_validation",
    })

    def __init__(self, profile: Dict[str, Any]):
        """Initialize from a profile dict."""
        self.profile = profile or {}

    def enabled_checks(self) -> Dict[str, bool]:
        """Extract enabled preflight checks.

        Returns a dict with every preflight check and every data check.
        Data checks always default to False in preflight.
        """
        result = {}

        # Preflight schema checks
        for check_name in self.PREFLIGHT_CHECKS:
            result[check_name] = self._check_enabled(check_name)

        # Data checks always disabled in preflight
        for check_name in self.DATA_CHECKS:
            result[check_name] = False

        return result

    def _check_enabled(self, check_name: str) -> bool:
        """Resolve whether a preflight check is enabled."""
        # Try direct presence
        if check_name in self.profile:
            value = self.profile[check_name]
            return self._to_bool(value, check_name)

        # Preflight has no section mappings; checks are flat
        # object_validation defaults to True (for backward compatibility)
        if check_name == "object_validation":
            return True
        return False

    @staticmethod
    def _to_bool(value: Any, path: str) -> bool:
        """Convert value to boolean."""
        if isinstance(value, bool):
            return value
        if isinstance(value, dict) and "enabled" in value:
            enabled = value["enabled"]
            if isinstance(enabled, bool):
                return enabled
            raise ValueError(
                f"{path}.enabled must be a boolean, got {type(enabled).__name__}"
            )
        raise ValueError(
            f"{path} must be a boolean or a dict with an 'enabled' key, "
            f"got {type(value).__name__}: {value!r}"
        )
