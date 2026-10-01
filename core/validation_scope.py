"""
fabric_migration_validator/core/validation_scope.py
───────────────────────────────────────────────────
Resolution of the data window one validation profile is allowed to read.

A profile declares *what kind* of window it validates (``full``,
``historical`` or ``incremental``) and *what to do* when that window cannot be
resolved from table metadata. Nothing here invents a column name, a bound or a
fallback: an incremental profile whose watermark cannot be resolved applies the
configured ``on_unresolved`` action instead of silently reading the whole table.

Every validator, the shared statistics scan and the optional preflight data
checks all render their filter from the same resolved scope, so one table is
never read through two different windows in one phase.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, replace
from typing import Any, Dict, List, Optional

#: Window kinds a profile may declare.
SCOPE_MODES = ("full", "historical", "incremental")

#: Actions available when a declared window cannot be resolved.
UNRESOLVED_ACTIONS = ("error", "warning", "skip_table", "full_scan")

#: Where a watermark bound may be read from. ``table_yaml`` is table
#: configuration; ``runtime_metadata`` is the optional estimate/metadata block.
WATERMARK_SOURCES = ("table_yaml", "runtime_metadata", "checkpoint")


class ValidationScopeError(ValueError):
    """Raised when scope configuration is absent or invalid."""


@dataclass(frozen=True)
class ScopePolicy:
    """One profile's declared window and unresolved-window behavior."""

    mode: str
    on_unresolved: str
    watermark_sources: tuple
    require_both_bounds: bool
    status_on_warning: str
    status_on_error: str
    status_on_skip: str

    @classmethod
    def from_config(
        cls,
        raw: Optional[Dict[str, Any]],
        path: str,
        defaults: Optional["ScopePolicy"] = None,
    ) -> "ScopePolicy":
        base = defaults or _DEFAULT_SENTINEL
        if raw is None:
            if defaults is None:
                raise ValidationScopeError(
                    f"{path} must be configured; validation scope has no implicit default"
                )
            return defaults
        if not isinstance(raw, dict):
            raise ValidationScopeError(f"{path} must be a YAML mapping")
        unknown = sorted(
            set(raw) - {
                "mode", "on_unresolved", "watermark_sources",
                "require_both_bounds", "status_on_warning",
                "status_on_error", "status_on_skip",
            }
        )
        if unknown:
            raise ValidationScopeError(
                f"{path} has unsupported field(s): " + ", ".join(unknown)
            )
        mode = raw.get("mode", base.mode if base is not None else None)
        if mode not in SCOPE_MODES:
            raise ValidationScopeError(
                f"{path}.mode must be one of: " + ", ".join(SCOPE_MODES)
            )
        action = raw.get(
            "on_unresolved", base.on_unresolved if base is not None else None
        )
        if action not in UNRESOLVED_ACTIONS:
            raise ValidationScopeError(
                f"{path}.on_unresolved must be one of: " + ", ".join(UNRESOLVED_ACTIONS)
            )
        sources = raw.get(
            "watermark_sources",
            list(base.watermark_sources) if base is not None else None,
        )
        if not isinstance(sources, (list, tuple)) or not sources:
            raise ValidationScopeError(
                f"{path}.watermark_sources must be a non-empty list"
            )
        unsupported = sorted(set(sources) - set(WATERMARK_SOURCES))
        if unsupported:
            raise ValidationScopeError(
                f"{path}.watermark_sources contains unsupported source(s): "
                + ", ".join(unsupported)
            )
        require_both = raw.get(
            "require_both_bounds",
            base.require_both_bounds if base is not None else None,
        )
        if not isinstance(require_both, bool):
            raise ValidationScopeError(f"{path}.require_both_bounds must be boolean")
        statuses = {}
        for field_name, default_value in (
            ("status_on_warning", base.status_on_warning if base else None),
            ("status_on_error", base.status_on_error if base else None),
            ("status_on_skip", base.status_on_skip if base else None),
        ):
            value = raw.get(field_name, default_value)
            if not isinstance(value, str) or not value.strip():
                raise ValidationScopeError(
                    f"{path}.{field_name} must be a non-empty status name"
                )
            statuses[field_name] = value.strip()
        return cls(
            mode=mode,
            on_unresolved=action,
            watermark_sources=tuple(sources),
            require_both_bounds=require_both,
            **statuses,
        )


_DEFAULT_SENTINEL: Optional[ScopePolicy] = None


@dataclass(frozen=True)
class ValidationScope:
    """The window actually applied to every read of one table in one phase."""

    mode: str
    resolved: bool
    action: Optional[str]
    predicates: tuple = ()
    source_predicates: tuple = ()
    target_predicates: tuple = ()
    source_parameters: tuple = ()
    target_parameters: tuple = ()
    column: Optional[str] = None
    lower_bound: Any = None
    upper_bound: Any = None
    lower_inclusive: bool = False
    upper_inclusive: bool = True
    bound_source: Optional[str] = None
    reason: Optional[str] = None
    policy_status: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def blocks_validation(self) -> bool:
        """Whether the configured action forbids reading the table at all."""
        return self.action in {"error", "skip_table"}

    @property
    def unfiltered_full_scan(self) -> bool:
        """Whether this scope reads both tables without any scope predicate."""
        return not (
            self.predicates or self.source_predicates or self.target_predicates
        )

    @property
    def source_scope_available(self) -> bool:
        """Whether source-side validators have an independent safe selector."""
        return not bool(self.details.get("source_scope_unavailable", False))

    def where_clause(self) -> str:
        if not self.predicates:
            return ""
        return " WHERE " + " AND ".join(self.predicates)

    @property
    def role_specific(self) -> bool:
        """Whether source and target read through different predicates.

        True for every incremental window: the source and target watermark
        (or key, or audit) columns may differ, so no caller may render this
        scope without saying which side it is rendering for.
        """
        return bool(
            self.source_predicates or self.target_predicates
            or self.source_parameters or self.target_parameters
        )

    def _require_role(self, role: Optional[str]) -> None:
        if role in ("source", "target"):
            return
        if self.role_specific:
            raise ValidationScopeError(
                "A role-specific validation scope must be rendered for "
                "role='source' or role='target'; a missing role never defaults "
                f"to the source predicate (got role={role!r})"
            )

    def predicates_for(self, role: Optional[str]) -> tuple:
        self._require_role(role)
        if role == "source":
            return self.source_predicates or self.predicates
        if role == "target":
            return self.target_predicates or self.predicates
        return self.predicates

    def parameters_for(self, role: Optional[str]) -> tuple:
        self._require_role(role)
        if role == "source":
            return self.source_parameters
        if role == "target":
            return self.target_parameters
        return ()

    # ── Incremental policy carried on the scope ───────────────
    @property
    def incremental_policy(self) -> Optional[Dict[str, Any]]:
        """The resolved incremental execution policy, when one was resolved."""
        policy = self.details.get("incremental_policy")
        return dict(policy) if isinstance(policy, dict) else None

    def historical_target_predicates(self) -> tuple:
        """Predicates selecting the target rows loaded before the current window."""
        policy = self.incremental_policy or {}
        return tuple(policy.get("historical_target_predicates") or ())

    def historical_target_parameters(self) -> tuple:
        policy = self.incremental_policy or {}
        return tuple(policy.get("historical_target_parameters") or ())

    def token(self) -> str:
        """Stable identity of this window, for execution-cache keys."""
        payload = json.dumps(
            {
                "mode": self.mode,
                "source_predicates": list(self.predicates_for("source")),
                "target_predicates": list(self.predicates_for("target")),
            },
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    def as_details(self) -> Dict[str, Any]:
        return {
            "validation_scope_mode": self.mode,
            "validation_scope_resolved": self.resolved,
            "validation_scope_action": self.action,
            "validation_scope_reason": self.reason,
            "validation_scope_predicates": list(self.predicates),
            "source_validation_scope_predicates": list(
                self.predicates_for("source")
            ),
            "target_validation_scope_predicates": list(
                self.predicates_for("target")
            ),
            "incremental_scope_applied": bool(
                self.mode == "incremental" and (
                    self.predicates or self.source_predicates or self.target_predicates
                )
            ),
            "incremental_column": self.column,
            "incremental_previous_value": self.lower_bound,
            "incremental_current_value": self.upper_bound,
            "incremental_bound_source": self.bound_source,
            **self.details,
        }

    def to_dict(self) -> Dict[str, Any]:
        """Serializable form carried on a table mapping across processes."""
        return {
            "mode": self.mode,
            "resolved": self.resolved,
            "action": self.action,
            "predicates": list(self.predicates),
            "source_predicates": list(self.source_predicates),
            "target_predicates": list(self.target_predicates),
            "source_parameters": list(self.source_parameters),
            "target_parameters": list(self.target_parameters),
            "column": self.column,
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
            "lower_inclusive": self.lower_inclusive,
            "upper_inclusive": self.upper_inclusive,
            "bound_source": self.bound_source,
            "reason": self.reason,
            "policy_status": self.policy_status,
            "details": dict(self.details),
        }

    @classmethod
    def from_dict(cls, raw: Optional[Dict[str, Any]]) -> Optional["ValidationScope"]:
        if not isinstance(raw, dict) or not raw:
            return None
        payload = dict(raw)
        payload["predicates"] = tuple(payload.get("predicates") or ())
        payload["source_predicates"] = tuple(
            payload.get("source_predicates") or ()
        )
        payload["target_predicates"] = tuple(
            payload.get("target_predicates") or ()
        )
        payload["source_parameters"] = tuple(
            payload.get("source_parameters") or ()
        )
        payload["target_parameters"] = tuple(
            payload.get("target_parameters") or ()
        )
        payload["details"] = dict(payload.get("details") or {})
        return cls(**payload)


class ValidationScopeResolver:
    """Resolve one table's window for one profile, from configuration only."""

    def __init__(self, literal_renderer) -> None:
        #: Callable turning a configured bound into a SQL literal. Supplied by
        #: the caller so quoting stays a dialect concern.
        self._literal = literal_renderer

    def resolve(
        self,
        table_map: Any,
        policy: ScopePolicy,
        base_predicates: Optional[List[str]] = None,
    ) -> ValidationScope:
        predicates = list(base_predicates or [])
        historical = self._historical_predicates(table_map)
        details: Dict[str, Any] = {
            "validation_scope_policy": {
                "mode": policy.mode,
                "on_unresolved": policy.on_unresolved,
                "require_both_bounds": policy.require_both_bounds,
                "watermark_sources": list(policy.watermark_sources),
            }
        }

        if policy.mode == "full":
            return ValidationScope(
                mode=policy.mode,
                resolved=True,
                action=None,
                predicates=tuple(predicates + historical),
                reason=None,
                details=details,
            )

        if policy.mode == "historical":
            if not historical:
                return self._unresolved(
                    policy,
                    "No historical range is configured; set date_filter_column "
                    "and date_range on the table",
                    tuple(predicates),
                    details,
                )
            return ValidationScope(
                mode=policy.mode,
                resolved=True,
                action=None,
                predicates=tuple(predicates + historical),
                details=details,
            )

        # Incremental
        column, bounds, source = self._watermark(table_map, policy)
        if not column:
            return self._unresolved(
                policy,
                "Incremental validation requires a resolvable incremental "
                "column; none is configured for this table",
                tuple(predicates + historical),
                details,
            )
        lower, upper, lower_inclusive, upper_inclusive = bounds
        if lower is None and upper is None:
            return self._unresolved(
                policy,
                f"Incremental column '{column}' has no resolvable watermark bound",
                tuple(predicates + historical),
                details,
                column=column,
            )
        if policy.require_both_bounds and (lower is None or upper is None):
            return self._unresolved(
                policy,
                f"Incremental column '{column}' requires both watermark bounds",
                tuple(predicates + historical),
                details,
                column=column,
                lower=lower,
                upper=upper,
            )
        incremental = []
        if lower is not None:
            operator = ">=" if lower_inclusive else ">"
            incremental.append(f"{column} {operator} {self._literal(lower)}")
        if upper is not None:
            operator = "<=" if upper_inclusive else "<"
            incremental.append(f"{column} {operator} {self._literal(upper)}")
        return ValidationScope(
            mode=policy.mode,
            resolved=True,
            action=None,
            predicates=tuple(predicates + historical + incremental),
            column=column,
            lower_bound=lower,
            upper_bound=upper,
            lower_inclusive=lower_inclusive,
            upper_inclusive=upper_inclusive,
            bound_source=source,
            details=details,
        )

    # ── Internals ─────────────────────────────────────────────
    def _unresolved(
        self,
        policy: ScopePolicy,
        reason: str,
        predicates: tuple,
        details: Dict[str, Any],
        column: Optional[str] = None,
        lower: Any = None,
        upper: Any = None,
    ) -> ValidationScope:
        status = {
            "error": policy.status_on_error,
            "skip_table": policy.status_on_skip,
            "warning": policy.status_on_warning,
            "full_scan": policy.status_on_warning,
        }[policy.on_unresolved]
        return ValidationScope(
            mode=policy.mode,
            resolved=False,
            action=policy.on_unresolved,
            predicates=predicates,
            column=column,
            lower_bound=lower,
            upper_bound=upper,
            reason=reason,
            policy_status=status,
            details=details,
        )

    def _historical_predicates(self, table_map: Any) -> List[str]:
        column = getattr(table_map, "date_filter_column", None)
        date_range = getattr(table_map, "date_range", None) or {}
        if not column or not isinstance(date_range, dict):
            return []
        start = date_range.get("start")
        end = date_range.get("end")
        if start is None or end is None:
            return []
        return [
            f"{column} BETWEEN {self._literal(start)} AND {self._literal(end)}"
        ]

    @staticmethod
    def _watermark(table_map: Any, policy: ScopePolicy):
        column = getattr(table_map, "incremental_column", None)
        column = str(column).strip() if column else None
        if not column:
            return None, (None, None, False, True), None

        source = None
        lower = upper = None
        if "table_yaml" in policy.watermark_sources:
            previous = getattr(table_map, "incremental_previous_value", None)
            current = getattr(table_map, "incremental_current_value", None)
            legacy = getattr(table_map, "incremental_value", None)
            if previous is None and current is None and legacy is not None:
                lower = legacy
                source = "table_yaml"
            else:
                lower, upper = previous, current
                if lower is not None or upper is not None:
                    source = "table_yaml"
        if (
            source is None
            and "runtime_metadata" in policy.watermark_sources
        ):
            metadata = getattr(table_map, "incremental_metadata", None) or {}
            if isinstance(metadata, dict):
                lower = metadata.get("previous_watermark")
                upper = metadata.get("current_watermark")
                if lower is not None or upper is not None:
                    source = "runtime_metadata"
        return (
            column,
            (
                lower,
                upper,
                bool(getattr(table_map, "incremental_lower_bound_inclusive", False)),
                bool(getattr(table_map, "incremental_upper_bound_inclusive", True)),
            ),
            source,
        )


class IncrementalStrategyResolver:
    """Resolve one table's configured incremental strategy and independent scope.

    The resolver produces the single effective incremental execution policy
    for a table: the generic configuration (``validations.incremental_strategy``)
    answers which strategies, operations, target behavior and fallbacks are
    allowed; the table metadata answers what selectors (keys, watermark, CDC,
    target audit column) this table actually has. Nothing is inferred from a
    column name, an empty placeholder or a maximum timestamp.
    """

    STRATEGIES = ("cdc", "watermark", "primary_key", "target_audit", "full_hash")
    #: Strategies with a current/historical target boundary; only these can
    #: classify per-record operations. A full-hash snapshot has no boundary.
    CLASSIFIABLE_STRATEGIES = ("cdc", "watermark", "primary_key", "target_audit")
    #: CDC target delete behaviors the framework can verify today. Any other
    #: configured behavior is reported as unsupported rather than guessed.
    IMPLEMENTED_DELETE_BEHAVIORS = ("hard_delete", "soft_delete")

    def __init__(self, literal_renderer, audit_boundary_reader=None) -> None:
        self._literal = literal_renderer
        self._audit_boundary_reader = audit_boundary_reader

    def resolve(
        self,
        table_map: Any,
        policy: ScopePolicy,
        common: Dict[str, Any],
        checkpoint: Optional[Dict[str, Any]] = None,
    ) -> ValidationScope:
        if policy.mode != "incremental":
            return ValidationScopeResolver(self._literal).resolve(table_map, policy)
        common = dict(common or {})
        if not common.get("enabled", True):
            return self._error(
                policy, "Incremental strategy is disabled", common=common
            )

        strategy_config = dict(common.get("strategy") or {})
        requested = (
            getattr(table_map, "incremental_strategy", None)
            or strategy_config.get("mode", "auto")
        )
        try:
            source_column, target_column = self._watermark_columns(table_map)
            cdc = self._table_cdc(table_map)
            if cdc and cdc["operation_values"].get("delete"):
                behavior = (common.get("cdc") or {}).get("delete_behavior")
                if behavior not in self.IMPLEMENTED_DELETE_BEHAVIORS:
                    raise ValidationScopeError("Configured CDC DELETE requires a supported target delete_behavior")
                if behavior == "soft_delete" and not (common.get("cdc") or {}).get("soft_delete_column"):
                    raise ValidationScopeError("Configured CDC soft_delete requires soft_delete_column")
            audit = self._table_audit(table_map, common) if not source_column else None
        except ValidationScopeError as exc:
            # Invalid metadata is a configuration error, never "not configured":
            # treating `[]` as no watermark would silently change the strategy.
            return self._attach_policy(
                self._error(policy, str(exc), common=common),
                table_map, common, {}, requested, None, None, None, None,
            )
        capabilities = self._capabilities(
            table_map, common, source_column, target_column, cdc, audit
        )

        auto_reason = None
        if requested == "auto":
            selected, auto_reason = self._auto_strategy(
                table_map, common, strategy_config, capabilities
            )
        else:
            selected = requested

        if selected == "cdc":
            scope = self._cdc_scope(
                table_map, policy, common, checkpoint,
                source_column, target_column, cdc, audit, capabilities,
            )
        elif selected == "watermark":
            scope = self._watermark_scope(
                table_map, policy, common, checkpoint,
                source_column, target_column,
            )
        elif selected == "primary_key":
            scope = self._primary_key_scope(table_map, policy, common, checkpoint)
        elif selected == "target_audit":
            scope = self._target_audit_scope(
                table_map, policy, common, checkpoint, audit
            )
        elif selected == "full_hash":
            scope = self._full_hash_scope(table_map, policy, common, requested)
        elif selected in ("error", "skip_table"):
            scope = self._error(
                policy,
                "No incremental selector is configured for this table (no CDC "
                "metadata, no watermark, no usable key checkpoint, no target "
                f"audit window) and the configured fallback is '{selected}'",
                common=common,
                action=selected,
            )
        else:
            scope = self._error(
                policy, f"Unsupported incremental strategy '{selected}'",
                common=common,
            )
        return self._attach_policy(
            scope, table_map, common, capabilities, requested, selected,
            auto_reason, cdc, audit,
        )

    # ── Capability detection (configuration only) ─────────────
    @staticmethod
    def _column_name(raw: Any, field: str) -> Optional[str]:
        """A configured column name, ``None`` when absent, error otherwise."""
        if raw is None:
            return None
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
        raise ValidationScopeError(
            f"{field} must be a non-empty column name or omitted; got {raw!r}"
        )

    def _watermark_columns(self, table_map):
        legacy = self._column_name(
            getattr(table_map, "incremental_column", None), "incremental_column"
        )
        source = self._column_name(
            getattr(table_map, "source_watermark_column", None),
            "source_watermark_column",
        )
        target = self._column_name(
            getattr(table_map, "target_watermark_column", None),
            "target_watermark_column",
        )
        return source or legacy, target or legacy

    def _table_cdc(self, table_map) -> Optional[Dict[str, Any]]:
        """The table's explicit CDC metadata, validated; never inferred."""
        raw = getattr(table_map, "incremental_cdc", None)
        if raw is None:
            return None
        if not isinstance(raw, dict):
            raise ValidationScopeError("incremental.cdc must be a mapping")
        cdc = dict(raw)
        if cdc.get("enabled", True) is False:
            return None
        cdc["enabled"] = True
        cdc["operation_column"] = self._column_name(
            cdc.get("operation_column"), "incremental.cdc.operation_column"
        )
        if cdc["operation_column"] is None:
            raise ValidationScopeError(
                "incremental.cdc.operation_column must be configured for CDC"
            )
        values = cdc.get("operation_values")
        if not isinstance(values, dict) or not any(
            values.get(kind) for kind in ("insert", "update", "delete")
        ):
            raise ValidationScopeError(
                "incremental.cdc.operation_values must configure the insert, "
                "update and/or delete operation values"
            )
        cdc["operation_values"] = {
            kind: [str(item).strip() for item in (
                configured if isinstance(configured, (list, tuple)) else [configured]
            )]
            for kind, configured in values.items() if configured
        }
        cdc["sequence_column"] = self._column_name(
            cdc.get("sequence_column"), "incremental.cdc.sequence_column"
        )
        return cdc

    def _table_audit(self, table_map, common) -> Optional[Dict[str, Any]]:
        raw = common.get("target_audit")
        if raw is None:
            return None
        if not isinstance(raw, dict):
            raise ValidationScopeError("validations.incremental_strategy.target_audit must be a mapping")
        if not raw.get("enabled", False):
            return None
        if getattr(table_map, "incremental_target_audit", None):
            raise ValidationScopeError(
                "Configure target audit only in validations.incremental_strategy.target_audit"
            )
        audit = dict(raw)
        audit["load_timestamp_column"] = self._column_name(
            audit.get("load_timestamp_column"),
            "incremental.target_audit.load_timestamp_column",
        )
        if audit["load_timestamp_column"] is None:
            raise ValidationScopeError(
                "validations.incremental_strategy.target_audit.load_timestamp_column must be configured"
            )
        return audit

    def _valid_key_mappings(self, table_map) -> List[Dict[str, str]]:
        mappings = []
        for mapping in getattr(table_map, "key_mappings", None) or ():
            try:
                source = self._mapping_value(mapping, "source_column")
                target = self._mapping_value(mapping, "target_column")
            except AttributeError:
                continue
            if all(isinstance(item, str) and item.strip() for item in (source, target)):
                mappings.append({"source_column": source.strip(), "target_column": target.strip()})
        return mappings

    def _capabilities(
        self, table_map, common, source_column, target_column, cdc, audit,
    ) -> Dict[str, Any]:
        keys = self._valid_key_mappings(table_map)
        return {
            "pk": bool(keys),
            "key_columns": {
                "source": [item["source_column"] for item in keys],
                "target": [item["target_column"] for item in keys],
            },
            "watermark": bool(source_column and target_column),
            "watermark_declared": bool(source_column or target_column),
            "cdc": bool(cdc),
            "target_audit": bool(audit),
            "key_checkpoint_strategy": self._usable_key(table_map, common),
        }

    def _auto_strategy(
        self, table_map, common, strategy_config, capabilities,
    ):
        """Pick this table's strategy from the configured priority order.

        ``strategy.priority`` is walked in order and the first entry this table
        can actually satisfy wins, so the order is configuration rather than a
        hard-coded chain. A capability counts only when its configured value is
        valid; a table that satisfies nothing in the list takes the configured
        no-selector fallback, which a table may override.
        """
        priority = [
            str(item) for item in (strategy_config.get("priority") or ())
        ] or list(self.STRATEGIES)
        fallback = dict(common.get("fallback") or {})
        override = getattr(table_map, "incremental_fallback", None)
        # With no source watermark, the configured target audit window owns
        # the cohort. CDC metadata remains available for classification of the
        # corresponding source rows, but must not reverse scope ownership.
        if (
            not capabilities.get("watermark_declared")
            and capabilities.get("target_audit")
            and "target_audit" in priority
        ):
            return "target_audit", "auto:no_watermark_target_audit_configured"
        for candidate in priority:
            if candidate == "cdc" and capabilities.get("cdc"):
                return "cdc", "auto:cdc_metadata_configured"
            # A partially declared watermark still selects the watermark
            # strategy so the incomplete configuration is reported, never
            # silently replaced by another strategy.
            if candidate == "watermark" and capabilities.get("watermark_declared"):
                return "watermark", "auto:watermark_configured"
            if candidate == "primary_key" and capabilities.get("key_checkpoint_strategy"):
                return "primary_key", "auto:usable_primary_key_checkpoint"
            if candidate == "target_audit" and capabilities.get("target_audit"):
                return "target_audit", "auto:target_audit_configured"
            if candidate == "full_hash":
                # No selector satisfied an earlier entry: the configured
                # no-selector fallback decides (full_hash, target_audit,
                # error or skip_table), never an implicit snapshot.
                return (
                    override or fallback.get("no_watermark_no_pk", "full_hash"),
                    "auto:no_incremental_selector",
                )
        return (
            override or fallback.get("no_watermark_no_pk", "full_hash"),
            "auto:configured_no_selector_fallback",
        )

    @staticmethod
    def _unresolved_action(policy: ScopePolicy, common: Dict[str, Any]) -> tuple:
        """Resolve what an unresolvable incremental window does, and its status.

        A window that cannot be resolved never silently widens into a full
        table scan. ``full_scan`` is honored only when the common configuration
        explicitly allows an implicit full-table fallback; every other action —
        including ``warning`` — stops this table's data reads and reports the
        configured status, so a failed watermark resolution can never be
        mistaken for a validated incremental window.
        """
        safety = dict(common.get("safety") or {})
        watermark = dict(common.get("watermark") or {})
        action = (
            safety.get("unresolved_watermark_action")
            or watermark.get("on_unresolved")
            or policy.on_unresolved
            or "error"
        )
        if action == "full_scan" and not safety.get(
            "allow_implicit_full_table_fallback", False
        ):
            action = "error"
        if action == "warning":
            # Warn, but still refuse to read the table unfiltered.
            return "skip_table", policy.status_on_warning
        if action == "skip_table":
            return "skip_table", policy.status_on_skip
        if action == "full_scan":
            return "full_scan", policy.status_on_warning
        return "error", policy.status_on_error

    def _watermark_scope(
        self, table_map, policy, common, checkpoint, source_column, target_column,
        bootstrap_mode: Optional[str] = None,
    ) -> ValidationScope:
        if not source_column or not target_column:
            return self._error(
                policy,
                "Watermark strategy requires source_watermark_column and "
                "target_watermark_column",
                common=common,
            )
        watermark = dict(common.get("watermark") or {})
        mode = (
            getattr(table_map, "incremental_watermark_mode", None)
            or watermark.get("mode", "hybrid")
        )
        sources = list(watermark.get("resolution_sources") or ())
        previous, previous_source = self._resolve_bound(
            "previous", mode, table_map, watermark, checkpoint, sources
        )
        current, current_source = self._resolve_bound(
            "current", mode, table_map, watermark, checkpoint, sources
        )
        bootstrap = bootstrap_mode or self._bootstrap_mode(common, "watermark")
        bootstrap_used = False
        bound_details = {
            **self._base_details(table_map, "watermark"),
            "watermark_mode": mode,
            "source_watermark_column": source_column,
            "target_watermark_column": target_column,
            "previous_watermark": previous,
            "current_watermark": current,
            "previous_watermark_source": previous_source,
            "current_watermark_source": current_source,
            "bootstrap_policy": bootstrap,
        }
        if current is None:
            return self._error(
                policy,
                f"{mode} watermark mode could not resolve the current watermark "
                "bound (table_yaml / runtime_metadata); a checkpoint only "
                "supplies the previous bound",
                column=source_column, details=bound_details, common=common,
            )
        same_boundary = False
        if previous is None:
            bootstrap_used = True
            if bootstrap == "open_lower_bound":
                # Preserve the existing initial-load behavior: validate all
                # rows up to the current watermark without a lower predicate.
                previous_source = "bootstrap:open_lower_bound"
            else:
                # With no committed lower bound on the first run, use the
                # current watermark as the starting checkpoint. Subsequent
                # runs still use the configured/checkpoint resolution chain.
                previous = current
                previous_source = "first_run:current_watermark"
                bound_details.update({
                    "previous_watermark": previous,
                    "previous_watermark_source": previous_source,
                })
                same_boundary = True
        else:
            try:
                invalid_order = self._invalid_boundary_order(previous, current)
                same_boundary = (
                    invalid_order
                    and self._invalid_boundary_order(current, previous)
                )
            except (TypeError, ValueError):
                return self._error(policy, "Watermark boundaries have incompatible types or timezones", common=common)
            if invalid_order and not same_boundary:
                return self._error(
                    policy, "current watermark is earlier than previous watermark",
                    details=bound_details, common=common,
                )
        filter_config = dict(watermark.get("filter") or {})
        lower_operator = filter_config.get("lower_operator", ">")
        upper_operator = filter_config.get("upper_operator", "<=")
        parameterized = bool(filter_config.get("parameterized", True))
        upper_value = "?" if parameterized else self._literal(current)
        source_predicates: List[str] = []
        target_predicates: List[str] = []
        parameters: List[Any] = []
        historical_predicates: List[str] = []
        historical_parameters: List[Any] = []
        if same_boundary:
            equality_value = "?" if parameterized else self._literal(current)
            source_predicates.append(f"{source_column} = {equality_value}")
            target_predicates.append(f"{target_column} = {equality_value}")
            parameters.append(current)
            # Equal-watermark rows belong to the current incremental dataset;
            # only strictly earlier target rows are historical.
            historical_predicates.append(f"{target_column} < {equality_value}")
            if parameterized:
                historical_parameters.append(current)
        elif previous is not None:
            lower_value = "?" if parameterized else self._literal(previous)
            source_predicates.append(f"{source_column} {lower_operator} {lower_value}")
            target_predicates.append(f"{target_column} {lower_operator} {lower_value}")
            parameters.append(previous)
            # Everything the target loaded at or before the previous watermark
            # is history: the exact complement of the window's lower bound.
            historical_predicates.append(
                f"{target_column} {self._complement(lower_operator)} {lower_value}"
            )
            if parameterized:
                historical_parameters.append(previous)
        if not same_boundary:
            source_predicates.append(f"{source_column} {upper_operator} {upper_value}")
            target_predicates.append(f"{target_column} {upper_operator} {upper_value}")
            parameters.append(current)
        details = {
            **bound_details,
            "previous_watermark_source": previous_source,
            "scope_type": "incremental_window",
            "full_table_strategy_used": False,
            "parameterized": parameterized,
            "bootstrap_used": bootstrap_used,
            "same_watermark_scope": same_boundary,
            "comparison_direction": "source_to_target",
            "_historical_predicates": historical_predicates,
            "_historical_parameters": historical_parameters,
        }
        return ValidationScope(
            mode="incremental", resolved=True, action=None,
            predicates=tuple(source_predicates),
            source_predicates=tuple(source_predicates),
            target_predicates=tuple(target_predicates),
            source_parameters=tuple(parameters) if parameterized else (),
            target_parameters=tuple(parameters) if parameterized else (),
            column=source_column, lower_bound=previous, upper_bound=current,
            lower_inclusive=lower_operator == ">=",
            upper_inclusive=upper_operator == "<=",
            bound_source=(
                previous_source if previous_source == current_source else "mixed"
            ),
            details=details,
        )

    @staticmethod
    def _complement(lower_operator: str) -> str:
        """The operator selecting everything the window's lower bound excludes."""
        return "<=" if lower_operator == ">" else "<"

    @staticmethod
    def _bootstrap_mode(common: Dict[str, Any], strategy: str) -> str:
        bootstrap = dict(common.get("bootstrap") or {})
        default = (
            "require_checkpoint" if strategy in ("primary_key", "cdc")
            else (
                "open_lower_bound"
                if strategy == "target_audit"
                else "require_previous_bound"
            )
        )
        return str(bootstrap.get(strategy) or default)

    def _primary_key_scope(
        self, table_map, policy, common, checkpoint,
        bootstrap_mode: Optional[str] = None,
    ):
        if not self._usable_key(table_map, common):
            action = (
                getattr(table_map, "incremental_fallback", None)
                or (common.get("primary_key") or {}).get(
                    "unsuitable_key_action", "full_hash"
                )
            )
            if action == "full_hash":
                full_hash = dict((common.get("fallback") or {}).get("full_hash") or {})
                return ValidationScope(
                    mode="incremental", resolved=True, action=None,
                    details={
                        **self._base_details(table_map, "full_hash"),
                        "scope_type": "full_table",
                        "full_table_strategy_used": True,
                        "full_table_strategy_reason": "explicit_unsuitable_primary_key",
                        "preserve_key_mappings": bool(
                            full_hash.get("preserve_key_mappings", True)
                        ),
                    },
                )
            return self._error(
                policy, "Configured primary key is not incremental-compatible",
                common=common,
            )
        mappings = list(getattr(table_map, "key_mappings", None) or ())
        source_key = self._mapping_value(mappings[0], "source_column")
        target_key = self._mapping_value(mappings[0], "target_column")
        key_checkpoint, checkpoint_source = None, None
        if checkpoint and checkpoint.get("last_processed_key") is not None:
            key_checkpoint, checkpoint_source = checkpoint["last_processed_key"], "checkpoint"
        metadata = getattr(table_map, "incremental_metadata", None) or {}
        if key_checkpoint is None and isinstance(metadata, dict):
            if metadata.get("last_processed_key") is not None:
                key_checkpoint = metadata["last_processed_key"]
                checkpoint_source = "runtime_metadata"
        bootstrap = bootstrap_mode or self._bootstrap_mode(common, "primary_key")
        bootstrap_used = False
        base = {
            **self._base_details(table_map, "primary_key"),
            "source_key_column": source_key,
            "target_key_column": target_key,
            "bootstrap_policy": bootstrap,
        }
        if key_checkpoint is None:
            if bootstrap != "open_lower_bound":
                return self._error(
                    policy,
                    "Primary-key incremental strategy requires a resolved key "
                    "checkpoint: no committed checkpoint and no runtime "
                    f"last_processed_key (bootstrap policy '{bootstrap}')",
                    details=base, common=common,
                )
            bootstrap_used = True
            checkpoint_source = "bootstrap:open_lower_bound"
        if isinstance(key_checkpoint, (list, tuple)):
            # A composite checkpoint is stored as a list; the key-checkpoint
            # strategy is limited to one configured key column.
            key_checkpoint = key_checkpoint[0] if key_checkpoint else None
        source_predicates = (f"{source_key} > ?",) if key_checkpoint is not None else ()
        target_predicates = (f"{target_key} > ?",) if key_checkpoint is not None else ()
        parameters = (key_checkpoint,) if key_checkpoint is not None else ()
        details = {
            **base,
            "scope_type": "key_checkpoint",
            "key_checkpoint": key_checkpoint,
            "key_checkpoint_source": checkpoint_source,
            "full_table_strategy_used": False,
            "bootstrap_used": bootstrap_used,
            "primary_key_limitation": (
                "Key checkpoint mode detects newly appended keys only; it is not CDC "
                "and does not detect updates to older keys."
            ),
            "_historical_predicates": (
                [f"{target_key} <= ?"] if key_checkpoint is not None else []
            ),
            "_historical_parameters": list(parameters),
        }
        return ValidationScope(
            mode="incremental", resolved=True, action=None,
            predicates=source_predicates,
            source_predicates=source_predicates,
            target_predicates=target_predicates,
            source_parameters=parameters,
            target_parameters=parameters,
            column=source_key, lower_bound=key_checkpoint,
            bound_source=checkpoint_source, details=details,
        )

    def _target_audit_scope(self, table_map, policy, common, checkpoint, audit,
                            bootstrap_mode: Optional[str] = None):
        """Current target load window on the configured audit/load column.

        The source has no selector in this mode, so the comparison is driven by
        the current target rows: every current row is looked up in the source
        and in the historical target scope, while source rows outside the
        current load are simply not part of this window. The boundary values
        come from configuration, runtime metadata or the committed checkpoint;
        ``MAX(load column)`` is never assumed to be one complete load.
        """
        if not audit:
            return self._error(
                policy,
                "Target audit strategy requires incremental.target_audit."
                "load_timestamp_column",
                common=common,
            )
        column = audit["load_timestamp_column"]
        sources = list((common.get("target_audit") or {}).get("resolution_sources") or ())
        audit = dict(audit)
        batch = audit.get("batch_window_resolution") or {}
        # ``enabled`` is the explicit opt-in for target-side batch discovery.
        # ConfigLoader normally appends this source during normalization, but
        # keep the resolver correct for programmatic/test configurations too.
        if batch.get("enabled") and "audit_values" not in sources:
            sources.append("audit_values")
        if "audit_values" in sources:
            if not batch.get("enabled") or batch.get("strategy") != "latest_two_distinct_values":
                return self._error(policy, "audit_values requires explicitly enabled latest_two_distinct_values", common=common)
            if audit.get("semantics") != "batch_value":
                return self._error(
                    policy,
                    "Configured target audit column does not safely identify "
                    "complete ETL batches; target_audit.semantics must be batch_value",
                    common=common,
                )
            earlier = sources[:sources.index("audit_values")]
            if any(self._resolve_audit_bound(name, table_map, audit, checkpoint, earlier)[0] is None
                   for name in ("previous", "current")):
                if self._audit_boundary_reader is None:
                    return self._error(policy, "Target audit boundary reader is unavailable", common=common)
                values = self._audit_boundary_reader(table_map, column)
                audit["_batch_values"] = {
                    "current": values[0] if values else None,
                    "previous": values[1] if len(values) > 1 else None,
                }
        previous, previous_source = self._resolve_audit_bound(
            "previous", table_map, audit, checkpoint, sources
        )
        current, current_source = self._resolve_audit_bound(
            "current", table_map, audit, checkpoint, sources
        )
        table_bootstrap = getattr(table_map, "incremental_bootstrap", None) or {}
        bootstrap = (
            bootstrap_mode
            or table_bootstrap.get("target_audit")
            or self._bootstrap_mode(common, "target_audit")
        )
        base = {
            **self._base_details(table_map, "target_audit"),
            "target_audit_column": column,
            "previous_load_boundary": previous,
            "current_load_boundary": current,
            "previous_load_boundary_source": previous_source,
            "current_load_boundary_source": current_source,
            "bootstrap_policy": bootstrap,
        }
        if current is None:
            return self._error(
                policy,
                "Target audit window requires a resolvable current load boundary "
                "(table_yaml current_load_value, runtime_metadata "
                "current_load_boundary, or explicitly enabled audit_values "
                "latest_two_distinct_values); MAX(load timestamp) is never assumed",
                column=column, details=base, common=common,
            )
        bootstrap_used = False
        if previous is None:
            if bootstrap != "open_lower_bound":
                return self._error(
                    policy,
                    "Target audit window requires a resolvable previous load "
                    "boundary: no committed checkpoint and no configured value "
                    f"(bootstrap policy '{bootstrap}')",
                    column=column, details=base, common=common,
                )
            bootstrap_used = True
            previous_source = "bootstrap:open_lower_bound"
        else:
            try:
                invalid_order = self._invalid_boundary_order(previous, current)
                same_boundary = (
                    invalid_order
                    and self._invalid_boundary_order(current, previous)
                )
            except (TypeError, ValueError):
                return self._error(policy, "Load boundaries have incompatible types or timezones", common=common)
            if same_boundary and previous_source == "checkpoint":
                return self._error(
                    policy,
                    "No new target-audit load found after committed checkpoint.",
                    column=column,
                    details={
                        **base,
                        "incremental_strategy": "target_audit",
                        "scope_type": "target_audit_window",
                        "comparison_direction": "target_to_source",
                        "no_new_load": True,
                        "reason_code": "NO_NEW_LOAD",
                        "checkpoint_boundary": previous,
                    },
                    common=common,
                    action="skip_table",
                )
            if invalid_order:
                return self._error(
                    policy,
                    "previous load boundary must be less than the current load boundary",
                    details=base, common=common,
                )
        filter_config = dict((common.get("watermark") or {}).get("filter") or {})
        lower_operator = filter_config.get("lower_operator", ">")
        upper_operator = filter_config.get("upper_operator", "<=")
        target_predicates: List[str] = []
        parameters: List[Any] = []
        historical_predicates: List[str] = []
        historical_parameters: List[Any] = []
        if previous is not None:
            target_predicates.append(f"{column} {lower_operator} ?")
            parameters.append(previous)
            historical_predicates.append(f"{column} {self._complement(lower_operator)} ?")
            historical_parameters.append(previous)
        target_predicates.append(f"{column} {upper_operator} ?")
        parameters.append(current)
        details = {
            **base,
            "previous_load_boundary_source": previous_source,
            "scope_type": "target_audit_window",
            "full_table_strategy_used": False,
            "comparison_direction": "target_to_source",
            "source_scope_unavailable": True,
            "bootstrap_used": bootstrap_used,
            "_historical_predicates": historical_predicates,
            "_historical_parameters": historical_parameters,
        }
        return ValidationScope(
            mode="incremental", resolved=True, action=None,
            predicates=(),
            source_predicates=(),
            target_predicates=tuple(target_predicates),
            source_parameters=(),
            target_parameters=tuple(parameters),
            column=column, lower_bound=previous, upper_bound=current,
            lower_inclusive=lower_operator == ">=",
            upper_inclusive=upper_operator == "<=",
            bound_source=(
                previous_source if previous_source == current_source else "mixed"
            ),
            details=details,
        )

    @staticmethod
    def _invalid_boundary_order(previous, current):
        # Preserve original values as SQL parameters, while comparing ISO
        # timestamps at their actual precision and UTC offsets.
        import re
        import pandas as pd
        def comparable(value):
            if isinstance(value, (list, dict, tuple, bool)):
                raise ValueError("Invalid boundary type")
            if isinstance(value, str) and re.match(r"^\d{4}-\d{2}-\d{2}", value):
                value = pd.Timestamp(value)
            if pd.isna(value):
                raise ValueError("Invalid null boundary")
            return value
        return comparable(previous) >= comparable(current)

    @staticmethod
    def _resolve_audit_bound(name, table_map, audit, checkpoint, sources):
        metadata = getattr(table_map, "incremental_metadata", None) or {}
        for source in sources:
            if source == "audit_values":
                value = (audit.get("_batch_values") or {}).get(name)
                if value is not None:
                    return value, source
            if source == "table_yaml":
                value = audit.get(f"{name}_load_value")
                if value is not None:
                    return value, source
            elif source == "runtime_metadata" and isinstance(metadata, dict):
                value = metadata.get(f"{name}_load_boundary")
                if value is not None:
                    return value, source
            elif source == "checkpoint" and checkpoint and name == "previous":
                # The boundary committed by the last successful run is the next
                # run's lower bound; a checkpoint never supplies an upper bound.
                value = checkpoint.get("current_load_boundary")
                if value is not None:
                    return value, source
        return None, None

    def _cdc_scope(
        self, table_map, policy, common, checkpoint, source_column, target_column,
        cdc, audit, capabilities,
    ):
        """CDC: the configured operation column says what happened; the window
        and the matching still come from the existing selectors."""
        if not cdc:
            return self._error(
                policy,
                "CDC strategy requires incremental.cdc metadata "
                "(operation_column and operation_values)",
                common=common,
            )
        generic = dict(common.get("cdc") or {})
        delete_behavior = generic.get("delete_behavior")
        if cdc["operation_values"].get("delete"):
            if not delete_behavior:
                return self._error(
                    policy,
                    "incremental.cdc configures DELETE operation values but "
                    "validations.incremental_strategy.cdc.delete_behavior is not "
                    "configured",
                    common=common,
                )
            if delete_behavior not in self.IMPLEMENTED_DELETE_BEHAVIORS:
                return self._error(
                    policy,
                    f"CDC delete behavior '{delete_behavior}' is not supported by "
                    "this framework version; supported: "
                    + ", ".join(self.IMPLEMENTED_DELETE_BEHAVIORS),
                    common=common,
                )
            if delete_behavior == "soft_delete" and not generic.get("soft_delete_column"):
                return self._error(
                    policy,
                    "CDC soft_delete behavior requires "
                    "validations.incremental_strategy.cdc.soft_delete_column",
                    common=common,
                )
        bootstrap = self._bootstrap_mode(common, "cdc")
        if source_column or target_column:
            base = self._watermark_scope(
                table_map, policy, common, checkpoint, source_column, target_column,
                bootstrap_mode=bootstrap,
            )
        elif audit:
            base = self._target_audit_scope(
                table_map, policy, common, checkpoint, audit, bootstrap_mode=bootstrap
            )
        elif capabilities.get("key_checkpoint_strategy"):
            base = self._primary_key_scope(
                table_map, policy, common, checkpoint, bootstrap_mode=bootstrap
            )
        else:
            return self._error(
                policy,
                "CDC strategy requires a resolvable current window: a watermark, "
                "a usable key checkpoint or a target audit window",
                common=common,
            )
        if not base.resolved or base.details.get("full_table_strategy_used"):
            return base
        details = {
            **base.details,
            "incremental_strategy": "cdc",
            "cdc_base_strategy": base.details.get("incremental_strategy"),
            "cdc_operation_column": cdc["operation_column"],
            "cdc_operation_values": cdc["operation_values"],
            "cdc_sequence_column": cdc.get("sequence_column"),
            "cdc_delete_behavior": delete_behavior,
        }
        return replace(base, details=details)

    def _full_hash_scope(self, table_map, policy, common, requested):
        safety = dict(common.get("safety") or {})
        full_hash = dict((common.get("fallback") or {}).get("full_hash") or {})
        if not safety.get("allow_explicit_full_hash_strategy", True):
            return self._error(
                policy, "Explicit full-hash strategy is disabled", common=common
            )
        if not full_hash.get("enabled", True) or not full_hash.get(
            "use_existing_hash_comparison", True
        ):
            return self._error(
                policy, "Existing full hash comparison is not enabled",
                common=common,
            )
        return ValidationScope(
            mode="incremental", resolved=True, action=None,
            details={
                **self._base_details(table_map, "full_hash"),
                "scope_type": "full_table",
                "full_table_strategy_used": True,
                "full_table_strategy_reason": (
                    "explicit_table_strategy" if requested == "full_hash"
                    else "explicit_no_watermark_no_usable_pk"
                ),
                "preserve_key_mappings": bool(
                    full_hash.get("preserve_key_mappings", True)
                ),
            },
        )

    def _attach_policy(
        self, scope, table_map, common, capabilities, requested, selected,
        auto_reason, cdc, audit,
    ) -> ValidationScope:
        """Carry the one resolved incremental execution policy on the scope."""
        details = dict(scope.details)
        strategy = details.get("incremental_strategy")
        classification = dict(common.get("classification") or {})
        operation_policy = dict(common.get("operation_policy") or {})
        enabled = bool(classification.get("enabled")) and bool(operation_policy)
        unavailable = None
        if not enabled:
            unavailable = (
                "operation classification is not enabled in "
                "validations.incremental_strategy"
            )
        elif not scope.resolved:
            unavailable = "incremental scope could not be resolved"
        elif strategy not in self.CLASSIFIABLE_STRATEGIES:
            unavailable = (
                "full-hash snapshot fallback has no current/historical target "
                "boundary; operations cannot be classified"
            )
        scope_type = details.get("scope_type")
        incremental_scope = {
            "incremental_window": "incremental_window",
            "key_checkpoint": "key_checkpoint",
            "target_audit_window": "target_audit_window",
            "full_table": "unavailable",
        }.get(scope_type, "unresolved")
        generic_cdc = dict(common.get("cdc") or {})
        resolved_cdc = None
        # CDC metadata describes source-side change events.  A target-audit
        # window has no source selector: its current target rows and previous
        # target history provide the operation evidence.  Carrying CDC into
        # that policy makes the record validator project/count an unscoped
        # source CDC column, which is both the wrong direction and potentially
        # a full source scan.  Keep the configured capability in
        # ``capabilities`` but do not activate source CDC semantics for C/D.
        if (
            strategy in self.CLASSIFIABLE_STRATEGIES
            and cdc
            and details.get("comparison_direction") != "target_to_source"
        ):
            resolved_cdc = {
                "enabled": True,
                "operation_column": cdc["operation_column"],
                "operation_values": cdc["operation_values"],
                "sequence_column": cdc.get("sequence_column"),
                "delete_behavior": generic_cdc.get("delete_behavior"),
                "soft_delete_column": generic_cdc.get("soft_delete_column"),
                "soft_delete_values": list(generic_cdc.get("soft_delete_values") or []),
            }
        incremental_policy = {
            "classification_enabled": enabled and unavailable is None,
            "classification_unavailable_reason": unavailable,
            "operation_policy": operation_policy,
            "target_behavior": (common.get("target_behavior") or {}).get("mode"),
            "historical_check": bool(classification.get("historical_check", True)),
            "historical_absence_proves_insert": bool(
                classification.get("historical_absence_proves_insert", False)
            ),
            "preserve_operation_and_status_separately": bool(
                classification.get("preserve_operation_and_status_separately", True)
            ),
            "capture_mismatch_evidence": bool(
                (common.get("reporting") or {}).get("capture_mismatch_evidence", True)
            ),
            "cdc": resolved_cdc,
            "target_audit_column": audit["load_timestamp_column"] if audit else None,
            "target_watermark_column": details.get("target_watermark_column"),
            "source_watermark_column": details.get("source_watermark_column"),
            "comparison_direction": details.get("comparison_direction", "bidirectional"),
            "historical_target_predicates": list(details.pop("_historical_predicates", []) or []),
            "historical_target_parameters": list(details.pop("_historical_parameters", []) or []),
            "capabilities": dict(capabilities or {}),
            "bootstrap_used": bool(details.get("bootstrap_used", False)),
        }
        details.update({
            "capabilities": dict(capabilities or {}),
            "requested_incremental_strategy": requested,
            "resolved_incremental_strategy": strategy,
            "auto_strategy_reason": auto_reason,
            "incremental_scope": incremental_scope,
            "fallback_used": bool(details.get("full_table_strategy_used", False)),
            "operation_classification_enabled": incremental_policy["classification_enabled"],
            "incremental_policy": incremental_policy,
        })
        return replace(scope, details=details)

    def _resolve_bound(self, name, mode, table_map, watermark, checkpoint, sources):
        field = f"incremental_{name}_value"
        metadata_key = f"{name}_watermark"
        if mode in {"manual", "hybrid"} and "table_yaml" in sources:
            value = getattr(table_map, field, None)
            if value is not None:
                return value, "table_yaml"
        if mode == "manual":
            return None, None
        metadata = getattr(table_map, "incremental_metadata", None) or {}
        for source in sources:
            if source == "runtime_metadata" and isinstance(metadata, dict):
                value = metadata.get(metadata_key)
                if value is not None:
                    return value, source
            if source == "checkpoint" and checkpoint:
                # The checkpoint chain is: the current_watermark committed by the
                # last successful run is this run's previous_watermark. A
                # checkpoint never supplies the upper bound; that always comes
                # from configuration or runtime metadata for the run being made.
                if name == "previous":
                    value = checkpoint.get("current_watermark")
                    if value is None:
                        value = checkpoint.get("previous_watermark")
                    if value is not None:
                        return value, source
            if source == "common_validation_config":
                value = watermark.get(metadata_key)
                if value is not None:
                    return value, source
        return None, None

    @classmethod
    def _usable_key(cls, table_map, common) -> bool:
        pk = dict(common.get("primary_key") or {})
        if getattr(table_map, "incremental_pk_enabled", None) is False:
            return False
        if not pk.get("enabled", True):
            return False
        mappings = list(getattr(table_map, "key_mappings", None) or ())
        if len(mappings) != 1:
            return False
        metadata = getattr(table_map, "incremental_metadata", None) or {}
        key_metadata = metadata.get("key_metadata", {}) if isinstance(metadata, dict) else {}
        compatible = getattr(table_map, "incremental_pk_compatible", None)
        monotonic = getattr(table_map, "incremental_pk_monotonic", None)
        if compatible is None:
            compatible = bool(
                key_metadata.get("incremental_compatible")
                or key_metadata.get("identity")
                or key_metadata.get("is_identity")
            )
        if monotonic is None:
            monotonic = bool(
                key_metadata.get("monotonic")
                or key_metadata.get("identity")
                or key_metadata.get("is_identity")
            )
        return bool(
            (compatible or not pk.get("require_incremental_compatible_key", True))
            and (monotonic or not pk.get("require_monotonic_key", True))
        )

    @staticmethod
    def _mapping_value(mapping, name):
        return mapping.get(name) if isinstance(mapping, dict) else getattr(mapping, name)

    @staticmethod
    def _text(value):
        return str(value).strip() if value else None

    @staticmethod
    def _base_details(table_map, strategy):
        return {
            "incremental_enabled": getattr(table_map, "incremental_enabled", None) is not False,
            "incremental_strategy": strategy,
        }

    @classmethod
    def _error(cls, policy, reason, column=None, details=None, common=None, action=None):
        resolved_action, status = cls._unresolved_action(policy, dict(common or {}))
        if action == "skip_table":
            resolved_action, status = "skip_table", policy.status_on_skip
        elif action == "error":
            resolved_action, status = "error", policy.status_on_error
        action = resolved_action
        error_details = {
            "incremental_enabled": True,
            "incremental_strategy": None,
            "scope_type": "unresolved",
            "full_table_strategy_used": False,
            **(details or {}),
            "unresolved_action": action,
        }
        if action == "full_scan":
            # Reached only when the common configuration explicitly permits an
            # implicit full-table fallback. Reported as such, never as a window.
            error_details.update(
                scope_type="full_table",
                full_table_strategy_used=True,
                full_table_strategy_reason="implicit_full_table_fallback_allowed",
            )
        return ValidationScope(
            mode="incremental", resolved=False, action=action,
            column=column, reason=reason, policy_status=status,
            details=error_details,
        )
