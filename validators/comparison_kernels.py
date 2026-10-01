"""
fabric_migration_validator/validators/comparison_kernels.py
───────────────────────────────────────────────────────────
Column-at-a-time value comparison.

Comparing two aligned cohorts value by value in Python is the dominant cost of
a full record comparison. This module removes that cost without changing a
single comparison outcome, using one observation:

    For two values that are both non-NULL, ``source == target`` implies the
    framework's full comparison also matches.

That holds because :func:`core.value_normalization.temporal_token` is a pure
function of its input, so equal inputs produce equal tokens, and
:func:`core.value_normalization.temporal_values_match` always matches a token
against itself under every configured precision and timezone policy.

So equality is evaluated for the whole column at once, and only the rows that
are *not* proven equal are handed to the configured per-value comparison. NULL
semantics are likewise decided for the whole column, directly from the
configured ``null_handling`` policy.

The fast path is enabled only for columns whose inferred content makes the
implication provable (numbers, booleans, strings, all-NULL). Temporal, binary
and mixed columns keep the existing row-by-row path unchanged, so historical
and out-of-range timestamps, ``NaT``, ``NaN`` and ``None`` continue to be
handled exactly as before.
"""

from __future__ import annotations

from typing import Any, Callable, List, Optional, Sequence

import numpy as np
import pandas as pd

from core.value_normalization import is_null_scalar

#: Inferred column contents for which ``==`` provably implies a full match.
#:
#: ``string`` is safe because two equal strings tokenize identically; a string
#: that looks temporal is still compared through the configured temporal rules
#: whenever the two sides are *not* equal.
VECTORIZABLE_INFERRED_TYPES = frozenset({
    "integer",
    "floating",
    "decimal",
    "mixed-integer-float",
    "boolean",
    "string",
    "empty",
})


def null_mask(values: pd.Series, blank_is_null: bool) -> np.ndarray:
    """Return the configured NULL mask for a whole column.

    Mirrors :func:`core.value_normalization.is_null_scalar` exactly, including
    the rule that only *string* blanks may count as NULL.
    """
    try:
        mask = pd.isna(values).to_numpy(dtype=bool, copy=True)
    except (TypeError, ValueError):
        return np.array(
            [is_null_scalar(value, blank_is_null=blank_is_null) for value in values],
            dtype=bool,
        )
    if not blank_is_null:
        return mask
    if values.dtype == object or isinstance(values.dtype, pd.StringDtype):
        try:
            is_text = values.map(lambda item: isinstance(item, str)).to_numpy(
                dtype=bool, copy=False
            )
            blank = (
                values.where(is_text, other="x").astype("string").str.strip().eq("")
            ).fillna(False).to_numpy(dtype=bool, copy=False)
            mask = mask | (is_text & blank)
        except (TypeError, ValueError):
            return np.array(
                [
                    is_null_scalar(value, blank_is_null=blank_is_null)
                    for value in values
                ],
                dtype=bool,
            )
    return mask


def vectorizable(values: pd.Series) -> bool:
    """Whether ``==`` on this column provably implies a full framework match."""
    try:
        inferred = pd.api.types.infer_dtype(values, skipna=True)
    except (TypeError, ValueError):  # pragma: no cover - exotic extension dtypes
        return False
    return inferred in VECTORIZABLE_INFERRED_TYPES


def proven_equal_mask(
    source_values: pd.Series, target_values: pd.Series
) -> Optional[np.ndarray]:
    """Rows this column proves equal, or ``None`` when no fast path applies."""
    if len(source_values) != len(target_values):
        return None
    if not (vectorizable(source_values) and vectorizable(target_values)):
        return None
    try:
        left = source_values.to_numpy(dtype=object, copy=False)
        right = target_values.to_numpy(dtype=object, copy=False)
        equal = np.equal(left, right)
    except (TypeError, ValueError):
        try:
            equal = source_values.eq(target_values).fillna(False).to_numpy(
                dtype=bool, copy=False
            )
        except (TypeError, ValueError):
            return None
    if not isinstance(equal, np.ndarray):
        return None
    if equal.dtype != bool:
        try:
            equal = equal.astype(bool)
        except (TypeError, ValueError):
            return None
    return equal


class ValueComparator:
    """Configured value comparison, applied column at a time.

    ``per_value`` is the framework's existing single-value comparison. It stays
    authoritative: the fast path never decides a mismatch, only a match that
    ``per_value`` would also have decided.
    """

    def __init__(
        self,
        per_value: Callable[[Any, Any], bool],
        blank_is_null: bool,
        both_null_matches: bool,
        one_null_matches: bool,
    ) -> None:
        self._per_value = per_value
        self._blank_is_null = bool(blank_is_null)
        self._both_null_matches = bool(both_null_matches)
        self._one_null_matches = bool(one_null_matches)
        self.vectorized_columns = 0
        self.scalar_columns = 0
        self.vectorized_rows = 0
        self.scalar_rows = 0

    @classmethod
    def from_policies(
        cls, policies: Any, per_value: Callable[[Any, Any], bool]
    ) -> "ValueComparator":
        nulls = policies.nulls
        return cls(
            per_value=per_value,
            blank_is_null=nulls["blank_is_null"],
            both_null_matches=nulls["both_null"] == "match",
            one_null_matches=nulls["one_null"] == "match",
        )

    # ── Column comparison ─────────────────────────────────────
    def mismatch_mask(
        self, source_values: pd.Series, target_values: pd.Series
    ) -> List[bool]:
        """Return one mismatch flag per aligned record for a single column."""
        record_count = len(source_values)
        if record_count == 0:
            return []

        source_null = null_mask(source_values, self._blank_is_null)
        target_null = null_mask(target_values, self._blank_is_null)
        both_null = source_null & target_null
        one_null = (source_null | target_null) & ~both_null

        mismatch = np.zeros(record_count, dtype=bool)
        mismatch |= both_null & (not self._both_null_matches)
        mismatch |= one_null & (not self._one_null_matches)

        # Rows whose outcome the NULL policy already decided.
        decided = both_null | one_null
        candidates = ~decided
        if not candidates.any():
            self.vectorized_columns += 1
            self.vectorized_rows += record_count
            return mismatch.tolist()

        proven = proven_equal_mask(source_values, target_values)
        if proven is not None:
            resolved = candidates & proven
            self.vectorized_rows += int(resolved.sum()) + int(decided.sum())
            remaining = candidates & ~proven
        else:
            resolved = np.zeros(record_count, dtype=bool)
            self.vectorized_rows += int(decided.sum())
            remaining = candidates

        remaining_positions = np.flatnonzero(remaining)
        if remaining_positions.size:
            self.scalar_columns += 1
            self.scalar_rows += int(remaining_positions.size)
            source_list = source_values.tolist()
            target_list = target_values.tolist()
            for position in remaining_positions:
                index = int(position)
                if not self._per_value(source_list[index], target_list[index]):
                    mismatch[index] = True
        else:
            self.vectorized_columns += 1
        return mismatch.tolist()

    def metrics(self) -> dict:
        """Report fields describing how much comparison stayed vectorized."""
        total_rows = self.vectorized_rows + self.scalar_rows
        return {
            "vectorized_columns": self.vectorized_columns,
            "scalar_columns": self.scalar_columns,
            "vectorized_value_comparisons": self.vectorized_rows,
            "scalar_value_comparisons": self.scalar_rows,
            "vectorized_comparison_ratio": (
                round(self.vectorized_rows / total_rows, 6) if total_rows else None
            ),
        }


def null_masks(
    values: Sequence[pd.Series], blank_is_null: bool
) -> List[np.ndarray]:
    """Configured NULL masks for several columns at once."""
    return [null_mask(series, blank_is_null) for series in values]
