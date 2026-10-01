"""
fabric_migration_validator/validators/key_cohort.py
───────────────────────────────────────────────────
Handing one batch of keys to the other database.

A batched full comparison reads a page of source rows and must then read the
matching target rows. Expressing that as one predicate per key
(``(k=v1) OR (k=v2) OR ...``) produces a statement the optimizer cannot seek,
so each batch degenerates into a full scan of the other side.

This module renders the same logical cohort through a strategy chosen from
configuration and connector capability:

``values_join``
    An equality join against an inline row constructor. Exact set semantics,
    seekable, and independent of how either side orders values.
``key_range``
    One lexicographic key-range filter per page. The cheapest option when both
    sides share collation and NULL ordering.
``predicate_or``
    The original per-key predicate list, kept for platforms that support
    neither of the above.

Every predicate is built with two-valued logic — each comparison is guarded by
an explicit ``IS NULL`` / ``IS NOT NULL`` test — so composite keys containing
NULLs are handled exactly, and a range bound can be safely negated.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Sequence, Tuple

from core.sql_dialect import (
    SqlDialect,
    SqlDialectError,
    bind_sql,
    resolve_cohort_strategy,
)
from core.value_normalization import is_null_scalar

#: Table alias used when a strategy has to qualify projected columns.
TABLE_ALIAS = "__mv_source_table"


@dataclass(frozen=True)
class CohortPlan:
    """What one cohort read will actually issue."""

    strategy: str
    queries: Tuple[str, ...]
    chunk_size: int
    key_count: int
    contains_null_keys: bool

    def as_details(self) -> dict:
        return {
            "key_cohort_strategy": self.strategy,
            "key_cohort_queries": len(self.queries),
            "key_cohort_chunk_size": self.chunk_size,
            "key_cohort_keys": self.key_count,
            "key_cohort_null_keys": self.contains_null_keys,
        }


class KeyCohortBuilder:
    """Render the target-side read for a page of source keys."""

    def __init__(
        self,
        dialect: SqlDialect,
        strategy: str,
        chunk_size: int,
        literal: Callable[[Any], str],
        values_join_max_keys: Optional[int] = None,
        union_values_blocks: bool = False,
    ) -> None:
        if not (
            isinstance(chunk_size, int)
            and not isinstance(chunk_size, bool)
            and chunk_size > 0
        ):
            raise SqlDialectError("Key cohort chunk size must be a positive integer")
        self.dialect = dialect
        self.strategy = strategy
        self.chunk_size = chunk_size
        self._literal = literal
        self.values_join_max_keys = values_join_max_keys
        #: When set, one ``values_join`` statement carries ``chunk_size`` keys
        #: as several row constructors of at most ``values_join_max_keys`` rows
        #: each, combined with ``UNION ALL``. Engines cap the rows of a single
        #: VALUES constructor, so this is how a bounded lookup page of several
        #: thousand keys becomes one seekable statement instead of dozens.
        self.union_values_blocks = bool(union_values_blocks)

    # ── Construction ──────────────────────────────────────────
    @classmethod
    def resolve(
        cls,
        dialect: SqlDialect,
        capabilities: Any,
        configured_strategy: Optional[str],
        chunk_size: int,
        literal: Callable[[Any], str],
        cohort_config: Any = None,
        union_values_blocks: bool = False,
    ) -> "KeyCohortBuilder":
        """Resolve the strategy from configuration, dialect and capability.

        ``cohort_config`` is ``performance.cohort_processing``. It supplies the
        requested strategy, the strategies a deployment permits at all, and the
        safety ceiling on a per-key predicate list.
        """
        requested = configured_strategy
        allowed = None
        predicate_ceiling = None
        values_join_ceiling = None
        if cohort_config is not None:
            requested = requested or getattr(cohort_config, "strategy", None)
            allowed = getattr(cohort_config, "allowed_strategies", None)
            predicate_or = getattr(cohort_config, "predicate_or", None)
            if predicate_or is not None:
                predicate_ceiling = getattr(predicate_or, "max_keys", None)
            values_join = getattr(cohort_config, "values_join", None)
            if values_join is not None:
                values_join_ceiling = getattr(values_join, "max_keys", None)
        strategy = resolve_cohort_strategy(
            dialect, capabilities, requested, allowed_strategies=allowed
        )
        if strategy == "values_join" and not values_join_ceiling:
            raise SqlDialectError(
                "performance.cohort_processing.values_join.max_keys must be "
                "configured for bounded VALUES cohorts"
            )
        if strategy == "predicate_or" and predicate_ceiling:
            # A per-key predicate list is only safe in small chunks.
            chunk_size = min(chunk_size, int(predicate_ceiling))
        return cls(
            dialect, strategy, chunk_size, literal,
            values_join_max_keys=values_join_ceiling,
            union_values_blocks=union_values_blocks,
        )

    @property
    def keys_per_statement(self) -> int:
        """How many keys one rendered statement carries for this strategy."""
        if self.strategy == "values_join" and self.values_join_max_keys:
            if self.union_values_blocks:
                return self.chunk_size
            return min(self.chunk_size, int(self.values_join_max_keys))
        return self.chunk_size

    # ── Cohort rendering ──────────────────────────────────────
    def plan(
        self,
        source: str,
        columns: Sequence[str],
        where_clause: str,
        key_columns: Sequence[str],
        key_values: Sequence[Tuple[Any, ...]],
        lower_bound: Optional[Sequence[Any]] = None,
        upper_bound: Optional[Sequence[Any]] = None,
    ) -> CohortPlan:
        """Return the statements that read the cohort for one page of keys."""
        if not key_columns:
            raise SqlDialectError("A key cohort requires configured key columns")
        unique_values = list(dict.fromkeys(key_values))
        contains_null = any(
            is_null_scalar(value, blank_is_null=False)
            for row in unique_values
            for value in row
        )
        strategy = self.strategy
        if strategy == "key_range" and upper_bound is None:
            # A range cohort needs the page's upper bound; without it the exact
            # set semantics of a join are used instead.
            strategy = "values_join" if self.dialect.supports_values_constructor else "predicate_or"
        if strategy == "values_join" and not self.values_join_max_keys:
            raise SqlDialectError(
                "A configured values_join key ceiling is required"
            )

        if strategy == "key_range":
            queries = self._range_queries(
                source, columns, where_clause, key_columns,
                lower_bound, upper_bound,
            )
        elif strategy == "values_join":
            queries = self._values_join_queries(
                source, columns, where_clause, key_columns, unique_values,
                contains_null,
            )
        else:
            queries = self._predicate_queries(
                source, columns, where_clause, key_columns, unique_values
            )
        effective_chunk_size = self.chunk_size
        if (
            strategy == "values_join"
            and self.values_join_max_keys
            and not self.union_values_blocks
        ):
            effective_chunk_size = min(
                effective_chunk_size, int(self.values_join_max_keys)
            )
        return CohortPlan(
            strategy=strategy,
            queries=tuple(queries),
            chunk_size=effective_chunk_size,
            key_count=len(unique_values),
            contains_null_keys=contains_null,
        )

    # ── Strategies ────────────────────────────────────────────
    def _range_queries(
        self,
        source: str,
        columns: Sequence[str],
        where_clause: str,
        key_columns: Sequence[str],
        lower_bound: Optional[Sequence[Any]],
        upper_bound: Optional[Sequence[Any]],
    ) -> List[str]:
        predicates = []
        if lower_bound is not None:
            predicates.append(self.greater_than(key_columns, lower_bound))
        if upper_bound is not None:
            predicates.append(self.less_or_equal(key_columns, upper_bound))
        combined = where_clause
        for predicate in predicates:
            combined = combine_where(combined, predicate)
        return [
            self.dialect.select(projection(columns, self.dialect), source, combined)
        ]

    def _values_join_queries(
        self,
        source: str,
        columns: Sequence[str],
        where_clause: str,
        key_columns: Sequence[str],
        key_values: Sequence[Tuple[Any, ...]],
        contains_null: bool,
    ) -> List[str]:
        if not key_values:
            return []
        alias = self.dialect.values_row_alias
        value_columns = [f"__mv_key_{index}" for index in range(len(key_columns))]
        queries = []
        block_rows = self.chunk_size
        if self.values_join_max_keys:
            block_rows = min(block_rows, int(self.values_join_max_keys))
        statement_rows = self.chunk_size if self.union_values_blocks else block_rows
        for chunk in batched(key_values, statement_rows):
            rows = [
                "(" + ", ".join(self._literal(value) for value in row) + ")"
                for row in chunk
            ]
            values_source = self._values_source(rows, value_columns, alias, block_rows)
            conditions = []
            for key_column, value_column in zip(key_columns, value_columns):
                left = self.dialect.column(key_column, TABLE_ALIAS)
                right = self.dialect.column(value_column, alias)
                if contains_null:
                    # NULL keys never satisfy an equality join, so a null-safe
                    # condition is used only when the cohort actually holds one.
                    conditions.append(
                        f"({left} = {right} OR "
                        f"({left} IS NULL AND {right} IS NULL))"
                    )
                else:
                    conditions.append(f"{left} = {right}")
            joined = (
                f"{source} AS {TABLE_ALIAS} INNER JOIN {values_source} "
                f"ON {' AND '.join(conditions)}"
            )
            queries.append(
                self.dialect.select(
                    projection(columns, self.dialect, qualifier=TABLE_ALIAS),
                    joined,
                    qualify_where(
                        where_clause, key_columns, columns, self.dialect
                    ),
                )
            )
        return queries

    def _values_source(
        self,
        rows: Sequence[str],
        value_columns: Sequence[str],
        alias: str,
        block_rows: int,
    ) -> str:
        """Render the inline key set for one statement.

        Up to ``block_rows`` keys form a single row constructor. A larger page
        is rendered as several constructors of at most ``block_rows`` rows
        combined with ``UNION ALL`` behind the usual alias, so the statement
        stays within the engine's per-constructor row limit while still being
        one set-based, seekable lookup.
        """
        if len(rows) <= block_rows:
            return self.dialect.values_source(rows, value_columns, alias)
        selects = []
        for block_index, block in enumerate(batched(rows, block_rows)):
            block_alias = f"{alias}_{block_index}"
            projected = ", ".join(
                self.dialect.column(column, block_alias) for column in value_columns
            )
            selects.append(
                f"SELECT {projected} FROM "
                f"{self.dialect.values_source(block, value_columns, block_alias)}"
            )
        return f"({' UNION ALL '.join(selects)}) AS {self.dialect.alias(alias)}"

    def _predicate_queries(
        self,
        source: str,
        columns: Sequence[str],
        where_clause: str,
        key_columns: Sequence[str],
        key_values: Sequence[Tuple[Any, ...]],
    ) -> List[str]:
        predicates = list(dict.fromkeys(
            self.row_predicate(key_columns, row) for row in key_values
        ))
        return [
            self.dialect.select(
                projection(columns, self.dialect),
                source,
                combine_where(where_clause, "(" + " OR ".join(chunk) + ")"),
            )
            for chunk in batched(predicates, self.chunk_size)
        ]

    # ── Two-valued predicate construction ─────────────────────
    def row_predicate(
        self, columns: Sequence[str], values: Sequence[Any]
    ) -> str:
        conditions = []
        for column, value in zip(columns, values):
            column = self.dialect.quote(column)
            if is_null_scalar(value, blank_is_null=False):
                conditions.append(f"{column} IS NULL")
            else:
                conditions.append(
                    f"({column} IS NOT NULL AND {column} = {self._literal(value)})"
                )
        return "(" + " AND ".join(conditions) + ")"

    def greater_than(
        self, columns: Sequence[str], values: Sequence[Any]
    ) -> str:
        """Lexicographic ``(columns) > (values)``, always TRUE or FALSE."""
        if len(columns) != len(values):
            raise SqlDialectError(
                "Keyset columns and values must have the same length"
            )
        alternatives = []
        for index, (column, value) in enumerate(zip(columns, values)):
            prefix = [
                self._equals(prefix_column, prefix_value)
                for prefix_column, prefix_value in zip(
                    columns[:index], values[:index]
                )
            ]
            strictly_greater = self._greater(column, value)
            if strictly_greater is None:
                continue
            alternatives.append(
                "(" + " AND ".join(prefix + [strictly_greater]) + ")"
            )
        if not alternatives:
            # Nothing can sort after this bound under the configured ordering.
            return "(1 = 0)"
        return "(" + " OR ".join(alternatives) + ")"

    def less_or_equal(
        self, columns: Sequence[str], values: Sequence[Any]
    ) -> str:
        """Lexicographic ``(columns) <= (values)``.

        Safe to express as the negation of ``>`` because every comparison in
        that expression is explicitly NULL-guarded, so it never evaluates to
        UNKNOWN.
        """
        return f"(NOT {self.greater_than(columns, values)})"

    def _equals(self, column: str, value: Any) -> str:
        column = self.dialect.quote(column)
        if is_null_scalar(value, blank_is_null=False):
            return f"{column} IS NULL"
        return f"({column} IS NOT NULL AND {column} = {self._literal(value)})"

    def _greater(self, column: str, value: Any) -> Optional[str]:
        column = self.dialect.quote(column)
        nulls_first = self.dialect.nulls_sort_first
        if is_null_scalar(value, blank_is_null=False):
            # Values after NULL exist only when NULLs sort first.
            return f"{column} IS NOT NULL" if nulls_first else None
        literal = self._literal(value)
        if nulls_first:
            return f"({column} IS NOT NULL AND {column} > {literal})"
        return f"({column} IS NULL OR {column} > {literal})"


# ── Helpers ───────────────────────────────────────────────────

def projection(
    columns: Sequence[str],
    dialect: SqlDialect,
    qualifier: Optional[str] = None,
) -> str:
    """Render a projection, optionally qualified by a table alias."""
    unique = list(dict.fromkeys(column for column in columns if column))
    if not unique:
        return f"{qualifier}.*" if qualifier else "*"
    if qualifier:
        return ", ".join(dialect.column(column, qualifier) for column in unique)
    return ", ".join(dialect.quote(column) for column in unique)


def combine_where(where_clause: str, predicate: str) -> str:
    if not predicate:
        return where_clause
    if where_clause.strip():
        return bind_sql(
            f"{where_clause} AND ({predicate})", where_clause, predicate
        )
    return bind_sql(f" WHERE {predicate}", predicate)


def qualify_where(
    where_clause: str,
    key_columns: Sequence[str],
    columns: Sequence[str],
    dialect: SqlDialect,
) -> str:
    """Qualify a configured filter so a joined statement stays unambiguous.

    Only bare occurrences of the columns this read projects are qualified; the
    filter's own literals and operators are untouched.
    """
    if not where_clause.strip():
        return where_clause
    import re

    names = sorted(
        {str(name) for name in list(key_columns) + list(columns) if name},
        key=len,
        reverse=True,
    )
    qualified = where_clause
    for name in names:
        quoted = dialect.quote(name)
        qualified = re.sub(
            rf"(?<![\w.]){re.escape(quoted)}(?![\w])",
            f"{TABLE_ALIAS}.{quoted}",
            qualified,
        )
    # ``re.sub`` returns a plain string. Reattach the parameters carried by a
    # bound incremental/watermark filter so the connector supplies one value
    # for every unchanged ``?`` placeholder in the qualified statement.
    return bind_sql(qualified, where_clause)


def batched(values: Sequence[Any], size: int):
    for index in range(0, len(values), size):
        yield values[index:index + size]
