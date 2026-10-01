from __future__ import annotations

from typing import Dict, List

from pandas.api.types import is_string_dtype

from core.models import CheckResult, ValidationStatus
from core.execution_engine import ExecutionContext
from .base_validator import BaseValidator
from .validator_utils import _fqn


class PIIScanner(BaseValidator):
    PATTERNS = {
        "email": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
        "ssn_us": r"\b\d{3}-\d{2}-\d{4}\b",
        "phone": r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b",
    }

    #: Rows sampled when the execution configuration does not set a smaller cap.
    DEFAULT_SAMPLE_SIZE = 100

    def validate(
        self,
        source_conn,
        target_conn,
        table_map,
        thresholds,
        context: ExecutionContext = None,
        source_type: str = "generic",
        execution_config=None,
        execution_cache=None,
    ) -> List[CheckResult]:
        dialect = self._dialect(target_conn)
        tgt_fqn = _fqn(
            table_map.target_schema, table_map.target_table, dialect
        )
        sample_size = self._sample_size(execution_config)
        # The sample is bounded on the engine, so scanning for PII never depends
        # on the size of the target table.
        sql = dialect.select(
            "*",
            tgt_fqn,
            self._build_where_clause(table_map, dialect),
            limit=sample_size,
        )

        try:
            df = target_conn.execute_query(sql)

            if df.empty:
                return [
                    CheckResult(
                        check_name="pii_scan",
                        status=ValidationStatus.PASS,
                        message="PII scan passed: target table is empty",
                        details={"target_sql": sql},
                    )
                ]

            sample_df = df.head(sample_size)
            findings = self._scan_dataframe(sample_df)
            status = ValidationStatus.WARN if findings else ValidationStatus.PASS

            return [
                CheckResult(
                    check_name="pii_scan",
                    status=status,
                    message=(
                        f"PII patterns found: {findings}"
                        if findings
                        else f"No PII patterns found in first {len(sample_df)} sampled rows"
                    ),
                    details={
                        "target_sql": sql,
                        "sample_size": len(sample_df),
                        "patterns_checked": list(self.PATTERNS.keys()),
                        "findings": findings,
                    },
                )
            ]

        except Exception as e:
            return [
                CheckResult(
                    check_name="pii_scan",
                    status=ValidationStatus.ERROR,
                    message=f"PII scan failed: {e}",
                    details={"target_sql": sql},
                )
            ]

    def _sample_size(self, execution_config) -> int:
        """Return the configured PII sample size, never above the memory cap."""
        pandas_strategy = getattr(
            getattr(execution_config, "strategies", None), "pandas", None
        )
        cap = int(getattr(pandas_strategy, "max_rows_in_memory", 0) or 0)
        if cap:
            return min(self.DEFAULT_SAMPLE_SIZE, cap)
        return self.DEFAULT_SAMPLE_SIZE

    def _scan_dataframe(self, df) -> Dict[str, List[str]]:
        findings = {}

        for col in df.columns:
            if not self._is_text_column(df[col]):
                continue

            values = df[col].dropna().astype(str)

            for pattern_name, pattern in self.PATTERNS.items():
                if values.str.contains(pattern, regex=True).any():
                    findings.setdefault(col, []).append(pattern_name)

        return findings

    def _is_text_column(self, series) -> bool:
        return is_string_dtype(series.dtype)
