"""
Enhanced logging for record comparison strategy display.

Provides clean, structured output showing:
- Row counts and strategy selection
- Processing mode (sample vs. full)
- Memory usage expectations
- NO column names or data values
"""

import logging
from typing import Dict, Any, Optional


class RecordComparisonLogger:
    """Format record comparison logging clearly."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def log_strategy_selection(
        self,
        table_id: str,
        source_count: int,
        target_count: int,
        strategy: str,
        high_workload: bool = False,
        workload_size: Optional[float] = None,
    ) -> None:
        """Log strategy selection with row counts and decision."""

        max_count = max(source_count, target_count)

        # Determine if mandatory sampling triggered
        if max_count >= 1_000_000:
            sample_info = "\n    Sample Size: 100,000"
            memory_info = "100-200 MB (sample only)"
            comparison_type = "Sample"
            load_memory = "No"
        else:
            sample_info = ""
            if high_workload and workload_size:
                memory_info = "Streaming (hash-first)"
                comparison_type = "Hash-First"
                load_memory = "No (streaming)"
            else:
                memory_info = "Bounded batches"
                comparison_type = "Full"
                load_memory = "No (bounded)"

        # Format output
        output = f"""
╔═══════════════════════════════════════════════════════════════╗
║ RECORD COMPARISON STRATEGY SELECTION                          ║
╠═══════════════════════════════════════════════════════════════╣
║ Table:                      {table_id:<40}║
║ Source Row Count:           {source_count:>20,}                  ║
║ Target Row Count:           {target_count:>20,}                  ║
║───────────────────────────────────────────────────────────────║
║ Strategy:                   {strategy:<40}║
║ Comparison Type:            {comparison_type:<40}║{sample_info}
║ Full Table Loaded:          {load_memory:<40}║
║ Processing Mode:            {memory_info:<40}║
╚═══════════════════════════════════════════════════════════════╝
"""
        self.logger.info(output)

    def log_sample_execution(
        self,
        table_id: str,
        sample_size: int,
        source_fetched: int,
        target_fetched: int,
    ) -> None:
        """Log sample comparison execution."""
        output = f"""
  [SAMPLE COMPARISON]
  Table: {table_id}
  Sample Size: {sample_size:,}
  Source Sample Fetched: {source_fetched:,} rows
  Target Sample Fetched: {target_fetched:,} rows
  Processing: Comparing samples only (no full table in memory)
"""
        self.logger.info(output)

    def log_full_execution(
        self,
        table_id: str,
        source_count: int,
        target_count: int,
        strategy: str,
    ) -> None:
        """Log full/hash-first execution."""
        output = f"""
  [FULL COMPARISON - {strategy}]
  Table: {table_id}
  Source Rows: {source_count:,}
  Target Rows: {target_count:,}
  Processing: Streaming/Bounded batches (no full table in memory)
  Data loaded: On-demand by batch
"""
        self.logger.info(output)

    def log_validation_complete(
        self,
        table_id: str,
        matched: int,
        mismatched: int,
        excess_source: int,
        excess_target: int,
        duration: float,
    ) -> None:
        """Log validation completion."""
        output = f"""
  [VALIDATION COMPLETE]
  Table: {table_id}
  Results: Matched={matched:,}, Mismatched={mismatched:,},
           Excess in Source={excess_source:,}, Excess in Target={excess_target:,}
  Duration: {duration:.2f}s
"""
        self.logger.info(output)


def configure_record_comparison_logging() -> RecordComparisonLogger:
    """Create configured logger for record comparison."""
    logger = logging.getLogger("record_comparison_production")
    return RecordComparisonLogger(logger)
