# Large-table record comparison: bounded 100K-sample cohort

Date: 2026-09-07

## Symptom

For tables with 1,000,000 rows or more the 100K source sample returned in about
two seconds, after which the run appeared stuck for seven hours or more and in
some runs ended with a memory error (a Spark JVM crash log showed the host with
849 MB of 16 GB RAM free and 5 MB of a 24 GB page file free while the JVM
itself used about 300 MB).

## Root causes (confirmed in code)

| # | Where | What |
|---|-------|------|
| 1 | `validators/record_comparison_validator.py`, former line 345 | `self._filter_batch_size = sample_size` made the 100K sample size the target lookup batch size. |
| 2 | `_build_signature_cohort_queries` (keyless tables) | One statement carried one predicate per sampled row over every compared column, repeated twice. For 100K rows x 124 columns that is about 12.4 million comparisons and roughly 750 MB of SQL. Four of the five configured tables have `key_mappings: []`. |
| 3 | `_build_filter_queries` (keyed tables) | One statement with 100K OR-predicates. |
| 4 | `core/connection_manager.py`, `_sql_code_only` | The read-only guard did `list(sql)`: one Python object pointer per character, about 6 GB for the 750 MB statement, plus several full copies. |
| 5 | Table parallelism | Up to four tables built such statements at the same time. |
| 6 | Query timeout | `performance.query_execution.timeout_seconds` was never applied to the ODBC connection; only the login timeout was. |
| 7 | `validators/record_comparison_refactored.py` | `_build_where_clause` called `ValidationScope.sql_filter`, which does not exist; every scoped table failed before its first read when that class was wired. |
| 8 | `validators/record_comparison_production.py` | `pd.merge(how="full")` is not a pandas join type; same-named compare columns also collided after the merge. |

## Runtime class

Before: `validators/__init__.py` exported `RecordComparisonProduction` (and the
2026-09-06 run failed with the `sql_filter` error coming from the refactored
class). After: it exports `validators.record_comparison_validator.RecordComparisonValidator`,
the complete implementation (dual keyset merge / streaming hash for tables
below the threshold, cached key-quality routing, evidence records,
checkpoints). Call path:

```
run_validation.py / cli
  -> core.orchestrator.ValidationOrchestrator._new_validator("record_comparison")
  -> core.validation_registry.ValidationRegistry (imports validators.RecordComparisonValidator)
  -> validators.record_comparison_validator.RecordComparisonValidator.validate
       -> mode: large_table_sampling applies at >= row_count_threshold  -> sample_comparison
       -> keyed: cached key statistics (key_validation) decide keyset vs hash fallback
       -> _sample_source_and_target
            -> source: SELECT TOP <sample_size> ... ORDER BY <keys | NEWID()>
            -> keyed:   _keyed_sample_cohort   (VALUES-join pages of target_key_lookup_batch_size keys)
            -> keyless: _keyless_sample_cohort (discriminator VALUES-join pages, exact fallback per batch)
       -> _compare_by_key / _compare_by_row_hash (unchanged)
```

## Design

### Sample size vs lookup batch size

`comparison_settings.large_table_sampling.sample_size` decides how many source
rows are examined. `comparison_settings.comparison_tiers.target_key_lookup_batch_size`
(existing key) and `target_keyless_lookup_batch_size` decide how many of those
rows one target statement asks about. They are never derived from each other.

### Keyed tables

Distinct sampled keys are looked up in pages of `target_key_lookup_batch_size`
(default 5000). Each page is one set-based statement rendered by
`KeyCohortBuilder` with the configured strategy (`performance.cohort_processing`):
a VALUES join by default, split inside the statement into `UNION ALL` blocks of
`values_join.max_keys` rows (500) so no row constructor exceeds the engine
limit. Composite keys join on every key column; NULL keys use null-safe
conditions. 100K sampled keys = 20 statements. If the target returns more than
`keys x keyless_candidate_row_multiplier` rows the cohort is refused (the key is
not unique) instead of being materialized without limit.

### Keyless tables

1. The source sample is hashed once with the existing canonical row hash.
2. Up to `keyless_discriminator_columns` (3) compared columns are chosen from
   the sample as a database-side candidate filter: columns without NULLs
   first, then non-floating-point, then highest distinct count.
3. For each batch of `target_keyless_lookup_batch_size` sampled rows the
   distinct discriminator tuples are looked up with the same VALUES-join
   statement a keyed table uses. Candidates are streamed, hashed in Python and
   kept only if identical on every compared column and the source multiplicity
   is not yet satisfied. The hash is an optimization only; the retained cohort
   and the multiset comparison are exactly what the previous implementation
   produced.
4. Fallback: if a batch returns more than `rows x keyless_candidate_row_multiplier`
   candidates (weak discriminator), that batch alone is re-read with the
   exact per-row predicate statement (all compared columns, ROW_NUMBER
   multiplicity bound), limited to `performance.cohort_processing.predicate_or.max_keys`
   rows per statement (100). A giant statement is never built.

### Statement safety

Every generated statement is checked before it runs against
`comparison_settings.query_safety.max_sql_length_bytes` (5 MB) and
`max_predicates_per_query` (5000). A statement over a ceiling causes its batch
to be split in half and re-rendered; a single row that still exceeds the
ceilings is an error. `_build_filter_queries` (also used by data value
validation) is capped by the same predicate ceiling.

### Read-only guard

`_ensure_read_only_sql` now scans by index (`_iter_sql_code_spans`) and checks
the leading keyword, statement separators and mutating keywords on the code
spans in place. Measured: a 13 MB statement is validated with 1.7 KB of
allocation. Behavior (reject non-SELECT, `;`, mutating keywords outside
literals and comments) is unchanged.

### Query timeout

`performance.query_execution.timeout_seconds` is applied as the pyodbc
`Connection.timeout` (query timeout inherited by every cursor) through a
SQLAlchemy `connect` listener for pooled connections and directly for raw
connections. Timeouts are logged as `QUERY_TIMEOUT | configured_timeout_seconds=...`.
Note: 120 seconds may be too low for some Fabric statements against very
large tables; it is a configuration value.

### Concurrency

`comparison_settings.large_table_sampling.max_concurrent_comparisons` (1) gates
how many sampled comparisons hold a sample and its cohort at once. Table
parallelism is unchanged for everything else; small tables never take the gate.
Wait time is recorded as `gate_wait_seconds`.

### Memory bound

Per sampled comparison: the source sample (<= sample_size rows), the retained
target cohort (<= sample_size rows for keyless, <= keys x multiplier for keyed),
one candidate frame in flight, and one statement of at most
`max_sql_length_bytes`. Frames are joined once; nothing is concatenated
cumulatively.

## Instrumentation

Every sampled comparison logs `[SAMPLE_LOOKUP]` per batch, `[SAMPLE_FETCH]`
after the fetch and `[SAMPLE_PERF]` after the comparison, and records the same
fields under `details["sampling_performance"]`: table_id, sample_size, keyed,
compare_columns, lookup_strategy, lookup_batch_size, lookup_batches,
target_statements, max_sql_bytes, safety_splits, fallback_batches,
sql_generation_seconds, source_query_seconds, target_execute_seconds,
target_extract_seconds, hash_seconds, comparison_seconds, gate_wait_seconds,
candidate_rows, peak_frame_rows, memory_before_mb, memory_after_mb,
total_seconds, discriminator_columns, discriminator_selectivity. Through
`pd.read_sql` execution and extraction are one measurement; when the connector
streams (`execute_query_batches`) the first frame is execution and later frames
are extraction.

## Configuration added (`config/validations/comparison_settings.yml`)

```yaml
large_table_sampling:
  max_concurrent_comparisons: 1
comparison_tiers:
  target_key_lookup_batch_size: 5000        # existing
  target_keyless_lookup_batch_size: 5000
  keyless_discriminator_columns: 3
  keyless_candidate_row_multiplier: 4
query_safety:
  max_sql_length_bytes: 5000000
  max_predicates_per_query: 5000
```

## Measured results (Python side, fake connections, 2026-09-07)

Database execution is excluded (no database in the harness); everything else
is the real code path through `validate`.

| Scenario | Statements | Max SQL bytes | Rows / statement | SQL generation | Hashing | Comparison | Total | RSS before -> after |
|---|---|---|---|---|---|---|---|---|
| Keyed, 100K sampled keys, 3 compare columns | 20 | 46,101 | 5,000 | 0.47 s | n/a | 1.6 s | 9.0 s | 139 -> 162 MB |
| Keyless, 100K sampled rows, 124 compare columns | 20 | 130,403 | 5,000 | 0.80 s | 145 s | 146 s | 323 s | 340 -> 676 MB |

Before the fix the keyless case rendered one statement of roughly 750 MB
(12.4 million comparisons) and the guard alone needed about 6 GB for its
character list; the keyed case rendered one statement of 100K OR-predicates.

The keyless hashing cost is the pre-existing canonical row hash
(`_hash_canonical_value` per cell, 12.4 million cells per side, hashed once
to trim the cohort and once in the multiset comparison); it is linear and
bounded but single-threaded, and is the dominant remaining Python cost for
very wide keyless tables.

## Batch-level parallel target lookups (2026-09-07, second pass)

The bounded batches above were executed strictly one after another. They are
now executed by a bounded worker pool inside one sampled comparison
(`RecordComparisonValidator._run_lookup_batches`):

```
source sample (100K)  ->  20 x 5K lookup batches (stable index 0..19)
                          |
                          v  bounded window = target_lookup_parallelism (4)
                     batch 0 batch 1 batch 2 batch 3   in flight
                          |
                          v  consumed strictly in index order
                     incremental processing -> exact compare
```

* **Configuration** (`comparison_settings.comparison_tiers`):
  `target_lookup_parallelism` (4), optional
  `target_lookup_parallelism_by_connector` keyed by the target connector type,
  and `adaptive_parallelism {enabled, min_workers, reduce_factor}`. When the
  key is absent the existing
  `performance.parallel_execution.batch_parallelism.max_workers_per_table`
  is used if that block is enabled, otherwise 1. The effective value is capped
  by the target pool (`connection_management.pool.pool_size + max_overflow`),
  by `parallel_execution.global_max_concurrency` (the connectors' governor)
  and by the batch count; every cap is logged as `[SAMPLE_PARALLELISM]`.
* **Two concurrency levels stay separate**:
  `large_table_sampling.max_concurrent_comparisons` (table level, 1) decides
  how many sampled comparisons hold a sample at once;
  `target_lookup_parallelism` (batch level, 4) decides how many lookups run
  inside one of them.
* **Determinism**: a batch is submitted only when fewer than `workers` are in
  flight; results are consumed in batch order whatever the completion order.
  Keyed: frames are appended in batch order and joined once (bounded by
  `keys x keyless_candidate_row_multiplier`). Keyless: each worker hashes and
  pre-filters every streamed candidate frame against the fixed source
  multiset and releases it; the consuming thread applies the cross-batch
  multiplicity cap in order. Each discriminator tuple belongs to exactly one
  cohort (`_keyless_cohort_batches`), so a physical target row is fetched by
  exactly one lookup; this also fixed a pre-existing double count when a
  duplicated source row straddled two batches.
* **Failure**: a failed batch cancels the pending ones, awaits the running
  ones and fails the comparison; partial counts are never reported. The log
  line is `Target lookup failed | table= | validation= | strategy= |
  batch=i/n | rows= | sql_bytes= | elapsed= | error_type= | error=` with the
  driver message only. A statement timeout or resource-pressure error shrinks
  the window (`[ADAPTIVE_PARALLELISM]`, e.g. 4 -> 2) and retries that batch
  once; any other deterministic error is not retried.
* **Retry policy**: `_execute_batch_query` retries only transient connection
  failures. `core.connection_manager._read_sql_dataframe` does the same and
  no longer retries a statement timeout unless
  `performance.query_execution.retry.retry_on_timeout: true`
  (`timeout_max_attempts` bounds it). Driver exceptions are re-raised as
  `DatabaseQueryError` whose text never contains the statement.
* **Timing**: `sampling_performance` now carries `batch_count`,
  `parallel_workers`, `configured_parallelism`, `parallelism_source`,
  `parallelism_caps`, `max_inflight_batches`, `target_batch_wall_seconds`
  (wall time of the batches), `batch_elapsed_sum_seconds` (sum of measured
  per-batch durations), `connection_wait_seconds` and `db_execution_seconds`
  (from the connector's per-thread `last_query_timing`),
  `adaptive_parallelism_events`, `source_fetch_seconds`,
  `total_record_comparison_seconds`, `peak_memory_mb`. The orchestrator
  measures the whole table with `time.perf_counter()`
  (`TableValidationResult.measured_duration_seconds`,
  `execution_metrics.table_duration_seconds`, `table_timing_breakdown`) and
  closes the clock before the completion log reads it.
* **Row counts**: every consumer goes through the single-flight
  `("row_count", role)` cache key (the null validator's per-side total now
  included), so one `COUNT_BIG(*)` per table/side/phase is issued even when
  validators run concurrently.
* **Console safety**: validator and connector error logs carry
  `error_type` and the sanitized driver message (`sanitize_error_text`), never
  the statement, row values or a traceback at ERROR level (available at
  DEBUG); check messages produced from exceptions are sanitized too.

* **Configuration actually reaching the validator**: `config/validations`
  contains both `comparison_settings.yml` (nested
  `validations.comparison_settings`) and two legacy documents
  (`config_record_comparison_production.yml`, `..._refactored.yml`) with a
  top-level `comparison_settings` block. `ConfigLoader._parse` replaced the
  nested block with the top-level one, so `comparison_tiers`, `query_safety`
  and `large_table_sampling.max_concurrent_comparisons` never reached the
  validator (it ran on its module defaults, and would have run the lookups
  sequentially). The loader now deep-merges the two: a key declared in both
  keeps the legacy top-level value (behaviour unchanged, e.g.
  `full_data_comparison.batching.retry_failed_batch: false`), keys declared
  only in the nested block become effective. Pinned by
  `tests/test_parallel_target_lookup.py::TestRuntimeConfiguration`.

Benchmark: `python benchmark_parallel_target_lookup.py --rows 100000
--latency 1.0` runs the real `validate` path sequentially and in parallel
over fake connections with a simulated per-statement latency and asserts the
two results are identical.

### Measured (2026-09-07, 100K sample, 5K batches, 20 batches, 1.0 s simulated latency per lookup statement, 12-core host)

| Scenario | workers | max in flight | target batch wall | sum of batch durations | hash | comparison | total record comparison | peak RSS | identical result |
|---|---|---|---|---|---|---|---|---|---|
| Keyed, 3 compare columns, sequential | 1 | 1 | 24.5 s | 24.5 s | 0 | 7.3 s | 38.1 s | 281 MB | - |
| Keyed, 3 compare columns, parallel | 4 | 4 | **8.7 s** (2.82x) | 34.7 s | 0 | 6.6 s | 20.6 s | 293 MB | yes (99,985 matched / 10 mismatched / 5 missing in target) |
| Keyless, 24 compare columns, sequential | 1 | 1 | 127.1 s | 126.7 s | 181 s | 166 s | 394.6 s | 526 MB | - |
| Keyless, 24 compare columns, parallel | 4 | 4 | **107.2 s** (1.19x) | 422.0 s | 441 s (GIL-inflated per-thread sum) | 166 s | 370.6 s | 549 MB | yes (99,995 matched / 5 missing in target) |

The keyed lookups are latency-bound and gain almost the full worker count.
The keyless lookups are bound by the pre-existing Python canonical row hash
(every candidate cell is hashed on the worker, then both sides are hashed
again in `_compare_by_row_hash`); only the database wait overlaps, so the
gain is the 20 s of latency. The hashing cost is unchanged by this work and
is the remaining lever for wide keyless tables. Memory stays bounded: the
parallel run holds at most `workers` streamed frames more than the sequential
one (+12 MB keyed, +23 MB keyless).

### Measured after the third pass (same benchmark, 100K sample, 1.0 s simulated latency)

| Scenario | workers | target batch wall | hash | comparison | total record comparison | peak RSS | identical result |
|---|---|---|---|---|---|---|---|
| Keyed, sequential | 1 | 25.2 s | 0 | 7.7 s | 40.0 s | 282 MB | - |
| Keyed, parallel | 4 | **8.8 s** (2.87x) | 0 | 6.6 s | 21.7 s | 292 MB | yes |
| Keyless 24 columns, sequential | 1 | 44.4 s | 21.1 s | 1.3 s | **69.0 s** (was 394.6 s) | 465 MB | - |
| Keyless 24 columns, parallel | 4 | **22.5 s** (1.97x) | 37.3 s (per-thread sum) | 1.3 s | **45.0 s** (was 370.6 s) | 488 MB | yes |

## Third pass (2026-09-07): compact keyless fallback, cached count failures

* **Wide row-predicate fallback removed.** A keyless cohort whose
  discriminator candidates exceed the budget no longer falls back to the
  per-row all-column predicate form (`_build_signature_cohort_queries` is no
  longer used by the sample path). Instead the discriminator is widened
  progressively (`_discriminator_tiers`: base 3 columns, then 6, then 12 by
  default; `comparison_tiers.keyless_discriminator_max_columns`), each tier
  still one compact VALUES join on a few columns and a prefix-superset of the
  previous tier, so cohort ownership by base tuple keeps each physical target
  row fetched by exactly one cohort. The widest tier streams every candidate
  without a budget; retention stays bounded by the source multiplicity (one
  streamed frame per worker). Floating-point and very long text columns are
  never used as database-side filters. Nothing is truncated, skipped or
  assumed matched.
* **Hash work halved.** `_hash_frame` memoizes the canonical token per
  distinct `(type, value)` for `str`/`int`/`bool`/`bytes` within a column
  (identical byte stream), and the final `_compare_by_row_hash` reuses the
  hashes the cohort already computed for both sides when the column lists and
  row counts line up.
* **Count failures are cached.** `ExecutionResultCache.get_or_compute(...,
  cache_failures=True)` / `put_failure` store the first failure under the key;
  later consumers raise `CachedFailure` (`error_class` statement_timeout /
  transient_connection / database_error) instead of re-issuing the statement.
  `SharedStatistics` reports a database failure of its scan through
  `failure_writers`, wired by the orchestrator to `("row_count", role)`. Row
  count, edge case, audit, null validation and record comparison all read the
  count through `BaseValidator._row_count_once`, so `COUNT_BIG(*)` runs at most
  once per table/side/phase, including when it times out.
* **pandas error echo.** `sanitize_error_text` now reaches the chained driver
  error behind pandas' `Execution failed on sql '<statement>': ...` wrapper and
  cuts any surviving SELECT list / VALUES fragment; `error_class_of` labels
  failures for logs (`Target lookup failed ... error_class=statement_timeout`).

## Known limitations

* If every compared column of a keyless table has NULLs in the sample, a
  discriminator column with NULLs is used; an all-NULL VALUES column has no
  type on T-SQL engines and the join may fail for text columns. The batch then
  fails with a framework error rather than a wrong result.
* Database-side equality on the discriminator columns must agree with the
  Python comparison; string, integer, decimal and temporal literals are
  rendered by the existing `_sql_literal` (non-ASCII text now as `N'...'`).
  Floating-point columns are avoided as discriminators.
* `details["target_sql"]` lists the first three statements plus a count when a
  lookup ran many statements (previously every statement was echoed).
