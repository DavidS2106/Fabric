Implementation status: **partial**.

Follow-up review: **still incomplete**. Cases C and D have not received verified
replacement execution, so their safety guards remain. This is remaining
engineering work, not a claim that deployment configuration alone will enable
them. No successful C/D execution tests have been added.

Changes in this follow-up:

* `RecordComparisonValidator.validate`: do not issue `_count_incremental_cdc`
  after sample mode is selected. Its aggregate query scans the complete scoped
  CDC population and therefore violated the requested sample-mode restriction.
  Previously established operation evidence can still be combined by the
  finalizer, but there is no production evidence-provider interface wired to
  supply such evidence for an unsampled identity. That remains unimplemented.
* `OperationClassifier._classify_current` and `_classify_source_only`: an empty
  history lookup without reliable CDC evidence yields UNKNOWN, not INSERT.
* `HashOperationClassifier.finalize`: matching non-replayed hashes remain
  UNKNOWN because this classifier cannot establish retained-history sufficiency.
* Restored the original explicitly configured `fallback.no_watermark_no_pk:
  full_hash`. The prior change to `error` was not needed to implement cohorts.
  Resolved target-audit loads still encounter the safety guard; this setting
  does not bypass that guard. Explicit invalid watermark handling is unchanged.
* `tests/test_incremental_regressions.py`: added sample-mode aggregate-scan
  prevention tests for either side reaching the threshold, and a hash-history
  sufficiency regression; updated the empty keyed-history expectation.

Latest verification: `python -m pytest -q` reports **62 passed** (local fixtures
and mocks, including one SQLite aggregate test; no live connector execution).
The 58-test result below describes the earlier implementation. No integration
test files were found beyond this regression suite. Git was unavailable on PATH
and no `.git` directory exists in the workspace; a Git diff could not be obtained.
Current code was reviewed against the preceding patch history and this report.

Exact outstanding contracts/evidence:

* Case C needs an implementation connecting existing bounded PK counterpart
  lookups to the comparable source population used by shared statistics,
  comparison, sampling and coverage. The lookup primitives exist; that
  integration is engineering work still to be done.
* Case D needs both a permitted source counterpart method and evidence defining
  comparable occurrence counts. Existing sample counterpart retention is capped
  by the driving sample's multiplicity; it does not establish independent
  expected-load multiplicity. Reusing that cap as a full-load count would hide
  missing occurrences. No configured expected-source load population was found.
* `_normalize_table_cdc` currently accepts only `enabled`, `operation_column`,
  `operation_values`, and `sequence_column`. There is no configured event identity,
  before/after mapping, pair identity, or independent CDC window adapter. A
  sequence column alone cannot define the requested event semantics.
* No history-retention completeness or retained-snapshot contract/provider was
  found in the incremental resolver/classifiers. Deployment must identify an
  authoritative retained snapshot or CDC before-image source and its coverage;
  code must then integrate that evidence. An empty query is insufficient.
* The real target audit column remains unset and audit discovery remains disabled.
  Live verification needs the target column, batch semantics, resolved runtime
  boundaries, and accessible source/target test datasets. These are distinct
  from the missing executor and CDC integration described above.

The examples and earlier method inventory below must be read with these
follow-up corrections: sample mode no longer discovers CDC operations by a new
aggregate scan, and empty history cannot confirm INSERT.

The requested four-case implementation is not complete. This patch fixes
incremental configuration, classification and safety defects in the existing
framework. Target-driven source cohort execution remains unsupported and is
explicitly blocked before unfiltered source statistics or comparison can run.

Framework review

* Runtime class: `validators.record_comparison_validator.RecordComparisonValidator`,
  exported by `validators/__init__.py` and selected by `core/validation_registry.py`.
  The production/refactored alternatives are not the runtime implementation.
* Existing functionality reused: resolved parameterized scopes, dual keyset merge,
  streaming hash spill and occurrence reconciliation, key quality fallback,
  bounded counterpart queries, operation classifiers, evidence records and limits,
  configured policy outcomes, and durable incremental checkpoints.
* Existing sampling already uses `max(source_count, target_count) >= threshold`.
  The configured threshold remains 1,000,000 and sample size/algorithm are unchanged.
* The prior target-audit path scanned the unrestricted source, then suppressed
  unrelated source rows. Its unrestricted source count cannot represent the
  current target load. Blocking that path prevents misleading coverage.
* Existing keyless CDC classification does not associate events with before/after
  business rows. Aggregate operation evidence can reject known disallowed events;
  it does not supply missing identity, event ordering or delete application checks.

Modified files and methods

| File | Methods/sections | Reason |
| --- | --- | --- |
| `core/validation_scope.py` | `resolve`, `_table_audit`, `_target_audit_scope`, `_resolve_audit_bound`, `_invalid_boundary_order`, `_cdc_scope`, `_attach_policy` | Central audit metadata; ordered, opt-in boundary discovery; precise timestamp comparison; CDC attachment with explicit watermark; controlled invalid metadata. |
| `core/config_loader.py` | `_normalize_table_target_audit`, `_normalize_incremental_operation_policy` | Reject table-level audit settings; validate the central audit settings and discovery opt-in. |
| `config/validations/incremental_strategy.yml` | Audit settings, auto priority, no-selector fallback | Single audit configuration authority; prefer target audit over key checkpoints; missing selectors default to an error. Explicit full-hash configuration remains available. |
| `core/orchestrator.py` | Table scope resolution, `_commit_incremental_checkpoint` | Read at most two distinct audit boundary values once when permitted; stop unsafe target-driven execution; prevent commits after failed or incomplete classification. |
| `validators/base_validator.py` | `_quote_filter_identifiers` | Quote the centrally resolved target audit identifier only for the target. |
| `validators/incremental_classification.py` | `OperationClassifier._latest`, `_classify_current`, `_classify_source_only`; `HashOperationClassifier.finalize` | Reject unordered history versions; retain replay/update violations for CDC INSERT; use UNKNOWN when history is unavailable/disabled; retain missing-current-load findings. |
| `validators/record_comparison_validator.py` | `validate`, `_incremental_row_counts`, `_count_incremental_cdc`, `_finalize_incremental_result` | Collect necessary incremental counts without enabling row-count validation; aggregate CDC policy evidence even outside samples; stop unsafe target-driven reads; report query errors separately. |
| `tests/test_incremental_regressions.py` | Regression tests | Exercise the corrected behavior and explicitly assert controlled rejection of unsupported target cohorts. |

Configuration

Use the existing flat operation-policy mapping; do not introduce an `outcomes`
subsection. Current outcomes remain insert/unchanged=pass and
update/delete/replay/unknown=fail.

Target audit is deliberately shipped disabled with a null column: the actual
column and whether its distinct timestamps represent batches must be supplied
by the deployment. Configure it only in `config/validations/incremental_strategy.yml`:

```yaml
validations:
  incremental_strategy:
    target_audit:
      enabled: true
      load_timestamp_column: YOUR_ACTUAL_TARGET_AUDIT_COLUMN
      resolution_sources:
        - runtime_metadata
        - checkpoint
      batch_window_resolution:
        enabled: false
        strategy: latest_two_distinct_values
```

Runtime `incremental_metadata` provides `previous_load_boundary` and
`current_load_boundary`. Checkpoints may provide the previous boundary only.
To permit database discovery, explicitly add `audit_values` at the desired
position in `resolution_sources` and enable `batch_window_resolution`.
Enable this only when distinct audit values identify batches.

For no-watermark tables use `incremental.strategy: auto`. Do not put an audit
column in their YAML. Invalid explicit watermark configuration remains an error.
Enabling audit resolution does **not** enable the missing cohort executor.

Tests and verification

* `python -m pytest -q`: 58 passed, 0 failed, 0 skipped in the final recorded run.
* `python -m compileall -q core validators tests`: syntax/import compilation check.
* Central incremental YAML was loaded and passed
  `_normalize_incremental_strategy_config`.
* An initial test run had 7 failures because the classifier fixture omitted the
  existing value-comparison policy. The fixture now loads the real policy YAML.
* Tests use local fixtures, mocks, and a SQLite CDC aggregate execution. They do
  not execute the full orchestration against source/target production connectors.

Covered regression areas include both watermark capability cases; both audit
capability cases' scope resolution and controlled execution rejection; configured
INSERT/UPDATE/REPLAY/UNKNOWN policy; CDC UPDATE without a before-image; DELETE
without a current target row; missing-current-load handling; hash occurrence
shortfall; disabled/unavailable history; audit precedence, empty/one/two audit
values and bootstrap; exact/invalid/timestamp/timezone boundaries; both sides of
the sampling threshold; cached scoped counts; disabled comparison; and failed or
incomplete checkpoint rejection.

Not verified end to end: all four cases' CDC event streams; duplicate/null PK
fallback; actual audit-column metadata failures; large historical populations;
connector parameter/time/memory limits; multiple-event and before/after pairing;
masked Excel/JSON exports; preflight, smoke_test, historical_backfill and full_load
workflows. Tests confirm non-incremental scope delegation remains unchanged,
but are not comprehensive regression coverage for those phases. Reporter models,
hash algorithms, sampling configuration, and evidence limits were not edited.

Illustrative existing report fields

```json
{
  "check_name": "record_comparison",
  "status": "FAIL",
  "details": {
    "comparison_mode": "sample_comparison",
    "operation_classification_performed": false,
    "incremental_operations": {"update": 1},
    "cdc_operation_policy_violations": {"UPDATE_NOT_ALLOWED": 1},
    "classification_unavailable_reason": "sampled comparison: operations are classified only by the full incremental comparison",
    "final_status": "FAIL"
  }
}
```

This is an illustrative subset, not a production result. Here the scoped CDC
aggregate proves an UPDATE exists, but no change-level comparison is claimed.
A matching record with earlier identical values instead receives the existing
REPLAY operation and REPLAY_NOT_ALLOWED policy finding. Missing history yields
UNKNOWN; target-driven execution currently yields CONFIG_ERROR.

Remaining work

1. Integrate bounded target-driven PK lookups with scoped source counting and
   shared-statistics consumers for case C. No source audit predicate or complete
   source-to-target coverage claim is permissible.
2. Establish an explicitly permitted source hash matching/occurrence scope for
   case D. The unrestricted source scan is not an incremental scope.
3. Integrate reliable CDC identity, before/after images, event ordering and
   delete-target semantics into the keyless path. Aggregate rejection alone
   does not fulfill those requirements.
4. Establish history-retention/snapshot evidence when earlier target versions
   were overwritten. An empty historical query cannot prove retention.
5. Exercise the required integration matrix and report/evidence regressions
   before calling this production-ready. No live database validation was run.
