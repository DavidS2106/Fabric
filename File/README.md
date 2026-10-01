# File → Microsoft Fabric validation

This module adds a **File** source route to the existing migration-validation
framework. It is an ingestion, discovery, routing and file-reconciliation
module only.

```
File module                     Existing framework
-----------                     ------------------
discovery                       validation engine
archive expansion               normalization
header / delimiter resolution   record comparison
format routing                  sampling and hashing
target matching                 audit validation
logical consolidation           evidence
file reconciliation             reporting
```

The datasets this module prepares are validated by
`core.orchestrator.ValidationOrchestrator`, with the profiles in
`config/validations/validation_profiles.yml` and the rules in
`config/validations/*.yml` — the same ones the SQL Server → Fabric and
Fabric → Fabric routes use. Nothing here enables, disables or reimplements a
validation.

## Running

File validation runs as part of the normal entry point, alongside the database
routes:

```bash
python run_validation.py --phase incremental
```

To run one comparison mode on its own, select it. A comparison mode is a
property of a File table only, so a mode run contains no database table:

```bash
python run_validation.py --phase smoke_test          --comparison-mode file_to_file
python run_validation.py --phase historical_backfill --comparison-mode file_to_file
python run_validation.py --phase incremental         --comparison-mode file_to_file
python run_validation.py --phase full_load           --comparison-mode file_to_file

python run_validation.py --phase smoke_test          --comparison-mode files_to_table
python run_validation.py --phase historical_backfill --comparison-mode files_to_table
python run_validation.py --phase incremental         --comparison-mode files_to_table
python run_validation.py --phase full_load           --comparison-mode files_to_table
```

`--phase`, `--table-ids`, `--tags`, `--checks` and `--skip-checks` apply to
File tables exactly as they do to database tables. Set
`file_validation.enabled: false` in `File/config/config_setting.yml` to switch
the capability off; a deployment without a `File/config` folder never enters
this code at all.

## How a File dataset reaches the existing validators

A file is not a table, and every validator in this framework reads its data
with SQL. So each prepared dataset — one file in `file_to_file`, one logical
dataset in `files_to_table` — is streamed, in bounded chunks, into a local
relational store, and exposed through an ordinary
`core.connection_manager.BaseConnector` (`FileDatasetConnector`) that renders
its statements through the `sqlite` profile in `config/dialects/dialects.yml`.

From the validation engine's point of view a File dataset is just another
configured connection. That is what makes row count, key validation, null
validation, duplicate detection, record comparison, sampling, hashing, audit
validation, evidence and reporting work unchanged.

Nothing is ever concatenated in memory: the reader yields at most
`staging.chunk_rows` rows at a time, all the way from the file to the store.

## Configuration

| Document | Owns |
| --- | --- |
| `config/config_setting.yml` | global File behaviour: supported formats, universal delimiters, discovery, archives, headers, staging, output |
| `config/connection/source.yml` | the local file root |
| `config/connection/target.yml` | the Lakehouse file target, and a *reference* to the project's existing Fabric SQL connection |
| `config/table/*.yml` | one logical table each: phase → folder, wildcard, format, options, keys, target |

Environment variables live in `File/config/connection/.env` (see
`.env.example`). The Fabric SQL details are **not** repeated here: they live in
`config/connections/targets.yml` under the name `fabric`, driven by the
`FABRIC_SQL_*` variables in `config/connections/.env`, and `target.yml` reaches
that connection with `connection_reference: fabric`. A File table document only
ever writes `connection: fabric`.

### Keys are optional

`key_mappings: []` — or no `key_mappings` at all — is valid for both modes. No
column is ever assumed to be a key, and no mapping is ever generated.

* **With** configured key mappings, the existing keyed comparison runs,
  unchanged.
* **Without** them, the existing engine falls back to its canonical complete-row
  signature comparison, which counts occurrences (`hash_comparison.
  duplicate_handling: multiset`). Duplicate identical rows are therefore
  reconciled rather than collapsed: a row present twice in the source and once
  in the target is reported as excess in source, and the dataset fails.

Both paths are the framework's own; the File module chooses neither. It only
supplies the comparable business columns, which are the source columns minus
the target audit columns configured in
`config/validations/audit_validation.yml`.

### Phases

`source.paths` maps a phase to the folder that phase reads:

```yaml
paths:
  historical_backfill: archive/customer
  incremental: incremental/customer
```

`--phase incremental` reads `incremental/customer` and nothing else. A phase
with no entry is a phase this table does not participate in.

### Comparison modes

`file_to_file` — every discovered source file is resolved to exactly one
Fabric Lakehouse file and validated on its own. A wildcard discovers many
files; it never merges them.

`files_to_table` — every discovered file contributes to one logical dataset,
compared against one Fabric SQL table. Schema compatibility is proved before a
row is staged.

### Delimiters

No delimiter is assumed. State one in `source.file.options.delimiter`, or let
the reader detect it from the universal candidate list in
`file_validation.delimiters.candidates` (comma, pipe, tab, semicolon, colon,
caret, tilde, hash, at, asterisk, space and the ASCII separator characters).
Ambiguous or undetectable input fails rather than guessing.

### Headers

`options.header` is `true`, `false` or `auto`. A headerless file may take the
column order of `options.column_names`, or of the first file in the same set
that did carry a header. Columns are never invented, renamed or silently
reordered.

## Target access

A `fabric_lakehouse_file` target is read either through OneLake
(`onelake_account_url` + workspace + lakehouse) or, when
`params.local_mount_path` is set, straight off a mounted lakehouse — which is
how a Fabric notebook sees `/lakehouse/default`.

A `fabric_sql_table` target names a connection in one of two ways:

* `connection_reference: fabric` — the project's own Fabric SQL connection in
  `config/connections/targets.yml`. This is the default and keeps every
  endpoint and credential in one place.
* a connection that declares its own `params.sql_endpoint` (plus `database`
  and `driver`). The shipped `fabric_lakehouse_file` connection carries these,
  so one lakehouse definition can serve both its OneLake files *and* its SQL
  analytics endpoint instead of being declared twice. Leave the
  `FABRIC_RAW_SQL_ENDPOINT` / `FABRIC_RAW_DATABASE` / `FABRIC_RAW_DRIVER`
  variables unset and the connection stays purely file-based.

## Output

```
File/output/
├── reports/
│   ├── <Phase>/<run>/{Excel,JSON,HTML}/   the existing report tree
│   ├── excel/  json/  html/               consolidated reports, filed by format
├── evidence/<TABLE_ID>/                   the per-table workbooks and payloads
└── reconciliation/                        file-level metadata (JSON, CSV, Excel)
```

The reports are produced by the existing reporters; this module only relocates
copies into the folders above and writes the reconciliation export.

## Known limitations

* **Local staging cost.** Reusing the validation engine means file content is
  materialized into a local SQLite store first. Memory stays bounded, but disk
  use is roughly the size of the data being validated. Point
  `file_validation.staging.directory` at a volume with room, and set
  `retain_after_run: true` only when debugging.
* **Spreadsheets are parsed whole.** `openpyxl` has no streaming reader here,
  so an `.xlsx` sheet is materialized by its engine before being handed on in
  bounded chunks. Delimited, JSON-lines and Parquet inputs stream end to end.
* **Audit validation on raw Lakehouse files.** `audit_validation` is enabled
  for `historical_backfill`, `incremental` and `full_load`, and it looks for
  the columns in `config/validations/audit_validation.yml`. A raw Lakehouse
  file usually has none, so those phases report an audit failure for a
  `file_to_file` table. That is the framework's configured behaviour and this
  module does not override it; change the shared audit configuration, or use
  `files_to_table` against a curated table, if the audit columns are expected.
* **Keyed record comparison needs key statistics.** When a table *does* declare
  key mappings, `record_comparison` uses the keyed strategy only if
  `key_validation` also ran in the same phase (the `historical_backfill`
  profile disables it, so that phase falls back to the configured hash
  comparison). This is existing, profile-driven behaviour, identical for
  database tables. A table with `key_mappings: []` always takes the row-hash
  path and is unaffected.
* **A T-SQL-only probe logs against non-T-SQL connectors.** The existing
  record comparison discovers its row-hash evidence column with a
  `SELECT TOP 0 *` probe. Against the staged store that statement is rejected,
  the probe treats the column as absent — its documented fallback — and the
  connection layer logs the rejection. Counts, matching and status are
  unaffected.
