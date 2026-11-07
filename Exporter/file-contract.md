# Darling export file contract

This file says what `darling-export` writes and how a consumer should read it.
It describes contract version 1. Every manifest carries `contract_version`.
A manifest without the field counts as version 1.

The exporter reads the Darling store and writes Parquet files plus one JSON manifest per run.
It does not load the files anywhere. Loading is the consumer's job.

## 1. Layout and file names

Every run covers one store and one window `[from, to)` in UTC.
The exporter writes these files under the destination prefix:

```
<prefix>/<store>/<table>/<table>__<store>__<from>__<to>.parquet
<prefix>/<store>/<table>/<table>__<store>__<from>__<to>__<part>.parquet
<prefix>/<store>/_manifests/<store>__<from>__<to>.json
<prefix>/<store>/_manifests/<store>__<from>__<to>__t<hash>.json
```

- `<from>` and `<to>` use the format `yyyyMMddTHHmmZ`, for example `20260901T0000Z`.
- A table can have more than one file for the same window. Extra files carry a `__<part>` suffix. The manifest lists every file.
- The manifest of a run of every core table has no suffix.
- A run of a subset of tables, or a run with a different dimension origin, adds `__t` and eight hex digits. The digits are the start of the SHA-256 of the sorted table list. A subset run never replaces the manifest of a full run for the same window. Running the same subset again replaces its own manifest.
- The store folder is `<store>`. A destination option can change it to `store=<store>` for Hive-style paths.
- A local destination uses the same relative paths under its folder.

## 2. Delivery

1. The exporter writes every Parquet file first.
2. It writes the manifest last, and only when every table in the run succeeded.
3. A manifest means every file it lists is complete. Read the files that the manifest lists and no others.
4. A file with no manifest is an unfinished or failed run. Ignore it.
5. A re-export of the same store, window and table set overwrites the same file names and the same manifest name.
6. Each manifest entry has `row_count`, `byte_size` and `sha256`. A consumer can check a file against them.

## 3. Manifest fields

A manifest is one JSON object.

| Field | Present | Meaning |
|---|---|---|
| `contract_version` | always | The contract version. First key in the file. |
| `store` | always | The store name from the config. |
| `window.from_utc`, `window.to_utc` | always | The window. `from_utc` is inclusive and `to_utc` is exclusive. |
| `exporter_version` | always | The exporter's version string. |
| `darling_schema_version` | always | The schema version of the store at export time. Can be null. |
| `straggler_warning` | always | The last count of open writing transactions that started before the window's cutoff. 0 means none. The exporter only records it. It never fails a run. |
| `straggler_wait_seconds` | always | Seconds the straggler guard waited. |
| `tables` | always | One entry per table. See below. |
| `skipped_optional` | optional manifests | Optional tables that were asked for and skipped. Each has `table` and `reason`. The reason is `unknown` or `absent`. |
| `failed_optional` | optional manifests | Optional tables that were attempted and failed. Each has `table` and `error`. |

Each entry in `tables` has these fields.

| Field | Present | Meaning |
|---|---|---|
| `table` | always | The table name. |
| `output_file` | always | The Parquet path relative to the store folder. It always uses `/`. |
| `row_count`, `byte_size`, `sha256` | always | Rows, bytes and SHA-256 of the file. |
| `columns` | always | The columns as `{ name, duckdb_type }`. |
| `store_query_elapsed_ms`, `write_elapsed_ms` | always | Time spent reading the store and writing the file. |
| `missing_content_count` | always | Keys whose plan or query text was empty because the content row was already purged. Zero for other tables. |
| `known_keys_skipped` | always | Dimension keys skipped because the local state already holds them. Zero for other tables. |
| `time_column` | always | The column the rows are filtered on. Null for a dimension with no time column of its own. |
| `kind` | always | `window`, `snapshot` or `dimension`. See section 6. |
| `key_columns` | always | The key columns of a dimension. Null for other kinds. |
| `unresolved_keys` | always | Set on `pg_statement_text` only. Keys whose server did not resolve and were dropped. Null for other tables. |
| `feature_digests_parsed`, `feature_digests_skipped`, `oversized_plan_rows`, `unparsed_plan_rows` | plan tables | Counters for plan parsing. |
| `below_threshold_deferred`, `deferred_now_fetched` | `plan_shape` | Counters for the plan fetch gate. Present only when the gate ran. |
| `procedure_plan_reuse_mode` and the `identity_*` counters | `procedure_plan` | Present when plan reuse is `shadow` or `on`. |
| `query_store_source_mode`, `query_store_source` and related fields | `query_store_stats` | Present when the read used the wide or compare source. |
| `policy_version` | optional tables with a column policy | The version of the column policy that was applied. |
| `grain`, `source_table`, `rollup_version` | hourly and daily optional tables | The grain, the source table and the rollup version. |
| `read_from`, `read_to` | tables with their own read window | The range actually read. |
| `daily_missing_servers` | daily grain | Servers missing from the expected set. |
| `change_key_columns`, `change_baseline`, `change_rows_suppressed` | change grain | The change key, whether this is a full baseline and the rows suppressed as unchanged. |
| `schema_fingerprint` | when computed | A fingerprint of the source schema, to detect drift. |
| `store_read_ms`, `store_connect_ms`, `window_cache_build_ms`, `pace_sleep_ms`, `parse_ms`, `content_fetch_calls`, `content_rows_fetched`, `content_bytes_fetched` | when measured | A split of the store time. A field that is not measured is left out. |

A field that does not apply to a table is left out or null. New fields can appear in any manifest without a version bump.

## 4. Rows can arrive twice

The same row can show up in more than one file. Causes:

- The fast and hourly families overlap on purpose.
- A window is exported again after a failure or a change in settings.
- A catch-up run covers windows that an earlier run already covered.

A consumer must dedupe on identity.

| Kind | Identity |
|---|---|
| window | The store, the table and the window. Replace every row of a (store, table, window) when a newer file for it arrives. |
| snapshot | The store, the table and the snapshot time. Replace by (store, table, snapshot time). |
| dimension | The key columns in the table below. Keep one row per key. |

The `source_store` column on a row is the store name. It matches the `store` field of the manifest.

## 5. Tables

The two tables below come from `darling-export describe-tables`.
Run that command for the list that matches your build.

Notes that apply to every table:

- Join rows across tables with `source_store` and `server_name`. Never join on `server_id`. The numeric id belongs to one store and does not match across stores.
- Timestamps are UTC and have no time zone.
- A column keeps the unit of the store column it comes from. The exporter does not convert units. Column names end in a unit when there is one, such as `_ms`, `_us`, `_mb`, `_bytes` or `_seconds`.
- A column that was added later in the store can be missing from older files. Treat a missing column as null.

### Core tables

| Table | Kind | Grain | Time column | Key columns |
|---|---|---|---|---|
| `perfmon_stats` | window | event | `collection_time` | none |
| `wait_stats` | window | event | `collection_time` | none |
| `file_io_stats` | window | event | `collection_time` | none |
| `memory_stats` | window | event | `collection_time` | none |
| `cpu_utilization_stats` | window | event | `collection_time` | none |
| `spinlock_stats` | window | event | `collection_time` | none |
| `latch_stats` | window | event | `collection_time` | none |
| `tempdb_stats` | window | event | `collection_time` | none |
| `deadlocks` | window | event | `collection_time` | none |
| `blocked_process_reports` | window | event | `collection_time` | none |
| `collection_log` | window | event | `collection_time` | none |
| `query_stats_hourly` | window | hourly | `hour` | none |
| `procedure_stats_hourly` | window | hourly | `hour` | none |
| `query_store_stats` | window | event | `interval_start_time_utc` | none |
| `plan_shape` | dimension | event | `last_seen` | `source_store`, `server_name`, `query_plan_hash` |
| `query_text` | dimension | event | `last_seen` | `source_store`, `server_name`, `database_name`, `query_hash` |
| `procedure_plan` | dimension | event | `last_seen` | `source_store`, `server_name`, `database_name`, `schema_name`, `object_name`, `shape_hash` |
| `system_health_cpu` | window | event | `collection_time` | none |
| `system_health_system` | window | event | `collection_time` | none |
| `system_health_memory` | window | event | `collection_time` | none |
| `system_health_query_processing` | window | event | `collection_time` | none |
| `system_health_io` | window | event | `collection_time` | none |
| `system_health_io_pending` | window | event | `collection_time` | none |
| `system_health_errors` | window | event | `collection_time` | none |
| `system_health_waits` | window | event | `collection_time` | none |
| `system_health_memory_broker` | window | event | `collection_time` | none |
| `system_health_memory_node_oom` | window | event | `collection_time` | none |
| `plan_features` | dimension | event | `collection_time` | `source_store`, `plan_digest`, `statement_ordinal`, `feature_version` |
| `plan_warnings` | dimension | event | `collection_time` | `source_store`, `plan_digest`, `statement_ordinal`, `feature_version`, `warning_seq` |
| `plan_missing_indexes` | dimension | event | `collection_time` | `source_store`, `plan_digest`, `statement_ordinal`, `feature_version`, `index_seq` |

The `_hourly` core tables hold one row per server and key per hour.
The `system_health_*` tables are parsed from the SQL Server `system_health` session.
`plan_features`, `plan_warnings` and `plan_missing_indexes` hold features that the exporter parses from the plan XML in `plan_shape` and `procedure_plan`.

### Optional tables

Optional tables are off unless the config or the command line names them.

An optional table is exported the same way wherever it is named: `--also`, `optional_tables` in the config, `--tables` on a manual run, or `families.fresh.tables`. It gets its column policy, its schema gate and its chunk guard, and its entry is in the optional manifest, never in the core manifest. A run whose `--tables` names only optional tables writes an optional manifest and no core manifest.

| Table | Kind | Grain | Time column | Key columns |
|---|---|---|---|---|
| `plan_correction` | window | change | `collection_time` | none |
| `default_trace_events` | window | event | `collection_time` | none |
| `memory_grant_stats` | window | hourly | `hour` | none |
| `plan_cache_stats` | window | event | `collection_time` | none |
| `session_stats` | window | event | `collection_time` | none |
| `session_summary_stats` | window | hourly | `hour` | none |
| `waiting_tasks` | window | event | `collection_time` | none |
| `dmv_blocking_snapshots` | window | event | `collection_time` | none |
| `memory_clerks` | window | event | `collection_time` | none |
| `memory_pressure_events` | window | event | `collection_time` | none |
| `cpu_scheduler_stats` | window | hourly | `hour` | none |
| `job_history` | window | event | `collection_time` | none |
| `index_object_stats` | window | daily | `collection_time` | none |
| `database_size_stats` | window | daily | `collection_time` | none |
| `database_states` | window | change | `collection_time` | none |
| `ag_database_replica_states` | window | change | `collection_time` | none |
| `ag_replica_states` | window | change | `collection_time` | none |
| `pvs_stats` | window | event | `collection_time` | none |
| `query_store_health` | window | change | `capture_time` | none |
| `server_config` | window | change | `capture_time` | none |
| `server_properties` | window | change | `collection_time` | none |
| `database_config` | window | change | `capture_time` | none |
| `database_scoped_config` | window | change | `capture_time` | none |
| `trace_flags` | window | change | `capture_time` | none |
| `analysis_findings` | window | event | `analysis_time` | none |
| `config_alert_log` | window | event | `alert_time` | none |
| `incident_occurrences` | window | event | `occurred_at` | none |
| `servers` | snapshot | event | `snapshot_time` | none |
| `query_snapshots` | window | event | `collection_time` | none |
| `pg_autovacuum_stats` | window | daily | `collection_time` | none |
| `pg_blocking_edges` | window | event | `collection_time` | none |
| `pg_column_stats` | window | daily | `collection_time` | none |
| `pg_cpu_utilization` | window | hourly | `hour` | none |
| `pg_database_size_stats` | window | daily | `collection_time` | none |
| `pg_database_stats` | window | hourly | `hour` | none |
| `pg_deadlocks` | window | event | `collection_time` | none |
| `pg_extension_availability` | window | event | `collection_time` | none |
| `pg_index_bloat` | window | daily | `collection_time` | none |
| `pg_index_usage_stats` | window | daily | `collection_time` | none |
| `pg_io_stats` | window | hourly | `hour` | none |
| `pg_lock_stats` | window | event | `collection_time` | none |
| `pg_log_events` | window | event | `collection_time` | none |
| `pg_plan_capture_readiness` | window | event | `collection_time` | none |
| `pg_replication_slot_stats` | window | hourly | `hour` | none |
| `pg_replication_stats` | window | hourly | `hour` | none |
| `pg_server_config` | window | change | `collection_time` | none |
| `pg_session_states` | window | event | `collection_time` | none |
| `pg_statement_stats` | window | hourly | `hour` | none |
| `pg_table_bloat_stats` | window | daily | `collection_time` | none |
| `pg_wait_stats` | window | hourly | `hour` | none |
| `pg_wraparound_stats` | window | daily | `collection_time` | none |
| `pg_write_stats` | window | hourly | `hour` | none |
| `pg_xmin_horizon` | window | hourly | `hour` | none |
| `pg_statement_text` | dimension | event | none | `source_store`, `server_name`, `queryid` |

A table with grain `hourly` writes its rollup to `<table>_hourly` and no raw rows. The tables that do this are: `ag_database_replica_states`, `cpu_scheduler_stats`, `memory_grant_stats`, `pg_cpu_utilization`, `pg_database_stats`, `pg_io_stats`, `pg_replication_slot_stats`, `pg_replication_stats`, `pg_statement_stats`, `pg_wait_stats`, `pg_write_stats`, `pg_xmin_horizon`, `session_summary_stats`.

A table with grain `daily` writes the last complete collection cycle per server before 00:00 UTC. It is written only in the run whose window starts at 00:00 UTC.

A table with grain `change` writes a row only when the tracked columns changed since the last exported row. The first export is a full baseline and its manifest entry has `change_baseline` set to true. The change keys are:

| Table | Change key columns |
|---|---|
| `plan_correction` | `server_name`, `database_name`, `recommendation_name` |
| `database_states` | `server_name`, `database_name` |
| `ag_database_replica_states` | `server_name`, `ag_name`, `database_name`, `replica_server_name`, `group_id` |
| `ag_replica_states` | `server_name`, `ag_name`, `replica_server_name`, `group_id` |
| `query_store_health` | `server_name`, `database_name` |
| `server_config` | `server_name`, `configuration_name` |
| `server_properties` | `server_name` |
| `database_config` | `server_name`, `database_name` |
| `database_scoped_config` | `server_name`, `database_name`, `configuration_name` |
| `trace_flags` | `server_name`, `trace_flag` |
| `pg_server_config` | `server_name`, `name`, `database_name`, `role_name` |

### Units of the main numeric columns

These are the units the store uses. The files keep them.

| Table | Column | Unit |
|---|---|---|
| `query_stats_hourly`, `procedure_stats_hourly` | `total_elapsed_time`, `total_worker_time` (and their rollups) | microseconds |
| `query_stats_hourly`, `procedure_stats_hourly` | `execution_count` | count |
| `query_store_stats` | `max_duration_us`, `max_cpu_time_us` | microseconds |
| `query_store_stats` | `execution_count` | count |
| `wait_stats` | `wait_time_ms`, `signal_wait_time_ms`, `delta_wait_time_ms` | milliseconds |
| `wait_stats` | `waiting_tasks_count` | count |
| `file_io_stats` | `read_bytes`, `write_bytes` | bytes |
| `file_io_stats` | `io_stall_read_ms`, `io_stall_write_ms` | milliseconds |
| `cpu_utilization_stats` | `sqlserver_cpu_utilization`, `other_process_cpu_utilization` | percent |
| `memory_stats` | columns ending in `_mb` | megabytes |
| `tempdb_stats` | columns ending in `_mb` | megabytes |
| `latch_stats` | `wait_time_ms` | milliseconds |
| `blocked_process_reports` | `wait_time_ms` | milliseconds |
| `perfmon_stats` | `cntr_value`, `delta_cntr_value` | the unit of the counter. See `counter_name`. |
| `memory_grant_stats`, `memory_clerks`, `plan_cache_stats`, `database_size_stats` | columns ending in `_mb` | megabytes |
| `query_snapshots` | `cpu_time_ms`, `total_elapsed_time_ms` | milliseconds |
| `query_snapshots` | `granted_query_memory_gb` | gigabytes |
| `waiting_tasks` | `wait_duration_ms` | milliseconds |
| `job_history` | `run_duration_seconds` | seconds |
| `ag_database_replica_states` | `log_send_queue_size`, `redo_queue_size` | kilobytes |
| `ag_database_replica_states` | `log_send_rate`, `redo_rate` | kilobytes per second |
| `ag_database_replica_states` | `secondary_lag_seconds` | seconds |
| `pg_statement_stats` | `total_exec_time_ms` | milliseconds |
| `pg_wait_stats` | `wait_time_us` | microseconds |
| `pg_replication_stats` | `write_lag_ms`, `flush_lag_ms`, `replay_lag_ms` | milliseconds |
| `pg_io_stats`, `pg_table_bloat_stats`, `pg_index_bloat`, `pg_replication_slot_stats` | columns ending in `_bytes` | bytes |
| `pg_session_states` | `state_duration_ms`, `xact_duration_ms`, `query_duration_ms` | milliseconds |
| `pg_cpu_utilization` | `cpu_percent`, `acu_utilization_percent` | percent |

Counter columns in cumulative tables grow until the source restarts. Rows in a `*_hourly` rollup have `_first`, `_last` and `_delta_in_hour` columns for a counter. A drop in a counter counts as a reset, and the delta then uses the value after the reset.

The Parquet types follow the store types. Exact numerics with a precision and scale stay `DECIMAL(p,s)`. Unconstrained numerics become `DOUBLE`. The manifest lists the DuckDB type of every column.

## 6. Kinds and grains

Kinds:

- **window**: rows with a time column, filtered to `[from, to)`.
- **snapshot**: a table with no time column of its own. The exporter stamps `snapshot_time` on each row. It is written once a day, in the window that starts at 00:00 UTC.
- **dimension**: a table that holds content or keys, such as `plan_shape` and `query_text`. The exporter writes a key once. It remembers the keys it already wrote in its local state, so a later run does not write them again. The `--ignore-state` flag writes them again.

Grains:

- **event**: every row in the window, unchanged.
- **hourly**: one aggregated row per server and key per hour.
- **daily**: the last complete cycle per server before 00:00 UTC.
- **change**: a row only when the tracked columns changed.

## 7. The schema-change rule and `contract_version`

The version is an integer. The current value is 1.

No bump:

- adding a column
- adding a table
- adding a manifest field

Bump:

- removing, renaming or retyping a column
- changing a column's units or meaning
- changing the key layout or the file names
- changing what a manifest field means

A consumer should check `contract_version`. If the value is higher than the consumer supports, it should stop and leave the manifest unread. It should not guess.

## 8. Cadence and retention

The contract does not say how often to run the exporter or how long to keep files. Both are your choice.

The `run` command has a scheduler for hourly use. See the README. It prunes only its own local output folder, logs and temporary files. It never deletes anything at the destination.
