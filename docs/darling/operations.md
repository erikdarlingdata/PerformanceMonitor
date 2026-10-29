# Operations

Audience: operators.

This page covers the store, TimescaleDB, the background workers, retention, the logs, the Viewer, restarts and how to watch the service from outside. It moved here from the Darling operator guide.

## The Store

The service migrates the store itself at startup — plain versioned SQL scripts, each applied once inside its own transaction, tracked in `darling_schema_version`, safe under concurrent starters (advisory-locked). `StorageVersion.SchemaVersion` is the source of truth for the schema version, and a test pins it to the highest rung in the ladder. The service logs the version it found at each start (`Postgres store ready (schema v<N>, ...)`).

For the complete schema, read [`Darling/Darling.Tests/Fixtures/migration-ladder-*.sql`](../../Darling/Darling.Tests/Fixtures/). Each file is the whole migration ladder as resolved SQL, as one release shipped it. `Darling/tools/generate-ladder-fixture` writes the file at each release, so do not edit it by hand. Rungs added since the last release are in [`PgMigrations.cs`](../../Darling/PerformanceMonitor.Darling.Storage/PgMigrations.cs).

All timestamps in the store are **naive-UTC** `timestamp` columns — the product-wide cross-store contract (Lite's DuckDB does the same).

To back up, restore or move the store, see [Back up, restore and move the store](../darling-store-backup.md).

## Reading the store directly (plan XML is compressed)

The store is deliberately queryable — it is documented PostgreSQL with named tables, and people build
panels and reports straight off it. One thing will surprise you if you do that: **execution-plan XML is
stored gzip-compressed**, and has been since v3.4.0.

`collect.query_plan_dim` holds plan content once, keyed by a content digest, in one of two columns:

| Column | Meaning |
|---|---|
| `query_plan_gz` (`bytea`) | The plan XML, **gzip-compressed** (magic bytes `1f 8b`). This is where new plans go. |
| `query_plan_xml` (`text`) | Uncompressed plan XML. Nullable since v3.4.0; only rows written by older builds still carry it. |

So a consumer that reads only `query_plan_xml` silently returns nothing for anything collected by a
current build. **`query_plan_xml IS NULL` does not mean "no plan" — it means look at `query_plan_gz`.**

Both apps and every MCP tool decompress client-side, so nothing in the product is affected; this note
exists because the change altered the contract for direct SQL consumers and the v3.4.0 release notes did
not say so. That omission is on us.

**A `query_stats` / `procedure_stats` row can also have no dimension row at all.** Plan XML larger than
512 KB for one row is not captured — see `QueryPlanXmlCaptureLimits` for the measured reason — so that row
carries no digest and there is nothing in `query_plan_dim` to join to. `query_plan_xml_bytes` on the fact
row is what tells those two cases apart: it is the size the monitored server measured, recorded whether or
not the content was kept.

| Fact row | Means |
|---|---|
| `query_plan_xml_bytes IS NULL` | The server had no plan for this row — the handle aged out before the plan was read, or this store's service does not capture plans |
| `query_plan_xml_bytes IS NOT NULL` and a digest is present | Captured; resolve it through `query_plan_dim` as above |
| `query_plan_xml_bytes IS NOT NULL` and no digest | Declined for size. Look in `collect.oversized_plan_backlog` |

`collect.oversized_plan_backlog` is where those plans are eventually collected, by an out-of-band sweep
that fetches one plan at a time, ten per server every fifteen minutes. Its `plan_xml` column is
**plain text, never gzip** — the content never passes through the plan dimension. A row with
`plan_xml IS NULL` has not been fetched yet; `expired_at` means the plan handle stopped resolving before
the sweep reached it. `get_plan_xml` and the `analyze_*_plan` tools read this table automatically, so
asking the product remains the shortest route.

`get_oversized_plan_backlog` reports the table itself rather than one plan out of it: per server the
pending / captured / expired counts, the newest capture and expiry instants, and `observed_bytes`
min/median/max, with an optional per-server row listing that carries each plan's claim key,
`database_name` and `query_hash`. That is where a `query_hash` worth passing to `get_plan_xml` comes
from, and it is also the read that answers whether the sweep's fetch half has ever succeeded on a given
server. `observed_bytes` is the same measurement, in the same UTF-16 bytes, as `query_plan_xml_bytes` on
the fact row — a `length(plan_xml)` in PostgreSQL is CHARACTERS and comes back at half of it.

**Getting the XML back.** PostgreSQL has no built-in gunzip for arbitrary `bytea`, so a plain-SQL
consumer cannot decompress in the database without an extension. Practical options, in the order most
people should try them:

1. **Ask the product for the plan** rather than the store — `get_plan_xml` over MCP, or the Viewer's
   plan surfaces. Both hand back decompressed XML and neither cares how it is stored.
2. **Decompress in your client.** Any language's gzip library reads the bytes directly. Python:
   `gzip.decompress(row['query_plan_gz']).decode('utf-8')`. PowerShell: a `GZipStream` over a
   `MemoryStream` of the bytes. C#: the same, which is exactly what the apps do.
3. **Ship a UDF into your own store** if your tooling is SQL-only (Grafana, a reporting view). A
   `plpython3u` function works and has been used in the field, at the cost of an untrusted-language
   extension in a monitoring database — weigh that against how much you need it.

Why compressed at all: plan XML dominates store size, and gzip took a production dim table from 885 GB
of raw text to 64 GB — a 14x reduction. That is the tradeoff being made on your behalf.

## TimescaleDB (Optional, Auto-Adopted)

At startup, right after migration, the service attempts `CREATE EXTENSION IF NOT EXISTS timescaledb` and checks `pg_extension`:

- **Present** — every collector table is converted to a hypertable (partitioned on its own time column into **1-day chunks**, existing rows migrated) and gets a compression policy: chunks older than **1 day** compress automatically (segmented by `server_id`), checked **hourly**. The hourly tick is passed explicitly because TimescaleDB's own default is **12 hours** for 1-day chunks — that is a second, separate wait *after* a chunk is already eligible, and on a field store it left the newest closed chunk (always the least-compressed data on disk) uncompressed for most of a day. Stores created before this shipped are retuned automatically on the next service start. The short intervals matter at the 1-minute collection cadence — a chunk cannot compress until it closes and then ages, so TimescaleDB's 7-day default left the store fully uncompressed for ~2 weeks (a near-idle 5-server fleet still reached ~1 GB in a couple of days); 1-day chunks + 1-day compress keep it compact (measured ~16.7x on perfmon, ~6.4x on the plan-XML-heavy query_stats). Compressed chunks stay fully queryable — this is Darling's archival tier, the centralized-store answer to Lite's Parquet archive. Everything is idempotent and re-converges on every service start; a table that fails conversion stays a plain table and keeps working.
- **Absent** — the service logs one Information line and runs in plain-PostgreSQL mode, which is a fully supported configuration, not a degraded one.
- **The continuous aggregates compress too** (#3581). The twenty rollups the store materializes — the hourly and daily history tiers, the Query Store dedup layers and the seven anomaly baselines — each get columnar compression on their materialization (segmented by `server_id`, ordered by the bucket column) and a **once-a-day** compression policy of their own. The delay is derived from each tier's refresh window rather than chosen: an hourly-refreshed aggregate compresses chunks older than **2 days** (its refresh re-materializes 1 day back, plus one day of margin), a daily-refreshed one after **4 days** (3 days back, plus the same margin), so a refresh never has to decompress what a policy just compressed. These jobs run **off the hourly phase grid** — one aggregate per hour at `:35` UTC from `01:35` — and on a store that already holds weeks of uncompressed rollups the first runs are **staged one aggregate per night, largest first**, so the backlog pass is one bounded relation a night; a fresh store simply gets all twenty policies at once. Why it matters: an uncompressed rollup of a 9x-compressed raw table is *larger than the raw table* (the hourly grain is only a ~4x row reduction), and on a large store the aggregates were more than half the database. Idempotent under the catalog and re-converged on every start like the raw tier's policies; in `timescaledb_information.jobs` they appear as `policy_compression` rows whose hypertable is `collect.<aggregate view>` with a `1 day` schedule.
- **The materializations are chunked at one day, like the raw tables** (#3620). TimescaleDB's default sizes a rollup's chunks at ten times the raw table's interval, so every aggregate carried 10-day chunks: `drop_chunks` removes only whole chunks, which made the 7-day interval-dedup tier hold anywhere from 7 to 17 days, and a compression policy compresses only whole chunks, which kept the newest ~10 days of every rollup uncompressed whatever its `compress_after`. The same startup ensure now holds every materialization at the raw chunk width (derived from the one constant, so the two tiers cannot drift apart), affecting chunks created from then on; existing wide chunks age out on their own schedule, so a store's short rollups tighten to their designed hold over about two weeks rather than overnight.

`IF NOT EXISTS` short-circuits before privilege checks, so a store whose administrator pre-created the extension works for a service login that could never create it.

## Background workers: sizing an unmanaged store, and what happens if you don't

**This section is for bring-your-own PostgreSQL only.** In managed mode the service sizes these itself on every start and there is nothing to do.

Every TimescaleDB policy — compression, retention, continuous-aggregate refresh — runs in a **background worker**, and a policy that cannot get a worker does not run. PostgreSQL's stock `max_worker_processes = 8` is far below what this store needs, so an unmanaged store left at the defaults silently does very little compressing.

Managed mode sizes the two settings from the host's RAM, bounded by the policy jobs the schema carries, and an unmanaged store wants the same numbers:

```
timescaledb.max_background_workers = clamp(16 * <host RAM in GB>, <hypertables> + 2, <ceiling>)
max_worker_processes               = 3 + timescaledb.max_background_workers + 8
<ceiling> = <hypertables> + 3 * <continuous aggregates> + 2 + 16
```

The ceiling is every policy job the schema can create (one compression policy per hypertable, up to three per continuous aggregate, two store-wide) plus 16 for the scheduler and slack. Count your own store's two inputs with these queries. Run them after the service's first start creates the tables, and again after each upgrade, because a new build can add collector tables. A few small collector tables stay plain tables, such as `attention_events`, which has its own 30-day delete. The first query does not count them.

```sql
SELECT count(*) AS hypertables FROM timescaledb_information.hypertables;
SELECT count(*) AS continuous_aggregates FROM timescaledb_information.continuous_aggregates;
```

For example, take a store with 72 hypertables and 29 continuous aggregates. Its ceiling is 177, and on an 8 GB host the two settings are **128** and **139**. The same store gets 74 and 85 on a host of 4 GB or less (the floor). It gets 177 and 188 on 12 GB or more (the ceiling). Your counts will differ, and they grow as collectors are added, so work the formula with the numbers the queries return. One fully migrated store carried **142 policy jobs** when measured for #5633 item 14: 94 compression, 23 continuous-aggregate refresh, 23 retention, plus TimescaleDB's own `policy_telemetry` and `policy_job_stat_history_retention`. The job query below gives your store's own count. The count depends on the schema (hypertables and aggregates), not on how much data is stored.

After any downtime every job is overdue at the next start, so they all ask for a worker at once. With 74 workers the postmaster log showed 61 to 68 `out of background workers` launch failures on one store at a restart (each job was retried later and ran, so the first pass is noisy and late, not lost).

A slot a restart fills is a postgres backend (about 4 to 6 MB private each, measured on an empty store), so the managed store does not hand a small host more slots than its RAM can hold: raise the numbers toward the ceiling on a larger host, or accept the retries on a small one.

```sql
SELECT proc_name, count(*) FROM timescaledb_information.jobs GROUP BY proc_name;
```

Both settings need a **server restart** (`max_worker_processes` is restart-only — a reload leaves the old value serving), and the hypertable count grows as collectors are added, so re-check it after a major upgrade rather than pinning one pair forever.

**One store per cluster is the assumption.** `timescaledb.max_background_workers` is a **cluster-wide** pool shared by every database, while the derivation above is **per-store**. Managed mode puts one store on one cluster so the two coincide, but if you run **N Darling stores on one PostgreSQL cluster** — or share the cluster with any other TimescaleDB database — multiply both numbers by N. Each database with the extension loaded also permanently holds a scheduler slot out of that same pool, so the sharing starts before any policy fires.

**What under-provisioning looks like.** The postmaster log (`pg.log`, or wherever your cluster logs) is where it shows up, in one of two shapes:

```
WARNING:  failed to launch job 1042 "Columnstore Policy [1042]": out of background workers
WARNING:  ... failed to start a background worker
```

The first means TimescaleDB's own pool is full; the second means PostgreSQL's is. Neither is fatal and neither corrupts anything — the job is skipped and retried on its next schedule, so **light contention is benign** and you may see a couple of these without any consequence. It matters at scale: when the shortfall is persistent rather than momentary, compression falls behind the 1-day policy and the store grows at its uncompressed rate (measured compression is ~16.7x on perfmon and ~6.4x on the plan-XML-heavy `query_stats`, so the gap is large), retention stops reclaiming chunks, and the jobs that keep losing the race are the ones whose backlog is worst. `timescaledb_information.job_stats` is the check that settles it — a healthy store shows successes with no failures:

```sql
SELECT sum(total_runs), sum(total_successes), sum(total_failures) FROM timescaledb_information.job_stats;
```

**Before you read the per-run history by hand, know who it shows itself to.** The view behind those totals — `timescaledb_information.job_history`, one row per run with its start, finish, outcome and error — has two conditions `jobs` and `job_stats` do not. It records successful runs only while `timescaledb.enable_job_execution_logging` is on (the managed conf block turns it on; TimescaleDB writes a *failed* run's row regardless), and it is **ownership-filtered**: its own `WHERE` clause shows a row only to a member of the job's owner role or of the database owner, while `jobs` and `job_stats` show every role every job. So the `admin` and `viewer` roles — and the `mcp` role the MCP host reads as in managed mode — can list all the jobs and every one of their `total_runs`, then read an **empty** `job_history` on a store that is recording perfectly. That is not a broken instrument; it is the filter. The jobs are owned by the role that created them, which is the service's owner (`darling` in managed mode; whatever your connection string names in bring-your-own), so read `job_history` as that role before concluding anything from a zero. `get_store_metrics`' `job_history` block reports which role it read as, whether the view's predicate lets that role see anything (`visibility`), and the rows it actually saw over the last 24 hours beside the jobs that started a run in the same window — on a managed store that block reads as `mcp` and says so. The owner's own count is there too: the service's hourly self-metrics sweep runs on the owner pool, persists what the owner sees in `job_history` over the same 24-hour window into `collect.store_metrics` (`object_kind = 'job_history'`), and the block reports it as `owner_rows_observed` / `owner_newest_row_at` / `owner_observed_at` beside its own verdict — so `recording` is a measurement on a managed store after all, taken by the service's sweep rather than by the connection asking, and the note says which.

## Retention

A purge runs on the first sweep after startup and then daily, and it does not draw from a single source. Every **collector** table takes its horizon from the shared per-collector `CollectorScheduleDefaults` Lite also uses (fleet-wide overrides layer on top); the non-collector tables purged alongside them each carry their own horizon, listed beneath the table.

| Horizon | Collector tables |
|---|---|
| 7 days | `query_snapshots`, `waiting_tasks`, `running_jobs`, `agent_status` |
| 14 days | `pg_plan_capture` |
| 30 days | Most collector tables (wait/query/procedure/Query Store stats, CPU, memory, file I/O, tempdb, perfmon, deadlocks, blocking, sessions, config snapshots), and `pg_log_events` — an event log sized by the target's `log_*` settings rather than by anything rare, so it gets the common horizon rather than `pg_deadlocks`' 90 (#3601) |
| 90 days | `database_size_stats`, `index_object_stats`, `pvs_stats`, and the PostgreSQL bloat/vacuum/replication families (`pg_table_bloat_stats`, `pg_index_bloat`, `pg_index_usage_stats`, `pg_autovacuum_stats`, `pg_replication_slots`, `pg_wraparound_stats`, `pg_deadlocks`) |
| 365 days | `server_properties`, `job_history`, `pg_server_config`, `pg_column_stats`, `pg_extension_availability`, `pg_plan_capture_readiness`, and `pg_database_size_stats` — a size series is asked "since last year" (#3691) |

`CollectorScheduleDefaults.cs` is the authority for that half; the rows above are its distinct horizon values, not a hand-maintained copy of every entry.

**The non-collector tables**, purged on the same daily tick, each for its own reason:

- **`collection_log` — 60 days**, deliberately **2x** the 30-day base window (`DarlingRetention.DataRetentionBaseDays`) rather than equal to it: a collector run-record has to outlive the metric rows it explains. At an equal horizon a failed collection and the data you would diagnose it against expire the same night, which erases the evidence at exactly the moment someone finally goes looking for it.
- **`config_alert_log` — 90 days.** Fired-alert history is low-volume and a useful audit trail, so the horizon is generous, but it is bounded — this purge is the only thing that ever deletes from it.
- **`collect.plan_force_actions` — 365 days** (`DarlingRetention.PlanForceLedgerRetentionDays`), the auto force-plan bot's decision journal and deliberately the longest horizon in the store: it audits a bot *writing* to production servers rather than measuring them, so it has to outlive the metrics that motivated each decision. Append-only, and never a hypertable because `action_id` is an identity primary key its own `related_action_id` points back to, so it purges by bounded `DELETE`.
- **`config.config_command` — 30 days, terminal rows only.** A `pending` or `in_progress` row is **never purged at any age**, because deleting a live command would strand the caller polling it. An abandoned `in_progress` row is the stale-command reaper's problem, not retention's — the reaper marks it failed, which makes it terminal and purgeable — and an ancient `pending` row means the service has not run it yet.
- **`analysis_findings` — 30 days**, purged by `PgFindingStore.CleanupOldFindingsAsync` on the same tick rather than by the sweep above. It is also not a hypertable — it was designed keyless so it could become one, but the conversion is deliberately not made until finding volume warrants it — so it purges by `DELETE` on every store.

On plain PostgreSQL the purge is DELETE-based. With TimescaleDB it switches to `drop_chunks` — a metadata-only detach of whole expired chunks (rows inside a partially-expired chunk survive until the whole chunk ages out; up to ~1 day of grace at the 1-day chunk width), with a per-table DELETE fallback for any table that is not a hypertable. Failure-isolated per table: one stuck purge is logged and retried the next day without stopping the sweep.

### The rollup tiers, on a TimescaleDB store

The table above is the **collector** horizon, and for three tables it is not the binding one. `query_stats`, `procedure_stats` and `query_store_stats` are rolled up into hourly and daily continuous aggregates, and a separate tiered policy drops their raw chunks at **4 days** — the aggregates hold the history past that point, and a read is routed to whichever tier covers the window it asks for. On a store without TimescaleDB none of this exists and the collector horizons above are the whole story.

| Tier | Horizon |
|---|---|
| Raw `query_stats`, `procedure_stats`, `query_store_stats` | 4 days |
| Hourly **history** rollups | 90 days |
| Daily **history** rollups | kept indefinitely (no policy) |
| Baseline aggregates | 35 days |
| `query_store_stats_interval_hourly`, `query_store_stats_interval_daily` | 7 days, 10 days |

Every one of these is visible in `timescaledb_information.jobs`, and the last row is the one worth knowing before you look: those two are **internal dedup plumbing, not history**. The corrected Query Store rollups are built from them, nothing reads them directly, and each horizon is sized only to outlive whatever gates on it — 7 days has to exceed raw's 4, and the 10-day layer has to outlive the 7-day one it consumes. So a horizon SHORTER than the tier above it is correct there and costs no history, which is the opposite of how it reads at a glance. The service's startup summary line names all of these for the same reason.

No raw tier is ever dropped before the aggregate that preserves it has caught up: each policy is created paused, and arms itself only once its rollup demonstrably covers what the tier below holds.

## Logs

The service's PRIMARY log is a **rolling file** under `%ProgramData%\PerformanceMonitorDarling\logs\darling-service_yyyyMMdd.log` — connect edges, reload notices, warnings, and errors land there (buffered writes, one file per day, 14-day retention, and a logging failure can never crash the service). Console runs write the same file plus console output.

**Per-cycle collector timing is at `Debug`, which the default level does not write.** The run lines that decompose a collector's cost — `=> N rows (sql:Nms, pg:Nms)`, the `open:` / `drain:` / `other:` phase split, and the `plan_fetch:` / `text_fetch:` sub-splits — are emitted once per collector per server per cycle, and per *database* on the collectors that fan out. At fleet scale that is millions of lines a day, all of it shaped alike, and the errors and `PERMISSIONS` lines the log is actually opened for are dozens a day sitting inside it. Suppressing them by default is what keeps a grep of this file able to distinguish *not present* from *not found*.

Turn them on when you are measuring, with any standard .NET logging configuration source — an environment variable on the service, a `--Logging:LogLevel:...` argument on a console run, or an `appsettings.json` beside the exe:

```
Logging__LogLevel__PerformanceMonitor.Darling.Service=Debug
```

Scope it to the namespace as above rather than setting `Logging__LogLevel__Default`, which also turns on every other component's `Debug` output. Nothing about the lines themselves changes when the level is turned up — same text, same numbers, same one-line-per-shape layout — so anything parsing them keeps working. The store carries the same timings independently of the log: `collection_log` records `duration_ms` and `sql_duration_ms` per collector run per server at every level, and the Collection Health tab and `get_collector_cost` read them, so the distribution is available without raising verbosity at all.

Warnings and errors also go to the **Windows Application event log** (source `PerformanceMonitor Darling`) — but only if that event source exists. Registering an event source requires elevation, and the recommended `NT SERVICE` virtual account cannot do it, so run the `New-EventLog` line in the install steps above (or any elevated run of the exe) once; without it, Windows silently drops the events and the file log is your only surface. Collection outcomes are also queryable in the store itself — `collection_log` records every collector run per server with status and timings, and the viewer's Collection Health tab renders exactly that.

## The Viewer

`PerformanceMonitor.Darling.Viewer.exe` is a WPF app that talks **only to the PostgreSQL store** — it never connects to your monitored SQL Servers. It reads the same `darling.json` the service uses, but only the `postgres` section, resolved in the same order (explicit path, then `DARLING_CONFIG`, then `darling.json` next to the binary) plus one viewer-only fallback: the parent directory, so the release zip's layout — viewer in a `viewer\` subfolder, `darling.json` beside the service exe — works with no setup. When none of those has a `darling.json`, the viewer asks Windows where the installed `PerformanceMonitor Darling` service runs from (the service's registered `ImagePath`, readable without elevation) and uses the `darling.json` in that folder, so a viewer installed in its own folder on the service host needs no path. If that file is restricted to the operator group and your account is not in it, the viewer says so and names the group. A viewer seat on **another machine** is set up by exporting that config folder from the service host — see [Connect a Remote Viewer](viewer.md#connect-a-remote-viewer). If the file is missing it shows a hint instead of crashing.

At startup the viewer writes **which of those rules won**, the absolute path it produced, and whether that file exists to `%APPDATA%\PerformanceMonitorDarling\logs\darling-viewer_yyyyMMdd.log` — before it tries to read the file, so a missing or malformed one still says where it looked. Once the file loads it adds a non-secret summary of what it parsed (host, port, username, database, SSL mode, search path, whether the connection string was read verbatim or derived from `postgres.managed`, and the certificate — the value as written, the absolute path it resolves to, the folder a relative one was anchored to, and whether that file exists). Credentials are never written. The same block appears in the connection-failure window with a **Copy details** button — see [Troubleshooting](troubleshooting.md).

The layout mirrors the Lite desktop app: a left sidebar lists the servers from the `servers` registry the service maintains, and the top tab strip holds the fixed **aggregate tabs** — Overview, Alert History, Job History, Agent Alerts, Database Protection, Availability Groups (shown once the fleet has an availability group), FinOps, and Recommendations — alongside a closable **per-server tab** for each server you open. Overview (the all-servers server-cards grid) and Alert History (the all-servers alert history) span every server; Recommendations has its own server selector, independent of the sidebar. **Double-click a server** in the sidebar — or **double-click its Overview card** — to open (or focus) its tab, and close it with the × on the tab header; an empty-state panel is shown until the store has at least one server. The Alert History tab has a Known Patterns button. Settings has Manage Investigations, which holds the Investigations window.

The inner tabs of a per-server tab and the three aggregate tabs are described in [Tabs](../tabs.md#the-darling-viewer-tab-by-tab). That section holds the detail that is true only of the Darling viewer.

Only the visible tab loads (Lite's visible-only rule). The Alerts tab and the visible server tab's active inner tab refresh every 60 seconds; the Overview refreshes on its own faster 30-second timer (Lite's Overview cadence); and **Recommendations** refreshes on tab activation, its Refresh button, and its own server-selector change only, never on the timer — its findings change on the service's 30-minute analysis cadence, so a 60-second auto-refresh would be pointless churn (and would reset the incident expanders under the reader), matching Lite. The one finding that can appear between passes is **same-statement pileup** (evaluated on the collection cadence — see the `alerts` section), so a live pileup shows up here on the next activation or Refresh rather than the instant it fires; its notification does not wait for either.

The viewer has the same three color themes as Lite — Dark, Light and Cool Breeze, picked from **Settings → Color theme** — and the same **Colors** section beneath that drop-down (#3577): one row per adjustable palette color of the theme on screen, with a swatch, the hex, a picker and the measured contrast ratio against the surface it renders on (green ≥ 4.50:1, amber 3.00–4.49, red below — a warning, never a gate). **Apply** writes and applies live, **Reset to default** returns that one theme to its shipped palette, **Open file** hands you the JSON. The file is `%APPDATA%\PerformanceMonitorDarling\theme-overrides.json`, beside `viewer-settings.json`, and it is **local to this machine**: a color is a preference of the person at the screen, so it lives in neither the store nor the control plane, and a second viewer seat keeps its own. The keys, the JSON shape and the parse rules are the same as Lite's — see [Themes and colors in Lite's README](../lite-guide.md#appearance).

The viewer is read-only over collected data, but it does perform a small set of **user-initiated writes** — and those go straight to the PostgreSQL store, which is the coordination point (the service honors them on its next read; there is no viewer-to-service channel). From the Alerts tab, creating a mute rule from an alert (**Mute This Alert** / **Mute Similar**) or adding, editing, toggling, deleting, or purging one via **Manage Mute Rules** writes `config_mute_rules` (a rule scopes to a server by name, exactly as Lite's mute rules do); and **dismissing alerts** sets the `dismissed` flag on `config_alert_log` so they drop out of the Alert History view (a single atomic UPDATE — Darling has no parquet archive tier, so there is no dismissed-archive sidecar). The viewer never writes collector data.

## Restart Semantics

The service is built to restart cleanly, any time:

- **Delta continuity** — every delta-based collector (wait stats, file I/O, perfmon, memory grants, latch and spinlock stats, procedure stats, query stats, and the PostgreSQL wait and statement stats) re-seeds its baselines from the store at startup — the latest row per key inside a 15-minute window — along with the per-collector pass window the recompiled-plan credit reads, so the first cycle after a restart produces real deltas instead of zeroes. `query_stats` joined the key seed with V128: its delta key is built from the two statement offsets, which the store persists since that rung, so a row written before it (NULL offsets) restores only the pass window and every row written since restores its baseline too.
- **Alert no-re-fire** — edge-trigger watermarks and the failed-job watermark persist in `config_edge_trigger_watermarks`, and per-alert cooldowns re-seed from `config_alert_log`, so a restart does not replay alerts you already received.
- **Idempotent store setup** — migrations are versioned and skip what is already applied; TimescaleDB conversion and compression policies re-converge as no-ops.
- **Per-connect snapshots** — the on-connect config snapshot collectors run once per (re)connect, mirroring Lite's server-open behavior.
- Mute rules (`config_mute_rules`) load once at service startup — restart the service after adding rows.

A monitored server that is down is retried every 60 seconds forever; a collector that errors is logged and retried at its next scheduled time; a mid-cycle connection-level failure forces a clean reconnect and re-probe. The loop never dies for one bad cycle.

## Watching the service from outside

If the service stops, nothing inside it can tell you. Two ways to have something outside the box notice.

Option 1 is the `heartbeat` block. The service sends a GET to a URL you choose (a dead-man's-switch check such as Healthchecks.io or Uptime Kuma). Your check alerts when the pings stop. Add this to `darling.json`:

```json
"heartbeat": {
  "url": "file:/etc/darling/heartbeat-url",
  "intervalSeconds": 300
}
```

The file holds one line, the check's full URL (for example `https://hc-ping.example/your-check-id`).

- It is off unless `url` is set (an absent block, `"heartbeat": null` and an empty `url` all mean off). `intervalSeconds` defaults to 300 and must be between 60 and 3600. A bad value stops the service at start, like the rest of the config checks.
- **The whole URL is a secret.** It carries the check's token, and anyone who holds it can send "up" pings that keep your check green while the service is down. Put it in a `file:/path` reference, with the file readable by the service account only, rather than writing it in `darling.json`. An `env:NAME` reference works too, but on Windows a service's environment is stored where other local users can read it, so prefer the file. These are the same two reference forms the other secrets take. A URL written straight into `darling.json` works, but it is plaintext in the config, readable by anyone who can read that file.
- The start-up check looks only at the shape of a reference (a variable name, an absolute path) and does not read it, so commands such as `--validate-config` and `--diagnostics-bundle` work from a shell that cannot see the service's variable or file. The service reads the reference on every ping, so a rotated token file is picked up without a restart. A file larger than 8 KB is refused. A reference that cannot be read logs one fixed warning, then at most one per hour, and skips the ping.
- A URL that carries a user name and password (`https://user:pass@host/`) is refused: the service would not send them. Put the token in the path.
- `http://` is allowed (a check on your own network is common) but the token then crosses the network unencrypted on every ping, so the service logs one warning at start. Use `https://` where the check host offers it.
- Because the file that a `file:` reference names decides where the service sends its GET, protect it as you protect `darling.json`.
- The service never writes the URL to the log, which names only the scheme and host, and a diagnostics bundle aliases that host. The web Admin, the MCP tools and the triage page cannot read or edit it. Edit it in the file and restart.
- The service pings only while it is collecting. Each interval it checks the newest `collection_log` time across enabled servers. It sends the ping when no server is enabled, or when the newest collection is no more than 15 minutes old and no more than 5 minutes ahead of the clock. It skips the ping when collection is older than that, when the newest time is further ahead of the clock than that (a clock jump or a stray row), when no enabled server has a collection row, or when the store cannot be read. The service still pings with no enabled servers, so a check on this heartbeat proves the process is up and the store is readable, not that any server is being monitored. So your check alerts when the process stops and also when it keeps running but no longer collects.
- A ping waits at most 10 seconds, follows no redirects, and counts only a 2xx answer as success. A failed ping logs one warning, then at most one more per hour until a ping succeeds, and then one line saying it recovered. A failed ping never raises an alert and never slows collection.

Option 2 is a check of your own. If your monitoring already runs checks on the host, use two. The first: the service process is running (the Windows service, the systemd unit or the container). The second: the newest `collection_log` time is within 15 minutes. Run this against the store. It returns `true` when collection is current:

```sql
SELECT coalesce(
         (SELECT max(n.collection_time)
          FROM config.config_monitored_servers c
          CROSS JOIN LATERAL (
              SELECT l.collection_time
              FROM collect.collection_log l
              WHERE l.server_id = c.server_id
              ORDER BY l.collection_time DESC
              LIMIT 1) n
          WHERE c.is_enabled),
         'epoch'::timestamp)
       > (now() AT TIME ZONE 'UTC') - interval '15 minutes' AS collecting;
```

`collection_time` is stored in UTC, which is why the check compares it with `now() AT TIME ZONE 'UTC'`. The query reads one row per enabled server from the `(server_id, collection_time)` index. It returns `false` when no enabled server has a row, so treat a store with no enabled servers as a case for your own rule.
