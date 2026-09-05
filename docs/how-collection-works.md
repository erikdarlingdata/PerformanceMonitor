# How Collection Works

A tour of the collection pipeline for people who know SQL but don't know this codebase. Read this, then read two or three collector definitions, and you'll understand 80% of what Performance Monitor is doing on your servers.

There is **one collection brain and two storage engines**. Every collector — the exact T-SQL sent to a monitored server, the result-row mapping, the delta rules, the default cadence, the retention horizon — is defined once in the shared `PerformanceMonitor.Collectors` library. **Lite** writes those rows to DuckDB; **Darling** writes the same rows to PostgreSQL. A collector change lands once and both editions get it.

> The **Full / Dashboard edition is deprecated** (SQL Agent jobs running T-SQL stored procedures into a `PerformanceMonitor` database). It still builds and existing installs keep working, but it does not share the collector library and is not described here. Its docs live with its code: [deprecated/Dashboard/README.md](../deprecated/Dashboard/README.md), [deprecated/Installer/README.md](../deprecated/Installer/README.md), and the `install/*.sql` scripts at the repo root belong to it.

---

## The shared collector library

**Project**: [`PerformanceMonitor.Collectors`](../PerformanceMonitor.Collectors/)

This library is deliberately dependency-free — it has **zero PackageReferences**. No `Microsoft.Data.SqlClient`, no DuckDB, no Npgsql. Definitions emit SQL *text* and read results through `System.Data.Common.DbDataReader`; the host SKU supplies the connection and the row writer. That is what makes one definition serve both storage engines.

### What a collector definition looks like

Each collector is a sealed singleton deriving from `CollectorDefinitionBase<TRow>`:

```csharp
public sealed class WaitStatsCollector : CollectorDefinitionBase<WaitStatsRow>
{
    public static readonly WaitStatsCollector Instance = new();

    public override string Name => "wait_stats";
    public override string TargetTable => "wait_stats";
    public override IReadOnlyList<CollectorColumn> PayloadColumns => ...;

    public override CollectorQuery BuildQuery(CollectorContext context) => ...;   // the T-SQL
    public override ValueTask<List<WaitStatsRow>> ReadAsync(DbDataReader reader, ...);
    public override void WritePayload(WaitStatsRow row, ICollectorRowWriter writer, ...);
}
```

The four members that matter: **`BuildQuery`** returns the SQL text plus parameters, **`ReadAsync`** maps the reader into typed rows, **`WritePayload`** hands each row's columns to whichever writer the SKU supplied, and **`PayloadColumns`** declares the table shape that the storage layer generates DDL from. Definitions are stateless and thread-safe; per-cycle state rides on `CollectorContext`.

Optional behaviours a definition can opt into, all with sensible defaults on the base class:

| Member | Purpose |
| --- | --- |
| `AppliesTo(target)` | Skip the collector on targets that can't serve it (Azure SQL DB, missing msdb, version floors) |
| `RunsPerDatabase(target)` | Run once per database instead of once per server (Azure SQL DB has no cross-database DMV reach) |
| `WatermarkColumn` / `NumericWatermarkColumn` / `PerDatabaseWatermarkColumn` | Incremental collection — only pull rows newer than what's already stored |
| `BuildEnumerationQuery` / `BuildPerItemQuery` | Two-phase collection: list the items (usually databases), then query each one |
| `BuildSupplementalQuery` | A best-effort second result set that enriches the primary rows; failure never fails the collector |
| `EmitsProbeFailures` | The definition returns a trailing `(item, error)` result set so per-item failures get summarized instead of lost |
| `YieldsOnLockTimeout` | Treat a lock timeout as "come back later," not an error (only `query_snapshots`) |
| `StateKeys` | Persist a cursor the SQL can't derive — e.g. `default_trace_events` remembering its last trace file |
| `CommandTimeoutSecondsOverride` | Raise the 60-second default (only `index_object_stats`, at 300s) |
| `PerItemTextByteBudget` | Stop a text-heavy drain mid-read and defer the backlog rather than ballooning memory |

### The three registration tables

There is **no DI container for collectors**. A new definition is wired up by adding it to three static tables, and a test pins them against each other so you can't add one and forget the others:

| Concern | File | Shape |
| --- | --- | --- |
| Schema catalog — drives DDL generation | [`CollectorCatalog.cs`](../PerformanceMonitor.Collectors/CollectorCatalog.cs) | `IReadOnlyList<ICollectorSchemaInfo> All` — 48 `XCollector.Instance` entries (41 SQL Server + 7 PostgreSQL) |
| Cadence, retention, default-enabled | [`CollectorScheduleDefaults.cs`](../PerformanceMonitor.Collectors/CollectorScheduleDefaults.cs) | `record Entry(int FrequencyMinutes, int RetentionDays, bool DefaultEnabled = true)` — 48 entries |
| Runtime dispatch (Darling) | [`DarlingWorker.cs`](../Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs) | `s_dispatch` — 48 typed lambdas |

The catalog is deliberately **engine-mixed**: the schema generator walks it to create tables and one
store can hold both engines' data, so splitting it per engine would fragment DDL generation. What keeps
dispatch honest is a separate gate — each definition declares a `TargetEngine`, and both SKUs drop
wrong-engine collectors before dispatch, so a PostgreSQL definition is never sent to a SQL Server target
or the reverse.

`FrequencyMinutes = 0` means **collect once on connect** — used for config snapshots (`server_config`, `database_config`, `database_scoped_config`, `trace_flags`, `server_properties`) that only change across restarts. `DefaultEnabled: false` ships a collector off; only `long_query_completions` does, because enabling it creates an Extended Events session on the target.

---

## Darling: the 24/7 service

**Project**: [`Darling/PerformanceMonitor.Darling.Service`](../Darling/PerformanceMonitor.Darling.Service/) · operator guide: [`Darling/README.md`](../Darling/README.md)

### The sweep loop

`DarlingWorker` is a `BackgroundService`. It ticks every **15 seconds** — that is the *scheduling* tick, not a collection interval. On each tick it walks every enabled server and runs whatever is due.

Per-server sweeps run concurrently behind a semaphore sized by `MaxConcurrentSweeps` (default 4, clamped 1–16, resizable at runtime). Within one server, a semaphore serializes the scheduled sweep against an on-demand `snapshot_now`, so a user-triggered snapshot never interleaves with the regular cadence.

`RunDueCollectorsAsync` iterates the collector names, resolves each one's effective schedule, and runs anything whose next-due time has passed, then sets `NextDue = now + FrequencyMinutes`. On reconnect, first-due times are seeded from the persisted `MAX(collection_time)` per collector plus a per-server jitter, so restarting the service resumes the real cadence instead of re-phasing every collector to the same instant.

### Running one collector

[`DarlingCollectorRunner.RunAsync`](../Darling/PerformanceMonitor.Darling.Service/DarlingCollectorRunner.cs) has three execution paths, chosen by what the definition declares:

1. **Plain** — one query, optional supplemental query, map rows, write.
2. **Per-database** — open a connection per database and run the same query in each (Azure SQL DB).
3. **Enumerate-then-iterate** — run the enumeration query to get an item list, optionally probe it, then run the per-item query for each.

Rows are written to PostgreSQL through `PgCollectorRowWriter` using **binary COPY**. Large text — query text and plan XML — is diverted to hash-keyed dimension tables (`query_text_dim`, `query_plan_dim`) instead of being stored inline, because inline payload was 94% of one 250 GB field store.

### Error isolation

Every run is wrapped so that one failure never stops the sweep. It writes exactly one row to `collect.collection_log` and returns zero rows. That row *is* the heartbeat — there is no separate heartbeat table.

| Status | Meaning |
| --- | --- |
| `SUCCESS` | Completed, including a legitimate zero rows |
| `PERMISSIONS` | A grant is missing — the collector is skipped, not broken |
| `SESSION_MISSING` | An expected Extended Events session isn't there |
| `YIELDED` | Lock timeout on a collector that opted into yielding; excluded from error rates and health bands |
| `ERROR` | Anything else. Fatal or timeout additionally forces a reconnect and re-probe on the next tick |

Health is *derived* from that log by the shared `CollectorHealthClassifier` (`NEVER_RUN`, `NO_PERMISSIONS`, `FAILING`, `STALE`, `WARNING`, `HEALTHY`). Its thresholds are **relative to each collector's own cadence**, with the old flat values as floors — `FAILING` at `max(24h, 2 × interval)`, `STALE` at `max(4h, 1.5 × interval)` — so a 60-minute collector isn't judged like a 1-minute one. The on-connect collectors are exempt from staleness. One classifier is shared by Lite, the viewer, and the service so the three can't drift.

Observability writes are deliberately failure-isolated: they log at debug and never throw, because an observability write must never break the collection loop.

### The store

Schema is **generated from the catalog**, not hand-written — there is no migration framework and no `.sql` files. [`PgSchemaGenerator`](../Darling/PerformanceMonitor.Darling.Storage/PgSchemaGenerator.cs) walks `CollectorCatalog.All` to emit DDL, and [`PgMigrations`](../Darling/PerformanceMonitor.Darling.Storage/PgMigrations.cs) is an append-only ladder of versioned rungs applied once each under an advisory lock.

Two schemas: **`collect`** is service-written and user-read; **`config`** is the operator's write surface (server list, alert thresholds, schedule overrides, commands). Every fact table starts with the same four columns —

```sql
collection_id   bigint    NOT NULL,   -- or deadlock_id, config_id, … per collector
collection_time timestamp NOT NULL,   -- or capture_time on config snapshots; the partition column
server_id       integer   NOT NULL,
server_name     text      NOT NULL
```

— followed by nullable payload columns. Fact tables have **no primary key** (a hypertable's unique constraint must include the partition column, and COPY ingest doesn't want one) and are indexed `(server_id, <time column>)`. Timestamps are naive UTC `timestamp`, never `timestamptz`.

`server_id` is not a sequence. It is a deterministic FNV-1a hash of `host[:database][:RO]`, computed client-side by `ServerIdHelper`, so both editions derive the same id for the same server and collected rows join desired-state config without a lookup. `server_id = 0` is a reserved fleet sentinel used for run-records that aren't about one server.

**TimescaleDB is optional and auto-adopted.** If the extension is present, the service converts collector tables to hypertables with 1-day chunks, compression after 1 day segmented by `server_id`, and continuous aggregates for hourly/daily rollups and baselines. Without it, the store runs in plain PostgreSQL mode, fully supported. There is no configuration flag either way. Compressed chunks stay queryable — compression *is* the archival tier.

### Schedules and overrides

Effective cadence and retention resolve per collector per server, **per column**, so a partial override falls through to the layer beneath it:

1. Per-server row in `config.config_collector_schedules`
2. Fleet-wide row in the same table (`server_id IS NULL`)
3. The code default in `CollectorScheduleDefaults`

That table is intentionally seeded empty — an absent row means "use the default," so deleting override rows is a clean reset. Any write to the control plane bumps `config_service.config_version` via a trigger, and the worker polls that one integer each sweep and hot-swaps its live config. There is no schedule knob in `darling.json`.

### Retention

Three independent mechanisms:

- **Daily service purge** — horizons come from `CollectorScheduleDefaults` (7 days for snapshot-ish collectors, 30 for most, 90 for size/index/PVS, 365 for `server_properties` and `job_history`). With TimescaleDB this is `drop_chunks`, which is metadata-only; without it, a time-sliced `DELETE` that is safe against compressed chunks. Failure-isolated per table, with an auditable run-record under `server_id = 0`.
- **TimescaleDB retention policies** for the rollup tiers — raw `query_stats` at 4 days, hourly aggregates at 90, daily kept indefinitely. Every policy is created *paused* and arms itself only once it can prove each downstream consumer has already captured the range it would drop. The governing rule, stated in the code: never drop what your consumer has not captured yet.
- **Bounded deletes** for the non-hypertable tables (alert history at 90 days, terminal commands at 30 — a pending command is never purged at any age, and the force-plan bot's decision journal at 365, the longest horizon in the store because it audits writes to production servers rather than measuring them).

Darling deliberately does not archive before deleting; compression is the archive.

---

## Lite: the desktop edition

**Project**: [`Lite`](../Lite/)

Lite is a standalone WPF app — no service, no central store. It runs the **same collector definitions**, and its `RemoteCollectorService.<Name>.cs` partials are thin delegations, one per collector:

```csharp
private Task<int> CollectCpuUtilizationAsync(ServerConnection server, CancellationToken cancellationToken)
    => RunCollectorDefinitionAsync(CpuUtilizationCollector.Instance, server, cancellationToken);
```

The SQL is not in those files — it is in the shared definition. That is the cross-SKU parity contract: engine quirks and dedup rules live in one place so Lite and Darling cannot disagree about what a collector means.

Storage is a local DuckDB file plus a Parquet archive. DuckDB is single-writer, so collectors for a given server run **sequentially**; multi-server parallelism still works, each server running its own serialized chain. Schedules come from [`Lite/config/collection_schedule.json`](../Lite/config/collection_schedule.json) rather than a table, and retention runs inline at the end of each cycle.

---

## Where to look next

**To understand a collector**, read its definition in `PerformanceMonitor.Collectors/<Name>Collector.cs`. The `BuildQuery` body is the exact SQL sent to your server — that is the whole story of what it touches.

**To understand what's stored**, read [`Darling/Darling.Tests/Fixtures/migration-ladder-*.sql`](../Darling/Darling.Tests/Fixtures/). It is the entire schema ladder as resolved SQL, regenerated per release, and it is the fastest way to see every table without reading a generator. Do not hand-edit it.

**To add a collector**, you need four edits: the definition itself, a `CollectorCatalog.All` entry, a `CollectorScheduleDefaults.All` entry, and — for Darling — an `s_dispatch` lambda plus an append-only migration rung spelling out the table in the generator's column order. Migration versions are never edited or reordered; the runner only applies versions above the store's current max. Pinned tests will tell you which of the four you forgot.

**To trace a UI element back to data**, follow the XAML binding to its `*.xaml.cs`, then the service call into the query layer (`LocalDataService.*` in Lite, `DarlingDataReader.*` in Darling).

If something feels genuinely undocumented rather than "read the code," open an issue. Gaps get prioritized based on what comes up.
