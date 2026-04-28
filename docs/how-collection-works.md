# How Collection Works

A tour of the collection pipeline for people who know SQL but don't know this codebase. Read this, then read three SQL files, and you'll understand 80% of what Performance Monitor is doing on your server.

This doc covers both editions. Full Edition first (SQL Agent → `PerformanceMonitor` database → Dashboard reads), Lite Edition second (WPF app → DuckDB file → same app reads). The shapes are similar; the surface area is different.

---

## Full Edition

### The minute loop

Everything happens inside one SQL Agent job:

| Job | What it runs |
| --- | --- |
| `PerformanceMonitor - Collection` | `EXEC collect.scheduled_master_collector @debug = 0;` on a 1-minute schedule (`Every 1 Minute`) |
| `PerformanceMonitor - Data Retention` | `EXEC config.data_retention @debug = 1;` once a day |
| `PerformanceMonitor - Hung Job Monitor` | Kills the Collection job if it's been stuck past its max duration |

When the Collection job fires, it calls the **scheduled master collector** — the dispatcher. The dispatcher is the heartbeat of the whole system. Every minute it wakes up, figures out which collectors are due, and runs them one at a time.

### The dispatcher

**File**: [`install/42_scheduled_master_collector.sql`](../install/42_scheduled_master_collector.sql)

At the core of the dispatcher is a cursor over `config.collection_schedule` that picks up anything due:

```sql
SELECT
    cs.schedule_id,
    cs.collector_name,
    cs.frequency_minutes,
    cs.max_duration_minutes
FROM config.collection_schedule AS cs
WHERE cs.enabled = 1
AND   (
          @force_run_all = 1
          OR cs.next_run_time <= SYSDATETIME()
          OR cs.next_run_time IS NULL
      )
ORDER BY
    cs.next_run_time;
```

For each row, the dispatcher has a big `IF/ELSE IF` block that maps `collector_name` to a specific stored procedure:

```sql
ELSE IF @collector_name = N'default_trace_collector'
BEGIN
    EXECUTE collect.default_trace_collector @debug = @debug;
END;
ELSE IF @collector_name = N'blocking_deadlock_analyzer'
BEGIN
    EXECUTE collect.blocking_deadlock_analyzer @debug = @debug;
END;
-- ...etc
```

Each collector runs inside its own `BEGIN TRY / BEGIN CATCH` block — a failure in one doesn't stop the rest of the cycle. After each run (success or failure), the dispatcher bumps `last_run_time` and `next_run_time = last_run_time + frequency_minutes` so the next tick knows when that collector is eligible again.

Before any of this, the dispatcher also does two self-heal steps:

- **Ensures config tables exist** (`config.ensure_config_tables`) — lets you recover from an accidentally-dropped table without reinstalling.
- **Detects server restarts** — if `sqlserver_start_time` has changed since last run, it captures a fresh snapshot of server properties. Config values only change across restarts, so this is the efficient moment to grab them.

### What a collector looks like

Pick any `install/NN_collect_*.sql` file — they all follow the same shape. A minimal example:

**File**: [`install/29_collect_default_trace.sql`](../install/29_collect_default_trace.sql)

```sql
ALTER PROCEDURE
    collect.default_trace_collector
(
    @hours_back integer = 2,
    @include_memory_events bit = 1,
    @include_autogrow_events bit = 1,
    @include_object_events bit = 1,
    -- ...more flags
    @debug bit = 0
)
AS
BEGIN
    BEGIN TRY
        -- 1. Validate parameters
        IF @hours_back <= 0 OR @hours_back > 168
        BEGIN
            RAISERROR(N'@hours_back must be between 1 and 168 hours', 16, 1);
            RETURN;
        END;

        -- 2. Detect first run (empty target table, no prior success in config.collection_log)
        IF NOT EXISTS (SELECT 1/0 FROM collect.default_trace_events)
        AND NOT EXISTS (SELECT 1/0 FROM config.collection_log WHERE collector_name = N'default_trace_collector' AND collection_status = N'SUCCESS')
        BEGIN
            SET @cutoff_time = CONVERT(datetime2(7), '19000101'); -- grab everything on first run
        END;

        -- 3. Query the DMV / system view
        INSERT INTO collect.default_trace_events (...)
        SELECT ...
        FROM sys.fn_trace_gettable(@trace_path, @max_files) AS ft
        WHERE ft.StartTime >= @cutoff_time
        AND   <per-collector filters>
        AND   NOT EXISTS (<dedupe lookup on event_time + event_class + spid + event_sequence>);

        -- 4. Log success to config.collection_log
        INSERT INTO config.collection_log (...) VALUES (..., 'SUCCESS', @rows_collected, ...);
    END TRY
    BEGIN CATCH
        -- 5. Log failure with error message
        INSERT INTO config.collection_log (...) VALUES (..., 'ERROR', 0, @error_message);
        THROW;
    END CATCH;
END;
```

Every collector does exactly these five things: **validate, detect first-run, pull from DMV, insert with dedupe, log**. Once you've read one, you've read all thirty. The differences are the source DMV, the filter conditions, and the shape of the destination table.

### The schedule table

**File**: [`install/03_create_config_tables.sql`](../install/03_create_config_tables.sql) (table definition)

`config.collection_schedule` is the single source of truth for *what runs and when*. It has one row per collector:

| Column | Meaning |
| --- | --- |
| `collector_name` | The name the dispatcher's `IF/ELSE` block matches on |
| `enabled` | Bit flag — off means the dispatcher skips this row entirely |
| `frequency_minutes` | How often to run. `0` means "on connect / daily / special" (see below) |
| `last_run_time` | When the collector last started — updated by the dispatcher |
| `next_run_time` | When the collector is next eligible — `last_run_time + frequency_minutes` |
| `max_duration_minutes` | Kill switch for the hung-job monitor |
| `retention_days` | How long to keep data in the target `collect.*` table |

You can edit this table directly, but **don't**. The supported knobs are:

- **`config.apply_collection_preset`** — bulk-sets `frequency_minutes` for all collectors at once (presets: `Aggressive`, `Balanced`, `Low-Impact`).
- **Individual `UPDATE` statements on `enabled`** — turn specific collectors on or off.

**File**: [`install/41_schedule_management.sql`](../install/41_schedule_management.sql) has the preset procedure and some helper procs for listing / resetting the schedule.

### Where does the data go?

Each collector writes to a table in the `collect` schema — `collect.query_stats`, `collect.default_trace_events`, `collect.wait_stats`, etc. Same shape each time: a `collection_time datetime2` column, plus whatever the DMV gave us, plus whatever we computed.

Some tables use `COMPRESS()` on large text/XML columns (query text, plan XML) — stored as `varbinary(max)` and wrapped in `DECOMPRESS()` on read. That's why query text looks like gibberish if you `SELECT * FROM collect.query_stats` directly — read through `v_query_stats` instead, which handles the decompression.

### The Dashboard read path

The Dashboard is a WPF app. It connects to the `PerformanceMonitor` database and issues SELECT queries. No collection happens in the app — the Dashboard is purely a reader. Every time you pick a time range, change a tab, or hit refresh, the app runs a SQL query against `collect.*` tables or `v_*` views, pulls rows into a `List<T>`, and binds that list to a WPF DataGrid or a ScottPlot chart.

The query layer lives in `Dashboard/Services/DatabaseService.*.cs` — split by concern (`DatabaseService.QueryPerformance.cs`, `DatabaseService.SystemEvents.cs`, etc.). Each file is just SQL in C# strings. If the Dashboard is showing you something, there's a method somewhere in that folder returning it.

### Retention

**File**: [`install/45_create_agent_jobs.sql`](../install/45_create_agent_jobs.sql) (job definition) and wherever `config.data_retention` lives.

Once a day, the `PerformanceMonitor - Data Retention` job runs a `DELETE` loop per `collect.*` table, respecting each row's `retention_days` from `config.collection_schedule`. Targeted batched deletes, not a truncate — history older than the retention window disappears; recent data is untouched.

---

## Lite Edition

### What's different

Lite is a standalone WPF app — **no SQL Agent involved, no PerformanceMonitor database**. The app itself is the collector, and the storage is a local DuckDB file (`%LocalAppData%\PerformanceMonitorLite\pm_lite.duckdb`).

The shape still mirrors Full: a dispatcher picks collectors, each collector pulls from DMVs and writes to a destination table, and a reader service hands data to the UI.

### The two services

**Writer**: [`Lite/Services/RemoteCollectorService.cs`](../Lite/Services/RemoteCollectorService.cs) plus one `RemoteCollectorService.<Name>.cs` partial per collector (19 of them). The service opens a `SqlConnection` to the monitored server, runs DMV queries, and bulk-inserts results into DuckDB.

**Reader**: [`Lite/Services/LocalDataService.*.cs`](../Lite/Services/) — queries DuckDB and returns results to the UI.

Only one connection writes at a time. DuckDB is single-writer, so within a given server the collectors run **sequentially** (not in parallel). Multi-server parallelism still works — each monitored server runs its own serialized collector chain.

### The schedule

**File**: [`Lite/config/collection_schedule.json`](../Lite/config/collection_schedule.json)

A JSON file, not a table. User-editable. The Lite app reads it at startup and at each wake-up tick. Same shape as the Full Edition schedule (name, enabled, frequency_minutes, retention_days) with one convention: `frequency_minutes: 0` means "run once at connect time" — used for server config, database config, trace flags, etc. that don't change between restarts.

### Data retention

Lite runs retention inline as part of each collection cycle — no separate job. Each collector checks its `retention_days` against the max timestamp in its target table and deletes older rows. DuckDB checkpoints after each cycle to flush the WAL.

---

## Where to look next

If you want to **understand a specific feature**, find the code from the UI outward:
1. Find the grid/chart in the app.
2. Find its XAML file (`Dashboard/*.xaml` or `Lite/Controls/*.xaml`).
3. Follow the `Click` handler or `ItemsSource` binding to the `*.xaml.cs` file.
4. Follow the service call (`_databaseService.GetXxxAsync(...)` in Full, `LocalDataService.GetXxxAsync(...)` in Lite) to the query.

If you want to **understand a specific collector**, read:
1. `install/NN_collect_<name>.sql` for Full Edition, or
2. `Lite/Services/RemoteCollectorService.<Name>.cs` for Lite.

If you want to **add a collector or a new data source**, the dispatcher file in Full (`42_scheduled_master_collector.sql`) or `RemoteCollectorService.cs` in Lite is where you wire it up — those are the files that know about every collector.

If something feels genuinely undocumented rather than "read the code," open an issue. Gaps get prioritized based on what comes up.
