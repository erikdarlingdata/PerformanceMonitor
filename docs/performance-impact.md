# Performance Impact

Audience: operators.

Part of the [Performance Monitor documentation](../README.md#documentation).

### On Monitored Servers

- All queries that run on a SQL Server target use `READ UNCOMMITTED` isolation
- Configurable collection intervals
- Lite: at most 7 connection attempts at a time, 30-second command timeout
- Darling: the service collects on the shared default cadences. TimescaleDB compresses the store

### Local Resources (Lite)

- DuckDB: ~50–200 MB per server per week
- Parquet archives: ~10x compression with ZSTD
- Charts do not use the GPU. Lite sets WPF to software rendering for the whole window, and ScottPlot draws each chart on the CPU through SkiaSharp. The Darling viewer does the same.

### What the Collectors Run

These are the defaults for a SQL Server target. Each collector run sends at least one query, and a few send more (Query Store, for example, reads each database that has it on). The cadences are in [`CollectorScheduleDefaults.cs`](../PerformanceMonitor.Collectors/CollectorScheduleDefaults.cs) for Darling and in `GetDefaultSchedules` in [`Lite/Services/ScheduleManager.cs`](../Lite/Services/ScheduleManager.cs) for Lite.

| Cadence | Collectors per server | Queries per hour |
| --- | --- | --- |
| Every minute | 20 | 1,200 |
| Every 5 minutes | 13 in Lite, 14 in Darling | 156 in Lite, 168 in Darling |
| Every hour | 3 | 3 |
| Once a day | 1 (`index_object_stats`) | under 1 |
| On connect | 5 configuration snapshots (`server_config`, `database_config`, `database_scoped_config`, `trace_flags`, `server_properties`). Darling reruns them once a day | none |

That is about 1,360 queries an hour per server, or roughly 114 in every 5 minutes. `long_query_completions` is off by default. Turning it on adds one more query a minute.

- One at a time per server in Lite. Collectors for one server run one after another, so a server sees one collector query at a time. In Darling, `query_store`, `plan_correction`, `pg_wait_sampling` and the daily collectors run detached from the server's pass. They can overlap the collectors in the pass.
- Darling. It sweeps up to 4 servers at once (`maxConcurrentSweeps`, default 4, range 1 to 16).
- Lite. The limit of 7 caps how many connection attempts are in flight at once. This smooths the log-in burst when many servers are polled together. It does not cap how many connections are open.
- `READ UNCOMMITTED`. Every query that reads a SQL Server target starts with `SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED`. The collectors take no shared locks, so they do not wait behind writers. The cost is that they can read uncommitted data.
- Command timeout. Darling cancels a collector command after 60 seconds (300 seconds for `index_object_stats`). Lite cancels it after 30 seconds.

### Extended Events Sessions

Collectors read from Extended Events sessions. `system_health` is the one that SQL Server ships. The other sessions are created on the monitored server. The session lists, and what each one captures, are in these sections:

- Lite: [The Extended Events Sessions Lite Creates](lite-extended-events.md)
- Darling: [The Extended Events Sessions the Service Creates](darling/monitored-servers.md#the-extended-events-sessions-the-service-creates)

The long-query session is created only when the opt-in `long_query_completions` collector is on.

### Measured Cost

Darling records how long each collector run took (`get_collector_cost`). The figures below are from one week on one fleet: 7 SQL Server targets and 2 PostgreSQL targets, with `long_query_completions` turned on.

- What the numbers are. The time the query took on the monitored server, from start to finish, including time spent waiting. It is not CPU time. A run that waits on a lock or a busy disk counts the wait.
- What they are not. They are not a promise for your servers. Another fleet, another workload and another week will differ. The `query_store` figure also includes the time of the store's own plan and text check. That check is 55% to 81% of the figure, so the figure overstates the cost on the monitored server.
- Rows is rows stored per run, not rows read. A collector can read more than it keeps.

| Collector | Cadence | Average | Worst single run | Rows stored per run |
| --- | --- | --- | --- | --- |
| `wait_stats` | 1 min | 6 ms | 0.8 s | 69 |
| `perfmon_stats` | 1 min | 9 ms | 0.5 s | 207 |
| `file_io_stats` | 1 min | 39 ms | 1.3 s | 17 |
| `cpu_utilization` | 1 min | 37 ms | 59 s | 1.0 |
| `query_stats` | 1 min | 42 ms | 24 s | 1.6 |
| `query_snapshots` | 1 min | 44 ms | 1.5 s | 0.09 |
| `procedure_stats` | 1 min | 163 ms | 25 s | 0.02 |
| `attention` | 1 min | 186 ms | 3.2 s | 0.05 |
| `blocked_process_report` | 1 min | 215 ms | 56 s | 0.03 |
| `long_query_completions` (off by default) | 1 min | 375 ms | 23 s | 0.19 |
| `deadlocks` | 5 min | 338 ms | 55 s | 0.24 |
| `query_store` | 5 min | 317 ms | 68 s | 12 |
| `default_trace_events` | 5 min | 473 ms | 26 s | 3.1 |
| `plan_correction` | 5 min | 504 ms | 15 s | 6.3 |
| `system_health_events` | 5 min | 733 ms | 42 s | 66 |
| `index_object_stats` | 1 day | 8.1 s | 221 s | 135 |
| `pg_wait_sampling` (PostgreSQL) | 5 min | 12.3 s | 30 s | 26 |

What the table shows:

- Summed over every collector run on the SQL Server targets, the query time was about 2,000 seconds (33 minutes) per server per day. That is about 2% of a day, spread over roughly 1,360 short queries an hour. Detached collectors can overlap the pass, so this is the sum of the query times, not time with one query at a time.
- Most of the frequent collectors average under 50 ms.
- By total time, the largest are the collectors that read Extended Events sessions or the default trace (`long_query_completions`, `blocked_process_report`, `attention`, `system_health_events`, `deadlocks`, `default_trace_events`) and `procedure_stats`. Together they are about 70% of the time. The data does not say why `procedure_stats` is slower than the other DMV collectors.
- Daily `index_object_stats` is the longest single collector. It averaged 8 seconds and took up to 221 seconds, under its 300-second limit.
- On a few collectors the worst run took a minute or more, once in about 55,000 runs. The data does not say whether the server was under load or something stalled.
- On PostgreSQL, `pg_wait_sampling` averaged 12 seconds. The other PostgreSQL collectors averaged under 0.4 seconds.
