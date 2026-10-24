# Darling store sizing

This guide gives the disk, RAM and vCPU counts for a Darling store host, by the number of monitored servers. The numbers come from three production stores that we measured on 2026-10-14. A load test on a restored copy of a store will update them ([#5794](https://github.com/erikdarlingdata/PerformanceMonitor/issues/5794)). The harness and its runbook are in [`Darling/loadtest/`](../Darling/loadtest/README.md).

To back up, restore or move a store, and to see how much disk it uses, see [Back up, restore and move the store](darling-store-backup.md).

Each row says if its number is measured or estimated. An estimate is a straight-line extension of a measured store. It is not a measurement.

## What one monitored server costs

The size of a store depends on the collector set that runs against each server. The three stores below each run one collector set.

| Store | Collector set | Servers | Store size | Size per server | Uncompressed chunks | Uncompressed per server | Source |
|---|---|---|---|---|---|---|---|
| A | SQL Server, Query Store collected | 43 | 496 GiB | about 10 GiB at steady state | 89 GiB | 2.1 GiB | Measured, except the steady-state size per server, which is an estimate |
| B | SQL Server, no Query Store | 42 | 225 GiB | 5.4 GiB (70% is plan XML) | 29 GiB | 0.7 GiB | Measured |
| C | PostgreSQL (Aurora) | 50 | 29 GiB | 0.6 GiB | 7 GiB | 0.14 GiB | Measured |

"Uncompressed chunks" are the TimescaleDB chunks that the compression policy has not compressed yet. On store A they hold the newest 25 to 32 hours of raw data and the newest 2 to 4 days of the continuous aggregates. Every compression job on store A succeeds.

### Size per server per day

We have a daily figure only for the Query Store interval tables. These tables are the largest cost per server on store A.

| Data | Per server per day | Store A total | Source |
|---|---|---|---|
| Query Store interval tables, both | 0.33 GiB | about 14 GiB a day | Measured on store A |
| `collect.query_store_interval_wide` | about 0.23 GiB | about 10 GiB a day | Measured on store A |
| `collect.query_store_interval_latest` | about 0.09 GiB | about 4 GiB a day | Measured on store A |
| Query Store interval tables at steady state | about 3.7 GiB per server in total | about 160 GiB | Computed from the daily sizes and the retention horizons: 10 GiB x 10 days + 4 GiB x 15 days = 160 GiB |
| Every other collector, SQL Server or PostgreSQL | not measured | not measured | The load test will add it |

The Query Store interval tables are plain PostgreSQL tables, partitioned by day. TimescaleDB compression does not apply to them, so all of their rows stay uncompressed.

The load test's compression step compresses a copy of one day of the wide table, in two shapes, and measures the size. The store shape is segmented by `server_id` and ordered by time descending, as the store compresses its other tables. The candidate is segmented by `server_id, database_name` and ordered by `first_execution_time`. Both sizes count the indexes, as the plain size does. This guide has no figures from that step yet.

## The retention horizons behind these numbers

The sizes above assume the default retention horizons. If you change a horizon, the size per server changes with it. The [Retention](../Darling/README.md#retention) section of the Darling README is the full list.

| Data | Horizon |
|---|---|
| `query_snapshots`, `waiting_tasks`, `running_jobs`, `agent_status` | 7 days |
| `query_store_stats_interval_hourly` (with TimescaleDB), a Query Store dedup aggregate | 7 days |
| `collect.query_store_interval_wide` | 10 days |
| `query_store_stats_interval_daily` (with TimescaleDB), a Query Store dedup aggregate | 10 days |
| `pg_plan_capture` | 14 days |
| `collect.query_store_interval_latest` | 15 days |
| Raw `query_stats`, `procedure_stats`, `query_store_stats` (with TimescaleDB) | 4 days, then the hourly and daily rollups |
| Most other collector tables, `config.config_command` (finished commands) and `analysis_findings` | 30 days |
| Baseline aggregates (with TimescaleDB) | 35 days |
| `collection_log` | 60 days |
| Hourly history rollups | 90 days |
| Size, index, bloat and replication tables, and `config_alert_log` | 90 days |
| `server_properties`, `job_history`, the yearly PostgreSQL tables and `collect.plan_force_actions` | 365 days |
| Daily history rollups | Kept with no limit |

With TimescaleDB, the service compresses each 1-day chunk when it is older than 1 day (`TimescaleSupport.CompressAfterDays` is 1, [#4211](https://github.com/erikdarlingdata/PerformanceMonitor/issues/4211) decision 1). Without TimescaleDB, nothing is compressed, and the store is larger than these numbers.

## Host size by server count

### The two facts that set the host size

1. Store A's host has 8 vCPUs and 64 GB of RAM. It serves 43 SQL Servers with Query Store collected. Every collection job keeps up and every compression job succeeds. The largest hourly aggregate refresh takes about 13 s. But reads that scan a day of Query Store data are already slow. For example, a one-day Custom Views panel on the wide table read 8.5 M rows in 48 s ([#5582](https://github.com/erikdarlingdata/PerformanceMonitor/issues/5582)).
2. `--check-settings` compares the bytes in uncompressed TimescaleDB chunks with the host RAM. It prints the result as a percentage of RAM ([Check Host/Store Sizing Settings](darling/install.md#check-hoststore-sizing-settings)). The writes and most reads touch these chunks. On store A the figure is about 150%: 89 GiB of uncompressed chunks against 64 GB of RAM. Store A keeps up at 150%, so 100% is not a measured limit. As a cautious rule, we recommend a host whose RAM holds the uncompressed chunks. That is a figure of 100% or less. We have not measured the figure at which a store falls behind.

The tables below use these rules:

- RAM: the uncompressed chunk size per server times the server count, rounded up to a common size. This is the cautious rule above. The chunk sizes are in GiB and the RAM sizes are in GB. 1 GiB is 1.074 GB, so we turn the sum into GB before we round it up. Store A runs at about 150% of RAM and keeps up, so 64 GB works for 43 servers. We have not run store A on a smaller host, so 64 GB is the one size that we know works, not a proven minimum.
- The RAM rule counts only the TimescaleDB chunks. It leaves out the Query Store interval tables, about 3.5 GiB per server, which are never compressed. [#5794](https://github.com/erikdarlingdata/PerformanceMonitor/issues/5794) expects them to matter less for RAM than the chunks. But this guide expects the reads on those tables to reach their limits first, so the load test measures them.
- Disk: the size per server times the server count, plus 50% for growth, vacuum and the temporary space of the compression jobs.
- vCPUs: one vCPU for each 5.4 servers, which is store A's ratio (43 servers on 8 vCPUs). At the top of each row that gives 3.7, 8.3, 16.7 and 27.8 vCPUs. We round them to 4, 8, 16 and 32. The last one is rounded up, to be cautious.

Only the store A row is measured. The other rows are estimates from these rules.

### SQL Server with Query Store collected (store A's collector set)

| Servers | vCPUs | RAM | Disk | Source |
|---|---|---|---|---|
| 1 to 20 | 4 | 64 GB | 300 GiB | Estimate: 20 x 2.1 GiB = 42 GiB uncompressed, 20 x 10 GiB x 1.5 = 300 GiB |
| 21 to 45 | 8 | 64 GB works, 128 GB recommended | 700 GiB | Measured on store A (43 servers, 8 vCPUs, 64 GB). 64 GB is the size we ran, not a proven minimum. 45 x 2.1 GiB = 95 GiB uncompressed |
| 46 to 90 | 16 | 256 GB | 1.4 TiB | Estimate: 90 x 2.1 GiB = 189 GiB (203 GB) uncompressed, 90 x 10 GiB x 1.5 = 1,350 GiB |
| 91 to 150 | 32 | 384 GB | 2.2 TiB | Estimate: 150 / 5.4 = 27.8 vCPUs, rounded up to 32. 150 x 2.1 GiB = 315 GiB (338 GB) uncompressed, 150 x 10 GiB x 1.5 = 2,250 GiB |

Of the stores we measured, the one with Query Store collected has 43 servers, so the rows above 45 servers are not tested. We expect the day-scan reads on the Query Store tables to get slower as you add servers. The load test will measure how much slower.

### SQL Server without Query Store, and PostgreSQL

We have no CPU measurement for stores B and C. Use the vCPU column of the table above as an upper limit.

| Servers | RAM, SQL Server without Query Store | Disk, SQL Server without Query Store | RAM, PostgreSQL | Disk, PostgreSQL | Source |
|---|---|---|---|---|---|
| 1 to 20 | 16 GB | 170 GiB | 8 GB | 20 GiB | Estimate |
| 21 to 45 | 32 GB | 370 GiB | 8 GB | 45 GiB | Estimate, near stores B (42 servers) and C (50 servers) |
| 46 to 90 | 64 GB | 730 GiB | 16 GB | 85 GiB | Estimate |
| 91 to 150 | 128 GB | 1.2 TiB | 32 GB | 135 GiB | Estimate |

For these columns, RAM is 0.7 GiB (SQL Server) or 0.14 GiB (PostgreSQL) per server, rounded up. Disk is 5.4 GiB or 0.6 GiB per server, plus 50%. The RAM column never goes below 8 GB, because the service and the store share the host.

A store that holds both kinds of server needs the sum of the two sizes.

## More than 150 servers

The sizes for more than 150 servers are pending the load test in [#5794](https://github.com/erikdarlingdata/PerformanceMonitor/issues/5794). We have no measurement above 50 servers in one store.

For reference, these are the straight-line estimates for 400 servers like those on store A. They are estimates, not measurements.

| Item | Estimate for 400 servers like store A |
|---|---|
| Store size | about 4 TiB |
| Uncompressed hypertable chunks | about 830 GiB |
| New Query Store interval rows | about 130 GiB a day |
| Query Store interval tables at steady state | about 1.4 TiB, all uncompressed |
| Rows in a day-scan read | about 10 times the rows on store A |
| Hourly aggregate refresh | about 2 minutes each hour |

We expect the day-scan reads on the Query Store tables to reach their limits first. The load test will show if that is correct.

## What to watch after you install

| Check | How | What to look for |
|---|---|---|
| Uncompressed chunks against RAM | `PerformanceMonitor.Darling.Service.exe --check-settings` | The "uncompressed chunks" line, as a percentage of RAM. A figure above 100% that grows from week to week means that the host needs more RAM. |
| Store size and growth | The `get_store_metrics` MCP tool | The size of the store and of its largest tables. Divide the growth by the server count to compare with the tables above. |
| Read times | The `get_read_latency` MCP tool, or the `collect.read_latency` rows | The `max_ms` and the histogram for the `24h` and `7d` window bands. A read at or above 5 s also writes a row to `collect.slow_reads`. |
| Collector write cost | The `get_collector_cost` MCP tool, and the collection log | The Query Store collector's time to write each cycle. A write time that grows toward the collection interval means that the store is falling behind. |
| Background jobs | `timescaledb_information.job_stats` | `last_run_duration` of the hourly aggregate refresh and of the compression jobs, and any `total_failures`. |

If a check grows toward its limit, add RAM or disk first. If the day-scan reads are the limit, move some servers to a second store.
