# Darling store sizing

Audience: operators.

This guide gives the disk, RAM and vCPU counts for a Darling store host, by the number of monitored servers. The numbers come from three production stores that we measured on 2026-10-14. A load test on a restored copy of a store ([#5794](https://github.com/erikdarlingdata/PerformanceMonitor/issues/5794)) went to 210 servers in one store. It ran four times and is done. The test hosts are torn down. This guide stays based on what was measured. The harness and its runbook are in [`Darling/loadtest/`](../Darling/loadtest/README.md).

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
| Every other collector, SQL Server or PostgreSQL | not measured | not measured | The load test did not cover it |

The Query Store interval tables are plain PostgreSQL tables, partitioned by day. TimescaleDB compression does not apply to them, so all of their rows stay uncompressed.

The load test's compression step compresses a copy of one day of the wide table, in two shapes, and measures the size. The store shape is segmented by `server_id` and ordered by time descending, as the store compresses its other tables. The candidate is segmented by `server_id, database_name` and ordered by `first_execution_time`. Both sizes count the indexes, as the plain size does. This guide has no figures from that step yet.

## The retention horizons behind these numbers

The sizes above assume the default retention horizons. If you change a horizon, the size per server changes with it. The [Retention](darling/operations.md#retention) section of the Darling operations page is the full list.

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
- The RAM rule counts only the TimescaleDB chunks. It leaves out the Query Store interval tables, about 3.5 GiB per server, which are never compressed. [#5794](https://github.com/erikdarlingdata/PerformanceMonitor/issues/5794) expects them to matter less for RAM than the chunks. But this guide expects the reads on those tables to reach their limits first. The load test did not measure their RAM use.
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

Of the stores we measured, the one with Query Store collected has 43 servers, so the rows above 45 servers are estimates. The load test below adds one measured point, at 210 servers. There, 256 GB was the smallest host on which one reader's reads met the 5 s budget. The RAM rule gives more than that for 150 servers, so the rule is cautious for one reader. Each phase ran once on each host, and no host that we tested served ten readers well.

#### The load test at 210 servers

The test copy held N copies of one production store with 42 servers. N=3 is 126 servers and N=5 is 210 servers, about 1 TB. In runs 1 and 2, the host had 8 vCPUs, 64 GB of RAM and a gp3 disk at 6,000 IOPS and 250 MB/s. Run 3 kept the run 2 build and data and changed the host (see [Run 3: larger hosts](#run-3-larger-hosts)). A reader is one client that reads at the same time as the others. K is the number of readers. All numbers below are measured.

| | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| Date | 2026-10-21 | 2026-10-24 | 2026-10-24 and 2026-10-25 |
| Build | An older build, before the read fixes | `dev` at `d659bef1a`, with [#6686](https://github.com/erikdarlingdata/PerformanceMonitor/pull/6686) and the later read fixes | The run 2 build |
| Host | 8 vCPUs, 64 GB, 250 MB/s disk | 8 vCPUs, 64 GB, 250 MB/s disk | 64 GB, 128 GB and 256 GB, faster disk |

**The finding: a host with 8 vCPUs and 64 GB takes the writes and the hourly jobs for 210 servers. The reads are the limit. In run 3, one reader needed about 256 GB. With ten readers, every host that we tested refused many calls.**

- **Writes:** replay lag stayed at 1.5 to 1.7 minutes in both runs. Run 2 had no replay errors.
- **Hourly aggregate jobs at steady state:** no hourly job ran over 60 minutes. After the catch-up, each job took 14 minutes or less. In the last two sampled hours, every job took 2 min 20 s or less.
- **After you add many servers at once:** expect a catch-up period. In run 2, we went from 126 to 210 servers. Two hourly jobs then ran about 2 hours each. Each skipped its next hour. Plan for this, and add servers in steps if you can.
- **Reads, one reader:** at 210 servers on 64 GB, the Query Store reads miss the 5 s budget even with one reader. The fleet panel does not finish in 60 s.
- **Reads, ten readers:** 25% of calls were refused as busy in run 2, and 28% in run 1. At 126 servers in run 1, 4.6% were refused. When a K=10 run overlapped the hourly refresh, 62% were refused.

Read times at one reader (K=1), at 210 servers:

| Read | Run 1 p95 | Run 2 p95 |
|---|---|---|
| Regressions, 24 h, MCP | 40.9 s | 17.7 to 29.3 s |
| Regressions, 24 h, web | 59.4 s | 32.3 to 34.8 s |
| Top statements, 24 h, MCP | 1.1 s | 11.6 to 20.5 s |
| Regressions, 168 h, MCP | Not measured | 16 to 19 s |
| Fleet panel | Timed out at 60 s | Timed out at 60 s, 5 of 5 calls |

- **Sample size:** each K=1 read in run 2 has 1 to 7 calls. Its p95 is its slowest call.
- **The fleet panel number is not final.** The 4 newer copies had not filled their wide table yet. That takes about 2 days after a copy starts.
- **The regressions read is faster, but not fast enough.** The slowest statement is still the regressions baseline. Its mean fell from 14.4 s to 3.8 s.
- **Not explained yet:** the 24 h top read was slower in run 2 than in run 1. The samples are small.
- **The disk:** run 1 found the disk to be the limit. Run 3 measured a faster disk and larger hosts.
- **Compose rollup in run 1:** 13 hour builds failed on the 60 second statement timeout at 126 servers, across 3 hourly runs. They shared a window with the 12:00Z policy jobs. We infer that link. We did not prove it.

The fixes since run 1 are [#6259](https://github.com/erikdarlingdata/PerformanceMonitor/pull/6259) (the fleet panel reads the interval table for the servers that can use it), [#6260](https://github.com/erikdarlingdata/PerformanceMonitor/pull/6260) (the regressions baseline comes from the per-day totals), [#6484](https://github.com/erikdarlingdata/PerformanceMonitor/pull/6484) (the text label is a keyed lookup) and #6686. On a production store with 42 servers, after #6260, a first regressions call takes 9 to 22 s and a second one under 2 s (measured).

##### Run 3: larger hosts

Run 3 ran on 2026-10-24 and 2026-10-25, with the run 2 build and the same 210 servers. It changed the RAM, the vCPUs and the disk. The faster disk is a gp3 disk at 16,000 IOPS and 1,000 MB/s. With it, the host's own disk limit is 312 MB/s on 8 vCPUs, 625 MB/s on 16 and 1,250 MB/s on 32. The run 2 row is in the table for comparison. All numbers are measured. Each host ran once.

| Host | `effective_cache_size` | One reader: reads with p95 over 5 s | One reader: slowest p95 | Ten readers: calls refused as busy | Fleet panel |
|---|---|---|---|---|---|
| 8 vCPUs, 64 GB, 250 MB/s disk (run 2) | 48,384 MB, Darling's value | Most | 34.8 s | 25% | Timed out at 60 s |
| 8 vCPUs, 64 GB, faster disk | 48,384 MB, Darling's value | About 6 | 17.2 s | 46% | Timed out at 60 s |
| 16 vCPUs, 128 GB, faster disk | 48,384 MB, half of Darling's value | About 7 | 14.5 s | 29% | Timed out at 60 s |
| 16 vCPUs, 128 GB, faster disk | 96,768 MB, Darling's value | 2, the 168 h regressions reads (10.6 s MCP, 9.2 s web) | 10.6 s | 61% | Timed out at 60 s |
| 32 vCPUs, 256 GB, faster disk | 48,384 MB, a quarter of Darling's value | 3 | 7.0 s | 55% | Timed out at 60 s |
| 32 vCPUs, 256 GB, faster disk | 193,536 MB, Darling's value | 0 in the second phase, 3 in the first | 3.3 s in the second phase, 12.7 s in the first | 49% | Refused before it ran. Not measured. |

- **`effective_cache_size`** tells the PostgreSQL planner how much memory caches the data. On a store that Darling manages, the service sets it to 75% of RAM at every start. The test store was a bring-your-own store, so the value stayed at the 64 GB figure after the host grew, until we set it by hand. The rows with Darling's value show what a managed install gets.
- **On a bring-your-own store,** Darling does not change this setting. After you change the host size, run `--check-settings` ([Check Host/Store Sizing Settings](darling/install.md#check-hoststore-sizing-settings)) and set `effective_cache_size` to the value that it derives.
- **The busy rate does not compare well across hosts.** Each reader sends its next call when the last one returns. A busy reply returns at once, so a faster host sends more calls and gets more of them refused. Compare the read times instead.
- **The slowest statement** is the regressions baseline. Its mean per call was 8.1 s on 64 GB with the faster disk, 2.5 to 2.7 s on 128 GB, 1.8 s on 256 GB and 0.78 s on 256 GB with Darling's value. On 256 GB, Darling's value cut the means of the five slowest statements by 34 to 62%. On 128 GB, it made no change that we could measure.
- **The fleet panel on 256 GB with Darling's value:** the product refused it before it ran, with "This panel needs about 6.9 million Query Store rows (42 servers, 1 day), over the limit of 6.8 million." That is the panel size limit from [#5582](https://github.com/erikdarlingdata/PerformanceMonitor/issues/5582). An hour later, on the 128 GB host, the same panel passed the limit and timed out. The estimate is near the limit, but we did not find why the two results differ. Without the 60 s panel calls, more heavy-read slots were free on the 256 GB host, so its ten-reader numbers look better than they would with the panel.
- **The hardware:** on 128 GB and 256 GB, the disk reads stayed under the host's limit, and the CPUs peaked at 62% or less. The highest disk read p95 was 414 MB/s, on 128 GB, against a limit of 625 MB/s. On 64 GB with the faster disk, the disk reads peaked at 521 MB/s, over the host's 312 MB/s limit for short times.
- **The hourly jobs on 64 GB with the faster disk:** in each of two sampled hours, all 16 jobs finished with no failures. The longest took 3 min 5 s. Together they took about 9 minutes each hour.
- **Limits of the test:** each host ran once. A change of host size empties the file cache, so the first phase after it ran on a cache that was still filling. Background aggregate builds overlapped some phases.

Our reading of run 3, which is an inference from these numbers:

- One store with about 210 servers and one heavy reader needs about 256 GB, with Darling's settings. On 128 GB, the 168 h regressions reads miss the budget.
- No host that we tested serves ten readers at once at 210 servers. The limits are in the product: the limit on heavy reads at the same time (the busy replies), the 60 s statement timeout and the panel size limit. More hardware does not raise them.
- If more than one person reads the store at the same time, use more than one store above about 150 servers. See [More than 150 servers](#more-than-150-servers).

##### Run 4: a newer build on 256 GB

Run 4 used build `629a72767` at 210 servers, on an r7i.8xlarge host with 256 GB of RAM. The hardware and the planner settings match the best run 3 host (256 GB, with Darling's value for `effective_cache_size`). Only the build is newer. All numbers are measured.

| Phase | Run 4 | Run 3 |
|---|---|---|
| One reader, warm | Every read p95 at or under 3.2 s, none refused as busy | The same pass |
| One reader, cold after the resize | 7 reads with p95 over 5 s | 3 |
| Ten readers | 15.0% refused as busy, 8 reads with p95 over 5 s, slowest 9.0 s | 48.8% refused as busy |

- **A plan capture** (`auto_explain` at 2 s) on the warm host found only 5 statements over 2 s. The top CPU ranking pass was not one of them. Its cost is a cold-cache cost.
- **Limits of the test:** each phase ran once. The cache was cold after each resize. About 10 million dead tuples were left by a cleanup during setup.

Our reading of run 4 is an inference from these numbers. Run 4 met the one-reader budget warm on 256 GB, as run 3 did. Ten readers still saw 15.0% of calls refused as busy, so the ten-reader advice above does not change.

The project to keep reads fast for many users ([#5506](https://github.com/erikdarlingdata/PerformanceMonitor/issues/5506)) is open. It was closed on 2026-10-24 and reopened on 2026-10-25. The last section of this page says to move some servers to a second store if the day-scan reads are the limit.

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

The sizes for more than 150 servers are estimates. The load test in [#5794](https://github.com/erikdarlingdata/PerformanceMonitor/issues/5794) went to 210 servers in one store. Writes and the hourly jobs fit on 8 vCPUs and 64 GB. One reader's Query Store reads met their budget only on 256 GB. With ten readers, every host that we tested refused many calls as busy. We have no measurement above 210 servers.

If more than one person reads the store at the same time, use more than one store above about 150 servers.

For reference, these are the straight-line estimates for 400 servers like those on store A. They are estimates, not measurements.

| Item | Estimate for 400 servers like store A |
|---|---|
| Store size | about 4 TiB |
| Uncompressed hypertable chunks | about 830 GiB |
| New Query Store interval rows | about 130 GiB a day |
| Query Store interval tables at steady state | about 1.4 TiB, all uncompressed |
| Rows in a day-scan read | about 10 times the rows on store A |
| Hourly aggregate refresh | about 2 minutes each hour |

We expect the day-scan reads on the Query Store tables to reach their limits first. The 210-server result points the same way.

## What to watch after you install

| Check | How | What to look for |
|---|---|---|
| Uncompressed chunks against RAM | `PerformanceMonitor.Darling.Service.exe --check-settings` | The "uncompressed chunks" line, as a percentage of RAM. A figure above 100% that grows from week to week means that the host needs more RAM. |
| Store size and growth | The `get_store_metrics` MCP tool | The size of the store and of its largest tables. Divide the growth by the server count to compare with the tables above. |
| Read times | The `get_read_latency` MCP tool, or the `collect.read_latency` rows | The `max_ms` and the histogram for the `24h` and `7d` window bands. A read at or above 5 s also writes a row to `collect.slow_reads`. |
| Collector write cost | The `get_collector_cost` MCP tool, and the collection log | The Query Store collector's time to write each cycle. A write time that grows toward the collection interval means that the store is falling behind. |
| Background jobs | `timescaledb_information.job_stats` | `last_run_duration` of the hourly aggregate refresh and of the compression jobs, and any `total_failures`. |

If a check grows toward its limit, add RAM or disk first. If the day-scan reads are the limit, move some servers to a second store.
