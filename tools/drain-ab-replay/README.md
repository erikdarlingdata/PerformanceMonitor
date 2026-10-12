# drain-ab-replay

A load script for the retention-drain A/B in #5592. You restore two copies of a large store that still holds a
retention backlog. You block outbound network on both. Copy A runs the 3.10 dev build, where the drain is not paced.
Copy B runs the #5592 build, where the drain waits while collection is behind and pauses after each batch. With the network blocked no real collector can run, so this script stands in for the collectors'
writes. It measures what the drain does to those writes, one table at a time.

You run it. You build nothing. It needs PowerShell 7, `psql` and `pgbench` (PostgreSQL 14 or newer client tools).
The `pgbench` that ships with the server is fine.

## What it does

1. **Rate from the store.** For every `collect.*` table that has a `collection_time` and a `server_id`, it counts the
   rows written in the last 24 hours of data (the window ends at the newest `collect.collection_log` row, so a stopped
   copy still has a "last day"). It also reads how many servers wrote in that window and the average rows per
   collection (one `(server_id, collection_time)` pair is one collection). The result is `rates.csv` in the output
   folder and is printed.
2. **Statement mix from the code.** It picks the tables that carry at least 90 percent of those rows
   (`-CoveragePercent`) and writes one pgbench script per table into `scripts/`. Rows get real `server_id` and
   `server_name` values from `collect.servers`, the current time, and plausible keys. Each script weight is that
   table's share of transactions per second.
3. **Run.** `pgbench -n -c <clients> -R <transactions per second> --log`, in 60 second segments, until the drain is
   done (no retention DELETE seen for 5 minutes after one was seen), or the hard cap (4 hours). If no DELETE shows up
   in 60 minutes it stops too. Clients are one per server that wrote in the window, capped at 32.
4. **Reads, every second.** `sampler.csv`: the table the retention backend is deleting from, its wait event, counts of
   IO, Lock and LWLock waiters (pgbench backends and everything else), the WAL position and, on PostgreSQL 16 or
   newer, the `pg_stat_io` totals (reads, writes, read and write time). On older servers those columns are zero and the
   report says "not available". `deletes.csv`: `n_tup_del` per table every 2 seconds (partitions and chunks roll up to
   their parent).
5. **Report.** `phases.csv`, `latency_by_script.csv` and `summary.md`. A phase is a table the retention backend was
   deleting from. "No drain" is every second with no retention DELETE seen. Per phase: start, end, seconds, rows
   deleted and rows per second (from `n_tup_del`), transactions per second, write latency p50, p95 and max, schedule
   lag, the IO, Lock and LWLock wait share, WAL per second. `-Compare` puts runs A and B side by side.

## What it copies from the collectors, and what it cannot

The collectors write each batch with one binary `COPY` per server per collection
(`PgCollectorRowWriter.cs`, `CopyCommandFor`, line 192). pgbench cannot issue a `COPY`, Each script writes the same
table, the same column list, and the same number of rows per batch with one `INSERT ... SELECT ... generate_series`,
inside one transaction. The tables that really upsert use the real upsert text, copied into the scripts and cited by
file and line in a comment at the top of each script:

| Table | Shape in the script | Source |
|---|---|---|
| `query_store_stats` | raw insert, then the Query Store interval apply into `query_store_interval_latest` and `query_store_interval_wide`, same transaction | `QueryStoreIntervalLatest.cs` line 239, `QueryStoreIntervalWide.cs` line 201, `DarlingCollectorRunner.cs` lines 4684 and 4692 |
| `query_stats`, `procedure_stats` | text and plan content upserted into `query_text_dim` and `query_plan_dim` first (most digests repeat, `-NewDigestPercent` are new), then the raw row carries only the digest | `PayloadDimensions.cs` line 395, lines 122-124 |
| every other table | plain insert of one batch | `PgCollectorRowWriter.cs` line 192 |

Before the run, each script is executed once inside a transaction that is rolled back. A table whose write does not
run against this store is dropped from the mix with a warning, and the covered share is recomputed. The run stops if
that falls under `-CoveragePercent` (override with `-AllowLowCoverage`).

It does not replace the real collectors:

- A real collector's scheduler skips a slot when the previous run has not finished. pgbench keeps its own schedule
  and reports `lag_*` (how late a transaction started against its slot). That is the closest stand-in for skipped
  slots, not the same thing. The self-alert and the "skipping relaunch" log lines need the real service.
- It does not reproduce the service's own work after a collection (hourly roll-ups, sweeps, config reads).
- Row contents are synthetic. Row width follows `-QueryTextBytes` and `-PlanBytes`, not the field store's.
- Tables the collectors write without a `collection_time` column are not in the mix.
- When the store has TimescaleDB hypertables, a drain that uses `drop_chunks` does not move `n_tup_del`. The
  phase still shows (the sampler sees the `drop_chunks` statement) but its rows per second is blank.
- Retention is recognized by its statement text: a statement that starts with `DELETE FROM <table>` or
  `SELECT drop_chunks('<table>'`. The application name is recorded in `sampler.csv` but is not enough on its own,
  because every connection the service opens carries the same name. `-RetentionAppName` narrows it if you want.
- The login needs to see other sessions' query text (superuser, `pg_read_all_stats` or `pg_monitor`). The script warns
  when it cannot. If it cannot, every phase reads "no drain".
- At a low rate few pgbench backends are caught active by a once-a-second sample. `active_samples` in `phases.csv`
  says how many. Read the wait shares only when it is large.

## Safety

- It refuses to run unless every server in `config.config_monitored_servers` has `is_enabled = false`, or you pass `-IKnowThisIsACopy`. That is the table the service collects from. On an older schema without it, the check reads `collect.servers`.
- It writes only the collector insert and upsert shapes above. It never drops, truncates or alters anything. The
  only other statements are `SELECT`s and the rolled-back validation run.
- The connection comes from parameters only. `-Password` is handed to `psql` and `pgbench` through `PGPASSWORD` for
  the run and removed afterwards. Leave it out when the login needs no password. The script never reads a connection
  setting from the environment.
- The extra rows it writes stay in the copy. Throw the copy away when the A/B is done.

## Commands

Restore a fresh copy for each run. Start the service on the copy, then start this script right away so the "no drain"
period before the drain is measured too. Run A, on the copy running the 3.10 dev build:

```powershell
pwsh ./tools/drain-ab-replay/Invoke-DrainAbReplay.ps1 `
    -DbHost localhost -Port 5432 -User postgres -Database darling -Password '<password>' `
    -OutputFolder D:\ab\runA -IKnowThisIsACopy -ServiceLog D:\logs\darling-A.log
```

Run B, on the copy running the #5592 build (change the port and database name to match that copy):

```powershell
pwsh ./tools/drain-ab-replay/Invoke-DrainAbReplay.ps1 `
    -DbHost localhost -Port 5433 -User postgres -Database darling -Password '<password>' `
    -OutputFolder D:\ab\runB -IKnowThisIsACopy -ServiceLog D:\logs\darling-B.log
```

Compare (writes `compare.md` and `compare.csv` into `-OutputFolder`):

```powershell
pwsh ./tools/drain-ab-replay/Invoke-DrainAbReplay.ps1 -Compare D:\ab\runA D:\ab\runB -OutputFolder D:\ab\compare
```

Rebuild one run's report (for example with another gap setting): add `-ReportOnly -OutputFolder D:\ab\runA`.

Useful parameters: `-RateScale` (1.0 is the measured rate), `-HardCapHours` (4), `-IdleMinutes` (5),
`-NoDrainTimeoutMinutes` (60), `-FixedDurationSeconds` (0 means run to the stop condition), `-MaxClients` (32),
`-Threads` (1), `-CoveragePercent` (90), `-MaxTables` (40), `-RateWindowHours` (24), `-PhaseGapSeconds` (120: a pause
this short between batches, or between two batches of the same table, stays in the phase), `-MinBatchRows` and
`-MaxBatchRows`, `-QsDatabases`, `-DigestPool`, `-NewDigestPercent`, `-QueryTextBytes`, `-PlanBytes`, `-PsqlPath`,
`-PgbenchPath`. Use the same values for A and B. `-ServiceLog` is optional. Its `Retention purge drained ...` and
`Retention purge: ...` lines are copied into `summary.md`.

## Tear down

Press Ctrl+C to stop early. The script stops the `psql` and `pgbench` processes it started (by process id) and removes
its temp files. Then stop the service on the copy and drop the copy. Nothing outside the output folder is left behind.

## Proving the script

`proof/` holds what was used to prove it on a throwaway PostgreSQL. `seed-proof-rig.sql` seeds a small migrated store
(3 disabled servers, a day of rows, a 12 day backlog) and `Start-FakeRetention.ps1` plays the service's retention
DELETEs so phase detection can be seen. `proof/example-output/` is the report from that run (3 clients, about 1 tx/s,
200 seconds). Do not run either file against a real store.
