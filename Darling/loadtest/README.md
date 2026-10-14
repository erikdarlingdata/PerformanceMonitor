# Load test for the Query Store tables of a Darling store

This runbook finds the largest number of monitored servers that one Darling store can serve. It is part 2 of [#5794](https://github.com/erikdarlingdata/PerformanceMonitor/issues/5794). The test raises the Query Store interval tables of a restored store to N times their rows. Then it measures the reads, the background jobs and the writes at each N.

The release package does not include this folder. Run the scripts from a clone of the repository.

| File | What it does |
|---|---|
| `inflate-query-store.sql` | Clones the rows of each server in the six Query Store interval tables into N - 1 synthetic servers. It prints the rows and bytes of each partition before and after. |
| `measure.ps1` | Runs the measurement steps and prints a Markdown table for each step. |
| `measure-*.sql` | The SQL behind the store steps of `measure.ps1`. You can also run each one with `psql`. |
| `teardown.sql` | Removes the synthetic rows and the `loadtest` schema from a copy that you keep. |

## Prerequisites

- A restored copy of a store like store A in #5794: SQL Servers with Query Store collected, and TimescaleDB. Never run these scripts on a store that a service uses for monitoring.
- A host for the copy with at least the CPU and RAM of the source store's host. Store A's host has 8 vCPUs and 64 GB of RAM.
- The same PostgreSQL and TimescaleDB versions as the source store, and `psql` on the host that runs the scripts.
- PowerShell 7, for `measure.ps1`.
- The Darling service of the same version as the source store, configured on the copy. Turn on MCP and the web dashboard, and keep their tokens.
- One SQL Server that is not a production server and has Query Store turned on. The service collects from it during the write step: put its name where the steps below say `<test server>`.
- Disk for the copy. The disk must hold the store plus N times its Query Store interval tables.

This is the disk calculation for store A at N = 10:

| Item | Size |
|---|---|
| The restored store | 496 GiB |
| N x the Query Store interval tables: 10 x 150 GiB | 1,500 GiB |
| Total | 1,996 GiB, about 2 TiB |
| With 25% free for WAL, the compression step and temporary files | about 2.5 TiB |

The clones need N - 1 times the tables, so the term "N times" leaves room for one day of the wide table in the compression step.

## Steps

### Prepare the copy

1. Restore the copy on the load-test host.
2. Before the service starts on the copy, disable every monitored server except the test server. The service must not connect to production servers from the copy:

    ```sql
    UPDATE config.config_monitored_servers SET is_enabled = FALSE WHERE server_name <> '<test server>';
    ```

3. Remove the notification channels from the copy's service configuration. An alert from the copy must not reach anybody.
4. Start the service on the copy. Let it run for one hour, so that every hourly job runs once.
5. Run the baseline at N = 1, which is the copy with no clones. Use the steps in "Measure at each N".

### Inflate to N

1. Stop the service on the copy. A clone of the newest partition is then complete.
2. Run the inflate script with the next N:

    ```text
    psql -X -q -v ON_ERROR_STOP=1 -v n=2 -d "<copy connection string>" -f inflate-query-store.sql
    ```

3. Save the two tables that the script prints: "Rows and bytes per partition" and "Totals per table".
4. Start the service. Let it run for one hour before you measure.

We recommend N = 2, 4, 6, 8 and 10, in that order. A run with a larger N adds only the new copies. A second run with the same N adds no rows. If a run stops, run it again with the same N, and it continues from the last partition that it completed.

### Measure at each N

1. Run the reads, with a real server name from the copy:

    ```powershell
    ./measure.ps1 -Psql psql -Store '<copy connection string>' -Steps Reads -ServerName '<server>' -McpToken '<token>' -WebToken '<token>' -CustomViewSpec panel.json
    ```

    The step calls the four MCP Query Store tools (`get_query_store_top`, `get_query_store_regressions`, `get_query_store_duration_trend` and `get_query_store_query_history`) with 1-day and 7-day windows. It calls the first three through the web dashboard routes that the Query Store page uses. It runs the Custom Views panel in `panel.json`, which is the one-day panel on the wide table from #5582. It prints the time of each call and the slow reads that the service recorded. It also prints a UTC start time.
2. Wait for the next full hour. The service writes `collect.read_latency` once an hour.
3. Run the read times, with the start time from step 1:

    ```powershell
    ./measure.ps1 -Psql psql -Store '<copy connection string>' -Steps ReadLatency -Since '<start time>'
    ```

4. Run the jobs, the writes and the compression candidate:

    ```powershell
    ./measure.ps1 -Psql psql -Store '<copy connection string>' -Steps Jobs,Writes,Compression -McpToken '<token>'
    ```

    The compression step copies one closed day of `collect.query_store_interval_wide` into a compressed TimescaleDB hypertable. It prints the size and the time of a one-day scan for the plain partition and for the compressed copy. The default day is yesterday in UTC. Use `-Day yyyy-MM-dd` for another day.

### What each step compares with

| Step | Limit |
|---|---|
| Reads through MCP and the web dashboard | 5 s. A read at or above 5 s is a slow read (`SlowReadLog.DefaultThresholdMs`). |
| A Custom Views panel | 45 s, which is 75% of its 60 s statement timeout (`ComposeSpec`). |
| Hourly jobs | The `hourly_total` column of the Jobs step must stay well under one hour. |
| Writes | The Query Store collector's `max_duration_ms` must stay under its collection interval. |

## What to record

For each N, record these tables:

- The "Totals per table" table from the inflate script.
- The client times and the slow reads from the Reads step.
- The ReadLatency table, with its `verdict` column.
- The Jobs, Writes and Compression tables.

Also record the host: vCPUs, RAM, disk type, and the PostgreSQL and TimescaleDB versions.

## The result to post on #5794

Post one table, with one row for each N, and the step tables under it:

| N | Servers in the store (N x source servers) | Query Store tables | Slowest 1-day read (ms) | Slowest 7-day read (ms) | Custom Views panel (ms) | Hourly jobs total | Query Store write, max (ms) | Wide day: plain, compressed | 1-day scan: plain, compressed (ms) | Every limit met |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 43 | | | | | | | | | |
| 2 | 86 | | | | | | | | | |

### The server count per store

Find the largest N at which every read meets its limit and the hourly jobs fit the hour. The server count per store is that N times the server count of the copy:

```text
servers per store = largest passing N x servers in the copy
```

For example, if N = 4 passes and N = 6 fails on a copy of store A, the store serves 4 x 43 = 172 servers. Record the step that failed first at the next N. That step decides which candidate in section 3 of #5794 to build.

## Known limits of the clones

- A synthetic server has a `collect.servers` row but no row in `config.config_monitored_servers`, so the service does not connect to it. On its next configuration reload, the service marks the synthetic server as disabled. A read that selects only enabled servers skips the synthetic rows. Check the row counts in the Custom Views panel result.
- The script clones the interval tables only. The query text and plan tables are not cloned, so a read that joins them finds no text for a synthetic row. The scan of the interval rows is the same.
- A late row in `collect.query_store_interval_latest` fires the plan regression trigger, so the service builds the daily plan regressions for the synthetic servers too. A real estate of that size has the same load.
- The service's retention removes Query Store interval partitions older than 9 days (wide) and 15 days (latest). The clones go with them. Finish each N within a few hours of its inflate run.

## Teardown

1. Stop the service on the copy.
2. Drop the copy, and release its disk.
3. If you must keep the copy, run `teardown.sql` instead. It deletes the synthetic rows and servers and drops the `loadtest` schema. It reads every partition, so it takes about as long as the inflate run.
4. Delete the MCP and web tokens of the copy's service.
