# Load test for the Query Store tables of a Darling store

This runbook finds the largest number of monitored servers that one Darling store can serve. It is part 2 of [#5794](https://github.com/erikdarlingdata/PerformanceMonitor/issues/5794). The test raises the Query Store interval tables of a restored store to N times their rows. Then it measures the reads, the hourly work and the writes at each N.

The release package does not include this folder. Run the scripts from a clone of the repository.

| File | What it does |
|---|---|
| `inflate-query-store.sql` | Clones the rows of each server in the six Query Store interval tables into N - 1 synthetic servers. It adds each synthetic server to both server registries. It prints the rows added and the bytes of each partition. |
| `measure.ps1` | Runs the measurement steps and prints a Markdown table for each step. |
| `measure-*.sql` | The SQL behind the store steps of `measure.ps1`. You can also run each one with `psql`. |
| `teardown.sql` | Removes the synthetic rows, the synthetic servers and the `loadtest` schema from a copy that you keep. |

## Prerequisites

- A restored copy of a store like store A in #5794: SQL Servers with Query Store collected, and TimescaleDB. Never run these scripts on a store that a service uses for monitoring.
- A host for the copy with at least the CPU and RAM of the source store's host. Store A's host has 8 vCPUs and 64 GB of RAM.
- A network for the copy host with no route to the production servers. Use a firewall rule that blocks the host's traffic to them, or an isolated network. The service on the copy keeps every server enabled, so this is the first guard. Step 2 below checks it.
- The same PostgreSQL and TimescaleDB versions as the source store, and `psql` on the host that runs the scripts.
- PowerShell 7, for `measure.ps1`.
- The Darling service of the same version as the source store, configured on the copy. Turn on MCP and the web dashboard, and keep their tokens.
- One SQL Server that is not a production server and has Query Store turned on. The copy host must be able to reach it. The service collects from it during the write step: put its name where the steps below say `<test server>`.
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

Keep every monitored server enabled. A disabled server drops out of the reads and of the hourly Query Store builders. A copy with disabled servers measures a store with one server, not the store you restored.

1. Restore the copy on the load-test host. Do not start the service yet.
2. Prove that the copy host cannot reach the production servers. Run this on the copy host. It prints one line for each server that it can reach. If it prints any line, stop and fix the firewall or the network first:

    ```powershell
    $rows = & psql -X -A -t -F '|' -d '<copy connection string>' -c "SELECT host, coalesce(port::text, '') FROM config.config_monitored_servers WHERE name <> '<test server>'"
    foreach ($row in $rows) {
        $hostName, $port = $row.Split('|')
        $hostName = ($hostName -split '[\\,]')[0]
        $ports = if ($port) { @([int] $port) } else { @(1433, 5432) }
        foreach ($p in $ports) {
            $client = [System.Net.Sockets.TcpClient]::new()
            try { $open = $client.ConnectAsync($hostName, $p).Wait(3000) } catch { $open = $false } finally { $client.Dispose() }
            if ($open) { Write-Host "REACHABLE: ${hostName}:$p" }
        }
    }
    ```

3. The second guard: point every production server at a host name that never resolves. The rows stay enabled, so the readers and the builders still see every server. Each collector fails at once on the name. The `.invalid` domain is reserved for this (RFC 2606):

    ```sql
    UPDATE config.config_monitored_servers
    SET host = 'loadtest.invalid', modified_at = (now() AT TIME ZONE 'UTC')
    WHERE name <> '<test server>';
    ```

    The server ids stay the same, because the store keeps each id and does not compute it again from the host. The service then writes a failed collection for each production server on each cycle. That is part of the load of the copy.
4. Remove the notification channels from the copy. An alert from the copy must not reach anybody. Every production server fails each cycle, so without this step the copy sends a "server unreachable" alert for each of them. The store holds the channels, and after the first start the service reads them from the store, not from `darling.json`. Run this on the copy before you start the service:

    ```sql
    UPDATE config.config_notification
    SET smtp_host = '', smtp_recipients = '', teams_url = '', slack_url = '', generic_url = '', pagerduty_routing_key = '',
        modified_at = (now() AT TIME ZONE 'UTC');
    DELETE FROM config.config_notification_routes;
    ```

    Then check that no channel is left. The query must print `0 | 0`:

    ```sql
    SELECT
        (SELECT count(*) FROM config.config_notification
         WHERE smtp_host <> '' OR smtp_recipients <> '' OR teams_url <> '' OR slack_url <> ''
         OR generic_url <> '' OR pagerduty_routing_key <> '') AS channels,
        (SELECT count(*) FROM config.config_notification_routes) AS routes;
    ```

    Also remove the channels from the `darling.json` of the copy's service. If the store has no channel row, the service fills it from that file at the first start.
5. Start the service on the copy. Let it run for one hour, so that every hourly job runs once.
6. Run the baseline at N = 1, which is the copy with no clones. Use the steps in "Measure at each N".

#### The time limit of a copy

The copy gets no new Query Store rows, because its production servers cannot be reached. The reads of the test end at the newest Query Store row of the server (see "Measure at each N"), so their windows stay full. But the service still purges rows by their age each day. You cannot change the purge age: it is fixed in the code, at 9 days for `collect.query_store_interval_wide` and 15 days for `collect.query_store_interval_latest`. No setting raises it. So:

- The 7-day reads stay complete for 2 days after the newest row of the copy. The 1-day reads stay complete for 8 days.
- The Reads step prints a `WARNING` line for each window that starts before the purge edge. When it does, the times of that window are too fast. Restore a newer copy, and start the sequence of N again.
- Plan the whole sequence of N to finish within 2 days of the backup, or restore a new copy in the middle of it.

### Inflate to N

1. Stop the service on the copy. A clone of the newest partition is then complete, and the script can change the triggers of the two tables.
2. Run the inflate script with the next N:

    ```text
    psql -X -q -v ON_ERROR_STOP=1 -v n=2 -d "<copy connection string>" -f inflate-query-store.sql
    ```

    The script adds each synthetic server, enabled, to `collect.servers` and to `config.config_monitored_servers`, with the host `loadtest.invalid`. Use `-v host=<name>` for another host name that never resolves.
3. Save the three tables that the script prints: "Rows and bytes per partition", "Totals per table" and "Synthetic servers". The `source_servers` column of the last one is the server count for the formula in "The server count per store".
4. Start the service. Run the Jobs step once an hour. Wait for a `deferred` count of 0 on the three Query Store builders. Then wait one more hour, and measure. After an inflate, the builders first build the data of the synthetic servers. The test does this work once for each N. A real store of that size does it a little at a time.

We recommend N = 2, 4, 6, 8 and 10, in that order. A run with a larger N adds only the new copies. A second run with the same N adds no rows. If a run stops, run it again with the same N, and it continues from the last partition that it completed.

#### The triggers during the inflate

Two row triggers fire on each late row. The one on `collect.query_store_interval_wide` marks the hours of the compose rollup to build again. The one on `collect.query_store_interval_latest` marks the days of the plan regression totals to build again. On a clone of a large partition, the triggers run for hours and hold one pending trigger event for each row in memory.

So the script saves the state of each user trigger of the two tables and their partitions in `loadtest.trigger_state`, and then disables them. At the end, also when the inflate fails with an error, it sets each trigger back to its saved state. A trigger that was disabled before the run stays disabled.

If psql itself is stopped during the run (Ctrl-C, or a closed window), the triggers stay disabled. Run this statement on the copy before you start the service. It sets each trigger back to its saved state:

```sql
CALL loadtest.restore_triggers();
```

You can also run the inflate again with the same N: it keeps the saved states and sets them back at its end.

The script then does the work of the triggers itself, so the builders rebuild the same data:

- Compose rollup: the rollup has built some hours for each source server. The script marks those hours of each synthetic copy as late, in one statement. The rollup's next ticks build those hours again with the clones in them. If a run stops after it copies the wide rows and before this mark, the next run makes the mark.
- Plan regression totals: no mark is needed. The builder builds each day of an enabled server that has no built row, and the synthetic servers have none.
- Query Store top daily summary: no trigger. The builder builds each day of an enabled server that has no built row.

#### The cost of the inflate

Each copy reads its source partition again. That is the price of a run that can stop and continue from the last partition it completed. A run from N = 1 to N reads each partition N - 1 times. For store A at N = 10, the script reads 9 x 150 GiB = 1,350 GiB and writes 1,350 GiB of new rows. A run from N = 8 to N = 10 reads each partition twice (copies 8 and 9). The partition report itself reads no rows: its row counts come from the catalog and from the rows that the script added.

### Measure at each N

1. Run the reads, with a real server name from the copy:

    ```powershell
    ./measure.ps1 -Psql psql -Store '<copy connection string>' -Steps Reads -ServerName '<server>' -McpToken '<token>' -WebToken '<token>' -CustomViewSpec panel.json
    ```

    The step calls the four MCP Query Store tools (`get_query_store_top`, `get_query_store_regressions`, `get_query_store_duration_trend` and `get_query_store_query_history`) with 1-day and 7-day windows. It calls the first three through the web dashboard routes that the Query Store page uses. It runs the Custom Views panel in `panel.json`, which is the one-day panel on the wide table from #5582. It prints the time of each call and the slow reads that the service recorded. It also prints a UTC start time.

    Each window ends one second after the newest Query Store row of the server, not at the current time. The step passes that time as `as_of` to the MCP tools and the web routes. It also passes it as `windowEnd` to the panel, unless `panel.json` sets its own window. The `as_of` column of the table shows it. If the server has no query in the day before that time, the step prints a line that says it skips `get_query_store_query_history`.

    A read that answers with a `status` of `error`, `invalid`, `not_found`, `unavailable`, `precondition`, `not_collected` or a timeout is listed as `failed`, with the message. A read like that returns in a few milliseconds, so it is not a fast read. Check that the outcome column says `ok` for every read before you use the times.
2. Wait for the next full hour. The service writes `collect.read_latency` once an hour.
3. Run the read times, with the start time from step 1:

    ```powershell
    ./measure.ps1 -Psql psql -Store '<copy connection string>' -Steps ReadLatency -Since '<start time>'
    ```

4. Run the jobs, the writes and the compression candidate. `-ServiceLog` is today's log file of the service on the copy, `darling-service_yyyyMMdd.log` in its log folder:

    ```powershell
    ./measure.ps1 -Psql psql -Store '<copy connection string>' -Steps Jobs,Writes,Compression -McpToken '<token>' -ServiceLog '<log folder>/darling-service_yyyyMMdd.log'
    ```

    The Jobs step prints the hourly work that grows with N. Four jobs write one line to the service log on each hourly tick, with the time they took in ms:

    - the plan regression daily totals
    - the Query Store top daily summary
    - the compose rollup
    - the store object convergence pass, which holds the partition upkeep of the Query Store tables.

    The step prints those lines for the last 2 hours (`-Hours` sets the window). It also prints `hourly_total_ms`, the sum of the newest run of each job. A job that fails writes no line, so the total leaves it out. So the step also prints a line that names each job with no line in the window. Then it prints the daily retention purge from the collection log, and the TimescaleDB aggregate refresh and compression jobs. Those jobs do not read the Query Store interval tables, so we expect their times to stay the same at each N.

    The compression step copies one closed day of `collect.query_store_interval_wide` into a compressed TimescaleDB hypertable. It prints the size and the time of a one-day scan for the plain partition and for the compressed copy. The default day is yesterday in UTC. Use `-Day yyyy-MM-dd` for another day.

### What each step compares with

| Step | Limit |
|---|---|
| Reads through MCP and the web dashboard | 5 s. A read at or above 5 s is a slow read (`SlowReadLog.DefaultThresholdMs`). Every outcome must be `ok`. |
| A Custom Views panel | 45 s, which is 75% of its 60 s statement timeout (`ComposeSpec`). |
| Hourly work | `hourly_total_ms` from the Jobs step must stay well under one hour. It is the time of the three Query Store builders and the convergence pass on one tick. A `deferred` count above 0 on tick after tick is a backlog that does not clear, and fails the limit. |
| Daily purge | The newest `data_retention` run must finish without an error. |
| Writes | The Query Store collector's `max_duration_ms` must stay under its collection interval. |

## What to record

For each N, record these tables:

- The "Totals per table" and "Synthetic servers" tables from the inflate script.
- The client times and the slow reads from the Reads step.
- The ReadLatency table, with its `verdict` column.
- The Jobs, Writes and Compression tables.

Also record the host: vCPUs, RAM, disk type, and the PostgreSQL and TimescaleDB versions.

## The result to post on #5794

Post one table, with one row for each N, and the step tables under it. `source_servers` is the value that the inflate script prints:

| N | Servers in the store (N x source_servers) | Query Store tables | Slowest 1-day read (ms) | Slowest 7-day read (ms) | Custom Views panel (ms) | Hourly work total (ms) | Query Store write, max (ms) | Wide day: plain, compressed | 1-day scan: plain, compressed (ms) | Every limit met |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | | | | | | | | | | |
| 2 | | | | | | | | | | |

### The server count per store

Find the largest N at which every read meets its limit and the hourly work fits the hour. The server count per store is that N times `source_servers`:

```text
servers per store = largest passing N x source_servers
```

`source_servers` counts only the servers that have Query Store interval rows, because the script clones only those. A copy of store A has 43 servers. If all 43 have Query Store rows, the script prints 43. Then if N = 4 passes and N = 6 fails, the store serves 4 x 43 = 172 servers like them. Record the step that failed first at the next N. That step decides which candidate in section 3 of #5794 to build.

## Known limits of the clones

- A synthetic server is enabled in both registries, with a host name that never resolves. The service tries to collect from it on each cycle and writes a failed collection, as it does for the production servers of the copy. The readers, the builders and the Custom Views read guard see it as a normal server.
- The script clones the interval tables only. The query text and plan tables are not cloned, so a read that joins them finds no text for a synthetic row. The scan of the interval rows is the same.
- The service's retention removes Query Store interval partitions older than 9 days (wide) and 15 days (latest). The clones go with them. Finish each N within a few hours of its inflate run. See "The time limit of a copy".

## Teardown

1. Stop the service on the copy.
2. Drop the copy, and release its disk.
3. If you must keep the copy, run `teardown.sql` instead. It deletes the synthetic rows and the rows that the builders wrote for the synthetic servers. It also deletes the rows that the service wrote about the synthetic servers. These are their failed collections in `collect.collection_log`, their alerts in `config.config_alert_log`, their alert state and their slow reads. Then it deletes the synthetic servers from both registries, and the `loadtest` schema. The read times in `collect.read_latency` stay, because that table has no server column. They go when the service's retention removes them. It commits after each partition, so a run that stops can run again. It reads every partition, so it takes about as long as the inflate run. A kept copy still has the `loadtest.invalid` hosts from step 3 of "Prepare the copy".
4. Delete the MCP and web tokens of the copy's service.
