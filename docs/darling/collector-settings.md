# Collector schedules and switches

Audience: operators.

This page says why `darling.json` has no schedule settings. It also covers the verbs and MCP tools that turn a collector on or off, set when it runs, make a new password key and clean up after a removed server. It moved here from the Darling operator guide.

## No Schedule Knobs, by Design

There are deliberately **no collection-schedule settings and no per-collector retention settings** in `darling.json`. The service consumes the shared per-collector defaults (`CollectorScheduleDefaults`) — the same cadences and retention horizons a fresh Lite install uses, identity-pinned by tests so the two editions cannot drift. If a schedule knob is ever genuinely needed, it will be added then, not speculatively.

One retention setting does exist in the file: `planContentRetentionDays`. It sets how many days a stored plan outlives its last sighting before it can be removed. The default is `21`, `0` turns it off, and any other value is held to 7 through 365. It is a first-run seed (see [Where each setting lives](configuration.md#where-each-setting-lives-first-run-seed-or-file-only)), so after the first start you change it in the store. The plan's query statistics keep their normal retention. A plan older than this limit shows as a missing plan.

The overrides that do exist live in the **store**, not the file. `config.config_collector_schedules` is a sparse table where an absent row means "the code default". The Viewer's Collector Schedules window is where cadence, retention, database scope, the enabled flag and the run time are edited, per fleet or per server. The two columns an operator *must* be able to change without a desktop are the enabled flag and the run time. The next two sections are those paths.

### What the run time sets

The run time sets one thing: the time of day a collector that runs once a day or less often starts. The `config.config_collector_run_times` table holds it, one row per collector (fleet-wide or for one server). The table stores it as minutes after midnight on the *monitored server's own clock*. A collector with no row has no run time.

The Viewer's Collector Schedules window writes the run time in the same Save as the schedule rows. Three actions delete the run times of the schedule they reset (the shipped defaults have no fixed time):

- The window's Reset to Defaults.
- A server's Use default schedule.
- Apply Default to All Servers.

A Viewer released before the table existed resets the schedule rows only. A run time set here therefore survives a Reset done in an older Viewer.

The Viewer and the CLI take the run time as a 24-hour `HH:MM` time such as `02:00`.

Without a run time, such a collector starts when the service happened to start. That can be the busiest hour of the server's day. With one, it starts in the hour you chose.

A run time layers like the other columns: the server's own value, then the fleet-wide one, then no fixed time. On a server's row, `None` (stored as `-1`) stops a fleet-wide time on that server only. `Use default` (NULL) lets the next level apply.

A run time works only when the collector's effective interval is a whole number of days (1440 minutes or a multiple of it). On an hourly collector a time only picks the minute past the hour. The Viewer and the CLI refuse it and tell you to change the frequency or clear the time.

The Viewer's editor shows what the time means in server time and in UTC. A fleet row shows the spread, and a server row shows its next run. For an Azure SQL Database, which always reports UTC, it says so.

### What stays fixed

Two numbers around the run time are not settings.

- Each server starts at the run time plus a fixed offset of under 60 minutes. The offset depends only on the server id, so it is the same after every restart. A fleet-wide `02:00` spreads the fleet across 02:00 to 03:00 instead of starting every server in the same minute.
- A run can start from that slot until 60 minutes after it. A collector that starts inside that hour runs at once. One that cannot (the service was down or busy) waits for the next day's slot. A missed day is skipped, not replayed. A run held by the concurrency cap starts when a place under the cap frees, which can be after the grace hour.

The time follows the server's clock through a daylight-saving change, so it stays at the same local time.

## Enable or disable a collector headlessly (`--enable-collector` / `--disable-collector`)

One collector ships **off**: `long_query_completions`, the long-running-query completion trace. A completion trace fires per statement or batch even though the duration predicate discards most of them, so it is opt-in per fleet — and opting in is a real action on the monitored servers: **enabling it creates this install's own long-query Extended Events session (`PerformanceMonitor_Darling_<id>_LongQueryCompletions`) on the next per-server sweep, and disabling it drops that session there** (the `CollectorScheduleDefaults` entry for it says exactly this, and `DarlingXeSessions.ReconcileLongQueryCompletionsAsync` is the code that follows the flag both ways). On-premises, disabling it keeps the session while another registration of this install on the same instance still has the trace on. On Azure SQL Database the session is database-scoped. Enabling it creates the session in each monitored database and drops it from any other database that has it. Disabling it drops the session from every database that has it. Neither drop removes the session from a database where another registration of that database still has the trace on. A database that is also monitored as its own server is left to that registration. Removing the server also drops this install's session on it, with one attempt within 15 seconds, unless another registration of this install keeps it. If that attempt failed or ran out of time, run [`--drop-xe-sessions --print-sql`](#drop-the-extended-events-sessions-a-removed-server-left-behind---drop-xe-sessions) and run the statements it prints on the server. Until [#3752](https://github.com/erikdarlingdata/PerformanceMonitor/issues/3752) the only writer of that flag was the Viewer's Collector Schedules window — an interactive desktop plus the `admin` role, which a remote viewer seat does not hold by default (`network.role` defaults to `viewer`). Two verbs, run on the **service host** (or, in bring-your-own mode, anywhere the store is reachable), close the gap:

```
PerformanceMonitor.Darling.Service.exe --enable-collector long_query_completions
PerformanceMonitor.Darling.Service.exe --disable-collector long_query_completions
PerformanceMonitor.Darling.Service.exe --enable-collector long_query_completions --server sql01
```

Without `--server` the change is **fleet-wide** — the schedule row with no server, which every server inherits unless it has a row of its own. `--server <name>` writes that one server's row instead; the name is the display name or storage name the Viewer's sidebar and the MCP tools show, matched exactly first and as a partial name only when the partial is unique — an ambiguous fragment is refused with the candidates listed rather than guessed at, since a toggle landing on the wrong sibling is the same coin flip `remove_server` refuses to make. Only the `enabled` flag is written: a cadence or retention override already on the row survives. The verb owns no SQL of its own — it hands the same `enable_collector` / `disable_collector` command the store's command plane carries to the service's own command executor and runs the executor's plan, so an unknown collector name is refused with the executor's message and the full list of known collectors, and the upsert is one implementation rather than two. After the write it reads the collector's rows **back from the store** and prints them — scope, enabled, cadence, retention, database scope — so what you see is what the service will resolve, not an echo of what you typed. The write bumps the config version the running service polls every sweep, so it takes effect within one sweep with no restart. Add `--config <path>` if `darling.json` is not next to the exe and `DARLING_CONFIG` is not set. Exit `0` only when the row was written and read back, so it works as a step in a provisioning script. A failure exits `1` for a usage or configuration problem and `2` when the store cannot be reached, refuses the login, or refuses the change (a wrong password exits `2`, not `1`), so a script can tell a problem with its own arguments or config from a problem at the store. Managed mode needs Windows (the store credential is DPAPI); a bring-your-own store works on any platform, exactly as `--add-server` does. There is deliberately **no MCP tool** for this: the MCP surface has config writers (`add_servers`, alert tuning, custom views) but no collector-schedule writer, and adding one is a product decision the verb does not take — the CLI was the smallest surface that closes the headless gap.

## Make a new password key (`--reset-password-key`)

The service keeps one key pair for the SQL login passwords saved in the store: it publishes the public half, and every place that saves a password seals it to that key. The verb marks the current key replaced, so the next service start retires the old key file and makes a new key.

```
PerformanceMonitor.Darling.Service.exe --reset-password-key
PerformanceMonitor.Darling.Service.exe --reset-password-key --config /etc/darling/darling.json
```

It changes the store only, as the store owner, and never touches a key file. It prints how many saved passwords were sealed to the old key: restart the service, then enter those passwords again.

## Set the time of day a daily collector runs (`--set-collector-run-at`)

A collector that runs once a day or less often, such as `index_object_stats`, can start at a time you choose. The verb writes the same row as the Viewer's Collector Schedules window. It takes a collector name and one of three values:

```
PerformanceMonitor.Darling.Service.exe --set-collector-run-at index_object_stats 02:00
PerformanceMonitor.Darling.Service.exe --set-collector-run-at index_object_stats 03:30 --server sql01
PerformanceMonitor.Darling.Service.exe --set-collector-run-at index_object_stats none --server sql01
PerformanceMonitor.Darling.Service.exe --set-collector-run-at index_object_stats default --server sql01
```

- `HH:MM` is a 24-hour time from `00:00` to `23:59` on the monitored server's own clock.
- `none` on a server's row keeps the fleet-wide time from applying to that server. On the fleet-wide row it clears the time.
- `default` clears the row's time so the next level applies again. A server's row falls back to the fleet-wide time. It creates no row.

Without `--server` the change is **fleet-wide**. `--server <name>` writes that server's own row, with the name matched as `--enable-collector` matches it. Only the run time is written. The enabled flag, cadence and retention already on the row are kept.

The verb refuses a time that is not a 24-hour `HH:MM` time. It also refuses a time on a collector whose cadence, for the row being written, is more often than once a day. In both cases it prints the text the Viewer shows, exits `1` and changes nothing. `none` and `default` are never refused, because clearing is the remedy that refusal names.

After the write the verb reads the collector's rows back from the store and prints them. A row that carries a time prints `run_at=02:00 server time`, so what you see is what the service will resolve. The exit codes, `--config` and the managed-mode rule are those of `--enable-collector`.

The write needs the run-time table, which the service adds when it starts. If the service has not migrated the store yet, the verb says so. It tells you to start the service once to migrate the store, then run the command again. It exits `1` and changes nothing. `--enable-collector` and `--disable-collector` read back without the `run_at=` part instead of failing.

## Stop, start or reschedule a collector from MCP (#6227)

With `mcp.allowWrites` on (and a token), seven MCP tools change collector schedules, and one read shows the result. Each write takes a `collector_name` and an optional `server_name`. Without a server it changes the fleet-wide row, which every server inherits unless it has a row of its own.

| Tool | What it does |
|---|---|
| `disable_collector` / `enable_collector` | Sets the collector's enabled flag. Data already collected stays. |
| `set_collector_frequency` | Sets `frequency_minutes` (0 = once when the server loads), or `clear: true` to remove the override. |
| `set_collector_run_at` | Sets the time of day a once-a-day collector starts (`HH:MM`, `none` or `default`), as `--set-collector-run-at` does. |
| `set_collector_retention` | Sets `retention_days` for the whole fleet, or `clear: true` to remove the override. Retention is fleet-wide, so a number with `server_name` is refused. `clear: true` with `server_name` removes a per-server value an older build stored. |
| `set_collector_databases` | Sets the collector's database scope (the editor's Databases column) from a comma-separated `databases` list, or `clear: true`. On a server, `databases: ""` is the explicit "no scope" that stops a fleet-wide scope reaching it. Only the collectors that fan out per database read it. |
| `reset_collector_schedules` | The editor's Reset to Defaults: deletes every override row and run time of one scope (the fleet-wide rows with no `server_name`, else that server's), and lists what it removed. The other scope is left alone. |
| `get_collector_schedules` | Lists the stored overrides (enabled, frequency, retention, run time, database scope), for one collector or all, fleet-wide and per server. A read: served without `mcp.allowWrites`. |

The writes run the same plans as `--enable-collector`, `--disable-collector` and `--set-collector-run-at`, and refuse the same things: an unknown collector name, a frequency that is negative or past the cap for a collector that stores deltas, a retention under 1 day, a run time on a collector that runs more often than daily. A refusal changes nothing and says why. A write changes only its own column: the other columns on the row are kept, and a row the tool creates never switches a collector on or off. Each answer gives what changed, the previous value and the exact call that undoes it (`undo`). The service picks the change up on its next config poll; there is no restart.

These tools are never offered to an agent that Darling sends to investigate an alert: it may read with `get_collector_schedules` and may not stop, start or reschedule collection.

The `mcp` database role gets `INSERT, UPDATE, DELETE` on `config.config_collector_schedules` and `config.config_collector_run_times` when the service provisions roles (every start). A bring-your-own store needs `tools/provision-roles.sql` run again to add the same two grants.

## Set the service flags from MCP (#6393)

With `mcp.allowWrites` on (and a token), `update_service_flags` sets the collection flags the Viewer's Settings window saves under Data Collection. Send only the ones to change; an omitted argument keeps the stored value.

| Argument | Meaning | Range |
|---|---|---|
| `capture_plans` | Global plan capture. | true or false |
| `query_store_backfill_enabled` | Query Store history backfill. | true or false |
| `query_store_text_budget_mb` | Query Store text budget. | 4 to 256 |
| `max_concurrent_sweeps` | Server sweeps that run at once. | 1 to 16 |
| `query_store_fetch_top_n` | Plans per database-hour and metric the Query Store collector fetches. | 0 to 100000 (0 = every new plan) |

The ranges are the Settings window's: both call one class (`ServiceFlagLimits`). An out-of-range value answers `invalid`, names the argument and its range, and changes nothing, including the other flags in the same call. The answer carries all five values before and after. The running service reloads on its next sweep.

The MCP on/off flag and port, the web on/off flag and port, and pause are not settable from MCP: turning MCP off from MCP would lock the admin out, and the `mcp` role's column grant covers only these five columns. Change those in the Viewer. Like the schedule tools, `update_service_flags` is never offered to an investigation agent. Lite's MCP server has no write tools, so none of this applies to Lite.

The service grants the `mcp` role `UPDATE` on exactly those five `config.config_service` columns when it provisions roles (every start). A bring-your-own store needs `tools/provision-roles.sql` run again to add that grant; until then `update_service_flags` answers `error` with a permission failure from the store.

## Drop the Extended Events sessions a removed server left behind (`--drop-xe-sessions`)

Removing a server (`remove_server`, the Viewer's Manage Servers dialog) stops collection and drops this install's own Extended Events sessions on the server. The service makes one attempt within 15 seconds. It leaves a session that another registration of this install keeps. A drop that fails or times out never stops the removal. Each install has an id, and the sessions it creates carry that id: `PerformanceMonitor_Darling_<id>_LongQueryCompletions` and, on Azure SQL Database, its own `_Deadlock`, `_BlockedProcess` and `_Attention` sessions. The service never drops the shared sessions: `PerformanceMonitor_Deadlock`, `PerformanceMonitor_BlockedProcess`, `PerformanceMonitor_Attention` and the old shared `PerformanceMonitor_LongQueryCompletions`. Lite, the deprecated Full Dashboard installer and any other Darling service that monitors the same server use those names too. Each session has a 4 MB ring buffer. The long query completions session also tests every completed statement and batch against its duration filter, which is why that collector is opt-in. This verb drops the shared sessions. It also drops this install's own sessions on a server that is still monitored. For a session a removal failed to drop, because the server was unreachable or the attempt timed out, use `--print-sql`. When nothing else monitors the server, run the named form **just before you remove the server**:

```
PerformanceMonitor.Darling.Service.exe --drop-xe-sessions sql01 --dry-run
PerformanceMonitor.Darling.Service.exe --drop-xe-sessions sql01
PerformanceMonitor.Darling.Service.exe --drop-xe-sessions --print-sql
```

`--drop-xe-sessions <server-name>` finds the server the way `--validate-config` does (the store's registry, or `darling.json`'s list when the store cannot be reached), connects with the same connector the service uses and the credentials in the configuration (none are ever taken from arguments), lists which of Darling's sessions exist (the shared names and this install's own), and drops each with `DROP EVENT SESSION [name] ON SERVER`. On Azure SQL Database it does the same for the database-scoped copies in each monitored database (`ON DATABASE`). For the long query completions session it also searches the databases the server excludes, because a session created before a database was excluded stays there with nothing else to drop it. It leaves master, a database that is monitored as its own server, and a database where another registration of the same logical server keeps the session. A registration keeps it when its long query setting is on. That setting is its own schedule row, else the fleet-wide row, else the collector's default, which is off. If the verb cannot read the schedule rows, it counts every other registration as keeping it. On any other server, the verb can leave this install's long query session in place. It does that, with a note on stderr, when another registration of this install on the same instance has the trace on. It matches registrations by the instance name each one last reported, and a name it does not know matches nothing. `--dry-run` lists them and drops nothing; `--config <path>` names a `darling.json`. Only the shared names and this install's own names are ever dropped. The verb reads this install's id from the store and never writes it, because the service makes the id when it starts. With no id, it leaves every session named for an install alone and says so. It lists the sessions of other installs under a heading, each with a guarded `DROP` statement, and never runs them. Run one only when that install no longer monitors the server. A database the server excludes that the login cannot open (no user in it, a paused serverless database) is reported in a note on stderr and does not change the exit code, so a long query completions session left in it needs a manual drop; a monitored database it cannot open makes the verb exit `2`.

The order matters. The named form finds the server in the store's list of enabled servers, so it reaches only a server this service still monitors. Dropping the shared sessions on such a server stops its deadlock and blocked-process capture until this service reconnects to it. The service creates a missing session when it connects. To drop the shared sessions too, run the named form just before you remove the server, and remove the server next.

On Azure SQL Database, a session can't be dropped over a read-only connection. For a registration with read-only intent, the verb first stops the session over the registration's own connection, if it runs there. It then drops the session over a connection without the intent, which reaches the primary. Every other drop is one statement over the registration's own connection. On Hyperscale with several high-availability replicas, that stop reaches only the replica the connection lands on (see the Hyperscale item under Known limits).

The login in the configuration needs the permission that [Microsoft Learn's `DROP EVENT SESSION` page](https://learn.microsoft.com/en-us/sql/t-sql/statements/drop-event-session-transact-sql) requires. On SQL Server and Azure SQL Managed Instance that is `DROP ANY EVENT SESSION` (SQL Server 2022 and later) or `ALTER ANY EVENT SESSION`; on Azure SQL Database it is `DROP ANY DATABASE EVENT SESSION` in each monitored database. A grant that only lets a login create the sessions (`CREATE ANY DATABASE EVENT SESSION` on Azure SQL Database, for one) does not cover the drop. When the login lacks the grant, the server refuses each drop, the verb prints `[FAILED]` for it, carries on with the next, and exits `2`.

A server that is no longer configured has no definition and no credentials left to connect with, so the named form works only until `remove_server` (or the Manage Servers dialog) removes the server, and a name that matches nothing exits `1` and points at `--print-sql`: `--drop-xe-sessions --print-sql` connects to nothing and prints guarded statements (`IF EXISTS` against `sys.server_event_sessions`, and against `sys.database_event_sessions` for Azure SQL Database) for the three shared names in both scopes, and a query per scope that lists the sessions named for any install, each with its `DROP` statement, to run on the server yourself. Exit codes: `0` when everything found was dropped (or listed) or nothing was there, `1` for a usage, configuration or unknown-name problem, `2` when the server cannot be reached or searched or a drop is refused.

Both modes print one warning, before they list or drop anything: a Lite app, a deprecated Full Dashboard install or another Darling service that still monitors the server uses the same names and creates a missing session again (Lite on its next collection cycle, the Dashboard on its next collection run, Darling on its next connect to the server or, for the long query completions session, within an hour; the long query completions session only where that collector is turned on, and never by the Dashboard), so run this only once nothing else monitors it. A run that drops at least one session also prints one `NOTE:` line after the drops: capture of deadlocks and blocked processes on that server stops until this service reconnects to it, so remove the server next. The line names only the captures whose session was dropped (`long query completions` when that session went too). `--dry-run` and `--print-sql` do not print it.
