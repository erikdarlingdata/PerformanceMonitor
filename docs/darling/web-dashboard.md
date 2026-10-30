# The Darling web dashboard

Audience: operators.

The web dashboard is a browser view of the fleet. The Darling service serves it. It is off by default. This page shows how to turn it on, what it shows, and how to reach it from another machine.

Install the service first. See [Install Darling](install.md).

## Why it is off

The dashboard reads the monitoring store. A sign-in that can edit also changes monitoring settings: the server list, alert routes, mute rules and custom alert rules. Once the dashboard is on, it needs a token on loopback too, and TLS is opt-in. So the dashboard is off by default. To turn it on, you set a token and run one command.

## Turn it on

First, give it a token. An enabled dashboard with no token does not start. The service log says so and names the settings. Add a `network` block with only a token to `web` in `darling.json`. Do not add `listen`, so the dashboard stays loopback-only. Then restart the service.

```json
"web": {
  "network": {
    "encryptedToken": "<output of --encrypt-password>"
  }
}
```

To make a token, use [the PowerShell one-liner on the network endpoints page](network-endpoints.md#mcp-endpoint-assistant-over-the-lan). On Windows, keep the token in `encryptedToken`. `web.network.token` also takes the token itself, an `env:NAME` or a `file:PATH`. The references are for the compose and Linux setups.

To serve local browsers with no token instead, set `"allowTokenlessLoopback": true` in the `web` block. The dashboard then serves reads only, and every start logs a line that says so.

Then turn the dashboard on. From an **elevated** PowerShell on the service host, run:

```powershell
cd "C:\Program Files\PerformanceMonitorDarling"
.\PerformanceMonitor.Darling.Service.exe --enable-web
```

Or tick **Enable web dashboard** in the Viewer's **Settings**. Both write the same control-plane flag. The running service applies the flag within one collection sweep, with no restart.

Both routes write the store, so the service must finish the [install](install.md) at least once. On a box where the store has no seed yet, the command refuses and tells you to start the service first. It does not fall back to an edit of the file.

**Proof:**

```
web dashboard endpoint ENABLED in the control-plane store (config.config_service.web_enabled = true).
The running service applies this LIVE within one collection sweep (the write self-bumps the reload beacon) — no restart needed.

Open the web dashboard at: http://localhost:5153/
It asks for the web token. Show it with: PerformanceMonitor.Darling.Service.exe --print-web-token (run elevated).
```

Then this line appears in the service log:

```
Starting web dashboard on http://localhost:5153 (loopback only) — enabled/port from the control plane (config.config_service)
```

## Look at it

On the service host, open `http://localhost:5153/`. The dashboard shows its sign-in page. Paste the token once, and the dashboard exchanges it for a session cookie. To print the token again, run `--print-web-token` elevated.

Only with `web.allowTokenlessLoopback` on does a loopback-only dashboard skip the sign-in. It then serves reads only. A change gets a 403 response and a message that names the token setting.

**Proof:** a sidebar titled **Darling Web**, a list of your servers under its links, and fleet cards in the main pane. The links are:

- **Fleet Overview**, **Availability Groups**, **FinOps**, **Fleet Sweeps**
- **Alert History**, **Investigations**, **Alert Rules**, **Admin**
- **Custom Views**, **Mute Rules**, **Known Patterns**
- **Manage Tags**, **Query Tags**, **Query Tag Usage**, **Job History**

The **Availability Groups** link stays hidden until the store holds AG data. The `#/ag` route works regardless.

To check from a script, run:

```powershell
Invoke-RestMethod http://localhost:5153/api/ping
```

**Proof:** `status : ok`, and `collecting : True`.

`/api/ping` is the only health surface that does not read the store. For this reason it is the only one that can tell you the store is the problem. It reports the startup verdict of the collector, in four states:

| HTTP | `status` | What it means |
|---|---|---|
| 200 | `ok` | The collection loop started. |
| 200 | `starting` | The service is still bootstrapping and has not reached a verdict. This is normal for the first seconds of a start. |
| 503 | `degraded` | A startup step failed transiently and the service retries it. `step`, `attempt`, `attempts` and `detail` say which step and why. The state clears itself, or becomes `stopped`. |
| 503 | `stopped` | A startup step failed terminally. Collection will not start until the service is restarted. `detail` carries the reason that the critical log line gives. |

Point an uptime check at this route and alert on the status code. In the two 503 states the service is up, the dashboard still answers, and the Windows service still reports **Running**, but nothing is collected. `Invoke-RestMethod` throws on a 503. That is the behavior you want from a scripted check. Catch the error and read `$_.ErrorDetails.Message` for the body.

## What it does and does not do

Read this section before you file a bug. The web dashboard reads the store as the least-privilege `viewer` role. Its `/api/read/*` surface comes from the MCP tool catalog, minus an explicit exclusion list. These exclusions matter to you:

- No plan analysis. The whole `analyze_query_plan`, `analyze_procedure_plan`, `analyze_query_store_plan` and `analyze_plan_xml` family is excluded. Use the [WPF viewer](viewer.md) for plans. It has the graphical plan viewer, the block-chain reconstruction and the deadlock-graph renderer.
- No `analyze_server`. That tool makes a live outbound connection to a monitored server, and the web host does not.
- No live "Current Active Queries" fetch, no actual-plan capture and no purge. Each of these is a `config_command` enqueue (see [Read-only versus read-write seats](viewer.md#read-only-versus-read-write-seats)). The web host has no route that queues them.

The web host also writes to the monitoring store. Everything below changes monitoring configuration or monitoring state. This page does not cover which seats can use each one.

- Custom Views: create, edit and delete, and preview one with `POST /api/compose/run`.
- Mute rules: add, edit, enable or disable, and delete.
- Alert rules: custom alert rules (add, edit, delete, validate and test), threshold overrides, environment profiles, and notification routes (add, edit and delete).
- Alert History: dismiss alerts.
- Servers: add a server and edit the connection of a server. The web host has no route that removes a server.
- Tags: server tags, query tags and query tag rules (create, edit, delete and assign).
- Known Patterns: add, edit and delete a known pattern.
- Run collectors now: queue one run of the collectors of a server, then read the result on the page.
- Investigations and agent actions: approve or reject investigation notes, undo or switch agent changes, and turn the agent actions of a server on or off.

### The tabs of a server

The sidebar links are listed in [Look at it](#look-at-it). The per-server page has one of two tab sets. The engine that the store recorded for the server picks the set ([#2530](https://github.com/erikdarlingdata/PerformanceMonitor/issues/2530)).

- SQL Server, thirteen sub-tabs: Overview, Wait Stats, CPU, Memory, Blocking, File I/O, I/O Trends, Queries, Configuration, Config Changes, Activity, System Events, Collection Health.
- PostgreSQL, eight sub-tabs: Overview, Activity, Vacuum, Waits, I/O, Replication, Storage, Configuration.
- A server whose engine the store has not recorded gets the SQL Server set.

The PostgreSQL set is smaller by design. It is where every `get_pg_*` read lives. The SQL Server tabs that it does not have (tempdb, Query Store, trace flags, plan cache, the `system_health` ring buffer) have no PostgreSQL analogue.

A NULL `engine_kind` means that no connect stamped the server. It is not a claim of either engine. Only a positive PostgreSQL claim moves a server off the default. A server that has never connected therefore shows SQL Server tabs until it does.

Vacuum is one tab, not three. An old xmin horizon starves vacuum. Starved vacuum falls behind on freezing, and freezing that falls behind ends in wraparound. Read separately, each looks survivable. Together they are one escalating story. Since #2540 that story is four panels. The first panel names the session that holds the horizon. The chain used to start one step after that session.

Storage is a separate tab from Vacuum. Table bloat is what the vacuum lag costs. A bloat panel in the middle of the three-panel causal sequence breaks the sequence. So the bloat panel sits beside index usage. Both panels answer one question: where the space went and whether it is earning its keep.

### What is on the web and what is not

Most of what the viewer shows is also on the web. What is missing falls into the groups below.

#### Reads that exist but no web page shows

These are `get_database_scoped_config` and `get_store_metrics`. The table renderer cannot draw the `databases[].settings[]` shape of `get_database_scoped_config`. `get_store_metrics` is store-wide and has no per-server home.

`get_query_trend` was on that list until [#2520](https://github.com/erikdarlingdata/PerformanceMonitor/issues/2520). It was the only entry in this section that was missing UI and not a stated boundary. The read needs a `query_hash` and a `database_name`, and nothing on the web supplied either. The Top Queries table on the Queries tab now has a picker. The query that you pick gets a chart of its average CPU and average duration over the window, and a grid of its snapshots for each collection. The picker offers exactly the queries that the table shows. It indexes into the rows rendered directly above it and does not read a second, wider list.

#### Every PostgreSQL capture path is on both surfaces

[#2530](https://github.com/erikdarlingdata/PerformanceMonitor/issues/2530) landed the web tabs first and the WPF viewer second. The desktop set has the same names and the same grouping as the web set, so the two front ends do not teach one engine two shapes. The one difference is Configuration, which only the web set has. The desktop panels are grids and not charts. The desktop Overview is the collection report for each collector: which collectors ran, when, with what result, and why one cannot run here. A PostgreSQL operator wants that answer first, and the web splits it across tiles.

The desktop set differs in two more ways:

- The database filter of a server is hidden at a PostgreSQL target. It drives the SQL Server database-scoped reads and nothing else. A filter that no view consults is worse than no filter.
- The Blocking panel prints its sampling denominator above the grid, whether or not the grid has rows. `pg_blocking` is a periodic sample, not an event log. So "two chains" means something different in a window of 60 captures than in a window of 4. An absent capture and a capture that found nothing are the same absence of rows in the stored edge list.

#### Two PostgreSQL capture paths are Amazon Aurora-only

On a stock PostgreSQL target, two panels are permanently empty, and that is not a fault. `get_pg_wait_stats` reads `aurora_stat_system_waits()`. `get_pg_top_queries` reads `aurora_stat_statements()`. Core PostgreSQL has an equivalent of neither, in any version. Both reads answer `not_collected` there. The answer names the server, the engine and the collector, and says that the gap is permanent. Both tabs carry a note that says so before you click.

Waits is the only tab that is wholly Aurora-only. The other half of Activity, the blocking panels, is collected at every PostgreSQL target, including standbys, where a recovery conflict is blocking that happens nowhere else. Waits is shown at stock PostgreSQL on purpose. This way the tab set does not change shape between two PostgreSQL servers in one fleet, and the Aurora capability is discoverable from a stock instance.

#### Three PostgreSQL capture paths are writer-only

This gap needs a different response. On a read replica, `pg_autovacuum_stats`, `pg_index_usage_stats` and `pg_table_bloat_stats` do not run at all. The reason is truth, not availability. A replica keeps its own statistics. On a replica, an index that the workload of the primary scans a million times an hour reads as never scanned. These collectors gate off and do not collect a confidently wrong answer. The Overview grid says so for each collector. Ask the writer.

#### The bloat figure on the Storage tab is an estimate

The tab note, the panel note and the column heading all say so. The figure is arithmetic over the column-width statistics of PostgreSQL. It never reads the table, so hourly collection is affordable. Measured against `pgstattuple`, it was within about 2 percentage points where the statistics were current, and 81 percentage points out where they were not. Rows whose statistics cannot be trusted show a dash and not a number.

If most rows show a dash and the reason column says "no column statistics", the cause is a permissions gap and not a broken collector. `pg_stats` is filtered by SELECT privilege, and `pg_monitor` does not confer it. The monitoring role needs `pg_read_all_data` (PostgreSQL 14 and later) before this panel can say anything. The measured sizes and the dead-tuple percentage beside it are not affected, because they come from the counters of the server.

#### The Sessions panel on the Vacuum tab needs `pg_monitor`, and without it the panel fails quietly

PostgreSQL does not refuse the read. For every backend that the login does not own, it returns the row with all but six columns NULL. This was measured column by column on PostgreSQL 16.15. These columns survive: `pid`, `application_name`, the database, the user, `backend_xid` and `backend_xmin`. These columns come back NULL: `state`, `state_change`, `xact_start`, `query_start`, `backend_start`, `wait_event`, `backend_type`, `client_addr` and `query_id`. The statement text becomes an insufficient-privilege literal.

`backend_xmin` and `backend_xid` stay visible. So the horizon still reads as pinned, and nothing on the screen can say by what. On the same nine backends, the privileged role saw four sessions idle in transaction and the least-privileged role saw zero. The panel marks rows in that state as redacted and gives them no severity. It does not paint them healthy. Unlike the bloat panel, this panel needs no `pg_read_all_data`. Plain `pg_monitor` is enough.

#### A long `idle in transaction` on that panel is not always a problem

The panel tells you which kind you have. On a live instance, two of the four idle-in-transaction shapes pin nothing:

- A READ COMMITTED transaction that only read has already released its snapshot.
- A transaction whose `UPDATE` matched zero rows never got a transaction id.

Those rows show a horizon age of "pins nothing" and not a number. Terminating them reclaims no dead row. Read the horizon column, not the clock.

#### The engine of a target is explicit in the store

`/api/fleet` used to carry the SQL Server `engine_edition` and no discriminator for the target engine. A browser cannot tell a PostgreSQL target from a SQL Server one that way. A PostgreSQL target lands at `engine_edition` 0, and so does a SQL Server that has never connected. The store now records the target engine (`collect.servers.engine_kind`, schema v82). The card carries `engine_kind`, the derived `is_postgres` and `is_aurora` booleans, and `engine_description`. The server composes `engine_description`, the name of the engine in words, so no viewer owns a second copy of the vocabulary.

Both tab registries branch on it: `serverTabsFor(card)` on the web and `ViewerPostgresTabs` in the viewer. The header of a server in the viewer carries the same engine label. A server that the store makes no claim about gets no badge at all, and not a badge that reads "SQL Server". Its tabs are a default and not a finding.

One consequence: a SQL Server read that targets a PostgreSQL server now answers `not_collected` and names the engine ("...runs Aurora PostgreSQL. The system_health_events collector is written for SQL Server..."). It no longer answers `unavailable` with advice to check that collection is running. That applies to the reads that use the capability helper, which is not every read yet.

#### Desktop features that a web imitation would make worse

- There is no graphical plan viewer.
- There is no interactive query heatmap plot. The read ships as a bucketed table on the Queries tab, which gives the same answer without a canvas.
- The Blocking tab gives you the captured blocked-process-report XML as it is, and the deadlock-graph XML beside a drawn graph that you can click.
- The **Block chain** button on a Blocking row draws the lead-blocker tree that the Viewer draws.
- There are no period-compare grids.
- The per-query drill-down charts the same history as the window of the viewer, but it is not that window. It has no stored-plan download and no cached-plan fetch. It has none of the grid features of the desktop. Those are per-column filter popups, CSV export, Copy Repro Script, and right-click drill-down into a window of 30 minutes either side.

Custom Views are the saved dashboards and notebooks in `config.custom_views`. You create and edit them in the built-in composer. The write routes listed above are the reason that the loopback token exemption went away (see [Reach it from a browser on another machine](#reach-it-from-a-browser-on-another-machine)).

The `viewer` role has one more narrow grant: `config.database_state_expected`. It is behind the per-database state-override editor of the WPF viewer. No web route reaches it. The WPF editor gates itself on the same read-only probe (see [Read-only versus read-write seats](viewer.md#read-only-versus-read-write-seats)), which a `viewer`-role seat fails. So no surface currently uses this grant.

## Reach it from a browser on another machine

Add a `network` block to `web` in `darling.json` (managed mode only), or let `--configure-network` write it:

```json
"web": {
  "enabled": true,
  "port": 5153,
  "network": {
    "listen": "192.168.1.205",
    "allowFrom": "192.168.1.0/24",
    "encryptedToken": "<output of --encrypt-password>"
  }
}
```

`--configure-network` generates a 32-character token and prints the plaintext once. You can bring your own token instead. Encrypt it with `--encrypt-password`. A plaintext `"token"` works, and the service logs a warning about it.

You can print the token again. Losing the plaintext does not mean that you must regenerate it and onboard every client again. Run:

```powershell
& "C:\Program Files\PerformanceMonitorDarling\PerformanceMonitor.Darling.Service.exe" --print-web-token
```

Run it elevated, on the box that encrypted it. DPAPI is `LocalMachine`-scoped, so the blob opens only there. The command writes a live token to stdout, so treat that console the way you treat the token.

The service reads the `network` block once at start, and the file is its authority. After you edit the block, run this from an **elevated** PowerShell:

```powershell
Restart-Service 'PerformanceMonitor Darling'
& "C:\Program Files\PerformanceMonitorDarling\PerformanceMonitor.Darling.Service.exe" --configure-firewall
```

### How the sign-in works in a browser

Open `http://192.168.1.205:5153/` and paste the token into the login form. The form POSTs the token to `/auth/login`. The host validates the token in constant time. It then sets a cookie named `darling_web_session` that is HMAC-signed, HttpOnly and SameSite=Strict, with a 12-hour lifetime. It redirects (302) to the page that you were opening.

A token in the URL (`?token=`) is not accepted, so the token never lands in history or a `Referer`. Every later request carries the cookie. The signing key is per process, so a service restart logs everyone out. Present the token again. The host throttles repeated misses from one address (429 with `Retry-After`). A token shorter than 16 characters keeps the dashboard loopback-only at start.

Two gates apply, and the order matters:

1. CIDR, checked in the app on the remote address. An address outside `allowFrom` gets a 403 with an empty body, with or without a cookie. A remote address that the host cannot determine also fails closed to 403. Loopback skips this check. Without that exception, the browser of the operator gets a 403 from `127.0.0.1`.
2. Credential. A valid cookie passes. A valid token that the login form POSTs is exchanged for a cookie. Any other request gets the login form back as HTTP 200, not a 401.

> Loopback does not skip step 2. Once a `web.network` block exposes the dashboard, every request authenticates, including one from the box itself. It used to pass without a token, because the surface was read-only. That stopped being true when the Custom Views composer gained create, update and delete, and `/api/compose/run`. Then a local process can read the whole store and change views with no credential. That process is, for example, a scheduled task, code that an SSRF attack drives, or the session of another user. In exposed mode the token is the loopback guard.

**Proof:** the dashboard renders in the remote browser, and the address bar shows no `?token=`.

The exposure is fail-closed. Any of these sends the endpoint back to loopback: an unparseable `listen`, an invalid `allowFrom`, a missing token, or bring-your-own PostgreSQL. The service logs a critical line that says which one. The endpoint never half-exposes. With no token at all, the loopback dashboard does not start either (see [Turn it on](#turn-it-on)).

TLS is opt-in. Without it, the token and the cookie travel in clear on the segment. The CIDR bounds who can route to the port. It does not protect the wire. An exposed dashboard with no certificate says so at every start. Point `web.network.tls` at a PKCS#12 bundle or a PEM pair (`Darling/README.md` has the shape), or put a TLS-terminating reverse proxy in front of the port. The certificate applies to the LAN listener only, and loopback stays plain HTTP. A missing or expired certificate keeps the dashboard loopback-only and does not fall back to cleartext.

Never expose this dashboard to the internet.
