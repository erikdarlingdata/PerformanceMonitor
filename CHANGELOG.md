# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- changelog-layout:begin -->
This file is an **index**. From 3.0.0 on, each released entry is compacted to its title and
the issues it references, and the full prose lives in one file per minor version under
[`docs/changelog/`](docs/changelog/). GitHub will not render a blob of the size the whole
history in one file had reached, so the prose moved rather than got shorter.

`[Unreleased]` keeps its full prose here, because this is where it is written. At the release
cut it is archived and compacted like every other version:

    python3 tools/changelog/changelog_archive.py split

Releases before 3.0.0 are not archived: those entries carry no prose to move.
<!-- changelog-layout:end -->

## [Unreleased]

### Important

- **Self-managed stores: re-run `provision-roles.sql` after upgrading** ([#7107]) - Run `Darling/tools/provision-roles.sql` again after the upgrade. Start the service on the new version first, so it applies store migration V237. The investigation journal trigger now refuses eight new people events (`person_added`, `person_renamed`, `person_enabled`, `person_disabled`, `person_identity_added`, `person_identity_removed`, `person_grant_added` and `person_grant_removed`) from any login other than the store owner. The script also revokes every write on the new people tables and their identity sequences from the monitoring roles. It grants them read access to the tables. Until the script is re-run, the trigger keeps the older list. The `admin` and `mcp` roles can then write one of those events to the journal. Such a row changes no table, and nothing reads people events yet. The upgrade itself already makes the people tables owner-only. The script skips that step with a warning on a store below version 237. The compose store and managed provisioning do all of this on their own.
- **Self-managed stores: re-run `provision-roles.sql` after upgrading for action approval** ([#7109]) - Store migration V238 adds the route settings `action_approval` and `approval_single_person` and the `config.agent_action_proposals` table. The upgrade sets approval to Off on every route that is already at Level 3, so those routes act as before. A person turns approval on for each route. A self-managed store is one where the service does not manage the roles. On such a store, start the service on the new version first, so it applies V238. Then run `Darling/tools/provision-roles.sql` again, because the script changed. The journal trigger now refuses twelve new events (eleven `proposal_` events and `person_message`) from any login other than the store owner. The script also makes the proposals table owner-only and grants the `admin`, `viewer` and `web` roles read access to it. It grants the `viewer`, `web` and `mcp` roles read access to the two new route settings. It replaces `config.set_notification_route_agent` with a version that takes the two approval settings. Until the script is re-run, MCP changes to a route's agent settings are refused with a message to re-run it. The trigger keeps the older list, so the `admin` and `mcp` roles can write one of the new events to the journal. The script skips the proposals step with a warning on a store below version 238. The compose store and managed provisioning do all of this on their own. To roll back, read the V238 line in the rollback list in `docs/darling/admin.md` ("Rolling back to an older build"). Before you roll back, lower every Level 3 route whose approval is required to Level 2 or below.
- **The Darling container runs as user 1654, not root** ([#8141], [#8290]) - The image now runs as the .NET base image's non-root user (UID 1654). A new compose install needs nothing extra. An install that ran as root needs two commands before the new image starts, from the compose folder. First run `chmod 700 secrets && chmod 644 darling.json && find secrets -type f ! -name password_key -exec chmod 644 {} +`, which leaves a password key kept in `secrets/` alone. Then run `docker compose run --rm --no-deps --user 0 --entrypoint chown darling -R 1654:1654 /var/lib/darling`, and start with `docker compose up -d`. If the new image started first, collection stops and the log gives what to run. Without the `chmod`, it names the secret it cannot read and gives both lines. Without the `chown`, it names the credentials directory and gives the `chown`. The container shows as unhealthy and the web dashboard and MCP stay down. Run the two commands, then `docker compose restart darling` (`up -d` leaves a running container as it is). To keep running as root until you can run the step, set `user: "0:0"` on the darling service. That works only before the step. To go back to an earlier image after the step, run the same `chown` with `-R 0:0`. On Kubernetes, run as user 1654 with `fsGroup: 1654` and `fsGroupChangePolicy: OnRootMismatch` and mount one volume at `/var/lib/darling`. A new volume needs nothing more on a volume type that kubelet applies `fsGroup` to. On one that ignores it, a root-owned volume root needs the Job too. A volume a root pod wrote needs a one-off Job (or initContainer) as root first. With a ReadWriteOnce volume, scale the Deployment to 0 before the Job and back after it. The Job runs `chown -R 1654:1654 /var/lib/darling && chmod 2770 /var/lib/darling && chmod 700 /var/lib/darling/credentials && chmod 600 /var/lib/darling/credentials/*`, so kubelet skips its recursive walk and the keys are kept. `docs/darling/security.md` covers a volume mounted at `credentials/`. `setup.sh` now writes `darling.json` and the files in `secrets/` as 0644. The 0700 `secrets/` folder keeps the secrets from other host users, so keep secrets there, never inline in `darling.json`.

### Added

- **Plan analysis lists the statistics the optimizer loaded** ([#8147]) - The four plan analysis MCP tools (`analyze_query_plan`, `analyze_procedure_plan`, `analyze_query_store_plan`, `analyze_plan_xml`), in Darling and Lite, now give each statement a `statistics_loaded` list: the statistics the optimizer loaded while compiling the statement (`OptimizerStatsUsage`, though the plan does not say which of them drove the estimates), most-modified first and capped at `statistics_cap`, with `modification_count`, `table_rows`, `modified_percent`, `stale_at_compile`, `last_update` and `sampling_percent` as of compile. `statistics_status` says whether the plan recorded them (`recorded`), has no record of them (`not_in_plan`), or had no query plan (`no_query_plan`). `stale_at_compile` is true when the modification count at compile passed the SQL Server 2016+ automatic update threshold at compatibility level 130 or higher, using the plan's table cardinality: 500 modifications for 500 rows or fewer, otherwise the smaller of 500 plus 20% of the rows and the square root of 1000 times the rows (null when the rows are unknown). It is a lead about the statistic's state at compile, not a cause of the estimate or plan shape. It is also approximate, because SQL Server counts the rows at the statistic's last update. Temp tables use other thresholds, and compatibility levels below 130 use 500 plus 20% of the rows. The Viewer's Statistics Used list now shows temp tables by their short name (`#orders`), as plan operators already do.
- **Named MCP tokens can be read-only, and can ask for less data detail** ([#7023]) - A new `reader` role in `mcp.tokens` serves the read tools only, whatever `mcp.allowWrites`, `mcp.allowServerAdmin` and `mcp.allowInvestigations` say, and a reader is not an agent. Any token entry can also set `dataDetail` (`full`, `maskLiterals` or `metadataOnly`) to narrow `mcp.dataDetail` for that token; it can never widen it.
- **GitHub Copilot in VS Code is documented as an MCP client** ([#7042]) - The MCP page has a setup section for Copilot, for Darling and Lite. It shows the `mcp.json` entry, with the token as a prompted input. For Darling it uses the `/core` URL, which serves 75 tools. VS Code allows at most 128 tools per chat request, and the root URL serves 190. It also suggests a `reader` token. Lite has no `/core` URL, and its 113 tools are under the limit.
- **Darling mute rules can repeat on a schedule** ([#7040]) - A mute rule can now apply only on chosen days, between a daily start and end time. The times are in an IANA time zone. A time zone that the store's PostgreSQL does not know is refused when the rule is saved. A window can cross midnight, and it belongs to the day it starts on. The schedule works inside the rule's own start and expiry dates. Set it with `create_mute_rule` and `update_mute_rule`, on the Mute Rules web page, or in the Viewer's mute editor. `get_mute_rules` shows each rule's schedule, whether it is in a window now, and when the next window opens. Agent mutes cannot repeat. Store schema version 231. Lite is unchanged.
- **Darling can send a Daily Health Report** ([#7041]) - The report is off by default. When it is on, Darling sends one report a day at the time you set. It uses the time zone you choose and the normal alert pipeline. Notification routes choose who gets it, by the `reports` family or the "Daily Health Report" metric, and mutes apply. The report covers the last 24 hours, worst servers first. It shows each server's health band, the alerts fired, failed Agent jobs, databases without a recent backup or CHECKDB, low disk space and collection problems. Email gets an HTML version. Slack, Teams and webhooks get a short text version. If the service is down at the send time, it sends when it comes back that day. It never sends twice in a day and never backfills. `get_daily_health_report` builds the report for any ended day without sending it. `send_daily_health_report_now` sends one at once when MCP writes are on. The settings are in `get_alert_settings`, `update_alert_settings`, the web Admin page (with a preview) and the Viewer settings window. Store schema version 232. Lite is unchanged.
- **Notification routes can post a Slack or Teams notice when an agent files a report** ([#6979]) - Each route has a new `report_notice` setting: `off`, `needs_person_or_change` or `all`. It defaults to `off`, so nothing changes until you set it. `needs_person_or_change` posts for a report with a `needs_person` verdict or a request that made at least one change, and `all` posts for every report. The Viewer's route dialog and grid, the web route editor and the `create_notification_route`, `update_notification_route` and `get_notification_routes` MCP tools carry the setting. A notice goes out once for each request, within 30 minutes of the report. It goes to the route's Slack or Teams destination, or to the Settings channel when the route has none of its own. It names the server, metric and verdict, the first line of the root cause and up to 10 agent changes. It links to the request and to the Agent changes tab when `web.publicBaseUrl` is set. `mcp.dataDetail` applies: `maskLiterals` masks literals in the root cause and the change objects, and `metadataOnly` leaves out the root cause. Lite sends nothing. The Investigations page can open one request from its link. It now reads that request's agent changes by request id on the service, so an older request no longer shows an incomplete list.
- **Mute rules can send alerts to the daily digest** ([#7083]) - A mute rule now has an action, `mute` or `digest`. A `digest` rule never mutes. While the rule is in force and its scope matches a fire, the fire is recorded as a Digest row that names the rule and its window. Nothing is sent. Outside the window the alert pages as before. When a mute rule and a digest rule both match, the row is muted. Set `action` with `create_mute_rule` and `update_mute_rule`. The Mute Rules web page and the Viewer's mute editor have an Action choice, Mute or Send to the daily digest. `get_mute_rules` returns it. An agent cannot set it. Store schema version 236 adds the column, and every existing rule stays a `mute` rule. A Viewer on a store below version 236 refuses to save a digest rule and says to upgrade the store. Darling only: Lite's mute rules have no schedule and never hold an alert.
- **People, sign-ins and grants** ([#7107]) - Administrators can name people, link their web and Slack sign-ins, and give them grants. Only the store owner writes these tables, from the upgrade on. A sign-in key is shown only to its own session and to administrators, and removing a server or tag removes its grants.
- **Alert details say why an alert was held or paged, in the Viewer and Lite** ([#7124]) - The Alert Details window has a "Routing reason" row, and "Copy all details" includes it. The Alerts grid shows the same text as a hover on the Status text. Rows with no routing record show nothing.
- **A route's Level 3 server actions can wait for a person's approval** ([#7109]) - Each alert route has an Action approval setting and a single-person option. Action approval is Required by default and Off on routes that were already at Level 3. The single-person option lets the person who asked approve the proposal. On a route that needs approval, an agent's force, unforce, evict, kill, recompile and plan guide calls are stored as proposals. A person with the approve grant opens the proposal in the web dashboard (Proposals) or the Viewer (Agent changes). The person reads the fresh facts and presses twice. A changed plan or session ends the proposal as superseded. `kill_session` now refuses a session that ran something new since the dry run (`session_changed`). A new proposal sends one notice to the route's destinations. MCP can turn approval on but never off.
- **Darling can read its password key from a secret** ([#8006]) - A new `passwordKey` setting in `darling.json` takes a `file:` or `env:` reference to the password key, so a container can keep it in a Kubernetes Secret or any secret store instead of the credentials volume. With it set, the service loads the key at start, checks it against the key the store publishes, and never generates, writes or retires a key file. After `--reset-password-key` it waits for a new key in that source. `--new-password-key <path>` writes a new key and prints only its fingerprint. Without the setting nothing changes. The password key's Linux self-check runs again on pull requests that touch the password key code, and nightly (`darling-linux-store.yml`). It had stopped running when `build.yml` became release-only.
- **Store Autovacuum Behind self-alert** ([#7368]) - The service now warns when autovacuum falls behind on its own PostgreSQL store. Once an hour it checks for tables with over 1,000,000 live rows whose changes since the last ANALYZE are over 3 times their threshold, and it alerts when that holds on two reads in a row. The text names the worst table, the workers in use and the longest worker, and says to raise `autovacuum_vacuum_cost_limit` and `autovacuum_max_workers`, or to turn autovacuum back on when it is off. Tables with `autovacuum_enabled` off are left out, and a standing condition alerts again only when it gets worse.
- **`darling-export` writes the Darling store to Parquet files** ([#7581]) - A new console tool reads the PostgreSQL store of a Darling monitoring service and writes Parquet files and one JSON manifest per run. It uploads them to a local or network folder, to Amazon S3 (with a custom endpoint for S3-compatible storage) or to Azure Blob Storage. It only reads the store and never writes to it. `darling-export run` is the scheduler. Start it once an hour and it works out which windows are due, exports them, uploads the Parquet files first and the manifest last, and cleans up old logs and output. The release now builds a signed `PerformanceMonitorDarlingExport-<version>-win-x64.zip` with the README, the file contract and a sample config.
- **One read shows a statement on every server** ([#7609]) - Locating where a statement runs and what it costs took one call per server. The new MCP read `get_query_fleet_summary` takes a query hash and returns one row per server and database with executions, CPU, elapsed time and the first and last hour seen, ranked by CPU. It reads up to 90 days of the plan cache and up to 7 days of Query Store (each row says which), and says when the row cap or the 7-day Query Store cut applied. A new store migration (V246) adds an index on the Query Store hourly rollup for this read.
- **Find the queries that contain a piece of SQL text** ([#7610]) - An agent that knew part of a statement, such as a table name, had no way to find it. The new find_queries_by_text tool searches one server's stored text in Query Store and in the raw plan-cache tier, returns the query_id or query_hash for the other query reads, and says how far back each source reaches. Each source stops after 4 seconds and says so, and naming a database makes the search much faster. The sensitive-statement filter judges each statement's whole text before the excerpt is cut, and a statement it withholds is listed with the withheld marker instead of an excerpt. It is refused when the data detail level hides query text.
- **An opt-in trigram index lets `find_queries_by_text` search every server's plan-cache text at once** ([#7674]) - Searching a busy server's whole plan-cache text can run past its time budget. A new setting, `queryTextTrigramIndexEnabled` (off by default), makes the service build a `pg_trgm` index on that text in the background with `CREATE INDEX CONCURRENTLY`, never during an upgrade and without pausing collection. Turning the setting off and restarting drops the index. A failed or cut-short build is dropped and tried again after a wait that doubles up to 12 hours, and if the store login cannot create the extension the service stops asking and shows the statement to run. `get_store_metrics` has a new `query_text_index` block with the state, size and last error, and it appears even before the first self-metrics snapshot. With the index ready, `find_queries_by_text` accepts a call with no `server_name` and searches the plan-cache text of all servers. In any other state it refuses that call with a sentence that names the setting and the state. Query Store text is still searched one server at a time, and searching one server never uses the index. The index serves the text match only, so on a large store an all-servers search can still reach its 4 second budget and report stopped.
- **Darling can serve web and MCP TLS from a certificate in the Windows certificate store, and a new command grants the service its key** ([#7718]) - A certificate enrolled into the Windows store with a key that cannot be exported could not be used, because both listeners took only a PFX or PEM file. The tls block now accepts a store entry, chosen by thumbprint or by DNS name to follow renewals. It is read when the service starts, so restart the service after a renewal. A missing, ambiguous, expired or not-yet-valid certificate, or a key the service account cannot open, keeps the listener on loopback and logs one Critical line. When the key cannot be opened, the line names the service account and the fix. The new command PerformanceMonitor.Darling.Service.exe --grant-cert-key-access gives that account Read, and only Read, on the key. It adds one entry, keeps the existing ones, and changes nothing without an elevated prompt for the LocalMachine store or when the account already has Read. It refuses a group, a mistyped NT SERVICE name, and a store name that is a path. The status line and the login hint now say TLS is refused for a store name that is a path.
- **Web dashboard: named tokens with a read-only role, like mcp.tokens** ([#7735]) - `web.network.tokens` takes named entries with a `reader` or `operator` role, shaped like `mcp.tokens`. A reader token signs in to the same read-only seat an OIDC viewer gets and is refused every change by the dashboard's one write gate, and the shared token keeps its edit seat.
- **Slack bot threads for investigation requests** ([#7656]) - A notification route takes a Slack bot channel id, the bot and app-level tokens are stored sealed in Notifications, and the route form explains the setup. On such a route Darling posts each investigation request as one thread, parent first, and replies in it as the journal grows with the claim, the report, each change and its result, each proposal and its decision, and how the request ended, with a follow-up joining its parent's thread, link previews off and every collected value escaped. A request that opened before the route got its channel gets no thread, and the setup text says so. The proposal card says when the proposal expires. A rate limit from Slack holds the posts for Retry-After and sends them afterwards.
- **Lite get_alert_history takes server_name and reaches 90 days** ([#7902]) - The MCP tool can scope the page to one server ahead of the row cap, and hours_back reaches the alert log's three-month retention, matching Darling.
- **Slack Socket Mode listener for the bot** ([#7657]) - With the app-level token saved in Notifications, Darling holds two Socket Mode connections to Slack, acknowledges every event the moment it arrives, drops the alert webhook's and the bot's own posts, anything from another workspace or app, and anything outside the bot channels of its own routes, reconnects at once when Slack asks, and reports its connections, drops and queue in the diagnostics bundle. The Slack app manifest and a setup page for the shared app, the two tokens and the per-store bot channel ship in the docs. No button or mention is acted on yet.
- **Slack buttons on the bot's thread posts** ([#7658]) - A proposal card carries Approve and Refuse, a done agent change with an undo path carries Undo, the report carries the three verdict buttons, and a pending note written from a request carries Approve and Reject and posts in the request's thread after the request's own card, in the pass that opens the thread or the next one. Approve and Undo take two presses by the same mapped person, the first answered to that person alone with the facts or the statement. A press on a card the bot posted runs as the mapped person with their grant, is journaled with the Slack identity, and edits the card once decided, and a press on a card already decided is refused with who decided it and when. A store with no app-level token posts its cards without buttons and points at the web page. Schema version 253 adds the card table.
- **Slack mentions ask the agent** ([#7659]) - Mentioning the bot in a request's thread opens a chat request for the agent, joins an unclaimed one, or waits for the request in play, under a cap of six per thread per hour and the route's hourly limit. A person needs the ask grant. Anyone else, a mention outside a request thread, or one on a thread older than seven days gets one sentence back. A message still waiting when the request ends goes to the agent only while its person is enabled and granted. The agent reads what granted people asked as person_requests on its claim answer and answers the thread with post_investigation_message. Schema version 254 adds the chat link, the askers and the message audit table.
- **Notification routes: `Analysis: *` matches every analysis finding** ([#7999]) - A route can now name `Analysis: *` to catch every analysis finding of any category, at any scope. Within one scope, it ranks between an `Analysis: <category>` route and a family route. A server or tag route still comes before a fleet-wide one. The route editors in Lite and the Darling Viewer offer it as "Every analysis finding (Analysis: *)", `get_notification_routes` reports it as `match_kind` `analysis_all`, and alert history records the route source as `AnalysisAll`. A route saved as `Analysis: *` before this change matched nothing and now matches every analysis finding, so check its destinations if you have one. An older build reads a saved `Analysis: *` as a category named `*` and matches nothing, so findings go back to the next matching route (a family route or the default channels).
- **Lost Extended Events ring buffer events are reported, and the ring buffer size is a setting** ([#8146]) - The deadlock, blocked-process and long-query collectors now notice when their Extended Events ring buffer loses events between two reads. They say how many were lost and what to change. Nothing about what is stored changes. Collection health goes to WARNING on the run that found evicted or dropped events, with the sentence as its note. After a session restart the counts are unknown and the note shows with no band change. `get_deadlocks`, `get_deadlock_trend`, `get_blocking`, `get_blocked_process_reports`, `get_blocking_trend`, `get_blocking_stats` and `get_long_query_completions`, in both apps, carry `ring_buffer_loss` with `evicted_events`, `dropped_events`, `unknown_spans`, `first_loss_utc`, `last_loss_utc` and `note`. The counts are null when no run in the window could be counted, never 0. Empty answers with a loss no longer say all-clear. On a database-filtered answer the note says the filter does not narrow the loss, because the session is server-wide. The new setting is `xeRingBufferMaxEvents` in `darling.json` (applies after a restart) and `xe_ring_buffer_max_events` in Lite's `settings.json` (read at start). The default is 1000, and any whole number is clamped to 1000 through 10000 with a log line. The shared deadlock and blocked-process sessions are never altered, because they are shared by name across installs. When such a session's limit is below the setting, the loss note says so and gives the steps to apply it. The install's own long-query session is recreated after a successful read when it is running and its limit is below the setting. `max_memory` stays at 4096 KB, so large deadlock XML fills the buffer by memory before 1000 events.

### Changed

- **The Darling install guide says where PowerShell is used** ([#7272]) - A new section lists what uses it and what does not. It also gives install, upgrade and uninstall steps that use only `sc.exe`, `icacls`, `net localgroup` and `netsh`.
- **Write-latency anomalies and the write latency fact now read log files and tempdb data files only** ([#6959]) - user data-file writes are checkpoint and lazy-writer work no session waits on, so databases with several data files no longer trip the gate. The write latency card now says its figure covers log files and tempdb, and a separate sentence gives the user data files' own write latency.
- **The alert pass no longer reads old chunks to find a server's newest CPU and tempdb sample** ([#6968]) - each read now starts at the newest time the last read returned. Before, each 30-second pass read every chunk between a server's newest sample and now. For a server with an old sample, or no sample, that was many chunks. The change watch's database-scoped configuration check now reads only the databases captured since its last check.
- **Six more alert-pass reads start from the newest row they last found** ([#7017]) - they use the same start point as the CPU and tempdb reads. These are the PVS pressure, SQL Agent status, volume free space and database state reads, and the two Availability Group state reads. The four database state upkeep statements use the same start point. On a production store, these reads used about 300 seconds of store time a day. The results do not change.
- **The hourly store size snapshot estimates hypertables, aggregates and the catch-all rows from the catalog** ([#6971]) - it no longer gets a file size for each chunk. The named tables and the dimension rows are still measured exactly each hour, and the daily snapshot is still exact.
- **The collection health refresh no longer reads every materialized hour of the aggregate to find missing hours** ([#6969])
- **The Overview timeline's I/O Latency row loads one row for each time bucket** ([#6970]) - it loaded one row for each file before. On a server with 16 files, that was about 15 times as many rows. Darling and Lite draw the same line.
- **Two hourly store upkeep reads no longer read the full index and job catalogs** ([#7016]) - the index check read every index in the database. It needs the indexes of one table only. The aggregate compression check read the job catalog once for each aggregate. On a test store with 8,321 chunks, the index check went from about 1.4 million buffer reads to about 100.

- **The FinOps Server Inventory no longer rereads the whole archive for memory, size and properties** ([#7014]) - the Lite Server Inventory read looked up each server's newest memory row, database-size snapshot and properties row through views that include the Parquet archive, so its cost grew with archived days (807,552 archive rows with 8 archived days on a 40-database store). It now reads the live tables for a server that still has rows there. It reads the archive only for a server whose rows are all archived, so a stopped server still shows its last values. The idle-database coverage check behind the Idle DBs count and the Idle Databases grid no longer scans the whole `query_stats` archive either. It counts days inside its 7-day window. It then asks the live table whether the oldest sample is 7 days old, and runs one early-stopping archive probe per server it cannot decide. The answers are the same as before.
- **The blocking health band counts waits, not report rows** ([#7028]) - Darling banded blocking on blocked-process report rows. This covered the fleet card, Overview, fleet sweep and Daily Health Calendar. SQL Server writes about eleven reports for one one-minute wait, so the band did not match the waits the blocking alerts count. The band now counts waits per hour. Each hour uses its blocked-process reports when it has any, and its DMV snapshots when it has none. The card shows the count it bands, as `waits/hr`. The 5 and 20 per hour tiers keep their stored values. A window where some hours have only DMV snapshots can now count more, because those hours are no longer dropped. One wait is still one count, however many reports SQL Server wrote for it. Two new tiers are off by default: `blocking_warn_snapshot_wait_seconds` and `blocking_critical_snapshot_wait_seconds`. They band on the largest total blocked wait in one DMV snapshot, which is the Blocking Wait Time alert's own measure. They apply the alert's consecutive-snapshot gate first. The gate runs on each server's own snapshot cadence, and a streak can cross midnight or the start of the window. The tiers are in `get_alert_settings`, `update_alert_settings`, the Viewer settings window and the web admin page. Store schema version 230. Lite is unchanged and keeps banding on report rows.
- **Error and refusal messages name the object and the next step** ([#7077]) - Many Darling and Lite messages were reworded across the MCP tools, the web dashboard, the Viewer, Lite and the service start-up log. A server that does not resolve now reads "No monitored server matches 'X'." or "server_name is required because N servers are monitored.", still after the opening "Could not resolve server.". The Viewer's read-only message no longer sends everyone to `postgres.connectAs`: it covers the bundled store, a network login and your own PostgreSQL. The web dashboard says "Your session has expired. Sign in again." It names the Darling service when it cannot be reached. It names the HTTP status and path when an answer has no message. An oversized request names the limit in KB. A network token that cannot be decrypted names the setting and the `--encrypt-password` fix. Lite's fatal and error dialogs show the message and the log file path, without the exception type or stack frames. Plan, export and test-notification failures name the server or file and what to try. Messages no longer carry issue numbers or schema rung names. Release packages now include `provision-roles.sql`. An operator of a self-managed store can run it from the install folder beside the service exe (zip, MSI, Linux tarball and container image). The messages that tell them to re-run it name that location.
- **SQL Server High CPU pages after 5 breaching samples, clears after 15 minutes under the bar, and no longer sends reminders** ([#7085]) - A High CPU incident used to open after 3 samples at or over the threshold and close after 2 under it. A flapping server closed and reopened the same incident within minutes and paged each time, and a standing incident paged again each time the alert cooldown ran out. An incident now opens after 5 samples in a row at or over the threshold, which is about 5 minutes at the one-minute CPU sample cadence. It clears only after CPU stays under the threshold for 15 minutes of sample time. CPU Resolved goes out once, when it clears. Inside an open incident only a rise from Warning to Critical pages, once. A drop back to Warning and a later return to Critical add nothing. The repeat page after the cooldown is gone, so the cooldown now only spaces retries after a failed send. Spikes of 1 to 4 samples and the held repeats are not sent, and they stay in the CPU history. The threshold (80% by default), the Warning and Critical levels and the cooldown setting keep their values. This covers Darling and Lite, and the Darling host's own CPU self-alert. The PostgreSQL target CPU gate keeps its own 3-sample open and 2-sample clear. Two fixes came with it. A High CPU page whose every channel failed, including a Warning to Critical escalation, is now retried. A restart no longer pages again for an open incident that was already paged, and a Warning paged long before a restart no longer swallows the Critical that follows. A restart in the middle of a dip starts the 15 minutes over. The old numbers were not settings, so every install gets the new rule on upgrade.
- **Small alert types page less: Runnable Queue, deadlocks, forced plans, PostgreSQL long-running queries and two Darling store alerts** ([#7086]) - Alerts that paged on every small event now hold the minor ones back. A held alert is written to the alert history as a digest row and sends nothing, and a real page for the same alert right after it still goes out. Held alerts reach a person once a day in the Daily Digest (below). Runnable Queue pages at Critical only (16 runnable tasks per scheduler), and a Warning is held, in Darling and Lite. A deadlock whose victim is Darling's or Lite's own monitoring session is left out of the count, the rollup and the card. It is still stored and shown in the grids. A deadlock where a user session was the victim still counts. A deadlock episode pages once. More deadlocks inside it add nothing until the 1-hour count falls below the threshold and the Deadlocks Cleared row is written. A rise to the Critical rate of 20 an hour inside the episode still pages. Forced Plan Failing is judged for each plan. The first failure is held, and the plan pages when it fails again within 24 hours of the first failure. After 24 hours the plan starts over. PostgreSQL Long-Running Query, in Darling, no longer pages from a `pg_session_states` capture older than the larger of 2 collection cycles and 10 minutes, and its detail now names the longest query's pid, minutes and statement. The statement follows `mcp.dataDetail`: it is shown in full, with literals masked, or left out under `metadataOnly`. Store Checkpointer Pressure and Collection Falling Behind, in Darling, are held until the condition has stood. Collection Falling Behind pages after 30 minutes, and Store Checkpointer Pressure after 3 hourly checks in a row. Their repeat pages are unchanged. The Daily Digest keeps its metric name, `Analysis Singles Digest`, so existing routes and mute rules still match. Its title on email, Slack and Teams is now "Daily Digest". It lists the held alerts for the day, grouped by server and metric. Each line gives the count, the first and last time and the newest reason, for the 40 largest groups, then says how many more were left out. A day with only held alerts now sends the digest, and a day with none sends nothing. The Daily Health Report shows `(N held)` after a metric's count when some of its alerts were held. In Lite, the Daily Summary actionable alerts count now leaves out digest rows, which also drops the existing analysis digest rows from it. No setting changed, and an install that set a Runnable Queue Warning level keeps it, but that Warning is now held. The forced plan and self-health memory lives in process only. After a restart the first failure of a plan or an episode can be held once more.
- **PostgreSQL Poison Wait Warnings and High CPU pages no longer fire on routine conditions** ([#7082]) - A Poison Wait Warning now pages only when the 10 minute window ending now and the 10 minutes before it each average 1.0 or more waiting backends on the event. A Warning-level episode shorter than about 20 minutes is held for the digest. A Critical (10.0 or more) still pages at once. The page now names the top three wait events and up to three statements seen waiting on the alerting event, or says that none were recorded in the window. Statement text follows `mcp.dataDetail`. High CPU counts a sample as a breach only when capacity use (percent of the configured ACU ceiling) and instance CPU are both at or over `cpuThresholdPercent`. Capacity at the ceiling with low instance CPU no longer pages. Such a sample writes one held High CPU row at most once per cooldown. Provisioned RDS and Aurora PostgreSQL instances publish no capacity figure, so High CPU never paged on them. A target with no capacity figure in the last 7 days is now judged on instance CPU alone. A missing figure on any other target is still left out. A repeat deadlock between a statement pair already seen in the last 30 days is held for the digest unless 5 or more reports land in an hour. A pair not seen in 30 days pages. The page and the digest row name up to three statement pairs with their report counts. A page about a first-ever pair opens its investigation request. Held rows show as Digest in the Alerts grids, the web alerts page and `get_alert_history`. No setting or column changed, and installs that set `poisonWaitEnabled`, `deadlockEnabled`, `pgDeadlockCountThreshold` or `cpuThresholdPercent` keep their values. SQL Server and Lite are unchanged.
- **Analysis findings for write latency, plan regressions and PostgreSQL wait profiles page only when they cost something** ([#7084]) - write latency pages at a 20 ms average or more, a plan-regression chain at 50% average CPU or more or when query timeouts rose, and a PostgreSQL wait profile at 1.0 or more waiting backends and at most once per server per story per 24 hours. A finding under its floor is still saved and shown but sends nothing. A story labelled as recurring (the same weekday and hour for 3 weeks in a row) now goes to the daily digest whatever else is true. So does a story with one fact and a matched check, when its severity is under the notify bar plus 0.25. That is 1.75 at the default bar of 1.5. Stories with two or more facts, absolution stories and same-statement pileups page as before. A held finding keeps its severity, is saved and shown, and is listed in the Analysis Singles Digest. Setting `analysis.uncorroboratedRoute` to `page` restores paging for both. A finding raised while High CPU, Blocking Detected, Blocking Wait Time or Deadlocks Detected is open on the same server now attaches to that alert instead of paging. Open means paged in the last 60 minutes and not resolved since. It is recorded as a Digest row that says `attached to <metric> fired HH:mm` and is listed under Attached findings in the engine alert's detail. If that lookup fails, the finding pages. This covers Darling and Lite, and the PostgreSQL wait profile rule is Darling only. No setting changed, and `analysis.notifySeverity` keeps its value and moves the page line with it.
- **Blocking alerts show the statement that ran, and PostgreSQL blocking alerts name both statements and the wait** ([#7083]) - The always-on blocking snapshot read the whole batch or module text. A request inside a procedure therefore stored the procedure's `CREATE PROCEDURE` definition as its query. It now cuts the text to the statement the request is on. A blocker with no running request, such as a sleeping session holding an open transaction, shows its last batch and starts with `[no running statement, last batch or module text]`. This reaches Darling and Lite and every surface that reads the stored text. Rows stored before the upgrade keep their old text until they age out. In Darling, a PostgreSQL blocking alert now carries detail text. For each root blocker (the first 10, then a `+N more` line) it gives the worst victim's wait, the database, the blocked statement and the blocking statement. Statements follow `mcp.dataDetail`.
- **Blocking pages need 3 events with a 15 second wait, or one event waiting 60 seconds** ([#7083]) - The count arm now pages when the events in the rolling hour reach `blockingCountThreshold` (3, was 1) and the worst wait among them is 15 seconds or more. One event now pages on its own when its wait reaches `blockingWaitSecondsThreshold` (60 seconds, was 0 which is off), whatever the count. A wait bar of 0 turns this route and the snapshot Blocking Wait Time alert off. Repeated blocked-process reports of one wait count as one event. Three or more events that all waited under 15 seconds, and one or two events under the wait bar, no longer page. PostgreSQL follows the same rule. An event is a root blocker, its wait is that root's worst victim wait, and `pgBlockingCountThreshold` is now 3. The Blocking Cleared text now says when the alert clears: when the last 60 minutes hold fewer than N blocked events and none of them waited the bar. It can arrive up to 60 minutes after the blocking ended. Severity, cooldown, edge trigger and the time an open incident clears are unchanged. Upgrade: Darling store migration V235 moves `blocking_count_threshold` from 1 to 3, `blocking_wait_seconds_threshold` from 0 to 60 and `pg_blocking_count_threshold` from 1 to 3, each only where it still holds the old shipped default. A store whose operator chose 1 and 0 on purpose is moved too, because the store cannot tell that apart from the default. Set it back once in the Settings window, the web settings page or `update_alert_settings`. Any other value is kept. An install that never set these sees 3, 60 and the 15 second floor. In Lite, a settings file that holds 1 or 0 for these loads as 3 or 60 once, and any other value is kept. Lite now writes a marker, `alert_blocking_defaults_applied`, with each save, and a 1 or 0 saved by this build is kept.
- **Blocking that no longer pages is kept for the daily digest** ([#7083]) - When the events in the hour reach the count bar but none waited 15 seconds, nothing pages, as before. One held Blocking Detected row per server is now recorded for the burst, such as `14 blocking events, worst wait 9 s, longest chain 2`. Another row is written only after the alert cooldown, and only when a newer wait arrives. A real page right after a held row still goes out. It shows as Digest in the Alerts tab in Darling and Lite, and sends nothing. No row is written when a Blocking Detected incident is open, when the server is silenced or muted, when the count bar is 0, or when the read fails. On PostgreSQL the line reads `N root blockers, worst wait X s, longest chain Y`. No setting or default changes with this part. An install with Blocking on and a count at the bar with waits under 15 seconds now gets held rows where it got nothing before. Nobody gets a new page.
- **Darling docs now cover Azure SQL with Entra** ([#7253]) - An Entra identity needs a `CREATE USER ... FROM EXTERNAL PROVIDER` user, and the interactive Entra ID (MFA) mode is Lite only. The service still needs a network route through the server firewall. The troubleshooting pages list error 40615 and `AADSTS` failures.
- **The web dashboard now has its own store login, and the read-only `viewer` login is read-only** ([#6991]) - The dashboard used to connect as `viewer`, which therefore held every write the pages make, and that same login is what the export verbs and the quick start give to remote read-only seats. The service now creates a `web` login (credential file `pg-web-credential.dpapi` on managed stores, `pg-web-credential` in the compose credentials volume) that holds those writes, and takes them away from `viewer` on the first start after the upgrade. Bring-your-own stores are not changed by this part.
- **Fleet-wide Query Store panels read a pre-summed rollup** ([#7146]) - an unfiltered all-server panel grouped by query hash reads one row per stamp time and hash instead of one per server, database and module. Migration V239 adds the table and the service fills it, newest hours first. Until an hour is rebuilt it reads the old way.
- **Removed an Entra MFA username box that did nothing from the Viewer's Add Server dialog** ([#7227]) - The "Username (optional)" box under Entra MFA was never read, and Save refuses that mode anyway. The box and its panel are gone and the warning for the mode is unchanged. Unused element names in the Viewer and Lite windows and the unused server argument of the proposals read were also removed, with no change in behavior.
- **Removed unused web UI code** ([#7226]) - Deleted ten unused JavaScript exports, six unused CSS rules for the retired custom-range input, and folded three copies of the browser time zone helper into one. No visible change.
- **Removed two remediation builders that could never produce an action** ([#7220]) - The clear cached plan and server config builders read drill-down data that Lite and Darling never write, so they always returned nothing. They are removed along with their notification item, risk text and copy-paste renderers. No visible card or alert changes, because neither ever appeared.
- **The blocking trend chart now says it counts blocked-process reports, or DMV snapshot rows when the window has none** ([#7219]) - the Lite and Viewer Blocking Trends chart, its tooltip and legend, the correlated timeline, the web dashboard's Overview Blocking strip and the `get_blocking_trend` tool text called the bars blocking incidents or events, which read like blocked sessions. The bars have always counted blocked-process report rows per minute, and one blocked wait can write several. When a window has no reports (an AWS RDS server, or a database filter), the trend counts DMV snapshot rows, and those surfaces now say so instead of calling them reports. On both apps' charts the axis title is now short ("Reports per min" or "Snapshots/min") and the legend carries the full name and the source. Only the wording changed: the counts and the baseline bands are the same.
- **Documented that a bring-your-own store's TimescaleDB jobs follow the server time zone, and what the sample's certificate setting does** ([#7229]) - The docs now say that on a store you bring, compression, retention and refresh windows follow the server's time zone (boundaries shift by up to the zone offset, no data is wrong). The compose sample and the configuration page now explain `trustServerCertificate` and say production targets should keep verification on.
- **`create_mute_rule` gives the same end-before-start sentence as the web, and route parameter descriptions state the real defaults** ([#7224]) - `create_mute_rule` now refuses an end-before-start window with the sentence "The end (...) must be after the start (...)", the one the web create and update already give. The `create_notification_route` parameter descriptions that MCP clients read now say "Omit for Inherit (the next level decides)" for `agent_level` and "Omit for the default (20 / 100 / 15 / 240)" for the four limits, matching the defaults the route really uses.
- **Darling runs 4 long reads at once by default, up from 2** ([#7357]) - On a large store with 32 vCPU, 2 slots refused 10 to 19 percent of MCP and web readers as busy while the fleet 24 hour panel loaded, and 4 slots refused none. `reads.maxHeavyReads` now defaults to 4. A store that sets the value keeps it.
- **The fleet resource-use-by-tag read splits its hourly Query Store part into plain hour ranges** ([#7365]) - The Query Store part of the fleet read ran its hourly part through a list of hour ranges that one process had to read whole. It now reads each hour range with plain bounds and groups each part by server and tag, so PostgreSQL can hash the joins and split the scan between workers without writing temp files. Results are unchanged.
- **PostgreSQL trend sections in the web viewer are now charts** ([#7394]) - Database Trend, Query Duration Trend, Wait Trend and I/O Trend were tables with one row per sample. They are now line charts in the same series groups as the desktop viewer, blank values (for example avg read ms when track_io_timing is off) stay gaps or leave the chart out with a reason, and a statistics or profile reset is marked on each chart. The data-start note these four sections used to show is no longer drawn, the axis shows where the data begins.
- **PostgreSQL panels say what is missing in one plain sentence** ([#7423]) - Panels that were empty because an extension was not installed, the monitoring login could not read a source, or plan capture was not ready showed several lines of tool text. They now show one short sentence, and the full text is behind a "More detail" click. Empty panels that share one read say it once. Several empty-state paragraphs on the Activity, Vacuum, Replication and Storage tabs are now one line. The tiles "Combinations returned", "Databases returned", "Tables returned", "Indexes returned" and "Sessions returned" are gone, and a page that was cut short says so in words under the tiles. Small table and index sizes on the Storage tab read in KB instead of 0 MB.
- **Query stats stores only the statements that carry the work in each run, and Darling reads statement text only for new statements** ([#7419]) - After #7238 the query_stats collector stored every statement that ran in the last ten minutes, which made each run several times slower and wrote several times more rows. A run now stores a statement only when it is in the top 200 of that run by CPU, elapsed time, executions or logical reads for the interval. Statements outside those lists are not stored for that run, but their baselines still advance so the next stored delta covers one interval. A statement that just recompiled keeps its credited work. A first run after a restart stores at most 200 rows. Darling also stops reading the text of statements it already stores: the text is read in a second query for statements it has not seen, and the first run after a restart reads the text of every kept statement once. Part 1 applies to Lite and Darling, the text change to Darling on SQL Server and RDS. The CPU by database, resource use by tag (plan cache section) and query heatmap pages now say their totals, counts and shares cover the stored statements.
- **The daily query count is labeled as queries stored** ([#7491]) - The collector keeps only the top 200 statements per ranking in each collection, so the daily count counts stored statements and on a busy server it is a lower bound. The web Overview labels and the calendar day detail now read "Queries stored", and the notice above the grid reads "Stored query count not materialized". The Viewer and Lite calendar line reads "Queries stored". The guide text of `get_daily_summary` and `get_daily_summary_range`, in both products, says what the field counts. The wire name stays `unique_queries` and the number is computed as before.
- **Trend and chart panels say what is empty in plain words** ([#7410]) - The PostgreSQL Wait Trend and Query Duration Trend show one short sentence when nothing is there, with the full text behind "More detail". Several SQL Server panels (Waiting Tasks, Blocking Severity, Lock Waits, the procedure and Query Store trends, the health parser panels and Memory Pressure Events) no longer explain how the data tool works in their empty text.
- **Query Store, Query Heatmap, Alerts, Mute rules and Known patterns say what is empty in plain words** ([#7410]) - An empty alert list that hid dismissed alerts now says how many and to tick Show dismissed. The Query Heatmap, Mute rules and Known patterns show one short sentence when empty, with the full text behind "More detail". The Query Store Memory Clerk panel no longer says the same thing twice, and the Query Store Overhead detail no longer names an internal setting. The Alert settings tab says in one sentence when no settings have been saved yet. In the Darling Viewer, fourteen PostgreSQL panels (Plan Capture Readiness, Captured Plans, Log Events, Wait Sampling, Predicate Statistics, Kernel Statistics, Deadlocks, Configuration Changes, Xmin Horizon, Replication Slots, Lock Stats, Autovacuum, I/O Stats and the Wait Trend) show one plain sentence when empty, with the full explanation in a tooltip, and the Plan Capture Readiness grid shows a plain label in its Facet column with the raw name in a tooltip.
- **The Database Protection overdue ages are now settings** ([#7611]) - A full backup was always overdue after 7 days, a log backup after 60 minutes and a CHECKDB after 14 days, with no way to change that. Each age is now a setting (backup_overdue.threshold_hours, log_backup_overdue.threshold_minutes, checkdb_overdue.threshold_hours) with the same defaults, editable in the Viewer, the web dashboard and MCP, and each can be overridden for a server or a database. An agent can set any age within range, since no value of these turns the alert off. The alerts, get_database_protection, the fleet page and the Viewer all judge each database against its own age.
- **FinOps, Fleet Sweeps and Availability Groups display details read right** ([#7562]) - The Verdict tile no longer breaks its word, the Stolen memory tile takes amber and red (red above 85, as on the desktop bars) with a sentence and a figure that keep one decimal in every app, the MCP note uses the same decimal point on every host, the Database Sizes column is headed Free and Max size reads in TB, High Impact Band has color, the Host OS cell shows the short name, Fleet Sweeps says what happened in words, and both views of an availability group print the primary the same way.
- **Lite and Viewer charts spend less space on chrome** ([#8086]) - The legend has no frame or fill. It sits at the right edge when the chart is at least 3 times as wide as it is tall, has 12 series or fewer and is tall enough to list them, and under the axis otherwise. The date shows only on a time label where the date changes, a window inside one day shows no date, and when the step is whole days each label is the date alone. A chart with no rows shows one centered muted sentence such as "No Query Duration data in this window", with no axes, grid or box. A chart whose values are all zero draws a flat line on a Y axis from 0. The FinOps Version Store legend no longer sits over the plot. The plot now keeps at least 55% of its chart.
- **The Lite tempdb chart now sits in a titled card** ([#8144]) - The tempdb usage chart on the server tab now has a card with its name as the title, the first chart of the visual refresh. Lite and the Viewer also share one set of title, subtitle, KPI and empty-message text styles that follow the Dark, Light and Cool Breeze themes.
- **Every Lite chart now sits in a titled card with only its unit on the Y axis** ([#8144]) - On the server tab and in FinOps, each chart has a card whose title is the chart's name, so the Y axis shows just the unit (ms, MB/s, %, count) and no longer ends in "..." on narrow windows. A chart whose values are all zero says "0 throughout" in its card, the blocking charts show which collector answered in the card subtitle, and the Session Stats tab scrolls on a small window instead of squeezing its chart.
- **Viewer charts now sit in titled cards and the Y axis shows only the unit** ([#8151]) - Every chart on the Viewer server tab and the FinOps tab, and the Postgres trend charts, now sits in a card titled with the chart's name. The Y axis title shows only the unit (ms, MB, count and so on), so narrow charts no longer cut the name off with "...". A chart whose values are all zero says "0 throughout" in its card header. On the Blocking Stats duration charts, the collector that answered now shows as the card subtitle instead of in the axis title.
- **Overview timeline strips fill their height and the Lite server card matches the Viewer's** ([#8162]) - each strip's plot now uses the room it is given instead of leaving about 45 px of blank space around it. The strip label moved from a left gutter to a small caption at the strip's top left (placed correctly on high-DPI displays), only the last strip keeps room for its time labels, and the normal-range band is drawn as one shaded shape that steps with each hour, so a marked point is always outside the band you see. The Lite server card now shows a coloured dot on the CPU, Blocking and Deadlocks rows and an "Other" CPU share beside the CPU value, and the card name and fleet counts use the shared title and KPI text styles in both apps (the card name is 14 px instead of 16).
- **Lite's server tabs and sidebar now match the Viewer** ([#8169]) - CPU Scheduler moved under CPU and Plan Cache under Memory, leaving 19 server tabs. The sidebar has a search box that filters servers by name or tag, four icon buttons (Add Server, Manage Servers, Investigations, Settings) and a More menu with the other nine actions, and the subtitle wraps. Lite's tabs are flat with no accent fill, and every tab strip is one row that scrolls with a » button listing the tabs that do not fit. Lite's themes take the Viewer's fixes for the editable combo box and the disabled list. The Spinlock Collisions chart's Y axis reads count/sec in Lite and the Viewer.
- **Grid headers are never cut, an empty window shows one message, and long notes wrap, in Lite and the Viewer** ([#8197]) - Column headers such as "Avg CPU (ms)" were cut mid-word on the Queries grids, also with no rows, because only a few tabs fitted their headers. Every grid now makes each column at least as wide as its header, the filter button and padding. An empty window shows one muted, centered message in the grid area and the time slicer band collapses, instead of an italic sentence in the slicer and a blank grid. The Plan Cache footer, banners above grids and the Collation column no longer clip in a small window, and the Memory Broker sub-tab keeps only a short "Target shrinks" subtitle instead of repeating its tab name.
- **Lite fits its 900x600 minimum window** ([#8219]) - Below 1100 DIPs of window width the sidebar shows its narrow rail by itself and opens again above that. Once you press the sidebar button, your choice is kept. The server toolbar is two rows, and a control that does not fit moves into that row's "..." menu. The status bar is one line, with the full text in its tooltip. On Alert History, Job History, Default Trace and Long Queries, each label wraps together with its control. When the window is under 700 DIPs high the time slicer strip is 70 high instead of 130, in Lite and in the Darling Viewer. Refresh, Databases, Apply to All and Use by tag keep the theme's button face in all three themes.
- **Lite's top tabs, tab rows and menus are flatter and never cut a tab name** ([#8222]) - The selected top tab and the Settings tabs now show an accent underline with no fill, like the sub-tabs, in all three themes. Every tab row scrolls whole tabs behind the "»" menu, so no name is cut at an edge, and every sub-tab row starts at the same left offset. The dotted focus box is replaced by a thin accent outline that shows only when you move focus with the keyboard. The "»" and More menus lose the empty icon column, the "»" menu marks the current tab, and the More menu opens beside its button.
- **Scroll bars are themed and only show when there is something to scroll** ([#8223]) - Lite and the Viewer showed the system scroll bar on every grid (a pale grey thumb, white arrows, a black track), both bars at once even on an empty grid, and an empty grid whose columns were wider than it was cut its last column with no way to scroll. Scroll bars now use a thin thumb in the theme's border colour with no arrow buttons, in Dark, Light and Cool Breeze. This applies to every scroll viewer in both apps, not only grids. Grid scroll bars show only when there is something to scroll, and an empty grid scrolls sideways to its last column.
- **Chart legends follow one rule in both apps** ([#8220]) - A legend beside the plot or with many entries could shrink a chart to a line at the minimum window and leave empty bands at 1280x800. The legend now sits beside the plot when that leaves it 60% of the width, otherwise under it, and a legend that would take the plot under 120 pixels shows as one row ending in "+N more". Charts that share a group agree on a side. The side is re-decided when the window is resized.
- **Empty, loading and count text in Lite and the Darling Viewer uses the refreshed look** ([#8225]) - Count labels are muted subtitles that read as natural plurals such as "3 events" and "4 indexes" (the Alert History dismiss question and status line read the same way, and the web pages' own count text agrees with its number through one rule), empty views show one muted sentence that is not italic, centred inside the grid and wrapping to its width, the Current Waits footnote stays in view at every window size, Index Analysis and the Optimization grids say why they are empty inside the grid, the Locking and Default Trace toolbars wrap beside the sidebar instead of cutting their text, the time-range slicer's empty sentence is muted and upright, and the Long Queries grid says why it is empty in one line under its banner.
- **The MCP server instructions give the real collection cadence** ([#8240]) - Both editions tell an agent that most collectors run every 1 to 15 minutes and some hourly or once a day, and Darling points at get_collector_schedules for each interval, so a daily collector's last run is not read as a stall. The configuration, alerts and tool-table pages are corrected alongside.
- **Chart units read one way, the slicer and heatmap carry one title each, and the Overview strips fit at 1280x800** ([#8224]) - In Lite and the Darling Viewer, every Y-axis unit is the same regular-weight size and the perfmon chart's mixed title is short. The time slicer names its metric in its header, keeps its first time label clear of the handle, and in the Viewer labels round wall-clock steps as Lite's does. The Query Heatmap has one title, a normal colour bar label, a visible colour for small counts in every theme, and colour bar labels that never overlap: a round value is left out when it would touch the maximum's label. The Overview's blocking strip labels whole counts, strip names sit on a solid background, the five strips fit at 1280x800 and keep their plot floor when they scroll, and the Wait strip's band is a lighter tint.
- **The remaining old-layout panels in Lite and the Darling Viewer take the card look** ([#8226]) - In both apps, Running Jobs, Collection Health, the Perfmon and Memory Clerks pickers, Memory Broker, the Memory Overview card and the FinOps Utilization and Optimization pages sit in titled cards with one scroll area per page and grids that size to their rows, the search boxes show a placeholder, the FinOps savings sub-tab is named Cost Savings, a recommendation names its title once, reads a fact token as words and offers Ask AI as an accent button, and the server toolbar has no separator before the Times group. The calendar shows only its month's weeks, Lite's sidebar names a server once when its display name is its server name, the Viewer's chart titles now match Lite's (title case, matching the tab headers above them), and count labels read "2,000 runs".

### Fixed

- **`uninstall-darling.ps1 -PurgeData` now removes the `PerformanceMonitor Darling Operators` group the install created** ([#6953]) - the purge deleted the store but left the local group behind. It now removes the group, with its members, only when its description is the one the installer sets. A group with any other description stays, and the script prints the command to remove it by hand.
- **Clicking the triage link in a Slack alert no longer shows "This app is not configured to handle interactive responses"** ([#6965]) - the link was a Slack button. Slack sends a request to the app on every button click, even when the button only opens a link. A Slack app set up with only an incoming webhook has nowhere to receive that request, so Slack showed the warning. The link is now plain text in the alert. It opens the triage page and sends nothing to the app.
- **The record of chunk-interval reconcile runs is now deleted after 30 days** ([#6977]) - before, no rows were deleted. The record gets one row a day, plus one for each service start.
- **Lite's windowed MCP reads now say when the window reaches past the data Lite holds** ([#7005]) - an empty answer over a window that starts before Lite's oldest data used to read as a quiet server. It now carries `window_truncated`, so an agent can tell "nothing happened" from "we hold no data that old". Answers that return rows carry `effective_start`, `window_truncated` and `truncation_note` as well. This covers the Queries, Wait Stats, System Health, Blocking, Latch and Spinlock, CPU, Memory, File I/O, TempDB, Perfmon, Plan Cache, Client Timeout and Current Waits reads. It also covers `get_alert_history`, `get_analysis_facts`, `get_analysis_findings` and `get_resource_use_by_tag`. A window that is fully covered still says so, including an empty alert log or no findings.
- **Darling's MCP reads now say when the window reaches past the data the store holds** ([#7010]) - this covers the SQL Server reads and the PostgreSQL reads. The PostgreSQL ones include the wait, query duration, I/O and database trends, autovacuum health, replication slots, wraparound risk, table bloat and extensions. Over a window that starts before the store's data, a data answer carries `effective_start`, `window_truncated` and `truncation_note`. An empty answer carries them under `hints`, so a quiet-looking answer no longer reads as an all-clear. Five reads used to answer `window_truncated` false over such a window: the query duration, procedure duration and Query Store duration trends, `get_job_history` and `get_query_store_aborts`. `get_fleet_overview` marks the server cards that hold less data than the window. The autovacuum and replication slot all-clear no longer claims the whole window was quiet when the store holds only part of it. `get_pg_index_bloat` and `get_pg_column_stats` carry no notice, because their own coverage verdict says what the window held. Reads that answer one newest snapshot, such as `get_pg_buffer_usage` and `get_pg_plan_capture_readiness`, state `captured_at` and `age_seconds` instead.
- **`backtest_custom_alert_rule` now says when the span reaches back past the stored samples** ([#7015]) - a replay over a span older than the store's raw samples did not say so, so a no-fire answer over a partly uncovered span read as complete. It now carries `effective_start`, `window_truncated` and `truncation_note`, as the other windowed tools do, and over several servers it reports the least covered one. `get_daily_summary_range` is unchanged: each day it returns already carries its own data state.
- **Client Timeouts no longer alerts on cancels under 1 ms or on Azure Data Studio's own metadata queries** ([#7061]) - Sampled events that lasted under 1 ms (SqlClient attentions on commands that had already finished) and Azure Data Studio's SMO metadata reads (`create table #SVer` and the column read) are left out of the judged count, in Darling and Lite. An event with no recorded duration still counts. Azure Data Studio's query editor still counts. It connects under the same `azdata` name as its metadata reads, so only those two statement shapes are left out. The baseline now leaves out the same events plus SSMS IntelliSense and VS Code metadata connections, so they no longer inflate it. The new baseline is read at the next service or Lite start. The alert text and `get_client_timeouts` name the left-out events: `excluded_from_alert` gains `below_duration_floor_events` and `azdata_metadata_events`, and each sampled event carries `below_duration_floor` and `azdata_metadata` flags.
- **The Darling service log no longer repeats connect failures and permanent precondition warnings** ([#7067]) - While a server's connect failure has the same cause, "Connect still failing" is logged at most once an hour for that server, with the attempt count and the time down, and the `Connected` line reports how many attempts failed over how long. A precondition warning such as `EXTENSION_MISSING` is logged when first seen and when its text changes, then as a reminder at most once a day. One host with seven switched-off servers had logged 1,982 "Connect still failing" lines in a day. `collection_log` rows, collection health, alerts and MCP reads are unchanged. Lite is unchanged.
- **Standing alerts no longer fire again after a service restart** ([#7068]) - A condition that stood across a restart wrote a new history row, logged `ALERT TRIGGERED` and offered a send again, because the alert engine kept its last-alerted level and cooldown only in memory. On the first sweep after a start the engine now reads the alert's open fire row, and a standing condition at the same level stays quiet. This covers Volume Free Space, Version Store (PVS), Force Plan Failures, Database File Growth, Capture Down, Collection Stopped, Agent Not Running, Agent Alert Gaps, Backup Overdue, Log Backup Overdue, CheckDB Overdue, Identity Capacity, AG Sync Fell Behind and the Custom Alert Rules Unhealthy, External Alerts Undelivered and Collection Falling Behind fleet alerts. Lite has the fix for the alerts it runs. A worse level, a lower free percent past the worsening margin, a different volume, file, plan or database, and a condition that cleared before the restart still fire. Database File Growth now remembers every breached file, so a tempdb with 8 files that breach together no longer announces files 6 to 8 again. The store-wide alerts keyed by job (Store Job Over Cadence, Retention Held, Raw Purge Over Horizon, Compose Rollup Hours Stuck and the Policy Job band alerts) are covered by the [#7076] entry below.
- **Store-wide alerts keyed by job no longer fire again after a service restart** ([#7076]) - Store Job Over Cadence (one alert per job), Retention Held, Raw Purge Over Horizon, Compose Rollup Hours Stuck and the Policy Job band alerts (compression, refresh and retention jobs) announced again after each restart while their condition stood, because their history rows carried nothing that told one key from another. Their rows now carry the key, so a standing condition stays quiet across a restart and a second job that sticks still fires. A stuck Policy Job that was re-armed before a restart still sends the one "re-hung" escalation it was owed and does not re-arm again. A job that cleared while the service was down is closed in the history and announced again if it sticks later. Store schema version 234 adds a nullable `alert_key` column to `config.config_alert_log`, with no backfill, and needs no action. Rows written before the upgrade have no key, so each of these alerts that is standing announces once after the upgrade and then stays quiet across restarts. Darling only: Lite does not write these alerts.
- **A report notice's root cause keeps its sentence under `maskLiterals`** ([#7106]) - with `mcp.dataDetail` set to `maskLiterals`, the notice text for a report treated every apostrophe as the start of a quoted string, so "The session's blocker has held 3 locks" became "The session'?'" and "both sessions' locks are held" lost the rest of its line. Text an agent wrote (the root cause and the object of each change) now keeps an apostrophe that follows a letter or digit (`session's`, `doesn't`, `sessions' locks`) and no longer reads ` -- ` or `/*` as a comment, so "the blocker -- session 87 -- held 3 locks" is not cut off. Quoted values (`'alice'`, `name='abc'`, `N'abc'`) and numbers are still replaced. SQL text is masked as before.
- **A retention hold no longer outlives its consumer's own retention** ([#7114]) - The coverage gate called a consumer short whenever its oldest bucket was later than the held tier's oldest row, ignoring the consumer's own retention. A hold on `query_store_stats_interval_hourly` that lasted more than 3 days became permanent, because `query_store_stats_interval_daily` trimmed itself to its 10-day horizon every day. A consumer with armed retention, whose own coverage check also passes, is now checked only as far back as it keeps, so such a store arms on the first hourly pass after the upgrade. The Retention Held alert no longer says arming by hand drops the only copy, and the `--backfill-rollups` race warning is gone.
- **The Viewer's Mute Rules list shows each rule's action, and the rule dialog's text matches it** ([#7122]) - A mute rule that sends matches to the daily digest looked the same as a plain mute in the list, and the dialog still said alerts were suppressed or muted. The list now has an Action column ("Mute" or "Daily digest"). The window's intro line, and the dialog's intro, daily-window text, filter hints and no-filter warning, follow the chosen action. The Mute Rules window opens wider to fit the new column.
- **The Blocking alert settings now say what they do, in the Viewer and Lite** ([#7123]) - The Settings windows labeled the count "Blocking sessions" and the wait "Total blocked wait". The count is blocking events in the last hour, and it pages only when the longest wait among them is 15 seconds or more. The wait box is also the bar at which one blocked session pages at once. The labels are now "Blocking events" and "Long blocking wait", the help text says so, and the boxes start at the current defaults (3 and 60).
- **The "Starts" label in the Viewer's mute rule dialog now shows in full** ([#7138]) - It was cut off at the right edge of the dialog. It now wraps onto a second line.
- **The multiple boxes in Settings, Alert rules, now fit two digits, in the Viewer and Lite** ([#7139]) - The client timeouts warning and critical multiples and the Agent jobs "x historical average" box were narrower than the boxes beside them, so a value like 10 was cut off. They now match.
- **The 31-day Daily Summary and calendar read far fewer pages on a store without TimescaleDB** ([#7147]) - On a bring-your-own PostgreSQL store with plain tables, the month read scanned every heap page of the month, and its distinct-query count read through a view that kept PostgreSQL from using an index-only scan. On a store with 14.2 M `wait_stats` rows the read timed out at 15 s. The count now reads `collect.query_stats` directly, and the service builds a covering `wait_stats` index once at startup on stores without TimescaleDB. The numbers are the same, and a measured run read about 7 times fewer pages.
- **`upgrade-darling.ps1` tries the backup-folder step before it stops the service, and has a `-Check` switch** ([#6982]) - Under PowerShell 7 the step after the stop failed in 3.11 and left the service down (#6920). The upgrade now tries that step on a throwaway folder under the install root first, and stops with the reason while the service is still running if it fails. `-Check` runs only the checks before the stop plus that trial, prints the PowerShell version, and changes nothing.
- **The statement filter now withholds more password shapes** ([#6993]) - statements that carry a libpq `sslpassword=` value or a `SQLCMDPASSWORD=` setting, a pgcrypto call (`crypt`, `hmac`, `encrypt`, `decrypt`, `pgp_sym_*`, `pgp_pub_*`) with a literal argument (at the call's own level or inside one nested call), an armored PGP private key, or a `bcp`, `sqlcmd`, `osql` or `isql` command line with a `-P` or `/P` password (for example inside `xp_cmdshell`, including a `-U user -P pass` pair written before the tool name) were stored and shown as they were. They are now replaced by the withheld-statement placeholder everywhere the filter runs, for SQL Server and PostgreSQL targets alike. A pgcrypto call whose key is a parameter but that carries an inline algorithm or option literal (`hmac(body, $1, 'sha256')`) is withheld too. A statement that mentions one of these tools or functions without the value (a parameter such as `crypt($1, pw)`, or no `-P`) stays visible. Statements stored before the upgrade keep their text until retention removes them. A `SET`, `SET LOCAL` or `set_config` of a setting whose name contains key, secret, passphrase or token (for example `app.encryption_key`) to a literal is withheld as well, and so is a column whose name contains pass or pwd compared with `=` to `HASHBYTES(..., 'literal')` or `digest('literal', ...)`.
- **`metadataOnly` now masks the login in `set_by` and in the investigation write answers** ([#6997]) - With `mcp.dataDetail` at `metadataOnly`, the login in `set_by` (server actions, threshold overrides) and in the investigation write answers (`closed_by`, `decided_by`, `opened_by`, `saved_by`, `modified_by`, `author`) is now masked like other logins.
- **`get_investigation_evidence` no longer journals a refused claim-token read** ([#6997]) - Repeated refused reads can no longer fill a request's agent journal.
- **On Linux the diagnostics bundle's alias map file is readable by its owner only** ([#6997]) - The file is now created that way.
- **An analysis finding's alert notebook shows its database name and story text as plain text** ([#6997]) - Markdown link syntax in them is no longer clickable.
- **A store owner with any name now migrates** ([#7003]) - Migration V8 created the `collect` and `config` schemas owned by a role named `darling`, so a bring-your-own PostgreSQL store whose owner login had another name failed to start with `role "darling" does not exist`. The schemas are now owned by the login that runs the migration. A managed store migrates as before, and a store already past V8 is not touched.
- **Overview utilization card no longer reads the whole archive for its newest-row lookups** ([#7000]) - the card read the newest memory and server properties rows through the full archive, so its cost grew with the number of archived days. It now reads the live table, and the archive only for a server with no live row, so a server whose rows were all archived still shows its last values. Top Queries by CPU is unchanged: it still names a procedure from all history, as Darling does.
- **`upgrade-darling.ps1` refuses to replace a self-contained install with a framework-dependent build** ([#7063]) - The release zip is self-contained and ships `hostfxr.dll` beside the service exe, while a plain `dotnet publish` is framework-dependent and ships none. The upgrade copied one over the other, the old runtime files stayed in the folder, and the new exe found no framework, so the service did not start. The script now compares the two shapes before it stops the service. It refuses when the source has no `hostfxr.dll` at its root and the install has one. The message names both shapes, says to use the release zip or publish with `--runtime win-x64 --self-contained`, and says nothing was stopped or copied. `-Check` reports it too.
- **The Daily Summary wait_stats index is now built when wait_stats is a plain table** ([#7152]) - the index was skipped on any store with the TimescaleDB extension installed, even when the service ran in plain-PostgreSQL mode with a plain wait_stats table, so the month read had no index. It now follows whether wait_stats itself is a hypertable. A store that later converts wait_stats to a hypertable has the index dropped at the next tuning pass, since it cannot serve the read there.
- **An external alert that rises to critical is no longer held back by the delivery cooldown** ([#7158]) - A source that posted the same alert as warning and then as critical inside the email or webhook cooldown never had the critical sent, and the alert recorded critical as delivered. This also happened when a new episode's opening warning was itself held back by the previous episode's cooldown. A critical on a firing alert now bypasses the cooldown unless critical was already sent, and a critical the cooldown held back is not recorded as sent.
- **A short failure of the deadlock or blocked-process session check no longer blocks reads for an hour** ([#7164]) - A timeout or dropped connection during the hourly session check was stored as "session missing", so deadlock and blocked-process collection stopped reading a running session and Capture Down could fire falsely. A transient failure now leaves the runs reading as normal and the check is retried after 5 minutes.
- **The Darling service now writes its read-latency and collector cost samples when it stops** ([#7153]) - a stop or restart used to lose up to one hour of both, because they were only written on the hourly tick. The service now writes them once while stopping, and a failure or a slow store logs a warning and does not delay the stop.
- **PostgreSQL wraparound, replication slot and poison wait alerts now carry the server in their PagerDuty dedup key** ([#7157]) - Two PostgreSQL servers with the same database, slot or wait event used to share one PagerDuty incident, so the second server's page was merged away and one server's clear could close the other's incident. Each server now gets its own incident. Each subject pages once more after the upgrade because its key changes.
- **A managed store no longer stays running after a cancelled or failed startup** ([#7162]) - If the service was stopped or hit an error right after starting its bundled PostgreSQL, the store kept running and blocked later upgrades. The service now stops the store it started on those paths.
- **Query-tag rollup blocking figures count each wait once** ([#7163]) - The blocking_reports section summed every blocked-process report, so a long wait was counted many times over and parallel-query self-waits counted on both sides of a tag. It now counts each wait once at its longest report and leaves out self-waits, in Lite and Darling.
- **Dismiss All no longer dismisses alerts you cannot see** ([#7160]) - With a column filter on, or when the list was cut at 500 rows, Dismiss All dismissed every alert in the time window instead of the rows on screen. It now dismisses only the rows shown, so over a window with more than 500 alerts it dismisses the 500 shown and leaves the rest for the next read. In Lite, archived alerts in the grid are now dismissed by Dismiss All and Dismiss Selected like any other row, instead of being refused with a message.
- **Query trend charts no longer dip when a query that runs every 15 or 30 minutes appears** ([#7161]) - Each collection was rated over the longest interval of any one query in it, so the duration and executions-per-second lines fell about 15-fold in that minute. Each collection is now rated over its gap to the previous collection, in Lite and Darling.
- **Top Queries no longer misses a query whose plan just recompiled or regressed** ([#7156], [#7337], [#7344]) - query_stats kept only the 200 heaviest plans by lifetime elapsed time, so a newly regressed plan stayed out until it out-weighed long-cached plans. It now keeps every statement that ran in the last ten minutes, newest first, and skips statements that did no work since the last run. Plan capture still takes at most 200 plans a run. By default those are the 200 statements with the most elapsed time since the last run. On Azure SQL Database, or with deferred plan fetch turned off, they are the 100 newest statements and the 100 heaviest by total elapsed time, leaving out the collector's own queries.
- **A PostgreSQL target no longer gets made-up zero counts and Healthy days from the daily summary tools** ([#7159]) - get_server_summary, get_daily_summary and get_daily_summary_range now answer not_collected for a PostgreSQL target and point at get_fleet_overview and the get_pg_* reads. A custom view on get_server_summary shows that sentence in its panel.
- **Fleet resource use by tag stays on the hourly rollup after a quiet spell** ([#7240]) - the rollup's materialization ceiling is now read on every coverage probe instead of being served from the cache with the floor, so a first read after hours of no coverage reads no longer falls to the raw tables, and hourly reads report the rollup's current edge.
- **The Collection Health tab header counts a collector only while its latest attempt was refused after its last success** ([#7241]) - The Darling Viewer and Lite counted any collector refused a permission in the last 7 days, so "(N no permission)" stayed for up to a week after a grant was added. Both now count a collector only when its latest refusal came after its last success, the same rule as `denied_since_last_success` in `get_collection_health`. A grant revoked after earlier successes is counted from its first refused run, and a collector the server's engine does not run is never counted.
- **The Agent Alerts "Last fired (server clock)" column now uses the display format** ([#7243]) - it showed a raw ISO stamp on the web Config tab and in the Viewer. The monitored server's own time is now written like the other times, with no time zone conversion, and the Viewer grid still sorts by the time.
- **A first start whose database setup was interrupted no longer gets stuck** ([#7191]) - If the first-run database setup was stopped partway by a timeout, a service stop or an outside kill, every later start failed until the data folder was deleted by hand. The next start now moves the half-made folder aside (to a sibling named like pgdata.interrupted-20261102153000, safe to delete once checked) and sets up again, after copying the shared database password file beside it (pgdata.interrupted-20261102153000.pg-credential.dpapi) so the moved folder can be restored. The marker records its data directory, so a marker left by a failed first run no longer causes a different data directory to be touched. A data folder that holds postmaster.opts or backup_label (a store you restored there) is left exactly as it is: the stale marker is deleted and the service starts against it. A restored file-level copy that never ran and has no backup_label is still treated as half-made and moved aside.
- **An idle server no longer reads WARNING after the Darling service was stopped for a weekend** ([#7242]) - the 72 hour quiet bar for `procedure_stats`, `query_snapshots` and the other activity collectors counted wall-clock hours, so hours the service was off counted as quiet. It now counts the time the service watched (the gaps between the zero-row runs, each gap counted at most three intervals and an hour at least) on the service, the Viewer overview and server tab, and Lite.
- **Default email recipients are now checked when saved** ([#7184]) - A default recipient list with an entry the mail send cannot read made every alert email on the default route fail. That covered two addresses joined with a semicolon, and a semicolon followed by a space dropped the first address with no error. The Viewer and Lite settings now refuse such a list on Save and name it in Validate. Lite keeps the stored recipients. A list stored before this fix is now refused at the send too, so alert history shows the same sentence instead of a raw error or a first address dropped with no error.
- **Viewer credential profiles survive an unreadable or interrupted save** ([#7209]) - An unreadable profile file is now copied aside before the next save replaces it, and profile, server and settings files are written to a temp file and moved into place so a crash cannot leave a truncated file.
- **A long overdue-database list no longer makes Teams reject the alert** ([#7202]) - The Backup, Log Backup and CHECKDB Overdue alerts named every overdue database, so a server with hundreds of them produced a Teams card over Teams' size limit that was rejected at every repeat. These alerts now name the 10 most overdue databases (oldest first) and count the rest, and the Teams card cuts any long detail text at a line or word boundary with a short notice. The cut is sized on the JSON-encoded card, so non-ASCII names (Cyrillic, CJK) and names with `&` stay under the limit too.
- **Lite investigation reads no longer run under a database reset** ([#7217]) - The investigation store's reads (the Investigations window and the MCP investigation tools) opened their connection without the store read lock, so an archive reset could delete and re-create the database while one was open. They now take the read lock first, so the reset waits for them.
- **Stale Mute Rules no longer flags recurring windows or digest rules** ([#7183]) - A recurring mute window or a digest rule with no expiry was reported as a stale mute after seven days, and as Critical "suppresses EVERY alert" when it had no scope. Both are permanent by design, so the alert now skips them. A recurring window that is open 23 hours or longer on every day of the week is not a real bound and is still reported, and so is a plain open-ended mute.
- **Query Store plans and statement text are no longer lost when a fetch times out** ([#7187]) - When a plan or statement-text fetch against a monitored database threw, the ids first found missing in that pass were forgotten, so a query that ran only once never got its plan or text. They are now kept for the next pass, and an id is dropped after 5 fetch attempts in a row that threw, so it cannot hold up the ones behind it. A store write that times out counts against no id, and a pass that did not try the id neither ends nor extends the run.
- **The web dashboard now switches to its write login by itself after a slow first start** ([#7192]) - On a managed store, if role provisioning finished more than 5 minutes after the service started (a large store or a major upgrade), the dashboard stayed on the read-only login and refused every write until the service was restarted. It now watches for the web credential and restarts itself once it appears.
- **RDS PostgreSQL stderr logs no longer lose a plan or a log event cut at a portion boundary** ([#7186]) - On RDS and Aurora PostgreSQL with stderr logging, an auto_explain plan or a multi-line log event (with its DETAIL and STATEMENT lines) that fell across two log portions was dropped or stored incomplete. The unfinished tail is now held and joined to the next portion.
- **Lite alert reads no longer scan every Parquet archive file, and slow reads say where the time went** ([#7256]) - The forced-plan, poison-wait and file-growth alert reads asked a window of a few minutes or hours but opened the footer of every archive file on each tick, holding the store lock for the whole read. They now read the hot table alone when the archive holds nothing in the window. The data-start probe stops at the first older row of the server in its hot table, and skips the archive when the whole archive holds nothing older than the window start. The slow-query log line now reports lock wait, connection open and query time separately.
- **Opening a server tab no longer mixes its collector timings and notes with the scheduled sweep's** ([#7215]) - Opening a tab while the sweep was collecting from the same server let both runs share one telemetry slot, so a collection log row could show the other run's timings or note, and an abandoned collector could be logged as a success. Runs on one server now take turns.
- **A saved custom view loads its panels two at a time and retries a busy read** ([#7257]) - A saved custom view now loads its panels two at a time, sends a read the service refused busy once more after the wait it names, and shows a Retry button on a panel whose read failed, so the later panels of a view are no longer refused.
- **A deadlock signature merge made with MCP writes on can now be undone** ([#7185]) - The merge_deadlock_signatures tool told you to undo a merge from Agent changes, but a merge made with mcp.allowWrites on was never journaled, so it was not listed and could not be undone. It is now journaled with actor mcp and Agent changes can undo it.
- **Darling and Lite MCP reads no longer call an offline SQL Server's empty window an all-clear** ([#7167]) - Darling's trend, top-query, Query Store, blocking, deadlock, active-query, config-change, alert-history, analysis-findings, FinOps and system_health reads answered `empty` when the monitored server had sent nothing for days, and the memory, file I/O, server and lock wait trend tools claimed the window was genuinely quiet even when it was never read. An offline server now answers `unavailable` with the offline sentence first, a cut window says nothing was read, and a window inside collected history stays `empty`. Lite applies the same rule to its windowed MCP reads, judged from the server's last successful collection (no successful run in 30 minutes counts as offline), and its memory, file I/O, wait, lock wait, query and Query Store trends make the same wording change.
- **The Viewer Proposals tab no longer shows an empty list when the service did not answer** ([#7168]) - If the service was stopped or busy for more than 30 seconds, or the read command failed, the tab cleared the grid with no message. It now keeps the old list and shows "The service has not listed the proposals yet. Reload the tab to read them again."
- **The wait-profile card's "x its baseline" multiple now matches the numbers beside it** ([#7177]) - When an analysis window spanned more than one local hour on a server with a trusted baseline, the card printed the worst hour's peak and baseline mean but a multiple taken against the first hour's baseline, so the three figures did not divide. The multiple now uses the same peak and baseline as the rest of the card. Which windows fire and how they score did not change.
- **The disk space card now quotes the free space of the volume that triggered it** ([#7179]) - The card took the smallest free MB figure from any volume instead of the volume with the lowest percent free, so a small healthy volume could be named as the tight one. Lite and Darling now take the free MB from the lowest-percent volume and the card names its mount point.
- **set_notification_route_fallback no longer reports a write that did not happen** ([#7199]) - If a route was deleted or given an agent setting while the call ran, the tool still answered updated. It now answers not found, refused, or invalid, and the write itself skips agent routes and fallbacks that are not routes.
- **Schema-modification lock waits now reach their own SCH_M analysis card** ([#7178]) - `LCK_M_SCH_M` waits were folded into the generic LCK fact, so the SCH_M scorer, advice card, link to the running Agent job and the maintenance grouping of wait-profile anomalies never fired. Both Lite and Darling now key those waits as SCH_M. `LCK_M_SCH_S` still counts toward LCK.
- **pvs_stats now collects every database on an Azure SQL Database logical server** ([#7165]) - A logical-server registration connected to master and ran the version store query once, so only master's version store was collected and no user database's growth could alert. The collector now runs once per database on Azure SQL Database, as the other database-scoped collectors do.
- **Daily Health Report no longer ends its period in the future inside a daylight saving gap** ([#7201]) - When the report's send time fell in the hour skipped on spring-forward day (for example 02:30 in America/New_York), the scheduled report covered a period that ended up to 59 minutes in the future, and `get_daily_health_report` disagreed with it. Both now end the period where the skipped hour ends (03:00) and use one shared calculation.
- **Editing a mute rule no longer changes when it expires** ([#7173]) - The edit dialog rounded the expiry to 1 hour, 24 hours or 7 days from now, so changing only the reason could extend a mute window by days or turn an expired rule back on. The Expires After list now starts with "Keep: <current expiry>" and saves it unchanged unless you pick another choice, in both the Viewer and Lite.
- **The High CPU and Runnable Queue alert notebooks now plot the hourly peak instead of the hourly average** ([#7203]) - The CPU and runnable-task timelines averaged each hour, so a short spike that fired the alert was flattened out of the chart. Each hourly point is now the highest reading in that hour.
- **Query Store top queries average and rank by how often each interval ran** ([#7169]) - The Viewer grid, the `get_query_store_top` tool, its long-window daily summary read and Lite's grid averaged the interval averages with no weight for execution count and ranked on executions times that average, so one 10 s run beside 10,000 runs of 1 ms showed about 5 s and ranked as about 50,000 s. Averages are now weighted by executions and the ranking is each plan's total duration. Store version 240 adds weighted sums to the daily summary. Days built before it are read from the interval table until the service rebuilds them, newest day first, 60 builds an hour, so a store with a few hundred servers takes more than a day to rebuild all of its roughly 10 days. Short windows return to the summary within the first hours. A rollback and roll-forward, or a newer and an older instance sharing one store, need no manual step.
- **By-tag resource use no longer opens every retained chunk to read blocking reports and deadlocks** ([#7204]) - The blocked-process report and deadlock sections of the by-tag read were bounded only on the event time, so a fleet-wide call opened every retained chunk of both tables. They now also carry the collection time floor the other blocking and deadlock reads use. Results are the same.
- **FinOps Database Sizes cost share is now of the whole server** ([#7174]) - With a database filter saved from the server tab, each database's monthly cost share was divided by the filtered total, so picking one database showed it costing 100% of the server. Shares now divide by the server's full allocated size, in both Darling and Lite.
- **FinOps Server Inventory Uptime is right when the server is in another time zone** ([#7176]) - Uptime subtracted the server's local start time from the viewing machine's clock, so it was off by the zone difference and could read "0d -4h" after a restart. Lite now takes the uptime from the server's tick counts, so a daylight saving change cannot move it, and the Darling Viewer converts the start time with the server's zone. A Darling row with no stored offset now shows a blank uptime.
- **Query and procedure history windows now save and show the clicked row's own plan** ([#7175]) - Download and View Stored Plan in the query history and procedure history windows read the newest stored plan for the query or procedure, no matter which snapshot row was clicked, so a regressed query's old row gave the new plan. They now read the plan named by the row's plan hash (queries) or plan handle (procedures) and fall back to the newest plan when that one was not stored. The saved file name and the plan viewer title name the plan that was actually read, and a fallback says so in plain text.
- **FinOps high-impact memory is now the current plan's peak grant, not a sum over collection rows** ([#7166]) - The memory figure for each query added up a high-water mark once per collection row, so a steady 1 GB grant seen 96 times showed as 96 GB and outranked a one-off 50 GB grant. It now takes the peak grant of the query's newest plan in the window, the same rule the max DOP figure uses, in both Darling and Lite. The column reads "Peak grant (MB)", the high-impact order breaks CPU ties by query hash, and the MCP key `total_memory_mb` on the high-impact tool is now `peak_memory_mb`.
- **PostgreSQL plan-capture readiness no longer reports ready when plans cannot be read, and no longer misjudges csvlog and jsonlog targets** ([#7182], [#7292]) - The readiness report passed `auto_explain.log_format = text` (the default) and a `log_line_prefix` with `%Q` in a position the stderr plan readers cannot match, even though plan capture stores nothing in either case. Both now show as unmet with a remedy: set `auto_explain.log_format = json`, and put `%Q` directly after `[%p]`, last in the prefix, followed by one space, with the prefix starting with `%t` or `%m` and a space or a colon right after it (for example `%m [%p] %Q `). A prefix with no leading timestamp, such as `%u [%p] %Q `, or with the pid bracket straight after the timestamp, such as `%t[%p] %Q `, now reads as unmet too, because the plan readers need the timestamp and a space or colon after it to find a line. The log line prefix check (`log_line_prefix_readable`) applies the same rule. A colon-separated prefix such as `%t:%r:%u@%d:[%p]:%Q ` now stores plans on self-hosted (`pg_read_file`) targets too, including plans from Unix-socket sessions, whose `%r` and `%h` render `[local]` (both plan readers used to drop those lines). A target whose `log_destination` lists csvlog is no longer judged by `log_line_prefix`, which the csv route never reads: `plan_attribution` reads as met on PostgreSQL 14 or later, where the log's own `query_id` column carries the id, and as unmet on PostgreSQL 13 or for jsonlog without csvlog or stderr, with a detail that says what to change. The MCP tool descriptions (`get_pg_plan_capture_readiness` and the logging audit note) and the Viewer plan text no longer say that a missing `%Q` leaves plans stored as orphans: on a stderr target no plan is stored, on csvlog before PostgreSQL 14 plans are stored without a query id, and on jsonlog only no plan is stored. A `log_line_prefix` with a literal bracket between the timestamp and `[%p]`, such as `%m [%l] [%p] %Q `, `%m [%a] [%p] %Q ` or `%m [%c] [%p] %Q `, now reads as unmet for both facets, because no stderr reader can parse those lines (only the `[local]` that `%r` and `%h` write may come before the pid bracket). The deadlock reader and the log-event reader now read lines from Unix-socket sessions under prefixes such as `%m %r [%p] ` and `%t:%r:%u@%d:[%p]:` too, where they used to drop them while the log line prefix check read met.
- **Lite query tag, server tag, database-state baseline and repair-marker times are now UTC** ([#7200]) - Lite stored the created time of query tags, rules, assignments and server tags, the updated time of database-state expected-state rows, and the Query Store slice repair completion marker in the machine's local time, so on a machine outside UTC those times were hours off from Lite's collected data. New rows are now stamped in UTC. Rows written earlier keep the time they were saved with. The repair attempt log and a few unused default columns still use local time.
- **Opening an engine alert's details no longer freezes Lite** ([#7180]) - Double-clicking an engine alert read its attached findings on the UI thread, so the window froze while archival or compaction held the store lock. The read now runs on a background thread.
- **A PostgreSQL deadlock alert no longer pages for an old pair because of a stale new one** ([#7188]) - A new deadlock pair that did not page on its own stayed marked as unannounced after its reports aged out of the hour, so a later report of an already known pair paged instead of being held for the digest. The unannounced set now drops pairs that no report in the current hour carries, except when the hour has 50 or more reports and the read cannot show them all.
- **A new PostgreSQL statement's first interval now records its real counts, and a statement idle across a restart keeps its first interval** ([#7189], [#7252]) - On PostgreSQL 17 and later, a pg_stat_statements entry that first appeared between two collection passes was stored with 0 calls, 0 time and 0 rows for that interval even though its `stats_since` showed it started inside the gap. The collector now credits the entry's counts to that first interval. Entries with no `stats_since`, or older than the previous pass, are unchanged. Separately, a statement or wait event that had been idle for more than fifteen minutes when the service restarted had its first interval after the restart stored as 0. The restart now reads the last hour for PostgreSQL statement and wait stats, the same window as the delta gap policy, so that interval is credited.
- **Closing a Lite server tab now cancels a running Get Actual Plan query** ([#7181]) - The re-run has no timeout and kept running on the monitored server after the tab closed or the server was removed, then showed a message for a tab that was gone. Closing the tab now cancels it.
- **A SQL Server CPU reading from days ago is no longer shown or banded as current** ([#7196]) - When a server's CPU collector was off or failing while other collectors still ran, the fleet overview, the Viewer Overview card, the Lite Overview card and `get_server_summary` (Darling and Lite) kept showing the last CPU reading as current, and the cards banded on it. A reading older than the larger of 15 minutes and twice the server's `cpu_utilization` interval now reads as not collected with no band, and `get_server_summary` returns no CPU figure for it. Lite uses the same rule and paints the stale CPU row the Unknown grey, not green, and its alert engine still reads the raw newest reading. Darling and Lite read one shared 15 minute floor.
- **A fleet-wide Query Store panel on a large fleet is no longer refused before it runs** ([#7285]) - The size guard counted the hours the fleet rollup serves from the per-server wide-row estimate, so a 24 hour panel with no filter or server scope was refused on a large store even though the fleet table can answer it quickly. The guard now counts those hours from the fleet table's own size. The newest hours still read from the wide table count as before, and panels the fleet table cannot serve keep the old estimate.
- **`get_daily_summary` answers `unavailable` for a day nothing was collected** ([#7193]) - A day the service was down, or a day before the server was registered but inside retention, answered `empty`, which reads as a true negative. It now answers `unavailable` on Darling and Lite, with a message that sends the caller to `get_collection_health`. A collected day with no events still returns its Healthy band, and `get_daily_summary_range` is unchanged.
- **A collection cycle no longer re-runs collectors that a tab open just ran** ([#7269]) - When a server tab was opened just as a collection cycle began, the cycle ran the same collectors again right after the tab-open run, daily collectors included. A collector that ran since the cycle's start time is now treated as not due.
- **Creating a mute rule with an expiry already in the past is now refused** ([#7198]) - `create_mute_rule` and the web create used to answer "created" for a rule whose `expires_at` was already past, so it muted nothing while the caller believed alerts were silenced. They now answer `invalid` and store nothing. Updating a rule's expiry is unchanged, and the tool examples now use a future date.
- **A bad monthly cost in the Add/Edit Server dialogs is now refused instead of stored as 0 or misread** ([#7197]) - In the Darling Viewer and Lite, typing "abc", "-5" or "$120" in the monthly cost box erased or ignored the cost with no message, and "120,50" was stored as 12050. The dialogs now show a message, and `edit_server` also refuses a comma in a string cost. In Lite, clearing the box on an edit now sets the cost to 0.
- **The fleet Query Store tag read takes its hourly rows with fewer rows read** ([#7240]) - the hourly part of the read now takes one range of hours for each run of hours served from the hourly table, so the days the day table answers are not scanned. It removes the hours served elsewhere and sums by hash before the tag join. The figures are unchanged.
- **get_analysis_findings, get_analysis_facts and get_alert_history check the right table on a PostgreSQL target** ([#7194], [#7309]) - a quiet PostgreSQL window was reported as "window_truncated" with a note that collection of wait_stats may have stopped, a SQL Server table that engine never writes. get_analysis_findings and get_analysis_facts now check pg_database_stats on a PostgreSQL target, as analyze_server does, and keep wait_stats for SQL Server. get_alert_history, which checks the whole fleet, now reads the earliest of wait_stats and pg_database_stats, so a fleet of only PostgreSQL targets no longer gets the false hint, and a fleet with no collection at all still gets it.
- **The default PostgreSQL wait event no longer follows lifetime totals across a target restart** ([#7205]) - When a trend window held a PostgreSQL restart and no wait event was named, the default event was the one that had run longest before the restart. It is now the event with the most samples gained inside the window, counting the counter from zero after the restart.
- **A refused press on the Proposals page shows the service's reason, and the press buttons lock while a press runs** ([#7207]) - A press the service refused up front (for example a person disabled after the page loaded) said "The press did not answer." instead of the reason. It now shows the reason. The press buttons are also disabled until the reply arrives, so a double click no longer sends two presses.
- **A slow schema check no longer resets notification route settings when you save** ([#7208]) - When the Viewer's store version check timed out or hit a network blip, the Notification Routes list showed every route with default severities, report notice and approval settings, and saving a route wrote those defaults over the stored values. The list now reads the real columns in that case.
- **An approved agent action that fails in the store before it is sent is now journaled as failed** ([#7206]) - When the store failed between a confirmed approval and the write, the agent-changes list showed "result unknown" and the proposal later turned unknown although nothing was sent. It now records the action as failed and ends the proposal failed.
- **`get_compiles_after_evict` no longer says a cheap statement may never appear** ([#7315]) - The collector reads the most recently run statements of the last ten minutes and does not rank them by cost. The tool now says a statement with no row has not run since the eviction or is outside the newest statements the collector reads.
- **The People screens no longer say grants do nothing** ([#7170]) - The Viewer's People window, empty-list note and Settings line, and the web People page, said nothing reads a grant. An approve grant now decides who can approve an agent-proposed action, so the text says that and says the other grants are not read yet.
- **A failed plan save in the Lite and Viewer history windows now says so** ([#7228]) - Saving a plan from the Procedure, Query Stats and Query Store history windows (Lite and Viewer) showed the general download or read error when the write itself failed. All three now share the common save routine. It shows "Failed to save plan" with the reason and cleans the suggested file name of characters a file name cannot hold. Lite's Query Plan and Procedure Plan downloads on the server tab marked the button "Saved" even after a cancelled dialog or a failed write. They now do that only after a real write. Closing Lite's Settings window no longer applies the theme revert twice, and four unused status labels were removed from the Settings window.
- **Underscores inside words now show in notebook cells** ([#7172]) - The notebook's markdown renderer treated an underscore in the middle of a word as italics, so the Story line of an analysis finding showed `SOSSCHEDULERYIELD` instead of `SOS_SCHEDULER_YIELD`, and names like `sys_dm_exec_requests` lost their underscores. Underscores inside a word now stay as typed.
- **A signed-in admin on the bundled store is no longer told to sign in on the proposals page** ([#7171]) - With `web.allowAdminWrites` off, the page told an already signed-in admin to "sign in" to press a proposal. It now says the dashboard is read-only on the bundled store and names `web.allowAdminWrites`, the same sentence the press itself gives.
- **Double-clicking a column header or scrollbar in a Lite or Viewer grid no longer opens a drill window or editor** ([#7210]) - With a row selected, double-clicking a column divider or the scrollbar track on the query history, block chain, deadlock, collection health and FinOps grids, or on the manage servers, mute rules, notification routes, credential profiles, known patterns and alert rules lists, opened the selected row's window or editor. The window now opens only when a row is double-clicked.
- **Renaming a server now updates its open tab in Lite and the Viewer** ([#7212]) - After a server was renamed in the edit dialog (or its read-only setting changed in Lite), its open tab kept the old name in the tab header until the tab was reopened. In the Viewer it also stayed in export file names and window titles. The tab now takes the new name as soon as the server list reloads. A removed server's tab still stays open.
- **Availability Groups cards in Lite now draw their border** ([#7218]) - The card outline used a theme color that no Lite theme defined, so it never showed. It is now defined in the Dark, Light and Cool Breeze themes. Two FinOps grids also lose a dead stripe setting that pointed at a color that does not exist.
- **A PostgreSQL fleet card now sends its threads waiting counts as null** ([#7195]) - `get_fleet_overview` documents the whole threads block as null for a PostgreSQL target, but `threads_waiting_for_cpu` and `requests_waiting_for_threads` came back as 0, which reads as "nothing waiting". They are now null whenever the server has no scheduler snapshot.
- **A new PostgreSQL deadlock pair counted before a service restart now still pages** ([#7295]) - The set of first-seen deadlock pairs waiting for their page lived only in memory, so a restart between the count and the page left the pair known to the store and the alert was held in the digest instead of paging. The pairs are now saved when counted, restored at start, and cleared once a page is delivered or when the pair's reports leave the hour, so the pair pages once after a restart and not again, a pair that aged out is not brought back by a restart, and that page opens its investigation request. A failed read of the saved count watermark on the first pass after a restart is no longer taken for an empty store: the PostgreSQL blocking and deadlock passes and the SQL Server blocking and deadlock checks skip that pass and read again, so counts a page already carried are not paged a second time. The same holds for the saved failed-job watermark and the saved CPU, tempdb and blocking wait persistence records: an unreadable store no longer looks like an empty one, so failed jobs a page already carried are not paged again after a restart and an open incident is not forgotten and announced again. The affected check skips that pass and reads again on the next. If the store stays unreadable, the third failed read in a row seeds that check as empty and logs one warning, so an unreadable state table never silences an alert for good and the cost is at most one repeat page.
- **Alert History Server filter no longer shrinks or goes blank** ([#7211]) - Picking a server narrowed the list to that server, and a time range with no alerts for it blanked the filter while the grid and Dismiss All covered every server. The list now only grows and the picked server stays picked, in the Viewer and in Lite.
- **Plan error messages name the right source** ([#7214]) - Viewer plan-read errors now say the plan could not be read from the store instead of from the server. Lite's Download Plan errors no longer claim the plan was read from the collected data.
- **People window counts and dates sort correctly, and its times follow the display mode** ([#7213]) - The Identities and Grants counts sorted as text ("10" before "2") and the Added column sorted by its text instead of its date. These now sort by number and date, and the times in the People, Agent changes, Server actions, Investigations and Proposals grids now follow the Server / Local / UTC display mode with a zone label instead of always showing unlabelled local time.
- **Lite's newest-value reads no longer scan the whole archive** ([#7216]) - The FinOps PVS grid and chart, the PVS-pressure, tempdb, low-disk and anomalous-job alert checks, and the newest-snapshot reads behind the running jobs, tempdb card, FinOps database sizes, server info sizes and sessions, file I/O, perfmon, memory clerks, automatic tuning, Index Usage grid and match count, memory, server properties and engine edition lookups looked up the newest sample through the archive view, so their cost grew with the archive. They now read the live table first and use the archive only for a server that has stopped being collected.
- **Lite no longer opens every archive file for short-window reads** ([#7256]) - The Queries tab top-N read, the data-start floor, the per-server watermark reads, the procedure stats and Query Store top-N reads, the Blocking tab's block chain view, and the analysis drill-down and fact reads opened every Parquet file of a table even when the window sat in the last 7 days of hot data. They now read the hot table alone in that case, and the watermarks read the archive once per table instead of once per server. With a 287-file archive the top-N read dropped from 118 ms to 51 ms and the floor from 47 ms to 2 ms on a fast local disk, and the watermark refresh after each archive pass dropped from 346 ms to 108 ms.
- **The Viewer's Added column and the agent alert, database protection and identity capacity times now follow the Server / Local / UTC switch** ([#7343]) - These four times showed this machine's local time with no zone label in every mode. The Manage Servers Added column now reads on each row's own clock like the Last Collected column (that server's time in Server mode, UTC in UTC mode, this machine's time in Local mode), and the capture time on the Agent Alerts and Database Protection tabs and the last-scanned times on Identity Capacity read in the picked server's own zone.
- **The statement filter's judging budget now counts XML parse time on its own clock** ([#7348]) - A budget measured its XML parse time from the real clock even when it was given another clock, which made one oversized-plan test fail on a slow test host. Production behavior is unchanged because production uses the real clock for both.
- **The Viewer's Overview keeps its Search box when a search matches no server** ([#7354]) - A search term that matched no server hid the whole Fleet Health block, and the Search box is inside that block. You could not clear the term, and the area stayed empty until the Viewer was restarted. The block now shows whenever any server is registered, and the all-healthy line needs at least one matched server, so it no longer reads "All 0 servers healthy". A fleet with nothing registered still hides the block, as before.
- **A hand-made store folder the 3.12 service refuses now gets repair steps that finish the job, and the upgrade script takes it over in place** ([#7267]) - A store folder made by hand (on another drive, under the administrator's own account) ran fine under 3.10, but the 3.12 service refuses to start its database on it. `upgrade-darling.ps1` saw the same problem but only printed a warning, then started the service, which showed Running with no postgres process. The commands the refusal printed did not finish the job. Every refusal and `docs/darling/install.md` now give one sequence of four commands in order. They set the owner on the folder and everything in it, reset the folder itself, protect it and grant SYSTEM, Administrators and the service account, then reset everything below it to inherit. Every printed copy starts with a sentence about links, because `icacls` follows a junction or symbolic link into whatever it points at. It says to list the links with `dir /s /a:l "<store>"`, remove each one with `rmdir`, run the four commands, then make the links again. When the data directory has `PG_VERSION` and every untrusted owner is the account running the script or a direct member of local Administrators, `upgrade-darling.ps1` now runs those steps itself after the copy and before the service starts, prints how many items each step changed, and judges the store again. Any link below the store folder is refused before anything is stopped, and the refusal names the link.
- **The fleet 24 hour tag read no longer fails under the 1 GB temp file limit** ([#7240]) - The Query Store statement behind `get_resource_use_by_tag` grouped the hourly table by query hash before joining tags, which wrote 1.34 GB of temp files on a large store and failed for the `mcp` and `viewer` roles. Each part of the statement now joins the tag map and groups by server and tag, so it sends out one row per server and tag and writes no temp files. The numbers returned are the same.
- **The Query Store size guard no longer refuses the fleet 24 hour Custom Views panel for half of each hour on a large fleet, and the service analyzes the current day's Query Store partition every hour without cancelling an autovacuum** ([#7353]) - On a large fleet the guard counted the newest raw hours at the cold read cost and refused the panel for about 34 minutes of every hour, although it ran in 10 to 13 s. A panel that reads the fleet rollup now counts those hours at half the cold weight (0.5, from a cold measurement of 2.91 us per row against the guard's 6.53), but only while the current day partition of the wide table was analyzed within the last 2 hours. Without statistics the same read is about four times slower, so the guard counts those hours at the full weight, as before. A fleet that is too big for the guard is still refused. The raw hours were slow because a new day's partition of the Query Store interval tables could go a whole day with no planner statistics when autovacuum was busy, so the planner chose the wrong index for them. The service now runs ANALYZE on the current day partition of both interval tables once an hour, and again a minute after 00:00 UTC. It waits for the lock at most half of `deadlock_timeout` and skips a partition that a vacuum or an analyze is working on, so it never makes PostgreSQL cancel an autovacuum. The once-a-day ANALYZE of the partitioned table waits by the same rule instead of a fixed 5 s. A store whose service role may not ANALYZE them logs one warning and carries on, and its panels are counted at the full weight. Panels that do not read the fleet rollup are unchanged.
- **The Darling service no longer rewrites the permissions of a whole drive when the data directory sits directly under a drive root** ([#7363]) - A data directory such as `D:\pg` made the drive root the store folder, and every service start locked it, so Users and Authenticated Users lost the access they had through the root for everything on that drive. The service now never changes a drive root. A new store there is refused before initdb with the steps to use a subfolder such as `D:\Darling\pg`, an existing store keeps running with an error logged on each start, and `--harden-files` leaves the drive root alone. Because a drive root is no longer locked, two things the service creates beside the data directory now get their own protected access instead of the folder's: the new cluster of a major upgrade is restricted right after initdb and before pg_upgrade copies the store into it, on every store, and an upgrade that cannot restrict it stops before pg_upgrade and leaves the store on its current major, and the copy of the store credential made when a half-made first-run data directory is moved aside is created with a protected ACL.
- **The upgrade script no longer says the service changes the permissions of a drive root** ([#7363]) - When the store folder was a drive root, the upgrade's warning said the service locks it at every start and changes the permissions of the whole drive. The service does not do that. The warning now says the service leaves the drive root alone, logs an error at every start and keeps running, and gives the same steps to move the store into its own folder.
- **An operator's own `random_page_cost` above the include in `postgresql.conf` is no longer overridden** ([#7370]) - On a managed store that migrated before `random_page_cost` became managed, the operator's line above the include lost to the managed 1.1 with no warning. The service now leaves the key out of `darling-managed.conf` when such a line exists and logs the key, value and line at every start.
- **Lite no longer asks you to look at the Always On collectors on a server without Always On** ([#7375]) - Lite's `get_collection_health` listed `ag_database_replica_states` and `ag_replica_states` as HEALTHY rows that need a look when Always On was off, while Darling listed them as not collected. Lite now reads the newest server properties row, lists a healthy Always On collector as `not_collected` with the same sentence Darling uses, and leaves it out of the compact and needs-a-look counts in `collector_detail_note`. A failing Always On collector keeps its row. Darling's `collector_detail_note` also stops counting those rows in its total.
- **A PostgreSQL server with no SQL Server data reads not_collected in the roll-up by query tag** ([#7374]) - The fleet form of `get_resource_use_by_tag`, the web dashboard that serves it, and the Viewer's "Resource use by query tag" window (set to "All servers") listed a PostgreSQL server as `empty` in five sections and `unavailable` (with the interval table's "no coverage row" text) in Query Store, and named it under "Sections not served". A PostgreSQL server has none of those SQL Server sources, so each of its rows now reads `not_collected` with the engine reason, as the one-server form already did. Every server is read first and only a server with no rows in any section is asked, so a server whose registry row says PostgreSQL but that holds SQL Server rows keeps its data, in the fleet form, in the window's "This server" scope and in its "All servers" scope. In the window, a PostgreSQL server with no rows in "This server" shows the engine reason instead of empty tables. If every server in scope has no rows and runs PostgreSQL, the whole answer is `not_collected`. A cancelled request is no longer turned into an empty answer by the engine check.
- **The first Query Store daily rebuild after the V240 upgrade no longer times out on a store with many days of history** ([#7379]) - the check for which builder wrote a built day read every summary row of every old day, so on a large store the rebuild plan ran past its timeout and built nothing. It now reads one row per day, so the rebuild starts on the first tick and long `get_query_store_top` windows over old days read far less.
- **The collector count no longer says "no permission" for a missing extension** ([#7384]) - The count of collectors that need setup at the monitored server includes both refused grants and missing PostgreSQL extensions, but it was labelled "no permission". The viewer status bar, the Overview card and the web viewer status bar and fleet chip now say "need setup". The Collection Health tab badge still says "no permission" because it counts only refused runs.
- **The query tag roll-up now says a readable secondary's Query Store is not collected** ([#7376]) - On a readable secondary whose Query Store Darling does not collect, the Query Store section of `get_resource_use_by_tag` (one server and fleet), the Viewer's "Resource use by query tag" window (both scopes) and the Query Tag Usage web page read "unavailable" with a "no coverage row" explanation and listed the server under "Sections not served", which said a gap would close when it never does. They now read `not_collected` with the same reason `get_query_store_top` gives, and the server is no longer listed as not served. Its other sections are unchanged, and a secondary that does hold Query Store rows keeps them. The web page also shows only the message, with no table of zeros, for any server or section that reads not_collected, which covers the PostgreSQL rows from #7374.
- **A multi-line statement no longer makes one Long Queries row fill the grid** ([#7382]) - The Statement cell now shows the statement on one line with line breaks and indentation collapsed, and trims with an ellipsis. The tooltip, Copy Cell, Copy Row and exports still carry the full text. The same one-line cell is used in the Default Trace Details column (Lite and Viewer) and the PostgreSQL blocking chains and top queries cells in the Viewer.
- **Grids with a fill column no longer squeeze their other columns** ([#7383]) - On wide grids such as the PostgreSQL Vacuum tab, the fixed columns were squeezed to 20 px, so PID and headers read "Bacl" and "Us", and no horizontal scrollbar appeared. Fixed columns now keep their width, the fill column takes what is left (at least 150 px), and the grid scrolls sideways when the columns do not fit, in the viewer and in Lite. The fill column itself can no longer be dragged to a new width.
- **PostgreSQL session states no longer rate the monitor's own sampler Critical** ([#7381]) - The session states and xmin horizon collectors now skip the service's own connections, and a session is Critical only when it was the oldest holder in at least 2 samples with a peak horizon age above 0. A passing sighting or a horizon age of 0 no longer shows "PINS THE XMIN HORIZON" or a red row, in the MCP read, the desktop viewer and the web viewer, including for rows already stored.
- **Number column headers in web grids line up with their numbers** ([#7392]) - Web viewer grids right-aligned number cells but left their headers on the left, so each number sat far from its own header. Number column headers are now right-aligned over their values.
- **Long web grids scroll inside their own box and keep their header** ([#7393]) - A grid with thousands of rows made the page tens of thousands of pixels tall and its header scrolled away. A grid is now at most 70 percent of the window tall (never under 18rem), scrolls inside itself, keeps its header row visible, and keeps its scroll position across the auto-refresh on every page that draws one (server tabs, saved views, Fleet, Availability Groups, FinOps, Fleet Sweeps and the Proposals list). Printing a page still prints every row of a grid.
- **PostgreSQL log reads no longer scan every time zone file on each read** ([#7397]) - A target whose `log_timezone` is a named zone ran a query on `pg_timezone_names` for every log read, 580 ms on average and 4.5 s at worst on a PostgreSQL 18 target. The check now runs once per server and setting, and later reads set the session zone from the kept name. A POSIX offset string or an unknown name is still refused as before.
- **Charts shown together keep the same plot size, and their layout follows the window** ([#7385]) - Charts side by side had different plot sizes, the legend stacked one line per series and took the plot's height, and several chart blocks had a fixed height. The legend now runs across and wraps only when it does not fit, charts in a block share one plot rectangle (an empty chart keeps its neighbors' frame and has no legend box), and the stacked chart blocks (tempdb, File I/O, memory grants, lock and blocking trends, the blocking, latch and system event blocks) and the PostgreSQL, Queries and FinOps trend charts choose their column count and height from the space available, scrolling below a floor. The PostgreSQL Waits and I/O grids get a usable height.
- **The server page's tiles use the page width and a long tile stops at three lines** ([#7395]) - The tiles above a server's tabs wrapped without limit and stopped at 900 px, so a server with many overdue backups pushed every tab down and left a blank column on a wide screen. The tiles now fill the page width with a column count that follows it, and a detail longer than about three lines stops there and opens on a click, with the full text still on the tooltip. Fleet cards keep their one-line ellipsis.
- **Config audit no longer suggests a negative max server memory on a server with under 4 GB of RAM, and the MAXDOP note names the figure it used** ([#7400]) - The audit reserved the larger of 4 GB and 10% of RAM for the OS with no floor, so a server with 1,023 MB of RAM was told to reduce max server memory to about -3,073 MB. The reserve is now never more than half of RAM (1,023 MB suggests about 511 MB) and the text states the reserve it used. Nothing changes at 8 GB and above in the audit tools, and the max memory advice card can differ by 1 MB. The "MAXDOP is above the guidance" note said logical processors in a NUMA node while the number came from cores per socket (vCores on Azure SQL Database). It now names the real basis. Darling and Lite both changed.
- **A quiet PostgreSQL target no longer gets a Warning for a statement that held almost none of the window** ([#7396]) - The bad-actor share anomaly judged a statement on its peak and mean per-collection share, so one quiet collection could make a statement holding 0.04% of the window look like a bad actor. It now also needs at least the concerning share (25%) of the whole window. The card's routine text also names the same measure as its "times its usual variation" count, so it no longer shows a classical spread beside a robust sigma.
- **Web viewer line charts no longer repeat a color at a sixth series, and the first two colors are easy to tell apart** ([#7401]) - File I/O Latency drew `tempdb ROWS` and `tempdb LOG` in the same blue, and SQL CPU % and System CPU % on the CPU charts were two close blues. Line, bar, pie and composed charts now share one 10-color palette with strongly different neighbors (blue, near-white, magenta, cyan, violet, teal and four more) that stays clear of the ok, warn and err colors. A line chart with more than 10 series draws the repeated colors dashed, in the lines and the legend. Series colors on bar, pie and composed charts also change to the new list.
- **Web viewer layout and wording fixes** ([#7402]) - Banners and the Run live now and Run all collectors now rows have a gap and match each other. FinOps tabs show routine counts and times as quiet captions and keep yellow for real caveats. The Fleet Overview cards start at the left edge. Query Tag Usage sections sort by display name. The access note now says changes from the page are turned off, with the steps to turn them on. A chart whose points have no rate yet says so in a sentence. The Replication and Index Bloat texts name what the page shows, the Optimization cost line starts with a capital, and the Pending Notes Note box matches the other fields.
- **PostgreSQL tabs state what the data proves** ([#7409]) - The Waits and Activity banners no longer say the tab needs Aurora when it fills on other targets, and panels that cannot apply to a target are left off. The octal mode settings show their default in octal. Settings written into the configuration file at their default value are no longer counted as non-default and are listed after the real differences. The I/O Busiest tile shows a dash when nothing was read. Wait series with no samples in the window are left out. Blocking Sampling shows 0 where the capture count proves it, and a dash in a small tile is full size.
- **An agent token can now silence an alert storm without a claim, within limits** ([#7414]) - An agent could not create a mute or digest rule for a fleet-wide alert storm, because the claim path needs a request and refuses all-server rules, the service's own alerts and digest rules. An agent token can now create, change, enable, disable and delete a mute or digest rule over MCP without `agent_action`, on one server or all servers. The rule must name a metric, must have a reason, and must expire in the future, at most 7 days ahead and no later than 7 days after it was created. An agent can shorten a rule but only an operator can extend it, and an agent cannot make a rule, or change one, onto the same server, metric and action again within 14 days (7 days on, 7 off). Text fields are capped at 256 characters and the path allows 20 writes an hour. The repeat schedule and a start time are still refused. Each write adds a row to the agent change journal with the agent, the reason and the rule before and after. This behaviour is on by default for agent tokens. To turn it off, set `mcp.allowAgentMuteRules` to `false` in `darling.json` and restart: those calls then refuse and write nothing, and the claim-based tools are unaffected. A claim-based update can no longer change a digest rule or push an agent-made rule past its 7 days.
- **Storage latency cards no longer fire on a handful of I/Os, and the plan regression card leads with the query that cost the most** ([#7417]) - The read and write latency cards (and their anomaly twins) now need 250 I/Os in the window. The plan regression card ranks by CPU lost, and the force statement lists the same query first. CPU-path regressions that lost under 10 CPU-seconds are left out. The card's severity now comes from the lead row's factor. The `worst_regression_factor` fact field is now `lead_regression_factor`. `forcing_state` replies use `is_empty` and `apc_is_on`, and the MAXDOP text uses the server's scheduler count. Head: 7294557e4
- **PostgreSQL backend counts no longer claim backends where nothing waited, and the trend and checkpoint panels say what they show** ([#7428]) - The Backends figure from pg_wait_sampling is a running total that only rises until a reset, so wait trend points with no samples reported backends and an analysis finding quoted it as backends seen in one collection. A point with no samples now reports none, the column is called Backends Seen and a note says what it counts on each instrument, and the finding no longer quotes it on the extension. Wait Trend, I/O Trend, Query Duration Trend and Database Trend name the event, backend and context, statement or database they follow on the web. The Checkpoints and WAL panel says why a tile is blank, including that PostgreSQL 17 removed Buffers (backend). Lock Activity no longer lists the monitoring service's own catalog locks.
- **The PostgreSQL Vacuum tab no longer says freezing is not keeping up on a healthy server** ([#7432]) - `freezing_is_keeping_up` was true only when the current age was below the window peak, so any database whose age was still climbing, or sat steady, read "no", which looks like autovacuum losing. The age now counts only once it reaches `autovacuum_freeze_max_age`. Below that, `get_pg_wraparound_risk` returns `freezing_is_keeping_up` null and a new `freeze_state` of `freeze_not_due`. At or above it, `freezing_keeping_up` means the age fell in the window and `freeze_due_age_not_falling` is the only problem state. The Per-Database Headroom grid shows the words in a Freezing column in place of the yes or no Keeping Up column.
- **PostgreSQL grids and notes read as plain text** ([#7410]) - By Database, Plan Capture Readiness, Logging Settings Audit and Top Query Shapes no longer show tool paragraphs in cells. By Database shows a verdict word per row with the paragraph on a click. Plan Capture Readiness names its settings in words. Logging Settings Audit fits the page width. Top Query Shapes shows the query text second and drops columns that are all zero. Index Bloat shows one sentence over the grid with the full note on a click. Long captions are one line. Three SQL Server panels (Query Store Overhead, Query Store Memory Clerk, Top Queries) got plain empty text or notes. Extensions, Top Query Shapes, Query Store Clutter and the FinOps memory note show one plain sentence with the tool text behind a click. About 20 panels on the PostgreSQL and SQL Server tabs say one plain sentence for a true empty result. By Table calls its Finding column Bloat Level. Lite's runtime precondition reply now carries the same structured `hints` as Darling's.
- **The Overview tab's baseline band now draws** ([#7433]) - the shaded normal range (mean plus or minus two standard deviations) behind the CPU, wait stats, blocking and I/O strips never appeared in the Darling Viewer or in Lite, because it was placed on the time axis instead of the value axis. Only the dashed mean line showed. The band now draws behind each series and does not change the strip's scale. Its fill is also a little stronger than before.
- **Memory pressure events no longer repeat on a quiet server** ([#7441]) - The collector stored the same memory pressure record again on poll after poll, because the time it computed for the record drifted a few milliseconds between reads. Event lists, memory charts and the daily health count were inflated, and a quiet day could read as Warning. The collector now recognises the record it already stored by its raw ring buffer timestamp, and uses the instance start time to tell a restart from a suspended VM or a clock step. Repeats already in the store age out with retention.
- **An empty top-queries reply no longer says the collector may have stopped when it ran** ([#7442]) - On Darling and Lite, an empty `get_top_queries_by_cpu` or `get_query_store_top` reply for a window the collector ran in but stored nothing could carry a note that the store held no collection and collection may have stopped, beside a sentence saying the collector ran and this was not a collection gap. On Lite this also happened for an `as_of` window that ended before the server's first stored row. Those replies now measure the window from the collector's first run in it: a first run near the start reports the window as covered, and a later first run reports a truncated window that starts at that run. A reply with no run in the window keeps its note.
- **A blocking Warning on a fleet card now says what fired it** ([#7444]) - A server could show `Blocking 1 (0.0 waits/hr)` when the longest single block or the largest snapshot wait had put it at Warning, which read as if nothing was wrong. The fleet card reason, in the web fleet overview, the viewer and `get_fleet_overview`, now names the cause, for example `Blocking 1 (longest block 19.4 s)`, `Blocking 1 (peak snapshot 2.0 min)` or `Blocking 40 (12.5 waits/hr)`. When more than one fired it names the worst. The wording follows the store's configured blocking tiers.
- **PostgreSQL `audit_config` no longer says autovacuum is off when it is on** ([#7447]) - healthy settings now state their value in plain words instead of advice written for a fault, the two sentences of a recommendation are joined with a full stop, and the advice names the real tools `get_pg_top_queries` and `get_pg_autovacuum_health`. The PostgreSQL Configuration tab in the web UI gets a Configuration Audit panel.
- **A small plan cache is no longer called bloated, and the advice puts ad hoc workloads first** ([#7445]) - The plan cache read compared single-use plans to all cached entries, including view parse trees, with no size floor. A tiny idle cache could read MEDIUM and be told to consider Forced Parameterization, a database-wide change. The ratio now counts compiled plans only, single-use plans under 100 MB are never bloat (the text says the cache is too small to judge only when the whole cache is under 100 MB, and otherwise says the single-use plans are under the floor and no action is needed), and above that the advice starts with `optimize for ad hoc workloads` and names Forced Parameterization as the next step. This applies to the web tile, the Darling Viewer, Lite, the `get_plan_cache_bloat` tools and the plan cache bloat recommendation card, which no longer scores thousands of single-use parse trees as bloat. The Viewer and Lite plan cache strips also show the compiled single-use percentage and size that decided the badge, and the web tile gains a `Compiled single-use %` stat.
- **The managed store no longer sends TimescaleDB's daily usage report** ([#7446]) - TimescaleDB defaults to sending a usage report to Timescale's server about once a day, and the product did not say so. The managed store now sets `timescaledb.telemetry_level` to `off`. A store from an earlier build gets the setting at its next start. The extension's `policy_telemetry` job would record a failed run each day with the level off, so the service also unschedules it at each start while the level is off. To turn the report back on, run `ALTER SYSTEM SET timescaledb.telemetry_level = 'basic'` and restart the service. Your value wins and the job is scheduled again. The Docker Compose store sets the level on its command line, so turning it back on there means removing that setting from the compose file. A store you run yourself is not changed.
- **The monitor's own statements no longer rank as real work in Query Store and Top Queries** ([#7443]) - In a database with forced parameterization, SQL Server drops comments from stored statement text, so the monitor's own Query Store statements were not recognized and topped the rankings on a quiet server. Each statement a collector runs in a user database now carries its marker as an alias, which survives. Rows stored before the upgrade age out with retention.
- **"Recurring at this hour" no longer appears on results that fire all the time** ([#7450]) - The label told readers to look for a job in a slot, even for settings like RCSI off and for chains that fired in nearly every hour. Chains rooted in a configuration fact never get it now, and neither does a chain that fired in more than half of the hours between its first and last saved result in the 21 day window. A weekly job on an otherwise healthy server keeps it. Rows saved before this change keep their old sentence.
- **Per-file latency under an I/O finding now adds up to its headline, and memory pressure, range-lock waits and other cards point at next tools** ([#7449]) - The file list under a read or write latency finding averaged each collection interval's ratio, so a single slow write in a quiet minute outweighed a thousand fast writes, and it ordered by read latency even on write findings, which could drop the worst-writing file off the page. It now divides total stall by total operations and orders by the stall of the fact it serves. A write finding lists the log and tempdb files its figure covers. The memory pressure result now states the notification count and peak process and system indicators and names `get_memory_pressure_events`, `get_memory_stats` and `get_memory_clerks`. The nine range-lock wait keys (`LCK_M_RS_*`, `LCK_M_RIn_*`, `LCK_M_RX_*`) and the other root keys that had a card but no next tools now have them, and a plan warning points at `analyze_plan_xml` in both products.
- **The database settings card now shows real RCSI blocking figures, and the RCSI recommendation can appear** ([#7448]) - The RCSI blocking, deadlock and reader-writer figures were fixed constants in Lite and Darling, so the card reported zero blocking and the RCSI recommendation never showed. The figures are now measured per database over the analysis window, tempdb, master, msdb and model are no longer listed, and when RCSI off is the only item the headline no longer says settings drifted and the advice names only the settings that fired. A database needs at least 10 reader or writer waits in the window before it gets a share or the recommendation.
- **Table bloat and index usage no longer read as if PostgreSQL statistics were never lost after crash recovery** ([#7456]) - After a crash restart `get_pg_table_bloat` published an estimate for tables whose last-analyze record was gone, and `get_pg_index_usage` said statistics had never been reset beside zero scans. The estimate is now withheld with its own reason until the table is analyzed, the index note says crash recovery discards statistics without recording a reset, and a scan counter that falls between samples marks the window as reset and keeps the index out of the unscanned list.
- **compare_analysis no longer reports worse and new issues against a window nobody collected** ([#7458]) - When the baseline or comparison window had no collected time, the tool still banded every fact against it and reported regressions and new issues that nothing measured, and described the window as "only partly collected". It now returns `comparable: false`, names the uncollected window first in the caveat, gives every row `status: not_compared` with its values kept, and counts worse, better, new_issues and resolved_issues as 0. A partly collected side compares as before, and the closing advice now lists new_issues. Lite and Darling.
- **A quiet PostgreSQL server is no longer said to lack WAL figures because it is Aurora** ([#7457]) - `analyze_server` printed "pg_stat_wal is not implemented on Aurora" for a stock server whose WAL counters read zero, because skipped idle checkpoints made it look like a broken counter. It now says no WAL was written in the window, says the counters are not being populated only when the columns are zero and checkpoints wrote buffers, says the cause could not be determined when the columns are NULL and the server version is unreadable, says the view is missing below PostgreSQL 14, and names Aurora only for an Aurora target.
- **Empty MCP replies now mean somebody looked** ([#7459]) - `get_running_jobs` and `get_trace_flags` answered `empty` while hinting the collector might not have run. They now answer `empty` only after a good collector run (naming its time) and `unavailable` when no run is on record. `get_pg_io_stats` needs two collector runs in the window before it says `empty`, the Optimization `idle_databases` section answers `unavailable` without 7 days of query stats coverage, and `get_query_heatmap` answers `unavailable` when nothing was collected in the window.
- **Memory conditions show gigabytes with 2 decimals** ([#7465]) - A server low on memory showed 0 for available physical memory, working set and committed memory because the values were cut to whole gigabytes. The desktop grids, web panel and MCP tools now show two decimals, so 900 MB reads 0.88 GB.
- **The memory broker health read now returns SHRINK events** ([#7466]) - `get_health_parser_memory_broker` kept only a resource monitor value the broker never carries, so it always answered empty. It now keeps SHRINK notifications and reports a count of the window by notification value (STABLE, GROW, SHRINK). The Viewer and Lite Memory Broker tabs use the same rule.
- **get_finops rounds its numbers and get_finops_inventory trims the OS version** ([#7467]) - get_finops printed store numbers with twenty decimal places and a zero as 0.00000000000000000000, and one server's host OS version ended in a line break. Percentages now round to 0.1, ratios to 3 places and megabytes to 2 (a value under 0.01 MB keeps two significant digits), a zero prints as 0, and the OS version is trimmed where it is read.
- **Grid cells that open a detail line up with the cells beside them, and the opened text stays readable in a right-hand column** ([#7482]) - a cell holding an expandable detail sat about 4 px lower than the rest of its row because a panel rule's top margin also applied inside the grid. The cell has no top margin now. Opening a detail in a right-hand column scrolls the grid just far enough to show the text, and the opened text is never wider than the window.
- **A configuration change that only adds or removes white space now says so** ([#7483]) - the From and To cells of a change such as `log_line_prefix` gaining a trailing space already showed the white space as marks. They now also end with "(white space only)", on the web Configuration Changes grids for PostgreSQL and SQL Server and on the desktop and Lite grids, so the row no longer looks like a bug.
- **Overview tiles: a long wait type wraps inside its tile and Total wait names its unit** ([#7485]) - On the web viewer's SQL Server Overview, a wait type such as `SOS_SCHEDULER_YIELD` ran past the edge of the Daily Summary "Top wait" tile, so tile values now wrap inside the tile. The Daily Summary tile now reads "Total wait (s)", as the table form does.
- **Analysis findings show plain category names** ([#7486]) - The web viewer's Analysis Findings grid, the Recommendations cards, the analysis alert notebook and the tray balloon title showed an internal word such as `bad_actor` or `database_config`. They now show names such as "Expensive query" and "Database settings". A category with no name shows as it is. `get_analysis_findings` adds a `category_name` field. Stored rows, the `category` field, mute and route matching and the grid's CSV export keep the raw value.
- **Lite's `get_procedure_duration_trend` answers `empty` when no procedure ran in the window** ([#7498]) - An unfiltered read returned a 0 point for each successful collection run that stored no procedure row, so an idle window read as a row of zeros. It now answers `empty` when the window holds no stored procedure row, as Darling's read does. A read filtered to one database already did.
- **`get_active_queries` flags only the top of a blocking chain as `is_head_blocker`** ([#7502]) - A session in the middle of a chain carried the flag beside the session blocking it. `is_head_blocker: true` now goes only to a session with no `blocking_session_id` of its own, in Darling and Lite. Every session the capture names as a blocker stays on the page, and `blocking_only` returns the same rows. A deadlock cycle has no session outside the cycle, so no row in it carries the flag. Both tool guides say what the flag means.
- **`get_daily_summary_range` with `as_of` rates its last day correctly** ([#7504]) - A bare date returned no blocking rate for that day, which could lower its health band, and a mid-day stamp overstated the rate. The day is now rated over its whole UTC date, or up to now if that date is still running.
- **The Blocking and Deadlocks tiles now say which window they cover** ([#7507]) - The tiles on a server page and on the Fleet Overview cards count the reply's one-hour window, but the page's Range control reads "Past day" beside them, so the counts looked like the day's. Each tile now begins its detail line with the window (for example "past hour"), including a tile with no events.
- **Blocking rows now show the head blocker's open transaction count from the blocked process report** ([#7497]) - The Head open transactions column and `blocking_open_transaction_count` were filled only when a DMV snapshot row was folded in, so most report rows left it empty, including rows whose head blocker was sleeping with an open transaction. The count is now read from the report's own `trancount` for the blocking process and stored in a new column (Darling store version 241, Lite schema version 79). Rows stored before the upgrade stay empty, and a folded DMV value fills it only when the report gave none. A folded DMV row now also supplies the head blocker's last transaction start. The column hint and tooltips now say which fields a report row can have.
- **Web chart axes in GB now land on round numbers** ([#7488]) - A size chart such as Memory Trend labelled its axis 19.5, 39.1 and 58.6 GB because it picked round MB ticks and then printed them in GB. The axis now picks its ticks in the unit its labels use, so it reads 20, 40 and 60 GB. This covers the left and overlay axes of line charts and composed panels. An axis whose largest value stays under 1,024 MB is unchanged.
- **MCP replies now say that a stamp with no offset is UTC** ([#7499]) - The server instructions of Darling and Lite now state that a stamp with no offset is UTC unless the tool's note says the field is on the monitored server's clock. `get_blocked_process_xml` now says the stamps inside its report XML are on the monitored server's clock, and Lite `get_cpu_utilization` says its `sample_time` is on that clock. The Lite instruction line about `as_of` is shorter to fit the size budget. No stamp format changed.
- **The Overview "Queries stored" tile no longer reads low for today** ([#7505]) - The month read counted today from the hourly rollup even while that rollup was up to two hours behind, so after a burst of new queries the tile could show 1 where the three-day read showed 2,445. A day the rollup holds only part of now takes the larger of the rollup's count and the raw count, so today is current. A closed day can still read a few hashes lower from the rollup than from raw, because the rollup leaves restart rows out.
- **Query Store Duration Trend no longer breaks its hourly part into lone dots** ([#7508]) - On the web Queries tab and in the desktop viewer, a trend that joins hourly points to per-interval points drew the hourly part as separate dots with no line once the interval points outnumbered them. Each part is now judged against its own spacing, so the line stays whole and a real outage on either side still breaks it.
- **Triage and notebook pages use a wide screen, Query Tag Usage fits a narrow one** ([#7479]) - On a 2560 px screen the triage page and custom notebooks used under half the width because the document was capped at 1100 px, which is now removed (prose keeps its own width). Query Tag Usage tables ran past the page edge at 600 px and now scroll inside their panel. A fleet card's long server name now shows in full on hover. The web smoke test also measures layout at three widths.
- **Charts keep a usable height on small windows** ([#7478]) - On a small window (for example 800x600) the Viewer's CPU Scheduler, Memory Overview, Plan Cache and Session Stats charts, and Lite's five Overview timeline strips, were squeezed until their plot area was too short to read. Each now keeps a minimum height and the tab scrolls when the window is too small. Larger windows look the same as before.
- **Lite plan tool text now says plans are never kept** ([#7522]) - The Lite descriptions of `analyze_query_plan` and `analyze_procedure_plan` claimed a miss could be `unavailable`, and `get_top_queries_by_cpu` sent readers to `analyze_query_plan` to confirm parallelism. Lite keeps no plans, so that call always answered `not_collected`. The heads now say so, and the parallelism pointer names `analyze_query_store_plan`.
- **`get_memory_pressure_events` no longer answers with half a million characters** ([#7516]) - On a server under memory pressure the default 24-hour read returned every sample, over 3,700 rows. Darling and Lite now return the newest 200 by default (new `limit` parameter, 1 to 1000), still oldest first, and every reply carries a summary of the whole window: the total, the count per notification, and the samples with an indicator at 2 or more, plus a note when rows were left out. The web Memory tab still reads every sample for its chart.
- **`get_pg_session_states` answers null instead of -1 for durations it did not measure** ([#7528]) - The four duration fields (`peak_xact_duration_ms`, `peak_state_duration_ms`, `peak_query_duration_ms`, `backend_age_ms`) showed a raw -1 for sessions with no measurement, such as an autovacuum worker in state `starting`, which could be read as a real duration. They now answer null, the same as the replication slot sizes. `peak_horizon_age` keeps its documented -1.
- **Lite's `list_servers` answers JSON like Darling's** ([#7527]) - Lite answered plain text for a non-empty server list, so a client that parses the reply failed on Lite while every other read, and Darling's `list_servers`, answered JSON. It now answers `server_count` and a `servers` array with `server_name`, `display_name`, `status`, `read_only` and `last_collection` as a UTC stamp, plus `last_collection_note` (`none retained` or `No data collected`) when there is no stamp. The empty reply is unchanged.
- **Lite shows the right last collection time for a server that has CPU data but no collection log row** ([#7527]) - the Overview card's Last Collect time, its freshness band, and `list_servers` `last_collection` took the monitored server's local clock and labelled it UTC, so a server east of UTC looked fresher than it was and one west of it looked staler. They now use the store's UTC collection time.
- **Analysis no longer shows two different counts as the same "blocking events"** ([#7537]) - The blocking spike card counts blocked process report rows while the blocking events card counts waits, so the same window showed two numbers under one name. The spike card now says "blocked process reports" and explains the unit, and the events card says each event is one wait. Same in Lite and Darling.
- **The Blocked Process Reports panel on the web shows every report the read selected, and says the true reason when some have no XML** ([#7545]) - A reply holds about 32 KB, so a page of 5 large reports could show 1 row, with a note that blamed the row limit. The reports left out for size now appear as rows without XML, a Load XML button on each reads that report alone and never attaches another report's XML, and the note says the reply size is the reason. `get_blocked_process_xml` now also sends `more_reports_than_limit`, so a page of exactly the limit is no longer described as stopping at its row limit.
- **PostgreSQL Bloat Estimates says "No estimate published" instead of 0 B, and counts the whole window** ([#7551]) - The Bloat Estimates tile showed 0 B when no estimate had been published, and its published and suppressed counts covered only the 25-row page. The counts and total now cover every table in the window, get_pg_table_bloat returns null for the total when nothing was published, the "window held more" notes on Bloat Estimates and Index Usage give the row counts, and the Index Bloat grids show each row's estimated percentage and basis.
- **The Alert Details link now switches to the server tab** ([#7554]) - Clicking "Open the Blocking view at this time" (or any other link in the Alert Details window) opened the server tab but left you on Alert History, because the Alert History grid took the selection back when the window closed. The link now runs after the window has closed, in both the Darling Viewer and Lite.
- **Dropped databases no longer count in Identity Capacity or show as "Scanned, clean"** ([#7547]) - A dropped database kept its last identity scan for 7 days, so the Identity Scan State panel listed it, `get_identity_capacity` counted it in `databases_scanned`, and a full column on it kept the Identity capacity tile, the Fleet card and the alert critical. The reader now leaves out a database the server's newest database snapshot no longer names, so the web panel, the desktop Viewer, the MCP tool, the Fleet tile and the alert agree. Offline and skipped databases keep their rows, and asking for a named database still returns its history.
- **PostgreSQL Analysis no longer raises standing Warnings on the monitoring service's own statements** ([#7567]) - On a quiet server the wait sampler's `pg_sleep` and the service's other reads led Top Query Shapes and drove Warnings that never cleared. The service now ends each statement it sends with a marker comment, Analysis leaves marked statements out of its candidates, shares and server-wide mean, Top Query Shapes on the web and in the Viewer keeps them with a mark, Change Watch no longer gives them a slot, and the trend charts no longer choose them automatically. The marker also goes on the log timezone batch, the capability probes, the search_path check and the endpoint login probe, and the `get_pg_top_queries` description names the `service_statement` field. The match is per login and database, so an application statement of the same shape under another login stays in. The headline no longer sets a window share beside a deviation measured on a per-collection share, and prints `at least 25` at the cap.
- **A blocking spike on a server with no blocked process reports names its unit correctly** ([#7537]) - On RDS and Azure the blocking spike counts DMV blocking snapshots, but its headline said "blocked process reports" and its text said the baseline was in report rows too. The detector now marks that case and the text says "blocking snapshot rows", and that the baseline is still built from report rows.
- **Active Queries leaves out the monitor's own statements by default** ([#7573]) - On a quiet server Active Queries, its strip and `get_active_queries` listed only the monitor's own sessions. They now have a `Monitor statements` switch, left out by default, on the web page, the Viewer and Lite, and an `include_monitor` option on `get_active_queries`. The Waiting Tasks table on the web Wait Stats tab and `get_waiting_tasks` (Darling and Lite) get the same switch, backed by a new `program_name` column on the stored waiting tasks (rows stored before it are never hidden). The rows are stored either way. A monitor session that heads a blocking chain stays on the default `get_active_queries` page so its victim can name it, and the live reads (web Run live now, the Viewer's Current Active Queries and Lite's Live Snapshot) follow the same switch.
- **The Blocked Sessions chart no longer counts a parallel query's own threads as blocked** ([#7577]) - The chart counted every waiting task with a blocker, and a parallel query's exchange threads wait on their own session, so a quiet server drew a count next to text that said nothing was blocked. It now counts distinct sessions blocked by another session, in the Viewer, in `get_current_waits_trend` and in Lite.
- **Identity Capacity keeps a new database listed until the next database snapshot names it** ([#7547]) - The dropped-database filter from #7580 hid a database that was created after the last database snapshot, so a new database dropped out of the Identity Scan State list as Not yet scanned or Skipped. The reader now keeps a database whose config capture is newer than the snapshot. A failed lookup in `get_identity_capacity` is now logged.
- **Query Store Health on a readable secondary now says why it is empty in Lite and Darling** ([#7584]) - In Lite and Darling, the Configuration tab and `get_query_store_health` told you the server predates Query Store or had not finished a cycle. The collector skips a readable secondary by design, so the read now answers `not_collected` with the same readable-secondary text, naming `query_store_replica`, that the other Query Store panels give.
- **A server the service cannot connect to now reads Offline at once and says why, including one that has never connected, and stops reading Offline once collection resumes** ([#7590]) - the Fleet Overview card, Needs attention line, server page header, Viewer Fleet page, `get_fleet_overview`, `list_servers`, the Manage Servers grid and `get_collection_health` kept showing `collection stale` and green tiles for up to 30 minutes after a failed connect. The service now stores when the connect failed and the last error (store rung 244, with the next retry time), the server reads Offline with `Server unreachable since <time>` and the error, its tiles are marked as last known values with their age, and the Collectors tile says it is waiting on the connection. Lite's sidebar tooltip, Overview card tooltip and `list_servers` now also say since when and the last connect error for a server whose connection check fails. Collection Health names the next retry time, and Run all collectors now is turned off with the offline tip.
- **Analysis now scores a nearly full volume Critical** ([#7583]) - A volume under 3 percent or 2 GB free scored the same Warning as one at 4.9% free, while the low disk alert called it Critical. The Analysis row now reads Critical under the alert's default critical cut, in both Darling and Lite, and still reads Warning from there up to 5% free.
- **PostgreSQL Statement Calls per Second no longer reads high for a statement that runs in bursts** ([#7566]) - the rate divided by only the collections where the statement moved, so a batch of 29 calls every 5 minutes charted near 0.48 per second instead of 0.097, and a quiet bucket drew a line across instead of zero. The rate now divides by the time the collector's runs covered, a bucket with runs but no calls reads zero, and a bucket with no run stays empty. This changes the web chart, get_pg_query_duration_trend and the Viewer.
- **The Analysis Findings grid and Recommendations tab show one row per sentence** ([#7582]) - The same sentence used to print on two or three rows, often at different severities, and a row could read Info while its peak was Warning. Rows with the same sentence are now merged: worst severity counting the peak, earliest first seen, latest last seen and the occurrences added up. A merged row opens to list the chains it covers in plain words, and the also-seen-as line on a card names its chains in plain words too. A same-statement pileup no longer prints its statement hash as a chain step, and pileups on different statements stay on separate rows with their own advice.
- **Waiting Tasks and Blocked Sessions on the Blocking tab are now stacked bars** ([#7571]) - The two charts drew one line through every row, so two wait types (or databases) at one collection time made the line drop straight down, and a few far-apart collections read as a wait that built for hours. Darling (web, Viewer) and Lite now draw one bar per collection time with a segment per wait type or database, keep bars wide enough to see when a window holds only a few of them, and the web adds a Threads column to the Waiting Tasks grid on the Wait Stats tab so the chart's total can be matched to a row. Pointing at a stacked segment now names that segment.
- **Query Store Regressions no longer marks a query CRITICAL or HIGH when it added little time** ([#7592]) - Darling and Lite graded a cost row on its duration percent alone, so a query that added 1.3 seconds over a day read CRITICAL. A cost row that added under a minute of duration over the window and under a second per run now reads LOW and stays listed, and a row over either floor keeps its percent band. The `get_query_store_regressions` description lists the bands and the floor.
- **A PostgreSQL bad-actor statement's calls per second now matches the trend** ([#7566]) - The `PG_BAD_ACTOR` fact and its advice divided a statement's calls by the seconds of only the snapshots where it had a row, so a statement that ran a batch every 5 minutes read about 0.48 calls per second where the trend showed 0.097. The fact now divides by the seconds the collector's runs covered, and the advice text reads "over the time the collector's runs covered".
- **A server with a full identity column now counts as Critical** ([#7561]) - The Fleet page showed a red Identity capacity CRIT tile on a server it counted as Healthy, with a green dot and `0 Critical` in the top strip. A column at or near its limit now bands the server Critical in the web page, the Viewer and the MCP reply, and shows in `Needs attention`. Servers with only a WARN identity, Agent alerts or Protection tile keep their band, appear in `Needs attention` and sort above servers without one. The daily health report now lists a server with a full identity column as Critical.
- **A PostgreSQL server's Memory and Blocking tiles no longer print OK and 0** ([#7560]) - The Fleet card and server header showed `Memory OK` and `Blocking 0` for PostgreSQL servers although neither is measured there. They now print `n/a` (`--` in the Viewer), and the fleet MCP reply sends null for the four memory-grant fields on PostgreSQL instead of false and zeros.
- **The Fleet page fills its row with server cards at every window width** ([#7559]) - At a 1536px window the cards sat in two columns with about 285px of empty page on the right, because a third column needed another full 460px. The card grid now adds a column whenever 380px fits and shares the rest, so the row is full at 1280, 1536 and 2560px.
- **Byte and MB chart axes tick on round numbers in one unit** ([#7555]) - A chart in bytes such as I/O Read Bytes per Second ticked at 19.5, 39.1 and 58.6 KB under a `bytes/s` title, and Memory Trend mixed `512 MB` with `1.0 GB` on one axis. Byte charts now pick ticks in the unit they print (20, 40, 60 KB), the I/O caption reads `/s`, and every tick on a size axis prints in the unit of its largest tick (0 GB, 0.5 GB, 1.0 GB). Tooltips and grid cells keep their own unit. Custom View MB charts follow the same rule.
- **Long names, narrow wrapping columns, Active Queries width and the Plan buttons are fixed in the web grids** ([#7533]) - A long database, file, path, job or instance name no longer pushes every number off screen: it stays on one line, is cut with an ellipsis at a width that follows the window, and shows the whole name on hover and in Copy cell and the CSV. Wrapping text columns such as Plan Corrections Reason and Blocking Wait Resource keep a minimum width, so a short sentence no longer takes five lines, and the FinOps High Impact Query Preview and PostgreSQL Storage By Index Definition show one line that opens on a click. The Active Queries Query column follows the window width so SPID and Status stay in view at 1280. The Plan, Copy and Download buttons in grids and open plans use the app button style.
- **A series keeps one color across File I/O Latency and across reloads of a Custom View** ([#7535]) - On File I/O Latency the same file was one color in the Read chart and another in the Write chart, because each chart colored its series by its own order. Both charts now take their colors from the two lists together, so a file is one color in both. On a Custom View chart with several series, a series took its color from the order its rows arrived in, so colors could change when the page loaded again. Series are now ordered by total (largest first, then name, with "(other)" last) and colored in that order, which also settles which series stay when several tie at the cap.
- **Quiet charts and range strips now break where nothing was collected** ([#7572]) - The File I/O Latency chart and the Overview I/O strip drew one straight line across hours with no readings, so one slow moment read as latency that climbed all evening. They now break at a step longer than the bucket (or longer than the rows' own step, when a collector is scheduled slower than the bucket) and show a lone reading as a dot. The Overview I/O strip also needs a minimum number of reads behind a bucket before it marks it with a red dot: 250 per 15 minutes of the span a row's reads cover (the bucket width, or the collection step when the collector is slower than the bucket) (the PostgreSQL I/O baseline's bar, not Analysis's window-wide floor). The desktop strips apply no read floor. The range strips above the grids (Query duration, Procedure duration, Query Store duration, Blocked process reports, Deadlocks) break their line and area at a hole instead of drawing a ramp, judged by their own step or the cadence the samples show, and a window with only a null reading draws no strip. Query Duration Trend, Executions per Second and Procedure Duration Trend keep their own-cadence rule, as before.
- **Size cells on the PostgreSQL Storage, I/O and Waits tabs no longer print digits that were never measured, and small sizes no longer read 0 MB** ([#7564]) - An index of 456 KB read 460.8 KB in By Index and 409.6 KB in Index Bloat, and 16 KB indexes read 0 B, because the page turned a rounded MB back into KB. Those cells, the bloat and index tiles, Buffer Pool Residency and the OS CPU by Query device columns now show the exact byte counts from the reply, and the buffer usage tool publishes a new buffer_bytes field for Resident. A size from a megabyte-only source now keeps its megabytes and its own decimals (0.45 MB), and a zero reads 0 MB. SQL Server memory, grant, plan cache, Query Store, tempdb and table size columns that can hold a few MB use the fitting format, so 0.5 MB no longer reads 1 MB.
- **PostgreSQL Index Usage tiles count every index in the window** ([#7575]) - The unscanned, MB held and invalid tiles counted only the indexes listed on the page, so a server with more indexes than the limit undercounted them. They now count every index in the window, using the same rule the page uses, and the Index Usage page loads fast on a server with thousands of indexes: the index read now joins two sets in a form PostgreSQL can merge, where it ran for minutes on 3,000 indexes before.
- **`get_query_trend` reads up to 90 days and gives one point per hour or per collection** ([#7608]) - In Darling, `get_query_trend` was limited to the default window and returned one row per `sql_handle` under the same timestamp, so a query that ran under several handles drew a jagged line. It now reads up to 90 days from the hourly rollup, sums the handles into one point per hour (or per collection on the raw tier), and takes `min_dop`, `max_dop` and `query_plan_hash` on a raw point only from the plans that ran at that time, with a new `plan_count` saying how many. A raw point with no plan hash now shows null instead of an empty string, and a point where nothing ran shows null `min_dop` and `max_dop` instead of a cached plan's lifetime DOP. Lite's `get_query_trend` folds its rows the same way.
- **Name grids open in the same order as a header click** ([#7550]) - Server Configuration, Database Configuration, Database Scoped Configuration, Query Store Health, Identity Scan State, Perfmon Counters and PostgreSQL Settings listed every capitalized name above every lower-case one on open, while a header click sorted without case. The reads now order names without case, then by the name, and Perfmon Counters adds the instance so `default` always comes before `internal`. The same order applies on the web, in the Viewer, in MCP replies and in Lite.
- **Overview I/O latency stops marking buckets with only a few reads as anomalies** ([#7624]) - The I/O strip on the desktop Overview in the Darling Viewer and Lite marked any bucket outside the baseline band at 2 ms or more, even when the bucket held one slow read, so it could show a red dot the web Overview did not. The strip now needs at least 250 reads per 15 minutes of the span the reads were counted over before it marks a point or shows the hover arrow, the same rule as the web strip. The 2 ms floor is unchanged.
- **A per-second perfmon counter in a Custom View or an alert rule now charts as a rate** ([#7565]) - A panel that charted a counter such as Transactions/sec with Perfmon counter value drew SQL Server's running total, a line that only climbs. There is a new Perfmon counter (per second) measure that charts the real rate for one counter, one instance and one server per series, and validate_custom_view and saving a view refuse a rate panel that leaves any of those open. A custom alert rule on that measure must pin counter_name and instance_name with eq filters or it is refused on save, because it would read many times too low. A rule saved earlier keeps evaluating until it is next saved. The Perfmon counter value measure now says it is a running total, and validate_custom_view, the view builder, the MCP create and update replies and the Viewer warn when a panel charts a counter ending in /sec that way.
- **Count charts label whole numbers and the last time label stays inside the chart** ([#7606]) - Blocked Sessions, Deadlocks, Blocking Trends, Memory Grant Activity, Memory Pressure Events, CPU Scheduler tasks, session counts and the System Health event charts in Lite and the Viewer no longer label 0.5 or 1.5 on small counts. Time charts now keep room right of the plot, so a time label on the end of the window is not cut off.
- **Web chart axes label the right gridline, and four Queries and I/O Trends details** ([#7586]) - On a small range the web axis printed a figure on the wrong gridline, so a throughput of 0.006 MB/s carried the label 0.01 MB/s. Axis labels now take their decimals from the tick step, and ticks that would repeat are formatted again with more decimals. The per-query history chart prints durations in one unit taken from its largest tick, as the grid does, with no extra ms caption. On Top Queries the History button now selects the query in the picker and scrolls to its chart instead of opening a second copy in the row. A read-only Query Store database shows its reason in words. The file throughput trend keeps four significant digits for rates, so 0.0042 MB/s no longer reads 0.
- **Web panels with no partner take the whole row** ([#7534]) - On the SQL Server and PostgreSQL tabs and in saved Custom Views, a half-width panel whose neighbour was a full-width panel sat at half width beside a page-wide gap (Database Sizes, Query Store Memory Clerk, Trace Flags, Statement Evictions and others). A half-width panel with no partner on its row now takes the whole row, also when its partner hides itself because it is empty. On very wide windows (about 2150 px of page and up) the panel grid no longer makes a third column, so half-width panels pair up as they do on a smaller screen, and that holds for Custom Views too.
- **Web counter charts with only zeros show one line** ([#7549]) - On the System Events tab, six corruption and contention counters drew a flat line at 0 in a full-size card each on a healthy server, and PostgreSQL Activity did the same for Database Temp Bytes and Database Temp Files and Deadlocks. A chart whose every point is 0 now shows one line of text, worded for that counter, such as No bad pages were recorded in this window, and a counter the server did not report at all keeps its own different sentence. A window that crossed a statistics reset on PostgreSQL says so in that line. One point above 0 in any series keeps the chart.
- **Query Heatmap columns fill the panel** ([#7542]) - The web Query Heatmap drew every column 6 px wide, so on a wide window the grid stopped short of its panel and left empty space on the right. The columns now share the panel's width, with 6 px as the minimum so a narrow window still scrolls inside the panel.
- **Blocking trend charts line up** ([#7543]) - On the web Blocking tab a Source line above the Blocked process report trend pushed its plot below the Deadlock trend plot beside it. The source now sits in the panel's subtitle next to the window label, for the trend and for Blocking Severity, so both plots start at the same place, even when a heading wraps. Panels in a grid that hold a chart now anchor the chart to the bottom of the panel.
- **The PostgreSQL Logging Settings Audit fits more settings on screen and names no tools** ([#7539]) - The Activity tab's Logging Settings Audit gave most of its width to the Change needs column and showed whole paragraphs, so only about two settings fit, and the text told you to call tools you cannot use on the page. Change needs now wraps, Recommended and Unlocks show their first sentence and open the rest on a click, and tool names read as the tab that shows the data. Plan Capture Readiness now labels its log line prefix row in words instead of showing the raw key.
- **The PostgreSQL Configuration Audit's Why column names tabs, not tools** ([#7557]) - On the PostgreSQL Configuration tab, 3 of the 13 audit rows told you to read `get_pg_write_stats` or `get_pg_top_queries`, tools you cannot call from the page. The Why column now names the tab that shows the number, such as the I/O tab or the Activity tab, and two reads on the same tab are named once. SQL Server audit rows are unchanged because their text names no tool. A sentence that starts with a tab name, anywhere the page words a tool, now starts with a capital letter instead of "the Activity tab ...".
- **The blocking spike on servers without blocked process reports now compares like with like in Darling and Lite** ([#7588]) - On RDS, Azure and any window with no blocked process report, the spike counted DMV snapshot rows but judged them against a history of report rows. With no reports the history was empty and a small burst of snapshot rows fired a spike. The window is now judged against the history of snapshot rows in both products, and the text names the source it counted.
- **PostgreSQL panels say one plain line when there is nothing wrong** ([#7548]) - On a healthy PostgreSQL server the Deadlocks, Deadlock Graphs, Column Statistics and Index Bloat panels showed several lines written for an MCP client, with issue numbers and key names. Each now says one short line, names the panel that holds the answer or the grant to run, and keeps the long text behind More detail. The service's own messages and notes no longer show issue references in parentheses.
- **Lite says which Azure SQL Database databases have no deadlock session, and stops retrying a create Azure keeps refusing** ([#7639]) - On an Azure SQL Database server with many databases, Azure refuses new event sessions once the pool reaches its session or memory limit. Lite read the databases that had a session but recorded the run as clean, retried the refused create in every database each cycle, and wrote one Warning per database each cycle. The run now names the databases without a session and why, the refusal is logged once and then at a lower level, and a refused create is held for an hour. When no database could get a session, Lite still reads the server-wide deadlock data before it reports the failure. The blocked process, client timeout and long query completions runs now carry the same note, and the long query trace holds a refused create for an hour too. In Darling the client timeout run now names the databases that refused its session, and says so when the list matches no database, as its deadlock and blocked process runs do. A new per-server list, Databases that get Extended Events sessions, lets you choose which databases get sessions on Azure SQL Database. Empty means every database, as before. Excluded Databases still wins, sessions this install already made in databases left off the list are not dropped (the old ownerless long query session still is), and Darling stores the list in a new store column (rung 247). In Darling the Viewer edits it, and so does the new MCP tool set_server_xe_session_databases, which edit_server now points to. The web admin page does not. A Darling edit reconnects that server on its next config poll, like an Excluded Databases edit, and the support bundle hides the listed names. A list that names no database on the server now says so in the runs' note instead of leaving an empty grid with no reason. In Lite, an Azure deadlock ensure that fails in every database with an error that is not a SQL error now reads the server-wide deadlock data before it reports the failure, as it already did for a SQL error. Saving the Lite Excluded Databases dialog, or the new Databases for XE Sessions dialog, no longer deletes the stored username of an Entra MFA server.
- **The system_health CPU legend no longer lists System CPU when the chart says it is not shown** ([#7593]) - On SQL Server on Linux the note above the chart said System CPU is not shown, but the legend still listed it and the Entries grid kept an all-dash System CPU % column. Both now leave it out whenever the note is shown.
- **Admin and FinOps explanations are quiet text and only real warnings are yellow** ([#7558]) - Every note on the Admin tabs, Proposals, FinOps Server Inventory and Index Analysis, and several server tab panels was drawn as a yellow warning, so the one real warning looked like the rest. Explanations are now plain grey captions and the yellow strip stays for real problems such as an unsigned webhook. "Changes are turned off" is said once under the Admin heading and not on every tab, and the Report Feedback and Practice Scores windows are written in days.
- **Configuration Audit review rows are coloured, ids print without separators and chart ticks drop needless decimals** ([#7552]) - A Configuration Audit row that said review had no colour, so it was the one row that did not stand out. It now has the warning colour on SQL Server and PostgreSQL. Identity Capacity "Values remaining" is aligned like the numbers beside it. Process ids, SPIDs, Query Store query and plan ids and Memory Broker ids print with no thousands separator, so they can be copied into a command. The System Events note is a quiet caption. Round chart ticks (0, 100, 200) no longer carry decimals while a fine step still shows what it needs. A long wait name in the Daily Summary breaks between words. The max server memory audit in Darling and Lite quotes the same physical memory as Server Properties, except on an Azure SQL Database, which keeps its own memory limit. The Automatic Tuning empty note names Plan Corrections. Top Queries by CPU on the Queries tab says the same short sentence as the CPU tab. Memory pressure bars sit at the middle of their hour.
- **The Overview timeline no longer waits for its baseline bands, and the I/O band matches its line** ([#7644]) - On a busy server the 24-hour timeline waited up to 23 seconds for the baseline read before drawing. The strips now draw as soon as their own data arrives and the band and markers are added when the baseline is ready. The baseline read now shares the computed result the background analysis already keeps, and a reload or server switch no longer throws away a baseline that was half computed. The I/O band is now built from the same per-collection read latency the strip draws, so a file with a few slow reads no longer pulls it away from the line. The band follows the server's local hour, one step per hour, and each point is judged against its own hour only. The Viewer's and Lite's timeline strips draw the same stepped band, and Lite's I/O band is built from the same per-collection read latency as its line. The strips are taller (96 px plots instead of 54) so the lines read clearly. The baseline read now refuses a window start in the future or more than 31 days back instead of walking every hour from it.
- **Seven small display details on the Memory, CPU, Wait Stats and File I/O tabs** ([#7578]) - Plan Cache panels now say they show the newest snapshot, not the window. Memory Pressure Events lists the newest row first. Chart axes print a unit once, either on the ticks or in the caption. The Wait Stats limit text is muted, not yellow. File I/O Stats latency headers carry (ms). Runnable % prints 0.0 on the web and 0.00 in the Viewer and Lite instead of N/A when no other request was running, with an explaining tooltip. Resource Semaphore grids print Regular and Small query, and the Resource Semaphore Pressure grid wraps its headers and date-times so it fits a 1280 px window without a sideways scroll. Chart headings on the File I/O and Collection Health trend panels no longer repeat the unit the ticks already show.
- **The collector's delta cache no longer grows without a limit** ([#7649]) - The cache that remembers each statement's last counters kept an entry for every key it ever saw. Since #7238 query_stats reads up to 2,000 recent statements a run, so the Darling service and Lite grew by gigabytes a day. Each group now drops entries older than its one hour gap once per collection pass. After an hour with no row for a statement, the first row that comes back with a fresh compile now gets its full count instead of zero.
- **The Database State alert no longer stores a number taken from the database name** ([#7646]) - A database named like `Sales2024` was stored with a current value of 2024, and the web Alert History showed it as `2,024.0`. The alert now stores 0, and the web, Viewer and Lite history grids show a dash for it. The web CSV export and copy commands write the same dash, and the `get_alert_history` tool in Darling and Lite marks such rows with a new `state_only` field.
- **Config Changes says when a change was first seen, not when it happened** ([#7662]) - The time on the Server and Database Configuration Changes grids is the first capture that holds the new value, and those captures run on connect and once a day. The column now says First seen and shows the span from the capture before it, in the web page, the Viewer and Lite. A database state change takes its time from the once-a-minute state collector, and one seen before the requested window is no longer listed. The latest snapshot headings now say how old the snapshot is.
- **A server with databases down no longer reads Healthy on the Fleet page** ([#7647]) - The Fleet page, `get_fleet_overview`, the Viewer and Fleet Sweeps now read database state through the Database State alert's own read. A database in SUSPECT, RECOVERY_PENDING or EMERGENCY bands the server Critical and the reason names it, and any other deviation such as OFFLINE adds a Database state tile and a reason without changing the band. The read needs two snapshots in the last 2 hours, so a server with fewer shows as not collected on the Fleet page and in `get_fleet_overview` instead of healthy, and the Viewer shows no database state row for it. `get_fleet_overview` and the Fleet page also carry the time of the newest snapshot as `collected_at`.
- **Skipped Cycles no longer stretches its tiles** ([#7660]) - The last-skip paragraph sat in one tile and made all seven tiles 361 px tall. It is now a full-width strip under the tiles on the SQL Server and PostgreSQL tabs, drawn only on a server that skipped cycles, worded in plain words (the previous pass was still running when the next one was due), with the gap in local time, a short line saying why the Read tile says OK, and the Cause tile in the same words as the sentence.
- **Web rate charts draw the points the plot can show** ([#7661]) - The web trend charts asked for up to 1,500 points whatever their width, so a statement that ran every 5 minutes drew as a solid block. A line chart over a rate (statement and procedure duration, waits, lock waits, file I/O, PostgreSQL I/O and database trends, per-second perfmon counters and the rate server trends) now asks for about one point per 2 px of plot, so a narrow window gets wider buckets and a wide one more detail, and a rate stays the true average over its bucket. The CPU, memory, tempdb, memory grant and system_health CPU charts, other level trends, and every table and stat panel keep their old one-minute detail, so a short spike is not averaged down. Brush-zoom and CSV export use the points the chart was drawn with.
- **The service's own PostgreSQL statements are now recognized on servers it monitored before the upgrade** ([#7681]) - an entry that the old, unmarked statement created kept its unmarked text, so Analysis and the top queries counted the service's statements as the application's. The hourly text fetch now also matches the unmarked entries of the service's own login against the statements the service sends, ignoring comments, case and the values of constants but not how many there are, and treats a match as the service's. A string with several statements, such as the Aurora probe, is matched one statement at a time. The service registers its statements at startup and the store keeps a learned entry across a restart, so a restart no longer forgets one. The fetch also no longer moves the raw text of a sensitive statement such as `ALTER ROLE ... PASSWORD` as a candidate.
- **A server that cannot report host CPU no longer shows it as 0 or 100% idle** ([#7682]) - SQL Server on Linux before 2025 CU1 cannot report host CPU. The CPU Utilization read, the CPU tab and the Overview CPU strip showed other-process and total CPU as 0 and idle as 100. They now return and draw only the SQL CPU, with a note that the host figure is not available, and the Fleet Overview card labels the value SQL CPU. The Darling alert detail card also shows the SQL CPU and no invented Total CPU line.
- **Scheduler Issues no longer lists every minute as 100% other-process CPU on SQL Server on Linux** ([#7688]) - On Linux the host's idle CPU reads 0 on some builds, so every scheduler sample looked like other processes used all the CPU and filled the grid. The reads now leave Other-Process CPU and Idle blank on Linux, only list samples that are high on SQL CPU or low on memory, and say why in a note that names the two blank columns. Windows servers are unchanged.
- **Fleet Sweeps now band a server Critical when a column is at its identity limit** ([#7669]) - Fleet Overview showed such a server as Critical while every sweep showed it as Warning or Healthy, and `critical_servers` in `get_sweep_reports` stayed empty. A sweep now reads the same state Fleet Overview does and is never better than it for a column at its limit, in the band, the changes and `critical_servers`. The Fleet Sweeps page now says that a band covers what happened in the sweep's span and Fleet Overview shows the state now.
- **Alert Settings, Pending Notes, Practice Scores and the Close request form no longer offer changes the service refuses** ([#7700]) - With changes from the web turned off, these pages still drew their forms and buttons, and a person only learned changes were off after pressing one. They now list their rows with a note that names the web.allowAdminWrites switch, as the other Admin pages do. The note at the top of Alert Settings now names both editable forms and says the page is read only when changes are off, and the Daily Health Report Preview button stays available.
- **FinOps High Impact names the database that did the most work** ([#7695]) - A query that ran in several databases was named for the alphabetically first one, which could be a database that barely ran it. The Database column now names the database with the most CPU and shows `name + N more` when the figures cover other databases. The `get_finops` high_impact reply carries `database_count`, and the Get Actual Plan confirmation says when the query also ran elsewhere.
- **The PostgreSQL Fleet card, server header and Viewer now show Blocking** ([#7616]) - They printed n/a for a PostgreSQL server although its blocking is sampled every minute. They now show how many of the past hour's blocking captures saw blocking, banded on the share of captures actually taken (scaled to one capture a minute) against the same per-hour bars as SQL Server, so a slower schedule or a newly added server does not read quieter. They say plainly when no capture ran, and read Unknown with fewer than 3 captures. The fleet total no longer counts a server nothing measured as 0, the Viewer's fleet total now includes PostgreSQL servers' blocking captures, the total tile and the settings window name both units (SQL Server waits, PostgreSQL captures), the get_alert_settings description and payload and the operator doc row now name the unit for each engine, a PostgreSQL blocking tier above 60 is read as every capture so a fully blocked server still reaches Critical, and the get_fleet_overview description and payload agree.
- **SQL Server on Linux now gets a Volume Free Space alert, a disk fact and a report row** ([#7704]) - SQL Server on Linux reports no volume name, and the volume reads left those rows out, so a full disk raised no alert, showed no disk card in Analysis, had no Daily Health Report row, and printed a dash in the Database Sizes Volume column. The reads now name such a volume by the folder its database files sit in, such as /var/opt/mssql/data. Servers that report volume names are unchanged.
- **Index Analysis marks databases whose snapshot is left over and keeps them out of the totals** ([#7701]) - A dropped database, or an old database id that shares a name with its re-created twin, kept its last index snapshot in the Index Analysis totals and recommendations. A database whose newest snapshot is more than 2 days older than the server's newest cycle now shows `Not collected since <date>` in the grid and is left out of the overall row and the recommendations. The grid also shows `Collected` next to `Database`, and small sizes print in MB instead of 0.00 GB.
- **The Query Heatmap shows a quiet server's few bins at their own place in the range** ([#7683]) - On a quiet server the grid ran from the first bin to the last, so one 10 minute bin was stretched across the whole panel and read as a day of activity. The columns now run across the time range the page asked for, or from the first stored data when the range is cut, with empty cells where nothing ran, and the last axis label is left blank when too little room is left for it.
- **Database names sort without regard to case in the Databases picker and its sibling grids** ([#7689]) - the picker, database sizes, version store, protection, plan correction, object locking database list, file IO and config changes reads listed capitalised names before lower-case ones. They now sort Alpha, Beta, delta, epsilon, Gamma.
- **One condition no longer shows up twice on the Analysis tab when its headline figure differs** ([#7696]) - Rows for the same root fact on different incidents, such as "Write latency spiked to 2300 ms" and "Write latency spiked to 4935.5 ms", are now one row with the worst severity, the newest figure and the occurrences added up. Rows that differ in their subject (a statement, or a table or index named in the headline) stay separate cards, and the "Same cause, also seen as" line is gone.
- **FinOps no longer right-sizes an availability group secondary from its own load** ([#7705]) - FinOps told a person that a secondary replica could safely lose most of its CPUs and memory, and printed OVER PROVISIONED, judged only from the work it does as a secondary. A secondary now gets no CPU or memory right-sizing row and no savings figure. It gets one informational row that says to size it with the primary for the load it must carry after a failover, and its verdict reads "Not judged (secondary replica)". Darling, the Viewer and Lite.
- **A Query Store in the ERROR state is now marked, raised and noted** ([#7675]) - A database whose Query Store stopped on an internal error looked like any other row, and its old rows were graded and returned as if live. The Query Store Health row now shows "! ERROR" with a one-line meaning, Analysis now flags it and names the database (higher when it should be recording, and naming at most three databases in the headline), the clutter view gives it a reason instead of a grade, and top queries from it carry a note.
- **Recommendations cards open the tab that holds their evidence** ([#7677]) - Every card on the Recommendations tab used to offer `Open Queries tab`, even a wait card that told you to open Wait Stats, and a PostgreSQL server has no Queries tab. Each card now opens its own tab (Wait Stats, Blocking, Memory, File I/O, CPU, Queries, Configuration, Activity, Vacuum, or another PostgreSQL tab) and the button names it, and a card whose evidence is on no tab has no button. The advice that said a signal would "surface as a first-class finding" now says it will appear on the Recommendations tab.
- **The Memory tab now shows the small-query resource semaphore** ([#7676]) - The collector dropped the small-query semaphore, so the two Resource Semaphore panels repeated the pool panels. It is now collected, the Resource Semaphore panels name each row Regular or Small query, and the pool panels add the two together. The collector now keeps the small-query semaphore's missing max target as NULL, so `get_resource_semaphore` reports a null max target for it and the Viewer grid shows a dash, not a 0. Scoring, FinOps and custom view measures still use the regular semaphore only.
- **An offline server's empty panels and trends say what was collected** ([#7724]) - When a server went offline partway through a window, its empty Blocking and Queries panels said nothing could be known, while the trend panels called the same window a genuine all-clear. Now the reads stay `unavailable`, carry `collected_through`, and say `Nothing was recorded through the last collection at <time>. Nothing after that is known.` The MCP message still opens with who is offline and why. The web page shows the sentence in local time. A window with no collection keeps the old text. Top Procedures on a server offline for more than a week now opens with the offline lead, ahead of its no-collection text, instead of only saying to check collection health. Lite does the same.
- **Top Procedures and Top Queries now show work from databases the server no longer lists** ([#7725]) - Top Procedures showed an empty grid and Top Queries ranked unmarked rows among the rest when the work ran in a database dropped later in the window. Listed databases still rank first, and rows from dropped databases fill what the limit leaves. The MCP tools get_top_procedures_by_cpu and get_top_queries_by_cpu return a database_dropped key on those rows and name any dropped database with no row in databases_left_out, and the web dashboard, the Viewer and the Lite desktop grids show the name as "name (dropped)". FinOps Database Resources, Top Databases and High Impact mark dropped databases too.
- **Query Store pages no longer lose real rows to the monitor's own statements, and old unmarked statements are recognized** ([#7736]) - The Query Store Queries grid could show far fewer rows than exist, because the monitor's own statements used up the candidate limit before they were removed. They now leave before the limit once the key backfill has run. Statements an older build sent without the marker are now recognized from a closed list and keyed after the upgrade, so once the backfill has run they also leave Query Store Regressions, Top, the aborted and failed queries read, the Viewer comparison, the plan regression fact and its drill-down, and the investigation regression targets. Until the backfill has run, those reads keep the marker-only text test.
- **The Overview timeline says why a strip has no band, and marks runs outside the band once** ([#7663]) - A strip whose metric sat at 0 for the hour said "not enough history", and on a quiet server a third of the Waits strip was covered in overlapping red dots. The web strip with no band now says the metric sat at 0 when that is the reason. On the web, in the Darling Viewer and in Lite, a run of neighbouring points outside the band is now one red segment drawn over the line, with a single small dot for a lone point. A run ends where the timeline's line breaks at a collection gap, so a mark is never drawn across a hole in collection.
- **PostgreSQL Top Query Shapes names the database** ([#7731]) - The same Query ID in two databases showed as two rows with different figures and nothing said why. The collector now stores the database name, the tool reply carries database_name beside database_id (rows collected before this release say "database <id>"), and the web page and the Viewer show a Database column. The upgrade adds one empty column and does not rewrite history.
- **Small display details on the web pages** ([#7690]) - The FinOps Locking and Contention page no longer shows a yellow "Complete" strip for a complete list. The Collection Log keeps a finished analysis pass's sentence under Run note instead of Error. The Collection Health status select fits its text on one row. The Persistent Version Store `Oldest active txn` column is right-aligned. The uncompressed objects recommendation reads "2 uncompressed objects over 1 GB (7.9 GB total)" on Darling and Lite. Fleet Sweeps says "No watch items are open or carried." The Refresh button shows on every page and the sidebar list no longer moves between pages. Counts in empty texts carry a thousands separator. The PostgreSQL Extensions, Wait Trend and Index Bloat notes behind "More detail" name the panel or column instead of a field name.
- **Small wording and display fixes on web pages and MCP texts** ([#7684]) - Long Query Completions no longer tells a reader to enable a collector that is on. The empty texts read in plain words. File I/O Stats breaks ties by database and file. A lone bar on Waiting Tasks is 8 px wide. Healthy server reasons are neutral. The alert read tiles say Failed alert reads. The People strip and the editor's Personal-view reason have a sign out link. Why Suppressed drops the repeated word. Analysis advice no longer describes the detector.
- **Query Duration Trend and its range strip no longer draw a line across hours with no data** ([#7692]) - On a quiet server, Query Duration Trend, Executions per Second, Procedure Duration Trend and the Query duration and Procedure duration strips above Top Queries and Top Procedures drew one straight diagonal across hours with no samples, which read as a slow climb or fall. They now break the line where nothing was collected, the way File I/O Latency does.
- **Custom View charts with a second axis are readable, and a saved running-total panel says so** ([#7692]) - The second line no longer draws as a hatched block over the first once its points are closer together than the dash, and the legend names each line from its measure's display name, with the second marked as the right axis. A panel that charts a perfmon counter ending in /sec as its running total now shows the same warning a save shows, as a note on the panel. The stored definition is not changed. The Viewer's Custom View run window shows the same warning.
- **Ten small display details on the web pages are fixed** ([#7699]) - Long Query Completions Result prints OK, Error, Abort or Skipped, the Automatic Tuning list leaves out tempdb when it has no state, Host OS on SQL Server 2016 prints only the product name, the High Impact Band and Latch Stats Severity print as words with colors, Memory Grant Efficiency keeps small sizes, the memory pressure sentence reads correctly for the unlimited default, the version store empty text names a version without one, Memory Broker prints whole numbers, the Query Store Duration Trend says where it changes grain, and latch waits are their own category in FinOps Wait Stats.
- **Memory Pressure Events rows now carry their memory node, and the web grid shows a Node column on multi-node servers** ([#7691]) - A server with several memory nodes writes one record per node for the same event, so the grid listed one event several times with nothing to tell the rows apart. The collector now reads the node and the Darling store (V250) and Lite (v81) keep it in a new nullable column, empty for rows stored before. The web Memory Pressure Events grid shows a Node column when the rows in view hold more than one node. get_memory_pressure_events returns memory_node_id on each event in Darling and Lite.
- **The PostgreSQL configuration audit no longer says a sentence twice, and six other web details read right** ([#7670]) - The Why column no longer repeats "max_wal_size is 1 GB" and no longer names a checkpoint-pressure card or a composite that the page does not have. Availability Group cards say "just now" under a minute, not "0m ago". Index Analysis says "Compressible" and "Showing the largest 10 of 14 databases". Admin Monthly Cost drops the doubled dollar sign and shows a dash when no cost is set. Collector duration axes read in seconds or minutes with round tick values (0, 0.5, 1.0, 1.5 min). Collection Health notes read "shred gated: 1 (no new events since the last run, so the XML was not read)" in every web table and in the Collection Health Note column of the Viewer and of Lite.
- **The web pages name the databases a read left out** ([#7538]) - The Query Store Regressions, Query Store Clutter, Top Procedures and Top Queries panels said databases were left out but named none and pointed at a box the page does not have. They now list up to 20 names and count the rest, and a single database reads "was" instead of "were". The reply carries the true count in a new `databases_left_out_count` field, so the "and N more" on the page no longer depends on the note's wording. The Viewer and Lite Top Procedures banner states the count once, with the names, and no longer tells the reader to name a database in `database_name`. The empty Top Procedures, index analysis and Query Store clutter sentences no longer repeat the count, and the FinOps Index Analysis page now draws the left-out sentence itself, above an empty answer and above an answer with rows.
- **FinOps grids now mark databases the server no longer lists in Lite and the Viewer** ([#7725]) - Database Resources, Top Databases by Total CPU, Top Databases by Avg CPU and High Impact showed a dropped database's name with no mark in Lite, and the Viewer marked only Database Resources. All four grids now show the name as "name (dropped)" in both desktop apps while sorting and filtering still use the plain name.
- **The CPU note and chip on a server with no host CPU figure read in plain words** ([#7682]) - On a server that cannot report host CPU (SQL Server on Linux before 2025 CU1), the Overview CPU strip's note drew on top of the strip title, the CPU tab printed the reply's raw field names under the chart, and the "SQL CPU" chip repeated its own figure underneath. The note now sits beside the title, the CPU tab says "This server cannot report host CPU (SQL Server on Linux before 2025 CU1), so only SQL Server's own CPU is drawn", and the chip says "host CPU not reported". A PostgreSQL target with only a CPU figure keeps the plain "CPU" label.
- **FinOps page details corrected** ([#7706]) - Optimization's Expensive Queries heading no longer prints a limit that disagreed with the caption's count, and an empty Recommendations tab says so once. The idle databases strip ends with a period in both apps. The low latency storage card prints `no writes` for a database with no writes and says it was judged on reads only. Index Analysis no longer offers compression for indexes with no rows or under 0.01 GB, and its notes say so. Version Store says when a database has no row because this server holds a secondary copy of it, on the web page, in `get_pvs_stats` (also on the empty reply a database filter gets when it picks only such copies) and on the Version Store grid in the Viewer and in Lite. The note judges the replica roles at the time of the snapshot, not today's. Storage Growth prints sizes the way Database Sizes does, shows a dash with a hint for a missing baseline, and the button column no longer sorts.
- **Recommendations and restart notes read in plain words** ([#7726]) - The single-use plans advice no longer cites a Dashboard source line, sub-millisecond figures in advice print as `µs`, the restart note for an unknown identity change says SQL Server restarted, was renamed, or the connection reached another server, the memory pressure advice no longer names a tool, settings the default trace could not date print as the setting and the database instead of store keys, and the configuration change card says what changed in words (the recovery model of the database changed from FULL to SIMPLE) and names the history and audit reads by what they are instead of by tool name, on the card for older findings too. The web pages print trace flag ids with no separator, an average under 1 ms in microseconds instead of `0 ms`, the connection sentence and the red strip under an unreachable server's name in local time with the same time as the tile beside it (the Last error tile is gone), backtick spans in Recommendations as code, four plainer panel captions, the Server Connection subtitle as `the service's own connection`, and one sentence for an empty PostgreSQL Log Events panel.
- **Small web details corrected** ([#7679]) - The Blocking Severity and Deadlock Severity axes print 60,000, not 60000. Health Checks starts its sentence with `Full backup:` and `Log backup:`. Identity Capacity Band reads Critical, Warning or OK in the web page and the Viewer. Empty Blocking answers say `in this window` instead of `in the specified time range`, in Darling and Lite. A server's physical memory is rounded to the nearest MB, not cut down, so Server Properties and Configuration Audit agree. The Memory Pressure Events tooltip names the start of the hour.
- **Small details on the web pages** ([#7729]) - High Impact calls its flag Has plan and keeps its Database column in view when it scrolls sideways, as do other wide grids. Whole numbers print without .0. Stat tiles balance their rows. Needs attention no longer shows Healthy for a server listed for an advisory tile, in the web page and the Viewer. Default Trace reads up to 1000 rows and counts events by name, in the web page and in the MCP read of both Darling and Lite. Deadlocks drops the Victim process id from all three deadlock grids. The Query Store Regressions strip says how far back each comparison reaches without table names, and the list is ordered by severity. Query Store Overhead leaves out waits with no delta. The Custom View row cap strip starts with a capital, has no dash and says where the data starts.
- **Lite no longer deletes its database file when it cannot back it up** ([#7769]) - When Lite could not read its database file and could not rename it to a backup (a very long data folder path, or a folder that blocks renames), it deleted the file and its write-ahead log, which could lose the collected history. Now it leaves both files in place and stops with a message naming the file. When the old file was exported before the rename failed, Lite used to delete it and start from the export, and now it stops instead, and the message names the folder that holds the copy. When the backup works, Lite also logs where the old file went.
- **Lite's Import Data names the real cause when it cannot read the old database** ([#7779]) - When the previous install's database was corrupt or written by a storage version this build does not read, Import Data said it was locked and asked again and again to close the previous Lite. Now a lock, including the Windows forms of it, still gets the retry prompt, a corrupt database or one from an unreadable storage version ends the import with a message that says what was and was not imported, and the old install's archive files are still imported and shown in the views.
- **The Viewer shows a disabled server as disabled, not Offline** ([#7773]) - Darling Viewer: a disabled server shows a grey dot that says collection is disabled, and it no longer appears as Offline on the Overview or in Needs Attention. The Overview's total and roll-up count enabled servers and their lines say so, while the sidebar's "Servers: N" still counts every registered server.
- **The Viewer retries a failed server-list reload instead of locking the shell until restart** ([#7774]) - Darling Viewer: a server list reload that fails once no longer locks the window until restart, the next refresh retries the read and the shell comes back when the store does.
- **Query Store grid rows match the slicer bars** ([#7776]) - The Viewer's and Lite's Query Store grids bounded their window on when a snapshot was collected, while the time slicer's bars use when the interval ran, so a slice of a few hours showed an interval that started before it and lacked the last one. The grid now uses the interval start, as the bars do, and the Viewer's grid shows the inline query text of an interval collected after the slice ended. Lite's get_query_store_top MCP tool uses the same read and moved with the grid, while Darling's get_query_store_top MCP tool and web grid still use the collection time (#7814).
- **Perfmon trends plot a counter's _Total row instead of adding it to its instances** ([#7771]) - Perfmon trends in Lite and Darling plot a counter's _Total row instead of adding it to the instances it already totals, so Transactions/sec, Log Flushes/sec, Lock Requests/sec and the other Databases and Locks counters no longer read at about twice their value. The choice is made per object, so a counter name that two objects share keeps both objects' rows.
- **Top Procedures by Duration keeps long-waiting procedures on hourly windows** ([#7772]) - In the Darling Viewer, a window served by the hourly rollup took the fifty procedures with the most CPU and then sorted them by duration, so a procedure that waits far more than it computes never appeared. The hourly read now ranks by total duration, the same as the raw read.
- **The grid keeps a narrowed slicer selection after an auto-refresh** ([#7775]) - The Darling Viewer and Lite re-read the grid for a narrowed slicer selection after an auto-refresh, so the highlight and the grid show the same range.
- **The Compare baseline no longer overlaps the current window** ([#7777]) - On the Queries tab, Compare moved both ends of the window back by one day or seven days, so a window longer than that shift shared most of its time with its own baseline. Now the baseline ends where the current window starts, in the Darling Viewer and in Lite.
- **The Darling Viewer's Overview search narrows the fleet panel's blocking and deadlock totals to the matching servers** ([#7778]) - The totals and the server counts beside them describe the same servers, on SQL Server and PostgreSQL targets alike.
- **The Viewer and Lite say when a read failed instead of showing it as data** ([#7780]) - The Darling Viewer and Lite say when a read failed instead of showing it as data: fleet totals read unavailable rather than zero, a failed Overview strip is marked and named on the status line, sidebar dots go grey with Collection: Unknown, and a failed picker chart read is reported.
- **Statement text stores are pruned on the horizon of the collectors whose facts reference them** ([#7784]) - Statement text for PostgreSQL and Query Store facts is kept for as long as the longest retention of the collectors that reference it, instead of about 32 days.
- **Lite runs its daily cleanups soon after launch** ([#7785]) - The retention pass and the purges of old analysis results and dismissed alerts ran only after Lite had stayed open for a full day, so their tables grew until the size reset wiped them. Now each runs on the first housekeeping pass after launch and daily after that.
- **Webhook failure health is counted per route destination, and the failing alert names the route** ([#7786]) - Webhook failure health is counted per route destination, so a broken destination is announced even when another route on the same channel delivers, and the alert names the route. A URL the settings row and a route share is announced for each place it is configured.
- **A service stop during a database read is recorded as a cancellation** ([#7787]) - A stop that lands mid-read is no longer counted as a failed database or recorded as an ERROR run. It is recorded as CANCELLED, on the per-database loops and on server-scoped collectors.
- **The fleet sweep's per-target store lookups run inside the tracked sweep task** ([#7788]) - The fleet sweep's per-target store lookups run inside the sweep task, so the collection loop no longer waits on the store when it fires a sweep with disconnected targets.
- **A broken store connection during the procedure totals gap fill is now logged** ([#7790]) - When the connection broke during the gap fill, the rollback failed too and hid the original fault before it could be logged. Now the service logs the original fault first, then rolls back.
- **The sweep failure log line now carries the full exception** ([#7791]) - The "Collection sweep failed" line used to show only the exception message. It now includes the exception type, stack trace and inner exception, so the cause can be found from the service log.
- **Counter collectors no longer lose an interval when the store write fails** ([#7782]) - The wait, latch, spinlock, file I/O, memory grant, perfmon, procedure and PostgreSQL statement and wait collectors no longer lose an interval of counters when the store write fails before their rows are stored.
- **Lite tells you where your old database went when it has to start without all of its old data** ([#7812]) - When Lite could not read its database and started a new one without all of the old data, only the log said where the old file was kept. Lite now shows a message once after start with the path of the kept file, and the folder holding the exported copy when the import failed. If starting fails after that rebuild, the startup error message carries the same path.
- **The web Query Store grid and the MCP get_query_store_top and get_query_store_aborts windows now match the duration strip** ([#7814]) - On a short window the grid placed each Query Store interval by when it was collected while the bars above it placed it by when it started, so the grid showed an interval that began before the window and left out one that began inside it. The raw and per-interval-table reads, and the aborts read, now place the window by the interval's start, so the aborts tool and get_query_store_top with execution_type=Aborted list the same intervals. This is the Darling half: the Viewer and Lite grids moved with #7794. Long windows that read whole days from the daily summary still use whole days.
- **get_pvs_stats no longer tells you to check the primary replica on servers with no persistent version store** ([#7822]) - On a SQL Server 2016 or 2017 availability group secondary, the empty reply said the version has no persistent version store and also said to check the primary replica for it. It also added that note to a not_collected reply. The note now appears only on SQL Server 2019 and later, and never on a not_collected reply, in both Lite and Darling. Lite also reads the newest PVS snapshot once instead of twice when a database filter matches nothing.
- **Lite no longer mistakes an unrelated DuckDB open failure for a storage version mismatch or a lock** ([#7824]) - Lite searched the whole exception text, which includes the quoted data or import folder path, so a folder name that contains a phrase like "storage version" could skip the retry prompt in Import Data or start the Parquet migration at startup. It now searches only the exception messages, with quoted paths removed.
- **The Lite and Viewer Version Store grids no longer say to check the primary replica on SQL Server 2016 and 2017 availability group secondaries** ([#7822]) - On a secondary of those versions the grid showed a note telling you to check the primary for its persistent version store, but those versions have no persistent version store, so the advice was wrong. The note is now left out before SQL Server 2019, in Lite and in the Viewer. The check lives in the read that builds the note, so the Lite grid and the Lite MCP tool share one copy.
- **A newly enrolled server reads the Query Store interval table from its first day, and keeps the fleet rollup on for the panel** ([#7746]) - A newly enrolled server reads the Query Store interval table for 24-hour panels from its first day instead of after two, and no longer turns the fleet rollup off for the whole panel.
- **Darling settles a stressed server's timeouts with one liveness probe, keeps the runtime when the probe itself times out, and still drops a server that never answers** ([#7810]) - Darling settles a stressed server's command timeouts with one liveness probe instead of one per timeout, and a probe login that times out no longer drops the server's connection. After three such probes in a row with no completed run between them, the connection is dropped and reconnected as before.
- **Darling counts a daily run lost behind its own unfinished run and shows the permit backlog beside it** ([#7811]) - Darling counts a daily collector run that is refused because its previous run has not finished as one lost slot against the server's collection health, and the Warning and the collection_cycle row state how many daily runs are waiting for a permit and for how long.
- **Shared alerting and notification catch-alls log the exception, not only its message** ([#7816]) - Service and Lite failure log lines carry the exception, not only its message, so the service log file names the exception type, the Lite log file names the type and adds the stack, and the console log also shows the stack, when an alert sweep, analysis pass, store sweep, fleet sweep or self-alert fails. A check or store read that keeps failing on every sweep writes the exception once per streak and the plain line after that.
- **The Query Heatmap's last label never runs past the table** ([#7653]) - The Query Heatmap's last time label no longer runs past the grid: a label over fewer columns than a tick is left blank and a label wider than its cell is clipped at the cell, so the panel scrolls sideways only when the cells themselves do not fit.
- **Lite's Overview tab no longer says "Last refresh" after the strips control itself fails** ([#7837]) - an exception from the strip drawing or setup code (outside the per-strip reads) was logged and treated as a good refresh, so the status line showed "Last refresh" over strips that could be part drawn. It now shows an error naming the failure, as it does for a failed strip read.
- **A web dashboard link that passes through a redirect page (Teams Safe Links, a mail gateway) now opens signed in when your session is live** ([#7846]) - the session cookie is `SameSite=Strict`, so a link that another site's script redirected to arrived without it and showed the sign-in page, and only a reload opened the page. The re-navigation page from #6286 was served only when the browser said a person had started the navigation, and #6289's entry said script redirects and meta refreshes still got the sign-in page. Now any cross-site page link with no session cookie gets that page, which reloads once from the dashboard's own address and arrives with the cookie. With no live session you still land on the sign-in page. Images, scripts, forms and API calls from other sites are still sent without the cookie. Reported by gotqn.
- **The 24 hour fleet Query Store panel reads a shorter tail of recent rows** ([#7762]) - the rollup built an hour only when it was 3 hours old, once an hour, and one late write on one server sent every later hour to the slow tail. Hours are now built at 90 minutes by their own 15 minute timer, the row trigger marks late writes from the next hour on (store schema 251), and one stale server and hour no longer ends the rollup for the hours after it.
- **A server that reconnects after being dropped no longer inherits a stale liveness answer** ([#7851]) - When two collectors timed out on the same server together and one probe found it dead, the other probe still in flight wrote its late timeout into the cleared memo. The reconnected server then started with one timeout already counted against it and held an "alive" answer from the old connection for 30 seconds. A probe that returns after the memo was cleared now answers its own caller and writes nothing, and the log line for that run says another run already dropped the connection instead of claiming the connection was kept.
- **Long chart unit captions are no longer cut off at the chart edge** ([#7856]) - A y-axis caption wider than the room beside its axis, such as "snapshot rows" on the Blocking trend, lost its first letters off the left edge of the chart. The same happened to a long caption on the right axis of a dual-axis chart. A caption that would leave the chart now sits at the chart's edge instead, and captions that already fit stay where they were.
- **The Blocking page's blocked process report and deadlock trends no longer draw a 0 over time the collector did not read** ([#7850]) - the trends drew a flat 0 across the whole window, including time the service was stopped, the server was unreachable, the server was added part way through, or the collector was off or failing. The blocking and deadlock trend replies now carry a covered list built from the collector's successful runs (runs no more than an hour apart), and the page draws 0 only inside it. A reply from an older service keeps the earlier whole-window fill.
- **The warm-up gate counts its waits, so the warm-up's own-calls test needs no clock** ([#7733]) - Internal: the statement filter warm-up test proves its own calls did not wait by a wait counter instead of a wall-clock bound, so a busy test runner cannot fail it.
- **Waiting Tasks and Lock Waits name their unit, PostgreSQL Severity tiles and grids print a word, and a finding's sentence wraps** ([#7655]) - Waiting Tasks and Lock Waits name their unit on the axis, the PostgreSQL Severity tiles and the Severity column of the grids under them print a plain word instead of the read's token, and an Analysis Findings row that covers several chains wraps its whole sentence in a wide column.
- **PostgreSQL statement statistics no longer load query text every minute** ([#7783]) - On stock PostgreSQL the collector read the pg_stat_statements view, which loads the whole query-text file from disk on every call. It now calls pg_stat_statements(false), as it already did on Aurora. Statement text is still captured by the hourly text fetch.
- **A long right-axis unit caption no longer sits under the chart's menu button** ([#7863]) - On a dual-axis chart the right-axis unit caption could be drawn under the three-dot menu button in the top-right corner, and now it ends to the left of the button.
- **The Automatic Tuning empty text says which case applies** ([#7813]) - When a server has no plan correction rows, the web page and both products' tools now say whether the SQL Server version has no automatic tuning or the collector has not stored a cycle yet and collection health shows whether it has run, instead of listing two possible causes.
- **A TLS certificate store entry in subject mode no longer takes the web dashboard and MCP to loopback only after a renewal that leaves an older certificate in the store** ([#7865]) - When the newest matching certificate's private key could not be opened by the service account, nothing was served even though an older valid certificate had a readable key. The service now serves the newest matching certificate whose key opens and logs one Warning with the grant command for the newer one. This covers an older certificate that is still in the store and not archived. A renewal that archives the old certificate is not covered, because the service does not list archived certificates. A certificate with a corrupt Subject Alternative Name in the same store is now skipped instead of stopping the lookup, and the Warning counts it when another certificate is served.
- **The regression baseline read no longer logs a built-days failure and falls back to the slower read** ([#7873]) - The built-days read refused a UTC-kind window start on the raw regression arm, logged a warning every time and read the interval table on its exact bounds instead. The start is now bound as a plain timestamp, so the daily totals are used.
- **Lite get_memory_grants shows the newest collection in grants[] on windows over 25 hours** ([#7879]) - On long windows grants[] was a bucket average stamped at the bucket start, so a waiter count of 4 could read as a mean and its age counted from the wrong time. grants[] now reads the latest collection per pool, as Darling does. window[] is unchanged.
- **Lite Query Store regressions no longer count one interval on both sides of the window start** ([#7875]) - An interval with a snapshot before the window start and another after it had its executions counted in both the baseline and the recent window, which pulled the percent change toward no change and inflated the per-hour rates. Each interval now counts on one side by its last execution time, as Darling does, and the baseline and recent coverage check agrees with the read.
- **get_pg_wait_trend reads the right rate on servers without pg_wait_sampling** ([#7876]) - On servers sampled by the service (every RDS target and any stock target without the extension) the wait trend divided by the whole 300 second gap instead of the 30 seconds actually watched, so rates read about one tenth of the real figure and disagreed with analyze_server. The rate now divides by the watched seconds, the web chart and the Viewer read the same answer, and the result carries an instrument key naming the instrument the newest collection cycle recorded for the server.
- **Memory grant waits read in the right unit** ([#7882]) - The plan parser read the showplan grant wait time, which is in seconds, as milliseconds. A 12 second wait showed as 12ms, only went Critical at 5,000 seconds, and scored 1,000 times too low. The parser now converts to milliseconds, and the benefit score is now given to the "Memory Grant Wait" warning, which never had one.
- **A modified table variable counts once in the warning totals** ([#7882]) - The plan analyzer reported each table variable twice, once for the statement and once for the operator, so `warning_count` and `critical_count` read one higher than the findings. The operator copy is gone and the statement warning keeps the operator ids, so an operator touching a table variable no longer shows its own warning badge in the plan graph.
- **get_current_waits_trend gives the same bucketed answer in Lite and Darling** ([#7877]) - Lite summed a bucket's wait duration with no width in the payload, so long look-backs read 2x to 10x too high, and Darling returned every collection with no cap. Both now bucket to the same point budget as get_wait_trend, accept bucket_minutes, and say what each point is: the per-collection figure averaged over the bucket, with peak_wait_ms, peak_blocked_count and collection_count beside it. blocked_count is now a bucket mean shown to one decimal on the web Blocked Sessions panel. A window so long that no bucket width fits the cap is refused with a note to narrow hours_back, move as_of or filter database_name, and one that only misses the 200-point target is served at the widest width with a note saying so.
- **A cleared xmin holder, a vacuumed table, a dropped replication slot and a dropped database stop being reported as current** ([#7878]) - get_pg_xmin_horizon, get_pg_autovacuum_health, get_pg_replication_slots and get_pg_wraparound_risk took the newest stored row per key in the window and presented it as the present state, so a holder that finished, a table vacuumed clean, or a slot or database that was dropped stayed reported (with its old severity and remedy) for the rest of the window. They now answer from the collector's newest successful run only. A cleared holder is listed under cleared_holders, a cleared table under cleared_tables, and a dropped slot or database under omitted_slots or omitted_databases, each with a note. Slot entries gain last_seen. The Viewer's xmin, autovacuum, slot and wraparound grids show current rows only, and each panel's note counts the rows from earlier in the window that are not in the newest capture. A collector run that recorded a partial database failure no longer counts as a capture, so a database that failed to read once is not reported as cleared.The Viewer's xmin, autovacuum, slot and wraparound grids show current rows only, and each panel's note counts the rows from earlier in the window that are not in the newest capture. A collector run that recorded a partial database failure no longer counts as a capture, so a database that failed to read once is not reported as cleared.
- **A single long-running Agent job now counts as one job in the analysis** ([#7883]) - The running jobs fact counted one row per job per collection tick, so one job that stayed past its limit across several ticks read as several jobs, inflated the severity and the advice text. Lite and Darling now count distinct job runs (job id plus start time).
- **Alert History Dismiss All and Dismiss Selected on the web page only act on the alerts the column filters leave showing** ([#7919]) - With a column filter on, Dismiss All counted and dismissed every alert that passed the server box, and Dismiss Selected posted checked alerts the filter had hidden. Both now ask about and dismiss only the shown alerts, a hidden checked alert stays checked until the filter is cleared, and Dismiss All greys out when the filters hide every live alert.
- **The web page asks again for your edit access after a failed check** ([#7928]) - one failed session read, for example during a service restart, left every edit control hidden and the footer on "Access: not connected" until the tab was reloaded. A failed check is now asked again after 5 seconds, and only a good answer is kept. Behind a front door, the store picker and store scope start on the first good answer, not only when the very first read succeeded. While the check has not succeeded, the editors and tag pages say the session could not be checked instead of saying read-only.
- **An empty Query Store regressions answer no longer claims recent activity that the read did not see** ([#7962]) - when the read ran over the raw Query Store table, the check that decides the empty-answer text counted a row collected inside the window for an interval that last ran before it as recent, while the read itself counted it as baseline. A short window on a quiet server then read as checked and clean instead of no recent runs. The check now splits by each interval's last run, as the read does.
- **A zoomed or drilled panel on a saved dashboard stays zoomed and drilled when the dashboard refreshes** ([#7923]) - Each refresh tick rebuilt every panel with no brush zoom and no drill, so a panel you had zoomed or drilled into snapped back to the full window. The zoom and drill are now held per panel and server, within one page, range and set of view variables, and clearing the chip clears them. Changing a view variable drops them.
- **A refused note decision or threshold override on the Admin page now shows the service's own sentence** ([#7955]) - The page matched the start of old service wording that the service no longer sends, so a store-login refusal was shown as "This sign-in is read-only" and the advice was lost. The service now sends a reason code with each refusal and the page shows the service's sentence for it.
- **Editing a route for a disabled server keeps its scope** ([#7949]) - Pressing Edit then Save on a notification route scoped to a disabled server stored it as All servers, because the Applies to box had no choice for that server. The route, profile and People scope boxes now list disabled servers marked (disabled), a route or profile keeps a stored scope the lists do not hold, and the tabs say when the server or tag list could not be read.
- **The query heatmap counts distinct queries per cell** ([#7881]) - A cell's query count was the number of collection samples, so a steady query showed five times in a 5 minute cell and the count grew with the bin width. Lite (the chart and get_query_heatmap), the Darling service and the viewer now count distinct query hashes per cell, which is what the tool description and the "queries" labels already said. Cells are now shaded by their distinct query count.
- **Plan tools rank top_operators by each operator's own time** ([#7887]) - The analyze plan tools list the operators that own the most elapsed time instead of the plan's root path, and every row carries self_elapsed_ms beside the cumulative actual_elapsed_ms. Exchanges, whose time is wait rather than work, carry a null self_elapsed_ms with a note saying so and rank after every operator that has one. Estimated plans still rank by cost_percent.
- **Plan warning counts no longer multiply by how often a plan was seen** ([#7886]) - The MISSING_INDEX and PLAN_WARNING facts and their drill-down read each of the window's top ten plans once, however many snapshots it appears in, in Lite and Darling.
- **Raw Query Store retention purges again after a store outage instead of going back on hold** ([#7961]) - After an outage gap, the 7-day Query Store interval hourly rollup trimmed itself to the first hour after the gap. The gate then re-held the raw query_store_stats purge, and running --backfill-rollups again did not help. The gate now treats a source gap at a rollup's own retention horizon as nothing to cover. The raw purge's hole scan now checks such a rollup only within its own retention. A raw row inside the rollup's horizon that the rollup never stored still keeps the purge held. The retention hold runbook has a new section on what to expect after a store outage.
- **Investigations keeps the Close reason being typed across the 60 s refresh** ([#7956]) - The reason, its focus and its caret survive the page's poll, and a close whose answer is still out keeps the refreshed button disabled so one close is sent.
- **Known Patterns saves a new entry once when Save is clicked twice** ([#7958]) - Save is disabled while the write is out and a second click sends nothing, so a double click no longer stores two identical entries.
- **The alert notebook opens the dismissed firing its link names** ([#7918]) - A triage link for a dismissed alert shows that alert and says it was dismissed, instead of another firing of the same metric.
- **FinOps Database Resources keeps the window you picked** ([#7922]) - The tab no longer goes back to 24 hours on the 60 second refresh. The picked window stays until you change it or pick another server.
- **A tiny CXPACKET or CXCONSUMER wait is no longer a Critical plan finding** ([#7888]) - A parallelism wait's benefit is bounded by the wait's own share of the statement's elapsed time, so a few milliseconds of CXCONSUMER on an I/O-bound plan is Info instead of the top-ranked Critical finding.
- **get_current_waits_trend averages each series over every collection that ran** ([#7974]) - A database blocked in 1 of 60 collections of an hour drew the same bar as one blocked the whole hour, because the average only covered collections where the series had a row. The average now covers every collection that ran in the bucket, so sporadic and steady activity differ, collection_count is the number of collections that ran, and the Blocked Sessions panel shows small values such as 0.017 instead of 0.
- **The Admin page says which web changes are off and that servers still save** ([#7950]) - With web.allowAdminWrites off, the line under the Admin heading names the changes the setting turns off and says adding and editing servers follow the edit seat and still work. The web configuration doc says the same.
- **Servers with a negative id can now have an environment profile and a known pattern** ([#7975]) - Darling server ids are a hash of the storage name and about half of them are negative, but saving a server-scoped environment profile refused any id of 0 or below. Known patterns refused the same ids, and the Known Patterns page dropped a negative server id and saved the entry for every server. All three now accept any nonzero server id, and the check that the server is registered still applies.
- **A skewed parallel plan no longer blames the operator above an exchange** ([#7979]) - In a parallel plan whose rows landed on fewer threads below an exchange than above it, the operator just above the exchange was given the whole branch's time as its own and could be named the plan's most expensive operator. Its own time now takes off the slowest thread below the exchange, so in the example plan the Sort drops from 17,111 ms to 1 ms.
- **A route save refused because someone else changed the route no longer refuses every later save** ([#7952]) - After the 409 the Admin Routes form reads the current route and its version, shows the service's sentence, and the next Edit then Save carries the current version. The Routes and Profiles Save buttons and the route Delete button are disabled while their write runs.
- **Report feedback shows on the page as soon as it is recorded** ([#7952]) - A 60 s refresh landing while the record was in flight no longer leaves "No feedback yet." beside "Feedback recorded." until the next refresh.
- **Admin asks before removing an override, flipping the agent switch, undoing an agent change, removing a person's sign-in or grant or disabling a person** ([#7952]) - Each of these writes now names what goes away and waits for a yes. Turning agent changes on shows a warning panel first, the way the per-server action switches do, and the Agent changes buttons are disabled while a write runs.
- **A time slicer drag to the right edge stops at the window end** ([#7931]) - The narrowed window's end is no longer rounded up to 30 seconds past the page range, so the grids read, the label names and the selection box shows the window end itself.
- **A picked range that ends later today is drawn up to now** ([#7931]) - The server page's charts, time slicer and labels end where the read did instead of at the future end, so no chart has an empty right part.
- **A time range of a fractional number of minutes is named by the minutes it holds** ([#7931]) - A typed 7.5 minutes is listed as Past 7 minutes beside its length of 7m, and its id reads back as that range, in the web picker and in the desktop picker alike.
- **Plan analysis no longer flags a TOP over an ordered index scan** ([#7890]) - A TOP that stops an ordered scan after n rows is the plan the query wants, so the "Top Above Scan" finding is kept for a TOP over an unordered scan and for a Top N Sort, which read the whole scan.
- **Bare Scan and Scan With Predicate no longer warn on small or barely filtered scans** ([#7890]) - Both plan analysis rules now need a scan of at least 10,000 rows that takes at least a tenth of the statement's time. Scan With Predicate also needs the predicate to discard more than half of the rows read, takes its Critical level from measured time rather than estimated cost, and says how many rows the scan read and returned.
- **Plan analysis text no longer contradicts the plan it describes** ([#7890]) - The data type mismatch finding says the optimizer built a seek range through the conversion instead of claiming seeks were prevented, a statement with OPTIMIZE FOR UNKNOWN is no longer also reported as local variables, the parameters card names OPTION(RECOMPILE) only when the statement has it, the table-valued function finding dates the 100 row guess to SQL Server 2014, and the plan viewer omits Est. Rows to Read when the plan does not carry it.
- **The QUERY_SPILLS finding grades spilled pages per observed hour, not the window total** ([#7884]) - The same spill rate scores the same at every hours_back, the bars sit at 57,000 and 173,000 pages an hour as measured on a production fleet, the finding text states the rate, and a few spilled pages no longer boost the disk space and memory grant findings.
- **The blocking finding grades a storm in a short or partly collected bucket at its real rate** ([#7884]) - Each 4 hour bucket divides by its own covered hours, never under one hour, and the busiest bucket by rate is the peak, so a 6 hour read whose storm sits in the last two hours no longer reads at half its rate.
- **The web Custom Views list keeps pasted import text and open menus through its refresh** ([#7925]) - The 60 s poll redraws only the cards, keeps the import box, the template menu and its server pick as they are, and reads a view's definition again only when its version changed.
- **The web Alert Rules list refreshes with two requests and keeps its template menu open** ([#7925]) - The 60 s poll reads a rule's definition again only when its version changed, redraws only the cards, and leaves the head and an open New from template menu as they are.
- **The sidebar's Views list keeps the last good list when a poll read fails** ([#7925]) - A failed or refused read of the saved views no longer empties the section to "No views yet". The last good list stays, under a note that says the views are unavailable.
- **Admin tabs keep what you are typing across the 60 s refresh** ([#7951]) - A refresh tick no longer rebuilds an Admin tab while a text box in it has the focus, the Daily Health Report and Database Protection editors keep typed values across a redraw, and a Preview that answers after a redraw shows on the page.
- **Query Store History chart tooltip names every plan at a hovered time** ([#7920]) - The hover showed one plan's duration and a dash for the others at every snapshot. The chart now gets one entry per snapshot with a column per plan, so all of them are named.
- **Investigations closes a request's pane when a Close takes it off the filtered list** ([#7957]) - Closing a request under a State filter no longer leaves its pane and Close form on the page once the request no longer matches the filter.
- **Investigations keeps a Close reason being typed when another request's close finishes, and does not move the focus into the reason box when the page is opened again** ([#7957]) - Closing one request no longer wipes the reason being typed for another, and leaving the page with the reason box focused no longer moves the focus back into it on return.
- **Known Patterns keeps a form opened after Cancel when an earlier save answers** ([#7957]) - A save's answer closes or redraws only the form it was sent from, so a new entry being typed after Cancel is no longer closed or redrawn when the cancelled save finishes.
- **Overview strips say when a read failed** ([#7921]) - A failed blocking or deadlock read beside a series with points is now named in the strip's header instead of the strip reading as no blocking. A page with no points and a failed read shows that failure instead of only the no-samples sentence.
- **The Run Now result table shows each run's time in the browser's zone** ([#7930]) - The When column read the service's UTC stamp as local time, so it was off by the viewer's UTC offset. It now reads the stamp as UTC like every other time cell.
- **The PostgreSQL Vacuum tab's Horizon Holders grid shows how many captures recorded each source** ([#7926]) - The column read a name the service never sends, so it showed a dash on every row. It now shows the count, labelled Captures (this source), with a header hint saying what it counts.
- **A Key Lookup warning is graded on what the lookup cost, not always Critical** ([#7889]) - A lookup that was half or more of a statement's time stays Critical, a tenth is Warning and a cheap or rarely run lookup is Info, so the PLAN_WARNING critical count and the plan tools point at the lookups that matter.
- **A server page poll now swaps its new panels in when the last panel to answer is one that hides itself** ([#7934]) - The in-place refresh watched only for panels drawing, so a panel that settled by hiding (Instance CPU on a PostgreSQL server that is not Aurora) could leave the page showing the previous poll's panels under a fresh Updated time.
- **A saved view panel the service refused as busy keeps its last figures through the retry wait** ([#7934]) - The poll swapped the Retrying notice in over the panel's data and stamped the page updated before the panel's second read had run.
- **Alert History keeps its table through a failed poll, so an opened Detail line stays open** ([#7934]) - One failed poll replaced the table with an error and the next good poll rebuilt it from nothing. The failure now shows as a notice above the table when the poll asked for what the table shows, and a failed read after a changed choice or a dismiss still shows the error in the table's place. The Updated time holds on a failed poll on the Fleet, Availability Groups and saved view pages too.
- **Resume and a tab return re-render the page at once, and a tick where the page and the sidebar are both due asks the store for one fleet roll-up** ([#7935]) - The page's render was decided after the sidebar's reads had started and counted them as its own, so it waited a tick or more and then sent a second fleet read.
- **Investigations no longer redraws a request's pane when its detail read answers after the request was dropped, on the web page and in Lite's window** ([#8007]) - A filter change or a list re-read that drops the picked request also discards its detail read still out, so a late answer no longer puts the pane back, Close form included on the web page, for a request that is no longer picked.
- **A held analysis finding no longer pages again when its story path gets shorter** ([#8012]) - A finding that was paged once and held could page again, and open a new investigation request, when its story lost a leaf fact, because the hold was keyed on the full path. The first analysis run after an upgrade did this for every held story that carried a leaf the new build no longer joins, and so did every later restart. A shorter path of the same server and root is now held when a wider path of that story was already paged, found among the paths stored for that root across the findings retention. A longer path or a different root still pages.
- **Procedure History tooltips show small rates with their digits** ([#7933]) - A rare procedure's Duration (ms/s) and Executions (per second) tooltips rounded to 0 ms/s and 0.000. They now keep two significant digits below 1, so 0.17 ms/s and 0.00028 a second read as such.
- **Latest-snapshot reads plan against one chunk instead of every retained chunk** ([#8013]) - get_session_stats, get_index_usage, get_object_locking, the Storage Growth index drill, the anomalous-jobs alert read and the query_store state prune resolve the newest collection time first and bind it, the way get_database_sizes already did. get_index_usage resolves it once for its rows and its match count, so the two always describe one capture. Results are otherwise unchanged.
- **A markdown line of inline triple-backtick code no longer swallows the rest of the cell** ([#7944]) - A line that opened and closed inline code with three backticks was read as a code fence, so everything after it was drawn as code. It now reads as a code span in a paragraph, and real fences open as before. Inline code spans now pair backtick runs of the same length, so a two-backtick span is one span and a shorter run inside a span is part of its text.
- **The Viewer's xmin horizon "Held In" column now divides over every capture in the window** ([#8014]) - A session seen in 2 captures that won both read "2 of 2 samples" and sorted with the chronic holders. The column now reads "2 of 60 captures" and sorts by that share, matching the get_pg_xmin_horizon tool, and shows a dash when the window has no captures.
- **Small web values read in the right unit and keep their digits** ([#7945]) - A duration or size just under a unit edge steps into the next unit (999.6 ms reads 1.0 s), a percent under 1 keeps a digit, byte columns on the PostgreSQL Activity tab read as sizes, a bucket mean under 1 and a query tag figure under 0.1 no longer read 0, a custom range leaves out the trend bucket that starts at its end, the Alert History filter line names the server by its display name, and a failed read with no message names its path.
- **The PostgreSQL replication slot total no longer adds up slots that hold the same WAL** ([#7939]) - get_pg_replication_slots reported total_retained_wal_gb as the sum of every slot's retained WAL, and the Retained WAL tile on the web Replication tab showed it. Slots on one cluster retain overlapping stretches of the same log, so with two or more slots behind the figure overstated the WAL on disk. The total is now the furthest slot's figure, which is what the server keeps on disk, and it is null rather than 0 when no slot has a measured size. Per-slot figures are unchanged.
- **Searching for a parent tag finds the servers tagged under it** ([#7984]) - The Darling Viewer's Overview and sidebar, Lite's Overview, and the web Fleet page and sidebar match the names of the tags above a server's tags, so a server tagged HOSTS under PROD > BULK is found by BULK or PROD. The pill still shows only the assigned tag.
- **--backfill-rollups no longer refreshes every slice for nothing after a rollup's own retention has trimmed it** ([#7981]) - TimescaleDB counts a range a rollup's retention dropped as still built, so a plain refresh over it does nothing. The verb now reads the rollup's invalidation log and runs such slices forced from the start, instead of running them plain and then one forced refresh over the whole range. The result is the same and the wasted slices are gone.
- **get_pg_top_queries and the PG_BAD_ACTOR drill-down count a statement's block and WAL figures for every interval its calls cover** ([#7909]) - A series' first sample in the window contributed nothing to the shared, read and temp block figures or the WAL bytes, so a statement that ran once in the window read 0 on all of them and blocks per call read low. The first sample is now differenced against the series' latest row before the window, in the tool and in the drill-down alike. The Viewer's PostgreSQL top-queries grid and Change Watch read the same statement.
- **get_long_query_completions picks events by when they happened, not by when they were stored** ([#7900]) - On both products the tool windowed on the collector's storage time, so a narrow window placed on an incident missed a completion stored a few minutes after the window ended and listed one that happened before the window began. The window is now on the event's own time, as the blocking and deadlock tools already do. Darling keeps a storage-time floor beside it so the store still skips its older partitions.
- **The Queries tab reads each duration trend once per paint** ([#7941]) - The Top Queries and Top Procedures slicer strips share the chart's request for the duration trend instead of sending a second, unsized one on every paint and poll.
- **A web read refused for temp_file_limit is recorded as a limit, not an error** ([#7938]) - The read health reads classify a tool-caught 53400 sentence the same way they classify the live exception, so a store refusing a read's spill no longer counts as a fault.
- **Parallelism advice recommends only a change from the server's own MAXDOP and cost threshold** ([#7894]) - The CXPACKET, THREADPOOL, high-DOP and scheduler-yield cards state the collected settings and the cores per socket, and say so when both are already within guidance instead of asking for a threshold of 50 and a cap of 8.
- **Object growth fires for a table that was empty the day before** ([#7894]) - A table that held no pages in the earlier daily snapshot and holds 100 MB or more in the latest now raises the growth anomaly, with the card saying it grew from empty instead of printing a percent. Growth from empty is still held to the 100 MB floor.
- **The CPU spike drill-down lists each request once** ([#7896]) - A request that was alive across several snapshots around the peak took one slot per snapshot and could hide the other consumers. Each request now takes one slot, at its longest sample, and one connection's successive requests are told apart by when each one started.
- **An expired session's sign-in strip stays on the Availability Groups page** ([#7959]) - A read answering 401 no longer lets Availability Groups draw its "no groups" line over the strip, and Known Patterns and the Recommendations tab no longer record an empty list or an emptied panel for a read that never answered.
- **Job History no longer says more runs matched when only runs outside the window were cut** ([#7960]) - A window that starts mid-hour shows its limit notice only when the row limit cut a run inside the window.
- **FinOps utilization says no data, not 0, for the memory grant figures when no grant sample exists** ([#7906]) - On both Darling and Lite the utilization read leaves max_grant_waiters, grant_timeouts, forced_grants and grant_utilization_pct null when the window holds no memory grant sample, and the get_finops utilization view says why in grant_reason, so a server whose grant collector is off no longer reads like an idle server with no grant pressure.
- **A deadlock storm grades the same on a 4 hour pass and a 24 hour read** ([#7893]) - The DEADLOCKS finding is graded on the busiest hour of the window instead of the whole-window average, so a burst no longer reads lower on a longer window. The average is still shown beside it.
- **Blocking and deadlock spike findings fire at 3 times a server's real baseline** ([#7885]) - A server whose usual rate is under one event an hour no longer needs three an hour before a spike is reported. The minimum counts of 5 blocking events and 3 deadlocks still apply.
- **get_analysis_findings refuses a limit of 0, a negative limit or one over 1000** ([#7901]) - The tool now validates limit like every other paged tool and answers the invalid envelope instead of an empty page that reads as a real answer.
- **get_perfmon_stats matches a counter or instance name whole before falling back to a substring** ([#7903]) - Asking for Transactions/sec returns that counter alone instead of every counter containing the words, and the texts of get_top_queries_by_cpu, get_query_fleet_summary and get_pg_wait_sampling now say what their lifetime fields, entries and instrument note are.
- **The blocking finding's top blocking chains list each wait once** ([#7892]) - The list under a blocking finding showed up to five escalating reports of one long wait, because SQL Server reports a still-blocked session every few seconds. Each wait now appears once with its longest report and a report count, on both products, folded the same way the finding counts its events.
- **find_queries_by_text lists a statement once for each database it runs in** ([#7905]) - The plan-cache source kept one row per statement hash on a server, so a statement shared by several databases was listed under the database that ran it most recently and the others were missing. Each database's copy is now its own row with its database_name, the key get_query_trend takes.
- **Lite get_wait_trend hides ignored wait types** ([#7880]) - The trend now applies the same ignored (benign) wait list that get_wait_stats and get_wait_types apply, so rows stored for a wait before it was ignored no longer come back as a series. The not_collected answer says the type is on the ignored list and is not shown, instead of claiming the collector never stores it.
- **Lite get_query_trend is bucketed and capped like the other trends** ([#7880]) - A day of one-minute collections no longer comes back as about 1,440 points. The read sizes its buckets to the shared 200-point target, takes bucket_minutes for another width, refuses a width over its cap with the narrowest that fits, and says what each point holds: deltas summed across the bucket's collections, averages weighted by executions, the DOP pair and plan facts from the plans that ran, and collection_count.
- **Lite top procedures and CPU trend answer the same way every time** ([#7880]) - get_top_procedures_by_cpu and the Top Procedures grid break a CPU tie by database, schema, object and type, so a quiet window lists the same procedures on every call. get_cpu_utilization buckets each sample on its UTC instant, so the two passes through a fall-back hour are two points instead of one average, each still stamped on the server's clock. The buckets are cut on the UTC grid, so an hourly stamp in a half-hour zone lands on :30 and a one-day bucket is stamped at UTC midnight in local time.
- **A saved notebook with auto-refresh on keeps your place when it refreshes** ([#7924]) - Each tick rebuilt the whole notebook from loading strips, so the page jumped back up and a zoomed chart snapped back to the full window. The notebook now stays on screen and each cell is refreshed in place once its new read returns, and a zoomed or drilled panel cell keeps its zoom and drill.
- **The notebook composer no longer rebuilds every cell when you add, move or remove one** ([#7936]) - Each of those clicks rebuilt every cell's editor, so every panel sent its preview read again and a field you were typing in lost its focus. Only the cell that changed is built now, and the other cells keep their editors and their previews.
- **The panel and alert-rule editors no longer let a slow reply overwrite what you chose while it was on its way** ([#7937]) - A read's sample that returned after you had picked a chart type replaced the pick, the older of two samples could fill the fields for a read you had left, and an alert rule's earlier check could draw its verdict over the newer one's "Checking" strip. Each editor now drops a reply that is no longer the latest.
- **find_queries_by_text's all-servers search reads the samples by the matched texts instead of joining the whole raw window** ([#7849]) - The search reads the matching texts' digests first and binds up to 1,000 of them to the sample read, so a rare pattern no longer scans every server's window. The reply says whether the samples were probed or joined. The trigram option also builds an index on query_stats over the text digest that the probe reaches on uncompressed chunks, and the option off drops it.
- **get_pg_lock_stats and the PostgreSQL lock tab count every collector run as a capture** ([#8055]) - total_captures is now the number of successful pg_lock_stats runs in the window, the runs that found no lock included, so a lock seen in 3 of 60 runs reads as 3 of 60 instead of 3 of 3. Where the run log does not cover the window, the figure falls back to the capture times the lock rows show.
- **The corrected Query Store hourly rollup refreshes after the hourly it reads from** ([#8053]) - An hourly aggregate built from another hourly now takes the hour's last minute, after its source's slot and window, so a bucket reaches it the same hour in one pass instead of an hour late and materialized twice. Existing stores are re-phased on the first start.
- **Lite tab reads keep flowing while a background write waits behind a long archive read** ([#7256]) - A write waiting for the local store lock asks for it every 50 ms and holds new reads back only for the last second of its budget, so a tab switch during a long archive read waits for at most that second instead of the rest of the read. The write still gives up at the same budget as before.
- **get_pg_top_queries and the PG temp-spill offender list read each statement's text once, after the ranking and the row cap** ([#8054]) - The statement text was joined onto every stored sample in the window and compared once per sample before the rows were ranked and capped, so the reads' buffer traffic and spill grew with the window's sample count and the stored text size. The text is now looked up once per returned row in both reads. The Viewer's PostgreSQL top-queries grid and Change Watch read the same top-queries statement.
- **Darling get_query_trend is bucketed and capped like the other trends** ([#8063]) - A day of one-minute collections no longer comes back as about 1,440 points. The read sizes its buckets to the shared 200-point target, takes bucket_minutes for another width, refuses a width over its cap with the narrowest that fits, and says what each point holds: deltas summed across the bucket's collections, averages weighted by executions, the DOP pair and plan facts from the plans that ran, and collection_count. Past the raw tier's four days the hourly rollup answers in buckets of whole hours, so a 90-day window is 181 twelve-hour points instead of 2,160 hourly ones.
- **A custom alert rule scope that names servers or a tag without a mode is refused instead of applying to every server** ([#8077]) - The validator now reads every key in the scope. A servers list or tagId with no mode, a key that belongs to another mode, and a key the scope does not know are each refused with an error naming the key. A rule already saved in that shape is reported as broken by the rule health alert rather than evaluated fleet-wide, and the web editor and the Viewer form open it as the scope its one key names, so saving it keeps that scope.
- **Web Mute Rules and Known Patterns tables scroll sideways inside their own box** ([#8082]) - A rule or pattern with a long scope or note no longer pushes the Expires, Created and Edit / Pause / Delete columns past the right edge of the page at laptop widths. The table scrolls inside its own box, as every other grid does.
- **memory_stats plan_cache_mb is the plan cache once** ([#8079]) - The collector reads the Plan Cache object's _Total counter instance instead of summing every cache store together with it, so plan_cache_mb now agrees with the memory clerks and the plan cache reads. Rows stored before the change keep the doubled figure.
- **tempdb_stats measures the top session by the pages it holds** ([#8079]) - The top session, its top_session_tempdb_mb and total_sessions_using_tempdb take allocation minus deallocation, so a long-lived session that churns temp tables is measured by its hold rather than by everything it has ever allocated, the top consumer can no longer read larger than tempdb itself, and a server where no session holds pages stores no top consumer at all instead of some session at 0.00 MB.
- **The PostgreSQL eviction note names the missing extension on Aurora** ([#8075]) - On an Aurora target where pg_stat_statements is not created in the collector's database, get_pg_top_queries, the web dashboard and the Viewer now say the eviction counter needs CREATE EXTENSION pg_stat_statements there, instead of naming an old major or an old extension version that cannot apply. The get_pg_extensions guide says that on Aurora get_pg_top_queries does not depend on that row.
- **FinOps idle databases are judged on a store that keeps less than 7 days of raw query stats** ([#8076]) - The idle-database section, grid and recommendation read coverage and activity from the hourly rollup for every whole hour and raw only past its watermark, so a store whose raw tier is dropped at 4 days no longer answers unavailable for ever.
- **The duration trends' execution_count is a count** ([#8073]) - get_query_duration_trend, get_procedure_duration_trend and get_query_store_duration_trend publish execution_count as the executions summed over each point, on Darling and Lite, so summing it over a window gives the executions that ran. It was the per-second rate truncated to an integer, which read 0 on a quiet server. executions_per_second is unchanged.
- **Query Tag Usage on a store with no query tags says so instead of drawing one untagged = Total table per server** ([#8083]) - The page asks for the tag list first and shows one line pointing at Manage Query Tags, and get_resource_use_by_tag in Darling and Lite answers empty with that reason without reading any section. With tags defined, a server whose rows are all untagged shows one line with its figures in place of a two-row table that said them twice.
- **The total wait rate no longer drops when one wait type returns after missing some collections** ([#8078]) - get_server_trend total_waits, the Overview strips in the web UI, the desktop viewer and Lite, and Change Watch's total wait measure credit each collection's whole wait over its cadence, so the total equals the sum of the per-type trends. One returning type's longer interval used to divide every other type's wait in that collection, so the total could read below a single wait type's own trend.
- **Web dashboard: no opener-policy console error over plain http, a clipped card name readable on hover, and one-line Collection Log notes** ([#8096]) - The Cross-Origin-Opener-Policy header is sent only where a browser honours it (https or a loopback address), so a plain-http dashboard on a LAN address no longer logs an error on every page. A Custom Views or Alert Rules card shows its whole name on hover when the card clips it. The Collection Log's Error and Run note cells are one line tall and open on a click, so a long note no longer stretches a row while it sits off-screen to the right.
- **get_pg_wait_trend and the Wait Trend chart serve Aurora PostgreSQL targets** ([#8074]) - The trend is read from the engine's cumulative wait counters on an Aurora target, where the sampler does not run, with instrument engine_cumulative and measured wait time per second of each collection interval. An Aurora target with no wait rows answers empty and points at get_pg_wait_stats instead of a permanent gap pointing at a read with no time series. The Viewer and the web's caption follow the same source.
- **get_sweep_reports and analyze_server fit the response budget and say when they cut** ([#8081]) - The sweep timeline is cut to the response budget newest first with truncated, sweeps_returned and budget_note, and latest is never cut. analyze_server on both products keeps its most severe findings within the budget with truncated, findings_returned and a truncation_note pointing at get_analysis_findings.
- **The FinOps High Impact read ships only the hashes it can keep** ([#8084]) - The ranking statement reads the base table, groups before it ranks, probes each hash once for its newest grant, and keeps the top N on each of the six measures in SQL. The scores, shares and order are unchanged. On a day of one server's query statistics at the collector's cadence the statement went from tens of seconds to under a second.
- **Three PostgreSQL reads say what their rows show** ([#8092]) - get_pg_session_states no longer grades a short writer Critical on a horizon age of 1 or 2: the sustained-holder rule needs a peak age of at least 10,000 transactions, and the finding names the age it saw. get_pg_io_stats and get_pg_io_trend report the write counters as untracked on Amazon Aurora whether the engine reports them as NULL or as 0, with the write fields null and the Aurora sentence in the note, and the Viewer follows. get_pg_lock_stats rows carry relation_oid beside relation_name, so relations in another database are distinguishable and resolvable.
- **The Memory Overview chart's Y axis covers Target Memory and Memory Grants, not only Total Server Memory** ([#8100]) - In Lite and the Darling viewer a Target or grant sample above Total was drawn off the top of the plot and the dashed target line looked broken. The axis is now sized from the largest sample across every plotted series, with the same headroom as before.
- **Five MCP reads say what they served** ([#8090]) - get_perfmon_stats answers a status word for a filter that matches nothing, the window_truncated sentence names the 90 minutes of slack the tiered trends allow, get_load_by_caller drops the sp_server_diagnostics session and counts only the window's part of a request that started before it, list_investigation_requests says which of its three server names to group by, and get_investigation_evidence refuses an unknown section name and never stores a one-character report fragment.
- **Web pages say what the data beside them shows** ([#8095]) - An Amazon RDS for SQL Server panel names what the engine does not expose instead of saying the data does not apply. The Fleet page's attention filter says it keeps Healthy servers with a warning tile and counts them in its notice and overflow line, and the front door's store sections carry the Needs attention list and the Updated stamp. A Cleared row in the alert notebook reads as the resolution of its firing, Job History under All servers says when no server in the store runs SQL Agent, and FinOps on a store with only PostgreSQL targets says so once at the top.
- **The Lite server tab toolbar wraps instead of clipping at the window's edge** ([#8098]) - Refresh, Auto-refresh and its interval, and the Times picker start a second line when the window is narrow, so every control is reachable at every size the window allows. The interval combo shows its whole caption when the row only just fits.
- **FinOps Query Preview cells show the statement on one line** ([#8110]) - The Expensive Queries and High Impact grids in Lite and the Viewer collapse the statement's line breaks and indentation to single spaces and trim with an ellipsis at the column's edge, instead of wrapping the raw text into a 90 px block that cut its last line. The full statement stays in the tooltip, and Copy Cell and the exports are unchanged.
- **The server Overview's five strips can all be reached at any window height** ([#8104]) - On a 1080p screen the I/O Latency strip was drawn below the window with no scrollbar, and at smaller heights the scroll stopped short of it. The strips now scroll whenever they do not fit, in the Darling Viewer and in Lite.
- **Darling Viewer: a read-only seat sees Collection Health's Purge Now, Run all collectors now and each row's Run now disabled, with the reason as their tooltip** ([#8103]) - The buttons are off from the moment the tab is built, as Alert History's Dismiss buttons are, instead of answering with a message box or status line after the click. The tooltip says the seat is read-only and how to connect with a login that can write.
- **Darling Viewer Overview cards keep their detail column readable** ([#8112]) - A card with an Identity capacity or Protection row no longer squeezes the CPU, Threads, Memory, Blocking, Deadlocks and Collectors detail to a few characters, the optional rows size their own line, every detail cell shows its full text on hover, and the Collectors detail trims instead of wrapping mid-word.
- **Collection Health says when one connection failure reached several collectors, and shows each row's last error beside its status** ([#8101]) - A target unreachable for one cycle fails every collector due in it. The tab now words that once above the grid, with the time, how many collectors it reached and whether each missed only that run. Last Error At and Last Error follow the Status column, and the message is trimmed to its cell with the whole text in a tooltip. The band and its failure-rate rule are unchanged. Lite and the Darling Viewer both.
- **Memory Pressure Events bars sit inside their hour** ([#8109]) - Each hour's SQL Server and Operating System stacks are drawn between the hour's tick and the next, so the first hour is no longer cut by the axis start, the newest hour no longer reads as empty, and no hour's count is drawn in the slot before it. The hours are binned in the display zone, so a half-hour zone lines up with its ticks too. Lite and the Viewer share the one calculation.
- **Lite's Job History and the Darling Viewer's status bar read the same clock as the grids beside them, and say which** ([#8102]) - Job History's picker and "Showing since" note follow the Times setting for All Servers the way Alert History does, and for one server read UTC or local time when that is chosen. The Viewer's status bar stamps each refresh with its zone like the tabs' "Last refreshed" strips, and Database Protection's capture time is stamped to the second with its zone instead of a short date.
- **Server tab grids keep their figures on screen at common window widths** ([#8099]) - Query Store Health puts Stale Days, Max Plans/Query, Interval Min and Size Cleanup right after State, sizes State and the usually blank Read-Only Reason to their content, and moves the reason to just before the capture modes, so the settings and usage fit without a sideways scroll. Top Queries puts Executions, Total CPU, Avg CPU, Total Duration and Avg Duration ahead of Query Text. Active Queries, Top Queries, Query Store, Plan Corrections, Significant Waits and the comparison grids show Query Text as one trimmed 320 px line instead of a 90 px block of wrapped SQL, with the full statement in the tooltip. The same change applies to the Darling Viewer, its current active queries grid included.
- **FinOps grids show long counts and totals in full** ([#8113]) - Every FinOps grid in Lite and the Viewer sizes its count, total, size and cost columns to their content, so a nine-digit execution count, read total or CPU total is no longer clipped at the cell edge and read as a smaller number.
- **Nine small Darling Viewer details read right** ([#8115]) - The fleet's attention counts say how many of their servers are healthy with an advisory, the Server column in Alert History and Job History sizes to its names, a database with no collected used size says so under its bar, the Recommendations count says when it is loading, a resolution toast names the server once, the Settings dialog fits its tab headers, a read-only seat's monitoring fields refuse edits and Save says what it saved, the People window's empty state fits the seat, and Query Tag Usage with no tags is one sentence. Lite gets the grid, label, loading and tag changes too.
- **Job History at All Servers shows every server's newest runs, and two more Viewer details read right** ([#8133]) - The All Servers read guarantees every server with runs in the window its share of the row cap before the newest runs of the rest, so one busy server cannot fill the page and a fleet that fits under the cap is shown whole, in the Viewer and in Lite. A Fleet Sweeps watch row says its hits are the current run and its first-seen column is the first ever sighting, in the Viewer and on the web page. The fleet Overview says it is loading while its first read runs.
- **Top Queries by Duration and get_top_queries_by_cpu read the window once** ([#8136]) - The raw Top Queries read picks each group's newest statement text inside its ranking pass and resolves it for the page's rows only, instead of reading the window's rows a second time to find it. Same rows, less store work on a busy day.
- **The FinOps High Impact read looks up text and plans from one pick per hash, and Lite's read ships only the hashes it can keep** ([#8117]) - Darling's text and plan lookup picks each kept hash's busiest row on the base table and reads the payload dimension once for it, rather than joining every row of the hash to the dimension three times over. Lite's High Impact statement groups before it ranks, keeps the top N on each of its six measures in SQL and looks up sample text for those alone. Scores, shares and order are unchanged in both.
- **get_top_queries_by_cpu with roll_up_by_host_object reads the window once per ranking pass** ([#8138]) - The host-object lens picks each group's newest statement text inside its ranking pass and resolves it for the page's rows only, instead of reading the window's rows a third time to find it. Same rows, less store work on a busy day.
- **Grid number filters read fractional values correctly in comma-decimal locales** ([#8159]) - In Lite and the Darling Viewer, a column filter such as `> 10` on a column with fractional values compared the number through its text, so on a Windows culture that writes 1.5 as 1,5 the value was read as 15 and the wrong rows were kept or hidden. The filter now compares the number itself, and reads a typed value by the separators it carries: 1.5 and 1,5 are both one and a half, 1,234.5 and 1.234,5 are both 1234.5, and only a lone separator before exactly three digits (1.234 or 1,234) follows your culture.
- **PostgreSQL-target rates leave a collector gap out of the counter deltas** ([#8156]) - An analysis window with a collector outage of more than an hour no longer counts the outage's transactions, deadlocks, temp spill, checkpoints, WAL, I/O and buffer traffic against the seconds that exclude it, so the per-second and per-hour rates and the TPS trend halves read what the collector observed.
- **The pileup finding's baseline covers the 45 minutes it states on a busy server** ([#8157]) - The active-query window the same-statement pileup check reads keeps a row cap of 50,000 as a tripwire instead of 5,000, so a server with hundreds of active sessions no longer has its baseline cut to the newest few minutes, and when the cap does fill short of the baseline the check skips that sweep with a log line rather than state a baseline it never read.
- **Copy All Rows, Copy Row and Export to CSV carry the Query Stats bar columns** ([#8153]) - Grid copies and the CSV export now read a template column through its sort member or the text inside it, and follow a nested binding path, so the Executions, CPU, Duration and Logical Reads columns and Lite's FinOps lock-wait cells no longer come out blank in both apps.
- **Hourly counter rollups carry the hour boundary, optional manifests list their columns, and a missed daily snapshot is caught up** ([#8168]) - Each counter in an hourly rollup gets a delta_hour_bridged column that counts the step from the previous hour's last sample, optional manifest entries now list the columns of their files, an absent Change or Hourly table is listed as skipped, a daily table or the servers snapshot runs in a later window of the same day when the midnight run did not produce it, and a denied optional name is recorded under the scheduler's optional failures.
- **An optional table named in --tables gets its column policy and schema gate** ([#8167]) - The exporter now routes an optional-registry table named in --tables, or in the fresh family's table list, through the optional runner, so its column policy, schema gate and chunk guard apply and it is listed in the optional manifest. The scheduler warns about an optional table in families.fresh.tables instead of exporting it raw.
- **Deadlock rows name the locks for every lock kind, the CPU scheduler's CRITICAL exhaustion level is reachable, and alert streaks survive a DST change** ([#8160]) - In Lite and the Darling Viewer, a deadlock on an application lock, a metadata lock or a HoBt, allocation unit or filegroup lock now shows the object names and the owner and waiter modes, and `get_deadlock_detail` returns them. The CPU Scheduler pressure level reads CRITICAL whenever the collector's worker-exhaustion flag is set, where it read HIGH above 90% before. The alert streak in investigation briefs and in `get_alert_history`'s `streak_days` counts a fire an hour off when a DST change in the server's time zone fell between it and the alert, so a job scheduled in a server's local time keeps its streak across the change. The 30-minute window is unchanged, and a server with no stored time zone is read in UTC as before.
- **Latch and spinlock legends tell long names apart** ([#8186]) - A legend name up to 32 characters is kept whole, and a longer one keeps its head and its tail instead of being cut at 20 characters. The three ACCESS_METHODS_HOBT latch classes now read as three different legend entries in both apps, and no two series in one legend ever read the same.
- **The plan viewer's Properties panel counts estimated executions the way the arrow does** ([#8185]) - Est. Executions and Est. Rows All Execs now count one run plus every rebind and every rewind, the same figure the arrow tooltip shows for the operator. An inner-side spool with 10 rebinds and 990 rewinds reads 1,001 executions in both places instead of 11 in the panel.
- **A fully filtered grid says so, Sick Spinlocks ranks its five, and the crosshair and plan tooltips keep one format** ([#8187]) - A grid whose column filters hide every row now says "No rows match the column filters" with a link that clears them, in both apps. The Sick Spinlocks chart draws the five types with the most backoffs, busiest first, and its label says how many types there were, in both apps and on the web System Events tab. The Overview crosshair prints a strip's value the same way whether the strip holds one series or several. The plan node tooltip and the plan arrow tooltip print an estimated row count the same way.
- **Charts follow your color overrides, the hover keeps small values, and Lite's axis clock matches the hover** ([#8154]) - A per-user color override now reaches the chart figure, data area, legend, hover popup and the in-grid bar accent in both apps. The hover prints a small value at the precision it needs instead of 0.0. Lite's axis labels use the same 24-hour clock as the hover and the Viewer. A date column with a display format exports the date as the grid shows it.
- **Light and Cool Breeze no longer keep dark colors after a theme switch, in Lite and the Darling Viewer** ([#8221]) - The FinOps Recommendations "Detail" header stayed a black block, the Recommendations grid in Lite had no column filters, healthy fleet cards had a near-black border, buttons and text boxes had no visible outline, the Utilization bar tracks were invisible, Cool Breeze grids had strong banding and a white header, the Daily Summary Warning color and hint text differed or were hard to read, Lite's server tabs kept the Dark tab text and underline, and the pale green "Healthy" and "SYNCHRONIZED" text on Availability Group cards (and the Viewer's fleet card values) was hard to read on the light themes, and a blocking card looked the same as a stale one once it was darkened for the light themes. Headers, grids, pickers and tabs now follow the current theme (one shared refresh for both apps), card text takes the theme's own status colors on Light and Cool Breeze, a healthy fleet card has the theme's quiet border while Warning, Critical, stale and offline cards keep their colors with a blocking card staying apart from a stale one in Lite, controls and Utilization bar tracks have a visible border in both apps, and the calendar uses one Warning color.
- **The compose quick start names its server, and an unknown key in a darling.json server entry is named at start** ([#8239]) - The compose sample now sets the server's name with the key the file reads, so a first run no longer seeds the server under its host string. A key in a servers entry that no setting declares is reported at start by its path, and the sample files' comment keys are not reported.
- **The grant script and the permissions page give the monitoring login SELECT on msdb.dbo.cdc_jobs** ([#8243]) - msdb does not grant that read to public, so on a server with CDC configured the query snapshots collector raised a permission error every cycle and matched CDC capture jobs by name. The grant is guarded for a server that has never configured CDC, left out of the RDS form, and optional, so the connection test does not ask for it. The page also names pg_read_binary_file(text, bigint, bigint) for self-hosted PostgreSQL targets and says the sysalerts and sysnotifications grants are unused on Azure SQL Managed Instance.
- **An SMTP relay that keeps refusing alert email is announced** ([#8245]) - Three failed alert emails in a row raise Notification Channel Failing in Darling and the tray notice in Lite, as a failing webhook already did, and the next delivery announces the recovery. The text names email and never the relay or an address.
- **Webhook alerts wait out a short rate limit instead of failing, and a long one holds only its destination** ([#8244]) - A webhook endpoint that answers HTTP 429 with a Retry-After of up to 10 seconds is posted to again after the wait and counts as a failure only when the retry fails too. A longer Retry-After fails the post with the endpoint's answer logged and holds that destination alone. Per-event posts to one destination are a second apart, a Summary card never waits behind them, the channels of one alert post together, and Darling's fleet self-alerts no longer run on the collection launch loop.
- **An alert whose delivery threw is tried again instead of being counted as sent** ([#8246]) - Both editions report a delivery that threw as failed, so the engine retries it and puts its markers back. In Lite's Per-event mode, Long-Running Query, Volume Free Space, Database File Growth, Version Store (PVS) and the anomalous and failed job alerts now split per incident as Darling's do, and every per-event history row in both editions keeps the alert's measurement as its value instead of the incident's occurrence count, so a restart restores the free-space or PVS level it last announced. A route that turns email off sends no email instead of mailing the default list.
- **force_query_store_plan and unforce_query_store_plan refuse a database the plan was not collected in** ([#8234]) - Query Store ids repeat across databases, so a wrong database_name could force an unrelated plan on an unrelated query. The tools now refuse with plan_not_collected unless Darling collected that database, query_id and plan_id together, the message names the database it did collect the pair in, and the force runs in the database as the store spells it, so a name that differs by case alone on a case-sensitive server cannot reach another database.
- **kill_session honours the excluded-database list on SQL Server** ([#8235]) - The kill batch now reads the session's current database, and a session in a database the server's entry excludes is refused as database_excluded on a dry run and on a write, as it already was on Azure SQL Database. A session whose database the remediation login cannot name is refused as session_database_unknown while the entry excludes any database, and a session whose running request moved it to another database since the dry run is refused as database_changed by the kill batch itself. The session facts name the database the session is in, and a pressed proposal refused for its database ends superseded instead of waiting.
- **create_plan_guide names the missing grant for an OBJECT guide** ([#8236]) - A remediation login without ALTER on the module was told the statement did not appear in it, because the definition is not readable without the grant. The refusal now names the grant first.
- **The wait-rate anomaly and its baseline no longer read low when one wait type returns after missing some collections** ([#8106]) - The anomaly's window, the young-baseline bar and the stored WaitMsPerSec baseline credit each collection's positive-interval wait whole and rate it over the collection's cadence, its shortest measured interval, on SQL Server and PostgreSQL targets and in Lite, so the total equals the sum of its parts. The baseline moves to a successor aggregate that backfills from raw while the old one keeps serving until the successor covers the tier.
- **Plan force checks the query behind the ids, and an encrypted module's plan guide refusal says so** ([#8262]) - A force or unforce, and a person's undo of one, is refused when the query behind the collected ids is no longer the one on the server. A plan guide refused for a statement in an encrypted module says the module is encrypted and its text cannot be read.
- **A new Darling store whose database already has TimescaleDB no longer logs a false "NO CLEARANCE" compression warning on its first housekeeping pass** ([#8275]) - migration V23 no longer adds collection_log's compression policy, so the policy comes only from the start-up path that puts it on the hourly cadence and its phase minute. A store already past V23 is unaffected.

## [3.12.0] - 2026-10-30

Full entries: [docs/changelog/3.12.md](docs/changelog/3.12.md)

### Important

- **Upgrading from 3.11 runs the Darling store migrations V215 through V229 on the service's first start**
- **The Client Timeouts alert is on after the upgrade, in Darling and Lite** ([#5736])
- **Lite's DuckDB schema stays at version 78**
- **Self-managed stores: re-run `provision-roles.sql` after upgrading** ([#6542], [#6395], [#6386], [#6389], [#6392], [#6393], [#6394], [#6396], [#6397], [#6398], [#6400], [#6401], [#6454], [#6455], [#6569], [#6928], [#6743])
- **New msdb grants for existing monitoring logins** ([#6874], [#6873])
- **The Darling web dashboard and MCP server, and Lite's MCP server, need a token once turned on** ([#6507], [#6557])
- **Web dashboard Admin writes stay off after an upgrade until `web.allowAdminWrites` is true** ([#6600], [#6745])
- **Adding servers is stricter** ([#6583], [#6752])

### Added

- **Notification routes can match on alert severity** ([#6928])
- **Blocking colours follow settable blocking bands** ([#6927])
- **Alert threshold overrides can be set for a server tag** ([#6926])
- **The Darling grant script has an Amazon RDS form** ([#6889])
- **Darling warns before an identity column runs out of values** ([#6875])
- **Darling checks the SQL Server Agent alert setup** ([#6874], [#6894], [#6908])
- **Darling watches backups and CHECKDB per database** ([#6873], [#6902], [#6946])
- **Darling reads PostgreSQL server logs written in a named local log_timezone** ([#6866])
- **A Darling docs page lists every MCP tool and the store tables it reads** ([#6853])
- **Darling can front several stores with one web address and one MCP endpoint** ([#6840], [#6917])
- **The Darling viewer's Collector Schedules window has a Find box and a Disabled only filter.** ([#6768])
- **`get_job_history` takes a `min_duration_seconds` filter, and I/O anomaly briefs name it.** ([#6614])
- **A signed-in web dashboard admin can make Admin writes on the managed store, once `web.allowAdminWrites` is on** ([#6600], [#6745])
- **Darling now collects Query Store hints, lists them with `get_query_store_hints`, and Change Watch sees one set or cleared.** ([#6581])
- **Agents can recompile a module and create or drop a plan guide through Level 3 server actions, with Undo.** ([#6569])
- **A request's brief names the earlier report with the same root cause** ([#6565])
- **Client Timeouts alerts name the client host and carry a client_timeouts evidence section** ([#6553])
- **Darling MCP supports named tokens with roles, and an operator token can set a notification route's agent settings** ([#6542], [#6395])
- **The Darling install scripts work with the MSI package and refuse a machine that the MSI owns** ([#6480])
- **The Darling installer offers to add you to the operator group** ([#6478])
- **Darling prints the monitoring login and grant script, and the connection test names missing grants** ([#6472])
- **The Darling zip ships Install-Darling.cmd, and the installer offers to restart elevated** ([#6469])
- **A Darling page on how to back up, restore and move the store** ([#6411])
- **Darling MCP write tools return the previous value and an undo call** ([#6403])
- **Darling MCP get_server_settings reads what edit_server changes** ([#6402])
- **Darling MCP can switch agent actions off and undo a non-plan agent change.** ([#6401])
- **Darling MCP can write, approve and reject agent notes, close an investigation request, and handle practice requests and report feedback.** ([#6400])
- **Darling MCP has a purge_expired_mute_rules write tool** ([#6399])
- **Darling MCP can add, edit and delete known patterns.** ([#6398])
- **Darling MCP can read and set database state expectations** ([#6397])
- **Darling MCP can set a collector's database scope and reset a scope's schedules** ([#6396])
- **Darling MCP can make and remove webhook signing keys, for the default webhook and for each route.** ([#6394])
- **Darling MCP can set the service flags** ([#6393])
- **Darling MCP can set the default notification channels** ([#6392])
- **The Darling viewer shows Fleet Sweeps, alert rules and Custom Views** ([#6390])
- **The web dashboard Recommendations tab has Generate now and Ask AI** ([#6389])
- **The Darling viewer shows Resource Semaphore, Plan Cache by Type, Sessions by Application and more panels that the web dashboard already had** ([#6388])
- **The Darling viewer shows PostgreSQL configuration changes, the logging audit and deadlock graphs** ([#6387])
- **The web dashboard Active Queries panel can read the running requests live** ([#6386])
- **The Darling viewer charts the four PostgreSQL trends** ([#6385])
- **The Darling viewer shows Sweep Pressure and Heaviest Collectors panels** ([#6384])

### Changed

- **Recommendations say "times its usual variation" instead of "robust σ"** ([#6925])
- **Updated to .NET SDK 10.0.403, DuckDB.NET 1.5.6.1 and newer AWSSDK and ProtectedData packages** ([#6915])
- **Client Timeouts leaves out the VS Code mssql extension's metadata cancels** ([#6856], [#6858])
- **get_forced_plans and get_query_store_hints read each first-seen time in one pass** ([#6833], [#6876])
- **The 31-day daily summary keeps closed days until the next UTC day plus 2 hours** ([#6809])
- **get_query_store_clutter and the Darling viewer's Query Store clutter view are faster on large servers** ([#6808])
- **Darling viewer Top Queries by Duration looks up modules only for the ranked statements.** ([#6804])
- **FinOps High Impact and Optimization are faster, with no temp spill.** ([#6803])
- **The Darling fleet `get_resource_use_by_tag` read is much faster: it reads narrow hourly and daily tables and is served from the read cache** ([#6800], [#6794], [#6865], [#6945])
- **Darling's Job History page reads its per-job statistics from a new index** ([#6798])
- **`--backfill-rollups` and `--recompress` can take the store's free space, and the data volume is measured on Linux.** ([#6791])
- **Query Store regressions list a query for cost only when it added at least 1 second of CPU** ([#6781])
- **A re-fire of an alert now joins a request that is already reported, so it opens no new request.** ([#6778])
- **The Darling viewer and Lite Running Jobs tabs now show a short note when the collector has not run.** ([#6764])
- **Four MCP empty answers now say what kind of nothing they are.** ([#6757])
- **Web server add and edit now refuse Windows sign-in over an unverified certificate unless the operator allows it, as MCP does.** ([#6752])
- **`analyze_server` now ranks a long blocking chain by how long it waited and says when it is still open.** ([#6746])
- **Self-managed store upgrades now say to re-run provision-roles.sql, the two CLI verbs accept the same `--auth` words, and a per-server collector retention is refused.** ([#6743])
- **`submit_investigation_report` now tells an agent that `family_of` needs the full request id.** ([#6729])
- **The Darling upgrade script now prints a post-upgrade checklist that an operator can work.** ([#6724])
- **Five MCP tool descriptions are fixed where agents guessed wrong** ([#6713])
- **The Darling viewer and Lite match the web dashboard on dropped databases, labels, job history, wording and strip gaps** ([#6651])
- **The health parser says how many events were read when it finds none that qualify** ([#6650])
- **The hourly Darling aggregate compression check reads sizes only for aggregates that have no policy** ([#6648])
- **get_store_log gives the TimescaleDB poor compression ratio warning its own class** ([#6647])
- **The PARAMETER_SENSITIVITY and QUERY_SPILLS findings now name the queries behind them.** ([#6640])
- **Store migration V215 adds six indexes on the Darling investigation tables** ([#6637])
- **The investigation resolve check is about 5 times faster in Darling and Lite.** ([#6633])
- **The Blocking Incidents chart says why it is empty when lock waits exist** ([#6622])
- **Darling opens no investigation request for a story that recurs at this hour** ([#6596])
- **Darling and Lite MCP tool descriptions are reworded where investigation agents went wrong** ([#6589])
- **A SQL Server port is refused on new entries and warned about on existing ones** ([#6583])
- **get_forced_plans pages its list and filters by query_id and plan_id** ([#6560])
- **The Darling web dashboard and MCP server, and Lite's MCP server, need a token once turned on** ([#6507], [#6557])
- **The Darling command line tests Windows authentication servers as the service** ([#6476])
- **--enable-web and --enable-mcp print the address and the next step** ([#6474])
- **The Darling viewer finds darling.json through the installed service** ([#6473])
- **The Darling Windows zip no longer needs a separate .NET runtime** ([#6471])
- **The Darling installer waits until the service is ready and then prints the next steps** ([#6470])
- **Darling starts with no monitored servers, and a first install needs no hand-edited darling.json** ([#6468])
- **Darling says a Query Store replica has no effect on Azure SQL Database** ([#6459])
- **The troubleshooting page is a symptom table for Lite and Darling** ([#6422])
- **The Lite README is a short front door, and the Lite guide puts install and configuration first** ([#6419])
- **The Darling install docs now tell one story** ([#6417], [#6418], [#6409], [#6410], [#6429], [#6430])
- **The Darling documentation is reorganized into task pages with an index** ([#6416], [#6420])
- **New Darling pages for day-to-day admin tasks and for every service command line verb** ([#6412], [#6413])
- **The Client Timeouts alert is on by default in Darling and Lite** ([#5736])
- **The Query Store regressions read and `get_collection_health` are much faster on large Darling stores, and the regressions read no longer misses cost regressions on long windows** ([#5506], [#6750])

### Fixed

- **The Darling web dashboard keeps the page on screen while it auto-refreshes** ([#6933])
- **Blocking chains in analysis no longer link unrelated sessions that reused a session number** ([#6931])
- **The Darling web Overview strips draw at full size, and the Daily Health Calendar fills its card** ([#6929])
- **Saving an edit to an existing notification route from the Darling web page works again** ([#6928])
- **The Darling upgrade script runs under PowerShell 7** ([#6920])
- **Unregistered external alerts from different sources or labels stay in separate investigation requests** ([#6919])
- **Idle servers answer empty, not unavailable, for query_stats and procedure_stats reads** ([#6903])
- **The new Agent Alerts, Database Protection and Identity Capacity views read cleanly** ([#6901], [#6906], [#6907])
- **Azure SQL Managed Instance system_health events are collected again** ([#6897])
- **The blocking snapshot no longer names its own session as the blocker** ([#6896])
- **Extended Events captures that cannot run are no longer counted as healthy or as down** ([#6893], [#6912])
- **Darling no longer writes its own ERROR lines into a monitored PostgreSQL server's log** ([#6888])
- **Azure SQL Managed Instance now reads SQL Agent as running** ([#6886])
- **The Darling viewer fleet card shows the whole Collectors line, and query tag text uses words** ([#6878])
- **Deadlocks and blocked process reports are collected again on Azure SQL Managed Instance** ([#6868])
- **The tempdb tab explains why it is empty in Lite and the Darling viewer** ([#6861], [#6869])
- **get_job_history and get_resource_use_by_tag answer with the display name, not the connection key** ([#6860])
- **Lite logs a permission-denied collector run as one Warning** ([#6859])
- **A missing Resource Use By Tag source is named in the same words in Lite, the Darling viewer and the MCP tool** ([#6850])
- **PostgreSQL 10 to 13 targets store statement stats and wait samples correctly** ([#6836], [#6849])
- **The slow-read log records the server when the server id is negative** ([#6834])
- **Charts keep a plot at least 100 pixels tall at 1366x768, and axis titles stay clear of tick labels** ([#6832])
- **Four Darling web pages use the page's own words instead of MCP text** ([#6831])
- **get_finops refuses an unsupported hours_back for the utilization and index_analysis views in the view's own words** ([#6830])
- **get_query_store_regressions explains a baseline or replay window that is older than the kept data** ([#6829], [#6877])
- **Collector counts agree on every Darling surface, and a refused collector names the grant** ([#6828], [#6932])
- **Health-parser reads check for a truncated ring buffer over their own window** ([#6827])
- **Save is visible in the Darling viewer's Add and Edit server dialog at its default width** ([#6823])
- **The Darling upgrade script takes a new rollback backup when the last upgrade finished** ([#6821])
- **Darling Query Store panels over a window older than 3 days are no longer refused by the row guard** ([#6807])
- **The slow-read log now keeps a read's slowest statements, and records a web read that the browser cancelled.** ([#6795])
- **Darling web pages cancel their reads when you leave the page** ([#6793])
- **The Overview tooltip's baseline arrow now shows only for the first series of a chart row.** ([#6787])
- **The Wait Drill-Down banner is now readable on the Light and Cool Breeze themes.** ([#6773])
- **A server's own Capture execution plans setting now takes effect.** ([#6771])
- **The Darling viewer logs a failed schedule read once, not once per waiting caller.** ([#6766])
- **Investigation blocking evidence no longer counts parallel self-waits, keeps the head blocker's facts, and honors the high-memory signal.** ([#6761])
- **Darling web: an empty Add route or Write note says what is missing without a request, and Running Jobs and Recommendations text reads plainly.** ([#6760])
- **Four Darling web pages no longer say two things at once.** ([#6759])
- **Darling viewer fixes to the grant script account, PostgreSQL notes and sidebar buttons.** ([#6758])
- **Three web layout fixes in the Darling dashboard.** ([#6756])
- **Lite keeps its first-run server step until the servers are bound to their settings.** ([#6755])
- **Query Store reads now give the right status word on a Secondary role, with Query Store off, and for an empty forced-plan window.** ([#6749])
- **Stopping the Darling service during a pass no longer logs a false alert-pass warning.** ([#6748])
- **The viewer now shows fleet-wide times in a fleet-wide zone, and each server tab's times in that server's own zone.** ([#6747])
- **The generated grant script and the permissions doc now loop databases with a cursor, not `sp_MSforeachdb`.** ([#6741])
- **Journal rows for refused agent actions, plan undo and operator route changes now record the right caller.** ([#6740], [#6751])
- **An investigation claim's later calls are now tied to the named token that made the claim.** ([#6738])
- **Plan guide drop answers now follow `mcp.dataDetail`, and forcing or unforcing a Query Store plan honors the excluded database list.** ([#6737])
- **Choosing the web dashboard option in the Darling MSI now makes a web token, so the dashboard starts.** ([#6736])
- **A server action switch no longer turns on for an Azure SQL target whose tool always refuses the action.** ([#6727])
- **A generic webhook can now close an alert, and a PagerDuty resolve now closes the incident the trigger opened.** ([#6718], [#6723])
- **Darling now copies the generic webhook, PagerDuty and send-resolutions settings from darling.json on its first start.** ([#6715])
- **Darling on plain PostgreSQL: the blocking and deadlock baselines stop at the purge horizon** ([#6706])
- **Alert texts use the right word for a count of one** ([#6700], [#6703])
- **ANOMALY_WRITE_LATENCY is scored against a write latency baseline** ([#6696])
- **Chart text keeps its size at high Windows display scaling** ([#6678])
- **Darling's Query Store reads no longer list the monitor's own statement or statements from dropped databases** ([#6654], [#6659])
- **Index Analysis no longer shows two banners that contradict each other, and two Settings and plan card labels are corrected** ([#6653])
- **The Darling viewer charts and grids stay readable on a 1366x768 screen** ([#6652])
- **Web dashboard pages say MCP field names and raw collector counts in plain words** ([#6649])
- **The right-sized verdict sentence says what decided the verdict** ([#6646])
- **A parallel query that waits on itself is no longer counted as blocking** ([#6645], [#6690])
- **A store that the operator brings now works for every MCP tool and web page that takes a server name** ([#6630])
- **Darling logs a usable web dashboard and MCP address when it listens on a specific IPv6 address** ([#6609])
- **Darling Query Store query history now reports a plan whose force was removed as REMOVED.** ([#6598])
- **The Darling install script registers the service path in quotes** ([#6597])
- **An agent that holds the live lease gets its claim_id back when it claims again** ([#6576])
- **Change Watch no longer counts log_reuse_wait_desc as a database configuration change** ([#6566])
- **get_change_watches accepts state=all, sends each fact once, and can read one watch by watch_id** ([#6561], [#6591])
- **A Darling Change Watch no longer calls a plan mix shift a regression** ([#6555])
- **Query Store reads on a readable secondary say it is not collected, and why** ([#6554])
- **The Client Timeouts alert leaves out SSMS IntelliSense's own cancels** ([#6552])
- **Change Watch requests get their own family, skip replicated plan changes, and name the database** ([#6551])
- **Change Watch says whether a plan was forced by automatic plan correction** ([#6550])
- **SQL Server's own cleanup statements are left out of regression targets, findings and Query Store reads** ([#6548])
- **The brief keeps the request's only alert when its text is long** ([#6547])
- **Darling table tuning waits at most 5 seconds for a lock** ([#6539])
- **Darling investigation requests from analysis findings get the resources and jobs families, not other** ([#6536])
- **Lite asks again before it uses a saved password for a changed server** ([#6504], [#6559])
- **Lite clears the Blocking and Deadlocks strips when the window has no events** ([#6482])
- **The Darling installer tells the truth when it stops, catches an unedited sample, and puts shortcuts where every user sees them** ([#6475])
- **The investigation brief lists the arguments that submit_investigation_report really takes** ([#6462], [#6546])
- **Darling builds all three large start-path indexes during the first start of an empty store** ([#6460])
- **Lite no longer logs a notification_routes warning on its first start** ([#6458])
- **Fleet-wide windows in the Darling viewer show times one way, and Azure SQL Database notes lead with what the engine lacks** ([#6457])
- **Darling investigation event webhook posts now send the default User-Agent** ([#6456])
- **Darling MCP write tools give honest statuses and clearer errors** ([#6455])
- **Darling server admin tools are current at once and clean up after remove_server** ([#6454])
- **Reads with nothing stored say empty when the collector ran clean** ([#6453])
- **The system_health collector keeps reading new events when the ring buffer is full** ([#6441], [#6544], [#6564])

## [3.11.0] - 2026-10-22

Full entries: [docs/changelog/3.11.md](docs/changelog/3.11.md)

### Important

- **Upgrading from 3.10 runs the Darling store migrations V174 through V214 on the service's first start**
- **Upgrading Lite from 3.10 moves its DuckDB schema from version 67 to 78 on the first start**
- **Self-managed stores: re-run `provision-roles.sql` after upgrading** ([#6066], [#6254], [#6383], [#6334])
- **A remote SQL Server reachable only over a named pipe no longer connects from the Darling service or Lite** ([#6352])

### Added

- **Investigations panel in the Darling viewer and Lite** ([#5907], [#6265])
- **Agent notes can be approved or rejected in the Darling web dashboard and in Lite** ([#5908])
- **A new Darling MCP tool, `backtest_custom_alert_rule`, shows where a custom alert rule would have fired and cleared over stored history** ([#5919])
- **Store schema for Level 2 agent investigations** ([#5920])
- **The alert engine can take per-server and per-database threshold overrides** ([#5921])
- **Investigation reports can record Jira, pull request and bug report links** ([#5922])
- **The Level 2 agent action gate and its journal** ([#5925], [#5632])
- **People can give feedback on investigation reports** ([#5926])
- **Lite: Level 1 agent investigations** ([#5820])
- **Deadlock graph window: a visible Copy button for the SQL text** ([#5663])
- **A Runnable Queue alert warns when runnable tasks per scheduler stay high** ([#5641])
- **Mute rules can match by client application** ([#5714])
- **Other services can raise alerts through Darling** ([#5726])
- **Enter opens a server tab** ([#5764])
- **Darling records the collection slots it stepped over** ([#5741])
- **Darling warns at start when the store was migrated by a newer build** ([#5773])
- **A notification route can be limited to one server or to a tag, in Lite and Darling** ([#5769])
- **Read latency capacity fields** ([#5792])
- **Read caller** ([#5795])
- **A store sizing guide for Darling, and a load-test harness for the Query Store tables** ([#5800])
- **Darling limits how many long reads run at once** ([#5806])
- **Darling can hand an alert to your own AI agent** ([#5632], [#5850], [#6066], [#6237], [#6273])
- **Alert Details window: resizable, selectable values, Copy all details, and a link to the full chain** ([#5854])
- **MCP long-poll: an agent waits for an investigation request to open for its routes** ([#5856])
- **Alert resolutions go to the channels that got the fire, opt-in per channel** ([#5855])
- **Generic webhook: {{investigation_json}} carries the alert's investigation request** ([#5853])
- **Lite: Level 1 agent investigations** ([#5902])
- **Practice requests with a score per agent** ([#5937])
- **Agents at Level 2 can change mute rules and custom alert rules, journaled** ([#5934])
- **Per-server alert threshold overrides** ([#5933])
- **Agent changes list with one-click undo and an off switch** ([#5936], [#6271])
- **CI lints the workflow files with actionlint** ([#5947])
- **Tests wait on conditions, not fixed sleeps, and CI failures are counted apart from the skip marker** ([#5943])
- **CI can set aside a test that dev's own latest push broke, for up to 72 hours, on pull requests that do not reach it** ([#5942])
- **Run all collectors now, or one collector now, for a server** ([#5941])
- **Client Timeouts measurement scripts** ([#5736])
- **Client Timeouts bars are now relative to each server's own normal rate** ([#5736])
- **Alert recurrence in investigation briefs** ([#6000])
- **Query tag rule MCP tools** ([#5982])
- **Lite has a query-tag store** ([#5989])
- **Tags on the Darling query reads** ([#5984])
- **Query tag MCP tools** ([#5981], [#6027])
- **Investigation reports carry a family, fix links, evidence basis and the queries run** ([#6004])
- **Query tag map built by the collector** ([#5985], [#5971])
- **Investigation evidence is frozen at the open** ([#6001], [#5997])
- **Lite now records which queries it saw and builds the query tag map** ([#5990])
- **Resource use by query tag** ([#5983])
- **Signed webhooks** ([#5998], [#5997])
- **Lite can roll resource use up by query tag** ([#5992])
- **Investigation evidence now covers deadlocks, blocking and plan regressions** ([#6031])
- **Lite MCP tools to manage query tags, assignments and rules** ([#5991], [#6027])
- **Lite can manage query tags and see resource use by tag** ([#5993])
- **Investigation request events reach the agent and its people** ([#6032])
- **Agents can read an investigation's frozen evidence** ([#6033])
- **Notification routes can now set a fallback route** ([#6036], [#6447])
- **The Darling viewer shows query tags: a Tag column and filter on the Queries grids, and a by-tag resource view** ([#5988])
- **Web dashboard: a Query Tag Usage page, and a tag column and filter on the query grids** ([#5987])
- **The web dashboard can manage query tags, their hand assignments and their rules** ([#5986], [#5971])
- **Investigation briefs hint at a plan change behind high CPU, and the reverse** ([#6007])
- **Investigation reports can schedule a follow-up request** ([#6008])
- **Lite investigations freeze evidence sections per request** ([#6010])
- **Deadlocks get a signature, a daily rollup, an agent gate and a 4-hour digest** ([#6005])
- **Investigation follow-ups can wait for a module deploy** ([#6009])
- **Lite investigation requests journal a quiet join and wait for every metric to resolve** ([#6010])
- **An agent can merge two deadlock signatures, journaled and undoable** ([#6006], [#5997])
- **Lite investigations carry server names, time-of-day recurrence and the new report fields** ([#6011])
- **Lite opens time follow-ups and signs generic webhooks** ([#6012])
- **An MCP read checks a Query Store plan before anyone forces it** ([#5632])
- **Darling collects each database's forced plans** ([#5632], [#6221])
- **Four small MCP read fixes for agents** ([#6074])
- **Darling ranks queries by aborted and failed executions** ([#6073])
- **Level 3 routes, and a per-server switch for each server action, each starting off** ([#5632], [#6271], [#6282], [#6281], [#6275], [#6285], [#6339])
- **Darling says whether each forced plan works** ([#5632], [#6160], [#6221])
- **Darling says who drove the load on a server** ([#6072])
- **Agents can force and unforce a Query Store plan on SQL Server** ([#5632], [#6207])
- **Darling checks every plan an agent forces, and asks the agent to keep or remove it** ([#5632])
- **A person can undo an agent's plan force or unforce from the Agent changes list** ([#5632])
- **An agent can evict one cached plan, then see its next compile** ([#5632], [#6207])
- **The blocking capture records what a person needs to judge a head blocker** ([#5632])
- **An agent can kill one SQL Server session, after checking it is still the session it named** ([#5632])
- **Volume Free Space alert: a fleet-wide excluded volumes list** ([#6098])
- **Sibling server tags can be put in order with Move Up and Move Down** ([#6111])
- **Lite answers "which database, which caller, which statement piled up" the way Darling does** ([#6124])
- **The Blocking grids show the head blocker's login time, last request end, open transactions and log used** ([#6146])
- **An agent can end one PostgreSQL session with `kill_session`, after Darling checks it is still the session the agent named** ([#6126], [#6143])
- **kill_session works on Azure SQL Database** ([#6184])
- **Custom views and notebooks can sit in shared folders and be personal** ([#6214])
- **Custom alert rules can expire** ([#6235])
- **Export the investigation cases that have feedback** ([#6236])
- **Darling MCP can now stop, start and reschedule a collector** ([#6227])
- **A recorded change opens a watch window, and an investigation opens only if something moved** ([#6234])
- **Darling MCP can pause collection, disable a server and change its excluded databases** ([#6230])
- **An agent with MCP write access can test a server connection, run an analysis pass, start a purge and undo a plan change** ([#6230])
- **Quiet windows: no alerts for a server or the fleet for a set time** ([#6231])
- **An MCP client can create and edit notification routes and dismiss alert history** ([#6231])
- **MCP route tools set a route's own destinations** ([#6241])
- **The web dashboard can create, edit and delete agent routes and edit environment profiles** ([#6264])
- **The web dashboard charts file I/O throughput, tempdb size and I/O, and collector duration** ([#6296], [#6297], [#6298], [#6443])
- **The web dashboard compares Top Queries, Top Procedures and Query Store with an earlier period** ([#6300])
- **The web Blocking page shows the lead-blocker tree of a blocked process report** ([#6292])
- **The web System Events tab charts the corruption and contention counters** ([#6295], [#6443])
- **The web Active Queries tab picks a stored snapshot** ([#6302])
- **Alerts show how many days in a row they fired at the same time of day** ([#6261])
- **The web server page shows the Daily Health Calendar as a month calendar** ([#6294])
- **The web dashboard draws the Query Heatmap as a colour grid** ([#6293])
- **The web Memory tab charts memory grant sizing and activity, and `get_memory_grants` can return the same series** ([#6299])
- **The web dashboard opens a per-row history for Top Queries and Top Procedures** ([#6303])
- **The web Overview draws five strips on one time axis** ([#6291])
- **Read Query Store from a readable replica, and stop losing late rows from a capturing secondary** ([#6254], [#6279])
- **The Viewer's Collection Health tab can run collectors now** ([#6382])
- **Darling MCP can now undo a finding mute with `unmute_analysis_finding`** ([#6383])
- **The web narrows its grids with a time-range slicer** ([#6301])
- **Registrations that report the same machine are now flagged** ([#6351])
- **Known-pattern notes show beside the alerts they match** ([#6334])

### Changed

- **Plan analysis and exports are bounded, spreadsheet-safe and warn on unencrypted mail** ([#5699])
- **darling.json and the store's admin and viewer credential files are limited to a local operator group** ([#5700])
- **A token in the web dashboard's address no longer signs anyone in; the dashboard and MCP host take the access token only from the sign-in form or bearer header, with a minimum length and a wait after failures** ([#5702])
- **harden-files and the install scripts cover TLS key files** ([#5700], [#5753])
- **get_collection_health partial rows leave out null, false and 0 keys, except collector, status, partial_detail, total_runs and errors, which always stay** ([#5748])
- **MCP tool descriptions and server instructions use plain wording** ([#5779])
- **The release signature guard checks the Darling PowerShell scripts** ([#5799])
- **Settings window: tabs grouped by task, in both the Darling Viewer and Lite** ([#5852])
- **The hourly store self-metrics sizing reads every hypertable in one catalog pass** ([#5885])
- **The repository's top page is now a short README with a quick start and links** ([#5869])
- **The hourly store self-metrics sweep sizes hypertables from the compression catalog** ([#5901])
- **CI finds the tests that read the other product's files by scanning the test sources** ([#5961])
- **CI now lints the shell scripts in the workflow files** ([#5970])
- **Server tag rules shared** ([#5978])
- **Test selection for shared projects** ([#5951])
- **Server names in investigations** ([#5999])
- **A PostgreSQL server's tabs read plainly in the Darling Viewer** ([#6151])
- **Web page text reads plainly** ([#6172])
- **The deadlock graph's detail pane shows each process's sql_handle** ([#6209])
- **The Query Store regressions read shows Regular, Aborted (timeouts) and Exception (errors) runs side by side, and flags a rise in timeouts or errors as well as cost** ([#5506])
- **The Query Store collector fetches text and plans only for each database-hour's top N plans, N a setting (default 100)** ([#6211])
- **The web Alert History grid puts Triage and Mute in the first columns** ([#5705])
- **PR CI runs the Guard tests beside the shards instead of before them** ([#5952])
- **PR CI builds the two test projects in parallel** ([#5955])
- **Darling alerts run on their own 30 second beat and show how old their newest sample is** ([#5680])
- **Lite's Query Store regressions now show timeouts and errors beside cost, and no longer report logical reads** ([#6308])
- **The PLAN_REGRESSION drill-down reads closed days from the per-day totals** ([#5794])

### Fixed

- **The Darling viewer's Manage Servers window now shows the service's version, once, instead of its own version on every server row** ([#5668])
- **A collector that recovered no longer stays at WARNING** ([#5665])
- **The upgrade and installer accept a hand-named `darling.json.bak-*` backup in the install folder** ([#5659])
- **The installer and the upgrade now compare a folder the same way whether it is typed as a short path, with a trailing backslash or in another case** ([#5682])
- **Get Actual Plan now warns before it re-runs a query that changes data, and the prompt shows what will run and where.** ([#5687])
- **The Darling viewer's Get Actual Plan runs only the query text you confirmed, and Lite FinOps no longer shows the previous server's rows after a server switch.** ([#5687])
- **The Get Actual Plan prompt shows a safer copy of the query text.** ([#5687])
- **Lite's one-time Query Store repair no longer runs its archive scan twice at startup or fails when the disk is nearly full.** ([#5690])
- **Dismiss All no longer blames archival when the local database is busy, the log now says what held it, and changes you make now wait up to 30 seconds for the database** ([#5688])
- **Startup Query Store repair now logs its progress lines in order, before it returns** ([#5694])
- **PLAN_REGRESSION no longer reports a query whose slower plan only reflects a different input** ([#5631])
- **Role provisioning stops before it sets a password when it cannot change the roles, and a store connection string that fails is reported without its value** ([#5697])
- **PostgreSQL log, deadlock and plan-capture text keep row values out of the stored text** ([#5698])
- **Job History "All Servers" no longer hides failures that one server's view shows** ([#5708])
- **A store host with about 4 GB of RAM no longer reports `shared_buffers` as stale when its hardware has not changed** ([#5720])
- **The raw procedure duration trend reads one small totals row per database per collection instead of every procedure row, and the duration trend tools pick one data source for a 95 hour window** ([#5706])
- **The CPU scheduler collector writes one row per collection, and readers combine the split rows already stored** ([#5712])
- **Long-query trace rows no longer all show query_hash 0 as one query** ([#5721])
- **`get_store_host` no longer fails when the store's size reads are slow** ([#5718])
- **The Darling viewer's query duration, procedure duration and execution-count trends no longer switch to hourly data on a window that starts 95 hours back** ([#5727])
- **The module_map refresh no longer warns every hour on a quiet fleet** ([#5729])
- **The Query Store per-database watermark read no longer scans hours of stats rows** ([#5707])
- **The web viewer no longer requests a missing `/favicon.ico` on first load** ([#5732])
- **Lite now logs and records a collection cycle it skipped** ([#5643])
- **Spill, cross join and anomaly advice text now says what the numbers are** ([#5704])
- **MCP empty answers now say the true reason, and min server memory 0 vs 16 is no longer a mismatch** ([#5704])
- **audit_config reports the server's real edition instead of calling Developer Edition "Enterprise"** ([#5704])
- **compare_analysis refuses overlapping windows and defaults the baseline from hours_back** ([#5704])
- **get_read_latency no longer reports a percentile above max_ms** ([#5704])
- **get_database_sizes shows percent growth, get_table_index_sizes sorts and reports unsized tables correctly, and get_index_usage no longer calls heaps and clustered indexes Unused** ([#5704])
- **FinOps gives one CPU right-sizing verdict per server, and it matches the Utilization view** ([#5704])
- **The MCP server inventory reports no cost as null with a reason, not 0** ([#5704])
- **FinOps wait categories put CXPACKET and hash waits under Parallelism** ([#5704])
- **Dropped databases no longer show as current in Query Store clutter, FinOps and top procedures** ([#5704])
- **Custom view panels accept a server's display name** ([#5704])
- **get_waiting_tasks shows one row per session and wait type** ([#5704])
- **Plan analysis no longer reports dop 0 for parallel cached plans** ([#5704])
- **Lite recommendations no longer list dropped databases** ([#5704])
- **Spill columns say they count 8 KB pages, and the spilling-query count counts queries** ([#5704])
- **Turning off the last enabled trace flag now shows up as a change** ([#5704])
- **A failed trace-flag read no longer reads as every flag turned off** ([#5704])
- **QUERY_HIGH_DOP counts distinct queries, not snapshot rows** ([#5704])
- **`analyze_server` through MCP no longer tries to write the collection-caveat table, so its permission-denied error is gone** ([#5704])
- **The managed PostgreSQL store no longer runs out of TimescaleDB background workers at start on hosts with the memory for them** ([#5704])
- **Slack and Teams alerts show alert text as plain text** ([#5726])
- **Lite housekeeping no longer holds up the collection loop** ([#5740])
- **A Warning that worsens to Critical is delivered at once for High CPU, Deadlocks Detected, Poison Wait and Volume Free Space** ([#5715])
- **Blocking counts one event per wait, not one per blocked-process report** ([#5705])
- **CPU samples are no longer stored twice** ([#5705])
- **FinOps "Top Databases by Avg CPU / Execution" lists the busiest database** ([#5705])
- **Version Store shows tempdb's current size** ([#5705])
- **Default Trace events are no longer repeated** ([#5705])
- **Default Trace events written late are no longer lost** ([#5705])
- **Memory tab shows the time of the peak grant** ([#5705])
- **The web dashboard's deadlock panels and blocking chart are labelled accurately** ([#5705])
- **Web dashboard labels match their values** ([#5705])
- **The web dashboard answers 400 to a non-integer `hours` (or other count) instead of silently using the default** ([#5705])
- **The restart banner no longer lists an unchanged server name as a change** ([#5705])
- **The web Job History grid no longer shows a "Last Success" time later than the run's own time** ([#5705])
- **The web Plan Corrections page shows one row per recommendation** ([#5705])
- **Web pages no longer show MCP-caller wording, and a normal zero is not "a look"** ([#5705])
- **Web dashboard charts break at collection gaps, long text cells show on one line, capped panels say so, and times use one format** ([#5705])
- **Custom Views count each Default Trace event once** ([#5742])
- **Lite's size-triggered database reset no longer pauses collection while it compacts the month's archive** ([#5771])
- **Lite saves webhook URLs of any length Windows allows, and says so when a save fails** ([#5772])
- **Archive export lock** ([#5790])
- **MCP tool count** ([#5791], [#5722])
- **System.Security.Cryptography.Pkcs is pinned to the patched 10.0.13** ([#5803])
- **A release no longer publishes the Linux tarball and container image before the Windows signature checks pass** ([#5805])
- **Query Store history for one query no longer reads its whole database** ([#5816], [#5807])
- **Rollup coverage floors no longer cost a cold sort on reads** ([#5818], [#5808])
- **get_query_store_top and the viewer's Query Store grid read the window once per call** ([#5827])
- **Query Store regressions read the per-interval table instead of re-sorting eight days of raw rows** ([#5826])
- **Collection log records each scheduled analysis pass, and skipped-slot rows can name it as the cause** ([#5828])
- **Slow-read rows record the window a read ran with, its default included** ([#5830])
- **Custom Views panels grouped by object name no longer sort a whole day of procedure statistics when the module map is current, and raw panels too large for the time limit are refused up front** ([#5832])
- **Plan regression reads no longer re-read three open days of Query Store intervals, and on-demand calls reuse the candidates for one analysis cadence** ([#5817])
- **A slow Query Store database no longer holds up collection** ([#5831])
- **Retention Held now fires for held retention policies on continuous aggregates** ([#5859])
- **get_collection_health reads its week of collection history from an hourly rollup** ([#5829])
- **The Query Store compose rollup stops retrying a failing hour every tick** ([#5871])
- **PR test selection no longer falls back to the full suite when the nightly failed on an unrelated job** ([#5954])
- **The plan regression drill-down no longer reads two weeks of Query Store rows per server** ([#5957])
- **The 24-hour Query Store top queries read touches fewer blocks on servers with short intervals** ([#5815], [#5882])
- **Ring-buffer collectors no longer fail forever on a deadlock involving a table or index with `&` in its name** ([#5948])
- **Darling no longer checks PostgreSQL targets for missing SQL Server capture sessions** ([#6042])
- **Module map repair runs once a day, not after every purge pass** ([#6044])
- **PostgreSQL statement-text due check reads one server's rows through a new index** ([#6043])
- **The docs give the right MCP tool counts, and say what a support subscription gets you** ([#6060])
- **`--backfill-rollups` runs on Linux** ([#6071])
- **The day-grain Query Store daily rollup fills again** ([#6070])
- **Darling fills a small rollup floor gap by itself** ([#6081])
- **The viewer's "darling.json not found" hint now points to the right file** ([#6103])
- **Job History sorts Duration by length and shows the full Message** ([#6105])
- **Query Store statement-text fetch now has a per-database time budget** ([#6102])
- **PostgreSQL grids in the Viewer sort size, duration, age, count and percent columns by number** ([#6104])
- **Excluded databases boxes take one name per line, so a name with a comma is no longer split in two** ([#6118])
- **Each release now ships the Docker Compose files as their own download, and the Darling README says which file each Linux shape needs** ([#6125])
- **`get_top_queries_by_cpu` no longer says "no query stats" for a window that stored snapshots** ([#6144])
- **PostgreSQL MCP reads use the documented status words, say when a server is offline, and name the server** ([#6145])
- **get_memory_grants and get_resource_semaphore no longer give a time to a zero peak, and say their peaks are sampled** ([#6148])
- **In the Darling Viewer, a PostgreSQL server's Vacuum tab shows several rows in every grid** ([#6142])
- **get_query_store_regressions now reports the true regression count** ([#6141])
- **analyze_server no longer labels a configuration finding as recurring, and no longer says collection "stopped or broken" after it has resumed** ([#6152])
- **A PostgreSQL finding's "Open in Active Queries" no longer opens SQL Server tabs** ([#6150])
- **Availability Groups and Query Tag Usage name servers by display name** ([#6162])
- **Text no longer cut off in Job History, Alert History, Settings, Manage Tags and the status bar** ([#6167])
- **A default Linux Docker Compose install needs one command for its secrets** ([#6140])
- **Web chart axes no longer repeat labels or cut off wide ones, and the System Events CPU chart covers the whole window** ([#6166])
- **PostgreSQL grids on the web server page keep short values in their cells** ([#6164])
- **In the Darling Viewer and Lite, Alert History and Job History read every row on one clock when they list several servers** ([#6161])
- **Query Store reads no longer say Query Store may be OFF before its first interval closes** ([#6187])
- **A long install path now explains itself instead of failing every SQL Server connection with "The type initializer for 'TdsParser' threw an exception"** ([#6192])
- **PostgreSQL monitoring connections pin search_path at sign-in** ([#6165])
- **Azure SQL Database procedures now keep their plans** ([#6193])
- **Azure SQL Database now says what it lacks** ([#6190])
- **FinOps no longer calls a server over provisioned from a handful of samples or from stolen memory** ([#6163])
- **Azure SQL Database: tempdb reads and Collection Health now say a contained user cannot read tempdb** ([#6185])
- **Three oversized MCP answers now stay near the response budget** ([#6149])
- **Web dashboard polish** ([#6168])
- **A screen reader or UI test tool can open a server from the sidebar or an Overview card, and the alert badge and Settings inputs have names** ([#6138])
- **An offline PostgreSQL target no longer reads as healthy** ([#6169])
- **Five MCP reads no longer answer past the 32 KB response target without saying so** ([#6195])
- **A collector the server keeps refusing is now tried less often** ([#6202])
- **The PostgreSQL "Test an index" check now runs under its own login, with only the grants planning needs** ([#6191])
- **MCP reads name an Azure SQL Database server by its display name** ([#6188])
- **A row with an empty cell no longer costs every server its first delta after a restart** ([#6212])
- **PostgreSQL collectors now work on older PostgreSQL targets** ([#6206])
- **PostgreSQL target connections run no type-loading query of their own first** ([#6143])
- **Scheduled reports no longer log as alerts, and a failing channel no longer logs a warning every hour** ([#6219])
- **The diagnostics bundle now finds a managed store that is starting or listening on another port, and says why when it cannot reach it** ([#6217])
- **The web deadlock graph's process pane wraps its SQL handle note and property lines** ([#6228])
- **A quiet server's top-queries reads say "nothing ran" instead of "unavailable"** ([#6232])
- **The PostgreSQL Activity grids show their values when a statement or log message has many lines, and the OS CPU grid shows rounded numbers** ([#6229])
- **Custom Views: a created, edited or deleted view reaches the sidebar and the list at once** ([#6243])
- **Darling Viewer: the Fleet Overview heading is no longer covered by its filter controls, and Help > About names the build** ([#6249])
- **A command nobody picks up within 15 minutes now expires instead of running later** ([#6251])
- **The hourly store-size row no longer walks every database file** ([#6250])
- **Plan correction and Query Store health skip a readable secondary replica** ([#6252])
- **A failed webhook's stored error now shows the HTTP status only** ([#6241])
- **Diagnostics bundle no longer rewrites known names inside ordinary words** ([#6258])
- **A fleet Query Store panel reads the interval table for the servers that can use it, and raw only for the rest** ([#6115])
- **A fresh install no longer logs a console error on Alert History, FinOps and Job History** ([#6280])
- **A change to a server's remediation login in darling.json now reaches a server that already exists** ([#6262])
- **The Overview Blocking & Deadlocking lane now shows a lone deadlock or blocking event as a visible bar, and hovering it reads 1** ([#6287])
- **"Sign in again" after a web dashboard session expiry now takes you to the sign-in** ([#6278])
- **A triage link clicked in Teams, mail or another site now opens signed in** ([#6286])
- **CPU scheduler collector counts offline CPUs and NUMA nodes correctly** ([#5657])
- **Azure error 40615 names the firewall and your client IP** ([#5658], [#5674])
- **A plain message when Windows is too old to start Darling and Lite** ([#5664])
- **A web page you leave mid-read no longer logs a failure** ([#5679])
- **Blocking snapshots no longer merge two separate waits of one pair** ([#5766])
- **Default trace events no longer time out on small servers** ([#6311])
- **Azure SQL Database: a connection outage is no longer logged as a missing XE session, and master is no longer a failed database** ([#6312])
- **MCP answers no longer misstate sub-millisecond CPU, the monitor's own xmin, the tag window minimum or a two-session move** ([#6305])
- **The Admin Agent changes list names the server, Admin tabs wrap, and the server page header stays current** ([#6310])
- **The web dashboard's sidebar no longer takes 260 px of a phone-width window** ([#6315])
- **The plan regression finding now also catches queries whose runs time out or fail** ([#6309])
- **The Query Store collector no longer joins Query Store views in one statement** ([#6257])
- **A server's host can no longer send Windows credentials over a named pipe** ([#6352])
- **The service and Lite no longer follow a mirroring partner a monitored server names** ([#6365])
- **The default trace collector no longer logs a spurious "unexpected end of trace file" error on SQL Server on Linux** ([#6381])
- **File I/O no longer invents reads and writes on Azure SQL Database Hyperscale** ([#6438])
- **Lite's Top Queries by CPU no longer ranks the whole procedure archive** ([#6096])
- **The web Job History Category list shows only the selected server's categories** ([#6443])
- **Editing or adding a server in the web dashboard right after another edit is no longer refused as busy** ([#6450])
- **`get_wait_trend` says `empty`, not `not_collected`, when a collected wait type had no waiting in the window** ([#6446])
- **The web dashboard no longer tells managed-store users to use an option their store refuses** ([#6451])
- **Known Patterns and Threshold Overrides server boxes show the server name, and the Metric box shows its value** ([#6452])
- **Viewer Job History can pick an Azure SQL Database, says it has no SQL Agent, and lists only that server's categories** ([#6391])
- **Collection health warns when an Extended Events ring buffer is too full to read** ([#6449])

## [3.10.0] - 2026-10-09

Full entries: [docs/changelog/3.10.md](docs/changelog/3.10.md)

### Important

- **Upgrading from 3.9 runs the Darling store migrations V158 through V173 on the service's first start**
- **On Windows, upgrading an install that ordinary users can write to stops once and prints steps to move it** ([#5627])

### Added

- **FinOps page shell in the web viewer** ([#4984])
- **Version Store (PVS) tab on the web FinOps page** ([#4993])
- **Database Sizes tab on the web FinOps page** ([#4994])
- **FinOps Locking & Contention tab** ([#4996])
- **FinOps Utilization tab on the web viewer** ([#4997])
- **Lite's `get_active_queries`, `get_waiting_tasks`, `get_query_store_regressions` and `get_query_heatmap` now say where their data starts** ([#5004])
- **FinOps high-impact queries over MCP and the web read route** ([#5009])
- **High Impact tab on the web FinOps page** ([#5019])
- **FinOps utilization over MCP and the web read route** ([#5023])
- **A daily collector can run at a time you choose** ([#5018], [#5007])
- **The tabs that show the newest snapshot now say when it was collected** ([#5032], [#5029], [#5038], [#5028], [#5047])
- **FinOps database resources over MCP and the web read route** ([#5025])
- **FinOps Server Inventory over MCP and the web read route** ([#5040])
- **Database Resources tab on the web FinOps page** ([#5041], [#5064])
- **Utilization tab on the web FinOps page** ([#5049])
- **get_finops gains an application_connections view** ([#5044])
- **The web viewer's FinOps page gains the Application Connections tab** ([#5055])
- **get_finops view index_analysis** ([#5056])
- **FinOps Server Inventory tab on the web viewer** ([#5058])
- **get_finops gains an optimization view** ([#5057])
- **The web viewer's FinOps page gains the Optimization tab** ([#5060])
- **get_finops gains a storage_growth view** ([#5062])
- **FinOps Index Analysis tab on the web viewer** ([#5066])
- **The web viewer's FinOps page gains the Storage Growth tab** ([#5067])
- **FinOps Recommendations over MCP and the web read route** ([#5074])
- **FinOps Recommendations tab on the web** ([#5075])
- **Mute Rules page in the web dashboard** ([#5087])
- **The web dashboard has a read-only Admin page** ([#5089])
- **Window pickers on the web FinOps Optimization and High Impact tabs, and the object-growth note** ([#5091])
- **Drag-to-zoom on web server-tab and FinOps line charts** ([#5092])
- **Version Store tab shows cleanup state and skipped pages** ([#5093])
- **Click a column header to sort any web dashboard grid** ([#5090])
- **Web server page: more columns on Query Store Regressions, Plan Corrections and Long Queries** ([#5099])
- **The store log keeps slow-statement plans** ([#5112])
- **Web Active Queries grid shows the desktop's columns** ([#5102])
- **Memory pressure chart and statement-eviction tile on the web dashboard** ([#5107])
- **Custom start and end on the web server page's Range picker** ([#5108])
- **`get_job_history`: SQL Agent job runs over MCP** ([#5095])
- **Web Configuration tab shows every Database Configuration column and the Scoped Configuration table** ([#5105])
- **Recommendations tab on the web server page** ([#5120])
- **Collection caveats on the web Collection Health tabs** ([#5123])
- **Web Alert History: time range, row limit, server filter and Show dismissed** ([#5109])
- **Web status bar shows seat, store size and collection state** ([#5116])
- **Server tags can be created, edited, deleted and assigned through MCP** ([#5088])
- **The managed PostgreSQL store logs the plans of its own slow statements** ([#5122])
- **Web Wait Stats chart charts several waits at once** ([#5119])
- **Copy and CSV export on web dashboard grids** ([#5110])
- **Server trends over MCP: total waits, CPU scheduler pressure, memory clerks and plan cache** ([#5117])
- **The web Database Sizes tab shows free space, growth, recovery model, VLFs and cost** ([#5121])
- **The web dashboard can create, edit, delete and assign server tags for an admin sign-in** ([#5137])
- **Per-process deadlock rows on the web Deadlocks tab** ([#5139])
- **Auto-refresh interval and Refresh button on web server and FinOps pages** ([#5141])
- **Manage server tags from the web dashboard** ([#5138])
- **Column toggles on the wide System Events grids** ([#5152])
- **Dismiss Selected and Dismiss All on the web Alert History page** ([#5151])
- **Job History page in the web dashboard** ([#5150])
- **Stored query plans on the web dashboard** ([#5156])
- **Web Blocking tab shows the desktop's blocking columns and saves report and deadlock XML** ([#5164])
- **Web Perfmon chart charts several counters at once** ([#5162])
- **Web line charts have a menu** ([#5166])
- **More server trends over MCP: latch, spinlock, session stats and collector duration** ([#5170])
- **Web Top Queries shows the desktop grid's columns** ([#5169])
- **CPU scheduler, memory clerk and plan cache trend charts on the web server page** ([#5161])
- **Per-column filters on the web grids** ([#5183])
- **Slow reads are recorded with their arguments, source and per-statement times** ([#5163])
- **Web Top Procedures shows the desktop grid's columns** ([#5186])
- **Web dashboard: favourites, acknowledgeable alert badges, severity colours and a collapsible sidebar** ([#5196])
- **Tempdb and file I/O trends over MCP** ([#5187])
- **Latch, spinlock and session stats trend charts on the web server page** ([#5189])
- **The Darling store keeps an hourly history of its own `pg_stat_statements` deltas** ([#5203])
- **Add monitored servers from the web dashboard's API** ([#5167])
- **Silence This Server on the web fleet page** ([#5221])
- **`get_store_query_history` reads the Darling store's own hourly statement history** ([#5217])
- **A diagnostics bundle for bug reports** ([#5218])
- **The web Collection Health tab opens a collector's run history** ([#5258])
- **The web FinOps storage growth view covers 7, 30 or 90 days** ([#5264])
- **Hide or isolate a series from any web chart legend** ([#5265])
- **The web Alert History shows an alert's advice and fix script** ([#5269])
- **PostgreSQL captured plans readable on the web** ([#5273])
- **Deadlock graph in the Darling web** ([#5275])
- **edit_server MCP tool** ([#5283])
- **Plan buttons on the web Active Queries, Query Store and Top Procedures grids** ([#5260])
- **The web Job History page says when SQL Agent is stopped** ([#5278])
- **Web charts can jump to the queries, blocking or deadlocks at a point in time** ([#5286])
- **MCP can serve HTTPS, and both listeners take a list of allowed ranges** ([#5295])
- **The web Query Store grid can show a plan's history over time** ([#5300])
- **The web's Top Queries and Top Procedures can rank by duration, reads or executions, and Top Queries no longer comes back short** ([#5299])
- **The web viewer shows the queries that waited on a wait type** ([#5322])
- **Edit a monitored server from the web API** ([#5301])
- **The web's Blocking and Deadlocks grids open each row's stored plans** ([#5298])
- **The web plan panel has a Repro script button, and MCP clients can fetch the same script** ([#5341])
- **The web server page can filter to several databases** ([#5314])
- **A repeated web query key reads as its first value** ([#5314])
- **A blank database_name means all databases** ([#5314])
- **The web Admin Servers grid can edit a configured server** ([#5356])
- **The Darling viewer's Overview now has a search box for server name or tag** ([#5359])
- **A SQL login password, the SMTP password, webhook URLs, the PagerDuty key and generic webhook headers can now be saved and used on Linux** ([#5387])
- **The Darling service now says when it was not collecting before a start** ([#5454])
- **PagerDuty can keep an outage and its recovery in one incident, and close it on recovery** ([#5368])
- **The Darling service pings a dead-man's-switch URL while it is collecting, so an outside check alerts when it stops or stops collecting** ([#5460])
- **The database filter reaches the configuration, config change, severe error and default trace reads** ([#5464])
- **A daily audit of the store's retained history raises one "Collection Gaps In History" warning** ([#5461])
- **Top Queries and Top Procedures can rank by reads from new hourly rollups** ([#5472])
- **Each PostgreSQL server in Darling can use its own AWS role** ([#5468])
- **Filter web grid columns by number, text operator or empty cell** ([#5255])
- **Web sidebar search and tag grouping** ([#5259])
- **Lite has a DuckDB memory limit setting** ([#5553])
- **Column filters list a text column's values to tick, like Excel, and stay set across refreshes and restarts** ([#5577])
- **One time range picker for Lite, the Darling Viewer and the Darling web pages** ([#5570])

### Changed

- **Darling web sends browser security headers, the actual-plan confirmation says where the query came from, and connection-test errors carry less detail, and a configured token gates the loopback endpoints** ([#5636])
- **Microsoft.Data.SqlClient moves to 7.1.1** ([#4963])
- **Fleet Custom Views Query Store panels read the store's chunk floors once** ([#5001])
- **Web System Events grids show the desktop's columns** ([#5118])
- **`get_read_latency` counts reads that fell back to raw** ([#5136])
- **get_query_store_top answers long windows from a daily summary** ([#5168])
- **Web Query Store Regressions, Long Query Completions and Plan Corrections grids put their wide columns behind column toggles** ([#5195])
- **Web File I/O tab charts write latency, and the Version Store trend spans 7 days** ([#5198])
- **The web PostgreSQL Overview and Vacuum tiles say where their data starts** ([#5193])
- **The composed-panel table, the alert-rule test result and the Fleet Sweeps tables get the main grid's tools** ([#5188])
- **query_stats fetches plan XML only for plans the store doesn't already hold** ([#5209])
- **The web Manage Servers grid shows disabled servers, auth mode, monthly cost and added time** ([#5262])
- **procedure_stats can skip re-rendering plans the store already holds (with a shadow mode)** ([#5272], [#5502])
- **PostgreSQL servers list the collectors that are not collected on their engine** ([#5287])
- **The diagnostics bundle's store statement history is now the ranked answer `get_store_query_history` gives** ([#5294])
- **PostgreSQL database growth findings now say their bars are measured** ([#5327])
- **The statement filter withholds more administrative statement forms** ([#5324])
- **Darling's MCP tool answers go through the statement filter** ([#5342])
- **Adding or editing a server from the web or MCP takes the password itself** ([#5364])
- **Alert emails and webhooks run SQL Server statement text and reports through the statement filter** ([#5360])
- **Plan reuse shadow mode no longer counts a memory grant change as a false hit, and splits false hits by cause** ([#5383])
- **The statement filter judges a typed variable declaration with an initial literal** ([#5369])
- **The database filter now reaches the blocking and waits reads** ([#5392])
- **The store takes a server's password itself from the viewer, admin and MCP roles** ([#5374])
- **Stored plans from the MCP plan tools go through the statement filter** ([#5362])
- **Web pages apply the statement filter to SQL Server statement text and execution plans** ([#5362])
- **The diagnostics bundle and the analysis drill-down plan warnings go through the statement filter** ([#5362])
- **SQL Server statement text and execution plans go through the statement filter when they are collected** ([#5367])
- **Analysis findings and alert history filter statement text as they are written** ([#5367])
- **A plan the statement filter withholds is shown as withheld** ([#5367])
- **Generic webhook headers are sent only to the generic URL or a path under it** ([#5387])
- **The database filter now reaches the duration trends, Query Store clutter, long-query and plan-correction reads** ([#5414])
- **The blocking charts now say which collector answered, and the desktop viewer's Blocking Severity follows the database filter** ([#5414])
- **A whitespace-only `database_name` now means every database on the configuration reads** ([#5464])
- **The database filter reaches the storage and file I/O reads** ([#5462])
- **Lite keeps query stats and Query Store stats in one archive file per day** ([#5482])
- **The plan regression check in Darling reads stored per-day totals for closed days** ([#5467])
- **procedure_stats now fetches plans only for plans the collector has not stored** ([#5502])
- **Recommendations skip databases that are a secondary copy on this node** ([#5564])
- **The Query Store interval tables are partitioned by day** ([#5573])
- **A long one-counter perfmon trend loads faster** ([#5579])
- **The retention purge gives way to collection** ([#5595])

### Fixed

- **The web viewer's server page Range now stops at 7 days** ([#4946])
- **Lite and Darling MCP tools now refuse `hours_back: 0.5`** ([#4945])
- **In the Darling Viewer, the Recommendations and FinOps pickers keep their servers** ([#4947])
- **On Azure SQL Database, the long-query trace runs only in monitored databases** ([#4944])
- **The first daily summary after a start no longer waits on the rollup coverage probe** ([#4968])
- **MCP tool calls whose arguments the server cannot read are refused with a message that names the argument** ([#4960])
- **Darling's Query Store top read no longer walks every index entry a server holds** ([#4952], [#5021], [#4972])
- **Active Queries, Current Waits and Custom Views panels say where their data starts, and the Queries tab stops showing it for a quiet start** ([#4953])
- **Darling compresses new `collection_log` chunks by collector as well as by server** ([#4951])
- **Faster "latest collector outcome" check on MCP reads** ([#4975])
- **Lite and Darling stop dropping Extended Events sessions that another install reads** ([#4967])
- **Custom Views panels that hit the 60 s limit say so** ([#4976])
- **get_collection_health and the Darling Collection Health grid read the newest row per collector through index lookups, not four full-window sorts** ([#4983])
- **Hourly store maintenance and self-metrics no longer stall collection** ([#4978])
- **Darling's README now states one PostgreSQL minimum for the store, 16** ([#4988])
- **Filtered Custom Views Query Store panels read less on the raw route** ([#4985])
- **Lite's Plan Corrections grid, Query Heatmap and Memory Pressure Events chart say where their data starts, and the heatmap draws its empty days** ([#4991])
- **Grids, charts and MCP replies in Lite, the Darling viewer and the web viewer say where their data starts when a range reaches back past it** ([#4966], [#5000], [#4998], [#4989], [#5015], [#5010], [#4990], [#5022], [#5031], [#5026], [#5030], [#5035], [#5086], [#5096], [#5111], [#5172])
- **RDS log reads for several targets at once no longer fail or skip log lines** ([#5006])
- **A once-a-day collector no longer stalls its server's 1-minute collectors** ([#4999])
- **A custom view panel whose store connection times out while opening says so** ([#5016], [#5052])
- **Query Store reads no longer scan the last day's raw rows to rule out legacy rows** ([#5021])
- **The Darling viewer's Overview blocking and deadlock chart says where its data starts** ([#5024])
- **The web dashboard's Availability Groups page shows every AG view again** ([#5045])
- **A run-time edit that lands just as that daily collector starts no longer makes it run twice that day** ([#5051])
- **The FinOps Index Analysis tab and get_finops index_analysis pick the same row every time when one index has two rows at the same collection time** ([#5059])
- **Long-window Custom Views panels over query statistics no longer time out** ([#5076])
- **The FinOps Optimization tab's most expensive queries no longer change which tied statements make the cut** ([#5078])
- **Storage Growth counts a table created during the window as growing by its whole size** ([#5080], [#4902])
- **The Storage Growth index drill shows when its snapshot was collected** ([#5079])
- **Index Analysis lists only the indexes in each database's newest snapshot, and answers in under a second on large servers** ([#5083])
- **Index Analysis says when each database's snapshot was collected** ([#5084])
- **Plans captured from PostgreSQL 16 and later servers no longer keep bind parameter values** ([#5112])
- **The stall probe no longer ranks idle background waits first** ([#5113])
- **Lite's deadlock, blocked process report and long query MCP tools say where their data starts** ([#5127])
- **The web server page's Memory Grant Pressure, Resource Semaphore Pressure, Plan Corrections and Query Store clutter panels now say where their data starts** ([#5144])
- **get_read_latency stays within the MCP response budget** ([#5149])
- **Lite's config change, collection log, plan correction and memory pressure MCP tools say where their data starts** ([#5147])
- **Darling's active queries, waiting tasks, query heatmap and Query Store regressions say where their data starts** ([#5147])
- **PostgreSQL plans are grouped by their shape** ([#5155])
- **collection_log records the monitored server's session id again** ([#5154])
- **PostgreSQL window aggregates, trend grids and Captured Plans on the web server page say where their data starts** ([#5165])
- **Darling's config change, collection log, plan correction and memory pressure MCP tools say where their data starts** ([#5181])
- **The blocking charts' "Showing since" note now follows the blocked process report collector when the chart draws its reports** ([#5159])
- **Darling's blocking, deadlock and long query tools say where their data starts** ([#5171])
- **Custom Views panels grouped by object no longer fail on large stores over a recent window** ([#5179])
- **The blocking charts' "Showing since" note no longer hides a stretch when the blocked process threshold was off** ([#5199])
- **Darling's system-health, default trace, wait, latch and spinlock MCP tools say where their data starts** ([#5191])
- **Two test classes now run in the shared live-store collection** ([#5214])
- **The web page's event grids say where their data starts** ([#5197])
- **Darling's blocking stats and PostgreSQL deadlock and log event MCP tools say where their data starts** ([#5205])
- **Fifteen Darling PostgreSQL MCP tools say where their data starts** ([#5201])
- **Darling's PostgreSQL write stats and index usage MCP tools say where their data starts** ([#5201])
- **The web page's System Events, Blocking, Deadlocks and PostgreSQL event panels say where their data starts** ([#5222])
- **MCP quiet and all-clear messages no longer contradict a cut window** ([#5250])
- **Query Store plans are found again** ([#5263])
- **Adding a server no longer refuses an immediate second add** ([#5268])
- **Collector stall probe no longer ranks an idle wait as the top wait** ([#5276])
- **MCP PostgreSQL empty answers no longer call a cut window quiet** ([#5285])
- **Long-window Custom Views panels over query statistics check the hourly rollup against a per-hour row count instead of scanning raw** ([#5290])
- **Lite's Query Store history window no longer overcounts Total Executions** ([#5310])
- **The web's Index Analysis list no longer stops at 50 rows** ([#5303])
- **The plan viewer's Parameterized row shows the parameterized text of an ad hoc statement** ([#5326])
- **The web Locking page filters by database, shades its wait columns and opens an index detail pane** ([#5372])
- **get_object_locking answers `empty`, not `unavailable`, when the collector looked and found no contended index** ([#5372])
- **Lite's archive watermark reads no longer pile up on one DuckDB engine** ([#5390])
- **Lite no longer starts a second refresh on top of a slow tab-switch read, and no longer drops a tab or range change made during a refresh** ([#5380])
- **Lite's post-collection CHECKPOINT no longer parks the app behind one slow read, and the hourly archive pass stops rebuilding the archive views when it moved nothing** ([#5385])
- **A login that cannot create Extended Events sessions now shows PERMISSIONS for the long-query collector** ([#5386])
- **Lite's archive compaction no longer rewrites a whole month of each table every hour, and says so when the disk is too full to merge** ([#5394])
- **Lite's Top Queries grid reads far less memory as the query_stats archive grows, and the procedure comparison reads less but can still run out of memory** ([#5398])
- **The Darling installer now refuses an install when the .NET Runtime 10 is missing** ([#5409])
- **Statement text is judged whole before it is cut** ([#5361])
- **A scripted `--encrypt-password` no longer writes to the error stream when it succeeds** ([#5417])
- **Lite's Query Store comparison and regressions no longer run out of memory on wide windows at a 1 GB memory limit, and top queries gets further** ([#5396])
- **Statements whose text was withheld are no longer acted on as if the marker were the statement** ([#5367])
- **Lite writes archive files in small row groups, so a windowed read decodes much less query text** ([#5402], [#5410])
- **Lite's Collection Health read is cheaper, can be cancelled, and says where its time goes** ([#5388])
- **Lite's and the Darling viewer's duration and execution-count charts report the true rate for a database-filtered view** ([#5414])
- **The Darling viewer's procedure comparison, its two comparison twins and the Query Store top reads look up statement text inside a time bound** ([#5427])
- **get_pg_io_trend and get_file_io_trend no longer time out on a store with stale table statistics** ([#5442])
- **The Darling service can now assume an AWS role from an `AWS_PROFILE` profile** ([#5446])
- **Severe errors show the database name their id had at the time of the error** ([#5432])
- **The Darling viewer's Top Queries and Top Procedures show blank, not 0, for columns the hourly rollups do not keep** ([#5443])
- **The saved database filter narrows more screens** ([#5438])
- **The retired-baseline sweep no longer leaves its connection dead when a drop loses a race with a refresh job** ([#5439])
- **Lite merges query_snapshots archive files into one file per day** ([#5434])
- **A convergence step that breaks the store connection no longer fails every step after it** ([#5453])
- **Saved passwords keep working on the first start after the schema 165 upgrade** ([#5456])
- **The one-time password pin step after the schema 165 upgrade is tightened** ([#5466])
- **Darling checks an RDS or Aurora target's host against the endpoint AWS reports** ([#5465])
- **PagerDuty auto-resolve now resolves a second outage that recovers inside the cooldown** ([#5470], [#5469])
- **Procedure stats keep every procedure that did work, not just 150, and the procedure trends and counts read the same either way** ([#5463])
- **An alert with a large blocked-process report no longer loses its details on a busy service** ([#5478])
- **The launch guard releases itself and the collection alerts keep working in a hold** ([#5481])
- **Alerts, MCP reads and web reads in the first seconds after a start keep their statement details** ([#5484])
- **The Query Store backfill stops retrying databases dropped from the server** ([#5486])
- **"Collection Stopped" alerts once per outage** ([#5490])
- **Azure SQL Database sizes now cover every database, and the firewall error names the firewall first** ([#5500])
- **Darling and Lite alert once per occurrence for lasting conditions** ([#5497])
- **Lite's FinOps tab loads real data on the first visit, and its advice claims only what the data supports** ([#5488])
- **A managed store near a free-disk step no longer reads "stale after a hardware change"** ([#5511])
- **The spinlock and PostgreSQL I/O reads no longer go through every stored row of a long window** ([#5501])
- **Offline servers read as stale, Collection Stopped counts from the last collection, and the web pages and FinOps read in plain words** ([#5492])
- **Query Store backfill no longer re-reads each server's database list every few minutes** ([#5534])
- **Plan regression day builds read only the days they need, and the Query Store top summary cleanup stops scanning its table** ([#5524])
- **Azure SQL Database Query Store collection stops reading each database's watermark from the store on every cycle** ([#5527])
- **Analysis reads the newest file I/O size and perfmon rows without scanning the whole window** ([#5529])
- **The hourly store self-metrics no longer count every row of the query text and plan tables** ([#5530])
- **Query Store top, history and trend reads scan a bounded range** ([#5533])
- **A managed store near a WAL size step no longer rewrites its settings file on every restart** ([#5544])
- **The start-up check for gaps in the rollups no longer reads a whole chunk of a daily rollup to look for one missing day** ([#5538])
- **The fleet overview tool and the web Fleet page total PostgreSQL deadlocks faster over multi-day windows** ([#5536])
- **The module map no longer re-reads two days of procedure stats at every restart** ([#5528])
- **Custom Views, custom alert rules and alert notebook charts read far fewer rows for a scoped server** ([#5537])
- **Query Store reads that use the stored interval tables no longer miss an interval that started up to about a day before the table's oldest data** ([#5543])
- **A quiet server's job history and default trace watermark read touches a week of data, not its whole history** ([#5532])
- **Small fixes across Lite and the Darling Viewer from the final release walk: Collector Schedules wording, Server Inventory health, time columns, screen-reader names and FinOps dashes** ([#5542])
- **Lite's Job History tab reads only the selected time range, and tab loads no longer wait on the database from the window** ([#5553])
- **Darling stops a rollup's refresh jobs before it drops the rollup** ([#5552])
- **The Darling Viewer's Top Queries, Top Procedures and Query Store tabs load faster over a network** ([#5556])
- **Two services starting against one PostgreSQL store no longer fail each other's role provisioning** ([#5566])
- **The Query Store interval purge finishes on a large store** ([#5572])
- **Running Jobs in Lite and the Viewer, and the get_running_jobs tool, list only jobs the collector's latest successful run found** ([#5554])
- **Each File I/O chart draws its own ten files, so Read Latency no longer goes blank when logs are busiest** ([#5554])
- **Lite's own queries no longer top Top Queries by Duration or Query Store by Duration** ([#5554])
- **The Live Snapshot button and the Get Actual Plan buttons in Lite judge statement text and plans with the screens' statement filter** ([#5554])
- **Select All in the Databases filter, or ticking every collected database, now means All** ([#5554])
- **Empty server-tab grids in Lite and the Viewer say why they are empty, and screen readers get names for toolbar combos and plan operators** ([#5554])
- **The web dashboard's footer time, Job History messages, saved views, offline fleet cards and service log read correctly** ([#5554])
- **The module map refresh no longer holds back the collectors at service start** ([#5580])
- **The module map's daily refresh no longer sorts every raw procedure_stats row** ([#5584])
- **The first Query Store read after a big retention drain is no longer slow** ([#5585])
- **Web charts draw at the panel's real width** ([#5590])
- **Autovacuum on the Query Store interval tables can finish again** ([#5593])
- **The Darling whole-tree guards CI job no longer fails after every test passes** ([#5598])
- **"Collection Falling Behind" no longer fires in the first hour after every start** ([#5599])
- **A hung test shard names its test and keeps its logs** ([#5591])
- **One-day Custom Views panels on Query Store data read an hourly rollup, or are refused up front** ([#5588], [#5610])
- **Darling's hourly maintenance step over stored analysis findings no longer times out on a large store** ([#5626])
- **Upgrade refusals no longer list the service's own files** ([#5628])
- **Darling's deadlock maintenance logs a line when a stage recovers after a failed pass** ([#5637])

## [3.9.0] - 2026-10-02

Full entries: [docs/changelog/3.9.md](docs/changelog/3.9.md)

### Important

- **Collectors run at their configured interval on large fleets, in Darling and Lite** ([#4644])
- **Upgrading from 3.8 runs the Darling store migrations V126 through V157 on the service's first start**

### Added

- **`--drop-xe-sessions`** ([#4852])
- **Collection Falling Behind self-alert** ([#4851])
- **PostgreSQL targets: a finding when pg_stat_statements keeps evicting statements** ([#4680])
- **Per-rule plan analyzer overrides** ([#4602])
- **Plan viewer: a Server Context card** ([#4614])
- **Plan viewer: a Parameters card** ([#4604])
- **Plan viewer minimap** ([#4596])
- **Plan analysis flags a possible Standard Edition batch-mode DOP limit, and marks legacy findings** ([#4585])
- **Plan viewer: a finding's header links to the operator it came from** ([#4559])
- **The MCP plan tools report which operator each finding came from** ([#4556])
- **Plan analysis turns a plan's wait statistics into findings, and gives external and preemptive waits a real benefit estimate** ([#4555])
- **Plan analysis now flags an operator that takes a large share of a statement's run time even when no other rule has advice for it** ([#4550])
- **Plan analysis warns on dynamic cursors and on cursors declared without `LOCAL`, and "Scan With Predicate" names a dynamic cursor when it is the likely cause** ([#4549])
- **Plan analysis flags a query whose text was cut off by SQL Server's showplan limit** ([#4548])
- **Plan analysis: added the "Bare Scan" finding for a full-table or full-clustered-index scan with no predicate** ([#4545])
- **Plan analysis now marks which findings are SQL Server's own warnings and which are inferences, and duplicate missing-index suggestions are no longer merged across different databases** ([#4543])
- **get_read_latency reports how long the web dashboard's and MCP server's reads take: p50, p95 and p99 per read over a chosen window, with timeout counts** ([#4454])
- **Self-monitoring alerts (Collection Stopped, Capture Down, Collector Cost Regression) open a notebook with the server's collection health and the collector's log and cost** ([#4437])
- **Analysis-finding alerts open a notebook with that finding's summary and evidence** ([#4436])
- **Custom-rule alerts open a notebook that charts the rule's own measure with its threshold or band** ([#4433])
- **Poison Wait alerts open an authored notebook: top waits on both engines, waiting tasks, resource-semaphore/memory-grant detail when the wait is RESOURCE_SEMAPHORE, and a trend bound to the firing wait type.** ([#4424])
- **Server Unreachable/Restored and the Agent-job alerts (Failed Agent Job, Long-Running Job, Agent Not Running) now open an authored notebook instead of the mechanical section-list fallback, and the collection-log web read can filter by status.** ([#4422])
- **High CPU alerts open an authored notebook: SQL Server and PostgreSQL CPU timelines, top queries/procedures by CPU, and scheduler pressure, instead of the mechanical read list.** ([#4420])
- **PostgreSQL Wraparound Risk, Vacuum Horizon Blocked and Replication Slot Retention alerts open an authored notebook: their own drill-down reads and a 24h trend panel, instead of the mechanical fallback section list.** ([#4419])
- **Long-Running Query and Forced Plan Failing alerts open an authored notebook: active/completed queries and a completion-duration timeline for Long-Running Query, plan corrections and a corrections timeline for Forced Plan Failing.** ([#4418])
- **Authored Blocking and Deadlocks alert-notebook templates** ([#4395])
- **Aurora per-query peak memory as a context fact** ([#4370])
- **Alert-notebook read-only render on `#/triage`** ([#4368])
- **Notebook `read` cell type** ([#4367])
- **Alert-notebook binding endpoint** ([#4366])
- **A shell-level pause/resume control for the web viewer's auto-refresh** ([#4365])
- **The store host profile now reports its cloud instance type** ([#4330])
- **Store host visibility: an MCP read and a web panel** ([#4282])
- **`--check-settings` reports whether a managed store's sizing still matches this host** ([#4271])
- **`--validate-config` (`--test-connection`) no longer fails a healthy store over a stale entry in darling.json** ([#4271])
- **Darling's MCP host now serves a /core endpoint alongside /** ([#4121])
- **get_resource_semaphore also returns interval_seconds** ([#4103])
- **get_query_store_top takes module_name, so one stored procedure's Query Store history can be read without widening top** ([#4057])
- **The store names its own slow queries: `get_store_query_stats` ranks the monitoring store's SQL by cost and by the role that ran it** ([#3899], [#3915], [#3914])
- **Maintain your own theme colors, per theme, with a reset to default** ([#3577])
- **A PostgreSQL target with every logging setting off no longer looks identical to an instrumented one** ([#3607])
- **A stock PostgreSQL target without pg_wait_sampling now has a wait profile instead of an empty chart** ([#3604])
- **PostgreSQL's server log is read by a classifier, not by two regexes** ([#3601])
- **The log said what the spill cost and what the vacuum cost; the store kept the sentence and threw away the number** ([#3602], [#3603])
- **Alert families route to their own channels** ([#3598])
- **PostgreSQL targets enter the analysis pipeline** ([#3542])
- **A PostgreSQL target's durability posture is stated, not inferred** ([#3542])
- **A PostgreSQL analysis pass now names the statement** ([#3542])
- **The two PostgreSQL knobs learn to speak** ([#3542])
- **A PostgreSQL target's analysis pass gains its vacuum family, graded on the same bars the Tier-0 alerts page on** ([#3542])
- **A PostgreSQL pool near its connection ceiling is graded as the outage it is** ([#3542])
- **A PostgreSQL analysis pass now names temp-file spill and gates work_mem on it** ([#3542])
- **A PostgreSQL analysis pass reads the wait profile — measured on Aurora, estimated on stock, and says which** ([#3542])
- **A PostgreSQL target learns its own normal** ([#3542])
- **The measurement contract had ten rules and two tests** ([#3653])
- **v2 plumbing for the PostgreSQL-target engine, and the wait-profile anomaly now folds onto its wait and can page** ([#3691])
- **Schema 133: the PostgreSQL saturation numerator and the sampler's duty cycle are stored** ([#3691])
- **The pull-request review learns the repository's traps and stops answering LGTM by default** ([#3710])
- **A server configuration change now has a consequence the engine states — `CONFIG_CHANGED` compares the ±4 h around it and says what moved, or that nothing did** ([#3653])
- **PostgreSQL deadlock cards carry the captured exemplars, grouped by shape and ranked by recurrence** ([#3691])
- **The PostgreSQL bloat family grades fourteen-day GROWTH behind a size floor, never a spot percentage** ([#3691])
- **A chain that fired every Tuesday at the same hour was rated a fresh incident each week and nobody was told; the pass now labels it "recurring at this hour" at unchanged severity, and a weekly Agent job whose slot slid gets "maintenance window moved"** ([#3653])
- **PostgreSQL read latency is now a graded fact, and the pass says why when it cannot know** ([#3691])
- **Replication lag and slot retention stories for the PostgreSQL-target engine** ([#3691])
- **The CPU sample's UTC instant and the server's time-zone id are now stored, so UTC-window readers stop deriving an offset that is an hour wrong across DST** ([#3653])
- **PostgreSQL sessions: an operator now learns which application left a transaction open and walked away** ([#3691])
- **The Long-Running Query opt-out knob was read-only on Darling — V135 gives it a store home with the production read's seeds as the column default, editable in the Viewer and through `update_alert_settings` on both SKUs** ([#3653])
- **PostgreSQL blocking: the pass names the head of the chain, how long the sessions behind it really waited, and whether the log agrees** ([#3691], [#4886])
- **A PostgreSQL table that turned autovacuum off and fell behind is its own card** ([#3691])
- **A collector can be enabled or disabled headlessly** ([#3752], [#4809])
- **Stock PostgreSQL's sampled waits read per second the sampler was watching, and get their own baseline and anomaly** ([#3691])
- **Rule 6's C# half is a census, not a stated bound** ([#3653])
- **CONFIG_CHANGED covers database options and trace flags, not only sp_configure** ([#3653])
- **Every trend surface renders the identity-epoch discontinuity marker the store already held** ([#3653])
- **v3 plumbing for the PostgreSQL-target engine** ([#3691])
- **PostgreSQL targets get a per-database size series and the host's memory** ([#3691])
- **PostgreSQL buffer-pressure cards say what the cache holds** ([#3691])
- **A PostgreSQL pass now names the statement whose plan changed and what it cost** ([#3691])
- **A PostgreSQL target's analysis pass measures CPU where no capacity percent exists** ([#3691])
- **PostgreSQL targets learn whether the memory configuration fits the host** ([#3691])
- **A baseline series can be scoped to one statement** ([#3691])
- **A PostgreSQL operator now learns which large relation a statement reads sequentially under a selective predicate, how often, and what an index would cost** ([#3691])
- **Darling V137 / Lite v64: eight nullable columns so the store can say what Query Store captures, where a lone finding goes, and how full the plan dimension's TOAST file is** ([#3796], [#3712], [#3783])
- **Nothing in the product could say which databases were cluttering Query Store, why, or what it cost the servers hosting them — while every input for that answer was already in the store** ([#3797])
- **`get_query_store_health` said "Query Store health" and skipped the one knob that names a plan-churn factory** ([#3796])
- **The store could say how big its plan dimension's TOAST file was and not how full, and could not see its own checkpointer at all** ([#3783])
- **The uncorroborated-finding route knob said FILE-LEVEL, edit darling.json and restart, while the rung had already given it a store column nothing read** ([#3712])
- **A PostgreSQL operator now learns which database is growing, how fast, and when it doubles** ([#3691])
- **The Query Store clutter view had no viewer and told every reader the capture mode was uncollected** ([#3797])
- **The three report alerts carry their rows as structure as well as prose, so every surface that can render a table has one** ([#3834])
- **A baseline's dispersion floor says in words that it does not cover a dead metric, a Flat-tier fact admits its distinct-day count is a ceiling proxy, and the three copies of the stamping helper are held to one key set** ([#3859])
- **A summary card can name its top few objects, not only its worst** ([#3691])
- **`get_collection_log` can be asked for the failures** ([#3869])
- **`get_collection_health` now says when the analysis pass could not read one of its fact families** ([#3691])
- **A `pg_server_config` row now says whose setting it is** ([#3691])
- **`get_object_locking` says when its snapshot was collected** ([#3880])

### Changed

- **The managed store sets `random_page_cost = 1.1`** ([#4863])
- **Per-server Query Store reads fetch fewer rows** ([#4861])
- **SQL Server blocking and deadlock baselines count the hours collection covered** ([#4853])
- **The plan viewer's Runtime Summary lists CE model above Optimization, with Early abort under it** ([#4849])
- **Purge Now runs in the background and is paced like the daily purge** ([#4835])
- **get_pg_deadlocks explains an empty answer more completely** ([#4832])
- **Darling reads a quiet database's watermark from its cache instead of from the store every cycle** ([#4818])
- **Query Store rows now record when each interval ended, in both apps' stores** ([#4802])
- **Query Store backfill: the per-tick database list no longer walks the newest chunk's index** ([#4703])
- **Query Store views over windows longer than the raw tier now show the full window from the interval table, and say how far back it reaches** ([#4700])
- **Analyzer wording for Nested Loops outer sides and row estimate mismatches now counts rows the way the plan viewer does** ([#4698])
- **PostgreSQL targets: the top-queries views say when pg_stat_statements evicted entries during the window** ([#4680])
- **Web dashboard: each notebook and Custom View has its own auto-refresh setting, notebooks start with it off, and slow pages back off** ([#4671])
- **Query Store collection: each database's watermark comes from the store only when a cached value can't be proven current** ([#4669])
- **Alert pass: the forced-plan failure check reads the store only when its answer can have changed** ([#4667])
- **Query Store collection: the orphaned per-database state cleanup runs at most once an hour per server** ([#4665])
- **The Darling Viewer's Query Store trend chart and `get_query_store_duration_trend` no longer re-scan the hourly rollup's oldest and newest hour on every load** ([#4618])
- **Custom Views Query Store panels over a recent window read the per-interval table instead of re-sorting raw snapshots** ([#4617])
- **`get_store_metrics` reports the per-interval Query Store tables by name** ([#4616])
- **Darling Viewer and MCP store reads can no longer spill unbounded temporary files** ([#4610])
- **Plan viewer minimap: resize, double-click zoom, and accuracy-coloured edges** ([#4603])
- **Rule 38 can now flag a Standard Edition DOP limit as a Warning** ([#4601])
- **Plan viewer: one neutral card design for the insights strip** ([#4599])
- **The plan viewer's Wait Stats rows show each wait's potential benefit and never clip** ([#4595])
- **Plan viewer costs are formatted as in PerformanceStudio** ([#4592])
- **The plan viewer's Wait Stats header shows the total wait time and wait-type count on hover** ([#4591])
- **The plan viewer's runtime summary card matches PerformanceStudio's** ([#4590])
- **The plan viewer's properties panel can be filtered and copied, and keeps its width** ([#4588])
- **The plan viewer rolls per-thread stats into one collapsed breakdown** ([#4587])
- **The plan viewer colours actual-plan edges by how far actual rows diverged from the estimate** ([#4586])
- **Wait-category colours use PerformanceStudio's contrast-checked palette** ([#4583])
- **Plan analysis drops three rules that PerformanceStudio removed** ([#4565])
- **Dependencies: Velopack to 1.2.158, with the release packer moved to match** ([#4563])
- **The store writes much less WAL refreshing its Query Store rollups** ([#4506])
- **The store writes less WAL maintaining its query plan and text dimensions** ([#4502])
- **The store's own PostgreSQL log now names the application behind each line** ([#4501])
- **Darling and Lite now record each Availability Group's `group_id`, so two monitored secondaries of one AG count as one group even without its primary** ([#4495])
- **The Viewer's Job History reads each server's newest runs through an index instead of sorting the whole fleet's window** ([#4493])
- **The service's startup watermark read looks up each collector's newest run through a new index instead of scanning the recent `collection_log` chunks** ([#4489])
- **Darling's service, Viewer, CLI and store-upgrade connections now name themselves in `application_name`** ([#4486])
- **The Viewer's Overview runs its fleet collection-health read once per refresh however many server cards load at once, and the status bar measures the store's size at most every 5 minutes** ([#4482])
- **The startup watermark read no longer walks every compressed `collection_log` chunk in retention** ([#4480])
- **`get_ag_health` returns at most 11 Availability Group views by default, the least healthy first, and says when more exist** ([#4474])
- **The Query Store stores write less WAL keeping their plan and text maps current (a quarter to a third less per touch on a test store)** ([#4472])
- **Scheduler issues now report SQL Server CPU, other-process CPU, idle CPU, memory utilization, page faults and working-set change from each scheduler-monitor sample, flagged the way sp_HealthParser flags them** ([#4456])
- **The store now gives the web dashboard's and MCP server's reads up to 60 s before cancelling them, instead of 15 s** ([#4447])
- **The managed store checkpoints every 15 minutes instead of 5, cutting write-ahead log volume** ([#4426])
- **Report alerts link to their own page: the Fleet Sweep Rollup opens the sweeps page, and the digests carry no link** ([#4421])
- **`get_top_procedures_by_cpu` and the Top Procedures grid now route to the hourly rollup once raw ages past its retention window, instead of returning nothing** ([#4413])
- **Sized the Linux compose store's background-worker slots and `work_mem` from the product's own
- **Cache the server-scoped watermark read across a runner's lifetime, cutting repeated `MAX()` reads against `job_history`, `default_trace_events`, `system_health_events`, and `memory_pressure_events` to one seed per (server, collector) pair instead of one per collection cycle** ([#4399])
- **Top-N-by-CPU queries now route to the hourly rollup once raw's retention has dropped the window, instead of returning empty** ([#4396])
- **The Performance Trends Query Store duration chart reads the interval table for windows of 48 hours or more** ([#4382])
- **The Darling viewer's TempDB file I/O trend now buckets like the File I/O tab's own reads** ([#4353])
- **Raw hypertables now re-tune their own chunk interval once a day from actual ingest** ([#4344])
- **The Queries grid and MCP's Query Store top read use a per-interval table for long windows** ([#4341])
- **Lite's memory clerk and File I/O trend charts bucket long windows in DuckDB** ([#4340])
- **Lite keeps one DuckDB connection open, so reads attach to it instead of reopening the database file** ([#4339])
- **Lite's Performance Trends charts bucket long windows in DuckDB** ([#4338])
- **Lite's Overview CPU, wait and memory lanes bucket long windows in DuckDB** ([#4337])
- **Settings the service manages now live in one included file, not stacked append blocks** ([#4336], [#4358])
- **Darling viewer's Performance Trends charts bucket server-side over long windows** ([#4333])
- **Lite's Wait Stats and Perfmon charts bucket long windows instead of shipping one point per collection** ([#4331])
- **Darling viewer's File I/O and memory clerk charts load faster over long windows** ([#4329])
- **Darling viewer's CPU, Overview wait and Overview memory charts bucket server-side over long windows** ([#4327])
- **PostgreSQL-target baselines recompute once a day instead of every hour** ([#4324])
- **Three legacy query-stats rollups stop refreshing** ([#4186])
- **llms.txt and CITATION.cff now match the shipped product** ([#4157])
- **The MCP tool list's size limit now matches its size** ([#4141])
- **get_spinlock_stats puts a short description in the tool list** ([#4127])
- **analyze_query_plan puts a short description in the tool list** ([#4126])
- **get_pvs_stats puts a short description in the tool list** ([#4125])
- **get_deadlocks (Darling and Lite) and get_blocked_process_reports (Lite) put a short description in the tool list** ([#4123])
- **get_long_query_completions puts a short description in the tool list** ([#4122])
- **get_memory_pressure_events puts a short description in the tool list** ([#4120])
- **get_latch_stats puts a short description in the tool list** ([#4119])
- **get_index_usage puts a short description in the tool list** ([#4118])
- **analyze_procedure_plan puts a short description in the tool list** ([#4117])
- **validate_custom_view and run_custom_view_panel put a short description in the tool list** ([#4116])
- **analyze_plan_xml puts a short description in the tool list** ([#4115])
- **get_ag_health and get_store_query_stats put a short description in the tool list** ([#4114])
- **Claude Code keeps the MCP entry tools loaded even when it defers the rest** ([#4113])
- **get_query_store_regressions puts a short description in the tool list** ([#4109])
- **get_blocking and get_pg_replication_slots put a short description in the tool list** ([#4108])
- **get_active_queries puts a short description in the tool list** ([#4107])
- **get_plan_corrections puts a short description in the tool list** ([#4105])
- **get_query_store_duration_trend and get_perfmon_trend put a short description in the tool list** ([#4101])
- **get_resource_semaphore and get_memory_grants put a short description in the tool list** ([#4100])
- **get_daily_summary and get_daily_summary_range put a short description in the tool list** ([#4099])
- **get_query_store_health and get_default_trace_events put a short description in the tool list** ([#4095])
- **get_pg_kernel_stats, get_pg_xmin_horizon and get_pg_table_bloat put a short description in the tool list** ([#4093])
- **get_pg_io_trend, get_pg_database_trend and get_pg_cpu_utilization put a short description in the tool list** ([#4092])
- **get_pg_column_stats, get_pg_replication_stats and get_pg_predicate_stats put a short description in the tool list** ([#4091])
- **The Darling service account can no longer change its own program files** ([#4090])
- **mute_analysis_finding and audit_config put a short description in the tool list** ([#4088])
- **get_pg_blocking, get_pg_wait_stats and get_pg_index_usage put a short description in the tool list** ([#4087])
- **get_pg_plan_capture_readiness, get_pg_plans and get_pg_session_states put a short description in the tool list** ([#4086])
- **get_query_duration_trend and get_procedure_duration_trend put a short description in the tool list** ([#4085])
- **get_analysis_facts and get_analysis_findings put a short description in the tool list** ([#4083])
- **get_pg_database_stats, get_pg_autovacuum_health and get_pg_top_queries put a short description in the tool list** ([#4082])
- **get_pg_logging_audit, get_pg_io_stats and get_pg_wait_sampling put a short description in the tool list** ([#4081])
- **validate_custom_alert_rule, test_custom_alert_rule, update_custom_alert_rule and list_custom_alert_templates put a short description in the tool list** ([#4079])
- **get_alert_history, set_notification_route_enabled, create_mute_rule and set_mute_rule_enabled put a short description in the tool list** ([#4077])
- **get_pg_extensions, get_pg_write_stats, get_pg_server_config and get_pg_server_config_changes put a short description in the tool list** ([#4078])
- **get_collector_stall_probes, describe_custom_view_catalog and get_sweep_reports put a short description in the tool list** ([#4074])
- **analyze_server and compare_analysis put a short description in the tool list** ([#4073])
- **get_fleet_overview, add_servers and remove_server put a short description in the tool list** ([#4071])
- **get_pg_log_events, get_pg_index_bloat and get_pg_deadlocks put a short description in the tool list** ([#4068])
- **get_query_store_clutter and get_oversized_plan_backlog put a short description in the tool list** ([#4067])
- **update_alert_settings and update_mute_rule put a short description in the tool list** ([#4066])
- **get_collection_log and get_query_store_top put a short description in the tool list** ([#4065])
- **Three self-monitoring MCP tools put a short description in the tool list** ([#4064])
- **get_collection_health's tools/list entry drops from 21,026 characters to 583** ([#4063])
- **get_alert_settings and get_notification_routes serve a short head, with the rest available from get_tool_guide** ([#4061])
- **Seven data-read MCP tools put a short description in the tool list** ([#4055])
- **MCP clients load less context up front, and a new get_tool_guide tool serves the long-form guidance on request** ([#4048])
- **Darling's MCP server instructions drop from 88,532 to 7,992 characters and Lite's from 52,942 to 7,721, both now under a pinned 8,000-character budget** ([#4039])
- **PostgreSQL log messages show as PostgreSQL wrote them, and the SQL in them is normalized with every literal replaced by ?** ([#3996])
- **The shared `as_of` description is 167 characters instead of 379, cutting Darling's `tools/list` by 22,040 characters (6.4%) and Lite's by 14,246 (8.8%)** ([#3965], [#3898])
- **The review guard tells a pending workflow fix from hostile drift**
- **The tempdb Space alert stops paging on a single collected sample** ([#3653])
- **The analysis names the Agent job that is running long instead of counting it** ([#3653])
- **MCP tool failures were two wire shapes — a bare sentence on 214 tools and a JSON envelope on the PostgreSQL reads — and a `status`-keyed client read the sentences as successful text; now every tool on both SKUs answers a caught exception with one envelope through `McpHelpers.FormatError`** ([#3653])
- **PostgreSQL bars the fleet measured now say so** ([#3691])
- **Anomaly detectors fired on one hot sample and fired more the longer the window was; the gate now judges the window peak AND the window mean, and I/O reads the pair like its siblings** ([#3653])
- **Confidence chooses the channel** ([#3712])
- **Blocking Wait Time paged on one snapshot and cleared on the next, and Long-Running Query had no way to stop reporting the same permanent background sessions forever; blocking now fires on one snapshot at 3× the bar or on 3 consecutive collections through the shared persistence gate, and long-running queries gain an opt-out knob seeded from the production read** ([#3653])
- **PostgreSQL saturation now tells queueing from load** ([#3691])
- **A story's root card now names the leaves it consumed** ([#3691])
- **The wait-profile anomaly judges the window peak AND the window mean** ([#3741])
- **CONFIG_CHANGED now anchors on when RECONFIGURE ran, not when the snapshot first noticed** ([#3740])
- **MCP refusals carry a status word: the invalid envelope on both SKUs, and PostgreSQL refusals answer 400 not 500** ([#3739])
- **PostgreSQL wait standouts, the I/O admission floor and the ratio multiple carry the second fleet read's lineage** ([#3691])
- **The managed store's WAL ceiling is derived from the data volume's headroom, not v4's fixed 4 GB on every box** ([#3802])
- **Stock PostgreSQL's sampled wait profile fires its first-occurrence reading on the peak alone, like its Aurora and SQL Server twins** ([#3691])
- **A PostgreSQL bad actor is graded against its OWN normal** ([#3691])
- **Dependencies: `Microsoft.Data.SqlClient` and `Microsoft.Data.SqlClient.Extensions.Azure` to 7.1.0, `AWSSDK.PI` and `AWSSDK.RDS` to their next patch, with every lock file the bump reaches regenerated**
- **`ANOMALY_PG_PLAN_REGRESSION` judges each plan-flipped statement against its own routine** ([#3691])
- **The alert pass retries a store read once before counting it failed** ([#3848])
- **Store-object convergence runs every hour, not only at start** ([#3817])
- **The alert pass's other seven store reads retry once on a command timeout, through the same seam as the adapter's twelve** ([#3854])
- **The compression band seats its three heaviest hypertables on spread minutes instead of the consecutive minutes registry order gave them** ([#3678], [#3781])
- **Daily continuous-aggregate refreshes run one bucket per transaction** ([#3745])
- **`audit_config` answers a PostgreSQL target with the target's own settings instead of redirecting it** ([#3691])
- **The same blocking storm grades the same on every pass length** ([#3871])
- **The Extended Events collectors use less CPU on the monitored server to filter events by time** ([#4912])

### Fixed

- **A shutdown during a managed PostgreSQL tool step is no longer missed** ([#4867])
- **Rollup reads stop at the window end** ([#4859])
- **Grid time columns sort by time** ([#4858])
- **Six Postgres time columns follow the time display mode** ([#4858])
- **Server times and charts are right across a daylight saving change** ([#4783], [#4829], [#4838], [#4840], [#4841], [#4847], [#4848], [#4855])
- **Anomaly tiles agree on the window end** ([#4854])
- **Lite shows a refused capture read as a failure** ([#4853])
- **A backward clock step no longer pauses collection or delays repeat alerts** ([#4851])
- **A server edited during its connect no longer runs on the old connection string** ([#4851])
- **PostgreSQL spike advice names a baseline that measured zero** ([#4850])
- **Lite's View Block Chain reads the Blocking slicer's window when a report has no event time** ([#4848])
- **Fleet views show WARNING when half or more of a collector's databases fail** ([#4846])
- **`run_custom_view_panel` runs count in read latency with the web dashboard off** ([#4839])
- **An alert that no channel delivered is tried again a minute later instead of waiting out its cooldown** ([#4786], [#4804], [#4826], [#4827], [#4828], [#4837])
- **A removed server no longer leaves alert state behind** ([#4837])
- **The daily retention purge paces its deletes** ([#4835])
- **The Store Checkpointer Pressure alert sees one long checkpoint sync** ([#4835])
- **get_store_metrics reports the longest checkpoint sync the alert judges** ([#4835])
- **Query Store trends no longer read low after a quiet interval** ([#4833])
- **Editing a server's host in the Darling viewer keeps its favorite star with the server** ([#4831])
- **get_store_metrics no longer reads two days of store growth as one** ([#4830])
- **Darling and Lite count the I/O anomaly tiles' samples the same way** ([#4820])
- **A tile's peak time no longer changes between runs when two samples tie** ([#4820])
- **Lite's archive views stay readable while compaction swaps files** ([#4816])
- **Lite no longer counts archived rows twice during an archive run** ([#4816])
- **"Recurring at this hour" survives a daylight saving change** ([#4814])
- **A nightly job no longer reads as "Maintenance window moved"** ([#4814])
- **Reading a PostgreSQL log no longer records a false error when the read starts inside a multi-byte character** ([#4813])
- **A byte that is invalid in the database encoding costs one log read per cycle, not four** ([#4813])
- **PostgreSQL error rows under the pgBadger log prefix now carry their user and database** ([#4813])
- **The plan-capture readiness read now reports a log prefix the log readers cannot read** ([#4813])
- **A deadlock report cut at the end of a read waits for its last lines instead of being stored as a fragment** ([#4813])
- **The fleet sweep bands a server outage as No Data instead of Warning** ([#4811])
- **A collector that fails on half or more of its databases now bands Warning** ([#4811])
- **The Darling viewer's Collection Health tab no longer re-reads the store's schema version on every refresh** ([#4811])
- **Plan-forcing advice says when SQL Server's own recommendation names the proposed plan as the regressed one** ([#4810])
- **Lite no longer archives the same rows twice after it is stopped in the middle of an archive run** ([#4808])
- **Lite's Query Store repair also covers archive months that compaction split into parts** ([#4808])
- **Darling retries a store that is at its connection limit when the service starts** ([#4806])
- **The compose `darling` container reports unhealthy when a startup failure has stopped collection** ([#4806])
- **The docs now name the oldest supported builds: SQL Server 2016 SP2 and SQL Server 2017 CU3** ([#4805])
- **A webhook that never answers no longer holds up alert delivery** ([#4803])
- **Query Store backfill now logs a warning when it cannot read its list of work** ([#4801])
- **Query Store backfill keeps a slice size that works instead of swinging back to the size that timed out** ([#4801])
- **PostgreSQL analysis: a configuration setting shown under another finding now carries its own advice** ([#4799])
- **The advice for turning autovacuum back on now names the provider's parameter group** ([#4799])
- **A webhook channel that keeps failing no longer goes unnoticed while another channel delivers** ([#4798])
- **The configuration change card no longer calls a metric "resolved" when only minutes have passed since the change** ([#4797])
- **A server that is down when the Darling service starts now gets a "Collection Stopped" alert once the threshold passes** ([#4796])
- **Adding a server whose id matches a different server's no longer overwrites it in the Darling viewer, and Lite now refuses to add or edit onto it** ([#4794])
- **`get_store_metrics` no longer reports a gap of several days in the store's size history as one day's growth, and it marks today's partial day as partial** ([#4792])
- **The store checkpointer alert no longer tells you to raise max_wal_size for every requested checkpoint** ([#4791])
- **`mute_analysis_finding`, in Darling and in Lite, no longer mutes the pattern on whichever matching server sorts first** ([#4790])
- **Repeating a `create_mute_rule` call no longer creates a second identical rule** ([#4790])
- **`add_servers` reports every server in the batch when a later entry fails to save, and no longer says "added" for a save that wrote nothing** ([#4787])
- **The plan-force bot no longer reads a database missing from the newest automatic-tuning capture as "automatic correction off"** ([#4784])
- **After a failed state read, the plan-force bot looks at the same plans again after 1 hour instead of holding them for a day** ([#4784])
- **Web viewer: a page left loading in a hidden or paused tab refreshes normally again** ([#4781])
- **The PostgreSQL plan-regression finding now reports the worst regression when more than 50 statements changed plans in the window** ([#4780])
- **A standby that was removed or replaced no longer outranks the live standbys for the rest of the window** ([#4780])
- **A dropped replication slot no longer grades Critical for the rest of the window** ([#4780])
- **The alert history now records whether each notification channel delivered or failed** ([#4779])
- **The alert notebook finds an alert's resolution even when it came late, was dismissed, or sat behind many newer alerts** ([#4778])
- **A fleet-level store alert with no resolution reads "No resolution recorded" instead of saying collection stopped** ([#4778])
- **Darling now sends alert email through notification routes when the default recipient list is blank, instead of treating email as not set up** ([#4777])
- **`--configure-network` keeps the web listener's `tls` and `oidc` settings** ([#4775])
- **A repaired hourly hole no longer leaves a partial day in the daily rollups, and days an earlier repair left short are rebuilt once after the upgrade** ([#4739], [#4763])
- **Darling upgrade no longer says "New build in place." after copying nothing** ([#4762])
- **An older Lite refuses to open a data file from a newer version** ([#4753])
- **Lite re-applies its newer columns on every start** ([#4753])
- **After the computer sleeps and resumes, Lite runs each due collector once** ([#4753])
- **Two analyze_server calls that overlap no longer give the second one a false all-clear** ([#4742])
- **In Lite, a server that just went down no longer holds a connection slot while its retries wait** ([#4740])
- **The self-hosted PostgreSQL log tail no longer stays on the outgoing log file after a rotation in the same second** ([#4738])
- **Store upgrade: copy mode is chosen from the data folder's own volume and a finished size count** ([#4725])
- **Store upgrade: hard-link mode removes the pre-upgrade directory even when a settings carry fails, after saving the old `postgresql.conf`** ([#4725])
- **Store settings migration: a repeated attempt restores the newest backup of `postgresql.conf`** ([#4725])
- **Store free-space checks and reports read the data folder's own volume** ([#4725])
- **Lite archive compaction no longer loses a month when a promote fails** ([#4718])
- **The size-triggered Lite reset no longer deletes the database after a failed export** ([#4718])
- **The tab Refresh button no longer runs collectors during a Lite database reset** ([#4718])
- **Lite archive views now include compaction's part files** ([#4718])
- **Importing a previous Lite install no longer overwrites this install's part files, and imported files now expire with retention** ([#4718])
- **A failed archive export no longer leaves a partial file on disk** ([#4718])
- **Reads of an archive view no longer fail after old archive files expire** ([#4718])
- **A managed store moved aside by a failed upgrade is no longer replaced by an empty one** ([#4717])
- **A start after an interrupted store upgrade resumes it instead of refusing every start** ([#4717])
- **A failed store upgrade that left no startable store stops the start** ([#4717])
- **The pre-upgrade rollback copy survives the two service starts it is kept for** ([#4717])
- **PostgreSQL 13 servers read through csvlog are no longer reported as quiet** ([#4714])
- **RDS and Aurora PostgreSQL log reads now collect the lines a log file received before RDS rotated it, and resume where they stopped after a restart** ([#4713])
- **Down servers no longer use up the fleet's collection slots, and a command timeout no longer drops a healthy connection** ([#4712])
- **A memory grant that used none of its memory now gets an Excessive Memory Grant finding** ([#4705])
- **Estimated Plan CE Guess names the predicate behind each default guess and follows the plan's CE model version** ([#4705])
- **Expensive Operator no longer names an exchange operator** ([#4705])
- **Self-hosted PostgreSQL targets no longer lose the log lines written just before a log rotation** ([#4704])
- **A Query Store backfill database that keeps failing no longer blocks the databases after it on the same server** ([#4703])
- **The plan viewer's row label, edge colours and minimap, and analyzer rules 5 and 26, no longer misread operators in a parallel zone** ([#4632], [#4688], [#4698])
- **Selecting the plan viewer's "+" tab through UI Automation (screen readers) adds one new sub-tab, not two** ([#4688])
- **The Darling Viewer's server list is no longer a solid white box when the store is unavailable** ([#4685])
- **Custom tab headers and the status-bar collector text now use readable, theme-following ink** ([#4682])
- **Web dashboard: a slow notebook or Custom View panel is no longer re-run while its previous read is still running** ([#4671])
- **Custom Views: window-total Query Store measures are no longer labelled "(ratio)"** ([#4664])
- **Cool Breeze's warning text now reads clearly on the plan viewer's properties panel** ([#4658])
- **Darling: the daily digests keep their time of day** ([#4656])
- **Darling Viewer: the Plan Viewer opens when the viewer has no configuration or can't reach the store** ([#4649])
- **Custom Views AVG panels read from a rollup match raw to the last digit** ([#4647])
- **Darling Viewer: restarting as administrator during an upgrade keeps its configuration** ([#4646])
- **Collectors run at their configured interval on large fleets, in Darling and Lite** ([#4644])
- **The plan viewer's minimap now draws the first time you open it** ([#4643])
- **Double-clicking a minimap node now centers it correctly** ([#4643])
- **Jumping from a plan warning to its operator now centers the operator** ([#4643])
- **Copy and paste no longer freeze the window for up to 9 seconds when another app holds the clipboard** ([#4634])
- **get_store_metrics no longer reports retired store objects as a failing sweep** ([#4633])
- **The Runtime Summary no longer says a query used "100%" of a memory grant it never got** ([#4632])
- **Plan viewer orange text now reads clearly in the Light theme** ([#4632])
- **Collector status text in the status bar now reads clearly in the Light theme** ([#4632])
- **The repro script declares each parameter once** ([#4626])
- **Collection health no longer calls an idle activity-only collector regressed** ([#4625])
- **The daily purge of the per-interval Query Store tables no longer scans each table in full** ([#4615])
- **A rare deadlock between a chunk drop and the alert pass's database-state check no longer fails that pass** ([#4612])
- **Copying from Lite or the Darling Viewer no longer crashes when another program holds the clipboard** ([#4600])
- **The plan viewer no longer crashes when another program is holding the clipboard, and "Copy Query Text" now hands back the full query on a truncated single-statement plan** ([#4593])
- **`get_ag_health` keeps its fleet-wide answer within the 32 KB MCP budget** ([#4568])
- **Plan analysis: the repro script matches PerformanceStudio's hardening** ([#4567])
- **A cancelled analysis, comparison, or collection-cycle read now propagates instead of returning an empty or fallback result** ([#4562])
- **Lite's MCP analysis tools stop reading when the client cancels the request** ([#4561])
- **Plan analysis stops when its caller cancels** ([#4560])
- **Plan analysis now looks inside a procedure, function or cursor body** ([#4557])
- **Plan analysis: the non-SARGable predicate check now knows which table a function, conversion or `ISNULL`/`COALESCE` call is wrapping, so it stops flagging a Nested Loops outer reference or a parameter-side conversion, recognizes `LIKE` as a comparison, and now also catches a function or conversion on an unaliased table variable's own column** ([#4554])
- **The rollup coverage check no longer writes about 170 MB of temp files every five minutes on a large store** ([#4553])
- **Plan analysis now scores each finding's benefit, and the plan viewer and MCP plan tools show it** ([#4552])
- **A deeply nested execution plan no longer crashes the Darling service, the MCP plan tools or the plan viewer** ([#4551])
- **Plan analysis: adaptive-join memory grants, redundant UDF findings, and culture-dependent percentages** ([#4548])
- **Plan analysis: an operator's reported self-time no longer includes the coordinator thread's wall-clock time or double-counts a batch-mode or Compute Scalar child's time, in either a parallel or a serial plan** ([#4547])
- **The Join OR Clause plan-analysis finding no longer fires on a parameterized `IN` list** ([#4544])
- **Plan analysis no longer flags an unexecuted operator as a row estimate mismatch, and stops recommending a rewrite for the engine's own table-valued functions (`STRING_SPLIT`, `OPENJSON`, `GENERATE_SERIES`, DMVs/DMFs)** ([#4542])
- **The store no longer compresses every heavy table at once after midnight** ([#4541])
- **Plan analysis rules for `MAXDOP 1`, `RECOMPILE`, `OPTIMIZE FOR UNKNOWN`, `NOT IN`, and row-goal causes ignore hints and keywords written inside a comment or a string literal** ([#4540])
- **A managed PostgreSQL or a config load that takes more than two minutes to start no longer leaves Darling running without collecting** ([#4538], [#4509])
- **After a long store outage at start, Darling now keeps retrying and starts collecting when the store is back** ([#4509])
- **After a SQL Agent job-history identity reset (a reseed, a restore or a failover), Darling and Lite re-read the recent history once instead of on every collection** ([#4496])
- **A gap of one hour in the collection-health rollup no longer makes the Viewer, web and MCP health reads scan seven days of raw collection log** ([#4494])
- **Lite's perfmon chart and `get_perfmon_trend` also set aside a one-sample Wait Statistics spike and say how many** ([#4492])
- **A one-sample spike in a Wait Statistics counter no longer flattens the perfmon chart** ([#4490])
- **Lite's Job History shows that it is loading instead of an empty grid, and says when it shows only the newest rows** ([#4488])
- **`query_store` no longer reads as "produced then stopped" between Query Store intervals** ([#4483])
- **The Availability Groups tab and `get_ag_health`'s `distinct_ag_count` no longer count differently-owned AGs that share a name as one group** ([#4481])
- **Job History shows that it is loading instead of an empty grid, and says when it shows only the newest rows** ([#4481])
- **Recommendations say "queries", not "querys"** ([#4481])
- **Plan analysis now reads the query plan inside an IF condition, and the query and plan hashes of statements that carry several plans** ([#4470])
- **Upgrading a busy store no longer stops partway and asks for a re-run when the service's process takes a few extra seconds to exit** ([#4467])
- **Stores that expose the managed PostgreSQL on the network re-verify a settings file left unstamped by a hand edit or an interrupted start, instead of reporting a failure on every start** ([#4465])
- **Stores that expose the managed PostgreSQL on the network now finish the settings-file migration, instead of restoring the previous file and logging a warning on every start** ([#4464])
- **The daily chunk-interval adjustment now shrinks a busy table's chunks on stores that hold more than 1,000 chunks in total** ([#4458])
- **Severe-error rows from system_health now carry their database id, and no longer list errors below severity 16 or the two routine connection-error numbers sp_HealthParser ignores** ([#4455])
- **A managed PostgreSQL 17 store whose settings carried an old 2 GB maintenance_work_mem starts again after the move to darling-managed.conf** ([#4444])
- **After a long outage across an upgrade, repaired hourly query statistics also reach the daily rollup, so the hourly tier's retention isn't held** ([#4439])
- **PostgreSQL statement statistics count a re-created statement entry's work since it was re-created, instead of under-counting it; on PostgreSQL 16, where the entry's creation time isn't available, such a row is recorded as unknown** ([#4435])
- **Wait statistics on servers where a scheduled job clears them are counted from each clear instead of being recorded as unknown; only the work between the last collection and the clear stays unknown** ([#4434])
- **The raw purge gate judges each rollup against the rows that rollup can hold, so an hour of CPU-unknown query statistics at the floor can't hold the purge forever** ([#4432])
- **Query statistics record a restarted cached plan's executions, duration and CPU since the restart in full, instead of under-counting them; a restart the collector cannot place in time is recorded as unknown rather than estimated** ([#4431])
- **After an outage across an upgrade, the raw purge resumes within the hour on a running store, with no second restart** ([#4430])
- **The daily retention sweep no longer drops raw query statistics over a hole no rollup holds; the gated service-triggered purge owns those tables, and purge_now reports what it held** ([#4429])
- **Query statistics no longer record a false zero CPU when only the CPU counter's delta is unknowable; those rows keep their executions and duration** ([#4423])
- **FinOps no longer reports a database as idle when its only recent samples were a plan's first sighting** ([#4417])
- **Alert notebook: the collector-freshness check now counts only successful collection runs** ([#4416])
- **`get_store_host` now honours request cancellation** ([#4412])
- **Web reads honour request cancellation for 2 more trend charts and 3 analysis tools** ([#4411])
- **Web reads for PostgreSQL index bloat, column stats, CPU utilization, log events and logging audit now honour request cancellation** ([#4409])
- **The settings redactor no longer masks a short value whole on its first call** ([#4408])
- **darling-managed.conf is rewritten only when its settings change** ([#4407])
- **Stop re-holding the raw retention purge for a healthy store whose successor rollup hasn't run its first refresh yet** ([#4406])
- **Carry an operator's own `postgresql.conf` lines below the `darling-managed.conf` include across a major PostgreSQL upgrade** ([#4405])
- **Web reads honour request cancellation (10 more tools)** ([#4402])
- **Fixed a raw-purge gate that could hold history forever, and a repair
- **Web reads honour request cancellation (11 more SQL Server tools)** ([#4393])
- **The PostgreSQL settings redactor has a bounded matching time** ([#4392])
- **An armed raw-retention purge no longer runs when PostgreSQL starts, before the service can hold it** ([#4391])
- **PostgreSQL web reads now honour request cancellation** ([#4390])
- **PostgreSQL-target MCP reads now cancel on client disconnect** ([#4388])
- **Web reads now honour request cancellation for 15 more MCP tools** ([#4387])
- **Web reads honour request cancellation for Alerts, Memory Grants and Health MCP tools** ([#4386])
- **Threaded cancellation through 10 PostgreSQL-target web reads** ([#4373])
- **Web reads for object-stats and config-history now honor request cancellation** ([#4372])
- **Deprecated Dashboard's XE ring-buffer collectors no longer reshred unchanged data** ([#4371])
- **Bucketed the memory-grant and TempDB usage viewer trend charts** ([#4364])
- **Bucketed the blocking-trend charts' lock-wait, waiting-task, and blocked-session reads** ([#4362])
- **Bucketed the CPU scheduler, session stats, and plan cache trend reads** ([#4361])
- **Blocking and deadlock web reads now cancel with the request** ([#4203])
- **Health Parser web reads now cancel with the browser request** ([#4359])
- **Four Performance-Trends web reads now stop their store query when the request is abandoned** ([#4357])
- **A slow first-run database create no longer fails Darling's Postgres bootstrap** ([#4352])
- **Passwords and other secrets in stored PostgreSQL settings are now redacted** ([#4351])
- **Eight Queries-tab reads in the web viewer stop their store query when you leave the page** ([#4350])
- **Configuration-tab reads in the web viewer stop their store query when you leave the page** ([#4347])
- **PostgreSQL `pending_restart` can now be trusted after a reload, on Windows targets** ([#4345])
- **Heal the Postgres v8 hardware-sizing block on stores resized before the #4225 fix shipped** ([#4342])
- **Fleet Sweeps reports an error instead of "no sweeps" when its store read fails** ([#4328])
- **Lite Overview baseline bands used the wrong hour under a custom time range on a server not on UTC**
- **Dashboard query comparison grids and Server Trends baseline bands now use the server's local time** ([#4321], [#4317])
- **`get_collection_health`'s default reply now fits the MCP response budget** ([#4319])
- **Dashboard Overview ghost line reads the right hours outside UTC** ([#4317])
- **The Query Store activity slicer reads less history on each refresh** ([#4311])
- **Lite's Overview ghost line reads the right hours outside UTC** ([#4309])
- **Availability Groups tab no longer freezes the UI every 30 seconds** ([#4308])
- **The Daily Summary calendar and `get_daily_summary_range` cache closed days for an hour** ([#4307])
- **Query Store backfill's candidate check no longer reads old compressed chunks, and no longer drops a hole on a database that goes quiet** ([#4306])
- **The Darling viewer's Wait Stats and Perfmon trend charts read time buckets, not every collection** ([#4304])
- **The Darling viewer's Active Queries grid and wait drill-down no longer load every snapshot's plan XML up front** ([#4303])
- **Lite's Queries-tab comparisons use the grid's time window** ([#4302])
- **Active Queries and wait drill-down no longer load every plan's XML just to show the grid** ([#4297])
- **The query heatmap loads much faster on large windows** ([#4295])
- **Less write-ahead log from the query, procedure and Query Store history tables** ([#4294])
- **Web viewer no longer shows raw error text for failed requests** ([#4293])
- **Empty legacy baseline aggregates now drop instead of staying forever** ([#4292])
- **CPU and I/O-latency baselines recompute once a day, not every hour** ([#4291])
- **Fewer needless writes to the Darling store's plan and text tables** ([#4288])
- **Managed store: less WAL, from compressed full-page images** ([#4287])
- **Web failure handling: log batches, bad requests and error text** ([#4286])
- **Time-based reads no longer shift on a store outside UTC** ([#4285])
- **Web viewer timeouts now show a message and get logged** ([#4281])
- **A major store upgrade keeps your ALTER SYSTEM settings** ([#4280])
- **Lite says when a query window was cut short** ([#4279])
- **Top-CPU reads disclose a short window** ([#4278])
- **`get_query_store_top` stays under the MCP response-size budget** ([#4198])
- **`describe_custom_view_catalog` stays under the MCP response budget** ([#4198])
- **MCP `get_collection_health` caps its response size** ([#4198])
- **MCP `get_blocking` (Darling) and `get_blocked_process_reports` (Lite) cap their response size** ([#4198])
- **MCP get_collection_log stays under the response-size budget by default** ([#4198])
- **`get_query_store_regressions` default calls stayed under the MCP response budget** ([#4198])
- **get_pg_io_trend's default call now fits the MCP response budget** ([#4263])
- **`get_active_queries` stays under its response-size budget by default** ([#4261])
- **get_index_usage's default answer now fits the response budget** ([#4260])
- **`get_query_heatmap` default call stays under the MCP response budget** ([#4259])
- **`get_object_locking` default response stays under the MCP budget** ([#4258])
- **`get_plan_corrections` default call stays under the MCP response budget** ([#4198])
- **Job History loads faster and stops polling every 30 seconds** ([#4256])
- **Availability Group reads: bounded the newest-snapshot lookup** ([#4228])
- **`get_deadlock_detail` no longer returns oversized deadlock graphs by default** ([#4254])
- **Database Sizes and Storage Growth load without a multi-chunk planning tax** ([#4252])
- **Alert triage links now skip only a disabled dashboard, and email carries one too** ([#4230])
- **PLAN_REGRESSION no longer deduplicates the whole raw Query Store slice every pass** ([#4208])
- **Force-plan bot no longer acts on a best plan older than 4 days** ([#4208])
- **`audit_config` no longer runs the full analysis pass** ([#4206])
- **`get_query_store_regressions`' comparison baseline is now a fixed 7 days** ([#4206])
- **`get_pg_cpu_utilization` now returns bucketed points instead of one row per minute** ([#4206])
- **The desktop viewer's Query Store Regressions grid had the same unbounded-baseline shape as `get_query_store_regressions` and is now bounded the same way** ([#4206])
- **The deadlock and plan-capture log patterns only offer a report whose ERROR: is the line's own label** ([#4042])
- **Plan capture keeps working under a custom log prefix with fields before the process id** ([#4016])
- **Trace flag reads no longer hide every enabled flag after an ordinary run** ([#4032])
- **The fleet card's collection-health figures no longer reread a week of raw log rows on every call** ([#3911])
- **The per-server collection-health summary stops leaking memory and stops going stale on a fleet with more than one active caller** ([#3900])
- **A collector that produced then stopped no longer reads healthy with zero rows** ([#3889])
- **job_history collection survives an msdb reseed** ([#3886])
- **The compression-stuck self-alert no longer pages on a healthy job's own run instant** ([#3588])
- **pg_deadlocks reads a self-hosted target's csvlog file, closing the same forged-line hole #4124 closed for pg_log_events** ([#4136])
- **README no longer lists community tools as installed automatically** ([#4159])
- **README corrected for Darling on Linux and PostgreSQL** ([#4158])
- **pg_plan_capture reads a self-hosted target's csvlog file** ([#4137])
- **pg_log_events on RDS and Aurora now reads the csvlog file too** ([#4135])
- **Darling's daily retention purge no longer holds up collection** ([#4133])
- **Lite's get_index_usage no longer hides active indexes behind a silent cap** ([#4131])
- **pg_log_events reads a self-hosted target's csvlog file, closing a forged-line hole** ([#4124])
- **Two install-lock tests pass on a non-elevated machine** ([#4111])
- **MCP tool schemas no longer list an internal service as a parameter** ([#4110])
- **Log reads through pg_read_binary_file now decode text in the database's own encoding** ([#4106])
- **Store Checkpointer Pressure no longer fires all the time on a healthy store** ([#4096])
- **A planted line in a PostgreSQL server's log can no longer stop plan capture** ([#4089])
- **A Lite Query Store test that never ran now runs** ([#4084])
- **A PostgreSQL log in a non-UTC zone no longer reports a deadlock problem on the log events collector** ([#4070])
- **The install and upgrade scripts name each account once in a refusal** ([#4069], [#4050])
- **One bad byte from a failed login no longer blinds the three PostgreSQL log readers, and RDS and Aurora targets get the time-zone fix too** ([#4051])
- **Darling's install and upgrade scripts refuse a folder that ordinary users can already write to** ([#4050])
- **A planted log line in another time zone no longer stops a PostgreSQL server's log events and deadlocks from being read** ([#4049], [#4051])
- **PostgreSQL servers whose log prefix puts fields before the pid now show their log events and deadlocks, and a client's port is no longer stored as an event's SQLSTATE** ([#4047])
- **On Windows the log-hash key is refused if anyone beyond SYSTEM, Administrators and the service account holds any right to it, and it is checked and read through one held file handle** ([#4044])
- **A PostgreSQL server that logs only to CSV is reported by name instead of looking quiet** ([#4040])
- **Install and upgrade lock the Darling install folder, so an ordinary local user can no longer replace the service's binaries** ([#4038])
- **The hourly re-mask of PostgreSQL deadlock data stored before #4005 now runs without a log-hash key, covers finding alerts, and never leaves a raw report hash behind** ([#4036])
- **On compose, a log-hash key or role password planted in a credentials directory that was open to other users is discarded instead of trusted, `--harden-files` no longer follows a junction or a hard link, and the Viewer's always-empty Statement Fingerprint column is gone** ([#4031])
- **Trace flags could show a flag as enabled days or weeks after it, and every other flag, was turned off** ([#4030])
- **On-load collectors could read HEALTHY forever even after their daily reschedule silently broke** ([#4029])
- **The PostgreSQL log tail could pick the csvlog or jsonlog file instead of the real log** ([#4025])
- **PostgreSQL deadlock reports, deadlock alerts and analysis findings stored before the SQL normalization are rewritten in place** ([#4022])
- **PostgreSQL log events are identified by hashes keyed with a per-store secret, so a store reader can no longer test guesses at the values a log line hid** ([#4020])
- **Plan capture could be spoofed by a statement's own author** ([#4015])
- **PostgreSQL deadlock reports store and show their SQL normalized, and no read returns a hash of the raw report** ([#4013])
- **Analysis passes stop recomputing every 30-day baseline on every pass** ([#4011])
- **CPU scheduler readings no longer flicker between two answers when a collection lands on a duplicate timestamp** ([#4010])
- **The rest of the trend family now buckets to a point budget instead of returning every collection** ([#4007])
- **get_pg_io_trend's automatic subject choice no longer scans the whole window in order** ([#4003])
- **Custom alert rules now evaluate on the Linux compose store** ([#4002], [#3983])
- **On-load config collectors recapture daily, so a long-lived connection keeps fresh config facts and a cleared trace flag actually clears** ([#4001])
- **get_store_metrics reads each store object's newest sample through an index instead of sorting a year of rows** ([#3998])
- **list_servers and the per-server summary tools no longer plan a store's whole collection history to find one timestamp** ([#3995])
- **PostgreSQL config and logging-audit tools no longer plan a year of chunks to read one row** ([#3992])
- **A store with no readable server-log directory keeps its hourly collector-cost flush** ([#3985])
- **Lite's daily summary starts its retention horizon where Lite's history actually starts** ([#3984], [#3975])
- **The web dashboard and the MCP server no longer connect to a compose or bring-your-own store as its owner** ([#3983])
- **A fresh store's fleet overview and web Fleet page work before the collection-health aggregate materializes** ([#3981])
- **Latest-value lookbacks follow the collector's cadence, and the PostgreSQL target's config reads stop planning every retained snapshot** ([#3980], [#3931])
- **The top-CPU drill-down resolves statement text for the five queries it prints, not for every plan-cache row in the window** ([#3979])
- **A server dark past the collection log's retention reads Offline everywhere, not "Awaiting first collection"** ([#3975], [#3966])
- **The service's start-up hole scan no longer reads every aggregate's whole materialization and source table** ([#3972])
- **The trend tools answer in time buckets sized to the window: a day of `get_file_io_trend` is 34 KB, not 1.4 MB** ([#3968])
- **A server dark for more than two days reads Offline on the fleet card, not "Awaiting first collection"** ([#3966])
- **A restart's shutdown checkpoint no longer counts toward checkpoint write and sync time either** ([#3964])
- **PostgreSQL config change history reports per-database and per-role overrides being set, changed and reset** ([#3957])
- **The plan-regression drill-down no longer re-deduplicates the whole Query Store slice, and the parameter-sensitivity drill-down stops resolving text for rows it never prints** ([#3956], [#3953])
- **A PostgreSQL restart no longer raises a false Store Checkpointer Pressure warning or reads as a WAL-forced checkpoint on a monitored server** ([#3955])
- **Log masking fails closed: a DETAIL or CONTEXT is read as SQL only where PostgreSQL writes SQL, a value cut before its close is masked to the end, and `get_store_query_stats` masks a raw text `pg_stat_statements` kept** ([#3952], [#3920])
- **The web server page's Overview tab no longer waits on the daily summary** ([#3950])
- **The fleet overview reads each server's newest sample instead of every retained row, and the web viewer asks for it once per refresh instead of twice** ([#3947])
- **A PostgreSQL target's analysis reads its statements' own baselines once per metric, not once per statement** ([#3946])
- **`get_store_metrics` answers with a bounded summary instead of every store object's daily series** ([#3942])
- **Managed role provisioning never sends a role's password in a statement** ([#3940])
- **A store without TimescaleDB re-runs its store-object convergence every hour, not only at restart** ([#3932])
- **Analysis latest-value reads look back a day instead of scanning a server's whole history, and stop counting dropped databases** ([#3931])
- **A store upgrade that fails before its commit point puts the old cluster back as it found it, and only claims a revert that happened** ([#3927])
- **Log text keeps no literal: the store's own log and every PostgreSQL target's log events mask the SQL inside DETAIL and CONTEXT, every retained store-log entry is masked, and the statement reader shows only normalized DML** ([#3920], [#3915])
- **The store's hourly self-metrics sweep works again on TimescaleDB 2.29 and later** ([#3918], [#3908])
- **The bundled store moves to TimescaleDB 2.30.1, which closes GHSA-hcfx-29v5-2rcw, and a store's extension now moves before the store opens instead of under a live store** ([#3908])
- **A runtime update no longer stops the store from starting when the previous runtime's folder cannot be cleared** ([#3919], [#3906])
- **A store still on PostgreSQL 17 no longer fails to start on a host with 40 GB of RAM or more** ([#3909])
- **The store's statement statistics, reviewed before release: the ALTER SYSTEM advice no longer bricks the store, and the reader's filter can no longer be switched off** ([#3915], [#3904])
- **The bundled PostgreSQL moves to 18.6, and the self-contained .NET runtime to 10.0.12, closing published CVEs in both** ([#3906], [#3908])
- **Each generated store TLS root certificate carries a unique per-generation name** ([#3557])
- **The by-CPU tools now actually rank by CPU** ([#3523])
- **analyze_server no longer answers "all metrics are within normal ranges" when the analysis window collected nothing** ([#3524])
- **The Performance Calendar, daily summary, and fleet sweep band deadlocks as a measured per-hour rate, not any-deadlock-is-Critical** ([#3525])
- **Perfmon rates are honest per-second values in analysis** ([#3527])
- **Floor the SQL count thresholds, give Store Disk Pressure a GB floor, and count measured metrics in the fleet Healthy label** ([#3528])
- **get_memory_trend stops reporting granted memory as a hardcoded zero** ([#3529])
- **Lite's Query Store time slicer reads physical reads from its own column** ([#3530])
- **LCK_M_IS advice carries the same RCSI caveats as its LCK_M_S twin** ([#3531])
- **Collector schedules refuse cadences that would fabricate quiet** ([#3532])
- **get_pg_plans finds the plan you asked for, not just the plans in the top page** ([#3533])
- **get_pg_autovacuum_health classifies severity from the same axis it ranks by** ([#3534])
- **get_pg_replication_slots headlines the worst-classified slot and never spells unknown WAL growth as stable** ([#3535])
- **get_pg_io_stats no longer renders track_io_timing=off as an impossibly fast disk** ([#3536])
- **The PostgreSQL xmin-horizon alert catches rotating holders and stops firing on single observations** ([#3537])
- **QueryStore slicer's physical-reads sort plots the physical series** ([#3547])
- **get_memory_trend joins the grants series so total_granted_mb carries real data** ([#3548])
- **Viewer Recommendations tabs stop saying "All clear" when the analysis window collected nothing** ([#3551])
- **Viewer pass for the Store Disk Pressure GB floor and fleet measured-metric qualifier** ([#3563])
- **Deprecated Dashboard analysis reads perfmon counters as true per-second rates** ([#3561])
- **Sorting the Procedures or Query Store grid by physical reads plots the physical series, in both apps** ([#3556])
- **The Locking & Contention grid names the table as schema.table** ([#3576])
- **Extreme, corroborated anomaly findings can now cross the notify floor, so the baseline engine is no longer notification-inert at shipped settings** ([#3526])
- **`get_store_metrics`' `job_history` block now says whose eyes its rows are visible to, and proves rows exist where it can** ([#3574])
- **The forced-plan-failures alert read no longer walks the whole fleet's two-hour slice of Query Store rows to find one server's** ([#3573])
- **The viewer's tray Snooze now silences the toast itself** ([#3570])
- **The compression-stuck self-alert no longer pages on a healthy job's run instant** ([#3575])
- **Text on an accent fill gets a measured ink in every theme, and the FinOps row marks become theme brushes Dark can actually see** ([#3577])
- **The duration-trend trio routes by retention tier and says what it served, so a 7-day request no longer returns 4 days labelled quiet** ([#3541])
- **Analysis facts divide by the time the collector actually observed, and a window with a hole in it says so** ([#3538])
- **SQL Server's Poison Wait alert measures accumulated starvation over a ten-minute window like its PostgreSQL twin, so one slow wait no longer pages and a THREADPOOL storm no longer sleeps** ([#3539], [#2711])
- **Six MCP pages now say what bounded them, so a limited page stops passing for a complete answer** ([#3541], [#3287], [#2159])
- **The four naked delta families store the interval their deltas accrued over, so a restart's fabricated zero reads as unknowable instead of 0.00 ms/sec** ([#3540])
- **Blocking and CPU health bands are rates over the window they were measured in, tiered from 14 days of fleet blocking and CPU data** ([#3539])
- **get_store_metrics stops answering for 38% of the store: continuous aggregates and the named plain tables join the inventory, every sweep is reconciled against pg_database_size, and the block says its own coverage** ([#3582])
- **The twenty continuous aggregates join the compression ladder** ([#3581])
- **PostgreSQL MCP percents name their denominator, so a three-row page stops summing to 100% of everything** ([#3541])
- **Every delta family is seeded from the store at service start, and the series-age rescue finally has passes to read, so a restart no longer fabricates one interval of quiet for six families** ([#3540])
- **MCP write tools report what happened** ([#3541])
- **The scorer's wait thresholds carry their measurement: WRITELOG stops saturating on a third of routine windows, deadlocks grade 5/hr apart from 90/hr, and the absolute gates scale with the hours actually observed** ([#3538])
- **Analysis pages with long compound stories now reach Slack instead of dying with invalid_attachments** ([#3612])
- **Continuous-aggregate materializations are chunked at one raw chunk, like every raw table** ([#3620])
- **The PostgreSQL first-target runbook stops denying three capabilities that ship** ([#3608])
- **Slack text cuts land on whole characters, not UTF-16 indexes** ([#3622])
- **A not-yet-valid web TLS certificate now raises the self-alert instead of reading as healthy** ([#3517])
- **Light and Cool Breeze status colors read against their own page, and text on a status fill gets a theme ink** ([#3609])
- **Forced Plan Failing fires once per observation, not once per cooldown** ([#3579])
- **The compression dead-job alert now says what a -infinity row is on the store's TimescaleDB** ([#3591])
- **Every delta family now stores the interval its deltas accrued over, and query_stats stores the statement offsets its delta key is made of** ([#3540])
- **The File Growth threshold means one thing — megabytes per hour — at every surface that shows it** ([#3539])
- **Story confidence measures corroboration instead of path length, and the nightly rebuild's three cards become one incident that names the job** ([#3538])
- **The daily digest and sweep rollup no longer re-announce on a service restart** ([#3580])
- **compare_analysis bands each delta by the server's own dispersion and folds one cause into one row, so same-hour-yesterday noise stops reading as a verdict** ([#3538])
- **A server nothing has banded yet is Unknown, not Healthy, and the alert-history grids show the severity the alert fired at instead of the colour its name implies** ([#3539])
- **Every latest-snapshot MCP read says when it was captured, and no tool accepts a window it does not read** ([#3541])
- **PostgreSQL servers get a measured deadlock band from their own counters, their Long-Running Query alert skips maintenance like SQL Server's does, and every one-sided alert says why it is one-sided** ([#3539])
- **Database File Growth's rise arm fires once per hourly observation, not once per cooldown** ([#3636])
- **The daily summary stops painting purged months green, and every MCP filter is part of the query** ([#3541])
- **Zero is a measurement: health parsers say whether their source was ever seen, a regression with no baseline stays null, and the first point of a differenced trend is no longer a fabricated 0** ([#3541])
- **The interval-hourly Query Store refresh stops paying twelve index inserts per re-materialized row for eleven indexes nothing reads, and the capture-down alert read stops decompressing a server's whole collection log to learn two statuses** ([#3597])
- **Top CPU Queries reads as one record per query on Slack, not seven fields interleaved across a two-column grid** ([#3644])
- **Top Cpu Queries' Max Dop is the newest plan's reading, with the cross-plan maximum kept as a dated history** ([#3648])
- **The force-plan remediation said "eligible, no blockers" for plans automatic plan correction already owned, had failed to force, or had withdrawn** ([#3652])
- **A Claude review that posted nothing no longer finishes green** ([#3650])
- **The deprecated Dashboard mirrors five of the brains-review honesty fixes in its own idiom** ([#3653])
- **Six small honesty riders from the brains-review residue** ([#3653])
- **The PostgreSQL poison-wait host holds on silence like the SQL Server engine does, and three presence-flat alerts grade their severity from the bars the health bands already measured** ([#3653])
- **The Darling viewer's trend charts and calendar tell the same truth the MCP tools learned today** ([#3653])
- **`get_pg_server_config` counted the page and called it the server** ([#3653])
- **The four PostgreSQL host alerts fire with the tier they earned instead of no tier at all** ([#3653])
- **Darling's `get_pvs_stats` trend drew an unmeasured pass as 0 MB** ([#3653])
- **The Query Store duration chart now says what served it and where the rollup's floor cut the window** ([#3653])
- **The analysis pass no longer says "collection appears to have stopped … NOT an all-clear" for a window the coverage witness proves the collector observed** ([#3653])
- **The PostgreSQL analysis vocabulary absorbs what six content lanes reported back** ([#3542])
- **A target that restarted, failed over or was re-pointed no longer keeps subtracting from the old instance's counters** ([#3653])
- **A quiet hour no longer halves the next hour's Query Store rate** ([#3653])
- **The query-stats trends read the interval the store has, on both SKUs** ([#3653])
- **Compose delta aggregates exclude the restart marker** ([#3653])
- **`mute_analysis_finding` stops writing the hash as the path and stops registering the same mute twice, `remove_server` can remove a server that never connected, and eight MCP descriptions stop saying what the code does not do** ([#3653])
- **The web server page labelled page sums as totals, drew one instant as a trend, and four tools called their capture time by another name** ([#3653])
- **The wait and perfmon anomaly baselines stop counting every restart's fabricated zero as a quiet sample** ([#3653])
- **Three PostgreSQL pages stop guessing truncation from a full page, and four of the eleven payload rules stop being sentences** ([#3653])
- **The MCP query-duration trend still divided a restart's zero into 0.00 after the viewer stopped, and two sentences that #3695/#3696 made false** ([#3653])
- **The perfmon chart plotted deltas under a "Value" label with the divisor sitting unused on the row, and a latch/spinlock restart read as zero** ([#3653])
- **One payload said NoData and No Data, and the page cut had five names** ([#3653])
- **Five families subtracted from the old instance once before anyone noticed it had changed, and Aurora's wait counters had no one watching for a restart at all** ([#3653])
- **Lite's plan-cache trend descriptions rated every point over the gap since the previous one, the shared unrated_note named one of two unrated reasons, and the latch/spinlock restart row was spelled two ways across SKUs** ([#3653])
- **Fifteen rate arms spelled "unknowable" as 0 behind a guard that happened to hide it** ([#3653])
- **A falling gauge read as a counter reset because the store did not know it was a gauge** ([#3653])
- **A maintenance job's own sub-threshold anomalies fold onto the job's incident and name the job** ([#3704])
- **PostgreSQL baselines survive a shortened retention, and pg_cpu carries the CPU dispersion floor** ([#3691])
- **`compare_analysis` sigma-bands the PostgreSQL baselined metrics** ([#3691])
- **`get_analysis_facts` now runs the anomaly detector on both engines** ([#3691])
- **Full-edition `perfmon_stats.cntr_value_per_second` divided as an integer, so every counter under one event per second read 0/sec** ([#3653])
- **The write family tells Aurora the truth** ([#3691])
- **Three hourly rollups counted every restart's fabricated zero as a sample, and a service outage longer than a day left a two-hour hole under a floor that said covered** ([#3653])
- **The PostgreSQL engine absorbs what the v2 lanes reported back** ([#3691])
- **`get_fleet_overview` runs its collection-health rollup once per minute per host, however many overview calls race** ([#3735])
- **`compare_analysis` sigma-bands the v2 PostgreSQL baselined metrics, and the write family has one definition of "WAL tracked"** ([#3691])
- **Hour-of-week baselines key on the target's local clock, not UTC** ([#3653])
- **PostgreSQL targets key their hour-of-week baselines on their own clock** ([#3691])
- **The long-query completion XE session could never be created on Azure SQL DB** ([#3753])
- **database_scoped_config collected nothing on Azure SQL DB, and said so nowhere: the three-part `[db].sys.sp_executesql` it called Azure-compatible is rejected there** ([#3755], [#3754])
- **A pass whose fact families could not be read says so, on both SKUs** ([#3691])
- **A collector run whose every item failed is no longer SUCCESS, and get_collection_health stops saying a faulted collector read and found nothing** ([#3754])
- **The PostgreSQL Long-Running Query twin honours the program/login opt-out knob** ([#3743])
- **The materialization hole scan reports on every start** ([#3756])
- **The Long-Running Query alert applies `excludedDatabases` ahead of the row cap on both SQL Server SKUs** ([#3742])
- **The CPU alert gate identifies a sample by equality, not order, and keys on the stored UTC instant** ([#3744])
- **query_store_health collects per database on Azure SQL DB** ([#3764])
- **The PostgreSQL Long-Running Query read applies `excludedDatabases` in the read, ahead of the row cap, and the Aurora wait-profile detector gates on peak AND mean** ([#3742], [#3691])
- **PostgreSQL connection saturation now divides the real population by the ceiling** ([#3691])
- **The rollup-routed daily calendar prints NULL, not 0, for a day the tier never materialized, and names it** ([#3653])
- **The CPU collector's watermark and dedup key on the UTC twin where the store has it, with the frame stated** ([#3778])
- **A quiet PostgreSQL pool is graded from numbackends alone, and the Aurora wait-profile advice names the window mean** ([#3691])
- **The trend family's window floor is `window_truncated`, not the page dialect's `truncated`** ([#3653])
- **Item 17's three unpinned description fixes get their pins: get_ag_health names its reader's traps, the catalogue's duration-trend line says rates not percentiles, and the server page reads every stamped tool by captured_at** ([#3653])
- **Four census pins the V137 wave outran**
- **The plan tools stopped saying "no CREATE INDEX text — a hint, not a design"** ([#3805])
- **pg_statement_stats runs again on PostgreSQL clusters whose pg_stat_statements is below 1.9 or installed outside public** ([#3818])
- **Held retention policies no longer wait for a restart: the coverage gate is re-judged on the running service's hourly store-maintenance tick** ([#3812])
- **The PostgreSQL engine absorbs what waves 4–5 reported back, and the stock load storm can now page** ([#3691])
- **Two counts nothing re-derived turned dev red on three checks**
- **The companion-object remedy told twenty-three clusters to UPDATE an extension none of them had** ([#3830])
- **A collector that STOPPED producing read as one that never produced** ([#3819])
- **TimescaleDB availability was decided once, by a probe whose failures last a minute** ([#3815])
- **`PG_XMIN_HOLD` grades the horizon's persistence, not one winner's** ([#3691])
- **Store job self-heal covers every policy family, and a held retention policy is never mistaken for a dead one** ([#3816])
- **A live store test stops racing the statistics collector**
- **A month of zeros is the strongest baseline there is, not the absence of one** ([#3691])
- **A configuration lever hanging off a diagnostic chain is attached to that finding instead of orphaned into a card of its own** ([#3691])
- **Darling's Entra service-principal connections can find their authentication provider** ([#3838])
- **Every button in every theme recognizes its access key again** ([#3835])
- **Store self-alert triage pages stop rendering "Could not resolve server" for every metric added after #2768** ([#3833])
- **`get_collection_health` per server no longer races the store's write bands with an unmemoized 7-day scan** ([#3856])
- **The typed story path reaches the finding, six readers stop splitting its display string, and the lever beside an incident finally names the read that acts on it** ([#3859])
- **An argument a tool does not declare is refused by name instead of silently dropped, on both SKUs, from one filter** ([#3870])
- **`checkpoint_timeout` says `not_applicable` in the FACT on Aurora, not just at the tool that renders it** ([#3868])
- **FinOps Locking & Contention stops showing databases that were renamed away** ([#3876])
- **Darling's four object-locking reads stop showing databases that were renamed away** ([#3878])
- **The pin that held the defect as intended behavior is renamed, which is why this was its own issue.**
- **A census caught what no pin was watching for, and its roster grew rather than shrank.**
- **`provision-roles.sql` stops at a role-name collision** ([#4809])
- **Command-line verbs that open the store report an unusable store setting instead of crashing** ([#4809])
- **Azure SQL Database and Managed Instance servers show their platform, not a SQL Server year** ([#4870])
- **Lite's MCP plan tools say Lite keeps no plans** ([#4871])
- **Lite's MCP tools and FinOps inventory read the collected server properties** ([#4872])
- **Alert History's expanded rows show their labels** ([#4873])
- **The server health starter dashboard can be created again, server pages show display names, and count charts use whole numbers** ([#4874])
- **Darling stores Query Store text and plans for Azure SQL Database** ([#4875])
- **Azure SQL Database no longer shows the host's hardware as its own** ([#4876], [#4882])
- **FinOps stops advising shrinks without CPU data, or VM and memory changes on Azure SQL Database** ([#4877])
- **File I/O shows real file sizes on Azure SQL Database, and no size for a Hyperscale log** ([#4878])
- **Web panels show the history their read keeps when the Range asks for more** ([#4881])
- **Azure SQL Database figures use the database's own vCores and memory limit** ([#4879])
- **A Hyperscale log file no longer counts as ~1 TB of database storage** ([#4880])
- **Wait names with a trailing space are trimmed, and two Hyperscale timer waits are ignored** ([#4884], [#4931], [#4939], [#4941])
- **Lite no longer stores events twice, or loses its settings, after its 512 MB archive-and-reset** ([#4887], [#4918], [#4910])
- **Perfmon rate counters show a per-second rate** ([#4883])
- **On Azure SQL Database, empty tabs say their collector doesn't run there, and the page file and memory state read n/a** ([#4888])
- **Lite keeps its saved settings through a crash during the archive-and-reset, and no longer double-counts an interrupted one** ([#4889])
- **Azure SQL Database shows its own CPU count and calls its memory a limit** ([#4882])
- **Other databases on an Azure SQL Database server show their allocated size, with used space beside it** ([#4890])
- **On Azure SQL Database, an unknown volume is no longer reported as 0 MB free** ([#4892])
- **A new Azure SQL Database Hyperscale database no longer raises a wait-profile anomaly on every analysis window for its first three days** ([#4893], [#4923])
- **Lite's FinOps advice, High Impact Queries and the SQL Server Agent check read archived data too** ([#4921])
- **Lite keeps its learned database states, and its database state alerts stay right, after the 512 MB archive step** ([#4917])
- **Alert History lists analysis alerts under the server's display name** ([#4926])
- **The Darling Viewer shows servers added or removed elsewhere without a restart** ([#4928])
- **The Darling Viewer and Lite no longer close with an error when you check or uncheck a picker item** ([#4920])
- **A new server no longer reports that a collector never ran, or that it's AWS RDS, before the collector is due** ([#4915])
- **Silencing one of several databases that share a display name no longer silences the others, and their alerts no longer share a dedup key** ([#4908])
- **Blocking and deadlock counts for a time window agree everywhere, counted by when each event happened** ([#4909], [#4913])
- **Collection Health no longer makes a collector's latest counts look like a total for every run** ([#4914])
- **An Azure SQL Database logical server monitored at master stores each deadlock once, and loses none** ([#4900], [#4910])
- **On Azure SQL Database, a monitored master no longer repeats the blocking and deadlock alerts, findings and counts of databases you also monitor on their own** ([#4894], [#4906], [#4925], [#4927], [#4932])
- **Lite's Recommendations tab says there isn't enough data yet for a newly added server, instead of "All clear"** ([#4901])
- **FinOps no longer gives an Azure SQL Database logical server's master database a provisioning verdict or right-sizing advice** ([#4896])
- **FinOps picks up a server added while the app is open** ([#4898])
- **FinOps right-sizing advice says how much data it's based on, and no longer advises "reduce from 4 GB to 4 GB"** ([#4905])
- **The Darling Viewer says when Azure SQL Database doesn't collect a tab's data, instead of showing an empty grid** ([#4904])
- **Databases on one Azure SQL Database server no longer get mixed up** ([#4899])
- **Lite says why a tab is empty when its collector doesn't run on Azure SQL Database** ([#4903])
- **Optimized locking is read on Azure SQL Database, and a flag that couldn't be read shows as unknown, not off** ([#4907])
- **Storage Growth shows n/a, not 0, when there's no earlier size to compare** ([#4902])
- **Lite no longer stops collecting and alerting for every server some time after a crash or forced close** ([#4930], [#4929])
- **A Darling update that brings a new PostgreSQL runtime no longer waits for the next restart when the old runtime folder is briefly locked** ([#4934], [#4935])

### Security

- **Scrubbed legacy plan-force-action audit text** ([#4384])
- **Collected PostgreSQL statement text applies the same sensitive-statement filter as the store's own statements** ([#4383])
- **Stored PostgreSQL setting values are redacted in more shapes, and the one-time scrub resumes a large server-day after a restart** ([#4380])
- **Plan-force journal reads no longer return pre-#4326 exception text** ([#4363])
- **Stopped `/api/ping` and the analysis notes from echoing raw exception text** ([#4326])

## [3.8.0] - 2026-09-17

Full entries: [docs/changelog/3.8.md](docs/changelog/3.8.md)

### Added

- **The web dashboard's TLS certificate warns before it lapses, on the channel you already watch** ([#3514])
- **A per-collector database scope, so an expensive per-database collector can be limited to a representative sample instead of turned off for the whole server** ([#3477])
- **The High CPU card names the active maintenance operation** ([#3495], [#3493], [#3013])
- **The Long-Running Query card names the Agent job** ([#3497], [#3495], [#1140])
- **A store's self-alerts can name the store they came from** ([#3500])
- **Fleet Sweep Reports: the sweep with memory, on the web dashboard** ([#3466])
- **A 90-second plan-flip pileup can page, from the collectors that were already current while it ran** ([#3467], [#2296], [#2138], [#3464])
- **A mute rule that has outlived its reason now reports itself** ([#3306])
- **Non-interactive Microsoft Entra auth for headless targets: Service Principal and Managed Identity** ([#3484], [#3485])
- **User-authored custom alert rules** ([#3285])

### Changed

- **The `fanout` block publishes the slowest item's share of the pass - the verdict `dominance` was being read as** ([#3502], [#3477])
- **The same-statement pileup is scoped to the database whose plan it is** ([#3474], [#3469], [#2138])
- **A server's deadlock health band reads a RATE, not a raw count** ([#3368])

### Fixed

- **The Viewer's Add Server dialog can say PostgreSQL** ([#3499], [#3244], [#2158], [#2218])
- **The alerting layer's recent-runs read no longer sorts a server's whole retention history to return ten rows** ([#3496])
- **The Collector Cost Digest reaches Slack on exactly the fleets busy enough to want it** ([#3493], [#3446])
- **Clicking a sweep on the timeline no longer answers "never recorded" about a document the page is displaying** ([#3487], [#3478])
- **The watch-item worklist names its servers** ([#3482], [#3476])
- **An alerts-on sweep's stored report no longer claims a would-have-paged check that was never made** ([#3478])
- **`get_collection_log` can answer the slow-run question it exists for, and says what window it actually reached** ([#3287], [#3278])
- **A failed mute-rule reload no longer drops every mute in force** ([#3354])
- **Webhook payload timestamps come from one clock read per firing** ([#3355])
- **The Retention Held thresholds are settable, so the alert an operator most needs to tune is no longer the one alert that cannot be** ([#3297], [#2136], [#2349])
- **`Stale Mute Rules` now honours a mute rule that NAMES it and ignores one that only reaches it incidentally, so the one alert an operator could not answer has an off switch** ([#3348], [#3343])
- **Per-event alert emails attach their own incident's graph** ([#3330])
- **`delivery.cooldown_minutes` is reachable from the control plane** ([#3314])
- **High CPU now requires the condition to persist** ([#3282])
- **A mute rule created or deleted outside the Viewer did not take effect, for an unbounded time, and every surface said it had** ([#3315])
- **Webhook and email alerts no longer re-render incidents that are still inside their own cooldown window** ([#3313])
- **A Collector Cost Regression self-alert now has to be worth reporting, not merely real** ([#3316])
- **Analysis-finding alerts no longer render their Diagnosis facts twice on every delivery channel** ([#3302])
- **A deadlock or blocked-process report whose statement sat inside a stored procedure now names the procedure** ([#3307])
- **The installer and the portable launcher are signed, and a release-time guard refuses to publish an unsigned executable anywhere else** ([#3288])
- **PostgreSQL instance CPU bands on Aurora Serverless v2 capacity headroom, not on percent-of-allocated** ([#3281])
- **`get_pg_index_bloat` now carries its own coverage denominator, so a limited page cannot read as a coverage claim** ([#3278])

## [3.7.0] - 2026-09-10

Full entries: [docs/changelog/3.7.md](docs/changelog/3.7.md)

### Added

- **The member-range scan now reports ranges that stop short of their own content, which it previously read as healthy** ([#3230])
- **The clock-frame census now sees a renderer reached through a one-hop wrapper** ([#3207])
- **`.github/dependabot.yml` now tells whoever reviews a `Microsoft.Data.SqlClient` or `Azure.Identity` bump to check whether `ExcludeBrokerCredential`'s default or the broker's packaging changed** ([#3219])
- **Lite now records which Azure credential `DefaultAzureCredential` selected for an Existing Sign-In (`az login`) connection** ([#3224], [#3219])
- **A guard over the boundary three clock-frame defects shipped through** ([#3208])
- **Added a sixth authentication mode, Azure — Existing Sign-In (`az login`), for Azure SQL Database where Microsoft Entra MFA cannot work** ([#3214], [#3196])
- **A collector's PostgreSQL extension dependency is declared rather than described** ([#3187])
- **A refresh-ceiling staleness finding, reported separately from where the reading sits against its slot** ([#3182])
- **A TimescaleDB compression run is watched against the clearance its own minute of the hour has before the next continuous-aggregate refresh starts** ([#3112])
- **Remediation credential seam and journal actor** ([#2138])
- **A collector definition can now record what it measured on its own `collection_log` row** ([#3161])
- **Every alerting-side store read that fails now records how long it ran before it faulted** ([#3099])
- **Every `<see cref>` target in the repository is now resolved, and the ones that resolve to nothing are inventoried** ([#3086], [#3025])
- **`TsqlConventionGuardTests` enforces the checkable part of `CONTRIBUTING.md`'s T-SQL Style list, which nothing had enforced: PR #3078 introduced `COUNT(*)` and `COUNT(DISTINCT …)` into a new collector query and passed the Linux build, the PostgreSQL tests, every whole-tree guard, the command-deadline family, `review` and `verify`** ([#3081])
- **A collector stalled mid-read now gets a server-wide wait sample taken out of band** ([#2880])
- **`get_pg_plan_capture_readiness`, and the PostgreSQL read-coverage ratchet reaches zero** ([#3070])
- **A monitored PostgreSQL target's `lc_messages` is now a named precondition rather than an invisible one** ([#3061])
- **Pinned the PostgreSQL panel registry against both the XAML that renders its panels and the load path that fills them, resolving every one of the 27 registry collectors to its panels through the two names its loader already carries** ([#3062])
- **Watch the heaviest hourly continuous-aggregate refresh's live runtime against the refresh slot it has to fit inside, on the existing hourly job-health sweep** ([#3044])
- **A guard that no XAML layout grid assigns a `Grid.Row` or `Grid.Column` at or past its own definitions** ([#3049])
- **The monitoring store now reads its OWN PostgreSQL server log, as a per-class census rather than as lines** ([#3021])
- **The alerting subsystem's own store reads failed silently** ([#3013])
- **The retention-horizon convergence's “no-op on every later start” claim is now asserted rather than only stated** ([#2960], [#1958])
- **A new command-timeout pin can no longer arrive with its own private copy of the deadline judgement, or quietly stop using the shared one** ([#2972], [#2938], [#2940], [#2966])
- **A new data-moving migration rung can no longer land without re-deriving the advisory-lock wait budget** ([#2894], [#2888], [#2936])
- **The per-database plan/text fetch split is now ten columns on the run's row, so the phase that owns a deferred fetch can be aggregated instead of scraped per server** ([#2860], [#2811], [#2859], [#2864], [#2902], [#2851])
- **The Azure per-database branch now emits a `connect:`/`open:`/`drain:` phase split, the last collection path that reported one blended number** ([#2855], [#2164], [#2312], [#2851], [#2854], [#2864], [#2811], [#2819], [#2859], [#2860])
- **A collector losing cycles to the wall-clock budget no longer reports `HEALTHY` with `errors: 0`** ([#2804], [#2803], [#2779], [#2784])
- **An abandoned collection cycle now records what it was DOING, not merely that it stopped** ([#2864], [#2673], [#2859])
- **The server-scoped phase split is now a set of columns, so it can be aggregated and read without an SSM session** ([#2859], [#2851], [#2811], [#2796])
- **Server-scoped collectors now emit the `open:`/`drain:` phase split, so the largest collector on the fleet can be attributed** ([#2851], [#2811], [#2816], [#2796])
- **The sweep-body detach policy (#2700/#2717) is now pinned, including the invariant that makes it safe** ([#2840], [#2841])
- **`query_store` plan and text fetch phases now name the STORE half separately from the TARGET half** ([#2811], [#2312])
- **OIDC sign-in for the web dashboard: token validation and claim-to-role mapping** ([#2550])
- **The auto force-plan bot's detection half arrives, and it cannot write to anything** ([#2138])
- **`timeBucket` + `topN` compose into a top-N time series** ([#2734], [#2737], [#1687], [#2733])
- **A notebook panel cell can pin its own time window** ([#2735], [#1606], [#2733])
- **Custom-view validation rejects unknown keys instead of silently ignoring them** ([#2733])
- **`get_query_store_duration_trend` stops ranking the raw Query Store slab per call** ([#2736], [#1849], [#1665], [#1759])
- **Query Store gets total-CPU and total-duration compose measures** ([#2732])
- **Alert deliveries carry a linked triage page** ([#2710])
- **Postgres targets get the Poison Wait alert** ([#2711])
- **PostgreSQL/Aurora targets get a High CPU alert, backed by a new instance-CPU collector** ([#2719])
- **Fleet overview stops false-flagging active servers as Offline** ([#2699])
- **`pg_wait_stats` stops re-shipping unchanged wait events** ([#2695], [#2691])
- **Collection health distinguishes a collector that has gone dark from one that is actively failing** ([#2694])
- **PostgreSQL wraparound Warning signals autovacuum losing the race, not routine proximity** ([#2693], [#2689])
- **`pg_deadlocks` now works on Aurora and RDS** ([#2692], [#2538])
- **`pg_statement_stats` stops re-shipping idle statements** ([#2691])
- **query_store retires the runaway detector for a flat candidate-plan ceiling** ([#2690], [#2683], [#2685])
- **`plan_correction` binds on compatibility-level-100 databases** ([#2688])
- **`plan_correction` stages its recommendations before touching Query Store** ([#2687], [#2673], [#2680], [#2686])
- **The live Query Store fetch stops joining a view it never reads** ([#2680])
- **The tool now monitors its OWN cost on the servers it watches** ([#2674])

### Changed

- **A census count stated over a population the summary does not republish can no longer be satisfied by assertion** ([#3200])
- **A sizing constant cannot be tightened on a population too thin for a maximum over it to bound anything** ([#3193])
- **Web dashboard custom views now record WHO changed them** ([#2550])
- **Assert collection size with `Assert.Single` and `Assert.Empty` in the source-scanning test pins, clearing all 24 `xUnit2013` analyzer warnings** ([#3132])
- **Lite: log verbosity becomes a setting, and per-database collection timing moves below the default** ([#3104])
- **The fleet card's collection flag is named for what it measures** ([#3098])
- **Darling: per-cycle collector timing telemetry moves to `Debug`** ([#3102])
- **Consolidated the 34 private `ReadRepoFile` helpers in `Darling.Tests` onto one shared `RepoFile` authority, in the `Compile Include` shape #2913 sanctioned, reconciling eight different path-resolution semantics onto one** ([#3090])
- **Rewrote the 33 abbreviated `int` data types across 7 collector and service query strings to `integer`, and moved `CONTRIBUTING.md`'s data-type abbreviation rule from unguarded to enforced in `TsqlConventionGuardTests`** ([#3087])
- **The cross-app guard's C# matcher stops at three syntactic shapes, and that boundary is now a decision with a census behind it** ([#3082], [#3063], [#3067], [#3076])
- **The cross-app guard asks MSBuild for a project's items instead of evaluating MSBuild by hand** ([#3074], [#3065])
- **Recorded the real provenance of `HeaviestHourlyRefreshObservedCeilingSeconds` — the single status-verified run behind it, the ten-run post-boundary distribution (194–594 s, mean 338 s), the transition-run ambiguity, the rule that keeps the unstatused self-metrics series out of it, and why the value is unchanged in both directions** ([#3069])
- **`CrossAppGuardCiGateTests` now sees a cross-app read spelled as a bare directory name** ([#3076])
- **Cross-app guard coverage now sees a SKU's test tree, not just its app directory**
- **The cross-app CI-gate guard now reads project files as well as C# source, so a cross-app source read expressed as an MSBuild `Compile Include` is seen rather than silently missed** ([#3063])
- **Kept `FleetIdentifierScrubTests`' commentary to what maintaining the guard requires, and restated its ordinal ceiling as a property of the shape being matched rather than an inference from past renames** ([#3018])
- **The migration rung bound turns out to bind, and the residual it leaves is a rung SHAPE rather than a missing mechanism** ([#2894], [#2935])
- **A migration rung can no longer trap its own cancellation** ([#2894])
- **#2894's own account of what breaks is corrected** ([#2894], [#2936])
- **`procedure_stats` renders execution-plan XML on one cycle in four instead of on every cycle, which buys us-east-1 roughly 30 s of collection cadence at no CPU cost** ([#2862], [#2843], [#1767], [#2849])
- **The identifier guard now sweeps for a server named by short name and ordinal, and records what it cannot sweep for** ([#2490])
- **The references #2900 left unpinned now use synthetic slugs too** ([#2490])
- **Fixture display names and short-name comment references now use synthetic slugs too** ([#2490])
- **Darling's slice-repair collapse keeps staging the FULL projection, because on PostgreSQL the width is what makes the plan safe** ([#2876], [#2771], [#1907])
- **Lite's server-scoped watermark read stays UNBOUNDED, and the asymmetry with its per-database twin is now documented as deliberate** ([#2800], [#2795], [#2344])

### Fixed

- **Fleet cards now report a PostgreSQL/Aurora target's instance CPU** ([#3267])
- **A server card no longer reports a metric as Healthy when nothing measured it** ([#3271])
- **Azure master-connected size collection no longer dies the moment sys.resource_stats has sibling rows** ([#3262])
- **The Query Store tab's first/last-execution columns had no row-level value pin — the coverage shape that let the #3207/#3221 clock-frame defects ship** ([#3253])
- **A test pinned the Query Store regression row's last-execution display as the raw server clock, which after the #3207 clock-frame work fails on any non-UTC machine and verifies nothing on CI's UTC runners** ([#3248])
- **The PostgreSQL first-target runbook stopped describing a nine-collector product** ([#3247], [#2566], [#2538], [#2692], [#2530], [#3102], [#2625], [#3239])
- **The Darling README's PostgreSQL inventory stopped at 12 of 27 collectors, and now cannot silently do that again** ([#3261], [#3072], [#2603], [#2566], [#2658], [#2661], [#2719], [#2625])
- **MCP `list_servers` no longer names the wrong engine in its version key** ([#3245])
- **The viewer's test-connect request now names the engine it wants probed, and the probe reply's engine facts are pinned as contract** ([#3244])
- **A PostgreSQL target's registry row no longer claims a SQL Server major of 0** ([#3243])
- **An absent optional PostgreSQL extension no longer reads as a permissions failure** ([#3240])
- **The PERMISSIONS hint on the self-hosted PostgreSQL log readers no longer prescribes a role that cannot fix them** ([#3239], [#2566])
- **A full solution Rebuild is warning-clean again** ([#3241])
- **The onboarding probe no longer claims msdb access on Azure SQL Database targets** ([#3237])
- **`upgrade-darling.ps1`'s post-upgrade checklist no longer reads a healthy server count as a failure** ([#3238])
- **The de-skew's accuracy claim is now scoped where a column can outlive a DST transition** ([#3231])
- **Desktop timestamp columns were rendered in the wrong clock frame in both SKUs, in opposite directions, with each SKU correct exactly where the other was wrong** ([#3207], [#3221])
- **Four doc comments stated a column's clock frame incorrectly, one of them appealing to a sibling tab as precedent and one asserting a cross-SKU parity that did not hold** ([#3207])
- **Composed panel queries now assert that every compiled statement binds exactly the parameters its plan predicts and cites every one of them** ([#3211])
- **`ServiceCommandDeadlines.SerialLoopSeconds` keeps its value of 5 and loses the chain bound that justified it** ([#3204])
- **The SQL-text pins over the object-stats, PVS and plan-correction reads are now insensitive to alias qualification and whitespace** ([#3217])
- **`CONTRIBUTING.md`'s build instructions name the deprecated Dashboard and CLI Installer projects at their real paths under `deprecated/`, matching what `README.md` already said** ([#3226])
- **Sixteen fields across five MCP tools returned the monitored server's local wall clock, unmarked, beside naive-UTC fields on the same JSON object** ([#3206])
- **The product version is declared in exactly one place** ([#3222])
- **`StoreSqlClockDisciplineTests` scanned store SQL one string literal at a time, so a predicate assembled by concatenation was split before the discriminator could see it** ([#3223])
- **`DarlingPgReadSqlParsesLiveTests` had a floor of 10 against a population of 49, and one shipped read was outside it** ([#3223])
- **The Query Store liveness touch guard is one constant, six hours wide** ([#3189])
- **`get_default_trace_events` returned `event_time` in the monitored server's local wall clock** ([#3198])
- **Composed-panel event annotations from `default_trace_events` were both selected and plotted in the monitored server's local clock** ([#3198])
- **Adding an Entra MFA server could fail with a Windows broker error that was recorded nowhere** ([#3196])
- **The store disk-pressure check stopped walking the whole store every five minutes to decorate an alert message** ([#3199])
- **`CompressionPhaseGuardMinutes` is a DECLARED four-minute width rather than the light-refresh ceiling rounded up** ([#3188])
- **The two refresh-ceiling constants state one estimator and one closure rule, word for word, and name themselves PREFIX MAXIMA rather than maxima over closed populations** ([#3188])
- **`query_store`'s `sql_duration_ms` was mostly store time, under a column documented as the monitored server's** ([#3192])
- **A PostgreSQL target's permissions are documented as what they actually gate: one grant, two extension install kinds, and the six collectors that need one** ([#3184])
- **The hourly refresh band separates the aggregates whose runs are long enough to collide, inside the minutes it already had** ([#3185])
- **The refresh-slot log-level test's routine-band comment states what the case establishes, instead of narrating a repaired failure** ([#3181])
- **The TimescaleDB hourly refresh grid is re-derived so contention between refresh policies cannot depend on where a view sits in a list** ([#3174], [#3168], [#3166])
- **`HeaviestHourlyRefreshObservedCeilingSeconds` is re-derived from a per-run census, 594 s becomes 896 s, and the watch-line ordering the #3044 signal rests on inverts** ([#3166])
- **Darling: the TimescaleDB job-execution-logging GUC now heals on existing stores instead of only fresh ones** ([#3175])
- **`pg_index_bloat`'s per-index ceiling and cycle budget are now sized against a MEASURED block rate, and it came in at half the assumed one** ([#3164], [#2997], [#3153], [#2673])
- **Alert history states which channel a row is about** ([#3169])
- **The PostgreSQL runbook lists `pg_index_bloat` in its cadence and MCP tool tables** ([#3165])
- **`pg_index_bloat` rotates which indexes it measures instead of re-measuring the largest one every cycle, so the `skipped_reason` it stamps on the rest states a deferral a later cycle actually honours** ([#3153])
- **The measured index-bloat read keeps the latest MEASUREMENT of each index in the window rather than the latest row** ([#3153])
- **`pg_index_bloat`'s per-database rotation cursors are retired when their database stops being enumerated, so they no longer accumulate for the life of the server** ([#3153])
- **`pg_index_bloat` and `pg_index_usage_stats` each state their own index population and name the other's** ([#3158])
- **`get_collection_health`'s `output_finding` no longer explains every zero-row collector as an event collector at rest** ([#3160])
- **PostgreSQL column statistics now say WHICH cause produced an empty result** ([#3154])
- **`pg_write_stats` now collects the checkpointer and background-writer counters on Aurora PostgreSQL** ([#3156])
- **The web dashboard's composer tells a read-only seat that its account is read-only** ([#2550])
- **Every locked-mode restore in CI can fail its step now, and the merge gate can finally see a lock-file mismatch** ([#3143])
- **The Darling viewer's fleet sidebar labelled a PostgreSQL target "SQL Server v0", and the MCP's `list_servers` published the same string** ([#3145])
- **The crude second-opinion parse in `CrossAppGuardCiGateTests` no longer loses project items to an apostrophe** ([#3140], [#3138])
- **The `tempdb Space` alert said "used" while measuring reserved space** ([#3144])
- **The store COPY's start phase — `BeginBinaryImportAsync` awaiting `CopyInResponse`, which is where a store-side lock wait stalls — ran on Npgsql's undocumented 30 s default rather than the collection-sweep deadline, because `NpgsqlBinaryImporter.Timeout` bounds only the row loop below it and the connection's `CommandTimeout` is read-only** ([#3139])
- **The refresh slot envelope is stated against a measured maximum, and `get_store_metrics` says a daily point is a last snapshot** ([#3119])
- **A UI-thread timeout was deciding `Lite.Tests` outcomes in a host with no UI thread** ([#3134])
- **`TimescaleSupport.cs` described the routine refresh band as a universal over a population that grows every hour, in three places** ([#3133])
- **The repo-file reader census in `Darling.Tests` could not see a qualified call to either reader, so its exact-set equality passed with an undeclared LF-reading file in the tree** ([#3129])
- **The gated-live PostgreSQL job no longer reports green having tested nothing when a shared library changes** ([#3116])
- **A PostgreSQL command timeout no longer decides WHICH MACHINE's deadline fired from SQLSTATE `57014` alone** ([#3118])
- **`get_pg_index_bloat` no longer fails outright on an empty index.** ([#3121])
- **`pg_index_bloat`'s size ceiling and work budgets are enforced by selecting the relation `pgstatindex` is applied to, rather than by quals on a `LEFT JOIN LATERAL` to it** ([#3109])
- **Extended #3095's COPY phase axis to the three AWS-API store ingestors (`RdsPlanIngestor`, `RdsCpuIngestor`, `RdsDeadlockIngestor`), so every binary COPY the service performs reports which phase a fault came out of, pinned by a scan asserting both that every COPY site stamps and that no other site does** ([#3111])
- **A collector store-write timeout on a server whose target is PostgreSQL no longer receives the target-read remedy ("shrinking the work") or the monitored target's database name** ([#3111])
- **`RefreshSlotWarningSeconds`' doc comment rejected the 480 s alternative on a miscount of the readings quoted beside it** ([#3107])
- **A collector's store write that fails on a transport fault in the COPY's start phase is re-attempted once on a fresh store connection instead of costing the cycle** ([#3099])
- **A COPY start-phase store-write failure is now distinguishable from a data-phase one, by a caller and not only in a log** ([#3095])
- **`HeaviestHourlyRefreshObservedCeilingSeconds` is re-derived from the clean post-narrowing distribution and now records its own derivation method** ([#3101])
- **Fixed the T-SQL convention guard naming the wrong member in its offender list: a type being constructed, a local variable, or a bare C# keyword at 48 of the 160 T-SQL literals it reads** ([#3094])
- **Hardened the PostgreSQL panel-ownership pin to read its two C# sources through the shared source walker, and to assert the loader-body scan it stands on** ([#3088])
- **Corrected and pinned the counted claims in `Darling/README.md` and `README.md`** ([#3072])
- **Store Job Over Cadence no longer tells an operator to change what the store collects** ([#3060], [#3044])
- **Source-scanning pins now read the shared C# walk instead of dropping lines by comment prefix, which read the body of a block comment as code because this codebase puts no asterisk on the continuation lines** ([#3052])
- **Managed store: pin `lc_messages = 'C'` in `postgresql.conf` (v10 marker-guarded block) so PostgreSQL writes its severity labels untranslated** ([#3053])
- **The PostgreSQL Activity tab's Query Shapes sub-tab assigned a grid row it never declared, so the captured-plans note and grid shared one cell; the Storage tab's predicate panel was numbered two rows low and drew on top of the index-bloat pair** ([#3049])
- **`pg_wait_sampling` and `pg_predicate_stats` were filled only by the Activity tab's load path while rendering on Waits and Storage, so both panels were empty until Activity had been visited — an empty wait-sampling panel reads as a server that offers only the Aurora instrument, and an empty predicate panel hides the index candidates the hypothetical-index action is driven from** ([#3050])
- **The AG page's rollup numbers now say what they count, and the id derivation is shared rather than copied** ([#3045], [#3038], [#3031])
- **The PostgreSQL deadlock panel rendered on the server tab's Vacuum tab while only the Activity tab's load path filled it, so it was empty until Activity had been visited — indistinguishable from a server with no deadlocks** ([#3048])
- **The `pg_deadlocks` schedule comment claimed a cadence match with `pg_plan_capture`; they run five and sixty minutes apart** ([#3047])
- **The WPF Overview's fleet deadlock total now reports how much of the fleet it covers** ([#3042])
- **The live-cleanup ratchet read only the top directory, excluded on a substring where it meant a call, and justified its exemption with a reason that was false** ([#3036], [#3014], [#1776], [#1902])
- **Compression policies are pinned to a phase grid clear of the refresh slots instead of drifting through them** ([#3035], [#3012], [#1778], [#1586])
- **Web dashboard: each fleet rollup tile's number is now programmatically associated with its label and its coverage sub-line via `aria-describedby`, rather than by reading-order proximity alone** ([#3031])
- **Deadlock capture read nothing at all from a managed PostgreSQL target's log, and reported the window as read and empty** ([#3030])
- **The viewer's read deadline is priced against the pool the seat actually has, and the two fleet-wide fan-outs no longer start more reads than it has permits** ([#3016])
- **`get_collection_health` reported what each collector spent and never what it bought: every statistic on a row was a cost figure and the rows count lived on `get_collector_cost`, a different tool over a separate hourly, fleet-wide series** ([#3017])
- **Corrected `PgDeadlockLogParser`'s account of what a truncated deadlock report costs** ([#3009])
- **The hourly continuous-aggregate refreshes re-materialize a day instead of three, and start on distinct minutes of the hour** ([#3012], [#1778], [#1937])
- **The live-cleanup ratchet decided which files to scan, which teardowns were exempt, whether a teardown was converted, and where a teardown's block ended by matching substrings against raw source, so a doc comment could make any of those decisions** ([#3014])
- **Viewer fan-out width declarations are now censused by shape rather than by their `Task.WhenAll`, so a fan-out that fires its reads unawaited is covered** ([#3019])
- **`get_fleet_overview`'s `total_deadlocks` now reports how much of the fleet it covers** ([#3027])
- **The RDS/Performance Insights ingest note no longer describes a source the cycle never opened** ([#3027])
- **Darling's whole-tree guards now run on changes the product-area path filters do not reach** ([#3026])
- **Collector runs that end in a classified fault now record the run's real elapsed time in `collection_log` instead of three literal zeros, so a permission denial, a lock-timeout yield and a missing XE session are no longer stored as instantaneous** ([#3022])
- **`get_collection_health` no longer presents `last_error` as though it were current** ([#3010])
- **Aurora/RDS PostgreSQL log ingestion no longer loses a window of deadlock reports or `auto_explain` plans when a collection cycle fails after fetching it** ([#3008])
- **A retention command was inheriting Npgsql's 30 s default, and the pins that exist to say so could not see it** ([#2874])
- **The retention-convergence live test no longer reads TimescaleDB's scheduler running the armed policy as a convergence bug** ([#2937], [#1889], [#2143], [#2818], [#1680])
- **The command plane, the two command handlers that read the store, the Query Store backfill and the config reload beacon now carry explicit deadlines - and two of them had an enclosing budget that never bounded them** ([#2874], [#2148], [#2795], [#2344], [#2796], [#2888], [#2901])
- **Lite's shared-surface loads are now ordered, so the earlier of two overlapping reads can no longer land last** ([#2933], [#2929])
- **#2874's three straggler regimes now carry FIVE derived deadlines, and the `hypopg_reset()` one goes UP where the others go down** ([#2874], [#2888], [#2901], [#2826], [#2882], [#2940], [#2819], [#2786], [#2928])
- **The collection-sweep deadline's floor no longer credits `CommandTimeout` with bounding a phase it cannot reach** ([#2874], [#2819])
- **A comment that quotes the deadline can no longer certify an untimed collection-sweep site, and the fixture that was supposed to witness that no longer passes for the wrong reason** ([#2874])
- **The collection-sweep census can now see a construction that carries its namespace, and it records which of its sites address the store rather than a monitored target** ([#2874], [#2928], [#2901])
- **A deadline written only inside a comment no longer reads as a real one on the MCP and web read surface** ([#2874])
- **The Query Store backfill's per-server gate now stays closed while an abandoned slice is still running, and releases on every outcome path** ([#2165], [#2148], [#2874])
- **The viewer scoped-paint pin reads the shared brace walk, so no copy of it is left to drift** ([#2927], [#2929])
- **A `CommandTimeout` spelled in a COMMENT satisfied `AlertPassCommandTimeoutTests`' deadline check, so an untimed alert-pass command read clean** ([#2942], [#2874])
- **The control-plane reload watermark now records the version that was actually applied**
- **The Lite MCP CPU and Default Trace reads now window in the offset of the server they were asked about** ([#2967], [#1262], [#2511])
- **The latest-CPU reads no longer scan a server's whole retained history to return one row** ([#2964])
- **A future copy of the latest-CPU read can no longer drift back to the old shape unseen** ([#2973])
- **`/api/ping` answered `"ok"` while collection had never started; it now reports the collector's real state** ([#2953], [#2936], [#2117])
- **The last two command-timeout pins stopped certifying a site from a NEIGHBOUR's deadline** ([#2972], [#2938])
- **A control-plane reload whose store read failed no longer discards the operator's change** ([#2951], [#2936])
- **Role provisioning no longer writes a `statement_timeout` it failed to read, and no longer swallows cancellation to do it** ([#2952], [#2931], [#2918])
- **Every store command on the startup path now carries an explicit deadline, and this is the one regime in the sweep whose value goes UP** ([#2874], [#1772])
- **`memory_pressure_events.sample_time` was written in the monitored server's LOCAL wall clock while both of its readers assume naive UTC, so the memory-pressure series sat one UTC offset away from every other lane** ([#2932], [#1262])
- **The horizon convergence is now pinned to name `config` and nothing else, so it cannot arm a retention policy** ([#1937], [#1680], [#1877])
- **A second migrator no longer dies undiagnosed waiting on the migration advisory lock, and no longer dies at all when it had nothing to apply** ([#2894], [#2888], [#2920], [#2874])
- **The alert pass's CPU read now carries the deadline the rest of the pass already had, and the guard that declared the pass clean can no longer miss a site by living in the wrong file** ([#2874], [#2826], [#2882], [#2928], [#2786])
- **Collection Health counts an abandoned cycle from the row, so a window that predates [#2803] no longer reports clean** ([#2926], [#2804])
- **A transient failure of any of the three collection-blocking startup steps no longer ends collection for the life of the process** ([#2936], [#2935], [#2038])
- **A viewer surface that carries every scope's answer no longer paints an answer read for a scope the operator has since left** ([#2924], [#2901], [#2907], [#2910])
- **The collection sweep's store reads and writes now carry an explicit deadline - including the binary COPY, a construction shape this sweep could not see** ([#2874], [#2810], [#2871], [#2882], [#2888], [#2901], [#2819], [#1581], [#2170], [#2822], [#1772], [#2913])
- **`ViewerFleetTimerGuardTests` no longer passes when the fleet-timer fan-out moves BELOW the tab early-return, which is the regression [#2907] named** ([#2923], [#2913])
- **The source-walking idiom behind five test pins no longer blanks interpolated-string holes, so a call written inside an interpolation is visible to the scans built on it** ([#2913])
- **The compose `statement_timeout` now reaches the live roles on a reload, instead of only on a service restart** ([#2918], [#2917])
- **A control-plane reload no longer drops the compose `statement_timeout` knob on the floor** ([#2917], [#2357], [#2918])
- **The per-database phase split is now printed on the fault path, which is the one case it was added to explain** ([#2896], [#2855], [#2811], [#2851], [#1875])
- **The viewer's two fleet timers can no longer stack store reads, and the Overview no longer double-fires one of them** ([#2907], [#1566], [#2901])
- **Deferred plan and text fetch debt is no longer paid by whichever collector visits the database next** ([#2902], [#2312], [#2847], [#2841], [#2860], [#2898])
- **The server-scoped watermark read no longer runs for a collector whose own cycle throws the answer away** ([#2797], [#2795], [#2796], [#2860])
- **The IL-scan idiom behind five reachability pins is now a real instruction decoder, and can see generic callees** ([#2898], [#2890])
- **Every command on the Darling service's MCP and web read surface now carries an explicit deadline** ([#2874], [#2871], [#2736], [#2826], [#2918])
- **Every command in the Darling viewer now carries an explicit deadline** ([#2874], [#2882], [#2888], [#1566], [#2871])
- **Every command in the Darling storage layer now carries an explicit deadline** ([#2874])
- **The analysis pass's last two commands now carry an explicit deadline, and the pin can finally see the shape that hid them** ([#2874], [#2871], [#2810])
- **The retention live tests now carry their evidence into the failure message** ([#2818], [#1564])
- **A healthy server mid-sweep is no longer painted with the red Offline overlay** ([#2794], [#2792], [#1562], [#2473])
- **Abandon-path forensics no longer record a session id of 0** ([#2884], [#2673])
- **Every read in the alert evaluation pass now carries an explicit deadline** ([#2874], [#2826], [#2810], [#2871])
- **A held Ctrl+V during a busy clipboard no longer spawns several 'Pasted Plan' tabs from one keypress** ([#2870], [#2837])
- **Retention held by the rollup-coverage gate is now visible, instead of reading as a healthy job** ([#2813], [#2809], [#2136])
- **Every command in the analysis pass now carries an explicit deadline, not just the fact collector's** ([#2871], [#2810], [#2820], [#2826], [#2827], [#2344])
- **The slice-repair survey no longer holds one read lock across its entire multi-phase read, and can now actually be cancelled** ([#2761], [#2748], [#2465], [#2770])
- **The one-time Query Store slice repair no longer runs out of memory on a large store** ([#2771], [#2770], [#1912])
- **The Plan Viewer's inner tab handler is now subscribed once, not re-subscribed on every reopen** ([#2828])
- **The plan-regression analysis read had no command deadline, so it inherited one nobody chose** ([#2810], [#2827], [#2826], [#2809], [#2344])
- **Managed-store Postgres sizing now re-derives when the HOST changes, not only when the formula does** ([#2845], [#1559])
- **Four enumerated-path phase stamps were skipped whenever the phase they timed threw** ([#2854])
- **`Collector Cost Regression` compared the day's TOTAL cost, so a cadence recovery read as a regression** ([#2846], [#2768])
- **PR CI now runs `Lite.Tests` when only Darling changes.** ([#2839])
- **Plan Viewer clipboard retry no longer blocks the UI thread while the clipboard is busy** ([#2837], [#2833])
- **Plan Viewer paste no longer crashes the app when the clipboard can't be opened** ([#2833])
- **The plan-regression query, ~90% of all store statement cancellations, is no longer sorted globally** ([#2827])
- **A fact collector that could not run is no longer indistinguishable from one that found nothing** ([#2826])
- **FinOps "View Stored Plan" no longer crashes the whole Darling Viewer** ([#2825])
- **The `query_store` fetch split lines now report the probe's INPUT size, not just the ids it attempted** ([#2823], [#2819], [#2822])
- **The Query Store plan and text fetches stop opening their own store connections** ([#2819], [#2811], [#2796])
- **Web dashboard: a sparse trend chart spanned only its data burst, so an old incident read as current** ([#2802])
- **The `io_latency` baseline no longer times out, and eight other metrics got faster with it** ([#2820])
- **A `query_store` fetch probe that throws no longer reports `probe:0ms` and hands its whole cost to the `other:` residual** ([#2816])
- **Headless web Alert History: a tray-only alert now reads "Logged", not a misleading "Sent"/"Not sent"** ([#2814], [#2781])
- **Web dashboard: the server Daily Summary showed "Health: NaN"** ([#2807])
- **A wall-clock-budget-abandoned cycle no longer records as `SUCCESS`** ([#2801])
- **Web dashboard: a pinned notebook panel cell showed no "Pinned" badge in the read view** ([#2788])
- **The server-scoped watermark read now carries #2344's chunk-pruning bound** ([#2795])
- **query_store plan and text fetch: force HASH JOIN so the in-memory Query Store TVF is read once** ([#2791])
- **Web dashboard: the composer's partial-window notice read "1 days" on a one-day store** ([#2785])
- **PostgreSQL statement text was never stored on ANY Aurora server, because the Aurora fetch does not deduplicate `queryid`** ([#2786], [#2651], [#2284])
- **Web dashboard: an offline server no longer shows a green "Collectors OK"** ([#2779])
- **Darling Viewer and Dashboard: an offline server's Overview card no longer shows a green "Collectors OK"** ([#2784], [#2779])
- **Web dashboard: an over-long time range showed a raw API validation error instead of a range hint** ([#2780])
- **Web dashboard: Alert History dropped the meaningless "tray" delivery channel** ([#2781])
- **`query_store` plan and text fetches: an EXPLICIT command timeout on both halves, replacing two defaults nobody chose** ([#2776])
- **query_store fetch failures re-paid full cost every cycle, forever** ([#2776])
- **Web dashboard: server cards got NARROWER as the window got WIDER, until the stat tiles clipped** ([#2772])
- **Web dashboard: a chart panel with a single collected time-bucket read as "not enough data points" beside siblings that plotted** ([#2773])
- **`/api/triage`: store self-alerts no longer render three "Could not resolve server" errors** ([#2768], [#2710])
- **Lite: a store with a large pre-#1907 Query Store backlog could not start at all** ([#2748])
- **`plan_correction` collector: `OPTION(RECOMPILE)` restored to the payload's two statements** ([#2766], [#2760], [#2764])
- **`plan_correction` collector: the shipping SELECT still joined the Query Store catalog views live** ([#2764], [#2673])
- **query_store collector's open-interval re-read cost outgrew the window it was tuned against, fleet-wide** ([#2759])
- **Darling Viewer: the Fleet Health overview tab's server total wobbled against the sidebar's stable count, and could claim a false all-clear** ([#2753])
- **MemoryPressureEventsCollector threw "Arithmetic overflow error converting expression to data type int" on every collection, minutes after #2749's fix deployed** ([#2755])
- **Darling Viewer: Availability Groups grid sorted numeric columns as text** ([#2757])
- **The CPU chart degrades to disconnected dots after roughly an hour, on every server** ([#2749])
- **query_stats grouped by object_name no longer collapses every ad-hoc statement into one null-labeled row** ([#2737], [#1568])
- **A read-only Viewer seat could silently discard local Default Time Range / Auto-refresh interval changes** ([#2715])
- **The plan-correction collector can no longer run minutes on a monitored server** ([#2673])
- **The runaway plan-fetch cap now HOLDS instead of oscillating off** ([#2685])
- **Heavy collectors can no longer occupy a monitored server for minutes** ([#2673])
- **The Query Store collector stops grinding on a plan-churning tenant** ([#2683])
- **Deadlocks collection no longer fails on every Azure SQL DB target** ([#2681])
- **PMLite lost Azure SQL DB deadlock and blocked-process capture until an app restart** ([#2677])
- **Query Store plan and text capture decompressed each item up to 3x per cycle** ([#2675])
- **`upgrade-darling.ps1` crashed before copying anything on an install with no prior rollback backups** ([#2671])

## [3.6.0] - 2026-08-27

Full entries: [docs/changelog/3.6.md](docs/changelog/3.6.md)

### Added

- **Store exposure admits both roles at once** ([#2665])
- **PostgreSQL I/O and database trends** ([#2663])
- **The first PostgreSQL trend reads** ([#2663])
- **PostgreSQL deadlocks, not just the count** ([#2661])
- **PostgreSQL configuration, and what changed in it** ([#2658])
- **Test an index from the predicate grid** ([#2612])
- **Azure SQL DB now reports every database's size, not just the connected one** ([#2643])
- **Mark rows in grids** ([#2645])
- **Test a PostgreSQL index without building it** ([#2612])
- **The last eight PostgreSQL collectors are readable outside the Windows Viewer** ([#2629])
- **Sampled waits and per-query OS CPU are readable from the MCP and the web dashboard** ([#2629])
- **Self-hosted PostgreSQL now answers "which queries cost the most"** ([#2625])
- **Permanent-gap messages now name the collector that DOES answer the question** ([#2625])
- **PostgreSQL plan capture reaches Aurora and RDS** ([#2538])
- **`get_pg_plans`: the plan itself, not a pointer to one** ([#2567])
- **PostgreSQL execution plans, captured and redacted** ([#2566], [#2538])
- **Which PostgreSQL columns are filtered on, and where the planner is wrong about them** ([#2603])
- **OS CPU and disk per query on PostgreSQL** ([#2603])
- **PostgreSQL wait analysis stops being Aurora-only** ([#2603])
- **Three PostgreSQL collectors could not say which database their rows described** ([#2599])
- **PostgreSQL index bloat, MEASURED rather than estimated** ([#2561])
- **What is resident in PostgreSQL shared buffers, with the join every published example gets wrong** ([#2544])
- **PostgreSQL replication connections, with the measure that actually catches a stalled standby** ([#2544])
- **PostgreSQL column statistics, with the customer data deliberately left behind** ([#2543])
- **PostgreSQL lock state by mode, type and relation** ([#2544])
- **PostgreSQL extension availability: the third capability axis, and the only actionable one** ([#2545])
- **PostgreSQL write-side collection: checkpoints, background writer and WAL** ([#2544])
- **A captured PostgreSQL plan is an orphan without `%Q`, so plan-capture readiness now checks for it** ([#2538])
- **The web session cookie can now carry WHO is holding it, and the signature covers it** ([#2550])
- **PostgreSQL targets now say whether plan capture is even possible** ([#2564])
- **HTTPS for the web dashboard, so the access token stops crossing the LAN in the clear** ([#2562], [#2550])
- **The sessions behind a pinned xmin horizon, and the ones that only look like it** ([#2540])
- **`idle in transaction` is not automatically starving your vacuum, and this is the finding the read exists to make** ([#2540])
- **A held horizon is a share of samples, not a flag** ([#2540])
- **No statement text is stored, and that is a decision rather than an omission** ([#2540])
- **The permission that gutted this feature silently, caught before it shipped** ([#2540], [#2542])
- **The denominator, and a defect only a live run could find** ([#2540], [#2508])
- **PostgreSQL index usage, with the half that decides whether an index can actually go** ([#2541], [#2530])
- **The scan count only a monitoring store can produce** ([#2541])
- **PostgreSQL table bloat: the damage, next to the cause chain we already collected** ([#2542])
- **The bloat measurement decision, settled by measurement rather than by reasoning** ([#2542])
- **The bloat estimate is suppressed, not captioned, when its inputs cannot be trusted** ([#2542])
- **The permissions trap that would have shipped a tool telling everyone to VACUUM FULL everything** ([#2542])
- **A Storage tab on both front ends, and neither read is MCP-only** ([#2541], [#2542])
- **Version floors for both collectors, measured against six live majors rather than read from documentation** ([#2541], [#2542])
- **Both new collectors are fan-outs, and the cost of that was budgeted rather than discovered** ([#2541], [#2542])
- **A fourth kind of nothing: `precondition`, for a setup step somebody can actually change** ([#2546])
- **The WPF viewer renders PostgreSQL tabs at a PostgreSQL target, and stops rendering nineteen empty SQL Server ones** ([#2530], [#2547])
- **The two Aurora-only PostgreSQL panels are SHOWN on stock PostgreSQL, not hidden** ([#2530])
- **One copy of the PostgreSQL read SQL, shared by the MCP surface and the desktop viewer** ([#2530])
- **The viewer's server rows carry the engine discriminator, from BOTH server reads** ([#2530])
- **PostgreSQL temp-file spills, cache hit ratio, deadlocks and the rollback ratio - one collector, one read** ([#2539])
- **A PostgreSQL statistics reset is reported AS a reset** ([#2539])
- **The web dashboard renders PostgreSQL tabs at a PostgreSQL target, and stops rendering twelve empty SQL Server ones** ([#2530], [#2539], [#2532])
- **The store records what ENGINE a monitored target is** ([#2530])
- **Ten viewer surfaces the browser and MCP could not reach now have read endpoints** ([#2484])
- **An in-place upgrade can now remove the files the new build stopped shipping, and always names them ([#2529])** ([#2185], [#2525])
- **A supported in-place upgrade script, because the step that had no script was the one accumulating 5.48 GB** ([#2525], [#2185])
- **The web dashboard gets a per-query drill-down, which is what makes `get_query_trend` reachable there at all** ([#2520], [#2353])
- **The analysis family can be anchored at a past window too, and `analyze_server` refuses to persist when it is** ([#2506], [#2495])
- **`analyze_server` accepts the anchor and does not persist an anchored run** ([#2506])
- **Every windowed read can be anchored at a past incident, not just at now** ([#2495])
- **Five ready-made dashboards, so a new user's Custom Views page is not an empty one** ([#2480], [#2437], [#1563])
- **The web dashboard's server page gets the viewer's tabs: twelve sections, 61 of the 82 served reads, and a time range** ([#2475], [#2422], [#1949])

### Changed

- **The SQL-Agent collectors no longer gate on msdb access, so running the GRANT we advise actually does something** ([#2559])
- **`get_pg_top_queries` and `get_pg_blocking` return their int8 IDENTITIES as strings, because a JSON number was rounding them** ([#2548])
- **Fifteen reads stopped answering "nothing happened" and "nothing was collected" with the same sentence** ([#2485])
- **`get_collection_health` serves what a HEAVY run costs, not only what runs cost on average** ([#2460], [#2459], [#2446])
- **`sweep_pressure` answers the single-sweep question as well as the sustained one** ([#2446], [#2296])
- **Lite's portable ZIP is self-contained, which HALVED it** ([#2501], [#2489], [#2499])

### Fixed

- **Ten PostgreSQL MCP tools were never registered with the host, so no agent could call them** ([#2659])
- **PostgreSQL 18 silently lost every I/O byte figure, and the estimate it replaced was off by an order of magnitude** ([#2655])
- **A PostgreSQL column that the server's VERSION removed read as a missing measurement** ([#2653])
- **Self-hosted PostgreSQL had no query TEXT, so `test_hypothetical_index` could never work there** ([#2651])
- **Azure SQL DB captured almost no deadlocks, and could not say so** ([#2641])
- **Lite forgot the time range you picked** ([#2640])
- **Lite's Database Sizes grid looked broken on Azure SQL DB** ([#2640])
- **A missing-extension message said CREATE EXTENSION without naming which database** ([#2638])
- **`get_index_usage` hid whole databases behind a fixed 200-row cap and never said so** ([#2636])
- **RDS plan capture reported SUCCESS "no new plans" when the AWS call was DENIED** ([#2633])
- **`IsAwsRds` was never set on a PostgreSQL target, so plain RDS PostgreSQL could never capture plans** ([#2633])
- **A summary count taken over a capped result read as a fact about the server** ([#2629])
- **`pg_wait_sampling` excluded only `Activity`, so `Client`/`ClientRead` was 100% of the profile** ([#2630])
- **An unknown column `format:` rendered as raw text instead of failing** ([#2629])
- **`--enable-web` on a non-Windows host reported the wrong reason and hid the path that works** ([#2626])
- **A per-database collector that failed in SOME databases reported SUCCESS with no note** ([#2623])
- **Three PostgreSQL collectors wrote fewer payload values than they declared columns** ([#2599])
- **`pg_index_bloat` had never returned a single row** ([#2617])
- **The README documented an upgrade script that no released build contains** ([#2593])
- **Tag management had no discoverable entry point in the Darling Viewer, and none at all on a viewer with no servers** ([#2595])
- **The plan-capture remedy recommended the one setting measured to be catastrophic** ([#2565])
- **`extension_available` said no on every PostgreSQL server in existence, including ones actively running auto_explain** ([#2564])
- **The Viewer's connection self-test probed a different connection string than startup opens** ([#2578])
- **A gated-off collector logged a fake SUCCESS on SQL Server targets** ([#2579])
- **Rollback backups kept an unhardened copy of `darling.json`** ([#2574])
- **The upgrade-path gate was climbing from a store nobody has** ([#2572])
- **`get_running_jobs` still claimed a server had no jobs running when we were never allowed to look** ([#2559])
- **A wildcard `listen` refused every LAN request with a 400 — which is what the compose distribution ships** ([#2569])
- **Editing a REGISTERED server in `darling.json` was silently ignored, and the warning about the adjacent case made that worse** ([#2552], [#2252], [#2254], [#2158])
- **`get_pg_top_queries` had never returned a row on any engine: its SQL did not parse** ([#2554], [#2219])
- **A read that throws now gets the same honest capability answer as one that comes back empty** ([#2554])
- **On a PostgreSQL target, 55 more reads stopped telling you to go and check a collector that will never run** ([#2532], [#2511], [#2530])
- **Eight PostgreSQL reads told a SQL Server target its vacuum was healthy, and stock PostgreSQL was told to go and check a collector that will never run** ([#2532], [#2530], [#2518])
- **The Overview lanes skewed apart when one lane's numbers got long** ([#2533])
- **Lite's Overview lanes carried the same latent gutter misalignment as [#2533], now fixed there too** ([#2535])
- **A SQL Server read aimed at a PostgreSQL target says so, instead of telling you to check collection** ([#2530], [#2511])
- **A fresh store never seeded, so the control plane silently overrode `darling.json`** ([#2524])
- **The `tempdb Space` alert measured distance to the next autogrow, not distance to the ceiling** ([#2515], [#2512])
- **Nineteen Darling reads (eighteen on Lite) told an Azure SQL Database user to go and start a capture that cannot exist there** ([#2511])
- **Azure SQL Database gets tempdb monitoring back, and the reason it lost it was checkably false** ([#2512], [#2515])
- **Lite's .NET runtime prerequisites were documented against the wrong artifact** ([#2489], [#2481])


## [3.5.0] - 2026-08-19

Full entries: [docs/changelog/3.5.md](docs/changelog/3.5.md)

### Added

- **Alert on database file SIZE growth, graded per server** ([#2349])
- **`Database` and `LastEventUtc` on the incident projection** ([#2361])
- **Every fingerprinted alert now carries a monotonic occurrence total, not just blocking and deadlocks** ([#2362])
- **`--harden-files`: re-apply the secret-file ACLs from an elevated prompt** ([#2352])
- **Declared peer stores: a Darling MCP server that names its siblings instead of answering "unknown server"** ([#2339])
- **`cpu_attribution` on the top-CPU rankings: what fraction of the box the ranking explains** ([#2320])
- **`get_query_store_health`: the MCP read for the new collector, both SKUs** ([#2319])
- **Per-database Query Store health: a new `query_store_health` collector, both SKUs, both stores** ([#2319])
- **V75 gives plan CONTENT its own retention horizon, because the fact-coupled one cannot bound a young store** ([#2316])
- **The generic webhook can now hand automation the alert's structure: `{{context_json}}`, `{{incidents_json}}` and `{{dedup_key}}`** ([#2302])
- **get_collection_health now carries a sweep_pressure verdict, so half-rate collection stops hiding behind 40 healthy collectors** ([#2296])
- **V74 stores query_store statement text out of the sorted stream, with its own watermarked fetch, prune and Viewer probe** ([#2150])
- **The query_store collector can now resolve statement text through a separate watermarked fetch, so the text stops being sorted** ([#2150], [#2210], [#1556], [#2164])
- **The darling.json reconciliation now tells "never registered" from "deliberately removed"** ([#2258], [#2252], [#2280])
- **The store's scale test now reports what the compression job DID, not only how long it took** ([#2266], [#2136], [#1902])
- **PostgreSQL statement text is stored, so `get_pg_top_queries` returns something readable** ([#2219])
- **`add_servers` refuses a registration whose connection lands in a database already monitored** ([#2280], [#2228], [#2220], [#2277], [#2158], [#2218])
- **Add Server warns when a SQL-auth password may not be decryptable by the service** ([#2279], [#2255], [#2273])
- **A skipped database-state maintenance cycle now says so, once** ([#2266], [#2189], [#2203])
- **`server_id` identity now carries engine and port, without re-keying anything that exists** ([#2218], [#2213], [#2158])
- **A registration connected to a database it does not name now says so** ([#2228], [#2220], [#2158], [#1535])
- **Editing a server's address keeps its identity, so its collected history stays attached** ([#2158], [#2228], [#2218])
- **The database-state heal tests no longer assume a best-effort maintenance cycle always runs** ([#2266], [#2189], [#2203])
- **`get_top_queries_by_cpu` can rank a procedure's dynamic SQL as ONE statement: `group_by: "host_object"`** ([#2235], [#2012])
- **The Query Store tick and the Query Store backfill no longer run against one server at the same time** ([#2165], [#2058], [#2148], [#1960])
- **A credential the service cannot decrypt now says why, once, instead of `Key not valid for use in specified state` every 60 seconds forever** ([#2255], [#2252])
- **The incident readers take an alert's Dedup Key: `get_deadlocks` / `get_deadlock_detail` / `get_blocking` accept `dedup_key`** ([#2159], [#1140], [#2138])
- **An `initdb` loader failure now names WHICH binary could not load** ([#2185], [#2186])
- **A headless host can register a monitored server: `--add-server`** ([#2256], [#2252], [#2254], [#2158], [#2097])
- **Darling captures PostgreSQL blocking chains** ([#2213])
- **Every PostgreSQL migration rung is now pinned identical to the generated schema**
- **The viewer's store-schema probe is pinned to its reader's arity**
- **Darling monitors PostgreSQL and Amazon Aurora PostgreSQL** ([#2213])
- **The store can keep plan XML readable over plain SQL: `plan_xml_compression` ([#2171], asked for by @argpna)**
- **Alerts carry a monotonic occurrence total per incident, not just a rolling-window count** ([#2216])
- **A failing forced plan now alerts** ([#2157])
- **Every previously-hardcoded alert threshold is now a real setting** ([#2107])
- **Per-database collection timing now separates server think-time from row streaming** ([#2164])
- **The two collector memory bounds are now operator knobs** ([#2164], [#2170])
- **The Query Store backfill has an off switch** ([#2167])
- **The store measures its own background jobs** ([#2136])
- **The store alerts when its own background jobs outgrow their schedule** ([#2136])
- **The #2136 capacity model is now proven by a synthetic scale test, not asserted from one observation**

### Changed

- **The compose `statement_timeout` is a store setting instead of a hardcoded 15s** ([#2357])
- **MCP tool results serialize compact instead of pretty-printed** ([#2350])
- **The test suites run under Microsoft.Testing.Platform instead of VSTest** ([#2347])
- **The query_store per-database log split now names the plan-XML and text fetches** ([#2312])
- **Query Store statement text is now resolved from `collect.query_store_text`, and the separate fetch is ON** ([#2150], [#1565])
- **Lite's Query Store prune names the state keys it retired, like Darling's** ([#2205], [#2195])
- **Query Store plan XML is stored once per plan instead of re-shipped every pass, and moves into the shared plan dimension** ([#2164], [#2210])
- **Query Store collection stops re-shipping plan XML the store already holds** ([#2164])
- **A deliberately offline database alerts once instead of every cooldown** ([#2166], [#2203])
- **Force-plan findings carry a machine-first verdict for MCP consumers** ([#2138])
- **Plan-regression detection scores CPU as the primary signal and gates on absolute spend** ([#2138])
- **The force-plan recommendation now warns when the regressed query is parameter-sensitive** ([#2138])

### Fixed

- **Server Inventory's `Last Updated` was a config-snapshot time wearing a freshness label** ([#2359])
- **get_query_store_top reported a window it could not serve** ([#2364])
- **Server Inventory's Last Updated looked broken on decommissioned servers** ([#2359])
- **Extended-length paths no longer slip past the install-location guards** ([#2348])
- **get_query_trend silently truncated any window past 4 days** ([#2353])
- **The store's scale test no longer asserts that TimescaleDB compresses more rows in more time** ([#2266], [#2136])
- **The service now says WHY it cannot start when it is installed somewhere its own account cannot read** ([#2185])
- **The per-database watermark read was an unbounded MAX over every chunk in retention** ([#2344])
- **Aurora detection called a catalog lookup instead of the function, and silently disabled two collectors on real Aurora** ([#2340])
- **query_store's plan/text fetch is activity-driven: the store is the watermark** ([#2312])
- **Darling Viewer crash on Queries -> Query Store by Duration** ([#2181], [#2331])
- **The store self-metrics sweep's ~5-a-day "Exception while reading from stream" ERRORs were command timeouts in a network-fault costume** ([#2317])
- **Gapped charts no longer bury their neighbours under opaque black fill** ([#2324])
- **The plan fetch's adaptive candidate sizing is finally wired** ([#2312])
- **max_cpu_ms can no longer masquerade as a this-window number** ([#2235])
- **The query_store collector stops re-aggregating the open interval every cycle** ([#2312])
- **FinOps column filters no longer follow you to the next server** ([#2306])
- **A clean service stop no longer reads as seven faults** ([#2299], [#2294])
- **Index Analysis could show 2,000+ recommendations and an empty Recommendations grid** ([#2300])
- **The MCP host can read its server registry again - by not reading it** ([#2298], [#2293])
- **The FinOps provisioning verdict said UNDER_PROVISIONED for every server alive** ([#2246], [#2150])
- **Half of every delta collector's output was a zero that never happened** ([#2233], [#2234])
- **`tempdb_stats` failed forever on Azure SQL Database, and could never have succeeded** ([#2150])
- **The installer's mapped-drive refusal failed open when WMI was unavailable** ([#2201], [#2187])
- **The service now names the `darling.json` servers it is not monitoring** ([#2254], [#2252], [#2158], [#2256])
- **Microsoft Entra MFA can actually connect - Lite** ([#2184])
- **Dropped databases no longer leave Query Store collection state behind forever - both apps** ([#2188], [#2191])
- **A managed-Postgres bootstrap failure now says what went wrong instead of printing a Win32 number** ([#2186], [#1738])
- **A missing store credential no longer reads as a first run after a bootstrap has already failed** ([#2197], [#2186])
- **A database observed mid-restore no longer learns RESTORING as its expected state and then alerts forever for being healthy** ([#2189], [#2203])
- **Lite's database-state alert now goes quiet after announcing a chosen state, like Darling's** ([#2203], [#2166])
- **The doc-comment hygiene pin now catches a stacked summary whose first block is never closed** ([#2190])
- **The installer refuses an install directory the service could never read** ([#2187])
- **The store's compressed plan format is now documented** ([#2171])
- **tempdb no longer reports more than 100% used in FinOps Database Sizes** ([#2169])
- **Backing out of Custom Range no longer strands an open calendar - both apps** ([#2154])
- **One wedged background task can no longer stop collection - in either app** ([#2148])
- **The retention purge retries once on a deadlock instead of wasting the cycle** ([#2143])
- **Remote viewers can finally use the exact connection string `--print-viewer-connection` prints** ([#2117])
- **`--collapse-legacy-slices` no longer dies at TimescaleDB's decompression rail on compressed chunks** ([#2105])
- **A Query Store member whose catch-up window can't fit the command timeout now shrinks it until one does** ([#2111])
- **`--collapse-legacy-slices` no longer dies with "Exception while reading from stream" on a store fresh off a large catch-up** ([#2105])
- **Query Store catch-up can no longer spiral a big database into permanent timeout** ([#2102])
- **Multi-incident alerts render as labeled, self-contained units instead of one flat fact list** ([#2108])
- **Every database-scoped alert exposes a discrete Database fact** ([#2109])
- **Query Store backfill yields to the live path on contended replicas** ([#2111])
- **Version stamps are single-sourced from `<Version>`** ([#2113])
- **Lite no longer crashes with an uncatchable stack overflow on Queries > Query Store by Duration** ([#2114])
- **Upgrading a 3.3.0-era Darling store to 3.4.0 no longer fails the migration ladder** ([#2119])
- **The upgrade path itself is now release-gated** ([#2119])
- **Store Disk Pressure no longer re-notifies every cooldown while free space sits at an unchanged level** ([#2101])
- **Compression Job Stuck no longer false-alarms on a job it caught mid-run**
- **The Job History tab speaks display names** ([#2126])
- **The long-query completion XE session actually gets created now** ([#2129])
- **`--collapse-legacy-slices` narrows its slice instead of dying when a day does not fit the statement timeout** ([#2105])
- **Query Store collection no longer has a fixed cost that big catalogs cannot pay** ([#2133])
- **`--collapse-legacy-slices` runs ~40% fewer window scans and narrates its progress**

## [3.4.0] - 2026-08-06

Full entries: [docs/changelog/3.4.md](docs/changelog/3.4.md)

### Important

- **Darling keeps hourly-grain history 90 days instead of 21, and existing stores are moved to the new horizon automatically** ([#1937], [#1939])
- **Lite repairs its stored Query Store history once, on the first launch after upgrading - expect a few extra seconds and a slightly larger archive** ([#1912], [#1907])
- **OPERATOR ACTION, Darling only: run `--collapse-legacy-slices` PROMPTLY after upgrading, and the sooner the better** ([#1912], [#1759], [#1907])
- **Version Store (PVS) pressure alert in both apps** ([#1984], [#1951])
- **Lite's data now lives OUTSIDE the install directory - and on any version older than this one, upgrading with `Setup.exe` destroys it** ([#1832])

### Added

- **Stable releases now ship the Linux artifacts**
- **OPTIONAL, Darling only, for stores that collected before this release: `--recompress-plan-dim` converts the plan dimension's existing text rows to the gzip form new plans already use** ([#2076], [#2069])
- **The store measures itself: an hourly self-metrics sweep records every hypertable's size and compression, the payload dimension tables, and the whole store into `collect.store_metrics`, with a `get_store_metrics` MCP/REST read for capacity forecasting** ([#2068])
- **New execution plans are stored gzip-compressed, shrinking the store's single largest object by a projected further ~35% with no change to what is captured** ([#2069], [#2068], [#2071])
- **The web fleet gains tags: coloured pills, a group-by-tag tree, and tag-name search** ([#2020])
- **Darling runs on Linux: a compose file for the whole stack, a systemd walkthrough, and the store connection string as a secret reference** ([#1804])
- **Enabling a default-OFF collector fleet-wide actually takes effect now, and the schedule editor stops showing it as already-enabled** ([#2064], [#2061])
- **The Viewer's Settings dialog keeps Save and Close pinned below the scroll** ([#2063])
- **Findings now keep their evidence: the drill-down behind every persisted finding survives read-back, and `get_analysis_findings` can return it** ([#2060])
- **Darling backfills the Query Store history the live path never takes — newest-first, on its own tick, never past the raw tier's horizon** ([#2022], [#1960], [#1556], [#1937], [#2058])
- **The "server silenced" muted-bell reaches Lite's sidebar too** ([#2031])
- **The Darling service builds for Linux on every PR and ships a nightly container image** ([#1804])
- **Network exposure works in a container: the bind ladder's managed-mode gate extends to \`managed OR containerized\`** ([#1804])
- **Secrets without DPAPI: every plaintext secret slot also takes an \`env:NAME\` or \`file:/path\` reference** ([#1804])
- **A headless \`--test\` flag on the Darling Viewer: the #1954 connection self-test without the UI** ([#2005])
- **\`get_pvs_stats\`: the ADR persistent version store gets a browsable MCP reader on both hosts, plus the web mirror** ([#2029], [#2028], [#2018], [#1984])
- **Automatic plan correction is finally agent-readable: a \`get_plan_corrections\` tool on both MCP hosts, the web read mirror, and custom-view measures** ([#2028])
- **A silenced server now LOOKS silenced - a muted-bell indicator beside the status dot, and Silence/Unsilence are mutually exclusive** ([#2031], [#2020], [#2011])
- **The Query Store grid gains the inline plan button its sibling grids always had** ([#1980], [#1949])
- **A PVS trend chart on the FinOps Version Store tab, in both apps** ([#1984], [#1951])
- **The Procedure Stats comparison grid gains the text column its two siblings always had** ([#1981], [#1949], [#1568], [#1767])
- **Lite's sidebar groups the fleet into its tag tree, with inline assign and tag CRUD** ([#2020])
- **Server tags come to Lite: a Manage Tags editor, colour, coloured pills, and search-by-tag on the Overview** ([#2020])
- **Tags get colour, and a server's tags show as coloured pills on the Overview cards (Viewer)** ([#2008], [#2011])
- **Find servers fast: a live search box on the server list across all three surfaces** ([#2008])
- **The Viewer's connect-failure overlay gains "Run self-test" - a layered probe that names WHICH layer broke instead of one collapsed error** ([#1954], [#1966])
- **You can see what automatic plan correction is doing, including the queries the engine is working on right now** ([#1952])
- **Both apps now collect and show the ADR persistent version store, so a database that grows for no visible reason finally has an explanation** ([#1951])
- **The Darling Viewer says which darling.json it read, and what it parsed out of it** ([#1954])
- **Darling hands you a working remote Viewer setup instead of a documentation exercise: `--export-viewer-config`** ([#1953], [#1970])
- **`--print-viewer-connection` warns BEFORE it prints, not after** ([#1953])
- **The PagerDuty channel can route through a proxy** ([#1945])
- **PagerDuty is a first-class alert channel in Lite and Darling** ([#1943])
- **Collection Health now says whether a collector that has come back empty all week had anything to collect** ([#1852], [#1837], [#1863], [#1867])
- **A blocking alert that fires on total blocked WAIT TIME, not just how many sessions are blocked** ([#1839], [#1812], [#1830], [#1840])
- **A database-state alert that fires when a monitored database leaves its expected state** ([#1986])

### Changed

- **A bounded Query Store cycle now DEFERS its backlog instead of dropping it - collection ships oldest-first from the watermark and resumes exactly where it stopped** ([#1960], [#1556])
- **Analysis findings re-notify only when they WORSEN or RESOLVE-and-return — a standing story no longer re-fires on every cooldown expiry** ([#2054], [#1984])
- **Top-queries reads now say when a hash group merged different statements, instead of labeling a blend with one arbitrary text** ([#2012], [#1767])
- **Top-queries reads now split same-hash statements by their hosting procedure, so INSERT...EXEC callers stop blending outright** ([#2012], [#1568], [#1767])
- **The orphaned CPU/IO baseline aggregates are retired — every store drops them on the next service start** ([#2007], [#1995])
- **Both MCP hosts move to ModelContextProtocol SDK 2.0 (the MCP 2026-07-28 specification)** ([#1990], [#1822], [#1074])
- **`get_analysis_findings` now returns one entry per diagnostic chain instead of one per engine cycle - a 24h read shrinks ~28x** ([#2000])
- **Memory-pressure anomalies no longer fire on healthy servers with young baselines** ([#1996])
- **CPU and I/O latency reach robust-baseline parity in Darling** ([#1743])
- **The anomaly engine judges deviations against median/MAD instead of mean/stddev, and its confidence is honest** ([#1743], [#1606])
- **The MCP docs now lead with the boundary instead of the warning**
- **Query text and query plan columns sit at the FRONT of every query grid now, right of the time column, in both apps and the web UI** ([#1949])
- **Setting up a remote Darling Viewer is its own section in the docs now, and it leads with the verb that does it for you** ([#1955], [#1953], [#1970])
- **The SQL Server grant block is documented in ONE place, and it now includes `ALTER SETTINGS`** ([#1955])
- **Deadlocks collection defaults to the 5-minute tier instead of every minute, in both apps** ([#1963])
- **Alt mnemonics for the three checkboxes that live on tab content: `Auto-refresh` in both apps, `All Databases` in Lite's FinOps tab** ([#1878], [#1847], [#1860])
- **Azure SQL Managed Instance now collects Query Store plan type and replica role, instead of storing NULL placeholders on every instance** ([#1886], [#1848], [#1872], [#1934])
- **Bring-your-own PostgreSQL now documents how to size TimescaleDB's background workers, and that the pool is shared by the whole cluster** ([#1899])
- **The Settings windows' section-local buttons have Alt mnemonics, and the three identically-labelled webhook test buttons now say which channel they test** ([#1856], [#1847])
- **Alt mnemonics reach the context menus and the dialog checkboxes and radio buttons [#1847] scoped out** ([#1860], [#1857], [#1878])
- **Alert Detail has a Close button, and the plan and graph viewer windows close on Esc** ([#1858], [#1847])
- **Enter no longer submits three Darling Viewer dialogs that their Lite twins leave alone** ([#1859], [#1828])
- **Every dialog in Lite and the Darling Viewer now has Alt mnemonics on its action buttons, and Esc closes it** ([#1847], [#1828], [#1856], [#1857], [#1858], [#1859])
- **The Long Running Query filter checkboxes stopped eating an underscore out of the wait and procedure names they name** ([#1847])
- **Azure SQL Database now attributes its Query Store rows to a replica role instead of storing a NULL** ([#1872], [#1844], [#1836], [#1848], [#1871], [#1841])
- **CI: the version-bump check survives the deprecated/ layout transition** ([#1821])

### Fixed

- **MCP no longer loses the monitored-server registry over an SMTP password it never reads**
- **A baseline query that runs out of time now SAYS so instead of reading as a broken connection**
- **A recompiled plan's CPU is no longer discarded, so per-query attribution stops under-reporting a plan-churning instance** ([#2235], [#2234], [#2233], [#2012])
- **`--encrypt-password` and `--configure-network` explain themselves on a non-interactive console instead of silently doing nothing** ([#2097])
- **Linux: `add_servers` accepts `env:`/`file:` secret references, unblocking onboarding on compose deployments** ([#2087])
- **An already-hardened config backup no longer errors on every service start** ([#2093])
- **Self-alerts and PVS alerts now carry their real severity in Teams/Slack/PagerDuty/webhook payloads instead of rendering INFO-blue** ([#2090])
- **`--configure-network` no longer corrupts its edit (and then refuses to write) when a postgres/mcp/web section's last member is a string with no trailing comma** ([#2073])
- **Lite's two server-delete doors now run the same deep cleanup - Manage Servers' Delete no longer leaves a removed server's runtime state behind for a re-add to resurrect** ([#2033], [#2026], [#1696])
- **A transient config read failure at boot no longer silently kills the web dashboard and MCP server for the whole process lifetime** ([#2038])
- **Removed servers leave the web dashboard instead of lingering as ghosts** ([#2030])
- **Add Multiple Servers accepts SQL Server's own \`host,port\` syntax instead of eating the port as a display name** ([#2027])
- **The Viewer's tag editors now honor the read-only seat instead of failing silently** ([#2008])
- **The Darling Viewer looks for a relative `Root Certificate` beside `darling.json`, not beside wherever it happened to be launched from** ([#1970], [#1954])
- **A failed `--export-viewer-config` no longer leaves a half-finished handoff folder behind without saying so** ([#1973])
- **A Lite test suite that failed about one run in six, on a race between the tests rather than anything in the product** ([#1965], [#1957])
- **The Darling installer's SECURITY WARNING was right, and the files it named really were left world-readable** ([#1957], [#1818])
- **Darling's retention summary line described a posture the store does not have** ([#1958], [#1849], [#1942])
- **default_trace_events reads the CURRENT trace file instead of the whole rollover set** ([#1962])
- **query_stats and procedure_stats rank BEFORE they render** ([#1959])
- **--collapse-legacy-slices was unreachable from the command line** ([#1912])
- **Chart lines no longer connect across collection gaps** ([#1944])
- **The #1912 destroy-pin live test is self-sufficient under a SearchPath-pinned rig** ([#1940])
- **Every remaining 21-day mention in code and CLI output caught up with the 90-day horizon** ([#1937])
- **The Query Store slicer overlay is drawn at the hour the work RAN, so it lines up with the bars underneath it again** ([#1921], [#1841], [#1892], [#1923], [#1845], [#683])
- **Darling gains `--collapse-legacy-slices`, which repairs the Query Store rows stored before the split-slice fix and re-materializes the rollups they fed** ([#1912], [#1907], [#1849], [#1759], [#1793])
- **The retention chunk-geometry guard now reads the chunks on disk, not just the interval the catalog is configured with** ([#1925], [#1915])
- **Query Store execution counts stopped reporting a sliver of an interval instead of the whole thing** ([#1907], [#1841], [#1845], [#1853], [#1912])
- **A live-test rig without TimescaleDB preloaded now says so, instead of failing every test with "Connection is not open"** ([#1922], [#1794], [#1896])
- **The force-plan recommendation was telling operators to write the one call form that errors** ([#1914], [#1882])
- **The Dashboard's "Collection Stopped" alert history row threw away the figure the alert was raised to report** ([#1913], [#1846], [#1881])
- **The other half of the retention coverage invariant - chunk GEOMETRY - is now guarded against a real TimescaleDB** ([#1915], [#1905], [#1877], [#1925])
- **The retention ordering that keeps purges from stopping is now derived from the policy list, not asserted against whichever pairs happened to exist** ([#1905], [#1877], [#1757], [#1784], [#1915])
- **Alert history stored the PostgreSQL major version as an alert's "current value", and other numbers lifted out of prose** ([#1881], [#1846], [#1913])
- **A force-plan recommendation driven by a read-only secondary replica's regression no longer silently recommends forcing on the primary** ([#1882], [#1850], [#1914])
- **Query Store charts no longer draw a bar outside the range you asked for, or drop the newest one** ([#1892], [#1841])
- **The Add Server dialogs now size themselves to the monitor they are actually on** ([#1891], [#1828], [#1829])
- **Lite's MCP `get_alert_settings` now reports `cpu.mode`, in the same vocabulary Darling uses** ([#1911], [#1895])
- **A retention policy whose coverage list GROWS is now re-held, instead of purging past the new tier forever** ([#1877], [#1869], [#1784], [#1905], [#1906])
- **A cross-database lock now names the database the contended object actually lives in, from both blocking collectors** ([#1893], [#1876], [#1865], [#1898])
- **CI's gated-live PostgreSQL now runs the worker sizing the product ships, so TimescaleDB's background jobs can actually launch** ([#1888], [#1862], [#1874], [#1899], [#1705])
- **The same blocked object no longer raises two different alerts depending on which collector saw it** ([#1876], [#1865], [#1893])
- **A blocked-process collector that runs per database no longer discards its own diagnostics** ([#1875], [#1851], [#1535], [#1865], [#1556])
- **Alerts that have no number to report show a dash instead of "0.00"** ([#1846], [#1881])
- **Plan-regression analysis stopped silently discarding one replica's Query Store rows on an Availability Group** ([#1850], [#1841], [#1845], [#1882])
- **A database or server whose name contains an underscore no longer renders with the underscore missing and a stray Alt key attached to it** ([#1857], [#1847])
- **Lite's data migration no longer leaves your real store in the folder `Setup.exe` deletes when an empty one beat it to the new location** ([#1842], [#1832])
- **Lite's new "Total blocked wait" box now grays out, resets, and shows up in the alert preview like every other threshold** ([#1840], [#1839])
- **Lite and Darling's MCP `get_alert_settings` now use the same key names for the same fields** ([#1840])
- **A grouped number in an alert's display text is no longer truncated to its first digit group** ([#1834], [#1830])
- **Installing Darling over a service that was re-homed to a domain account no longer silently re-ACLs the config around the wrong principal** ([#1824])
- **The documentation stops recommending `SQLAgentReaderRole` in the four places [#1823] missed** ([#1826])
- **A blocked-process row that says `Unresolved` now says WHY it is unresolved** ([#1865], [#1851], [#1854], [#1875], [#1876])
- **A database whose file space `database_size_stats` could not read is now named on the collection-log row instead of vanishing** ([#1851], [#1837])
- **Collection Health's Note and Last Error now show the NEWEST message, not the alphabetically greatest one** ([#1855])
- **The Query Store hourly and daily rollups stopped baking in the pre-dedup inflation - a Custom Views panel reaching past the raw tier no longer steps by two orders of magnitude at the boundary** ([#1849], [#1841], [#1759], [#1793], [#1680], [#1581], [#1869])
- **The Query Store DAILY numbers stopped over-counting by roughly 2x - the tier that is kept indefinitely is now the accurate one** ([#1869], [#1879], [#1877], [#1849], [#1581])
- **Query Store charts now put an interval's work in the hour it RAN, and the duration trend stopped overstating** ([#1841], [#1849], [#1850])
- **Query Store totals no longer count the same interval once per collection - live stores were overstating hot queries by one to two orders of magnitude** ([#1841])
- **Alert History rows stored under the old "TempDB Space" metric name show their percentage again** ([#1830])
- **An enumerated collector that enumerates NOTHING now says so on its collection-log row** ([#1837], [#1833])
- **Collection Health now shows what a collector that collected NOTHING reported - and an enumeration can finally say why it found nothing** ([#1837], [#1855], [#1852], [#1836], [#1851], [#1823])
- **A Lite collector that fails before it runs no longer logs the previous collector's timings as its own**
- **Opening the Settings window after a data loss no longer deletes the webhook URLs that survived it** ([#1832])
- **A tray notification when an update is available, not just a title-bar suffix** ([#1832])
- **Query Store collects on Azure SQL Database** ([#1836], [#1833], [#1546], [#1558], [#1556])
- **Top Procedures collects on Azure SQL Database** ([#1833], [#1836], [#1837])
- **Chart x-axis timestamps now honor the Local/Server/UTC time toggle** ([#1831])
- **High CPU alerts no longer record 0 as their value in Alert History** ([#1830])
- **The Add Server dialog's buttons can no longer land off-screen when a taller auth mode is selected** ([#1828])
- **The CDC capture-job probe no longer fires an "Invalid object name 'msdb.dbo.cdc_jobs'" error event on servers that never configured CDC** ([#1096])
- **The documented least-privilege monitoring grants now actually run every collector, verified live** ([#1823])
- **The managed-store credential refusal now recognizes a re-homed service account and prints the ownership fix** ([#1823])
- **Darling installs re-homed to a domain account or gMSA survive upgrades and get correct remediation text** ([#1823], [#1802])
- **The gated-live Darling test suite no longer depends on which class the runner happens to schedule first** ([#1862])
- **The compression tests no longer race the background job that `add_compression_policy` starts on its own** ([#1889], [#1788], [#1760], [#1888])
- **A live test that fails to clean up after itself now fails the run instead of reporting success** ([#1873], [#1784], [#1862], [#1788], [#1794], [#1896])
- **A live test that fails mid-body now reports its OWN error, not whatever its teardown said afterwards** ([#1896], [#1794], [#1873], [#1902])
- **The live teardowns that could corrupt OTHER tests' results are converted first, and the backlog can now only shrink** ([#1902], [#1896])
- **The whole Viewer read-path test family now reports its own failures too, and the ratchet drops to 19** ([#1902])
- **A compression test that asserts a job has NEVER RUN stopped losing to the job actually running** ([#1888], [#1760], [#1788], [#1889])
- **No live test cleans up on the connection its own body used any more** ([#1902], [#1874], [#1776])
- **The live compression tests had two ~200-second windows around midnight where they failed for the clock rather than for the code** ([#1972])
- **The gated-live plan-capture E2E no longer lets the target's plan cache decide its verdict** ([#1988], [#1767])
- **Dependabot's PRs now target `dev`, where they can actually merge** ([#1822], [#1990])

## [3.3.0] - 2026-07-29

Full entries: [docs/changelog/3.3.md](docs/changelog/3.3.md)

### Important

- **Darling stores still on PostgreSQL 17 are upgraded IN PLACE to PostgreSQL 18.4 + TimescaleDB 2.28.1 on the first service start** ([#1718], [#1752])
- **On Darling stores with pre-existing history, the raw retention purges stay safely HELD until rollup coverage reaches the oldest raw row** ([#1788], [#1799])
- **The first start on this release also retunes every compression policy to an hourly tick and raises `maintenance_work_mem` to the measured compression floor** ([#1779], [#1780], [#1813])
- **The Full Dashboard and the CLI Installer are retired**

### Added

- **The gated live-Postgres leg now asserts the cluster it connected to serves the pinned runtime versions** ([#1806], [#1787])
- **The backfill's slice refreshes now pass `refresh_newest_first` explicitly, and a degrade is impossible to ignore** ([#1797])
- **Two guards that turn structural properties into checked ones** ([#1796], [#1789], [#1793])
- **The catalog retention sweep no longer deletes history the rollups never captured** ([#1793], [#1784])
- **The payload-dimension GC now stands down when the purge it depends on failed** ([#1789], [#1782], [#1784])
- **The query text and plan XML now live once each, not once per collected row** ([#1768], [#1767], [#1759])
- **Eight members had someone else's documentation, and seven had lost their own** ([#1751], [#1739], [#1744])
- **Darling store runtime upgrades: in-place 17 to 18, validated end to end** ([#1718], [#1705])
- **The AG replica chips now mark which node is the local one** ([#1735])
- **Darling: an AG failover is reported once, not once per monitored node, and a standing replica disconnect can re-alert** ([#1734], [#1674])
- **Availability Groups tab in Lite, and one shared AG projection behind every surface** ([#1731])
- **Lite gets the Availability Group alerts, off a shared policy** ([#1726], [#1688])
- **Availability Groups tab in the WPF viewer** ([#1722])
- **The bundled PostgreSQL runtime now proves its own version instead of being taken at its word** ([#1713])
- **AG fixture evidence: state the commit-time conclusion the guidance rests on** ([#1709], [#1708])
- **AG fixture evidence: the frozen suspended-row surface, and a lag correction** ([#1707], [#1702], [#1703], [#1704])
- **AG fixture evidence: what `secondary_lag_seconds` actually measures on a suspended row** ([#1704], [#1707])
- **Darling Web: an "AG Health" seed notebook** ([#1699], [#1688], [#1695])
- **AG latency: commit-time columns, drain-time estimates, and the primary-side perfmon counters** ([#1695], [#1688])
- **AG collection: document the second grant it needs, and make the lag trap filterable** ([#1691], [#1688])
- **Darling: Availability Group alerts - failover, replica disconnected, sync fell behind, database suspended** ([#1692], [#1688], [#1674])
- **Availability Group health collection in Lite + Darling** ([#1688], [#1692])
- **A Docker Availability Group fixture, and what it found about the AG DMVs** ([#1689])
- **Availability Group topology, in the browser and over MCP** ([#1690])
- **Darling Web: the reporting layer grows scatter, dual-axis, and brush-zoom** ([#1683])
- **CI: every `report.*` view now EXECUTES against seeded rows, not just exists** ([#1677])
- **Connection alerts for servers that are already down, and re-alerts during a standing outage** ([#1674])
- **Darling: `--configure-network` can now expose the WEB DASHBOARD** ([#1617])
- **Darling Web: adaptive `auto` time bucket for composed time-series panels** ([#1619])
- **Darling Web: custom time span on the view range picker** ([#1619])
- **Darling: hourly continuous aggregates for query_stats + procedure_stats (composer query acceleration)** ([#1621])
- **Darling: three-tier retention - downsample past the raw hot window, keep the rollups** ([#1623])
- **Darling Web: composed panels read the CAGG rollups for old windows instead of truncating at the raw horizon** ([#1625])
- **Darling Web: Query Store panels route past the 21-day hourly horizon (query_store_stats_daily)** ([#1626], [#1627])
- **Darling Web: composed `object_name` panels on query_stats route to the CAGG rollups too — the last read-routing exception, closed** ([#1627])
- **Darling viewer: organize the fleet into nested tags** ([#1640])

### Changed

- **Darling store: `maintenance_work_mem` raised to the measured compression floor, and existing stores actually get it** ([#1780], [#1777])
- **CI: a dev/main push now cancels that branch's superseded in-flight build — newest SHA wins** ([#1729], [#1715], [#1716])
- **CI: build.yml handles the merge_group event** ([#1716], [#1715])
- **CI: the change classification the v4 pin bump silently broke is restored - and this time it is probe-validated** ([#1715], [#1712], [#1701])
- **CI: a documentation change stops paying for a .NET restore** ([#1712])
- **CI: the Lite fast / analysis-heavy test split collapses back into one step** ([#1701], [#1693], [#1694], [#1698])
- **Lite.Tests: the shared DuckDB fixture extends to 20 fast-bucket classes** ([#1698], [#1693], [#1694])
- **Lite.Tests: seeding is batched - one connection and one transaction per seed helper instead of a connection and an auto-commit per ROW** ([#1694], [#1693])
- **Lite.Tests: the analysis-heavy classes share one DuckDB per test class instead of rebuilding the schema per test** ([#1693])
- **Quarterly maintenance pass: dependency bumps, a zero-warning build restored, repo hygiene** ([#1643])
- **CI actually runs the deprecated Dashboard's build and tests again** ([#1643])
- **Full Dashboard and the CLI Installer are being retired.** ([#1612])
- **Darling Web: the custom-view panel grid is capped at two columns on wide screens** ([#1619])
- **Darling: the query_store_stats + procedure_stats continuous aggregates are reshaped to the composer's dimensions** ([#1624])

### Fixed

- **The service now hardens EXISTING config backups, not just the live config** ([#1818], [#1816])
- **The dimension GC's floor measurement now survives compressed history** ([#1817], [#1815])
- **A stale running_jobs snapshot no longer re-fires the same Long-Running Job alert forever** ([#1814], [#1812])
- **The dimension GC prunes against the oldest SURVIVING digest-carrying fact row, not an assumed horizon** ([#1813], [#1795])
- **Recommendations warm-up now survives the 512 MB archive/reset, and analysis reads the archive tier everywhere** ([#1811], [#1809])
- **Live-Postgres test cleanup no longer depends on the connection the failure just destroyed** ([#1810], [#1794])
- **A benign 1-second lock-timeout yield no longer paints a Critical health day** ([#1808], [#1805])
- **The .NET SDK band is now pinned three ways instead of agreeing by float** ([#1807], [#1758])
- **The payload-dimension batch upsert deadlocked against itself across concurrent collection cycles** ([#1803], [#1801])
- **`--backfill-rollups` converged every rollup to raw's oldest row, but the arming gates for the hourly tier measure the SOURCE - so a fully-DONE run could leave retention policies held** ([#1799], [#1798], [#1680])
- **The retention rollups only ever served what they had materialized, so old windows read empty while raw still held every row** ([#1788], [#1759], [#1680], [#1665])
- **The directories cluttering a field instance's install folder were invisible to the report built to find them** ([#1785], [#1770], [#1775])
- **Four test classes shared the live Postgres store without serializing against it, and three more were blamed for it by a substring match** ([#1783], [#1776])
- **The config backups handed a second readable copy of every secret to any interactively-logged-on user** ([#1786], [#1769], [#1792])
- **The compression self-heal read TimescaleDB's never-ran sentinel as a run that started in year 1** ([#1781], [#1760])
- **A chunk that was already eligible to compress could still sit uncompressed for most of a day** ([#1779], [#1778], [#1768], [#1680], [#1585])
- **One stuck rollback copy froze the ageing-out of every other one** ([#1775], [#1770], [#1718])
- **The restart delta seed read the whole store to find rows it was about to throw away** ([#1774], [#1772])
- **The service tried to open its own firewall on every start, could never succeed, and a fresh networked install got no rule at all** ([#1773], [#1771])
- **Anomaly baselines were being computed from a fraction of their intended history, silently** ([#1762], [#1757])
- **The Lite AG alert guard now captures the data service before checking it** ([#1756], [#1754], [#1755])
- **A package with an OLDER PostgreSQL major no longer replaces a working newer runtime and takes the store down** ([#1752], [#1738], [#1737])
- **The Lite AG alert sweep could throw instead of quietly doing nothing before the store opened** ([#1753], [#1726], [#1750])
- **Alert FIRINGS are logged now, in the shared engine and in Lite - not just resolutions** ([#1750], [#1685])
- **The config backups were world-readable copies of the credentials they back up** ([#1749], [#1721])
- **The daily-summary MCP live test no longer fails in the five minutes after UTC midnight** ([#1748])
- **Darling: the TimescaleDB ensure-summary log stopped lying about which continuous aggregates exist** ([#1747])
- **The in-place store upgrade could not authenticate anywhere but a developer's box, and one of its new guard tests could pass with the bug present** ([#1739])
- **The store-upgrade commit point is now pinned, and the degraded-upgrade alert stops asking for cleanup the product does itself** ([#1739], [#1718])
- **Azure SQL Database: `database_size_stats` stopped touching `master`, and error 300 now explains itself** ([#1732], [#1634], [#1642])
- **Lite: prove the Agent-status SQL, not just the decision it feeds** ([#1730], [#1725])
- **Credential-file ACL failures now name the owner, and the DPAPI credential files verify the result instead of assuming it** ([#1727])
- **Lite: the Job History Agent indicator stops calling absence of signal a problem** ([#1725])
- **Darling Web: a capped time-series panel kept the OLDEST slice of the window and said nothing** ([#1724])
- **Darling: "Agent Not Running" no longer nags servers that never ran SQL Agent** ([#1719])
- **Darling: retention policies were silently not being created AT ALL, on every store** ([#1711])
- **Darling: state the rule that decides how a new AG measure gets handled** ([#1710])
- **Darling: corrected the AG doc guidance on commit-time deltas** ([#1708], [#1703])
- **Darling: the AG lag alert now says what its number actually measures** ([#1703], [#1700])
- **AG suspension semantics: one rule that holds whether or not the vendor docs are right** ([#1702])
- **Darling: a suspended secondary that is falling behind now raises the sync alert** ([#1700], [#1692])
- **Darling: the Availability Group store reads are now executed against a real Postgres in CI** ([#1697], [#1692])
- **Lite + Darling: recovery notices were being styled and counted as live alerts in Alert History and the Daily Summary** ([#1692])
- **Darling: the MCP `get_alert_settings` tool under-reported the store by five columns** ([#1692], [#1674])
- **Darling test suite: a genuinely intermittent failure, roughly one full-suite run in six** ([#1692])
- **Lite + Darling: the query tools no longer present `max_dop` as current parallelism** ([#1682])
- **Darling: self-alert FIRINGS are logged, not just their recoveries** ([#1681])
- **Darling: the managed PostgreSQL now records a per-run audit trail for TimescaleDB background jobs** ([#1681])
- **Darling: retention policies no longer destroy history on the deploy that creates them** ([#1680])
- **Lite + Darling: a permission-denied collector is now visible without opening the Collection Health tab** ([#1591])
- **Docs: which collectors run on which platform** ([#1591])
- **Darling viewer: Server Inventory now explains missing hardware instead of showing zeros** ([#1591], [#1535], [#1663])
- **Upgrade scripts (deprecated path): the 2.1.0-to-2.2.0 compression migrations are now genuinely idempotent** ([#1676], [#1673])
- **Full Dashboard (deprecated): `report.daily_summary_v2` no longer garbles `worst_query_hash`** ([#1667], [#1666])
- **Darling Web: composed custom-view panels no longer route onto rollups the store doesn't have** ([#1675])
- **Full Dashboard (deprecated): the Trace Flag Changes grid and the `get_trace_flag_changes` MCP tool throw `InvalidCastException` now that the view returns rows** ([#1668])
- **Darling: managed PostgreSQL's `pg.log` finally rotates** ([#1670])
- **Darling viewer: built-in tabs no longer silently lose history past the raw retention horizon** ([#1661], [#1623], [#1625], [#1626], [#1627])
- **Lite + Darling: a monitoring login without `VIEW SERVER STATE` no longer loses its entire `server_properties` row** ([#1591])
- **Lite + Darling: the `get_active_queries` MCP tool no longer tells the model its data comes from sp_WhoIsActive** ([#1650])
- **Darling Web: a LAN-exposed dashboard no longer lets any local process in without a credential** ([#1649])
- **Darling + Lite: security hardening - firewall command injection via `allowFrom`, an unprotected `darling.json`, and MCP hosts with no DNS-rebinding guard** ([#1646], [#1647], [#1648], [#1576])
- **Lite + Darling: four things that grew forever - two tables and two log directories** ([#1651], [#1652])
- **Lite: the Excluded Databases picker throws a raw firewall error on an Azure database-level-firewall server** ([#1642])
- **Darling: a bring-your-own-Postgres `viewer` seat gets `permission denied` on the Manage Servers list and the Settings notification prefill** ([#1639], [#1506], [#1551])
- **Lite + Darling: Linux hosts on SQL Server 2025 CU1+ get real other-process CPU again** ([#1630], [#1048], [#1629])
- **Lite + Darling: default trace collection works on SQL Server on Linux** ([#1633], [#1632])
- **Full Dashboard (deprecated): the `report.trace_flag_changes` view no longer fails with Msg 245 on a trace-flag toggle** ([#1637], [#1635])
- **Default trace: the rollover strip no longer mangles paths whose DIRECTORY contains an underscore** ([#1636], [#1633])
- **Lite + Darling: Azure SQL DB collection works again when the client is firewalled out at the logical server but allowed at the DATABASE level** ([#1634], [#1631], [#857], [#1506])
- **Darling: collectors no longer fail with `22021: invalid byte sequence for encoding "UTF8": 0x00`** ([#1614])
- **Darling Web: a custom view no longer resets the picked time range / server / filters every 60 seconds** ([#1619])
- **Darling: the Custom Views composer and analyze_*_plan no longer time out on large stores** ([#1620])
- **Darling + Lite: the Long-Running Query alert's `sp_server_diagnostics` exclusion no longer fires a false alert when the health-check session is in a non-diagnostics wait** ([#1622])
- `install-darling.ps1`: the viewer-shortcut step no longer throws when run with no loaded user profile (a Windows service, scheduled task, CI runner, or remote session) where `GetFolderPath('Desktop'/'StartMenu')` returns empty. It creates only the shortcuts whose folder resolves and skips cleanly otherwise. ([#1613])

### Documentation

- **A second stale doc comment describing behaviour that no longer exists** ([#1744], [#1739])
- Darling README: added a "verify it's actually reachable" recipe for the opt-in LAN MCP/web endpoints — confirm the listener is on the LAN address (not loopback), the scoped firewall rule covers the client, and the client connects to the box IP rather than `localhost` — plus what to re-run after a reinstall. Enabling an endpoint is not the same as reaching it. ([#1616])

## [3.2.0] - 2026-07-21

Full entries: [docs/changelog/3.2.md](docs/changelog/3.2.md)

### Added

- **Darling MCP: bulk add/remove monitored servers — an MCP client can now stand up FLEET monitoring conversationally** ([#1609], [#1600], [#1608], [#1549])
- **Darling MCP: alert-tuning write tools — an MCP client can now tune thresholds and manage mute rules conversationally** ([#1608], [#1600])
- **Darling: headless `--enable-mcp` / `--disable-mcp` / `--enable-web` / `--disable-web` CLI verbs — bring an endpoint up (store + firewall) without the Viewer** ([#1601])
- **Darling MCP: `describe_custom_view_catalog` — the compose vocabulary an LLM needs to author a view without guessing** ([#1602], [#1600])
- **Darling MCP: create and manage Custom Views (CV2) over MCP** ([#1600])
- **Darling Web: Custom Views v2 - QS execution-weighted average + per-server measure availability greying** ([#1584])
- **Darling Web: Custom Views v2 - notebook mode (investigation documents)** ([#1583])
- **Darling Web: Custom Views v2 follow-ups (wave 2) - event annotations + per-panel drill-down** ([#1582])
- **Darling Web: Custom Views v2 follow-ups (wave 1) - gauge ratios, stacked-bar, threshold lines, measure availability** ([#1580])
- **Darling Web: Custom Views v2 - compose your own metrics, filters, grouping, units, and chart types** ([#1579])
- **Darling Web: custom views / notebooks — compose your own dashboards from the read catalog** ([#1576])
- **Darling Web: a read-only browser dashboard served by the service** ([#1574])
- **Long-running query completion trace (opt-in, default OFF) in Lite and the Darling viewer** ([#1572])
- **Darling viewer: ships as a Velopack `Setup.exe` for remote seats, like Lite and Dashboard** ([#1571])
- **Queries: Top Queries by Duration now attributes each statement to its named module â€” or "ad hoc" â€” in Lite and the Darling viewer** ([#1569])
- **Darling: `--configure-network` â€” a guided, fail-closed wizard for the opt-in LAN endpoints** ([#1570])
- **Darling: scripted install/uninstall ships in the zip â€” one elevated command instead of the manual service recipe** ([#1554])
- **Darling: a real diagnostic surface, store write-throughput headroom, and honest bootstrap status** ([#1552])
- **Nightly builds now include Darling** ([#1550])
- **Add Multiple Servers: onboard a whole fleet from one pasted list, in both apps** ([#1549])
- **NOC Overview: sort the server tiles by CPU% descending, with a persisted CPU%/Name selector (both apps)** ([#1547])
- **Front-end shell collapse: the standalone Plan Viewer's tab management hoisted to `Ui.StandalonePlanViewerController`** ([#1536])
- **Front-end shell collapse: the Latches & Spinlocks grouped trend-chart render body hoisted to `Ui.GroupedTrendChartRenderer`** ([#1534], [#1532])
- **Front-end shell collapse: the Session Stats trend chart + summary projection hoisted to `Ui.SessionStatsChartRenderer` / `Common.SessionStatsSummary`** ([#1533], [#1531], [#1527], [#1532])
- **Front-end shell collapse: the CPU-scheduler trend-chart render body hoisted to `Ui.CpuSchedulerChartRenderer`** ([#1532], [#1531], [#1527])
- **Front-end shell collapse: the CPU-scheduler metric projection + pressure classification hoisted to `Common.CpuSchedulerMetrics`** ([#1531])
- **Front-end shell collapse: the column-filter popup host/apply plumbing hoisted to `Ui.ColumnFilterPopupController`** ([#1530], [#1529])
- **Front-end shell collapse: the column-filter popup promoted to the shared `PerformanceMonitor.Ui.ColumnFilterPopup`** ([#1529], [#1523])
- **Front-end shell collapse: the deadlock-graph per-process walk hoisted to `Common.DeadlockGraphProcessParser`** ([#1528])
- **Front-end shell collapse: System Events counter-chart bodies hoisted to `Ui.SystemHealthChartRenderer`** ([#1527])
- **Front-end shell collapse: stateful chart-render helpers hoisted to `Ui.ChartRenderHelper`** ([#1526])
- **Front-end shell collapse: file-save dialogs hoisted to `Ui.FileSaveHelper`** ([#1525])
- **Front-end shell collapse â€” Batch B: `FindParentDataGrid` visual-tree helpers hoisted to `PerformanceMonitor.Ui`** ([#1524], [#1522])
- **Front-end shell collapse â€” Batch A: `SelectableItem` hoisted to `PerformanceMonitor.Ui`** ([#1523], [#1522])
- **Front-end shell collapse â€” PR0: the capability-inventory pin (safety net before any hoist)** ([#1522])
- **Analysis engine: plan-cache single-use bloat + ring-buffer memory-pressure issue-classification arms in the shared FactScorer (Tier-2 parity, batch-2b remainder)** ([#1521])
- **Analysis engine: two server-config issue-classification arms (priority boost + lightweight pooling) in the shared FactScorer (Tier-2 parity, batch-2b)** ([#1520])
- **Analysis engine: two tempdb-deep issue-classification arms in the shared FactScorer (Tier-2 parity)** ([#1519])
- **Analysis engine: three more issue-classification arms in the shared FactScorer (Tier-2 parity)** ([#1517])
- **Lite MCP: the 18 Darling-only tools ported (drains the MCP inventory ratchet to ZERO)** ([#1518])
- **Darling viewer: Query Store Regressions sub-tab (Dashboard parity BUILD)** ([#1516], [#1515])
- **Darling viewer: live Current Active Queries sub-tab (service-mediated)** ([#1515])
- **Lite viewer: Blocking Stats sub-tab (Tier-1 parity BUILD)** ([#1514])
- **Lite viewer: Expensive Queries sub-tab (Tier-1 parity BUILD)** ([#1513])
- **Config-diff collapse to shared Common + Lite Configuration Changes tab (Tier-1 parity)** ([#1512], [#1507])
- **Lite: connection-lost / connection-restored alerts, and a generic webhook channel for both apps** ([#1506])
- Alongside it, a third webhook channel in the shared `WebhookAlertService` â€” **generic HTTP POST** â€” beside Teams and Slack, for **Lite and Darling**. It POSTs an operator-authored JSON body with `{{metric}}`, `{{server}}`, `{{value}}`, `{{threshold}}`, `{{severity}}`, `{{context}}` and `{{timestamp}}` placeholders and an arbitrary JSON headers object, which covers PagerDuty / Opsgenie / n8n and â€” the motivating use case â€” a GitHub `repository_dispatch` that re-runs a workflow when an alert fires. **This is deliberately not a script/exe runner:** arbitrary command execution in a signed binary is an EDR-flagged attack path, and a webhook reaches the same automation with no process-execution surface. Every substituted placeholder is **JSON-escaped** (via `JsonSerializer`, which â€” unlike `JsonEncodedText.Encode` â€” tolerates the lone surrogates SQL Server names can carry instead of throwing), so a server name carrying a quote or backslash â€” `HOST\INSTANCE`, or something hostile â€” cannot break out of the template and corrupt or inject into the posted JSON; the substituted body is validated as JSON before it goes on the wire, malformed headers JSON degrades to a clear logged error instead of throwing into the alert loop, and a `User-Agent` is defaulted (GitHub's API 403s requests without one). Settings live beside Teams/Slack in both apps' Settings windows with a "Send Test" button and a `repository_dispatch` example; the URL and the headers JSON are treated as **secrets** â€” Windows Credential Manager in Lite, and in Darling's Postgres control plane (schema **V26**) `generic_url` + `generic_headers` are column-`REVOKE`d from the read-only viewer role like the existing webhook URLs, since the headers carry the `Authorization` bearer token. The deprecated Dashboard satisfies the shared settings interface with the channel off and gains no new UI.
- **Lite viewer: System Events tab (Tier-1 parity BUILD â€” drains the coverage pin to ZERO)** ([#1509])
- **Lite viewer: CPU Scheduler, Plan Cache & Session Stats tabs (Tier-1 parity BUILD, batch of 3)** ([#1505])
- **Lite viewer: Latches & Spinlocks tab (Tier-1 parity BUILD)** ([#1504])
- **Parity "net" test guards, round 2 (Tier 0): three cross-app PIN tests** ([#1501])
- **Parity "net" test guards (Tier 0): collector-coverage pin + Lite<->Darling theme pin** ([#1499])
- **Retained fleet-wide "Job History" tab (Lite + Darling)** ([#1489])
- **Global per-server database filter** ([#1490])
- **Darling: "Get Actual Plan" (service-mediated), matching the Dashboard's surfaces**
- **Shared `ActualPlanExecutor` and `QueryModificationDetector`**
- **Darling: managed Postgres derives a memory-tuning `postgresql.conf` block from host RAM â€” scale-readiness for the up-to-500-servers store** ([#1478], [#1463])
- **MCP: `get_analysis_findings` and `analyze_server` now return the full copy-paste remediation command (Lite + Darling)** ([#1475], [#1473], [#1474])
- **Lite: recommendation cards now produce the SAME runnable copy-paste T-SQL command the Darling viewer does â€” killing an advise-only-vs-command drift** ([#1474], [#1473])
- **Darling Viewer: a runnable copy-paste T-SQL command for EVERY remediable recommendation, since the headless viewer has no in-app "Apply Fix"** ([#1473])
- **Darling Viewer: per-server alert badge with Acknowledge (ack-until-worse), so you can see at a glance which servers need attention** ([#1470])
- **Shared `system_health_events` collector (Stage 1: raw `system_health` Extended Events capture), so both Lite and Darling collect them** ([#1262])
- **Darling Viewer: the System Events tab gains the CPU Tasks and I/O Issues sub-tabs (`system_health` parity, Stage 2b)** ([#1262])
- **Shared `cpu_scheduler_stats` + `plan_cache_stats` collectors, so both Lite and Darling collect them** ([#1262])
- **Shared `latch_stats` + `spinlock_stats` collectors, so both Lite and Darling collect them** ([#1262])
- **Darling security hardening Phase 1: least-privilege schema split + roles, so the Viewer stops connecting as the superuser** ([#1262])
- **Execution-plan capture extended to procedure_stats, blocked_process_report, and deadlocks** ([#1262])
- **Darling Viewer W1j: the Memory inner tab â€” Lite's four Memory sub-tabs, copied** ([#1262])
- **PerformanceMonitor.Collectors: the shared collector-definition library â€” ALL 26 of Lite's collectors extracted** ([#1262], [#1180], [#1240], [#1048], [#989], [#857], [#1135], [#1286], [#1140])
- **Darling: the Postgres schema is now generated from the shared collector definitions** ([#1262], [#857])
- **Darling captures execution plans; Lite still does not** ([#1262])
- **Darling Viewer W2a: the top-level aggregate surfaces â€” the Overview server-cards grid and full Alert History parity, copied from Lite** ([#1262])
- **Darling Viewer: the Plan Viewer inner tab lights up View Plan against the stored plans â€” copied from Lite** ([#1262])
- **Darling M3: the first Viewer shell â€” servers, collection health, wait trend** ([#1262], [#1134], [#1140])
- **Darling Viewer W1a: the CPU and tempdb inner tabs, copied from Lite** ([#1262])
- **Darling Viewer W1c: the File I/O tab and the Blocking tab's Trends + Current Waits sub-tabs, copied from Lite** ([#1262])
- **Darling Viewer W1e: the Blocking tab reaches full Lite depth â€” widened Blocked Process Reports, the Deadlocks sub-tab, and the block-chain + deadlock-graph viewers, copied from Lite** ([#1262])
- **Darling Viewer W1f-1: the Queries tab becomes Lite's nested sub-tab group â€” the three big grids (Top Queries / Top Procedures / Query Store), copied from Lite** ([#1262])
- **Darling Viewer W1f-2: the Queries tab is completed to Lite's full six sub-tabs â€” Performance Trends, Active Queries, Query Heatmap, copied from Lite** ([#1262])
- **Darling Viewer W1d: the Overview inner tab becomes Lite's correlated timeline lanes, copied** ([#1262])
- **Darling Viewer W1i: the Daily Summary inner tab + Collection Health upgraded to Lite's 3-sub-tab shape â€” copied from Lite** ([#1262], [#1248])
- **Darling Viewer W1h: the Perfmon and Running Jobs inner tabs â€” copied from Lite** ([#1262])
- **Darling Viewer W1g: the Configuration inner tab â€” Lite's four read-only snapshot grids, copied** ([#1262])
- **Darling Viewer W0: copy-parity foundations + navigation inverted to Lite's shape** ([#1262])
- **Darling Viewer W1b: the Wait Stats inner tab â€” Lite's wait-type picker + trend, copied** ([#1262])
- **Lite and Dashboard: the six alert-context builders move into the new shared `PerformanceMonitor.Alerting` library** ([#1262], [#1140], [#1091], [#1145], [#754], [#1136], [#1141], [#1154], [#1202], [#1236], [#749], [#1128])
- **Darling: optional TimescaleDB adoption â€” hypertables, chunk-based retention, and compression as the archival tier** ([#1262])
- **Darling: managed bundled Postgres â€” the zero-admin first-run bootstrap** ([#1262])
- **Darling: scaffold for the net-new headless/centralized edition** ([#1262])
- **Darling: operator documentation for the headless edition** ([#1262])
- **Darling: wired into the release pipeline** ([#1262])
- **Darling: nightly CI now covers the gated live-PostgreSQL tests** ([#1262])
- **README: Managed Identity and Service Principal authentication documented** ([#1321], [#1042], [#1268])
- **Darling: Index Analysis rollup gains the workload-impact columns** ([#1262])

### Changed

- **CI: PR build/test/publish is now gated per product, so a change to one edition doesn't rebuild the others** ([#1599], [#1598])
- **CI: the gated-live Darling PostgreSQL test suite now runs on pull requests, not only in the nightly** ([#1598], [#1586])
- **Viewer + Lite: plan-action menu verbs renamed to end the "Actual" overload** ([#1542])
- **Dashboard: the add/edit-server state machine and its predicates move to `Installer.Core`, where CI can test them** ([#1498])
- **Darling + Lite: the `system_health` significance predicates are collapsed into one shared copy in `PerformanceMonitor.Common` (Tier-1 parity groundwork)** ([#1507])
- **Lite and Darling: the triplicated baseline analysis MODEL is collapsed into one shared copy in `PerformanceMonitor.Analysis` (Tier 0 parity collapse)** ([#1503])
- **Lite: the 35 collector-table DuckDB schemas are now GENERATED from the shared `CollectorCatalog` (Tier 0 parity collapse) instead of hand-written** ([#1502])
- **Analysis findings: parallelism (CXPACKET) can no longer reach CRITICAL on amplifier stacking alone; a real thread-exhaustion meltdown escalates through THREADPOOL instead (Stage B of the recs-engine remainder â€” the severity half)** ([#1494])
- **Analysis findings: anomaly detections fold into the regular incident, and alerts dedup per incident (Stage B of the recs-engine remainder)** ([#1493])
- **Darling: `collect.collection_log` becomes a TimescaleDB hypertable â€” O(1) `drop_chunks` retention + compression, scale-readiness for the up-to-500-servers store** ([#1479], [#1478])
- **Darling viewer W2b: the Recommendations aggregate tab re-skinned to Lite's advise-only card design** ([#1262])
- **Lite: `date_diff` replaced with the PostgreSQL-shared spelling, bit-exactly (headless groundwork, phase 1a)** ([#1262])
- **Lite: DuckDB SQL normalized to the PostgreSQL-shared dialect (headless groundwork, phase 1a)** ([#1262])
- **Darling viewer: the Latches & Spinlocks tab is consolidated into one view** ([#1488])

### Removed

- **Plan Cache tab: dropped the momentary Cache Object Type breakdown grid (both apps)** ([#1539])
- **Queries > Expensive Queries sub-tab dropped from both apps; the Performance Calendar day-drill repoints to Top Queries** ([#1544])

### Fixed

- **Darling.Tests: the compression self-heal gated-live test no longer races the TimescaleDB scheduler** ([#1604], [#1585], [#1586], [#1588])
- **Darling service: the sweep hang-watchdog no longer cries wolf when a server is merely waiting its turn** ([#1597], [#1590], [#1593], [#1553], [#1557], [#1581])
- **Darling service: settled the #1581 "narrow the credential ACL scope" question — the lockdown does NOT silence file logging, and the scope stays** ([#1596])
- **Darling service: removing an already-absent firewall rule no longer logs a bogus warning on every shutdown** ([#1594])
- **FinOps Server Inventory: an Azure SQL DB monitoring login without VIEW DATABASE STATE no longer loses the entire server row** ([#1592], [#1589])
- **Darling analysis: the RegressedQueries plan-regression drill-down no longer scans all Query Store history on large stores** ([#1593])
- **Connectivity probe: Azure SQL DB is no longer silently mis-detected as on-prem when the monitoring login lacks VIEW DATABASE STATE** ([#1589])
- **Darling service: file logging now fails LOUDLY instead of going silent** ([#1590])
- **Darling scheduler: a service restart no longer herds all servers' first catch-up sweep into one tick** ([#1590], [#1577], [#1553])
- **Darling service: the compression-job self-heal can now actually re-arm a stuck job (`alter_job` job_id type fix)** ([#1586], [#1585], [#1588])
- **Darling service: TimescaleDB compression-job self-heal + single-instance / argument-handling hardening** ([#1585], [#1586])
- **Darling scheduler: a service restart no longer starves long-frequency collectors - NextDue is seeded from the persisted collection watermark, not a full-interval offset** ([#1577], [#1553])
- **Collector health: a healthy DAILY collector no longer bands STALE/FAILING, and no longer inflates the Overview "collectors failing" count** ([#1578])
- **Darling: the retention purge's batched DELETE no longer breaks on compressed chunks** ([#1567])
- **Darling: the viewer's store pool is now capped too, and the postgres.exe process count is documented for what it actually is** ([#1566], [#1559])
- **query_store collector: the self-exclusion filter was 75% of the read's cost â€” bounded to a 100-char scan; per-database log lines; rdsadmin default-excluded** ([#1565], [#1557])
- **Darling: the viewer's MCP toggle now actually starts and stops the MCP server â€” live, no service restart** ([#1560])
- **Darling managed store: size shared_buffers for the co-located reality, healing the Windows backend-spawn failures** ([#1559])
- **Query Store: stop collecting from readable-secondary replicas â€” their Query Store is the primary's replicated content, not local activity** ([#1558], [#1557], [#1546])
- **Collectors: a stalled query_store cycle can no longer run the collector out of memory and take the box down â€” reads are byte-bounded, writes flush per database, and an over-memory service stops launching new work** ([#1557], [#1553])
- **Darling: one slow server can no longer strand the rest of the fleet â€” the collection sweep goes bounded-parallel with cadence jitter and per-server exception containment** ([#1553], [#1552])
- **Index Analysis: the monitor-side dedupe engine absorbs the sp_IndexCleanup review fixes it predated** ([#1548])
- **Query Store: attribute runtime stats to their replica role, so an AG's primary stops silently reporting secondary workload as its own** ([#1546], [#1535])
- **Lite: the plan verbs no longer sit on every grid â€” only on the query grids that can actually use them** ([#1545], [#1543])
- **Darling viewer: stop the hard crash when interacting with the system-tray icon (Hardcodet TrayToolTip issue #422)** ([#1538])
- **Charts: increase the shared top headroom from 5% to 15% so a series that plateaus at a hard ceiling no longer reads as clipped at the top** ([#1538])
- **The gated live-PostgreSQL test suite runs green for the first time â€” one real grant gap and five latent test defects** ([#1551])
- **Darling viewer: query-tab plan menus fixed â€” no greyed-out verbs, right-click acts on the clicked row, and the Regressions / live Active Queries grids get plan access** ([#1543])
- **Darling viewer: the first click on an unsorted grid column now sorts descending, and header-copy plus middle-click panning work on every grid** ([#1541])
- **Charts: finish the window-pin campaign so trend charts stay pinned to the selected window** ([#1540])
- **Lite and Darling: Azure SQL DB deadlock and blocked-process capture now covers EVERY monitored database, not just the connection's own** ([#1346], [#1535], [#1251], [#1086])
- **Lite and Darling: an Azure SQL DB outage no longer permanently wedges database-scoped collection until the app is restarted** ([#1506], [#857])
- **Dashboard and Lite: the server-list version display no longer risks a crash on a two-part version string** ([#1510], [#1498])
- **Clean install is Azure SQL Managed Instance-safe, and a failed drop no longer leaves the database bricked** ([#1498])
- **CLI installer: a password beginning with `--` gets a clear message instead of a misleading "password required"** ([#1498])
- **Dashboard: a failed upgrade migration no longer stamps the database as successfully upgraded** ([#1498], [#963])
- **Dashboard: a failed version check no longer reinstalls over an existing database and reports it as up to date** ([#1498])
- **A non-destructive repair path, so aborting on a failed migration doesn't leave you stuck** ([#1498])
- **`--repair` exit-code contract** ([#1498])
- **Dashboard: retyping the server name no longer leaves the previous server's answers on screen** ([#1498])
- **Cancelling an install is reported as a cancellation, not a failure â€” at every stage** ([#1498])
- **Cancelling a clean install no longer reports success over a database it just dropped** ([#1498])
- **Dashboard: "Save" is no longer enabled on a server with no PerformanceMonitor database** ([#1498])
- **Dashboard: cancelling an edit no longer silently renames the server you were editing** ([#1498])
- **Dashboard: a failed or cancelled clean install no longer hides the install log** ([#1498])
- **Both installers now refuse to run over a database that is NEWER than the binary** ([#1498])
- **Lite and Darling: the collector gate surface is collapsed to ONE shared layer, so Darling stops attempting collectors it can't run (no-msdb / Azure SQL DB / pre-2016) every cycle** ([#1500], [#1492])
- **Darling viewer: the collector-schedule presets now include `job_history`, `agent_status`, and `default_trace_events`, matching Lite** ([#1495], [#1494])
- **Lite: the `v_job_history` and `v_default_trace_events` archive views no longer double-count rows after a 512 MB emergency reset** ([#1492])
- **Lite and Darling: `agent_status` and `running_jobs` are now gated off AWS RDS in BOTH apps** ([#1492])
- **Dashboard: the cross-app `ThemeParityTests` pass again â€” the Dashboard theme palettes are re-synced to Lite** ([#1492], [#1441])
- **Lite, Dashboard, and Darling: anomaly-engine remainder Stage A â€” baseline-quality gate, one-fact wait-profile detection, and honest no-baseline rendering** ([#1491])
- **Lite and Darling: the default-trace collector can now suppress Object-DDL (schema-change) events, so a create/drop-happy workload no longer floods the System Events tab** ([#1485])
- **Lite, Dashboard, and Darling: young-store anomaly noise -- absolute-magnitude floors and a sigma display cap** ([#1486])
- **Viewer Memory and Query Performance Trends charts: the last of the trend-chart dead space is gone** ([#1487], [#1483], [#1484])
- **Viewer File I/O charts: the dead space at the chart edges is gone** ([#1484], [#1483])
- **Viewer trend charts: the empty "dead space" at the tempdb, CPU, and Blocking Stats chart edges is gone** ([#1483])
- **Darling viewer: grid default-sort audit -- history grids now newest-first, the FinOps Database Sizes grid now biggest-first** ([#1481])
- **Darling: the viewer no longer falsely gates a correctly-migrated V23 store as "schema v22"** ([#1480], [#1479])
- **Darling: the fired-alert history (`config_alert_log`) now has a retention purge, so it no longer grows unbounded** ([#1477])
- **Darling: schema V22 adds a supporting index for the FinOps Index Analysis read** ([#1477])
- **Lite and Darling: "Current Waits" now honors the ignored-waits filter, matching Wait Stats** ([#1476])
- **Lite and Dashboard: the analysis-findings retention purge is now actually scheduled** ([#1471])
- **Darling: schema V14 refreshes every `v_*` passthrough view so a store upgraded across a column-adding migration serves the new columns** ([#1262])
- **Darling viewer: the CPU series now aligns with every other timeline on non-UTC servers** ([#1262])
- **Darling: schema V5 adds the five missing passthrough views** ([#1262])
- **Darling: managed PostgreSQL now sizes background workers for TimescaleDB's compression jobs** ([#1262])
- **Lite, Dashboard, and Darling: muting a finding "across all servers" now actually mutes everywhere** ([#1262])

## [3.1.0] - 2026-06-28

Full entries: [docs/changelog/3.1.md](docs/changelog/3.1.md)

### Added

- **Lite and Dashboard: a shared block-chain viewer reconstructs the full apex â†’ victim blocking tree** ([#1207])
- **Lite and Dashboard: a shared deadlock graph viewer shows the waits-for cycles** ([#1216])
- **Lite and Dashboard: an always-on DMV blocking-snapshot fallback keeps blocking visible without the blocked-process report** ([#1227])
- **Lite and Dashboard: analysis findings are grouped into trackable incidents on the Recommendations surface** ([#1214])
- **Lite and Dashboard: FinOps storage analysis becomes an object-growth â†’ index drill with in-grid heatmaps** ([#1138], [#1135])
- **Lite and Dashboard: per-server override for the alert delivery mode** ([#1236], [#1141])
- **Lite and Dashboard: MCP tools return a structured status envelope for non-data outcomes** ([#1224])
- **Lite and Dashboard: in-app plan navigation on every query-identifying surface** ([#1184])
- **Full Dashboard: a "Collection Stopped" alert for when the collector itself goes dark** ([#1246])

### Changed

- **Lite and Dashboard: the recommendation engine's operator advice is rebuilt to be sourced and composed from your server's own facts** ([#1189], [#1196], [#1197], [#1198], [#1203], [#1244])
- **Lite and Dashboard: click a chart's legend key (or a series line) to isolate that series**
- **Lite and Dashboard: alert payloads carry a stable dedup fingerprint and involved-object list** ([#1140], [#1154], [#1141], [#1145])
- **Lite and Dashboard: optional per-event notification mode for deadlocks and blocking** ([#1141], [#1140])
- **Lite and Dashboard: the low-disk (Volume Free Space) alert is now severity-graded instead of always INFO** ([#1136])
- **Lite and Dashboard: the execution-plan viewer is now one shared control** ([#1205])
- **Lite and Dashboard: "Show Active Queries at This Time" right-click drill-down now works on every resource chart** ([#1208], [#682])
- **Lite and Dashboard: the Overview correlated-timeline lanes get the same Active Queries drill-down** ([#1210], [#1208])
- **Dashboard: the Queries and Resource tabs load much faster** ([#1181], [#1182], [#1190])
- **Lite and Dashboard: the Active Queries view refreshes when you open it** ([#1183])
- **Lite and Dashboard: resolved/cleared tray toasts are themed to match the condition cards** ([#1186])

### Fixed

- **Dashboard: analysis findings persist their database name again, and the latest-run read returns the remediation action** ([#1262])
- **Lite and Dashboard: Collection Health no longer flags skip-if-unchanged collectors as STALE/NEVER_RUN** ([#1248], [#1246])
- **Lite and Dashboard: the Overview's empty Blocking/Deadlocking lane now renders as a live grid instead of a dead black box** ([#1245])
- **Lite and Dashboard: corrected wrong and misleading recommendation advice, plus a MAXDOP code bug** ([#1185], [#1187], [#1192], [#1194])
- **Lite: the Alert History Value/Threshold columns no longer show a raw full-precision float** ([#1134])
- **Dashboard: muted and resolved alerts no longer inflate the sidebar Alert badge** ([#1226], [#1228])
- **Lite and Dashboard: Alert History rows are classified by one shared metric classifier, fixing the "Restored" row styling** ([#1228])
- **Lite and Dashboard: the Capture Down and Failed Agent Job alerts render at their real severity in email and webhook notifications** ([#1229], [#1136])
- **Lite and Dashboard: webhook (Teams/Slack) alerts no longer re-fire after an app restart** ([#1145])
- **Lite and Dashboard: alert cooldown is keyed on the [#1140] fingerprint, not just (server, metric)** ([#1154])
- **Lite: the `index_object_stats` collector no longer times out and returns zero index data on larger estates** ([#1135])
- **Dashboard: the execution-plan node "actual of estimated rows" badge no longer over-reports for multi-execution operators** ([#1205])
- **Lite: a fresh install no longer floods collection and the wait-stats tab with benign waits** ([#1240])
- **Lite: picker-chart trends no longer run an N+1 query loop** ([#1209])
- **Lite and Dashboard: upgrading the desktop app now replaces the running tray instance instead of surfacing the stale one** ([#1148])
- **Lite: the Blocking tab's ~930ms hitch and interactive UI-thread stalls are removed** ([#1193], [#1202])
- **Dashboard: "View Plan" was a silent no-op on several surfaces** ([#1181], [#1190])
- **Dashboard: the FinOps Database Sizes tab no longer shows blank until a manual refresh** ([#1179])
- **Lite and Dashboard: failed-Agent-job alerts no longer coalesce or replay after a restart** ([#1157], [#1173], [#1140], [#1145])
- **Dashboard: anomaly and baseline detection corrected to match Lite's already-fixed behavior** ([#1155])

## [3.0.0] - 2026-06-15

Full entries: [docs/changelog/3.0.md](docs/changelog/3.0.md)

### Important

- **Major release â€” 2.11.0 â†’ 3.0.0, no breaking changes.**

### Fixed

- **Lite and Dashboard: Azure SQL Database shows its real product name in FinOps â†’ Server Inventory**
- **Dashboard: "Deadlocks Cleared" no longer flaps right after every deadlock** ([#1091])
- **Lite: blocking and deadlock alerts no longer re-fire for the same events every cooldown** ([#1091])
- **Lite and Dashboard: low-disk (Volume Free Space) alert no longer re-fires every cooldown for a standing full volume** ([#1091])
- **Lite and Dashboard: low-disk and failed-Agent-job conditions now light the server tab badge** ([#754], [#749])
- **Lite: blocking/deadlock XE sessions now self-heal and failures are surfaced** ([#1086])
- **Dashboard: blocking/deadlock XE sessions self-heal, Azure SQL DB sessions are actually created, and a missing session raises a Capture Down alert** ([#1086])
- **Blocked-process and deadlock XML processors no longer loop on un-parseable events**
- **Lite and Dashboard UI no longer goes blank or disappears after sleep/wake** ([#1050])
- **"Silence All Alerts" now suppresses email too** ([#1035])
- **Dashboard time labels are now consistently 24-hour** ([#1012])
- **Lite UI no longer freezes during archival** ([#979])
- **Lite FinOps no longer recommends an edition downgrade on an Availability Group secondary** ([#980])
- **Lite alert emails no longer re-fire after an app restart** ([#981])
- **Dashboard alert emails no longer re-fire after an app restart** ([#981])
- **Analysis-finding notification cooldowns now persist across restarts on both Lite and Dashboard**
- **Data Retention job no longer fails with `xp_delete_file` error 22049** ([#972])
- **Codebase-wide correctness and security hardening pass**
- **FinOps no longer recommends downgrading to Standard Edition on a server running Availability Groups** ([#1085], [#980])
- **Server-tab alert badge is now clearable** ([#1092], [#1122])
- **Long-running-query alert no longer constantly trips on CDC capture jobs** ([#1096])

### Changed

- **Plan parsing / analysis extracted to shared library `PerformanceMonitor.PlanAnalysis`**
- **`PlanIconMapper` split to break a shared-library WPF dependency**
- **Analysis engine extracted to shared library `PerformanceMonitor.Analysis`**
- **Trace files are now bounded at the source** ([#972])
- **Blocked-process reports expose blocker-side fields as typed columns**
- **Blocking-chain reconstruction now reads typed columns from `collect.blocking_BlockedProcessReport`**
- **Analysis minimum-data threshold lowered to 24 hours**
- **Major UI-responsiveness overhaul â€” the data path now runs off the WPF dispatcher in both apps** ([#1121], [#1116], [#1050])

### Added

- **`tools/Remove-OrphanedTraceFiles.ps1`** ([#972])
- **`FactAdvice` and `FactRemediation` in `PerformanceMonitor.Analysis`**
- **Object- and index-level collection: sizes, growth, usage, and locking/contention** ([#1103])
- **Recommendations / Apply Fix engine (advise-and-act rebuild)**
- **Low volume free-space alert** ([#754])
- **Failed SQL Agent job alert** ([#749])
- **Installer: optional custom data/log file locations** ([#768])

[#3517]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3517
[#3523]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3523
[#3524]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3524
[#3525]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3525
[#3526]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3526
[#3527]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3527
[#3528]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3528
[#3529]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3529
[#3530]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3530
[#3531]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3531
[#3532]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3532
[#3533]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3533
[#3534]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3534
[#3535]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3535
[#3536]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3536
[#3537]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3537
[#3538]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3538
[#3539]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3539
[#3540]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3540
[#3541]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3541
[#3542]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3542
[#3547]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3547
[#3548]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3548
[#3551]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3551
[#3556]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3556
[#3557]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3557
[#3561]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3561
[#3563]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3563
[#3570]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3570
[#3573]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3573
[#3574]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3574
[#3575]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3575
[#3576]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3576
[#3577]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3577
[#3579]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3579
[#3580]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3580
[#3581]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3581
[#3582]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3582
[#3588]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3588
[#3591]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3591
[#3597]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3597
[#3598]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3598
[#3601]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3601
[#3602]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3602
[#3603]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3603
[#3604]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3604
[#3607]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3607
[#3608]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3608
[#3609]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3609
[#3612]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3612
[#3620]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3620
[#3622]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3622
[#3636]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3636
[#3644]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3644
[#3648]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3648
[#3650]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3650
[#3652]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3652
[#3653]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3653
[#3678]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3678
[#3691]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3691
[#3704]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3704
[#3710]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3710
[#3712]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3712
[#3735]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3735
[#3739]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3739
[#3740]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3740
[#3741]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3741
[#3742]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3742
[#3743]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3743
[#3744]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3744
[#3745]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3745
[#3752]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3752
[#3753]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3753
[#3754]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3754
[#3755]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3755
[#3756]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3756
[#3764]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3764
[#3778]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3778
[#3781]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3781
[#3783]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3783
[#3796]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3796
[#3797]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3797
[#3802]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3802
[#3805]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3805
[#3812]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3812
[#3815]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3815
[#3816]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3816
[#3817]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3817
[#3818]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3818
[#3819]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3819
[#3830]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3830
[#3833]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3833
[#3834]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3834
[#3835]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3835
[#3838]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3838
[#3848]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3848
[#3854]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3854
[#3856]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3856
[#3859]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3859
[#3868]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3868
[#3869]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3869
[#3870]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3870
[#3871]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3871
[#3876]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3876
[#3878]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3878
[#3880]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3880
[#3898]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3898
[#3899]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3899
[#3904]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3904
[#3914]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3914
[#3915]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3915
[#3514]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3514
[#3477]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3477
[#3495]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3495
[#3497]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3497
[#3500]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3500
[#3502]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3502
[#2862]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2862
[#2849]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2849
[#2843]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2843
[#2818]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2818
[#1564]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1564
[#2833]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2833
[#2825]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2825
[#2820]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2820
[#2802]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2802
[#2814]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2814
[#2807]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2807
[#2788]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2788
[#749]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/749
[#754]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/754
[#768]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/768
[#972]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/972
[#979]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/979
[#980]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/980
[#981]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/981
[#989]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/989
[#1012]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1012
[#1035]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1035
[#1048]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1048
[#1050]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1050
[#1180]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1180
[#1085]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1085
[#1086]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1086
[#1091]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1091
[#1092]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1092
[#1096]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1096
[#1103]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1103
[#1116]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1116
[#1121]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1121
[#1122]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1122
[#1134]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1134
[#1135]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1135
[#1136]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1136
[#1205]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1205
[#1208]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1208
[#1210]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1210
[#1140]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1140
[#1141]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1141
[#1145]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1145
[#1154]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1154
[#1226]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1226
[#1228]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1228
[#1229]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1229
[#1138]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1138
[#1207]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1207
[#1209]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1209
[#1214]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1214
[#1216]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1216
[#1224]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1224
[#1227]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1227
[#1236]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1236
[#1240]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1240
[#1185]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1185
[#1187]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1187
[#1189]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1189
[#1192]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1192
[#1194]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1194
[#1196]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1196
[#1197]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1197
[#1198]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1198
[#1203]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1203
[#1244]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1244
[#1245]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1245
[#1246]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1246
[#1248]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1248
[#1262]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1262
[#1479]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1479
[#1480]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1480
[#1481]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1481
[#1483]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1483
[#1485]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1485
[#1484]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1484
[#1488]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1488
[#1539]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1539
[#1486]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1486
[#1540]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1540
[#1517]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1517
[#1346]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1346
[#1535]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1535
[#1251]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1251
[#1510]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1510
[#1507]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1507
[#1506]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1506
[#1509]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1509
[#1512]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1512
[#1513]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1513
[#1514]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1514
[#1515]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1515
[#1543]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1543
[#1544]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1544
[#1545]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1545
[#1542]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1542
[#1541]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1541
[#1538]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1538
[#1548]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1548
[#1546]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1546
[#1547]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1547
[#1549]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1549
[#1550]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1550
[#1551]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1551
[#1552]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1552
[#1553]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1553
[#1554]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1554
[#1556]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1556
[#1557]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1557
[#1558]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1558
[#1559]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1559
[#1560]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1560
[#1565]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1565
[#1566]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1566
[#1567]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1567
[#1569]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1569
[#1570]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1570
[#1572]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1572
[#1571]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1571
[#1574]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1574
[#1577]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1577
[#1576]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1576
[#1579]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1579
[#1580]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1580
[#1582]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1582
[#1128]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1128
[#1581]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1581
[#1583]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1583
[#1585]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1585
[#1586]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1586
[#1588]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1588
[#1584]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1584
[#1589]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1589

[#1590]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1590
[#1592]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1592
[#1593]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1593
[#1594]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1594
[#1596]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1596
[#1597]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1597
[#1598]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1598
[#1599]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1599
[#1608]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1608
[#1609]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1609
[#1612]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1612
[#1613]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1613
[#1614]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1614
[#1616]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1616
[#1619]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1619
[#1620]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1620
[#1622]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1622
[#1623]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1623
[#1624]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1624
[#1625]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1625
[#1626]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1626
[#1627]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1627
[#1631]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1631
[#1629]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1629
[#1630]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1630
[#1632]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1632
[#1633]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1633
[#1634]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1634
[#1635]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1635
[#1636]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1636
[#1637]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1637
[#1639]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1639
[#1674]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1674
[#1668]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1668
[#1670]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1670
[#1675]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1675
[#1677]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1677
[#1683]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1683
[#1689]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1689
[#1676]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1676
[#1640]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1640
[#1642]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1642
[#1646]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1646
[#1647]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1647
[#1648]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1648
[#1651]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1651
[#1652]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1652
[#1617]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1617
[#1621]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1621
[#1601]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1601
[#1604]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1604
[#1602]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1602
[#1600]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1600
[#1578]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1578
[#1536]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1536
[#1534]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1534
[#1533]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1533
[#1532]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1532
[#1531]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1531
[#1530]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1530
[#1529]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1529
[#1528]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1528
[#1527]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1527
[#1526]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1526
[#1525]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1525
[#1524]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1524
[#1523]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1523
[#1522]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1522
[#1521]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1521
[#1520]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1520
[#1519]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1519
[#1518]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1518
[#1516]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1516
[#1505]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1505
[#1504]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1504
[#1503]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1503
[#1502]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1502
[#1501]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1501
[#1500]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1500
[#1499]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1499
[#1498]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1498
[#963]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/963
[#1495]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1495
[#1494]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1494
[#1493]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1493
[#1492]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1492
[#1491]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1491
[#1487]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1487
[#1441]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1441
[#1490]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1490
[#1478]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1478
[#1477]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1477
[#1476]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1476
[#1475]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1475
[#1474]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1474
[#1473]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1473
[#1471]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1471
[#1463]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1463
[#1489]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1489
[#1042]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1042
[#1268]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1268
[#1321]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1321
[#1286]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1286
[#1470]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1470
[#1148]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1148
[#1155]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1155
[#1157]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1157
[#1173]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1173
[#1179]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1179
[#1181]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1181
[#1182]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1182
[#1183]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1183
[#1184]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1184
[#1186]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1186
[#1190]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1190
[#1193]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1193
[#1202]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1202

## [2.11.0] - 2026-05-19

### Important

- **.NET 10 upgrade** â€” Dashboard, Lite, Installer, and Installer.Core now target `net10.0` (Windows projects target `net10.0-windows`). Building from source now requires the .NET 10 SDK; CI is pinned to 10.0.204 via `global.json` for reproducible builds. End users running prebuilt Velopack installers do not need to install anything separately â€” runtime is bundled ([#958])
- **Setup.exe is now the recommended install path** for Dashboard and Lite â€” the README steers users to the Velopack `Setup.exe`, which installs to `%LocalAppData%`, registers the apps under Apps & Features, creates Start Menu and Desktop shortcuts, and wires up auto-update. Portable ZIPs are still produced for both apps (CI release pipeline and local build scripts) as a fallback for advanced or air-gapped users. The Installer ZIP (CLI installer + SQL scripts) is unchanged
- **Shared `servers.json` location** â€” Dashboard and Lite now store `servers.json` under `%ProgramData%\PerformanceMonitor{Dashboard,Lite}\` so every Windows user on the same machine shares one server list. First run migrates an existing per-user `servers.json` to the new location and grants Authenticated Users Modify on the directory. SQL credentials remain per-user in Windows Credential Manager â€” each DBA re-enters SQL passwords on first connect; Windows Auth works with no re-entry

### Added

- **One-click snooze from the alert tray popup** in Lite â€” snooze an alert directly from the tray notification balloon without opening the main window ([#944])
- **Snooze hint in email and Teams/Slack alert payloads** â€” alert messages now show the snooze duration / scheduled wake time when an alert is fired while a snooze is active ([#944])
- **Process memory logging per collection cycle** in Lite â€” the collector now logs working set and private bytes at the end of each cycle, making it easier to track memory growth in long-running sessions

### Changed

- **Lite compaction memory tuning** ([#933]) â€” multiple changes to make parquet compaction robust on wide-row tables and large datasets:
    - Cap the main collector connection's `memory_limit` and raise it transiently only for the `COPY` step
    - Detect compaction `EXCLUDE` columns per merge step instead of once up front
    - Raise the compaction `memory_limit` floor to 4 GB
    - Set DuckDB `temp_directory` explicitly so spill files don't blow the OS temp drive
    - Compact parquet in size-budgeted batches instead of one mega-batch
- **Trace collectors honor `config.collector_database_exclusions`** ([#887] follow-up) â€” the trace-file based collectors now filter against the exclusions table, matching the behavior of the eight DMV-based per-database collectors shipped in v2.9.0
- **InstallerGui project directory removed** â€” the WPF InstallerGui was retired in v2.9.0 in favor of the Dashboard's integrated Add Server dialog. The project directory has now been deleted from the repo
- **Build warnings cleaned up** across Lite, Dashboard, and Installer ([#945])
- **GitHub Actions runners bumped** to Node 24-compatible major versions to silence deprecation warnings

### Fixed

- **Re-run `installation_history` column widening** for servers that crossed v2.4.0 â†’ v2.5.0 before PR #828's fix shipped in v2.7.0. Those servers ran the original widen script as a no-op against `master`, then advanced their installer_version past 2.5, so the now-fixed script never reapplied. Adds an idempotent ALTER under an `IF EXISTS` guard checking `max_length = 510` ([#828])
- **Mute rules preserved across size-triggered DuckDB reset** in Lite â€” when the local DuckDB exceeded the configured size budget and was reset, mute rules were being lost. They now survive the reset ([#938])
- **Chart tooltips break after tab switch** â€” root-cause fix for the popup-wedge issue first patched in v2.10.0. Both the Memory tab handlers and `CorrelatedCrosshairManager` are now resilient to tab churn ([#916], [#937])
- **Stale `Monitor_LongQueries_*.trc` files cleaned up** by `config.data_retention` â€” the trace-file cleanup step previously left old `.trc` files behind on disk ([#951])
- **Nullability guards** added to the remaining comparison overlay tasks that were producing CS86xx warnings

[#916]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/916
[#933]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/933
[#937]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/937
[#938]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/938
[#944]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/944
[#945]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/945
[#951]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/951
[#958]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/958

## [2.10.0] - 2026-05-04

### Fixed

- **Memory tab tooltip** stops working after switching away and returning to the tab. Both Dashboard and Lite Memory tab crosshair tooltip handlers now reattach correctly on tab re-entry; the same popup-wedge fix is also applied to `CorrelatedCrosshairManager` ([#916])
- **FinOps memory recommendation** now bases sizing on a 7-day P95 of memory samples instead of a single snapshot, so recommendations no longer swing based on instantaneous workload state. Applied in both Dashboard and Lite ([#917])

### Changed

- **Per-database grants for FinOps Index Analysis** documented in the README â€” sp_IndexCleanup-backed Index Analysis requires per-database `EXECUTE` grants on each user database you want to analyze ([#915])

[#915]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/915
[#917]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/917

## [2.9.0] - 2026-04-29

### Important

- **Breaking change to `config.data_retention`** â€” the `@truncate_all` parameter has been removed. Pass `@retention_days = 0` for the same behavior. `@retention_days = NULL` (default) respects per-collector retention from `config.collection_schedule` with a 30-day fallback for unscheduled tables; `@retention_days = N > 0` overrides every table to N days. Any existing Agent jobs or scripts calling `data_retention @truncate_all = 1` need to be updated ([#900])
- **New `config.collector_database_exclusions` table** for per-database collector exclusions. Eight per-database collectors filter against this table; system databases remain hard-skipped by the collectors themselves. Existing installs get the table on the next upgrade â€” `install/01_install_database.sql` and `config.ensure_config_tables` both create it under an `IF OBJECT_ID â€¦ IS NULL` guard ([#887])

### Added

- **Per-database collector exclusions** â€” exclude noisy or unimportant databases from per-database collectors. Dashboard side adds `config.collector_database_exclusions` and filters 8 collectors (`query_stats`, `query_store`, `procedure_stats`, `file_io_stats`, `waiting_tasks`, `database_configuration`, `database_size_stats`, `server_properties`). Lite side adds an `ExcludedDatabases` list per server in `servers.json` and filters 9 collectors ([#887])
- **`Off` collection preset** â€” `EXECUTE config.apply_collection_preset @preset_name = N'Off'` disables every collector in one call. Pair with a second Agent job that applies a non-`Off` preset at the start of your active window for overnight / quiet-hours scoping. Non-`Off` presets now also set `enabled = 1` across the board so the switch from `Off â†’ Balanced` reliably resumes collection ([#888])
- **Purge Now action** in Manage Servers â€” confirm dialog with a mode picker (Use configured / 1 / 3 / 7 / Custom / All) drives `config.data_retention`; right-click menu on the Manage Servers grid mirrors every per-row action (Edit, Toggle Favorite, Check Server Version, Purge Now, Remove) ([#900])
- **Total non-idle CPU on Lite Overview** â€” headline value shows total CPU with the SQL-only value alongside (e.g. `64% (SQL 60%)`); new `CpuAlertMode` dropdown in Settings â†’ Alerts (Total / SqlOnly) drives both the alert evaluator and headline color; tray notifications and email alerts label the value as "Total CPU" or "SQL CPU" ([#899])
- **Resume gap detection** â€” `query_stats`, `procedure_stats`, and `query_store` collectors skip the historical sweep on first run after an Off preset, Agent stoppage, or server reboot. When the last successful run is older than 5Ã— the configured `frequency_minutes` (floored at 30 minutes), the cutoff clamps to `SYSDATETIME()` so only forward-going data is collected on resume â€” preventing the tempdb blowout that hit the original reporter ([#892])
- **Right-click View Plan** on Dashboard Blocked Process Reports (View Blocked Plan + View Blocking Plan), Dashboard Deadlocks, and Lite Deadlocks grids. Plan lookup hits `sys.dm_exec_query_stats` + `sys.dm_exec_text_query_plan` on the monitored server, falling back to `executionStack/frame` entries when the process-level `sql_handle` is empty or evicted ([#880])
- **Open Log Folder** sidebar button in Lite â€” opens `%LocalAppData%\PerformanceMonitorLite\logs\` in Explorer for grabbing historical logs to attach to bug reports. Sits below View Log, which retains its existing behavior of opening today's log file ([#873])
- **Installed Version column** in the Manage Servers grid for both Dashboard and Lite. Dashboard shows the PerformanceMonitor database version on each server (probed in parallel via `GetInstalledVersionAsync`, with `Not installed` / `Unavailable` fallbacks). Lite shows the running app's own version on every row, mirroring Full's column header for consistency.
- **Lite-style server card indicators in Full** â€” back-ported the Ellipse-with-DataTriggers status dot (Online/Offline/Warning/Unknown) and the right-aligned favorite star from Lite to the Full Dashboard's server list, matching Lite's visual treatment.
- **Architecture overview** at `docs/how-collection-works.md` covering the minute loop, dispatcher, collector shape, `config.collection_schedule`, retention, and the Dashboard read path

### Changed

- **PlanIconMapper synced** with PerformanceStudio v1.9.0 improvements â€” columnstore storage type on scan/delete/insert/update/merge operators routes to `columnstore_index_*` icons (covers CCI and NCCI); `Parallelism` operator subtypes (Repartition Streams, Distribute Streams, Gather Streams) get their own icons
- **`Microsoft.Data.SqlClient` 6.1.4 â†’ 7.0.1** â€” major-version bump. Azure/Entra dependencies were split out of the core package in 7.0; `Microsoft.Data.SqlClient.Extensions.Azure 1.0.0` added to Dashboard, Lite, and Installer.Core for `ActiveDirectoryInteractive` connections
- **`ModelContextProtocol` 0.7.0-preview.1 â†’ 1.2.0** â€” off the preview tag and onto stable 1.x in Dashboard and Lite
- **`DuckDB.NET` 1.5.0 â†’ 1.5.2** in Lite â€” fixes unbounded row group growth on indexed tables under repeated load+insert cycles, memory leaks and race conditions in prepared statements, WAL checkpoint marking, and Windows UTF-8/UTF-16 handling
- **`Microsoft.Extensions.*` 10.0.5 â†’ 10.0.7**, **`System.Text.Json` 10.0.5 â†’ 10.0.7**, **`ScottPlot.WPF` 5.1.57 â†’ 5.1.58** â€” patch-level bumps with no expected behavioral change
- **Theme polish** on grids and plan viewer in Dashboard and Lite â€” thanks [@ClaudioESSilva](https://github.com/ClaudioESSilva) ([#889])

### Fixed

- **Install loop timeout** raised from 5 minutes to 1 hour. `install/98_validate_installation.sql` runs every enabled collector with `@debug = 1` in a single batch; on large databases (reporter had 7.2M rows in `collect.query_stats`, 4.4M in `collect.query_store_data`) this took ~9 minutes and was blowing the 5-minute timeout, failing the install or upgrade ([#884])

[#873]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/873
[#880]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/880
[#884]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/884
[#887]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/887
[#888]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/888
[#889]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/889
[#892]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/892
[#899]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/899
[#900]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/900

## [2.8.0] - 2026-04-22

### Important

- **New nonclustered indexes** on `collect.query_stats`, `collect.procedure_stats`, and `collect.query_store_data` to eliminate Eager Index Spools in Dashboard grid queries. On large installations these indexes may take several minutes to build; the upgrade script uses `ONLINE = ON` on Enterprise/Developer/Azure editions and falls back to offline on Standard/Web ([#835])

### Added

- **Memory Pressure Events in Lite** â€” the collector, chart, and `get_memory_pressure_events` MCP tool previously only in the Full Edition are now available in Lite ([#865])
- **Grid auto-scrolling** in Lite and Dashboard ([#843]) â€” thanks [@ClaudioESSilva](https://github.com/ClaudioESSilva)

### Changed

- **PlanAnalyzer and BenefitScorer** synced with PerformanceStudio's Apr 9â€“16 improvements
- **Query/Procedure/Query Store stats** refactored to a phased DECOMPRESS approach; removed unhelpful `WAITFOR DECOMPRESS` filters
- **Query/Procedure/Query Store grids** capped to TOP 500 to prevent UI freezes on large datasets
- **Server tabs lazy-load** â€” only the visible server tab loads on startup; remaining tabs load on first visit
- **Webhook URLs (Dashboard)** encrypted with DPAPI via Windows Credential Manager â€” Lite webhook URLs remain in plaintext settings for now
- **DuckDB queries hardened** â€” parameterized values, escaped paths, fixed `IsArchiving` race
- **Lite chart axes and sub-tab styling** polished, then ported to Dashboard

### Fixed

- **Memory Pressure Events chart filter** was dropping valid rows; added MCP interpretation guidance ([#865])
- **FinOps recommendation severity sort order** in Lite and Dashboard ([#872])
- **Overview crosshair** disappearing after tab switches or layout passes
- **Blocked process report plan lookup** returning the wrong plan ([#867])
- **FinOps TDE recommendation** flagging Standard edition on SQL Server 2019+ where TDE is free ([#854])
- **Azure SQL DB collector** falls back to single-database mode when `master` is inaccessible ([#857])
- **Azure SQL DB query snapshots** scoped to the current database ([#857])
- **Azure SQL DB query snapshot prefilter** â€” request set is narrowed into `#temp` before joining DMVs to avoid Azure-specific execution plan issues ([#857])
- **Azure SQL DB live query plans** â€” now skipped gracefully instead of erroring ([#857])
- **Azure SQL DB memory_stats collector** â€” dropped `sys.dm_os_schedulers` which is blocked on elastic-pool contained users regardless of DB-scoped grants ([#857])
- **Non-transient permission denials** now stop collector retries instead of looping forever ([#857])

[#835]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/835
[#843]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/843
[#854]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/854
[#857]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/857
[#865]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/865
[#867]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/867
[#872]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/872

## [2.7.0] - 2026-04-13

### Added

- **Host OS column** in Server Inventory for both Dashboard and Lite ([#748], [#823])
- **Offline community script support** via `community/` directory for user-contributed scripts ([#814], [#822])
- **MultiSubnetFailover connection option** in Dashboard and Lite for Always On availability groups ([#813], [#821])

### Changed

- **PlanAnalyzer and ShowPlanParser** synced from PerformanceStudio with latest improvements ([#816])
- **MCP query tools** optimized for large databases ([#826])
- **Add Server dialog UX** improved with inline connection status and full-height window
- **"CPUs" renamed to "Logical CPUs"** for clarity in Lite ([#825])

### Fixed

- **Dashboard auto-refresh stalling under load** â€” replaced DispatcherTimer with async Task.Delay loop to prevent priority starvation during heavy chart rendering ([#833], [#834])
- **Lite auto-refresh silently skipping** every tick ([#824])
- **Deadlock count not resetting** between collections ([#803], [#820])
- **Upgrade filter skipping patch versions** during version comparison ([#817], [#819])
- **Upgrade script executing against master** instead of PerformanceMonitor database ([#828])
- **Duplicate release builds** triggering on both created and published events

[#748]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/748
[#803]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/803
[#813]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/813
[#814]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/814
[#816]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/816
[#817]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/817
[#819]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/819
[#820]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/820
[#821]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/821
[#822]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/822
[#823]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/823
[#824]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/824
[#825]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/825
[#826]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/826
[#828]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/828
[#833]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/833
[#834]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/834

## [2.6.0] - 2026-04-08

### Added

- **Correlated timeline lanes** on Lite Overview and Dashboard â€” synchronized CPU, memory, waits, and TempDB trend lanes for at-a-glance correlation ([#688])
- **Dynamic baselines and anomaly detection** in Lite and Dashboard â€” automatic baseline calculation with anomaly highlighting on key metrics ([#692], [#693])
- **Query grid comparison** â€” before/after comparison mode for query grids in Lite and Dashboard with global Compare dropdown ([#687])
- **Nonclustered index count badge** on modification operators in plan viewer ([#788])
- **Upgrade detection in Edit Server** dialog â€” see pending upgrades without adding a new server ([#772])
- **CLI installer interactive mode** prompts for trust-cert and encryption settings ([#784])
- **SignPath code signing** â€” release binaries are now digitally signed via the [SignPath FOSS](https://signpath.io) program

### Changed

- **PlanAnalyzer Rule 3 (Serial Plan)** comprehensively refined â€” severity demotion for TRIVIAL and 0ms plans, `CouldNotGenerateValidParallelPlan` treated as actionable, all 25 `NonParallelPlanReason` values now covered
- **PlanAnalyzer warning rules** ported from PerformanceStudio improvements
- **Text readability** â€” replaced all muted/dim text colors with full foreground colors for readability

### Fixed

- **Embedded resource upgrade discovery** broken â€” upgrades silently returned zero results for Dashboard installs ([#772])
- **Archive compaction OOM** on large parquet groups
- **CLI installer argument parsing** treating flags as positional args ([#786])
- **Lite long-running query alerts** firing on stale DuckDB snapshots
- **FinOps Enterprise feature detection** now queries all databases and filters to TDE only ([#780])
- **Second launch error** â€” now brings existing window to foreground instead ([#769])
- **Overview tab Memory Grant** showing 0 for all timestamps ([#776])
- **Lite FinOps Enterprise features** query error on servers without `database_id` column ([#777])
- **Collector health status** incorrect for on-load collectors
- **CSV and clipboard exports** writing `System.Windows.Controls.StackPanel` as column headers instead of actual header text ([#805])

[#687]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/687
[#688]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/688
[#692]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/692
[#693]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/693
[#769]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/769
[#772]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/772
[#776]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/776
[#777]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/777
[#780]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/780
[#784]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/784
[#786]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/786
[#788]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/788
[#805]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/805

## [2.5.0] - 2026-03-30

### Important

- **InstallerGui retired**: The standalone GUI installer has been removed. Installation, upgrade, and uninstall are now handled directly from the Dashboard's Add Server dialog, powered by the new Installer.Core shared library. The CLI installer continues to work as before. ([#755])

### Added

- **Dashboard integrated installer** â€” Add Server dialog now installs, upgrades, and uninstalls PerformanceMonitor directly, replacing the standalone InstallerGui ([#755])
- **Installer.Core shared library** â€” shared installation logic used by both the CLI installer and Dashboard ([#755])
- **Overview tab** for Lite with 2x2 resource chart grid (CPU, Memory, Wait Stats, TempDB) ([#689])
- **Chart drill-down** on CPU, Memory, TempDB, Blocking, and Deadlock charts in both Dashboard and Lite â€” right-click any chart point to jump to Active Queries for that time window ([#682])
- **Grid-to-slicer overlay** for Query Stats, Procedure Stats, and Query Store tabs â€” click a row to overlay its trend on the slicer chart ([#683])
- **Query heatmap** tab in both Dashboard and Lite â€” visual heat map of query activity over time ([#739], [#743])
- **Webhook notifications** for alerts â€” configurable webhook endpoint for alert delivery ([#725])
- **Per-server collector schedule intervals** â€” customize collection frequency per server ([#703])
- **Investigate button** in Critical Issues grid â€” jump directly to relevant tab from an alert ([#684])
- **Dismiss Selected** context menu and View Log sidebar button for alert management ([#718], [#740])
- **Alert archival awareness** â€” dismissed_archive_alerts sidecar table, source column for live vs archived alerts, stale-data indicator, structured telemetry ([#718])
- **Dashboard read-only connection intent** â€” connections use `ApplicationIntent=ReadOnly` where supported ([#728])
- FUNDING.yml for GitHub Sponsors ([#752])

### Changed

- **Installer architecture** refactored: CLI installer is now a thin wrapper over Installer.Core ([#755])
- **DuckDB memory capped** at 2 GB during parquet compaction to prevent out-of-memory on large archives ([#758])
- **Text rendering** improved with `TextOptions.TextFormattingMode="Display"` for sharper text ([#710])
- **installation_history version columns** widened from nvarchar(255) to nvarchar(512) to handle long @@VERSION strings ([#712])

### Fixed

- **Memory leaks in Lite** â€” delta cache, event handlers, and chart helpers properly disposed ([#758])
- **Doomed transaction errors** in delta framework and ensure_collection_table â€” ROLLBACK now occurs before error logging ([#756])
- **XACT_STATE check** added after third-party stored procedure calls (sp_HumanEventsBlockViewer, sp_BlitzLock) to prevent doomed transaction errors ([#695])
- **CREATE DATABASE failure** when model database has large default file sizes ([#676])
- **CPU metrics mixed** for different Azure SQL databases on the same logical server ([#680])
- **Azure SQL DB vCore** FinOps calculations incorrect for serverless/vCore tiers ([#736])
- **Webhook alert recording** not persisting correctly ([#726])
- **Drill-down timezone** misalignment between chart and detail view ([#747], [#750])
- **Drill-down refresh** losing context on auto-refresh ([#744])
- **Drill-down target** incorrectly routing Memory to Memory Grants instead of Active Queries ([#706])
- **Heatmap colorbar stacking** when switching between servers ([#746])
- **Display mode pickers** not reflecting current state on tab switch ([#751])
- **Slicer custom range** handling and sub-hour display issues ([#704])
- **Overlay selection** lost on Dashboard auto-refresh ([#683])
- **Numeric values** in alert details treated as strings instead of numbers ([#732])
- **FinOps VM right-sizing** query error â€” `PERCENTILE_CONT` missing required `OVER()` clause
- **FinOps Enterprise features** query error on AWS RDS â€” `database_id` column not present in `sys.dm_db_persisted_sku_features` on RDS
- **FinOps right-click copy** broken on all Dashboard FinOps grids â€” context menu walked to row instead of grid
- **FinOps recommendation error logs** now include server name for easier troubleshooting

### Deprecated

- **InstallerGui** â€” removed from the solution and build pipeline. Use the Dashboard or CLI installer instead. ([#755])

[#676]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/676
[#680]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/680
[#682]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/682
[#683]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/683
[#684]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/684
[#689]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/689
[#695]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/695
[#703]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/703
[#704]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/704
[#706]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/706
[#710]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/710
[#712]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/712
[#718]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/718
[#725]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/725
[#726]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/726
[#728]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/728
[#732]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/732
[#736]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/736
[#739]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/739
[#740]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/740
[#743]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/743
[#744]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/744
[#746]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/746
[#747]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/747
[#750]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/750
[#751]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/751
[#752]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/752
[#755]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/755
[#756]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/756
[#758]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/758

## [2.4.0] - 2026-03-23

### Important

- **Lite data directory moved**: Lite now stores all data (config, DuckDB, archives, logs) in `%LOCALAPPDATA%\PerformanceMonitorLite\` instead of alongside the executable. This enables auto-update support. Existing users upgrading from the zip should use **Import Settings** and **Import Data** to bring over their configuration and historical data from the old install folder.
- **Auto-update (Windows)**: Both Dashboard and Lite now include Velopack auto-update. Users who install via the new Setup.exe will receive update notifications and can download + apply updates from within the app. Existing zip distribution continues to work as before.

### Added

- **Velopack auto-update** for Dashboard and Lite â€” check on startup, download + apply from About window with confirmation dialog before restart ([#635])
- **Per-tab time range slicers** on Dashboard and Lite query tabs â€” filter data directly on each tab without changing global time range ([#655], [#662])
- **Time display picker** (Local/UTC/Server) in Dashboard and Lite toolbars ([#646])
- **Import Settings** â€” renamed from "Import Connections", now also copies `settings.json`, `collection_schedule.json`, `ignored_wait_types.json`, and `alert_state.json` from a previous install
- **Alert muting improvements** â€” pre-fill context fields (database, query, wait type, job name) from alert detail text, configurable default expiration for new mute rules, tooltip on query text field ([#642])
- **Missing date columns** on Query Stats and Procedure Stats tabs (`creation_time`, `last_execution_time`) ([#649], [#651], [#654])
- **Trace pattern drill-down** now includes `CollectionTime` and `NtUserName` columns ([#663])
- **DataGrid sort preservation** across auto-refresh â€” sort order no longer resets when data refreshes ([#659])
- **CLI installer**: colored output (green/red/yellow) and version check on startup ([#639])
- **GUI installer**: version check on startup
- **Growth rate and VLF count** columns in Database Sizes (from v2.3.0 nightly, now in upgrade path) ([#567])
- `llms.txt` and `CITATION.cff` for project discoverability ([#630])

### Changed

- **Lite data directory** moved to `%LOCALAPPDATA%\PerformanceMonitorLite\` for Velopack compatibility
- **Delta gap detection** added to all cumulative-counter collectors (file I/O, wait stats, query stats, procedure stats, memory grants) â€” prevents inflated spikes after app restart ([#633])
- **File I/O NULL fallbacks** improved when `sys.master_files` is inaccessible â€” falls back to `DB_NAME()` and `File_{id}` instead of generic "Unknown" ([#633])
- **Running jobs collector** skipped gracefully when login lacks msdb access ([#656])
- NuGet packages updated to latest minor versions ([#653])

### Fixed

- **Installer writing SUCCESS when files fail** â€” CLI tolerated 1 failure in automated mode, GUI had a similar workaround. Now any failure = not success.
- **Query stats collector causing SQL dumps** on passive mirror servers â€” removed `dm_exec_plan_attributes` CROSS APPLY, uses temp table of ONLINE database IDs instead ([#632])
- **Trigger name extraction** fails when comment before `CREATE TRIGGER` contains " ON " ([#666])
- **FinOps expensive queries** DuckDB error â€” query referenced `statement_start_offset` column that doesn't exist in schema
- **Imported parquet files** not recognized by archive compaction â€” added regex patterns for `imported_` prefix
- **Auto-refresh after Import Data** â€” views now refresh immediately after import completes

[#630]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/630
[#632]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/632
[#633]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/633
[#635]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/635
[#639]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/639
[#642]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/642
[#646]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/646
[#649]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/649
[#651]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/651
[#653]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/653
[#654]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/654
[#655]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/655
[#656]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/656
[#659]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/659
[#662]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/662
[#663]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/663
[#666]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/666
[#567]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/567

## [2.3.0] - 2026-03-18

### Important

- **Schema upgrade**: Six columns widened across three tables (`query_stats`, `cpu_scheduler_stats`, `waiting_tasks`, `database_size_stats`) to match DMV documentation types. These are in-place ALTER COLUMN operations â€” fast on any table size, no data migration. Upgrade scripts run automatically via the CLI/GUI installer.
- **SQL Server version check**: Both installers now reject SQL Server 2014 and earlier before running any scripts, with a clear error message. Azure MI (EngineEdition 8) is always accepted. ([#543])
- **Installer adversarial tests**: 35 automated tests covering upgrade failures, data survival, idempotency, version detection fallback, file filtering, restricted permissions, and more. These run as part of pre-release validation. ([#543])

### Added

- **ErikAI analysis engine** â€” rule-based inference engine for Lite that scores server health across wait stats, CPU, memory, I/O, blocking, tempdb, and query performance. Surfaces actionable findings with severity, detail, and recommended actions. Includes anomaly detection (baseline comparison for acute deviations), bad actor detection (per-query scoring for consistently terrible queries), and CPU spike detection for bursty workloads. ([#589], [#593])
- **ErikAI Dashboard port** â€” full analysis engine ported to Dashboard with SQL Server backend ([#590])
- **FinOps cost optimization recommendations** â€” Phase 1-4 checks: enterprise feature audit, CPU/memory right-sizing, compression savings estimator, unused index cost quantification, dormant database detection, dev/test workload detection, VM right-sizing, storage tier optimization, reserved capacity candidates ([#564])
- **FinOps High Impact Queries** â€” 80/20 analysis showing which queries consume the most resources across all dimensions ([#564])
- **FinOps dollar-denominated cost attribution** â€” per-server monthly cost setting with proportional database-level breakdown ([#564])
- **On-demand plan fetch** for bad actor and analysis findings â€” click to retrieve execution plans for flagged queries ([#604])
- **Plan analysis integration** â€” findings include execution plan analysis when plans are available ([#594])
- **Server unreachable email alerts** â€” Dashboard sends email (not just tray notification) when a monitored server goes offline or comes back online ([#529])
- **Column filters on all FinOps DataGrids** â€” filter funnel icons on every column header across all 7 FinOps grids in Lite and Dashboard ([#562])
- **Column filters on Dashboard** IdleDatabases, TempDB, and Index Analysis grids
- **Lite data import** â€” "Import Data" button brings in monitoring history from a previous Lite install via parquet files, preserving trend data across version upgrades ([#566])
- **Per-server Utility Database setting** â€” Lite can call community stored procedures (sp_IndexCleanup) from a database other than master ([#555])
- **SQL Server version check** in both CLI and GUI installers â€” rejects 2014 and earlier with a clear message ([#543])
- **Execution plan analysis MCP tools** for both Dashboard and Lite
- **Full MCP tool coverage** â€” Dashboard expanded from 28 to 57 tools, Lite from 32 to 51 tools ([#576], [#577])
- **Self-sufficient analyze_server drill-down** â€” MCP tool returns complete analysis, not breadcrumb trail ([#578])
- **NuGet package dependency licenses** in THIRD_PARTY_NOTICES.md

### Changed

- **Azure SQL DB FinOps** â€” all collectors (database sizes, query stats, file I/O) now connect to each database individually instead of only querying master. Server Inventory uses dynamic SQL to avoid `sys.master_files` dependency. ([#557])
- **Index Analysis scroll fix** â€” both summary and detail grids now use proportional heights instead of Auto, so they scroll independently with large result sets ([#554])
- **Dashboard Add Server dialog** â€” increased MaxHeight from 700 to 850px so buttons are visible when SQL auth fields are shown
- **GUI installer** â€” Uninstall button now correctly enables after a successful install
- **GUI installer** â€” fixed encryption mapping and history logging ([#612])
- **Dashboard visible sub-tab only refresh** on auto-refresh ticks ([#528])
- Analysis engine decouples data maturity check from analysis window

### Fixed

- **Installer dropping database on every upgrade** â€” `00_uninstall.sql` excluded from install file list, installer aborts on upgrade failure, version detection fallback returns "1.0.0" instead of null ([#538], [#539])
- **SQL dumps on mirroring passive servers** from FinOps collectors ([#535])
- **RetrievedFromCache** always showing False ([#536])
- **Arithmetic overflow** in query_stats collector for dop/thread columns ([#547])
- **Lite perfmon chart bugs** and Dashboard ScottPlot crash handling ([#544], [#545])
- **PLE=0 scoring bug** â€” was scored as harmless, now correctly flagged ([#543])
- **PercentRank >1.0** bug in HealthCalculator
- **6 verified Lite bugs** from code review ([#611])
- **Enterprise feature audit text** â€” partitioning is not Enterprise-only
- **FinOps collector scheduling**, server switch, and utilization bugs
- **Dashboard drill-down** Unicode arrow in story path split
- **Empty DataGrid scrollbar artifacts** â€” hide grids when empty across all FinOps tabs
- **Query preview** â€” truncated in row, full text in tooltip

[#529]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/529
[#535]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/535
[#536]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/536
[#538]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/538
[#539]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/539
[#543]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/543
[#544]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/544
[#545]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/545
[#547]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/547
[#554]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/554
[#555]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/555
[#557]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/557
[#562]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/562
[#564]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/564
[#566]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/566
[#576]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/576
[#577]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/577
[#578]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/578
[#528]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/528
[#589]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/589
[#590]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/590
[#593]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/593
[#594]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/594
[#604]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/604
[#611]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/611
[#612]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/612

## [2.2.0] - 2026-03-11

**Contributors:** [@HannahVernon](https://github.com/HannahVernon), [@ClaudioESSilva](https://github.com/ClaudioESSilva), [@dphugo](https://github.com/dphugo), [@Orestes](https://github.com/Orestes) â€” thank you!

### Important

- **Schema upgrade**: Three large collection tables (`query_stats`, `procedure_stats`, `query_store_data`) are migrated to use `COMPRESS()` for query text and plan columns. The upgrade performs a table swap (create new â†’ migrate data â†’ rename) which may take several minutes on large tables. A `row_hash` column is added for deduplication. Three new tracking tables are also created. Volume stats columns are added to `database_size_stats`. Upgrade scripts run automatically via the CLI/GUI installer and use idempotent checks.

  Compression results measured on a production instance:

  | Table | Compressed | Uncompressed | Ratio |
  |---|---|---|---|
  | query_stats | 18.0 MB | 339.0 MB | 18.8x |
  | query_store_data | 13.5 MB | 258.0 MB | 19.1x |
  | **Total** | **31.5 MB** | **597 MB** | **~19x** |

### Added

- **FinOps monitoring tab** â€” database size tracking, server properties, storage growth analysis (7d/30d), index analysis with unused/duplicate/compressible detection, utilization efficiency, idle database identification, and estate-level resource views ([#474])
- **Named collection presets** â€” Aggressive, Balanced, and Low-Impact schedule profiles via `config.apply_collection_preset` ([#454])
- **Entra ID interactive MFA authentication** in both CLI and GUI installers for Azure SQL MI connections ([#481])
- **MCP port validation** â€” TCP port conflict detection, range validation (1024+), Auto port button, and auto-restart on settings change ([#453])
- **Alert database exclusion filters** â€” filter blocking and deadlock alerts by database in both Dashboard and Lite ([#410], [#412])
- **Configurable alert cooldown periods** for tray notifications and email alerts
- **Wait stats query drill-down** â€” click a wait type to see the queries causing it ([#372])
- **Configurable long-running query settings** â€” max results, WAITFOR/backup/diagnostics exclusions ([#415])
- **Uninstall option** in both CLI and GUI installers ([#431])
- **Session stats collector** for active session tracking ([#474])
- **LOB compression and deduplication** for query stats tables to reduce storage ([#419])
- **Volume-level drive space** enrichment in database size stats via `dm_os_volume_stats`
- **GUI installer installation history** logging to `config.installation_history` ([#414])
- **ReadOnlyIntent connection option** â€” Lite connections can set `ApplicationIntent=ReadOnly` for automatic read routing to Always On AG readable secondaries ([#515])
- **Alert muting** â€” mute individual alerts or create pattern-based mute rules by server, metric, database, or application. Manage Mute Rules window with enable/disable toggle. Alert history detail view with double-click drill-down and context-sensitive detail text. Poison wait type documentation links. ([#512])
- **SignPath code signing** â€” all release binaries (Dashboard, Lite, Installers) are digitally signed, eliminating Windows SmartScreen warnings ([#511])
- CI version bump check on PRs to main
- Permissions section in README with least-privilege setup ([#421])

### Changed

- **Utilization tab redesigned** â€” ported to Dashboard with aligned metrics between apps ([#478])
- PlanAnalyzer rules synced from PerformanceStudio â€” Rule 5 message format, seek predicate parsing, spool labels, unmatched index detail ([#416], [#475], [#480])
- Data retention now purges processed XE staging rows
- GeneratedRegex conversion for compile-time regex patterns ([#346], [#420])
- Server health card width increased from 260 to 300 for less text truncation ([#489])
- User's locale used for date/time formatting in WPF bindings ([#459])
- XML processing instructions stripped from sql_command/sql_text display
- Parameterized queries in blocking/deadlock alert filtering
- **DuckDB 1.5.0 upgrade** â€” non-blocking checkpointing eliminates read stalls during WAL flushes, free block reuse stabilizes database file size without archive-and-reset cycles ([#516])
- **Automatic parquet compaction** â€” archive files are merged into monthly files after each archive cycle, reducing file count from 2,600+ to ~75 and eliminating per-file metadata overhead on glob scans ([#516])

  Combined with the UI responsiveness overhaul (#510), Lite's refresh cycle improved 13-26x:

  | Metric | Before | After |
  |---|---|---|
  | Lite `RefreshAllDataAsync` | 6-13s | < 500ms |
  | Parquet files scanned per query | 233 | 19 |
  | Archive-and-reset frequency | 21/day | ~0 |
  | `v_wait_stats` query time | 1,700ms | 27ms |

- **Monthly archive retention** â€” switched from 90-day file-age deletion to 3-month calendar-month rolling window, aligned with compacted monthly filenames ([#516])
- **Lite status bar** shows used data size vs file size (e.g., "Database: 175.5 / 423.8 MB") via DuckDB `pragma_database_size()` ([#517])
- **Query Store collector diagnostics** â€” reader/append/flush timing breakdown logged when collection exceeds 2 seconds, for identifying SQL Server DMV contention under heavy workloads ([#518])
- SSMS-parity edge tooltips on plan viewer operator connections and ManyToMany indicator always shown for merge join operators ([#504])
- **Lite UI responsiveness overhaul** â€” visible-tab-only refresh, sub-tab awareness, Query Store collector optimization (NULL plan XML + LOOP JOIN hint), and DuckDB write reduction ([#510])

  Timer tick improvements measured under TPC-C load on SQL2022:

  | Scenario | Before | After | Improvement |
  |---|---|---|---|
  | Lite idle | 6-13s | 546-750ms | ~90% |
  | Lite under TPC-C | 6-13s | ~3s | ~70% |
  | Dashboard idle | 5.6s | 0.6-0.8s | 86% |
  | Dashboard under TPC-C | 5.6s | 1.8-2.0s | 64% |

  Query Store collector specifically:

  | Metric | Before | After |
  |---|---|---|
  | query_store collector total | 6-18s | ~600ms |
  | query_store SQL time | 374-1,104ms | ~300ms (LOOP JOIN hint) |
  | query_store DuckDB write | 6-16s | ~75-230ms (NULL plan XML) |

### Fixed

- **UI hang** when opening Dashboard tab for offline server â€” replaced synchronous `.GetAwaiter().GetResult()` with proper `await` ([#477])
- **First-collection spike** skewing PerfMon, wait stats, file I/O, memory grant, query stats, and procedure stats charts â€” first cumulative value now treated as baseline ([#482])
- **Wait type filter TextBox** too small to read ([#488])
- **Poison wait false positives** and alert log parsing ([#445], [#448])
- **RID Lookup** analyzer rule matching new PhysicalOp label ([#429])
- **procedure_stats** plan query using DECOMPRESS after compression migration
- **database_size_stats** InvalidCastException on compatibility_level
- **Deadlock filter** using wrong column reference in `GetFilteredDeadlockCountAsync`
- **RESTORING database** filter added to waiting_tasks collector ([#430])
- Custom TrayToolTip crash â€” replaced with plain ToolTipText ([#422])
- **Lite tab switch freeze** â€” added `_isRefreshing` guard to prevent tab switch handler from competing with timer ticks for DuckDB connection, eliminating "not responding" hangs ([#510])
- DuckDB read lock acquisition resilience
- Formatted duration columns sorting alphabetically instead of numerically
- Settings window staying open on validation errors
- Deserialization clamping and validation abort issues
- **sp_IndexCleanup** summary grid column mapping off-by-one, expanded both grids to show all columns from both result sets ([#503])
- **Rule 22 table variable** false positive on modification operators â€” INSERT/UPDATE/DELETE on table variables is expected ([#513])
- **ComboBox focus steal** in plan viewer stealing keyboard focus from other controls ([#508])
- **DOP 2 skew** false positive â€” parallel skew rule no longer fires at DOP 2 ([#508])
- **ReadOnlyIntent connections** sharing server_id in DuckDB when the same server was added with and without ReadOnlyIntent ([#521])

[2.2.0]: https://github.com/erikdarlingdata/PerformanceMonitor/compare/v2.1.0...v2.2.0

## [2.1.0] - 2026-03-04

### Important

- **Schema upgrade**: The `config.collection_schedule` table gains two new columns (`collect_query`, `collect_plan`) for optional query text and execution plan collection. Both default to enabled to preserve existing behavior. Upgrade scripts run automatically via the CLI/GUI installer and use idempotent checks.

### Added

- **Light theme and "Cool Breeze" theme** â€” full light mode support for both Dashboard and Lite with live preview in settings ([#347])
- **Standalone Plan Viewer** â€” open, paste (Ctrl+V), or drag & drop `.sqlplan` files independent of any server connection, with tabbed multi-plan support ([#359])
- **Time display mode toggle** â€” show timestamps in Server Time, Local Time, or UTC with timezone labels across all grids and tooltips ([#17])
- **30 PlanAnalyzer rules** â€” expanded from 12 to 30 rules covering implicit conversions, GetRangeThroughConvert, lazy spools, OR expansion, exchange spills, RID lookups, and more ([#327], [#349], [#356], [#379])
- **Wait stats banner** in plan viewer showing top waits for the query ([#373])
- **UDF runtime details** â€” CPU and elapsed time shown in Runtime Summary pane when UDFs are present ([#382])
- **Sortable statement grid** and canvas panning in plan viewer ([#331])
- **Comma-separated column filters** â€” enter multiple values separated by commas in text filters ([#348])
- **Optional query text and plan collection** â€” per-collector flags in `config.collection_schedule` to disable query text or plan capture ([#337])
- **`--preserve-jobs` installer flag** â€” keep existing SQL Agent job schedules during upgrade ([#326])
- **Copy Query Text** context menu on Dashboard statements grid ([#367])
- **Server list sorting** by display name in both Dashboard and Lite ([#30])
- **Warning status icon** in server health indicators ([#355])
- Reserved threads and 10 missing ShowPlan XML attributes in plan viewer ([#378])
- Nightly build workflow for CI ([#332])

### Changed

- PlanAnalyzer warning messages rewritten to be actionable with expert-guided per-rule advice ([#370], [#371])
- PlanAnalyzer rule tuning: time-based spill analysis (Rule 7), lowered parallel skew thresholds (Rule 8), memory grant floor raised to 1GB/4GB (Rule 9), skip PROBE-only bitmap predicates (Rule 11) ([#341], [#342], [#343], [#358])
- First-run collector lookback reduced from 3-7 days to 1 hour for faster initial data ([#335])
- Plan canvas aligns top-left and resets scroll on statement switch ([#366])
- Plan viewer polish: index suggestions, property panel improvements, muted brush audit ([#365])
- Add Server dialog visual parity between Dashboard and Lite with theme-driven PasswordBox styling ([#289])

### Fixed

- **OverflowException** on wait stats page with large decimal values â€” SQL Server `decimal(38,24)` exceeding .NET precision ([#395])
- **SQL dumps** on mirroring passive servers with RESTORING databases ([#384])
- **UI hang** when adding first server to Dashboard ([#387])
- **UTC/local timezone mismatch** in blocked process XML processor ([#383])
- **AG secondary filter** skipping all inaccessible databases in cross-database collectors ([#325])
- DuckDB column aliases in long-running queries ([#391])
- sp_server_diagnostics and WAITFOR excluded from long-running query alerts ([#362])
- UDF timing units corrected: microseconds to milliseconds ([#338])
- DuckDB migration ordering after archive-and-reset ([#314])
- Int16 cast error in long-running query alerts ([#313])
- Missing dark mode on 19 SystemEventsContent charts ([#321])
- Missing tooltips on charts after theme changes ([#319])
- Operator time per-thread calculation synced across all plan viewers ([#392])
- Theme StaticResource/DynamicResource binding fix for runtime theme switching
- Memory grant MB display, missing index quality scoring, wildcard LIKE detection ([#393])
- **Installer validation** reporting historical collection errors as current failures â€” now filters to current run only ([#400])
- **query_snapshots schema mismatch** after sp_WhoIsActive upgrade â€” collector auto-recreates daily table when column order changes ([#401])
- **Missing upgrade script** for `default_trace_events` columns (`duration_us`, `end_time`) on 2.0.0â†’2.1.0 upgrade path ([#400])

## [2.0.0] - 2026-02-25

### Important

- **Schema upgrade**: The `collect.memory_grant_stats` table gains new delta columns and drops unused warning columns. The `collect.session_wait_stats` table, its collector procedure, reporting view, and schedule entry are removed (zero UI coverage). Upgrade scripts run automatically via the CLI/GUI installer and use idempotent checks.

### Added

- **Graphical query plan viewer** â€” native ShowPlan rendering in both Dashboard and Lite with SSMS-parity operator icons, properties panel, tooltips, warning/parallelism badges, and tabbed plan display ([#220])
- **Actual execution plan support** â€” execute queries with SET STATISTICS XML ON to capture actual plans, with loading indicator and confirmation dialog ([#233])
- **PlanAnalyzer** â€” automated plan analysis with rules for missing indexes, eager spools, key lookups, implicit conversions, memory grants, and more
- **Current Active Queries live snapshot** â€” real-time view of running queries with estimated/live plan download ([#149])
- **Memory clerks tab** in Lite with picker-driven chart ([#145])
- **Current Waits charts** in Blocking tab for both Dashboard and Lite ([#280])
- **File I/O throughput charts** â€” read/write throughput trends, file-level latency breakdown, queued I/O overlay ([#281])
- **Memory grant stats charts** â€” standardized collection with delta framework integration and trend visualization ([#281])
- **CPU scheduler pressure status** â€” real-time scheduler, worker, runnable task counts with color-coded pressure level below CPU chart
- **Collection log drill-down** and daily summary in Lite ([#138])
- **Collector duration trends chart** in Dashboard Collection Health ([#138])
- **Themed perfmon counter packs** â€” 14 new counters with organized themed groups ([#255])
- **User-configurable connection timeout** setting ([#236])
- **Per-collector retention** â€” uses per-collector retention from `config.collection_schedule` in data retention ([#237])
- **Query identifiers** in drill-down windows â€” query hash, plan hash, SQL handle visible for identification ([#268])
- **Trace pattern drill-down** with missing columns and query text tooltips ([#273])
- **Query Store Regressions drill-down** with TVF rewrite for performance ([#274])
- **CLI `--help` flag** for installer ([#111])
- Sort arrows, right-aligned numerics, and initial sort indicators across all grids ([#110])
- Copyable plan viewer properties ([#269])
- Standardized chart save/export filenames between Dashboard and Lite ([#284])
- Full Dashboard column parity for query_stats, procedure_stats, and query_store_stats
- Min/max extremes surfaced in both apps â€” physical reads, rows, grant KB, spills, CLR time, log bytes ([#281])

### Changed

- Query Store detection uses `sys.database_query_store_options` instead of `sys.databases.is_query_store_on` for Azure SQL DB compatibility ([#287])
- Config tab consolidation, DB drop on server remove, DuckDB-first plan lookups, procedure stats parity
- Collector health status now detects consecutive recent failures â€” 5+ consecutive errors = FAILING, 3+ = WARNING
- Plan buttons now show a MessageBox when no plan is available instead of silently doing nothing
- CSV export uses locale-appropriate separators for non-US locales ([#240])
- Query Store Regressions and Query Trace Patterns migrated to popup grid filtering ([#260])
- NuGet packages updated; xUnit v3 migration

### Fixed

- **DuckDB file corruption** during maintenance â€” ReaderWriterLockSlim coordination, archive-all-and-reset at 512MB replaces compaction ([#218])
- Archive view column mismatch, wait_stats thread-safety, and percent_complete type cast ([#234])
- Collector health status bar text color ([#234])
- View Plan for Query Store and Query Store Regressions tabs ([#261])
- Query Store drill-down time filter alignment with main view ([#263])
- Execution count mismatches between main views and drill-downs
- Drill-down chart UX â€” sparse data markers, hover tooltips, window sizing ([#271])
- Truncated status text in Add Server dialog ([#257])
- Scrollbar visibility, self-filtering artifacts, missing columns, and context menus ([#245], [#246], [#247], [#248])
- query_stats and procedure_stats collectors ignoring recent queries
- Blank tooltips on warning and parallel badge icons
- Missing chart context menu on File I/O Throughput charts in Lite

### Removed

- `collect.session_wait_stats` table, `collect.session_wait_stats_collector` procedure, `report.session_wait_analysis` view, and schedule entry â€” zero UI coverage, never surfaced in Dashboard or Lite ([#281])

## [1.3.0] - 2026-02-20

### Important

- **Schema upgrade**: The `collect.memory_stats` table gains two new columns (`total_physical_memory_mb`, `committed_target_memory_mb`). The upgrade script runs automatically via the CLI/GUI installer and uses `IF NOT EXISTS` checks, so it is safe to re-run. On servers with very large `memory_stats` tables this ALTER may take a moment.

### Added

- Physical Memory, SQL Server Memory, and Target Memory columns in Memory Overview ([#140])
- Current Configuration view (Server Config, Database Config, Trace Flags) in Dashboard Overview ([#143])
- Popup column filters and right-click context menus in all drill-down history windows ([#206])
- Consistent popup column filters across all Dashboard grids â€” replaced remaining TextBox-in-header filters and added filters to Trace Flags ([#200])
- 7-day time filter option in drill-down queries ([#165])
- Alert badge/count on sidebar Alerts button ([#109])
- Missing poison wait defaults in wait stats picker ([#188])

### Changed

- Default Trace tabs moved from Resource Metrics to Overview section ([#169])
- Trends tab shown first in Locking section ([#171])
- Wait stats cap raised from 20 to 30 (Dashboard) / 50 (Lite) so poison waits are never dropped ([#139])
- Settings time range dropdown now matches dashboard button options ([#210])
- "Total Executions" label in drill-down summaries renamed to clarify meaning ([#194])
- WAITFOR sessions excluded from long-running query alerts ([#151])

### Fixed

- Deadlock XML processor timezone mismatch â€” sp_BlitzLock returning 0 results because UTC dates were passed instead of local time
- Sidebar alert badge not updating when alerts dismissed from server sub-tabs ([#214])
- Sidebar alert badge not clearing on acknowledge ([#186])
- NOC deadlock/blocking showing "just now" for stale events instead of actual timestamp ([#187])
- NOC deadlock severity using extended events timestamp ([#170])
- Newly added servers not appearing on Overview until app restart ([#199])
- Double-click on column header incorrectly triggering row drill-down ([#195])
- Squished drill-down charts â€” now use proportional sizing ([#166])
- Unreliable chart tooltips â€” now use X-axis proximity matching ([#167])
- Query Trace Patterns showing empty despite data existing ([#168])
- Drill-down windows: removed inline plan XML, added time range filtering, aggregated by collection_time ([#189])
- Row clipping in Default Trace and Current Configuration grids ([#183], [#184])
- Numeric filter negative range parsing ([#113])
- MCP shutdown deadlock risk ([#112])
- Lite DBNull cast error in database_config collector on SQL 2016 Express ([#192])
- DuckDB concurrent file access IO errors ([#164])

## [1.2.0] - 2026-02-15

### Added

- Alert types, alerts history view, column filtering, and dismiss/hide for alerts ([#52], [#56])
- Average ms per wait chart toggle in both apps ([#22])
- Collection Health tab in Lite UI ([#39])
- Collector performance diagnostics in Lite UI ([#40])
- Hover tooltips on all Dashboard charts ([#70])
- Minimize-to-tray setting added to Lite ([#53])
- Persist dismissed alerts across app restarts ([#44])
- Locale-aware date/time formatting throughout UI ([#41])
- 24-hour format in time range picker ([#41])
- CI pipelines for build validation, SQL install testing, and DuckDB schema tests
- Expanded Lite database config collector to 28 sys.databases columns ([#142])
- Parquet archive visibility and scheduled DuckDB database compaction ([#160], [#161])
- DuckDB checkpoint optimization and collection timing accuracy
- Installer `--reset-schedule` flag to reset collection schedule on re-install

### Fixed

- Deadlock charts not populating data ([#73])
- Chart X-axis double-converting custom range to server time ([#49])
- query_cost overflow in memory grant collector ([#47])
- XE ring buffer query timeouts on large buffers ([#37])
- Dashboard sub-tab badge state and DuckDB migration for dismissed column
- Lite duplicate blocking/deadlock events from missing WHERE clause ([#61])
- Procedure_stats_collector truncation on DDL triggers ([#69])
- DataGrid row height increased from 25 to 28 to fix text clipping
- Skip offline servers during Lite collection and reduce connection timeout ([#90])
- Mutex crash on Lite app exit ([#89])
- Permission denied errors handled gracefully in collector health ([#150])

## [1.1.0] - 2026-02-13

### Added

- Hover tooltips on all multi-series charts â€” Wait Stats, Sessions, Latch Stats, Spinlock Stats, File I/O, Perfmon, TempDB ([#21])
- Microsoft Entra MFA authentication for Azure SQL DB connections in Lite ([#20])
- Column-level filtering on all 11 Lite DataGrids ([#18])
- Chart visual parity â€” Material Design 300 color palette, data point markers, consistent grid styling ([#16])
- Smart Select All for wait types + expand from 12 to 20 wait types ([#12])
- Trend chart legends always visible in Dashboard ([#11])
- Per-server collector health in Lite status bar ([#5])
- Server Online/Offline status in Lite overview ([#2])
- Check for updates feature in both apps ([#1])
- High DPI support for both Dashboard and Lite

### Fixed

- Query text off-by-one truncation ([#25])
- Blocking/deadlock XML processors truncating parsed data every run ([#23])
- WAITFOR queries appearing in top queries views ([#4])
- Wait type Clear All not refreshing search filter in Dashboard

## [1.0.0] - 2026-02-11

### Added

- Full Edition: Dashboard + CLI/GUI Installer with 30+ automated SQL Agent collectors
- Lite Edition: Agentless monitoring with local DuckDB storage
- Support for SQL Server 2016-2025, Azure SQL DB, Azure SQL MI, AWS RDS
- Real-time charts and trend analysis for wait stats, CPU, memory, query performance, index usage, file I/O, blocking, deadlocks
- Email alerts for blocking, deadlocks, and high CPU
- MCP server integration for AI-assisted analysis
- System tray operation with background collection and alert notifications
- Data retention with configurable automatic cleanup
- Delta normalization for per-second rate calculations
- Dark theme UI

[2.1.0]: https://github.com/erikdarlingdata/PerformanceMonitor/compare/v2.0.0...v2.1.0
[2.0.0]: https://github.com/erikdarlingdata/PerformanceMonitor/compare/v1.3.0...v2.0.0
[1.3.0]: https://github.com/erikdarlingdata/PerformanceMonitor/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/erikdarlingdata/PerformanceMonitor/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/erikdarlingdata/PerformanceMonitor/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/erikdarlingdata/PerformanceMonitor/releases/tag/v1.0.0
[#1]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1
[#2]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2
[#4]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/4
[#5]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/5
[#11]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/11
[#12]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/12
[#16]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/16
[#18]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/18
[#20]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/20
[#21]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/21
[#22]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/22
[#23]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/23
[#25]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/25
[#37]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/37
[#39]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/39
[#40]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/40
[#41]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/41
[#44]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/44
[#47]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/47
[#49]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/49
[#52]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/52
[#53]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/53
[#56]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/56
[#61]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/61
[#69]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/69
[#70]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/70
[#73]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/73
[#85]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/85
[#86]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/86
[#89]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/89
[#90]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/90
[#109]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/109
[#112]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/112
[#113]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/113
[#139]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/139
[#140]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/140
[#142]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/142
[#143]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/143
[#150]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/150
[#151]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/151
[#160]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/160
[#161]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/161
[#164]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/164
[#165]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/165
[#166]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/166
[#167]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/167
[#168]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/168
[#169]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/169
[#170]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/170
[#171]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/171
[#1724]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1724
[#1735]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1735
[#1732]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1732
[#183]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/183
[#184]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/184
[#186]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/186
[#187]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/187
[#188]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/188
[#189]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/189
[#192]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/192
[#194]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/194
[#195]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/195
[#199]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/199
[#200]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/200
[#206]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/206
[#210]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/210
[#214]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/214
[#218]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/218
[#220]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/220
[#233]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/233
[#234]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/234
[#236]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/236
[#237]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/237
[#240]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/240
[#245]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/245
[#246]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/246
[#247]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/247
[#248]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/248
[#255]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/255
[#257]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/257
[#260]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/260
[#261]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/261
[#263]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/263
[#268]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/268
[#269]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/269
[#271]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/271
[#273]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/273
[#274]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/274
[#280]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/280
[#281]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/281
[#284]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/284
[#287]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/287
[#313]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/313
[#314]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/314
[#17]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/17
[#30]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/30
[#319]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/319
[#321]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/321
[#325]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/325
[#326]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/326
[#327]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/327
[#331]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/331
[#332]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/332
[#335]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/335
[#337]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/337
[#338]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/338
[#341]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/341
[#342]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/342
[#343]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/343
[#347]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/347
[#348]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/348
[#349]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/349
[#355]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/355
[#356]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/356
[#358]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/358
[#359]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/359
[#362]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/362
[#365]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/365
[#366]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/366
[#367]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/367
[#370]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/370
[#371]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/371
[#373]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/373
[#378]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/378
[#379]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/379
[#382]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/382
[#383]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/383
[#384]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/384
[#387]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/387
[#391]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/391
[#392]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/392
[#393]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/393
[#289]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/289
[#395]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/395
[#400]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/400
[#401]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/401
[#410]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/410
[#412]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/412
[#414]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/414
[#415]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/415
[#416]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/416
[#419]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/419
[#420]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/420
[#421]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/421
[#422]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/422
[#429]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/429
[#430]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/430
[#431]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/431
[#445]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/445
[#448]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/448
[#453]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/453
[#454]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/454
[#459]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/459
[#474]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/474
[#475]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/475
[#477]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/477
[#478]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/478
[#480]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/480
[#481]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/481
[#482]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/482
[#488]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/488
[#489]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/489
[#503]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/503
[#504]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/504
[#508]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/508
[#510]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/510
[#512]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/512
[#511]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/511
[#513]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/513
[#515]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/515
[#516]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/516
[#517]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/517
[#518]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/518
[#521]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/521
[#1643]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1643
[#1649]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1649
[#1650]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1650
[#1591]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1591
[#1661]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1661
[#1667]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1667
[#1666]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1666
[#1682]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1682
[#1681]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1681
[#1680]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1680
[#1688]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1688
[#1691]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1691
[#1692]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1692
[#1695]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1695
[#1699]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1699
[#1704]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1704
[#1707]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1707
[#1713]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1713
[#1711]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1711
[#1719]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1719
[#1722]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1722
[#1731]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1731
[#1709]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1709
[#1702]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1702
[#1697]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1697
[#1700]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1700
[#1703]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1703
[#1708]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1708
[#1712]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1712
[#1715]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1715
[#1716]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1716
[#1710]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1710
[#1726]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1726
[#1734]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1734
[#1750]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1750
[#1725]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1725
[#1730]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1730
[#1739]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1739
[#1744]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1744
[#1737]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1737
[#1738]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1738
[#1752]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1752
[#1751]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1751
[#1749]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1749
[#1753]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1753
[#1756]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1756
[#1748]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1748
[#1747]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1747
[#1727]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1727
[#1690]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1690
[#1693]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1693
[#1694]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1694
[#1698]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1698
[#1701]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1701
[#1705]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1705
[#1718]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1718
[#1729]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1729
[#1762]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1762
[#1757]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1757
[#1759]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1759
[#1760]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1760
[#1769]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1769
[#1776]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1776
[#1767]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1767
[#1768]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1768
[#1770]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1770
[#1771]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1771
[#1772]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1772
[#1773]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1773
[#1774]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1774
[#1775]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1775
[#1785]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1785
[#1777]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1777
[#1780]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1780
[#1665]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1665
[#1799]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1799
[#1798]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1798
[#1821]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1821
[#1797]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1797
[#1788]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1788
[#1781]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1781
[#1782]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1782
[#1783]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1783
[#1784]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1784
[#1786]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1786
[#1789]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1789
[#1792]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1792
[#1793]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1793
[#1796]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1796
[#1778]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1778
[#1779]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1779
[#1801]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1801
[#1803]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1803
[#1805]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1805
[#1808]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1808
[#1816]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1816
[#1818]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1818
[#1815]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1815
[#1817]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1817
[#1812]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1812
[#1814]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1814
[#1795]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1795
[#1813]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1813
[#1809]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1809
[#1811]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1811
[#1794]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1794
[#1810]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1810
[#1787]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1787
[#1806]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1806
[#1758]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1758
[#1807]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1807
[#1802]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1802
[#1823]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1823
[#1828]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1828
[#1831]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1831
[#1833]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1833
[#1836]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1836
[#1837]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1837
[#1830]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1830
[#1839]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1839
[#1832]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1832
[#1841]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1841
[#1849]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1849
[#1850]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1850
[#1851]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1851
[#1852]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1852
[#1855]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1855
[#1847]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1847
[#1856]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1856
[#1857]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1857
[#1858]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1858
[#1859]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1859
[#1862]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1862
[#1863]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1863
[#1867]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1867
[#1869]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1869
[#1846]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1846
[#1881]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1881
[#1882]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1882

[#1860]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1860
[#1878]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1878

[#1844]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1844
[#1848]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1848
[#1871]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1871
[#1872]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1872

[#110]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/110
[#111]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/111
[#138]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/138
[#145]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/145
[#149]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/149
[#346]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/346
[#372]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/372
[#1074]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1074
[#1562]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1562
[#1563]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1563
[#1568]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1568
[#1606]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1606
[#1663]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1663
[#1673]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1673
[#1685]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1685
[#1687]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1687
[#1696]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1696
[#1721]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1721
[#1743]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1743
[#1754]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1754
[#1755]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1755
[#1804]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1804
[#1822]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1822
[#1824]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1824
[#1826]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1826
[#1829]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1829
[#1834]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1834
[#1840]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1840
[#1842]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1842
[#1845]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1845
[#1853]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1853
[#1854]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1854
[#1865]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1865
[#1873]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1873
[#1874]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1874
[#1875]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1875
[#1876]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1876
[#1877]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1877
[#1879]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1879
[#1886]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1886
[#1888]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1888
[#1889]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1889
[#1891]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1891
[#1892]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1892
[#1893]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1893
[#1895]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1895
[#1896]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1896
[#1898]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1898
[#1899]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1899
[#1902]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1902
[#1905]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1905
[#1906]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1906
[#1907]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1907
[#1911]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1911
[#1912]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1912
[#1913]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1913
[#1914]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1914
[#1915]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1915
[#1921]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1921
[#1922]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1922
[#1923]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1923
[#1925]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1925
[#1934]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1934
[#1937]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1937
[#1939]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1939
[#1940]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1940
[#1942]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1942
[#1943]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1943
[#1944]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1944
[#1945]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1945
[#1949]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1949
[#1951]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1951
[#1952]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1952
[#1953]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1953
[#1954]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1954
[#1955]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1955
[#1957]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1957
[#1958]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1958
[#1959]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1959
[#1960]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1960
[#1962]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1962
[#1963]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1963
[#1965]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1965
[#1966]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1966
[#1970]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1970
[#1972]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1972
[#1973]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1973
[#1980]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1980
[#1981]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1981
[#1984]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1984
[#1986]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1986
[#1988]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1988
[#1990]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1990
[#1995]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1995
[#1996]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1996
[#2000]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2000
[#2005]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2005
[#2007]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2007
[#2008]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2008
[#2011]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2011
[#2012]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2012
[#2018]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2018
[#2020]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2020
[#2022]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2022
[#2026]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2026
[#2027]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2027
[#2028]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2028
[#2029]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2029
[#2030]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2030
[#2031]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2031
[#2033]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2033
[#2038]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2038
[#2054]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2054
[#2058]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2058
[#2060]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2060
[#2061]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2061
[#2063]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2063
[#2064]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2064
[#2068]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2068
[#2069]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2069
[#2071]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2071
[#2073]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2073
[#2076]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2076
[#2087]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2087
[#2090]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2090
[#2093]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2093
[#2097]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2097
[#2101]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2101
[#2102]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2102
[#2105]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2105
[#2107]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2107
[#2108]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2108
[#2109]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2109
[#2111]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2111
[#2113]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2113
[#2114]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2114
[#2117]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2117
[#2119]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2119
[#2126]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2126
[#2129]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2129
[#2133]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2133
[#2136]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2136
[#2138]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2138
[#2143]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2143
[#2148]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2148
[#2150]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2150
[#2154]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2154
[#2157]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2157
[#2158]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2158
[#2159]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2159
[#2164]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2164
[#2165]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2165
[#2166]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2166
[#2167]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2167
[#2169]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2169
[#2170]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2170
[#2171]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2171
[#2181]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2181
[#2184]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2184
[#2185]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2185
[#2186]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2186
[#2187]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2187
[#2188]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2188
[#2189]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2189
[#2190]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2190
[#2191]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2191
[#2195]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2195
[#2197]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2197
[#2201]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2201
[#2203]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2203
[#2205]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2205
[#2210]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2210
[#2213]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2213
[#2216]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2216
[#2218]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2218
[#2219]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2219
[#2220]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2220
[#2228]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2228
[#2233]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2233
[#2234]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2234
[#2235]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2235
[#2246]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2246
[#2252]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2252
[#2254]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2254
[#2255]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2255
[#2256]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2256
[#2258]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2258
[#2266]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2266
[#2273]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2273
[#2277]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2277
[#2279]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2279
[#2280]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2280
[#2284]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2284
[#2293]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2293
[#2294]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2294
[#2296]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2296
[#2298]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2298
[#2299]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2299
[#2300]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2300
[#2302]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2302
[#2306]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2306
[#2312]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2312
[#2316]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2316
[#2317]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2317
[#2319]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2319
[#2320]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2320
[#2324]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2324
[#2331]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2331
[#2339]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2339
[#2340]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2340
[#2344]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2344
[#2347]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2347
[#2348]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2348
[#2349]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2349
[#2350]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2350
[#2352]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2352
[#2353]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2353
[#2357]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2357
[#2359]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2359
[#2361]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2361
[#2362]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2362
[#2364]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2364
[#2422]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2422
[#2437]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2437
[#2446]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2446
[#2459]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2459
[#2460]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2460
[#2465]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2465
[#2473]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2473
[#2475]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2475
[#2480]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2480
[#2481]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2481
[#2484]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2484
[#2485]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2485
[#2489]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2489
[#2490]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2490
[#2495]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2495
[#2499]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2499
[#2501]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2501
[#2506]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2506
[#2508]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2508
[#2511]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2511
[#2512]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2512
[#2515]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2515
[#2518]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2518
[#2520]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2520
[#2524]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2524
[#2525]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2525
[#2529]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2529
[#2530]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2530
[#2532]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2532
[#2533]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2533
[#2535]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2535
[#2538]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2538
[#2539]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2539
[#2540]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2540
[#2541]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2541
[#2542]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2542
[#2543]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2543
[#2544]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2544
[#2545]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2545
[#2546]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2546
[#2547]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2547
[#2548]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2548
[#2550]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2550
[#2552]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2552
[#2554]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2554
[#2559]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2559
[#2561]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2561
[#2562]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2562
[#2564]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2564
[#2565]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2565
[#2566]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2566
[#2567]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2567
[#2569]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2569
[#2572]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2572
[#2574]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2574
[#2578]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2578
[#2579]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2579
[#2593]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2593
[#2595]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2595
[#2599]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2599
[#2603]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2603
[#2612]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2612
[#2617]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2617
[#2623]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2623
[#2625]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2625
[#2626]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2626
[#2629]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2629
[#2630]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2630
[#2633]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2633
[#2636]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2636
[#2638]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2638
[#2640]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2640
[#2641]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2641
[#2643]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2643
[#2645]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2645
[#2651]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2651
[#2653]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2653
[#2655]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2655
[#2658]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2658
[#2659]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2659
[#2661]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2661
[#2663]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2663
[#2665]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2665
[#2671]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2671
[#2673]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2673
[#2674]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2674
[#2675]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2675
[#2677]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2677
[#2680]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2680
[#2681]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2681
[#2683]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2683
[#2685]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2685
[#2686]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2686
[#2687]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2687
[#2688]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2688
[#2689]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2689
[#2690]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2690
[#2691]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2691
[#2692]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2692
[#2693]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2693
[#2694]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2694
[#2695]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2695
[#2699]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2699
[#2710]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2710
[#2711]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2711
[#2715]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2715
[#2719]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2719
[#2732]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2732
[#2733]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2733
[#2734]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2734
[#2735]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2735
[#2736]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2736
[#2737]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2737
[#2748]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2748
[#2749]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2749
[#2753]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2753
[#2755]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2755
[#2757]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2757
[#2759]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2759
[#2760]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2760
[#2761]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2761
[#2764]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2764
[#2766]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2766
[#2768]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2768
[#2770]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2770
[#2771]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2771
[#2772]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2772
[#2773]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2773
[#2776]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2776
[#2779]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2779
[#2780]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2780
[#2781]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2781
[#2784]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2784
[#2785]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2785
[#2786]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2786
[#2791]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2791
[#2792]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2792
[#2794]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2794
[#2795]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2795
[#2796]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2796
[#2797]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2797
[#2800]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2800
[#2801]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2801
[#2803]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2803
[#2804]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2804
[#2809]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2809
[#2810]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2810
[#2811]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2811
[#2813]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2813
[#2816]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2816
[#2819]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2819
[#2822]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2822
[#2823]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2823
[#2826]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2826
[#2827]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2827
[#2828]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2828
[#2837]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2837
[#2839]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2839
[#2840]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2840
[#2841]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2841
[#2845]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2845
[#2846]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2846
[#2847]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2847
[#2851]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2851
[#2854]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2854
[#2855]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2855
[#2859]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2859
[#2860]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2860
[#2864]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2864
[#2870]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2870
[#2871]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2871
[#2874]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2874
[#2876]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2876
[#2880]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2880
[#2882]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2882
[#2884]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2884
[#2888]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2888
[#2890]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2890
[#2894]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2894
[#2896]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2896
[#2898]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2898
[#2901]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2901
[#2902]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2902
[#2907]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2907
[#2910]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2910
[#2913]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2913
[#2917]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2917
[#2918]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2918
[#2920]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2920
[#2923]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2923
[#2924]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2924
[#2926]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2926
[#2927]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2927
[#2928]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2928
[#2929]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2929
[#2931]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2931
[#2932]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2932
[#2933]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2933
[#2935]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2935
[#2936]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2936
[#2937]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2937
[#2938]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2938
[#2940]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2940
[#2942]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2942
[#2951]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2951
[#2952]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2952
[#2953]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2953
[#2960]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2960
[#2964]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2964
[#2966]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2966
[#2967]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2967
[#2972]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2972
[#2973]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2973
[#2997]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2997
[#3008]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3008
[#3009]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3009
[#3010]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3010
[#3012]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3012
[#3013]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3013
[#3014]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3014
[#3016]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3016
[#3017]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3017
[#3018]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3018
[#3019]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3019
[#3021]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3021
[#3022]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3022
[#3025]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3025
[#3026]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3026
[#3027]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3027
[#3030]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3030
[#3031]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3031
[#3035]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3035
[#3036]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3036
[#3038]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3038
[#3042]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3042
[#3044]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3044
[#3045]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3045
[#3047]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3047
[#3048]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3048
[#3049]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3049
[#3050]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3050
[#3052]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3052
[#3053]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3053
[#3060]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3060
[#3061]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3061
[#3062]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3062
[#3063]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3063
[#3065]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3065
[#3067]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3067
[#3069]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3069
[#3070]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3070
[#3072]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3072
[#3074]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3074
[#3076]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3076
[#3081]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3081
[#3082]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3082
[#3086]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3086
[#3087]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3087
[#3088]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3088
[#3090]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3090
[#3094]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3094
[#3095]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3095
[#3098]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3098
[#3099]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3099
[#3101]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3101
[#3102]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3102
[#3104]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3104
[#3107]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3107
[#3109]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3109
[#3111]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3111
[#3112]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3112
[#3116]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3116
[#3118]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3118
[#3119]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3119
[#3121]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3121
[#3129]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3129
[#3132]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3132
[#3133]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3133
[#3134]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3134
[#3138]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3138
[#3139]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3139
[#3140]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3140
[#3143]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3143
[#3144]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3144
[#3145]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3145
[#3153]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3153
[#3154]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3154
[#3156]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3156
[#3158]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3158
[#3160]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3160
[#3161]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3161
[#3164]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3164
[#3165]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3165
[#3166]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3166
[#3168]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3168
[#3169]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3169
[#3174]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3174
[#3175]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3175
[#3181]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3181
[#3182]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3182
[#3184]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3184
[#3185]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3185
[#3187]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3187
[#3188]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3188
[#3189]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3189
[#3192]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3192
[#3193]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3193
[#3196]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3196
[#3198]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3198
[#3199]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3199
[#3200]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3200
[#3204]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3204
[#3206]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3206
[#3207]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3207
[#3208]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3208
[#3211]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3211
[#3214]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3214
[#3217]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3217
[#3219]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3219
[#3221]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3221
[#3222]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3222
[#3223]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3223
[#3224]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3224
[#3226]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3226
[#3230]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3230
[#3231]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3231
[#3237]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3237
[#3238]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3238
[#3239]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3239
[#3240]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3240
[#3241]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3241
[#3243]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3243
[#3244]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3244
[#3245]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3245
[#3247]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3247
[#3248]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3248
[#3253]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3253
[#3261]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3261
[#3262]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3262
[#3267]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3267
[#3271]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3271
[#3278]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3278
[#3281]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3281
[#3282]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3282
[#3285]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3285
[#3287]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3287
[#3288]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3288
[#3297]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3297
[#3302]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3302
[#3306]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3306
[#3307]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3307
[#3313]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3313
[#3314]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3314
[#3315]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3315
[#3316]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3316
[#3330]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3330
[#3343]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3343
[#3348]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3348
[#3354]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3354
[#3355]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3355
[#3368]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3368
[#3446]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3446
[#3464]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3464
[#3466]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3466
[#3467]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3467
[#3469]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3469
[#3474]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3474
[#3476]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3476
[#3478]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3478
[#3482]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3482
[#3484]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3484
[#3485]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3485
[#3487]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3487
[#3493]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3493
[#3496]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3496
[#3499]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3499
[#3886]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3886
[#3889]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3889
[#3900]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3900
[#3906]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3906
[#3908]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3908
[#3909]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3909
[#3911]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3911
[#3918]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3918
[#3919]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3919
[#3920]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3920
[#3927]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3927
[#3931]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3931
[#3932]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3932
[#3940]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3940
[#3942]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3942
[#3946]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3946
[#3947]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3947
[#3950]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3950
[#3952]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3952
[#3953]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3953
[#3955]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3955
[#3956]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3956
[#3957]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3957
[#3964]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3964
[#3965]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3965
[#3966]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3966
[#3968]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3968
[#3972]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3972
[#3975]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3975
[#3979]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3979
[#3980]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3980
[#3981]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3981
[#3983]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3983
[#3984]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3984
[#3985]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3985
[#3992]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3992
[#3995]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3995
[#3996]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3996
[#3998]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3998
[#4001]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4001
[#4002]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4002
[#4003]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4003
[#4007]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4007
[#4010]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4010
[#4011]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4011
[#4013]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4013
[#4015]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4015
[#4016]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4016
[#4020]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4020
[#4022]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4022
[#4025]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4025
[#4029]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4029
[#4030]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4030
[#4031]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4031
[#4032]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4032
[#4036]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4036
[#4038]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4038
[#4039]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4039
[#4040]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4040
[#4042]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4042
[#4044]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4044
[#4047]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4047
[#4048]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4048
[#4049]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4049
[#4050]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4050
[#4051]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4051
[#4055]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4055
[#4057]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4057
[#4061]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4061
[#4063]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4063
[#4064]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4064
[#4065]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4065
[#4066]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4066
[#4067]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4067
[#4068]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4068
[#4069]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4069
[#4070]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4070
[#4071]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4071
[#4073]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4073
[#4074]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4074
[#4077]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4077
[#4078]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4078
[#4079]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4079
[#4081]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4081
[#4082]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4082
[#4083]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4083
[#4084]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4084
[#4085]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4085
[#4086]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4086
[#4087]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4087
[#4088]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4088
[#4089]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4089
[#4090]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4090
[#4091]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4091
[#4092]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4092
[#4093]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4093
[#4095]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4095
[#4096]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4096
[#4099]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4099
[#4100]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4100
[#4101]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4101
[#4103]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4103
[#4105]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4105
[#4106]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4106
[#4107]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4107
[#4108]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4108
[#4109]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4109
[#4110]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4110
[#4111]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4111
[#4113]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4113
[#4114]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4114
[#4115]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4115
[#4116]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4116
[#4117]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4117
[#4118]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4118
[#4119]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4119
[#4120]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4120
[#4121]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4121
[#4122]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4122
[#4123]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4123
[#4124]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4124
[#4125]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4125
[#4126]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4126
[#4127]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4127
[#4131]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4131
[#4133]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4133
[#4135]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4135
[#4136]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4136
[#4137]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4137
[#4141]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4141
[#4157]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4157
[#4158]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4158
[#4159]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4159
[#4186]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4186
[#4198]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/4198
[#4203]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/4203
[#4206]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4206
[#4208]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4208
[#4228]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4228
[#4230]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4230
[#4252]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4252
[#4254]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4254
[#4256]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4256
[#4258]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4258
[#4259]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4259
[#4260]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4260
[#4261]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4261
[#4263]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4263
[#4271]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4271
[#4278]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4278
[#4279]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4279
[#4280]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4280
[#4281]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4281
[#4282]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4282
[#4285]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4285
[#4286]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4286
[#4287]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4287
[#4288]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4288
[#4291]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4291
[#4292]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4292
[#4293]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4293
[#4294]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4294
[#4295]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4295
[#4297]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4297
[#4302]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4302
[#4303]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4303
[#4304]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4304
[#4306]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4306
[#4307]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4307
[#4308]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4308
[#4309]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4309
[#4311]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4311
[#4317]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4317
[#4319]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4319
[#4321]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4321
[#4324]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4324
[#4325]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4325
[#4326]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4326
[#4327]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4327
[#4328]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4328
[#4329]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4329
[#4330]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4330
[#4331]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4331
[#4333]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4333
[#4336]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4336
[#4337]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4337
[#4338]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4338
[#4339]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4339
[#4340]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4340
[#4341]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4341
[#4342]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4342
[#4344]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4344
[#4345]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4345
[#4347]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4347
[#4350]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4350
[#4351]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4351
[#4352]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4352
[#4353]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4353
[#4357]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4357
[#4358]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/4358
[#4359]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4359
[#4361]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4361
[#4362]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4362
[#4363]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4363
[#4364]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4364
[#4365]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4365
[#4366]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4366
[#4367]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4367
[#4368]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4368
[#4370]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4370
[#4371]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4371
[#4372]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4372
[#4373]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4373
[#4380]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4380
[#4382]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4382
[#4383]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4383
[#4384]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4384
[#4386]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4386
[#4387]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4387
[#4388]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4388
[#4390]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4390
[#4391]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4391
[#4392]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4392
[#4393]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4393
[#4395]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4395
[#4396]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4396
[#4399]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4399
[#4400]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4400
[#4401]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4401
[#4402]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4402
[#4405]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4405
[#4406]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4406
[#4407]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4407
[#4408]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4408
[#4409]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4409
[#4411]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4411
[#4412]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4412
[#4413]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4413
[#4416]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4416
[#4417]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4417
[#4418]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4418
[#4419]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4419
[#4420]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4420
[#4421]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4421
[#4422]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4422
[#4423]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4423
[#4424]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4424
[#4426]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4426
[#4429]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4429
[#4430]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4430
[#4431]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4431
[#4432]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4432
[#4433]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4433
[#4434]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4434
[#4435]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4435
[#4436]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4436
[#4437]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4437
[#4439]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4439
[#4444]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4444
[#4447]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4447
[#4454]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4454
[#4455]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4455
[#4456]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4456
[#4458]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4458
[#4464]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4464
[#4465]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4465
[#4467]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4467
[#4470]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4470
[#4472]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4472
[#4474]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4474
[#4480]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4480
[#4481]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4481
[#4482]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4482
[#4483]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4483
[#4486]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4486
[#4488]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4488
[#4489]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4489
[#4490]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4490
[#4492]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4492
[#4493]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4493
[#4494]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4494
[#4495]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4495
[#4496]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4496
[#4501]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4501
[#4502]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4502
[#4506]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4506
[#4509]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4509
[#4538]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4538
[#4540]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4540
[#4541]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4541
[#4542]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4542
[#4543]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4543
[#4544]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4544
[#4545]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4545
[#4547]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4547
[#4548]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4548
[#4549]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4549
[#4550]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4550
[#4551]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4551
[#4552]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4552
[#4553]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4553
[#4554]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4554
[#4555]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4555
[#4556]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4556
[#4557]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4557
[#4559]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4559
[#4560]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4560
[#4561]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4561
[#4562]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4562
[#4563]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4563
[#4565]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4565
[#4567]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4567
[#4568]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4568
[#4583]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4583
[#4585]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4585
[#4586]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4586
[#4587]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4587
[#4588]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4588
[#4590]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4590
[#4591]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4591
[#4592]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4592
[#4593]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4593
[#4595]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4595
[#4596]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4596
[#4599]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4599
[#4600]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4600
[#4601]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4601
[#4602]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4602
[#4603]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4603
[#4604]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4604
[#4610]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4610
[#4612]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4612
[#4614]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4614
[#4615]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4615
[#4616]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4616
[#4617]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4617
[#4618]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4618
[#4625]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4625
[#4626]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4626
[#4632]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4632
[#4633]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4633
[#4634]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4634
[#4643]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4643
[#4644]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4644
[#4646]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4646
[#4647]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4647
[#4649]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4649
[#4656]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4656
[#4658]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4658
[#4664]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4664
[#4665]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4665
[#4667]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4667
[#4669]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4669
[#4671]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4671
[#4680]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4680
[#4682]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4682
[#4685]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4685
[#4688]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4688
[#4698]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4698
[#4700]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4700
[#4703]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4703
[#4704]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4704
[#4705]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4705
[#4712]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4712
[#4713]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4713
[#4714]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4714
[#4717]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4717
[#4718]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4718
[#4725]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4725
[#4738]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4738
[#4739]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4739
[#4740]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4740
[#4742]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4742
[#4753]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4753
[#4762]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4762
[#4763]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4763
[#4775]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4775
[#4777]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4777
[#4778]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4778
[#4779]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4779
[#4780]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4780
[#4781]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4781
[#4783]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4783
[#4784]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4784
[#4786]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4786
[#4787]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4787
[#4790]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4790
[#4791]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4791
[#4792]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4792
[#4794]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4794
[#4796]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4796
[#4797]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4797
[#4798]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4798
[#4799]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4799
[#4801]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4801
[#4802]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4802
[#4803]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4803
[#4804]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4804
[#4805]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4805
[#4806]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4806
[#4808]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4808
[#4809]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4809
[#4810]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4810
[#4811]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4811
[#4813]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4813
[#4814]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4814
[#4816]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4816
[#4818]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4818
[#4820]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4820
[#4826]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4826
[#4827]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4827
[#4828]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4828
[#4829]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4829
[#4830]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4830
[#4831]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4831
[#4832]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4832
[#4833]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4833
[#4835]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4835
[#4837]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4837
[#4838]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4838
[#4839]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4839
[#4840]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4840
[#4841]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4841
[#4846]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4846
[#4847]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4847
[#4848]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4848
[#4849]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4849
[#4850]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4850
[#4851]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4851
[#4852]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4852
[#4853]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4853
[#4854]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4854
[#4855]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4855
[#4858]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4858
[#4859]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4859
[#4861]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4861
[#4863]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4863
[#4867]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4867
[#4870]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4870
[#4871]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4871
[#4872]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4872
[#4873]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4873
[#4874]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4874
[#4875]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4875
[#4876]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4876
[#4877]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4877
[#4878]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4878
[#4879]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4879
[#4880]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4880
[#4881]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4881
[#4882]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4882
[#4883]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4883
[#4884]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4884
[#4886]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4886
[#4887]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4887
[#4888]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4888
[#4889]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4889
[#4890]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4890
[#4892]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4892
[#4893]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4893
[#4894]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4894
[#4896]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4896
[#4898]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4898
[#4899]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4899
[#4900]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4900
[#4901]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4901
[#4902]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4902
[#4903]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4903
[#4904]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4904
[#4905]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4905
[#4906]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4906
[#4907]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4907
[#4908]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4908
[#4909]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4909
[#4910]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4910
[#4912]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4912
[#4913]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4913
[#4914]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4914
[#4915]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4915
[#4917]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4917
[#4918]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4918
[#4920]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4920
[#4921]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4921
[#4923]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4923
[#4925]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4925
[#4926]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4926
[#4927]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4927
[#4928]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4928
[#4929]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4929
[#4930]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4930
[#4931]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4931
[#4932]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4932
[#4934]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4934
[#4935]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4935
[#4939]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4939
[#4941]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4941
[#4944]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4944
[#4945]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4945
[#4946]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4946
[#4947]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4947
[#4951]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4951
[#4952]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4952
[#4953]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4953
[#4960]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4960
[#4963]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4963
[#4966]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4966
[#4967]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4967
[#4968]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4968
[#4972]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4972
[#4975]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4975
[#4976]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4976
[#4978]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4978
[#4983]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4983
[#4984]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4984
[#4985]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4985
[#4988]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4988
[#4989]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4989
[#4990]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4990
[#4991]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4991
[#4993]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4993
[#4994]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4994
[#4996]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4996
[#4997]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4997
[#4998]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4998
[#4999]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4999
[#5000]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5000
[#5001]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5001
[#5004]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5004
[#5006]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5006
[#5007]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5007
[#5009]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5009
[#5010]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5010
[#5015]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5015
[#5016]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5016
[#5018]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5018
[#5019]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5019
[#5021]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5021
[#5022]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5022
[#5023]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5023
[#5024]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5024
[#5025]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5025
[#5026]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5026
[#5028]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5028
[#5029]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5029
[#5030]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5030
[#5031]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5031
[#5032]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5032
[#5035]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5035
[#5038]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5038
[#5040]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5040
[#5041]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5041
[#5044]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5044
[#5045]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5045
[#5047]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5047
[#5049]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5049
[#5051]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5051
[#5052]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5052
[#5055]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5055
[#5056]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5056
[#5057]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5057
[#5058]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5058
[#5059]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5059
[#5060]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5060
[#5062]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5062
[#5064]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5064
[#5066]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5066
[#5067]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5067
[#5074]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5074
[#5075]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5075
[#5076]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5076
[#5078]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5078
[#5079]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5079
[#5080]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5080
[#5083]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5083
[#5084]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5084
[#5086]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5086
[#5087]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5087
[#5088]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5088
[#5089]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5089
[#5090]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5090
[#5091]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5091
[#5092]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5092
[#5093]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5093
[#5095]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5095
[#5096]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5096
[#5099]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5099
[#5102]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5102
[#5105]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5105
[#5107]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5107
[#5108]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5108
[#5109]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5109
[#5110]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5110
[#5111]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5111
[#5112]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5112
[#5113]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5113
[#5116]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5116
[#5117]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5117
[#5118]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5118
[#5119]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5119
[#5120]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5120
[#5121]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5121
[#5122]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5122
[#5123]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5123
[#5127]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5127
[#5136]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5136
[#5137]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5137
[#5138]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5138
[#5139]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5139
[#5141]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5141
[#5144]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5144
[#5147]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5147
[#5149]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5149
[#5150]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5150
[#5151]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5151
[#5152]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5152
[#5154]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5154
[#5155]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5155
[#5156]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5156
[#5159]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5159
[#5161]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5161
[#5162]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5162
[#5163]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5163
[#5164]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5164
[#5165]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5165
[#5166]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5166
[#5167]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5167
[#5168]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5168
[#5169]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5169
[#5170]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5170
[#5171]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5171
[#5172]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5172
[#5179]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5179
[#5181]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5181
[#5183]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5183
[#5186]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5186
[#5187]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5187
[#5188]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5188
[#5189]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5189
[#5191]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5191
[#5193]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5193
[#5195]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5195
[#5196]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5196
[#5197]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5197
[#5198]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5198
[#5199]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5199
[#5201]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5201
[#5203]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5203
[#5205]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5205
[#5209]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5209
[#5214]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5214
[#5217]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5217
[#5218]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5218
[#5221]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5221
[#5222]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5222
[#5250]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5250
[#5255]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5255
[#5258]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5258
[#5259]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5259
[#5260]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5260
[#5262]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5262
[#5263]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5263
[#5264]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5264
[#5265]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5265
[#5268]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5268
[#5269]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5269
[#5272]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5272
[#5273]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5273
[#5275]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5275
[#5276]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5276
[#5278]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5278
[#5283]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5283
[#5285]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5285
[#5286]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5286
[#5287]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5287
[#5290]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5290
[#5294]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5294
[#5295]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5295
[#5298]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5298
[#5299]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5299
[#5300]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5300
[#5301]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5301
[#5303]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5303
[#5310]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5310
[#5314]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5314
[#5322]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5322
[#5324]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5324
[#5326]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5326
[#5327]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5327
[#5341]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5341
[#5342]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5342
[#5356]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5356
[#5359]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5359
[#5360]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5360
[#5361]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5361
[#5362]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5362
[#5364]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5364
[#5367]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5367
[#5368]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5368
[#5369]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5369
[#5372]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5372
[#5374]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5374
[#5380]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5380
[#5383]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5383
[#5385]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5385
[#5386]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5386
[#5387]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5387
[#5388]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5388
[#5390]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5390
[#5392]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5392
[#5394]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5394
[#5396]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5396
[#5398]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5398
[#5402]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5402
[#5409]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5409
[#5410]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5410
[#5414]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5414
[#5417]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5417
[#5427]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5427
[#5432]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5432
[#5434]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5434
[#5438]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5438
[#5439]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5439
[#5442]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5442
[#5443]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5443
[#5446]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5446
[#5453]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5453
[#5454]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5454
[#5456]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5456
[#5460]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5460
[#5461]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5461
[#5462]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5462
[#5463]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5463
[#5464]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5464
[#5465]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5465
[#5466]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5466
[#5467]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5467
[#5468]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5468
[#5469]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5469
[#5470]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5470
[#5472]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5472
[#5478]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5478
[#5481]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5481
[#5482]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5482
[#5484]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5484
[#5486]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5486
[#5488]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5488
[#5490]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5490
[#5492]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5492
[#5497]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5497
[#5500]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5500
[#5501]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5501
[#5502]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5502
[#5506]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5506
[#5511]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5511
[#5524]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5524
[#5527]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5527
[#5528]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5528
[#5529]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5529
[#5530]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5530
[#5532]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5532
[#5533]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5533
[#5534]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5534
[#5536]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5536
[#5537]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5537
[#5538]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5538
[#5542]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5542
[#5543]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5543
[#5544]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5544
[#5552]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5552
[#5553]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5553
[#5554]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5554
[#5556]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5556
[#5564]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5564
[#5566]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5566
[#5570]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5570
[#5572]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5572
[#5573]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5573
[#5577]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5577
[#5579]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5579
[#5580]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5580
[#5584]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5584
[#5585]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5585
[#5588]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5588
[#5590]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5590
[#5591]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5591
[#5593]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5593
[#5595]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5595
[#5598]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5598
[#5599]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5599
[#5610]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5610
[#5626]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5626
[#5627]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5627
[#5628]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5628
[#5631]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5631
[#5632]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5632
[#5636]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5636
[#5637]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5637
[#5641]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5641
[#5643]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5643
[#5657]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5657
[#5658]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5658
[#5659]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5659
[#5663]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5663
[#5664]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5664
[#5665]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5665
[#5668]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5668
[#5674]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5674
[#5679]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5679
[#5680]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5680
[#5682]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5682
[#5687]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5687
[#5688]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5688
[#5690]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5690
[#5694]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5694
[#5697]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5697
[#5698]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5698
[#5699]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5699
[#5700]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5700
[#5702]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5702
[#5704]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5704
[#5705]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5705
[#5706]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5706
[#5707]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5707
[#5708]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5708
[#5712]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5712
[#5714]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5714
[#5715]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5715
[#5718]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5718
[#5720]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5720
[#5721]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5721
[#5722]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5722
[#5726]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5726
[#5727]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5727
[#5729]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5729
[#5732]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5732
[#5736]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5736
[#5740]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5740
[#5741]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5741
[#5742]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5742
[#5748]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5748
[#5753]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5753
[#5764]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5764
[#5766]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5766
[#5769]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5769
[#5771]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5771
[#5772]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5772
[#5773]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5773
[#5779]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5779
[#5790]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5790
[#5791]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5791
[#5792]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5792
[#5794]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/5794
[#5795]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5795
[#5799]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5799
[#5800]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5800
[#5801]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5801
[#5803]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5803
[#5805]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5805
[#5806]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5806
[#5807]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5807
[#5808]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5808
[#5815]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5815
[#5816]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5816
[#5817]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5817
[#5818]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5818
[#5820]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/5820
[#5826]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5826
[#5827]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5827
[#5828]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5828
[#5829]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5829
[#5830]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5830
[#5831]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5831
[#5832]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5832
[#5835]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5835
[#5836]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5836
[#5837]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5837
[#5838]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5838
[#5839]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5839
[#5840]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5840
[#5841]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5841
[#5842]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5842
[#5845]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5845
[#5848]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5848
[#5850]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5850
[#5852]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5852
[#5853]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5853
[#5854]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5854
[#5855]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5855
[#5856]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5856
[#5859]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5859
[#5869]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5869
[#5870]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5870
[#5871]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5871
[#5877]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5877
[#5879]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5879
[#5880]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5880
[#5882]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5882
[#5885]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5885
[#5886]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5886
[#5895]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5895
[#5900]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5900
[#5901]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5901
[#5902]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5902
[#5906]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5906
[#5907]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5907
[#5908]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5908
[#5919]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5919
[#5920]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5920
[#5921]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5921
[#5922]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5922
[#5925]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5925
[#5926]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5926
[#5930]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5930
[#5933]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5933
[#5934]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5934
[#5935]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5935
[#5936]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5936
[#5937]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5937
[#5941]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5941
[#5942]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5942
[#5943]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5943
[#5944]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5944
[#5947]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5947
[#5948]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5948
[#5951]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5951
[#5952]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5952
[#5954]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5954
[#5955]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5955
[#5957]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5957
[#5961]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5961
[#5970]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5970
[#5971]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5971
[#5977]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5977
[#5978]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5978
[#5979]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5979
[#5980]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5980
[#5981]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5981
[#5982]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5982
[#5983]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5983
[#5984]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5984
[#5985]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5985
[#5986]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5986
[#5987]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5987
[#5988]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5988
[#5989]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5989
[#5990]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5990
[#5991]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5991
[#5992]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5992
[#5993]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5993
[#5997]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5997
[#5998]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5998
[#5999]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/5999
[#6000]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6000
[#6001]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6001
[#6004]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6004
[#6005]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6005
[#6006]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6006
[#6007]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6007
[#6008]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6008
[#6009]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6009
[#6010]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6010
[#6011]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6011
[#6012]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6012
[#6027]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6027
[#6031]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6031
[#6032]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6032
[#6033]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6033
[#6036]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6036
[#6042]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6042
[#6043]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6043
[#6044]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6044
[#6048]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6048
[#6060]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6060
[#6066]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6066
[#6070]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6070
[#6071]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6071
[#6072]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6072
[#6073]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6073
[#6074]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6074
[#6081]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6081
[#6096]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6096
[#6098]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6098
[#6102]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6102
[#6103]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6103
[#6104]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6104
[#6105]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6105
[#6111]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6111
[#6115]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6115
[#6118]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6118
[#6124]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6124
[#6125]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6125
[#6126]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6126
[#6138]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6138
[#6140]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6140
[#6141]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6141
[#6142]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6142
[#6143]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6143
[#6144]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6144
[#6145]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6145
[#6146]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6146
[#6148]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6148
[#6149]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6149
[#6150]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6150
[#6151]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6151
[#6152]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6152
[#6160]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6160
[#6161]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6161
[#6162]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6162
[#6163]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6163
[#6164]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6164
[#6165]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6165
[#6166]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6166
[#6167]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6167
[#6168]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6168
[#6169]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6169
[#6172]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6172
[#6184]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6184
[#6185]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6185
[#6187]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6187
[#6188]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6188
[#6190]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6190
[#6191]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6191
[#6192]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6192
[#6193]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6193
[#6195]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6195
[#6202]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6202
[#6206]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6206
[#6207]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6207
[#6209]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6209
[#6211]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6211
[#6212]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6212
[#6214]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6214
[#6217]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6217
[#6219]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6219
[#6221]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6221
[#6227]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6227
[#6228]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6228
[#6229]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6229
[#6230]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6230
[#6231]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6231
[#6232]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6232
[#6234]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6234
[#6235]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6235
[#6236]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6236
[#6237]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6237
[#6241]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6241
[#6243]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6243
[#6249]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6249
[#6250]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6250
[#6251]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6251
[#6252]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6252
[#6254]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6254
[#6257]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6257
[#6258]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6258
[#6261]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6261
[#6262]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6262
[#6264]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6264
[#6265]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6265
[#6271]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6271
[#6273]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6273
[#6275]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6275
[#6278]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6278
[#6279]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6279
[#6280]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6280
[#6281]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6281
[#6282]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6282
[#6285]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6285
[#6286]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6286
[#6287]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6287
[#6291]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6291
[#6292]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6292
[#6293]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6293
[#6294]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6294
[#6295]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6295
[#6296]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6296
[#6297]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6297
[#6298]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6298
[#6299]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6299
[#6300]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6300
[#6301]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6301
[#6302]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6302
[#6303]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6303
[#6305]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6305
[#6308]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6308
[#6309]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6309
[#6310]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6310
[#6311]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6311
[#6312]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6312
[#6315]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6315
[#6334]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6334
[#6339]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6339
[#6351]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6351
[#6352]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6352
[#6365]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6365
[#6381]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6381
[#6382]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6382
[#6383]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6383
[#6384]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6384
[#6385]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6385
[#6386]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6386
[#6387]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6387
[#6388]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6388
[#6389]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6389
[#6390]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6390
[#6392]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6392
[#6393]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6393
[#6394]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6394
[#6395]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6395
[#6396]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6396
[#6397]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6397
[#6398]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6398
[#6399]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6399
[#6400]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6400
[#6401]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6401
[#6402]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6402
[#6403]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6403
[#6409]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6409
[#6410]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6410
[#6411]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6411
[#6412]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6412
[#6413]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6413
[#6416]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6416
[#6417]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6417
[#6418]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6418
[#6419]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6419
[#6420]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6420
[#6422]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6422
[#6429]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6429
[#6430]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6430
[#6438]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6438
[#6441]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6441
[#6447]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6447
[#6443]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6443
[#6450]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6450
[#6446]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6446
[#6451]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6451
[#6452]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6452
[#6391]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6391
[#6449]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6449
[#6453]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6453
[#6454]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6454
[#6455]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6455
[#6456]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6456
[#6457]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6457
[#6458]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6458
[#6459]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6459
[#6460]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6460
[#6462]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6462
[#6468]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6468
[#6469]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6469
[#6470]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6470
[#6471]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6471
[#6472]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6472
[#6473]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6473
[#6474]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6474
[#6475]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6475
[#6476]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6476
[#6478]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6478
[#6480]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6480
[#6482]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6482
[#6504]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6504
[#6507]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6507
[#6536]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/6536
[#6539]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6539
[#6542]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6542
[#6544]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6544
[#6546]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6546
[#6547]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6547
[#6548]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6548
[#6550]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6550
[#6551]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6551
[#6552]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6552
[#6553]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6553
[#6554]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6554
[#6555]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6555
[#6557]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6557
[#6559]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6559
[#6560]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6560
[#6561]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6561
[#6564]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6564
[#6565]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6565
[#6566]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6566
[#6569]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6569
[#6576]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6576
[#6581]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6581
[#6583]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6583
[#6589]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6589
[#6591]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6591
[#6596]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6596
[#6597]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6597
[#6598]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6598
[#6600]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6600
[#6609]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6609
[#6614]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6614
[#6622]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6622
[#6630]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6630
[#6633]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6633
[#6637]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6637
[#6640]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6640
[#6645]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6645
[#6646]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6646
[#6647]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6647
[#6648]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6648
[#6649]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6649
[#6650]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6650
[#6651]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6651
[#6652]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6652
[#6653]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6653
[#6654]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6654
[#6659]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6659
[#6678]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6678
[#6690]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6690
[#6696]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6696
[#6700]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6700
[#6703]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6703
[#6706]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6706
[#6713]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6713
[#6715]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6715
[#6718]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6718
[#6723]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6723
[#6724]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6724
[#6727]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6727
[#6729]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6729
[#6736]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6736
[#6737]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6737
[#6738]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6738
[#6740]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6740
[#6741]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6741
[#6743]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6743
[#6745]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6745
[#6746]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6746
[#6747]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6747
[#6748]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6748
[#6749]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6749
[#6750]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6750
[#6751]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6751
[#6752]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6752
[#6755]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6755
[#6756]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6756
[#6757]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6757
[#6758]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6758
[#6759]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6759
[#6760]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6760
[#6761]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6761
[#6764]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6764
[#6766]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6766
[#6768]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6768
[#6771]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6771
[#6773]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6773
[#6778]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6778
[#6781]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6781
[#6787]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6787
[#6791]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6791
[#6793]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6793
[#6794]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6794
[#6795]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6795
[#6798]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6798
[#6800]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6800
[#6803]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6803
[#6804]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6804
[#6807]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6807
[#6808]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6808
[#6809]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6809
[#6821]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6821
[#6823]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6823
[#6827]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6827
[#6828]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6828
[#6829]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6829
[#6830]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6830
[#6831]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6831
[#6832]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6832
[#6833]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6833
[#6834]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6834
[#6836]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6836
[#6840]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6840
[#6849]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6849
[#6850]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6850
[#6853]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6853
[#6856]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6856
[#6858]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6858
[#6859]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6859
[#6860]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6860
[#6861]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6861
[#6865]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6865
[#6866]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6866
[#6868]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6868
[#6869]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6869
[#6873]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6873
[#6874]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6874
[#6875]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6875
[#6876]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6876
[#6877]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6877
[#6878]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6878
[#6886]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6886
[#6888]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6888
[#6889]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6889
[#6893]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6893
[#6894]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6894
[#6896]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6896
[#6897]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6897
[#6901]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6901
[#6902]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6902
[#6903]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6903
[#6906]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6906
[#6907]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6907
[#6908]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6908
[#6912]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6912
[#6915]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6915
[#6917]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6917
[#6919]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6919
[#6920]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6920
[#6925]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6925
[#6926]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6926
[#6927]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6927
[#6928]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6928
[#6929]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6929
[#6931]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6931
[#6932]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6932
[#6933]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6933
[#6945]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6945
[#6946]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6946
[#6953]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6953
[#6959]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6959
[#6965]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6965
[#6968]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6968
[#6969]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6969
[#6970]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6970
[#6971]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6971
[#6977]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6977
[#6979]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6979
[#7005]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7005
[#7010]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7010
[#7014]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7014
[#7015]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7015
[#7016]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7016
[#7017]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7017
[#7023]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7023
[#7028]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7028
[#7040]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7040
[#7041]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7041
[#7042]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7042
[#7061]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7061
[#7067]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7067
[#7068]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7068
[#7076]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7076
[#7077]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7077
[#7082]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7082
[#7083]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7083
[#7084]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7084
[#7085]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7085
[#7086]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7086
[#7106]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7106
[#7107]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7107
[#7109]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7109
[#7147]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7147
[#7114]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7114
[#7122]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7122
[#7123]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7123
[#7124]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7124
[#7138]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7138
[#7139]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7139
[#7253]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7253
[#7272]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7272
[#8006]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8006
[#8147]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8147
[#6982]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6982
[#6991]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6991
[#6993]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6993
[#6997]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/6997
[#7000]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7000
[#7003]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7003
[#7063]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7063
[#7146]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7146
[#7152]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7152
[#7153]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7153
[#7156]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7156
[#7157]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7157
[#7158]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7158
[#7159]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7159
[#7160]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7160
[#7161]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7161
[#7162]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7162
[#7163]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7163
[#7164]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7164
[#7165]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7165
[#7166]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7166
[#7167]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7167
[#7168]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7168
[#7169]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7169
[#7170]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7170
[#7171]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7171
[#7172]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7172
[#7173]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7173
[#7174]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7174
[#7175]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7175
[#7176]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7176
[#7177]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7177
[#7178]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7178
[#7179]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7179
[#7180]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7180
[#7181]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7181
[#7182]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7182
[#7183]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7183
[#7184]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7184
[#7185]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7185
[#7186]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7186
[#7187]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7187
[#7188]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7188
[#7189]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7189
[#7191]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7191
[#7192]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7192
[#7193]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7193
[#7194]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7194
[#7195]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7195
[#7196]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7196
[#7197]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7197
[#7198]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7198
[#7199]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7199
[#7200]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7200
[#7201]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7201
[#7202]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7202
[#7203]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7203
[#7204]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7204
[#7205]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7205
[#7206]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7206
[#7207]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7207
[#7208]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7208
[#7209]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7209
[#7210]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7210
[#7211]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7211
[#7212]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7212
[#7213]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7213
[#7214]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7214
[#7215]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7215
[#7216]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7216
[#7217]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7217
[#7218]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7218
[#7219]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7219
[#7220]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7220
[#7224]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7224
[#7226]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7226
[#7227]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7227
[#7228]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7228
[#7229]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7229
[#7240]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7240
[#7241]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7241
[#7242]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7242
[#7243]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7243
[#7252]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7252
[#7256]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7256
[#7257]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7257
[#7267]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7267
[#7269]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7269
[#7285]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7285
[#7292]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7292
[#7295]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7295
[#7309]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7309
[#7315]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7315
[#7337]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7337
[#7343]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7343
[#7344]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7344
[#7348]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7348
[#7353]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7353
[#7354]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7354
[#7357]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7357
[#7363]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7363
[#7365]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7365
[#7368]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7368
[#7370]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7370
[#7374]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7374
[#7375]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7375
[#7376]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7376
[#7379]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7379
[#7381]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7381
[#7382]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7382
[#7383]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7383
[#7384]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7384
[#7385]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7385
[#7392]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7392
[#7393]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7393
[#7394]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7394
[#7395]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7395
[#7396]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7396
[#7397]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7397
[#7400]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7400
[#7401]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7401
[#7402]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7402
[#7409]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7409
[#7410]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7410
[#7414]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7414
[#7417]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7417
[#7419]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7419
[#7423]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7423
[#7428]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7428
[#7432]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7432
[#7433]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7433
[#7441]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7441
[#7442]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7442
[#7443]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7443
[#7444]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7444
[#7445]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7445
[#7446]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7446
[#7447]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7447
[#7448]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7448
[#7449]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7449
[#7450]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7450
[#7456]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7456
[#7457]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7457
[#7458]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7458
[#7459]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7459
[#7465]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7465
[#7466]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7466
[#7467]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7467
[#7478]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7478
[#7479]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7479
[#7482]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7482
[#7483]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7483
[#7485]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7485
[#7486]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7486
[#7488]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7488
[#7491]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7491
[#7497]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7497
[#7498]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7498
[#7499]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7499
[#7502]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7502
[#7504]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7504
[#7505]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7505
[#7507]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7507
[#7508]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7508
[#7516]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7516
[#7522]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7522
[#7527]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7527
[#7528]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7528
[#7533]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7533
[#7534]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7534
[#7535]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7535
[#7537]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7537
[#7538]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7538
[#7539]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7539
[#7542]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7542
[#7543]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7543
[#7545]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7545
[#7547]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7547
[#7548]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7548
[#7549]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7549
[#7550]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7550
[#7551]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7551
[#7552]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7552
[#7554]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7554
[#7555]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7555
[#7557]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7557
[#7558]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7558
[#7559]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7559
[#7560]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7560
[#7561]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7561
[#7562]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7562
[#7564]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7564
[#7565]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7565
[#7566]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7566
[#7567]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7567
[#7571]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7571
[#7572]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7572
[#7573]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7573
[#7575]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7575
[#7577]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7577
[#7578]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7578
[#7581]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7581
[#7582]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7582
[#7583]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7583
[#7584]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7584
[#7586]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7586
[#7588]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7588
[#7590]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7590
[#7592]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7592
[#7593]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7593
[#7606]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7606
[#7608]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7608
[#7609]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7609
[#7610]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7610
[#7611]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7611
[#7616]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7616
[#7624]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7624
[#7639]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7639
[#7644]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7644
[#7646]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7646
[#7647]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7647
[#7649]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7649
[#7653]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7653
[#7655]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7655
[#7656]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7656
[#7657]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7657
[#7658]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7658
[#7659]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7659
[#7660]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7660
[#7661]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7661
[#7662]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7662
[#7663]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7663
[#7669]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7669
[#7670]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7670
[#7674]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7674
[#7675]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7675
[#7676]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7676
[#7677]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7677
[#7679]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7679
[#7681]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7681
[#7682]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7682
[#7683]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7683
[#7684]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7684
[#7688]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7688
[#7689]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7689
[#7690]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7690
[#7691]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7691
[#7692]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7692
[#7695]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7695
[#7696]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7696
[#7699]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7699
[#7700]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7700
[#7701]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7701
[#7704]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7704
[#7705]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7705
[#7706]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7706
[#7718]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7718
[#7724]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7724
[#7725]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7725
[#7726]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7726
[#7729]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7729
[#7731]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7731
[#7733]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7733
[#7735]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7735
[#7736]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7736
[#7746]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7746
[#7762]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7762
[#7769]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7769
[#7771]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7771
[#7772]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7772
[#7773]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7773
[#7774]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7774
[#7775]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7775
[#7776]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7776
[#7777]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7777
[#7778]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7778
[#7779]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7779
[#7780]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7780
[#7782]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7782
[#7783]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7783
[#7784]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7784
[#7785]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7785
[#7786]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7786
[#7787]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7787
[#7788]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7788
[#7790]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7790
[#7791]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7791
[#7810]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7810
[#7811]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7811
[#7812]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7812
[#7813]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7813
[#7814]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7814
[#7816]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7816
[#7822]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7822
[#7824]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7824
[#7837]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7837
[#7846]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7846
[#7849]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7849
[#7850]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7850
[#7851]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7851
[#7856]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7856
[#7863]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7863
[#7865]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7865
[#7873]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7873
[#7875]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7875
[#7876]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7876
[#7877]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7877
[#7878]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7878
[#7879]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7879
[#7880]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7880
[#7881]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7881
[#7882]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7882
[#7883]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7883
[#7884]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7884
[#7885]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7885
[#7886]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7886
[#7887]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7887
[#7888]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7888
[#7889]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7889
[#7890]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7890
[#7892]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7892
[#7893]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7893
[#7894]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7894
[#7896]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7896
[#7900]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7900
[#7901]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7901
[#7902]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7902
[#7903]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7903
[#7905]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7905
[#7906]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7906
[#7909]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7909
[#7918]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7918
[#7919]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7919
[#7920]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7920
[#7921]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7921
[#7922]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7922
[#7923]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7923
[#7924]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7924
[#7925]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7925
[#7926]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7926
[#7928]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7928
[#7930]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7930
[#7931]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7931
[#7933]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7933
[#7934]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7934
[#7935]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7935
[#7936]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7936
[#7937]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7937
[#7938]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7938
[#7939]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7939
[#7941]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7941
[#7944]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7944
[#7945]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7945
[#7949]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7949
[#7950]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7950
[#7951]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7951
[#7952]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7952
[#7955]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7955
[#7956]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7956
[#7957]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7957
[#7958]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7958
[#7959]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7959
[#7960]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7960
[#7961]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7961
[#7962]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7962
[#7974]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7974
[#7975]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7975
[#7979]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7979
[#7981]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7981
[#7984]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7984
[#7999]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/7999
[#8007]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8007
[#8012]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8012
[#8013]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8013
[#8014]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8014
[#8053]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8053
[#8054]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8054
[#8055]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8055
[#8063]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8063
[#8073]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8073
[#8074]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8074
[#8075]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8075
[#8076]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8076
[#8077]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8077
[#8078]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8078
[#8079]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8079
[#8081]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8081
[#8082]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8082
[#8083]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8083
[#8084]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8084
[#8086]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8086
[#8090]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8090
[#8092]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8092
[#8095]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8095
[#8096]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8096
[#8098]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8098
[#8099]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8099
[#8100]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8100
[#8101]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8101
[#8102]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8102
[#8103]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8103
[#8104]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8104
[#8106]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8106
[#8109]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8109
[#8110]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8110
[#8112]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8112
[#8113]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8113
[#8115]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8115
[#8117]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8117
[#8133]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8133
[#8136]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8136
[#8138]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8138
[#8141]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8141
[#8144]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8144
[#8146]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8146
[#8151]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8151
[#8153]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8153
[#8154]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8154
[#8156]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8156
[#8157]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8157
[#8159]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8159
[#8160]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8160
[#8162]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8162
[#8167]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8167
[#8168]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8168
[#8169]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8169
[#8185]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8185
[#8186]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8186
[#8187]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8187
[#8197]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8197
[#8219]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8219
[#8220]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8220
[#8221]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8221
[#8222]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8222
[#8223]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8223
[#8224]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8224
[#8225]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8225
[#8226]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8226
[#8234]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8234
[#8235]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8235
[#8236]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8236
[#8239]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8239
[#8240]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8240
[#8243]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8243
[#8244]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8244
[#8245]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8245
[#8246]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8246
[#8262]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8262
[#8275]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8275
[#8290]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/8290
