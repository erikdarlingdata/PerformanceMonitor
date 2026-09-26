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

### Added

- **Self-monitoring alerts (Collection Stopped, Capture Down, Collector Cost Regression) open a notebook with the server's collection health and the collector's log and cost** ([#4437])
- **Analysis-finding alerts open a notebook with that finding's summary and evidence** ([#4436])
- **Custom-rule alerts open a notebook that charts the rule's own measure with its threshold or band** ([#4433])
- **Poison Wait alerts open an authored notebook: top waits on both engines, waiting tasks, resource-semaphore/memory-grant detail when the wait is RESOURCE_SEMAPHORE, and a trend bound to the firing wait type.** ([#4424])
- **Server Unreachable/Restored and the Agent-job alerts (Failed Agent Job, Long-Running Job, Agent Not Running) now open an authored notebook instead of the mechanical section-list fallback, and the collection-log web read can filter by status.** ([#4422])
- **High CPU alerts open an authored notebook: SQL Server and PostgreSQL CPU timelines, top queries/procedures by CPU, and scheduler pressure, instead of the mechanical read list.** ([#4420])
- **PostgreSQL Wraparound Risk, Vacuum Horizon Blocked and Replication Slot Retention alerts open an authored notebook: their own drill-down reads and a 24h trend panel, instead of the mechanical fallback section list.** ([#4419])
- **Long-Running Query and Forced Plan Failing alerts open an authored notebook: active/completed queries and a completion-duration timeline for Long-Running Query, plan corrections and a corrections timeline for Forced Plan Failing.** ([#4418])
- **Authored Blocking and Deadlocks alert-notebook templates** ([#4395]) - `/api/alert-notebook` now returns a forensic, multi-cell layout for Blocking Detected, Blocking Wait Time, and Deadlocks Detected instead of the flat mechanical read list every other alert type gets: a blocked-process-reports or deadlocks timeline with a cross-annotation, ranked breakdowns by object/database/lock-mode, and the existing header/status/drill-down reads, all bound to the alert's own window and database.
- **Aurora per-query peak memory as a context fact** ([#4370]) - PostgreSQL-target top-statement findings now state a statement's peak executor memory (Aurora's `aurora_stat_statements()` only) as context when available, alongside its share of window time; it never changes a finding's severity, rank, or whether it fires, and reads NULL rather than 0 off Aurora.
- **Alert-notebook read-only render on `#/triage`** ([#4368]) - the alert deep link can now render the bound notebook document returned by `/api/alert-notebook` (server, status, and forensic read cells for the firing), read-only with no fleet scope picker; falls back to the existing triage assembly when the endpoint isn't present.
- **Notebook `read` cell type** ([#4367]) - Notebooks can now include a `read` cell (`{type:"read", read, params, viz, title}`), validated by the same rules as a dashboard's v1 read panel and rendered through the shared panel renderer. Those shared rules now also check a read panel's `params` on save, for dashboards as well, so a saved dashboard panel with a malformed `params` object is rejected the next time it is saved.
- **Alert-notebook binding endpoint** ([#4366]) - added `GET /api/alert-notebook`, which turns an alert link into a bound, read-only notebook definition (a dedup-matched alert, a four-arm live status, and a mechanical per-metric read cell list), the server half of the alert-to-notebook MVP (#4222).
- **A shell-level pause/resume control for the web viewer's auto-refresh** ([#4365]) - the 60s refresh loop previously only paused automatically while a browser tab was hidden. Added a button in the sidebar chrome, present on every page, that lets the operator pause and resume the tick directly; the paused state persists across reloads. Resuming runs one refresh immediately. Also added the alert-notebook triage deep-link route to the existing poll-skip guard so the periodic tick does not re-render it.
- **The store host profile now reports its cloud instance type** ([#4330]) - `get_store_host`, `--check-settings` and the store host web panel now show the EC2 instance type or Azure VM size of the machine the Darling service runs on. For a managed store, or a store on the same machine, that is the store's own host. A short, bounded probe of the cloud metadata endpoint reads it, as the metadata service reports it, and the panel says "not detected" off the cloud or when the probe finds nothing.
- **Store host visibility: an MCP read and a web panel** ([#4282]) - The managed PostgreSQL store's own host
- **`--check-settings` reports whether a managed store's sizing still matches this host** ([#4271]) - a new CLI verb prints the host and store facts (RAM, CPUs, PostgreSQL/TimescaleDB versions, store size, buffer hit ratio, uncompressed TimescaleDB chunk bytes against RAM) and a verdict for each of the eight sizing-relevant settings: matches, stale after a hardware change, operator override, or not managed. `--json` prints the same facts for scripting. Separate exit codes for a config error, an unreachable store, and a stale setting let an install or upgrade script gate on the outcome that matters to it.
- **`--validate-config` (`--test-connection`) no longer fails a healthy store over a stale entry in darling.json** ([#4271]) - once the store is reachable, this now probes its own registry of monitored servers instead of the config file's first-bootstrap seed list. A server that is only in the file (never registered, or since removed from the registry) is now a warning, not a failure. A store that cannot be reached still falls back to the file's list, and says so on the console. This closes the false failure reported on a 42-server production store whose darling.json still listed two long-removed servers.
- **Darling's MCP host now serves a /core endpoint alongside /** ([#4121]) - / keeps serving every Darling MCP tool, unchanged. /core serves a fixed 74-tool subset: the four tools a fresh conversation starts from (list_servers, get_fleet_overview, analyze_server, get_tool_guide) plus every tool the analysis engine's next_tools recommendations can point at. /core sits behind the exact same Host, bearer-token and CIDR checks as /, and a tool outside the subset fails the same way an unknown tool name would. A /core session's shared instructions now lead with a short note. The note says /core serves 74 tools, and that the tool count and tool notes after it describe the full set on /. It also says a tool outside /core fails as an unknown tool, though get_tool_guide can still describe it. Along the way, fixed eight next_tools entries that recommended get_blocked_process_reports, a tool Darling has never had (Darling's name is get_blocking), so those findings always pointed at a call that failed.
- **get_resource_semaphore also returns interval_seconds** ([#4103]) - On Darling and Lite, get_resource_semaphore now returns interval_seconds beside sample_interval_seconds. It carries the sample interval under the name get_latch_stats and get_spinlock_stats already use, and it is null when the interval cannot be known. sample_interval_seconds stays for existing readers.
- **get_query_store_top takes module_name, so one stored procedure's Query Store history can be read without widening top** ([#4057], contributed by @kendra-little) - the read ranked the whole window before capping it, so a procedure an investigation named but whose queries sat outside the top N could not be reached at all. The filter is part of the query on both SKUs. It matches the exact, case-sensitive, schema-qualified name the collector records (the full_name get_top_procedures_by_cpu returns, Adhoc for ad-hoc statements), and it applies after the #1841 interval dedup and before the ranking and the cap. Every row now carries module_name, beside the execution_type #4060 added, and the two filters compose. A filter that matches nothing answers empty only when the same read without the filters has rows, and the message names both filters. On Darling, a module miss also carries effective_start, effective_hours_back and window_truncated as hints. Otherwise the read answers as an unfiltered one would.
- **The store names its own slow queries: `get_store_query_stats` ranks the monitoring store's SQL by cost and by the role that ran it** ([#3899], hardened before release by [#3915]) - Field reports that the web viewer and MCP tools were slow could not be traced to a query from anything the product recorded: the store loaded only TimescaleDB and logged no statement durations. The managed store now preloads `pg_stat_statements` through a new v13 conf block that MERGES it into the effective `shared_preload_libraries` list (includes followed), so an operator's own libraries survive, and turns `pg_stat_statements.track_utility` off, because provisioning's `ALTER ROLE ... PASSWORD` literals are otherwise recorded verbatim (measured on the bundled PostgreSQL 18.4). Every start then checks which preload assignment is in force and logs the exact fix when an `ALTER SYSTEM` override, a later line, or an edit the block overrides leaves the library out; the `ALTER SYSTEM` fix is written one quoted literal per library, since a single literal holding the list is stored as one library name and the store will not start. A store-object convergence step, run at every start and hourly on a TimescaleDB store, creates the extension and two SECURITY DEFINER readers (pinned `search_path` and `standard_conforming_strings`, revoked from PUBLIC) granted to the admin, viewer and mcp roles. The readers refuse every statement whose text can carry a credential (role, user-mapping, subscription or server DDL, or a `PASSWORD` literal), with a backslash-free pattern a caller's string mode cannot switch off, and the step scrubs such statements from this database's entries and warns once if utility tracking is on. `get_store_query_stats` (MCP, and the `/api/read` mirror) ranks statements by total, mean or max time, calls, rows or disk reads, attributes each to its role with each role's share of the recorded time, and returns query ids as strings. It says what the figures leave out: `COPY` ingest while utility tracking is off, statements whose text the store owner may not read (and the `pg_read_all_stats` grant that fixes it), and a compose or bring-your-own store's web and MCP hosts, which connect as the owner ([#3914]). It answers `precondition` with the remedy, from the catalog, when the module is missing, not loaded, older than 1.8, or not granted, and a 1.8 install gets the ranking without the reset stamp. The viewer and mcp roles also log any statement past a third of their `statement_timeout` (5 s at the default), re-asserted with the timeout on a reload, with bind parameters off; the store-log sweep keeps those under `slow_statement`, one row per statement with comments stripped, every literal masked and parameter lines dropped, whichever role's statement it was. The Linux compose store gets the same preload on its command line, and `provision-roles.sql` carries the bring-your-own steps, including utility tracking off for its own session. Covered by unit tests for the merge, parse, include and coverage checks, the version gates, the redaction and the precondition answers, and live tests on the bundled runtime proving that a role password never reaches the reader (even with `standard_conforming_strings` off), that statements are attributed per role and normalized, that the scrub removes a password recorded with tracking forced on, that a non-superuser store owner can run the setup, and that an extension walked up from 1.7 is gated and then served.
- **Maintain your own theme colors, per theme, with a reset to default** ([#3577]) - Settings gains a Colors section beneath the theme drop-down in both Lite and the Darling viewer: twelve palette colors of the theme on screen (backgrounds, text, accent and its ink, alternating row, the four status colors), each with a swatch, hex, an HSV picker and the measured WCAG contrast ratio against the surface it renders on, color-coded at 4.5:1 and 3:1 - a warning, never a gate. Apply writes a per-user theme-overrides.json beside settings.json (the viewer's beside viewer-settings.json, local to the machine) and re-applies live; Reset to default returns that one theme to its shipped palette; Open file hands you the JSON, and saved edits apply on their own. A bad file, key or value is a logged warning and the stock theme, never a crash.
- **A PostgreSQL target with every logging setting off no longer looks identical to an instrumented one** ([#3607]) - `get_pg_logging_audit` judges log_min_duration_statement, log_lock_waits, log_temp_files, log_autovacuum_min_duration, log_checkpoints and log_connections/log_disconnections from the stored configuration snapshot, setting by setting, with what each unlocks, the recommended value and its cost, and the remedy in the hosting flavour's own syntax (ALTER SYSTEM plus a reload, or a parameter group where rds.* parameters prove RDS/Aurora). It sits beside `get_pg_plan_capture_readiness`, whose auto_explain facets it lists but never re-judges, so target onboarding has one "is this target telling us everything it could" answer.
- **A stock PostgreSQL target without pg_wait_sampling now has a wait profile instead of an empty chart** ([#3604]) - pg_wait_sampling grows a service-side arm that polls pg_stat_activity once a second for a 30-second window every five minutes and accumulates the tallies itself when the extension is absent, so the three PostgreSQL wait tiers (Aurora native › extension › service sampler) are one connect-time decision with exactly one instrument per target. get_pg_wait_stats, get_pg_wait_sampling and the Viewer disclose instrument: engine_cumulative | extension_sampled | service_sampled, with the floor caveat on the service tier. Aurora is gated off pg_wait_sampling (it cannot preload the module) and is pointed at pg_wait_stats instead. No migration.
- **PostgreSQL's server log is read by a classifier, not by two regexes** ([#3601]) - Darling now tails the same log the deadlock and plan readers tail — `pg_read_file` self-hosted, the RDS log API on Aurora/RDS — and classifies every line into families: `error` (WARNING or worse), `connection`, `lock_wait`, plus `temp_file`/`autovacuum`/`checkpoint` recognised for the structured tables #3602/#3603 add. Events land in `collect.pg_log_events` (V129, 30-day retention) with `message`/`detail`/`context` redacted by the plan parser's own patterns and the statement never stored, only fingerprinted; `get_pg_log_events` reads them with `family`/`min_severity` filters and the honest page contract. The three log readers now share one tailer (`PgServerLogTail`) instead of three copies.
- **The log said what the spill cost and what the vacuum cost; the store kept the sentence and threw away the number** ([#3602], [#3603]) - Two parser families on the log-event pipeline lift the figures out of the prose: a `temp_file` event carries the spilled file's exact bytes beside the fingerprint of the statement that spilled, and an `autovacuum` event carries the run's relation, duration, pages, tuples, buffers and WAL, on V130 columns of `pg_log_events` rather than sibling tables. `get_pg_log_events` publishes them where the line carried them; `get_pg_autovacuum_health` gains `recent_runs` per table, so "is it keeping up" and "what does each run cost" answer from one place. Line shapes pinned per PostgreSQL 16/17/18 from the server source.
- **Alert families route to their own channels** ([#3598]) - Every alert used to fan out to every configured channel from one install-time row, so self-monitor alerts, scheduled reports, job failures and performance pages interleaved in one stream and the reader did the routing in their head. A sparse `config_notification_routes` table (V131) layers over that row: a route names a family (`self-monitor`, `reports`, `agent-jobs`, `performance` — a closed taxonomy every alert maps into, census-pinned) or an exact metric, and carries a destination per channel; resolution runs once per firing after cooldown — exact name, then the firing a recovery pairs with, then family, then the parent's default — per channel, so an empty column inherits and a store with zero routes behaves byte-for-byte as before. The delivery ledger records which route matched, recoveries land where their firing did, the viewer gains a Routes grid, `get_notification_routes` / `set_notification_route` / `delete_notification_route` expose it to agents, and routes hot-reload through the same beacon mute rules use.
- **PostgreSQL targets enter the analysis pipeline** ([#3542] v1 plumbing) - The scheduled pass and every MCP analysis tool used to skip a PostgreSQL target behind an engine tombstone ("does not apply … use the get_pg_* reads"), because the pipeline read SQL Server tables and would have sat at "0 hours, still collecting" forever. The analysis service now holds two engine component sets and picks one PER CALL from the registry's `engine_kind` (two of its construction sites are singletons shared by every server, so the constructor could not be told); a PostgreSQL pass measures its 24-hour gate, its coverage witness and its baseline gate on `pg_database_stats`, scores a `pg_`-prefixed fact vocabulary declared once before any row persists, and clears the tombstone row on its first real pass. Every shared switch (scorer, advisory roots, graph, advice, anomaly reconciler, tool recommendations) gained exactly one delegating `pg_` arm so the nine content lanes build in parallel without touching shared files; `audit_config` answers `not_collected` for a PostgreSQL target instead of "the config collector may not have run yet". The SQL Server pass is byte-identical. No schema, no content. Rider (#3653/#3616): `HADR_SYNC_COMMIT` findings on Darling now carry next_tools (`get_wait_trend`, `get_ag_health`, `get_perfmon_trend`, `get_file_io_stats`), the pair of Lite's #3659.
- **A PostgreSQL target's durability posture is stated, not inferred** ([#3542] lane 8) - The analysis pass read nothing about `fsync`, `full_page_writes` or `synchronous_commit`, so a cluster running with fsync off analysed exactly like one running safe. Three `pg_posture` facts now come off the newest `pg_server_config` snapshot: fsync/full_page_writes off is a CRITICAL policy card naming the corruption trade, synchronous_commit off a 0.4 advisory naming the acknowledged-then-lost commit window, and Aurora's platform-managed pair is present, marked managed and ungraded. No amplifier, graph edge or optimisation word can reach a posture fact — pinned in source and at runtime (D6).
- **A PostgreSQL analysis pass now names the statement** ([#3542] lane 7) - A PostgreSQL target's stories ended at a server-level symptom and never said which statement. `PG_BAD_ACTOR_<queryid>` facts from `pg_statement_stats` stored deltas now carry one shape's share of the window's total execution time (denominator over the whole window, never the page), its calls/sec over the span its deltas accrued and mean/max ms; the scorer grades the share behind an idle-window gate with every bar declared unmeasured (`threshold_lineage = 0`); the card states the figures, both levers with their counter-objectives and the queryid re-key caveat; the drill-down carries the normalised text, its hash and the per-database breakdown.
- **The two PostgreSQL knobs learn to speak** ([#3542] lane 2) - The analysis pass had nothing to say about `shared_buffers` and `max_wal_size`, the two settings the OtterTune field study found carry 75–95% of measured tuning value. Both are now cut from the latest `pg_server_config` snapshot: at their shipped defaults (128 MB, the line `initdb` writes; 1 GB) they root a 0.4 ADVISORY card and never an incident on their own, and reach the incident line only when the engine's own workload signal co-fires — `PG_CHECKPOINT_PRESSURE` (requested-dominant checkpoints from `pg_write_stats` deltas, with WAL bytes per interval) or `PG_BUFFER_CACHE_PRESSURE` (a composite of hit ratio, bgwriter and, on PostgreSQL 16+, `pg_io_stats` evictions, each arm carrying its own evidence and honesty flag: `evictions_tracked` below 16, `hit_ratio_suppressed` on Aurora where the storage layer answers reads). Every bar carries lineage — defaults engine-defined, ramps declared unmeasured with the calibrating read named — and the advice states the measured share, the value on the host, both levers and each lever's counter-objective. A `PgSettingValue` helper normalises PostgreSQL's unit zoo (8kB pages, kB/MB/GB, ms/s/min, -1 sentinels) once.
- **A PostgreSQL target's analysis pass gains its vacuum family, graded on the same bars the Tier-0 alerts page on** ([#3542]) - `PG_AUTOVACUUM_BACKLOG` names the worst table persistently past its own autovacuum trigger line with the ratio to that line, the dead-tuple slope over the run and whether autovacuum ran and lost (cumulative counts differenced, never summed); `PG_WRAPAROUND_TREND` grades XID and MultiXact separately through the alert evaluator's four arms and adds a time-to-wall estimate with an explicit not-computable branch; `PG_XMIN_HOLD` grades the alert's identity arm and names the fix per holder kind. The wraparound/xmin/slot constants move to one shared `PostgresOutagePredictorThresholds` that both the alert evaluator and the analysis scorer reference, pinned so the surface that pages and the surface that narrates cannot disagree; the three facts chain into one story with value-stated advice that never suggests disabling autovacuum.
- **A PostgreSQL pool near its connection ceiling is graded as the outage it is** ([#3542]) - A PostgreSQL target's analysis pass had no session fact: a pool at 96 of 97 usable slots — one deploy from `FATAL: too many clients already` — produced an empty verdict at full coverage. `PG_CONNECTION_SATURATION` now grades the window's peak session count from `pg_session_states` against the engine's own line, `max_connections − superuser_reserved_connections` (read off the config facts, never re-read), scoring zero below 0.8 and 1.0 at 0.9 with `threshold_lineage = 0`; the card states the division with all three numbers, the peak capture's active / idle-in-transaction / other breakdown, and both levers — a pooler or a higher ceiling — with what each costs. When the monitoring login lacks `pg_read_all_stats` and most stored rows are redacted, `PG_MONITORING_PERMISSIONS` says so instead of a ratio built on blank rows.
- **A PostgreSQL analysis pass now names temp-file spill and gates work_mem on it** ([#3542]) - PG_TEMP_SPILL grades spilled bytes per second of observed time from the reset-aware pg_database_stats difference (a pg_stat_reset() mid-window is clamped and reported), names the database that spilled the most, and leads a story to CONFIG_PG_WORK_MEM, which scores 0 without that evidence and reaches the incident line only beside it; the drill-down lists the statements that wrote the most temp blocks and the advice states the host's own work_mem × max_connections arithmetic. The same read emits PG_TPS (with a half-window trend) and PG_DEADLOCK_RATE from the engine's counter on the alert band's measured tiers, saying how many the log captured beside how many the engine counted.
- **A PostgreSQL analysis pass reads the wait profile — measured on Aurora, estimated on stock, and says which** ([#3542]) - Lock, LWLock, IO and IPC rollups with the Lock:relation, LWLock:WALWrite, IO:DataFileRead and IO:WALSync standouts, each a fraction of the wait source's own observed time. Aurora's pg_wait_stats deltas are summed under the three-state interval (a restart collection is not a sample); a stock target's pg_wait_sampling counts × profile_period are read only when the Aurora table is empty and every such fact carries is_sampled and its resolution, with advice that says "estimated from sampling" and never "spent". Bars are unmeasured and say so; a rollup yields to its fired standout so one wait is graded once; WAL waits chain into checkpoint pressure, data-file reads into buffer-cache pressure, and Lock waits mesh with connection saturation. Lock advice names PostgreSQL's levers, not the SQL Server isolation remedy.
- **A PostgreSQL target learns its own normal** ([#3542] lane 9) - A PostgreSQL analysis pass had no baseline and no anomaly detector, so a 6× throughput surge, a tripled connection pool or a serverless instance pinned at its configured ceiling was indistinguishable from a quiet Tuesday - and had the anomalies existed, they would have scored 0 and never left the 1.49 cap (#3584's defect, again). Five baselines over the pg_* raw hypertables (TPS and deadlocks/hour off the reset-aware per-database difference, the capture's total_sessions, the Aurora all-types wait rate with CPU excluded, percent of the configured ACU ceiling - never raw cpu_percent) and five detectors through the shared AnomalyGate writing the metadata the shared ramp and extremity escape read; a PostgreSQL ratio ramp with no sentinel for a first occurrence; load-family co-fires confirmed only by a measured capacity reading; PG_CPU_PERCENT graded only when capacity was measured; advice in PostgreSQL nouns. Every unmeasured bar carries its lineage and threshold_lineage = 0. Stock PostgreSQL's sampled waits get no anomaly baseline in v1.
- **The measurement contract had ten rules and two tests** ([#3653]) - The contract #3540 stated in one sentence is now a numbered list in `Lite.Tests/MeasurementContractCensusTests` (4 and 7 keep their tests' numbers; 9–10 are left empty because the review's enumeration is not on the record), and rules 1, 2, 3 and 6 are censuses over both SKUs' sources with positive controls and population floors: no reader divides by a stored interval except through NULLIF, no rate assumes a cadence, exactly two carriers observe an epoch and every forget site is named, every per-second alias is a quotient. Rule 5 is rostered against the real continuous-aggregate definitions — the #3698 successors carry the interval predicate; the superseded pair and three hourly rollups do not — and rule 8 waits, pinned, on the `cntr_type` rung.
- **v2 plumbing for the PostgreSQL-target engine, and the wait-profile anomaly now folds onto its wait and can page** ([#3691]) - The I/O, replication and bloat families exist as stubs behind one delegating arm per shared switch, with the v2 vocabulary declared once so lanes 11–15 never touch a shared file. `ANOMALY_PG_WAIT_PROFILE` stopped sitting one hundredth under the page line: it folds onto the wait card its dominant contributor names and leaves the 1.49 cap on the same three readings as its SQL Server twin, anchored on its own firing multiple.
- **Schema 133: the PostgreSQL saturation numerator and the sampler's duty cycle are stored** ([#3691]) - `pg_database_stats` gains `numbackends` (the client backends connected per database at the instant of the read, a level, never differenced) and `pg_wait_sampling` gains `sampled_ms` (the milliseconds each five-minute collection actually observed - 30,000 from the service-side sampler, NULL from the extension arm that watches the whole interval), because "count / max_connections" had only an exception-capture count to divide and "samples × period / interval" understated the sampler arm ~10× with nothing in the row saying so. Written by the collectors from this rung; read by nothing yet. Lite stores no `pg_*` table, so no DuckDB bump.
- **The pull-request review learns the repository's traps and stops answering LGTM by default** ([#3710]) - The bot review had read several hundred pull requests and posted LGTM on very nearly all of them; every defect it missed was later found by behaviour, not by reading. Three structural causes, three fixes: a `.github/REVIEW-TRAPS.md` of the repository's recurring defect shapes, each citing the pull request that bit, which the review must consult and name; a blind first phase that reads the diff and its callers before it is allowed the author's account, then reports where the two diverge; and a structured verdict — every claim in the body VERIFIED, UNVERIFIED or REFUTED, the three riskiest lines and what pins each, a machine-readable ledger line — without which the run fails, so a bare LGTM is no longer a valid review. A paths filter sends analysis, collectors, storage, service, alerting and notification changes to a full-tier Opus pass with the 1M context window and everything else to a lighter Sonnet pass; the verdict contract is identical either way. Lands dark under the workflow-drift rule; the first live run is the first pull request after the release sync.
- **A server configuration change now has a consequence the engine states — `CONFIG_CHANGED` compares the ±4 h around it and says what moved, or that nothing did** ([#3653]) - The analysis engine graded the current value of seven `sp_configure` settings and never asked whether a change had an effect; that join lived in the operator's head between `get_server_config_changes` and `compare_analysis`. Both SKUs now emit one Information-level `CONFIG_CHANGED` finding per change event observed inside the pass window, running `ComparePeriodsAsync` over the four hours before the observation and the (honestly clamped) four hours after, banded by the same dispersion rules as `compare_analysis`, with the setting, old → new, and the moved metrics — or the sentence that none moved — frozen into the finding. Because `server_config` is captured on connect, the finding says "first observed", states the span the real change landed in, and never claims to be a causal test; database-config and trace-flag changes are the next slice.
- **PostgreSQL deadlock cards carry the captured exemplars, grouped by shape and ranked by recurrence** ([#3691]) - The card said "the engine counted N; M were captured from the log" and stopped. The drill-down now reads pg_deadlocks for the window, groups reports into shapes (participants, lock modes, resources — deadlock_hash identifies one report, not a shape), carries the top three with bounded statement and graph text, reuses the counter the card was graded on, and re-freezes the advice with the value-stated sentence and the ordering lever. Zero capture beside a non-zero counter names the logging-posture settings instead of showing an empty list.
- **The PostgreSQL bloat family grades fourteen-day GROWTH behind a size floor, never a spot percentage** ([#3691]) - `bloat_pct_estimate` read 71–99.8 % on most of the measured fleet's small heaps while their fourteen-day growth was zero bytes, so a percentage-graded finding would have paged everywhere and argued for VACUUM FULL on a Tuesday. `PG_BLOAT_TREND` (hourly `pg_table_bloat_stats`) and `PG_INDEX_BLOAT_TREND` (daily `pg_index_bloat`) now grade the estimate's growth across a fourteen-day lookback — 256 MiB and 25 % of the earlier value, 1 GiB critical, 64 MiB floor, all measured on the dogfood fleet — exclude `estimate_unavailable` and skipped rows outright, name the top three objects, state their own sample counts, and chain onto a same-table `PG_AUTOVACUUM_BACKLOG` as its damage. Withheld estimates are facts that say why, never zero bloat.
- **A chain that fired every Tuesday at the same hour was rated a fresh incident each week and nobody was told; the pass now labels it "recurring at this hour" at unchanged severity, and a weekly Agent job whose slot slid gets "maintenance window moved"** ([#3653]) - Ruling Q3: label, not discount. After the anomaly fold and before the finding is persisted, one store read of the prior three weeks (on the target's clock via `server_properties.utc_offset_minutes`, UTC disclosed when absent) lets `RecurrenceLabeler` append one sentence to the frozen advice of every chain that fired in the same hour×weekday slot for three consecutive weeks, and one naming the job and both slots to every story tied to a fired `RUNNING_JOBS` whose slot moved since last week. Severity is bit-identical with and without the label; both `get_analysis_findings` twins publish `recurring_at_this_hour`, `recurrence_weeks` and `maintenance_window_moved`.
- **PostgreSQL read latency is now a graded fact, and the pass says why when it cannot know** ([#3691]) - `PG_IO_READ_LATENCY_MS` states milliseconds per data-file read from `pg_stat_io`, graded against bars measured on the dogfood fleet's Aurora storage (10 / 30 ms, population named), with `ANOMALY_PG_IO_LATENCY` when an hour departs this server's own routine. With `track_io_timing` off the pass reports the reads it counted and that none were timed — never 0.000 ms — and on a pre-16 major it says `pg_stat_io` is absent; write latency is stated, not graded.
- **Replication lag and slot retention stories for the PostgreSQL-target engine** ([#3691]) - A PostgreSQL operator's agent now hears whether a standby is behind or FALLING behind and at which stage (sent / write / flush / replay), whether a replication slot is filling the disk or has already lost the WAL its consumer needs, and whether a slot's horizon is what holds vacuum back — three facts, one chain into PG_XMIN_HOLD, and the replay-lag anomaly against the server's own hour-of-week baseline. Every shared bar is the Tier-0 slot alert's own by reference; bytes are graded and NULL replay_lag_ms is tolerated; the drift multiple is unmeasured (n = 1).
- **The CPU sample's UTC instant and the server's time-zone id are now stored, so UTC-window readers stop deriving an offset that is an hour wrong across DST** ([#3653]) - `cpu_utilization_stats.sample_time` is the monitored server's LOCAL wall clock, and every reader that aligned it against UTC derived the offset per batch or from the single collected `utc_offset_minutes`, placing the samples nearest a DST transition an hour wrong. Darling V134 / Lite v63 add `sample_time_utc` beside the unchanged local stamp (the same instant off `SYSUTCDATETIME()`), which the viewer, MCP `get_cpu_utilization` and the Lite CPU window now prefer, and `server_properties.time_zone_id` (`CURRENT_TIMEZONE_ID()`, SQL Server 2022+ / Azure SQL; NULL elsewhere) beside the offset that could not say which side of a transition an instant fell on. Nullable, no backfill: the offset a server had at a past sample's instant is exactly what the store never recorded.
- **PostgreSQL sessions: an operator now learns which application left a transaction open and walked away** ([#3691]) - The analysis pass said nothing about idle-in-transaction sessions beyond a share of the pool; `PG_IDLE_IN_TRANSACTION` now names the longest holder (application, role, database) from `pg_session_states`, graded on duration bars the fleet calibration measured (60 s / 10 min — zero rows reached 60 s over 7 days × 50 clusters), a band higher when it pins the xmin horizon (never on the collector's "pins nothing" sentinel), amplified when the same holder recurs, joined to the saturation story only when parked sessions are a quarter of the pool, and honest when the monitoring login cannot see state (`idle_in_transaction_unobservable = 1`). The lever is the application; both server-side backstops state that they roll the work back.
- **The Long-Running Query opt-out knob was read-only on Darling — V135 gives it a store home with the production read's seeds as the column default, editable in the Viewer and through `update_alert_settings` on both SKUs** ([#3653]) - Two `text[]` columns on `config_alert_settings` default to the seeded lists, so a pre-rung store evaluates exactly what it did before and a list an operator clears stays cleared; `get_alert_settings` publishes `long_running_query.excluded_program_name_prefixes` / `excluded_logins` on Lite and Darling alike, and the viewer's connect-time probe maps a current store to V135.
- **PostgreSQL blocking: the pass names the head of the chain, how long the sessions behind it really waited, and whether the log agrees** ([#3691]) - `PG_BLOCKING_CHAIN` reconstructs each capture's chains from the sampled `pg_blocking_edges` (a head is a blocker not itself blocked; cycles are counted, never attributed), names the head with the most sessions behind it and grades on the blocked side's own duration — never captures × cadence — routing to the idle-in-transaction holder or the long-running query behind it. `PG_LOCK_WAIT_EVENTS` reads the event-grain `log_lock_waits` lines (count, longest, relation) and says `log_lock_waits` is off rather than reporting zero; `PG_LONG_RUNNING_QUERY` grades client backends' own query duration; `ANOMALY_PG_BLOCKING` baselines blocked sessions per minute with `collection_log` supplying the "looked and found nothing" zeros a healthy instance never writes to the edges table, gated on peak AND mean so one mid-flight handoff cannot fire it. Every bar unmeasured and says so; `deadlock_timeout` is the one engine-defined line, stated as context.
- **A PostgreSQL table that turned autovacuum off and fell behind is its own card** ([#3691]) - v1 folded a table's `autovacuum_enabled = off` reloption into the backlog fact's metadata, so an operator saw "a backlog" and had to read the fine print to learn the engine had been told not to act. `CONFIG_PG_AUTOVACUUM_DISABLED` now names the worst such table with how far past its own trigger line it sits and for how long, whether autovacuum is also off server-wide, and the shape of the other disabled tables that met the gate; it joins the backlog's story on the same table and offers re-enable or a scheduled manual `VACUUM (ANALYZE)` with each lever's cost. A disabled table with no backlog is not a finding.
- **A collector can be enabled or disabled headlessly** ([#3752]) - `long_query_completions` ships opt-in, and opting in is what creates its Extended Events session on the monitored servers — yet the only surface that could flip it was the WPF Viewer's Collector Schedules window with the store's `admin` role, on a product whose design point is running headless. `--enable-collector <name> [--server <name>]` and `--disable-collector` on the service exe build the same `enable_collector` / `disable_collector` command the store's command plane already carries and run it through the executor's own plan — one write path, no SQL of their own — then read the resulting schedule back. Documented in the CLI section.
- **Stock PostgreSQL's sampled waits read per second the sampler was watching, and get their own baseline and anomaly** ([#3691]) - The service-side sampler watches 30 s of every 300 s cycle, so the sampled wait rate over the interval understated the truth ten times and stock servers had no notion of their own waiting normal. The rate now divides by V133's `sampled_ms` (a pre-V133 row reads as its whole interval and says `sampled_ms_known = 0`; never a guessed 30 s), a `pg_sampled_wait_ms_per_sec` baseline and `ANOMALY_PG_SAMPLED_WAIT_PROFILE` detector judge the peak and the mean against the server's own hour-of-week, folding onto the dominant sampled wait's card, and when both wait tables write one window the exact source wins and the fact says so.
- **Rule 6's C# half is a census, not a stated bound** ([#3653]) - Every per-second payload key on both SKUs — an MCP tool's anonymous-object member, a fact's Metadata entry, a key written through a const — is now traced to a reader field of its own rate name, a C# quotient over a measured span, a rate helper handed the interval, or an aggregate over a rate; a stored delta passed through under the name, or a cadence literal in the divisor, is a red build naming its line. Seven keys whose provenance is another name are rostered shrink-only, each resting on an alias or model quotient the census asserts.
- **CONFIG_CHANGED covers database options and trace flags, not only sp_configure** ([#3653]) - The analysis pass read one of the store's three on-connect configuration snapshot families; a database flipped to SIMPLE recovery or a trace flag enabled in the window was a history row and no finding. The same fact now diffs all three families through the shared ConfigChangeDiff arms, runs the same ±4 h banded compare, and folds changes the same connect observed together into one card naming each in its family's words; a database option's text values ride the fact's object name and its database in the database seam. Measured on a SQL Server 2022 container: DBCC TRACEON writes msgs 17550/17551 stamped with the session's database (dropped by the default-trace collector today) and ALTER DATABASE SET leaves no ErrorLog trace event, so both new families anchor on the observation with its disclosures; log_reuse_wait_desc is excluded as a live status, not a setting.
- **Every trend surface renders the identity-epoch discontinuity marker the store already held** ([#3653]) - #3694/#3705 marked every epoch change (restart, rename, failover, stats_reset, postmaster restart) on the carrier run's collection_log note and in collector_state, and no read layer drew it: the re-baselined minute looked like the workload stepping. Eight trend tools per SKU now end with a trailing discontinuities[] ({ at, reason, detail }, empty when none), both desktop viewers draw a dashed marker line per event on the four Performance Trends charts, and the web trend panels carry the sentence as a notice; one shared helper in Collectors owns the model, the closed reason vocabulary, the two store-neutral SQL texts and the grammar's inverse (CollectorMeasurementNote.TryReadCount). Two SQL Server carriers that mark one epoch fold to one marker at the first observation; the specific reason word is derived only where collector_state still holds THIS event's pair, otherwise the marker says identity and why.
- **v3 plumbing for the PostgreSQL-target engine** ([#3691]) - The plan, kernel and memory families exist as vocabulary and inert stubs behind every shared switch (scorer, amplifiers, advice, anomaly composers, graph, collector, detector, baseline provider, tool rows), so lanes 27/28/32 fill file-disjoint partials and never touch a shared file. No operator-visible change: `analyze_server` returns the same payload; a collection caveat now counts 19 family reads.
- **PostgreSQL targets get a per-database size series and the host's memory** ([#3691]) - Darling schema 136: `collect.pg_database_size_stats` stores `pg_database_size()` per database hourly for a year (BYTES; NULL, never 0, where the role may not size a database, and the instance total goes NULL with it), and `pg_cpu_utilization` gains six byte columns from Performance Insights' `os.memory.*` counters on the same call — total, free, cached, buffers, active, and the Serverless v2 capacity × 2 GiB as configured memory. A stock PostgreSQL target has no row, which a memory consumer must read as unavailable. Nothing reads either series yet; the object-growth and memory-composition lanes are next.
- **PostgreSQL buffer-pressure cards say what the cache holds** ([#3691]) - A `PG_BUFFER_CACHE_PRESSURE` card now carries `pg_buffer_composition`: the latest `pg_buffer_usage` capture as pool fill, the ten largest resident relations, the index/heap/TOAST split, the dirty share and the cold share (relations the clock sweep evicts first), with the root fact's counts beside them, and the advice states the numbers and what they mean for sizing `shared_buffers`. No capture is a read sentence — a cadence gap, or `pg_buffercache` available / absent per `pg_extension_availability` — never an empty list. Before this the card had the miss rate and none of the composition.
- **A PostgreSQL pass now names the statement whose plan changed and what it cost** ([#3691] lane 27) - `PG_PLAN_REGRESSION` joins `pg_plan_capture`'s `plan_hash` flip to `pg_statement_stats`' stored per-call deltas split at the flip (opening plan vs closing plan), graded on the after÷before ratio behind a 50 ms per-call delta and a 20-call floor per side; `PG_PARAMETER_SENSITIVITY` names the skewed predicate column (`pg_qualstats` × `pg_column_stats.top_value_frequency`) behind a statement captured under three or more plans, an advisory card lifted to 0.6 only when the regression co-fires on the same statement; `ANOMALY_PG_PLAN_REGRESSION` baselines the server-wide per-call mean hour-of-week and folds onto the regression. Absence of a captured plan is "not slow enough to log", never "no plan"; `auto_explain` off and `pg_qualstats` absent are said as such. Every bar unmeasured (`threshold_lineage = 0`); no `CREATE INDEX` anywhere.
- **A PostgreSQL target's analysis pass measures CPU where no capacity percent exists** ([#3691] lane 28) - `PG_CPU_BURN_CORES` sums pg_stat_kcache's per-statement user+system+plan CPU into cores busy, graded only by `ANOMALY_PG_CPU_BURN` against the server's own hour-of-week routine (peak AND mean), naming the statement that burned it; `PG_CPU_DECOMPOSITION` says whether backend time was burning or waiting and becomes a finding only when the dominant side's own instrument co-fires. Where the extension is absent (every Aurora cluster today) the pass emits one honest `unavailable` fact with the reason - never a zero. Every bar unmeasured and said so.
- **PostgreSQL targets learn whether the memory configuration fits the host** ([#3691]) - The engine had no line between "what the config could allocate" and "what the box has". CONFIG_PG_MEMORY_OVERCOMMIT now sums shared_buffers + max_connections × work_mem × (1 + parallel workers) + autovacuum workers × maintenance_work_mem + wal_buffers against the window's smallest memory_total_bytes (Serverless scales), an advisory at 0.4 lifted only when host-memory pressure or temp spills co-fire; PG_HOST_MEMORY_PRESSURE grades the OS's sustained reclaimable share; a stock target reads "unavailable", never a guessed ratio.
- **A baseline series can be scoped to one statement** ([#3691]) - the hour-of-week baseline provider keyed a series on (server, metric) only, so a PostgreSQL bad actor could only be judged against the whole server's habit. `GetBaselineAsync` gains a keyed overload (server, metric, key) with a third resolve seam and a `$7` bind after the local-clock parameters, and the PostgreSQL provider declares per-queryid arms for a statement's share of the collection's execution time and its per-call mean. No operator-visible change yet — the seam the statement-scoped baselines (lane 34) stand on; the SQL Server pass is byte-identical.
- **A PostgreSQL operator now learns which large relation a statement reads sequentially under a selective predicate, how often, and what an index would cost** ([#3691] lane 30) - `PG_SEQ_SCAN_ADVISORY` walks the window's captured `auto_explain` plans for Seq Scan nodes with a Filter, joins each (statement, relation) pair to `pg_qualstats`' sampled selectivity and `pg_table_bloat_stats`' heap size, and grades 0.5 only when the predicate is selective (≥ 90 % filtered), the relation large (≥ 256 MiB) and the scan recurring (≥ 3 captures per observed hour — a lower bound under `auto_explain.log_min_duration`); lifted to 0.65 when the scanning statement is the window's bad actor or its plan regressed. The advice states the evidence first and names a candidate index only where pg_qualstats named the columns and every gate holds, with its write/storage cost, the "a new index can cause regressions elsewhere — test it" caveat, and its impact labelled an estimate. All bars unmeasured, `threshold_lineage = 0`.
- **Darling V137 / Lite v64: eight nullable columns so the store can say what Query Store captures, where a lone finding goes, and how full the plan dimension's TOAST file is** ([#3796], [#3712], [#3783]) - One rung on three tables that already exist, no new table and no new hypertable (the count stays 72). `query_store_health` gains `query_capture_mode` and `wait_stats_capture_mode` in the DMV's own `*_desc` spellings, with the collector's first per-column version gate behind them — on a 2016 engine a body naming the wait-stats column fails to COMPILE for the whole database, no row at all rather than one NULL cell, so the SELECT carries it only where the engine has it and the stored NULL means "the engine predates the option", never `OFF`. `config_alert_settings` gains `analysis_uncorroborated_route`, a nullable tri-state where every numeric knob on that row ships a default, because NULL is "not set in the store — the file-level knob or its shipped `digest` governs", which is what every store reads the morning after the upgrade; it carries a CHECK where the numeric rungs deliberately did not, since a number has a clamp and a route has none — a misspelling read as "no opinion" would silently turn a decision to PAGE back into the digest. `store_metrics` gains `toast_bytes` (written from this rung on, dimension rows only) beside `toast_live_bytes` left NULL by a measurement rather than a shrug — the tuple-share proxy reads 100 % on exactly this shape where the truth is 47.9 %, and the honest instrument needs an extension the bundled image ships and does not install — and three checkpointer columns for a row the code half writes. Read by nothing here, by design, with a census holding each consumer lane to releasing its clause deliberately.
- **Nothing in the product could say which databases were cluttering Query Store, why, or what it cost the servers hosting them — while every input for that answer was already in the store** ([#3797]) - `get_query_store_clutter` composes four facts that each lived on their own surface and were never put beside each other. The fan-out rollup on `collection_log` says which database the `query_store` collector spends its time in: how often it was the run's slowest item, that item's discrete median and p95, its share of the whole pass, and this database's median cost against the pooled median of every other database on the same server. The raw rows' `plan_id` — the identity `MAX_PLANS_PER_QUERY` caps, and an index-only walk where a shape hash would fetch every heap row and collapse the parameter-sniffed recompiles that ARE this churn — says how many plans a query carries, how many were seen exactly once, and how fast new ones arrive. The newest health capture per database says what the store is configured to keep. And the `QDS_*` wait deltas with the Query Store memory clerk say what Query Store costs the instance, rated only over rows carrying a measured interval, with the restart rows and the pre-interval rows counted beside the rate rather than divided into it. Not a new query: an analysis of what was already collected. The composite is deliberately the BAND and its reason tokens rather than a number — a weighted sum of a share, a percentile and a fraction would have to invent the weights, and the first operator to ask why a database is a 7.3 could not be answered from the row — with every bar published under `thresholds` so a reader can disagree on the evidence, and rows ordered by band, then read-cost share, then plans per query. Unknown means unmeasured and never escalates. A readable secondary is excluded by architecture with the reason on the row, because its read cost and churn are its primary's. The fleet reference is opt-in, because it walks two raw hypertables fleet-wide and a default call that times out is a tool nobody uses.
- **`get_query_store_health` said "Query Store health" and skipped the one knob that names a plan-churn factory** ([#3796]) - V137 stored `query_capture_mode` and `wait_stats_capture_mode` and then nothing read them, so the tool described itself as per-database Query Store health while answering every question about the row except what the store CAPTURES, and both Query Store grids showed nine knobs of ten. Both SKUs' rows now carry the two as their TRAILING fields with the `capture_time` stamp after them, so a client that indexed the row by position still finds its ten fields where they were; both are `string?` end to end where the row's other strings coalesce an absence to the empty string, because a NULL here is an observed state — the row predates the rung, or the engine is 2016, where the wait-stats mode does not exist — and coalescing it would let a reader confuse "never asked" with a value the DMV never emits. Both desktop grids and the web tile gain `Capture Mode` and `Wait Stats Capture`, rendering the absence as the page's own em dash and never as `OFF`. The description, byte-identical on both SKUs, says what each mode costs: `ALL` captures every query the engine compiles, ad hoc one-offs included, each distinct text a new query with a new plan on the way to the cap; `AUTO` skips the insignificant ones on the engine's own thresholds; `CUSTOM` says the thresholds were tuned but not to what, since the row does not collect them; `NONE` stops capturing new queries while runtime stats keep flowing for the ones already held. No verdict on the value — that is the clutter view's arm.
- **The store could say how big its plan dimension's TOAST file was and not how full, and could not see its own checkpointer at all** ([#3783]) - The plan XML and statement text do not live in a dimension's heap, and `pg_total_relation_size` says how big that file is and nothing about how full it is, so a dimension cycling hundreds of thousands of rows a day through row-capped deletes sits at its high-water mark and the store reports the slack as size. `get_store_metrics` now publishes `toast_bytes`, `toast_live_bytes`, `toast_utilisation_pct` and a `toast_utilisation_note` on every dimension row and daily point, and the sweep writes one `checkpointer` row per run carrying the server's CUMULATIVE write-phase, sync-phase and requested-checkpoint counters (`pg_stat_checkpointer` on 17+, `pg_stat_bgwriter`'s older names below it) which the read differences into an interval whose length is measured between the two stamps rather than assumed from the cadence, with a four-state status so one row, a counter that went backwards or an absent view is SAID rather than clamped to a zero or a negative. Live bytes are NULL on the shipped store by measurement, not by shrug: the tuple-share proxy reads 100 % where the truth is 47.9 % on exactly this shape, because after a `VACUUM` the dead tuples are gone and the file keeps its pages, and the honest `file − sum(avail)` reading needs an extension the bundled image ships and does not install — a product-dependency decision — so the note names the one statement that lights the whole surface with no release, and the sweep fills the column the moment it is there, from the row's own recorded size so the quotient stays internally consistent. Nothing is ever computed from `total_bytes` or from tuple statistics. Two informational self-alerts follow, and they are conditions rather than actions: Store TOAST Slack (a dimension file over 10 GiB at a MEASURED utilisation under 50 %, dormant wherever live bytes are NULL, re-fired daily because the fact moves on a scale of days, cleared by the rebuild or by the floor) and Store Checkpointer Pressure (an observed sync phase past the read deadline the kills were measured against, or any WAL-forced checkpoint, naming the WAL-sizing and refresh-slicing levers). Neither reclaims anything, at any tier.
- **The uncorroborated-finding route knob said FILE-LEVEL, edit darling.json and restart, while the rung had already given it a store column nothing read** ([#3712]) - #3732 shipped the corroboration gate with one knob and shipped it file-level — read once at service start, refused by name on `update_alert_settings`, invisible to the Viewer, and reported only as whatever the running host had published off its file — because two rungs were in flight and the issue said so. V137 landed the column and left it written by nothing and read by nothing. One resolver now decides for every surface, and its precedence is stated: a parseable value in the store wins over the file, NULL there defers to darling.json, and a file value that is neither spelling falls to the shipped `digest`. The store wins because the operator who flips the combo or calls the write tool expects the change to take effect without a restart, a file edit on the service box, or knowing which of two places holds the losing value — and the store is the surface this product already hot-reloads, so a route written there is live within one beacon tick. The seed leaves the column NULL on purpose: seeding the file's value in would make the store win from the first start on every install, so the file could never govern again and the tri-state's third state would exist in the schema and be reachable on no store. The two config members stay distinct for the same reason — "over file" needs the file's value to still exist when the column is NULL, including the moment an operator clears it back. A value the CHECK is supposed to make impossible is ignored and logged with the value, the home that decided and the route — never obeyed. `get_alert_settings` publishes the EFFECTIVE route, never null, beside `uncorroborated_route_source` (`store` / `file` / `default`); `update_alert_settings` writes `digest` or `page` in the CHECK's canonical spelling, writes SQL NULL for an explicit JSON null (the only way back to the third state), refuses anything else naming the three accepted spellings, and warns when a whole round-tripped payload moves the decision into the store and how to move it back; and Settings → Notifications → Automated Analysis gains the tri-state combo whose third item is the service's darling.json value, with Restore Defaults landing on it rather than on `digest`. Lite has one home, keeps its checkbox, and deliberately publishes no source key it could only answer as a constant.
- **A PostgreSQL operator now learns which database is growing, how fast, and when it doubles** ([#3691] lane 38) - `PG_DATABASE_GROWTH` trends the hourly `pg_database_size_stats` series (V136) over fourteen days — never a spot size — grading the WEAKER of two ramps above an AND gate (≥ 1 GiB and ≥ 10 %; 10 GiB / 25 % critical), naming the database, the bytes, the fraction, the slope per day and a straight-line days-to-double stated as such; databases the monitoring role cannot size are counted, never read as zero growth; `ANOMALY_PG_DATABASE_GROWTH` judges the instance's growth rate against its own hour-of-week; growth chains onto a bloat trend on the same database as its first question. Disk free space is not collected and the card says so. Every bar unmeasured and says so.
- **The Query Store clutter view had no viewer and told every reader the capture mode was uncollected** ([#3797]) - The tool shipped with its config arm hard-coded to a null capture mode beside a note saying the column would land with the rung — which had already landed, is filled hourly by the shared collector and is published by `get_query_store_health` on both SKUs — so the clutter view was the last surface in the product telling an operator the mode was uncollected, and it said so inside the recommendation they would act on. The reader and the pure composition move to the shared storage project beside the other shared readers, for the reason that project exists: the web page reads the tool over HTTP and the WPF Viewer reads the store directly, and the alternative to one reader is a second copy of four statements carrying a window floor, a discrete-median definition that has to agree with `percentile_disc`, and a replica gate read off one engine bit — a copy that drifts on any of them calls a database Critical on one surface and Healthy on the other, and the copy that drifted is never the one being read. The config arm selects the column, publishes it with `capture_mode_known` as the AND over the rows so one pre-rung database makes the flag false rather than being averaged away, and publishes a NULL as never-asked and never as the engine's `NONE`, which is a mode a store can really be in; the one sentence both churn recommendations end on is written from the mode the row carries — `ALL` beside churn is the switch-to-`AUTO` case, `AUTO` points at the workload, `CUSTOM` is `AUTO` with thresholds this row does not collect, `NONE` means the churn is inside the queries already held, and anything else prints verbatim rather than being mapped onto a default — while no band moves, which is pinned so a capture mode cannot become a threshold nobody published. The web server page gains one fan-out on the Queries tab feeding three panels, its verdict cell coloured off the band the server computed with the reason tokens beside it so the colour is never the only evidence. The Viewer gains a Query Store Clutter sub-tab whose data service contains no SQL at all, every measured column bound as a nullable number with the absence rendered by the binding, because a `DataGrid` sorts on the binding's path and text columns would sort 9 above 1,234 on the one grid in the app whose purpose is to rank by magnitude — and it would look like a working sort. It takes no default sort, and that is the decision rather than the omission: the composition's arrival order IS the ranking, and the nearest single-column stand-in sorts the worst database to the bottom. A readable secondary stays in the grid on both surfaces with its exclusion and the architectural reason named, because dropping the row would leave an operator who knows the database exists unable to tell "not cluttered" from "not shown", and file the second as the first.
- **The three report alerts carry their rows as structure as well as prose, so every surface that can render a table has one** ([#3834]) - `Collector Cost Digest`, `Fleet Sweep Rollup` and `Analysis Singles Digest` are the three alerts that carry a table's worth of content - the collector digest names thirteen (server, collector) pairs and each line holds eleven fields - and all three reached the emit path as one prose string with `context: null`. The renderers that would display that well already existed and were already wired to something else: the Viewer's `AlertDetailWindow` binds `AlertContext.Details` as Heading/Label/Value pairs and falls back to the raw string in a box roughly 45 characters across when the context is null, the Teams card renders fact sets and has split them at the ten-fact ceiling since #3442, Slack renders field sections, and `{{context_json}}` carries structure to downstream automation - so the data was tabular, the surfaces could render tables, and the only thing in between was that these three alerts did not build a context. Three pure static builders now sit beside the three renderers and project the same figures into detail items: one per (server, collector) mover for the digest plus the heaviest-collector census that is #2862's whole catch, one per band transition, watch event and would-have-paged family for the rollup under a leading item carrying the covered window and its aggregate counts, and one per top mover and one per family for the singles digest, each carrying the KEY a page would have carried, the gate's routing reason and the #1140 dedup fingerprints - which is the point of structuring that one at all, because an operator promoting a single to a drill-down needs those identifiers by name and an automation reading `{{context_json}}` was previously parsing them out of a sentence. The prose is unchanged to the byte, and that is load-bearing rather than merely polite: it states that this is a REPORT and not an incident, that nothing is degraded and nothing needs doing before morning, and it names the materiality floor and the absence of a ratio factor, none of which a field list contains - and `AlertDetailText.ProseForDelivery` suppresses an alert's prose only when it is textually EQUAL to `Flatten()` of its context (the engine alerts, whose detail text IS their flattened context), so an essay can never trip that gate and the paragraph keeps delivering on every channel exactly as it did. That is pinned rather than assumed, because had the equality fired this change would have deleted the very sentence the issue went out of its way to keep. Every cap the prose applies is applied to the rows and a cap that binds says so in the structure - the census cut, the transition and watch-event cuts and the singles cuts each append the heading-only `+N more` remainder item the paragraph prints, so a surface rendering only rows cannot read the document as complete - and none of the three populates `Incidents`, because those keys are other findings' fingerprints quoted for lookup and an incident-carrying context would enter a once-a-day report into per-event splitting and the incident delivery filter, which are paging mechanisms these documents stay outside of. That last fact retires a comment in `DarlingAlertDeliverer.DeliverAndReportAsync` that had just become false ("the digest and the rollup fire with `Context: null`"): the invariant that actually holds is Details-but-no-Incidents, and the Per-event branch is gated on incidents rather than on a context existing, so it stays unreachable for them and their disposition is still always reported, which is what #3580's delivery stamps read.
- **A baseline's dispersion floor says in words that it does not cover a dead metric, a Flat-tier fact admits its distinct-day count is a ceiling proxy, and the three copies of the stamping helper are held to one key set** ([#3859] items 1–3) - Three residue findings from #3691 lane 41, all of them contracts that read as something they are not rather than defects in shipped behaviour. `EffectiveStdDev` and `EffectiveRobustSigma` each open with a zero-activity arm returning 0 and only then consult `AbsStdDevFloor`, while both doc comments described the floor as unconditional — so an all-zero bucket on a metric that carries one (memory 4.0, CPU 5.0) reports dispersion 0, not 4.0, and every reader of those comments believed otherwise. The RULING is that the ordering is right and the prose was wrong: the floor exists to spread a LIVE metric's dispersion so a variance-collapsed baseline cannot manufacture a 25σ event out of one ordinary point of movement, not to hide a dead one, and that ordering is the only reason #3849's zero-history arm is reachable on the bounded metrics at all — a floor consulted first hands the gate a dispersion no sample ever showed and the month of measured quiet scores against a number it never produced. No behaviour moves; both comments gain the exemption and the ordering gains a pin, because making the comments true by lifting the `Math.Max` above the zero check is a one-line change that looks like a correction, reds nothing else in the suite, and silently re-gates the arm on exactly the metrics it was written to reach. The pin asserts both reads return 0 under memory's real floor (read from `AbsStdDevFloorFor`, not retyped), that the bucket still reads `IsZeroHistory`, and — the other half, so this is an exemption and not the floor abandoned — that the same floor on a bucket with real movement under it still binds at 4.0. Second, `CollapseToFlat` has always admitted in a comment that its `MAX(DistinctDays)` is a ~5 CEILING (a calendar day recurs across the 24 hour buckets so summing would double-count; each (hour, dow) bucket holds at most about five same-weekday dates in a 30-day window) chosen over a second global DISTINCT-days query — but since lane 41 that number is stamped as `baseline_distinct_days` on every z-family fact and surfaced through `get_analysis_facts`, where a comment cannot travel and an operator sees "5 days" beside a sample count with no way to tell a ceiling from a count. The Flat tier now stamps `baseline_distinct_days_is_proxy = 1` beside it in all three live `AddBaselineContext` copies, conditioned on `BaselineTier.Flat` — the same signal the existing `baseline_tier` stamp writes, and the reason the flag can be trusted, since the Full and HourOnly tiers count their own days and an unconditional flag would say nothing about any of them. No existing key moves and no value changes, so the SQL Server pass is byte-identical on every non-Flat bucket and gains one key on the Flat ones, and the `CollapseToFlat` site now names the stamp it feeds because that is where whoever replaces the proxy with a real read will be standing. Third, those three bodies are hand-maintained copies that nothing pinned: lane 41 edited all three identically and got away with it, and the next such edit is the one that lands in two — which fails silently in the direction that costs, because the payload keeps its shape for the SKU that got the stamp and a reader of the other SKU's facts cannot tell a key that was never stamped from a condition that never held. The new delegation-equality census locates each declaration on a comment-stripped copy through the shared walker, reads the body back out of the ORIGINAL text over the same offsets (the keys are string literals, which the stripped copy has blanked), and compares the SET of stamped keys rather than the body text — the Darling copies carry extra comments on purpose and text equality would red on prose. Three floors stop it passing on nothing: a minimum body length, a per-file key count against the named contract, and an assertion that the expectation itself has not been emptied; the expected set is written out rather than derived from one of the bodies, which would have made the census a tautology on whichever file it read. Verified by mutation — dropping the new stamp from `PgTargetAnomalyDetector` alone reds it by name, which is the exact drift the stamp is exposed to. Items 4–5 of #3859 (the typed story path and the side-leaf payload) are a second PR, so this one does not close the issue.
- **A summary card can name its top few objects, not only its worst** ([#3691]) — `Fact` gains a typed `Ranked` list (capped at three; the first entry IS the fact's own object, pinned) so a card stops saying "two more, 3.1× for 5 h" without saying which. Both engines' `analyze_server` root fact and `get_analysis_facts` entries carry `ranked` (object, database, value, figures) only when two or more objects ride — every other card, and the whole SQL Server pass, is byte-identical. The autovacuum-disabled card names the other disabled tables, the bloat trends name their top three instead of hiding names inside metadata keys, and the vacuum-backlog card ranks two more tables beside the worst; the three encodings those cards used are retired. `Fact` is never persisted, so no migration.
- **`get_collection_log` can be asked for the failures** ([#3869]) - the log's single most common triage question had no filter: a caller hunting failures paged the newest-first tail, which on a busy fleet covers seconds per page, so a failure twenty minutes old was unreachable by the tool that exists to show it. Both SKUs' tools gain `status`, matched case-insensitively against the log's own nine-word vocabulary (SUCCESS, SKIPPED, YIELDED, ABANDONED, ERROR, PERMISSIONS, EXTENSION_MISSING, SESSION_MISSING, WARNING), promoted to one shared roster on the collector driver so the filter, its validation and its docs cannot drift apart. An unknown value is REFUSED naming the accepted set - the sibling of #3870's unknown-argument refusal, because an equality filter on a misspelled status returns an empty page a caller reads as "no failures" - and the applied value echoes as `status_filter` since `status` on a miss is the miss word. The filter applies in SQL before the row cap, so `run_count`/`truncated` describe matching rows, and both instruction tables name the third filter beside the two that existed.
- **`get_collection_health` now says when the analysis pass could not read one of its fact families** ([#3691]) — both SKUs carry `analysis_caveats` when a recent pass failed a family read: which families, how each read failed, and "failed on 6 of the last 6 passes" — the persistence figure that tells one timeout from a family dark all night. A different layer from the collector rows beside it (which say whether data was WRITTEN): forty-three healthy collectors can sit above a family the pass timed out reading, and until now the only record was one Warning line, because the scheduled sweep builds a fresh analysis service per pass and its caveats reached no readable surface. The `analyze_now` result carries the same sentence, composed with the window-empty one rather than replacing it. The ledger behind both is process memory by design — the last 24 passes per server, never persisted (persisting it would mean writing to the store the pass just failed to read), and every block states that a restart forgets, so an absent block never reads as "every pass was clean".
- **A `pg_server_config` row now says whose setting it is** ([#3691]) — `pg_settings` is the resolved view for the collector's own backend, so a database or role that overrode `work_mem` or `statement_timeout` with `ALTER DATABASE … SET` / `ALTER ROLE … SET` was invisible, and the `CONFIG_PG_*` facts graded a value those sessions never ran with. V138 adds `database_name` and `role_name` (nullable; NULL means server-wide), the collector unions `pg_db_role_setting` onto `pg_settings`, every server-wide read excludes the overrides (ten readers, pinned by anchor so the eleventh trips it) and one selects them, and `get_pg_server_config` publishes them as `database_overrides` — present only when the cluster has any, so a pre-V138 snapshot and "no overrides" both say nothing rather than `[]`. Disk free space is struck from the line: no in-engine SQL source exists on either engine.
- **`get_object_locking` says when its snapshot was collected** ([#3880], Erik's ruling on #3878's flagged call) - #3879's fix made the read visible to the latest-anchored census for the first time (its old per-name anchor sat in a CTE the scanner ignores - the defect had hidden it), and the lane rostered it on the shrink-only exemption list with the debt-made-visible argument. Erik ruled for the better end state: both SKUs' payloads now carry the snapshot's own `collection_time` stamp, projected on the row statement rather than re-read (a second MAX could stamp the NEXT capture), the sweep takes the read through its stamped arm, the roster entry is deleted so the shrink-only census resumes shrinking, and the live rename pin asserts the stamp against a real store.

### Changed

- **Report alerts link to their own page: the Fleet Sweep Rollup opens the sweeps page, and the digests carry no link** ([#4421])
- **`get_top_procedures_by_cpu` and the Top Procedures grid now route to the hourly rollup once raw ages past its retention window, instead of returning nothing** ([#4413]) - Mirrors the routing already shipped for `get_top_queries_by_cpu`; the payload discloses which tier answered and what an hourly-routed row is missing (`object_type`).
- **Sized the Linux compose store's background-worker slots and `work_mem` from the product's own
- **Cache the server-scoped watermark read across a runner's lifetime, cutting repeated `MAX()` reads against `job_history`, `default_trace_events`, `system_health_events`, and `memory_pressure_events` to one seed per (server, collector) pair instead of one per collection cycle** ([#4399])
- **Top-N-by-CPU queries now route to the hourly rollup once raw's retention has dropped the window, instead of returning empty** ([#4396]) - `get_top_queries_by_cpu` (MCP tool, Storage reader, and Viewer) degrades to the hourly continuous aggregate for a window older than raw's floor and discloses the precision loss (`tier_used`, `precision_note`) rather than silently returning nothing.
- **The Performance Trends Query Store duration chart reads the interval table for windows of 48 hours or more** ([#4382]) - the table measured faster than raw from 48 hours (361 ms vs 423 ms at 48 h; 374 ms vs 1,045 ms at 7 days) and returns exactly raw's points; shorter windows stay on raw. The interval-table gate also reads raw for any window that still holds legacy rows without an interval start.
- **The Darling viewer's TempDB file I/O trend now buckets like the File I/O tab's own reads** ([#4353]) - a 7-day window used to return one row per collection per tempdb file (40,324 rows measured for 4 files at 1-minute cadence); it now buckets to the chart's point budget (4,036 rows measured for the same seed) and still stamps a short window's raw points unchanged.
- **Raw hypertables now re-tune their own chunk interval once a day from actual ingest** ([#4344]) - Every raw hypertable used the same fixed one-day chunk interval whatever its write rate, so a busy table's open chunk, with its indexes, could outgrow the memory meant to hold it. Darling now reads each raw hypertable's compressed ingest rate daily, alongside a RAM-derived budget, and narrows the interval one rung at a time through `set_chunk_time_interval` when the store-wide total is over budget, and widens it one rung when the total would stay under half the budget; a new rung history table records every change, alongside a per-run WAL-volume record. On by default; set `rawChunkIntervalReconcileEnabled: false` in the config file to turn it off.
- **The Queries grid and MCP's Query Store top read use a per-interval table for long windows** ([#4341]) - both used to deduplicate the whole raw Query Store slice on every read. The grid now reads a table that keeps one row per interval for windows of 12 hours or more, and the MCP read for 24 hours or more. On a 15-day seed of 3.47 million raw rows, a 7-day window took 2,754 ms against 7,065 ms for the grid, and 1,627 ms against 9,249 ms for the MCP read. Shorter windows, and any window the table's history doesn't cover yet (after an upgrade), read raw as before.
- **Lite's memory clerk and File I/O trend charts bucket long windows in DuckDB** ([#4340]) - The memory clerk, File I/O latency, File I/O throughput and TempDB File I/O trend charts, and the Overview I/O lane, read one point per collection. They now gather collections into buckets sized to the window, at most 1,500 points per series. A bucket's latency is its summed stall over its summed operations, and its throughput is its summed bytes over its summed seconds. A window short enough that every bucket holds one collection plots each point at its own collection time, as before. The memory clerk picker's name list is cached for 15 minutes.
- **Lite keeps one DuckDB connection open, so reads attach to it instead of reopening the database file** ([#4339]) - Every read used to open and close its own connection to the local database file. One connection now stays open for the app's life, which cut a 20-tick, ten-way Overview read on a 1 GB store from 871 ms to 43 ms. Because that connection keeps DuckDB's buffer pool resident, a periodic check trims it after a heavy read: on a 1.1 GB store, one trim took process private bytes from 1,115 MB to 148 MB.
- **Lite's Performance Trends charts bucket long windows in DuckDB** ([#4338]) - The query duration, procedure duration and execution count charts read one point per collection, about 10,080 points each for a 7-day window at one collection a minute. They now gather collections into buckets sized to the window, rating each bucket as its summed work over its summed seconds, and a window short enough that every bucket holds one collection plots each point at its own collection time, as before.
- **Lite's Overview CPU, wait and memory lanes bucket long windows in DuckDB** ([#4337]) - The Overview tab's CPU, wait and memory lines, and the CPU and Memory tab charts, read one point per collection: about 10,080 points each for a 7-day window at one sample a minute. They now gather collections into at most 1,500 points per line, and a window short enough that every bucket holds one sample or collection plots each point at its own collection time, as before.
- **Settings the service manages now live in one included file, not stacked append blocks** ([#4336]) - the v1-v15 `postgresql.conf` append blocks are replaced by a single included file, `darling-managed.conf`, rewritten wholesale on every start. Existing stores migrate their current effective values into it on the first start after upgrade, without changing any value. An operator's own lines are kept, moved below the include so they keep winning, and settings set with `ALTER SYSTEM` are never changed. From then on, sizing values can change on the first start after an upgrade, and each change is logged with the inputs that produced it. A failed verification restores the previous settings files and raises the store-settings alert. Operator lines placed below the include are not yet carried across a major PostgreSQL version upgrade ([#4358]).
- **Darling viewer's Performance Trends charts bucket server-side over long windows** ([#4333]) - The query duration, procedure duration and execution count charts read one point per collection on the raw tier, about 10,080 points each for a 7-day window at one collection a minute. The store now gathers collections into buckets sized to the window, rating each bucket as its summed work over its summed seconds, and the chart title names the bucket width. A window narrow enough that every bucket holds one collection plots each point at its own collection time, as before.
- **Lite's Wait Stats and Perfmon charts bucket long windows instead of shipping one point per collection** ([#4331]) - A 7-day Wait Stats or Perfmon chart carried one row per stored collection, about 10,080 rows per wait type or counter at one collection a minute (about 200,000 for 20 wait types). It now buckets each wait type or counter to at most about 1,500 points (the chart budget, not the MCP tools' 200-point budget), and a window short enough that every bucket holds one collection still plots each point at its own collection time. The wait-type and perfmon-counter pickers also cache their name list for 15 minutes per server and window length instead of re-reading it on every 1-minute auto-refresh.
- **Darling viewer's File I/O and memory clerk charts load faster over long windows** ([#4329]) - the
- **Darling viewer's CPU, Overview wait and Overview memory charts bucket server-side over long windows** ([#4327]) - The CPU chart, and the Overview tab's total-wait and buffer-pool lanes, used to return one row per ring-buffer sample or per collection with no cap: about 10,080 rows each for a 7-day window at one collection a minute. Postgres now buckets all three to a fixed row budget, matching the sizing already applied to the Wait Stats and Perfmon trend charts. A window narrow enough that every bucket holds exactly one sample or collection still renders each point at its own exact timestamp rather than a bucket boundary, so short windows look unchanged.
- **PostgreSQL-target baselines recompute once a day instead of every hour** ([#4324]) - Every PostgreSQL-target baseline arm (13 unkeyed metrics plus the 2 statement-keyed ones) was recomputing its full 30-day window every hour; one of them cost up to 2.45 seconds and 262 MB of temp space per pass. They now share the once-a-day cache lifetime the two heaviest SQL Server baselines (Cpu, IoLatency) already used. Because a daily baseline covers history up to the start of the UTC day, a PostgreSQL target added today has no baselines, and its baseline-driven anomaly checks sit out, until the next UTC midnight.
- **Three legacy query-stats rollups stop refreshing** ([#4186]) - `query_stats_hourly`, `procedure_stats_hourly`, `query_stats_db_hourly` and their daily rollups keep the history they already hold. Their interval-honest successors take over. Reads older than the successors' own history still use them. New data goes only to the successors, so the store no longer spends refresh time on the old rollups. The raw `query_stats` and `procedure_stats` purge now waits until the successors and the old rollups together cover every raw row. After a long outage, the service fills the gap at startup, up to a day of it per start. Only then does the purge resume. `--backfill-rollups` fills it in one run. Existing compression jobs move to new hours once. Dashboards, alerts and queries do not change.
- **llms.txt and CITATION.cff now match the shipped product** ([#4157]) - llms.txt said 41 T-SQL collectors (it is 42), listed Azure SQL Database under Lite only (Darling supports it too), never mentioned Darling's 29 PostgreSQL collectors or its Windows/Linux and web-dashboard support, named only email and tray alerts (Teams, Slack, PagerDuty and generic webhooks also fire), and undercounted downloads (15,000+, not 4,300+). SentryOne is now named as SolarWinds SQL Sentry. CITATION.cff drops a version and release date no release step keeps current. A new test compares both collector counts in llms.txt against the collector catalog so they cannot go stale silently again.
- **The MCP tool list's size limit now matches its size** ([#4141]) - #3898 moved reading guidance out of the tool list. The budget test's total limit now equals the tool list's measured size. On Darling that is 170,798 bytes, down from a limit of 329,494. On Lite it is 89,719 bytes, down from 148,216. Any later growth has to raise the limit on purpose. The window_truncated note on the time-series tools also drops its issue number, and now says the field was formerly named truncated.
- **get_spinlock_stats puts a short description in the tool list** ([#4127]) - Before this change, this Darling and Lite MCP tool put 597 and 1,306 characters into the tool list. It now serves a head of 595 characters on both, under the usual 620-character cap. The get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **analyze_query_plan puts a short description in the tool list** ([#4126]) - Before this change, this Darling and Lite MCP tool put 915 and 903 characters into the tool list. It now serves a head of 616 characters on both products, and the get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **get_pvs_stats puts a short description in the tool list** ([#4125]) - Before this change, this Darling and Lite MCP tool put 1,156 characters into the tool list. The tool now serves 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. The guide now also says that a database size of 0 leaves pct_of_database null, as the tool already did. Nothing was deleted, and no tool behavior changed.
- **get_deadlocks (Darling and Lite) and get_blocked_process_reports (Lite) put a short description in the tool list** ([#4123]) - Before this change, these MCP tools put up to 822 characters (get_deadlocks on Darling), 694 characters (get_deadlocks on Lite), and 1,340 characters (get_blocked_process_reports on Lite) into the tool list. get_deadlocks now serves the same 591-character head on both products, and get_blocked_process_reports serves a 483-character head, and the get_tool_guide tool returns the rest of the guide by tool name. Darling's get_deadlocks dedup_key parameter lost its reference to issue #1140 in the move. No other guidance was deleted, and no tool behavior changed.
- **get_long_query_completions puts a short description in the tool list** ([#4122]) - Before this change, this Darling and Lite MCP tool put 1,037 characters into the tool list on each product. It now serves a head of 604 characters, and the get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **get_memory_pressure_events puts a short description in the tool list** ([#4120]) - Before this change, this Darling and Lite MCP tool put 835 and 1,018 characters into the tool list on the two products. It now serves a head of fewer than 620 characters on both. The get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **get_latch_stats puts a short description in the tool list** ([#4119]) - Before this change, this Darling and Lite MCP tool put 1,027 and 1,372 characters into the tool list. It now serves a head of fewer than 620 characters on both products. The get_tool_guide tool returns the rest of the guide by tool name. The description also no longer says that high ACCESS_METHODS_DATASET_PARENT or FGCB_ADD_REMOVE latch waits mean TempDB allocation contention. Microsoft's documentation says the first synchronizes parallel operations and the second covers file add, drop, grow and shrink operations. The Dashboard's copy of the sentence gets the same fix. No tool behavior changed.
- **get_index_usage puts a short description in the tool list** ([#4118]) - Before this change, this Darling and Lite MCP tool put 856 and 493 characters into the tool list. The tool now serves a head of 488 characters on both products, and the get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **analyze_procedure_plan puts a short description in the tool list** ([#4117]) - Before this change, this Darling and Lite MCP tool put 926 and 912 characters into the tool list. The tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **validate_custom_view and run_custom_view_panel put a short description in the tool list** ([#4116]) - Before this change, these two Darling MCP tools put 1,576 and 1,084 characters into the tool list. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **analyze_plan_xml puts a short description in the tool list** ([#4115]) - Before this change, this Darling and Lite MCP tool put 880 characters into the tool list on both products. It now serves a head of 618 characters, and the get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **get_ag_health and get_store_query_stats put a short description in the tool list** ([#4114]) - Before this change, these two Darling MCP tools put 1,529 and 984 characters into the tool list. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **Claude Code keeps the MCP entry tools loaded even when it defers the rest** ([#4113]) - list_servers, get_fleet_overview (Darling only), analyze_server and get_tool_guide now carry anthropic/alwaysLoad in their tool metadata. A Claude Code session that defers other tools to save context still sees these four at session start.
- **get_query_store_regressions puts a short description in the tool list** ([#4109]) - Before this change, this Darling and Lite MCP tool put 1,187 characters into the tool list on both products. The tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **get_blocking and get_pg_replication_slots put a short description in the tool list** ([#4108]) - Before this change, these two Darling MCP tools put 1,645 and 803 characters into the tool list. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. No guidance was deleted, and no tool behavior changed.
- **get_active_queries puts a short description in the tool list** ([#4107]) - Before this change, this Darling and Lite MCP tool put 1,406 and 1,421 characters into the tool list. It now serves a head of fewer than 620 characters on both. The get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **get_plan_corrections puts a short description in the tool list** ([#4105]) - Before this change, this Darling and Lite MCP tool put 1,672 characters into the tool list. It now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **get_query_store_duration_trend and get_perfmon_trend put a short description in the tool list** ([#4101]) - Before this change, get_query_store_duration_trend put 1,537 characters (Darling) and 1,276 characters (Lite) into the tool list, and get_perfmon_trend put 1,057 characters into the tool list on both products. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **get_resource_semaphore and get_memory_grants put a short description in the tool list** ([#4100]) - Before this change, these two Darling and Lite MCP tools put 1,310 and 988 characters into the tool list. Each tool now serves a head of 617 and 556 characters. The get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **get_daily_summary and get_daily_summary_range put a short description in the tool list** ([#4099]) - Before this change, these two Darling and Lite MCP tools put 1,414 and 1,903 characters into Darling's tool list, and 1,034 and 1,474 characters into Lite's. Each tool now serves a head of fewer than 620 characters on both products, and the get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **get_query_store_health and get_default_trace_events put a short description in the tool list** ([#4095]) - Before this change, these two Darling and Lite MCP tools put up to 2,587 and 866 characters into the tool list. On both products, each tool now serves a head of fewer than 620 characters. The get_tool_guide tool returns the rest of the guide by tool name. The heads also state what an empty or unavailable answer means for each tool. Two issue numbers and a story about one production store were removed from the text of get_query_store_health. No guidance was deleted, and no tool behavior changed.
- **get_pg_kernel_stats, get_pg_xmin_horizon and get_pg_table_bloat put a short description in the tool list** ([#4093]) - Before this change, these three Darling MCP tools put 1,298, 1,294 and 1,191 characters into the tool list. Each tool now serves a head under 620 characters (613, 590 and 605). The get_tool_guide tool returns the rest of the guide by tool name. The heads also state what an empty answer means for get_pg_kernel_stats and get_pg_table_bloat, which the old descriptions left out. Two long parameter descriptions also got shorter, and their full text moved to the guide. The limit parameter of get_pg_kernel_stats went from 204 to 180 characters, and that of get_pg_table_bloat went from 270 to 187. Nothing was deleted, and no tool behavior changed.
- **get_pg_io_trend, get_pg_database_trend and get_pg_cpu_utilization put a short description in the tool list** ([#4092]) - Before this change, these three Darling MCP tools put 1,596, 1,373 and 1,341 characters into the tool list. Each tool now serves a head of 620 characters or fewer, and the get_tool_guide tool returns the rest of the guide by tool name. The backend_type parameter of get_pg_io_trend went from 205 to 86 characters, and the rest of its text moved to the guide. get_pg_cpu_utilization's description also corrects a wrong claim. It said the tool covered both Aurora and RDS, with only a self-hosted server returning empty. The collector behind this tool runs only on Aurora, so a plain RDS server was never covered either. The description now says that a plain RDS server and a self-hosted server both report not_collected, not empty. No guidance was deleted, and no tool behavior changed.
- **get_pg_column_stats, get_pg_replication_stats and get_pg_predicate_stats put a short description in the tool list** ([#4091]) - Before this change, these three Darling MCP tools put 1,105, 925 and 841 characters into the tool list. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. The heads also state facts that the old descriptions left out. get_pg_column_stats returns the newest capture for each column, and a window shorter than a day can be empty because the collector runs daily. get_pg_predicate_stats reports filtered_pct as a percent that is null when no rows were evaluated, and its queryid is a string. Nothing was deleted, and no tool behavior changed.
- **The Darling service account can no longer change its own program files** ([#4090]) - The install and upgrade scripts gave the service account Modify rights on the whole install folder. It now gets Read & Execute there. It keeps Modify only on the bundled PostgreSQL runtime folders (pg-runtime and pg-runtime-prev) and, when you bring your own PostgreSQL, on the darling-keys folder. A compromised service or database process can no longer replace the service's program files to survive a restart.
- **mute_analysis_finding and audit_config put a short description in the tool list** ([#4088]) - Before this change, these two Darling and Lite MCP tools put as much as 1,444 and 932 characters into the tool list. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **get_pg_blocking, get_pg_wait_stats and get_pg_index_usage put a short description in the tool list** ([#4087]) - Before this change, these three Darling MCP tools put 1,606, 1,483 and 1,471 characters into the tool list. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. Two long parameter descriptions also shrank with no loss of meaning: get_pg_wait_stats's limit went from 214 to 190 characters, and get_pg_index_usage's limit went from 257 to 93. Nothing was deleted, and no tool behavior changed.
- **get_pg_plan_capture_readiness, get_pg_plans and get_pg_session_states put a short description in the tool list** ([#4086]) - Before this change, these three Darling MCP tools put 1,950, 1,374 and 1,617 characters into the tool list. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. The description of each tool's limit parameter is also shorter: 389 to 158, 235 to 72 and 282 to 117 characters. get_pg_plans' query_id parameter is shorter too, 320 to 88. Nothing was deleted, and no tool behavior changed.
- **get_query_duration_trend and get_procedure_duration_trend put a short description in the tool list** ([#4085]) - Before this change, these two Darling and Lite MCP tools put up to 1,180 characters into the tool list. get_query_duration_trend served 1,172 characters on Darling and 1,136 on Lite. get_procedure_duration_trend served 1,180 characters on both. Each tool now serves a head of 614 or 445 characters on both products. The get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **get_analysis_facts and get_analysis_findings put a short description in the tool list** ([#4083]) - Before this change, these two Darling and Lite MCP tools put 1,825 and 2,875 characters into the tool list. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. The description of get_analysis_facts's source parameter is also shorter (515 to 180 characters), and so is get_analysis_findings's include_drilldown parameter (235 to 122 characters). get_analysis_findings' description now also says that its truncated and truncation_note fields can mean its occurrence counts are undercounted. get_analysis_facts' guide now also says what its total_facts, shown and filters fields count. get_analysis_findings' head and tail also lost their two issue-number references. The guardrails they carried stay. get_analysis_facts carried none to begin with. No tool behavior changed.
- **get_pg_database_stats, get_pg_autovacuum_health and get_pg_top_queries put a short description in the tool list** ([#4082]) - Before this change, these three Darling MCP tools put 2,361, 2,215 and 2,210 characters into the tool list. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. The description of each tool's limit parameter is also shorter: 229 to 175, 261 to 161 and 228 to 163 characters. get_pg_autovacuum_health's description now also says that its disabled, past-threshold and growing counts cover only the returned page, not the whole server. No guidance was deleted, and no tool behavior changed.
- **get_pg_logging_audit, get_pg_io_stats and get_pg_wait_sampling put a short description in the tool list** ([#4081]) - Before this change, these three Darling MCP tools put 2,086, 2,045 and 2,032 characters into the tool list. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. The description of get_pg_io_stats's limit parameter also went from 235 to 194 characters, with no loss of meaning. get_pg_logging_audit's tail also lost its parenthetical issue numbers (#3601, #3602 and #3603). The sentence still says PLANNED without naming them. No tool behavior changed.
- **validate_custom_alert_rule, test_custom_alert_rule, update_custom_alert_rule and list_custom_alert_templates put a short description in the tool list** ([#4079]) - Before this change, these four Darling MCP tools put 2,078, 922, 890 and 751 characters into the tool list. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. The descriptions of update_custom_alert_rule and create_custom_alert_rule now say that enabling a rule is refused once 100 rules are enabled across the fleet. The guide of test_custom_alert_rule now says that a rule with no in-scope servers returns the status no_in_scope_servers. Nothing was deleted, and no tool behavior changed.
- **get_alert_history, set_notification_route_enabled, create_mute_rule and set_mute_rule_enabled put a short description in the tool list** ([#4077]) - Before this change, get_alert_history put 4,740 characters into the tool list on Darling and 3,573 on Lite. The three Darling-only write tools put 841, 959 and 1,477 characters. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. The description of get_alert_history's include_dismissed parameter also went from 360 to 114 characters on Darling and from 357 to 111 on Lite, and the text it lost moved to the guide. get_alert_history's tail also lost its issue-number references on both Darling and Lite. The sentences that carried them are otherwise unchanged. The three write tools carried none to begin with. No tool behavior changed.
- **get_pg_extensions, get_pg_write_stats, get_pg_server_config and get_pg_server_config_changes put a short description in the tool list** ([#4078]) - Before this change, these four Darling MCP tools put 1,727, 1,509, 2,252 and 1,146 characters into the tool list. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **get_collector_stall_probes, describe_custom_view_catalog and get_sweep_reports put a short description in the tool list** ([#4074]) - Before this change, these three Darling MCP tools put 2,678, 2,589 and 2,274 characters into the tool list. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. The guide of get_collector_stall_probes lost one example of a single measured stall. Nothing else was deleted, and no tool behavior changed.
- **analyze_server and compare_analysis put a short description in the tool list** ([#4073]) - Before this change, these two MCP tools put 4,160 and 1,647 characters into the tool list, on both Lite and Darling. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. The description of compare_analysis's baseline_hours_back parameter also went from 211 to 139 characters, and the sentence it lost moved to the guide. Nothing was deleted, and no tool behavior changed.
- **get_fleet_overview, add_servers and remove_server put a short description in the tool list** ([#4071]) - Before this change, these three Darling MCP tools put 3,287, 3,285 and 1,784 characters into the tool list. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. No guidance was deleted, and no tool behavior changed.
- **get_pg_log_events, get_pg_index_bloat and get_pg_deadlocks put a short description in the tool list** ([#4068]) - Before this change, these three Darling MCP tools put 4,199, 3,491 and 2,839 characters into the tool list. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted, and no tool behavior changed.
- **get_query_store_clutter and get_oversized_plan_backlog put a short description in the tool list** ([#4067]) - Before this change, these two Darling MCP tools put 5,147 and 2,997 characters into the tool list. Each tool now serves a head of about 610 characters, and the get_tool_guide tool returns the rest of the guide by tool name. No guidance was deleted, and no tool behavior changed.
- **update_alert_settings and update_mute_rule put a short description in the tool list** ([#4066]) - Before this change, these two Darling MCP tools put 8,123 and 2,530 characters into the tool list. Each tool now serves a head of fewer than 620 characters, and the get_tool_guide tool returns the rest of the guide by tool name. Nothing was deleted. The heads keep the facts that a caller needs to change settings safely. Both tools make partial updates. In update_alert_settings, one invalid field means that no field is written. The retired poison_wait.threshold_ms setting does not change when the alert fires. health_bands and fleet_sweep are not alert families. In update_mute_rule, a rule with every scope field cleared mutes every alert. The reading guides of get_alert_settings and get_notification_routes also lost their issue numbers, on Lite and Darling.
- **get_collection_log and get_query_store_top put a short description in the tool list** ([#4065]) - Before this change, get_collection_log put its whole reading guide into the MCP tool list. The description was 6,995 characters on Darling and 4,405 characters on Lite. The tool list now gets a head of 606 characters, and the get_tool_guide tool returns the rest of the guide by tool name. get_query_store_top now serves the same head on both products. That head states that only Darling marks a window floor with window_truncated. Three parameter descriptions of get_collection_log were longer than 200 characters. Each one keeps its main fact, and the rest moves to the guide under the parameter name. Nothing was deleted.
- **Three self-monitoring MCP tools put a short description in the tool list** ([#4064]) - The tools are get_store_metrics, get_store_log and get_collector_cost. They are Darling only. Before this change, each tool put its whole reading guide into the MCP tool list. Each description is now split the same way as the earlier system_health and data-read tools. The tool list keeps the facts that a caller needs to read the answer correctly. get_store_metrics and get_store_log report on the monitoring store itself, not on a monitored SQL Server. get_collector_cost reports what the collectors cost, not the health of a server. A daily size point is the last snapshot of the day, not its peak. An empty job-history block can mean that the reader has no visibility, not that nothing ran. For query_store, most of the time that get_collector_cost reports is the store's own probe and write work. The get_tool_guide tool returns the rest of the guide by tool name.
- **get_collection_health's tools/list entry drops from 21,026 characters to 583** ([#4063]) - This was the largest tool description an MCP client had to load before asking a question, on both Darling and Lite. No guidance is lost. The guardrail facts a caller needs stay in the short entry every client loads: a stopped collector is not a failing one, a zero rows-stored count is not automatically a fault, last_error can be stale, a regression is counted separately from a failure, and the alert-read-health block covers a different window than the rest of the payload. The rest of the reading guide moves to get_tool_guide, available on request.
- **get_alert_settings and get_notification_routes serve a short head, with the rest available from get_tool_guide** ([#4061]) - These two MCP tools served 7,530 and 1,746 characters on Darling, and 3,954 and 600 on Lite, before the first question. Both now serve a head under 610 characters on both products, with a pointer to get_tool_guide for the full text. Nothing was deleted. The head keeps the facts a caller needs to read the answer correctly. For example, one cooldown gates whether an alert fires, and a separate one bounds the resulting post. It also says what an unseeded store or an empty knob list means.
- **Seven data-read MCP tools put a short description in the tool list** ([#4055]) - The tools are get_wait_stats, get_file_io_stats, get_perfmon_stats, get_server_properties, get_top_queries_by_cpu, get_top_procedures_by_cpu and list_servers. Before this change, each tool put its whole reading guide into the MCP tool list, on both Lite and Darling. Each description is now split in the same way as the system_health tools. The tool list keeps the facts that a caller needs to read the answer correctly. For example, a page holds only the heaviest rows, and a null latency is not 0 ms. Also, the min and max CPU values cover the whole life of the plan in the cache. Offline means different things on Lite and Darling. The get_tool_guide tool returns the rest of the guide by tool name.
- **MCP clients load less context up front, and a new get_tool_guide tool serves the long-form guidance on request** ([#4048]) - Every tool description went into the MCP tool list in full, so a client that loads the whole catalog paid for all of it before the first question (about 330 KB on Darling). A description can now split into a short head, which the tool list serves, and a reading guide, which get_tool_guide returns by tool name or cross-tool topic, on both Lite and Darling. The nine system_health tools are converted first, at about half their previous size. Heads keep the facts a caller needs to read the answer correctly, such as which events are filtered out and what an empty answer means. A size check keeps the tool list from growing back.
- **Darling's MCP server instructions drop from 88,532 to 7,992 characters and Lite's from 52,942 to 7,721, both now under a pinned 8,000-character budget** ([#4039]) - Phase 2 of #3898: the per-tool "Tool Reference" tables and prose that used to restate what each tool's own tools/list description already says are gone, because nothing enforced that the two copies stayed in sync. What stays in the instructions is what no single tool's description can say by itself: the read-only preamble, the status-word vocabulary (empty, unavailable, not_collected, precondition, error, invalid), the as_of/hours_back anchor rule, and a one-line tool census. Every fact the old tables carried that a tool needs to be used correctly was checked against where it landed. get_collection_health's analysis_caveats clause, get_ag_health's per-replica-column trap, get_blocking/get_deadlocks/get_deadlock_detail's dedup_key scoping, and get_sweep_reports's and get_query_store_clutter's family facts all moved into the tool's or parameter's own description. Several others (add_servers, remove_server, mute_analysis_finding, compare_analysis, the plan tools' create_statement caveat) were already fully stated there, so the instructions row was pure duplication. get_memory_pressure_events's description pointed at an instructions section this phase renamed and gutted. It now states its own follow-up-tool guidance instead of pointing anywhere. Both SKUs' tools/list grew by under 400 bytes total from the handful of relocated facts, negligible next to the instructions cut.
- **PostgreSQL log messages show as PostgreSQL wrote them, and the SQL in them is normalized with every literal replaced by ?** ([#3996]) - Regex masking of free-form log prose kept missing shapes: RAISE text, libxml lines, a value typed after a noun, and a command's stderr. So it is removed from the store's own log (`get_store_log`) and from every PostgreSQL target's log events (`get_pg_log_events` and both viewers' Log Events panels). Messages, DETAIL and CONTEXT prose, HINT and LOCATION now appear exactly as the server logged them. SQL is still read by the lexer, and withheld when it cannot be read to its end. That covers STATEMENT and QUERY lines, slow statements, a crash's or a deadlock's queries, a function's SQL frames, a postgres_fdw remote command, a statement's bound parameters, and PL/pgSQL `print_strict_params` values. An auto_explain plan logged at WARNING keeps only its duration line. The review also closed two ways one statement's text could decide how another's was read. A deadlock query cut inside a literal no longer pairs its quote with the next process's query, which could show that query's literal as a plain word. A COPY value shaped like a CONTEXT frame can no longer swallow the real frame after it. Store-log rows now count messages that differ only in a quoted value or a number as one. The existing hourly pass brings rows stored by earlier releases to these rules. A security review then closed more. The token a syntax error quotes is read as SQL in every language PostgreSQL's catalogues write it in, not only English: Spanish, Indonesian and Japanese keep the ERROR label in English, so their syntax errors are stored. An unterminated literal or a jsonpath token there is withheld. A deadlock report cut between its lines withholds every query after one that cannot be read to its end, unless the entry proves the report whole. PL/pgSQL parameter lists and portal parameter lists must read back as name and masked-value pairs, or they are withheld. Under a translated `lc_messages`, a log line whose field label this reader does not know is withheld, with the rest of its entry, rather than kept as prose, and the Turkish and Korean labels written without spaces are labels whatever follows their colon. Such a line never opens an entry from inside a statement's literal. The same applies to a `log_line_prefix` that renders a label-shaped token of its own, such as an application name ending in a colon and two spaces. Reading a deadlock report of hundreds of waiters, or a CONTEXT of a thousand frames, no longer allocates over 100 MB. `get_pg_log_events` no longer returns `raw_line_hash`, an unkeyed hash of the raw line that a reader could use to test guesses at a hidden value.
- **The shared `as_of` description is 167 characters instead of 379, cutting Darling's `tools/list` by 22,040 characters (6.4%) and Lite's by 14,246 (8.8%)** ([#3965]) - It is the `as_of` parameter on 91 Darling and 56 Lite tools, so at 379 characters it was a tenth of Darling's catalog, paid on every request by every client that loads the full tool list. The short text keeps the rule a caller acts on: the anchor is the window's END, a bare date is 00:00 UTC, omitting it means now, and a past incident is `as_of` at its end, never a wider `hours_back`; the reason stays in both SKUs' server instructions. The days twin (416 → 166), the `window_truncated` clause (441 → 254, keeping its WIRE CHANGE sentence until the rename ships) and the `discontinuities[]` sentence (489 → 317, now naming all six reason words) are cut the same way. No description grew, `get_procedure_duration_trend` drops back under the 2,048-character cut Claude Code applies, and a new pin holds the rule's words, the 200-character parameter budget and the instructions' reason on both SKUs. Part of [#3898].
- **The review guard tells a pending workflow fix from hostile drift** - When `dev`'s `claude-review.yml` differs from `main`'s because a reviewed dev PR changed it and the release has not synced yet, the guard now warns "review pending release" and names the merged PR instead of failing every PR in the repo as if the file had been tampered with; unexplained drift still fails, and an origin the guard cannot determine fails closed.
- **The tempdb Space alert stops paging on a single collected sample** ([#3653]) - it fired on the first `tempdb_stats` row at or above the threshold and resolved on the next one under it, which on one production store class was 22 page/resolve pairs in 14 days about runs of 1-4 samples lasting at most 206 s. The arm now sits behind the shared persistence gate High CPU adopted in #3282: 3 consecutive breaching COLLECTIONS to fire (identified by the row's `collection_time`, so a 30 s sweep re-reading the last row does not count twice), the first collected sample under the bar to resolve, the open-incident bit persisted under its own metric name in the existing state table so a restart neither re-announces nor forgets it, and a tempdb reading that stops arriving freezes the gate instead of announcing "back to N/A". Blocking Wait Time and Long-Running Query await their rulings.
- **The analysis names the Agent job that is running long instead of counting it** ([#3653]) - the RUNNING_JOBS fact on both SKUs now carries the name of the job furthest past its own history (chosen among the running-long rows by percent of average, then duration), and the job card's headline, first sentence and remediation say which job — and "and N others" when several overran — where they used to send the operator to the Running Jobs view to find out. Findings persisted before this, and windows where jobs ran but none ran long, read exactly as before.
- **MCP tool failures were two wire shapes — a bare sentence on 214 tools and a JSON envelope on the PostgreSQL reads — and a `status`-keyed client read the sentences as successful text; now every tool on both SKUs answers a caught exception with one envelope through `McpHelpers.FormatError`** ([#3653]) - **WIRE CHANGE.** A tool that throws returns `{"status":"error","message":"Error during <tool>: …","hints":{"operation":"<tool>"}}` — the same envelope and serializer the four miss words use, the message text unchanged — including the frozen Dashboard twin, which links the same helper. The 30 PostgreSQL catches drop their "Reading X failed" dialect for the one grammar; the web surface maps the envelope to HTTP 500 (so PostgreSQL failures that passed through as 200 are 500 now) and keeps its `{"error": sentence}` body; the payload-contract census retires its two-shape inventory and fails by name on any catch that bypasses the helper. Validation refusals are unchanged and still bare sentences — a separate ruling.
- **PostgreSQL bars the fleet measured now say so** ([#3691]) - Every v1 bar the 2026-09-19 calibration of 50 Aurora PostgreSQL clusters validated (temp 1/10 MiB/s, backlog 3/10×/5, deadlocks 5/20, bad-actor busy floor 0.05, wait rollups 0.20/1.0 and io 0.40/2.0, CPU 80/95, TPS floor 50 / fallback 500, CPU floor 40 / fallback 95, deadlock floor 1/h, wait fallback 500 ms/s) carries a measured comment naming its percentile, population and date, and the facts graded on them show `threshold_lineage = 1` in get_analysis_facts instead of calling the bar a judgment; sessions 0.8/0.9 stay engine-defined with the measured note (fleet max 10.3 % of ceiling). Still unmeasured and still saying so: wait standout bars and sampling floor, session COUNT floors, the ratio families' firing multiple, every co-fire boost, checkpoint pressure (not applicable on Aurora) and buffer cache. No bar value changed.
- **Anomaly detectors fired on one hot sample and fired more the longer the window was; the gate now judges the window peak AND the window mean, and I/O reads the pair like its siblings** ([#3653]) - Every z-score detector tested the window MAX against a per-sample hour×dow distribution, so a 24-hour anchored pass mechanically reported more anomalies than the 4-hour scheduled pass on identical behaviour. The shared AnomalyGate fires only when both the peak and the window mean clear the existing cutoffs (z on both when the baseline is trustworthy; the fallback bar on the peak and the magnitude floor on the mean when it is not) — no cutoff moves, the reported sigma stays the peak's, and the mean's sigma rides beside it in the story. I/O latency, the one family reading AVG alone, now reads MAX and AVG on both SKUs and reports the peak.
- **Confidence chooses the channel** ([#3712]) - a scheduled-analysis finding now earns a page by corroboration, never by severity alone: two or more facts in its chain, a matched co-fire check on its root, or one of the two by-construction stories. A lone uncorroborated fact at or above the notify floor is persisted, visible on the web and MCP surfaces the instant it fires, recorded in the alert history as `notification_type: digest` with the gate's reason (`routing` / `routing_reason` on `get_alert_history`), marked *Not paged* in Lite's Recommendations, and named once a day in Darling's new Analysis Singles Digest - but delivered to no paging channel; the day it gains corroboration it pages as a new firing. Measured on the first day the recalibrated engine ran on a large production fleet: forty-plus anomaly pages in seven hours at confidence 0.20-0.35, mostly true and redundant. One knob, `analysis.uncorroborated_route` (`digest` default, `page` restores the old behaviour): Lite's Settings → Alerts, Darling's file-level `analysis.uncorroboratedRoute`.
- **Blocking Wait Time paged on one snapshot and cleared on the next, and Long-Running Query had no way to stop reporting the same permanent background sessions forever; blocking now fires on one snapshot at 3× the bar or on 3 consecutive collections through the shared persistence gate, and long-running queries gain an opt-out knob seeded from the production read** ([#3653]) - Measured on one production store class, 97 of 102 Blocking Wait Time episodes were a single snapshot, so a plain consecutive gate would have dropped exactly the severe pile-ups; the fire names which arm admitted it (`single_snapshot_3x` / `consecutive_k3`), a quiet collector cycle starts a new episode, and a stale snapshot still clears. The Long-Running Query knob is two editable lists — program-name PREFIXES seeded with `SQLAgent - TSQL JobStep` and exact logins seeded with the two `NT AUTHORITY` service accounts, the classes a 7-day read of one large production store showed to be permanent background; the application's admin login is deliberately not excluded and named humans never are — applied inside the read ahead of the row cap, with the card counting the sessions each list removed. Darling's store column and both SKUs' `get_alert_settings` twins follow as rung V135.
- **PostgreSQL saturation now tells queueing from load** ([#3691]) - the connection-saturation finding is amplified when the session-count anomaly fired against the server's own hour-of-week baseline while the transaction-rate anomaly did not — more connections than this hour usually carries, doing no more work — and says so in the advice with the measured multiple. The parked raw TPS-trend co-fire is retired: the fleet calibration showed routine 20–50× in-window TPS bursts on every cluster, so that trend was noise.
- **A story's root card now names the leaves it consumed** ([#3691]) - When the greedy traversal walked a higher-severity root down to a leaf that named something — the application parked in a transaction, the lead-blocker session, the Agent job — the leaf rooted no card and its name reached no surface: the exit check's grep for the holder over `analyze_server` found nothing. The engine now records the identity-bearing hops on the story (highest severity first, at most three) and the root's investigation gains one "Behind it:" sentence per hop, on both engines; chains that name nothing are byte-identical.
- **The wait-profile anomaly judges the window peak AND the window mean** ([#3741]) - The last baseline detector still firing on the window peak alone goes through the shared pair gate #3724 gave the others: the all-types wait rate's window mean must clear the same heavy-tail 5.0 robust cutoff beside the peak, so one hot collection in a quiet window no longer reads as a profile shift. The no-baseline fallback keeps its peak-only absolute bar; the story now names both rates.
- **CONFIG_CHANGED now anchors on when RECONFIGURE ran, not when the snapshot first noticed** ([#3740]) - The finding's ±4 h compare anchors on the default trace's sp_configure line (msg 15457) when the store holds one for the changed option, the prose says "changed at … (default trace)" and the fact records which clock it used; with no matching line the observation anchor and its disclosures stand unchanged. The collector had been dropping that line before storage whenever sp_configure ran from master or an excluded database (the trace stamps the session's database context); it is now kept outside every database predicate, while the config-change event classes stay dropped and the snapshot diff stays the source of what changed.
- **MCP refusals carry a status word: the invalid envelope on both SKUs, and PostgreSQL refusals answer 400 not 500** ([#3739]) - Every validation bail — an unresolvable server, an hours_back or limit the tool cannot honor, a missing required parameter — now answers {"status":"invalid","message":…,"hints":{"parameter":…}} instead of a bare sentence, built once in the validators and resolvers. The web read surface passes it through as a 400 exactly as the mute-rule routes always did; nine PostgreSQL refusals that borrowed the error word stop answering 500. Wire change for MCP clients and the frozen Dashboard.
- **PostgreSQL wait standouts, the I/O admission floor and the ratio multiple carry the second fleet read's lineage** ([#3691]) - The 2026-09-20 calibration (50 Aurora PostgreSQL clusters) read per event, per quarter-hour and per hour-of-week ratio what the 2026-09-19 read could not: `WaitStandoutConcerning`/`Critical` (0.15/1.0 ≈ p99.6/p99.99 of non-IO event buckets), `IoBaselineBucketMinimumReads` (250; 57 % of fleet buckets under it, hour-of-DAY tier for half the fleet stated) and `PgRatioAnomalyThreshold` (3.0; well placed for TPS, routine alone for the wait rate where the floor and peak-AND-mean gate decide, unplaced for deadlocks) now say *measured* with that date. Every Aurora wait fact — standouts and yielded rollups included — reads `threshold_lineage = 1`; stock sampled wait facts, the deadlock-rate and wait-profile anomalies stay 0 (sampled population 0 on the fleet; SQL Server heavy-tail cutoff by reference; chosen ramp spans), each stamp saying why. No bar value changed.
- **The managed store's WAL ceiling is derived from the data volume's headroom, not v4's fixed 4 GB on every box** ([#3802]) - The bundled store's `max_wal_size` was one constant for every install (v4's `4GB`, not PostgreSQL's 1 GB default as the issue read), so a small box risked filling its data volume with WAL while a large one checkpointed more often than its disk asked for. The v12 conf block derives `max_wal_size` from free space on the data volume (free / 8, clamped to a 1–16 GB power-of-two ladder so the heal does not churn as free disk drifts), sets `min_wal_size` to a quarter of it (80 MB floor), pins `checkpoint_completion_target = 0.9` only where PostgreSQL is older than 14 (14 made it the default), heals on every start by stamp, and logs one INFORMATION line with the derived values and the free-disk figure. An `ALTER SYSTEM` override in `postgresql.auto.conf` for any of the three is logged with the `RESET` that would lift it, never fought — PostgreSQL's precedence, the `lc_messages` precedent.
- **Stock PostgreSQL's sampled wait profile fires its first-occurrence reading on the peak alone, like its Aurora and SQL Server twins** ([#3691]) - The `pg_wait_sampling` profile detector pair-gated its `is_new` arm on the window mean as well as the peak, so a young stock server whose sampler saw a full backend-equivalent of waiting for one cycle said nothing while the same numbers on Aurora fired. By the 2026-09-20 ruling the arm now takes the twins' peak-only bar; the robust and ratio arms keep the peak-AND-mean gate. Lineage unmeasured (population 0 on the fleet), revisit when a sampled cluster exists.
- **A PostgreSQL bad actor is graded against its OWN normal** ([#3691] lane 34) - The absolute share bars (0.25 / 0.60) were routine on the measured fleet — a single statement dominating a busy hour is the normal shape of a concentrated workload — so `ANOMALY_PG_BAD_ACTOR_SHARE` now judges each top statement's window share against THAT statement's hour-of-week share bucket (the per-key baseline seam, #3810) on the peak-AND-mean gate, and walks into the `PG_BAD_ACTOR_<queryid>` card it names; the card itself becomes a context band under the story line with the absolute share carried as measured-routine context. A statement at its usual 60 % is the workload; a statement at 55 % that usually takes 10 % is the story. The busy floor (measured) still admits.
- **Dependencies: `Microsoft.Data.SqlClient` and `Microsoft.Data.SqlClient.Extensions.Azure` to 7.1.0, `AWSSDK.PI` and `AWSSDK.RDS` to their next patch, with every lock file the bump reaches regenerated** - the driver and its companion packages share one aligned version from 7.0.2 onward, so the Entra extension package #3838 added has to move with the core package rather than behind it; `AssemblyVersion` is unchanged across the pair, so no binding redirect follows. The lock files are regenerated by restore rather than patched textually, which is what a grouped bump needs and what the group had previously got wrong.
- **`ANOMALY_PG_PLAN_REGRESSION` judges each plan-flipped statement against its own routine** ([#3691]) - the detector now reads each plan-flipped statement's OWN hour-of-week per-call mean (the keyed series #3810 made possible) and names ONE statement on `ObjectName`, folding onto `PG_PLAN_REGRESSION` only for the SAME `queryid` through one shared predicate behind both the amplifier and the edge; the server-wide mean is retained solely as the cold fallback (`series = 0`), whose advice now says it is a pointer, not a verdict. The third fleet calibration read (2026-09-21, 50 clusters, fenced before the .453 statement-stats regression) evidenced the switch: the server-wide ratio never reached 3.0 in a week while the keyed series carries the tail. Lineage round 3 cites `ANOMALY_PG_BAD_ACTOR_SHARE`'s 3.0 and the keyed mean-ms multiple as measured (≈ p99.1 / p99.3); no bar value changed.
- **The alert pass retries a store read once before counting it failed** ([#3848]) - A read that crosses the alert pass's ten-second store deadline is now retried once, two seconds later, on a command timeout and nothing else - never on an error the store returned, and never during shutdown. The week's alert-read kills were all sparse transients inside the store's own write bands, where the next pass thirty seconds later ran into the same band still on. `get_collection_health`'s `alert_read_health` block reports `retried_reads` beside the failure counts, so a write band's cost stays a visible count instead of a blind alert.
- **Store-object convergence runs every hour, not only at start** ([#3817]) - Every store object the service builds — hypertables, compression policies, continuous aggregates and their refresh windows, the baseline relations, the composer's covering indexes — is re-ensured on the hourly store-maintenance tick from the same one list the start path walks, so a single ensure that failed at startup heals within the hour instead of lasting until someone restarts the service. Each pass writes one `Store object convergence:` line naming what it changed and what failed, even when that is nothing. The compression-enable `ALTER` is now guarded by a catalog read: measured on TimescaleDB 2.28.1, the no-op form of that statement takes an `AccessExclusiveLock` and queues every writer behind one long reader — seventy-three of them an hour at :30 would have been a lock convoy on every store.
- **The alert pass's other seven store reads retry once on a command timeout, through the same seam as the adapter's twelve** ([#3854]) - #3851 gave every read on `DarlingAlertReadAdapter` one retry two seconds after a command timeout; the seven store reads the same pass issues elsewhere - six in `DarlingSelfAlertEvaluator` (collection signals, missing capture sessions, the two agent-status reads, the two Availability-Group grains) and the worker's latest-CPU read - still ran bare, so a light read crossing ten seconds during a write band skipped a cycle unretried. The seam stays ONE truth: `ExecuteWithOneRetryAsync`'s body moves to a static overload taking the counter and the pause, the adapter's twelve forwarders and their census pin are untouched, the evaluator forwards through a private wrapper over its own `_readFailures`, and the worker calls it directly - so the discrimination (a `TimeoutException` anywhere in the chain and nothing else: never a store-returned `PostgresException`, never a pass-token cancellation, never a torn stream), the cancellation-first ordering, and the single attempt-pair count are defined once for all nineteen retried reads. Each of the seven became a thin forwarder over a `…CoreAsync` sibling holding its body byte-identical, read names promoted to constants at the catch arm's granularity so one spelling reaches both the retry and the failure count, and `AlertReadFailureCounter.FleetScopedReads`' surrounding prose now says what the seam covers. `alert_read_health.retried_reads` counts them with no payload change. Follow-up to #3848 / PR #3851.
- **The compression band seats its three heaviest hypertables on spread minutes instead of the consecutive minutes registry order gave them** ([#3678], [#3781]) - Every hypertable's hourly compression ran at the minute its registry position implied, and the three largest raw tables sit at consecutive positions, so their 550 / 360 / 200-second compressions overlapped three-wide for five minutes every hour and cancelled eleven alert reads in one such window on the largest store. A declared, measured heaviest set now takes the band's first, ninth and seventeenth minutes — widest clearance to the longest run — and every other table fills the remaining seats in registry order, three per minute; the band's width, start and minute list are unchanged, so the refresh ceiling and its margin do not move. The :35 aggregate-compression minute's coordination fact — it is also the minute readers read — is recorded on its constant, and that issue closes count-only with its re-open bar.
- **Daily continuous-aggregate refreshes run one bucket per transaction** ([#3745]) - A daily refresh policy materialized its whole three-day window in one transaction — on the largest store, eight million rows at once, enough to blow through the WAL ceiling into a forced second checkpoint and kill every read for four minutes; the 16 GB ceiling absorbed the storm class, but the checkpoint and fsync tail still scale with the burst. The daily policies now set `buckets_per_batch = 1`, which TimescaleDB runs as one transaction per bucket (measured on 2.28.1: `batch N of M` per day), and the hourly store-object convergence sets the same key on every already-deployed daily policy without touching its schedule. Hourly tiers have one bucket per window and are unchanged.
- **`audit_config` answers a PostgreSQL target with the target's own settings instead of redirecting it** ([#3691]) — the analysis pass's `CONFIG_PG_*` facts project into the same recommendations shape: `engine` stands where `edition` does, each `current_value` carries its unit as a string, and there is no `suggested_value` — the recommendation sentence carries the evidence. On Aurora, `checkpoint_timeout` and `max_wal_size` read `not_applicable` (the storage layer owns checkpointing), counted apart and excluded from the checked count. The description changed once; three doc comments that still claimed edition-aware analysis now say resource-based. The SQL Server arm is untouched.
- **The same blocking storm grades the same on every pass length** ([#3871]) - `BLOCKING_EVENTS` graded the pass window's average, so 232 blocked-process reports in one hour read CRITICAL on a 4-hour pass (58/hr) and Information on a 24-hour one (9.7/hr); the (10, 50) pair was measured on 4-HOUR windows (#3653 box (a): non-zero p99 = 59/hr, five windows ≥ 50), so it sits on the storm mode at that grain and no other. The pair stays; the value moves to its grain: all three collectors compute the busiest 4-hour sub-window inside the same statement (bucketing CTE + MAX, origin at the WINDOW START so a ≤4h window is one bucket and grades exactly what it always did - an epoch origin would split a scheduled pass and read its peak below its own average), divided by min(4, observed hours) per #3538 A2 (the frozen Dashboard, which has no observed-hours concept, divides by the nominal min(4, period hours), stated at the site). The whole-window average stays as `events_per_hour` context beside `events_per_hour_peak_4h` and `peak_4h_event_count`. The scorer's "inherited, not measured" paragraph is superseded by the measurement's lineage. Live-verified against a real store before leaving the branch: both pre-existing fixture pins hold to six decimals, the storm fixture grades past CRITICAL on the 24-hour pass, and a steady 3/hr day stays under the concern bar. Rider (fold-in): `DefaultRatioThreshold = 4.0` carries its measured lineage (3,655 windows / two store classes / 14 d / p99 3.14 / p99.9 28.3 / 0.63% above bar, 2026-09-22, value unchanged) and the two live wait-profile facts stamp `threshold_lineage = 1`; the frozen Dashboard mirror deliberately does not, since its private copy is pinned as unmeasured on that tier.

### Fixed

- **PostgreSQL statement statistics count a re-created statement entry's work since it was re-created, instead of under-counting it; on PostgreSQL 16, where the entry's creation time isn't available, such a row is recorded as unknown** ([#4435])
- **Wait statistics on servers where a scheduled job clears them are counted from each clear instead of being recorded as unknown; only the work between the last collection and the clear stays unknown** ([#4434])
- **The raw purge gate judges each rollup against the rows that rollup can hold, so an hour of CPU-unknown query statistics at the floor can't hold the purge forever** ([#4432])
- **Query statistics record a restarted cached plan's executions, duration and CPU since the restart in full, instead of under-counting them; a restart the collector cannot place in time is recorded as unknown rather than estimated** ([#4431])
- **After an outage across an upgrade, the raw purge resumes within the hour on a running store, with no second restart** ([#4430]) - a legacy/successor seam an outage opens across an upgrade used to require a second full service start before the raw purge gate would release; the hourly retention tick now closes that seam on its own.
- **The daily retention sweep no longer drops raw query statistics over a hole no rollup holds; the gated service-triggered purge owns those tables, and purge_now reports what it held** ([#4429])
- **Query statistics no longer record a false zero CPU when only the CPU counter's delta is unknowable; those rows keep their executions and duration** ([#4423]) - a `query_stats` row whose CPU (worker time) counter reset or was seen for the first time, while its execution-count or elapsed-time counters had a real delta in the same pass, previously wrote CPU as a false 0 over a fabricated zero interval — which then caused the row's real executions and duration to be dropped by the interval-honest filter along with the fake CPU. That row now records CPU as unknown (NULL) and keeps its real interval, executions, and duration. A CPU counter that is genuinely unchanged over a real measured interval still records 0, unchanged.
- **FinOps no longer reports a database as idle when its only recent samples were a plan's first sighting** ([#4417]) - idle-database detection now counts a recent last-execution time as activity, instead of deciding on the summed execution count alone (#4394).
- **Alert notebook: the collector-freshness check now counts only successful collection runs** ([#4416]) - it previously treated a collector's own failed runs as proof it was still fresh, used no read timeout, and always logged a zero elapsed time on failure. It also now recognizes AG and Server Unreachable/Restored recovery pairs when resolving a notebook's status.
- **`get_store_host` now honours request cancellation** ([#4412]) - an abandoned or superseded call to `get_store_host` kept its store-host gather running to the tool's own fixed deadline instead of stopping when the caller went away; the gather now also observes the request's own cancellation, and a cancelled gather is never cached.
- **Web reads honour request cancellation for 2 more trend charts and 3 analysis tools** ([#4411]) - `get_file_io_trend`, `get_perfmon_trend`, `compare_analysis`, `get_analysis_facts` and `get_analysis_findings` kept running their store reads (and, for the analysis tools, the inference engine's fact collection and scoring) to completion after a web request was abandoned. They now take a cancellation token and stop when the client disconnects.
- **Web reads for PostgreSQL index bloat, column stats, CPU utilization, log events and logging audit now honour request cancellation** ([#4409]) - an abandoned web request for one of these five reads previously ran its underlying store query to completion instead of stopping; each now threads the request's cancellation token through every store call.
- **The settings redactor no longer masks a short value whole on its first call** ([#4408]) - its patterns are warmed once at start-up, so a first-use compile cost can't count against the match time bound.
- **darling-managed.conf is rewritten only when its settings change** ([#4407]) - a change in a header display field, such as the data volume's free space crossing a GiB boundary, no longer rewrites the file or re-verifies it.
- **Stop re-holding the raw retention purge for a healthy store whose successor rollup hasn't run its first refresh yet** ([#4406]) - An upgrade with no outage was reporting the raw purge's safety gate as unsafe while a freshly-added rollup's history was still empty, even though no row was actually at risk. The gate's fallback probe now stops one refresh-window short of the current time, matching how far the rollup's own first refresh is expected to reach.
- **Carry an operator's own `postgresql.conf` lines below the `darling-managed.conf` include across a major PostgreSQL upgrade** ([#4405]) - previously only `postgresql.auto.conf` (`ALTER SYSTEM`) settings survived a major upgrade; a hand-edited line below the include stayed behind in the retained old data directory. Each carried line is now probed against the new binaries and skipped (never fatal) if rejected.
- **Web reads honour request cancellation (10 more tools)** ([#4402]) - `get_store_query_stats`, `get_store_metrics`, `get_store_log`, `get_collector_stall_probes`, `get_plan_xml`, `get_oversized_plan_backlog`, `get_fleet_overview`, `get_sweep_reports`, `get_collector_cost` and `get_pg_blocking` kept running their store query to completion after a `/api/read` client disconnected or timed out, because their dispatch entries and tool methods never threaded the request's `CancellationToken` through to the query. Each now takes the token and passes it to every store call, so an abandoned request stops promptly instead of finishing unread.
- **Fixed a raw-purge gate that could hold history forever, and a repair
- **Web reads honour request cancellation (11 more SQL Server tools)** ([#4393]) - `get_session_stats`, `get_waiting_tasks`, `get_cpu_scheduler_pressure`, `get_plan_cache_bloat`, `get_latch_stats`, `get_spinlock_stats`, `get_memory_trend`, `get_default_trace_events`, `get_running_jobs`, `get_pvs_stats`, and `get_ag_health` now take a `CancellationToken` and pass it down to every store call, so an abandoned `/api/read` request stops its query instead of running to completion. Removed all from `CancellationAllowlist`.
- **The PostgreSQL settings redactor has a bounded matching time** ([#4392]) - Each redaction pattern runs under a time bound. A value that reaches it is masked whole, and the warning names only the setting.
- **An armed raw-retention purge no longer runs when PostgreSQL starts, before the service can hold it** ([#4391]) - After a long enough outage, TimescaleDB's own scheduler could run the `query_stats`, `procedure_stats` or `query_store_stats` retention job the moment PostgreSQL came up, dropping raw history that no rollup had materialized yet. These three jobs are now never scheduled on TimescaleDB's runner. The service runs the purge itself, from its hourly evaluation only, and only after it measures coverage itself, confirms the hole repair finished cleanly since the current PostgreSQL start, and finds no hole in the range it would drop. A new **Raw Purge Over Horizon** alert says why a purge didn't run, or that the purge trigger has stopped. The first start after the upgrade still runs the old armed job once.
- **PostgreSQL web reads now honour request cancellation** ([#4390]) - `get_pg_xmin_horizon`, `get_pg_wraparound_risk`, `get_pg_session_states`, `get_pg_replication_stats`, `get_pg_predicate_stats`, `get_pg_kernel_stats`, `get_pg_io_stats`, `get_pg_index_usage`, `get_pg_database_stats` and `get_pg_autovacuum_health` ran every store call to completion even after the web caller disconnected. Each now takes a `CancellationToken` and passes it to every store call, so an abandoned request stops its query instead of running to completion.
- **PostgreSQL-target MCP reads now cancel on client disconnect** ([#4388]) - nine PostgreSQL-target read tools (`get_pg_deadlocks`, `get_pg_deadlock_detail`, `get_pg_wait_stats`, `get_pg_wait_sampling`, `get_pg_replication_slots`, `get_pg_top_queries`, `get_pg_table_bloat`, `get_pg_plans`, `get_pg_plan_capture_readiness`) ran to completion even after the requesting client disconnected. Each now takes a `CancellationToken` threaded to every store call and is dropped from the web-read cancellation allowlist.
- **Web reads now honour request cancellation for 15 more MCP tools** ([#4387]) - `get_cpu_utilization`, `get_wait_stats`, `get_wait_types`, `get_wait_trend`, `get_memory_stats`, `get_memory_clerks`, `get_file_io_stats`, `get_tempdb_trend`, `get_perfmon_stats`, `get_query_store_top`, `list_servers`, `get_collection_health`, `get_collection_log`, `get_current_waits_trend` and `get_blocking_stats` used to run their store query to completion even after the web viewer's request was aborted; each now takes a `CancellationToken` threaded to every store call, so an abandoned request stops promptly.
- **Web reads honour request cancellation for Alerts, Memory Grants and Health MCP tools** ([#4386]) - `get_alert_history`, `get_alert_settings`, `get_mute_rules`, `get_notification_routes`, `get_resource_semaphore`, `get_memory_grants`, `get_memory_pressure_events`, `get_server_summary`, `get_daily_summary` and `get_daily_summary_range` previously ran their store query to completion even after an abandoned web request; they now take a `CancellationToken` and pass it through every store call.
- **Threaded cancellation through 10 PostgreSQL-target web reads** ([#4373]) - get_pg_buffer_usage, get_pg_extensions, get_pg_lock_stats, get_pg_server_config, get_pg_server_config_changes, get_pg_write_stats, get_pg_database_trend, get_pg_io_trend, get_pg_query_duration_trend and get_pg_wait_trend ran their store queries to completion after a web request was abandoned; each tool now takes a CancellationToken and passes it to every store call, and is removed from the /api/read cancellation allowlist.
- **Web reads for object-stats and config-history now honor request cancellation** ([#4372]) - get_database_sizes, get_index_usage, get_object_locking, get_table_index_sizes, get_database_config_changes, get_database_scoped_config, get_server_config_changes and get_trace_flag_changes ran their store query to completion even after a web caller abandoned the request; all eight now take a CancellationToken threaded through every store call and are removed from the cancellation allowlist.
- **Deprecated Dashboard's XE ring-buffer collectors no longer reshred unchanged data** ([#4371]) - `install/22_collect_blocked_processes.sql` and `install/24_collect_deadlock_xml.sql` converted the whole Extended Events ring buffer to XML and shredded it on every scheduled run, even when nothing new had arrived since the last cycle. Both now read the ring buffer target's own delivered-event counter first and skip the conversion when it hasn't changed, matching the fix #4212 shipped for Darling and Lite. Applies to existing installs still running the deprecated Dashboard's SQL Agent collectors; the rows each collector stores are unchanged. A no-change collector run (median of 5, on a filled ring buffer) went from 150 ms to 35 ms for blocked-process and from 56 ms to 28 ms for deadlock.
- **Bucketed the memory-grant and TempDB usage viewer trend charts** ([#4364]) - The memory-grant overlay, Memory Grants chart, and TempDB usage chart (Darling and Lite) each returned one row per collection over a 7-day window. On a 7-day seed at each collector's cadence, the memory-grant overlay went from 10,080 to 1,008 rows, the Memory Grants chart from 20,160 to 2,016, and TempDB usage from 10,080 to 1,008. All three now bucket to the chart's point budget the same way the CPU, wait, and file-I/O trend charts already do, averaging gauges and summing true deltas per bucket, with no change to what the series measures.
- **Bucketed the blocking-trend charts' lock-wait, waiting-task, and blocked-session reads** ([#4362]) - the Blocking Trends and Current Waits charts read one row per collection at wide windows, unlike the other trend charts #4353 and #4340 already bucketed. On a 7-day seed at the 1-minute cadence, lock waits went from 20,160 to 2,016 rows, waiting tasks from 30,240 to 3,024, and blocked sessions from 20,160 to 2,016, on both products. Blocked sessions is now averaged per collection within each bucket, so its value no longer grows with the bucket width.
- **Bucketed the CPU scheduler, session stats, and plan cache trend reads** ([#4361]) - These three reads returned one row per collection with no cap, unlike the file I/O, memory, wait stats, and other trend reads #4234 already fixed. On a 7-day seed at each collector's cadence, CPU scheduler pressure went from 10,080 to 1,008 rows, and Sessions and Plan cache each from 2,016 to 1,008. All three now bucket to the same width ladder the other trend charts use, averaging each gauge per bucket and (for Session Stats) carrying forward the newest collection's application/host attribution rather than blending it.
- **Blocking and deadlock web reads now cancel with the request** ([#4203]) - the seven blocking/deadlock tools behind the web viewer's Blocking page (blocked process XML, blocking, blocking trend, deadlock detail, deadlock trend, deadlocks, lock wait trend) kept running their store queries after a browser navigated away or a request was abandoned. Each now takes a cancellation token and passes it through every store call, so an abandoned request actually stops.
- **Health Parser web reads now cancel with the browser request** ([#4359]) - the nine `get_health_parser_*` `/api/read` handlers ran their Postgres queries to completion even after the browser tab closed or the request was aborted; they now thread `RequestAborted` through every store call so an abandoned read stops promptly.
- **Four Performance-Trends web reads now stop their store query when the request is abandoned** ([#4357]) - `get_query_trend`, `get_query_duration_trend`, `get_procedure_duration_trend` and `get_query_store_duration_trend` used to keep running their store query after a web caller gave up or navigated away; they now observe the request's cancellation all the way down to Npgsql.
- **A slow first-run database create no longer fails Darling's Postgres bootstrap** ([#4352]) - The managed store's first-run `CREATE DATABASE` was retried up to 6 times inside the same 10 second window used for the post-start connection probe. Because a timed-out `CREATE DATABASE` is cancelled and rolled back, every retry re-copied the template database from scratch under the same 10 second budget, so a copy that took longer than 10 seconds failed outright no matter how many retries ran. The create now runs once, on a 60 second budget, outside that retry.
- **Passwords and other secrets in stored PostgreSQL settings are now redacted** ([#4351]) - The config collector stored `pg_settings` and per-database/per-role override values verbatim, including secrets a `pg_monitor`-only monitoring role can read but should not retain in plain text, such as a standby's `primary_conninfo` replication password or a backup tool's key/token in `archive_command`/`restore_command`; new collections now mask the secret, and existing stored rows are scrubbed in place, in batches, and the scrub completes at any settings cadence; a target that fails is retried on the next start. Non-secret password-policy settings (`rds.accepted_password_auth_method`, `rds.restrict_password_commands`, `passwordcheck.min_password_length`) are no longer masked. "Keep the rest of the value" is not true for every case: `ssl_passphrase_command` and a dotted extension setting naming a secret are masked whole. Rotate any password or key that was ever stored this way; the store's WAL archive, replicas and dead row versions can still hold the old plaintext, not only backups and exports, until they age out. Upgrade every service that writes to a store - a service still on an old build keeps storing plaintext after the scrub's marker is set, and nothing re-runs it.
- **Eight Queries-tab reads in the web viewer stop their store query when you leave the page** ([#4350]) - get_active_queries, get_plan_corrections, get_query_heatmap, get_query_store_clutter, get_top_queries_by_cpu, get_top_procedures_by_cpu, get_query_store_regressions and get_long_query_completions used to run their store query to the end after the browser gave up on the request. They now stop when the request is abandoned. The tab's duration trends and Query Store top list still run to the end; later PRs convert them.
- **Configuration-tab reads in the web viewer stop their store query when you leave the page** ([#4347]) - Leaving the Configuration tab, or an audit_config read, before it finished left its store query running to the end for a caller who was gone. Those six reads now cancel when the browser abandons the request. The viewer's other reads still run to the end; follow-up PRs convert them.
- **PostgreSQL `pending_restart` can now be trusted after a reload, on Windows targets** ([#4345]) - `pg_settings.pending_restart` is backend-local, so on Windows, where the collector's reconnect-every-cycle model always sees a brand new backend, a setting that had actually been changed and reloaded but still needed a restart could read `false` forever. Unix targets never had this gap: there, the postmaster itself applies the reload and every backend it forks inherits the flag, so `pg_settings` alone was already correct. The collector now also checks `pg_file_settings` on Windows targets, which is read from the file rather than a connection's own state, when the monitoring role can read it (two grants: `SELECT` on the view and `EXECUTE` on `pg_show_all_file_settings()`). `get_pg_server_config` and `get_pg_logging_audit` note when a Windows target's role cannot read it, and say what the two grants expose, so an operator can weigh the trade before granting them; a non-Windows target is untouched and never asked to widen its role for this.
- **Heal the Postgres v8 hardware-sizing block on stores resized before the #4225 fix shipped** ([#4342]) - A managed store whose hardware had not changed since its last hardware-sizing rewrite never re-triggered the heal, so a store that already carried duplicate sizing blocks, or a block written before `work_mem` rejoined the formula, stayed on stale settings even after upgrading past the earlier fix. The heal now also runs when the conf carries more than one sizing block, or when the newest block's content no longer matches what the current build would write.
- **Fleet Sweeps reports an error instead of "no sweeps" when its store read fails** ([#4328]) - A failed read for the sweep timeline, a sweep's verdicts, its would-have-paged ledger, or the watch-item worklist used to log the failure and silently answer an empty list, so the web page showed its normal "No sweeps in this span" empty state and the `get_sweep_reports` MCP tool answered an empty or silently incomplete document instead of reporting that the read failed. Both surfaces now answer the failure: the web page shows its error strip, and the tool answers its usual error status.
- **Lite Overview baseline bands used the wrong hour under a custom time range on a server not on UTC**
- **Dashboard query comparison grids and Server Trends baseline bands now use the server's local time** ([#4321]) - Under a preset time range (not a custom one), the Query Stats, Proc Stats and Query Store "Compare to" grids, and the Server Trends baseline bands, built their current time window from the Dashboard machine's clock in UTC instead of the monitored SQL Server's own local time. On a server not running in UTC, this shifted the comparison grids' "current" window and picked the wrong hour-of-day/day-of-week baseline bucket, by the server's UTC offset. Fixed to use the server's local time, matching the Server Trends ghost-line fix in [#4317].
- **`get_collection_health`'s default reply now fits the MCP response budget** ([#4319]) - A collector row that
- **Dashboard Overview ghost line reads the right hours outside UTC** ([#4317]) - Under a preset time range, the
- **The Query Store activity slicer reads less history on each refresh** ([#4311]) - The slicer read one extra day of Query Store history below its window, as a margin for clock skew. The margin is now one hour. For the last 24 hours, it now reads 25 hours of history instead of 48.
- **Lite's Overview ghost line reads the right hours outside UTC** ([#4309]) - Under a preset time range,
- **Availability Groups tab no longer freezes the UI every 30 seconds** ([#4308]) - The Darling viewer's and Lite's Availability Groups tabs rebuilt every card and every per-database grid from scratch on every refresh. In the Darling viewer, that blocked the UI thread for over 2.5 seconds on a 42-AG fleet, even when nothing had changed. Refreshes now update existing cards in place, skip entirely when the data has not changed, and only render the cards that are actually on screen.
- **The Daily Summary calendar and `get_daily_summary_range` cache closed days for an hour** ([#4307]) - The WPF Daily Summary calendar (every 1-minute refresh) and `get_daily_summary_range` (the web Overview tab and MCP clients) recomputed every day in the displayed range on every poll, even though days before today do not change. A refresh now recomputes only today (and briefly yesterday, for a late collector run) and reuses the rest from a one-hour cache. A row that lands late for a closed day -- an outage catch-up, a backfill -- can take up to an hour to show, where it showed immediately before; an explicit `as_of` timestamp still always reads live. The cache holds at most 1,024 ranges, and drops entries older than an hour the next time it computes a range.
- **Query Store backfill's candidate check no longer reads old compressed chunks, and no longer drops a hole on a database that goes quiet** ([#4306]) - The backfill's candidate-database scan read across all of raw retention on every check, which decompressed chunks outside the backfill window: measured at 206 ms and over 21,000 buffers per check on a heavy server. It is now bounded at the backfill's own horizon (1.7 ms and 792 buffers in the design measurement). The per-database floor lookup now stops at the first row at or before that horizon instead of taking the minimum over every row. A database that stopped sending Query Store data while a repair "hole" was still open for it used to age out of the backfill scan and never get serviced. It now stays in scope until the hole is filled or expires. Both Lite and the Darling service get the change.
- **The Darling viewer's Wait Stats and Perfmon trend charts read time buckets, not every collection** ([#4304]) - Both charts read every collection in the window on each refresh: for 20 wait types over 7 days at a 1-minute cadence, that was about 170,000 rows. They now read buckets sized to the chart's point budget, which cut the rows returned about tenfold for a 7-day window in the measurement. A bucket's rate is its summed wait (or counter delta) over its summed collection interval. When the budget covers every collection, the chart still gets each raw point at its own timestamp. The wait-type and counter pickers ran a full-window DISTINCT on every 1-minute refresh; they now reuse the list for up to 15 minutes for the same server and window.
- **The Darling viewer's Active Queries grid and wait drill-down no longer load every snapshot's plan XML up front** ([#4303]) -
- **Lite's Queries-tab comparisons use the grid's time window** ([#4302]) - On a server not on UTC, the "Compare to" baseline on the Queries tab read a window shifted by the server's offset. That covers the Top Queries, Top Procedures and Query Store grids. It happened under a custom date range or after a slicer drag. Comparisons now read the same UTC window as the grid beside them. One behavior changes on purpose. After a slicer drag, the baseline is the dragged window shifted back by a day or a week, not the whole preset range.
- **Active Queries and wait drill-down no longer load every plan's XML just to show the grid** ([#4297]) -
- **The query heatmap loads much faster on large windows** ([#4295]) - The heatmap read resolved the query text of every row in the window, then kept one row per cell. Now it resolves the preview text only for the row that each cell shows. The cells, counts, top queries and preview text do not change. On a seeded window of 180,000 rows over 24 hours, the read went from 2,035 ms to 242 ms.
- **Less write-ahead log from the query, procedure and Query Store history tables** ([#4294]) - Five indexes on these tables wrote close to a quarter of the store's write-ahead log. They were read only a few times in several days. They are now dropped. Each feature that read them was timed without its index and stays well within its time limit.
- **Web viewer no longer shows raw error text for failed requests** ([#4293]) - Several pages and API routes answered a failed request with the underlying exception's own text, which could include a PostgreSQL SQLSTATE and message or a store's host and port: saving or deleting a Custom View, an alert rule or a mute rule (including turning a mute rule on or off), running a Custom View panel, the two fleet-sweep reads, a server lookup that could not read the server registry, and the triage page's notes and per-section cards. These now answer a fixed, generic message; the real text still goes to the service log so an operator can see what failed. A statement timeout now answers with a "took too long" message and the right status code in each of these places, matching what other reads already did. The one exception is a Custom View panel's own query error (a syntax error, a missing column, a bad value, or its own statement timeout), which still shows the database's message so the author can fix the panel.
- **Empty legacy baseline aggregates now drop instead of staying forever** ([#4292]) - On a store where nothing ever fed the old `perfmon_baseline` or `wait_stats_baseline` aggregate, both it and its replacement stayed empty. The empty replacement has no oldest bucket, so the old aggregate's cleanup rule never passed. Now an old aggregate that holds no rows drops on the next start, with its refresh, retention and compression jobs.
- **CPU and I/O-latency baselines recompute once a day, not every hour** ([#4291]) - Darling and Lite recomputed these two anomaly baselines every hour. Each time, they scanned 30 days of raw rows. The I/O-latency scan alone spilled about 50 MB to temp. They now recompute once per UTC day, and their 30-day window ends at midnight UTC. A 30-day baseline barely moves within a day. A failed compute still retries within the hour.
- **Fewer needless writes to the Darling store's plan and text tables** ([#4288]) - A plan or text seen again within the hour still cost a row lock and a WAL record. PostgreSQL locks the row before it checks the freshness rule. On production stores, that was 12 to 23 percent of all WAL. The upsert now skips a fresh row before it takes any lock. Separately, the touch that keeps Query Store plan and text rows from expiring re-stamped each row every 6 hours. It now waits 12 hours, which halves those writes. Rows are kept just as long as before.
- **Managed store: less WAL, from compressed full-page images** ([#4287]) - Most of a managed store's WAL was full-page images, written without compression. Every managed store now sets `wal_compression = lz4` on its next service start. In a test burst, that cut WAL by about a quarter. A longer checkpoint interval cuts WAL further, but it can slow checkpoint syncs past the store's own alert bar. So it waits for a production measurement. Bring-your-own PostgreSQL stores do not change.
- **Web failure handling: log batches, bad requests and error text** ([#4286]) - A route cut inside a character made Darling's service log drop its whole 5-second batch. The viewer's, Lite's and the Dashboard's log files had the same bug for any text with a broken character. A malformed or oversized request, or a client that dropped the connection, cost a 500 and an Error line. It now gets its own status and no Error line. A line break in exception text started what looked like a second entry in the service log. Each line is now cleaned before it is written. The read dispatcher and the fleet sweep reads answered some failures with raw exception text, such as a connection error that names the store's host. They now answer with a fixed message and log the detail. Darling's web and MCP hosts now always run as Production, so a stray Development setting on the machine cannot turn on the developer error page.
- **Time-based reads no longer shift on a store outside UTC** ([#4285]) - Every timestamp in the Darling store holds UTC, but a bring-your-own store keeps the session time zone its owner set. Reads that compare those timestamps with the current time shifted by the zone's offset. Their window was wider or narrower than asked, with no error. Every store connection that the service, the CLI and the desktop viewer open now sets its session time zone to UTC. Managed stores already set UTC in their `postgresql.conf`. The service and the viewer also keep a connection-string setting that has an empty value, such as `Password=''`, when they add their own settings. Before, they dropped it, and Npgsql then used the `PGPASSWORD` environment variable or a password file if either one was there.
- **Web viewer timeouts now show a message and get logged** ([#4281]) - A read that ran past the store's timeout used to show a blank error. The service log had nothing, so a timeout looked the same as a bug. Now the page says "The store took too long to answer this read. Try again in a moment." Other failures show a short generic message. For both, the service log records the route, how long the read ran and what kind of failure it was.
- **A major store upgrade keeps your ALTER SYSTEM settings** ([#4280]) - A major PostgreSQL upgrade of the managed store used to drop every `ALTER SYSTEM` setting. A raised `shared_buffers`, for example, went back to the default with no warning. The upgrade now checks each setting against the new version, test-starts the new store with the settings it accepts, and keeps them if that start works. A setting the new version rejects is left out with a warning that names it. If the test start fails, the store starts on its defaults instead, and a warning names each setting it dropped. The logs name settings but never show their values. The original file is kept as `postgresql.auto.conf.pre-upgrade` next to the data directory, so a dropped setting can be re-applied by hand.
- **Lite says when a query window was cut short** ([#4279]) - Three MCP tools read raw tables with no rollup fallback: `get_top_queries_by_cpu`, `get_top_procedures_by_cpu` and `get_query_store_top`. Lite keeps those tables 30 days by default, and a user can lower that per collector. So a long window was read from much less history, with nothing to say so. Each tool now returns `effective_start`, `effective_hours_back` and `window_truncated`, plus a `truncation_note` when the window was cut short. The three grids show "Showing since" and the effective start above the grid in the same case, after a time-range change or a slicer drag. On Lite and Darling alike, each tool's short description now explains `window_truncated`. The false line that said Lite always reads the full window is gone.
- **Top-CPU reads disclose a short window** ([#4278]) - Two MCP tools read raw tables that the store purges at 4 days with the rollups on. They are `get_top_queries_by_cpu` and `get_top_procedures_by_cpu`. A longer window came back short, and `hours_back` came back unchanged. Both tools now return `effective_start`, `effective_hours_back` and `window_truncated`, plus a `truncation_note`, like `get_query_store_top`. The desktop viewer's Top Queries, Top Procedures and Query Store grids show "Showing since" and the effective start in the same case. The web viewer shows the same note on those panels.
- **`get_query_store_top` stays under the MCP response-size budget** ([#4198]): The default call returned about 48 KB, over the 32 KB limit some MCP clients enforce. `query_text` is now a 400-character preview by default. A new `full_text` argument returns the whole statement. A new `query_text_truncated` flag on each row says whether the text was cut. The web viewer is unchanged and still shows up to 2,000 characters.
- **`describe_custom_view_catalog` stays under the MCP response budget** ([#4198]): The Custom Views compose catalog returned 98 KB by default, three times the budget. It now returns a compact, source-grouped list by default (name, purpose, aggregate/unit vocabulary only). A new `source` argument drills into one source's full detail. A new `full_detail` argument returns the complete catalog as before. The web Custom Views editor is unaffected: it reads the full catalog through its own endpoint.
- **MCP `get_collection_health` caps its response size** ([#4198]): A default call on a busy SQL Server fleet reached 41,669 bytes, over the 32 KB limit. Healthy collectors with nothing to report now compact to a shorter row by default. Any failing, stale, stopped, erroring, denied, or regressed collector keeps every field. A new `full_detail` argument restores every field on request. The web dashboard and Custom Views are unaffected.
- **MCP `get_blocking` (Darling) and `get_blocked_process_reports` (Lite) cap their response size** ([#4198]): A default call measured 89,096 bytes on a 30-row test seed, over the 32 KB limit. The default row limit drops from 30 to 15. The blocked and blocking SQL text come back as a 150-character preview instead of a 2,000-character cut, and each has its own `*_truncated` flag. A new `full_text` option returns both texts whole. On Darling, a call with `dedup_key` names one incident, so it always gets the whole text. The web viewer's Blocking table and Custom Views keep 30 rows and the 2,000-character text.
- **MCP get_collection_log stays under the response-size budget by default** ([#4198]) -
- **`get_query_store_regressions` default calls stayed under the MCP response budget** ([#4198]). The
- **get_pg_io_trend's default call now fits the MCP response budget** ([#4263]): The default 24-hour call
- **`get_active_queries` stays under its response-size budget by default** ([#4261]) - A default call on a busy server returned well over the 32 KB target. The number of fields per row and an uncapped query text preview were the cause. The default page size is now 25 rows (was 50). Query text now previews to 500 characters with a `query_text_truncated` flag. A new `full_text` argument opts back into the whole text. The web viewer's Active Queries tab keeps its pre-existing 2,000-character preview.
- **get_index_usage's default answer now fits the response budget** ([#4260]) - A default call, with no `limit` argument, always returned up to 200 rows regardless of width. That measured over 69 KB on a busy server, large enough for an MCP client to refuse it outright. The default `limit` is now 75 on both Darling and Lite. An explicit `limit` still returns exactly what was asked for.
- **`get_query_heatmap` default call stays under the MCP response budget** ([#4259]): A default call returned
- **`get_object_locking` default response stays under the MCP budget** ([#4258]): Before this fix, the tool returned up to 200 locking rows with no row limit. On a busy server it pushed 71 KB, and some MCP clients refused the response. It now returns 75 rows by default, highest contention first. It reports `truncated`/`objects_returned` when there are more. Pass `limit` for more rows, up to 1000.
- **`get_plan_corrections` default call stays under the MCP response budget** ([#4198]): A default call returned nearly 100 KB on a busy server. Each row carries 20+ fields and the query text ran to 2,000 characters. The text preview is now 150 characters. Each row gets a `query_text_truncated` flag and a `full_text` opt-in. The default row limit dropped from 50 to 25.
- **Job History loads faster and stops polling every 30 seconds** ([#4256]): The tab used a window function
- **Availability Group reads: bounded the newest-snapshot lookup** ([#4228]). All five AG reads used an
- **`get_deadlock_detail` no longer returns oversized deadlock graphs by default** ([#4254]) - The tool's `deadlock_graph_xml` field could push a default call past 100 KB, because each graph runs tens of kilobytes and the tool did not cap that field's size. It now previews each graph to 2000 characters by default and marks it `deadlock_graph_xml_truncated: true`. A new `full_graph` argument returns the whole graph. A `dedup_key` call, naming one incident, always returns it in full.
- **Database Sizes and Storage Growth load without a multi-chunk planning tax** ([#4252]): The latest snapshot reads had no bound on collection time. PostgreSQL had to plan a step for every retained chunk before running each read. Each read now resolves the snapshot timestamp in a separate step first. The main read then skips chunks it does not need. Clock skew between reader and collector does not drop the latest snapshot.
- **Alert triage links now skip only a disabled dashboard, and email carries one too** ([#4230]) - the per-alert triage-page link (`web.publicBaseUrl`) used to go out even when the dashboard was off, and the web host rejected every request naming the DNS host that setting suggested using. The link is now omitted only when the dashboard is disabled. The web host accepts that configured host. Alert email now carries the same link the other channels already did, in both the HTML and plain-text bodies.
- **PLAN_REGRESSION no longer deduplicates the whole raw Query Store slice every pass** ([#4208]). On a store ingesting many servers' Query Store data, the plan-regression check was the single heaviest statement in the store. It spent most of its time sorting and spilling the server's entire raw Query Store history to temp files. This adds store schema rung V143, the `collect.query_store_interval_latest` table, holding only each interval's latest snapshot. The check now reads that table and falls back to the old full read only when coverage is incomplete for a server. Findings are unchanged: the same regressions, the same snapshots.
- **Force-plan bot no longer acts on a best plan older than 4 days** ([#4208]). The unattended force-plan bot now gates on `ForcePlanBotPolicy.MaxBestPlanAgeDays` = 4. A target whose best plan has not been seen in more than 4 days is blocked with reason `best_plan_stale`. This prevents a force on a plan the server evicted. The gate value is pinned by `ForcePlanBotPolicyTests`.
- **`audit_config` no longer runs the full analysis pass** ([#4206]). It read a full ~30-family collection, detector and scorer pass just to answer 8 point-in-time configuration facts, measured at 7-15 seconds per call. It now runs only the specific family reads those 8 facts come from. The facts and recommendations returned are unchanged.
- **`get_query_store_regressions`' comparison baseline is now a fixed 7 days** ([#4206]). The baseline used to be every Query Store capture the store retained before the requested window. Both cost and the comparison period grew with retention. It is now a fixed 7-day lookback ending at the window's start, reported as `baseline_start` and `baseline_end` in the response. A regression against something that changed more than 7 days before the window is no longer caught. A store retaining less than 7 days is unaffected. Performance Monitor Lite's own copy of this same tool had the same unbounded baseline and is fixed the same way.
- **`get_pg_cpu_utilization` now returns bucketed points instead of one row per minute** ([#4206]). A wide window (a week, or longer with the V136 host-memory columns) returned several megabytes per call. It now buckets to a point budget the same way the rest of the trend family does. It averages CPU/ACU per bucket while keeping each bucket's peak. The worst (not average) memory-pressure sample per bucket is kept, so a brief spike is not smoothed away.
- **The desktop viewer's Query Store Regressions grid had the same unbounded-baseline shape as `get_query_store_regressions` and is now bounded the same way** ([#4206]). It was a third, separate copy of the defect, in the WPF app rather than the MCP surface, with its own 7-day fixed lookback.
- **The deadlock and plan-capture log patterns only offer a report whose ERROR: is the line's own label** ([#4042]) - A crafted SQL statement that echoed "ERROR:  deadlock detected" plus a tab-continued DETAIL line matched the deadlock pattern through to the line's real process-id bracket. Measured end to end this forgery stored nothing, because the log assembler already takes a report's label from the line it actually came from, and new tests pin that for both the SQL and C# copies of the pattern. Both copies are narrowed anyway, matching the rule behind them exactly: the gap before ERROR: and DETAIL: can no longer cross another field's label, and the managed log-prefix family's gap to the process-id bracket can no longer slide past the real bracket into a statement's text (the same fix #4016 made for plan capture). Fewer forged lines are offered as candidates at all.
- **Plan capture keeps working under a custom log prefix with fields before the process id** ([#4016]) - Plan capture required exactly one token between the timestamp and the process-id bracket in log_line_prefix. A self-hosted prefix with more than one field there (for example '%m %u@%d [%p] %Q ') matched nothing, so those stores silently stopped capturing plans. The gap before the process id now allows any number of fields, as long as none of them is itself a bracket, so a forged log line behind the real bracket still cannot be read as a plan.
- **Trace flag reads no longer hide every enabled flag after an ordinary run** ([#4032]) - get_trace_flags, the web viewer's Trace Flags grid, and Lite's equivalent compared the newest captured row's timestamp against the newest successful run's timestamp, but Darling stamps a run's collection-log row when the run ENDS, after its capture rows are written - so that comparison read false after every ordinary run and hid every enabled flag. All three reads now keep the newest capture unless the newest successful run explicitly captured zero rows (which means every flag is off); a failed run, or one with no row count, keeps the reading from before this fix.
- **The fleet card's collection-health figures no longer reread a week of raw log rows on every call** ([#3911]) - The seven-day fleet-wide collection-health read decompressed six of its seven days of collection_log on every call. It now reads from a new hourly rollup instead, cutting the read down to the buckets it needs. The fleet card's numbers are unchanged; only how they are produced is faster.
- **The per-server collection-health summary stops leaking memory and stops going stale on a fleet with more than one active caller** ([#3900]) - The health memo keyed its cache on the window's start time, which two calls a few seconds apart would each compute freshly, so the cache never hit in production and grew without bound as more distinct start times accumulated. It now keys on the window's length, so calls asking the same question in different moments share one cached answer and old entries age out.
- **A collector that produced then stopped no longer reads healthy with zero rows** ([#3889]) - Collection health banding keyed a regressed collector's WARNING floor on the collector's STATUS word rather than on whether it was actually producing output, so a collector whose query still ran and still returned SUCCESS - just with zero rows, after previously producing rows - kept reading HEALTHY indefinitely. On one production server this hid a stopped collector for up to two weeks and over 1,900 consecutive zero-row successes. Banding now also watches for a productive collector's output falling to zero for three or more consecutive runs, and reports it as regressed. A collector whose normal resting state is zero rows (an event capture with nothing to report) is unaffected.
- **job_history collection survives an msdb reseed** ([#3886]) - The collector deduped job-history rows against a stored high-water mark taken from the target's job-history identity column. A maintenance window that purges msdb's job history also resets that identity to a lower value, so the stored watermark (in the millions) outlived the identity it was taken from (back in the thousands), and the collector's filter matched nothing from then on - forever, on that server, with no error. The watermark now yields to a bounded time window whenever the target's current identity max falls below the stored watermark, the regression is logged once, and job history collection self-heals on its next run without intervention.
- **The compression-stuck self-alert no longer pages on a healthy job's own run instant** ([#3588]) - TimescaleDB's job_stats view assembles job_status from pg_stat_activity and next_start from the background-worker job-stat row, so for a moment at both edges of every healthy run one read sees the same shape as a genuinely dead job (next_start = -infinity while the schedule still reads Scheduled). A production store paged on exactly that, 53 ms into a 63 ms run that went on to succeed. The check now re-reads five seconds after a -infinity trip and only reports the job if the trip still holds; a failed confirmation defers judgement to the next hourly check rather than paging. The hourly check itself is now snapped to :30 past the minute instead of drifting a few seconds later each time, keeping it off the fixed-schedule policies' own :00 run instants.
- **pg_deadlocks reads a self-hosted target's csvlog file, closing the same forged-line hole #4124 closed for pg_log_events** ([#4136]) - pg_deadlocks only read stderr-format log text. A target with csvlog but not stderr in log_destination produced no deadlock rows at all. A target with both was open to the same forged-line risk #4124 describes for pg_log_events. A failed login can plant a newline into a logged name. Unquoted stderr text turns that newline into a fake extra line. pg_deadlocks now parses the target's own .csv file as CSV when log_destination includes csvlog, the same forger-safe, whole-record join #4124 built. A planted newline now stays inside its own quoted field instead of becoming a fake deadlock. This change leaves pg_log_events and plan capture as they were. #4137 covers plan capture.
- **pg_plan_capture reads a self-hosted target's csvlog file** ([#4137]) - pg_plan_capture only read auto_explain plans from stderr-format log text. A self-hosted target with csvlog but not stderr in log_destination captured no plans. pg_plan_capture now reads the target's own .csv file when log_destination includes csvlog, through the same whole-record CSV parser #4124 built. It takes the query id from csvlog's own query_id column. The stderr route is unchanged.
- **Darling's daily retention purge no longer holds up collection** ([#4133]) - The purge ran inside the loop that starts each server's collection, and the loop waited for it to finish. A slow purge, 346 to 400 seconds on a large store, stalled collection for every server for that long. The purge now runs in the background, and a new purge never starts while one is still running.
- **Lite's get_index_usage no longer hides active indexes behind a silent cap** ([#4131]) - This MCP tool capped at 200 rows server-wide, with no way to ask about one database, no way to raise the cap, and no sign when rows were cut. Since unused indexes always sort first, a server with many unused indexes in one database could fill the whole answer with them, hiding every active index elsewhere, with a result that looked identical to a collection failure. It now takes a database_name filter and a limit, and reports matching_index_count and truncated so a short answer is never mistaken for an empty one. Darling's twin tool has worked this way since #2636. On Lite, Darling and the Dashboard, indexes of the same size now sort by name, so a capped answer keeps the same rows from one call to the next. An index with no recorded size, and a heap's missing index name, now sort last on all three products; Darling used to put an index with no size first.
- **pg_log_events reads a self-hosted target's csvlog file, closing a forged-line hole** ([#4124]) - Before this change, pg_log_events only read stderr-format log text. A target with csvlog but not stderr in log_destination produced no log events at all. A target with both was open to a forged line. A failed login plants a newline into a logged user or database name before authentication runs. Stderr text has no quoting, so the planted newline becomes a second, fake log line. pg_log_events now parses the target's own .csv file as CSV when log_destination includes csvlog. A planted newline inside a quoted field stays inside that field, never becoming an event of its own. Deadlocks, plan capture and the RDS route are unchanged.
- **Two install-lock tests pass on a non-elevated machine** ([#4111]) - After #4090, two `DarlingInstallLocationTests` tests failed on every non-elevated local run. There the lock cannot create `pg-runtime-prev\`, as designed, and the tests read that directory's ACL anyway. The tests now check the elevated and the non-elevated outcome separately. The elevated checks that CI runs are unchanged, and no product behavior changed.
- **MCP tool schemas no longer list an internal service as a parameter** ([#4110]) - A race in .NET reflection sometimes put a service the app supplies itself, such as the PostgreSQL connection, into tools/list as a tool parameter. It happened when two threads built the same tool's schema at once. Tool schemas now leave out every parameter whose type is a registered service.
- **Log reads through pg_read_binary_file now decode text in the database's own encoding** ([#4106]) - This route used to decode every PostgreSQL log tail as UTF-8. It now uses the connected database's server_encoding: every LATIN and ISO_8859 variant, WIN866, WIN874, WIN1250 to WIN1258, KOI8R, KOI8U, EUC_JP, EUC_CN, EUC_KR, UTF8 and SQL_ASCII. A database in one of those encodings no longer has to fall back to the text route, where one bad byte can stop the read.
- **Store Checkpointer Pressure no longer fires all the time on a healthy store** ([#4096]) - The alert compared the checkpoint sync time summed over the whole interval against a limit meant for one checkpoint. A store whose checkpoints each took a few seconds to sync breached it every hour. It now judges the average sync time per checkpoint, and a checkpoint forced by WAL volume still fires it. The alert and get_store_metrics now report the checkpoint count and the average. This adds store schema rung V140, the checkpoints_timed column on store_metrics.
- **A planted line in a PostgreSQL server's log can no longer stop plan capture** ([#4089]) - A client that fails to log in with a crafted name can put text into a PostgreSQL target's server log. A fake plan-capture line with an out-of-range query id or duration used to fail the whole read. That hid every real plan in the log tail. Darling now skips that line, counts it (forged_captures_skipped), and reads the real plans. On the pg_read_binary_file route, the deadlock and plan-capture readers also skip their pattern search when the tail holds no line they look for.
- **A Lite Query Store test that never ran now runs** ([#4084]) - The test that checks that the module filter runs after interval dedup had no [Fact] attribute, so xUnit never ran it. It now runs and passes. No product behavior changed.
- **A PostgreSQL log in a non-UTC zone no longer reports a deadlock problem on the log events collector** ([#4070]) - When log_timezone is not UTC, the log events and deadlock collectors store nothing from the server log and record why. That message said "deadlock reports" and "NOT 'no deadlocks were detected'" on both collectors. It now names the log, says that the log is not empty, and still tells you to set log_timezone = 'UTC'.
- **The install and upgrade scripts name each account once in a refusal** ([#4069]) - A folder made under `C:\` inherits two entries for `BUILTIN\Users`. One allows appending data, and one allows writing it. The [#4050] refusal listed `BUILTIN\Users` twice for the same folder. It now lists each group or account once for each folder.
- **One bad byte from a failed login no longer blinds the three PostgreSQL log readers, and RDS and Aurora targets get the time-zone fix too** ([#4051]) - A client that fails to log in with a crafted role or database name plants one invalid character in the server log. That character used to stop deadlock capture, plan capture and log-event collection from reading the server log until it aged out. Grant the monitoring role EXECUTE on `pg_read_binary_file`. Then, on a UTF8 or SQL_ASCII database, Darling reads the log in a way that tolerates the bad character. On other encodings, the error message says that the grant does not help there. While that grant is missing, Darling reminds you once a day. RDS and Aurora targets whose `log_timezone` is UTC now skip a planted line that is stamped in another zone, instead of refusing the whole read. Self-hosted targets already did this.
- **Darling's install and upgrade scripts refuse a folder that ordinary users can already write to** ([#4050]) - When you extract the zip into a folder made directly under `C:\`, the folder inherits write access for every signed-in user. The #4038 lock stops writes only from the moment that it runs, so it could never catch a binary that was swapped before then. `install-darling.ps1` and `upgrade-darling.ps1` now check every file in the tree, and the existing install, before they change anything. They name who can write to the tree, and each folder that they cannot look inside. Then they refuse, unless you pass `-AcceptWritableExtraction` for a deliberate dev loop. An upgrade copies its zip to an administrators-only folder and verifies the zip there before it extracts anything. Every doc and message now names `C:\Program Files\PerformanceMonitorDarling` as the install folder. This includes the refusal for a folder inside a user profile, which used to send people to a folder that this check refuses.
- **A planted log line in another time zone no longer stops a PostgreSQL server's log events and deadlocks from being read** ([#4049]) - Before this fix, one line stamped in a zone other than UTC anywhere in the log tail made Darling refuse every log-event and deadlock read of that server, for as long as the line stayed in the window. Any client that can reach the port can put such a line there with one failed login, because %u and %d in log_line_prefix echo the requested role and database names unescaped. Now the self-hosted readers check the server's own log_timezone in the same query. When it is UTC, a line in another zone cannot be the server's own: it is skipped and counted on the collection log row, with a note naming log_timezone and #4046, and the read goes on. A server whose log_timezone really is another zone is still refused, as before. RDS and Aurora read the log through the AWS API, cannot check the setting, and are unchanged. Plan capture never read the zone and was not affected. A failed login whose role name carries a byte that is not valid UTF-8 still makes the server's pg_read_file fail for the whole window, under any prefix. [#4051] in this release covers that when the monitoring role has EXECUTE on `pg_read_binary_file`.
- **PostgreSQL servers whose log prefix puts fields before the pid now show their log events and deadlocks, and a client's port is no longer stored as an event's SQLSTATE** ([#4047]) - Under a log_line_prefix such as '%m %u@%d [%p] ', the log reader matched no line, so the server stored no log events and no deadlocks and looked quiet. It now reads fields between the zone and the pid, and it still takes the pid from the first bracket and the label from the line itself, so text inside a statement cannot open an event. The self-hosted deadlock read also stopped missing reports under '%t' prefixes (pgBadger's recommended one) and colon-delimited prefixes. Under any prefix with %r, the RDS default among them, the client's five-digit port was read as the event's SQLSTATE, often one in the resource or system-error classes. Now the port inside %r is ignored. A bare five-digit %l, %x or %v in the prefix can still be read as a SQLSTATE. Telling those apart from %e needs the target's own log_line_prefix.
- **On Windows the log-hash key is refused if anyone beyond SYSTEM, Administrators and the service account holds any right to it, and it is checked and read through one held file handle** ([#4044]) - The key was trusted through the check it shares with the credential files. That check refuses only a file Users, Authenticated Users or Everyone can read, so INTERACTIVE, Domain Users or a single named user with read access passed it, and so did an account that could write the key or change its permissions. That account could plant a key of its own or grant itself the read, and then test guesses against the stored log hashes. The key is now held to an allowlist of the three accounts the service writes, for every right. It is opened once in a way nobody else can rename, replace or write while it is open, and it is refused if it is a link or has a second name. Its owner and permissions are checked through that same handle, and the read is capped at 1 KB. When a key is refused, log events stop being collected and the refusal names the accounts to remove.
- **A PostgreSQL server that logs only to CSV is reported by name instead of looking quiet** ([#4040]) - With log_destination set to csvlog, PostgreSQL still leaves a small stale stderr-format file in its log directory, and the plan, deadlock and log-event collectors read that file every cycle and reported a server with nothing to say. They now check log_destination itself and report that it has no stderr output, with the one-line fix (add stderr and reload, no restart needed).
- **Install and upgrade lock the Darling install folder, so an ordinary local user can no longer replace the service's binaries** ([#4038]) - The documented install location, a folder made directly under C:\, inherits write access for ordinary local users from the drive root. On Windows client editions that is Modify for every authenticated user, so any local user could replace the service exe, a DLL, or the bundled postgres.exe and have their code run as the service account. install-darling.ps1 now locks the folder before anything runs from it: it stops the folder inheriting, removes the broad groups, makes Administrators the owner, closes any write grant a user left on a file of their own, and grants back read and execute to Users and Modify to the service account only. upgrade-darling.ps1 applies the same lock as soon as the service stops, so an existing install is closed at its next upgrade, and anything that stays open is named with the command that fixes it.
- **The hourly re-mask of PostgreSQL deadlock data stored before #4005 now runs without a log-hash key, covers finding alerts, and never leaves a raw report hash behind** ([#4036]) - a service with no usable log-hash key (an untrusted key directory or ACL, an unreadable key file, or DPAPI after the service moved machines) skipped the whole re-mask, so raw SQL literals stayed in deadlock reports, alerts and findings for their full retention. Now everything is re-masked and only re-keying an alert whose report is gone waits for a key. Analysis finding alerts sent before #4005 kept a raw report hash and the raw victim statement in their stored detail, which the Viewer's alert detail showed. They are rewritten from the finding, or withheld once the finding is gone. A finding whose deadlock fact sits behind its root is rewritten too. An alert missed by the pass is keyed rather than keeping its raw hash, a stage that gives up is retried a day later instead of waiting for a restart, a page cut short by the hourly budget, or a row moved by a dismissal or VACUUM FULL, can no longer end a walk early with a row still raw, and with a log-hash key no raw report hash is sent to the store as a query parameter, where log_min_duration_statement would copy it into the store's own log. An alert is never cut from its report by the order the pass rewrites them in, and a finding alert is rewritten only from the analysis run it was sent for, never from a later run of the same story. Each hourly pass works through pages until its 60-second budget. On a test table of 120,000 compressed rows (20,000 reports, each seen 6 times), one pass rewrote about 5,500 reports. The whole table took about 4 passes. Once every row was rewritten, a walk of the whole table took 2.7 seconds. The README says precisely what is rewritten: pg_log_events rows from before #4020 keep their old hash until their 30-day retention drops them.
- **On compose, a log-hash key or role password planted in a credentials directory that was open to other users is discarded instead of trusted, `--harden-files` no longer follows a junction or a hard link, and the Viewer's always-empty Statement Fingerprint column is gone** ([#4031]) - Each file the service read judged the credentials directory on its own, and role provisioning set the directory owner-only before the log-hash key loaded, so a key planted while a bind-mounted directory was world-writable was trusted and kept, letting whoever planted it test guesses against the stored log hashes. Worse, a start that closed the directory but never provisioned (a store that was down, or a crash) left a planted admin password for the next start to re-assert on the admin role. Now whichever check first finds the directory open to other users' writes removes the role passwords and the key there and then, logs it as an error, and new ones are generated. The first `pg_log_events` run after a replaced key says so on its Collection Log row. Every credential name is tried, so a directory planted at one name no longer shields the files at the others. A directory with something in it is left alone, the credentials directory stays owner-only, and that start uses nothing in it and names each path to remove. Run one service per credentials volume (on Kubernetes, `strategy: Recreate`). `--harden-files` now covers the log-hash key, refuses (exit 1) any target reached through a junction or symbolic link, or a file with a second name (a hard link), instead of rewriting whatever it points at, checks every folder on the path when darling.json sits at a volume root, and only touches `darling-keys` on a bring-your-own install, where it is the key's directory. A directory left where the key's temporary file goes is now named as the reason instead of "it did not exist". The PostgreSQL log grid no longer shows a Statement Fingerprint column that could never fill, and the key is flushed to disk before it is renamed into place.
- **Trace flags could show a flag as enabled days or weeks after it, and every other flag, was turned off** ([#4030]) - get_trace_flags, the web viewer's Trace Flags grid, and Lite's equivalent all read the newest trace_flags row to decide what is currently set. A capture that finds every flag off writes no row at all, so the read silently fell back to an older capture that still had a flag on. Both surfaces now also check the trace_flags collector's newest successful run in the collection log, so a capture that found nothing on is recognized as current and reports no flags instead of a stale one.
- **On-load collectors could read HEALTHY forever even after their daily reschedule silently broke** ([#4029]) - server_config, database_config, database_scoped_config, trace_flags and server_properties are on-load collectors: they capture on every connect, and since #3929/#3930 also recapture once a day. Collection health banding still exempted them from every staleness check, so a daily reschedule that quietly stopped firing kept reading HEALTHY as long as its historical runs were all successes. These five collectors now band on the same STOPPED/FAILING/STALE ladder as any other collector, at their daily cadence, on the collection health grid, the fleet overview and the get_collection_health tool.
- **The PostgreSQL log tail could pick the csvlog or jsonlog file instead of the real log** ([#4025]) - On a target logging to both stderr and csvlog or jsonlog, the shared reader behind plan capture, deadlock capture and log events picked whichever file had the newer timestamp, and a csvlog or jsonlog file could win the tie. Its contents do not look like an ordinary log line, so the cycle read nothing and reported the target as quiet instead of naming the real file it needed. The tail now always reads the real log file, and a target with no such file gets a clear, named result instead of a silent miss.
- **PostgreSQL deadlock reports, deadlock alerts and analysis findings stored before the SQL normalization are rewritten in place** ([#4022]) - Since the deadlock SQL was normalized, every read showed older data normalized, but the stored rows kept their raw queries and a hash over the raw report: deadlock reports for their 90 days, deadlock alerts fired before the fix in the alert history, and analysis findings in their deadlock drill-down for 30 days, all readable by any role with a direct SELECT such as the MCP login. The service now rewrites them in small hourly slices. Each report takes the text every read already showed and the current identity, so its sightings group with newer ones. Each alert takes the incident the live alert builds today, and each finding takes what its read already showed. Only rows still raw are touched, a second pass changes nothing, and no store migration is needed.
- **PostgreSQL log events are identified by hashes keyed with a per-store secret, so a store reader can no longer test guesses at the values a log line hid** ([#4020]) - `pg_log_events` stored `raw_line_hash` and `statement_fingerprint` as plain SHA-256 over text a store reader can mostly rebuild, so anyone who could read the rows could test candidate values offline. A review recovered a four-digit PIN in 6 ms. Both are now HMAC-SHA-256 under a 32-byte key the service generates once and keeps outside the store: DPAPI-protected beside the managed credentials, in the `darling-credentials` volume on compose, and in `darling-keys` beside `darling.json` on bring-your-own, owner-only in each case. The service never replaces an existing key. If the key file cannot be trusted or read, log-event collection stops with an error that names the file and says how to fix it or delete it to rotate. No tool or web view returns either hash any more (`statement_fingerprint` left `get_pg_log_events` and the log tab). Rows stored before the upgrade keep their old hashes until retention drops them, so each log entry still in the overlap window is stored once more, and each statement's fingerprint changes once.
- **Plan capture could be spoofed by a statement's own author** ([#4015]) - Plan capture matched auto_explain's plan header anywhere in the server log, not just at the start of a real log line. PostgreSQL echoes a rejected statement back verbatim after a syntax error, so a user who could run SQL on a monitored target could write a fake header into their own query text and have it read back as a real captured plan, attached to any query id and duration they chose. Both the plan parser and the self-hosted collector's SQL now require a genuine log-line prefix immediately before the header, so only PostgreSQL's own auto_explain output can produce a stored plan.
- **PostgreSQL deadlock reports store and show their SQL normalized, and no read returns a hash of the raw report** ([#4013]) - The deadlock collector stored each report's queries and the victim's statement exactly as PostgreSQL wrote them, literals included, and get_pg_deadlocks, get_pg_deadlock_detail, the web and desktop viewers, the deadlock alert's email and webhook bodies, and the analysis drill-down returned them that way. Every query is now put through the same normalizer as the log events before it is stored: each literal becomes ?, and a query cut inside a literal is withheld. Every read also normalizes reports stored before this change, including stored analysis findings. The report hash, which could be used to test guesses at the hidden values, is now computed over the normalized report, and an older report is named by its time and process ID instead. A logged statement that quoted a deadlock message is no longer stored as a deadlock, and the graph panel now shows the newest reports first.
- **Analysis passes stop recomputing every 30-day baseline on every pass** ([#4011]) - Each scheduled analysis pass built its baselines from scratch. That meant 9 to 12 thirty-day store reads per server per pass, and on a 31-day test store they were 76 to 94 percent of a pass's time. `analyze_server` and `compare_analysis` then read the same baselines again. The Darling service now keeps one shared set of computed baselines for its scheduled passes, its MCP server and its web dashboard, so a baseline is read from the store once per analysis hour between them. A second pass in the same hour read none. On the test store, a PostgreSQL target's pass fell from about a second to between 36 and 301 ms. Only successful reads are shared, so one caller's timeout cannot hide another caller's baselines. The baseline window now ends on the analysis hour instead of the analysis minute, which is what makes a shared baseline identical to a fresh one. Lite gets the same fix for its scheduler, MCP server, Recommendations tab and overview charts.
- **CPU scheduler readings no longer flicker between two answers when a collection lands on a duplicate timestamp** ([#4010]) - A run-overlap or clock-resolution collision could occasionally store two different cpu_scheduler_stats snapshots under one collection_time. Every reader that shows the current scheduler reading, the fleet card's Threads chip, get_cpu_scheduler_pressure, the CPU Scheduler viewer tab and overview, and the RUNNABLE_TASKS analysis fact, picked between the two snapshots with no tiebreak, so the value could change from one refresh to the next with nothing actually changing on the server. Collections are now stamped so this cannot happen going forward, and every affected reader in both Darling and Lite breaks a tie deterministically, correcting reads of rows already stored this way.
- **The rest of the trend family now buckets to a point budget instead of returning every collection** ([#4007]) - get_wait_trend, get_cpu_utilization, get_tempdb_trend, get_memory_trend, get_perfmon_trend and get_pg_query_duration_trend used to return one point per collection, so a week of one-minute data ran from 160 KB up to 2 MB depending on the tool. They now bucket to the same roughly 200-point budget the file I/O, lock wait and duration trends already used, with an optional bucket_minutes to ask for a specific width. A week of get_tempdb_trend is now under 50 KB by default instead of nearly 2 MB, with the same shape of change across the other five. get_memory_trend's memory-grant figures also switched from matching the nearest grant snapshot within 30 seconds to averaging every grant snapshot inside a point's bucket, which answers correctly for a bucket that spans several snapshots instead of picking one arbitrarily.
- **get_pg_io_trend's automatic subject choice no longer scans the whole window in order** ([#4003]) - Calling get_pg_io_trend without a backend type or context asked it to sort every pg_io_stats row in the window before picking which one to follow, which took nearly 4 seconds over a week on a busy TimescaleDB store. It now reads each candidate's starting and ending values instead, which answers in a fraction of a second and still falls back to the exact original method whenever a counter reset could make that shortcut wrong. The chart it draws does not change.
- **Custom alert rules now evaluate on the Linux compose store** ([#4002]) - On the compose distribution, a user could author custom alert rules in the web dashboard, but they were never evaluated and never fired, since the evaluator only ever built its metric-reading pool from a managed Windows store's credential. The evaluator now reads with the compose store's least-privilege viewer role, which [#3983] set up for the web dashboard. Its rules now fire like any other. A bring-your-own store is unaffected: its rules still do not fire, since its viewer role does not have the resolve function the evaluator needs.
- **On-load config collectors recapture daily, so a long-lived connection keeps fresh config facts and a cleared trace flag actually clears** ([#4001]) - server_config, database_config, database_scoped_config, trace_flags and server_properties captured once per connect and never again while the connection stayed healthy. A server connected for more than 30 days lost its only capture to the 30-day retention purge, so CONFIG_*, DB_CONFIG, TRACE_FLAGS and the config audit had nothing to read until the next reconnect. Separately, TRACE_FLAGS took each trace flag's latest row across all retained history with no time bound. DBCC TRACESTATUS(-1) lists only flags that are currently on, so a flag turned off never got another row to supersede its stale ON one, and it kept reporting as enabled for up to 30 days after being turned off. Both SKUs now also re-run the on-load collectors on a daily cadence, in addition to on connect, so a long-lived connection's snapshot never gets close to aging out of retention. TRACE_FLAGS now reads the newest capture inside that same daily-aware window (about two days at the shipped cadence), so a flag turned off drops out at the next daily capture. When every flag is off, a capture writes nothing, and the window bounds the fallback to the capture before it. An operator's own schedule override for any of these collectors is unaffected.
- **get_store_metrics reads each store object's newest sample through an index instead of sorting a year of rows** ([#3998]) - The store-metrics latest read took each object's newest row with a DISTINCT ON over the whole `collect.store_metrics` table. The table keeps 400 days of hourly sweeps, and the only index was on the time column, so every call sorted every retained row: 7.8 seconds and a sort spilling to disk at full retention on a CI-sized store, and growing for over a year after a store is created. The read now steps from one object to the next through a new `(object_kind, object_name, metric_time DESC)` index and takes each object's newest row directly. The answers are identical, and the same read at full scale takes 19 ms.
- **list_servers and the per-server summary tools no longer plan a store's whole collection history to find one timestamp** ([#3995]) - list_servers, the Overview summary card, and the get_server_summary tool each read a server's newest collection with a query shape that forced TimescaleDB to plan every retained day of history before returning an answer that takes a fraction of a millisecond to compute. These reads now use the same fast per-server lookup the sidebar's freshness indicator already used, so a store that has run for months answers just as quickly as a brand new one. Answers are unchanged: a server that has gone dark still reports correctly.
- **PostgreSQL config and logging-audit tools no longer plan a year of chunks to read one row** ([#3992]) - get_pg_server_config, get_pg_logging_audit and the PostgreSQL config panel read the newest configuration snapshot with no lower bound, so on a store that had run for months TimescaleDB spent seconds planning every retained daily chunk before returning an answer that took milliseconds to compute. These reads now bound the same way the collector's own config reads already did, planning only the last day's chunks and falling back to the full history only for a target whose collector was dark for a while. Answers are unchanged.
- **A store with no readable server-log directory keeps its hourly collector-cost flush** ([#3985]) - The store-log capture's own catch kept its failure from stopping the hourly tick. But Npgsql closes the connection outright on the capture's error, so the re-mask and the collector-cost flush that ran next on that connection failed with "Connection is not open". That happened every hour on a store with no log directory, which is the Linux compose store's shape, and `get_collector_cost` never got a row there. The connection is now reopened before either runs. A capture the store will never allow (no log directory, or a login without the log read) now warns once per process instead of every hour, and `get_store_log` still reports the gap. Separately, the sizing pass gets its own narrow catch, so the next statement a store permanently rejects there can no longer take the store-log census and the collector-cost flush down with it.
- **Lite's daily summary starts its retention horizon where Lite's history actually starts** ([#3984]) - `get_daily_summary` and `get_daily_summary_range` put the retention horizon on the archive cutoff day, three months back. The archive cleanup deletes a whole month's file once that month falls before the cutoff, so the days between the cutoff and the next month start were already gone. Those days were judged as retained with no data instead of purged, and `retention_horizon` read up to a month earlier than the data really goes. The horizon is now the first month start the cleanup keeps, the same instant the Overview freshness band uses since [#3975].
- **The web dashboard and the MCP server no longer connect to a compose or bring-your-own store as its owner** ([#3983]) - Outside managed mode both hosts used `postgres.connectionString`, the collection login, which owns the store. So a web session or an MCP token-holder there had none of the `viewer`/`mcp` roles' protections: no secret-column carve over the SMTP password, webhook URLs and stored server passwords, no narrow write grants, and no `statement_timeout` backstop. The README said they had all three. Three changes follow Erik's ruling on #3914. First, the Linux compose store provisions the `admin`, `viewer` and `mcp` roles itself, the way a managed store does, and the two hosts connect as `viewer` and `mcp`. The service recognizes that store as its own because its login is the store's bootstrap superuser (`POSTGRES_USER`) and the cluster holds no other database, so nothing needs configuring and an existing compose deployment gets the roles on its first start with the new image. The generated passwords are one owner-only file per role in a new `darling-credentials` volume. The roles carry their own marker, `darling-compose`, so the service never re-keys a role `provision-roles.sql` created; a same-named role it did not create is left alone. When a start does not provision the roles (a refusal, a failure, or a collector that stopped first), each surface keeps its role with the credential an earlier start wrote, and falls back to the owner, with the reason, only when there is none. Every start also switches off the roles' widening attributes and revokes any role membership granted to them. Second, a bring-your-own store gets two optional settings, `postgres.webConnectionString` and `postgres.mcpConnectionString`, in the same forms as `connectionString`. Left unset, a surface connects as the owner and the service warns at startup that it gives up the carve, the narrow write grants and the backstop. `provision-roles.sql` now creates the `mcp` role too, with managed's grants and carve, and gives `viewer` the `custom_alert_rules` write it was missing, so the web rule editor works when pointed at it. Third, a fresh managed or compose store let the MCP tools read the continuous aggregates only after a restart, because they are created after provisioning and `mcp` had no default read on `collect`; it now has one, SELECT on `collect` only. The README, `get_store_query_stats` and the instructions now describe the owner-identity behavior accurately.
- **A fresh store's fleet overview and web Fleet page work before the collection-health aggregate materializes** ([#3981]) - On every new TimescaleDB store, `get_fleet_overview` answered "Out of range of DateTime" and `/api/fleet` returned a bare 500, so the web dashboard's Fleet Overview and sidebar were broken until `collection_health_hourly` had materialized something. That meant until collection had rows and an hourly refresh had covered them, because a refresh over an empty log leaves the aggregate unmaterialized. The reader guarded the aggregate's watermark with `isfinite()`, expecting "nothing materialized" to read `-infinity`. TimescaleDB reports the minimum finite timestamp (4714 BC) instead, and Npgsql cannot hold that as a date. A watermark earlier than 2000 now reads as nothing materialized, so the collection-health half is read in full from the log, as designed. A guard read the client cannot convert now falls back to the raw scan instead of failing the overview.
- **Latest-value lookbacks follow the collector's cadence, and the PostgreSQL target's config reads stop planning every retained snapshot** ([#3980]) - [#3931] bounded the analysis latest-value reads to a day, but collector cadences are editable. A database-size, memory-clerk or plan-cache collector scheduled daily or slower lost its facts from 24 hours after each sample until its next run, so the findings built on them resolved and re-fired. The lookback is now a day or twice the collector's actual interval, whichever is longer. The interval comes from the same schedule rule the collector runs by: Darling's per-server and fleet-wide overrides, Lite's schedules, the Dashboard's `config.collection_schedule`. A collector set to run only on connect anchors on its newest capture however old. Every default cadence is under twelve hours, so nothing changes until someone slows a collector down. Separately, the PostgreSQL target's five reads of the newest `pg_server_config` snapshot had no lower bound. A year in, TimescaleDB planned all 366 one-day chunks: 4.0 to 8.3 seconds of planning per read, four of them on every analysis pass. They now read the last day first and fall back to every retained snapshot only when that finds nothing, so every answer is unchanged.
- **The top-CPU drill-down resolves statement text for the five queries it prints, not for every plan-cache row in the window** ([#3979]) - The drill-down behind CPU_SQL_PERCENT and CPU_SPIKE findings projected query_text inside its window. On Darling, v_query_stats resolves that column from the fleet's query-text dimension for every row, so the read looked up and sorted the whole window's text to print five. It now ranks and cuts to five without the text, then reads the same MAX over the same rows for the five that print. A group with a NULL key reads NULL-safe, because GROUP BY groups NULLs and equality cannot match them. Lite runs the same SQL, byte-identical by pin, and the frozen Dashboard mirrors it; its MAX over every window row's compressed statement drops from 1,140-1,360 ms to 32-47 ms on a 72K-row window. On a rig with a 400K-text dimension and a 72K-row window, Darling's read goes from 260.6 ms to 132.2 ms. Output is identical in all three SKUs against an old-SQL oracle, NULL keys and trap rows included.
- **A server dark past the collection log's retention reads Offline everywhere, not "Awaiting first collection"** ([#3975]) - [#3966] fixed this on the fleet card. `list_servers`, the viewer's sidebar dot and Overview card, and Lite's Overview card still read "no collection found" as "never collected". `collection_log` keeps 60 days and Lite's archive keeps three months, so a decommissioned server left enabled read Offline until its last row aged out, then turned amber: "Awaiting first collection" on Darling, and "Never" with "No collection has ever landed for this server" on Lite. One shared rule now bands a server with no collection in view by when it was registered. A server registered before the earliest instant the read can see is Offline; one registered since then is still awaiting its first collection. Lite's horizon is the first month start its archive cleanup keeps, because the cleanup deletes a month's file whole.
- **The service's start-up hole scan no longer reads every aggregate's whole materialization and source table** ([#3972]) - At every start the service scans each of its 23 continuous aggregates for materialization holes (#3731), and the scan's two per-bucket checks ran as joins: one read the aggregate's entire materialization, and the other read the entire source table once for every bucket with nothing materialized, which after an outage is every outage hour. On DARLING01 the wait-stats baseline's check wrote all 9.8 million wait_stats rows to disk and read them four times, 4.5 seconds for one aggregate. Each check now runs once per bucket as an index lookup, and finds exactly the same holes. One start's scans across all 23 aggregates went from 5.9 s to 75 ms there.
- **The trend tools answer in time buckets sized to the window: a day of `get_file_io_trend` is 34 KB, not 1.4 MB** ([#3968]) - `get_file_io_trend`, `get_lock_wait_trend`, `get_query_duration_trend`, `get_procedure_duration_trend`, `get_pg_io_trend` and `get_pg_database_trend` returned every collection in the window, so an answer grew with the window and the collection cadence: a default day of file I/O on a monitored SQL Server 2022 was 12,500 points and 1.4 MB, and a week 9.8 MB. Each now gathers its collections into buckets sized to keep the answer near 200 points (a day is ten-minute points, a week hourly), the same way on Lite and Darling; `bucket_minutes` asks for another width up to a per-tool cap that keeps the largest answer under 256 KB, and a width out of range or over the cap is refused with the width that fits. Counts are summed and rates and ratios recomputed from each bucket's sums, never averaged; every point keeps its worst single collection as a peak, so a spike survives; and the answer says what a point is (`bucket`, `bucket_minutes`, `aggregate_note`). `get_file_io_trend` charts each database's data and log files as two lines ranked by I/O stall, folding everything past the top five into one `(other)` line, and `database_name` charts one database file by file; it used to list tempdb's files under one name, indistinguishably. `get_lock_wait_trend` charts the whole lock family as one series with a legend of the types that waited, where it returned a row per type per collection, mostly zeros. On the PostgreSQL pair `point_count` now counts buckets and `interval_count` the intervals it used to count. The web viewer charts the same reads at a larger point budget, and `get_query_heatmap`'s `bucket_minutes` and `metric` refusals now carry the `invalid` status and the parameter name.
- **A server dark for more than two days reads Offline on the fleet card, not "Awaiting first collection"** ([#3966]) - The web fleet page and `get_fleet_overview` look back 48 hours for each server's newest collection. A server silent for longer fell out of that read and was banded as if it had never collected: an amber "Awaiting first collection", counted under Warning, with the same words in Needs attention. The WPF viewer called the same server Offline. The window stays, because planning the read over every retained `collection_log` chunk on every call is the cost it exists to avoid. The read now also returns each enabled server's registration instant from the registry. A server registered before the window with nothing in it reads Offline and counts in `offline_count`. One registered inside it that has not collected yet still reads awaiting. The web card's Offline status line shows how long ago the last collection was, or "no recent collection", instead of a time of day that read as today.
- **A restart's shutdown checkpoint no longer counts toward checkpoint write and sync time either** ([#3964]) - The shutdown checkpoint's own write and sync work lands in the same counters as the live checkpoints', and nothing can separate the two, so an interval that spans a PostgreSQL restart is now judged on neither arm. Store Checkpointer Pressure neither fires nor resolves on it, even when the shutdown sync is slow, and get_store_metrics reports the interval as status Restarted with no delta. PG_CHECKPOINT_PRESSURE leaves restart intervals out of its write, sync and checkpoint-buffer figures as well as the requested count, and get_pg_write_stats returns checkpoint write time, sync time and checkpoint buffers as null for a window that holds a restart. The cost is one skipped interval after a restart; the next interval is judged normally.
- **PostgreSQL config change history reports per-database and per-role overrides being set, changed and reset** ([#3957]) - `get_pg_server_config_changes` and its web twin said nothing about `ALTER DATABASE ... SET` or `ALTER ROLE ... SET`. The override rows V138 added to `pg_server_config` had to be kept out of the server-wide change read, and even a per-scope comparison could only see an existing override's value move, never one being set or reset. A second read now compares each pair of consecutive snapshots for every database and role scope. An override that appears reads `set`, one that disappears reads `reset`, and a moved value reads `changed`. Overrides already present when the store first captured them are the baseline and are not reported as set. The server-wide rows, their order and their payload shape are unchanged, and a page with no scoped rows is byte-identical to before.
- **The plan-regression drill-down no longer re-deduplicates the whole Query Store slice, and the parameter-sensitivity drill-down stops resolving text for rows it never prints** ([#3956]) - Every analysis pass that raised a PLAN_REGRESSION finding ran the fact's 14-day Query Store deduplication twice: once for the fact, and again in the drill-down that lists its top five. Those five are the head of the ranking the fact had just computed. The fact now records the queries it reported on the pass, and the drill-down deduplicates only those queries' rows. It reads the whole slice, as before, when the fact did not run, failed or found nothing. Lite and the Dashboard do the same. On a rig seeded to the production funnel (4.15M raw rows per server), the drill-down went from 27.1 s to 0.41 s, and on DARLING01 from 194 ms to 2.1 ms, with md5-identical output. The parameter-sensitivity drill-down read v_query_stats, which joins the fleet's query-text dimension for every row in the window and carries the text through its sort. It now resolves text for the five rows it prints: 520 ms to 165 ms on the rig. The Dashboard's twin carried each row's compressed statement through the same sort, and it now does the same. PlanRegressionSql's own cost needs a storage change, proposed in [#3953].
- **A PostgreSQL restart no longer raises a false Store Checkpointer Pressure warning or reads as a WAL-forced checkpoint on a monitored server** ([#3955]) - PostgreSQL counts a shutdown checkpoint as a requested checkpoint and keeps the count across the restart, so every service restart that stopped the store fired Store Checkpointer Pressure as WAL-forced checkpoints, and a monitored server's restart read as WAL pressure in PG_CHECKPOINT_PRESSURE and get_pg_write_stats. Every checkpointer sample now records the server's postmaster start time (store schema V139), so an interval that spans a restart is recognised and no longer counted as a requested checkpoint. get_store_metrics and get_pg_write_stats report postmaster_restarted and postmaster_start_time so the gap is explained rather than hidden.
- **Log masking fails closed: a DETAIL or CONTEXT is read as SQL only where PostgreSQL writes SQL, a value cut before its close is masked to the end, and `get_store_query_stats` masks a raw text `pg_stat_statements` kept** ([#3952]) - A fourth review of [#3920] found values still reaching the viewer and mcp roles through the store's own log and every PostgreSQL target's `get_pg_log_events`. The DETAIL and CONTEXT masking split at any line shaped like `Process N:` or `SQL statement "`, a value's own lines included, which kept a literal's middle as identifiers and cut a key tuple or failing row away from the close its mask needed; now a DETAIL is SQL only in a deadlock report (a `Process N:` line starts a query only for one of the deadlock's own processes, and only once the query before it reads to its end), a crash report or a logged EXECUTE's `prepare:`, and a CONTEXT frame is SQL only where a frame can start. A pasted value that reads as an identifier to PostgreSQL (a no-break space, full-width digits) is masked, every prose value shape masks to the end when its close never arrived, a field over 64 KB is withheld and a hostile field can no longer stall capture (one key-tuple pattern took minutes on 90 KB), and `get_store_query_stats` masks the statement text `pg_stat_statements` stores raw when an entry is evicted or reset mid-statement. Covered by the reviewer's inputs as regression tests, timing pins for the hostile shapes, and a live test that forces the raw text into `pg_stat_statements`.
- **The web server page's Overview tab no longer waits on the daily summary** ([#3950]) - On the largest production store the two daily-summary reads were 26 of the Overview tab's 28 seconds, and one of them ran into the `viewer` role's 15 s timeout. The month read was slow in its query count, not in its raw sources. The check that names a day the rollup never materialized (#3653) ran as a join over the server's whole query history, rescanned once per day of the window and spilled to disk, and the distinct-query count sorted every rollup row of the month. Each check now runs once per day as an index lookup and the count is hashed, with identical results on every tier. On one busy server's seeded data the month read went from 3.3 s to 0.19 s and a 120-day read from 12.4 s to 0.16 s. The rollup-coverage lookup took 1.7 s or more in production and ran on every call. It is now cached for five minutes, concurrent reads share one lookup, and a failed lookup is retried rather than routed on. The tab's today tile now comes from the same read as the month grid, so the page runs one daily-summary query instead of two, and the tile's unique-query count matches the grid's last row.
- **The fleet overview reads each server's newest sample instead of every retained row, and the web viewer asks for it once per refresh instead of twice** ([#3947]) - Every `get_fleet_overview` call and every `/api/fleet` request behind the web sidebar, fleet page and server page read its four "latest value per server" metrics (CPU, memory, memory-grant pressure, worker threads) with a `DISTINCT ON (server_id)` or a `MAX` joined back over the whole table. That read decompressed and sorted every retained row to keep one per server. It also counted blocking and deadlocks bounded only on the event's own timestamp, which no chunk is partitioned on, so a one-hour count opened every retained chunk. Both grew with servers × retained days, and one overview took 12.3 s cold on a 43-server store. Each newest-row read is now one `LATERAL ... LIMIT 1` probe per registry server, which TimescaleDB's ordered ChunkAppend stops in the newest chunk; PostgreSQL targets, which never write those tables, are skipped. The incident counts carry a `collection_time` floor one chunk before the window, which keeps every row the window wants because a row is collected after its event. The 48-hour last-collection read is a per-server probe too. On DARLING01 the sub-reads went from 1,317 ms and 26,836 buffers to 32 ms and 182, returning identical rows. The web viewer's sidebar and page each fetched `/api/fleet` on every 60-second tick, doubling the cost per open tab; they now share one in-flight request. The WPF viewer's copies get the same fixes (its sidebar freshness read was a `GROUP BY` over every retained `collection_log` row on every refresh, 303 ms to 13 ms on DARLING01), as do `get_server_summary`'s windowed counts, and Lite's Overview card reads its newest CPU, memory and last collection from the hot table before falling back to the archive.
- **A PostgreSQL target's analysis reads its statements' own baselines once per metric, not once per statement** ([#3946]) - The bad-actor share detector and the plan-regression detector asked for each candidate statement's hour-of-week baseline one at a time, and each ask read the server's whole 30-day `pg_statement_stats` slice (the table's only index is (server_id, collection_time), so the statement predicate is a heap filter; the share arm also re-summed the same per-collection total each time). A cold pass paid five share reads plus one per flipped statement, and on the production PostgreSQL store that was about 70 % of a cold `analyze_server`. Each detector now asks once for its whole candidate set: the keyed arm makes the per-statement arm's own read with one filter per statement and runs the unchanged robust scaffold once per statement over it, so every statement's buckets are the ones its own read produced (identical tiers, counts, medians and MADs; means and standard deviations to float8 summation order). On DARLING01's PostgreSQL target the five share reads (920–1,053 ms) became one (409–466 ms), and on a 4.3M-row compressed month the share read halved and the mean fell by a third. The per-statement scaffold is the floor that remains; scheduled passes never reusing the baseline cache is tracked separately (#3941).
- **`get_store_metrics` answers with a bounded summary instead of every store object's daily series** ([#3942]) - The tool returned every store object's latest row and its daily series on every call. The object count is fixed by the schema (about 250: 72 hypertables, 25 continuous aggregates, 146 background jobs), not by the fleet, so the default answer was about 1.9 MB on every production store. That is around 500k tokens, more than an MCP client's context, and it was 1,246 KB on the dogfood store. The default is now the store-level blocks (inventory, retention, job_history, checkpointer, and the whole-store daily growth with the per-server ingest rate) plus three ranked lists bounded by `limit` (default 10): the largest objects, the fastest-growing, and the background jobs, failing ones first and then those closest to their cadence. Each row carries its change over the window in place of its series. `object_kind` lists one kind, and an exact `object_name` returns that object's daily series. On the dogfood store's own rows the default answer drops from 1,246 KB to 19 KB. Wire change: `daily` is gone from the default response, `objects` is a bounded page, and job rows no longer carry the byte fields, which were always null. The store alerts' triage page gains a background-jobs section.
- **Managed role provisioning never sends a role's password in a statement** ([#3940]) - Every service start sent all three login roles' plaintext passwords in `CREATE ROLE` / `ALTER ROLE ... PASSWORD`, and any surface that records statement text could keep them: an operator-created role with a managed role's name made provisioning fail on every start, and the store's own log then wrote the whole batch, passwords included, where the viewer role reads it for 400 days. Provisioning now sends each role's SCRAM-SHA-256 verifier, computed client-side (PostgreSQL stores a verifier as-is, which is what psql's `\password` does), and only for a role whose stored verifier no longer accepts its credential file, so a steady-state start sends none. `tools/provision-roles.sql` recommends a verifier or `\password` for a store you run yourself.
- **A store without TimescaleDB re-runs its store-object convergence every hour, not only at restart** ([#3932]) - #3817 made the service re-run its store-object convergence hourly (the baseline views, the store's statement statistics, the composer's covering indexes), but the hourly pass sat inside the tick's TimescaleDB gate, so on a plain-PostgreSQL store, a supported configuration, a dropped fallback view or an extension a DBA created by hand waited for the next restart. The hourly pass now runs on every store shape and skips only the steps that need TimescaleDB, the same ones the start path gates.
- **Analysis latest-value reads look back a day instead of scanning a server's whole history, and stop counting dropped databases** ([#3931]) - The database-size, autogrowth, disk-space, memory-clerk, plan-cache and memory facts, and the autogrowth drill-down, took each series' newest row with no lower time bound. So every analysis pass numbered every row the server had retained, and TimescaleDB could exclude no chunk. On the largest field store `DatabaseSizeSql` alone was about a tenth of `analyze_server`. On Lite the archive views could skip no parquet row group. The answers were wrong too: "latest ever" kept a dropped database's files in `DATABASE_TOTAL_SIZE_MB` and in the autogrowth drill-down until retention aged them out. The seven reads now bind a lower bound one day before the window's end, in Darling, Lite and the Dashboard, so a series with no sample in that day yields no fact rather than a stale one. `DB_CONFIG` anchors on the newest capture, as its drill-down already did, which drops the dropped databases it was still counting.
- **A store upgrade that fails before its commit point puts the old cluster back as it found it, and only claims a revert that happened** ([#3927]) - In hard-link mode pg_upgrade renames the old cluster's `global/pg_control` once linking starts, and a failure after that point left a store that could not start on either runtime while the log said the pre-upgrade data directory was never modified. The failure and shutdown paths now rename it back before reverting the runtime, and log CRITICAL with the exact rename when they cannot. The runtime revert refuses while a PostgreSQL server is still running on the data directory instead of moving its binaries out from under it, and a revert whose second move fails puts the first one back instead of leaving no runtime at all. A directory swap that could not undo itself is moved back too, so the next start can no longer initialize an empty store over the real one. The failure outcome and the Failed self-alert now say only what was actually put back, and name the manual step before the next restart when something was not.
- **Log text keeps no literal: the store's own log and every PostgreSQL target's log events mask the SQL inside DETAIL and CONTEXT, every retained store-log entry is masked, and the statement reader shows only normalized DML** ([#3920]) - Two further reviews of [#3915] found text still leaking to the viewer and mcp roles. The store-log sweep kept `statement_timeout`, `lock_timeout`, `deadlock` and `unclassified` entries as the server wrote them, so an ERROR's `STATEMENT:` line carried whatever the failed statement did, a DBA's `ALTER ROLE ... PASSWORD` literal included, for 400 days. Both log surfaces masked a deadlock's `Process N:` queries, a crashed process's query and a PL/pgSQL `SQL statement "..."` context as prose, which keeps bare numbers and dollar-quoted strings, so a monitored server's deadlock kept both sessions' values in `get_pg_log_events`. Every retained entry now has its values masked, with statements and the SQL inside DETAIL and CONTEXT going through a single-pass SQL lexer that masks every literal spelling and withholds text it cannot read to its end, a backslash before a quote that reads two ways included. Rows stored before this build are re-masked in bounded hourly slices, and both readers mask on the way out. `get_store_query_stats` shows statement text only for normalized DML that opens with no comment (every other statement keeps its timings and reads as withheld), which closes comment-split role DDL, a DML keyword hidden in a leading comment, connection strings, and a `SELECT ... INTO` on a pre-16 store that tracks utility statements; its advice about `pg_read_all_stats` now says what that grant opens. The managed store's conf reading de-escapes values the way PostgreSQL does (an include directory ending in a backslash was dropped from the preload merge), reports a preload list PostgreSQL rejects as an Error rather than as fine, and bounds its include walk. Covered by the reviewers' leak cases as regression tests, the statement patterns judged by PostgreSQL's own regex engine, and live tests of the reader and of the re-mask pass on a migrated store.
- **The store's hourly self-metrics sweep works again on TimescaleDB 2.29 and later** ([#3918]) - TimescaleDB 2.29.0 rebuilt its chunk catalog and dropped the two name columns the sweep's catch-all census joined on, so on 2.29+ that statement failed every hour with 42703. The store-size series stopped, the store-log census and collector-cost flush on the same tick stopped with it, and `get_store_metrics` silently lost its `largest_unenumerated` list. That was live on the Linux compose image, which runs 2.30.1, and it blocked the bundle bump ([#3908]). The census now reads the public `timescaledb_information.chunks` view, whose columns keep their names across the rewrite, so one statement serves 2.28.1 and 2.29+ stores with no version check. It also excludes each chunk's compressed relation through `compression_settings.compress_relid`, because from 2.29.0 compressed data is no longer a chunk catalog row, and without that the system row would count it a second time (34.8 MB of a 129 MB test store). Verified on 2.28.1, a fresh 2.30.1, a 2.28.1 store upgraded in place, and a store whose extension update never ran: in each, the sweep's rows add up to the database's relation total to the byte.
- **The bundled store moves to TimescaleDB 2.30.1, which closes GHSA-hcfx-29v5-2rcw, and a store's extension now moves before the store opens instead of under a live store** ([#3908]) - Every release since 3.2 shipped TimescaleDB 2.28.1, which that advisory covers, and the update path could not move an existing store off it. The old post-start `ALTER EXTENSION` ran on a store that the web host, MCP host, Viewer seats and the job scheduler were already connected to. Once the extension moved, every session that had loaded the old library failed its next statement, and the update's own pre-step killed running jobs, which then sat in crash backoff for up to an hour. Now, when the runtime ships a newer TimescaleDB, the service starts the store on a private loopback port with background workers off, runs the update as the first statement of its own session, verifies it, stops the store, and only then starts it normally. Nothing else can connect in that window. The runtime carries the 2.28.1 libraries beside 2.30.1, so nothing ever has to be reverted. A store whose update fails still opens on its own version, and a Critical `Store Runtime Upgrade` alert names the reason; the update is retried on the next start. A PostgreSQL 17 store's pg_upgrade restores its extension as it is, and the update follows once the upgrade commits, so a failure there can no longer cost the rollback copy. The store's version is recorded in `darling-timescaledb.version` in the data directory, and the service refuses to swap in a package that has no libraries for it, which is what a rollback past this release would otherwise do. Releases 3.3 through 3.8 keep this runtime if you roll back to one (the service keeps `pg-runtime.stamp` pinned to the package they shipped). Verified on real servers: a 2.28.1 store with compressed hypertables and continuous aggregates moves to 2.30.1 with every row intact, stamped and unstamped. A package missing the update script leaves the store on 2.28.1, running and collecting, with no runtime reverted. A server left on the update's private port by a killed service is stopped at the next start instead of being adopted.
- **A runtime update no longer stops the store from starting when the previous runtime's folder cannot be cleared** ([#3919]) - A runtime update deletes what the last one left in `pg-runtime-prev` before rescuing the current runtime into it, and that delete sat outside the guard that turns a failed rescue into a skipped update. A file held open under the folder, by an antivirus scan or a shell sitting in it, threw out of startup and the store did not start that time. This release moves the bundled runtime to PostgreSQL 18.6 ([#3906]), so every host takes that path on its first start. Clearing or re-creating the folder now fails the way the rescue does: a warning naming the folder and the error, no swap this start, the live runtime left alone, and a retry on the next start.
- **A store still on PostgreSQL 17 no longer fails to start on a host with 40 GB of RAM or more** ([#3909]) - The managed store's memory sizing derived `maintenance_work_mem = 2048MB` on large hosts. PostgreSQL 17 on Windows accepts at most 2097151 kB, one kB less, and refuses to start over it. PostgreSQL 18 accepts the value, which is why current stores never showed it. A store still on 17 got those blocks after a failed upgrade to 18 reverted. From then on it could not start, and a later release could not upgrade it either, because the upgrade's first step starts the old cluster. The cap is now 2047 MB. A store on 17 whose conf is already over the limit gets a corrective line before anything starts it. A command-line override does not help, because 17 still validates the file. An over-limit value set with `ALTER SYSTEM` is reported with the fix, since nothing appended to postgresql.conf can override it.
- **The store's statement statistics, reviewed before release: the ALTER SYSTEM advice no longer bricks the store, and the reader's filter can no longer be switched off** ([#3915]) - #3899's first version ([#3904]) merged before its adversarial review finished, and the review found four serious faults. The v13 warning told an operator to pass the whole preload list to `ALTER SYSTEM` as ONE literal, which `shared_preload_libraries` stores as a single library name, so the store would not start again; the fix is now written one quoted literal per library, and a list stored that way is reported as an Error with a remedy that works while the store is down. The reader functions' body is parsed at call time under the caller's settings, so one `SET standard_conforming_strings = off` from any grantee turned the backslash filter into letters and returned recorded role passwords; both functions now pin the setting, and the pattern has no backslash. The `slow_statement` store-log class kept any role's raw duration lines, bind-parameter DETAIL included, where the viewer and mcp roles read them; it now keeps the statement with comments stripped, every literal masked (the shared log redactor gains dollar-quoted and `E''` strings) and DETAIL dropped, one row per statement however often it ran. And the setup probe failed at plan time for any store owner that is not a superuser; it now reads whether the module is loaded from `pg_settings`, which any role can. The same pass makes `get_store_query_stats` answer `precondition` from the catalog for each setup state (including an extension older than 1.8, which `pg_upgrade` leaves in place, and a dropped one), return query ids as strings, rename `entries_evicted` to `eviction_passes`, and say what its figures leave out; derives the viewer and mcp slow-statement line from a third of their `statement_timeout`; follows include directives in the v13 merge and checks the preload line in force on every start; scopes the scrub to the store's own database; and corrects the role identities and the hourly-cadence claims. Nothing public shipped the first version (nightlies build from main). Covered by unit tests for each fix and live tests on the bundled runtime, including the `standard_conforming_strings` bypass attempt and a non-superuser store owner.
- **The bundled PostgreSQL moves to 18.6, and the self-contained .NET runtime to 10.0.12, closing published CVEs in both** ([#3906]) - A security review of 3.8.0 flagged two things. First, the Darling store runtime was on PostgreSQL 18.4, one release behind 18.6, which fixes 23 CVEs in it. Second, Lite's builds (zip, Setup and Portable) and the Darling Viewer's Setup and Portable builds carried a `Microsoft.DiaSymReader.Native` affected by CVE-2026-69439, CVE-2026-71328 and CVE-2026-69522. Those are heap overflows in PDB parsing, fixed in .NET 10.0.12. That runtime is whatever the SDK packs into a self-contained build. `global.json` pinned SDK 10.0.302 with `latestPatch`, which never leaves the 3xx feature band, and Microsoft stopped servicing that band at 10.0.303 (runtime 10.0.11). Dependabot had moved the `Microsoft.Extensions.*` packages to 10.0.12, but it never touches the SDK. The SDK is now 10.0.401, and Dependabot's `dotnet-sdk` updates keep it current. A pin test makes each of those pull requests move the Darling container's SDK line with it. Existing Darling installs pick up the new PostgreSQL on upgrade: same data directory, no pg_upgrade. A host whose runtime predates stamping is now compared on the full PostgreSQL version rather than the major alone, so a readable 18.4 is no longer adopted as the 18.6 package. The nightly now runs that same-major upgrade on a real store built on the previous release's runtime, stamped and unstamped. A weekly `Runtime currency` workflow fails when a newer PostgreSQL minor for the pinned major is out, or when a TimescaleDB advisory covers the pinned version. An advisory already tracked by an issue is reported as a warning until the change that clears it. TimescaleDB stays at 2.28.1 for now. Testing the bump on real data showed that the store upgrade cannot yet move an existing store's extension to a new version, and would have left every upgraded store's database failing. That fix, and the bump, are [#3908].
- **Each generated store TLS root certificate carries a unique per-generation name** ([#3557]) - Windows caches every root a viewer's TLS stack ever sees into the user's intermediate-CA store, one per certificate rotation; with all rotations sharing one name, that cache eventually breaks certificate chain building outright on long-running viewer machines. Distinct names per rotation mean the pile can never form.
- **The by-CPU tools now actually rank by CPU** ([#3523]) - get_top_queries_by_cpu and get_top_procedures_by_cpu ordered by summed elapsed time in both SKUs, so on a wait-bound server the real CPU consumers could be missing from the page entirely - and attributed_cpu_ratio read as "hidden CPU" when it actually meant "wrong sort key". Every ranking site now orders by worker time, including the over-fetch cut that could drop a CPU-heavy query before the final sort ever saw it. The viewer's Duration grids keep their elapsed ranking, which is what they promise.
- **analyze_server no longer answers "all metrics are within normal ranges" when the analysis window collected nothing** ([#3524]) - The analysis gate passes on lifetime history, so a server whose collection died still reached the all-clear path with an empty window. Both SKUs' analysis services now flag the zero-facts window and analyze_server returns the "unavailable" envelope pointing at get_collection_health; the genuine all-clear (facts collected, zero findings) is unchanged.
- **The Performance Calendar, daily summary, and fleet sweep band deadlocks as a measured per-hour rate, not any-deadlock-is-Critical** ([#3525]) - The shared daily classifier routed the Deadlocks signal through the Overview card's store-backed rate tiers (#3368, V120) with each surface's real window as the denominator, so one deadlock no longer paints a calendar day red, sweep verdicts stop scaling with the cadence knob, sub-hour spans fall to Warning instead of a multiplied rate, and the day tooltip/reasons report the rate beside the count. The still-forming day clamps its window to the elapsed portion, so an active storm bands on its true in-progress rate instead of diluting against hours that have not happened yet.
- **Perfmon rates are honest per-second values in analysis** ([#3527]) - The PERFMON_*_SEC facts, the batch-request anomaly window, and both SKUs' batch-request baselines read the per-collection-interval delta as if per-second (60-300x overstatement); every read now divides by the measured sample_interval_seconds (Darling's baseline derives it from collection-time gaps, since the continuous aggregate stores no interval), and interval-0 rows - where no delta was knowable - are skipped instead of read as rates.
- **Floor the SQL count thresholds, give Store Disk Pressure a GB floor, and count measured metrics in the fleet Healthy label** ([#3528]) - The SQL Server deadlock and blocking count thresholds now floor at 1 on read like their PostgreSQL twins, so a store row hand-edited to 0 can't fire on a quiet server; Store Disk Pressure gains a self_alerts.disk_free_warn_gb floor (default 50, 0 disables) so a large store volume at a low percent stops paging CRITICAL with hundreds of GB of runway; and fleet cards carry measured_metric_count/metric_count so a Healthy label built off one measured metric of six says so ("1 of 6 measured" on the web fleet page).
- **get_memory_trend stops reporting granted memory as a hardcoded zero** ([#3529]) - The MCP payload shipped a literal total_granted_mb = 0.0 on both SKUs, steering agents away from memory grants during grant-pressure investigations. The field became an explicit null with the envelope naming get_memory_grants as the grants series' source, and both tool descriptions stopped promising granted memory.
- **Lite's Query Store time slicer reads physical reads from its own column** ([#3530]) - The reader mapped both read fields to the logical-reads ordinal and never read the SELECT's total_physical_reads column, so the physical aggregate was computed and dropped. Pinned with a test whose fixture rows carry distinct values per I/O column.
- **LCK_M_IS advice carries the same RCSI caveats as its LCK_M_S twin** ([#3531]) - The intent-shared-lock advice handed out the READ_COMMITTED_SNAPSHOT ALTER with no caveats. Both lock twins now name the brief exclusive lock the ALTER takes, the tempdb version-store cost, and the test-on-a-copy warning for NOLOCK-dependent code.
- **Collector schedules refuse cadences that would fabricate quiet** ([#3532]) - Delta-family collectors (wait/latch/spinlock/query/procedure/file I/O/memory-grant/perfmon stats, and the PostgreSQL wait/statement collectors) now cap at 30 minutes in both Lite's and Darling's schedule editors, with the store-side resolver ignoring out-of-policy rows: past the shared 60-minute delta gap policy every cycle re-baselined and recorded zeros forever, so the charts flatlined green precisely because collection stopped measuring.
- **get_pg_plans finds the plan you asked for, not just the plans in the top page** ([#3533]) - the queryid filter ran client-side over a fetched top-duration page: the reader had no query_id predicate, so the tool pulled the top limit x 10 shapes by total time and filtered them in C#, which made any plan ranked below that page unfindable at every window size - and the filtered-empty branch then told the caller capture was working, the plan was never captured, and the statement was "not the query to look at", while the plan sat in the store. The predicate now runs in the store's SQL over every capture in the window, the over-fetch is gone, and the miss text says what was actually searched and what a miss can mean, with get_pg_top_queries and get_pg_plan_capture_readiness cross-references. The web dashboard's read dispatch already passed query_id through, so it gains the server-side search with no wiring change.
- **get_pg_autovacuum_health classifies severity from the same axis it ranks by** ([#3534]) - The reader ranks tables by GREATEST(dead ratio, insert ratio) but severity only read the dead side, so an append-only worst_table ten times past its insert-vacuum threshold reported "ok"; severity now comes from the worse of the two ratios, the insert-side ratio is published as insert_threshold_ratio, a one-sample window reports growth as unknown instead of "flat" (with first_seen_at showing the window), and the page-scoped summary counts carry the sibling limit_reached flag.
- **get_pg_replication_slots headlines the worst-classified slot and never spells unknown WAL growth as stable** ([#3535]) - worst_slot was picked by retained size, so an active 45 GB keeping-pace slot ("ok") outranked an inactive growing orphan ("critical_orphan_filling_disk"); slots now rank by severity with size only breaking ties, growth is null when either endpoint is the collector's -1 sentinel or the window holds one sample, new unknown-growth severities say so explicitly, and raw retained_wal_bytes nulls the sentinel like its _gb sibling.
- **get_pg_io_stats no longer renders track_io_timing=off as an impossibly fast disk** ([#3536]) - track_io_timing is OFF by default in PostgreSQL, so on a stock server the read and write time counters are never populated - and this read divided the zeros out to 0.000 ms latencies while its trend sibling had shipped the honest contract all along. The single-window read now mirrors that contract exactly: the setting is read from the collected pg_server_config bounded by the window's end (inferred from the window's data when never collected, with io_timing_source saying which), io_timing_tracked and timing_note are published, and every time-derived figure is null when untracked rather than zero, while the operation counts, hit ratios and byte figures stand untouched. The new busiest_basis field states which key decided the "busiest" ranking.
- **The PostgreSQL xmin-horizon alert catches rotating holders and stops firing on single observations** ([#3537]) - The persistence gate gains a horizon arm that fires when the horizon sits past the age threshold in a majority of the window's real captures (counted from the collector's own collection_log runs) regardless of who holds it, naming the rotating-holder pattern under a stable dedup subject; a minimum-observations floor stops the first holder after quiet hours from reading 1-of-1 as chronic.
- **QueryStore slicer's physical-reads sort plots the physical series** ([#3547]) - Sorting the Query Store grid by physical reads relabeled the slicer but kept plotting logical reads, and the selected-row overlay had no physical series to draw at all. Bars and overlay both plot the real physical-reads aggregate now.
- **get_memory_trend joins the grants series so total_granted_mb carries real data** ([#3548]) - Completing #3529's honest null: both SKUs join the memory-grant series the viewers already chart into the trend payload, matching each memory point to the nearest grants snapshot within 30 seconds. A snapshot measuring nothing granted is a genuine 0.0, an uncovered point stays null, and the granted_note appears only when there is a gap to explain.
- **Viewer Recommendations tabs stop saying "All clear" when the analysis window collected nothing** ([#3551]) - Both viewers only branched on the insufficient-data message, so a server whose collection died still rendered the all-clear on a zero-finding read. Lite's Generate now consumes the analysis service's window-empty state directly; the Darling service persists it into the analysis-state marker (false-with-a-message, no schema change) so the viewer's read and Generate now doors both render a "nothing was measured - check Collection Health" notice. The genuine all-clear (facts measured, zero findings) is unchanged.
- **Viewer pass for the Store Disk Pressure GB floor and fleet measured-metric qualifier** ([#3563]) - The Settings window can now edit the Store Disk Pressure warning's GB floor (V126, 0 disables) next to its percent sibling, and the WPF fleet card's band label carries the web fleet page's "N of M measured" qualifier when a band folded over unmeasured metrics, so an Unknown-heavy server no longer reads as an unqualified green.
- **Deprecated Dashboard analysis reads perfmon counters as true per-second rates** ([#3561]) - The Dashboard's analysis facts, batch-request anomaly window, and batch-request baseline read the per-collection-interval perfmon delta as if it were per-second, overstating by 60-300x at common cadences; all three now divide by the row's measured sample interval and skip rows with no knowable delta, matching the Lite/Darling fix in #3560.
- **Sorting the Procedures or Query Store grid by physical reads plots the physical series, in both apps** ([#3556]) - In Lite and the Darling viewer, the Procedures grid's physical-reads sort plotted the logical aggregate under a physical label, and the Query Store slicer labeled execution-weighted slice totals "Avg". The Darling viewer's Query Store grid also still carried the original #3547 physical/logical swap, and none of its grids re-projected a selected row's overlay when the sort metric changed, so the bars and the overlay could show different metrics. Both apps now plot the series the sorted column names, label slice totals as totals, and keep the overlay on the bars' metric - pinned by distinct-per-column reader tests and source-text pins over the sorting handlers in both apps.
- **The Locking & Contention grid names the table as schema.table** ([#3576]) - On Azure SQL DB and anywhere per-tenant schemas are the pattern, the same table name lives in several schemas, so the grid's bare table name made `dbo.Orders` and `archive.Orders` read as one object. The Table column now shows the qualified name, and sorting, filtering, and CSV export all treat it as one string. Mirrored in the Darling viewer and the deprecated Dashboard, and the Darling viewer's PostgreSQL Index Usage grid, which had the same gap, names its tables the same way.
- **Extreme, corroborated anomaly findings can now cross the notify floor, so the baseline engine is no longer notification-inert at shipped settings** ([#3526]) - every `ANOMALY_*` ramp saturates its base at 1.0, the Layer-3 tuning-class cap held the final at 1.49, no amplifier arm existed, and the shipped `notify_severity` is 1.5 - so a 20σ session spike beside a 15σ batch-request spike at 3am could never page unless THREADPOOL/SOS/RESOURCE_SEMAPHORE happened to co-fire; the product's best per-server-calibrated statistics were structurally its quietest. The shared `FactScorer` gains a second, per-fact escape for `ANOMALY_*` only: a deviation at 3x the cutoff the fact fired at (10.5σ on the 3.5σ robust path, 15σ heavy-tail, 6σ classical, 3x the absolute bar on the never-blind fallback path, bounded at the 25σ display cap; never for `is_new` profiles or the ratio/count families) leaves the cap on its own evidence, and a new anomaly co-fire amplifier arm (sibling anomaly families and measured absolute facts at the bars their own arms use) carries an extreme anomaly with two corroborators to 1.6. Routine 2-4σ saturation still caps at 1.49, a lone extreme anomaly scores its 1.0 base, one corroborator reaches 1.3, and CXPACKET's cap and the impact-peer escape are untouched. Lite + Darling (+ the deprecated Dashboard via the shared scorer).
- **`get_store_metrics`' `job_history` block now says whose eyes its rows are visible to, and proves rows exist where it can** ([#3574]) - `recording: true` was a GUC echo: `timescaledb_information.job_history` shows a row only to members of the job's owner role or the database owner, while `jobs` and `job_stats` show every role everything, so a least-privilege role could list all 110 jobs and read an empty history on a store recording perfectly - which cost a real postmortem. The block now evaluates the view's own predicate for its reader (`reader_role`, `visibility`), counts the rows it actually sees over a fixed 24-hour window beside the jobs `job_stats` says ran, and names the contradiction - recording on, reader admitted, jobs ran, zero rows - as a finding. In managed mode the MCP host reads as the `mcp` role the view filters out, and the block says so rather than reporting a zero it produced by construction. Darling only.
- **The forced-plan-failures alert read no longer walks the whole fleet's two-hour slice of Query Store rows to find one server's** ([#3573]) - the read's own composite index was in the store all along, and the planner priced it out because server ids interleave physically across collection passes, so every 30-second pass re-read the entire fleet window per server and cold excursions crossed the 10-second deadline as the table grew from 6 GB to 23 GB. A covering index over exactly the read's columns makes it an Index Only Scan over one server's rows (50 buffers against 1,514 on the rig, 11x fewer pages measured live), applied at startup like the composer's indexes. The deadline does not move.
- **The viewer's tray Snooze now silences the toast itself** ([#3570]) - Snoozing or silencing an alert from the Darling viewer wrote the mute rule and then waited for the background service to reload its own cache and flag the next alert muted before the tray went quiet; when that reload was slow or failed, the rule sat in Manage Mute Rules while the toasts kept coming. The viewer now applies its own mute rules to the tray on the very next poll, the same way Lite always has in-process, and the status bar says what stops now (the tray) and what the service applies within 15 s (email/webhook). A snooze that could not be saved is now reported in the status bar instead of only logged.
- **The compression-stuck self-alert no longer pages on a healthy job's run instant** ([#3575]) - TimescaleDB's job_stats view reads job_status from pg_stat_activity and next_start from the job-stat row, so for a few milliseconds at either edge of every run it reports the dead-job shape (next_start = -infinity, not Running); a production store paged on exactly that, 53 ms into a 63 ms run that succeeded. The check now re-reads five seconds after a -infinity trip and reports the job only if it persists, and its hourly sample is pinned to :30 past the minute, off the :MM:00 instants the compression policies fire on. Re-arm and escalation semantics and the six-hour hung-run bound are unchanged.
- **Text on an accent fill gets a measured ink in every theme, and the FinOps row marks become theme brushes Dark can actually see** ([#3577]) - The selected tab on Cool Breeze read 2.71:1 because the TabItem's selected-state Foreground never reached the header text (the app-level implicit TextBlock style out-ranked it); the header presenter now shadows that style and a per-theme `AccentForegroundBrush` is the ink on every accent fill (5.39:1 on Cool Breeze, 7.52:1 on Dark, unchanged on Light), applied to tabs, highlighted combo items, accent buttons, the calendar and Dark's grid selection. The "Mark Done / To Do / Do Not Do" row tints move from three fixed 20% overlays (1.2-1.4:1 against Dark's rows) into theme brushes, with Dark's built from its own status colors at 2.9-3.1:1 while row text holds AA.
- **The duration-trend trio routes by retention tier and says what it served, so a 7-day request no longer returns 4 days labelled quiet** ([#3541] A2) - get_query_duration_trend and get_procedure_duration_trend read raw query_stats / procedure_stats only, whose rows a TimescaleDB store drops at four days, while hours_back accepts 168 — a 7-day request came back as 4 days under a label saying 7, and when nothing survived the empty branch called the window "genuinely quiet — widen hours_back", which is false (dropped, not absent) and harmful (widening cannot help). Both now route through the ladder get_query_trend already uses (age → availability → materialized coverage) onto the hourly rollups, dividing by the bucket width instead of a LAG, and every sibling — the Query Store one included — publishes `source`, `effective_start`, `effective_hours_back`, `truncated`, `bucket` and `aggregate_note` on the data path and the empty one, with the empty branch naming an unserved head and `--backfill-rollups` where widening cannot help. get_query_trend gains the availability and coverage rungs (it answered 42P01 on plain PostgreSQL past four days). Lite's twins, get_query_trend included, publish the same block with Lite's truth (raw, per-collection); `value` gains its named twin `elapsed_ms_per_second` on both SKUs.
- **Analysis facts divide by the time the collector actually observed, and a window with a hole in it says so** ([#3538] A2) - Every wait fraction, blocking rate and deadlock rate divided by the nominal window, so collector downtime deflated severity in proportion to the data lost: a CXPACKET storm at a true 25% read as 6% with the collector down three of four hours, and nothing consulted collection health. Both SKUs now stamp observed coverage from the wait_stats collection series (real LAG intervals, an interval past the delta gap policy credited as zero, first row clipped to the window) and divide by it; an unobserved window emits no rate fact and composes with the #3524 unavailable envelope even when point-in-time config facts exist. Below 70% coverage a COLLECTION_GAP context fact carries the fraction and largest gap, and analyze_server, get_analysis_facts and compare_analysis carry a coverage block and a partial-coverage caveat.
- **SQL Server's Poison Wait alert measures accumulated starvation over a ten-minute window like its PostgreSQL twin, so one slow wait no longer pages and a THREADPOOL storm no longer sleeps** ([#3539] A4) - the alert judged ONE collector delta's average ms per wait against a 500 ms bar, presence-flat CRITICAL: a single 600 ms wait paged, and ten thousand 50 ms THREADPOOL waits - 500 seconds of worker starvation a minute - never moved the average. The PostgreSQL edition had fired on TOTAL accumulated wait over a rolling ten-minute window since [#2711], graded WARNING at one backend continuously stuck (600 s) and CRITICAL at ten, under the same alert name, switch and mute key - so one mute rule meant two things. Both SKUs' reads now SUM every wait_stats delta per poison type over the window (no completed-task filter, no row cap, no threshold in the read), a shared PoisonWaitEvaluator grades the sum against constants the PostgreSQL evaluator now aliases rather than duplicates, and the message shape mirrors PostgreSQL's clause for clause. Measured first on one production store class (43 SQL Server primaries, four days, 24,757 server-ten-minute buckets): the worst bucket anywhere held 5,795 ms of THREADPOOL - 0.0097 average waiters, ~100x under the WARNING bar, the same order as the PostgreSQL research's 160x - while the old trigger's near-misses showed both wrong shapes (single rows at 247 ms over 2 tasks; a 703-task row averaging 8 ms). The clear arm now needs an observed window: a collector that stops delivering holds the alert open instead of announcing "Cleared" on silence. The `poison wait threshold (ms)` setting stays persisted and reported but is no longer consulted; both Settings windows say so and disable the box.
- **Six MCP pages now say what bounded them, so a limited page stops passing for a complete answer** ([#3541]) - `get_blocking`/`get_blocked_process_reports`, `get_deadlocks`/`get_deadlock_detail`/`get_blocked_process_xml`, `get_alert_history`, `get_long_query_completions`, `get_plan_corrections`, `get_waiting_tasks` and `get_wait_stats` carried a hidden reader cap (`LIMIT 200`/`50`/`500`, or none) under a tool that advertised `limit`, took `limit` off the top, and published the capped count as `total_*` - so an agent read a 200-row page of a 5,000-event window as the window, and "widen hours_back" could never help because the cap was on rows. One pattern on both SKUs, the one [#3287] established: the cap binds to the caller's `limit` as a SQL parameter, the read fetches `limit + 1` and reports `truncated` only when the extra row existed, the page publishes `oldest_returned_*`/`newest_returned_*` and its `order`, and every page count is `*_returned`. `get_alert_history` now states and measures its `dismissed = FALSE` filter (`dismissed_excluded`, `dismissed_excluded_count`) and takes `include_dismissed`; the dedup_key fingerprint scan on `get_blocking`/`get_deadlocks` honours [#2159]'s whole-window promise up to a stated 2,000-row ceiling (`rows_examined`, `scan_truncated`); Lite's `get_long_query_completions` now keeps the window's SLOWEST like Darling instead of the newest 200 re-ranked, which could omit the slowest run entirely.
- **The four naked delta families store the interval their deltas accrued over, so a restart's fabricated zero reads as unknowable instead of 0.00 ms/sec** ([#3540]) - wait_stats, file_io_stats, latch_stats and spinlock_stats discarded the calculator's measured interval at the write, so its (0, 0) "no delta knowable" marker could not survive and every per-second reader LAG-divided a restart's zero into a confident 0.00 (file-I/O latency rendered "0.00 ms" mid-restart; the wait-rate window counted the restart as a sample). Darling V127 / Lite v60 add sample_interval_seconds to all four (perfmon_stats and query_stats already had it); the collectors write the minimum over each row's delta groups; every reader treats a stored 0 as NULL (the point is absent, MCP fields are null), a NULL as a pre-upgrade row that keeps its LAG-derived interval, and a measured value as the divisor. The wait_stats_baseline continuous aggregate still sums the restart zero - the documented follow-up is a new aggregate, since a rebuild would forfeit 35 days of baseline history.
- **Blocking and CPU health bands are rates over the window they were measured in, tiered from 14 days of fleet blocking and CPU data** ([#3539] A2/A3/A8d) - `BlockingSeverity` banded a raw count over a caller-chosen 1–168 h window (`count >= 5 → Critical`), so five blocked-process reports were Critical at a week's read and Healthy at an hour's; the daily classifier reddened a cell on six hot CPU samples or eleven blocking events whatever the span (a 15-minute sweep or a 24-hour day — 5.9% and 5.1% of measured server-days), and on a single collection ERROR row. Blocking now bands reports per hour (Warning 5/hr, Critical 20/hr — the tiers sit at the top of the measured quiet mode and the foot of the storm mode, with the 60 s block a rate-independent Critical), the calendar/sweep delegate to that band over their own window, the high-CPU bar scales with the window (an excursion-scale minimum of 6 below ~5 h, a sustained-heat 30 per day above), collection errors band as a share of the window's runs against the collector-health classifier's own 20% bar at its Warning tier, and the collector dot grades one failing collector of forty apart from forty of forty. Cards, calendar tooltips, sweep evidence and the MCP day tools disclose the rate or share they banded on.
- **get_store_metrics stops answering for 38% of the store: continuous aggregates and the named plain tables join the inventory, every sweep is reconciled against pg_database_size, and the block says its own coverage** ([#3582]) - On the largest production store the inventory accounted for 159 GiB of 415 GiB, because timescaledb_information.hypertables never lists a materialization and the walk never named collect.query_store_text and its siblings. The hourly sweep now writes one continuous_aggregate row per aggregate (sized through its materialization, under its view name), three table rows, and two catch-all rows (other / system), and the tool reconciles one sweep's rows against that sweep's pg_database_size, states enumerated and attributed coverage with the residual and a reconciled verdict, and names the largest un-enumerated relations live. Each aggregate object carries compression_enabled and its refresh/compression/retention policy job ids read from the catalog. The same sweep persists the owner's own job_history count so the managed-mode block (#3574) is a measurement rather than a GUC echo, and the GUC-off note no longer claims an empty view — failed runs are written regardless.
- **The twenty continuous aggregates join the compression ladder** ([#3581]) - Every raw hypertable compressed since the archival tier existed; no continuous aggregate ever did, and on the largest production store the twenty materializations were 235 GiB of a 415 GiB database — an uncompressed rollup of a 9x-compressed source is larger than the source. An idempotent startup ensure now enables columnar compression on each materialization (segmentby server_id, orderby bucket) and attaches a once-a-day policy whose compress_after is derived from the tier's refresh window plus one raw chunk (2 days for hourly-refreshed aggregates, 4 for the dailies), so a refresh never decompresses what a policy just compressed. The jobs run off the hourly phase grid, one aggregate per hour at :35Z, and on an existing store the first runs are staged one aggregate per night, largest first, in the schedule itself; a fresh store gets all twenty at once.
- **PostgreSQL MCP percents name their denominator, so a three-row page stops summing to 100% of everything** ([#3541] A7) - `get_pg_top_queries`, `get_pg_wait_stats`, `get_pg_wait_sampling`, `get_pg_kernel_stats` and `get_pg_io_stats` divided every row by the sum of the rows they had fetched and published it as `pct_of_total_*` beside a `total_*` that was the same page sum, so at `limit = 3` the three shares summed to 100% by construction and read as "these three are everything"; `get_pg_top_queries` compounded it with a literal `LIMIT 50` under `Take(limit)`. Each reader now computes the window's total on the SAME statement as its rows - a `SUM(...) OVER ()` over the grouped result, evaluated before `LIMIT`, so it cannot drift from them and costs no second read - and each tool fetches `limit + 1`, observes `truncated`, publishes the window figure as `total_*`, the page's own sum as `returned_*`, the ratio once as `returned_pct_of_total`, and says in its description which denominator its shares use. Executing the statements against live PostgreSQL 18 also found that `get_pg_kernel_stats` had never been ranked by CPU: its `ORDER BY 3 + 4 DESC` is an expression PostgreSQL folds to a constant and drops, so every page was the alphabetically-first series; it now sorts by the named differenced columns, and the Viewer's kernel grid inherits the fix. A cross-SKU census over every paged tool body pins the rule, with `get_pg_database_stats`' disclosed page-summed totals as its one stated allowance.
- **Every delta family is seeded from the store at service start, and the series-age rescue finally has passes to read, so a restart no longer fabricates one interval of quiet for six families** ([#3540] A4) - both hosts' restart seeders covered four of the ten delta families, so `latch_stats`, `spinlock_stats`, `procedure_stats`, `query_stats` and the PostgreSQL wait/statement pair re-baselined on every restart and deploy and stored one interval of fabricated quiet each time - and the per-group pass window that the #2235 recompiled-plan credit reads was never seeded at all, so that credit was inert on exactly the post-restart pass it exists for. Every family a host monitors now restores its keys (latest row per key inside the 15-minute window via `DISTINCT ON` for the collectors that do not write every key every pass; the bounded latest-collection probe for the rest) and its pass window from the same read, under a per-family guard so one slow or missing table cannot cost the others their continuity; `query_stats` restores its pass window only, because its delta key carries statement offsets the store does not persist (a rung, tracked on #3540). `ClearServer` is wired into Lite's server-removal cleanup and Darling's reconcile-remove branch, so a remove-and-re-add inside the gap policy's hour starts from a first pass instead of subtracting another server's counters. A census in Lite.Tests reads both hosts' seeders against the calculator's family list and each collector's own delta-group names, so an eleventh family cannot ship unseeded.
- **MCP write tools report what happened** ([#3541] A14) - `add_servers` counted a `collides` refusal in no counter, so a collided server went unmonitored under `failed: 0`; every status now maps to exactly one of `added / skipped / collided / failed`, which sum to a new `requested`. `remove_server` no longer deletes the first partial match of an ambiguous name - it refuses with the candidates listed and honors a partial only when unique. `update_custom_view` and `update_custom_alert_rule` share one vocabulary (omitted = unchanged, empty string = cleared; the view tool used to clear on omission). `mute_analysis_finding` on both SKUs reports `registered` and `matched_now`, says `muted_unmatched` for a hash no stored finding carries, and surfaces a swallowed store failure instead of `muted`. The instruction table stops denying the Entra modes #3484 accepted, and `/api/read/get_alert_history` gains the `include_dismissed` parameter.
- **The scorer's wait thresholds carry their measurement: WRITELOG stops saturating on a third of routine windows, deadlocks grade 5/hr apart from 90/hr, and the absolute gates scale with the hours actually observed** ([#3538] A5/A7) - Every `GetWaitThresholds` entry now names its lineage against the 43-server dogfood read (1,075 server-4h-windows) or says "unmeasured", and a census pins that. WRITELOG's inherited (0.10, null) saturated base 1.0 on 339 of 1,075 windows (31.5%) and is re-derived to (0.25, 0.50); DEADLOCKS adopts the alerting layer's measured (5, 20) instead of saturating at the WARNING entry; the 0.01-saturating waits ramp to (0.01, 0.10); HADR_SYNC_COMMIT - the fleet's second-largest wait, previously invisible to the engine - gains an entry, evidence-gated advice that names synchronous commit as a durability policy (never "switch to async"), and graph edges to and from WRITELOG; PREEMPTIVE_OS_QUERYREGISTRY is an explicit no-threshold entry with its fleet-uniform measurement. THREADPOOL (>= 15 min per observed hour + 1 s avg) and PAGELATCH_UP (> 10 s per observed hour, the ported source's own hourly bar) now divide by observed time, so hours_back no longer changes the verdict on an unchanged server. The five CALIBRATE ON SQL2025/HAMMERDB markers are replaced with what the fleet pass did and did not measure.
- **Analysis pages with long compound stories now reach Slack instead of dying with invalid_attachments** ([#3612]) - The Slack builder rendered every structured detail without ever counting blocks, and a five-fact plan-regression story — the highest-severity compound finding the product makes — carries enough drill-down and incident items to cross Slack's 50-block message limit; three such pages were lost across two builds. Details are now budgeted the way #3493 budgeted the prose: head and footer costed first, the longest leading run of details kept, the rest dropped with one omission item naming what was dropped and where the full page lives. Bodies split under their heading through the same splitter and fields cap at Slack's 2,000-character field limit as hygiene; every page that fit before renders byte-identically.
- **Continuous-aggregate materializations are chunked at one raw chunk, like every raw table** ([#3620]) - TimescaleDB sizes a rollup's chunks at ten times the raw table's interval, so every aggregate carried 10-day chunks: the 7-day interval tier held up to 17 days and compression could not reach the newest ten days of any rollup. The startup ensure that compresses the aggregates now holds each materialization at the raw chunk width, derived from the one constant so the tiers cannot drift apart. Existing wide chunks age out on their own schedule, so short rollups tighten to their designed hold over about two weeks rather than overnight.
- **The PostgreSQL first-target runbook stops denying three capabilities that ship** ([#3608]) - §12 and step 9 said plan capture, blocking chains and configurable alert thresholds did not exist; all three do. Step 9 now lays out all eight PostgreSQL alert families in two groups — the three Tier 0 predictors, which carry no knob by decision, and the five shared-name alerts, two of which have their own PostgreSQL count thresholds (`pgDeadlockCountThreshold`, `pgBlockingCountThreshold`, #3444) — and §12 points each struck denial at the step that carries it. Same-vintage drift in the README's "not yet" sentence, the blocking design note's "not started" header, and the runbook's schema version (v114 → v127) corrected in the same pass.
- **Slack text cuts land on whole characters, not UTF-16 indexes** ([#3622]) - The six places the Slack builder cuts text to a limit — the prose hard split and omission fragment, the detail heading, the field label and value, and the omitted-headings list — cut by UTF-16 index, so an emoji or accented letter astride a limit reached the reader as a replacement glyph the value never held, the hard split showed two of them and no emoji, and a cut field's "N more characters" counted code units. Every cut now lands on a whole character through one helper, falling back to a code-point boundary only when a single character is wider than the whole limit, and the field's omitted count is in characters the reader can count.
- **A not-yet-valid web TLS certificate now raises the self-alert instead of reading as healthy** ([#3517]) - a `web.network.tls` certificate whose `NotBefore` was still ahead at service start (a skewed clock, or one issued for a future rotation) is refused and the LAN dashboard binds loopback-only, but the #3514 self-alert derived everything from `NotAfter` and saw a far-out expiry. The web host now carries its refusal verdict to the sweep, which fires the same `Web TLS Certificate Expiring` family at Critical — same key and mute scope — naming the validity window, the loopback-only consequence, and the restart the host needs to re-decide.
- **Light and Cool Breeze status colors read against their own page, and text on a status fill gets a theme ink** ([#3609]) - Light's Warning and Info and Cool Breeze's Warning were under the 3:1 marker floor against their page (2.47, 2.32 and 2.09:1) — the new Settings contrast readout painted the stock palette red. Re-derived to clear 4.5:1 on the page and the card, and a new StatusForegroundBrush carries the text on the Recommendations severity badge and the viewer's alert badges, which had hard-coded inks (the viewer's White was 1.41:1 on Dark's amber). Dark is unchanged.
- **Forced Plan Failing fires once per observation, not once per cooldown** ([#3579]) - The adapter now returns the collection that produced the rise and the engine remembers which one it already reported, so a plan whose force failed once produces one card instead of one per cooldown until the next collection lands (six cards for one event on a production store). A plan failing across successive collections still re-fires per collection; recovery is unchanged.
- **The compression dead-job alert now says what a -infinity row is on the store's TimescaleDB** ([#3591]) - Below TimescaleDB 2.26.4 the sentence "the scheduler will never run it again" stands and the job is re-armed once as before. From 2.26.4 on (upstream #9360) a persistent -infinity is a crashed run in the scheduler's own crash backoff; the alert says so, and the service no longer re-arms it — measured on 2.28.1, alter_job against a job in crash backoff resets the backoff rather than shortening it and would have posted a false Recovered. Such a row is logged on first sight and pages Critical only if the scheduler's retry has not cleared it an hour later.
- **Every delta family now stores the interval its deltas accrued over, and query_stats stores the statement offsets its delta key is made of** ([#3540], Darling V128 / Lite v61) - V127 dressed four of the naked delta families; this dresses the last four (`procedure_stats`, `memory_grant_stats`, `pg_wait_stats`, `pg_statement_stats`), so the calculator's (0, 0) "no delta knowable" marker reaches the store from all ten and the procedure duration trend, the per-statement PostgreSQL trend and `get_resource_semaphore` stop reading a restart as 0.00 ms/sec, 0.00 calls/sec or a quiet semaphore. `query_stats` gains `statement_start_offset` / `statement_end_offset` (byte offsets into the batch text, `-1` = end of batch, stored verbatim), so both hosts' restart seeds rebuild that family's delta key from the store instead of restoring only its pass window; pre-upgrade rows (NULL offsets) still feed the pass window and seed no fabricated key. The census's still-naked list and the seeding census's pass-window-only set are both empty and asserted so.
- **The File Growth threshold means one thing — megabytes per hour — at every surface that shows it** ([#3539] A8c) - The rise knob was compared against the raw growth inside the lookback window while the lookback was a second knob clamped 5–1440 minutes, so the same 10 GB bar meant 10 GB per five minutes on one store and per day on another. The knob is now a rate averaged over the lookback (`rate × lookback / 60`, byte-identical on the shipped 60-minute default); both Settings rows, both preview lines, the alert's threshold line (rate, window, and the bar in MB), the card and both MCP descriptions say "MB/hr averaged over the lookback" in one shared phrase a nine-surface census holds. Key, column and stored value unchanged.
- **Story confidence measures corroboration instead of path length, and the nightly rebuild's three cards become one incident that names the job** ([#3538] A6/A9) - `confidence` was (n-1)/n over the story path with a lone symptom at 1.0, so the least-evidenced finding ranked highest under an evidence name. It is now 0.20 for the fired symptom plus up to 0.48 for the share of the root fact's amplifier checks that matched and up to 0.32 for chain depth; both SKUs publish `confidence_basis`, define the term, label pre-change rows `path-shape (pre-#3538)` by their provably-disjoint value shape (no schema change), and name an anomaly fact's baseline trustworthiness `baseline_confidence`. Three symptom → RUNNING_JOBS graph edges (SCH_M, IO_WRITE_LATENCY_MS, WRITELOG), gated on the job having fired, fold a maintenance window into one incident whose advice names the long-running job's overrun.
- **The daily digest and sweep rollup no longer re-announce on a service restart** ([#3580]) - Both once-a-day gates lived in process memory, so the first tick after any start delivered both documents again; on one install night three restarts produced six re-posts among ~23 channel posts. The gate now stamps delivered-today in the store's existing state table (no migration) and reads it back after a start, while a delivery the channel reported failed writes nothing, so the next tick still retries it.
- **compare_analysis bands each delta by the server's own dispersion and folds one cause into one row, so same-hour-yesterday noise stops reading as a verdict** ([#3538] A3) - The tool compared one window against one and banded every key by its severity delta on a flat ±0.1 dead-band, so a saturated ladder's doubling read "stable", a trace wait's slope read "worse", one I/O stall was six worse keys, and plan-cache hash churn inflated new/resolved counts. Both SKUs now share one `ComparisonBanding`: keys with a same-unit per-server baseline (CPU %, read latency, connections) are banded in that hour's robust sigma (`delta_sigma`, `band_source: "baseline"`); every other key changes status only when the value moved ≥ 25% of the larger side AND sits ≥ 0.25 up its own severity ladder; `BAD_ACTOR_*` appearances are `plan_cache_churn`; rows carry a physical-cause `family` with one family row per cause; the rules are stated in `band_rules`, every verdict row flags `coverage_caveat` when a side was partly observed, and the description says what "worse" cannot mean at N=1 vs N=1.
- **A server nothing has banded yet is Unknown, not Healthy, and the alert-history grids show the severity the alert fired at instead of the colour its name implies** ([#3539]) - A collector dot with zero banded collectors read Healthy, and an online server on which not one metric was measured folded to Healthy and counted in the fleet's healthy mass with "0 of 6 measured" as its only tell; both now read Unknown, and the nothing-measured card bands Warning like a server awaiting its first collection, with the reason "no metric measured yet" on every surface. Both Alert History grids classified a row by its metric NAME, so a Warning-graded Poison Wait rendered red and a Critical-graded low-disk fire amber; the alert's fired severity is now persisted in the row's context JSON on both SKUs (no schema change) and every grid, both SKUs' get_alert_history (severity + severity_source) and the web alerts page render it, falling back to the name only for rows that predate it.
- **Every latest-snapshot MCP read says when it was captured, and no tool accepts a window it does not read** ([#3541] A10) - Latest-snapshot reads (memory stats/clerks, file I/O, perfmon, the on-connect configuration family, plan cache, scheduler) answered with the newest row a store held and nothing saying when; two memory-grant tools took hours_back and read only the newest row in it, so a grant storm three hours ago was invisible. Every such read now publishes captured_at (anchored ones age_seconds against as_of); get_resource_semaphore/get_memory_grants read the window (peak waiters and when, summed timeout/forced deltas) beside the stamped snapshot; get_cpu_scheduler_pressure has one (server_name, hours_back, as_of) contract on both SKUs and Lite publishes its verdict; get_server_summary names its three clocks; the latch band names the interval it came from. Census-pinned on both SKUs.
- **PostgreSQL servers get a measured deadlock band from their own counters, their Long-Running Query alert skips maintenance like SQL Server's does, and every one-sided alert says why it is one-sided** ([#3539]) - The fleet card's deadlock dot read Unknown on every PostgreSQL target while the store held the server's own `pg_stat_database.deadlocks` counter. Both fleet surfaces now difference that counter per database (reset-clamped, summed) and band it through the same per-hour tiers as SQL Server's graph count, measured only where a difference was actually taken; the coverage block counts those servers as read and names the instrument. The PostgreSQL Long-Running Query read gains the noise opt-outs SQL Server's has had (autovacuum/walsender, VACUUM/ANALYZE/REINDEX/CLUSTER, pg_dump-family on the shared backups switch, excludedDatabases), and the Darling README gains an engine-coverage table stating why each remaining one-sided alert and band is one-sided.
- **Database File Growth's rise arm fires once per hourly observation, not once per cooldown** ([#3636]) - The rise gate reads a stored delta between two collections of a collector that runs hourly, and re-fired on every 5-minute cooldown against the same two rows — up to twelve cards per growth event. Both SKUs' reads now return the newest sample's collection_time as the observation's identity, and the engine remembers per (server, database, file) which observation the rise arm last fired on and declines to re-fire it regardless of cooldown; a file still growing across successive hourly collections re-fires per collection, and the level gate (file ≥ X% of volume) re-fires on the cooldown exactly as before. #3579's mechanism, one condition over.
- **The daily summary stops painting purged months green, and every MCP filter is part of the query** ([#3541] A9/A13) - The daily aggregate's day spine outlived the signals it banded: the seven per-signal tables purge at 30 days, the collection log at 60 and the alert log at 90, so for every day in between the spine still had the day while deadlocks, blocking, CPU and memory had all been dropped and each `COALESCE`d to a MEASURED zero - which bands Healthy - and a 366-day `get_daily_summary_range` painted the purged months green. Both SKUs now decide each day from one shared `DailySummaryRetention.StateFor`: the reader's effective retention horizon (Darling: the shortest fleet-effective signal retention, so an operator-shortened retention moves it; Lite: its archive horizon; wall-clock deliberately, because a backdated `as_of` cannot un-purge a table), a `signal_sources_present` count so "the tables no longer hold it" is never said of a held row, and #3596's `collection_runs`. Days read `collected` / `purged` (NoData, never Healthy) / `past_horizon` (rows survive, verdict withheld) / `no_run_record` (disclosure only), the range publishes `retention_horizon` and the counts, and `summary_date` is ISO-only. On the filter side `parallel_only`/`min_dop` are a HAVING floor before the CPU ordering and cap instead of a `.Where` over an already-cut page, `get_active_queries` pushes `database_name`/`blocking_only` into SQL, counts the filtered population with `COUNT(*) OVER ()`, joins the paged dialect and never strips a head blocker a row in the same capture names (`blocker_not_shown` says `not_captured` / `filtered` / `past_page` otherwise), three uncapped reads refuse a non-positive `hours_back` instead of `Math.Abs`-ing it, and `get_analysis_facts` validates `source` against `FactScorer.KnownSources` (fifteen, pinned to every literal) and refuses with the whole set.
- **Zero is a measurement: health parsers say whether their source was ever seen, a regression with no baseline stays null, and the first point of a differenced trend is no longer a fabricated 0** ([#3541] A12) - Eight of the nine `get_health_parser_*` tools answered a dead system_health session with the same `empty` a healthy hour earns; every one now carries `source_observed` / `last_captured_at` and a nothing-ever-captured store answers `unavailable`. `get_query_store_regressions` keeps a NULL percent null (a 0 baseline has no ratio) with the reason under `undefined_percents`; the duration trends publish the window's first collection with null rates and `unrated_points` instead of 0.0; `get_pg_xmin_horizon` divides by every capture in the window (the alert evaluator's own denominator); `get_pvs_stats` reports a measured 0 MB as 0.00% with `pvs_measured`; `get_table_index_sizes` derives growth only from baselines the store holds, with `history_days_available` and `growth_over_available_history_*`.
- **The interval-hourly Query Store refresh stops paying twelve index inserts per re-materialized row for eleven indexes nothing reads, and the capture-down alert read stops decompressing a server's whole collection log to learn two statuses** ([#3597]) - Measured on a TimescaleDB rig: a refresh recomputes exactly the hour-buckets any write dirtied, so a single backdated row re-materializes the whole fleet's hour, and each of those rows carried eleven per-column group indexes on the dedup materialization that no reader uses. The aggregate is now created without them and existing stores drop them at startup under a lock timeout that yields to a refresh in flight. The capture-down self-alert read, the heaviest statement on the alert pass, now asks each collector for its newest row through the partition column and stops at the newest chunk.
- **Top CPU Queries reads as one record per query on Slack, not seven fields interleaved across a two-column grid** ([#3644]) - A drill-down's rows now travel as records beside their flat fields, and Slack renders each as a bold summary line over its SQL in a code block, packed into as few sections as fit. The five-fact analysis page's fixed items cost 20 blocks instead of 33, so thirteen incidents fit where six did and the three pages #3612 could only deliver by dropping incidents now deliver whole. Email and Teams render the same compact list; every payload that carries no record array is byte-identical.
- **Top Cpu Queries' Max Dop is the newest plan's reading, with the cross-plan maximum kept as a dated history** ([#3648]) - The High CPU card's drill-down (and the bad-actor detail) reported MAX(max_dop) grouped by query_hash, which folded every cached plan's lifetime high-water mark into one provenance-free number: a MAXDOP-1 instance whose stored plan was serial read 16 and drove a tuning recommendation that had to be retracted. max_dop is now the newest plan's DOP; max_dop_any_plan, max_dop_any_plan_last_seen, plan_count and a dop_note sentence carry the history beside it, and an unknown reading is null rather than 0. Both SKUs, byte-identical SQL.
- **The force-plan remediation said "eligible, no blockers" for plans automatic plan correction already owned, had failed to force, or had withdrawn** ([#3652]) - The verdict now reads the store's forcing and APC state at read time — `query_store_stats` (is_forced_plan, plan_forcing_type, force_failure_count) and `plan_correction` (recommendation state/reason, last-good plan, FORCE_LAST_GOOD_PLAN enablement) — and adds five named blockers with evidence and snapshot time: `apc_owns_it`, `already_forced`, `forcing_failed_on_this_plan`, `apc_withdrew_it`, `apc_resolved_differently`. When APC is on for the database the target carries `apc_mode: on` and a guidance sentence ("the engine is Verifying via plan N; intervene only if it reverts or expires"), and whenever the state could not be read the target says so in `state_note` rather than reading as eligible by silence. Both SKUs; the in-app arming gate disarms on the new blockers unchanged.
- **A Claude review that posted nothing no longer finishes green** ([#3650]) - the reviewer's allowlist forbade every read tool the prompt invited, so swallowed runs read denials ≈ turns and ended without a verdict. Read-only tools (Read, Grep, Glob, git diff/log/show) join the allowlist; the job now fails itself when no non-empty-bodied claude[bot] review was submitted since the run's start, with the transcript's result block in the log and the transcript retained seven days as an artifact.
- **The deprecated Dashboard mirrors five of the brains-review honesty fixes in its own idiom** ([#3653]) - The frozen Full-edition Dashboard (bug-fix support) said the same five untrue things the shipping SKUs fixed today: `get_blocking` / `get_deadlocks` / `get_alert_history` published a capped page count as a total (now `*_returned` + `truncated`, with the reader's fixed cap named as `limit_applied_by_reader`); the Poison Wait alert paged on one slow wait's average and slept through a THREADPOOL storm (now accumulated over the ten-minute window and graded on the shared `PoisonWaitEvaluator` bars, clearing only on an observed quiet window; the ms preference is retired but round-trips); `mute_analysis_finding` answered "muted" for any hash (now `registered` / `matched_now` / `muted_unmatched`); an online server with no collector banded was a green "OK" (now Unknown / `--`); and the Perfmon per-second rate was integer division (now fractional on the read). The anomaly detector's "calibrate on the lab box" markers now state what the fleet pass measured and that it never reached the Dashboard tier. The wait-fact nominal-window divisor stays declined per the issue.
- **Six small honesty riders from the brains-review residue** ([#3653]) - `poison_wait.threshold_ms` (both SKUs) now carries `threshold_ms_note` saying #3593 retired it, and Darling's `update_alert_settings` stores it with a `warnings[]` entry instead of pretending it tunes anything; `get_pg_server_config` publishes `captured_at` and joins the latest-snapshot census the `GetCurrent*Async` name had kept it out of; Lite's `get_latch_stats` / `get_spinlock_stats` bind their hidden `LIMIT 20` to the caller's `limit` and report `*_returned` / `truncated` (the grid keeps its 20 by name); a no-server census fails any reader on either SKU whose `ORDER BY` is an integer expression PostgreSQL/DuckDB would fold to a constant (zero hits today); V79/V122 rung docs name the PRs that superseded them; Lite's `analyze_server` recommends next reads for `HADR_SYNC_COMMIT`.
- **The PostgreSQL poison-wait host holds on silence like the SQL Server engine does, and three presence-flat alerts grade their severity from the bars the health bands already measured** ([#3653]) - The PG Poison Wait host announced "Cleared" the moment its read came back empty — collector silence read as recovery. Because `pg_wait_stats` skips idle events (#2694), the rows cannot witness their own absence, so the read now carries the collector's own `collection_log` run count over the same window and the host clears only on an observed window (the #3593 contract, ported). Deadlocks Detected grades Warning/Critical on the #3368 rate tiers (Darling's V120 knob honoured via `IAlertEngineSettings.DeadlockRateThresholds`; Lite the shipped pair), High CPU grades Critical at the CPU health band's 95% bar, tempdb Space fires an explicit Warning with no Critical tier (no measured bar exists to cite) — so history grids, `get_alert_history` and every channel render the tier the alert fired at instead of the colour its name implies.
- **The Darling viewer's trend charts and calendar tell the same truth the MCP tools learned today** ([#3653]) - The Performance Trends tab read the raw tier only, so a 7-day chart on a TimescaleDB store plotted the 4 days raw still held under an axis that said 7; the query-duration, procedure-duration and execution-count charts now route by retention tier through the same ladder and hourly SQL the MCP trend tools use (moved to Storage as `DurationTrendRouting`, pinned equal on both sides) and each chart titles itself with the tier it served from and a truncation note when the store no longer holds the window's head. The viewer's trend SQL no longer fabricates a 0 for the first differenced point; an unrated point is skipped, not plotted or interpolated. The Performance Calendar judges every day against the store's retention horizon with the one `DailySummaryRetention` decision the MCP reader makes (`DailySummaryHorizon`, Storage; the purge's retention constants and fleet rule move down and the service aliases them), so a purged day is grey, not green, and its tooltip says why - on both SKUs. The PVS top-5 trend no longer plots an unmeasured size as 0 MB (Lite's `get_pvs_trend` carries it as null with `pvs_measured`).
- **`get_pg_server_config` counted the page and called it the server** ([#3653]) - `non_default_count` was a count over the rows fetched, so a limit below the number of chosen settings reported the limit as a fact about the snapshot, and `truncated` was inferred from a full page — always up on the default view. The count is now the snapshot's, computed on the row statement above the cap; the page's own count is `non_default_returned`; the `include_defaults` filter is in the statement so `truncated` is about the population asked for; and the three sibling pages in the same file observe truncation off a `limit + 1` fetch.
- **The four PostgreSQL host alerts fire with the tier they earned instead of no tier at all** ([#3653]) - High CPU, Deadlocks Detected, Blocking Detected and Long-Running Query on a PostgreSQL target passed no severity, so the history grids, get_alert_history and every channel coloured them by name after #3635. CPU now grades on the CPU health band's own classifier (Critical at 95% of the configured ACU ceiling), deadlocks on the shared rate grader against the store's deadlock tiers, blocking and long-running query as an explicit Warning with the missing measured bar named. Rows written before this keep their by-name colour.
- **Darling's `get_pvs_stats` trend drew an unmeasured pass as 0 MB** ([#3653]) - a collection on which the DMV reported no PVS size for a database was published as a real zero in the top-5 series, a cliff in a series that had none. The point is now null with `pvs_measured` beside it, the same two keys Lite's twin has carried since #3666; a measured 0 MB is still a zero.
- **The Query Store duration chart now says what served it and where the rollup's floor cut the window** ([#3653]) - The viewer's fourth Performance Trends chart plotted what the corrected Query Store rollup had materialized under an axis spanning the requested window, with no title; it now carries the sibling charts' disclosure — the route word the MCP tool publishes, the grain seam, and a "data begins" clause only when the rollup's measured floor sits above the requested start. The MCP trend reader's tier ladder and hourly SQL became aliases of the Storage definitions rather than pinned-equal copies, so the tool and the viewer cannot drift.
- **The analysis pass no longer says "collection appears to have stopped … NOT an all-clear" for a window the coverage witness proves the collector observed** ([#3653]) - The empty-window rule in both analysis services tested `facts.Count == 0` before it tested the coverage witness, so an observed window that simply produced no fact wore the dead-collector envelope: `unavailable` with a pointer at collection health from `analyze_server`, and the Viewer's dead-collector marker from the scheduled pass. The rule now gates on `ObservedDurationMs <= 0` alone; an observed window with no facts runs the pass and reads `empty` at its stated coverage, with one Information line naming that coverage for the scheduled pass.
- **The PostgreSQL analysis vocabulary absorbs what six content lanes reported back** ([#3542]) - Two `pg_config` facts were inert by construction: `autovacuum = off` and `maintenance_work_mem` had no scorer arm, so they scored 0 and no co-fire could lift them; `autovacuum = off` is now a 0.9 posture fact and `maintenance_work_mem` is evidence-gated on the backlog the way `work_mem` is on the spill. The connection ceiling subtracts PostgreSQL 16's `reserved_connections`, edges can name "the bad actor" through a stable `PG_BAD_ACTOR` alias the graph resolves to the window's heaviest statement, and a bad actor's next reads lead with `get_pg_query_duration_trend`.
- **A target that restarted, failed over or was re-pointed no longer keeps subtracting from the old instance's counters** ([#3653]) - Every delta family subtracted from a baseline cached under the server_id with nothing checking the readings came from the same instance; a failover to a busier replica stored `new total − old baseline` as one interval's work. Both hosts now detect an identity epoch on a read they already make — `sqlserver_start_time`/`@@SERVERNAME` for SQL Server, `pg_stat_statements_info.stats_reset` for the PostgreSQL statements family — compared against the pair persisted in collector_state, forget the affected baselines before subtracting, mark the run's collection_log note (`identity_epoch_changes=1`) and log old→new at Information; Darling's same-id reconnect on a definition edit forgets its baselines too.
- **A quiet hour no longer halves the next hour's Query Store rate** ([#3653]) - the rollup-routed Query Store duration trend rated every point over the gap to the previous EMITTED point, so a corrected-hourly bucket with no rows made its neighbour's denominator 7,200 seconds and published half its true rate; each point class now carries its own denominator - a rollup bucket its bucket width (and is therefore always rated, first bucket included), a raw Query Store interval its spacing (the store holds no interval length for it, said on the CTE). Both the MCP tool and the viewer read one builder.
- **The query-stats trends read the interval the store has, on both SKUs** ([#3653]) - `query_stats` has carried `sample_interval_seconds` from the start, yet the query-duration and execution-count trends (Darling viewer, Lite) LAG-recomputed it from row spacing and divided a restart row's fabricated 0 into a confident 0.00 ms/sec; they now read it three-state like the procedure trends have since V128 - stored interval, 0 = unrated, LAG only for a pre-upgrade collection - through one Storage builder. The MCP reader's own raw query-stats const is the named one-line follow-up.
- **Compose delta aggregates exclude the restart marker** ([#3653]) - every raw-tier SUM/AVG/MIN/MAX over a delta column on one of the ten interval-carrying families now carries its own `FILTER (WHERE sample_interval_seconds IS DISTINCT FROM 0)`, so a restart's (0, 0) marker is no longer averaged in as a measured zero or reported as the window's minimum; gauges, overlays, ratios, COUNT(*), Query Store and the rollup route are untouched.
- **`mute_analysis_finding` stops writing the hash as the path and stops registering the same mute twice, `remove_server` can remove a server that never connected, and eight MCP descriptions stop saying what the code does not do** ([#3653]) - The mute registry write is one guarded `INSERT … WHERE NOT EXISTS … RETURNING` on both SKUs: idempotent per (scope, hash), the `story_path` column names the chain resolved from the retained findings (the hash only as a disclosed placeholder), and the payload says `registered` / `already_muted` / `story_path` truthfully. `remove_server` resolves against `config_monitored_servers` — the table it deletes from — so a definition that never connected is removable, with `ever_connected` and `matched_in` disclosed. Descriptions fixed: `audit_config` (no edition branch exists; PostgreSQL targets are refused and redirected), Lite `get_cpu_utilization` (one ring-buffer record per minute; 15 s is Azure's `dm_db_resource_stats`), the plan tools (`top_operators` publishes its cap/count/truncation and its ranking basis; `missing_indexes` carries a labelled statement-scoped impact and column lists, no paste-ready DDL), `get_ag_health` (lag not banded, lag 0 while suspended, stale last snapshot), the duration-trend catalogue line, and `get_resource_semaphore` row order now matches across SKUs.
- **The web server page labelled page sums as totals, drew one instant as a trend, and four tools called their capture time by another name** ([#3653]) - `get_pg_database_stats` now computes `total_temp_files` / `total_temp_bytes` / `total_deadlocks`, `database_count`, a cluster-wide `cache_hit_pct` and the reset flag as window aggregates on the same statement as its rows, publishes the page's own figures as `returned_*` / `databases_returned` / `cache_hit_pct_of_returned`, observes `truncated` off a `limit + 1` fetch instead of inferring `limit_reached`, and the A7 census's one stated allowance is gone; `get_pg_io_stats` spells its page count `combinations_returned` like every other paged tool. The Memory Grants line chart drew `grants[]` — one instant, every row at the same stamp — under a window caption; it and the Resource Semaphore table are now two tables per read, the `window[]` aggregates (peak waiters and when, peak granted, floor of available, timeouts and forced grants across the window) first and the newest snapshot second, labelled as a moment. `get_database_sizes`, `get_running_jobs`, `get_server_properties` and `get_session_stats` stamp `captured_at` on both SKUs and join the latest-is-a-time roster; the `StampedUnderCollectionTime` allowance is deleted.
- **The wait and perfmon anomaly baselines stop counting every restart's fabricated zero as a quiet sample** ([#3653]) - Interval-honest successor aggregates (`sample_interval_seconds IS DISTINCT FROM 0` baked in, the measured interval carried) replace the legacy pair in place; the provider reads whichever supply covers its window, divides by the stored interval and applies the `LAG > N` heuristic only to pre-column rows, and the legacy pair retires by a self-executing coverage condition about five days after the release. The startup baseline backfill now fires on the first start (its gate read coverage through the real-time view), and a Daily-routed Performance Calendar answers the days its rollup has not reached from raw instead of printing `unique_queries = 0`.
- **Three PostgreSQL pages stop guessing truncation from a full page, and four of the eleven payload rules stop being sentences** ([#3653]) - `get_pg_deadlocks`, `get_pg_plan_capture_readiness` and `get_pg_index_bloat` published `truncated = rows.Count >= limit`, and the two that withhold their summaries on that flag withheld them for pages that were the whole set; `get_pg_plans` cut at `limit` and said nothing. All four now fetch `limit + 1` and observe through one shared `McpHelpers.BoundPage`, the page census sweeps the inference across every tool body on both SKUs instead of a roster (which is why it missed these), and `McpPayloadContractCensusTests` pins errors-one-shape, refuse-what-you-cannot-honor and name-is-truth as far as a census honestly can, with the one-vocabulary inventories (14 truncation dialects, 8 severity spellings, two error shapes) as exact rosters for the lane that collapses them.
- **The MCP query-duration trend still divided a restart's zero into 0.00 after the viewer stopped, and two sentences that #3695/#3696 made false** ([#3653]) - `get_query_duration_trend`'s raw route now reads `query_stats.sample_interval_seconds` three-state through the same Storage builder the viewer runs (`DarlingTrendReader.QueryDurationTrendSql` and both `ProcedureDurationTrendSql` consts are aliases of `DurationTrendRouting`), so a restart collection is unrated instead of a confident 0.00 ms/sec and a measured pass is no longer halved by a LAG spanning it. The trend trio's descriptions say which points are rated over what (rollup buckets over their width, first bucket included; raw points over the stored interval or, lacking one, the LAG). `get_cpu_utilization`'s Darling note ports Lite's corrected cadences (one ring-buffer record per minute on-prem; 15 s is Azure's `sys.dm_db_resource_stats`), pinned byte-identical across SKUs.
- **The perfmon chart plotted deltas under a "Value" label with the divisor sitting unused on the row, and a latch/spinlock restart read as zero** ([#3653]) - both viewers now shape every perfmon series through the stored `sample_interval_seconds` via one shared `DeltaSeriesShaping`: a counter whose name ends in `/sec` plots per second (a name proxy until the `cntr_type` rung lands, its mis-classes named), every other counter plots its raw delta under a legend and axis that say so, and a restart's (0, 0) is a line break, not a trough. Both latch/spinlock snapshot grids render the (0, 0) marker as "—" with an Interval (sec) column saying "restart / first sample", and Lite's `get_latch_stats` / `get_spinlock_stats` publish null, not 0, for it. The gap clause was already landed by #1944's cadence rule; this adds the restart-at-normal-cadence break it could not see.
- **One payload said NoData and No Data, and the page cut had five names** ([#3653]) - get_daily_summary / get_daily_summary_range publish overall_health and health_band from one enum token on both SKUs; four PostgreSQL tools, the autovacuum run history and get_analysis_findings stop inferring a cut from a full page and fetch one past their cap through McpHelpers.BoundPage, publishing truncated and *_returned; McpPayloadContractCensusTests classifies every cut key and severity-word literal by what it is (six of #3699's eight "severity spellings" were other vocabularies) and fails a retired or unclassified spelling by name. McpHelpers.ValidateDaysBack adopts the five inline days_back refusals and list_servers goes through FormatError; FormatError's wire shape awaits ruling Q11.
- **Five families subtracted from the old instance once before anyone noticed it had changed, and Aurora's wait counters had no one watching for a restart at all** ([#3653]) - #3694's only SQL Server identity carrier was `cpu_utilization`, tenth in the schedule, so on the pass where a restart, failover or re-point first became visible wait_stats, latch_stats, spinlock_stats, query_stats and procedure_stats each subtracted the new instance's counters from the dead instance's baseline once and then ate a second (0, 0) re-baseline pass. `WaitStatsCollector`, first in the order on both hosts, now carries the identity pair (`sqlserver_start_time`, `@@SERVERNAME`) as a second result set observed after the rows are read and before anything subtracts, so every SQL Server delta family re-baselines against nothing on the epoch pass; the CPU carrier stays as the fallback for an operator who disables wait_stats, and `ServerEpoch` remembers per calculator and per server the identity it last forgot to, so two carriers seeing one epoch forget once. `PgWaitStatsCollector` carries `pg_postmaster_start_time()` as its own epoch (a clean restart zeroes `aurora_stat_system_waits()` while leaving `pg_stat_statements_info.stats_reset` continuous) and forgets only the wait groups. Both census rosters (`FamiliesThatSubtractOnceBeforeTheCarrier`, `FamiliesWithoutAnEpochCarrier`) are empty and asserted empty. No Azure SQL DB epoch (#3694's ruling stands); the marker is still the run's `collection_log` note plus `collector_state`, not rendered.
- **Lite's plan-cache trend descriptions rated every point over the gap since the previous one, the shared unrated_note named one of two unrated reasons, and the latch/spinlock restart row was spelled two ways across SKUs** ([#3653]) - Lite's `get_query_duration_trend` / `get_procedure_duration_trend` descriptions and instructions row now carry the three-state rule (stored interval; a restart's 0 is unrated; LAG only where no interval was stored). The trio's `unrated_note` names both the stored-0 restart and the first-in-window LAG in one sentence, byte-identical on both SKUs. `get_latch_stats` / `get_spinlock_stats` publish the unknowable row one way on both SKUs — per-second rates null, `interval_seconds` null beside them, the latest delta null — with Lite gaining `interval_seconds` and the per-second pair, Darling's latch nulling `severity` and the banded-from delta instead of a LOW from a zero nobody measured, and Darling's spinlock query returning the interval its null rates lacked.
- **Fifteen rate arms spelled "unknowable" as 0 behind a guard that happened to hide it** ([#3653]) - Every `CASE WHEN interval > 0 THEN delta / interval` that fell back to `ELSE 0` — the file-I/O throughput trend on both SKUs, nine LAG-differenced PostgreSQL trend rates, and the two wait-ms/sec baseline arms — now ends at `END`, so a collection with no interval to rate over yields NULL rather than a measured 0.00; the PostgreSQL trend readers and `get_pg_*_trend` carry that null instead of reading it back as 0, and `evictions_per_second` follows. Two of the fifteen were live: two collections landing in one second on a pre-column store rated a 0 ms/sec sample into the wait baseline (mean 66.7 where the rated collections say 100), so the baseline's row filter also takes Lite's `interval_sec > 0` before the restart LAG. Both census rosters are empty and asserted empty.
- **A falling gauge read as a counter reset because the store did not know it was a gauge** ([#3653]) - `perfmon_stats` gains `cntr_type` (Darling V132 / Lite v62), so the collector writes a gauge such as Total Server Memory (KB) as its level with no delta instead of differencing it like Batch Requests/sec and storing the (0, 0) "counter reset" marker when the level fell. Both viewers' perfmon charts plot a gauge as its value, a rate per second and everything else as its per-interval delta, classified by the stored type with the `/sec` name proxy kept only for rows written before the rung; both SKUs' `get_perfmon_stats` / `get_perfmon_trend` publish `cntr_type` and `counter_kind` and null a gauge's `delta_value`. The ten Wait Statistics "Average wait time (ms)" instances are PERF_AVERAGE_BULK numerators whose average needs a base row this store does not join — stated, not fixed.
- **A maintenance job's own sub-threshold anomalies fold onto the job's incident and name the job** ([#3704]) - #3632's three maintenance edges (SCH_M, IO_WRITE_LATENCY_MS, WRITELOG → RUNNING_JOBS, gated on the job having fired) were keyed on the regular symptom facts, and AnomalyIncidentReconciler folded an ANOMALY_* story only onto a regular story carrying its family — so four one-fact anomalies during one server's 116-minute index-maintenance job paged separately and never said the job's name. A maintenance-family anomaly's fold-target set now includes any story carrying RUNNING_JOBS (the same fired-gate, seen through the story set; no new graph edge), and its frozen Investigation gains one sentence naming the job from the fact's ObjectName (#3693). Non-maintenance anomalies stay solo, as #3632 ruled.
- **PostgreSQL baselines survive a shortened retention, and pg_cpu carries the CPU dispersion floor** ([#3691]) - The four `pg_*` raw hypertables the PostgreSQL-target baselines read directly were user-editable and unfloored, so a 7-day retention would have silently switched every PostgreSQL anomaly detector off; the purge now floors them at the 30-day baseline window, the detector's own minimum-history gate, exactly as #1757 floored `cpu_utilization` / `file_io_stats`. `BaselineMath.AbsStdDevFloorFor` gives `pg_cpu` (percent of the capacity ceiling) the 5-point floor SQL Server CPU carries, by unit parity, so a flat month of ACU utilisation no longer manufactures a display-capped z-score on a one-point wobble.
- **`compare_analysis` sigma-bands the PostgreSQL baselined metrics** ([#3691]) - On a PostgreSQL target the tool banded a 10 → 60 tps move "stable" (absolute rule; `PG_TPS` sits at ladder 0 by design) in the window the detector called 25σ, because only SQL Server keys were mapped to baselines. `PG_TPS`, `PG_CONNECTION_SATURATION` (on its peak session count, never the fraction), `PG_DEADLOCK_RATE` and `PG_CPU_PERCENT` (capacity unit only) now band against their own `pg_*` hour-of-week buckets; the `plan_cache_churn` note speaks the engine of the facts. SQL Server output is byte-identical.
- **`get_analysis_facts` now runs the anomaly detector on both engines** ([#3691]) - the read the tool sold as "every observation the engine sees" was collector + scorer only on Lite and Darling alike, so it never showed an ANOMALY_* / ANOMALY_PG_* fact and the detector's gate metadata (deviation_sigma, fire_threshold, baseline_samples, threshold_lineage) was reachable only through a finding that had already crossed the severity floor. The resolved engine's detector now runs between collector and scorer over the same window, gated on the coverage witness like the pass, so anomaly facts arrive scored with their metadata — including the ones that fired but stayed under the finding floor. Descriptions and instructions say so on both SKUs.
- **Full-edition `perfmon_stats.cntr_value_per_second` divided as an integer, so every counter under one event per second read 0/sec** ([#3653]) - The computed column is now `cntr_value_delta * 1.0 / NULLIF(sample_interval_seconds, 0)` (numeric(33,12)) in `install/02` and `06`, and `06` converges an existing install idempotently, guarded on `sys.computed_columns` by the column's integer type rather than its rewritten definition text. `report.daily_summary_v2`'s throughput rows stop averaging truncated zeros; the Dashboard's read was already fixed in #3658, this is the schema half.
- **The write family tells Aurora the truth** ([#3691]) - On aurora-postgres, PG_CHECKPOINT_PRESSURE and CONFIG_PG_MAX_WAL_SIZE were a permanent measured 0 and a forever-0.4 advisory about a knob the engine never consults (fifty clusters: sixty synthetic timed checkpoints an hour, requested share 0). Both are now emitted `not_applicable` with the shape and reason in metadata, score 0, root nothing, and say what Aurora does instead. Where the engine reports WAL, PG_WAL_VOLUME_SHIFT states the window's mean/peak bytes/s (or why it cannot) and ANOMALY_PG_WAL_VOLUME grades the peak against the server's own hour-of-week baseline, lifting and folding onto the checkpoint incident it leads.
- **Three hourly rollups counted every restart's fabricated zero as a sample, and a service outage longer than a day left a two-hour hole under a floor that said covered** ([#3653]) - `query_stats_interval_hourly`, `procedure_stats_interval_hourly` and `query_stats_db_interval_hourly` carry the collector's knowability verdict in their WHERE and the measured interval summed, every hourly-tier reader takes them where they reach as far as the legacy, and the refresh phase grid re-derived itself around them (heaviest refresh at :18, watch line 900 s). At service start every continuous aggregate is scanned for bucket ranges its source holds rows for and it never materialized, and each is closed with one refresh over exactly its bounds — no policy window widened.
- **The PostgreSQL engine absorbs what the v2 lanes reported back** ([#3691]) - Shortening PostgreSQL retention no longer starves the I/O-latency and WAL-volume detectors (both tables floored at the 30-day window). The I/O-latency baseline is built at a quarter-hour grain with a 250-read floor, so it is judged against this hour of the week rather than collapsing to the hour of the day. A slot-retention or replication-lag finding is now lifted by a fired WAL-volume anomaly (the co-fire had read a fact that could never fire), a Lock wait walks to the idle-in-transaction holder behind it, and the idle finding's next reads include the xmin-horizon and blocking tools. compare_analysis says which PostgreSQL keys are baseline-banded. Wave-3 stubs for the blocking family (lane 17).
- **`get_fleet_overview` runs its collection-health rollup once per minute per host, however many overview calls race** ([#3735]) - the 7-day per-collector health aggregate behind every fleet card is the one read in the overview that does not depend on `hours_back`, so a caller racing three calls with three windows had the store run three identical copies of it concurrently — photographed on the largest production store inside the collectors' flush band, the slowest crossing the `mcp` role's 15 s `statement_timeout` while the plan runs in 680 ms alone. Concurrent callers now share one in-flight scan and a result under 60 seconds old (the fastest collector cadence, so nothing the rollup bands can have moved) is served from memory, per host; a caller that gives up releases only itself, a failed scan is never memoized, and the payload's new trailing `collection_health_age_seconds` says how old that half of the roll-up is. The SQL, the command deadline and the server-side cap are unchanged - the cap did its job.
- **`compare_analysis` sigma-bands the v2 PostgreSQL baselined metrics, and the write family has one definition of "WAL tracked"** ([#3691]) - The v2 lanes stored three more hour-of-week buckets (I/O read latency, replay lag bytes, WAL bytes per second) and their detectors judged against them, but the compare tool still banded those keys by the absolute ladder — a 16× latency move read as a ladder step in the same window its detector called 25σ. `PG_IO_READ_LATENCY_MS`, `PG_REPLICATION_LAG` and `PG_WAL_VOLUME_SHIFT` now band in their bucket's sigma (a fact that is `unavailable`, under its ops floor or holding a mean rather than the peak is withheld from sigma, never banded in the wrong unit); the checkpoint and WAL-volume reads apply the same `WalIsTracked` predicate, so a 0-not-NULL WAL series can no longer be "tracked" to one fact and "not reported" to the other. SQL Server output unchanged.
- **Hour-of-week baselines key on the target's local clock, not UTC** ([#3653]) - Both SKUs keyed the hour×dow baseline on the collector's UTC `collection_time`, so one bucket pooled two local hours across a DST change and a finding's `baseline_bucket` named an hour nobody on the server keeps. The scaffold and the two event arms now key on `collection_time` shifted by the target's offset step function (from `server_properties.time_zone_id`, falling back to `utc_offset_minutes`, then UTC), the lookup uses the same numbers, and nothing keyed is stored, so existing baselines re-bucket at the next compute with no migration.
- **PostgreSQL targets key their hour-of-week baselines on their own clock** ([#3691]) - Q6 (#3749) read the target's clock from `server_properties`, a table only the SQL Server collectors write, so every PostgreSQL baseline bucket stayed UTC hour-of-week while the SQL Server side went local. The clock read is now a second seam on the baseline provider, and the PostgreSQL provider reads the `TimeZone` setting from the latest `pg_server_config` snapshot at or before the window end (session-scoped sources excluded); UTC is the fallback when no snapshot carries one, not the rule. A "Tue 09:00" bucket on a New York target is 09:00 in New York.
- **The long-query completion XE session could never be created on Azure SQL DB** ([#3753]) - `sqlserver.server_principal_name` is not an action a database-scoped session may carry, so the `CREATE` failed with error 25744 on every database, every sweep, and the collector was dead on Azure by construction. The Azure DDL is now generated from a closed list of the actions Azure rejects (`nt_username`, `server_principal_name`) and carries `sqlserver.username` in their place — the action Microsoft's own Azure profiler template substitutes — so the `server_principal_name` column is populated on Azure rather than NULL. On-prem DDL and payload are unchanged; pins assert the Azure DDL names none of the rejects.
- **database_scoped_config collected nothing on Azure SQL DB, and said so nowhere: the three-part `[db].sys.sp_executesql` it called Azure-compatible is rejected there** ([#3755]) - on a logical-server registration every enumerated database was rejected with "Reference to database and/or server name ... is not supported", the host tolerated each as a per-database skip, and the run logged SUCCESS with zero rows behind a HEALTHY row (the swallowing itself is [#3754]). The collector's Azure branch had only ever been in its database LIST. It now declares `RunsPerDatabase` on Azure SQL DB and rides the per-database connection loop both hosts already check before enumeration - the `query_store` / `plan_correction` shape - running one shared payload body bare on the connected database while on-prem, RDS and Managed Instance keep the enumeration idiom byte-for-byte in structure; the same body carries the on-prem list's system-database screen on `DB_ID()`, so master's read-only defaults stay out of the Azure row set exactly as they always have on a box.
- **A pass whose fact families could not be read says so, on both SKUs** ([#3691]) - every family read in the three fact collectors (Lite DuckDB, Darling SQL Server, Darling PostgreSQL target) degraded to "no facts" and only logged, so a pass whose reads timed out or hit a half-migrated store scored an empty list and `analyze_server` rendered the `empty` all-clear. Each failure is now recorded on the analysis context beside its log line (family, read, outcome: timeout / cancelled / missing_schema / error) and both `analyze_server` and `get_analysis_facts` carry `collection_caveats` ({families_failed, families_total, entries}) plus a "N of M fact families could not be read — the absence of findings is not evidence" sentence, emitted ONLY when a family failed so a clean pass's payload is byte-identical; the `empty` envelope also states `fact_count` and `facts_scored`. #3767 corrected the live pin to count missing families rather than reads after step 22 gave `pg_autovacuum_stats` a second vacuum read.
- **A collector run whose every item failed is no longer SUCCESS, and get_collection_health stops saying a faulted collector read and found nothing** ([#3754]) - On an Azure SQL DB logical server two collectors failed on every database every sweep and recorded `status = SUCCESS, rows_collected = 0`; the health tool banded them HEALTHY with `errors: 0` and a finding that said, in one sentence, four false things. Three mechanisms produced it — the enumerated fan-out's per-item catch handed the exception to a closure that only logged, the XE reconcile's failure never reached the run record, and the health prose inferred "read and found nothing" from the absence of a note that the swallow itself had guaranteed — and each takes the fix its sibling path already had: all-items-failed is `FAILED` with the first error carried, some-failed carries the partial note (#2623's shape), and the "found nothing" sentence is gated on zero recorded errors. The two Azure platform gaps underneath are #3753 and #3755, fixed separately; the collectors' SQL is untouched here.
- **The PostgreSQL Long-Running Query twin honours the program/login opt-out knob** ([#3743]) - Since #3734/#3736 the knob was visible, editable and read back on a PostgreSQL target and did nothing there, so a permanent ETL or replication worker under a service role filled the page every sweep. The twin now applies the same knob through the shared predicate builder over `application_name`/`usename` (prefix / exact, case-insensitive, escaped), ahead of the row cap, and renders the excluded-by-arm counts on the card exactly as the SQL Server twin does. The SQL Server seeds match nothing on PostgreSQL by construction; PostgreSQL-specific seeds are a production read, not a guess.
- **The materialization hole scan reports on every start** ([#3756]) - The scan wrote a summary only when it had repaired, deferred or failed something, so a start that found nothing left the same silence as a start that never reached it. The worker now writes one unconditional INFORMATION line per start with the pass's full tally (aggregates walked, holes and buckets found before the cap, repaired, deferred, still-reading, isolated failures, elapsed), a distinct line when shutdown cancels the scan, and the pass itself no longer writes a summary of its own.
- **The Long-Running Query alert applies `excludedDatabases` ahead of the row cap on both SQL Server SKUs** ([#3742]) - Both reads dropped excluded databases' rows client-side AFTER `LIMIT`, so an excluded database whose sessions were the longest consumed the page and the alert came back short or empty while matches existed. The list is now the read's third pre-cap flag beside the opt-out knob's two, the card carries an `Excluded By Database` receipt when the list is set, and the two adapters' post-read filters are gone. The PostgreSQL-target read follows separately.
- **The CPU alert gate identifies a sample by equality, not order, and keys on the stored UTC instant** ([#3744]) - The persistence gate counted a sample as fresh only when its stamp was strictly greater than the last one, fed the target's local clock, so every autumn fall-back left CPU alerting blind for the repeated hour on non-UTC servers, and #3730 had to leave the gate on the local stamp to avoid the same freeze at the V134 upgrade. A different instant is now a different sample whichever clock stamped it; both SKUs' latest-CPU reads hand the gate sample_time_utc where the row has one, and the frame switch costs at most one extra count of one sample per server, once.
- **query_store_health collects per database on Azure SQL DB** ([#3764]) - The collector fanned out through EXECUTE [db].sys.sp_executesql, which Azure SQL DB rejects for every database that is not the connection's own, so logical-server registrations stored nothing while reading HEALTHY and the READ_ONLY cap-hit transition the collector exists for was never observed there. It now takes the host's per-database connection loop on Azure (the #3760 shape), one payload body serves both paths, and the Azure enumerator set is pinned empty.
- **The PostgreSQL Long-Running Query read applies `excludedDatabases` in the read, ahead of the row cap, and the Aurora wait-profile detector gates on peak AND mean** ([#3742], [#3691]) - The PostgreSQL twin dropped excluded databases in C# after `LIMIT`, so a reporting database's ETL could fill the page and the alert came back short or empty while real long-running sessions existed; the list is now the `candidates` CTE's third flag through the shared builder's five-argument overload, counted on the card as `Excluded By Database` beside the knob's counts. The Aurora `ANOMALY_PG_WAIT_PROFILE` detector took #3773's shared peak-AND-mean gate, so one hot five-minute delta in a quiet window no longer reads as a profile shift; `mean_ms_per_sec` / `mean_modified_z` ride beside the peak. No bar values changed.
- **PostgreSQL connection saturation now divides the real population by the ceiling** ([#3691]) - `PG_CONNECTION_SATURATION` read its peak off `pg_session_states`, an exception table — high on servers that capture often, blind on ones that never do. The numerator is now `pg_stat_database.numbackends` (V133), summed over databases at one minute and peaked over the window, with the capture peak as the stated fallback on stores that lack the column; the fact carries both readings and which one decided, the advice names the instrument, and the 0.8 / 0.9 bars stand (measured 2026-09-20: numbackends/ceiling p99 median 1.8 %, fleet max 10.0 %).
- **The rollup-routed daily calendar prints NULL, not 0, for a day the tier never materialized, and names it** ([#3653]) - A day at or below a server's rollup ceiling with no rollup row fell out of the queries LEFT JOIN and COALESCE printed unique_queries = 0 beside that day's real wait, CPU and deadlock numbers — the same 0 a measured-quiet day prints. The routed CTE now asks the hole scan's own two questions per day (no rollup row AND the aggregate's source holds admitted rows), emits NULL for such a day, and get_daily_summary / get_daily_summary_range carry unique_queries: null plus a trailing days_missing[]; the desktop and web calendars print "not materialized at this tier" and the band, which never read the count, does not move. Darling only — Lite has no rollup tier.
- **The CPU collector's watermark and dedup key on the UTC twin where the store has it, with the frame stated** ([#3778]) - The ring-buffer read deduplicated against MAX(sample_time), the target's LOCAL clock, so from the autumn fall-back every sample for the repeated hour read as already collected and no cpu_utilization_stats row landed for ~1 h on every non-UTC server once a year — the collector half of #3744's blind hour. Definitions may now declare a UTC watermark twin; both hosts read the pair in one statement and hand the collector the twin's maximum with its frame when any row carries it, else the local maximum, and the dedup compares the row in the watermark's frame. The first post-upgrade run is local-to-local exactly as before (zero duplicates, zero extra drops), every run after it is UTC-to-UTC; Azure SQL DB's server-side arm is pinned unchanged.
- **A quiet PostgreSQL pool is graded from numbackends alone, and the Aurora wait-profile advice names the window mean** ([#3691]) - A window with no session capture emitted no connection-saturation fact even at 96% of its usable ceiling; now, where pg_stat_database.numbackends covers the window, the card is emitted as a level-only card that says its state, application and idle-in-transaction shares are unobserved (no capture-derived keys, so the parked-share amplifier, the idle edge and compare banding read "not measured", not 0), while a window with a capture is byte-identical. The Aurora wait-profile advice now states the window's mean rate against its routine beside the peak, so one hot collection is no longer read as the whole window's number.
- **The trend family's window floor is `window_truncated`, not the page dialect's `truncated`** ([#3653]) - get_query_trend, the three duration-trend tools and get_query_store_top published "the store did not hold the whole requested window" under the same key twenty paged tools use for "your limit bit", two facts with opposite remedies — no limit changes the window floor. The flag is now window_truncated beside effective_start / effective_hours_back on both SKUs; every consumer (the web query-trend notice, nine tool descriptions, nine instructions rows, five web catalog rows) follows, each description states the wire change, and the payload census fails a bare truncated beside effective_hours_back on either SKU so the homonym cannot return. The C# members keep their names; the page cut keeps truncated.
- **Item 17's three unpinned description fixes get their pins: get_ag_health names its reader's traps, the catalogue's duration-trend line says rates not percentiles, and the server page reads every stamped tool by captured_at** ([#3653]) - The six A15/A16 drifts the item still listed were already delivered by #3665, #3696 and #3697; three of the fixed texts had nothing holding them. `DarlingMcpAgToolsSurfaceTests` anchors on `DarlingAgReader`'s own trap paragraphs and holds the description and instructions to both; `DarlingMcpTrendToolsSurfaceAndSqlTests` holds the web catalogue line to the two rates `SerializeTrend` publishes and refuses "percentile" anywhere in the catalogue unless negated; `ServerPageTabsTests` derives the Stamped roster from the stamp census and refuses a `collection_time` descriptor against any of them, tile or column. Pins only — no wire change.
- **Four census pins the V137 wave outran** - The rung shipped with a "no product source names any of these eight columns yet" scan so that each consumer lane would have to release its clause deliberately, and that pin's honest lifetime runs from the rung's merge to the first consumer's — which was forty minutes. Three pull requests edited the one method body inside an hour, each to get a reader past it, before it was retired whole in favour of the positive ownership pins the consumer lanes already carry and which are stronger than a name scan: a census cannot tell a payload key that PROMISES a column from a read of it. Beside it, three roster repairs: the clutter lane's fence was moved off the tool file, which contains no SQL and so matched nothing, onto the reader and judgment files where a capture-mode read would actually land; the DuckDB ladder pin restated the climb the rung had just moved; and the member scan's known-truncated roster followed dev's own run in both directions at once, one expression-bodied property arriving with the plan-regression lane and one leaving as the buffer-composition lane reshaped it into a block the walk reads whole. Test rosters only — no product change, and nothing a reader of the product sees.
- **The plan tools stopped saying "no CREATE INDEX text — a hint, not a design"** ([#3805]) - #3696 dropped `missing_indexes[].create_statement` from `analyze_*_plan` on both SKUs and the Dashboard citing a rule the maintainer never made. The statement is back on every row beside `impact_basis`, each row carries one fixed `caveat` (requests are weak, plan-cache-bounded evidence that corroborate a measured-slow plan and never drive a finding; any new index can regress other statements and adds write cost — test it), the descriptions say "corroboration, with caveats", and the analysis engine's MISSING_INDEX card opens with the same sentence at the Information rung, below every standing misconfiguration.
- **pg_statement_stats runs again on PostgreSQL clusters whose pg_stat_statements is below 1.9 or installed outside public** ([#3818]) - The statements-epoch read of pg_stat_statements_info was gated on the server major, which says nothing about the extension's catalog version; on an upgraded fleet it failed the whole collector with 42P01 on every cluster left at 1.8. The read is now gated on the relation's existence at query time, in the schema the extension actually lives in, and a missing companion object is recorded as the extension present below 1.9 with ALTER EXTENSION ... UPDATE as the remedy, never as the extension missing.
- **Held retention policies no longer wait for a restart: the coverage gate is re-judged on the running service's hourly store-maintenance tick** ([#3812]) - EnsureRetentionPoliciesAsync had one call site, the start path, so a backfill that worked left the policy held and the Retention Held alert firing until someone restarted. The sweep now also runs after the compression-job check on the hourly tick, failure-isolated under a five-minute budget, counts transitions (armed/re-held this pass) off the job's scheduled flag rather than verdicts, and writes one unconditional 'Retention re-evaluation: N policies held, M armed this pass, K unchanged' line every hour. The alert text, runbook, --backfill-rollups verb and README now make the restart optional.
- **The PostgreSQL engine absorbs what waves 4–5 reported back, and the stock load storm can now page** ([#3691]) - `get_pg_database_stats` surfaces `numbackends`; `get_pg_cpu_utilization` projects the six host-memory columns the composition checks read; a temp spill chains onto the memory-overcommit advisory; `ANOMALY_PG_CPU_BURN` joins the load-family confirmers, so the TPS / session-spike pair lane 36's real-stock storm measured at exactly 1.30 reaches 1.6 with kernel CPU firing beside it; the replication live test is anchored on its own `as_of`; four stale docs say what the code does. And a real defect from that storm: `PgAutovacuumStatsCollector` compared the reloption to `'false'` while PostgreSQL stores `off` (also `0`, `no`, `f`, `n`, `fal`) — `NOT option_value::boolean` now, in both places the flag is computed, so `CONFIG_PG_AUTOVACUUM_DISABLED` can fire for the docs' own spelling.
- **Two counts nothing re-derived turned dev red on three checks** - The PostgreSQL anomaly-key census asserted thirteen keys where fourteen are declared and the comment directly above the assertion enumerated all fourteen. Neither lane was wrong alone: two of them each added one key to a base of twelve and each wrote the same `Assert.Equal(13, ...)` line, so because the two lines were textually identical the three-way merge took them as ONE change, raised no conflict, and the arithmetic was never re-done — the spelled-out sibling in the same file did conflict, and was corrected. The second defect is the fleet-scoped read inventory, the prose every collection-health surface concatenates to name the reads that can put a number in the instance total and in no server's count: it promised nine reads where eleven can fail, because the plan dimension's slack read and the store checkpointer read were added and named nowhere, and an operator reading a nonzero instance total against that list would have hunted a cause the list does not contain. That half had been red for two merges and nothing reported it, because the pull request that introduced it merged while its own build check was CANCELLED rather than failing. Both counts are now re-derived from source, both new sites are named in the constant and asserted present in the recorded set, and the Darling-only tally in the mute-rule comment is recounted as well — it said four where the answer is nine.
- **The companion-object remedy told twenty-three clusters to UPDATE an extension none of them had** ([#3830]) - A 42P01 naming `pg_stat_statements_info` was recorded as a companion fault with one remedy, `ALTER EXTENSION pg_stat_statements UPDATE`, and that remedy was reached by inference: the base object resolved, therefore the extension is present, therefore it is present below 1.9. The middle step does not survive the Aurora flavor, where the object that resolved is the native statements function — not the extension's, and needing no extension at all — and the store-side read of extension availability settled what those twenty-three clusters of the measured fifty actually were: no `pg_extension` row in any database, the extension never created anywhere, the collector productive throughout. The row now ARRIVES at the fault mapping instead of being inferred: one more scalar on the statement every PostgreSQL connect already runs — no second query, no extra round trip, no grant beyond what the connect already needs — carried on the runtime with an `Observed` flag whose default is "not observed" rather than "no row", because those two readings are opposite remedies, and scoped to the database the fault came from, since `pg_extension` is per database and a per-database collector faults in databases the connect-time read never saw. Four verdicts, four sentences, each asserted as a whole string because the defect was a sentence whose clauses were individually plausible and wrong together: no row on Aurora says the create is OPTIONAL and buys the companion and nothing else; no row on vanilla asks for the create with the preload-library clause back; below 1.9 names the version read out of the catalog rather than offering it as the first of two possibilities; at or above 1.9 says no update is owed and would change nothing. A version nothing can rank keeps the old two-possibilities hedge, kept only where there is no answer to prefer over it — and the version comparison is a comparison rather than a string compare, because an ordinal compare ranks 1.10 below 1.9 and would send a fleet two releases ahead back to run an update it has already run twice, which is this defect one version further on. One tripwire pin was repaired rather than worked around on the way past: it asserted that the connected-database column ends each detection query while its own doc said what it was protecting — the readers index by number, so a column inserted AHEAD of it shifts five other fields onto the wrong values — and "last in the list" forbids appending, which cannot shift anything, while passing the insertion that can. It now parses each select list's trailing aliases and compares that ordinal against the one the reader's own body passes, with neither number written down.
- **A collector that STOPPED producing read as one that never produced** ([#3819]) - `get_collection_health` had no way to tell a regression from an optional module's resting state: one install took a PostgreSQL collector from 85 % productive to `EXTENSION_MISSING` every cycle on twenty-three of the measured fifty clusters, and every surface called it the Aurora optional-extension class — a legitimate resting state — for a full day. The band agreed, and it is worth being precise about why, because it is not the reason the issue assumes: the classifier's two skip arms are gated on the window holding zero successes, so a regressed collector's seven-day window still holds its productive days, never reaches either arm, and falls through to the staleness ladder where its last productive cycle is hours old and everything reads fine. It banded HEALTHY, not the skip status, until that success aged out of the failing cutoff — and then FAILING, which is loud and says the wrong thing: nothing is failing, something changed what the collector can read. Both per-server reads and both fleet rollups now select `last_non_skip_time` (a NULL status counts as non-skip, deliberately, so a status this build never wrote cannot manufacture a streak) and `last_productive_time`, and with the run time already there those three instants answer the question: a collector is regressed when its newest run postdates its newest non-skip and its newest productive run sits at or before that same instant. All three are plain aggregates, which is what lets the fleet rollup ask at all — a window function would put a sort of the fleet's whole seven-day log in front of a `GROUP BY` on the very statement that had to be memoized because three concurrent copies of it crossed the store role's statement timeout. The band is a FLOOR, never a re-band: WARNING where the ladder said HEALTHY, the ladder's own answer everywhere else, so a regression can only make a row louder and a row that keeps its louder band keeps the attribution that came with it. The fleet card gains `regressed_collector_count`, counted off the PREDICATE rather than the band — keyed on the band it would go quiet exactly when the regression got worse — and it overlaps `failed_collector_count` and must never be added to it. Both MCP tools publish the flag, the last productive instant, the pre-regression row count under a name that claims seven pre-regression days only where the flag is true, and one finding sentence; the three named skip statuses are enumerated from the constants that declare them into the SQL list the reads interpolate, so a rename reaches the store with nobody editing SQL. The Viewer's twin reads take the instants and the floor through the same shared classifier. A skip older than the window leaves no productive row to find and the collector reverts to its ordinary band, which is the honest answer, since at that point the read holds no evidence of a regression.
- **TimescaleDB availability was decided once, by a probe whose failures last a minute** ([#3815]) - `TryEnableAsync` had one call site, the start path, and the block around it force-cleared the availability latch in its catch. The catch is right and the fallback is right — a partially converted store works fine on DELETE-based retention, so degrading to plain-PostgreSQL mode is always safe — but the fallback was PERMANENT on a service designed to run for months, while the conditions that trigger it are minutes long: the store restarting, a failover, a lock held somewhere else, a connection that dropped once. The migrate step directly above it retries with a budget; this one never reconsidered. The flag is not "does the store have TimescaleDB", it is "did one detection attempt succeed", and six later decision points read it as if it were the first thing. The one that matters is the hourly store background-job health check, the only surface that reports a dead compression policy job, a job running past its cadence or a held retention policy — so a store running on a spuriously false latch had that backstop off AND could not say so, because the alert that would say so is behind the same gate. The re-probe now runs on the hourly store-maintenance tick, and the ordering is the whole of the fix: due-time guard, stamp, availability probe, THEN the flag gate and the reads behind it. Above the latch, because inside it the correction is unreachable in exactly the state it exists for — a false value suppresses its own correction, forever, quietly. Above the stamp, because left behind the flag a plain-PostgreSQL store's due time would never move and the probe would fire on every fifteen-second sweep pass; hoisted, it costs one statement an hour on every store shape. It takes a connection of its own, runs under one linked ten-second budget derived from the compression read's phase guard band rather than picked, and is failure-isolated with nothing rethrown into the sweep loop. This is the re-probe, not a re-probe-and-converge: conversion, compression policies, continuous aggregates and retention policies stay on the start path, because putting that sequence on a cadence is another issue's entire subject — and it is not a partial fix, because every consumer reads the flag at call time, so the flip alone restores the job health check, the chunk-drop branch of the daily purge, the per-hypertable self-metrics rows and both provider delegates on their next pass. The recovery is announced at Information naming the three checks that were off and stating that anything the last start left unbuilt stays unbuilt, so an operator is not left believing more was restored than was; the unchanged pass is Debug, and the probe hands down a null logger so a store that is genuinely plain PostgreSQL — a fully supported configuration — is not punished with a line an hour for it.
- **`PG_XMIN_HOLD` grades the horizon's persistence, not one winner's** ([#3691]) - the fact read 0 when several equally-old transactions alternated as the horizon's `is_winner` — the identity arm saw each win once and a set of holders that together pinned the horizon for an hour was invisible. The fact now grades the horizon's PERSISTENCE (the longest run of captures with the oldest xmin above the bar, on the run's floor age) with attribution as a separate arm (distinct holders, the modal holder's share, whether the set is attributable); the advice names the set rather than the latest winner.
- **Store job self-heal covers every policy family, and a held retention policy is never mistaken for a dead one** ([#3816]) - #1581's watch over background jobs read compression and columnstore jobs only, so a `policy_refresh_continuous_aggregate` or `policy_retention` job the scheduler had retired with `next_start = -infinity` was never detected, alerted or re-armed — a rollup could stop materializing silently until the coverage gate held its retention. One read now covers every store-scoped policy job and bands per family: `Refresh Job Stuck` (CRITICAL — readers go quietly stale), `Retention Job Stuck` (WARNING — disk, nothing lost) and `Store Job Failing` (INFO — a job failing and retrying is alive; read the log) beside the untouched `Compression Job Stuck`. A policy the coverage gate paused (`scheduled = false`) is counted as held, never dead, and never handed to `alter_job`; on TimescaleDB 2.26.4 and later a persistent `-infinity` is the scheduler's own crash backoff, so every family is detected and alerted there but not re-armed, as compression has been since #3629. Every hourly pass logs one line — jobs read per family, dead, stuck, held, re-armed, alerted — so a tick can prove it ran.
- **A live store test stops racing the statistics collector** - the TOAST-utilisation live test deleted half a dimension's rows, ran `VACUUM`, and asserted the tuple-share proxy read ~100 % on the vacuumed file; the `DELETE`'s dead-tuple increment sits in the backend's pending statistics until PostgreSQL's once-a-second report while `VACUUM` writes its reset straight to shared statistics, so on a fast runner the zero landed first and the count second and the proxy read 64 %. `pg_stat_force_next_flush()` around the `VACUUM` puts the counters in order. Test-only.
- **A month of zeros is the strongest baseline there is, not the absence of one** ([#3691]) — An all-zero bucket has no dispersion, so it could never be trustworthy, and a server with thirty logged days that never blocked was told "first occurrence, no baseline yet": 92 blocked sessions against a clean month scored 0.5. A bucket that clears its tier's sample and day floors with nothing but zeros is now zero-history: any peak over the magnitude floor fires as an extremity (σ pinned at the cap, `baseline_zero_history = 1`, `baseline_distinct_days` stamped), on both SKUs, and the advice says this hour saw none of it all month. No existing verdict on a non-zero history changed.
- **A configuration lever hanging off a diagnostic chain is attached to that finding instead of orphaned into a card of its own** ([#3691]) — The greedy story walk followed one edge per node, so `CONFIG_PG_MAINT_WORK_MEM` — the memory bound on how fast a vacuum backlog can clear — was never visited, never consumed, and rooted its own card at 0.6 beside the backlog → wraparound → hold chain it belongs to: two cards, neither mentioning the other. Config leaves off any path node now ride on the story as `side_leaves` (both engines' `analyze_server`), with one sentence in the story text naming the lever, and are consumed so they no longer root alone. Path, hash, severity and confidence are untouched — incident identity is byte-identical; the SQL Server graph cannot produce a side leaf and is pinned so.
- **Darling's Entra service-principal connections can find their authentication provider** ([#3838]) - `Microsoft.Data.SqlClient` 7.x split the Active Directory (Entra ID) authentication providers out of the core package into `Microsoft.Data.SqlClient.Extensions.Azure`; Lite picked that package up when the 7.0 bump landed, and Darling then grew non-interactive Entra auth (#3484) and its add-server GUI (#3485) without it. Every service-principal and managed-identity connection the service opened therefore failed at `Open()` with *Cannot find an authentication provider for 'ActiveDirectoryServicePrincipal'* — a runtime failure on a path that compiles clean, which is why it arrived as an operator report against a managed instance rather than as a red build. The driver resolves the provider by the assembly being PRESENT in the running application's dependency closure (measured on 7.0.3: `GetProvider` returns null for all four AAD methods without the package and `ActiveDirectoryAuthenticationProvider` for all four with it), so there is no registration call and no startup wiring — the fix is the reference itself, on the Service (every SQL Server connection the collector opens, and the `test_connect` command the Viewer's Test Connection button enqueues rather than opening itself) and on Darling.Analysis (the plan fetcher opens its own connection on the same resolved string). The Viewer needs nothing: it opens no `SqlConnection` at all. The version was already pinned centrally for Lite, so `Directory.Packages.props` is untouched and the whole product change is two versionless `PackageReference` lines. `EntraProviderPackageCensusTests` now derives the requirement from the tree — every shipped project that assigns `SqlAuthenticationMethod.ActiveDirectory*` declares the package, and every project whose reference closure reaches one of those has it somewhere in its own closure — so a third SKU or a new Entra call site inherits the rule instead of remembering it.
- **Every button in every theme recognizes its access key again** ([#3835]) - `ContentPresenter.RecognizesAccessKey` defaults to FALSE and WPF's stock `Button` template sets it true, so a CUSTOM button template that omits it silently opts every button out of access keys: the marker in `Content` renders as a literal character and the `Alt` combination binds to nothing. That is how `E_dit Collector Schedules...`, `_Save` and `_Close` reached an operator's screen with the underscore visible — with a green build, no warning and no runtime error, because the defect is a rendering fact about a template and nothing executes it. The report counted fifteen templates in three theme files; the tree holds **fifty-five**, across three PARALLEL theme families (`Darling/…Viewer/Themes/`, `Lite/Themes/`, and `deprecated/Dashboard/Themes/`, the last still in the solution) plus five one-off templates outside any of them — including `ViewerDarkTheme.xaml`'s `ViewerButton`, the implicit `Button` style for every secondary window and therefore the template the reported `_Save` and `_Close` actually hit. Forty-two presenters took the attribute; `TabCloseButton` is outside the rule by construction (it renders a literal `×` `TextBlock` and has no presenter), and the `ToggleButton` and calendar templates are deliberately left alone because none of them carries a caption. Turning the attribute on changes how EVERY underscore in every caption is read, so the escaping was censused BEFORE the switch rather than after: all fifty-one distinct underscore-bearing `Button` captions carry exactly one underscore and every one is an intended access key, and the only literal identifiers in this idiom (`SP_SERVER_DIAGNOSTICS` and the three beside it) sit on CheckBoxes, whose template has always set the attribute and whose captions were therefore already doubled in Darling and Lite — the `deprecated/Dashboard` copy was not, and was doubled in the same change so the fix does not leave a known-wrong rendering behind it. `ViewerButtonAccessKeyTests` now derives both halves from the tree, so a fourth theme family or a new button template inherits the rule instead of remembering it, and a caption that grows a `snake_case` identifier reds on the odd-length underscore run instead of quietly losing a character.
- **Store self-alert triage pages stop rendering "Could not resolve server" for every metric added after #2768** ([#3833]) - #2768 fixed the store family's triage pages by naming the four store metrics that existed in `SectionsByMetric`, and the seven store families that landed since (retention hold, custom-rule health, TOAST slack, checkpointer pressure, and the three report digests) each reopened the defect because the fallback for an unmapped metric is the per-server section pair. `SectionsFor` now derives the decision from the metric's `AlertFamily` - the census a new alert cannot ship without joining - with the map kept as an override layer for tailored reads and a three-name exception list for the self-monitor metrics that genuinely fire under a monitored server's name. Resolution titles are covered too: four store families' all-clear rows ("Retention Hold Cleared", "Custom Alert Rules Recovered", "Store TOAST Slack Cleared", "Store Checkpointer Pressure Recovered") had no `ResolutionAliases` entry and would have kept rendering the defect from the history's triage links; they are aliased, the retention title is promoted from an inline literal to a shared constant, and the alias loop's type-initialization guard widens to accept a fleet-level canonical while still failing loudly on a typo. Three census tests pin the rule from `AlertFamily.MetricFamilies`, so the next store metric is in the census the day it registers, and every evaluator resolution-title constant must carry an alias or the suite reds.
- **`get_collection_health` per server no longer races the store's write bands with an unmemoized 7-day scan** ([#3856]) - #3738 single-flighted the fleet rollup's collection-health scan after three racing copies died at the `mcp` role's 15 s `statement_timeout` inside write bands; the per-server twin kept running its own `v_collection_log` scan on every call and was cancelled by that same cap (57014) for the first time on 2026-09-21. The issue preferred deriving the per-server read from the fleet memo's scan and gated that on the windows matching - they do (both cut `now - 7 days`) - but the derivation fails one level deeper: the fleet statement holds twelve aggregates with every per-collector fact summed away, while the per-server read carries twenty-nine columns per collector over four window functions, so there is nothing to filter and widening the fleet scan would spend the headroom #3735 bought. So the fix is the issue's first shape with the window in the key: `PerServerCollectionHealthMemo`, keyed `(server id, window start)`, 60 s lifetime read off the fleet reader's own declaration so one cadence governs both, single-flight primitive copied from the fleet memo rather than re-derived. The payload gains `collection_health_age_seconds` - trailing, unconditional, the field the fleet overview has carried since #3735 - and the tool's instruction row says why repeat calls inside a minute read the memo. Eleven pins mirror the fleet suite: one scan under concurrency, positive age on a memo hit, a failed scan never memoized, a different window missing by key.
- **The typed story path reaches the finding, six readers stop splitting its display string, and the lever beside an incident finally names the read that acts on it** ([#3859] items 4–5) - `AnalysisStory` has always known its chain as a LIST of fact keys; on the way to `AnalysisFinding` that list was joined into a display string and then dropped, so six consumers on both SKUs recovered it by splitting the rendered path back apart on `" → "` — the two `ToolRecommendations.GetForStoryPath` bodies, the three drill-down collectors, and the frozen `deprecated/Dashboard` copy of each. The producer had the typed list the whole time; only the string crossed, and the string was a rendering. That is a corruption surface rather than a style complaint: a fact key that ever contained the arrow, or any edit to the separator in `InferenceEngine.BuildStory`, would have broken all six readers at once — each splitting one key into two halves that match nothing — with a green build and no line in the product saying so. `SameStatementPileupDetector` is the sharper case and was already living it: it arrow-joins the root key to a STATEMENT HASH, which is not a fact key and never was, so every split reader has been treating 16 hex characters as a fact key on every pileup finding and silently matching nothing. `AnalysisFinding.PathKeys` now carries the list, populated at the join in both finding stores beside the `SideLeafKeys` they already copied, and `StoryKeys` pairs the chain with the levers for the consumers that want both, with one rule (`All`) for reading them in one order — chain first, levers after, de-duplicated ordinally, because a recommendation list is read top-down and the incident is what the operator came for, while a lever that is also a hop on another chain must not appear twice as two different suggestions. `PathKeys` is EPHEMERAL, exactly like `SideLeafKeys`: no `analysis_findings` column, none added, no migration rung, and both are write-path lists for write-path consumers (the collectors enrich the pass's own findings; `next_tools` is rendered by `analyze_server`, which renders the pass's own findings), so the persisted, read-back spelling of a chain stays the `StoryPath` string — which does not change by one byte anywhere, on either engine. Item 5 rides the same widening, which is why the issue paired them: with the path alone, `GetForStoryPath` could not see the config leaf #3691 swept onto the story, so a finding whose lever was, say, `CONFIG_PG_MAINT_WORK_MEM` told its reader three times that a lever existed (the `side_leaves` card, the sentence in the root's investigation, the key itself) and then handed over a `next_tools` list pointing at no read for it — `get_pg_autovacuum_health` was reachable only if that key ALSO rooted a finding of its own, which is precisely what #3691 stopped it from doing, so the fix for one gap opened another. Widening the input to `StoryKeys` closes it and MOVES `next_tools` BYTES, deliberately, on every finding that carries a lever: the levers' reads append after the chain's. A finding with no lever is byte-for-byte what it was — nearly all of them, and all of the SQL Server pass except where #3691 swept a lever — so the #3849/#3850-era byte-identity pins hold, which is the evidence that the rendered string and the incident identity did not move. Both call sites sit BESIDE the `StorySideLeaves.Attach` and `FactRanked.Attach` shapes from #3864 and neither attach is restructured: those render the lever's CARD and the root's ranked objects, and this renders the third thing a lever owes a reader, the read that acts on it. Nine test call sites follow the widened input by NAMING their keys instead of rendering a path for the code to re-split — the Darling surface census keeps its identical assertions, which is the evidence the parse was overhead and not behaviour — and the six planted-finding drill-down fixtures gain `PathKeys` beside their `StoryPath`, without which their key tests would match nothing and each test would pass vacuously on a drill-down that never ran.
- **An argument a tool does not declare is refused by name instead of silently dropped, on both SKUs, from one filter** ([#3870]) - `get_collection_log {"server_name":"…","hours":1,"status_filter":"failure"}` returned two hundred rows, every one SUCCESS, at the 24-hour default: `status_filter` does not exist and the real window knob is `hours_back`, so BOTH arguments the caller set were discarded by the SDK's bind-by-name and the tool answered a question nobody asked, with no tell in the payload beyond an `hours_back` echo the caller had no reason to re-read. For a surface whose callers are language models that is the worst available failure shape - the caller believes it asked something narrower than it did and reads the answer under that belief - and the codebase already held the principle one level down, in that same tool, on `limit`: "rejects out of range rather than silently clamping, so a caller asking for 5000 is told no instead of quietly given 1000." A wrong VALUE was refused and a wrong NAME was swallowed, same defect class, and the misspelled name is by far the likelier mistake from an LLM caller. The SDK owns deserialization, so there was nothing to fix inside a tool body - by the time a method runs the unknown key is already gone - which makes the choke point a validation pass over the RAW argument keys against the tool's own advertised input schema, BEFORE dispatch. That is a call-tool filter, the seam `GcfCallToolFilter` already uses and the same "registered once; covers every tool" argument: one `McpUnknownArgumentGuard` in `PerformanceMonitor.Common`, registered by both hosts, so the two SKUs cannot drift into refusing differently and a tool added tomorrow is covered the day it is registered. Nothing in any of the ~240 tool bodies changed.
- **`checkpoint_timeout` says `not_applicable` in the FACT on Aurora, not just at the tool that renders it** ([#3868]) - lane 15 (#3728) stamped `not_applicable` / `not_applicable_on_aurora` on `CONFIG_PG_MAX_WAL_SIZE` alone, so on an `aurora-postgres` target its sibling `CONFIG_PG_CHECKPOINT_TIMEOUT` came out of the collector unqualified: `get_analysis_facts` showed it as an ordinary graded fact and its composed advice told an operator to "tune it with max_wal_size" — a knob Aurora's storage layer ignores, on a fleet where all fifty measured clusters reported sixty timed checkpoints an hour and a requested share of 0. `audit_config` had been made to say the right thing at the surface by #3867's engine-side arm (`isAurora && key == CONFIG_PG_CHECKPOINT_TIMEOUT`), which satisfied rider 1 of Erik's ruling for that one tool and left every other reader of the fact — the pass, the stories, the drill-downs — reading a knob as live that the engine does not consult. The collector now takes the same stamp for the pair, off the same one registry read `max_wal_size` uses (`is_aurora` on the `SERVER_MAJOR_VERSION` fact, through `MonitoredEngineKind`, never a column's presence — #2530), so the fact is the truth and the tools agree with it rather than compensating for it. The value is still emitted, because what a parameter group holds for a knob nothing reads is a fact about the parameter group and worth seeing; `PgTargetScorer.Config`'s existing `not_applicable` read scores it 0 unchanged, which it already did for the stamped sibling, so nothing roots and no co-fire can lift it.
- **FinOps Locking & Contention stops showing databases that were renamed away** ([#3876], user-reported) - the grid and its database selector resolved "latest" PER NAME (`MAX(collection_time) GROUP BY database_name`), which makes every name the store has ever seen its own immortal group: a renamed database's old name still owns the last pre-rename capture forever, so a month after a rename the old names rendered beside the new ones and the dropdown still offered them - while the three sibling tabs, anchored on the server's latest capture or a sliding window, had already moved on (the reporter's own four-tab split was the diagnosis). Rename propagation was never broken: the collector re-derives the name via `DB_NAME()` every pass, and one run stamps all its databases with a single `collection_time`, so the per-name MAX equals the server-wide MAX for every live database - the grouping only ever added dead names. Both reads now anchor on the server's latest capture, the same resolution the working tabs use; capture-time names stay in the store as history. Pinned with the reporter's repro: old-name rows in an older capture, new-name in the newest - grid and selector return only the current name, and scoping to the dead name finds nothing rather than resurrecting pre-rename rows. The Darling edition ports this read verbatim (viewer + both SKUs' MCP object-locking tools); that is a separate issue.
- **Darling's four object-locking reads stop showing databases that were renamed away** ([#3878]) - #3877 fixed Lite's Locking & Contention grid; the Darling edition carried the same read, ported verbatim before the fix, and the audit Erik ordered after that merge found the mechanism at exactly four remaining sites and nowhere else in either SKU: the viewer's `IndexLockingAllSql`, `IndexLockingByDbSql` and `IndexLockingDatabasesSql`, plus `DarlingObjectStatsReader.IndexLockingSql` behind the MCP tool `get_object_locking`. Resolving "latest" as `MAX(collection_time) GROUP BY database_name` makes every name the store has ever seen its own immortal group, so a renamed-away database keeps a group whose newest row is the last capture before the rename - truthfully "the latest row for that name", and therefore returned forever beside the live names as a peer. On the viewer that is the reporter's stale grid; through MCP it is worse, because an agent asking which objects are contended is handed month-dead database names with nothing in the payload marking them dead. All four now anchor on `(SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1)`, the shape `DatabaseSizeLatestSql`, `StorageGrowthSql` and `IndexUsageSql` have always used. The grouping being removed never bought what it looks like it bought: one collector run stamps every database it collects with a single `collection_time`, so for any database present in the newest pass the per-name MAX IS the server-wide MAX - the very same rows - while for one absent from it the grouping adds nothing but a name that is gone. Grid, filtered arm and selector move together deliberately: the selector is how a dead name could still be PICKED rather than merely displayed, and the filtered arm is what a pick lands on, so a partial fix leaves the old name reachable by the act of selecting it. Capture-time names stay in the store untouched - honest history, simply no longer read as the present.
- **The pin that held the defect as intended behavior is renamed, which is why this was its own issue.** `DarlingMcpObjectStatsToolsTests.IndexLockingSql_PerDatabaseLatest_NonzeroWaits_ContendedFirst` asserted per-database "latest" as the read's purpose, by name; rewriting a behavior a test claims as intended is a stated design change rather than a quiet edit, and the rename is the statement. It is now `..._ServerLatestCapture_...`, and its anchor assertion pins the whole server-latest subquery instead of a bare `MAX(collection_time)` - the retired shape contained one of those too, so the old needle could not tell the fix from the defect - with the grouping asserted absent so the pin cannot pass again by going back. `ViewerFinOpsSqlTests`' three literal fragments are re-fragmented to the new SQL with the anchor pinned in all three, the database filter moving to `SqlTextPin` because the fix qualified it to `ios.database_name` when the CTE referencing the bare name went away - #3217's exact case, where an alias appearing in front of a column must not red a pin asserting a read still filters. `McpLatestSnapshotStampTests`' comment claiming this read "takes MAX per database, not one instant" was true when written and is now false; the correction is recorded rather than deleted, because the read IS one instant now, which is precisely what a `captured_at` stamp needs to be truthful and makes the A10 residual it still sits in easier to close. `StorageCommandTimeoutTests` needed no re-anchoring - its fixture pins the `GetIndexLockingAsync` ternary's C# body text and the substitution changed only the const strings that method selects between - verified by reading the method after the edit rather than assumed.
- **A census caught what no pin was watching for, and its roster grew rather than shrank.** `McpPayloadContractCensusTests` rosters latest-anchored reads that do not project their anchor column, shrink-only by design, and the fix ADDED `IndexLockingSql` to it. The reason is the defect restated from another angle: the read was invisible to that census because its anchor sat inside a CTE, which the scan deliberately ignores on the grounds that such an anchor picks a SET rather than a snapshot - exactly true of per-name groups, which picked one row per name the store had ever seen. Becoming a real snapshot read is what made the census see it at all. It is rostered beside its two neighbours with that reasoning written into the roster's own comment, and all three leave together when the A10 lane stamps the family.

### Security

- **Scrubbed legacy plan-force-action audit text** ([#4384]) - a one-time startup pass rewrites audit lines written by older builds to the same safe form the read path already applies; every other field and row is left untouched.
- **Collected PostgreSQL statement text applies the same sensitive-statement filter as the store's own statements** ([#4383]) - matching text is withheld before storage, and a one-time job rewrites rows stored earlier.
- **Stored PostgreSQL setting values are redacted in more shapes, and the one-time scrub resumes a large server-day after a restart** ([#4380]).
- **Plan-force journal reads no longer return pre-#4326 exception text** ([#4363]) - Rows written before #4326 kept a state-read failure's raw exception message in collect.plan_force_actions.detail. Both journal reads now replace that legacy block with a fixed sentence. Post-#4326 evidence lines are returned unchanged. The stored rows are not rewritten.
- **Stopped `/api/ping` and the analysis notes from echoing raw exception text** ([#4326]) - `/api/ping`'s

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

[#1877]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1877
[#1879]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1879
[#1854]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1854
[#1865]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1865
[#1875]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1875
[#1876]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1876
[#1874]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1874
[#1888]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1888
[#1899]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1899
[#1889]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1889
[#1893]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1893
[#1845]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1845
[#1853]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1853
[#1873]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1873
[#1896]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1896
[#1898]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1898
[#1913]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1913
[#1914]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1914
[#1902]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1902
[#1907]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1907
[#1912]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1912
[#1905]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1905
[#1906]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1906
[#1824]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1824
[#1826]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1826
[#1834]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1834
[#1840]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1840
[#1842]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1842
[#1891]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1891
[#1892]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1892
[#1915]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1915
[#1925]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1925
[#1895]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1895
[#1911]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1911
[#1829]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1829
[#1922]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1922
[#1886]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1886
[#1934]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1934
[#1943]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1943
[#1937]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1937
[#1939]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1939
[#1945]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1945
[#1959]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1959
[#1953]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1953
[#1942]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1942
[#1957]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1957
[#1958]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1958
[#1962]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1962
[#1963]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1963
[#1965]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1965
[#110]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/110
[#111]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/111
[#138]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/138
[#145]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/145
[#149]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/149
[#346]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/346
[#372]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/372
[#1663]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1663
[#1673]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1673
[#1685]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1685
[#1721]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1721
[#1754]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1754
[#1755]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1755
[#1940]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1940
[#1921]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1921
[#1923]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1923
[#1954]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1954
[#1970]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1970
[#1972]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1972
[#1973]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1973
[#1955]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1955
[#1980]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1980
[#1949]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1949
[#1952]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1952
[#1951]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1951
[#1960]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1960
[#1822]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1822
[#1990]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1990
[#1988]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1988
[#1984]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1984
[#1743]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1743
[#1606]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1606
[#1996]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1996
[#2000]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2000
[#1074]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1074
[#1966]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1966
[#1995]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1995
[#2007]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2007
[#2008]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2008
[#2020]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2020
[#2027]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2027
[#2028]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2028
[#2029]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2029
[#2005]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2005
[#1804]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1804
[#2030]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2030
[#2031]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2031
[#2033]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2033
[#2038]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2038
[#2011]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2011
[#1986]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1986
[#2012]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2012
[#1568]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1568
[#1981]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1981
[#2022]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2022
[#2054]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2054
[#2058]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2058
[#2060]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2060
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
[#2105]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2105
[#2107]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2107
[#2102]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2102
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
[#2143]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2143
[#2148]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2148
[#2154]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2154
[#2157]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2157
[#2164]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2164
[#2210]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2210
[#2188]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2188
[#2191]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2191
[#2171]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2171
[#2170]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2170
[#2169]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2169
[#2166]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2166
[#2167]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2167
[#2213]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2213
[#2216]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2216
[#2138]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2138
[#2190]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2190
[#2186]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2186
[#2185]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2185
[#2258]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2258
[#2219]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2219
[#2280]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2280
[#2277]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2277
[#2279]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2279
[#2273]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2273
[#2220]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2220
[#2228]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2228
[#2218]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2218
[#2235]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2235
[#2165]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2165
[#2255]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2255
[#2159]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2159
[#2197]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2197
[#2203]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2203
[#2187]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2187
[#2189]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2189
[#2184]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2184
[#2254]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2254
[#2252]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2252
[#2256]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2256
[#2158]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2158
[#2246]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2246
[#2319]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2319
[#2340]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2340
[#2344]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2344
[#2349]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2349
[#2357]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2357
[#2361]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2361
[#2362]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2362
[#2364]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2364
[#2347]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2347
[#2359]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2359
[#2348]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2348
[#2350]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2350
[#2353]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2353
[#2352]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2352
[#2331]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2331
[#2181]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2181
[#2317]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2317
[#2320]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2320
[#2339]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2339
[#2316]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2316
[#2324]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2324
[#2300]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2300
[#2312]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2312
[#2306]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2306
[#2302]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2302
[#2501]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2501
[#2499]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2499
[#2495]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2495
[#2506]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2506
[#2511]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2511
[#2512]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2512
[#2515]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2515
[#2520]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2520
[#2525]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2525
[#2530]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2530
[#2539]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2539
[#2540]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2540
[#2508]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2508
[#2541]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2541
[#2542]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2542
[#2532]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2532
[#2518]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2518
[#2533]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2533
[#2535]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2535
[#2480]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2480
[#2489]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2489
[#2481]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2481
[#2437]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2437
[#1563]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1563
[#2475]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2475
[#2422]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2422
[#2460]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2460
[#2459]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2459
[#2446]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2446
[#2296]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2296
[#2299]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2299
[#2294]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2294
[#2298]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2298
[#2293]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2293
[#2233]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2233
[#2234]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2234
[#2150]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2150
[#2484]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2484
[#2485]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2485
[#2524]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2524
[#2201]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2201
[#2205]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2205
[#2195]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2195
[#2548]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2548
[#2554]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2554
[#2546]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2546
[#2552]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2552
[#2529]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2529
[#2569]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2569
[#2572]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2572
[#2574]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2574
[#2579]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2579
[#2559]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2559
[#2562]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2562
[#2550]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2550
[#2578]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2578
[#2564]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2564
[#2653]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2653
[#2655]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2655
[#2658]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2658
[#2659]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2659
[#2661]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2661
[#2663]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2663
[#2665]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2665
[#2671]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2671
[#2674]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2674
[#2675]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2675
[#2677]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2677
[#2680]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2680
[#2681]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2681
[#2683]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2683
[#2685]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2685
[#2690]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2690
[#2673]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2673
[#2687]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2687
[#2688]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2688
[#2691]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2691
[#2695]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2695
[#2692]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2692
[#2693]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2693
[#2694]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2694
[#2699]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2699
[#2715]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2715
[#2710]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2710
[#2711]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2711
[#2719]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2719
[#2733]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2733
[#2735]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2735
[#2736]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2736
[#2732]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2732
[#2737]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2737
[#2734]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2734
[#2759]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2759
[#2749]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2749
[#2764]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2764
[#2766]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2766
[#2785]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2785
[#2791]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2791
[#2795]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2795
[#2794]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2794
[#2473]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2473
[#2792]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2792
[#1562]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1562
[#2800]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2800
[#2801]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2801
[#2803]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2803
[#2804]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2804
[#2779]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2779
[#2784]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2784
[#2786]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2786
[#2284]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2284
[#2776]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2776
[#2772]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2772
[#2773]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2773
[#2748]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2748
[#2771]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2771
[#2761]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2761
[#2465]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2465
[#2770]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2770
[#2768]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2768
[#2780]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2780
[#2781]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2781
[#2757]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2757
[#2755]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2755
[#2753]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2753
[#1687]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1687
[#2811]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2811
[#2816]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2816
[#2796]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2796
[#2809]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2809
[#2810]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2810
[#2813]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2813
[#2819]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2819
[#2822]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2822
[#2823]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2823
[#2826]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2826
[#2827]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2827
[#2839]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2839
[#2840]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2840
[#2846]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2846
[#2841]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2841
[#2845]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2845
[#2851]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2851
[#2854]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2854
[#2859]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2859
[#2855]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2855
[#2860]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2860
[#2871]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2871
[#2828]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2828
[#2837]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2837
[#2870]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2870
[#2884]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2884
[#2864]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2864
[#2874]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2874
[#2876]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2876
[#2797]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2797
[#2490]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2490
[#2898]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2898
[#2890]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2890
[#2936]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2936
[#2935]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2935
[#2882]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2882
[#2888]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2888
[#2917]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2917
[#2918]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2918
[#2907]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2907
[#2924]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2924
[#2910]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2910
[#2896]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2896
[#2901]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2901
[#2902]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2902
[#2894]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2894
[#1696]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1696
[#1944]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1944
[#2018]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2018
[#2026]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2026
[#2061]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2061
[#2266]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2266
[#2538]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2538
[#2543]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2543
[#2544]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2544
[#2545]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2545
[#2547]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2547
[#2561]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2561
[#2565]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2565
[#2566]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2566
[#2567]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2567
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
[#2686]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2686
[#2689]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2689
[#2760]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2760
[#2847]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2847
[#2913]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2913
[#2928]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2928
[#2932]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2932
[#2923]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2923
[#2920]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2920
[#2926]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2926
[#2951]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2951
[#2952]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2952
[#2931]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2931
[#2927]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2927
[#2929]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2929
[#2933]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2933
[#2937]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2937
[#2938]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2938
[#2940]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2940
[#2942]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2942
[#2953]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2953
[#2960]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2960
[#2964]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2964
[#2966]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2966
[#2967]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2967
[#2972]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2972
[#2973]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2973
[#2880]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2880
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
[#3112]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3112
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
[#3288]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3288
[#3281]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3281
[#3278]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3278
[#3285]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3285
[#3307]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3307
[#3302]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3302
[#3316]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3316
[#3315]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3315
[#3313]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3313
[#3282]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3282
[#3314]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3314
[#3330]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3330
[#3297]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3297
[#3348]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3348
[#3343]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3343
[#3306]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3306
[#3355]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3355
[#3354]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3354
[#3287]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3287
[#3368]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3368
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
[#3446]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3446
[#3493]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3493
[#3496]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3496
[#3499]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3499
[#3906]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3906
[#3908]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3908
[#3909]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3909
[#3918]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3918
[#3919]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3919
[#3886]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3886
[#3889]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3889
[#3900]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3900
[#3911]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3911
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
[#4136]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4136
[#4137]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4137
[#4141]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4141
[#4157]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4157
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
[#4429]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4429
[#4430]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4430
[#4431]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4431
[#4432]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4432
[#4433]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4433
[#4434]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4434
[#4435]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4435
[#4436]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4436
[#4437]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4437
