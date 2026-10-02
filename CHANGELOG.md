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
- **pg_plan_capture reads a self-hosted target's csvlog file** ([#4137])
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
[#4426]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4426
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
[#4809]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/4809
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
