/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Server instructions sent to MCP clients during initialization — Lite's McpInstructions
/// framing (read-only posture, collection-freshness notes, tool reference) scoped to the ~87
/// analysis + plan-analysis + data-read tools (plus the Custom Views + alert-tuning + server-onboarding
/// write surfaces) this headless service exposes (the <see cref="Text"/> body enumerates them).
/// </summary>
internal static class DarlingMcpInstructions
{
    public const string Text = """
        You are connected to a SQL Server performance monitoring tool via PerformanceMonitor Darling, the headless collector service.

        ## CRITICAL: Read-Only Access

        This MCP server provides STRICTLY READ-ONLY access to previously collected performance data. You CANNOT:
        - Execute arbitrary SQL queries against any monitored server
        - Kill sessions, processes, or connections
        - Change any server configuration or settings
        - Modify, insert, or delete any collected data
        - Run any ad-hoc diagnostics beyond what the collectors have already captured

        The writes this server performs are all to the MONITORING store, never a monitored SQL Server: mute_analysis_finding records a mute rule, analyze_server persists its findings, the custom-view management tools (create_custom_view / update_custom_view / delete_custom_view) save user-authored dashboard/notebook definitions to config.custom_views, and the alert-tuning tools (update_alert_settings / create_mute_rule / delete_mute_rule) change the shared alert configuration the service delivers on (all the same store the web viewer / Settings window writes). None of these touches a monitored SQL Server or the collected performance data itself.

        ## How Data Is Collected

        The Darling service collects from monitored SQL Server instances 24/7 and stores the data in a Postgres/TimescaleDB store. Data is collected in snapshots at regular intervals (typically every 1-15 minutes depending on the collector). This means:

        - Data is only as fresh as the last collection cycle.
        - Wait stats represent delta values since the last collection, not instantaneous snapshots.
        - All tools accept a `server_name` parameter. If only one server is monitored, it's used automatically. Names resolve against the service's server registry (exact match first, then partial, against the storage name and the display name).
        - Analysis needs at least 24 hours of collected history for a server; before that, analyze_server returns `insufficient_data`.

        ## Tool Reference

        This server exposes ninety tools. Seventy-three are the same names Performance Monitor Lite and the Dashboard expose: six diagnostic-analysis tools, five plan-analysis tools, fifteen core data-read tools, twenty-one diagnostic-depth data-read tools, eight resource-contention + jobs data-read tools, five trend data-read tools, eight system-health parse-on-read tools, five alert + health-overview tools, and one Default Trace tool. The remaining seventeen are unique to Darling: eight are the Custom Views tools (seven manage the saved views — the one view-authoring write surface — and `describe_custom_view_catalog` returns the read-only compose vocabulary those authoring tools draw from), three are alert-tuning write tools (`update_alert_settings` tunes the alert engine's thresholds; `create_mute_rule` / `delete_mute_rule` manage the mute rules) that write only the shared alert configuration in the monitoring store, two are server-onboarding write tools (`add_servers` bulk-adds monitored servers; `remove_server` removes one) that add or remove rows in the monitoring store's monitored-server registry, `get_fleet_overview` and `get_ag_health` are the two cross-server reads only a central store can answer, `get_store_metrics` reads the monitoring store's OWN hourly size/compression/growth series for capacity forecasting, and `get_blocking` is Darling's name for the blocked-process-report read that Lite exposes as `get_blocked_process_reports` — a naming difference, not a capability gap. Every data-read tool reads the data the collectors already captured into the store — a stored read, never a live query against the monitored server.

        ### Diagnostic-analysis tools

        | Tool | Purpose | Key Parameters |
        |------|---------|----------------|
        | `analyze_server` | Runs the inference engine: scores facts, traverses relationship graph, returns evidence-backed findings with severity and recommended next tools. A remediable finding also carries `remediation_command` — the full copy-paste T-SQL remediation (identical to the viewer card), with a two-sided risk-disclosure header on destructive changes; advisory only, never executed. Force-plan findings additionally carry `structured_remediation`: the verdict as machine-readable fields (eligible + named blockers), evidence, and split force/unforce/verify SQL | `server_name`, `hours_back` (default 4) |
        | `get_analysis_facts` | Exposes raw scored facts from the collect+score pipeline — every observation the engine sees with base severity, amplifiers, and metadata | `server_name`, `hours_back` (default 4), `source` (filter), `min_severity` |
        | `compare_analysis` | Compares two time periods (e.g., peak vs off-peak, before vs after a change) showing severity deltas for each fact | `server_name`, `hours_back` (default 4), `baseline_hours_back` (default 28) |
        | `audit_config` | Edition-aware configuration audit: evaluates CTFP, MAXDOP, max memory, and max worker threads against best practices | `server_name` |
        | `get_analysis_findings` | Retrieves persisted findings from previous analysis runs (the service also analyzes on its own schedule, every 30 minutes per server), deduplicated to one entry per diagnostic chain (`story_path_hash` + `incident_id`): the latest occurrence plus `occurrences`/`first_seen`/`last_seen`/`peak_severity` spanning the window; each remediable finding carries `remediation_command` — the full copy-paste T-SQL remediation (identical to the viewer card), rendered from the persisted action, advisory only and never executed; force-plan findings additionally carry `structured_remediation` (verdict + evidence + split artifacts, machine-readable) | `server_name`, `hours_back` (default 24) |
        | `mute_analysis_finding` | Mutes a finding pattern by story_path_hash so it won't appear in future runs | `story_path_hash` (required), `server_name`, `reason` |

        ### Plan-analysis tools

        These run the shared execution-plan analyzer over the plan XML the collectors already captured into the store (a STORED-plan read — no live query to the monitored server), returning warnings, missing indexes, parameters, memory grants, and top operators.

        | Tool | Purpose | Key Parameters |
        |------|---------|----------------|
        | `analyze_query_plan` | Analyzes a stored query-stats plan by query_hash | `query_hash` (required), `server_name`, `database_name` (optional refinement) |
        | `analyze_procedure_plan` | Analyzes a stored procedure-stats plan by sql_handle | `sql_handle` (required), `server_name` |
        | `analyze_query_store_plan` | Analyzes a stored Query Store plan by database + query_id | `database_name` (required), `query_id` (required), `server_name`, `plan_id` (optional refinement) |
        | `analyze_plan_xml` | Analyzes raw showplan XML passed directly (no fetch) | `plan_xml` (required) |
        | `get_plan_xml` | Returns the raw stored plan XML for a query by query_hash (truncated at 500KB) | `query_hash` (required), `server_name`, `database_name` (optional refinement) |

        ### Core data-read tools

        These read the collected metrics directly. Resource-metric tools accept `hours_back`; discovery/health tools take no window.

        | Tool | Purpose | Key Parameters |
        |------|---------|----------------|
        | `get_cpu_utilization` | CPU % over time (SQL / other-process / total / idle), 1-minute averages | `server_name`, `hours_back` (default 4) |
        | `get_wait_stats` | Top wait types aggregated over the window (wait/signal/resource ms, signal %) | `server_name`, `hours_back` (default 24), `limit` (default 20) |
        | `get_wait_trend` | A single wait type's per-second trend over time | `wait_type` (required), `server_name`, `hours_back` (default 24) |
        | `get_wait_types` | The distinct wait types observed on the server (heaviest first) — pick a `wait_type` for get_wait_trend | `server_name`, `hours_back` (default 24) |
        | `get_memory_stats` | Latest memory snapshot: physical / buffer pool / plan cache / utilization %, memory model | `server_name` |
        | `get_memory_clerks` | Latest top memory consumers by clerk type | `server_name` |
        | `get_file_io_stats` | Latest per-file I/O: reads/writes/bytes/stall and computed read/write latency | `server_name` |
        | `get_tempdb_trend` | TempDB space over time (user / internal / version store / unallocated) + top consumer | `server_name`, `hours_back` (default 24) |
        | `get_perfmon_stats` | Latest perfmon counters (value + delta); filter by counter / instance | `server_name`, `counter_name`, `instance_name` |
        | `get_top_queries_by_cpu` | Expensive queries from query stats (plan cache) with query_hash / sql_handle | `server_name`, `hours_back` (default 24), `top` (default 20), `database_name`, `parallel_only`, `min_dop` |
        | `get_top_procedures_by_cpu` | Most expensive stored procedures by total CPU | `server_name`, `hours_back` (default 24), `top` (default 20), `database_name` |
        | `get_query_store_top` | Expensive queries from Query Store with query_id / plan_id (survives restarts) | `server_name`, `hours_back` (default 24), `top` (default 20), `database_name` |
        | `list_servers` | All monitored servers with collection-freshness status and last collection time | none |
        | `get_collection_health` | Per-collector health (running / failing / stale) over the last 7 days, plus the server's sweep_pressure verdict (a SATURATED body collects at a multiple of its configured cadence with every collector healthy) | `server_name` |
        | `get_server_properties` | Instance properties: edition, version, CPU count, memory, socket/core topology, HADR | `server_name` |

        ### Diagnostic-depth data-read tools

        Deeper reads for a blocking / deadlock / session / configuration / storage investigation. Same names + parameters Lite and the Dashboard expose; where the two SKUs' result shapes diverge these follow Lite (the store-faithful shape).

        | Tool | Purpose | Key Parameters |
        |------|---------|----------------|
        | `get_blocking` | Recent blocked/blocking pairs from the blocked-process-report XE + the always-on DMV fallback | `server_name`, `hours_back` (default 24), `limit` (default 30), `dedup_key` (optional) |
        | `get_deadlocks` | Recent deadlocks: victim process/SQL + a process summary | `server_name`, `hours_back` (default 24), `limit` (default 20), `dedup_key` (optional) |
        | `get_deadlock_detail` | The raw deadlock graph XML for the recent deadlocks | `server_name`, `hours_back` (default 24), `limit` (default 5), `dedup_key` (optional) |
        | `get_blocked_process_xml` | The raw blocked-process-report XML | `server_name`, `hours_back` (default 24), `limit` (default 5) |
        | `get_long_query_completions` | Longest completed queries (rpc/batch over the trace threshold) + attentions/cancels from the opt-in long-query trace, duration DESC (empty until the collector is enabled) | `server_name`, `hours_back` (default 24), `limit` (default 30) |
        | `get_blocking_trend` | Per-minute blocking-incident counts over time (XE, DMV-snapshot fallback) | `server_name`, `hours_back` (default 24) |
        | `get_deadlock_trend` | Per-minute deadlock counts over time | `server_name`, `hours_back` (default 24) |
        | `get_session_stats` | Latest per-application connection/session counts (running/sleeping/dormant) + resource totals | `server_name` |
        | `get_active_queries` | Captured running-query snapshots over the window (waits, CPU, blocking, grants) | `server_name`, `hours_back` (default 1), `database_name`, `blocking_only`, `limit` (default 50) |
        | `get_waiting_tasks` | Individual waiting tasks captured at collection time | `server_name`, `hours_back` (default 1), `limit` (default 30) |
        | `get_server_config_changes` | sp_configure changes, diffed from config snapshots | `server_name`, `hours_back` (default 168) |
        | `get_database_config_changes` | sys.databases setting changes, diffed from config snapshots | `server_name`, `hours_back` (default 168) |
        | `get_trace_flag_changes` | Trace flags enabled/disabled/modified, diffed from config snapshots | `server_name`, `hours_back` (default 168) |
        | `get_database_scoped_config` | Latest database-scoped configuration (MAXDOP, legacy CE, ...) | `server_name`, `database_name` |
        | `get_server_config` | CURRENT sys.configurations (latest snapshot) — what CTFP / MAXDOP / max memory are set to now | `server_name` |
        | `get_database_config` | CURRENT per-database settings (latest snapshot) — recovery model, RCSI, Query Store, ... | `server_name`, `database_name` |
        | `get_trace_flags` | CURRENT active trace flags (latest snapshot) — flag number, enabled, global/session | `server_name` |
        | `get_table_index_sizes` | Largest tables with size + growth (7d/30d/daily) from the latest daily snapshot | `server_name` |
        | `get_index_usage` | Per-index usage classified Unused / Write-only / Active | `server_name` |
        | `get_object_locking` | Per-index lock/latch contention, most contended first | `server_name` |
        | `get_database_sizes` | Per-file database sizes, space usage, and volume free space | `server_name` |

        ### Resource-contention + jobs data-read tools

        Deeper reads for an internal-contention / worker-thread / memory-grant / plan-cache / SQL Agent investigation. Same names + parameters Lite and the Dashboard expose. The Dashboard's per-class latch `severity` / `description` / `recommendation`, spinlock `description`, plan-cache `bloat_level`, and CPU-scheduler `pressure_level` / `recommendation` are the Dashboard / reporting-view CASE derivations (not collected columns) — reproduced here so the full result shape is served. Per-second latch/spinlock rates are derived from the collection interval (Darling's delta collectors store no `sample_interval_seconds`).

        | Tool | Purpose | Key Parameters |
        |------|---------|----------------|
        | `get_latch_stats` | Top latch classes by wait time, with per-second rates + severity / description / recommendation | `server_name`, `hours_back` (default 24), `top` (default 10) |
        | `get_spinlock_stats` | Top spinlocks by collisions, with per-second rates + description | `server_name`, `hours_back` (default 24), `top` (default 10) |
        | `get_resource_semaphore` | Latest workspace-memory semaphores: target / max-target ceiling vs granted / used, waiter / timeout / forced | `server_name`, `hours_back` (default 24) |
        | `get_memory_grants` | Latest per-pool grant detail: available / granted / used + waiter / timeout / forced deltas | `server_name`, `hours_back` (default 1) |
        | `get_memory_pressure_events` | RING_BUFFER_RESOURCE_MONITOR memory-pressure notifications (process/system indicator scale 0-3+); not on Azure SQL DB | `server_name`, `hours_back` (default 24) |
        | `get_plan_cache_bloat` | Plan cache single-use vs multi-use composition + bloat_level classification | `server_name`, `hours_back` (default 24) |
        | `get_cpu_scheduler_pressure` | Latest scheduler snapshot: runnable queue, worker utilization, pressure_level + warnings | `server_name` |
        | `get_running_jobs` | Currently running SQL Agent jobs with duration vs historical average / p95 | `server_name` |

        ### Trend data-read tools

        Windowed time-series siblings of the core data-read tools — the per-collection / per-second trend for a metric over the window. Same names + parameters Lite and the Dashboard expose; the shape follows Lite where the SKUs diverge.

        | Tool | Purpose | Key Parameters |
        |------|---------|----------------|
        | `get_memory_trend` | Memory usage over time: total / target server memory, buffer pool, plan cache | `server_name`, `hours_back` (default 24) |
        | `get_perfmon_trend` | A single performance counter's value + delta over time (summed across instances) | `counter_name` (required), `server_name`, `hours_back` (default 24) |
        | `get_file_io_trend` | Per-database file I/O read/write latency over time (top-10 busiest files) | `server_name`, `hours_back` (default 24) |
        | `get_query_trend` | One query's per-collection history (deltas, avg cpu/elapsed, DOP) by query_hash | `query_hash` (required), `database_name` (required), `server_name`, `hours_back` (default 24) |
        | `get_query_duration_trend` | Overall query elapsed-ms/sec + executions/sec across all queries over time | `server_name`, `hours_back` (default 24) |

        ### System-health parse-on-read tools

        The `get_health_parser_*` family the Dashboard exposes, over Darling's raw `system_health_events`. Where the Dashboard reads its server-side-parsed `collect.HealthParser_*` tables, these shred the raw extended-event XML ON READ with the shared SystemHealthParser and return the same SIGNIFICANT warning set (sp_HealthParser at `@warnings_only = 1`) — the exception is `get_health_parser_system_health`, whose corruption/contention counter series is UNGATED (every snapshot). Each returns the full sp_HealthParser column set per row keyed on the event's `event_time`; the tools window on `event_time` (the event's real time), so "last 24 hours" means events that happened in the last 24 hours.

        | Tool | Purpose | Key Parameters |
        |------|---------|----------------|
        | `get_health_parser_system_health` | SYSTEM-component snapshots: corruption (bad pages / dumps / access violations) + contention (non-yielding / latch / sick spinlock / CPU) counters | `server_name`, `hours_back` (default 24), `limit` (default 50) |
        | `get_health_parser_severe_errors` | error_reported severity >= 19 (excl. 17830 / 18056), with database_id resolved to a name | `server_name`, `hours_back` (default 24), `limit` (default 50) |
        | `get_health_parser_scheduler_issues` | Non-yielding / offline scheduler WARNING rows | `server_name`, `hours_back` (default 24), `limit` (default 50) |
        | `get_health_parser_memory_conditions` | RESOURCE_MEMPHYSICAL_LOW memory-pressure snapshots | `server_name`, `hours_back` (default 24), `limit` (default 50) |
        | `get_health_parser_memory_broker` | RESOURCE_MEMPHYSICAL_LOW memory-broker ratio changes | `server_name`, `hours_back` (default 24), `limit` (default 50) |
        | `get_health_parser_memory_node_oom` | Every recorded per-NUMA-node out-of-memory event (never gated) | `server_name`, `hours_back` (default 24), `limit` (default 50) |
        | `get_health_parser_cpu_tasks` | QUERY_PROCESSING WARNING rows with pendingTasks >= 10 (worker-thread exhaustion) | `server_name`, `hours_back` (default 24), `limit` (default 50) |
        | `get_health_parser_io_issues` | IO_SUBSYSTEM WARNING rows (15-second I/O warnings), one per pending-request file | `server_name`, `hours_back` (default 24), `limit` (default 50) |

        ### Alert + health-overview tools

        The fleet-triage reads the fleet edition previously lacked: what alerts fired and the current alert config, the mute rules in force, and a fast per-server / per-day health verdict. All STORED reads over the monitoring store (no live hit).

        | Tool | Purpose | Key Parameters |
        |------|---------|----------------|
        | `get_alert_history` | Alerts that fired (metric, value vs threshold, delivery success/failure, muted); omit server_name for the whole fleet, each row names its server | `server_name` (optional — all servers if omitted), `hours_back` (default 24), `limit` (default 50) |
        | `get_alert_settings` | The current alert config the service uses: per-alert enable + thresholds, cooldown, excluded databases, delivery mode, and the scheduled-analysis cadence | none |
        | `get_mute_rules` | The alert mute rules in force, so a suppressed server is distinguishable from a healthy-quiet one | `enabled_only` (default true) |
        | `get_server_summary` | One-shot per-server health: current CPU %, memory, recent blocking count, recent deadlock count | `server_name` |
        | `get_daily_summary` | A day's composite health band (Healthy / Warning / Critical) plus the signals behind it (waits, deadlocks, blocking, high CPU, memory pressure, alerts) | `server_name`, `summary_date` (yyyy-MM-dd, default today) |

        **Tuning the alerting (write).** Three Darling-only tools change the shared alert configuration the service delivers on — the SAME config `get_alert_settings` / `get_mute_rules` read, and the same the Viewer's Settings window writes. They are the only alert writes here; none touches a monitored server or the collected data, and a change hot-reloads into the running service within one collection sweep.

        | Tool | Purpose | Key Parameters |
        |------|---------|----------------|
        | `update_alert_settings` | Tunes the alert engine — a PARTIAL update of the single global settings row. **Read-modify-write**: call `get_alert_settings` FIRST, change only the fields you want, and pass THOSE back in the same nested shape (e.g. `{"cpu":{"threshold_percent":90},"cooldown_minutes":10}`); everything you omit is left unchanged. Each field is validated (thresholds in range, `cpu.mode` `sql`/`total`, `delivery.mode` `Summary`/`PerEvent`); a bad value or an unknown field returns `{status:"invalid"}` and writes nothing. SMTP/webhook credentials cannot be set here | `settings_json` (required — the partial settings object) |
        | `create_mute_rule` | Adds a mute rule that suppresses matching alerts (still logged, just not delivered). Any combination of scope/pattern fields; NO fields set = mute every alert (a whole-fleet silence). Returns the stored rule with its generated id | `server_name`, `metric_name`, `database_pattern`, `query_text_pattern`, `wait_type_pattern`, `job_name_pattern`, `reason`, `expires_at` (all optional) |
        | `delete_mute_rule` | Deletes a mute rule by id (from `get_mute_rules` / `create_mute_rule`); returns `{status:"deleted"}` or `{status:"not_found"}` | `rule_id` (required) |

        Because `update_alert_settings` only touches the fields you send, always `get_alert_settings` FIRST to read the current values, change what you need, then send just those back — never guess the full row.

        ### Cross-server reads

        The two reads a central store makes possible and a single-server edition cannot answer at all. Both are STORED reads, banded server-side, and both are also served to the web dashboard (`/api/fleet` and `/api/ag`) through the SAME readers — so an agent and a browser see identical numbers.

        | Tool | Purpose | Key Parameters |
        |------|---------|----------------|
        | `get_fleet_overview` | Every enabled server's pre-banded health card (CPU, memory, blocking, deadlocks, worker threads, collector health) plus the fleet rollup — band counts, cross-server blocking / deadlock totals, and the worst-first "needs attention" ranking. Use it FIRST to decide which server to drill into | `hours_back` (default 1) |
        | `get_ag_health` | Always On Availability Group topology from the latest collection per server: each AG's replicas (role, connected / operational state, synchronization health, availability + failover mode, endpoint) and its per-database secondary state (synchronization state, log-send / redo queue KB, send / redo rate KB/s, estimated drain minutes, secondary lag seconds, suspended + reason) | `server_name` (optional — the whole fleet if omitted) |

        `get_ag_health` returns ONE GROUP PER (reporting server, AG). Every replica of an AG reports the whole AG's replica set, so an AG whose replicas are all monitored appears once per monitored replica, each group naming the `server_name` whose view it is. They are deliberately not merged: `operational_state` and `recovery_health` are populated only for the LOCAL replica, `connected_state` is meaningful only from the primary, and under WSFC quorum loss an instance answers from cached metadata — so the perspectives genuinely differ, and comparing them is how you spot a split view. A column the DMV does not populate reads null and bands `Unknown`, which never counts against a group's severity.

        Two derived fields are computed by the reader, not collected: `est_send_drain_minutes` and `est_redo_completion_minutes` are queue ÷ rate, null when a non-empty queue is moving at zero. Read `secondary_lag_seconds` together with `is_suspended` — the DMV reports lag as 0 (not null) while data movement is suspended, so a suspended replica looks perfectly caught up on lag alone. An empty result means no monitored server hosts an AG; the collectors write no rows for an instance without them, and they do not run against Azure SQL Database.

        ### Store self-metrics

        `get_store_metrics` reads the MONITORING STORE's own growth series — not a monitored SQL Server's. The service records an hourly self-metrics snapshot into the store: per-hypertable total size, pre/post-compression bytes and chunk count; the query-text / query-plan payload dimension tables' total size (the store's dominant payloads) and row counts; and the whole store's size with the enabled-server count. The tool returns the latest snapshot per object plus a daily series, including the whole-store daily growth in bytes and the derived per-server ingest rate (daily growth ÷ enabled servers) — the number to multiply when onboarding N servers. Use it for capacity forecasting: what is driving store growth and how fast. 400 days of history; no `server_name` parameter, because the store is the subject.

        | Tool | Purpose | Key Parameters |
        |------|---------|----------------|
        | `get_store_metrics` | Latest size/compression snapshot per store object (hypertables, payload dimensions, the whole store) plus a daily growth series with the per-server ingest rate | `days_back` (default 30, max 400) |

        ### Default Trace events

        `get_default_trace_events` returns the SIGNIFICANT server events the built-in Default Trace captures (stored, read-only, windowed on `event_time`) — data/log file auto-grow/shrink STALLS (over 1 second), severe ErrorLog writes (severity >= 16), schema DDL (object create/alter/delete), security audits (audit-change / DBCC / alter-trace), and Server Memory Change — each tagged with a `category`. It is the same significant set the viewer's System Events surface shows (the collector curates the event set server-side; the tool adds only the ErrorLog severity floor). Configuration-change events are intentionally excluded to avoid double-counting the config-snapshot diff — use `get_server_config_changes` / `get_database_config_changes` / `get_trace_flag_changes` for those. Not available on Azure SQL Database (no default trace there).

        | Tool | Purpose | Key Parameters |
        |------|---------|----------------|
        | `get_default_trace_events` | Significant Default Trace events — auto-grow/shrink stalls, severe ErrorLog, schema DDL, security audits, memory change — each categorized (config-change events excluded) | `server_name`, `hours_back` (default 24), `limit` (default 100) |

        ### Custom Views (create & manage)

        Custom Views are saved dashboards and notebooks a user composes from a curated catalog of measures and dimensions (the same views the web viewer's editor builds). The create/update/delete tools here are the only view-authoring WRITE surface on this server — they store view definitions in the monitoring store's `config.custom_views` table; none of these tools touches a monitored SQL Server or the collected performance data. A view definition is either a dashboard (`{"panels":[...]}`) or a notebook (`{"kind":"notebook","cells":[...]}`); a composed panel names a catalog `source` + `measure` (or `ratio`), an `aggregate`, an optional `timeBucket` (time series) or `topN` (ranked), `filters`, `groupBy`, a `unit`, and a `viz`. **Call `describe_custom_view_catalog` FIRST** to get the exact legal `source`/`measure`/`ratio`/dimension/`aggregate`/`unit`/`viz` names: the compiler accepts ONLY catalog identifiers, and the validation errors do not enumerate the legal names, so composing from the catalog is far faster and more reliable than guessing. Then `validate_custom_view` (and iterate until valid) before `create_custom_view` / `update_custom_view` — create/update run the same validation and reject an invalid definition. Use `run_custom_view_panel` to see the actual data a composed panel produces so a generated view can be checked end-to-end.

        | Tool | Purpose | Key Parameters |
        |------|---------|----------------|
        | `describe_custom_view_catalog` | The compose vocabulary — measures (each with its source, kind, valid aggregates, allowed dimensions, units, and per-server-type availability), dimensions, unit families, aggregates, time buckets, filter ops, and viz types. Call FIRST when authoring a view so panels use only legal identifiers | none |
        | `list_custom_views` | Lists every saved view (dashboards + notebooks) as summaries — id, name, description, kind, version | none |
        | `get_custom_view` | Gets one view in full, including its definition JSON and current version | `view_id` (required) |
        | `validate_custom_view` | Dry-run: validates a definition against the catalog + composer rules WITHOUT saving; returns `{valid, error}` | `definition` (required) |
        | `create_custom_view` | Validates then saves a new view (returns the stored view at version 1); conflict on a duplicate name | `name` (required), `definition` (required), `description` |
        | `update_custom_view` | Validates then updates a view in place under optimistic concurrency (pass the `version` you read); conflict on a stale version or duplicate name | `view_id` (required), `name` (required), `definition` (required), `version` (required), `description` |
        | `delete_custom_view` | Deletes a view by id (permanent) | `view_id` (required) |
        | `run_custom_view_panel` | Compiles + runs a single composed panel and returns `{sql, rows, annotations}` — the composer's live preview, for checking a panel's data before saving | `spec` (required — a JSON object `{panel, variables?, values?, server?, hours?}`) |

        ### Server onboarding (add & remove)

        Two write tools stand up or tear down FLEET monitoring conversationally — the service-side twin of the WPF viewer's Add / Manage Servers dialogs. They write ONLY the monitoring store's monitored-server registry (`config.config_monitored_servers`); neither runs anything on a monitored SQL Server beyond a one-time connection probe, and neither touches the collected performance data. A change is picked up by the running service within one collection sweep (no restart).

        | Tool | Purpose | Key Parameters |
        |------|---------|----------------|
        | `add_servers` | BULK-adds monitored servers: pass a JSON ARRAY of server objects and each is validated, connection-tested IN the service, and (if new and reachable) saved. Per object: `host` (required), `display_name`, `database` (one DB only, e.g. an Azure SQL Database), `auth` (`Windows`/`SQL`, default `Windows`), `username`+`password` (required for `SQL`), `encrypt_mode` (`Optional`/`Mandatory`/`Strict`, default `Mandatory`), `trust_server_certificate` (default false), `read_only_intent`, `multi_subnet_failover`. Servers are processed in order; a duplicate (case-folded, vs existing or an earlier entry) is `duplicate`, an unreachable server is `connection_failed` (the batch continues), Entra/MFA/Service-Principal/Managed-Identity auth is `invalid` (Windows/SQL only). Returns `{added, skipped, failed, results:[{server, status, detail}]}` | `servers_json` (required — a JSON array of server objects) |
        | `remove_server` | Removes a monitored server by name (resolved like every `server_name`). Returns `{status:"removed", server}` or `{status:"not_found"}`. Already-collected history is NOT deleted | `server_name` (required) |

        A SQL password is encrypted at rest (DPAPI, the service identity) and is NEVER returned by a read tool. It DOES travel to this endpoint inside `add_servers`' request JSON, so on a LAN deployment reach the endpoint only through the documented TLS reverse proxy.

        The three config-change tools diff the store's config snapshots. This edition captures configuration WHEN THE SERVICE CONNECTS to a server (not on a fixed schedule), so a change is detected between two connect snapshots and at least two are needed — a stable, always-connected deployment may show no changes until the next connect. They emit only the values the collectors capture; the Dashboard's `requires_restart` / setting `description` / `setting_type` / generated change-narrative enrichment is not collected here and is omitted. `get_blocking_deadlock_stats` (the Dashboard's blocking/deadlock aggregate) is NOT hosted: this edition has no blocking/deadlock rollup table — use `get_blocking` / `get_deadlocks` for the raw events.

        Jumping from an alert to its incident: `get_blocking`, `get_deadlocks` and `get_deadlock_detail` accept an
        optional `dedup_key` — the #1140 alert fingerprint, shown to operators as the alert's **Dedup Key** fact and
        carried onto downstream tickets. Pass it to get exactly that incident instead of pulling a server+time window
        and guessing which row the alert meant. Those three tools also RETURN a `dedup_key` on every row, so an
        incident found by browsing can be correlated back to its alerts, or handed to another agent as a stable
        identifier. Two things to know when it matches nothing: the fingerprint is scoped to the server's DISPLAY name
        and to the incident's involved objects, so a server renamed since the alert fired has different keys now; and
        `hours_back` still bounds the search, so widen it before concluding the incident is gone. The no-match response
        says how many rows it examined, which distinguishes those cases.

        Note on `next_tools`: analyze_server findings include `next_tools` recommendations. Most are hosted on this server — the plan-analysis tools (`analyze_query_plan`, `analyze_query_store_plan`) and the data-read tools listed above (`get_wait_stats`, `get_top_queries_by_cpu`, `get_cpu_utilization`, `get_memory_stats`, `get_file_io_stats`, `get_tempdb_trend`, `get_blocking`, `get_deadlocks`, `get_waiting_tasks`, `get_active_queries`, ...) — so follow those here. `get_top_queries_by_cpu` / `get_top_procedures_by_cpu` / `get_query_store_top` are where the `query_hash` / `sql_handle` / `query_id` + `plan_id` keys for the plan-analysis tools come from. The resource-contention + jobs tools (`get_latch_stats`, `get_spinlock_stats`, `get_resource_semaphore`, `get_memory_grants`, `get_plan_cache_bloat`, `get_cpu_scheduler_pressure`, `get_running_jobs`), the trend siblings (`get_memory_trend`, `get_perfmon_trend`, `get_file_io_trend`, `get_query_trend`, `get_query_duration_trend`), the `get_health_parser_*` system-health family, and the blocking/deadlock trend + memory-pressure reads (`get_blocking_trend`, `get_deadlock_trend`, `get_memory_pressure_events`) are all hosted here too — follow those `next_tools` on this server. Two `next_tools` names differ from what this edition hosts: `get_blocked_process_reports` (a Lite name) is served here as `get_blocked_process_xml` (with `get_blocking` for a quick overview), and `get_blocking_deadlock_stats` (the Dashboard's blocking/deadlock rollup) is not hosted at all — use `get_blocking` / `get_deadlocks` instead.

        ## Recommended Workflow

        1. **Discover**: `list_servers` — see the monitored servers and their collection freshness; `get_collection_health` confirms the collectors are current before you trust the data
        2. **Diagnose**: `analyze_server` — run the inference engine for an evidence-backed assessment with severity-ranked findings, each carrying `next_tools`
        3. **Review history**: `get_analysis_findings` — see what the service's scheduled analysis has already found
        4. **Investigate the metrics**: follow a finding's `next_tools` into the data tools — `get_cpu_utilization`, `get_wait_stats` / `get_wait_trend`, `get_memory_stats` / `get_memory_clerks`, `get_file_io_stats`, `get_tempdb_trend`, `get_perfmon_stats`
        5. **Find the query**: `get_top_queries_by_cpu` / `get_top_procedures_by_cpu` / `get_query_store_top` — identify the expensive query/procedure and get its `query_hash` / `sql_handle` / `query_id` + `plan_id`
        6. **Analyze its plan**: `analyze_query_plan` / `analyze_procedure_plan` / `analyze_query_store_plan` — analyze the captured plan for warnings, missing indexes, and grant/spill problems (or `analyze_plan_xml` for plan XML you already have)
        7. **Deep dive / compare / config**: `get_analysis_facts` (what the engine sees), `compare_analysis` (new vs baseline), `audit_config` (edition-aware config)
        8. **Silence noise**: `mute_analysis_finding` — mute a finding pattern the operator has accepted
        """;
}
