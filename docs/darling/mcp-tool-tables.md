# MCP tools and the store tables they read

Audience: operators, and anyone who builds on the store.

This page lists every Darling MCP tool and the store objects that its SQL uses. Use it to find the collector behind a tool, or to choose the tables to keep when you copy the store to another place.

## How to read the tables

- **Store objects** are tables, views and continuous aggregates in the store's `collect` and `config` schemas.
- When a tool reads a view or a continuous aggregate, the page names the base table first. The view or aggregate follows in parentheses.
- A note in parentheses after a table, such as `(only when tag given)`, means that the tool reads the table only in that case.
- For a tool that changes something, `(insert)`, `(update)` and `(delete)` show what the tool does to each table. Tables that such a tool only reads have no note.
- The page leaves out the lookups that almost every tool makes. These are the server name against `config.config_monitored_servers` and `collect.servers`, the server tags, and the token and audit checks. A tool's row names these tables only when the tool's answer comes from them, as with `list_servers`.

## Limits

- The page matches the `dev` branch at the commit that added it, with the store at schema version 221. The `tools/list` call on a running server gives the full list for that build.
- A tool that has no row here was added after this page. A changed tool can read other tables than the ones shown here.
- Which tools a server serves depends on the `mcp` switches. See [MCP configuration](configuration-mcp.md#mcp).
- The page was made by reading each tool's code down to the SQL that runs. It was not made from a store trace.


## Tools that read collected data (147)

| Tool | Store objects |
|---|---|
| `analyze_procedure_plan` | `collect.procedure_stats`<br>`collect.query_plan_dim`<br>`collect.server_properties`<br>`collect.server_config` |
| `analyze_query_plan` | `collect.query_stats` (through the view `collect.v_query_stats`)<br>`collect.query_plan_dim`<br>`collect.server_properties`<br>`collect.server_config`<br>`collect.database_config` |
| `analyze_query_store_plan` | `collect.query_store_plan_map`<br>`collect.query_plan_dim`<br>`collect.query_store_stats`<br>`collect.server_properties`<br>`collect.server_config`<br>`collect.database_config`<br>`config.config_service` |
| `analyze_server` | *SQL Server targets:*<br>`collect.wait_stats`<br>`collect.deadlocks`<br>`collect.blocked_process_reports`<br>`collect.dmv_blocking_snapshots`<br>`collect.query_stats`<br>`collect.query_store_stats`<br>`collect.query_store_interval_wide`<br>`collect.query_store_interval_latest`<br>`collect.query_store_text`<br>`collect.query_store_plan_map`<br>`collect.query_plan_dim`<br>`collect.query_text_dim`<br>`collect.plan_regression_daily`<br>`collect.procedure_stats`<br>`collect.plan_cache_stats`<br>`collect.query_snapshots`<br>`collect.running_jobs`<br>`collect.session_stats`<br>`collect.cpu_utilization_stats`<br>`collect.memory_stats`<br>`collect.memory_grant_stats`<br>`collect.memory_clerks`<br>`collect.memory_pressure_events`<br>`collect.perfmon_stats`<br>`collect.file_io_stats`<br>`collect.tempdb_stats`<br>`collect.database_size_stats`<br>`collect.database_config`<br>`collect.server_config`<br>`collect.server_properties`<br>`collect.trace_flags`<br>`collect.index_object_stats`<br>`collect.default_trace_events`<br>`collect.collection_log`<br>`collect.ag_replica_states`<br>`collect.ag_database_replica_states`<br>`collect.v_wait_stats`<br>`collect.v_deadlocks`<br>`collect.v_blocked_process_reports`<br>`collect.v_dmv_blocking_snapshots`<br>`collect.v_query_stats`<br>`collect.v_query_store_stats`<br>`collect.v_query_snapshots`<br>`collect.v_session_stats`<br>`collect.v_cpu_utilization_stats`<br>`collect.v_memory_stats`<br>`collect.v_memory_grant_stats`<br>`collect.v_perfmon_stats`<br>`collect.v_file_io_stats`<br>`collect.v_tempdb_stats`<br>`collect.v_database_size_stats`<br>`collect.v_database_config`<br>`collect.v_index_object_stats`<br>*Anomaly baselines:*<br>`collect.wait_stats` (through the continuous aggregate `collect.wait_stats_interval_baseline`)<br>`collect.wait_stats_baseline` (a retired continuous aggregate)<br>`collect.perfmon_stats` (through the continuous aggregate `collect.perfmon_interval_baseline`)<br>`collect.perfmon_baseline` (a retired continuous aggregate)<br>`collect.session_stats` (through the continuous aggregate `collect.session_stats_baseline`)<br>`collect.query_stats` (through the continuous aggregate `collect.query_stats_baseline`)<br>`collect.memory_stats` (through the continuous aggregate `collect.memory_baseline`)<br>`collect.blocked_process_reports` (through the continuous aggregate `collect.blocked_process_baseline`)<br>`collect.deadlock_baseline`<br>`collect.cpu_utilization_stats`<br>`collect.file_io_stats`<br>*PostgreSQL targets:*<br>`collect.pg_server_config`<br>`collect.pg_statement_stats`<br>`collect.pg_statement_text`<br>`collect.pg_database_stats`<br>`collect.pg_database_size_stats`<br>`collect.pg_wait_stats`<br>`collect.pg_wait_sampling`<br>`collect.pg_cpu_utilization`<br>`collect.pg_session_states`<br>`collect.pg_write_stats`<br>`collect.pg_extension_availability`<br>`collect.pg_plan_capture`<br>`collect.pg_plan_capture_readiness`<br>`collect.pg_io_stats`<br>`collect.pg_replication_stats`<br>`collect.pg_replication_slot_stats`<br>`collect.pg_log_events`<br>`collect.pg_kernel_stats`<br>`collect.pg_buffer_usage`<br>`collect.pg_blocking_edges`<br>`collect.pg_xmin_horizon`<br>`collect.pg_wraparound_stats`<br>`collect.pg_table_bloat_stats`<br>`collect.pg_index_bloat`<br>`collect.pg_predicate_stats`<br>`collect.pg_deadlocks`<br>`collect.pg_column_stats`<br>`collect.pg_autovacuum_stats`<br>*Force-plan remediation state:*<br>`collect.query_store_stats`<br>`collect.plan_correction`<br>`collect.analysis_findings`<br>`config.analysis_muted` |
| `audit_config` | *SQL Server targets:*<br>`collect.server_config`<br>`collect.server_properties`<br>`collect.memory_stats`<br>`collect.file_io_stats`<br>*PostgreSQL targets:*<br>`collect.pg_server_config`<br>`collect.pg_autovacuum_stats` |
| `backtest_custom_alert_rule` | The table that the rule's measure names. Also its hourly or daily aggregate, when the window uses one.<br>`config.custom_alert_rules` (when rule_id given)<br>`config.server_tags`<br>`config.server_tag_map`<br>`config.config_collector_schedules` |
| `compare_analysis` | *SQL Server targets:*<br>`collect.wait_stats`<br>`collect.deadlocks`<br>`collect.blocked_process_reports`<br>`collect.dmv_blocking_snapshots`<br>`collect.query_stats`<br>`collect.query_store_stats`<br>`collect.query_store_interval_wide`<br>`collect.query_store_interval_latest`<br>`collect.query_store_text`<br>`collect.query_store_plan_map`<br>`collect.query_plan_dim`<br>`collect.query_text_dim`<br>`collect.plan_regression_daily`<br>`collect.procedure_stats`<br>`collect.plan_cache_stats`<br>`collect.query_snapshots`<br>`collect.running_jobs`<br>`collect.session_stats`<br>`collect.cpu_utilization_stats`<br>`collect.memory_stats`<br>`collect.memory_grant_stats`<br>`collect.memory_clerks`<br>`collect.memory_pressure_events`<br>`collect.perfmon_stats`<br>`collect.file_io_stats`<br>`collect.tempdb_stats`<br>`collect.database_size_stats`<br>`collect.database_config`<br>`collect.server_config`<br>`collect.server_properties`<br>`collect.trace_flags`<br>`collect.index_object_stats`<br>`collect.default_trace_events`<br>`collect.collection_log`<br>`collect.ag_replica_states`<br>`collect.ag_database_replica_states`<br>`collect.v_wait_stats`<br>`collect.v_deadlocks`<br>`collect.v_blocked_process_reports`<br>`collect.v_dmv_blocking_snapshots`<br>`collect.v_query_stats`<br>`collect.v_query_store_stats`<br>`collect.v_query_snapshots`<br>`collect.v_session_stats`<br>`collect.v_cpu_utilization_stats`<br>`collect.v_memory_stats`<br>`collect.v_memory_grant_stats`<br>`collect.v_perfmon_stats`<br>`collect.v_file_io_stats`<br>`collect.v_tempdb_stats`<br>`collect.v_database_size_stats`<br>`collect.v_database_config`<br>`collect.v_index_object_stats`<br>*Anomaly baselines:*<br>`collect.wait_stats` (through the continuous aggregate `collect.wait_stats_interval_baseline`)<br>`collect.wait_stats_baseline` (a retired continuous aggregate)<br>`collect.perfmon_stats` (through the continuous aggregate `collect.perfmon_interval_baseline`)<br>`collect.perfmon_baseline` (a retired continuous aggregate)<br>`collect.session_stats` (through the continuous aggregate `collect.session_stats_baseline`)<br>`collect.query_stats` (through the continuous aggregate `collect.query_stats_baseline`)<br>`collect.memory_stats` (through the continuous aggregate `collect.memory_baseline`)<br>`collect.blocked_process_reports` (through the continuous aggregate `collect.blocked_process_baseline`)<br>`collect.deadlock_baseline`<br>`collect.cpu_utilization_stats`<br>`collect.file_io_stats`<br>*PostgreSQL targets:*<br>`collect.pg_server_config`<br>`collect.pg_statement_stats`<br>`collect.pg_statement_text`<br>`collect.pg_database_stats`<br>`collect.pg_database_size_stats`<br>`collect.pg_wait_stats`<br>`collect.pg_wait_sampling`<br>`collect.pg_cpu_utilization`<br>`collect.pg_session_states`<br>`collect.pg_write_stats`<br>`collect.pg_extension_availability`<br>`collect.pg_plan_capture`<br>`collect.pg_plan_capture_readiness`<br>`collect.pg_io_stats`<br>`collect.pg_replication_stats`<br>`collect.pg_replication_slot_stats`<br>`collect.pg_log_events`<br>`collect.pg_kernel_stats`<br>`collect.pg_buffer_usage`<br>`collect.pg_blocking_edges`<br>`collect.pg_xmin_horizon`<br>`collect.pg_wraparound_stats`<br>`collect.pg_table_bloat_stats`<br>`collect.pg_index_bloat`<br>`collect.pg_predicate_stats`<br>`collect.pg_deadlocks`<br>`collect.pg_column_stats`<br>`collect.pg_autovacuum_stats` |
| `find_database` | `collect.database_states`<br>`collect.servers` |
| `get_active_queries` | `collect.query_snapshots`<br>`config.query_tag_map` (only when tag given)<br>`config.query_tags` (only when tag given) |
| `get_active_query_plan_xml` | `collect.query_snapshots` |
| `get_agent_alerts` | `collect.agent_alerts`<br>`collect.servers`<br>`collect.collection_log` |
| `get_ag_health` | `collect.ag_replica_states`<br>`collect.ag_database_replica_states` |
| `get_alert_history` | `config.config_alert_log` |
| `get_analysis_facts` | *SQL Server targets:*<br>`collect.wait_stats`<br>`collect.deadlocks`<br>`collect.blocked_process_reports`<br>`collect.dmv_blocking_snapshots`<br>`collect.query_stats`<br>`collect.query_store_stats`<br>`collect.query_store_interval_wide`<br>`collect.query_store_interval_latest`<br>`collect.query_store_text`<br>`collect.query_store_plan_map`<br>`collect.query_plan_dim`<br>`collect.query_text_dim`<br>`collect.plan_regression_daily`<br>`collect.procedure_stats`<br>`collect.plan_cache_stats`<br>`collect.query_snapshots`<br>`collect.running_jobs`<br>`collect.session_stats`<br>`collect.cpu_utilization_stats`<br>`collect.memory_stats`<br>`collect.memory_grant_stats`<br>`collect.memory_clerks`<br>`collect.memory_pressure_events`<br>`collect.perfmon_stats`<br>`collect.file_io_stats`<br>`collect.tempdb_stats`<br>`collect.database_size_stats`<br>`collect.database_config`<br>`collect.server_config`<br>`collect.server_properties`<br>`collect.trace_flags`<br>`collect.index_object_stats`<br>`collect.default_trace_events`<br>`collect.collection_log`<br>`collect.ag_replica_states`<br>`collect.ag_database_replica_states`<br>`collect.v_wait_stats`<br>`collect.v_deadlocks`<br>`collect.v_blocked_process_reports`<br>`collect.v_dmv_blocking_snapshots`<br>`collect.v_query_stats`<br>`collect.v_query_store_stats`<br>`collect.v_query_snapshots`<br>`collect.v_session_stats`<br>`collect.v_cpu_utilization_stats`<br>`collect.v_memory_stats`<br>`collect.v_memory_grant_stats`<br>`collect.v_perfmon_stats`<br>`collect.v_file_io_stats`<br>`collect.v_tempdb_stats`<br>`collect.v_database_size_stats`<br>`collect.v_database_config`<br>`collect.v_index_object_stats`<br>*Anomaly baselines:*<br>`collect.wait_stats` (through the continuous aggregate `collect.wait_stats_interval_baseline`)<br>`collect.wait_stats_baseline` (a retired continuous aggregate)<br>`collect.perfmon_stats` (through the continuous aggregate `collect.perfmon_interval_baseline`)<br>`collect.perfmon_baseline` (a retired continuous aggregate)<br>`collect.session_stats` (through the continuous aggregate `collect.session_stats_baseline`)<br>`collect.query_stats` (through the continuous aggregate `collect.query_stats_baseline`)<br>`collect.memory_stats` (through the continuous aggregate `collect.memory_baseline`)<br>`collect.blocked_process_reports` (through the continuous aggregate `collect.blocked_process_baseline`)<br>`collect.deadlock_baseline`<br>`collect.cpu_utilization_stats`<br>`collect.file_io_stats`<br>*PostgreSQL targets:*<br>`collect.pg_server_config`<br>`collect.pg_statement_stats`<br>`collect.pg_statement_text`<br>`collect.pg_database_stats`<br>`collect.pg_database_size_stats`<br>`collect.pg_wait_stats`<br>`collect.pg_wait_sampling`<br>`collect.pg_cpu_utilization`<br>`collect.pg_session_states`<br>`collect.pg_write_stats`<br>`collect.pg_extension_availability`<br>`collect.pg_plan_capture`<br>`collect.pg_plan_capture_readiness`<br>`collect.pg_io_stats`<br>`collect.pg_replication_stats`<br>`collect.pg_replication_slot_stats`<br>`collect.pg_log_events`<br>`collect.pg_kernel_stats`<br>`collect.pg_buffer_usage`<br>`collect.pg_blocking_edges`<br>`collect.pg_xmin_horizon`<br>`collect.pg_wraparound_stats`<br>`collect.pg_table_bloat_stats`<br>`collect.pg_index_bloat`<br>`collect.pg_predicate_stats`<br>`collect.pg_deadlocks`<br>`collect.pg_column_stats`<br>`collect.pg_autovacuum_stats` |
| `get_analysis_findings` | `collect.analysis_findings`<br>`config.analysis_muted`<br>`collect.query_store_stats`<br>`collect.plan_correction`<br>`collect.ag_replica_states`<br>`collect.ag_database_replica_states` |
| `get_blocked_process_xml` | `collect.blocked_process_reports` |
| `get_blocking` | `collect.blocked_process_reports`<br>`collect.dmv_blocking_snapshots` (through the view `collect.v_dmv_blocking_snapshots`) |
| `get_blocking_plan_xml` | `collect.blocked_process_reports` |
| `get_blocking_stats` | `collect.blocked_process_reports`<br>`collect.dmv_blocking_snapshots`<br>`collect.deadlocks`<br>`collect.collection_log`<br>`collect.v_blocked_process_reports`<br>`collect.v_dmv_blocking_snapshots`<br>`collect.v_deadlocks` |
| `get_blocking_trend` | `collect.blocked_process_reports` (through the view `collect.v_blocked_process_reports`)<br>`collect.dmv_blocking_snapshots` (through the view `collect.v_dmv_blocking_snapshots`)<br>`collect.collection_log` (through the view `collect.v_collection_log`) |
| `get_client_timeouts` | `collect.attention_events`<br>`collect.collection_log` |
| `get_compiles_after_evict` | `config.investigation_journal`<br>`collect.query_stats`<br>`collect.procedure_stats` |
| `get_cpu_by_database` | `collect.query_stats`<br>`collect.query_stats` (through the continuous aggregate `collect.query_stats_io_hourly`)<br>`collect.procedure_stats`<br>`collect.procedure_stats` (through the continuous aggregate `collect.procedure_stats_io_hourly`)<br>`collect.query_text_dim` |
| `get_cpu_scheduler_pressure` | `collect.cpu_scheduler_stats` (through the view `collect.v_cpu_scheduler_stats`) |
| `get_cpu_utilization` | `collect.cpu_utilization_stats` |
| `get_current_waits_trend` | `collect.waiting_tasks` |
| `get_daily_summary` | `collect.wait_stats` (through the view `collect.v_wait_stats`)<br>`collect.query_stats` (through the view `collect.v_query_stats`)<br>`collect.deadlocks` (through the view `collect.v_deadlocks`)<br>`collect.blocked_process_reports` (through the view `collect.v_blocked_process_reports`)<br>`collect.dmv_blocking_snapshots` (through the view `collect.v_dmv_blocking_snapshots`)<br>`collect.cpu_utilization_stats` (through the view `collect.v_cpu_utilization_stats`)<br>`collect.collection_log` (through the view `collect.v_collection_log`)<br>`collect.memory_pressure_events` (through the view `collect.v_memory_pressure_events`)<br>`config.config_alert_log`<br>`config.config_alert_settings`<br>`config.config_collector_schedules`<br>`collect.query_stats_hourly`<br>`collect.query_stats_interval_hourly`<br>`collect.query_stats_daily` |
| `get_daily_summary_range` | `collect.wait_stats` (through the view `collect.v_wait_stats`)<br>`collect.query_stats` (through the view `collect.v_query_stats`)<br>`collect.deadlocks` (through the view `collect.v_deadlocks`)<br>`collect.blocked_process_reports` (through the view `collect.v_blocked_process_reports`)<br>`collect.dmv_blocking_snapshots` (through the view `collect.v_dmv_blocking_snapshots`)<br>`collect.cpu_utilization_stats` (through the view `collect.v_cpu_utilization_stats`)<br>`collect.collection_log` (through the view `collect.v_collection_log`)<br>`collect.memory_pressure_events` (through the view `collect.v_memory_pressure_events`)<br>`config.config_alert_log`<br>`config.config_alert_settings`<br>`config.config_collector_schedules`<br>`collect.query_stats_hourly`<br>`collect.query_stats_interval_hourly`<br>`collect.query_stats_daily` |
| `get_database_config` | `collect.database_config`<br>`collect.v_database_config` |
| `get_database_config_changes` | `collect.database_config`<br>`collect.v_database_config` |
| `get_database_protection` | `collect.database_protection`<br>`collect.servers`<br>`collect.collection_log` |
| `get_database_scoped_config` | `collect.database_scoped_config`<br>`collect.v_database_scoped_config` |
| `get_database_sizes` | `collect.database_size_stats` (through the view `collect.v_database_size_stats`) |
| `get_database_state_expectations` | `collect.database_states`<br>`config.database_state_expected` |
| `get_deadlock_detail` | `collect.deadlocks` |
| `get_deadlock_plan_xml` | `collect.deadlocks` |
| `get_deadlock_trend` | `collect.deadlocks` (through the view `collect.v_deadlocks`)<br>`collect.collection_log` (through the view `collect.v_collection_log`) |
| `get_deadlocks` | `collect.deadlocks` |
| `get_default_trace_events` | `collect.default_trace_events`<br>`collect.server_properties` |
| `get_file_io_stats` | `collect.file_io_stats`<br>`collect.v_file_io_stats` |
| `get_file_io_trend` | `collect.file_io_stats`<br>`collect.v_file_io_stats` |
| `get_finops` | *The tables depend on `view`:*<br>`collect.cpu_utilization_stats`<br>`collect.memory_stats` (through a view)<br>`collect.memory_grant_stats` (through a view)<br>`collect.server_properties`<br>`collect.index_object_stats`<br>`collect.database_states`<br>`collect.query_stats`<br>`collect.file_io_stats` (through a view)<br>`collect.query_stats` (through the continuous aggregate `collect.query_stats_hourly` or `collect.query_stats_daily`)<br>`collect.query_stats` (through the continuous aggregate `collect.query_stats_db_hourly` or `collect.query_stats_db_daily`)<br>`collect.session_stats`<br>`collect.database_size_stats`<br>`collect.query_stats` (through a view)<br>`collect.tempdb_stats` (through a view)<br>`collect.wait_stats` (through a view)<br>`collect.query_text_dim`<br>`collect.query_plan_dim`<br>`collect.index_object_stats` (through a view)<br>`collect.servers` |
| `get_finops_inventory` | `collect.servers`<br>`collect.server_properties`<br>`collect.cpu_utilization_stats`<br>`collect.memory_stats`<br>`collect.memory_grant_stats`<br>`collect.database_size_stats`<br>`collect.query_stats`<br>`collect.query_stats` (through the continuous aggregate `collect.query_stats_db_hourly`)<br>`collect.query_stats` (through the continuous aggregate `collect.query_stats_db_interval_hourly`)<br>`collect.collection_log`<br>`collect.v_cpu_utilization_stats`<br>`collect.v_memory_stats`<br>`collect.v_memory_grant_stats`<br>`collect.v_database_size_stats`<br>`collect.v_query_stats`<br>`collect.v_collection_log` |
| `get_finops_recommendations` | `collect.servers`<br>`collect.server_properties`<br>`collect.memory_stats`<br>`collect.running_jobs`<br>`collect.file_io_stats`<br>`collect.cpu_utilization_stats`<br>`collect.memory_grant_stats`<br>`collect.database_config`<br>`collect.database_size_stats`<br>`collect.index_object_stats`<br>`collect.query_stats`<br>`collect.database_states`<br>`collect.ag_replica_states`<br>`collect.ag_database_replica_states`<br>`collect.v_memory_stats`<br>`collect.v_running_jobs`<br>`collect.v_file_io_stats`<br>`collect.v_cpu_utilization_stats`<br>`collect.v_memory_grant_stats`<br>`collect.v_database_config`<br>`collect.v_database_size_stats`<br>`collect.v_index_object_stats`<br>`collect.v_query_stats` |
| `get_fleet_overview` | `collect.servers`<br>`collect.cpu_utilization_stats`<br>`collect.pg_cpu_utilization`<br>`collect.memory_stats`<br>`collect.memory_grant_stats`<br>`collect.cpu_scheduler_stats`<br>`collect.blocked_process_reports` (through a view)<br>`collect.dmv_blocking_snapshots` (through a view)<br>`collect.deadlocks` (through a view)<br>`collect.pg_database_stats`<br>`collect.collection_log`<br>`collect.collection_log` (through the continuous aggregate `collect.collection_health_hourly`)<br>`config.config_mute_rules`<br>`config.config_alert_settings`<br>`config.server_tags`<br>`config.server_tag_map`<br>`config.config_service`<br>`config.config_command`<br>`collect.v_cpu_utilization_stats`<br>`collect.v_memory_stats`<br>`collect.v_memory_grant_stats`<br>`collect.v_cpu_scheduler_stats`<br>`collect.v_collection_log` |
| `get_forced_plans` | `collect.query_store_forced_plans`<br>`collect.query_store_plan_map`<br>`collect.query_plan_dim`<br>`collect.query_store_stats`<br>`collect.v_query_store_stats` |
| `get_health_parser_cpu_tasks` | `collect.system_health_events`<br>`collect.collection_log`<br>`collect.v_system_health_events`<br>`collect.v_collection_log` |
| `get_health_parser_io_issues` | `collect.system_health_events`<br>`collect.collection_log`<br>`collect.v_system_health_events`<br>`collect.v_collection_log` |
| `get_health_parser_memory_broker` | `collect.system_health_events`<br>`collect.collection_log`<br>`collect.v_system_health_events`<br>`collect.v_collection_log` |
| `get_health_parser_memory_conditions` | `collect.system_health_events`<br>`collect.collection_log`<br>`collect.v_system_health_events`<br>`collect.v_collection_log` |
| `get_health_parser_memory_node_oom` | `collect.system_health_events`<br>`collect.collection_log`<br>`collect.v_system_health_events`<br>`collect.v_collection_log` |
| `get_health_parser_scheduler_issues` | `collect.system_health_events`<br>`collect.collection_log`<br>`collect.v_system_health_events`<br>`collect.v_collection_log` |
| `get_health_parser_severe_errors` | `collect.system_health_events`<br>`collect.collection_log`<br>`collect.database_size_stats`<br>`collect.v_system_health_events`<br>`collect.v_collection_log`<br>`collect.v_database_size_stats` |
| `get_health_parser_significant_waits` | `collect.system_health_events`<br>`collect.collection_log`<br>`collect.v_system_health_events`<br>`collect.v_collection_log` |
| `get_health_parser_system_health` | `collect.system_health_events`<br>`collect.collection_log`<br>`collect.v_system_health_events`<br>`collect.v_collection_log` |
| `get_identity_capacity` | `collect.identity_capacity`<br>`collect.servers` |
| `get_index_usage` | `collect.index_object_stats` (through the view `collect.v_index_object_stats`) |
| `get_job_history` | `collect.job_history`<br>`collect.server_properties`<br>`collect.servers`<br>`collect.agent_status`<br>`config.config_alert_settings` |
| `get_latch_stats` | `collect.latch_stats` (through the view `collect.v_latch_stats`) |
| `get_load_by_caller` | `collect.query_snapshots` |
| `get_lock_wait_trend` | `collect.wait_stats` (through the view `collect.v_wait_stats`) |
| `get_long_query_completions` | `collect.long_query_completions` |
| `get_memory_clerks` | `collect.memory_clerks`<br>`collect.v_memory_clerks` |
| `get_memory_grants` | `collect.memory_grant_stats` (through the view `collect.v_memory_grant_stats`) |
| `get_memory_pressure_events` | `collect.memory_pressure_events` (through the view `collect.v_memory_pressure_events`) |
| `get_memory_stats` | `collect.memory_stats`<br>`collect.v_memory_stats` |
| `get_memory_trend` | `collect.memory_stats`<br>`collect.memory_grant_stats`<br>`collect.v_memory_stats`<br>`collect.v_memory_grant_stats` |
| `get_object_locking` | `collect.index_object_stats` (through the view `collect.v_index_object_stats`)<br>`collect.database_config` |
| `get_perfmon_stats` | `collect.perfmon_stats`<br>`collect.v_perfmon_stats` |
| `get_perfmon_trend` | `collect.perfmon_stats`<br>`collect.v_perfmon_stats` |
| `get_pg_autovacuum_health` | `collect.pg_autovacuum_stats`<br>`collect.pg_log_events` |
| `get_pg_blocking` | `collect.pg_blocking_edges`<br>`collect.collection_log` |
| `get_pg_buffer_usage` | `collect.pg_buffer_usage` |
| `get_pg_column_stats` | `collect.pg_column_stats`<br>`collect.pg_table_bloat_stats`<br>`collect.collection_log` |
| `get_pg_cpu_utilization` | `collect.pg_cpu_utilization` |
| `get_pg_database_stats` | `collect.pg_database_stats` |
| `get_pg_database_trend` | `collect.pg_database_stats` |
| `get_pg_deadlock_detail` | `collect.pg_deadlocks` |
| `get_pg_deadlocks` | `collect.pg_deadlocks` |
| `get_pg_extensions` | `collect.pg_extension_availability` |
| `get_pg_index_bloat` | `collect.pg_index_bloat`<br>`collect.collection_log` |
| `get_pg_index_usage` | `collect.pg_index_usage_stats` |
| `get_pg_io_stats` | `collect.pg_io_stats`<br>`collect.pg_io_stats_hourly`<br>`collect.pg_io_stats_hourly_state`<br>`collect.pg_server_config` |
| `get_pg_io_trend` | `collect.pg_io_stats`<br>`collect.pg_server_config` |
| `get_pg_kernel_stats` | `collect.pg_kernel_stats` |
| `get_pg_lock_stats` | `collect.pg_lock_stats` |
| `get_pg_log_events` | `collect.pg_log_events` |
| `get_pg_logging_audit` | `collect.pg_server_config` |
| `get_pg_plan_capture_readiness` | `collect.pg_plan_capture_readiness` |
| `get_pg_plans` | `collect.pg_plan_capture`<br>`collect.pg_plan_capture_readiness` |
| `get_pg_predicate_stats` | `collect.pg_predicate_stats` |
| `get_pg_query_duration_trend` | `collect.pg_statement_stats` |
| `get_pg_replication_slots` | `collect.pg_replication_slot_stats` |
| `get_pg_replication_stats` | `collect.pg_replication_stats` |
| `get_pg_server_config` | `collect.pg_server_config` |
| `get_pg_server_config_changes` | `collect.pg_server_config`<br>`collect.darling_schema_version` |
| `get_pg_session_states` | `collect.pg_session_states`<br>`collect.collection_log` |
| `get_pg_table_bloat` | `collect.pg_table_bloat_stats` |
| `get_pg_top_queries` | `collect.pg_statement_stats`<br>`collect.pg_statement_text`<br>`collect.pg_server_config`<br>`collect.collection_log`<br>`collect.v_collection_log` |
| `get_pg_wait_sampling` | `collect.pg_wait_sampling`<br>`collect.collector_state` |
| `get_pg_wait_stats` | `collect.pg_wait_stats` |
| `get_pg_wait_trend` | `collect.pg_wait_sampling` |
| `get_pg_wraparound_risk` | `collect.pg_wraparound_stats` |
| `get_pg_write_stats` | `collect.pg_write_stats` |
| `get_pg_xmin_horizon` | `collect.pg_xmin_horizon`<br>`collect.collection_log` |
| `get_plan_cache_bloat` | `collect.plan_cache_stats` (through the view `collect.v_plan_cache_stats`) |
| `get_plan_corrections` | `collect.plan_correction` |
| `get_plan_force_precheck` | `collect.query_store_stats`<br>`collect.query_store_interval_wide`<br>`collect.query_store_text`<br>`collect.query_store_health` (through the view `collect.v_query_store_health`)<br>`collect.index_object_stats` (through the view `collect.v_index_object_stats`)<br>`collect.job_history`<br>`collect.plan_correction`<br>`collect.analysis_findings`<br>`collect.servers` |
| `get_plan_xml` | `collect.query_stats` (through the view `collect.v_query_stats`)<br>`collect.query_plan_dim` |
| `get_procedure_duration_trend` | `collect.procedure_stats`<br>`collect.procedure_stats` (through the continuous aggregate `collect.procedure_stats_hourly`)<br>`collect.procedure_stats` (through the continuous aggregate `collect.procedure_stats_interval_hourly`)<br>`collect.procedure_stats` (through the continuous aggregate `collect.procedure_stats_daily`)<br>`collect.collection_log` |
| `get_procedure_plan_xml` | `collect.procedure_stats`<br>`collect.query_plan_dim` |
| `get_pvs_stats` | `collect.pvs_stats` (through the view `collect.v_pvs_stats`) |
| `get_query_duration_trend` | `collect.query_stats`<br>`collect.query_stats` (through the continuous aggregate `collect.query_stats_hourly`)<br>`collect.query_stats` (through the continuous aggregate `collect.query_stats_interval_hourly`)<br>`collect.query_stats` (through the continuous aggregate `collect.query_stats_daily`)<br>`collect.query_stats_interval_daily` (continuous aggregate) |
| `get_query_heatmap` | `collect.query_stats`<br>`collect.query_text_dim` |
| `get_query_pileups` | `collect.query_snapshots`<br>`collect.dmv_blocking_snapshots` |
| `get_query_repro_script` | `collect.query_stats` (through the view `collect.v_query_stats`)<br>`collect.query_store_text`<br>`collect.query_store_stats`<br>`collect.query_snapshots`<br>`collect.query_plan_dim`<br>`collect.server_properties`<br>`collect.server_config`<br>`collect.database_config` |
| `get_query_store_aborts` | `collect.query_store_stats`<br>`collect.query_store_interval_wide`<br>`collect.query_store_text` |
| `get_query_store_clutter` | `collect.collection_log` (through the view `collect.v_collection_log`)<br>`collect.query_store_stats`<br>`collect.query_store_health` (through the view `collect.v_query_store_health`)<br>`collect.wait_stats` (through the view `collect.v_wait_stats`)<br>`collect.memory_clerks` (through the view `collect.v_memory_clerks`) |
| `get_query_store_duration_trend` | `collect.query_store_stats`<br>`collect.query_store_stats_interval_hourly` (through the continuous aggregate `collect.query_store_stats_corrected_hourly`) |
| `get_query_store_health` | `collect.query_store_health`<br>`collect.v_query_store_health` |
| `get_query_store_hints` | `collect.query_store_hints`<br>`collect.query_store_text`<br>`collect.query_store_stats` |
| `get_query_store_plan_xml` | `collect.query_store_plan_map`<br>`collect.query_plan_dim`<br>`collect.query_store_stats`<br>`config.config_service` |
| `get_query_store_query_history` | `collect.query_store_stats`<br>`collect.query_store_interval_wide`<br>`collect.query_store_forced_plans`<br>`collect.query_store_plan_map`<br>`collect.query_plan_dim` |
| `get_query_store_regressions` | `collect.query_store_stats`<br>`collect.query_store_interval_wide`<br>`collect.query_store_interval_latest`<br>`collect.plan_regression_daily`<br>`collect.query_store_text` |
| `get_query_store_top` | `collect.query_store_stats`<br>`collect.query_store_interval_wide`<br>`collect.query_store_top_daily`<br>`collect.query_store_top_daily_built`<br>`collect.query_store_text`<br>`collect.collection_log` |
| `get_query_trend` | `collect.query_stats`<br>`collect.query_stats` (through the continuous aggregate `collect.query_stats_hourly`)<br>`collect.query_stats` (through the continuous aggregate `collect.query_stats_interval_hourly`)<br>`collect.query_stats` (through the continuous aggregate `collect.query_stats_daily`) |
| `get_resource_semaphore` | `collect.memory_grant_stats` (through the view `collect.v_memory_grant_stats`) |
| `get_resource_use_by_tag` | `config.query_tags`<br>`config.query_tag_map`<br>`collect.query_stats`<br>`collect.query_stats` (through the continuous aggregate `collect.query_stats_io_hourly`)<br>`collect.query_store_interval_wide`<br>`collect.query_store_hash_hourly`<br>`collect.query_store_hash_daily`<br>`collect.procedure_stats`<br>`collect.procedure_stats` (through the continuous aggregate `collect.procedure_stats_io_hourly`)<br>`collect.blocked_process_reports`<br>`collect.dmv_blocking_snapshots`<br>`collect.deadlocks` |
| `get_running_jobs` | `collect.running_jobs` (through the view `collect.v_running_jobs`)<br>`collect.collection_log` (through the view `collect.v_collection_log`) |
| `get_server_config` | `collect.server_config`<br>`collect.v_server_config` |
| `get_server_config_changes` | `collect.server_config`<br>`collect.v_server_config` |
| `get_server_properties` | `collect.server_properties` |
| `get_server_summary` | `collect.cpu_utilization_stats` (through the view `collect.v_cpu_utilization_stats`)<br>`collect.memory_stats` (through the view `collect.v_memory_stats`)<br>`collect.blocked_process_reports` (through the view `collect.v_blocked_process_reports`)<br>`collect.dmv_blocking_snapshots` (through the view `collect.v_dmv_blocking_snapshots`)<br>`collect.deadlocks` (through the view `collect.v_deadlocks`)<br>`collect.collection_log` (through the view `collect.v_collection_log`) |
| `get_server_trend` | `collect.wait_stats` (through the view `collect.v_wait_stats`)<br>`collect.cpu_scheduler_stats` (through the view `collect.v_cpu_scheduler_stats`)<br>`collect.memory_clerks` (through the view `collect.v_memory_clerks`)<br>`collect.plan_cache_stats` (through the view `collect.v_plan_cache_stats`)<br>`collect.latch_stats` (through the view `collect.v_latch_stats`)<br>`collect.spinlock_stats` (through the view `collect.v_spinlock_stats`)<br>`collect.session_summary_stats` (through the view `collect.v_session_summary_stats`)<br>`collect.collection_log` (through the view `collect.v_collection_log`)<br>`collect.file_io_stats` (through the view `collect.v_file_io_stats`)<br>`collect.tempdb_stats` (through the view `collect.v_tempdb_stats`) |
| `get_session_stats` | `collect.session_stats`<br>`collect.v_session_stats` |
| `get_spinlock_stats` | `collect.spinlock_stats` (through the view `collect.v_spinlock_stats`) |
| `get_sweep_reports` | `collect.fleet_sweep_runs`<br>`collect.fleet_sweep_server_verdicts`<br>`collect.fleet_sweep_would_have_paged`<br>`collect.fleet_sweep_watch_items` |
| `get_table_index_sizes` | `collect.index_object_stats` (through the view `collect.v_index_object_stats`) |
| `get_tempdb_trend` | `collect.tempdb_stats`<br>`collect.v_tempdb_stats` |
| `get_top_procedures_by_cpu` | `collect.procedure_stats`<br>`collect.cpu_utilization_stats`<br>`collect.server_properties`<br>`collect.procedure_stats` (through the continuous aggregate `collect.procedure_stats_hourly`)<br>`collect.procedure_stats` (through the continuous aggregate `collect.procedure_stats_interval_hourly`)<br>`collect.procedure_stats` (through the continuous aggregate `collect.procedure_stats_io_hourly`)<br>`collect.collection_log` |
| `get_top_queries_by_cpu` | `collect.query_stats`<br>`collect.query_text_dim`<br>`collect.cpu_utilization_stats`<br>`collect.server_properties`<br>`collect.query_stats` (through the continuous aggregate `collect.query_stats_hourly`)<br>`collect.query_stats` (through the continuous aggregate `collect.query_stats_interval_hourly`)<br>`collect.query_stats` (through the continuous aggregate `collect.query_stats_io_hourly`)<br>`collect.collection_log` |
| `get_trace_flag_changes` | `collect.trace_flags`<br>`collect.collection_log`<br>`collect.v_trace_flags`<br>`collect.v_collection_log` |
| `get_trace_flags` | `collect.trace_flags`<br>`collect.collection_log`<br>`collect.v_trace_flags`<br>`collect.v_collection_log` |
| `get_wait_stats` | `collect.wait_stats`<br>`collect.v_wait_stats` |
| `get_wait_trend` | `collect.wait_stats`<br>`collect.collection_log`<br>`collect.collector_state`<br>`collect.v_wait_stats` |
| `get_wait_types` | `collect.wait_stats`<br>`collect.v_wait_stats` |
| `get_waiting_tasks` | `collect.waiting_tasks` |
| `run_custom_view_panel` | The table that the panel's measure names. Also its hourly or daily aggregate, when the window uses one.<br>`collect.module_map` (module join)<br>`collect.servers` (query_store_interval_wide join)<br>`config.query_tag_map` |
| `test_custom_alert_rule` | The table that the rule's measure names. Also its hourly or daily aggregate, when the window uses one.<br>`config.custom_alert_rules` (when rule_id given)<br>`config.server_tags`<br>`config.server_tag_map` (tag scope) |
| `test_query_tag_rule` | `collect.query_tag_sightings`<br>`config.query_tag_rules`<br>`config.query_tags`<br>`config.query_tag_assignments`<br>`collect.servers` |

## Tools about Darling itself (40)

| Tool | Store objects |
|---|---|
| `analyze_plan_xml` | None. It analyses the plan XML that you pass in. |
| `describe_custom_view_catalog` | None. The catalog is built into the service. |
| `get_agent_changes` | `config.investigation_journal`<br>`config.config_alert_log`<br>`config.investigation_requests`<br>`config.investigation_evidence`<br>`config.config_service`<br>`collect.servers` |
| `get_alert_settings` | `config.config_alert_settings`<br>`config.config_notification`<br>`config.config_threshold_overrides` |
| `get_change_watches` | `config.change_watches`<br>`config.change_watch_changes` |
| `get_collection_health` | `collect.collection_log_hourly`<br>`collect.collection_log`<br>`collect.database_size_stats`<br>`config.config_collector_schedules`<br>`config.config_collector_run_times`<br>`collect.analysis_collection_caveats`<br>`collect.v_collection_log`<br>`collect.v_database_size_stats` |
| `get_collection_log` | `collect.collection_log`<br>`collect.servers`<br>`collect.v_collection_log` |
| `get_collection_state` | `config.config_service`<br>`config.config_monitored_servers` |
| `get_collector_cost` | `collect.collector_cost` |
| `get_collector_schedules` | `config.config_collector_schedules`<br>`config.config_collector_run_times` |
| `get_collector_stall_probes` | `collect.collector_stall_probes` |
| `get_command_result` | `config.config_command`<br>`config.read_mcp_command_result` |
| `get_custom_alert_rule` | `config.custom_alert_rules` |
| `get_custom_view` | `config.custom_views` |
| `get_investigation_evidence` | `config.investigation_evidence`<br>`config.investigation_requests` |
| `get_investigation_reports` | `config.investigation_requests`<br>`config.investigation_reports`<br>`config.investigation_journal`<br>`collect.servers` |
| `get_known_patterns` | `config.config_known_patterns` |
| `get_mute_rules` | `config.config_mute_rules` |
| `get_notification_routes` | `config.config_notification_routes`<br>`config.investigation_requests`<br>`config.config_monitored_servers`<br>`config.server_tags`<br>`config.server_tag_map` |
| `get_oversized_plan_backlog` | `collect.oversized_plan_backlog`<br>`collect.servers` |
| `get_read_latency` | `collect.read_latency` |
| `get_server_actions` | `config.config_monitored_servers`<br>`config.config_server_actions`<br>`collect.servers` |
| `get_server_settings` | `config.config_monitored_servers` |
| `get_slow_reads` | `collect.slow_reads` |
| `get_store_host` | `collect.store_metrics` |
| `get_store_log` | `collect.store_log_captures`<br>`collect.store_log_events` |
| `get_store_metrics` | `collect.store_metrics`<br>`collect.query_store_compose_stamp_failures` |
| `get_store_query_history` | `collect.store_statement_history`<br>`collect.store_statement_captures`<br>`config.store_statement_stats()` (function over pg_stat_statements) |
| `get_store_query_stats` | `config.store_statement_stats()` (function over pg_stat_statements)<br>`config.store_statement_stats_info()` (function over pg_stat_statements_info) |
| `get_tool_guide` | None. The guides are built into the service. |
| `list_custom_alert_rules` | `config.custom_alert_rules`<br>`config.config_alert_log` |
| `list_custom_alert_templates` | None. The templates are built into the service. |
| `list_custom_views` | `config.custom_views` |
| `list_investigation_requests` | `config.investigation_requests`<br>`config.investigation_request_alerts`<br>`collect.servers` |
| `list_query_tag_rules` | `config.query_tag_rules`<br>`config.query_tags`<br>`config.query_tag_state` |
| `list_query_tags` | `config.query_tags`<br>`config.query_tag_assignments`<br>`config.query_tag_rules`<br>`config.query_tag_state`<br>`collect.servers` |
| `list_servers` | `collect.servers`<br>`config.config_monitored_servers`<br>`collect.collection_log`<br>`collect.v_collection_log` |
| `validate_custom_alert_rule` | None. It checks the definition that you pass in. |
| `validate_custom_view` | None. It checks the definition that you pass in. |
| `wait_for_investigation_requests` | `config.investigation_journal`<br>`config.investigation_requests`<br>`config.investigation_request_alerts`<br>`collect.servers` |

## Tools that change something (86)

| Tool | Store objects |
|---|---|
| `add_servers` | `config.config_monitored_servers` (insert) |
| `analyze_now` | `config.config_command`<br>`config.request_mcp_command` |
| `approve_investigation_note` | `config.investigation_notes` (update)<br>`config.investigation_journal` (insert) |
| `assign_query_tag` | `config.query_tag_assignments` (insert/update)<br>`config.query_tags`<br>`collect.servers` |
| `assign_server_tag` | `config.server_tag_map` (insert)<br>`config.server_tags`<br>`collect.servers` |
| `claim_investigation_request` | `config.investigation_requests` (update claim/lease/brief)<br>`config.investigation_journal` (insert)<br>`config.investigation_request_alerts`<br>`config.config_alert_log`<br>`config.investigation_reports`<br>`collect.analysis_findings`<br>`config.investigation_evidence`<br>`config.investigation_notes`<br>`config.agent_environment_profiles`<br>`config.server_tag_map`<br>`config.config_mute_rules`<br>`config.analysis_muted`<br>`collect.plan_force_actions`<br>`config.service_version`<br>`collect.servers` |
| `clear_quiet_window` | `config.config_mute_rules` (update) |
| `close_investigation_request` | `config.investigation_requests` (update)<br>`config.investigation_journal` (insert) |
| `create_custom_alert_rule` | `config.custom_alert_rules` |
| `create_custom_view` | `config.custom_views` |
| `create_known_pattern` | `config.config_known_patterns`<br>`config.config_monitored_servers` (name lookup) |
| `create_mute_rule` | `config.config_mute_rules` |
| `create_notification_route` | `config.config_notification_routes` |
| `create_plan_guide` | No collected data. It acts on the monitored server and records the change in the agent change list. |
| `create_query_tag` | `config.query_tags` (insert) |
| `create_query_tag_rule` | `config.query_tag_rules` (insert)<br>`config.query_tags`<br>`config.query_tag_state`<br>`collect.servers` |
| `create_server_tag` | `config.server_tags` (insert) |
| `delete_custom_alert_rule` | `config.custom_alert_rules`<br>`config.custom_alert_state`<br>`config.config_alert_log` |
| `delete_custom_view` | `config.custom_views` |
| `delete_known_pattern` | `config.config_known_patterns`<br>`config.config_monitored_servers` (name lookup) |
| `delete_mute_rule` | `config.config_mute_rules` |
| `delete_notification_route` | `config.config_notification_routes` |
| `delete_query_tag` | `config.query_tags` (delete)<br>`config.query_tag_assignments`<br>`config.query_tag_rules` |
| `delete_query_tag_rule` | `config.query_tag_rules` (delete)<br>`config.query_tags`<br>`config.query_tag_state` |
| `delete_server_tag` | `config.server_tags` (delete)<br>`config.server_tag_map`<br>`config.custom_alert_rules` |
| `disable_collector` | `config.config_collector_schedules` |
| `disable_server` | `config.config_monitored_servers` |
| `dismiss_alert_history` | `config.config_alert_log` |
| `drop_plan_guide` | No collected data. It acts on the monitored server and records the change in the agent change list. |
| `edit_server` | `config.config_monitored_servers`<br>`config.edit_monitored_server` |
| `enable_collector` | `config.config_collector_schedules` |
| `enable_server` | `config.config_monitored_servers` |
| `evict_cached_plan` | `collect.query_stats`<br>`collect.procedure_stats` |
| `force_query_store_plan` | No collected data. It acts on the monitored server and records the change in the agent change list. |
| `generate_webhook_signing_key` | `config.config_notification`<br>`config.config_notification_routes` |
| `kill_session` | `collect.server_properties`<br>`collect.dmv_blocking_snapshots` (through the view `collect.v_dmv_blocking_snapshots`)<br>`collect.blocked_process_reports` |
| `merge_deadlock_signatures` | `config.investigation_signature_merges`<br>`config.investigation_signature_days` |
| `mute_analysis_finding` | `config.analysis_muted`<br>`collect.analysis_findings` |
| `pause_collection` | `config.config_command`<br>`config.config_service` |
| `purge_expired_mute_rules` | `config.config_mute_rules` |
| `purge_now` | `config.config_command`<br>`config.config_collector_schedules` (retention overrides read for the preview)<br>`config.request_mcp_command` |
| `recompile_module` | No collected data. It acts on the monitored server and records the change in the agent change list. |
| `record_investigation_feedback` | `config.investigation_feedback` (insert)<br>`config.investigation_reports` (exists check)<br>`config.investigation_requests` |
| `reject_investigation_note` | `config.investigation_notes` (update)<br>`config.investigation_journal` (insert) |
| `reload_config` | `config.config_service` |
| `remove_server` | `config.config_monitored_servers` (delete)<br>`config.server_tag_map` (delete)<br>`config.query_tag_assignments` (delete)<br>`config.query_tag_rules` (delete)<br>`config.query_tag_map` (delete)<br>`config.config_collector_schedules` (delete)<br>`config.config_collector_run_times` (delete) |
| `remove_webhook_signing_key` | `config.config_notification`<br>`config.config_notification_routes` |
| `reset_collector_schedules` | `config.config_collector_schedules`<br>`config.config_collector_run_times` |
| `restore_alert_history` | `config.config_alert_log` |
| `resume_collection` | `config.config_command`<br>`config.config_service` |
| `run_collectors_now` | `config.config_command` |
| `save_environment_profile` | `config.agent_environment_profiles` (upsert/delete)<br>`config.server_tags` (tag exists check)<br>`config.config_monitored_servers` (server exists check)<br>`config.investigation_journal` |
| `set_agent_actions_enabled` | `config.config_service` |
| `set_alert_threshold_override` | `config.config_threshold_overrides` |
| `set_collector_databases` | `config.config_collector_schedules` |
| `set_collector_frequency` | `config.config_collector_schedules` |
| `set_collector_retention` | `config.config_collector_schedules` |
| `set_collector_run_at` | `config.config_collector_run_times` |
| `set_database_state_expected` | `config.database_state_expected`<br>`collect.database_states` (read for the current state) |
| `set_mute_rule_enabled` | `config.config_mute_rules` |
| `set_notification_route_enabled` | `config.config_notification_routes` |
| `set_notification_route_fallback` | `config.config_notification_routes` |
| `set_quiet_window` | `config.config_mute_rules` (insert) |
| `set_server_action` | `config.config_server_actions` (insert/update)<br>`config.config_monitored_servers`<br>`collect.servers` |
| `set_server_excluded_databases` | `config.config_monitored_servers` |
| `start_practice_request` | `config.investigation_requests` (insert)<br>`config.investigation_request_alerts` (copy/insert)<br>`config.investigation_reports` (read source verdicts)<br>`config.investigation_feedback` (read) |
| `submit_investigation_report` | `config.investigation_reports` (insert)<br>`config.investigation_requests` (update state)<br>`config.investigation_journal` (insert)<br>`config.investigation_notes` (insert)<br>`config.investigation_requests` (family check) |
| `test_connection` | `config.config_command`<br>`config.request_mcp_command` |
| `unassign_query_tag` | `config.query_tag_assignments` (delete) |
| `unassign_server_tag` | `config.server_tag_map` (delete)<br>`config.server_tags` |
| `undo_agent_change` | `config.investigation_journal`<br>`config.config_mute_rules`<br>`config.custom_alert_rules`<br>`config.config_threshold_overrides`<br>`config.investigation_signature_merges` |
| `undo_plan_change` | `config.investigation_journal`<br>`config.investigation_requests`<br>`config.investigation_evidence`<br>`config.config_alert_log` |
| `unforce_query_store_plan` | No collected data. It acts on the monitored server and records the change in the agent change list. |
| `unmute_analysis_finding` | `config.analysis_muted` |
| `update_alert_settings` | `config.config_alert_settings`<br>`config.config_notification` |
| `update_custom_alert_rule` | `config.custom_alert_rules` |
| `update_custom_view` | `config.custom_views` |
| `update_known_pattern` | `config.config_known_patterns`<br>`config.config_monitored_servers` (name lookup) |
| `update_mute_rule` | `config.config_mute_rules` |
| `update_notification_channels` | `config.config_notification` |
| `update_notification_route` | `config.config_notification_routes` |
| `update_query_tag` | `config.query_tags` (update) |
| `update_query_tag_rule` | `config.query_tag_rules` (update)<br>`config.query_tags`<br>`config.query_tag_state`<br>`collect.servers` |
| `update_server_tag` | `config.server_tags` (update) |
| `update_service_flags` | `config.config_service` |
| `write_investigation_note` | `config.investigation_notes` (insert)<br>`collect.servers` (exists check) |
