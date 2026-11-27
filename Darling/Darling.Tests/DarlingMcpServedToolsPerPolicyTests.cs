/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The tools the host serves under each write policy, built through the host's own registration (<see cref="DarlingMcpHostService.ConfigureMcpServices"/>)
/// and read back from the options the SDK serves from. Each tool's row says which policies serve it, its front door class and whether it takes
/// <c>server_name</c>. A change to how tools are registered must leave every row as it is, and a new tool adds its row here. Each policy's
/// served list, schemas and annotations included, and its instructions are also hashed, for a registration change to compare by hand: with
/// PM_SERVED_TOOLS_CAPTURE set, the test writes the rows, the hashes and the pinned hashes in <see cref="ExpectedHashes"/>. The hashes are not
/// asserted, because any description edit moves them.
/// </summary>
public sealed class DarlingMcpServedToolsPerPolicyTests
{
    private static readonly (string Name, DarlingMcpWritePolicy.Effective Policy, bool AgentMuteRules)[] Policies =
    {
        ("off", DarlingMcpWritePolicy.Effective.Off, true),
        ("writes", new DarlingMcpWritePolicy.Effective(Writes: true, ServerAdmin: false), true),
        ("writes+admin", new DarlingMcpWritePolicy.Effective(Writes: true, ServerAdmin: true), true),
        ("investigations", new DarlingMcpWritePolicy.Effective(Writes: false, ServerAdmin: false, Investigations: true), true),
        ("investigations, agent mute rules off", new DarlingMcpWritePolicy.Effective(Writes: false, ServerAdmin: false, Investigations: true), false),
    };

    private static McpServerOptions ServedOptions(DarlingMcpWritePolicy.Effective policy, bool agentMuteRules)
    {
        var services = new ServiceCollection();
        foreach (var serviceType in McpToolsListBudgetTests.ToolServiceParameterTypes())
        {
            services.AddSingleton(serviceType, _ => null!);
        }

        DarlingMcpHostService.ConfigureMcpServices(
            services, DarlingPeerDirectory.Snapshot.Empty, writes: policy, agentMuteRules: agentMuteRules);
        using var provider = services.BuildServiceProvider();
        return provider.GetRequiredService<IOptions<McpServerOptions>>().Value;
    }

    private static string Sha256(string text) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text)));

    private static bool TakesServerName(McpServerTool tool) =>
        tool.ProtocolTool.InputSchema.TryGetProperty("properties", out var properties)
        && properties.TryGetProperty(DarlingFrontDoorRouting.ServerArgument, out _);

    private static (List<string> Rows, List<string> Hashes) Actual()
    {
        var served = Policies
            .Select(p => (p.Name, Options: ServedOptions(p.Policy, p.AgentMuteRules)))
            .ToList();
        var byName = new SortedDictionary<string, McpServerTool>(StringComparer.Ordinal);
        foreach (var (tools, _, _) in new[] { McpToolsListBudgetTests.BuildServedTools() })
        {
            foreach (var tool in tools)
            {
                byName[tool.ProtocolTool.Name] = tool;
            }
        }

        var rows = new List<string>();
        foreach (var (name, tool) in byName)
        {
            var flags = string.Concat(served.Select(s => s.Options.ToolCollection!.TryGetPrimitive(name, out _) ? "1" : "0"));
            rows.Add($"{name} {flags} {DarlingFrontDoorRouting.ClassOf(name)}{(TakesServerName(tool) ? " server_name" : string.Empty)}");
        }

        var hashes = new List<string>();
        foreach (var (policyName, options) in served)
        {
            var protocolTools = options.ToolCollection!.Select(t => t.ProtocolTool).OrderBy(t => t.Name, StringComparer.Ordinal).ToList();
            var json = JsonSerializer.Serialize(new ListToolsResult { Tools = protocolTools }, McpJsonUtilities.DefaultOptions);
            hashes.Add($"{policyName}: {protocolTools.Count} tools {Sha256(json)} instructions {Sha256(options.ServerInstructions ?? string.Empty)}");
        }

        return (rows, hashes);
    }

    [Fact]
    public void EveryTool_KeepsItsPolicies_FrontDoorClassAndServerName()
    {
        var (rows, hashes) = Actual();
        if (Environment.GetEnvironmentVariable("PM_SERVED_TOOLS_CAPTURE") is { Length: > 0 } capture)
        {
            File.WriteAllLines(capture, rows.Select(r => $"        \"{r}\",").Append("----").Concat(hashes.Select(h => $"        \"{h}\","))
                .Append("---- pinned").Concat(ExpectedHashes.Select(h => $"        \"{h}\",")));
        }

        Assert.Equal(ExpectedRows, rows);
    }

    /// <summary>One row per registered tool: its name, then one flag per entry of <see cref="Policies"/> in order (1 when that policy serves it),
    /// then its front door class, then <c>server_name</c> when it takes one.</summary>
    private static readonly string[] ExpectedRows =
    {
        "add_alert_source 00100 StoreRequired",
        "add_servers 00100 StoreRequired",
        "analyze_now 01100 StoreRequired server_name",
        "analyze_plan_xml 11111 AnyOneStore",
        "analyze_procedure_plan 11111 FanOut server_name",
        "analyze_query_plan 11111 FanOut server_name",
        "analyze_query_store_plan 11111 FanOut server_name",
        "analyze_server 11111 FanOut server_name",
        "approve_investigation_note 01100 StoreRequired",
        "assign_query_tag 01100 StoreRequired",
        "assign_server_tag 01100 StoreRequired",
        "audit_config 11111 FanOut server_name",
        "backtest_custom_alert_rule 11111 StoreRequired",
        "call_tool 11111 Local",
        "claim_investigation_request 00011 StoreRequired",
        "clear_change_window 01111 StoreRequired",
        "clear_quiet_window 01100 StoreRequired",
        "close_investigation_request 01100 StoreRequired",
        "compare_analysis 11111 FanOut server_name",
        "create_custom_alert_rule 01111 StoreRequired",
        "create_custom_view 01100 StoreRequired",
        "create_known_pattern 01100 StoreRequired",
        "create_mute_rule 01111 StoreRequired server_name",
        "create_notification_route 01100 StoreRequired",
        "create_plan_guide 01111 StoreRequired server_name",
        "create_query_tag 01100 StoreRequired",
        "create_query_tag_rule 01100 StoreRequired",
        "create_server_tag 01100 StoreRequired",
        "create_web_access_group 00100 StoreRequired",
        "declare_change_window 01111 StoreRequired",
        "delete_custom_alert_rule 01111 StoreRequired",
        "delete_custom_view 01100 StoreRequired",
        "delete_known_pattern 01100 StoreRequired",
        "delete_mute_rule 01111 StoreRequired",
        "delete_notification_route 01100 StoreRequired",
        "delete_query_tag 01100 StoreRequired",
        "delete_query_tag_rule 01100 StoreRequired",
        "delete_server_tag 01100 StoreRequired",
        "delete_web_access_group 00100 StoreRequired",
        "describe_custom_view_catalog 11111 AnyOneStore",
        "disable_collector 01100 StoreRequired server_name",
        "disable_server 01100 StoreRequired server_name",
        "dismiss_alert_history 01100 StoreRequired",
        "drop_plan_guide 01111 StoreRequired server_name",
        "edit_server 00100 StoreRequired server_name",
        "enable_collector 01100 StoreRequired server_name",
        "enable_server 01100 StoreRequired server_name",
        "evict_cached_plan 01111 StoreRequired server_name",
        "find_database 11111 FanOut",
        "find_queries_by_text 11111 FanOut server_name",
        "find_schema_objects 11111 FanOut server_name",
        "find_tools 11111 Local",
        "force_query_store_plan 01111 StoreRequired server_name",
        "generate_webhook_signing_key 01100 StoreRequired",
        "get_active_queries 11111 FanOut server_name",
        "get_active_query_plan_xml 11111 FanOut server_name",
        "get_ag_health 11111 FanOut server_name",
        "get_agent_alerts 11111 FanOut server_name",
        "get_agent_changes 11111 FanOut",
        "get_alert_history 11111 FanOut server_name",
        "get_alert_settings 11111 FanOut server_name",
        "get_analysis_facts 11111 FanOut server_name",
        "get_analysis_findings 11111 FanOut server_name",
        "get_blocked_process_xml 11111 FanOut server_name",
        "get_blocking 11111 FanOut server_name",
        "get_blocking_plan_xml 11111 FanOut server_name",
        "get_blocking_stats 11111 FanOut server_name",
        "get_blocking_trend 11111 FanOut server_name",
        "get_change_watches 11111 FanOut server_name",
        "get_change_windows 11111 FanOut",
        "get_client_timeouts 11111 FanOut server_name",
        "get_collection_health 11111 FanOut server_name",
        "get_collection_log 11111 FanOut server_name",
        "get_collection_state 11111 FanOut",
        "get_collector_cost 11111 FanOut",
        "get_collector_readiness 11111 FanOut server_name",
        "get_collector_schedules 11111 FanOut server_name",
        "get_collector_stall_probes 11111 FanOut server_name",
        "get_command_result 11111 FanOut",
        "get_compiles_after_evict 11111 FanOut",
        "get_cpu_by_database 11111 FanOut server_name",
        "get_cpu_scheduler_pressure 11111 FanOut server_name",
        "get_cpu_utilization 11111 FanOut server_name",
        "get_current_waits_trend 11111 FanOut server_name",
        "get_custom_alert_rule 11111 FanOut",
        "get_custom_view 11111 FanOut",
        "get_daily_health_report 11111 FanOut",
        "get_daily_summary 11111 FanOut server_name",
        "get_daily_summary_range 11111 FanOut server_name",
        "get_database_config 11111 FanOut server_name",
        "get_database_config_changes 11111 FanOut server_name",
        "get_database_protection 11111 FanOut server_name",
        "get_database_scoped_config 11111 FanOut server_name",
        "get_database_sizes 11111 FanOut server_name",
        "get_database_state_expectations 11111 FanOut server_name",
        "get_deadlock_detail 11111 FanOut server_name",
        "get_deadlock_plan_xml 11111 FanOut server_name",
        "get_deadlock_trend 11111 FanOut server_name",
        "get_deadlocks 11111 FanOut server_name",
        "get_default_trace_events 11111 FanOut server_name",
        "get_file_io_stats 11111 FanOut server_name",
        "get_file_io_trend 11111 FanOut server_name",
        "get_finops 11111 FanOut server_name",
        "get_finops_inventory 11111 FanOut",
        "get_finops_recommendations 11111 FanOut server_name",
        "get_fleet_overview 11111 FanOut",
        "get_forced_plans 11111 FanOut server_name",
        "get_health_parser_cpu_tasks 11111 FanOut server_name",
        "get_health_parser_io_issues 11111 FanOut server_name",
        "get_health_parser_memory_broker 11111 FanOut server_name",
        "get_health_parser_memory_conditions 11111 FanOut server_name",
        "get_health_parser_memory_node_oom 11111 FanOut server_name",
        "get_health_parser_scheduler_issues 11111 FanOut server_name",
        "get_health_parser_severe_errors 11111 FanOut server_name",
        "get_health_parser_significant_waits 11111 FanOut server_name",
        "get_health_parser_system_health 11111 FanOut server_name",
        "get_identity_capacity 11111 FanOut server_name",
        "get_index_usage 11111 FanOut server_name",
        "get_investigation_evidence 11111 FanOut",
        "get_investigation_reports 11111 FanOut server_name",
        "get_job_history 11111 FanOut server_name",
        "get_known_patterns 11111 FanOut server_name",
        "get_latch_stats 11111 FanOut server_name",
        "get_load_by_caller 11111 FanOut server_name",
        "get_lock_wait_trend 11111 FanOut server_name",
        "get_long_query_completions 11111 FanOut server_name",
        "get_memory_clerks 11111 FanOut server_name",
        "get_memory_grants 11111 FanOut server_name",
        "get_memory_pressure_events 11111 FanOut server_name",
        "get_memory_stats 11111 FanOut server_name",
        "get_memory_trend 11111 FanOut server_name",
        "get_mute_rules 11111 FanOut",
        "get_notification_routes 11111 FanOut",
        "get_object_definition 11111 FanOut server_name",
        "get_object_locking 11111 FanOut server_name",
        "get_object_usage 11111 FanOut server_name",
        "get_oversized_plan_backlog 11111 FanOut server_name",
        "get_people 11111 FanOut",
        "get_perfmon_stats 11111 FanOut server_name",
        "get_perfmon_trend 11111 FanOut server_name",
        "get_personal_tokens 00100 StoreRequired",
        "get_pg_autovacuum_health 11111 FanOut server_name",
        "get_pg_blocking 11111 FanOut server_name",
        "get_pg_buffer_usage 11111 FanOut server_name",
        "get_pg_column_stats 11111 FanOut server_name",
        "get_pg_cpu_utilization 11111 FanOut server_name",
        "get_pg_database_stats 11111 FanOut server_name",
        "get_pg_database_trend 11111 FanOut server_name",
        "get_pg_deadlock_detail 11111 FanOut server_name",
        "get_pg_deadlocks 11111 FanOut server_name",
        "get_pg_extensions 11111 FanOut server_name",
        "get_pg_index_bloat 11111 FanOut server_name",
        "get_pg_index_usage 11111 FanOut server_name",
        "get_pg_io_stats 11111 FanOut server_name",
        "get_pg_io_trend 11111 FanOut server_name",
        "get_pg_kernel_stats 11111 FanOut server_name",
        "get_pg_lock_stats 11111 FanOut server_name",
        "get_pg_log_events 11111 FanOut server_name",
        "get_pg_logging_audit 11111 FanOut server_name",
        "get_pg_plan_capture_readiness 11111 FanOut server_name",
        "get_pg_plans 11111 FanOut server_name",
        "get_pg_predicate_stats 11111 FanOut server_name",
        "get_pg_query_duration_trend 11111 FanOut server_name",
        "get_pg_replication_slots 11111 FanOut server_name",
        "get_pg_replication_stats 11111 FanOut server_name",
        "get_pg_server_config 11111 FanOut server_name",
        "get_pg_server_config_changes 11111 FanOut server_name",
        "get_pg_session_states 11111 FanOut server_name",
        "get_pg_table_bloat 11111 FanOut server_name",
        "get_pg_top_queries 11111 FanOut server_name",
        "get_pg_wait_sampling 11111 FanOut server_name",
        "get_pg_wait_stats 11111 FanOut server_name",
        "get_pg_wait_trend 11111 FanOut server_name",
        "get_pg_wraparound_risk 11111 FanOut server_name",
        "get_pg_write_stats 11111 FanOut server_name",
        "get_pg_xmin_horizon 11111 FanOut server_name",
        "get_plan_cache_bloat 11111 FanOut server_name",
        "get_plan_corrections 11111 FanOut server_name",
        "get_plan_force_precheck 11111 FanOut server_name",
        "get_plan_xml 11111 FanOut server_name",
        "get_procedure_duration_trend 11111 FanOut server_name",
        "get_procedure_plan_xml 11111 FanOut server_name",
        "get_pvs_stats 11111 FanOut server_name",
        "get_query_duration_trend 11111 FanOut server_name",
        "get_query_fleet_summary 11111 FanOut",
        "get_query_heatmap 11111 FanOut server_name",
        "get_query_pileups 11111 FanOut server_name",
        "get_query_repro_script 11111 FanOut server_name",
        "get_query_store_aborts 11111 FanOut server_name",
        "get_query_store_clutter 11111 FanOut server_name",
        "get_query_store_duration_trend 11111 FanOut server_name",
        "get_query_store_health 11111 FanOut server_name",
        "get_query_store_hints 11111 FanOut server_name",
        "get_query_store_plan_xml 11111 FanOut server_name",
        "get_query_store_query_history 11111 FanOut server_name",
        "get_query_store_regressions 11111 FanOut server_name",
        "get_query_store_top 11111 FanOut server_name",
        "get_query_trend 11111 FanOut server_name",
        "get_read_latency 11111 FanOut",
        "get_resource_semaphore 11111 FanOut server_name",
        "get_resource_use_by_tag 11111 FanOut server_name",
        "get_running_jobs 11111 FanOut server_name",
        "get_schema_changes 11111 FanOut server_name",
        "get_server_actions 11111 FanOut",
        "get_server_config 11111 FanOut server_name",
        "get_server_config_changes 11111 FanOut server_name",
        "get_server_properties 11111 FanOut server_name",
        "get_server_settings 11111 FanOut server_name",
        "get_server_summary 11111 FanOut server_name",
        "get_server_trend 11111 FanOut server_name",
        "get_service_starts 11111 FanOut",
        "get_session_stats 11111 FanOut server_name",
        "get_slow_reads 11111 FanOut server_name",
        "get_spinlock_stats 11111 FanOut server_name",
        "get_store_checkpoints 11111 FanOut",
        "get_store_compression_health 11111 FanOut",
        "get_store_connections 11111 FanOut",
        "get_store_host 11111 FanOut",
        "get_store_job_runs 11111 FanOut",
        "get_store_lock_waits 11111 FanOut",
        "get_store_log 11111 FanOut",
        "get_store_maintenance_readiness 11111 FanOut",
        "get_store_metrics 11111 FanOut",
        "get_store_pass_steps 11111 FanOut",
        "get_store_query_history 11111 FanOut",
        "get_store_query_stats 11111 FanOut",
        "get_store_setting_changes 11111 FanOut",
        "get_store_table_health 11111 FanOut",
        "get_sweep_reports 11111 FanOut",
        "get_table_index_sizes 11111 FanOut server_name",
        "get_tempdb_trend 11111 FanOut server_name",
        "get_tool_guide 11111 AnyOneStore",
        "get_top_procedures_by_cpu 11111 FanOut server_name",
        "get_top_queries_by_cpu 11111 FanOut server_name",
        "get_trace_flag_changes 11111 FanOut server_name",
        "get_trace_flags 11111 FanOut server_name",
        "get_wait_stats 11111 FanOut server_name",
        "get_wait_trend 11111 FanOut server_name",
        "get_wait_types 11111 FanOut server_name",
        "get_waiting_tasks 11111 FanOut server_name",
        "get_web_access_groups 00100 StoreRequired",
        "kill_session 01111 StoreRequired server_name",
        "list_alert_sources 11111 FanOut",
        "list_custom_alert_rules 11111 FanOut",
        "list_custom_alert_templates 11111 AnyOneStore",
        "list_custom_views 11111 FanOut",
        "list_investigation_requests 11111 FanOut server_name",
        "list_query_tag_rules 11111 FanOut",
        "list_query_tags 11111 FanOut",
        "list_servers 11111 FanOut",
        "measure_store_table 11111 FanOut",
        "merge_deadlock_signatures 01111 StoreRequired server_name",
        "mint_personal_token 00100 StoreRequired",
        "mute_analysis_finding 01100 StoreRequired server_name",
        "new_password_key 00100 StoreRequired",
        "pause_collection 01100 StoreRequired",
        "post_investigation_message 00011 StoreRequired",
        "purge_expired_mute_rules 01100 StoreRequired",
        "purge_now 01100 StoreRequired",
        "recompile_module 01111 StoreRequired server_name",
        "record_investigation_feedback 01100 StoreRequired",
        "reject_investigation_note 01100 StoreRequired",
        "reload_config 01100 StoreRequired",
        "remove_alert_source 00100 StoreRequired",
        "remove_server 00100 StoreRequired server_name",
        "remove_webhook_signing_key 01100 StoreRequired",
        "reset_collector_schedules 01100 StoreRequired server_name",
        "reset_password_key 00100 StoreRequired",
        "restore_alert_history 01100 StoreRequired",
        "resume_collection 01100 StoreRequired",
        "revoke_personal_token 00100 StoreRequired",
        "rotate_alert_source 00100 StoreRequired",
        "rotate_web_access_group_token 00100 StoreRequired",
        "run_collectors_now 01100 StoreRequired server_name",
        "run_custom_view_panel 11111 StoreRequired",
        "save_environment_profile 01100 StoreRequired server_name",
        "send_daily_health_report_now 01100 StoreRequired",
        "send_test_notification 01100 StoreRequired",
        "set_advanced_web_access 00100 StoreRequired",
        "set_agent_actions_enabled 01100 StoreRequired",
        "set_alert_threshold_override 01111 StoreRequired server_name",
        "set_collector_databases 01100 StoreRequired server_name",
        "set_collector_frequency 01100 StoreRequired server_name",
        "set_collector_retention 01100 StoreRequired server_name",
        "set_collector_run_at 01100 StoreRequired server_name",
        "set_database_state_expected 01100 StoreRequired server_name",
        "set_mute_rule_enabled 01111 StoreRequired",
        "set_notification_route_enabled 01100 StoreRequired",
        "set_notification_route_fallback 01100 StoreRequired",
        "set_quiet_window 01100 StoreRequired server_name",
        "set_server_action 00100 StoreRequired server_name",
        "set_server_excluded_databases 01100 StoreRequired server_name",
        "set_server_xe_session_databases 01100 StoreRequired server_name",
        "start_practice_request 01100 StoreRequired",
        "submit_investigation_report 00011 StoreRequired",
        "test_connection 01100 StoreRequired server_name",
        "test_custom_alert_rule 11111 StoreRequired",
        "test_query_tag_rule 11111 FanOut",
        "unassign_query_tag 01100 StoreRequired",
        "unassign_server_tag 01100 StoreRequired",
        "undo_agent_change 01100 StoreRequired",
        "undo_plan_change 01100 StoreRequired",
        "unforce_query_store_plan 01111 StoreRequired server_name",
        "unmute_analysis_finding 01100 StoreRequired server_name",
        "update_alert_settings 01100 StoreRequired",
        "update_custom_alert_rule 01111 StoreRequired",
        "update_custom_view 01100 StoreRequired",
        "update_known_pattern 01100 StoreRequired",
        "update_mute_rule 01111 StoreRequired",
        "update_notification_channels 01100 StoreRequired",
        "update_notification_route 01100 StoreRequired",
        "update_query_tag 01100 StoreRequired",
        "update_query_tag_rule 01100 StoreRequired",
        "update_server_tag 01100 StoreRequired",
        "update_service_flags 01100 StoreRequired",
        "update_web_access_group 00100 StoreRequired",
        "validate_custom_alert_rule 11111 AnyOneStore",
        "validate_custom_view 11111 AnyOneStore",
        "wait_for_investigation_requests 11111 StoreRequired",
        "write_investigation_note 01100 StoreRequired server_name",
    };

    /// <summary>Per policy: the served count, the hash of its tools/list answer sorted by name, and the hash of its instructions, as captured at the
    /// base the per-class registration was proved against. Written out by the capture, not asserted.</summary>
    private static readonly string[] ExpectedHashes =
    {
        "off: 213 tools 7153F759705297B5A26526CEF227483A3852ED4F53AF8A8D6241949753ADFE0B instructions 0ECF401F25F759A7FC61FE7B1DB5B51D09E1DB3099E66E7D2EF6B67E972BF796",
        "writes: 298 tools 6EF6055064A0A51B18DDE1017611C9DD27758E9AA1D50D89EFF6274AE8194E75 instructions F391D2043863EDC5E24C0F7650A7CE1E1B18B91EB9D4F16627F983EC3E4E0E35",
        "writes+admin: 304 tools 23DCD3800CFB3E01BE9BE8DB13E23188696FC0CEF6A6AE8555DCDCB3ADD37B51 instructions 4E8CF545871705EC5CA479052778633C447242E307E9AF889A2F323117122D02",
        "investigations: 234 tools C0C2E12BC624AEDC5DD740FD1579D1E593DFE24B87F1C2FB7124930030C9DD14 instructions C31D4A9F002B902E55B0BA5EB35304EA883B2D27ECB95DDB6707AFBE80137B72",
        "investigations, agent mute rules off: 234 tools C0C2E12BC624AEDC5DD740FD1579D1E593DFE24B87F1C2FB7124930030C9DD14 instructions EEE200C16B48ADE8371B84445AB1E1C5E3E5E3C824C8048184536F7BCE648FC3",
    };
}
