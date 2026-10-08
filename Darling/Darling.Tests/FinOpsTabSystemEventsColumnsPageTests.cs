/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Source pins for the System Events tab's parsed-row grids: each grid's column keys are fields its read
/// returns, and the wide grids show every field the read returns, in the desktop grid's order.
/// </summary>
public sealed class FinOpsTabSystemEventsColumnsPageTests
{
    private static string Tab() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js")
            .ReplaceLineEndings("\n");

    private static string Source(string file) =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", file).ReplaceLineEndings("\n");

    /// <summary>The keys of a top-level column constant, in order.</summary>
    private static List<string> Keys(string constName)
    {
        var tab = Tab();
        var start = tab.IndexOf("const " + constName + " = [", StringComparison.Ordinal);
        Assert.True(start >= 0, constName + " not found");
        var end = tab.IndexOf("\n];", start, StringComparison.Ordinal);
        return Regex.Matches(tab[start..end], "\\bkey: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToList();
    }

    /// <summary>The field names of the anonymous object a read projects with <c>.Select(r =&gt; new</c>.</summary>
    private static List<string> Projection(string file, string tool)
    {
        var src = Source(file);
        var start = src.IndexOf("Name = \"" + tool + "\"", StringComparison.Ordinal);
        Assert.True(start >= 0, tool + " not found");
        var next = src.IndexOf("McpServerTool(Name", start + 10, StringComparison.Ordinal);
        var seg = next > 0 ? src[start..next] : src[start..];
        var sel = seg.IndexOf("Select(r =>", StringComparison.Ordinal);
        Assert.True(sel >= 0, tool + " has no row projection");
        var close = seg.IndexOf("\n                })", sel, StringComparison.Ordinal);
        if (close < 0) close = seg.IndexOf("\n                }", sel, StringComparison.Ordinal);
        return Regex.Matches(seg[sel..close], "^\\s+([a-z_]+) = ", RegexOptions.Multiline).Select(m => m.Groups[1].Value).ToList();
    }

    public static IEnumerable<object[]> Grids() => new[]
    {
        new object[] { "SEVERE_ERROR_COLUMNS", "DarlingMcpHealthParserTools.cs", "get_health_parser_severe_errors" },
        new object[] { "SCHEDULER_ISSUE_COLUMNS", "DarlingMcpHealthParserTools.cs", "get_health_parser_scheduler_issues" },
        new object[] { "IO_ISSUE_COLUMNS", "DarlingMcpHealthParserTools.cs", "get_health_parser_io_issues" },
        new object[] { "CPU_TASK_COLUMNS", "DarlingMcpHealthParserTools.cs", "get_health_parser_cpu_tasks" },
        new object[] { "MEMORY_CONDITION_COLUMNS", "DarlingMcpHealthParserTools.cs", "get_health_parser_memory_conditions" },
        new object[] { "MEMORY_BROKER_COLUMNS", "DarlingMcpHealthParserTools.cs", "get_health_parser_memory_broker" },
        new object[] { "MEMORY_OOM_COLUMNS", "DarlingMcpHealthParserTools.cs", "get_health_parser_memory_node_oom" },
        new object[] { "SIGNIFICANT_WAIT_COLUMNS", "DarlingMcpHealthParserTools.cs", "get_health_parser_significant_waits" },
        new object[] { "DEFAULT_TRACE_COLUMNS", "DarlingMcpDefaultTraceTools.cs", "get_default_trace_events" },
    };

    [Theory]
    [MemberData(nameof(Grids))]
    public void EveryColumnKeyIsAFieldTheReadReturns(string constName, string file, string tool)
    {
        var keys = Keys(constName);
        var fields = Projection(file, tool);
        Assert.NotEmpty(keys);
        Assert.NotEmpty(fields);
        Assert.Empty(keys.Except(fields));
        Assert.Equal(keys.Count, keys.Distinct().Count());
    }

    [Fact]
    public void TheMemoryConditionGridShowsAllThirtyTwoFieldsInTheDesktopOrder()
    {
        Assert.Equal(new[]
        {
            "event_time", "last_notification", "name", "out_of_memory_exceptions", "is_any_pool_out_of_memory",
            "process_out_of_memory_period", "available_physical_memory_gb", "available_virtual_memory_gb",
            "available_paging_file_gb", "working_set_gb", "percent_of_committed_memory_in_ws", "page_faults",
            "system_physical_memory_high", "system_physical_memory_low", "process_physical_memory_low",
            "process_virtual_memory_low", "vm_reserved_gb", "vm_committed_gb", "locked_pages_allocated",
            "large_pages_allocated", "emergency_memory_gb", "emergency_memory_in_use_gb", "target_committed_gb",
            "current_committed_gb", "pages_allocated", "pages_reserved", "pages_free", "pages_in_use",
            "page_alloc_potential", "numa_growth_phase", "last_oom_factor", "last_os_error",
        }, Keys("MEMORY_CONDITION_COLUMNS"));
    }

    [Fact]
    public void TheNodeOomGridShowsAllTwentyEightFieldsInTheDesktopOrder()
    {
        Assert.Equal(new[]
        {
            "event_time", "node_id", "memory_node_id", "memory_utilization_pct", "total_physical_memory_kb",
            "available_physical_memory_kb", "total_page_file_kb", "available_page_file_kb",
            "total_virtual_address_space_kb", "available_virtual_address_space_kb", "target_kb", "reserved_kb",
            "committed_kb", "shared_committed_kb", "awe_kb", "pages_kb", "failure_type", "failure_value",
            "resources", "factor_text", "factor_value", "last_error", "pool_metadata_id", "is_process_in_job",
            "is_system_physical_memory_high", "is_system_physical_memory_low", "is_process_physical_memory_low",
            "is_process_virtual_memory_low",
        }, Keys("MEMORY_OOM_COLUMNS"));
    }

    [Fact]
    public void TheDefaultTraceGridShowsTheDesktopFourteenColumns()
    {
        Assert.Equal(new[]
        {
            "event_time", "category", "event_name", "database_name", "object_name", "login_name", "host_name",
            "application_name", "spid", "duration_ms", "growth_mb", "severity", "error_number", "text_data",
        }, Keys("DEFAULT_TRACE_COLUMNS"));
    }

    [Fact]
    public void TheOtherParsedGridsShowTheDesktopColumns()
    {
        Assert.Equal(new[] { "event_time", "error_number", "severity", "state", "database_name", "database_id", "message" }, Keys("SEVERE_ERROR_COLUMNS"));
        Assert.Equal(new[] { "event_time", "sql_cpu_utilization", "other_process_cpu", "system_idle", "memory_utilization", "page_faults", "working_set_delta_mb" }, Keys("SCHEDULER_ISSUE_COLUMNS"));
        Assert.Equal(new[] { "event_time", "broker_id", "broker", "notification", "pool_metadata_id", "delta_time", "memory_ratio", "new_target", "overall", "rate", "currently_predicated", "currently_allocated", "previously_allocated" }, Keys("MEMORY_BROKER_COLUMNS"));
        Assert.Equal(new[] { "event_time", "state", "max_workers", "workers_created", "workers_idle", "tasks_completed_within_interval", "pending_tasks", "oldest_pending_task_waiting_time", "has_unresolvable_deadlock_occurred", "has_deadlocked_schedulers_occurred", "did_blocking_occur" }, Keys("CPU_TASK_COLUMNS"));
    }
}
