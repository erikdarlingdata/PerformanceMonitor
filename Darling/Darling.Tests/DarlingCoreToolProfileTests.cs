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
using ModelContextProtocol.Server;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the <c>/core</c> profile's closure (#3898 D7, Erik's ruling): the four entry tools plus every tool a
/// <c>next_tools</c> recommendation can name, computed by <see cref="DarlingCoreToolProfile"/> from the SAME
/// tables that build those recommendations, never hand-kept here. Erik's ruling: ship <c>/core</c> only while
/// the count sits inside [70, 90].
/// </summary>
public sealed class DarlingCoreToolProfileTests
{
    [Fact]
    public void EveryClosureNameIsARegisteredDarlingTool()
    {
        var registered = McpToolsListBudgetTests.Measure().Tools
            .Select(t => t.Name)
            .ToHashSet(StringComparer.Ordinal);

        var unregistered = DarlingCoreToolProfile.Closure
            .Where(name => !registered.Contains(name))
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToList();

        Assert.True(
            unregistered.Count == 0,
            "/core names these tools, but none of them is a tool the MCP host actually registers: "
            + string.Join(", ", unregistered));
    }

    [Fact]
    public void ClosureCountIsInsideTheRuledBand()
    {
        // D7: below 70 or above 90, /core does not ship — report the count, seed and sources under Handoff
        // instead. This is the tripwire, not the pin: ClosureIsPinned below pins the exact set so any
        // change to it is a reviewed diff.
        Assert.InRange(DarlingCoreToolProfile.Closure.Count, 70, 90);
    }

    [Fact]
    public void IsCorePath_MatchesTheCoreSegmentOnly()
    {
        Assert.True(DarlingCoreToolProfile.IsCorePath("/core"));
        Assert.True(DarlingCoreToolProfile.IsCorePath("/core/"));
        Assert.False(DarlingCoreToolProfile.IsCorePath("/"));
        Assert.False(DarlingCoreToolProfile.IsCorePath(""));
        Assert.False(DarlingCoreToolProfile.IsCorePath("/corex"));
        Assert.False(DarlingCoreToolProfile.IsCorePath("/coreish"));
    }

    /// <summary>
    /// #3898 D7's hardest security requirement: <c>/core</c> must be a REAL subset, not a <c>tools/list</c>
    /// filter. Run through the SAME <see cref="McpServerPrimitiveCollection{T}"/> shape
    /// <c>ConfigureSessionOptions</c> hands <see cref="DarlingCoreToolProfile.FilterToolCollection"/>, built
    /// from the REAL tool set the host registers (<see cref="McpToolsListBudgetTests.BuildServedTools"/>), not a
    /// fabricated stand-in. A tool outside the closure is ABSENT from the result, which is what makes it
    /// unreachable to the SDK's <c>tools/call</c> dispatch — that dispatch consults exactly this collection
    /// before falling back to any handler, and Darling registers no fallback <c>CallToolHandler</c>.
    /// </summary>
    [Fact]
    public void FilterToolCollection_RemovesToolsOutsideTheClosure_ItDoesNotJustHideThem()
    {
        var (tools, _, _) = McpToolsListBudgetTests.BuildServedTools();
        var allTools = new McpServerPrimitiveCollection<McpServerTool>();
        foreach (var tool in tools)
            allTools.Add(tool);

        Assert.True(
            allTools.Count > DarlingCoreToolProfile.Closure.Count,
            "the full Darling tool set should be strictly larger than /core's closure, or this test cannot prove removal");

        var filtered = DarlingCoreToolProfile.FilterToolCollection(allTools);
        var filteredNames = filtered.Select(t => t.ProtocolTool.Name).ToHashSet(StringComparer.Ordinal);

        Assert.True(
            filteredNames.SetEquals(DarlingCoreToolProfile.Closure),
            "the filtered collection's tool names should be exactly the closure, no more and no fewer");
        Assert.True(filtered.Count < allTools.Count);

        var excludedName = allTools
            .Select(t => t.ProtocolTool.Name)
            .First(name => !DarlingCoreToolProfile.Closure.Contains(name));

        Assert.DoesNotContain(filtered, t => t.ProtocolTool.Name == excludedName);
    }

    [Fact]
    public void FilterToolCollection_NullToolCollection_ReturnsEmptyRatherThanThrowing()
    {
        var filtered = DarlingCoreToolProfile.FilterToolCollection(null);

        Assert.Empty(filtered);
    }

    /// <summary>/core serves reads only. The closure comes from the next_tools tables, so a future row naming a
    /// write tool would widen /core with no other signal. Every Darling write tool starts with one of these
    /// verbs, and the first assertion keeps the check from passing vacuously.</summary>
    [Fact]
    public void ClosureHoldsNoWriteTool()
    {
        string[] writeVerbs = ["create_", "update_", "delete_", "set_", "add_", "remove_", "mute_"];
        var writers = McpToolsListBudgetTests.Measure().Tools
            .Select(t => t.Name)
            .Where(name => writeVerbs.Any(verb => name.StartsWith(verb, StringComparison.Ordinal)))
            .ToList();

        Assert.NotEmpty(writers);
        Assert.DoesNotContain(writers, DarlingCoreToolProfile.Closure.Contains);
    }

    /// <summary>The shared instructions count and describe every tool on /. A /core session leads with a note
    /// that says what it serves, then keeps the full text unchanged.</summary>
    [Fact]
    public void CoreInstructions_LeadWithTheCoreNote_AndKeepTheFullText()
    {
        const string full = "## Tools\n\nThis server exposes every tool.";

        var core = DarlingCoreToolProfile.CoreInstructions(full);

        Assert.StartsWith("## This is the /core endpoint", core, StringComparison.Ordinal);
        Assert.Contains($"It serves {DarlingCoreToolProfile.Closure.Count} of this server's tools", core, StringComparison.Ordinal);
        Assert.EndsWith("\n\n" + full, core, StringComparison.Ordinal);
        Assert.Equal(DarlingCoreToolProfile.CoreNote, DarlingCoreToolProfile.CoreInstructions(null));
    }

    [Fact]
    public void ClosureIsPinned()
    {
        var actual = DarlingCoreToolProfile.Closure.OrderBy(name => name, StringComparer.Ordinal).ToList();

        Assert.Equal(ExpectedClosure, actual);
    }

    /// <summary>
    /// The pinned <c>/core</c> closure, sorted ordinal. A deliberate change to
    /// <see cref="DarlingCoreToolProfile"/>'s sources (a new <c>next_tools</c> row, a new entry tool) updates
    /// this list in the same PR, with the reason.
    /// </summary>
    private static readonly List<string> ExpectedClosure =
    [
        "analyze_query_plan",
        "analyze_query_store_plan",
        "analyze_server",
        "audit_config",
        "compare_analysis",
        "get_active_queries",
        "get_ag_health",
        "get_analysis_facts",
        "get_blocking",
        "get_blocking_trend",
        "get_collection_health",
        "get_collection_log",
        "get_cpu_utilization",
        "get_database_config_changes",
        "get_database_sizes",
        "get_deadlock_detail",
        "get_deadlock_trend",
        "get_deadlocks",
        "get_file_io_stats",
        "get_file_io_trend",
        "get_fleet_overview",
        "get_latch_stats",
        "get_memory_clerks",
        "get_memory_grants",
        "get_memory_pressure_events",
        "get_memory_stats",
        "get_perfmon_trend",
        "get_pg_autovacuum_health",
        "get_pg_blocking",
        "get_pg_buffer_usage",
        "get_pg_column_stats",
        "get_pg_cpu_utilization",
        "get_pg_database_stats",
        "get_pg_database_trend",
        "get_pg_deadlock_detail",
        "get_pg_deadlocks",
        "get_pg_extensions",
        "get_pg_index_bloat",
        "get_pg_index_usage",
        "get_pg_io_stats",
        "get_pg_io_trend",
        "get_pg_kernel_stats",
        "get_pg_lock_stats",
        "get_pg_log_events",
        "get_pg_plans",
        "get_pg_predicate_stats",
        "get_pg_query_duration_trend",
        "get_pg_replication_slots",
        "get_pg_replication_stats",
        "get_pg_server_config",
        "get_pg_server_config_changes",
        "get_pg_session_states",
        "get_pg_table_bloat",
        "get_pg_top_queries",
        "get_pg_wait_sampling",
        "get_pg_wait_stats",
        "get_pg_wait_trend",
        "get_pg_wraparound_risk",
        "get_pg_write_stats",
        "get_pg_xmin_horizon",
        "get_query_duration_trend",
        "get_query_store_health",
        "get_query_store_top",
        "get_query_trend",
        "get_running_jobs",
        "get_server_config_changes",
        "get_session_stats",
        "get_tempdb_trend",
        "get_tool_guide",
        "get_top_queries_by_cpu",
        "get_trace_flag_changes",
        "get_wait_stats",
        "get_wait_trend",
        "get_waiting_tasks",
        "list_servers"
    ];
}
