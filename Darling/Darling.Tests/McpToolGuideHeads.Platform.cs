/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3898 D3 head pins for the "Platform" family: the Darling-only self-monitoring tools that read the
/// monitoring STORE itself (Postgres/TimescaleDB size and growth, its own server log, and per-collector cost
/// on the monitored fleet), never a monitored SQL Server. No Lite twins exist for any of these three - Lite
/// has no separate store process to self-monitor. Follows the pattern in
/// <see cref="McpToolGuideHeadsDataTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsPlatformTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_store_metrics",
        "get_store_log",
        "get_collector_cost",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_store_metrics", "no server_name"),
        ("get_store_metrics", "Daily points are each day's LAST snapshot"),
        ("get_store_metrics", "job_history is ownership-filtered"),
        ("get_store_metrics", "arming by hand drops history's only copy"),
        ("get_store_metrics", "inventory.reconciled=false flags bytes no row explains"),
        ("get_store_log", "no server_name"),
        ("get_store_log", "not_collected (captures=0) means nobody read the log"),
        ("get_store_log", "NO health band"),
        ("get_store_log", "capture denominator"),
        ("get_collector_cost", "no server_name"),
        ("get_collector_cost", "sql_ms is a DURATION"),
        ("get_collector_cost", "CRITICAL on query_store"),
        ("get_collector_cost", "never read it as server-slowness"),
    ];

    [Fact]
    public void EveryConvertedHead_CarriesItsGuardrailFact_AndThePointer()
    {
        foreach (var tool in ConvertedTools)
        {
            var served = McpToolGuideTests.Served(tool);
            Assert.NotNull(served.Tail);
            Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
            Assert.True(served.Served.Length <= 620, $"{tool}: served head {served.Served.Length} is over the 620 target");
            Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"{tool}.{p.Parameter}: {p.Length} > 200"));
        }

        foreach (var (tool, fact) in HeadFacts)
        {
            Assert.Contains(fact, McpToolGuideTests.Served(tool).Served, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// get_store_metrics' TOAST and checkpointer blocks are two of its many facts and would blow the 620-char
    /// head budget on their own; they stay off the shared head and ride the tail instead, verbatim (D3/D9: a
    /// model reading only the head still gets the scope, navigation, daily-point, job_history, retention and
    /// inventory guardrails; TOAST/checkpointer detail is a `get_tool_guide` call away, not lost).
    /// </summary>
    [Fact]
    public void StoreMetrics_KeepsToastAndCheckpointerDetailOffTheHead_ButInTheTail()
    {
        var served = McpToolGuideTests.Served("get_store_metrics");
        Assert.DoesNotContain("TOAST UTILISATION", served.Served, StringComparison.Ordinal);
        Assert.DoesNotContain("CHECKPOINTER (V137)", served.Served, StringComparison.Ordinal);
        Assert.Contains("TOAST UTILISATION (V137)", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("CHECKPOINTER (V137)", served.Tail!, StringComparison.Ordinal);
    }

    /// <summary>
    /// get_store_log's per-class detail (which classes are the expected floor, which are worth reading one at
    /// a time) is the reason to call get_tool_guide after a census comes back; it stays off the head and rides
    /// the tail verbatim.
    /// </summary>
    [Fact]
    public void StoreLog_KeepsPerClassDetailOffTheHead_ButInTheTail()
    {
        var served = McpToolGuideTests.Served("get_store_log");
        Assert.DoesNotContain("crash recovery", served.Served, StringComparison.Ordinal);
        Assert.Contains("crash recovery", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("administrator terminations", served.Tail!, StringComparison.Ordinal);
    }

    /// <summary>
    /// get_collector_cost's fleet-measured store-probe share (55.4% of plan_fetch, 80.6% of text_fetch) is the
    /// number a regression investigation needs; the head states the rounded range (D9), the tail keeps the
    /// exact figures verbatim.
    /// </summary>
    [Fact]
    public void CollectorCost_HeadRoundsTheStoreProbeShare_TailKeepsTheExactFigures()
    {
        var served = McpToolGuideTests.Served("get_collector_cost");
        Assert.Contains("55-81%", served.Served, StringComparison.Ordinal);
        Assert.Contains("55.4% of plan_fetch and 80.6% of text_fetch", served.Tail!, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3898 lane L4b: the fleet-overview and server-admin tools, added to this file per the coordinator's
    /// placement rather than a new "Fleet"/"ServerAdmin" family file. get_fleet_overview is the Darling-only
    /// cross-server roll-up; add_servers / remove_server are the Darling-only WRITE surface for fleet
    /// onboarding. All three read/write the central store, never a monitored server's own state, matching the
    /// "self-monitoring and administration, not a monitored SQL Server" scope this file already covers.
    /// </summary>
    private static readonly string[] FleetAdminConvertedTools =
    [
        "get_fleet_overview",
        "add_servers",
        "remove_server",
    ];

    /// <summary>The per-tool guardrail phrase each head must state: for get_fleet_overview, what a card shows
    /// and what a grey/null value means (D9: never misread as unhealthy); for the two write tools, what they
    /// change, that there is no confirm step, and whether the change is destructive or shared.</summary>
    private static readonly (string Tool, string Fact)[] FleetAdminHeadFacts =
    [
        ("get_fleet_overview", "One pre-banded card per server"),
        ("get_fleet_overview", "collector-health fields use a separate scan, not hours_back"),
        ("get_fleet_overview", "NoSourceForEngine (non-Aurora PostgreSQL) is structural"),
        ("get_fleet_overview", "PostgreSQL memory_mb, buffer_pool_mb and threads are always null"),
        ("add_servers", "written to the shared central store immediately if new and reachable, no confirm step"),
        ("add_servers", "visible to every client"),
        ("add_servers", "one failed connection or a duplicate does not stop the rest"),
        ("add_servers", "only added servers are monitored"),
        ("add_servers", "A password or client secret is encrypted at rest and never returned"),
        ("remove_server", "immediately, no confirm step"),
        ("remove_server", "Already-collected historical data is NOT deleted"),
        ("remove_server", "a partial match is honored only if exactly one definition contains it"),
        ("remove_server", "Matches against DEFINITIONS, not the connected registry"),
    ];

    [Fact]
    public void EveryFleetAdminConvertedHead_CarriesItsGuardrailFact_AndThePointer()
    {
        foreach (var tool in FleetAdminConvertedTools)
        {
            var served = McpToolGuideTests.Served(tool);
            Assert.NotNull(served.Tail);
            Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
            Assert.True(served.Served.Length <= 620, $"{tool}: served head {served.Served.Length} is over the 620 target");
            Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"{tool}.{p.Parameter}: {p.Length} > 200"));
        }

        foreach (var (tool, fact) in FleetAdminHeadFacts)
        {
            Assert.Contains(fact, McpToolGuideTests.Served(tool).Served, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// get_fleet_overview's exhaustive cpu_source enumeration (RingBuffer / PerformanceInsights / NotCollected /
    /// NoSourceForEngine, each with its own parenthetical) would blow the 620-char head budget on its own; the
    /// head states the two outcomes that matter (a real gap vs. a structural null) and the full enumeration
    /// rides the tail verbatim, a get_tool_guide call away.
    /// </summary>
    [Fact]
    public void FleetOverview_KeepsTheFullCpuSourceEnumerationOffTheHead_ButInTheTail()
    {
        var served = McpToolGuideTests.Served("get_fleet_overview");
        Assert.DoesNotContain("RingBuffer (SQL Server", served.Served, StringComparison.Ordinal);
        Assert.DoesNotContain("PerformanceInsights (an Aurora PostgreSQL", served.Served, StringComparison.Ordinal);
        Assert.Contains("RingBuffer (SQL Server", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("PerformanceInsights (an Aurora PostgreSQL", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("blocking_severity stays", served.Tail!, StringComparison.Ordinal);
    }

    /// <summary>
    /// add_servers' full per-object field list (host/display_name/database/engine/auth/username/password/port/
    /// encrypt_mode/trust_server_certificate/read_only_intent/multi_subnet_failover) and its two-object JSON
    /// example would blow the head budget; both ride the tail verbatim (D2: the servers_json parameter's own
    /// description shrank from 395 to under 200 characters for the same reason, with its JSON example moved to
    /// the tail).
    /// </summary>
    [Fact]
    public void AddServers_KeepsThePerObjectFieldListAndJsonExampleOffTheHead_ButInTheTail()
    {
        var served = McpToolGuideTests.Served("add_servers");
        Assert.DoesNotContain("display_name (optional", served.Served, StringComparison.Ordinal);
        Assert.DoesNotContain("\"host\":\"sql01\"", served.Served, StringComparison.Ordinal);
        Assert.Contains("display_name (optional", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("\"host\":\"sql01\"", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("\"host\":\"sql01\"", served.Tail!, StringComparison.Ordinal);
    }

    /// <summary>
    /// remove_server's ambiguous-match JSON shape and the not_found/matched_in disclosures are a get_tool_guide
    /// call away; the head keeps only the rule (partial match refused unless unique) and the two outcomes that
    /// matter most: no confirm step, and history survives the definition's deletion.
    /// </summary>
    [Fact]
    public void RemoveServer_KeepsTheResponseShapesOffTheHead_ButInTheTail()
    {
        var served = McpToolGuideTests.Served("remove_server");
        Assert.DoesNotContain("status:\"ambiguous\"", served.Served, StringComparison.Ordinal);
        Assert.DoesNotContain("matched_in", served.Served, StringComparison.Ordinal);
        Assert.Contains("status:\"ambiguous\"", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("matched_in", served.Tail!, StringComparison.Ordinal);
    }
}
