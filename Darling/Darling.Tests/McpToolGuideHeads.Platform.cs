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
}
