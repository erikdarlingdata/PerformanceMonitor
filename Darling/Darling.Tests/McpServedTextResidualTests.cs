/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>#8572: the text a tool serves is plain words with grouped figures and a true window, on the strings a field check of
/// nightly 673 still found wrong.</summary>
public sealed class McpServedTextResidualTests
{
    [Fact]
    public void TheGeneratedForcePlanComment_GroupsTheCpuPerExecFigures()
    {
        var finding = new AnalysisFinding
        {
            FindingId = 1,
            AnalysisTime = new DateTime(2026, 11, 10, 12, 0, 0, DateTimeKind.Utc),
            ServerId = 42,
            ServerName = "SQL01",
            DatabaseName = "orders",
            Severity = 1.6,
            Confidence = 1.0,
            Category = "queries",
            StoryPath = "PLAN_REGRESSION",
            StoryPathHash = "hash_PLAN_REGRESSION",
            StoryText = "story",
            RootFactKey = "PLAN_REGRESSION",
            DrillDown = new Dictionary<string, object>
            {
                ["regressed_queries"] = new[]
                {
                    new Dictionary<string, object?>
                    {
                        ["database"] = "orders", ["query_id"] = 42L, ["best_plan_id"] = 7L, ["regression_factor"] = 10.0,
                        ["latest_plan_hash"] = "0xAA", ["latest_cpu_per_exec_us"] = 466737.0,
                        ["best_plan_hash"] = "0xBB", ["best_cpu_per_exec_us"] = 46673.0,
                    },
                },
            },
        };

        var script = FactRemediation.GenerateForFinding(finding);

        Assert.NotNull(script);
        Assert.Contains("(cpu/exec 466,737 us)", script, StringComparison.Ordinal);
        Assert.Contains("(cpu/exec 46,673 us)", script, StringComparison.Ordinal);
        Assert.DoesNotContain("466737", script, StringComparison.Ordinal);
    }

    [Fact]
    public void AnEmptyWindowSentence_NamesTheWindowAtTheAnchor_OnlyWhenAsOfWasGiven()
    {
        Assert.Null(McpHelpers.ResolveAsOf("2026-11-10T12:00:00Z", out var end));
        Assert.Equal(new DateTime(2026, 11, 10, 12, 0, 0, DateTimeKind.Utc), end);
        Assert.Equal("in the 24 hours ending at 2026-11-10 12:00 UTC", McpHelpers.InTheLast(CountText.Plural(24, "hour")));
        Assert.Equal("in the 1 hour ending at 2026-11-10 12:00 UTC", McpHelpers.InTheLast(CountText.Plural(1, "hour")));

        /* A call that names no anchor resets it, so the sentence reads as the window that ended now. */
        Assert.Null(McpHelpers.ResolveAsOf(null, out _));
        Assert.Equal("in the last 24 hours", McpHelpers.InTheLast(CountText.Plural(24, "hour")));
    }

    [Fact]
    public void TheUncorroboratedRouteNote_NamesNoMigrationNumber()
    {
        Assert.DoesNotMatch(@"\bV\d{2,3}\b", DarlingMcpAlertTools.UncorroboratedRouteNote);
        Assert.DoesNotMatch(@"#\d{3,}", DarlingMcpAlertTools.UncorroboratedRouteNote);
        Assert.DoesNotContain('—', DarlingMcpAlertTools.UncorroboratedRouteNote);
    }
}
