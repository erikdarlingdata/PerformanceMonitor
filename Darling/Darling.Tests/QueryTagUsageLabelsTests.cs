/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6878: the Resource use by query tag screens (the Darling viewer's dialog and Lite's grid) draw the words the web Query Tag Usage
/// page uses for a section heading and a column header, never the MCP field names (plan_cache, cpu_ms).
/// </summary>
public sealed class QueryTagUsageLabelsTests
{
    private static readonly QueryTagRollupSection[] AllSections =
    [
        QueryTagRollupSection.PlanCache, QueryTagRollupSection.QueryStore, QueryTagRollupSection.Procedures,
        QueryTagRollupSection.BlockingReports, QueryTagRollupSection.BlockingSnapshots, QueryTagRollupSection.Deadlocks,
    ];

    [Theory]
    [InlineData("plan_cache", "Plan cache")]
    [InlineData("query_store", "Query Store")]
    [InlineData("procedures", "Procedures")]
    [InlineData("blocking_reports", "Blocking reports")]
    [InlineData("blocking_snapshots", "Blocking snapshots")]
    [InlineData("deadlocks", "Deadlocks")]
    public void SectionTitle_IsTheWebPagesHeading(string name, string expected) =>
        Assert.Equal(expected, QueryTagUsageLabels.SectionTitle(name));

    [Theory]
    [InlineData("cpu_ms", "CPU (ms)")]
    [InlineData("duration_ms", "Duration (ms)")]
    [InlineData("logical_reads", "Logical reads")]
    [InlineData("logical_writes", "Logical writes")]
    [InlineData("executions", "Executions")]
    [InlineData("peak_memory_pages", "Peak memory (pages)")]
    [InlineData("blocked_ms", "Blocked (ms)")]
    [InlineData("caused_ms", "Caused (ms)")]
    [InlineData("blocked_reports", "Blocked reports")]
    [InlineData("caused_reports", "Caused reports")]
    [InlineData("blocked_snapshots", "Blocked snapshots")]
    [InlineData("caused_snapshots", "Caused snapshots")]
    [InlineData("deadlocks", "Deadlocks")]
    [InlineData("processes", "Processes")]
    public void MeasureLabel_IsTheWebPagesColumnHeader(string name, string expected) =>
        Assert.Equal(expected, QueryTagUsageLabels.MeasureLabel(name));

    [Fact]
    public void EverySectionAndMeasureTheStoreServes_HasALabel_ThatIsNotAFieldName()
    {
        foreach (var section in AllSections)
        {
            Assert.DoesNotContain('_', QueryTagUsageLabels.SectionTitle(section.Name));
            foreach (var measure in section.Measures)
            {
                Assert.DoesNotContain('_', QueryTagUsageLabels.MeasureLabel(measure.Name));
            }
        }
    }

    [Fact]
    public void ANameWithNoLabel_IsReturnedAsItCame() =>
        Assert.Equal("brand_new_measure", QueryTagUsageLabels.MeasureLabel("brand_new_measure"));
}
