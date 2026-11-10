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
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Every time-series read names the clock its stamps are on, in the text a reader is sent to for its fields: the
/// served description of an unsplit tool, the get_tool_guide tail of a split one. Lite stamps every read here in
/// UTC (collection_time and the buckets cut from it, the Extended Events event_time and deadlock_time, Query
/// Store's interval_start_time_utc, a snapshot's captured_at), the same sentence Darling's twin pins on the same
/// tool names. get_cpu_utilization is the one read on the monitored server's clock and keeps its own sentence
/// (<see cref="McpToolGuideHeadsDataTests.GetCpuUtilization_HeadNamesItsClock"/>).
/// </summary>
public sealed class McpToolGuideHeadsClocksTests
{
    private const string UtcStamps = "Every stamp here is UTC, not the monitored server's clock";
    private const string OffsetPointer = "get_server_properties has its utc_offset_minutes";

    /// <summary>The SQL Server reads whose every stamp is UTC; Darling serves each under the same name.</summary>
    private static readonly string[] UtcStampedReads =
    [
        "get_cpu_by_database",
        "get_cpu_scheduler_pressure",
        "get_wait_trend",
        "get_current_waits_trend",
        "get_memory_trend",
        "get_memory_stats",
        "get_perfmon_stats",
        "get_perfmon_trend",
        "get_file_io_trend",
        "get_tempdb_trend",
        "get_blocking_trend",
        "get_deadlock_trend",
        "get_lock_wait_trend",
        "get_query_trend",
        "get_query_duration_trend",
        "get_procedure_duration_trend",
        "get_query_store_duration_trend",
        "get_query_heatmap",
        "get_blocking_stats",
    ];

    /// <summary>The text that explains a tool's fields: the guide tail when the description is split, else the
    /// served description itself.</summary>
    internal static string FieldText(McpToolsListBudgetTests.MeasuredTool served) => served.Tail ?? served.Served;

    [Fact]
    public void EveryUtcStampedRead_NamesItsClock_WhereItsFieldsAreExplained()
    {
        var missing = new List<string>();
        foreach (var tool in UtcStampedReads)
        {
            var text = FieldText(McpToolGuideTests.Served(tool));
            if (!text.Contains(UtcStamps, StringComparison.Ordinal) || !text.Contains(OffsetPointer, StringComparison.Ordinal))
            {
                missing.Add(tool);
            }
        }

        Assert.True(missing.Count == 0, "reads whose text does not say their stamps are UTC: " + string.Join(", ", missing));
    }

    /// <summary>On a split description the sentence rides the guide tail, so the served head keeps its budget.</summary>
    [Fact]
    public void OnASplitDescription_TheClockSentenceRidesTheGuide_NotTheHead()
    {
        var split = UtcStampedReads.Select(McpToolGuideTests.Served).Where(t => t.Tail is not null).ToList();
        Assert.NotEmpty(split);
        Assert.All(split, t => Assert.DoesNotContain(UtcStamps, t.Served, StringComparison.Ordinal));
    }

    [Fact]
    public void DailySummaryRange_NamesItsDayAsUtc()
    {
        var text = FieldText(McpToolGuideTests.Served("get_daily_summary_range"));
        Assert.Contains("summary_date is a UTC day, not the monitored server's", text, StringComparison.Ordinal);
        Assert.Contains(OffsetPointer, text, StringComparison.Ordinal);
    }
}
