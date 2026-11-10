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

namespace Darling.Tests;

/// <summary>
/// Every time-series read names the clock its stamps are on, in the text a reader is sent to for its fields: the
/// served description of an unsplit tool, the get_tool_guide tail of a split one. Darling stamps every read here
/// in UTC (collection_time and the buckets cut from it, the Extended Events event_time and deadlock_time, Query
/// Store's interval_start_time_utc, a snapshot's captured_at), so the sentence is one shared one; Lite's twin
/// pins the same sentence on its own tools. get_cpu_utilization keeps its own sentence
/// (<see cref="McpToolGuideHeadsDataTests.GetCpuUtilization_HeadNamesItsClock"/>).
/// </summary>
public sealed class McpToolGuideHeadsClocksTests
{
    private const string UtcStamps = "Every stamp here is UTC, not the monitored server's clock";
    private const string OffsetPointer = "get_server_properties has its utc_offset_minutes";

    /// <summary>The SQL Server reads whose every stamp is UTC. Each is served under the same name by Lite,
    /// except get_server_trend, which only Darling serves.</summary>
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
        "get_server_trend",
    ];

    /// <summary>Every PostgreSQL time series: the four collection_time trends and the Performance Insights CPU
    /// series, whose points are stamped sample_time.</summary>
    private static readonly string[] PostgresTrends =
    [
        "get_pg_wait_trend",
        "get_pg_query_duration_trend",
        "get_pg_io_trend",
        "get_pg_database_trend",
        "get_pg_cpu_utilization",
    ];

    /// <summary>The text that explains a tool's fields: the guide tail when the description is split, else the
    /// served description itself.</summary>
    internal static string FieldText(McpToolsListBudgetTests.MeasuredTool served) => served.Tail ?? served.Served;

    [Fact]
    public void EveryUtcStampedSqlServerRead_NamesItsClock_WhereItsFieldsAreExplained()
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

    /// <summary>A PostgreSQL target has no get_server_properties offset to point at, so its series name the clock
    /// without the pointer, each by its own stamp key (collection_time on the trends, sample_time on the CPU
    /// series).</summary>
    [Fact]
    public void EveryPostgresTimeSeries_NamesItsStampAsUtc()
    {
        var missing = PostgresTrends
            .Where(tool => !FieldText(McpToolGuideTests.Served(tool)).Contains("is UTC, not the monitored server's clock", StringComparison.Ordinal))
            .ToList();
        Assert.True(missing.Count == 0, "PostgreSQL series whose text does not say their stamp is UTC: " + string.Join(", ", missing));
        Assert.Contains("collection_time is UTC", FieldText(McpToolGuideTests.Served("get_pg_wait_trend")), StringComparison.Ordinal);
        Assert.Contains("sample_time is UTC", FieldText(McpToolGuideTests.Served("get_pg_cpu_utilization")), StringComparison.Ordinal);
    }

    /// <summary>The store's own slow-read log is stamped on the store's clock, and its guide says so. get_read_latency
    /// returns per-route sums over the window with no stamp at all, so it has no clock to name.</summary>
    [Fact]
    public void SlowReads_SayTheirInstantsAreTheStoresOwnUtc()
    {
        var text = FieldText(McpToolGuideTests.Served("get_slow_reads"));
        Assert.Contains("Every instant is UTC from the store's own clock", text, StringComparison.Ordinal);
    }
}
