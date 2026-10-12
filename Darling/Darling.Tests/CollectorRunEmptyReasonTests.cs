/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5633 items 3, 19 and 9. An empty read says WHY from the collector's own run history, and the one shared rule
/// (<see cref="CollectorRunFacts"/>, <see cref="ConfigValuesMatch"/>) is what every surface calls. Each assertion fails
/// on the old shape: the old procedure text was returned for every empty page, the old trace-flag text claimed fewer
/// than two snapshots whatever the log said, and the old match flag was plain equality.
/// </summary>
public sealed class CollectorRunEmptyReasonTests
{
    private static readonly DateTime Last = new(2026, 10, 12, 8, 30, 0, DateTimeKind.Utc);

    private static string Field(string json, string name) =>
        JsonDocument.Parse(json).RootElement.GetProperty(name).GetString()!;

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public void ProcedureStats_BeforeTwoSuccessfulRuns_KeepsTheTwoCyclesReason(int runs)
    {
        var json = DarlingMcpDataTools.EmptyProcedureStatsStatus(new CollectorRunFacts(runs, runs, Last, 0), 24);

        Assert.Equal("unavailable", Field(json, "status"));
        Assert.Contains("two collection cycles", Field(json, "message"), StringComparison.Ordinal);
    }

    [Fact]
    public void ProcedureStats_AfterTheCollectorHasRun_SaysNoProcedureExecuted()
    {
        var json = DarlingMcpDataTools.EmptyProcedureStatsStatus(new CollectorRunFacts(2, 30, Last, 0), 24);

        Assert.Equal("empty", Field(json, "status"));
        var message = Field(json, "message");
        Assert.Contains("No procedure executed in the last 24h", message, StringComparison.Ordinal);
        Assert.DoesNotContain("two collection cycles", message, StringComparison.Ordinal);
    }

    [Fact]
    public void ProcedureStats_WithNoRunInTheWindow_IsAGapNotAnAllClear()
    {
        var json = DarlingMcpDataTools.EmptyProcedureStatsStatus(new CollectorRunFacts(2, 0, Last, 0), 6);

        Assert.Equal("unavailable", Field(json, "status"));
        Assert.Contains("no successful run inside the last 6h", Field(json, "message"), StringComparison.Ordinal);
    }

    [Fact]
    public void TraceFlagChanges_AfterARunThatStoredNothing_SaysNoFlagIsEnabled()
    {
        var facts = new CollectorRunFacts(1, null, Last, 0);

        var json = DarlingMcpConfigHistoryTools.NoTraceFlagChanges("srv", 168, 0, McpWindowNotice.Unavailable, facts);

        Assert.Equal("empty", Field(json, "status"));
        var message = Field(json, "message");
        Assert.Contains("No trace flags are enabled as of", message, StringComparison.Ordinal);
        Assert.DoesNotContain("fewer than two", message, StringComparison.Ordinal);
        Assert.Equal(0, JsonDocument.Parse(json).RootElement.GetProperty("hints").GetProperty("snapshot_count").GetInt32());
    }

    [Fact]
    public void TraceFlagChanges_AfterFlagsWereCapturedThenNone_SaysWhyTheRemovalIsNotListed()
    {
        var facts = new CollectorRunFacts(2, null, Last, 0);

        var message = Field(DarlingMcpConfigHistoryTools.NoTraceFlagChanges("srv", 168, 1, McpWindowNotice.Unavailable, facts), "message");

        Assert.Contains("No trace flags are enabled as of", message, StringComparison.Ordinal);
        Assert.Contains("turning off the last flag does not appear as a change", message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0, null, 0)]
    [InlineData(1, 4L, 1)]
    public void TraceFlagChanges_WithoutAnEmptyRun_KeepTheFewerThanTwoSnapshotsText(long runs, long? rows, int snapshots)
    {
        var facts = new CollectorRunFacts(runs, null, runs == 0 ? null : Last, rows);

        var message = Field(DarlingMcpConfigHistoryTools.NoTraceFlagChanges("srv", 168, snapshots, McpWindowNotice.Unavailable, facts), "message");

        Assert.Contains("fewer than two config snapshots", message, StringComparison.Ordinal);
    }

    [Fact]
    public void TraceFlags_EmptyReadAfterARunThatStoredNothing_SaysNoFlagIsEnabled()
    {
        var after = Field(DarlingMcpConfigTools.EmptyTraceFlagsStatus(new CollectorRunFacts(1, null, Last, 0)), "message");
        var never = Field(DarlingMcpConfigTools.EmptyTraceFlagsStatus(CollectorRunFacts.None), "message");

        Assert.Contains("No trace flags are enabled as of", after, StringComparison.Ordinal);
        Assert.Contains("config collector has not run yet", never, StringComparison.Ordinal);
    }

    [Fact]
    public void RunFactsReaderSql_CountsOnlySuccessfulRuns()
    {
        Assert.Contains("status = 'SUCCESS'", DarlingCollectorRunFactsReader.LatestSuccessSql, StringComparison.Ordinal);
        Assert.Contains("status = 'SUCCESS'", DarlingCollectorRunFactsReader.RunsInWindowSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $3", DarlingCollectorRunFactsReader.LatestSuccessSql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("min server memory (MB)", 0, 16, true)]
    [InlineData("min server memory (MB)", 0, 0, true)]
    [InlineData("min server memory (MB)", 0, 32, false)]
    [InlineData("min server memory (MB)", 16, 0, false)]
    [InlineData("max server memory (MB)", 0, 16, false)]
    [InlineData("max degree of parallelism", 8, 4, false)]
    public void MinServerMemoryZeroRunsAsSixteen_OnlyForThatSetting_OnEverySurface(string name, long configured, long inUse, bool expected)
    {
        Assert.Equal(expected, ConfigValuesMatch.Matches(name, configured, inUse));
        Assert.Equal(expected, new DarlingCurrentConfigReader.ServerConfigReadRow(name, configured, inUse, true, true).ValuesMatch);
        Assert.Equal(expected, new ServerConfigRow { ConfigurationName = name, ValueConfigured = configured, ValueInUse = inUse }.ValuesMatch);
    }
}
