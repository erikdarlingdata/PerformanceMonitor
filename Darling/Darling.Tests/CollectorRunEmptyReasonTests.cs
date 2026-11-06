/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
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

    /// <summary>Review L-b: the UTC stamp in these sentences is a custom format, whose ':' is the CURRENT culture's time separator.
    /// On a host whose culture uses another one the stamp was mangled; the sentences format with the invariant culture.</summary>
    [Fact]
    public void TheUtcStamps_UseTheInvariantTimeSeparator_WhateverTheHostCulture()
    {
        var odd = (CultureInfo)CultureInfo.InvariantCulture.Clone();
        odd.DateTimeFormat.TimeSeparator = ".";
        var saved = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = odd;
            var gap = new CollectorRunFacts(2, 0, Last, 0).ProcedureStatsEmpty(6).Message;
            var traceFlags = new CollectorRunFacts(1, 0, Last, 0).NoTraceFlagsEnabled();

            Assert.Contains("2026-10-12 08:30:00 UTC", gap, StringComparison.Ordinal);
            Assert.Contains("2026-10-12 08:30:00 UTC", traceFlags, StringComparison.Ordinal);
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
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

    /// <summary>#5633 review (finding D): a read that left databases out claims only the ones the server still has, and never says the
    /// collector "recorded nothing for it". The gap and new-collector branches are unchanged by the count.</summary>
    [Fact]
    public void ProcedureStats_AfterALeftOutDatabase_ClaimsOnlyTheDatabasesTheServerStillHas()
    {
        var json = DarlingMcpDataTools.EmptyProcedureStatsStatus(new CollectorRunFacts(2, 30, Last, 0), 24, databasesLeftOut: 3);

        Assert.Equal("empty", Field(json, "status"));
        var message = Field(json, "message");
        Assert.DoesNotContain("recorded nothing for it", message, StringComparison.Ordinal);
        Assert.DoesNotContain("matches this request", message, StringComparison.Ordinal);
        Assert.Contains("No procedure ran in the last 24h in a database this server still has", message, StringComparison.Ordinal);
        /* #7538: the count of left-out databases is stated once, in the reply's databases_left_out_count and the page's strip, so the
           empty sentence does not say it again. */
        Assert.DoesNotContain("left out", message, StringComparison.Ordinal);
        Assert.DoesNotContain("database(s)", message, StringComparison.Ordinal);
        Assert.Equal(new CollectorRunFacts(2, 0, Last, 0).ProcedureStatsEmpty(24).Message, new CollectorRunFacts(2, 0, Last, 0).ProcedureStatsEmpty(24, 3).Message);
        Assert.Equal(new CollectorRunFacts(1, 1, Last, 0).ProcedureStatsEmpty(24).Message, new CollectorRunFacts(1, 1, Last, 0).ProcedureStatsEmpty(24, 3).Message);
    }

    [Fact]
    public void DatabasesLeftOut_ListsTheFirstTwentyNames_AndTheNoteCarriesTheFullCount()
    {
        var all = DatabasesLeftOut.Distinct(Enumerable.Range(0, 25).Select(i => $"Db{i:D2}").Concat(new[] { "Db00", " " }));

        Assert.Equal(25, all!.Count);
        Assert.Equal(20, DatabasesLeftOut.Named(all)!.Count);
        Assert.Contains("25 in all", DatabasesLeftOut.Note(all), StringComparison.Ordinal);
        Assert.Null(DatabasesLeftOut.Distinct(new string?[] { null, "" }));
        Assert.Null(DatabasesLeftOut.Note(null));

        var wrapped = McpHelpers.WithDatabasesLeftOut(McpHelpers.Status("empty", "m"), all)!;
        using var doc = JsonDocument.Parse(wrapped);
        Assert.Equal(20, doc.RootElement.GetProperty("databases_left_out").GetArrayLength());
        /* #7538: the reply carries the true count, so a page says "and 5 more" without reading the note's words. */
        Assert.Equal(25, doc.RootElement.GetProperty("databases_left_out_count").GetInt32());
        Assert.Equal(25, DatabasesLeftOut.Count(all));
        Assert.Null(DatabasesLeftOut.Count(null));
        Assert.Equal(McpHelpers.Status("empty", "m"), McpHelpers.WithDatabasesLeftOut(McpHelpers.Status("empty", "m"), null));
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
        Assert.Contains("counts as an empty capture", message, StringComparison.Ordinal);
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
        var afterJson = DarlingMcpConfigTools.EmptyTraceFlagsStatus(new CollectorRunFacts(1, null, Last, 0));

        Assert.Equal("empty", Field(afterJson, "status"));
        Assert.Contains("No trace flags are enabled as of", Field(afterJson, "message"), StringComparison.Ordinal);
    }

    /// <summary>#7459: "empty" means somebody looked. With no successful trace_flags run nobody did, and a newest run that did not record
    /// finding none cannot say no flag is on either, so both answer unavailable.</summary>
    [Fact]
    public void TraceFlags_EmptyReadWithNoRunOrNoRecordedNothing_IsUnavailable_NotEmpty()
    {
        var never = DarlingMcpConfigTools.EmptyTraceFlagsStatus(CollectorRunFacts.None);
        var storedRows = DarlingMcpConfigTools.EmptyTraceFlagsStatus(new CollectorRunFacts(1, null, Last, 3));

        Assert.Equal("unavailable", Field(never, "status"));
        Assert.Contains("has not completed a run yet", Field(never, "message"), StringComparison.Ordinal);
        Assert.Equal("unavailable", Field(storedRows, "status"));
        Assert.DoesNotContain("has not run yet", Field(storedRows, "message"), StringComparison.Ordinal);
    }

    /// <summary>#7459: get_running_jobs' last fallback. A good run on record is the look (empty, stating the run's time); no run is unavailable.</summary>
    [Fact]
    public void RunningJobs_EmptyRead_IsEmptyOnlyAfterAGoodRun()
    {
        var after = DarlingMcpJobTools.EmptyRunningJobsStatus(new CollectorRunFacts(1, null, Last, 0));
        var never = DarlingMcpJobTools.EmptyRunningJobsStatus(CollectorRunFacts.None);

        Assert.Equal("empty", Field(after, "status"));
        Assert.Contains("as of the running_jobs collector's latest successful run (", Field(after, "message"), StringComparison.Ordinal);
        Assert.DoesNotContain("has not run yet", Field(after, "message"), StringComparison.Ordinal);
        Assert.Equal("unavailable", Field(never, "status"));
        Assert.Contains("has not completed a run yet", Field(never, "message"), StringComparison.Ordinal);
    }

    /// <summary>#7459: get_pg_io_stats differences cumulative counters, so an empty window is a measurement only with two runs inside it.</summary>
    [Theory]
    [InlineData(2L, "empty")]
    [InlineData(5L, "empty")]
    [InlineData(1L, "unavailable")]
    [InlineData(0L, "unavailable")]
    public void PgIoStats_EmptyWindow_IsEmptyOnlyWithTwoRunsInIt(long runsInWindow, string expected)
    {
        var (status, finding) = DarlingMcpPgIoTools.EmptyWindowAnswer(new CollectorRunFacts(runsInWindow, runsInWindow, runsInWindow == 0 ? null : Last, 0), 24);

        Assert.Equal(expected, status);
        Assert.DoesNotContain("has not run yet", finding, StringComparison.Ordinal);
    }

    /// <summary>#7459: the optimization idle_databases section says unavailable, not empty, when the query stats do not cover 7 days.</summary>
    [Theory]
    [InlineData(null, 0, false, "unavailable")]
    [InlineData(null, 0, true, "empty")]
    [InlineData(null, 2, true, "ok")]
    [InlineData("gated", 0, false, "not_collected")]
    public void IdleDatabasesSection_IsUnavailableWithoutCoverage(string? gate, int rows, bool covered, string expected) =>
        Assert.Equal(expected, DarlingMcpFinOpsTools.IdleSectionStatus(gate, rows, covered));

    [Fact]
    public void RunFactsReaderSql_CountsOnlySuccessfulRuns()
    {
        Assert.Contains("status = 'SUCCESS'", DarlingCollectorRunFactsReader.LatestSuccessSql, StringComparison.Ordinal);
        Assert.Contains("status = 'SUCCESS'", DarlingCollectorRunFactsReader.RunsInWindowSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $3", DarlingCollectorRunFactsReader.LatestSuccessSql, StringComparison.Ordinal);
        /* #5633 review: an as_of read bounds the newest run at the anchor, the same text otherwise. */
        Assert.Contains("collection_time <= $4", DarlingCollectorRunFactsReader.LatestSuccessAtOrBeforeSql, StringComparison.Ordinal);
        Assert.Contains("status = 'SUCCESS'", DarlingCollectorRunFactsReader.LatestSuccessAtOrBeforeSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $3", DarlingCollectorRunFactsReader.LatestSuccessAtOrBeforeSql, StringComparison.Ordinal);
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
