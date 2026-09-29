/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.McpServerClockTestSupport;

namespace Darling.Tests;

/// <summary>
/// #4793: <c>get_default_trace_events</c> no longer subtracts the ONE newest offset in SQL. The stored
/// server-local event time is converted in C# with the server's <see cref="ServerClock"/>, so an event on
/// either side of a daylight saving change lands at its real UTC time. The SQL keeps the offset only as a
/// pre-filter an hour wider than the window on each side, and the window is applied exactly after the
/// conversion. US Eastern springs forward on 2026-03-08 (07:00Z) and falls back on 2026-11-01 (06:00Z). The
/// reader is fed a <see cref="DataTable"/> in the SELECT's column order, as
/// <c>ViewerDefaultTraceServerClockTests</c> does for the viewer.
/// </summary>
public sealed class DarlingDefaultTraceReaderServerClockTests
{
    /* Column 0 is the server-local event time; 4 is database_name, which carries a label so a test can say
       which event came back. The other columns stay NULL. */
    private static DataTable NewTable() => Table(15, 0);

    private static void AddEvent(DataTable table, DateTime? local, string label) =>
        AddRow(table, (0, local.HasValue ? (object)local.Value : DBNull.Value), (1, "Data File Auto Grow"), (4, label));

    private static async Task<List<DarlingDefaultTraceReader.DefaultTraceEventRow>> ReadAsync(
        DataTable table, ServerClock clock, DateTime startUtc, DateTime endUtc)
    {
        using var reader = table.CreateDataReader();
        return await DarlingDefaultTraceReader.ReadEventRowsAsync(reader, clock, startUtc, endUtc, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task AnEventStoredAtServerLocalTime_OnEachSideOfAChange_LandsAtTheRightUtcTime()
    {
        var table = NewTable();
        AddEvent(table, Naive(2026, 3, 7, 8), "before-spring");
        AddEvent(table, Naive(2026, 3, 9, 8), "after-spring");
        AddEvent(table, Naive(2026, 10, 31, 8), "before-fall");
        AddEvent(table, Naive(2026, 11, 2, 8), "after-fall");

        /* The newest snapshot says -240 (daylight time). */
        var rows = await ReadAsync(table, Eastern(snapshotOffsetMinutes: -240), Naive(2026, 1, 1, 0), Naive(2026, 12, 31, 0));

        /* Standard time (-5 h), daylight time (-4 h), daylight time, standard time. One subtracted offset puts
           two of these four an hour off, whichever offset the latest snapshot happened to carry. */
        var byLabel = rows.ToDictionary(r => r.DatabaseName!, r => r.EventTimeUtc);
        Assert.Equal(Naive(2026, 3, 7, 13), byLabel["before-spring"]);
        Assert.Equal(Naive(2026, 3, 9, 12), byLabel["after-spring"]);
        Assert.Equal(Naive(2026, 10, 31, 12), byLabel["before-fall"]);
        Assert.Equal(Naive(2026, 11, 2, 13), byLabel["after-fall"]);
    }

    [Fact]
    public async Task AnEventInASkippedOrRepeatedLocalHour_DoesNotThrow()
    {
        var table = NewTable();
        AddEvent(table, Naive(2026, 3, 8, 2, 30), "skipped");
        AddEvent(table, Naive(2026, 11, 1, 1, 30), "repeated");

        var rows = await ReadAsync(table, Eastern(), Naive(2026, 1, 1, 0), Naive(2026, 12, 31, 0));

        var byLabel = rows.ToDictionary(r => r.DatabaseName!, r => r.EventTimeUtc);
        Assert.Equal(Naive(2026, 3, 8, 7, 30), byLabel["skipped"]);
        Assert.Equal(Naive(2026, 11, 1, 5, 30), byLabel["repeated"]);
    }

    [Fact]
    public async Task AWindowWithItsEdgeNearTheFallBack_KeepsExactlyTheEventsInsideIt()
    {
        var table = NewTable();
        AddEvent(table, Naive(2026, 11, 1, 0, 59), "00-59");  // 04:59Z, before the window
        AddEvent(table, Naive(2026, 11, 1, 1, 15), "01-15");  // 05:15Z, before the window
        AddEvent(table, Naive(2026, 11, 1, 1, 45), "01-45");  // 05:45Z, inside
        AddEvent(table, Naive(2026, 11, 1, 2, 10), "02-10");  // 07:10Z, inside (standard time again)
        AddEvent(table, Naive(2026, 11, 1, 2, 40), "02-40");  // 07:40Z, after the window

        var rows = await ReadAsync(table, Eastern(snapshotOffsetMinutes: -240), Naive(2026, 11, 1, 5, 30), Naive(2026, 11, 1, 7, 30));

        Assert.Equal(["02-10", "01-45"], rows.Select(r => r.DatabaseName).ToArray());
    }

    [Fact]
    public async Task AWindowWithItsEdgeNearTheSpringForward_KeepsExactlyTheEventsInsideIt_EdgesIncluded()
    {
        var table = NewTable();
        AddEvent(table, Naive(2026, 3, 8, 1, 0), "01-00");   // 06:00Z, before the window
        AddEvent(table, Naive(2026, 3, 8, 1, 30), "01-30");  // 06:30Z, on the start edge
        AddEvent(table, Naive(2026, 3, 8, 3, 15), "03-15");  // 07:15Z, inside
        AddEvent(table, Naive(2026, 3, 8, 4, 30), "04-30");  // 08:30Z, on the end edge
        AddEvent(table, Naive(2026, 3, 8, 5, 0), "05-00");   // 09:00Z, after the window

        var rows = await ReadAsync(table, Eastern(snapshotOffsetMinutes: -300), Naive(2026, 3, 8, 6, 30), Naive(2026, 3, 8, 8, 30));

        Assert.Equal(["04-30", "03-15", "01-30"], rows.Select(r => r.DatabaseName).ToArray());
    }

    [Fact]
    public async Task TheEvents_ComeBackNewestFirstByRealUtcTime_WhateverOrderTheRowsArriveIn()
    {
        var table = NewTable();
        AddEvent(table, Naive(2026, 11, 1, 0, 30), "oldest");
        AddEvent(table, Naive(2026, 11, 1, 3, 0), "newest");
        AddEvent(table, Naive(2026, 11, 1, 2, 30), "middle");

        var rows = await ReadAsync(table, Eastern(), Naive(2026, 11, 1, 0), Naive(2026, 11, 2, 0));

        Assert.Equal(["newest", "middle", "oldest"], rows.Select(r => r.DatabaseName).ToArray());
    }

    [Fact]
    public async Task AFixedOffsetServerConvertsWithoutAZone_AndAnEventWithNoTimeIsDropped()
    {
        var table = NewTable();
        AddEvent(table, null, "no-time");
        AddEvent(table, Naive(2026, 11, 2, 8), "dated");

        var rows = await ReadAsync(table, ServerClock.FixedOffset(-240), Naive(2026, 1, 1, 0), Naive(2026, 12, 31, 0));

        /* An event with no time cannot be inside a window (the SQL's predicate never returns one). */
        var only = Assert.Single(rows);
        Assert.Equal("dated", only.DatabaseName);
        Assert.Equal(Naive(2026, 11, 2, 12), only.EventTimeUtc);
    }

    /// <summary>
    /// The SQL returns the raw local time and keeps the offset only in the window pre-filter, an hour wider
    /// than the window on each side (the offset in force when the event happened can differ from the newest
    /// one by an hour). The read has no LIMIT, so the extra hour drops nothing: the exact window is applied in
    /// C# after the conversion. The <c>collection_time</c> floor still sits on the caller's start.
    /// </summary>
    [Fact]
    public void TheSql_ReturnsTheRawLocalTime_AndPreFiltersAnHourWideOnEachSide()
    {
        var sql = DarlingDefaultTraceReader.EventsByWindowSql;

        Assert.Contains("dte.event_time AS event_time_local,", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("AS event_time_utc", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT", sql, StringComparison.Ordinal);

        Assert.Contains("dte.event_time - make_interval(mins => svr.offset_minutes) >= $2 - interval '1 hour'", sql, StringComparison.Ordinal);
        Assert.Contains("dte.event_time - make_interval(mins => svr.offset_minutes) <= $3 + interval '1 hour'", sql, StringComparison.Ordinal);
        Assert.Contains("dte.collection_time >= $4", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY event_time_local DESC", sql, StringComparison.Ordinal);

        /* The offset appears only in the two pre-filter bounds: never as a projected or ordered value. */
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(sql, "make_interval").Count);
    }
}
