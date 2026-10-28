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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6831: the notes the desktop Viewer's Query Tag Usage view puts under a section say the hourly edges in the viewer's own words and clock,
/// the way the web page does: no raw 100 ns ISO stamp and no <c>collect.</c> relation name, and every instant through the display mode and
/// time zone the person chose.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class QueryTagUsageNotesTests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;
    private readonly ServerClock _savedClock = ViewerTimeHelper.ActiveServerClock;

    public QueryTagUsageNotesTests()
    {
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        ViewerTimeHelper.ActiveServerClock = ServerClock.Utc;
    }

    public void Dispose()
    {
        ViewerTimeHelper.CurrentDisplayMode = _savedMode;
        ViewerTimeHelper.ActiveServerClock = _savedClock;
    }

    private static DateTime Utc(int day, int hour, int minute, int second = 0) => new(2026, 10, day, hour, minute, second, DateTimeKind.Utc);

    /// <summary>What the MCP note looked like: an ISO stamp at 100 ns precision, a Z, a T between the date and the time.</summary>
    private static readonly Regex Iso = new(@"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}|\d{2}:\d{2}:\d{2}\.\d{3,7}", RegexOptions.Compiled);

    private static void AssertPlain(string note)
    {
        Assert.DoesNotMatch(Iso, note);
        Assert.DoesNotContain("collect.", note, StringComparison.Ordinal);
        Assert.DoesNotContain("_hourly", note, StringComparison.Ordinal);
        Assert.DoesNotContain("_daily", note, StringComparison.Ordinal);
        Assert.DoesNotContain("_io_", note, StringComparison.Ordinal);
    }

    [Fact]
    public void An_unaligned_start_and_end_say_the_dropped_minutes_and_the_whole_last_hour_in_the_viewers_clock()
    {
        var start = Utc(26, 13, 49, 30);
        var end = Utc(27, 13, 49, 30);
        var edges = HourlyWindowEdges.Describe(start, null, end, ceiling: Utc(27, 20, 0));

        var note = QueryTagUsageNotes.Hourly(edges, QueryTagUsageNotes.Stamper(1));

        AssertPlain(note);
        Assert.Equal(
            "Read from the hourly rollup, from 2026-10-26 14:00 UTC to 2026-10-27 14:00 UTC. "
            + "The 10 minutes before the first full hour are not included. "
            + "The last hour is counted whole, so the data runs to 2026-10-27 14:00 UTC, past the end of the window.",
            note);
        /* What the MCP note says for the same edges, which the viewer no longer prints. */
        Assert.Matches(Iso, HourlyWindowEdges.Note(start, null, end, Utc(27, 20, 0))!);
    }

    [Fact]
    public void A_rollup_built_only_part_of_the_way_names_where_it_stops()
    {
        var start = Utc(26, 14, 0);
        var end = Utc(27, 14, 0);
        var edges = HourlyWindowEdges.Describe(start, null, end, ceiling: Utc(27, 9, 0));

        var note = QueryTagUsageNotes.Hourly(edges, QueryTagUsageNotes.Stamper(1));

        AssertPlain(note);
        Assert.Equal(
            "Read from the hourly rollup, from 2026-10-26 14:00 UTC to 2026-10-27 09:00 UTC. "
            + "The hourly rollup is built only up to 2026-10-27 09:00 UTC, so nothing after that is included.",
            note);
    }

    [Fact]
    public void An_unknown_ceiling_says_the_end_is_not_checked_and_a_window_with_no_moved_edge_says_where_it_read_to()
    {
        var unknown = QueryTagUsageNotes.Hourly(HourlyWindowEdges.Describe(Utc(26, 14, 0), null, Utc(27, 14, 0), null), QueryTagUsageNotes.Stamper(1));
        AssertPlain(unknown);
        Assert.Equal("Read from the hourly rollup, from 2026-10-26 14:00 UTC. How far the hourly rollup is built is not known, so its end is not checked.", unknown);

        var untouched = QueryTagUsageNotes.Hourly(HourlyWindowEdges.Describe(Utc(26, 14, 0), null, Utc(27, 14, 0), Utc(27, 20, 0)), QueryTagUsageNotes.Stamper(1));
        AssertPlain(untouched);
        Assert.Equal("Read from the hourly rollup up to 2026-10-27 14:00 UTC.", untouched);
    }

    [Fact]
    public void A_start_a_few_seconds_before_the_hour_does_not_say_zero_minutes()
    {
        var note = QueryTagUsageNotes.Hourly(HourlyWindowEdges.Describe(Utc(26, 13, 59, 40), null, Utc(27, 14, 0), Utc(27, 20, 0)), QueryTagUsageNotes.Stamper(1));

        Assert.Contains("Less than a minute before the first full hour is not included.", note, StringComparison.Ordinal);
        Assert.DoesNotContain("0 minutes", note, StringComparison.Ordinal);
    }

    [Fact]
    public void A_first_bucket_hours_after_the_start_names_the_span_not_a_partial_first_hour()
    {
        // The first stored bucket is 14:00 and the window began 10:30: the 3.5 hours between are a gap, not "the N minutes before the first full hour".
        var note = QueryTagUsageNotes.Hourly(HourlyWindowEdges.Describe(Utc(26, 10, 30), Utc(26, 14, 0), Utc(27, 14, 0), Utc(27, 20, 0)), QueryTagUsageNotes.Stamper(1));

        AssertPlain(note);
        Assert.Contains("The data from 2026-10-26 10:30 UTC to 2026-10-26 14:00 UTC is not included.", note, StringComparison.Ordinal);
        Assert.DoesNotContain("before the first full hour", note, StringComparison.Ordinal);

        var queryStore = QueryTagUsageNotes.QueryStore(false, Utc(26, 14, 0), Utc(26, 10, 30), Utc(27, 13, 20), QueryTagUsageNotes.Stamper(1));
        Assert.Contains("The data from 2026-10-26 10:30 UTC to 2026-10-26 14:00 UTC is not included.", queryStore, StringComparison.Ordinal);
        Assert.DoesNotContain("before the first full hour", queryStore, StringComparison.Ordinal);
    }

    [Fact]
    public void The_query_store_note_names_the_tables_in_words_and_says_the_dropped_start()
    {
        var served = Utc(26, 14, 0);
        var dropped = Utc(26, 13, 20);
        var end = Utc(27, 13, 20);

        var hourly = QueryTagUsageNotes.QueryStore(false, served, dropped, end, QueryTagUsageNotes.Stamper(2));
        var daily = QueryTagUsageNotes.QueryStore(true, served, dropped, end, QueryTagUsageNotes.Stamper(2));

        AssertPlain(hourly);
        AssertPlain(daily);
        Assert.Equal(
            "Read from the hourly Query Store table, with the raw intervals for the edges, from 2026-10-26 14:00 UTC to 2026-10-27 13:20 UTC. "
            + "The 40 minutes before the first full hour are not included.",
            hourly);
        Assert.StartsWith("Read from the daily and hourly Query Store tables, with the raw intervals for the edges, from ", daily, StringComparison.Ordinal);
    }

    [Fact]
    public void The_raw_floor_and_missing_table_messages_carry_no_table_name_or_stamp()
    {
        var floor = QueryTagUsageNotes.RawFloor("query stats", Utc(26, 13, 0), QueryTagUsageNotes.Stamper(1));
        var missing = QueryTagUsageNotes.Missing("plan_cache");

        AssertPlain(floor);
        AssertPlain(missing);
        Assert.Equal("The raw query stats collections no longer hold the whole window. The figures start at 2026-10-26 13:00 UTC.", floor);
        Assert.Equal("This store has no query stats data yet.", missing);
    }

    [Fact]
    public void One_server_reads_its_own_clock_and_several_servers_read_the_fleet_rule()
    {
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        ViewerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(-300);
        var instant = Utc(26, 14, 0);

        var one = QueryTagUsageNotes.Stamper(1)(instant);
        var several = QueryTagUsageNotes.Stamper(3)(instant);

        /* The viewer's own formatter and zone label, on the server's clock, then the fleet-wide rule (UTC when the mode is not Local). */
        var naive = DateTime.SpecifyKind(instant, DateTimeKind.Unspecified);
        Assert.Equal(ViewerTimeHelper.FormatForDisplay(naive, QueryTagUsageNotes.StampFormat) + " "
            + ViewerTimeHelper.GetTimezoneLabel(TimeDisplayMode.ServerTime, ViewerTimeHelper.ActiveServerClock, naive), one);
        Assert.StartsWith("2026-10-26 09:00 ", one, StringComparison.Ordinal);
        Assert.Equal("2026-10-26 14:00 UTC", several);

        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        Assert.Equal("2026-10-26 14:00 UTC", QueryTagUsageNotes.Stamper(1)(instant));
    }

    [Fact]
    public void The_note_reaches_the_whole_fleet_table_and_every_servers_table_unchanged()
    {
        var note = QueryTagUsageNotes.Hourly(HourlyWindowEdges.Describe(Utc(26, 13, 49, 30), null, Utc(27, 13, 49, 30), Utc(27, 20, 0)), QueryTagUsageNotes.Stamper(2));
        var data = new QueryTagRollupData(
            [], new Dictionary<string, List<QueryTagRollupFact>>(), [], false, new Dictionary<string, string>(), new Dictionary<int, string>(),
            new Dictionary<string, string> { [QueryTagRollupSection.PlanCache.Name] = note });

        var result = QueryTagUsageView.Build(data, [(1, "one"), (2, "two")], allServers: true, tagFilter: null);

        var planCache = result.Tables.Where(t => t.Section == QueryTagRollupSection.PlanCache.Name).ToList();
        Assert.NotEmpty(planCache);
        Assert.All(planCache, t => Assert.Equal(note, t.Note));
        Assert.All(result.Tables, t => AssertPlain(t.Note ?? ""));
    }
}
