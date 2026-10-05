/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The server page's Waiting Tasks grid over a store (#4966): a server added two days ago, read over 7 days, answers
/// with where its data starts; a server collected for a month whose first waiting task comes late in the window
/// answers with nothing, because the store covered the whole window. The answer is the tool's own payload, so the
/// notice is added to rows the tool really returned.
///
/// <para>The grid lists the newest 30 rows, so a window with more than that is cut by the row cap, not by the store,
/// and the note names the oldest row the read returned, covered range or not. The two coverage cases therefore seed
/// fewer than 30 rows (the read is not capped); the capped cases seed more.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE
   and DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class WebDataStartNoteLiveTests
{
    private const int NewServerId = -496601;
    private const string NewServerName = "web-data-start-added-two-days-ago";
    private const int QuietServerId = -496602;
    private const string QuietServerName = "web-data-start-quiet-start";
    private const int CappedServerId = -496603;
    private const string CappedServerName = "web-data-start-capped-list";
    private const int CoveredServerId = -496604;
    private const string CoveredServerName = "web-data-start-capped-covered";
    private const int AggregateServerId = -496605;
    private const string AggregateServerName = "web-data-start-capped-aggregate";

    /// <summary>The page's row cap for the grid: the newest 30 rows.</summary>
    private const int PageCap = 30;

    private static DateTime ParseUtc(JsonNode? node) =>
        DateTime.Parse(node!.GetValue<string>(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

    [Fact]
    public async Task AServerAddedTwoDaysAgo_GetsANoteThatNamesItsFirstCollection_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);
        var added = store.End.AddDays(-2);
        /* Hourly rows from a day back: 25, under the cap, so the store, not the cap, is what cuts the window. */
        await store.SeedAsync(NewServerId, NewServerName, added, firstRow: store.End.AddDays(-1), stepMinutes: 60, ct);

        var answer = await store.AskAsync(NewServerName, hours: 168, ct);

        Assert.False(answer["truncated"]?.GetValue<bool>());
        Assert.True(answer["window_truncated"]?.GetValue<bool>());
        var effectiveStart = DateTime.Parse(answer["effective_start"]!.GetValue<string>(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        Assert.True(Math.Abs((effectiveStart - added).TotalSeconds) < 1, "the data starts at the server's first collection, not its first row");
        var note = answer["truncation_note"]!.GetValue<string>();
        Assert.StartsWith("partial window:", note, StringComparison.Ordinal);
        Assert.Contains(added.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC", note, StringComparison.Ordinal);
        Assert.NotNull(answer["tasks"]);

        /* The same instants as fields (#4966): the page composes the note from them in the browser's zone. The data start
           is the one effective_start names, the window is the 168 hours asked for, and none of them is a capped list's. */
        Assert.Equal(effectiveStart.Ticks, ParseUtc(answer["data_start_utc"]).Ticks);
        var windowStart = ParseUtc(answer["window_start_utc"]);
        var windowEnd = ParseUtc(answer["window_end_utc"]);
        Assert.Equal(TimeSpan.FromHours(168), windowEnd - windowStart);
        Assert.True(Math.Abs((windowEnd - DateTime.UtcNow).TotalSeconds) < 120, "the window ends when the grid was asked");
        Assert.EndsWith("Z", answer["data_start_utc"]!.GetValue<string>(), StringComparison.Ordinal);
        Assert.Null(answer["oldest_shown_utc"]);
    }

    [Fact]
    public async Task AWindowTheStoreCovered_GetsNoNote_WhenTheFirstRowComesLate_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);
        /* One row every six hours from six days back: 25, under the cap. */
        await store.SeedAsync(QuietServerId, QuietServerName, store.End.AddDays(-30), firstRow: store.End.AddDays(-6), stepMinutes: 360, ct);

        var week = await store.AskAsync(QuietServerName, hours: 168, ct);
        var threeDays = await store.AskAsync(QuietServerName, hours: 72, ct);

        Assert.False(week["truncated"]?.GetValue<bool>());
        /* The tool's own keys (#4966) say covered: false and no note. The web adds nothing of its own, and the page draws
           a note only for true. */
        Assert.NotEqual(true, week["window_truncated"]?.GetValue<bool>());
        Assert.Null(week["truncation_note"]);
        Assert.Null(week["data_start_utc"]);
        Assert.NotNull(week["tasks"]);
        Assert.NotEqual(true, threeDays["window_truncated"]?.GetValue<bool>());
        Assert.Null(threeDays["data_start_utc"]);
    }

    /// <summary>A read that hit its cap lists the newest rows only, so the note names the oldest row it returned, not
    /// where the table starts (the server was added two days ago, and the rows run a day back at 30 minutes: 49 rows,
    /// the newest 30 reach 14.5 hours).</summary>
    [Fact]
    public async Task ACappedRead_NamesTheOldestRowItReturned_NotWhereTheTableStarts_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);
        var added = store.End.AddDays(-2);
        await store.SeedAsync(CappedServerId, CappedServerName, added, firstRow: store.End.AddDays(-1), stepMinutes: 30, ct);

        var answer = await store.AskAsync(CappedServerName, hours: 168, ct);

        var oldestShown = store.End.AddMinutes(-(PageCap - 1) * 30);
        Assert.True(answer["truncated"]?.GetValue<bool>());
        Assert.True(answer["window_truncated"]?.GetValue<bool>());
        Assert.True(Math.Abs((ParseUtc(answer["effective_start"]) - oldestShown).TotalSeconds) < 1, "the grid starts at the oldest row the read returned");
        var note = answer["truncation_note"]!.GetValue<string>();
        Assert.StartsWith("partial window: this grid shows only the newest rows, back to " + oldestShown.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC", note, StringComparison.Ordinal);
        Assert.DoesNotContain(added.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture), note, StringComparison.Ordinal);
        Assert.Equal(PageCap, answer["tasks"]!.AsArray().Count);

        /* The capped note's instants as fields (#4966): the oldest row shown, never where the table starts. */
        Assert.True(Math.Abs((ParseUtc(answer["oldest_shown_utc"]) - oldestShown).TotalSeconds) < 1);
        Assert.Equal(TimeSpan.FromHours(168), ParseUtc(answer["window_end_utc"]) - ParseUtc(answer["window_start_utc"]));
        Assert.Null(answer["data_start_utc"]);
    }

    /// <summary>The same cap over a range the store covered: the server has been collected for a month and its rows
    /// run six days back (289 at 30 minutes). Nothing is missing from the store, and the grid still stops 14.5
    /// hours back, so the note is there.</summary>
    [Fact]
    public async Task ACappedRead_OverARangeTheStoreCovered_StillNamesTheOldestRowItReturned_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);
        await store.SeedAsync(CoveredServerId, CoveredServerName, store.End.AddDays(-30), firstRow: store.End.AddDays(-6), stepMinutes: 30, ct);

        var week = await store.AskAsync(CoveredServerName, hours: 168, ct);

        var oldestShown = store.End.AddMinutes(-(PageCap - 1) * 30);
        Assert.True(week["truncated"]?.GetValue<bool>());
        Assert.True(week["window_truncated"]?.GetValue<bool>());
        Assert.True(Math.Abs((ParseUtc(week["effective_start"]) - oldestShown).TotalSeconds) < 1);
        Assert.StartsWith("partial window: this grid shows only the newest rows, back to " + oldestShown.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC", week["truncation_note"]!.GetValue<string>(), StringComparison.Ordinal);
    }

    /// <summary>A capped aggregate read keeps today's rule. The latch read keeps its top classes of the whole window,
    /// so its cap hides no time range: its answer carries the same cap fields, and the note still names where the
    /// table starts (the server's first collection), not the field's time.</summary>
    [Fact]
    public async Task ACappedAggregateRead_KeepsTheCoverageRule_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);
        var added = store.End.AddDays(-2);
        await store.SeedAsync(AggregateServerId, AggregateServerName, added, firstRow: store.End.AddDays(-1), stepMinutes: 60, ct);
        var oldestShown = store.End.AddMinutes(-(PageCap - 1) * 30);
        var payload = new JsonObject
        {
            ["server"] = AggregateServerName,
            ["truncated"] = true,
            ["oldest_returned_collection_time"] = oldestShown.ToString("o", CultureInfo.InvariantCulture),
            ["latches"] = new JsonArray(new JsonObject { ["latch_class"] = "BUFFER" }),
        };

        var answered = await WebDataStartNote.AddAsync(
            store.DataSource, "get_latch_stats", AggregateServerName, 168, null, payload.ToJsonString(), null, ct);

        var answer = Assert.IsType<JsonObject>(JsonNode.Parse(answered));
        Assert.True(answer["window_truncated"]?.GetValue<bool>());
        Assert.True(Math.Abs((ParseUtc(answer["effective_start"]) - added).TotalSeconds) < 1, "the table starts at the server's first collection, not at the cap fields' time");
        Assert.StartsWith("partial window: this panel's data starts at", answer["truncation_note"]!.GetValue<string>(), StringComparison.Ordinal);
    }

    /// <summary>The Memory Grant Pressure read over its own snapshot table (#4966): a server added two days ago whose
    /// snapshots start a day back is asked for 7 days, through the real tool, so the note is added to rows the tool
    /// returned. The data starts between the server's first collection and its first snapshot, as for the other
    /// snapshot tables; the same server asked for a window its snapshots cover (rows from eight days back) gets none.</summary>
    [Fact]
    public async Task TheMemoryGrantsRead_WhoseSnapshotsStartInsideTheRange_GetsANoteNamingWhereItsDataStarts_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);
        var added = store.End.AddDays(-2);
        var firstRow = store.End.AddDays(-1);
        await store.SeedTableAsync("memory_grant_stats", -496630, "web-data-start-memory-grants", added, firstRow, ct);

        var payload = await DarlingMcpMemoryGrantTools.GetMemoryGrants(store.DataSource, "web-data-start-memory-grants", 168, null, cancellationToken: ct);
        var answered = await WebDataStartNote.AddAsync(store.DataSource, "get_memory_grants", "web-data-start-memory-grants", 168, null, payload, null, ct);

        var answer = Assert.IsType<JsonObject>(JsonNode.Parse(answered));
        Assert.NotNull(answer["window"]);
        Assert.True(answer["window_truncated"]?.GetValue<bool>(), "the tool returned rows and the read gets its note");
        var start = ParseUtc(answer["effective_start"]);
        Assert.True(start >= added.AddSeconds(-1) && start <= firstRow.AddSeconds(1), "the data starts between the first collection and the first snapshot, got " + start.ToString("o", CultureInfo.InvariantCulture));
        Assert.StartsWith("partial window: this panel's data starts at", answer["truncation_note"]!.GetValue<string>(), StringComparison.Ordinal);
        Assert.Equal(start.Ticks, ParseUtc(answer["data_start_utc"]).Ticks);
        Assert.Equal(TimeSpan.FromHours(168), ParseUtc(answer["window_end_utc"]) - ParseUtc(answer["window_start_utc"]));

        await store.SeedTableAsync("memory_grant_stats", -496631, "web-data-start-memory-grants-covered", store.End.AddDays(-30), firstRow: store.End.AddDays(-8), ct);
        var covered = await DarlingMcpMemoryGrantTools.GetMemoryGrants(store.DataSource, "web-data-start-memory-grants-covered", 168, null, cancellationToken: ct);
        Assert.Same(covered, await WebDataStartNote.AddAsync(store.DataSource, "get_memory_grants", "web-data-start-memory-grants-covered", 168, null, covered, null, ct));
    }

    /// <summary>What a PostgreSQL grid's answer looks like to the note: rows, with no status, error or floor of its
    /// own. The note reads the store, not the rows, so a stand-in keeps these two tests on the table each read lists.</summary>
    private const string StandInRows = "{\"server\":\"pg01\",\"rows\":[{\"n\":1}]}";

    private static readonly string[] PostgresReads =
        [.. WebDataStartNote.TableByRead.Keys.Where(k => k.StartsWith("get_pg_", StringComparison.Ordinal) && !WebDataStartNote.EventTimeByRead.ContainsKey(k)).OrderBy(k => k, StringComparer.Ordinal)];

    /// <summary>Each of the twenty-three PostgreSQL reads (the configuration changes, the window aggregates, the trend grids, Captured Plans and the vacuum, horizon, slot and write tiles among them, #4966), over its own table, for a server added two days ago whose rows
    /// start a day back: the data starts inside the 7-day range, so the note is there and names a start between the
    /// server's first collection and its first row. Which of the two a table reports depends on whether the schedule
    /// gives it a purge edge (the first collection) or not (the oldest row it holds), so the test holds the bounds the
    /// two rules share.</summary>
    [Fact]
    public async Task EachPostgresRead_WhoseRowsStartInsideTheRange_GetsANoteNamingWhereItsDataStarts_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);
        Assert.Equal(23, PostgresReads.Length);

        for (var i = 0; i < PostgresReads.Length; i++)
        {
            var read = PostgresReads[i];
            var name = "web-data-start-" + read.Replace('_', '-');
            var added = store.End.AddDays(-2);
            var firstRow = store.End.AddDays(-1);
            /* The sparse reads are probed on their collector's runs, so their server logs them from the first collection. */
            await store.SeedTableAsync(
                WebDataStartNote.TableByRead[read], -496610 - i, name, added, firstRow, ct,
                runsCollector: WebDataStartNote.CollectorRunsByRead.GetValueOrDefault(read));

            var answered = await WebDataStartNote.AddAsync(store.DataSource, read, name, 168, null, StandInRows, null, ct);

            var answer = Assert.IsType<JsonObject>(JsonNode.Parse(answered));
            Assert.True(answer["window_truncated"]?.GetValue<bool>(), read + " gives a note");
            var start = ParseUtc(answer["effective_start"]);
            Assert.True(start >= added.AddSeconds(-1) && start <= firstRow.AddSeconds(1), read + " names a start between the server's first collection and its first row, got " + start.ToString("o", CultureInfo.InvariantCulture));
            Assert.StartsWith("partial window: this panel's data starts at " + start.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC", answer["truncation_note"]!.GetValue<string>(), StringComparison.Ordinal);
        }
    }

    /// <summary>The same twenty-three reads for a server collected for a month whose rows reach back past the 7-day window's
    /// start (eight days): the store covered the range, so there is no note, over 7 days and over 3. (A quiet start,
    /// rows that begin late in a covered range, is the Waiting Tasks test above: a table the schedule gives no purge
    /// edge reports the oldest row it holds, so for those rows that begin late are a late start.)</summary>
    [Fact]
    public async Task EachPostgresRead_WhoseRowsReachBackPastTheRange_GetsNoNote_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        for (var i = 0; i < PostgresReads.Length; i++)
        {
            var read = PostgresReads[i];
            var name = "web-data-start-covered-" + read.Replace('_', '-');
            await store.SeedTableAsync(
                WebDataStartNote.TableByRead[read], -496620 - i, name, store.End.AddDays(-30), firstRow: store.End.AddDays(-8), ct,
                runsCollector: WebDataStartNote.CollectorRunsByRead.GetValueOrDefault(read));

            Assert.Same(StandInRows, await WebDataStartNote.AddAsync(store.DataSource, read, name, 168, null, StandInRows, null, ct));
            Assert.Same(StandInRows, await WebDataStartNote.AddAsync(store.DataSource, read, name, 72, null, StandInRows, null, ct));
        }
    }

    /// <summary>The six sparse reads (blocking, session states, replication stats, the xmin horizon holders, the autovacuum backlog and the replication slots) store a row only when something exists,
    /// so their first row says nothing about when collection began. Each is probed on its collector's own logged runs.</summary>
    private static readonly (string Read, string Table, string Collector)[] SparseReads =
    [
        ("get_pg_blocking", "pg_blocking_edges", "pg_blocking"),
        ("get_pg_session_states", "pg_session_states", "pg_session_states"),
        ("get_pg_replication_stats", "pg_replication_stats", "pg_replication_stats"),
        ("get_pg_xmin_horizon", "pg_xmin_horizon", "pg_xmin_horizon"),
        ("get_pg_autovacuum_health", "pg_autovacuum_stats", "pg_autovacuum_stats"),
        ("get_pg_replication_slots", "pg_replication_slot_stats", "pg_replication_slots"),
    ];

    /// <summary>A server registered 30 days ago whose collector ran all week and stored ONE row a day back (the first blocking chain,
    /// the first long transaction, the first connected replica, the first horizon holder, the first table behind on vacuum, the first
    /// slot): the 7-day range was covered, so there is no note. Reading the
    /// oldest row instead would name yesterday as where the data starts.</summary>
    [Fact]
    public async Task ASparseRead_OnAnOldServer_WhoseFirstRowIsADayBack_GetsNoNote_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        for (var i = 0; i < SparseReads.Length; i++)
        {
            var (read, table, collector) = SparseReads[i];
            var name = "web-data-start-sparse-old-" + read.Replace('_', '-');
            var oneDayBack = store.End.AddDays(-1);
            await store.SeedTableAsync(table, -496640 - i, name, store.End.AddDays(-30), oneDayBack, ct, runsCollector: collector, lastRow: oneDayBack);

            Assert.Same(StandInRows, await WebDataStartNote.AddAsync(store.DataSource, read, name, 168, null, StandInRows, null, ct));
        }
    }

    /// <summary>The other side: a server registered two days ago, its collector running since, has a 7-day range it cannot cover.</summary>
    [Fact]
    public async Task ASparseRead_OnAServerRegisteredTwoDaysAgo_GetsANote_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        for (var i = 0; i < SparseReads.Length; i++)
        {
            var (read, table, collector) = SparseReads[i];
            var name = "web-data-start-sparse-new-" + read.Replace('_', '-');
            var added = store.End.AddDays(-2);
            var oneDayBack = store.End.AddDays(-1);
            await store.SeedTableAsync(table, -496650 - i, name, added, oneDayBack, ct, runsCollector: collector, lastRow: oneDayBack);

            var answer = Assert.IsType<JsonObject>(JsonNode.Parse(await WebDataStartNote.AddAsync(store.DataSource, read, name, 168, null, StandInRows, null, ct)));
            Assert.True(answer["window_truncated"]?.GetValue<bool>(), read + " gives a note");
            var start = ParseUtc(answer["effective_start"]);
            Assert.True(start >= added.AddSeconds(-1) && start <= added.AddHours(1), read + " names the first collection, not its first row, got " + start.ToString("o", CultureInfo.InvariantCulture));
        }
    }

    /// <summary>Each event read: table, the column that holds the event's own time, and the tool's rows key.</summary>
    private static readonly (string Read, string Table, string EventColumn)[] EventReads =
    [
        ("get_blocked_process_xml", "blocked_process_reports", "event_time"),
        ("get_long_query_completions", "long_query_completions", "event_time"),
        ("get_memory_pressure_events", "memory_pressure_events", "sample_time"),
        ("get_default_trace_events", "default_trace_events", "event_time"),

        /* PostgreSQL event logs, Blocking and Deadlocks (#4966). */
        ("get_pg_deadlocks", "pg_deadlocks", "occurred_at"),
        ("get_pg_log_events", "pg_log_events", "occurred_at"),
        ("get_blocking", "blocked_process_reports", "event_time"),
        ("get_deadlocks", "deadlocks", "deadlock_time"),
        ("get_deadlock_detail", "deadlocks", "deadlock_time"),
    ];

    /// <summary>A server added two days ago whose stored events carry times from before it (a first collection stores the
    /// server's event history): over 7 days the note names the earliest event shown, which is earlier than the coverage start
    /// and still after the window's start.</summary>
    [Fact]
    public async Task AnEventRead_WhoseHistoryReachesBeforeCoverage_NamesTheEarliestEvent_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);
        var added = store.End.AddDays(-2);

        for (var i = 0; i < EventReads.Length; i++)
        {
            var (read, table, column) = EventReads[i];
            var name = "web-data-start-event-history-" + read.Replace('_', '-');
            await store.SeedTableAsync(table, -496700 - i, name, added, added, ct, eventColumn: column, eventShift: TimeSpan.FromDays(2), extraSet: ExtraSet(table));

            var answer = await store.AskEventReadAsync(read, name, 168, ct);

            Assert.True(answer["window_truncated"]?.GetValue<bool>(), read + " gives a note");
            var start = ParseUtc(answer["effective_start"]);
            var earliestEvent = added.AddDays(-2);
            Assert.True(Math.Abs((start - earliestEvent).TotalSeconds) < 1, read + " names the earliest event, got " + start.ToString("o", CultureInfo.InvariantCulture) + " vs " + earliestEvent.ToString("o", CultureInfo.InvariantCulture) + " " + answer.ToJsonString()[..Math.Min(500, answer.ToJsonString().Length)]);
            Assert.Equal(start.Ticks, ParseUtc(answer["data_start_utc"]).Ticks);
            Assert.StartsWith("partial window:", answer["truncation_note"]!.GetValue<string>(), StringComparison.Ordinal);
        }
    }

    /// <summary>The same server whose events reach the window's start: the page shows the whole range, so there is no note.</summary>
    [Fact]
    public async Task AnEventRead_WhoseHistoryReachesTheWindowsStart_GetsNoNote_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);
        var added = store.End.AddDays(-2);

        for (var i = 0; i < EventReads.Length; i++)
        {
            var (read, table, column) = EventReads[i];
            var name = "web-data-start-event-reaches-" + read.Replace('_', '-');
            await store.SeedTableAsync(table, -496710 - i, name, added, added, ct, eventColumn: column, eventShift: TimeSpan.FromDays(5.5), extraSet: ExtraSet(table));

            var answer = await store.AskEventReadAsync(read, name, 168, ct);

            Assert.NotEqual(true, answer["window_truncated"]?.GetValue<bool>());
            Assert.Null(answer["data_start_utc"]);
            Assert.Null(answer["effective_start"]);
            Assert.Null(answer["truncation_note"]);
        }
    }

    /// <summary>Two of the nine system_health reads over one seeded <c>system_health_events</c> table (a server per read, every row the
    /// read's own event type): history reaching back before coverage names the earliest event; history reaching the window's start
    /// gives no note.</summary>
    [Theory]
    [InlineData(-496730, 2.0, true)]
    [InlineData(-496740, 5.5, false)]
    public async Task TheSystemHealthReads_FollowTheEventTimeRule_AgainstDevPostgres(int serverId, double shiftDays, bool expectNote)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);
        var added = store.End.AddDays(-2);

        var reads = new[] { ("get_health_parser_severe_errors", SystemHealthParser.ErrorReportedEvent, "error_reported.xml"), ("get_health_parser_system_health", SystemHealthParser.SpServerDiagnosticsEvent, "sp_server_diagnostics_system.xml") };
        for (var i = 0; i < reads.Length; i++)
        {
            var (read, eventType, fixture) = reads[i];
            var name = "web-data-start-health-" + (expectNote ? "before-" : "reaches-") + i;
            await store.SeedTableAsync("system_health_events", serverId - 100 * (i + 1), name, added, added, ct, eventColumn: "event_time", eventShift: TimeSpan.FromDays(shiftDays));
            await store.GiveHealthRowsTheirXmlAsync(serverId - 100 * (i + 1), eventType, fixture, ct);

            var answer = await store.AskHealthReadAsync(read, name, 168, ct);
            if (!expectNote)
            {
                Assert.NotEqual(true, answer["window_truncated"]?.GetValue<bool>());
                Assert.Null(answer["data_start_utc"]);
                continue;
            }

            Assert.True(answer["window_truncated"]?.GetValue<bool>(), read + " gives a note");
            var earliestEvent = added.AddDays(-2);
            Assert.True(Math.Abs((ParseUtc(answer["effective_start"]) - earliestEvent).TotalSeconds) < 1, read + " names the earliest event");
            Assert.StartsWith("partial window:", answer["truncation_note"]!.GetValue<string>(), StringComparison.Ordinal);
        }
    }

    /// <summary>The composite probe, proved: the XE table starts late inside the window (the page shows only XE rows), while the DMV
    /// snapshots reach back from before the window. The earlier start covers the window, so there is NO note; a probe of the XE table
    /// alone would name the late start and add one.</summary>
    [Fact]
    public async Task GetBlocking_WhenTheDmvSnapshotsCoverTheWindowButTheXeRowsStartLate_GivesNoNote_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);
        const string server = "web-data-start-blocking-composite";

        /* DMV: hourly snapshots from 10 days back to the window end, so they cover the 7-day window from before its start. */
        await store.SeedTableAsync("dmv_blocking_snapshots", -496732, server, store.End.AddDays(-10), store.End.AddDays(-10), ct);
        /* XE: starts one day back, late inside the window; these are the rows the page shows. */
        await store.SeedTableAsync("blocked_process_reports", -496732, server, store.End.AddDays(-1), store.End.AddDays(-1), ct, eventColumn: "event_time", extraSet: ExtraSet("blocked_process_reports"));

        var answer = await store.AskEventReadAsync("get_blocking", server, 168, ct);
        Assert.True(answer["events_returned"]?.GetValue<int>() > 0, "the page shows the XE rows");
        Assert.True(answer["window_truncated"] is null, "the DMV snapshots cover the window, so no note: " + answer["truncation_note"]);
        Assert.Null(answer["truncation_note"]);
    }

    /// <summary>Each single-table server still gets the note from its own table's start (does not by itself prove the composite).</summary>
    [Fact]
    public async Task GetBlocking_OnRowsFromOneTableOnly_NamesThatTablesStart_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);
        var added = store.End.AddDays(-2);

        await store.SeedTableAsync("dmv_blocking_snapshots", -496730, "web-data-start-blocking-dmv-only", added, added, ct);
        var dmv = await store.AskEventReadAsync("get_blocking", "web-data-start-blocking-dmv-only", 168, ct);
        Assert.True(dmv["window_truncated"]?.GetValue<bool>(), "the DMV-only server gives a note");
        Assert.True(Math.Abs((ParseUtc(dmv["effective_start"]) - added).TotalSeconds) < 1, "names the DMV table's start");

        await store.SeedTableAsync("blocked_process_reports", -496731, "web-data-start-blocking-xe-only", added, added, ct, extraSet: ExtraSet("blocked_process_reports"));
        var xe = await store.AskEventReadAsync("get_blocking", "web-data-start-blocking-xe-only", 168, ct);
        Assert.True(xe["window_truncated"]?.GetValue<bool>(), "the XE-only server gives a note");
        Assert.True(Math.Abs((ParseUtc(xe["effective_start"]) - added).TotalSeconds) < 1, "names the XE table's start");
    }

    /* The Blocked Process Reports read counts only rows that carry a report; the stand-in seed leaves that column null. */
    private static string? ExtraSet(string table) => table switch
    {
        "blocked_process_reports" => "blocked_process_report_xml = '<blocked-process-report/>'",
        "deadlocks" => "deadlock_graph_xml = '<deadlock/>'",
        "pg_deadlocks" => "deadlock_hash = 'x'",
        "pg_log_events" => "raw_line_hash = 'x', severity = 'ERROR', family = 'error'",
        _ => null,
    };

    private sealed class Store : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;

        private Store(ScratchPostgres scratch, NpgsqlDataSource dataSource, DateTime end)
        {
            _scratch = scratch;
            DataSource = dataSource;
            End = end;
        }

        public NpgsqlDataSource DataSource { get; }

        /// <summary>The minute the seeded history ends at, naive UTC.</summary>
        public DateTime End { get; }

        public static async Task<Store> CreateAsync(CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the live data-start tests.");

            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            try
            {
                await using var connection = new NpgsqlConnection(scratch.ConnectionString);
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);

                var now = DateTime.UtcNow;
                var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
                return new Store(scratch, NpgsqlDataSource.Create(scratch.ConnectionString), end);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        /// <summary>A server first collected at <paramref name="added"/> whose waiting tasks run from
        /// <paramref name="firstRow"/> to the end, one every <paramref name="stepMinutes"/> minutes; the waiting-task and
        /// latch collectors' runs are logged every 30 minutes from the first collection, whether or not anything
        /// waited.</summary>
        public async Task SeedAsync(int serverId, string serverName, DateTime added, DateTime firstRow, int stepMinutes, CancellationToken ct)
        {
            await using var connection = await DataSource.OpenConnectionAsync(ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);

            await using (var update = new NpgsqlCommand("UPDATE collect.servers SET created_date = $2 WHERE server_id = $1", connection))
            {
                update.Parameters.AddWithValue(serverId);
                update.Parameters.AddWithValue(DateTime.SpecifyKind(added, DateTimeKind.Unspecified));
                await update.ExecuteNonQueryAsync(ct);
            }

            await using (var log = new NpgsqlCommand(@"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT row_number() OVER (), $1, $2, c.name, t, 12, 'SUCCESS', 0
FROM generate_series($3::timestamp, $4::timestamp, interval '30 minutes') AS t
CROSS JOIN (VALUES ('waiting_tasks'), ('latch_stats')) AS c(name)", connection))
            {
                log.Parameters.AddWithValue(serverId);
                log.Parameters.AddWithValue(serverName);
                log.Parameters.AddWithValue(DateTime.SpecifyKind(added, DateTimeKind.Unspecified));
                log.Parameters.AddWithValue(DateTime.SpecifyKind(End, DateTimeKind.Unspecified));
                await log.ExecuteNonQueryAsync(ct);
            }

            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.waiting_tasks (collection_id, collection_time, server_id, server_name, wait_type, wait_duration_ms, database_name)
SELECT row_number() OVER (), t, $1, $2, 'LCK_M_X', 250, 'WebDb'
FROM generate_series($3::timestamp, $4::timestamp, make_interval(mins => $5)) AS t", connection);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(serverName);
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(firstRow, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(End, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(stepMinutes);
            await insert.ExecuteNonQueryAsync(ct);
        }

        /// <summary>A server first collected at <paramref name="added"/> with one row an hour in <paramref name="table"/>
        /// from <paramref name="firstRow"/> to the end. Each column the table requires is filled with a stand-in by type:
        /// the note reads only that the server holds a row at a time.</summary>
        public async Task SeedTableAsync(
            string table, int serverId, string serverName, DateTime added, DateTime firstRow, CancellationToken ct,
            string? runsCollector = null, DateTime? lastRow = null, string? eventColumn = null, TimeSpan? eventShift = null, string? extraSet = null)
        {
            await using var connection = await DataSource.OpenConnectionAsync(ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);

            await using (var update = new NpgsqlCommand("UPDATE collect.servers SET created_date = $2 WHERE server_id = $1", connection))
            {
                update.Parameters.AddWithValue(serverId);
                update.Parameters.AddWithValue(DateTime.SpecifyKind(added, DateTimeKind.Unspecified));
                await update.ExecuteNonQueryAsync(ct);
            }

            if (runsCollector is not null)
            {
                /* The collector's runs, logged every 30 minutes from the first collection to the end, whether or not a run stored a row. */
                await using var log = new NpgsqlCommand(@"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT $1::bigint * 1000000 + row_number() OVER (), $1, $2, $5, t, 12, 'SUCCESS', 0
FROM generate_series($3::timestamp, $4::timestamp, interval '30 minutes') AS t", connection);
                log.Parameters.AddWithValue(serverId);
                log.Parameters.AddWithValue(serverName);
                log.Parameters.AddWithValue(DateTime.SpecifyKind(added, DateTimeKind.Unspecified));
                log.Parameters.AddWithValue(DateTime.SpecifyKind(End, DateTimeKind.Unspecified));
                log.Parameters.AddWithValue(runsCollector);
                await log.ExecuteNonQueryAsync(ct);
            }

            var time = CollectorCatalog.All.First(c => c.TargetTable == table).PrefixTimeColumnName;
            var columns = new List<string>();
            var values = new List<string>();
            await using (var required = new NpgsqlCommand(@"
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'collect' AND table_name = $1 AND is_nullable = 'NO' AND column_default IS NULL AND is_generated = 'NEVER'
ORDER BY ordinal_position", connection))
            {
                required.Parameters.AddWithValue(table);
                await using var reader = await required.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    var column = reader.GetString(0);
                    var type = reader.GetString(1);
                    columns.Add(column);
                    values.Add(column == time ? "t"
                        : column == "server_id" ? serverId.ToString(CultureInfo.InvariantCulture)
                        : column == "server_name" ? "'" + serverName + "'"
                        : column == "collection_id" ? "row_number() OVER ()"
                        : type switch
                        {
                            "integer" or "bigint" or "smallint" or "numeric" or "double precision" or "real" => "1",
                            "boolean" => "false",
                            "text" or "character varying" or "character" => "'x'",
                            "timestamp without time zone" => "t",
                            "timestamp with time zone" => "t::timestamptz",
                            "json" or "jsonb" => "'{}'",
                            "ARRAY" => "'{}'",
                            _ => throw new NotSupportedException(table + "." + column + " is " + type + ", which this stand-in seed does not fill"),
                        });
                }
            }

            await using var insert = new NpgsqlCommand(
                "INSERT INTO collect." + table + " (" + string.Join(", ", columns) + ") SELECT " + string.Join(", ", values)
                + " FROM generate_series($1::timestamp, $2::timestamp, interval '60 minutes') AS t", connection);
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(firstRow, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(lastRow ?? End, DateTimeKind.Unspecified));
            await insert.ExecuteNonQueryAsync(ct);

            /* An event carries its own time, apart from the collection that stored it: stamp each row's event column that far
               before its collection_time, as a first collection stores the server's event history. */
            if (eventColumn is not null)
            {
                await using var stamp = new NpgsqlCommand(
                    "UPDATE collect." + table + " SET " + eventColumn + " = " + time + " - make_interval(secs => $2)" + (extraSet is null ? "" : ", " + extraSet) + " WHERE server_id = $1", connection);
                stamp.Parameters.AddWithValue(serverId);
                stamp.Parameters.AddWithValue((eventShift ?? TimeSpan.Zero).TotalSeconds);
                await stamp.ExecuteNonQueryAsync(ct);
            }
        }

        /// <summary>The tool's own payload for an event read over a window, then the data-start note: what the web mirror answers.</summary>
        public async Task<JsonObject> AskEventReadAsync(string read, string server, int hours, CancellationToken ct)
        {
            var payload = read switch
            {
                "get_blocked_process_xml" => await DarlingMcpBlockingTools.GetBlockedProcessXml(DataSource, server_name: server, hours_back: hours, limit: 100, cancellationToken: ct),
                "get_long_query_completions" => await DarlingMcpLongQueryTools.GetLongQueryCompletions(DataSource, server_name: server, hours_back: hours, limit: 100, cancellationToken: ct),
                "get_memory_pressure_events" => await DarlingMcpMemoryGrantTools.GetMemoryPressureEvents(DataSource, server_name: server, hours_back: hours, cancellationToken: ct),
                "get_default_trace_events" => await DarlingMcpDefaultTraceTools.GetDefaultTraceEvents(DataSource, server_name: server, hours_back: hours, limit: 100, cancellationToken: ct),
                "get_pg_deadlocks" => await DarlingMcpPgDeadlockTools.GetPgDeadlocks(DataSource, server_name: server, hours_back: hours, limit: 100, cancellationToken: ct),
                "get_pg_log_events" => await DarlingMcpPgLogEventTools.GetPgLogEvents(DataSource, server_name: server, hours_back: hours, limit: 100, cancellationToken: ct),
                "get_blocking" => await DarlingMcpBlockingTools.GetBlocking(DataSource, server_name: server, hours_back: hours, limit: 100, cancellationToken: ct),
                "get_deadlocks" => await DarlingMcpBlockingTools.GetDeadlocks(DataSource, server_name: server, hours_back: hours, limit: 100, cancellationToken: ct),
                "get_deadlock_detail" => await DarlingMcpBlockingTools.GetDeadlockDetail(DataSource, server_name: server, hours_back: hours, limit: 100, cancellationToken: ct),
                _ => throw new ArgumentOutOfRangeException(nameof(read), read, "not an event read"),
            };
            var answered = await WebDataStartNote.AddAsync(DataSource, read, server, hours, null, payload, null, ct);
            return Assert.IsType<JsonObject>(JsonNode.Parse(answered));
        }

        /// <summary>The stand-in seed leaves each row's event type and XML blank: make every row one event of this type, stamped with
        /// the row's own event time as a real capture has it.</summary>
        public async Task GiveHealthRowsTheirXmlAsync(int serverId, string eventType, string fixture, CancellationToken ct)
        {
            var xml = await System.IO.File.ReadAllTextAsync(System.IO.Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", fixture), ct);
            await using var connection = await DataSource.OpenConnectionAsync(ct);
            await using var update = new NpgsqlCommand(@"
UPDATE collect.system_health_events
SET event_type = $2,
    event_xml = regexp_replace($3, 'timestamp=""[^""]*""', 'timestamp=""' || to_char(event_time, 'YYYY-MM-DD""T""HH24:MI:SS.MS') || 'Z""')
WHERE server_id = $1", connection);
            update.Parameters.AddWithValue(serverId);
            update.Parameters.AddWithValue(eventType);
            update.Parameters.AddWithValue(xml);
            await update.ExecuteNonQueryAsync(ct);
        }

        /// <summary>The tool's own payload for a system_health read, then the data-start note.</summary>
        public async Task<JsonObject> AskHealthReadAsync(string read, string server, int hours, CancellationToken ct)
        {
            var payload = read switch
            {
                "get_health_parser_severe_errors" => await DarlingMcpHealthParserTools.GetSevereErrors(DataSource, server_name: server, hours_back: hours, limit: 100, cancellationToken: ct),
                "get_health_parser_system_health" => await DarlingMcpHealthParserTools.GetSystemHealth(DataSource, server_name: server, hours_back: hours, limit: 100, cancellationToken: ct),
                _ => throw new ArgumentOutOfRangeException(nameof(read), read, "not a seeded system_health read"),
            };
            var answered = await WebDataStartNote.AddAsync(DataSource, read, server, hours, null, payload, null, ct);
            return Assert.IsType<JsonObject>(JsonNode.Parse(answered));
        }

        /// <summary>What the web mirror answers for the grid: the tool's own payload, then the data-start note.</summary>
        public async Task<JsonObject> AskAsync(string server, int hours, CancellationToken ct)
        {
            var payload = await DarlingMcpSessionTools.GetWaitingTasks(DataSource, server, hours, PageCap, null, cancellationToken: ct);
            var answered = await WebDataStartNote.AddAsync(DataSource, "get_waiting_tasks", server, hours, null, payload, null, ct);
            return Assert.IsType<JsonObject>(JsonNode.Parse(answered));
        }

        public async ValueTask DisposeAsync()
        {
            await DataSource.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}
