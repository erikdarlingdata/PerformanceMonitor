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
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The server page's Config Changes grids (server, database, trace flags, and PostgreSQL's) and its two Collection Log
/// grids say where their data starts (#4966). Each is a listed read (<see cref="WebDataStartNote.TableByRead"/>) over
/// the snapshot table or the run log it diffs or shows; the two lists that can hit a row cap (the collection log and
/// the PostgreSQL changes) name their oldest row. These cover what needs no store; the store's side is
/// <see cref="WebDataStartNoteLiveTests"/>.
/// </summary>
public sealed class WebDataStartNoteConfigAndLogTests
{
    private static readonly IReadOnlyDictionary<string, string> NewReads = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["get_server_config_changes"] = "server_config",
        ["get_database_config_changes"] = "database_config",
        ["get_trace_flag_changes"] = "trace_flags",
        ["get_pg_server_config_changes"] = "pg_server_config",
        ["get_collection_log"] = "collection_log",
    };

    private const string WindowEnd = "2026-01-03T00:00:00Z";

    /// <summary>What <c>get_collection_log</c> answers for a window holding more runs than its limit: the newest runs, with
    /// the fields that say the cap cut the list, where the page ends and which order it came back in.</summary>
    private const string CappedLog =
        "{\"server\":\"sql01\",\"hours_back\":720,\"run_count\":200,\"truncated\":true,"
        + "\"oldest_returned_collection_time\":\"2026-01-02T12:30:00.0000000\",\"newest_returned_collection_time\":\"2026-01-03T00:00:00.0000000\","
        + "\"order\":\"collection_time_desc\",\"runs\":[{\"collector\":\"wait_stats\"}]}";

    /// <summary>What <c>get_pg_server_config_changes</c> answers when its page was cut: the newest changes (listed here out
    /// of order on purpose, so the oldest is found by value), no oldest-returned field, and a <c>status</c> that names the
    /// page rather than an envelope.</summary>
    private const string CappedPgChanges =
        "{\"server\":\"pg01\",\"hours_back\":168,\"status\":\"config_changes\",\"change_count\":3,\"truncated\":true,"
        + "\"changes\":[{\"changed_at\":\"2026-01-02T20:00:00\",\"name\":\"a\"},{\"changed_at\":\"2026-01-02T08:15:00\",\"name\":\"b\"},{\"changed_at\":\"2026-01-02T12:00:00\",\"name\":\"c\"}]}";

    private static NpgsqlDataSource NeverConnects() =>
        NpgsqlDataSource.Create("Host=127.0.0.1;Port=9;Username=x;Database=x;Timeout=1;Pooling=false");

    private static readonly CancellationToken Cancelled = new(canceled: true);

    /* A page of rows that is not capped, in the shape each read answers (the PostgreSQL one carries its page status). */
    private static string Rows(string read) =>
        string.Equals(read, "get_pg_server_config_changes", StringComparison.Ordinal)
            ? "{\"server\":\"pg01\",\"hours_back\":168,\"status\":\"config_changes\",\"change_count\":1,\"truncated\":false,\"changes\":[{\"changed_at\":\"2026-01-02T20:00:00\"}]}"
            : "{\"server\":\"sql01\",\"hours_back\":168,\"truncated\":false,\"order\":\"collection_time_desc\",\"changes\":[{\"x\":1}],\"runs\":[{\"collector\":\"wait_stats\"}]}";

    [Fact]
    public void EachGrid_IsAListedRead_TheWebMirrorServes_OverItsOwnSnapshotTableOrTheRunLog()
    {
        var dispatch = DarlingWebEndpoints.BuildReadDispatch();
        var tabs = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");

        foreach (var (read, table) in NewReads)
        {
            Assert.Equal(table, WebDataStartNote.TableByRead[read]);
            Assert.True(dispatch.ContainsKey(read), read + " is not a read the web mirror serves");
            Assert.Contains("\"" + read + "\"", tabs, StringComparison.Ordinal);

            Assert.True(WebDataStartNote.TryGetSource(table, out var source), read + " names " + table + ", which the probe cannot read");
            Assert.Equal(table, source.Relation);
        }

        /* The run log is its own source, edge and all; the snapshot tables are the collector tables the probe reads. */
        Assert.True(WebDataStartNote.TryGetSource("collection_log", out var log));
        Assert.Null(log.CollectorName);
        Assert.False(PerformanceMonitor.Darling.Storage.DataWindowFloor.Source.TryForCollectorTable("collection_log", out _));
    }

    /* A range of an hour or less can never be cut by the store's coverage (the probe's 90-minute slack), so none of these reads asks
       the store, which a data source that never connects, a cancelled token and a control read pin: the same answer at 168 hours
       does reach the store. */
    [Fact]
    public async Task ARangeOfAnHour_MakesNoProbeCall_ForAnyOfThem_WhereTheSameReadOverAWeekDoes()
    {
        await using var store = NeverConnects();

        foreach (var read in NewReads.Keys)
        {
            var rows = Rows(read);

            Assert.Same(rows, await WebDataStartNote.AddAsync(store, read, "sql01", 1, null, rows, null, Cancelled));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => WebDataStartNote.AddAsync(store, read, "sql01", 168, null, rows, null, Cancelled));
        }
    }

    /* A newest-first list that hit its cap ends at the oldest row it returned, whatever the store covers: the collection log's
       page (200 runs) names its oldest run at any length of window (the log keeps 60 days, so the page may ask 720 hours), and
       asks the store nothing. */
    [Fact]
    public async Task ACappedCollectionLog_NamesItsOldestRun_AtAnyWindowLength_WithoutAskingTheStore()
    {
        var ct = TestContext.Current.CancellationToken;

        var answered = await WebDataStartNote.AddAsync(null!, "get_collection_log", "sql01", 720, WindowEnd, CappedLog, null, ct);

        var answer = Assert.IsType<JsonObject>(JsonNode.Parse(answered));
        Assert.True(answer["window_truncated"]?.GetValue<bool>());
        Assert.Equal("2026-01-02T12:30:00.0000000", answer["effective_start"]?.GetValue<string>());
        Assert.Contains("back to 2026-01-02 12:30 UTC", answer["truncation_note"]?.GetValue<string>(), StringComparison.Ordinal);
        Assert.Equal("2026-01-02T12:30:00.0000000Z", answer["oldest_shown_utc"]?.GetValue<string>());
        Assert.Equal("2025-12-04T00:00:00.0000000Z", answer["window_start_utc"]?.GetValue<string>());
        Assert.Equal("2026-01-03T00:00:00.0000000Z", answer["window_end_utc"]?.GetValue<string>());
        Assert.Null(answer["data_start_utc"]);

        /* The other listed reads keep the shared 168-hour ceiling: the same cap fields on a 720-hour answer name nothing. */
        Assert.Same(CappedLog, await WebDataStartNote.AddAsync(null!, "get_waiting_tasks", "sql01", 720, WindowEnd, CappedLog, null, ct));
    }

    /* A duration floor flips the log to slowest first: the page is a sample of the whole window, so its oldest run names no reach
       and the coverage rule stands (the null source makes the probe fail, so the answer comes back as it was). */
    [Fact]
    public async Task ACollectionLogRankedSlowestFirst_KeepsTheCoverageRule()
    {
        var ct = TestContext.Current.CancellationToken;
        var slowest = CappedLog.Replace("collection_time_desc", "duration_ms_desc", StringComparison.Ordinal);

        Assert.Same(slowest, await WebDataStartNote.AddAsync(null!, "get_collection_log", "sql01", 48, WindowEnd, slowest, null, ct));
    }

    /* The PostgreSQL changes page names no oldest-returned field: its oldest row is the earliest changed_at it carries. Its status
       word names the page, not an envelope, so a page that was not cut still goes to the coverage probe, and a real envelope
       does not. */
    [Fact]
    public async Task ACappedPostgresChangesPage_NamesItsEarliestChange_AndItsPageStatusIsNotAnEnvelope()
    {
        var ct = TestContext.Current.CancellationToken;

        var answered = await WebDataStartNote.AddAsync(null!, "get_pg_server_config_changes", "pg01", 168, WindowEnd, CappedPgChanges, null, ct);

        var answer = Assert.IsType<JsonObject>(JsonNode.Parse(answered));
        Assert.True(answer["window_truncated"]?.GetValue<bool>());
        Assert.Equal("2026-01-02T08:15:00", answer["effective_start"]?.GetValue<string>());
        Assert.Contains("back to 2026-01-02 08:15 UTC", answer["truncation_note"]?.GetValue<string>(), StringComparison.Ordinal);
        Assert.Equal("2026-01-02T08:15:00.0000000Z", answer["oldest_shown_utc"]?.GetValue<string>());
        Assert.Equal(3, answer["changes"]!.AsArray().Count);

        await using var store = NeverConnects();
        const string Envelope = "{\"status\":\"no_changes\",\"message\":\"No configuration parameter changed value.\"}";
        Assert.Same(Envelope, await WebDataStartNote.AddAsync(store, "get_pg_server_config_changes", "pg01", 168, null, Envelope, null, Cancelled));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => WebDataStartNote.AddAsync(store, "get_pg_server_config_changes", "pg01", 168, null, Rows("get_pg_server_config_changes"), null, Cancelled));
    }
}

/// <summary>
/// A Custom Views panel's partial-window notice is written in the browser's zone (#4966). The run's answer carries the
/// instants its data-start notice names (<c>data_start_utc</c>, <c>window_start_utc</c>, <c>window_end_utc</c>) and the
/// sentence itself, and the shipped <c>compose.js</c>, run under Node in a zone 5.5 hours from UTC, writes the sentence again
/// through <c>windowNoteText</c> in the clock the panel's own times use. A row-cap sentence beside it stays as sent, and an
/// answer without the fields draws the sentence as the server wrote it.
/// </summary>
public sealed class ComposedPanelNoticeZoneTests
{
    private const string Panel =
        "{\"source\":\"waiting_tasks\",\"measure\":\"waiting_task_duration_ms\",\"aggregate\":\"max\",\"timeBucket\":\"hour\",\"viz\":\"line\"}";

    private const string Sentence =
        "partial window: this panel's data starts at 2026-01-02 00:00 UTC, after the window's start at 2025-12-26 00:00 UTC. The panel covers 2026-01-02 00:00 to 2026-01-03 00:00 UTC.";

    private const string RowCapSentence = "Showing the first 500 rows; narrow the window or add a filter to see the rest.";

    private static JsonObject Answer(bool withFields)
    {
        var answer = new JsonObject
        {
            ["sql"] = "SELECT 1",
            ["rows"] = new JsonArray(),
            ["annotations"] = new JsonArray(),
            ["notice"] = Sentence + " " + RowCapSentence,
        };

        if (withFields)
        {
            answer["data_start_note"] = Sentence;
            answer["data_start_utc"] = "2026-01-02T00:00:00.0000000Z";
            answer["window_start_utc"] = "2025-12-26T00:00:00.0000000Z";
            answer["window_end_utc"] = "2026-01-03T00:00:00.0000000Z";
        }

        return answer;
    }

    private static string? Drawn(bool withFields, string zone)
    {
        var scenario = new JsonObject
        {
            ["panel"] = JsonNode.Parse(Panel),
            ["scope"] = new JsonObject { ["server"] = "sql01", ["hours"] = 168 },
            ["answer"] = Answer(withFields),
        };

        if (!ComposeDataFloorLiveTests.TryRender(scenario, out var drawn, zone))
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return null;
        }

        Assert.Empty(drawn.GetProperty("errors").EnumerateArray());
        return Assert.Single(drawn.GetProperty("notices").EnumerateArray()).GetString();
    }

    [Fact]
    public void TheNotice_IsWrittenInTheBrowsersZone_NotNamedInUtc_AndTheRowCapSentenceStays()
    {
        var shown = Drawn(withFields: true, "Asia/Kolkata");
        if (shown is null)
        {
            return;
        }

        /* 2026-01-02 00:00 UTC is 05:30 in Kolkata, and the window's start 2025-12-26 05:30: the page's own localTime. */
        Assert.StartsWith("partial window: this panel's data starts at ", shown, StringComparison.Ordinal);
        Assert.DoesNotContain("UTC", shown, StringComparison.Ordinal);
        Assert.Contains("5:30", shown, StringComparison.Ordinal);
        Assert.EndsWith(" " + RowCapSentence, shown, StringComparison.Ordinal);
    }

    [Fact]
    public void AnAnswerWithoutTheInstants_IsDrawnAsTheServerSentIt()
    {
        var shown = Drawn(withFields: false, "Asia/Kolkata");
        if (shown is null)
        {
            return;
        }

        Assert.Equal(Sentence + " " + RowCapSentence, shown);
    }
}
