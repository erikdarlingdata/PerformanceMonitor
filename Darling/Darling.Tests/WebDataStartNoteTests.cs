/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The server page's grids say where the data starts (#4966). A grid over a window hides a short history, so the
/// web mirror adds <c>window_truncated</c>, <c>effective_start</c> and <c>truncation_note</c> to the grid reads
/// listed in <see cref="WebDataStartNote.TableByRead"/>, and the page draws the note above the rows. These cover what
/// needs no store: the list itself, every case that must come back untouched, the page code run under Node (the
/// shipped <c>util.js</c>, <c>panels.js</c> and <c>pages/server-tabs.js</c>), and pins on the wiring. The store's
/// side, rows that start inside the range and a quiet start, is <see cref="WebDataStartNoteLiveTests"/>.
/// </summary>
public sealed class WebDataStartNoteTests
{
    private const string Rows = "{\"server\":\"sql01\",\"hours_back\":168,\"waiting_tasks\":[{\"wait_type\":\"LCK_M_X\"}]}";

    private const string FloorNote = "partial window: this panel's data starts at 2026-01-02 00:00 UTC";

    /// <summary>What <c>get_waiting_tasks</c> answers when the window held more than its row cap: the newest rows,
    /// the page's 30, with the two fields that say the cap cut the list and where the shown rows end.</summary>
    private const string CappedTasks =
        "{\"server\":\"sql01\",\"hours_back\":48,\"tasks_returned\":30,\"truncated\":true,"
        + "\"oldest_returned_collection_time\":\"2026-01-02T12:30:00.0000000\",\"newest_returned_collection_time\":\"2026-01-03T00:00:00.0000000\","
        + "\"order\":\"collection_time_desc\",\"tasks\":[{\"wait_type\":\"LCK_M_X\"}]}";

    private const string WindowEnd = "2026-01-03T00:00:00Z";

    private static string[] Strings(JsonElement node, string name) =>
        node.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

    /// <summary>A data source that never connects (nothing listens on port 9) and a token that is already cancelled: the
    /// instrument for "this answer never asked the store". A read that does reach the store throws
    /// <see cref="OperationCanceledException"/>, because neither the resolver's fault sentence nor <c>AddAsync</c>'s
    /// own catch folds a cancellation (#4203). A null source cannot show that: its throw becomes a fault sentence, and
    /// the answer comes back untouched whether or not a guard held. Every use pairs with a control that does reach
    /// the store, so an instrument that stopped seeing a reach would fail loudly rather than pass every case.</summary>
    private static NpgsqlDataSource NeverConnects() =>
        NpgsqlDataSource.Create("Host=127.0.0.1;Port=9;Username=x;Database=x;Timeout=1;Pooling=false");

    private static readonly CancellationToken Cancelled = new(canceled: true);

    private static DateTime Utc(int year, int month, int day, int hour = 0, int minute = 0) =>
        new(year, month, day, hour, minute, 0, DateTimeKind.Utc);

    [Fact]
    public void EveryListedRead_IsServedByTheWebMirror_AsAGridThePageAsksAWindowOf_OverATableTheProbeCanRead()
    {
        var dispatch = DarlingWebEndpoints.BuildReadDispatch();
        var tabs = ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");

        foreach (var (read, table) in WebDataStartNote.TableByRead)
        {
            Assert.True(dispatch.ContainsKey(read), read + " is not a read the web mirror serves");
            Assert.True(
                WebDataStartNote.TryGetSource(table, out _),
                read + " names " + table + ", which the data-start probe cannot read by index");
            Assert.Contains("\"" + read + "\"", tabs, StringComparison.Ordinal);
        }

        Assert.Equal(WebDataStartNote.TableByRead.Count, WebDataStartNote.TableByRead.Keys.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task AnyAnswerThatIsNotAGridReadOverAWindow_ComesBackUntouched_WithoutAskingTheStore()
    {
        await using var store = NeverConnects();

        /* None of these reach the store, and each one would if its guard were gone: that would throw
           OperationCanceledException here (NeverConnects), where a null source folded the throw into a fault
           sentence and handed back the same answer with or without the guard. */
        async Task<string> Run(string tool, string? server, int? hours, string payload) =>
            await WebDataStartNote.AddAsync(store, tool, server, hours, null, payload, null, Cancelled);

        // The control: a listed read with a server and a window does reach the store, and the instrument sees it.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Run("get_waiting_tasks", "sql01", 168, Rows));

        // A read that is not on the list: a chart, a newest-snapshot read, an event surface.
        Assert.Same(Rows, await Run("get_cpu_utilization", "sql01", 168, Rows));
        Assert.Same(Rows, await Run("get_blocked_process_xml", "sql01", 168, Rows));

        // A listed read with no server, no window, or a window of nothing.
        Assert.Same(Rows, await Run("get_waiting_tasks", null, 168, Rows));
        Assert.Same(Rows, await Run("get_waiting_tasks", "  ", 168, Rows));
        Assert.Same(Rows, await Run("get_waiting_tasks", "sql01", null, Rows));
        Assert.Same(Rows, await Run("get_waiting_tasks", "sql01", 0, Rows));

        // An envelope that keeps its own message (unavailable, not_collected), an error, text that is not an object, and a tool that
        // already reports its own floor (not on the listed reads any more: see below). The empty envelope is not here any more (#4966): it says the read looked and found nothing,
        // so it reaches the store like rows do (WebDataStartNoteConfigAndLogTests holds that, read by read).
        const string Empty = "{\"status\":\"unavailable\",\"message\":\"No waiting tasks in this window.\"}";
        const string NotCollected = "{\"status\":\"not_collected\",\"message\":\"This engine has no waiting tasks.\"}";
        const string Failed = "{\"error\":\"the store did not answer\"}";
        const string Own = "{\"window_truncated\":false,\"waiting_tasks\":[]}";
        Assert.Same(Empty, await Run("get_waiting_tasks", "sql01", 168, Empty));
        Assert.Same(NotCollected, await Run("get_waiting_tasks", "sql01", 168, NotCollected));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => Run("get_waiting_tasks", "sql01", 168, "{\"status\":\"empty\",\"message\":\"No waiting tasks captured in the specified time range.\"}"));
        Assert.Same(Failed, await Run("get_waiting_tasks", "sql01", 168, Failed));
        Assert.Same("[1,2]", await Run("get_waiting_tasks", "sql01", 168, "[1,2]"));
        Assert.Same("not json", await Run("get_waiting_tasks", "sql01", 168, "not json"));
        /* A tool's own window-floor keys on a LISTED read no longer end the web's decision (#4966: get_waiting_tasks writes them
           itself): they are stripped and the note is decided as if they were absent, so this answer reaches the store like rows do.
           A read outside the list is untouched whatever it carries. */
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Run("get_waiting_tasks", "sql01", 168, Own));
        Assert.Same(Own, await Run("get_query_store_top", "sql01", 168, Own));
    }

    /// <summary>
    /// A list that reads newest first under a row cap and hit it does not reach back to the window's start, whatever
    /// the store covers: the grid ends at the oldest row the read returned, so the note names that row. The answer
    /// already says where that is (<c>oldest_returned_collection_time</c>), so the note needs no probe: the null data
    /// source here would hand the answer back untouched if the note asked the store.
    /// </summary>
    [Fact]
    public async Task ACappedWaitingTasksRead_NamesTheOldestRowItReturned_WithoutAskingTheStore()
    {
        var ct = TestContext.Current.CancellationToken;

        var answered = await WebDataStartNote.AddAsync(null!, "get_waiting_tasks", "sql01", 48, WindowEnd, CappedTasks, null, ct);

        var answer = Assert.IsType<JsonObject>(JsonNode.Parse(answered));
        Assert.True(answer["window_truncated"]?.GetValue<bool>());
        Assert.Equal("2026-01-02T12:30:00.0000000", answer["effective_start"]?.GetValue<string>());
        Assert.Equal(
            "partial window: this grid shows only the newest rows, back to 2026-01-02 12:30 UTC, because it stops at its row limit. "
            + "The window started at 2026-01-01 00:00 UTC. The grid covers 2026-01-02 12:30 to 2026-01-03 00:00 UTC.",
            answer["truncation_note"]?.GetValue<string>());

        /* The same instants as fields (#4966): the page composes the note from these in the browser's zone, and the
           sentence above stays for any other reader. A capped list names the oldest row it shows, never a data start. */
        Assert.Equal("2026-01-02T12:30:00.0000000Z", answer["oldest_shown_utc"]?.GetValue<string>());
        Assert.Equal("2026-01-01T00:00:00.0000000Z", answer["window_start_utc"]?.GetValue<string>());
        Assert.Equal("2026-01-03T00:00:00.0000000Z", answer["window_end_utc"]?.GetValue<string>());
        Assert.Null(answer["data_start_utc"]);

        /* The tool's own fields and rows come through as they were. */
        Assert.True(answer["truncated"]?.GetValue<bool>());
        Assert.Equal(30, answer["tasks_returned"]?.GetValue<int>());
        Assert.Single(answer["tasks"]!.AsArray());
    }

    /// <summary>
    /// #4966: the tool writes its own <c>effective_start</c>, <c>window_truncated</c> and <c>truncation_note</c> after
    /// <c>hours_back</c> (the MCP dialect, in UTC). On a listed read the web still decides its note itself: a capped
    /// <c>get_waiting_tasks</c> payload that carries the tool's <c>window_truncated: false</c> gets the web's capped
    /// note, byte for byte what the same payload without the tool's keys gets.
    /// </summary>
    [Fact]
    public async Task ACappedWaitingTasksRead_ThatCarriesTheToolsOwnWindowKeys_StillGetsTheWebsCappedNote()
    {
        var ct = TestContext.Current.CancellationToken;
        var withKeys = CappedTasks.Replace(
            "\"hours_back\":48,",
            "\"hours_back\":48,\"effective_start\":\"2026-01-01T00:00:00.0000000Z\",\"window_truncated\":false,\"truncation_note\":null,",
            StringComparison.Ordinal);
        Assert.NotEqual(CappedTasks, withKeys);

        var plain = await WebDataStartNote.AddAsync(null!, "get_waiting_tasks", "sql01", 48, WindowEnd, CappedTasks, null, ct);
        var answered = await WebDataStartNote.AddAsync(null!, "get_waiting_tasks", "sql01", 48, WindowEnd, withKeys, null, ct);

        var answer = Assert.IsType<JsonObject>(JsonNode.Parse(answered));
        Assert.True(answer["window_truncated"]?.GetValue<bool>());
        Assert.Equal("2026-01-02T12:30:00.0000000", answer["effective_start"]?.GetValue<string>());
        Assert.StartsWith("partial window: this grid shows only the newest rows, back to 2026-01-02 12:30 UTC", answer["truncation_note"]?.GetValue<string>(), StringComparison.Ordinal);
        Assert.Equal("2026-01-02T12:30:00.0000000Z", answer["oldest_shown_utc"]?.GetValue<string>());
        Assert.Equal(1, answer.Count(p => p.Key == "window_truncated"));

        /* The same fields the plain payload got, with the same values (key order aside: the tool's keys sit earlier). */
        var plainAnswer = Assert.IsType<JsonObject>(JsonNode.Parse(plain));
        foreach (var (key, value) in plainAnswer)
        {
            Assert.Equal(value?.ToJsonString(), answer[key]?.ToJsonString());
        }
    }

    /// <summary>
    /// #4966: the active-queries panel is not in <see cref="WebDataStartNote.TableByRead"/>, so the page draws the tool's
    /// own <c>truncation_note</c> as it comes, through <c>windowFloorStrip</c> (a grid only, and only when
    /// <c>window_truncated</c> is true). That is safe without a page change: the note names no instant, so the page
    /// has no UTC time to convert and nothing in it mixes two clocks; <c>windowNoteText</c> hands a sentence with no
    /// matching instant back as sent.
    /// </summary>
    [Fact]
    public void AToolsOwnTruncationNote_OnAPanelOutsideTheList_NamesNoInstant_AndThePageDrawsItOnlyForATruncatedGrid()
    {
        Assert.DoesNotContain("get_active_queries", WebDataStartNote.TableByRead.Keys);
        var notices = new[]
        {
            DarlingMcpWindowNotice.Build(null, Utc(2026, 1, 1), "query_snapshots", emptyAnswer: true).TruncationNote!,
            DarlingMcpWindowNotice.Build(Utc(2026, 1, 3), Utc(2026, 1, 1), "query_snapshots").TruncationNote!,
        };
        foreach (var note in notices)
        {
            Assert.DoesNotMatch(@"\d{4}-\d{2}-\d{2}|\d{1,2}:\d{2}", note);
        }

        var util = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "util.js").ReplaceLineEndings("\n");
        Assert.Contains("if (!desc || desc.viz !== \"table\") return null;", util, StringComparison.Ordinal);
        Assert.Contains("if (!data || data.window_truncated !== true) return null;", util, StringComparison.Ordinal);
        Assert.Contains("return sent;\n}", util, StringComparison.Ordinal);
    }

    /// <summary>
    /// The capped rule is for the one list ordered by time. A read that did not hit its cap, a capped answer with no
    /// usable oldest row, and every other listed read, whose cap keeps the top rows of an aggregate over the whole
    /// window (or of a list ranked by something other than time, <c>get_pg_predicate_stats</c>), stay on the
    /// coverage rule: the answer comes back through the probe, and the null data source makes the probe fail, so it
    /// is returned as it was. A capped read the old rule rewrote would not be the same instance.
    /// </summary>
    [Fact]
    public async Task OnlyTheNewestFirstListIsCutByItsCap_EveryOtherAnswerKeepsTheCoverageRule()
    {
        var ct = TestContext.Current.CancellationToken;

        async Task<string> Run(string tool, string payload) =>
            await WebDataStartNote.AddAsync(null!, tool, "sql01", 48, WindowEnd, payload, null, ct);

        // The list's cap was not reached.
        var uncapped = CappedTasks.Replace("\"truncated\":true", "\"truncated\":false", StringComparison.Ordinal);
        Assert.Same(uncapped, await Run("get_waiting_tasks", uncapped));

        // Capped, but the answer does not say where its rows end (or says it unreadably): nothing to name.
        const string NoOldest = "{\"server\":\"sql01\",\"truncated\":true,\"tasks\":[{\"wait_type\":\"LCK_M_X\"}]}";
        const string BadOldest = "{\"server\":\"sql01\",\"truncated\":true,\"oldest_returned_collection_time\":\"not a time\",\"tasks\":[{\"wait_type\":\"LCK_M_X\"}]}";
        Assert.Same(NoOldest, await Run("get_waiting_tasks", NoOldest));
        Assert.Same(BadOldest, await Run("get_waiting_tasks", BadOldest));

        // Every listed read the cap does not cut (the capped list names its own), with the same cap fields on its answer.
        foreach (var read in WebDataStartNote.TableByRead.Keys.Where(k => !WebDataStartNote.NewestFirstCappedReads.Contains(k)))
        {
            Assert.Same(CappedTasks, await Run(read, CappedTasks));
        }
    }

    /// <summary>The capped rule's premises, in the source: each list is a read ordered by time, newest first, and
    /// the tool answers the two fields the rule reads (the live tests run the real tool). The collection log and the
    /// PostgreSQL configuration changes join the waiting tasks (#4966): <c>WebDataStartNoteConfigAndLogTests</c> pins
    /// theirs.</summary>
    [Fact]
    public void TheCappedRule_IsThreeReadsOrderedByTime_WhoseToolsAnswerTheFieldsItReads()
    {
        Assert.Equal(
            ["get_collection_log", "get_pg_server_config_changes", "get_waiting_tasks"],
            WebDataStartNote.NewestFirstCappedReads.Order(StringComparer.Ordinal).ToArray());
        foreach (var read in WebDataStartNote.NewestFirstCappedReads)
        {
            Assert.Contains(read, WebDataStartNote.TableByRead.Keys);
        }

        Assert.Contains("ORDER BY collection_time DESC", PerformanceMonitor.Darling.Service.Mcp.DarlingSessionReader.WaitingTasksSql, StringComparison.Ordinal);
        var tools = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpSessionTools.cs");
        var tool = tools.IndexOf("public static async Task<string> GetWaitingTasks(", StringComparison.Ordinal);
        Assert.True(tool > 0);
        var body = tools[tool..];
        Assert.Contains("truncated,", body, StringComparison.Ordinal);
        /* #4966: the field is written through the shared formatter (UTC, with the Z); the rule reads it as UTC with or without one. */
        Assert.Contains("oldest_returned_collection_time = McpHelpers.FormatEffectiveStart(page.Min(r => r.CollectionTime))", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheWebMirror_AsksAfterTheTool_InsideTheTryThatOwnsCancellation()
    {
        var endpoints = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");
        var call = endpoints.IndexOf("await WebDataStartNote.AddAsync(", StringComparison.Ordinal);
        var tool = endpoints.IndexOf("result = await handler(context, postgres, analysis);", StringComparison.Ordinal);
        var cancelled = endpoints.IndexOf("catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)", tool, StringComparison.Ordinal);

        Assert.True(tool > 0 && call > tool && call < cancelled, "the note is added to the tool's answer, before the catch that classifies a cancelled request");
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(endpoints, @"WebDataStartNote\.AddAsync\("));
    }

    [Fact]
    public void AGridWhoseTableStartsLate_ShowsWhereTheDataStarts()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("floorTableTruncated", out var r)) return;

        var notice = Assert.Single(Strings(r, "notices"));
        Assert.StartsWith("partial window: this panel's data starts at 2026-01-02 00:00 UTC", notice, StringComparison.Ordinal);
        Assert.Single(Strings(r, "fetches"));
        Assert.Empty(Strings(r, "errors"));
    }

    [Fact]
    public void AGridTheTableCovered_ShowsNoNotice()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("floorTableCovered", out var r)) return;

        Assert.Empty(Strings(r, "notices"));
        Assert.Empty(Strings(r, "errors"));
    }

    [Fact]
    public void AChart_NeverDrawsTheNote_ItsAxisAlreadySpansTheRange()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("floorChartIgnored", out var r)) return;

        Assert.Empty(Strings(r, "notices"));
    }

    [Fact]
    public void AGridThatRendersTruncationNoteItself_DrawsItOnce()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("floorOwnNoteOnce", out var r)) return;

        Assert.StartsWith("partial window:", Assert.Single(Strings(r, "notices")), StringComparison.Ordinal);
    }

    /// <summary>
    /// An empty grid is where a short history misleads most (#4966): "no changes" over a server added two days ago reads as
    /// quiet when it is only new. The server adds the note to the empty answer, and the page draws it above the empty message,
    /// composed in the browser's zone like the note over rows. An empty answer without the note fields (unavailable, or a
    /// covered window) draws only its message.
    /// </summary>
    [Fact]
    public void AnEmptyGrid_ShowsTheNoteAboveItsMessage_InTheBrowsersZone_AndWithoutTheFieldsOnlyTheMessage()
    {
        var answer = CoverageAnswer();
        answer.Remove("tasks");
        answer["status"] = "empty";
        answer["message"] = "No waiting tasks were captured in this window.";
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("floorLocal:" + NewYork, out var r, answer.ToJsonString())) return;

        Assert.NotEqual(0, r.GetProperty("tzOffset").GetInt32());
        var notice = Assert.Single(Strings(r, "notices"));
        Assert.Equal(InTheBrowsersZone(answer["truncation_note"]!.GetValue<string>(), r.GetProperty("local")), notice);
        Assert.DoesNotContain("UTC", notice, StringComparison.Ordinal);
        Assert.Equal("No waiting tasks were captured in this window.", Assert.Single(Strings(r, "empties")));

        var plain = "{\"status\":\"unavailable\",\"message\":\"No latch statistics available in the requested time range.\"}";
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("floorLocal:" + NewYork, out var p, plain)) return;
        Assert.Empty(Strings(p, "notices"));
        Assert.Equal("No latch statistics available in the requested time range.", Assert.Single(Strings(p, "empties")));
    }

    [Fact]
    public void TheWaitStatsGrid_ShowsTheNoteAboveItsRows()
    {
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("floorWaitStats", out var r)) return;

        Assert.StartsWith("partial window:", Assert.Single(Strings(r, "notices")), StringComparison.Ordinal);
    }

    /// <summary>The page wiring in the source text, so it holds without Node: the shared strip is exported, a
    /// descriptor panel draws it for a grid only, and the two hand-built paths that do not go through the descriptor
    /// loader (the fanout tables and the Wait Stats grid) draw it too.</summary>
    [Fact]
    public void TheNote_IsDrawnByEveryGridPath_AndByNoChartPath()
    {
        var util = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "util.js");
        var panels = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js");
        var tabs = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");

        Assert.Contains("export function windowFloorStrip(data, desc) {", util, StringComparison.Ordinal);
        Assert.Contains("desc.viz !== \"table\"", util, StringComparison.Ordinal);
        Assert.Contains("data.window_truncated !== true", util, StringComparison.Ordinal);

        Assert.Contains("const floor = windowFloorStrip(res.data, desc);", panels, StringComparison.Ordinal);

        /* #4966: the empty envelope carries the note too, drawn above its message, and a read's answer is composed in the
           browser's zone for the envelope as it is for rows. */
        Assert.Contains("mount(body, [kept, windowFloorStrip(res.data, desc), emptyStrip(res.message)]);", panels, StringComparison.Ordinal);
        Assert.Contains("res.kind !== \"data\" && res.kind !== \"empty\"", util, StringComparison.Ordinal);

        Assert.Contains("windowFloorStrip(res.data, spec)", tabs, StringComparison.Ordinal);
        Assert.Contains("const parts = [keptWindowStrip(res), windowFloorStrip(res.data, { viz: \"table\" }), VIZ.table(res.data, { rowsKey: \"waits\"", tabs, StringComparison.Ordinal);

        /* The strip is a grid's. No line path asks for it. */
        Assert.DoesNotContain("windowFloorStrip(trend", tabs, StringComparison.Ordinal);

        /* The coverage sentence the server builds opens the way the page's composer keeps it. (This line used to assert
           that the FloorNote constant was not empty: a constant, so it could not fail.) */
        Assert.StartsWith(FloorNote, ComposeStoreAvailability.BuildDataStartNotice(Utc(2026, 1, 2), Utc(2025, 12, 26), Utc(2026, 1, 3)), StringComparison.Ordinal);
    }

    /// <summary>
    /// A window no longer than the 90-minute slack can never be called cut by the store: the coverage rule needs a start
    /// more than the slack after the window's start, and the probe reports none at or after the window's end. So such an
    /// answer comes back without the registry read or the floor read (<see cref="NeverConnects"/>: asking would throw).
    /// The capped list does not use the probe, so a capped read over the same hour still gets its note.
    /// </summary>
    [Fact]
    public async Task AWindowNoLongerThanTheSlack_NeverAsksTheStoreForCoverage_ButACappedListStillGetsItsNote()
    {
        await using var store = NeverConnects();
        Task<string> Run(string tool, int hours, string payload) =>
            WebDataStartNote.AddAsync(store, tool, "sql01", hours, WindowEnd, payload, null, Cancelled);

        Assert.True(
            TimeSpan.FromHours(1) <= DurationTrendRouting.TruncationSlack && TimeSpan.FromHours(2) > DurationTrendRouting.TruncationSlack,
            "one hour is inside the slack, two hours is past it");

        // The control: two hours is past the slack, so the coverage read happens and the instrument sees it.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Run("get_latch_stats", 2, Rows));
        // One hour is inside it: no read, the same answer back.
        Assert.Same(Rows, await Run("get_latch_stats", 1, Rows));

        // A capped list over that hour: its oldest row is inside the window, and the note names it.
        var capped = CappedTasks
            .Replace("\"hours_back\":48", "\"hours_back\":1", StringComparison.Ordinal)
            .Replace("2026-01-02T12:30:00.0000000", "2026-01-02T23:30:00.0000000", StringComparison.Ordinal);
        var answer = Assert.IsType<JsonObject>(JsonNode.Parse(await Run("get_waiting_tasks", 1, capped)));
        Assert.True(answer["window_truncated"]?.GetValue<bool>());
        Assert.Equal(
            "partial window: this grid shows only the newest rows, back to 2026-01-02 23:30 UTC, because it stops at its row limit. "
            + "The window started at 2026-01-02 23:00 UTC. The grid covers 2026-01-02 23:30 to 2026-01-03 00:00 UTC.",
            answer["truncation_note"]?.GetValue<string>());
    }

    private const string NewYork = "America/New_York";

    /// <summary>The server's sentence with each UTC instant it names swapped for what the page's own <c>localTime</c>
    /// makes of that instant (the harness reports it per instant): what the page must draw when it composes the note in
    /// the browser's zone, with the wording the server's sentence has.</summary>
    private static string InTheBrowsersZone(string sentence, JsonElement local) =>
        Regex.Replace(sentence, @"\d{4}-\d{2}-\d{2} \d{2}:\d{2}( UTC)?", m => local.GetProperty(m.Value[..16]).GetString()!);

    /// <summary>What a coverage note's answer carries: the sentence the server builds, and the three instants as fields.</summary>
    private static JsonObject CoverageAnswer() => new()
    {
        ["tasks"] = new JsonArray(new JsonObject { ["wait_type"] = "LCK_M_X" }),
        ["window_truncated"] = true,
        ["effective_start"] = "2026-01-02T00:00:00.0000000",
        ["truncation_note"] = ComposeStoreAvailability.BuildDataStartNotice(Utc(2026, 1, 2), Utc(2025, 12, 26), Utc(2026, 1, 3)),
        ["data_start_utc"] = "2026-01-02T00:00:00.0000000Z",
        ["window_start_utc"] = "2025-12-26T00:00:00.0000000Z",
        ["window_end_utc"] = "2026-01-03T00:00:00.0000000Z",
    };

    /// <summary>
    /// The grids print their times in the browser's zone (<c>localTime</c>), so a note above one that named its instants
    /// in UTC mixed two clocks on one page. The page composes the note again from the instants the answer carries, with
    /// <c>localTime</c>, in the zone the browser is in (the harness runs it in New York): the same wording, the
    /// instants the grid's own times show, no "UTC" left in it.
    /// </summary>
    [Fact]
    public void ACoverageNote_IsComposedInTheBrowsersZone_FromTheFields_NotNamedInUtc()
    {
        var answer = CoverageAnswer();
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("floorLocal:" + NewYork, out var r, answer.ToJsonString())) return;

        Assert.NotEqual(0, r.GetProperty("tzOffset").GetInt32());
        var notice = Assert.Single(Strings(r, "notices"));
        Assert.Equal(InTheBrowsersZone(answer["truncation_note"]!.GetValue<string>(), r.GetProperty("local")), notice);
        Assert.DoesNotContain("UTC", notice, StringComparison.Ordinal);
        Assert.Empty(Strings(r, "errors"));
    }

    [Fact]
    public async Task ACappedListNote_IsComposedInTheBrowsersZone_FromTheFields_NotNamedInUtc()
    {
        var ct = TestContext.Current.CancellationToken;
        var answered = await WebDataStartNote.AddAsync(null!, "get_waiting_tasks", "sql01", 48, WindowEnd, CappedTasks, null, ct);
        if (!WebRangeKeptHistoryBehaviourTests.TryRun("floorLocal:" + NewYork, out var r, answered)) return;

        var sentence = JsonNode.Parse(answered)!["truncation_note"]!.GetValue<string>();
        Assert.NotEqual(0, r.GetProperty("tzOffset").GetInt32());
        var notice = Assert.Single(Strings(r, "notices"));
        Assert.Equal(InTheBrowsersZone(sentence, r.GetProperty("local")), notice);
        Assert.DoesNotContain("UTC", notice, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4966: Darling's capped lists print where their rows stop as UTC with the Z (the shared formatter), where they used
    /// to print the store's naive form. The web reads either: the field is read as UTC both ways, so the note, the
    /// instants beside it and the page's composition in the browser's zone are what the naive answer gave, and
    /// <c>effective_start</c> keeps the tool's own text.
    /// </summary>
    [Fact]
    public async Task ACappedList_WhoseOldestRowIsStampedUtc_ReadsAsTheNaiveOneDid_OnTheServerAndOnThePage()
    {
        var ct = TestContext.Current.CancellationToken;
        var stamped = CappedTasks.Replace("2026-01-02T12:30:00.0000000\"", "2026-01-02T12:30:00.0000000Z\"", StringComparison.Ordinal);
        Assert.NotEqual(CappedTasks, stamped);

        var naiveAnswered = await WebDataStartNote.AddAsync(null!, "get_waiting_tasks", "sql01", 48, WindowEnd, CappedTasks, null, ct);
        var answered = await WebDataStartNote.AddAsync(null!, "get_waiting_tasks", "sql01", 48, WindowEnd, stamped, null, ct);

        var naive = Assert.IsType<JsonObject>(JsonNode.Parse(naiveAnswered));
        var answer = Assert.IsType<JsonObject>(JsonNode.Parse(answered));
        Assert.Equal("2026-01-02T12:30:00.0000000Z", answer["effective_start"]?.GetValue<string>());
        foreach (var key in new[] { "truncation_note", "oldest_shown_utc", "window_start_utc", "window_end_utc" })
        {
            Assert.Equal(naive[key]?.GetValue<string>(), answer[key]?.GetValue<string>());
        }

        if (!WebRangeKeptHistoryBehaviourTests.TryRun("floorLocal:" + NewYork, out var r, answered)) return;

        var sentence = answer["truncation_note"]!.GetValue<string>();
        var notice = Assert.Single(Strings(r, "notices"));
        Assert.Equal(InTheBrowsersZone(sentence, r.GetProperty("local")), notice);
        Assert.DoesNotContain("UTC", notice, StringComparison.Ordinal);
    }

    /// <summary>A note whose answer lacks an instant (an older server, another reader), or carries one the page cannot
    /// read, is drawn as the server sent it: the sentence, in UTC. Better a note in UTC than none.</summary>
    [Fact]
    public void ANoteWithoutAllItsFields_IsDrawnAsTheServerSentIt()
    {
        var none = CoverageAnswer();
        none.Remove("data_start_utc");
        none.Remove("window_start_utc");
        none.Remove("window_end_utc");
        var partial = CoverageAnswer();
        partial.Remove("window_end_utc");
        var unreadable = CoverageAnswer();
        unreadable["data_start_utc"] = "not a time";

        foreach (var answer in new[] { none, partial, unreadable })
        {
            if (!WebRangeKeptHistoryBehaviourTests.TryRun("floorLocal:" + NewYork, out var r, answer.ToJsonString())) return;
            Assert.Equal(answer["truncation_note"]!.GetValue<string>(), Assert.Single(Strings(r, "notices")));
        }
    }

    /// <summary>
    /// The Queries tab's window notes (#4231) get the same clock. Two of the Queries reads word their note with no time in
    /// it; the Query Store note names the instant its history starts when the interval table served it, as the
    /// <c>effective_start</c> the answer carries. The page shows that instant in the browser's zone, on the descriptor grid
    /// and on the Top Queries composite, and the MCP answer's own text (in UTC) is not touched. The note is the one the
    /// tool serves (#4966), so the instant it names is the field's exact text, Z included: that is what the page finds.
    /// </summary>
    [Fact]
    public void TheQueryStoreNote_NamesItsInstantInTheBrowsersZone_OnTheGridAndOnTheTopQueriesComposite()
    {
        var effectiveStart = new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Unspecified);
        var stamp = PerformanceMonitor.Common.McpHelpers.FormatEffectiveStart(effectiveStart);
        var note = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpDataTools.QueryStoreTableNote(effectiveStart, QueryStoreIntervalWide.WideStartBound.FilledSince);
        Assert.Contains(stamp, note, StringComparison.Ordinal);
        var answer = new JsonObject
        {
            ["queries"] = new JsonArray(new JsonObject { ["query_id"] = 1, ["query_text"] = "select 1" }),
            ["window_truncated"] = true,
            ["effective_start"] = stamp,
            ["truncation_note"] = note,
        };

        foreach (var scenario in new[] { "queryStoreLocal", "topQueriesLocal" })
        {
            if (!WebRangeKeptHistoryBehaviourTests.TryRun(scenario + ":" + NewYork, out var r, answer.ToJsonString())) return;

            var notice = Assert.Single(Strings(r, "notices"));
            Assert.Equal(note.Replace(stamp, r.GetProperty("local").GetProperty(stamp).GetString()!, StringComparison.Ordinal), notice);
            Assert.DoesNotContain(stamp, notice, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void AQueriesNoteThatNamesNoInstant_IsDrawnAsSent()
    {
        const string Note = "The window reaches further back than this server's raw query_stats retains (or this server has been monitored for less time than that), so the older part of it was not read.";
        var answer = new JsonObject
        {
            ["queries"] = new JsonArray(new JsonObject { ["query_id"] = 1, ["query_text"] = "select 1" }),
            ["window_truncated"] = true,
            ["effective_start"] = "2026-01-02T00:00:00.0000000",
            ["truncation_note"] = Note,
        };

        foreach (var scenario in new[] { "queryStoreLocal", "topQueriesLocal" })
        {
            if (!WebRangeKeptHistoryBehaviourTests.TryRun(scenario + ":" + NewYork, out var r, answer.ToJsonString())) return;
            Assert.Equal(Note, Assert.Single(Strings(r, "notices")));
        }
    }
}
