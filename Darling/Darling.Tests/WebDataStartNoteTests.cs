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
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
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

    private static string[] Strings(JsonElement node, string name) =>
        node.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

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
                DataWindowFloor.Source.TryForCollectorTable(table, out _),
                read + " names " + table + ", which the data-start probe cannot read by index");
            Assert.Contains("\"" + read + "\"", tabs, StringComparison.Ordinal);
        }

        Assert.Equal(WebDataStartNote.TableByRead.Count, WebDataStartNote.TableByRead.Keys.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task AnyAnswerThatIsNotAGridReadOverAWindow_ComesBackUntouched()
    {
        var ct = TestContext.Current.CancellationToken;

        /* None of these reach the store: the null data source would throw if one did. */
        async Task<string> Run(string tool, string? server, int? hours, string payload) =>
            await WebDataStartNote.AddAsync(null!, tool, server, hours, null, payload, null, ct);

        // A read that is not on the list: a chart, a newest-snapshot read, an event surface.
        Assert.Same(Rows, await Run("get_cpu_utilization", "sql01", 168, Rows));
        Assert.Same(Rows, await Run("get_blocked_process_xml", "sql01", 168, Rows));

        // A listed read with no server, no window, or a window of nothing.
        Assert.Same(Rows, await Run("get_waiting_tasks", null, 168, Rows));
        Assert.Same(Rows, await Run("get_waiting_tasks", "  ", 168, Rows));
        Assert.Same(Rows, await Run("get_waiting_tasks", "sql01", null, Rows));
        Assert.Same(Rows, await Run("get_waiting_tasks", "sql01", 0, Rows));

        // An envelope, an error, text that is not an object, and a tool that already reports its own floor.
        const string Empty = "{\"status\":\"empty\",\"message\":\"No waiting tasks in this window.\"}";
        const string Failed = "{\"error\":\"the store did not answer\"}";
        const string Own = "{\"window_truncated\":false,\"waiting_tasks\":[]}";
        Assert.Same(Empty, await Run("get_waiting_tasks", "sql01", 168, Empty));
        Assert.Same(Failed, await Run("get_waiting_tasks", "sql01", 168, Failed));
        Assert.Same("[1,2]", await Run("get_waiting_tasks", "sql01", 168, "[1,2]"));
        Assert.Same("not json", await Run("get_waiting_tasks", "sql01", 168, "not json"));
        Assert.Same(Own, await Run("get_waiting_tasks", "sql01", 168, Own));
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

        Assert.Contains("export function windowFloorStrip(data) {", util, StringComparison.Ordinal);
        Assert.Contains("data.window_truncated !== true", util, StringComparison.Ordinal);

        Assert.Contains("desc.viz === \"table\" && desc.noteKey !== \"truncation_note\"", panels, StringComparison.Ordinal);
        Assert.Contains("? windowFloorStrip(res.data)", panels, StringComparison.Ordinal);

        Assert.Contains("spec.viz === \"table\" && spec.noteKey !== \"truncation_note\" ? windowFloorStrip(res.data) : null", tabs, StringComparison.Ordinal);
        Assert.Contains("const parts = [keptWindowStrip(res), windowFloorStrip(res.data), VIZ.table(res.data, { rowsKey: \"waits\"", tabs, StringComparison.Ordinal);

        /* The strip is a grid's. No line path asks for it. */
        Assert.DoesNotContain("windowFloorStrip(trend", tabs, StringComparison.Ordinal);
        Assert.NotEmpty(FloorNote);
    }
}
