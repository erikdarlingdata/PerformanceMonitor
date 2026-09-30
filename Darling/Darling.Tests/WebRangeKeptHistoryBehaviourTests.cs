/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The server page's Range goes to 30 days, and most reads keep 7 (<c>McpHelpers.MaxHoursBack</c>). A read that
/// refuses the page's window for that reason is asked again, once, for the hours it keeps, and the panel shows that
/// data with a notice naming the window it covers. Run from the shipped <c>util.js</c>, <c>panels.js</c> and
/// <c>pages/server-tabs.js</c> under Node (<c>web-kept-history-harness.mjs</c>): the descriptor loader and the
/// hand-built composites, their fetches, notices, errors and chart windows. Node is skipped when it is not
/// installed, the way <see cref="AlertNotebookRenderBehaviourTests"/> does; the last test pins the routing in the
/// source text so it holds without Node too.
/// </summary>
public sealed class WebRangeKeptHistoryBehaviourTests
{
    private const string KeptNotice = "This view keeps up to 168 hours (7 days) of history, so it shows the last 7 days.";

    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-kept-history-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the kept-history harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the kept-history harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    private static string[] Strings(JsonElement node, string name) =>
        node.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

    private static int?[] ChartHours(JsonElement node) =>
        node.GetProperty("chartHours").EnumerateArray().Select(e => e.ValueKind == JsonValueKind.Null ? (int?)null : e.GetInt32()).ToArray();

    [Fact]
    public void ARefusedWindow_IsAskedAgainOnceForTheHoursTheReadKeeps_AndThePanelSaysSo()
    {
        if (!TryRun("loaderRefused", out var r)) return;

        Assert.Equal(
            new[]
            {
                "/api/read/get_cpu_utilization?server=SRV1&hours=720",
                "/api/read/get_cpu_utilization?server=SRV1&hours=168",
            },
            Strings(r, "fetches"));
        Assert.Equal(KeptNotice, Assert.Single(Strings(r, "notices")));
        Assert.Empty(Strings(r, "errors"));
        Assert.Equal(0, r.GetProperty("loading").GetInt32());
        /* The chart spans the 7 days the read answered for, not the 30 the page asked for (#2802). */
        Assert.Equal(new int?[] { 168 }, ChartHours(r));
    }

    [Fact]
    public void ARefusedWindowWhoseRetryIsEmpty_ShowsTheNoticeBesideTheEmptyAnswer()
    {
        if (!TryRun("loaderRefusedEmpty", out var r)) return;

        Assert.Equal(2, Strings(r, "fetches").Length);
        Assert.Equal(KeptNotice, Assert.Single(Strings(r, "notices")));
        Assert.Equal("No CPU samples in this window.", Assert.Single(Strings(r, "empties")));
        Assert.Empty(Strings(r, "errors"));
    }

    [Fact]
    public void AnAcceptedWindow_MakesNoSecondCall_AndShowsNoNotice()
    {
        if (!TryRun("loaderAccepted", out var loader)) return;
        Assert.Equal("/api/read/get_cpu_utilization?server=SRV1&hours=720", Assert.Single(Strings(loader, "fetches")));
        Assert.Empty(Strings(loader, "notices"));
        Assert.Equal(new int?[] { 720 }, ChartHours(loader));

        TryRun("fileIoAccepted", out var composite);
        Assert.Equal("/api/read/get_file_io_trend?server=SRV1&hours=720", Assert.Single(Strings(composite, "fetches")));
        Assert.Empty(Strings(composite, "notices"));
        Assert.Equal(new int?[] { 720 }, ChartHours(composite));
    }

    [Fact]
    public void AnyOtherError_StaysAnError_WithNoSecondCall()
    {
        if (!TryRun("loaderOtherError", out var failed)) return;
        Assert.Single(Strings(failed, "fetches"));
        Assert.Equal("The store did not answer.", Assert.Single(Strings(failed, "errors")));
        Assert.Empty(Strings(failed, "notices"));

        /* A row-cap refusal also says "exceeds maximum of", but with no hours: it is not a window refusal. */
        TryRun("loaderTopRefusal", out var top);
        Assert.Single(Strings(top, "fetches"));
        Assert.Equal("top value '1001' exceeds maximum of 1000. Use a smaller value.", Assert.Single(Strings(top, "errors")));
        Assert.Empty(Strings(top, "notices"));
    }

    [Fact]
    public void ARetryThatIsRefusedToo_IsNotAskedAThirdTime()
    {
        if (!TryRun("loaderRefusedTwice", out var r)) return;

        Assert.Equal(2, Strings(r, "fetches").Length);
        /* The second refusal falls back to the #2780 notice, which names the window the read now reports. */
        Assert.Equal(
            "This view keeps up to 96 hours (4 days) of history — pick a shorter range.",
            Assert.Single(Strings(r, "notices")));
        Assert.Empty(Strings(r, "errors"));
    }

    [Fact]
    public void AHandBuiltComposite_IsAskedAgainTheSameWay()
    {
        if (!TryRun("fileIoRefused", out var fileIo)) return;
        Assert.Equal(
            new[]
            {
                "/api/read/get_file_io_trend?server=SRV1&hours=720",
                "/api/read/get_file_io_trend?server=SRV1&hours=168",
            },
            Strings(fileIo, "fetches"));
        Assert.Equal(KeptNotice, Assert.Single(Strings(fileIo, "notices")));
        Assert.Equal(new int?[] { 168 }, ChartHours(fileIo));

        /* Wait Stats: its table read keeps 7 days; the trend read below it accepts 30 and is left unchanged. */
        TryRun("waitsTableRefusedTrendAccepted", out var waits);
        Assert.Equal(
            new[]
            {
                "/api/read/get_wait_stats?server=SRV1&hours=720&limit=20",
                "/api/read/get_wait_stats?server=SRV1&hours=168&limit=20",
                "/api/read/get_wait_trend?server=SRV1&wait_type=LCK_M_X&hours=720",
            },
            Strings(waits, "fetches"));
        Assert.Equal(KeptNotice, Assert.Single(Strings(waits, "notices")));
        Assert.Equal(new int?[] { 720 }, ChartHours(waits));
    }

    /// <summary>
    /// The view editor asks a read panel's read for a sample to find its fields: on Save for a panel with none, for
    /// Auto-detect fields, and for the field lists. The preview goes through renderPanel, so a sample read that did
    /// not follow the same rule would get the refusal: the preview showed 7 days while Auto-detect found no sample and
    /// Save refused the panel for having no fields. This runs the Save path (<c>ensureFieldConfigs</c>), and
    /// <see cref="TheLoaderAndTheComposites_RouteRangedReadsThroughTheSharedHelper"/> holds all three sample reads.
    /// </summary>
    [Fact]
    public void TheViewEditorsSampleRead_IsAskedAgainTheSameWay_SoSaveFindsTheFieldsThePreviewShows()
    {
        if (!TryRun("editorSampleRefused", out var r)) return;

        Assert.Equal(
            new[]
            {
                "/api/read/get_cpu_utilization?server=SRV1&hours=720",
                "/api/read/get_cpu_utilization?server=SRV1&hours=168",
            },
            Strings(r, "fetches"));
        var vizcfg = r.GetProperty("vizcfg");
        Assert.Equal(JsonValueKind.Object, vizcfg.ValueKind);
        Assert.Equal("samples", vizcfg.GetProperty("rowsKey").GetString());
        Assert.Equal(new[] { "sample_time", "cpu" }, vizcfg.GetProperty("columns").EnumerateArray().Select(c => c.GetProperty("key").GetString()!).ToArray());
    }

    /// <summary>
    /// Every tab of both registries at 30 days, with every read refusing it: each ranged read on the page, whether
    /// a descriptor, a fanout or a hand-built composite, is asked exactly once more at 168 hours, and no panel is
    /// left on the "pick a shorter range" notice. The picker reads (wait, counter and query trends) are in the count,
    /// so a composite that bypasses the shared helper fails here.
    /// </summary>
    [Fact]
    public void EveryRangedReadOnEveryTab_IsAskedAgainOnceAtTheHoursItKeeps()
    {
        if (!TryRun("census", out var r)) return;

        var fetches = Strings(r, "fetches");
        var hoursParam = new Regex(@"([?&])hours=(\d+)");
        var ranged = fetches.Where(f => hoursParam.IsMatch(f)).ToArray();
        Assert.All(ranged, f => Assert.Contains(hoursParam.Match(f).Groups[2].Value, new[] { "720", "168" }));

        foreach (var read in ranged.GroupBy(f => hoursParam.Replace(f, "${1}hours=")))
        {
            var asked = read.Count(f => f.Contains("hours=720", StringComparison.Ordinal));
            var retried = read.Count(f => f.Contains("hours=168", StringComparison.Ordinal));
            Assert.True(asked > 0 && asked == retried, read.Key + " was asked at 720 hours " + asked + " time(s) and at 168 hours " + retried + " time(s)");
        }

        foreach (var trend in new[] { "get_wait_trend", "get_perfmon_trend", "get_query_trend", "get_file_io_trend", "get_wait_stats", "get_top_queries_by_cpu" })
        {
            Assert.Contains(ranged, f => f.StartsWith("/api/read/" + trend + "?", StringComparison.Ordinal) && f.Contains("hours=168", StringComparison.Ordinal));
        }

        Assert.DoesNotContain(Strings(r, "notices"), n => n.Contains("pick a shorter range", StringComparison.Ordinal));
        Assert.Contains(KeptNotice, Strings(r, "notices"));
        Assert.Empty(Strings(r, "errors"));
        Assert.Empty(Strings(r, "rejections"));
        Assert.Equal(0, r.GetProperty("loading").GetInt32());
    }

    /// <summary>
    /// The same routing as source, so it holds where Node is not installed: the descriptor loader runs through the
    /// shared helper, and no hand-built composite calls <c>readTool</c> directly with a window.
    /// </summary>
    [Fact]
    public void TheLoaderAndTheComposites_RouteRangedReadsThroughTheSharedHelper()
    {
        var util = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "util.js");
        Assert.Contains("export async function readWithinKeptHistory(fetchWith, params) {", util, StringComparison.Ordinal);
        Assert.Contains("export function readToolWithinKeptHistory(tool, params, signal) {", util, StringComparison.Ordinal);
        Assert.Contains("export function keptWindowStrip(res) {", util, StringComparison.Ordinal);

        var panels = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js");
        Assert.Contains("const res = await readWithinKeptHistory(", panels, StringComparison.Ordinal);
        Assert.Contains("const kept = keptWindowStrip(res);", panels, StringComparison.Ordinal);

        var serverTabs = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        Assert.DoesNotMatch(new Regex(@"readTool\(\s*[A-Za-z_""]+\s*,\s*\{[^}]*\bhours\b"), serverTabs);
        Assert.Contains("const res = await readToolWithinKeptHistory(read, params);", serverTabs, StringComparison.Ordinal);

        /* The view editor's three sample reads (Save, Auto-detect fields, the field lists) send the panel's own params,
           hours included, so none of them may call readTool directly. */
        var editor = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "editor.js");
        Assert.DoesNotMatch(new Regex(@"\breadTool\("), editor);
        Assert.Equal(3, Regex.Matches(editor, Regex.Escape("await readToolWithinKeptHistory(p.read, cleanParams(p.params));")).Count);
    }
}
