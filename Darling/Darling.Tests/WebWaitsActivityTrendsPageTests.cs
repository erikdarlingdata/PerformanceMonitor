/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>Source pins for the web Wait Stats and Activity tabs' get_server_trend panels: the read names, params and where each panel sits.</summary>
public sealed class WebWaitsActivityTrendsPageTests
{
    private static string Js() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js").ReplaceLineEndings("\n");

    private static string Between(string source, string from, string to)
    {
        var start = source.IndexOf(from, System.StringComparison.Ordinal);
        Assert.True(start >= 0, from);
        var end = source.IndexOf(to, start + from.Length, System.StringComparison.Ordinal);
        Assert.True(end > start, to);
        return source[start..end];
    }

    [Fact]
    public void TheWaitStatsTab_DrawsTheLatchAndSpinlockTrends_AfterTheirTables()
    {
        var waits = Between(Js(), "    id: \"waits\",\n    label: \"Wait Stats\"", "    id: \"cpu\",");
        var latchTable = waits.IndexOf("\"get_latch_stats\"", System.StringComparison.Ordinal);
        var latch = waits.IndexOf("namedTrendPanel(server, ctx, \"latch\")", System.StringComparison.Ordinal);
        var spinTable = waits.IndexOf("\"get_spinlock_stats\"", System.StringComparison.Ordinal);
        var spin = waits.IndexOf("namedTrendPanel(server, ctx, \"spinlock\")", System.StringComparison.Ordinal);
        Assert.True(latchTable >= 0 && latch > latchTable && spinTable > latch && spin > spinTable);
    }

    [Fact]
    public void TheActivityTab_DrawsTheSessionStatsTrend()
    {
        var activity = Between(Js(), "    id: \"activity\",\n    label: \"Activity\",\n    build:", "      table(\n        \"Running Jobs\"");
        Assert.Contains("sessionStatsTrendPanel(server, ctx)", activity);
    }

    [Fact]
    public void EveryTrendRead_GoesThroughTheKeptHistoryRead_WithTheRangeAndTheNamesParameter()
    {
        var js = Js();
        var session = Between(js, "export function sessionStatsTrendPanel(", "export function memoryPressurePanels(");
        Assert.Contains("readToolWithinKeptHistory(\"get_server_trend\", { server, metric: \"session_stats\", hours: ctx.hours }, ctx && ctx.signal)", session);
        var panel = Between(js, "export function namedTrendPanel(", "export async function drawNamedTrends(");
        Assert.Contains("readToolWithinKeptHistory(spec.optionsTool, { server, hours: ctx.hours, top: MAX_NAMES_CHARTED }, ctx && ctx.signal)", panel);
        var draw = Between(js, "export async function drawNamedTrends(", "const SESSION_TREND_SERIES");
        Assert.Contains("{ server, metric: kind, hours: ctx.hours, names: names.join(\",\") }", draw);
        Assert.Contains("data.missing_names", draw);
        Assert.Contains("data.hints.missing_names", draw);
        Assert.Contains("discontinuityNotes(trend.data)", draw);
    }

    [Fact]
    public void ThePicker_IsTheSharedPicker_CappedAtTenNames_KeyedByKindAndServer()
    {
        var js = Js();
        Assert.Contains("const MAX_NAMES_CHARTED = 10;", js);
        var panel = Between(js, "export function namedTrendPanel(", "export async function drawNamedTrends(");
        Assert.Contains("key: kind + \"|\" + server,", panel);
        Assert.Contains("max: MAX_NAMES_CHARTED,", panel);
        Assert.Contains("multiPicker({", panel);
    }

    [Fact]
    public void TheCharts_AreZoomable_AndRenderNoInnerHtml()
    {
        var section = Between(Js(), "const NAMED_TRENDS = {", "export function memoryPressurePanels(");
        Assert.Contains("zoomableLineChart(", section);
        Assert.DoesNotContain("innerHTML", section);
    }
}
