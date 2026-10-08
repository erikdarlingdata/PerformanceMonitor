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

/// <summary>Source pins for the web CPU and Memory tabs' get_server_trend panels: the read names and params, and where each panel sits.</summary>
public sealed class WebServerTrendsPageTests
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
    public void TheCpuTab_DrawsTheSchedulerTrend()
    {
        var cpu = Between(Js(), "    id: \"cpu\",", "    id: \"memory\",");
        Assert.Contains("serverTrendPanel(server, ctx, \"cpu_scheduler\")", cpu);
    }

    [Fact]
    public void TheMemoryTab_DrawsTheClerksAndPlanCacheTrends()
    {
        var memory = Between(Js(), "    id: \"memory\",", "      ...memoryPressurePanels(server, ctx),");
        Assert.Contains("memoryClerksTrendPanel(server, ctx)", memory);
        Assert.Contains("serverTrendPanel(server, ctx, \"plan_cache\")", memory);
    }

    [Fact]
    public void EveryTrendRead_GoesThroughTheKeptHistoryRead_WithTheRangeAndTheSignal()
    {
        var js = Js();
        var panel = Between(js, "export function serverTrendPanel(", "const MAX_CLERKS_CHARTED");
        Assert.Contains("readToolWithinKeptHistory(\"get_server_trend\", { server, metric: spec.metric, hours: ctx.hours }, ctx && ctx.signal)", panel);
        var clerks = Between(js, "export function memoryClerksTrendPanel(", "export async function drawClerkTrends(");
        Assert.Contains("readToolWithinKeptHistory(\"get_memory_clerks\", { server }, ctx && ctx.signal)", clerks);
        var draw = Between(js, "export async function drawClerkTrends(", "export function memoryPressurePanels(");
        Assert.Contains("\"get_server_trend\",", draw);
        Assert.Contains("{ server, metric: \"memory_clerks\", hours: ctx.hours, clerk_types: clerkTypes.join(\",\") }", draw);
        Assert.Contains("discontinuityNotes(trend.data)", draw);
        Assert.Contains("missing_clerk_types", draw);
    }

    [Fact]
    public void TheClerkSelector_IsTheSharedPicker_CappedAtTheReadsTenClerkTypes_KeyedByServer()
    {
        var js = Js();
        Assert.Contains("const MAX_CLERKS_CHARTED = 10;", js);
        var clerks = Between(js, "export function memoryClerksTrendPanel(", "export async function drawClerkTrends(");
        Assert.Contains("const key = \"clerks|\" + server;", clerks);
        Assert.Contains("max: MAX_CLERKS_CHARTED,", clerks);
        Assert.Contains("multiPicker({", clerks);
    }

    [Fact]
    public void TheTrendCharts_AreZoomable_AndRenderNoInnerHtml()
    {
        var js = Js();
        var section = Between(js, "const SERVER_TRENDS = {", "export function memoryPressurePanels(");
        Assert.Contains("zoomableLineChart(", section);
        Assert.DoesNotContain("innerHTML", section);
    }
}
