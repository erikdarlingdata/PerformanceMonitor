/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Source pins for the two web charts that match the desktop's window and series: the File I/O latency panel reads
/// get_file_io_trend and draws read AND write latency, and the Version Store trend asks for seven days, the most the
/// read serves.
/// </summary>
public sealed class WebFileIoAndPvsWindowPageTests
{
    private static string Tabs => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js").ReplaceLineEndings("\n");

    private static string Pvs => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", "version-store.js").ReplaceLineEndings("\n");

    private static string FileIoPanel
    {
        get
        {
            var start = Tabs.IndexOf("export function fileIoPanel(", System.StringComparison.Ordinal);
            Assert.True(start >= 0);
            return Tabs.Substring(start, Tabs.IndexOf("return panel;", start, System.StringComparison.Ordinal) - start);
        }
    }

    [Fact]
    public void FileIoPanel_ReadsTheTrendOnce_AndPivotsReadAndWriteLatency()
    {
        var panel = FileIoPanel;
        Assert.Single(Regex.Matches(panel, "readToolWithinKeptHistory\\("));
        Assert.Contains("readToolWithinKeptHistory(\"get_file_io_trend\", { server, hours: ctx.hours })", panel);
        Assert.Contains("valueKey: \"avg_read_latency_ms\"", panel);
        Assert.Contains("valueKey: \"avg_write_latency_ms\"", panel);
        Assert.Contains("\"file-io-latency\"", panel);
        Assert.Contains("\"file-io-write-latency\"", panel);
        Assert.Contains("discontinuityNotes(res.data)", panel);
        Assert.Contains("chartZoomScope(ctx.hours)", panel);
    }

    [Fact]
    public void FileIoReadAlreadyReturnsWriteLatency_AndNoQueuedFigure()
    {
        var payloads = ReadRepoFile("PerformanceMonitor.Common", "Mcp", "TrendPayloads.cs");
        Assert.Contains("avg_write_latency_ms = Round2(p.AvgWriteLatencyMs)", payloads);
        Assert.DoesNotContain("queued", Tabs.Substring(Tabs.IndexOf("export function fileIoPanel(", System.StringComparison.Ordinal), 2500));
    }

    [Fact]
    public void VersionStore_AsksForSevenDays_WhichIsTheReadsCeiling()
    {
        Assert.Contains("const TREND_HOURS = 168;", Pvs);
        Assert.Contains("trend_hours_back: TREND_HOURS", Pvs);
        var helpers = ReadRepoFile("PerformanceMonitor.Common", "Mcp", "McpHelpers.cs");
        Assert.Contains("public const int MaxHoursBack = 168;", helpers);
        Assert.Contains("McpHelpers.ValidateHoursBack(trend_hours_back)", ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpPvsTools.cs"));
    }
}
