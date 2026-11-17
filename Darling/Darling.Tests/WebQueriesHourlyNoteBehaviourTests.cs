/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #8927: the Queries tab's Query Duration Trend, Executions per Second and Procedure Duration Trend charts are drawn from the hourly
/// rollup at "Past week", which trails the clock by two to three hours, so each line stops short of the right edge. The shipped
/// <c>pages/server-tabs.js</c>, <c>panels.js</c> and <c>util.js</c> run under Node (<c>web-queries-hourly-note-harness.mjs</c>) with a
/// stub fetch whose answers carry the scenario's <c>source</c>. An hourly source puts the trailing-hours sentence on each of the three
/// panels. A raw source says what a point covers and nothing about a trailing edge. Node is skipped when it is not installed.
/// </summary>
public sealed class WebQueriesHourlyNoteBehaviourTests
{
    private const string Trail = "The newest two or three hours are not in this chart yet";

    private static string[] NotesOf(string source, string title) => Run(source, null, title).Notes;

    private static (string[] Notes, int Points) Run(string source, string? queryStoreReply, string title)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-queries-hourly-note-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(source);
        if (queryStoreReply is not null) psi.ArgumentList.Add(queryStoreReply);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return ([], 0);
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the queries hourly note harness did not finish in 60 s for source " + source);
            }

            Assert.True(proc.ExitCode == 0, "the queries hourly note harness failed for source " + source + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            Assert.Empty(doc.RootElement.GetProperty("rejections").EnumerateArray());
            var panels = doc.RootElement.GetProperty("panels").EnumerateArray()
                .Where(p => p.GetProperty("title").GetString() == title).ToArray();
            Assert.Single(panels);
            var notes = panels[0].GetProperty("notes").EnumerateArray().Select(n => n.GetString()!).ToArray();
            var charts = doc.RootElement.GetProperty("charts").EnumerateArray()
                .Where(c => c.GetProperty("title").GetString() == title).ToArray();
            Assert.Single(charts);
            return (notes, charts[0].GetProperty("points").GetInt32());
        }
    }

    [Theory]
    [InlineData("Query Duration Trend")]
    [InlineData("Executions per Second")]
    [InlineData("Procedure Duration Trend")]
    public void HourlySource_SaysTheNewestHoursAreNotInTheChartYet(string title)
    {
        var notes = NotesOf("hourly", title);

        var note = Assert.Single(notes);
        Assert.Contains("Each point covers 1 hour.", note);
        Assert.Contains(Trail, note);
    }

    [Theory]
    [InlineData("Query Duration Trend")]
    [InlineData("Executions per Second")]
    [InlineData("Procedure Duration Trend")]
    public void RawSource_SaysWhatAPointCoversAndNothingAboutATrailingEdge(string title)
    {
        var notes = NotesOf("raw", title);

        var note = Assert.Single(notes);
        Assert.Contains("Each point covers 1 hour.", note);
        Assert.DoesNotContain(Trail, note);
    }

    [Fact]
    public void QueryStoreDurationTrend_ReadsNewestHoursFromRawRows_DrawsItsPointsAndNoTrailingHoursNote()
    {
        // The reply it really gets: source "rollup+raw" (never "hourly"), a bucket sentence, bucket_minutes null. It reads the newest
        // hours from the raw rows and names its seam itself, so the hourly sentence would be false there.
        var (notes, points) = Run("raw", null, "Query Store Duration Trend");

        Assert.Equal(3, points);
        Assert.DoesNotContain(notes, n => n.Contains(Trail));
    }

    [Fact]
    public void QueryStoreDurationTrend_DoesNotOptIntoTheNote_EvenForAReplyThatSaysHourly()
    {
        // The panel names no noteKind, so even a reply whose source is "hourly" and that states a width draws no sentence under it.
        var (notes, points) = Run("raw", "qs-hourly", "Query Store Duration Trend");

        Assert.Equal(3, points);
        Assert.Empty(notes);
    }
}
