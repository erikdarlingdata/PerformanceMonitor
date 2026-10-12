using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5244: Lite's blocking charts name the collector that answered. The blocking trend and severity reads draw from the blocked-process-
/// report XE session when it holds a row for the chosen database, and from the DMV snapshot only when it holds none, so a chart that
/// does not say which arm it drew can look like the same data under two filters. <see cref="BlockingSourceLabel"/> builds the axis label
/// (shared with the Darling viewer); <c>ServerTab.UpdateBlockingTrendChart</c> and the two Blocking Stats charts put it on the axis
/// only when they draw rows, and the empty chart keeps its plain label.
/// </summary>
public sealed class BlockingSourceLabelTests
{
    private static readonly DateTime T0 = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static List<TrendPoint> Trend(string? source) =>
        new() { new TrendPoint { Time = T0, Count = 2, Source = source }, new TrendPoint { Time = T0.AddMinutes(1), Count = 1, Source = source } };

    private static List<BlockingDurationStatsPoint> Stats(string? source) =>
        new() { new BlockingDurationStatsPoint(T0, 2, 500, 300, 250, source) };

    [Fact]
    public void TheTags_AreTheOnesTheAlertRowsAndTheMcpAnswersCarry()
    {
        Assert.Equal(BlockedProcessAlertRow.XeReportSource, BlockingSourceLabel.BlockedProcessReport);
        Assert.Equal(BlockedProcessAlertRow.DmvSnapshotSource, BlockingSourceLabel.DmvSnapshot);
    }

    [Fact]
    public void TrendChart_XeOnly_NamesTheReports()
    {
        var label = BlockingSourceLabel.For("Blocking Incidents", Trend("blocked-process-report").Select(d => d.Source));
        Assert.Equal("Blocking Incidents (blocked-process-report)", label);
    }

    [Fact]
    public void TrendChart_DmvOnly_NamesTheDmvSnapshot()
    {
        var label = BlockingSourceLabel.For("Blocking Incidents", Trend("DMV snapshot").Select(d => d.Source));
        Assert.Equal("Blocking Incidents (DMV snapshot)", label);
    }

    [Fact]
    public void TrendChart_Empty_KeepsThePlainLabel()
    {
        Assert.Equal("Blocking Incidents", BlockingSourceLabel.For("Blocking Incidents", new List<TrendPoint>().Select(d => d.Source)));
    }

    [Fact]
    public void StatsCharts_NameTheArm_AndTheEmptyChartDoesNot()
    {
        Assert.Equal("Block Duration (ms) (blocked-process-report)", BlockingSourceLabel.For("Block Duration (ms)", Stats("blocked-process-report").Select(d => d.Source)));
        Assert.Equal("Total Block Duration (ms) (DMV snapshot)", BlockingSourceLabel.For("Total Block Duration (ms)", Stats("DMV snapshot").Select(d => d.Source)));
        Assert.Equal("Total Block Duration (ms)", BlockingSourceLabel.For("Total Block Duration (ms)", Stats(null).Take(0).Select(d => d.Source)));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("something else")]
    [InlineData("<b>x</b>")]
    public void RowsWithNoRecognizedTag_KeepThePlainLabel(string? source)
    {
        Assert.Equal("Blocking Incidents", BlockingSourceLabel.For("Blocking Incidents", Trend(source).Select(d => d.Source)));
    }

    [Fact]
    public void BlockingTrendChart_PutsTheLabelOnTheAxisOnlyWhenItDrawsRows()
    {
        var src = Read("ServerTab.Charts.cs");
        var chart = Between(src, "private void UpdateBlockingTrendChart(", "private void UpdateDeadlockTrendChart(");

        Assert.Single(Occurrences(chart, "BlockingSourceLabel.For(\"Blocking Incidents\", data.Select(d => d.Source))"));
        /* The zero-row branch returns early with the plain label: nothing answered, so nothing is named. */
        var empty = chart[..chart.IndexOf("return;", StringComparison.Ordinal)];
        Assert.Contains("YLabel(\"Blocking Incidents\")", empty, StringComparison.Ordinal);
        Assert.DoesNotContain("BlockingSourceLabel", empty, StringComparison.Ordinal);
    }

    [Fact]
    public void BlockingStatsCharts_PutTheLabelOnTheAxisOnlyWhenTheyDrawRows()
    {
        var src = Read("ServerTab.BlockingStats.cs");
        var duration = Between(src, "private void UpdateBlockingDurationChart(", "private void UpdateBlockingTotalDurationChart(");
        var total = Between(src, "private void UpdateBlockingTotalDurationChart(", "private void UpdateDeadlockWaitChart(");

        Assert.Single(Occurrences(duration, "BlockingSourceLabel.For(\"Block Duration (ms)\", data.Select(d => d.Source))"));
        Assert.Single(Occurrences(total, "BlockingSourceLabel.For(\"Total Block Duration (ms)\", data.Select(d => d.Source))"));
        Assert.DoesNotContain("BlockingSourceLabel", duration[..duration.IndexOf("return;", StringComparison.Ordinal)], StringComparison.Ordinal);
        Assert.DoesNotContain("BlockingSourceLabel", total[..total.IndexOf("return;", StringComparison.Ordinal)], StringComparison.Ordinal);
    }

    private static IEnumerable<int> Occurrences(string text, string needle)
    {
        for (var i = text.IndexOf(needle, StringComparison.Ordinal); i >= 0; i = text.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
            yield return i;
    }

    private static string Between(string text, string from, string to)
    {
        var start = text.IndexOf(from, StringComparison.Ordinal);
        Assert.True(start >= 0, from);
        var end = text.IndexOf(to, start, StringComparison.Ordinal);
        Assert.True(end > start, to);
        return text[start..end];
    }

    private static string Read(string name, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls", name))).ReplaceLineEndings("\n");
}
