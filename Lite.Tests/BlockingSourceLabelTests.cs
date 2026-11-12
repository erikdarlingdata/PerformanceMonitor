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

    /// <summary>#7219: the trend counts blocked-process report rows, so the label the chart wears (and the get_blocking_trend tool text) says reports,
    /// never blocked sessions, incidents or events.</summary>
    [Fact]
    public void TrendChart_LabelSaysReportsPerMinute_AndSoDoesTheToolText()
    {
        var axis = BlockingSourceLabel.For(BlockingTrendLabels.PerMinute, Trend("blocked-process-report").Select(d => d.Source));
        Assert.Equal("Blocked-process reports per minute (blocked-process-report)", axis);
        Assert.Equal("Blocked-process reports per minute", BlockingSourceLabel.For(BlockingTrendLabels.PerMinute, new List<TrendPoint>().Select(d => d.Source)));
        Assert.Equal("Blocked-process reports", BlockingTrendLabels.Reports);

        var description = typeof(PerformanceMonitorLite.Mcp.McpBlockingTools)
            .GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Instance)
            .Single(m => System.Reflection.CustomAttributeExtensions.GetCustomAttribute<ModelContextProtocol.Server.McpServerToolAttribute>(m)?.Name == "get_blocking_trend")
            .GetCustomAttributes(typeof(System.ComponentModel.DescriptionAttribute), false).Cast<System.ComponentModel.DescriptionAttribute>().Single().Description;
        Assert.Contains("blocked-process report counts per minute", description, StringComparison.Ordinal);
        Assert.DoesNotContain("blocking event counts", description, StringComparison.Ordinal);
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
}
