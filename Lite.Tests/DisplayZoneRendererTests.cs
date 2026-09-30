/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: the four shared chart renderers take an optional display zone. Passed, a renderer plots each sample at its
/// UTC time (the app's projection is not applied) and draws the bottom axis with the zone's tick generator; left
/// out, it plots the projection and draws the axis exactly as before. One case per renderer, each driving the real
/// renderer on a real chart, in both forms.
/// </summary>
public sealed class DisplayZoneRendererTests
{
    private static readonly DateTime SampleUtc = DisplayZoneFixtures.At(2026, 11, 1, 6, 30);
    private static readonly double ProjectedOffsetDays = 1.0;

    private static DateTime Project(DateTime utc) => utc.AddDays(ProjectedOffsetDays);

    private static T OnStaThread<T>(Func<T> body)
    {
        T result = default!;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { result = body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }

        return result;
    }

    /// <summary>Runs one render and returns the first plotted X and the bottom tick generator's type.</summary>
    private static (double FirstX, Type TickGenerator) Drive(Action<ScottPlot.WPF.WpfPlot> render)
        => OnStaThread(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            render(chart);
            var scatter = chart.Plot.PlottableList.OfType<ScottPlot.Plottables.Scatter>().First();
            return (scatter.Data.GetScatterPoints().First().X, chart.Plot.Axes.Bottom.TickGenerator.GetType());
        });

    private static readonly double MinX = DisplayZoneFixtures.At(2026, 11, 1, 5).ToOADate();
    private static readonly double MaxX = DisplayZoneFixtures.At(2026, 11, 1, 8).ToOADate();

    private static void AssertZoneSwitchesToUtc(Func<Func<TimeZoneInfo>?, (double FirstX, Type TickGenerator)> drive)
    {
        var withZone = drive(() => DisplayZoneFixtures.Eastern);
        Assert.Equal(SampleUtc.ToOADate(), withZone.FirstX);
        Assert.Equal(typeof(DisplayZoneTickGenerator), withZone.TickGenerator);

        var without = drive(null);
        Assert.Equal(Project(SampleUtc).ToOADate(), without.FirstX);
        Assert.Equal(typeof(ScottPlot.TickGenerators.DateTimeAutomatic), without.TickGenerator);
    }

    private sealed record SchedulerPoint(DateTime CollectionTime, int RunnableTasks, int BlockedTasks, int QueuedRequests) : ICpuSchedulerTrendPoint;

    private sealed record SessionPoint(DateTime CollectionTime) : ISessionStatsPoint
    {
        public int TotalSessions => 5;
        public int RunningSessions => 1;
        public int SleepingSessions => 2;
        public int BackgroundSessions => 1;
        public int DormantSessions => 1;
        public int IdleSessionsOver30Min => 1;
        public int SessionsWaitingForMemory => 1;
        public int DatabasesWithConnections => 1;
        public string? TopApplicationName => null;
        public int? TopApplicationConnections => null;
        public string? TopHostName => null;
        public int? TopHostConnections => null;
    }

    [Fact]
    public void CpuScheduler_WithAZone_PlotsUtcAndUsesTheZoneTicks()
        => AssertZoneSwitchesToUtc(zone => Drive(chart =>
            new CpuSchedulerChartRenderer(new ChartRenderHelper(), Project, zone)
                .Render(chart, null, new[] { new SchedulerPoint(SampleUtc, 1, 2, 3) }, MinX, MaxX)));

    [Fact]
    public void GroupedTrend_WithAZone_PlotsUtcAndUsesTheZoneTicks()
        => AssertZoneSwitchesToUtc(zone => Drive(chart =>
            new GroupedTrendChartRenderer(new ChartRenderHelper(), Project, zone)
                .Render(chart, null, new[] { SampleUtc }, t => t, _ => "group", _ => 4.0, "empty", "Y", MinX, MaxX)));

    [Fact]
    public void SessionStats_WithAZone_PlotsUtcAndUsesTheZoneTicks()
        => AssertZoneSwitchesToUtc(zone => Drive(chart =>
            new SessionStatsChartRenderer(new ChartRenderHelper(), Project, zone)
                .Render(chart, null, new[] { new SessionPoint(SampleUtc) }, MinX, MaxX)));

    [Fact]
    public void SystemHealth_WithAZone_PlotsUtcAndUsesTheZoneTicks_OnEveryChart()
    {
        var record = new SystemHealthRecord { EventTime = SampleUtc, SpinlockBackoffs = 7, SickSpinlockType = "SOS_CACHESTORE" };
        var data = new List<SystemHealthRecord> { record };

        AssertZoneSwitchesToUtc(zone => Drive(chart =>
            new SystemHealthChartRenderer(new ChartRenderHelper(), Project, zone)
                .RenderCounterChart(chart, null, data, r => r.SpinlockBackoffs, "Backoffs", "#4FC3F7", MinX, MaxX)));
        AssertZoneSwitchesToUtc(zone => Drive(chart =>
            new SystemHealthChartRenderer(new ChartRenderHelper(), Project, zone)
                .RenderSickSpinlocksChart(chart, null, data, MinX, MaxX)));
        AssertZoneSwitchesToUtc(zone => Drive(chart =>
            new SystemHealthChartRenderer(new ChartRenderHelper(), Project, zone)
                .RenderCpuComparisonChart(chart, null, data, MinX, MaxX)));
    }
}
