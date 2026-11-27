/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitorLite.Helpers;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9853 - the start-up timeline writes each phase in the order it was marked, with the time since the mark before it, and a
/// total that is the sum of the phases. A fake clock gives the timings, so no test waits.
/// </summary>
public sealed class StartupTimelineTests
{
    [Fact]
    public void Format_names_each_phase_in_mark_order_with_its_own_duration_and_the_sum_as_total()
    {
        long now = 5_000;
        var timeline = new StartupTimeline(14_000, "process start to OnStartup", () => now);

        now += 120;
        timeline.Mark("logger initialize");
        now += 7_300;
        timeline.Mark("MainWindow InitializeComponent");
        now += 40;
        timeline.Mark("ServerManager load");

        Assert.Equal(
            "Startup phases: process start to OnStartup 14000 ms, logger initialize 120 ms, "
            + "MainWindow InitializeComponent 7300 ms, ServerManager load 40 ms, total 21460 ms",
            timeline.Format());
    }

    [Fact]
    public void A_clock_that_reads_earlier_than_the_last_mark_gives_a_zero_phase_not_a_negative_one()
    {
        long now = 1_000;
        var timeline = new StartupTimeline(0, "first", () => now);

        now = 900;
        timeline.Mark("second");

        Assert.Equal("Startup phases: first 0 ms, second 0 ms, total 0 ms", timeline.Format());
    }

    [Fact]
    public void Report_returns_the_line_once_and_nothing_marks_or_reports_after_it()
    {
        StartupTimeline.Begin("process start to OnStartup");
        StartupTimeline.MarkPhase("logger initialize");

        var first = StartupTimeline.Report();
        StartupTimeline.MarkPhase("a reset's store open");
        var second = StartupTimeline.Report();

        Assert.NotNull(first);
        Assert.StartsWith("Startup phases: process start to OnStartup ", first);
        Assert.Contains(" ms, logger initialize ", first);
        Assert.DoesNotContain("a reset's store open", first);
        Assert.Null(second);
    }

    [Fact]
    public void MarkPhase_before_Begin_does_nothing()
    {
        Assert.Null(StartupTimeline.Report());
        StartupTimeline.MarkPhase("never begun");
        Assert.Null(StartupTimeline.Report());
    }
}
