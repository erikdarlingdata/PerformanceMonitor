/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Collection Health tab's load, past its reads (#4966): which part of the tab a failing read can take down, and which parts
/// wait for the "Showing since" probe. Each fact drives the tab's own steps (<c>ViewerServerTab.ReadOrEmptyAsync</c>, the join the
/// load runs, and <c>ViewerServerTab.ShowCollectionHealthAsync</c>) on a thread with a dispatcher, as the tab runs them, with real
/// controls for the note and a probe the test holds open. A regression that makes the note wait for the probe fails these on
/// <c>IsCompleted</c> and never hangs them.
/// </summary>
/* The banner's time text reads the process-wide display mode, which the facts set (restored in Dispose); the shared collection
   serializes it with every other class that flips the viewer's time statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerCollectionHealthLoadTests : IDisposable
{
    private static readonly DateTime RangeStart = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    /* A store nothing listens on: a read against it fails at once, which is the failure a timed-out read stands in for. */
    private const string UnreachableStore = "Host=127.0.0.1;Port=1;Username=nobody;Database=nothing;Timeout=3;Pooling=false";

    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;

    public void Dispose() => ViewerTimeHelper.CurrentDisplayMode = _savedMode;

    /// <summary>
    /// What the tab drew, in the order it drew it, and the note the banner ended with (null when hidden). The banner is a WPF control,
    /// so it is made (<see cref="Start"/>) and read (<see cref="Capture"/>) on the dispatcher's thread, seeded visible with stale
    /// text so a no-op cannot pass as a hidden banner.
    /// </summary>
    private sealed class Drawn
    {
        public List<string> Order { get; } = [];
        public List<CollectorDurationBucket>? Chart { get; set; }
        public TextBlock Banner { get; private set; } = null!;
        public string? Note { get; private set; }

        public void Start() => Banner = new TextBlock { Visibility = Visibility.Visible, Text = "stale" };

        public void Capture() => Note = Banner.Visibility == Visibility.Visible ? Banner.Text : null;
    }

    /// <summary>The tab's step over the given results and probe, with the drawing recorded in <paramref name="drawn"/>.</summary>
    private static Task Step(
        Drawn drawn, List<CollectionLogRow> log, List<CollectorDurationBucket> trend, Task<DateTime?> probe, Action<string, string>? warn = null) =>
        ViewerServerTab.ShowCollectionHealthAsync(
            [new CollectorHealthRow { CollectorName = "wait_stats" }], log, trend, [new CollectionCaveatRow { Family = "x", Reason = "y" }],
            probe, RangeStart, drawn.Banner,
            _ => drawn.Order.Add("health"), _ => drawn.Order.Add("log"), t => { drawn.Order.Add("chart"); drawn.Chart = t; }, _ => drawn.Order.Add("caveats"),
            warn);

    private static List<CollectionLogRow> Runs(int count, DateTime first) =>
        Enumerable.Range(0, count).Select(i => new CollectionLogRow { CollectorName = "wait_stats", CollectionTime = first.AddMinutes(10 * i), Status = "SUCCESS" }).ToList();

    private static readonly string[] AllFour = ["health", "log", "chart", "caveats"];

    /// <summary>Runs <paramref name="body"/> on an STA thread with a dispatcher, as the tab's own load runs, and rethrows what it threw.</summary>
    private static void OnDispatcher(Func<Task> body)
    {
        Exception? error = null;
        var thread = new Thread(() =>
        {
            var dispatcher = Dispatcher.CurrentDispatcher;
            dispatcher.BeginInvoke(new Action(async () =>
            {
                try
                {
                    ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
                    await body();
                }
                catch (Exception ex)
                {
                    error = ex;
                }
                finally
                {
                    dispatcher.BeginInvokeShutdown(DispatcherPriority.Background);
                }
            }));
            Dispatcher.Run();
        })
        {
            IsBackground = true,
        };
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();

        if (!thread.Join(TimeSpan.FromSeconds(60)))
        {
            throw new TimeoutException("the tab's step did not finish: it is waiting on something it should not wait for");
        }

        if (error is not null)
        {
            ExceptionDispatchInfo.Capture(error).Throw();
        }
    }

    /* The chart's read fails (here a real read against a store nothing listens on): the wrapper answers an empty list and logs it, the
       join the load runs does not throw, and the grids, the caveats and the note all draw, with the chart drawn empty. The coverage
       starts two days into the range and the runs begin after it, so the note names the coverage. */
    [Fact]
    public async Task AChartReadThatFails_BlanksOnlyTheChart_TheGridsTheNoteAndTheCaveatsStillDraw()
    {
        await using var viewer = new ViewerDataService(UnreachableStore);
        var warned = new List<string>();

        var trendTask = ViewerServerTab.ReadOrEmptyAsync(
            () => viewer.GetCollectorDurationTrendAsync(1, RangeStart, RangeStart.AddDays(7)), "Test chart", (_, message) => warned.Add(message));
        var logTask = Task.FromResult(Runs(3, RangeStart.AddDays(3)));
        await Task.WhenAll(logTask, trendTask);

        var drawn = new Drawn();
        OnDispatcher(async () =>
        {
            drawn.Start();
            await Step(drawn, logTask.Result, trendTask.Result, Task.FromResult<DateTime?>(RangeStart.AddDays(2)));
            drawn.Capture();
        });

        Assert.Equal(AllFour, drawn.Order);
        Assert.NotNull(drawn.Chart);
        Assert.Empty(drawn.Chart!);
        Assert.Single(warned);
        Assert.Contains("Test chart", warned[0], StringComparison.Ordinal);
        Assert.Equal(DataStartBannerReadout.Since(RangeStart.AddDays(2)), drawn.Note);
    }

    /* A cancelled chart read is not a failure to hide: it still throws. */
    [Fact]
    public async Task ACancelledChartRead_StillThrows()
    {
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            ViewerServerTab.ReadOrEmptyAsync<CollectorDurationBucket>(() => throw new OperationCanceledException(), "Test chart"));
    }

    /* A page under the cap needs the probe for its note, so the note waits; the chart and the caveats do not wait with it. */
    [Fact]
    public void TheChartAndTheCaveats_DrawBeforeTheNoteAwaitsItsProbe()
    {
        var probe = new TaskCompletionSource<DateTime?>();
        var drawn = new Drawn();

        OnDispatcher(async () =>
        {
            drawn.Start();
            var step = Step(drawn, Runs(3, RangeStart.AddDays(3)), [], probe.Task);

            Assert.Equal(AllFour, drawn.Order);
            Assert.False(step.IsCompleted, "a page under the cap names its note from the probe, so the note waits for it");
            Assert.Equal("stale", drawn.Banner.Text);

            probe.SetResult(RangeStart.AddDays(2));
            await step;
            drawn.Capture();
        });

        Assert.Equal(DataStartBannerReadout.Since(RangeStart.AddDays(2)), drawn.Note);
    }

    /* A full page names its oldest run whatever the probe answers: the probe here never completes, and the step still finishes, the
       chart and the caveats are drawn, and the note names the oldest run. */
    [Fact]
    public void AFullPage_NamesItsOldestRun_WithoutWaitingForAProbeThatNeverAnswers()
    {
        var oldest = RangeStart.AddDays(2);
        var probe = new TaskCompletionSource<DateTime?>();
        var drawn = new Drawn();

        OnDispatcher(async () =>
        {
            drawn.Start();
            var step = Step(drawn, Runs(ViewerDataService.CollectionLogRowCap, oldest), [], probe.Task);

            Assert.True(step.IsCompleted, "a full page waits for no probe");
            await step;
            drawn.Capture();
        });

        Assert.Equal(AllFour, drawn.Order);
        Assert.Equal(DataStartBannerReadout.Since(oldest), drawn.Note);
    }

    /* The probe a full page no longer waits for is still watched: when it fails, the failure is logged as any probe's is. */
    [Fact]
    public void AFullPage_StillLogsAProbeThatFailsLater()
    {
        var probe = new TaskCompletionSource<DateTime?>();
        var warned = new List<string>();

        OnDispatcher(async () =>
        {
            var drawn = new Drawn();
            drawn.Start();
            var step = Step(drawn, Runs(ViewerDataService.CollectionLogRowCap, RangeStart.AddDays(2)), [], probe.Task, (_, message) => warned.Add(message));
            Assert.True(step.IsCompleted);
            await step;

            probe.SetException(new InvalidOperationException("the probe failed"));
            await Task.Yield();
        });

        Assert.Single(warned);
        Assert.Contains("the probe failed", warned[0], StringComparison.Ordinal);
    }
}
