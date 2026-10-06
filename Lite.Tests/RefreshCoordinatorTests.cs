/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5371: a Lite server tab's timer refresh, tab switches and time-range changes share one in-flight read. A user's
/// trace showed a tab-switch read pending while the timer started a second full refresh beside it (484 s), and the old
/// one-way <c>_isRefreshing</c> check also dropped a tab switch or range change that arrived during a refresh.
/// <see cref="RefreshCoordinator"/> is the seam. The first block drives it with reads the test controls (a gate per
/// pass, released by hand, no sleeps), standing in for "a tab-switch read that is still waiting"; the second block pins
/// the ServerTab wiring by source scan, since the control itself needs a WPF dispatcher and a store.
/// </summary>
public sealed class RefreshCoordinatorTests
{
    /// <summary>One pass the harness started: what it was asked for, what it saw selected, and the gate that holds its read.</summary>
    private sealed class Pass
    {
        internal required RefreshScope Scope { get; init; }
        internal required string Tab { get; init; }
        internal required string Window { get; init; }
        internal required CancellationToken Token { get; init; }
        internal TaskCompletionSource Gate { get; } = new();
        internal bool Painted { get; set; }
    }

    /// <summary>
    /// Stands in for the server tab: <see cref="SelectedTab"/> and <see cref="Window"/> are the UI state a pass reads at
    /// its own start, each pass blocks on a gate the test releases, and a pass paints only if its token was not cancelled
    /// when its read came back (the real pass checks the token at the same boundary).
    /// </summary>
    private sealed class Harness
    {
        internal string SelectedTab = "A";
        internal string Window = "4h";
        internal readonly List<Pass> Passes = new();
        internal readonly List<Exception> Faults = new();
        internal readonly List<string> Paints = new();
        internal int Concurrent;
        internal int MaxConcurrent;
        internal Func<Pass, Exception?>? Fail;
        private readonly Dictionary<int, TaskCompletionSource<Pass>> _started = new();

        private TaskCompletionSource<Pass> StartSignal(int index)
        {
            if (!_started.TryGetValue(index, out var signal))
            {
                _started[index] = signal = new TaskCompletionSource<Pass>(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            return signal;
        }

        /// <summary>Completes once pass <paramref name="index"/> has started, so a test never reads state a replay has not reached yet.</summary>
        internal async Task<Pass> Started(int index) => await Completes(StartSignal(index).Task);

        internal RefreshCoordinator Coordinator { get; }

        internal Harness()
        {
            Coordinator = new RefreshCoordinator(RunPassAsync, Faults.Add);
        }

        private async Task RunPassAsync(RefreshScope scope, CancellationToken ct)
        {
            var pass = new Pass { Scope = scope, Tab = SelectedTab, Window = Window, Token = ct };
            Passes.Add(pass);
            StartSignal(Passes.Count - 1).TrySetResult(pass);
            Concurrent++;
            MaxConcurrent = Math.Max(MaxConcurrent, Concurrent);
            try
            {
                await pass.Gate.Task;
                if (ct.IsCancellationRequested)
                {
                    return;
                }

                pass.Painted = true;
                Paints.Add($"{scope}:{pass.Tab}:{pass.Window}");
            }
            finally
            {
                Concurrent--;
            }
        }

        /// <summary>Lets pass <paramref name="index"/> finish, or fail if <see cref="Fail"/> says so.</summary>
        internal void Release(int index)
        {
            var pass = Passes[index];
            var failure = Fail?.Invoke(pass);
            if (failure is null)
            {
                pass.Gate.SetResult();
            }
            else
            {
                pass.Gate.SetException(failure);
            }
        }
    }

    private static async Task Completes(Task task)
    {
        var finished = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.Same(task, finished);
        await task;
    }

    private static async Task<T> Completes<T>(Task<T> task)
    {
        var finished = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.Same(task, finished);
        return await task;
    }

    [Fact]
    public async Task PendingTabSwitchRead_StopsATimerTick_FromStartingDuplicateWork()
    {
        var h = new Harness { SelectedTab = "CollectionHealth" };

        /* The user opens Collection Health: its read starts and then waits (248 s in the field trace). */
        var navigation = h.Coordinator.RequestAsync(RefreshScope.VisibleTab);
        Assert.Single(h.Passes);

        /* The timer fires while it waits. On dev this started a second full refresh beside it. */
        var tick = h.Coordinator.PollAsync();
        Assert.Single(h.Passes);
        Assert.Equal(1, h.MaxConcurrent);
        Assert.False(h.Coordinator.HasPending);

        h.Release(0);
        await Completes(navigation);
        await Completes(tick);

        Assert.Single(h.Passes);
        Assert.Equal(new[] { "VisibleTab:CollectionHealth:4h" }, h.Paints);
        Assert.False(h.Coordinator.IsRunning);
        Assert.Equal(1, h.MaxConcurrent);
    }

    [Fact]
    public async Task TabSwitchDuringATimerRefresh_EndsWithTheLatestTabLoaded()
    {
        var h = new Harness { SelectedTab = "Overview" };

        var tick = h.Coordinator.PollAsync();
        Assert.Equal(RefreshScope.Full, h.Passes[0].Scope);

        /* The user moves to Waits while the tick's pass is reading Overview. Dev dropped this switch outright. */
        h.SelectedTab = "Waits";
        var navigation = h.Coordinator.RequestAsync(RefreshScope.VisibleTab);
        Assert.Single(h.Passes);
        Assert.True(h.Passes[0].Token.IsCancellationRequested, "the tick's pass is superseded");
        Assert.True(h.Coordinator.HasPending);

        h.Release(0);
        await h.Started(1);
        Assert.Equal(2, h.Passes.Count);
        Assert.False(h.Passes[0].Painted, "the superseded Overview read must not paint");

        /* The replay reads the selection at ITS start (Waits), and carries the tick's wider scope so the badges still refresh. */
        Assert.Equal("Waits", h.Passes[1].Tab);
        Assert.Equal(RefreshScope.Full, h.Passes[1].Scope);
        Assert.False(h.Passes[1].Token.IsCancellationRequested);

        h.Release(1);
        await Completes(navigation);
        await Completes(tick);

        Assert.Equal(new[] { "Full:Waits:4h" }, h.Paints);
        Assert.Equal(1, h.MaxConcurrent);
        Assert.False(h.Coordinator.IsRunning);
    }

    [Fact]
    public async Task RapidTabChanges_DoNotPileUpRefreshes_OrPaintASupersededTab()
    {
        var h = new Harness { SelectedTab = "A" };

        var first = h.Coordinator.RequestAsync(RefreshScope.VisibleTab);
        foreach (var tab in new[] { "B", "C", "D", "E", "F" })
        {
            h.SelectedTab = tab;
            _ = h.Coordinator.RequestAsync(RefreshScope.VisibleTab);
        }

        /* Five more switches, still one read in flight, and one request remembered rather than five. */
        Assert.Single(h.Passes);
        Assert.Equal(1, h.MaxConcurrent);
        Assert.True(h.Coordinator.HasPending);

        h.Release(0);
        await h.Started(1);
        Assert.Equal(2, h.Passes.Count);
        Assert.Equal("F", h.Passes[1].Tab);
        Assert.False(h.Passes[0].Painted);

        h.Release(1);
        await Completes(first);

        Assert.Equal(new[] { "VisibleTab:F:4h" }, h.Paints);
        Assert.Equal(2, h.Passes.Count);
        Assert.Equal(1, h.MaxConcurrent);
        Assert.False(h.Coordinator.IsRunning);
    }

    [Fact]
    public async Task AFailedRead_ReleasesTheGuard_AndTheNextRefreshWorks()
    {
        var h = new Harness();
        h.Fail = p => p.Tab == "Broken" ? new InvalidOperationException("the read failed") : null;
        h.SelectedTab = "Broken";

        var failed = h.Coordinator.RequestAsync(RefreshScope.Full);
        h.Release(0);
        await Completes(failed);

        var fault = Assert.Single(h.Faults);
        Assert.Equal("the read failed", fault.Message);
        Assert.False(h.Coordinator.IsRunning, "a failed pass must not leave the guard set");

        /* The very next refresh runs, and so does a timer tick. */
        h.SelectedTab = "Fine";
        var next = h.Coordinator.RequestAsync(RefreshScope.Full);
        Assert.Equal(2, h.Passes.Count);
        h.Release(1);
        await Completes(next);
        Assert.Equal(new[] { "Full:Fine:4h" }, h.Paints);

        var tick = h.Coordinator.PollAsync();
        Assert.Equal(3, h.Passes.Count);
        h.Release(2);
        await Completes(tick);
        Assert.False(h.Coordinator.IsRunning);
    }

    [Fact]
    public async Task AFailedRead_StillRunsTheRequestThatArrivedDuringIt()
    {
        var h = new Harness();
        h.Fail = p => p.Tab == "Broken" ? new InvalidOperationException("boom") : null;
        h.SelectedTab = "Broken";

        var run = h.Coordinator.RequestAsync(RefreshScope.VisibleTab);
        h.SelectedTab = "Fine";
        _ = h.Coordinator.RequestAsync(RefreshScope.VisibleTab);

        h.Release(0);
        await h.Started(1);
        Assert.Equal(2, h.Passes.Count);
        h.Release(1);
        await Completes(run);

        Assert.Single(h.Faults);
        Assert.Equal(new[] { "VisibleTab:Fine:4h" }, h.Paints);
        Assert.False(h.Coordinator.IsRunning);
    }

    [Fact]
    public async Task AFaultCallbackThatThrows_StillReleasesTheGuard()
    {
        var coordinator = new RefreshCoordinator(
            (_, _) => throw new InvalidOperationException("the read threw before its first await"),
            _ => throw new InvalidOperationException("and so did the log"));

        await Completes(coordinator.RequestAsync(RefreshScope.Full));

        Assert.False(coordinator.IsRunning);
        Assert.False(coordinator.HasPending);
    }

    [Fact]
    public async Task ATimeRangeChangeDuringARefresh_IsRememberedAndReadsTheNewWindow()
    {
        var h = new Harness { SelectedTab = "Queries", Window = "4h" };

        var tick = h.Coordinator.PollAsync();

        /* The user picks 24 hours while the tick's read is still on the old window. Dev's range handler bailed here. */
        h.Window = "24h";
        var rangeChange = h.Coordinator.RequestAsync(RefreshScope.Full);
        Assert.Single(h.Passes);

        h.Release(0);
        await h.Started(1);
        Assert.Equal("24h", h.Passes[1].Window);
        h.Release(1);
        await Completes(rangeChange);
        await Completes(tick);

        Assert.Equal(new[] { "Full:Queries:24h" }, h.Paints);
    }

    [Fact]
    public async Task ATickThatArrivesWhileARequestIsWaiting_AddsNothing()
    {
        var h = new Harness();

        var run = h.Coordinator.RequestAsync(RefreshScope.Full);
        _ = h.Coordinator.RequestAsync(RefreshScope.VisibleTab);
        _ = h.Coordinator.PollAsync();
        _ = h.Coordinator.PollAsync();

        h.Release(0);
        await h.Started(1);
        Assert.Equal(2, h.Passes.Count);
        h.Release(1);
        await Completes(run);

        Assert.Equal(2, h.Passes.Count);
        Assert.Equal(1, h.MaxConcurrent);
    }

    [Fact]
    public async Task AWiderRequest_IsNotNarrowedByALaterNarrowerOne()
    {
        var h = new Harness();

        var run = h.Coordinator.RequestAsync(RefreshScope.VisibleTab);
        _ = h.Coordinator.RequestAsync(RefreshScope.Full);
        _ = h.Coordinator.RequestAsync(RefreshScope.VisibleTab);

        h.Release(0);
        await h.Started(1);
        Assert.Equal(RefreshScope.Full, h.Passes[1].Scope);
        h.Release(1);
        await Completes(run);
    }

    [Fact]
    public async Task ACallerThatJoinedARun_CompletesWhenTheWholeRunHasFinished()
    {
        var h = new Harness();

        var first = h.Coordinator.RequestAsync(RefreshScope.VisibleTab);
        var joined = h.Coordinator.RequestAsync(RefreshScope.VisibleTab);
        Assert.False(joined.IsCompleted, "a joined request waits for the run, replay included, so a button can stay disabled for it");

        h.Release(0);
        await h.Started(1);
        Assert.False(joined.IsCompleted);
        h.Release(1);

        await Completes(joined);
        await Completes(first);
    }

    // ---- ServerTab wiring pins: each of these fails on the source dev had before #5371. ----

    private static string ControlsDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls"));

    private static string ServerTabSource(string fileName) => File.ReadAllText(Path.Combine(ControlsDir(), fileName));

    /// <summary>The code of the first method named <paramref name="name"/>: comments and literals blanked, braces balanced.</summary>
    private static string Body(string fileName, string name)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(ServerTabSource(fileName));
        var match = Regex.Match(code, @"\b(?:Task|void)(?:<[^>]+>)?\s+" + Regex.Escape(name) + @"\s*\(");
        Assert.True(match.Success, $"{name} not found in {fileName}");
        var open = code.IndexOf('{', match.Index);
        return CSharpSourceWalker.BraceBalanced(code, open);
    }

    [Fact]
    public void TheTabSwitchHandler_NoLongerBailsOnARefreshInFlight_AndRoutesThroughTheCoordinator()
    {
        var body = Body("ServerTab.xaml.cs", "MainTabControl_SelectionChanged");

        Assert.DoesNotContain("_isRefreshing", body);
        Assert.Contains("RefreshVisibleTabOnlyAsync()", body);
        Assert.DoesNotContain("RefreshVisibleTabAsync(", body);
    }

    [Fact]
    public void TheTimerTick_IsAPoll_ThroughTheCoordinator()
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(ServerTabSource("ServerTab.xaml.cs"));
        var at = source.IndexOf("_refreshTimer.Tick +=", StringComparison.Ordinal);
        Assert.True(at >= 0, "the timer subscription is gone");
        var block = source.Substring(at, Math.Min(200, source.Length - at));
        Assert.Contains("RefreshAllDataOnTimerAsync()", block);
        Assert.DoesNotContain("RefreshAllDataAsync()", block);
    }

    [Theory]
    [InlineData("TimeRangeCombo_SelectionChanged")]
    [InlineData("CustomDateRange_Changed")]
    [InlineData("CustomTimeCombo_Changed")]
    public void TheTimeRangeHandlers_DoNotDropAChangeBecauseARefreshIsRunning(string handler)
    {
        var body = Body("ServerTab.TimeRange.cs", handler);

        Assert.DoesNotContain("_isRefreshing", body);
        Assert.Contains("_suppressRangeRefresh", body);
    }

    [Fact]
    public void EveryDirectRefreshOfTheVisibleTab_IsInsideACoordinatorPass()
    {
        /* A load that starts outside the coordinator is exactly the overlap #5371 closed. */
        var callers = Directory.EnumerateFiles(ControlsDir(), "ServerTab*.cs")
            .SelectMany(f => Regex.Matches(CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(f)), @"\bRefreshVisibleTabAsync\s*\(")
                .Select(_ => Path.GetFileName(f)))
            .ToList();

        /* The definition, plus the two coordinator passes' calls: all in ServerTab.Refresh.cs. */
        Assert.All(callers, f => Assert.Equal("ServerTab.Refresh.cs", f));
        Assert.Equal(3, callers.Count);
    }

    [Fact]
    public void TheInRefreshFlag_IsOnlyEverSetImmediatelyBeforeATry_AndOnlyByTheFullPass()
    {
        /* On dev the flag was set, then the server clock was awaited, then the try began: a throw in between left every refresh bailing forever. */
        var setters = new List<string>();
        foreach (var file in Directory.EnumerateFiles(ControlsDir(), "ServerTab*.cs"))
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file));
            foreach (Match m in Regex.Matches(code, @"\b_isRefreshing\s*=\s*true\s*;"))
            {
                var after = code.Substring(m.Index + m.Length);
                Assert.Matches(@"^\s*try\b", after);
                setters.Add(Path.GetFileName(file));
            }
        }

        Assert.Equal(new[] { "ServerTab.Refresh.cs" }, setters);
    }
}
