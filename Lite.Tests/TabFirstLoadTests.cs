/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitorLite.Helpers;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9232: a tab's first load writes ONE log line with its elapsed milliseconds, and a tab draws each panel as its own reads
/// land. WPF cannot run in the suite, so both decisions live in small helpers the server tab calls.
/// </summary>
public sealed class TabFirstLoadTests
{
    /// <summary>A clock the test moves by hand, one tick per millisecond.</summary>
    private sealed class HandClock : TimeProvider
    {
        private long _ticks;
        public override long TimestampFrequency => 1000;
        public override long GetTimestamp() => _ticks;
        public void Advance(int ms) => _ticks += ms;
    }

    [Fact]
    public async Task TheFirstLoadOfATab_WritesOneLine_WithTheElapsedMilliseconds()
    {
        var lines = new List<string>();
        var clock = new HandClock();
        var log = new TabFirstLoad(lines.Add, clock);

        var result = await log.RunAsync("tab 2/0", "Queries > Performance Trends", () =>
        {
            clock.Advance(1234);
            return Task.FromResult(7);
        });

        Assert.Equal(7, result);
        var line = Assert.Single(lines);
        Assert.Equal("First load of Queries > Performance Trends: 1234 ms", line);
    }

    [Fact]
    public async Task ALaterLoadOfTheSameTab_WritesNothing_ButEachNewKeyWritesItsOwnLine()
    {
        var lines = new List<string>();
        var clock = new HandClock();
        var log = new TabFirstLoad(lines.Add, clock);

        for (var tick = 0; tick < 5; tick++)
        {
            await log.RunAsync("tab 2/0", "Trends", () => { clock.Advance(10); return Task.FromResult(0); });
        }

        Assert.Single(lines);

        await log.RunAsync("tab 2/1", "Active Queries", () => { clock.Advance(20); return Task.FromResult(0); });
        await log.RunAsync("tab 2/1", "Active Queries", () => { clock.Advance(20); return Task.FromResult(0); });

        Assert.Equal(2, lines.Count);
        Assert.Equal("First load of Active Queries: 20 ms", lines[1]);
    }

    [Fact]
    public async Task ALoadANewerRequestAbandoned_IsNotAFirstOpen_SoItsReplayGetsTheLine()
    {
        var lines = new List<string>();
        var clock = new HandClock();
        var log = new TabFirstLoad(lines.Add, clock);

        await log.RunAsync("tab 4", "CPU", () => { clock.Advance(5); return Task.FromResult(0); }, abandoned: () => true);
        Assert.Empty(lines);

        await log.RunAsync("tab 4", "CPU", () => { clock.Advance(9); return Task.FromResult(0); }, abandoned: () => false);
        Assert.Equal("First load of CPU: 9 ms", Assert.Single(lines));
    }

    [Fact]
    public async Task ALoadThatThrows_WritesNoLine_AndTheNextLoadIsStillTheFirst()
    {
        var lines = new List<string>();
        var log = new TabFirstLoad(lines.Add, new HandClock());

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            log.RunAsync<int>("tab 6", "File I/O", () => throw new InvalidOperationException("read failed")));
        Assert.Empty(lines);

        await log.RunAsync("tab 6", "File I/O", () => Task.FromResult(0));
        Assert.Single(lines);
    }

    [Fact]
    public async Task ANullKey_IsNotTracked()
    {
        var lines = new List<string>();
        var log = new TabFirstLoad(lines.Add, new HandClock());

        await log.RunAsync(null, "anything", () => Task.FromResult(0));

        Assert.Empty(lines);
    }

    [Fact]
    public async Task APanel_IsDrawnWhenItsOwnReadLands_WithoutWaitingForASlowerPanel()
    {
        var fast = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var slow = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var drawn = new List<string>();

        var all = Task.WhenAll(
            LandedPanels.DrawAsync(fast.Task, value => drawn.Add($"fast={value}")),
            LandedPanels.DrawAsync(slow.Task, value => drawn.Add($"slow={value}")));

        fast.SetResult(1);
        await Eventually(() => drawn.Count == 1);
        Assert.Equal(["fast=1"], drawn);
        Assert.False(all.IsCompleted, "the slow panel's read is still running");

        slow.SetResult(2);
        await all.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        Assert.Equal(["fast=1", "slow=2"], drawn);
    }

    [Fact]
    public async Task APanelThatNeedsTwoReads_WaitsForBoth_AndDrawsOnceWithBothResults()
    {
        var chart = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var markers = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var drawn = new List<string>();

        var draw = LandedPanels.DrawAsync(chart.Task, markers.Task, (value, mark) => drawn.Add($"{value}/{mark}"));

        chart.SetResult(3);
        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.Empty(drawn);

        markers.SetResult("m");
        await draw.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        Assert.Equal(["3/m"], drawn);
    }

    private static async Task Eventually(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        Assert.True(condition());
    }
}
