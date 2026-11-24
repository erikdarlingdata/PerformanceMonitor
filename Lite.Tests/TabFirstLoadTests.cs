/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
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
        Assert.Equal("First load of Queries > Performance Trends: 1234 ms (before read 0 ms, read 1234 ms, read lock wait 0 ms over 0 connections, draw 0 ms)", line);
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
        Assert.StartsWith("First load of Active Queries: 20 ms (", lines[1]);
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
        Assert.StartsWith("First load of CPU: 9 ms (", Assert.Single(lines));
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

    private static long LockTicks(int ms) => Stopwatch.Frequency * ms / 1000;

    /// <summary>#9366: the total is the pick to the first drawn row, split into queued, before read, read and draw.</summary>
    [Fact]
    public async Task TheLine_TimesFromThePick_ToTheFirstDrawnRow_AndSplitsTheTimeBetween()
    {
        var lines = new List<string>();
        var clock = new HandClock();
        var log = new TabFirstLoad(lines.Add, clock, () => { clock.Advance(50); return Task.CompletedTask; });

        log.Pick("tab 2/4");
        clock.Advance(300);                         // queued behind another pass
        var passStarted = log.Timestamp();
        clock.Advance(200);                         // engine edition, data start, collector runs

        await log.RunAsync("tab 2/4", "Queries > Query Store by Duration", () =>
        {
            clock.Advance(1000);                    // the read
            ReadLockWaitTally.Current!.RecordAcquire(LockTicks(150));
            ReadLockWaitTally.Current!.RecordAcquire(LockTicks(250));
            TabFirstLoad.RowsHanded();              // the rows go to the grid, the render pass takes 50 ms
            clock.Advance(700);                     // the comparison and banner reads that follow are not the first row
            return Task.FromResult(0);
        }, passStartedAt: passStarted);

        Assert.Equal(
            "First load of Queries > Query Store by Duration: 1550 ms (queued 300 ms, before read 200 ms, read 1000 ms, read lock wait 400 ms over 2 connections, draw 50 ms)",
            Assert.Single(lines));
    }

    [Fact]
    public async Task ALoadThatNeverHandsItsRows_EndsItsReadAtTheEndOfTheLoad()
    {
        var lines = new List<string>();
        var clock = new HandClock();
        var log = new TabFirstLoad(lines.Add, clock, () => { clock.Advance(20); return Task.CompletedTask; });

        log.Pick("tab 4");
        clock.Advance(10);
        var passStarted = log.Timestamp();

        await log.RunAsync("tab 4", "CPU", () => { clock.Advance(500); return Task.FromResult(0); }, passStartedAt: passStarted);

        Assert.Equal("First load of CPU: 530 ms (queued 10 ms, before read 0 ms, read 500 ms, read lock wait 0 ms over 0 connections, draw 20 ms)", Assert.Single(lines));
    }

    [Fact]
    public async Task ANewerPickOfTheSameTab_ReplacesTheStamp_SoTheNewerPickIsTheOneMeasured()
    {
        var lines = new List<string>();
        var clock = new HandClock();
        var log = new TabFirstLoad(lines.Add, clock);

        log.Pick("tab 2/4");
        clock.Advance(5000);
        log.Pick("tab 2/4");
        clock.Advance(100);

        await log.RunAsync("tab 2/4", "Query Store", () => { clock.Advance(10); return Task.FromResult(0); }, passStartedAt: log.Timestamp());

        Assert.StartsWith("First load of Query Store: 110 ms (queued 100 ms,", Assert.Single(lines));
    }

    [Fact]
    public async Task ASupersededPick_WritesNoLine_AndTheNewPicksLoadIsMeasuredFromItsOwnStamp()
    {
        var lines = new List<string>();
        var clock = new HandClock();
        var log = new TabFirstLoad(lines.Add, clock);

        log.Pick("tab 2/4");
        clock.Advance(100);
        var abandonedStart = log.Timestamp();
        log.Pick("tab 2/5");                        // the user moved on before the first load finished
        clock.Advance(40);
        await log.RunAsync("tab 2/4", "Query Store", () => { clock.Advance(900); return Task.FromResult(0); }, abandoned: () => true, passStartedAt: abandonedStart);
        Assert.Empty(lines);

        var secondStart = log.Timestamp();
        clock.Advance(15);
        await log.RunAsync("tab 2/5", "Plan Corrections", () => { clock.Advance(60); return Task.FromResult(0); }, passStartedAt: secondStart);

        Assert.Equal("First load of Plan Corrections: 1015 ms (queued 940 ms, before read 15 ms, read 60 ms, read lock wait 0 ms over 0 connections, draw 0 ms)", Assert.Single(lines));
    }

    [Fact]
    public async Task APickAfterTheFirstLoad_IsIgnored_AndTheLaterLoadWritesNothing()
    {
        var lines = new List<string>();
        var clock = new HandClock();
        var log = new TabFirstLoad(lines.Add, clock);

        await log.RunAsync("tab 4", "CPU", () => Task.FromResult(0));
        log.Pick("tab 4");
        clock.Advance(5000);
        await log.RunAsync("tab 4", "CPU", () => Task.FromResult(0));

        Assert.Single(lines);
    }

    [Fact]
    public async Task ARowsHandedCall_OutsideAFirstLoad_DoesNothing()
    {
        var log = new TabFirstLoad(_ => { }, new HandClock());
        await log.RunAsync("tab 4", "CPU", () => Task.FromResult(0));

        TabFirstLoad.RowsHanded();
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
