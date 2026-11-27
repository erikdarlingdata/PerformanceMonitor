using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9184: a headless start whose store open fails is retried in the process, because the scheduler's restart setting does not
/// restart a task that exits with code 3. None of these tests waits: the delay and the clock are the test's own.
/// </summary>
public sealed class HeadlessStartRetryTests
{
    /// <summary>A clock the fake delay advances, and the waits it was asked for.</summary>
    private sealed class FakeTime
    {
        public TimeSpan Now { get; set; }
        public List<TimeSpan> Waits { get; } = [];

        public HeadlessStartRetry.Timing Timing => new(
            (wait, ct) =>
            {
                ct.ThrowIfCancellationRequested();
                Waits.Add(wait);
                Now += wait;
                return Task.CompletedTask;
            },
            () => Now);
    }

    private static Exception Locked => new InvalidOperationException("the file is held by another process");

    [Fact]
    public async Task AStartThatFailsThenSucceeds_RetriesAndEnds_WithoutGivingUp()
    {
        var time = new FakeTime();
        var attempts = 0;
        var log = new List<string>();

        await HeadlessStartRetry.RunAsync(
            () => ++attempts < 3 ? Task.FromException(Locked) : Task.CompletedTask,
            time.Timing, log.Add, () => { }, () => { }, CancellationToken.None);

        Assert.Equal(3, attempts);
        Assert.Equal([TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(20)], time.Waits);
        Assert.DoesNotContain(log, line => line.Contains("giving up", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task AStartThatNeverOpens_GivesUpAfterTheWindow_AndRethrowsTheLastError()
    {
        var time = new FakeTime();
        var attempts = 0;

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => HeadlessStartRetry.RunAsync(
            () => { attempts++; return Task.FromException(Locked); },
            time.Timing, _ => { }, () => { }, () => { }, CancellationToken.None));

        Assert.Equal(Locked.Message, thrown.Message);
        Assert.Equal(HeadlessStartRetry.Window, time.Now);
        Assert.Equal(time.Waits.Count + 1, attempts);
    }

    [Fact]
    public async Task TheWaitsDoubleFromTenSecondsToACeilingOfFiveMinutes_AndTheLastOneEndsAtTheWindow()
    {
        var time = new FakeTime();

        await Assert.ThrowsAsync<InvalidOperationException>(() => HeadlessStartRetry.RunAsync(
            () => Task.FromException(Locked), time.Timing, _ => { }, () => { }, () => { }, CancellationToken.None));

        Assert.Equal(TimeSpan.FromSeconds(10), time.Waits[0]);
        Assert.Equal(TimeSpan.FromSeconds(20), time.Waits[1]);
        Assert.Equal(TimeSpan.FromSeconds(40), time.Waits[2]);
        Assert.Equal(TimeSpan.FromSeconds(80), time.Waits[3]);
        Assert.Equal(TimeSpan.FromSeconds(160), time.Waits[4]);
        Assert.All(time.Waits.Skip(5).SkipLast(1), wait => Assert.Equal(HeadlessStartRetry.MaxDelay, wait));
        Assert.True(time.Waits[^1] <= HeadlessStartRetry.MaxDelay);
        Assert.Equal(HeadlessStartRetry.Window, time.Waits.Aggregate(TimeSpan.Zero, (sum, wait) => sum + wait));
    }

    [Fact]
    public async Task AnAttemptThatTakesTime_CountsAgainstTheWindow()
    {
        var time = new FakeTime();
        var attempts = 0;

        await Assert.ThrowsAsync<InvalidOperationException>(() => HeadlessStartRetry.RunAsync(
            () =>
            {
                attempts++;
                time.Now += TimeSpan.FromMinutes(7);
                return Task.FromException(Locked);
            },
            time.Timing, _ => { }, () => { }, () => { }, CancellationToken.None));

        /* Each attempt costs seven minutes, so the window holds far fewer than the eleven it holds for an instant failure. */
        Assert.InRange(attempts, 2, 5);
        Assert.True(time.Now >= HeadlessStartRetry.Window);
    }

    [Fact]
    public async Task EachFailedAttemptLogsItsNumberItsErrorAndTheNextDelay_AndTheLastLineSaysLiteIsGivingUp()
    {
        var time = new FakeTime();
        var log = new List<string>();

        await Assert.ThrowsAsync<InvalidOperationException>(() => HeadlessStartRetry.RunAsync(
            () => Task.FromException(Locked), time.Timing, log.Add, () => { }, () => { }, CancellationToken.None));

        var attempts = time.Waits.Count + 1;
        Assert.Contains("attempt 1 failed", log[0]);
        Assert.Contains(Locked.Message, log[0]);
        Assert.Contains("Next attempt in 10 s", log[0]);
        Assert.Contains("attempt 2 failed", log[1]);
        Assert.Contains("Next attempt in 20 s", log[1]);
        Assert.Contains("Next attempt in 5 min", log[6]);

        Assert.Equal(attempts + 1, log.Count);
        Assert.Contains($"attempt {attempts} failed", log[^2]);
        Assert.Contains(Locked.Message, log[^2]);
        Assert.Contains($"giving up after {attempts} attempts", log[^1], StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task TheStopListenerIsOpenOnlyWhileWaiting_AndClosedBeforeEveryAttempt()
    {
        var events = new List<string>();
        var time = new FakeTime();
        var attempts = 0;
        var timing = new HeadlessStartRetry.Timing(
            (wait, ct) =>
            {
                events.Add("wait");
                return time.Timing.Delay(wait, ct);
            },
            () => time.Now);

        await HeadlessStartRetry.RunAsync(
            () => { events.Add("attempt"); return ++attempts < 3 ? Task.FromException(Locked) : Task.CompletedTask; },
            timing, _ => { }, () => events.Add("open"), () => events.Add("close"), CancellationToken.None);

        Assert.Equal(
            ["attempt", "open", "wait", "close", "attempt", "open", "wait", "close", "attempt"], events);
    }

    [Fact]
    public async Task AWaitThatIsCancelled_ClosesTheStopListener_BeforeTheRetryEnds()
    {
        var open = false;
        using var stop = new CancellationTokenSource();
        var timing = new HeadlessStartRetry.Timing(
            (_, ct) => { stop.Cancel(); ct.ThrowIfCancellationRequested(); return Task.CompletedTask; },
            () => TimeSpan.Zero);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => HeadlessStartRetry.RunAsync(
            () => Task.FromException(Locked),
            timing, _ => { }, () => open = true, () => open = false, stop.Token));

        Assert.False(open);
    }

    [Fact]
    public async Task AStopThatArrivesDuringAWaitThatEndsNormally_EndsTheRetryBeforeTheNextAttempt()
    {
        var attempts = 0;
        using var stop = new CancellationTokenSource();
        var timing = new HeadlessStartRetry.Timing(
            (_, _) => { stop.Cancel(); return Task.CompletedTask; },
            () => TimeSpan.Zero);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => HeadlessStartRetry.RunAsync(
            () => { attempts++; return Task.FromException(Locked); },
            timing, _ => { }, () => { }, () => { }, stop.Token));

        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task AStopDuringAWait_EndsTheRetryAtOnce_WithoutAnotherAttemptOrAGivingUpLine()
    {
        var attempts = 0;
        var log = new List<string>();
        var waiting = new TaskCompletionSource();
        using var stop = new CancellationTokenSource();
        var timing = new HeadlessStartRetry.Timing(
            async (wait, ct) =>
            {
                waiting.SetResult();
                await Task.Delay(Timeout.Infinite, ct);
            },
            () => TimeSpan.Zero);

        var run = HeadlessStartRetry.RunAsync(
            () => { attempts++; return Task.FromException(Locked); },
            timing, log.Add, () => { }, () => { }, stop.Token);
        await waiting.Task;
        Assert.False(run.IsCompleted);

        stop.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Equal(1, attempts);
        Assert.Single(log);
        Assert.DoesNotContain(log, line => line.Contains("giving up", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task AStopThatArrivesWhileAnAttemptIsRunning_EndsTheRetryWithoutWaiting()
    {
        var waits = 0;
        var log = new List<string>();
        using var stop = new CancellationTokenSource();
        var timing = new HeadlessStartRetry.Timing(
            (_, _) => { waits++; return Task.CompletedTask; },
            () => TimeSpan.Zero);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => HeadlessStartRetry.RunAsync(
            () => { stop.Cancel(); return Task.FromException(Locked); },
            timing, log.Add, () => { }, () => { }, stop.Token));

        Assert.Equal(0, waits);
        Assert.Empty(log);
    }

    [Fact]
    public async Task AStopThatArrivesWhileAnAttemptRuns_AndTheAttemptThenSucceeds_EndsTheRetryWithoutReturning()
    {
        var waits = 0;
        using var stop = new CancellationTokenSource();
        var timing = new HeadlessStartRetry.Timing(
            (_, _) => { waits++; return Task.CompletedTask; },
            () => TimeSpan.Zero);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => HeadlessStartRetry.RunAsync(
            () => { stop.Cancel(); return Task.CompletedTask; },
            timing, _ => { }, () => { }, () => { }, stop.Token));

        Assert.Equal(0, waits);
    }
}
