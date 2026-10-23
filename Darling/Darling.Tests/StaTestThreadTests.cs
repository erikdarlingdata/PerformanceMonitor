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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Threading;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5602: the shared STA test thread. Bodies run in order on one thread, an exception keeps its stack, a body that pumps does not
/// let a second one in, the hang guard fires and names the test, and the drain runs the work a body queued before the next body
/// starts. The hang-guard test uses a host of its own: it leaves that host's thread stuck, which the shared one must never be.
/// In the <c>timing</c> collection because it judges 200 to 300 ms limits (the hang guard, a gate that must hold a body out), which a
/// parallel run's load would stretch (#5602).
/// </summary>
[Collection("timing")]
public sealed class StaTestThreadTests
{
    [Fact]
    public void Bodies_RunOneAfterAnother_OnTheSameBackgroundStaThread()
    {
        var first = StaTestThread.Run(Describe);
        var second = StaTestThread.Run(Describe);

        Assert.Equal(first.Id, second.Id);
        Assert.NotEqual(Environment.CurrentManagedThreadId, first.Id);
        Assert.Equal(ApartmentState.STA, first.Apartment);
        Assert.True(first.Background, "the shared thread must be a background thread, or a stuck body keeps the test process alive");
    }

    [Fact]
    public void ABodysException_ComesBackToTheCaller_WithItsOwnStack()
    {
        var thrown = Assert.Throws<InvalidOperationException>(() => StaTestThread.Run(() => ThrowFromNamedFrame()));

        Assert.Equal("boom from the body", thrown.Message);
        Assert.Contains(nameof(ThrowFromNamedFrame), thrown.StackTrace);

        /* The thread survives the failure: the next body still runs. */
        Assert.Equal(5, StaTestThread.Run(() => 5));
    }

    [Fact]
    public void ABodyThatPumps_DoesNotLetASecondBodyIn()
    {
        var inside = 0;
        var most = 0;
        var errors = new List<Exception>();
        var callers = Enumerable.Range(0, 3).Select(_ => new Thread(() =>
        {
            try
            {
                for (var i = 0; i < 3; i++)
                {
                    StaTestThread.Run(() =>
                    {
                        var now = Interlocked.Increment(ref inside);
                        InterlockedMax(ref most, now);

                        /* A nested frame: while it runs, anything else queued on this dispatcher would run inside it. */
                        var frame = new DispatcherFrame();
                        var timer = new DispatcherTimer(TimeSpan.FromMilliseconds(15), DispatcherPriority.Normal, (_, _) => frame.Continue = false, Dispatcher.CurrentDispatcher);
                        timer.Start();
                        Dispatcher.PushFrame(frame);
                        timer.Stop();

                        Interlocked.Decrement(ref inside);
                    });
                }
            }
            catch (Exception ex)
            {
                lock (errors)
                {
                    errors.Add(ex);
                }
            }
        })).ToList();

        callers.ForEach(t => t.Start());
        callers.ForEach(t => Assert.True(t.Join(TimeSpan.FromSeconds(60)), "a caller never finished"));

        Assert.Empty(errors);
        Assert.Equal(1, most);
    }

    [Fact]
    public void ABodyThatNeverReturns_FailsItsOwnTest_ByName_AndEveryLaterCallFailsAtOnce()
    {
        var host = new StaHost { HangGuard = TimeSpan.FromMilliseconds(300) };
        using var release = new ManualResetEventSlim();
        try
        {
            var timedOut = Assert.Throws<TimeoutException>(() => host.Run(() => release.Wait()));
            Assert.Contains(nameof(ABodyThatNeverReturns_FailsItsOwnTest_ByName_AndEveryLaterCallFailsAtOnce), timedOut.Message);
            Assert.Contains(nameof(ABodyThatNeverReturns_FailsItsOwnTest_ByName_AndEveryLaterCallFailsAtOnce), host.StuckTest);

            var ran = false;
            var started = Environment.TickCount64;
            var refused = Assert.Throws<InvalidOperationException>(() => host.Run(() => ran = true));
            Assert.Contains(nameof(ABodyThatNeverReturns_FailsItsOwnTest_ByName_AndEveryLaterCallFailsAtOnce), refused.Message);
            Assert.False(ran);
            Assert.True(Environment.TickCount64 - started < 250, "a later call must fail at once, not wait out its own guard");
        }
        finally
        {
            release.Set();
            host.Shutdown();
        }
    }

    /// <summary>
    /// #6632: a body the guard gave up on may still return later (a cold first WPF start under a loaded runner), or never. The later
    /// tests' refusals say which, and when, so the next CI report tells a slow start from a body blocked for good.
    /// </summary>
    [Fact]
    public void ALaterRefusal_SaysWhetherTheStrandedBodyReturned_AndHowLongAfterItStarted()
    {
        var host = new StaHost { HangGuard = TimeSpan.FromMilliseconds(300) };
        using var release = new ManualResetEventSlim();
        using var returned = new ManualResetEventSlim();
        try
        {
            Assert.Throws<TimeoutException>(() => host.Run(() =>
            {
                release.Wait();
                returned.Set();
            }));

            var whileBlocked = Assert.Throws<InvalidOperationException>(() => host.Run(() => { }));
            Assert.Contains("has not returned yet (#6632)", whileBlocked.Message, StringComparison.Ordinal);

            release.Set();
            Assert.True(returned.Wait(TimeSpan.FromSeconds(10)), "the released body did not return");
            /* The record is written when the host's job finishes, just after its drain; poll the refusal until it shows. */
            Assert.True(SpinWait.SpinUntil(() => Refusal(host).Contains("did return", StringComparison.Ordinal), TimeSpan.FromSeconds(10)),
                "a refusal after the stranded body returned must say that it did, and when: " + Refusal(host));
            Assert.Matches(@"That body did return, \d+ ms after it started", Refusal(host));
        }
        finally
        {
            release.Set();
            host.Shutdown();
        }
    }

    private static string Refusal(StaHost host) => Assert.Throws<InvalidOperationException>(() => host.Run(() => { })).Message;

    /// <summary>#6216: a stuck body leaves no stack to read, so the guard's message names the last step the body reached and what the thread and dispatcher were doing.</summary>
    [Fact]
    public void TheHangGuardsMessage_NamesTheLastStep_AndSaysABlockedBodyWasWaitingWithNothingToRun()
    {
        var host = new StaHost { HangGuard = TimeSpan.FromMilliseconds(300) };
        using var release = new ManualResetEventSlim();
        try
        {
            var timedOut = Assert.Throws<TimeoutException>(() => host.Run(() =>
            {
                host.Step("tab 3, waiting for the release");
                release.Wait();
            }));

            Assert.Contains("tab 3, waiting for the release", timedOut.Message);
            Assert.Contains("it is waiting, and the dispatcher has nothing to run", timedOut.Message);
        }
        finally
        {
            release.Set();
            host.Shutdown();
        }
    }

    [Fact]
    public void TheHangGuardsMessage_SaysABodyThatSpinsInOneCall_WasUsingTheCpu()
    {
        var host = new StaHost { HangGuard = TimeSpan.FromMilliseconds(300) };
        using var release = new ManualResetEventSlim();
        try
        {
            var timedOut = Assert.Throws<TimeoutException>(() => host.Run(() =>
            {
                host.Step("spinning");
                while (!release.IsSet)
                {
                    Thread.SpinWait(100);
                }
            }));

            Assert.Contains("\"spinning\"", timedOut.Message);
            Assert.Contains("it is spinning inside one call", timedOut.Message);
        }
        finally
        {
            release.Set();
            host.Shutdown();
        }
    }

    [Fact]
    public void TheHangGuardsMessage_SaysABodyWhoseDispatcherNeverRunsOut_IsStillRunningWork()
    {
        var host = new StaHost { HangGuard = TimeSpan.FromMilliseconds(300) };
        using var release = new ManualResetEventSlim();
        try
        {
            var timedOut = Assert.Throws<TimeoutException>(() => host.Run(() =>
            {
                host.Step("pumping");
                var dispatcher = Dispatcher.CurrentDispatcher;
                var frame = new DispatcherFrame();
                Action again = null!;
                /* The shape of a layout that never settles: each operation queues the next, so the idle priority is never reached. */
                again = () =>
                {
                    if (release.IsSet)
                    {
                        frame.Continue = false;
                        return;
                    }

                    dispatcher.BeginInvoke(DispatcherPriority.Render, again);
                };
                again();
                Dispatcher.PushFrame(frame);
            }));

            Assert.Contains("\"pumping\"", timedOut.Message);
            Assert.Contains("it is still running dispatcher work that does not end", timedOut.Message);
        }
        finally
        {
            release.Set();
            host.Shutdown();
        }
    }

    /// <summary>#6216: the wait for idle that every layout probe makes is bounded, so a dispatcher that never reaches idle fails that one test and leaves the shared thread usable.</summary>
    [Fact]
    public void ASettleWhoseDispatcherNeverReachesIdle_FailsAtItsLimit_AndTheSharedThreadStaysUsable()
    {
        var stop = false;
        var thrown = Assert.Throws<TimeoutException>(() => StaTestThread.Run(() =>
        {
            var window = new Window { Width = 50, Height = 50, ShowInTaskbar = false };
            var dispatcher = Dispatcher.CurrentDispatcher;
            Action again = null!;
            again = () =>
            {
                if (!stop)
                {
                    dispatcher.BeginInvoke(DispatcherPriority.Render, again);
                }
            };
            again();
            try
            {
                ClippedTextProbe.Settle(window, TimeSpan.FromMilliseconds(300));
            }
            finally
            {
                /* The body's drain waits for idle too, so the churn this test makes stops before the body returns. */
                stop = true;
            }
        }));

        Assert.Contains("did not reach application idle within 300 ms", thrown.Message);
        Assert.Matches(@"it ran [1-9]\d* operations meanwhile", thrown.Message);
        Assert.Equal(9, StaTestThread.Run(() => 9));
    }

    [Fact]
    public void AfterABody_TheDispatcherIsDrained_AndAWindowItLeftOpenIsClosed()
    {
        var queuedRan = false;
        Window? left = null;
        StaTestThread.Run(() =>
        {
            Dispatcher.CurrentDispatcher.BeginInvoke(DispatcherPriority.ApplicationIdle, new Action(() => queuedRan = true));
            left = new Window { Width = 50, Height = 50, ShowInTaskbar = false };
            left.Show();
        });

        Assert.True(queuedRan, "work the body queued must run before the next test, not during it");
        Assert.False(StaTestThread.Run(() => left!.IsVisible), "a window the body left open must be closed by the drain");
    }

    [Fact]
    public void AnAsyncBody_ResumesOnTheSharedThread_AfterEachAwait()
    {
        var ids = new List<int>();
        StaTestThread.Run(async () =>
        {
            ids.Add(Environment.CurrentManagedThreadId);
            await Task.Delay(10);
            ids.Add(Environment.CurrentManagedThreadId);
            await Dispatcher.Yield(DispatcherPriority.Background);
            ids.Add(Environment.CurrentManagedThreadId);
        });

        Assert.Single(ids.Distinct());
        Assert.NotEqual(Environment.CurrentManagedThreadId, ids[0]);
    }

    [Fact]
    public void ExceptionFromWorkTheBodyQueued_ComesBackToTheTest_AndDoesNotEndTheThread()
    {
        var thrown = Assert.Throws<AggregateException>(() => StaTestThread.Run(() =>
            Dispatcher.CurrentDispatcher.BeginInvoke(DispatcherPriority.Background, new Action(() => throw new FormatException("queued work failed")))));

        Assert.Contains(thrown.InnerExceptions, e => e is FormatException { Message: "queued work failed" });
        Assert.Equal(7, StaTestThread.Run(() => 7));
    }

    [Fact]
    public async Task EnterExclusive_KeepsABodyOut_UntilItIsDisposed()
    {
        var ran = new ManualResetEventSlim();
        Task caller;
        using (StaTestThread.EnterExclusive())
        {
            caller = Task.Run(() => StaTestThread.Run(() => ran.Set()));
            Assert.False(ran.Wait(TimeSpan.FromMilliseconds(200)), "a body ran while a thread that owns its own thread held the gate");
        }

        await caller.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        Assert.True(ran.IsSet);
    }

    private static (int Id, ApartmentState Apartment, bool Background) Describe() =>
        (Environment.CurrentManagedThreadId, Thread.CurrentThread.GetApartmentState(), Thread.CurrentThread.IsBackground);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowFromNamedFrame() => throw new InvalidOperationException("boom from the body");

    private static void InterlockedMax(ref int target, int value)
    {
        int seen;
        while ((seen = Volatile.Read(ref target)) < value && Interlocked.CompareExchange(ref target, value, seen) != seen)
        {
        }
    }
}
