/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5208 (W2b): pins the per-test CPU accountant. Each case measures a charge opened inside its own
/// <see cref="Task.Run(Func{Task})"/>, so the assembly-level charge of the test running it is untouched. Every burn
/// is at least 40 ms of CPU. Thread CPU comes from GetThreadTimes, which reads in scheduler ticks (about 15.6 ms),
/// so the bounds are loose on purpose: they separate "charged about the burn" from "charged about nothing". A side
/// that must read as nothing is allowed one stray tick (#5459): a bound under the counter's own tick fails whenever
/// the clock interrupt lands on an idle thread, which no test can prevent.
/// </summary>
public sealed class TestCpuAccountantTests
{
    private const long BurnMs = 40;
    private const long ChargedAtLeastMs = 10;

    private static readonly Lazy<long> ClockTick = new(MeasureClockTick);

    /// <summary>One step of the thread CPU counter, in 100 ns ticks (about 156250 on a 15.6 ms scheduler tick).</summary>
    private static long TickTicks => ClockTick.Value;

    /// <summary>What a "busy" side burns: the usual 40 ms, or four counter ticks when the tick is longer than 10 ms.</summary>
    private static long BusyTicks => Math.Max(BurnMs * TimeSpan.TicksPerMillisecond, 4 * TickTicks);

    /// <summary>
    /// The most a side that must read as "nothing" may be charged: two counter ticks (one stray tick is a step of the
    /// counter, not CPU), and never under the 10 ms floor the other tests use when the counter is finer than that.
    /// </summary>
    private static long IdleLimitTicks => Math.Max(ChargedAtLeastMs * TimeSpan.TicksPerMillisecond, 2 * TickTicks);

    /// <summary>
    /// Reads the counter's step on this thread: spins until the counter moves, then times the next two moves and
    /// keeps the larger step. A tick counter steps by one tick every time; a finer counter steps by almost nothing,
    /// and then the 10 ms floors above apply. Costs up to three ticks of CPU, once per process.
    /// </summary>
    private static long MeasureClockTick()
    {
        if (!TestCpuAccountant.Enabled)
        {
            return 156250;
        }
        var wall = Stopwatch.StartNew();
        long last = TestCpuAccountant.ThreadCpuTicks();
        long step = 0;
        int moves = 0;
        while (moves < 3 && wall.Elapsed < TimeSpan.FromSeconds(30))
        {
            long now = TestCpuAccountant.ThreadCpuTicks();
            if (now == last)
            {
                continue;
            }
            if (++moves > 1)
            {
                step = Math.Max(step, now - last);
            }
            last = now;
        }
        Assert.True(step > 0, "the thread CPU counter never moved while the test spun");
        return step;
    }

    /// <summary>
    /// Spends <paramref name="ticks"/> (100 ns) of THIS thread's CPU time, read from the same counter the accountant
    /// charges from. A wall-clock burn (a Stopwatch loop) charged only the slices the thread got in those 40 ms, so on
    /// a loaded runner two burns could add up to 46 ms and miss the 50 ms lower bound (#5459). This loop keeps
    /// spinning until the thread's own CPU counter has advanced by the burn, so the charge covers it however the
    /// thread was scheduled. The 30 s wall limit is only a hang backstop (a counter that never moves).
    /// </summary>
    private static void Burn(long ticks = BurnMs * TimeSpan.TicksPerMillisecond)
    {
        var wall = Stopwatch.StartNew();
        var start = TestCpuAccountant.ThreadCpuTicks();
        long sink = 0;
        while (TestCpuAccountant.ThreadCpuTicks() - start < ticks
               && wall.Elapsed < TimeSpan.FromSeconds(30))
        {
            sink += wall.ElapsedTicks & 1;
        }
        GC.KeepAlive(sink);
    }

    /// <summary>Runs <paramref name="work"/> under a fresh charge on its own execution context; returns the 100 ns ticks charged.</summary>
    private static Task<long> ChargedTicks(Func<Task> work) => Task.Run(async () =>
    {
        var charge = TestCpuAccountant.Begin();
        await work();
        return TestCpuAccountant.End(charge);
    });

    /// <summary>The same, in whole milliseconds.</summary>
    private static async Task<long> ChargedMs(Func<Task> work) => await ChargedTicks(work) / TimeSpan.TicksPerMillisecond;

    [Fact]
    public async Task Work_on_the_tests_own_thread_is_charged()
    {
        Assert.True(TestCpuAccountant.Enabled);
        long ms = await ChargedMs(() => { Burn(); return Task.CompletedTask; });
        Assert.InRange(ms, ChargedAtLeastMs, BurnMs * 4);
    }

    [Fact]
    public async Task Work_after_an_await_on_another_pool_thread_is_charged_to_the_same_test()
    {
        long ms = await ChargedMs(async () =>
        {
            await Task.Run(() => Burn());
            await Task.Delay(1);
            Burn();
        });
        // Two burns, on up to three different threads: both must land on the one charge.
        Assert.InRange(ms, BurnMs + ChargedAtLeastMs, BurnMs * 8);
    }

    [Fact]
    public async Task Two_tests_in_parallel_are_charged_separately()
    {
        long busyBurn = BusyTicks;
        var busy = ChargedTicks(() => { Burn(busyBurn); return Task.CompletedTask; });
        var idle = ChargedTicks(async () => await Task.Delay((int)BurnMs));
        await Task.WhenAll(busy, idle);
        // The busy side burned at least four counter ticks; the idle side may read one stray tick, never two.
        Assert.InRange(busy.Result, busyBurn, busyBurn * 4);
        Assert.True(idle.Result < IdleLimitTicks,
            $"the idle test was charged {idle.Result / TimeSpan.TicksPerMillisecond} ms (limit {IdleLimitTicks / TimeSpan.TicksPerMillisecond} ms)");
    }

    [Fact]
    public async Task One_stray_tick_on_the_idle_side_does_not_hide_the_separation()
    {
        // #5459: the clock interrupt can land on a test that is only waiting, and the counter then reads one whole
        // tick for it. Plant exactly that: the idle side burns one counter tick of its own. A bound under one tick
        // (the old 10 ms) failed here every time; two tick-sized bounds keep the sides apart and still pass.
        long busyBurn = BusyTicks;
        long strayTick = TickTicks;
        var busy = ChargedTicks(() => { Burn(busyBurn); return Task.CompletedTask; });
        var idle = ChargedTicks(async () =>
        {
            await Task.Delay((int)BurnMs);
            Burn(strayTick);
        });
        await Task.WhenAll(busy, idle);
        Assert.InRange(busy.Result, busyBurn, busyBurn * 4);
        Assert.InRange(idle.Result, strayTick, IdleLimitTicks - 1);
    }

    [Fact]
    public async Task Work_queued_without_the_execution_context_is_not_charged()
    {
        // Documented limit, not a bug: UnsafeQueueUserWorkItem does not flow the context, so nobody is charged.
        // The unflowed work burns four counter ticks; the waiting thread may still read one stray tick.
        long busyBurn = BusyTicks;
        long ticks = await ChargedTicks(() =>
        {
            using var done = new ManualResetEventSlim();
            ThreadPool.UnsafeQueueUserWorkItem(_ => { Burn(busyBurn); done.Set(); }, null);
            Assert.True(done.Wait(TimeSpan.FromSeconds(30)));
            return Task.CompletedTask;
        });
        Assert.True(ticks < IdleLimitTicks,
            $"unflowed work was charged {ticks / TimeSpan.TicksPerMillisecond} ms (limit {IdleLimitTicks / TimeSpan.TicksPerMillisecond} ms)");
    }

    [Fact]
    public void The_run_summary_line_and_sidecar_path_read_the_xml_argument()
    {
        Assert.Equal("Lite test CPU coverage: charged 600 ms over 3 tests, process 1000 ms, ratio 60.0 %",
            TestCpuRunSummary.Line(600, 1000, 3));
        Assert.Equal("TestResults/a.xml.cpu.json", TestCpuRunSummary.SidecarPath(new[] { "-class", "X", "-xml", "TestResults/a.xml" }));
        Assert.Null(TestCpuRunSummary.SidecarPath(new[] { "-class", "X" }));
        Assert.Null(TestCpuRunSummary.SidecarPath(new[] { "-xml" }));
    }
}
