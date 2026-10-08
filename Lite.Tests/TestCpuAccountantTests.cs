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
/// is 40 ms of CPU. Thread CPU comes from GetThreadTimes, which reads in scheduler ticks (about 15.6 ms), so the
/// bounds are loose on purpose: they separate "charged about the burn" from "charged about nothing".
/// </summary>
public sealed class TestCpuAccountantTests
{
    private const long BurnMs = 40;
    private const long ChargedAtLeastMs = 10;

    /// <summary>
    /// Spends <see cref="BurnMs"/> of THIS thread's CPU time, read from the same counter the accountant charges from.
    /// A wall-clock burn (a Stopwatch loop) charged only the slices the thread got in those 40 ms, so on a loaded
    /// runner two burns could add up to 46 ms and miss the 50 ms lower bound (#5459). This loop keeps spinning until
    /// the thread's own CPU counter has advanced by the burn, so the charge covers it however the thread was
    /// scheduled. The 30 s wall limit is only a hang backstop (a counter that never moves).
    /// </summary>
    private static void Burn()
    {
        var wall = Stopwatch.StartNew();
        var start = TestCpuAccountant.ThreadCpuTicks();
        long sink = 0;
        while ((TestCpuAccountant.ThreadCpuTicks() - start) / TimeSpan.TicksPerMillisecond < BurnMs
               && wall.Elapsed < TimeSpan.FromSeconds(30))
        {
            sink += wall.ElapsedTicks & 1;
        }
        GC.KeepAlive(sink);
    }

    /// <summary>Runs <paramref name="work"/> under a fresh charge on its own execution context; returns the ms charged.</summary>
    private static Task<long> ChargedMs(Func<Task> work) => Task.Run(async () =>
    {
        var charge = TestCpuAccountant.Begin();
        await work();
        return TestCpuAccountant.End(charge) / TimeSpan.TicksPerMillisecond;
    });

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
            await Task.Run(Burn);
            await Task.Delay(1);
            Burn();
        });
        // Two burns, on up to three different threads: both must land on the one charge.
        Assert.InRange(ms, BurnMs + ChargedAtLeastMs, BurnMs * 8);
    }

    [Fact]
    public async Task Two_tests_in_parallel_are_charged_separately()
    {
        var busy = ChargedMs(() => { Burn(); return Task.CompletedTask; });
        var idle = ChargedMs(async () => await Task.Delay((int)BurnMs));
        await Task.WhenAll(busy, idle);
        Assert.InRange(busy.Result, ChargedAtLeastMs, BurnMs * 4);
        Assert.True(idle.Result < ChargedAtLeastMs, $"the idle test was charged {idle.Result} ms");
    }

    [Fact]
    public async Task Work_queued_without_the_execution_context_is_not_charged()
    {
        // Documented limit, not a bug: UnsafeQueueUserWorkItem does not flow the context, so nobody is charged.
        long ms = await ChargedMs(() =>
        {
            using var done = new ManualResetEventSlim();
            ThreadPool.UnsafeQueueUserWorkItem(_ => { Burn(); done.Set(); }, null);
            Assert.True(done.Wait(TimeSpan.FromSeconds(30)));
            return Task.CompletedTask;
        });
        Assert.True(ms < ChargedAtLeastMs, $"unflowed work was charged {ms} ms");
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
