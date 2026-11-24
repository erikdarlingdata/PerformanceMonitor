/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9424: a slow MCP call's row carries the highest process working set and the highest DuckDB memory read while the call ran, plus
/// the words that say where each came from, and a fast call starts no sampler. The recorder is driven through its own seams (the
/// readings, the sampling start delay, the slow threshold) and a held call, so no case waits on a clock.
/// </summary>
public sealed class McpCallMemoryPeakTests
{
    private static (McpCallTimingRecorder Recorder, ConcurrentQueue<McpSlowCall> Rows) Make(Func<long?> duckDb, TimeSpan slowThreshold)
    {
        var rows = new ConcurrentQueue<McpSlowCall>();
        var recorder = new McpCallTimingRecorder(c => { rows.Enqueue(c); return Task.CompletedTask; }, duckDb)
        {
            SlowThreshold = slowThreshold,
            SamplingStartDelay = TimeSpan.Zero,
            SampleInterval = TimeSpan.FromMilliseconds(1),
            ReadWorkingSet = () => 5_000,
        };
        return (recorder, rows);
    }

    [Fact]
    public async Task ASlowCallHeldOpen_RecordsTheHighestDuckDbReadAndTheWorkingSet_NotTheLastOne_AndNamesTheirSource()
    {
        /* The first read, taken as the call joins the sampler, is the highest. Every later read, the closing one included, is lower. */
        var reads = 0;
        var (recorder, rows) = Make(() => Interlocked.Increment(ref reads) == 1 ? 9_000 : 3_000, TimeSpan.Zero);
        var hold = new TaskCompletionSource();

        var call = recorder.Begin("held_tool", null);
        await Task.WhenAny(hold.Task, Task.Delay(1));
        call.Finish(McpCallTimingRecorder.OutcomeOk, null);
        call.Dispose();
        await recorder.WhenWritesDoneAsync();

        var row = Assert.Single(rows);
        Assert.Equal(5_000, row.ProcessPeakWorkingSetBytes);
        Assert.Equal(9_000, row.DuckDbPeakMemoryBytes);
        Assert.False(string.IsNullOrWhiteSpace(row.MemorySource));
    }

    [Fact]
    public async Task TheSamplerLoop_ReadsWhileTheCallRuns_AndTheRowKeepsTheHighestReading()
    {
        /* Read 1 is the call joining, read 2 the first tick (the high one), read 3 the second tick, which opens the gate. The call
           ends only after read 3, so read 2 has been handed to the call by then. The closing read is lower than the tick. */
        var reads = 0;
        using var secondTick = new ManualResetEventSlim(false);
        var (recorder, rows) = Make(() =>
        {
            var n = Interlocked.Increment(ref reads);
            if (n == 3) secondTick.Set();
            return n switch { 1 => 100, 2 => 7_000, 3 => 50, _ => 10 };
        }, TimeSpan.Zero);

        var call = recorder.Begin("held_tool", null);
        Assert.True(secondTick.Wait(TimeSpan.FromSeconds(30)), "the sampler loop never ticked");
        call.Finish(McpCallTimingRecorder.OutcomeOk, null);
        call.Dispose();
        await recorder.WhenWritesDoneAsync();

        var row = Assert.Single(rows);
        Assert.Equal(7_000, row.DuckDbPeakMemoryBytes);
        Assert.Equal(1, recorder.SamplerStarts);
    }

    [Fact]
    public async Task AFastCall_StartsNoSamplerAndReadsNoDuckDbMemory()
    {
        var duckReads = 0;
        var (recorder, rows) = Make(() => { Interlocked.Increment(ref duckReads); return 1; }, TimeSpan.FromHours(1));
        recorder.SamplingStartDelay = McpCallTimingRecorder.DefaultSamplingStartDelay;

        for (var i = 0; i < 20; i++)
        {
            var call = recorder.Begin("fast_tool", null);
            call.Finish(McpCallTimingRecorder.OutcomeOk, null);
            call.Dispose();
        }

        await recorder.WhenWritesDoneAsync();
        Assert.Equal(0, recorder.SamplerStarts);
        Assert.Equal(0, duckReads);
        Assert.Empty(rows);
    }

    [Fact]
    public async Task CallsThatOverlap_ShareOneSamplerLoop_AndEachRowGetsAFigure()
    {
        var (recorder, rows) = Make(() => 4_000, TimeSpan.Zero);

        var first = recorder.Begin("tool_a", null);
        var second = recorder.Begin("tool_b", null);
        first.Finish(McpCallTimingRecorder.OutcomeOk, null);
        first.Dispose();
        second.Finish(McpCallTimingRecorder.OutcomeOk, null);
        second.Dispose();
        await recorder.WhenWritesDoneAsync();

        Assert.Equal(1, recorder.SamplerStarts);
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal(4_000, r.DuckDbPeakMemoryBytes));
        Assert.All(rows, r => Assert.True(r.ProcessPeakWorkingSetBytes > 0));
    }

    [Fact]
    public async Task ASlowCallWhoseDuckDbReadsAllFindTheLockBusy_RecordsNoDuckDbFigure_ButStillTheWorkingSet()
    {
        var (recorder, rows) = Make(() => null, TimeSpan.Zero);

        var call = recorder.Begin("busy_tool", null);
        call.Finish(McpCallTimingRecorder.OutcomeOk, null);
        call.Dispose();
        await recorder.WhenWritesDoneAsync();

        var row = Assert.Single(rows);
        Assert.Null(row.DuckDbPeakMemoryBytes);
        Assert.Equal(5_000, row.ProcessPeakWorkingSetBytes);
        Assert.NotNull(rows.Single().MemorySource);
    }
}
