/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins #4823's self-alert half: the once-a-minute sample of the checkpointer's cumulative sync time and the
/// window maximum it keeps. The Store Checkpointer Pressure alert judged only the hour's AVERAGE sync per
/// checkpoint, so one checkpoint that synced for 23.5 s among four in the hour stayed under the 10 s bar while
/// collection stalled. The sampler differences the cumulative <c>sync_time</c> minute by minute and keeps the
/// largest difference, which the hourly evaluation then judges beside the average
/// (<see cref="StoreToastAndCheckpointerTests"/> holds the evaluator half).
///
/// <para>Why the SYNC delta alone, with no checkpoint count beside it: PostgreSQL adds a checkpoint's whole
/// sync time to <c>sync_time</c> when the checkpoint ENDS, while <c>num_timed</c> counts it when it STARTS (and
/// counts skipped ones on 17), so pairing a minute's count with that minute's sync time misattributes. A
/// minute's sync delta is one checkpoint's sync, or the sum of two that finished inside the same minute, which
/// can only over-report.</para>
/// </summary>
public sealed class CheckpointSyncSamplerTests
{
    private static readonly DateTime T0 = new(2026, 9, 29, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void TheFirstSample_OnlySetsTheBaseline_AndStatesNoDifference()
    {
        var sampler = new CheckpointSyncSampler();

        /* A cumulative counter read once says nothing about one minute: whatever it holds was earned since the
           statistics started, not since the last sample. */
        sampler.Observe(T0, 500_000);

        Assert.Null(sampler.TakeWindowMax());
    }

    [Fact]
    public void OneJump_IsTheWindowMaximum_StampedWithTheSampleThatSawIt()
    {
        var sampler = new CheckpointSyncSampler();

        sampler.Observe(T0, 500_000);
        sampler.Observe(T0.AddMinutes(1), 500_000);
        sampler.Observe(T0.AddMinutes(2), 523_542);
        sampler.Observe(T0.AddMinutes(3), 523_542);

        Assert.Equal(new CheckpointSyncMax(T0.AddMinutes(2), 23_542), sampler.TakeWindowMax());
    }

    [Fact]
    public void TheLargerOfTwoJumps_Wins_InEitherOrder()
    {
        var largerFirst = new CheckpointSyncSampler();
        largerFirst.Observe(T0, 0);
        largerFirst.Observe(T0.AddMinutes(1), 9_000);
        largerFirst.Observe(T0.AddMinutes(2), 13_000);
        Assert.Equal(new CheckpointSyncMax(T0.AddMinutes(1), 9_000), largerFirst.TakeWindowMax());

        var largerLast = new CheckpointSyncSampler();
        largerLast.Observe(T0, 0);
        largerLast.Observe(T0.AddMinutes(1), 4_000);
        largerLast.Observe(T0.AddMinutes(2), 13_000);
        Assert.Equal(new CheckpointSyncMax(T0.AddMinutes(2), 9_000), largerLast.TakeWindowMax());
    }

    [Fact]
    public void AQuietWindow_IsAMeasuredZero_NotAnAbsence()
    {
        var sampler = new CheckpointSyncSampler();

        sampler.Observe(T0, 12_345);
        sampler.Observe(T0.AddMinutes(1), 12_345);

        /* Null means "no difference was taken"; a minute in which no sync time finished IS a difference, of zero. */
        Assert.Equal(new CheckpointSyncMax(T0.AddMinutes(1), 0), sampler.TakeWindowMax());
    }

    [Fact]
    public void ACounterThatFalls_IsANewBaseline_WithNoDifference()
    {
        var sampler = new CheckpointSyncSampler();

        sampler.Observe(T0, 900_000);
        sampler.Observe(T0.AddMinutes(1), 100);

        /* A restart or a statistics reset: the new value is not a difference (neither the fall nor the 100 ms). */
        Assert.Null(sampler.TakeWindowMax());

        sampler.Observe(T0.AddMinutes(2), 400);

        Assert.Equal(new CheckpointSyncMax(T0.AddMinutes(2), 300), sampler.TakeWindowMax());
    }

    [Fact]
    public void ACounterThatFalls_DoesNotDiscardAMaximumTheWindowAlreadyHolds()
    {
        var sampler = new CheckpointSyncSampler();

        sampler.Observe(T0, 0);
        sampler.Observe(T0.AddMinutes(1), 23_542);
        sampler.Observe(T0.AddMinutes(2), 50);

        Assert.Equal(new CheckpointSyncMax(T0.AddMinutes(1), 23_542), sampler.TakeWindowMax());
    }

    [Fact]
    public void TakeWindowMax_ClearsTheWindow_ButKeepsTheBaseline()
    {
        var sampler = new CheckpointSyncSampler();

        sampler.Observe(T0, 0);
        sampler.Observe(T0.AddMinutes(1), 5_000);
        Assert.Equal(new CheckpointSyncMax(T0.AddMinutes(1), 5_000), sampler.TakeWindowMax());
        Assert.Null(sampler.TakeWindowMax());

        /* The counter still reads 5,000 as its baseline, so the next window measures 700, not 5,700. */
        sampler.Observe(T0.AddMinutes(2), 5_700);
        Assert.Equal(new CheckpointSyncMax(T0.AddMinutes(2), 700), sampler.TakeWindowMax());
    }

    [Fact]
    public void TheWorker_SamplesOnAOneMinuteGateAheadOfTheHourlyTick_AndSkipsAFailureAtDebug()
    {
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs")
            .Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Matches(@"s_checkpointSyncSampleInterval\s*=\s*TimeSpan\.FromMinutes\(1\);", worker);
        Assert.Contains("private DateTime _nextCheckpointSyncSampleUtc = DateTime.MinValue;", worker, StringComparison.Ordinal);
        Assert.Contains("private readonly CheckpointSyncSampler _checkpointSyncSampler = new();", worker, StringComparison.Ordinal);

        /* The gate sits AHEAD of the hourly tick, so the sample taken on a tick's own iteration is inside the
           window the tick's evaluation takes. */
        var gate = worker.IndexOf("DateTime.UtcNow >= _nextCheckpointSyncSampleUtc", StringComparison.Ordinal);
        var tick = worker.IndexOf("if (DateTime.UtcNow >= _nextStoreMetricsUtc)", StringComparison.Ordinal);
        Assert.True(gate >= 0 && gate < tick, "the one-minute sample gate must run ahead of the hourly store self-metrics tick");
        Assert.Contains(
            "_nextCheckpointSyncSampleUtc = NextGridStamp(_nextCheckpointSyncSampleUtc, DateTime.UtcNow, s_checkpointSyncSampleInterval);",
            worker, StringComparison.Ordinal);

        /* The sample: one read, one Observe; a failure is a Debug line and a skipped minute, never a stopped loop. */
        var method = worker.IndexOf("private async Task SampleCheckpointSyncAsync(", StringComparison.Ordinal);
        Assert.True(method > 0, "the worker must own SampleCheckpointSyncAsync");
        var end = worker.IndexOf("\n    }\n", method, StringComparison.Ordinal);
        var body = worker[method..end];
        Assert.Contains("StoreSelfMetrics.ReadCheckpointerSyncTimeMsAsync(", body, StringComparison.Ordinal);
        Assert.Contains("_checkpointSyncSampler.Observe(", body, StringComparison.Ordinal);
        Assert.Contains("_logger.LogDebug(", body, StringComparison.Ordinal);
        Assert.DoesNotContain("throw;", body, StringComparison.Ordinal);
    }
}
