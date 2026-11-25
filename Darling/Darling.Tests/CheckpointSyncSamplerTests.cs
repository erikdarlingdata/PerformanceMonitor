/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins #4823's self-alert half: the once-a-minute sample of the checkpointer's cumulative sync time and the
/// window maximum it keeps. The hour's AVERAGE sync per checkpoint hides one checkpoint that synced for 23.5 s
/// among four, the sync collection stalled inside. The sampler differences the cumulative <c>sync_time</c> minute
/// by minute, keeps the largest difference for the hour's stored row, and keeps every sync with the minute it
/// finished in, which is what the Store Checkpointer Pressure alert sets a suffering store read or write against
/// (#9549; <see cref="StoreToastAndCheckpointerTests"/> holds the evaluator half). The same class pins the two one-column
/// reads in <c>StoreSelfMetrics</c> (which statement each PostgreSQL major gets) and the worker's wiring: a one-minute
/// gate ahead of the hourly tick, and a failed read that is a Debug line and a skipped minute.
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

    /* ---- #9549: every sync of the window, with the minute it finished in ---------------------------------------- */

    [Fact]
    public void TakeWindowMax_HandsOverEverySyncOfTheWindow_WithTheSampleBeforeItAndTheOneThatSawIt()
    {
        var sampler = new CheckpointSyncSampler();
        Assert.Empty(sampler.TakenSyncs());

        sampler.Observe(T0, 500_000);
        sampler.Observe(T0.AddMinutes(1), 500_400);
        sampler.Observe(T0.AddMinutes(2), 500_400);
        sampler.Observe(T0.AddMinutes(3), 523_942);

        /* Nothing is handed over until the hourly tick takes the window. */
        Assert.Empty(sampler.TakenSyncs());

        Assert.Equal(new CheckpointSyncMax(T0.AddMinutes(3), 23_542), sampler.TakeWindowMax());

        /* One sync per minute in which sync time finished, oldest first. The quiet minute is not a sync. */
        Assert.Equal(
            new[]
            {
                new CheckpointSync(T0, T0.AddMinutes(1), 400),
                new CheckpointSync(T0.AddMinutes(2), T0.AddMinutes(3), 23_542),
            },
            sampler.TakenSyncs());

        /* Reading them does not take them: the same window reads the same until the next take. */
        Assert.Equal(2, sampler.TakenSyncs().Count);

        /* The next window holds no sync, and the take hands over none. */
        sampler.Observe(T0.AddMinutes(4), 523_942);
        Assert.Equal(new CheckpointSyncMax(T0.AddMinutes(4), 0), sampler.TakeWindowMax());
        Assert.Empty(sampler.TakenSyncs());
    }

    [Fact]
    public void ASkippedMinute_WidensTheSyncBackToTheLastSampleThatWasRead()
    {
        var sampler = new CheckpointSyncSampler();

        /* The samples at 12:01 and 12:02 failed, so the 9 s the counter grew by finished somewhere in three minutes. */
        sampler.Observe(T0, 0);
        sampler.Observe(T0.AddMinutes(3), 9_000);
        sampler.TakeWindowMax();

        var sync = Assert.Single(sampler.TakenSyncs());
        Assert.Equal(new CheckpointSync(T0, T0.AddMinutes(3), 9_000), sync);
        Assert.Equal(T0.AddSeconds(-9), sync.StartedUtc);
    }

    [Fact]
    public void ACounterThatFalls_IsNotASync_AndTheNextDifferenceStartsAtTheNewBaseline()
    {
        var sampler = new CheckpointSyncSampler();

        sampler.Observe(T0, 500_000);
        sampler.Observe(T0.AddMinutes(1), 200);
        sampler.Observe(T0.AddMinutes(2), 500);
        sampler.TakeWindowMax();

        Assert.Equal(new[] { new CheckpointSync(T0.AddMinutes(1), T0.AddMinutes(2), 300) }, sampler.TakenSyncs());
    }

    [Fact]
    public void AWindow_KeepsAtMostADaysWorthOfSyncs_TheNewest()
    {
        var sampler = new CheckpointSyncSampler();
        sampler.Observe(T0, 0);
        for (var minute = 1; minute <= CheckpointSyncSampler.MaxWindowSyncs + 2; minute++)
        {
            sampler.Observe(T0.AddMinutes(minute), minute);
        }

        sampler.TakeWindowMax();

        var syncs = sampler.TakenSyncs();
        Assert.Equal(CheckpointSyncSampler.MaxWindowSyncs, syncs.Count);
        Assert.Equal(T0.AddMinutes(3), syncs[0].SampledUtc);
        Assert.Equal(T0.AddMinutes(CheckpointSyncSampler.MaxWindowSyncs + 2), syncs[^1].SampledUtc);
    }

    [Theory]
    [InlineData(14)]
    [InlineData(15)]
    [InlineData(16)]
    public void TheSyncTimeRead_BelowSeventeen_ReadsTheBgwriterColumn(int major) =>
        Assert.Equal(
            "SELECT round(checkpoint_sync_time)::bigint FROM pg_stat_bgwriter",
            StoreSelfMetrics.CheckpointerSyncTimeSqlFor(major));

    [Theory]
    [InlineData(17)]
    [InlineData(18)]
    public void TheSyncTimeRead_FromSeventeen_ReadsTheCheckpointerView(int major) =>
        Assert.Equal(
            "SELECT round(sync_time)::bigint FROM pg_stat_checkpointer",
            StoreSelfMetrics.CheckpointerSyncTimeSqlFor(major));

    [Fact]
    public void TheSyncTimeRead_IsOneColumnOfMilliseconds_RoundedTheWayTheHourlyRowRoundsIt()
    {
        /* Both statements select one rounded bigint and write nothing: the sampler keeps its baseline in memory. */
        foreach (var sql in new[] { StoreSelfMetrics.CheckpointerSyncTimeSql, StoreSelfMetrics.CheckpointerBgwriterSyncTimeSql })
        {
            Assert.StartsWith("SELECT round(", sql, StringComparison.Ordinal);
            Assert.EndsWith("::bigint FROM " + (sql.Contains("pg_stat_bgwriter", StringComparison.Ordinal) ? "pg_stat_bgwriter" : "pg_stat_checkpointer"), sql, StringComparison.Ordinal);
            Assert.DoesNotContain("INSERT", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(",", sql, StringComparison.Ordinal);
        }

        /* The same expression the hourly row stores, so a minute's difference and the hour's compare. */
        Assert.Contains("round(c.sync_time)::bigint", StoreSelfMetrics.CheckpointerInsertSql, StringComparison.Ordinal);
        Assert.Contains("round(b.checkpoint_sync_time)::bigint", StoreSelfMetrics.CheckpointerBgwriterInsertSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSyncTimeRead_SwitchesAtTheMajorTheHourlyRowSwitchesAt()
    {
        Assert.Equal(17, StoreSelfMetrics.CheckpointerViewMajorVersion);
        Assert.Equal(StoreSelfMetrics.CheckpointerSyncTimeSql, StoreSelfMetrics.CheckpointerSyncTimeSqlFor(StoreSelfMetrics.CheckpointerViewMajorVersion));
        Assert.Equal(StoreSelfMetrics.CheckpointerBgwriterSyncTimeSql, StoreSelfMetrics.CheckpointerSyncTimeSqlFor(StoreSelfMetrics.CheckpointerViewMajorVersion - 1));

        /* Both reads take the major from the open connection, with no round trip of their own. */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "StoreSelfMetrics.cs");
        Assert.Contains("connection.PostgreSqlVersion.Major >= CheckpointerViewMajorVersion", source, StringComparison.Ordinal);
        Assert.Contains("CheckpointerSyncTimeSqlFor(connection.PostgreSqlVersion.Major)", source, StringComparison.Ordinal);
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
        var gate = worker.IndexOf("StampIsDue(_nextCheckpointSyncSampleUtc, s_checkpointSyncSampleInterval, DateTime.UtcNow)", StringComparison.Ordinal);
        var tick = worker.IndexOf("if (StampIsDue(_nextStoreMetricsUtc, s_storeMetricsInterval, DateTime.UtcNow))", StringComparison.Ordinal);
        Assert.True(gate >= 0 && gate < tick, "the one-minute sample gate must run ahead of the hourly store self-metrics tick");

        /* #4834: the hourly row stores the window's maximum whether or not a self-alert evaluator exists, so the
           sample is not gated on one. */
        var gateLine = worker[worker.LastIndexOf('\n', gate)..worker.IndexOf('\n', gate)];
        Assert.DoesNotContain("_selfAlerts", gateLine, StringComparison.Ordinal);
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
