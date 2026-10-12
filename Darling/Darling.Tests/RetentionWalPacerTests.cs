/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4823: the daily retention purge deleted 743,502 rows in seven minutes, 386,718 of them from the plan
/// dimension, and the checkpoint interval that covered it carried 6.89 GB of WAL against 0.6-1.5 GB normally.
/// <see cref="RetentionWalPacer"/> holds the purge's WAL rate to half of what the store's own checkpoint
/// schedule absorbs. Everything here is offline: the pacer takes an injected delay and clock, so no test
/// sleeps.
/// </summary>
public sealed class RetentionWalPacerTests
{
    private const long MiB = 1_048_576;

    /// <summary>A clock and a delay that never sleep: waits are recorded, time moves only when a test says so.</summary>
    private sealed class FakeTime
    {
        public double Now { get; set; }

        public List<double> Waits { get; } = new();

        public Task Delay(TimeSpan wait, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Waits.Add(wait.TotalSeconds);
            return Task.CompletedTask;
        }
    }

    private static RetentionWalPacer PacerAt(long rateBytesPerSecond, FakeTime time, ILogger? logger = null) =>
        new(rateBytesPerSecond, time.Delay, () => time.Now, logger);

    private static List<(string Name, string Setting, string? Unit)> Rows(
        string maxWalSize = "16384", string? maxWalUnit = "MB", string target = "0.9", string timeout = "900") =>
        new()
        {
            ("max_wal_size", maxWalSize, maxWalUnit),
            ("checkpoint_completion_target", target, null),
            ("checkpoint_timeout", timeout, "s"),
        };

    /* ---------------- the rate, from the store's own settings ---------------- */

    [Fact]
    public void RateFromSettings_IsHalfOfWhatTheCheckpointScheduleAbsorbs()
    {
        /* 16384 MB / (1 + 0.9) / 900 s / 2 */
        Assert.Equal(5_023_353, RetentionWalPacer.RateFromSettings(16_384, 0.9, 900));
        Assert.Equal(60_280_242, RetentionWalPacer.RateFromSettings(65_536, 0.9, 300));
    }

    [Fact]
    public void RateFromSettings_PostgreSqlDefaults_ClampToTheFloor()
    {
        /* max_wal_size 1024 MB, completion target 0.9, timeout 300 s: 941,878 unclamped, under the floor. */
        Assert.Equal(941_878, RetentionWalPacer.RawRateFromSettings(1_024, 0.9, 300));
        Assert.Equal(1_048_576, RetentionWalPacer.RateFromSettings(1_024, 0.9, 300));
    }

    [Fact]
    public void RateFromSettings_ClampsToTheCeiling()
    {
        Assert.Equal(67_108_864, RetentionWalPacer.RateFromSettings(1_048_576, 0.9, 300));
    }

    [Fact]
    public void FromSettingRows_ReadsTheThreeSettingsWithoutNoise()
    {
        var log = new CapturingTestLogger();

        var pacer = RetentionWalPacer.FromSettingRows(Rows(), log);

        Assert.Equal(5_023_353, pacer.RateBytesPerSecond);
        Assert.Empty(log.Lines);
    }

    [Fact]
    public void FromSettingRows_ANullReadResult_UsesTheDefaultRateAndLogsOnce()
    {
        var log = new CapturingTestLogger();

        var pacer = RetentionWalPacer.FromSettingRows(null, log);

        Assert.Equal(4_194_304, pacer.RateBytesPerSecond);
        Assert.Equal(1, log.CountAtLevel(LogLevel.Information));
        Assert.Contains("4194304", log.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public void FromSettingRows_ASettingThatCannotBeUsed_UsesTheDefaultRateAndLogsOnce()
    {
        var bad = new List<List<(string Name, string Setting, string? Unit)>>
        {
            Rows(maxWalUnit: "8kB"),                                       // an unexpected unit
            Rows(timeout: "0"),                                            // a zero divisor
            Rows(target: "fast"),                                          // not a number
            Rows().Where(r => r.Name != "checkpoint_timeout").ToList(),    // a missing row
        };

        foreach (var rows in bad)
        {
            var log = new CapturingTestLogger();

            var pacer = RetentionWalPacer.FromSettingRows(rows, log);

            Assert.Equal(4_194_304, pacer.RateBytesPerSecond);
            Assert.Equal(1, log.CountAtLevel(LogLevel.Information));
        }
    }

    [Fact]
    public async Task CreateAsync_WhenTheStoreCannotBeReached_UsesTheDefaultRateAndLogsOnce()
    {
        var log = new CapturingTestLogger();

        var pacer = await RetentionWalPacer.CreateAsync(null!, log, TestContext.Current.CancellationToken);

        Assert.Equal(4_194_304, pacer.RateBytesPerSecond);
        Assert.Equal(1, log.CountAtLevel(LogLevel.Information));
    }

    [Fact]
    public void TheBurstIsTenSecondsOfRate_AndABatchTargetIsThirtySeconds()
    {
        var pacer = PacerAt(2 * MiB, new FakeTime());

        Assert.Equal(20 * MiB, pacer.BurstBytes);
        Assert.Equal(60 * MiB, pacer.BatchWalTargetBytes);
    }

    /* ---------------- the token bucket ---------------- */

    [Fact]
    public async Task ABatchUnderTheBurst_DoesNotWait()
    {
        var time = new FakeTime();
        var pacer = PacerAt(MiB, time);

        await pacer.AfterBatchAsync(5 * MiB, TestContext.Current.CancellationToken);

        Assert.Empty(time.Waits);
        Assert.Equal(5 * MiB, pacer.TotalWalBytes);
        Assert.Equal(0, pacer.TotalWaitSeconds);
    }

    [Fact]
    public async Task ABatchOverTheBurst_WaitsForTheRefill()
    {
        var time = new FakeTime();
        var pacer = PacerAt(MiB, time);

        /* The bucket starts with 10 MiB; a 20 MiB batch is 10 MiB in debt, which the rate repays in 10 s. */
        await pacer.AfterBatchAsync(20 * MiB, TestContext.Current.CancellationToken);

        Assert.Equal(new[] { 10.0 }, time.Waits);
        Assert.Equal(10.0, pacer.TotalWaitSeconds, 6);
    }

    [Fact]
    public async Task EachWaitIsAtMostThirtySeconds_AndTheWaitsAddUpToTheDebt()
    {
        var time = new FakeTime();
        var pacer = PacerAt(MiB, time);

        /* 100 MiB against a 10 MiB bucket: 90 MiB in debt, 90 s at 1 MiB/s, in three waits. */
        await pacer.AfterBatchAsync(100 * MiB, TestContext.Current.CancellationToken);

        Assert.Equal(new[] { 30.0, 30.0, 30.0 }, time.Waits);
        Assert.Equal(90.0, pacer.TotalWaitSeconds, 6);
    }

    [Fact]
    public async Task TheBucketCarriesItsDebtAndRefillsWithElapsedTime()
    {
        var time = new FakeTime();
        var pacer = PacerAt(MiB, time);
        var ct = TestContext.Current.CancellationToken;

        await pacer.AfterBatchAsync(10 * MiB, ct);           // empties the bucket, no wait
        time.Now += 4;                                       // 4 s of real time refills 4 MiB
        await pacer.AfterBatchAsync(8 * MiB, ct);            // 4 MiB in debt: 4 s
        await pacer.AfterBatchAsync(2 * MiB, ct);            // paid off exactly, then 2 MiB in debt: 2 s

        Assert.Equal(new[] { 4.0, 2.0 }, time.Waits);
        Assert.Equal(20 * MiB, pacer.TotalWalBytes);
        Assert.Equal(6.0, pacer.TotalWaitSeconds, 6);
    }

    [Fact]
    public async Task TheBucketNeverHoldsMoreThanTheBurst()
    {
        var time = new FakeTime();
        var pacer = PacerAt(MiB, time);

        time.Now += 3600;                                    // an idle hour must not bank an hour of WAL
        await pacer.AfterBatchAsync(20 * MiB, TestContext.Current.CancellationToken);

        Assert.Equal(new[] { 10.0 }, time.Waits);
    }

    [Fact]
    public async Task ZeroAndNegativeWal_AddNothingAndNeverWait()
    {
        var time = new FakeTime();
        var pacer = PacerAt(MiB, time);
        var ct = TestContext.Current.CancellationToken;

        await pacer.AfterBatchAsync(0, ct);
        await pacer.AfterBatchAsync(-5 * MiB, ct);

        Assert.Empty(time.Waits);
        Assert.Equal(0, pacer.TotalWalBytes);
    }

    [Fact]
    public async Task CancellingDuringAWait_StopsIt()
    {
        using var cts = new CancellationTokenSource();
        var pacer = new RetentionWalPacer(
            MiB,
            delay: async (wait, ct) =>
            {
                await cts.CancelAsync();
                await Task.Delay(Timeout.Infinite, ct);
            },
            secondsClock: () => 0);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => pacer.AfterBatchAsync(100 * MiB, cts.Token));
    }

    [Fact]
    public async Task AnAlreadyCancelledToken_NeverStartsAWait()
    {
        var time = new FakeTime();
        var pacer = PacerAt(MiB, time);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => pacer.AfterBatchAsync(100 * MiB, cts.Token));

        Assert.Empty(time.Waits);
    }

    [Fact]
    public async Task AWalPositionThatCannotBeRead_LogsOnce_AndTheRestOfThePurgeRunsUnpaced()
    {
        var time = new FakeTime();
        var log = new CapturingTestLogger();
        var pacer = PacerAt(MiB, time, log);
        var ct = TestContext.Current.CancellationToken;

        /* No connection at all: the read fails the way a broken one would. */
        Assert.Null(await pacer.ReadWalPositionAsync(null!, ct));
        Assert.True(pacer.IsUnpaced);
        Assert.Null(await pacer.ReadWalPositionAsync(null!, ct));

        await pacer.AfterBatchAsync(500 * MiB, ct);

        Assert.Equal(1, log.CountAtLevel(LogLevel.Information) + log.CountAtLevel(LogLevel.Warning));
        Assert.Empty(time.Waits);
    }

    /* ---------------- every batch is paced ---------------- */

    [Fact]
    public async Task DrainBatches_PacesEveryBatchAfterItRuns()
    {
        var time = new FakeTime();
        var pacer = PacerAt(MiB, time);
        var plan = new Queue<(int Deleted, int Cap, long WalBytes)>(new[]
        {
            (1000, 1000, 30 * MiB),
            (1000, 1000, 30 * MiB),
            (10, 1000, 1 * MiB),
        });
        var calls = 0;

        var total = await DarlingRetention.DrainBatchesAsync(
            _ => { calls++; return Task.FromResult(plan.Dequeue()); }, pacer, TestContext.Current.CancellationToken);

        Assert.Equal(2010, total);
        Assert.Equal(3, calls);

        /* Batch 1: 10 MiB in the bucket - 30 = 20 MiB in debt. Batch 2: 30 more. Batch 3: 1 more. */
        Assert.Equal(new[] { 20.0, 30.0, 1.0 }, time.Waits);
        Assert.Equal(61 * MiB, pacer.TotalWalBytes);
    }

    [Fact]
    public async Task DrainBatches_WithoutAPacer_NeverWaits()
    {
        var plan = new Queue<(int Deleted, int Cap, long WalBytes)>(new[] { (1000, 1000, 500 * MiB), (3, 1000, 500 * MiB) });

        var total = await DarlingRetention.DrainBatchesAsync(
            _ => Task.FromResult(plan.Dequeue()), pacer: null, TestContext.Current.CancellationToken);

        Assert.Equal(1003, total);
    }

    /* ---------------- the plan dimension's batch cap follows the WAL too ---------------- */

    private const long Target = 150 * MiB;   // 30 s of a 5 MiB/s rate

    [Fact]
    public void NextPlanDimBatchCap_ABatchWithTooMuchWal_ShrinksTheNextCap()
    {
        /* Fast enough for the time rule to double it, but the batch wrote 2x the target: half the rows. */
        Assert.Equal(
            10_000,
            DarlingRetention.NextPlanDimBatchCap(20_000, 5.0, 1_000, 50_000, lastBatchWalBytes: 2 * Target, walTargetBytes: Target));
    }

    [Fact]
    public void NextPlanDimBatchCap_TheWalRuleScalesTheCapByTargetOverWritten()
    {
        Assert.Equal(
            12_500,
            DarlingRetention.NextPlanDimBatchCap(50_000, 45.0, 1_000, 50_000, lastBatchWalBytes: 4 * Target, walTargetBytes: Target));
    }

    [Fact]
    public void NextPlanDimBatchCap_ABatchUnderTheTarget_DoesNotShrink()
    {
        /* In the time rule's hold band, and under target: the cap holds. */
        Assert.Equal(
            20_000,
            DarlingRetention.NextPlanDimBatchCap(20_000, 45.0, 1_000, 50_000, lastBatchWalBytes: Target / 2, walTargetBytes: Target));

        /* Fast and far under target: the time rule still doubles it. */
        Assert.Equal(
            40_000,
            DarlingRetention.NextPlanDimBatchCap(20_000, 5.0, 1_000, 50_000, lastBatchWalBytes: Target / 8, walTargetBytes: Target));
    }

    [Fact]
    public void NextPlanDimBatchCap_TheSmallerOfTheTimeRuleAndTheWalRuleWins()
    {
        /* Time says halve (152 s: 25,000); WAL is barely over target (41,666): the time rule is smaller. */
        Assert.Equal(
            25_000,
            DarlingRetention.NextPlanDimBatchCap(50_000, 152.0, 1_000, 50_000, lastBatchWalBytes: Target * 6 / 5, walTargetBytes: Target));

        /* Time says halve (25,000); WAL is 5x target (10,000): the WAL rule is smaller. */
        Assert.Equal(
            10_000,
            DarlingRetention.NextPlanDimBatchCap(50_000, 152.0, 1_000, 50_000, lastBatchWalBytes: 5 * Target, walTargetBytes: Target));
    }

    [Fact]
    public void NextPlanDimBatchCap_TheFloorAndTheCeilingHold()
    {
        Assert.Equal(
            1_000,
            DarlingRetention.NextPlanDimBatchCap(2_000, 5.0, 1_000, 50_000, lastBatchWalBytes: 1_000 * Target, walTargetBytes: Target));
        Assert.Equal(
            50_000,
            DarlingRetention.NextPlanDimBatchCap(50_000, 2.0, 1_000, 50_000, lastBatchWalBytes: 1, walTargetBytes: Target));
    }

    [Fact]
    public void NextPlanDimBatchCap_WithNoWalMeasured_IsTheTimeRuleAlone()
    {
        Assert.Equal(25_000, DarlingRetention.NextPlanDimBatchCap(50_000, 152.7, 1_000, 50_000, lastBatchWalBytes: 0, walTargetBytes: Target));
        Assert.Equal(25_000, DarlingRetention.NextPlanDimBatchCap(50_000, 152.7, 1_000, 50_000, lastBatchWalBytes: 10 * Target, walTargetBytes: 0));
    }

    /* ---------------- the run's summary says what it paced ---------------- */

    [Fact]
    public void BuildRunRecordSummary_APacedRun_SaysTheStoresWalDuringTheBatchesAndTheSecondsPaced()
    {
        var (status, message) = DarlingRetention.BuildRunRecordSummary(
            tablesPurged: 33, totalRowsDeleted: 1200, totalChunksDropped: 42, tablesFailed: 0,
            paced: true, walBytes: 6_890_000_000, pacedSeconds: 412.4);

        Assert.Equal("SUCCESS", status);
        Assert.Contains("1200 row(s) deleted", message, StringComparison.Ordinal);
        Assert.Contains("store WAL during the purge's batches: 6571 MB", message, StringComparison.Ordinal);
        Assert.Contains("paced 412 s", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The WAL figure is the WHOLE store's WAL across the purge's batches, collection included
    /// (<c>RetentionWalPacer.WalWrittenSinceAsync</c>), so neither the service log line nor the run record may call
    /// it what the purge wrote. The run record's words are pinned above; the log line has no other pin, so this reads
    /// the source and holds both to the same words.
    /// </summary>
    [Fact]
    public void ThePurgeLogLine_AndTheRunRecord_NameTheWalTheStoresNotThePurges()
    {
        var source = ReadServiceFile("DarlingRetention.cs");

        Assert.DoesNotContain("WAL written", source, StringComparison.Ordinal);
        Assert.Contains("ms; store WAL during the purge's batches: {WalMb:F0} MB, paced", source, StringComparison.Ordinal);
        Assert.Contains("; store WAL during the purge's batches: {(walBytes / 1_048_576.0)", source, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildRunRecordSummary_AnUnpacedRun_SaysNothingAboutWal()
    {
        var (_, message) = DarlingRetention.BuildRunRecordSummary(
            tablesPurged: 33, totalRowsDeleted: 1200, totalChunksDropped: 42, tablesFailed: 0);

        Assert.DoesNotContain("WAL", message, StringComparison.Ordinal);
    }

    /* ---------------- PurgeAsync: paceWal is opt-in ---------------- */

    [Fact]
    public async Task PurgeAsync_WithoutPaceWal_NeverReadsTheCheckpointSettings()
    {
        /* The same DB-free shape as the unexpected-throw test: the null data source is never reached, so a
           pacer built anyway would have to try it and log the fallback. */
        var log = new CapturingTestLogger();

        await DarlingRetention.PurgeAsync(
            postgres: null!, timescaleAvailable: false, log, TestContext.Current.CancellationToken,
            retentionDaysFor: _ => throw new InvalidOperationException("resolver boom"));

        Assert.DoesNotContain(log.Lines, line => line.Contains("WAL pacing", StringComparison.Ordinal));
    }

    [Fact]
    public async Task PurgeAsync_WithPaceWal_ReadsTheCheckpointSettingsOnce()
    {
        var log = new CapturingTestLogger();

        await DarlingRetention.PurgeAsync(
            postgres: null!, timescaleAvailable: false, log, TestContext.Current.CancellationToken,
            retentionDaysFor: _ => throw new InvalidOperationException("resolver boom"),
            paceWal: true);

        var pacing = log.Lines.Where(line => line.Contains("WAL pacing", StringComparison.Ordinal)).ToList();
        Assert.Single(pacing);
        Assert.Contains("4194304", pacing[0], StringComparison.Ordinal);
    }

    /* ---------------- the callers ---------------- */

    [Fact]
    public void TheDailyPurge_AndPurgeNow_BothPaceTheirWal()
    {
        var worker = ReadServiceFile("DarlingWorker.cs");

        var dailyAt = worker.IndexOf("private async Task RunScheduledPurgeAsync(", StringComparison.Ordinal);
        Assert.True(dailyAt >= 0, "RunScheduledPurgeAsync moved");
        Assert.Contains("paceWal: true", CallText(worker, dailyAt), StringComparison.Ordinal);

        /* #4825: purge_now used to run inline on the command loop, which runs nothing else until it returns, so
           it could not be paced without holding pause, resume and test_connect for the length of the pacing.
           It now starts in the daily purge's own slot and answers at once, so it paces its WAL the same way. */
        var nowAt = worker.IndexOf("internal async Task RunPurgeNowBackgroundAsync(", StringComparison.Ordinal);
        Assert.True(nowAt >= 0, "RunPurgeNowBackgroundAsync moved");
        var nowEnd = worker.IndexOf("\n    /// <summary>", nowAt, StringComparison.Ordinal);
        var nowCallAt = worker.IndexOf("DarlingRetention.PurgeAsync(", nowAt, StringComparison.Ordinal);
        Assert.True(nowCallAt >= 0 && nowCallAt < nowEnd, "RunPurgeNowBackgroundAsync no longer calls PurgeAsync itself");
        Assert.Contains("paceWal: true", CallText(worker, nowAt), StringComparison.Ordinal);
    }

    private static string CallText(string source, int methodAt)
    {
        var callAt = source.IndexOf("DarlingRetention.PurgeAsync(", methodAt, StringComparison.Ordinal);
        Assert.True(callAt >= 0, "the PurgeAsync call moved");
        var end = source.IndexOf(");", callAt, StringComparison.Ordinal);
        return source[callAt..(end + 2)];
    }

    private static string ReadServiceFile(string fileName, [System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Service", fileName);
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate);
            }
        }

        throw new FileNotFoundException($"Could not locate {relative}");
    }
}
