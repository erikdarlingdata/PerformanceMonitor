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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// The state machine around the legacy CHECK (#5571 review H1, M1): until a table is promoted, the CHECK is a wall at S,
/// so the hourly convergence step has to keep S ahead of the clock (arm, one promote, re-arm), and the long work (the
/// VALIDATE) runs on a background loop that repeats hourly. The clock is injected, so "S is close" is a clock argument,
/// not a wait; each test runs on its own scratch database.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it, so it cannot race
   live collection. */
public sealed class QueryStoreIntervalConvergenceLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 live tests.";

    private static IReadOnlyList<QueryStoreIntervalPartitions.IntervalTable> OnlyWide { get; } = new[] { Wide };

    /* S as armed at Now: 2026-10-10 00:00. */
    private static DateTime FirstS => QueryStoreIntervalPartitions.ArmBound(Now);

    private static async Task<QueryStoreIntervalPartitions.PartitionState> StateAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await QueryStoreIntervalPartitions.ReadStateAsync(connection, Wide, ct);

    private static DateTime Micro(DateTime value) => value.AddTicks(10);

    [Fact]
    public async Task AnUnvalidatedTable_WithTheClockNearItsS_IsReArmedByTheHourlyPass_AndARowJustPastTheOldSLands()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, Now, logger, ct)).Outcome);
        var oldS = (await StateAsync(connection, ct)).CheckBound;
        Assert.Equal(FirstS, oldS);

        /* The row the old S would refuse: one microsecond past it. */
        var refused = await Assert.ThrowsAsync<PostgresException>(() => InsertAsync(connection, Wide, Micro(FirstS), 1, ct));
        Assert.Equal(PostgresErrorCodes.CheckViolation, refused.SqlState);

        /* A pass well before S leaves the CHECK alone... */
        var early = await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, OnlyWide, FirstS.AddHours(-20), logger, ct);
        Assert.Equal(0, early.Failed);
        Assert.Equal(FirstS, (await StateAsync(connection, ct)).CheckBound);

        /* ...and one with less than twelve hours left re-arms it, before S, with a later S, without a VALIDATE. */
        var near = FirstS.AddHours(-1);
        var pass = await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, OnlyWide, near, logger, ct);
        Assert.Equal(0, pass.Failed);
        var state = await StateAsync(connection, ct);
        Assert.Equal(QueryStoreIntervalPartitions.ArmBound(near), state.CheckBound);
        Assert.True(state.CheckBound > FirstS);
        Assert.False(state.CheckValid);
        Assert.False(state.Promoted);

        await InsertAsync(connection, Wide, Micro(FirstS), 1, ct);
        Assert.Equal(Wide.Legacy, await PartitionOfAsync(connection, Wide, 1, ct));

        /* Another pass at the same clock changes nothing. */
        await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, OnlyWide, near, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.ArmBound(near), (await StateAsync(connection, ct)).CheckBound);
        Assert.False(logger.HasAtLeast(LogLevel.Warning), logger.Dump());
    }

    [Fact]
    public async Task AValidCheck_WhosePromoteIsBlockedForSeveralPasses_IsReArmedBeforeS_AndPromotesOnceTheLockIsGone()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var blocker = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, Now, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.ValidateAsync(connection, Wide, Now, logger, ct)).Outcome);
        Assert.True((await StateAsync(connection, ct)).CheckValid);

        /* A long read of the parent holds ACCESS SHARE on it; the promotion's ACCESS EXCLUSIVE waits 5 s and gives up. ONLY keeps the
           legacy table free, so the re-arm (which locks the legacy table) is not the thing under test. */
        await using (var hold = await blocker.BeginTransactionAsync(ct))
        {
            await ExecAsync(blocker, $"LOCK TABLE ONLY {Wide.Parent} IN ACCESS SHARE MODE", ct);

            foreach (var hoursBeforeS in new[] { 20, 16, 13 })
            {
                var pass = await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, OnlyWide, FirstS.AddHours(-hoursBeforeS), logger, ct);
                Assert.Equal(0, pass.Failed);
                var waiting = await StateAsync(connection, ct);
                Assert.Equal(FirstS, waiting.CheckBound);
                Assert.True(waiting.CheckValid);
                Assert.False(waiting.Promoted);
            }

            var close = FirstS.AddHours(-11);
            var last = await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, OnlyWide, close, logger, ct);
            Assert.Equal(0, last.Failed);
            var reArmed = await StateAsync(connection, ct);
            Assert.False(reArmed.Promoted);
            Assert.Equal(QueryStoreIntervalPartitions.ArmBound(close), reArmed.CheckBound);
            Assert.True(reArmed.CheckBound > FirstS);
            Assert.False(reArmed.CheckValid);   /* the price of re-arming a valid CHECK: a new VALIDATE */

            await InsertAsync(connection, Wide, Micro(FirstS), 2, ct);   /* the old S's first refused instant now lands */
            await hold.RollbackAsync(ct);
        }

        /* The lock is gone: the background step validates the new S, and the next hourly pass promotes. */
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.ValidateAsync(connection, Wide, FirstS.AddHours(-11), logger, ct)).Outcome);
        var promoting = await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, OnlyWide, FirstS.AddHours(-10), logger, ct);
        Assert.Equal(0, promoting.Failed);
        Assert.True((await StateAsync(connection, ct)).Promoted);
        Assert.Equal(Wide.Legacy, await PartitionOfAsync(connection, Wide, 2, ct));
    }

    [Fact]
    public async Task AReArm_BlockedByALongRead_IsRetryLaterNotAnError_AndTheNextPassTriesAgain()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var blocker = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, Now, logger, ct);
        var close = FirstS.AddHours(-6);

        await using (var hold = await blocker.BeginTransactionAsync(ct))
        {
            await ExecAsync(blocker, $"LOCK TABLE {Wide.Legacy} IN ACCESS SHARE MODE", ct);   /* a read of the legacy table */
            var blocked = await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, OnlyWide, close, logger, ct);
            Assert.Equal(0, blocked.Failed);
            Assert.Equal(FirstS, (await StateAsync(connection, ct)).CheckBound);
            await hold.RollbackAsync(ct);
        }

        await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, OnlyWide, close.AddHours(1), logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.ArmBound(close.AddHours(1)), (await StateAsync(connection, ct)).CheckBound);
    }

    [Fact]
    public async Task ALegacyRowPastTheNormalS_MakesTheArmPickALaterS_TheValidatePasses_AndThePromotionSucceeds()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        /* A monitored server whose clock is three days ahead: one row at 10-13 05:00, past the normal S of 10-10. */
        var ahead = new DateTime(2026, 10, 13, 5, 0, 0, DateTimeKind.Unspecified);
        await InsertAsync(connection, Wide, ahead, 1, ct);

        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, Now, logger, ct)).Outcome);
        var laterS = new DateTime(2026, 10, 14, 0, 0, 0, DateTimeKind.Unspecified);
        Assert.Equal(laterS, (await StateAsync(connection, ct)).CheckBound);
        Assert.True(logger.Lines.Any(l => l.Level == LogLevel.Warning && l.Message.Contains("2026-10-13T05:00:00", StringComparison.Ordinal)), logger.Dump());

        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.ValidateAsync(connection, Wide, Now, logger, ct)).Outcome);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.PromoteAsync(connection, Wide, Now, logger, ct)).Outcome);

        /* S is more than three days ahead of Now, so no day partition exists: legacy (below S) and DEFAULT. */
        var names = (await PartitionNamesAsync(connection, Wide, ct)).OrderBy(n => n, StringComparer.Ordinal).ToList();
        Assert.Equal(new[] { Wide.Default, Wide.Legacy }.OrderBy(n => n, StringComparer.Ordinal).ToList(), names);
        Assert.Equal(Wide.Legacy, await PartitionOfAsync(connection, Wide, 1, ct));
        await InsertAsync(connection, Wide, Now, 2, ct);
        Assert.Equal(Wide.Legacy, await PartitionOfAsync(connection, Wide, 2, ct));

        /* Create-ahead makes no day below S, even eight days later when the clock has passed it; the first day made is S. */
        var maintained = await QueryStoreIntervalPartitions.RunMaintenanceAsync(connection, Wide, Now.AddDays(1), logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, maintained.Outcome);
        Assert.DoesNotContain(await PartitionNamesAsync(connection, Wide, ct), n => n.Contains("_p2026", StringComparison.Ordinal));
        await QueryStoreIntervalPartitions.RunMaintenanceAsync(connection, Wide, new DateTime(2026, 10, 12, 1, 0, 0, DateTimeKind.Utc), logger, ct);
        var days = (await PartitionNamesAsync(connection, Wide, ct)).Where(n => n.Contains("_p2026", StringComparison.Ordinal)).OrderBy(n => n, StringComparer.Ordinal).ToList();
        Assert.Equal(new[] { Wide.DayPartition(laterS), Wide.DayPartition(laterS.AddDays(1)) }, days);

        /* The legacy table stays until S is at or below the cutoff (nine days after S), and its row is still there. */
        await QueryStoreIntervalPartitions.RunMaintenanceAsync(connection, Wide, new DateTime(2026, 10, 22, 12, 0, 0, DateTimeKind.Utc), logger, ct);
        Assert.True(await ExistsAsync(connection, Wide.Legacy, ct));
        await QueryStoreIntervalPartitions.RunMaintenanceAsync(connection, Wide, new DateTime(2026, 10, 23, 0, 0, 0, DateTimeKind.Utc), logger, ct);
        Assert.False(await ExistsAsync(connection, Wide.Legacy, ct));
    }

    [Fact]
    public async Task ALegacyIndexThatIsMissing_MakesTheArmUseTheNormalS_AndLogsIt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        await InsertAsync(connection, Wide, new DateTime(2026, 10, 13, 5, 0, 0, DateTimeKind.Unspecified), 1, ct);
        /* An index left INVALID (a failed concurrent build): the catalog flag is the whole fact the arm looks at. */
        await ExecAsync(connection, "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'collect.idx_query_store_interval_wide_first_exec_legacy'::regclass", ct);

        await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, Now, logger, ct);

        /* Never a heap scan to find the maximum: with no usable index the normal S is armed. */
        Assert.Equal(FirstS, (await StateAsync(connection, ct)).CheckBound);
        Assert.True(logger.Lines.Any(l => l.Level == LogLevel.Warning && l.Message.Contains("missing or unusable", StringComparison.Ordinal)), logger.Dump());
    }

    [Fact]
    public async Task AValidateThatFailsWith23514_DropsTheCheckAndLogsTheLegacyMax_AndTheNextPassArmsAgainWithALaterS()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        /* The shape an older arm left: a NOT VALID CHECK at the normal S, over a legacy table that holds a row past it. */
        var ahead = new DateTime(2026, 10, 12, 7, 30, 0, DateTimeKind.Unspecified);
        await InsertAsync(connection, Wide, ahead, 1, ct);
        await ExecAsync(connection, QueryStoreIntervalPartitions.AddCheckSql(Wide, FirstS), ct);

        var validate = await QueryStoreIntervalPartitions.ValidateAsync(connection, Wide, Now, logger, ct);

        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Refused, validate.Outcome);
        Assert.False((await StateAsync(connection, ct)).CheckPresent);
        Assert.True(logger.Lines.Any(l => l.Level == LogLevel.Warning && l.Message.Contains("2026-10-12T07:30:00", StringComparison.Ordinal) && l.Message.Contains("dropped", StringComparison.Ordinal)), logger.Dump());

        var pass = await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, OnlyWide, Now.AddHours(1), logger, ct);
        Assert.Equal(0, pass.Failed);
        var rearmed = await StateAsync(connection, ct);
        Assert.Equal(new DateTime(2026, 10, 13, 0, 0, 0, DateTimeKind.Unspecified), rearmed.CheckBound);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.ValidateAsync(connection, Wide, Now.AddHours(1), logger, ct)).Outcome);
    }

    [Fact]
    public async Task AValidate_NeverStartsWithinItsDeadlinePlusTheReArmWindowOfS()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, Now, logger, ct);

        var tooClose = QueryStoreIntervalPartitions.ValidateAsync(connection, Wide, FirstS.AddHours(-2), logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.ReArmFirst, (await tooClose).Outcome);
        Assert.False((await StateAsync(connection, ct)).CheckValid);

        /* The background step as a whole re-arms first and then validates the new S. */
        var whole = await QueryStoreIntervalPartitions.RunPromotionAsync(
            connection, Wide, FirstS.AddHours(-2), logger, TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(50), ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, whole.Outcome);
        Assert.True((await StateAsync(connection, ct)).Promoted);
    }

    [Fact]
    public async Task AValidateThatLosesItsLock_IsRetriedByTheBackgroundLoop_UntilTheTableIsPromoted()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var blocker = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();
        var clock = DateTime.UtcNow;
        var indexEnsures = 0;

        await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, clock, logger, ct);

        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task loop;
        await using (var hold = await blocker.BeginTransactionAsync(ct))
        {
            await ExecAsync(blocker, $"LOCK TABLE {Wide.Legacy} IN SHARE UPDATE EXCLUSIVE MODE", ct);   /* an anti-wraparound vacuum */

            loop = QueryStoreIntervalPartitions.RunDelayedAsync(
                logger,
                TimeSpan.Zero,
                TimeSpan.FromMilliseconds(200),
                OnlyWide,
                async (table, token) =>
                {
                    await using var own = new NpgsqlConnection(scratch.ConnectionString);
                    await own.OpenAsync(token);
                    return await QueryStoreIntervalPartitions.RunPromotionAsync(
                        own, table, DateTime.UtcNow, logger, TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(50), token);
                },
                (table, token) => Task.FromResult(new QueryStoreIntervalPartitions.StepResult(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, "test")),
                token =>
                {
                    Interlocked.Increment(ref indexEnsures);
                    return Task.CompletedTask;
                },
                stop.Token);

            /* The first VALIDATE gives up on the held lock after 5 s. */
            var deadline = DateTime.UtcNow.AddSeconds(60);
            while (!logger.Lines.Any(l => l.Message.Contains("validating the legacy bound hit a lock timeout", StringComparison.Ordinal)) && DateTime.UtcNow < deadline)
            {
                await Task.Delay(100, ct);
            }

            Assert.True(logger.Lines.Any(l => l.Message.Contains("validating the legacy bound hit a lock timeout", StringComparison.Ordinal)), logger.Dump());
            Assert.False((await StateAsync(connection, ct)).Promoted);
            await hold.RollbackAsync(ct);
        }

        /* Not once per start: the loop's next pass validates and promotes with no restart. */
        var promotedBy = DateTime.UtcNow.AddSeconds(60);
        while (!(await StateAsync(connection, ct)).Promoted && DateTime.UtcNow < promotedBy)
        {
            await Task.Delay(100, ct);
        }

        var promoted = (await StateAsync(connection, ct)).Promoted;
        await stop.CancelAsync();
        await loop;
        Assert.True(promoted, logger.Dump());
        Assert.Equal(1, Volatile.Read(ref indexEnsures));   /* the index ensures ran on the first pass only */
    }

    [Fact]
    public async Task TheAnalyze_UnderAHeldLock_IsRetryLaterNotAnError_AndRunsOnceTheLockIsGone()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var blocker = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        await PromoteAsync(connection, Wide, Now, logger, ct);

        await using (var hold = await blocker.BeginTransactionAsync(ct))
        {
            await ExecAsync(blocker, $"LOCK TABLE ONLY {Wide.Parent} IN SHARE UPDATE EXCLUSIVE MODE", ct);
            var blocked = await QueryStoreIntervalPartitions.AnalyzeAsync(connection, Wide, logger, ct);
            Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.RetryLater, blocked.Outcome);
            await hold.RollbackAsync(ct);
        }

        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.AnalyzeAsync(connection, Wide, logger, ct)).Outcome);
        Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM pg_stat_user_tables WHERE relid = '{Wide.Parent}'::regclass AND last_analyze IS NOT NULL", ct));

        /* A table that is not promoted is not due: the daily check leaves it to its promotion. */
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NotReady, (await QueryStoreIntervalPartitions.AnalyzeIfDueAsync(connection, Latest, Now, logger, ct)).Outcome);
    }

    [Fact]
    public async Task APromotedStoreWhoseLegacyTableIsGone_ReportsNothingToDo_NotNotReady()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        await PromoteAsync(connection, Wide, Now.AddDays(-30), logger, ct);
        await QueryStoreIntervalPartitions.RunMaintenanceAsync(connection, Wide, Now, logger, ct);
        Assert.False(await ExistsAsync(connection, Wide.Legacy, ct));

        /* L2: Phase A on every later start. */
        foreach (var step in new[]
        {
            await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, Now, logger, ct),
            await QueryStoreIntervalPartitions.ValidateAsync(connection, Wide, Now, logger, ct),
            await QueryStoreIntervalPartitions.PromoteAsync(connection, Wide, Now, logger, ct),
            await QueryStoreIntervalPartitions.RunPromotionAsync(connection, Wide, Now, logger, ct),
        })
        {
            Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, step.Outcome);
        }
    }

    [Fact]
    public async Task APromotionThatFindsTheTableAlreadyPromoted_IsNothingToDo_AndAnalyzesNothing()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        var first = await QueryStoreIntervalPartitions.RunPromotionAsync(connection, Wide, Now, logger, TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(50), ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, first.Outcome);
        Assert.True(logger.Lines.Any(l => l.Message.Contains("analyzed the parent", StringComparison.Ordinal)), logger.Dump());

        /* L1: a restart of a promoted store runs no ANALYZE. */
        var before = logger.Lines.Count(l => l.Message.Contains("analyzed the parent", StringComparison.Ordinal));
        var second = await QueryStoreIntervalPartitions.RunPromotionAsync(connection, Wide, Now.AddHours(1), logger, TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(50), ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, second.Outcome);
        Assert.Equal(before, logger.Lines.Count(l => l.Message.Contains("analyzed the parent", StringComparison.Ordinal)));
    }

    private static int FarMaxWarnings(ListLogger logger) =>
        logger.Lines.Count(l => l.Level == LogLevel.Warning && l.Message.Contains("stays unpartitioned until that row is gone", StringComparison.Ordinal));

    [Fact]
    public async Task ALegacyRowDatedMoreThanOneHorizonAhead_IsNotArmedOver_WarnsOncePerMaximum_AndTheArmGoesAheadOnceItIsGone()
    {
        /* #5571 review round 2 M1. A promoted table is never re-armed, so S must not follow a far-future row. */
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();
        QueryStoreIntervalPartitions.ResetFarFutureMaxWarnings();

        /* The normal S is 10-10 and the wide horizon is nine days: a row at 10-19 00:00 would put S at 10-20, one day past the limit. */
        var far = new DateTime(2026, 10, 19, 0, 0, 0, DateTimeKind.Unspecified);
        await InsertAsync(connection, Wide, far, 1, ct);

        var arm = await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, Now, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NotReady, arm.Outcome);
        Assert.False((await StateAsync(connection, ct)).CheckPresent);
        Assert.Equal(1, FarMaxWarnings(logger));
        Assert.True(logger.Lines.Any(l => l.Level == LogLevel.Warning && l.Message.Contains("2026-10-19T00:00:00", StringComparison.Ordinal)), logger.Dump());

        /* Every hourly pass reads the maximum again and still does not arm, and says nothing more while the maximum is the same. */
        for (var hour = 1; hour <= 3; hour++)
        {
            var pass = await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, OnlyWide, Now.AddHours(hour), logger, ct);
            Assert.Equal(0, pass.Failed);
            Assert.False((await StateAsync(connection, ct)).CheckPresent);
        }

        Assert.Equal(1, FarMaxWarnings(logger));

        /* A different maximum is a new fact: it is warned about once. */
        await InsertAsync(connection, Wide, new DateTime(2026, 10, 20, 5, 0, 0, DateTimeKind.Unspecified), 2, ct);
        await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, OnlyWide, Now.AddHours(4), logger, ct);
        Assert.Equal(2, FarMaxWarnings(logger));
        Assert.True(logger.Lines.Any(l => l.Level == LogLevel.Warning && l.Message.Contains("2026-10-20T05:00:00", StringComparison.Ordinal)), logger.Dump());
        await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, OnlyWide, Now.AddHours(5), logger, ct);
        Assert.Equal(2, FarMaxWarnings(logger));
        Assert.False((await StateAsync(connection, ct)).CheckPresent);

        /* The row is deleted: the first pass after that arms, with no restart. */
        await ExecAsync(connection, $"DELETE FROM {Wide.Parent} WHERE first_execution_time >= timestamp '2026-10-19 00:00:00'", ct);
        var armed = await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, OnlyWide, Now.AddHours(6), logger, ct);
        Assert.Equal(0, armed.Failed);
        var state = await StateAsync(connection, ct);
        Assert.True(state.CheckPresent);
        Assert.Equal(FirstS, state.CheckBound);
        Assert.Equal(2, FarMaxWarnings(logger));
    }

    [Fact]
    public async Task ALegacyRowOneMicrosecondInsideTheHorizon_IsArmedOver_AndTheValidatePasses()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();
        QueryStoreIntervalPartitions.ResetFarFutureMaxWarnings();

        /* 10-18 23:59:59.999999 puts S at 10-19, exactly the normal S plus nine days. */
        await InsertAsync(connection, Wide, new DateTime(2026, 10, 19, 0, 0, 0, DateTimeKind.Unspecified).AddTicks(-10), 1, ct);

        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, Now, logger, ct)).Outcome);
        Assert.Equal(new DateTime(2026, 10, 19, 0, 0, 0, DateTimeKind.Unspecified), (await StateAsync(connection, ct)).CheckBound);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.ValidateAsync(connection, Wide, Now, logger, ct)).Outcome);
        Assert.Equal(0, FarMaxWarnings(logger));
    }

    [Fact]
    public async Task APromotionThatFailsForAnyReasonButALock_DoesNotSkipTheReArm_WhenSIsClose()
    {
        /* #5571 review round 2 L3. Left alone, the valid CHECK would refuse every current row at S. */
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, Now, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.ValidateAsync(connection, Wide, Now, logger, ct)).Outcome);

        /* A stray table with the first day partition's name: the promotion's CREATE TABLE ... PARTITION OF fails with 42P07, not 55P03. */
        await ExecAsync(connection, $"CREATE TABLE {Wide.DayPartition(FirstS)} (id integer)", ct);
        var close = FirstS.AddHours(-11);
        await Assert.ThrowsAsync<PostgresException>(() => QueryStoreIntervalPartitions.PromoteAsync(connection, Wide, close, logger, ct));

        var step = await QueryStoreIntervalPartitions.ConvergeUnpromotedAsync(connection, Wide, close, logger, ct);

        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, step.Outcome);
        var state = await StateAsync(connection, ct);
        Assert.False(state.Promoted);
        Assert.Equal(QueryStoreIntervalPartitions.ArmBound(close), state.CheckBound);
        Assert.True(logger.Lines.Any(l => l.Level == LogLevel.Warning && l.Message.Contains("the promotion failed", StringComparison.Ordinal)), logger.Dump());
    }

    [Fact]
    public async Task AValidateThatFailsWith23514_WhenTheLegacyMaxCannotBeRead_IsRetryLater_AndTheLoopValidatesOncePerPass()
    {
        /* #5571 review round 2 L4. With the maximum unknown a second Phase A in the same pass would read the whole heap again for nothing. */
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        await InsertAsync(connection, Wide, new DateTime(2026, 10, 12, 7, 30, 0, DateTimeKind.Unspecified), 1, ct);
        await ExecAsync(connection, "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'collect.idx_query_store_interval_wide_first_exec_legacy'::regclass", ct);
        await ExecAsync(connection, QueryStoreIntervalPartitions.AddCheckSql(Wide, FirstS), ct);

        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var loop = QueryStoreIntervalPartitions.RunDelayedAsync(
            logger,
            TimeSpan.Zero,
            TimeSpan.FromHours(1),
            OnlyWide,
            async (table, token) =>
            {
                await using var own = new NpgsqlConnection(scratch.ConnectionString);
                await own.OpenAsync(token);
                return await QueryStoreIntervalPartitions.RunPromotionAsync(own, table, Now, logger, TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(50), token);
            },
            (table, token) => Task.FromResult(new QueryStoreIntervalPartitions.StepResult(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, "test")),
            token => Task.CompletedTask,
            stop.Token);

        /* The analyze step is the last thing a pass does. */
        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (!logger.Lines.Any(l => l.Message.Contains("analyze step ended", StringComparison.Ordinal)) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(100, ct);
        }

        await stop.CancelAsync();
        await loop;

        Assert.True(logger.Lines.Any(l => l.Message.Contains("promotion step ended RetryLater", StringComparison.Ordinal)), logger.Dump());
        Assert.Equal(1, logger.Lines.Count(l => l.Level == LogLevel.Warning && l.Message.Contains("could not be validated", StringComparison.Ordinal)));
        Assert.False((await StateAsync(connection, ct)).CheckPresent);
    }

    [Fact]
    public async Task ADropThatLosesTheRaceForTheLegacyTable_WaitsOnTheLock_ThenSucceedsWithoutAFailure()
    {
        /* #5571 review round 2 L5. The product lists the expired partitions (reads that finish before the race starts), then its DROP waits on the
           parent's lock. A second connection that already holds a lock on the parent is granted its own DROP ahead of that waiter, commits,
           and the product's DROP finds the table gone: DROP TABLE IF EXISTS looks the name up again after the wait, where a plain DROP TABLE fails
           with 42P01. */
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var other = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        await PromoteAsync(connection, Wide, Now.AddDays(-30), logger, ct);
        Assert.True(await ExistsAsync(connection, Wide.Legacy, ct));

        /* Dropping a partition takes the parent's lock first. ACCESS SHARE on the parent does not block the product's reads, and blocks its DROP. */
        await using var hold = await other.BeginTransactionAsync(ct);
        await ExecAsync(other, $"LOCK TABLE ONLY {Wide.Parent} IN ACCESS SHARE MODE", ct);

        var product = QueryStoreIntervalPartitions.DropExpiredAsync(connection, Wide, Now, logger, ct);

        /* Wait until the product's DROP is queued behind the uncommitted one; the lock wait is 5 s, so this must be well inside it. */
        var waitUntil = DateTime.UtcNow.AddSeconds(3);
        long waiting = 0;
        while (waiting == 0 && DateTime.UtcNow < waitUntil)
        {
            await Task.Delay(50, ct);
            waiting = await CountAsync(other, "SELECT count(*) FROM pg_locks WHERE NOT granted", ct);
        }

        Assert.True(waiting > 0, "the product's DROP never queued behind the first one");
        Assert.False(product.IsCompleted, "the product's DROP did not wait for the lock");

        /* The second drop jumps the queue (it already holds a lock on the parent), so it wins the race. */
        await ExecAsync(other, $"DROP TABLE {Wide.Legacy}", ct);
        await hold.CommitAsync(ct);
        var result = await product;

        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, result.Outcome);
        Assert.False(await ExistsAsync(connection, Wide.Legacy, ct));
        Assert.False(logger.HasAtLeast(LogLevel.Warning), logger.Dump());
    }

    /* The source text of the product file the pins below read. */
    private static class ProductSource
    {
        internal static string Read(string project, string file, [CallerFilePath] string callerFile = "")
        {
            var relative = Path.Combine("Darling", project, file);
            for (var dir = new DirectoryInfo(Path.GetDirectoryName(callerFile)!); dir is not null; dir = dir.Parent)
            {
                var candidate = Path.Combine(dir.FullName, relative);
                if (File.Exists(candidate))
                {
                    return File.ReadAllText(candidate);
                }
            }

            throw new FileNotFoundException(file + " not found above " + callerFile);
        }
    }
}

/// <summary>
/// Source pins for the convergence and background split (#5571 review H1, M1), which the live tests above prove by behavior.
/// These catch the rewrite before it reaches a database.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class QueryStoreIntervalConvergencePinTests
{
    private static string Source([CallerFilePath] string callerFile = "")
    {
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Storage", "QueryStoreIntervalPartitions.cs");
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(callerFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate);
            }
        }

        throw new FileNotFoundException("QueryStoreIntervalPartitions.cs not found above " + callerFile);
    }

    private static string Between(string source, string start, string end)
    {
        var from = source.IndexOf(start, StringComparison.Ordinal);
        Assert.True(from >= 0, start + " moved or was renamed");
        var to = source.IndexOf(end, from, StringComparison.Ordinal);
        Assert.True(to > from, "the end of " + start + " could not be found");
        return source.Substring(from, to - from);
    }

    [Fact]
    public void TheHourlyConvergenceStep_NeverValidatesAndNeverAnalyzes()
    {
        var source = Source();
        var converge = Between(source, "public static async Task<StepResult> ConvergeUnpromotedAsync(", "/// Phase A step 2, run only by the background loop");
        var maintenance = Between(source, "public static async Task<StepResult> RunMaintenanceAsync(", "public static Task RunDelayedAsync(");

        foreach (var body in new[] { converge, maintenance })
        {
            Assert.DoesNotContain("ValidateAsync(", body, StringComparison.Ordinal);
            Assert.DoesNotContain("AnalyzeAsync(", body, StringComparison.Ordinal);
            Assert.DoesNotContain("AnalyzeIfDueAsync(", body, StringComparison.Ordinal);
            Assert.DoesNotContain("Task.Delay(", body, StringComparison.Ordinal);
        }

        /* One attempt, no sleeping: the retry count is zero. */
        Assert.Contains("TimeSpan.Zero, 0, reArmValid: false", converge, StringComparison.Ordinal);
        Assert.Contains("TimeSpan.Zero, 0, reArmValid: true", converge, StringComparison.Ordinal);
    }

    [Fact]
    public void TheBackgroundLoop_RepeatsUntilShutdown_AndOwnsTheValidateAndTheDailyAnalyze()
    {
        var source = Source();
        var loop = Between(source, "internal static async Task RunDelayedAsync(", "private static async Task<StepOutcome?> TryStepAsync(");

        Assert.Contains("for (var pass = 0; ; pass++)", loop, StringComparison.Ordinal);
        Assert.Contains("await Task.Delay(interval, cancellationToken)", loop, StringComparison.Ordinal);
        Assert.Contains("\"analyze\", analyze", loop, StringComparison.Ordinal);
        Assert.Contains("RunPromotionAsync(connection, table, DateTime.UtcNow", source, StringComparison.Ordinal);
        Assert.Contains("AnalyzeIfDueAsync(connection, table, DateTime.UtcNow", source, StringComparison.Ordinal);
    }

    [Fact]
    public void TheAnalyze_HasALockTimeout_AndIsOnlyOnPostgres18AndLater()
    {
        var source = Source();
        var analyze = Between(source, "public static async Task<StepResult> AnalyzeAsync(", "public static async Task<StepResult> AnalyzeIfDueAsync(");

        Assert.Contains("LockTimeoutSql", analyze, StringComparison.Ordinal);
        Assert.Contains("IsLockTimeout(ex)", analyze, StringComparison.Ordinal);
        Assert.Contains("PostgreSqlVersion.Major >= AnalyzeOnlyMajorVersion", analyze, StringComparison.Ordinal);
        Assert.Equal(18, QueryStoreIntervalPartitions.AnalyzeOnlyMajorVersion);
        Assert.Equal("ANALYZE ONLY collect.query_store_interval_wide;", QueryStoreIntervalPartitions.AnalyzeSql(QueryStoreIntervalPartitions.Wide, true));
        Assert.Equal("ANALYZE collect.query_store_interval_wide;", QueryStoreIntervalPartitions.AnalyzeSql(QueryStoreIntervalPartitions.Wide, false));
    }

    [Fact]
    public void TheArmReadsTheLegacyMax_UnderTheLockAndAStatementTimeout_ThroughTheIndex()
    {
        var source = Source();
        var arm = Between(source, "internal static async Task<StepResult> ArmCoreAsync(", "/// The hourly convergence step for a table that is not promoted");
        var read = Between(source, "internal static async Task<DateTime?> ReadLegacyMaxAsync(", "/// Phase A step 1.");

        Assert.True(
            arm.IndexOf("IN ACCESS EXCLUSIVE MODE", StringComparison.Ordinal) < arm.IndexOf("ReadLegacyMaxAsync(", StringComparison.Ordinal),
            "the maximum is read after the lock");
        Assert.Contains("SET LOCAL statement_timeout", read, StringComparison.Ordinal);
        Assert.Contains("LegacyFirstExecIndexUsableSql", read, StringComparison.Ordinal);
        Assert.Contains("ROLLBACK TO SAVEPOINT legacy_max", read, StringComparison.Ordinal);
        Assert.Contains("RESET statement_timeout", read, StringComparison.Ordinal);
        Assert.Equal("collect.idx_query_store_interval_wide_first_exec_legacy", QueryStoreIntervalPartitions.Wide.LegacyFirstExecIndex);
    }

    [Fact]
    public void TheRound2Fixes_AreInTheProductText()
    {
        var source = Source();
        var arm = Between(source, "internal static async Task<StepResult> ArmCoreAsync(", "private static void WarnFarFutureMax(");
        var validate = Between(source, "public static async Task<StepResult> ValidateAsync(", "private static async Task<StepResult> DropViolatedCheckAsync(");
        var converge = Between(source, "public static async Task<StepResult> ConvergeUnpromotedAsync(", "/// Phase A step 2, run only by the background loop");
        var dropViolated = Between(source, "private static async Task<StepResult> DropViolatedCheckAsync(", "Task<StepResult> PromoteAsync(");

        /* M1: a far-future legacy maximum is never armed over; the transaction ends before the CHECK is added. */
        Assert.True(
            arm.IndexOf("IsLegacyMaxBeyondHorizon(", StringComparison.Ordinal) is var m1 && m1 > arm.IndexOf("ReadLegacyMaxAsync(", StringComparison.Ordinal)
            && m1 < arm.IndexOf("AddCheckSql(", StringComparison.Ordinal),
            "the horizon test sits between the maximum read and the ADD CONSTRAINT");
        Assert.Contains("RollbackAsync", arm[arm.IndexOf("IsLegacyMaxBeyondHorizon(", StringComparison.Ordinal)..arm.IndexOf("AddCheckSql(", StringComparison.Ordinal)], StringComparison.Ordinal);

        /* L1: the guard is derived from the deadline and the re-arm window. */
        Assert.Contains("ValidateGuard = TimeSpan.FromSeconds(ValidateTimeoutSeconds) + ReArmWithin;", source, StringComparison.Ordinal);

        /* L2: the server enforces the VALIDATE deadline, set before the VALIDATE runs. */
        var timeout = validate.IndexOf("SET LOCAL statement_timeout = '{ValidateTimeoutSeconds}s'", StringComparison.Ordinal);
        Assert.True(timeout > 0 && timeout < validate.IndexOf("ValidateSql(table)", StringComparison.Ordinal), "statement_timeout is set before the VALIDATE");

        /* L3: a promote that throws does not skip the re-arm that follows it. */
        var promoteAt = converge.IndexOf("PromoteAsync(", StringComparison.Ordinal);
        Assert.True(promoteAt > 0 && converge.IndexOf("catch (Exception ex) when (ex is not OperationCanceledException)", promoteAt, StringComparison.Ordinal) > promoteAt, "the promote is in a try with a catch");
        Assert.True(converge.IndexOf("catch (", promoteAt, StringComparison.Ordinal) < converge.IndexOf("ShouldReArm(", promoteAt, StringComparison.Ordinal));

        /* L4: an unknown maximum is RetryLater, so the loop does not re-run Phase A. */
        Assert.Contains("if (!legacyMax.HasValue)", dropViolated, StringComparison.Ordinal);
        Assert.Contains("StepOutcome.RetryLater", dropViolated, StringComparison.Ordinal);
    }
}
