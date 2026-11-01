/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// The 24 h retention of the day-partitioned Query Store interval tables (#5571), through
/// <see cref="DarlingRetention.PurgeIntervalTableAsync"/> on a real store that has been through the migration rungs and
/// the promotion: whole expired days are dropped and a partial day is kept, the legacy table is row-purged by name only while the store
/// is not promoted, and is left for a whole-table drop once it is (and dropped once its bound has expired), a drop that hits a lock timeout gives up and succeeds on the
/// next pass, the DEFAULT partition loses only rows before the cutoff, and a daily partition's row survives a purge of
/// a legacy row that has the same <c>ctid</c>. The step-level drops are in <see cref="QueryStoreIntervalPartitionsLiveTests"/>;
/// these go through the retention entry point the sweep calls.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it, so it cannot race
   live collection. */
public sealed class QueryStoreIntervalRetentionLiveTests
{
    private static Task<DarlingRetention.IntervalPartitionPurge> PurgeAsync(
        NpgsqlDataSource postgres, QueryStoreIntervalPartitions.IntervalTable table, DateTime utcNow, ILogger logger, System.Threading.CancellationToken ct) =>
        DarlingRetention.PurgeIntervalTableAsync(postgres, table, utcNow, logger, null, ct);

    /* Promoted far enough back that the legacy table's bound S and the first day partitions are all older than the cutoff:
       a whole day exists that has fully expired, and the day holding the cutoff is a partial one. */
    private static async Task AssertWholeDayDroppedAndPartialDayKeptAsync(QueryStoreIntervalPartitions.IntervalTable table)
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var logger = new ListLogger();

        await PromoteAsync(connection, table, Now.AddDays(-(table.HorizonDays + 4)), logger, ct);
        await QueryStoreIntervalPartitions.CreateAheadAsync(connection, table, Now, logger, ct);

        var cutoff = Now.AddDays(-table.HorizonDays);
        var wholeDay = QueryStoreIntervalPartitions.DayStart(cutoff).AddDays(-1);   /* ends at the cutoff's midnight: expired */
        var partialDay = QueryStoreIntervalPartitions.DayStart(cutoff);             /* straddles the cutoff: kept */
        Assert.True(await ExistsAsync(connection, table.DayPartition(wholeDay), ct), "the whole-day partition exists before the purge");

        await InsertAsync(connection, table, wholeDay.AddHours(23), 1, ct);
        await InsertAsync(connection, table, partialDay.AddHours(1), 2, ct);        /* before the cutoff, in a partial day */
        await InsertAsync(connection, table, partialDay.AddHours(20), 3, ct);       /* after the cutoff, same day */
        Assert.Equal(table.DayPartition(wholeDay), await PartitionOfAsync(connection, table, 1, ct));
        Assert.Equal(table.DayPartition(partialDay), await PartitionOfAsync(connection, table, 2, ct));

        var result = await PurgeAsync(postgres, table, Now, logger, ct);

        Assert.Equal(0, result.Failed);
        Assert.False(await ExistsAsync(connection, table.DayPartition(wholeDay), ct), "the whole expired day is dropped");
        Assert.True(await ExistsAsync(connection, table.DayPartition(partialDay), ct), "the partial day is kept");
        Assert.Equal(2L, await CountAsync(connection, $"SELECT count(*) FROM {table.Parent}", ct));
        Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM {table.Parent} WHERE query_id = 2", ct));   /* the expired row of a partial day stays until its day ends */
        Assert.False(logger.HasAtLeast(LogLevel.Warning), logger.Dump());
    }

    [Fact]
    public Task RetentionDropsAWholeExpiredDay_AndKeepsAPartialOne_Wide() => AssertWholeDayDroppedAndPartialDayKeptAsync(Wide);

    [Fact]
    public Task RetentionDropsAWholeExpiredDay_AndKeepsAPartialOne_Latest() => AssertWholeDayDroppedAndPartialDayKeptAsync(Latest);

    [Fact]
    public async Task ThePromotedLegacyTable_IsKeptWithItsExpiredRows_ThenDroppedWholeOnceItsBoundHasExpired()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var logger = new ListLogger();

        foreach (var table in QueryStoreIntervalPartitions.All)
        {
            /* Legacy holds one expired row (20 d old) and one live row (6 d old, before S); promoted 5 days ago, so S = 10-05. */
            await InsertAsync(connection, table, Now.AddDays(-20), 1, ct);
            await InsertAsync(connection, table, Now.AddDays(-6), 2, ct);
            await PromoteAsync(connection, table, Now.AddDays(-5), logger, ct);
            Assert.Equal(table.Legacy, await PartitionOfAsync(connection, table, 1, ct));
            Assert.Equal(table.Legacy, await PartitionOfAsync(connection, table, 2, ct));

            /* S (10-05) is after the cutoff (wide 9-29, latest 9-23): the legacy table is not droppable yet. The store is promoted, so
               the whole table is dropped later and its expired row is NOT row-deleted now. */
            var first = await PurgeAsync(postgres, table, Now, logger, ct);
            Assert.Equal(0, first.Failed);
            Assert.Equal(0, first.RowsDeleted);
            Assert.True(await ExistsAsync(connection, table.Legacy, ct), "the legacy table still holds a live row");
            Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM {table.Parent} WHERE query_id = 1", ct));
            Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM {table.Parent} WHERE query_id = 2", ct));

            /* Once S is at or below the cutoff the whole legacy table goes in one DROP. */
            var later = Now.AddDays(table.HorizonDays + 5);
            var drop = await PurgeAsync(postgres, table, later, logger, ct);
            Assert.Equal(0, drop.Failed);
            Assert.False(await ExistsAsync(connection, table.Legacy, ct), "the legacy table is dropped after promotion once S has expired");
            Assert.Equal(0, drop.RowsDeleted);
            Assert.Equal(0L, await CountAsync(connection, $"SELECT count(*) FROM {table.Parent} WHERE query_id IN (1, 2)", ct));

            /* And a purge with no legacy table left skips it without an error or a warning. */
            var again = await PurgeAsync(postgres, table, later, logger, ct);
            Assert.Equal(0, again.Failed);
        }

        Assert.False(logger.HasAtLeast(LogLevel.Warning), logger.Dump());
    }

    /// <summary>
    /// A promoted store leaves the legacy table's expired rows for the whole-table drop. The row delete would pay WAL, index work
    /// and vacuum on a table that is about to be dropped. Here S is after the cutoff, so nothing is droppable yet, and many expired
    /// rows (more than one drain batch is not needed: any row is enough to show the DELETE ran) stay.
    /// </summary>
    [Fact]
    public async Task APromotedStore_WithExpiredLegacyRows_DeletesNoLegacyRows()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var logger = new ListLogger();

        foreach (var table in QueryStoreIntervalPartitions.All)
        {
            for (var i = 1; i <= 5; i++)
            {
                await InsertAsync(connection, table, Now.AddDays(-20).AddMinutes(i), i, ct);   /* expired */
            }

            await PromoteAsync(connection, table, Now.AddDays(-5), logger, ct);               /* S = Now - 5 d + 2 d, after the cutoff */
            var state = await QueryStoreIntervalPartitions.ReadStateAsync(connection, table, ct);
            Assert.True(state.Promoted);
            Assert.True(state.LegacyUpper > Now.AddDays(-table.HorizonDays), "S is after the cutoff, so the drop does not take it yet");

            var result = await PurgeAsync(postgres, table, Now, logger, ct);

            Assert.Equal(0, result.Failed);
            Assert.Equal(0, result.RowsDeleted);
            Assert.True(await ExistsAsync(connection, table.Legacy, ct));
            Assert.Equal(5L, await CountAsync(connection, $"SELECT count(*) FROM {table.Legacy}", ct));
        }

        Assert.False(logger.HasAtLeast(LogLevel.Warning), logger.Dump());
    }

    /// <summary>The other side of the gate: a store that is not promoted holds live data in the legacy table and keeps row-deleting its expired rows.</summary>
    [Fact]
    public async Task AnUnpromotedStore_StillRowDeletesExpiredLegacyRows()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var logger = new ListLogger();

        foreach (var table in QueryStoreIntervalPartitions.All)
        {
            for (var i = 1; i <= 5; i++)
            {
                await InsertAsync(connection, table, Now.AddDays(-20).AddMinutes(i), i, ct);   /* expired */
            }

            await InsertAsync(connection, table, Now.AddDays(-1), 6, ct);                      /* live */
            Assert.False((await QueryStoreIntervalPartitions.ReadStateAsync(connection, table, ct)).Promoted);

            var result = await PurgeAsync(postgres, table, Now, logger, ct);

            Assert.Equal(0, result.Failed);
            Assert.Equal(5, result.RowsDeleted);
            Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM {table.Parent}", ct));
            Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM {table.Parent} WHERE query_id = 6", ct));
        }

        Assert.False(logger.HasAtLeast(LogLevel.Warning), logger.Dump());
    }

    [Fact]
    public async Task TheLegacyTable_IsNeverDroppedBeforePromotion_BecauseItHoldsEveryRow()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var logger = new ListLogger();

        foreach (var table in QueryStoreIntervalPartitions.All)
        {
            await InsertAsync(connection, table, Now.AddDays(-30), 1, ct);
            await InsertAsync(connection, table, Now.AddDays(-1), 2, ct);

            /* Armed and validated but not promoted: the legacy table is still the unbounded partition holding every row. */
            Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.ArmAsync(connection, table, Now, logger, ct)).Outcome);
            Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.ValidateAsync(connection, table, Now, logger, ct)).Outcome);
            var state = await QueryStoreIntervalPartitions.ReadStateAsync(connection, table, ct);
            Assert.False(state.Promoted);
            Assert.True(state.LegacyUnbounded);

            /* A utcNow far enough ahead that even a bounded legacy table would be expired. The drop must still not happen. */
            var farFuture = Now.AddDays(table.HorizonDays + 400);
            var result = await PurgeAsync(postgres, table, farFuture, logger, ct);

            Assert.Equal(0, result.Failed);
            Assert.True(await ExistsAsync(connection, table.Legacy, ct), "the legacy table survives a purge before promotion");
            Assert.Contains(table.Legacy, await PartitionNamesAsync(connection, table, ct));
            Assert.False((await QueryStoreIntervalPartitions.ReadStateAsync(connection, table, ct)).Promoted);
            /* The row-capped purge of the legacy table still removed what the cutoff expired: both rows are older than it. */
            Assert.Equal(2, result.RowsDeleted);
        }

        Assert.False(logger.HasAtLeast(LogLevel.Warning), logger.Dump());
    }

    [Fact]
    public async Task ADropThatHitsALockTimeout_GivesUp_AndSucceedsOnTheNextPass()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var blocker = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var logger = new ListLogger();
        var table = Wide;

        /* Promoted thirteen days ago (S = 9-27): under the 10-day horizon (#5809) the cutoff is 9-28 14:55, so 9-27 is the whole expired day. */
        await PromoteAsync(connection, table, Now.AddDays(-13), logger, ct);
        await QueryStoreIntervalPartitions.CreateAheadAsync(connection, table, Now, logger, ct);
        var wholeDay = new DateTime(2026, 9, 27);
        Assert.True(await ExistsAsync(connection, table.DayPartition(wholeDay), ct));

        /* A reader of the parent blocks the DROP's ACCESS EXCLUSIVE lock for longer than the 5 s lock timeout. */
        await using (var hold = await blocker.BeginTransactionAsync(ct))
        {
            await ExecAsync(blocker, $"LOCK TABLE {table.Parent} IN ACCESS SHARE MODE", ct);
            var blocked = await PurgeAsync(postgres, table, Now, logger, ct);

            Assert.Equal(0, blocked.Failed);   /* a lock timeout is "retry next hour", not a failure */
            Assert.True(await ExistsAsync(connection, table.DayPartition(wholeDay), ct), "nothing was dropped under the lock");
            Assert.True(await ExistsAsync(connection, table.Legacy, ct));
            await hold.RollbackAsync(ct);
        }

        var next = await PurgeAsync(postgres, table, Now, logger, ct);
        Assert.Equal(0, next.Failed);
        Assert.False(await ExistsAsync(connection, table.DayPartition(wholeDay), ct), "the next pass drops it");
        Assert.False(await ExistsAsync(connection, table.Legacy, ct));
        Assert.False(logger.HasAtLeast(LogLevel.Warning), logger.Dump());
    }

    [Fact]
    public async Task TheDefaultDelete_RemovesOnlyRowsBeforeTheCutoff()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var logger = new ListLogger();

        foreach (var table in QueryStoreIntervalPartitions.All)
        {
            await PromoteAsync(connection, table, Now.AddDays(-(table.HorizonDays + 10)), logger, ct);
            await QueryStoreIntervalPartitions.CreateAheadAsync(connection, table, Now, logger, ct);

            /* Drop the expired days and the legacy table first: their range is then a gap, so a row in it lands in DEFAULT. */
            var cutoff = Now.AddDays(-table.HorizonDays);
            Assert.Equal(0, (await PurgeAsync(postgres, table, Now, logger, ct)).Failed);
            Assert.False(await ExistsAsync(connection, table.Legacy, ct), "the legacy table is gone, so its old range is a gap");

            await InsertAsync(connection, table, cutoff.AddDays(-3), 1, ct);     /* before the cutoff: in the gap */
            await InsertAsync(connection, table, cutoff.AddDays(-1), 2, ct);     /* a whole expired day, already dropped: in the gap */
            await InsertAsync(connection, table, Now.AddDays(60), 3, ct);          /* far ahead of every partition */
            Assert.Equal(table.Default, await PartitionOfAsync(connection, table, 3, ct));
            Assert.Equal(table.Default, await PartitionOfAsync(connection, table, 1, ct));
            Assert.Equal(3L, await CountAsync(connection, $"SELECT count(*) FROM {table.Default}", ct));

            var result = await PurgeAsync(postgres, table, Now, logger, ct);

            Assert.Equal(0, result.Failed);
            Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM {table.Parent} WHERE query_id = 3", ct));
            Assert.Equal(0L, await CountAsync(connection, $"SELECT count(*) FROM {table.Default} WHERE first_execution_time < '{cutoff:yyyy-MM-dd HH:mm:ss}'", ct));
            Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM {table.Default}", ct));
        }

        Assert.False(logger.HasAtLeast(LogLevel.Warning), logger.Dump());
    }

    [Fact]
    public async Task ALegacyRowAndADailyPartitionRowThatShareACtid_AreBothLeftAlone_ByThePromotedPurge()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var logger = new ListLogger();

        foreach (var table in QueryStoreIntervalPartitions.All)
        {
            /* Three expired rows in the legacy table, then promote, then three live rows in the first daily partition: each
               heap starts at page 0, so the two sets have the same ctids, (0,1) through (0,3). */
            for (var i = 1; i <= 3; i++)
            {
                await InsertAsync(connection, table, Now.AddDays(-30).AddMinutes(i), i, ct);
            }

            await PromoteAsync(connection, table, Now, logger, ct);
            var s = QueryStoreIntervalPartitions.ArmBound(Now);
            for (var i = 4; i <= 6; i++)
            {
                await InsertAsync(connection, table, s.AddHours(i), i, ct);
            }

            Assert.Equal(table.Legacy, await PartitionOfAsync(connection, table, 1, ct));
            Assert.Equal(table.DayPartition(s), await PartitionOfAsync(connection, table, 4, ct));
            var shared = await CountAsync(connection,
                $"SELECT count(*) FROM {table.Legacy} AS l WHERE EXISTS (SELECT 1 FROM {table.DayPartition(s)} AS d WHERE d.ctid = l.ctid)", ct);
            Assert.Equal(3L, shared);   /* the precondition that makes this test able to fail */

            /* The promoted purge leaves the legacy rows for the whole-table drop, so it deletes nothing, and a ctid delete that
               reached across partitions would show as a missing daily row. */
            var result = await PurgeAsync(postgres, table, Now, logger, ct);

            Assert.Equal(0, result.Failed);
            Assert.Equal(0, result.RowsDeleted);
            Assert.Equal(3L, await CountAsync(connection, $"SELECT count(*) FROM {table.Parent} WHERE query_id <= 3", ct));
            Assert.Equal(3L, await CountAsync(connection, $"SELECT count(*) FROM {table.DayPartition(s)}", ct));
        }

        Assert.False(logger.HasAtLeast(LogLevel.Warning), logger.Dump());
    }

    [Fact]
    public async Task OnePartOfThePurgeFailing_DoesNotStopTheOthers()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var logger = new ListLogger();

        /* The wide table: legacy bounded at S = 9-28 and already expired, with a view on it so its DROP fails with 2BP01 (a real
           error, not a lock timeout). The legacy table is the oldest expired partition, so the drop part fails at once. */
        var wide = Wide;
        await InsertAsync(connection, wide, Now.AddDays(-20), 1, ct);   /* legacy: expired */
        await PromoteAsync(connection, wide, Now.AddDays(-12), logger, ct);
        await QueryStoreIntervalPartitions.CreateAheadAsync(connection, wide, Now, logger, ct);
        await ExecAsync(connection, $"CREATE VIEW collect.zz_blocks_the_drop AS SELECT query_id FROM {wide.Legacy}", ct);
        /* The latest table is healthy and has an expired row in its legacy table. */
        var latest = Latest;
        await PromoteAsync(connection, latest, Now.AddDays(-20), logger, ct);
        await QueryStoreIntervalPartitions.CreateAheadAsync(connection, latest, Now, logger, ct);
        await InsertAsync(connection, latest, Now.AddDays(-40), 9, ct);   /* legacy: expired */

        var failed = await PurgeAsync(postgres, wide, Now, logger, ct);
        var healthy = await PurgeAsync(postgres, latest, Now, logger, ct);

        /* The failed drop is counted and logged, and the same call still finished (the promoted legacy table's rows are left for the drop, so no row is deleted). */
        Assert.Equal(1, failed.Failed);
        Assert.True(logger.Lines.Any(l => l.Level == LogLevel.Warning && l.Message.Contains("could not drop expired partitions", StringComparison.Ordinal)), logger.Dump());
        Assert.Equal(0, failed.RowsDeleted);
        Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM {wide.Parent} WHERE query_id = 1", ct));
        Assert.True(await ExistsAsync(connection, wide.Legacy, ct), "the blocked legacy table is still there");

        /* The other table purged normally. */
        Assert.Equal(0, healthy.Failed);
        Assert.Equal(0L, await CountAsync(connection, $"SELECT count(*) FROM {latest.Parent} WHERE query_id = 9", ct));

        /* Remove the dependency and the next pass drops what was stuck. */
        await ExecAsync(connection, "DROP VIEW collect.zz_blocks_the_drop", ct);
        Assert.Equal(0, (await PurgeAsync(postgres, wide, Now, logger, ct)).Failed);
        Assert.False(await ExistsAsync(connection, wide.Legacy, ct));
    }
}
