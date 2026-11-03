/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The hourly ANALYZE of the current day partition (#7353) without a store: which partition holds the clock, and a step
/// that throws never stops the other table, the BRIN summarize, or the next pass.
/// </summary>
public sealed class QueryStoreCurrentPartitionAnalyzeTests
{
    private static DateTime Day(int month, int day) => new(2026, month, day, 0, 0, 0, DateTimeKind.Unspecified);

    private static QueryStoreIntervalPartitions.PartitionInfo DayPartition(string name, DateTime lower) =>
        new(name, false, lower, lower.AddDays(1), false);

    [Fact]
    public void TheCurrentPartition_IsTheDayPartitionWhoseBoundsHoldTheClock_NeverTheLegacyOrTheDefault()
    {
        var legacy = new QueryStoreIntervalPartitions.PartitionInfo("collect.t_legacy", false, null, Day(11, 5), false);
        var unbounded = new QueryStoreIntervalPartitions.PartitionInfo("collect.t_old", false, null, null, true);
        var def = new QueryStoreIntervalPartitions.PartitionInfo("collect.t_default", true, null, null, false);
        var d5 = DayPartition("collect.t_p5", Day(11, 5));
        var d6 = DayPartition("collect.t_p6", Day(11, 6));
        var all = new[] { legacy, unbounded, def, d6, d5 };

        Assert.Equal("collect.t_p5", QueryStoreIntervalPartitions.CurrentPartition(all, Day(11, 5).AddHours(7))?.Name);

        /* The lower bound is inclusive and the upper bound is exclusive. */
        Assert.Equal("collect.t_p5", QueryStoreIntervalPartitions.CurrentPartition(all, Day(11, 5))?.Name);
        Assert.Equal("collect.t_p5", QueryStoreIntervalPartitions.CurrentPartition(all, Day(11, 6).AddTicks(-1))?.Name);
        Assert.Equal("collect.t_p6", QueryStoreIntervalPartitions.CurrentPartition(all, Day(11, 6))?.Name);

        /* A clock in the legacy table's range, or past the last day partition, has no day partition: the legacy table (the 91 GB
           one) and the DEFAULT are never picked. */
        Assert.Null(QueryStoreIntervalPartitions.CurrentPartition(all, Day(11, 4).AddHours(23)));
        Assert.Null(QueryStoreIntervalPartitions.CurrentPartition(all, Day(11, 7)));
        Assert.Null(QueryStoreIntervalPartitions.CurrentPartition(Array.Empty<QueryStoreIntervalPartitions.PartitionInfo>(), Day(11, 5)));
    }

    [Fact]
    public async Task AnalyzeStepThatThrows_IsLoggedAndNeverStopsTheOtherTable_TheSummarize_OrTheNextPass()
    {
        var logger = new QueryStoreIntervalPartitionsLiveTests.ListLogger();
        var nothing = new QueryStoreIntervalPartitions.StepResult(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, "test");
        var analyzeCalls = new List<string>();
        var summarized = 0;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        await QueryStoreIntervalPartitions.RunDelayedAsync(
            logger,
            TimeSpan.Zero,
            TimeSpan.FromMilliseconds(20),
            QueryStoreIntervalPartitions.All,
            (_, _) => Task.FromResult(nothing),
            (_, _) => Task.FromResult(nothing),
            _ => Task.CompletedTask,
            cts.Token,
            (_, _) =>
            {
                Interlocked.Increment(ref summarized);
                return Task.FromResult(nothing);
            },
            (table, _) =>
            {
                lock (analyzeCalls)
                {
                    analyzeCalls.Add(table.Name);
                    if (analyzeCalls.Count(x => x == QueryStoreIntervalPartitions.Latest.Name) >= 2)
                    {
                        cts.Cancel();
                    }
                }

                return table == QueryStoreIntervalPartitions.Wide
                    ? throw new InvalidOperationException("the ANALYZE was refused")
                    : Task.FromResult(new QueryStoreIntervalPartitions.StepResult(QueryStoreIntervalPartitions.StepOutcome.Done, "analyzed"));
            });

        /* Two passes reached both tables, so the first table's throw stopped neither the second table nor the next pass. */
        Assert.True(analyzeCalls.Count(x => x == QueryStoreIntervalPartitions.Wide.Name) >= 2);
        Assert.True(analyzeCalls.Count(x => x == QueryStoreIntervalPartitions.Latest.Name) >= 2);

        /* The first pass still ran the BRIN summarize for both tables after the throw. */
        Assert.True(Volatile.Read(ref summarized) >= 2);
        Assert.True(logger.Has(LogLevel.Warning, "current partition analyze step failed (the ANALYZE was refused)"));
    }
}

/// <summary>
/// The hourly ANALYZE of the current day partition (#7353) on a real store: a partition with no statistics is analyzed, and
/// only that one. Both interval tables are plain declarative partitions on every store, TimescaleDB or not, so one path
/// covers both store kinds.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it, so it cannot race
   live collection. The role the refusal test creates is cluster-wide, so it is dropped in a finally. */
public sealed class QueryStoreCurrentPartitionAnalyzeLiveTests
{
    private static readonly TimeSpan NoDelay = TimeSpan.FromMilliseconds(50);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DateTime Today => DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified);

    private static async Task<NpgsqlConnection> OpenStoreAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    private static Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        QueryStoreIntervalWideBrinIndexLiveTests.ExecAsync(connection, sql, ct);

    private static async Task<long> CountAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        Convert.ToInt64(await QueryStoreIntervalWideBrinIndexLiveTests.ScalarAsync(connection, sql, ct), CultureInfo.InvariantCulture);

    /* The planner's view of one partition: reltuples is -1 (PostgreSQL 14 and later) or 0 until an ANALYZE, and pg_stats has no
       column row for it. */
    private static async Task<(double RelTuples, long ColumnStats)> StatsAsync(NpgsqlConnection connection, string partition, CancellationToken ct)
    {
        var reltuples = Convert.ToDouble(
            await QueryStoreIntervalWideBrinIndexLiveTests.ScalarAsync(connection, $"SELECT reltuples FROM pg_class WHERE oid = '{partition}'::regclass", ct),
            CultureInfo.InvariantCulture);
        var stats = await CountAsync(
            connection, $"SELECT count(*) FROM pg_stats WHERE schemaname = 'collect' AND tablename = '{partition["collect.".Length..]}'", ct);
        return (reltuples, stats);
    }

    /* A promoted table whose two day partitions, today + 2 and today + 3, were dropped and made again, so they are new and have never
       been analyzed on any PostgreSQL version (before 18 the promotion's ANALYZE of the parent samples every leaf), and each holds
       20 rows. Under the 50 rows of autovacuum's analyze threshold, so autovacuum leaves them alone too. */
    private static async Task<(string Current, string Next)> SeedFreshDaysAsync(
        NpgsqlConnection connection, QueryStoreIntervalPartitions.IntervalTable table, ILogger logger, CancellationToken ct)
    {
        await QueryStoreIntervalPartitions.RunPromotionAsync(connection, table, DateTime.UtcNow, logger, NoDelay, NoDelay, ct);
        var first = Today.AddDays(2);
        foreach (var day in new[] { first, first.AddDays(1) })
        {
            await ExecAsync(connection, $"DROP TABLE {table.DayPartition(day)};", ct);
            await ExecAsync(connection, QueryStoreIntervalPartitions.CreateDaySql(table, day), ct);
        }

        for (var i = 0; i < 20; i++)
        {
            await QueryStoreIntervalPartitionsLiveTests.InsertAsync(connection, table, first.AddMinutes(i), i, ct);
            await QueryStoreIntervalPartitionsLiveTests.InsertAsync(connection, table, first.AddDays(1).AddMinutes(i), 100 + i, ct);
        }

        return (table.DayPartition(first), table.DayPartition(first.AddDays(1)));
    }

    [Fact]
    public async Task ACurrentPartitionWithNoStatistics_IsAnalyzed_AndNoOtherPartition_OnBothIntervalTables()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #7353 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new QueryStoreIntervalPartitionsLiveTests.ListLogger();

        foreach (var table in QueryStoreIntervalPartitions.All)
        {
            var (current, next) = await SeedFreshDaysAsync(connection, table, logger, ct);
            var clock = Today.AddDays(2).AddHours(5).AddMinutes(30);

            /* The legacy table holds the rows of today, and the step must leave it alone (a 91 GB table on a real store). */
            for (var i = 0; i < 40; i++)
            {
                await QueryStoreIntervalPartitionsLiveTests.InsertAsync(connection, table, Today.AddMinutes(i), 500 + i, ct);
            }

            var legacyBefore = await StatsAsync(connection, table.Legacy, ct);

            var before = await StatsAsync(connection, current, ct);
            Assert.True(before.RelTuples <= 0, $"{table.Name}: reltuples was {before.RelTuples} before the step");
            Assert.Equal(0L, before.ColumnStats);

            var result = await QueryStoreIntervalPartitions.AnalyzeCurrentPartitionAsync(connection, table, clock, logger, ct);

            Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, result.Outcome);
            Assert.Contains(current, result.Detail, StringComparison.Ordinal);
            var after = await StatsAsync(connection, current, ct);
            Assert.Equal(20d, after.RelTuples);
            Assert.True(after.ColumnStats > 0, $"{table.Name}: the current partition has no column statistics after the step");

            /* Tomorrow's partition and the legacy table were not touched. */
            var tomorrow = await StatsAsync(connection, next, ct);
            Assert.True(tomorrow.RelTuples <= 0 && tomorrow.ColumnStats == 0, $"{table.Name}: the next day's partition was analyzed");
            Assert.Equal(legacyBefore, await StatsAsync(connection, table.Legacy, ct));

            /* A clock inside the legacy table's range holds no day partition, and nothing is analyzed. */
            var none = await QueryStoreIntervalPartitions.AnalyzeCurrentPartitionAsync(connection, table, Today.AddHours(3), logger, ct);
            Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, none.Outcome);
            Assert.Equal(legacyBefore, await StatsAsync(connection, table.Legacy, ct));
        }
    }

    [Fact]
    public async Task ARoleThatMayNotAnalyze_IsSkippedAndLoggedOnce_AndMaintainOnPostgres17Admits()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #7353 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new QueryStoreIntervalPartitionsLiveTests.ListLogger();
        var table = QueryStoreIntervalPartitions.Wide;
        var (current, _) = await SeedFreshDaysAsync(connection, table, logger, ct);
        var clock = Today.AddDays(2).AddHours(5);
        QueryStoreIntervalPartitions.ResetNotPermittedWarnings();

        var role = "qsp_noanalyze_" + Guid.NewGuid().ToString("N")[..12];
        await ExecAsync(connection, $"CREATE ROLE {role} NOLOGIN NOSUPERUSER", ct);
        try
        {
            await ExecAsync(connection, $"GRANT USAGE ON SCHEMA collect TO {role}", ct);
            await ExecAsync(connection, $"SET ROLE {role}", ct);

            /* ANALYZE by this role would skip the partition with a WARNING and succeed, so the step must not call that Done. */
            var first = await QueryStoreIntervalPartitions.AnalyzeCurrentPartitionAsync(connection, table, clock, logger, ct);
            var second = await QueryStoreIntervalPartitions.AnalyzeCurrentPartitionAsync(connection, table, clock, logger, ct);
            Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NotReady, first.Outcome);
            Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NotReady, second.Outcome);
            Assert.Contains("may not ANALYZE", first.Detail, StringComparison.Ordinal);
            Assert.Equal(1, logger.Lines.Count(l => l.Level == LogLevel.Warning && l.Message.Contains("may not ANALYZE", StringComparison.Ordinal)));

            await ExecAsync(connection, "RESET ROLE", ct);
            Assert.Equal(0L, (await StatsAsync(connection, current, ct)).ColumnStats);

            var version = Convert.ToInt32(await QueryStoreIntervalWideBrinIndexLiveTests.ScalarAsync(connection, "SELECT current_setting('server_version_num')::int", ct), CultureInfo.InvariantCulture);
            if (version >= 170000)
            {
                await ExecAsync(connection, $"GRANT MAINTAIN ON {current} TO {role}", ct);
                await ExecAsync(connection, $"SET ROLE {role}", ct);
                var admitted = await QueryStoreIntervalPartitions.AnalyzeCurrentPartitionAsync(connection, table, clock, logger, ct);
                await ExecAsync(connection, "RESET ROLE", ct);
                Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, admitted.Outcome);
                Assert.True((await StatsAsync(connection, current, ct)).ColumnStats > 0);
            }
        }
        finally
        {
            await ExecAsync(connection, "RESET ROLE", CancellationToken.None);
            await ExecAsync(connection, $"DROP OWNED BY {role}", CancellationToken.None);
            await ExecAsync(connection, $"DROP ROLE {role}", CancellationToken.None);
        }
    }

    [Fact]
    public async Task ALockOnThePartition_IsRetryLaterNotAnError_AndTheNextTryAnalyzes()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #7353 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var blocker = new NpgsqlConnection(scratch.ConnectionString);
        await blocker.OpenAsync(ct);
        var logger = new QueryStoreIntervalPartitionsLiveTests.ListLogger();
        var table = QueryStoreIntervalPartitions.Wide;
        var (current, _) = await SeedFreshDaysAsync(connection, table, logger, ct);
        var clock = Today.AddDays(2).AddHours(5);

        /* An anti-wraparound vacuum or a running ANALYZE holds SHARE UPDATE EXCLUSIVE on the partition. */
        await using (var hold = await blocker.BeginTransactionAsync(ct))
        {
            await ExecAsync(blocker, $"LOCK TABLE {current} IN SHARE UPDATE EXCLUSIVE MODE", ct);
            var blocked = await QueryStoreIntervalPartitions.AnalyzeCurrentPartitionAsync(connection, table, clock, logger, ct);
            Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.RetryLater, blocked.Outcome);
            Assert.Equal(0L, (await StatsAsync(connection, current, ct)).ColumnStats);
            await hold.RollbackAsync(ct);
        }

        var retried = await QueryStoreIntervalPartitions.AnalyzeCurrentPartitionAsync(connection, table, clock, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, retried.Outcome);
        Assert.True((await StatsAsync(connection, current, ct)).ColumnStats > 0);
    }
}
