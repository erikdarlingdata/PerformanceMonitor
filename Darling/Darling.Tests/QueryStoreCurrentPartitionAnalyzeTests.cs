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
    public void TheNextBackgroundPass_RunsAMinuteAfterMidnight_WhenTheDayChangesBeforeTheHourIsUp()
    {
        var hour = TimeSpan.FromHours(1);
        DateTime At(int h, int m, int s) => new(2026, 11, 5, h, m, s, DateTimeKind.Utc);

        /* A pass that ends at 23:40 waits 21 minutes, not an hour, so the new day's partition is analyzed a minute into the day. */
        Assert.Equal(TimeSpan.FromMinutes(21), QueryStoreIntervalPartitions.NextPassDelay(At(23, 40, 0), hour));
        Assert.Equal(TimeSpan.FromSeconds(30), QueryStoreIntervalPartitions.NextPassDelay(At(0, 0, 30), hour));

        /* Every other time of day keeps the hourly interval, including the instant the grace ends and a pass that ends at 23:00. */
        Assert.Equal(hour, QueryStoreIntervalPartitions.NextPassDelay(At(12, 0, 0), hour));
        Assert.Equal(hour, QueryStoreIntervalPartitions.NextPassDelay(At(0, 1, 0), hour));
        Assert.Equal(hour, QueryStoreIntervalPartitions.NextPassDelay(At(0, 30, 0), hour));
        Assert.Equal(TimeSpan.FromMinutes(60), QueryStoreIntervalPartitions.NextPassDelay(At(23, 0, 0), hour));

        /* A short test interval is never lengthened. */
        Assert.Equal(TimeSpan.FromMilliseconds(20), QueryStoreIntervalPartitions.NextPassDelay(At(23, 59, 59), TimeSpan.FromMilliseconds(20)));
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

    /* Each test reads the clock once and passes that one value to every step, so a run that crosses 00:00 UTC cannot split its seeded days. */
    private static DateTime DayOf(DateTime now) => DateTime.SpecifyKind(now.Date, DateTimeKind.Unspecified);

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
        NpgsqlConnection connection, QueryStoreIntervalPartitions.IntervalTable table, DateTime now, ILogger logger, CancellationToken ct)
    {
        await QueryStoreIntervalPartitions.RunPromotionAsync(connection, table, now, logger, NoDelay, NoDelay, ct);
        var first = DayOf(now).AddDays(2);
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
        var now = DateTime.UtcNow;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new QueryStoreIntervalPartitionsLiveTests.ListLogger();

        foreach (var table in QueryStoreIntervalPartitions.All)
        {
            var (current, next) = await SeedFreshDaysAsync(connection, table, now, logger, ct);
            var clock = DayOf(now).AddDays(2).AddHours(5).AddMinutes(30);

            /* The legacy table holds the rows of today, and the step must leave it alone (a 91 GB table on a real store). */
            for (var i = 0; i < 40; i++)
            {
                await QueryStoreIntervalPartitionsLiveTests.InsertAsync(connection, table, DayOf(now).AddMinutes(i), 500 + i, ct);
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
            var none = await QueryStoreIntervalPartitions.AnalyzeCurrentPartitionAsync(connection, table, DayOf(now).AddHours(3), logger, ct);
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
        var now = DateTime.UtcNow;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new QueryStoreIntervalPartitionsLiveTests.ListLogger();
        var table = QueryStoreIntervalPartitions.Wide;
        var (current, _) = await SeedFreshDaysAsync(connection, table, now, logger, ct);
        var clock = DayOf(now).AddDays(2).AddHours(5);
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
        var now = DateTime.UtcNow;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var blocker = new NpgsqlConnection(scratch.ConnectionString);
        await blocker.OpenAsync(ct);
        var logger = new QueryStoreIntervalPartitionsLiveTests.ListLogger();
        var table = QueryStoreIntervalPartitions.Wide;
        var (current, _) = await SeedFreshDaysAsync(connection, table, now, logger, ct);
        var clock = DayOf(now).AddDays(2).AddHours(5);

        /* A lock held from a normal backend. The wait is bounded by half of deadlock_timeout (#5594): with deadlock_timeout at 4 s the
           lock timeout is 2 s, so the step gives up before the deadlock check could cancel an autovacuum worker holding the lock, where
           the old fixed 5 s lock timeout waited past it. */
        await ExecAsync(connection, "SET deadlock_timeout = '4s'", ct);
        await using (var hold = await blocker.BeginTransactionAsync(ct))
        {
            await ExecAsync(blocker, $"LOCK TABLE {current} IN SHARE UPDATE EXCLUSIVE MODE", ct);
            var waited = System.Diagnostics.Stopwatch.StartNew();
            var blocked = await QueryStoreIntervalPartitions.AnalyzeCurrentPartitionAsync(connection, table, clock, logger, ct);
            waited.Stop();
            Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.RetryLater, blocked.Outcome);
            Assert.True(waited.Elapsed < TimeSpan.FromSeconds(4), $"the step waited {waited.Elapsed.TotalSeconds:F1} s for a lock, past deadlock_timeout (4 s)");
            Assert.Equal(0L, (await StatsAsync(connection, current, ct)).ColumnStats);
            await hold.RollbackAsync(ct);
        }

        await ExecAsync(connection, "RESET deadlock_timeout", ct);
        var retried = await QueryStoreIntervalPartitions.AnalyzeCurrentPartitionAsync(connection, table, clock, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, retried.Outcome);
        Assert.True((await StatsAsync(connection, current, ct)).ColumnStats > 0);
    }

    /// <summary>
    /// A manual VACUUM in a second session, throttled to one page per 100 ms so it is still running when the step asks. It shows in
    /// pg_stat_progress_vacuum as an autovacuum worker would. A real autovacuum cannot be started on demand without changing the
    /// cluster's autovacuum settings, which every other test shares, so the skip decision is exercised with this look-alike: the step
    /// reads the same progress row, by relation, for either. Disposing cancels it and waits for it to end.
    /// </summary>
    private sealed class SlowVacuum : IAsyncDisposable
    {
        private readonly NpgsqlConnection _connection;
        private readonly CancellationTokenSource _cancel = new();
        private readonly Task _run;

        private SlowVacuum(NpgsqlConnection connection, string table)
        {
            _connection = connection;
            _run = Task.Run(
                async () =>
                {
                    try
                    {
                        await QueryStoreIntervalWideBrinIndexLiveTests.ExecAsync(connection, "SET vacuum_cost_delay = 100; SET vacuum_cost_limit = 1;", CancellationToken.None);
                        await using var vacuum = new NpgsqlCommand($"VACUUM {table}", connection) { CommandTimeout = 300 };
                        await vacuum.ExecuteNonQueryAsync(_cancel.Token);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    catch (PostgresException)
                    {
                    }
                });
        }

        public static async Task<SlowVacuum> StartAsync(ScratchPostgres scratch, NpgsqlConnection probe, string table, CancellationToken ct)
        {
            var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            var vacuum = new SlowVacuum(connection, table);
            for (var attempt = 0; attempt < 300; attempt++)
            {
                if (await CountAsync(probe, $"SELECT count(*) FROM pg_stat_progress_vacuum WHERE relid = '{table}'::regclass AND pid = {connection.ProcessID}", ct) > 0)
                {
                    return vacuum;
                }

                await Task.Delay(100, ct);
            }

            await vacuum.DisposeAsync();
            throw new InvalidOperationException($"the throttled VACUUM of {table} never showed in pg_stat_progress_vacuum");
        }

        public async ValueTask DisposeAsync()
        {
            await _cancel.CancelAsync();
            await _run;
            await _connection.DisposeAsync();
        }
    }

    [Fact]
    public async Task APartitionBeingVacuumed_IsSkippedAsRetryLater_WithoutAskingForALock_AndTheNextTryAnalyzes()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #7353 live tests.");
        var ct = TestContext.Current.CancellationToken;
        var now = DateTime.UtcNow;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new QueryStoreIntervalPartitionsLiveTests.ListLogger();
        var table = QueryStoreIntervalPartitions.Wide;
        var (current, _) = await SeedFreshDaysAsync(connection, table, now, logger, ct);
        var clock = DayOf(now).AddDays(2).AddHours(5);

        /* Enough pages that the throttled VACUUM is still running when the step asks. */
        await ExecAsync(connection, $"ALTER TABLE {current} SET (autovacuum_enabled = false)", ct);
        await ExecAsync(
            connection,
            $"INSERT INTO {current} (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, "
            + "query_text, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc) "
            + $"SELECT t, 1, 'db', g, g, 'Regular', t, t, repeat('select 1 ', 20), 1, 1000, 1, t FROM (SELECT g, timestamp '{DayOf(now).AddDays(2):yyyy-MM-dd} 01:00:00' + g * interval '1 second' AS t FROM generate_series(1000, 21000) AS g) AS s",
            ct);

        QueryStoreIntervalPartitions.StepResult step;
        await using (var vacuum = await SlowVacuum.StartAsync(scratch, connection, current, ct))
        {
            step = await QueryStoreIntervalPartitions.AnalyzeCurrentPartitionAsync(connection, table, clock, logger, ct);
        }

        /* Skipped without a lock request (a request would have ended as a lock timeout, not as "being vacuumed"), and nothing was analyzed. */
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.RetryLater, step.Outcome);
        Assert.Contains("being vacuumed", step.Detail, StringComparison.Ordinal);
        Assert.Equal(0L, (await StatsAsync(connection, current, ct)).ColumnStats);

        var retried = await QueryStoreIntervalPartitions.AnalyzeCurrentPartitionAsync(connection, table, clock, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, retried.Outcome);
        Assert.True((await StatsAsync(connection, current, ct)).ColumnStats > 0);
    }

    private static readonly DateTime PanelStart = new(2026, 10, 1, 0, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task TheGuardCountsTheFleetTailAtHalfOnlyWhileTheCurrentPartitionHasFreshStatistics_OnARealStore()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #7353 live tests.");
        var ct = TestContext.Current.CancellationToken;
        var now = DateTime.UtcNow;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var logger = new QueryStoreIntervalPartitionsLiveTests.ListLogger();
        var table = QueryStoreIntervalPartitions.Wide;
        await SeedFreshDaysAsync(connection, table, now, logger, ct);
        var clock = DayOf(now).AddDays(2).AddHours(5);

        /* The 210-server load-test figures: a 24 hour panel whose newest 6 hours are the raw tail, the other 18 served by the fleet
           table. At the 0.5 tail weight it is 6.5 M to 6.8 M rows, under the one-scan limit; at 1.0 it is over. */
        var estimate = new QueryStoreWideReadGuard.Estimate(42_720_000, 210);
        var end = PanelStart.AddDays(1);
        var through = end.AddHours(-6);
        string? RefusalFor(QueryStoreWideReadGuard.FleetServing served) =>
            QueryStoreWideReadGuard.Refusal(
                estimate, PanelStart, end, QueryStoreWideReadGuard.LimitFor(false), through, served with { Hours = 18, Rows = 4_333_333 });

        /* The partition holding the clock has never been analyzed: the weight falls back to 1.0 and the panel is refused. */
        var unanalyzed = await QueryStoreWideReadGuard.FleetServingAsync(source, PanelStart, through, null, 30, ct, clock);
        Assert.False(unanalyzed!.Value.TailAnalyzed);
        Assert.NotNull(RefusalFor(unanalyzed.Value));

        /* The hourly ANALYZE ran: the same panel is admitted. */
        var analyze = await QueryStoreIntervalPartitions.AnalyzeCurrentPartitionAsync(connection, table, clock, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, analyze.Outcome);
        var analyzed = await QueryStoreWideReadGuard.FleetServingAsync(source, PanelStart, through, null, 30, ct, clock);
        Assert.True(analyzed!.Value.TailAnalyzed);
        Assert.Null(RefusalFor(analyzed.Value));

        /* A clock in the next day's partition, which was never analyzed (a new day before the first pass), is stale again. */
        var tomorrow = await QueryStoreWideReadGuard.FleetServingAsync(source, PanelStart, through, null, 30, ct, clock.AddDays(1));
        Assert.False(tomorrow!.Value.TailAnalyzed);
        Assert.NotNull(RefusalFor(tomorrow.Value));

        /* A clock that no day partition holds has no statistics to rely on. */
        var none = await QueryStoreWideReadGuard.FleetServingAsync(source, PanelStart, through, null, 30, ct, DayOf(now).AddYears(-1));
        Assert.False(none!.Value.TailAnalyzed);
    }
}
