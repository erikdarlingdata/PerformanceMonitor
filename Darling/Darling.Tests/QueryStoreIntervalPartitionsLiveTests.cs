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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The day-partition background steps (#5571) on a real store that has been through the migration rungs: arm, validate,
/// promote, create-ahead with drain, drop-expired, ANALYZE, and the partitioned index path. Each test runs on its own
/// scratch database, so no test can race another's DDL.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it, so it cannot race
   live collection. */
public sealed class QueryStoreIntervalPartitionsLiveTests
{
    private static readonly QueryStoreIntervalPartitions.IntervalTable Wide = QueryStoreIntervalPartitions.Wide;
    private static readonly QueryStoreIntervalPartitions.IntervalTable Latest = QueryStoreIntervalPartitions.Latest;
    private static readonly TimeSpan NoDelay = TimeSpan.FromMilliseconds(50);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DateTime Today => DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified);

    private static DateTime Now => DateTime.UtcNow;

    private sealed class ListLogger : ILogger
    {
        public List<(LogLevel Level, string Message)> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (Lines)
            {
                Lines.Add((logLevel, formatter(state, exception)));
            }
        }

        public bool Has(LogLevel level, string fragment)
        {
            lock (Lines)
            {
                return Lines.Any(l => l.Level == level && l.Message.Contains(fragment, StringComparison.Ordinal));
            }
        }
    }

    private static async Task<NpgsqlConnection> OpenStoreAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    private static async Task<NpgsqlConnection> OpenSecondAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    private static Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        QueryStoreIntervalWideBrinIndexLiveTests.ExecAsync(connection, sql, ct);

    private static Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        QueryStoreIntervalWideBrinIndexLiveTests.ScalarAsync(connection, sql, ct);

    private static async Task<long> CountAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        Convert.ToInt64(await ScalarAsync(connection, sql, ct), CultureInfo.InvariantCulture);

    private static string Lit(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

    /* The three phases, with no waits between retries. */
    private static Task<QueryStoreIntervalPartitions.StepResult> PromoteWholeAsync(
        NpgsqlConnection connection, QueryStoreIntervalPartitions.IntervalTable table, DateTime utcNow, ILogger logger, CancellationToken ct) =>
        QueryStoreIntervalPartitions.RunPromotionAsync(connection, table, utcNow, logger, NoDelay, NoDelay, ct);

    private static async Task InsertAsync(NpgsqlConnection connection, QueryStoreIntervalPartitions.IntervalTable table, DateTime first, long query, CancellationToken ct)
    {
        var sql = table == Wide
            ? "INSERT INTO collect.query_store_interval_wide (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, query_text, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc) "
              + "VALUES ($1, 1, 'db', $2, $2, 'Regular', $1, $1, 'select 1', 1, 1000, 1, $1)"
            : "INSERT INTO collect.query_store_interval_latest (collection_time, server_id, database_name, query_id, plan_id, first_execution_time, last_execution_time, query_text, execution_count, avg_duration_us, runtime_stats_interval_id) "
              + "VALUES ($1, 1, 'db', $2, $2, $1, $1, 'select 1', 1, 1000, 1)";
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = first });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = query });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> PartitionOfAsync(NpgsqlConnection connection, QueryStoreIntervalPartitions.IntervalTable table, long query, CancellationToken ct) =>
        (string)(await ScalarAsync(connection, $"SELECT 'collect.' || (SELECT c.relname FROM pg_class AS c WHERE c.oid = t.tableoid) FROM {table.Parent} AS t WHERE query_id = {query}", ct))!;

    private static async Task<IReadOnlyList<QueryStoreIntervalPartitions.PartitionInfo>> PartitionsAsync(
        NpgsqlConnection connection, QueryStoreIntervalPartitions.IntervalTable table, CancellationToken ct) =>
        await QueryStoreIntervalPartitions.ReadPartitionsAsync(connection, table, ct);

    // ---------------------------------------------------------------------------------------------------------------

    [Fact]
    public async Task TheSteps_AreOrderedAndIdempotent_ArmValidatePromote()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();
        var now = Now;

        /* Before the rung's shape (or on any non-partitioned parent) every step says not ready and writes nothing. */
        var state = await QueryStoreIntervalPartitions.ReadStateAsync(connection, Wide, ct);
        Assert.True(state.ParentIsPartitioned);
        Assert.True(state.LegacyExists);
        Assert.False(state.Promoted);
        Assert.False(state.CheckPresent);
        Assert.True(state.LegacyUnbounded);

        /* Promote and create-ahead before the arm: not ready, nothing changes. */
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NotReady, (await QueryStoreIntervalPartitions.PromoteAsync(connection, Wide, now, logger, ct)).Outcome);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NotReady, (await QueryStoreIntervalPartitions.CreateAheadAsync(connection, Wide, now, logger, ct)).Outcome);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NotReady, (await QueryStoreIntervalPartitions.DropExpiredAsync(connection, Wide, now, logger, ct)).Outcome);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NotReady, (await QueryStoreIntervalPartitions.ValidateAsync(connection, Wide, now, logger, ct)).Outcome);

        var arm = await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, now, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, arm.Outcome);
        state = await QueryStoreIntervalPartitions.ReadStateAsync(connection, Wide, ct);
        Assert.True(state.CheckPresent);
        Assert.False(state.CheckValid);
        Assert.Equal(Today.AddDays(2), state.CheckBound);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, (await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, now, logger, ct)).Outcome);

        /* A promote before the validate is not ready (an invalid CHECK would make the re-attach scan under lock). */
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NotReady, (await QueryStoreIntervalPartitions.PromoteAsync(connection, Wide, now, logger, ct)).Outcome);

        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.ValidateAsync(connection, Wide, now, logger, ct)).Outcome);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, (await QueryStoreIntervalPartitions.ValidateAsync(connection, Wide, now, logger, ct)).Outcome);
        Assert.True((await QueryStoreIntervalPartitions.ReadStateAsync(connection, Wide, ct)).CheckValid);

        /* The attach of legacy MINVALUE..S scans nothing: the heap is not read again. */
        await ExecAsync(connection, "SELECT pg_stat_force_next_flush()", ct);
        var scansBefore = await CountAsync(connection, $"SELECT COALESCE(pg_stat_get_numscans('{Wide.Legacy}'::regclass), 0)", ct);
        var promote = await QueryStoreIntervalPartitions.PromoteAsync(connection, Wide, now, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, promote.Outcome);
        Assert.Equal(2, promote.Count);
        await ExecAsync(connection, "SELECT pg_stat_force_next_flush()", ct);
        Assert.True(scansBefore > 0, "the validate's heap read is counted, so the comparison below is not vacuous");
        Assert.Equal(scansBefore, await CountAsync(connection, $"SELECT COALESCE(pg_stat_get_numscans('{Wide.Legacy}'::regclass), 0)", ct));

        state = await QueryStoreIntervalPartitions.ReadStateAsync(connection, Wide, ct);
        Assert.True(state.Promoted);
        Assert.True(state.DefaultExists);
        Assert.False(state.LegacyUnbounded);
        Assert.Equal(Today.AddDays(2), state.LegacyUpper);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, (await QueryStoreIntervalPartitions.PromoteAsync(connection, Wide, now, logger, ct)).Outcome);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, (await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, now, logger, ct)).Outcome);

        /* Days S and S+1 exist, each with fillfactor 50, and DEFAULT too; legacy keeps its own. */
        var partitions = await PartitionsAsync(connection, Wide, ct);
        Assert.Equal(
            new[] { Wide.Default, Wide.Legacy, Wide.DayPartition(Today.AddDays(2)), Wide.DayPartition(Today.AddDays(3)) }.OrderBy(n => n, StringComparer.Ordinal),
            partitions.Select(p => p.Name).OrderBy(n => n, StringComparer.Ordinal));
        foreach (var name in new[] { Wide.Default, Wide.DayPartition(Today.AddDays(2)), Wide.DayPartition(Today.AddDays(3)) })
        {
            Assert.Equal("{fillfactor=50}", (await ScalarAsync(connection, $"SELECT reloptions::text FROM pg_class WHERE oid = '{name}'::regclass", ct))!.ToString());
        }

        /* The parent's indexes are valid and every leaf holds one. */
        Assert.Equal(0L, await CountAsync(connection, $"SELECT count(*) FROM pg_index WHERE indrelid = '{Wide.Parent}'::regclass AND NOT indisvalid", ct));

        /* Re-arm is a no-op once promoted, and a re-run of the whole phase finds nothing to do. */
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, (await PromoteWholeAsync(connection, Wide, now, logger, ct)).Outcome);

        await LiveStoreCleanup.RunAsync(scratch.ConnectionString, true, (_, _) => Task.CompletedTask);
    }

    [Fact]
    public async Task AnInvalidCheckThatIsTooNearItsBound_IsReArmed()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        var armedAt = new DateTime(2026, 10, 8, 3, 0, 0, DateTimeKind.Utc);
        await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, armedAt, logger, ct);
        var s0 = (await QueryStoreIntervalPartitions.ReadStateAsync(connection, Wide, ct)).CheckBound!.Value;
        Assert.Equal(new DateTime(2026, 10, 10), s0);

        /* 12 h or more left: untouched. */
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo,
            (await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, s0.AddHours(-12), logger, ct)).Outcome);

        /* Less than 12 h left and still not valid: dropped and added again with a new S. */
        var result = await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, s0.AddHours(-6), logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, result.Outcome);
        var s1 = (await QueryStoreIntervalPartitions.ReadStateAsync(connection, Wide, ct)).CheckBound!.Value;
        Assert.Equal(s0.AddDays(1), s1);
        Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM pg_constraint WHERE conrelid = '{Wide.Legacy}'::regclass AND conname = '{Wide.CheckName}'", ct));

        /* A valid CHECK is never re-armed, however near its bound. */
        await QueryStoreIntervalPartitions.ValidateAsync(connection, Wide, armedAt, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo,
            (await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, s1.AddHours(-1), logger, ct)).Outcome);
    }

    [Fact]
    public async Task AnUpsertDuringValidate_Succeeds()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var validator = await OpenStoreAsync(scratch, ct);
        await using var writer = await OpenSecondAsync(scratch, ct);
        var logger = new ListLogger();
        var now = Now;

        await QueryStoreIntervalPartitions.ArmAsync(validator, Wide, now, logger, ct);
        await InsertAsync(validator, Wide, Today.AddHours(5), 1, ct);

        /* The validate's own statement, left open: its lock is held until the transaction ends, so an upsert from the
           other connection either proceeds (SHARE UPDATE EXCLUSIVE) or fails on the writer's own 2 s lock_timeout. */
        await using (var transaction = await validator.BeginTransactionAsync(ct))
        {
            await using (var validate = new NpgsqlCommand(QueryStoreIntervalPartitions.ValidateSql(Wide), validator, transaction))
            {
                await validate.ExecuteNonQueryAsync(ct);
            }

            Assert.Equal("ShareUpdateExclusiveLock", (string)(await ScalarAsync(writer,
                $"SELECT mode FROM pg_locks WHERE locktype = 'relation' AND relation = '{Wide.Legacy}'::regclass AND mode = 'ShareUpdateExclusiveLock' LIMIT 1", ct))!);

            await ExecAsync(writer, "SET lock_timeout = '2s'", ct);
            var upsert = $@"
INSERT INTO collect.query_store_interval_wide
    (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, query_text, execution_count, avg_duration_us, runtime_stats_interval_id)
VALUES (now() AT TIME ZONE 'UTC', 1, 'db', 1, 1, 'Regular', '{Lit(Today.AddHours(5))}', now() AT TIME ZONE 'UTC', 'select 1', 7, 1000, 1)
ON CONFLICT (server_id, database_name, runtime_stats_interval_id, plan_id, query_id, replica_role, first_execution_time, execution_type_desc)
DO UPDATE SET execution_count = EXCLUDED.execution_count;";
            await ExecAsync(writer, upsert, ct);
            await InsertAsync(writer, Wide, Today.AddHours(6), 2, ct);
            await transaction.CommitAsync(ct);
        }

        Assert.Equal(7L, await CountAsync(validator, $"SELECT execution_count FROM {Wide.Parent} WHERE query_id = 1", ct));
        Assert.Equal(2L, await CountAsync(validator, $"SELECT count(*) FROM {Wide.Parent}", ct));
        Assert.True((await QueryStoreIntervalPartitions.ReadStateAsync(validator, Wide, ct)).CheckValid);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.PromoteAsync(validator, Wide, now, logger, ct)).Outcome);
        Assert.Equal(2L, await CountAsync(validator, $"SELECT count(*) FROM {Wide.Legacy}", ct));
    }

    [Fact]
    public async Task APromotionWithAParentIndexThatLacksALegacyChild_IsRefusedAndRolledBack()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();
        var now = Now;

        await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, now, logger, ct);
        await QueryStoreIntervalPartitions.ValidateAsync(connection, Wide, now, logger, ct);

        /* An ON ONLY parent index: INVALID, no child anywhere. Attaching legacy under the bound would build it on legacy's heap. */
        await ExecAsync(connection, $"CREATE INDEX ix_a2_probe ON ONLY {Wide.Parent} (server_id, plan_id)", ct);
        var refused = await QueryStoreIntervalPartitions.PromoteAsync(connection, Wide, now, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Refused, refused.Outcome);
        Assert.Contains("ix_a2_probe", refused.Detail, StringComparison.Ordinal);
        Assert.True(logger.Has(LogLevel.Warning, "ix_a2_probe"));

        /* State unchanged: legacy still unbounded, no daily partition, no DEFAULT, legacy still attached. */
        var state = await QueryStoreIntervalPartitions.ReadStateAsync(connection, Wide, ct);
        Assert.False(state.Promoted);
        Assert.True(state.LegacyUnbounded);
        Assert.False(state.DefaultExists);
        var partitions = await PartitionsAsync(connection, Wide, ct);
        Assert.Single(partitions);
        Assert.Equal(Wide.Legacy, partitions[0].Name);

        /* Give legacy its child and attach it: the same call now promotes. */
        await ExecAsync(connection, $"CREATE INDEX ix_a2_probe_legacy ON {Wide.Legacy} (server_id, plan_id)", ct);
        await ExecAsync(connection, "ALTER INDEX collect.ix_a2_probe ATTACH PARTITION collect.ix_a2_probe_legacy", ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.PromoteAsync(connection, Wide, now, logger, ct)).Outcome);
        Assert.True((await QueryStoreIntervalPartitions.ReadStateAsync(connection, Wide, ct)).Promoted);
        Assert.Equal(0L, await CountAsync(connection, $"SELECT count(*) FROM pg_index WHERE indrelid = '{Wide.Parent}'::regclass AND NOT indisvalid", ct));
    }

    [Fact]
    public async Task ARowAtOrAfterS_IsRefusedBeforePromotion_AndWritesThroughAfter()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();
        var now = Now;
        var s = Today.AddDays(2);

        await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, now, logger, ct);

        /* Armed and not valid yet: a row one microsecond before S goes in, a row at S is refused with 23514. */
        await InsertAsync(connection, Wide, s.AddTicks(-10), 1, ct);
        var refused = await Assert.ThrowsAsync<PostgresException>(() => InsertAsync(connection, Wide, s, 2, ct));
        Assert.Equal(PostgresErrorCodes.CheckViolation, refused.SqlState);
        await Assert.ThrowsAsync<PostgresException>(() => InsertAsync(connection, Wide, s.AddDays(1), 3, ct));
        Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM {Wide.Parent}", ct));

        /* The latest table, which was not armed, takes the same row (the bound is per table). */
        await InsertAsync(connection, Latest, s, 2, ct);

        await QueryStoreIntervalPartitions.ValidateAsync(connection, Wide, now, logger, ct);
        await QueryStoreIntervalPartitions.PromoteAsync(connection, Wide, now, logger, ct);

        /* After promotion the same insert writes through, into its own day. */
        await InsertAsync(connection, Wide, s, 2, ct);
        Assert.Equal(Wide.DayPartition(s), await PartitionOfAsync(connection, Wide, 2, ct));
        Assert.Equal(Wide.Legacy, await PartitionOfAsync(connection, Wide, 1, ct));
    }

    [Fact]
    public async Task CreateAhead_LeavesDefaultEmpty_AndDrainsADefaultRowIntoTheNewDay()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();
        var now = Now;
        await PromoteWholeAsync(connection, Wide, now, logger, ct);

        /* Nothing to create the day it was promoted; DEFAULT is empty. */
        var again = await QueryStoreIntervalPartitions.CreateAheadAsync(connection, Wide, now, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, again.Outcome);
        Assert.Equal(0L, await CountAsync(connection, $"SELECT count(*) FROM {Wide.Default}", ct));

        /* Two days past the last partition: they land in DEFAULT. */
        var far = Today.AddDays(5);
        await InsertAsync(connection, Wide, far.AddHours(7), 10, ct);
        await InsertAsync(connection, Wide, far.AddDays(1).AddTicks(-10), 11, ct);
        await InsertAsync(connection, Wide, far.AddDays(1), 12, ct);
        Assert.Equal(Wide.Default, await PartitionOfAsync(connection, Wide, 10, ct));
        Assert.Equal(3L, await CountAsync(connection, $"SELECT count(*) FROM {Wide.Default}", ct));

        /* Three days later the clock reaches them: today+3 becomes today+6, so the days of the rows are created, each
           draining its rows from DEFAULT in the same transaction (the attach would otherwise fail on them). */
        var result = await QueryStoreIntervalPartitions.CreateAheadAsync(connection, Wide, now.AddDays(3), logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, result.Outcome);
        Assert.Equal(3, result.Count);
        Assert.Equal(0L, await CountAsync(connection, $"SELECT count(*) FROM {Wide.Default}", ct));
        Assert.Equal(Wide.DayPartition(far), await PartitionOfAsync(connection, Wide, 10, ct));
        Assert.Equal(Wide.DayPartition(far), await PartitionOfAsync(connection, Wide, 11, ct));
        Assert.Equal(Wide.DayPartition(far.AddDays(1)), await PartitionOfAsync(connection, Wide, 12, ct));
        Assert.Equal(3L, await CountAsync(connection, $"SELECT count(*) FROM {Wide.Parent}", ct));
        Assert.True(logger.Has(LogLevel.Warning, "out of DEFAULT"));

        /* No holes: the day partitions run unbroken from S to today + 6. */
        var days = (await PartitionsAsync(connection, Wide, ct)).Where(p => p.Lower.HasValue && p.Upper.HasValue).OrderBy(p => p.Lower).ToList();
        Assert.Equal(Today.AddDays(2), days.First().Lower);
        Assert.Equal(Today.AddDays(6).AddDays(1), days.Last().Upper);
        for (var i = 1; i < days.Count; i++)
        {
            Assert.Equal(days[i - 1].Upper, days[i].Lower);
        }

        /* An outage: three days with no step at all, then one catch-up pass, makes every missed day. */
        var catchUp = await QueryStoreIntervalPartitions.CreateAheadAsync(connection, Wide, now.AddDays(9), logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, catchUp.Outcome);
        Assert.Equal(6, catchUp.Count);
        Assert.Equal(Today.AddDays(13), (await PartitionsAsync(connection, Wide, ct)).Where(p => p.Upper.HasValue).Max(p => p.Upper));
    }

    [Fact]
    public async Task ANewDayPartition_KeepsFillfactorFifty_AndTheUpdatesStayHot()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();
        var now = Now;
        await PromoteWholeAsync(connection, Wide, now, logger, ct);
        await QueryStoreIntervalPartitions.CreateAheadAsync(connection, Wide, now.AddDays(1), logger, ct);

        var created = Wide.DayPartition(Today.AddDays(4));
        Assert.Equal("{fillfactor=50}", (await ScalarAsync(connection, $"SELECT reloptions::text FROM pg_class WHERE oid = '{created}'::regclass", ct))!.ToString());

        /* The writer's upsert never assigns an indexed column, so an update is HOT when the page has room: fillfactor 50 leaves it. */
        await using var transaction = await connection.BeginTransactionAsync(ct);
        for (var i = 0; i < 20; i++)
        {
            await InsertAsync(connection, Wide, Today.AddDays(4).AddHours(1).AddSeconds(i), 100 + i, ct);
        }

        await ExecAsync(connection, $"UPDATE {Wide.Parent} SET execution_count = execution_count + 1, collection_time = collection_time + interval '1 second' WHERE query_id >= 100", ct);
        Assert.Equal(20L, await CountAsync(connection, $"SELECT pg_stat_get_xact_tuples_updated('{created}'::regclass)", ct));
        Assert.Equal(20L, await CountAsync(connection, $"SELECT pg_stat_get_xact_tuples_hot_updated('{created}'::regclass)", ct));
        await transaction.RollbackAsync(ct);
    }

    [Fact]
    public async Task TheLatestTablesTrigger_FiresForARowInADayPartition()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();
        var now = Now;

        /* Promoted ten days ago, then created ahead to today: the day partitions cover the trigger's window
           (a day more than one and fewer than seventeen old). */
        await PromoteWholeAsync(connection, Latest, now.AddDays(-10), logger, ct);
        await QueryStoreIntervalPartitions.CreateAheadAsync(connection, Latest, now, logger, ct);

        var day = Today.AddDays(-4);
        var partition = Latest.DayPartition(day);
        Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM pg_trigger WHERE tgrelid = '{partition}'::regclass AND tgname = 'trg_plan_regression_daily_late' AND tgparentid <> 0", ct));
        Assert.Equal(0L, await CountAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily_built", ct));

        await InsertAsync(connection, Latest, day.AddHours(12), 1, ct);
        Assert.Equal(partition, await PartitionOfAsync(connection, Latest, 1, ct));

        /* The trigger bumped the built row for the row's day and the next one. */
        Assert.Equal(2L, await CountAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily_built WHERE server_id = 1", ct));
        Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM collect.plan_regression_daily_built WHERE server_id = 1 AND day = '{day:yyyy-MM-dd}'", ct));
    }

    [Fact]
    public async Task DropExpired_TakesWholeExpiredDays_KeepsAPartialOne_AndDropsLegacyOnlyOnceBounded()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();
        var now = new DateTime(2026, 10, 8, 14, 55, 30, DateTimeKind.Utc);

        /* Promoted twelve days ago: legacy is bounded at the 28th of September; days from there on exist after create-ahead. */
        var promotedAt = now.AddDays(-12);
        await PromoteWholeAsync(connection, Wide, promotedAt, logger, ct);
        Assert.Equal(new DateTime(2026, 9, 28), (await QueryStoreIntervalPartitions.ReadStateAsync(connection, Wide, ct)).LegacyUpper);
        await QueryStoreIntervalPartitions.CreateAheadAsync(connection, Wide, now, logger, ct);

        /* Rows: legacy (before S), day 9-28 (whole day expired by 10-08), day 9-29 (ends 9-30 00:00; the cutoff is 9-29 14:55 on 10-08, so it is a partial day). */
        await InsertAsync(connection, Wide, new DateTime(2026, 9, 20), 1, ct);
        await InsertAsync(connection, Wide, new DateTime(2026, 9, 28, 23, 59, 59).AddTicks(9999990), 2, ct);
        await InsertAsync(connection, Wide, new DateTime(2026, 9, 29, 0, 0, 0), 3, ct);
        await InsertAsync(connection, Wide, new DateTime(2026, 9, 29, 23, 59, 59).AddTicks(9999990), 4, ct);
        await InsertAsync(connection, Wide, new DateTime(2026, 9, 30, 0, 0, 0), 5, ct);
        Assert.Equal(Wide.Legacy, await PartitionOfAsync(connection, Wide, 1, ct));
        Assert.Equal(Wide.DayPartition(new DateTime(2026, 9, 28)), await PartitionOfAsync(connection, Wide, 2, ct));
        Assert.Equal(Wide.DayPartition(new DateTime(2026, 9, 29)), await PartitionOfAsync(connection, Wide, 3, ct));
        Assert.Equal(Wide.DayPartition(new DateTime(2026, 9, 29)), await PartitionOfAsync(connection, Wide, 4, ct));
        Assert.Equal(Wide.DayPartition(new DateTime(2026, 9, 30)), await PartitionOfAsync(connection, Wide, 5, ct));

        /* The 9-29 day's upper bound is 9-30 00:00; it equals the cutoff when now is 10-09 00:00. One microsecond earlier it is still partial. */
        var edge = new DateTime(2026, 10, 9, 0, 0, 0, DateTimeKind.Utc);
        var early = await QueryStoreIntervalPartitions.DropExpiredAsync(connection, Wide, edge.AddTicks(-10), logger, ct);
        Assert.Equal(2, early.Count);

        /* Dropped: legacy (S 9-28 <= cutoff) and the whole 9-28 day; the straddling 9-29 day stays. */
        var names = (await PartitionsAsync(connection, Wide, ct)).Select(p => p.Name).ToList();
        Assert.DoesNotContain(Wide.Legacy, names);
        Assert.DoesNotContain(Wide.DayPartition(new DateTime(2026, 9, 28)), names);
        Assert.Contains(Wide.DayPartition(new DateTime(2026, 9, 29)), names);
        Assert.Contains(Wide.Default, names);
        Assert.Equal(3L, await CountAsync(connection, $"SELECT count(*) FROM {Wide.Parent}", ct));

        /* Exactly at the cutoff the 9-29 day is whole-expired (its upper bound <= cutoff). */
        var exact = await QueryStoreIntervalPartitions.DropExpiredAsync(connection, Wide, edge, logger, ct);
        Assert.Equal(1, exact.Count);
        Assert.DoesNotContain(Wide.DayPartition(new DateTime(2026, 9, 29)), (await PartitionsAsync(connection, Wide, ct)).Select(p => p.Name));
        Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM {Wide.Parent}", ct));
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, (await QueryStoreIntervalPartitions.DropExpiredAsync(connection, Wide, edge, logger, ct)).Outcome);

        /* A store promoted only five days ago keeps its legacy table (S = 10-05 > cutoff 9-29), until S is old enough. */
        var second = Latest;
        await PromoteWholeAsync(connection, second, now.AddDays(-5), logger, ct);
        await InsertAsync(connection, second, new DateTime(2026, 10, 1), 1, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, (await QueryStoreIntervalPartitions.DropExpiredAsync(connection, second, now, logger, ct)).Outcome);
        Assert.Equal(second.Legacy, await PartitionOfAsync(connection, second, 1, ct));
        var late = await QueryStoreIntervalPartitions.DropExpiredAsync(connection, second, new DateTime(2026, 10, 20, 0, 0, 0, DateTimeKind.Utc), logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, late.Outcome);
        Assert.Equal(1, late.Count);
        Assert.DoesNotContain(second.Legacy, (await PartitionsAsync(connection, second, ct)).Select(p => p.Name));
        Assert.Equal(0L, await CountAsync(connection, $"SELECT count(*) FROM {second.Parent}", ct));
    }

    [Fact]
    public async Task ALockTimeout_IsRetryLaterNotAnError_AndChangesNothing()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var blocker = await OpenSecondAsync(scratch, ct);
        var logger = new ListLogger();
        var now = Now;

        /* Arm: a reader holding the legacy table blocks the ACCESS EXCLUSIVE ADD CONSTRAINT through every retry. */
        await using (var hold = await blocker.BeginTransactionAsync(ct))
        {
            await ExecAsync(blocker, $"LOCK TABLE {Wide.Legacy} IN ACCESS SHARE MODE", ct);
            var armed = await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, now, logger, TimeSpan.FromMilliseconds(100), ct);
            Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.RetryLater, armed.Outcome);
            Assert.False((await QueryStoreIntervalPartitions.ReadStateAsync(connection, Wide, ct)).CheckPresent);
            await hold.RollbackAsync(ct);
        }

        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, now, logger, ct)).Outcome);

        /* Validate: SHARE UPDATE EXCLUSIVE conflicts with itself (an autovacuum, a concurrent index build). */
        await using (var hold = await blocker.BeginTransactionAsync(ct))
        {
            await ExecAsync(blocker, $"LOCK TABLE {Wide.Legacy} IN SHARE UPDATE EXCLUSIVE MODE", ct);
            var validated = await QueryStoreIntervalPartitions.ValidateAsync(connection, Wide, now, logger, ct);
            Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.RetryLater, validated.Outcome);
            Assert.False((await QueryStoreIntervalPartitions.ReadStateAsync(connection, Wide, ct)).CheckValid);
            await hold.RollbackAsync(ct);
        }

        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.ValidateAsync(connection, Wide, now, logger, ct)).Outcome);

        /* Promote: a reader of the parent blocks the ACCESS EXCLUSIVE LOCK; the transaction rolls back untouched. */
        await using (var hold = await blocker.BeginTransactionAsync(ct))
        {
            await ExecAsync(blocker, $"LOCK TABLE {Wide.Parent} IN ACCESS SHARE MODE", ct);
            var promoted = await QueryStoreIntervalPartitions.PromoteAsync(connection, Wide, now, logger, ct);
            Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.RetryLater, promoted.Outcome);
            var state = await QueryStoreIntervalPartitions.ReadStateAsync(connection, Wide, ct);
            Assert.True(state.LegacyUnbounded);
            Assert.False(state.DefaultExists);
            await hold.RollbackAsync(ct);
        }

        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.PromoteAsync(connection, Wide, now, logger, ct)).Outcome);

        /* Drop: a reader of the parent blocks the DROP; the partition stays and the step says retry later. */
        var expiredNow = now.AddDays(40);
        await using (var hold = await blocker.BeginTransactionAsync(ct))
        {
            await ExecAsync(blocker, $"LOCK TABLE {Wide.Parent} IN ACCESS SHARE MODE", ct);
            var dropped = await QueryStoreIntervalPartitions.DropExpiredAsync(connection, Wide, expiredNow, logger, ct);
            Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.RetryLater, dropped.Outcome);
            Assert.Contains(Wide.Legacy, (await PartitionsAsync(connection, Wide, ct)).Select(p => p.Name));
            await hold.RollbackAsync(ct);
        }

        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.DropExpiredAsync(connection, Wide, expiredNow, logger, ct)).Outcome);
    }

    [Fact]
    public async Task AnalyzeIfDue_RunsOnceADay_AndGivesTheParentStatistics()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();
        var now = Now;
        await PromoteWholeAsync(connection, Wide, now, logger, ct);
        for (var i = 0; i < 30; i++)
        {
            await InsertAsync(connection, Wide, Today.AddDays(2).AddMinutes(i), i, ct);
        }

        /* The promotion analyzed the parent a moment ago, and the parent's last_analyze is tracked, so a call now is not due. */
        await ExecAsync(connection, "SELECT pg_stat_force_next_flush()", ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, (await QueryStoreIntervalPartitions.AnalyzeIfDueAsync(connection, Wide, now, logger, ct)).Outcome);

        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, (await QueryStoreIntervalPartitions.AnalyzeIfDueAsync(connection, Wide, now.AddHours(1), logger, ct)).Outcome);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, (await QueryStoreIntervalPartitions.AnalyzeIfDueAsync(connection, Wide, now.AddHours(25), logger, ct)).Outcome);
        /* The parent has column statistics only after an ANALYZE over rows (autovacuum never analyzes a partitioned table). */
        Assert.True(await CountAsync(connection, "SELECT count(*) FROM pg_stats WHERE schemaname = 'collect' AND tablename = 'query_store_interval_wide' AND attname = 'server_id'", ct) > 0);
    }

    [Fact]
    public async Task PhaseC_OnAPartitionedParent_BuildsAndAttachesAChildPerLeaf_AndTheParentEndsValid()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();
        var now = Now;
        var specs = new[] { QueryStoreBackgroundIndexes.WideServerFirstExec, QueryStoreIntervalWideBrinIndex.Spec };

        await PromoteWholeAsync(connection, Wide, now, logger, ct);
        var leaves = (await PartitionsAsync(connection, Wide, ct)).Count;
        Assert.Equal(4, leaves);
        foreach (var spec in specs)
        {
            Assert.True((bool)(await ScalarAsync(connection, $"SELECT to_regclass('{spec.IndexName}') IS NULL", ct))!, spec.IndexName);
        }

        /* Two leftovers: an INVALID child of an interrupted build on one day, and a VALID child nobody attached on another. */
        var invalidDay = Today.AddDays(2);
        var unattachedDay = Today.AddDays(3);
        var invalidChild = $"collect.ix_query_store_interval_wide_server_first_exec_p{invalidDay:yyyyMMdd}";
        var unattachedChild = $"collect.ix_query_store_interval_wide_server_first_exec_p{unattachedDay:yyyyMMdd}";
        await ExecAsync(connection, $"CREATE INDEX {invalidChild.Split('.')[1]} ON {Wide.DayPartition(invalidDay)} (server_id, first_execution_time)", ct);
        await ExecAsync(connection, $"UPDATE pg_index SET indisvalid = false WHERE indexrelid = '{invalidChild}'::regclass", ct);
        await ExecAsync(connection, $"CREATE INDEX {unattachedChild.Split('.')[1]} ON {Wide.DayPartition(unattachedDay)} (server_id, first_execution_time)", ct);
        var unattachedOid = await ScalarAsync(connection, $"SELECT '{unattachedChild}'::regclass::oid", ct);

        foreach (var spec in specs)
        {
            await QueryStoreBackgroundIndexes.EnsureAsync(connection, spec, logger, ct);
            Assert.True((bool)(await ScalarAsync(connection, $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{spec.IndexName}'::regclass", ct))!, spec.IndexName);
            Assert.Equal(leaves, await CountAsync(connection, $"SELECT count(*) FROM pg_inherits WHERE inhparent = '{spec.IndexName}'::regclass", ct));
        }

        Assert.True(logger.Has(LogLevel.Warning, "INVALID"));
        Assert.True((bool)(await ScalarAsync(connection, $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{invalidChild}'::regclass", ct))!);
        Assert.Equal(unattachedOid, await ScalarAsync(connection, $"SELECT '{unattachedChild}'::regclass::oid", ct));
        Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM pg_inherits WHERE inhrelid = '{unattachedChild}'::regclass", ct));

        /* A second run changes nothing. */
        foreach (var spec in specs)
        {
            var oid = await ScalarAsync(connection, $"SELECT '{spec.IndexName}'::regclass::oid", ct);
            await QueryStoreBackgroundIndexes.EnsureAsync(connection, spec, logger, ct);
            Assert.Equal(oid, await ScalarAsync(connection, $"SELECT '{spec.IndexName}'::regclass::oid", ct));
        }

        /* A day made afterwards builds its own child on the empty table, so the parent index never goes INVALID. */
        await QueryStoreIntervalPartitions.CreateAheadAsync(connection, Wide, now.AddDays(2), logger, ct);
        foreach (var spec in specs)
        {
            Assert.True((bool)(await ScalarAsync(connection, $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{spec.IndexName}'::regclass", ct))!, spec.IndexName);
            Assert.Equal(leaves + 2, await CountAsync(connection, $"SELECT count(*) FROM pg_inherits WHERE inhparent = '{spec.IndexName}'::regclass", ct));
        }

        /* The latest table's btree takes the same path. */
        await PromoteWholeAsync(connection, Latest, now, logger, ct);
        await QueryStoreBackgroundIndexes.EnsureAsync(connection, QueryStoreBackgroundIndexes.LatestServerFirstExec, logger, ct);
        Assert.True((bool)(await ScalarAsync(connection, $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{QueryStoreBackgroundIndexes.LatestServerFirstExecIndexName}'::regclass", ct))!);
    }

    [Fact]
    public async Task PhaseC_BeforePromotion_LetsThePromotionPass_BecauseLegacyHoldsTheChildren()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();
        var now = Now;
        var specs = new[] { QueryStoreBackgroundIndexes.WideServerFirstExec, QueryStoreIntervalWideBrinIndex.Spec };

        /* Legacy alone: the ON ONLY parent index turns valid once legacy's child is attached. */
        foreach (var spec in specs)
        {
            await QueryStoreBackgroundIndexes.EnsureAsync(connection, spec, logger, ct);
            Assert.True((bool)(await ScalarAsync(connection, $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{spec.IndexName}'::regclass", ct))!, spec.IndexName);
        }

        /* The re-attach finds each equivalent legacy index and builds nothing, so the pre-check passes and the promotion is done. */
        var promoted = await PromoteWholeAsync(connection, Wide, now, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, promoted.Outcome);
        foreach (var spec in specs)
        {
            Assert.True((bool)(await ScalarAsync(connection, $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{spec.IndexName}'::regclass", ct))!, spec.IndexName);
            Assert.Equal(4L, await CountAsync(connection, $"SELECT count(*) FROM pg_inherits WHERE inhparent = '{spec.IndexName}'::regclass", ct));
        }
    }

    [Fact]
    public async Task ARowRefusedBeforePromotion_TakesTheExistingPendingPath_AndReplaysAfterPromotion()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 live tests.");
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -5571900;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var logger = new ListLogger();
        var now = Now;
        var s = Today.AddDays(2);

        QueryStoreCollector.Row RowAt(long queryId, DateTime first) => new()
        {
            DatabaseName = "qsA",
            QueryId = queryId,
            PlanId = queryId + 10,
            ExecutionTypeDesc = "Regular",
            FirstExecutionTime = first,
            LastExecutionTime = first.AddMinutes(1),
            QueryHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
            QueryPlanHash = "0x" + (queryId + 10).ToString("X8", CultureInfo.InvariantCulture),
            ExecutionCount = 5,
            AvgCpuTimeUs = 100,
            AvgDurationUs = 200,
            IsForcedPlan = false,
            ForceFailureCount = 0,
            RuntimeStatsIntervalId = 7,
        };

        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "qsip-5571", Host = "qsip-5571-host" },
            ConnectionString = "Server=qsip-5571-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsip-5571-host",
            ServerId = serverId,
            EngineEdition = 3,
        };
        var context = new CollectorContext { ServerId = serverId, ServerName = "qsip-5571-host", CollectionTime = DateTime.UtcNow, Deltas = new CollectorDeltaCalculator() };

        /* Armed, not yet validated: a normal row and a row a monitored clock 2 days fast produced, in one batch. */
        await QueryStoreIntervalPartitions.ArmAsync(connection, Wide, now, logger, ct);
        var batch1 = Today.AddHours(1);
        await runner.WriteBackfillBatchAsync(
            QueryStoreCollector.Instance,
            new List<QueryStoreCollector.Row> { RowAt(1, Today.AddMinutes(30)), RowAt(2, s.AddHours(1)) },
            server, batch1, context, ct);

        /* The wide apply faulted (23514) and was parked; the latest table, not armed, took both rows; raw kept both. */
        Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_interval_wide_pending WHERE server_id = {serverId} AND failure LIKE '23514%'", ct));
        Assert.Equal(0L, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_interval_wide WHERE server_id = {serverId}", ct));
        Assert.Equal(2L, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_interval_latest WHERE server_id = {serverId}", ct));
        Assert.Equal(2L, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_stats WHERE server_id = {serverId}", ct));

        await QueryStoreIntervalPartitions.ValidateAsync(connection, Wide, now, logger, ct);
        await QueryStoreIntervalPartitions.PromoteAsync(connection, Wide, now, logger, ct);

        /* The next batch replays the parked one: the pending row clears and the refused row is in its own day. */
        await runner.WriteBackfillBatchAsync(
            QueryStoreCollector.Instance,
            new List<QueryStoreCollector.Row> { RowAt(3, Today.AddMinutes(45)) },
            server, batch1.AddMinutes(30), context, ct);
        Assert.Equal(0L, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_interval_wide_pending WHERE server_id = {serverId}", ct));
        Assert.Equal(3L, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_interval_wide WHERE server_id = {serverId}", ct));
        Assert.Equal(Wide.DayPartition(s), await PartitionOfAsync(connection, Wide, 2, ct));
    }
}
