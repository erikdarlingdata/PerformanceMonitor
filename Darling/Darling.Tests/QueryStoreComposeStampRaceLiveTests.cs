/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.ComposeStampLiveSupport;
using static Darling.Tests.QueryStoreComposeStampLiveTests;

namespace Darling.Tests;

/// <summary>
/// #5582 review round 1: the rollup builder's concurrency protocol against a real PostgreSQL store (<c>DARLING_TEST_PG</c>). The
/// margin guard (a writing transaction that began inside the trigger's window defers the hour; a read-only one, or one that began
/// after the window, does not), the store clock that bounds the plan, step 1's plain read of <c>late_seq</c> (a writer that bumped
/// the pair and has not committed leaves it stale), the once-per-transaction mark and its savepoint, and two builders on one hour.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it, so it never touches the shared database and cannot race the
   live collection. */
public sealed class QueryStoreComposeStampRaceLiveTests
{
    private static async Task<NpgsqlConnection> OpenAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    /// <summary>Polls until a backend of this database waits on a lock (the advisory lock or a row lock), or gives up after about 10 s.</summary>
    private static async Task<bool> SomeoneWaitsOnALockAsync(NpgsqlConnection watcher, CancellationToken ct)
    {
        for (var i = 0; i < 100; i++)
        {
            if (await CountAsync(watcher, "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'", ct) > 0)
            {
                return true;
            }

            await Task.Delay(100, ct);
        }

        return false;
    }

    private static Task<long> HourRowsAsync(NpgsqlConnection connection, DateTime hour, CancellationToken ct) => CountAsync(connection,
        $"SELECT count(*) FROM collect.query_store_compose_stamp_hours WHERE hour = TIMESTAMP '{At(hour)}'", ct);

    /// <summary>Gives a build connection a lock wait limit, so a build that waits where an old shape would wait forever (a row lock the
    /// writer never releases, an advisory lock nobody has a reason to release) fails with an error instead of blocking the test host.</summary>
    private static async Task BoundLockWaitsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SET lock_timeout = '10s'", connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private const string WritersSql = "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND backend_xid IS NOT NULL AND pid <> pg_backend_pid()";

    [Fact]
    public async Task AnOpenWritingTransactionThatBeganInsideTheMarginWindow_DefersTheHour_AndTheHourBuildsAfterItCommits()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            /* The hour before this one: its trigger margin (H + 2 h) is still ahead, so a row written into it now is unmarked. */
            var hour = hourNow.AddHours(-1);
            await using var writer = await OpenAsync(scratch, ct);
            await using var builder = await source.OpenConnectionAsync(ct);
            await using var open = await writer.BeginTransactionAsync(ct);
            await InsertRowAsync(writer, 1, hour.AddMinutes(10), 9_100_001, ct);
            /* The row is inside the trigger's window, so nothing marked it: no pair of the hour exists, and nothing else is stale. */
            Assert.Equal(0, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE hour = TIMESTAMP '{At(hour)}'", ct));
            Assert.Equal(1, await CountAsync(connection, WritersSql, ct));

            Assert.Null(await QueryStoreComposeStamp.BuildHourAsync(builder, hour, DateTime.UtcNow, ct));
            Assert.Equal(0, await HourRowsAsync(connection, hour, ct));
            Assert.Equal(0, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp WHERE collection_time >= TIMESTAMP '{At(hour)}'", ct));

            await open.CommitAsync(ct);
            Assert.NotNull(await QueryStoreComposeStamp.BuildHourAsync(builder, hour, DateTime.UtcNow, ct));
            Assert.Equal(1, await HourRowsAsync(connection, hour, ct));
            Assert.Equal(0, await MismatchesAsync(connection, hour, hour.AddHours(1), ct));
            Assert.Equal(0, await StalePairsAsync(connection, ct));
            Assert.True(await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp WHERE collection_time = TIMESTAMP '{At(hour.AddMinutes(10))}' AND module_name = 'modX'", ct) > 0, "the row the writer committed is in the rollup");
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task ALongReadOnlyTransaction_AndAWriterThatBeganAfterTheWindow_DoNotDeferTheHour()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct, servers: 4);
        var bodySucceeded = false;
        try
        {
            var young = hourNow.AddHours(-1);
            await using var reader = await OpenAsync(scratch, ct);
            await using var builder = await source.OpenConnectionAsync(ct);
            await using (var readOnly = await reader.BeginTransactionAsync(ct))
            {
                await CountAsync(reader, "SELECT count(*) FROM collect.query_store_interval_wide", ct);
                Assert.Equal(0, await CountAsync(connection, WritersSql, ct));

                /* The young hour, whose window is still open: nothing here has written, so nothing defers it. */
                Assert.NotNull(await QueryStoreComposeStamp.BuildHourAsync(builder, young, DateTime.UtcNow, ct));
                Assert.Equal(1, await HourRowsAsync(connection, young, ct));
                await readOnly.RollbackAsync(ct);
            }

            /* An hour whose window has closed (the youngest the builder does is three hours old) is not deferred by a transaction that is
               writing now: it began after H + 2 h, so every row it writes into that hour is marked. Server 4 has no pair in the hour, so
               the open writer's new pair row does not hold the build's own upsert up. */
            var old = hourNow.AddHours(-3);
            await using var writer = await OpenAsync(scratch, ct);
            await using var open = await writer.BeginTransactionAsync(ct);
            await InsertRowAsync(writer, 4, old.AddMinutes(20), 9_100_002, ct);
            Assert.Equal(1, await CountAsync(connection, WritersSql, ct));
            Assert.NotNull(await QueryStoreComposeStamp.BuildHourAsync(builder, old, DateTime.UtcNow, ct));
            await open.CommitAsync(ct);
            Assert.Equal(Pair(4, old), await StaleListAsync(connection, ct));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task ThePlan_IsBoundedByTheStoresClock_NotTheServicesClock()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct, build: false);
        var bodySucceeded = false;
        try
        {
            /* A service clock two hours ahead of the store would plan the hour that is only an hour old by the store's clock, where the
               trigger's margin is still open. The plan reads the store's clock, so the newest hour it plans is three hours old by it. */
            var plan = await QueryStoreComposeStamp.PlanBuildsAsync(connection, DateTime.UtcNow.AddHours(2), RetentionDays, ct);
            Assert.NotEmpty(plan);
            Assert.Equal(hourNow.AddHours(-3), plan[0].Hour);
            Assert.All(plan, b => Assert.True(b.Hour <= hourNow.AddHours(-3), $"{b.Hour:O} is too young"));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task AWriterThatBumpedThePairBeforeStep1AndHasNotCommitted_LeavesThePairStale_AndTheLookupRoutesTheHourToTheWideTable()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var hour = hourNow.AddHours(-3);
            await using var writer = await OpenAsync(scratch, ct);
            await using var builder = await source.OpenConnectionAsync(ct);
            await BoundLockWaitsAsync(builder, ct);
            await using var open = await writer.BeginTransactionAsync(ct);

            /* The writer bumps server 1's pair (a late row) and holds the bump uncommitted. Step 1, a plain read, sees the committed value;
               the aggregation does not see the row; the build's upsert of that pair waits for the writer's row lock. */
            await InsertRowAsync(writer, 1, hour.AddMinutes(20), 9_100_003, ct);
            Assert.Equal(0, await StalePairsAsync(connection, ct));
            var build = QueryStoreComposeStamp.BuildHourAsync(builder, hour, DateTime.UtcNow, ct);
            Assert.True(await SomeoneWaitsOnALockAsync(connection, ct), "the build should reach its upsert of the pair and wait for the writer");
            Assert.False(build.IsCompleted);
            await open.CommitAsync(ct);
            Assert.NotNull(await build);

            /* The pair ends stale (late_seq is above the built_seq the build read), so the lookup stops the rollup at the hour and the
               panel reads the wide table from it on: the answer is exact even though the rollup lacks the row. */
            Assert.Equal(Pair(1, hour), await StaleListAsync(connection, ct));
            Assert.True(await MismatchesAsync(connection, hour, hour.AddHours(1), ct) > 0, "the rollup does not hold the writer's row");
            Assert.Equal(hour, await StampThroughAsync(connection, hour.AddHours(-2), hourNow.AddHours(-2), ct));

            var tick = await QueryStoreComposeStamp.RunTickAsync(source, DateTime.UtcNow, RetentionDays, NullLogger.Instance, ct);
            Assert.Equal(0, tick.Failed);
            Assert.Equal(1, tick.BuiltStale);
            Assert.Equal(0, await StalePairsAsync(connection, ct));
            Assert.Equal(0, await MismatchesAsync(connection, hour, hour.AddHours(1), ct));
            Assert.Equal(hourNow.AddHours(-2), await StampThroughAsync(connection, hour.AddHours(-2), hourNow.AddHours(-2), ct));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task TheTriggerMarksAPairOncePerTransaction_AndARollbackToASavepointUndoesTheMark()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct, build: false);
        var bodySucceeded = false;
        try
        {
            /* An hour the seed does not cover, so no pair exists yet. Every row is late (before the trigger's threshold). */
            var hour = hourNow.AddHours(-40);
            string LateSeq(int server) => $"SELECT coalesce(max(late_seq), 0) FROM collect.query_store_compose_stamp_built WHERE server_id = {server} AND hour = TIMESTAMP '{At(hour)}'";
            await using var writer = await OpenAsync(scratch, ct);
            await using (var open = await writer.BeginTransactionAsync(ct))
            {
                await InsertRowAsync(writer, 1, hour.AddMinutes(5), 9_200_001, ct);
                await InsertRowAsync(writer, 1, hour.AddMinutes(10), 9_200_002, ct);
                await InsertRowAsync(writer, 1, hour.AddMinutes(15), 9_200_003, ct);
                Assert.Equal(1, await CountAsync(writer, LateSeq(1), ct));

                await open.SaveAsync("sp", ct);
                await InsertRowAsync(writer, 2, hour.AddMinutes(20), 9_200_004, ct);
                await InsertRowAsync(writer, 2, hour.AddMinutes(25), 9_200_005, ct);
                await InsertRowAsync(writer, 1, hour.AddMinutes(30), 9_200_006, ct);
                Assert.Equal(1, await CountAsync(writer, LateSeq(2), ct));
                Assert.Equal(1, await CountAsync(writer, LateSeq(1), ct));
                Assert.Contains(",2:", await TextAsync(writer, "SELECT current_setting('darling.query_store_compose_stamp_marked')", ct), StringComparison.Ordinal);

                /* The rollback takes server 2's pair row and its mark in the setting with it, and leaves server 1's, made before the savepoint. */
                await open.RollbackAsync("sp", ct);
                Assert.Equal(0, await CountAsync(writer, LateSeq(2), ct));
                var marked = await TextAsync(writer, "SELECT current_setting('darling.query_store_compose_stamp_marked')", ct);
                Assert.DoesNotContain(",2:", marked, StringComparison.Ordinal);
                Assert.Contains(",1:", marked, StringComparison.Ordinal);
                Assert.Equal(1, await CountAsync(writer, LateSeq(1), ct));

                /* Server 2 is marked again, not skipped as already marked: the pair is back with its one bump. */
                await InsertRowAsync(writer, 2, hour.AddMinutes(35), 9_200_007, ct);
                Assert.Equal(1, await CountAsync(writer, LateSeq(2), ct));
                await open.CommitAsync(ct);
            }

            Assert.Equal(1, await CountAsync(connection, LateSeq(1), ct));
            Assert.Equal(1, await CountAsync(connection, LateSeq(2), ct));

            /* The setting is transaction-local: the next transaction's first late row bumps the pair again. */
            await InsertRowAsync(connection, 1, hour.AddMinutes(40), 9_200_008, ct);
            Assert.Equal(2, await CountAsync(connection, LateSeq(1), ct));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task TwoBuildersOnTheSameHour_BuildItOnce_TheSecondWaitsForTheFirstAndTheHoursRowsAreNotDoubled()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var hour = hourNow.AddHours(-3);
            await using var first = await source.OpenConnectionAsync(ct);
            await using var second = await source.OpenConnectionAsync(ct);
            await BoundLockWaitsAsync(second, ct);
            Task<long?>? secondBuild = null;

            /* The first build holds the hour's advisory lock and has not committed. The second must wait for it, and then redo the hour
               on top of the first's committed rows: without the lock both delete nothing and both insert, which doubles the hour. */
            await QueryStoreComposeStamp.BuildHourAsync(first, hour, DateTime.UtcNow, ct, async () =>
            {
                secondBuild = QueryStoreComposeStamp.BuildHourAsync(second, hour, DateTime.UtcNow, ct);
                Assert.True(await SomeoneWaitsOnALockAsync(connection, ct), "the second build should wait on the hour's advisory lock");
                Assert.Equal(1, await CountAsync(connection, "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted", ct));
                Assert.False(secondBuild.IsCompleted);
            });
            Assert.NotNull(await secondBuild!);

            var wideRows = await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_interval_wide WHERE collection_time >= TIMESTAMP '{At(hour)}' AND collection_time < TIMESTAMP '{At(hour.AddHours(1))}'", ct);
            Assert.True(wideRows > 0);
            Assert.Equal(wideRows, await CountAsync(connection, $"SELECT sum(wide_rows) FROM collect.query_store_compose_stamp WHERE collection_time >= TIMESTAMP '{At(hour)}' AND collection_time < TIMESTAMP '{At(hour.AddHours(1))}'", ct));
            Assert.Equal(0, await CountAsync(connection, @"SELECT count(*) FROM (SELECT 1 FROM collect.query_store_compose_stamp
                GROUP BY collection_time, server_id, database_name, module_name, query_hash HAVING count(*) > 1) AS d", ct));
            Assert.Equal(0, await MismatchesAsync(connection, hour, hour.AddHours(1), ct));
            Assert.Equal(1, await HourRowsAsync(connection, hour, ct));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    /// <summary>S1: the builder's <c>first_execution_time</c> floor is the hour start less 26 h, so a row whose first execution is 25 h
    /// before its collection time is still built into its hour. Run against the wide table's own read (the mismatch count is the
    /// rollup against a fresh aggregate of the same hour), so a floor that drops the row shows as a mismatch.</summary>
    [Fact]
    public async Task ARowWhoseFirstExecutionIs25HoursBeforeItsCollectionTime_IsBuiltIntoItsHour()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct, build: false);
        var bodySucceeded = false;
        try
        {
            var hour = hourNow.AddHours(-3);
            var collected = hour.AddMinutes(5);
            await ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, runtime_stats_interval_id)
VALUES (TIMESTAMP '{At(collected)}', 1, 'dbOld', 98, 98, 'Regular', TIMESTAMP '{At(collected.AddHours(-25))}', TIMESTAMP '{At(collected)}',
        'modOld', 'hashOld', 7, 1000, 500, 2000, 900, 9300001)", ct);

            await using var builder = await source.OpenConnectionAsync(ct);
            Assert.NotNull(await QueryStoreComposeStamp.BuildHourAsync(builder, hour, DateTime.UtcNow, ct));
            Assert.Equal(1, await CountAsync(connection,
                $"SELECT coalesce(sum(wide_rows), 0) FROM collect.query_store_compose_stamp WHERE collection_time = TIMESTAMP '{At(collected)}' AND database_name = 'dbOld'", ct));
            Assert.Equal(0, await MismatchesAsync(connection, hour, hour.AddHours(1), ct));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }
}
