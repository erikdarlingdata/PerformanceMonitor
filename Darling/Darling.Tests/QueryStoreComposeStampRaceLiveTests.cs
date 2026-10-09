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
}
