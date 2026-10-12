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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.ComposeStampLiveSupport;
using static Darling.Tests.QueryStoreComposeStampLiveTests;

namespace Darling.Tests;

/// <summary>
/// #5582 review round 2 (M1 and L1): the margin guard counts only the writers of the wide table (a granted <c>RowExclusiveLock</c> on it
/// or on a partition), a writer whose start time is not readable counts as open, the plan leaves out the hours such a writer would defer
/// so they do not take the tick's build slots, and a wait that repeats is logged as a Warning that names the writer. A writer blocks a
/// plannable hour only once it has been open for over an hour, so the plan tests pass a store clock of their own
/// (<see cref="QueryStoreComposeStamp.TickOptions.StoreNow"/>) instead of waiting.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it, so it never touches the shared database and cannot race the
   live collection. */
public sealed class QueryStoreComposeStampWriterGuardLiveTests
{
    private const string WriterApplication = "rollup-writer-5582";

    private static async Task<NpgsqlConnection> OpenWriterAsync(ScratchPostgres scratch, CancellationToken ct, string? options = null)
    {
        var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { ApplicationName = WriterApplication };
        if (options is not null)
        {
            builder.Options = options;
        }

        var connection = new NpgsqlConnection(builder.ConnectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    private static Task<long> HoursBuiltAsync(NpgsqlConnection connection, DateTime hour, CancellationToken ct) => CountAsync(connection,
        $"SELECT count(*) FROM collect.query_store_compose_stamp_hours WHERE hour = TIMESTAMP '{At(hour)}'", ct);

    [Fact]
    public async Task AnUnrelatedLongWriter_ATempTableAndAWriteToAnotherTable_NoLongerDefersTheHour()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct, build: false);
        var bodySucceeded = false;
        try
        {
            /* The hour before this one: its trigger margin (H + 2 h) is still ahead, so an open writer of the wide table would defer it. */
            var hour = hourNow.AddHours(-1);
            await using var writer = await OpenWriterAsync(scratch, ct);
            await using var builder = await source.OpenConnectionAsync(ct);
            await using var open = await writer.BeginTransactionAsync(ct);
            await ExecAsync(writer, "CREATE TEMP TABLE scratch_5582 (i int); INSERT INTO scratch_5582 VALUES (1); UPDATE collect.servers SET is_enabled = is_enabled WHERE server_id = 1", ct);
            /* It holds an xid, the whole of what the round-1 guard asked for. */
            Assert.Equal(1, await CountAsync(connection, "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND backend_xid IS NOT NULL AND pid <> pg_backend_pid()", ct));

            Assert.NotNull(await QueryStoreComposeStamp.BuildHourAsync(builder, hour, DateTime.UtcNow, ct));
            Assert.Equal(1, await HoursBuiltAsync(connection, hour, ct));
            await open.RollbackAsync(ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task AWriteStraightIntoAPartition_AndAWriterWhoseStartTimeIsNotReadable_StillDeferTheHour()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct, build: false);
        var bodySucceeded = false;
        try
        {
            var hour = hourNow.AddHours(-1);
            await using var builder = await source.OpenConnectionAsync(ct);

            /* A direct write into the leaf that holds the hour's seed rows locks the leaf, not the parent. */
            var leaf = await TextAsync(connection, $"SELECT tableoid::regclass::text FROM collect.query_store_interval_wide WHERE collection_time = TIMESTAMP '{At(hour.AddMinutes(5))}' LIMIT 1", ct);
            Assert.False(string.IsNullOrEmpty(leaf));
            await using (var leafWriter = await OpenWriterAsync(scratch, ct))
            await using (var open = await leafWriter.BeginTransactionAsync(ct))
            {
                await InsertRowAsync(leafWriter, 1, hour.AddMinutes(10), 9_200_001, ct, leaf);
                Assert.Equal(0, await CountAsync(connection, $"SELECT count(*) FROM pg_locks WHERE locktype = 'relation' AND mode = 'RowExclusiveLock' AND pid = {leafWriter.ProcessID} AND relation = 'collect.query_store_interval_wide'::regclass", ct));
                Assert.Null(await QueryStoreComposeStamp.BuildHourAsync(builder, hour, DateTime.UtcNow, ct));
                await open.CommitAsync(ct);
            }

            /* track_activities off at session start: the session's xact_start reads NULL, and the guard cannot show it began late enough. */
            await using (var blind = await OpenWriterAsync(scratch, ct, "-c track_activities=off"))
            await using (var open = await blind.BeginTransactionAsync(ct))
            {
                await InsertRowAsync(blind, 2, hour.AddMinutes(20), 9_200_002, ct);
                Assert.Equal(1, await CountAsync(connection, $"SELECT count(*) FROM pg_stat_activity WHERE pid = {blind.ProcessID} AND xact_start IS NULL", ct));
                Assert.Null(await QueryStoreComposeStamp.BuildHourAsync(builder, hour, DateTime.UtcNow, ct));
                await open.CommitAsync(ct);
            }

            Assert.NotNull(await QueryStoreComposeStamp.BuildHourAsync(builder, hour, DateTime.UtcNow, ct));
            Assert.Equal(0, await MismatchesAsync(connection, hour, hour.AddHours(1), ct));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task AWriterWhoseStartTimeIsNotReadable_HoldsEveryPlannedHourBack_AndAnUnrelatedWriterHoldsNone()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct, build: false);
        var bodySucceeded = false;
        try
        {
            Assert.NotEmpty((await QueryStoreComposeStamp.PlanAsync(connection, DateTime.UtcNow, RetentionDays, ct)).Builds);

            await using (var other = await OpenWriterAsync(scratch, ct))
            await using (var open = await other.BeginTransactionAsync(ct))
            {
                await ExecAsync(other, "CREATE TEMP TABLE scratch_5582 (i int); INSERT INTO scratch_5582 VALUES (1)", ct);
                var plan = await QueryStoreComposeStamp.PlanAsync(connection, DateTime.UtcNow, RetentionDays, ct);
                Assert.NotEmpty(plan.Builds);
                Assert.Equal(0, plan.HeldBack);
                await open.RollbackAsync(ct);
            }

            await using (var blind = await OpenWriterAsync(scratch, ct, "-c track_activities=off"))
            await using (var open = await blind.BeginTransactionAsync(ct))
            {
                await InsertRowAsync(blind, 1, hourNow.AddMinutes(-50), 9_200_004, ct);
                Assert.Equal(1, await CountAsync(connection, $"SELECT count(*) FROM pg_stat_activity WHERE pid = {blind.ProcessID} AND xact_start IS NULL", ct));

                /* No start time to compare: every hour the plan could pick might be one the writer holds an unmarked row of. */
                var plan = await QueryStoreComposeStamp.PlanAsync(connection, DateTime.UtcNow, RetentionDays, ct);
                Assert.Empty(plan.Builds);
                Assert.True(plan.HeldBack > 0);
                await open.RollbackAsync(ct);
            }

            Assert.NotEmpty((await QueryStoreComposeStamp.PlanAsync(connection, DateTime.UtcNow, RetentionDays, ct)).Builds);
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task TheGuard_SeesAWriterOfAPlainWideTable_BeforeItIsPartitioned()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        /* An empty database with a plain (not partitioned) table of the wide table's name: pg_partition_tree returns no row for it. */
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var observer = new NpgsqlConnection(scratch.ConnectionString);
            await observer.OpenAsync(ct);
            await ExecAsync(observer, "CREATE SCHEMA collect; CREATE TABLE collect.query_store_interval_wide (i int)", ct);
            Assert.Equal(0, await CountAsync(observer, "SELECT count(*) FROM pg_partition_tree('collect.query_store_interval_wide'::regclass)", ct));

            await using var writer = await OpenWriterAsync(scratch, ct);
            await using var open = await writer.BeginTransactionAsync(ct);
            await ExecAsync(writer, "INSERT INTO collect.query_store_interval_wide VALUES (1)", ct);

            await using var command = new NpgsqlCommand(QueryStoreComposeStamp.OpenWriterSql, observer);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = ThisHour().AddHours(-1) });
            Assert.Equal(writer.ProcessID, Convert.ToInt32(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture));
            await open.RollbackAsync(ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task WithAWideTableWriterOpen_TheOldestNeverBuiltHoursStillBuildInTheSameTick_AndTheWarningNamesTheWriterWithoutItsQuery()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct, build: false);
        var bodySucceeded = false;
        try
        {
            await using var writer = await OpenWriterAsync(scratch, ct);
            await using var open = await writer.BeginTransactionAsync(ct);
            await InsertRowAsync(writer, 1, hourNow.AddMinutes(-50), 9_200_003, ct);

            /* A store clock eight hours ahead makes the plan's newest hours (up to hourNow + 5 h) ones that this writer, begun now,
               would defer: every hour from hourNow - 1 h on, seven of them, one more than a tick builds. */
            var logger = new CapturingTestLogger();
            var now = new DateTime(2026, 10, 8, 12, 0, 0, DateTimeKind.Utc);
            var watch = new QueryStoreComposeStamp.DeferralWatch(() => now);
            var options = new QueryStoreComposeStamp.TickOptions(watch, DateTime.UtcNow.AddHours(8));

            var first = await QueryStoreComposeStamp.RunTickAsync(source, DateTime.UtcNow, RetentionDays, logger, () => TimeSpan.Zero, ct, options);
            Assert.Equal(0, first.Failed);
            Assert.Equal(0, first.DeferredForWriters);
            Assert.True(first.HeldBack > 0, "the plan should report the hours the writer holds back");
            Assert.Equal(QueryStoreComposeStamp.MaxBuildsPerTick, first.BuiltMissing);
            for (var back = 2; back <= 1 + QueryStoreComposeStamp.MaxBuildsPerTick; back++)
            {
                Assert.Equal(1, await HoursBuiltAsync(connection, hourNow.AddHours(-back), ct));
            }

            Assert.Equal(0, await HoursBuiltAsync(connection, hourNow.AddHours(-1), ct));
            Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));

            /* The second tick in a row that held hours back: the Warning, naming the writer, never its query. */
            var second = await QueryStoreComposeStamp.RunTickAsync(source, DateTime.UtcNow, RetentionDays, logger, () => TimeSpan.Zero, ct, options);
            Assert.Equal(0, second.Failed);
            Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
            var line = Assert.Single(logger.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal));
            Assert.Contains($"pid {writer.ProcessID},", line, StringComparison.Ordinal);
            Assert.Contains("started 20", line, StringComparison.Ordinal);
            Assert.Contains($"application {WriterApplication}", line, StringComparison.Ordinal);
            Assert.Contains("backend type client backend", line, StringComparison.Ordinal);
            Assert.DoesNotContain("modX", line, StringComparison.Ordinal);
            Assert.DoesNotContain("INSERT", line, StringComparison.Ordinal);

            /* A third tick inside the ten minutes says nothing more at Warning. */
            now = now.AddMinutes(9);
            await QueryStoreComposeStamp.RunTickAsync(source, DateTime.UtcNow, RetentionDays, logger, () => TimeSpan.Zero, ct, options);
            Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));

            await open.CommitAsync(ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }
}
