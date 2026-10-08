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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5515: a server with no job run or trace event for hours missed the 6-hour probe and went straight to the
/// unbounded MAX, which opens every chunk of the server's slice (5,426 blocks a call on job_history, 3,758 on
/// default_trace_events, on the 43-server store). The ladder tries a week between the two. These pins run
/// against a live store and check both halves of the contract: the answer is the unbounded MAX's answer for the
/// columns tested (a watermark behind the store's re-reads stored rows, one ahead of it skips new ones; job_history's
/// run_datetime, which does not rise with collection_time, can read older, which only widens a re-read), and the whole-slice
/// read runs only when the week found nothing. Counted with Npgsql's own command log, as the cache's pins are
/// (<see cref="CommandCountingLoggerFactory"/>). The server-scoped reads, the cpu_utilization pair read and the
/// Azure per-database read all use the ladder.
/// </summary>
[Trait("Stage", "Live")]
[Collection("live-postgres")]
public sealed class QuietServerWatermarkLadderLiveTests
{
    private const int ServerId = -880155;

    private static string? Pg => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string TraceBounded =>
        DarlingCollectorRunner.BuildServerWatermarkSql("default_trace_events", "event_time", bounded: true);

    private static string TraceUnbounded =>
        DarlingCollectorRunner.BuildServerWatermarkSql("default_trace_events", "event_time", bounded: false);

    private static string JobBounded =>
        DarlingCollectorRunner.BuildServerWatermarkInstanceIdSql("job_history", "instance_id", bounded: true);

    private static string JobUnbounded =>
        DarlingCollectorRunner.BuildServerWatermarkInstanceIdSql("job_history", "instance_id", bounded: false);

    /// <summary>The unbounded text is a prefix of the bounded one, so its own count is what is left after the bounded ones.</summary>
    private static int Unbounded(CommandCountingLoggerProvider log, string unbounded, string bounded) =>
        log.CountContaining(unbounded) - log.CountContaining(bounded);

    [Fact]
    public async Task AQuietServer_IsAnsweredByTheWeek_WithTheSameValueAsTheUnboundedRead()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Pg), "Set DARLING_TEST_PG to run the #5515 quiet-server pins.");
        var ct = TestContext.Current.CancellationToken;

        var loggerFactory = new CommandCountingLoggerFactory();
        await using var dataSource = new NpgsqlDataSourceBuilder(Pg!).UseLoggerFactory(loggerFactory).Build();
        await using (var migrate = await dataSource.OpenConnectionAsync(ct))
        {
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        var log = loggerFactory.Provider;
        var runner = new DarlingCollectorRunner(dataSource, new CollectorDeltaCalculator());
        await CleanAsync(dataSource, ct);
        var bodySucceeded = false;
        try
        {
            /* Newest event two days ago: past the 6-hour probe, inside the week. */
            var twoDays = DateTime.UtcNow.Date - TimeSpan.FromDays(2) + TimeSpan.FromHours(3);
            await InsertTraceEventAsync(dataSource, 1, twoDays, ct);
            await InsertTraceEventAsync(dataSource, 2, twoDays - TimeSpan.FromHours(5), ct);
            await InsertJobAsync(dataSource, 1, 410, DateTime.UtcNow - TimeSpan.FromDays(3), ct);
            await InsertJobAsync(dataSource, 2, 400, DateTime.UtcNow - TimeSpan.FromDays(4), ct);

            /* The store's own answer first: this read is itself an unbounded statement, so it runs before the counting starts. */
            var storeAnswer = await UnboundedMaxAsync(dataSource, ct);
            log.Reset();
            var trace = await runner.GetLastCollectedTimeAsync(ServerId, "default_trace_events", "event_time", ct);
            var job = await runner.GetLastCollectedInstanceIdAsync(ServerId, "job_history", "instance_id", ct);

            Assert.Equal(twoDays, trace);
            Assert.Equal(410L, job);
            Assert.Equal(storeAnswer, trace);
            Assert.Equal(0, Unbounded(log, TraceUnbounded, TraceBounded));
            Assert.Equal(0, log.CountContaining(JobUnbounded));
            /* Two bounded reads each: the 6-hour probe, then the week. */
            Assert.Equal(2, log.CountContaining(TraceBounded));
            Assert.Equal(2, log.CountContaining(JobBounded));

            /* A row lands since: the 6-hour probe answers, in one read, and it is the new row (never skipped). */
            /* Whole microseconds: the store keeps no finer tick, so a finer one here could never be read back equal. */
            var recentTicks = (DateTime.UtcNow - TimeSpan.FromMinutes(5)).Ticks;
            var recent = new DateTime(recentTicks - recentTicks % TimeSpan.TicksPerMicrosecond, DateTimeKind.Utc);
            await InsertTraceEventAsync(dataSource, 3, recent, ct);
            await InsertJobAsync(dataSource, 3, 420, recent, ct);
            log.Reset();
            Assert.Equal(recent, await runner.GetLastCollectedTimeAsync(ServerId, "default_trace_events", "event_time", ct));
            Assert.Equal(420L, await runner.GetLastCollectedInstanceIdAsync(ServerId, "job_history", "instance_id", ct));
            Assert.Equal(1, log.CountContaining(TraceBounded));
            Assert.Equal(1, log.CountContaining(JobBounded));
            Assert.Equal(0, Unbounded(log, TraceUnbounded, TraceBounded));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Pg!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task AServerQuietForMoreThanAWeek_AndOneWithNoRow_StillGetTheUnboundedAnswer()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Pg), "Set DARLING_TEST_PG to run the #5515 quiet-server pins.");
        var ct = TestContext.Current.CancellationToken;

        var loggerFactory = new CommandCountingLoggerFactory();
        await using var dataSource = new NpgsqlDataSourceBuilder(Pg!).UseLoggerFactory(loggerFactory).Build();
        await using (var migrate = await dataSource.OpenConnectionAsync(ct))
        {
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        var log = loggerFactory.Provider;
        var runner = new DarlingCollectorRunner(dataSource, new CollectorDeltaCalculator());
        await CleanAsync(dataSource, ct);
        var bodySucceeded = false;
        try
        {
            /* No row at all: null after all three reads, so a first run still reads as one. */
            log.Reset();
            Assert.Null(await runner.GetLastCollectedTimeAsync(ServerId, "default_trace_events", "event_time", ct));
            Assert.Null(await runner.GetLastCollectedInstanceIdAsync(ServerId, "job_history", "instance_id", ct));
            Assert.Equal(1, Unbounded(log, TraceUnbounded, TraceBounded));
            Assert.Equal(1, log.CountContaining(JobUnbounded));

            /* Newest row twenty days ago: past the week, so the whole-slice read still finds it. */
            var old = DateTime.UtcNow.Date - TimeSpan.FromDays(20) + TimeSpan.FromHours(1);
            await InsertTraceEventAsync(dataSource, 1, old, ct);
            await InsertJobAsync(dataSource, 1, 90, old, ct);
            log.Reset();
            Assert.Equal(old, await runner.GetLastCollectedTimeAsync(ServerId, "default_trace_events", "event_time", ct));
            Assert.Equal(90L, await runner.GetLastCollectedInstanceIdAsync(ServerId, "job_history", "instance_id", ct));
            Assert.Equal(1, Unbounded(log, TraceUnbounded, TraceBounded));
            Assert.Equal(1, log.CountContaining(JobUnbounded));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Pg!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanAsync(cleanup, cleanupCt));
        }
    }

    private static string PairBounded =>
        DarlingCollectorRunner.BuildServerWatermarkPairSql("cpu_utilization_stats", "sample_time", "sample_time_utc", bounded: true);

    private static string PairUnbounded =>
        DarlingCollectorRunner.BuildServerWatermarkPairSql("cpu_utilization_stats", "sample_time", "sample_time_utc", bounded: false);

    private static string DbBounded =>
        DarlingCollectorRunner.BuildServerWatermarkForDatabaseSql("deadlocks", "deadlock_time", "database_name", bounded: true);

    private static string DbUnbounded =>
        DarlingCollectorRunner.BuildServerWatermarkForDatabaseSql("deadlocks", "deadlock_time", "database_name", bounded: false);

    private const string QuietDatabase = "QuietDb";

    /// <summary>The #3778 pair read (cpu_utilization) takes the same week rung: a server whose newest sample is
    /// days old is answered by the week, and only a server quiet longer, or with no row, runs the unbounded pair.</summary>
    [Fact]
    public async Task ThePairRead_OfAQuietServer_IsAnsweredByTheWeek_AndOnlyAnOlderOrEmptyOneRunsTheUnboundedPair()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Pg), "Set DARLING_TEST_PG to run the #5515 quiet-server pins.");
        var ct = TestContext.Current.CancellationToken;

        var loggerFactory = new CommandCountingLoggerFactory();
        await using var dataSource = new NpgsqlDataSourceBuilder(Pg!).UseLoggerFactory(loggerFactory).Build();
        await using (var migrate = await dataSource.OpenConnectionAsync(ct))
        {
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        var log = loggerFactory.Provider;
        var runner = new DarlingCollectorRunner(dataSource, new CollectorDeltaCalculator());
        await CleanAsync(dataSource, ct);
        var bodySucceeded = false;
        try
        {
            /* No row at all: (null, false) after the 6-hour probe, the week and the unbounded pair. */
            log.Reset();
            Assert.Equal((null, false), await runner.GetLastCollectedTimeWithFrameAsync(
                ServerId, "cpu_utilization_stats", "sample_time", "sample_time_utc", ct));
            Assert.Equal(2, log.CountContaining(PairBounded));
            Assert.Equal(1, log.CountContaining(PairUnbounded) - log.CountContaining(PairBounded));

            /* Newest sample two days ago: past the 6-hour probe, inside the week. No unbounded read. */
            var twoDays = DateTime.UtcNow.Date - TimeSpan.FromDays(2) + TimeSpan.FromHours(3);
            await InsertCpuAsync(dataSource, 1, twoDays, ct);
            log.Reset();
            Assert.Equal((twoDays, true), await runner.GetLastCollectedTimeWithFrameAsync(
                ServerId, "cpu_utilization_stats", "sample_time", "sample_time_utc", ct));
            Assert.Equal(2, log.CountContaining(PairBounded));
            Assert.Equal(0, log.CountContaining(PairUnbounded) - log.CountContaining(PairBounded));

            /* Newest sample twenty days ago: past the week, so the unbounded pair still finds it. */
            await CleanAsync(dataSource, ct);
            var old = DateTime.UtcNow.Date - TimeSpan.FromDays(20) + TimeSpan.FromHours(1);
            await InsertCpuAsync(dataSource, 2, old, ct);
            log.Reset();
            Assert.Equal((old, true), await runner.GetLastCollectedTimeWithFrameAsync(
                ServerId, "cpu_utilization_stats", "sample_time", "sample_time_utc", ct));
            Assert.Equal(1, log.CountContaining(PairUnbounded) - log.CountContaining(PairBounded));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Pg!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The per-database read (the Azure deadlock, blocked-process and long-query collectors call it with no
    /// floor, once per database per cycle) takes the same week rung: a quiet database whose last row is days old
    /// stops paying the unbounded read every cycle.</summary>
    [Fact]
    public async Task ThePerDatabaseRead_OfAQuietDatabase_IsAnsweredByTheWeek_AndOnlyAnOlderOrEmptyOneRunsTheUnboundedRead()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Pg), "Set DARLING_TEST_PG to run the #5515 quiet-server pins.");
        var ct = TestContext.Current.CancellationToken;

        var loggerFactory = new CommandCountingLoggerFactory();
        await using var dataSource = new NpgsqlDataSourceBuilder(Pg!).UseLoggerFactory(loggerFactory).Build();
        await using (var migrate = await dataSource.OpenConnectionAsync(ct))
        {
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        var log = loggerFactory.Provider;
        var runner = new DarlingCollectorRunner(dataSource, new CollectorDeltaCalculator());
        await CleanAsync(dataSource, ct);
        var bodySucceeded = false;
        try
        {
            /* No row for the database: null after all three reads. */
            log.Reset();
            Assert.Null(await runner.GetLastCollectedTimeForDatabaseAsync(
                ServerId, "deadlocks", "deadlock_time", "database_name", QuietDatabase, ct));
            Assert.Equal(2, log.CountContaining(DbBounded));
            Assert.Equal(1, log.CountContaining(DbUnbounded) - log.CountContaining(DbBounded));

            /* Newest deadlock two days ago: past the 6-hour probe, inside the week. No unbounded read. */
            var twoDays = DateTime.UtcNow.Date - TimeSpan.FromDays(2) + TimeSpan.FromHours(3);
            await InsertDeadlockAsync(dataSource, 1, twoDays, ct);
            log.Reset();
            Assert.Equal(twoDays, await runner.GetLastCollectedTimeForDatabaseAsync(
                ServerId, "deadlocks", "deadlock_time", "database_name", QuietDatabase, ct));
            Assert.Equal(2, log.CountContaining(DbBounded));
            Assert.Equal(0, log.CountContaining(DbUnbounded) - log.CountContaining(DbBounded));

            /* Newest deadlock twenty days ago: past the week, so the unbounded read still finds it. */
            await CleanAsync(dataSource, ct);
            var old = DateTime.UtcNow.Date - TimeSpan.FromDays(20) + TimeSpan.FromHours(1);
            await InsertDeadlockAsync(dataSource, 2, old, ct);
            log.Reset();
            Assert.Equal(old, await runner.GetLastCollectedTimeForDatabaseAsync(
                ServerId, "deadlocks", "deadlock_time", "database_name", QuietDatabase, ct));
            Assert.Equal(1, log.CountContaining(DbUnbounded) - log.CountContaining(DbBounded));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Pg!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertCpuAsync(NpgsqlDataSource dataSource, long id, DateTime sampleTime, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        using var command = new NpgsqlCommand(@"
INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sample_time_utc, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $5, $5, 1, 1)", connection);
        command.Parameters.AddWithValue(id + 9_300_000);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(sampleTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue("QUIET-WM-SRV");
        command.Parameters.AddWithValue(DateTime.SpecifyKind(sampleTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertDeadlockAsync(NpgsqlDataSource dataSource, long id, DateTime deadlockTime, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        using var command = new NpgsqlCommand(@"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name)
VALUES ($1, $2, $3, $4, $2, '<deadlock/>', $5)", connection);
        command.Parameters.AddWithValue(id + 9_300_000);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(deadlockTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue("QUIET-WM-SRV");
        command.Parameters.AddWithValue(QuietDatabase);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<DateTime?> UnboundedMaxAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        using var command = new NpgsqlCommand(
            "SELECT MAX(event_time) FROM default_trace_events WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(ServerId);
        return await command.ExecuteScalarAsync(ct) as DateTime?;
    }

    private static async Task InsertTraceEventAsync(NpgsqlDataSource dataSource, long id, DateTime eventTime, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        using var command = new NpgsqlCommand(@"
INSERT INTO default_trace_events (default_trace_event_id, collection_time, server_id, server_name, event_time)
VALUES ($1, $2, $3, $4, $5)", connection);
        command.Parameters.AddWithValue(id);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue("QUIET-WM-SRV");
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertJobAsync(
        NpgsqlDataSource dataSource, long id, long instanceId, DateTime collectionTime, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        using var command = new NpgsqlCommand(@"
INSERT INTO job_history (job_history_id, collection_time, server_id, server_name, instance_id)
VALUES ($1, $2, $3, $4, $5)", connection);
        command.Parameters.AddWithValue(id + 9_300_000);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue("QUIET-WM-SRV");
        command.Parameters.AddWithValue(instanceId);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task CleanAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await CleanAsync(connection, ct);
    }

    private static async Task CleanAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "default_trace_events", "job_history", "cpu_utilization_stats", "deadlocks" })
        {
            using var command = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = $1", connection);
            command.Parameters.AddWithValue(ServerId);
            await command.ExecuteNonQueryAsync(ct);
        }
    }
}
