/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7153: the stop-time flush of the collector-cost and read-latency samples. A stop used to lose every sample since the
/// last hourly tick. These facts run the worker's own stop helper against a scratch store and read the tables back.
/// </summary>
/* #1776 own-store: each live fact mints its own scratch database through ScratchPostgres and never touches the shared
   store's tables. */
public sealed class StopTimeSampleFlushLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<(long Runs, long SqlMs, long Reads, long ReadMs)> ReadTotalsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            @"SELECT (SELECT coalesce(sum(run_count), 0) FROM collect.collector_cost WHERE collector_name = 'wait_stats'),
                     (SELECT coalesce(sum(total_sql_ms), 0) FROM collect.collector_cost WHERE collector_name = 'wait_stats'),
                     (SELECT coalesce(sum(run_count), 0) FROM collect.read_latency WHERE route = 'get_wait_stats'),
                     (SELECT coalesce(sum(total_ms), 0) FROM collect.read_latency WHERE route = 'get_wait_stats')",
            connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3));
    }

    [Fact]
    public async Task SamplesRecordedBeforeStop_AreInBothTablesAfterTheStopFlush()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the stop-time flush pin.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        try
        {
            var cost = new CollectorCostAccumulator();
            var latency = new ReadLatencyAccumulator();
            cost.Record(serverId: 1, "wait_stats", rows: 10, sqlMs: 40, storageMs: 5);
            cost.Record(serverId: 1, "wait_stats", rows: 12, sqlMs: 60, storageMs: 7);
            latency.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 50);
            latency.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 90);
            var logger = new CapturingTestLogger();

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            await DarlingWorker.FlushSamplesOnStopAsync(postgres, cost, latency, TimeSpan.FromSeconds(30), logger);

            var totals = await ReadTotalsAsync(connection, ct);
            Assert.Equal((2L, 100L, 2L, 140L), totals);
            Assert.False(cost.HasSamples);
            Assert.False(latency.HasSamples);
            Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task StopFlushThenTheNextHourlyFlushInTheSameHour_GivesTheRightTotals_WithNoDoubleCount()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the stop-time flush pin.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        try
        {
            var cost = new CollectorCostAccumulator();
            var latency = new ReadLatencyAccumulator();
            var logger = new CapturingTestLogger();
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* Before the stop: one run and one read. The stop flush writes them. */
            cost.Record(1, "wait_stats", rows: 10, sqlMs: 40, storageMs: 5);
            latency.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 50);
            await DarlingWorker.FlushSamplesOnStopAsync(postgres, cost, latency, TimeSpan.FromSeconds(30), logger);

            /* After the restart a new process records more, and its hourly tick flushes later in the same hour. Each flush stamps
               its own microsecond metric_time, so the stop flush and this one are two rows in each table (ON CONFLICT never
               fires between them) and the readers sum them. */
            var hour = DateTime.UtcNow;
            var newCost = new CollectorCostAccumulator();
            var newLatency = new ReadLatencyAccumulator();
            newCost.Record(1, "wait_stats", rows: 8, sqlMs: 60, storageMs: 4);
            newLatency.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 90);
            await newCost.FlushAsync(connection, hour, logger, ct);
            await newLatency.FlushAsync(connection, hour, logger, ct);

            /* A flush with nothing recorded adds nothing: a second stop flush straight after is a no-op, not a repeat. */
            await DarlingWorker.FlushSamplesOnStopAsync(postgres, cost, latency, TimeSpan.FromSeconds(30), logger);

            var totals = await ReadTotalsAsync(connection, ct);
            Assert.Equal((2L, 100L, 2L, 140L), totals);
            Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task NothingRecorded_OpensNoConnection_AndLogsNothing()
    {
        /* A data source no server answers: a flush that tried to connect would log the one Warning. */
        await using var unreachable = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=1");
        var logger = new CapturingTestLogger();

        await DarlingWorker.FlushSamplesOnStopAsync(
            unreachable, new CollectorCostAccumulator(), new ReadLatencyAccumulator(), TimeSpan.FromSeconds(5), logger);

        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
    }

    [Fact]
    public async Task AStoreThatCannotBeReached_LogsOneWarning_AndTheStopContinues()
    {
        await using var unreachable = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=1");
        var cost = new CollectorCostAccumulator();
        cost.Record(1, "wait_stats", rows: 1, sqlMs: 1, storageMs: 1);
        var logger = new CapturingTestLogger();

        await DarlingWorker.FlushSamplesOnStopAsync(
            unreachable, cost, new ReadLatencyAccumulator(), TimeSpan.FromSeconds(5), logger);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        /* A refusal is not a timeout: the Warning carries the real error, not the budget wording. */
        Assert.DoesNotContain("did not finish within", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("failed", logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ABudgetThatRunsOut_LogsOneWarning_WithTheTimeoutWording()
    {
        await using var unreachable = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=1");
        var cost = new CollectorCostAccumulator();
        cost.Record(1, "wait_stats", rows: 1, sqlMs: 1, storageMs: 1);
        var logger = new CapturingTestLogger();

        /* A budget that is already spent cancels the connection open, which is what the 5 s budget does on a slow store. */
        await DarlingWorker.FlushSamplesOnStopAsync(
            unreachable, cost, new ReadLatencyAccumulator(), TimeSpan.Zero, logger);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Contains("did not finish within", logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WhenOnlyTheCollectorCostFlushFails_TheReadLatencyRowsStillLand_AndTheStopContinues()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the stop-time flush pin.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        try
        {
            /* The collector-cost table is gone, so only its flush can fail. */
            await using (var rename = new NpgsqlCommand("ALTER TABLE collect.collector_cost RENAME TO collector_cost_gone", connection))
            {
                await rename.ExecuteNonQueryAsync(ct);
            }

            var cost = new CollectorCostAccumulator();
            var latency = new ReadLatencyAccumulator();
            cost.Record(1, "wait_stats", rows: 10, sqlMs: 40, storageMs: 5);
            latency.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 50);
            latency.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 90);
            var logger = new CapturingTestLogger();

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            await DarlingWorker.FlushSamplesOnStopAsync(postgres, cost, latency, TimeSpan.FromSeconds(30), logger);

            await using var command = new NpgsqlCommand(
                "SELECT coalesce(sum(run_count), 0), coalesce(sum(total_ms), 0) FROM collect.read_latency WHERE route = 'get_wait_stats'",
                connection);
            await using var reader = await command.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal((2L, 140L), (reader.GetInt64(0), reader.GetInt64(1)));

            Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
            Assert.Contains("Collector-cost flush at stop failed", logger.Joined, StringComparison.Ordinal);
            Assert.DoesNotContain("did not finish within", logger.Joined, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
