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
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7417: the read and write latency facts need <see cref="IoVolumeFloor"/> I/Os in the window. Live, over seeded
/// file_io_stats rows: each side of the floor for reads and for writes (writes count log files and tempdb only), and a
/// sustained problem with real volume still fires. Lite's twin is <c>IoVolumeFloorTests</c>.
/// </summary>
[Collection("live-postgres")]
public sealed class IoVolumeFloorLiveTests
{
    private const int ServerId = -741701;

    [Theory]
    [InlineData(2, 0, false, false)]
    [InlineData(249, 0, false, false)]
    [InlineData(250, 0, true, false)]
    [InlineData(100_000, 0, true, false)]
    [InlineData(0, 249, false, false)]
    [InlineData(0, 250, false, true)]
    [InlineData(0, 100_000, false, true)]
    public async Task TheLatencyFactsFireOnlyWithTheFloorsIos_AgainstDevPostgres(int logReads, int logWrites, bool readFires, bool writeFires)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live #7417 I/O floor test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DeleteAsync(connection, ct);
        }

        try
        {
            var end = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            await using (var connection = new NpgsqlConnection(connectionString))
            {
                await connection.OpenAsync(ct);
                /* Read rows on a user data file (40 ms a read), write rows on the log (60 ms a write). Four rows each, so the totals split evenly. */
                for (var i = 0; i < 4; i++)
                {
                    var reads = (logReads / 4) + (i < logReads % 4 ? 1 : 0);
                    var writes = (logWrites / 4) + (i < logWrites % 4 ? 1 : 0);
                    if (reads > 0)
                        await InsertAsync(connection, end.AddMinutes(-10 - i), "ROWS", reads, reads * 40L, 0, 0, ct);
                    if (writes > 0)
                        await InsertAsync(connection, end.AddMinutes(-10 - i), "LOG", 0, 0, writes, writes * 60L, ct);
                }
            }

            await using var postgres = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString);
            var facts = await new PgFactCollector(postgres).CollectFactsAsync(new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = "io-floor",
                TimeRangeStart = end.AddHours(-4),
                TimeRangeEnd = end,
                ServerUtcOffset = TimeSpan.Zero,
            });

            var read = facts.FirstOrDefault(f => f.Key == "IO_READ_LATENCY_MS");
            var write = facts.FirstOrDefault(f => f.Key == "IO_WRITE_LATENCY_MS");
            Assert.Equal(readFires, read is not null);
            Assert.Equal(writeFires, write is not null);
            if (read is not null)
                Assert.Equal(40.0, read.Value, 6);
            if (write is not null)
                Assert.Equal(60.0, write.Value, 6);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task UserDataFileWritesDoNotLendVolumeToAQuietLog_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live #7417 I/O floor test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DeleteAsync(connection, ct);
        }

        try
        {
            var end = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            await using (var connection = new NpgsqlConnection(connectionString))
            {
                await connection.OpenAsync(ct);
                await InsertAsync(connection, end.AddMinutes(-10), "LOG", 0, 0, 10, 600, ct);
                await InsertAsync(connection, end.AddMinutes(-11), "ROWS", 0, 0, 100_000, 6_000_000, ct);
            }

            await using var postgres = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString);
            var facts = await new PgFactCollector(postgres).CollectFactsAsync(new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = "io-floor",
                TimeRangeStart = end.AddHours(-4),
                TimeRangeEnd = end,
                ServerUtcOffset = TimeSpan.Zero,
            });

            Assert.DoesNotContain(facts, f => f.Key == "IO_WRITE_LATENCY_MS");
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertAsync(NpgsqlConnection connection, DateTime at, string fileType, long reads, long stallRead, long writes, long stallWrite, System.Threading.CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            @"INSERT INTO collect.file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type,
                  delta_reads, delta_writes, delta_stall_read_ms, delta_stall_write_ms)
              VALUES ($1, $2, $3, 'io-floor', 'AppDb', $4, $5, $6, $7, $8, $9)", connection);
        cmd.Parameters.AddWithValue(CollectionIdGenerator.Next());
        cmd.Parameters.AddWithValue(at);
        cmd.Parameters.AddWithValue(ServerId);
        cmd.Parameters.AddWithValue(fileType + at.Ticks);
        cmd.Parameters.AddWithValue(fileType);
        cmd.Parameters.AddWithValue(reads);
        cmd.Parameters.AddWithValue(writes);
        cmd.Parameters.AddWithValue(stallRead);
        cmd.Parameters.AddWithValue(stallWrite);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand($"DELETE FROM collect.file_io_stats WHERE server_id = {ServerId}", connection);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
