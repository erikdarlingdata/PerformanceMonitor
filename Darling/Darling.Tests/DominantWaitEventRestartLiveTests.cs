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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7205: the default wait event for a PostgreSQL trend is ranked on the in-window growth of each event's
/// cumulative sample count. A target restart inside the window zeroes the counters, and the pick used
/// MAX - MIN, which across a restart is the pre-restart lifetime total of whichever event ran longest.
/// </summary>
[Collection("live-postgres")]
public sealed class DominantWaitEventRestartLiveTests
{
    private const string ServerName = "dominant-wait-restart";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly DateTime T0 = new(2026, 3, 5, 10, 0, 0);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task WindowWithARestart_RanksOnTheDeltasInTheWindow()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            /* DataFileRead: 100,000 lifetime samples before the restart, then the counter restarts from 0.
               In-window growth: 500 + 50 + 70 = 620. relation grows 890 steadily. */
            await SeedAsync(connection, "DataFileRead", ct, 100_000, 100_500, 50, 120);
            await SeedAsync(connection, "relation", ct, 10, 300, 600, 900);

            var chosen = await DarlingPgTrendReader.GetDominantWaitEventAsync(postgres, ServerId, T0, T0.AddHours(1), ct);

            Assert.Equal("relation", chosen);
        });
    }

    [Fact]
    public async Task WindowWithoutARestart_KeepsTheLargestGrowth()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            await SeedAsync(connection, "DataFileRead", ct, 100_000, 100_500, 101_000, 101_900);
            await SeedAsync(connection, "relation", ct, 10, 300, 600, 900);

            var chosen = await DarlingPgTrendReader.GetDominantWaitEventAsync(postgres, ServerId, T0, T0.AddHours(1), ct);

            Assert.Equal("DataFileRead", chosen);
        });
    }

    private static async Task SeedAsync(NpgsqlConnection connection, string eventName, CancellationToken ct, params long[] counts)
    {
        for (var i = 0; i < counts.Length; i++)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_wait_sampling
    (collection_id, collection_time, server_id, server_name, event_type, event, query_id, sample_count, profile_period_ms, backend_count, sampled_ms)
VALUES ($1, $2, $3, $4, 'IO', $5, 1, $6, 10, 1, 1000)",
                CollectionIdGenerator.Next(), T0.AddMinutes(i), ServerId, ServerName, eventName, counts[i]);
        }
    }

    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live dominant wait event tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await body(connection, postgres, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "pg_wait_sampling", "servers" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", ServerId);
        }
    }
}
