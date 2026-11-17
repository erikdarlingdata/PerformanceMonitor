/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8916: an idle background process asleep in its main loop waits in the Activity class around the clock. Left in the ranking it
/// is the top wait, and the wait trend charts it as "the event that dominates this window". The automatic choice skips the class,
/// and the grids keep the rows but rank them after every other wait. An Extension wait is not skipped: the rows carry no backend
/// type, so a real wait of an extension cannot be told from an idle worker's.
/// </summary>
[Collection("live-postgres")]
public sealed class PgIdleWaitClassRankingLiveTests
{
    private const string ServerName = "pg8916-idle-wait-class";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly DateTime T0 = new(2026, 3, 5, 10, 0, 0);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheSamplerArmChoice_SkipsAnActivityWait_AndPicksTheNextRealOne()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            await SeedSampledAsync(connection, "Activity", "BgWriterHibernate", ct, 0, 90_000);
            await SeedSampledAsync(connection, "IO", "DataFileRead", ct, 0, 300);

            var chosen = await DarlingPgTrendReader.GetDominantWaitEventAsync(postgres, ServerId, T0, T0.AddHours(1), DarlingPgTrendReader.PgWaitTrendArm.Sampled, ct);

            Assert.Equal("DataFileRead", chosen);
        });
    }

    [Fact]
    public async Task TheEngineArmChoice_SkipsAnActivityWait_AndPicksTheNextRealOne()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            await SeedEngineAsync(connection, "Activity", "WalWriterMain", ct, 90_000_000);
            await SeedEngineAsync(connection, "IO", "XactSync", ct, 200_000);

            var chosen = await DarlingPgTrendReader.GetDominantWaitEventAsync(postgres, ServerId, T0, T0.AddHours(1), DarlingPgTrendReader.PgWaitTrendArm.EngineCumulative, ct);

            Assert.Equal("XactSync", chosen);
        });
    }

    [Fact]
    public async Task TheEngineArmChoice_KeepsAnExtensionWait()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            await SeedEngineAsync(connection, "Extension", "Extension", ct, 90_000_000);
            await SeedEngineAsync(connection, "IO", "XactSync", ct, 200_000);

            var chosen = await DarlingPgTrendReader.GetDominantWaitEventAsync(postgres, ServerId, T0, T0.AddHours(1), DarlingPgTrendReader.PgWaitTrendArm.EngineCumulative, ct);

            Assert.Equal("Extension", chosen);
        });
    }

    [Fact]
    public async Task TheEngineGrid_KeepsTheActivityRow_ButRanksItAfterEveryOtherWait()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            await SeedEngineAsync(connection, "Activity", "WalWriterMain", ct, 90_000_000);
            await SeedEngineAsync(connection, "IO", "XactSync", ct, 200_000);
            await SeedEngineAsync(connection, "LWLock", "WALWrite", ct, 5_000);

            var rows = await DarlingPgWaitReader.GetPgWaitStatsAsync(postgres, ServerId, T0, T0.AddHours(1), 20, ct);

            Assert.Equal(new[] { "XactSync", "WALWrite", "WalWriterMain" }, rows.Select(r => r.WaitEvent).ToArray());
        });
    }

    [Fact]
    public async Task TheSampledGrid_KeepsTheActivityRow_ButRanksItAfterEveryOtherWait()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            await SeedSampledAsync(connection, "Activity", "BgWriterHibernate", ct, 0, 90_000);
            await SeedSampledAsync(connection, "IO", "DataFileRead", ct, 0, 300);
            await SeedSampledAsync(connection, "LWLock", "WALWrite", ct, 0, 20);

            var page = await DarlingPgWaitSamplingReader.GetPgWaitSamplingPageAsync(postgres, ServerId, T0, T0.AddHours(1), 21, ct);

            Assert.Equal(new[] { "DataFileRead", "WALWrite", "BgWriterHibernate" }, page.Rows.Select(r => r.Event).ToArray());
        });
    }

    private static async Task SeedSampledAsync(NpgsqlConnection connection, string eventType, string eventName, CancellationToken ct, params long[] counts)
    {
        for (var i = 0; i < counts.Length; i++)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_wait_sampling
    (collection_id, collection_time, server_id, server_name, event_type, event, query_id, sample_count, profile_period_ms, backend_count, sampled_ms)
VALUES ($1, $2, $3, $4, $5, $6, 1, $7, 10, 1, 1000)",
                CollectionIdGenerator.Next(), T0.AddMinutes(i * 2), ServerId, ServerName, eventType, eventName, counts[i]);
        }
    }

    /// <summary>Two collections a minute apart: the first is the baseline, the second carries the wait time as its delta.</summary>
    private static async Task SeedEngineAsync(NpgsqlConnection connection, string waitType, string waitEvent, CancellationToken ct, long waitTimeUs)
    {
        for (var i = 0; i < 2; i++)
        {
            var first = i == 0;
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type_id, wait_event_id, wait_type, wait_event, waits, wait_time_us, delta_waits, delta_wait_time_us, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 1, 100, $5, $6, $7, $8, $9, $10, $11)",
                CollectionIdGenerator.Next(), T0.AddMinutes(i), ServerId, ServerName, waitType, waitEvent,
                first ? 0L : 100L, first ? 0L : waitTimeUs, first ? 0L : 100L, first ? 0L : waitTimeUs, first ? 0 : 60);
        }
    }

    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live idle wait class tests.");

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
        foreach (var table in new[] { "pg_wait_sampling", "pg_wait_stats", "servers" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", ServerId);
        }
    }
}
