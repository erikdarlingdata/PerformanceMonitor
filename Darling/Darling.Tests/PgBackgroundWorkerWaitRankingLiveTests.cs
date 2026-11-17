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
/// #8916: background workers (the pg_cron launcher, the TimescaleDB scheduler) sleep in the generic Extension / Extension wait
/// around the clock, so by time alone it outranks every client wait. Activity, Client and Timeout waits never reach the tables:
/// the collectors drop them. The sampler arm drops the generic Extension wait of a backend that is not a client backend at
/// collection. Every arm ranks the event after every other wait in the grids and never picks it as the dominant wait of the
/// automatic trend choice, because the Aurora arm has no backend type and rows stored before the collection rule keep the old
/// shape. The fixtures carry the shapes the engines emit: an Aurora Extension row with a one-second average wait and a total
/// near the window length, beside a real client wait.
/// </summary>
[Collection("live-postgres")]
public sealed class PgBackgroundWorkerWaitRankingLiveTests
{
    private const string ServerName = "pg8916-idle-wait-class";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly DateTime T0 = new(2026, 3, 5, 10, 0, 0);

    /// <summary>One hour of wall clock in microseconds: what a worker asleep for the whole window accrues on the Aurora counters.</summary>
    private const long WindowUs = 3_600_000_000;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheEngineArmChoice_SkipsTheGenericExtensionWait_AndPicksTheClientWait()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            await SeedEngineAsync(connection, "Extension", "Extension", ct, WindowUs, 3_600);
            await SeedEngineAsync(connection, "IO", "XactSync", ct, 200_000_000, 200_000);

            var chosen = await DarlingPgTrendReader.GetDominantWaitEventAsync(postgres, ServerId, T0, T0.AddHours(1), DarlingPgTrendReader.PgWaitTrendArm.EngineCumulative, ct);

            Assert.Equal("XactSync", chosen);
        });
    }

    [Fact]
    public async Task TheEngineArmChoice_WithOnlyTheGenericExtensionWait_PicksNothing()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            await SeedEngineAsync(connection, "Extension", "Extension", ct, WindowUs, 3_600);

            var chosen = await DarlingPgTrendReader.GetDominantWaitEventAsync(postgres, ServerId, T0, T0.AddHours(1), DarlingPgTrendReader.PgWaitTrendArm.EngineCumulative, ct);

            Assert.Null(chosen);
        });
    }

    [Fact]
    public async Task TheEngineArmChoice_KeepsAnExtensionThatNamesItsOwnEvent()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            await SeedEngineAsync(connection, "Extension", "Extension", ct, WindowUs, 3_600);
            await SeedEngineAsync(connection, "Extension", "PostgresFdwGetResult", ct, 900_000_000, 1_000);
            await SeedEngineAsync(connection, "IO", "XactSync", ct, 200_000_000, 200_000);

            var chosen = await DarlingPgTrendReader.GetDominantWaitEventAsync(postgres, ServerId, T0, T0.AddHours(1), DarlingPgTrendReader.PgWaitTrendArm.EngineCumulative, ct);

            Assert.Equal("PostgresFdwGetResult", chosen);
        });
    }

    [Fact]
    public async Task TheSamplerArmChoice_SkipsTheGenericExtensionWait_AndPicksTheNextRealOne()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            await SeedSampledAsync(connection, "Extension", "Extension", ct, 0, 90_000);
            await SeedSampledAsync(connection, "IO", "DataFileRead", ct, 0, 300);

            var chosen = await DarlingPgTrendReader.GetDominantWaitEventAsync(postgres, ServerId, T0, T0.AddHours(1), DarlingPgTrendReader.PgWaitTrendArm.Sampled, ct);

            Assert.Equal("DataFileRead", chosen);
        });
    }

    [Fact]
    public async Task TheEngineGrid_KeepsTheGenericExtensionRow_ButRanksItAfterEveryOtherWait()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            await SeedEngineAsync(connection, "Extension", "Extension", ct, WindowUs, 3_600);
            await SeedEngineAsync(connection, "IO", "XactSync", ct, 200_000_000, 200_000);
            await SeedEngineAsync(connection, "LWLock", "WALWrite", ct, 5_000_000, 5_000);

            var rows = await DarlingPgWaitReader.GetPgWaitStatsAsync(postgres, ServerId, T0, T0.AddHours(1), 20, ct);

            Assert.Equal(new[] { "XactSync", "WALWrite", "Extension" }, rows.Select(r => r.WaitEvent).ToArray());

            var capped = await DarlingPgWaitReader.GetPgWaitStatsAsync(postgres, ServerId, T0, T0.AddHours(1), 2, ct);

            Assert.Equal(new[] { "XactSync", "WALWrite" }, capped.Select(r => r.WaitEvent).ToArray());
        });
    }

    [Fact]
    public async Task TheSampledGrid_KeepsTheGenericExtensionRow_ButRanksItAfterEveryOtherWait()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            await SeedSampledAsync(connection, "Extension", "Extension", ct, 0, 90_000);
            await SeedSampledAsync(connection, "IO", "DataFileRead", ct, 0, 300);
            await SeedSampledAsync(connection, "LWLock", "WALWrite", ct, 0, 20);

            var page = await DarlingPgWaitSamplingReader.GetPgWaitSamplingPageAsync(postgres, ServerId, T0, T0.AddHours(1), 21, ct);

            Assert.Equal(new[] { "DataFileRead", "WALWrite", "Extension" }, page.Rows.Select(r => r.Event).ToArray());
        });
    }

    /// <summary>
    /// The sampler's own snapshot statement, run over a stand-in pg_stat_activity so each backend type is present at once:
    /// a background worker's generic Extension wait is dropped, a client backend's is kept, a background worker's named wait is kept, and an active background worker on CPU (no wait type, no wait event) is kept.
    /// </summary>
    [Fact]
    public async Task TheSamplerSnapshot_DropsABackgroundWorkersGenericExtensionWait_AndKeepsAClientBackends()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live background worker wait tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        foreach (var statement in new[]
        {
            "SET search_path = pg_temp, pg_catalog",
            "CREATE TEMP TABLE pg_stat_activity (pid int, wait_event_type text, wait_event text, state text, application_name text, query_id bigint, backend_type text)",
            @"INSERT INTO pg_stat_activity VALUES
                (101, 'Extension', 'Extension', 'idle', '', NULL, 'pg_cron launcher'),
                (102, 'Extension', 'Extension', 'idle', '', NULL, 'TimescaleDB Background Worker Scheduler'),
                (103, 'Extension', 'Extension', 'active', 'app', 7, 'client backend'),
                (104, 'IO', 'WalSync', 'active', '', NULL, 'walwriter'),
                (105, 'Extension', 'PostgresFdwGetResult', 'active', 'app', 8, 'client backend'),
                (106, NULL, NULL, 'active', '', NULL, 'autovacuum worker')",
        })
        {
            await using var setup = new NpgsqlCommand(statement, connection);
            await setup.ExecuteNonQueryAsync(ct);
        }

        var kept = new System.Collections.Generic.List<int>();
        await using (var snapshot = new NpgsqlCommand(PgWaitSamplingCollector.SamplerSnapshotSql(hasQueryId: true), connection))
        await using (var reader = await snapshot.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                kept.Add(reader.GetInt32(3));
            }
        }

        Assert.Equal(new[] { 103, 104, 105, 106 }, kept.OrderBy(pid => pid).ToArray());
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
    private static async Task SeedEngineAsync(NpgsqlConnection connection, string waitType, string waitEvent, CancellationToken ct, long waitTimeUs, long waits = 100)
    {
        for (var i = 0; i < 2; i++)
        {
            var first = i == 0;
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type_id, wait_event_id, wait_type, wait_event, waits, wait_time_us, delta_waits, delta_wait_time_us, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 1, 100, $5, $6, $7, $8, $9, $10, $11)",
                CollectionIdGenerator.Next(), T0.AddMinutes(i), ServerId, ServerName, waitType, waitEvent,
                first ? 0L : waits, first ? 0L : waitTimeUs, first ? 0L : waits, first ? 0L : waitTimeUs, first ? 0 : 60);
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
