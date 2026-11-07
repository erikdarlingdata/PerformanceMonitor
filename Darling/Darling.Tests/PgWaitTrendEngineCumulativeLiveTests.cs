/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8074, against a live store: on an Aurora PostgreSQL target the wait history is the per-collection deltas of the engine's
/// cumulative counters in <c>pg_wait_stats</c> (the sampler collector is gated off there, so <c>pg_wait_sampling</c> holds
/// nothing for it), and <c>get_pg_wait_trend</c> serves its series from those rows, disclosing <c>engine_cumulative</c>. An
/// Aurora target with no wait rows in the window is <c>empty</c>, not a permanent gap pointing at a read with no time axis. A
/// stock target still reads the sampler.
/// </summary>
[Collection("live-postgres")]
public sealed class PgWaitTrendEngineCumulativeLiveTests
{
    internal const string ServerName = "pg-wait-trend-aurora-e2e";
    internal static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    internal static readonly DateTime T0 = new(2026, 3, 5, 10, 0, 0);
    internal const string AsOfText = "2026-03-05T11:00:00Z";
    internal const int Hours = 1;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AnAuroraTarget_ServesTheTrendFromTheCumulativeCounters()
    {
        await RunAsync(MonitoredEngineKind.AuroraPostgres, async (connection, postgres, ct) =>
        {
            await SeedCountersAsync(connection, ct);

            var root = Root(await DarlingMcpPgTrendTools.GetPgWaitTrend(postgres, ServerName, "DataFileRead", Hours, AsOfText, cancellationToken: ct));

            Assert.Equal("wait_trend", root.GetProperty("status").GetString());
            Assert.Equal(PgWaitInstrument.EngineCumulative, root.GetProperty("instrument").GetString());
            Assert.Equal("DataFileRead", root.GetProperty("wait_event").GetString());

            var points = root.GetProperty("points").EnumerateArray().ToArray();
            /* The first sighting carries no measured interval: it is the series' left edge, not a point. */
            Assert.Equal(2, points.Length);
            Assert.Equal(2, root.GetProperty("point_count").GetInt32());
            Assert.Equal(0, root.GetProperty("counter_reset_count").GetInt32());

            Assert.Equal(Stamp(T0.AddMinutes(10)), Stamp(points[0].GetProperty("collection_time")));
            Assert.Equal(20, points[0].GetProperty("sample_count").GetInt64());
            Assert.Equal(2_000.0 / 300, points[0].GetProperty("estimated_wait_ms_per_second").GetDouble(), 3);
            Assert.Equal(JsonValueKind.Null, points[0].GetProperty("backend_count").ValueKind);
            Assert.False(points[0].GetProperty("counter_reset").GetBoolean());

            Assert.Equal(Stamp(T0.AddMinutes(15)), Stamp(points[1].GetProperty("collection_time")));
            Assert.Equal(40, points[1].GetProperty("sample_count").GetInt64());
            Assert.Equal(20.0, points[1].GetProperty("estimated_wait_ms_per_second").GetDouble(), 3);
            Assert.Equal(JsonValueKind.Null, points[1].GetProperty("backend_count").ValueKind);
        });
    }

    [Fact]
    public async Task AnAuroraTarget_WithNoEventNamed_FollowsTheEventWithTheMostWaitTime()
    {
        await RunAsync(MonitoredEngineKind.AuroraPostgres, async (connection, postgres, ct) =>
        {
            await SeedCountersAsync(connection, ct);

            var root = Root(await DarlingMcpPgTrendTools.GetPgWaitTrend(postgres, ServerName, null, Hours, AsOfText, cancellationToken: ct));

            Assert.Equal("wait_trend", root.GetProperty("status").GetString());
            /* DataFileRead accrued 8 s of wait time against BufferMapping's 1 s; the choice is by time, not by name. */
            Assert.Equal("DataFileRead", root.GetProperty("wait_event").GetString());
            Assert.StartsWith("chosen automatically", root.GetProperty("wait_event_source").GetString(), StringComparison.Ordinal);
            Assert.Equal(PgWaitInstrument.EngineCumulative, root.GetProperty("instrument").GetString());
            Assert.Equal(2, root.GetProperty("point_count").GetInt32());
        });
    }

    [Fact]
    public async Task AnAuroraTarget_WithNoWaitRows_IsEmpty_AndIsNotSentToTheWindowRead()
    {
        await RunAsync(MonitoredEngineKind.AuroraPostgres, async (connection, postgres, ct) =>
        {
            var root = Root(await DarlingMcpPgTrendTools.GetPgWaitTrend(postgres, ServerName, "DataFileRead", Hours, AsOfText, cancellationToken: ct));

            /* The engine supplies the signal and the collector runs here, so nothing was found is the whole story. */
            Assert.Equal("empty", root.GetProperty("status").GetString());
            var message = root.GetProperty("message").GetString()!;
            Assert.DoesNotContain("never will", message, StringComparison.Ordinal);
            Assert.DoesNotContain("Check that panel", message, StringComparison.Ordinal);
            Assert.Contains("get_pg_wait_stats", message, StringComparison.Ordinal);
        });
    }

    /// <summary>A guard, not a red: a stock target's trend still comes from the sampler rows, whichever arm the store holds for
    /// other servers.</summary>
    [Fact]
    public async Task AStockTarget_StillReadsTheSampler()
    {
        await RunAsync(MonitoredEngineKind.Postgres, async (connection, postgres, ct) =>
        {
            for (var i = 0; i < 5; i++)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO pg_wait_sampling (collection_id, collection_time, server_id, server_name, event_type, event, query_id, sample_count, profile_period_ms, backend_count, sampled_ms) VALUES ($1, $2, $3, $4, 'Lock', 'relation', 1, $5, 10, 2, $6)",
                    CollectionIdGenerator.Next(), T0.AddMinutes(i), ServerId, ServerName, 100L + 40L * i * (i + 1), 1_000L + 400L * i * (i + 1));
            }

            var root = Root(await DarlingMcpPgTrendTools.GetPgWaitTrend(postgres, ServerName, null, Hours, AsOfText, cancellationToken: ct));

            Assert.Equal("wait_trend", root.GetProperty("status").GetString());
            Assert.Equal("relation", root.GetProperty("wait_event").GetString());
            Assert.NotEqual(PgWaitInstrument.EngineCumulative, root.GetProperty("instrument").GetString());
            Assert.Equal(4, root.GetProperty("point_count").GetInt32());
        });
    }

    /// <summary>The rows the collector writes for two events over three collections: the first sighting of each carries no
    /// measured interval (delta 0, interval 0), then two real intervals of 300 seconds.</summary>
    internal static async Task SeedCountersAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await SeedCounterAsync(connection, ct, T0.AddMinutes(5), "IO", 1, "DataFileRead", 101, waits: 1_000, waitTimeUs: 50_000_000, deltaWaits: 0, deltaWaitTimeUs: 0, interval: 0);
        await SeedCounterAsync(connection, ct, T0.AddMinutes(10), "IO", 1, "DataFileRead", 101, waits: 1_020, waitTimeUs: 52_000_000, deltaWaits: 20, deltaWaitTimeUs: 2_000_000, interval: 300);
        await SeedCounterAsync(connection, ct, T0.AddMinutes(15), "IO", 1, "DataFileRead", 101, waits: 1_060, waitTimeUs: 58_000_000, deltaWaits: 40, deltaWaitTimeUs: 6_000_000, interval: 300);

        await SeedCounterAsync(connection, ct, T0.AddMinutes(5), "LWLock", 2, "BufferMapping", 201, waits: 500, waitTimeUs: 9_000_000, deltaWaits: 0, deltaWaitTimeUs: 0, interval: 0);
        await SeedCounterAsync(connection, ct, T0.AddMinutes(10), "LWLock", 2, "BufferMapping", 201, waits: 505, waitTimeUs: 9_500_000, deltaWaits: 5, deltaWaitTimeUs: 500_000, interval: 300);
        await SeedCounterAsync(connection, ct, T0.AddMinutes(15), "LWLock", 2, "BufferMapping", 201, waits: 510, waitTimeUs: 10_000_000, deltaWaits: 5, deltaWaitTimeUs: 500_000, interval: 300);
    }

    private static Task SeedCounterAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime t, string waitType, int waitTypeId, string waitEvent, long waitEventId,
        long waits, long waitTimeUs, long deltaWaits, long deltaWaitTimeUs, int interval) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO pg_wait_stats (collection_id, collection_time, server_id, server_name, wait_type_id, wait_event_id, wait_type, wait_event, waits, wait_time_us, delta_waits, delta_wait_time_us, sample_interval_seconds) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, waitTypeId, waitEventId, waitType, waitEvent, waits, waitTimeUs, deltaWaits, deltaWaitTimeUs, interval);

    internal static async Task RunAsync(string engineKind, Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live PostgreSQL wait trend tests.");

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
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", ServerId, engineKind);
            await body(connection, postgres, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    internal static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "pg_wait_stats", "pg_wait_sampling", "collector_state", "servers" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", ServerId);
        }
    }

    private static string Stamp(DateTime value) => value.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture);

    private static string Stamp(JsonElement value) => value.GetString()![..19];

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;
}
