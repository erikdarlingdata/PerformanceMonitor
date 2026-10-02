/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;
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
/// #4198: <c>get_pg_io_trend</c>'s own default-call byte census, against a real store. Its point is the widest
/// in the trend family (28 fields, all numeric/boolean - no text to truncate), so before this issue the shared
/// 200-point MCP auto-sizing target (<see cref="TrendBuckets.McpPointBudget"/>) put a default (24-hour,
/// one-series) call's 10-minute bucketing at 144-145 points and 56-59 KB, comfortably over
/// <see cref="McpResponseBudget.DefaultBytes"/> (32 KB). The fix narrows <c>get_pg_io_trend</c>'s own MCP
/// auto-sizing target so the default call lands on a wider bucket, while an explicit <c>bucket_minutes</c>
/// still reaches the read's own cap (<see cref="TrendBuckets.PgIoMaxPoints"/>), unaffected by this change and
/// covered by <see cref="TrendPayloadBudgetLiveTests"/>.
/// </summary>
[Collection("live-postgres")]
public sealed class PgIoTrendDefaultBudgetLiveTests
{
    private const string ServerName = "pg-io-trend-default-budget";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /// <summary>A day past the default window plus change, at the collector's real one-minute cadence, so the
    /// default 24-hour call's auto-sized bucket has a full window of data on both sides.</summary>
    private const int SeedMinutes = 25 * 60;

    [Fact]
    public async Task DefaultCall_OnADayOfOneMinuteCollections_StaysUnderTheMcpResponseBudget()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live pg_io_trend default-budget census.");

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

            var now = DateTime.UtcNow;
            var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0).AddMinutes(-1);
            var start = end.AddMinutes(-SeedMinutes);
            await SeedAsync(connection, start, ct);

            /* The literal default call: every argument left to the tool, exactly what an MCP client sends when
               it asks for "I/O for this server" with nothing more specific in mind. One pair seeded, so the
               tool's own dominant-subject auto-detection lands on it without backend_type/context named. */
            var answer = await DarlingMcpPgTrendTools.GetPgIoTrend(postgres, ServerName);
            var root = JsonDocument.Parse(answer).RootElement;
            Assert.Equal("io_trend", root.GetProperty("status").GetString());

            var points = root.GetProperty("point_count").GetInt32();
            var bytes = Encoding.UTF8.GetByteCount(answer);
            TestContext.Current.TestOutputHelper?.WriteLine($"get_pg_io_trend default: {points} points, {bytes / 1024.0:F1} KB");

            Assert.True(points > 0, "default call returned no points");
            Assert.True(bytes <= McpResponseBudget.DefaultBytes,
                $"get_pg_io_trend default call is {bytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget ({points} points)");

            /* An explicit width still gets what it asks for (#3897's design, unchanged by #4198): naming
               bucket_minutes is not "leave it to the tool", so it is not held to the narrower default target. */
            var explicitWidth = await DarlingMcpPgTrendTools.GetPgIoTrend(
                postgres, ServerName, backend_type: null, context: null, hours_back: 24, as_of: null, bucket_minutes: 10);
            var explicitRoot = JsonDocument.Parse(explicitWidth).RootElement;
            Assert.Equal("io_trend", explicitRoot.GetProperty("status").GetString());
            Assert.Equal(10, explicitRoot.GetProperty("bucket_minutes").GetInt32());
            Assert.True(explicitRoot.GetProperty("point_count").GetInt32() >= 144,
                "an explicit 10-minute bucket over 24h must still return the full window's points, not the narrower default width");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /// <summary>One (backend_type, context) pair over two object types (summed by the tool), monotonically
    /// increasing counters at one-minute cadence - the same shape <see cref="TrendPayloadBudgetLiveTests"/>
    /// seeds for its census, in this file's own rows so a dozen #4198 lanes running the same night do not
    /// share seeding.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime start, CancellationToken ct)
    {
        using var plant = new NpgsqlCommand(@"
WITH s AS (
    SELECT n, o.i, o.object_type, 1100 + (n % 13) * 57 AS r
    FROM generate_series(0, $5) AS n
    CROSS JOIN (VALUES (0, 'relation'), (1, 'temp relation')) AS o(i, object_type)
)
INSERT INTO pg_io_stats
    (collection_id, collection_time, server_id, server_name, backend_type, object_type, context,
     reads, read_time_ms, writes, write_time_ms, extends, op_bytes, hits, evictions, stats_reset)
SELECT $1 + n * 2 + i, $2 + n * interval '1 minute', $3, $4, 'client backend', object_type, 'normal',
       SUM(r) OVER w, SUM(r * 1.37) OVER w, SUM(r / 5) OVER w, SUM(r / 5 * 0.81) OVER w, SUM(r / 30) OVER w,
       8192, SUM(r * 83) OVER w, SUM(r / 50) OVER w, NULL::timestamp
FROM s
WINDOW w AS (PARTITION BY i ORDER BY n)", connection) { CommandTimeout = 300 };
        plant.Parameters.AddWithValue(CollectionIdGenerator.Next() + 14_000_000L);
        plant.Parameters.AddWithValue(DarlingMcpTestData.Naive(start));
        plant.Parameters.AddWithValue(ServerId);
        plant.Parameters.AddWithValue(ServerName);
        plant.Parameters.AddWithValue(SeedMinutes);
        await plant.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "pg_io_stats", "servers" })
        {
            using var delete = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = $1", connection) { CommandTimeout = 300 };
            delete.Parameters.AddWithValue(ServerId);
            await delete.ExecuteNonQueryAsync(ct);
        }
    }
}
