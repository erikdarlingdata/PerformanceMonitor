/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3548: get_memory_trend joins the memory-grant series so total_granted_mb carries real data — the
/// complete fix #3529's null was the honest placeholder for. Lite's twin coverage is
/// <c>MemoryTrendGrantJoinToolTests</c>; the two pin the same three claims so the SKUs cannot drift: a
/// matched point carries the pool-summed measurement, a matched point measuring NOTHING granted is a
/// genuine 0.0 (a snapshot existed — zero is a measurement, not a fabrication), and an unmatched point is
/// null with the envelope's granted_note explaining the gap — a note that vanishes entirely when every
/// point matched. The join is nearest-match within 30 seconds because each collector stamps its own
/// DateTime.UtcNow per run: same-cycle rows sit seconds apart, so equality returns nothing, while a wider
/// match would smear a slower grants cadence across points it never measured.
///
/// <para>Gated on DARLING_TEST_PG like every other live class.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMemoryTrendGrantJoinTests
{
    private const int ServerId = -949583;
    private const string ServerName = "grant-join";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task JoinedPoints_CarryThePoolSum_AGenuineZero_ANullForTheUncovered_AndTheNoteOnlyWithAGap()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live grant-join test.");

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

            /* ── fully covered: one memory point, one grants snapshot 3s later — value, and NO note ── */
            var t0 = MinutesAgo(30);
            await SeedMemoryAsync(connection, ct, t0);
            await SeedGrantAsync(connection, ct, t0.AddSeconds(3), poolId: 2, grantedMb: 50m);

            var covered = JsonDocument.Parse(await DarlingMcpTrendTools.GetMemoryTrend(postgres, ServerName, 4)).RootElement;
            Assert.Equal(50.0, covered.GetProperty("trend")[0].GetProperty("total_granted_mb").GetDouble(), precision: 6);
            Assert.False(covered.TryGetProperty("granted_note", out _),
                "a window the grants series fully covers must not be captioned with a gap note");

            /* ── the gap shapes: a two-pool sum, a genuine zero, and an uncovered point ── */
            var t1 = t0.AddMinutes(1);
            var t2 = t0.AddMinutes(2);
            var t3 = t0.AddMinutes(3);
            await SeedMemoryAsync(connection, ct, t1);
            await SeedMemoryAsync(connection, ct, t2);
            await SeedMemoryAsync(connection, ct, t3);

            var snap1 = t1.AddSeconds(4);
            await SeedGrantAsync(connection, ct, snap1, poolId: 1, grantedMb: 25m);
            await SeedGrantAsync(connection, ct, snap1, poolId: 2, grantedMb: 100m);
            await SeedGrantAsync(connection, ct, t2.AddSeconds(6), poolId: 2, grantedMb: 0m);
            /* nothing anywhere near t3 */

            var root = JsonDocument.Parse(await DarlingMcpTrendTools.GetMemoryTrend(postgres, ServerName, 4)).RootElement;
            var trend = root.GetProperty("trend");
            Assert.Equal(4, trend.GetArrayLength());
            Assert.Equal(125.0, trend[1].GetProperty("total_granted_mb").GetDouble(), precision: 6);
            Assert.Equal(JsonValueKind.Number, trend[2].GetProperty("total_granted_mb").ValueKind);
            Assert.Equal(0.0, trend[2].GetProperty("total_granted_mb").GetDouble(), precision: 6);
            Assert.Equal(JsonValueKind.Null, trend[3].GetProperty("total_granted_mb").ValueKind);

            var note = root.GetProperty("granted_note").GetString()!;
            Assert.Contains("get_memory_grants", note, StringComparison.Ordinal);
            Assert.Contains("30 seconds", note, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static async Task SeedMemoryAsync(NpgsqlConnection connection, CancellationToken ct, DateTime t) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name,
     total_server_memory_mb, target_server_memory_mb, buffer_pool_mb, plan_cache_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), ServerId, ServerName,
            40000m, 49152m, 35000m, 5000m);

    private static async Task SeedGrantAsync(NpgsqlConnection connection, CancellationToken ct, DateTime t, int poolId, decimal grantedMb) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO memory_grant_stats
    (collection_id, collection_time, server_id, server_name,
     resource_semaphore_id, pool_id, granted_memory_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), ServerId, ServerName,
            (short)0, poolId, grantedMb);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM memory_grant_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM memory_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
