/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6299: <c>get_memory_grants</c> with <c>chart: true</c> serves the whole window as one point per collection per pool (what the
/// web's two Memory Grants charts draw), where the default answer is the newest snapshot plus window aggregates. Live against a
/// seeded store: the series has a point for every collection (not one instant), a collection with no grants is a point of zeros, a
/// window with no collection has no points, and the default answer is unchanged.
/// </summary>
[Collection("live-postgres")]
public sealed class MemoryGrantChartToolLiveTests
{
    private const string ServerName = "darling-mcp-grant-chart-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task SeedAsync(NpgsqlConnection connection, DateTime t, int pool, decimal granted, decimal available, int grantees, int waiters, long timeouts, long forced, System.Threading.CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO memory_grant_stats (collection_id, collection_time, server_id, server_name, resource_semaphore_id, pool_id, target_memory_mb, max_target_memory_mb, total_memory_mb, available_memory_mb, granted_memory_mb, used_memory_mb, grantee_count, waiter_count, timeout_error_count, forced_grant_count, timeout_error_count_delta, forced_grant_count_delta, sample_interval_seconds)
VALUES ($1,$2,$3,$4,0,$5,8000,12000,8000,$6,$7,$7,$8,$9,0,0,$10,$11,60)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, pool, available, granted, grantees, waiters, timeouts, forced);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM memory_grant_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task Chart_ServesOnePointPerCollectionPerPool_AZeroGrantSnapshotIsZeros_AndAnEmptyWindowHasNoPoints_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live memory-grant chart test.");

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
            var end = DateTime.SpecifyKind(DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow), DateTimeKind.Utc);
            var asOf = WebDataStartNote.FormatWindowEnd(end);
            var t1 = end.AddMinutes(-40);
            var t2 = end.AddMinutes(-30);
            var t3 = end.AddMinutes(-5);

            // t1: a busy snapshot. t2: a snapshot with no grants (everything 0). t3: busy again. Two pools throughout.
            await SeedAsync(connection, t1, 1, granted: 400m, available: 600m, grantees: 4, waiters: 2, timeouts: 1, forced: 0, ct);
            await SeedAsync(connection, t1, 2, granted: 50m, available: 950m, grantees: 1, waiters: 0, timeouts: 0, forced: 0, ct);
            await SeedAsync(connection, t2, 1, granted: 0m, available: 1000m, grantees: 0, waiters: 0, timeouts: 0, forced: 0, ct);
            await SeedAsync(connection, t2, 2, granted: 0m, available: 1000m, grantees: 0, waiters: 0, timeouts: 0, forced: 0, ct);
            await SeedAsync(connection, t3, 1, granted: 100m, available: 900m, grantees: 1, waiters: 0, timeouts: 0, forced: 2, ct);
            await SeedAsync(connection, t3, 2, granted: 10m, available: 990m, grantees: 1, waiters: 0, timeouts: 0, forced: 0, ct);

            var chartJson = await DarlingMcpMemoryGrantTools.GetMemoryGrants(postgres, ServerName, 1, asOf, chart: true, cancellationToken: ct);
            using (var doc = JsonDocument.Parse(chartJson))
            {
                Assert.True(doc.RootElement.TryGetProperty("entries", out var entriesEl), chartJson);
                var entries = entriesEl.EnumerateArray().ToList();
                // Every collection of every pool is a point (the window is short enough to draw raw collections): 3 times x 2 pools.
                Assert.Equal(6, entries.Count);
                Assert.Equal(3, entries.Select(e => e.GetProperty("collection_time").GetString()).Distinct().Count());
                Assert.False(doc.RootElement.TryGetProperty("grants", out _));

                var zeroSnapshot = entries.Where(e => e.GetProperty("collection_time").GetString()!.StartsWith(t2.ToString("yyyy-MM-ddTHH:mm:ss"), StringComparison.Ordinal)).ToList();
                Assert.Equal(2, zeroSnapshot.Count);
                Assert.All(zeroSnapshot, e =>
                {
                    Assert.Equal(0, e.GetProperty("granted_memory_mb").GetDouble());
                    Assert.Equal(0, e.GetProperty("grantee_count").GetInt64());
                    Assert.Equal(0, e.GetProperty("waiter_count").GetInt64());
                    Assert.Equal(1000, e.GetProperty("available_memory_mb").GetDouble());
                });

                var busy = entries.Single(e => e.GetProperty("pool_id").GetInt32() == 1 && e.GetProperty("granted_memory_mb").GetDouble() == 400);
                Assert.Equal(4, busy.GetProperty("grantee_count").GetInt64());
                Assert.Equal(2, busy.GetProperty("waiter_count").GetInt64());
                Assert.Equal(1, busy.GetProperty("timeout_error_count_delta").GetInt64());
                Assert.Equal(8000, busy.GetProperty("target_memory_mb").GetDouble());
                Assert.Equal(12000, busy.GetProperty("max_target_memory_mb").GetDouble());
            }

            // A wider bucket merges collections: the mean of the gauges, so the three snapshots of pool 1 land in fewer points.
            var wide = await DarlingMcpMemoryGrantTools.GetMemoryGrants(postgres, ServerName, 1, asOf, chart: true, bucket_minutes: 60, cancellationToken: ct);
            using (var doc = JsonDocument.Parse(wide))
            {
                Assert.Equal(60, doc.RootElement.GetProperty("bucket_minutes").GetInt32());
                Assert.True(doc.RootElement.GetProperty("entries").GetArrayLength() < 6);
            }

            // The default answer is unchanged: grants[] and window[], no series.
            var plain = await DarlingMcpMemoryGrantTools.GetMemoryGrants(postgres, ServerName, 1, asOf, cancellationToken: ct);
            using (var doc = JsonDocument.Parse(plain))
            {
                Assert.True(doc.RootElement.TryGetProperty("grants", out _));
                Assert.True(doc.RootElement.TryGetProperty("window", out _));
                Assert.False(doc.RootElement.TryGetProperty("entries", out _));
            }

            // bucket_minutes only means something with chart: true, so it is refused rather than ignored.
            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(
                await DarlingMcpMemoryGrantTools.GetMemoryGrants(postgres, ServerName, 1, asOf, bucket_minutes: 10, cancellationToken: ct)));

            // A window no collection fell in has no points at all: the empty status, never a series of zeros.
            var emptyWindow = WebDataStartNote.FormatWindowEnd(end.AddDays(-3));
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(
                await DarlingMcpMemoryGrantTools.GetMemoryGrants(postgres, ServerName, 1, emptyWindow, chart: true, cancellationToken: ct)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }
}
