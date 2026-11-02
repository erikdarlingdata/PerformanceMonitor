/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
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
/// #7167: the offline-to-unavailable upgrade #6155 gave the get_pg_* reads now covers the SQL Server trend reads. A server the store
/// has had no collection from for days answers <c>unavailable</c> with the offline sentence first, never an <c>empty</c> that reads as
/// "genuinely quiet", and a server being collected keeps its true negative.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpSqlServerOfflineEmptyLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly string[] Tables = ["memory_stats"];

    private static async Task SeedMemoryAsync(NpgsqlConnection c, string name, DateTime t) =>
        await DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken, @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name,
     total_server_memory_mb, target_server_memory_mb, buffer_pool_mb, plan_cache_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), ServerIdHelper.GetDeterministicHashCode(name), name,
            40000m, 49152m, 35000m, 5000m);

    /// <summary>Last collected ten days ago, memory history older than the window: get_memory_trend says the server is offline.</summary>
    [Fact]
    public async Task AnEmptyMemoryTrend_FromAnOfflineServer_IsUnavailable_WithTheOfflineLeadFirst_AgainstDevPostgres()
    {
        const string Name = "sql7167-offline";
        await WindowFloorLiveHarness.RunAsync(ConnectionString, ["memory_stats"], [Name], Tables, async (c, ds, now) =>
        {
            var lastCollection = now.AddDays(-10);
            await WindowFloorLiveHarness.SeedServerAsync(
                c, Name, now.AddDays(-40), "memory_stats", lastCollection.AddHours(-3), 30, lastCollection, Tables, TestContext.Current.CancellationToken);
            await SeedMemoryAsync(c, Name, lastCollection);

            var root = WindowFloorLiveHarness.Parse(await DarlingMcpTrendTools.GetMemoryTrend(ds, Name, 1));

            Assert.Equal("unavailable", root.GetProperty("status").GetString());
            Assert.StartsWith(
                Name + " is offline: the store has had no collection from it since "
                + lastCollection.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC",
                root.GetProperty("message").GetString()!,
                StringComparison.Ordinal);
        });
    }

    /// <summary>The same read for a server collected a minute ago keeps its true negative.</summary>
    [Fact]
    public async Task AnEmptyMemoryTrend_FromAServerCollectedJustNow_StaysEmpty_AgainstDevPostgres()
    {
        const string Name = "sql7167-fresh";
        await WindowFloorLiveHarness.RunAsync(ConnectionString, ["memory_stats"], [Name], Tables, async (c, ds, now) =>
        {
            await WindowFloorLiveHarness.SeedServerAsync(
                c, Name, now.AddDays(-40), "memory_stats", DateTime.UtcNow.AddMinutes(-90), 1, DateTime.UtcNow.AddMinutes(-1), Tables, TestContext.Current.CancellationToken);
            await SeedMemoryAsync(c, Name, now.AddHours(-48));

            var root = WindowFloorLiveHarness.Parse(await DarlingMcpTrendTools.GetMemoryTrend(ds, Name, 1));

            Assert.Equal("empty", root.GetProperty("status").GetString());
        });
    }

    /// <summary>A window anchored before the server went quiet lies inside collected history, so its empty read is still a finding.</summary>
    [Fact]
    public async Task AnAsOfMemoryTrend_JudgesOfflineAsOfItsAnchor_NotAsOfNow_AgainstDevPostgres()
    {
        const string Name = "sql7167-asof";
        await WindowFloorLiveHarness.RunAsync(ConnectionString, ["memory_stats"], [Name], Tables, async (c, ds, now) =>
        {
            var lastCollection = now.AddDays(-10);
            await WindowFloorLiveHarness.SeedServerAsync(
                c, Name, now.AddDays(-40), "memory_stats", lastCollection.AddHours(-3), 30, lastCollection, Tables, TestContext.Current.CancellationToken);
            await SeedMemoryAsync(c, Name, lastCollection.AddDays(-5));

            var atAnchor = WindowFloorLiveHarness.Parse(await DarlingMcpTrendTools.GetMemoryTrend(
                ds, Name, 1, WebDataStartNote.FormatWindowEnd(lastCollection.AddHours(-1))));
            Assert.Equal("empty", atAnchor.GetProperty("status").GetString());
        });
    }
}
