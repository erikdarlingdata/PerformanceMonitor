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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6155: an offline server's empty read is missing data, not a healthy answer. The reads the web PostgreSQL panels use answer
/// <c>unavailable</c>, with the offline sentence first, instead of "no dead tuples" or "no replication slots", and a server that is
/// being collected still gets its true negative.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpPgOfflineEmptyLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly string[] Tables = ["pg_blocking_edges", "pg_wait_stats", "pg_session_states"];

    private static Task<string> ReadAsync(NpgsqlDataSource ds, string read, string name, string? asOf = null) => read switch
    {
        "get_pg_autovacuum_health" => DarlingMcpPgAutovacuumTools.GetPgAutovacuumHealth(ds, name, 1, as_of: asOf),
        "get_pg_replication_slots" => DarlingMcpPgSlotTools.GetPgReplicationSlots(ds, name, 1, asOf),
        _ => DarlingMcpPgReplicationStatsTools.GetPgReplicationStats(ds, name, 1, as_of: asOf),
    };

    /// <summary>A target last collected ten days ago: each read says it is offline first and gives no healthy sentence.</summary>
    [Theory]
    [InlineData("get_pg_autovacuum_health")]
    [InlineData("get_pg_replication_slots")]
    [InlineData("get_pg_replication_stats")]
    public async Task AnEmptyRead_FromAnOfflineServer_IsUnavailable_WithTheOfflineLeadFirst_AgainstDevPostgres(string read)
    {
        const string Name = "pg6155-offline.example:appdb:pg";
        await WindowFloorLiveHarness.RunAsync(ConnectionString, ["pg_blocking"], [Name], Tables, async (c, ds, now) =>
        {
            var lastCollection = now.AddDays(-10);
            await WindowFloorLiveHarness.SeedServerAsync(
                c, Name, now.AddDays(-40), "pg_blocking", lastCollection.AddHours(-3), 30, lastCollection, Tables, TestContext.Current.CancellationToken);

            var root = WindowFloorLiveHarness.Parse(await ReadAsync(ds, read, Name));

            Assert.Equal("unavailable", root.GetProperty("status").GetString());
            var message = root.GetProperty("message").GetString()!;
            Assert.StartsWith(
                Name + " is offline: the store has had no collection from it since "
                + lastCollection.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC",
                message,
                StringComparison.Ordinal);
            Assert.DoesNotContain("healthy", message, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("no replication slots", message, StringComparison.OrdinalIgnoreCase);
        });
    }

    /// <summary>The same reads against a target being collected keep their true negative: <c>empty</c>.</summary>
    [Theory]
    [InlineData("get_pg_autovacuum_health")]
    [InlineData("get_pg_replication_slots")]
    public async Task AnEmptyRead_FromAServerCollectedJustNow_StaysEmpty_AgainstDevPostgres(string read)
    {
        const string Name = "pg6155-fresh.example:appdb:pg";
        await WindowFloorLiveHarness.RunAsync(ConnectionString, ["pg_blocking"], [Name], Tables, async (c, ds, now) =>
        {
            await WindowFloorLiveHarness.SeedServerAsync(
                c, Name, now.AddDays(-40), "pg_blocking", DateTime.UtcNow.AddMinutes(-10), 1, DateTime.UtcNow.AddMinutes(-1), Tables, TestContext.Current.CancellationToken);

            var root = WindowFloorLiveHarness.Parse(await ReadAsync(ds, read, Name));

            Assert.Equal("empty", root.GetProperty("status").GetString());
        });
    }

    /// <summary>A window anchored before the server went quiet lies inside collected history, so its empty read is still a finding.</summary>
    [Fact]
    public async Task AnEmptyRead_OverAWindowBeforeTheServerWentOffline_StaysEmpty_AgainstDevPostgres()
    {
        const string Name = "pg6155-history.example:appdb:pg";
        await WindowFloorLiveHarness.RunAsync(ConnectionString, ["pg_blocking"], [Name], Tables, async (c, ds, now) =>
        {
            var lastCollection = now.AddDays(-10);
            await WindowFloorLiveHarness.SeedServerAsync(
                c, Name, now.AddDays(-40), "pg_blocking", lastCollection.AddHours(-3), 30, lastCollection, Tables, TestContext.Current.CancellationToken);

            var inHistory = WebDataStartNote.FormatWindowEnd(lastCollection.AddHours(-1));
            var root = WindowFloorLiveHarness.Parse(await ReadAsync(ds, "get_pg_autovacuum_health", Name, inHistory));

            Assert.Equal("empty", root.GetProperty("status").GetString());
        });
    }

    /// <summary>Only the <c>empty</c> word is replaced: any other answer for an offline server comes back as it was.</summary>
    [Fact]
    public async Task OnlyAnEmptyAnswer_IsReplaced_AgainstDevPostgres()
    {
        const string Name = "pg6155-words.example:appdb:pg";
        await WindowFloorLiveHarness.RunAsync(ConnectionString, ["pg_blocking"], [Name], Tables, async (c, ds, now) =>
        {
            var lastCollection = now.AddDays(-10);
            await WindowFloorLiveHarness.SeedServerAsync(
                c, Name, now.AddDays(-40), "pg_blocking", lastCollection.AddHours(-3), 30, lastCollection, Tables, TestContext.Current.CancellationToken);
            var serverId = PerformanceMonitor.Common.ServerIdHelper.GetDeterministicHashCode(Name);

            const string Rows = "{\"status\":\"ok\",\"rows\":[1]}";
            const string Gated = "{\"status\":\"not_collected\",\"message\":\"x\"}";
            const string Empty = "{\"status\":\"empty\",\"message\":\"a healthy server\"}";

            Assert.Equal(Rows, await DarlingServerOfflineNote.EmptyAsync(ds, serverId, Name, now, Rows, TestContext.Current.CancellationToken));
            Assert.Equal(Gated, await DarlingServerOfflineNote.EmptyAsync(ds, serverId, Name, now, Gated, TestContext.Current.CancellationToken));
            var replaced = WindowFloorLiveHarness.Parse(
                await DarlingServerOfflineNote.EmptyAsync(ds, serverId, Name, now, Empty, TestContext.Current.CancellationToken));
            Assert.Equal("unavailable", replaced.GetProperty("status").GetString());
            Assert.DoesNotContain("healthy", replaced.GetProperty("message").GetString()!, StringComparison.Ordinal);
        });
    }

    /// <summary>The sweep's read of a target with nothing in the span also reads its newest collection, so the classifier can tell
    /// one that was offline before the restart from one that went quiet after it.</summary>
    [Fact]
    public async Task TheSweepRead_OfAQuietOfflineTarget_CarriesItsLastCollection_AgainstDevPostgres()
    {
        const string Name = "pg6155-sweep.example:appdb:pg";
        await WindowFloorLiveHarness.RunAsync(ConnectionString, ["pg_blocking"], [Name], Tables, async (c, ds, now) =>
        {
            var lastCollection = now.AddDays(-9);
            await WindowFloorLiveHarness.SeedServerAsync(
                c, Name, now.AddDays(-40), "pg_blocking", lastCollection.AddHours(-3), 30, lastCollection, Tables, TestContext.Current.CancellationToken);
            var serverId = PerformanceMonitor.Common.ServerIdHelper.GetDeterministicHashCode(Name);

            var reading = await FleetSweepEngine.ReadServerSignalsAsync(
                ds, serverId, Name, now.AddHours(-1), now, TestContext.Current.CancellationToken);

            Assert.Null(reading.ReadFault);
            Assert.False(reading.Signals.HasData);
            Assert.NotNull(reading.LastCollectionUtc);
            Assert.True(Math.Abs((reading.LastCollectionUtc!.Value - lastCollection).TotalMinutes) < 1);
            Assert.True(reading.WasOfflineBeforeRestart(now.AddMinutes(-3)));
        });
    }
}
