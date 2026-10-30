// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6828: for one server at one time, every surface that counts collectors bands each of them with the shared classifier and counts a
/// WARNING collector as a warning, never as healthy. One seeded server carries a HEALTHY collector, an <c>attention</c> capture that produced
/// rows and then went quiet (HEALTHY, the normal answer for a capture that stores a row only when a client times out), a polled collector that
/// produced and then went quiet (WARNING) and an Always On collector on a server with Always On off (not collected). The
/// <c>get_collection_health</c> rows, the fleet card's counts, the Viewer's fleet read, its Overview card counts and its Collection
/// Health grid must all give the same numbers.
/// </summary>
// #1776 own-store: the live tests mint their own scratch database through ScratchPostgres.
[Collection("live-postgres")]
public sealed class CollectorCountsAgreeAcrossSurfacesLiveTests
{
    private const int ServerId = -6828001;
    private const string ServerName = "counts-agree";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* The newest server_properties row says Always On is off by a FALSE flag or by a NULL one: the per-server read collapses NULL to false and
       reads that as off, so the fleet card and the Viewer must too. An older row that said TRUE must not matter either way. */
    [Theory]
    [InlineData(false)]
    [InlineData(null)]
    public async Task OneServer_GivesTheSameCollectorCounts_FromTheMcpRead_TheFleetCard_AndTheViewer_AgainstDevPostgres(bool? newestHadrEnabled)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collector-count agreement test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_monitored_servers (server_id, name, host, is_enabled) VALUES ($1, $2, $2, TRUE)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ServerId, ServerName);

            /* Always On is off on this server: the newest properties row says FALSE or NULL, an older one said TRUE. */
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition, is_hadr_enabled)
VALUES (6828000, $1, $2, $3, 'Edition', '15.0.2000.5', 'RTM', 3, TRUE)", DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-90)), ServerId, ServerName);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition, is_hadr_enabled)
VALUES (6828001, $1, $2, $3, 'Edition', '15.0.2000.5', 'RTM', 3, $4::boolean)", DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-30)), ServerId, ServerName, newestHadrEnabled);

            /* Twenty one-minute runs ending two minutes ago. The first twelve of a collector that "stops" store rows, the last eight store none. */
            var end = DateTime.UtcNow.AddMinutes(-2);
            long logId = 6_828_000;
            for (var i = 0; i < 20; i++)
            {
                var at = end.AddMinutes(i - 19);
                var producing = i < 12;
                await SeedAsync(connection, ct, "wait_stats", logId++, at, 5);
                await SeedAsync(connection, ct, "attention", logId++, at, producing ? 3 : 0);
                await SeedAsync(connection, ct, "memory_grant_stats", logId++, at, producing ? 5 : 0);
                await SeedAsync(connection, ct, "ag_replica_states", logId++, at, 0);
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* get_collection_health: one row per collector, counted by status. */
            var json = await DarlingMcpDataTools.GetCollectionHealth(postgres, ServerName, cancellationToken: ct);
            using var doc = JsonDocument.Parse(json);
            var statuses = doc.RootElement.GetProperty("collectors").EnumerateArray()
                .Select(c => c.GetProperty("status").GetString()!).ToList();
            var mcp = new CollectorHealthClassifier.BandTally(
                statuses.Count(s => s == CollectorHealthClassifier.Healthy),
                statuses.Count(s => s == CollectorHealthClassifier.Warning),
                statuses.Count(s => s == CollectorHealthClassifier.Failing),
                statuses.Count(s => s == "not_collected"),
                statuses.Count(s => s == CollectorHealthClassifier.NoPermissions || s == CollectorHealthClassifier.ExtensionMissing),
                statuses.Count);
            Assert.Equal(new CollectorHealthClassifier.BandTally(2, 1, 0, 1, 0, 4), mcp);

            /* The fleet card's counts. */
            var read = typeof(DarlingFleetReader).GetMethod("ReadFailingCollectorCountsAsync", BindingFlags.NonPublic | BindingFlags.Static)
                ?? throw new InvalidOperationException("ReadFailingCollectorCountsAsync not found");
            var counts = await (Task<Dictionary<int, DarlingFleetReader.CollectorCounts>>)read.Invoke(null, [postgres, DateTime.UtcNow, ct])!;
            var card = counts[ServerId];
            Assert.Equal(mcp, new CollectorHealthClassifier.BandTally(card.Healthy, card.Warning, card.Failing, card.NotCollected, card.NoPermission, card.Total));

            /* The Viewer's fleet read (the Overview cards and the status bar) and its Collection Health grid. */
            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var fleetRows = (await viewer.GetFleetCollectionHealthByServerAsync(ct))[ServerId];
            var viewerFleet = new CollectorHealthClassifier.BandTally();
            foreach (var row in fleetRows)
            {
                viewerFleet = viewerFleet.Add(row.CollectorName, row.HealthStatus, row.ServerAlwaysOnOff, engineGated: false);
            }

            Assert.Equal(mcp, viewerFleet);

            var grid = (await viewer.GetCollectionHealthAsync(ServerId, ct)).ToDictionary(r => r.CollectorName, r => r.HealthStatus);
            Assert.Equal(CollectorHealthClassifier.Healthy, grid["attention"]);
            Assert.Equal(CollectorHealthClassifier.Warning, grid["memory_grant_stats"]);
            Assert.Equal(CollectorHealthClassifier.Healthy, grid["wait_stats"]);

            /* The Viewer's Overview card counts, the number its Healthy / Warning / Failing text is built from. */
            var cardRead = typeof(ViewerDataService).GetMethod("GetCollectorHealthCountsAsync", BindingFlags.NonPublic | BindingFlags.Instance)
                ?? throw new InvalidOperationException("GetCollectorHealthCountsAsync not found");
            var (healthy, warning, failing, notCollected, noPermission, total, _, _) =
                await (Task<(int, int, int, int, int, int, string?, string?)>)cardRead.Invoke(viewer, [ServerId, ct])!;
            Assert.Equal(mcp, new CollectorHealthClassifier.BandTally(healthy, warning, failing, notCollected, noPermission, total));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static Task SeedAsync(NpgsqlConnection c, CancellationToken ct, string collector, long logId, DateTime atUtc, int rows) =>
        DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message,
     rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, 100, 'SUCCESS', NULL, $6, 80, 20)",
            logId, ServerId, ServerName, collector, DarlingMcpTestData.Naive(atUtc), rows);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM server_properties WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
