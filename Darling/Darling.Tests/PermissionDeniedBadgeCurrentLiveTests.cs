// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7241: the Collection Health tab header counts the collectors the monitored server refuses NOW, the same count the
/// status bar shows, not every collector refused at some point in the 7-day window.
/// </summary>
// #1776 own-store: the live tests mint their own scratch database through ScratchPostgres.
[Collection("live-postgres")]
public sealed class PermissionDeniedBadgeCurrentLiveTests
{
    private const int GrantedServerId = -7241001;
    private const int RefusedServerId = -7241002;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheHeaderCount_IgnoresACollectorRefusedEarlierThatSucceededSince_AndCountsOneRefusedOnItsLatestRun_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live permission badge test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, GrantedServerId, "badge-granted", ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, RefusedServerId, "badge-refused", ct);

            var start = DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc).AddMinutes(-150);

            /* Server 1: refused for its first three runs, then the grant was added and every run since succeeded. */
            for (var i = 0; i < 12; i++)
            {
                await SeedAsync(connection, ct, GrantedServerId, "badge-granted", "agent_alerts", 1000 + i, start.AddMinutes(i * 5), i < 3 ? "PERMISSIONS" : "SUCCESS");
            }

            /* Server 2: one collector granted the same way, one refused on every run including the latest. */
            for (var i = 0; i < 12; i++)
            {
                await SeedAsync(connection, ct, RefusedServerId, "badge-refused", "agent_alerts", 2000 + i, start.AddMinutes(i * 5), i < 3 ? "PERMISSIONS" : "SUCCESS");
                await SeedAsync(connection, ct, RefusedServerId, "badge-refused", "msdb_jobs", 3000 + i, start.AddMinutes(i * 5), "PERMISSIONS");
            }

            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            Assert.Equal(0, await viewer.GetPermissionDeniedCollectorCountAsync(GrantedServerId, ct));
            Assert.Equal(1, await viewer.GetPermissionDeniedCollectorCountAsync(RefusedServerId, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static Task SeedAsync(NpgsqlConnection c, CancellationToken ct, int serverId, string serverName, string collector, long logId, DateTime atUtc, string status) =>
        DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message,
     rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, 100, $6, NULL, 5, 80, 20)",
            logId, serverId, serverName, collector, DarlingMcpTestData.Naive(atUtc), status);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var serverId in new[] { GrantedServerId, RefusedServerId })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", serverId);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", serverId);
        }
    }
}
