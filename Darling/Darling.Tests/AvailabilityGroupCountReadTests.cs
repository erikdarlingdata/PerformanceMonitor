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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4189: the nav-gate count-only read never disagreeing with the topology read it replaces in
/// <c>refreshAgNav</c>, plus the source pin that <c>refreshAgNav</c> actually calls it.
/// </summary>
[Collection("live-postgres")]
public sealed class AvailabilityGroupCountReadTests
{
    private const string ServerName = "darling-ag-count-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>Plants TWO distinct AGs under one server, so a count/topology pair that "agree" by both
    /// landing on 0 or both landing on 1 does not pass this — <see cref="PerformanceMonitor.Darling.Storage.DarlingAgStatesReader.ReplicaGroupCountSql"/>'s
    /// doc explains why the database grain and the ORDER BY the topology read carries cannot change the
    /// number either read reports.</summary>
    [Fact]
    public async Task Count_MatchesTopologysAvailabilityGroupCount_ForTheSameSeededData()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live AG-count test.");

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
            var when = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);

            foreach (var agName in new[] { "AG_ONE", "AG_TWO" })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, operational_state_desc, connected_state_desc, recovery_health_desc, synchronization_health_desc, availability_mode_desc, failover_mode_desc, endpoint_url)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(when), ServerId, ServerName, agName, "AGNODE1", "PRIMARY",
                    "ONLINE", "CONNECTED", "ONLINE", "HEALTHY", "SYNCHRONOUS_COMMIT", "AUTOMATIC", "TCP://AGNODE1:5022");
            }

            var unfilteredCount = await DarlingAgReader.GetAvailabilityGroupCountAsync(postgres, null, ct);
            var unfilteredTopology = await DarlingAgReader.GetAgHealthAsync(postgres, null, DateTime.UtcNow, ct);
            Assert.Equal(2, unfilteredCount);
            Assert.Equal(unfilteredTopology.AvailabilityGroupCount, unfilteredCount);

            // $1 binds the same way on both reads (the server-filtered path /api/read/get_ag_health takes).
            var filteredCount = await DarlingAgReader.GetAvailabilityGroupCountAsync(postgres, ServerId, ct);
            var filteredTopology = await DarlingAgReader.GetAgHealthAsync(postgres, ServerId, DateTime.UtcNow, ct);
            Assert.Equal(2, filteredCount);
            Assert.Equal(filteredTopology.AvailabilityGroupCount, filteredCount);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The source pin: <c>refreshAgNav</c> calls the count-only endpoint, not <c>/api/ag</c>. A plain
    /// substring check rather than a function-body slice — the two calls differ before their closing quote
    /// (<c>/api/ag")</c> vs <c>/api/ag/count")</c>), so "contains the new call" and "does not contain the old
    /// one" are already exact, and app.js has exactly one <c>/api/ag*</c> call site to find either way.</summary>
    [Fact]
    public void RefreshAgNav_CallsTheCountEndpoint_NotTheFullTopologyEndpoint()
    {
        var appJs = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "app.js");

        Assert.Contains("apiGet(\"/api/ag/count\")", appJs, StringComparison.Ordinal);
        Assert.DoesNotContain("apiGet(\"/api/ag\")", appJs, StringComparison.Ordinal);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var sql = string.Join(" ", new[] { "ag_replica_states", "ag_database_replica_states" }
            .Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"))
            + $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
