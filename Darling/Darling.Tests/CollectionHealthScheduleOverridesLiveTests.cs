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
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4999: the collection-health reads judge each collector against the interval it is scheduled at on its server, read
/// from <c>config.config_collector_schedules</c>. The schedule read fails open to the shipped cadences (a collection
/// health read must still answer when it cannot read the table), so every other test of the health reads would
/// still pass if <c>DarlingDataReader.ScheduleOverridesSql</c> broke. This one seeds the rows and reads the health back,
/// so a read that falls back shows up as the wrong band.
///
/// <para>Three five-minute collectors, each with a success six hours old: past the four hours a five-minute collector
/// goes STALE at, well inside the 18 hours (one and a half intervals) a collector scheduled every 720 minutes gets.
/// The first has a server row (720) AND a fleet row (5): the server row wins, so HEALTHY. The second has only a fleet
/// row (720): HEALTHY. The third has no row: the shipped five minutes, so STALE. The same server is read through the
/// MCP read and through the viewer's server tab and its fleet breakdown.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class CollectionHealthScheduleOverridesLiveTests
{
    private const string ServerName = "darling-collection-health-schedule-overrides-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const string ServerRowWins = "memory_clerks";
    private const string FleetRowApplies = "memory_pressure_events";
    private const string ShippedDefault = "session_summary_stats";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task EachCollectorIsJudgedAgainstTheServerRow_ThenTheFleetRow_ThenTheShippedDefault()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live collection-health schedule-override test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_monitored_servers (server_id, name, host, is_enabled) VALUES ($1, $2, $2, TRUE)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ServerId, ServerName);

            var now = DateTime.UtcNow;
            foreach (var collector in new[] { ServerRowWins, FleetRowApplies, ShippedDefault })
            {
                for (var i = 0; i < 10; i++)
                {
                    await InsertSuccessAsync(connection, collector, now.AddHours(-6).AddMinutes(-5 * i), ct);
                }
            }

            /* The server's own row beats the fleet's; the fleet's row alone applies; the third has neither. */
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled)
VALUES ($1, $2, 720, NULL, TRUE)", ServerId, ServerRowWins);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled)
VALUES (NULL, $1, 5, NULL, TRUE)", ServerRowWins);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled)
VALUES (NULL, $1, 720, NULL, TRUE)", FleetRowApplies);

            Assert.Equal(5, CollectorScheduleDefaults.All[ServerRowWins].FrequencyMinutes);
            Assert.Equal(5, CollectorScheduleDefaults.All[FleetRowApplies].FrequencyMinutes);
            Assert.Equal(5, CollectorScheduleDefaults.All[ShippedDefault].FrequencyMinutes);

            /* The MCP per-server read. */
            var mcp = await DarlingDataReader.GetCollectionHealthAsync(
                postgres, ServerId, DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-7)), ct);
            AssertJudged("MCP", mcp.ToDictionary(r => r.CollectorName, r => (r.FrequencyMinutes, r.HealthStatus)));

            /* The viewer's server tab, and the per-server breakdown the Overview cards and status bar count. */
            await using var viewer = new ViewerDataService(cs!);
            var tab = await viewer.GetCollectionHealthAsync(ServerId, ct);
            AssertJudged("viewer server tab", tab.ToDictionary(r => r.CollectorName, r => (r.EffectiveFrequencyMinutes ?? 0, r.HealthStatus)));

            var byServer = await viewer.GetFleetCollectionHealthByServerAsync(ct);
            AssertJudged("viewer fleet breakdown", byServer[ServerId].ToDictionary(r => r.CollectorName, r => (r.EffectiveFrequencyMinutes ?? 0, r.HealthStatus)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static void AssertJudged(string surface, System.Collections.Generic.Dictionary<string, (int FrequencyMinutes, string Status)> rows)
    {
        Assert.True(rows.TryGetValue(ServerRowWins, out var server), $"{surface}: {ServerRowWins} is missing");
        Assert.Equal((720, CollectorHealthClassifier.Healthy), server);

        Assert.True(rows.TryGetValue(FleetRowApplies, out var fleet), $"{surface}: {FleetRowApplies} is missing");
        Assert.Equal((720, CollectorHealthClassifier.Healthy), fleet);

        Assert.True(rows.TryGetValue(ShippedDefault, out var shipped), $"{surface}: {ShippedDefault} is missing");
        Assert.Equal((5, CollectorHealthClassifier.Stale), shipped);
    }

    private static async Task InsertSuccessAsync(NpgsqlConnection connection, string collectorName, DateTime collectionTime, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected, error_message)
VALUES ($1,$2,$3,$4,$5,'SUCCESS',20,10,NULL)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(collectorName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collection_log WHERE server_id = {ServerId}; "
            + $"DELETE FROM config_collector_schedules WHERE collector_name IN ('{ServerRowWins}', '{FleetRowApplies}', '{ShippedDefault}') AND (server_id IS NULL OR server_id = {ServerId}); "
            + $"DELETE FROM config_monitored_servers WHERE server_id = {ServerId}; "
            + $"DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
