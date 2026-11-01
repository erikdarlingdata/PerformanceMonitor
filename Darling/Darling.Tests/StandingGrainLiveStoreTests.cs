/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7068, live against PostgreSQL: <see cref="PgAlertHistoryStore.GetStandingAsync"/> returns every open fire so two availability
/// group databases behind each seed from their own row, a clear naming one closes only it, and a single-key fleet metric is read
/// from the server_id 0 bucket while a per-job metric is not. The same cases as Lite's <c>StandingAgSyncRestartSeedTests</c>.
/// </summary>
[Collection("live-postgres")]
public sealed class StandingGrainLiveStoreTests
{
    private const int Server = -571520;
    private const string Ag = "AG1";
    private const string Replica = "NODE2";
    private static readonly DateTime Now = new DateTime(2026, 11, 1, 12, 0, 0, DateTimeKind.Unspecified).AddTicks(10 * 123);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task InsertAsync(
        NpgsqlConnection connection, DateTime at, string metric, string? context, string? detail, int serverId = Server)
    {
        using var cmd = new NpgsqlCommand(@"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error, muted, detail_text, context_json)
VALUES ($1, $2, 'Srv', $3, 0, 0, true, 'email', NULL, false, $4, $5)", connection);
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = at });
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(metric);
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = (object?)detail ?? DBNull.Value });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = (object?)context ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM config_alert_log WHERE server_id IN ({Server}, 0);", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task RunLiveAsync(Func<PgAlertHistoryStore, NpgsqlConnection, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live standing-incident store test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(Ct);
        await PgMigrations.MigrateAsync(connection, Ct);
        await DeleteTestRowsAsync(connection, Ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            await body(new PgAlertHistoryStore(postgres, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds), connection);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public Task EveryOpenFireIsReturned_AndAClearNamingOneDatabaseClosesOnlyIt() => RunLiveAsync(async (store, c) =>
    {
        var ctxA = AlertContextSerializer.Serialize(AgAlertContexts.ForDatabase("SalesA", Ag, Replica));
        var ctxB = AlertContextSerializer.Serialize(AgAlertContexts.ForDatabase("SalesB", Ag, Replica));
        await InsertAsync(c, Now.AddHours(-9), "AG Sync Fell Behind", ctxA, "a");
        await InsertAsync(c, Now.AddHours(-5), "AG Sync Fell Behind", ctxB, "b");
        var titles = new[] { "AG Sync Recovered" };
        var key = Server.ToString(System.Globalization.CultureInfo.InvariantCulture);

        var standing = await store.GetStandingAsync(key, "AG Sync Fell Behind", titles, null, Now);
        Assert.Equal(2, standing.Fires.Count);
        bool OwnsA(AlertHistoryFire f) => AgAlertContexts.OwnsSyncFire(f.ContextJson, Ag, "SalesA", Replica);
        bool OwnsB(AlertHistoryFire f) => AgAlertContexts.OwnsSyncFire(f.ContextJson, Ag, "SalesB", Replica);
        Assert.NotNull(standing.OpenFor(OwnsA));
        Assert.NotNull(standing.OpenFor(OwnsB));
        Assert.Equal(Now.AddHours(-5), (await store.GetOpenFireAsync(key, "AG Sync Fell Behind", titles, null, Now))!.FireAtUtc);

        await InsertAsync(c, Now.AddHours(-7), "AG Sync Recovered", null, $"SRV: database SalesA in AG {Ag} on replica {Replica} has caught up with the primary");
        standing = await store.GetStandingAsync(key, "AG Sync Fell Behind", titles, null, Now);
        Assert.Null(standing.OpenFor(OwnsA, x => AgAlertContexts.OwnsSyncClear(x.DetailText, Ag, "SalesA", Replica)));
        Assert.NotNull(standing.OpenFor(OwnsB, x => AgAlertContexts.OwnsSyncClear(x.DetailText, Ag, "SalesB", Replica)));
    });

    [Fact]
    public Task ASingleKeyFleetMetricIsReadFromTheZeroBucket_AndAPerJobMetricIsNot() => RunLiveAsync(async (store, c) =>
    {
        await InsertAsync(c, Now.AddDays(-2), "Collection Falling Behind", null, "fleet", serverId: 0);
        await InsertAsync(c, Now.AddDays(-2), "Store Job Over Cadence", null, "job 1", serverId: 0);

        Assert.NotNull(await store.GetOpenFireAsync("fleetgate", "Collection Falling Behind", new[] { "Collection Falling Behind Cleared" }, null, Now));
        Assert.Null(await store.GetOpenFireAsync("store:job:1", "Store Job Over Cadence", new[] { "Store Job Cadence Recovered" }, null, Now));

        await InsertAsync(c, Now.AddDays(-1), "Collection Falling Behind Cleared", null, "clear", serverId: 0);
        Assert.Null(await store.GetOpenFireAsync("fleetgate", "Collection Falling Behind", new[] { "Collection Falling Behind Cleared" }, null, Now));
    });
}
