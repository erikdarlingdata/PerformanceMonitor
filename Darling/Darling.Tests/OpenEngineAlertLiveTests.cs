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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The PostgreSQL half of the attach seam (#7084 part 4): <see cref="PgAlertHistoryStore.FindOpenEngineAlertAsync"/> answers the
/// newest delivered, un-muted, non-digest engine fire in the last 60 minutes with no resolution row after it, and
/// <c>DarlingAlertReader.GetAttachedFindingsAsync</c> lists the findings attached to one alert.
/// </summary>
[Collection("live-postgres")]
public sealed class OpenEngineAlertLiveTests
{
    private const int Server = -391840;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task InsertAsync(NpgsqlConnection connection, string metric, DateTime at, bool sent, string type, bool muted = false, string? contextJson = null)
    {
        using var insert = new NpgsqlCommand(
            "INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, muted, context_json) " +
            "VALUES ($1, $2, 'attach-test', $3, 1, 1, $4, $5, $6, $7)", connection);
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(at, DateTimeKind.Unspecified) });
        insert.Parameters.AddWithValue(Server);
        insert.Parameters.AddWithValue(metric);
        insert.Parameters.AddWithValue(sent);
        insert.Parameters.AddWithValue(type);
        insert.Parameters.AddWithValue(muted);
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = (object?)contextJson ?? DBNull.Value });
        await insert.ExecuteNonQueryAsync(Ct);
    }

    private static async Task ClearAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM config_alert_log WHERE server_id = {Server};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task RunLiveAsync(Func<PgAlertHistoryStore, NpgsqlDataSource, NpgsqlConnection, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live attach test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(Ct);
        await PgMigrations.MigrateAsync(connection, Ct);
        await ClearAsync(connection, Ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            await body(new PgAlertHistoryStore(postgres, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds), postgres, connection);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) => await ClearAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task OpenMeansDeliveredUnmutedNonDigestInsideTheHourAndUnresolved()
    {
        await RunLiveAsync(async (store, _, connection) =>
        {
            var now = DateTime.UtcNow;
            var key = Server.ToString(System.Globalization.CultureInfo.InvariantCulture);

            /* A muted row, a digest row and a 90 minute old delivered row attract nothing. */
            await InsertAsync(connection, "High CPU", now.AddMinutes(-20), sent: false, type: "muted", muted: true);
            await InsertAsync(connection, "Deadlocks Detected", now.AddMinutes(-15), sent: false, type: "digest");
            await InsertAsync(connection, "Blocking Detected", now.AddMinutes(-90), sent: true, type: "email");
            Assert.Null(await store.FindOpenEngineAlertAsync(key, now));

            /* A delivered High CPU 20 minutes ago is open. */
            var firedAt = now.AddMinutes(-20);
            await InsertAsync(connection, "High CPU", firedAt, sent: true, type: "email");
            var open = await store.FindOpenEngineAlertAsync(key, now);
            Assert.Equal("High CPU", open!.Metric);
            Assert.Equal(AlertHistoryEscalationSeed.TruncateToMicroseconds(firedAt), open.AlertTimeUtc);

            /* A resolution row after it closes it. */
            await InsertAsync(connection, "CPU Resolved", now.AddMinutes(-5), sent: false, type: "not_applicable");
            Assert.Null(await store.FindOpenEngineAlertAsync(key, now));

            /* A key that is not a server id has no rows. */
            Assert.Null(await store.FindOpenEngineAlertAsync("not-a-server", now));
        });
    }

    [Fact]
    public async Task TheAlertsDetailListsExactlyTheFindingsAttachedToIt()
    {
        await RunLiveAsync(async (_, postgres, connection) =>
        {
            var firedAt = AlertHistoryEscalationSeed.TruncateToMicroseconds(DateTime.UtcNow.AddMinutes(-30));
            await InsertAsync(connection, "High CPU", firedAt, sent: true, type: "email");
            string Held(DateTime at) => AlertContextSerializer.Serialize(
                new AlertContext { AttachedTo = new AlertAttachedToDto("High CPU", at), Routing = new AlertRoutingDto("digest", "attached to High CPU") });
            await InsertAsync(connection, "Analysis: held one", firedAt.AddMinutes(5), sent: false, type: "digest", contextJson: Held(firedAt));
            await InsertAsync(connection, "Analysis: other alert", firedAt.AddMinutes(6), sent: false, type: "digest", contextJson: Held(firedAt.AddMinutes(-90)));
            await InsertAsync(connection, "Analysis: plain digest", firedAt.AddMinutes(7), sent: false, type: "digest",
                contextJson: AlertContextSerializer.Serialize(new AlertContext()));

            var found = await DarlingAlertReader.GetAttachedFindingsAsync(postgres, Server, "High CPU", firedAt, Ct);

            var one = Assert.Single(found);
            Assert.Equal("Analysis: held one", one.MetricName);
            Assert.Equal("attached to High CPU", one.Reason);
            Assert.Empty(await DarlingAlertReader.GetAttachedFindingsAsync(postgres, Server, "Failed Jobs", firedAt, Ct));
            Assert.DoesNotContain(found, f => f.MetricName.Contains("other"));
        });
    }
}
