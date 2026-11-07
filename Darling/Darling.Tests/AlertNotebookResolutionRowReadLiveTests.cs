/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8095 item 3, gated live round-trips (DARLING_TEST_PG): the alert notebook's status for a row that is itself a resolution
/// ("Blocking Cleared", "Server Restored"), the row the Alert History "Open" link passes for a Cleared entry. The status reads
/// (<see cref="AlertNotebookEndpoint.ReadStatusRowsAsync"/>) look after the anchor for a resolution or a re-fire of the metric
/// they are given, so for a resolution row they found nothing and the notebook said "No resolution recorded" about the
/// resolution. For such a row the one read is the newest firing of the resolved metric at or before the anchor, on the row's
/// own server, and the status names it. Every row is tagged with a per-class server name and deleted in cleanup, so the shared
/// store is left as it was found.
/// </summary>
[Collection("live-postgres")]
public sealed class AlertNotebookResolutionRowReadLiveTests
{
    private const string ServerNamePrefix = "alert-notebook-resolution-row-8095";
    private const int ServerA = -809501;
    private const int ServerB = -809502;

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the alert-notebook resolution-row live tests.");
        return connectionString!;
    }

    private static async Task<NpgsqlConnection> OpenMigratedAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        return connection;
    }

    private static NpgsqlDataSource OpenDataSource(string connectionString) =>
        NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString);

    /// <summary>Naive-UTC whole-second anchor two hours in the past, so the seeded rows stay in the past and the wire text
    /// ("fired at T") round-trips exactly.</summary>
    private static DateTime Anchor() => DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-2));

    private static string Stamp(DateTime value) => value.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);

    private static Task InsertAsync(NpgsqlConnection connection, CancellationToken ct, DateTime alertTimeUtc, int serverId, string metric) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, $4, 0, 0, FALSE, 'none', NULL, FALSE, FALSE, NULL, NULL)",
            DarlingMcpTestData.Naive(alertTimeUtc), serverId, ServerNamePrefix + "-" + serverId.ToString(CultureInfo.InvariantCulture), metric);

    private static DarlingAlertReader.AlertHistoryReadRow MatchedRow(DateTime alertTime, int serverId, string metric) =>
        new(alertTime, serverId, ServerNamePrefix, metric, 0, 0, false, "none", null, false, null, false);

    /// <summary>The endpoint's own pair: the targeted reads, then the status arms over what they returned.</summary>
    private static async Task<(List<DarlingAlertReader.AlertHistoryReadRow> Rows, string Status)> StatusAsync(
        NpgsqlDataSource postgres, int serverId, string metric, DateTime anchor, CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        var matchedRow = MatchedRow(anchor, serverId, metric);
        var rows = await AlertNotebookEndpoint.ReadStatusRowsAsync(postgres, serverId, false, metric, anchor, now, matchedRow, ct);
        var status = await AlertNotebookEndpoint.ResolveStatusAsync(
            postgres, serverId, false, metric, anchor, now, rows, matchedRow, NullLogger.Instance, ct);
        return (rows, status);
    }

    private static Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_alert_log WHERE server_name LIKE $1", ServerNamePrefix + "%");

    [Fact]
    public async Task AClearedRow_ReadsAsTheResolutionOfTheNewestFiringBeforeIt_OnItsOwnServer()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedAsync(cs, ct);

        var bodySucceeded = false;
        try
        {
            var cleared = Anchor();
            var fired = cleared.AddMinutes(-25);
            await InsertAsync(connection, ct, cleared.AddHours(-3), ServerA, "Blocking Detected"); /* an older firing: not the one this row closed */
            await InsertAsync(connection, ct, fired, ServerA, "Blocking Detected");
            await InsertAsync(connection, ct, cleared, ServerA, "Blocking Cleared");
            await InsertAsync(connection, ct, cleared.AddMinutes(10), ServerA, "Blocking Detected"); /* a later firing: after the row, not before it */
            await InsertAsync(connection, ct, cleared.AddMinutes(-5), ServerB, "Blocking Detected"); /* another server's firing: not this server's */

            await using var postgres = OpenDataSource(cs);
            var (rows, status) = await StatusAsync(postgres, ServerA, "Blocking Cleared", cleared, ct);

            var row = Assert.Single(rows);
            Assert.Equal("Blocking Detected", row.MetricName);
            Assert.Equal(fired, row.AlertTime);
            Assert.Equal("Resolution of the Blocking Detected alert that fired at " + Stamp(fired), status);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task ARecoveryRow_ReadsAsTheResolutionOfTheFiringItClosed()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedAsync(cs, ct);

        var bodySucceeded = false;
        try
        {
            var restored = Anchor();
            var fired = restored.AddMinutes(-10);
            await InsertAsync(connection, ct, fired, ServerA, "Server Unreachable");
            await InsertAsync(connection, ct, restored, ServerA, "Server Restored");

            await using var postgres = OpenDataSource(cs);
            var (_, status) = await StatusAsync(postgres, ServerA, "Server Restored", restored, ct);

            Assert.Equal("Resolution of the Server Unreachable alert that fired at " + Stamp(fired), status);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task AClearedRow_WithNoFiringInTheStore_StillReadsAsAResolution()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedAsync(cs, ct);

        var bodySucceeded = false;
        try
        {
            var cleared = Anchor();
            await InsertAsync(connection, ct, cleared, ServerA, "Blocking Cleared");

            await using var postgres = OpenDataSource(cs);
            var (rows, status) = await StatusAsync(postgres, ServerA, "Blocking Cleared", cleared, ct);

            Assert.Empty(rows);
            Assert.Equal("Resolution of a Blocking Detected alert", status);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteRowsAsync);
        }
    }
}
