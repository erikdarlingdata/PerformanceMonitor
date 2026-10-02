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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Gated live round-trips (DARLING_TEST_PG) for the name Alert History SHOWS and FILTERS on. An analysis alert is
/// stored under the server's storage name (<c>host:database</c>) while an engine alert is stored under its display
/// name, so the web grid's Server filter, typed with the display name, hid every analysis row, and the Server
/// column showed two spellings for one server. The reads now return the registry display name for any row whose
/// <c>server_id</c> is registered. What is STORED does not change, and the Viewer's mute rules still match the
/// stored spelling through <see cref="ViewerAlertRow.StoredServerName"/>.
/// </summary>
[Collection("live-postgres")]
public sealed class AlertHistoryDisplayNameLiveTests
{
    private const int ServerId = -482201;
    private const string DisplayName = "Prod GP";
    private const string StorageName = "host-482201:db";
    private const string AnalysisMetric = "Analysis: cpu_pressure [482201aa]";
    private const string EngineMetric = "High CPU";

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the alert-history display-name live tests.");
        return connectionString!;
    }

    private static NpgsqlDataSource OpenDataSource(string connectionString) =>
        NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString);

    private static async Task SeedAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        await DeleteAsync(connection, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 15, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET server_name = EXCLUDED.server_name, display_name = EXCLUDED.display_name, is_enabled = TRUE",
            ServerId, StorageName, DisplayName, DarlingMcpTestData.Naive(now));

        /* The analysis family stores the storage name; the engine family stores the display name. */
        await InsertAlertAsync(connection, ct, now.AddMinutes(-2), StorageName, AnalysisMetric);
        await InsertAlertAsync(connection, ct, now.AddMinutes(-1), DisplayName, EngineMetric);
    }

    private static Task InsertAlertAsync(NpgsqlConnection connection, CancellationToken ct, DateTime alertTime, string storedName, string metric) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, $4, 1, 1, FALSE, 'none', NULL, FALSE, FALSE, NULL, NULL)",
            DarlingMcpTestData.Naive(alertTime), ServerId, storedName, metric);

    private static async Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_alert_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
    }

    [Fact]
    public async Task TheReader_ShowsTheDisplayName_OnAnalysisAndEngineRows_ForTheFleetAndForOneServer()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            await SeedAsync(connection, now, ct);
            await using var postgres = OpenDataSource(cs);

            var scoped = await DarlingAlertReader.GetAlertHistoryPageAsync(
                postgres, now.AddHours(-1), now.AddMinutes(1), ServerId, 50, includeDismissed: false, ct);
            Assert.Equal(2, scoped.Count);
            Assert.All(scoped, r => Assert.Equal(DisplayName, r.ServerName));

            var fleet = await DarlingAlertReader.GetAlertHistoryPageAsync(
                postgres, now.AddHours(-1), now.AddMinutes(1), null, 500, includeDismissed: false, ct);
            var ours = fleet.Where(r => r.ServerId == ServerId).ToList();
            Assert.Equal(2, ours.Count);
            Assert.All(ours, r => Assert.Equal(DisplayName, r.ServerName));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteAsync);
        }
    }

    [Fact]
    public async Task GetAlertHistory_ByDisplayName_ReturnsBothRows_EachNamedByTheDisplayName()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            await SeedAsync(connection, now, ct);
            await using var postgres = OpenDataSource(cs);

            var json = await DarlingMcpAlertTools.GetAlertHistory(postgres, server_name: DisplayName, hours_back: 1, limit: 50, cancellationToken: ct);
            using var doc = JsonDocument.Parse(json);
            var alerts = doc.RootElement.GetProperty("alerts").EnumerateArray().ToList();
            Assert.Equal(2, alerts.Count);
            Assert.All(alerts, a => Assert.Equal(DisplayName, a.GetProperty("server_name").GetString()));
            Assert.Contains(alerts, a => a.GetProperty("metric_name").GetString() == AnalysisMetric);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteAsync);
        }
    }

    [Fact]
    public async Task TheViewerGrid_ShowsTheDisplayName_WhileTheMuteContextKeepsTheStoredSpelling()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            await SeedAsync(connection, now, ct);
            await using var viewer = new ViewerDataService(cs);

            foreach (var rows in new[]
            {
                await viewer.GetAlertHistoryAsync(now.AddHours(-1), ServerId, 50, ct),
                (await viewer.GetAlertHistoryAsync(now.AddHours(-1), null, 500, ct)).Where(r => r.ServerId == ServerId).ToList(),
            })
            {
                Assert.Equal(2, rows.Count);
                Assert.All(rows, r => Assert.Equal(DisplayName, r.ServerName));

                var analysis = rows.Single(r => r.MetricName == AnalysisMetric);
                var engine = rows.Single(r => r.MetricName == EngineMetric);
                /* A mute rule is authored from, and judged against, the spelling the row was STORED with. */
                Assert.Equal(StorageName, analysis.StoredServerName);
                Assert.Equal(StorageName, analysis.ToMuteContext().ServerName);
                Assert.Equal(DisplayName, engine.StoredServerName);
                Assert.Equal(DisplayName, engine.ToMuteContext().ServerName);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteAsync);
        }
    }
}
