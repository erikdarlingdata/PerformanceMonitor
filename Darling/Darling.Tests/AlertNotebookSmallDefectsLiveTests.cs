/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8555 gated live round-trip (DARLING_TEST_PG), the alert notebook's small defects over the real route. A Capture Restored
/// row reads as the resolution of the Capture Down before it, and the Capture Down reads Resolved. An External alert whose source
/// is not a monitored server keeps its own card and draws no per-server cell and no "Could not resolve server" note. The CPU
/// Resolved notebook follows the target's engine, so a SQL Server target has no Performance Insights cell. Rows are tagged with
/// per-class server names and metric names and removed in cleanup.
/// </summary>
[Collection("live-postgres")]
public sealed class AlertNotebookSmallDefectsLiveTests
{
    // #1776 own-store: every row is keyed by a server name or an external source private to this class and deleted in cleanup.
    private const string SqlServerName = "alert-notebook-small-8555-sql";
    private const string PgServerName = "alert-notebook-small-8555-pg";
    private const string ExternalSource = "alert-notebook-small-8555-src";
    private const string ExternalMetric = "External:" + ExternalSource + ":probe";

    private static readonly int SqlServerId = ServerIdHelper.GetDeterministicHashCode(SqlServerName);
    private static readonly int PgServerId = ServerIdHelper.GetDeterministicHashCode(PgServerName);

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the small alert-notebook defects live test.");
        return connectionString!;
    }

    private static NpgsqlDataSource OpenDataSource(string connectionString) =>
        NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "config_alert_log", "servers" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = ANY($1)", new[] { PgServerId, SqlServerId });
        }

        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_alert_log WHERE metric_name LIKE $1", "External:" + ExternalSource + ":%");
    }

    private static Task SeedServerAsync(NpgsqlConnection connection, DateTime alertTimeUtc, string engineKind, int serverId, string serverName, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, engine_kind, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, $3, $4, $4)",
            serverId, serverName, engineKind, DarlingMcpTestData.Naive(alertTimeUtc.AddDays(-30)));

    private static Task SeedAlertAsync(NpgsqlConnection connection, DateTime alertTimeUtc, int serverId, string serverName, string metric, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, $4, 1, 1, TRUE, 'webhook', NULL, FALSE, FALSE, NULL, NULL)",
            DarlingMcpTestData.Naive(alertTimeUtc), serverId, serverName, metric);

    private static async Task<TestServer> BuildServerAsync(NpgsqlDataSource postgres, CancellationToken ct)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(postgres);
        var app = builder.Build();
        DarlingWebEndpoints.MapAll(app, postgres, new CollectorRuntimeState(), new CapturingTestLogger());
        await app.StartAsync(ct);
        return app.GetTestServer();
    }

    private static string Iso(DateTime utc) => utc.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);

    /// <summary>The notebook envelope for <paramref name="metric"/> at <paramref name="alertTimeUtc"/> on <paramref name="serverName"/>.</summary>
    private static async Task<JsonElement> NotebookAsync(TestServer server, string serverName, string metric, DateTime alertTimeUtc, CancellationToken ct)
    {
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "GET";
            c.Request.Path = "/api/alert-notebook";
            c.Request.QueryString = new QueryString(
                "?server=" + Uri.EscapeDataString(serverName) + "&metric=" + Uri.EscapeDataString(metric) + "&at=" + Uri.EscapeDataString(Iso(alertTimeUtc)));
            c.Request.Headers.Host = "localhost";
        });
        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        using var reader = new StreamReader(ctx.Response.Body);
        using var doc = JsonDocument.Parse(await reader.ReadToEndAsync(ct));
        return doc.RootElement.Clone();
    }

    private static string[] CellTypes(JsonElement notebook) =>
        notebook.GetProperty("definition").GetProperty("cells").EnumerateArray().Select(c => c.GetProperty("type").GetString()!).ToArray();

    private static string[] ReadNames(JsonElement notebook) =>
        notebook.GetProperty("definition").GetProperty("cells").EnumerateArray()
            .Where(c => c.GetProperty("type").GetString() == "read").Select(c => c.GetProperty("read").GetString()!).ToArray();

    private static async Task WithStoreAsync(Func<NpgsqlConnection, TestServer, DateTime, CancellationToken, Task> body)
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await using var postgres = OpenDataSource(cs);
            using var server = await BuildServerAsync(postgres, ct);
            var alertTime = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-1));
            await body(connection, server, alertTime, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Theory]
    [InlineData("Capture Down", "Capture Restored")]
    [InlineData("Capture Down", "Capture Cleared")]
    [InlineData("Collection Stopped", "Collection Resumed")]
    public async Task ASelfMonitorRecovery_IsMatchedToItsFiring_InBothDirections(string firing, string recovery)
    {
        await WithStoreAsync(async (connection, server, alertTime, ct) =>
        {
            var recoveredAt = alertTime.AddSeconds(75);
            await SeedServerAsync(connection, alertTime, MonitoredEngineKind.SqlServer, SqlServerId, SqlServerName, ct);
            await SeedAlertAsync(connection, alertTime, SqlServerId, SqlServerName, firing, ct);
            await SeedAlertAsync(connection, recoveredAt, SqlServerId, SqlServerName, recovery, ct);

            var recoveryRow = await NotebookAsync(server, SqlServerName, recovery, recoveredAt, ct);
            Assert.Equal("Resolution of the " + firing + " alert that fired at " + Iso(alertTime), recoveryRow.GetProperty("status").GetString());

            var firingRow = await NotebookAsync(server, SqlServerName, firing, alertTime, ct);
            Assert.Equal("Resolved at " + Iso(recoveredAt), firingRow.GetProperty("status").GetString());
        });
    }

    [Fact]
    public async Task AnExternalAlertFromAnUnmonitoredSource_KeepsItsCard_AndDrawsNoPerServerCellAndNoResolveBanner()
    {
        await WithStoreAsync(async (connection, server, alertTime, ct) =>
        {
            await SeedAlertAsync(connection, alertTime, 0, ExternalSource, ExternalMetric, ct);
            await SeedAlertAsync(connection, alertTime.AddMinutes(5), 0, ExternalSource, ExternalMetric + " Resolved", ct);

            var notebook = await NotebookAsync(server, ExternalSource, ExternalMetric, alertTime, ct);

            Assert.Equal(JsonValueKind.Object, notebook.GetProperty("alert").ValueKind);
            Assert.Equal(ExternalMetric, notebook.GetProperty("alert").GetProperty("metric_name").GetString());
            Assert.Empty(notebook.GetProperty("notes").EnumerateArray());
            Assert.Equal(new[] { "header", "status" }, CellTypes(notebook));
            Assert.Equal("Resolved at " + Iso(alertTime.AddMinutes(5)), notebook.GetProperty("status").GetString());
        });
    }

    [Fact]
    public async Task AnExternalAlertFromAMonitoredServer_StillGetsItsPerServerCells()
    {
        await WithStoreAsync(async (connection, server, alertTime, ct) =>
        {
            await SeedServerAsync(connection, alertTime, MonitoredEngineKind.SqlServer, SqlServerId, SqlServerName, ct);
            var metric = "External:" + ExternalSource + ":probe-monitored";
            await SeedAlertAsync(connection, alertTime, SqlServerId, SqlServerName, metric, ct);

            var notebook = await NotebookAsync(server, SqlServerName, metric, alertTime, ct);

            Assert.Equal(
                new[] { "get_collection_health", "get_server_summary" },
                ReadNames(notebook).OrderBy(n => n, StringComparer.Ordinal).ToArray());
        });
    }

    [Fact]
    public async Task CpuResolved_OnASqlServerTarget_HasNoPerformanceInsightsCell_AndOnAPostgresTargetHasNoSqlServerRead()
    {
        await WithStoreAsync(async (connection, server, alertTime, ct) =>
        {
            await SeedServerAsync(connection, alertTime, MonitoredEngineKind.SqlServer, SqlServerId, SqlServerName, ct);
            await SeedAlertAsync(connection, alertTime, SqlServerId, SqlServerName, "CPU Resolved", ct);
            await SeedServerAsync(connection, alertTime, MonitoredEngineKind.Postgres, PgServerId, PgServerName, ct);
            await SeedAlertAsync(connection, alertTime, PgServerId, PgServerName, "CPU Resolved", ct);

            var sql = await NotebookAsync(server, SqlServerName, "CPU Resolved", alertTime, ct);
            Assert.Equal(new[] { "get_cpu_utilization", "get_top_queries_by_cpu", "get_cpu_scheduler_pressure" }, ReadNames(sql));

            var pg = await NotebookAsync(server, PgServerName, "CPU Resolved", alertTime, ct);
            Assert.Equal(new[] { "get_pg_cpu_utilization", "get_pg_top_queries" }, ReadNames(pg));
        });
    }
}
