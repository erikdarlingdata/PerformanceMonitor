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
using System.IO;
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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7918 gated live round-trip (DARLING_TEST_PG) for which alert-history row <c>GET /api/alert-notebook</c> shows as
/// the alert a link points at. Two firings of one metric sit inside the match window; the one the link's dedup key
/// names is dismissed, the other is not. The page must open the dismissed firing and say it is dismissed, never the
/// other firing under the link's name. Every row is tagged with a per-class server name and deleted in cleanup, so the
/// shared store is left as it was found.
/// </summary>
[Collection("live-postgres")]
public sealed class AlertNotebookDismissedMatchLiveTests
{
    private const string ServerNamePrefix = "alert-notebook-dismissed-7918";
    private const int ServerId = -791801;

    /* A metric with no authored template, so the notebook takes the mechanical path and nothing beyond the match read
       depends on the store's content. */
    private const string Metric = "tempdb Space";
    private const string TargetKey = "7918-dismissed-firing";
    private const string OtherKey = "7918-live-firing";

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the alert-notebook dismissed-match live test.");
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

    private static string ContextWithDedup(string dedupKey) =>
        AlertContextSerializer.Serialize(new AlertContext { Incidents = new List<AlertIncident> { new(dedupKey, new[] { "obj" }) } });

    private static Task InsertAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime alertTimeUtc, string dedupKey, bool dismissed) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, $4, 90, 80, TRUE, 'webhook', NULL, FALSE, $5, NULL, $6)",
            DarlingMcpTestData.Naive(alertTimeUtc), ServerId, ServerNamePrefix, Metric, dismissed, ContextWithDedup(dedupKey));

    private static Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_alert_log WHERE server_name = $1", ServerNamePrefix);

    /// <summary>The web routes in loopback mode, the way <see cref="AlertHistoryDismissTests"/> hosts them: no credential
    /// gate in front, so the request reaches the notebook lambda and the store it is given.</summary>
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

    private static Task<HttpContext> GetAsync(TestServer server, string pathAndQuery) =>
        server.SendAsync(ctx =>
        {
            ctx.Request.Method = "GET";
            ctx.Request.Path = pathAndQuery[..pathAndQuery.IndexOf('?')];
            ctx.Request.QueryString = new QueryString(pathAndQuery[pathAndQuery.IndexOf('?')..]);
            ctx.Request.Headers.Host = "localhost";
        });

    /// <summary>The link names the dismissed firing by its dedup key. The notebook must show that firing, not the
    /// undismissed firing of the same metric twenty minutes earlier, and must say the firing is dismissed.</summary>
    [Fact]
    public async Task LinkToADismissedFiring_OpensThatFiring_NotTheNearestLiveOne()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedAsync(cs, ct);

        var bodySucceeded = false;
        try
        {
            var anchor = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-2));
            await InsertAsync(connection, ct, anchor, TargetKey, dismissed: true);
            await InsertAsync(connection, ct, anchor.AddMinutes(-20), OtherKey, dismissed: false);

            await using var postgres = OpenDataSource(cs);
            using var server = await BuildServerAsync(postgres, ct);

            var at = anchor.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);
            var ctx = await GetAsync(server,
                "/api/alert-notebook?server=&metric=" + Uri.EscapeDataString(Metric) + "&at=" + Uri.EscapeDataString(at) + "&dedup=" + Uri.EscapeDataString(TargetKey));

            Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
            using var reader = new StreamReader(ctx.Response.Body);
            using var doc = JsonDocument.Parse(await reader.ReadToEndAsync(ct));
            var alert = doc.RootElement.GetProperty("alert");

            Assert.Equal(JsonValueKind.Object, alert.ValueKind);
            var shown = DateTime.Parse(alert.GetProperty("alert_time").GetString()!, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
            Assert.Equal(anchor, shown);
            Assert.True(alert.TryGetProperty("dismissed", out var dismissed) && dismissed.GetBoolean(),
                "the alert the page shows is the dismissed firing, and the envelope must say so");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteRowsAsync);
        }
    }
}
