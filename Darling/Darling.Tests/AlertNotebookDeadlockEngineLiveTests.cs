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
/// #8553 gated live round-trip (DARLING_TEST_PG): the alert notebook a deadlock alert links to follows the target's
/// engine. A PostgreSQL target with one stored deadlock gets the PostgreSQL deadlock reads (firing and cleared), one of
/// them lists the deadlock, and no cell answers "not collected" or draws a SQL Server panel. A SQL Server target keeps the
/// cells it always had. Rows are tagged with per-class server names and removed in cleanup.
/// </summary>
[Collection("live-postgres")]
public sealed class AlertNotebookDeadlockEngineLiveTests
{
    // #1776 own-store: every row is keyed by a server name private to this class and deleted in cleanup.
    private const string PgServerName = "alert-notebook-deadlock-8553-pg";
    private const string SqlServerName = "alert-notebook-deadlock-8553-sql";
    private const string DeadlockHash = "hash-8553-notebook";
    private const string VictimStatement = "UPDATE notebook_8553 SET x = ?";

    private static readonly int PgServerId = ServerIdHelper.GetDeterministicHashCode(PgServerName);
    private static readonly int SqlServerId = ServerIdHelper.GetDeterministicHashCode(SqlServerName);

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the deadlock alert-notebook engine live test.");
        return connectionString!;
    }

    private static NpgsqlDataSource OpenDataSource(string connectionString) =>
        NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "config_alert_log", "pg_deadlocks", "collection_log", "servers" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = ANY($1)", new[] { PgServerId, SqlServerId });
        }
    }

    private static async Task SeedAsync(NpgsqlConnection connection, DateTime alertTimeUtc, string engineKind, int serverId, string serverName, string metric, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, engine_kind, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, $3, $4, $4)",
            serverId, serverName, engineKind, DarlingMcpTestData.Naive(alertTimeUtc.AddDays(-30)));

        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, $4, 1, 1, TRUE, 'webhook', NULL, FALSE, FALSE, NULL, NULL)",
            DarlingMcpTestData.Naive(alertTimeUtc), serverId, serverName, metric);
    }

    private static Task SeedPgDeadlockAsync(NpgsqlConnection connection, DateTime occurredUtc, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO pg_deadlocks (collection_id, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count, deadlock_hash, lock_modes, resources, victim_statement, graph_text)
VALUES ($1,$2,$3,$4,$5,4242,2,$6,'ShareLock','relation',$7,'graph text')",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(occurredUtc), PgServerId, PgServerName,
            DarlingMcpTestData.Naive(occurredUtc), DeadlockHash, VictimStatement);

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

    private static async Task<string> GetBodyAsync(TestServer server, string path, string query, CancellationToken ct)
    {
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "GET";
            c.Request.Path = path;
            c.Request.QueryString = new QueryString(query);
            c.Request.Headers.Host = "localhost";
        });
        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        using var reader = new StreamReader(ctx.Response.Body);
        return await reader.ReadToEndAsync(ct);
    }

    private static string Iso(DateTime utc) => utc.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);

    /// <summary>The notebook's cells for <paramref name="metric"/> at <paramref name="alertTimeUtc"/> on <paramref name="serverName"/>.</summary>
    private static async Task<List<JsonElement>> CellsAsync(TestServer server, string serverName, string metric, DateTime alertTimeUtc, CancellationToken ct)
    {
        var body = await GetBodyAsync(server, "/api/alert-notebook",
            "?server=" + Uri.EscapeDataString(serverName) + "&metric=" + Uri.EscapeDataString(metric) + "&at=" + Uri.EscapeDataString(Iso(alertTimeUtc)), ct);
        using var doc = JsonDocument.Parse(body);
        return doc.RootElement.GetProperty("definition").GetProperty("cells").EnumerateArray().Select(c => c.Clone()).ToList();
    }

    /// <summary>Runs a read cell through the same <c>/api/read</c> route the page dispatches it to, binding its params.</summary>
    private static Task<string> RunReadCellAsync(TestServer server, JsonElement cell, CancellationToken ct)
    {
        var query = string.Join("&", cell.GetProperty("params").EnumerateObject()
            .Select(p => Uri.EscapeDataString(p.Name) + "=" + Uri.EscapeDataString(p.Value.GetString()!)));
        return GetBodyAsync(server, "/api/read/" + cell.GetProperty("read").GetString(), "?" + query, ct);
    }

    private static string[] ReadNames(IEnumerable<JsonElement> cells) =>
        cells.Where(c => c.GetProperty("type").GetString() == "read").Select(c => c.GetProperty("read").GetString()!).ToArray();

    private static async Task WithStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, TestServer, DateTime, CancellationToken, Task> body)
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
            await body(connection, postgres, server, alertTime, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Theory]
    [InlineData("Deadlocks Detected")]
    [InlineData("Deadlocks Cleared")]
    public async Task PostgresTarget_NotebookIsThePostgresDeadlockReads_AndListsTheStoredDeadlock(string metric)
    {
        await WithStoreAsync(async (connection, _, server, alertTime, ct) =>
        {
            await SeedAsync(connection, alertTime, MonitoredEngineKind.Postgres, PgServerId, PgServerName, metric, ct);
            await SeedPgDeadlockAsync(connection, alertTime, ct);

            var cells = await CellsAsync(server, PgServerName, metric, alertTime, ct);

            Assert.Equal(new[] { "get_pg_deadlocks", "get_pg_deadlock_detail" }, ReadNames(cells));
            Assert.DoesNotContain(cells, c => c.GetProperty("type").GetString() == "panel");

            var answers = new List<string>();
            foreach (var cell in cells.Where(c => c.GetProperty("type").GetString() == "read"))
            {
                var answer = await RunReadCellAsync(server, cell, ct);
                Assert.DoesNotContain("not_collected", answer, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("does not apply", answer, StringComparison.OrdinalIgnoreCase);
                answers.Add(answer);
            }

            Assert.Contains(DeadlockHash, answers[0], StringComparison.Ordinal);
            Assert.Contains(DeadlockHash, answers[1], StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task SqlServerTarget_DeadlockNotebooksKeepTheirSqlServerCells()
    {
        await WithStoreAsync(async (connection, _, server, alertTime, ct) =>
        {
            await SeedAsync(connection, alertTime, MonitoredEngineKind.SqlServer, SqlServerId, SqlServerName, "Deadlocks Detected", ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, 'Deadlocks Cleared', 1, 1, TRUE, 'webhook', NULL, FALSE, FALSE, NULL, NULL)",
                DarlingMcpTestData.Naive(alertTime), SqlServerId, SqlServerName);

            var firing = await CellsAsync(server, SqlServerName, "Deadlocks Detected", alertTime, ct);
            Assert.Equal(
                new[] { "header", "status", "read", "panel", "panel", "read" },
                firing.Select(c => c.GetProperty("type").GetString()!).ToArray());
            Assert.Equal(new[] { "get_deadlock_detail", "get_deadlock_trend" }, ReadNames(firing));
            Assert.Equal(
                new[] { "Deadlock detail", "Deadlocks over time", "Deadlocks by database", "Deadlock trend" },
                firing.Skip(2).Select(c => c.GetProperty("title").GetString()!).ToArray());

            var cleared = await CellsAsync(server, SqlServerName, "Deadlocks Cleared", alertTime, ct);
            Assert.Equal(new[] { "get_deadlocks", "get_deadlock_detail", "get_deadlock_trend" }, ReadNames(cleared));
            Assert.Equal(new[] { "header", "status", "read", "read", "read" }, cleared.Select(c => c.GetProperty("type").GetString()!).ToArray());
        });
    }
}
