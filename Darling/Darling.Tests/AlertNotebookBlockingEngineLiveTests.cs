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
/// #8584 gated live round-trip (DARLING_TEST_PG): the alert notebooks a blocking or long-running-query alert links to, and
/// the /api/triage page for a deadlock, blocking or long-running-query alert, follow the target's engine. A PostgreSQL target
/// with one stored event of that kind gets the PostgreSQL reads, one of them shows the event, and no cell says "not
/// collected" or "does not apply". A SQL Server target keeps the cells it always had. Rows are tagged with per-class server
/// names and removed in cleanup.
/// </summary>
[Collection("live-postgres")]
public sealed class AlertNotebookBlockingEngineLiveTests
{
    // #1776 own-store: every row is keyed by a server name private to this class and deleted in cleanup.
    private const string PgServerName = "alert-notebook-blocking-8584-pg";
    private const string SqlServerName = "alert-notebook-blocking-8584-sql";
    private const string RootQuery = "UPDATE notebook_8584_root SET x = ?";
    private const string SessionApp = "notebook-8584-app";
    private const string DeadlockHash = "hash-8584-triage";

    private static readonly int PgServerId = ServerIdHelper.GetDeterministicHashCode(PgServerName);
    private static readonly int SqlServerId = ServerIdHelper.GetDeterministicHashCode(SqlServerName);

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the blocking alert-notebook engine live test.");
        return connectionString!;
    }

    private static NpgsqlDataSource OpenDataSource(string connectionString) =>
        NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "config_alert_log", "pg_blocking_edges", "pg_session_states", "pg_deadlocks", "collection_log", "servers" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = ANY($1)", new[] { PgServerId, SqlServerId });
        }
    }

    private static Task SeedServerAsync(NpgsqlConnection connection, DateTime alertTimeUtc, string? engineKind, int serverId, string serverName, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, engine_kind, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, $3, $4, $4)",
            serverId, serverName, (object?)engineKind ?? DBNull.Value, DarlingMcpTestData.Naive(alertTimeUtc.AddDays(-30)));

    private static Task SeedAlertAsync(NpgsqlConnection connection, DateTime alertTimeUtc, int serverId, string serverName, string metric, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, $4, 1, 1, TRUE, 'webhook', NULL, FALSE, FALSE, NULL, NULL)",
            DarlingMcpTestData.Naive(alertTimeUtc), serverId, serverName, metric);

    private static Task SeedPgBlockingAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_blocking_edges
    (collection_id, collection_time, server_id, server_name, blocked_backend_id, blocked_pid, blocking_backend_id, blocking_pid,
     database_name, blocked_username, blocked_application_name, blocked_client_addr, blocked_state, blocked_wait_event_type,
     blocked_wait_event, blocked_query, blocked_xact_duration_ms, blocked_query_duration_ms, blocking_username,
     blocking_application_name, blocking_client_addr, blocking_state, blocking_wait_event_type, blocking_wait_event,
     blocking_query, blocking_xact_duration_ms, blocking_query_duration_ms, blocked_pid_count,
     blocking_is_idle_in_transaction, query_text_may_be_truncated)
VALUES ($1, $2, $3, $4, 100, 100, 200, 200, 'appdb', 'user1', 'app', '10.0.0.1', 'active', 'Lock', 'relation', 'select 1',
        100, 100, 'user2', 'app', '10.0.0.2', 'idle in transaction', NULL, NULL, $5, 200, 200, 1, TRUE, FALSE)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), PgServerId, PgServerName, RootQuery);

    private static Task SeedPgSessionAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO pg_session_states (collection_id, collection_time, server_id, server_name, backend_id, pid, application_name, state, xact_duration_ms, state_duration_ms, query_duration_ms, backend_duration_ms, horizon_age, is_idle_in_transaction, is_horizon_holder, state_is_redacted, total_sessions, active_sessions, idle_in_transaction_sessions, reportable_sessions) "
            + "VALUES ($1, $2, $3, $4, 1, 101, $5, 'idle in transaction', 60000, 60000, 1000, 120000, 5, TRUE, TRUE, FALSE, 10, 2, 4, 4)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), PgServerId, PgServerName, SessionApp);

    private static Task SeedPgDeadlockAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO pg_deadlocks (collection_id, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count, deadlock_hash, lock_modes, resources, victim_statement, graph_text)
VALUES ($1,$2,$3,$4,$5,4242,2,$6,'ShareLock','relation','UPDATE t SET x = ?','graph text')",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), PgServerId, PgServerName,
            DarlingMcpTestData.Naive(at), DeadlockHash);

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

    private static string PageQuery(string serverName, string metric, DateTime alertTimeUtc) =>
        "?server=" + Uri.EscapeDataString(serverName) + "&metric=" + Uri.EscapeDataString(metric) + "&at=" + Uri.EscapeDataString(Iso(alertTimeUtc));

    /// <summary>The notebook's cells for <paramref name="metric"/> at <paramref name="alertTimeUtc"/> on <paramref name="serverName"/>.</summary>
    private static async Task<List<JsonElement>> CellsAsync(TestServer server, string serverName, string metric, DateTime alertTimeUtc, CancellationToken ct)
    {
        var body = await GetBodyAsync(server, "/api/alert-notebook", PageQuery(serverName, metric, alertTimeUtc), ct);
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

    private static string[] Types(IEnumerable<JsonElement> cells) =>
        cells.Select(c => c.GetProperty("type").GetString()!).ToArray();

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

    /// <summary>Runs every read cell and returns the answers, asserting none says "not collected" or "does not apply".</summary>
    private static async Task<List<string>> RunAllReadsAsync(TestServer server, IEnumerable<JsonElement> cells, CancellationToken ct)
    {
        var answers = new List<string>();
        foreach (var cell in cells.Where(c => c.GetProperty("type").GetString() == "read"))
        {
            var answer = await RunReadCellAsync(server, cell, ct);
            Assert.DoesNotContain("not_collected", answer, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("does not apply", answer, StringComparison.OrdinalIgnoreCase);
            answers.Add(answer);
        }

        return answers;
    }

    [Theory]
    [InlineData("Blocking Detected")]
    [InlineData("Blocking Wait Time")]
    [InlineData("Blocking Cleared")]
    [InlineData("Blocking Wait Cleared")]
    public async Task PostgresTarget_BlockingNotebookIsThePostgresBlockingReads_AndListsTheStoredChain(string metric)
    {
        await WithStoreAsync(async (connection, server, alertTime, ct) =>
        {
            await SeedServerAsync(connection, alertTime, MonitoredEngineKind.Postgres, PgServerId, PgServerName, ct);
            await SeedAlertAsync(connection, alertTime, PgServerId, PgServerName, metric, ct);
            await SeedPgBlockingAsync(connection, alertTime, ct);
            await SeedPgSessionAsync(connection, alertTime, ct);

            var cells = await CellsAsync(server, PgServerName, metric, alertTime, ct);

            Assert.Equal(new[] { "get_pg_blocking", "get_pg_session_states" }, ReadNames(cells));
            Assert.DoesNotContain(cells, c => c.GetProperty("type").GetString() == "panel");

            var answers = await RunAllReadsAsync(server, cells, ct);
            Assert.Contains(RootQuery, answers[0], StringComparison.Ordinal);
            Assert.Contains(SessionApp, answers[1], StringComparison.Ordinal);
        });
    }

    [Theory]
    [InlineData("Long-Running Query")]
    [InlineData("Long-Running Queries Cleared")]
    public async Task PostgresTarget_LongRunningQueryNotebookIsThePostgresSessionRead_AndListsTheStoredSession(string metric)
    {
        await WithStoreAsync(async (connection, server, alertTime, ct) =>
        {
            await SeedServerAsync(connection, alertTime, MonitoredEngineKind.Postgres, PgServerId, PgServerName, ct);
            await SeedAlertAsync(connection, alertTime, PgServerId, PgServerName, metric, ct);
            await SeedPgSessionAsync(connection, alertTime, ct);

            var cells = await CellsAsync(server, PgServerName, metric, alertTime, ct);

            Assert.Equal(new[] { "get_pg_session_states" }, ReadNames(cells));
            Assert.DoesNotContain(cells, c => c.GetProperty("type").GetString() is "panel" or "markdown");

            var answers = await RunAllReadsAsync(server, cells, ct);
            Assert.Contains(SessionApp, answers[0], StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task SqlServerTarget_BlockingAndLongRunningNotebooksKeepTheirSqlServerCells()
    {
        await WithStoreAsync(async (connection, server, alertTime, ct) =>
        {
            await SeedServerAsync(connection, alertTime, MonitoredEngineKind.SqlServer, SqlServerId, SqlServerName, ct);

            var blocking = await CellsAsync(server, SqlServerName, "Blocking Detected", alertTime, ct);
            Assert.Equal(new[] { "header", "status", "read", "panel", "panel", "panel", "panel", "read" }, Types(blocking));
            Assert.Equal(new[] { "get_blocking", "get_active_queries" }, ReadNames(blocking));

            var blockingCleared = await CellsAsync(server, SqlServerName, "Blocking Cleared", alertTime, ct);
            Assert.Equal(new[] { "get_blocking", "get_blocking_stats", "get_active_queries" }, ReadNames(blockingCleared));
            Assert.Equal(new[] { "header", "status", "read", "read", "read" }, Types(blockingCleared));

            var longRunning = await CellsAsync(server, SqlServerName, "Long-Running Query", alertTime, ct);
            Assert.Equal(new[] { "header", "status", "read", "markdown", "panel", "read" }, Types(longRunning));
            Assert.Equal(new[] { "get_active_queries", "get_long_query_completions" }, ReadNames(longRunning));

            var longRunningCleared = await CellsAsync(server, SqlServerName, "Long-Running Queries Cleared", alertTime, ct);
            Assert.Equal(new[] { "get_active_queries", "get_long_query_completions" }, ReadNames(longRunningCleared));
            Assert.Equal(new[] { "header", "status", "read", "read" }, Types(longRunningCleared));
        });
    }

    /// <summary>The triage page's sections: (read name, the section's data as text), in page order, each asserted free of an error.</summary>
    private static async Task<List<(string Read, string Data)>> TriageSectionsAsync(TestServer server, string serverName, string metric, DateTime alertTimeUtc, CancellationToken ct)
    {
        var body = await GetBodyAsync(server, "/api/triage", PageQuery(serverName, metric, alertTimeUtc), ct);
        using var doc = JsonDocument.Parse(body);
        var sections = new List<(string, string)>();
        foreach (var section in doc.RootElement.GetProperty("sections").EnumerateArray())
        {
            Assert.False(section.TryGetProperty("error", out _), section.GetRawText());
            sections.Add((section.GetProperty("read").GetString()!, section.GetProperty("data").GetRawText()));
        }

        return sections;
    }

    [Fact]
    public async Task PostgresTarget_TriagePagesRunThePostgresReads_AndListTheStoredEvents()
    {
        await WithStoreAsync(async (connection, server, alertTime, ct) =>
        {
            await SeedServerAsync(connection, alertTime, MonitoredEngineKind.Postgres, PgServerId, PgServerName, ct);
            await SeedPgDeadlockAsync(connection, alertTime, ct);
            await SeedPgBlockingAsync(connection, alertTime, ct);
            await SeedPgSessionAsync(connection, alertTime, ct);

            var deadlocks = await TriageSectionsAsync(server, PgServerName, "Deadlocks Detected", alertTime, ct);
            Assert.Equal(new[] { "get_pg_deadlocks", "get_pg_deadlock_detail", "get_collection_log" }, deadlocks.Select(s => s.Read).ToArray());
            Assert.Contains(DeadlockHash, deadlocks[0].Data, StringComparison.Ordinal);
            Assert.Contains(DeadlockHash, deadlocks[1].Data, StringComparison.Ordinal);

            var blocking = await TriageSectionsAsync(server, PgServerName, "Blocking Cleared", alertTime, ct);
            Assert.Equal(new[] { "get_pg_blocking", "get_pg_session_states", "get_collection_log" }, blocking.Select(s => s.Read).ToArray());
            Assert.Contains(RootQuery, blocking[0].Data, StringComparison.Ordinal);

            var longRunning = await TriageSectionsAsync(server, PgServerName, "Long-Running Query", alertTime, ct);
            Assert.Equal(new[] { "get_pg_session_states", "get_collection_log" }, longRunning.Select(s => s.Read).ToArray());
            Assert.Contains(SessionApp, longRunning[0].Data, StringComparison.Ordinal);

            foreach (var page in new[] { deadlocks, blocking, longRunning })
            {
                Assert.DoesNotContain(page, s => s.Data.Contains("does not apply", StringComparison.OrdinalIgnoreCase)
                    || s.Data.Contains("not_collected", StringComparison.OrdinalIgnoreCase));
            }
        });
    }

    [Fact]
    public async Task SqlServerTarget_TriagePagesKeepTheirSqlServerSections()
    {
        await WithStoreAsync(async (connection, server, alertTime, ct) =>
        {
            await SeedServerAsync(connection, alertTime, MonitoredEngineKind.SqlServer, SqlServerId, SqlServerName, ct);

            var deadlocks = await TriageSectionsAsync(server, SqlServerName, "Deadlocks Detected", alertTime, ct);
            Assert.Equal(new[] { "get_deadlocks", "get_deadlock_detail", "get_deadlock_trend", "get_collection_log" }, deadlocks.Select(s => s.Read).ToArray());

            var blocking = await TriageSectionsAsync(server, SqlServerName, "Blocking Detected", alertTime, ct);
            Assert.Equal(new[] { "get_blocking", "get_blocking_stats", "get_active_queries", "get_collection_log" }, blocking.Select(s => s.Read).ToArray());

            var longRunning = await TriageSectionsAsync(server, SqlServerName, "Long-Running Query", alertTime, ct);
            Assert.Equal(new[] { "get_active_queries", "get_long_query_completions", "get_collection_log" }, longRunning.Select(s => s.Read).ToArray());
        });
    }

    [Fact]
    public async Task UnresolvableEngine_NotebookAndTriageKeepBothEnginesReads()
    {
        await WithStoreAsync(async (connection, server, alertTime, ct) =>
        {
            /* A registry row no connect has stamped: engine_kind NULL. */
            await SeedServerAsync(connection, alertTime, null, SqlServerId, SqlServerName, ct);

            var blocking = await CellsAsync(server, SqlServerName, "Blocking Detected", alertTime, ct);
            Assert.Equal(new[] { "get_blocking", "get_active_queries", "get_pg_blocking", "get_pg_session_states" }, ReadNames(blocking));

            var triage = await TriageSectionsAsync(server, SqlServerName, "Long-Running Query", alertTime, ct);
            Assert.Equal(new[] { "get_active_queries", "get_long_query_completions", "get_pg_session_states", "get_collection_log" }, triage.Select(s => s.Read).ToArray());
        });
    }
}
