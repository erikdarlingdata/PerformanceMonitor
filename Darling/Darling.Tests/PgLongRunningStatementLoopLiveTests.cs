/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8970 through the worker: the PostgreSQL Long-Running Query card names each session it shows, and each shown session
/// carries its OWN stored statement. Drives <c>EvaluatePgLongRunningQueryAsync</c> itself, so the per-session statement
/// lookup loop in the host is under test (the builder tests hand it rows that already carry their text). Four sessions run
/// past the threshold, each with its own query id and stored text. The card shows the three longest, each with its own
/// statement, and the fourth session's statement is on no item.
/// </summary>
/* #1776 own-store: ScratchPostgres mints a database of its own. */
[Collection("live-postgres")]
public sealed class PgLongRunningStatementLoopLiveTests
{
    private const string ServerName = "pglongrunning-statement-loop";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly string Key = ServerId.ToString(CultureInfo.InvariantCulture);

    [Fact]
    public async Task EachShownSession_CarriesItsOwnStoredStatement_AndTheFourthSessionHasNoItem()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the PostgreSQL long-running statement loop test.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            await using var dataSource = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                SearchPath = "collect,config,public",
            }.ConnectionString);

            var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            var captureAt = now.AddMinutes(-1);
            var collectionId = CollectionIdGenerator.Next();
            string[] markers = { "alphaStatementMarker", "bravoStatementMarker", "charlieStatementMarker", "deltaStatementMarker" };
            for (var i = 0; i < markers.Length; i++)
            {
                /* Longest first: 90, 80, 70 and 60 minutes over a 30 minute threshold. */
                await PlantSessionAsync(connection, collectionId, captureAt, backendId: i + 1, pid: 7001 + i,
                    queryId: 9_000 + i, durationMs: (90 - (10L * i)) * 60_000L, ct);
                await PlantStatementAsync(connection, queryId: 9_000 + i, $"SELECT {markers[i]} FROM orders", now, ct);
            }

            using var endpoint = new CapturingWebhookEndpoint();
            var pass = StartLife(dataSource, endpoint.Url, ct);
            await pass();

            var body = Assert.Single(endpoint.Bodies);
            foreach (var shown in markers.Take(3))
            {
                Assert.True(CountOf(body, shown) > 0, $"{shown} is on no item");
            }

            Assert.Equal(0, CountOf(body, markers[3]));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    private static int CountOf(string text, string value)
    {
        var count = 0;
        for (var at = text.IndexOf(value, StringComparison.Ordinal); at >= 0; at = text.IndexOf(value, at + value.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    private static async Task PlantSessionAsync(
        NpgsqlConnection connection, long collectionId, DateTime at, long backendId, int pid, long queryId, long durationMs,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.pg_session_states
    (collection_id, collection_time, server_id, server_name, backend_id, pid, database_name, username, application_name, client_addr,
     backend_type, state, wait_event_type, wait_event, command_tag, query_id,
     state_duration_ms, xact_duration_ms, query_duration_ms, backend_duration_ms, xmin_age, xid_age, horizon_age,
     is_idle_in_transaction, is_horizon_holder, state_is_redacted,
     total_sessions, active_sessions, idle_in_transaction_sessions, reportable_sessions)
VALUES ($1, $2, $3, $4, $5, $6, 'appdb', 'analyst', 'psql', NULL,
        'client backend', 'active', NULL, NULL, 'SELECT', $8,
        $7, $7, $7, $7, -1, -1, -1,
        false, false, false,
        4, 4, 0, 4)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(backendId);
        command.Parameters.AddWithValue(pid);
        command.Parameters.AddWithValue(durationMs);
        command.Parameters.AddWithValue(queryId);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantStatementAsync(
        NpgsqlConnection connection, long queryId, string text, DateTime at, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO collect.pg_statement_text (server_id, queryid, query_text, first_seen, last_seen) VALUES ($1, $2, $3, $4, $4)",
            connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(text);
        command.Parameters.AddWithValue(at);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One life of the service: a fresh worker sending to <paramref name="webhookUrl"/>, one pass of the long-running check per call.</summary>
    private static Func<Task> StartLife(NpgsqlDataSource dataSource, string webhookUrl, CancellationToken ct)
    {
        var config = new DarlingConfig();
        config.Webhooks.GenericUrl = webhookUrl;
        config.Mcp.DataDetail = "full";
        var settings = new DarlingAlertSettings(config);
        var history = new PgAlertHistoryStore(dataSource, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance);
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance);

        var worker = new DarlingWorker(
            NullLogger<DarlingWorker>.Instance,
            NullLoggerFactory.Instance,
            new McpRuntimeState(),
            new WebRuntimeState(),
            new MonitoredServerRegistryState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache(),
            new ReadLatencyAccumulator())
        {
            StoreForTests = dataSource,
        };
        var flags = BindingFlags.NonPublic | BindingFlags.Instance;
        typeof(DarlingWorker).GetField("_alertDeliverer", flags)!.SetValue(worker, deliverer);
        var evaluate = typeof(DarlingWorker).GetMethod("EvaluatePgLongRunningQueryAsync", flags)!;

        var runtime = new ServerRuntime
        {
            Config = new MonitoredServer { Name = ServerName, Host = ServerName },
            ConnectionString = "Host=unused",
            Target = new CollectorTargetInfo(),
            StorageName = ServerName,
            ServerId = ServerId,
        };
        var snapshot = new AlertServerSnapshot(Key, ServerName, true, null, null, false, false, null) { ServerId = ServerId };

        return () => (Task)evaluate.Invoke(worker, new object[] { runtime, snapshot, config, ct })!;
    }
}
