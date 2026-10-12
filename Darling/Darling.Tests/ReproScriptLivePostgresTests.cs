/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5233: get_query_repro_script builds each kind's script from STORED text and plan, and nothing else. Seeded through
/// the inline <c>query_plan_text</c> column for Query Store (the plan map read arrives separately), the inline
/// <c>query_text</c> on query_stats and query_store_stats, a <c>collect.query_store_text</c> row for the precedence
/// check, and a query_snapshots row with an isolation level. Fixed anchors only.
/// </summary>
[Collection("live-postgres")]
public sealed class ReproScriptLivePostgresTests
{
    private const string ServerName = "darling-repro-script-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const string Db = "ReproDb";
    private const string StatsHash = "0xA0B1C2D3E4F50617";

    private static readonly DateTime Anchor = new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Unspecified).AddTicks(1_234_560);

    private static string PlanXml(string marker) =>
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\">" +
        $"<BatchSequence><Batch><Statements><StmtSimple StatementText=\"SELECT '{marker}'\" StatementId=\"1\" /></Statements></Batch></BatchSequence></ShowPlanXML>";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static JsonElement Parse(string json, out JsonDocument doc)
    {
        doc = JsonDocument.Parse(json);
        return doc.RootElement;
    }

    [Fact]
    public async Task EachKind_BuildsAScriptFromStoredText_AndAWrongKeyIsUnavailable()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live repro-script test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);

            await ExecAsync(connection, ct,
                "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text, query_plan_xml) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                CollectionIdGenerator.Next(), Anchor, ServerId, ServerName, Db, StatsHash, "SELECT 'stats-text';", PlanXml("stats"));

            /* Query Store 4242: collector text AND an older inline text, with a plan. 4243: inline text only, no plan. */
            await ExecAsync(connection, ct,
                "INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_text, query_plan_text) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                CollectionIdGenerator.Next(), Anchor, ServerId, ServerName, Db, 4242L, 7L, "SELECT 'inline-text';", PlanXml("query-store"));
            await ExecAsync(connection, ct,
                "INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_text) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                CollectionIdGenerator.Next(), Anchor, ServerId, ServerName, Db, 4243L, 8L, "SELECT 'no-plan-text';");
            await ExecAsync(connection, ct,
                "INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen) VALUES ($1, $2, $3, $4, $5)",
                ServerId, Db, 4242L, "SELECT 'collector-text';", Anchor);

            await ExecAsync(connection, ct,
                "INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, request_id, database_name, query_text, transaction_isolation_level, query_plan) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                CollectionIdGenerator.Next(), Anchor, ServerId, ServerName, 51, 3, Db, "SELECT 'snapshot-text';", "SERIALIZABLE", PlanXml("snapshot"));

            /* ---- query_hash */
            var root = Parse(await DarlingMcpPlanTools.GetQueryReproScript(postgres, "query_hash", ServerName, query_hash: StatsHash, cancellationToken: ct), out var d1);
            using (d1)
            {
                Assert.True(root.GetProperty("plan_found").GetBoolean());
                Assert.Contains("SELECT 'stats-text';", root.GetProperty("script").GetString(), StringComparison.Ordinal);
            }

            /* ---- query_store: the collector's text wins over the inline text. */
            root = Parse(await DarlingMcpPlanTools.GetQueryReproScript(postgres, "query_store", ServerName, Db, query_id: 4242, plan_id: 7, cancellationToken: ct), out var d2);
            using (d2)
            {
                var script = root.GetProperty("script").GetString()!;
                Assert.True(root.GetProperty("plan_found").GetBoolean());
                Assert.Contains("SELECT 'collector-text';", script, StringComparison.Ordinal);
                Assert.DoesNotContain("inline-text", script, StringComparison.Ordinal);
            }

            /* ---- query_store: text but no plan builds a script with plan_found false. */
            root = Parse(await DarlingMcpPlanTools.GetQueryReproScript(postgres, "query_store", ServerName, Db, query_id: 4243, cancellationToken: ct), out var d3);
            using (d3)
            {
                Assert.False(root.GetProperty("plan_found").GetBoolean());
                Assert.Contains("SELECT 'no-plan-text';", root.GetProperty("script").GetString(), StringComparison.Ordinal);
            }

            /* ---- active_snapshot: the collection_time the grid hands back, with the stored isolation level. */
            var key = Anchor.ToString("o", CultureInfo.InvariantCulture);
            root = Parse(await DarlingMcpPlanTools.GetQueryReproScript(postgres, "active_snapshot", ServerName, collection_time: key, session_id: 51, request_id: 3, cancellationToken: ct), out var d4);
            using (d4)
            {
                var script = root.GetProperty("script").GetString()!;
                Assert.True(root.GetProperty("plan_found").GetBoolean());
                Assert.Contains("SELECT 'snapshot-text';", script, StringComparison.Ordinal);
                Assert.Contains("SET TRANSACTION ISOLATION LEVEL", script, StringComparison.Ordinal);
            }

            /* ---- wrong keys are unavailable, never an empty script. */
            await AssertUnavailableAsync(await DarlingMcpPlanTools.GetQueryReproScript(postgres, "query_hash", ServerName, query_hash: "0xDEADBEEF", cancellationToken: ct));
            await AssertUnavailableAsync(await DarlingMcpPlanTools.GetQueryReproScript(postgres, "query_store", ServerName, Db, query_id: 9999, cancellationToken: ct));
            await AssertUnavailableAsync(await DarlingMcpPlanTools.GetQueryReproScript(postgres, "active_snapshot", ServerName, collection_time: key, session_id: 999, request_id: 3, cancellationToken: ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static Task AssertUnavailableAsync(string json)
    {
        using var doc = JsonDocument.Parse(json);
        Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
        return Task.CompletedTask;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params object[] values)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var v in values)
            command.Parameters.AddWithValue(v is DateTime t ? DateTime.SpecifyKind(t, DateTimeKind.Unspecified) : v);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ExecAsync(connection, ct,
            @"INSERT INTO servers (server_id, server_name, display_name, is_enabled, created_date, modified_date)
              VALUES ($1, $2, $3, TRUE, $4, $4)
              ON CONFLICT (server_id) DO UPDATE SET server_name = EXCLUDED.server_name, display_name = EXCLUDED.display_name,
                  is_enabled = TRUE, modified_date = EXCLUDED.modified_date;",
            ServerId, ServerName, ServerName, Anchor);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var cleanup = new NpgsqlCommand(
            $"DELETE FROM query_snapshots WHERE server_id = {ServerId}; " +
            $"DELETE FROM query_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM query_store_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM collect.query_store_text WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
