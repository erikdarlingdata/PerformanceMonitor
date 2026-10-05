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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5228: the three row-keyed plan reads the web grids call (get_active_query_plan_xml,
/// get_query_store_plan_xml, get_procedure_plan_xml) return the EXACT stored XML for the row's own key and
/// answer <c>unavailable</c> on a key that matches nothing. The Active Queries read is keyed by the row's
/// natural key, so its collection_time must survive the trip through <c>ToString("o")</c> with the
/// microseconds Postgres keeps, and a snapshot row collected with no request_id must answer to request_id 0.
/// Query Store plans are seeded through the inline <c>query_plan_text</c> column. Fixed anchors only.
/// </summary>
[Collection("live-postgres")]
public sealed class RowPlanReadLivePostgresTests
{
    private const string ServerName = "darling-row-plan-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const string Db = "RowPlanDb";
    private const string SqlHandle = "0xA0B1C2D3E4F50617";

    /// <summary>Fixed anchor with a non-zero microsecond part (123456 us), naive UTC as the store keeps it.</summary>
    private static readonly DateTime Anchor = new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Unspecified).AddTicks(1_234_560);

    private static string PlanXml(string marker) =>
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\">" +
        $"<BatchSequence><Batch><Statements><StmtSimple StatementText=\"SELECT '{marker}'\" StatementId=\"1\" /></Statements></Batch></BatchSequence></ShowPlanXML>";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string StatusOf(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("status").GetString()!;
    }

    [Fact]
    public async Task EachRowPlanTool_ReturnsTheExactStoredXml_AndUnavailableOnAWrongKey()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live row-plan test.");
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

            var estimated = PlanXml("estimated");
            var live = PlanXml("live");
            var noRequest = PlanXml("no-request-id");
            var queryStore = PlanXml("query-store");
            var procedure = PlanXml("procedure");

            await InsertSnapshotAsync(connection, Anchor, 51, 3, estimated, live, ct);
            await InsertSnapshotAsync(connection, Anchor, 52, null, noRequest, null, ct);
            await InsertQueryStoreAsync(connection, Anchor, 4242, 7, queryStore, ct);
            await InsertProcedureAsync(connection, Anchor, procedure, ct);

            /* ---- get_active_query_plan_xml: the collection_time the grid hands back is Anchor.ToString("o"). */
            var key = Anchor.ToString("o", CultureInfo.InvariantCulture);
            Assert.Equal(estimated, await DarlingMcpPlanTools.GetActiveQueryPlanXml(postgres, key, 51, ServerName, 3, cancellationToken: ct));
            Assert.Equal(live, await DarlingMcpPlanTools.GetActiveQueryPlanXml(postgres, key, 51, ServerName, 3, live: true, cancellationToken: ct));

            /* The microseconds are the key: the same instant rounded to the second matches nothing. */
            var rounded = Anchor.AddTicks(-(Anchor.Ticks % TimeSpan.TicksPerSecond)).ToString("o", CultureInfo.InvariantCulture);
            Assert.Equal("unavailable", StatusOf(await DarlingMcpPlanTools.GetActiveQueryPlanXml(postgres, rounded, 51, ServerName, 3, cancellationToken: ct)));

            /* A row collected with a NULL request_id answers to request_id 0 (the grids carry it as 0). */
            Assert.Equal(noRequest, await DarlingMcpPlanTools.GetActiveQueryPlanXml(postgres, key, 52, ServerName, 0, cancellationToken: ct));
            Assert.Equal(noRequest, await DarlingMcpPlanTools.GetActiveQueryPlanXml(postgres, key, 52, ServerName, cancellationToken: ct));

            /* Wrong keys, and a row with no live plan, are unavailable. */
            Assert.Equal("unavailable", StatusOf(await DarlingMcpPlanTools.GetActiveQueryPlanXml(postgres, key, 999, ServerName, 3, cancellationToken: ct)));
            Assert.Equal("unavailable", StatusOf(await DarlingMcpPlanTools.GetActiveQueryPlanXml(postgres, key, 51, ServerName, 4, cancellationToken: ct)));
            Assert.Equal("unavailable", StatusOf(await DarlingMcpPlanTools.GetActiveQueryPlanXml(postgres, key, 52, ServerName, 0, live: true, cancellationToken: ct)));

            /* ---- get_query_store_plan_xml, through the inline query_plan_text column. */
            Assert.Equal(queryStore, await DarlingMcpPlanTools.GetQueryStorePlanXml(postgres, Db, 4242, ServerName, cancellationToken: ct));
            Assert.Equal(queryStore, await DarlingMcpPlanTools.GetQueryStorePlanXml(postgres, Db, 4242, ServerName, plan_id: 7, cancellationToken: ct));
            Assert.Equal("unavailable", StatusOf(await DarlingMcpPlanTools.GetQueryStorePlanXml(postgres, Db, 9999, ServerName, cancellationToken: ct)));
            Assert.Equal("unavailable", StatusOf(await DarlingMcpPlanTools.GetQueryStorePlanXml(postgres, Db, 4242, ServerName, plan_id: 8, cancellationToken: ct)));

            /* ---- get_procedure_plan_xml. */
            Assert.Equal(procedure, await DarlingMcpPlanTools.GetProcedurePlanXml(postgres, SqlHandle, ServerName, cancellationToken: ct));
            Assert.Equal("unavailable", StatusOf(await DarlingMcpPlanTools.GetProcedurePlanXml(postgres, "0xDEADBEEF", ServerName, cancellationToken: ct)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET server_name = EXCLUDED.server_name, display_name = EXCLUDED.display_name,
    is_enabled = TRUE, modified_date = EXCLUDED.modified_date;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(Anchor, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertSnapshotAsync(
        NpgsqlConnection connection, DateTime collectionTime, int sessionId, int? requestId, string? plan, string? livePlan,
        System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, request_id, query_plan, live_query_plan)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(sessionId);
        command.Parameters.AddWithValue((object?)requestId ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)plan ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)livePlan ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertQueryStoreAsync(
        NpgsqlConnection connection, DateTime collectionTime, long queryId, long planId, string planText,
        System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_plan_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Db);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(planId);
        command.Parameters.AddWithValue(planText);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertProcedureAsync(
        NpgsqlConnection connection, DateTime collectionTime, string planXml, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle, query_plan_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Db);
        command.Parameters.AddWithValue("dbo");
        command.Parameters.AddWithValue("usp_RowPlan");
        command.Parameters.AddWithValue(SqlHandle);
        command.Parameters.AddWithValue(planXml);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await using var cleanup = new NpgsqlCommand(
            $"DELETE FROM query_snapshots WHERE server_id = {ServerId}; " +
            $"DELETE FROM query_store_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM procedure_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
