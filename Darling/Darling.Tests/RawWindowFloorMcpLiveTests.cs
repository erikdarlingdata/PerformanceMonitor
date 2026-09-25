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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4231 against live PostgreSQL: <c>get_top_queries_by_cpu</c> and <c>get_top_procedures_by_cpu</c> read
/// <c>query_stats</c> / <c>procedure_stats</c> raw only, and must disclose a floor the same way
/// <c>get_query_store_top</c> does (#2364) — a window whose raw tier does not reach the requested start comes
/// back <c>window_truncated: true</c> with the real <c>effective_start</c> / <c>effective_hours_back</c>, and a
/// floor inside <see cref="DurationTrendRouting.TruncationSlack"/> of the request stays quiet.
/// </summary>
[Collection("live-postgres")]
public sealed class RawWindowFloorMcpLiveTests
{
    private const string ServerName = "darling-raw-window-floor-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "StackOverflow";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task RawTopReads_DiscloseATruncatedFloor_AndStayQuietInsideTheSlack()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live raw-window-floor test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);

            /* The raw tier holds one row from five minutes ago; a 24-hour ask must report the real floor
               rather than echo hours_back back unchanged (#4231, the #2364 shape). */
            var recent = now.AddMinutes(-5);
            await PlantQueryAsync(connection, ct, recent);
            await PlantProcedureAsync(connection, ct, recent);

            var queries = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, 24)).RootElement;
            Assert.True(queries.GetProperty("window_truncated").GetBoolean());
            Assert.True(queries.GetProperty("effective_hours_back").GetDouble() < 1.0);
            /* #4231 parity: exactly Lite's (#4279) sentence, "or this server has been monitored for less time
               than that" included — a caller reading Darling and Lite side by side sees the same words. */
            Assert.Equal(
                "The window reaches further back than this server's raw query_stats retains (or this server "
                + "has been monitored for less time than that), so the older part of it was not read.",
                queries.GetProperty("truncation_note").GetString());

            var procedures = JsonDocument.Parse(await DarlingMcpDataTools.GetTopProceduresByCpu(postgres, ServerName, 24)).RootElement;
            Assert.True(procedures.GetProperty("window_truncated").GetBoolean());
            Assert.True(procedures.GetProperty("effective_hours_back").GetDouble() < 1.0);
            Assert.Equal(
                "The window reaches further back than this server's raw procedure_stats retains (or this server "
                + "has been monitored for less time than that), so the older part of it was not read.",
                procedures.GetProperty("truncation_note").GetString());

            /* Inside the #2364 slack (90 minutes): a floor sitting 30 minutes after the requested start of a
               2-hour window must NOT be called truncated. */
            await DeleteRowsAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var insideSlack = now.AddHours(-2).AddMinutes(30);
            await PlantQueryAsync(connection, ct, insideSlack);

            var quiet = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, 2)).RootElement;
            Assert.False(quiet.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(JsonValueKind.Null, quiet.GetProperty("truncation_note").ValueKind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static Task PlantQueryAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle, query_text, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, min_dop, max_dop)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, Db, "0xFLOORHASH", "0xPLANHASH", "0xSQLH", "0xPLANH", "SELECT 1", 10L, 900_000L, 900_000L, 100L, 1, 1);

    private static Task PlantProcedureAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type, sql_handle, plan_handle, delta_execution_count, delta_worker_time, delta_elapsed_time)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, Db, "dbo", "usp_Floor", "SQL_STORED_PROCEDURE", "0xPROCH", "0xPROCPLANH", 5L, 900_000L, 900_000L);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var sql = string.Join(" ", new[] { "query_stats", "procedure_stats" }.Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"));
        sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

/// <summary>
/// #4231: a source pin that <c>query_stats</c>, <c>procedure_stats</c> and <c>query_store_stats</c> all reach
/// their window floor through the ONE shared probe (<see cref="RawWindowFloor"/>), never a hand-copied SQL
/// constant per table — the defect #2364 would have repeated twice more.
/// </summary>
public sealed class RawWindowFloorSharedHelperSourcePinTests
{
    private static string ReaderSource => RepoFile.ReadRepoFile(
        "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingDataReader.cs");

    [Fact]
    public void EveryRawTableFloor_RoutesThroughTheSharedHelper()
    {
        var source = ReaderSource;
        Assert.Contains("RawWindowFloor.GetAsync(postgres, RawWindowFloor.Table.QueryStats,", source, StringComparison.Ordinal);
        Assert.Contains("RawWindowFloor.GetAsync(postgres, RawWindowFloor.Table.ProcedureStats,", source, StringComparison.Ordinal);
        Assert.Contains("RawWindowFloor.GetAsync(postgres, RawWindowFloor.Table.QueryStoreStats,", source, StringComparison.Ordinal);

        /* Never a fourth, hand-rolled "SELECT MIN(collection_time) FROM <table>" beside the shared one. */
        var occurrences = System.Text.RegularExpressions.Regex.Matches(source, "SELECT MIN\\(collection_time\\)").Count;
        Assert.Equal(0, occurrences);
    }

    [Fact]
    public void TheTruncationBoundary_ReusesTheSharedSlackConstant_NeverA90MinuteLiteral()
    {
        Assert.DoesNotContain("AddMinutes(90)", ReaderSource, StringComparison.Ordinal);
        var toolSource = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs");
        Assert.DoesNotContain("AddMinutes(90)", toolSource, StringComparison.Ordinal);
        Assert.Contains("RawWindowFloor.IsTruncated", toolSource, StringComparison.Ordinal);
    }
}
