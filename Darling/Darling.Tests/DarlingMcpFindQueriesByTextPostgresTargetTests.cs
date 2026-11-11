/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8683: find_queries_by_text searches two SQL Server sources. On a PostgreSQL target both used to answer "empty" (a true negative) for a
/// statement the store does hold text for. They now answer not_collected, name the engine and point at get_pg_top_queries, and neither
/// read runs. A SQL Server target is unchanged.
/// </summary>
/* #1776 own-store: the fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT [Collection("live-postgres")]. */
public sealed class DarlingMcpFindQueriesByTextPostgresTargetTests
{
    private const int PgServerId = 81;
    private const string PgServerName = "pg-eight-one";
    private const int SqlServerId = 82;
    private const string SqlServerName = "sql-eight-two";

    [Fact]
    public async Task PostgresTarget_AnswersNotCollectedForBothSources_AndSqlServerTargetStillSearches()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the find_queries_by_text PostgreSQL target test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var storeString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var connection = new NpgsqlConnection(storeString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(storeString);

        foreach (var (id, name, kind) in new[] { (PgServerId, PgServerName, "postgres"), (SqlServerId, SqlServerName, "sqlserver") })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO servers (server_id, server_name, display_name, is_enabled, engine_kind, created_date, modified_date) VALUES ($1, $2, $2, true, $3, now(), now())",
                id, name, kind);
        }

        var pgReply = await DarlingMcpFindQueriesByTextTools.FindQueriesByText(
            postgres, "CASE WHEN query_text", PgServerName, null, 3, McpDataDetail.Full,
            DarlingMcpFindQueriesByTextTools.SourceBudget.Default, DarlingMcpFindQueriesByTextTools.SourceBudget.Default, ct);
        using (var doc = JsonDocument.Parse(pgReply))
        {
            var sources = doc.RootElement.GetProperty("sources");
            foreach (var source in new[] { "query_store", "plan_cache" })
            {
                var one = sources.GetProperty(source);
                Assert.Equal("not_collected", one.GetProperty("status").GetString());
                var message = one.GetProperty("message").GetString()!;
                Assert.Contains("PostgreSQL", message, StringComparison.Ordinal);
                Assert.Contains("get_pg_top_queries", message, StringComparison.Ordinal);
                Assert.Equal(0, one.GetProperty("rows").GetArrayLength());
            }
        }

        var sqlReply = await DarlingMcpFindQueriesByTextTools.FindQueriesByText(
            postgres, "CASE WHEN query_text", SqlServerName, null, 3, McpDataDetail.Full,
            DarlingMcpFindQueriesByTextTools.SourceBudget.Default, DarlingMcpFindQueriesByTextTools.SourceBudget.Default, ct);
        using (var doc = JsonDocument.Parse(sqlReply))
        {
            var sources = doc.RootElement.GetProperty("sources");
            Assert.Equal("empty", sources.GetProperty("query_store").GetProperty("status").GetString());
            Assert.Equal("empty", sources.GetProperty("plan_cache").GetProperty("status").GetString());
        }
    }
}
