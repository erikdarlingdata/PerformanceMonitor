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
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>The web Active Queries grid shows the desktop grid's columns, and every one of them is a key get_active_queries emits.</summary>
public sealed class ActiveQueriesWidenedColumnsTests
{
    private static readonly string[] DesktopOrder =
    {
        "collection_time", "query_text", "session_id", "database_name", "login_name", "host_name", "program_name",
        "status", "elapsed_time_formatted", "cpu_time_ms", "logical_reads", "reads", "writes", "wait_type",
        "wait_time_ms", "wait_resource", "blocking_session_id", "dop", "parallel_worker_count",
        "granted_query_memory_gb", "transaction_isolation_level", "open_transaction_count", "percent_complete", "query_hash",
    };

    private static string Tabs() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js").ReplaceLineEndings("\n");

    private static string ToolSource() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpSessionTools.cs").ReplaceLineEndings("\n");

    private static string ActiveColumnsBlock()
    {
        var tabs = Tabs();
        var start = tabs.IndexOf("const ACTIVE_COLUMNS = [", StringComparison.Ordinal);
        Assert.True(start >= 0);
        return tabs[start..tabs.IndexOf("\n];", start, StringComparison.Ordinal)];
    }

    [Fact]
    public void TheGridShowsTheDesktopColumnsInDesktopOrder_WithTheAnchorAndQueryTextFirst()
    {
        var keys = Regex.Matches(ActiveColumnsBlock(), "\\bkey: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToArray();
        Assert.Equal(DesktopOrder, keys);
    }

    [Fact]
    public void EveryGridColumnKeyIsEmittedByTheRead()
    {
        var source = ToolSource();
        var projection = source[source.IndexOf("var result = shown.Select", StringComparison.Ordinal)..];
        foreach (var key in Regex.Matches(ActiveColumnsBlock(), "\\bkey: \"([a-z_]+)\"").Select(m => m.Groups[1].Value))
            Assert.Contains(key + " = ", projection);
    }

    [Fact]
    public void NumbersAreFormattedByTheGridNotPreFormattedByTheRead()
    {
        var block = ActiveColumnsBlock();
        Assert.Contains("key: \"percent_complete\", label: \"% Done\", format: \"num1\"", block);
        Assert.Contains("key: \"wait_time_ms\", label: \"Wait (ms)\", format: \"int\"", block);
        Assert.DoesNotContain("_formatted_text", block);
    }

    [Fact]
    public void TheReadSelectsTheThreeStoredColumns()
    {
        var sql = DarlingSessionReader.ActiveQueriesSql;
        Assert.Contains("wait_resource,", sql);
        Assert.Contains("CAST(percent_complete AS double precision) AS percent_complete", sql);
        Assert.Contains("query_hash", sql);
    }
}

/// <summary>The three widened fields equal the stored snapshot columns, on a fixed seed.</summary>
[Collection("live-postgres")]
public sealed class ActiveQueriesWidenedColumnsLiveTests
{
    private const string ServerName = "darling-mcp-active-queries-cols-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task WaitResourcePercentCompleteAndQueryHash_EqualTheStoredColumns_AndAreNullWhenNotStored()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live active-queries columns test.");

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
            var t = DarlingMcpTestData.Naive(DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2));
            const string insert = @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status, blocking_session_id, wait_type, cpu_time_ms, total_elapsed_time_ms, request_id, wait_resource, percent_complete, query_hash)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)";
            await DarlingMcpTestData.ExecAsync(connection, ct, insert,
                CollectionIdGenerator.Next(), t, ServerId, ServerName, 71, "Db", "ALTER INDEX ALL ON dbo.T REBUILD", "suspended", 0,
                "PAGEIOLATCH_SH", 900L, 1800L, 0, "PAGE: 5:1:12345", 37.5m, "0x1A2B3C4D5E6F7080");
            await DarlingMcpTestData.ExecAsync(connection, ct, insert,
                CollectionIdGenerator.Next(), t, ServerId, ServerName, 72, "Db", "SELECT 1", "running", 0,
                null, 100L, 200L, 0, null, null, null);

            /* #5228: session 71 carries an estimated plan only; 72 carries none. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "UPDATE query_snapshots SET query_plan = $1 WHERE server_id = $2 AND session_id = 71", "<ShowPlanXML />", ServerId);

            var json = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 1, limit: 10)).RootElement;
            var rows = json.GetProperty("queries").EnumerateArray().ToDictionary(r => r.GetProperty("session_id").GetInt32());

            using var stored = new NpgsqlCommand(
                "SELECT wait_resource, percent_complete, query_hash FROM query_snapshots WHERE server_id = $1 AND session_id = 71", connection);
            stored.Parameters.AddWithValue(ServerId);
            await using (var reader = await stored.ExecuteReaderAsync(ct))
            {
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(reader.GetString(0), rows[71].GetProperty("wait_resource").GetString());
                Assert.Equal((double)reader.GetDecimal(1), rows[71].GetProperty("percent_complete").GetDouble());
                Assert.Equal(reader.GetString(2), rows[71].GetProperty("query_hash").GetString());
            }

            Assert.Equal("PAGE: 5:1:12345", rows[71].GetProperty("wait_resource").GetString());
            Assert.Equal(37.5, rows[71].GetProperty("percent_complete").GetDouble());
            Assert.Equal("0x1A2B3C4D5E6F7080", rows[71].GetProperty("query_hash").GetString());
            /* The plan flags are written only when true: a row with a plan has the key, a row without has neither. */
            Assert.True(rows[71].GetProperty("has_query_plan").GetBoolean());
            Assert.False(rows[71].TryGetProperty("has_live_query_plan", out _));
            Assert.False(rows[72].TryGetProperty("has_query_plan", out _));
            Assert.False(rows[72].TryGetProperty("has_live_query_plan", out _));
            Assert.Equal(0, rows[72].GetProperty("request_id").GetInt32());
            Assert.Equal(JsonValueKind.Null, rows[72].GetProperty("wait_resource").ValueKind);
            Assert.Equal(JsonValueKind.Null, rows[72].GetProperty("percent_complete").ValueKind);
            Assert.Equal(JsonValueKind.Null, rows[72].GetProperty("query_hash").ValueKind);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM query_snapshots WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
