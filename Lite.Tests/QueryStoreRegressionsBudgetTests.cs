/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4198: get_query_store_regressions' own response-budget pin, Lite's twin of
/// <c>Darling.Tests.DarlingMcpQueryStoreRegressionsBudgetLiveTests</c> - the worst #4198 offender, a busy
/// production store's default call (limit 50 then) measured 211 KB, almost all of it fifty rows' worth of
/// unbounded query_text plus ten un-rounded numeric fields each. Plants forty regressed queries with
/// ~4,000-character text (a realistic wide generated statement) and asserts the default call stays under
/// <see cref="McpResponseBudget.DefaultBytes"/>, that <c>full_text: true</c> opts back into the whole text,
/// and that the default row limit (now 30) truncates with <c>truncated</c> observed rather than inferred. New
/// file (not the shared seeding in <see cref="QueryStoreRegressionsToolTests"/>) because #4198 ran a dozen
/// lanes against this store tonight.
/// </summary>
public sealed class QueryStoreRegressionsBudgetTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "RegressionsBudgetSrv";
    private const string Db = "AppDb";
    private const int RegressedQueryCount = 40;

    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public QueryStoreRegressionsBudgetTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-regressions-budget-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = ServerName,
            IsEnabled = true,
        };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [Fact]
    public async Task GetQueryStoreRegressions_Default_StaysUnderResponseBudget_WithFortyWideRows()
    {
        var service = new LocalDataService(_duckDb);
        var baseNow = Truncate(DateTime.UtcNow);
        var queryText = BuildLongQueryText(approxLength: 4_000);

        for (var i = 0; i < RegressedQueryCount; i++)
        {
            var queryId = 1000 + i;
            await SeedAsync(baseNow.AddHours(-40), executions: 50, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 2 * i + 1, queryId: queryId, text: "SELECT 1");
            await SeedAsync(baseNow.AddMinutes(-30), executions: 200, avgDurationUs: 4000, avgCpuUs: 4000, intervalId: 2 * i + 2, queryId: queryId, text: queryText);
        }

        var defaultJson = await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName);
        var defaultRoot = Root(defaultJson);
        Assert.Equal(30, defaultRoot.GetProperty("regression_count").GetInt32());
        Assert.True(defaultRoot.GetProperty("truncated").GetBoolean());

        var defaultBytes = Encoding.UTF8.GetByteCount(defaultJson);
        Assert.True(defaultBytes < McpResponseBudget.DefaultBytes,
            $"get_query_store_regressions' default call is {defaultBytes:N0} bytes over {RegressedQueryCount} planted {queryText.Length:N0}-char rows, at or over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

        var firstRow = defaultRoot.GetProperty("regressions")[0];
        Assert.True(firstRow.GetProperty("query_text_truncated").GetBoolean());
        var previewText = firstRow.GetProperty("query_text").GetString();
        Assert.NotNull(previewText);
        Assert.True(previewText!.Length < queryText.Length, "the default call's query_text should be a preview shorter than the planted text.");

        /* full_text opts back into the whole text. */
        var fullJson = await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, full_text: true);
        Assert.DoesNotContain("\"query_text_truncated\":true", fullJson, StringComparison.Ordinal);
        var fullRoot = Root(fullJson);
        Assert.Equal(queryText, fullRoot.GetProperty("regressions")[0].GetProperty("query_text").GetString());
    }

    /// <summary>Builds SQL text near <paramref name="approxLength"/> characters, ASCII only so its length in
    /// .NET UTF-16 chars and its size in UTF-8 bytes stay close (production application-generated SQL is
    /// almost entirely ASCII: identifiers, literals, punctuation).</summary>
    private static string BuildLongQueryText(int approxLength)
    {
        var sb = new StringBuilder();
        sb.Append("INSERT INTO dbo.Widgets (Id, Name, Description, CreatedAt, OwnerId) VALUES ");
        var i = 0;
        while (sb.Length < approxLength)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"({i}, N'widget-{i}', N'a generated row from a bulk insert batch', '2026-09-25T00:00:00', {i % 97})");
            i++;
        }

        return sb.ToString();
    }

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private static DateTime Truncate(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedAsync(
        DateTime collectionTime, long executions, long avgDurationUs, long avgCpuUs, long intervalId, long queryId, string text)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads,
     runtime_stats_interval_id, query_text, last_execution_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = Db });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryId });
        cmd.Parameters.Add(new DuckDBParameter { Value = 9L });
        cmd.Parameters.Add(new DuckDBParameter { Value = "Regular" });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgDurationUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgCpuUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = 100L });
        cmd.Parameters.Add(new DuckDBParameter { Value = intervalId });
        cmd.Parameters.Add(new DuckDBParameter { Value = text });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        await cmd.ExecuteNonQueryAsync();
    }
}
