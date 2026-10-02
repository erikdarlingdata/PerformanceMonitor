/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
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
/// #4198, Lite's twin of Darling's <c>QueryStoreTopBudgetLiveTests</c> (kept in its own file for the same
/// reason <see cref="PlanCorrectionBudgetTests"/> is: several #4198 lanes touch Lite.Tests tonight, and a
/// shared seeding method would conflict across every one of them). Same shape, DuckDB instead of Postgres:
/// twenty-five rows across five query-text lengths (120 to 2,600 characters), ranked so the twenty highest-cost
/// rows (the default top=20 page) still span every length.
/// </summary>
public sealed class QueryStoreTopBudgetTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "query-store-top-budget-4198";

    private static readonly int[] QueryTextLengths = { 120, 600, 1400, 2000, 2600 };
    private const int RowCount = 25;

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public QueryStoreTopBudgetTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "QueryStoreTopBudget_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        _dataService = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(configDir);

        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(server);

        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task DefaultCall_StaysUnderTheResponseBudget_AndFullTextOptInStillGetsTheWholeStatement()
    {
        var mostExpensiveQueryText = await SeedAsync();

        var defaultAnswer = await McpQueryTools.GetQueryStoreTop(_dataService, _serverManager, ServerName);
        Assert.False(McpHelpers.IsErrorEnvelope(defaultAnswer), $"tool returned an error: {defaultAnswer}");

        var defaultBytes = Encoding.UTF8.GetByteCount(defaultAnswer);
        Assert.True(
            defaultBytes < McpResponseBudget.DefaultBytes,
            $"get_query_store_top at default arguments answered {defaultBytes} bytes, ranked from {RowCount} "
            + $"seeded rows spanning query_text lengths {string.Join(",", QueryTextLengths)} — over the "
            + $"{McpResponseBudget.DefaultBytes}-byte budget (#4198 measured 48 KB on a production store's "
            + "single server).");

        using var defaultDoc = JsonDocument.Parse(defaultAnswer);
        var defaultRoot = defaultDoc.RootElement;

        var sawTruncatedPreview = false;
        var sawUntouchedPreview = false;
        var rowCount = 0;
        foreach (var row in defaultRoot.GetProperty("queries").EnumerateArray())
        {
            rowCount++;
            var preview = row.GetProperty("query_text").GetString();
            Assert.NotNull(preview);
            var wasTruncated = row.GetProperty("query_text_truncated").GetBoolean();

            if (wasTruncated)
            {
                sawTruncatedPreview = true;
                Assert.True(preview!.Length < 2000, "a row marked query_text_truncated still carried the old 2,000-character preview.");
            }
            else
            {
                sawUntouchedPreview = true;
            }
        }

        Assert.Equal(20, rowCount);
        Assert.True(sawTruncatedPreview, "no row in the default page reported query_text_truncated=true.");
        Assert.True(sawUntouchedPreview, "no row in the default page reported query_text_truncated=false.");

        var fullTextAnswer = await McpQueryTools.GetQueryStoreTop(_dataService, _serverManager, ServerName, top: 1, full_text: true);
        Assert.False(McpHelpers.IsErrorEnvelope(fullTextAnswer), $"tool returned an error: {fullTextAnswer}");

        using var fullTextDoc = JsonDocument.Parse(fullTextAnswer);
        var fullRowEnumerator = fullTextDoc.RootElement.GetProperty("queries").EnumerateArray();
        Assert.True(fullRowEnumerator.MoveNext());
        var fullRow = fullRowEnumerator.Current;
        Assert.Equal(mostExpensiveQueryText, fullRow.GetProperty("query_text").GetString());
        Assert.False(fullRow.GetProperty("query_text_truncated").GetBoolean());
    }

    /// <summary>
    /// Twenty-five distinct (database, query_id, plan_id) groups, twenty minutes apart, with execution_count
    /// and avg_duration_us both growing with <c>i</c> so the ranking is deterministic and the single most
    /// expensive row (the last one seeded) is the one the full_text assertion checks.
    /// </summary>
    private async Task<string> SeedAsync()
    {
        var newest = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-2));
        string? mostExpensiveQueryText = null;
        var highestCost = -1L;

        for (var i = 0; i < RowCount; i++)
        {
            var collectionTime = newest.AddMinutes(-20 * (RowCount - i));
            var databaseName = $"query_store_top_budget_db_{i % 4}";
            var queryId = 5_000_000_000L + i;
            var planId = 6_000_000_000L + i;
            var queryText = BuildQueryText(i, QueryTextLengths[i % QueryTextLengths.Length]);
            var executionCount = 100L + (i * 37);
            var avgDurationUs = 5_000L + (i * 211);
            var cost = executionCount * avgDurationUs;

            if (cost > highestCost)
            {
                highestCost = cost;
                mostExpensiveQueryText = queryText;
            }

            await ExecAsync(
                """
                INSERT INTO query_store_stats
                    (collection_id, collection_time, server_id, server_name, database_name,
                     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
                     module_name, query_text, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
                     avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, avg_rowcount,
                     query_plan_hash, runtime_stats_interval_id)
                VALUES
                    ($1, $2, $3, $4, $5,
                     $6, $7, $8, $2, $2,
                     $9, $10, $11, $12, $13, $14,
                     $15, $16, $17, $18,
                     $19, $20)
                """,
                _nextId--, collectionTime, _serverId, ServerName, databaseName,
                queryId, planId, "Regular",
                i % 3 == 0 ? null : $"dbo.usp_QueryStoreBudgetProbe_{i}",
                queryText, "0xQ" + queryId, executionCount, avgDurationUs / 2, avgDurationUs,
                120.5 + i, 3.25 + (i % 5), 45.0 + i, 1_000.0 + (i * 10),
                "0xP" + planId, i);
        }

        return mostExpensiveQueryText!;
    }

    /// <summary>
    /// A synthetic but Query-Store-shaped statement, an IN-list padded out to <paramref name="length"/>
    /// characters.
    /// </summary>
    private static string BuildQueryText(int index, int length)
    {
        var sb = new StringBuilder(length + 64);
        sb.Append("SELECT o.OrderId, o.CustomerId, o.OrderDate, o.TotalAmount FROM Sales.Orders AS o ")
          .Append("WHERE o.RegionId = ").Append(index % 12).Append(" AND o.StatusCode IN (");

        var n = 0;
        while (sb.Length < length)
        {
            sb.Append(n).Append(',');
            n++;
        }

        sb.Append(") ORDER BY o.OrderDate DESC;");
        return sb.ToString()[..length];
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn == null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task ExecAsync(string sql, params object?[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
            cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
}
