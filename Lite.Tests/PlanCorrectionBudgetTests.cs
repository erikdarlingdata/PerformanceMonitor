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
/// #4198, Lite's twin of Darling's <see cref="PlanCorrectionBudgetLiveTests"/> (kept in its own file for the
/// same reason: several #4198 lanes touch Lite.Tests tonight, and a shared seeding method would conflict
/// across every one of them). Same shape, DuckDB instead of Postgres: fifty-one rows across five query-text
/// lengths (100 to 4,600 characters), four of them sharing the newest <c>collection_time</c> across four
/// databases the way one capture cycle would.
/// </summary>
public sealed class PlanCorrectionBudgetTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "plan-correction-budget-4198";

    private static readonly int[] QueryTextLengths = { 100, 2200, 2800, 3600, 4600 };
    private const int RowCount = 51;

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public PlanCorrectionBudgetTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "PlanCorrectionBudget_" + Guid.NewGuid().ToString("N")[..8]);
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
        var longestQueryText = await SeedAsync();

        var defaultAnswer = await McpPlanCorrectionTools.GetPlanCorrections(_dataService, _serverManager, ServerName);
        Assert.False(McpHelpers.IsErrorEnvelope(defaultAnswer), $"tool returned an error: {defaultAnswer}");

        var defaultBytes = Encoding.UTF8.GetByteCount(defaultAnswer);
        Assert.True(
            defaultBytes < McpResponseBudget.DefaultBytes,
            $"get_plan_corrections at default arguments answered {defaultBytes} bytes, paged from {RowCount} "
            + $"seeded rows spanning query_text lengths {string.Join(",", QueryTextLengths)} — over the "
            + $"{McpResponseBudget.DefaultBytes}-byte budget (#4198 measured 95,428 on a production store's "
            + "single busiest server).");

        using var defaultDoc = JsonDocument.Parse(defaultAnswer);
        var defaultRoot = defaultDoc.RootElement;
        Assert.True(defaultRoot.GetProperty("truncated").GetBoolean());
        Assert.Equal(25, defaultRoot.GetProperty("recommendations_returned").GetInt32());
        Assert.Equal(4, defaultRoot.GetProperty("automatic_tuning").GetArrayLength());

        var sawTruncatedPreview = false;
        var sawUntouchedPreview = false;
        foreach (var row in defaultRoot.GetProperty("recommendations").EnumerateArray())
        {
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

        Assert.True(sawTruncatedPreview, "no row in the default page reported query_text_truncated=true.");
        Assert.True(sawUntouchedPreview, "no row in the default page reported query_text_truncated=false.");

        var fullTextAnswer = await McpPlanCorrectionTools.GetPlanCorrections(_dataService, _serverManager, ServerName, limit: 1, full_text: true);
        Assert.False(McpHelpers.IsErrorEnvelope(fullTextAnswer), $"tool returned an error: {fullTextAnswer}");

        using var fullTextDoc = JsonDocument.Parse(fullTextAnswer);
        var fullTextRoot = fullTextDoc.RootElement;
        var fullRowEnumerator = fullTextRoot.GetProperty("recommendations").EnumerateArray();
        Assert.True(fullRowEnumerator.MoveNext());
        var fullRow = fullRowEnumerator.Current;
        Assert.Equal(longestQueryText, fullRow.GetProperty("query_text").GetString());
        Assert.False(fullRow.GetProperty("query_text_truncated").GetBoolean());
    }

    private async Task<string> SeedAsync()
    {
        var newest = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-2));
        string? longestQueryText = null;

        for (var i = 0; i < RowCount; i++)
        {
            var collectionTime = i >= RowCount - 4
                ? newest
                : newest.AddMinutes(-20 * (RowCount - i));

            var databaseName = $"plan_correction_budget_db_{i % 4}";
            var queryText = BuildQueryText(i, QueryTextLengths[i % QueryTextLengths.Length]);
            var wasActioned = i % 5 == 4;

            if (collectionTime == newest && (longestQueryText == null || queryText.Length > longestQueryText.Length))
            {
                longestQueryText = queryText;
            }

            await ExecAsync(
                """
                INSERT INTO plan_correction
                    (collection_id, collection_time, server_id, server_name, database_name,
                     force_last_good_plan_desired_state, force_last_good_plan_actual_state, force_last_good_plan_reason,
                     recommendation_name, recommendation_state, recommendation_state_reason, recommendation_reason,
                     valid_since, last_refresh, score, query_id, query_text, regressed_plan_id, last_good_plan_id,
                     last_good_plan_forcing_type, last_good_plan_is_forced, last_good_plan_force_failure_reason,
                     regressed_plan_execution_count, regressed_plan_cpu_time_average_ms,
                     last_good_plan_execution_count, last_good_plan_cpu_time_average_ms,
                     estimated_gain_seconds, execute_action_initiated_by, execute_action_initiated_time,
                     revert_action_initiated_by, revert_action_initiated_time)
                VALUES
                    ($1, $2, $3, $4, $5,
                     $6, $6, $7,
                     $8, $9, $10, $11,
                     $2, $2, $12, $13, $14, $15, $16,
                     $17, $18, $19,
                     $20, $21,
                     $22, $23,
                     $24, $25, $26,
                     $25, $26)
                """,
                _nextId--, Naive(collectionTime), _serverId, ServerName, databaseName,
                "Enabled", "Automatic tuning option is enabled at the database level.",
                $"PlanRegression_budget_probe_{i}", "Active", "A new plan was recommended after a regression was detected.",
                i % 3 == 0 ? "Query Duration Increased" : "Query CPU Time Increased",
                50 + (i % 50), 1_000_000_000L + i, queryText, 2_000_000_000L + i, 3_000_000_000L + i,
                "Auto", i % 2 == 0, null,
                500L + i, 120.5 + i, 500L + i, 45.25 + i,
                3.5 + (i % 10),
                wasActioned ? "AUTOMATIC" : null,
                wasActioned ? Naive(collectionTime) : (DateTime?)null);
        }

        return longestQueryText!;
    }

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

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
}
