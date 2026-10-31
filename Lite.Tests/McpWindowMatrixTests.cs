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
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6984: runs every MCP read tool across window sizes and <c>as_of</c> against a store whose raw data starts about
/// three days back, because #6829 then #6877 (Query Store regressions, a window past the data, then an <c>as_of</c>
/// before it) each fixed one of two cases the first report named, and no test ran a read over both. Lite's twin of the
/// all-tools fixture <see cref="McpReadToolBudgetTests"/> (same discovery and binding, its own seed).
/// <para>Contracts only, never the wording (#5833): when the requested window starts before the store's floor the
/// answer carries <c>window_truncated</c> true and, when it names one, an <c>effective_start</c> (top level or under
/// <c>hints</c>) that is not earlier than the requested start and not later than the window's end; and a read over
/// these windows does not error. A tool that breaks the contract is listed in <see cref="ExemptTools"/> with its
/// reason; a listed tool that now passes fails the test, so a fix deletes its row.</para>
/// </summary>
public sealed class McpWindowMatrixTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private static readonly string ServerName = McpReadToolBudgetTests.ServerName;

    /// <summary>Where the store's rows start (the raw floor), and the collection log starts a little earlier.</summary>
    private static readonly TimeSpan RawFloorBack = TimeSpan.FromDays(3);
    private static readonly TimeSpan LogFloorBack = TimeSpan.FromHours(84);

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly LocalDataService _dataService;
    private readonly int _serverId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public McpWindowMatrixTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-mcpwindow-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));

        _dataService = new LocalDataService(_duckDb);
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_configDir)) Directory.Delete(_configDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    /// <summary>
    /// The tools the window contract does not hold for yet, one row each with the reason. A fix deletes its row.
    /// </summary>
    private static readonly Dictionary<string, string> ExemptTools = new(StringComparer.Ordinal)
    {
    };

    public enum AsOfCase { None, PartlyInside, WhollyBefore }

    /// <summary>The three as_of cases as the offset back from now of the window's END: none is now, partly inside ends
    /// two days back (inside the three days of raw data), wholly before ends 4.5 days back (before the raw floor).</summary>
    private static TimeSpan EndBack(AsOfCase c) => c switch
    {
        AsOfCase.PartlyInside => TimeSpan.FromDays(2),
        AsOfCase.WhollyBefore => TimeSpan.FromHours(108),
        _ => TimeSpan.Zero,
    };

    [Fact]
    public async Task EveryReadTool_AnswersTheWindowContract_AcrossHoursBackAndAsOf_UnlessListed()
    {
        await SeedAsync();
        var analysisService = new AnalysisService(_duckDb) { MinimumDataHours = 0 };
        var catalog = new McpToolGuideCatalog();

        var violations = new List<(string Tool, string Scenario, string Reason)>();
        var ran = new HashSet<string>(StringComparer.Ordinal);
        var windowed = 0;

        foreach (var (name, method) in McpReadToolBudgetTests.ReadToolMethods())
        {
            var hasHours = method.GetParameters().Any(p => p.Name == "hours_back" && p.ParameterType == typeof(int));
            var hasAsOf = method.GetParameters().Any(p => p.Name == "as_of" && p.ParameterType == typeof(string));
            if (!hasHours && !hasAsOf) continue;
            windowed++;

            var hourList = hasHours ? new[] { 24, 96, 168 } : new[] { 0 };
            var asOfList = hasAsOf ? new[] { AsOfCase.None, AsOfCase.PartlyInside, AsOfCase.WhollyBefore } : new[] { AsOfCase.None };
            foreach (var hours in hourList)
            {
                foreach (var asOfCase in asOfList)
                {
                    if (!hasHours && asOfCase == AsOfCase.None) continue;
                    var scenario = (hasHours ? $"hours_back {hours}" : "default window") + ", " + asOfCase;
                    var end = DateTime.UtcNow - EndBack(asOfCase);
                    var overrides = new Dictionary<string, object?>();
                    if (hasHours) overrides["hours_back"] = hours;
                    if (hasAsOf && asOfCase != AsOfCase.None) overrides["as_of"] = end.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

                    string json;
                    try
                    {
                        var args = McpReadToolBudgetTests.BindArgs(method, _dataService, _serverManager, analysisService, catalog, new List<string>(), name, overrides);
                        json = await McpReadToolBudgetTests.InvokeAsync(method, args);
                    }
                    catch (Exception ex)
                    {
                        var inner = (ex as TargetInvocationException)?.InnerException ?? ex;
                        violations.Add((name, scenario, $"threw {inner.GetType().Name}: {inner.Message.Split('\n')[0]}"));
                        continue;
                    }

                    ran.Add(name);
                    var reason = CheckContract(json, hasHours ? end - TimeSpan.FromHours(hours) : (DateTime?)null, end);
                    if (reason is not null) violations.Add((name, scenario, reason));
                }
            }
        }

        var failing = violations.Select(v => v.Tool).ToHashSet(StringComparer.Ordinal);
        var report = string.Join(Environment.NewLine, violations.GroupBy(v => v.Tool).OrderBy(g => g.Key, StringComparer.Ordinal)
            .Select(g => $"{g.Key}: {g.First().Reason} [{g.First().Scenario}; {g.Count()} scenario(s)]"));
        TestContext.Current.TestOutputHelper?.WriteLine($"windowed tools {windowed}, failing {failing.Count}, exempt {ExemptTools.Count}" + Environment.NewLine + report);

        Assert.True(windowed > 40, $"only {windowed} read tools take hours_back or as_of -- reflection likely under-enumerated");
        var unlisted = report.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries)
            .Where(line => !ExemptTools.ContainsKey(line[..line.IndexOf(':')])).ToArray();
        Assert.True(unlisted.Length == 0, "tools that break the #6984 window contract and are not in ExemptTools:" + Environment.NewLine + string.Join(Environment.NewLine, unlisted));
        var stale = ExemptTools.Keys.Where(t => !failing.Contains(t)).ToArray();
        Assert.True(stale.Length == 0, "exempt tools that now pass -- remove their ExemptTools row:" + Environment.NewLine + string.Join(Environment.NewLine, stale));
    }

    /// <summary>The contract for one answer, null when it holds. <paramref name="requestedStart"/> is null for a tool
    /// with no hours_back (its default window is the tool's own), so only the bounds that need no start are checked.</summary>
    private static string? CheckContract(string json, DateTime? requestedStart, DateTime windowEnd)
    {
        JsonDocument doc;
        try { doc = JsonDocument.Parse(json); }
        catch (JsonException) { return "answer is not JSON"; }

        using (doc)
        {
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object) return null;

            if (root.TryGetProperty("status", out var status) && status.ValueKind == JsonValueKind.String
                && status.GetString() is "error" or "invalid")
            {
                var message = root.TryGetProperty("message", out var m) && m.ValueKind == JsonValueKind.String ? m.GetString() : "";
                return $"answered status {status.GetString()}: {message?.Split('\n')[0]}";
            }

            var truncated = Find(root, "window_truncated");
            var effective = Find(root, "effective_start");

            if (effective is { ValueKind: JsonValueKind.String } e)
            {
                if (!DateTime.TryParse(e.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var start))
                    return $"effective_start '{e.GetString()}' is not a time";
                var slack = TimeSpan.FromMinutes(2);
                if (requestedStart is DateTime req && start < req - slack) return $"effective_start {start:o} is earlier than the requested start {req:o}";
                if (start > windowEnd + slack) return $"effective_start {start:o} is later than the window end {windowEnd:o}";
            }

            /* The store's floor: the collection log starts LogFloorBack back, so a window that starts before that
               (past the slack the tools allow) is one the store cannot cover. */
            if (requestedStart is DateTime rs && rs < DateTime.UtcNow - LogFloorBack - McpQueryTools.TruncationSlack - TimeSpan.FromMinutes(5))
            {
                if (truncated is not { ValueKind: JsonValueKind.True })
                    return truncated is null ? "no window_truncated key (top level or hints) over a window that starts before the store's data" : "window_truncated is not true over a window that starts before the store's data";
            }
            return null;
        }
    }

    /// <summary>The key at the top level, else under <c>hints</c>.</summary>
    private static JsonElement? Find(JsonElement root, string key)
    {
        if (root.TryGetProperty(key, out var top)) return top;
        if (root.TryGetProperty("hints", out var hints) && hints.ValueKind == JsonValueKind.Object && hints.TryGetProperty(key, out var inHints)) return inHints;
        return null;
    }

    /* ═══════════════════════════ get_query_store_regressions: 3 x 3 ═══════════════════════════ */

    /// <summary>What the seed says each window of the regressions read is: a regression listed, a recent side with no
    /// baseline, or a window older than the raw data whose collector was running before it.</summary>
    public enum Cause { Regression, NoBaseline, AgedOut }

    [Theory]
    [InlineData(24, AsOfCase.None, Cause.Regression)]
    [InlineData(96, AsOfCase.None, Cause.NoBaseline)]
    [InlineData(168, AsOfCase.None, Cause.NoBaseline)]
    [InlineData(24, AsOfCase.PartlyInside, Cause.NoBaseline)]
    [InlineData(96, AsOfCase.PartlyInside, Cause.NoBaseline)]
    [InlineData(168, AsOfCase.PartlyInside, Cause.NoBaseline)]
    [InlineData(24, AsOfCase.WhollyBefore, Cause.AgedOut)]
    [InlineData(96, AsOfCase.WhollyBefore, Cause.AgedOut)]
    [InlineData(168, AsOfCase.WhollyBefore, Cause.AgedOut)]
    public async Task GetQueryStoreRegressions_NamesTheCauseTheSeedHolds(int hours, AsOfCase asOfCase, Cause expected)
    {
        await SeedAsync();
        /* Seed (SeedAsync): Query Store rows at 3 days back (the floor, 1 ms) and 30 minutes back (the 55 ms regression), the
           collector's logged runs from 30 days back. Recent window = hours before the end; baseline = the 7 days before that.
           End now: 24 h holds both rows (baseline = the floor row, recent = the regression); 96 h and 168 h hold both in the recent
           window, so no baseline. End 2 days 6 h back (inside the raw data): the floor row is the only recent row, no baseline.
           End 4.5 days back (before the raw data): no row on either side, and the collector ran before, so the data aged out. */
        var asOf = asOfCase == AsOfCase.None ? null
            : (DateTime.UtcNow - EndBack(asOfCase) - (asOfCase == AsOfCase.PartlyInside ? TimeSpan.FromHours(6) : TimeSpan.Zero))
                .ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

        var json = await McpQueryTools.GetQueryStoreRegressions(_dataService, _serverManager, ServerName, hours_back: hours, as_of: asOf);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        var floor = DateTime.UtcNow - RawFloorBack;

        var truncated = Find(root, "window_truncated");
        Assert.True(truncated is { ValueKind: JsonValueKind.True }, "the baseline starts before the store's rows in every scenario, so window_truncated must be true: " + json);
        var effective = Find(root, "effective_start");
        Assert.True(effective is { ValueKind: JsonValueKind.String }, "effective_start must name where the store's rows start: " + json);
        var start = DateTime.Parse(effective!.Value.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        Assert.InRange((start - floor).TotalMinutes, -10, 10);

        switch (expected)
        {
            case Cause.Regression:
                Assert.False(root.TryGetProperty("status", out _), json);
                Assert.True(root.GetProperty("regression_count").GetInt32() > 0, json);
                break;
            case Cause.NoBaseline:
                Assert.Equal("unavailable", root.GetProperty("status").GetString());
                Assert.Contains("no Query Store capture in the 7-day baseline window", root.GetProperty("message").GetString(), StringComparison.Ordinal);
                break;
            case Cause.AgedOut:
                Assert.Equal("unavailable", root.GetProperty("status").GetString());
                var message = root.GetProperty("message").GetString()!;
                Assert.Contains("is older than this server's raw Query Store data, which starts " + floor.ToString("MMM d", CultureInfo.InvariantCulture), message, StringComparison.Ordinal);
                Assert.DoesNotContain("No Query Store data has EVER been collected", message, StringComparison.Ordinal);
                break;
        }
    }

    /* ═══════════════════════════ the fixture ═══════════════════════════ */

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(
        new DateTime(utc.Ticks - (utc.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    /// <summary>
    /// Raw data from <see cref="RawFloorBack"/> back (Query Store rows: a baseline interval at the floor and a regressed one
    /// 30 minutes back) and a collection log that starts earlier, <see cref="LogFloorBack"/> back, for every SQL Server collector,
    /// with the Query Store collector also logged 30 days back (it ran long before the raw rows that are left).
    /// </summary>
    private async Task SeedAsync()
    {
        var now = DateTime.UtcNow;
        var conn = await SeedConnectionAsync();
        using var batch = new SeedBatch(_duckDb, conn);

        foreach (var (back, executions, avgUs, interval) in new[] { (RawFloorBack, 50L, 1_000L, 1L), (TimeSpan.FromMinutes(30), 480L, 55_000L, 2L) })
        {
            for (var q = 0; q < 3; q++)
            {
                await ExecAsync(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads,
     runtime_stats_interval_id, query_text, last_execution_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
                    _nextId--, Naive(now - back), _serverId, ServerName, "TenantDb00", 1000L + q, 9L, "Regular",
                    executions, avgUs, avgUs, 100L, interval * 10 + q, "SELECT " + q, Naive(now - back));
            }
        }

        var collectors = CollectorCatalog.All.Where(d => d.TargetEngine == CollectorTargetEngine.SqlServer).Select(d => d.Name).ToArray();
        foreach (var collector in collectors)
        {
            var backs = new List<TimeSpan> { LogFloorBack, TimeSpan.FromDays(2), TimeSpan.FromDays(1), TimeSpan.FromMinutes(40), TimeSpan.FromMinutes(5) };
            if (collector == "query_store") backs.Insert(0, TimeSpan.FromDays(30));
            foreach (var back in backs)
            {
                await ExecAsync(@"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                    _nextId--, _serverId, ServerName, collector, Naive(now - back), 100.0, "SUCCESS", null, 10, 80.0, 20.0);
            }
        }

        batch.Commit();
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
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
}
