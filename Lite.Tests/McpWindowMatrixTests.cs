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
    /// The tools the window contract does not hold for yet, one row each with the reason. A fix deletes its row. Empty since #7005 part 3: all 60 windowed tools answer the contract.
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
            /* The investigation store and brief composer are not part of the collected store this test seeds, and BindArgs cannot build them. */
            if (method.GetParameters().Any(p => p.ParameterType.Name == "IInvestigationRequestStore")) continue;
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
                    if (method.GetParameters().Any(p => p.Name == "counter_name")) overrides["counter_name"] = "Batch Requests/sec";
                    /* #7005 part 4: get_wait_trend's wait_type is a required string, so BindArgs passes null, which no MCP client can send; with a wait_stats row in the store its miss path then
                       throws on waitType.Trim(). The seeded wait type is what a caller would ask for. */
                    if (method.GetParameters().Any(p => p.Name == "wait_type")) overrides["wait_type"] = "CXPACKET";
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
                    var requestedStart = hasHours ? end - TimeSpan.FromHours(hours) : (DateTime?)null;
                    /* The regressions tool compares the 7 days BEFORE the window, so its effective_start is that baseline's start; the collection log's floor is the Query Store collector's 30-day-old run. */
                    var lowerBound = name == "get_query_store_regressions" && requestedStart is DateTime rsStart ? rsStart - TimeSpan.FromDays(7) : requestedStart;
                    var floorBack = name == "get_collection_log" ? TimeSpan.FromDays(30) : LogFloorBack;
                    var reason = CheckContract(json, requestedStart, lowerBound, end, floorBack);
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
    private static string? CheckContract(string json, DateTime? requestedStart, DateTime? lowerBound, DateTime windowEnd, TimeSpan floorBack)
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
                if (lowerBound is DateTime req && start < req - slack) return $"effective_start {start:o} is earlier than the requested start {req:o}";
                /* #6885: a window that ends before the store's first row answers effective_start = that first row (after the window end), so only that window skips the "not later than the window end" check. */
                var endsBeforeStore = windowEnd < DateTime.UtcNow - RawFloorBack;
                if (!endsBeforeStore && start > windowEnd + slack) return $"effective_start {start:o} is later than the window end {windowEnd:o}";
            }

            /* The store's floor: the collection log starts LogFloorBack back, so a window that starts before that
               (past the slack the tools allow) is one the store cannot cover. */
            if (requestedStart is DateTime rs && rs < DateTime.UtcNow - floorBack - McpQueryTools.TruncationSlack - TimeSpan.FromMinutes(5))
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

    /* ═══════════════════════════ #7005 part 3: the floor rules ═══════════════════════════ */

    private static JsonElement? Hint(JsonElement root, string key) => Find(root, key);

    /// <summary>The notice answers "does the store hold data this far back", never "did anything happen": an alert log with no alert over a
    /// window the collectors cover is an empty answer that says covered; a window that starts before the data says truncated.</summary>
    [Theory]
    [InlineData(24, false)]
    [InlineData(168, true)]
    public async Task GetAlertHistory_EmptyAlertLog_IsTruncatedOnlyWhenTheStoreLacksTheWindow(int hours, bool truncated)
    {
        await SeedAsync();
        var json = await McpAlertTools.GetAlertHistory(_dataService, hours_back: hours);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal(truncated, Hint(doc.RootElement, "window_truncated")?.GetBoolean());
    }

    /// <summary>A window wholly before the store's first collected row (#7005 part 4): the empty alert log says truncated and names the first
    /// row the collectors kept as <c>effective_start</c>, not the no-collection answer (which has no start to name).</summary>
    [Fact]
    public async Task GetAlertHistory_WindowWhollyBeforeTheFloor_NamesTheFirstKeptRow()
    {
        await SeedAsync();
        var end = DateTime.UtcNow - EndBack(AsOfCase.WhollyBefore);
        var json = await McpAlertTools.GetAlertHistory(_dataService, hours_back: 24, as_of: end.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture));
        using var doc = JsonDocument.Parse(json);
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
        Assert.True(Hint(doc.RootElement, "window_truncated")?.GetBoolean());
        var start = DateTime.Parse(Hint(doc.RootElement, "effective_start")!.Value.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        Assert.InRange((DateTime.UtcNow - RawFloorBack - start).Duration(), TimeSpan.Zero, TimeSpan.FromMinutes(5));
    }

    /// <summary>The floor of the analysis reads is the oldest analysis run the store holds for the server; a store with no run at all answers
    /// as it did before, with no notice.</summary>
    [Fact]
    public async Task GetAnalysisFindings_StoreWithNoRun_CarriesNoNotice()
    {
        var json = await McpAnalysisTools.GetAnalysisFindings(new AnalysisService(_duckDb) { MinimumDataHours = 0 }, _serverManager, ServerName, hours_back: 168);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
        Assert.Null(Hint(doc.RootElement, "window_truncated"));
    }

    [Theory]
    [InlineData(24, false)]
    [InlineData(168, true)]
    public async Task GetAnalysisFindings_WindowOlderThanTheOldestRun_SaysTruncated(int hours, bool truncated)
    {
        await SeedAsync();
        var json = await McpAnalysisTools.GetAnalysisFindings(new AnalysisService(_duckDb) { MinimumDataHours = 0 }, _serverManager, ServerName, hours_back: hours);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(truncated, Hint(doc.RootElement, "window_truncated")?.GetBoolean());
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

        /* #7005: the Queries tools' other probe tables, so a tool that says nothing over an empty table is told apart from one that
           breaks the contract over rows: query_stats and procedure_stats, a row at the raw floor and one 30 minutes back. */
        foreach (var back in new[] { RawFloorBack, TimeSpan.FromMinutes(30) })
        {
            await ExecAsync(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_hash, sql_handle, last_execution_time, delta_execution_count,
     delta_worker_time, delta_elapsed_time, sample_interval_seconds, query_text)
VALUES ($1, $2, $3, $4, 'TenantDb00', '0xAB', '0xH0xAB', $5, 10, 5000, 5000, 60, 'SELECT 1')",
                _nextId--, Naive(now - back), _serverId, ServerName, Naive(now - back));
            await ExecAsync(@"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
     execution_count, total_worker_time, total_elapsed_time, total_logical_reads, total_physical_reads, total_logical_writes,
     delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'TenantDb00', 'dbo', 'p1', 'PROCEDURE', 10, 5000, 5000, 1, 1, 1, 10, 5000, 5000, 60)",
                _nextId--, Naive(now - back), _serverId, ServerName);
        }

        /* #7005 part 2: the probe tables of the periodic collectors the trend and snapshot reads use, a row at the raw floor and one 30 minutes
           back, so a tool that passes did it by saying so over rows, not because its table is empty. Only the columns the schema requires. */
        foreach (var back in new[] { RawFloorBack, TimeSpan.FromMinutes(30) })
        {
            var at = Naive(now - back);
            await ExecAsync("INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time) VALUES ($1, $2, $3, $4, $5)", _nextId--, at, _serverId, ServerName, at);
            foreach (var table in new[] { "memory_stats", "memory_grant_stats", "file_io_stats", "tempdb_stats", "cpu_scheduler_stats" })
            {
                await ExecAsync($"INSERT INTO {table} (collection_id, collection_time, server_id, server_name) VALUES ($1, $2, $3, $4)", _nextId--, at, _serverId, ServerName);
            }

            /* #7005 part 4: a wait_stats row the way the collector writes it (every payload column set, so the deltas and the interval are real), which is what the alert floor reads. */
            await ExecAsync("INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds) VALUES ($1, $2, $3, $4, 'CXPACKET', 10, 100, 10, 5, 50, 5, 60)", _nextId--, at, _serverId, ServerName);
            await ExecAsync("INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name) VALUES ($1, $2, $3, $4, 'SQLServer:SQL Statistics', 'Batch Requests/sec')", _nextId--, at, _serverId, ServerName);
            await ExecAsync("INSERT INTO plan_cache_stats (collection_id, collection_time, server_id, server_name, cacheobjtype, objtype) VALUES ($1, $2, $3, $4, 'Compiled Plan', 'Adhoc')", _nextId--, at, _serverId, ServerName);
            await ExecAsync("INSERT INTO latch_stats (collection_id, collection_time, server_id, server_name, latch_class) VALUES ($1, $2, $3, $4, 'BUFFER')", _nextId--, at, _serverId, ServerName);
            await ExecAsync("INSERT INTO spinlock_stats (collection_id, collection_time, server_id, server_name, spinlock_name) VALUES ($1, $2, $3, $4, 'LOCK_HASH')", _nextId--, at, _serverId, ServerName);
        }

        /* #7005 part 3: the analysis runs get_analysis_findings and get_analysis_facts take their floor from, a run at the raw floor and one 30 minutes back. */
        foreach (var (back, id) in new[] { (RawFloorBack, 1L), (TimeSpan.FromMinutes(30), 2L) })
        {
            await ExecAsync(@"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, severity, confidence, category,
     story_path, story_path_hash, story_text, root_fact_key, fact_count, incident_id)
VALUES ($1, $2, $3, $4, 0.5, 0.5, 'waits', 'CXPACKET', 'hash-' || $1::VARCHAR, 'CXPACKET leads', 'CXPACKET', 1, 'inc-1')",
                id, Naive(now - back), _serverId, ServerName);
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
