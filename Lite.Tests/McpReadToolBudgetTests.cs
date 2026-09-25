/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using ModelContextProtocol.Server;
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
/// #4198 part 2, Lite's twin of Darling's <c>Darling.Tests.McpReadToolBudgetLiveTests</c>. Enumerates every
/// <c>[McpServerTool]</c> method on every <c>[McpServerToolType]</c> class in the Lite service assembly BY
/// REFLECTION (<see cref="AllToolMethods"/>) so a brand-new tool inherits this test with no line changing here,
/// excludes the same surface #4198 excluded on Darling (write tools, <c>analyze_*</c>, <c>compare_*</c>,
/// <c>audit_config</c> -- see <see cref="IsExcluded"/> and <see cref="WriteToolRoster"/>), binds each remaining
/// tool's parameters generically (<see cref="BindArgs"/>: its DI services, <c>server_name</c> at the seeded
/// server, everything else at its declared default) and asserts the UTF-8 reply is under
/// <see cref="McpResponseBudget.DefaultBytes"/> -- unless the tool is named in <see cref="ExemptOffenders"/>,
/// today's #4198 backlog. A fix lands by deleting its row: an exempt tool that now fits fails this test until
/// its row is gone, and a tool that is NOT exempt and goes over fails it too.
///
/// <para><b>Why a roster, not a heuristic, for write tools.</b> Lite serves 89 MCP tools and none of their
/// names is add_servers/create_*/update_*/delete_*/remove_server/set_*_enabled -- Lite has no server-onboarding,
/// custom-view, custom-alert-rule or notification-route MCP surface at all. Checked against every tool name in
/// the assembly (not just a grep for a mutating verb): the only tool outside <c>get_*</c>/<c>list_servers</c>/
/// <c>analyze_*</c>/<c>compare_*</c>/<c>audit_config</c> is <c>mute_analysis_finding</c>, so it is the whole
/// write surface. <see cref="WriteToolRoster"/> stays an exact, hand-maintained list regardless, so a new write
/// tool has to be added to it on purpose, or this test will actually invoke it.</para>
///
/// <para><b>The fixture</b> is DuckDB only -- no live rig, no Postgres, no fleet simulation (Lite has no
/// <c>get_fleet_overview</c>). One busy server carries query-store regressions with ~4 KB of text each
/// (baseline interval past the default 24h recent window, then a stepped-up regressed interval -- the #4198
/// field offender and Darling's #1 measured tool, same shape <c>QueryStoreRegressionsBudgetTests</c> already
/// proves against this schema), 30 multi-process deadlock graphs, 30 blocked-process reports with report XML,
/// 10 DMV-sourced blocking snapshots, every SQL Server collector's worth of collection-log history with mixed
/// SUCCESS/ERROR rows so nothing compacts away, plan-correction recommendations across several databases, and
/// a page of long query completions -- the same seven tables Darling's twin seeds, using the exact column
/// lists <c>McpPageContractTests</c>, <c>QueryStoreRegressionsBudgetTests</c> and <c>QueryStoreTopBudgetTests</c>
/// already prove against Lite's DuckDB schema (generated from the same engine-neutral collector catalog
/// Darling's Postgres schema is, per <c>DuckDbSchemaGenerator</c>'s own remarks).</para>
///
/// <para><b>Known gaps.</b> A tool whose required parameter has no default and is not one of the DI services
/// below gets a placeholder (0/false/null) instead -- it likely answers empty/not-found and passes trivially,
/// because this fixture does not seed its backing rows/state. Object-level stats (<c>get_index_usage</c>/
/// <c>get_object_locking</c>, backed by <c>index_object_stats</c>), DMV query stats (<c>get_query_heatmap</c>/
/// <c>get_top_queries_by_cpu</c>/<c>get_top_procedures_by_cpu</c>, backed by <c>query_stats</c>), analysis
/// findings (<c>get_analysis_findings</c>), and every single-row drill-down (<c>get_plan_xml</c>, ...) are in
/// this bucket, matching Darling's twin's own gaps for the same tables; each run's GAPS output (and the PR
/// body) names exactly which.</para>
/// </summary>
public sealed class McpReadToolBudgetTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "mcp-read-tool-budget-4198";

    private const int DbCount = 4;
    private const int QueriesPerDb = 10;
    private const int DeadlockRows = 30;
    private const int BlockedRows = 30;
    private const int DmvBlockingRows = 10;
    private const int CollectionLogSamplesPerCollector = 10;
    private const int PlanCorrectionRows = 30;
    private const int LongQueryRows = 20;

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly LocalDataService _dataService;
    private readonly int _serverId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public McpReadToolBudgetTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-mcpbudget-" + Guid.NewGuid().ToString("N")[..8]);
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
    /// The write surface #4198 excludes from its measurement, same reasoning as Darling's twin: invoking a
    /// write tool during a "measure every read tool" pass would mutate the fixture instead of just reading it.
    /// See the class remarks for how this was confirmed to be the whole write surface.
    /// </summary>
    private static readonly HashSet<string> WriteToolRoster = new(StringComparer.Ordinal)
    {
        "mute_analysis_finding",
    };

    /// <summary>
    /// #4198's per-tool backlog, measured on THIS fixture: name -&gt; bytes at the time the row was added. A
    /// row is removed by whichever lane fixes that tool's defaults; the test fails if a listed tool now fits
    /// (a stale exemption hiding a real fix) as loudly as it fails for a new, un-exempted offender.
    /// </summary>
    private static readonly Dictionary<string, int> ExemptOffenders = new(StringComparer.Ordinal)
    {
        /* #4268 (merged) compacts only HEALTHY collectors with nothing to report; this fixture's collectors are
           deliberately not healthy (mixed SUCCESS/ERROR), so nothing compacts and the reply is still over.
           Measured on this fixture at 43,115 B. Stays open under #4198, like Darling's twin row. */
        ["get_collection_health"] = 43_115,          // #4198
    };

    [Fact]
    public async Task EveryReadToolAnswersUnderTheDefaultBudget_UnlessListedAsA4198Offender()
    {
        await SeedAsync();

        var analysisService = new AnalysisService(_duckDb) { MinimumDataHours = 0 };
        var catalog = new McpToolGuideCatalog();

        var measured = new List<(string Name, int Bytes)>();
        var gaps = new List<string>();

        foreach (var (name, method) in ReadToolMethods())
        {
            var args = BindArgs(method, _dataService, _serverManager, analysisService, catalog, gaps, name);
            string json;
            try
            {
                json = await InvokeAsync(method, args);
            }
            catch (Exception ex)
            {
                var inner = (ex as TargetInvocationException)?.InnerException ?? ex;
                gaps.Add($"{name}: threw {inner.GetType().Name}: {inner.Message.Split('\n')[0]}");
                continue;
            }

            measured.Add((name, Encoding.UTF8.GetByteCount(json)));
        }

        var report = string.Join(Environment.NewLine, measured.OrderByDescending(m => m.Bytes).Select(m =>
            $"{m.Name}: {m.Bytes:N0} B{(ExemptOffenders.ContainsKey(m.Name) ? " (exempt, #4198 per-tool lane)" : m.Bytes > McpResponseBudget.DefaultBytes ? " OVER" : "")}"));
        TestContext.Current.TestOutputHelper?.WriteLine(report);
        if (gaps.Count > 0)
            TestContext.Current.TestOutputHelper?.WriteLine("GAPS (no usable default; passes trivially):" + Environment.NewLine + string.Join(Environment.NewLine, gaps));

        var overBudget = measured
            .Where(m => m.Bytes > McpResponseBudget.DefaultBytes && !ExemptOffenders.ContainsKey(m.Name))
            .Select(m => $"{m.Name}: {m.Bytes:N0} B")
            .ToArray();
        var staleExemptions = ExemptOffenders.Keys
            .Where(name => measured.Any(m => m.Name == name && m.Bytes <= McpResponseBudget.DefaultBytes))
            .ToArray();
        var missingExemptTools = ExemptOffenders.Keys.Where(name => measured.All(m => m.Name != name)).ToArray();

        /* 89 tools total, minus mute_analysis_finding, five analyze_*, compare_analysis and audit_config = 81
           read tools attempted; a tool with no usable default throws and lands in gaps, not measured, so the
           floor is set under 81 rather than at it. */
        Assert.True(measured.Count > 60, $"only {measured.Count} read tools were measured -- reflection likely under-enumerated the service assembly");
        Assert.True(overBudget.Length == 0,
            "over the #4198 default budget and not in ExemptOffenders:" + Environment.NewLine + string.Join(Environment.NewLine, overBudget) +
            Environment.NewLine + "GAPS:" + Environment.NewLine + string.Join(Environment.NewLine, gaps));
        Assert.True(staleExemptions.Length == 0,
            "exempt tools that now fit the budget -- remove their ExemptOffenders row:" + Environment.NewLine + string.Join(Environment.NewLine, staleExemptions));
        Assert.True(missingExemptTools.Length == 0,
            "ExemptOffenders names a tool that was not measured (renamed, or newly excluded):" + Environment.NewLine + string.Join(Environment.NewLine, missingExemptTools));

        var allNames = AllToolMethods().Select(x => x.Name).ToHashSet(StringComparer.Ordinal);
        Assert.All(WriteToolRoster, w => Assert.Contains(w, allNames));
    }

    /* ═══════════════════════════ reflection: enumerate, exclude, bind, invoke ═══════════════════════════ */

    /// <summary>Every <c>[McpServerTool]</c> method on every <c>[McpServerToolType]</c> class in the Lite
    /// service assembly -- read AND write, so <see cref="WriteToolRoster"/> can be sanity-checked against it.</summary>
    private static (string Name, MethodInfo Method)[] AllToolMethods()
    {
        Type[] types;
        try
        {
            types = typeof(McpQueryTools).Assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            Assert.Fail("the service assembly did not fully load, so a missing tool would look like a passing pin: " +
                string.Join("; ", ex.LoaderExceptions.Where(e => e is not null).Select(e => e!.Message).Distinct()));
            return Array.Empty<(string, MethodInfo)>();
        }

        return types
            .Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() is not null)
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Static))
            .Select(m => (Attr: m.GetCustomAttribute<McpServerToolAttribute>(), Method: m))
            .Where(x => x.Attr is not null)
            .Select(x => (Name: x.Attr!.Name!, Method: x.Method))
            .OrderBy(x => x.Name, StringComparer.Ordinal)
            .ToArray();
    }

    private static bool IsExcluded(string name) =>
        name.StartsWith("analyze_", StringComparison.Ordinal) ||
        name.StartsWith("compare_", StringComparison.Ordinal) ||
        name == "audit_config" ||
        WriteToolRoster.Contains(name);

    private static (string Name, MethodInfo Method)[] ReadToolMethods() =>
        AllToolMethods().Where(x => !IsExcluded(x.Name)).ToArray();

    /// <summary>Binds one tool's parameters: its DI services, <c>server_name</c> at the seeded server, every
    /// other optional parameter at its declared default. A required parameter that is none of those gets a
    /// placeholder and a <paramref name="gaps"/> note -- see the class remarks' "Known gaps".</summary>
    private static object?[] BindArgs(MethodInfo method, LocalDataService dataService, ServerManager serverManager,
        AnalysisService analysisService, McpToolGuideCatalog catalog, List<string> gaps, string toolName)
    {
        var parameters = method.GetParameters();
        var args = new object?[parameters.Length];
        for (var i = 0; i < parameters.Length; i++)
        {
            var p = parameters[i];
            if (p.ParameterType == typeof(LocalDataService)) { args[i] = dataService; continue; }
            if (p.ParameterType == typeof(ServerManager)) { args[i] = serverManager; continue; }
            if (p.ParameterType == typeof(AnalysisService)) { args[i] = analysisService; continue; }
            if (p.ParameterType == typeof(McpToolGuideCatalog)) { args[i] = catalog; continue; }
            if (p.Name == "server_name") { args[i] = ServerName; continue; }
            if (p.HasDefaultValue) { args[i] = p.DefaultValue; continue; }

            gaps.Add($"{toolName}: '{p.Name}' ({p.ParameterType.Name}) has no default and is not a known DI service -- placeholder passed, likely answers empty/invalid.");
            args[i] = p.ParameterType.IsValueType ? Activator.CreateInstance(p.ParameterType) : null;
        }
        return args;
    }

    /// <summary>Every MCP tool method returns either <c>string</c> (<c>get_tool_guide</c>) or <c>Task&lt;string&gt;</c>
    /// (everything else); this is the reply an MCP client would receive either way.</summary>
    private static async Task<string> InvokeAsync(MethodInfo method, object?[] args)
    {
        var result = method.Invoke(null, args);
        return result switch
        {
            Task<string> t => await t,
            string s => s,
            _ => throw new InvalidOperationException($"{method.Name} returned {(result?.GetType().Name ?? "null")}, expected string or Task<string>"),
        };
    }

    /* ═══════════════════════════ the fixture: one busy server, DuckDB only ═══════════════════════════ */

    private static string PadBlock(int approxChars)
    {
        const string Chunk = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_";
        var sb = new StringBuilder(approxChars + Chunk.Length);
        while (sb.Length < approxChars) sb.Append(Chunk);
        return sb.ToString();
    }

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private async Task SeedAsync()
    {
        var now = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-1));

        /* query_store_stats: DbCount tenant databases x QueriesPerDb queries, a baseline interval (40h back)
           and a regressed recent interval (30m back, duration/CPU stepped up), carrying ~3.8 KB of
           parameterized query text -- the #4198 field offender and Darling's #1 measured tool.
           get_query_store_regressions' "baseline" is everything collected BEFORE the default 24h recent
           window, so the baseline capture has to sit past that cutoff or the comparison sees no baseline at
           all (QueryStoreRegressionsBudgetTests' proven shape). */
        for (var db = 0; db < DbCount; db++)
        {
            var dbName = $"TenantDb{db:00}";
            for (var q = 0; q < QueriesPerDb; q++)
            {
                var queryId = 1000L + (db * 100) + q;
                var text = $"EXEC dbo.usp_ProcessOrder_{q} @OrderId = {5000 + q}, @TenantId = {db}, @Payload = N'" + PadBlock(3800) + "'";
                await SeedQueryStoreAsync(now.AddHours(-40), executions: 50, avgDurationUs: 1_000, avgCpuUs: 1_000, intervalId: 2 * (db * 100 + q) + 1, queryId, dbName, "SELECT 1");
                await SeedQueryStoreAsync(now.AddMinutes(-30), executions: 480, avgDurationUs: 55_000, avgCpuUs: 48_000, intervalId: 2 * (db * 100 + q) + 2, queryId, dbName, text);
            }
        }

        /* deadlocks: multi-process graphs the width of a real one. */
        for (var i = 0; i < DeadlockRows; i++)
        {
            var t = now.AddMinutes(-2 * i);
            var db = $"TenantDb{i % DbCount:00}";
            var xml = "<deadlock><victim-list><victimProcess id=\"process" + i + "a\"/></victim-list><process-list>" +
                "<process id=\"process" + i + "a\" waitresource=\"KEY: 7:72057594043170816 (fedcba9876543210)\" lockMode=\"X\" isolationlevel=\"read committed\">" +
                "<executionStack><frame procname=\"" + db + ".dbo.usp_UpdateOrder\" line=\"18\">UPDATE dbo.Orders SET Status = @NewStatus WHERE OrderId = @OrderId</frame></executionStack>" +
                "<inputbuf>" + PadBlock(1200) + "</inputbuf></process>" +
                "<process id=\"process" + i + "b\" waitresource=\"KEY: 7:72057594043170816 (0123456789abcdef)\" lockMode=\"S\" isolationlevel=\"read committed\">" +
                "<executionStack><frame procname=\"" + db + ".dbo.usp_ReadOrder\" line=\"9\">SELECT * FROM dbo.Orders WHERE OrderId = @OrderId</frame></executionStack>" +
                "<inputbuf>" + PadBlock(1200) + "</inputbuf></process></process-list></deadlock>";
            await ExecAsync(
                "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                _nextId--, Naive(t), _serverId, ServerName, Naive(t), "process" + i + "a",
                "UPDATE dbo.Orders SET Status = @NewStatus WHERE OrderId = @OrderId", xml);
        }

        /* blocked_process_reports: report XML the width of a real one. */
        for (var i = 0; i < BlockedRows; i++)
        {
            var t = now.AddMinutes(-90).AddSeconds(-3 * i);
            var db = $"TenantDb{i % DbCount:00}";
            var xml = "<blocked-process-report><blocked-process><process spid=\"" + (50 + i) + "\" waitresource=\"KEY\">" +
                "<inputbuf>" + PadBlock(1000) + "</inputbuf></process></blocked-process>" +
                "<blocking-process><process spid=\"" + (150 + i) + "\"><inputbuf>" + PadBlock(1000) + "</inputbuf></process></blocking-process></blocked-process-report>";
            await ExecAsync(
                @"INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text, blocked_process_report_xml, contentious_object)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
                _nextId--, Naive(t), _serverId, ServerName, Naive(t), db, 50 + i, 150 + i, 4000L + i, "X",
                "UPDATE dbo.Orders SET Status = @NewStatus WHERE OrderId = @OrderId", "SELECT * FROM dbo.Orders WHERE OrderId = @OrderId", xml, db + ".dbo.Orders");
        }

        /* dmv_blocking_snapshots: the DMV-sourced arm beside the XE one, the merged population
           get_blocked_process_reports reads alongside blocked_process_reports. */
        for (var i = 0; i < DmvBlockingRows; i++)
        {
            var t = now.AddMinutes(-95).AddSeconds(-2 * i);
            var db = $"TenantDb{i % DbCount:00}";
            await ExecAsync(
                @"INSERT INTO dmv_blocking_snapshots
    (collection_id, collection_time, server_id, server_name, monitor_loop, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocking_status, contentious_object, blocked_sql_text, blocking_sql_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
                _nextId--, Naive(t), _serverId, ServerName, -1, Naive(t), db, 200 + i, 300 + i, 2500L, "S", "suspended",
                db + ".dbo.Inventory", "SELECT * FROM dbo.Inventory WHERE Sku = @Sku", "WAITFOR DELAY '00:00:05'");
        }

        /* collection_log: every SQL Server collector, mixed SUCCESS/ERROR so nothing compacts away -- matching
           Darling's #4268 note that get_collection_health only compacts collectors with nothing to report. */
        var collectors = CollectorCatalog.All.Where(d => d.TargetEngine == CollectorTargetEngine.SqlServer).Select(d => d.Name).ToArray();
        foreach (var collector in collectors)
        {
            for (var s = 0; s < CollectionLogSamplesPerCollector; s++)
            {
                var t = now.AddMinutes(-5 * s);
                await ExecAsync(
                    @"INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                    _nextId--, _serverId, ServerName, collector, Naive(t), 100.0 + s, s % 6 == 0 ? "ERROR" : "SUCCESS",
                    s % 6 == 0 ? "Login failed for user 'darling_monitor'." : null, s % 6 == 0 ? (int?)null : 50 + s, 80.0, 20.0);
            }
        }

        /* plan_correction: recommendations across every tenant database. */
        for (var i = 0; i < PlanCorrectionRows; i++)
        {
            var t = now.AddMinutes(-4 * i);
            var db = $"TenantDb{i % DbCount:00}";
            await ExecAsync(
                @"INSERT INTO plan_correction (collection_id, collection_time, server_id, server_name, database_name, recommendation_name, recommendation_state, score)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                _nextId--, Naive(t), _serverId, ServerName, db, "PR_" + (i % 6), i % 3 == 0 ? "Active" : "Superseded", 40 + i);
        }

        /* long_query_completions: a page of slow statements with real text width. */
        for (var i = 0; i < LongQueryRows; i++)
        {
            var t = now.AddMinutes(-6 * i);
            var db = $"TenantDb{i % DbCount:00}";
            await ExecAsync(
                @"INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, duration_microseconds, statement_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                _nextId--, Naive(t), _serverId, ServerName, Naive(t), "sql_batch_completed", db,
                (long)(i + 1) * 500_000, "EXEC dbo.usp_NightlyReconcile_" + i + " " + PadBlock(400));
        }
    }

    private Task SeedQueryStoreAsync(DateTime collectionTime, long executions, long avgDurationUs, long avgCpuUs, long intervalId, long queryId, string dbName, string queryText) =>
        ExecAsync(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads,
     runtime_stats_interval_id, query_text, last_execution_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
            _nextId--, Naive(collectionTime), _serverId, ServerName, dbName, queryId, 9L, "Regular",
            executions, avgDurationUs, avgCpuUs, 100L, intervalId, queryText, Naive(collectionTime));

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
