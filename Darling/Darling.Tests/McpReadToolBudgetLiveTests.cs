/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4198 part 2, the budget pin every MCP read tool answers to. Enumerates every <c>[McpServerTool]</c> method
/// on every <c>[McpServerToolType]</c> class in the Darling service assembly BY REFLECTION (<see cref="AllToolMethods"/>)
/// so a brand-new tool inherits this test without a line changing here, excludes what #4198's own measurement
/// excluded (write tools, <c>analyze_*</c>, <c>compare_*</c>, <c>audit_config</c> -- see <see cref="IsExcluded"/>
/// and <see cref="WriteToolRoster"/>), binds each remaining tool's parameters generically (<see cref="BindArgs"/>:
/// its DI services, <c>server_name</c> at the seeded server, everything else at its declared default) and
/// asserts the UTF-8 reply is under <see cref="McpResponseBudget.DefaultBytes"/> -- unless the tool is named in
/// <see cref="ExemptOffenders"/>, today's #4198 backlog. A fix lands by deleting its row: an exempt tool that
/// now fits fails this test until its row is gone, and a tool that is NOT exempt and goes over fails it too.
///
/// <para><b>Why a roster, not a heuristic, for write tools.</b> <c>analyze_*</c>/<c>compare_*</c>/<c>audit_config</c>
/// share a name pattern a rule can catch; the write surface (create/update/delete/mute/enable-toggle tools) does
/// not, and calling one of those during a "measure every read tool" pass would mutate the fixture instead of
/// just reading it. <see cref="WriteToolRoster"/> is therefore an exact, hand-maintained list: a new write tool
/// has to be added to it on purpose, or this test will actually invoke it.</para>
///
/// <para><b>The fixture</b> is one busy, multi-tenant server: 12 tenant databases each carrying regressed query
/// text (a baseline interval and a stepped-up recent interval, ~3 KB of parameterized text each -- the #4198
/// field offender), 30 multi-process deadlock graphs, 30 blocked-process reports with report XML, 20 collectors'
/// worth of collection-log history, plan-correction recommendations across every tenant database, a page of long
/// query completions, and 50 sibling servers (one collection each) so <c>get_fleet_overview</c> answers with the
/// #4198-measured 43-50-card shape instead of the one server this fixture otherwise seeds.</para>
///
/// <para><b>Known gaps.</b> A tool whose required parameter has no default and is not one of the three DI
/// services below gets a placeholder (0/false/null) instead -- it likely answers empty/not-found and passes
/// trivially, because this fixture does not seed its backing rows/state. <c>get_analysis_findings</c>'s
/// <c>analysis_findings</c> table, <c>get_index_usage</c>/<c>get_object_locking</c>'s object-level stats, and
/// every single-row drill-down (<c>get_custom_view</c>, <c>get_plan_xml</c>, <c>get_custom_alert_rule</c>, ...)
/// are in this bucket; each run's GAPS output (and the PR body) names exactly which. <c>describe_custom_view_catalog</c>
/// takes no parameters at all -- a static catalog description -- so it reproduces its #4198 size with no seeding.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class McpReadToolBudgetLiveTests
{
    private const string ServerName = "mcp-read-tool-budget";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const int DbCount = 12;
    private const int QueriesPerDb = 5;
    private const int DeadlockRows = 30;
    private const int BlockedRows = 30;
    private const int DmvBlockingRows = 10;
    private const int CollectionLogCollectors = 20;
    private const int CollectionLogSamplesPerCollector = 10;
    private const int PlanCorrectionRows = 30;
    private const int LongQueryRows = 20;
    private const int FleetServerCount = 50;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The write surface #4198 excluded from its measurement. See the class remarks for why this must be an
    /// exact roster rather than a name-pattern rule.
    /// </summary>
    private static readonly HashSet<string> WriteToolRoster = new(StringComparer.Ordinal)
    {
        "add_servers", "create_custom_alert_rule", "create_custom_view", "create_mute_rule",
        "delete_custom_alert_rule", "delete_custom_view", "delete_mute_rule", "delete_notification_route",
        "mute_analysis_finding", "remove_server", "set_mute_rule_enabled", "set_notification_route_enabled",
        "update_alert_settings", "update_custom_alert_rule", "update_custom_view", "update_mute_rule",
    };

    /// <summary>Empty since get_collection_health's per-field cut (the last #4198 row on this list, like Lite's
    /// twin): every other #4198 per-tool lane had already merged and fit here (get_blocking #4267,
    /// get_collection_log #4265, get_query_store_regressions #4264, describe_custom_view_catalog #4272,
    /// get_query_store_top #4273), and get_fleet_overview already fit. A new offender gets a row here only with
    /// an issue for its own fix.</summary>
    private static readonly Dictionary<string, int> ExemptOffenders = new(StringComparer.Ordinal);

    [Fact]
    public async Task EveryReadToolAnswersUnderTheDefaultBudget_UnlessListedAsA4198Offender()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live MCP read-tool budget pin.");

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
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-1);
            await SeedAsync(connection, ct, now);

            var analysisService = new DarlingAnalysisService(postgres);
            var catalog = new McpToolGuideCatalog();

            var measured = new List<(string Name, int Bytes)>();
            var gaps = new List<string>();

            foreach (var (name, method) in ReadToolMethods())
            {
                var args = BindArgs(method, postgres, analysisService, catalog, gaps, name);
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

            Assert.True(measured.Count > 100, $"only {measured.Count} read tools were measured -- reflection likely under-enumerated the service assembly");
            Assert.True(overBudget.Length == 0,
                "over the #4198 default budget and not in ExemptOffenders:" + Environment.NewLine + string.Join(Environment.NewLine, overBudget) +
                Environment.NewLine + "GAPS:" + Environment.NewLine + string.Join(Environment.NewLine, gaps));
            Assert.True(staleExemptions.Length == 0,
                "exempt tools that now fit the budget -- remove their ExemptOffenders row:" + Environment.NewLine + string.Join(Environment.NewLine, staleExemptions));
            Assert.True(missingExemptTools.Length == 0,
                "ExemptOffenders names a tool that was not measured (renamed, or newly excluded):" + Environment.NewLine + string.Join(Environment.NewLine, missingExemptTools));

            var allNames = AllToolMethods().Select(x => x.Name).ToHashSet(StringComparer.Ordinal);
            Assert.All(WriteToolRoster, w => Assert.Contains(w, allNames));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /* ═══════════════════════════ reflection: enumerate, exclude, bind, invoke ═══════════════════════════ */

    /// <summary>Every <c>[McpServerTool]</c> method on every <c>[McpServerToolType]</c> class in the Darling
    /// service assembly -- read AND write, so <see cref="WriteToolRoster"/> can be sanity-checked against it.</summary>
    private static (string Name, MethodInfo Method)[] AllToolMethods()
    {
        Type[] types;
        try
        {
            types = typeof(DarlingMcpDataTools).Assembly.GetTypes();
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
    private static object?[] BindArgs(MethodInfo method, NpgsqlDataSource postgres, DarlingAnalysisService analysisService,
        McpToolGuideCatalog catalog, List<string> gaps, string toolName)
    {
        var parameters = method.GetParameters();
        var args = new object?[parameters.Length];
        for (var i = 0; i < parameters.Length; i++)
        {
            var p = parameters[i];
            if (p.ParameterType == typeof(NpgsqlDataSource)) { args[i] = postgres; continue; }
            if (p.ParameterType == typeof(DarlingAnalysisService)) { args[i] = analysisService; continue; }
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

    /* ═══════════════════════════ the fixture: one busy, multi-tenant server ═══════════════════════════ */

    private static string PadBlock(int approxChars)
    {
        const string Chunk = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_";
        var sb = new StringBuilder(approxChars + Chunk.Length);
        while (sb.Length < approxChars) sb.Append(Chunk);
        return sb.ToString();
    }

    private static string FleetServerName(int i) => $"mcp-read-tool-budget-fleet-{i:000}";
    private static int FleetServerId(int i) => ServerIdHelper.GetDeterministicHashCode(FleetServerName(i));

    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct, DateTime now)
    {
        /* query_store_stats: DbCount tenant databases x QueriesPerDb queries, each with a baseline interval (30h
           back) and a regressed recent interval (30m back, duration/CPU stepped up) -- get_query_store_regressions'
           own shape -- carrying ~3 KB of parameterized query text, the #4198 field offender (a regression row
           with query text was measured at roughly 10 KB in production). */
        for (var db = 0; db < DbCount; db++)
        {
            var dbName = $"TenantDb{db:000}";
            for (var q = 0; q < QueriesPerDb; q++)
            {
                var queryId = (long)(db * 100 + q + 1);
                var text = $"EXEC dbo.usp_ProcessOrder_{q} @OrderId = {5000 + q}, @TenantId = {db}, @Payload = N'" + PadBlock(3000) + "'";
                /* get_query_store_regressions' "baseline" is everything collected BEFORE the default 24h recent
                   window, not just an earlier row -- so the baseline capture has to sit past that cutoff or the
                   comparison sees no baseline at all and answers empty. */
                await SeedQueryStoreAsync(connection, ct, now.AddHours(-30), executions: 500, avgDurationUs: 4_000, avgCpuUs: 3_500, intervalId: 1, queryId, dbName, text);
                await SeedQueryStoreAsync(connection, ct, now.AddMinutes(-30), executions: 480, avgDurationUs: 55_000, avgCpuUs: 48_000, intervalId: 2, queryId, dbName, text);
            }
        }

        /* deadlocks: multi-process graphs the width of a real one (#4198's #2 offender, get_deadlock_detail at 120 KB default). */
        for (var i = 0; i < DeadlockRows; i++)
        {
            var t = now.AddMinutes(-2 * i);
            var db = $"TenantDb{i % DbCount:000}";
            var xml = "<deadlock><victim-list><victimProcess id=\"process" + i + "a\"/></victim-list><process-list>" +
                "<process id=\"process" + i + "a\" waitresource=\"KEY: 7:72057594043170816 (fedcba9876543210)\" lockMode=\"X\" isolationlevel=\"read committed\">" +
                "<executionStack><frame procname=\"" + db + ".dbo.usp_UpdateOrder\" line=\"18\">UPDATE dbo.Orders SET Status = @NewStatus WHERE OrderId = @OrderId</frame></executionStack>" +
                "<inputbuf>" + PadBlock(1200) + "</inputbuf></process>" +
                "<process id=\"process" + i + "b\" waitresource=\"KEY: 7:72057594043170816 (0123456789abcdef)\" lockMode=\"S\" isolationlevel=\"read committed\">" +
                "<executionStack><frame procname=\"" + db + ".dbo.usp_ReadOrder\" line=\"9\">SELECT * FROM dbo.Orders WHERE OrderId = @OrderId</frame></executionStack>" +
                "<inputbuf>" + PadBlock(1200) + "</inputbuf></process></process-list></deadlock>";
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), ServerId, ServerName, DarlingMcpTestData.Naive(t), "process" + i + "a",
                "UPDATE dbo.Orders SET Status = @NewStatus WHERE OrderId = @OrderId", xml);
        }

        /* blocked_process_reports: report XML the width of a real one (get_blocking / get_blocked_process_xml / get_object_locking). */
        for (var i = 0; i < BlockedRows; i++)
        {
            var t = now.AddMinutes(-90).AddSeconds(-3 * i);
            var db = $"TenantDb{i % DbCount:000}";
            var xml = "<blocked-process-report><blocked-process><process spid=\"" + (50 + i) + "\" waitresource=\"KEY\">" +
                "<inputbuf>" + PadBlock(1000) + "</inputbuf></process></blocked-process>" +
                "<blocking-process><process spid=\"" + (150 + i) + "\"><inputbuf>" + PadBlock(1000) + "</inputbuf></process></blocking-process></blocked-process-report>";
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text, blocked_process_report_xml, contentious_object)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), ServerId, ServerName, DarlingMcpTestData.Naive(t), db, 50 + i, 150 + i, 4000L + i, "X",
                "UPDATE dbo.Orders SET Status = @NewStatus WHERE OrderId = @OrderId", "SELECT * FROM dbo.Orders WHERE OrderId = @OrderId", xml, db + ".dbo.Orders");
        }

        /* dmv_blocking_snapshots: the DMV-sourced arm beside the XE one, the merged population get_blocking reads. */
        for (var i = 0; i < DmvBlockingRows; i++)
        {
            var t = now.AddMinutes(-95).AddSeconds(-2 * i);
            var db = $"TenantDb{i % DbCount:000}";
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, monitor_loop, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocking_status, contentious_object, blocked_sql_text, blocking_sql_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), ServerId, ServerName, -1, DarlingMcpTestData.Naive(t), db, 200 + i, 300 + i, 2500L, "S", "suspended",
                db + ".dbo.Inventory", "SELECT * FROM dbo.Inventory WHERE Sku = @Sku", "WAITFOR DELAY '00:00:05'");
        }

        /* collection_log: many collectors over many samples -- #4198's #6 offender (get_collection_log, 88 KB
           default) is width from ROW COUNT, not row size. */
        for (var c = 0; c < CollectionLogCollectors; c++)
        {
            var collector = $"collector_{c:00}_wait_stats_and_queries";
            for (var s = 0; s < CollectionLogSamplesPerCollector; s++)
            {
                var t = now.AddMinutes(-5 * s);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1,$2,$3,$4,$5,$6)",
                    CollectionIdGenerator.Next(), ServerId, ServerName, collector, DarlingMcpTestData.Naive(t), s % 11 == 0 ? "FAILED" : "SUCCESS");
            }
        }

        /* plan_correction: recommendations across every tenant database (get_plan_corrections, #4198's #5 offender at 95 KB). */
        for (var i = 0; i < PlanCorrectionRows; i++)
        {
            var t = now.AddMinutes(-4 * i);
            var db = $"TenantDb{i % DbCount:000}";
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO plan_correction (collection_id, collection_time, server_id, server_name, database_name, recommendation_name, recommendation_state, score)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), ServerId, ServerName, db, "PR_" + (i % 6), i % 3 == 0 ? "Active" : "Superseded", 40 + i);
        }

        /* long_query_completions: a page of slow statements with real text width. */
        for (var i = 0; i < LongQueryRows; i++)
        {
            var t = now.AddMinutes(-6 * i);
            var db = $"TenantDb{i % DbCount:000}";
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, duration_microseconds, statement_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), ServerId, ServerName, DarlingMcpTestData.Naive(t), "sql_batch_completed", db,
                (long)(i + 1) * 500_000, "EXEC dbo.usp_NightlyReconcile_" + i + " " + PadBlock(400));
        }

        /* The fleet: FleetServerCount siblings, one SUCCESS collection each, so get_fleet_overview answers with
           #4198's measured 43-50-card shape instead of the one server this fixture otherwise seeds. */
        for (var i = 0; i < FleetServerCount; i++)
        {
            var name = FleetServerName(i);
            await DarlingMcpTestData.RegisterServerAsync(connection, FleetServerId(i), name, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1,$2,$3,'wait_stats',$4,'SUCCESS')",
                CollectionIdGenerator.Next(), FleetServerId(i), name, DarlingMcpTestData.Naive(now.AddMinutes(-5)));
        }
    }

    private static Task SeedQueryStoreAsync(NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime,
        long executions, long avgDurationUs, long avgCpuUs, long intervalId, long queryId, string dbName, string queryText) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads,
     runtime_stats_interval_id, query_text, last_execution_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTime), ServerId, ServerName, dbName, queryId, 9L, "Regular",
            executions, avgDurationUs, avgCpuUs, 100L, intervalId, queryText, DarlingMcpTestData.Naive(collectionTime));

    private static readonly string[] SeededTables =
    {
        "deadlocks", "blocked_process_reports", "dmv_blocking_snapshots", "collection_log",
        "query_store_stats", "plan_correction", "long_query_completions",
    };

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = Enumerable.Range(0, FleetServerCount).Select(FleetServerId).Append(ServerId).ToArray();
        foreach (var table in SeededTables)
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = ANY($1::int[])", ids);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = ANY($1::int[])", ids);
    }
}
