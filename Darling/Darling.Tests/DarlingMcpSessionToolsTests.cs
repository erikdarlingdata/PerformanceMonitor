/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the session diagnostic-depth MCP slice — get_session_stats, get_active_queries, get_waiting_tasks
/// over the Postgres store. Ungated: tool surface, per-tool param contract (matching Lite), the read SQL
/// pins (v_session_stats latest snapshot; query_snapshots windowed with the WAITFOR trim; waiting_tasks base
/// table — no v_ view), and the Gemini-clean advertised schema.
/// </summary>
public sealed class DarlingMcpSessionToolsSurfaceAndSqlTests
{
    private static readonly string[] SessionToolSurface =
    {
        "get_active_queries",
        "get_session_stats",
        "get_waiting_tasks",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpSessionTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheThreeSessionTools()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(SessionToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpSessionTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(toolMethods, m => Assert.True(m.IsStatic, $"{m.Name} must be static"));
        Assert.All(toolMethods, m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    private static (string Name, bool Optional)[] McpParams(string toolName)
    {
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == toolName);
        return method.GetParameters()
            .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(p => (p.Name!, p.HasDefaultValue))
            .ToArray();
    }

    [Theory]
    [InlineData("get_session_stats", "server_name")]
    [InlineData("get_active_queries", "server_name,hours_back,database_name,blocking_only,limit,full_text,as_of")]
    [InlineData("get_waiting_tasks", "server_name,hours_back,limit,as_of")]
    public void ParamContract_MatchesLite(string toolName, string expectedCsv)
    {
        Assert.Equal(expectedCsv.Split(','), McpParams(toolName).Select(p => p.Name).ToArray());
    }

    [Fact]
    public void ParamContract_ServerNameAlwaysOptional()
    {
        foreach (var tool in SessionToolSurface)
            Assert.True(McpParams(tool).Single(x => x.Name == "server_name").Optional, $"{tool}.server_name must be optional");
    }

    [Fact]
    public void SessionStatsSql_LatestSnapshot_PerApplication()
    {
        var sql = DarlingSessionReader.LatestSessionStatsSql;
        Assert.Contains("FROM v_session_stats", sql, StringComparison.Ordinal);
        Assert.Contains("program_name", sql, StringComparison.Ordinal);
        Assert.Contains("connection_count", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY connection_count DESC", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ActiveQueriesSql_ReadsBaseTable_WindowedWithWaitforTrim()
    {
        var sql = DarlingSessionReader.ActiveQueriesSql;
        Assert.Contains("FROM query_snapshots", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_query_snapshots", sql, StringComparison.Ordinal);
        Assert.Contains("granted_query_memory_gb", sql, StringComparison.Ordinal);
        Assert.Contains("blocking_session_id", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("NOT LIKE 'WAITFOR%'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void WaitingTasksSql_ReadsBaseTable_NoView_Windowed()
    {
        var sql = DarlingSessionReader.WaitingTasksSql;
        Assert.Contains("FROM waiting_tasks", sql, StringComparison.Ordinal);   /* no v_waiting_tasks view exists */
        Assert.DoesNotContain("v_waiting_tasks", sql, StringComparison.Ordinal);
        Assert.Contains("wait_duration_ms", sql, StringComparison.Ordinal);
        Assert.Contains("resource_description", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        /* #3541 A3: the cap is the caller's ($4), not the 500 the reader hid under a tool that advertised
           `limit` and then published a bare envelope. */
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT 500", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(DarlingSessionReader.LatestSessionStatsSql))]
    [InlineData(nameof(DarlingSessionReader.ActiveQueriesSql))]
    [InlineData(nameof(DarlingSessionReader.WaitingTasksSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = sqlName switch
        {
            nameof(DarlingSessionReader.LatestSessionStatsSql) => DarlingSessionReader.LatestSessionStatsSql,
            nameof(DarlingSessionReader.ActiveQueriesSql) => DarlingSessionReader.ActiveQueriesSql,
            _ => DarlingSessionReader.WaitingTasksSql,
        };
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("convert(", lower);
        Assert.DoesNotContain("top (", lower);
        Assert.DoesNotContain("isnull(", lower);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ReadColumns_ExistInTheGeneratedCollectorTables()
    {
        var ss = PgSchemaGenerator.CreateTable(SessionStatsCollector.Instance);
        Assert.Equal("session_stats", SessionStatsCollector.Instance.TargetTable);
        Assert.Contains("program_name", ss, StringComparison.Ordinal);
        Assert.Contains("connection_count", ss, StringComparison.Ordinal);
        Assert.Contains("total_logical_reads", ss, StringComparison.Ordinal);

        var qs = PgSchemaGenerator.CreateTable(QuerySnapshotsCollector.Instance);
        Assert.Equal("query_snapshots", QuerySnapshotsCollector.Instance.TargetTable);
        Assert.Contains("granted_query_memory_gb", qs, StringComparison.Ordinal);
        Assert.Contains("elapsed_time_formatted", qs, StringComparison.Ordinal);
        Assert.Contains("blocking_session_id", qs, StringComparison.Ordinal);

        var wt = PgSchemaGenerator.CreateTable(WaitingTasksCollector.Instance);
        Assert.Equal("waiting_tasks", WaitingTasksCollector.Instance.TargetTable);
        Assert.Contains("wait_duration_ms", wt, StringComparison.Ordinal);
        Assert.Contains("resource_description", wt, StringComparison.Ordinal);

        /* waiting_tasks deliberately has NO v_ passthrough view (the base reads confirm it). */
        Assert.DoesNotContain("v_waiting_tasks", string.Join(",", PgSchemaGenerator.AllPassthroughViews));
        Assert.Contains("v_session_stats", PgSchemaGenerator.AllPassthroughViews);
    }

    private static List<ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpSessionTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_ForAllThreeTools_NoRequiredParams()
    {
        var tools = BuildToolSchemas();
        Assert.Equal(3, tools.Count);
        var violations = tools.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
        foreach (var t in tools)
            Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(t.InputSchema));
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the session tools. Plants a session_stats snapshot, a couple
/// of query_snapshots (one blocking), and a waiting_tasks row, then asserts each tool returns its data-bearing
/// envelope, the blocking_only/database filters narrow without erroring, and an empty store returns the miss.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpSessionToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-session-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "StackOverflow";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task SessionTools_ReadPlantedRows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live session-tools test.");

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
            var t = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO session_stats (collection_id, collection_time, server_id, server_name, program_name, connection_count, running_count, sleeping_count, dormant_count, total_cpu_time_ms, total_logical_reads)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, "SQLCMD", 12L, 3, 9, 0, 4200L, 99999L);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status, blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads, granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count, request_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, 55, Db, "00 00:00:05.000", "SELECT * FROM Posts", "running", 60, "CXPACKET", 500L, 5000L, 5000L, 100L, 0L, 2000L, 0.5m, "Read Committed", 4, 3, "sa", "APP01", "SSMS", 1, 0);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO waiting_tasks (collection_id, collection_time, server_id, server_name, session_id, wait_type, wait_duration_ms, blocking_session_id, resource_description, database_name)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, 55, "LCK_M_X", 3000L, 60, null, Db);

            DarlingMcpTestData.AssertEnvelope(await DarlingMcpSessionTools.GetSessionStats(postgres, ServerName), ServerName, "applications");
            var aq = await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(aq, ServerName, "queries");
            Assert.Contains("SELECT * FROM Posts", aq, StringComparison.Ordinal);
            DarlingMcpTestData.AssertEnvelope(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 1, Db, true), ServerName, "queries");
            var tasks = await DarlingMcpSessionTools.GetWaitingTasks(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(tasks, ServerName, "tasks");

            /* #4966: where the page's rows stop prints as UTC with the Z, at the instant the rows carry. */
            foreach (var page in new[] { aq, tasks })
            {
                var oldest = System.Text.Json.JsonDocument.Parse(page).RootElement.GetProperty("oldest_returned_collection_time").GetString()!;
                Assert.Equal(DateTime.SpecifyKind(t, DateTimeKind.Utc).ToString("o", System.Globalization.CultureInfo.InvariantCulture), oldest);
            }

            await DeleteRowsAsync(connection, ct, keepServer: true);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(await DarlingMcpSessionTools.GetSessionStats(postgres, ServerName)));
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpSessionTools.GetWaitingTasks(postgres, ServerName)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }


    /* #4966 window-floor cases. The coverage probe measures the purge edge (the schedule's seven days) from the store's own
       clock, so a window anchored months back would read as lying wholly before the coverage: the anchor is the current minute,
       taken once per test, and every seeded instant is an offset from it. */
    private static DateTime AnchorNow()
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
    }

    private static readonly string[] WindowTools = ["get_active_queries", "get_waiting_tasks"];

    private static string TableOf(string tool) => tool == "get_active_queries" ? "query_snapshots" : "waiting_tasks";

    private static Task<string> CallAsync(NpgsqlDataSource postgres, string tool, string server, int hours, DateTime end) =>
        tool == "get_active_queries"
            ? DarlingMcpSessionTools.GetActiveQueries(postgres, server, hours, as_of: WebDataStartNote.FormatWindowEnd(end))
            : DarlingMcpSessionTools.GetWaitingTasks(postgres, server, hours, as_of: WebDataStartNote.FormatWindowEnd(end));

    private static string WindowServerName(string tool, string window) => "darling-mcp-session-window-" + window + "-" + TableOf(tool);

    /// <summary>A server registered at <paramref name="created"/>, its runs of both collectors logged every
    /// <paramref name="stepMinutes"/> minutes from <paramref name="runsFrom"/> to <paramref name="end"/> (none when null),
    /// and three rows of <paramref name="tool"/>'s table an hour apart from <paramref name="firstRow"/> (none when null).</summary>
    private static async Task SeedWindowServerAsync(
        NpgsqlConnection connection, string tool, string name, DateTime created, DateTime? runsFrom, int stepMinutes, DateTime? firstRow, DateTime end, System.Threading.CancellationToken ct)
    {
        var serverId = ServerIdHelper.GetDeterministicHashCode(name);
        await DeleteWindowServerAsync(connection, name, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, name, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET created_date = $2 WHERE server_id = $1", serverId, DarlingMcpTestData.Naive(created));

        if (runsFrom is DateTime from)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT row_number() OVER (), $1, $2, $3, t, 12, 'SUCCESS', 0
FROM generate_series($4::timestamp, $5::timestamp, make_interval(mins => $6)) AS t",
                serverId, name, TableOf(tool), DarlingMcpTestData.Naive(from), DarlingMcpTestData.Naive(end), stepMinutes);
        }

        if (firstRow is DateTime first)
        {
            for (var i = 0; i < 3; i++)
            {
                var at = DarlingMcpTestData.Naive(first.AddHours(i));
                if (tool == "get_active_queries")
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status, blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads, granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count, request_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)",
                        CollectionIdGenerator.Next(), at, serverId, name, 55, Db, "00 00:00:05.000", "SELECT 1", "running", 0, "CXPACKET", 500L, 5000L, 5000L, 100L, 0L, 2000L, 0.5m, "Read Committed", 4, 3, "sa", "APP01", "SSMS", 1, 0);
                }
                else
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        @"INSERT INTO waiting_tasks (collection_id, collection_time, server_id, server_name, session_id, wait_type, wait_duration_ms, blocking_session_id, resource_description, database_name)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                        CollectionIdGenerator.Next(), at, serverId, name, 55, "LCK_M_X", 3000L, 60, null, Db);
                }
            }
        }
    }

    private static async Task DeleteWindowServerAsync(NpgsqlConnection connection, string name, System.Threading.CancellationToken ct)
    {
        var serverId = ServerIdHelper.GetDeterministicHashCode(name);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "DELETE FROM query_snapshots WHERE server_id = $1; DELETE FROM waiting_tasks WHERE server_id = $1; DELETE FROM collection_log WHERE server_id = $1; DELETE FROM servers WHERE server_id = $1;",
            serverId);
    }

    /// <summary>Runs <paramref name="body"/> for each window tool against its own seeded server, then removes what it seeded.</summary>
    private async Task ForEachWindowToolAsync(
        string window, Func<NpgsqlConnection, NpgsqlDataSource, string, string, DateTime, Task> seedAndAssert)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live session-tools test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            foreach (var tool in WindowTools)
            {
                await seedAndAssert(connection, postgres, tool, WindowServerName(tool, window), AnchorNow());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                foreach (var tool in WindowTools)
                {
                    await DeleteWindowServerAsync(cleanup, WindowServerName(tool, window), cleanupCt);
                }
            });
        }
    }

    private static System.Text.Json.JsonElement Parse(string json) => System.Text.Json.JsonDocument.Parse(json).RootElement.Clone();

    /// <summary>(a) The server was registered two days ago and read over 168 hours: the data starts at its first collection, and the keys sit right after hours_back.</summary>
    [Fact]
    public async Task ARowsAnswer_ForAServerAddedTwoDaysAgo_NamesWhereCoverageStarts_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachWindowToolAsync("added", async (connection, postgres, tool, name, end) =>
        {
            var added = end.AddDays(-2);
            await SeedWindowServerAsync(connection, tool, name, added, added, 30, end.AddDays(-1), end, ct);

            var json = await CallAsync(postgres, tool, name, 168, end);

            var root = Parse(json);
            Assert.True(root.GetProperty("window_truncated").GetBoolean(), tool);
            Assert.Equal(McpHelpers.FormatEffectiveStart(added), root.GetProperty("effective_start").GetString());
            Assert.EndsWith("Z", root.GetProperty("effective_start").GetString()!, StringComparison.Ordinal);
            Assert.Equal(
                DarlingMcpWindowNotice.Build(added, end.AddHours(-168), TableOf(tool)).TruncationNote,
                root.GetProperty("truncation_note").GetString());
            Assert.Contains("raw " + TableOf(tool) + " retains", root.GetProperty("truncation_note").GetString(), StringComparison.Ordinal);
            Assert.False(root.TryGetProperty("effective_hours_back", out _), tool);

            /* Written right after hours_back, in the contract's order. */
            var names = root.EnumerateObject().Select(p => p.Name).ToList();
            var at = names.IndexOf("hours_back");
            Assert.Equal(["effective_start", "window_truncated", "truncation_note"], names.Skip(at + 1).Take(3));
        });
    }

    /// <summary>(b) A quiet start: registered a month ago, its first row two days into the window. The store covered the whole window, so false and the asked start.</summary>
    [Fact]
    public async Task ARowsAnswer_WhoseFirstRowComesLate_IsCovered_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachWindowToolAsync("quiet", async (connection, postgres, tool, name, end) =>
        {
            await SeedWindowServerAsync(connection, tool, name, end.AddDays(-30), end.AddDays(-8), 60, end.AddDays(-5), end, ct);

            var root = Parse(await CallAsync(postgres, tool, name, 168, end));

            Assert.False(root.GetProperty("window_truncated").GetBoolean(), tool);
            Assert.Equal(System.Text.Json.JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
            Assert.Equal(McpHelpers.FormatEffectiveStart(end.AddHours(-168)), root.GetProperty("effective_start").GetString());
        });
    }

    /// <summary>(c) An empty one-hour window with no run in it: the store holds no collection, so the hints say NOT covered, with no start to name.</summary>
    [Fact]
    public async Task AnEmptyHour_WithNoRun_SaysTheStoreHoldsNoCollection_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachWindowToolAsync("norun", async (connection, postgres, tool, name, end) =>
        {
            await SeedWindowServerAsync(connection, tool, name, end.AddDays(-30), null, 30, null, end, ct);

            var root = Parse(await CallAsync(postgres, tool, name, 1, end));

            Assert.Equal("empty", root.GetProperty("status").GetString());
            var hints = root.GetProperty("hints");
            Assert.True(hints.GetProperty("window_truncated").GetBoolean(), tool);
            Assert.Equal(System.Text.Json.JsonValueKind.Null, hints.GetProperty("effective_start").ValueKind);
            Assert.Equal(
                DarlingMcpWindowNotice.Build(null, end.AddHours(-1), TableOf(tool), emptyAnswer: true).TruncationNote,
                hints.GetProperty("truncation_note").GetString());
            Assert.Contains("no collection of " + TableOf(tool), hints.GetProperty("truncation_note").GetString(), StringComparison.Ordinal);
        });
    }

    /// <summary>(d) An empty one-hour window with a run 35 minutes in: the store ran for it, so covered (false, null) at the asked start.</summary>
    [Fact]
    public async Task AnEmptyHour_WithARunInIt_IsCovered_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachWindowToolAsync("runin", async (connection, postgres, tool, name, end) =>
        {
            var start = end.AddHours(-1);
            /* One run, 35 minutes after the window's start: the generate_series step is longer than the window. */
            await SeedWindowServerAsync(connection, tool, name, end.AddDays(-30), start.AddMinutes(35), 600, null, start.AddMinutes(35), ct);

            var root = Parse(await CallAsync(postgres, tool, name, 1, end));

            Assert.Equal("empty", root.GetProperty("status").GetString());
            var hints = root.GetProperty("hints");
            Assert.False(hints.GetProperty("window_truncated").GetBoolean(), tool);
            Assert.Equal(System.Text.Json.JsonValueKind.Null, hints.GetProperty("truncation_note").ValueKind);
            Assert.Equal(McpHelpers.FormatEffectiveStart(start), hints.GetProperty("effective_start").GetString());
        });
    }

    /// <summary>The empty answers carry the hints and the not_collected answer stays bare: the hints ride only on the empty status.</summary>
    [Fact]
    public void TheEmptyAnswers_CarryTheHints_AndNotCollectedStaysBare_InTheToolSource()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpSessionTools.cs").ReplaceLineEndings("\n");
        Assert.Contains("?? McpHelpers.Status(\"empty\", \"No active query snapshots found in the requested time range.\", notice.AsHints());", source, StringComparison.Ordinal);
        Assert.Contains("?? McpHelpers.Status(\"empty\", \"No waiting tasks captured in the specified time range.\", notice.AsHints());", source, StringComparison.Ordinal);
        Assert.DoesNotContain("NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, \"query_snapshots\", cancellationToken, notice", source, StringComparison.Ordinal);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, bool keepServer = false)
    {
        var sql = string.Join(" ", new[] { "session_stats", "query_snapshots", "waiting_tasks" }
            .Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"));
        if (!keepServer) sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
