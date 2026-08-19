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
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the blocking / deadlock diagnostic-depth MCP slice — get_blocking, get_deadlocks,
/// get_deadlock_detail, get_blocked_process_xml, and the per-minute get_blocking_trend / get_deadlock_trend
/// over the Postgres store. Ungated: the tool surface is
/// EXACTLY the six names (all static, on a [McpServerToolType] class, returning Task&lt;string&gt;); each
/// param contract matches Lite's; every read SQL is Postgres-dialect, positional-param, reads the collector
/// columns the schema generator emits, and windows on the naive-UTC collection_time; and the advertised
/// tools/list schema is Gemini-clean.
/// </summary>
public sealed class DarlingMcpBlockingToolsSurfaceAndSqlTests
{
    private static readonly string[] BlockingToolSurface =
    {
        "get_blocked_process_xml",
        "get_blocking",
        "get_blocking_trend",
        "get_deadlock_detail",
        "get_deadlock_trend",
        "get_deadlocks",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpBlockingTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheSixBlockingTools()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(BlockingToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpBlockingTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(toolMethods, m => Assert.True(m.IsStatic, $"{m.Name} must be static for WithGeminiCompatibleTools"));
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

    /// <summary>
    /// Lite's parameter contract, pinned as a PREFIX rather than as the whole list (#2159).
    ///
    /// <para>This used to assert the full parameter list, which was the same thing until Darling's incident
    /// readers gained a trailing optional <c>dedup_key</c> that Lite does not have. The guarantee that actually
    /// matters was never "the lists are identical" — it is that a client written against LITE's contract still
    /// calls Darling correctly. A trailing optional parameter preserves exactly that, positionally and by name,
    /// so the prefix is the honest form of the assertion and the full-list form was over-tight.</para>
    ///
    /// <para>Anything appended must therefore stay optional AND stay at the end. <c>StartsWith</c> semantics are
    /// spelled out by comparing the first N rather than by trusting a substring of a joined string, so a
    /// REORDERING that keeps the same names still fails.</para>
    /// </summary>
    [Theory]
    [InlineData("get_blocking", "server_name,hours_back,limit")]
    [InlineData("get_deadlocks", "server_name,hours_back,limit")]
    [InlineData("get_deadlock_detail", "server_name,hours_back,limit")]
    [InlineData("get_blocked_process_xml", "server_name,hours_back,limit")]
    [InlineData("get_blocking_trend", "server_name,hours_back")]
    [InlineData("get_deadlock_trend", "server_name,hours_back")]
    public void ParamContract_MatchesLite(string toolName, string expectedCsv)
    {
        var expected = expectedCsv.Split(',');
        var actual = McpParams(toolName).Select(p => p.Name).ToArray();

        Assert.True(actual.Length >= expected.Length,
            $"{toolName} dropped a parameter Lite has: [{string.Join(",", actual)}]");
        Assert.Equal(expected, actual.Take(expected.Length).ToArray());
    }

    /// <summary>
    /// #2159's <c>dedup_key</c>, pinned as a TRAILING OPTIONAL parameter on exactly the three incident readers
    /// that can resolve a fingerprint — and pinned as absent everywhere else.
    ///
    /// <para>Optional and trailing is what keeps <see cref="ParamContract_MatchesLite"/> true, so it is asserted
    /// here rather than left to review. Absent on the trend tools because a per-minute count series has no single
    /// incident to resolve to, and absent on <c>get_blocked_process_xml</c> because it is reached FROM an incident
    /// the operator has already identified rather than used to find one.</para>
    /// </summary>
    [Fact]
    public void ParamContract_DedupKeyIsATrailingOptionalOnTheIncidentReaders()
    {
        foreach (var tool in new[] { "get_blocking", "get_deadlocks", "get_deadlock_detail" })
        {
            var ps = McpParams(tool);
            Assert.Equal("dedup_key", ps[^1].Name);
            Assert.True(ps[^1].Optional, $"{tool}.dedup_key must be optional so Lite-shaped calls still work");
        }

        foreach (var tool in new[] { "get_blocking_trend", "get_deadlock_trend", "get_blocked_process_xml" })
            Assert.DoesNotContain("dedup_key", McpParams(tool).Select(p => p.Name));
    }

    /// <summary>
    /// The instructions have to ADVERTISE <c>dedup_key</c>, or the feature is unreachable in practice: an agent
    /// picks tools and arguments from this text, and a parameter it never reads is one it never passes. Same
    /// reason the store-metrics and AG tools pin their own mentions.
    ///
    /// <para>Also pins the two caveats that turn an empty result into a diagnosable one — the display-name
    /// scoping and that <c>hours_back</c> still bounds the search — because those are the failure modes an agent
    /// would otherwise report as "no such incident".</para>
    /// </summary>
    [Fact]
    public void Instructions_AdvertiseDedupKeyAndItsScoping()
    {
        var text = DarlingMcpInstructions.Text;

        Assert.Contains("dedup_key", text, StringComparison.Ordinal);
        Assert.Contains("Dedup Key", text, StringComparison.Ordinal);
        Assert.Contains("DISPLAY name", text, StringComparison.Ordinal);
        Assert.Contains("hours_back` still bounds the search", text, StringComparison.Ordinal);
    }

    [Fact]
    public void ParamContract_ServerNameAlwaysOptional()
    {
        foreach (var tool in BlockingToolSurface)
            Assert.True(McpParams(tool).Single(x => x.Name == "server_name").Optional, $"{tool}.server_name must be optional");
    }

    [Fact]
    public void BlockedProcessReportsSql_ReadsBaseTable_XmlAndPairColumns_WindowsOnCollectionTime()
    {
        var sql = DarlingBlockingReader.BlockedProcessReportsSql;
        Assert.Contains("FROM blocked_process_reports", sql, StringComparison.Ordinal);  /* base table for the V7 plan-column safety */
        Assert.DoesNotContain("v_blocked_process_reports", sql, StringComparison.Ordinal);
        Assert.Contains("blocked_process_report_xml", sql, StringComparison.Ordinal);
        Assert.Contains("contentious_object", sql, StringComparison.Ordinal);
        Assert.Contains("blocked_spid", sql, StringComparison.Ordinal);
        Assert.Contains("blocking_spid", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY event_time DESC", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DmvBlockingSnapshotsSql_ReadsView_NoXmlColumn()
    {
        var sql = DarlingBlockingReader.DmvBlockingSnapshotsSql;
        Assert.Contains("FROM v_dmv_blocking_snapshots", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("blocked_process_report_xml", sql, StringComparison.Ordinal);  /* the DMV snapshot has no report XML */
        Assert.Contains("contentious_object", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void RecentDeadlocksSql_ReadsBaseTable_GraphXml_OrdersByDeadlockTime()
    {
        var sql = DarlingBlockingReader.RecentDeadlocksSql;
        Assert.Contains("FROM deadlocks", sql, StringComparison.Ordinal);                  /* base table for the V7 victim-plan column */
        Assert.DoesNotContain("v_deadlocks", sql, StringComparison.Ordinal);
        Assert.Contains("deadlock_graph_xml", sql, StringComparison.Ordinal);
        Assert.Contains("victim_process_id", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY deadlock_time DESC", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BlockingTrendSql_And_DeadlockTrendSql_PerMinuteBuckets_PostgresDialect()
    {
        var blocking = DarlingBlockingTrendReader.BlockingTrendSql;
        Assert.Contains("v_blocked_process_reports", blocking, StringComparison.Ordinal);
        Assert.Contains("v_dmv_blocking_snapshots", blocking, StringComparison.Ordinal);   /* XE-preferred, DMV fallback */
        Assert.Contains("WHERE NOT EXISTS", blocking, StringComparison.Ordinal);
        Assert.Contains("DATE_TRUNC('minute', event_time)", blocking, StringComparison.Ordinal);

        var deadlock = DarlingBlockingTrendReader.DeadlockTrendSql;
        Assert.Contains("FROM v_deadlocks", deadlock, StringComparison.Ordinal);
        Assert.Contains("DATE_TRUNC('minute', deadlock_time)", deadlock, StringComparison.Ordinal);

        foreach (var sql in new[] { blocking, deadlock })
        {
            var lower = sql.ToLowerInvariant();
            Assert.DoesNotContain("getdate", lower);
            Assert.DoesNotContain("top (", lower);
            Assert.DoesNotContain("isnull(", lower);
            Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData(nameof(DarlingBlockingReader.BlockedProcessReportsSql))]
    [InlineData(nameof(DarlingBlockingReader.DmvBlockingSnapshotsSql))]
    [InlineData(nameof(DarlingBlockingReader.RecentDeadlocksSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = sqlName switch
        {
            nameof(DarlingBlockingReader.BlockedProcessReportsSql) => DarlingBlockingReader.BlockedProcessReportsSql,
            nameof(DarlingBlockingReader.DmvBlockingSnapshotsSql) => DarlingBlockingReader.DmvBlockingSnapshotsSql,
            _ => DarlingBlockingReader.RecentDeadlocksSql,
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
        var bpr = PgSchemaGenerator.CreateTable(BlockedProcessReportCollector.Instance);
        Assert.Equal("blocked_process_reports", BlockedProcessReportCollector.Instance.TargetTable);
        Assert.Contains("blocked_process_report_xml", bpr, StringComparison.Ordinal);
        Assert.Contains("contentious_object", bpr, StringComparison.Ordinal);
        Assert.Contains("blocked_isolation_level", bpr, StringComparison.Ordinal);
        Assert.Contains("blocking_priority", bpr, StringComparison.Ordinal);

        var dmv = PgSchemaGenerator.CreateTable(DmvBlockingSnapshotCollector.Instance);
        Assert.Equal("dmv_blocking_snapshots", DmvBlockingSnapshotCollector.Instance.TargetTable);
        Assert.Contains("blocking_status", dmv, StringComparison.Ordinal);
        Assert.Contains("blocking_last_tran_started", dmv, StringComparison.Ordinal);

        var dl = PgSchemaGenerator.CreateTable(DeadlocksCollector.Instance);
        Assert.Equal("deadlocks", DeadlocksCollector.Instance.TargetTable);
        Assert.Contains("deadlock_graph_xml", dl, StringComparison.Ordinal);
        Assert.Contains("victim_sql_text", dl, StringComparison.Ordinal);
    }

    private static List<ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpBlockingTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_ForAllSixTools()
    {
        var tools = BuildToolSchemas();
        Assert.Equal(6, tools.Count);
        var violations = tools.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
    }

    [Fact]
    public void AdvertisedSchema_NoRequiredParams()
    {
        var tools = BuildToolSchemas().ToDictionary(t => t.Name, t => t.InputSchema);
        foreach (var tool in BlockingToolSurface)
            Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(tools[tool]));
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the blocking tools. Registers a sentinel server, plants an
/// XE blocked-process-report (with report XML) + a DMV snapshot + a deadlock (with graph XML), calls the tool
/// methods and asserts each returns its data-bearing envelope; an empty store returns the "empty" miss.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpBlockingToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-blocking-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "StackOverflow";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task BlockingTools_ReadPlantedRows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live blocking-tools test.");

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
                @"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text, blocked_process_report_xml, contentious_object)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, t, Db, 55, 60, 8000L, "X", "SELECT 1", "UPDATE Posts SET Score = Score + 1", "<blocked-process-report><blocked-process><process spid=\"55\"/></blocked-process></blocked-process-report>", "dbo.Posts");

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, monitor_loop, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocking_status, contentious_object, blocked_sql_text, blocking_sql_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, -1, t, Db, 70, 80, 3000L, "S", "suspended", "dbo.Users", "SELECT 2", "WAITFOR DELAY '00:01'");

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, t, "process123", "DELETE FROM Posts", "<deadlock><victim-list><victimProcess id=\"process123\"/></victim-list><process-list><process id=\"process123\"><inputbuf>DELETE FROM Posts</inputbuf></process></process-list></deadlock>");

            var blocking = await DarlingMcpBlockingTools.GetBlocking(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(blocking, ServerName, "events");
            Assert.Contains("dbo.Posts", blocking, StringComparison.Ordinal);

            DarlingMcpTestData.AssertEnvelope(await DarlingMcpBlockingTools.GetDeadlocks(postgres, ServerName), ServerName, "deadlocks");
            var detail = await DarlingMcpBlockingTools.GetDeadlockDetail(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(detail, ServerName, "deadlock_graph_xml");
            var xml = await DarlingMcpBlockingTools.GetBlockedProcessXml(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(xml, ServerName, "blocked_process_report_xml");

            /* The per-minute trend series (the planted BPR + deadlock fall inside the default 24h window). */
            DarlingMcpTestData.AssertEnvelope(await DarlingMcpBlockingTools.GetBlockingTrend(postgres, ServerName), ServerName, "trend");
            DarlingMcpTestData.AssertEnvelope(await DarlingMcpBlockingTools.GetDeadlockTrend(postgres, ServerName), ServerName, "trend");

            /* Unknown server resolves to the listing error. */
            Assert.StartsWith("Could not resolve server.", await DarlingMcpBlockingTools.GetDeadlocks(postgres, "darling-no-such-server"), StringComparison.Ordinal);

            /* Empty store → the "empty" miss. */
            await DeleteRowsAsync(connection, ct, keepServer: true);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpBlockingTools.GetDeadlocks(postgres, ServerName)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, bool keepServer = false)
    {
        var sql = string.Join(" ", new[] { "blocked_process_reports", "dmv_blocking_snapshots", "deadlocks" }
            .Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"));
        if (!keepServer) sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
