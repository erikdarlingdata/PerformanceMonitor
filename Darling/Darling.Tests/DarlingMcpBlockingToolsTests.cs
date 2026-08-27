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
/// get_deadlock_detail, get_blocked_process_xml, the per-minute get_blocking_trend / get_deadlock_trend and
/// the aggregate get_lock_wait_trend (#2484) over the Postgres store. Ungated: the tool surface is
/// EXACTLY the seven names (all static, on a [McpServerToolType] class, returning Task&lt;string&gt;); each
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
        "get_lock_wait_trend",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpBlockingTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheSevenBlockingTools()
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
    /// Lite's parameter contract, pinned as an ORDERED SUBSEQUENCE of Darling's (#2159, widened by #2495).
    ///
    /// <para>This used to assert the full parameter list, which was the same thing until Darling's incident
    /// readers gained a trailing optional <c>dedup_key</c> that Lite does not have; it then asserted a PREFIX,
    /// which held only while every parameter Lite gained after that landed before <c>dedup_key</c>. #2495
    /// appended <c>as_of</c> to BOTH SKUs — last on each, the same convention <c>dedup_key</c> followed — so
    /// Lite's list is no longer a prefix of Darling's: Darling reads <c>…, limit, dedup_key, as_of</c> while
    /// Lite reads <c>…, limit, as_of</c>.</para>
    ///
    /// <para>The guarantee that actually matters was never "the lists are identical", and it is not positional
    /// either: MCP invokes by NAME, and the only C# call sites (the <c>/api/read</c> dispatch) do not pass
    /// Darling's extra optionals at all. It is that <b>a client written against Lite's contract still calls
    /// Darling correctly</b> — every Lite name present in Lite's relative order, and every Darling-only extra
    /// OPTIONAL so the client never has to supply one. A dropped parameter, a REORDERING, or a required
    /// Darling-only addition each still fail.</para>
    /// </summary>
    [Theory]
    [InlineData("get_blocking", "server_name,hours_back,limit,as_of")]
    [InlineData("get_deadlocks", "server_name,hours_back,limit,as_of")]
    [InlineData("get_deadlock_detail", "server_name,hours_back,limit,as_of")]
    [InlineData("get_blocked_process_xml", "server_name,hours_back,limit,as_of")]
    [InlineData("get_blocking_trend", "server_name,hours_back,as_of")]
    [InlineData("get_deadlock_trend", "server_name,hours_back,as_of")]
    [InlineData("get_lock_wait_trend", "server_name,hours_back,as_of")]
    public void ParamContract_MatchesLite(string toolName, string expectedCsv)
    {
        var expected = expectedCsv.Split(',');
        var actual = McpParams(toolName);
        var actualNames = actual.Select(p => p.Name).ToArray();

        Assert.True(actual.Length >= expected.Length,
            $"{toolName} dropped a parameter Lite has: [{string.Join(",", actualNames)}]");

        /* Every Lite name, in Lite's order, with Darling's own additions filtered out. */
        Assert.Equal(expected, actualNames.Where(n => expected.Contains(n, StringComparer.Ordinal)).ToArray());

        /* And nothing Darling adds on top may be required, or a Lite-shaped call could not be made at all. */
        foreach (var extra in actual.Where(p => !expected.Contains(p.Name, StringComparer.Ordinal)))
            Assert.True(extra.Optional, $"{toolName}.{extra.Name} is Darling-only and must be optional");
    }

    /// <summary>
    /// #2159's <c>dedup_key</c>, pinned as an APPENDED OPTIONAL parameter on exactly the three incident readers
    /// that can resolve a fingerprint — and pinned as absent everywhere else.
    ///
    /// <para>It was pinned as the LAST parameter until #2495 appended <c>as_of</c> to both SKUs behind it.
    /// Moving <c>dedup_key</c> to keep it last would have relocated an already-shipped parameter to preserve a
    /// property (position) that no caller of an MCP tool can observe, so what is pinned now is what the
    /// property was always standing in for, and what keeps <see cref="ParamContract_MatchesLite"/> true:
    /// <c>dedup_key</c> is optional, and it sits AFTER every parameter Lite has, so a Lite-shaped call never
    /// meets it. Absent on the trend tools because a per-minute count series has no single
    /// incident to resolve to, and absent on <c>get_blocked_process_xml</c> because it is reached FROM an incident
    /// the operator has already identified rather than used to find one.</para>
    /// </summary>
    [Fact]
    public void ParamContract_DedupKeyIsAnAppendedOptionalOnTheIncidentReaders()
    {
        foreach (var tool in new[] { "get_blocking", "get_deadlocks", "get_deadlock_detail" })
        {
            var ps = McpParams(tool);
            var names = ps.Select(p => p.Name).ToArray();

            Assert.Contains("dedup_key", names);
            Assert.True(ps.Single(p => p.Name == "dedup_key").Optional,
                $"{tool}.dedup_key must be optional so Lite-shaped calls still work");
            Assert.True(
                Array.IndexOf(names, "dedup_key") > Array.IndexOf(names, "limit"),
                $"{tool}.dedup_key must sit after every parameter Lite has, so a Lite-shaped call never meets it");
        }

        foreach (var tool in new[] { "get_blocking_trend", "get_deadlock_trend", "get_lock_wait_trend", "get_blocked_process_xml" })
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

    /// <summary>
    /// The lock-wait lane (#2484), pinned as the VIEWER'S query rather than as a query that happens to work.
    ///
    /// <para>Three properties, each of which is a real defect if it drifts. The LCK filter, or the read stops
    /// being about locks. The LAG partitioned BY WAIT TYPE, without which one wait type's cadence is used to
    /// divide another's delta. And the CAST to double precision before the division — integer division would
    /// report a 3 ms delta over a 60-second interval as ZERO, which is #2507's defect (a quiet server reading
    /// as an idle one) one read over.</para>
    /// </summary>
    [Fact]
    public void LockWaitTrendSql_FiltersLockWaits_LagsPerWaitType_AndDividesAsDouble()
    {
        var sql = DarlingBlockingTrendReader.LockWaitTrendSql;

        Assert.Contains("FROM v_wait_stats", sql, StringComparison.Ordinal);
        Assert.Contains("wait_type LIKE 'LCK%'", sql, StringComparison.Ordinal);
        Assert.Contains("LAG(collection_time) OVER (PARTITION BY wait_type ORDER BY collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(delta_wait_time_ms AS double precision) / interval_seconds", sql, StringComparison.Ordinal);

        /* A negative delta is the counter reset across a restart, not a negative wait. */
        Assert.Contains("WHERE delta_wait_time_ms >= 0", sql, StringComparison.Ordinal);

        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("top (", lower);
        Assert.DoesNotContain("isnull(", lower);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The anchor pinned by IDENTITY, not merely by name: <c>get_lock_wait_trend</c>'s <c>as_of</c> must carry
    /// the SHARED description constant. That constant exists because the same parameter described two
    /// different ways on two SKUs is a divergence no other test would see.
    /// </summary>
    [Fact]
    public void LockWaitTrend_AnchorCarriesTheSharedDescription()
    {
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == "get_lock_wait_trend");
        var asOf = method.GetParameters().Single(p => p.Name == "as_of");

        Assert.Equal(McpHelpers.AsOfDescription, asOf.GetCustomAttribute<DescriptionAttribute>()!.Description);
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
    public void AdvertisedSchema_IsGeminiClean_ForAllSevenTools()
    {
        var tools = BuildToolSchemas();
        Assert.Equal(7, tools.Count);
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
