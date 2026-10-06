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
using PerformanceMonitor.Darling.Service;
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
    [InlineData("get_lock_wait_trend", "server_name,hours_back,as_of,bucket_minutes")]
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
    /// The parameter's OWN description has to ADVERTISE <c>dedup_key</c>'s scoping, or the feature is
    /// unreachable in practice: an agent picks tools and arguments from a tool's own description, and a
    /// caveat it never reads is one it never accounts for. #3898 Phase 2 (D5) moved this pin off the
    /// instructions (which used to carry a second, shorter mention) onto the parameter description that was
    /// always the primary surface — the display-name scoping is the failure mode an agent would otherwise
    /// report as "no such incident".
    /// </summary>
    [Theory]
    [InlineData("get_blocking")]
    [InlineData("get_deadlocks")]
    [InlineData("get_deadlock_detail")]
    public void ParamContract_DedupKeyDescription_AdvertisesItsScoping(string tool)
    {
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == tool);
        var description = method.GetParameters().Single(p => p.Name == "dedup_key")
            .GetCustomAttribute<DescriptionAttribute>()!.Description;

        Assert.Contains("Dedup Key", description, StringComparison.Ordinal);
        Assert.Contains("display name", description, StringComparison.Ordinal);
    }

    [Fact]
    public void ParamContract_ServerNameAlwaysOptional()
    {
        foreach (var tool in BlockingToolSurface)
            Assert.True(McpParams(tool).Single(x => x.Name == "server_name").Optional, $"{tool}.server_name must be optional");
    }

    [Fact]
    public void BlockedProcessReportsSql_ReadsBaseTable_XmlAndPairColumns_WindowsOnEventTime()
    {
        var sql = DarlingBlockingReader.BlockedProcessReportsSql;
        Assert.Contains("FROM blocked_process_reports", sql, StringComparison.Ordinal);  /* base table for the V7 plan-column safety */
        Assert.DoesNotContain("v_blocked_process_reports", sql, StringComparison.Ordinal);
        Assert.Contains("blocked_process_report_xml", sql, StringComparison.Ordinal);
        Assert.Contains("contentious_object", sql, StringComparison.Ordinal);
        Assert.Contains("blocked_spid", sql, StringComparison.Ordinal);
        Assert.Contains("blocking_spid", sql, StringComparison.Ordinal);
        Assert.Contains("event_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("event_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $5", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY event_time DESC", sql, StringComparison.Ordinal);
        /* #3541 A3: the cap is the CALLER'S ($4), not the 200 the reader used to hide under a tool that
           advertised `limit`. */
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT 200", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3541 A3: <c>get_blocked_process_xml</c> pages over rows that CARRY a report, and the predicate is in
    /// the SQL — filtering for XML in C# after a capped fetch was the defect (a run of graph-less rows at the
    /// newest end read as "no XML in the window"). Pinned as the SAME projection as the unfiltered read plus
    /// exactly the predicate, so the two consts cannot drift a column apart.
    /// </summary>
    [Fact]
    public void BlockedProcessReportsWithXmlSql_IsTheUnfilteredRead_PlusTheXmlPredicate_InSql()
    {
        var plain = DarlingBlockingReader.BlockedProcessReportsSql;
        var withXml = DarlingBlockingReader.BlockedProcessReportsWithXmlSql;

        Assert.Contains("AND   blocked_process_report_xml IS NOT NULL", withXml, StringComparison.Ordinal);
        Assert.Contains("AND   blocked_process_report_xml <> ''", withXml, StringComparison.Ordinal);
        Assert.DoesNotContain("blocked_process_report_xml IS NOT NULL", plain, StringComparison.Ordinal);

        /* Everything up to the window predicate is byte-identical. */
        const string Cut = "AND   collection_time <= $3";
        Assert.Equal(plain[..(plain.IndexOf(Cut, StringComparison.Ordinal) + Cut.Length)],
                     withXml[..(withXml.IndexOf(Cut, StringComparison.Ordinal) + Cut.Length)]);
        Assert.EndsWith("ORDER BY event_time DESC\nLIMIT $4", withXml.Replace("\r\n", "\n"), StringComparison.Ordinal);
    }

    [Fact]
    public void DmvBlockingSnapshotsSql_ReadsView_NoXmlColumn()
    {
        var sql = DarlingBlockingReader.DmvBlockingSnapshotsSql;
        Assert.Contains("FROM v_dmv_blocking_snapshots", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("blocked_process_report_xml", sql, StringComparison.Ordinal);  /* the DMV snapshot has no report XML */
        Assert.Contains("contentious_object", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT 200", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void RecentDeadlocksSql_ReadsBaseTable_GraphXml_OrdersByDeadlockTime()
    {
        var sql = DarlingBlockingReader.RecentDeadlocksSql;
        Assert.Contains("FROM deadlocks", sql, StringComparison.Ordinal);                  /* base table for the V7 victim-plan column */
        Assert.DoesNotContain("v_deadlocks", sql, StringComparison.Ordinal);
        Assert.Contains("deadlock_graph_xml", sql, StringComparison.Ordinal);
        Assert.Contains("victim_process_id", sql, StringComparison.Ordinal);
        Assert.Contains("database_name", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY deadlock_time DESC", sql, StringComparison.Ordinal);
        /* #3541 A3: the cap is the caller's, not the 50 a caller asking for 100 deadlocks never saw. */
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT 50", sql, StringComparison.Ordinal);
    }

    /// <summary>Same shape as the blocked-process pair: <c>get_deadlock_detail</c>'s <c>limit</c> counts graphs
    /// because the graph predicate is in the SQL, and the two consts share one body.</summary>
    [Fact]
    public void RecentDeadlocksWithGraphSql_IsTheUnfilteredRead_PlusTheGraphPredicate_InSql()
    {
        var plain = DarlingBlockingReader.RecentDeadlocksSql;
        var withGraph = DarlingBlockingReader.RecentDeadlocksWithGraphSql;

        Assert.Contains("AND   deadlock_graph_xml IS NOT NULL", withGraph, StringComparison.Ordinal);
        Assert.Contains("AND   deadlock_graph_xml <> ''", withGraph, StringComparison.Ordinal);
        Assert.DoesNotContain("deadlock_graph_xml IS NOT NULL", plain, StringComparison.Ordinal);

        const string Cut = "AND   collection_time <= $3";
        Assert.Equal(plain[..(plain.IndexOf(Cut, StringComparison.Ordinal) + Cut.Length)],
                     withGraph[..(withGraph.IndexOf(Cut, StringComparison.Ordinal) + Cut.Length)]);
        Assert.EndsWith("ORDER BY deadlock_time DESC\nLIMIT $4", withGraph.Replace("\r\n", "\n"), StringComparison.Ordinal);
    }

    /// <summary>
    /// The fingerprint scan ceiling (#3541 A3): #2159 promised the dedup_key filter runs over the window
    /// BEFORE limit, and a hidden 200-row cap was quietly breaking it. The ceiling has to be materially
    /// wider than that cap or the promise is still hollow, and bounded because a scan row carries the graph
    /// or both SQL texts; 5,000 is what the analysis pair-row readers fetch WITHOUT the XML.
    /// </summary>
    [Fact]
    public void FingerprintScanCeiling_IsWiderThanTheOldHiddenCap_AndBounded()
    {
        Assert.True(DarlingBlockingReader.FingerprintScanCeiling >= 1000, "the scan ceiling is not materially wider than the 200-row cap #2159's promise was hollow under");
        Assert.True(DarlingBlockingReader.FingerprintScanCeiling <= 5000, "the scan carries XML per row; the analysis readers fetch 5,000 WITHOUT it");
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
    /// The lock-wait lane (#2484), pinned on the viewer's per-row read it still starts from, and (#3897) on the
    /// family bucketing and per-type legend built over it.
    ///
    /// <para>The viewer's properties, each a real defect if it drifts: the LCK filter, or the read stops being
    /// about locks; the LAG partitioned BY WAIT TYPE, without which one wait type's cadence divides another's
    /// delta; the CAST to double precision before any division — integer division would report a 3 ms delta
    /// over a 60-second interval as ZERO, #2507's defect one read over. #3897's: the family sums the types PER
    /// COLLECTION over that collection's ONE interval (MAX, never a sum of the types' intervals, which would
    /// divide by the number of types), then time-weights across the bucket as summed wait over summed seconds;
    /// the peak is the worst collection's family rate; and the two statements share the rated rows, so the
    /// legend and the series count the same waits.</para>
    /// </summary>
    [Fact]
    public void LockWaitTrendSql_FiltersLockWaits_LagsPerWaitType_AndDividesAsDouble()
    {
        foreach (var sql in new[] { DarlingBlockingTrendReader.LockWaitTrendSql, DarlingBlockingTrendReader.LockWaitTypesSql })
        {
            Assert.Contains("FROM v_wait_stats", sql, StringComparison.Ordinal);
            Assert.Contains("wait_type LIKE 'LCK%'", sql, StringComparison.Ordinal);
            Assert.Contains("LAG(collection_time) OVER (PARTITION BY wait_type ORDER BY collection_time)", sql, StringComparison.Ordinal);
            Assert.Contains("CAST(delta_wait_time_ms AS double precision) AS wait_ms", sql, StringComparison.Ordinal);
            /* #3540: the STORED interval first (0, the unknowable marker, → NULL through NULLIF); the LAG only for
               pre-V127 rows; an unknowable interval leaves the row out rather than reading 0.00. */
            Assert.Contains("CASE WHEN sample_interval_seconds IS NULL", sql, StringComparison.Ordinal);
            Assert.Contains("ELSE NULLIF(sample_interval_seconds, 0)", sql, StringComparison.Ordinal);
            Assert.Contains("END AS interval_seconds", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("ELSE 0 END", sql, StringComparison.Ordinal);
            Assert.Contains("WHERE interval_seconds > 0", sql, StringComparison.Ordinal);

            /* A negative delta is the counter reset across a restart, not a negative wait. */
            Assert.Contains("AND   delta_wait_time_ms >= 0", sql, StringComparison.Ordinal);

            var lower = sql.ToLowerInvariant();
            Assert.DoesNotContain("getdate", lower);
            Assert.DoesNotContain("top (", lower);
            Assert.DoesNotContain("isnull(", lower);
            Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("AVG(", sql, StringComparison.OrdinalIgnoreCase);
        }

        var family = DarlingBlockingTrendReader.LockWaitTrendSql;
        Assert.Contains("MAX(interval_seconds) AS interval_seconds", family, StringComparison.Ordinal);
        Assert.Contains("GROUP BY collection_time", family, StringComparison.Ordinal);
        Assert.Contains("SUM(wait_ms) / SUM(interval_seconds) AS wait_time_ms_per_second", family, StringComparison.Ordinal);
        Assert.Contains("MAX(CASE WHEN interval_seconds > 0 THEN wait_ms / interval_seconds END) AS peak_wait_time_ms_per_second", family, StringComparison.Ordinal);
        Assert.Contains("GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, " + TrendBucketSql.OriginSql + "), $2) AS bucket_start", family, StringComparison.Ordinal);

        var legend = DarlingBlockingTrendReader.LockWaitTypesSql;
        Assert.Contains("GROUP BY wait_type", legend, StringComparison.Ordinal);
        Assert.Contains("SUM(wait_ms) AS total_wait_ms", legend, StringComparison.Ordinal);
        Assert.Contains("SUM(interval_seconds) AS rated_seconds", legend, StringComparison.Ordinal);
        Assert.Contains("MAX(CASE WHEN interval_seconds > 0 THEN wait_ms / interval_seconds END) AS peak_wait_time_ms_per_second", legend, StringComparison.Ordinal);
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
    [InlineData(nameof(DarlingBlockingReader.BlockedProcessReportsWithXmlSql))]
    [InlineData(nameof(DarlingBlockingReader.DmvBlockingSnapshotsSql))]
    [InlineData(nameof(DarlingBlockingReader.RecentDeadlocksSql))]
    [InlineData(nameof(DarlingBlockingReader.RecentDeadlocksWithGraphSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = sqlName switch
        {
            nameof(DarlingBlockingReader.BlockedProcessReportsSql) => DarlingBlockingReader.BlockedProcessReportsSql,
            nameof(DarlingBlockingReader.BlockedProcessReportsWithXmlSql) => DarlingBlockingReader.BlockedProcessReportsWithXmlSql,
            nameof(DarlingBlockingReader.DmvBlockingSnapshotsSql) => DarlingBlockingReader.DmvBlockingSnapshotsSql,
            nameof(DarlingBlockingReader.RecentDeadlocksWithGraphSql) => DarlingBlockingReader.RecentDeadlocksWithGraphSql,
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

    /// <summary>
    /// #5236 W6: a list flag and the point read it feeds test presence with the SAME predicate, <c>IS NOT NULL AND &lt;&gt; ''</c>
    /// (the house pattern the report-XML read uses). A flag that tested only <c>IS NOT NULL</c> would draw a button for an
    /// empty-string plan, and the read behind it would answer "unavailable". The flags live in the two page-keyed flag statements.
    /// </summary>
    [Fact]
    public void TheListFlags_UseThePointReadsPresencePredicate()
    {
        var flagReads = new (string Sql, string Column)[]
        {
            (DarlingBlockingReader.BlockedPlanFlagsSql, "blocked_query_plan_xml"),
            (DarlingBlockingReader.BlockedPlanFlagsSql, "blocking_query_plan_xml"),
            (DarlingBlockingReader.DeadlockVictimPlanFlagsSql, "victim_query_plan_xml"),
        };
        foreach (var (sql, column) in flagReads)
        {
            Assert.Contains($"({column} IS NOT NULL AND {column} <> '')", sql, StringComparison.Ordinal);
        }

        var pointReads = new (string Sql, string Column)[]
        {
            (DarlingStoredPlanReader.BlockedPlanSql, "blocked_query_plan_xml"),
            (DarlingStoredPlanReader.BlockingPlanSql, "blocking_query_plan_xml"),
            (DarlingStoredPlanReader.DeadlockVictimPlanSql, "victim_query_plan_xml"),
        };
        foreach (var (sql, column) in pointReads)
        {
            Assert.Contains($"AND   {column} IS NOT NULL", sql, StringComparison.Ordinal);
            Assert.Contains($"AND   {column} <> ''", sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// #5236: no list statement, and neither of the two variants that page over XML or graphs, names a plan column. In a
    /// compressed TimescaleDB chunk every named column is decompressed for each batch the list scans, and a flag computed there
    /// was evaluated after the per-chunk sorts, so each row's whole plan text went through them (measured: the 7-day deadlock
    /// list on a server where half the rows carry a plan went from 0.3 s to 1.9 s). The flags come from the two keyed
    /// statements, which read only the page's own rows.
    /// </summary>
    [Fact]
    public void TheListStatements_NameNoPlanColumn_AndTheFlagStatementsAreKeyedToThePage()
    {
        foreach (var sql in new[]
        {
            DarlingBlockingReader.BlockedProcessReportsSql,
            DarlingBlockingReader.BlockedProcessReportsWithXmlSql,
            DarlingBlockingReader.RecentDeadlocksSql,
            DarlingBlockingReader.RecentDeadlocksWithGraphSql,
        })
        {
            Assert.DoesNotContain("query_plan_xml", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("has_blocked_plan", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("has_blocking_plan", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("has_victim_plan", sql, StringComparison.Ordinal);
        }

        /* The key rides in the list: collection_time and the id come back so the page can be re-read for its flags. */
        Assert.Contains("blocked_report_id", DarlingBlockingReader.BlockedProcessReportsSql, StringComparison.Ordinal);
        Assert.Contains("deadlock_id", DarlingBlockingReader.RecentDeadlocksSql, StringComparison.Ordinal);

        foreach (var (sql, table, id) in new[]
        {
            (DarlingBlockingReader.BlockedPlanFlagsSql, "blocked_process_reports", "blocked_report_id"),
            (DarlingBlockingReader.DeadlockVictimPlanFlagsSql, "deadlocks", "deadlock_id"),
        })
        {
            Assert.Contains($"FROM {table}", sql, StringComparison.Ordinal);
            Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
            Assert.Contains("AND   collection_time = ANY($2)", sql, StringComparison.Ordinal);
            Assert.Contains($"AND   {id} = ANY($3)", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("LIMIT", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("ORDER BY", sql, StringComparison.Ordinal);
        }
    }

    /// <summary>#5236: the blocked and the blocking plan reads are one statement over two columns, so a change to the key, the
    /// floor or the copy order cannot reach one side and miss the other.</summary>
    [Fact]
    public void TheTwoBlockingPlanSqls_DifferOnlyInTheColumn()
    {
        Assert.NotEqual(DarlingStoredPlanReader.BlockedPlanSql, DarlingStoredPlanReader.BlockingPlanSql);
        Assert.Equal(
            DarlingStoredPlanReader.BlockingPlanSql,
            DarlingStoredPlanReader.BlockedPlanSql.Replace("blocked_query_plan_xml", "blocking_query_plan_xml", StringComparison.Ordinal));

        /* The key is the row's own, the floor is the partition bound, and the copy taken is the EARLIEST one with a plan. */
        Assert.Contains("AND   event_time = $2", DarlingStoredPlanReader.BlockedPlanSql, StringComparison.Ordinal);
        Assert.Contains("AND   collection_time >= $7", DarlingStoredPlanReader.BlockedPlanSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time, blocked_report_id", DarlingStoredPlanReader.BlockedPlanSql, StringComparison.Ordinal);
        Assert.DoesNotContain("DESC", DarlingStoredPlanReader.BlockedPlanSql, StringComparison.Ordinal);
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
    public async Task GetDeadlocks_NamesTheDatabase_AndNullStaysNull_AgainstDevPostgres()
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
                @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, database_name, deadlock_time, victim_process_id, victim_sql_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, "GP", t, "process1", "DELETE FROM Posts");
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, database_name, deadlock_time, victim_process_id, victim_sql_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), t.AddMinutes(-10), ServerId, ServerName, DBNull.Value, t.AddMinutes(-10), "process2", "DELETE FROM Posts");

            using var doc = System.Text.Json.JsonDocument.Parse(await DarlingMcpBlockingTools.GetDeadlocks(postgres, ServerName, 24, 10));
            var rows = doc.RootElement.GetProperty("deadlocks").EnumerateArray().ToList();

            Assert.Equal(2, rows.Count);
            Assert.Equal("GP", rows[0].GetProperty("database_name").GetString());
            Assert.Equal(System.Text.Json.JsonValueKind.Null, rows[1].GetProperty("database_name").ValueKind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

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
            /* #3541 A3: two planted rows (one XE, one DMV on a different pair) merge to a two-row page well
               under the default limit, so the page says so — and no `total_` key is on it. */
            JsonAssert.Contains("\"events_returned\": 2", blocking);
            JsonAssert.Contains("\"truncated\": false", blocking);
            Assert.DoesNotContain("total_events", blocking, StringComparison.Ordinal);

            DarlingMcpTestData.AssertEnvelope(await DarlingMcpBlockingTools.GetDeadlocks(postgres, ServerName), ServerName, "deadlocks");
            var detail = await DarlingMcpBlockingTools.GetDeadlockDetail(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(detail, ServerName, "deadlock_graph_xml");
            var xml = await DarlingMcpBlockingTools.GetBlockedProcessXml(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(xml, ServerName, "blocked_process_report_xml");

            /* The per-minute trend series (the planted BPR + deadlock fall inside the default 24h window). */
            DarlingMcpTestData.AssertEnvelope(await DarlingMcpBlockingTools.GetBlockingTrend(postgres, ServerName), ServerName, "trend");
            DarlingMcpTestData.AssertEnvelope(await DarlingMcpBlockingTools.GetDeadlockTrend(postgres, ServerName), ServerName, "trend");

            /* Unknown server resolves to the listing error. */
            Assert.StartsWith("Could not resolve server.", McpHelpers.ErrorMessageOf(await DarlingMcpBlockingTools.GetDeadlocks(postgres, "darling-no-such-server")), StringComparison.Ordinal);

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

    /* #4966: where the data starts, on the five event-list tools. The coverage probe is DataWindowFloor.Source.ForCollectorTable (the later
       of the retention edge and the server's registration), moved earlier by the oldest event the page shows. Every instant is an offset
       from one anchor minute, so the probe's purge edge never lands inside a window. */
    private sealed record EventTool(string Name, string Table, string Label, string ArrayKey);

    private static readonly EventTool[] EventTools =
    [
        new("get_blocking", "blocked_process_reports", "blocked_process_reports and dmv_blocking_snapshots", "events"),
        new("get_blocking_dmv", "dmv_blocking_snapshots", "blocked_process_reports and dmv_blocking_snapshots", "events"),
        new("get_deadlocks", "deadlocks", "deadlocks", "deadlocks"),
        new("get_deadlock_detail", "deadlocks", "deadlocks", "deadlocks"),
        new("get_blocked_process_xml", "blocked_process_reports", "blocked_process_reports", "reports"),
        new("get_long_query_completions", "long_query_completions", "long_query_completions", "completions"),
    ];

    private static DateTime EventAnchor()
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
    }

    private static string EventServer(EventTool tool, string window) => "darling-mcp-evt-" + window + "-" + tool.Name.Replace('_', '-');

    private static System.Text.Json.JsonElement ParseEvent(string json) => System.Text.Json.JsonDocument.Parse(json).RootElement.Clone();

    private static Task<string> CallEventAsync(NpgsqlDataSource postgres, EventTool tool, string server, int hours, DateTime end, int limit = 15)
    {
        var asOf = WebDataStartNote.FormatWindowEnd(end);
        return tool.Name switch
        {
            "get_blocking" or "get_blocking_dmv" => DarlingMcpBlockingTools.GetBlocking(postgres, server, hours, limit, as_of: asOf),
            "get_deadlocks" => DarlingMcpBlockingTools.GetDeadlocks(postgres, server, hours, limit, as_of: asOf),
            "get_deadlock_detail" => DarlingMcpBlockingTools.GetDeadlockDetail(postgres, server, hours, limit, as_of: asOf),
            "get_blocked_process_xml" => DarlingMcpBlockingTools.GetBlockedProcessXml(postgres, server, hours, limit, as_of: asOf),
            _ => DarlingMcpLongQueryTools.GetLongQueryCompletions(postgres, server, hours, limit, as_of: asOf),
        };
    }

    /// <summary>A server registered at <paramref name="created"/> with three events an hour apart from <paramref name="firstEvent"/> (none when null).</summary>
    private static async Task SeedEventServerAsync(
        NpgsqlConnection connection, EventTool tool, string name, DateTime created, DateTime? firstEvent, System.Threading.CancellationToken ct,
        DateTime? collectedAt = null)
    {
        var serverId = ServerIdHelper.GetDeterministicHashCode(name);
        await DeleteEventServerAsync(connection, name, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, name, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET created_date = $2 WHERE server_id = $1", serverId, DarlingMcpTestData.Naive(created));
        if (firstEvent is not DateTime first) return;

        for (var i = 0; i < 3; i++)
        {
            var at = DarlingMcpTestData.Naive(first.AddHours(i));
            var collected = collectedAt is DateTime c ? DarlingMcpTestData.Naive(c) : at;
            switch (tool.Table)
            {
                case "blocked_process_reports":
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        @"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text, blocked_process_report_xml, contentious_object)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
                        CollectionIdGenerator.Next(), collected, serverId, name, at, Db, 55 + i, 60, 8000L, "X", "SELECT 1", "UPDATE Posts SET Score = Score + 1", "<blocked-process-report><blocked-process><process spid=\"55\"/></blocked-process></blocked-process-report>", "dbo.Posts");
                    break;
                case "dmv_blocking_snapshots":
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        @"INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, monitor_loop, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocking_status, contentious_object, blocked_sql_text, blocking_sql_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
                        CollectionIdGenerator.Next(), collected, serverId, name, -1, at, Db, 70 + i, 80, 3000L, "S", "suspended", "dbo.Users", "SELECT 2", "WAITFOR DELAY '00:01'");
                    break;
                case "deadlocks":
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                        CollectionIdGenerator.Next(), collected, serverId, name, at, "process123", "DELETE FROM Posts", "<deadlock><victim-list><victimProcess id=\"process123\"/></victim-list><process-list><process id=\"process123\"><inputbuf>DELETE FROM Posts</inputbuf></process></process-list></deadlock>");
                    break;
                default:
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        @"INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, duration_microseconds, statement_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                        CollectionIdGenerator.Next(), collected, serverId, name, at, "rpc_completed", Db, (long)(i + 1) * 1_000_000, "EXEC p" + i);
                    break;
            }
        }
    }

    private static async Task DeleteEventServerAsync(NpgsqlConnection connection, string name, System.Threading.CancellationToken ct)
    {
        var serverId = ServerIdHelper.GetDeterministicHashCode(name);
        foreach (var table in new[] { "blocked_process_reports", "dmv_blocking_snapshots", "deadlocks", "long_query_completions", "servers" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", serverId);
        }
    }

    /// <summary>Runs <paramref name="body"/> for each event tool against its own seeded server, then removes what it seeded.</summary>
    private static async Task ForEachEventToolAsync(
        string window, Func<NpgsqlConnection, NpgsqlDataSource, EventTool, string, DateTime, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live blocking-tools test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            var end = EventAnchor();
            foreach (var tool in EventTools)
            {
                await body(connection, postgres, tool, EventServer(tool, window), end);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                foreach (var tool in EventTools)
                {
                    await DeleteEventServerAsync(cleanup, EventServer(tool, window), cleanupCt);
                }
            });
        }
    }

    [Fact]
    public async Task EventLists_ForAServerAddedTwoDaysAgo_NameWhereCoverageStarts_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachEventToolAsync("added", async (connection, postgres, tool, name, end) =>
        {
            var added = end.AddDays(-2);
            await SeedEventServerAsync(connection, tool, name, added, end.AddDays(-1), ct);

            var root = ParseEvent(await CallEventAsync(postgres, tool, name, 168, end));

            Assert.True(root.GetProperty("window_truncated").GetBoolean(), tool.Name);
            Assert.Equal(McpHelpers.FormatEffectiveStart(added), root.GetProperty("effective_start").GetString());
            Assert.Equal(
                DarlingMcpWindowNotice.Build(added, end.AddHours(-168), tool.Label).TruncationNote,
                root.GetProperty("truncation_note").GetString());
            Assert.Contains("raw " + tool.Label + " retains", root.GetProperty("truncation_note").GetString(), StringComparison.Ordinal);
            Assert.False(root.TryGetProperty("effective_hours_back", out _), tool.Name);
            Assert.Equal(3, root.GetProperty(tool.ArrayKey).GetArrayLength());

            /* Written right after hours_back, in the contract's order. */
            var names = root.EnumerateObject().Select(p => p.Name).ToList();
            var at = names.IndexOf("hours_back");
            Assert.Equal(["effective_start", "window_truncated", "truncation_note"], names.Skip(at + 1).Take(3));
        });
    }

    [Fact]
    public async Task EventLists_WhoseFirstEventComesLate_AreCovered_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachEventToolAsync("quiet", async (connection, postgres, tool, name, end) =>
        {
            await SeedEventServerAsync(connection, tool, name, end.AddDays(-30), end.AddDays(-5), ct);

            var root = ParseEvent(await CallEventAsync(postgres, tool, name, 168, end));

            Assert.False(root.GetProperty("window_truncated").GetBoolean(), tool.Name);
            Assert.Equal(System.Text.Json.JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
            var effectiveStart = DateTime.Parse(root.GetProperty("effective_start").GetString()!, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind);
            Assert.InRange((effectiveStart - end.AddHours(-168)).TotalSeconds, 0, 120);
        });
    }

    /// <summary>An event stamped before the server's first collection (a backfill) is shown, so the notice cannot name a start later than it.</summary>
    [Fact]
    public async Task AnEvent_OlderThanTheFirstCollection_GivesNoNotice_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachEventToolAsync("backfill", async (connection, postgres, tool, name, end) =>
        {
            /* Registered an hour ago and collected 30 minutes ago, but the events are stamped 23.5 hours back. The probe reads collection_time, so only
               the oldest event time on the page puts the notice's start before the registration: it stays quiet only if that time is folded in. */
            await SeedEventServerAsync(connection, tool, name, end.AddHours(-1), end.AddHours(-23.5), ct, collectedAt: end.AddMinutes(-30));

            var root = ParseEvent(await CallEventAsync(postgres, tool, name, 24, end));

            Assert.Equal(3, root.GetProperty(tool.ArrayKey).GetArrayLength());
            Assert.False(root.GetProperty("window_truncated").GetBoolean(), tool.Name);
            Assert.Equal(System.Text.Json.JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
            Assert.Equal(McpHelpers.FormatEffectiveStart(end.AddHours(-23.5)), root.GetProperty("effective_start").GetString());
        });
    }

    [Fact]
    public async Task AnEmptyAnswer_PastCoverage_CarriesTheNoticeUnderHints_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachEventToolAsync("empty", async (connection, postgres, tool, name, end) =>
        {
            var added = end.AddDays(-2);
            await SeedEventServerAsync(connection, tool, name, added, null, ct);

            var root = ParseEvent(await CallEventAsync(postgres, tool, name, 1, end.AddDays(-5)));

            Assert.Equal("empty", root.GetProperty("status").GetString());
            var hints = root.GetProperty("hints");
            Assert.True(hints.GetProperty("window_truncated").GetBoolean(), tool.Name);
            Assert.Equal(System.Text.Json.JsonValueKind.Null, hints.GetProperty("effective_start").ValueKind);
            Assert.Contains("no collection of " + tool.Label, hints.GetProperty("truncation_note").GetString(), StringComparison.Ordinal);
            Assert.False(root.TryGetProperty("window_truncated", out _), tool.Name);
        });
    }

    /// <summary>A window of 90 minutes or less that answered rows starts no probe: the stand-in would throw if it ran.</summary>
    [Fact]
    public async Task AShortWindow_WithRows_StartsNoProbe_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachEventToolAsync("short", async (connection, postgres, tool, name, end) =>
        {
            await SeedEventServerAsync(connection, tool, name, end.AddDays(-30), end.AddMinutes(-40), ct);

            var probes = 0;
            DarlingMcpWindowNotice.TestOnlyProbe = () => { probes++; throw new TimeoutException("the probe must not run"); };
            var bodySucceeded = false;
            try
            {
                var root = ParseEvent(await CallEventAsync(postgres, tool, name, 1, end));

                Assert.Equal(0, probes);
                Assert.True(root.GetProperty(tool.ArrayKey).GetArrayLength() > 0, tool.Name);
                Assert.False(root.GetProperty("window_truncated").GetBoolean(), tool.Name);
                bodySucceeded = true;
            }
            finally
            {
                await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () =>
                {
                    DarlingMcpWindowNotice.TestOnlyProbe = null;
                    return Task.CompletedTask;
                });
            }
        });
    }

    /// <summary>A page cut by the limit still carries the notice, computed over the events it shows.</summary>
    [Fact]
    public async Task ACappedPage_StillCarriesTheNotice_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachEventToolAsync("capped", async (connection, postgres, tool, name, end) =>
        {
            var added = end.AddDays(-2);
            await SeedEventServerAsync(connection, tool, name, added, end.AddDays(-1), ct);

            var root = ParseEvent(await CallEventAsync(postgres, tool, name, 168, end, limit: 2));

            Assert.True(root.GetProperty("truncated").GetBoolean(), tool.Name);
            Assert.Equal(2, root.GetProperty(tool.ArrayKey).GetArrayLength());
            Assert.True(root.GetProperty("window_truncated").GetBoolean(), tool.Name);
            Assert.Equal(McpHelpers.FormatEffectiveStart(added), root.GetProperty("effective_start").GetString());
        });
    }

    /// <summary>A coverage probe that throws costs the notice, never the rows: the rows come back without the three keys, and an empty answer without hints.</summary>
    [Fact]
    public async Task AFailedProbe_CostsTheNotice_NeverTheRows_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachEventToolAsync("probefail", async (connection, postgres, tool, name, end) =>
        {
            await SeedEventServerAsync(connection, tool, name, end.AddDays(-2), end.AddDays(-1), ct);

            var bodySucceeded = false;
            DarlingMcpWindowNotice.TestOnlyProbe = () => throw new TimeoutException("the probe's deadline passed");
            try
            {
                var root = ParseEvent(await CallEventAsync(postgres, tool, name, 168, end));

                Assert.False(root.TryGetProperty("status", out _), tool.Name);
                Assert.False(root.TryGetProperty("error", out _), tool.Name);
                Assert.Equal(3, root.GetProperty(tool.ArrayKey).GetArrayLength());
                Assert.False(root.TryGetProperty("effective_start", out _), tool.Name);
                Assert.False(root.TryGetProperty("window_truncated", out _), tool.Name);
                Assert.False(root.TryGetProperty("truncation_note", out _), tool.Name);

                var empty = ParseEvent(await CallEventAsync(postgres, tool, name, 1, end.AddDays(-5)));
                Assert.Equal("empty", empty.GetProperty("status").GetString());
                Assert.False(empty.TryGetProperty("hints", out _), tool.Name);
                bodySucceeded = true;
            }
            finally
            {
                await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () =>
                {
                    DarlingMcpWindowNotice.TestOnlyProbe = null;
                    return Task.CompletedTask;
                });
            }
        });
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
