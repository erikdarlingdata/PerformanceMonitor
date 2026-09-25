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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the index / object diagnostic-depth MCP slice — get_table_index_sizes, get_index_usage,
/// get_object_locking, get_database_sizes over the Postgres store. Ungated: tool surface, the single
/// server_name param each, the latest-snapshot read SQL pins (v_index_object_stats / v_database_size_stats),
/// and the Gemini-clean advertised schema.
/// </summary>
public sealed class DarlingMcpObjectStatsToolsSurfaceAndSqlTests
{
    private static readonly string[] ObjectStatsToolSurface =
    {
        "get_database_sizes",
        "get_index_usage",
        "get_object_locking",
        "get_table_index_sizes",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpObjectStatsTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheFourObjectStatsTools()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(ObjectStatsToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpObjectStatsTools).GetCustomAttribute<McpServerToolTypeAttribute>());
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
    [InlineData("get_table_index_sizes")]
    [InlineData("get_object_locking")]
    [InlineData("get_database_sizes")]
    public void ParamContract_ServerNameOnly_Optional(string toolName)
    {
        var p = McpParams(toolName);
        Assert.Equal(new[] { "server_name" }, p.Select(x => x.Name).ToArray());
        Assert.True(p.Single().Optional);
    }

    /// <summary>
    /// #2636 moved <c>get_index_usage</c> off the server-name-only contract, and it had to.
    ///
    /// <para>Rows sort unused-first across the WHOLE server behind a cap, so on an instance with enough
    /// unused indexes in one database the entire answer comes from that database and every Active index
    /// elsewhere is invisible. A field report hit exactly that: healthy collection, full retention, zero
    /// returned rows for the database asked about, and nothing in the answer to distinguish "not returned"
    /// from "not collected". <c>database_name</c> is what makes the question askable and <c>limit</c> is
    /// what makes the cap the caller's; both stay OPTIONAL, so every existing caller behaves as before.</para>
    /// </summary>
    [Fact]
    public void IndexUsage_TakesADatabaseFilterAndALimit_BothOptional()
    {
        var p = McpParams("get_index_usage");

        Assert.Equal(new[] { "server_name", "database_name", "limit" }, p.Select(x => x.Name).ToArray());
        Assert.All(p, x => Assert.True(x.Optional, $"{x.Name} must stay optional — existing callers pass neither"));
    }

    [Fact]
    public void ObjectSizeGrowthSql_RollsUpPerTable_ComputesGrowth()
    {
        var sql = DarlingObjectStatsReader.ObjectSizeGrowthSql;
        /* Bare relation and column names survive aliasing, so they stay plain substring searches; the
           clause-shaped needles go through SqlTextPin (#3217). */
        Assert.Contains("FROM v_index_object_stats", sql, StringComparison.Ordinal);
        SqlTextPin.AssertExpresses("SUM(reserved_mb)", sql, "the per-table size is no longer a sum of its indexes");
        SqlTextPin.AssertExpresses(
            "GROUP BY database_name, schema_name, table_name",
            sql,
            "the rollup is no longer per table");
        /* #3541 A12: the growth figures are no longer derived in SQL — the raw baselines come back as their own
           nullable columns and ObjectSizeGrowthRow derives each figure from exactly the baseline it names
           (McpZeroIsAMeasurementTests pins the fold's absence and the derivations). */
        Assert.Contains("reserved_mb_7d_ago", sql, StringComparison.Ordinal);
        Assert.Contains("reserved_mb_30d_ago", sql, StringComparison.Ordinal);
        Assert.Contains("reserved_mb_oldest", sql, StringComparison.Ordinal);
        SqlTextPin.AssertExpresses("ORDER BY l.current_reserved_mb DESC", sql, "the biggest table no longer sorts first");
    }

    [Fact]
    public void IndexUsageSql_LatestSnapshot_ClassifiesUnusedWriteOnlyActive()
    {
        var sql = DarlingObjectStatsReader.IndexUsageSql;
        /* #4134: PostgreSQL sorts NULLs FIRST on DESC, Lite's DuckDB sorts them last. NULLS LAST keeps the two
           products' order the same, and the names break reserved_mb ties so a capped page is stable. */
        SqlTextPin.AssertExpresses("reserved_mb DESC NULLS LAST", sql, "an unsized index sorts ahead of sized ones");
        SqlTextPin.AssertExpresses(
            "reserved_mb DESC NULLS LAST, database_name, schema_name, table_name, index_name NULLS LAST", sql,
            "reserved_mb ties no longer break by name (a heap's NULL name last, as on Lite), so a capped page can change between calls");
        Assert.Contains("FROM v_index_object_stats", sql, StringComparison.Ordinal);
        SqlTextPin.AssertExpresses("MAX(collection_time)", sql, "the read is no longer the latest snapshot");
        /* Case is NOT normalised: these are the values the tool emits and callers compare. */
        Assert.Contains("'Unused'", sql, StringComparison.Ordinal);
        Assert.Contains("'Write-only'", sql, StringComparison.Ordinal);
        Assert.Contains("'Active'", sql, StringComparison.Ordinal);
        Assert.Contains("user_seeks", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// This pin used to be named <c>IndexLockingSql_PerDatabaseLatest_…</c> and, in that name, asserted the
    /// defect as intended behaviour: "latest" resolved PER <c>database_name</c>, which #3876 proved makes
    /// every name the store has ever seen an immortal group, so a renamed-away database is returned forever.
    /// #3878 substituted the server-latest anchor at this read and the Viewer's three, and renaming the test
    /// with it is the point rather than a tidy-up: a behaviour pinned by name is a claim about what the read
    /// is FOR, and that claim is what changed. The two halves the old name also carried — only contended rows,
    /// most contended first — are unchanged and still asserted here.
    ///
    /// <para>A bare <c>MAX(collection_time)</c> is no longer a sufficient needle, because the shape being
    /// retired contained one too. The anchor is pinned whole, and the retired grouping asserted absent, so
    /// this cannot pass again by going back.</para>
    /// </summary>
    [Fact]
    public void IndexLockingSql_ServerLatestCapture_NonzeroWaits_ContendedFirst()
    {
        var sql = DarlingObjectStatsReader.IndexLockingSql;
        Assert.Contains("FROM v_index_object_stats", sql, StringComparison.Ordinal);
        Assert.Contains("row_lock_wait_in_ms", sql, StringComparison.Ordinal);
        Assert.Contains("page_io_latch_wait_in_ms", sql, StringComparison.Ordinal);
        Assert.Contains("index_lock_promotion_count", sql, StringComparison.Ordinal);
        SqlTextPin.AssertExpresses(
            "collection_time = (SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1)",
            sql,
            "the read is no longer anchored on the server's latest capture — #3878's immortal per-name groups are back");
        Assert.DoesNotContain("GROUP BY database_name", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3880, Erik's ruling on the call #3878/#3879 recorded: the read PROJECTS its anchor column, so
    /// <c>get_object_locking</c> can stamp the snapshot it answered from. The always-runs half of the live
    /// assertion in <c>DarlingIndexLockingRenamedDatabaseLivePostgresTests</c>: the column is on the row
    /// statement's select list (the general rule
    /// <see cref="McpLatestSnapshotStampTests.EveryStampedRead_SelectsItsStampColumn_OnTheRowStatement"/>
    /// holds per read, and this names the tool) and the record carries it in the position the tool reads.
    /// The negative half matters as much: a stamp fetched by a SECOND <c>MAX(collection_time)</c> read can
    /// resolve to the next capture landing between the two queries, so there is exactly one such subquery in
    /// this constant — the anchor itself.
    ///
    /// <para>That the PAYLOAD publishes <c>captured_at</c> is asserted where every other stamped read's is:
    /// <c>get_object_locking</c> is a <c>Shape.Stamped</c> roster row in <see cref="McpLatestSnapshotStampTests"/>
    /// on both SKUs since #3880, and that roster sweeps both tool bodies for the key and both descriptions
    /// for the words. Repeating the body scan here would be a second spelling of one contract, and the one
    /// that drifts is always the copy.</para>
    /// </summary>
    [Fact]
    public void IndexLockingSql_ProjectsItsAnchorColumn_SoTheToolCanStampTheSnapshot()
    {
        var sql = DarlingObjectStatsReader.IndexLockingSql;
        Assert.Contains("ios.collection_time,", sql, StringComparison.Ordinal);

        /* The projection is on the ROW statement's select list, not only inside the anchor subquery: from the
           statement's SELECT to its FROM. */
        var from = sql.IndexOf("\nFROM ", StringComparison.Ordinal);
        Assert.True(from > 0, "no column-zero FROM — the read's shape has changed; re-anchor this pin");
        var select = sql.LastIndexOf("\nSELECT", from, StringComparison.Ordinal);
        Assert.Contains("collection_time", sql[(select < 0 ? 0 : select)..from], StringComparison.Ordinal);

        /* Exactly one MAX(collection_time): the anchor. A second one would be a stamp read that can see a
           capture the returned rows did not come from. */
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(sql, @"MAX\(collection_time\)"));

        /* The stamp reaches the row type at ordinal 0, which is the mapping the reader's GetDateTime(0) and
           the tool's rows[0].CollectionTime both depend on: a member inserted ahead of it would shift every
           positional read silently. */
        Assert.Equal(
            nameof(DarlingObjectStatsReader.IndexLockingRow.CollectionTime),
            typeof(DarlingObjectStatsReader.IndexLockingRow).GetConstructors().Single().GetParameters()[0].Name);
    }

    [Fact]
    public void DatabaseSizeLatestSql_LatestSnapshot_CarriesSizeAndVolume()
    {
        var sql = DarlingObjectStatsReader.DatabaseSizeLatestSql;
        Assert.Contains("FROM v_database_size_stats", sql, StringComparison.Ordinal);
        Assert.Contains("total_size_mb", sql, StringComparison.Ordinal);
        Assert.Contains("max_size_mb", sql, StringComparison.Ordinal);
        Assert.Contains("volume_free_mb", sql, StringComparison.Ordinal);

        /* #4245: the MAX(collection_time) anchor moved OFF this statement and onto the probe
           GetLatestSnapshotTimeAsync runs first (a windowed probe, falling back to an unbounded one) - this
           statement now binds the resolved stamp as a literal $2, which is what lets the planner exclude
           every other chunk at plan time instead of re-deriving the anchor per call. The "still the latest
           snapshot" guarantee this pin protects now lives on the probes: both still compute MAX(collection_time),
           and the windowed probe's correctness (its MAX, when found, IS the true unbounded MAX) is what makes
           relocating the anchor safe. */
        Assert.Contains("collection_time = $2", sql, StringComparison.Ordinal);
        SqlTextPin.AssertExpresses("MAX(collection_time)", DarlingObjectStatsReader.DatabaseSizeLatestSnapshotWindowedProbeSql,
            "the windowed probe is no longer anchored to the latest snapshot in its window");
        SqlTextPin.AssertExpresses("MAX(collection_time)", DarlingObjectStatsReader.DatabaseSizeLatestSnapshotFallbackProbeSql,
            "the fallback probe is no longer anchored to the latest snapshot");
    }

    [Theory]
    [InlineData(nameof(DarlingObjectStatsReader.ObjectSizeGrowthSql))]
    [InlineData(nameof(DarlingObjectStatsReader.IndexUsageSql))]
    [InlineData(nameof(DarlingObjectStatsReader.IndexLockingSql))]
    [InlineData(nameof(DarlingObjectStatsReader.DatabaseSizeLatestSql))]
    [InlineData(nameof(DarlingObjectStatsReader.DatabaseSizeLatestSnapshotWindowedProbeSql))]
    [InlineData(nameof(DarlingObjectStatsReader.DatabaseSizeLatestSnapshotFallbackProbeSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = sqlName switch
        {
            nameof(DarlingObjectStatsReader.ObjectSizeGrowthSql) => DarlingObjectStatsReader.ObjectSizeGrowthSql,
            nameof(DarlingObjectStatsReader.IndexUsageSql) => DarlingObjectStatsReader.IndexUsageSql,
            nameof(DarlingObjectStatsReader.IndexLockingSql) => DarlingObjectStatsReader.IndexLockingSql,
            nameof(DarlingObjectStatsReader.DatabaseSizeLatestSnapshotWindowedProbeSql) => DarlingObjectStatsReader.DatabaseSizeLatestSnapshotWindowedProbeSql,
            nameof(DarlingObjectStatsReader.DatabaseSizeLatestSnapshotFallbackProbeSql) => DarlingObjectStatsReader.DatabaseSizeLatestSnapshotFallbackProbeSql,
            _ => DarlingObjectStatsReader.DatabaseSizeLatestSql,
        };
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("convert(", lower);   /* CAST(... AS double precision), not T-SQL CONVERT */
        Assert.DoesNotContain("top (", lower);
        Assert.DoesNotContain("isnull(", lower);     /* COALESCE, not ISNULL */
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ReadColumns_ExistInTheGeneratedCollectorTables()
    {
        var ios = PgSchemaGenerator.CreateTable(IndexObjectStatsCollector.Instance);
        Assert.Equal("index_object_stats", IndexObjectStatsCollector.Instance.TargetTable);
        Assert.Contains("reserved_mb", ios, StringComparison.Ordinal);
        Assert.Contains("user_seeks", ios, StringComparison.Ordinal);
        Assert.Contains("row_lock_wait_in_ms", ios, StringComparison.Ordinal);
        Assert.Contains("index_lock_promotion_count", ios, StringComparison.Ordinal);
        Assert.Contains("page_io_latch_wait_in_ms", ios, StringComparison.Ordinal);

        var dss = PgSchemaGenerator.CreateTable(DatabaseSizeStatsCollector.Instance);
        Assert.Equal("database_size_stats", DatabaseSizeStatsCollector.Instance.TargetTable);
        Assert.Contains("total_size_mb", dss, StringComparison.Ordinal);
        Assert.Contains("max_size_mb", dss, StringComparison.Ordinal);
        Assert.Contains("volume_free_mb", dss, StringComparison.Ordinal);

        Assert.Contains("v_index_object_stats", PgSchemaGenerator.AllPassthroughViews);
        Assert.Contains("v_database_size_stats", PgSchemaGenerator.AllPassthroughViews);
    }

    private static List<ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpObjectStatsTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_ForAllFourTools_NoRequiredParams()
    {
        var tools = BuildToolSchemas();
        Assert.Equal(4, tools.Count);
        var violations = tools.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
        foreach (var t in tools)
            Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(t.InputSchema));
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the object-stats tools. Plants an index_object_stats row (with
/// usage + locking counters) and a database_size_stats row, then asserts each tool returns its data-bearing
/// envelope and an empty store returns the miss.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpObjectStatsToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-objectstats-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "StackOverflow";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ObjectStatsTools_ReadPlantedRows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live object-stats-tools test.");

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
                @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_count, row_lock_wait_in_ms, page_lock_wait_count, page_lock_wait_in_ms, index_lock_promotion_count, page_latch_wait_in_ms, page_io_latch_wait_in_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, Db, "dbo", 100, "Posts", 1, "PK_Posts", "CLUSTERED", 5000m, 4800m, 1000000L, 500L, 10L, 20L, 300L, 40L, 12000L, 5L, 800L, 3L, 60L, 2L, 30L);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc, file_name, physical_name, total_size_mb, used_size_mb, max_size_mb, volume_mount_point, volume_total_mb, volume_free_mb)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
                CollectionIdGenerator.Next(), t, ServerId, ServerName, Db, 5, 1, "ROWS", "so_data", "D:\\so.mdf", 100000m, 90000m, -1m, "D:\\", 500000m, 250000m);

            var sizes = await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(sizes, ServerName, "tables");
            Assert.Contains("Posts", sizes, StringComparison.Ordinal);

            DarlingMcpTestData.AssertEnvelope(await DarlingMcpObjectStatsTools.GetIndexUsage(postgres, ServerName), ServerName, "indexes");
            DarlingMcpTestData.AssertEnvelope(await DarlingMcpObjectStatsTools.GetObjectLocking(postgres, ServerName), ServerName, "objects");
            var dbSizes = await DarlingMcpObjectStatsTools.GetDatabaseSizes(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(dbSizes, ServerName, "databases");
            Assert.Contains("volume_free_mb", dbSizes, StringComparison.Ordinal);

            await DeleteRowsAsync(connection, ct, keepServer: true);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, ServerName)));

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
        var sql = string.Join(" ", new[] { "index_object_stats", "database_size_stats" }
            .Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"));
        if (!keepServer) sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

/// <summary>
/// #3878, the reporter's repro from #3876 executed against Darling's store: a database renamed between
/// captures. The OLD name's contended rows exist only in an EARLIER capture, the NEW name's only in the
/// newest — which is what a rename actually looks like in the store, because the collector re-derives the
/// name from the server on every pass and never revisits old rows.
///
/// <para>Under the retired anchor, "latest" was resolved per <c>database_name</c>, so the old name kept a
/// group of its own whose newest row was the last capture before the rename — forever. Anchored on the
/// SERVER's newest capture, all four reads see only what the newest pass collected. All four are asserted in
/// one test deliberately: they are one user-visible surface, and the interesting failure is a PARTIAL fix.
/// The MCP tool would hand an agent a dead name as a live peer; the Viewer's grid would display it; its DB
/// selector would OFFER it, which is how it could still be picked rather than merely seen; and the filtered
/// arm is what a pick lands on, so leaving that one behind would make the dead name reachable by the very
/// act of selecting it. The old name's rows are asserted still PRESENT in the store at their own capture
/// time, because none of this rewrites history — it only stops reading it as the present.</para>
///
/// <para>Live-gated (<c>DARLING_TEST_PG</c>): the claim is about what SQL returns over planted rows, and the
/// always-runs structural guards for the same property are the anchor pins in
/// <c>DarlingMcpObjectStatsToolsSurfaceAndSqlTests</c> and <c>ViewerFinOpsSqlTests</c>.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingIndexLockingRenamedDatabaseLivePostgresTests
{
    private const string ServerName = "darling-locking-rename-3878";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string OldName = "SalesDb_Old";
    private const string NewName = "SalesDb";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AfterADatabaseRename_AllFourLockingReads_ShowOnlyTheCurrentName()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live locking-rename test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        await using var viewer = new PerformanceMonitor.Darling.Viewer.ViewerDataService(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var newest = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);
            var beforeRename = newest.AddDays(-30);

            /* Pre-rename capture: contention under the OLD name only. */
            await InsertContendedRowAsync(connection, ct, beforeRename, OldName, rowLockWaitMs: 40_000);
            /* Newest capture: the same workload under the NEW name; the old name is absent from this pass. */
            await InsertContendedRowAsync(connection, ct, newest, NewName, rowLockWaitMs: 70_000);

            /* 1. The MCP reader behind get_object_locking. */
            var mcpRows = await DarlingObjectStatsReader.GetIndexLockingAsync(postgres, ServerId, 200, ct);
            Assert.Contains(mcpRows, r => r.DatabaseName == NewName && r.RowLockWaitInMs == 70_000);
            Assert.DoesNotContain(mcpRows, r => r.DatabaseName == OldName);

            /* #3880: every row the reader returns carries the NEWEST capture's stamp, because the anchor and
               the stamp are now the same column of the same statement. The pre-rename capture is 30 days back
               in this fixture, so a stamp taken from the wrong rows or re-read with a second MAX() over a
               different predicate would be a MONTH off, not a rounding difference. */
            Assert.All(mcpRows, r => Assert.Equal(newest, r.CollectionTime));

            /* ...and through the tool itself, so the envelope an agent reads is checked too. Parsed, never
               substring-matched on quoted text: the serializer escapes apostrophes. */
            var payload = await DarlingMcpObjectStatsTools.GetObjectLocking(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(payload, ServerName, "objects");
            var names = DatabaseNamesIn(payload);
            Assert.Contains(NewName, names);
            Assert.DoesNotContain(OldName, names);

            /* The published stamp, read as JSON: present, round-trippable, and the newest capture's instant
               rather than the rename-era one. #3541 A10's point is that a latest read says WHEN, and #3880's
               is that a read anchored on ONE instant can say WHICH — this is that claim, executed. */
            using (var doc = System.Text.Json.JsonDocument.Parse(payload))
            {
                var stamp = doc.RootElement.GetProperty("captured_at").GetString();
                Assert.False(string.IsNullOrWhiteSpace(stamp));
                var parsed = DateTime.Parse(stamp!, System.Globalization.CultureInfo.InvariantCulture,
                    System.Globalization.DateTimeStyles.RoundtripKind);
                Assert.Equal(newest, parsed);
                Assert.NotEqual(beforeRename, parsed);
            }

            /* 2. The Viewer's all-databases grid. */
            var gridRows = await viewer.GetIndexLockingAsync(ServerId, 200, null, ct);
            Assert.Contains(gridRows, r => r.DatabaseName == NewName && r.RowLockWaitInMs == 70_000);
            Assert.DoesNotContain(gridRows, r => r.DatabaseName == OldName);

            /* 3. The DB selector: a name it does not offer cannot be picked. */
            var selector = await viewer.GetIndexLockingDatabasesAsync(ServerId, ct);
            Assert.Contains(NewName, selector);
            Assert.DoesNotContain(OldName, selector);

            /* 4. The filtered arm, asked for the dead name directly — the one a stale bookmark or a
               hand-typed filter would still reach. */
            Assert.Empty(await viewer.GetIndexLockingAsync(ServerId, 200, OldName, ct));
            Assert.NotEmpty(await viewer.GetIndexLockingAsync(ServerId, 200, NewName, ct));

            /* History is intact: the pre-rename rows are still there, at the capture that saw them. */
            using var history = new NpgsqlCommand(
                "SELECT COUNT(*) FROM index_object_stats WHERE server_id = $1 AND database_name = $2", connection);
            history.Parameters.AddWithValue(ServerId);
            history.Parameters.AddWithValue(OldName);
            Assert.Equal(1L, (long)(await history.ExecuteScalarAsync(ct))!);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The database names the tool's <c>objects</c> array carries, read as JSON rather than scanned
    /// as text.</summary>
    private static List<string> DatabaseNamesIn(string payload)
    {
        using var doc = System.Text.Json.JsonDocument.Parse(payload);
        var names = new List<string>();
        foreach (var item in doc.RootElement.GetProperty("objects").EnumerateArray())
        {
            if (item.TryGetProperty("database_name", out var name) && name.GetString() is { } value)
                names.Add(value);
        }
        return names;
    }

    private static Task InsertContendedRowAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct,
        DateTime collectionTime, string databaseName, long rowLockWaitMs) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, row_lock_wait_count, row_lock_wait_in_ms, page_lock_wait_count, page_lock_wait_in_ms, index_lock_promotion_count, page_latch_wait_in_ms, page_io_latch_wait_in_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)",
            CollectionIdGenerator.Next(), collectionTime, ServerId, ServerName, databaseName, "dbo", 100, "Orders", 1,
            "PK_Orders", "CLUSTERED", 90m, 85m, 500_000L, 80L, rowLockWaitMs, 5L, 60L, 2L, 30L, 10L);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM index_object_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
