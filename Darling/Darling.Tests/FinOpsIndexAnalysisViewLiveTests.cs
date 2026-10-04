/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// Live pins for <c>get_finops</c> view <c>index_analysis</c>: the tool's payload against the Storage reader on the
/// same seeded store, the empty and not-collected answers, the <c>database_name</c> filter, the <c>limit</c> cut with
/// its counts, and the <c>full_text</c> cap. Server A has a short uptime; server C has a 400-day uptime and long
/// index definitions; server B has no index rows.
/// </summary>
public sealed class FinOpsIndexAnalysisViewLiveTests
{
    internal const string ServerNameA = "darling-finops-iav-a";
    internal const string ServerNameB = "darling-finops-iav-b";
    internal const string ServerNameC = "darling-finops-iav-c";
    internal static readonly int ServerIdA = ServerIdHelper.GetDeterministicHashCode(ServerNameA);
    private static readonly int ServerIdB = ServerIdHelper.GetDeterministicHashCode(ServerNameB);
    private static readonly int ServerIdC = ServerIdHelper.GetDeterministicHashCode(ServerNameC);

    private static string? Cs => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    internal static async Task<ScratchPostgres> SeedAsync(string cs, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(cs, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerNameA, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdB, ServerNameB, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdC, ServerNameC, ct);

        var anchor = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified);
        var old = anchor.AddDays(-3).AddHours(2);
        var recent = anchor.AddDays(-1).AddHours(2);
        const string props = "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, engine_edition, product_version, sqlserver_start_time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";
        await DarlingMcpTestData.ExecAsync(connection, ct, props,
            CollectionIdGenerator.Next(), recent, ServerIdA, ServerNameA, "Standard Edition", 2, "16.0.1000.6", DateTime.Now.AddDays(-5).AddHours(-1));
        await DarlingMcpTestData.ExecAsync(connection, ct, props,
            CollectionIdGenerator.Next(), recent, ServerIdC, ServerNameC, "Standard Edition", 2, "16.0.1000.6", anchor.AddDays(-400));

        foreach (var (id, name) in new[] { (ServerIdA, ServerNameA), (ServerIdC, ServerNameC) })
        {
            await InsertAsync(connection, old, "tenant_alpha", 1, 100, 1, "PK_Orders", "CLUSTERED", "[OrderId]", null, true, true, 900m, 50000, 10, 5, 0, 700, id, name, ct);
            await InsertAsync(connection, recent, "tenant_alpha", 1, 100, 1, "PK_Orders", "CLUSTERED", "[OrderId]", null, true, true, 910.5m, 51000, 12, 6, 1, 800, id, name, ct);
            await InsertAsync(connection, recent, "tenant_alpha", 1, 100, 2, "IX_Orders_Cust", "NONCLUSTERED", "[CustomerId]", "[Total]", false, false, 120.25m, 51000, 400, 20, 0, 800, id, name, ct);
            await InsertAsync(connection, recent, "tenant_alpha", 1, 100, 3, "IX_Orders_Cust_Dup", "NONCLUSTERED", "[CustomerId]", "[Total]", false, false, 118m, 51000, 0, 0, 0, 800, id, name, ct);
            await InsertAsync(connection, recent, "tenant_alpha", 1, 100, 4, "IX_Orders_Unused", "NONCLUSTERED", "[ShipDate]", null, false, false, 64m, 51000, 0, 0, 0, 800, id, name, ct);
            await InsertAsync(connection, recent, "tenant_beta", 2, 200, 1, "PK_Jobs", "CLUSTERED", "[JobId]", null, true, true, 5m, 900, 3, 1, 0, 40, id, name, ct);
            await InsertAsync(connection, recent, "tenant_beta", 2, 200, 2, "IX_Jobs_State", "NONCLUSTERED", "[State]", "[Owner]", false, false, 2.5m, 900, 0, 0, 0, 40, id, name, ct);
        }

        /* Server C: a pair with the same keys and different included columns (the analyzer merges them) whose key and included column lists are long enough that the script and the
           original definition both pass the 300-character cap. */
        var keys = string.Join(", ", Enumerable.Range(1, 14).Select(i => $"[KeyColumn_{i:D2}_padding_padding]"));
        var included = string.Join(", ", Enumerable.Range(1, 14).Select(i => $"[IncludedColumn_{i:D2}_padding]"));
        await InsertAsync(connection, recent, "tenant_gamma", 3, 300, 1, "PK_Wide", "CLUSTERED", "[WideId]", null, true, true, 7m, 1000, 5, 1, 0, 30, ServerIdC, ServerNameC, ct);
        await InsertAsync(connection, recent, "tenant_gamma", 3, 300, 2, "IX_Wide_One", "NONCLUSTERED", keys, included, false, false, 300m, 1000, 90, 1, 0, 30, ServerIdC, ServerNameC, ct);
        await InsertAsync(connection, recent, "tenant_gamma", 3, 300, 3, "IX_Wide_Two", "NONCLUSTERED", keys, "[OtherIncluded_padding_one], [OtherIncluded_padding_two]", false, false, 290m, 1000, 40, 2, 0, 30, ServerIdC, ServerNameC, ct);
        return scratch;
    }

    private static Task InsertAsync(NpgsqlConnection c, DateTime at, string db, int dbId, int objectId, int indexId,
        string index, string type, string keys, string? included, bool unique, bool primary, decimal reservedMb,
        long rows, long seeks, long scans, long lookups, long updates, int serverId, string serverName, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, schema_name, object_id, table_name, index_id, index_name, index_type_desc, key_columns, included_columns, is_unique, is_primary_key, reserved_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_count, row_lock_wait_in_ms, partition_count, sqlserver_start_time) VALUES ($1,$2,$3,$4,$5,$6,'dbo',$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,1,$24)",
            CollectionIdGenerator.Next(), at, serverId, serverName, db, dbId, objectId, "T" + objectId, indexId, index, type, keys,
            (object?)included ?? DBNull.Value, unique, primary, reservedMb, rows, seeks, scans, lookups, updates, indexId * 3L, indexId * 11L,
            at.Date.AddDays(-400));

    private static JsonElement Elem(object value) => JsonSerializer.SerializeToElement(value, McpHelpers.JsonOptions);

    private static void AssertSame(JsonElement expected, JsonElement actual, string what) =>
        Assert.True(JsonElement.DeepEquals(expected, actual), $"{what}: expected {expected.GetRawText()} but got {actual.GetRawText()}");

    private static async Task<JsonDocument> ToolAsync(NpgsqlDataSource pg, string server, int limit = 10, string? db = null, bool full = false, CancellationToken ct = default) =>
        JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(pg, "index_analysis", server, 24, limit, db, full, cancellationToken: ct));

    [Theory]
    [InlineData(ServerNameA)]
    [InlineData(ServerNameC)]
    public async Task Payload_EqualsTheStorageReader_ThroughTheToolsOwnProjectors(string server)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live index_analysis view test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs!, ct);
        await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);
        var id = ServerIdHelper.GetDeterministicHashCode(server);

        var withTimes = await DarlingFinOpsIndexAnalysisReader.GetIndexAnalysisWithSnapshotTimesAsync(pg, id, 30, ct);
        var read = withTimes.Result;
        using var doc = await ToolAsync(pg, server, limit: 50, ct: ct);
        var root = doc.RootElement;

        AssertSame(Elem(DarlingMcpFinOpsTools.IndexAnalysisRollupRow(read.OverallRollup, false)), root.GetProperty("overall"), "overall");
        var dbs = DarlingMcpFinOpsTools.OrderIndexAnalysisDatabases(read.DatabaseRollups);
        Assert.Equal(dbs.Count, root.GetProperty("databases").GetArrayLength());
        for (var i = 0; i < dbs.Count; i++)
            AssertSame(Elem(DarlingMcpFinOpsTools.IndexAnalysisRollupRow(dbs[i], true, withTimes.SnapshotTimes[dbs[i].DatabaseId!.Value])), root.GetProperty("databases")[i], "database " + i);
        var recs = DarlingMcpFinOpsTools.OrderIndexAnalysisRecommendations(read.Recommendations);
        Assert.True(recs.Count > 0);
        Assert.Equal(recs.Count, root.GetProperty("recommendation_count").GetInt32());
        Assert.Equal(recs.Count, root.GetProperty("recommendations").GetArrayLength());
        for (var i = 0; i < recs.Count; i++)
            AssertSame(Elem(DarlingMcpFinOpsTools.IndexAnalysisRecommendationRow(recs[i], false)), root.GetProperty("recommendations")[i], "recommendation " + i);
        Assert.Equal(read.UptimeWarning, root.GetProperty("uptime_warning").GetBoolean());
        Assert.Equal(read.DedupeOnlyApplied, root.GetProperty("dedupe_only_applied").GetBoolean());
        Assert.True(read.Notes.All(n => root.GetProperty("notes").EnumerateArray().Any(x => x.GetString() == n)));
    }

    [Fact]
    public void NoteConstants_EqualTheViewersBannerStrings()
    {
        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.IndexAnalysis.cs").ReplaceLineEndings("\n");
        Assert.Contains("\"" + DarlingMcpFinOpsTools.IndexAnalysisUptimeNote.Replace("\"", "\\\"") + "\"", viewer, StringComparison.Ordinal);
        Assert.Contains("\"" + DarlingMcpFinOpsTools.IndexAnalysisDedupeNote + "\"", viewer, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SeededRecommendations_CarryTheLiteralFieldValues()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live index_analysis view test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs!, ct);
        await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var doc = await ToolAsync(pg, ServerNameC, limit: 50, db: "tenant_alpha", ct: ct);
        var rows = doc.RootElement.GetProperty("recommendations").EnumerateArray().ToList();
        /* Ordered by size: the 118 MB duplicate (0.115 GB) precedes the 64 MB unused index (0.063 GB). */
        Assert.Equal(new[] { "IX_Orders_Cust_Dup", "IX_Orders_Unused" }, rows.Select(r => r.GetProperty("index_name").GetString()).ToArray());
        var dup = rows[0];
        Assert.Equal("DISABLE", dup.GetProperty("action").GetString());
        Assert.Equal(3, dup.GetProperty("index_id").GetInt32());
        Assert.Equal(0.115m, dup.GetProperty("index_size_gb").GetDecimal());
        Assert.Equal(51000, dup.GetProperty("index_rows").GetInt64());
        Assert.StartsWith("ALTER INDEX [IX_Orders_Cust_Dup] ON [tenant_alpha].[dbo].", dup.GetProperty("script").GetString(), StringComparison.Ordinal);
        Assert.Equal(0.063m, rows[1].GetProperty("index_size_gb").GetDecimal());
    }

    [Fact]
    public async Task HandPinnedFigures_AreRoundedAndAveragedAsDocumented()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live index_analysis view test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs!, ct);
        await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var doc = await ToolAsync(pg, ServerNameC, limit: 50, ct: ct);
        var alpha = doc.RootElement.GetProperty("databases").EnumerateArray().Single(d => d.GetProperty("database_name").GetString() == "tenant_alpha");
        /* 910.5 + 120.25 + 118 + 64 MB reserved = 1212.75 MB; / 1024 = 1.18433... GB, so three decimals is 1.184. */
        Assert.Equal(1.184m, alpha.GetProperty("total_size_gb").GetDecimal());
        /* Lock waits: indexes 1-4 carry 3, 6, 9 and 12 waits and 11, 22, 33 and 44 ms; 110 ms / 30 waits = 3.666... -> 3.67. */
        Assert.Equal(30, alpha.GetProperty("lock_wait_count").GetInt64());
        Assert.Equal(3.67m, alpha.GetProperty("avg_lock_wait_ms").GetDecimal());
        Assert.Equal(0m, alpha.GetProperty("avg_latch_wait_ms").GetDecimal());
    }

    [Fact]
    public async Task ServerWithNoIndexRows_AnswersEmpty_AndAPostgresTargetAnswersNotCollected()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live index_analysis view test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs!, ct);
        await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);

        using (var doc = await ToolAsync(pg, ServerNameB, ct: ct))
        {
            Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
            Assert.Contains("No index statistics were collected", doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        }

        /* index_object_stats is a SQL Server collector, so a PostgreSQL target is a permanent gap, not an empty window. */
        using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", ServerIdB, MonitoredEngineKind.Postgres);
        }
        using var gated = await ToolAsync(pg, ServerNameB, ct: ct);
        Assert.Equal("not_collected", gated.RootElement.GetProperty("status").GetString());
        Assert.Contains("index_object_stats", gated.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task DatabaseName_FiltersDatabasesAndRecommendations_AndLeavesOverallNull()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live index_analysis view test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs!, ct);
        await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);

        using (var doc = await ToolAsync(pg, ServerNameC, limit: 50, db: "TENANT_ALPHA", ct: ct))
        {
            var root = doc.RootElement;
            Assert.Equal(JsonValueKind.Null, root.GetProperty("overall").ValueKind);
            Assert.Equal("TENANT_ALPHA", root.GetProperty("database_name").GetString());
            var dbs = root.GetProperty("databases").EnumerateArray().ToList();
            Assert.Single(dbs);
            Assert.Equal("tenant_alpha", dbs[0].GetProperty("database_name").GetString());
            var recs = root.GetProperty("recommendations").EnumerateArray().ToList();
            Assert.NotEmpty(recs);
            Assert.All(recs, r => Assert.Equal("tenant_alpha", r.GetProperty("database_name").GetString()));
            Assert.Equal(recs.Count, root.GetProperty("recommendation_count").GetInt32());
        }

        using var unknown = await ToolAsync(pg, ServerNameC, db: "no_such_database", ct: ct);
        Assert.Equal("empty", unknown.RootElement.GetProperty("status").GetString());
        Assert.Equal("No analyzed indexes for database 'no_such_database' in the latest snapshot.", unknown.RootElement.GetProperty("message").GetString());
    }

    [Fact]
    public async Task Limit_CutsRecommendations_ButCountsTheWholeFilteredSet()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live index_analysis view test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs!, ct);
        await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var all = await ToolAsync(pg, ServerNameC, limit: 50, ct: ct);
        var total = all.RootElement.GetProperty("recommendation_count").GetInt32();
        Assert.True(total > 1);
        Assert.False(all.RootElement.GetProperty("truncated").GetBoolean());

        using var one = await ToolAsync(pg, ServerNameC, limit: 1, ct: ct);
        var root = one.RootElement;
        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.Equal(1, root.GetProperty("limit").GetInt32());
        Assert.Equal(1, root.GetProperty("recommendations").GetArrayLength());
        Assert.Equal(total, root.GetProperty("recommendation_count").GetInt32());
        AssertSame(all.RootElement.GetProperty("recommendations")[0], root.GetProperty("recommendations")[0], "first row");
        AssertSame(all.RootElement.GetProperty("counts_by_action"), root.GetProperty("counts_by_action"), "counts_by_action");
        var sum = root.GetProperty("counts_by_action").EnumerateObject().Sum(p => p.Value.GetInt32());
        Assert.Equal(total, sum);
    }

    [Fact]
    public async Task FullText_FalseCutsAtThreeHundredCharacters_TrueReturnsWholeText()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live index_analysis view test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs!, ct);
        await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var cut = await ToolAsync(pg, ServerNameC, limit: 50, db: "tenant_gamma", ct: ct);
        using var whole = await ToolAsync(pg, ServerNameC, limit: 50, db: "tenant_gamma", full: true, ct: ct);
        Assert.Equal(300, cut.RootElement.GetProperty("text_cap_chars").GetInt32());
        Assert.Equal(JsonValueKind.Null, whole.RootElement.GetProperty("text_cap_chars").ValueKind);

        var cutRows = cut.RootElement.GetProperty("recommendations").EnumerateArray().ToList();
        var wholeRows = whole.RootElement.GetProperty("recommendations").EnumerateArray().ToList();
        Assert.Equal(cutRows.Count, wholeRows.Count);
        var seenScript = false;
        var seenDefinition = false;
        for (var i = 0; i < cutRows.Count; i++)
        {
            var fullScript = wholeRows[i].GetProperty("script").GetString()!;
            var fullDef = wholeRows[i].GetProperty("original_index_definition").GetString()!;
            Assert.False(wholeRows[i].GetProperty("script_truncated").GetBoolean());
            Assert.False(wholeRows[i].GetProperty("definition_truncated").GetBoolean());
            if (fullScript.Length > 300)
            {
                seenScript = true;
                Assert.True(cutRows[i].GetProperty("script_truncated").GetBoolean());
                Assert.Equal(fullScript[..300] + "…", cutRows[i].GetProperty("script").GetString());
            }
            if (fullDef.Length > 300)
            {
                seenDefinition = true;
                Assert.True(cutRows[i].GetProperty("definition_truncated").GetBoolean());
                Assert.Equal(fullDef[..300] + "…", cutRows[i].GetProperty("original_index_definition").GetString());
            }
        }
        Assert.True(seenScript, "no seeded script passed the cap: " + string.Join(" ; ", wholeRows.Select(r => r.GetProperty("action").GetString() + "/" + r.GetProperty("script").GetString()!.Length + "/" + r.GetProperty("original_index_definition").GetString()!.Length)));
        Assert.True(seenDefinition, "no seeded definition passed the cap");
    }
}
