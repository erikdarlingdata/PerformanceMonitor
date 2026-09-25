/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Plan Viewer host's stored-plan reads against the Darling store contract (no live Postgres):
/// the on-demand plan lookups (query_stats' plan by db+query_hash; procedure_stats' by db+schema+object;
/// query_store_stats.query_plan_text by db+query_id+plan_id) and the has_query_plan presence flag the Top
/// Queries grid read carries to gate its Query Plan column.
///
/// <para>Since #1767 the plan XML lives in <c>query_plan_dim</c> and the fact row carries only a digest,
/// so each of these reads must resolve the dimension — query_stats' through <c>v_query_stats</c>,
/// procedure_stats' through its own LEFT JOIN, since no v_procedure_stats exists. Every one of them fails
/// SILENTLY if it does not: the raw column is not missing, it is NULL, so the read returns no rows and the
/// product reports "no plan captured".</para>
///
/// <para>Ordinal correctness + PG execution are covered by the gated live round-trips below.</para>
/// </summary>
public sealed class ViewerPlanHostSqlTests
{
    [Fact]
    public void QueryStatsPlanXmlSql_ReadsStoredXml_KeyedByDatabaseAndHash_LatestNonNull()
    {
        var sql = ViewerDataService.QueryStatsPlanXmlSql;
        /* Both content forms (#2069): plans written since V54 are gzip bytes with the text NULL. */
        Assert.Contains("SELECT query_plan_xml, query_plan_gz", sql, StringComparison.Ordinal);

        /* The RESOLVING view, not the base table (#1767): query_stats.query_plan_xml is NULL on every row
           written since the migration, with the plan itself in query_plan_dim behind a digest. Reading the
           base table here would not error — it would return NULL and look like "no plan captured". */
        Assert.Contains("FROM v_query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("database_name = $2", sql, StringComparison.Ordinal);
        Assert.Contains("query_hash = $3", sql, StringComparison.Ordinal);
        /* The presence guard must accept EITHER form — on the text column alone it discards every
           post-V54 plan silently (zero rows, no error), the #1767 bare-column regression re-run. */
        Assert.Contains("(query_plan_xml IS NOT NULL OR query_plan_gz IS NOT NULL)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal); /* most-recent plan for the key */
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStorePlanTextSql_ReadsStoredText_KeyedByQueryIdAndPlanId_LatestNonNull()
    {
        var sql = ViewerDataService.QueryStorePlanTextSql;
        Assert.Contains("SELECT query_plan_text", sql, StringComparison.Ordinal);
        Assert.Contains("FROM query_store_stats", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("database_name = $2", sql, StringComparison.Ordinal);
        Assert.Contains("query_id = $3", sql, StringComparison.Ordinal);
        Assert.Contains("plan_id = $4", sql, StringComparison.Ordinal); /* (query_id, plan_id) names one specific compiled plan */
        Assert.Contains("query_plan_text IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ProcedureStatsPlanXmlSql_ResolvesThePlanDimensionItself_GuardingOnTheCoalescedExpression()
    {
        var sql = ViewerDataService.ProcedureStatsPlanXmlSql;

        /* There is no v_procedure_stats to resolve the #1767 plan dimension (that view has never existed),
           so this read joins query_plan_dim itself and coalesces. */
        Assert.Contains("SELECT COALESCE(ps.query_plan_xml, qpd.query_plan_xml), qpd.query_plan_gz", sql, StringComparison.Ordinal);
        Assert.Contains("FROM procedure_stats AS ps", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_procedure_stats", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN query_plan_dim AS qpd", sql, StringComparison.Ordinal);
        Assert.Contains("ON qpd.digest = ps.query_plan_digest", sql, StringComparison.Ordinal);

        Assert.Contains("WHERE ps.server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("ps.database_name = $2", sql, StringComparison.Ordinal);
        Assert.Contains("ps.schema_name = $3", sql, StringComparison.Ordinal);
        Assert.Contains("ps.object_name = $4", sql, StringComparison.Ordinal); /* (database, schema, object) = the grid's group key */

        /* THE regression to guard. The presence filter must ride the COALESCED expression, not the bare
           inline column: rows written since #1767 leave query_plan_xml NULL and carry the plan in the
           dimension, so `AND ps.query_plan_xml IS NOT NULL` would discard every new row BEFORE the join
           could resolve it — zero rows back, no error, indistinguishable from "no plan captured". Putting
           the guard back on the bare column is the single most likely way to break this read, and it fails
           completely silently, so assert the coalesced form explicitly rather than by substring luck. */
        Assert.Contains("AND   (COALESCE(ps.query_plan_xml, qpd.query_plan_xml) IS NOT NULL OR qpd.query_plan_gz IS NOT NULL)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("AND   ps.query_plan_xml IS NOT NULL", sql, StringComparison.Ordinal);

        Assert.Contains("ORDER BY ps.collection_time DESC", sql, StringComparison.Ordinal); /* most-recent plan for the key */
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ProcedureStatsPlanColumn_ExistsInTheGeneratedProcedureStatsTable()
    {
        /* The keyed fetch reads procedure_stats.query_plan_xml — pin that the collector-generated table has it. */
        Assert.Equal("procedure_stats", ProcedureStatsCollector.Instance.TargetTable);
        Assert.Contains("query_plan_xml", PgSchemaGenerator.CreateTable(ProcedureStatsCollector.Instance), StringComparison.Ordinal);
    }

    [Fact]
    public void TopQueriesSql_CarriesCheapHasPlanFlag_NotThePlanItself()
    {
        /* The grid gates its Query Plan column on a group-level presence flag; the multi-KB plan XML is
           never pulled into the grid row (it is fetched on demand by GetQueryStatsPlanXmlAsync). The digest
           arm keeps that flag true for rows written since #1767, where the plan lives in query_plan_dim and
           the fact row carries only the key — a digest answers presence without resolving the dimension. */
        var sql = ViewerDataService.TopQueriesSql;
        Assert.Contains("bool_or(query_plan_xml IS NOT NULL OR query_plan_digest IS NOT NULL OR query_plan_xml_bytes IS NOT NULL) AS has_query_plan", sql, StringComparison.Ordinal);
        Assert.Contains("r.has_query_plan", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(ViewerDataService.QueryStatsPlanXmlSql))]
    [InlineData(nameof(ViewerDataService.QueryStorePlanTextSql))]
    [InlineData(nameof(ViewerDataService.ProcedureStatsPlanXmlSql))]
    public void PlanReads_ArePostgresDialect_PositionalParams_NoTsqlIsms(string sqlName)
    {
        var sql = sqlName switch
        {
            nameof(ViewerDataService.QueryStatsPlanXmlSql) => ViewerDataService.QueryStatsPlanXmlSql,
            nameof(ViewerDataService.QueryStorePlanTextSql) => ViewerDataService.QueryStorePlanTextSql,
            _ => ViewerDataService.ProcedureStatsPlanXmlSql,
        };
        Assert.DoesNotContain("getdate", sql.ToLowerInvariant());
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
    }
}

/// <summary>The plan-gating property on the Top Queries row model (the flag the Query Plan column binds).</summary>
public sealed class ViewerPlanHostGatingTests
{
    [Fact]
    public void HasQueryPlan_DefaultsFalse_SoTheDownloadColumnIsDisabledUntilTheReadSaysOtherwise()
    {
        Assert.False(new ViewerQueryStatsRow().HasQueryPlan);
        Assert.True(new ViewerQueryStatsRow { HasQueryPlan = true }.HasQueryPlan);
    }

    /// <summary>#4239: ViewerQuerySnapshotRow's plan flags now follow the same independently-settable pattern
    /// as ViewerQueryStatsRow above (a stored-row read sets the flag from the store's presence column while
    /// leaving the XML null; only the live DMV path sets both together) — this class had no snapshot-row
    /// case before.</summary>
    [Fact]
    public void QuerySnapshotRow_PlanFlags_DefaultFalse_SettableIndependentlyOfPlanXml()
    {
        Assert.False(new ViewerQuerySnapshotRow().HasQueryPlan);
        Assert.False(new ViewerQuerySnapshotRow().HasLiveQueryPlan);
        Assert.True(new ViewerQuerySnapshotRow { HasQueryPlan = true }.HasQueryPlan);
        Assert.True(new ViewerQuerySnapshotRow { HasLiveQueryPlan = true }.HasLiveQueryPlan);
    }

    [Fact]
    public void BlockedProcessRow_PlanFlags_DefaultFalse_GateBlockedAndBlockingIndependently()
    {
        var empty = new ViewerBlockedProcessRow();
        Assert.False(empty.HasBlockedQueryPlan);
        Assert.False(empty.HasBlockingQueryPlan);

        var both = new ViewerBlockedProcessRow
        {
            BlockedQueryPlanXml = "<ShowPlanXML/>",
            BlockingQueryPlanXml = "<ShowPlanXML/>",
        };
        Assert.True(both.HasBlockedQueryPlan);
        Assert.True(both.HasBlockingQueryPlan);

        /* Best-effort: one side can be captured while the other is NULL — the two "View Plan" items gate apart. */
        var blockedOnly = new ViewerBlockedProcessRow { BlockedQueryPlanXml = "<ShowPlanXML/>" };
        Assert.True(blockedOnly.HasBlockedQueryPlan);
        Assert.False(blockedOnly.HasBlockingQueryPlan);
    }

    [Fact]
    public void DeadlockProcessDetail_VictimPlanFlag_DefaultsFalse_ReflectsPresence()
    {
        Assert.False(new DeadlockProcessDetail().HasVictimQueryPlan);
        Assert.True(new DeadlockProcessDetail { VictimQueryPlanXml = "<ShowPlanXML/>" }.HasVictimQueryPlan);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the Plan Viewer host reads. Each plants query/query-store
/// rows — with and without a stored plan — for a negative sentinel server, then asserts the on-demand plan
/// fetch returns the exact stored text (newest wins) and the grid's has_query_plan gate reflects presence.
/// Shares the serialized "live-postgres" collection and cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerPlanHostLivePostgresTests
{
    private const int QueryStatsPlanServerId = -970811;
    private const int QueryStatsNoPlanServerId = -970812;
    private const int QueryStorePlanServerId = -970813;

    /* A small but well-formed ShowPlanXML — the reads only round-trip the text (the WPF host parses it),
       so any stable string proves the fetch; a realistic plan keeps the fixture honest. */
    private const string SamplePlanXml =
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements>" +
        "<StmtSimple StatementText=\"SELECT 1\" StatementId=\"1\" StatementType=\"SELECT\" />" +
        "</Statements></Batch></BatchSequence></ShowPlanXML>";

    private const string NewerPlanXml =
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements>" +
        "<StmtSimple StatementText=\"SELECT 2\" StatementId=\"1\" StatementType=\"SELECT\" />" +
        "</Statements></Batch></BatchSequence></ShowPlanXML>";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task QueryStatsPlan_FetchesNewestStoredXml_AndGridFlagsHasPlan_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live query-stats plan test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "query_stats", QueryStatsPlanServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(cs!);
        var end = TruncateToSeconds(DateTime.UtcNow);
        var start = end.AddHours(-24);
        var older = start.AddHours(1);
        var newer = start.AddHours(2);

        var bodySucceeded = false;
        try
        {
            /* Same (database, query_hash) captured twice with different plans — the newer collection wins. */
            await InsertQueryStatsWithPlanAsync(connection, QueryStatsPlanServerId, older, "StackOverflow", "0xPLAN",
                deltaExec: 1, deltaWorker: 10_000, deltaElapsed: 20_000, queryText: "SELECT plan", planXml: SamplePlanXml);
            await InsertQueryStatsWithPlanAsync(connection, QueryStatsPlanServerId, newer, "StackOverflow", "0xPLAN",
                deltaExec: 1, deltaWorker: 10_000, deltaElapsed: 20_000, queryText: "SELECT plan", planXml: NewerPlanXml);

            var plan = await viewer.GetQueryStatsPlanXmlAsync(QueryStatsPlanServerId, "StackOverflow", "0xPLAN");
            Assert.Equal(NewerPlanXml, plan); /* newest non-null plan for the key */

            var rows = await viewer.GetTopQueriesByCpuAsync(QueryStatsPlanServerId, start, end);
            var row = Assert.Single(rows);
            Assert.Equal("0xPLAN", row.QueryHash);
            Assert.True(row.HasQueryPlan); /* bool_or over the group: a plan exists -> column enabled */

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "query_stats", QueryStatsPlanServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task QueryStatsPlan_NullPlan_FetchReturnsNull_AndGridFlagIsFalse_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live no-plan test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "query_stats", QueryStatsNoPlanServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(cs!);
        var end = TruncateToSeconds(DateTime.UtcNow);
        var start = end.AddHours(-24);

        var bodySucceeded = false;
        try
        {
            /* A query with activity but no captured plan (query_plan_xml NULL). */
            await InsertQueryStatsWithPlanAsync(connection, QueryStatsNoPlanServerId, start.AddHours(1), "StackOverflow", "0xNOPLAN",
                deltaExec: 1, deltaWorker: 10_000, deltaElapsed: 20_000, queryText: "SELECT noplan", planXml: null);

            var plan = await viewer.GetQueryStatsPlanXmlAsync(QueryStatsNoPlanServerId, "StackOverflow", "0xNOPLAN");
            Assert.Null(plan);

            var rows = await viewer.GetTopQueriesByCpuAsync(QueryStatsNoPlanServerId, start, end);
            var row = Assert.Single(rows);
            Assert.False(row.HasQueryPlan); /* no plan captured -> column disabled */

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "query_stats", QueryStatsNoPlanServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task QueryStorePlan_RoundTripsStoredText_ByQueryIdAndPlanId_NullElsewhere_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live query-store plan test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "query_store_stats", QueryStorePlanServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(cs!);
        var t = TruncateToSeconds(DateTime.UtcNow).AddHours(-1);

        var bodySucceeded = false;
        try
        {
            /* plan_id 7 has a stored plan; plan_id 9 (same query) has none. */
            await InsertQueryStoreWithPlanAsync(connection, QueryStorePlanServerId, t, "StackOverflow",
                queryId: 42, planId: 7, execCount: 3, queryText: "SELECT qs", planText: SamplePlanXml);
            await InsertQueryStoreWithPlanAsync(connection, QueryStorePlanServerId, t, "StackOverflow",
                queryId: 42, planId: 9, execCount: 3, queryText: "SELECT qs", planText: null);

            var plan = await viewer.GetQueryStorePlanTextAsync(QueryStorePlanServerId, "StackOverflow", 42, 7);
            Assert.Equal(SamplePlanXml, plan);

            /* The NULL-plan plan_id and an absent plan_id both fetch null. */
            Assert.Null(await viewer.GetQueryStorePlanTextAsync(QueryStorePlanServerId, "StackOverflow", 42, 9));
            Assert.Null(await viewer.GetQueryStorePlanTextAsync(QueryStorePlanServerId, "StackOverflow", 42, 999));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "query_store_stats", QueryStorePlanServerId, cleanupCt));
        }
    }

    // ── Insert helpers (mirror ViewerQueriesTests' column lists + the trailing plan column) ──

    private static async Task InsertQueryStatsWithPlanAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, string databaseName, string queryHash,
        long deltaExec, long deltaWorker, long deltaElapsed, string queryText, string? planXml)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
     last_execution_time, creation_time, sample_interval_seconds,
     delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
     delta_rows, delta_logical_writes, delta_physical_reads, delta_spills,
     total_clr_time, plan_generation_num, query_text, query_plan_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("viewer-plan-e2e");
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue(queryHash);
        command.Parameters.AddWithValue("0xPLANHASH");
        command.Parameters.AddWithValue("0xSQLHANDLE");
        command.Parameters.AddWithValue("0xPLANHANDLE");
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc.AddHours(-1), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(60);
        command.Parameters.AddWithValue(deltaExec);
        command.Parameters.AddWithValue(deltaWorker);
        command.Parameters.AddWithValue(deltaElapsed);
        command.Parameters.AddWithValue(100L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(queryText);
        command.Parameters.AddWithValue((object?)planXml ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertQueryStoreWithPlanAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, string databaseName,
        long queryId, long planId, long execCount, string queryText, string? planText)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, query_id, plan_id, query_hash, query_plan_hash, query_text, module_name,
     execution_type_desc, first_execution_time, last_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes,
     avg_physical_io_reads, avg_rowcount, avg_clr_time_us, avg_log_bytes_used, avg_tempdb_space_used,
     avg_num_physical_io_reads, avg_query_max_used_memory, min_dop, max_dop,
     plan_type, plan_forcing_type, is_forced_plan, force_failure_count, compatibility_level, query_plan_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
        $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("viewer-qs-plan-e2e");
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(planId);
        command.Parameters.AddWithValue("0xQSHASH");
        command.Parameters.AddWithValue("0xQSPLANHASH");
        command.Parameters.AddWithValue(queryText);
        command.Parameters.AddWithValue("dbo.usp_Qs");
        command.Parameters.AddWithValue("Regular");
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc.AddHours(-1), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(execCount);
        command.Parameters.AddWithValue(2000L);
        command.Parameters.AddWithValue(1000L);
        command.Parameters.AddWithValue(100L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(10L);
        command.Parameters.AddWithValue(50L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(5L);
        command.Parameters.AddWithValue(1024L);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue("Compiled");
        command.Parameters.AddWithValue("None");
        command.Parameters.AddWithValue(false);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(160);
        command.Parameters.AddWithValue((object?)planText ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, string table, int serverId, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
