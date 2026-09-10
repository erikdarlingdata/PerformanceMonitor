/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the W1f-1 Queries-tab SQL against the Darling store contract (no live Postgres): the three grid
/// reads (Top Queries / Top Procedures / Query Store), their comparison reads, and their slicer bucket
/// reads. The pins guard the load-bearing clauses the string-only tests can catch — the ranking clause,
/// the LATERAL latest-text shape, the WAITFOR trim, the over-fetch + cap, WHICH RELATION each part reads
/// (since #1767 the text reads must go through <c>v_query_stats</c> to resolve the payload dimensions,
/// while the window aggregates deliberately stay on the base table), the CAST-back-to-bigint on summed
/// aggregates, and the FULL OUTER JOIN / IS NOT DISTINCT FROM comparison shape. Ordinal correctness + PG
/// execution are covered by the gated live round-trips below.
/// </summary>
public sealed class ViewerQueriesSqlTests
{
    // ── Top Queries ──

    [Fact]
    public void TopQueriesSql_AggregatesTheBaseTable_ButResolvesTextThroughTheView()
    {
        var sql = ViewerDataService.TopQueriesSql;

        /* TWO relations on purpose, and the split is the load-bearing part (#1767).

           The ranked CTE aggregates the whole window and projects NO text, so it deliberately keeps reading
           the BASE table: through v_query_stats Postgres would join the plan dimension per row merely to
           evaluate the presence flag (it can drop an unreferenced unique join, but the view's COALESCE
           references it). The LATERAL below needs the actual text, so that one reads the VIEW — which is
           what resolves the payload dimension. Note "FROM v_query_stats" does not contain
           "FROM query_stats", so these two assertions really are about two different relations. */
        var rankedRead = sql.IndexOf("FROM query_stats", StringComparison.Ordinal);
        var lateral = sql.IndexOf("LEFT JOIN LATERAL", StringComparison.Ordinal);
        var textRead = sql.IndexOf("FROM v_query_stats", StringComparison.Ordinal);
        Assert.True(rankedRead >= 0, "the ranked CTE must aggregate the base query_stats table");
        Assert.True(textRead > lateral && lateral > rankedRead,
            "the base-table aggregate comes first; the resolving view is read by the latest-text LATERAL");

        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal); /* end bound for the slicer */
        /* #2012 stage 2: host_object_name joins the key so same-hash statements hosted by different
           procs (INSERT...EXEC callers) split; SQL GROUP BY treats NULLs as equal, so ad-hoc rows
           still collapse. The LATERAL's host constraint keeps each group's representative text from
           another caller's rows (NOT DISTINCT FROM so ad-hoc NULL hosts still match ad-hoc rows). */
        Assert.Contains("GROUP BY database_name, query_hash, host_object_name", sql, StringComparison.Ordinal);
        Assert.Contains("host_object_name IS NOT DISTINCT FROM r.host_object_name", sql, StringComparison.Ordinal);
        Assert.Contains("HAVING SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TopQueriesSql_RanksByTotalElapsedDelta_OverFetchesFive_CapsAtTop()
    {
        var sql = ViewerDataService.TopQueriesSql;
        Assert.Contains("ORDER BY SUM(delta_elapsed_time) DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4 + 5", sql, StringComparison.Ordinal); /* over-fetch so the WAITFOR trim can't shrink below top */
        Assert.Contains("ORDER BY r.total_elapsed_us DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
        Assert.Contains("NOT LIKE 'WAITFOR%'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TopQueriesSql_FetchesTheLatestNonNullQueryText_ViaLateralJoin_WithPlanPresenceFlag()
    {
        var sql = ViewerDataService.TopQueriesSql;
        Assert.Contains("LEFT JOIN LATERAL", sql, StringComparison.Ordinal);
        Assert.Contains("query_text IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
        /* The LATERAL still fetches only query_text (never the multi-KB plan), and it reads v_query_stats so
           the #1767 payload dimension is resolved — the base table's inline query_text is NULL on every row
           written since. The plan is surfaced by a cheap group-level presence flag that gates the grid's
           Query Plan column; the plan XML itself is read on demand (GetQueryStatsPlanXmlAsync). */
        Assert.Contains("FROM v_query_stats", sql, StringComparison.Ordinal);

        /* The flag now has a digest arm: since #1767 the plan lives in query_plan_dim and the fact row
           carries only the key, so a bare `query_plan_xml IS NOT NULL` would report "no plan captured" for
           every new row — and silently, since the grid just stops offering the download. A digest is enough
           to answer presence without resolving the dimension. */
        Assert.Contains("bool_or(query_plan_xml IS NOT NULL OR query_plan_digest IS NOT NULL) AS has_query_plan", sql, StringComparison.Ordinal);
        Assert.Contains("r.has_query_plan", sql, StringComparison.Ordinal); /* the flag rides from the ranked CTE, not the LATERAL */
    }

    [Fact]
    public void TopQueriesSql_CastsEverySummedAggregateBackToBigint()
    {
        /* Postgres SUM(bigint) returns numeric; without the CAST the typed GetInt64 readers throw. */
        var sql = ViewerDataService.TopQueriesSql;
        Assert.Contains("CAST(SUM(delta_execution_count) AS bigint)", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(delta_worker_time) AS bigint)", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(delta_elapsed_time) AS bigint)", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(delta_logical_reads) AS bigint)", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(delta_rows) AS bigint)", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(delta_logical_writes) AS bigint)", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(delta_physical_reads) AS bigint)", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(delta_spills) AS bigint)", sql, StringComparison.Ordinal);
        /* Peak CPU/sec keeps Lite's per-sample expression (double precision). */
        Assert.Contains("NULLIF(sample_interval_seconds, 0)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TopQueriesSql_DropsLitesStalenessFilterAndUtcOffset()
    {
        /* The viewer has no per-server UTC offset; the delta HAVING already excludes never-ran plans. */
        var sql = ViewerDataService.TopQueriesSql;
        Assert.DoesNotContain("INTERVAL", sql, StringComparison.Ordinal);
        /* #1319: $5 is now the global database filter (database_name = ANY($5)), NOT Lite's utc-offset
           staleness param — the viewer still drops Lite's INTERVAL staleness filter. */
        Assert.Contains("database_name = ANY($5)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$6", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TopQueriesSql_StitchesModuleByHandle_DedupsToLatest_LeftJoinsForAdHocFallback()
    {
        /* #1568: read-time attribution. A dedicated CTE picks ONE procedure_stats identity per sql_handle
           (latest collection_time, via ROW_NUMBER + WHERE rn = 1 — Postgres has no QUALIFY), then a LEFT
           JOIN on the shared normalized handle surfaces db.schema.object; unmatched rows stay ad hoc. */
        var sql = ViewerDataService.TopQueriesSql;
        Assert.Contains("module AS (", sql, StringComparison.Ordinal);
        Assert.Contains("FROM procedure_stats", sql, StringComparison.Ordinal);
        Assert.Contains("ROW_NUMBER() OVER (PARTITION BY sql_handle ORDER BY collection_time DESC) AS rn", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE rn = 1", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN module AS m ON m.sql_handle = r.sql_handle", sql, StringComparison.Ordinal);
        Assert.Contains("m.object_name AS module_object_name", sql, StringComparison.Ordinal);
        Assert.Contains("m.schema_name AS module_schema_name", sql, StringComparison.Ordinal);
        Assert.Contains("m.database_name AS module_database_name", sql, StringComparison.Ordinal);
    }

    // ── Top Procedures ──

    [Fact]
    public void TopProceduresSql_GroupsProcedureStatsByObject_RanksByTotalElapsed_BaseTable()
    {
        var sql = ViewerDataService.TopProceduresSql;
        Assert.Contains("FROM procedure_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_procedure_stats", sql, StringComparison.Ordinal); /* no such view exists in Darling */
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY database_name, schema_name, object_name, object_type", sql, StringComparison.Ordinal);
        Assert.Contains("HAVING SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY SUM(delta_elapsed_time) DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(delta_execution_count) AS bigint)", sql, StringComparison.Ordinal);
        /* avg_spills is the one derived double in the proc read. */
        Assert.Contains("CAST(SUM(delta_spills) AS double precision) / NULLIF(SUM(delta_execution_count), 0)", sql, StringComparison.Ordinal);
        /* Item 7: whether the collector stored a plan for the object — gates the grid's Download button on the
           same presence test GetProcedureStatsPlanXmlAsync fetches on. Since #1767 the plan itself lives in
           query_plan_dim and the fact row carries only the digest, so the flag needs the digest arm: without
           it every row written since the migration reports "no plan captured" and the Download button
           silently disappears, with nothing anywhere reporting an error. */
        Assert.Contains("bool_or(query_plan_xml IS NOT NULL OR query_plan_digest IS NOT NULL) AS has_query_plan", sql, StringComparison.Ordinal);
    }

    // ── Query Store ──

    [Fact]
    public void QueryStoreTopSql_GroupsByQueryAndPlan_RanksByTotalDuration_BaseTable()
    {
        var sql = ViewerDataService.QueryStoreTopSql;
        Assert.Contains("FROM query_store_stats", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        /* replica_role is a grouping key: an AG's shared Query Store (2022+) would otherwise report
           primary and secondary workload blended into one row. */
        Assert.Contains("GROUP BY database_name, query_id, plan_id, query_hash, replica_role", sql, StringComparison.Ordinal);
        /* Rank by total duration = executions * avg duration, over-fetch 5, cap at top (Lite's shape). */
        Assert.Contains("ORDER BY SUM(execution_count) * AVG(CAST(avg_duration_us AS double precision)) DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4 + 5", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY r.total_executions * r.avg_duration_ms DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
        Assert.Contains("NOT LIKE 'WAITFOR%'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStoreTopSql_ConvertsMetrics_ForcedPlanBoolean_MemoryPagesToMb_NoPlanText()
    {
        var sql = ViewerDataService.QueryStoreTopSql;
        Assert.Contains("CAST(SUM(execution_count) AS bigint)", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(CAST(avg_duration_us AS double precision)) / 1000.0", sql, StringComparison.Ordinal);
        /* Postgres has no MAX(boolean); the forced-plan flag folds through bool_or. */
        Assert.Contains("bool_or(is_forced_plan)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("MAX(CASE WHEN is_forced_plan", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(CAST(avg_query_max_used_memory AS double precision)) * 8.0 / 1024.0", sql, StringComparison.Ordinal);
        /* View-Plan deferred: the collected query_plan_text column is not read this wave. */
        Assert.DoesNotContain("query_plan_text", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #1841. query_store_stats rows are CUMULATIVE per-Query-Store-interval snapshots: the collector is
    /// incremental on last_execution_time, so the OPEN interval is re-fetched every cycle and stored again
    /// with a growing execution_count. A live store held 496 collections of ONE interval inside a single
    /// hour bucket. Every Query Store read that aggregates across collections must therefore collapse each
    /// interval to its LATEST snapshot BEFORE aggregating, keyed on runtime_stats_interval_id — the REAL
    /// interval identity, collected since #1841 tier 2 — with first_execution_time kept beside it as the
    /// tier-1 proxy for rows collected before it existed.
    ///
    /// <para>Pinned as one theory over every affected constant so a new Query Store read, or an edit that
    /// drops the CTE from an existing one, fails here rather than silently shipping inflated totals. The
    /// two reads deliberately left un-deduped are NOT in this list: QueryStoreHistorySql (a raw
    /// per-collection projection with no aggregate — the "show me every snapshot" surface) and the CAGG
    /// route, whose sums were materialized from un-deduped rows and cannot be repaired at read time.</para>
    /// </summary>
    [Theory]
    [InlineData(nameof(ViewerDataService.QueryStoreTopSql))]
    [InlineData(nameof(ViewerDataService.QueryStoreComparisonSql))]
    [InlineData(nameof(ViewerDataService.QueryStoreSlicerSql))]
    [InlineData(nameof(ViewerDataService.QueryStoreRegressionsSql))]
    public void QueryStoreAggregates_DedupToTheLatestSnapshotPerInterval_BeforeAggregating(string sqlName)
    {
        var sql = SqlByName(sqlName);

        /* The interval identity is the load-bearing part of the key: without it the dedup would collapse
           SEPARATE intervals of the same query+plan and under-count instead of double-count. Both the real
           id and the legacy proxy are present, and NEITHER may be dropped — the id is NULL on every
           pre-tier-2 row, the proxy is NULL on rows Query Store never attributed a first execution to. */
        Assert.Contains(
            "PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc",
            sql, StringComparison.Ordinal);
        /* "Latest" is decided by collection_time, never by execution_count — an interval can be
           re-collected many times without its count ever moving (the 496x shape). */
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("AS rn", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE rn = 1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStoreTopSql_KeepsReplicaRoleInTheDedupKey_BecauseItGroupsOnIt()
    {
        /* The dedup key must be at least as fine as the read's own row identity. This grid GROUPs BY
           replica_role, so a dedup without it could drop a replica's row entirely rather than
           de-duplicate a re-collection — turning a double-count into a silent under-count. */
        Assert.Contains(
            "PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role",
            ViewerDataService.QueryStoreTopSql, StringComparison.Ordinal);
    }

    // ── Comparisons ──

    [Theory]
    [InlineData(nameof(ViewerDataService.QueryStatsComparisonSql), "query_stats", "GROUP BY th.database_name, th.query_hash")]
    [InlineData(nameof(ViewerDataService.QueryStoreComparisonSql), "query_store_stats", "GROUP BY th.database_name, th.query_hash")]
    [InlineData(nameof(ViewerDataService.ProcedureStatsComparisonSql), "procedure_stats", "GROUP BY tp.database_name, tp.schema_name, tp.object_name")]
    public void ComparisonSql_UnionsTop100_FullOuterJoins_NullSafe(string sqlName, string table, string finalGroupBy)
    {
        var sql = SqlByName(sqlName);
        Assert.Contains($"FROM {table}", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY SUM(", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 100", sql, StringComparison.Ordinal);
        Assert.Contains("UNION ALL", sql, StringComparison.Ordinal);
        Assert.Contains("FULL OUTER JOIN baseline_period b", sql, StringComparison.Ordinal);
        /* The CTE INNER JOINs keep Lite's null-safe IS NOT DISTINCT FROM (legal in a PG inner join)... */
        Assert.Contains("IS NOT DISTINCT FROM", sql, StringComparison.Ordinal);
        /* ...but the FULL JOIN must be COALESCE-equality — PG can't FULL-JOIN on IS NOT DISTINCT FROM. */
        Assert.Contains("COALESCE(c.database_name, '') = COALESCE(b.database_name, '')", sql, StringComparison.Ordinal);
        Assert.Contains(finalGroupBy, sql, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStoreComparisonSql_UsesExecutionCountWeightedAverages()
    {
        /* Query Store rows are per-interval averages; the comparison weights each by execution_count. */
        var sql = ViewerDataService.QueryStoreComparisonSql;
        Assert.Contains("SUM(qs.execution_count * qs.avg_duration_us::double precision) / NULLIF(SUM(qs.execution_count), 0)", sql, StringComparison.Ordinal);
    }

    // ── Slicers ──

    /// <param name="bucketExpression">
    /// What the bars are keyed on. The delta-based slicers bucket on <c>collection_time</c>, because a
    /// per-cycle delta IS the work done in that cycle. Query Store's rows are per-interval snapshots, so
    /// since #1841 tier 2 it buckets on the interval's own start when there is one — an interval was
    /// otherwise drawn in the hour it was last COLLECTED, reliably one bar late on Query Store's default
    /// 60-minute interval. COALESCE, not a bare column, so pre-tier-2 rows keep collection_time.
    /// </param>
    /// <param name="windowFilter">
    /// The predicate the window is applied through — which must be the SAME expression as
    /// <paramref name="bucketExpression"/>, and is now a per-slicer fact rather than a shared assertion
    /// (#1892). Filtering on one instant while bucketing on another puts rows in bars outside the range the
    /// caller asked for; the delta slicers never had the problem because for them the two are one column.
    /// </param>
    [Theory]
    [InlineData(nameof(ViewerDataService.QueryStatsSlicerSql), "query_stats", "COUNT(DISTINCT query_hash)", "date_trunc('hour', collection_time)", "collection_time")]
    [InlineData(nameof(ViewerDataService.ProcStatsSlicerSql), "procedure_stats", "COUNT(DISTINCT object_name)", "date_trunc('hour', collection_time)", "collection_time")]
    [InlineData(nameof(ViewerDataService.QueryStoreSlicerSql), "query_store_stats", "COUNT(DISTINCT query_id)", "date_trunc('hour', COALESCE(interval_start_time_utc, collection_time))", "COALESCE(interval_start_time_utc, collection_time)")]
    public void SlicerSql_BucketsByHour_SevenColumnShape(string sqlName, string table, string distinctCount, string bucketExpression, string windowFilter)
    {
        var sql = SqlByName(sqlName);
        Assert.Contains(bucketExpression, sql, StringComparison.Ordinal);
        Assert.Contains($"FROM {table}", sql, StringComparison.Ordinal);
        Assert.Contains(distinctCount, sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains($"{windowFilter} >= $2", sql, StringComparison.Ordinal);
        Assert.Contains($"{windowFilter} <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY bucket", sql, StringComparison.Ordinal);

        /* The bucket key and the window filter agree, stated as the invariant rather than left implicit in
           two InlineData columns that a future edit could change one of. */
        Assert.Contains(windowFilter, bucketExpression, StringComparison.Ordinal);
    }

    /// <summary>
    /// The Query Store slicer keeps collection_time bounds even though it no longer WINDOWS on that column
    /// (#1892). query_store_stats is a hypertable partitioned on collection_time, so without them TimescaleDB
    /// cannot exclude chunks and an old fixed date range decompresses everything from the window through the
    /// present.
    ///
    /// <para>Both are pinned in their SLACKENED form, which is the whole point: a bare
    /// <c>collection_time &gt;= $2</c> / <c>&lt;= $3</c> pair would silently be the window filter again and
    /// re-introduce exactly the edge bug this fixed. The floor is provably implied by the COALESCE
    /// predicate; the ceiling is a month, being Query Store's 1-day maximum interval plus 29 days of
    /// collector-outage allowance.</para>
    /// </summary>
    [Fact]
    public void QueryStoreSlicerSql_KeepsSlackenedCollectionTimeBoundsForChunkExclusion()
    {
        var sql = SqlByName(nameof(ViewerDataService.QueryStoreSlicerSql));

        Assert.Contains("collection_time >= $2 - interval '1 day'", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3 + interval '30 days'", sql, StringComparison.Ordinal);

        /* The tight forms, asserted absent: either one would be a window filter wearing a pruning bound's
           clothes, and the failure would look exactly like the bug #1892 fixed. */
        Assert.DoesNotContain("collection_time >= $2\n", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("collection_time <= $3\n", sql, StringComparison.Ordinal);
    }

    // ── PG dialect (all Queries reads) ──

    [Theory]
    [InlineData(nameof(ViewerDataService.TopQueriesSql))]
    [InlineData(nameof(ViewerDataService.TopProceduresSql))]
    [InlineData(nameof(ViewerDataService.QueryStoreTopSql))]
    [InlineData(nameof(ViewerDataService.QueryStoreRegressionsSql))]
    [InlineData(nameof(ViewerDataService.QueryStatsComparisonSql))]
    [InlineData(nameof(ViewerDataService.ProcedureStatsComparisonSql))]
    [InlineData(nameof(ViewerDataService.QueryStoreComparisonSql))]
    [InlineData(nameof(ViewerDataService.QueryStatsSlicerSql))]
    [InlineData(nameof(ViewerDataService.ProcStatsSlicerSql))]
    [InlineData(nameof(ViewerDataService.QueryStoreSlicerSql))]
    public void QueriesReads_ArePostgresDialect_PositionalParams_NoTsqlIsms(string sqlName)
    {
        var sql = SqlByName(sqlName);
        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.DoesNotContain("getdate", sql.ToLowerInvariant());
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
    }

    private static string SqlByName(string name) => name switch
    {
        nameof(ViewerDataService.TopQueriesSql) => ViewerDataService.TopQueriesSql,
        nameof(ViewerDataService.TopProceduresSql) => ViewerDataService.TopProceduresSql,
        nameof(ViewerDataService.QueryStoreTopSql) => ViewerDataService.QueryStoreTopSql,
        nameof(ViewerDataService.QueryStoreRegressionsSql) => ViewerDataService.QueryStoreRegressionsSql,
        nameof(ViewerDataService.QueryStatsComparisonSql) => ViewerDataService.QueryStatsComparisonSql,
        nameof(ViewerDataService.ProcedureStatsComparisonSql) => ViewerDataService.ProcedureStatsComparisonSql,
        nameof(ViewerDataService.QueryStoreComparisonSql) => ViewerDataService.QueryStoreComparisonSql,
        nameof(ViewerDataService.QueryStatsSlicerSql) => ViewerDataService.QueryStatsSlicerSql,
        nameof(ViewerDataService.ProcStatsSlicerSql) => ViewerDataService.ProcStatsSlicerSql,
        _ => ViewerDataService.QueryStoreSlicerSql,
    };
}

/// <summary>The Queries row models' pure display helpers: raw server-clock formatting, the Query Store
/// row's stored-UTC execution stamps, FullName, totals.</summary>
public sealed class ViewerQueriesDisplayTests
{
    /// <summary>
    /// <c>query_stats</c>' and <c>procedure_stats</c>' execution and cache stamps are the SQL server's
    /// own wall clock, shown raw — NOT run through the naive-UTC conversion the <c>collection_time</c>
    /// columns get. <c>query_store_stats</c>' stamps are not in this family: the collector normalises
    /// them to UTC, so they take <c>ViewerTimeHelper.ForDisplay</c> instead (#3207).
    ///
    /// <para>Only the NULL contract is asserted here, and deliberately: both renderers honour the display
    /// mode, so a pinned rendered value depends on the process-wide
    /// <c>ViewerTimeHelper.CurrentDisplayMode</c>, which this class does not serialize — xUnit runs classes
    /// in parallel and three others flip it. The values are asserted in
    /// <c>ViewerTimeHelperTests.FormatServerClock_HonoursTheDisplayMode_AtTheFleetOffset</c>, which is in
    /// the <c>viewer-time-statics</c> collection. Empty-for-null is the same in every mode.</para>
    /// </summary>
    [Fact]
    public void TheTwoClockRenderers_RenderEmptyForNull()
    {
        Assert.Equal("", ViewerDataService.FormatServerClock(null));
        Assert.Equal("", ViewerDataService.FormatStoredUtc(null));
    }

    [Fact]
    public void QueryStatsRow_ConvertsMicrosecondsToMs_AndAveragesByExecutions()
    {
        var row = new ViewerQueryStatsRow { TotalExecutions = 4, TotalCpuUs = 8000, TotalElapsedUs = 20000, TotalLogicalReads = 400 };
        Assert.Equal(8.0, row.TotalCpuMs);
        Assert.Equal(20.0, row.TotalElapsedMs);
        Assert.Equal(2.0, row.AvgCpuMs);
        Assert.Equal(5.0, row.AvgElapsedMs);
        Assert.Equal(100.0, row.AvgReads);
    }

    [Fact]
    public void ProcedureStatsRow_FullName_ComposesSchemaAndObject()
    {
        Assert.Equal("dbo.usp_Get", new ViewerProcedureStatsRow { SchemaName = "dbo", ObjectName = "usp_Get" }.FullName);
        Assert.Equal("usp_Get", new ViewerProcedureStatsRow { SchemaName = "", ObjectName = "usp_Get" }.FullName);
    }

    [Fact]
    public void QueryStatsRow_ModuleName_ComposesDbSchemaObject_OrAdHocWhenUnmatched()
    {
        /* #1568: attributed statement shows database.schema.object; an unmatched statement is ad hoc. */
        var attributed = new ViewerQueryStatsRow { ModuleDatabaseName = "StackOverflow", ModuleSchemaName = "dbo", ModuleObjectName = "usp_Get" };
        Assert.Equal("StackOverflow.dbo.usp_Get", attributed.ModuleName);
        Assert.Equal("ad hoc", new ViewerQueryStatsRow().ModuleName);
        Assert.Equal("ad hoc", new ViewerQueryStatsRow { ModuleDatabaseName = "StackOverflow", ModuleSchemaName = "dbo" }.ModuleName);
    }

    [Fact]
    public void QueryStatsRow_ModuleName_PrefersCollectionTimeHostObject_OverHandleStitch()
    {
        /* #2012 stage 2: host_object_name is resolved ON the monitored server at collection, so it
           wins over the #1568 sql_handle stitch (which requires the module to also be in the
           procedure-stats cache); pre-upgrade rows have a NULL host and keep the stitch. */
        var both = new ViewerQueryStatsRow
        {
            DatabaseName = "StackOverflow",
            HostObjectName = "dbo.usp_Host",
            ModuleDatabaseName = "OtherDb",
            ModuleSchemaName = "dbo",
            ModuleObjectName = "usp_Stitched",
        };
        Assert.Equal("StackOverflow.dbo.usp_Host", both.ModuleName);

        var hostOnly = new ViewerQueryStatsRow { DatabaseName = "StackOverflow", HostObjectName = "dbo.usp_Host" };
        Assert.Equal("StackOverflow.dbo.usp_Host", hostOnly.ModuleName);
    }

    [Fact]
    public void QueryStoreRow_TotalsAreExecutionsTimesAverage()
    {
        var row = new ViewerQueryStoreRow { TotalExecutions = 10, AvgDurationMs = 3.5, AvgCpuTimeMs = 1.25 };
        Assert.Equal(35.0, row.TotalDurationMs);
        Assert.Equal(12.5, row.TotalCpuMs);
    }

    [Fact]
    public void QueryStoreRow_ExecutionTimesLocal_ConvertStoredNaiveUtc_EmptyForNull()
    {
        /* query_store_stats' execution stamps are naive UTC in the store (QueryStoreCollector normalises
           Query Store's datetimeoffset), so the row renders both through the stored-UTC conversion (#3207),
           not the raw server-clock family the dm_exec_* stamps use. Expected is derived through the same
           ForDisplay the renderer uses — a pinned literal only holds where the conversion is the identity
           (a UTC machine). Distinct inputs, so First wired to Last (or the reverse) fails. */
        var firstUtc = new DateTime(2026, 7, 1, 9, 30, 15, DateTimeKind.Unspecified);
        var lastUtc = new DateTime(2026, 7, 2, 21, 5, 59, DateTimeKind.Unspecified);
        var row = new ViewerQueryStoreRow { FirstExecutionTime = firstUtc, LastExecutionTime = lastUtc };
        Assert.Equal(ViewerTimeHelper.ForDisplay(firstUtc).ToString("yyyy-MM-dd HH:mm:ss"), row.FirstExecutionTimeLocal);
        Assert.Equal(ViewerTimeHelper.ForDisplay(lastUtc).ToString("yyyy-MM-dd HH:mm:ss"), row.LastExecutionTimeLocal);
        Assert.Equal("", new ViewerQueryStoreRow().FirstExecutionTimeLocal);
        Assert.Equal("", new ViewerQueryStoreRow().LastExecutionTimeLocal);
    }

    [Fact]
    public void ComparisonItems_AreTheSharedUiTypes()
    {
        /* The three comparison reads return the shared .Ui derivatives the collapsed grids bind. */
        Assert.IsAssignableFrom<ComparisonItemBase>(new QueryStatsComparisonItem());
        Assert.IsAssignableFrom<ComparisonItemBase>(new ProcedureStatsComparisonItem());
        Assert.Equal("NEW", new QueryStatsComparisonItem { ExecutionCount = 5, BaselineExecutionCount = 0 }.StatusBadge);
        Assert.Equal("GONE", new ProcedureStatsComparisonItem { ExecutionCount = 0, BaselineExecutionCount = 5 }.StatusBadge);
    }
}

/// <summary>
/// Pins the Query Store Regressions read (the Dashboard's <c>report.query_store_regressions</c> TVF ported
/// to Postgres): the baseline-before-window vs. recent-in-window split ON <c>collection_time</c> (not the
/// TVF's <c>server_last_execution_time</c>), the CPU-regression &gt; 25% gate, the added-duration ranking +
/// TOP (50) cap, the duration-driven severity bands, the summed/counted CASTs, and the #1319 database
/// filter — plus the row model's raw-server-clock display. String + pure-logic pins only (no live Postgres).
/// </summary>
public sealed class ViewerQueryStoreRegressionsTests
{
    [Fact]
    public void RegressionsSql_SplitsBaselineBeforeWindow_RecentInWindow_OverTheBaseTable()
    {
        var sql = ViewerDataService.QueryStoreRegressionsSql;
        Assert.Contains("FROM query_store_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_query_store_stats", sql, StringComparison.Ordinal); /* viewer reads base tables */
        Assert.Contains("collection_time < $2", sql, StringComparison.Ordinal);   /* baseline: everything before the window */
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);  /* recent: window start */
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);  /* recent: window end */
        Assert.Contains("GROUP BY database_name, query_id", sql, StringComparison.Ordinal);
        /* Darling windows the split on collection_time — the Dashboard TVF's server_last_execution_time is
           the server's LOCAL wall clock in Darling's store and must not be windowed against UTC bounds. */
        Assert.DoesNotContain("server_last_execution_time", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void RegressionsSql_GatesOnCpuRegressionOver25_RanksByAddedDuration_CapsAt50_InnerJoinsBaseline()
    {
        var sql = ViewerDataService.QueryStoreRegressionsSql;
        /* The TVF's single-metric gate: CPU regression > 25% (recent vs baseline), NULLIF-guarded. */
        Assert.Contains("(r.avg_cpu_time_ms - b.avg_cpu_time_ms) * 100.0 / NULLIF(b.avg_cpu_time_ms, 0) > 25", sql, StringComparison.Ordinal);
        /* Extra total time = per-exec duration delta × recent exec count, and the read's ranking. */
        Assert.Contains("(r.avg_duration_ms - b.avg_duration_ms) * r.exec_count AS additional_duration_ms", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY additional_duration_ms DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 50", sql, StringComparison.Ordinal);
        /* INNER JOIN — a query with no baseline (NEW) can't regress, exactly like the Dashboard TVF. */
        Assert.Contains("JOIN baseline_performance", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void RegressionsSql_DurationDrivenSeverityBands_MatchTheDashboardThresholds()
    {
        var sql = ViewerDataService.QueryStoreRegressionsSql;
        Assert.Contains("> 100 THEN 'CRITICAL'", sql, StringComparison.Ordinal);
        Assert.Contains("> 50 THEN 'HIGH'", sql, StringComparison.Ordinal);
        Assert.Contains("> 25 THEN 'MEDIUM'", sql, StringComparison.Ordinal);
        Assert.Contains("ELSE 'LOW'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void RegressionsSql_CastsSummedExecCountToBigint_PlanCountToInteger_ConvertsUnits_HonorsDatabaseFilter()
    {
        var sql = ViewerDataService.QueryStoreRegressionsSql;
        Assert.Contains("CAST(SUM(execution_count) AS bigint)", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(COUNT(DISTINCT plan_id) AS integer)", sql, StringComparison.Ordinal);
        /* µs → ms on duration + CPU; reads stay raw pages (matching the Dashboard TVF units). */
        Assert.Contains("AVG(CAST(avg_duration_us AS double precision)) / 1000.0", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(CAST(avg_cpu_time_us AS double precision)) / 1000.0", sql, StringComparison.Ordinal);
        /* #1319 global database filter, the same guarded ANY() idiom as the sibling reads ($4 here). */
        Assert.Contains("$4::text[] IS NULL OR database_name = ANY($4)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void RegressionRow_LastExecutionLocal_ShowsRawServerClock_EmptyForNull()
    {
        var row = new ViewerQueryStoreRegressionRow { LastExecutionTime = new DateTime(2026, 7, 1, 9, 30, 15) };
        Assert.Equal("2026-07-01 09:30:15", row.LastExecutionTimeLocal);
        Assert.Equal("", new ViewerQueryStoreRegressionRow { LastExecutionTime = null }.LastExecutionTimeLocal);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the three Queries reads + a comparison + a slicer. Each
/// plants query/procedure/query-store rows for a negative sentinel server across two collections, then
/// asserts the read executes on real Postgres and returns the expected grouping / ordering / unit
/// conversion / WAITFOR trim end-to-end (the string pins can't catch a dialect error or an ordinal
/// slip). Shares the serialized "live-postgres" collection and cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerQueriesLivePostgresTests
{
    private const int QueryStatsServerId = -970801;
    private const int ProcedureStatsServerId = -970802;
    private const int QueryStoreServerId = -970803;
    private const int ComparisonServerId = -970804;
    private const int SlicerServerId = -970805;
    private const int ProcedureStatsPlanServerId = -970806;
    private const int RegressionsServerId = -970807;
    private const int ModuleAttributionServerId = -970808;
    private const int DedupServerId = -970809;
    private const int IntervalIdentityServerId = -970810;
    private const int WindowEdgeServerId = -970811;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TopQueries_GroupsSums_ExcludesWaitfor_OrdersByTotalDuration_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live Top Queries test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "query_stats", QueryStatsServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(cs!);
        var end = TruncateToSeconds(DateTime.UtcNow);
        var start = end.AddHours(-24);
        var t1 = start.AddHours(1);
        var t2 = start.AddHours(2);

        var bodySucceeded = false;
        try
        {
            /* "slow" query: two collections summed. total_elapsed = 300_000 us across the two rows. */
            await InsertQueryStatsAsync(connection, QueryStatsServerId, t1, "StackOverflow", "0xSLOW",
                deltaExec: 2, deltaWorker: 40_000, deltaElapsed: 120_000, deltaReads: 200, queryText: "SELECT slow");
            await InsertQueryStatsAsync(connection, QueryStatsServerId, t2, "StackOverflow", "0xSLOW",
                deltaExec: 3, deltaWorker: 60_000, deltaElapsed: 180_000, deltaReads: 300, queryText: "SELECT slow");
            /* "fast" query: smaller total elapsed → ranks second. */
            await InsertQueryStatsAsync(connection, QueryStatsServerId, t1, "StackOverflow", "0xFAST",
                deltaExec: 5, deltaWorker: 5_000, deltaElapsed: 10_000, deltaReads: 50, queryText: "SELECT fast");
            /* WAITFOR shell: must be trimmed. */
            await InsertQueryStatsAsync(connection, QueryStatsServerId, t1, "StackOverflow", "0xWAIT",
                deltaExec: 1, deltaWorker: 1, deltaElapsed: 999_999, deltaReads: 1, queryText: "WAITFOR DELAY '00:00:05'");

            var rows = await viewer.GetTopQueriesByCpuAsync(QueryStatsServerId, start, end);

            Assert.Equal(2, rows.Count); /* WAITFOR excluded despite its huge elapsed */
            Assert.DoesNotContain(rows, r => r.QueryHash == "0xWAIT");

            /* Ordered by total elapsed descending: slow first. */
            Assert.Equal("0xSLOW", rows[0].QueryHash);
            Assert.Equal(5, rows[0].TotalExecutions);           /* 2 + 3 summed across collections */
            Assert.Equal(300.0, rows[0].TotalElapsedMs);        /* (120000 + 180000) us -> ms */
            Assert.Equal(100.0, rows[0].TotalCpuMs);            /* (40000 + 60000) us -> ms */
            Assert.Equal("SELECT slow", rows[0].QueryText);
            Assert.Equal("0xFAST", rows[1].QueryHash);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "query_stats", QueryStatsServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task TopQueries_AttributesModuleByHandle_DedupsToLatest_AdHocWhenUnmatched_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live module attribution test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "query_stats", ModuleAttributionServerId, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "procedure_stats", ModuleAttributionServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(cs!);
        var end = TruncateToSeconds(DateTime.UtcNow);
        var start = end.AddHours(-24);
        var t1 = start.AddHours(1);

        var bodySucceeded = false;
        try
        {
            /* Attributed: query whose sql_handle matches a cached module; bigger elapsed → ranks first. */
            await InsertQueryStatsAsync(connection, ModuleAttributionServerId, t1, "StackOverflow", "0xATTRIB",
                deltaExec: 5, deltaWorker: 60_000, deltaElapsed: 300_000, deltaReads: 300, queryText: "SELECT attributed", sqlHandle: "0xMOD");
            /* Same handle captured twice — the newer identity (usp_New) wins, and the query row must not fan
               out across the two procedure_stats rows. */
            await InsertProcedureStatsAsync(connection, ModuleAttributionServerId, t1, "StackOverflow", "dbo", "usp_Old", "PROCEDURE",
                deltaExec: 1, deltaWorker: 10_000, deltaElapsed: 20_000, deltaReads: 100, sqlHandle: "0xMOD");
            await InsertProcedureStatsAsync(connection, ModuleAttributionServerId, t1.AddMinutes(30), "StackOverflow", "dbo", "usp_New", "PROCEDURE",
                deltaExec: 1, deltaWorker: 10_000, deltaElapsed: 20_000, deltaReads: 100, sqlHandle: "0xMOD");
            /* Ad hoc: query whose sql_handle matches no cached module. */
            await InsertQueryStatsAsync(connection, ModuleAttributionServerId, t1, "StackOverflow", "0xADHOC",
                deltaExec: 3, deltaWorker: 20_000, deltaElapsed: 100_000, deltaReads: 100, queryText: "SELECT adhoc", sqlHandle: "0xLOOSE");

            var rows = await viewer.GetTopQueriesByCpuAsync(ModuleAttributionServerId, start, end);

            Assert.Equal(2, rows.Count);           /* the two module rows did not fan out the one query */
            var attributed = rows.Single(r => r.QueryHash == "0xATTRIB");
            Assert.Equal("StackOverflow.dbo.usp_New", attributed.ModuleName);   /* latest collection_time wins */
            Assert.Equal("ad hoc", rows.Single(r => r.QueryHash == "0xADHOC").ModuleName);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, "query_stats", ModuleAttributionServerId, cleanupCt);
                await DeleteRowsAsync(cleanup, "procedure_stats", ModuleAttributionServerId, cleanupCt);
            });
        }
    }

    [Fact]
    public async Task TopProcedures_GroupsByObject_ComposesFullName_OrdersByTotalDuration_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live Top Procedures test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "procedure_stats", ProcedureStatsServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(cs!);
        var end = TruncateToSeconds(DateTime.UtcNow);
        var start = end.AddHours(-24);

        var bodySucceeded = false;
        try
        {
            await InsertProcedureStatsAsync(connection, ProcedureStatsServerId, start.AddHours(1), "StackOverflow", "dbo", "usp_Slow", "PROC",
                deltaExec: 4, deltaWorker: 40_000, deltaElapsed: 200_000, deltaReads: 400);
            await InsertProcedureStatsAsync(connection, ProcedureStatsServerId, start.AddHours(1), "StackOverflow", "dbo", "usp_Fast", "PROC",
                deltaExec: 10, deltaWorker: 5_000, deltaElapsed: 20_000, deltaReads: 100);

            var rows = await viewer.GetTopProceduresByCpuAsync(ProcedureStatsServerId, start, end);

            Assert.Equal(2, rows.Count);
            Assert.Equal("dbo.usp_Slow", rows[0].FullName);
            Assert.Equal(4, rows[0].TotalExecutions);
            Assert.Equal(200.0, rows[0].TotalElapsedMs);
            Assert.Equal(50.0, rows[0].AvgElapsedMs);           /* 200000 us / 4 execs -> ms */
            Assert.Equal("dbo.usp_Fast", rows[1].FullName);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "procedure_stats", ProcedureStatsServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task ProcedureStatsPlan_FetchesNewestStoredXml_KeyedByObject_NullWhenUncaptured_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live procedure-plan test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "procedure_stats", ProcedureStatsPlanServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(cs!);
        var t = TruncateToSeconds(DateTime.UtcNow).AddHours(-2);

        var bodySucceeded = false;
        try
        {
            /* Same (db, schema, object) captured twice — the newer plan wins (ORDER BY collection_time DESC). */
            await InsertProcedureStatsAsync(connection, ProcedureStatsPlanServerId, t, "StackOverflow", "dbo", "usp_WithPlan", "PROC",
                deltaExec: 1, deltaWorker: 10_000, deltaElapsed: 20_000, deltaReads: 100, planXml: "<ShowPlanXML>older</ShowPlanXML>");
            await InsertProcedureStatsAsync(connection, ProcedureStatsPlanServerId, t.AddMinutes(30), "StackOverflow", "dbo", "usp_WithPlan", "PROC",
                deltaExec: 1, deltaWorker: 10_000, deltaElapsed: 20_000, deltaReads: 100, planXml: "<ShowPlanXML>newer</ShowPlanXML>");
            /* A procedure with activity but no captured plan (query_plan_xml NULL). */
            await InsertProcedureStatsAsync(connection, ProcedureStatsPlanServerId, t, "StackOverflow", "dbo", "usp_NoPlan", "PROC",
                deltaExec: 1, deltaWorker: 10_000, deltaElapsed: 20_000, deltaReads: 100);

            Assert.Equal("<ShowPlanXML>newer</ShowPlanXML>",
                await viewer.GetProcedureStatsPlanXmlAsync(ProcedureStatsPlanServerId, "StackOverflow", "dbo", "usp_WithPlan"));
            /* No plan captured, and an absent object, both fetch null. */
            Assert.Null(await viewer.GetProcedureStatsPlanXmlAsync(ProcedureStatsPlanServerId, "StackOverflow", "dbo", "usp_NoPlan"));
            Assert.Null(await viewer.GetProcedureStatsPlanXmlAsync(ProcedureStatsPlanServerId, "StackOverflow", "dbo", "usp_Absent"));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "procedure_stats", ProcedureStatsPlanServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task QueryStore_AveragesIntervals_ForcedPlanBoolean_MemoryToMb_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "query_store_stats", QueryStoreServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(cs!);
        var end = TruncateToSeconds(DateTime.UtcNow);
        var start = end.AddHours(-24);

        var bodySucceeded = false;
        try
        {
            /* One query, one plan, two intervals averaged: avg_duration_us 2000 & 4000 -> AVG 3000us = 3ms. */
            await InsertQueryStoreAsync(connection, QueryStoreServerId, start.AddHours(1), "StackOverflow", queryId: 42, planId: 7,
                execCount: 3, avgDurationUs: 2000, avgCpuUs: 1000, forced: true, maxMemPages: 1024, queryText: "SELECT qs");
            await InsertQueryStoreAsync(connection, QueryStoreServerId, start.AddHours(2), "StackOverflow", queryId: 42, planId: 7,
                execCount: 3, avgDurationUs: 4000, avgCpuUs: 2000, forced: true, maxMemPages: 1024, queryText: "SELECT qs");

            var rows = await viewer.GetQueryStoreTopQueriesAsync(QueryStoreServerId, start, end);

            var r = Assert.Single(rows);
            Assert.Equal(42, r.QueryId);
            Assert.Equal(7, r.PlanId);
            Assert.Equal(6, r.TotalExecutions);            /* 3 + 3 summed */
            Assert.Equal(3.0, r.AvgDurationMs, 3);         /* AVG(2000, 4000) us -> ms */
            Assert.True(r.IsForcedPlan);
            Assert.Equal(8.0, r.AvgMemoryMb, 3);           /* 1024 pages * 8 KB / 1024 -> 8 MB */
            Assert.Equal("SELECT qs", r.QueryText);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "query_store_stats", QueryStoreServerId, cleanupCt));
        }
    }

    /// <summary>
    /// #1841 end-to-end on real Postgres: ONE runtime-stats interval re-collected across several cycles
    /// must be counted ONCE, at its latest cumulative values, by every Query Store aggregate read.
    ///
    /// <para>Seeds both live shapes from issue #1841 inside a single hour: interval A collected four times
    /// with a FLAT execution_count of 1 (the 496x shape — one interval, many collections, no growth), and
    /// interval B collected three times with a GROWING cumulative count (10 → 25 → 40). The expected
    /// numbers are the true totals, so an un-deduped read fails loudly rather than drifting.</para>
    ///
    /// <para>Deliberately exercises the four reads that had no live coverage at all (slicer, comparison,
    /// duration trend, item timeline) alongside the top-queries grid: the string pins cannot catch a
    /// column the dedup CTE forgot to project, which only shows up when Postgres actually runs it.</para>
    /// </summary>
    [Fact]
    public async Task QueryStoreAggregates_CountARecollectedIntervalOnce_AtItsLatestValues_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store dedup test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "query_store_stats", DedupServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(cs!);

        /* One hour bucket, so the slicer assertion is about dedup rather than bucket boundaries. */
        var bucketStart = TruncateToSeconds(DateTime.UtcNow).AddHours(-3);
        bucketStart = bucketStart.AddMinutes(-bucketStart.Minute).AddSeconds(-bucketStart.Second);
        var firstExecA = bucketStart.AddMinutes(1);
        var firstExecB = bucketStart.AddMinutes(2);
        var start = bucketStart.AddMinutes(-1);
        var end = bucketStart.AddMinutes(59);

        var bodySucceeded = false;
        try
        {
            /* Interval A: execution_count never moves off 1, collected four times. True work: 1 execution
               at 1,000us CPU / 2,000us duration. Un-deduped a read reports 4x that. */
            foreach (var minute in new[] { 5, 10, 15, 20 })
            {
                await InsertQueryStoreAsync(connection, DedupServerId, bucketStart.AddMinutes(minute), "DedupDb",
                    queryId: 1, planId: 11, execCount: 1, avgDurationUs: 2000, avgCpuUs: 1000, forced: false,
                    maxMemPages: 0, queryText: "SELECT flat", firstExecutionTimeUtc: firstExecA);
            }

            /* Interval B: the cumulative shape — count and averages both grow as the interval accumulates.
               True work is the LAST snapshot only: 40 executions at 300us CPU / 7,000us duration.
               Un-deduped: 10x100 + 25x200 + 40x300 = 18,000us CPU against a true 12,000us. */
            var growth = new (int Minute, long Execs, long Cpu, long Dur)[]
            {
                (5, 10L, 100L, 5_000L),
                (10, 25L, 200L, 6_000L),
                (15, 40L, 300L, 7_000L),
            };
            foreach (var g in growth)
            {
                await InsertQueryStoreAsync(connection, DedupServerId, bucketStart.AddMinutes(g.Minute), "DedupDb",
                    queryId: 2, planId: 22, execCount: g.Execs, avgDurationUs: g.Dur, avgCpuUs: g.Cpu, forced: false,
                    maxMemPages: 0, queryText: "SELECT growing", firstExecutionTimeUtc: firstExecB);
            }

            /* ── the top-queries grid ── */
            var top = await viewer.GetQueryStoreTopQueriesAsync(DedupServerId, start, end);
            var a = Assert.Single(top, r => r.QueryId == 1);
            var b = Assert.Single(top, r => r.QueryId == 2);
            Assert.Equal(1, a.TotalExecutions);            /* four collections of a 1-execution interval is ONE */
            Assert.Equal(40, b.TotalExecutions);           /* 10 → 25 → 40 reached 40, it did not run 75 times */
            Assert.Equal(2.0, a.AvgDurationMs, 3);
            Assert.Equal(7.0, b.AvgDurationMs, 3);         /* the latest snapshot, not AVG(5000,6000,7000) */

            /* ── the slicer bars ── CPU (1x1000 + 40x300)/1000 = 13 ms; un-deduped 22 ms.
                  Duration (1x2000 + 40x7000)/1000 = 282 ms; un-deduped 488 ms. */
            var buckets = await viewer.GetQueryStoreSlicerDataAsync(DedupServerId, start, end);
            var bucket = Assert.Single(buckets);
            Assert.Equal(2, bucket.SessionCount);           /* COUNT(DISTINCT query_id), guards the seed */
            Assert.Equal(13.0, bucket.TotalCpu, 3);
            Assert.Equal(282.0, bucket.TotalElapsed, 3);

            /* ── the comparison ── this read groups by (database, query_hash), which is COARSER than the
                  interval grain, and the seed gives both queries the same hash — so it is also the pin that
                  dedup happens at the INTERVAL grain FIRST and only then re-aggregates up to the hash.
                  One row: 1 + 40 = 41 executions (un-deduped: 4 + 75 = 79), and the execution-weighted mean
                  duration is (1x2000 + 40x7000) / 41 = 6.878 ms.

                  Both arms cover the same window, so a correct read reports identical current and baseline
                  numbers; any dedup asymmetry between the arms would surface here as a false delta. */
            var comparison = await viewer.GetQueryStoreComparisonAsync(DedupServerId, start, end, start, end);
            var cb = Assert.Single(comparison);
            Assert.Equal(41, cb.ExecutionCount);
            Assert.Equal(41, cb.BaselineExecutionCount);
            Assert.Equal(6.878, cb.AvgDurationMs, 3);
            Assert.Equal(cb.ExecutionCount, cb.BaselineExecutionCount);
            Assert.Equal(0.317, cb.AvgCpuMs, 3);           /* (1x1000 + 40x300) / 41 us -> ms */

            /* ── the duration trend, LEGACY arm ── this whole seed is pre-tier-2 (no interval identity),
                  so it exercises the fallback: un-deduped, one point per COLLECTION, still overstating.
                  That is deliberate and it is what a store holding pre-upgrade history must keep doing —
                  nothing can reconstruct an interval start for these rows. Four points, exactly as before
                  tier 2. The corrected arm has its own live test below. */
            var trend = await viewer.GetQueryStoreDurationTrendAsync(DedupServerId, start, end);
            Assert.Equal(4, trend.Count);

            /* ── the slicer overlay ── one point for the one interval, at its final values, so the overlay
                  agrees with the deduped bars it is drawn over instead of showing a rising staircase. */
            var timeline = await viewer.GetQueryStoreItemTimelineAsync(DedupServerId, "DedupDb", queryId: 2, planId: 22, start, end);
            var point = Assert.Single(timeline);
            Assert.Equal(bucketStart.AddMinutes(15), point.PointTime);
            Assert.Equal(280.0, point.ElapsedMs, 3);       /* 40 x 7,000us; un-deduped this is 3 points, 50/150/280 */
            Assert.Equal(12.0, point.CpuMs, 3);            /* 40 x 300us */

            /* ── the MCP / REST surface ── the same dedup, so an agent and the web dashboard see the grid's
                  numbers rather than the inflated ones. */
            await using var postgres = NpgsqlDataSource.Create(cs!);
            var mcp = await DarlingDataReader.GetQueryStoreTopAsync(
                postgres, DedupServerId, start, end, top: 10, databaseName: null, TestContext.Current.CancellationToken);
            Assert.Equal(40, Assert.Single(mcp, r => r.QueryId == 2).TotalExecutions);
            Assert.Equal(1, Assert.Single(mcp, r => r.QueryId == 1).TotalExecutions);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "query_store_stats", DedupServerId, cleanupCt));
        }
    }

    /// <summary>
    /// WATCHED (mutation): put the window filter back on collection_time and both halves go red.
    ///
    /// <para>#1892 against a real store. Once #1841 keyed the bars on the interval's start while the window
    /// was still filtered on collection_time, the two disagreed at BOTH edges, in opposite directions:</para>
    ///
    /// <list type="bullet">
    /// <item>an interval that STARTED before the range but whose closing fetch landed inside it passed the
    /// filter and drew a bar dated before the range began — a chart with a bar outside its own axis;</item>
    /// <item>the range's own final interval, still open when the range ended and therefore collected after
    /// it, failed the filter and vanished — the newest bar simply missing.</item>
    /// </list>
    ///
    /// <para>Live rather than a string pin because the string pins cannot tell which ROWS come back, and the
    /// left-edge case in particular reads as a perfectly ordinary bar until you notice its timestamp.</para>
    /// </summary>
    [Fact]
    public async Task QueryStoreWindowEdges_MatchTheBucketKey_NotTheCollectionClock_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store window-edge test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "query_store_stats", WindowEdgeServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(cs!);

        /* h1..h3 is the requested range. h0 sits an hour before it and h4 an hour after, so each edge case
           is a whole bucket outside the window rather than a near-miss. */
        var h0 = TruncateToHour(TruncateToSeconds(DateTime.UtcNow).AddHours(-8));
        var h1 = h0.AddHours(1);
        var h3 = h0.AddHours(3);
        var h4 = h0.AddHours(4);
        var start = h1;
        var end = h3.AddMinutes(59);

        var bodySucceeded = false;
        try
        {
            /* BEFORE the window, collected INSIDE it: the interval ran in h0 and its closing fetch landed in
               h2. Filtering on collection_time admits it, and it then buckets at h0 — left of `start`. */
            await InsertQueryStoreAsync(connection, WindowEdgeServerId, h0.AddHours(2).AddMinutes(5), "EdgeDb",
                queryId: 1, planId: 11, execCount: 3, avgDurationUs: 2_000, avgCpuUs: 1_000, forced: false,
                maxMemPages: 0, queryText: "SELECT before", firstExecutionTimeUtc: h0.AddMinutes(1),
                intervalId: 8801, intervalStartUtc: h0);

            /* INSIDE the window, collected AFTER it: the interval ran in h3 — the range's last hour — and is
               still open when the range ends, so its closing fetch lands in h4. Filtering on collection_time
               drops it and the newest bar disappears. */
            await InsertQueryStoreAsync(connection, WindowEdgeServerId, h4.AddMinutes(5), "EdgeDb",
                queryId: 2, planId: 22, execCount: 7, avgDurationUs: 3_000, avgCpuUs: 1_000, forced: false,
                maxMemPages: 0, queryText: "SELECT after", firstExecutionTimeUtc: h3.AddMinutes(1),
                intervalId: 8802, intervalStartUtc: h3);

            /* A plain interior interval, so the assertions below are about the edges rather than about the
               window returning anything at all. */
            await InsertQueryStoreAsync(connection, WindowEdgeServerId, h1.AddMinutes(50), "EdgeDb",
                queryId: 3, planId: 33, execCount: 5, avgDurationUs: 1_000, avgCpuUs: 1_000, forced: false,
                maxMemPages: 0, queryText: "SELECT inside", firstExecutionTimeUtc: h1.AddMinutes(1),
                intervalId: 8803, intervalStartUtc: h1);

            var buckets = await viewer.GetQueryStoreSlicerDataAsync(WindowEdgeServerId, start, end);

            /* Exactly the two intervals whose work RAN inside the range, and nothing dated outside it. */
            Assert.Equal(2, buckets.Count);
            Assert.DoesNotContain(buckets, b => b.BucketTime < start || b.BucketTime > end);
            Assert.DoesNotContain(buckets, b => b.BucketTime == h0);

            var interior = Assert.Single(buckets, b => b.BucketTime == h1);
            Assert.Equal(5.0, interior.TotalCpu, 3);        /* 5 x 1,000us */

            var lastBar = Assert.Single(buckets, b => b.BucketTime == h3);
            Assert.Equal(7.0, lastBar.TotalCpu, 3);         /* 7 x 1,000us — the bar that used to be missing */

            /* The duration trend carries the identical mismatch, and the two charts share a screen. */
            var trend = await viewer.GetQueryStoreDurationTrendAsync(WindowEdgeServerId, start, end);
            Assert.DoesNotContain(trend, p => p.CollectionTime < start || p.CollectionTime > end);
            Assert.Contains(trend, p => p.CollectionTime == h3);
            Assert.DoesNotContain(trend, p => p.CollectionTime == h0);

            bodySucceeded = true;
        }
        finally
        {
            /* #1902: cleanup gets its OWN connection, so a failure in the body cannot leave these rows
               behind for the next run to trip over. */
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "query_store_stats", WindowEdgeServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task QueryStoreIntervalIdentity_PlacesWorkWhenItRan_AndMixesWithLegacyRows_AgainstDevPostgres()
    {
        /* #1841 tier 2, against a REAL store — the half a string pin cannot reach. Three things at once:
           the slicer's one-bucket lag is gone, the duration trend places each interval's final total at
           the hour that work ran, and a window holding BOTH generations counts every row exactly once.

           The seed makes collection time and interval time genuinely disagree, which is the whole point:
           interval 1 RAN in hour 0 but every one of its collections landed in hour 1, so a
           collection_time bucket drew it one bar late. */
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store interval-identity test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "query_store_stats", IntervalIdentityServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(cs!);

        var h0 = TruncateToHour(TruncateToSeconds(DateTime.UtcNow).AddHours(-5));
        var h1 = h0.AddHours(1);
        var start = h0.AddMinutes(-1);
        var end = h0.AddHours(4);

        var bodySucceeded = false;
        try
        {
            /* Interval 1 — ran in hour 0, collected twice inside hour 1, growing 10 -> 40 cumulatively. */
            await InsertQueryStoreAsync(connection, IntervalIdentityServerId, h1.AddMinutes(5), "IdentityDb",
                queryId: 1, planId: 11, execCount: 10, avgDurationUs: 1_000, avgCpuUs: 100, forced: false,
                maxMemPages: 0, queryText: "SELECT ran_in_h0", firstExecutionTimeUtc: h0.AddMinutes(3),
                intervalId: 5001, intervalStartUtc: h0);
            await InsertQueryStoreAsync(connection, IntervalIdentityServerId, h1.AddMinutes(10), "IdentityDb",
                queryId: 1, planId: 11, execCount: 40, avgDurationUs: 7_000, avgCpuUs: 300, forced: false,
                maxMemPages: 0, queryText: "SELECT ran_in_h0", firstExecutionTimeUtc: h0.AddMinutes(3),
                intervalId: 5001, intervalStartUtc: h0);

            /* Interval 2 — ran in hour 1, collected once in hour 2. */
            await InsertQueryStoreAsync(connection, IntervalIdentityServerId, h1.AddHours(1).AddMinutes(5), "IdentityDb",
                queryId: 2, planId: 22, execCount: 5, avgDurationUs: 2_000, avgCpuUs: 200, forced: false,
                maxMemPages: 0, queryText: "SELECT ran_in_h1", firstExecutionTimeUtc: h1.AddMinutes(4),
                intervalId: 5002, intervalStartUtc: h1);

            /* ── the slicer ── two bars, at the hours the work RAN (h0 and h1), not the hours it was
                  collected (h1 and h2). Interval 1's bar carries its FINAL snapshot only: 40 x 300us =
                  12 ms of CPU, not 10x100 + 40x300 = 13 ms. */
            var buckets = await viewer.GetQueryStoreSlicerDataAsync(IntervalIdentityServerId, start, end);
            Assert.Equal(2, buckets.Count);
            Assert.Equal(h0, buckets[0].BucketTime);
            Assert.Equal(h1, buckets[1].BucketTime);
            Assert.Equal(12.0, buckets[0].TotalCpu, 3);
            Assert.Equal(1.0, buckets[1].TotalCpu, 3);     /* 5 x 200us */

            /* ── the duration trend ── one point per interval, at its start. The first has no predecessor
                  so its rate is 0 (the same convention the delta trends use); the second divides interval
                  2's true total (5 x 2,000us = 10 ms) by the 3,600s between interval starts. */
            var trend = await viewer.GetQueryStoreDurationTrendAsync(IntervalIdentityServerId, start, end);
            Assert.Equal(2, trend.Count);
            Assert.Equal(h0, trend[0].CollectionTime);
            Assert.Equal(h1, trend[1].CollectionTime);
            Assert.Equal(0d, trend[0].Value);
            Assert.Equal(10.0 / 3600.0, trend[1].Value, 9);

            /* ── the slicer OVERLAY (#1921, Erik's option 1) ── the point sits at the hour the work RAN, the
                  same h0 the bar above is drawn at, NOT at h1+10m where the collector observed it. That is
                  the whole decision: the overlay is drawn over those bars, and #1841 moved the bars while
                  leaving this series on collection_time, so a point sat up to one Query Store interval to the
                  right of the bar describing the very same work.

                  This is the BEHAVIORAL proof of the move, and it has to live here rather than on the older
                  dedup test: that test's rows are pre-tier-2 legacy rows with no interval start, so
                  COALESCE correctly falls back to collection_time there and the point does not move at all.
                  Its assertion is unchanged and now pins the FALLBACK — both halves of the two-generation
                  guarantee are covered, by the test whose data can actually show each.

                  Values are interval 1's FINAL snapshot, matching the bar: 40 x 7,000us = 280 ms elapsed and
                  40 x 300us = 12 ms CPU — the same 12.0 the h0 bar carries. */
            var overlay = await viewer.GetQueryStoreItemTimelineAsync(
                IntervalIdentityServerId, "IdentityDb", queryId: 1, planId: 11, start, end);
            var overlayPoint = Assert.Single(overlay);
            Assert.Equal(h0, overlayPoint.PointTime);
            Assert.NotEqual(h1.AddMinutes(10), overlayPoint.PointTime);
            Assert.Equal(280.0, overlayPoint.ElapsedMs, 3);
            Assert.Equal(12.0, overlayPoint.CpuMs, 3);
            Assert.Equal(buckets[0].TotalCpu, overlayPoint.CpuMs, 3);

            /* ── the mixed window ── add a LEGACY row (no identity) in hour 2 and nothing already counted
                  may move: the two identified bars keep their placement and values, and the legacy row
                  gets its own collection_time bar. The arms split on IS NULL / IS NOT NULL, so a row can
                  neither land in both nor fall between them. */
            await InsertQueryStoreAsync(connection, IntervalIdentityServerId, h1.AddHours(1).AddMinutes(30), "IdentityDb",
                queryId: 3, planId: 33, execCount: 7, avgDurationUs: 1_000, avgCpuUs: 1_000, forced: false,
                maxMemPages: 0, queryText: "SELECT legacy", firstExecutionTimeUtc: h1.AddHours(1).AddMinutes(20));

            var mixed = await viewer.GetQueryStoreSlicerDataAsync(IntervalIdentityServerId, start, end);
            Assert.Equal(3, mixed.Count);
            Assert.Equal(h0, mixed[0].BucketTime);
            Assert.Equal(h1, mixed[1].BucketTime);
            Assert.Equal(h1.AddHours(1), mixed[2].BucketTime);   /* legacy: placed at its COLLECTION hour */
            Assert.Equal(12.0, mixed[0].TotalCpu, 3);            /* unchanged by the legacy row */
            Assert.Equal(1.0, mixed[1].TotalCpu, 3);
            Assert.Equal(7.0, mixed[2].TotalCpu, 3);             /* 7 x 1,000us */

            /* And the top-queries grid still dedups interval 1 to its final total across both generations. */
            var top = await viewer.GetQueryStoreTopQueriesAsync(IntervalIdentityServerId, start, end);
            Assert.Equal(40, Assert.Single(top, r => r.QueryId == 1).TotalExecutions);
            Assert.Equal(7, Assert.Single(top, r => r.QueryId == 3).TotalExecutions);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "query_store_stats", IntervalIdentityServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task QueryStoreRegressions_ContrastsBaselineVsRecent_GatesOnCpu_RanksByAddedDuration_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store regressions test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "query_store_stats", RegressionsServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(cs!);
        var end = TruncateToSeconds(DateTime.UtcNow);
        var start = end.AddHours(-24);
        var baseline = start.AddHours(-2);   /* before the window → the baseline arm */
        var recent = start.AddHours(1);      /* inside the window → the recent arm */

        var bodySucceeded = false;
        try
        {
            /* Query 100 REGRESSED: baseline 2ms dur / 1ms cpu → recent 6ms dur / 4ms cpu.
               CPU regression = (4-1)/1 = 300% (clears the > 25% gate); duration regression = (6-2)/2 = 200%
               (CRITICAL band); additional = (6-2) * recent exec 5 = 20 ms. */
            await InsertQueryStoreAsync(connection, RegressionsServerId, baseline, "StackOverflow", queryId: 100, planId: 1,
                execCount: 4, avgDurationUs: 2000, avgCpuUs: 1000, forced: false, maxMemPages: 0, queryText: "SELECT regressed");
            await InsertQueryStoreAsync(connection, RegressionsServerId, recent, "StackOverflow", queryId: 100, planId: 2,
                execCount: 5, avgDurationUs: 6000, avgCpuUs: 4000, forced: false, maxMemPages: 0, queryText: "SELECT regressed");

            /* Query 200 STABLE: cpu 1ms → 1.1ms = 10% (below the gate) → excluded. */
            await InsertQueryStoreAsync(connection, RegressionsServerId, baseline, "StackOverflow", queryId: 200, planId: 1,
                execCount: 5, avgDurationUs: 3000, avgCpuUs: 1000, forced: false, maxMemPages: 0, queryText: "SELECT stable");
            await InsertQueryStoreAsync(connection, RegressionsServerId, recent, "StackOverflow", queryId: 200, planId: 1,
                execCount: 5, avgDurationUs: 3000, avgCpuUs: 1100, forced: false, maxMemPages: 0, queryText: "SELECT stable");

            /* Query 300 NEW (recent only, no baseline) → the INNER JOIN drops it. */
            await InsertQueryStoreAsync(connection, RegressionsServerId, recent, "StackOverflow", queryId: 300, planId: 1,
                execCount: 9, avgDurationUs: 9000, avgCpuUs: 9000, forced: false, maxMemPages: 0, queryText: "SELECT new");

            var rows = await viewer.GetQueryStoreRegressionsAsync(RegressionsServerId, start, end);

            var r = Assert.Single(rows);                    /* only query 100 clears the CPU gate + INNER JOIN */
            Assert.Equal(100, r.QueryId);
            Assert.Equal("CRITICAL", r.Severity);
            Assert.Equal(2.0, r.BaselineDurationMs, 3);
            Assert.Equal(6.0, r.RecentDurationMs, 3);
            Assert.Equal(200.0, r.DurationRegressionPercent, 1);
            Assert.Equal(300.0, r.CpuRegressionPercent, 1);
            Assert.Equal(20.0, r.AdditionalDurationMs, 1); /* (6-2)ms * 5 recent execs */
            Assert.Equal(4, r.BaselineExecCount);
            Assert.Equal(5, r.RecentExecCount);
            Assert.Equal("SELECT regressed", r.QueryTextSample);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "query_store_stats", RegressionsServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task QueryStatsComparison_FlagsNewAndGone_AcrossWindows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live comparison test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "query_stats", ComparisonServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(cs!);
        var currentEnd = TruncateToSeconds(DateTime.UtcNow);
        var currentStart = currentEnd.AddHours(-1);
        var baselineEnd = currentStart;
        var baselineStart = baselineEnd.AddHours(-1);

        var bodySucceeded = false;
        try
        {
            /* Present in both -> normal; present only current -> NEW; only baseline -> GONE. */
            await InsertQueryStatsAsync(connection, ComparisonServerId, currentStart.AddMinutes(10), "DB", "0xBOTH",
                deltaExec: 5, deltaWorker: 5000, deltaElapsed: 10000, deltaReads: 50, queryText: "both");
            await InsertQueryStatsAsync(connection, ComparisonServerId, baselineStart.AddMinutes(10), "DB", "0xBOTH",
                deltaExec: 5, deltaWorker: 5000, deltaElapsed: 8000, deltaReads: 50, queryText: "both");
            await InsertQueryStatsAsync(connection, ComparisonServerId, currentStart.AddMinutes(10), "DB", "0xNEW",
                deltaExec: 3, deltaWorker: 3000, deltaElapsed: 6000, deltaReads: 30, queryText: "new");
            await InsertQueryStatsAsync(connection, ComparisonServerId, baselineStart.AddMinutes(10), "DB", "0xGONE",
                deltaExec: 7, deltaWorker: 7000, deltaElapsed: 14000, deltaReads: 70, queryText: "gone");

            var items = await viewer.GetQueryStatsComparisonAsync(ComparisonServerId, currentStart, currentEnd, baselineStart, baselineEnd);

            Assert.Equal("NEW", items.Single(i => i.QueryHash == "0xNEW").StatusBadge);
            Assert.Equal("GONE", items.Single(i => i.QueryHash == "0xGONE").StatusBadge);
            Assert.Equal("", items.Single(i => i.QueryHash == "0xBOTH").StatusBadge);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "query_stats", ComparisonServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task QueryStatsSlicer_BucketsByHour_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live slicer test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "query_stats", SlicerServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(cs!);
        var end = TruncateToSeconds(DateTime.UtcNow);
        var start = end.AddHours(-24);
        var hour1 = TruncateToHour(start.AddHours(3));
        var hour2 = TruncateToHour(start.AddHours(5));

        var bodySucceeded = false;
        try
        {
            await InsertQueryStatsAsync(connection, SlicerServerId, hour1.AddMinutes(5), "DB", "0xA",
                deltaExec: 1, deltaWorker: 60_000, deltaElapsed: 120_000, deltaReads: 10, queryText: "a");
            await InsertQueryStatsAsync(connection, SlicerServerId, hour1.AddMinutes(35), "DB", "0xB",
                deltaExec: 1, deltaWorker: 60_000, deltaElapsed: 120_000, deltaReads: 10, queryText: "b");
            await InsertQueryStatsAsync(connection, SlicerServerId, hour2.AddMinutes(5), "DB", "0xA",
                deltaExec: 1, deltaWorker: 30_000, deltaElapsed: 60_000, deltaReads: 10, queryText: "a");

            var buckets = await viewer.GetQueryStatsSlicerDataAsync(SlicerServerId, start, end);

            Assert.Equal(2, buckets.Count); /* two distinct hours */
            var first = buckets[0];
            Assert.Equal(hour1, first.BucketTime);
            Assert.Equal(2, first.SessionCount);        /* two distinct query hashes in hour1 */
            Assert.Equal(120.0, first.TotalCpu, 3);     /* (60000 + 60000) us / 1000 -> ms */

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "query_stats", SlicerServerId, cleanupCt));
        }
    }

    // ── Insert helpers (only the columns each read touches; the rest default to NULL) ──

    private static async Task InsertQueryStatsAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, string databaseName, string queryHash,
        long deltaExec, long deltaWorker, long deltaElapsed, long deltaReads, string queryText, string sqlHandle = "0xSQLHANDLE")
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
     last_execution_time, creation_time, sample_interval_seconds,
     delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
     delta_rows, delta_logical_writes, delta_physical_reads, delta_spills,
     total_clr_time, plan_generation_num, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("viewer-queries-e2e");
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue(queryHash);
        command.Parameters.AddWithValue("0xPLANHASH");
        command.Parameters.AddWithValue(sqlHandle);
        command.Parameters.AddWithValue("0xPLANHANDLE");
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc.AddHours(-1), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(60);
        command.Parameters.AddWithValue(deltaExec);
        command.Parameters.AddWithValue(deltaWorker);
        command.Parameters.AddWithValue(deltaElapsed);
        command.Parameters.AddWithValue(deltaReads);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(queryText);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertProcedureStatsAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc,
        string databaseName, string schemaName, string objectName, string objectType,
        long deltaExec, long deltaWorker, long deltaElapsed, long deltaReads, string? planXml = null, string sqlHandle = "0xSQLHANDLE")
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, schema_name, object_name, object_type,
     cached_time, last_execution_time, sql_handle, plan_handle,
     delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
     delta_logical_writes, delta_physical_reads, delta_spills, query_plan_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("viewer-procs-e2e");
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue(schemaName);
        command.Parameters.AddWithValue(objectName);
        command.Parameters.AddWithValue(objectType);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc.AddHours(-1), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(sqlHandle);
        command.Parameters.AddWithValue("0xPLANHANDLE");
        command.Parameters.AddWithValue(deltaExec);
        command.Parameters.AddWithValue(deltaWorker);
        command.Parameters.AddWithValue(deltaElapsed);
        command.Parameters.AddWithValue(deltaReads);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue((object?)planXml ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <param name="firstExecutionTimeUtc">
    /// The tier-1 interval-identity PROXY (#1841). Defaults to one hour before the collection, so each
    /// collection models a distinct interval — the shape most tests want. Pass the SAME value across
    /// several collection times to model one interval being re-collected, which is what the dedup exists
    /// to collapse.
    /// </param>
    /// <param name="intervalId">
    /// The REAL interval identity (#1841 tier 2). Left NULL by default so the existing callers keep
    /// seeding the LEGACY generation — rows collected before tier 2, which every read still has to handle.
    /// Pass it to model a post-tier-2 row.
    /// </param>
    /// <param name="intervalStartUtc">
    /// When the interval STARTED, in UTC. NULL alongside a NULL <paramref name="intervalId"/> is the
    /// legacy shape, and it is what the reads key their legacy fallbacks on: bucket and trend placement
    /// both revert to collection_time for exactly these rows.
    /// </param>
    private static async Task InsertQueryStoreAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, string databaseName,
        long queryId, long planId, long execCount, long avgDurationUs, long avgCpuUs, bool forced, long maxMemPages, string queryText,
        DateTime? firstExecutionTimeUtc = null, long? intervalId = null, DateTime? intervalStartUtc = null)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, query_id, plan_id, query_hash, query_plan_hash, query_text, module_name,
     execution_type_desc, first_execution_time, last_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes,
     avg_physical_io_reads, avg_rowcount, avg_clr_time_us, avg_log_bytes_used, avg_tempdb_space_used,
     avg_num_physical_io_reads, avg_query_max_used_memory, min_dop, max_dop,
     plan_type, plan_forcing_type, is_forced_plan, force_failure_count, compatibility_level,
     runtime_stats_interval_id, interval_start_time_utc)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
        $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("viewer-qs-e2e");
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(planId);
        command.Parameters.AddWithValue("0xQSHASH");
        command.Parameters.AddWithValue("0xQSPLANHASH");
        command.Parameters.AddWithValue(queryText);
        command.Parameters.AddWithValue("dbo.usp_Qs");
        command.Parameters.AddWithValue("Regular");
        command.Parameters.AddWithValue(DateTime.SpecifyKind(firstExecutionTimeUtc ?? collectionTimeUtc.AddHours(-1), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(execCount);
        command.Parameters.AddWithValue(avgDurationUs);
        command.Parameters.AddWithValue(avgCpuUs);
        command.Parameters.AddWithValue(100L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(10L);
        command.Parameters.AddWithValue(50L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(5L);
        command.Parameters.AddWithValue(maxMemPages);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue("Compiled");
        command.Parameters.AddWithValue("None");
        command.Parameters.AddWithValue(forced);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(160);
        command.Parameters.AddWithValue((object?)intervalId ?? DBNull.Value);
        command.Parameters.AddWithValue(intervalStartUtc is null
            ? DBNull.Value
            : DateTime.SpecifyKind(intervalStartUtc.Value, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static DateTime TruncateToHour(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Year, value.Month, value.Day, value.Hour, 0, 0), DateTimeKind.Unspecified);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, string table, int serverId, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
