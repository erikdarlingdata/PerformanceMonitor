/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the W1f-2 Queries sub-tab SQL against the Darling store contract (no live Postgres): the four
/// Performance-Trends reads, the Query-Heatmap DuckDB→PG rewrite, and the Active-Queries snapshot + latest
/// batch + slicer reads. String pins guard the load-bearing clauses — the LAG per-interval rate, the
/// base-table names, and (the wave's real work) the heatmap's <c>time_bucket → date_bin</c> and
/// <c>ARG_MAX → ROW_NUMBER</c> rewrites. Ordinal correctness + PG execution are covered by the gated live
/// round-trips below.
/// </summary>
public sealed class ViewerQueryTrendsSqlTests
{
    /// <summary>
    /// The three DELTA-based trends. Query Store is deliberately not here: its x-axis stopped being
    /// collection_time in #1841 tier 2 (see <see cref="QueryStoreDurationTrend_PlacesWorkAtTheIntervalStart_WithALegacyArm"/>),
    /// so folding it into this theory would force the shared pins to be loosened for all four and the
    /// delta trends would stop being pinned to the axis they genuinely use.
    /// </summary>
    [Theory]
    [InlineData(nameof(ViewerDataService.QueryDurationTrendSql), "query_stats")]
    [InlineData(nameof(ViewerDataService.ProcedureDurationTrendSql), "procedure_stats")]
    [InlineData(nameof(ViewerDataService.ExecutionCountTrendSql), "query_stats")]
    public void TrendSql_ComputesPerSecondRate_OverTheStoredInterval_BaseTable(string sqlName, string table)
    {
        var sql = SqlByName(sqlName);
        Assert.Contains($"FROM {table}", sql, StringComparison.Ordinal);
        Assert.DoesNotContain($"v_{table}", sql, StringComparison.Ordinal); /* viewer reads base tables */
        /* The per-second rate denominator is the collection's STORED interval (#3540 V128 for the procedure
           trend, #3653 A11 for the two query-stats trends): MAX(sample_interval_seconds), 0 → NULL (unrated),
           and the seconds-since-previous-snapshot LAG only where a pre-V128 collection recorded none. */
        Assert.Contains("CASE WHEN MAX(sample_interval_seconds) IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ELSE NULLIF(MAX(sample_interval_seconds), 0)", sql, StringComparison.Ordinal);
        Assert.Contains("LAG(collection_time) OVER (ORDER BY collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("extract(epoch FROM", sql, StringComparison.Ordinal);
        Assert.Contains("date_trunc('second', collection_time)", sql, StringComparison.Ordinal);
        /* interval_seconds > 0 guards the first row: its LAG is NULL, its rate unknowable, and the CASE has no
           ELSE — NULL, not a fabricated 0 (#3653; #3642 on the MCP copies). The reader skips the row. */
        Assert.Contains("CASE WHEN interval_seconds > 0 THEN", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ELSE 0", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3540 (V128): the procedure trend reads the collection's STORED interval — MAX over the collection's
    /// rows, 0 → NULL through NULLIF so a restart's marker collection drops rather than plotting 0.00 — and
    /// falls back to the LAG derivation only for a pre-V128 collection (NULL). No ELSE 0 anywhere in it: the
    /// rate is NULL when the interval is unknowable or absent and the reader drops the point. Its
    /// query-stats siblings kept the LAG-only form until #3653 A11 (the residual #3540 reported rather than
    /// rewrote); the theory above now pins all three to this read.
    /// </summary>
    [Fact]
    public void ProcedureDurationTrendSql_PrefersTheStoredInterval_AndNeverFabricatesZero()
    {
        var sql = ViewerDataService.ProcedureDurationTrendSql;
        Assert.Contains("CASE WHEN MAX(sample_interval_seconds) IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ELSE NULLIF(MAX(sample_interval_seconds), 0)", sql, StringComparison.Ordinal);
        Assert.Contains("THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ELSE 0", sql, StringComparison.Ordinal);
        Assert.Contains("CASE WHEN interval_seconds > 0 THEN total_elapsed_ms / interval_seconds END AS elapsed_ms_per_second", sql, StringComparison.Ordinal);
        Assert.Contains("CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds END AS executions_per_second", sql, StringComparison.Ordinal);

        /* And the shared reader DROPS a NULL-rate row rather than reading it as 0 — the C# half of the idiom.
           #3653: one loop (ReadTrendPointsAsync) serves every trend here, including the execution-count one
           that used to coerce its NULL to 0 in a loop of its own, so the file has no `IsDBNull(n) ? 0` left. */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.QueryTrends.cs");
        var reader = source[source.IndexOf("private async Task<List<QueryTrendPoint>> ReadTrendPointsAsync(", StringComparison.Ordinal)..];
        reader = reader[..reader.IndexOf("return items;", StringComparison.Ordinal)];
        Assert.Contains("if (reader.IsDBNull(valueOrdinal))", reader, StringComparison.Ordinal);
        Assert.Contains("continue;", reader, StringComparison.Ordinal);
        Assert.DoesNotContain("reader.IsDBNull(1) ? 0", source, StringComparison.Ordinal);
        Assert.DoesNotContain("IsDBNull(valueOrdinal) ? 0", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3653: the Query Store trend's raw shape — the fallback on a store without the corrected rollup — no
    /// longer fabricates a 0 for the first placed interval either; the rollup-routed builder stopped in #3642,
    /// and a store's two routes must open a series the same way.
    /// </summary>
    [Fact]
    public void QueryStoreDurationTrendSql_FirstPlacedInterval_IsUnrated_NotZero()
    {
        var sql = ViewerDataService.QueryStoreDurationTrendSql;
        Assert.DoesNotContain("ELSE 0", sql, StringComparison.Ordinal);
        Assert.Contains("CASE WHEN interval_seconds > 0 THEN total_duration_ms / interval_seconds END AS duration_ms_per_second", sql, StringComparison.Ordinal);
        Assert.Contains("CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds END AS executions_per_second", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ELSE 0", ViewerDataService.QueryStoreDurationTrendRollupSql, StringComparison.Ordinal);
    }

    [Fact]
    public void DurationTrendSql_SumsElapsedMs_ExecutionTrendSql_OnlyExecutions()
    {
        Assert.Contains("SUM(delta_elapsed_time) / 1000.0", ViewerDataService.QueryDurationTrendSql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_elapsed_time) / 1000.0", ViewerDataService.ProcedureDurationTrendSql, StringComparison.Ordinal);
        /* Query Store has no delta_elapsed_time: duration = execution_count * avg_duration_us. */
        Assert.Contains("SUM(execution_count * avg_duration_us / 1000.0)", ViewerDataService.QueryStoreDurationTrendSql, StringComparison.Ordinal);
        /* The execution-count trend projects only the executions/sec column (no elapsed). */
        Assert.DoesNotContain("elapsed", ViewerDataService.ExecutionCountTrendSql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_execution_count)", ViewerDataService.ExecutionCountTrendSql, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStoreDurationTrend_PlacesWorkAtTheIntervalStart_WithALegacyArm()
    {
        /* DELIBERATE FLIP of the #1845 pin that stood here (#1841 tier 2). That pin recorded the one
           Query Store aggregate left un-deduped, and its reasoning held right up to a premise that was
           FALSE: that placing the work at the interval's own clock trades a magnitude bug for a timezone
           bug, because that clock is the monitored server's LOCAL wall time. It is not. Query Store's
           interval start_time and first_execution_time are both datetimeoffset — verified on a live
           server reading +00:00 while the host sat at UTC-4 — and the collector normalizes through
           DateTimeOffset.UtcDateTime before storing. So the interval clock shares an axis with
           collection_time, and dedup + interval-start placement together give a series that neither
           overstates nor collapses.

           Both arms must stay, and the split must stay TOTAL. */
        var sql = ViewerDataService.QueryStoreDurationTrendSql;

        /* Arm 1: deduped per interval, keyed on the real identity, placed at the interval start. */
        Assert.Contains("WHERE rn = 1", sql, StringComparison.Ordinal);
        Assert.Contains("PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role", sql, StringComparison.Ordinal);
        Assert.Contains("interval_start_time_utc AS point_time", sql, StringComparison.Ordinal);
        Assert.Contains("AND   interval_start_time_utc IS NOT NULL", sql, StringComparison.Ordinal);

        /* Arm 2: pre-tier-2 rows, un-deduped at collection_time — the behavior they always had, because
           nothing can reconstruct an interval start for them. */
        Assert.Contains("collection_time AS point_time", sql, StringComparison.Ordinal);
        Assert.Contains("AND   interval_start_time_utc IS NULL", sql, StringComparison.Ordinal);

        /* IS NULL / IS NOT NULL partitions the rows with no overlap and no gap: a mixed window counts
           every row exactly once. A predicate pair that could ever both match, or both miss, would make
           this chart double-count or silently drop the transition window. */
        Assert.Single(Regex.Matches(sql, @"AND   interval_start_time_utc IS NOT NULL"));
        Assert.Single(Regex.Matches(sql, @"AND   interval_start_time_utc IS NULL"));
        Assert.Contains("UNION ALL", sql, StringComparison.Ordinal);

        /* The rate denominator now measures interval-start to interval-start, not cycle to cycle. */
        Assert.Contains("LAG(point_time) OVER (ORDER BY point_time)", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY point_time", sql, StringComparison.Ordinal);

        /* The delta-based trends never needed any of this: their columns are already per-cycle increments. */
        Assert.DoesNotContain("first_execution_time", ViewerDataService.QueryDurationTrendSql, StringComparison.Ordinal);
        Assert.DoesNotContain("first_execution_time", ViewerDataService.ExecutionCountTrendSql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(ViewerDataService.QueryDurationTrendSql))]
    [InlineData(nameof(ViewerDataService.ProcedureDurationTrendSql))]
    [InlineData(nameof(ViewerDataService.QueryStoreDurationTrendSql))]
    [InlineData(nameof(ViewerDataService.ExecutionCountTrendSql))]
    public void TrendSql_IsPostgresDialect_PositionalParams(string sqlName)
    {
        var sql = SqlByName(sqlName);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
        Assert.Contains("$2", sql, StringComparison.Ordinal);
        Assert.Contains("$3", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("getdate", sql.ToLowerInvariant());
    }

    private static string SqlByName(string name) => name switch
    {
        nameof(ViewerDataService.QueryDurationTrendSql) => ViewerDataService.QueryDurationTrendSql,
        nameof(ViewerDataService.ProcedureDurationTrendSql) => ViewerDataService.ProcedureDurationTrendSql,
        nameof(ViewerDataService.QueryStoreDurationTrendSql) => ViewerDataService.QueryStoreDurationTrendSql,
        _ => ViewerDataService.ExecutionCountTrendSql,
    };
}

/// <summary>
/// Pins the Query-Heatmap DuckDB→PG rewrite — the wave's load-bearing SQL work. Guards that both
/// dialect swaps landed for EVERY metric: <c>time_bucket</c> is gone (replaced by <c>date_bin</c> with an
/// explicit epoch origin) and <c>ARG_MAX</c> is gone (replaced by a <c>ROW_NUMBER … rn = 1</c> top-1
/// window plus <c>COUNT(*) OVER</c> for the per-cell count).
/// </summary>
public sealed class ViewerQueryHeatmapSqlTests
{
    private static readonly HeatmapMetric[] AllMetrics =
    {
        HeatmapMetric.Duration, HeatmapMetric.Cpu, HeatmapMetric.LogicalReads,
        HeatmapMetric.LogicalWrites, HeatmapMetric.ExecutionCount,
    };

    [Fact]
    public void HeatmapSql_RewritesTimeBucketToDateBin_WithEpochOrigin_ForEveryMetric()
    {
        foreach (var metric in AllMetrics)
        {
            var sql = ViewerDataService.BuildQueryHeatmapSql(metric);
            Assert.DoesNotContain("time_bucket", sql, StringComparison.Ordinal); /* DuckDB-only */
            Assert.Contains("date_bin(INTERVAL '5 minutes', collection_time, TIMESTAMP '1970-01-01 00:00:00')", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void HeatmapSql_RewritesArgMaxToTop1Window_ForEveryMetric()
    {
        foreach (var metric in AllMetrics)
        {
            var sql = ViewerDataService.BuildQueryHeatmapSql(metric);
            Assert.DoesNotContain("ARG_MAX", sql, StringComparison.Ordinal); /* DuckDB-only */
            /* Hottest query per cell = the max-delta_execution_count row, taken by a top-1 window. */
            Assert.Contains("ROW_NUMBER() OVER (PARTITION BY time_bin, bucket_index ORDER BY delta_execution_count DESC) AS rn", sql, StringComparison.Ordinal);
            Assert.Contains("COUNT(*) OVER (PARTITION BY time_bin, bucket_index) AS query_count", sql, StringComparison.Ordinal);
            Assert.Contains("WHERE rn = 1", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void HeatmapSql_KeepsLitesMagnitudeBuckets_Filters_And_Preview()
    {
        var sql = ViewerDataService.BuildQueryHeatmapSql(HeatmapMetric.Duration);
        /* v_query_stats, not the base table (#1767): the cell preview is LEFT(query_text, 120), and the
           base table's inline query_text is NULL on every row written since the migration — the heatmap
           would render with blank previews rather than fail. */
        Assert.Contains("FROM v_query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("delta_execution_count > 0", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT(query_text, 120) AS query_preview", sql, StringComparison.Ordinal);
        /* The 7-way log-magnitude CASE (only the boundaries are pinned). */
        Assert.Contains("WHEN metric_value < 1 THEN 0", sql, StringComparison.Ordinal);
        Assert.Contains("WHEN metric_value < 100000 THEN 5", sql, StringComparison.Ordinal);
        Assert.Contains("ELSE 6", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY time_bin, bucket_index", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(HeatmapMetric.Duration, "(delta_elapsed_time / 1000.0) / NULLIF(delta_execution_count, 0)")]
    [InlineData(HeatmapMetric.Cpu, "(delta_worker_time / 1000.0) / NULLIF(delta_execution_count, 0)")]
    [InlineData(HeatmapMetric.LogicalReads, "CAST(delta_logical_reads AS DOUBLE PRECISION) / NULLIF(delta_execution_count, 0)")]
    [InlineData(HeatmapMetric.LogicalWrites, "CAST(delta_logical_writes AS DOUBLE PRECISION) / NULLIF(delta_execution_count, 0)")]
    [InlineData(HeatmapMetric.ExecutionCount, "CAST(delta_execution_count AS DOUBLE PRECISION)")]
    public void HeatmapSql_UsesLitesPerMetricExpression(HeatmapMetric metric, string expr)
    {
        var sql = ViewerDataService.BuildQueryHeatmapSql(metric);
        Assert.Contains(expr, sql, StringComparison.Ordinal);
        /* $1/$2/$3 positional params, no T-SQL-isms. */
        Assert.Contains("$1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }
}

/// <summary>Pins the Active-Queries reads (snapshot window, latest batch, slicer) against the store contract.</summary>
public sealed class ViewerQuerySnapshotsSqlTests
{
    [Fact]
    public void LatestQuerySnapshotsSql_SelectsTheWindow_TrimsWaitfor_OrdersNewestThenCpu_BaseTable()
    {
        var sql = ViewerDataService.LatestQuerySnapshotsSql;
        Assert.Contains("FROM query_snapshots", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_query_snapshots", sql, StringComparison.Ordinal); /* viewer reads base tables */
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("query_text NOT LIKE 'WAITFOR%'", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC, cpu_time_ms DESC", sql, StringComparison.Ordinal);
        /* The plan columns the Estimated / Actual buttons bind are selected. */
        Assert.Contains("query_plan", sql, StringComparison.Ordinal);
        Assert.Contains("live_query_plan", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void LatestQuerySnapshotBatchSql_TakesOnlyTheNewestCaptureForTheServer()
    {
        var sql = ViewerDataService.LatestQuerySnapshotBatchSql;
        Assert.Contains("FROM query_snapshots", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time = (SELECT MAX(collection_time) FROM query_snapshots WHERE server_id = $1)", sql, StringComparison.Ordinal);
        Assert.Contains("query_text NOT LIKE 'WAITFOR%'", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY cpu_time_ms DESC", sql, StringComparison.Ordinal);
        /* #1319: the batch stays server-scoped (the MAX(collection_time) subquery reuses $1, not a window),
           plus the global database filter's single nullable text[] param ($2 = ANY). */
        Assert.Contains("database_name = ANY($2)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ActiveQuerySlicerSql_BucketsByHour_SevenColumnShape()
    {
        var sql = ViewerDataService.ActiveQuerySlicerSql;
        Assert.Contains("date_trunc('hour', collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM query_snapshots", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS session_count", sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(SUM(cpu_time_ms), 0)", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY bucket", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void QuerySnapshotColumns_SelectTheHash286TriageColumns_ForTheWaitDrillDownGrid()
    {
        /* Item 8: the #1286 memory-grant / tempdb / transaction triage columns Lite's WaitDrillDownWindow
           binds are now selected (they were dropped from the viewer's ~26-column read). Pinned via the
           shared LatestQuerySnapshotsSql, which is built from QuerySnapshotColumns. */
        var sql = ViewerDataService.LatestQuerySnapshotsSql;
        foreach (var column in new[]
        {
            "requested_memory_mb", "used_memory_mb", "max_used_memory_mb",
            "tempdb_current_mb", "tempdb_allocations_mb", "tran_log_used_mb", "tran_start_time", "request_id",
        })
        {
            Assert.Contains(column, sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void QuerySnapshotTriageColumns_ExistInTheGeneratedStoreTable()
    {
        /* Verification gate for item 8: every triage column the viewer now surfaces IS collected into
           query_snapshots (no collector gap). */
        Assert.Equal("query_snapshots", QuerySnapshotsCollector.Instance.TargetTable);
        var ddl = PgSchemaGenerator.CreateTable(QuerySnapshotsCollector.Instance);
        foreach (var column in new[]
        {
            "requested_memory_mb", "used_memory_mb", "max_used_memory_mb",
            "tempdb_current_mb", "tempdb_allocations_mb", "tran_log_used_mb", "tran_start_time", "request_id",
        })
        {
            Assert.Contains(column, ddl, StringComparison.Ordinal);
        }
    }
}

/// <summary>The W1f-2 row models' pure helpers: snapshot plan gating + naive-UTC collection-time display.</summary>
public sealed class ViewerActiveQueriesDisplayTests
{
    [Fact]
    public void QuerySnapshotRow_HasPlanFlags_GateOnNonEmptyPlanXml()
    {
        Assert.False(new ViewerQuerySnapshotRow().HasQueryPlan);
        Assert.False(new ViewerQuerySnapshotRow().HasLiveQueryPlan);
        Assert.True(new ViewerQuerySnapshotRow { QueryPlan = "<ShowPlanXML/>" }.HasQueryPlan);
        Assert.True(new ViewerQuerySnapshotRow { LiveQueryPlan = "<ShowPlanXML/>" }.HasLiveQueryPlan);
    }

    [Fact]
    public void QuerySnapshotRow_CollectionTimeLocal_ConvertsNaiveUtc_EmptyForMinValue()
    {
        /* collection_time is naive UTC in the store; the display converts to local (unlike the raw
           server-clock last_execution_time). MinValue (no row) renders empty. */
        var row = new ViewerQuerySnapshotRow { CollectionTime = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Unspecified) };
        var expected = ViewerTimeHelper.ForDisplay(row.CollectionTime).ToString("yyyy-MM-dd HH:mm:ss");
        Assert.Equal(expected, row.CollectionTimeLocal);
        Assert.Equal("", new ViewerQuerySnapshotRow().CollectionTimeLocal);
    }

    [Fact]
    public void QueryTrendPoint_CarriesRateAndExecutionCount()
    {
        var p = new QueryTrendPoint { CollectionTime = new DateTime(2026, 7, 1, 9, 0, 0), Value = 2.5, ExecutionCount = 7 };
        Assert.Equal(2.5, p.Value);
        Assert.Equal(7, p.ExecutionCount);
    }

    [Fact]
    public void QuerySnapshotRow_TranStartTimeLocal_RendersRawServerClock_EmptyWhenNoOpenTransaction()
    {
        /* Item 8: the WaitDrillDownWindow's "Tran Start" column. transaction_begin_time is a server-local
           wall-clock time (NOT naive UTC), so it renders raw via FormatServerClock like the other dm_exec_*
           times — no UTC→display conversion. A null TranStartTime (no open transaction) renders empty. */
        Assert.Equal("", new ViewerQuerySnapshotRow().TranStartTimeLocal);
        var row = new ViewerQuerySnapshotRow { TranStartTime = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Unspecified) };
        Assert.Equal("2026-07-01 12:00:00", row.TranStartTimeLocal);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the W1f-2 reads: a duration trend, the heatmap rewrite,
/// the snapshot window + latest batch, and the slicer. Each plants rows for a negative sentinel server and
/// asserts the read executes on real Postgres and returns the expected shape end-to-end (the string pins
/// can't catch a date_bin / window-function dialect error or an ordinal slip). Serialized on the
/// "live-postgres" collection; cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerQueriesRestLivePostgresTests
{
    private const int TrendServerId = -970811;
    private const int HeatmapServerId = -970812;
    private const int SnapshotServerId = -970813;
    private const int SlicerServerId = -970814;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task QueryDurationTrend_ComputesPerSecondRate_AcrossTwoCollections_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live trend test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "query_stats", TrendServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(cs!);
        var end = TruncateToSeconds(DateTime.UtcNow);
        var start = end.AddHours(-24);
        var t1 = start.AddHours(1);
        var t2 = t1.AddSeconds(60); /* one 60-second interval later */

        var bodySucceeded = false;
        try
        {
            await InsertQueryStatsAsync(connection, TrendServerId, t1, "0xA", deltaExec: 10, deltaWorker: 30_000, deltaElapsed: 60_000, deltaReads: 100, deltaWrites: 0, queryText: "SELECT a");
            await InsertQueryStatsAsync(connection, TrendServerId, t2, "0xA", deltaExec: 120, deltaWorker: 60_000, deltaElapsed: 120_000, deltaReads: 200, deltaWrites: 0, queryText: "SELECT a");

            var series = await viewer.GetQueryDurationTrendAsync(TrendServerId, start, end);
            var points = series.Points;

            /* #3653 A11: BOTH rows were seeded with a STORED sample_interval_seconds of 60, so both collections
               have a knowable rate and both are plotted. Until #3653 this pin held one point: the read LAG-
               recomputed the interval from row spacing and the first collection's LAG was NULL — the LAG idiom's
               rule applied to a row whose interval the store had carried all along. Now the interval is read:
               t1 is 60 ms over its stored 60 s (1 ms/sec, 10 executions -> 0.17/sec, truncated to 0), t2 is 120 ms
               over 60 s. A collection whose stored interval is 0 (a restart) or a pre-V128 collection with
               nothing to LAG against would still be unrated and skipped — DeltaFamilyIntervalCompletionLivePostgresTests
               pins those rows. */
            Assert.Equal(2, points.Count);
            Assert.Equal(t1, points[0].CollectionTime);
            Assert.Equal(1.0, points[0].Value, 3);       /* 60_000 us = 60 ms over the STORED 60 s -> 1 ms/sec */
            Assert.Equal(0, points[0].ExecutionCount);   /* 10 execs over 60 s -> 0.17/sec (truncated) */
            var point = points[1];
            Assert.Equal(t2, point.CollectionTime);
            Assert.Equal(2.0, point.Value, 3);          /* 120_000 us = 120 ms over 60 s -> 2 ms/sec */
            Assert.Equal(2, point.ExecutionCount);       /* 120 execs over 60 s -> 2/sec (truncated) */

            /* A 24-hour window routes raw, and the series says so; its head is the first SERVED point (t1,
               60 minutes past the start — inside the 90-minute slack, so not truncated). */
            Assert.Equal(RetentionTier.Raw, series.Tier);
            Assert.Equal("raw", series.Source);
            Assert.Equal(t1, series.EffectiveStartUtc);
            Assert.False(series.Truncated);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "query_stats", TrendServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task QueryHeatmap_BinsByTime_BucketsByMagnitude_TopQueryByExecutions_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live heatmap test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "query_stats", HeatmapServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(cs!);
        var end = TruncateToSeconds(DateTime.UtcNow);
        var start = end.AddHours(-24);
        var t1 = start.AddHours(1); /* all rows share one collection_time -> one 5-min bin */

        var bodySucceeded = false;
        try
        {
            /* Duration metric = (delta_elapsed_time / 1000) / delta_exec (ms/exec).
               Two rows at ~50ms -> bucket 2; the higher-exec one is the cell's top query. */
            await InsertQueryStatsAsync(connection, HeatmapServerId, t1, "0xHOT", deltaExec: 5, deltaWorker: 0, deltaElapsed: 250_000, deltaReads: 0, deltaWrites: 0, queryText: "SELECT hot");
            await InsertQueryStatsAsync(connection, HeatmapServerId, t1, "0xCOLD", deltaExec: 1, deltaWorker: 0, deltaElapsed: 50_000, deltaReads: 0, deltaWrites: 0, queryText: "SELECT cold");
            /* 0.5ms -> bucket 0. */
            await InsertQueryStatsAsync(connection, HeatmapServerId, t1, "0xLOW", deltaExec: 2, deltaWorker: 0, deltaElapsed: 1_000, deltaReads: 0, deltaWrites: 0, queryText: "SELECT low");
            /* delta_exec = 0 -> excluded by the > 0 filter (must not inflate any cell). */
            await InsertQueryStatsAsync(connection, HeatmapServerId, t1, "0xZERO", deltaExec: 0, deltaWorker: 0, deltaElapsed: 999_000, deltaReads: 0, deltaWrites: 0, queryText: "SELECT zero");

            var result = await viewer.GetQueryHeatmapAsync(HeatmapServerId, HeatmapMetric.Duration, start, end);

            Assert.Single(result.TimeBuckets);                        /* one 5-min bin */
            Assert.Equal(7, result.Intensities.GetLength(0));         /* seven magnitude buckets */
            Assert.Equal(2.0, result.Intensities[2, 0]);              /* bucket 2: 0xHOT + 0xCOLD */
            Assert.Equal(1.0, result.Intensities[0, 0]);              /* bucket 0: 0xLOW */
            Assert.Equal("0xHOT", result.CellDetails[2, 0].TopQueryHash); /* ARG_MAX replacement: higher delta_exec wins */
            Assert.Equal("0xLOW", result.CellDetails[0, 0].TopQueryHash);
            /* 0xZERO (delta_exec = 0) contributed to no cell. */
            Assert.Equal(0.0, result.Intensities[6, 0]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "query_stats", HeatmapServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task QuerySnapshots_TrimsWaitfor_OrdersNewestThenCpu_AndLatestBatch_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live snapshots test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "query_snapshots", SnapshotServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(cs!);
        var end = TruncateToSeconds(DateTime.UtcNow);
        var start = end.AddHours(-24);
        var older = start.AddHours(1);
        var newer = start.AddHours(2);

        var bodySucceeded = false;
        try
        {
            await InsertQuerySnapshotAsync(connection, SnapshotServerId, older, spid: 51, cpu: 100, hash: "0xB1", queryText: "SELECT older");
            await InsertQuerySnapshotAsync(connection, SnapshotServerId, older, spid: 99, cpu: 999, hash: "0xWAIT", queryText: "WAITFOR DELAY '00:00:05'");
            await InsertQuerySnapshotAsync(connection, SnapshotServerId, newer, spid: 52, cpu: 500, hash: "0xB2", queryText: "SELECT mid");
            await InsertQuerySnapshotAsync(connection, SnapshotServerId, newer, spid: 53, cpu: 900, hash: "0xB3", queryText: "SELECT hot");

            var rows = await viewer.GetLatestQuerySnapshotsAsync(SnapshotServerId, start, end);

            Assert.Equal(3, rows.Count);                       /* WAITFOR excluded */
            Assert.DoesNotContain(rows, r => r.QueryHash == "0xWAIT");
            Assert.Equal(53, rows[0].SessionId);               /* newest batch, highest cpu first */
            Assert.Equal(52, rows[1].SessionId);
            Assert.Equal(51, rows[2].SessionId);               /* older batch last */

            var (batchTime, batch) = await viewer.GetLatestQuerySnapshotBatchAsync(SnapshotServerId);

            Assert.Equal(newer, batchTime);                    /* only the newest capture */
            Assert.Equal(2, batch.Count);
            Assert.Equal(53, batch[0].SessionId);              /* cpu DESC within the batch */
            Assert.Equal(52, batch[1].SessionId);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "query_snapshots", SnapshotServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task ActiveQuerySlicer_BucketsByHour_CountsSessions_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live active-queries slicer test.");

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "query_snapshots", SlicerServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(cs!);
        var end = TruncateToSeconds(DateTime.UtcNow);
        var start = end.AddHours(-24);
        var hour1 = TruncateToHour(start.AddHours(3));
        var hour2 = TruncateToHour(start.AddHours(5));

        var bodySucceeded = false;
        try
        {
            await InsertQuerySnapshotAsync(connection, SlicerServerId, hour1.AddMinutes(5), spid: 60, cpu: 100, hash: "0xA", queryText: "a");
            await InsertQuerySnapshotAsync(connection, SlicerServerId, hour1.AddMinutes(35), spid: 61, cpu: 200, hash: "0xB", queryText: "b");
            await InsertQuerySnapshotAsync(connection, SlicerServerId, hour2.AddMinutes(5), spid: 62, cpu: 300, hash: "0xC", queryText: "c");

            var buckets = await viewer.GetActiveQuerySlicerDataAsync(SlicerServerId, start, end);

            Assert.Equal(2, buckets.Count); /* two distinct hours */
            Assert.Equal(hour1, buckets[0].BucketTime);
            Assert.Equal(2, buckets[0].SessionCount);      /* two snapshots in hour1 */
            Assert.Equal(300.0, buckets[0].TotalCpu, 3);   /* 100 + 200 */

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "query_snapshots", SlicerServerId, cleanupCt));
        }
    }

    // ── Insert helpers (only the columns each read touches; the rest default to NULL) ──

    private static async Task InsertQueryStatsAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, string queryHash,
        long deltaExec, long deltaWorker, long deltaElapsed, long deltaReads, long deltaWrites, string queryText)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, query_hash, sample_interval_seconds,
     delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes,
     query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("viewer-queries-rest-e2e");
        command.Parameters.AddWithValue("StackOverflow");
        command.Parameters.AddWithValue(queryHash);
        command.Parameters.AddWithValue(60);
        command.Parameters.AddWithValue(deltaExec);
        command.Parameters.AddWithValue(deltaWorker);
        command.Parameters.AddWithValue(deltaElapsed);
        command.Parameters.AddWithValue(deltaReads);
        command.Parameters.AddWithValue(deltaWrites);
        command.Parameters.AddWithValue(queryText);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertQuerySnapshotAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, int spid, long cpu, string hash, string queryText)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name,
     session_id, database_name, query_text, status,
     cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads,
     wait_type, query_hash)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("viewer-snapshots-e2e");
        command.Parameters.AddWithValue(spid);
        command.Parameters.AddWithValue("StackOverflow");
        command.Parameters.AddWithValue(queryText);
        command.Parameters.AddWithValue("running");
        command.Parameters.AddWithValue(cpu);
        command.Parameters.AddWithValue(cpu * 2);
        command.Parameters.AddWithValue(10L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(20L);
        command.Parameters.AddWithValue("CXPACKET");
        command.Parameters.AddWithValue(hash);
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
