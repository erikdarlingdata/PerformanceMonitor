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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Performance Calendar / Daily Summary per-day-range aggregate (<c>DailySummaryRangeSql</c>)
/// against the Darling store contract — no live Postgres. The pins are the load-bearing clauses: per-day
/// bucketing, the day spine, the per-source CTEs, the top-wait ranking, the blocking XE→DMV fallback, the
/// total-host-CPU ≥ 80 rule, the memory tiers, and the actionable-alert filter.
/// </summary>
public sealed class ViewerDailySummarySqlTests
{
    [Fact]
    public void DailySummaryRangeSql_RollsUpEverySource_GroupedPerDay()
    {
        var sql = ViewerDataService.DailySummaryRangeSql;

        /* Per-day bucketing + a day spine so a quiet-but-collected day still appears. */
        Assert.Contains("date_trunc('day', collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("day_spine AS (", sql, StringComparison.Ordinal);
        Assert.Contains("UNION SELECT d FROM", sql, StringComparison.Ordinal);

        /* Total wait seconds + per-day top wait type off v_wait_stats, positive deltas only. */
        Assert.Contains("FROM v_wait_stats", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(ms) / 1000.0", sql, StringComparison.Ordinal);
        Assert.Contains("delta_wait_time_ms > 0", sql, StringComparison.Ordinal);
        Assert.Contains("DISTINCT ON (d)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY d, ms DESC", sql, StringComparison.Ordinal);

        /* Distinct query count, deadlock count. */
        Assert.Contains("COUNT(DISTINCT query_hash)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_deadlocks", sql, StringComparison.Ordinal);

        /* Blocking events: XE blocked-process reports, falling back to the DMV snapshot count when the
           XE count is zero (NULLIF → COALESCE). */
        Assert.Contains("COALESCE(NULLIF(b.c, 0), dm.c, 0)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_blocked_process_reports", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_dmv_blocking_snapshots", sql, StringComparison.Ordinal);

        /* Peak block wait (ms) for the day-detail blocking reason: MAX(wait_time_ms) per source, taken from
           the SAME source the count came from (BPR preferred, DMV fallback) so it reconciles with the count. */
        Assert.Contains("MAX(wait_time_ms) AS max_wait_ms", sql, StringComparison.Ordinal);
        Assert.Contains("CASE WHEN COALESCE(b.c, 0) > 0 THEN b.max_wait_ms ELSE dm.max_wait_ms END", sql, StringComparison.Ordinal);

        /* High-CPU count uses total host CPU = SQL + other-process (Linux NULL → 0), threshold 80, via FILTER. */
        Assert.Contains("(sqlserver_cpu_utilization + COALESCE(other_process_cpu_utilization, 0)) >= 80", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_cpu_utilization_stats", sql, StringComparison.Ordinal);
        Assert.Contains("FILTER (WHERE", sql, StringComparison.Ordinal);

        /* Collect errors off the collection_log ERROR rows; all-status runs mark the day collected. */
        Assert.Contains("status = 'ERROR'", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_collection_log", sql, StringComparison.Ordinal);

        /* Memory pressure (process OR system indicator >= 2) and the severe escalation (process >= 3). */
        Assert.Contains("FROM v_memory_pressure_events", sql, StringComparison.Ordinal);
        Assert.Contains("memory_indicators_process >= 2 OR memory_indicators_system >= 2", sql, StringComparison.Ordinal);
        Assert.Contains("memory_indicators_process >= 3", sql, StringComparison.Ordinal);

        /* Actionable alerts: non-dismissed, excluding resolution notices, off config_alert_log. */
        Assert.Contains("FROM config_alert_log", sql, StringComparison.Ordinal);
        Assert.Contains("dismissed = FALSE", sql, StringComparison.Ordinal);
        Assert.Contains("NOT LIKE '%Cleared%'", sql, StringComparison.Ordinal);

        /* Every subquery windows on the half-open [rangeStart, rangeEnd) range. */
        Assert.Contains("collection_time >= $2 AND collection_time < $3", sql, StringComparison.Ordinal);
        Assert.Contains("alert_time >= $2 AND alert_time < $3", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DailySummaryRangeSql_MemoryPressureColumns_ExistInTheGeneratedSourceTable()
    {
        var ddl = PgSchemaGenerator.CreateTable(MemoryPressureEventsCollector.Instance);
        Assert.Contains("memory_indicators_process", ddl, StringComparison.Ordinal);
        Assert.Contains("memory_indicators_system", ddl, StringComparison.Ordinal);
        Assert.Contains("v_memory_pressure_events", PgSchemaGenerator.AllPassthroughViews);
    }

    [Fact]
    public void DailySummaryRangeSql_IsPgDialect_PositionalParams_NoBareNow_NoNLiterals()
    {
        var sql = ViewerDataService.DailySummaryRangeSql;
        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
        Assert.Contains("$2", sql, StringComparison.Ordinal);
        Assert.Contains("$3", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DailySummaryRangeSql_ReadsColumnsThatExistInTheGeneratedSourceTables()
    {
        Assert.Contains("delta_wait_time_ms", PgSchemaGenerator.CreateTable(WaitStatsCollector.Instance), StringComparison.Ordinal);
        Assert.Contains("wait_type", PgSchemaGenerator.CreateTable(WaitStatsCollector.Instance), StringComparison.Ordinal);
        Assert.Contains("query_hash", PgSchemaGenerator.CreateTable(QueryStatsCollector.Instance), StringComparison.Ordinal);

        var cpuDdl = PgSchemaGenerator.CreateTable(CpuUtilizationCollector.Instance);
        Assert.Contains("sqlserver_cpu_utilization", cpuDdl, StringComparison.Ordinal);
        Assert.Contains("other_process_cpu_utilization", cpuDdl, StringComparison.Ordinal);
    }

    [Fact]
    public void PassthroughViews_ForEverySource_ExistInTheMigrations()
    {
        /* The daily-summary sources read the v_* passthrough views; they are created across the
           migration set. The alert signal reads the config_alert_log base table directly. */
        var allMigrationSql = string.Concat(PgMigrations.Scripts.Select(m => m.Sql));
        foreach (var view in new[]
        {
            "v_wait_stats", "v_query_stats", "v_deadlocks", "v_blocked_process_reports",
            "v_dmv_blocking_snapshots", "v_cpu_utilization_stats", "v_collection_log",
            "v_memory_pressure_events", "config_alert_log",
        })
        {
            Assert.Contains(view, allMigrationSql, StringComparison.Ordinal);
        }
    }
}

/// <summary>
/// Pins the Collection Health tab's three ported reads (W1i, copied from Lite's
/// LocalDataService.CollectionHealth.cs) — the 7-day per-collector aggregate, the recent-log read, and
/// the per-collector drill read — against the Darling store contract, no live Postgres.
/// </summary>
public sealed class ViewerCollectionHealthSqlTests
{
    [Fact]
    public void CollectionHealthSql_AggregatesPerCollectorOverTheWindow_WithTheBandingBuckets()
    {
        var sql = ViewerDataService.CollectionHealthSql;

        Assert.Contains("FROM v_collection_log", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY collector_name", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collector_name", sql, StringComparison.Ordinal);

        /* Success / error / permission buckets feed the row's HealthStatus banding. */
        Assert.Contains("SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(CASE WHEN status = 'PERMISSIONS' THEN 1 ELSE 0 END)", sql, StringComparison.Ordinal);

        /* SKIPPED counts as a healthy run for the last-success watermark. */
        Assert.Contains("MAX(CASE WHEN status IN ('SUCCESS', 'SKIPPED') THEN collection_time END)", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(duration_ms)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void RecentCollectionLogSql_BoundsTheWindowOnBothSides_NewestFirst_Capped()
    {
        var sql = ViewerDataService.RecentCollectionLogSql;
        Assert.Contains("FROM v_collection_log", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        /* Both-sided window bound so the toolbar's custom From/To is honored EXACTLY (not rounded to a
           now-relative hours-back span). $2 = window start, $3 = window end, $4 = row cap. */
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
        /* The two phase-timing columns the grid surfaces (Store (ms) = duckdb_duration_ms). */
        Assert.Contains("sql_duration_ms", sql, StringComparison.Ordinal);
        Assert.Contains("duckdb_duration_ms", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void CollectionLogByCollectorSql_FiltersToOneCollector_NewestFirst()
    {
        var sql = ViewerDataService.CollectionLogByCollectorSql;
        Assert.Contains("FROM v_collection_log", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collector_name = $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $3", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void CollectionHealthSql_AllThree_ArePgDialect_PositionalParams_NoBareNow_NoNLiterals()
    {
        foreach (var sql in new[]
        {
            ViewerDataService.CollectionHealthSql,
            ViewerDataService.RecentCollectionLogSql,
            ViewerDataService.CollectionLogByCollectorSql,
        })
        {
            Assert.DoesNotContain("now(", sql.ToLowerInvariant());
            Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
            Assert.Contains("$1", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void CollectionLog_ColumnsTheReadsUse_ExistInTheGeneratedTable()
    {
        var v2 = PgMigrations.Scripts.First(m => m.Name == "server-registry-and-collection-log").Sql;
        foreach (var column in new[]
        {
            "collector_name", "collection_time", "duration_ms", "sql_duration_ms",
            "duckdb_duration_ms", "rows_collected", "status", "error_message", "server_name",
        })
        {
            Assert.Contains(column, v2, StringComparison.Ordinal);
        }
    }
}

/// <summary>
/// Unit-tests the pure display helpers copied verbatim from Lite: the Daily Summary row's wait
/// formatting and the rich Collection Health row's HealthStatus banding + duration/time formatting.
/// These are the load-bearing bits computed in C# (not SQL), so they run without any Postgres.
/// </summary>
public sealed class ViewerDailyHealthRowTests
{
    [Theory]
    [InlineData(0, "0.0 s")]
    [InlineData(12.5, "12.5 s")]
    [InlineData(999.9, "999.9 s")]
    [InlineData(1000, "16.7 min")]     // >= 1000 s flips to minutes
    [InlineData(3600, "60.0 min")]
    public void DailySummaryRow_TotalWaitFormatted_FlipsToMinutesAtAThousandSeconds(double seconds, string expected)
    {
        var row = new DailySummaryRow { TotalWaitTimeSec = (decimal)seconds };
        Assert.Equal(expected, row.TotalWaitFormatted);
    }

    [Fact]
    public void DailySummaryRow_SummaryDateFormatted_IsIsoDate()
    {
        var row = new DailySummaryRow { SummaryDate = new DateTime(2026, 7, 3, 14, 0, 0, DateTimeKind.Utc) };
        Assert.Equal("2026-07-03", row.SummaryDateFormatted);
    }

    // The Daily Summary health banding moved to the shared, app-agnostic DailyHealthBandCalculator
    // (composite No-Data / Healthy / Warning / Critical for the Performance Calendar) and is pinned by
    // DailyHealthBandTests here in Darling.Tests and, identically, in Lite.Tests.

    [Fact]
    public void CollectorHealthRow_NeverRun_WhenNoRuns()
    {
        Assert.Equal("NEVER_RUN", new CollectorHealthRow { CollectorName = "wait_stats", TotalRuns = 0 }.HealthStatus);
    }

    [Fact]
    public void CollectorHealthRow_NoPermissions_WhenOnlyPermissionDenials()
    {
        var row = new CollectorHealthRow
        {
            CollectorName = "wait_stats",
            TotalRuns = 5,
            PermissionDeniedCount = 5,
            SuccessCount = 0,
            ErrorCount = 0,
        };
        Assert.Equal("NO_PERMISSIONS", row.HealthStatus);
    }

    [Fact]
    public void CollectorHealthRow_Stopped_WhenLastSuccessOlderThan24HoursAndNothingSince()
    {
        /* This row never sets LastRunTime, so HoursSinceLastRun falls back to HoursSinceLastSuccess (30h) -
           a 100%-success collector whose last success was 30h ago has attempted NOTHING since (no later
           success, no failure either), which is the STOPPED signature, not FAILING's "still running and
           erroring". Reclassified from FAILING now that the two clocks are distinguished; see
           CollectorHealthClassifierTests for the paired case proving FAILING still fires when the
           collector has a RECENT attempt (of any status) despite an old last success. */
        var row = new CollectorHealthRow
        {
            CollectorName = "wait_stats",
            TotalRuns = 10,
            SuccessCount = 10,
            LastSuccessTime = DateTime.UtcNow.AddHours(-30),
        };
        Assert.Equal("STOPPED", row.HealthStatus);
    }

    [Fact]
    public void CollectorHealthRow_Stale_WhenLastSuccessBetween4And24Hours()
    {
        var row = new CollectorHealthRow
        {
            CollectorName = "wait_stats",
            TotalRuns = 10,
            SuccessCount = 10,
            LastSuccessTime = DateTime.UtcNow.AddHours(-10),
        };
        Assert.Equal("STALE", row.HealthStatus);
    }

    [Fact]
    public void CollectorHealthRow_Warning_WhenRecentButFailureRateOver20Percent()
    {
        var row = new CollectorHealthRow
        {
            CollectorName = "wait_stats",
            TotalRuns = 10,
            SuccessCount = 7,
            ErrorCount = 3,   // 30% > 20%
            LastSuccessTime = DateTime.UtcNow.AddMinutes(-1),
        };
        Assert.Equal("WARNING", row.HealthStatus);
    }

    [Fact]
    public void CollectorHealthRow_Healthy_WhenRecentAndLowFailureRate()
    {
        var row = new CollectorHealthRow
        {
            CollectorName = "wait_stats",
            TotalRuns = 10,
            SuccessCount = 10,
            ErrorCount = 0,
            LastSuccessTime = DateTime.UtcNow.AddMinutes(-1),
        };
        Assert.Equal("HEALTHY", row.HealthStatus);
    }

    [Fact]
    public void CollectorHealthRow_OnLoadCollector_IsExemptFromStaleness()
    {
        /* server_config runs once per tab open, not on the loop — a 100-hour-old last success is NOT
           stale/failing for it (would be FAILING for a scheduled collector). */
        var row = new CollectorHealthRow
        {
            CollectorName = "server_config",
            TotalRuns = 3,
            SuccessCount = 3,
            LastSuccessTime = DateTime.UtcNow.AddHours(-100),
        };
        Assert.Equal("HEALTHY", row.HealthStatus);
    }

    [Fact]
    public void CollectorHealthRow_OnLoadCollector_StillWarnsOnHighFailureRate()
    {
        var row = new CollectorHealthRow
        {
            CollectorName = "server_config",
            TotalRuns = 10,
            SuccessCount = 7,
            ErrorCount = 3,   // 30% > 20%
            LastSuccessTime = DateTime.UtcNow.AddHours(-100),
        };
        Assert.Equal("WARNING", row.HealthStatus);
    }

    [Theory]
    [InlineData(0, 0, 0.0)]     // no runs → 0, no divide-by-zero
    [InlineData(10, 2, 20.0)]
    public void CollectorHealthRow_FailureRatePercent(long totalRuns, long errorCount, double expected)
    {
        var row = new CollectorHealthRow { TotalRuns = totalRuns, ErrorCount = errorCount };
        Assert.Equal(expected, row.FailureRatePercent, 3);
    }

    [Theory]
    [InlineData(500, "500 ms")]
    [InlineData(1500, "1.5 s")]
    public void CollectorHealthRow_AvgDurationFormatted_FlipsToSecondsAtAThousandMs(double avgMs, string expected)
    {
        Assert.Equal(expected, new CollectorHealthRow { AvgDurationMs = avgMs }.AvgDurationFormatted);
    }

    [Fact]
    public void CollectorHealthRow_NeverFormatting_WhenNoTimestamps()
    {
        var row = new CollectorHealthRow();
        Assert.Equal("Never", row.LastSuccessFormatted);
        Assert.Equal("Never", row.LastRunFormatted);
        Assert.Equal("", row.LastErrorFormatted);
    }

    [Theory]
    [InlineData(null, "")]
    [InlineData(500, "500 ms")]
    [InlineData(1500, "1.5 s")]
    public void CollectionLogRow_DurationFormatted_FlipsToSecondsAtAThousandMs(int? durationMs, string expected)
    {
        Assert.Equal(expected, new CollectionLogRow { DurationMs = durationMs }.DurationFormatted);
    }

    [Fact]
    public void CollectionLogRow_PhaseDurations_FormatAsMilliseconds()
    {
        var row = new CollectionLogRow { SqlDurationMs = 42, DuckDbDurationMs = 7 };
        Assert.Equal("42 ms", row.SqlDurationFormatted);
        Assert.Equal("7 ms", row.DuckDbDurationFormatted);   // surfaced as the "Store (ms)" column
    }

    [Fact]
    public void CollectionLogRow_CollectionTimeFormatted_TreatsTheStoredValueAsUtc()
    {
        /* Npgsql reads a naive timestamp as Unspecified; ToLocalTime treats Unspecified as UTC, so the
           formatted local time round-trips back to the stored instant on any machine. */
        var storedUtc = new DateTime(2026, 7, 3, 3, 30, 0, DateTimeKind.Unspecified);
        var row = new CollectionLogRow { CollectionTime = storedUtc };
        var expected = storedUtc.ToLocalTime().ToString("g");
        Assert.Equal(expected, row.CollectionTimeFormatted);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the Daily Summary aggregate and the Collection Health
/// reads. Shares the serialized "live-postgres" collection; uses negative sentinel server_ids and cleans
/// up in finally. These prove the copied SQL runs value-identically on real Postgres (the aggregate's
/// COALESCE/NULLIF fallback, the total-CPU ≥ 80 rule, the 7-day banding).
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerDailyHealthLivePostgresTests
{
    private const int SummaryServerId = -949601;
    private const int HealthServerId = -949602;
    private const string ServerName = "viewer-w1i-e2e";

    [Fact]
    public async Task DailySummary_RollsUpEverySource_ForTheSelectedDay_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live daily-summary test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteSummaryRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var day = DateTime.UtcNow.Date.AddDays(-2);
            var inDay = day.AddHours(12);
            var priorDay = day.AddDays(-1).AddHours(12);   // outside the window — must be excluded

            /* Waits: CXPACKET (100s) beats PAGEIOLATCH_SH (50s); a prior-day row must not leak in. */
            await InsertWaitAsync(connection, SummaryServerId, inDay, "CXPACKET", 100_000);
            await InsertWaitAsync(connection, SummaryServerId, inDay, "PAGEIOLATCH_SH", 50_000);
            await InsertWaitAsync(connection, SummaryServerId, priorDay, "CXPACKET", 999_000);

            /* Queries: two distinct hashes + one dup → COUNT(DISTINCT) = 2. */
            await InsertQueryAsync(connection, SummaryServerId, inDay, "0xAAA");
            await InsertQueryAsync(connection, SummaryServerId, inDay, "0xBBB");
            await InsertQueryAsync(connection, SummaryServerId, inDay, "0xAAA");

            /* CPU: 70+20 = 90 ≥ 80 counts; 40+10 = 50 does not. */
            await InsertCpuAsync(connection, SummaryServerId, inDay, sqlCpu: 70, otherCpu: 20);
            await InsertCpuAsync(connection, SummaryServerId, inDay, sqlCpu: 40, otherCpu: 10);

            await InsertDeadlockAsync(connection, SummaryServerId, inDay);
            await InsertBlockedProcessAsync(connection, SummaryServerId, inDay);
            await InsertCollectionLogAsync(connection, SummaryServerId, "wait_stats", inDay, "ERROR");

            var summary = await viewer.GetDailySummaryAsync(SummaryServerId, day);

            Assert.NotNull(summary);
            Assert.Equal(150m, summary!.TotalWaitTimeSec);         // (100000 + 50000) / 1000
            Assert.Equal("CXPACKET", summary.TopWaitType);
            Assert.Equal(2, summary.UniqueQueries);
            Assert.Equal(1, summary.DeadlockCount);
            Assert.Equal(1, summary.BlockingEvents);               // XE report count (fallback not used)
            Assert.Equal(1, summary.HighCpuEvents);                // only the 90% sample
            Assert.Equal(1, summary.CollectionErrors);
            Assert.Equal("Critical", summary.OverallHealth);       // deadlocks -> Critical composite band

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteSummaryRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task DailySummary_BlockingEvents_FallBackToTheDmvSnapshotCount_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live blocking-fallback test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteSummaryRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var day = DateTime.UtcNow.Date.AddDays(-3);
            var inDay = day.AddHours(9);

            /* No XE blocked-process reports; two DMV snapshots → the COALESCE(NULLIF(...)) falls back to
               the DMV count. No deadlocks / sustained CPU / heavy blocking, but 2 blocking events is
               "some blocking" → the composite band is Warning. */
            await InsertDmvBlockingAsync(connection, SummaryServerId, inDay);
            await InsertDmvBlockingAsync(connection, SummaryServerId, inDay);

            var summary = await viewer.GetDailySummaryAsync(SummaryServerId, day);

            Assert.NotNull(summary);
            Assert.Equal(2, summary!.BlockingEvents);
            Assert.Equal(0, summary.DeadlockCount);
            Assert.Equal("Warning", summary.OverallHealth);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteSummaryRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #1664: a window FIRMLY past the raw route margin, against the dev store, which is plain PostgreSQL and
    /// has no continuous aggregates. #1661's first cut routed this by age alone straight onto
    /// <c>collect.query_stats_hourly</c> and threw 42P01 — exactly what a bring-your-own plain-PG store would
    /// have hit in the field. The availability gate must detect the missing rollups and answer from raw,
    /// which on a plain-PG store holds full history (no retention policies without the extension).
    /// </summary>
    [Fact]
    public async Task DailySummary_OldWindow_FallsBackToRaw_WhenTheStoreHasNoRollups_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live rollup-fallback test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteSummaryRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            /* -10 days is deterministically Hourly-age (the -3d sibling above straddles the margin by
               time-of-day, which is how the first cut's live runs happened to pass). */
            var day = DateTime.UtcNow.Date.AddDays(-10);
            var inDay = day.AddHours(9);

            await InsertDmvBlockingAsync(connection, SummaryServerId, inDay);
            await InsertDmvBlockingAsync(connection, SummaryServerId, inDay);

            var summary = await viewer.GetDailySummaryAsync(SummaryServerId, day);

            Assert.NotNull(summary);
            Assert.Equal(2, summary!.BlockingEvents);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteSummaryRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task CollectionHealth_SevenDayAggregate_BandsEachCollector_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collection-health test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteHealthLogRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var recent = DateTime.UtcNow.AddMinutes(-5);
            var old = DateTime.UtcNow.AddHours(-30);

            /* healthy_collector: recent SUCCESS only → HEALTHY. */
            await InsertCollectionLogAsync(connection, HealthServerId, "healthy_collector", recent, "SUCCESS");
            /* skipped_collector: recent SKIPPED counts as a healthy run → HEALTHY (not STALE). */
            await InsertCollectionLogAsync(connection, HealthServerId, "skipped_collector", recent, "SKIPPED");
            /* failing_collector: only an old SUCCESS (>24h) → FAILING. */
            await InsertCollectionLogAsync(connection, HealthServerId, "failing_collector", old, "SUCCESS");

            var rows = await viewer.GetCollectionHealthAsync(HealthServerId);

            var healthy = rows.Single(r => r.CollectorName == "healthy_collector");
            var skipped = rows.Single(r => r.CollectorName == "skipped_collector");
            var failing = rows.Single(r => r.CollectorName == "failing_collector");

            Assert.Equal("HEALTHY", healthy.HealthStatus);
            Assert.Equal("HEALTHY", skipped.HealthStatus);   // SKIPPED is a healthy run
            Assert.Equal("FAILING", failing.HealthStatus);
            Assert.Equal(1, healthy.TotalRuns);
            Assert.Equal(1, healthy.SuccessCount);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteHealthLogRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task CollectionLogByCollector_ReturnsOneCollectorNewestFirst_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live per-collector log test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteHealthLogRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-20));
            var t2 = t1.AddMinutes(5);

            /* Two runs of wait_stats + one of a different collector that must not leak in. */
            await InsertCollectionLogAsync(connection, HealthServerId, "wait_stats", t1, "SUCCESS", durationMs: 120, sqlMs: 100, storeMs: 20, rows: 42);
            await InsertCollectionLogAsync(connection, HealthServerId, "wait_stats", t2, "ERROR", durationMs: 5, sqlMs: 5, storeMs: 0, rows: 0);
            await InsertCollectionLogAsync(connection, HealthServerId, "cpu_utilization", t2, "SUCCESS");

            var logs = await viewer.GetCollectionLogByCollectorAsync(HealthServerId, "wait_stats");

            Assert.Equal(2, logs.Count);
            Assert.All(logs, l => Assert.Equal("wait_stats", l.CollectorName));
            Assert.Equal(t2.Ticks, logs[0].CollectionTime.Ticks);   // newest first
            Assert.Equal(t1.Ticks, logs[1].CollectionTime.Ticks);
            Assert.Equal(120, logs[1].DurationMs);
            Assert.Equal(20, logs[1].DuckDbDurationMs);             // the store-phase column

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteHealthLogRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task RecentCollectionLog_HonorsTheExactWindow_ExcludingRowsOutsideBothBounds_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live windowed-log test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteHealthLogRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            /* A three-hour custom window [start, end] set entirely in the PAST (end is an hour ago). A row 30
               min BEFORE start and a row 30 min AFTER end must BOTH be excluded. The after-end exclusion is
               the behavior the old hours-back read (a single now-relative lower bound) could not express — it
               would have swept everything up to "now", pulling in the after-end row. */
            var end = TruncateToSeconds(DateTime.UtcNow.AddHours(-1));
            var start = end.AddHours(-3);
            var beforeStart = start.AddMinutes(-30);
            var inWindowEarly = start.AddMinutes(15);
            var inWindowLate = end.AddMinutes(-15);
            var afterEnd = end.AddMinutes(30);

            await InsertCollectionLogAsync(connection, HealthServerId, "wait_stats", beforeStart, "SUCCESS");
            await InsertCollectionLogAsync(connection, HealthServerId, "wait_stats", inWindowEarly, "SUCCESS");
            await InsertCollectionLogAsync(connection, HealthServerId, "cpu_utilization", inWindowLate, "SUCCESS");
            await InsertCollectionLogAsync(connection, HealthServerId, "wait_stats", afterEnd, "SUCCESS");

            var logs = await viewer.GetRecentCollectionLogAsync(HealthServerId, start, end);

            Assert.Equal(2, logs.Count);
            Assert.All(logs, l => Assert.InRange(l.CollectionTime.Ticks, start.Ticks, end.Ticks));
            Assert.Equal(inWindowLate.Ticks, logs[0].CollectionTime.Ticks);    // newest first
            Assert.Equal(inWindowEarly.Ticks, logs[1].CollectionTime.Ticks);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteHealthLogRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertWaitAsync(NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, string waitType, long deltaWaitMs)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(waitType);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(deltaWaitMs);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertQueryAsync(NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, string queryHash)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, query_hash) VALUES ($1, $2, $3, $4, $5)",
            connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(queryHash);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertCpuAsync(NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, int sqlCpu, int otherCpu)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(sqlCpu);
        command.Parameters.AddWithValue(otherCpu);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertDeadlockAsync(NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name) VALUES ($1, $2, $3, $4)",
            connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertBlockedProcessAsync(NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name) VALUES ($1, $2, $3, $4)",
            connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertDmvBlockingAsync(NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name) VALUES ($1, $2, $3, $4)",
            connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertCollectionLogAsync(
        NpgsqlConnection connection, int serverId, string collectorName, DateTime collectionTimeUtc, string status,
        int? durationMs = null, int? sqlMs = null, int? storeMs = null, int? rows = null)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, status, duration_ms, sql_duration_ms, duckdb_duration_ms, rows_collected)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(collectorName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(status);
        command.Parameters.AddWithValue((object?)durationMs ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)sqlMs ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)storeMs ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)rows ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task DeleteSummaryRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        foreach (var table in new[]
        {
            "wait_stats", "query_stats", "cpu_utilization_stats", "deadlocks",
            "blocked_process_reports", "dmv_blocking_snapshots", "collection_log",
        })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = {SummaryServerId};", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task DeleteHealthLogRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM collection_log WHERE server_id = {HealthServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
