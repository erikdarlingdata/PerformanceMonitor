/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using PerformanceMonitorDashboard.Analysis;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// #3527/#3561: cntr_value_delta spans one COLLECTION INTERVAL, not one second — read raw, the
/// perfmon analysis reads overstate by the cadence (60x at 60s, 300x at 5min). All three reads
/// must divide by the row's measured sample_interval_seconds and skip interval &lt;= 0 rows
/// (no delta was knowable: first sighting, reset, gap) — together or the baseline comparison is
/// cross-unit. The Dashboard has no test database, so these pin the SQL text, the mirror of the
/// Darling twin's #3560 pins (PgFactCollector.PerfmonSql / PgAnomalyDetector.BatchRequestWindowSql).
/// </summary>
public class PerfmonPerSecondSqlTests
{
    /* ---------------- read 1: the fact collector ---------------- */

    [Fact]
    public void PerfmonFactSql_SelectsTheMeasuredInterval_AndFiltersUnknowableRows()
    {
        var sql = SqlServerFactCollector.PerfmonSql;

        /* The interval rides both the CTE and the outer select so the C# division has its divisor. */
        Assert.Contains("sample_interval_seconds,", sql);
        Assert.Contains("SELECT counter_name, cntr_value, cntr_value_delta, sample_interval_seconds", sql);

        /* Interval <= 0 rows are filtered INSIDE the CTE, so rn = 1 lands on the newest row a rate
           can honestly be derived from — a counter with only interval-0 rows emits no fact, never 0. */
        Assert.Contains("AND   sample_interval_seconds > 0", sql);
        var filterAt = sql.IndexOf("AND   sample_interval_seconds > 0", System.StringComparison.Ordinal);
        var rnFilterAt = sql.IndexOf("FROM latest WHERE rn = 1", System.StringComparison.Ordinal);
        Assert.True(filterAt >= 0 && filterAt < rnFilterAt, "the interval filter must sit inside the latest CTE, before rn = 1");
    }

    /* ---------------- read 2: the anomaly detector's window ---------------- */

    [Fact]
    public void BatchRequestWindow_DividesByMeasuredInterval_AndSkipsUnknowableRows()
    {
        var sql = SqlServerAnomalyDetector.BatchRequestWindowSql;

        Assert.Contains("AVG(cntr_value_delta * 1.0 / NULLIF(sample_interval_seconds, 0))", sql);
        Assert.Contains("MAX(cntr_value_delta * 1.0 / NULLIF(sample_interval_seconds, 0))", sql);
        Assert.Contains("sample_interval_seconds > 0", sql);

        /* A raw AVG/MAX of the delta is exactly the #3527 defect — pin its absence. */
        Assert.DoesNotContain("AVG(cntr_value_delta)", sql);
        Assert.DoesNotContain("MAX(cntr_value_delta)", sql);
    }

    /* ---------------- read 3: the baseline arm ---------------- */

    [Fact]
    public void BatchRequestBaseline_PopulationIsPerSecond_RestartSignatureStaysRaw()
    {
        var sql = SqlServerBaselineProvider.GetBaselineQuery(SqlServerMetricNames.BatchRequests);

        Assert.NotNull(sql);

        /* The baseline population is the per-second rate, in the detector's window unit. */
        Assert.Contains("cntr_value_delta * 1.0 / NULLIF(sample_interval_seconds, 0) AS v", sql);
        Assert.Contains("AVG(v) AS mean_val", sql);
        Assert.Contains("STDEV(v) AS stddev_val", sql);
        Assert.Contains("AND   sample_interval_seconds > 0", sql);

        /* The restart signature stays on the RAW delta: its > 1000 bar predates the division and
           marks a counter reset regardless of cadence. */
        Assert.Contains("LAG(cntr_value_delta) OVER (ORDER BY collection_time) AS prev_value", sql);
        Assert.Contains("WHERE NOT (cntr_value_delta = 0 AND ISNULL(prev_value, 0) > 1000)", sql);

        /* A raw AVG/STDEV of the delta is exactly the #3527 defect — pin its absence. */
        Assert.DoesNotContain("AVG(cntr_value_delta)", sql);
        Assert.DoesNotContain("STDEV(cntr_value_delta)", sql);
    }

    /* ---------------- the consistency constraint ---------------- */

    /// <summary>
    /// The whole fix is the three reads agreeing: the window statistic, the baseline population,
    /// and the fact value all divide by the measured interval, or the z-score compares across
    /// units. The fact collector's division happens in C# (delta / sample_interval_seconds after
    /// the SQL delivers the divisor), so its SQL is pinned for the divisor column and the
    /// interval filter; the two aggregate reads divide in the SQL itself.
    /// </summary>
    [Fact]
    public void AllThreeReads_FilterUnknowableRows()
    {
        var factSql = SqlServerFactCollector.PerfmonSql;
        var windowSql = SqlServerAnomalyDetector.BatchRequestWindowSql;
        var baselineSql = SqlServerBaselineProvider.GetBaselineQuery(SqlServerMetricNames.BatchRequests);

        Assert.Contains("sample_interval_seconds > 0", factSql);
        Assert.Contains("sample_interval_seconds > 0", windowSql);
        Assert.Contains("sample_interval_seconds > 0", baselineSql);
    }
}
