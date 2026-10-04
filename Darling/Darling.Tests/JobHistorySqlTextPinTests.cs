/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The job-history statement moved from the viewer into Darling.Storage. These pins hold the no-filter text to a saved
/// copy of the statement the viewer ran before the move, so the move cannot have changed the viewer's SQL, and pin the
/// end-instant clauses to the filter that adds them.
/// </summary>
public sealed class JobHistorySqlTextPinTests
{
    private static string Normalize(string sql) => Regex.Replace(sql, @"\s+", " ").Trim();

    private const string ViewerFleetSql = """
WITH svr AS (
    SELECT DISTINCT ON (server_id)
        server_id,
        utc_offset_minutes
    FROM server_properties
    WHERE utc_offset_minutes IS NOT NULL
    ORDER BY server_id, collection_time DESC
),
active_servers AS (
    SELECT DISTINCT jh.server_id
    FROM job_history AS jh
    WHERE jh.collection_time >= $2
    
),
server_offsets AS (
    SELECT
        a.server_id,
        reg.display_name,
        COALESCE(svr.utc_offset_minutes, 0) AS offset_minutes
    FROM active_servers AS a
    LEFT JOIN svr ON svr.server_id = a.server_id
    LEFT JOIN servers AS reg ON reg.server_id = a.server_id
),
top_by_server AS (
    SELECT
        so.server_id,
        COALESCE(so.display_name, top.server_name) AS server_name,
        top.instance_id,
        top.job_id,
        top.job_name,
        top.job_enabled,
        top.category_name,
        top.step_id,
        top.step_name,
        top.run_status,
        top.run_status_desc,
        top.run_datetime AS run_datetime_local,
        top.run_datetime - make_interval(mins => so.offset_minutes) AS approx_run_utc,
        top.run_duration_seconds,
        top.retries_attempted,
        top.message
    FROM server_offsets AS so
    CROSS JOIN LATERAL (
        SELECT
            jh.server_name,
            jh.instance_id,
            jh.job_id,
            jh.job_name,
            jh.job_enabled,
            jh.category_name,
            jh.step_id,
            jh.step_name,
            jh.run_status,
            jh.run_status_desc,
            jh.run_datetime,
            jh.run_duration_seconds,
            jh.retries_attempted,
            jh.message
        FROM job_history AS jh
        WHERE jh.server_id = so.server_id
        AND   jh.collection_time >= $2
        AND   jh.run_datetime >= $1 + make_interval(mins => so.offset_minutes) - interval '1 hour'
        ORDER BY jh.run_datetime DESC, jh.instance_id DESC
        LIMIT $3
    ) AS top
),
base AS (
    SELECT *
    FROM top_by_server
    ORDER BY approx_run_utc DESC, instance_id DESC
    LIMIT $3
),
job_stats AS (
    SELECT
        so.server_id,
        js.job_id,
        js.avg_success_duration,
        js.last_success_run_local
    FROM (SELECT DISTINCT server_id FROM base) AS b
    JOIN server_offsets AS so ON so.server_id = b.server_id
    CROSS JOIN LATERAL (
        SELECT
            jh.job_id,
            AVG(jh.run_duration_seconds) AS avg_success_duration,
            MAX(jh.run_datetime) AS last_success_run_local
        FROM job_history AS jh
        WHERE jh.server_id = so.server_id
        AND   jh.step_id = 0
        AND   jh.run_status = 1
        AND   jh.collection_time >= $2
        AND   jh.run_datetime >= $1 + make_interval(mins => so.offset_minutes)
        AND   jh.job_id IN (SELECT job_id FROM base WHERE base.server_id = so.server_id)
        GROUP BY jh.job_id
    ) AS js
)
SELECT
    base.server_id,
    base.server_name,
    base.instance_id,
    base.job_id,
    base.job_name,
    base.job_enabled,
    base.category_name,
    base.step_id,
    base.step_name,
    base.run_status,
    base.run_status_desc,
    base.run_datetime_local,
    base.run_duration_seconds,
    base.retries_attempted,
    base.message,
    job_stats.last_success_run_local,
    CASE
        WHEN base.step_id = 0
        AND  job_stats.avg_success_duration IS NOT NULL
        AND  job_stats.avg_success_duration > 0
        AND  base.run_duration_seconds > job_stats.avg_success_duration * 2
        AND  base.run_duration_seconds > 60
        THEN true
        ELSE false
    END AS is_long_running
FROM base
LEFT JOIN job_stats
    ON  job_stats.server_id = base.server_id
    AND job_stats.job_id = base.job_id
ORDER BY base.approx_run_utc DESC, base.instance_id DESC
""";

    private const string ViewerServerSql = """
WITH svr AS (
    SELECT DISTINCT ON (server_id)
        server_id,
        utc_offset_minutes
    FROM server_properties
    WHERE utc_offset_minutes IS NOT NULL
    ORDER BY server_id, collection_time DESC
),
active_servers AS (
    SELECT DISTINCT jh.server_id
    FROM job_history AS jh
    WHERE jh.collection_time >= $3
    AND   jh.server_id = $2
),
server_offsets AS (
    SELECT
        a.server_id,
        reg.display_name,
        COALESCE(svr.utc_offset_minutes, 0) AS offset_minutes
    FROM active_servers AS a
    LEFT JOIN svr ON svr.server_id = a.server_id
    LEFT JOIN servers AS reg ON reg.server_id = a.server_id
),
top_by_server AS (
    SELECT
        so.server_id,
        COALESCE(so.display_name, top.server_name) AS server_name,
        top.instance_id,
        top.job_id,
        top.job_name,
        top.job_enabled,
        top.category_name,
        top.step_id,
        top.step_name,
        top.run_status,
        top.run_status_desc,
        top.run_datetime AS run_datetime_local,
        top.run_datetime - make_interval(mins => so.offset_minutes) AS approx_run_utc,
        top.run_duration_seconds,
        top.retries_attempted,
        top.message
    FROM server_offsets AS so
    CROSS JOIN LATERAL (
        SELECT
            jh.server_name,
            jh.instance_id,
            jh.job_id,
            jh.job_name,
            jh.job_enabled,
            jh.category_name,
            jh.step_id,
            jh.step_name,
            jh.run_status,
            jh.run_status_desc,
            jh.run_datetime,
            jh.run_duration_seconds,
            jh.retries_attempted,
            jh.message
        FROM job_history AS jh
        WHERE jh.server_id = so.server_id
        AND   jh.collection_time >= $3
        AND   jh.run_datetime >= $1 + make_interval(mins => so.offset_minutes) - interval '1 hour'
        ORDER BY jh.run_datetime DESC, jh.instance_id DESC
        LIMIT $4
    ) AS top
),
base AS (
    SELECT *
    FROM top_by_server
    ORDER BY approx_run_utc DESC, instance_id DESC
    LIMIT $4
),
job_stats AS (
    SELECT
        so.server_id,
        js.job_id,
        js.avg_success_duration,
        js.last_success_run_local
    FROM (SELECT DISTINCT server_id FROM base) AS b
    JOIN server_offsets AS so ON so.server_id = b.server_id
    CROSS JOIN LATERAL (
        SELECT
            jh.job_id,
            AVG(jh.run_duration_seconds) AS avg_success_duration,
            MAX(jh.run_datetime) AS last_success_run_local
        FROM job_history AS jh
        WHERE jh.server_id = so.server_id
        AND   jh.step_id = 0
        AND   jh.run_status = 1
        AND   jh.collection_time >= $3
        AND   jh.run_datetime >= $1 + make_interval(mins => so.offset_minutes)
        AND   jh.job_id IN (SELECT job_id FROM base WHERE base.server_id = so.server_id)
        GROUP BY jh.job_id
    ) AS js
)
SELECT
    base.server_id,
    base.server_name,
    base.instance_id,
    base.job_id,
    base.job_name,
    base.job_enabled,
    base.category_name,
    base.step_id,
    base.step_name,
    base.run_status,
    base.run_status_desc,
    base.run_datetime_local,
    base.run_duration_seconds,
    base.retries_attempted,
    base.message,
    job_stats.last_success_run_local,
    CASE
        WHEN base.step_id = 0
        AND  job_stats.avg_success_duration IS NOT NULL
        AND  job_stats.avg_success_duration > 0
        AND  base.run_duration_seconds > job_stats.avg_success_duration * 2
        AND  base.run_duration_seconds > 60
        THEN true
        ELSE false
    END AS is_long_running
FROM base
LEFT JOIN job_stats
    ON  job_stats.server_id = base.server_id
    AND job_stats.job_id = base.job_id
ORDER BY base.approx_run_utc DESC, base.instance_id DESC
""";

    [Fact]
    public void NoFilter_FleetSql_EqualsTheViewersSavedStatement() =>
        Assert.Equal(Normalize(ViewerFleetSql), Normalize(DarlingJobHistoryReader.BuildJobHistorySql(scopedToServer: false, filter: null)));

    [Fact]
    public void NoFilter_ServerSql_EqualsTheViewersSavedStatement() =>
        Assert.Equal(Normalize(ViewerServerSql), Normalize(DarlingJobHistoryReader.BuildJobHistorySql(scopedToServer: true, filter: null)));

    [Fact]
    public void AnEmptyFilter_ChangesNothing() =>
        Assert.Equal(DarlingJobHistoryReader.BuildJobHistorySql(false, null), DarlingJobHistoryReader.BuildJobHistorySql(false, new JobHistoryFilter()));

    [Theory]
    [InlineData(false, 4)]
    [InlineData(true, 5)]
    public void UntilUtc_BoundsTheNewestRunsWidenedAndTheStatsExactly(bool scoped, int firstParam)
    {
        var sql = DarlingJobHistoryReader.BuildJobHistorySql(scoped, new JobHistoryFilter(JobName: "x", UntilUtc: new System.DateTime(2026, 3, 11)));
        var untilParam = "$" + (firstParam + 1).ToString(System.Globalization.CultureInfo.InvariantCulture);
        Assert.Contains($"jh.run_datetime < {untilParam} + make_interval(mins => so.offset_minutes) + interval '1 hour'", sql);
        Assert.Contains($"jh.run_datetime < {untilParam} + make_interval(mins => so.offset_minutes)\n", sql);
        Assert.Equal(2, Regex.Matches(sql, Regex.Escape(untilParam + " + make_interval")).Count);
    }
}
