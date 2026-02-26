/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

*/

SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET NUMERIC_ROUNDABORT OFF;
SET IMPLICIT_TRANSACTIONS OFF;
SET STATISTICS TIME, IO OFF;
GO

USE PerformanceMonitor;
GO

/*
Actionable reporting views
These views provide ready-to-query interfaces for common troubleshooting scenarios
*/

/*
Collection Health View - CRITICAL for detecting silent failures
Shows last successful collection time and failure rate for each collector
ALERT if any collector hasn't run successfully in 24 hours
*/
CREATE OR ALTER VIEW
    report.collection_health
AS
WITH
    collector_stats AS
(
    SELECT
        collector_name = cl.collector_name,
        last_success_time =
            MAX
            (
                CASE
                    WHEN cl.collection_status = N'SUCCESS'
                    THEN cl.collection_time
                    ELSE NULL
                END
            ),
        last_run_time = MAX(cl.collection_time),
        total_runs =
            COUNT_BIG
            (
                CASE
                    WHEN cl.collection_status NOT IN (N'CONFIG_CHANGE', N'TABLE_MISSING', N'TABLE_CREATED', N'SKIPPED')
                    THEN 1
                    ELSE NULL
                END
            ),
        failed_runs =
            SUM
            (
                CASE
                    WHEN cl.collection_status = N'ERROR'
                    THEN 1
                    ELSE 0
                END
            ),
        avg_duration_ms =
            AVG
            (
                CASE
                    WHEN cl.collection_status = N'SUCCESS'
                    THEN cl.duration_ms
                    ELSE NULL
                END
            ),
        total_rows_collected =
            SUM
            (
                CASE
                    WHEN cl.collection_status = N'SUCCESS'
                    THEN cl.rows_collected
                    ELSE 0
                END
            )
    FROM config.collection_log AS cl
    WHERE cl.collection_time >= DATEADD(DAY, -7, SYSDATETIME())
    GROUP BY
        cl.collector_name
),
    /*
    Count consecutive recent failures per collector.
    If the last 5+ runs are all errors, the collector is
    actively broken regardless of 7-day failure percentage.
    */
    recent_failures AS
(
    SELECT
        collector_name = r.collector_name,
        consecutive_failures =
            MIN
            (
                CASE
                    WHEN r.collection_status <> N'ERROR'
                    THEN r.rn
                    ELSE NULL
                END
            )
    FROM
    (
        SELECT
            cl.collector_name,
            cl.collection_status,
            rn = ROW_NUMBER() OVER
            (
                PARTITION BY
                    cl.collector_name
                ORDER BY
                    cl.collection_time DESC
            )
        FROM config.collection_log AS cl
        WHERE cl.collection_status NOT IN (N'CONFIG_CHANGE', N'TABLE_MISSING', N'TABLE_CREATED', N'SKIPPED')
        AND   cl.collection_time >= DATEADD(DAY, -7, SYSDATETIME())
    ) AS r
    WHERE r.rn <= 20
    GROUP BY
        r.collector_name
)
SELECT
    collector_name = cs.collector_name,
    last_success_time = cs.last_success_time,
    hours_since_success =
        DATEDIFF(HOUR, cs.last_success_time, SYSDATETIME()),
    last_run_time = cs.last_run_time,
    health_status =
        CASE
            WHEN cs.last_success_time IS NULL
            THEN N'NEVER_RUN'
            WHEN DATEDIFF(HOUR, cs.last_success_time, SYSDATETIME()) > 24
            THEN N'STALE'
            WHEN rf.consecutive_failures IS NULL
            THEN N'FAILING'
            WHEN rf.consecutive_failures > 5
            THEN N'FAILING'
            WHEN rf.consecutive_failures > 3
            THEN N'WARNING'
            WHEN cs.failed_runs * 100.0 / NULLIF(cs.total_runs, 0) > 50
            THEN N'FAILING'
            WHEN cs.failed_runs * 100.0 / NULLIF(cs.total_runs, 0) > 10
            THEN N'WARNING'
            ELSE N'HEALTHY'
        END,
    failure_rate_percent =
        CONVERT
        (
            decimal(5,2),
            cs.failed_runs * 100.0 / NULLIF(cs.total_runs, 0)
        ),
    total_runs_7d = cs.total_runs,
    failed_runs_7d = cs.failed_runs,
    avg_duration_ms = CONVERT(integer, cs.avg_duration_ms),
    total_rows_collected_7d = cs.total_rows_collected
FROM collector_stats AS cs
LEFT JOIN recent_failures AS rf
    ON cs.collector_name = rf.collector_name;
GO

/*
Top Waits View - Shows most impactful waits over last hour
*/
CREATE OR ALTER VIEW
    report.top_waits_last_hour
AS
WITH
    recent_waits AS
(
    SELECT
        ws.wait_type,
        ws.wait_time_ms_delta,
        ws.waiting_tasks_count_delta,
        ws.signal_wait_time_ms_delta,
        ws.collection_time,
        row_number = ROW_NUMBER() OVER
        (
            PARTITION BY
                ws.wait_type
            ORDER BY
                ws.collection_time DESC
        )
    FROM collect.wait_stats AS ws
    WHERE ws.collection_time >= DATEADD(HOUR, -1, SYSDATETIME())
    AND   ws.wait_time_ms_delta > 0
)
SELECT TOP (50)
    wait_type = rw.wait_type,
    wait_time_ms = rw.wait_time_ms_delta,
    wait_time_sec = rw.wait_time_ms_delta / 1000.0,
    waiting_tasks = rw.waiting_tasks_count_delta,
    signal_wait_ms = rw.signal_wait_time_ms_delta,
    resource_wait_ms = rw.wait_time_ms_delta - rw.signal_wait_time_ms_delta,
    avg_wait_ms_per_task =
        CASE
            WHEN rw.waiting_tasks_count_delta > 0
            THEN rw.wait_time_ms_delta / rw.waiting_tasks_count_delta
            ELSE 0
        END,
    last_seen = rw.collection_time
FROM recent_waits AS rw
WHERE rw.row_number = 1
ORDER BY
    rw.wait_time_ms_delta DESC;
GO


/*
Memory Pressure Events - Recent memory issues from ring buffers
*/
CREATE OR ALTER VIEW
    report.memory_pressure_events
AS
SELECT
    event_time = mpe.sample_time,
    notification = mpe.memory_notification,
    process_indicator = mpe.memory_indicators_process,
    system_indicator = mpe.memory_indicators_system,
    severity =
        CASE
            WHEN mpe.memory_indicators_process >= 3 OR mpe.memory_indicators_system >= 3
            THEN N'HIGH'
            WHEN mpe.memory_indicators_process >= 2 OR mpe.memory_indicators_system >= 2
            THEN N'MEDIUM'
            ELSE N'LOW'
        END
FROM collect.memory_pressure_events AS mpe
WHERE mpe.collection_time >= DATEADD(HOUR, -24, SYSDATETIME());
GO

/*
CPU Spikes - High CPU utilization events from cpu_utilization_stats
*/
CREATE OR ALTER VIEW
    report.cpu_spikes
AS
SELECT
    event_time = cus.sample_time,
    sql_server_cpu = cus.sqlserver_cpu_utilization,
    other_process_cpu = cus.other_process_cpu_utilization,
    total_cpu = cus.total_cpu_utilization,
    severity =
        CASE
            WHEN cus.sqlserver_cpu_utilization >= 90
            THEN N'CRITICAL'
            WHEN cus.sqlserver_cpu_utilization >= 80
            THEN N'HIGH'
            WHEN cus.sqlserver_cpu_utilization >= 60
            THEN N'MEDIUM'
            ELSE N'LOW'
        END
FROM collect.cpu_utilization_stats AS cus;
GO

/*
Blocking Summary - Recent blocking from blocked process reports
*/
CREATE OR ALTER VIEW
    report.blocking_summary
AS
SELECT TOP (100)
    event_time = bpx.event_time,
    blocked_process_xml = bpx.blocked_process_xml,
    collection_time = bpx.collection_time
FROM collect.blocked_process_xml AS bpx
WHERE bpx.collection_time >= DATEADD(HOUR, -24, SYSDATETIME())
ORDER BY
    bpx.event_time DESC;
GO

/*
Deadlock Summary - Recent deadlocks
*/
CREATE OR ALTER VIEW
    report.deadlock_summary
AS
SELECT TOP (100)
    event_time = dx.event_time,
    deadlock_xml = dx.deadlock_xml,
    collection_time = dx.collection_time
FROM collect.deadlock_xml AS dx
WHERE dx.collection_time >= DATEADD(HOUR, -24, SYSDATETIME())
ORDER BY
    dx.event_time DESC;
GO

/*
Daily Summary - One-page dashboard view
*/
CREATE OR ALTER VIEW
    report.daily_summary
AS
SELECT
    summary_date = CONVERT(date, SYSDATETIME()),
    total_wait_time_sec =
    (
        SELECT
            SUM(ws.wait_time_ms_delta) / 1000.0
        FROM collect.wait_stats AS ws
        WHERE ws.collection_time >= DATEADD(DAY, 0, CONVERT(date, SYSDATETIME()))
        AND   ws.wait_time_ms_delta > 0
    ),
    top_wait_type =
    (
        SELECT TOP (1)
            ws.wait_type
        FROM collect.wait_stats AS ws
        WHERE ws.collection_time >= DATEADD(DAY, 0, CONVERT(date, SYSDATETIME()))
        AND   ws.wait_time_ms_delta > 0
        ORDER BY
            ws.wait_time_ms_delta DESC
    ),
    expensive_queries_count =
    (
        SELECT
            COUNT_BIG(DISTINCT qs.query_hash)
        FROM collect.query_stats AS qs
        WHERE qs.collection_time >= DATEADD(DAY, 0, CONVERT(date, SYSDATETIME()))
    ),
    deadlock_count =
    (
        SELECT
            COUNT_BIG(*)
        FROM collect.deadlock_xml AS dx
        WHERE dx.collection_time >= DATEADD(DAY, 0, CONVERT(date, SYSDATETIME()))
    ),
    blocking_events_count =
    (
        SELECT
            COUNT_BIG(*)
        FROM collect.blocked_process_xml AS bpx
        WHERE bpx.collection_time >= DATEADD(DAY, 0, CONVERT(date, SYSDATETIME()))
    ),
    memory_pressure_events =
    (
        SELECT
            COUNT_BIG(*)
        FROM collect.memory_pressure_events AS mpe
        WHERE mpe.collection_time >= DATEADD(DAY, 0, CONVERT(date, SYSDATETIME()))
        AND   (mpe.memory_indicators_process >= 2 OR mpe.memory_indicators_system >= 2)
    ),
    high_cpu_events =
    (
        SELECT
            COUNT_BIG(*)
        FROM collect.cpu_utilization_stats AS cus
        WHERE cus.sqlserver_cpu_utilization >= 80
        AND   cus.collection_time >= DATEADD(DAY, 0, CONVERT(date, SYSDATETIME()))
    ),
    collectors_failing =
    (
        SELECT
            COUNT_BIG(*)
        FROM report.collection_health AS ch
        WHERE ch.health_status IN (N'STALE', N'FAILING', N'NEVER_RUN')
    ),
    overall_health =
        CASE
            WHEN EXISTS
            (
                SELECT
                    1/0
                FROM report.collection_health AS ch
                WHERE ch.health_status = N'STALE'
            )
            THEN N'MONITORING_STALE'
            WHEN EXISTS
            (
                SELECT
                    1/0
                FROM collect.deadlock_xml AS dx
                WHERE dx.collection_time >= DATEADD(HOUR, -1, SYSDATETIME())
            )
            THEN N'DEADLOCKS_ACTIVE'
            WHEN EXISTS
            (
                SELECT
                    1/0
                FROM collect.cpu_utilization_stats AS cus
                WHERE cus.sqlserver_cpu_utilization >= 90
                AND   cus.collection_time >= DATEADD(HOUR, -1, SYSDATETIME())
            )
            THEN N'CPU_CRITICAL'
            WHEN EXISTS
            (
                SELECT
                    1/0
                FROM collect.memory_pressure_events AS mpe
                WHERE mpe.memory_indicators_process >= 3
                AND   mpe.collection_time >= DATEADD(HOUR, -1, SYSDATETIME())
            )
            THEN N'MEMORY_CRITICAL'
            ELSE N'NORMAL'
        END;
GO

/*
Daily Summary V2 - Enhanced one-page dashboard view with actionable insights
Pivoted vertical format with sql_variant for type preservation

Improvements over V1:
- Top 5 wait types (not just #1)
- Throughput context (batch requests/sec)
- Comparison to yesterday (deltas)
- Worst query of the day by total CPU
- Trend indicators (UP/DOWN/SAME)
- Vertical format for better readability (no horizontal scrolling)
- sort_order column for logical grouping

Usage: SELECT sort_order, metric_name, metric_value FROM report.daily_summary_v2 ORDER BY sort_order;
*/
CREATE OR ALTER VIEW
    report.daily_summary_v2
AS
WITH
    today_boundary AS
(
    SELECT
        start_time = CONVERT(datetime2(7), CONVERT(date, SYSDATETIME())),
        end_time = SYSDATETIME()
),
    yesterday_boundary AS
(
    SELECT
        start_time = DATEADD(DAY, -1, CONVERT(datetime2(7), CONVERT(date, SYSDATETIME()))),
        end_time = CONVERT(datetime2(7), CONVERT(date, SYSDATETIME()))
),
    today_waits AS
(
    SELECT
        wait_type = ws.wait_type,
        total_wait_ms = SUM(ws.wait_time_ms_delta),
        wait_rank = ROW_NUMBER() OVER
            (
                ORDER BY
                    SUM(ws.wait_time_ms_delta) DESC
            )
    FROM collect.wait_stats AS ws
    CROSS JOIN today_boundary AS tb
    WHERE ws.collection_time >= tb.start_time
    AND   ws.wait_time_ms_delta > 0
    GROUP BY
        ws.wait_type
),
    total_wait_today AS
(
    SELECT
        total_ms = SUM(tw.total_wait_ms)
    FROM today_waits AS tw
),
    yesterday_stats AS
(
    SELECT
        wait_time_ms = ISNULL(SUM(ws.wait_time_ms_delta), 0),
        deadlock_count =
        (
            SELECT
                COUNT_BIG(*)
            FROM collect.deadlock_xml AS dx
            CROSS JOIN yesterday_boundary AS yb
            WHERE dx.collection_time >= yb.start_time
            AND   dx.collection_time < yb.end_time
        ),
        blocking_count =
        (
            SELECT
                COUNT_BIG(*)
            FROM collect.blocked_process_xml AS bpx
            CROSS JOIN yesterday_boundary AS yb
            WHERE bpx.collection_time >= yb.start_time
            AND   bpx.collection_time < yb.end_time
        ),
        high_cpu_count =
        (
            SELECT
                COUNT_BIG(*)
            FROM collect.cpu_utilization_stats AS cus
            CROSS JOIN yesterday_boundary AS yb
            WHERE cus.sqlserver_cpu_utilization >= 80
            AND   cus.collection_time >= yb.start_time
            AND   cus.collection_time < yb.end_time
        )
    FROM collect.wait_stats AS ws
    CROSS JOIN yesterday_boundary AS yb
    WHERE ws.collection_time >= yb.start_time
    AND   ws.collection_time < yb.end_time
    AND   ws.wait_time_ms_delta > 0
),
    today_stats AS
(
    SELECT
        deadlock_count =
        (
            SELECT
                COUNT_BIG(*)
            FROM collect.deadlock_xml AS dx
            CROSS JOIN today_boundary AS tb
            WHERE dx.collection_time >= tb.start_time
        ),
        blocking_count =
        (
            SELECT
                COUNT_BIG(*)
            FROM collect.blocked_process_xml AS bpx
            CROSS JOIN today_boundary AS tb
            WHERE bpx.collection_time >= tb.start_time
        ),
        high_cpu_count =
        (
            SELECT
                COUNT_BIG(*)
            FROM collect.cpu_utilization_stats AS cus
            CROSS JOIN today_boundary AS tb
            WHERE cus.sqlserver_cpu_utilization >= 80
            AND   cus.collection_time >= tb.start_time
        ),
        memory_pressure_count =
        (
            SELECT
                COUNT_BIG(*)
            FROM collect.memory_pressure_events AS mpe
            CROSS JOIN today_boundary AS tb
            WHERE mpe.collection_time >= tb.start_time
            AND   (mpe.memory_indicators_process >= 2 OR mpe.memory_indicators_system >= 2)
        )
),
    throughput AS
(
    SELECT
        avg_batch_requests_sec = AVG(ps.cntr_value_per_second),
        avg_compilations_sec = AVG(ps2.cntr_value_per_second)
    FROM collect.perfmon_stats AS ps
    LEFT JOIN collect.perfmon_stats AS ps2
      ON  ps2.collection_time = ps.collection_time
      AND ps2.counter_name = N'SQL Compilations/sec'
    WHERE ps.counter_name = N'Batch Requests/sec'
    AND   ps.collection_time >= DATEADD(HOUR, -1, SYSDATETIME())
),
    worst_query AS
(
    SELECT TOP (1)
        query_hash = qs.query_hash,
        total_cpu_ms = SUM(qs.total_worker_time_delta) / 1000,
        execution_count = SUM(qs.execution_count_delta),
        avg_cpu_ms = SUM(qs.total_worker_time_delta) / NULLIF(SUM(qs.execution_count_delta), 0) / 1000,
        database_name = MAX(qs.database_name)
    FROM collect.query_stats AS qs
    CROSS JOIN today_boundary AS tb
    WHERE qs.collection_time >= tb.start_time
    AND   qs.total_worker_time_delta > 0
    GROUP BY
        qs.query_hash
    ORDER BY
        SUM(qs.total_worker_time_delta) DESC
),
    collectors_failing AS
(
    SELECT
        failing_count = COUNT_BIG(*)
    FROM report.collection_health AS ch
    WHERE ch.health_status IN (N'STALE', N'FAILING', N'NEVER_RUN')
),
    health_status AS
(
    SELECT
        overall_health =
            CASE
                WHEN EXISTS
                (
                    SELECT
                        1/0
                    FROM report.collection_health AS ch
                    WHERE ch.health_status = N'STALE'
                )
                THEN N'MONITORING_STALE'
                WHEN ts.deadlock_count > 0
                AND  EXISTS
                (
                    SELECT
                        1/0
                    FROM collect.deadlock_xml AS dx
                    WHERE dx.collection_time >= DATEADD(HOUR, -1, SYSDATETIME())
                )
                THEN N'DEADLOCKS_ACTIVE'
                WHEN EXISTS
                (
                    SELECT
                        1/0
                    FROM collect.cpu_utilization_stats AS cus
                    WHERE cus.sqlserver_cpu_utilization >= 90
                    AND   cus.collection_time >= DATEADD(HOUR, -1, SYSDATETIME())
                )
                THEN N'CPU_CRITICAL'
                WHEN EXISTS
                (
                    SELECT
                        1/0
                    FROM collect.memory_pressure_events AS mpe
                    WHERE mpe.memory_indicators_process >= 3
                    AND   mpe.collection_time >= DATEADD(HOUR, -1, SYSDATETIME())
                )
                THEN N'MEMORY_CRITICAL'
                WHEN ts.blocking_count > 10
                THEN N'BLOCKING_ELEVATED'
                ELSE N'NORMAL'
            END
    FROM today_stats AS ts
),
    pivoted AS
(
    /*
    Pivoted output: sort_order, metric_name, metric_value (using sql_variant to preserve original types)
    Sort order groups: 1-3 status, 10-14 waits, 20-21 throughput, 30-32 deadlocks,
                       40-42 blocking, 50-52 CPU, 60 memory, 70-74 worst query
    */
    SELECT
        sort_order = 1,
        metric_name = N'summary_date',
        metric_value = CONVERT(sql_variant, CONVERT(date, SYSDATETIME()))
    FROM today_stats AS ts

    UNION ALL

    SELECT
        sort_order = 2,
        metric_name = N'overall_health',
        metric_value = CONVERT(sql_variant, hs.overall_health)
    FROM health_status AS hs

    UNION ALL

    SELECT
        sort_order = 3,
        metric_name = N'collectors_failing',
        metric_value = CONVERT(sql_variant, cf.failing_count)
    FROM collectors_failing AS cf

    UNION ALL

    SELECT
        sort_order = 10,
        metric_name = N'top_wait_1',
        metric_value = CONVERT(sql_variant,
            tw.wait_type + N' (' +
            CONVERT(nvarchar(10), CONVERT(decimal(5,1), tw.total_wait_ms * 100.0 / NULLIF(twt.total_ms, 0))) + N'%)')
    FROM today_waits AS tw
    CROSS JOIN total_wait_today AS twt
    WHERE tw.wait_rank = 1

    UNION ALL

    SELECT
        sort_order = 11,
        metric_name = N'top_wait_2',
        metric_value = CONVERT(sql_variant,
            tw.wait_type + N' (' +
            CONVERT(nvarchar(10), CONVERT(decimal(5,1), tw.total_wait_ms * 100.0 / NULLIF(twt.total_ms, 0))) + N'%)')
    FROM today_waits AS tw
    CROSS JOIN total_wait_today AS twt
    WHERE tw.wait_rank = 2

    UNION ALL

    SELECT
        sort_order = 12,
        metric_name = N'top_wait_3',
        metric_value = CONVERT(sql_variant,
            tw.wait_type + N' (' +
            CONVERT(nvarchar(10), CONVERT(decimal(5,1), tw.total_wait_ms * 100.0 / NULLIF(twt.total_ms, 0))) + N'%)')
    FROM today_waits AS tw
    CROSS JOIN total_wait_today AS twt
    WHERE tw.wait_rank = 3

    UNION ALL

    SELECT
        sort_order = 13,
        metric_name = N'top_wait_4',
        metric_value = CONVERT(sql_variant,
            tw.wait_type + N' (' +
            CONVERT(nvarchar(10), CONVERT(decimal(5,1), tw.total_wait_ms * 100.0 / NULLIF(twt.total_ms, 0))) + N'%)')
    FROM today_waits AS tw
    CROSS JOIN total_wait_today AS twt
    WHERE tw.wait_rank = 4

    UNION ALL

    SELECT
        sort_order = 14,
        metric_name = N'top_wait_5',
        metric_value = CONVERT(sql_variant,
            tw.wait_type + N' (' +
            CONVERT(nvarchar(10), CONVERT(decimal(5,1), tw.total_wait_ms * 100.0 / NULLIF(twt.total_ms, 0))) + N'%)')
    FROM today_waits AS tw
    CROSS JOIN total_wait_today AS twt
    WHERE tw.wait_rank = 5

    UNION ALL

    SELECT
        sort_order = 20,
        metric_name = N'batch_requests_per_sec',
        metric_value = CONVERT(sql_variant, t.avg_batch_requests_sec)
    FROM throughput AS t

    UNION ALL

    SELECT
        sort_order = 21,
        metric_name = N'compilations_per_sec',
        metric_value = CONVERT(sql_variant, t.avg_compilations_sec)
    FROM throughput AS t

    UNION ALL

    SELECT
        sort_order = 30,
        metric_name = N'deadlocks_today',
        metric_value = CONVERT(sql_variant, ts.deadlock_count)
    FROM today_stats AS ts

    UNION ALL

    SELECT
        sort_order = 31,
        metric_name = N'deadlocks_yesterday',
        metric_value = CONVERT(sql_variant, ys.deadlock_count)
    FROM yesterday_stats AS ys

    UNION ALL

    SELECT
        sort_order = 32,
        metric_name = N'deadlock_trend',
        metric_value = CONVERT(sql_variant,
            CASE
                WHEN ts.deadlock_count > ys.deadlock_count THEN N'UP'
                WHEN ts.deadlock_count < ys.deadlock_count THEN N'DOWN'
                ELSE N'SAME'
            END)
    FROM today_stats AS ts
    CROSS JOIN yesterday_stats AS ys

    UNION ALL

    SELECT
        sort_order = 40,
        metric_name = N'blocking_events_today',
        metric_value = CONVERT(sql_variant, ts.blocking_count)
    FROM today_stats AS ts

    UNION ALL

    SELECT
        sort_order = 41,
        metric_name = N'blocking_events_yesterday',
        metric_value = CONVERT(sql_variant, ys.blocking_count)
    FROM yesterday_stats AS ys

    UNION ALL

    SELECT
        sort_order = 42,
        metric_name = N'blocking_trend',
        metric_value = CONVERT(sql_variant,
            CASE
                WHEN ts.blocking_count > ys.blocking_count THEN N'UP'
                WHEN ts.blocking_count < ys.blocking_count THEN N'DOWN'
                ELSE N'SAME'
            END)
    FROM today_stats AS ts
    CROSS JOIN yesterday_stats AS ys

    UNION ALL

    SELECT
        sort_order = 50,
        metric_name = N'high_cpu_events_today',
        metric_value = CONVERT(sql_variant, ts.high_cpu_count)
    FROM today_stats AS ts

    UNION ALL

    SELECT
        sort_order = 51,
        metric_name = N'high_cpu_events_yesterday',
        metric_value = CONVERT(sql_variant, ys.high_cpu_count)
    FROM yesterday_stats AS ys

    UNION ALL

    SELECT
        sort_order = 52,
        metric_name = N'cpu_trend',
        metric_value = CONVERT(sql_variant,
            CASE
                WHEN ts.high_cpu_count > ys.high_cpu_count THEN N'UP'
                WHEN ts.high_cpu_count < ys.high_cpu_count THEN N'DOWN'
                ELSE N'SAME'
            END)
    FROM today_stats AS ts
    CROSS JOIN yesterday_stats AS ys

    UNION ALL

    SELECT
        sort_order = 60,
        metric_name = N'memory_pressure_events',
        metric_value = CONVERT(sql_variant, ts.memory_pressure_count)
    FROM today_stats AS ts

    UNION ALL

    SELECT
        sort_order = 70,
        metric_name = N'worst_query_hash',
        metric_value = CONVERT(sql_variant, wq.query_hash)
    FROM worst_query AS wq

    UNION ALL

    SELECT
        sort_order = 71,
        metric_name = N'worst_query_total_cpu_ms',
        metric_value = CONVERT(sql_variant, wq.total_cpu_ms)
    FROM worst_query AS wq

    UNION ALL

    SELECT
        sort_order = 72,
        metric_name = N'worst_query_executions',
        metric_value = CONVERT(sql_variant, wq.execution_count)
    FROM worst_query AS wq

    UNION ALL

    SELECT
        sort_order = 73,
        metric_name = N'worst_query_avg_cpu_ms',
        metric_value = CONVERT(sql_variant, wq.avg_cpu_ms)
    FROM worst_query AS wq

    UNION ALL

    SELECT
        sort_order = 74,
        metric_name = N'worst_query_database',
        metric_value = CONVERT(sql_variant, wq.database_name)
    FROM worst_query AS wq
)
SELECT
    p.sort_order,
    p.metric_name,
    p.metric_value
FROM pivoted AS p;
GO

PRINT 'Reporting views created successfully';
PRINT '';
PRINT 'Available report views:';
GO

/*
Server Info View - shows current server configuration with uptime
Provides convenient access to most recent server information from history table
*/
CREATE OR ALTER VIEW
    config.server_info
AS
SELECT TOP (1)
    server_name = h.server_name,
    instance_name = h.instance_name,
    sql_version = h.sql_version,
    edition = h.edition,
    physical_memory_mb = h.physical_memory_mb,
    cpu_count = h.cpu_count,
    environment_type = h.environment_type,
    sqlserver_start_time = h.sqlserver_start_time,
    uptime_days = DATEDIFF(DAY, h.sqlserver_start_time, SYSDATETIME()),
    uptime_hours = DATEDIFF(HOUR, h.sqlserver_start_time, SYSDATETIME()),
    last_updated = h.collection_time
FROM config.server_info_history AS h
ORDER BY
    h.collection_time DESC;
GO

PRINT 'Created config.server_info view (shows current server configuration)';
GO

/*
Configuration History View - Server Configuration Changes
Shows instance-level configuration changes over time with old/new values
Only shows actual changes, excludes initial baseline
*/
CREATE OR ALTER VIEW
    report.server_configuration_changes
AS
WITH
    ranked_changes AS
(
    SELECT
        h.collection_time,
        h.configuration_id,
        h.configuration_name,
        h.value_configured,
        h.value_in_use,
        h.description,
        h.is_dynamic,
        h.is_advanced,
        previous_value_configured =
            LAG(h.value_configured, 1, h.value_configured) OVER
            (
                PARTITION BY
                    h.configuration_name
                ORDER BY
                    h.collection_time
            ),
        previous_value_in_use =
            LAG(h.value_in_use, 1, h.value_in_use) OVER
            (
                PARTITION BY
                    h.configuration_name
                ORDER BY
                    h.collection_time
            ),
        change_number =
            ROW_NUMBER() OVER
            (
                PARTITION BY
                    h.configuration_name
                ORDER BY
                    h.collection_time
            )
    FROM config.server_configuration_history AS h
)
SELECT
    change_time = rc.collection_time,
    configuration_name = rc.configuration_name,
    old_value_configured = rc.previous_value_configured,
    new_value_configured = rc.value_configured,
    old_value_in_use = rc.previous_value_in_use,
    new_value_in_use = rc.value_in_use,
    requires_restart =
        CASE
            WHEN rc.is_dynamic = 0 AND rc.value_configured != rc.value_in_use
            THEN 1
            ELSE 0
        END,
    change_description =
        CASE
            WHEN rc.value_configured != rc.previous_value_configured
            THEN N'Configured value changed from ' + CONVERT(nvarchar(50), rc.previous_value_configured) + N' to ' + CONVERT(nvarchar(50), rc.value_configured)
            WHEN rc.value_in_use != rc.previous_value_in_use
            THEN N'In-use value changed from ' + CONVERT(nvarchar(50), rc.previous_value_in_use) + N' to ' + CONVERT(nvarchar(50), rc.value_in_use)
            ELSE N'Value unchanged'
        END,
    description = rc.description,
    is_dynamic = rc.is_dynamic,
    is_advanced = rc.is_advanced
FROM ranked_changes AS rc
WHERE rc.change_number > 1
AND   rc.previous_value_configured IS NOT NULL
AND   rc.previous_value_in_use IS NOT NULL
AND
(
    rc.value_configured != rc.previous_value_configured
    OR rc.value_in_use != rc.previous_value_in_use
);
GO

PRINT 'Created report.server_configuration_changes view (shows instance configuration changes over time)';
GO

/*
Configuration History View - Database Configuration Changes
Shows database-level configuration changes over time with old/new values
Only shows actual changes, excludes initial baseline
*/
CREATE OR ALTER VIEW
    report.database_configuration_changes
AS
WITH
    ranked_changes AS
(
    SELECT
        h.collection_time,
        h.database_id,
        h.database_name,
        h.setting_type,
        h.setting_name,
        h.setting_value,
        previous_value =
            LAG(h.setting_value, 1) OVER
            (
                PARTITION BY
                    h.database_id,
                    h.setting_type,
                    h.setting_name
                ORDER BY
                    h.collection_time
            ),
        change_number =
            ROW_NUMBER() OVER
            (
                PARTITION BY
                    h.database_id,
                    h.setting_type,
                    h.setting_name
                ORDER BY
                    h.collection_time
            )
    FROM config.database_configuration_history AS h
)
SELECT
    change_time = rc.collection_time,
    database_name = rc.database_name,
    setting_type = rc.setting_type,
    setting_name = rc.setting_name,
    old_value = CONVERT(nvarchar(256), rc.previous_value),
    new_value = CONVERT(nvarchar(256), rc.setting_value),
    change_description =
        CASE
            WHEN rc.previous_value IS NULL AND rc.setting_value IS NOT NULL
            THEN N'Set to: ' + CONVERT(nvarchar(256), rc.setting_value)
            WHEN rc.previous_value IS NOT NULL AND rc.setting_value IS NULL
            THEN N'Cleared (was: ' + CONVERT(nvarchar(256), rc.previous_value) + N')'
            ELSE N'Changed from ' + CONVERT(nvarchar(256), rc.previous_value) + N' to ' + CONVERT(nvarchar(256), rc.setting_value)
        END
FROM ranked_changes AS rc
WHERE rc.change_number > 1;
GO

PRINT 'Created report.database_configuration_changes view (shows database configuration changes over time)';
GO

/*
Configuration History View - Trace Flag Changes
Shows trace flag changes over time (enabled/disabled events)
Only shows actual changes, excludes initial baseline
*/
CREATE OR ALTER VIEW
    report.trace_flag_changes
AS
WITH
    ranked_changes AS
(
    SELECT
        h.collection_time,
        h.trace_flag,
        h.status,
        h.is_global,
        h.is_session,
        previous_status =
            LAG(h.status, 1) OVER
            (
                PARTITION BY
                    h.trace_flag,
                    h.is_global
                ORDER BY
                    h.collection_time
            ),
        change_number =
            ROW_NUMBER() OVER
            (
                PARTITION BY
                    h.trace_flag,
                    h.is_global
                ORDER BY
                    h.collection_time
            )
    FROM config.trace_flags_history AS h
)
SELECT
    change_time = rc.collection_time,
    trace_flag = rc.trace_flag,
    previous_status = rc.previous_status,
    new_status = rc.status,
    scope =
        CASE
            WHEN rc.is_global = 1 THEN N'GLOBAL'
            WHEN rc.is_session = 1 THEN N'SESSION'
            ELSE N'UNKNOWN'
        END,
    change_description =
        CASE
            WHEN rc.previous_status = N'OFF' AND rc.status = N'ON'
            THEN N'Trace flag ' + CONVERT(nvarchar(10), rc.trace_flag) + N' ENABLED'
            WHEN rc.previous_status = N'ON' AND rc.status = N'OFF'
            THEN N'Trace flag ' + CONVERT(nvarchar(10), rc.trace_flag) + N' DISABLED'
            ELSE N'Status unchanged'
        END,
    is_global = rc.is_global,
    is_session = rc.is_session
FROM ranked_changes AS rc
WHERE rc.change_number > 1;
GO

PRINT 'Created report.trace_flag_changes view (shows trace flag changes over time)';
GO


/*
=============================================================================
LATCH CONTENTION ANALYSIS
Shows top latch classes causing contention - internal SQL Server synchronization
Latch = lightweight synchronization primitive for in-memory structures
=============================================================================
*/
CREATE OR ALTER VIEW
    report.top_latch_contention
AS
WITH
    recent_latches AS
(
    SELECT
        latch_class,
        wait_time_ms = SUM(wait_time_ms_delta),
        waiting_requests = SUM(waiting_requests_count_delta),
        collection_time = MAX(collection_time)
    FROM collect.latch_stats
    WHERE collection_time >= DATEADD(HOUR, -1, SYSDATETIME())
    GROUP BY
        latch_class
)
SELECT TOP (20)
    rl.latch_class,
    rl.wait_time_ms,
    wait_time_sec = rl.wait_time_ms / 1000.0,
    rl.waiting_requests,
    avg_wait_ms_per_request =
        CASE
            WHEN rl.waiting_requests > 0
            THEN rl.wait_time_ms / rl.waiting_requests
            ELSE 0
        END,
    severity =
        CASE
            WHEN rl.wait_time_ms > 10000 THEN N'HIGH'
            WHEN rl.wait_time_ms > 5000 THEN N'MEDIUM'
            ELSE N'LOW'
        END,
    /*
    Latch class descriptions from Microsoft documentation:
    https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-latch-stats-transact-sql
    */
    latch_description =
        CASE rl.latch_class
            /*Buffer pool latches*/
            WHEN N'BUFFER'
            THEN N'Synchronize short term access to database pages. Required before reading or modifying any database page.'
            WHEN N'BUFFER_POOL_GROW'
            THEN N'Internal buffer manager synchronization during buffer pool grow operations.'
            /*Allocation latches*/
            WHEN N'ALLOC_CREATE_RINGBUF'
            THEN N'Initialize synchronization of allocation ring buffer creation.'
            WHEN N'ALLOC_CREATE_FREESPACE_CACHE'
            THEN N'Initialize synchronization of internal free space caches for heaps.'
            WHEN N'ALLOC_CACHE_MANAGER'
            THEN N'Synchronize internal coherency tests.'
            WHEN N'ALLOC_FREESPACE_CACHE'
            THEN N'Synchronize access to cache of pages with available space for heaps and BLOBs.'
            WHEN N'ALLOC_EXTENT_CACHE'
            THEN N'Synchronize access to cache of extents containing unallocated pages.'
            /*Access methods latches*/
            WHEN N'ACCESS_METHODS_DATASET_PARENT'
            THEN N'Synchronize child dataset access to parent dataset during parallel operations.'
            WHEN N'ACCESS_METHODS_HOBT_FACTORY'
            THEN N'Synchronize access to an internal hash table.'
            WHEN N'ACCESS_METHODS_HOBT'
            THEN N'Synchronize access to the in-memory representation of a HoBt.'
            WHEN N'ACCESS_METHODS_HOBT_COUNT'
            THEN N'Synchronize access to HoBt page and row counters.'
            WHEN N'ACCESS_METHODS_HOBT_VIRTUAL_ROOT'
            THEN N'Synchronize access to root page abstraction of an internal B-tree.'
            WHEN N'ACCESS_METHODS_CACHE_ONLY_HOBT_ALLOC'
            THEN N'Synchronize worktable access.'
            WHEN N'ACCESS_METHODS_BULK_ALLOC'
            THEN N'Synchronize access within bulk allocators.'
            WHEN N'ACCESS_METHODS_SCAN_RANGE_GENERATOR'
            THEN N'Synchronize access to range generator during parallel scans.'
            WHEN N'ACCESS_METHODS_KEY_RANGE_GENERATOR'
            THEN N'Synchronize access to read-ahead operations during key range parallel scans.'
            /*Append-only storage (version store) latches*/
            WHEN N'APPEND_ONLY_STORAGE_INSERT_POINT'
            THEN N'Synchronize inserts in fast append-only storage units.'
            WHEN N'APPEND_ONLY_STORAGE_FIRST_ALLOC'
            THEN N'Synchronize first allocation for an append-only storage unit.'
            WHEN N'APPEND_ONLY_STORAGE_UNIT_MANAGER'
            THEN N'Internal data structure access synchronization in fast append-only storage unit manager.'
            WHEN N'APPEND_ONLY_STORAGE_MANAGER'
            THEN N'Synchronize shrink operations in fast append-only storage unit manager.'
            /*Backup latches*/
            WHEN N'BACKUP_RESULT_SET'
            THEN N'Synchronize parallel backup result sets.'
            WHEN N'BACKUP_TAPE_POOL'
            THEN N'Synchronize backup tape pools.'
            WHEN N'BACKUP_LOG_REDO'
            THEN N'Synchronize backup log redo operations.'
            WHEN N'BACKUP_INSTANCE_ID'
            THEN N'Synchronize generation of instance IDs for backup performance monitor counters.'
            WHEN N'BACKUP_MANAGER'
            THEN N'Synchronize the internal backup manager.'
            WHEN N'BACKUP_MANAGER_DIFFERENTIAL'
            THEN N'Synchronize differential backup operations with DBCC.'
            WHEN N'BACKUP_OPERATION'
            THEN N'Internal data structure synchronization within a backup operation.'
            WHEN N'BACKUP_FILE_HANDLE'
            THEN N'Synchronize file open operations during a restore operation.'
            /*Database checkpoint*/
            WHEN N'DATABASE_CHECKPOINT'
            THEN N'Serialize checkpoints within a database.'
            /*File and filegroup latches*/
            WHEN N'FCB'
            THEN N'Synchronize access to the file control block.'
            WHEN N'FGCB_ALLOC'
            THEN N'Synchronize access to round robin allocation information within a filegroup.'
            WHEN N'FGCB_ADD_REMOVE'
            THEN N'Synchronize access to filegroups for add, drop, grow, and shrink file operations.'
            /*Service Broker*/
            WHEN N'SERVICE_BROKER_WAITFOR_MANAGER'
            THEN N'Synchronize an instance level map of waiter queues.'
            /*Internal use only latches - common ones*/
            WHEN N'LOG_MANAGER'
            THEN N'Internal use only.'
            WHEN N'TRACE_CONTROLLER'
            THEN N'Internal use only.'
            WHEN N'DATABASE_MIRRORING_CONNECTION'
            THEN N'Internal use only.'
            WHEN N'TRANSACTION_DISTRIBUTED_MARK'
            THEN N'Internal use only.'
            WHEN N'NESTING_TRANSACTION_READONLY'
            THEN N'Internal use only.'
            WHEN N'NESTING_TRANSACTION_FULL'
            THEN N'Internal use only.'
            WHEN N'VERSIONING_TRANSACTION'
            THEN N'Internal use only.'
            WHEN N'VERSIONING_TRANSACTION_LIST'
            THEN N'Internal use only.'
            WHEN N'VERSIONING_TRANSACTION_CHAIN'
            THEN N'Internal use only.'
            WHEN N'VERSIONING_STATE'
            THEN N'Internal use only.'
            WHEN N'VERSIONING_STATE_CHANGE'
            THEN N'Internal use only.'
            WHEN N'DBCC_PERF'
            THEN N'Synchronize internal performance monitor counters.'
            /*Catch-all*/
            ELSE N'Internal use only.'
        END,
    recommendation =
        CASE
            /*Page I/O latches indicate I/O subsystem issues*/
            WHEN rl.latch_class LIKE N'PAGEIOLATCH%'
            THEN N'I/O bottleneck - check disk latency, add memory to reduce I/O'
            /*Page latches indicate hot pages or tempdb contention*/
            WHEN rl.latch_class LIKE N'PAGELATCH%'
            THEN N'Page contention - check for hot pages, tempdb contention, or PFS/GAM/SGAM issues'
            /*Buffer contention*/
            WHEN rl.latch_class = N'BUFFER'
            THEN N'Buffer pool contention - check for memory pressure or hot pages'
            /*Access methods indicate scan/allocation issues*/
            WHEN rl.latch_class LIKE N'ACCESS_METHODS%'
            THEN N'Index/heap access contention - check for parallel scan issues or allocation hotspots'
            /*Allocation latches indicate growth issues*/
            WHEN rl.latch_class LIKE N'ALLOC%'
            THEN N'Allocation contention - check for autogrow events, consider pre-sizing files'
            /*Filegroup operations*/
            WHEN rl.latch_class IN (N'FGCB_ADD_REMOVE', N'FCB')
            THEN N'File/filegroup operations - check for file growth or filegroup modifications'
            /*Log manager*/
            WHEN rl.latch_class IN (N'LOG_MANAGER', N'LOGCACHE_ACCESS')
            THEN N'Transaction log contention - check log disk performance, consider faster log storage'
            /*Version store/tempdb*/
            WHEN rl.latch_class LIKE N'APPEND_ONLY_STORAGE%'
            THEN N'Version store contention - check tempdb performance, add tempdb files'
            /*DBCC operations*/
            WHEN rl.latch_class = N'DBCC_MULTIOBJECT_SCANNER'
            THEN N'DBCC operation running - normal during consistency checks'
            ELSE N'Review latch class documentation for specific guidance'
        END,
    last_seen = rl.collection_time
FROM recent_latches AS rl
WHERE rl.wait_time_ms > 0
ORDER BY
    rl.wait_time_ms DESC;
GO

/*
=============================================================================
SPINLOCK CONTENTION ANALYSIS
Shows spinlocks with contention - severity based on spins/collision and backoffs
=============================================================================
*/
CREATE OR ALTER VIEW
    report.top_spinlock_contention
AS
WITH
    recent_spinlocks AS
(
    SELECT
        spinlock_name,
        collisions = SUM(collisions_delta),
        spins = SUM(spins_delta),
        spins_per_collision =
            CASE
                WHEN SUM(collisions_delta) > 0
                THEN SUM(spins_delta) / SUM(collisions_delta)
                ELSE 0
            END,
        sleep_time = SUM(sleep_time_delta),
        backoffs = SUM(backoffs_delta),
        collection_time = MAX(collection_time)
    FROM collect.spinlock_stats
    WHERE collection_time >= DATEADD(HOUR, -1, SYSDATETIME())
    GROUP BY
        spinlock_name
)
SELECT TOP (20)
    rs.spinlock_name,
    rs.collisions,
    rs.spins,
    rs.spins_per_collision,
    rs.backoffs,
    sleep_time_ms = rs.sleep_time,
    /*
    Spinlock descriptions from Microsoft documentation:
    https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-spinlock-stats-transact-sql
    Note: Most spinlocks are documented as "Internal use only"
    */
    spinlock_description =
        CASE rs.spinlock_name
            WHEN N'BACKUP_CTX'
            THEN N'Protects access to list of pages involved in I/O while a backup is happening. High spins during long checkpoints or lazywriter activity during backup.'
            WHEN N'DBTABLE'
            THEN N'Controls access to an in-memory data structure for every database containing database properties.'
            WHEN N'DP_LIST'
            THEN N'Controls access to the list of dirty pages for a database with indirect checkpoint enabled.'
            WHEN N'LOCK_HASH'
            THEN N'Protects access to lock manager hash table storing information about locks held in a database.'
            WHEN N'LOCK_RW_SECURITY_CACHE'
            THEN N'Protects cache entries related to security tokens and access checks. High spins if TokenAndPermUserStore grows continuously.'
            WHEN N'MUTEX'
            THEN N'Protects cache entries related to security tokens and access checks (SQL Server up to 2012).'
            WHEN N'SECURITY_CACHE'
            THEN N'Protects cache entries related to security tokens and access checks (SQL Server 2014-2016 CU1).'
            WHEN N'SOS_CACHESTORE'
            THEN N'Synchronizes access to various in-memory caches such as plan cache or temp table cache.'
            ELSE N'Internal use only.'
        END,
    last_seen = rs.collection_time
FROM recent_spinlocks AS rs
WHERE rs.collisions > 0
ORDER BY
    rs.spins DESC;
GO

/*
=============================================================================
TEMPDB PRESSURE ANALYSIS
Shows TempDB version store pressure and allocation patterns
=============================================================================
*/
CREATE OR ALTER VIEW
    report.tempdb_pressure
AS
SELECT TOP (1)
    collection_time,
    user_objects_mb = user_object_reserved_mb,
    internal_objects_mb = internal_object_reserved_mb,
    version_store_mb = version_store_reserved_mb,
    total_used_mb = total_reserved_mb,
    unallocated_mb,
    version_store_percent =
        CASE
            WHEN total_reserved_mb > 0
            THEN CONVERT(decimal(5,2), version_store_reserved_mb * 100.0 / total_reserved_mb)
            ELSE 0
        END,
    pressure_level =
        CASE
            WHEN version_store_reserved_mb > 5000 THEN N'CRITICAL - Version store > 5GB'
            WHEN version_store_reserved_mb > 2000 THEN N'HIGH - Version store > 2GB'
            WHEN version_store_reserved_mb > 1000 THEN N'MEDIUM - Version store > 1GB'
            WHEN unallocated_mb < 100 THEN N'MEDIUM - Low free space'
            ELSE N'NORMAL'
        END,
    recommendation =
        CASE
            WHEN version_store_reserved_mb > 1000 THEN N'Check for long-running transactions, snapshot isolation usage'
            WHEN unallocated_mb < 100 THEN N'Consider increasing TempDB file sizes'
            WHEN internal_object_reserved_mb > user_object_reserved_mb * 2 THEN N'High internal object usage - check for sorts, hash operations'
            ELSE N'TempDB usage is within normal range'
        END
FROM collect.tempdb_stats
ORDER BY
    collection_time DESC;
GO

/*
=============================================================================
PLAN CACHE BLOAT ANALYSIS
Shows plan cache composition and single-use plan bloat
=============================================================================
*/
CREATE OR ALTER VIEW
    report.plan_cache_bloat
AS
WITH
    aggregated_cache AS
(
    SELECT TOP (1)
        collection_time,
        total_plan_count = SUM(total_plans),
        single_use_plan_count = SUM(single_use_plans),
        total_size_mb = SUM(total_size_mb),
        single_use_size_mb = SUM(single_use_size_mb)
    FROM collect.plan_cache_stats
    GROUP BY
        collection_time
    ORDER BY
        collection_time DESC
)
SELECT
    collection_time,
    total_plans = total_plan_count,
    single_use_plans = single_use_plan_count,
    single_use_percent =
        CONVERT(decimal(5,2), single_use_plan_count * 100.0 / NULLIF(total_plan_count, 0)),
    total_cache_mb = total_size_mb,
    single_use_mb = single_use_size_mb,
    single_use_cache_percent =
        CONVERT(decimal(5,2), single_use_size_mb * 100.0 / NULLIF(total_size_mb, 0)),
    bloat_level =
        CASE
            WHEN single_use_plan_count * 100.0 / NULLIF(total_plan_count, 0) > 50 THEN N'CRITICAL'
            WHEN single_use_plan_count * 100.0 / NULLIF(total_plan_count, 0) > 30 THEN N'HIGH'
            WHEN single_use_plan_count * 100.0 / NULLIF(total_plan_count, 0) > 20 THEN N'MEDIUM'
            ELSE N'NORMAL'
        END,
    recommendation =
        CASE
            WHEN single_use_plan_count * 100.0 / NULLIF(total_plan_count, 0) > 20
            THEN N'Check for unparameterized queries/Consider Forced Parameterization'
            ELSE N'Plan cache composition is healthy'
        END
FROM aggregated_cache;
GO

/*
=============================================================================
MEMORY CLERKS ANALYSIS
Shows top memory consumers by clerk type with contextual concern levels
=============================================================================
*/
CREATE OR ALTER VIEW
    report.top_memory_consumers
AS
WITH
    recent_clerks AS
(
    SELECT
        clerk_type,
        memory_mb = SUM(pages_kb) / 1024.0,
        collection_time = MAX(collection_time)
    FROM collect.memory_clerks_stats
    WHERE collection_time >= DATEADD(MINUTE, -15, SYSDATETIME())
    GROUP BY
        clerk_type
),
    with_buffer_pool AS
(
    SELECT
        rc.*,
        buffer_pool_mb =
            MAX
            (
                CASE
                    WHEN rc.clerk_type = N'MEMORYCLERK_SQLBUFFERPOOL'
                    THEN rc.memory_mb
                END
            ) OVER ()
    FROM recent_clerks AS rc
)
SELECT TOP (20)
    wbp.clerk_type,
    wbp.memory_mb,
    memory_gb = wbp.memory_mb / 1024.0,
    percent_of_total =
        CONVERT(decimal(5,2), wbp.memory_mb * 100.0 / NULLIF(SUM(wbp.memory_mb) OVER (), 0)),
    /*
    Concern levels based on clerk type purpose and relative size
    */
    concern_level =
        CASE
            /*Buffer pool should be large - only flag if unusually small*/
            WHEN wbp.clerk_type = N'MEMORYCLERK_SQLBUFFERPOOL'
            THEN N'NORMAL'
            /*Plan cache bloat - flag if > 10% of buffer pool or > 8GB*/
            WHEN wbp.clerk_type IN (N'CACHESTORE_SQLCP', N'CACHESTORE_OBJCP')
            AND  (wbp.memory_mb > wbp.buffer_pool_mb * 0.10 OR wbp.memory_mb > 8192)
            THEN N'REVIEW - Possible plan cache bloat'
            /*Lock manager - flag if > 1GB indicates heavy locking*/
            WHEN wbp.clerk_type = N'OBJECTSTORE_LOCK_MANAGER'
            AND  wbp.memory_mb > 1024
            THEN N'REVIEW - Heavy lock activity'
            /*Query execution memory - flag if > 5GB indicates memory-hungry queries*/
            WHEN wbp.clerk_type = N'MEMORYCLERK_SQLQUERYEXEC'
            AND  wbp.memory_mb > 5120
            THEN N'REVIEW - Large query execution memory'
            /*Token/permission cache - flag if > 1GB indicates many logins or permission issues*/
            WHEN wbp.clerk_type = N'USERSTORE_TOKENPERM'
            AND  wbp.memory_mb > 1024
            THEN N'REVIEW - Large token/permission cache'
            /*Column store - expected to be large if using columnstore indexes*/
            WHEN wbp.clerk_type LIKE N'%COLUMNSTORE%'
            THEN N'NORMAL'
            /*In-Memory OLTP - expected to be large if using memory-optimized tables*/
            WHEN wbp.clerk_type LIKE N'%XTP%'
            THEN N'NORMAL'
            /*Other clerks - flag only if unexpectedly large (> 2GB)*/
            WHEN wbp.memory_mb > 2048
            AND  wbp.clerk_type NOT IN
                 (
                     N'MEMORYCLERK_SQLBUFFERPOOL',
                     N'MEMORYCLERK_SQLGENERAL',
                     N'CACHESTORE_SQLCP',
                     N'CACHESTORE_OBJCP'
                 )
            THEN N'MONITOR - Unusually large'
            ELSE N'NORMAL'
        END,
    /*
    Memory clerk descriptions from Microsoft documentation:
    https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-memory-clerks-transact-sql
    */
    clerk_description =
        CASE wbp.clerk_type
            /*MEMORYCLERK types*/
            WHEN N'MEMORYCLERK_SQLBUFFERPOOL'
            THEN N'Data and index pages cache (Buffer Pool) - typically largest memory consumer.'
            WHEN N'MEMORYCLERK_SQLGENERAL'
            THEN N'Multiple consumers including replication, diagnostics, parser, and security.'
            WHEN N'MEMORYCLERK_SQLQUERYEXEC'
            THEN N'Batch mode, parallel query execution, sort and hash operations.'
            WHEN N'MEMORYCLERK_SQLQUERYCOMPILE'
            THEN N'Query optimizer memory during query compilation.'
            WHEN N'MEMORYCLERK_SQLOPTIMIZER'
            THEN N'Query compilation phases memory allocations.'
            WHEN N'MEMORYCLERK_SQLSTORENG'
            THEN N'Storage engine components allocations.'
            WHEN N'MEMORYCLERK_SQLLOGPOOL'
            THEN N'SQL Server Log Pool caching.'
            WHEN N'MEMORYCLERK_SQLCLR'
            THEN N'SQLCLR allocations.'
            WHEN N'MEMORYCLERK_SQLCLRASSEMBLY'
            THEN N'SQLCLR assemblies allocations.'
            WHEN N'MEMORYCLERK_SQLCONNECTIONPOOL'
            THEN N'Client application connection information caching.'
            WHEN N'MEMORYCLERK_SQLQERESERVATIONS'
            THEN N'Memory Grant allocations for sort and hash operations.'
            WHEN N'MEMORYCLERK_SQLQUERYPLAN'
            THEN N'Heap page management, DBCC CHECKTABLE, and cursor allocations.'
            WHEN N'MEMORYCLERK_SQLUTILITIES'
            THEN N'Backup, Restore, Log Shipping, Database Mirroring, DBCC, BCP, and parallelism.'
            WHEN N'MEMORYCLERK_SQLSERVICEBROKER'
            THEN N'Service Broker memory allocations.'
            WHEN N'MEMORYCLERK_SNI'
            THEN N'Server Network Interface (SNI) components and TDS packets.'
            WHEN N'MEMORYCLERK_SOSMEMMANAGER'
            THEN N'SQLOS thread scheduling, memory and I/O management.'
            WHEN N'MEMORYCLERK_SOSNODE'
            THEN N'SQLOS thread scheduling, memory and I/O management.'
            WHEN N'MEMORYCLERK_SOSOS'
            THEN N'SQLOS thread scheduling, memory and I/O management.'
            WHEN N'MEMORYCLERK_XTP'
            THEN N'In-Memory OLTP memory allocations.'
            WHEN N'MEMORYCLERK_XE'
            THEN N'Extended Events memory allocations.'
            WHEN N'MEMORYCLERK_XE_BUFFER'
            THEN N'Extended Events memory allocations.'
            WHEN N'MEMORYCLERK_LANGSVC'
            THEN N'SQL T-SQL statements and commands (parser, algebrizer).'
            WHEN N'MEMORYCLERK_FULLTEXT'
            THEN N'Full-Text engine structures.'
            WHEN N'MEMORYCLERK_HADR'
            THEN N'Always On functionality allocations.'
            WHEN N'MEMORYCLERK_BACKUP'
            THEN N'Backup functionality allocations.'
            WHEN N'MEMORYCLERK_QUERYDISKSTORE'
            THEN N'Query Store memory allocations.'
            WHEN N'MEMORYCLERK_QUERYDISKSTORE_HASHMAP'
            THEN N'Query Store hashmap allocations.'
            WHEN N'MEMORYCLERK_SPATIAL'
            THEN N'Spatial Data components allocations.'
            WHEN N'MEMORYCLERK_SQLTRACE'
            THEN N'Server-side SQL Trace memory allocations.'
            /*CACHESTORE types*/
            WHEN N'CACHESTORE_SQLCP'
            THEN N'Ad hoc queries, prepared statements, and server-side cursors in plan cache.'
            WHEN N'CACHESTORE_OBJCP'
            THEN N'Cached objects with compiled plans (stored procedures, functions, triggers).'
            WHEN N'CACHESTORE_PHDR'
            THEN N'Temporary memory caching during parsing for views, constraints, defaults.'
            WHEN N'CACHESTORE_XPROC'
            THEN N'Extended Stored procedures structures in plan cache.'
            WHEN N'CACHESTORE_TEMPTABLES'
            THEN N'Temporary tables and table variables caching.'
            WHEN N'CACHESTORE_COLUMNSTOREOBJECTPOOL'
            THEN N'Columnstore Indexes segments and dictionaries.'
            WHEN N'CACHESTORE_QDSRUNTIMESTATS'
            THEN N'Query Store runtime statistics.'
            WHEN N'CACHESTORE_SYSTEMROWSET'
            THEN N'Internal structures for transaction logging and recovery.'
            WHEN N'CACHESTORE_NOTIF'
            THEN N'Query Notification functionality.'
            WHEN N'CACHESTORE_FULLTEXTSTOPLIST'
            THEN N'Full-Text engine stoplist functionality.'
            WHEN N'CACHESTORE_VIEWDEFINITIONS'
            THEN N'View definitions caching for query optimization.'
            /*OBJECTSTORE types*/
            WHEN N'OBJECTSTORE_LOCK_MANAGER'
            THEN N'Lock Manager allocations.'
            WHEN N'OBJECTSTORE_SNI_PACKET'
            THEN N'Server Network Interface (SNI) connectivity management.'
            WHEN N'OBJECTSTORE_XACT_CACHE'
            THEN N'Transaction information caching.'
            WHEN N'OBJECTSTORE_LBSS'
            THEN N'Temporary LOBs: variables, parameters, and intermediate expression results.'
            WHEN N'OBJECTSTORE_SECAUDIT_EVENT_BUFFER'
            THEN N'SQL Server Audit memory allocations.'
            /*USERSTORE types*/
            WHEN N'USERSTORE_TOKENPERM'
            THEN N'Security context, login, user, permission, and audit entries.'
            WHEN N'USERSTORE_DBMETADATA'
            THEN N'Database metadata structures.'
            WHEN N'USERSTORE_OBJPERM'
            THEN N'Object security/permission tracking structures.'
            WHEN N'USERSTORE_SCHEMAMGR'
            THEN N'Schema manager metadata caching (tables, procedures, etc.).'
            WHEN N'USERSTORE_QDSSTMT'
            THEN N'Query Store statements caching.'
            ELSE N'See documentation for clerk type details.'
        END,
    last_seen = wbp.collection_time
FROM with_buffer_pool AS wbp
WHERE wbp.memory_mb > 0
ORDER BY
    wbp.memory_mb DESC;
GO

/*
=============================================================================
MEMORY GRANT PRESSURE ANALYSIS
Shows memory grant waits indicating memory pressure
=============================================================================
*/
CREATE OR ALTER VIEW
    report.memory_grant_pressure
AS
SELECT TOP (1)
    collection_time,
    active_grants = grantee_count,
    queries_waiting = waiter_count,
    available_memory_mb,
    granted_memory_mb,
    used_memory_mb,
    memory_utilization_percent =
        CONVERT(decimal(5,2), used_memory_mb * 100.0 / NULLIF(granted_memory_mb, 0)),
    timeout_errors = timeout_error_count,
    forced_grants = forced_grant_count,
    pressure_level =
        CASE
            WHEN waiter_count > 10 THEN N'CRITICAL - High wait queue'
            WHEN waiter_count > 5 THEN N'HIGH - Moderate wait queue'
            WHEN waiter_count > 0 THEN N'MEDIUM - Some grant waits'
            WHEN available_memory_mb < 100 THEN N'MEDIUM - Low available memory'
            ELSE N'NORMAL'
        END,
    recommendation =
        CASE
            WHEN waiter_count > 10 THEN N'Memory grant pressure - review query memory grants, consider adding memory'
            WHEN waiter_count > 0 THEN N'Queries waiting for memory grants - check for large sorts/hashes'
            WHEN timeout_error_count > 0 THEN N'Memory grant timeouts detected - review query resource governor settings'
            ELSE N'No memory grant pressure detected'
        END
FROM collect.memory_grant_stats
ORDER BY
    collection_time DESC;
GO

/*
=============================================================================
FILE I/O LATENCY ANALYSIS
Shows files with high I/O latency - read >20ms, write >50ms are problematic
=============================================================================
*/
CREATE OR ALTER VIEW
    report.file_io_latency
AS
WITH
    recent_io AS
(
    SELECT
        fio.database_name,
        fio.file_type_desc,
        fio.file_name,
        fio.io_stall_read_ms_delta,
        fio.num_of_reads_delta,
        fio.io_stall_write_ms_delta,
        fio.num_of_writes_delta,
        fio.collection_time
    FROM collect.file_io_stats AS fio
    WHERE fio.collection_time >= DATEADD(MINUTE, -15, SYSDATETIME())
)
SELECT
    database_name,
    file_type = file_type_desc,
    file_name,
    avg_read_latency_ms =
        CASE
            WHEN SUM(num_of_reads_delta) > 0
            THEN CONVERT(bigint, SUM(io_stall_read_ms_delta) / SUM(num_of_reads_delta))
            ELSE 0
        END,
    avg_write_latency_ms =
        CASE
            WHEN SUM(num_of_writes_delta) > 0
            THEN CONVERT(bigint, SUM(io_stall_write_ms_delta) / SUM(num_of_writes_delta))
            ELSE 0
        END,
    reads_last_15min = SUM(ISNULL(num_of_reads_delta, 0)),
    writes_last_15min = SUM(ISNULL(num_of_writes_delta, 0)),
    latency_issue =
        CASE
            WHEN SUM(num_of_reads_delta) > 0 AND SUM(io_stall_read_ms_delta) / SUM(num_of_reads_delta) > 50
            THEN N'CRITICAL - Read latency > 50ms'
            WHEN SUM(num_of_reads_delta) > 0 AND SUM(io_stall_read_ms_delta) / SUM(num_of_reads_delta) > 20
            THEN N'HIGH - Read latency > 20ms'
            WHEN SUM(num_of_writes_delta) > 0 AND SUM(io_stall_write_ms_delta) / SUM(num_of_writes_delta) > 100
            THEN N'CRITICAL - Write latency > 100ms'
            WHEN SUM(num_of_writes_delta) > 0 AND SUM(io_stall_write_ms_delta) / SUM(num_of_writes_delta) > 50
            THEN N'HIGH - Write latency > 50ms'
            WHEN SUM(num_of_reads_delta) > 0 AND SUM(io_stall_read_ms_delta) / SUM(num_of_reads_delta) > 10
            THEN N'MEDIUM - Read latency > 10ms'
            WHEN SUM(num_of_writes_delta) > 0 AND SUM(io_stall_write_ms_delta) / SUM(num_of_writes_delta) > 20
            THEN N'MEDIUM - Write latency > 20ms'
            ELSE N'NORMAL'
        END,
    recommendation =
        CASE
            WHEN SUM(num_of_reads_delta) > 0 AND SUM(io_stall_read_ms_delta) / SUM(num_of_reads_delta) > 20
            THEN N'Check storage subsystem - high read latency indicates disk bottleneck'
            WHEN SUM(num_of_writes_delta) > 0 AND SUM(io_stall_write_ms_delta) / SUM(num_of_writes_delta) > 50
            THEN N'Check storage subsystem - high write latency, consider write caching'
            ELSE N'Latency within acceptable range'
        END,
    last_seen = MAX(collection_time)
FROM recent_io
WHERE database_name IS NOT NULL
GROUP BY
    database_name,
    file_type_desc,
    file_name;
GO

/*
=============================================================================
CPU SCHEDULER PRESSURE ANALYSIS
Shows scheduler runnable task queues indicating CPU pressure
=============================================================================
*/
CREATE OR ALTER VIEW
    report.cpu_scheduler_pressure
AS
SELECT TOP (1)
    collection_time,
    total_schedulers = scheduler_count,
    total_runnable_tasks = total_runnable_tasks_count,
    avg_runnable_tasks_per_scheduler = avg_runnable_tasks_count,
    total_workers = total_current_workers_count,
    max_workers = max_workers_count,
    worker_utilization_percent =
        CONVERT(decimal(5,2), total_current_workers_count * 100.0 / NULLIF(max_workers_count, 0)),
    runnable_percent,
    total_queued_requests = total_queued_request_count,
    total_active_requests = total_active_request_count,
    pressure_level =
        CASE
            WHEN total_runnable_tasks_count > 50 THEN N'CRITICAL - High runnable task queue'
            WHEN total_runnable_tasks_count > 20 THEN N'HIGH - Moderate runnable task queue'
            WHEN total_runnable_tasks_count > 10 THEN N'MEDIUM - Some runnable tasks queued'
            WHEN total_current_workers_count * 100.0 / NULLIF(max_workers_count, 0) > 90 THEN N'HIGH - Worker thread exhaustion'
            WHEN worker_thread_exhaustion_warning = 1 THEN N'CRITICAL - Worker thread exhaustion warning'
            WHEN runnable_tasks_warning = 1 THEN N'HIGH - Runnable tasks warning'
            WHEN queued_requests_warning = 1 THEN N'MEDIUM - Queued requests warning'
            ELSE N'NORMAL'
        END,
    recommendation =
        CASE
            WHEN total_runnable_tasks_count > 20 THEN N'CPU pressure detected - check for CPU-intensive queries, consider adding CPU cores'
            WHEN worker_thread_exhaustion_warning = 1 THEN N'Worker thread exhaustion - check max worker threads setting'
            WHEN total_queued_request_count > 0 THEN N'Requests queued for execution - CPU or worker thread pressure'
            ELSE N'No CPU scheduler pressure detected'
        END,
    /*Warning flags*/
    worker_thread_exhaustion_warning,
    runnable_tasks_warning,
    blocked_tasks_warning,
    queued_requests_warning,
    physical_memory_pressure_warning
FROM collect.cpu_scheduler_stats
ORDER BY
    collection_time DESC;
GO

/*
=============================================================================
QUERY STORE REGRESSION ANALYSIS
Shows regressed queries comparing recent performance to historical baseline
Parameters:
    @start_date - Beginning of the "recent" window
    @end_date   - End of the "recent" window
    Baseline is calculated from all data BEFORE @start_date
=============================================================================
*/
CREATE OR ALTER FUNCTION
    report.query_store_regressions
(
    @start_date datetime2(7),
    @end_date datetime2(7)
)
RETURNS TABLE
AS
RETURN
(
    WITH
        baseline_performance AS
    (
        SELECT
            database_name = qsd.database_name,
            query_id = qsd.query_id,
            avg_duration_ms = AVG(qsd.avg_duration / 1000.0),
            avg_cpu_time_ms = AVG(qsd.avg_cpu_time / 1000.0),
            avg_logical_io_reads = AVG(qsd.avg_logical_io_reads)
        FROM collect.query_store_data AS qsd
        WHERE qsd.server_last_execution_time < @start_date
        GROUP BY
            qsd.database_name,
            qsd.query_id
    ),
        recent_performance AS
    (
        SELECT
            database_name = qsd.database_name,
            query_id = qsd.query_id,
            query_text_sample = MAX(qsd.query_sql_text),
            avg_duration_ms = AVG(qsd.avg_duration / 1000.0),
            avg_cpu_time_ms = AVG(qsd.avg_cpu_time / 1000.0),
            avg_logical_io_reads = AVG(qsd.avg_logical_io_reads),
            last_execution_time = MAX(qsd.server_last_execution_time)
        FROM collect.query_store_data AS qsd
        WHERE qsd.server_last_execution_time >= @start_date
        AND   qsd.server_last_execution_time <= @end_date
        GROUP BY
            qsd.database_name,
            qsd.query_id
    )
    SELECT TOP (50)
        database_name = r.database_name,
        query_id = r.query_id,
        baseline_duration_ms = b.avg_duration_ms,
        recent_duration_ms = r.avg_duration_ms,
        duration_regression_percent =
            CONVERT(decimal(10,2), (r.avg_duration_ms - b.avg_duration_ms) * 100.0 / NULLIF(b.avg_duration_ms, 0)),
        baseline_cpu_ms = b.avg_cpu_time_ms,
        recent_cpu_ms = r.avg_cpu_time_ms,
        cpu_regression_percent =
            CONVERT(decimal(10,2), (r.avg_cpu_time_ms - b.avg_cpu_time_ms) * 100.0 / NULLIF(b.avg_cpu_time_ms, 0)),
        baseline_reads = b.avg_logical_io_reads,
        recent_reads = r.avg_logical_io_reads,
        io_regression_percent =
            CONVERT(decimal(10,2), (r.avg_logical_io_reads - b.avg_logical_io_reads) * 100.0 / NULLIF(b.avg_logical_io_reads, 0)),
        severity =
            CASE
                WHEN (r.avg_duration_ms - b.avg_duration_ms) * 100.0 / NULLIF(b.avg_duration_ms, 0) > 100 THEN N'CRITICAL'
                WHEN (r.avg_duration_ms - b.avg_duration_ms) * 100.0 / NULLIF(b.avg_duration_ms, 0) > 50 THEN N'HIGH'
                WHEN (r.avg_duration_ms - b.avg_duration_ms) * 100.0 / NULLIF(b.avg_duration_ms, 0) > 25 THEN N'MEDIUM'
                ELSE N'LOW'
            END,
        query_text_sample = CONVERT(nvarchar(500), r.query_text_sample),
        last_execution_time = r.last_execution_time
    FROM recent_performance AS r
    JOIN baseline_performance AS b
      ON  b.database_name = r.database_name
      AND b.query_id = r.query_id
    WHERE (r.avg_cpu_time_ms - b.avg_cpu_time_ms) * 100.0 / NULLIF(b.avg_cpu_time_ms, 0) > 25
    ORDER BY
        (r.avg_cpu_time_ms - b.avg_cpu_time_ms) * 100.0 / NULLIF(b.avg_cpu_time_ms, 0) DESC
);
GO

/*
=============================================================================
TRACE ANALYSIS - LONG RUNNING QUERY PATTERNS
Shows patterns of long-running queries from SQL Trace
Groups by database and normalized query text (first 200 chars)
=============================================================================
*/
CREATE OR ALTER VIEW
    report.long_running_query_patterns
AS
WITH
    query_patterns AS
(
    SELECT
        database_name,
        query_pattern = LEFT(sql_text, 200),
        executions = COUNT_BIG(*),
        avg_duration_ms = AVG(duration_ms),
        max_duration_ms = MAX(duration_ms),
        avg_cpu_ms = AVG(cpu_ms),
        avg_reads = AVG(reads),
        avg_writes = AVG(writes),
        sample_query_text = MAX(sql_text),
        last_execution = MAX(end_time)
    FROM collect.trace_analysis
    GROUP BY
        database_name,
        LEFT(sql_text, 200)
)
SELECT TOP (50)
    database_name,
    query_pattern,
    executions,
    avg_duration_sec = avg_duration_ms / 1000.0,
    max_duration_sec = max_duration_ms / 1000.0,
    avg_cpu_sec = avg_cpu_ms / 1000.0,
    avg_reads,
    avg_writes,
    concern_level =
        CASE
            WHEN avg_duration_ms > 60000 THEN N'CRITICAL - Avg > 1 minute'
            WHEN avg_duration_ms > 30000 THEN N'HIGH - Avg > 30 seconds'
            WHEN avg_duration_ms > 10000 THEN N'MEDIUM - Avg > 10 seconds'
            ELSE N'INFO'
        END,
    recommendation =
        CASE
            WHEN avg_reads > 1000000 THEN N'High read count - check for missing indexes, table scans'
            WHEN avg_cpu_ms > avg_duration_ms * 0.8 THEN N'CPU-bound query - check for complex calculations, functions'
            WHEN avg_writes > 100000 THEN N'High write volume - review update/delete patterns'
            ELSE N'Review execution plan for optimization opportunities'
        END,
    sample_query_text = CONVERT(nvarchar(500), sample_query_text),
    last_execution
FROM query_patterns
WHERE executions > 1
ORDER BY
    avg_duration_ms DESC;
GO

/*
=============================================================================
MEMORY PRESSURE INDICATORS
Combines memory stats, memory clerks, wait stats for holistic memory view
=============================================================================
*/
CREATE OR ALTER VIEW
    report.memory_pressure_indicators
AS
WITH
    recent_memory AS
(
    SELECT TOP (1)
        ms.collection_time,
        ms.buffer_pool_mb,
        ms.plan_cache_mb,
        ms.total_memory_mb,
        ms.physical_memory_in_use_mb,
        ms.available_physical_memory_mb,
        ms.memory_utilization_percentage,
        ms.buffer_pool_pressure_warning,
        ms.plan_cache_pressure_warning
    FROM collect.memory_stats AS ms
    ORDER BY
        ms.collection_time DESC
),
    memory_waits AS
(
    SELECT
        wait_type,
        wait_time_ms = SUM(wait_time_ms_delta),
        waiting_tasks = SUM(waiting_tasks_count_delta)
    FROM collect.wait_stats AS ws
    WHERE ws.collection_time >= DATEADD(HOUR, -1, SYSDATETIME())
    AND   ws.wait_type IN
          (
              N'RESOURCE_SEMAPHORE',
              N'RESOURCE_SEMAPHORE_QUERY_COMPILE',
              N'RESOURCE_SEMAPHORE_SMALL_QUERY',
              N'PAGEIOLATCH_SH',
              N'PAGEIOLATCH_EX',
              N'PAGEIOLATCH_UP'
          )
    GROUP BY
        ws.wait_type
)
SELECT
    rm.collection_time,
    rm.buffer_pool_mb,
    rm.plan_cache_mb,
    rm.total_memory_mb,
    rm.physical_memory_in_use_mb,
    rm.available_physical_memory_mb,
    rm.memory_utilization_percentage,
    /*Memory grant pressure*/
    memory_grant_waiters = ISNULL(mgs.waiter_count, 0),
    memory_grant_timeouts = ISNULL(mgs.timeout_error_count, 0),
    memory_grant_forced = ISNULL(mgs.forced_grant_count, 0),
    memory_grant_available_mb = ISNULL(mgs.available_memory_mb, 0),
    /*Memory-related waits*/
    resource_semaphore_wait_ms =
        ISNULL
        (
            (
                SELECT
                    mw.wait_time_ms
                FROM memory_waits AS mw
                WHERE mw.wait_type = N'RESOURCE_SEMAPHORE'
            ),
            0
        ),
    pageiolatch_wait_ms =
        ISNULL
        (
            (
                SELECT
                    SUM(mw.wait_time_ms)
                FROM memory_waits AS mw
                WHERE mw.wait_type LIKE N'PAGEIOLATCH%'
            ),
            0
        ),
    /*Overall assessment*/
    pressure_level =
        CASE
            WHEN rm.memory_utilization_percentage > 95 THEN N'CRITICAL - Memory > 95%'
            WHEN ISNULL(mgs.waiter_count, 0) > 10 THEN N'CRITICAL - High grant waiters'
            WHEN rm.memory_utilization_percentage > 90 THEN N'HIGH - Memory > 90%'
            WHEN ISNULL(mgs.waiter_count, 0) > 5 THEN N'HIGH - Grant waiters'
            WHEN rm.buffer_pool_pressure_warning = 1 THEN N'MEDIUM - Buffer pool pressure'
            WHEN rm.plan_cache_pressure_warning = 1 THEN N'MEDIUM - Plan cache pressure'
            ELSE N'NORMAL'
        END,
    recommendation =
        CASE
            WHEN rm.memory_utilization_percentage > 90 THEN N'Memory pressure detected - consider adding memory or reducing workload'
            WHEN ISNULL(mgs.waiter_count, 0) > 5 THEN N'Memory grant waits - check for queries with large sorts/hashes'
            WHEN rm.plan_cache_mb > rm.buffer_pool_mb * 0.20 THEN N'Plan cache > 20% of buffer pool - check for unparameterized queries'
            ELSE N'Memory subsystem healthy'
        END
FROM recent_memory AS rm
OUTER APPLY
(
    SELECT TOP (1)
        mgs2.waiter_count,
        mgs2.timeout_error_count,
        mgs2.forced_grant_count,
        mgs2.available_memory_mb
    FROM collect.memory_grant_stats AS mgs2
    ORDER BY
        mgs2.collection_time DESC
) AS mgs;
GO

/*
=============================================================================
FILE I/O AND WAIT CORRELATION
Correlates file I/O latency with PAGEIOLATCH waits for storage analysis
=============================================================================
*/
CREATE OR ALTER VIEW
    report.file_io_wait_correlation
AS
WITH
    recent_file_io AS
(
    SELECT
        fio.database_name,
        fio.file_type_desc,
        fio.file_name,
        fio.physical_name,
        total_reads = SUM(fio.num_of_reads_delta),
        total_writes = SUM(fio.num_of_writes_delta),
        total_read_stall_ms = SUM(fio.io_stall_read_ms_delta),
        total_write_stall_ms = SUM(fio.io_stall_write_ms_delta),
        avg_read_latency_ms =
            CASE
                WHEN SUM(fio.num_of_reads_delta) > 0
                THEN SUM(fio.io_stall_read_ms_delta) / SUM(fio.num_of_reads_delta)
                ELSE 0
            END,
        avg_write_latency_ms =
            CASE
                WHEN SUM(fio.num_of_writes_delta) > 0
                THEN SUM(fio.io_stall_write_ms_delta) / SUM(fio.num_of_writes_delta)
                ELSE 0
            END,
        collection_time = MAX(fio.collection_time)
    FROM collect.file_io_stats AS fio
    WHERE fio.collection_time >= DATEADD(HOUR, -1, SYSDATETIME())
    GROUP BY
        fio.database_name,
        fio.file_type_desc,
        fio.file_name,
        fio.physical_name
),
    io_waits AS
(
    SELECT
        wait_type,
        wait_time_ms = SUM(wait_time_ms_delta),
        waiting_tasks = SUM(waiting_tasks_count_delta)
    FROM collect.wait_stats AS ws
    WHERE ws.collection_time >= DATEADD(HOUR, -1, SYSDATETIME())
    AND   ws.wait_type IN
          (
              N'PAGEIOLATCH_SH',
              N'PAGEIOLATCH_EX',
              N'PAGEIOLATCH_UP',
              N'WRITELOG',
              N'IO_COMPLETION',
              N'ASYNC_IO_COMPLETION'
          )
    GROUP BY
        ws.wait_type
)
SELECT
    rfio.database_name,
    rfio.file_type_desc,
    rfio.file_name,
    rfio.physical_name,
    rfio.total_reads,
    rfio.total_writes,
    rfio.avg_read_latency_ms,
    rfio.avg_write_latency_ms,
    /*Correlated wait stats (server-wide)*/
    pageiolatch_sh_ms =
        ISNULL
        (
            (
                SELECT
                    iw.wait_time_ms
                FROM io_waits AS iw
                WHERE iw.wait_type = N'PAGEIOLATCH_SH'
            ),
            0
        ),
    pageiolatch_ex_ms =
        ISNULL
        (
            (
                SELECT
                    iw.wait_time_ms
                FROM io_waits AS iw
                WHERE iw.wait_type = N'PAGEIOLATCH_EX'
            ),
            0
        ),
    writelog_ms =
        ISNULL
        (
            (
                SELECT
                    iw.wait_time_ms
                FROM io_waits AS iw
                WHERE iw.wait_type = N'WRITELOG'
            ),
            0
        ),
    /*Latency assessment*/
    latency_concern =
        CASE
            WHEN rfio.avg_read_latency_ms > 50 THEN N'CRITICAL - Read > 50ms'
            WHEN rfio.avg_write_latency_ms > 100 THEN N'CRITICAL - Write > 100ms'
            WHEN rfio.avg_read_latency_ms > 20 THEN N'HIGH - Read > 20ms'
            WHEN rfio.avg_write_latency_ms > 50 THEN N'HIGH - Write > 50ms'
            WHEN rfio.avg_read_latency_ms > 10 THEN N'MEDIUM - Read > 10ms'
            ELSE N'NORMAL'
        END,
    recommendation =
        CASE
            WHEN rfio.file_type_desc = N'LOG' AND rfio.avg_write_latency_ms > 5
            THEN N'Transaction log write latency - consider faster storage for log files'
            WHEN rfio.avg_read_latency_ms > 20
            THEN N'High read latency - check storage subsystem, consider adding memory to reduce I/O'
            WHEN rfio.avg_write_latency_ms > 50
            THEN N'High write latency - check storage write caching, RAID configuration'
            ELSE N'I/O latency within acceptable range'
        END,
    last_seen = rfio.collection_time
FROM recent_file_io AS rfio
WHERE rfio.total_reads > 0
   OR rfio.total_writes > 0;
GO

/*
=============================================================================
BLOCKING CHAIN ANALYSIS
Shows blocking hierarchies with query details from waiting_tasks
=============================================================================
*/
CREATE OR ALTER VIEW
    report.blocking_chain_analysis
AS
WITH
    blocking_sessions AS
(
    SELECT
        wt.collection_time,
        wt.session_id,
        wt.blocking_session_id,
        wt.wait_type,
        wt.wait_duration_ms,
        wt.database_name,
        wt.query_text,
        wt.statement_text,
        wt.query_plan,
        wt.resource_description,
        wt.command,
        wt.cpu_time_ms,
        wt.logical_reads,
        /*Identify head blockers (blocking others but not blocked themselves)*/
        is_head_blocker =
            CASE
                WHEN wt.blocking_session_id = 0
                AND  EXISTS
                     (
                         SELECT
                             1/0
                         FROM collect.waiting_tasks AS wt2
                         WHERE wt2.collection_time = wt.collection_time
                         AND   wt2.blocking_session_id = wt.session_id
                     )
                THEN 1
                ELSE 0
            END,
        blocked_session_count =
        (
            SELECT
                COUNT_BIG(*)
            FROM collect.waiting_tasks AS wt2
            WHERE wt2.collection_time = wt.collection_time
            AND   wt2.blocking_session_id = wt.session_id
        )
    FROM collect.waiting_tasks AS wt
    WHERE wt.blocking_session_id > 0
       OR EXISTS
          (
              SELECT
                  1/0
              FROM collect.waiting_tasks AS wt2
              WHERE wt2.collection_time = wt.collection_time
              AND   wt2.blocking_session_id = wt.session_id
          )
)
SELECT
    bs.collection_time,
    bs.session_id,
    bs.blocking_session_id,
    blocking_chain_position =
        CASE
            WHEN bs.is_head_blocker = 1 THEN N'HEAD BLOCKER'
            WHEN bs.blocking_session_id > 0 AND bs.blocked_session_count > 0 THEN N'INTERMEDIATE'
            ELSE N'BLOCKED'
        END,
    sessions_blocked = bs.blocked_session_count,
    bs.wait_type,
    bs.wait_duration_ms,
    wait_duration_sec = bs.wait_duration_ms / 1000.0,
    bs.database_name,
    bs.command,
    bs.cpu_time_ms,
    bs.logical_reads,
    bs.resource_description,
    query_text = CONVERT(nvarchar(500), bs.query_text),
    statement_text = CONVERT(nvarchar(500), bs.statement_text),
    has_query_plan =
        CASE
            WHEN bs.query_plan IS NOT NULL THEN 1
            ELSE 0
        END,
    severity =
        CASE
            WHEN bs.is_head_blocker = 1 AND bs.blocked_session_count > 5 THEN N'CRITICAL'
            WHEN bs.is_head_blocker = 1 AND bs.blocked_session_count > 2 THEN N'HIGH'
            WHEN bs.wait_duration_ms > 30000 THEN N'HIGH'
            WHEN bs.wait_duration_ms > 10000 THEN N'MEDIUM'
            ELSE N'LOW'
        END,
    recommendation =
        CASE
            WHEN bs.is_head_blocker = 1 AND bs.wait_type = N'LCK_M_IX'
            THEN N'Head blocker holding IX lock - check for long-running transaction'
            WHEN bs.is_head_blocker = 1 AND bs.wait_type LIKE N'LCK_M_S%'
            THEN N'Head blocker holding shared lock - check for uncommitted read transaction'
            WHEN bs.is_head_blocker = 1 AND bs.wait_type LIKE N'LCK_M_X%'
            THEN N'Head blocker holding exclusive lock - check for long-running modification'
            WHEN bs.is_head_blocker = 1
            THEN N'Review head blocker query and consider query tuning or isolation level changes'
            ELSE N'Blocked by another session - resolve head blocker first'
        END
FROM blocking_sessions AS bs;
GO

/*
=============================================================================
TEMPDB CONTENTION ANALYSIS
Combines TempDB stats with PFS/GAM/SGAM waits and session tempdb usage
=============================================================================
*/
CREATE OR ALTER VIEW
    report.tempdb_contention_analysis
AS
WITH
    recent_tempdb AS
(
    SELECT TOP (1)
        ts.collection_time,
        ts.user_object_reserved_mb,
        ts.internal_object_reserved_mb,
        ts.version_store_reserved_mb,
        ts.total_reserved_mb,
        ts.unallocated_mb,
        ts.top_task_session_id,
        ts.top_task_total_mb,
        ts.total_sessions_using_tempdb,
        ts.version_store_high_warning,
        ts.allocation_contention_warning
    FROM collect.tempdb_stats AS ts
    ORDER BY
        ts.collection_time DESC
),
    tempdb_waits AS
(
    SELECT
        wait_type,
        wait_time_ms = SUM(wait_time_ms_delta),
        waiting_tasks = SUM(waiting_tasks_count_delta)
    FROM collect.wait_stats AS ws
    WHERE ws.collection_time >= DATEADD(HOUR, -1, SYSDATETIME())
    AND   ws.wait_type IN
          (
              N'PAGELATCH_UP',
              N'PAGELATCH_EX',
              N'PAGELATCH_SH'
          )
    GROUP BY
        ws.wait_type
),
    latch_waits AS
(
    SELECT
        latch_class,
        wait_time_ms = SUM(wait_time_ms_delta),
        waiting_requests = SUM(waiting_requests_count_delta)
    FROM collect.latch_stats AS ls
    WHERE ls.collection_time >= DATEADD(HOUR, -1, SYSDATETIME())
    AND   ls.latch_class IN
          (
              N'ALLOC_EXTENT_CACHE',
              N'ALLOC_FREESPACE_CACHE',
              N'FGCB_ADD_REMOVE'
          )
    GROUP BY
        ls.latch_class
),
    tempdb_session_waits AS
(
    SELECT
        wt.session_id,
        wt.wait_type,
        total_wait_ms = SUM(wt.wait_duration_ms),
        wt.query_text
    FROM collect.waiting_tasks AS wt
    WHERE wt.collection_time >= DATEADD(HOUR, -1, SYSDATETIME())
    AND   wt.database_name = N'tempdb'
    AND   wt.wait_type LIKE N'PAGELATCH%'
    GROUP BY
        wt.session_id,
        wt.wait_type,
        wt.query_text
)
SELECT
    rt.collection_time,
    rt.user_object_reserved_mb,
    rt.internal_object_reserved_mb,
    rt.version_store_reserved_mb,
    rt.total_reserved_mb,
    rt.unallocated_mb,
    rt.total_sessions_using_tempdb,
    /*Top consumer session*/
    rt.top_task_session_id,
    rt.top_task_total_mb,
    /*Page latch waits (allocation contention)*/
    pagelatch_up_ms =
        ISNULL
        (
            (
                SELECT
                    tw.wait_time_ms
                FROM tempdb_waits AS tw
                WHERE tw.wait_type = N'PAGELATCH_UP'
            ),
            0
        ),
    pagelatch_ex_ms =
        ISNULL
        (
            (
                SELECT
                    tw.wait_time_ms
                FROM tempdb_waits AS tw
                WHERE tw.wait_type = N'PAGELATCH_EX'
            ),
            0
        ),
    /*Allocation latch waits*/
    alloc_extent_cache_ms =
        ISNULL
        (
            (
                SELECT
                    lw.wait_time_ms
                FROM latch_waits AS lw
                WHERE lw.latch_class = N'ALLOC_EXTENT_CACHE'
            ),
            0
        ),
    /*Contention assessment*/
    contention_level =
        CASE
            WHEN rt.allocation_contention_warning = 1 THEN N'CRITICAL - Allocation contention detected'
            WHEN rt.version_store_high_warning = 1 THEN N'HIGH - Version store pressure'
            WHEN rt.version_store_reserved_mb > 5000 THEN N'HIGH - Version store > 5GB'
            WHEN ISNULL
                 (
                     (
                         SELECT
                             tw.wait_time_ms
                         FROM tempdb_waits AS tw
                         WHERE tw.wait_type = N'PAGELATCH_UP'
                     ),
                     0
                 ) > 10000 THEN N'MEDIUM - PAGELATCH_UP contention'
            WHEN rt.unallocated_mb < 100 THEN N'MEDIUM - Low free space'
            ELSE N'NORMAL'
        END,
    recommendation =
        CASE
            WHEN rt.allocation_contention_warning = 1
            THEN N'Add more tempdb data files (one per CPU core up to 8), use TF 1118'
            WHEN rt.version_store_reserved_mb > 2000
            THEN N'Check for long-running transactions using snapshot isolation'
            WHEN ISNULL
                 (
                     (
                         SELECT
                             tw.wait_time_ms
                         FROM tempdb_waits AS tw
                         WHERE tw.wait_type = N'PAGELATCH_UP'
                     ),
                     0
                 ) > 5000
            THEN N'PFS/GAM/SGAM contention - add tempdb files, consider uniform extents'
            WHEN rt.internal_object_reserved_mb > rt.user_object_reserved_mb * 2
            THEN N'High internal object usage - review queries with sorts/hashes/spools'
            ELSE N'TempDB healthy'
        END
FROM recent_tempdb AS rt;
GO

/*
=============================================================================
PARAMETER SENSITIVITY DETECTION
Identifies queries with same query_hash but different query_plan_hash
indicating parameter sniffing or plan instability
=============================================================================
*/
CREATE OR ALTER VIEW
    report.parameter_sensitivity_detection
AS
WITH
    query_plan_variations AS
(
    SELECT
        qs.query_hash,
        qs.database_name,
        plan_count = COUNT_BIG(DISTINCT qs.query_plan_hash),
        execution_count = SUM(qs.execution_count_delta),
        total_worker_time_ms = SUM(qs.total_worker_time_delta) / 1000.0,
        total_elapsed_time_ms = SUM(qs.total_elapsed_time_delta) / 1000.0,
        total_logical_reads = SUM(qs.total_logical_reads_delta),
        min_worker_time_ms = MIN(qs.min_worker_time) / 1000.0,
        max_worker_time_ms = MAX(qs.max_worker_time) / 1000.0,
        min_elapsed_time_ms = MIN(qs.min_elapsed_time) / 1000.0,
        max_elapsed_time_ms = MAX(qs.max_elapsed_time) / 1000.0,
        sample_query_text = MAX(qs.query_text),
        last_execution_time = MAX(qs.last_execution_time)
    FROM collect.query_stats AS qs
    WHERE qs.collection_time >= DATEADD(DAY, -7, SYSDATETIME())
    AND   qs.query_hash IS NOT NULL
    AND   qs.execution_count_delta > 0
    GROUP BY
        qs.query_hash,
        qs.database_name
    HAVING
        COUNT_BIG(DISTINCT qs.query_plan_hash) > 1
)
SELECT TOP (50)
    qpv.query_hash,
    qpv.database_name,
    qpv.plan_count,
    qpv.execution_count,
    qpv.total_worker_time_ms,
    qpv.total_elapsed_time_ms,
    qpv.total_logical_reads,
    /*Performance variance*/
    qpv.min_worker_time_ms,
    qpv.max_worker_time_ms,
    worker_time_variance_ratio =
        CASE
            WHEN qpv.min_worker_time_ms > 0
            THEN qpv.max_worker_time_ms / qpv.min_worker_time_ms
            ELSE 0
        END,
    qpv.min_elapsed_time_ms,
    qpv.max_elapsed_time_ms,
    elapsed_time_variance_ratio =
        CASE
            WHEN qpv.min_elapsed_time_ms > 0
            THEN qpv.max_elapsed_time_ms / qpv.min_elapsed_time_ms
            ELSE 0
        END,
    /*Severity based on plan count and variance*/
    sensitivity_level =
        CASE
            WHEN qpv.plan_count > 5 THEN N'CRITICAL - > 5 plans'
            WHEN qpv.plan_count > 3 AND qpv.max_elapsed_time_ms / NULLIF(qpv.min_elapsed_time_ms, 0) > 10
            THEN N'HIGH - Multiple plans with high variance'
            WHEN qpv.plan_count > 2 THEN N'MEDIUM - Multiple plans'
            ELSE N'LOW'
        END,
    recommendation =
        CASE
            WHEN qpv.plan_count > 3
            THEN N'Consider OPTIMIZE FOR UNKNOWN, plan guides, or Query Store plan forcing'
            WHEN qpv.max_elapsed_time_ms / NULLIF(qpv.min_elapsed_time_ms, 0) > 10
            THEN N'High variance suggests parameter sniffing - review execution plans'
            ELSE N'Monitor for plan stability'
        END,
    sample_query_text = CONVERT(nvarchar(500), qpv.sample_query_text),
    qpv.last_execution_time
FROM query_plan_variations AS qpv
ORDER BY
    qpv.plan_count DESC,
    qpv.total_worker_time_ms DESC;
GO

/*
=============================================================================
SCHEDULER CPU ANALYSIS
Shows CPU scheduler health with runnable task queue and parallelism metrics
=============================================================================
*/
CREATE OR ALTER VIEW
    report.scheduler_cpu_analysis
AS
WITH
    recent_scheduler_stats AS
(
    SELECT
        collection_time,
        scheduler_count,
        cpu_count,
        max_workers_count,
        total_current_workers_count,
        total_runnable_tasks_count,
        avg_runnable_tasks_count,
        total_active_request_count,
        total_queued_request_count,
        total_blocked_task_count,
        total_active_parallel_thread_count,
        runnable_percent,
        worker_thread_exhaustion_warning,
        runnable_tasks_warning,
        blocked_tasks_warning,
        queued_requests_warning,
        physical_memory_pressure_warning,
        total_node_count,
        nodes_online_count,
        offline_cpu_count,
        offline_cpu_warning,
        system_memory_state_desc,
        available_physical_memory_kb
    FROM collect.cpu_scheduler_stats
    WHERE collection_time >= DATEADD(HOUR, -1, SYSDATETIME())
),
    latest AS
(
    SELECT TOP (1)
        *
    FROM recent_scheduler_stats
    ORDER BY
        collection_time DESC
)
SELECT
    l.collection_time,
    l.scheduler_count,
    l.cpu_count,
    l.max_workers_count,
    l.total_current_workers_count,
    worker_utilization_percent =
        CONVERT(decimal(5,2), l.total_current_workers_count * 100.0 / NULLIF(l.max_workers_count, 0)),
    /*Current snapshot*/
    l.total_runnable_tasks_count,
    l.avg_runnable_tasks_count,
    l.total_blocked_task_count,
    l.total_active_parallel_thread_count,
    l.runnable_percent,
    /*Trend over last hour*/
    st.avg_runnable_tasks,
    st.max_runnable_tasks,
    st.avg_blocked_tasks,
    st.max_blocked_tasks,
    st.avg_parallel_threads,
    st.max_parallel_threads,
    /*Warning counts*/
    st.worker_exhaustion_events,
    st.runnable_warning_events,
    st.blocked_warning_events,
    /*NUMA and memory*/
    l.total_node_count,
    l.nodes_online_count,
    l.offline_cpu_count,
    l.system_memory_state_desc,
    available_memory_mb = l.available_physical_memory_kb / 1024,
    /*Overall assessment*/
    cpu_pressure_level =
        CASE
            WHEN ISNULL(st.worker_exhaustion_events, 0) > 0 THEN N'CRITICAL - Worker thread exhaustion detected'
            WHEN ISNULL(st.max_runnable_tasks, 0) > 50 THEN N'CRITICAL - High runnable task queue'
            WHEN ISNULL(st.runnable_warning_events, 0) > 5 THEN N'HIGH - Frequent runnable task warnings'
            WHEN ISNULL(st.avg_runnable_tasks, 0) > 20 THEN N'HIGH - Sustained runnable task queue'
            WHEN ISNULL(st.avg_blocked_tasks, 0) > 10 THEN N'MEDIUM - Blocked tasks'
            WHEN l.offline_cpu_warning = 1 THEN N'MEDIUM - Offline CPUs detected'
            ELSE N'NORMAL'
        END,
    recommendation =
        CASE
            WHEN ISNULL(st.worker_exhaustion_events, 0) > 0
            THEN N'Worker thread exhaustion - check max worker threads, reduce parallelism'
            WHEN ISNULL(st.max_runnable_tasks, 0) > 50
            THEN N'CPU pressure - review CPU-intensive queries, consider adding CPU'
            WHEN ISNULL(st.max_parallel_threads, 0) > l.cpu_count * 2
            THEN N'High parallelism - review MAXDOP settings, check for excessive parallelism'
            WHEN ISNULL(st.avg_blocked_tasks, 0) > 10
            THEN N'Blocking detected - review blocking chains and lock contention'
            WHEN l.offline_cpu_count > 0
            THEN N'Offline CPUs - check server hardware or VM configuration'
            ELSE N'CPU scheduler healthy'
        END
FROM latest AS l
OUTER APPLY
(
    SELECT
        avg_runnable_tasks = AVG(rss.total_runnable_tasks_count),
        max_runnable_tasks = MAX(rss.total_runnable_tasks_count),
        avg_blocked_tasks = AVG(rss.total_blocked_task_count),
        max_blocked_tasks = MAX(rss.total_blocked_task_count),
        avg_parallel_threads = AVG(rss.total_active_parallel_thread_count),
        max_parallel_threads = MAX(rss.total_active_parallel_thread_count),
        worker_exhaustion_events = SUM(CONVERT(integer, rss.worker_thread_exhaustion_warning)),
        runnable_warning_events = SUM(CONVERT(integer, rss.runnable_tasks_warning)),
        blocked_warning_events = SUM(CONVERT(integer, rss.blocked_tasks_warning))
    FROM recent_scheduler_stats AS rss
) AS st;
GO

PRINT '';
PRINT 'Report views created successfully:';
PRINT '  - config.server_info (current server configuration)';
PRINT '  - report.collection_health (CRITICAL - monitor the monitoring)';
PRINT '  - report.top_waits_last_hour';
PRINT '  - report.expensive_queries_today';
PRINT '  - report.memory_pressure_events';
PRINT '  - report.cpu_spikes';
PRINT '  - report.blocking_summary';
PRINT '  - report.deadlock_summary';
PRINT '  - report.server_configuration_changes';
PRINT '  - report.database_configuration_changes';
PRINT '  - report.trace_flag_changes';
PRINT '  - report.daily_summary (one-page dashboard)';
PRINT '';
PRINT 'Analytical views:';
PRINT '  - report.top_latch_contention (latch waits by class)';
PRINT '  - report.top_spinlock_contention (spinlock collisions)';
PRINT '  - report.tempdb_pressure (version store and allocation)';
PRINT '  - report.plan_cache_bloat (single-use plan detection)';
PRINT '  - report.top_memory_consumers (memory clerks analysis)';
PRINT '  - report.memory_grant_pressure (grant waits)';
PRINT '  - report.file_io_latency (high latency files)';
PRINT '  - report.cpu_scheduler_pressure (runnable task queues)';
PRINT '  - report.query_store_regressions (performance regressions)';
PRINT '  - report.long_running_query_patterns (trace analysis)';
GO

/*
=============================================================================
CRITICAL ISSUES
Friendly view over config.critical_issues for monitoring configuration problems
=============================================================================
*/
CREATE OR ALTER VIEW
    report.critical_issues
AS
SELECT
    ci.issue_id,
    ci.log_date,
    age_hours = DATEDIFF(HOUR, ci.log_date, SYSDATETIME()),
    ci.severity,
    severity_order =
        CASE ci.severity
            WHEN N'CRITICAL' THEN 1
            WHEN N'WARNING' THEN 2
            WHEN N'INFO' THEN 3
            ELSE 4
        END,
    ci.problem_area,
    ci.source_collector,
    ci.affected_database,
    ci.message,
    /*Threshold info when available*/
    threshold_info =
        CASE
            WHEN ci.threshold_value IS NOT NULL
            AND  ci.threshold_limit IS NOT NULL
            THEN N'Value: ' + CONVERT(nvarchar(50), ci.threshold_value) +
                 N' / Limit: ' + CONVERT(nvarchar(50), ci.threshold_limit)
            ELSE NULL
        END,
    ci.threshold_value,
    ci.threshold_limit,
    ci.investigate_query,
    /*Actionable recommendation based on problem area*/
    recommendation =
        CASE ci.problem_area
            WHEN N'Query Store Disabled'
            THEN N'Enable Query Store: ALTER DATABASE [db] SET QUERY_STORE = ON;'
            WHEN N'Auto Shrink Enabled'
            THEN N'Disable auto shrink: ALTER DATABASE [db] SET AUTO_SHRINK OFF;'
            WHEN N'Auto Close Enabled'
            THEN N'Disable auto close: ALTER DATABASE [db] SET AUTO_CLOSE OFF;'
            WHEN N'Memory Pressure'
            THEN N'Review memory configuration, check for memory-intensive queries'
            WHEN N'CPU Pressure'
            THEN N'Review CPU-intensive queries, check MAXDOP settings'
            WHEN N'Blocking'
            THEN N'Review blocking chains, check for long-running transactions'
            WHEN N'Deadlock'
            THEN N'Review deadlock graphs, check for lock ordering issues'
            ELSE N'Review issue details and investigate_query'
        END
FROM config.critical_issues AS ci
WHERE ci.log_date >= DATEADD(DAY, -7, SYSDATETIME());
GO

PRINT '';
PRINT 'Correlation views (NEW):';
PRINT '  - report.memory_pressure_indicators (memory + grants + waits combined)';
PRINT '  - report.file_io_wait_correlation (file I/O + PAGEIOLATCH waits)';
PRINT '  - report.blocking_chain_analysis (blocking hierarchies with query plans)';
PRINT '  - report.tempdb_contention_analysis (tempdb + PFS/GAM waits + sessions)';
PRINT '  - report.parameter_sensitivity_detection (same query_hash, different plans)';
PRINT '  - report.scheduler_cpu_analysis (scheduler health + runnable task trends)';
PRINT '  - report.critical_issues (configuration problems with recommendations)';
PRINT '';
PRINT 'Quick health check:';
PRINT '  SELECT * FROM report.collection_health ORDER BY health_status DESC;';
PRINT '  SELECT * FROM report.daily_summary;';
PRINT '  SELECT * FROM config.server_info;';
GO
