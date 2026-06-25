/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

FinOps Reporting Views
Provides cost allocation, utilization scoring, peak analysis,
and application attribution from existing collected data.
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

/*******************************************************************************
View 1: Per-Database Resource Usage
Shows CPU time, logical reads, execution counts, and I/O per database
for cost allocation and showback reporting.
Source: collect.query_stats, collect.procedure_stats, collect.file_io_stats
*******************************************************************************/

CREATE OR ALTER VIEW
    report.finops_database_resource_usage
AS
WITH
    /*
    Combine query and procedure stats deltas by database
    Filter to last 24 hours with valid deltas
    */
    workload_stats AS
    (
        SELECT
            database_name = qs.database_name,
            cpu_time_ms =
                SUM(qs.total_worker_time_delta) / 1000,
            logical_reads =
                SUM(qs.total_logical_reads_delta),
            physical_reads =
                SUM(qs.total_physical_reads_delta),
            logical_writes =
                SUM(qs.total_logical_writes_delta),
            execution_count =
                SUM(qs.execution_count_delta)
        FROM collect.query_stats AS qs
        WHERE qs.collection_time >= DATEADD(HOUR, -24, SYSDATETIME())
        AND   qs.total_worker_time_delta IS NOT NULL
        GROUP BY
            qs.database_name
    ),
    /*
    File I/O deltas by database
    */
    io_stats AS
    (
        SELECT
            database_name = fio.database_name,
            io_read_bytes =
                SUM(fio.num_of_bytes_read_delta),
            io_write_bytes =
                SUM(fio.num_of_bytes_written_delta),
            io_stall_ms =
                SUM(fio.io_stall_ms_delta)
        FROM collect.file_io_stats AS fio
        WHERE fio.collection_time >= DATEADD(HOUR, -24, SYSDATETIME())
        AND   fio.num_of_bytes_read_delta IS NOT NULL
        GROUP BY
            fio.database_name
    ),
    /*
    Server-wide totals for percentage calculations
    */
    totals AS
    (
        SELECT
            total_cpu_ms =
                NULLIF(SUM(ws.cpu_time_ms), 0),
            total_io_bytes =
                NULLIF
                (
                    SUM(ios.io_read_bytes) +
                    SUM(ios.io_write_bytes),
                    0
                )
        FROM workload_stats AS ws
        FULL JOIN io_stats AS ios
          ON ios.database_name = ws.database_name
    )
SELECT
    database_name =
        COALESCE(ws.database_name, ios.database_name),
    cpu_time_ms =
        ISNULL(ws.cpu_time_ms, 0),
    logical_reads =
        ISNULL(ws.logical_reads, 0),
    physical_reads =
        ISNULL(ws.physical_reads, 0),
    logical_writes =
        ISNULL(ws.logical_writes, 0),
    execution_count =
        ISNULL(ws.execution_count, 0),
    io_read_mb =
        CONVERT
        (
            decimal(19,2),
            ISNULL(ios.io_read_bytes, 0) / 1048576.0
        ),
    io_write_mb =
        CONVERT
        (
            decimal(19,2),
            ISNULL(ios.io_write_bytes, 0) / 1048576.0
        ),
    io_stall_ms =
        ISNULL(ios.io_stall_ms, 0),
    pct_cpu_share =
        CONVERT
        (
            decimal(5,2),
            ISNULL(ws.cpu_time_ms, 0) * 100.0 /
              t.total_cpu_ms
        ),
    pct_io_share =
        CONVERT
        (
            decimal(5,2),
            (ISNULL(ios.io_read_bytes, 0) + ISNULL(ios.io_write_bytes, 0)) * 100.0 /
              t.total_io_bytes
        )
FROM workload_stats AS ws
FULL JOIN io_stats AS ios
  ON ios.database_name = ws.database_name
CROSS JOIN totals AS t;
GO

PRINT 'Created report.finops_database_resource_usage view';
GO

/*******************************************************************************
View 2: Utilization Efficiency Score
Calculates whether the server is over-provisioned, right-sized, or
under-provisioned based on CPU, memory, and worker thread utilization.
Source: collect.cpu_utilization_stats, collect.memory_stats,
        collect.cpu_scheduler_stats
*******************************************************************************/

CREATE OR ALTER VIEW
    report.finops_utilization_efficiency
AS
WITH
    /*
    CPU p95 via window function (must be separate from aggregates)
    */
    cpu_p95 AS
    (
        SELECT
            p95_cpu_pct =
                CONVERT
                (
                    decimal(5,2),
                    PERCENTILE_CONT(0.95) WITHIN GROUP
                    (
                        ORDER BY
                            cus.sqlserver_cpu_utilization
                    ) OVER ()
                )
        FROM collect.cpu_utilization_stats AS cus
        WHERE cus.collection_time >= DATEADD(HOUR, -24, SYSDATETIME())
    ),
    /*
    CPU aggregates
    */
    cpu_agg AS
    (
        SELECT
            avg_cpu_pct =
                AVG(CONVERT(decimal(5,2), cus.sqlserver_cpu_utilization)),
            max_cpu_pct =
                MAX(cus.sqlserver_cpu_utilization),
            sample_count =
                COUNT_BIG(*)
        FROM collect.cpu_utilization_stats AS cus
        WHERE cus.collection_time >= DATEADD(HOUR, -24, SYSDATETIME())
    ),
    /*
    Combine CPU stats
    */
    cpu_dedup AS
    (
        SELECT
            ca.avg_cpu_pct,
            ca.max_cpu_pct,
            p95_cpu_pct =
                (SELECT TOP (1) cp.p95_cpu_pct FROM cpu_p95 AS cp),
            ca.sample_count
        FROM cpu_agg AS ca
    ),
    /*
    Latest memory stats
    */
    memory_latest AS
    (
        SELECT TOP (1)
            ms.total_memory_mb,
            ms.committed_target_memory_mb,
            ms.total_physical_memory_mb,
            ms.buffer_pool_mb,
            ms.memory_utilization_percentage,
            memory_ratio =
                CONVERT
                (
                    decimal(5,2),
                    ms.total_memory_mb /
                      NULLIF(ms.committed_target_memory_mb, 0)
                )
        FROM collect.memory_stats AS ms
        ORDER BY
            ms.collection_time DESC
    ),
    /*
    Latest scheduler stats
    */
    scheduler_latest AS
    (
        SELECT TOP (1)
            ss.total_current_workers_count,
            ss.max_workers_count,
            ss.cpu_count,
            worker_ratio =
                CONVERT
                (
                    decimal(5,2),
                    ss.total_current_workers_count * 1.0 /
                      NULLIF(ss.max_workers_count, 0)
                )
        FROM collect.cpu_scheduler_stats AS ss
        ORDER BY
            ss.collection_time DESC
    )
SELECT
    avg_cpu_pct =
        cd.avg_cpu_pct,
    max_cpu_pct =
        cd.max_cpu_pct,
    p95_cpu_pct =
        cd.p95_cpu_pct,
    cpu_samples =
        cd.sample_count,
    total_memory_mb =
        ml.total_memory_mb,
    target_memory_mb =
        ml.committed_target_memory_mb,
    physical_memory_mb =
        ml.total_physical_memory_mb,
    memory_ratio =
        ml.memory_ratio,
    memory_utilization_pct =
        ml.memory_utilization_percentage,
    worker_threads_current =
        sl.total_current_workers_count,
    worker_threads_max =
        sl.max_workers_count,
    worker_thread_ratio =
        sl.worker_ratio,
    cpu_count =
        sl.cpu_count,
    provisioning_status =
        CASE
            WHEN cd.avg_cpu_pct < 15
            AND  cd.max_cpu_pct < 40
            AND  ml.memory_ratio < 0.5
            THEN N'OVER_PROVISIONED'
            WHEN cd.p95_cpu_pct > 85
            OR   ml.memory_ratio > 0.95
            OR   sl.worker_ratio > 0.8
            THEN N'UNDER_PROVISIONED'
            ELSE N'RIGHT_SIZED'
        END
FROM cpu_dedup AS cd
CROSS JOIN memory_latest AS ml
CROSS JOIN scheduler_latest AS sl;
GO

PRINT 'Created report.finops_utilization_efficiency view';
GO

/*******************************************************************************
View 3: Peak Utilization Windows
Shows average and maximum CPU/memory utilization per hour of day (0-23)
to identify peak and idle windows for capacity planning.
Source: collect.cpu_utilization_stats, collect.memory_stats (last 7 days)
*******************************************************************************/

CREATE OR ALTER VIEW
    report.finops_peak_utilization
AS
WITH
    /*
    CPU utilization bucketed by hour of day
    */
    cpu_by_hour AS
    (
        SELECT
            hour_of_day =
                DATEPART(HOUR, cus.collection_time),
            avg_cpu_pct =
                AVG(CONVERT(decimal(5,2), cus.sqlserver_cpu_utilization)),
            max_cpu_pct =
                MAX(cus.sqlserver_cpu_utilization),
            sample_count =
                COUNT_BIG(*)
        FROM collect.cpu_utilization_stats AS cus
        WHERE cus.collection_time >= DATEADD(DAY, -7, SYSDATETIME())
        GROUP BY
            DATEPART(HOUR, cus.collection_time)
    ),
    /*
    Memory utilization bucketed by hour of day
    */
    memory_by_hour AS
    (
        SELECT
            hour_of_day =
                DATEPART(HOUR, ms.collection_time),
            avg_memory_pct =
                AVG(CONVERT(decimal(5,2), ms.memory_utilization_percentage)),
            max_memory_pct =
                MAX(ms.memory_utilization_percentage)
        FROM collect.memory_stats AS ms
        WHERE ms.collection_time >= DATEADD(DAY, -7, SYSDATETIME())
        GROUP BY
            DATEPART(HOUR, ms.collection_time)
    ),
    /*
    Overall averages for classification
    */
    overall AS
    (
        SELECT
            overall_avg_cpu =
                NULLIF(AVG(cbh.avg_cpu_pct), 0)
        FROM cpu_by_hour AS cbh
    )
SELECT
    hour_of_day =
        cbh.hour_of_day,
    avg_cpu_pct =
        cbh.avg_cpu_pct,
    max_cpu_pct =
        cbh.max_cpu_pct,
    avg_memory_pct =
        ISNULL(mbh.avg_memory_pct, 0),
    max_memory_pct =
        ISNULL(mbh.max_memory_pct, 0),
    cpu_samples =
        cbh.sample_count,
    hour_classification =
        CASE
            WHEN cbh.avg_cpu_pct > (o.overall_avg_cpu * 1.5)
            THEN N'PEAK'
            WHEN cbh.avg_cpu_pct < (o.overall_avg_cpu * 0.3)
            THEN N'IDLE'
            ELSE N'NORMAL'
        END
FROM cpu_by_hour AS cbh
LEFT JOIN memory_by_hour AS mbh
  ON mbh.hour_of_day = cbh.hour_of_day
CROSS JOIN overall AS o;
GO

PRINT 'Created report.finops_peak_utilization view';
GO

/*******************************************************************************
View 4: Application Resource Usage (Connection-Level Attribution)
Shows per-application connection patterns from session stats.
Note: Plan cache (query_stats/procedure_stats) does not capture program_name.
Full CPU/reads per application would require Resource Governor or Query Store.
Source: collect.session_stats (last 24 hours)
*******************************************************************************/

CREATE OR ALTER VIEW
    report.finops_application_resource_usage
AS
SELECT
    application_name =
        ss.top_application_name,
    avg_connections =
        AVG(ss.top_application_connections),
    max_connections =
        MAX(ss.top_application_connections),
    sample_count =
        COUNT_BIG(*),
    first_seen =
        MIN(ss.collection_time),
    last_seen =
        MAX(ss.collection_time)
FROM collect.session_stats AS ss
WHERE ss.collection_time >= DATEADD(HOUR, -24, SYSDATETIME())
AND   ss.top_application_name IS NOT NULL
GROUP BY
    ss.top_application_name;
GO

PRINT 'Created report.finops_application_resource_usage view';
GO

PRINT 'FinOps reporting views created successfully';
PRINT 'Views: report.finops_database_resource_usage, report.finops_utilization_efficiency,';
PRINT '       report.finops_peak_utilization, report.finops_application_resource_usage';
GO
