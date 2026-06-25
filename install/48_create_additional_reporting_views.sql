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

/*******************************************************************************
Additional Reporting Views for Previously Uncovered Collection Tables
Created: 2026-01-05
Purpose: Provide analytical views for 6 collection tables that previously
         had no reporting coverage
*******************************************************************************/

/*
=============================================================================
MEMORY USAGE TRENDS
Shows overall SQL Server memory consumption over time
Detects significant CHANGES in memory allocation using tiered thresholds
based on buffer pool size:
    < 16 GB:    25% change threshold
    16-64 GB:   20% change threshold
    64-256 GB:  15% change threshold
    256 GB+:    10% change threshold
=============================================================================
*/
CREATE OR ALTER VIEW
    report.memory_usage_trends
AS
WITH
    memory_with_deltas AS
(
    SELECT
        ms.collection_time,
        ms.buffer_pool_mb,
        ms.plan_cache_mb,
        ms.other_memory_mb,
        ms.total_memory_mb,
        ms.physical_memory_in_use_mb,
        ms.available_physical_memory_mb,
        ms.memory_utilization_percentage,
        ms.buffer_pool_percentage,
        ms.plan_cache_percentage,
        ms.buffer_pool_pressure_warning,
        ms.plan_cache_pressure_warning,
        /*
        Calculate changes from previous collection
        */
        prev_buffer_pool_mb = LAG(ms.buffer_pool_mb) OVER
            (
                ORDER BY
                    ms.collection_time
            ),
        prev_plan_cache_mb = LAG(ms.plan_cache_mb) OVER
            (
                ORDER BY
                    ms.collection_time
            ),
        prev_memory_utilization = LAG(ms.memory_utilization_percentage) OVER
            (
                ORDER BY
                    ms.collection_time
            ),
        /*
        Track previous pressure warnings to detect new alerts
        */
        prev_buffer_pool_pressure = LAG(ms.buffer_pool_pressure_warning) OVER
            (
                ORDER BY
                    ms.collection_time
            ),
        prev_plan_cache_pressure = LAG(ms.plan_cache_pressure_warning) OVER
            (
                ORDER BY
                    ms.collection_time
            )
    FROM collect.memory_stats AS ms
    WHERE ms.collection_time >= DATEADD(HOUR, -24, SYSDATETIME())
),
    memory_with_pct_change AS
(
    SELECT
        mwd.*,
        /*
        Calculate percentage changes
        */
        buffer_pool_pct_change =
            CASE
                WHEN mwd.prev_buffer_pool_mb IS NULL
                OR   mwd.prev_buffer_pool_mb = 0
                THEN 0
                ELSE ((mwd.buffer_pool_mb - mwd.prev_buffer_pool_mb) / mwd.prev_buffer_pool_mb) * 100
            END,
        plan_cache_pct_change =
            CASE
                WHEN mwd.prev_plan_cache_mb IS NULL
                OR   mwd.prev_plan_cache_mb = 0
                THEN 0
                ELSE ((mwd.plan_cache_mb - mwd.prev_plan_cache_mb) / mwd.prev_plan_cache_mb) * 100
            END,
        /*
        Determine threshold based on buffer pool size (in GB)
        < 16 GB:    25%
        16-64 GB:   20%
        64-256 GB:  15%
        256 GB+:    10%
        */
        change_threshold =
            CASE
                WHEN mwd.buffer_pool_mb < 16384
                THEN 25.0
                WHEN mwd.buffer_pool_mb < 65536
                THEN 20.0
                WHEN mwd.buffer_pool_mb < 262144
                THEN 15.0
                ELSE 10.0
            END
    FROM memory_with_deltas AS mwd
)
SELECT TOP (100)
    mpc.collection_time,
    mpc.buffer_pool_mb,
    mpc.plan_cache_mb,
    mpc.other_memory_mb,
    mpc.total_memory_mb,
    mpc.physical_memory_in_use_mb,
    mpc.available_physical_memory_mb,
    mpc.memory_utilization_percentage,
    mpc.buffer_pool_percentage,
    mpc.plan_cache_percentage,
    /*
    Show actual deltas
    */
    buffer_pool_change_mb = mpc.buffer_pool_mb - mpc.prev_buffer_pool_mb,
    plan_cache_change_mb = mpc.plan_cache_mb - mpc.prev_plan_cache_mb,
    buffer_pool_pct_change = mpc.buffer_pool_pct_change,
    plan_cache_pct_change = mpc.plan_cache_pct_change,
    memory_utilization_change = mpc.memory_utilization_percentage - mpc.prev_memory_utilization,
    /*
    State based on significant CHANGES using tiered thresholds
    */
    memory_state =
        CASE
            WHEN mpc.prev_memory_utilization IS NULL
            THEN N'BASELINE'
            WHEN mpc.memory_utilization_percentage - mpc.prev_memory_utilization >= 5
            THEN N'SPIKE'
            WHEN mpc.memory_utilization_percentage - mpc.prev_memory_utilization <= -5
            THEN N'DROP'
            ELSE N'STABLE'
        END,
    buffer_pool_state =
        CASE
            /*
            Only show pressure warning if it is NEW (was not set before)
            */
            WHEN mpc.buffer_pool_pressure_warning = 1
            AND  ISNULL(mpc.prev_buffer_pool_pressure, 0) = 0
            THEN N'PRESSURE WARNING (new)'
            WHEN mpc.prev_buffer_pool_mb IS NULL
            THEN N'BASELINE'
            WHEN mpc.buffer_pool_pct_change <= -mpc.change_threshold
            THEN N'SHRINK'
            WHEN mpc.buffer_pool_pct_change >= mpc.change_threshold
            THEN N'GROWTH'
            ELSE N'STABLE'
        END,
    plan_cache_state =
        CASE
            /*
            Only show pressure warning if it is NEW (was not set before)
            */
            WHEN mpc.plan_cache_pressure_warning = 1
            AND  ISNULL(mpc.prev_plan_cache_pressure, 0) = 0
            THEN N'PRESSURE WARNING (new)'
            WHEN mpc.prev_plan_cache_mb IS NULL
            THEN N'BASELINE'
            /*
            Plan cache flush: only flag when substantial cache was wiped
            Previous > 100 MB AND current < 20 MB suggests real flush
            */
            WHEN mpc.prev_plan_cache_mb > 100
            AND  mpc.plan_cache_mb < 20
            THEN N'FLUSH'
            WHEN mpc.plan_cache_pct_change >= mpc.change_threshold
            THEN N'GROWTH'
            ELSE N'STABLE'
        END,
    recommendation =
        CASE
            WHEN mpc.buffer_pool_pressure_warning = 1
            AND  ISNULL(mpc.prev_buffer_pool_pressure, 0) = 0
            THEN N'Buffer pool pressure detected - check for memory grants, large queries'
            WHEN mpc.plan_cache_pressure_warning = 1
            AND  ISNULL(mpc.prev_plan_cache_pressure, 0) = 0
            THEN N'Plan cache pressure - check for ad-hoc queries, enable optimize for ad hoc'
            WHEN mpc.buffer_pool_pct_change <= -mpc.change_threshold
            THEN N'Investigate buffer pool drop - possible memory pressure or large query'
            WHEN mpc.prev_plan_cache_mb > 100
            AND  mpc.plan_cache_mb < 20
            THEN N'Plan cache flush detected - check for DBCC FREEPROCCACHE or memory pressure'
            ELSE N''
        END
FROM memory_with_pct_change AS mpc
ORDER BY
    mpc.collection_time DESC;
GO

/*
=============================================================================
RUNNING JOBS
Shows the latest snapshot of currently running SQL Agent jobs with
formatted durations and historical comparison
=============================================================================
*/
CREATE OR ALTER VIEW
    report.running_jobs
AS
WITH
    latest_snapshot AS
(
    SELECT TOP (1)
        latest_time = rj.collection_time
    FROM collect.running_jobs AS rj
    ORDER BY
        rj.collection_time DESC
)
SELECT
    rj.collection_time,
    rj.job_name,
    rj.job_id,
    rj.job_enabled,
    rj.start_time,
    rj.current_duration_seconds,
    current_duration_formatted =
        RIGHT(N'00' + CONVERT(nvarchar(10), rj.current_duration_seconds / 3600), 2) + N':' +
        RIGHT(N'00' + CONVERT(nvarchar(10), (rj.current_duration_seconds % 3600) / 60), 2) + N':' +
        RIGHT(N'00' + CONVERT(nvarchar(10), rj.current_duration_seconds % 60), 2),
    rj.avg_duration_seconds,
    avg_duration_formatted =
        CASE
            WHEN rj.avg_duration_seconds IS NULL
            THEN N'N/A'
            ELSE
                RIGHT(N'00' + CONVERT(nvarchar(10), rj.avg_duration_seconds / 3600), 2) + N':' +
                RIGHT(N'00' + CONVERT(nvarchar(10), (rj.avg_duration_seconds % 3600) / 60), 2) + N':' +
                RIGHT(N'00' + CONVERT(nvarchar(10), rj.avg_duration_seconds % 60), 2)
        END,
    rj.p95_duration_seconds,
    p95_duration_formatted =
        CASE
            WHEN rj.p95_duration_seconds IS NULL
            THEN N'N/A'
            ELSE
                RIGHT(N'00' + CONVERT(nvarchar(10), rj.p95_duration_seconds / 3600), 2) + N':' +
                RIGHT(N'00' + CONVERT(nvarchar(10), (rj.p95_duration_seconds % 3600) / 60), 2) + N':' +
                RIGHT(N'00' + CONVERT(nvarchar(10), rj.p95_duration_seconds % 60), 2)
        END,
    rj.successful_run_count,
    rj.is_running_long,
    rj.percent_of_average,
    duration_status =
        CASE
            WHEN rj.is_running_long = 1
            THEN N'LONG RUNNING'
            WHEN rj.percent_of_average IS NULL
            THEN N'NO HISTORY'
            WHEN rj.percent_of_average > 150.0
            THEN N'ABOVE AVERAGE'
            ELSE N'NORMAL'
        END
FROM collect.running_jobs AS rj
JOIN latest_snapshot AS ls
  ON ls.latest_time = rj.collection_time;
GO

PRINT 'Additional reporting views created successfully';
GO
