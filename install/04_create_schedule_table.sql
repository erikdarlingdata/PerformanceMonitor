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
NOTE: The config.collection_schedule table is created in 01_install_database.sql
This script initializes the schedule with default collector configurations
*/
/*
Initialize schedule with default collector configurations
Balanced for general monitoring with reasonable frequencies
Add trace analysis collector entry (disabled by default)
*/

INSERT INTO
    config.collection_schedule
(
    collector_name,
    enabled,
    frequency_minutes,
    max_duration_minutes,
    retention_days,
    description
)
SELECT
    v.*
FROM
(
    VALUES
    (N'wait_stats_collector', 1, 1, 2, 30, N'Wait statistics - high frequency for trending'),
    (N'query_stats_collector', 1, 2, 5, 30, N'Plan cache queries - recent activity focused'),
    (N'memory_stats_collector', 1, 1, 2, 30, N'Memory pressure monitoring'),
    (N'memory_pressure_events_collector', 1, 1, 5, 30, N'Ring buffer system events'),
    (N'system_health_collector', 1, 5, 10, 30, N'System health extended events via sp_HealthParser'),
    (N'blocked_process_xml_collector', 1, 1, 2, 30, N'Fast blocked process XML collection (chain-triggers parser + analyzer)'),
    (N'deadlock_xml_collector', 1, 1, 3, 30, N'Fast deadlock XML collection (chain-triggers parser + analyzer)'),
    (N'process_blocked_process_xml', 1, 5, 5, 30, N'Parse blocked process XML via sp_HumanEventsBlockViewer (also chain-triggered)'),
    (N'blocking_deadlock_analyzer', 1, 5, 5, 30, N'Analyze blocking/deadlock trends (also chain-triggered)'),
    (N'process_deadlock_xml', 1, 5, 5, 30, N'Parse deadlock XML via sp_BlitzLock (also chain-triggered)'),
    (N'query_store_collector', 1, 2, 10, 30, N'Query Store data collection'),
    (N'procedure_stats_collector', 1, 2, 10, 30, N'Procedure/trigger/function statistics'),
    (N'query_snapshots_collector', 1, 1, 2, 10, N'Currently executing queries with session wait stats (every minute - high frequency)'),
    (N'file_io_stats_collector', 1, 1, 2, 30, N'File I/O statistics from dm_io_virtual_file_stats'),
    (N'memory_grant_stats_collector', 1, 1, 2, 30, N'Memory grant semaphore pressure monitoring'),
    (N'cpu_scheduler_stats_collector', 1, 1, 2, 30, N'CPU scheduler and workload group statistics'),
    (N'memory_clerks_stats_collector', 1, 5, 3, 30, N'Memory clerk allocation tracking'),
    (N'perfmon_stats_collector', 1, 5, 2, 30, N'Performance counter statistics from dm_os_performance_counters'),
    (N'cpu_utilization_stats_collector', 1, 1, 2, 30, N'CPU utilization from ring buffer (SQL vs other processes)'),
    (N'trace_management_collector', 1, 1440, 5, 30, N'SQL Trace management for long-running queries'),
    (N'trace_analysis_collector', 1, 2, 5, 30, N'Process trace files into analysis tables'),
    (N'default_trace_collector', 1, 5, 3, 30, N'System events from default trace (memory, autogrow, config changes)'),
    (N'server_configuration_collector', 1, 1440, 5, 30, N'Server-level configuration settings and trace flags (daily collection)'),
    (N'database_configuration_collector', 1, 1440, 10, 30, N'Database-level configuration settings including scoped configs (daily collection)'),
    (N'configuration_issues_analyzer', 1, 1, 2, 30, N'Analyze configuration for issues: database config (Query Store, auto shrink/close), memory/CPU pressure warnings, server config (MAXDOP, priority boost)'),
    (N'latch_stats_collector', 1, 1, 3, 30, N'Latch contention statistics - internal synchronization object waits'),
    (N'spinlock_stats_collector', 1, 1, 3, 30, N'Spinlock contention statistics - lightweight synchronization primitive collisions'),
    (N'tempdb_stats_collector', 1, 1, 2, 30, N'TempDB space usage - version store, user/internal objects, allocation contention'),
    (N'plan_cache_stats_collector', 1, 5, 5, 30, N'Plan cache composition statistics - single-use plans and plan cache bloat detection'),
    (N'session_stats_collector', 1, 1, 2, 30, N'Session and connection statistics - connection leaks and application patterns'),
    (N'waiting_tasks_collector', 1, 1, 2, 30, N'Currently waiting tasks - blocking chains and wait analysis'),
    (N'running_jobs_collector', 1, 1, 2, 7, N'Currently running SQL Agent jobs with historical duration comparison')
) AS v (collector_name, enabled, frequency_minutes, max_duration_minutes, retention_days, description)
WHERE NOT EXISTS
(
    SELECT
        1/0
    FROM config.collection_schedule AS cs
    WHERE cs.collector_name = v.collector_name
);

PRINT 'Initialized collection schedule with default configurations';

/*
Disable collectors that are incompatible with Azure SQL Managed Instance (engine edition 8)

default_trace_collector:          default trace uses local file paths, MI uses Azure blob storage
trace_management_collector:       SQL Trace (sp_trace_*) not supported on MI
trace_analysis_collector:         processes trace files from trace_management, no data on MI
system_health_collector:          XE file targets require https:// blob URLs on MI, sp_HealthParser incompatible
*/
IF CONVERT(integer, SERVERPROPERTY('EngineEdition')) = 8
BEGIN
    UPDATE
        config.collection_schedule
    SET
        enabled = 0
    WHERE collector_name IN
    (
        N'default_trace_collector',
        N'trace_management_collector',
        N'trace_analysis_collector',
        N'system_health_collector'
    );

    PRINT 'Disabled 4 collectors incompatible with Azure SQL Managed Instance';
END;

/*
Disable collectors that are incompatible with AWS RDS for SQL Server

running_jobs_collector:          msdb.dbo.syssessions not accessible with RDS permissions
*/
IF DB_ID('rdsadmin') IS NOT NULL
BEGIN
    UPDATE
        config.collection_schedule
    SET
        enabled = 0
    WHERE collector_name IN
    (
        N'running_jobs_collector'
    );

    PRINT 'Disabled 1 collector incompatible with AWS RDS for SQL Server';
END;

/*
Calculate initial next_run_time for all collectors
Stagger start times to avoid contention
*/
UPDATE
    config.collection_schedule
SET
    next_run_time = DATEADD(SECOND, (schedule_id * 2), SYSDATETIME()),
    last_run_time = NULL
WHERE next_run_time IS NULL;

PRINT 'Collection schedule configured successfully';
PRINT 'Use config.collection_schedule table to modify collector frequencies and settings';
GO

/*
Display current schedule configuration
*/
SELECT
    collector_name,
    enabled,
    frequency_minutes,
    next_run_time,
    max_duration_minutes,
    description
FROM config.collection_schedule
ORDER BY
    next_run_time;
GO
