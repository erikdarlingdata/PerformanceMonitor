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
Troubleshooting Commands for Performance Monitor Collectors
Run individual collectors with @debug = 1 to troubleshoot collection issues
*/

/*
Check recent collection log for errors
*/
SELECT TOP (100)
    cl.collection_time,
    cl.collector_name,
    cl.collection_status,
    cl.rows_collected,
    cl.duration_ms,
    cl.error_message
FROM config.collection_log AS cl
ORDER BY
    cl.collection_time DESC;
GO

/*
Check for recent errors only
*/
SELECT TOP (100)
    cl.collection_time,
    cl.collector_name,
    cl.collection_status,
    cl.rows_collected,
    cl.duration_ms,
    cl.error_message
FROM config.collection_log AS cl
WHERE cl.collection_status = N'ERROR'
ORDER BY
    cl.collection_time DESC;
GO

/*
Check collection schedule configuration
*/
SELECT
    cs.collector_name,
    cs.enabled,
    cs.frequency_minutes,
    cs.last_run_time,
    cs.next_run_time
FROM config.collection_schedule AS cs
ORDER BY
    cs.collector_name;
GO

/*
===============================================================================
CORE PERFORMANCE COLLECTORS
===============================================================================
*/

/*
1. Wait Stats Collector
Collects cumulative wait statistics with delta calculations
*/
EXECUTE collect.wait_stats_collector
    @debug = 1;
GO

/*
2. Query Stats Collector
Collects query execution statistics with comprehensive min/max/last metrics
*/
EXECUTE collect.query_stats_collector
    @debug = 1;
GO

/*
3. Memory Stats Collector
Collects memory usage with pressure warnings for buffer pool and plan cache drops
*/
EXECUTE collect.memory_stats_collector
    @debug = 1;
GO

/*
4. File I/O Stats Collector
Collects file I/O statistics with delta calculations
*/
EXECUTE collect.file_io_stats_collector
    @debug = 1;
GO

/*
5. Memory Grant Stats Collector
Collects memory grant semaphore data with pressure warnings
*/
EXECUTE collect.memory_grant_stats_collector
    @debug = 1;
GO

/*
6. CPU Scheduler Stats Collector
Collects scheduler and workload group data with CPU pressure warnings
*/
EXECUTE collect.cpu_scheduler_stats_collector
    @debug = 1;
GO

/*
7. Memory Clerks Stats Collector
Collects all memory clerk data with delta calculations for memory growth tracking
*/
EXECUTE collect.memory_clerks_stats_collector
    @debug = 1;
GO

/*
8. CPU Utilization Stats Collector
Collects CPU utilization from ring buffers
*/
EXECUTE collect.cpu_utilization_stats_collector
    @debug = 1;
GO

/*
9. Performance Counter Stats Collector
Collects performance counter statistics from dm_os_performance_counters
*/
EXECUTE collect.perfmon_stats_collector
    @debug = 1;
GO

/*
10. Procedure Stats Collector
Collects stored procedure, trigger, and function statistics
*/
EXECUTE collect.procedure_stats_collector
    @debug = 1;
GO
/*
11. Query Snapshots Collector (sp_WhoIsActive)
Captures currently executing queries using sp_WhoIsActive
Note: Requires sp_WhoIsActive to be installed
*/
EXECUTE collect.query_snapshots_collector
    @debug = 1;
GO

/*
12. Query Store Collector
Collects Query Store data from all enabled databases
*/
EXECUTE collect.query_store_collector
    @debug = 1;
GO

/*
===============================================================================
EXTENDED EVENTS COLLECTORS
===============================================================================
*/

/*
13. Ring Buffer Events Collector
Collects extended events from ring buffer targets
*/
EXECUTE collect.memory_pressure_events_collector
    @debug = 1;
GO

/*
14. System Health Collector (sp_HealthParser)
Processes system health extended events
Note: Requires sp_HealthParser to be installed
*/
EXECUTE collect.system_health_collector
    @what_to_check = 'all',
    @hours_back = 1,
    @warnings_only = 1,
    @log_retention_days = 30,
    @debug = 1;
GO

/*
15. Blocked Process XML Collector
Collects blocked process report XML from extended events
*/
EXECUTE collect.blocked_process_xml_collector
    @debug = 1;
GO

/*
16. Deadlock XML Collector
Collects deadlock graph XML from extended events
*/
EXECUTE collect.deadlock_xml_collector
    @debug = 1;
GO
/*
===============================================================================
XML PROCESSING PROCEDURES
===============================================================================
*/

/*
17. Process Blocked Process XML (sp_HumanEventsBlockViewer)
Processes blocked process report XML events
Note: Requires sp_HumanEventsBlockViewer to be installed
*/
EXECUTE collect.process_blocked_process_xml
    @max_events_to_process = 1000,
    @start_date = NULL,
    @end_date = NULL,
    @log_retention_days = 30,
    @debug = 1;
GO

/*
18. Process Deadlock XML (sp_BlitzLock)
Processes deadlock graph XML events
Note: Requires sp_BlitzLock to be installed
*/
EXECUTE collect.process_deadlock_xml
    @start_date = NULL,
    @end_date = NULL,
    @log_retention_days = 30,
    @debug = 1;
GO

/*
===============================================================================
MONITORING AND ANALYSIS PROCEDURES
===============================================================================
*/

/*
19. Blocking and Deadlock Analyzer
Aggregates blocking/deadlock events by database and alerts on significant increases
*/
EXECUTE collect.blocking_deadlock_analyzer
    @lookback_hours = 1000,
    @blocking_increase_threshold_percent = 1,
    @deadlock_increase_threshold_percent = 1,
    @min_blocking_duration_ms = 100,
    @debug = 1;
GO

/*
===============================================================================
SQL TRACE COLLECTORS
===============================================================================
*/

/*
20. Trace Management Collector
Creates and manages SQL Server traces for long-running queries
*/
EXECUTE collect.trace_management_collector
    @action = 'STATUS',
    @debug = 1;
GO

/*
21. Trace Analysis Collector
Processes SQL Trace files into analysis tables
*/
EXECUTE collect.trace_analysis_collector
    @debug = 1;
GO

/*
22. Default Trace Collector
Collects system events from default trace (memory, autogrow, config changes)
*/
EXECUTE collect.default_trace_collector
    @debug = 1;
GO

/*
===============================================================================
CONFIGURATION COLLECTORS
===============================================================================
*/

/*
23. Server Configuration Collector
Collects server-level configuration settings and trace flags
*/
EXECUTE collect.server_configuration_collector
    @debug = 1;
GO

/*
24. Database Configuration Collector
Collects database-level configuration settings including scoped configs
*/
EXECUTE collect.database_configuration_collector
    @debug = 1;
GO

/*
===============================================================================
ANALYZERS
===============================================================================
*/

/*
25. Configuration Issues Analyzer
Analyzes database configuration for issues and logs findings to config.critical_issues table
Database Checks: Query Store disabled, auto shrink enabled, auto close enabled
Pre-Calculated Warnings: Memory pressure, memory grant pressure, CPU scheduler pressure
Server Config Checks: Priority boost, lightweight pooling, MAXDOP, cost threshold for parallelism
*/
EXECUTE collect.configuration_issues_analyzer
    @debug = 1;
GO

/*
26. Latch Stats Collector
Collects latch contention statistics from sys.dm_os_latch_stats
*/
EXECUTE collect.latch_stats_collector
    @debug = 1;
GO

/*
27. Spinlock Stats Collector
Collects spinlock contention statistics from sys.dm_os_spinlock_stats
*/
EXECUTE collect.spinlock_stats_collector
    @debug = 1;
GO

/*
28. TempDB Stats Collector
Collects TempDB space usage and contention metrics combining file space, task space, and session space
*/
EXECUTE collect.tempdb_stats_collector
    @debug = 1;
GO

/*
29. Plan Cache Stats Collector
Collects plan cache composition statistics for detecting single-use plans and plan cache bloat
*/
EXECUTE collect.plan_cache_stats_collector
    @debug = 1;
GO

/*
30. Session Stats Collector
Collects aggregated session and connection statistics for identifying connection leaks and patterns
*/
EXECUTE collect.session_stats_collector
    @debug = 1;
GO

/*
31. Waiting Tasks Collector
Collects currently waiting tasks with blocking information and query details
*/
EXECUTE collect.waiting_tasks_collector
    @debug = 1;
GO

/*
===============================================================================
UTILITY COMMANDS
===============================================================================
*/

/*
Check row counts in all collection tables
*/
SELECT
    table_name = N'wait_stats',
    row_count = COUNT_BIG(*)
FROM collect.wait_stats

UNION ALL

SELECT
    table_name = N'query_stats',
    row_count = COUNT_BIG(*)
FROM collect.query_stats

UNION ALL

SELECT
    table_name = N'memory_stats',
    row_count = COUNT_BIG(*)
FROM collect.memory_stats

UNION ALL

SELECT
    table_name = N'file_io_stats',
    row_count = COUNT_BIG(*)
FROM collect.file_io_stats

UNION ALL

SELECT
    table_name = N'memory_grant_stats',
    row_count = COUNT_BIG(*)
FROM collect.memory_grant_stats

UNION ALL

SELECT
    table_name = N'cpu_scheduler_stats',
    row_count = COUNT_BIG(*)
FROM collect.cpu_scheduler_stats

UNION ALL

SELECT
    table_name = N'memory_clerks_stats',
    row_count = COUNT_BIG(*)
FROM collect.memory_clerks_stats

UNION ALL

SELECT
    table_name = N'cpu_utilization_stats',
    row_count = COUNT_BIG(*)
FROM collect.cpu_utilization_stats

UNION ALL

SELECT
    table_name = N'procedure_stats',
    row_count = COUNT_BIG(*)
FROM collect.procedure_stats

UNION ALL

SELECT
    table_name = N'query_store_data',
    row_count = COUNT_BIG(*)
FROM collect.query_store_data

UNION ALL

SELECT
    table_name = N'memory_pressure_events',
    row_count = COUNT_BIG(*)
FROM collect.memory_pressure_events

UNION ALL

SELECT
    table_name = N'blocked_process_xml',
    row_count = COUNT_BIG(*)
FROM collect.blocked_process_xml

UNION ALL

SELECT
    table_name = N'deadlock_xml',
    row_count = COUNT_BIG(*)
FROM collect.deadlock_xml

UNION ALL

SELECT
    table_name = N'perfmon_stats',
    row_count = COUNT_BIG(*)
FROM collect.perfmon_stats

UNION ALL

/*Note: query_snapshots table requires sp_WhoIsActive to be installed*/
/*SELECT
    table_name = N'query_snapshots',
    row_count = COUNT_BIG(*)
FROM collect.query_snapshots

UNION ALL*/

SELECT
    table_name = N'trace_analysis',
    row_count = COUNT_BIG(*)
FROM collect.trace_analysis

UNION ALL

SELECT
    table_name = N'default_trace_events',
    row_count = COUNT_BIG(*)
FROM collect.default_trace_events

UNION ALL

/*Note: server_configuration and database_configuration use config schema tables*/
/*SELECT
    table_name = N'server_configuration',
    row_count = COUNT_BIG(*)
FROM collect.server_configuration

UNION ALL

SELECT
    table_name = N'database_configuration',
    row_count = COUNT_BIG(*)
FROM collect.database_configuration

UNION ALL*/

SELECT
    table_name = N'latch_stats',
    row_count = COUNT_BIG(*)
FROM collect.latch_stats

UNION ALL

SELECT
    table_name = N'spinlock_stats',
    row_count = COUNT_BIG(*)
FROM collect.spinlock_stats

UNION ALL

SELECT
    table_name = N'tempdb_stats',
    row_count = COUNT_BIG(*)
FROM collect.tempdb_stats

UNION ALL

SELECT
    table_name = N'plan_cache_stats',
    row_count = COUNT_BIG(*)
FROM collect.plan_cache_stats

UNION ALL

SELECT
    table_name = N'session_stats',
    row_count = COUNT_BIG(*)
FROM collect.session_stats

UNION ALL

SELECT
    table_name = N'waiting_tasks',
    row_count = COUNT_BIG(*)
FROM collect.waiting_tasks

ORDER BY
    table_name;
GO

/*
Check most recent collection times for each table
*/
SELECT
    table_name = N'wait_stats',
    max_collection_time = MAX(ws.collection_time),
    max_collection_id = MAX(ws.collection_id)
FROM collect.wait_stats AS ws

UNION ALL

SELECT
    table_name = N'query_stats',
    max_collection_time = MAX(qs.collection_time),
    max_collection_id = MAX(qs.collection_id)
FROM collect.query_stats AS qs

UNION ALL

SELECT
    table_name = N'memory_stats',
    max_collection_time = MAX(ms.collection_time),
    max_collection_id = MAX(ms.collection_id)
FROM collect.memory_stats AS ms

UNION ALL

SELECT
    table_name = N'file_io_stats',
    max_collection_time = MAX(fios.collection_time),
    max_collection_id = MAX(fios.collection_id)
FROM collect.file_io_stats AS fios

UNION ALL

SELECT
    table_name = N'memory_grant_stats',
    max_collection_time = MAX(mgs.collection_time),
    max_collection_id = MAX(mgs.collection_id)
FROM collect.memory_grant_stats AS mgs

UNION ALL

SELECT
    table_name = N'cpu_scheduler_stats',
    max_collection_time = MAX(css.collection_time),
    max_collection_id = MAX(css.collection_id)
FROM collect.cpu_scheduler_stats AS css

UNION ALL

SELECT
    table_name = N'memory_clerks_stats',
    max_collection_time = MAX(mcs.collection_time),
    max_collection_id = MAX(mcs.collection_id)
FROM collect.memory_clerks_stats AS mcs

UNION ALL

SELECT
    table_name = N'cpu_utilization_stats',
    max_collection_time = MAX(cus.collection_time),
    max_collection_id = MAX(cus.collection_id)
FROM collect.cpu_utilization_stats AS cus

UNION ALL

SELECT
    table_name = N'procedure_stats',
    max_collection_time = MAX(ps.collection_time),
    max_collection_id = MAX(ps.collection_id)
FROM collect.procedure_stats AS ps

UNION ALL

SELECT
    table_name = N'query_store_data',
    max_collection_time = MAX(qsd.collection_time),
    max_collection_id = MAX(qsd.collection_id)
FROM collect.query_store_data AS qsd

UNION ALL

SELECT
    table_name = N'perfmon_stats',
    max_collection_time = MAX(pms.collection_time),
    max_collection_id = MAX(pms.collection_id)
FROM collect.perfmon_stats AS pms

UNION ALL

/*Note: query_snapshots table requires sp_WhoIsActive to be installed*/
/*SELECT
    table_name = N'query_snapshots',
    max_collection_time = MAX(qsnap.collection_time),
    max_collection_id = MAX(qsnap.collection_id)
FROM collect.query_snapshots AS qsnap

UNION ALL*/

SELECT
    table_name = N'trace_analysis',
    max_collection_time = MAX(ta.collection_time),
    max_collection_id = MAX(ta.analysis_id)
FROM collect.trace_analysis AS ta

UNION ALL

SELECT
    table_name = N'default_trace_events',
    max_collection_time = MAX(dte.collection_time),
    max_collection_id = MAX(dte.event_id)
FROM collect.default_trace_events AS dte

UNION ALL

/*Note: server_configuration and database_configuration use config schema tables*/
/*SELECT
    table_name = N'server_configuration',
    max_collection_time = MAX(sc.collection_time),
    max_collection_id = MAX(sc.collection_id)
FROM collect.server_configuration AS sc

UNION ALL

SELECT
    table_name = N'database_configuration',
    max_collection_time = MAX(dc.collection_time),
    max_collection_id = MAX(dc.collection_id)
FROM collect.database_configuration AS dc

UNION ALL*/

SELECT
    table_name = N'latch_stats',
    max_collection_time = MAX(ls.collection_time),
    max_collection_id = MAX(ls.collection_id)
FROM collect.latch_stats AS ls

UNION ALL

SELECT
    table_name = N'spinlock_stats',
    max_collection_time = MAX(ss.collection_time),
    max_collection_id = MAX(ss.collection_id)
FROM collect.spinlock_stats AS ss

UNION ALL

SELECT
    table_name = N'tempdb_stats',
    max_collection_time = MAX(ts.collection_time),
    max_collection_id = MAX(ts.collection_id)
FROM collect.tempdb_stats AS ts

UNION ALL

SELECT
    table_name = N'plan_cache_stats',
    max_collection_time = MAX(pcs.collection_time),
    max_collection_id = MAX(pcs.collection_id)
FROM collect.plan_cache_stats AS pcs

UNION ALL

SELECT
    table_name = N'session_stats',
    max_collection_time = MAX(ses.collection_time),
    max_collection_id = MAX(ses.collection_id)
FROM collect.session_stats AS ses

UNION ALL

SELECT
    table_name = N'waiting_tasks',
    max_collection_time = MAX(wt.collection_time),
    max_collection_id = MAX(wt.collection_id)
FROM collect.waiting_tasks AS wt

ORDER BY
    table_name;
GO

/*
Clear collection log (use with caution - commented out by default)

TRUNCATE TABLE config.collection_log;
*/

/*
Manually trigger scheduled master collector (commented out by default)

-- Run all collectors immediately (ignore schedule):
EXECUTE collect.scheduled_master_collector
    @force_run_all = 1,
    @debug = 1;

-- Run only collectors that are due per schedule:
EXECUTE collect.scheduled_master_collector
    @debug = 1;
*/

PRINT 'Troubleshooting commands completed';
PRINT 'Review output for errors and warnings';
GO
