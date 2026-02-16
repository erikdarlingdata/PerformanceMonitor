/*
CI Validation: Verify all expected objects exist after installation.
Run with sqlcmd -b to fail on RAISERROR.
*/

SET NOCOUNT ON;

USE PerformanceMonitor;
GO

PRINT '========================================';
PRINT 'CI Installation Validation';
PRINT '========================================';
PRINT '';

DECLARE
    @missing int = 0,
    @checked int = 0;

/*
Schemas (4)
*/
PRINT 'Checking schemas...';

IF SCHEMA_ID(N'collect') IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: schema collect'; END; SET @checked += 1;
IF SCHEMA_ID(N'analyze') IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: schema analyze'; END; SET @checked += 1;
IF SCHEMA_ID(N'config')  IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: schema config'; END;  SET @checked += 1;
IF SCHEMA_ID(N'report')  IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: schema report'; END;  SET @checked += 1;

PRINT '';

/*
Procedures in collect schema (37)
*/
PRINT 'Checking collect procedures...';

IF OBJECT_ID(N'collect.calculate_deltas', N'P')                IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.calculate_deltas'; END;                SET @checked += 1;
IF OBJECT_ID(N'collect.wait_stats_collector', N'P')             IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.wait_stats_collector'; END;             SET @checked += 1;
IF OBJECT_ID(N'collect.query_stats_collector', N'P')            IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.query_stats_collector'; END;            SET @checked += 1;
IF OBJECT_ID(N'collect.query_store_collector', N'P')            IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.query_store_collector'; END;            SET @checked += 1;
IF OBJECT_ID(N'collect.procedure_stats_collector', N'P')        IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.procedure_stats_collector'; END;        SET @checked += 1;
IF OBJECT_ID(N'collect.query_snapshots_collector', N'P')        IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.query_snapshots_collector'; END;        SET @checked += 1;
IF OBJECT_ID(N'collect.query_snapshots_create_views', N'P')     IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.query_snapshots_create_views'; END;     SET @checked += 1;
IF OBJECT_ID(N'collect.query_snapshots_retention', N'P')        IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.query_snapshots_retention'; END;        SET @checked += 1;
IF OBJECT_ID(N'collect.memory_stats_collector', N'P')           IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.memory_stats_collector'; END;           SET @checked += 1;
IF OBJECT_ID(N'collect.memory_grant_stats_collector', N'P')     IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.memory_grant_stats_collector'; END;     SET @checked += 1;
IF OBJECT_ID(N'collect.memory_clerks_stats_collector', N'P')    IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.memory_clerks_stats_collector'; END;    SET @checked += 1;
IF OBJECT_ID(N'collect.cpu_scheduler_stats_collector', N'P')    IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.cpu_scheduler_stats_collector'; END;    SET @checked += 1;
IF OBJECT_ID(N'collect.cpu_utilization_stats_collector', N'P')  IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.cpu_utilization_stats_collector'; END;  SET @checked += 1;
IF OBJECT_ID(N'collect.perfmon_stats_collector', N'P')          IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.perfmon_stats_collector'; END;          SET @checked += 1;
IF OBJECT_ID(N'collect.file_io_stats_collector', N'P')          IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.file_io_stats_collector'; END;          SET @checked += 1;
IF OBJECT_ID(N'collect.blocked_process_xml_collector', N'P')    IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.blocked_process_xml_collector'; END;    SET @checked += 1;
IF OBJECT_ID(N'collect.process_blocked_process_xml', N'P')      IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.process_blocked_process_xml'; END;      SET @checked += 1;
IF OBJECT_ID(N'collect.deadlock_xml_collector', N'P')           IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.deadlock_xml_collector'; END;           SET @checked += 1;
IF OBJECT_ID(N'collect.process_deadlock_xml', N'P')             IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.process_deadlock_xml'; END;             SET @checked += 1;
IF OBJECT_ID(N'collect.blocking_deadlock_analyzer', N'P')       IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.blocking_deadlock_analyzer'; END;       SET @checked += 1;
IF OBJECT_ID(N'collect.memory_pressure_events_collector', N'P') IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.memory_pressure_events_collector'; END; SET @checked += 1;
IF OBJECT_ID(N'collect.system_health_collector', N'P')          IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.system_health_collector'; END;          SET @checked += 1;
IF OBJECT_ID(N'collect.default_trace_collector', N'P')          IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.default_trace_collector'; END;          SET @checked += 1;
IF OBJECT_ID(N'collect.trace_management_collector', N'P')       IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.trace_management_collector'; END;       SET @checked += 1;
IF OBJECT_ID(N'collect.trace_analysis_collector', N'P')         IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.trace_analysis_collector'; END;         SET @checked += 1;
IF OBJECT_ID(N'collect.latch_stats_collector', N'P')            IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.latch_stats_collector'; END;            SET @checked += 1;
IF OBJECT_ID(N'collect.spinlock_stats_collector', N'P')         IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.spinlock_stats_collector'; END;         SET @checked += 1;
IF OBJECT_ID(N'collect.tempdb_stats_collector', N'P')           IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.tempdb_stats_collector'; END;           SET @checked += 1;
IF OBJECT_ID(N'collect.plan_cache_stats_collector', N'P')       IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.plan_cache_stats_collector'; END;       SET @checked += 1;
IF OBJECT_ID(N'collect.session_stats_collector', N'P')          IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.session_stats_collector'; END;          SET @checked += 1;
IF OBJECT_ID(N'collect.waiting_tasks_collector', N'P')          IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.waiting_tasks_collector'; END;          SET @checked += 1;
IF OBJECT_ID(N'collect.session_wait_stats_collector', N'P')     IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.session_wait_stats_collector'; END;     SET @checked += 1;
IF OBJECT_ID(N'collect.server_configuration_collector', N'P')   IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.server_configuration_collector'; END;   SET @checked += 1;
IF OBJECT_ID(N'collect.database_configuration_collector', N'P') IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.database_configuration_collector'; END; SET @checked += 1;
IF OBJECT_ID(N'collect.configuration_issues_analyzer', N'P')    IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.configuration_issues_analyzer'; END;    SET @checked += 1;
IF OBJECT_ID(N'collect.scheduled_master_collector', N'P')       IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.scheduled_master_collector'; END;       SET @checked += 1;
IF OBJECT_ID(N'collect.running_jobs_collector', N'P')           IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: collect.running_jobs_collector'; END;           SET @checked += 1;

PRINT '';

/*
Procedures in config schema (10)
*/
PRINT 'Checking config procedures...';

IF OBJECT_ID(N'config.ensure_config_tables', N'P')       IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: config.ensure_config_tables'; END;       SET @checked += 1;
IF OBJECT_ID(N'config.ensure_collection_table', N'P')    IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: config.ensure_collection_table'; END;    SET @checked += 1;
IF OBJECT_ID(N'config.update_collector_frequency', N'P') IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: config.update_collector_frequency'; END; SET @checked += 1;
IF OBJECT_ID(N'config.set_collector_enabled', N'P')      IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: config.set_collector_enabled'; END;      SET @checked += 1;
IF OBJECT_ID(N'config.enable_realtime_monitoring', N'P') IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: config.enable_realtime_monitoring'; END; SET @checked += 1;
IF OBJECT_ID(N'config.enable_consulting_analysis', N'P') IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: config.enable_consulting_analysis'; END; SET @checked += 1;
IF OBJECT_ID(N'config.enable_baseline_monitoring', N'P') IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: config.enable_baseline_monitoring'; END; SET @checked += 1;
IF OBJECT_ID(N'config.show_collection_schedule', N'P')   IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: config.show_collection_schedule'; END;   SET @checked += 1;
IF OBJECT_ID(N'config.data_retention', N'P')             IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: config.data_retention'; END;             SET @checked += 1;
IF OBJECT_ID(N'config.check_hung_collector_job', N'P')   IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: config.check_hung_collector_job'; END;   SET @checked += 1;

PRINT '';

/*
Views in config schema (2)
*/
PRINT 'Checking config views...';

IF OBJECT_ID(N'config.current_version', N'V') IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: config.current_version'; END; SET @checked += 1;
IF OBJECT_ID(N'config.server_info', N'V')     IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: config.server_info'; END;     SET @checked += 1;

PRINT '';

/*
Views in report schema (37)
Note: report.query_snapshots and report.query_snapshots_blocking are created
dynamically by collect.query_snapshots_create_views, so they are not checked here.
*/
PRINT 'Checking report views...';

IF OBJECT_ID(N'report.query_stats_with_formatted_plans', N'V')    IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.query_stats_with_formatted_plans'; END;    SET @checked += 1;
IF OBJECT_ID(N'report.procedure_stats_with_formatted_plans', N'V') IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.procedure_stats_with_formatted_plans'; END; SET @checked += 1;
IF OBJECT_ID(N'report.query_store_stats_with_formatted_plans', N'V') IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.query_store_stats_with_formatted_plans'; END; SET @checked += 1;
IF OBJECT_ID(N'report.expensive_queries_today', N'V')             IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.expensive_queries_today'; END;             SET @checked += 1;
IF OBJECT_ID(N'report.query_stats_summary', N'V')                 IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.query_stats_summary'; END;                 SET @checked += 1;
IF OBJECT_ID(N'report.procedure_stats_summary', N'V')             IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.procedure_stats_summary'; END;             SET @checked += 1;
IF OBJECT_ID(N'report.query_store_summary', N'V')                 IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.query_store_summary'; END;                 SET @checked += 1;
IF OBJECT_ID(N'report.collection_health', N'V')                   IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.collection_health'; END;                   SET @checked += 1;
IF OBJECT_ID(N'report.top_waits_last_hour', N'V')                 IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.top_waits_last_hour'; END;                 SET @checked += 1;
IF OBJECT_ID(N'report.memory_pressure_events', N'V')              IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.memory_pressure_events'; END;              SET @checked += 1;
IF OBJECT_ID(N'report.cpu_spikes', N'V')                          IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.cpu_spikes'; END;                          SET @checked += 1;
IF OBJECT_ID(N'report.blocking_summary', N'V')                    IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.blocking_summary'; END;                    SET @checked += 1;
IF OBJECT_ID(N'report.deadlock_summary', N'V')                    IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.deadlock_summary'; END;                    SET @checked += 1;
IF OBJECT_ID(N'report.daily_summary', N'V')                       IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.daily_summary'; END;                       SET @checked += 1;
IF OBJECT_ID(N'report.daily_summary_v2', N'V')                    IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.daily_summary_v2'; END;                    SET @checked += 1;
IF OBJECT_ID(N'report.server_configuration_changes', N'V')        IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.server_configuration_changes'; END;        SET @checked += 1;
IF OBJECT_ID(N'report.database_configuration_changes', N'V')      IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.database_configuration_changes'; END;      SET @checked += 1;
IF OBJECT_ID(N'report.trace_flag_changes', N'V')                  IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.trace_flag_changes'; END;                  SET @checked += 1;
IF OBJECT_ID(N'report.top_latch_contention', N'V')                IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.top_latch_contention'; END;                SET @checked += 1;
IF OBJECT_ID(N'report.top_spinlock_contention', N'V')             IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.top_spinlock_contention'; END;             SET @checked += 1;
IF OBJECT_ID(N'report.tempdb_pressure', N'V')                     IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.tempdb_pressure'; END;                     SET @checked += 1;
IF OBJECT_ID(N'report.plan_cache_bloat', N'V')                    IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.plan_cache_bloat'; END;                    SET @checked += 1;
IF OBJECT_ID(N'report.top_memory_consumers', N'V')                IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.top_memory_consumers'; END;                SET @checked += 1;
IF OBJECT_ID(N'report.memory_grant_pressure', N'V')               IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.memory_grant_pressure'; END;               SET @checked += 1;
IF OBJECT_ID(N'report.file_io_latency', N'V')                     IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.file_io_latency'; END;                     SET @checked += 1;
IF OBJECT_ID(N'report.cpu_scheduler_pressure', N'V')              IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.cpu_scheduler_pressure'; END;              SET @checked += 1;
IF OBJECT_ID(N'report.long_running_query_patterns', N'V')         IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.long_running_query_patterns'; END;         SET @checked += 1;
IF OBJECT_ID(N'report.memory_pressure_indicators', N'V')          IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.memory_pressure_indicators'; END;          SET @checked += 1;
IF OBJECT_ID(N'report.file_io_wait_correlation', N'V')            IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.file_io_wait_correlation'; END;            SET @checked += 1;
IF OBJECT_ID(N'report.blocking_chain_analysis', N'V')             IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.blocking_chain_analysis'; END;             SET @checked += 1;
IF OBJECT_ID(N'report.tempdb_contention_analysis', N'V')          IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.tempdb_contention_analysis'; END;          SET @checked += 1;
IF OBJECT_ID(N'report.parameter_sensitivity_detection', N'V')     IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.parameter_sensitivity_detection'; END;     SET @checked += 1;
IF OBJECT_ID(N'report.scheduler_cpu_analysis', N'V')              IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.scheduler_cpu_analysis'; END;              SET @checked += 1;
IF OBJECT_ID(N'report.session_wait_analysis', N'V')               IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.session_wait_analysis'; END;               SET @checked += 1;
IF OBJECT_ID(N'report.critical_issues', N'V')                     IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.critical_issues'; END;                     SET @checked += 1;
IF OBJECT_ID(N'report.memory_usage_trends', N'V')                 IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.memory_usage_trends'; END;                 SET @checked += 1;
IF OBJECT_ID(N'report.running_jobs', N'V')                        IS NULL BEGIN SET @missing += 1; PRINT '  MISSING: report.running_jobs'; END;                        SET @checked += 1;

PRINT '';

/*
Functions in report schema (1)
*/
PRINT 'Checking report functions...';

IF OBJECT_ID(N'report.query_store_regressions', N'IF') IS NULL
AND OBJECT_ID(N'report.query_store_regressions', N'TF') IS NULL
AND OBJECT_ID(N'report.query_store_regressions', N'FN') IS NULL
BEGIN SET @missing += 1; PRINT '  MISSING: report.query_store_regressions'; END;
SET @checked += 1;

PRINT '';

/*
Table count checks (minimum expected per schema)
*/
PRINT 'Checking table counts...';

DECLARE
    @collect_tables int,
    @config_tables int;

SELECT @collect_tables = COUNT_BIG(*)
FROM sys.tables AS t
WHERE OBJECT_SCHEMA_NAME(t.object_id) = N'collect';

SELECT @config_tables = COUNT_BIG(*)
FROM sys.tables AS t
WHERE OBJECT_SCHEMA_NAME(t.object_id) = N'config';

PRINT '  collect schema tables: ' + CONVERT(varchar(10), @collect_tables);
PRINT '  config schema tables: ' + CONVERT(varchar(10), @config_tables);

IF @collect_tables < 20 BEGIN SET @missing += 1; PRINT '  MISSING: expected >= 20 collect tables, found ' + CONVERT(varchar(10), @collect_tables); END; SET @checked += 1;
IF @config_tables < 5  BEGIN SET @missing += 1; PRINT '  MISSING: expected >= 5 config tables, found ' + CONVERT(varchar(10), @config_tables); END;  SET @checked += 1;

PRINT '';

/*
Summary
*/
PRINT '========================================';
PRINT 'Checked ' + CONVERT(varchar(10), @checked) + ' objects';

IF @missing > 0
BEGIN
    PRINT 'FAILED: ' + CONVERT(varchar(10), @missing) + ' object(s) missing';
    RAISERROR('CI validation failed: %d object(s) missing', 16, 1, @missing);
END;
ELSE
BEGIN
    PRINT 'PASSED: All objects present';
END;

PRINT '========================================';
GO
