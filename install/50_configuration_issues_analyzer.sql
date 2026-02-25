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
Configuration Issues Analyzer
Analyzes collected configuration data to identify potential problems
Logs findings to config.critical_issues for alerting and monitoring
Designed to be extended with additional configuration checks over time
*/

IF OBJECT_ID(N'collect.configuration_issues_analyzer', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.configuration_issues_analyzer AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.configuration_issues_analyzer
(
    @debug bit = 0 /*Print debugging information*/
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE
        @rows_collected bigint = 0,
        @start_time datetime2(7) = SYSDATETIME(),
        @error_message nvarchar(4000);

    BEGIN TRY
        IF @debug = 1
        BEGIN
            RAISERROR(N'Starting configuration issues analysis', 0, 1) WITH NOWAIT;
        END;

        /*
        =============================================================================
        QUERY STORE CONFIGURATION CHECKS
        =============================================================================
        */

        /*
        Check for databases with Query Store disabled
        Query Store provides valuable query performance history
        This is an INFO level issue as Query Store is optional but recommended
        */
        IF @debug = 1
        BEGIN
            RAISERROR(N'Checking for databases with Query Store disabled', 0, 1) WITH NOWAIT;
        END;

        INSERT INTO
            config.critical_issues
        (
            severity,
            problem_area,
            source_collector,
            affected_database,
            message,
            investigate_query
        )
        SELECT
            severity = N'INFO',
            problem_area = N'Query Store Configuration',
            source_collector = N'configuration_issues_analyzer',
            affected_database = dch.database_name,
            message =
                N'Query Store is disabled on database [' + dch.database_name + N']. ' +
                N'Consider enabling Query Store for query performance monitoring and troubleshooting.',
            investigate_query =
                N'-- Enable Query Store on database' + CHAR(13) + CHAR(10) +
                N'ALTER DATABASE ' + QUOTENAME(dch.database_name) + N' SET QUERY_STORE = ON;' + CHAR(13) + CHAR(10) +
                N'-- Configure Query Store settings' + CHAR(13) + CHAR(10) +
                N'ALTER DATABASE ' + QUOTENAME(dch.database_name) + N' SET QUERY_STORE (' + CHAR(13) + CHAR(10) +
                N'    OPERATION_MODE = READ_WRITE,' + CHAR(13) + CHAR(10) +
                N'    MAX_STORAGE_SIZE_MB = 1024,' + CHAR(13) + CHAR(10) +
                N'    INTERVAL_LENGTH_MINUTES = 60,' + CHAR(13) + CHAR(10) +
                N'    QUERY_CAPTURE_MODE = AUTO' + CHAR(13) + CHAR(10) +
                N');'
        FROM config.database_configuration_history AS dch
        WHERE dch.setting_type = N'DATABASE_PROPERTY'
        AND   dch.setting_name = N'is_query_store_on'
        AND   CONVERT(bit, dch.setting_value) = 0
        AND   dch.database_name NOT LIKE N'%[_]master' /*exclude contained AG system databases*/
        AND   dch.database_name NOT LIKE N'%[_]msdb' /*exclude contained AG system databases*/
        AND   dch.collection_time =
              (
                  SELECT
                      MAX(dch2.collection_time)
                  FROM config.database_configuration_history AS dch2
                  WHERE dch2.database_name = dch.database_name
                  AND   dch2.setting_name = N'is_query_store_on'
              )
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.affected_database = dch.database_name
                  AND   ci.problem_area = N'Query Store Configuration'
                  AND   ci.message LIKE N'Query Store is disabled%'
                  AND   ci.log_date >= DATEADD(DAY, -7, SYSDATETIME()) /*Don't re-log same issue within 7 days*/
              )
        OPTION(RECOMPILE);

        SET @rows_collected = ROWCOUNT_BIG();

        IF @debug = 1
        BEGIN
            RAISERROR(N'Logged %d Query Store disabled issues', 0, 1, @rows_collected) WITH NOWAIT;
        END;

        /*
        =============================================================================
        AUTO SHRINK CONFIGURATION CHECKS
        =============================================================================
        */

        /*
        Check for databases with auto shrink enabled
        Auto shrink causes performance issues and file fragmentation
        This is a WARNING level issue as it impacts performance
        */
        IF @debug = 1
        BEGIN
            RAISERROR(N'Checking for databases with auto shrink enabled', 0, 1) WITH NOWAIT;
        END;

        INSERT INTO
            config.critical_issues
        (
            severity,
            problem_area,
            source_collector,
            affected_database,
            message,
            investigate_query
        )
        SELECT
            severity = N'WARNING',
            problem_area = N'Database Configuration',
            source_collector = N'configuration_issues_analyzer',
            affected_database = dch.database_name,
            message =
                N'Auto shrink is enabled on database [' + dch.database_name + N']. ' +
                N'Auto shrink causes performance issues and file fragmentation. Disable auto shrink and manually shrink if needed.',
            investigate_query =
                N'-- Disable auto shrink' + CHAR(13) + CHAR(10) +
                N'ALTER DATABASE ' + QUOTENAME(dch.database_name) + N' SET AUTO_SHRINK OFF;'
        FROM config.database_configuration_history AS dch
        WHERE dch.setting_type = N'DATABASE_PROPERTY'
        AND   dch.setting_name = N'is_auto_shrink_on'
        AND   CONVERT(bit, dch.setting_value) = 1
        AND   dch.database_name NOT LIKE N'%[_]master' /*exclude contained AG system databases*/
        AND   dch.database_name NOT LIKE N'%[_]msdb' /*exclude contained AG system databases*/
        AND   dch.collection_time =
              (
                  SELECT
                      MAX(dch2.collection_time)
                  FROM config.database_configuration_history AS dch2
                  WHERE dch2.database_name = dch.database_name
                  AND   dch2.setting_name = N'is_auto_shrink_on'
              )
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.affected_database = dch.database_name
                  AND   ci.problem_area = N'Database Configuration'
                  AND   ci.message LIKE N'Auto shrink is enabled%'
                  AND   ci.log_date >= DATEADD(DAY, -1, SYSDATETIME()) /*Don't re-log same issue within 1 day*/
              )
        OPTION(RECOMPILE);

        DECLARE @auto_shrink_issues integer = ROWCOUNT_BIG();
        SET @rows_collected = @rows_collected + @auto_shrink_issues;

        IF @debug = 1
        BEGIN
            RAISERROR(N'Logged %d auto shrink enabled issues', 0, 1, @auto_shrink_issues) WITH NOWAIT;
        END;

        /*
        =============================================================================
        AUTO CLOSE CONFIGURATION CHECKS
        =============================================================================
        */

        /*
        Check for databases with auto close enabled
        Auto close causes connection delays and unnecessary overhead
        This is a WARNING level issue as it impacts performance
        */
        IF @debug = 1
        BEGIN
            RAISERROR(N'Checking for databases with auto close enabled', 0, 1) WITH NOWAIT;
        END;

        INSERT INTO
            config.critical_issues
        (
            severity,
            problem_area,
            source_collector,
            affected_database,
            message,
            investigate_query
        )
        SELECT
            severity = N'WARNING',
            problem_area = N'Database Configuration',
            source_collector = N'configuration_issues_analyzer',
            affected_database = dch.database_name,
            message =
                N'Auto close is enabled on database [' + dch.database_name + N']. ' +
                N'Auto close causes connection delays and unnecessary overhead. Disable auto close for better performance.',
            investigate_query =
                N'-- Disable auto close' + CHAR(13) + CHAR(10) +
                N'ALTER DATABASE ' + QUOTENAME(dch.database_name) + N' SET AUTO_CLOSE OFF;'
        FROM config.database_configuration_history AS dch
        WHERE dch.setting_type = N'DATABASE_PROPERTY'
        AND   dch.setting_name = N'is_auto_close_on'
        AND   CONVERT(bit, dch.setting_value) = 1
        AND   dch.database_name NOT LIKE N'%[_]master' /*exclude contained AG system databases*/
        AND   dch.database_name NOT LIKE N'%[_]msdb' /*exclude contained AG system databases*/
        AND   dch.collection_time =
              (
                  SELECT
                      MAX(dch2.collection_time)
                  FROM config.database_configuration_history AS dch2
                  WHERE dch2.database_name = dch.database_name
                  AND   dch2.setting_name = N'is_auto_close_on'
              )
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.affected_database = dch.database_name
                  AND   ci.problem_area = N'Database Configuration'
                  AND   ci.message LIKE N'Auto close is enabled%'
                  AND   ci.log_date >= DATEADD(DAY, -1, SYSDATETIME()) /*Don't re-log same issue within 1 day*/
              )
        OPTION(RECOMPILE);

        DECLARE @auto_close_issues integer = ROWCOUNT_BIG();
        SET @rows_collected = @rows_collected + @auto_close_issues;

        IF @debug = 1
        BEGIN
            RAISERROR(N'Logged %d auto close enabled issues', 0, 1, @auto_close_issues) WITH NOWAIT;
        END;

        /*
        =============================================================================
        PHASE 0: PRE-CALCULATED WARNING FLAGS
        =============================================================================
        Collectors already set these flags - just query and log!
        */

        /*
        Memory Pressure Warnings from collect.memory_stats
        Buffer pool pressure = sudden drop in buffer pool size (>10% drop in 5 minutes)
        Plan cache pressure = sudden drop in plan cache size (>10% drop in 5 minutes)
        */
        IF @debug = 1
        BEGIN
            RAISERROR(N'Checking for memory pressure warnings', 0, 1) WITH NOWAIT;
        END;

        INSERT INTO
            config.critical_issues
        (
            severity,
            problem_area,
            source_collector,
            affected_database,
            message,
            investigate_query
        )
        SELECT
            severity = N'WARNING',
            problem_area = N'Memory Pressure',
            source_collector = N'configuration_issues_analyzer',
            affected_database = NULL,
            message =
                N'Buffer pool pressure detected - sudden drop in buffer pool size at ' +
                CONVERT(nvarchar(30), ms.collection_time, 121) +
                N'. Current buffer pool: ' +
                CONVERT(nvarchar(20), ms.buffer_pool_mb) + N' MB.',
            investigate_query = N'SELECT * FROM collect.memory_stats WHERE collection_time >= DATEADD(HOUR, -1, SYSDATETIME()) ORDER BY collection_time DESC;'
        FROM collect.memory_stats AS ms
        WHERE ms.buffer_pool_pressure_warning = 1
        AND   ms.collection_time >= DATEADD(MINUTE, -5, SYSDATETIME())
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.problem_area = N'Memory Pressure'
                  AND   ci.message LIKE N'Buffer pool pressure%'
                  AND   ci.log_date >= DATEADD(DAY, -1, SYSDATETIME())
              )

        UNION ALL

        SELECT
            severity = N'WARNING',
            problem_area = N'Memory Pressure',
            source_collector = N'configuration_issues_analyzer',
            affected_database = NULL,
            message =
                N'Plan cache pressure detected - sudden drop in plan cache size at ' +
                CONVERT(nvarchar(30), ms.collection_time, 121) +
                N'. Current plan cache: ' +
                CONVERT(nvarchar(20), ms.plan_cache_mb) + N' MB.',
            investigate_query = N'SELECT * FROM collect.memory_stats WHERE collection_time >= DATEADD(HOUR, -1, SYSDATETIME()) ORDER BY collection_time DESC;'
        FROM collect.memory_stats AS ms
        WHERE ms.plan_cache_pressure_warning = 1
        AND   ms.collection_time >= DATEADD(MINUTE, -5, SYSDATETIME())
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.problem_area = N'Memory Pressure'
                  AND   ci.message LIKE N'Plan cache pressure%'
                  AND   ci.log_date >= DATEADD(DAY, -1, SYSDATETIME())
              )
        OPTION(RECOMPILE);

        DECLARE @memory_pressure_issues integer = ROWCOUNT_BIG();
        SET @rows_collected = @rows_collected + @memory_pressure_issues;

        IF @debug = 1
        BEGIN
            RAISERROR(N'Logged %d memory pressure warning issues', 0, 1, @memory_pressure_issues) WITH NOWAIT;
        END;

        /*
        Memory Grant Warnings from collect.memory_grant_stats
        Available memory pressure = low memory available for grants (<100MB)
        Waiter count warning = high number of queries waiting for memory grants (>10)
        Timeout error warning = queries timing out waiting for memory (>0 timeouts)
        Forced grant warning = queries forced to run with insufficient memory (>0 forced)
        */
        IF @debug = 1
        BEGIN
            RAISERROR(N'Checking for memory grant warnings', 0, 1) WITH NOWAIT;
        END;

        INSERT INTO
            config.critical_issues
        (
            severity,
            problem_area,
            source_collector,
            affected_database,
            message,
            investigate_query
        )
        /*Available memory pressure warning — low available memory for grants (<100MB)*/
        SELECT
            severity = N'WARNING',
            problem_area = N'Memory Grant Pressure',
            source_collector = N'configuration_issues_analyzer',
            affected_database = NULL,
            message =
                N'Low available memory for grants detected at ' +
                CONVERT(nvarchar(30), mgs.collection_time, 121) +
                N'. Available: ' + CONVERT(nvarchar(20), mgs.available_memory_mb) + N' MB.',
            investigate_query = N'SELECT * FROM collect.memory_grant_stats WHERE collection_time >= DATEADD(HOUR, -1, SYSDATETIME()) ORDER BY collection_time DESC;'
        FROM collect.memory_grant_stats AS mgs
        WHERE mgs.available_memory_mb < 100
        AND   mgs.granted_memory_mb > 0
        AND   mgs.collection_time >= DATEADD(MINUTE, -5, SYSDATETIME())
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.problem_area = N'Memory Grant Pressure'
                  AND   ci.message LIKE N'Low available memory%'
                  AND   ci.log_date >= DATEADD(DAY, -1, SYSDATETIME())
              )

        UNION ALL

        /*Waiter count warning — high number of queries waiting for memory grants (>10)*/
        SELECT
            severity = N'WARNING',
            problem_area = N'Memory Grant Pressure',
            source_collector = N'configuration_issues_analyzer',
            affected_database = NULL,
            message =
                N'High number of queries waiting for memory grants at ' +
                CONVERT(nvarchar(30), mgs.collection_time, 121) +
                N'. Waiters: ' + CONVERT(nvarchar(10), mgs.waiter_count) + N'.',
            investigate_query = N'SELECT * FROM collect.memory_grant_stats WHERE collection_time >= DATEADD(HOUR, -1, SYSDATETIME()) ORDER BY collection_time DESC;'
        FROM collect.memory_grant_stats AS mgs
        WHERE mgs.waiter_count > 10
        AND   mgs.collection_time >= DATEADD(MINUTE, -5, SYSDATETIME())
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.problem_area = N'Memory Grant Pressure'
                  AND   ci.message LIKE N'High number of queries waiting%'
                  AND   ci.log_date >= DATEADD(DAY, -1, SYSDATETIME())
              )

        UNION ALL

        /*Timeout error warning — queries timing out waiting for memory (delta > 0), CRITICAL severity*/
        SELECT
            severity = N'CRITICAL',
            problem_area = N'Memory Grant Pressure',
            source_collector = N'configuration_issues_analyzer',
            affected_database = NULL,
            message =
                N'CRITICAL: Queries timing out waiting for memory grants at ' +
                CONVERT(nvarchar(30), mgs.collection_time, 121) +
                N'. Timeout errors: ' + CONVERT(nvarchar(10), ISNULL(mgs.timeout_error_count_delta, 0)) + N'.',
            investigate_query = N'SELECT * FROM collect.memory_grant_stats WHERE collection_time >= DATEADD(HOUR, -1, SYSDATETIME()) ORDER BY collection_time DESC;'
        FROM collect.memory_grant_stats AS mgs
        WHERE ISNULL(mgs.timeout_error_count_delta, 0) > 0
        AND   mgs.collection_time >= DATEADD(MINUTE, -5, SYSDATETIME())
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.problem_area = N'Memory Grant Pressure'
                  AND   ci.message LIKE N'CRITICAL: Queries timing out%'
                  AND   ci.log_date >= DATEADD(DAY, -1, SYSDATETIME())
              )

        UNION ALL

        /*Forced grant warning — queries forced to run with insufficient memory (delta > 0)*/
        SELECT
            severity = N'WARNING',
            problem_area = N'Memory Grant Pressure',
            source_collector = N'configuration_issues_analyzer',
            affected_database = NULL,
            message =
                N'Queries forced to run with insufficient memory at ' +
                CONVERT(nvarchar(30), mgs.collection_time, 121) +
                N'. Forced grants: ' + CONVERT(nvarchar(10), ISNULL(mgs.forced_grant_count_delta, 0)) +
                N'. These queries will spill to tempdb.',
            investigate_query = N'SELECT * FROM collect.memory_grant_stats WHERE collection_time >= DATEADD(HOUR, -1, SYSDATETIME()) ORDER BY collection_time DESC;'
        FROM collect.memory_grant_stats AS mgs
        WHERE ISNULL(mgs.forced_grant_count_delta, 0) > 0
        AND   mgs.collection_time >= DATEADD(MINUTE, -5, SYSDATETIME())
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.problem_area = N'Memory Grant Pressure'
                  AND   ci.message LIKE N'Queries forced to run%'
                  AND   ci.log_date >= DATEADD(DAY, -1, SYSDATETIME())
              )
        OPTION(RECOMPILE);

        DECLARE @memory_grant_issues integer = ROWCOUNT_BIG();
        SET @rows_collected = @rows_collected + @memory_grant_issues;

        IF @debug = 1
        BEGIN
            RAISERROR(N'Logged %d memory grant pressure issues', 0, 1, @memory_grant_issues) WITH NOWAIT;
        END;

        /*
        CPU Scheduler Warnings from collect.cpu_scheduler_stats
        Worker thread exhaustion = current workers >= 90% of max workers
        Runnable tasks warning = high number of tasks waiting for CPU (>10)
        Blocked tasks warning = high number of blocked tasks (>20)
        Queued requests warning = requests waiting in queue (>0)
        */
        IF @debug = 1
        BEGIN
            RAISERROR(N'Checking for CPU scheduler warnings', 0, 1) WITH NOWAIT;
        END;

        INSERT INTO
            config.critical_issues
        (
            severity,
            problem_area,
            source_collector,
            affected_database,
            message,
            investigate_query
        )
        /*Worker thread exhaustion warning - CRITICAL severity*/
        SELECT
            severity = N'CRITICAL',
            problem_area = N'CPU Scheduling Pressure',
            source_collector = N'configuration_issues_analyzer',
            affected_database = NULL,
            message =
                N'CRITICAL: Worker thread exhaustion detected at ' +
                CONVERT(nvarchar(30), css.collection_time, 121) +
                N'. Current workers: ' + CONVERT(nvarchar(10), css.total_current_workers_count) +
                N' / Max: ' + CONVERT(nvarchar(10), css.max_workers_count) + N'.',
            investigate_query = N'SELECT * FROM collect.cpu_scheduler_stats WHERE collection_time >= DATEADD(HOUR, -1, SYSDATETIME()) ORDER BY collection_time DESC;'
        FROM collect.cpu_scheduler_stats AS css
        WHERE css.worker_thread_exhaustion_warning = 1
        AND   css.collection_time >= DATEADD(MINUTE, -5, SYSDATETIME())
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.problem_area = N'CPU Scheduling Pressure'
                  AND   ci.message LIKE N'CRITICAL: Worker thread exhaustion%'
                  AND   ci.log_date >= DATEADD(DAY, -1, SYSDATETIME())
              )

        UNION ALL

        /*Runnable tasks warning*/
        SELECT
            severity = N'WARNING',
            problem_area = N'CPU Scheduling Pressure',
            source_collector = N'configuration_issues_analyzer',
            affected_database = NULL,
            message =
                N'High number of tasks waiting for CPU at ' +
                CONVERT(nvarchar(30), css.collection_time, 121) +
                N'. Runnable tasks: ' + CONVERT(nvarchar(10), css.total_runnable_tasks_count) + N'.',
            investigate_query = N'SELECT * FROM collect.cpu_scheduler_stats WHERE collection_time >= DATEADD(HOUR, -1, SYSDATETIME()) ORDER BY collection_time DESC;'
        FROM collect.cpu_scheduler_stats AS css
        WHERE css.runnable_tasks_warning = 1
        AND   css.collection_time >= DATEADD(MINUTE, -5, SYSDATETIME())
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.problem_area = N'CPU Scheduling Pressure'
                  AND   ci.message LIKE N'High number of tasks waiting for CPU%'
                  AND   ci.log_date >= DATEADD(DAY, -1, SYSDATETIME())
              )

        UNION ALL

        /*Blocked tasks warning*/
        SELECT
            severity = N'WARNING',
            problem_area = N'CPU Scheduling Pressure',
            source_collector = N'configuration_issues_analyzer',
            affected_database = NULL,
            message =
                N'High number of blocked tasks at ' +
                CONVERT(nvarchar(30), css.collection_time, 121) +
                N'. Blocked tasks: ' + CONVERT(nvarchar(10), css.total_blocked_task_count) + N'.',
            investigate_query = N'SELECT * FROM collect.cpu_scheduler_stats WHERE collection_time >= DATEADD(HOUR, -1, SYSDATETIME()) ORDER BY collection_time DESC;'
        FROM collect.cpu_scheduler_stats AS css
        WHERE css.blocked_tasks_warning = 1
        AND   css.collection_time >= DATEADD(MINUTE, -5, SYSDATETIME())
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.problem_area = N'CPU Scheduling Pressure'
                  AND   ci.message LIKE N'High number of blocked tasks%'
                  AND   ci.log_date >= DATEADD(DAY, -1, SYSDATETIME())
              )

        UNION ALL

        /*Queued requests warning*/
        SELECT
            severity = N'WARNING',
            problem_area = N'CPU Scheduling Pressure',
            source_collector = N'configuration_issues_analyzer',
            affected_database = NULL,
            message =
                N'Requests queuing for execution at ' +
                CONVERT(nvarchar(30), css.collection_time, 121) +
                N'. Queued: ' + CONVERT(nvarchar(10), css.total_queued_request_count) + N'.',
            investigate_query = N'SELECT * FROM collect.cpu_scheduler_stats WHERE collection_time >= DATEADD(HOUR, -1, SYSDATETIME()) ORDER BY collection_time DESC;'
        FROM collect.cpu_scheduler_stats AS css
        WHERE css.queued_requests_warning = 1
        AND   css.collection_time >= DATEADD(MINUTE, -5, SYSDATETIME())
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.problem_area = N'CPU Scheduling Pressure'
                  AND   ci.message LIKE N'Requests queuing for execution%'
                  AND   ci.log_date >= DATEADD(DAY, -1, SYSDATETIME())
              )
        OPTION(RECOMPILE);

        DECLARE @cpu_scheduler_issues integer = ROWCOUNT_BIG();
        SET @rows_collected = @rows_collected + @cpu_scheduler_issues;

        IF @debug = 1
        BEGIN
            RAISERROR(N'Logged %d CPU scheduler pressure issues', 0, 1, @cpu_scheduler_issues) WITH NOWAIT;
        END;

        /*
        =============================================================================
        MEMORY DUMP DETECTION
        =============================================================================
        Check for recent SQL Server memory dumps from sys.dm_server_memory_dumps
        Memory dumps indicate crashes, assertion failures, or other critical issues
        */
        IF @debug = 1
        BEGIN
            RAISERROR(N'Checking for recent memory dumps', 0, 1) WITH NOWAIT;
        END;

        INSERT INTO
            config.critical_issues
        (
            severity,
            problem_area,
            source_collector,
            affected_database,
            message,
            investigate_query,
            threshold_value
        )
        SELECT
            severity = N'CRITICAL',
            problem_area = N'SQL Server Stability',
            source_collector = N'configuration_issues_analyzer',
            affected_database = NULL,
            message =
                N'Memory dump detected: ' + md.filename +
                N' created at ' + CONVERT(nvarchar(30), md.creation_time, 121) + N'. ' +
                N'Indicates crash, assertion failure, or critical error. ' +
                N'Review dump file and SQL Server error log for details.',
            investigate_query =
                N'-- Check recent dumps' + CHAR(13) + CHAR(10) +
                N'SELECT * FROM sys.dm_server_memory_dumps ORDER BY creation_time DESC;' + CHAR(13) + CHAR(10) +
                N'-- Check error log for related errors' + CHAR(13) + CHAR(10) +
                N'EXECUTE sp_readerrorlog 0, 1, N''dump'';',
            threshold_value = CONVERT(decimal(38,2), md.size_in_bytes) / 1024 / 1024
        FROM sys.dm_server_memory_dumps AS md
        WHERE md.creation_time >= DATEADD(DAY, -7, SYSDATETIME())
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.problem_area = N'SQL Server Stability'
                  AND   ci.message LIKE N'%' + md.filename + N'%'
                  AND   ci.log_date >= DATEADD(DAY, -7, SYSDATETIME()) /*Don't re-log same dump file*/
              )
        OPTION(RECOMPILE);

        DECLARE @memory_dump_issues integer = ROWCOUNT_BIG();
        SET @rows_collected = @rows_collected + @memory_dump_issues;

        IF @debug = 1
        BEGIN
            RAISERROR(N'Logged %d memory dump issues', 0, 1, @memory_dump_issues) WITH NOWAIT;
        END;

        /*
        =============================================================================
        SERVER CONFIGURATION CHECKS
        =============================================================================
        Check server-level sp_configure settings
        */

        /*
        Priority Boost and Lightweight Pooling (High Priority Warnings)
        Priority boost = interferes with Windows scheduling, can cause instability
        Lightweight pooling = fiber mode causes issues with OLEDB and other components
        */
        IF @debug = 1
        BEGIN
            RAISERROR(N'Checking for priority boost and lightweight pooling', 0, 1) WITH NOWAIT;
        END;

        INSERT INTO
            config.critical_issues
        (
            severity,
            problem_area,
            source_collector,
            affected_database,
            message,
            investigate_query
        )
        /*Priority boost warning*/
        SELECT
            severity = N'WARNING',
            problem_area = N'Server Configuration',
            source_collector = N'configuration_issues_analyzer',
            affected_database = NULL,
            message =
                N'Priority boost is enabled. This can cause Windows scheduling priority issues and is not recommended.',
            investigate_query =
                N'EXECUTE sp_configure ''priority boost'', 0; RECONFIGURE;'
        FROM config.server_configuration_history AS sch
        WHERE sch.configuration_name = N'priority boost'
        AND   CONVERT(integer, sch.value_in_use) = 1
        AND   sch.collection_time =
              (
                  SELECT
                      MAX(sch2.collection_time)
                  FROM config.server_configuration_history AS sch2
                  WHERE sch2.configuration_name = N'priority boost'
              )
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.problem_area = N'Server Configuration'
                  AND   ci.message LIKE N'Priority boost is enabled%'
                  AND   ci.log_date >= DATEADD(DAY, -3, SYSDATETIME())
              )

        UNION ALL

        /*Lightweight pooling warning*/
        SELECT
            severity = N'WARNING',
            problem_area = N'Server Configuration',
            source_collector = N'configuration_issues_analyzer',
            affected_database = NULL,
            message =
                N'Lightweight pooling (fiber mode) is enabled. This causes issues with OLEDB and other components.',
            investigate_query =
                N'EXECUTE sp_configure ''lightweight pooling'', 0; RECONFIGURE;'
        FROM config.server_configuration_history AS sch
        WHERE sch.configuration_name = N'lightweight pooling'
        AND   CONVERT(integer, sch.value_in_use) = 1
        AND   sch.collection_time =
              (
                  SELECT
                      MAX(sch2.collection_time)
                  FROM config.server_configuration_history AS sch2
                  WHERE sch2.configuration_name = N'lightweight pooling'
              )
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.problem_area = N'Server Configuration'
                  AND   ci.message LIKE N'Lightweight pooling%'
                  AND   ci.log_date >= DATEADD(DAY, -3, SYSDATETIME())
              )
        OPTION(RECOMPILE);

        DECLARE @server_config_warning_issues integer = ROWCOUNT_BIG();
        SET @rows_collected = @rows_collected + @server_config_warning_issues;

        IF @debug = 1
        BEGIN
            RAISERROR(N'Logged %d server configuration warning issues', 0, 1, @server_config_warning_issues) WITH NOWAIT;
        END;

        /*
        MAXDOP and Cost Threshold for Parallelism (INFO)
        MAXDOP = 0 can lead to excessive parallelism
        Cost threshold <= 5 can cause small queries to go parallel unnecessarily
        */
        IF @debug = 1
        BEGIN
            RAISERROR(N'Checking for MAXDOP and cost threshold issues', 0, 1) WITH NOWAIT;
        END;

        INSERT INTO
            config.critical_issues
        (
            severity,
            problem_area,
            source_collector,
            affected_database,
            message,
            investigate_query
        )
        /*MAXDOP = 0 warning*/
        SELECT
            severity = N'INFO',
            problem_area = N'Server Configuration',
            source_collector = N'configuration_issues_analyzer',
            affected_database = NULL,
            message =
                N'MAXDOP is set to 0 (unlimited) which can lead to excessive parallelism. Consider setting to 8 or the number of CPUs, whichever is lower.',
            investigate_query =
                N'EXECUTE sp_configure ''max degree of parallelism'', 8; RECONFIGURE;'
        FROM config.server_configuration_history AS sch
        WHERE sch.configuration_name = N'max degree of parallelism'
        AND   CONVERT(integer, sch.value_in_use) = 0
        AND   sch.collection_time =
              (
                  SELECT
                      MAX(sch2.collection_time)
                  FROM config.server_configuration_history AS sch2
                  WHERE sch2.configuration_name = N'max degree of parallelism'
              )
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.problem_area = N'Server Configuration'
                  AND   ci.message LIKE N'MAXDOP is set to 0%'
                  AND   ci.log_date >= DATEADD(DAY, -7, SYSDATETIME())
              )

        UNION ALL

        /*Cost threshold for parallelism warning*/
        SELECT
            severity = N'INFO',
            problem_area = N'Server Configuration',
            source_collector = N'configuration_issues_analyzer',
            affected_database = NULL,
            message =
                N'Cost threshold for parallelism is set to ' + CONVERT(nvarchar(20), sch.value_in_use) +
                N'. Values <= 5 can cause small queries to go parallel unnecessarily. Consider setting to 50 or higher.',
            investigate_query =
                N'EXECUTE sp_configure ''cost threshold for parallelism'', 50; RECONFIGURE;'
        FROM config.server_configuration_history AS sch
        WHERE sch.configuration_name = N'cost threshold for parallelism'
        AND   CONVERT(integer, sch.value_in_use) <= 5
        AND   sch.collection_time =
              (
                  SELECT
                      MAX(sch2.collection_time)
                  FROM config.server_configuration_history AS sch2
                  WHERE sch2.configuration_name = N'cost threshold for parallelism'
              )
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.problem_area = N'Server Configuration'
                  AND   ci.message LIKE N'Cost threshold for parallelism is set to%'
                  AND   ci.log_date >= DATEADD(DAY, -7, SYSDATETIME())
              )
        OPTION(RECOMPILE);

        DECLARE @server_config_info_issues integer = ROWCOUNT_BIG();
        SET @rows_collected = @rows_collected + @server_config_info_issues;

        IF @debug = 1
        BEGIN
            RAISERROR(N'Logged %d server configuration info issues', 0, 1, @server_config_info_issues) WITH NOWAIT;
        END;

        /*
        =============================================================================
        MEMORY CLERK CHECKS
        Check for security cache growth (TokenAndPermUserStore)
        =============================================================================
        */

        /*
        TokenAndPermUserStore Excessive Growth
        USERSTORE_TOKENPERM (TokenAndPermUserStore) growing out of control
        Can cause performance issues, memory pressure, and odd SQL Server behavior
        Reference: https://www.erikdarling.com/troubleshooting-security-cache-issues-userstore_tokenperm-and-tokenandpermuserstore/
        */
        IF @debug = 1
        BEGIN
            RAISERROR(N'Checking for TokenAndPermUserStore excessive growth', 0, 1) WITH NOWAIT;
        END;

        INSERT INTO
            config.critical_issues
        (
            severity,
            problem_area,
            source_collector,
            affected_database,
            message,
            investigate_query
        )
        SELECT
            severity = N'WARNING',
            problem_area = N'Memory Clerk Growth',
            source_collector = N'configuration_issues_analyzer',
            affected_database = NULL,
            message =
                N'TokenAndPermUserStore memory clerk has grown to ' +
                CONVERT
                (
                    nvarchar(20),
                    CONVERT
                    (
                        decimal(10,2),
                        (mcs.pages_kb / 1024. / 1024.)
                    )
                ) +
                N' GB. This security cache can cause performance issues and memory pressure when it grows excessively. ' +
                N'Consider running DBCC FREESYSTEMCACHE(''TokenAndPermUserStore'') during a maintenance window.',
            investigate_query =
                N'SELECT type, name, pages_kb, (pages_kb / 1024. / 1024.) AS size_gb FROM sys.dm_os_memory_clerks WHERE type = N''USERSTORE_TOKENPERM'' AND name = N''TokenAndPermUserStore'';'
        FROM collect.memory_clerks_stats AS mcs
        WHERE mcs.clerk_type = N'USERSTORE_TOKENPERM'
        AND   (mcs.pages_kb / 1024. / 1024.) >= 1.0 /*1GB threshold*/
        AND   mcs.collection_time =
              (
                  SELECT
                      MAX(mcs2.collection_time)
                  FROM collect.memory_clerks_stats AS mcs2
                  WHERE mcs2.clerk_type = N'USERSTORE_TOKENPERM'
              )
        AND   NOT EXISTS
              (
                  SELECT
                      1/0
                  FROM config.critical_issues AS ci
                  WHERE ci.source_collector = N'configuration_issues_analyzer'
                  AND   ci.problem_area = N'Memory Clerk Growth'
                  AND   ci.message LIKE N'%TokenAndPermUserStore%'
                  AND   ci.log_date >= DATEADD(DAY, -1, SYSDATETIME()) /*Don't re-log same issue within 1 day*/
              )
        OPTION(RECOMPILE);

        DECLARE @token_perm_issues integer = ROWCOUNT_BIG();
        SET @rows_collected = @rows_collected + @token_perm_issues;

        IF @debug = 1
        BEGIN
            RAISERROR(N'Logged %d TokenAndPermUserStore growth issues', 0, 1, @token_perm_issues) WITH NOWAIT;
        END;

        /*
        Log successful collection
        */
        INSERT INTO
            config.collection_log
        (
            collector_name,
            collection_status,
            rows_collected,
            duration_ms
        )
        VALUES
        (
            N'configuration_issues_analyzer',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Configuration issues analysis complete: %d total issues logged', 0, 1, @rows_collected) WITH NOWAIT;
        END;

    END TRY
    BEGIN CATCH
        /*
        Log the error
        */
        SET @error_message = ERROR_MESSAGE();

        INSERT INTO
            config.collection_log
        (
            collector_name,
            collection_status,
            duration_ms,
            error_message
        )
        VALUES
        (
            N'configuration_issues_analyzer',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Error in configuration issues analyzer: %s', 16, 1, @error_message);
        END;
    END CATCH;
END;
GO

PRINT 'Configuration issues analyzer created successfully';
PRINT 'Analyzes collected configuration data and logs issues to config.critical_issues';
PRINT 'Database Checks: Query Store disabled, auto shrink enabled, auto close enabled';
PRINT 'Pre-Calculated Warnings: Memory pressure, memory grant pressure, CPU scheduler pressure';
PRINT 'Memory Dump Detection: Recent SQL Server memory dumps (CRITICAL severity)';
PRINT 'Server Config Checks: Priority boost, lightweight pooling, MAXDOP, cost threshold for parallelism';
GO
