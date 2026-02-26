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
Installation Validation for Performance Monitor
Combined from multiple validation files
*******************************************************************************/

PRINT 'Running installation validation...';
GO

/*
Post-Installation Validation Script
Validates that all collectors are working correctly and catching silent failures
Run this after installation to verify everything is functioning properly
*/

SET NOCOUNT ON;
GO

USE PerformanceMonitor;
GO

PRINT '================================================================================';
PRINT 'Performance Monitor Installation Validation';
PRINT '================================================================================';
PRINT '';

/*
Step 1: Check for errors in collection_log
*/
PRINT 'Step 1: Checking config.collection_log for ERROR status...';
PRINT '';

IF EXISTS (SELECT 1/0 FROM config.collection_log WHERE collection_status = N'ERROR')
BEGIN
    PRINT '*** ERRORS FOUND IN COLLECTION LOG ***';
    PRINT '';

    SELECT
        collection_time,
        collector_name,
        collection_status,
        error_message
    FROM config.collection_log
    WHERE collection_status = N'ERROR'
    ORDER BY
        collection_time DESC;

    PRINT '';
END;
ELSE
BEGIN
    PRINT '  � No errors found in collection log';
    PRINT '';
END;

/*
Step 2: Validate all collectors execute with @debug = 1
This will surface errors that are being swallowed by TRY/CATCH blocks
*/
PRINT '================================================================================';
PRINT 'Step 2: Testing all collectors with @debug = 1...';
PRINT '================================================================================';
PRINT '';

DECLARE
    @collector_name sysname,
    @sql nvarchar(max),
    @error_count integer = 0,
    @success_count integer = 0;

DECLARE @collector_cursor CURSOR;

SET @collector_cursor =
    CURSOR
    LOCAL
    FAST_FORWARD
FOR
SELECT
    collector_name
FROM config.collection_schedule
WHERE enabled = 1
ORDER BY
    collector_name;

OPEN @collector_cursor;

FETCH NEXT
FROM @collector_cursor
INTO @collector_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT 'Testing: ' + @collector_name;

    SET @sql = N'EXECUTE collect.' + QUOTENAME(@collector_name) + N' @debug = 1;';

    BEGIN TRY
        EXECUTE sys.sp_executesql @sql;
        SET @success_count = @success_count + 1;
        PRINT '  � Success';
    END TRY
    BEGIN CATCH
        SET @error_count = @error_count + 1;
        PRINT '  ? FAILED: ' + ERROR_MESSAGE();
    END CATCH;

    PRINT '';

    FETCH NEXT
    FROM @collector_cursor
    INTO @collector_name;
END;

PRINT '--------------------------------------------------------------------------------';
PRINT 'Collector Test Summary:';
PRINT '  Successful: ' + CONVERT(varchar(10), @success_count);
PRINT '  Failed: ' + CONVERT(varchar(10), @error_count);
PRINT '';

/*
Step 3: Check row counts in all collection tables
*/
PRINT '================================================================================';
PRINT 'Step 3: Validating data collection (row counts)...';
PRINT '================================================================================';
PRINT '';

DECLARE @table_counts TABLE
(
    schema_name sysname NOT NULL,
    table_name sysname NOT NULL,
    row_count bigint NOT NULL
);

INSERT INTO @table_counts (schema_name, table_name, row_count)
SELECT
    schema_name = OBJECT_SCHEMA_NAME(t.object_id),
    table_name = OBJECT_NAME(t.object_id),
    row_count = SUM(p.rows)
FROM sys.tables AS t
JOIN sys.partitions AS p
  ON p.object_id = t.object_id
WHERE OBJECT_SCHEMA_NAME(t.object_id) = N'collect'
AND   p.index_id IN (0, 1)
GROUP BY
    t.object_id;

SELECT
    table_name,
    row_count,
    status =
        CASE
            WHEN row_count = 0 THEN N'� WARNING: No data collected'
            WHEN row_count < 10 THEN N'� Low row count'
            ELSE N'� OK'
        END
FROM @table_counts
ORDER BY
    CASE WHEN row_count = 0 THEN 0 ELSE 1 END,
    table_name;

PRINT '';

/*
Step 4: Check for NULL values in critical required columns
*/
PRINT '================================================================================';
PRINT 'Step 4: Checking for NULL values in required columns...';
PRINT '================================================================================';
PRINT '';

DECLARE @null_checks TABLE
(
    table_name sysname NOT NULL,
    column_name sysname NOT NULL,
    null_count bigint NOT NULL
);

/*
Query stats - database_name should never be NULL
*/
IF OBJECT_ID(N'collect.query_stats', N'U') IS NOT NULL
BEGIN
    INSERT INTO @null_checks
    SELECT
        table_name = N'query_stats',
        column_name = N'database_name',
        null_count = COUNT_BIG(*)
    FROM collect.query_stats
    WHERE database_name IS NULL
    HAVING COUNT_BIG(*) > 0;
END;

/*
Procedure stats - object_name should never be NULL
*/
IF OBJECT_ID(N'collect.procedure_stats', N'U') IS NOT NULL
BEGIN
    INSERT INTO @null_checks
    SELECT
        table_name = N'procedure_stats',
        column_name = N'object_name',
        null_count = COUNT_BIG(*)
    FROM collect.procedure_stats
    WHERE object_name IS NULL
    HAVING COUNT_BIG(*) > 0;
END;

/*
Query Store - database_name should never be NULL
*/
IF OBJECT_ID(N'collect.query_store_data', N'U') IS NOT NULL
BEGIN
    INSERT INTO @null_checks
    SELECT
        table_name = N'query_store_data',
        column_name = N'database_name',
        null_count = COUNT_BIG(*)
    FROM collect.query_store_data
    WHERE database_name IS NULL
    HAVING COUNT_BIG(*) > 0;
END;

/*
Latch stats - latch_class should never be NULL
*/
IF OBJECT_ID(N'collect.latch_stats', N'U') IS NOT NULL
BEGIN
    INSERT INTO @null_checks
    SELECT
        table_name = N'latch_stats',
        column_name = N'latch_class',
        null_count = COUNT_BIG(*)
    FROM collect.latch_stats
    WHERE latch_class IS NULL
    HAVING COUNT_BIG(*) > 0;
END;

/*
Spinlock stats - spinlock_name should never be NULL
*/
IF OBJECT_ID(N'collect.spinlock_stats', N'U') IS NOT NULL
BEGIN
    INSERT INTO @null_checks
    SELECT
        table_name = N'spinlock_stats',
        column_name = N'spinlock_name',
        null_count = COUNT_BIG(*)
    FROM collect.spinlock_stats
    WHERE spinlock_name IS NULL
    HAVING COUNT_BIG(*) > 0;
END;

/*
Plan cache stats - cacheobjtype and objtype should never be NULL
*/
IF OBJECT_ID(N'collect.plan_cache_stats', N'U') IS NOT NULL
BEGIN
    INSERT INTO @null_checks
    SELECT
        table_name = N'plan_cache_stats',
        column_name = N'cacheobjtype',
        null_count = COUNT_BIG(*)
    FROM collect.plan_cache_stats
    WHERE cacheobjtype IS NULL
    HAVING COUNT_BIG(*) > 0;

    INSERT INTO @null_checks
    SELECT
        table_name = N'plan_cache_stats',
        column_name = N'objtype',
        null_count = COUNT_BIG(*)
    FROM collect.plan_cache_stats
    WHERE objtype IS NULL
    HAVING COUNT_BIG(*) > 0;
END;

IF EXISTS (SELECT 1/0 FROM @null_checks)
BEGIN
    PRINT '*** NULL VALUES FOUND IN REQUIRED COLUMNS ***';
    PRINT '';

    SELECT
        table_name,
        column_name,
        null_count
    FROM @null_checks
    ORDER BY
        null_count DESC;

    PRINT '';
END;
ELSE
BEGIN
    PRINT '  � No NULL values found in required columns';
    PRINT '';
END;

/*
Step 5: Check for recent collections (last 5 minutes)
*/
PRINT '================================================================================';
PRINT 'Step 5: Checking for recent data collection (last 5 minutes)...';
PRINT '================================================================================';
PRINT '';

DECLARE @recent_collections TABLE
(
    collector_name sysname NOT NULL,
    last_collection datetime2(7) NULL,
    rows_collected integer NULL,
    status nvarchar(20) NULL
);

INSERT INTO @recent_collections
SELECT
    collector_name,
    last_collection = MAX(collection_time),
    rows_collected = SUM(CASE WHEN collection_status = N'SUCCESS' THEN rows_collected ELSE 0 END),
    status = MAX(collection_status)
FROM config.collection_log
WHERE collection_time >= DATEADD(MINUTE, -5, SYSDATETIME())
GROUP BY
    collector_name;

SELECT
    collector_name,
    last_collection,
    rows_collected,
    status,
    result =
        CASE
            WHEN status = N'ERROR' THEN N'? FAILED'
            WHEN rows_collected = 0 THEN N'� No data collected'
            ELSE N'� OK'
        END
FROM @recent_collections
ORDER BY
    CASE WHEN status = N'ERROR' THEN 0 ELSE 1 END,
    collector_name;

PRINT '';

/*
Final Summary
*/
PRINT '================================================================================';
PRINT 'Validation Summary';
PRINT '================================================================================';
PRINT '';

DECLARE
    @total_errors integer,
    @total_warnings integer,
    @total_tables_empty integer;

SELECT @total_errors = COUNT_BIG(*) FROM config.collection_log WHERE collection_status = N'ERROR';
SELECT @total_warnings = COUNT_BIG(*) FROM @null_checks;
SELECT @total_tables_empty = COUNT_BIG(*) FROM @table_counts WHERE row_count = 0;

IF @total_errors > 0
BEGIN
    PRINT '? VALIDATION FAILED';
    PRINT '  Errors in collection_log: ' + CONVERT(varchar(10), @total_errors);
END;

IF @total_warnings > 0
BEGIN
    PRINT '� WARNINGS FOUND';
    PRINT '  Tables with NULL in required columns: ' + CONVERT(varchar(10), @total_warnings);
END;

IF @total_tables_empty > 0
BEGIN
    PRINT '� WARNINGS FOUND';
    PRINT '  Empty collection tables: ' + CONVERT(varchar(10), @total_tables_empty);
END;

IF @total_errors = 0 AND @total_warnings = 0 AND @total_tables_empty = 0
BEGIN
    PRINT '� VALIDATION PASSED';
    PRINT '  All collectors are functioning correctly';
END;

PRINT '';
PRINT 'Validation complete';
GO

GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

USE PerformanceMonitor;
GO

/*******************************************************************************
Quick Validation: Hung Job Monitor Installation
Purpose: Fast smoke test to verify hung monitor is installed correctly
Run this immediately after installation to confirm setup
*******************************************************************************/

PRINT '';
PRINT '============================================================================';
PRINT 'Hung Job Monitor - Quick Validation';
PRINT '============================================================================';
PRINT '';

DECLARE
    @test_passed bit = 1,
    @test_count integer = 0,
    @passed_count integer = 0;

/*******************************************************************************
CHECK 1: LOCK_TIMEOUT in master collector
*******************************************************************************/
SET @test_count = @test_count + 1;

PRINT 'CHECK 1: Verifying LOCK_TIMEOUT in scheduled_master_collector';

DECLARE
    @procedure_text nvarchar(max),
    @has_lock_timeout bit = 0;

SELECT
    @procedure_text = OBJECT_DEFINITION(OBJECT_ID(N'collect.scheduled_master_collector'));

IF @procedure_text LIKE N'%SET LOCK_TIMEOUT%30000%'
BEGIN
    SET @has_lock_timeout = 1;
    SET @passed_count = @passed_count + 1;
    PRINT '  ✓ PASS: LOCK_TIMEOUT 30000 found in procedure';
END;
ELSE
BEGIN
    SET @test_passed = 0;
    PRINT '  ❌ FAIL: LOCK_TIMEOUT not found in scheduled_master_collector';
    PRINT '         Expected: SET LOCK_TIMEOUT 30000;';
END;

PRINT '';

/*******************************************************************************
CHECK 2: Hung monitor procedure exists
*******************************************************************************/
SET @test_count = @test_count + 1;

PRINT 'CHECK 2: Verifying config.check_hung_collector_job exists';

IF OBJECT_ID(N'config.check_hung_collector_job', N'P') IS NOT NULL
BEGIN
    SET @passed_count = @passed_count + 1;
    PRINT '  ✓ PASS: Procedure config.check_hung_collector_job exists';

    /*
    Check parameters
    */
    IF EXISTS
    (
        SELECT
            1/0
        FROM sys.parameters AS p
        WHERE p.object_id = OBJECT_ID(N'config.check_hung_collector_job')
        AND   p.name IN
        (
            N'@job_name',
            N'@normal_max_duration_minutes',
            N'@first_run_max_duration_minutes',
            N'@stop_hung_job',
            N'@debug'
        )
        HAVING COUNT_BIG(*) = 5
    )
    BEGIN
        PRINT '         All required parameters present';
    END;
    ELSE
    BEGIN
        PRINT '         ⚠ Warning: Missing expected parameters';
    END;
END;
ELSE
BEGIN
    SET @test_passed = 0;
    PRINT '  ❌ FAIL: Procedure config.check_hung_collector_job does not exist';
    PRINT '         Run install/34a_hung_job_monitor.sql';
END;

PRINT '';

/*******************************************************************************
CHECK 3: Hung monitor job exists
*******************************************************************************/
SET @test_count = @test_count + 1;

PRINT 'CHECK 3: Verifying PerformanceMonitor - Hung Job Monitor job';

DECLARE
    @monitor_job_id uniqueidentifier;

SELECT
    @monitor_job_id = j.job_id
FROM msdb.dbo.sysjobs AS j
WHERE j.name = N'PerformanceMonitor - Hung Job Monitor';

IF @monitor_job_id IS NOT NULL
BEGIN
    SET @passed_count = @passed_count + 1;
    PRINT '  ✓ PASS: SQL Agent job exists';

    /*
    Check job is enabled and schedule details
    Wrapped in TRY/CATCH for environments with limited msdb permissions (e.g., AWS RDS)
    */
    BEGIN TRY
        DECLARE
            @job_enabled bit,
            @schedule_enabled bit,
            @freq_interval integer;

        SELECT
            @job_enabled = j.enabled
        FROM msdb.dbo.sysjobs AS j
        WHERE j.job_id = @monitor_job_id;

        SELECT
            @schedule_enabled = s.enabled,
            @freq_interval = s.freq_subday_interval
        FROM msdb.dbo.sysjobschedules AS js
        JOIN msdb.dbo.sysschedules AS s
          ON s.schedule_id = js.schedule_id
        WHERE js.job_id = @monitor_job_id;

        IF @job_enabled = 1
        BEGIN
            PRINT '         Job is enabled';
        END;
        ELSE
        BEGIN
            PRINT '         ⚠ Warning: Job is disabled';
        END;

        IF @schedule_enabled = 1
        BEGIN
            PRINT '         Schedule is enabled';
        END;
        ELSE
        BEGIN
            PRINT '         ⚠ Warning: Schedule is disabled';
        END;

        IF @freq_interval = 5
        BEGIN
            PRINT '         Runs every 5 minutes (correct)';
        END;
        ELSE
        BEGIN
            PRINT '         ⚠ Warning: Interval is ' + CONVERT(nvarchar(10), ISNULL(@freq_interval, 0)) + ' minutes (expected: 5)';
        END;
    END TRY
    BEGIN CATCH
        PRINT '         Note: Schedule details unavailable (insufficient permissions on msdb schedule tables).';
    END CATCH;
END;
ELSE
BEGIN
    SET @test_passed = 0;
    PRINT '  ❌ FAIL: SQL Agent job does not exist';
    PRINT '         Run install/35_create_agent_jobs.sql';
END;

PRINT '';

/*******************************************************************************
CHECK 4: Collection job exists (to monitor)
*******************************************************************************/
SET @test_count = @test_count + 1;

PRINT 'CHECK 4: Verifying PerformanceMonitor - Collection job exists';

DECLARE
    @collection_job_id uniqueidentifier;

SELECT
    @collection_job_id = j.job_id
FROM msdb.dbo.sysjobs AS j
WHERE j.name = N'PerformanceMonitor - Collection';

IF @collection_job_id IS NOT NULL
BEGIN
    SET @passed_count = @passed_count + 1;
    PRINT '  ✓ PASS: Collection job exists';
END;
ELSE
BEGIN
    SET @test_passed = 0;
    PRINT '  ❌ FAIL: Collection job does not exist';
    PRINT '         Run install/35_create_agent_jobs.sql';
END;

PRINT '';

/*******************************************************************************
CHECK 5: SQL Server Agent is running
*******************************************************************************/
SET @test_count = @test_count + 1;

PRINT 'CHECK 5: Verifying SQL Server Agent service';

BEGIN TRY
    IF (
        SELECT
            COUNT_BIG(*)
        FROM sys.dm_server_services AS ss
        WHERE ss.servicename LIKE N'SQL Server Agent%'
        AND   ss.status_desc = N'Running'
    ) > 0
    BEGIN
        SET @passed_count = @passed_count + 1;
        PRINT '  ✓ PASS: SQL Server Agent is running';
    END;
    ELSE
    BEGIN
        SET @test_passed = 0;
        PRINT '  ❌ FAIL: SQL Server Agent is not running';
        PRINT '         Start the SQL Server Agent service';
    END;
END TRY
BEGIN CATCH
    IF ERROR_NUMBER() IN (297, 300) /*Permission denied errors*/
    BEGIN
        PRINT '  ⚠ SKIP: Cannot check SQL Server Agent (VIEW SERVER STATE permission required)';
    END;
    ELSE
    BEGIN
        THROW;
    END;
END CATCH;

PRINT '';

/*******************************************************************************
CHECK 6: Test monitor procedure execution
*******************************************************************************/
SET @test_count = @test_count + 1;

PRINT 'CHECK 6: Testing monitor procedure execution';

BEGIN TRY
    EXECUTE config.check_hung_collector_job
        @job_name = N'PerformanceMonitor - Collection',
        @stop_hung_job = 0, /*Don't actually stop job in validation*/
        @debug = 0;

    SET @passed_count = @passed_count + 1;
    PRINT '  ✓ PASS: Procedure executed without errors';
END TRY
BEGIN CATCH
    SET @test_passed = 0;
    PRINT '  ❌ FAIL: Procedure threw error';
    PRINT '         Error: ' + ERROR_MESSAGE();
END CATCH;

PRINT '';

/*******************************************************************************
CHECK 7: Verify first run detection logic
*******************************************************************************/
SET @test_count = @test_count + 1;

PRINT 'CHECK 7: Verifying first run detection logic';

DECLARE
    @is_first_run bit = 0;

IF EXISTS
(
    SELECT
        1/0
    FROM config.collection_schedule AS cs
    WHERE cs.enabled = 1
    AND   NOT EXISTS
    (
        SELECT
            1/0
        FROM config.collection_log AS cl
        WHERE cl.collector_name = cs.collector_name
        AND   cl.collection_status = N'SUCCESS'
    )
)
BEGIN
    SET @is_first_run = 1;
END;

SET @passed_count = @passed_count + 1;

IF @is_first_run = 1
BEGIN
    PRINT '  ✓ INFO: First run mode active (30-minute timeout)';
    PRINT '         This is expected for new installations';
    PRINT '         Will switch to 5-minute timeout after first successful collection';
END;
ELSE
BEGIN
    PRINT '  ✓ INFO: Normal mode active (5-minute timeout)';
    PRINT '         All collectors have succeeded at least once';
END;

PRINT '';

/*******************************************************************************
Summary
*******************************************************************************/

PRINT '============================================================================';
PRINT 'Validation Summary';
PRINT '============================================================================';
PRINT '';
PRINT '  Tests Passed: ' + CONVERT(nvarchar(10), @passed_count) + ' / ' + CONVERT(nvarchar(10), @test_count);
PRINT '';

IF @test_passed = 1
BEGIN
    PRINT '  ✓ ALL CHECKS PASSED';
    PRINT '';
    PRINT '  Hung Job Monitor is installed and configured correctly.';
    PRINT '';
    PRINT '  Next Steps:';
    PRINT '    1. Monitor is running automatically every 5 minutes';
    PRINT '    2. Collection job is protected with 30-second LOCK_TIMEOUT';
    PRINT '    3. Review install/98_test_hung_monitor.sql for comprehensive tests';
    PRINT '    4. Check config.collection_log periodically for hung job alerts';
END;
ELSE
BEGIN
    PRINT '  ❌ SOME CHECKS FAILED';
    PRINT '';
    PRINT '  Review failures above and fix before proceeding.';
    PRINT '';
    PRINT '  Common fixes:';
    PRINT '    - Run missing installation scripts';
    PRINT '    - Start SQL Server Agent service';
    PRINT '    - Enable disabled jobs/schedules';
END;

PRINT '';
PRINT '============================================================================';
PRINT '';
GO
