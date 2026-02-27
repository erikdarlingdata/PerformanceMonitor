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
System health data collector wrapper
Calls sp_HealthParser with table logging to collect comprehensive system health data
This replaces the simple system health events collector
*/

IF OBJECT_ID(N'collect.system_health_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.system_health_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.system_health_collector
(
    @what_to_check varchar(10) = 'all', /*What portion of data to collect (all, waits, cpu, memory, disk, system, locking)*/
    @hours_back integer = 1, /*How many hours back to analyze*/
    @warnings_only bit = 1, /*Only collect data from recorded warnings*/
    @skip_locks bit = 1, /*Skip the blocking and deadlocks section*/
    @skip_waits bit = 1, /*Skip the wait stats section*/
    @log_retention_days integer = 30, /*Days to retain sp_HealthParser data*/
    @procedure_database sysname = NULL, /*Database where sp_HealthParser is installed (NULL = search PerformanceMonitor then master)*/
    @debug bit = 0 /*Print debugging information*/
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE
        @start_time datetime2(7) = SYSDATETIME(),
        @start_date datetimeoffset(7) = DATEADD(HOUR, -@hours_back, SYSDATETIMEOFFSET()),
        @end_date datetimeoffset(7) = SYSDATETIMEOFFSET(),
        @rows_collected bigint = 0,
        @total_rows_before integer = 0,
        @total_rows_after integer = 0,
        @healthparser_database sysname = NULL,
        @sql nvarchar(max) = N'';

    BEGIN TRY
        /*
        Locate sp_HealthParser
        If user provided a database name, check there
        Otherwise search PerformanceMonitor first, then master
        */
        IF @procedure_database IS NOT NULL
        BEGIN
            SET @sql = N'
            IF OBJECT_ID(N''' + QUOTENAME(@procedure_database) + N'.dbo.sp_HealthParser'', N''P'') IS NOT NULL
            BEGIN
                SELECT @healthparser_database = N''' + QUOTENAME(@procedure_database) + N''';
            END;';

            EXECUTE sys.sp_executesql
                @sql,
                N'@healthparser_database sysname OUTPUT',
                @healthparser_database = @healthparser_database OUTPUT;

            IF @healthparser_database IS NULL
            BEGIN
                RAISERROR(N'sp_HealthParser not found in specified database %s', 16, 1, @procedure_database);
                RETURN;
            END;
        END;
        ELSE
        BEGIN
            IF OBJECT_ID(N'dbo.sp_HealthParser', N'P') IS NOT NULL
            BEGIN
                SET @healthparser_database = N'PerformanceMonitor';
            END;
            ELSE IF OBJECT_ID(N'master.dbo.sp_HealthParser', N'P') IS NOT NULL
            BEGIN
                SET @healthparser_database = N'master';
            END;
            ELSE
            BEGIN
                RAISERROR(N'sp_HealthParser not found in PerformanceMonitor or master - cannot collect system health data', 16, 1);
                RETURN;
            END;
        END;

        /*
        First run detection - collect 3 days of history if this is the first execution
        */
        IF NOT EXISTS (SELECT 1/0 FROM config.collection_log WHERE collector_name = N'system_health_collector')
        BEGIN
            SET @hours_back = 72; /*3 days*/
            SET @start_date = DATEADD(HOUR, -@hours_back, SYSDATETIMEOFFSET());

            IF @debug = 1
            BEGIN
                RAISERROR(N'First run detected - collecting last 3 days of system health data', 0, 1) WITH NOWAIT;
            END;
        END;

        /*
        Get row counts before collection for all HealthParser tables
        Use dynamic SQL to safely handle tables that don't exist yet
        (sp_HealthParser creates them on first run)
        Dynamic SQL is required because SQL Server validates table names at compile time
        */
        SET @sql = N'
        SELECT
            @total_rows_before =
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_SignificantWaits), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_WaitsByCount), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_WaitsByDuration), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_IOIssues), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_CPUTasks), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_MemoryConditions), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_MemoryBroker), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_MemoryNodeOOM), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_SystemHealth), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_SchedulerIssues), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_SevereErrors), 0);';

        BEGIN TRY
            EXECUTE sys.sp_executesql
                @sql,
                N'@total_rows_before integer OUTPUT',
                @total_rows_before = @total_rows_before OUTPUT;
        END TRY
        BEGIN CATCH
            /*
            Tables don't exist yet - this is first run
            sp_HealthParser will create them
            */
            SET @total_rows_before = 0;
        END CATCH;
        
        IF @debug = 1
        BEGIN
            DECLARE @debug_message nvarchar(200) = N'Calling sp_HealthParser from ' + @healthparser_database + N' database with table logging enabled';
            RAISERROR(@debug_message, 0, 1) WITH NOWAIT;
        END;

        /*
        Call sp_HealthParser with table logging enabled
        Using our monitoring database and collect schema
        Build dynamic SQL to call from correct database
        */
        SET @sql = N'
        EXECUTE ' + QUOTENAME(@healthparser_database) + N'.dbo.sp_HealthParser
            @what_to_check = @what_to_check,
            @start_date = @start_date,
            @end_date = @end_date,
            @warnings_only = @warnings_only,
            @skip_locks = @skip_locks,
            @skip_waits = @skip_waits,
            @log_to_table = 1,
            @log_database_name = N''PerformanceMonitor'',
            @log_schema_name = N''collect'',
            @log_table_name_prefix = N''HealthParser'',
            @log_retention_days = @log_retention_days,
            @debug = @debug;';

        EXECUTE sys.sp_executesql
            @sql,
            N'@what_to_check varchar(10), @start_date datetimeoffset(7), @end_date datetimeoffset(7), @warnings_only bit, @skip_locks bit, @skip_waits bit, @log_retention_days integer, @debug bit',
            @what_to_check = @what_to_check,
            @start_date = @start_date,
            @end_date = @end_date,
            @warnings_only = @warnings_only,
            @skip_locks = @skip_locks,
            @skip_waits = @skip_waits,
            @log_retention_days = @log_retention_days,
            @debug = @debug;
        
        /*
        Get row counts after collection to calculate rows added
        */
        SET @sql = N'
        SELECT
            @total_rows_after =
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_SignificantWaits), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_WaitsByCount), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_WaitsByDuration), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_IOIssues), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_CPUTasks), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_MemoryConditions), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_MemoryBroker), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_MemoryNodeOOM), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_SystemHealth), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_SchedulerIssues), 0) +
                ISNULL((SELECT COUNT_BIG(*) FROM collect.HealthParser_SevereErrors), 0);';

        BEGIN TRY
            EXECUTE sys.sp_executesql
                @sql,
                N'@total_rows_after integer OUTPUT',
                @total_rows_after = @total_rows_after OUTPUT;
        END TRY
        BEGIN CATCH
            /*
            Tables still don't exist - unexpected
            */
            SET @total_rows_after = 0;
        END CATCH;
        
        SET @rows_collected = @total_rows_after - @total_rows_before;
        
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
            N'system_health_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );
        
        IF @debug = 1
        BEGIN
            RAISERROR(N'sp_HealthParser completed - %d total rows collected across all tables', 0, 1, @rows_collected) WITH NOWAIT;
        END;
        
    END TRY
    BEGIN CATCH
        DECLARE
            @error_message nvarchar(4000) = ERROR_MESSAGE();
        
        /*
        Log the error
        */
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
            N'system_health_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );
        
        RAISERROR(N'Error in system health collector wrapper: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'System health collector wrapper created successfully';
PRINT 'This procedure calls sp_HealthParser with table logging to collect comprehensive system health data';
PRINT 'Tables will be created in the collect schema with HealthParser_ prefix when first executed';
GO
