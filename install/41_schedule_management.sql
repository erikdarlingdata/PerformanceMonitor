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
Schedule management procedures
Provides easy configuration of collection frequencies and settings
*/

/*
Update collector frequency
*/
IF OBJECT_ID(N'config.update_collector_frequency', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE config.update_collector_frequency AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    config.update_collector_frequency
(
    @collector_name sysname,
    @frequency_minutes integer,
    @enabled bit = NULL,
    @max_duration_minutes integer = NULL
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE
        @rows_updated bigint = 0;
    
    BEGIN TRY
        UPDATE
            config.collection_schedule
        SET
            frequency_minutes = @frequency_minutes,
            enabled = ISNULL(@enabled, enabled),
            max_duration_minutes = ISNULL(@max_duration_minutes, max_duration_minutes),
            modified_date = SYSDATETIME(),
            next_run_time = 
                CASE 
                    WHEN ISNULL(@enabled, enabled) = 1 
                    THEN SYSDATETIME() 
                    ELSE next_run_time 
                END
        WHERE collector_name = @collector_name;
        
        SET @rows_updated = ROWCOUNT_BIG();
        
        IF @rows_updated = 0
        BEGIN
            RAISERROR(N'Collector "%s" not found in schedule', 16, 1, @collector_name);
            RETURN;
        END;
        
        PRINT 'Updated ' + @collector_name + ' frequency to ' + CONVERT(varchar(10), @frequency_minutes) + ' minutes';
        
    END TRY
    BEGIN CATCH
        DECLARE @error_message nvarchar(4000) = ERROR_MESSAGE();
        RAISERROR(N'Error updating collector frequency: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

/*
Enable/disable collector
*/
IF OBJECT_ID(N'config.set_collector_enabled', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE config.set_collector_enabled AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    config.set_collector_enabled
(
    @collector_name sysname,
    @enabled bit
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE
        @rows_updated bigint = 0;
    
    BEGIN TRY
        UPDATE
            config.collection_schedule
        SET
            enabled = @enabled,
            modified_date = SYSDATETIME(),
            next_run_time = 
                CASE 
                    WHEN @enabled = 1 
                    THEN SYSDATETIME() 
                    ELSE NULL 
                END
        WHERE collector_name = @collector_name;
        
        SET @rows_updated = ROWCOUNT_BIG();
        
        IF @rows_updated = 0
        BEGIN
            RAISERROR(N'Collector "%s" not found in schedule', 16, 1, @collector_name);
            RETURN;
        END;
        
        PRINT @collector_name + ' collector ' + 
              CASE WHEN @enabled = 1 THEN 'enabled' ELSE 'disabled' END;
        
    END TRY
    BEGIN CATCH
        DECLARE @error_message nvarchar(4000) = ERROR_MESSAGE();
        RAISERROR(N'Error setting collector enabled status: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

/*
Set up real-time monitoring profile (frequent collection)
*/
IF OBJECT_ID(N'config.enable_realtime_monitoring', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE config.enable_realtime_monitoring AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    config.enable_realtime_monitoring
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    BEGIN TRY
        /*High frequency for real-time dashboard*/
        EXECUTE config.update_collector_frequency N'query_snapshots_collector', 1, 1;
        EXECUTE config.update_collector_frequency N'wait_stats_collector', 1, 1;
        EXECUTE config.update_collector_frequency N'query_stats_collector', 1, 1;
        EXECUTE config.update_collector_frequency N'procedure_stats_collector', 2, 1;
        EXECUTE config.update_collector_frequency N'query_store_collector', 2, 1;
        EXECUTE config.update_collector_frequency N'blocked_process_xml_collector', 2, 1;
        EXECUTE config.update_collector_frequency N'cpu_utilization_stats_collector', 2, 1;
        EXECUTE config.update_collector_frequency N'memory_stats_collector', 2, 1;
        EXECUTE config.update_collector_frequency N'perfmon_stats_collector', 2, 1;
        EXECUTE config.update_collector_frequency N'file_io_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'deadlock_xml_collector', 5, 1;

        /*Medium frequency for context*/
        EXECUTE config.update_collector_frequency N'memory_grant_stats_collector', 2, 1;
        EXECUTE config.update_collector_frequency N'cpu_scheduler_stats_collector', 2, 1;
        EXECUTE config.update_collector_frequency N'memory_clerks_stats_collector', 2, 1;
        EXECUTE config.update_collector_frequency N'latch_stats_collector', 2, 1;
        EXECUTE config.update_collector_frequency N'spinlock_stats_collector', 2, 1;
        EXECUTE config.update_collector_frequency N'default_trace_collector', 2, 1;
        EXECUTE config.update_collector_frequency N'system_health_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'memory_pressure_events_collector', 2, 1;
        EXECUTE config.update_collector_frequency N'plan_cache_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'blocking_deadlock_analyzer', 2, 1;
        EXECUTE config.update_collector_frequency N'process_blocked_process_xml', 2, 1;
        EXECUTE config.update_collector_frequency N'process_deadlock_xml', 2, 1;
        EXECUTE config.update_collector_frequency N'trace_analysis_collector', 5, 1;

        PRINT 'Real-time monitoring profile enabled';
        PRINT 'Query/procedure stats every 1-2 minutes, everything else 2-5 minutes';
        
    END TRY
    BEGIN CATCH
        DECLARE @error_message nvarchar(4000) = ERROR_MESSAGE();
        RAISERROR(N'Error enabling real-time monitoring: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

/*
Set up consulting analysis profile (balanced collection during business hours)
*/
IF OBJECT_ID(N'config.enable_consulting_analysis', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE config.enable_consulting_analysis AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    config.enable_consulting_analysis
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    BEGIN TRY
        /*Balanced frequencies for consulting work*/
        EXECUTE config.update_collector_frequency N'query_snapshots_collector', 1, 1;
        EXECUTE config.update_collector_frequency N'wait_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'query_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'procedure_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'query_store_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'cpu_utilization_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'blocked_process_xml_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'perfmon_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'memory_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'file_io_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'memory_grant_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'cpu_scheduler_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'memory_clerks_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'deadlock_xml_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'latch_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'spinlock_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'default_trace_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'system_health_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'memory_pressure_events_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'plan_cache_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'blocking_deadlock_analyzer', 5, 1;
        EXECUTE config.update_collector_frequency N'process_blocked_process_xml', 5, 1;
        EXECUTE config.update_collector_frequency N'process_deadlock_xml', 5, 1;
        EXECUTE config.update_collector_frequency N'trace_analysis_collector', 5, 1;

        PRINT 'Consulting analysis profile enabled';
        PRINT 'Balanced collection frequencies for comprehensive analysis';
        
    END TRY
    BEGIN CATCH
        DECLARE @error_message nvarchar(4000) = ERROR_MESSAGE();
        RAISERROR(N'Error enabling consulting analysis: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

/*
Set up baseline monitoring profile (minimal resource usage)
*/
IF OBJECT_ID(N'config.enable_baseline_monitoring', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE config.enable_baseline_monitoring AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    config.enable_baseline_monitoring
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    BEGIN TRY
        /*Baseline: max 5 minutes for everything*/
        EXECUTE config.update_collector_frequency N'wait_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'query_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'procedure_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'query_store_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'memory_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'cpu_utilization_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'system_health_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'plan_cache_stats_collector', 5, 1;
        EXECUTE config.update_collector_frequency N'blocking_deadlock_analyzer', 5, 1;

        /*Disable high-frequency collectors*/
        EXECUTE config.set_collector_enabled N'query_snapshots_collector', 0;

        PRINT 'Baseline monitoring profile enabled';
        PRINT 'All collectors at 5-minute intervals, snapshots disabled';
        
    END TRY
    BEGIN CATCH
        DECLARE @error_message nvarchar(4000) = ERROR_MESSAGE();
        RAISERROR(N'Error enabling baseline monitoring: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

/*
Show current schedule status
*/
IF OBJECT_ID(N'config.show_collection_schedule', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE config.show_collection_schedule AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    config.show_collection_schedule
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    SELECT
        collector_name,
        enabled,
        frequency_minutes,
        next_run_time,
        minutes_until_next_run = 
            CASE 
                WHEN enabled = 1 AND next_run_time IS NOT NULL 
                THEN DATEDIFF(MINUTE, SYSDATETIME(), next_run_time)
                ELSE NULL 
            END,
        last_run_time,
        max_duration_minutes,
        description
    FROM config.collection_schedule
    ORDER BY
        enabled DESC,
        next_run_time;
END;
GO

PRINT 'Schedule management procedures created successfully';
PRINT '';
PRINT 'Available procedures:';
PRINT '- config.update_collector_frequency - Change frequency for specific collector';
PRINT '- config.set_collector_enabled - Enable/disable specific collector';
PRINT '- config.enable_realtime_monitoring - High frequency for dashboards';
PRINT '- config.enable_consulting_analysis - Balanced for analysis work';
PRINT '- config.enable_baseline_monitoring - Minimal overhead monitoring';
PRINT '- config.show_collection_schedule - Display current schedule';
GO
