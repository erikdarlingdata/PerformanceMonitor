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
Scheduled master collector procedure
Runs collectors based on config.collection_schedule table
Provides configurable, resilient collection with isolated error handling
*/

IF OBJECT_ID(N'collect.scheduled_master_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.scheduled_master_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.scheduled_master_collector
(
    @force_run_all bit = 0, /*Ignore schedule and run all enabled collectors*/
    @debug bit = 0 /*Print debugging information*/
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SET LOCK_TIMEOUT 30000; /*30 seconds - protects against blocking waits*/
    
    DECLARE
        @total_collectors_run integer = 0,
        @total_errors integer = 0,
        @start_time datetime2(7) = SYSDATETIME(),
        @collector_name sysname,
        @collector_start_time datetime2(7),
        @collector_duration_ms integer,
        @schedule_id integer,
        @frequency_minutes integer,
        @max_duration_minutes integer,
        @minutes_back integer,
        @error_message nvarchar(4000);
    
    BEGIN TRY
        /*
        Self-heal: Ensure all config tables exist and are properly initialized
        */
        EXECUTE config.ensure_config_tables
            @debug = @debug;

        IF @debug = 1
        BEGIN
            RAISERROR(N'Starting scheduled master collection cycle', 0, 1) WITH NOWAIT;
        END;

        /*
        Collect server information only after server restarts
        Check if sqlserver_start_time has changed since last collection
        Configuration changes require restart, so this is the optimal time to collect
        */
        DECLARE
            @current_start_time datetime2(7),
            @last_recorded_start_time datetime2(7);

        SELECT
            @current_start_time = osi.sqlserver_start_time
        FROM sys.dm_os_sys_info AS osi
        OPTION(RECOMPILE);

        SELECT TOP (1)
            @last_recorded_start_time = h.sqlserver_start_time
        FROM config.server_info_history AS h
        ORDER BY
            h.collection_time DESC
        OPTION(RECOMPILE);

        /*
        Only collect if server restarted OR first run (table empty)
        */
        IF @last_recorded_start_time IS NULL OR @current_start_time > @last_recorded_start_time
        BEGIN
            IF @debug = 1
            BEGIN
                DECLARE @restart_time_msg nvarchar(30) = CONVERT(nvarchar(30), @current_start_time, 120);
                RAISERROR(N'Server restart detected (start time: %s), collecting server info', 0, 1, @restart_time_msg) WITH NOWAIT;
            END;

            INSERT INTO
                config.server_info_history
            (
                collection_time,
                sqlserver_start_time,
                server_name,
                instance_name,
                sql_version,
                edition,
                physical_memory_mb,
                cpu_count,
                environment_type
            )
            SELECT
                collection_time = @start_time,
                sqlserver_start_time = osi.sqlserver_start_time,
                server_name = @@SERVERNAME,
                instance_name = ISNULL(CONVERT(nvarchar(128), SERVERPROPERTY('InstanceName')), N'DEFAULT'),
                sql_version =
                    CONVERT(nvarchar(128), SERVERPROPERTY('ProductVersion')) + N' - ' +
                    CONVERT(nvarchar(128), SERVERPROPERTY('ProductLevel')),
                edition = CONVERT(nvarchar(128), SERVERPROPERTY('Edition')),
                physical_memory_mb = osi.physical_memory_kb / 1024,
                cpu_count = osi.cpu_count,
                environment_type =
                    CASE
                        WHEN SERVERPROPERTY('EngineEdition') = 5 THEN N'AzureDB'
                        WHEN SERVERPROPERTY('EngineEdition') = 8 THEN N'AzureMI'
                        WHEN @@VERSION LIKE '%EC2%' THEN N'AWSRDS'
                        ELSE N'OnPrem'
                    END
            FROM sys.dm_os_sys_info AS osi
            OPTION (RECOMPILE);
        END;
        
        /*
        Cursor for collectors that should run based on schedule
        */
        DECLARE 
            @collector_cursor CURSOR 
            
        SET @collector_cursor =
            CURSOR
            LOCAL
            STATIC
            READ_ONLY
            FOR
            SELECT
                cs.schedule_id,
                cs.collector_name,
                cs.frequency_minutes,
                cs.max_duration_minutes
            FROM config.collection_schedule AS cs
            WHERE cs.enabled = 1
            AND   (
                      @force_run_all = 1
                      OR cs.next_run_time <= SYSDATETIME()
                      OR cs.next_run_time IS NULL
                  )
            ORDER BY
                cs.next_run_time
            OPTION(RECOMPILE);
        
        OPEN @collector_cursor;
        
        FETCH NEXT 
        FROM @collector_cursor 
        INTO 
            @schedule_id, 
            @collector_name, 
            @frequency_minutes, 
            @max_duration_minutes;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @collector_start_time = SYSDATETIME();
            SET @minutes_back = @frequency_minutes * 2;

            IF @debug = 1
            BEGIN
                RAISERROR(N'Running collector: %s (frequency: %d minutes)', 0, 1, @collector_name, @frequency_minutes) WITH NOWAIT;
            END;
            
            /*
            Execute the appropriate collector with isolated error handling
            */
            BEGIN TRY
                IF @collector_name = N'wait_stats_collector'
                BEGIN
                    EXECUTE collect.wait_stats_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'query_stats_collector'
                BEGIN
                    EXECUTE collect.query_stats_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'memory_stats_collector'
                BEGIN
                    EXECUTE collect.memory_stats_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'memory_pressure_events_collector'
                BEGIN
                    EXECUTE collect.memory_pressure_events_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'system_health_collector'
                BEGIN
                    EXECUTE collect.system_health_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'blocked_process_xml_collector'
                BEGIN
                    EXECUTE collect.blocked_process_xml_collector @minutes_back = @minutes_back, @debug = @debug;
                END;
                ELSE IF @collector_name = N'process_blocked_process_xml'
                BEGIN
                    EXECUTE collect.process_blocked_process_xml @debug = @debug;
                END;
                ELSE IF @collector_name = N'deadlock_xml_collector'
                BEGIN
                    EXECUTE collect.deadlock_xml_collector @minutes_back = @minutes_back, @debug = @debug;
                END;
                ELSE IF @collector_name = N'process_deadlock_xml'
                BEGIN
                    EXECUTE collect.process_deadlock_xml @debug = @debug;
                END;
                ELSE IF @collector_name = N'query_store_collector'
                BEGIN
                    EXECUTE collect.query_store_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'procedure_stats_collector'
                BEGIN
                    EXECUTE collect.procedure_stats_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'query_snapshots_collector'
                BEGIN
                    EXECUTE collect.query_snapshots_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'file_io_stats_collector'
                BEGIN
                    EXECUTE collect.file_io_stats_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'memory_grant_stats_collector'
                BEGIN
                    EXECUTE collect.memory_grant_stats_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'cpu_scheduler_stats_collector'
                BEGIN
                    EXECUTE collect.cpu_scheduler_stats_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'memory_clerks_stats_collector'
                BEGIN
                    EXECUTE collect.memory_clerks_stats_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'perfmon_stats_collector'
                BEGIN
                    EXECUTE collect.perfmon_stats_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'cpu_utilization_stats_collector'
                BEGIN
                    EXECUTE collect.cpu_utilization_stats_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'trace_management_collector'
                BEGIN
                    EXECUTE collect.trace_management_collector @action = N'RESTART', @debug = @debug;
                END;
                ELSE IF @collector_name = N'trace_analysis_collector'
                BEGIN
                    EXECUTE collect.trace_analysis_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'default_trace_collector'
                BEGIN
                    EXECUTE collect.default_trace_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'blocking_deadlock_analyzer'
                BEGIN
                    EXECUTE collect.blocking_deadlock_analyzer @debug = @debug;
                END;
                ELSE IF @collector_name = N'server_configuration_collector'
                BEGIN
                    EXECUTE collect.server_configuration_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'database_configuration_collector'
                BEGIN
                    EXECUTE collect.database_configuration_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'configuration_issues_analyzer'
                BEGIN
                    EXECUTE collect.configuration_issues_analyzer @debug = @debug;
                END;
                ELSE IF @collector_name = N'latch_stats_collector'
                BEGIN
                    EXECUTE collect.latch_stats_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'spinlock_stats_collector'
                BEGIN
                    EXECUTE collect.spinlock_stats_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'tempdb_stats_collector'
                BEGIN
                    EXECUTE collect.tempdb_stats_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'plan_cache_stats_collector'
                BEGIN
                    EXECUTE collect.plan_cache_stats_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'session_stats_collector'
                BEGIN
                    EXECUTE collect.session_stats_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'waiting_tasks_collector'
                BEGIN
                    EXECUTE collect.waiting_tasks_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'session_wait_stats_collector'
                BEGIN
                    EXECUTE collect.session_wait_stats_collector @debug = @debug;
                END;
                ELSE IF @collector_name = N'running_jobs_collector'
                BEGIN
                    EXECUTE collect.running_jobs_collector @debug = @debug;
                END;
                ELSE
                BEGIN
                    RAISERROR(N'Unknown collector: %s', 11, 1, @collector_name);
                END;
                
                SET @total_collectors_run = @total_collectors_run + 1;
                
            END TRY
            BEGIN CATCH
                SET @total_errors = @total_errors + 1;
                SET @error_message = ERROR_MESSAGE();

                /*
                Log individual collector error but continue with others
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
                    @collector_name,
                    N'ERROR',
                    DATEDIFF(MILLISECOND, @collector_start_time, SYSDATETIME()),
                    @error_message
                );

                IF @debug = 1
                BEGIN
                    RAISERROR(N'Error in collector %s: %s', 0, 1, @collector_name, @error_message) WITH NOWAIT;
                END;
            END CATCH;
            
            /*
            Update schedule for next run regardless of success/failure
            */
            SET @collector_duration_ms = DATEDIFF(MILLISECOND, @collector_start_time, SYSDATETIME());
            
            UPDATE
                config.collection_schedule
            SET
                last_run_time = @collector_start_time,
                next_run_time = DATEADD(MINUTE, @frequency_minutes, @collector_start_time)
            WHERE schedule_id = @schedule_id;
            
            IF @debug = 1
            BEGIN
                DECLARE @next_run_time nvarchar(20) = CONVERT(nvarchar(20), DATEADD(MINUTE, @frequency_minutes, @collector_start_time), 120);
                RAISERROR(N'Completed %s in %d ms. Next run: %s', 0, 1,
                    @collector_name,
                    @collector_duration_ms,
                    @next_run_time
                ) WITH NOWAIT;
            END;
            
            FETCH NEXT 
            FROM @collector_cursor 
            INTO 
                @schedule_id, 
                @collector_name, 
                @frequency_minutes, 
                @max_duration_minutes;
        END;
        
        /*
        Log master collection cycle completion
        */
        INSERT INTO
            config.collection_log
        (
            collector_name,
            collection_status,
            rows_collected,
            duration_ms,
            error_message
        )
        VALUES
        (
            N'scheduled_master_collector',
            CASE 
                WHEN @total_errors = 0 THEN N'SUCCESS'
                ELSE N'PARTIAL'
            END,
            @total_collectors_run,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            CASE 
                WHEN @total_errors > 0 
                THEN N'Completed with ' + CONVERT(nvarchar(10), @total_errors) + N' collector errors'
                ELSE NULL
            END
        );
        
        IF @debug = 1
        BEGIN
            RAISERROR(N'Scheduled collection cycle completed - %d collectors run, %d errors', 0, 1, @total_collectors_run, @total_errors) WITH NOWAIT;
        END;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;

        SET @error_message = ERROR_MESSAGE();
        
        /*
        Log the master collection error
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
            N'scheduled_master_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );
        
        RAISERROR(N'Error in scheduled master collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Scheduled master collector created successfully';
PRINT 'Use collect.scheduled_master_collector to run collectors based on schedule table';
PRINT 'Create SQL Server Agent job to run this procedure every minute for flexible scheduling';
GO
