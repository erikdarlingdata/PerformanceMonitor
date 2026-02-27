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

IF OBJECT_ID(N'collect.default_trace_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.default_trace_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.default_trace_collector
(
    @hours_back integer = 2, /*how many hours back to collect events*/
    @include_memory_events bit = 1, /*include Server Memory Change events*/
    @include_autogrow_events bit = 1, /*include Database Auto Grow/Shrink events*/
    @include_config_events bit = 1, /*include configuration change events*/
    @include_errorlog_events bit = 1, /*include ErrorLog events*/
    @include_object_events bit = 1, /*include Object Created/Altered/Deleted events*/
    @include_audit_events bit = 1, /*include Security Audit events*/
    @debug bit = 0 /*prints additional diagnostic information*/
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    /*
    Variable declarations for default trace processing
    */
    DECLARE
        @collection_start_time datetime2(7) = SYSDATETIME(),
        @rows_collected bigint = 0,
        @error_message nvarchar(2048) = N'',
        @cutoff_time datetime2(7) = DATEADD(HOUR, -@hours_back, SYSDATETIME()),
        @trace_path nvarchar(260) = N'',
        @max_files integer = 0;

    BEGIN TRY
        /*
        Parameter validation
        */
        IF @hours_back <= 0 OR @hours_back > 168 -- 1 week
        BEGIN
            SET @error_message = N'@hours_back must be between 1 and 168 hours';
            RAISERROR(@error_message, 16, 1);
            RETURN;
        END;

        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.default_trace_events', N'U') IS NULL
        BEGIN
            /*
            Log missing table before attempting to create
            */
            INSERT INTO
                config.collection_log
            (
                collection_time,
                collector_name,
                collection_status,
                rows_collected,
                duration_ms,
                error_message
            )
            VALUES
            (
                @collection_start_time,
                N'default_trace_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.default_trace_events does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'default_trace_events',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.default_trace_events', N'U') IS NULL
            BEGIN
                RAISERROR(N'Table collect.default_trace_events still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        First run detection - collect all available trace data if this is the first execution
        Ignore CONFIG_CHANGE entries when checking for first run (those are just from enabling the trace)
        */
        IF NOT EXISTS (SELECT 1/0 FROM collect.default_trace_events)
        AND NOT EXISTS (SELECT 1/0 FROM config.collection_log WHERE collector_name = N'default_trace_collector' AND collection_status = N'SUCCESS')
        BEGIN
            SET @cutoff_time = CONVERT(datetime2(7), '19000101');

            IF @debug = 1
            BEGIN
                RAISERROR(N'First run detected - collecting all available default trace events', 0, 1) WITH NOWAIT;
            END;
        END;

        /*
        Check if default trace is enabled and running
        SQL Server bug: sometimes the trace is configured to be enabled but doesn't actually start
        */
        DECLARE @trace_configured bit = 0;

        SELECT
            @trace_configured = CONVERT(integer, c.value_in_use)
        FROM sys.configurations AS c
        WHERE c.name = N'default trace enabled';

        IF NOT EXISTS (SELECT 1/0 FROM sys.traces WHERE is_default = 1)
        BEGIN
            /*
            Trace not running - check if it's configured
            */
            IF @trace_configured = 0
            BEGIN
                IF @debug = 1
                BEGIN
                    RAISERROR(N'Default trace not enabled - enabling now', 0, 1) WITH NOWAIT;
                END;

                /*
                Enable default trace
                */
                EXECUTE sys.sp_configure
                    @configname = N'default trace enabled',
                    @configvalue = 1;

                RECONFIGURE;

                /*
                Log that we enabled it
                */
                INSERT INTO
                    config.collection_log
                (
                    collection_time,
                    collector_name,
                    collection_status,
                    rows_collected,
                    duration_ms,
                    error_message
                )
                VALUES
                (
                    @collection_start_time,
                    N'default_trace_collector',
                    N'CONFIG_CHANGE',
                    0,
                    DATEDIFF(MILLISECOND, @collection_start_time, SYSDATETIME()),
                    N'Enabled default trace (sp_configure ''default trace enabled'', 1) - trace will start on next collection run'
                );

                /*
                Return after enabling - trace needs to start before we can collect from it
                Next run will collect data once trace is active
                */
                RETURN;
            END;
            ELSE
            BEGIN
                /*
                SQL Server bug: trace is configured to be enabled but not actually running
                Attempt to restart it by disabling and re-enabling
                */
                IF @debug = 1
                BEGIN
                    RAISERROR(N'Default trace configured but not running - attempting disable/re-enable cycle', 0, 1) WITH NOWAIT;
                END;

                /*
                Disable the trace
                */
                EXECUTE sys.sp_configure
                    @configname = N'default trace enabled',
                    @configvalue = 0;

                RECONFIGURE;

                /*
                Re-enable the trace
                */
                EXECUTE sys.sp_configure
                    @configname = N'default trace enabled',
                    @configvalue = 1;

                RECONFIGURE;

                /*
                Verify the trace is now running
                */
                IF NOT EXISTS (SELECT 1/0 FROM sys.traces WHERE is_default = 1)
                BEGIN
                    /*
                    Restart failed - log and return
                    */
                    INSERT INTO
                        config.collection_log
                    (
                        collection_time,
                        collector_name,
                        collection_status,
                        rows_collected,
                        duration_ms,
                        error_message
                    )
                    VALUES
                    (
                        @collection_start_time,
                        N'default_trace_collector',
                        N'ERROR',
                        0,
                        DATEDIFF(MILLISECOND, @collection_start_time, SYSDATETIME()),
                        N'Default trace restart failed - performed disable/re-enable but trace still not running - may require SQL Server restart'
                    );

                    RETURN;
                END;

                /*
                Log that we restarted it successfully
                */
                INSERT INTO
                    config.collection_log
                (
                    collection_time,
                    collector_name,
                    collection_status,
                    rows_collected,
                    duration_ms,
                    error_message
                )
                VALUES
                (
                    @collection_start_time,
                    N'default_trace_collector',
                    N'CONFIG_CHANGE',
                    0,
                    DATEDIFF(MILLISECOND, @collection_start_time, SYSDATETIME()),
                    N'Default trace restarted successfully - proceeding to collect data'
                );

                IF @debug = 1
                BEGIN
                    RAISERROR(N'Default trace restart successful - proceeding to collect data', 0, 1) WITH NOWAIT;
                END;

                /*
                Continue to data collection below (don't return)
                */
            END;
        END;

        /*
        Get default trace path and configuration
        */
        SELECT
            @trace_path = st.path,
            @max_files = st.max_files
        FROM sys.traces AS st
        WHERE st.is_default = 1;

        IF @debug = 1
        BEGIN
            DECLARE @cutoff_time_string nvarchar(30) = CONVERT(nvarchar(30), @cutoff_time, 120);
            RAISERROR(N'Default trace path: %s', 0, 1, @trace_path) WITH NOWAIT;
            RAISERROR(N'Processing events from: %s onward', 0, 1, @cutoff_time_string) WITH NOWAIT;
        END;

        /*
        Collect default trace events with filtering
        */
        INSERT INTO
            collect.default_trace_events
        (
            collection_time,
            event_time,
            event_name,
            event_class,
            spid,
            database_name,
            database_id,
            login_name,
            host_name,
            application_name,
            server_name,
            object_name,
            filename,
            integer_data,
            integer_data_2,
            text_data,
            binary_data,
            session_login_name,
            error_number,
            severity,
            state,
            event_sequence,
            is_system,
            request_id,
            duration_us,
            end_time
        )
        SELECT
            collection_time = @collection_start_time,
            event_time = ft.StartTime,
            event_name = te.name,
            event_class = ft.EventClass,
            spid = ft.SPID,
            database_name = ft.DatabaseName,
            database_id = ft.DatabaseID,
            login_name = ft.LoginName,
            host_name = ft.HostName,
            application_name = ft.ApplicationName,
            server_name = ft.ServerName,
            object_name = ft.ObjectName,
            filename = ft.FileName,
            integer_data = ft.IntegerData,
            integer_data_2 = ft.IntegerData2,
            text_data = ft.TextData,
            binary_data = ft.BinaryData,
            session_login_name = ft.SessionLoginName,
            error_number = ft.Error,
            severity = ft.Severity,
            state = ft.State,
            event_sequence = ft.EventSequence,
            is_system = ft.IsSystem,
            request_id = ft.RequestID,
            duration_us = ft.Duration,
            end_time = ft.EndTime
        FROM sys.traces AS st
        CROSS APPLY sys.fn_trace_gettable
        (
            LEFT(st.path, LEN(st.path) - CHARINDEX('_', REVERSE(st.path))) + 
            RIGHT(st.path, 4), 
            st.max_files
        ) AS ft
        INNER JOIN sys.trace_events AS te
          ON ft.EventClass = te.trace_event_id
        WHERE st.is_default = 1
        AND   st.status = 1
        AND   ft.StartTime >= @cutoff_time
        AND   ISNULL(ft.DatabaseID, 0) NOT IN (DB_ID(N'PerformanceMonitor'), 1, 3, 4)
        AND   ISNULL(ft.DatabaseID, 0) < 32761 /*exclude contained AG system databases*/
        /*
        Filter for useful system events, excluding login failures
        */
        AND
        (
            /*Server Memory Change events*/
            (@include_memory_events = 1 AND te.name LIKE N'%Server Memory Change%')
            OR
            /*Database Auto Grow/Shrink events (only collect if duration > 1 second)*/
            (@include_autogrow_events = 1 AND
             ISNULL(ft.Duration, 0) > 1000000 AND
             (te.name LIKE N'%Data File Auto Grow%' OR
              te.name LIKE N'%Log File Auto Grow%' OR
              te.name LIKE N'%Data File Auto Shrink%' OR
              te.name LIKE N'%Log File Auto Shrink%'))
            OR
            /*Configuration change events*/
            (@include_config_events = 1 AND
             (te.name LIKE N'%Server Configuration Change%' OR
              te.name LIKE N'%Database Configuration Change%' OR
              te.name LIKE N'%Alter Database%'))
            OR
            /*ErrorLog events*/
            (@include_errorlog_events = 1 AND te.name = N'ErrorLog')
            OR
            /*Object DDL events (exclude tempdb and auto-stats)*/
            (@include_object_events = 1 AND
             ISNULL(ft.DatabaseID, 0) <> 2 AND
             ISNULL(ft.ObjectName, N'') NOT LIKE N'[_]WA[_]%' AND
             (te.name = N'Object:Created' OR
              te.name = N'Object:Altered' OR
              te.name = N'Object:Deleted'))
            OR
            /*Security Audit events*/
            (@include_audit_events = 1 AND
             (te.name = N'Audit Change Audit Event' OR
              te.name = N'Audit DBCC Event' OR
              te.name = N'Audit Server Alter Trace Event'))
        )
        /*
        Avoid duplicates by checking if we've already processed this event
        */
        AND NOT EXISTS
        (
            SELECT
                1/0
            FROM collect.default_trace_events AS dte
            WHERE dte.event_time = ft.StartTime
            AND   dte.event_class = ft.EventClass
            AND   dte.spid = ft.SPID
            AND   dte.event_sequence = ft.EventSequence
            AND   dte.collection_time >= DATEADD(HOUR, -1, @collection_start_time)
        )
        ORDER BY
            ft.StartTime DESC;

        SELECT @rows_collected = ROWCOUNT_BIG();

        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %d default trace events', 0, 1, @rows_collected) WITH NOWAIT;
        END;

        /*
        Log collection activity
        */
        INSERT INTO
            config.collection_log
        (
            collection_time,
            collector_name,
            collection_status,
            rows_collected,
            duration_ms,
            error_message
        )
        VALUES
        (
            @collection_start_time,
            N'default_trace_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @collection_start_time, SYSDATETIME()),
            NULL
        );

        /*
        Return summary information
        */
        SELECT
            collection_time = @collection_start_time,
            rows_collected = @rows_collected,
            hours_processed = @hours_back,
            cutoff_time = @cutoff_time,
            trace_path = @trace_path,
            include_memory_events = @include_memory_events,
            include_autogrow_events = @include_autogrow_events,
            include_config_events = @include_config_events,
            include_errorlog_events = @include_errorlog_events,
            include_object_events = @include_object_events,
            include_audit_events = @include_audit_events,
            success = 1;

    END TRY
    BEGIN CATCH
        SET @error_message = ERROR_MESSAGE();

        /*
        Log errors to collection log
        */
        INSERT INTO
            config.collection_log
        (
            collection_time,
            collector_name,
            collection_status,
            rows_collected,
            duration_ms,
            error_message
        )
        VALUES
        (
            @collection_start_time,
            N'default_trace_collector',
            N'ERROR',
            @rows_collected,
            DATEDIFF(MILLISECOND, @collection_start_time, SYSDATETIME()),
            @error_message
        );

        IF @@TRANCOUNT > 0
        BEGIN
            ROLLBACK;
        END;

        THROW;
    END CATCH;
END;
GO

PRINT 'Default trace collector created successfully';
GO
