/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

For usage, licensing, and support:
https://github.com/erikdarlingdata/DarlingData

Trace File Analysis Collector - Performance Monitor
Erik Darling - erik@erikdarling.com
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

IF OBJECT_ID(N'collect.trace_analysis_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.trace_analysis_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.trace_analysis_collector
(
    @trace_file_pattern nvarchar(260) = N'Monitor_LongQueries_*.trc', /*pattern to match trace files*/
    @hours_back integer = 2, /*how many hours back to process*/
    @max_files_per_run integer = 5, /*maximum files to process per run*/
    @min_duration_ms bigint = 1000, /*minimum duration to store (1 second)*/
    @debug bit = 0 /*prints additional diagnostic information*/
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    /*
    Variable declarations for trace file processing
    */
    DECLARE
        @collection_start_time datetime2(7) = SYSDATETIME(),
        @rows_collected bigint = 0,
        @files_processed integer = 0,
        @error_message nvarchar(2048) = N'',
        @sql nvarchar(max) = N'',
        @trace_directory nvarchar(4000) = N'',
        @cutoff_time datetime2(7) = DATEADD(HOUR, -@hours_back, SYSDATETIME());

    BEGIN TRY
        /*
        Parameter validation
        */
        IF @hours_back <= 0 OR @hours_back > 72
        BEGIN
            SET @error_message = N'@hours_back must be between 1 and 72 hours';
            RAISERROR(@error_message, 16, 1);
            RETURN;
        END;

        IF @max_files_per_run <= 0 OR @max_files_per_run > 50
        BEGIN
            SET @error_message = N'@max_files_per_run must be between 1 and 50';
            RAISERROR(@error_message, 16, 1);
            RETURN;
        END;

        IF @min_duration_ms < 0
        BEGIN
            SET @error_message = N'@min_duration_ms must be >= 0';
            RAISERROR(@error_message, 16, 1);
            RETURN;
        END;

        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.trace_analysis', N'U') IS NULL
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
                N'trace_analysis_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.trace_analysis does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'trace_analysis',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.trace_analysis', N'U') IS NULL
            BEGIN
                RAISERROR(N'Table collect.trace_analysis still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Get SQL Server error log directory for trace file location
        */
        SELECT @trace_directory =
            LEFT
            (
                CONVERT(nvarchar(4000), SERVERPROPERTY('ErrorLogFileName')),
                LEN(CONVERT(nvarchar(4000), SERVERPROPERTY('ErrorLogFileName'))) -
                CHARINDEX('\', REVERSE(CONVERT(nvarchar(4000), SERVERPROPERTY('ErrorLogFileName')))) + 1
            );

        IF @debug = 1
        BEGIN
            DECLARE @cutoff_time_string nvarchar(30) = CONVERT(nvarchar(30), @cutoff_time, 120);
            RAISERROR(N'Looking for trace files in directory: %s with pattern: %s', 0, 1, @trace_directory, @trace_file_pattern) WITH NOWAIT;
            RAISERROR(N'Processing events from: %s onward', 0, 1, @cutoff_time_string) WITH NOWAIT;
        END;

        /*
        Create temp table for file processing
        */
        CREATE TABLE
            #trace_files
        (
            file_path nvarchar(500) NOT NULL,
            file_name nvarchar(260) NOT NULL,
            processed bit NOT NULL DEFAULT 0
        );

        /*
        Build search pattern for sys.traces
        fn_trace_gettable does NOT support wildcards - we must get actual file paths
        */
        DECLARE
            @trace_search_pattern nvarchar(500) = @trace_directory +
                REPLACE(@trace_file_pattern, N'*.trc', N'%'),
            @current_trace_path nvarchar(500) = N'',
            @trace_count integer = 0;

        /*
        Create temp table to store trace data before filtering
        */
        CREATE TABLE
            #trace_events
        (
            trace_file nvarchar(260) NOT NULL,
            event_class integer NOT NULL,
            duration_microseconds bigint NULL,
            cpu_time bigint NULL,
            reads bigint NULL,
            writes bigint NULL,
            spid integer NULL,
            start_time datetime2(7) NULL,
            end_time datetime2(7) NULL,
            database_name nvarchar(128) NULL,
            login_name nvarchar(128) NULL,
            nt_user_name nvarchar(128) NULL,
            application_name nvarchar(256) NULL,
            host_name nvarchar(128) NULL,
            sql_text nvarchar(max) NULL,
            object_id bigint NULL,
            client_process_id integer NULL,
            row_counts bigint NULL
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Looking for traces matching pattern: %s', 0, 1, @trace_search_pattern) WITH NOWAIT;
        END;

        /*
        Get trace file paths from sys.traces
        fn_trace_gettable requires actual file paths, not wildcards
        */
        DECLARE @trace_cursor CURSOR;

        SET @trace_cursor =
            CURSOR
            LOCAL
            STATIC
            READ_ONLY
        FOR
            SELECT
                t.path
            FROM sys.traces AS t
            WHERE t.path LIKE @trace_search_pattern
            AND   t.is_default = 0
            ORDER BY
                t.id;

        OPEN @trace_cursor;

        FETCH NEXT
        FROM @trace_cursor
        INTO @current_trace_path;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @trace_count += 1;

            IF @debug = 1
            BEGIN
                RAISERROR(N'Reading trace file: %s', 0, 1, @current_trace_path) WITH NOWAIT;
            END;

            /*
            Read trace data from this specific file
            */
            BEGIN TRY
                INSERT INTO
                    #trace_events
                (
                    trace_file,
                    event_class,
                    duration_microseconds,
                    cpu_time,
                    reads,
                    writes,
                    spid,
                    start_time,
                    end_time,
                    database_name,
                    login_name,
                    nt_user_name,
                    application_name,
                    host_name,
                    sql_text,
                    object_id,
                    client_process_id,
                    row_counts
                )
                SELECT TOP (100000)
                    trace_file = @current_trace_path,
                    event_class = trc.EventClass,
                    duration_microseconds = trc.Duration,
                    cpu_time = trc.CPU,
                    reads = trc.Reads,
                    writes = trc.Writes,
                    spid = trc.SPID,
                    start_time = trc.StartTime,
                    end_time = trc.EndTime,
                    database_name = trc.DatabaseName,
                    login_name = trc.LoginName,
                    nt_user_name = trc.NTUserName,
                    application_name = trc.ApplicationName,
                    host_name = trc.HostName,
                    sql_text = trc.TextData,
                    object_id = trc.ObjectID,
                    client_process_id = trc.ClientProcessID,
                    row_counts = trc.RowCounts
                FROM fn_trace_gettable(@current_trace_path, DEFAULT) AS trc
                WHERE trc.StartTime >= @cutoff_time
                AND   trc.Duration >= (@min_duration_ms * 1000)
                AND   trc.EventClass IN (10, 12, 16)
                AND   trc.DatabaseName IS NOT NULL
                AND   trc.DatabaseName NOT IN (N'master', N'msdb', N'model', N'tempdb', N'PerformanceMonitor')
                AND   trc.DatabaseName NOT LIKE N'%[_]master' /*exclude contained AG system databases*/
                AND   trc.DatabaseName NOT LIKE N'%[_]msdb' /*exclude contained AG system databases*/
                AND   NOT EXISTS
                (
                    SELECT
                        1/0
                    FROM config.collector_database_exclusions AS e
                    WHERE e.database_name = trc.DatabaseName
                )
                ORDER BY
                    trc.StartTime DESC;

                IF @debug = 1
                BEGIN
                    DECLARE @events_from_file bigint = ROWCOUNT_BIG();
                    RAISERROR(N'Retrieved %d events from this trace file', 0, 1, @events_from_file) WITH NOWAIT;
                END;

            END TRY
            BEGIN CATCH
                SET @error_message = ERROR_MESSAGE();

                IF @debug = 1
                BEGIN
                    RAISERROR(N'Error reading trace file %s: %s', 0, 1, @current_trace_path, @error_message) WITH NOWAIT;
                END;
            END CATCH;

            FETCH NEXT
            FROM @trace_cursor
            INTO @current_trace_path;
        END;

        SELECT @rows_collected = COUNT_BIG(*) FROM #trace_events;

        IF @debug = 1
        BEGIN
            RAISERROR(N'Found %d traces, collected %d total events', 0, 1, @trace_count, @rows_collected) WITH NOWAIT;
        END;

        IF @trace_count = 0
        AND @debug = 1
        BEGIN
            RAISERROR(N'No traces found matching pattern. Is the trace running?', 0, 1) WITH NOWAIT;
        END;

        /*
        Process the trace events if we found any
        */
        IF @rows_collected > 0
        BEGIN
            /*
            Insert filtered and processed data into the analysis table
            */
            INSERT INTO
                collect.trace_analysis
            (
                collection_time,
                trace_file_name,
                event_class,
                event_name,
                database_name,
                login_name,
                nt_user_name,
                application_name,
                host_name,
                spid,
                duration_ms,
                cpu_ms,
                reads,
                writes,
                row_counts,
                start_time,
                end_time,
                sql_text,
                object_id,
                client_process_id
            )
            SELECT
                collection_time = @collection_start_time,
                trace_file_name = te.trace_file,
                event_class = te.event_class,
                event_name = 
                    CASE te.event_class
                        WHEN 10 THEN N'RPC:Completed'
                        WHEN 12 THEN N'SQL:BatchCompleted' 
                        WHEN 16 THEN N'Attention'
                        ELSE N'Unknown'
                    END,
                database_name = te.database_name,
                login_name = te.login_name,
                nt_user_name = te.nt_user_name,
                application_name = te.application_name,
                host_name = te.host_name,
                spid = te.spid,
                duration_ms = te.duration_microseconds / 1000,
                cpu_ms = te.cpu_time,
                reads = te.reads,
                writes = te.writes,
                row_counts = te.row_counts,
                start_time = te.start_time,
                end_time = te.end_time,
                sql_text = 
                    CASE 
                        WHEN LEN(te.sql_text) > 8000 
                        THEN LEFT(te.sql_text, 8000) + N'... [TRUNCATED]'
                        ELSE te.sql_text
                    END,
                object_id = te.object_id,
                client_process_id = te.client_process_id
            FROM #trace_events AS te
            WHERE te.duration_microseconds >= (@min_duration_ms * 1000)
            /*
            Avoid duplicates by checking if we've already processed this event
            */
            AND NOT EXISTS
            (
                SELECT
                    1/0
                FROM collect.trace_analysis AS ta
                WHERE ta.spid = te.spid
                AND   ta.start_time = te.start_time
                AND   ta.duration_ms = te.duration_microseconds / 1000
                AND   ta.collection_time >= DATEADD(HOUR, -1, @collection_start_time)
            );

            SELECT @rows_collected = ROWCOUNT_BIG();
            SET @files_processed = 1;

            IF @debug = 1
            BEGIN
                RAISERROR(N'Inserted %d new trace analysis records', 0, 1, @rows_collected) WITH NOWAIT;
            END;
        END;
        ELSE
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'No trace events found matching criteria', 0, 1) WITH NOWAIT;
            END;
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
            N'trace_analysis_collector',
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
            files_processed = @files_processed,
            rows_collected = @rows_collected,
            hours_processed = @hours_back,
            min_duration_ms = @min_duration_ms,
            trace_pattern = @trace_file_pattern,
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
            N'trace_analysis_collector',
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

PRINT 'Trace analysis collector created successfully';
GO