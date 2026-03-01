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
Blocked process XML processor
Background task that polls for new blocked process XML and parses it via sp_HumanEventsBlockViewer
This is the second phase - the CPU-intensive parsing is separated from fast collection
*/

IF OBJECT_ID(N'collect.process_blocked_process_xml', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.process_blocked_process_xml AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.process_blocked_process_xml
(
    @max_events_to_process integer = 1000, /*Maximum events to process in one execution*/
    @start_date datetime2(7) = NULL, /*Only process events after this date*/
    @end_date datetime2(7) = NULL, /*Only process events before this date*/
    @log_retention_days integer = 30, /*How long to keep parsed results*/
    @procedure_database sysname = NULL, /*Database where sp_HumanEventsBlockViewer is installed (NULL = search PerformanceMonitor then master)*/
    @debug bit = 0 /*Print debugging information*/
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE
        @rows_available integer = 0,
        @rows_deleted bigint = 0,
        @rows_marked bigint = 0,
        @rows_parsed bigint = 0,
        @start_time datetime2(7) = SYSDATETIME(),
        @utc_offset_minutes integer = DATEDIFF(MINUTE, GETUTCDATE(), SYSDATETIME()),
        @start_date_local datetime2(7) = NULL,
        @end_date_local datetime2(7) = NULL,
        @error_message nvarchar(4000),
        @error_number integer,
        @blockviewer_database sysname = NULL,
        @sql nvarchar(max) = N'',
        @debug_msg nvarchar(500) = N'';

    BEGIN TRY
        BEGIN TRANSACTION;

        /*
        Locate sp_HumanEventsBlockViewer
        If user provided a database name, check there
        Otherwise search PerformanceMonitor first, then master
        */
        IF @procedure_database IS NOT NULL
        BEGIN
            SET @sql = N'
            IF OBJECT_ID(N''' + QUOTENAME(@procedure_database) + N'.dbo.sp_HumanEventsBlockViewer'', N''P'') IS NOT NULL
            BEGIN
                SELECT @blockviewer_database = N''' + QUOTENAME(@procedure_database) + N''';
            END;';

            EXECUTE sys.sp_executesql
                @sql,
                N'@blockviewer_database sysname OUTPUT',
                @blockviewer_database = @blockviewer_database OUTPUT;

            IF @blockviewer_database IS NULL
            BEGIN
                SET @error_message = N'sp_HumanEventsBlockViewer not found in specified database ' + @procedure_database + N'. Please install it from https://github.com/erikdarlingdata/DarlingData';
                RAISERROR(@error_message, 16, 1);
            END;
        END;
        ELSE
        BEGIN
            IF OBJECT_ID(N'dbo.sp_HumanEventsBlockViewer', N'P') IS NOT NULL
            BEGIN
                SET @blockviewer_database = N'PerformanceMonitor';
            END;
            ELSE IF OBJECT_ID(N'master.dbo.sp_HumanEventsBlockViewer', N'P') IS NOT NULL
            BEGIN
                SET @blockviewer_database = N'master';
            END;
            ELSE
            BEGIN
                SET @error_message = N'sp_HumanEventsBlockViewer is not installed in PerformanceMonitor or master. Please install it from https://github.com/erikdarlingdata/DarlingData';
                RAISERROR(@error_message, 16, 1);
            END;
        END;

        /*
        Count unprocessed events
        When no date range specified, only count unprocessed rows
        When date range IS specified (manual re-processing), count all rows in range
        */
        SELECT
            @rows_available = COUNT_BIG(*)
        FROM collect.blocked_process_xml AS bx
        WHERE (@start_date IS NOT NULL OR bx.is_processed = 0)
        AND   (@start_date IS NULL OR bx.collection_time >= @start_date)
        AND   (@end_date IS NULL OR bx.collection_time <= @end_date)
        OPTION(RECOMPILE);

        IF @debug = 1
        BEGIN
            RAISERROR(N'Found %d blocked process XML events to process', 0, 1, @rows_available) WITH NOWAIT;
        END;

        IF @rows_available > 0
        BEGIN
            /*
            Derive date range from unprocessed rows when not explicitly provided
            This ensures we only parse new data and pass proper bounds to sp_HumanEventsBlockViewer
            */
            IF @start_date IS NULL AND @end_date IS NULL
            BEGIN
                SELECT
                    @start_date = MIN(bx.event_time),
                    @end_date = MAX(bx.event_time)
                FROM collect.blocked_process_xml AS bx
                WHERE bx.is_processed = 0
                AND   bx.event_time IS NOT NULL
                OPTION(RECOMPILE);

                /*
                Convert UTC event_time to local time for sp_HumanEventsBlockViewer
                The proc expects local time inputs and converts to UTC internally
                Raw table event_time is UTC (from XE @timestamp attribute)
                */
                SELECT
                    @start_date_local = DATEADD(MINUTE, @utc_offset_minutes, @start_date),
                    @end_date_local = DATEADD(MINUTE, @utc_offset_minutes, @end_date);

                IF @debug = 1
                BEGIN
                    SET @debug_msg = N'Derived date range (UTC): ' + ISNULL(CONVERT(nvarchar(30), @start_date, 121), N'NULL') + N' to ' + ISNULL(CONVERT(nvarchar(30), @end_date, 121), N'NULL');
                    RAISERROR(@debug_msg, 0, 1) WITH NOWAIT;
                    SET @debug_msg = N'Converted to local: ' + ISNULL(CONVERT(nvarchar(30), @start_date_local, 121), N'NULL') + N' to ' + ISNULL(CONVERT(nvarchar(30), @end_date_local, 121), N'NULL');
                    RAISERROR(@debug_msg, 0, 1) WITH NOWAIT;
                END;
            END;
            ELSE
            BEGIN
                /*
                User provided explicit dates (assumed local time)
                No conversion needed — pass through directly
                */
                SELECT
                    @start_date_local = @start_date,
                    @end_date_local = @end_date;
            END;

            /*
            Delete existing parsed blocking events for the time range to prevent duplicates
            sp_HumanEventsBlockViewer will re-insert fresh parsed data
            */
            IF @start_date_local IS NOT NULL AND @end_date_local IS NOT NULL
            BEGIN
                DELETE b
                FROM collect.blocking_BlockedProcessReport AS b
                WHERE b.event_time >= @start_date_local
                AND   b.event_time <= @end_date_local;

                SELECT
                    @rows_deleted = ROWCOUNT_BIG();

                IF @debug = 1
                BEGIN
                    RAISERROR(N'Deleted %I64d existing parsed blocking events for time range', 0, 1, @rows_deleted) WITH NOWAIT;
                END;
            END;

            /*
            Call sp_HumanEventsBlockViewer to parse the XML
            It will read from collect.blocked_process_xml and write parsed results
            to collect.blocking_BlockedProcessReport table
            Build dynamic SQL to call from correct database
            */
            SET @sql = N'
            EXECUTE ' + QUOTENAME(@blockviewer_database) + N'.dbo.sp_HumanEventsBlockViewer
                @target_type = N''table'',
                @target_database = N''PerformanceMonitor'',
                @target_schema = N''collect'',
                @target_table = N''blocked_process_xml'',
                @target_column = N''blocked_process_xml'',
                @timestamp_column = N''event_time'',
                @log_to_table = 1,
                @log_database_name = N''PerformanceMonitor'',
                @log_schema_name = N''collect'',
                @log_table_name_prefix = N''blocking'',
                @log_retention_days = @log_retention_days,
                @max_blocking_events = @max_events_to_process,
                @start_date = @start_date,
                @end_date = @end_date,
                @debug = @debug;';

            EXECUTE sys.sp_executesql
                @sql,
                N'@log_retention_days integer, @max_events_to_process integer, @start_date datetime2(7), @end_date datetime2(7), @debug bit',
                @log_retention_days = @log_retention_days,
                @max_events_to_process = @max_events_to_process,
                @start_date = @start_date_local,
                @end_date = @end_date_local,
                @debug = @debug;

            /*
            Verify sp_HumanEventsBlockViewer produced parsed results before marking rows as processed
            If no results were inserted, leave rows unprocessed so they are retried next run
            Parsed results use local time (sp_HumanEventsBlockViewer converts UTC to local)
            */
            SELECT
                @rows_parsed = COUNT_BIG(*)
            FROM collect.blocking_BlockedProcessReport AS b
            WHERE b.event_time >= @start_date_local
            AND   b.event_time <= @end_date_local
            OPTION(RECOMPILE);

            IF @rows_parsed > 0
            BEGIN
                /*
                Mark raw XML rows as processed
                Only mark the rows in the date range we just processed
                */
                UPDATE bx
                SET    bx.is_processed = 1
                FROM collect.blocked_process_xml AS bx
                WHERE bx.is_processed = 0
                AND   (@start_date IS NULL OR bx.event_time >= @start_date)
                AND   (@end_date IS NULL OR bx.event_time <= @end_date);

                SELECT
                    @rows_marked = ROWCOUNT_BIG();

                IF @debug = 1
                BEGIN
                    RAISERROR(N'Marked %I64d raw XML rows as processed (%I64d parsed blocking events)', 0, 1, @rows_marked, @rows_parsed) WITH NOWAIT;
                END;
            END;
            ELSE
            BEGIN
                IF @debug = 1
                BEGIN
                    RAISERROR(N'sp_HumanEventsBlockViewer produced 0 parsed results for %d XML events - rows left unprocessed for retry', 0, 1, @rows_available) WITH NOWAIT;
                END;
            END;
        END;

        /*
        Log processing result
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
            N'process_blocked_process_xml',
            CASE WHEN @rows_available = 0 THEN N'SUCCESS'
                 WHEN @rows_parsed > 0 THEN N'SUCCESS'
                 ELSE N'NO_RESULTS'
            END,
            @rows_available,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            CASE WHEN @rows_available > 0 AND @rows_parsed = 0
                 THEN N'sp_HumanEventsBlockViewer returned 0 parsed results for '
                      + CAST(@rows_available AS nvarchar(20))
                      + N' XML events - rows left unprocessed for retry'
                 ELSE NULL
            END
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Processed %d blocked process XML events (%I64d parsed results)', 0, 1, @rows_available, @rows_parsed) WITH NOWAIT;
        END;

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;

        SELECT
            @error_message = ERROR_MESSAGE(),
            @error_number = ERROR_NUMBER();

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
            N'process_blocked_process_xml',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error processing blocked process XML: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Blocked process XML processor created successfully';
PRINT 'This procedure calls sp_HumanEventsBlockViewer to parse raw XML into reviewable tables';
GO
