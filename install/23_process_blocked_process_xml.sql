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
        @rows_typed bigint = 0,
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
                SELECT @blockviewer_database = N''' + REPLACE(@procedure_database, '''', '''''') + N''';
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
                /*
                Pad the upper bound by one second. sp_HumanEventsBlockViewer
                filters the source table with a half-open window
                (event_time < @end_date), so without the pad the newest
                event(s) - and an entire batch sharing a single timestamp
                (MIN = MAX, the common case because a blocked-process monitor
                loop emits every report at one instant) - fall outside
                [MIN, MAX) and are never parsed. The local/UTC basis itself
                round-trips correctly: this proc shifts UTC event_time to local
                and sp_HumanEventsBlockViewer shifts it back to UTC internally.
                */
                SELECT
                    @start_date_local = DATEADD(MINUTE, @utc_offset_minutes, @start_date),
                    @end_date_local = DATEADD(SECOND, 1, DATEADD(MINUTE, @utc_offset_minutes, @end_date));

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
            If sp_HumanEventsBlockViewer failed internally it may have doomed our transaction
            Check XACT_STATE and surface the real error before it gets swallowed
            */
            IF XACT_STATE() = -1
            BEGIN
                ROLLBACK TRANSACTION;
                RAISERROR(N'sp_HumanEventsBlockViewer failed and doomed the transaction - check procedure version and compatibility', 16, 1);
                RETURN;
            END;

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
                Populate blocker-side typed columns on the rows just parsed by
                sp_HumanEventsBlockViewer so the Dashboard analysis path can
                read structured columns instead of re-parsing the XML on every
                BLOCKING_CHAIN fact. Only activity='blocked' rows carry the
                full XML; activity='blocking' rows stay NULL on the new
                columns (they describe the blocker side via their own
                spid/status columns).

                XQuery uses the descendant axis (//blocked-process-report/...)
                because the stored XML is <event>-rooted with the report
                nested two levels deep at
                /event/data[@name="blocked_process"]/value/blocked-process-report.
                The descendant axis sidesteps the wrap and was empirically
                validated; a leading-slash (/blocked-process-report/...)
                returns NULL on every row.

                LTRIM/RTRIM matches the C# parser's .Trim() for spaces only
                (not CR/LF/TAB); the reconstructor keys on session pair, not
                SQL text, so the divergence is cosmetic.

                Runs BEFORE the is_processed=1 mark below so a crash here
                rolls back inside the surrounding transaction and the raw XML
                rows stay unmarked - the next run retries them.
                */
                UPDATE
                    b
                SET
                    b.blocking_spid =
                        b.blocked_process_report_xml.value
                        (
                            N'(//blocked-process-report/blocking-process/process/@spid)[1]',
                            N'integer'
                        ),
                    b.blocking_last_tran_started =
                        b.blocked_process_report_xml.value
                        (
                            N'(//blocked-process-report/blocking-process/process/@lasttranstarted)[1]',
                            N'datetime2(7)'
                        ),
                    b.blocking_status =
                        b.blocked_process_report_xml.value
                        (
                            N'(//blocked-process-report/blocking-process/process/@status)[1]',
                            N'nvarchar(10)'
                        ),
                    b.blocking_login_name =
                        b.blocked_process_report_xml.value
                        (
                            N'(//blocked-process-report/blocking-process/process/@loginname)[1]',
                            N'nvarchar(256)'
                        ),
                    b.blocking_host_name =
                        b.blocked_process_report_xml.value
                        (
                            N'(//blocked-process-report/blocking-process/process/@hostname)[1]',
                            N'nvarchar(256)'
                        ),
                    b.blocking_client_app =
                        b.blocked_process_report_xml.value
                        (
                            N'(//blocked-process-report/blocking-process/process/@clientapp)[1]',
                            N'nvarchar(256)'
                        ),
                    b.blocked_sql_text =
                        LTRIM(RTRIM(b.blocked_process_report_xml.value
                        (
                            N'(//blocked-process-report/blocked-process/process/inputbuf/text())[1]',
                            N'nvarchar(max)'
                        ))),
                    b.blocking_sql_text =
                        LTRIM(RTRIM(b.blocked_process_report_xml.value
                        (
                            N'(//blocked-process-report/blocking-process/process/inputbuf/text())[1]',
                            N'nvarchar(max)'
                        )))
                FROM collect.blocking_BlockedProcessReport AS b
                WHERE b.event_time >= @start_date_local
                AND   b.event_time <= @end_date_local
                AND   b.activity = 'blocked'
                AND   b.blocking_spid IS NULL
                AND   b.blocked_process_report_xml IS NOT NULL;

                SELECT
                    @rows_typed = ROWCOUNT_BIG();

                IF @debug = 1
                BEGIN
                    RAISERROR(N'Populated blocker-side typed columns for %I64d rows', 0, 1, @rows_typed) WITH NOWAIT;
                END;

                /*
                Defense-in-depth: if sp_HumanEventsBlockViewer wrote rows
                (@rows_parsed > 0) but the typed-column UPDATE populated zero
                (@rows_typed = 0), the XQuery is silently failing - most
                likely a future wire-format change in
                sp_HumanEventsBlockViewer or the upstream XE shape. Log this
                clearly so it surfaces in config.collection_log instead of
                only revealing itself when the analysis path returns garbage
                chains. The condition can't be a hard error because new-row
                UPDATEs can also legitimately populate 0 if every row had
                non-NULL blocking_spid already (re-processing same window),
                so we just record it.
                */
                IF @rows_parsed > 0 AND @rows_typed = 0
                BEGIN
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
                        N'TYPED_COLUMNS_EMPTY',
                        @rows_parsed,
                        DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
                        N'sp_HumanEventsBlockViewer wrote '
                        + CAST(@rows_parsed AS nvarchar(20))
                        + N' rows but XQuery extraction populated 0 blocker-side typed columns - '
                        + N'likely a wire-format change in blocked_process_report_xml; '
                        + N'check //blocked-process-report path against a sample row.'
                    );
                END;

            END;

            /*
            Mark the raw XML rows we handed to sp_HumanEventsBlockViewer as
            processed - UNCONDITIONALLY after a clean parse run, not only when
            @rows_parsed > 0. The viewer legitimately returns zero rows for
            events that carry no lock-blocking chain between distinct sessions:
            self-blocks, and non-lock GENERIC/NL waits such as a memory-grant
            RESOURCE_SEMAPHORE wait that tripped blocked_process_threshold.
            Those never parse, so gating the mark on @rows_parsed > 0 left them
            unprocessed forever - the processor re-ran the viewer over the same
            dead events every cycle and re-logged NO_RESULTS indefinitely.

            Safe because the upper bound was padded (+1s) above, so the viewer's
            half-open window actually covers every unprocessed event - we never
            mark a row the viewer did not get to see. Genuine failures never
            reach here either: the XACT_STATE() = -1 check and the CATCH block
            both roll back without marking, so a real parse failure still
            retries next run. Raw XML is retained (is_processed = 1, not
            deleted); data-retention handles cleanup. event_time is UTC,
            matching @start_date / @end_date.
            */
            IF @rows_parsed = 0 AND @debug = 1
            BEGIN
                RAISERROR(N'sp_HumanEventsBlockViewer produced 0 parsed results for %d XML event(s) - no lock-blocking chains (self-block / non-lock waits); events still marked processed', 0, 1, @rows_available) WITH NOWAIT;
            END;

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
            /*
            A clean parse run is SUCCESS even when it produced 0 blocking
            chains: the events were processed and marked, they simply carried
            no lock-blocking between distinct sessions (self-block / non-lock
            waits). Genuine failures take the CATCH path and log ERROR. This
            ends the perpetual NO_RESULTS this collector used to emit for
            un-parseable-by-design events.
            */
            N'SUCCESS',
            @rows_available,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            NULL
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
