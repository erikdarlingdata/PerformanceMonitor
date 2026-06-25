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
Deadlock XML processor
Background task that polls for new deadlock XML and parses it via sp_BlitzLock
This is the second phase - the CPU-intensive parsing is separated from fast collection
*/

IF OBJECT_ID(N'collect.process_deadlock_xml', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.process_deadlock_xml AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.process_deadlock_xml
(
    @start_date datetime2(7) = NULL, /*Only process events after this date*/
    @end_date datetime2(7) = NULL, /*Only process events before this date*/
    @log_retention_days integer = 30, /*How long to keep parsed results*/
    @procedure_database sysname = NULL, /*Database where sp_BlitzLock is installed (NULL = search PerformanceMonitor then master)*/
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
        @error_message nvarchar(4000),
        @error_number integer,
        @blitzlock_database sysname = NULL,
        @sql nvarchar(max) = N'',
        @debug_msg nvarchar(500) = N'',
        @utc_offset_minutes integer = DATEDIFF(MINUTE, SYSUTCDATETIME(), SYSDATETIME()),
        @start_date_local datetime2(7) = NULL,
        @end_date_local datetime2(7) = NULL;

    BEGIN TRY
        BEGIN TRANSACTION;

        /*
        Locate sp_BlitzLock
        If user provided a database name, check there
        Otherwise search PerformanceMonitor first, then master
        */
        IF @procedure_database IS NOT NULL
        BEGIN
            SET @sql = N'
            IF OBJECT_ID(N''' + QUOTENAME(@procedure_database) + N'.dbo.sp_BlitzLock'', N''P'') IS NOT NULL
            BEGIN
                SELECT @blitzlock_database = N''' + REPLACE(@procedure_database, '''', '''''') + N''';
            END;';

            EXECUTE sys.sp_executesql
                @sql,
                N'@blitzlock_database sysname OUTPUT',
                @blitzlock_database = @blitzlock_database OUTPUT;

            IF @blitzlock_database IS NULL
            BEGIN
                SET @error_message = N'sp_BlitzLock not found in specified database ' + @procedure_database + N'. Please install it from https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit';
                RAISERROR(@error_message, 16, 1);
            END;
        END;
        ELSE
        BEGIN
            IF OBJECT_ID(N'dbo.sp_BlitzLock', N'P') IS NOT NULL
            BEGIN
                SET @blitzlock_database = N'PerformanceMonitor';
            END;
            ELSE IF OBJECT_ID(N'master.dbo.sp_BlitzLock', N'P') IS NOT NULL
            BEGIN
                SET @blitzlock_database = N'master';
            END;
            ELSE
            BEGIN
                SET @error_message = N'sp_BlitzLock is not installed in PerformanceMonitor or master. Please install it from https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit';
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
        FROM collect.deadlock_xml AS dx
        WHERE (@start_date IS NOT NULL OR dx.is_processed = 0)
        AND   (@start_date IS NULL OR dx.collection_time >= @start_date)
        AND   (@end_date IS NULL OR dx.collection_time <= @end_date)
        OPTION(RECOMPILE);

        IF @debug = 1
        BEGIN
            RAISERROR(N'Found %d deadlock XML events to process', 0, 1, @rows_available) WITH NOWAIT;
        END;

        IF @rows_available > 0
        BEGIN
            /*
            Derive date range from unprocessed rows when not explicitly provided
            This ensures we only parse new data and pass proper bounds to sp_BlitzLock
            */
            IF @start_date IS NULL AND @end_date IS NULL
            BEGIN
                SELECT
                    @start_date = MIN(dx.event_time),
                    @end_date = MAX(dx.event_time)
                FROM collect.deadlock_xml AS dx
                WHERE dx.is_processed = 0
                AND   dx.event_time IS NOT NULL
                OPTION(RECOMPILE);

                IF @debug = 1
                BEGIN
                    SET @debug_msg = N'Derived date range from unprocessed rows: ' + ISNULL(CONVERT(nvarchar(30), @start_date, 121), N'NULL') + N' to ' + ISNULL(CONVERT(nvarchar(30), @end_date, 121), N'NULL');
                    RAISERROR(@debug_msg, 0, 1) WITH NOWAIT;
                END;
            END;

            /*
            Convert UTC dates to local time for sp_BlitzLock and comparison
            with collect.deadlocks.event_date (which sp_BlitzLock stores in local time).
            event_time in collect.deadlock_xml stores UTC (from XE timestamps).
            sp_BlitzLock converts @StartDate/@EndDate from local time to UTC internally.
            */
            SELECT
                @start_date_local = DATEADD(MINUTE, @utc_offset_minutes, @start_date),
                /* +1s on the PARSER's local upper bound (not on @end_date itself) so sp_BlitzLock
                   includes events at exactly @end_date, while the mark below uses the un-padded UTC
                   @end_date — so a deadlock inserted concurrently in that extra second is left for the
                   next run rather than marked unparsed. Mirrors process_blocked_process_xml. */
                @end_date_local = DATEADD(SECOND, 1, DATEADD(MINUTE, @utc_offset_minutes, @end_date));

            IF @debug = 1
            BEGIN
                SET @debug_msg = N'UTC offset: ' + CAST(@utc_offset_minutes AS nvarchar(10)) + N' minutes. Local dates: ' + ISNULL(CONVERT(nvarchar(30), @start_date_local, 121), N'NULL') + N' to ' + ISNULL(CONVERT(nvarchar(30), @end_date_local, 121), N'NULL');
                RAISERROR(@debug_msg, 0, 1) WITH NOWAIT;
            END;

            /*
            Delete existing parsed deadlocks for the time range to prevent duplicates
            sp_BlitzLock will re-insert fresh parsed data
            Uses local-time dates because sp_BlitzLock stores event_date in local time
            */
            IF @start_date IS NOT NULL AND @end_date IS NOT NULL
            BEGIN
                DELETE d
                FROM collect.deadlocks AS d
                WHERE d.event_date >= @start_date_local
                AND   d.event_date <= @end_date_local;

                SELECT
                    @rows_deleted = ROWCOUNT_BIG();

                IF @debug = 1
                BEGIN
                    RAISERROR(N'Deleted %I64d existing parsed deadlocks for time range', 0, 1, @rows_deleted) WITH NOWAIT;
                END;
            END;

            /*
            Call sp_BlitzLock to parse the deadlock XML
            Point it at our collect.deadlock_xml table to read raw XML
            It will write parsed results to collect.deadlocks table
            Build dynamic SQL to call from correct database
            */
            SET @sql = N'
            EXECUTE ' + QUOTENAME(@blitzlock_database) + N'.dbo.sp_BlitzLock
                @TargetDatabaseName = N''PerformanceMonitor'',
                @TargetSchemaName = N''collect'',
                @TargetTableName = N''deadlock_xml'',
                @TargetColumnName = N''deadlock_xml'',
                @TargetTimestampColumnName = N''event_time'',
                @StartDate = @start_date_local,
                @EndDate = @end_date_local,
                @OutputDatabaseName = N''PerformanceMonitor'',
                @OutputSchemaName = N''collect'',
                @OutputTableName = N''deadlocks'',
                @Debug = @debug;';

            EXECUTE sys.sp_executesql
                @sql,
                N'@start_date_local datetime2(7), @end_date_local datetime2(7), @debug bit',
                @start_date_local = @start_date_local,
                @end_date_local = @end_date_local,
                @debug = @debug;

            /*
            If sp_BlitzLock failed internally it may have doomed our transaction
            Check XACT_STATE and surface the real error before it gets swallowed
            */
            IF XACT_STATE() = -1
            BEGIN
                ROLLBACK TRANSACTION;
                RAISERROR(N'sp_BlitzLock failed and doomed the transaction - check sp_BlitzLock version and compatibility', 16, 1);
                RETURN;
            END;

            /*
            Verify sp_BlitzLock produced parsed results before marking rows as processed
            If no results were inserted, leave rows unprocessed so they are retried next run
            Uses local-time dates because sp_BlitzLock stores event_date in local time
            */
            SELECT
                @rows_parsed = COUNT_BIG(*)
            FROM collect.deadlocks AS d
            WHERE d.event_date >= @start_date_local
            AND   d.event_date <= @end_date_local
            OPTION(RECOMPILE);

            /*
            Mark the raw XML rows we handed to sp_BlitzLock as processed -
            UNCONDITIONALLY after a clean parse run, not only when
            @rows_parsed > 0. sp_BlitzLock legitimately returns zero rows for
            deadlock graphs it cannot parse (malformed/partial graphs, or
            non-deadlock events captured by the session). Gating the mark on
            @rows_parsed > 0 left those unprocessed forever - the processor
            re-ran sp_BlitzLock over the same dead events every cycle and
            re-logged NO_RESULTS indefinitely. Genuine failures never reach
            here: the XACT_STATE() = -1 check and the CATCH block both roll back
            without marking, so a real parse failure still retries next run. Raw
            XML is retained (is_processed = 1, not deleted); data-retention
            handles cleanup. The +1s pad on @end_date_local (the parser's local bound,
            set above) guarantees sp_BlitzLock sees every event up to and including
            @end_date, while this mark uses the un-padded UTC @end_date so a row inserted
            concurrently after @end_date is left for the next run. event_time is UTC,
            matching @start_date / @end_date.
            */
            IF @rows_parsed = 0 AND @debug = 1
            BEGIN
                RAISERROR(N'sp_BlitzLock produced 0 parsed results for %d XML event(s) - no parseable deadlock graphs; events still marked processed', 0, 1, @rows_available) WITH NOWAIT;
            END;

            UPDATE dx
            SET    dx.is_processed = 1
            FROM collect.deadlock_xml AS dx
            WHERE dx.is_processed = 0
            AND   (@start_date IS NULL OR dx.event_time >= @start_date)
            AND   (@end_date IS NULL OR dx.event_time <= @end_date);

            SELECT
                @rows_marked = ROWCOUNT_BIG();

            IF @debug = 1
            BEGIN
                RAISERROR(N'Marked %I64d raw XML rows as processed (%I64d parsed deadlocks)', 0, 1, @rows_marked, @rows_parsed) WITH NOWAIT;
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
            N'process_deadlock_xml',
            /*
            A clean parse run is SUCCESS even when sp_BlitzLock produced 0 parsed
            deadlocks: the events were processed and marked (above), they simply held
            no reconstructable deadlock graph (un-parseable-by-design). Genuine failures
            take the CATCH path and log ERROR. This ends the perpetual NO_RESULTS this
            collector used to emit and mirrors process_blocked_process_xml exactly
            (previously this proc still logged NO_RESULTS + "left unprocessed for retry"
            after it had already marked the rows processed — a false signal).
            Tradeoff: a silent sp_BlitzLock failure that returns 0 rows with no error and
            a committable transaction is indistinguishable from "nothing to parse", so
            those events are marked processed — an accepted cost of ending the retry loop.
            */
            N'SUCCESS',
            @rows_available,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            NULL
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Processed %d deadlock XML events (%I64d parsed results)', 0, 1, @rows_available, @rows_parsed) WITH NOWAIT;
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
            N'process_deadlock_xml',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error processing deadlock XML: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Deadlock XML processor created successfully';
PRINT 'This procedure calls sp_BlitzLock to parse raw deadlock XML into reviewable tables';
GO
