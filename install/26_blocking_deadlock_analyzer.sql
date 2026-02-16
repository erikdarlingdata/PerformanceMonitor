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
Blocking and deadlock analyzer
Aggregates blocking and deadlock data by database from parsed events
Calculates deltas and detects significant increases
Logs critical issues when blocking or deadlocking increases significantly
*/

IF OBJECT_ID(N'collect.blocking_deadlock_analyzer', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.blocking_deadlock_analyzer AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.blocking_deadlock_analyzer
(
    @lookback_hours integer = 1, /*How far back to aggregate events*/
    @blocking_increase_threshold_percent integer = 50, /*Alert if blocking events increase by this percent*/
    @deadlock_increase_threshold_percent integer = 100, /*Alert if deadlocks increase by this percent*/
    @min_blocking_duration_ms integer = 5000, /*Only alert for blocking >5 seconds*/
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
        @cutoff_time datetime2(7) = DATEADD(HOUR, -@lookback_hours, SYSDATETIME()),
        @last_deadlock_collection datetime2(7) = NULL,
        @sql nvarchar(max) = N'',
        @error_message nvarchar(4000);

    /*
    Get the last successful collection time for deadlock counting
    This prevents recounting the same deadlocks every run
    Blocking uses rolling window (@cutoff_time) because blocking events
    are already unique per event_time and delta calculation works correctly
    */
    SELECT TOP (1)
        @last_deadlock_collection = bds.collection_time
    FROM collect.blocking_deadlock_stats AS bds
    WHERE bds.collection_time < @start_time
    ORDER BY
        bds.collection_time DESC;

    /*
    If no previous collection, use the lookback window
    */
    IF @last_deadlock_collection IS NULL
    BEGIN
        SET @last_deadlock_collection = @cutoff_time;
    END;

    BEGIN TRY
        BEGIN TRANSACTION;

        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.blocking_deadlock_stats', N'U') IS NULL
        BEGIN
            EXECUTE config.ensure_collection_table
                @table_name = N'blocking_deadlock_stats',
                @debug = @debug;

            IF OBJECT_ID(N'collect.blocking_deadlock_stats', N'U') IS NULL
            BEGIN
                RAISERROR(N'Table collect.blocking_deadlock_stats still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        IF @debug = 1
        BEGIN
            DECLARE
                @blocking_range nvarchar(200) = CONVERT(nvarchar(30), @cutoff_time, 121) + N' to now (rolling window)',
                @deadlock_range nvarchar(200) = CONVERT(nvarchar(30), @last_deadlock_collection, 121) + N' to ' + CONVERT(nvarchar(30), @start_time, 121);

            RAISERROR(N'Aggregating blocking and deadlock events from the last %d hour(s)', 0, 1, @lookback_hours) WITH NOWAIT;
            RAISERROR(N'Blocking: counting events from %s', 0, 1, @blocking_range) WITH NOWAIT;
            RAISERROR(N'Deadlock: counting events collected from %s (by collection_time)', 0, 1, @deadlock_range) WITH NOWAIT;
        END;

        /*
        Aggregate blocking events by database
        sp_HumanEventsBlockViewer creates collect.blocking_BlockedProcessReport table
        Only process if the table exists
        */
        IF OBJECT_ID(N'collect.blocking_BlockedProcessReport', N'U') IS NOT NULL
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Processing blocking events from blocking_BlockedProcessReport', 0, 1) WITH NOWAIT;
            END;

            /*
            Insert aggregated blocking metrics by database
            */
            INSERT INTO
                collect.blocking_deadlock_stats
            (
                database_name,
                blocking_event_count,
                total_blocking_duration_ms,
                max_blocking_duration_ms,
                avg_blocking_duration_ms,
                deadlock_count,
                total_deadlock_wait_time_ms,
                victim_count
            )
            SELECT
                database_name = ISNULL(bg.database_name, N'UNKNOWN'),
                blocking_event_count = COUNT_BIG(*),
                total_blocking_duration_ms = SUM(bg.wait_time_ms),
                max_blocking_duration_ms = MAX(bg.wait_time_ms),
                avg_blocking_duration_ms = AVG(CONVERT(decimal(19,2), bg.wait_time_ms)),
                deadlock_count = 0,
                total_deadlock_wait_time_ms = 0,
                victim_count = 0
            FROM collect.blocking_BlockedProcessReport AS bg
            WHERE bg.event_time >= @cutoff_time
            GROUP BY
                bg.database_name
            OPTION(RECOMPILE);

            SET @rows_collected = ROWCOUNT_BIG();

            IF @debug = 1
            BEGIN
                RAISERROR(N'Aggregated %d database(s) with blocking events', 0, 1, @rows_collected) WITH NOWAIT;
            END;
        END
        ELSE
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'blocking_BlockedProcessReport table does not exist - skipping blocking aggregation', 0, 1) WITH NOWAIT;
            END;
        END;

        /*
        Aggregate deadlock events by database
        sp_BlitzLock creates collect.deadlocks table
        Update existing rows or insert new ones
        */
        IF OBJECT_ID(N'collect.deadlocks', N'U') IS NOT NULL
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Processing deadlock events from deadlocks table', 0, 1) WITH NOWAIT;
            END;

            /*
            Aggregate deadlock data by database
            Update rows if database already exists from blocking aggregation
            Otherwise insert new rows
            */
            WITH
                deadlock_aggregates AS
            (
                SELECT
                    database_name = ISNULL(bl.database_name, N'UNKNOWN'),
                    deadlock_count = COUNT_BIG(DISTINCT LEFT(bl.deadlock_group, CHARINDEX(N',', bl.deadlock_group) - 1)),
                    total_deadlock_wait_time_ms = SUM(bl.wait_time),
                    victim_count = SUM(CASE WHEN bl.deadlock_group LIKE N'%- VICTIM' THEN 1 ELSE 0 END)
                FROM collect.deadlocks AS bl
                WHERE bl.collection_time >= @last_deadlock_collection
                AND   bl.collection_time < @start_time
                GROUP BY
                    bl.database_name
            )
            MERGE collect.blocking_deadlock_stats WITH (SERIALIZABLE) AS target
            USING deadlock_aggregates AS source
                ON  target.database_name = source.database_name
                AND target.collection_time >= @start_time
            WHEN MATCHED 
            THEN
                UPDATE SET
                    deadlock_count = source.deadlock_count,
                    total_deadlock_wait_time_ms = source.total_deadlock_wait_time_ms,
                    victim_count = source.victim_count
            WHEN NOT MATCHED 
            THEN
                INSERT
                (
                    database_name,
                    blocking_event_count,
                    total_blocking_duration_ms,
                    max_blocking_duration_ms,
                    avg_blocking_duration_ms,
                    deadlock_count,
                    total_deadlock_wait_time_ms,
                    victim_count
                )
                VALUES
                (
                    source.database_name,
                    0,
                    0,
                    0,
                    NULL,
                    source.deadlock_count,
                    source.total_deadlock_wait_time_ms,
                    source.victim_count
                )
            OPTION(RECOMPILE);

            IF @debug = 1
            BEGIN
                RAISERROR(N'Merged deadlock data for databases', 0, 1) WITH NOWAIT;
            END;
        END
        ELSE
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'deadlocks table does not exist - skipping deadlock aggregation', 0, 1) WITH NOWAIT;
            END;
        END;

        /*
        Always insert a baseline zero row when no blocking or deadlock events were found.
        This ensures continuous time-series data for dashboard charts.
        */
        IF NOT EXISTS
        (
            SELECT
                1
            FROM collect.blocking_deadlock_stats AS bds
            WHERE bds.collection_time >= @start_time
        )
        BEGIN
            INSERT INTO
                collect.blocking_deadlock_stats
            (
                database_name,
                blocking_event_count,
                total_blocking_duration_ms,
                max_blocking_duration_ms,
                avg_blocking_duration_ms,
                deadlock_count,
                total_deadlock_wait_time_ms,
                victim_count
            )
            VALUES
            (
                N'(none)',
                0,
                0,
                0,
                NULL,
                0,
                0,
                0
            );

            IF @debug = 1
            BEGIN
                RAISERROR(N'No blocking or deadlock events found - inserted baseline zero row', 0, 1) WITH NOWAIT;
            END;
        END;

        /*
        Calculate deltas for the newly inserted data
        */
        EXECUTE collect.calculate_deltas
            @table_name = N'blocking_deadlock_stats',
            @debug = @debug;

        /*
        Analyze for critical issues - significant increases in blocking or deadlocking
        */
        INSERT INTO
            config.critical_issues
        (
            severity,
            problem_area,
            source_collector,
            affected_database,
            message,
            investigate_query,
            threshold_value,
            threshold_limit
        )
        SELECT
            severity =
                CASE
                    WHEN bds.max_blocking_duration_ms_delta > 60000
                    OR   bds.blocking_event_count_delta > 50
                    OR   bds.deadlock_count_delta > 5
                    THEN N'CRITICAL'
                    WHEN bds.max_blocking_duration_ms_delta > 30000
                    OR   bds.blocking_event_count_delta > 20
                    OR   bds.deadlock_count_delta > 2
                    THEN N'WARNING'
                    ELSE N'INFO'
                END,
            problem_area =
                CASE
                    WHEN bds.blocking_event_count_delta > 0 AND bds.deadlock_count_delta > 0
                    THEN N'Blocking and Deadlocking'
                    WHEN bds.blocking_event_count_delta > 0
                    THEN N'Blocking'
                    ELSE N'Deadlocking'
                END,
            source_collector = N'blocking_deadlock_analyzer',
            affected_database = bds.database_name,
            message =
                CASE
                    WHEN bds.blocking_event_count_delta > 0 AND bds.deadlock_count_delta > 0
                    THEN
                        N'Database [' + bds.database_name + N'] experienced ' +
                        CONVERT(nvarchar(20), bds.blocking_event_count_delta) + N' blocking events (' +
                        CONVERT(nvarchar(20), bds.total_blocking_duration_ms_delta) + N'ms total, ' +
                        CONVERT(nvarchar(20), bds.max_blocking_duration_ms_delta) + N'ms max) and ' +
                        CONVERT(nvarchar(20), bds.deadlock_count_delta) + N' deadlock(s) (' +
                        CONVERT(nvarchar(20), bds.victim_count_delta) + N' victim(s)) in the last ' +
                        CONVERT(nvarchar(10), @lookback_hours) + N' hour(s)'
                    WHEN bds.blocking_event_count_delta > 0
                    THEN
                        N'Database [' + bds.database_name + N'] experienced ' +
                        CONVERT(nvarchar(20), bds.blocking_event_count_delta) + N' blocking events (' +
                        CONVERT(nvarchar(20), bds.total_blocking_duration_ms_delta) + N'ms total, ' +
                        CONVERT(nvarchar(20), bds.max_blocking_duration_ms_delta) + N'ms max duration) in the last ' +
                        CONVERT(nvarchar(10), @lookback_hours) + N' hour(s)'
                    ELSE
                        N'Database [' + bds.database_name + N'] experienced ' +
                        CONVERT(nvarchar(20), bds.deadlock_count_delta) + N' deadlock(s) with ' +
                        CONVERT(nvarchar(20), bds.victim_count_delta) + N' victim(s) in the last ' +
                        CONVERT(nvarchar(10), @lookback_hours) + N' hour(s)'
                END,
            investigate_query =
                CASE
                    WHEN bds.blocking_event_count_delta > 0 AND bds.deadlock_count_delta > 0
                    THEN
                        N'-- Blocking events:' + CHAR(13) + CHAR(10) +
                        N'SELECT * FROM collect.blocking_BlockedProcessReport WHERE database_name = N''' + bds.database_name + N''' AND event_time >= DATEADD(HOUR, -' + CONVERT(nvarchar(10), @lookback_hours) + N', SYSDATETIME()) ORDER BY blocked_time_ms DESC;' + CHAR(13) + CHAR(10) +
                        N'-- Deadlock events:' + CHAR(13) + CHAR(10) +
                        N'SELECT * FROM collect.deadlocks WHERE database_name = N''' + bds.database_name + N''' AND event_date >= DATEADD(HOUR, -' + CONVERT(nvarchar(10), @lookback_hours) + N', SYSDATETIME()) ORDER BY event_date DESC;'
                    WHEN bds.blocking_event_count_delta > 0
                    THEN
                        N'SELECT * FROM collect.blocking_BlockedProcessReport WHERE database_name = N''' + bds.database_name + N''' AND event_time >= DATEADD(HOUR, -' + CONVERT(nvarchar(10), @lookback_hours) + N', SYSDATETIME()) ORDER BY blocked_time_ms DESC;'
                    ELSE
                        N'SELECT * FROM collect.deadlocks WHERE database_name = N''' + bds.database_name + N''' AND event_date >= DATEADD(HOUR, -' + CONVERT(nvarchar(10), @lookback_hours) + N', SYSDATETIME()) ORDER BY event_date DESC;'
                END,
            threshold_value =
                CASE
                    WHEN bds.blocking_event_count_delta > 0
                    THEN bds.blocking_event_count_delta
                    ELSE bds.deadlock_count_delta
                END,
            threshold_limit =
                CASE
                    WHEN bds.blocking_event_count_delta > 0
                    THEN @blocking_increase_threshold_percent
                    ELSE @deadlock_increase_threshold_percent
                END
        FROM collect.blocking_deadlock_stats AS bds
        WHERE bds.collection_time >= @start_time
        AND   (
                  (bds.blocking_event_count_delta > 0 AND bds.max_blocking_duration_ms_delta >= @min_blocking_duration_ms)
                  OR bds.deadlock_count_delta > 0
              )
        OPTION(RECOMPILE);

        SET @rows_collected = ROWCOUNT_BIG();

        IF @debug = 1
        BEGIN
            RAISERROR(N'Logged %d critical issue(s) for blocking/deadlock events', 0, 1, @rows_collected) WITH NOWAIT;
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
            N'blocking_deadlock_analyzer',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;

        SET @error_message = ERROR_MESSAGE();

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
            N'blocking_deadlock_analyzer',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in blocking/deadlock analyzer: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Blocking and deadlock analyzer created successfully';
PRINT 'Aggregates blocking and deadlock events by database';
PRINT 'Calculates deltas and logs critical issues for significant increases';
GO
