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
Data retention procedure for performance monitoring system
Automatically purges old data from ALL collection tables
Uses per-collector retention from config.collection_schedule when available,
falls back to @retention_days parameter for unmatched tables
DYNAMIC VERSION - automatically discovers tables in collect schema with time columns
*/

IF OBJECT_ID(N'config.data_retention', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE config.data_retention AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    config.data_retention
(
    @retention_days integer = NULL, /*NULL = use per-collector retention from config.collection_schedule (30-day fallback). 0 = TRUNCATE every collect.* table. N > 0 = override every table's cutoff to N days.*/
    @batch_size integer = 10000, /*Number of rows to delete per batch to avoid blocking*/
    @debug bit = 0 /*Print debugging information*/
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    /*
    NULL @retention_days means "respect per-collector schedule" with a 30-day fallback for unscheduled tables.
    Non-NULL means "use this value as the cutoff everywhere" — schedule overrides are skipped.
    */
    DECLARE
        @apply_schedule_override bit = CASE WHEN @retention_days IS NULL THEN 1 ELSE 0 END,
        @effective_retention_days integer = ISNULL(@retention_days, 30);

    DECLARE
        @retention_date datetime2(7) = DATEADD(DAY, -@effective_retention_days, SYSDATETIME()),
        @total_deleted bigint = 0,
        @start_time datetime2(7) = SYSDATETIME(),
        @table_name sysname,
        @time_column_name sysname,
        @sql nvarchar(max),
        @rows_deleted bigint,
        @table_count integer = 0,
        @message nvarchar(1000);

    BEGIN TRY
        /*
        Parameter validation
        */
        IF @retention_days IS NOT NULL AND @retention_days < 0
        BEGIN
            RAISERROR(N'@retention_days must be 0 or greater', 16, 1);
            RETURN;
        END;

        IF @batch_size < 1000 OR @batch_size > 100000
        BEGIN
            RAISERROR(N'@batch_size must be between 1000 and 100000', 16, 1);
            RETURN;
        END;

        /*
        Purge-all branch: TRUNCATE every collect.* table when @retention_days = 0.
        No FKs, schema-bound views, or indexed views reference collect.* so this is safe.
        Config tables (including config.collection_log) are intentionally left alone.
        */
        IF @retention_days = 0
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Starting purge-all: TRUNCATE every collect.* table', 0, 1) WITH NOWAIT;
            END;

            DECLARE
                @truncate_table_name sysname,
                @truncate_sql nvarchar(max),
                @truncate_table_count integer = 0,
                @truncate_error_count integer = 0,
                @truncate_rows_before bigint = 0,
                @truncate_cursor cursor;

            /*
            Snapshot total row count across collect.* before truncating so we can report
            how many rows the user actually wiped. TRUNCATE doesn't return a count.
            */
            SELECT
                @truncate_rows_before = ISNULL(SUM(p.rows), 0)
            FROM sys.tables AS t
            JOIN sys.schemas AS s
              ON s.schema_id = t.schema_id
            JOIN sys.partitions AS p
              ON p.object_id = t.object_id
              AND p.index_id IN (0, 1)
            WHERE s.name = N'collect'
            AND   t.is_ms_shipped = 0;

            SET @truncate_cursor = CURSOR LOCAL FAST_FORWARD FOR
                SELECT
                    t.name
                FROM sys.tables AS t
                JOIN sys.schemas AS s
                  ON s.schema_id = t.schema_id
                WHERE s.name = N'collect'
                AND   t.is_ms_shipped = 0
                ORDER BY
                    t.name;

            OPEN @truncate_cursor;
            FETCH NEXT FROM @truncate_cursor INTO @truncate_table_name;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                BEGIN TRY
                    SET @truncate_sql = N'TRUNCATE TABLE collect.' + QUOTENAME(@truncate_table_name) + N';';

                    IF @debug = 1
                    BEGIN
                        RAISERROR(N'  %s', 0, 1, @truncate_sql) WITH NOWAIT;
                    END;

                    EXECUTE sys.sp_executesql @truncate_sql;
                    SET @truncate_table_count = @truncate_table_count + 1;
                END TRY
                BEGIN CATCH
                    SET @truncate_error_count = @truncate_error_count + 1;
                    SET @message = N'TRUNCATE failed for collect.' + QUOTENAME(@truncate_table_name) + N': ' + ERROR_MESSAGE();

                    IF @debug = 1
                    BEGIN
                        RAISERROR(@message, 0, 1) WITH NOWAIT;
                    END;

                    INSERT INTO
                        config.collection_log
                    (
                        collector_name,
                        collection_status,
                        error_message
                    )
                    VALUES
                    (
                        N'data_retention',
                        N'ERROR',
                        @message
                    );
                END CATCH;

                FETCH NEXT FROM @truncate_cursor INTO @truncate_table_name;
            END;

            CLOSE @truncate_cursor;

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
                N'data_retention',
                CASE WHEN @truncate_error_count = 0 THEN N'SUCCESS' ELSE N'WARNING' END,
                /*rows_collected is INT; clamp the bigint snapshot to int range*/
                CONVERT(integer, CASE WHEN @truncate_rows_before > 2147483647 THEN 2147483647 ELSE @truncate_rows_before END),
                DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
                N'TRUNCATE all: ' + CONVERT(nvarchar(10), @truncate_table_count) + N' tables truncated, '
                + CONVERT(nvarchar(20), @truncate_rows_before) + N' rows wiped'
                + CASE WHEN @truncate_error_count > 0
                       THEN N', ' + CONVERT(nvarchar(10), @truncate_error_count) + N' errors'
                       ELSE N''
                  END
            );

            IF @debug = 1
            BEGIN
                RAISERROR(N'Purge-all completed: %d tables truncated, %I64d rows wiped, %d errors', 0, 1,
                    @truncate_table_count, @truncate_rows_before, @truncate_error_count) WITH NOWAIT;
            END;

            RETURN;
        END;

        IF @debug = 1
        BEGIN
            DECLARE @retention_date_string nvarchar(30) = CONVERT(nvarchar(30), @retention_date, 120);
            RAISERROR(N'Starting data retention: keeping data newer than %s', 0, 1, @retention_date_string) WITH NOWAIT;
        END;

        /*
        Purge processed XE staging rows early.
        After parsers set is_processed = 1 the raw XML is never read again.
        Keep a 1-day grace period for re-parsing failures.
        */
        DECLARE
            @staging_deleted bigint = 0;

        IF OBJECT_ID(N'collect.deadlock_xml', N'U') IS NOT NULL
        AND EXISTS
        (
            SELECT
                1/0
            FROM sys.columns AS c
            WHERE c.object_id = OBJECT_ID(N'collect.deadlock_xml')
            AND   c.name = N'is_processed'
        )
        BEGIN
            DELETE FROM collect.deadlock_xml
            WHERE is_processed = 1
            AND   collection_time < DATEADD(DAY, -1, SYSDATETIME());

            SET @staging_deleted += ROWCOUNT_BIG();
        END;

        IF OBJECT_ID(N'collect.blocked_process_xml', N'U') IS NOT NULL
        AND EXISTS
        (
            SELECT
                1/0
            FROM sys.columns AS c
            WHERE c.object_id = OBJECT_ID(N'collect.blocked_process_xml')
            AND   c.name = N'is_processed'
        )
        BEGIN
            DELETE FROM collect.blocked_process_xml
            WHERE is_processed = 1
            AND   collection_time < DATEADD(DAY, -1, SYSDATETIME());

            SET @staging_deleted += ROWCOUNT_BIG();
        END;

        IF @debug = 1 AND @staging_deleted > 0
        BEGIN
            RAISERROR(N'Purged %I64d processed XE staging rows (older than 1 day)', 0, 1, @staging_deleted) WITH NOWAIT;
        END;

        SET @total_deleted += @staging_deleted;

        /*
        Create temp table to hold list of tables to clean
        */
        CREATE TABLE
            #tables_to_clean
        (
            schema_name sysname NOT NULL,
            table_name sysname NOT NULL,
            time_column_name sysname NOT NULL,
            retention_date datetime2(7) NOT NULL
        );

        /*
        Find all collect schema tables with collection_time column
        */
        INSERT INTO
            #tables_to_clean
        (
            schema_name,
            table_name,
            time_column_name,
            retention_date
        )
        SELECT
            schema_name = s.name,
            table_name = t.name,
            time_column_name = N'collection_time',
            retention_date = @retention_date
        FROM sys.tables AS t
        JOIN sys.schemas AS s
          ON s.schema_id = t.schema_id
        WHERE s.name = N'collect'
        AND   t.is_ms_shipped = 0
        AND   EXISTS
        (
            SELECT
                1/0
            FROM sys.columns AS c
            WHERE c.object_id = t.object_id
            AND   c.name = N'collection_time'
        );

        /*
        Find PressureDetector tables with CheckDate column
        */
        INSERT INTO
            #tables_to_clean
        (
            schema_name,
            table_name,
            time_column_name,
            retention_date
        )
        SELECT
            schema_name = s.name,
            table_name = t.name,
            time_column_name = N'CheckDate',
            retention_date = @retention_date
        FROM sys.tables AS t
        JOIN sys.schemas AS s
          ON s.schema_id = t.schema_id
        WHERE s.name = N'collect'
        AND   t.is_ms_shipped = 0
        AND   t.name LIKE N'%PressureDetector%'
        AND   EXISTS
        (
            SELECT
                1/0
            FROM sys.columns AS c
            WHERE c.object_id = t.object_id
            AND   c.name = N'CheckDate'
        )
        AND   NOT EXISTS
        (
            SELECT
                1/0
            FROM #tables_to_clean AS ttc
            WHERE ttc.table_name = t.name
        );

        /*
        Find HealthParser tables with event_time column
        */
        INSERT INTO
            #tables_to_clean
        (
            schema_name,
            table_name,
            time_column_name,
            retention_date
        )
        SELECT
            schema_name = s.name,
            table_name = t.name,
            time_column_name = N'event_time',
            retention_date = @retention_date
        FROM sys.tables AS t
        JOIN sys.schemas AS s
          ON s.schema_id = t.schema_id
        WHERE s.name = N'collect'
        AND   t.is_ms_shipped = 0
        AND   t.name LIKE N'%HealthParser%'
        AND   EXISTS
        (
            SELECT
                1/0
            FROM sys.columns AS c
            WHERE c.object_id = t.object_id
            AND   c.name = N'event_time'
        )
        AND   NOT EXISTS
        (
            SELECT
                1/0
            FROM #tables_to_clean AS ttc
            WHERE ttc.table_name = t.name
        );

        /*
        Override retention_date per-collector from config.collection_schedule.
        Skipped when @retention_days was supplied — caller wants a flat cutoff across every table.
        */
        IF @apply_schedule_override = 1
        BEGIN
            /*
            Direct match: strip _collector/_analyzer suffix and match table name prefix.
            */
            UPDATE ttc
            SET ttc.retention_date = DATEADD(DAY, -cs.retention_days, SYSDATETIME())
            FROM #tables_to_clean AS ttc
            JOIN config.collection_schedule AS cs
              ON ttc.table_name LIKE REPLACE(REPLACE(cs.collector_name, N'_collector', N''), N'_analyzer', N'') + N'%';

            /*
            Special mappings for tables whose names don't match their collector:
            - HealthParser_* tables -> system_health_collector
            - blocking_BlockedProcessReport -> process_blocked_process_xml
            - deadlocks (sp_BlitzLock output) -> process_deadlock_xml
            */
            UPDATE ttc
            SET ttc.retention_date = DATEADD(DAY, -cs.retention_days, SYSDATETIME())
            FROM #tables_to_clean AS ttc
            CROSS JOIN config.collection_schedule AS cs
            WHERE
            (
                ttc.table_name LIKE N'HealthParser%'
                AND cs.collector_name = N'system_health_collector'
            )
            OR
            (
                ttc.table_name = N'blocking_BlockedProcessReport'
                AND cs.collector_name = N'process_blocked_process_xml'
            )
            OR
            (
                ttc.table_name = N'deadlocks'
                AND cs.collector_name = N'process_deadlock_xml'
            );
        END;

        /*
        Special handling for config.collection_log - keep 2x longer than the effective retention
        */
        INSERT INTO
            #tables_to_clean
        (
            schema_name,
            table_name,
            time_column_name,
            retention_date
        )
        SELECT
            schema_name = N'config',
            table_name = N'collection_log',
            time_column_name = N'collection_time',
            retention_date = DATEADD(DAY, -(@effective_retention_days * 2), SYSDATETIME())
        WHERE EXISTS
        (
            SELECT
                1/0
            FROM sys.tables AS t
            JOIN sys.schemas AS s
              ON s.schema_id = t.schema_id
            WHERE s.name = N'config'
            AND   t.name = N'collection_log'
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'', 0, 1) WITH NOWAIT;
            RAISERROR(N'Tables to clean:', 0, 1) WITH NOWAIT;

            SELECT
                ttc.schema_name,
                ttc.table_name,
                ttc.time_column_name,
                retention_date = CONVERT(nvarchar(30), ttc.retention_date, 120)
            FROM #tables_to_clean AS ttc
            ORDER BY
                ttc.schema_name,
                ttc.table_name;

            RAISERROR(N'', 0, 1) WITH NOWAIT;
        END;

        /*
        Loop through tables and delete old data in batches
        */
        DECLARE
            @schema_name sysname,
            @full_table_name sysname,
            @table_retention_date datetime2(7),
            @table_cursor cursor;

        SET @table_cursor = CURSOR LOCAL FAST_FORWARD FOR
            SELECT
                ttc.schema_name,
                ttc.table_name,
                ttc.time_column_name,
                ttc.retention_date
            FROM #tables_to_clean AS ttc
            ORDER BY
                ttc.schema_name,
                ttc.table_name;

        OPEN @table_cursor;
        FETCH NEXT FROM @table_cursor INTO @schema_name, @table_name, @time_column_name, @table_retention_date;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @table_count = @table_count + 1;
            SET @full_table_name = QUOTENAME(@schema_name) + N'.' + QUOTENAME(@table_name);

            IF @debug = 1
            BEGIN
                SET @message = N'Cleaning ' + @full_table_name + N' (keeping data after ' + CONVERT(nvarchar(30), @table_retention_date, 120) + N')';
                RAISERROR(@message, 0, 1) WITH NOWAIT;
            END;

            /*
            Delete in batches to avoid blocking.
            Per-table TRY/CATCH so one table failing doesn't stop cleanup of the rest.
            */
            BEGIN TRY
                SET @rows_deleted = 1;

                WHILE @rows_deleted > 0
                BEGIN
                    SET @sql = N'
DELETE TOP (' + CONVERT(nvarchar(10), @batch_size) + N')
FROM ' + @full_table_name + N'
WHERE ' + QUOTENAME(@time_column_name) + N' < @retention_date_param;';

                    EXECUTE sys.sp_executesql
                        @sql,
                        N'@retention_date_param datetime2(7)',
                        @retention_date_param = @table_retention_date;

                    SET @rows_deleted = ROWCOUNT_BIG();
                    SET @total_deleted = @total_deleted + @rows_deleted;

                    IF @debug = 1 AND @rows_deleted > 0
                    BEGIN
                        SET @message = N'  Deleted ' + CONVERT(nvarchar(20), @rows_deleted) + N' rows from ' + @full_table_name;
                        RAISERROR(@message, 0, 1) WITH NOWAIT;
                    END;
                END;
            END TRY
            BEGIN CATCH
                SET @message = N'Error cleaning ' + @full_table_name + N': ' + ERROR_MESSAGE();

                IF @debug = 1
                BEGIN
                    RAISERROR(@message, 0, 1) WITH NOWAIT;
                END;

                INSERT INTO
                    config.collection_log
                (
                    collector_name,
                    collection_status,
                    error_message
                )
                VALUES
                (
                    N'data_retention',
                    N'ERROR',
                    @message
                );
            END CATCH;

            FETCH NEXT FROM @table_cursor INTO @schema_name, @table_name, @time_column_name, @table_retention_date;
        END;

        CLOSE @table_cursor;
        /*Cursor variables don't require DEALLOCATE*/

        /*
        Log retention operation
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
            N'data_retention',
            N'SUCCESS',
            @total_deleted,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            N'Cleaned ' + CONVERT(nvarchar(10), @table_count) + N' tables'
        );

        IF @debug = 1
        BEGIN
            DECLARE @duration_ms integer = DATEDIFF(MILLISECOND, @start_time, SYSDATETIME());
            RAISERROR(N'', 0, 1) WITH NOWAIT;
            RAISERROR(N'Data retention completed: %d total rows deleted from %d tables in %d ms', 0, 1,
                @total_deleted,
                @table_count,
                @duration_ms
            ) WITH NOWAIT;
        END;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;

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
            N'data_retention',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in data retention: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Data retention procedure created successfully (DYNAMIC VERSION)';
PRINT 'Use config.data_retention to automatically purge old monitoring data';
PRINT '';
PRINT '  @retention_days = NULL (default): respect per-collector retention in config.collection_schedule';
PRINT '                                    (30-day fallback for unscheduled tables)';
PRINT '  @retention_days = 0            : TRUNCATE every collect.* table';
PRINT '  @retention_days = N (N > 0)    : override every table''s cutoff to N days';
PRINT '';
PRINT 'Examples:';
PRINT '  -- Use per-collector retention (default)';
PRINT '  EXECUTE config.data_retention @debug = 1;';
PRINT '';
PRINT '  -- Override every table to 7-day retention';
PRINT '  EXECUTE config.data_retention @retention_days = 7;';
PRINT '';
PRINT '  -- Purge all collected data (TRUNCATE every collect.* table)';
PRINT '  EXECUTE config.data_retention @retention_days = 0;';
GO
