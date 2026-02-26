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
    @retention_days integer = 30, /*Fallback retention for tables without a collection_schedule entry*/
    @batch_size integer = 10000, /*Number of rows to delete per batch to avoid blocking*/
    @debug bit = 0 /*Print debugging information*/
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE
        @retention_date datetime2(7) = DATEADD(DAY, -@retention_days, SYSDATETIME()),
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
        IF @retention_days < 1
        BEGIN
            RAISERROR(N'@retention_days must be at least 1', 16, 1);
            RETURN;
        END;

        IF @batch_size < 1000 OR @batch_size > 100000
        BEGIN
            RAISERROR(N'@batch_size must be between 1000 and 100000', 16, 1);
            RETURN;
        END;

        IF @debug = 1
        BEGIN
            DECLARE @retention_date_string nvarchar(30) = CONVERT(nvarchar(30), @retention_date, 120);
            RAISERROR(N'Starting data retention: keeping data newer than %s', 0, 1, @retention_date_string) WITH NOWAIT;
        END;

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

        /*
        Special handling for config.collection_log - keep 2x longer
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
            retention_date = DATEADD(DAY, -(@retention_days * 2), SYSDATETIME())
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
PRINT 'Uses per-collector retention from config.collection_schedule when available';
PRINT 'Falls back to @retention_days parameter for unmatched tables';
PRINT '';
PRINT 'Examples:';
PRINT '  -- Use per-collector retention (default)';
PRINT '  EXECUTE config.data_retention @debug = 1;';
PRINT '';
PRINT '  -- Override fallback to 90 days for tables without schedule entries';
PRINT '  EXECUTE config.data_retention @retention_days = 90;';
GO
