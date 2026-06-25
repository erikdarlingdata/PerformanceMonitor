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

/*******************************************************************************
Collector: index_object_stats_collector
Purpose: Captures per-table and per-index size, usage, and locking statistics
         for growth trending, unused-index detection, and contention analysis.
Collection Type: Point-in-time snapshot for sizes; cumulative counters for
         usage/locking (deltas derived in the read layer using
         sqlserver_start_time as the reset boundary).
Target Table: collect.index_object_stats
Frequency: Every 1440 minutes (daily) - object grain is high volume.
Dependencies: sys.dm_db_partition_stats, sys.dm_db_index_usage_stats,
         sys.dm_db_index_operational_stats, sys.indexes, sys.objects, sys.schemas
Notes: All three DMVs are database-scoped, so the full join executes inside each
       database's context via dynamic SQL. Azure SQL DB (engine edition 5)
       collects only the connected database (no cross-database enumeration).
       In-Memory OLTP (Hekaton) objects are not represented by these DMVs.
*******************************************************************************/

IF OBJECT_ID(N'collect.index_object_stats_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.index_object_stats_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.index_object_stats_collector
(
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
        @sqlserver_start_time datetime2(7),
        @error_message nvarchar(4000),
        @engine_edition integer =
            CONVERT(integer, SERVERPROPERTY(N'EngineEdition'));

    BEGIN TRY
        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.index_object_stats', N'U') IS NULL
        BEGIN
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
                @start_time,
                N'index_object_stats_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.index_object_stats does not exist, calling ensure procedure'
            );

            EXECUTE config.ensure_collection_table
                @table_name = N'index_object_stats',
                @debug = @debug;

            IF OBJECT_ID(N'collect.index_object_stats', N'U') IS NULL
            BEGIN
                RAISERROR(N'Table collect.index_object_stats still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Reset boundary for cumulative usage/locking counters
        */
        SELECT
            @sqlserver_start_time = osi.sqlserver_start_time
        FROM sys.dm_os_sys_info AS osi;

        /*
        Azure SQL DB: single database scope (no cross-database enumeration)
        */
        IF @engine_edition = 5
        BEGIN
            /*
            Stage each DMV with a single scan so the final join gets real cardinality.
            A single monolithic multi-DMV join can pick a bad plan on large databases;
            staging avoids that (the sp_IndexCleanup technique - see #1135).
            */
            SELECT
                dps.object_id,
                dps.index_id,
                partition_count = COUNT_BIG(*),
                reserved_pages = SUM(dps.reserved_page_count),
                used_pages = SUM(dps.used_page_count),
                in_row_pages = SUM(dps.in_row_data_page_count),
                lob_pages = SUM(dps.lob_used_page_count),
                row_overflow_pages = SUM(dps.row_overflow_used_page_count),
                total_rows = SUM(dps.row_count)
            INTO #sizes
            FROM sys.dm_db_partition_stats AS dps
            GROUP BY
                dps.object_id,
                dps.index_id
            OPTION(RECOMPILE);

            SELECT
                us.object_id,
                us.index_id,
                us.user_seeks,
                us.user_scans,
                us.user_lookups,
                us.user_updates,
                us.last_user_seek,
                us.last_user_scan,
                us.last_user_lookup,
                us.last_user_update
            INTO #usage
            FROM sys.dm_db_index_usage_stats AS us
            WHERE us.database_id = DB_ID()
            OPTION(RECOMPILE);

            SELECT
                ios.object_id,
                ios.index_id,
                leaf_insert_count = SUM(ios.leaf_insert_count),
                leaf_update_count = SUM(ios.leaf_update_count),
                leaf_delete_count = SUM(ios.leaf_delete_count),
                range_scan_count = SUM(ios.range_scan_count),
                singleton_lookup_count = SUM(ios.singleton_lookup_count),
                row_lock_count = SUM(ios.row_lock_count),
                row_lock_wait_count = SUM(ios.row_lock_wait_count),
                row_lock_wait_in_ms = SUM(ios.row_lock_wait_in_ms),
                page_lock_count = SUM(ios.page_lock_count),
                page_lock_wait_count = SUM(ios.page_lock_wait_count),
                page_lock_wait_in_ms = SUM(ios.page_lock_wait_in_ms),
                index_lock_promotion_attempt_count = SUM(ios.index_lock_promotion_attempt_count),
                index_lock_promotion_count = SUM(ios.index_lock_promotion_count),
                page_latch_wait_count = SUM(ios.page_latch_wait_count),
                page_latch_wait_in_ms = SUM(ios.page_latch_wait_in_ms),
                page_io_latch_wait_count = SUM(ios.page_io_latch_wait_count),
                page_io_latch_wait_in_ms = SUM(ios.page_io_latch_wait_in_ms)
            INTO #ops
            FROM sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL) AS ios
            GROUP BY
                ios.object_id,
                ios.index_id
            OPTION(RECOMPILE);

            INSERT INTO
                collect.index_object_stats
            (
                collection_time,
                sqlserver_start_time,
                database_name,
                database_id,
                schema_name,
                object_id,
                table_name,
                index_id,
                index_name,
                index_type_desc,
                is_unique,
                is_primary_key,
                is_filtered,
                partition_count,
                reserved_mb,
                used_mb,
                in_row_data_mb,
                lob_data_mb,
                row_overflow_mb,
                total_rows,
                user_seeks,
                user_scans,
                user_lookups,
                user_updates,
                last_user_seek,
                last_user_scan,
                last_user_lookup,
                last_user_update,
                leaf_insert_count,
                leaf_update_count,
                leaf_delete_count,
                range_scan_count,
                singleton_lookup_count,
                row_lock_count,
                row_lock_wait_count,
                row_lock_wait_in_ms,
                page_lock_count,
                page_lock_wait_count,
                page_lock_wait_in_ms,
                index_lock_promotion_attempt_count,
                index_lock_promotion_count,
                page_latch_wait_count,
                page_latch_wait_in_ms,
                page_io_latch_wait_count,
                page_io_latch_wait_in_ms
            )
            SELECT
                collection_time = @start_time,
                sqlserver_start_time = @sqlserver_start_time,
                database_name = DB_NAME(),
                database_id = DB_ID(),
                schema_name = s.name,
                object_id = o.object_id,
                table_name = o.name,
                index_id = i.index_id,
                index_name = i.name,
                index_type_desc = i.type_desc,
                is_unique = i.is_unique,
                is_primary_key = i.is_primary_key,
                is_filtered = i.has_filter,
                partition_count = ps.partition_count,
                reserved_mb = CONVERT(decimal(19,2), ps.reserved_pages * 8.0 / 1024.0),
                used_mb = CONVERT(decimal(19,2), ps.used_pages * 8.0 / 1024.0),
                in_row_data_mb = CONVERT(decimal(19,2), ps.in_row_pages * 8.0 / 1024.0),
                lob_data_mb = CONVERT(decimal(19,2), ps.lob_pages * 8.0 / 1024.0),
                row_overflow_mb = CONVERT(decimal(19,2), ps.row_overflow_pages * 8.0 / 1024.0),
                total_rows = ps.total_rows,
                user_seeks = us.user_seeks,
                user_scans = us.user_scans,
                user_lookups = us.user_lookups,
                user_updates = us.user_updates,
                last_user_seek = us.last_user_seek,
                last_user_scan = us.last_user_scan,
                last_user_lookup = us.last_user_lookup,
                last_user_update = us.last_user_update,
                leaf_insert_count = os.leaf_insert_count,
                leaf_update_count = os.leaf_update_count,
                leaf_delete_count = os.leaf_delete_count,
                range_scan_count = os.range_scan_count,
                singleton_lookup_count = os.singleton_lookup_count,
                row_lock_count = os.row_lock_count,
                row_lock_wait_count = os.row_lock_wait_count,
                row_lock_wait_in_ms = os.row_lock_wait_in_ms,
                page_lock_count = os.page_lock_count,
                page_lock_wait_count = os.page_lock_wait_count,
                page_lock_wait_in_ms = os.page_lock_wait_in_ms,
                index_lock_promotion_attempt_count = os.index_lock_promotion_attempt_count,
                index_lock_promotion_count = os.index_lock_promotion_count,
                page_latch_wait_count = os.page_latch_wait_count,
                page_latch_wait_in_ms = os.page_latch_wait_in_ms,
                page_io_latch_wait_count = os.page_io_latch_wait_count,
                page_io_latch_wait_in_ms = os.page_io_latch_wait_in_ms
            FROM sys.indexes AS i
            JOIN sys.objects AS o
              ON o.object_id = i.object_id
            JOIN sys.schemas AS s
              ON s.schema_id = o.schema_id
            LEFT JOIN #sizes AS ps
              ON  ps.object_id = i.object_id
              AND ps.index_id = i.index_id
            LEFT JOIN #usage AS us
              ON  us.object_id = i.object_id
              AND us.index_id = i.index_id
            LEFT JOIN #ops AS os
              ON  os.object_id = i.object_id
              AND os.index_id = i.index_id
            WHERE o.is_ms_shipped = 0
            AND   o.type IN (N'U', N'V')
            OPTION(RECOMPILE);

            SET @rows_collected = ROWCOUNT_BIG();

            DROP TABLE #sizes, #usage, #ops;
        END;
        ELSE
        BEGIN
            /*
            On-prem / Azure MI / AWS RDS: cursor over all online databases.
            All three DMVs are database-scoped, so the entire join runs inside
            each database's context and inserts into the fully-qualified target.
            */
            DECLARE
                @db_name sysname,
                @db_id integer,
                @sql nvarchar(max),
                @exec_sql nvarchar(max);

            DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
                SELECT
                    d.name,
                    d.database_id
                FROM sys.databases AS d
                WHERE d.state = 0 /*ONLINE only - skip RESTORING (mirroring/AG secondary)*/
                AND   d.database_id > 0
                AND   HAS_DBACCESS(d.name) = 1
                AND   NOT EXISTS
                (
                    SELECT
                        1/0
                    FROM config.collector_database_exclusions AS e
                    WHERE e.database_name = d.name
                )
                ORDER BY
                    d.database_id;

            OPEN db_cursor;
            FETCH NEXT FROM db_cursor INTO @db_name, @db_id;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                BEGIN TRY
                    SET @sql = N'
                    SET NOCOUNT ON;
                    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                    /* Stage each DMV with a single scan (real cardinality -> sane plan on
                       large databases; the sp_IndexCleanup technique - see #1135). These
                       #temps live only inside this per-database batch. */
                    SELECT
                        dps.object_id,
                        dps.index_id,
                        partition_count = COUNT_BIG(*),
                        reserved_pages = SUM(dps.reserved_page_count),
                        used_pages = SUM(dps.used_page_count),
                        in_row_pages = SUM(dps.in_row_data_page_count),
                        lob_pages = SUM(dps.lob_used_page_count),
                        row_overflow_pages = SUM(dps.row_overflow_used_page_count),
                        total_rows = SUM(dps.row_count)
                    INTO #sizes
                    FROM sys.dm_db_partition_stats AS dps
                    GROUP BY
                        dps.object_id,
                        dps.index_id
                    OPTION(RECOMPILE);

                    SELECT
                        us.object_id,
                        us.index_id,
                        us.user_seeks,
                        us.user_scans,
                        us.user_lookups,
                        us.user_updates,
                        us.last_user_seek,
                        us.last_user_scan,
                        us.last_user_lookup,
                        us.last_user_update
                    INTO #usage
                    FROM sys.dm_db_index_usage_stats AS us
                    WHERE us.database_id = DB_ID()
                    OPTION(RECOMPILE);

                    SELECT
                        ios.object_id,
                        ios.index_id,
                        leaf_insert_count = SUM(ios.leaf_insert_count),
                        leaf_update_count = SUM(ios.leaf_update_count),
                        leaf_delete_count = SUM(ios.leaf_delete_count),
                        range_scan_count = SUM(ios.range_scan_count),
                        singleton_lookup_count = SUM(ios.singleton_lookup_count),
                        row_lock_count = SUM(ios.row_lock_count),
                        row_lock_wait_count = SUM(ios.row_lock_wait_count),
                        row_lock_wait_in_ms = SUM(ios.row_lock_wait_in_ms),
                        page_lock_count = SUM(ios.page_lock_count),
                        page_lock_wait_count = SUM(ios.page_lock_wait_count),
                        page_lock_wait_in_ms = SUM(ios.page_lock_wait_in_ms),
                        index_lock_promotion_attempt_count = SUM(ios.index_lock_promotion_attempt_count),
                        index_lock_promotion_count = SUM(ios.index_lock_promotion_count),
                        page_latch_wait_count = SUM(ios.page_latch_wait_count),
                        page_latch_wait_in_ms = SUM(ios.page_latch_wait_in_ms),
                        page_io_latch_wait_count = SUM(ios.page_io_latch_wait_count),
                        page_io_latch_wait_in_ms = SUM(ios.page_io_latch_wait_in_ms)
                    INTO #ops
                    FROM sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL) AS ios
                    GROUP BY
                        ios.object_id,
                        ios.index_id
                    OPTION(RECOMPILE);

                    INSERT INTO
                        PerformanceMonitor.collect.index_object_stats
                    (
                        collection_time,
                        sqlserver_start_time,
                        database_name,
                        database_id,
                        schema_name,
                        object_id,
                        table_name,
                        index_id,
                        index_name,
                        index_type_desc,
                        is_unique,
                        is_primary_key,
                        is_filtered,
                        partition_count,
                        reserved_mb,
                        used_mb,
                        in_row_data_mb,
                        lob_data_mb,
                        row_overflow_mb,
                        total_rows,
                        user_seeks,
                        user_scans,
                        user_lookups,
                        user_updates,
                        last_user_seek,
                        last_user_scan,
                        last_user_lookup,
                        last_user_update,
                        leaf_insert_count,
                        leaf_update_count,
                        leaf_delete_count,
                        range_scan_count,
                        singleton_lookup_count,
                        row_lock_count,
                        row_lock_wait_count,
                        row_lock_wait_in_ms,
                        page_lock_count,
                        page_lock_wait_count,
                        page_lock_wait_in_ms,
                        index_lock_promotion_attempt_count,
                        index_lock_promotion_count,
                        page_latch_wait_count,
                        page_latch_wait_in_ms,
                        page_io_latch_wait_count,
                        page_io_latch_wait_in_ms
                    )
                    SELECT
                        collection_time = @start_time,
                        sqlserver_start_time = @sqlserver_start_time,
                        database_name = DB_NAME(),
                        database_id = DB_ID(),
                        schema_name = s.name,
                        object_id = o.object_id,
                        table_name = o.name,
                        index_id = i.index_id,
                        index_name = i.name,
                        index_type_desc = i.type_desc,
                        is_unique = i.is_unique,
                        is_primary_key = i.is_primary_key,
                        is_filtered = i.has_filter,
                        partition_count = ps.partition_count,
                        reserved_mb = CONVERT(decimal(19,2), ps.reserved_pages * 8.0 / 1024.0),
                        used_mb = CONVERT(decimal(19,2), ps.used_pages * 8.0 / 1024.0),
                        in_row_data_mb = CONVERT(decimal(19,2), ps.in_row_pages * 8.0 / 1024.0),
                        lob_data_mb = CONVERT(decimal(19,2), ps.lob_pages * 8.0 / 1024.0),
                        row_overflow_mb = CONVERT(decimal(19,2), ps.row_overflow_pages * 8.0 / 1024.0),
                        total_rows = ps.total_rows,
                        user_seeks = us.user_seeks,
                        user_scans = us.user_scans,
                        user_lookups = us.user_lookups,
                        user_updates = us.user_updates,
                        last_user_seek = us.last_user_seek,
                        last_user_scan = us.last_user_scan,
                        last_user_lookup = us.last_user_lookup,
                        last_user_update = us.last_user_update,
                        leaf_insert_count = os.leaf_insert_count,
                        leaf_update_count = os.leaf_update_count,
                        leaf_delete_count = os.leaf_delete_count,
                        range_scan_count = os.range_scan_count,
                        singleton_lookup_count = os.singleton_lookup_count,
                        row_lock_count = os.row_lock_count,
                        row_lock_wait_count = os.row_lock_wait_count,
                        row_lock_wait_in_ms = os.row_lock_wait_in_ms,
                        page_lock_count = os.page_lock_count,
                        page_lock_wait_count = os.page_lock_wait_count,
                        page_lock_wait_in_ms = os.page_lock_wait_in_ms,
                        index_lock_promotion_attempt_count = os.index_lock_promotion_attempt_count,
                        index_lock_promotion_count = os.index_lock_promotion_count,
                        page_latch_wait_count = os.page_latch_wait_count,
                        page_latch_wait_in_ms = os.page_latch_wait_in_ms,
                        page_io_latch_wait_count = os.page_io_latch_wait_count,
                        page_io_latch_wait_in_ms = os.page_io_latch_wait_in_ms
                    FROM sys.indexes AS i
                    JOIN sys.objects AS o
                      ON o.object_id = i.object_id
                    JOIN sys.schemas AS s
                      ON s.schema_id = o.schema_id
                    LEFT JOIN #sizes AS ps
                      ON  ps.object_id = i.object_id
                      AND ps.index_id = i.index_id
                    LEFT JOIN #usage AS us
                      ON  us.object_id = i.object_id
                      AND us.index_id = i.index_id
                    LEFT JOIN #ops AS os
                      ON  os.object_id = i.object_id
                      AND os.index_id = i.index_id
                    WHERE o.is_ms_shipped = 0
                    AND   o.type IN (N''U'', N''V'')
                    OPTION(RECOMPILE);';

                    SET @exec_sql = QUOTENAME(@db_name) + N'.sys.sp_executesql';

                    EXECUTE @exec_sql
                        @sql,
                        N'@start_time datetime2(7), @sqlserver_start_time datetime2(7)',
                        @start_time = @start_time,
                        @sqlserver_start_time = @sqlserver_start_time;

                    SET @rows_collected = @rows_collected + ROWCOUNT_BIG();
                END TRY
                BEGIN CATCH
                    /*
                    Log per-database errors but continue with remaining databases
                    */
                    IF @debug = 1
                    BEGIN
                        DECLARE @db_error_message nvarchar(4000) = ERROR_MESSAGE();
                        RAISERROR(N'Error collecting index/object stats for database [%s]: %s', 0, 1, @db_name, @db_error_message) WITH NOWAIT;
                    END;
                END CATCH;

                FETCH NEXT FROM db_cursor INTO @db_name, @db_id;
            END;

            CLOSE db_cursor;
            DEALLOCATE db_cursor;
        END;

        /*
        Debug output
        */
        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %I64d index/object stat rows', 0, 1, @rows_collected) WITH NOWAIT;

            SELECT TOP (20)
                ios.database_name,
                ios.schema_name,
                ios.table_name,
                ios.index_name,
                ios.index_type_desc,
                ios.reserved_mb,
                ios.total_rows,
                ios.total_reads,
                ios.user_updates,
                ios.row_lock_wait_in_ms,
                ios.index_lock_promotion_count
            FROM collect.index_object_stats AS ios
            WHERE ios.collection_time = @start_time
            ORDER BY
                ios.reserved_mb DESC;
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
            N'index_object_stats_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;

        /*
        Clean up cursor if open
        */
        IF CURSOR_STATUS(N'local', N'db_cursor') >= 0
        BEGIN
            CLOSE db_cursor;
            DEALLOCATE db_cursor;
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
            N'index_object_stats_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in index/object stats collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Index/object stats collector created successfully';
PRINT 'Captures per-table/per-index size, usage, and locking stats for trending and contention analysis';
PRINT 'Use: EXECUTE collect.index_object_stats_collector @debug = 1;';
GO
