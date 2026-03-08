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
Collector: database_size_stats_collector
Purpose: Captures per-file database sizes for growth trending and capacity
         planning. Collects total allocated size and used space per file.
Collection Type: Point-in-time snapshot (no deltas)
Target Table: collect.database_size_stats
Frequency: Every 60 minutes
Dependencies: sys.master_files, sys.databases, sys.dm_db_file_space_used
Notes: Uses cursor with dynamic SQL for cross-database used space collection.
       Azure SQL DB uses sys.database_files (single database scope).
*******************************************************************************/

IF OBJECT_ID(N'collect.database_size_stats_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.database_size_stats_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.database_size_stats_collector
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
        @error_message nvarchar(4000),
        @engine_edition integer =
            CONVERT(integer, SERVERPROPERTY(N'EngineEdition'));

    BEGIN TRY
        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.database_size_stats', N'U') IS NULL
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
                N'database_size_stats_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.database_size_stats does not exist, calling ensure procedure'
            );

            EXECUTE config.ensure_collection_table
                @table_name = N'database_size_stats',
                @debug = @debug;

            IF OBJECT_ID(N'collect.database_size_stats', N'U') IS NULL
            BEGIN
                RAISERROR(N'Table collect.database_size_stats still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Azure SQL DB: single database scope
        */
        IF @engine_edition = 5
        BEGIN
            INSERT INTO
                collect.database_size_stats
            (
                collection_time,
                database_name,
                database_id,
                file_id,
                file_type_desc,
                file_name,
                physical_name,
                total_size_mb,
                used_size_mb,
                auto_growth_mb,
                max_size_mb,
                recovery_model_desc,
                compatibility_level,
                state_desc,
                volume_mount_point,
                volume_total_mb,
                volume_free_mb
            )
            SELECT
                collection_time = @start_time,
                database_name = DB_NAME(),
                database_id = DB_ID(),
                file_id = df.file_id,
                file_type_desc = df.type_desc,
                file_name = df.name,
                physical_name = df.physical_name,
                total_size_mb =
                    CONVERT(decimal(19,2), df.size * 8.0 / 1024.0),
                used_size_mb =
                    CONVERT
                    (
                        decimal(19,2),
                        FILEPROPERTY(df.name, N'SpaceUsed') * 8.0 / 1024.0
                    ),
                auto_growth_mb =
                    CASE
                        WHEN df.is_percent_growth = 1
                        THEN NULL
                        ELSE CONVERT(decimal(19,2), df.growth * 8.0 / 1024.0)
                    END,
                max_size_mb =
                    CASE
                        WHEN df.max_size = -1
                        THEN CONVERT(decimal(19,2), -1)
                        WHEN df.max_size = 268435456
                        THEN CONVERT(decimal(19,2), 2097152) /*2 TB*/
                        ELSE CONVERT(decimal(19,2), df.max_size * 8.0 / 1024.0)
                    END,
                recovery_model_desc =
                    CONVERT(nvarchar(12), DATABASEPROPERTYEX(DB_NAME(), N'Recovery')),
                compatibility_level = NULL,
                state_desc = N'ONLINE',
                volume_mount_point = NULL,
                volume_total_mb = NULL,
                volume_free_mb = NULL
            FROM sys.database_files AS df
            OPTION(RECOMPILE);

            SET @rows_collected = ROWCOUNT_BIG();
        END;
        ELSE
        BEGIN
            /*
            On-prem / Azure MI / AWS RDS: cursor over all online databases
            Collect file sizes from sys.master_files and used space via
            dynamic SQL executing FILEPROPERTY in each database context
            */
            DECLARE
                @db_name sysname,
                @db_id integer,
                @sql nvarchar(max);

            DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
                SELECT
                    d.name,
                    d.database_id
                FROM sys.databases AS d
                WHERE d.state_desc = N'ONLINE'
                AND   d.database_id > 0
                ORDER BY
                    d.database_id;

            OPEN db_cursor;
            FETCH NEXT FROM db_cursor INTO @db_name, @db_id;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                BEGIN TRY
                    SET @sql = N'
                    USE ' + QUOTENAME(@db_name) + N';

                    INSERT INTO
                        PerformanceMonitor.collect.database_size_stats
                    (
                        collection_time,
                        database_name,
                        database_id,
                        file_id,
                        file_type_desc,
                        file_name,
                        physical_name,
                        total_size_mb,
                        used_size_mb,
                        auto_growth_mb,
                        max_size_mb,
                        recovery_model_desc,
                        compatibility_level,
                        state_desc,
                        volume_mount_point,
                        volume_total_mb,
                        volume_free_mb
                    )
                    SELECT
                        collection_time = @start_time,
                        database_name = DB_NAME(),
                        database_id = DB_ID(),
                        file_id = df.file_id,
                        file_type_desc = df.type_desc,
                        file_name = df.name,
                        physical_name = df.physical_name,
                        total_size_mb =
                            CONVERT(decimal(19,2), df.size * 8.0 / 1024.0),
                        used_size_mb =
                            CONVERT
                            (
                                decimal(19,2),
                                FILEPROPERTY(df.name, N''SpaceUsed'') * 8.0 / 1024.0
                            ),
                        auto_growth_mb =
                            CASE
                                WHEN df.is_percent_growth = 1
                                THEN NULL
                                ELSE CONVERT(decimal(19,2), df.growth * 8.0 / 1024.0)
                            END,
                        max_size_mb =
                            CASE
                                WHEN df.max_size = -1
                                THEN CONVERT(decimal(19,2), -1)
                                WHEN df.max_size = 268435456
                                THEN CONVERT(decimal(19,2), 2097152)
                                ELSE CONVERT(decimal(19,2), df.max_size * 8.0 / 1024.0)
                            END,
                        recovery_model_desc = d.recovery_model_desc,
                        compatibility_level = d.compatibility_level,
                        state_desc = d.state_desc,
                        volume_mount_point =
                            RTRIM(vs.volume_mount_point),
                        volume_total_mb =
                            CONVERT(decimal(19,2), vs.total_bytes / 1048576.0),
                        volume_free_mb =
                            CONVERT(decimal(19,2), vs.available_bytes / 1048576.0)
                    FROM sys.database_files AS df
                    CROSS JOIN sys.databases AS d
                    CROSS APPLY sys.dm_os_volume_stats(DB_ID(), df.file_id) AS vs
                    WHERE d.database_id = DB_ID();';

                    EXECUTE sys.sp_executesql
                        @sql,
                        N'@start_time datetime2(7)',
                        @start_time = @start_time;

                    SET @rows_collected = @rows_collected + ROWCOUNT_BIG();
                END TRY
                BEGIN CATCH
                    /*
                    Log per-database errors but continue with remaining databases
                    */
                    IF @debug = 1
                    BEGIN
                        RAISERROR(N'Error collecting size stats for database [%s]: %s', 0, 1, @db_name, @error_message) WITH NOWAIT;
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
            RAISERROR(N'Collected %d database size rows', 0, 1, @rows_collected) WITH NOWAIT;

            SELECT TOP (20)
                dss.database_name,
                dss.file_type_desc,
                dss.file_name,
                dss.total_size_mb,
                dss.used_size_mb,
                dss.free_space_mb,
                dss.used_pct,
                dss.volume_mount_point,
                dss.volume_total_mb,
                dss.volume_free_mb
            FROM collect.database_size_stats AS dss
            WHERE dss.collection_time = @start_time
            ORDER BY
                dss.total_size_mb DESC;
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
            N'database_size_stats_collector',
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
            N'database_size_stats_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in database size stats collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Database size stats collector created successfully';
PRINT 'Captures per-file database sizes for growth trending and capacity planning';
PRINT 'Use: EXECUTE collect.database_size_stats_collector @debug = 1;';
GO
