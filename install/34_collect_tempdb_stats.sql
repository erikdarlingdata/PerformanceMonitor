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
TempDB statistics collector
Collects tempdb space usage and contention metrics from multiple DMVs
Single table combining file space usage, task space usage, and session space usage
Helps identify version store growth, allocation contention, and top tempdb consumers
*/

IF OBJECT_ID(N'collect.tempdb_stats_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.tempdb_stats_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.tempdb_stats_collector
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
        @error_message nvarchar(4000);

    BEGIN TRY
        BEGIN TRANSACTION;

        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.tempdb_stats', N'U') IS NULL
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
                @start_time,
                N'tempdb_stats_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.tempdb_stats does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'tempdb_stats',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.tempdb_stats', N'U') IS NULL
            BEGIN
                ROLLBACK TRANSACTION;
                RAISERROR(N'Table collect.tempdb_stats still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Collect tempdb space usage from multiple DMVs
        Point-in-time snapshot combining file space, task space, and session space
        */
        INSERT INTO
            collect.tempdb_stats
        (
            /*File space usage from dm_db_file_space_usage*/
            user_object_reserved_page_count,
            internal_object_reserved_page_count,
            version_store_reserved_page_count,
            mixed_extent_page_count,
            unallocated_extent_page_count,
            /*Task space usage - top 5 consumers*/
            top_task_user_objects_mb,
            top_task_internal_objects_mb,
            top_task_total_mb,
            top_task_session_id,
            top_task_request_id,
            /*Session counts*/
            total_sessions_using_tempdb,
            sessions_with_user_objects,
            sessions_with_internal_objects,
            /*Warning flags*/
            version_store_high_warning,
            allocation_contention_warning
        )
        SELECT
            /*File space usage from dm_db_file_space_usage*/
            user_object_reserved_page_count = fsu.user_object_reserved_page_count,
            internal_object_reserved_page_count = fsu.internal_object_reserved_page_count,
            version_store_reserved_page_count = fsu.version_store_reserved_page_count,
            mixed_extent_page_count = fsu.mixed_extent_page_count,
            unallocated_extent_page_count = fsu.unallocated_extent_page_count,
            /*Task space usage - top consumer*/
            top_task_user_objects_mb = top_task.user_objects_alloc_mb,
            top_task_internal_objects_mb = top_task.internal_objects_alloc_mb,
            top_task_total_mb = top_task.total_alloc_mb,
            top_task_session_id = top_task.session_id,
            top_task_request_id = top_task.request_id,
            /*Session counts*/
            total_sessions_using_tempdb = session_counts.total_sessions,
            sessions_with_user_objects = session_counts.user_object_sessions,
            sessions_with_internal_objects = session_counts.internal_object_sessions,
            /*Warning flags*/
            version_store_high_warning =
                CASE
                    WHEN fsu.version_store_reserved_page_count * 8 / 1024 > 1024 /*1GB*/
                    THEN 1
                    ELSE 0
                END,
            allocation_contention_warning =
                CASE
                    WHEN EXISTS
                    (
                        SELECT
                            1/0
                        FROM sys.dm_os_waiting_tasks AS wt
                        WHERE wt.wait_type IN (N'PAGELATCH_UP', N'PAGELATCH_SH', N'PAGELATCH_EX')
                        AND   wt.wait_duration_ms > 1000
                        AND   wt.resource_description LIKE N'2:%' /*TempDB database_id = 2*/
                    )
                    THEN 1
                    ELSE 0
                END
        FROM
        (
            SELECT
                user_object_reserved_page_count = SUM(fsu.user_object_reserved_page_count),
                internal_object_reserved_page_count = SUM(fsu.internal_object_reserved_page_count),
                version_store_reserved_page_count = SUM(fsu.version_store_reserved_page_count),
                mixed_extent_page_count = SUM(fsu.mixed_extent_page_count),
                unallocated_extent_page_count = SUM(fsu.unallocated_extent_page_count)
            FROM tempdb.sys.dm_db_file_space_usage AS fsu
        ) AS fsu
        CROSS APPLY
        (
            SELECT TOP (1)
                session_id = tsu.session_id,
                request_id = tsu.request_id,
                user_objects_alloc_mb =
                    (tsu.user_objects_alloc_page_count - tsu.user_objects_dealloc_page_count) * 8.0 / 1024.0,
                internal_objects_alloc_mb =
                    (tsu.internal_objects_alloc_page_count - tsu.internal_objects_dealloc_page_count) * 8.0 / 1024.0,
                total_alloc_mb =
                    (
                        (tsu.user_objects_alloc_page_count - tsu.user_objects_dealloc_page_count) +
                        (tsu.internal_objects_alloc_page_count - tsu.internal_objects_dealloc_page_count)
                    ) * 8.0 / 1024.0
            FROM tempdb.sys.dm_db_task_space_usage AS tsu
            WHERE tsu.session_id > 50
            ORDER BY
                (
                    (tsu.user_objects_alloc_page_count - tsu.user_objects_dealloc_page_count) +
                    (tsu.internal_objects_alloc_page_count - tsu.internal_objects_dealloc_page_count)
                ) DESC
        ) AS top_task
        CROSS APPLY
        (
            SELECT
                total_sessions = COUNT_BIG(*),
                user_object_sessions =
                    ISNULL
                    (
                        SUM
                        (
                            CASE
                                WHEN (ssu.user_objects_alloc_page_count - ssu.user_objects_dealloc_page_count) > 0
                                THEN 1
                                ELSE 0
                            END
                        ),
                        0
                    ),
                internal_object_sessions =
                    ISNULL
                    (
                        SUM
                        (
                            CASE
                                WHEN (ssu.internal_objects_alloc_page_count - ssu.internal_objects_dealloc_page_count) > 0
                                THEN 1
                                ELSE 0
                            END
                        ),
                        0
                    )
            FROM tempdb.sys.dm_db_session_space_usage AS ssu
            WHERE ssu.session_id > 50
            AND   (
                    (ssu.user_objects_alloc_page_count - ssu.user_objects_dealloc_page_count) > 0
                    OR (ssu.internal_objects_alloc_page_count - ssu.internal_objects_dealloc_page_count) > 0
                  )
        ) AS session_counts
        OPTION(RECOMPILE);

        SET @rows_collected = ROWCOUNT_BIG();

        /*
        Debug output for warnings
        */
        IF @debug = 1
        BEGIN
            DECLARE
                @version_store_mb integer,
                @version_store_warning bit,
                @allocation_warning bit;

            SELECT
                @version_store_mb = ts.version_store_reserved_page_count * 8 / 1024,
                @version_store_warning = ts.version_store_high_warning,
                @allocation_warning = ts.allocation_contention_warning
            FROM collect.tempdb_stats AS ts
            WHERE ts.collection_id =
            (
                SELECT
                    MAX(ts2.collection_id)
                FROM collect.tempdb_stats AS ts2
            );

            IF @version_store_warning = 1
            BEGIN
                RAISERROR(N'WARNING: Version store using %d MB (>1GB threshold)', 0, 1, @version_store_mb) WITH NOWAIT;
            END;

            IF @allocation_warning = 1
            BEGIN
                RAISERROR(N'WARNING: TempDB allocation contention detected (PAGELATCH waits)', 0, 1) WITH NOWAIT;
            END;
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
            N'tempdb_stats_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %I64d tempdb stats rows', 0, 1, @rows_collected) WITH NOWAIT;
        END;

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
            N'tempdb_stats_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in tempdb stats collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'TempDB stats collector created successfully';
GO
