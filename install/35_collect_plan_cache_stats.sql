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
Plan cache composition statistics collector
Collects aggregated plan cache statistics from sys.dm_exec_cached_plans
Tracks plan cache bloat, single-use plans, and plan cache composition by type
Helps identify ad-hoc query issues and plan cache efficiency problems
*/

IF OBJECT_ID(N'collect.plan_cache_stats_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.plan_cache_stats_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.plan_cache_stats_collector
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
        IF OBJECT_ID(N'collect.plan_cache_stats', N'U') IS NULL
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
                N'plan_cache_stats_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.plan_cache_stats does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'plan_cache_stats',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.plan_cache_stats', N'U') IS NULL
            BEGIN
                RAISERROR(N'Table collect.plan_cache_stats still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Collect plan cache composition statistics
        Aggregated by cacheobjtype and objtype to show plan cache composition
        Includes single-use plan tracking for ad-hoc query detection
        */
        INSERT INTO
            collect.plan_cache_stats
        (
            cacheobjtype,
            objtype,
            total_plans,
            total_size_mb,
            single_use_plans,
            single_use_size_mb,
            multi_use_plans,
            multi_use_size_mb,
            avg_use_count,
            avg_size_kb,
            oldest_plan_create_time
        )
        SELECT
            cacheobjtype = cp.cacheobjtype,
            objtype = cp.objtype,
            total_plans = COUNT_BIG(*),
            total_size_mb = CONVERT(integer, SUM(CONVERT(bigint, cp.size_in_bytes)) / 1024 / 1024),
            single_use_plans =
                SUM
                (
                    CASE
                        WHEN cp.usecounts = 1
                        THEN 1
                        ELSE 0
                    END
                ),
            single_use_size_mb =
                CONVERT
                (
                    integer,
                    SUM
                    (
                        CASE
                            WHEN cp.usecounts = 1
                            THEN CONVERT(bigint, cp.size_in_bytes)
                            ELSE 0
                        END
                    ) / 1024 / 1024
                ),
            multi_use_plans =
                SUM
                (
                    CASE
                        WHEN cp.usecounts > 1
                        THEN 1
                        ELSE 0
                    END
                ),
            multi_use_size_mb =
                CONVERT
                (
                    integer,
                    SUM
                    (
                        CASE
                            WHEN cp.usecounts > 1
                            THEN CONVERT(bigint, cp.size_in_bytes)
                            ELSE 0
                        END
                    ) / 1024 / 1024
                ),
            avg_use_count = CONVERT(decimal(38,2), AVG(CONVERT(bigint, cp.usecounts))),
            avg_size_kb =
                CASE
                    WHEN AVG(CONVERT(bigint, cp.size_in_bytes)) / 1024 > 2147483647
                    THEN 2147483647
                    ELSE CONVERT(integer, AVG(CONVERT(bigint, cp.size_in_bytes)) / 1024)
                END,
            oldest_plan_create_time = 
            (
                SELECT
                    MIN(deqs.creation_time)
                FROM sys.dm_exec_query_stats AS deqs            
            )
        FROM sys.dm_exec_cached_plans AS cp
        GROUP BY
            cp.cacheobjtype,
            cp.objtype
        OPTION(RECOMPILE);

        SET @rows_collected = ROWCOUNT_BIG();

        /*
        Debug output for single-use plan warnings
        */
        IF @debug = 1
        BEGIN
            DECLARE
                @total_plans integer,
                @single_use_plans integer,
                @single_use_percent decimal(5,2),
                @single_use_percent_int integer;

            SELECT
                @total_plans = SUM(pcs.total_plans),
                @single_use_plans = SUM(pcs.single_use_plans)
            FROM collect.plan_cache_stats AS pcs
            WHERE pcs.collection_id IN
            (
                SELECT
                    pcs2.collection_id
                FROM collect.plan_cache_stats AS pcs2
                WHERE pcs2.collection_time =
                (
                    SELECT
                        MAX(pcs3.collection_time)
                    FROM collect.plan_cache_stats AS pcs3
                )
            );

            IF @total_plans > 0
            BEGIN
                SET @single_use_percent =
                    (CONVERT(decimal(19,2), @single_use_plans) * 100.0) /
                    CONVERT(decimal(19,2), @total_plans);

                SET @single_use_percent_int = CONVERT(integer, @single_use_percent);

                IF @single_use_percent > 50.0
                BEGIN
                    RAISERROR(N'WARNING: Single-use plans = %d of %d total (%d%%). Consider enabling "optimize for ad hoc workloads"',
                        0, 1, @single_use_plans, @total_plans, @single_use_percent_int) WITH NOWAIT;
                END;
                ELSE
                BEGIN
                    RAISERROR(N'Single-use plans = %d of %d total (%d%%)',
                        0, 1, @single_use_plans, @total_plans, @single_use_percent_int) WITH NOWAIT;
                END;
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
            N'plan_cache_stats_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %I64d plan cache stats rows', 0, 1, @rows_collected) WITH NOWAIT;
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
            N'plan_cache_stats_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in plan cache stats collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Plan cache stats collector created successfully';
GO
