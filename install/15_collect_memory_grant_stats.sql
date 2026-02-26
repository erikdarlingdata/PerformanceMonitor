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
Memory grant statistics collector
Collects memory grant semaphore data from sys.dm_exec_query_resource_semaphores
Point-in-time snapshot data for memory grant pressure monitoring
Delta calculation for cumulative counters handled by collect.calculate_deltas
*/

IF OBJECT_ID(N'collect.memory_grant_stats_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.memory_grant_stats_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.memory_grant_stats_collector
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
        IF OBJECT_ID(N'collect.memory_grant_stats', N'U') IS NULL
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
                N'memory_grant_stats_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.memory_grant_stats does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'memory_grant_stats',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.memory_grant_stats', N'U') IS NULL
            BEGIN
                ROLLBACK TRANSACTION;
                RAISERROR(N'Table collect.memory_grant_stats still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Collect memory grant semaphore statistics
        Point-in-time state data showing current memory grant pressure
        */
        INSERT INTO
            collect.memory_grant_stats
        (
            server_start_time,
            resource_semaphore_id,
            pool_id,
            target_memory_mb,
            max_target_memory_mb,
            total_memory_mb,
            available_memory_mb,
            granted_memory_mb,
            used_memory_mb,
            grantee_count,
            waiter_count,
            timeout_error_count,
            forced_grant_count
        )
        SELECT
            server_start_time =
            (
                SELECT
                    dosi.sqlserver_start_time
                FROM sys.dm_os_sys_info AS dosi
            ),
            resource_semaphore_id = deqrs.resource_semaphore_id,
            pool_id = deqrs.pool_id,
            target_memory_mb = deqrs.target_memory_kb / 1024.0,
            max_target_memory_mb = deqrs.max_target_memory_kb / 1024.0,
            total_memory_mb = deqrs.total_memory_kb / 1024.0,
            available_memory_mb = deqrs.available_memory_kb / 1024.0,
            granted_memory_mb = deqrs.granted_memory_kb / 1024.0,
            used_memory_mb = deqrs.used_memory_kb / 1024.0,
            grantee_count = deqrs.grantee_count,
            waiter_count = deqrs.waiter_count,
            timeout_error_count = ISNULL(deqrs.timeout_error_count, 0),
            forced_grant_count = ISNULL(deqrs.forced_grant_count, 0)
        FROM sys.dm_exec_query_resource_semaphores AS deqrs
        WHERE deqrs.max_target_memory_kb IS NOT NULL
        OPTION(RECOMPILE);

        SET @rows_collected = ROWCOUNT_BIG();

        /*
        Calculate deltas for cumulative counters
        */
        EXECUTE collect.calculate_deltas
            @table_name = N'memory_grant_stats',
            @debug = @debug;

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
            N'memory_grant_stats_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %d memory grant stats rows', 0, 1, @rows_collected) WITH NOWAIT;
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
            N'memory_grant_stats_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in memory grant stats collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Memory grant stats collector created successfully';
PRINT 'Collects point-in-time memory grant semaphore data from sys.dm_exec_query_resource_semaphores';
PRINT 'Delta calculation for timeout_error_count and forced_grant_count via collect.calculate_deltas';
GO
