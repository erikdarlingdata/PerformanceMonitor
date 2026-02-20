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
Memory statistics collector
Collects memory usage from sys.dm_os_memory_clerks, sys.dm_os_process_memory, and sys.dm_os_sys_memory
*/

IF OBJECT_ID(N'collect.memory_stats_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.memory_stats_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.memory_stats_collector
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
        @previous_buffer_pool_mb decimal(19,2),
        @previous_plan_cache_mb decimal(19,2);

    BEGIN TRY
        BEGIN TRANSACTION;

        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.memory_stats', N'U') IS NULL
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
                N'memory_stats_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.memory_stats does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'memory_stats',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.memory_stats', N'U') IS NULL
            BEGIN
                ROLLBACK TRANSACTION;
                RAISERROR(N'Table collect.memory_stats still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Get previous collection values for pressure detection
        A 20%+ drop in buffer pool or plan cache indicates memory pressure
        */
        SELECT
            @previous_buffer_pool_mb = ms.buffer_pool_mb,
            @previous_plan_cache_mb = ms.plan_cache_mb
        FROM collect.memory_stats AS ms
        WHERE ms.collection_id =
        (
            SELECT
                MAX(ms2.collection_id)
            FROM collect.memory_stats AS ms2
        );

        /*
        Collect memory statistics from various DMVs
        Aggregates memory clerk data into useful categories
        Calculates pressure warnings based on comparison to previous collection
        */
        WITH
            memory_clerks AS
        (
            SELECT
                buffer_pool_mb =
                    SUM
                    (
                        CASE
                            WHEN mc.type = N'MEMORYCLERK_SQLBUFFERPOOL'
                            THEN mc.pages_kb
                            ELSE 0
                        END
                    ) / 1024.0,
                plan_cache_mb =
                    SUM
                    (
                        CASE
                            WHEN mc.type IN
                            (
                                N'CACHESTORE_SQLCP',
                                N'CACHESTORE_OBJCP'
                            )
                            THEN mc.pages_kb
                            ELSE 0
                        END
                    ) / 1024.0,
                other_memory_mb =
                    SUM
                    (
                        CASE
                            WHEN mc.type NOT IN
                            (
                                N'MEMORYCLERK_SQLBUFFERPOOL',
                                N'CACHESTORE_SQLCP',
                                N'CACHESTORE_OBJCP'
                            )
                            THEN mc.pages_kb
                            ELSE 0
                        END
                    ) / 1024.0,
                total_memory_mb = SUM(mc.pages_kb) / 1024.0
            FROM sys.dm_os_memory_clerks AS mc
            WHERE mc.pages_kb > 0
        ),
        process_memory AS
        (
            SELECT
                physical_memory_in_use_mb = pm.physical_memory_in_use_kb / 1024.0,
                memory_utilization_percentage = pm.memory_utilization_percentage
            FROM sys.dm_os_process_memory AS pm
        ),
        system_memory AS
        (
            SELECT
                available_physical_memory_mb = sm.available_physical_memory_kb / 1024.0,
                total_physical_memory_mb = sm.total_physical_memory_kb / 1024.0
            FROM sys.dm_os_sys_memory AS sm
        ),
        system_info AS
        (
            SELECT
                committed_target_memory_mb = si.committed_target_kb / 1024.0
            FROM sys.dm_os_sys_info AS si
        )
        INSERT INTO
            collect.memory_stats
        (
            buffer_pool_mb,
            plan_cache_mb,
            other_memory_mb,
            total_memory_mb,
            physical_memory_in_use_mb,
            available_physical_memory_mb,
            memory_utilization_percentage,
            total_physical_memory_mb,
            committed_target_memory_mb,
            buffer_pool_pressure_warning,
            plan_cache_pressure_warning
        )
        SELECT
            buffer_pool_mb = mc.buffer_pool_mb,
            plan_cache_mb = mc.plan_cache_mb,
            other_memory_mb = mc.other_memory_mb,
            total_memory_mb = mc.total_memory_mb,
            physical_memory_in_use_mb = pm.physical_memory_in_use_mb,
            available_physical_memory_mb = sm.available_physical_memory_mb,
            memory_utilization_percentage = pm.memory_utilization_percentage,
            total_physical_memory_mb = sm.total_physical_memory_mb,
            committed_target_memory_mb = si.committed_target_memory_mb,
            buffer_pool_pressure_warning =
                CASE
                    WHEN @previous_buffer_pool_mb IS NOT NULL
                    AND  mc.buffer_pool_mb < (@previous_buffer_pool_mb * 0.80)
                    THEN 1
                    ELSE 0
                END,
            plan_cache_pressure_warning =
                CASE
                    WHEN @previous_plan_cache_mb IS NOT NULL
                    AND  mc.plan_cache_mb < (@previous_plan_cache_mb * 0.80)
                    THEN 1
                    ELSE 0
                END
        FROM memory_clerks AS mc
        CROSS JOIN process_memory AS pm
        CROSS JOIN system_memory AS sm
        CROSS JOIN system_info AS si
        OPTION(RECOMPILE);
        
        SET @rows_collected = ROWCOUNT_BIG();

        /*
        Debug output for pressure warnings
        */
        IF @debug = 1
        BEGIN
            DECLARE
                @current_buffer_pool_mb decimal(19,2),
                @current_plan_cache_mb decimal(19,2),
                @buffer_warning bit,
                @plan_warning bit;

            SELECT
                @current_buffer_pool_mb = ms.buffer_pool_mb,
                @current_plan_cache_mb = ms.plan_cache_mb,
                @buffer_warning = ms.buffer_pool_pressure_warning,
                @plan_warning = ms.plan_cache_pressure_warning
            FROM collect.memory_stats AS ms
            WHERE ms.collection_id =
            (
                SELECT
                    MAX(ms2.collection_id)
                FROM collect.memory_stats AS ms2
            );

            IF @buffer_warning = 1
            BEGIN
                DECLARE @buffer_msg nvarchar(500) =
                    N'WARNING: Buffer pool dropped from ' +
                    CONVERT(nvarchar(20), @previous_buffer_pool_mb) + N' MB to ' +
                    CONVERT(nvarchar(20), @current_buffer_pool_mb) + N' MB (>20% drop)';
                RAISERROR(@buffer_msg, 0, 1) WITH NOWAIT;
            END;

            IF @plan_warning = 1
            BEGIN
                DECLARE @plan_msg nvarchar(500) =
                    N'WARNING: Plan cache dropped from ' +
                    CONVERT(nvarchar(20), @previous_plan_cache_mb) + N' MB to ' +
                    CONVERT(nvarchar(20), @current_plan_cache_mb) + N' MB (>20% drop)';
                RAISERROR(@plan_msg, 0, 1) WITH NOWAIT;
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
            N'memory_stats_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );
        
        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %d memory stats rows', 0, 1, @rows_collected) WITH NOWAIT;
        END;
        
        COMMIT TRANSACTION;
        
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
            N'memory_stats_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );
        
        RAISERROR(N'Error in memory stats collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Memory stats collector created successfully';
PRINT 'Includes pressure warnings for 20%+ drops in buffer pool or plan cache';
GO
