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
Memory clerks statistics collector
Collects ALL memory clerk data from sys.dm_os_memory_clerks
Stores raw KB values with delta calculations for tracking memory changes over time
No categorization labels or TOP N filtering - collects all clerks
*/

IF OBJECT_ID(N'collect.memory_clerks_stats_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.memory_clerks_stats_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.memory_clerks_stats_collector
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
        @server_start_time datetime2(7) =
        (
            SELECT
                osi.sqlserver_start_time
            FROM sys.dm_os_sys_info AS osi
        ),
        @error_message nvarchar(4000);

    BEGIN TRY
        BEGIN TRANSACTION;

        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.memory_clerks_stats', N'U') IS NULL
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
                N'memory_clerks_stats_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.memory_clerks_stats does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'memory_clerks_stats',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.memory_clerks_stats', N'U') IS NULL
            BEGIN
                ROLLBACK TRANSACTION;
                RAISERROR(N'Table collect.memory_clerks_stats still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Collect ALL memory clerk statistics (not just top N)
        Aggregated by clerk type and memory node
        Stores raw KB values - no pre-conversion to GB or categorization
        Delta framework will calculate changes over time for memory growth/shrinkage tracking
        */
        INSERT INTO
            collect.memory_clerks_stats
        (
            server_start_time,
            clerk_type,
            memory_node_id,
            pages_kb,
            virtual_memory_reserved_kb,
            virtual_memory_committed_kb,
            awe_allocated_kb,
            shared_memory_reserved_kb,
            shared_memory_committed_kb
        )
        SELECT
            server_start_time = @server_start_time,
            clerk_type = domc.type,
            memory_node_id = domc.memory_node_id,
            pages_kb =
                SUM(domc.pages_kb),
            virtual_memory_reserved_kb =
                SUM(domc.virtual_memory_reserved_kb),
            virtual_memory_committed_kb =
                SUM(domc.virtual_memory_committed_kb),
            awe_allocated_kb =
                SUM(domc.awe_allocated_kb),
            shared_memory_reserved_kb =
                SUM(domc.shared_memory_reserved_kb),
            shared_memory_committed_kb =
                SUM(domc.shared_memory_committed_kb)
        FROM sys.dm_os_memory_clerks AS domc
        WHERE domc.memory_node_id < 64
        GROUP BY
            domc.type,
            domc.memory_node_id
        HAVING
            SUM
            (
                domc.pages_kb +
                domc.virtual_memory_committed_kb +
                domc.awe_allocated_kb +
                domc.shared_memory_committed_kb
            ) > 0
        OPTION(RECOMPILE);

        SET @rows_collected = ROWCOUNT_BIG();

        /*
        Calculate deltas for the newly inserted data
        */
        EXECUTE collect.calculate_deltas
            @table_name = N'memory_clerks_stats',
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
            N'memory_clerks_stats_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %I64d memory clerk stats rows', 0, 1, @rows_collected) WITH NOWAIT;
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
            N'memory_clerks_stats_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in memory clerks stats collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Memory clerks stats collector created successfully';
PRINT 'Collects ALL memory clerk data aggregated by type and memory node';
PRINT 'Stores raw KB values with delta calculations for tracking memory changes over time';
GO
