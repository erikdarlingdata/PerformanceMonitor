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
Latch statistics collector
Collects cumulative latch statistics from sys.dm_os_latch_stats
Latches are lightweight internal synchronization objects used by SQL Server
High latch waits can indicate contention on internal structures
*/

IF OBJECT_ID(N'collect.latch_stats_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.latch_stats_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.latch_stats_collector
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
                sqlserver_start_time
            FROM sys.dm_os_sys_info
        );

    BEGIN TRY
        BEGIN TRANSACTION;

        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.latch_stats', N'U') IS NULL
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
                N'latch_stats_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.latch_stats does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'latch_stats',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.latch_stats', N'U') IS NULL
            BEGIN
                ROLLBACK TRANSACTION;
                RAISERROR(N'Table collect.latch_stats still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Collect latch statistics from DMV
        Includes all latch classes with non-zero wait time
        */
        INSERT INTO
            collect.latch_stats
        (
            server_start_time,
            latch_class,
            waiting_requests_count,
            wait_time_ms,
            max_wait_time_ms
        )
        SELECT
            server_start_time = @server_start_time,
            latch_class = ls.latch_class,
            waiting_requests_count = ls.waiting_requests_count,
            wait_time_ms = ls.wait_time_ms,
            max_wait_time_ms = ls.max_wait_time_ms
        FROM sys.dm_os_latch_stats AS ls
        WHERE ls.wait_time_ms > 0
        OPTION(RECOMPILE);

        SET @rows_collected = ROWCOUNT_BIG();

        /*
        Calculate deltas for the newly inserted data
        */
        EXECUTE collect.calculate_deltas
            @table_name = N'latch_stats',
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
            N'latch_stats_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %I64d latch stats rows', 0, 1, @rows_collected) WITH NOWAIT;
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
            N'latch_stats_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in latch stats collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Latch stats collector created successfully';
GO
