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
Spinlock statistics collector
Collects cumulative spinlock statistics from sys.dm_os_spinlock_stats
Spinlocks are lightweight synchronization primitives used for very short duration locks
High spinlock contention can indicate internal SQL Server bottlenecks
*/

IF OBJECT_ID(N'collect.spinlock_stats_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.spinlock_stats_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.spinlock_stats_collector
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
        IF OBJECT_ID(N'collect.spinlock_stats', N'U') IS NULL
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
                N'spinlock_stats_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.spinlock_stats does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'spinlock_stats',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.spinlock_stats', N'U') IS NULL
            BEGIN
                ROLLBACK TRANSACTION;
                RAISERROR(N'Table collect.spinlock_stats still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Collect spinlock statistics from DMV
        Includes all spinlock types with non-zero collisions or spins
        */
        INSERT INTO
            collect.spinlock_stats
        (
            server_start_time,
            spinlock_name,
            collisions,
            spins,
            spins_per_collision,
            sleep_time,
            backoffs
        )
        SELECT
            server_start_time = @server_start_time,
            spinlock_name = ss.name,
            collisions = ss.collisions,
            spins = ss.spins,
            spins_per_collision = ss.spins_per_collision,
            sleep_time = ss.sleep_time,
            backoffs = ss.backoffs
        FROM sys.dm_os_spinlock_stats AS ss
        WHERE ss.collisions > 0
        OR    ss.spins > 0
        OPTION(RECOMPILE);

        SET @rows_collected = ROWCOUNT_BIG();

        /*
        Calculate deltas for the newly inserted data
        */
        EXECUTE collect.calculate_deltas
            @table_name = N'spinlock_stats',
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
            N'spinlock_stats_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %I64d spinlock stats rows', 0, 1, @rows_collected) WITH NOWAIT;
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
            N'spinlock_stats_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in spinlock stats collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Spinlock stats collector created successfully';
GO
