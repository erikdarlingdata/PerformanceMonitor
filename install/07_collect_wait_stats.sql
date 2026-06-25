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
Wait statistics collector
Collects cumulative wait statistics from sys.dm_os_wait_stats
*/

IF OBJECT_ID(N'collect.wait_stats_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.wait_stats_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.wait_stats_collector
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
        IF OBJECT_ID(N'collect.wait_stats', N'U') IS NULL
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
                N'wait_stats_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.wait_stats does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'wait_stats',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.wait_stats', N'U') IS NULL
            BEGIN
                ROLLBACK TRANSACTION;
                RAISERROR(N'Table collect.wait_stats still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Ensure config.ignored_wait_types exists and has data
        */
        IF OBJECT_ID(N'config.ignored_wait_types', N'U') IS NULL
        OR NOT EXISTS (SELECT 1/0 FROM config.ignored_wait_types WHERE is_enabled = 1)
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'config.ignored_wait_types table missing or empty - calling ensure_config_tables', 0, 1) WITH NOWAIT;
            END;

            EXECUTE config.ensure_config_tables
                @debug = @debug;
        END;

        /*
        Collect wait statistics from DMV
        Filters out unimportant wait types that just add noise
        */
        INSERT INTO
            collect.wait_stats
        (
            server_start_time,
            wait_type,
            waiting_tasks_count,
            wait_time_ms,
            signal_wait_time_ms
        )
        SELECT
            server_start_time = @server_start_time,
            wait_type = ws.wait_type,
            waiting_tasks_count = ws.waiting_tasks_count,
            wait_time_ms = ws.wait_time_ms,
            signal_wait_time_ms = ws.signal_wait_time_ms
        FROM sys.dm_os_wait_stats AS ws
        WHERE ws.wait_time_ms > 0
        AND   NOT EXISTS
        (
              SELECT
                  1/0
              FROM config.ignored_wait_types AS iwt
              WHERE iwt.wait_type = ws.wait_type
              AND   iwt.is_enabled = 1
        )
        OPTION(RECOMPILE);
        
        SET @rows_collected = ROWCOUNT_BIG();
        
        /*
        Calculate deltas for the newly inserted data
        */
        EXECUTE collect.calculate_deltas
            @table_name = N'wait_stats',
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
            N'wait_stats_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );
        
        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %I64d wait stats rows', 0, 1, @rows_collected) WITH NOWAIT;
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
            N'wait_stats_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );
        
        RAISERROR(N'Error in wait stats collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Wait stats collector created successfully';
GO
