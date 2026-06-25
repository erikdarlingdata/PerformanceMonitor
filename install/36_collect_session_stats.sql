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
Session statistics collector
Collects aggregated session and connection metrics for trending analysis
Tracks connection counts by status, database, application, and host
Helps identify connection leaks and application connection patterns
*/

IF OBJECT_ID(N'collect.session_stats_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.session_stats_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.session_stats_collector
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
        IF OBJECT_ID(N'collect.session_stats', N'U') IS NULL
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
                N'session_stats_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.session_stats does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'session_stats',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.session_stats', N'U') IS NULL
            BEGIN
                ROLLBACK TRANSACTION;
                RAISERROR(N'Table collect.session_stats still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Collect aggregated session statistics
        Point-in-time snapshot of session counts by status, database, application, host
        */
        INSERT INTO
            collect.session_stats
        (
            total_sessions,
            running_sessions,
            sleeping_sessions,
            background_sessions,
            dormant_sessions,
            idle_sessions_over_30min,
            sessions_waiting_for_memory,
            databases_with_connections,
            top_application_name,
            top_application_connections,
            top_host_name,
            top_host_connections
        )
        SELECT
            total_sessions = COUNT_BIG(*),
            running_sessions =
                SUM
                (
                    CASE
                        WHEN des.status = N'running'
                        THEN 1
                        ELSE 0
                    END
                ),
            sleeping_sessions =
                SUM
                (
                    CASE
                        WHEN des.status = N'sleeping'
                        THEN 1
                        ELSE 0
                    END
                ),
            background_sessions =
                SUM
                (
                    CASE
                        WHEN des.is_user_process = 0
                        THEN 1
                        ELSE 0
                    END
                ),
            dormant_sessions =
                SUM
                (
                    CASE
                        WHEN des.status = N'dormant'
                        THEN 1
                        ELSE 0
                    END
                ),
            idle_sessions_over_30min =
                SUM
                (
                    CASE
                        WHEN des.status = N'sleeping'
                        AND des.last_request_end_time < DATEADD(MINUTE, -30, SYSDATETIME())
                        AND des.is_user_process = 1
                        THEN 1
                        ELSE 0
                    END
                ),
            sessions_waiting_for_memory =
                SUM
                (
                    CASE
                        WHEN der.wait_type LIKE N'%MEMORY%'
                        THEN 1
                        ELSE 0
                    END
                ),
            databases_with_connections = COUNT_BIG(DISTINCT des.database_id),
            top_application_name = MAX(top_app.program_name),
            top_application_connections = MAX(top_app.connection_count),
            top_host_name = MAX(top_host.host_name),
            top_host_connections = MAX(top_host.connection_count)
        FROM sys.dm_exec_sessions AS des
        LEFT JOIN sys.dm_exec_requests AS der
          ON der.session_id = des.session_id
        OUTER APPLY
        (
            SELECT TOP (1)
                program_name = des2.program_name,
                connection_count = COUNT_BIG(*)
            FROM sys.dm_exec_sessions AS des2
            WHERE des2.session_id > 50
            AND   des2.is_user_process = 1
            AND   des2.program_name IS NOT NULL
            GROUP BY
                des2.program_name
            ORDER BY
                COUNT_BIG(*) DESC
        ) AS top_app
        OUTER APPLY
        (
            SELECT TOP (1)
                host_name = des3.host_name,
                connection_count = COUNT_BIG(*)
            FROM sys.dm_exec_sessions AS des3
            WHERE des3.session_id > 50
            AND   des3.is_user_process = 1
            AND   des3.host_name IS NOT NULL
            GROUP BY
                des3.host_name
            ORDER BY
                COUNT_BIG(*) DESC
        ) AS top_host
        WHERE des.session_id > 50
        OPTION(RECOMPILE);

        SET @rows_collected = ROWCOUNT_BIG();

        /*
        Debug output for warnings
        */
        IF @debug = 1
        BEGIN
            DECLARE
                @idle_sessions integer,
                @total_sessions integer,
                @idle_percent decimal(5,2),
                @idle_percent_int integer;

            SELECT
                @idle_sessions = ss.idle_sessions_over_30min,
                @total_sessions = ss.total_sessions
            FROM collect.session_stats AS ss
            WHERE ss.collection_id =
            (
                SELECT
                    MAX(ss2.collection_id)
                FROM collect.session_stats AS ss2
            );

            IF @total_sessions > 0
            BEGIN
                SET @idle_percent =
                    CONVERT(decimal(5,2), @idle_sessions) /
                    CONVERT(decimal(5,2), @total_sessions) * 100.0;

                SET @idle_percent_int = CONVERT(integer, @idle_percent);

                IF @idle_percent > 30.0
                BEGIN
                    RAISERROR(N'WARNING: High idle session count = %d of %d total (%d%%). Check for connection leaks',
                        0, 1, @idle_sessions, @total_sessions, @idle_percent_int) WITH NOWAIT;
                END;
                ELSE
                BEGIN
                    RAISERROR(N'Idle sessions = %d of %d total (%d%%)',
                        0, 1, @idle_sessions, @total_sessions, @idle_percent_int) WITH NOWAIT;
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
            N'session_stats_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %I64d session stats rows', 0, 1, @rows_collected) WITH NOWAIT;
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
            N'session_stats_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in session stats collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Session stats collector created successfully';
GO
