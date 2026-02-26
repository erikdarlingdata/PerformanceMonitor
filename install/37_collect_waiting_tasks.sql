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
Waiting tasks collector
Captures point-in-time snapshot of waiting tasks and sessions with their associated queries
Purpose: Correlate specific queries with the wait types they experience
Helps answer: "What queries are causing high wait times?" and "Why is this query slow?"
*/

IF OBJECT_ID(N'collect.waiting_tasks_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.waiting_tasks_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.waiting_tasks_collector
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
        IF OBJECT_ID(N'collect.waiting_tasks', N'U') IS NULL
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
                N'waiting_tasks_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.waiting_tasks does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'waiting_tasks',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.waiting_tasks', N'U') IS NULL
            BEGIN
                ROLLBACK TRANSACTION;
                RAISERROR(N'Table collect.waiting_tasks still missing after ensure procedure', 16, 1);
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
        Collect waiting tasks with query correlation
        Captures sessions currently waiting and their associated queries
        Joins sys.dm_os_waiting_tasks with sys.dm_exec_requests and sys.dm_exec_sql_text
        */
        INSERT INTO
            collect.waiting_tasks
        (
            session_id,
            wait_type,
            wait_duration_ms,
            blocking_session_id,
            resource_description,
            database_id,
            database_name,
            query_text,
            statement_text,
            query_plan,
            sql_handle,
            plan_handle,
            request_status,
            command,
            cpu_time_ms,
            total_elapsed_time_ms,
            logical_reads,
            writes,
            row_count
        )
        SELECT
            session_id = wt.session_id,
            wait_type = wt.wait_type,
            wait_duration_ms = wt.wait_duration_ms,
            blocking_session_id = ISNULL(wt.blocking_session_id, 0),
            resource_description = NULL,
            database_id = NULL,
            database_name = DB_NAME(der.database_id),
            query_text = NULL,
            statement_text = NULL,
            query_plan = NULL,
            sql_handle = NULL,
            plan_handle = NULL,
            request_status = NULL,
            command = NULL,
            cpu_time_ms = NULL,
            total_elapsed_time_ms = NULL,
            logical_reads = NULL,
            writes = NULL,
            row_count = NULL
        FROM sys.dm_os_waiting_tasks AS wt
        LEFT JOIN sys.dm_exec_requests AS der
          ON der.session_id = wt.session_id
        OUTER APPLY sys.dm_exec_sql_text(der.sql_handle) AS dest
        OUTER APPLY sys.dm_exec_text_query_plan
        (
            der.plan_handle,
            der.statement_start_offset,
            der.statement_end_offset
        ) AS qp
        WHERE wt.session_id > 50
        AND   ISNULL(der.database_id, 0) NOT IN (1, 3, 4, DB_ID(N'PerformanceMonitor'))
        AND   ISNULL(der.database_id, 0) < 32761 /*exclude contained AG system databases*/
        AND   NOT EXISTS
        (
              SELECT
                  1/0
              FROM config.ignored_wait_types AS iwt
              WHERE iwt.wait_type = wt.wait_type
              AND   iwt.is_enabled = 1
        )
        OPTION(RECOMPILE);

        SET @rows_collected = ROWCOUNT_BIG();

        /*
        Debug output
        */
        IF @debug = 1
        BEGIN
            IF @rows_collected > 0
            BEGIN
                RAISERROR(N'Collected %d waiting tasks', 0, 1, @rows_collected) WITH NOWAIT;

                /*
                Show top wait types from this collection
                */
                SELECT TOP (10)
                    wait_type = wt.wait_type,
                    waiting_count = COUNT_BIG(*),
                    avg_wait_ms = AVG(wt.wait_duration_ms),
                    max_wait_ms = MAX(wt.wait_duration_ms),
                    total_wait_ms = SUM(wt.wait_duration_ms)
                FROM collect.waiting_tasks AS wt
                WHERE wt.collection_time >= @start_time
                GROUP BY
                    wt.wait_type
                ORDER BY
                    SUM(wt.wait_duration_ms) DESC;
            END;
            ELSE
            BEGIN
                RAISERROR(N'No waiting tasks found (server is healthy)', 0, 1) WITH NOWAIT;
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
            N'waiting_tasks_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

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
            N'waiting_tasks_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in waiting tasks collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Waiting tasks collector created successfully';
PRINT 'Collects point-in-time snapshot of waiting tasks with query correlation';
PRINT 'Use: EXECUTE collect.waiting_tasks_collector @debug = 1;';
GO
