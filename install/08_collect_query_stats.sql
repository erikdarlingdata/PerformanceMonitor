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
Query performance collector
Collects query execution statistics from sys.dm_exec_query_stats
Captures min/max values for parameter sensitivity detection
*/

IF OBJECT_ID(N'collect.query_stats_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.query_stats_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.query_stats_collector
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
        @server_start_time datetime2(7),
        @last_collection_time datetime2(7),
        @cutoff_time datetime2(7),
        @frequency_minutes integer,
        @error_message nvarchar(4000);

    BEGIN TRY
        BEGIN TRANSACTION;

        /*
        Get server start time for restart detection
        */
        SELECT
            @server_start_time = osi.sqlserver_start_time
        FROM sys.dm_os_sys_info AS osi;

        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.query_stats', N'U') IS NULL
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
                N'query_stats_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.query_stats does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'query_stats',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.query_stats', N'U') IS NULL
            BEGIN
                ROLLBACK TRANSACTION;
                RAISERROR(N'Table collect.query_stats still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        First run detection - collect last 1 hour of queries if this is the first execution
        */
        IF NOT EXISTS (SELECT 1/0 FROM collect.query_stats)
        AND NOT EXISTS (SELECT 1/0 FROM config.collection_log WHERE collector_name = N'query_stats_collector')
        BEGIN
            SET @cutoff_time = DATEADD(HOUR, -1, SYSDATETIME());

            IF @debug = 1
            BEGIN
                RAISERROR(N'First run detected - collecting last 1 hour of query stats', 0, 1) WITH NOWAIT;
            END;
        END;
        ELSE
        BEGIN
            /*
            Determine cutoff time for collecting queries
            Use last collection time or fall back to scheduled interval from config table
            */
            SELECT
                @last_collection_time = MAX(qs.collection_time)
            FROM collect.query_stats AS qs;

            /*
            Get actual collection interval from schedule table
            */
            SELECT
                @frequency_minutes = cs.frequency_minutes
            FROM config.collection_schedule AS cs
            WHERE cs.collector_name = N'query_stats_collector'
            AND   cs.enabled = 1;

            SELECT
                @cutoff_time =
                    ISNULL
                    (
                        @last_collection_time,
                        DATEADD(MINUTE, -ISNULL(@frequency_minutes, 15), SYSDATETIME())
                    );
        END;

        IF @debug = 1
        BEGIN
            DECLARE @cutoff_time_string nvarchar(30) = CONVERT(nvarchar(30), @cutoff_time, 120);
            RAISERROR(N'Collecting queries executed since %s', 0, 1, @cutoff_time_string) WITH NOWAIT;
        END;

        /*
        Collect query statistics directly from DMV
        Only collects queries executed since last collection
        Excludes PerformanceMonitor and system databases (including 32761, 32767)
        */
        INSERT INTO
            collect.query_stats
        (
            server_start_time,
            database_name,
            sql_handle,
            statement_start_offset,
            statement_end_offset,
            plan_generation_num,
            plan_handle,
            creation_time,
            last_execution_time,
            execution_count,
            total_worker_time,
            min_worker_time,
            max_worker_time,
            total_physical_reads,
            min_physical_reads,
            max_physical_reads,
            total_logical_writes,
            total_logical_reads,
            total_clr_time,
            total_elapsed_time,
            min_elapsed_time,
            max_elapsed_time,
            query_hash,
            query_plan_hash,
            total_rows,
            min_rows,
            max_rows,
            statement_sql_handle,
            statement_context_id,
            min_dop,
            max_dop,
            min_grant_kb,
            max_grant_kb,
            min_used_grant_kb,
            max_used_grant_kb,
            min_ideal_grant_kb,
            max_ideal_grant_kb,
            min_reserved_threads,
            max_reserved_threads,
            min_used_threads,
            max_used_threads,
            total_spills,
            min_spills,
            max_spills,
            query_text,
            query_plan_text
        )
        SELECT
            server_start_time = @server_start_time,
            database_name = d.name,
            sql_handle = qs.sql_handle,
            statement_start_offset = qs.statement_start_offset,
            statement_end_offset = qs.statement_end_offset,
            plan_generation_num = qs.plan_generation_num,
            plan_handle = qs.plan_handle,
            creation_time = qs.creation_time,
            last_execution_time = qs.last_execution_time,
            execution_count = qs.execution_count,
            total_worker_time = qs.total_worker_time,
            min_worker_time = qs.min_worker_time,
            max_worker_time = qs.max_worker_time,
            total_physical_reads = qs.total_physical_reads,
            min_physical_reads = qs.min_physical_reads,
            max_physical_reads = qs.max_physical_reads,
            total_logical_writes = qs.total_logical_writes,
            total_logical_reads = qs.total_logical_reads,
            total_clr_time = qs.total_clr_time,
            total_elapsed_time = qs.total_elapsed_time,
            min_elapsed_time = qs.min_elapsed_time,
            max_elapsed_time = qs.max_elapsed_time,
            query_hash = qs.query_hash,
            query_plan_hash = qs.query_plan_hash,
            total_rows = qs.total_rows,
            min_rows = qs.min_rows,
            max_rows = qs.max_rows,
            statement_sql_handle = qs.statement_sql_handle,
            statement_context_id = qs.statement_context_id,
            min_dop = qs.min_dop,
            max_dop = qs.max_dop,
            min_grant_kb = qs.min_grant_kb,
            max_grant_kb = qs.max_grant_kb,
            min_used_grant_kb = qs.min_used_grant_kb,
            max_used_grant_kb = qs.max_used_grant_kb,
            min_ideal_grant_kb = qs.min_ideal_grant_kb,
            max_ideal_grant_kb = qs.max_ideal_grant_kb,
            min_reserved_threads = qs.min_reserved_threads,
            max_reserved_threads = qs.max_reserved_threads,
            min_used_threads = qs.min_used_threads,
            max_used_threads = qs.max_used_threads,
            total_spills = qs.total_spills,
            min_spills = qs.min_spills,
            max_spills = qs.max_spills,
            query_text =
                CASE
                    WHEN qs.statement_start_offset = 0
                    AND  qs.statement_end_offset = -1
                    THEN st.text
                    ELSE
                        SUBSTRING
                        (
                            st.text,
                            (qs.statement_start_offset / 2) + 1,
                            (
                                CASE
                                    WHEN qs.statement_end_offset = -1
                                    THEN DATALENGTH(st.text)
                                    ELSE qs.statement_end_offset
                                END - qs.statement_start_offset
                            ) / 2 + 1
                        )
                END,
            query_plan_text = tqp.query_plan
        FROM sys.dm_exec_query_stats AS qs
        OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
        OUTER APPLY
            sys.dm_exec_text_query_plan
            (
                qs.plan_handle,
                qs.statement_start_offset,
                qs.statement_end_offset
            ) AS tqp
        CROSS APPLY
        (
            SELECT 
                dbid = CONVERT(integer, pa.value)
            FROM sys.dm_exec_plan_attributes(qs.plan_handle) AS pa
            WHERE pa.attribute = N'dbid'
        ) AS pa
        INNER JOIN sys.databases AS d
          ON pa.dbid = d.database_id
        WHERE qs.last_execution_time >= @cutoff_time
        AND   pa.dbid NOT IN
        (
            1, 2, 3, 4, 32761, 32767,
            DB_ID(N'PerformanceMonitor')
        )
        AND   pa.dbid < 32761 /*exclude contained AG system databases*/
        OPTION(RECOMPILE);

        SET @rows_collected = ROWCOUNT_BIG();

        /*
        Calculate deltas for the newly inserted data
        */
        EXECUTE collect.calculate_deltas
            @table_name = N'query_stats',
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
            N'query_stats_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %d query stats rows', 0, 1, @rows_collected) WITH NOWAIT;
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
            N'query_stats_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in query stats collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Query stats collector created successfully';
PRINT 'Collects queries executed since last collection from sys.dm_exec_query_stats';
PRINT 'Includes min/max values for parameter sensitivity detection';
GO
