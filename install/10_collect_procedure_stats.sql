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
Procedure, trigger, and function stats collector
Collects execution statistics from sys.dm_exec_procedure_stats, 
sys.dm_exec_trigger_stats, and sys.dm_exec_function_stats
Includes execution plans for performance analysis
*/

IF OBJECT_ID(N'collect.procedure_stats_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.procedure_stats_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.procedure_stats_collector
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
        @last_collection_time datetime2(7) = NULL,
        @frequency_minutes integer = NULL,
        @cutoff_time datetime2(7) = NULL;

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
        IF OBJECT_ID(N'collect.procedure_stats', N'U') IS NULL
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
                N'procedure_stats_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.procedure_stats does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'procedure_stats',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.procedure_stats', N'U') IS NULL
            BEGIN
                ROLLBACK TRANSACTION;
                RAISERROR(N'Table collect.procedure_stats still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        First run detection - collect last 1 hour of procedures if this is the first execution
        */
        IF NOT EXISTS (SELECT 1/0 FROM collect.procedure_stats)
        AND NOT EXISTS (SELECT 1/0 FROM config.collection_log WHERE collector_name = N'procedure_stats_collector')
        BEGIN
            SET @cutoff_time = DATEADD(HOUR, -1, SYSDATETIME());

            IF @debug = 1
            BEGIN
                RAISERROR(N'First run detected - collecting last 1 hour of procedure stats', 0, 1) WITH NOWAIT;
            END;
        END;
        ELSE
        BEGIN
            /*
            Get last collection time for this collector
            */
            SELECT
                @last_collection_time = MAX(ps.collection_time)
            FROM collect.procedure_stats AS ps;

            /*
            Get collection interval from schedule table
            */
            SELECT
                @frequency_minutes = cs.frequency_minutes
            FROM config.collection_schedule AS cs
            WHERE cs.collector_name = N'procedure_stats_collector'
            AND   cs.enabled = 1;

            /*
            Calculate cutoff time
            If we have a previous collection, use that time
            Otherwise use the configured interval (or default to 15 minutes)
            */
            SELECT
                @cutoff_time = ISNULL(@last_collection_time,
                    DATEADD(MINUTE, -ISNULL(@frequency_minutes, 15), SYSDATETIME()));
        END;

        IF @debug = 1
        BEGIN
            DECLARE @cutoff_time_string nvarchar(30) = CONVERT(nvarchar(30), @cutoff_time, 120);
            RAISERROR(N'Collecting procedure stats with cutoff time: %s', 0, 1, @cutoff_time_string) WITH NOWAIT;
        END;

        /*
        Collect procedure, trigger, and function statistics
        Single query with UNION ALL to collect from all three DMVs
        */
        INSERT INTO
            collect.procedure_stats
        (
            server_start_time,
            object_type,
            database_name,
            object_id,
            object_name,
            schema_name,
            type_desc,
            sql_handle,
            plan_handle,
            cached_time,
            last_execution_time,
            execution_count,
            total_worker_time,
            min_worker_time,
            max_worker_time,
            total_elapsed_time,
            min_elapsed_time,
            max_elapsed_time,
            total_logical_reads,
            min_logical_reads,
            max_logical_reads,
            total_physical_reads,
            min_physical_reads,
            max_physical_reads,
            total_logical_writes,
            min_logical_writes,
            max_logical_writes,
            total_spills,
            min_spills,
            max_spills,
            query_plan_text
        )
        SELECT
            server_start_time = @server_start_time,
            object_type = N'PROCEDURE',
            database_name = d.name,
            object_id = ps.object_id,
            object_name = OBJECT_NAME(ps.object_id, ps.database_id),
            schema_name = OBJECT_SCHEMA_NAME(ps.object_id, ps.database_id),
            type_desc = N'PROCEDURE',
            sql_handle = ps.sql_handle,
            plan_handle = ps.plan_handle,
            cached_time = ps.cached_time,
            last_execution_time = ps.last_execution_time,
            execution_count = ps.execution_count,
            total_worker_time = ps.total_worker_time,
            min_worker_time = ps.min_worker_time,
            max_worker_time = ps.max_worker_time,
            total_elapsed_time = ps.total_elapsed_time,
            min_elapsed_time = ps.min_elapsed_time,
            max_elapsed_time = ps.max_elapsed_time,
            total_logical_reads = ps.total_logical_reads,
            min_logical_reads = ps.min_logical_reads,
            max_logical_reads = ps.max_logical_reads,
            total_physical_reads = ps.total_physical_reads,
            min_physical_reads = ps.min_physical_reads,
            max_physical_reads = ps.max_physical_reads,
            total_logical_writes = ps.total_logical_writes,
            min_logical_writes = ps.min_logical_writes,
            max_logical_writes = ps.max_logical_writes,
            total_spills = ps.total_spills,
            min_spills = ps.min_spills,
            max_spills = ps.max_spills,
            query_plan_text = CONVERT(nvarchar(max), tqp.query_plan)
        FROM sys.dm_exec_procedure_stats AS ps
        OUTER APPLY
            sys.dm_exec_text_query_plan
            (
                ps.plan_handle,
                0,
                -1
            ) AS tqp
        OUTER APPLY
        (
            SELECT 
                dbid = CONVERT(integer, pa.value)
            FROM sys.dm_exec_plan_attributes(ps.plan_handle) AS pa
            WHERE pa.attribute = N'dbid'
        ) AS pa
        LEFT JOIN sys.databases AS d
          ON pa.dbid = d.database_id
        WHERE ps.last_execution_time >= @cutoff_time
        AND   pa.dbid NOT IN
        (
            1, 3, 4, 32761, 32767,
            DB_ID(N'PerformanceMonitor')
        )
        AND   pa.dbid < 32761 /*exclude contained AG system databases*/

        UNION ALL

        SELECT
            server_start_time = @server_start_time,
            object_type = N'TRIGGER',
            database_name = d.name,
            object_id = ts.object_id,
            object_name = COALESCE(
                OBJECT_NAME(ts.object_id, ts.database_id),
                /*Parse trigger name from trigger definition text.
                  Handles: CREATE TRIGGER, CREATE OR ALTER TRIGGER,
                  DML triggers (ON table), DDL triggers (ON DATABASE/ALL SERVER),
                  and newlines between trigger name and ON clause.*/
                CONVERT
                (
                    sysname,
                    CASE
                        WHEN st.text LIKE N'%CREATE OR ALTER TRIGGER%'
                        THEN LTRIM(RTRIM(REPLACE(REPLACE(
                            SUBSTRING
                            (
                                st.text,
                                CHARINDEX(N'CREATE OR ALTER TRIGGER', st.text) + 23,
                                /*Find the earliest delimiter after the trigger name:
                                  newline (CR/LF) or ON keyword on same line*/
                                ISNULL
                                (
                                    NULLIF
                                    (
                                        CHARINDEX
                                        (
                                            CHAR(13),
                                            SUBSTRING(st.text, CHARINDEX(N'CREATE OR ALTER TRIGGER', st.text) + 23, 256)
                                        ),
                                        0
                                    ),
                                    ISNULL
                                    (
                                        NULLIF
                                        (
                                            CHARINDEX
                                            (
                                                CHAR(10),
                                                SUBSTRING(st.text, CHARINDEX(N'CREATE OR ALTER TRIGGER', st.text) + 23, 256)
                                            ),
                                            0
                                        ),
                                        ISNULL
                                        (
                                            NULLIF
                                            (
                                                CHARINDEX
                                                (
                                                    N' ON ',
                                                    SUBSTRING(st.text, CHARINDEX(N'CREATE OR ALTER TRIGGER', st.text) + 23, 256)
                                                ),
                                                0
                                            ),
                                            128
                                        )
                                    )
                                ) - 1
                            ), N'[', N''), N']', N'')))
                        WHEN st.text LIKE N'%CREATE TRIGGER%'
                        THEN LTRIM(RTRIM(REPLACE(REPLACE(
                            SUBSTRING
                            (
                                st.text,
                                CHARINDEX(N'CREATE TRIGGER', st.text) + 15,
                                ISNULL
                                (
                                    NULLIF
                                    (
                                        CHARINDEX
                                        (
                                            CHAR(13),
                                            SUBSTRING(st.text, CHARINDEX(N'CREATE TRIGGER', st.text) + 15, 256)
                                        ),
                                        0
                                    ),
                                    ISNULL
                                    (
                                        NULLIF
                                        (
                                            CHARINDEX
                                            (
                                                CHAR(10),
                                                SUBSTRING(st.text, CHARINDEX(N'CREATE TRIGGER', st.text) + 15, 256)
                                            ),
                                            0
                                        ),
                                        ISNULL
                                        (
                                            NULLIF
                                            (
                                                CHARINDEX
                                                (
                                                    N' ON ',
                                                    SUBSTRING(st.text, CHARINDEX(N'CREATE TRIGGER', st.text) + 15, 256)
                                                ),
                                                0
                                            ),
                                            128
                                        )
                                    )
                                ) - 1
                            ), N'[', N''), N']', N'')))
                        ELSE N'trigger_' + CONVERT(nvarchar(20), ts.object_id)
                    END
                )
            ),
            schema_name = ISNULL(OBJECT_SCHEMA_NAME(ts.object_id, ts.database_id), N'dbo'),
            type_desc = N'TRIGGER',
            sql_handle = ts.sql_handle,
            plan_handle = ts.plan_handle,
            cached_time = ts.cached_time,
            last_execution_time = ts.last_execution_time,
            execution_count = ts.execution_count,
            total_worker_time = ts.total_worker_time,
            min_worker_time = ts.min_worker_time,
            max_worker_time = ts.max_worker_time,
            total_elapsed_time = ts.total_elapsed_time,
            min_elapsed_time = ts.min_elapsed_time,
            max_elapsed_time = ts.max_elapsed_time,
            total_logical_reads = ts.total_logical_reads,
            min_logical_reads = ts.min_logical_reads,
            max_logical_reads = ts.max_logical_reads,
            total_physical_reads = ts.total_physical_reads,
            min_physical_reads = ts.min_physical_reads,
            max_physical_reads = ts.max_physical_reads,
            total_logical_writes = ts.total_logical_writes,
            min_logical_writes = ts.min_logical_writes,
            max_logical_writes = ts.max_logical_writes,
            total_spills = ts.total_spills,
            min_spills = ts.min_spills,
            max_spills = ts.max_spills,
            query_plan_text = CONVERT(nvarchar(max), tqp.query_plan)
        FROM sys.dm_exec_trigger_stats AS ts
        CROSS APPLY sys.dm_exec_sql_text(ts.sql_handle) AS st
        OUTER APPLY
            sys.dm_exec_text_query_plan
            (
                ts.plan_handle,
                0,
                -1
            ) AS tqp
        OUTER APPLY
        (
            SELECT
                dbid = CONVERT(integer, pa.value)
            FROM sys.dm_exec_plan_attributes(ts.plan_handle) AS pa
            WHERE pa.attribute = N'dbid'
        ) AS pa
        LEFT JOIN sys.databases AS d
          ON pa.dbid = d.database_id
        WHERE ts.last_execution_time >= @cutoff_time
        AND   pa.dbid NOT IN
        (
            1, 3, 4, 32761, 32767,
            DB_ID(N'PerformanceMonitor')
        )
        AND   pa.dbid < 32761 /*exclude contained AG system databases*/
        UNION ALL

        SELECT
            server_start_time = @server_start_time,
            object_type = N'FUNCTION',
            database_name = d.name,
            object_id = fs.object_id,
            object_name = OBJECT_NAME(fs.object_id, fs.database_id),
            schema_name = OBJECT_SCHEMA_NAME(fs.object_id, fs.database_id),
            type_desc = N'FUNCTION',
            sql_handle = fs.sql_handle,
            plan_handle = fs.plan_handle,
            cached_time = fs.cached_time,
            last_execution_time = fs.last_execution_time,
            execution_count = fs.execution_count,
            total_worker_time = fs.total_worker_time,
            min_worker_time = fs.min_worker_time,
            max_worker_time = fs.max_worker_time,
            total_elapsed_time = fs.total_elapsed_time,
            min_elapsed_time = fs.min_elapsed_time,
            max_elapsed_time = fs.max_elapsed_time,
            total_logical_reads = fs.total_logical_reads,
            min_logical_reads = fs.min_logical_reads,
            max_logical_reads = fs.max_logical_reads,
            total_physical_reads = fs.total_physical_reads,
            min_physical_reads = fs.min_physical_reads,
            max_physical_reads = fs.max_physical_reads,
            total_logical_writes = fs.total_logical_writes,
            min_logical_writes = fs.min_logical_writes,
            max_logical_writes = fs.max_logical_writes,
            total_spills = NULL,
            min_spills = NULL,
            max_spills = NULL,
            query_plan_text = CONVERT(nvarchar(max), tqp.query_plan)
        FROM sys.dm_exec_function_stats AS fs
        OUTER APPLY
            sys.dm_exec_text_query_plan
            (
                fs.plan_handle,
                0,
                -1
            ) AS tqp
        OUTER APPLY
        (
            SELECT 
                dbid = CONVERT(integer, pa.value)
            FROM sys.dm_exec_plan_attributes(fs.plan_handle) AS pa
            WHERE pa.attribute = N'dbid'
        ) AS pa
        LEFT JOIN sys.databases AS d
          ON pa.dbid = d.database_id
        WHERE fs.last_execution_time >= @cutoff_time
        AND   pa.dbid NOT IN
        (
            1, 3, 4, 32761, 32767,
            DB_ID(N'PerformanceMonitor')
        )
        AND   pa.dbid < 32761 /*exclude contained AG system databases*/
        OPTION(RECOMPILE);
        
        SET @rows_collected = ROWCOUNT_BIG();
        
        /*
        Calculate deltas for the newly inserted data
        */
        EXECUTE collect.calculate_deltas
            @table_name = N'procedure_stats',
            @debug = @debug;

        /*Tie statement sto procedures when possible*/
        UPDATE
            qs
        SET
            qs.object_type = ISNULL(ps.object_type,'STATEMENT'),
            qs.schema_name = ISNULL(ps.schema_name, N'N/A'),
            qs.object_name = ISNULL(ps.object_name, N'N/A')
        FROM collect.query_stats AS qs
        LEFT JOIN collect.procedure_stats AS ps
          ON  ps.sql_handle = qs.sql_handle
          AND ps.collection_time >= DATEADD(MINUTE, -1, @cutoff_time)
        WHERE qs.object_type = 'STATEMENT'
        AND   qs.schema_name IS NULL
        AND   qs.object_name IS NULL
        OPTION(RECOMPILE);

        
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
            N'procedure_stats_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );
        
        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %d procedure/trigger/function stats rows', 0, 1, @rows_collected) WITH NOWAIT;
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
            N'procedure_stats_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );
        
        RAISERROR(N'Error in procedure stats collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Procedure stats collector created successfully';
GO
