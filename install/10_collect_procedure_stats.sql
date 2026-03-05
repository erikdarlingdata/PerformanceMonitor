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
LOB columns are compressed with COMPRESS() before storage
Unchanged rows are skipped via row_hash deduplication
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
        @cutoff_time datetime2(7) = NULL,
        @collect_query bit = 1,
        @collect_plan bit = 1;

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
        Read collection flags for optional plan collection
        */
        SELECT
            @collect_query = cs.collect_query,
            @collect_plan = cs.collect_plan
        FROM config.collection_schedule AS cs
        WHERE cs.collector_name = N'procedure_stats_collector';

        /*
        First run detection - collect all procedures if this is the first execution
        */
        IF NOT EXISTS (SELECT 1/0 FROM collect.procedure_stats)
        AND NOT EXISTS (SELECT 1/0 FROM config.collection_log WHERE collector_name = N'procedure_stats_collector')
        BEGIN
            SET @cutoff_time = CONVERT(datetime2(7), '19000101');

            IF @debug = 1
            BEGIN
                RAISERROR(N'First run detected - collecting all procedures from sys.dm_exec_procedure_stats', 0, 1) WITH NOWAIT;
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
        Stage 1: Collect procedure, trigger, and function statistics into temp table
        Temp table stays nvarchar(max) — COMPRESS happens at INSERT to permanent table
        */
        CREATE TABLE
            #procedure_stats_staging
        (
            server_start_time datetime2(7) NOT NULL,
            object_type nvarchar(20) NOT NULL,
            database_name sysname NOT NULL,
            object_id integer NOT NULL,
            object_name sysname NULL,
            schema_name sysname NULL,
            type_desc nvarchar(60) NULL,
            sql_handle varbinary(64) NOT NULL,
            plan_handle varbinary(64) NOT NULL,
            cached_time datetime2(7) NOT NULL,
            last_execution_time datetime2(7) NOT NULL,
            execution_count bigint NOT NULL,
            total_worker_time bigint NOT NULL,
            min_worker_time bigint NOT NULL,
            max_worker_time bigint NOT NULL,
            total_elapsed_time bigint NOT NULL,
            min_elapsed_time bigint NOT NULL,
            max_elapsed_time bigint NOT NULL,
            total_logical_reads bigint NOT NULL,
            min_logical_reads bigint NOT NULL,
            max_logical_reads bigint NOT NULL,
            total_physical_reads bigint NOT NULL,
            min_physical_reads bigint NOT NULL,
            max_physical_reads bigint NOT NULL,
            total_logical_writes bigint NOT NULL,
            min_logical_writes bigint NOT NULL,
            max_logical_writes bigint NOT NULL,
            total_spills bigint NULL,
            min_spills bigint NULL,
            max_spills bigint NULL,
            query_plan_text nvarchar(max) NULL,
            row_hash binary(32) NULL
        );

        INSERT INTO
            #procedure_stats_staging
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
            query_plan_text =
                CASE
                    WHEN @collect_plan = 1
                    THEN CONVERT(nvarchar(max), tqp.query_plan)
                    ELSE NULL
                END
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
        INNER JOIN sys.databases AS d
          ON pa.dbid = d.database_id
        WHERE ps.last_execution_time >= @cutoff_time
        AND   d.state = 0 /*ONLINE only — skip RESTORING databases (mirroring/AG secondary)*/
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
            query_plan_text =
                CASE
                    WHEN @collect_plan = 1
                    THEN CONVERT(nvarchar(max), tqp.query_plan)
                    ELSE NULL
                END
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
        INNER JOIN sys.databases AS d
          ON pa.dbid = d.database_id
        WHERE ts.last_execution_time >= @cutoff_time
        AND   d.state = 0 /*ONLINE only — skip RESTORING databases (mirroring/AG secondary)*/
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
            query_plan_text =
                CASE
                    WHEN @collect_plan = 1
                    THEN CONVERT(nvarchar(max), tqp.query_plan)
                    ELSE NULL
                END
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
        INNER JOIN sys.databases AS d
          ON pa.dbid = d.database_id
        WHERE fs.last_execution_time >= @cutoff_time
        AND   d.state = 0 /*ONLINE only — skip RESTORING databases (mirroring/AG secondary)*/
        AND   pa.dbid NOT IN
        (
            1, 3, 4, 32761, 32767,
            DB_ID(N'PerformanceMonitor')
        )
        AND   pa.dbid < 32761 /*exclude contained AG system databases*/
        OPTION(RECOMPILE);

        /*
        Stage 2: Compute row_hash on staging data
        Hash of cumulative metric columns — changes when procedure executes
        total_spills is nullable (functions don't have spills), use ISNULL
        */
        UPDATE
            #procedure_stats_staging
        SET
            row_hash =
                HASHBYTES
                (
                    'SHA2_256',
                    CAST(execution_count AS binary(8)) +
                    CAST(total_worker_time AS binary(8)) +
                    CAST(total_elapsed_time AS binary(8)) +
                    CAST(total_logical_reads AS binary(8)) +
                    CAST(total_physical_reads AS binary(8)) +
                    CAST(total_logical_writes AS binary(8)) +
                    ISNULL(CAST(total_spills AS binary(8)), 0x0000000000000000)
                );

        /*
        Ensure tracking table exists
        */
        IF OBJECT_ID(N'collect.procedure_stats_latest_hash', N'U') IS NULL
        BEGIN
            CREATE TABLE
                collect.procedure_stats_latest_hash
            (
                database_name sysname NOT NULL,
                object_id integer NOT NULL,
                plan_handle varbinary(64) NOT NULL,
                row_hash binary(32) NOT NULL,
                last_seen datetime2(7) NOT NULL
                    DEFAULT SYSDATETIME(),
                CONSTRAINT
                    PK_procedure_stats_latest_hash
                PRIMARY KEY CLUSTERED
                    (database_name, object_id, plan_handle)
                WITH
                    (DATA_COMPRESSION = PAGE)
            );
        END;

        /*
        Stage 3: INSERT only changed rows with COMPRESS on LOB columns
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
            query_plan_text,
            row_hash
        )
        SELECT
            s.server_start_time,
            s.object_type,
            s.database_name,
            s.object_id,
            s.object_name,
            s.schema_name,
            s.type_desc,
            s.sql_handle,
            s.plan_handle,
            s.cached_time,
            s.last_execution_time,
            s.execution_count,
            s.total_worker_time,
            s.min_worker_time,
            s.max_worker_time,
            s.total_elapsed_time,
            s.min_elapsed_time,
            s.max_elapsed_time,
            s.total_logical_reads,
            s.min_logical_reads,
            s.max_logical_reads,
            s.total_physical_reads,
            s.min_physical_reads,
            s.max_physical_reads,
            s.total_logical_writes,
            s.min_logical_writes,
            s.max_logical_writes,
            s.total_spills,
            s.min_spills,
            s.max_spills,
            COMPRESS(s.query_plan_text),
            s.row_hash
        FROM #procedure_stats_staging AS s
        LEFT JOIN collect.procedure_stats_latest_hash AS h
            ON  h.database_name = s.database_name
            AND h.object_id = s.object_id
            AND h.plan_handle = s.plan_handle
            AND h.row_hash = s.row_hash
        WHERE h.database_name IS NULL /*no match = new or changed*/
        OPTION(RECOMPILE);

        SET @rows_collected = ROWCOUNT_BIG();

        /*
        Stage 4: Update tracking table with current hashes
        */
        MERGE collect.procedure_stats_latest_hash AS t
        USING
        (
            SELECT
                database_name,
                object_id,
                plan_handle,
                row_hash
            FROM
            (
                SELECT
                    s2.database_name,
                    s2.object_id,
                    s2.plan_handle,
                    s2.row_hash,
                    rn = ROW_NUMBER() OVER
                    (
                        PARTITION BY
                            s2.database_name,
                            s2.object_id,
                            s2.plan_handle
                        ORDER BY
                            s2.last_execution_time DESC
                    )
                FROM #procedure_stats_staging AS s2
            ) AS ranked
            WHERE ranked.rn = 1
        ) AS s
            ON  t.database_name = s.database_name
            AND t.object_id = s.object_id
            AND t.plan_handle = s.plan_handle
        WHEN MATCHED
        THEN UPDATE SET
            t.row_hash = s.row_hash,
            t.last_seen = SYSDATETIME()
        WHEN NOT MATCHED
        THEN INSERT
        (
            database_name,
            object_id,
            plan_handle,
            row_hash,
            last_seen
        )
        VALUES
        (
            s.database_name,
            s.object_id,
            s.plan_handle,
            s.row_hash,
            SYSDATETIME()
        );

        IF @debug = 1
        BEGIN
            DECLARE @staging_count bigint;
            SELECT @staging_count = COUNT_BIG(*) FROM #procedure_stats_staging;
            RAISERROR(N'Staged %I64d rows, inserted %I64d changed rows', 0, 1, @staging_count, @rows_collected) WITH NOWAIT;
        END;

        /*
        Calculate deltas for the newly inserted data
        */
        EXECUTE collect.calculate_deltas
            @table_name = N'procedure_stats',
            @debug = @debug;

        /*Tie statements to procedures when possible*/
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
PRINT 'LOB columns compressed with COMPRESS(), unchanged rows skipped via row_hash';
GO
