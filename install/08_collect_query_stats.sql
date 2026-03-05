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
LOB columns are compressed with COMPRESS() before storage
Unchanged rows are skipped via row_hash deduplication
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
        @error_message nvarchar(4000),
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
        Read collection flags for optional query text and plan collection
        */
        SELECT
            @collect_query = cs.collect_query,
            @collect_plan = cs.collect_plan
        FROM config.collection_schedule AS cs
        WHERE cs.collector_name = N'query_stats_collector';

        /*
        First run detection - collect all queries if this is the first execution
        */
        IF NOT EXISTS (SELECT 1/0 FROM collect.query_stats)
        AND NOT EXISTS (SELECT 1/0 FROM config.collection_log WHERE collector_name = N'query_stats_collector')
        BEGIN
            SET @cutoff_time = CONVERT(datetime2(7), '19000101');

            IF @debug = 1
            BEGIN
                RAISERROR(N'First run detected - collecting all queries from sys.dm_exec_query_stats', 0, 1) WITH NOWAIT;
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
        Stage 1: Collect query statistics into temp table
        Temp table stays nvarchar(max) — COMPRESS happens at INSERT to permanent table
        */
        CREATE TABLE
            #query_stats_staging
        (
            server_start_time datetime2(7) NOT NULL,
            database_name sysname NOT NULL,
            sql_handle varbinary(64) NOT NULL,
            statement_start_offset integer NOT NULL,
            statement_end_offset integer NOT NULL,
            plan_generation_num bigint NOT NULL,
            plan_handle varbinary(64) NOT NULL,
            creation_time datetime2(7) NOT NULL,
            last_execution_time datetime2(7) NOT NULL,
            execution_count bigint NOT NULL,
            total_worker_time bigint NOT NULL,
            min_worker_time bigint NOT NULL,
            max_worker_time bigint NOT NULL,
            total_physical_reads bigint NOT NULL,
            min_physical_reads bigint NOT NULL,
            max_physical_reads bigint NOT NULL,
            total_logical_writes bigint NOT NULL,
            total_logical_reads bigint NOT NULL,
            total_clr_time bigint NOT NULL,
            total_elapsed_time bigint NOT NULL,
            min_elapsed_time bigint NOT NULL,
            max_elapsed_time bigint NOT NULL,
            query_hash binary(8) NULL,
            query_plan_hash binary(8) NULL,
            total_rows bigint NOT NULL,
            min_rows bigint NOT NULL,
            max_rows bigint NOT NULL,
            statement_sql_handle varbinary(64) NULL,
            statement_context_id bigint NULL,
            min_dop smallint NOT NULL,
            max_dop smallint NOT NULL,
            min_grant_kb bigint NOT NULL,
            max_grant_kb bigint NOT NULL,
            min_used_grant_kb bigint NOT NULL,
            max_used_grant_kb bigint NOT NULL,
            min_ideal_grant_kb bigint NOT NULL,
            max_ideal_grant_kb bigint NOT NULL,
            min_reserved_threads integer NOT NULL,
            max_reserved_threads integer NOT NULL,
            min_used_threads integer NOT NULL,
            max_used_threads integer NOT NULL,
            total_spills bigint NOT NULL,
            min_spills bigint NOT NULL,
            max_spills bigint NOT NULL,
            query_text nvarchar(max) NULL,
            query_plan_text nvarchar(max) NULL,
            row_hash binary(32) NULL
        );

        INSERT INTO
            #query_stats_staging
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
                    WHEN @collect_query = 0
                    THEN NULL
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
            query_plan_text =
                CASE
                    WHEN @collect_plan = 1
                    THEN tqp.query_plan
                    ELSE NULL
                END
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
        AND   d.state = 0 /*ONLINE only — skip RESTORING databases (mirroring/AG secondary)*/
        AND   pa.dbid NOT IN
        (
            1, 2, 3, 4, 32761, 32767,
            DB_ID(N'PerformanceMonitor')
        )
        AND   pa.dbid < 32761 /*exclude contained AG system databases*/
        OPTION(RECOMPILE);

        /*
        Stage 2: Compute row_hash on staging data
        Hash of cumulative metric columns — changes when query executes
        Binary concat: works on SQL 2016+, no CONCAT_WS dependency
        */
        UPDATE
            #query_stats_staging
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
                    CAST(total_rows AS binary(8)) +
                    CAST(total_spills AS binary(8))
                );

        /*
        Ensure tracking table exists
        */
        IF OBJECT_ID(N'collect.query_stats_latest_hash', N'U') IS NULL
        BEGIN
            CREATE TABLE
                collect.query_stats_latest_hash
            (
                sql_handle varbinary(64) NOT NULL,
                statement_start_offset integer NOT NULL,
                statement_end_offset integer NOT NULL,
                plan_handle varbinary(64) NOT NULL,
                row_hash binary(32) NOT NULL,
                last_seen datetime2(7) NOT NULL
                    DEFAULT SYSDATETIME(),
                CONSTRAINT
                    PK_query_stats_latest_hash
                PRIMARY KEY CLUSTERED
                    (sql_handle, statement_start_offset,
                     statement_end_offset, plan_handle)
                WITH
                    (DATA_COMPRESSION = PAGE)
            );
        END;

        /*
        Stage 3: INSERT only changed rows with COMPRESS on LOB columns
        A row is "changed" if its natural key is new or its hash differs
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
            query_plan_text,
            row_hash
        )
        SELECT
            s.server_start_time,
            s.database_name,
            s.sql_handle,
            s.statement_start_offset,
            s.statement_end_offset,
            s.plan_generation_num,
            s.plan_handle,
            s.creation_time,
            s.last_execution_time,
            s.execution_count,
            s.total_worker_time,
            s.min_worker_time,
            s.max_worker_time,
            s.total_physical_reads,
            s.min_physical_reads,
            s.max_physical_reads,
            s.total_logical_writes,
            s.total_logical_reads,
            s.total_clr_time,
            s.total_elapsed_time,
            s.min_elapsed_time,
            s.max_elapsed_time,
            s.query_hash,
            s.query_plan_hash,
            s.total_rows,
            s.min_rows,
            s.max_rows,
            s.statement_sql_handle,
            s.statement_context_id,
            s.min_dop,
            s.max_dop,
            s.min_grant_kb,
            s.max_grant_kb,
            s.min_used_grant_kb,
            s.max_used_grant_kb,
            s.min_ideal_grant_kb,
            s.max_ideal_grant_kb,
            s.min_reserved_threads,
            s.max_reserved_threads,
            s.min_used_threads,
            s.max_used_threads,
            s.total_spills,
            s.min_spills,
            s.max_spills,
            COMPRESS(s.query_text),
            COMPRESS(s.query_plan_text),
            s.row_hash
        FROM #query_stats_staging AS s
        LEFT JOIN collect.query_stats_latest_hash AS h
            ON  h.sql_handle = s.sql_handle
            AND h.statement_start_offset = s.statement_start_offset
            AND h.statement_end_offset = s.statement_end_offset
            AND h.plan_handle = s.plan_handle
            AND h.row_hash = s.row_hash
        WHERE h.sql_handle IS NULL /*no match = new or changed*/
        OPTION(RECOMPILE);

        SET @rows_collected = ROWCOUNT_BIG();

        /*
        Stage 4: Update tracking table with current hashes
        */
        MERGE collect.query_stats_latest_hash AS t
        USING
        (
            SELECT
                sql_handle,
                statement_start_offset,
                statement_end_offset,
                plan_handle,
                row_hash
            FROM
            (
                SELECT
                    s2.sql_handle,
                    s2.statement_start_offset,
                    s2.statement_end_offset,
                    s2.plan_handle,
                    s2.row_hash,
                    rn = ROW_NUMBER() OVER
                    (
                        PARTITION BY
                            s2.sql_handle,
                            s2.statement_start_offset,
                            s2.statement_end_offset,
                            s2.plan_handle
                        ORDER BY
                            s2.last_execution_time DESC
                    )
                FROM #query_stats_staging AS s2
            ) AS ranked
            WHERE ranked.rn = 1
        ) AS s
            ON  t.sql_handle = s.sql_handle
            AND t.statement_start_offset = s.statement_start_offset
            AND t.statement_end_offset = s.statement_end_offset
            AND t.plan_handle = s.plan_handle
        WHEN MATCHED
        THEN UPDATE SET
            t.row_hash = s.row_hash,
            t.last_seen = SYSDATETIME()
        WHEN NOT MATCHED
        THEN INSERT
        (
            sql_handle,
            statement_start_offset,
            statement_end_offset,
            plan_handle,
            row_hash,
            last_seen
        )
        VALUES
        (
            s.sql_handle,
            s.statement_start_offset,
            s.statement_end_offset,
            s.plan_handle,
            s.row_hash,
            SYSDATETIME()
        );

        IF @debug = 1
        BEGIN
            DECLARE @staging_count bigint;
            SELECT @staging_count = COUNT_BIG(*) FROM #query_stats_staging;
            RAISERROR(N'Staged %I64d rows, inserted %I64d changed rows', 0, 1, @staging_count, @rows_collected) WITH NOWAIT;
        END;

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
PRINT 'LOB columns compressed with COMPRESS(), unchanged rows skipped via row_hash';
GO
