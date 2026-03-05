/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 2.1.0 to 2.2.0
Migrates collect.query_stats to compressed LOB storage:
  - query_text nvarchar(max) -> varbinary(max) via COMPRESS()
  - query_plan_text nvarchar(max) -> varbinary(max) via COMPRESS()
  - Drops unused query_plan xml column (never populated by collectors)
  - Adds row_hash binary(32) for deduplication
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
Skip if already migrated (query_text is already varbinary)
*/
IF EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'collect.query_stats')
    AND   name = N'query_text'
    AND   system_type_id = 165 /*varbinary*/
)
BEGIN
    PRINT 'collect.query_stats already migrated to compressed storage — skipping.';
    RETURN;
END;
GO

/*
Skip if source table doesn't exist
*/
IF OBJECT_ID(N'collect.query_stats', N'U') IS NULL
BEGIN
    PRINT 'collect.query_stats does not exist — skipping.';
    RETURN;
END;
GO

PRINT '=== Migrating collect.query_stats to compressed LOB storage ===';
PRINT '';
GO

BEGIN TRY

    /*
    Step 1: Create the _new table with compressed column types
    */
    IF OBJECT_ID(N'collect.query_stats_new', N'U') IS NOT NULL
    BEGIN
        DROP TABLE collect.query_stats_new;
        PRINT 'Dropped existing collect.query_stats_new';
    END;

    CREATE TABLE
        collect.query_stats_new
    (
        collection_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        server_start_time datetime2(7) NOT NULL,
        object_type nvarchar(20) NOT NULL
            DEFAULT N'STATEMENT',
        database_name sysname NOT NULL,
        object_name sysname NULL,
        schema_name sysname NULL,
        sql_handle varbinary(64) NOT NULL,
        statement_start_offset integer NOT NULL,
        statement_end_offset integer NOT NULL,
        plan_generation_num bigint NOT NULL,
        plan_handle varbinary(64) NOT NULL,
        creation_time datetime2(7) NOT NULL,
        last_execution_time datetime2(7) NOT NULL,
        /*Raw cumulative values*/
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
        /*Delta calculations*/
        execution_count_delta bigint NULL,
        total_worker_time_delta bigint NULL,
        total_elapsed_time_delta bigint NULL,
        total_logical_reads_delta bigint NULL,
        total_physical_reads_delta bigint NULL,
        total_logical_writes_delta bigint NULL,
        sample_interval_seconds integer NULL,
        /*Analysis helpers - computed columns*/
        avg_rows AS
        (
            total_rows /
              NULLIF(execution_count, 0)
        ),
        avg_worker_time_ms AS
        (
            total_worker_time /
              NULLIF(execution_count, 0) / 1000.
        ),
        avg_elapsed_time_ms AS
        (
            total_elapsed_time /
              NULLIF(execution_count, 0) / 1000.
        ),
        avg_physical_reads AS
        (
            total_physical_reads /
              NULLIF(execution_count, 0)
        ),
        worker_time_per_second AS
        (
            total_worker_time_delta /
              NULLIF(sample_interval_seconds, 0) / 1000.
        ),
        /*Query text and execution plan (compressed with COMPRESS/DECOMPRESS)*/
        query_text varbinary(max) NULL,
        query_plan_text varbinary(max) NULL,
        /*Deduplication hash for skipping unchanged rows*/
        row_hash binary(32) NULL,
        CONSTRAINT
            PK_query_stats_new
        PRIMARY KEY CLUSTERED
            (collection_time, collection_id)
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    PRINT 'Created collect.query_stats_new';

    /*
    Step 2: Reseed IDENTITY to continue from the old table
    */
    DECLARE
        @max_id bigint;

    SELECT
        @max_id = ISNULL(MAX(collection_id), 0)
    FROM collect.query_stats;

    DBCC CHECKIDENT(N'collect.query_stats_new', RESEED, @max_id);

    PRINT 'Reseeded IDENTITY to ' + CAST(@max_id AS varchar(20));

    /*
    Step 3: Migrate data in batches with COMPRESS on LOB columns
    Omits query_plan xml (never populated, dropping it)
    Omits computed columns (avg_rows, avg_worker_time_ms, avg_elapsed_time_ms,
    avg_physical_reads, worker_time_per_second) — can't appear in OUTPUT
    */
    DECLARE
        @batch_size integer = 10000,
        @rows_moved bigint = 0,
        @batch_rows integer = 1;

    PRINT '';
    PRINT 'Migrating data in batches of ' + CAST(@batch_size AS varchar(10)) + '...';

    SET IDENTITY_INSERT collect.query_stats_new ON;

    WHILE @batch_rows > 0
    BEGIN
        DELETE TOP (@batch_size)
        FROM collect.query_stats
        OUTPUT
            deleted.collection_id,
            deleted.collection_time,
            deleted.server_start_time,
            deleted.object_type,
            deleted.database_name,
            deleted.object_name,
            deleted.schema_name,
            deleted.sql_handle,
            deleted.statement_start_offset,
            deleted.statement_end_offset,
            deleted.plan_generation_num,
            deleted.plan_handle,
            deleted.creation_time,
            deleted.last_execution_time,
            deleted.execution_count,
            deleted.total_worker_time,
            deleted.min_worker_time,
            deleted.max_worker_time,
            deleted.total_physical_reads,
            deleted.min_physical_reads,
            deleted.max_physical_reads,
            deleted.total_logical_writes,
            deleted.total_logical_reads,
            deleted.total_clr_time,
            deleted.total_elapsed_time,
            deleted.min_elapsed_time,
            deleted.max_elapsed_time,
            deleted.query_hash,
            deleted.query_plan_hash,
            deleted.total_rows,
            deleted.min_rows,
            deleted.max_rows,
            deleted.statement_sql_handle,
            deleted.statement_context_id,
            deleted.min_dop,
            deleted.max_dop,
            deleted.min_grant_kb,
            deleted.max_grant_kb,
            deleted.min_used_grant_kb,
            deleted.max_used_grant_kb,
            deleted.min_ideal_grant_kb,
            deleted.max_ideal_grant_kb,
            deleted.min_reserved_threads,
            deleted.max_reserved_threads,
            deleted.min_used_threads,
            deleted.max_used_threads,
            deleted.total_spills,
            deleted.min_spills,
            deleted.max_spills,
            deleted.execution_count_delta,
            deleted.total_worker_time_delta,
            deleted.total_elapsed_time_delta,
            deleted.total_logical_reads_delta,
            deleted.total_physical_reads_delta,
            deleted.total_logical_writes_delta,
            deleted.sample_interval_seconds,
            COMPRESS(deleted.query_text),
            COMPRESS(deleted.query_plan_text)
        INTO collect.query_stats_new
        (
            collection_id,
            collection_time,
            server_start_time,
            object_type,
            database_name,
            object_name,
            schema_name,
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
            execution_count_delta,
            total_worker_time_delta,
            total_elapsed_time_delta,
            total_logical_reads_delta,
            total_physical_reads_delta,
            total_logical_writes_delta,
            sample_interval_seconds,
            query_text,
            query_plan_text
        );

        SET @batch_rows = @@ROWCOUNT;
        SET @rows_moved += @batch_rows;

        IF @batch_rows > 0
        BEGIN
            RAISERROR(N'  Migrated %I64d rows so far...', 0, 1, @rows_moved) WITH NOWAIT;
        END;
    END;

    SET IDENTITY_INSERT collect.query_stats_new OFF;

    PRINT '';
    PRINT 'Migration complete: ' + CAST(@rows_moved AS varchar(20)) + ' rows moved';

    /*
    Step 4: Rename old -> _old, new -> original
    */
    EXEC sp_rename
        N'collect.query_stats',
        N'query_stats_old',
        N'OBJECT';

    /* Rename old table's PK first to free the name */
    EXEC sp_rename
        N'collect.query_stats_old.PK_query_stats',
        N'PK_query_stats_old',
        N'INDEX';

    EXEC sp_rename
        N'collect.query_stats_new',
        N'query_stats',
        N'OBJECT';

    EXEC sp_rename
        N'collect.query_stats.PK_query_stats_new',
        N'PK_query_stats',
        N'INDEX';

    PRINT '';
    PRINT 'Renamed tables: query_stats -> query_stats_old, query_stats_new -> query_stats';
    PRINT '';
    PRINT '=== collect.query_stats migration complete ===';
    PRINT '';
    PRINT 'The old table is preserved as collect.query_stats_old.';
    PRINT 'After verifying the migration, you can drop it:';
    PRINT '  DROP TABLE IF EXISTS collect.query_stats_old;';

END TRY
BEGIN CATCH
    PRINT '';
    PRINT '*** ERROR migrating collect.query_stats ***';
    PRINT 'Error ' + CAST(ERROR_NUMBER() AS varchar(10)) + ': ' + ERROR_MESSAGE();
    PRINT '';
    PRINT 'The original table has not been renamed.';
    PRINT 'If collect.query_stats_new exists, it contains partial data.';
    PRINT 'Review and resolve the error, then re-run this script.';
END CATCH;
GO
