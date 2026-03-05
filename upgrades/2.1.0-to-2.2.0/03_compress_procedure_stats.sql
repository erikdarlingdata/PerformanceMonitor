/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 2.1.0 to 2.2.0
Migrates collect.procedure_stats to compressed LOB storage:
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
Skip if already migrated (query_plan_text is already varbinary)
*/
IF EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'collect.procedure_stats')
    AND   name = N'query_plan_text'
    AND   system_type_id = 165 /*varbinary*/
)
BEGIN
    PRINT 'collect.procedure_stats already migrated to compressed storage — skipping.';
    RETURN;
END;
GO

/*
Skip if source table doesn't exist
*/
IF OBJECT_ID(N'collect.procedure_stats', N'U') IS NULL
BEGIN
    PRINT 'collect.procedure_stats does not exist — skipping.';
    RETURN;
END;
GO

PRINT '=== Migrating collect.procedure_stats to compressed LOB storage ===';
PRINT '';
GO

BEGIN TRY

    /*
    Step 1: Create the _new table with compressed column types
    */
    IF OBJECT_ID(N'collect.procedure_stats_new', N'U') IS NOT NULL
    BEGIN
        DROP TABLE collect.procedure_stats_new;
        PRINT 'Dropped existing collect.procedure_stats_new';
    END;

    CREATE TABLE
        collect.procedure_stats_new
    (
        collection_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
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
        /*Raw cumulative values*/
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
        /*Delta calculations*/
        execution_count_delta bigint NULL,
        total_worker_time_delta bigint NULL,
        total_elapsed_time_delta bigint NULL,
        total_logical_reads_delta bigint NULL,
        total_physical_reads_delta bigint NULL,
        total_logical_writes_delta bigint NULL,
        sample_interval_seconds integer NULL,
        /*Analysis helpers - computed columns*/
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
        /*Execution plan (compressed with COMPRESS/DECOMPRESS)*/
        query_plan_text varbinary(max) NULL,
        /*Deduplication hash for skipping unchanged rows*/
        row_hash binary(32) NULL,
        CONSTRAINT
            PK_procedure_stats_new
        PRIMARY KEY CLUSTERED
            (collection_time, collection_id)
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    PRINT 'Created collect.procedure_stats_new';

    /*
    Step 2: Reseed IDENTITY to continue from the old table
    */
    DECLARE
        @max_id bigint;

    SELECT
        @max_id = ISNULL(MAX(collection_id), 0)
    FROM collect.procedure_stats;

    DBCC CHECKIDENT(N'collect.procedure_stats_new', RESEED, @max_id);

    PRINT 'Reseeded IDENTITY to ' + CAST(@max_id AS varchar(20));

    /*
    Step 3: Migrate data in batches with COMPRESS on LOB columns
    Omits query_plan xml (never populated, dropping it)
    Omits computed columns (avg_worker_time_ms, avg_elapsed_time_ms,
    avg_physical_reads, worker_time_per_second) — can't appear in OUTPUT
    */
    DECLARE
        @batch_size integer = 10000,
        @rows_moved bigint = 0,
        @batch_rows integer = 1;

    PRINT '';
    PRINT 'Migrating data in batches of ' + CAST(@batch_size AS varchar(10)) + '...';

    SET IDENTITY_INSERT collect.procedure_stats_new ON;

    WHILE @batch_rows > 0
    BEGIN
        DELETE TOP (@batch_size)
        FROM collect.procedure_stats
        OUTPUT
            deleted.collection_id,
            deleted.collection_time,
            deleted.server_start_time,
            deleted.object_type,
            deleted.database_name,
            deleted.object_id,
            deleted.object_name,
            deleted.schema_name,
            deleted.type_desc,
            deleted.sql_handle,
            deleted.plan_handle,
            deleted.cached_time,
            deleted.last_execution_time,
            deleted.execution_count,
            deleted.total_worker_time,
            deleted.min_worker_time,
            deleted.max_worker_time,
            deleted.total_elapsed_time,
            deleted.min_elapsed_time,
            deleted.max_elapsed_time,
            deleted.total_logical_reads,
            deleted.min_logical_reads,
            deleted.max_logical_reads,
            deleted.total_physical_reads,
            deleted.min_physical_reads,
            deleted.max_physical_reads,
            deleted.total_logical_writes,
            deleted.min_logical_writes,
            deleted.max_logical_writes,
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
            COMPRESS(deleted.query_plan_text)
        INTO collect.procedure_stats_new
        (
            collection_id,
            collection_time,
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
            execution_count_delta,
            total_worker_time_delta,
            total_elapsed_time_delta,
            total_logical_reads_delta,
            total_physical_reads_delta,
            total_logical_writes_delta,
            sample_interval_seconds,
            query_plan_text
        );

        SET @batch_rows = @@ROWCOUNT;
        SET @rows_moved += @batch_rows;

        IF @batch_rows > 0
        BEGIN
            RAISERROR(N'  Migrated %I64d rows so far...', 0, 1, @rows_moved) WITH NOWAIT;
        END;
    END;

    SET IDENTITY_INSERT collect.procedure_stats_new OFF;

    PRINT '';
    PRINT 'Migration complete: ' + CAST(@rows_moved AS varchar(20)) + ' rows moved';

    /*
    Step 4: Rename old -> _old, new -> original
    */
    EXEC sp_rename
        N'collect.procedure_stats',
        N'procedure_stats_old',
        N'OBJECT';

    /* Rename old table's PK first to free the name */
    EXEC sp_rename
        N'collect.procedure_stats_old.PK_procedure_stats',
        N'PK_procedure_stats_old',
        N'INDEX';

    EXEC sp_rename
        N'collect.procedure_stats_new',
        N'procedure_stats',
        N'OBJECT';

    EXEC sp_rename
        N'collect.procedure_stats.PK_procedure_stats_new',
        N'PK_procedure_stats',
        N'INDEX';

    PRINT '';
    PRINT 'Renamed tables: procedure_stats -> procedure_stats_old, procedure_stats_new -> procedure_stats';
    PRINT '';
    PRINT '=== collect.procedure_stats migration complete ===';
    PRINT '';
    PRINT 'The old table is preserved as collect.procedure_stats_old.';
    PRINT 'After verifying the migration, you can drop it:';
    PRINT '  DROP TABLE IF EXISTS collect.procedure_stats_old;';

END TRY
BEGIN CATCH
    PRINT '';
    PRINT '*** ERROR migrating collect.procedure_stats ***';
    PRINT 'Error ' + CAST(ERROR_NUMBER() AS varchar(10)) + ': ' + ERROR_MESSAGE();
    PRINT '';
    PRINT 'The original table has not been renamed.';
    PRINT 'If collect.procedure_stats_new exists, it contains partial data.';
    PRINT 'Review and resolve the error, then re-run this script.';
END CATCH;
GO
