/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 2.1.0 to 2.2.0
Migrates collect.query_store_data to compressed LOB storage:
  - query_sql_text nvarchar(max) -> varbinary(max) via COMPRESS()
  - query_plan_text nvarchar(max) -> varbinary(max) via COMPRESS()
  - compilation_metrics xml -> varbinary(max) via COMPRESS(CAST(... AS nvarchar(max)))
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
Skip if already migrated (query_sql_text is already varbinary)
*/
IF EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'collect.query_store_data')
    AND   name = N'query_sql_text'
    AND   system_type_id = 165 /*varbinary*/
)
BEGIN
    PRINT 'collect.query_store_data already migrated to compressed storage — skipping.';
    RETURN;
END;
GO

/*
Skip if source table doesn't exist
*/
IF OBJECT_ID(N'collect.query_store_data', N'U') IS NULL
BEGIN
    PRINT 'collect.query_store_data does not exist — skipping.';
    RETURN;
END;
GO

PRINT '=== Migrating collect.query_store_data to compressed LOB storage ===';
PRINT '';
GO

BEGIN TRY

    /*
    Step 1: Create the _new table with compressed column types
    */
    IF OBJECT_ID(N'collect.query_store_data_new', N'U') IS NOT NULL
    BEGIN
        DROP TABLE collect.query_store_data_new;
        PRINT 'Dropped existing collect.query_store_data_new';
    END;

    CREATE TABLE
        collect.query_store_data_new
    (
        collection_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        database_name sysname NOT NULL,
        query_id bigint NOT NULL,
        plan_id bigint NOT NULL,
        execution_type_desc nvarchar(60) NULL,
        utc_first_execution_time datetimeoffset(7) NOT NULL,
        utc_last_execution_time datetimeoffset(7) NOT NULL,
        server_first_execution_time datetime2(7) NOT NULL,
        server_last_execution_time datetime2(7) NOT NULL,
        module_name nvarchar(261) NULL,
        query_sql_text varbinary(max) NULL,
        query_hash binary(8) NULL,
        /*Execution count*/
        count_executions bigint NOT NULL,
        /*Duration metrics (microseconds)*/
        avg_duration bigint NOT NULL,
        min_duration bigint NOT NULL,
        max_duration bigint NOT NULL,
        /*CPU time metrics (microseconds)*/
        avg_cpu_time bigint NOT NULL,
        min_cpu_time bigint NOT NULL,
        max_cpu_time bigint NOT NULL,
        /*Logical IO reads*/
        avg_logical_io_reads bigint NOT NULL,
        min_logical_io_reads bigint NOT NULL,
        max_logical_io_reads bigint NOT NULL,
        /*Logical IO writes*/
        avg_logical_io_writes bigint NOT NULL,
        min_logical_io_writes bigint NOT NULL,
        max_logical_io_writes bigint NOT NULL,
        /*Physical IO reads*/
        avg_physical_io_reads bigint NOT NULL,
        min_physical_io_reads bigint NOT NULL,
        max_physical_io_reads bigint NOT NULL,
        /*Number of physical IO reads - NULL on SQL 2016*/
        avg_num_physical_io_reads bigint NULL,
        min_num_physical_io_reads bigint NULL,
        max_num_physical_io_reads bigint NULL,
        /*CLR time (microseconds)*/
        avg_clr_time bigint NOT NULL,
        min_clr_time bigint NOT NULL,
        max_clr_time bigint NOT NULL,
        /*DOP (degree of parallelism)*/
        min_dop bigint NOT NULL,
        max_dop bigint NOT NULL,
        /*Memory grant (8KB pages)*/
        avg_query_max_used_memory bigint NOT NULL,
        min_query_max_used_memory bigint NOT NULL,
        max_query_max_used_memory bigint NOT NULL,
        /*Row count*/
        avg_rowcount bigint NOT NULL,
        min_rowcount bigint NOT NULL,
        max_rowcount bigint NOT NULL,
        /*Log bytes used*/
        avg_log_bytes_used bigint NULL,
        min_log_bytes_used bigint NULL,
        max_log_bytes_used bigint NULL,
        /*Tempdb space used (8KB pages)*/
        avg_tempdb_space_used bigint NULL,
        min_tempdb_space_used bigint NULL,
        max_tempdb_space_used bigint NULL,
        /*Plan information*/
        plan_type nvarchar(60) NULL,
        is_forced_plan bit NOT NULL,
        force_failure_count bigint NULL,
        last_force_failure_reason_desc nvarchar(128) NULL,
        plan_forcing_type nvarchar(60) NULL,
        compatibility_level smallint NULL,
        query_plan_text varbinary(max) NULL,
        compilation_metrics varbinary(max) NULL,
        query_plan_hash binary(8) NULL,
        /*Deduplication hash for skipping unchanged rows*/
        row_hash binary(32) NULL,
        CONSTRAINT
            PK_query_store_data_new
        PRIMARY KEY CLUSTERED
            (collection_time, collection_id)
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    PRINT 'Created collect.query_store_data_new';

    /*
    Step 2: Reseed IDENTITY to continue from the old table
    */
    DECLARE
        @max_id bigint;

    SELECT
        @max_id = ISNULL(MAX(collection_id), 0)
    FROM collect.query_store_data;

    DBCC CHECKIDENT(N'collect.query_store_data_new', RESEED, @max_id);

    PRINT 'Reseeded IDENTITY to ' + CAST(@max_id AS varchar(20));

    /*
    Step 3: Migrate data in batches with COMPRESS on LOB columns
    compilation_metrics is xml, so CAST to nvarchar(max) before COMPRESS
    */
    DECLARE
        @batch_size integer = 10000,
        @rows_moved bigint = 0,
        @batch_rows integer = 1;

    PRINT '';
    PRINT 'Migrating data in batches of ' + CAST(@batch_size AS varchar(10)) + '...';

    SET IDENTITY_INSERT collect.query_store_data_new ON;

    WHILE @batch_rows > 0
    BEGIN
        DELETE TOP (@batch_size)
        FROM collect.query_store_data
        OUTPUT
            deleted.collection_id,
            deleted.collection_time,
            deleted.database_name,
            deleted.query_id,
            deleted.plan_id,
            deleted.execution_type_desc,
            deleted.utc_first_execution_time,
            deleted.utc_last_execution_time,
            deleted.server_first_execution_time,
            deleted.server_last_execution_time,
            deleted.module_name,
            COMPRESS(deleted.query_sql_text),
            deleted.query_hash,
            deleted.count_executions,
            deleted.avg_duration,
            deleted.min_duration,
            deleted.max_duration,
            deleted.avg_cpu_time,
            deleted.min_cpu_time,
            deleted.max_cpu_time,
            deleted.avg_logical_io_reads,
            deleted.min_logical_io_reads,
            deleted.max_logical_io_reads,
            deleted.avg_logical_io_writes,
            deleted.min_logical_io_writes,
            deleted.max_logical_io_writes,
            deleted.avg_physical_io_reads,
            deleted.min_physical_io_reads,
            deleted.max_physical_io_reads,
            deleted.avg_num_physical_io_reads,
            deleted.min_num_physical_io_reads,
            deleted.max_num_physical_io_reads,
            deleted.avg_clr_time,
            deleted.min_clr_time,
            deleted.max_clr_time,
            deleted.min_dop,
            deleted.max_dop,
            deleted.avg_query_max_used_memory,
            deleted.min_query_max_used_memory,
            deleted.max_query_max_used_memory,
            deleted.avg_rowcount,
            deleted.min_rowcount,
            deleted.max_rowcount,
            deleted.avg_log_bytes_used,
            deleted.min_log_bytes_used,
            deleted.max_log_bytes_used,
            deleted.avg_tempdb_space_used,
            deleted.min_tempdb_space_used,
            deleted.max_tempdb_space_used,
            deleted.plan_type,
            deleted.is_forced_plan,
            deleted.force_failure_count,
            deleted.last_force_failure_reason_desc,
            deleted.plan_forcing_type,
            deleted.compatibility_level,
            COMPRESS(deleted.query_plan_text),
            COMPRESS(CAST(deleted.compilation_metrics AS nvarchar(max))),
            deleted.query_plan_hash
        INTO collect.query_store_data_new
        (
            collection_id,
            collection_time,
            database_name,
            query_id,
            plan_id,
            execution_type_desc,
            utc_first_execution_time,
            utc_last_execution_time,
            server_first_execution_time,
            server_last_execution_time,
            module_name,
            query_sql_text,
            query_hash,
            count_executions,
            avg_duration,
            min_duration,
            max_duration,
            avg_cpu_time,
            min_cpu_time,
            max_cpu_time,
            avg_logical_io_reads,
            min_logical_io_reads,
            max_logical_io_reads,
            avg_logical_io_writes,
            min_logical_io_writes,
            max_logical_io_writes,
            avg_physical_io_reads,
            min_physical_io_reads,
            max_physical_io_reads,
            avg_num_physical_io_reads,
            min_num_physical_io_reads,
            max_num_physical_io_reads,
            avg_clr_time,
            min_clr_time,
            max_clr_time,
            min_dop,
            max_dop,
            avg_query_max_used_memory,
            min_query_max_used_memory,
            max_query_max_used_memory,
            avg_rowcount,
            min_rowcount,
            max_rowcount,
            avg_log_bytes_used,
            min_log_bytes_used,
            max_log_bytes_used,
            avg_tempdb_space_used,
            min_tempdb_space_used,
            max_tempdb_space_used,
            plan_type,
            is_forced_plan,
            force_failure_count,
            last_force_failure_reason_desc,
            plan_forcing_type,
            compatibility_level,
            query_plan_text,
            compilation_metrics,
            query_plan_hash
        );

        SET @batch_rows = @@ROWCOUNT;
        SET @rows_moved += @batch_rows;

        IF @batch_rows > 0
        BEGIN
            RAISERROR(N'  Migrated %I64d rows so far...', 0, 1, @rows_moved) WITH NOWAIT;
        END;
    END;

    SET IDENTITY_INSERT collect.query_store_data_new OFF;

    PRINT '';
    PRINT 'Migration complete: ' + CAST(@rows_moved AS varchar(20)) + ' rows moved';

    /*
    Step 4: Rename old -> _old, new -> original
    */
    EXEC sp_rename
        N'collect.query_store_data',
        N'query_store_data_old',
        N'OBJECT';

    /* Rename old table's PK first to free the name */
    EXEC sp_rename
        N'collect.query_store_data_old.PK_query_store_data',
        N'PK_query_store_data_old',
        N'INDEX';

    EXEC sp_rename
        N'collect.query_store_data_new',
        N'query_store_data',
        N'OBJECT';

    EXEC sp_rename
        N'collect.query_store_data.PK_query_store_data_new',
        N'PK_query_store_data',
        N'INDEX';

    PRINT '';
    PRINT 'Renamed tables: query_store_data -> query_store_data_old, query_store_data_new -> query_store_data';
    PRINT '';
    PRINT '=== collect.query_store_data migration complete ===';
    PRINT '';
    PRINT 'The old table is preserved as collect.query_store_data_old.';
    PRINT 'After verifying the migration, you can drop it:';
    PRINT '  DROP TABLE IF EXISTS collect.query_store_data_old;';

END TRY
BEGIN CATCH
    PRINT '';
    PRINT '*** ERROR migrating collect.query_store_data ***';
    PRINT 'Error ' + CAST(ERROR_NUMBER() AS varchar(10)) + ': ' + ERROR_MESSAGE();
    PRINT '';
    PRINT 'The original table has not been renamed.';
    PRINT 'If collect.query_store_data_new exists, it contains partial data.';
    PRINT 'Review and resolve the error, then re-run this script.';
END CATCH;
GO
