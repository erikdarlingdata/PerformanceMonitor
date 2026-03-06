/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

For usage, licensing, and support:
https://github.com/erikdarlingdata/DarlingData

Ensure Collection Table - Performance Monitor
Erik Darling - erik@erikdarling.com
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

IF OBJECT_ID(N'config.ensure_collection_table', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE config.ensure_collection_table AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    config.ensure_collection_table
(
    @table_name sysname, /*Name of table to ensure exists (without schema)*/
    @debug bit = 0 /*Print debugging information*/
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    /*
    Variable declarations
    */
    DECLARE
        @start_time datetime2(7) = SYSDATETIME(),
        @full_table_name sysname = N'collect.' + @table_name,
        @error_message nvarchar(2048) = N'';

    BEGIN TRY
        /*
        Check if table already exists
        */
        IF OBJECT_ID(@full_table_name, N'U') IS NOT NULL
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Table %s already exists, checking for schema upgrades', 0, 1, @full_table_name) WITH NOWAIT;
            END;

            /*
            Schema upgrade: Add is_processed column to raw XML tables if missing
            This handles upgrades from older versions that did not track processing state
            */
            IF @table_name IN (N'blocked_process_xml', N'deadlock_xml')
            BEGIN
                IF NOT EXISTS
                (
                    SELECT
                        1/0
                    FROM sys.columns AS c
                    JOIN sys.tables AS t
                      ON t.object_id = c.object_id
                    JOIN sys.schemas AS s
                      ON s.schema_id = t.schema_id
                    WHERE s.name = N'collect'
                    AND   t.name = @table_name
                    AND   c.name = N'is_processed'
                )
                BEGIN
                    DECLARE
                        @alter_sql nvarchar(500) = N'ALTER TABLE ' + @full_table_name + N' ADD is_processed bit NOT NULL DEFAULT 0;';

                    EXECUTE sys.sp_executesql
                        @alter_sql;

                    IF @debug = 1
                    BEGIN
                        RAISERROR(N'Added is_processed column to %s', 0, 1, @full_table_name) WITH NOWAIT;
                    END;

                    INSERT INTO
                        config.collection_log
                    (
                        collector_name,
                        collection_status,
                        error_message
                    )
                    VALUES
                    (
                        N'ensure_collection_table',
                        N'SCHEMA_UPGRADE',
                        N'Added is_processed column to ' + @full_table_name
                    );
                END;
            END;

            RETURN;
        END;

        /*
        Log that table is missing before attempting to create
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
            N'ensure_collection_table',
            N'TABLE_MISSING',
            0,
            0,
            N'Table ' + @full_table_name + N' does not exist, attempting to create'
        );

        /*
        Create table based on name using IF/ELSE blocks
        Each block contains the full CREATE TABLE statement for the specific table
        */
        IF @table_name = N'wait_stats'
        BEGIN
            CREATE TABLE
        collect.wait_stats
    (
        collection_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL 
            DEFAULT SYSDATETIME(),
        server_start_time datetime2(7) NOT NULL,
        wait_type nvarchar(60) NOT NULL,
        waiting_tasks_count bigint NOT NULL,
        wait_time_ms bigint NOT NULL,
        signal_wait_time_ms bigint NOT NULL,
        /*Delta calculations*/
        waiting_tasks_count_delta bigint NULL,
        wait_time_ms_delta bigint NULL,
        signal_wait_time_ms_delta bigint NULL,
        sample_interval_seconds integer NULL,
        /*Analysis helpers*/
        wait_time_ms_per_second AS 
        (
            wait_time_ms_delta / 
              NULLIF(sample_interval_seconds, 0)
        ),
        signal_wait_time_ms_per_second AS 
        (
            signal_wait_time_ms_delta / 
              NULLIF(sample_interval_seconds, 0)
        ),
        CONSTRAINT 
            PK_wait_stats 
        PRIMARY KEY CLUSTERED 
            (collection_time, collection_id) 
        WITH 
            (DATA_COMPRESSION = PAGE)
    );
        END;
        ELSE IF @table_name = N'query_stats'
        BEGIN
            CREATE TABLE
        collect.query_stats
    (
        collection_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        server_start_time datetime2(7) NOT NULL,
        object_type nvarchar(20) NOT NULL 
            DEFAULT N'STATEMENT', /*PROCEDURE, TRIGGER, FUNCTION*/
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
        /*Query text and execution plan*/
        query_text nvarchar(max) NULL,
        query_plan_text nvarchar(max) NULL,
        query_plan xml NULL,
        CONSTRAINT
            PK_query_stats
        PRIMARY KEY CLUSTERED
            (collection_time, collection_id)
        WITH
            (DATA_COMPRESSION = PAGE)
    );
        END;
        ELSE IF @table_name = N'memory_stats'
        BEGIN
            CREATE TABLE
        collect.memory_stats
    (
        collection_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL 
            DEFAULT SYSDATETIME(),
        /*Memory clerks summary*/
        buffer_pool_mb decimal(19,2) NOT NULL,
        plan_cache_mb decimal(19,2) NOT NULL,
        other_memory_mb decimal(19,2) NOT NULL,
        total_memory_mb decimal(19,2) NOT NULL,
        /*Process memory*/
        physical_memory_in_use_mb decimal(19,2) NOT NULL,
        available_physical_memory_mb decimal(19,2) NOT NULL,
        memory_utilization_percentage integer NOT NULL,
        /*Server and target memory*/
        total_physical_memory_mb decimal(19,2) NULL,
        committed_target_memory_mb decimal(19,2) NULL,
        /*Pressure warnings*/
        buffer_pool_pressure_warning bit NOT NULL DEFAULT 0,
        plan_cache_pressure_warning bit NOT NULL DEFAULT 0,
        /*Analysis helpers - computed columns*/
        buffer_pool_percentage AS
        (
            buffer_pool_mb * 100.0 /
              NULLIF(total_memory_mb, 0)
        ),
        plan_cache_percentage AS
        (
            plan_cache_mb * 100.0 /
              NULLIF(total_memory_mb, 0)
        ),
        CONSTRAINT
            PK_memory_stats
        PRIMARY KEY CLUSTERED
            (collection_time, collection_id)
        WITH
            (DATA_COMPRESSION = PAGE)
    );
        END;
        ELSE IF @table_name = N'memory_pressure_events'
        BEGIN
            CREATE TABLE
        collect.memory_pressure_events
    (
        collection_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        sample_time datetime2(7) NOT NULL,
        memory_notification nvarchar(100) NOT NULL,
        memory_indicators_process integer NOT NULL,
        memory_indicators_system integer NOT NULL,

        CONSTRAINT
            PK_memory_pressure_events
        PRIMARY KEY CLUSTERED
            (collection_time, collection_id)
        WITH
            (DATA_COMPRESSION = PAGE)
    );
        END;
        ELSE IF @table_name = N'deadlock_xml'
        BEGIN
            CREATE TABLE
        collect.deadlock_xml
    (
        id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        event_time datetime2(7) NULL,
        deadlock_xml xml NOT NULL,
        is_processed bit NOT NULL DEFAULT 0,
        CONSTRAINT
            PK_deadlock_xml
        PRIMARY KEY CLUSTERED
            (collection_time, id)
        WITH
            (DATA_COMPRESSION = PAGE)
    );
        END;
        ELSE IF @table_name = N'blocked_process_xml'
        BEGIN
            CREATE TABLE
        collect.blocked_process_xml
    (
        id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        event_time datetime2(7) NULL,
        blocked_process_xml xml NOT NULL,
        is_processed bit NOT NULL DEFAULT 0,
        CONSTRAINT
            PK_blocked_process_xml
        PRIMARY KEY CLUSTERED
            (collection_time, id)
        WITH
            (DATA_COMPRESSION = PAGE)
    );
        END;
        ELSE IF @table_name = N'procedure_stats'
        BEGIN
            CREATE TABLE
        collect.procedure_stats
    (
        collection_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        server_start_time datetime2(7) NOT NULL,
        object_type nvarchar(20) NOT NULL, /*PROCEDURE, TRIGGER, FUNCTION*/
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
        /*Execution plan*/
        query_plan_text nvarchar(max) NULL,
        query_plan xml NULL,
        CONSTRAINT
            PK_procedure_stats
        PRIMARY KEY CLUSTERED
            (collection_time, collection_id)
        WITH
            (DATA_COMPRESSION = PAGE)
    );
        END;
        ELSE IF @table_name = N'query_snapshots'
        BEGIN
            SELECT
                @table_name = @table_name;
            /*
            Table: collect.query_snapshots
            Purpose: sp_WhoIsActive query snapshots
            Collection Frequency: Every minute
            Type: Snapshot
            Note: This table is created dynamically by sp_WhoIsActive on first collection.
                  Daily tables are created with pattern: query_snapshots_YYYYMMDD
                  Schema matches sp_WhoIsActive output structure.
                  Do not create statically - sp_WhoIsActive defines the schema.
            */
            -- Table created dynamically by query_snapshots_collector on first run
            -- No static table creation needed
        END;
        ELSE IF @table_name = N'query_store_data'
        BEGIN
            CREATE TABLE
        collect.query_store_data
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
        query_sql_text nvarchar(max) NULL,
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
        query_plan_text nvarchar(max) NULL,
        compilation_metrics xml NULL,
        query_plan_hash binary(8) NULL,
        CONSTRAINT
            PK_query_store_data
        PRIMARY KEY CLUSTERED
            (collection_time, collection_id)
        WITH
            (DATA_COMPRESSION = PAGE)
    );
        END;
        ELSE IF @table_name = N'trace_analysis'
        BEGIN
            CREATE TABLE
        collect.trace_analysis
    (
        analysis_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
        trace_file_name nvarchar(260) NOT NULL,
        event_class integer NOT NULL,
        event_name nvarchar(50) NOT NULL,
        database_name nvarchar(128) NULL,
        login_name nvarchar(128) NULL,
        nt_user_name nvarchar(128) NULL,
        application_name nvarchar(256) NULL,
        host_name nvarchar(128) NULL,
        spid integer NULL,
        duration_ms bigint NULL,
        cpu_ms bigint NULL,
        reads bigint NULL,
        writes bigint NULL,
        row_counts bigint NULL,
        start_time datetime2(7) NULL,
        end_time datetime2(7) NULL,
        sql_text nvarchar(max) NULL,
        object_id bigint NULL,
        client_process_id integer NULL,
        session_context nvarchar(500) NULL,
        CONSTRAINT PK_trace_analysis PRIMARY KEY CLUSTERED (collection_time, analysis_id) WITH (DATA_COMPRESSION = PAGE)
    );
        END;
        ELSE IF @table_name = N'default_trace_events'
        BEGIN
            CREATE TABLE
        collect.default_trace_events
    (   
        event_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
        event_time datetime2(7) NOT NULL,
        event_name nvarchar(128) NOT NULL,
        event_class integer NOT NULL,
        spid integer NULL,
        database_name sysname NULL,
        database_id integer NULL,
        login_name nvarchar(128) NULL,
        host_name nvarchar(128) NULL,
        application_name nvarchar(256) NULL,
        server_name nvarchar(128) NULL,
        object_name sysname NULL,
        filename nvarchar(260) NULL,
        integer_data bigint NULL,
        integer_data_2 bigint NULL,
        text_data nvarchar(max) NULL,
        binary_data varbinary(max) NULL,
        session_login_name nvarchar(128) NULL,
        error_number integer NULL,
        severity integer NULL,
        state integer NULL,
        event_sequence bigint NULL,
        is_system bit NULL,
        request_id integer NULL,
        duration_us bigint NULL,
        end_time datetime2(7) NULL,
        CONSTRAINT PK_default_trace_events
        PRIMARY KEY CLUSTERED
        (collection_time, event_id) WITH (DATA_COMPRESSION = PAGE)
    );
        END;
        ELSE IF @table_name = N'file_io_stats'
        BEGIN
            CREATE TABLE
        collect.file_io_stats
    (
        collection_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
        server_start_time datetime2(7) NOT NULL,
        database_id integer NOT NULL,
        database_name sysname NULL,
        file_id integer NOT NULL,
        file_name sysname NULL,
        file_type_desc nvarchar(60) NULL,
        physical_name nvarchar(260) NULL,
        size_on_disk_bytes bigint NULL,
        num_of_reads bigint NULL,
        num_of_bytes_read bigint NULL,
        io_stall_read_ms bigint NULL,
        num_of_writes bigint NULL,
        num_of_bytes_written bigint NULL,
        io_stall_write_ms bigint NULL,
        io_stall_ms bigint NULL,
        io_stall_queued_read_ms bigint NULL,
        io_stall_queued_write_ms bigint NULL,
        sample_ms bigint NULL,
        /*Delta columns calculated by framework*/
        num_of_reads_delta bigint NULL,
        num_of_bytes_read_delta bigint NULL,
        io_stall_read_ms_delta bigint NULL,
        num_of_writes_delta bigint NULL,
        num_of_bytes_written_delta bigint NULL,
        io_stall_write_ms_delta bigint NULL,
        io_stall_ms_delta bigint NULL,
        io_stall_queued_read_ms_delta bigint NULL,
        io_stall_queued_write_ms_delta bigint NULL,
        sample_ms_delta bigint NULL,
        CONSTRAINT PK_file_io_stats PRIMARY KEY CLUSTERED (collection_time, collection_id) WITH (DATA_COMPRESSION = PAGE)
    );
        END;
        ELSE IF @table_name = N'memory_grant_stats'
        BEGIN
            CREATE TABLE
        collect.memory_grant_stats
    (
        collection_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
        server_start_time datetime2(7) NOT NULL,
        resource_semaphore_id smallint NOT NULL,
        pool_id integer NOT NULL,
        target_memory_mb decimal(19,2) NULL,
        max_target_memory_mb decimal(19,2) NULL,
        total_memory_mb decimal(19,2) NULL,
        available_memory_mb decimal(19,2) NULL,
        granted_memory_mb decimal(19,2) NULL,
        used_memory_mb decimal(19,2) NULL,
        grantee_count integer NULL,
        waiter_count integer NULL,
        timeout_error_count bigint NULL,
        forced_grant_count bigint NULL,
        /*Delta columns calculated by framework*/
        timeout_error_count_delta bigint NULL,
        forced_grant_count_delta bigint NULL,
        sample_interval_seconds integer NULL,
        CONSTRAINT
        PK_memory_grant_stats
        PRIMARY KEY CLUSTERED
            (collection_time, collection_id)
        WITH
            (DATA_COMPRESSION = PAGE)
    );
        END;
        ELSE IF @table_name = N'cpu_scheduler_stats'
        BEGIN
            CREATE TABLE
        collect.cpu_scheduler_stats
    (
        collection_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
        max_workers_count integer NULL,
        scheduler_count integer NULL,
        cpu_count integer NULL,
        total_runnable_tasks_count integer NULL,
        total_work_queue_count integer NULL,
        total_current_workers_count integer NULL,
        avg_runnable_tasks_count decimal(38,2) NULL,
        total_active_request_count integer NULL,
        total_queued_request_count integer NULL,
        total_blocked_task_count integer NULL,
        total_active_parallel_thread_count integer NULL,
        runnable_request_count integer NULL,
        total_request_count integer NULL,
        runnable_percent decimal(38,2) NULL,
        /*Pressure warnings*/
        worker_thread_exhaustion_warning bit NULL,
        runnable_tasks_warning bit NULL,
        blocked_tasks_warning bit NULL,
        queued_requests_warning bit NULL,
        /*OS Memory metrics from sys.dm_os_sys_memory*/
        total_physical_memory_kb bigint NULL,
        available_physical_memory_kb bigint NULL,
        system_memory_state_desc nvarchar(120) NULL,
        physical_memory_pressure_warning bit NULL,
        /*NUMA node metrics from sys.dm_os_nodes*/
        total_node_count integer NULL,
        nodes_online_count integer NULL,
        offline_cpu_count integer NULL,
        offline_cpu_warning bit NULL,
        CONSTRAINT PK_cpu_scheduler_stats PRIMARY KEY CLUSTERED (collection_time, collection_id) WITH (DATA_COMPRESSION = PAGE)    );
        END;
        ELSE IF @table_name = N'memory_clerks_stats'
        BEGIN
            CREATE TABLE
        collect.memory_clerks_stats
    (
        collection_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
        server_start_time datetime2(7) NOT NULL,
        clerk_type nvarchar(60) NOT NULL,
        memory_node_id smallint NOT NULL,
        /*Raw cumulative values*/
        pages_kb bigint NULL,
        virtual_memory_reserved_kb bigint NULL,
        virtual_memory_committed_kb bigint NULL,
        awe_allocated_kb bigint NULL,
        shared_memory_reserved_kb bigint NULL,
        shared_memory_committed_kb bigint NULL,
        /*Delta calculations*/
        pages_kb_delta bigint NULL,
        virtual_memory_reserved_kb_delta bigint NULL,
        virtual_memory_committed_kb_delta bigint NULL,
        awe_allocated_kb_delta bigint NULL,
        shared_memory_reserved_kb_delta bigint NULL,
        shared_memory_committed_kb_delta bigint NULL,
        sample_interval_seconds integer NULL,
        CONSTRAINT PK_memory_clerks_stats PRIMARY KEY CLUSTERED (collection_time, collection_id) WITH (DATA_COMPRESSION = PAGE)
    );
        END;
        ELSE IF @table_name = N'perfmon_stats'
        BEGIN
            CREATE TABLE
        collect.perfmon_stats
    (
        collection_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
        server_start_time datetime2(7) NOT NULL,
        object_name sysname NOT NULL,
        counter_name sysname NOT NULL,
        instance_name sysname NOT NULL,
        cntr_value bigint NOT NULL,
        cntr_type bigint NOT NULL,
        /*Delta column calculated by framework for cumulative counters*/
        cntr_value_delta bigint NULL,
        sample_interval_seconds integer NULL,
        /*Analysis helper - per-second rate*/
        cntr_value_per_second AS
        (
            cntr_value_delta /
              NULLIF(sample_interval_seconds, 0)
        ),
        CONSTRAINT PK_perfmon_stats PRIMARY KEY CLUSTERED (collection_time, collection_id) WITH (DATA_COMPRESSION = PAGE)
    );
        END;
        ELSE IF @table_name = N'cpu_utilization_stats'
        BEGIN
            CREATE TABLE
        collect.cpu_utilization_stats
    (
        collection_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
        sample_time datetime2(7) NOT NULL,
        sqlserver_cpu_utilization integer NOT NULL,
        other_process_cpu_utilization integer NOT NULL,
        total_cpu_utilization AS (sqlserver_cpu_utilization + other_process_cpu_utilization) PERSISTED,
        CONSTRAINT PK_cpu_utilization_stats PRIMARY KEY CLUSTERED (collection_time, collection_id) WITH (DATA_COMPRESSION = PAGE)
    );
        END;
        ELSE IF @table_name = N'blocking_deadlock_stats'
        BEGIN
            CREATE TABLE
        collect.blocking_deadlock_stats
    (
        collection_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        database_name sysname NOT NULL,
        /*Blocking metrics from sp_HumanEventsBlockViewer*/
        blocking_event_count bigint NOT NULL
            DEFAULT 0,
        total_blocking_duration_ms bigint NOT NULL
            DEFAULT 0,
        max_blocking_duration_ms bigint NOT NULL
            DEFAULT 0,
        avg_blocking_duration_ms decimal(19,2) NULL,
        /*Deadlock metrics from sp_BlitzLock*/
        deadlock_count bigint NOT NULL
            DEFAULT 0,
        total_deadlock_wait_time_ms bigint NOT NULL
            DEFAULT 0,
        victim_count bigint NOT NULL
            DEFAULT 0,
        /*Delta calculations*/
        blocking_event_count_delta bigint NULL,
        total_blocking_duration_ms_delta bigint NULL,
        max_blocking_duration_ms_delta bigint NULL,
        deadlock_count_delta bigint NULL,
        total_deadlock_wait_time_ms_delta bigint NULL,
        victim_count_delta bigint NULL,
        sample_interval_seconds integer NULL,
        CONSTRAINT
            PK_blocking_deadlock_stats
        PRIMARY KEY CLUSTERED
            (collection_time, collection_id)
        WITH
            (DATA_COMPRESSION = PAGE)
    );
        END;
        ELSE IF @table_name = N'latch_stats'
        BEGIN
            CREATE TABLE
                collect.latch_stats
            (
                collection_id bigint IDENTITY NOT NULL,
                collection_time datetime2(7) NOT NULL
                    DEFAULT SYSDATETIME(),
                server_start_time datetime2(7) NOT NULL,
                latch_class nvarchar(60) NOT NULL,
                waiting_requests_count bigint NOT NULL,
                wait_time_ms bigint NOT NULL,
                max_wait_time_ms bigint NOT NULL,
                /*Delta calculations*/
                waiting_requests_count_delta bigint NULL,
                wait_time_ms_delta bigint NULL,
                max_wait_time_ms_delta bigint NULL,
                sample_interval_seconds integer NULL,
                /*Analysis helpers*/
                wait_time_ms_per_second AS
                (
                    wait_time_ms_delta /
                      NULLIF(sample_interval_seconds, 0)
                ),
                waiting_requests_count_per_second AS
                (
                    waiting_requests_count_delta /
                      NULLIF(sample_interval_seconds, 0)
                ),
                CONSTRAINT
                    PK_latch_stats
                PRIMARY KEY CLUSTERED
                    (collection_time, collection_id)
                WITH
                    (DATA_COMPRESSION = PAGE)
            );
        END;
        ELSE IF @table_name = N'spinlock_stats'
        BEGIN
            CREATE TABLE
                collect.spinlock_stats
            (
                collection_id bigint IDENTITY NOT NULL,
                collection_time datetime2(7) NOT NULL
                    DEFAULT SYSDATETIME(),
                server_start_time datetime2(7) NOT NULL,
                spinlock_name nvarchar(256) NOT NULL,
                collisions bigint NOT NULL,
                spins bigint NOT NULL,
                spins_per_collision decimal(38,2) NOT NULL,
                sleep_time bigint NOT NULL,
                backoffs bigint NOT NULL,
                /*Delta calculations*/
                collisions_delta bigint NULL,
                spins_delta bigint NULL,
                sleep_time_delta bigint NULL,
                backoffs_delta bigint NULL,
                sample_interval_seconds integer NULL,
                /*Analysis helpers*/
                collisions_per_second AS
                (
                    collisions_delta /
                      NULLIF(sample_interval_seconds, 0)
                ),
                spins_per_second AS
                (
                    spins_delta /
                      NULLIF(sample_interval_seconds, 0)
                ),
                CONSTRAINT
                    PK_spinlock_stats
                PRIMARY KEY CLUSTERED
                    (collection_time, collection_id)
                WITH
                    (DATA_COMPRESSION = PAGE)
            );
        END;
        ELSE IF @table_name = N'tempdb_stats'
        BEGIN
            CREATE TABLE
                collect.tempdb_stats
            (
                collection_id bigint IDENTITY NOT NULL,
                collection_time datetime2(7) NOT NULL
                    DEFAULT SYSDATETIME(),
                /*File space usage from dm_db_file_space_usage*/
                user_object_reserved_page_count bigint NOT NULL,
                internal_object_reserved_page_count bigint NOT NULL,
                version_store_reserved_page_count bigint NOT NULL,
                mixed_extent_page_count bigint NOT NULL,
                unallocated_extent_page_count bigint NOT NULL,
                /*Calculated MB values*/
                user_object_reserved_mb AS
                    (user_object_reserved_page_count * 8 / 1024),
                internal_object_reserved_mb AS
                    (internal_object_reserved_page_count * 8 / 1024),
                version_store_reserved_mb AS
                    (version_store_reserved_page_count * 8 / 1024),
                total_reserved_mb AS
                    ((user_object_reserved_page_count + internal_object_reserved_page_count + version_store_reserved_page_count) * 8 / 1024),
                unallocated_mb AS
                    (unallocated_extent_page_count * 8 / 1024),
                /*Task space usage - top consumer*/
                top_task_user_objects_mb integer NULL,
                top_task_internal_objects_mb integer NULL,
                top_task_total_mb integer NULL,
                top_task_session_id integer NULL,
                top_task_request_id integer NULL,
                /*Session counts*/
                total_sessions_using_tempdb integer NOT NULL,
                sessions_with_user_objects integer NOT NULL,
                sessions_with_internal_objects integer NOT NULL,
                /*Warning flags*/
                version_store_high_warning bit NOT NULL,
                allocation_contention_warning bit NOT NULL,
                CONSTRAINT
                    PK_tempdb_stats
                PRIMARY KEY CLUSTERED
                    (collection_time, collection_id)
                WITH
                    (DATA_COMPRESSION = PAGE)
            );
        END;
        ELSE IF @table_name = N'plan_cache_stats'
        BEGIN
            CREATE TABLE
                collect.plan_cache_stats
            (
                collection_id bigint IDENTITY NOT NULL,
                collection_time datetime2(7) NOT NULL
                    DEFAULT SYSDATETIME(),
                cacheobjtype nvarchar(34) NOT NULL,
                objtype nvarchar(16) NOT NULL,
                total_plans integer NOT NULL,
                total_size_mb integer NOT NULL,
                single_use_plans integer NOT NULL,
                single_use_size_mb integer NOT NULL,
                multi_use_plans integer NOT NULL,
                multi_use_size_mb integer NOT NULL,
                avg_use_count decimal(38,2) NOT NULL,
                avg_size_kb integer NOT NULL,
                oldest_plan_create_time datetime2(7) NULL,
                CONSTRAINT
                    PK_plan_cache_stats
                PRIMARY KEY CLUSTERED
                    (collection_time, collection_id)
                WITH
                    (DATA_COMPRESSION = PAGE)
            );
        END;
        ELSE IF @table_name = N'session_stats'
        BEGIN
            CREATE TABLE
                collect.session_stats
            (
                collection_id bigint IDENTITY NOT NULL,
                collection_time datetime2(7) NOT NULL
                    DEFAULT SYSDATETIME(),
                total_sessions integer NOT NULL,
                running_sessions integer NOT NULL,
                sleeping_sessions integer NOT NULL,
                background_sessions integer NOT NULL,
                dormant_sessions integer NOT NULL,
                idle_sessions_over_30min integer NOT NULL,
                sessions_waiting_for_memory integer NOT NULL,
                databases_with_connections integer NOT NULL,
                top_application_name nvarchar(128) NULL,
                top_application_connections integer NULL,
                top_host_name nvarchar(128) NULL,
                top_host_connections integer NULL,
                CONSTRAINT
                    PK_session_stats
                PRIMARY KEY CLUSTERED
                    (collection_time, collection_id)
                WITH
                    (DATA_COMPRESSION = PAGE)
            );
        END;
        ELSE IF @table_name = N'waiting_tasks'
        BEGIN
            CREATE TABLE
                collect.waiting_tasks
            (
                collection_id bigint IDENTITY NOT NULL,
                collection_time datetime2(7) NOT NULL
                    DEFAULT SYSDATETIME(),
                session_id integer NOT NULL,
                wait_type nvarchar(60) NOT NULL,
                wait_duration_ms bigint NOT NULL,
                blocking_session_id integer NOT NULL,
                resource_description nvarchar(1000) NULL,
                database_id integer NULL,
                database_name sysname NULL,
                query_text nvarchar(max) NULL,
                statement_text nvarchar(max) NULL,
                query_plan nvarchar(max) NULL,
                sql_handle varbinary(64) NULL,
                plan_handle varbinary(64) NULL,
                request_status nvarchar(30) NULL,
                command nvarchar(32) NULL,
                cpu_time_ms integer NULL,
                total_elapsed_time_ms integer NULL,
                logical_reads bigint NULL,
                writes bigint NULL,
                row_count bigint NULL,
                CONSTRAINT
                    PK_waiting_tasks
                PRIMARY KEY CLUSTERED
                    (collection_time, collection_id)
                WITH
                    (DATA_COMPRESSION = PAGE)
            );

        END;
        ELSE IF @table_name = N'running_jobs'
        BEGIN
            CREATE TABLE
                collect.running_jobs
            (
                collection_id bigint IDENTITY NOT NULL,
                collection_time datetime2(7) NOT NULL
                    DEFAULT SYSDATETIME(),
                server_start_time datetime2(7) NOT NULL,
                job_name sysname NOT NULL,
                job_id uniqueidentifier NOT NULL,
                job_enabled bit NOT NULL,
                start_time datetime2(7) NOT NULL,
                current_duration_seconds bigint NOT NULL,
                avg_duration_seconds bigint NULL,
                p95_duration_seconds bigint NULL,
                successful_run_count bigint NULL,
                is_running_long bit NOT NULL DEFAULT 0,
                percent_of_average decimal(10,1) NULL,
                CONSTRAINT
                    PK_running_jobs
                PRIMARY KEY CLUSTERED
                    (collection_time, collection_id)
                WITH
                    (DATA_COMPRESSION = PAGE)
            );

        END;
        ELSE
        BEGIN
            SET @error_message = N'Unknown table name: ' + @table_name + N'. Valid table names are: wait_stats, query_stats, memory_stats, memory_pressure_events, deadlock_xml, blocked_process_xml, procedure_stats, query_snapshots, query_store_data, trace_analysis, default_trace_events, file_io_stats, memory_grant_stats, cpu_scheduler_stats, memory_clerks_stats, perfmon_stats, cpu_utilization_stats, blocking_deadlock_stats, latch_stats, spinlock_stats, tempdb_stats, plan_cache_stats, session_stats, waiting_tasks, running_jobs';
            RAISERROR(@error_message, 16, 1);
            RETURN;
        END;

        /*
        Log successful table creation
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
            N'ensure_collection_table',
            N'TABLE_CREATED',
            0,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            N'Successfully created table ' + @full_table_name
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Successfully created table %s', 0, 1, @full_table_name) WITH NOWAIT;
        END;

    END TRY
    BEGIN CATCH
        SET @error_message = ERROR_MESSAGE();

        /*
        Log errors to collection log
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
            N'ensure_collection_table',
            N'ERROR',
            0,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        IF @@TRANCOUNT > 0
        BEGIN
            ROLLBACK;
        END;

        THROW;
    END CATCH;
END;
GO

PRINT 'Ensure collection table procedure created successfully';
GO

/*
Schema upgrade: Add is_processed column to raw XML tables if missing
This runs during installation to ensure the column exists before
processors (files 23/25) reference it in their procedure definitions.
The ensure_collection_table procedure also handles this for runtime upgrades.
*/
IF OBJECT_ID(N'collect.blocked_process_xml', N'U') IS NOT NULL
AND NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns AS c
    JOIN sys.tables AS t
      ON t.object_id = c.object_id
    JOIN sys.schemas AS s
      ON s.schema_id = t.schema_id
    WHERE s.name = N'collect'
    AND   t.name = N'blocked_process_xml'
    AND   c.name = N'is_processed'
)
BEGIN
    ALTER TABLE collect.blocked_process_xml ADD is_processed bit NOT NULL DEFAULT 0;
    PRINT 'Added is_processed column to collect.blocked_process_xml';
END;
GO

IF OBJECT_ID(N'collect.deadlock_xml', N'U') IS NOT NULL
AND NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns AS c
    JOIN sys.tables AS t
      ON t.object_id = c.object_id
    JOIN sys.schemas AS s
      ON s.schema_id = t.schema_id
    WHERE s.name = N'collect'
    AND   t.name = N'deadlock_xml'
    AND   c.name = N'is_processed'
)
BEGIN
    ALTER TABLE collect.deadlock_xml ADD is_processed bit NOT NULL DEFAULT 0;
    PRINT 'Added is_processed column to collect.deadlock_xml';
END;
GO
