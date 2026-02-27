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
Query Store data collector
Collects query statistics from Query Store enabled databases
Mimics query_stats collector pattern with min/max/last values
*/

IF OBJECT_ID(N'collect.query_store_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.query_store_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.query_store_collector
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
        @last_collection_time datetimeoffset(7) = NULL,
        @collection_interval_minutes integer = NULL,
        @cutoff_time datetimeoffset(7) = NULL,
        @sql nvarchar(max) = N'',
        @error_message nvarchar(4000),
        @new bit = 0,
        @plan_type_available bit = 0, /*plan_type_desc requires SQL Server 2022+*/
        @engine integer =
            CONVERT
            (
                integer,
                SERVERPROPERTY('ENGINEEDITION')
            ),
        @product_version integer =
            CONVERT
            (
                integer,
                PARSENAME
                (
                    CONVERT
                    (
                        sysname,
                        SERVERPROPERTY('PRODUCTVERSION')
                    ),
                    4
                )
            );

        /*
        @new = 1 for SQL Server 2017+ (version 14+)
        Controls: avg_num_physical_io_reads, avg_log_bytes_used, avg_tempdb_space_used,
                  plan_forcing_type_desc (all added in SQL Server 2017)
        */
        IF
        (
           @product_version > 13
        OR @engine IN (5, 8)
        )
        BEGIN
           SELECT
               @new = 1;
        END;

        /*
        @plan_type_available = 1 for SQL Server 2022+ (version 16+)
        Controls: plan_type_desc (added in SQL Server 2022)
        */
        IF @product_version >= 16
        BEGIN
            SELECT
                @plan_type_available = 1;
        END;

    BEGIN TRY

        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.query_store_data', N'U') IS NULL
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
                N'query_store_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.query_store_data does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'query_store_data',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.query_store_data', N'U') IS NULL
            BEGIN
                RAISERROR(N'Table collect.query_store_data still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        First run detection - collect last 1 hour of history if this is the first execution
        */
        IF NOT EXISTS (SELECT 1/0 FROM collect.query_store_data)
        AND NOT EXISTS (SELECT 1/0 FROM config.collection_log WHERE collector_name = N'query_store_collector')
        BEGIN
            SET @cutoff_time = TODATETIMEOFFSET(DATEADD(HOUR, -1, SYSUTCDATETIME()), 0);

            IF @debug = 1
            BEGIN
                RAISERROR(N'First run detected - collecting last 1 hour of Query Store data', 0, 1) WITH NOWAIT;
            END;
        END;
        ELSE
        BEGIN
            /*
            Get last collection time for this collector
            */
            SELECT
                @last_collection_time = MAX(qsd.utc_last_execution_time)
            FROM collect.query_store_data AS qsd;

            /*
            Get collection interval from schedule table
            */
            SELECT
                @collection_interval_minutes = cs.frequency_minutes
            FROM config.collection_schedule AS cs
            WHERE cs.collector_name = N'query_store_collector'
            AND   cs.enabled = 1;

            /*
            Calculate cutoff time
            If we have a previous collection, use that time
            Otherwise use the configured interval (or default to 15 minutes)
            Convert to datetimeoffset for Query Store compatibility
            */
            SELECT
                @cutoff_time =
                    TODATETIMEOFFSET
                    (
                        ISNULL
                        (
                            @last_collection_time,
                            DATEADD(MINUTE, -ISNULL(@collection_interval_minutes, 15), SYSUTCDATETIME())
                        ),
                        0
                    );
        END;

        IF @debug = 1
        BEGIN
            DECLARE @debug_msg nvarchar(200) = N'Collecting Query Store data with cutoff time: ' + CONVERT(nvarchar(50), @cutoff_time, 127);
            RAISERROR(@debug_msg, 0, 1) WITH NOWAIT;
        END;

        /*
        Read collection flags for query text and plans
        */
        DECLARE
            @collect_query bit = 1,
            @collect_plan bit = 1;

        SELECT
            @collect_query = cs.collect_query,
            @collect_plan = cs.collect_plan
        FROM config.collection_schedule AS cs
        WHERE cs.collector_name = N'query_store_collector';

        /*
        Create temp table to hold Query Store data from all databases
        */
        CREATE TABLE
            #query_store_data
        (
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
            count_executions bigint NOT NULL,
            avg_duration bigint NOT NULL,
            min_duration bigint NOT NULL,
            max_duration bigint NOT NULL,
            avg_cpu_time bigint NOT NULL,
            min_cpu_time bigint NOT NULL,
            max_cpu_time bigint NOT NULL,
            avg_logical_io_reads bigint NOT NULL,
            min_logical_io_reads bigint NOT NULL,
            max_logical_io_reads bigint NOT NULL,
            avg_logical_io_writes bigint NOT NULL,
            min_logical_io_writes bigint NOT NULL,
            max_logical_io_writes bigint NOT NULL,
            avg_physical_io_reads bigint NOT NULL,
            min_physical_io_reads bigint NOT NULL,
            max_physical_io_reads bigint NOT NULL,
            avg_num_physical_io_reads bigint NULL,
            min_num_physical_io_reads bigint NULL,
            max_num_physical_io_reads bigint NULL,
            avg_clr_time bigint NOT NULL,
            min_clr_time bigint NOT NULL,
            max_clr_time bigint NOT NULL,
            min_dop bigint NOT NULL,
            max_dop bigint NOT NULL,
            avg_query_max_used_memory bigint NOT NULL,
            min_query_max_used_memory bigint NOT NULL,
            max_query_max_used_memory bigint NOT NULL,
            avg_rowcount bigint NOT NULL,
            min_rowcount bigint NOT NULL,
            max_rowcount bigint NOT NULL,
            avg_log_bytes_used bigint NULL,
            min_log_bytes_used bigint NULL,
            max_log_bytes_used bigint NULL,
            avg_tempdb_space_used bigint NULL,
            min_tempdb_space_used bigint NULL,
            max_tempdb_space_used bigint NULL,
            plan_type nvarchar(60) NULL,
            plan_forcing_type nvarchar(60) NULL,
            is_forced_plan bit NOT NULL,
            force_failure_count bigint NULL,
            last_force_failure_reason_desc nvarchar(128) NULL,
            compatibility_level smallint NULL,
            query_plan_text nvarchar(max) NULL,
            compilation_metrics xml NULL,
            query_plan_hash binary(8) NULL
        );

        /*
        Build list of databases where Query Store is actually enabled.
        Uses sys.database_query_store_options.actual_state instead of
        sys.databases.is_query_store_on, which can be out of sync on Azure SQL DB.
        */
        DECLARE
            @database_name sysname,
            @qs_check_sql NVARCHAR(500);

        DECLARE
            @qs_databases TABLE (name sysname);

        DECLARE @db_check_cursor CURSOR

            SET @db_check_cursor =
                CURSOR
                LOCAL
                FAST_FORWARD
            FOR
            SELECT
                d.name
            FROM sys.databases AS d
            WHERE d.state_desc = N'ONLINE'
            AND   d.database_id > 4
            AND   d.is_read_only = 0
            AND   d.name <> N'PerformanceMonitor'
            AND   d.database_id < 32761 /*exclude contained AG system databases*/
            AND   d.database_id NOT IN
                  (
                      SELECT
                          d2.database_id
                      FROM sys.databases AS d2
                      JOIN sys.availability_replicas AS r
                        ON d2.replica_id = r.replica_id
                      WHERE NOT EXISTS
                            (
                                SELECT
                                    1/0
                                FROM sys.dm_hadr_availability_group_states AS s
                                WHERE s.primary_replica = r.replica_server_name
                            )
                      AND   r.secondary_role_allow_connections_desc = N'READ_ONLY'
                      AND   r.replica_server_name = @@SERVERNAME
                  )
            OPTION(RECOMPILE);

        OPEN @db_check_cursor;

        FETCH NEXT
        FROM @db_check_cursor
        INTO @database_name;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            BEGIN TRY
                SET @qs_check_sql =
                    N'USE ' + QUOTENAME(@database_name) + N';
                    SELECT ' + QUOTENAME(@database_name, '''') + N'
                    WHERE EXISTS
                    (
                        SELECT
                            1
                        FROM sys.database_query_store_options
                        WHERE actual_state > 0
                    );';

                INSERT @qs_databases (name)
                EXEC(@qs_check_sql);
            END TRY
            BEGIN CATCH
            END CATCH;

            FETCH NEXT
            FROM @db_check_cursor
            INTO @database_name;
        END;

        CLOSE @db_check_cursor;
        DEALLOCATE @db_check_cursor;

        /*
        Loop through databases where Query Store is enabled
        */
        DECLARE @database_cursor CURSOR

            SET @database_cursor =
                CURSOR
                LOCAL
                FAST_FORWARD
            FOR
            SELECT
                q.name
            FROM @qs_databases AS q
            ORDER BY
                q.name;

        OPEN @database_cursor;

        FETCH NEXT
        FROM @database_cursor
        INTO @database_name;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Collecting Query Store data from database: %s', 0, 1, @database_name) WITH NOWAIT;
            END;

            /*
            Collect Query Store data for this database
            */
           SET @sql = N'

            SELECT
                o.object_id,
                object_name =
                    QUOTENAME(s.name) +
                    N''.'' +
                    QUOTENAME(o.name)
            INTO #objects
            FROM ' + QUOTENAME(@database_name) + N'.sys.objects AS o
            JOIN ' + QUOTENAME(@database_name) + N'.sys.schemas AS s
              ON s.schema_id = o.schema_id
            WHERE o.type IN (''P'', ''FN'', ''FS'', ''FT'', ''TF'');

            SELECT
                database_name = @database_name,
                query_id = p.query_id,
                plan_id = rs.plan_id,
                rs.execution_type_desc,
                utc_first_execution_time = rs.first_execution_time,
                server_first_execution_time = DATEADD(MINUTE, DATEDIFF(MINUTE, GETUTCDATE(), SYSDATETIME()), CONVERT(datetime2(7), rs.first_execution_time)),
                utc_last_execution_time = rs.last_execution_time,
                server_last_execution_time = DATEADD(MINUTE, DATEDIFF(MINUTE, GETUTCDATE(), SYSDATETIME()), CONVERT(datetime2(7), rs.last_execution_time)),
                module_name =
                    CASE
                        WHEN q.object_id = 0 THEN N''Adhoc''
                        WHEN q.object_id > 0
                        AND  o.object_id IS NULL
                        THEN N''Unknown''
                        WHEN q.object_id > 0
                        AND  o.object_id IS NOT NULL
                        THEN o.object_name
                    END,';

            IF @collect_query = 1
            BEGIN
                SET @sql += N'
                query_sql_text = qt.query_sql_text,';
            END;
            ELSE
            BEGIN
                SET @sql += N'
                query_sql_text = NULL,';
            END;

            SET @sql += N'
                query_hash = q.query_hash,
                count_executions = rs.count_executions,
                avg_duration = rs.avg_duration,
                min_duration = rs.min_duration,
                max_duration = rs.max_duration,
                avg_cpu_time = rs.avg_cpu_time,
                min_cpu_time = rs.min_cpu_time,
                max_cpu_time = rs.max_cpu_time,
                avg_logical_io_reads = rs.avg_logical_io_reads,
                min_logical_io_reads = rs.min_logical_io_reads,
                max_logical_io_reads = rs.max_logical_io_reads,
                avg_logical_io_writes = rs.avg_logical_io_writes,
                min_logical_io_writes = rs.min_logical_io_writes,
                max_logical_io_writes = rs.max_logical_io_writes,
                avg_physical_io_reads = rs.avg_physical_io_reads,
                min_physical_io_reads = rs.min_physical_io_reads,
                max_physical_io_reads = rs.max_physical_io_reads,';

            IF @new = 1
            BEGIN
                SET @sql += N'
                rs.avg_num_physical_io_reads,
                rs.min_num_physical_io_reads,
                rs.max_num_physical_io_reads,';
            END;
            ELSE
            BEGIN
                SET @sql += N'
                avg_num_physical_io_reads = NULL,
                min_num_physical_io_reads = NULL,
                max_num_physical_io_reads = NULL,';
            END;

            SET @sql += N'
                avg_clr_time = rs.avg_clr_time,
                min_clr_time = rs.min_clr_time,
                max_clr_time = rs.max_clr_time,
                min_dop = rs.min_dop,
                max_dop = rs.max_dop,
                avg_query_max_used_memory = rs.avg_query_max_used_memory,
                min_query_max_used_memory = rs.min_query_max_used_memory,
                max_query_max_used_memory = rs.max_query_max_used_memory,
                avg_rowcount = rs.avg_rowcount,
                min_rowcount = rs.min_rowcount,
                max_rowcount = rs.max_rowcount,';

            /*
            SQL Server 2017+ columns from sys.query_store_runtime_stats
            */
            IF @new = 1
            BEGIN
                SET @sql += N'
                avg_log_bytes_used = rs.avg_log_bytes_used,
                min_log_bytes_used = rs.min_log_bytes_used,
                max_log_bytes_used = rs.max_log_bytes_used,
                avg_tempdb_space_used = rs.avg_tempdb_space_used,
                min_tempdb_space_used = rs.min_tempdb_space_used,
                max_tempdb_space_used = rs.max_tempdb_space_used,
                plan_forcing_type = p.plan_forcing_type_desc,';
            END;
            ELSE
            BEGIN
                SET @sql += N'
                avg_log_bytes_used = NULL,
                min_log_bytes_used = NULL,
                max_log_bytes_used = NULL,
                avg_tempdb_space_used = NULL,
                min_tempdb_space_used = NULL,
                max_tempdb_space_used = NULL,
                plan_forcing_type = NULL,';
            END;

            /*
            SQL Server 2022+ columns from sys.query_store_plan
            plan_type_desc was added in SQL Server 2022
            */
            IF @plan_type_available = 1
            BEGIN
                SET @sql += N'
                plan_type = p.plan_type_desc,';
            END;
            ELSE
            BEGIN
                SET @sql += N'
                plan_type = NULL,';
            END;

            SET @sql += N'                
                is_forced_plan = p.is_forced_plan,
                p.force_failure_count,
                p.last_force_failure_reason_desc,
                p.compatibility_level,';

            IF @collect_plan = 1
            BEGIN
                SET @sql += N'
                query_plan_text = CONVERT(nvarchar(max), p.query_plan),';
            END;
            ELSE
            BEGIN
                SET @sql += N'
                query_plan_text = NULL,';
            END;

            SET @sql += N'
                compilation_metrics =
                    (
                        SELECT
                            plan_count_compiles = p.count_compiles,
                            query_count_compiles = q.count_compiles,
                            plan_avg_compile_duration_ms = CONVERT(decimal(38, 2), p.avg_compile_duration / 1000.),
                            query_avg_compile_duration_ms = CONVERT(decimal(38, 2), q.avg_compile_duration / 1000.),
                            query_avg_optimize_duration_ms = CONVERT(decimal(38, 2), q.avg_optimize_duration / 1000.),
                            query_avg_optimize_cpu_time_ms = CONVERT(decimal(38, 2), q.avg_optimize_cpu_time / 1000.),
                            query_avg_compile_memory_mb = CONVERT(decimal(38, 2), q.avg_compile_memory_kb / 1000.),
                            query_max_compile_memory_mb = CONVERT(decimal(38, 2), q.max_compile_memory_kb / 1000.)
                        FOR
                            XML
                            PATH(''compilation_metrics''),
                            TYPE
                    ),
                query_plan_hash = p.query_plan_hash
            FROM ' + QUOTENAME(@database_name) + N'.sys.query_store_runtime_stats AS rs
            JOIN ' + QUOTENAME(@database_name) + N'.sys.query_store_plan AS p
              ON p.plan_id = rs.plan_id
            JOIN ' + QUOTENAME(@database_name) + N'.sys.query_store_query AS q
              ON q.query_id = p.query_id
            JOIN ' + QUOTENAME(@database_name) + N'.sys.query_store_query_text AS qt
              ON qt.query_text_id = q.query_text_id
            LEFT JOIN #objects AS o
              ON q.object_id = o.object_id
            WHERE rs.last_execution_time >= @cutoff_time;';

            IF @debug = 1
            BEGIN
                PRINT @sql;
            END;

            BEGIN TRY
                INSERT INTO
                    #query_store_data
                (
                    database_name,
                    query_id,
                    plan_id,
                    execution_type_desc,
                    utc_first_execution_time,
                    server_first_execution_time,
                    utc_last_execution_time,
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
                    plan_forcing_type,
                    is_forced_plan,
                    force_failure_count,
                    last_force_failure_reason_desc,
                    compatibility_level,
                    query_plan_text,
                    compilation_metrics,
                    query_plan_hash
                )
                EXECUTE sys.sp_executesql
                    @sql,
                  N'@cutoff_time datetimeoffset(7),
                    @database_name sysname',
                    @cutoff_time = @cutoff_time,
                    @database_name = @database_name;
            END TRY
            BEGIN CATCH
                SET @error_message = ERROR_MESSAGE();

                /*
                Log error for this database but continue with next database
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
                    N'query_store_collector',
                    N'ERROR_DATABASE',
                    DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
                    N'Database: ' + @database_name + N' - ' + @error_message
                );

                IF @debug = 1
                BEGIN
                    RAISERROR(N'Error collecting from database %s: %s', 0, 1, @database_name, @error_message) WITH NOWAIT;
                END;
            END CATCH;

            FETCH NEXT
            FROM @database_cursor
            INTO @database_name;
        END;

        /*
        Insert collected data into the permanent table
        */
        INSERT INTO
            collect.query_store_data
        (
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
        )
        SELECT
            qsd.database_name,
            qsd.query_id,
            qsd.plan_id,
            qsd.execution_type_desc,
            qsd.utc_first_execution_time,
            qsd.utc_last_execution_time,
            qsd.server_first_execution_time,
            qsd.server_last_execution_time,
            qsd.module_name,
            qsd.query_sql_text,
            qsd.query_hash,
            qsd.count_executions,
            qsd.avg_duration,
            qsd.min_duration,
            qsd.max_duration,
            qsd.avg_cpu_time,
            qsd.min_cpu_time,
            qsd.max_cpu_time,
            qsd.avg_logical_io_reads,
            qsd.min_logical_io_reads,
            qsd.max_logical_io_reads,
            qsd.avg_logical_io_writes,
            qsd.min_logical_io_writes,
            qsd.max_logical_io_writes,
            qsd.avg_physical_io_reads,
            qsd.min_physical_io_reads,
            qsd.max_physical_io_reads,
            qsd.avg_num_physical_io_reads,
            qsd.min_num_physical_io_reads,
            qsd.max_num_physical_io_reads,
            qsd.avg_clr_time,
            qsd.min_clr_time,
            qsd.max_clr_time,
            qsd.min_dop,
            qsd.max_dop,
            qsd.avg_query_max_used_memory,
            qsd.min_query_max_used_memory,
            qsd.max_query_max_used_memory,
            qsd.avg_rowcount,
            qsd.min_rowcount,
            qsd.max_rowcount,
            qsd.avg_log_bytes_used,
            qsd.min_log_bytes_used,
            qsd.max_log_bytes_used,
            qsd.avg_tempdb_space_used,
            qsd.min_tempdb_space_used,
            qsd.max_tempdb_space_used,
            qsd.plan_type,
            qsd.is_forced_plan,
            qsd.force_failure_count,
            qsd.last_force_failure_reason_desc,
            qsd.plan_forcing_type,
            qsd.compatibility_level,
            qsd.query_plan_text,
            qsd.compilation_metrics,
            qsd.query_plan_hash
        FROM #query_store_data AS qsd
        OPTION(RECOMPILE, KEEPFIXED PLAN);

        SET @rows_collected = ROWCOUNT_BIG();

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
            N'query_store_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %d Query Store rows', 0, 1, @rows_collected) WITH NOWAIT;
        END;
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
            N'query_store_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in Query Store collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Query Store collector created successfully';
PRINT 'Collects comprehensive runtime statistics from all Query Store enabled databases';
GO
