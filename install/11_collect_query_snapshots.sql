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
Query snapshots collector
Uses sp_WhoIsActive to capture snapshots of currently executing queries
Creates daily tables (query_snapshots_YYYYMMDD) to manage data volume
Automatically creates views and manages retention
*/

IF OBJECT_ID(N'collect.query_snapshots_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.query_snapshots_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.query_snapshots_collector
(
    @procedure_database sysname = NULL, /*Database where sp_WhoIsActive is installed (NULL = search PerformanceMonitor then master)*/
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
        @retention_days integer,
        @whoisactive_database sysname = NULL,
        @destination_table sysname =
            N'query_snapshots_' +
            REPLACE
            (
                CONVERT
                (
                    date,
                    SYSDATETIME()
                ),
                N'-',
                N''
            ),
        @destination_schema sysname = N'collect',
        @destination_database sysname = N'PerformanceMonitor',
        @full_table_name sysname = N'',
        @schema nvarchar(max) = N'',
        @sql nvarchar(max) = N'',
        @error_message nvarchar(4000);

    /*
    Get retention days from collection schedule
    */
    SELECT
        @retention_days = cs.retention_days
    FROM config.collection_schedule AS cs
    WHERE cs.collector_name = N'query_snapshots_collector';

    /*
    Default to 30 days if not configured
    */
    IF @retention_days IS NULL
    BEGIN
        SET @retention_days = 30;
    END;

    BEGIN TRY
        /*
        Locate sp_WhoIsActive
        If user provided a database name, check there
        Otherwise search PerformanceMonitor first, then master
        */
        IF @procedure_database IS NOT NULL
        BEGIN
            SET @sql = N'
            IF OBJECT_ID(N''' + QUOTENAME(@procedure_database) + N'.dbo.sp_WhoIsActive'', N''P'') IS NOT NULL
            BEGIN
                SELECT @whoisactive_database = N''' + @procedure_database + N''';
            END;';

            EXECUTE sys.sp_executesql
                @sql,
                N'@whoisactive_database sysname OUTPUT',
                @whoisactive_database = @whoisactive_database OUTPUT;

            IF @whoisactive_database IS NULL
            BEGIN
                SET @error_message = N'sp_WhoIsActive not found in specified database ' + @procedure_database + N'. Please install it from https://github.com/amachanic/sp_whoisactive';
                RAISERROR(@error_message, 16, 1);
                RETURN;
            END;
        END;
        ELSE
        BEGIN
            IF OBJECT_ID(N'PerformanceMonitor.dbo.sp_WhoIsActive', N'P') IS NOT NULL
            BEGIN
                SET @whoisactive_database = N'PerformanceMonitor';
            END;
            ELSE IF OBJECT_ID(N'master.dbo.sp_WhoIsActive', N'P') IS NOT NULL
            BEGIN
                SET @whoisactive_database = N'master';
            END;
            ELSE
            BEGIN
                SET @error_message = N'sp_WhoIsActive is not installed in PerformanceMonitor or master. Please install it from https://github.com/amachanic/sp_whoisactive';
                RAISERROR(@error_message, 16, 1);
                RETURN;
            END;
        END;

        /*
        Build full destination table name
        */
        SET @full_table_name =
            QUOTENAME(@destination_database) +
            N'.' +
            QUOTENAME(@destination_schema) +
            N'.' +
            QUOTENAME(@destination_table);

        /*
        Create the daily table if it doesn't exist
        */
        IF OBJECT_ID(@full_table_name, N'U') IS NULL
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Table %s does not exist, creating from sp_WhoIsActive schema', 0, 1, @full_table_name) WITH NOWAIT;
            END;

            /*
            Call sp_WhoIsActive to get the table schema
            */
            SET @sql = N'
            EXECUTE ' + QUOTENAME(@whoisactive_database) + N'.dbo.sp_WhoIsActive
                @get_transaction_info = 1,
                @get_outer_command = 1,
                @get_plans = 1,
                @get_task_info = 2,
                @get_additional_info = 1,
                @find_block_leaders = 1,
                @get_memory_info = 1,
                @not_filter_type = ''database'',
                @not_filter = ''PerformanceMonitor'',
                @return_schema = 1,
                @schema = @schema OUTPUT;';

            EXECUTE sys.sp_executesql
                @sql,
                N'@schema nvarchar(max) OUTPUT',
                @schema = @schema OUTPUT;

            /*
            Replace placeholder with actual table name
            */
            SET @schema =
                REPLACE
                (
                    @schema,
                    N'<table_name>',
                    @full_table_name
                );

            EXECUTE sys.sp_executesql
                @schema;

            IF @debug = 1
            BEGIN
                RAISERROR(N'Created %s from sp_WhoIsActive schema', 0, 1, @full_table_name) WITH NOWAIT;
            END;

            /*
            Recreate view immediately so new daily table is visible
            */
            EXECUTE collect.query_snapshots_create_views;
        END;

        /*
        Collect currently executing queries using sp_WhoIsActive
        sp_WhoIsActive inserts by ordinal position, not column name.
        If sp_WhoIsActive was updated and column order changed,
        existing daily tables will have a schema mismatch.
        Detect this and recreate the table if needed.
        */
        SET @sql = N'
        EXECUTE ' + QUOTENAME(@whoisactive_database) + N'.dbo.sp_WhoIsActive
            @get_transaction_info = 1,
            @get_outer_command = 1,
            @get_plans = 1,
            @get_task_info = 2,
            @get_additional_info = 1,
            @find_block_leaders = 1,
            @get_memory_info = 1,
            @destination_table = ''' + @full_table_name + N''';';

        BEGIN TRY
            EXECUTE sys.sp_executesql
                @sql;
        END TRY
        BEGIN CATCH
            IF ERROR_NUMBER() = 257 /*Implicit conversion = column order mismatch*/
            BEGIN
                IF @debug = 1
                BEGIN
                    RAISERROR(N'Schema mismatch detected on %s, recreating table', 0, 1, @full_table_name) WITH NOWAIT;
                END;

                /*Drop the mismatched table*/
                SET @sql =
                    N'DROP TABLE ' +
                    @full_table_name +
                    N';';

                EXECUTE sys.sp_executesql
                    @sql;

                /*Recreate from current sp_WhoIsActive schema*/
                SET @schema = N'';

                SET @sql = N'
                EXECUTE ' + QUOTENAME(@whoisactive_database) + N'.dbo.sp_WhoIsActive
                    @get_transaction_info = 1,
                    @get_outer_command = 1,
                    @get_plans = 1,
                    @get_task_info = 2,
                    @get_additional_info = 1,
                    @find_block_leaders = 1,
                    @get_memory_info = 1,
                    @not_filter_type = ''database'',
                    @not_filter = ''PerformanceMonitor'',
                    @return_schema = 1,
                    @schema = @schema OUTPUT;';

                EXECUTE sys.sp_executesql
                    @sql,
                    N'@schema nvarchar(max) OUTPUT',
                    @schema = @schema OUTPUT;

                SET @schema =
                    REPLACE
                    (
                        @schema,
                        N'<table_name>',
                        @full_table_name
                    );

                EXECUTE sys.sp_executesql
                    @schema;

                /*Retry the insert*/
                SET @sql = N'
                EXECUTE ' + QUOTENAME(@whoisactive_database) + N'.dbo.sp_WhoIsActive
                    @get_transaction_info = 1,
                    @get_outer_command = 1,
                    @get_plans = 1,
                    @get_task_info = 2,
                    @get_additional_info = 1,
                    @find_block_leaders = 1,
                    @get_memory_info = 1,
                    @destination_table = ''' + @full_table_name + N''';';

                EXECUTE sys.sp_executesql
                    @sql;

                /*Recreate views for new schema*/
                EXECUTE collect.query_snapshots_create_views;
            END;
            ELSE
            BEGIN
                THROW;
            END;
        END CATCH;

        /*
        Get row count from last insertion
        */
        SET @sql = N'
        SELECT
            @rows_collected = COUNT_BIG(*)
        FROM ' + @full_table_name + N' AS qs
        WHERE qs.collection_time >= @start_time;';

        EXECUTE sys.sp_executesql
            @sql,
            N'@rows_collected bigint OUTPUT, @start_time datetime2(7)',
            @rows_collected = @rows_collected OUTPUT,
            @start_time = @start_time;

        /*
        Run daily maintenance tasks (view recreation and retention cleanup)
        Only run once per day - check if they've run in the last 24 hours
        */
        DECLARE
            @last_view_recreation datetime2(7),
            @last_retention_cleanup datetime2(7);

        SELECT TOP (1)
            @last_view_recreation = cl.collection_time
        FROM config.collection_log AS cl
        WHERE cl.collector_name = N'query_snapshots_create_views'
        AND   cl.collection_status = N'SUCCESS'
        ORDER BY
            cl.collection_time DESC;

        SELECT TOP (1)
            @last_retention_cleanup = cl.collection_time
        FROM config.collection_log AS cl
        WHERE cl.collector_name = N'query_snapshots_retention'
        AND   cl.collection_status = N'SUCCESS'
        ORDER BY
            cl.collection_time DESC;

        /*
        Recreate views if not run in last 24 hours
        */
        IF @last_view_recreation IS NULL OR @last_view_recreation < DATEADD(HOUR, -24, SYSDATETIME())
        BEGIN
            IF @debug = 1
            BEGIN
                DECLARE @last_view_msg nvarchar(30) = ISNULL(CONVERT(nvarchar(30), @last_view_recreation, 120), N'never');
                RAISERROR(N'Running query_snapshots_create_views (last run: %s)', 0, 1, @last_view_msg) WITH NOWAIT;
            END;

            EXECUTE collect.query_snapshots_create_views;
        END;

        /*
        Execute retention cleanup if not run in last 24 hours
        */
        IF @last_retention_cleanup IS NULL OR @last_retention_cleanup < DATEADD(HOUR, -24, SYSDATETIME())
        BEGIN
            IF @debug = 1
            BEGIN
                DECLARE @last_retention_msg nvarchar(30) = ISNULL(CONVERT(nvarchar(30), @last_retention_cleanup, 120), N'never');
                RAISERROR(N'Running query_snapshots_retention (last run: %s)', 0, 1, @last_retention_msg) WITH NOWAIT;
            END;

            EXECUTE collect.query_snapshots_retention;
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
            N'query_snapshots_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %d query snapshots', 0, 1, @rows_collected) WITH NOWAIT;
        END;

    END TRY
    BEGIN CATCH
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
            N'query_snapshots_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in query snapshots collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Query snapshots collector created successfully';
PRINT 'Uses sp_WhoIsActive to capture comprehensive snapshots of currently executing queries';
PRINT 'Creates daily tables (query_snapshots_YYYYMMDD) and auto-manages views and retention';
GO
