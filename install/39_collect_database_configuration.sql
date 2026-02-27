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
Database Configuration Collector
Collects database-level configuration settings from sys.databases and sys.database_scoped_configurations
Only logs changes from previous collection to track configuration drift
Uses dynamic SQL for database scoped configurations to avoid hardcoding configuration names
*/

IF OBJECT_ID(N'collect.database_configuration_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.database_configuration_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.database_configuration_collector
(
    @debug bit = 0 /*print debug information*/
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE
        @collection_time datetime2(7) = SYSDATETIME(),
        @rows_inserted integer = 0,
        @error_message nvarchar(4000),
        @database_name sysname,
        @database_id integer,
        @sql nvarchar(max) = N'';

    BEGIN TRY
        IF @debug = 1
        BEGIN
            RAISERROR(N'Starting database configuration collection', 0, 1) WITH NOWAIT;
        END;

        /*
        Create temp table for current database configuration snapshot
        */
        CREATE TABLE
            #current_database_config
        (
            database_id integer NOT NULL,
            database_name sysname NOT NULL,
            setting_type varchar(50) NOT NULL,
            setting_name nvarchar(128) NOT NULL,
            setting_value sql_variant NULL
        );

        /*
        Collect database properties from sys.databases
        */
        INSERT
            #current_database_config
        WITH
            (TABLOCK)
        (
            database_id,
            database_name,
            setting_type,
            setting_name,
            setting_value
        )
        SELECT
            database_id = d.database_id,
            database_name = d.name,
            setting_type = 'DATABASE_PROPERTY',
            setting_name = property_name,
            setting_value = property_value
        FROM sys.databases AS d
        CROSS APPLY
        (
            VALUES
                (N'compatibility_level', CONVERT(sql_variant, d.compatibility_level)),
                (N'collation_name', CONVERT(sql_variant, d.collation_name)),
                (N'recovery_model_desc', CONVERT(sql_variant, d.recovery_model_desc)),
                (N'page_verify_option_desc', CONVERT(sql_variant, d.page_verify_option_desc)),
                (N'is_auto_close_on', CONVERT(sql_variant, d.is_auto_close_on)),
                (N'is_auto_shrink_on', CONVERT(sql_variant, d.is_auto_shrink_on)),
                (N'is_auto_create_stats_on', CONVERT(sql_variant, d.is_auto_create_stats_on)),
                (N'is_auto_update_stats_on', CONVERT(sql_variant, d.is_auto_update_stats_on)),
                (N'is_auto_update_stats_async_on', CONVERT(sql_variant, d.is_auto_update_stats_async_on)),
                (N'is_ansi_null_default_on', CONVERT(sql_variant, d.is_ansi_null_default_on)),
                (N'is_ansi_nulls_on', CONVERT(sql_variant, d.is_ansi_nulls_on)),
                (N'is_ansi_padding_on', CONVERT(sql_variant, d.is_ansi_padding_on)),
                (N'is_ansi_warnings_on', CONVERT(sql_variant, d.is_ansi_warnings_on)),
                (N'is_arithabort_on', CONVERT(sql_variant, d.is_arithabort_on)),
                (N'is_concat_null_yields_null_on', CONVERT(sql_variant, d.is_concat_null_yields_null_on)),
                (N'is_numeric_roundabort_on', CONVERT(sql_variant, d.is_numeric_roundabort_on)),
                (N'is_quoted_identifier_on', CONVERT(sql_variant, d.is_quoted_identifier_on)),
                (N'is_recursive_triggers_on', CONVERT(sql_variant, d.is_recursive_triggers_on)),
                (N'is_cursor_close_on_commit_on', CONVERT(sql_variant, d.is_cursor_close_on_commit_on)),
                (N'is_local_cursor_default', CONVERT(sql_variant, d.is_local_cursor_default)),
                (N'is_fulltext_enabled', CONVERT(sql_variant, d.is_fulltext_enabled)),
                (N'is_trustworthy_on', CONVERT(sql_variant, d.is_trustworthy_on)),
                (N'is_db_chaining_on', CONVERT(sql_variant, d.is_db_chaining_on)),
                (N'is_parameterization_forced', CONVERT(sql_variant, d.is_parameterization_forced)),
                (N'is_master_key_encrypted_by_server', CONVERT(sql_variant, d.is_master_key_encrypted_by_server)),
                (N'is_read_committed_snapshot_on', CONVERT(sql_variant, d.is_read_committed_snapshot_on)),
                (N'is_honor_broker_priority_on', CONVERT(sql_variant, d.is_honor_broker_priority_on)),
                (N'is_encrypted', CONVERT(sql_variant, d.is_encrypted)),
                (N'is_query_store_on', CONVERT(sql_variant, d.is_query_store_on)),
                (N'snapshot_isolation_state_desc', CONVERT(sql_variant, d.snapshot_isolation_state_desc)),
                (N'state_desc', CONVERT(sql_variant, d.state_desc)),
                (N'user_access_desc', CONVERT(sql_variant, d.user_access_desc)),
                (N'is_read_only', CONVERT(sql_variant, d.is_read_only)),
                (N'is_in_standby', CONVERT(sql_variant, d.is_in_standby)),
                (N'is_cleanly_shutdown', CONVERT(sql_variant, d.is_cleanly_shutdown)),
                (N'target_recovery_time_in_seconds', CONVERT(sql_variant, d.target_recovery_time_in_seconds)),
                (N'delayed_durability_desc', CONVERT(sql_variant, d.delayed_durability_desc)),
                (N'is_cdc_enabled', CONVERT(sql_variant, d.is_cdc_enabled)),
                (N'is_broker_enabled', CONVERT(sql_variant, d.is_broker_enabled)),
                (N'log_reuse_wait_desc', CONVERT(sql_variant, d.log_reuse_wait_desc)),
                (N'is_date_correlation_on', CONVERT(sql_variant, d.is_date_correlation_on)),
                (N'is_published', CONVERT(sql_variant, d.is_published)),
                (N'is_subscribed', CONVERT(sql_variant, d.is_subscribed)),
                (N'is_merge_published', CONVERT(sql_variant, d.is_merge_published)),
                (N'is_distributor', CONVERT(sql_variant, d.is_distributor)),
                (N'is_sync_with_backup', CONVERT(sql_variant, d.is_sync_with_backup))
        ) AS properties (property_name, property_value)
        WHERE d.database_id > 4
        AND   d.name != DB_NAME()
        AND   d.state_desc = N'ONLINE'
        AND   d.database_id < 32761 /*exclude contained AG system databases*/
        OPTION (RECOMPILE);

        IF @debug = 1
        BEGIN
            DECLARE @property_count integer = ROWCOUNT_BIG();
            RAISERROR(N'Collected %d database properties', 0, 1, @property_count) WITH NOWAIT;
        END;

        /*
        Collect database scoped configurations using dynamic SQL
        This avoids hardcoding configuration names which may change over time
        */
        DECLARE database_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT
                database_id = d.database_id,
                database_name = d.name
            FROM sys.databases AS d
            WHERE d.database_id > 4
            AND   d.name != DB_NAME()
            AND   d.state_desc = N'ONLINE'
            AND   d.database_id < 32761 /*exclude contained AG system databases*/
            ORDER BY
                d.name
            OPTION (RECOMPILE);

        OPEN database_cursor;
        FETCH NEXT FROM database_cursor INTO @database_id, @database_name;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Collecting scoped configurations for database: %s', 0, 1, @database_name) WITH NOWAIT;
            END;

            /*
            Build dynamic SQL to query sys.database_scoped_configurations in target database
            */
            SET @sql = N'

INSERT
    #current_database_config
WITH
    (TABLOCK)
(
    database_id,
    database_name,
    setting_type,
    setting_name,
    setting_value
)
SELECT
    database_id = ' + CONVERT(nvarchar(20), @database_id) + N',
    database_name = N' + QUOTENAME(@database_name, '''') + N',
    setting_type = ''DATABASE_SCOPED_CONFIG'',
    setting_name = dsc.name,
    setting_value = CONVERT
    (
        sql_variant,
        CASE
            WHEN dsc.value IS NOT NULL
            THEN CONVERT(nvarchar(128), dsc.value)
            WHEN dsc.value_for_secondary IS NOT NULL
            THEN CONVERT(nvarchar(128), dsc.value_for_secondary)
            ELSE NULL
        END
    )
FROM ' + QUOTENAME(@database_name) + N'.sys.database_scoped_configurations AS dsc
OPTION (RECOMPILE);
';

            BEGIN TRY
                EXECUTE sys.sp_executesql
                    @sql;
            END TRY
                        BEGIN CATCH
                            DECLARE
                                @scoped_config_error nvarchar(2048) = ERROR_MESSAGE();
            
                            /*
                            Log per-database error
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
                                N'database_configuration_collector',
                                N'ERROR',
                                DATEDIFF(MILLISECOND, @collection_time, SYSDATETIME()),
                                N'Database: ' + @database_name + N' - ' + @scoped_config_error
                            );
            
                            IF @debug = 1
                            BEGIN
                                RAISERROR(N'Error collecting scoped configs from %s: %s', 0, 1, @database_name, @scoped_config_error) WITH NOWAIT;
                            END;
                        END CATCH;

            FETCH NEXT FROM database_cursor INTO @database_id, @database_name;
        END;

        CLOSE database_cursor;
        DEALLOCATE database_cursor;

        /*
        Switch back to PerformanceMonitor database
        */
        IF @debug = 1
        BEGIN
            SELECT
                total_settings = COUNT_BIG(*)
            FROM #current_database_config;
        END;

        /*
        Insert only configurations that have changed since last collection
        Compare against most recent collection for each database/setting combination
        */
        INSERT INTO
            config.database_configuration_history
        (
            collection_time,
            database_id,
            database_name,
            setting_type,
            setting_name,
            setting_value
        )
        SELECT
            collection_time = @collection_time,
            c.database_id,
            c.database_name,
            c.setting_type,
            c.setting_name,
            c.setting_value
        FROM #current_database_config AS c
        WHERE NOT EXISTS
        (
            SELECT
                1/0
            FROM config.database_configuration_history AS h
            WHERE h.database_id = c.database_id
            AND   h.setting_type = c.setting_type
            AND   h.setting_name = c.setting_name
            AND   h.collection_time =
            (
                SELECT TOP (1)
                    h2.collection_time
                FROM config.database_configuration_history AS h2
                WHERE h2.database_id = c.database_id
                AND   h2.setting_type = c.setting_type
                AND   h2.setting_name = c.setting_name
                ORDER BY
                    h2.collection_time DESC
            )
            AND   (
                      (h.setting_value = c.setting_value)
                      OR
                      (h.setting_value IS NULL AND c.setting_value IS NULL)
                  )
        )
        OPTION (RECOMPILE);

        SET @rows_inserted = ROWCOUNT_BIG();

        IF @debug = 1
        BEGIN
            RAISERROR(N'Inserted %d database configuration changes', 0, 1, @rows_inserted) WITH NOWAIT;
        END;

        /*
        Log collection success
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
            N'database_configuration_collector',
            N'SUCCESS',
            @rows_inserted,
            DATEDIFF(MILLISECOND, @collection_time, SYSDATETIME())
        );

    END TRY
    BEGIN CATCH
        SET @error_message = ERROR_MESSAGE();

        /*
        Log collection error
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
            N'database_configuration_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @collection_time, SYSDATETIME()),
            @error_message
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Error in database_configuration_collector: %s', 0, 1, @error_message) WITH NOWAIT;
        END;

        THROW;
    END CATCH;
END;
GO

PRINT 'Database configuration collector created successfully';
PRINT 'Use collect.database_configuration_collector to collect database-level settings';
GO
