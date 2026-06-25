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
Server Configuration Collector
Collects SQL Server instance-level configuration settings and trace flags
Only logs changes from previous collection to track configuration drift
*/

IF OBJECT_ID(N'collect.server_configuration_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.server_configuration_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.server_configuration_collector
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
        @error_message nvarchar(4000);

    BEGIN TRY
        IF @debug = 1
        BEGIN
            RAISERROR(N'Starting server configuration collection', 0, 1) WITH NOWAIT;
        END;

        /*
        Create temp table for current server configuration snapshot
        */
        CREATE TABLE
            #current_server_config
        (
            configuration_id integer NOT NULL,
            configuration_name nvarchar(128) NOT NULL,
            value_configured sql_variant NULL,
            value_in_use sql_variant NULL,
            value_minimum sql_variant NULL,
            value_maximum sql_variant NULL,
            is_dynamic bit NOT NULL,
            is_advanced bit NOT NULL,
            description nvarchar(512) NULL
        );

        /*
        Collect current server configuration from sys.configurations
        */
        INSERT
            #current_server_config
        WITH
            (TABLOCK)
        (
            configuration_id,
            configuration_name,
            value_configured,
            value_in_use,
            value_minimum,
            value_maximum,
            is_dynamic,
            is_advanced,
            description
        )
        SELECT
            configuration_id = c.configuration_id,
            configuration_name = c.name,
            value_configured = c.value,
            value_in_use = c.value_in_use,
            value_minimum = c.minimum,
            value_maximum = c.maximum,
            is_dynamic = c.is_dynamic,
            is_advanced = c.is_advanced,
            description = c.description
        FROM sys.configurations AS c
        OPTION (RECOMPILE);

        /*
        Insert only configurations that have changed since last collection
        Compare against most recent collection for each configuration
        */
        INSERT INTO
            config.server_configuration_history
        (
            collection_time,
            configuration_id,
            configuration_name,
            value_configured,
            value_in_use,
            value_minimum,
            value_maximum,
            is_dynamic,
            is_advanced,
            description
        )
        SELECT
            collection_time = @collection_time,
            c.configuration_id,
            c.configuration_name,
            c.value_configured,
            c.value_in_use,
            c.value_minimum,
            c.value_maximum,
            c.is_dynamic,
            c.is_advanced,
            c.description
        FROM #current_server_config AS c
        WHERE NOT EXISTS
        (
            SELECT
                1/0
            FROM config.server_configuration_history AS h
            WHERE h.configuration_id = c.configuration_id
            AND   h.collection_time =
            (
                SELECT TOP (1)
                    h2.collection_time
                FROM config.server_configuration_history AS h2
                WHERE h2.configuration_id = c.configuration_id
                ORDER BY
                    h2.collection_time DESC
            )
            AND   h.value_configured = c.value_configured
            AND   h.value_in_use = c.value_in_use
        )
        OPTION (RECOMPILE);

        SET @rows_inserted = ROWCOUNT_BIG();

        IF @debug = 1
        BEGIN
            RAISERROR(N'Inserted %d server configuration changes', 0, 1, @rows_inserted) WITH NOWAIT;
        END;

        /*
        Collect trace flags
        */
        CREATE TABLE
            #current_trace_flags
        (
            trace_flag integer NOT NULL,
            status bit NOT NULL,
            is_global bit NOT NULL,
            is_session bit NOT NULL
        );

        /*
        Use DBCC TRACESTATUS to get current trace flags
        */
        INSERT
            #current_trace_flags
        WITH
            (TABLOCK)
        (
            trace_flag,
            status,
            is_global,
            is_session
        )
        EXECUTE(N'DBCC TRACESTATUS(-1) WITH NO_INFOMSGS;');

        /*
        Insert only trace flags that have changed since last collection
        */
        INSERT INTO
            config.trace_flags_history
        (
            collection_time,
            trace_flag,
            status,
            is_global,
            is_session
        )
        SELECT
            collection_time = @collection_time,
            t.trace_flag,
            t.status,
            t.is_global,
            t.is_session
        FROM #current_trace_flags AS t
        WHERE NOT EXISTS
        (
            SELECT
                1/0
            FROM config.trace_flags_history AS h
            WHERE h.trace_flag = t.trace_flag
            AND   h.collection_time =
            (
                SELECT TOP (1)
                    h2.collection_time
                FROM config.trace_flags_history AS h2
                WHERE h2.trace_flag = t.trace_flag
                ORDER BY
                    h2.collection_time DESC
            )
            AND   h.status = t.status
            AND   h.is_global = t.is_global
            AND   h.is_session = t.is_session
        )

        UNION ALL

        /*
        Also log trace flags that were removed (exist in history but not in current)
        */
        SELECT
            collection_time = @collection_time,
            trace_flag = h.trace_flag,
            status = CONVERT(bit, 0),
            is_global = CONVERT(bit, 0),
            is_session = CONVERT(bit, 0)
        FROM
        (
            SELECT DISTINCT
                h.trace_flag
            FROM config.trace_flags_history AS h
            WHERE h.collection_time =
            (
                SELECT TOP (1)
                    h2.collection_time
                FROM config.trace_flags_history AS h2
                ORDER BY
                    h2.collection_time DESC
            )
            AND   h.status = 1
        ) AS h
        WHERE NOT EXISTS
        (
            SELECT
                1/0
            FROM #current_trace_flags AS t
            WHERE t.trace_flag = h.trace_flag
        )
        OPTION (RECOMPILE);

        SET @rows_inserted = @rows_inserted + ROWCOUNT_BIG();

        IF @debug = 1
        BEGIN
            RAISERROR(N'Total rows inserted: %d', 0, 1, @rows_inserted) WITH NOWAIT;
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
            N'server_configuration_collector',
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
            N'server_configuration_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @collection_time, SYSDATETIME()),
            @error_message
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Error in server_configuration_collector: %s', 0, 1, @error_message) WITH NOWAIT;
        END;

        THROW;
    END CATCH;
END;
GO

PRINT 'Server configuration collector created successfully';
PRINT 'Use collect.server_configuration_collector to collect server-level settings';
GO
