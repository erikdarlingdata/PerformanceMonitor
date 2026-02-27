/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Blocked process XML collector
Collects raw blocked process XML from ring_buffer target for later analysis with sp_HumanEventsBlockViewer
Stores raw XML without parsing for optimal collection performance

Supports:
  - On-premises SQL Server (server-scoped session)
  - Azure SQL Managed Instance (server-scoped session)
  - AWS RDS for SQL Server (server-scoped session)
  - Azure SQL DB (database-scoped session, auto-created)
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

IF OBJECT_ID(N'collect.blocked_process_xml_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.blocked_process_xml_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.blocked_process_xml_collector
(
    @minutes_back integer = 15, /*How many minutes back to collect events*/
    @debug bit = 0 /*Print debugging information*/
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE
        @session_name sysname = N'PerformanceMonitor_BlockedProcess',
        @rows_collected bigint = 0,
        @start_time datetime2(7) = SYSDATETIME(),
        @cutoff_time datetime2(7) = DATEADD(MINUTE, -@minutes_back, SYSUTCDATETIME()),
        @blocked_threshold_configured integer,
        @is_azure_sql_db bit = 0,
        @sql nvarchar(max) = N'';

    BEGIN TRY
        /*
        Detect Azure SQL DB (engine edition 5)
        */
        IF CONVERT(integer, SERVERPROPERTY('EngineEdition')) = 5
        BEGIN
            SET @is_azure_sql_db = 1;
        END;

        BEGIN TRANSACTION;

        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.blocked_process_xml', N'U') IS NULL
        BEGIN
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
                N'blocked_process_xml_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.blocked_process_xml does not exist, calling ensure procedure'
            );

            EXECUTE config.ensure_collection_table
                @table_name = N'blocked_process_xml',
                @debug = @debug;

            IF OBJECT_ID(N'collect.blocked_process_xml', N'U') IS NULL
            BEGIN
                RAISERROR(N'Table collect.blocked_process_xml still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Check if blocked process threshold is configured (not applicable to Azure SQL DB)
        */
        IF @is_azure_sql_db = 0
        BEGIN
            SELECT
                @blocked_threshold_configured = CONVERT(integer, c.value_in_use)
            FROM sys.configurations AS c
            WHERE c.name = N'blocked process threshold (s)';

            IF @blocked_threshold_configured = 0
            BEGIN
                IF @debug = 1
                BEGIN
                    RAISERROR(N'Blocked process threshold not configured - skipping collection', 0, 1) WITH NOWAIT;
                END;

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
                    N'blocked_process_xml_collector',
                    N'SKIPPED',
                    DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
                    N'Blocked process threshold not configured'
                );

                COMMIT TRANSACTION;
                RETURN;
            END;
        END;

        /*
        First run detection - collect 3 days of history if this is the first execution
        */
        IF NOT EXISTS (SELECT 1/0 FROM collect.blocked_process_xml)
        AND NOT EXISTS (SELECT 1/0 FROM config.collection_log WHERE collector_name = N'blocked_process_xml_collector')
        BEGIN
            SET @minutes_back = 4320; /*3 days*/
            SET @cutoff_time = DATEADD(MINUTE, -@minutes_back, SYSUTCDATETIME());

            IF @debug = 1
            BEGIN
                RAISERROR(N'First run detected - collecting last 3 days of blocked process events', 0, 1) WITH NOWAIT;
            END;
        END;

        /*
        Collect raw blocked process XML from ring_buffer target
        Azure SQL DB uses database-scoped sessions (dm_xe_database_*)
        On-prem/MI/RDS uses server-scoped sessions (dm_xe_*)
        */
        IF @is_azure_sql_db = 1
        BEGIN
            SET @sql = N'
            DECLARE
                @ring_buffer TABLE
            (
                ring_buffer xml NOT NULL
            );

            INSERT
                @ring_buffer
            (
                ring_buffer
            )
            SELECT
                ring_xml = TRY_CAST(xet.target_data AS xml)
            FROM sys.dm_xe_database_session_targets AS xet
            JOIN sys.dm_xe_database_sessions AS xes
              ON xes.address = xet.event_session_address
            WHERE xes.name = @session_name
            AND   xet.target_name = N''ring_buffer''
            OPTION(RECOMPILE);

            INSERT INTO
                collect.blocked_process_xml
            (
                event_time,
                blocked_process_xml
            )
            SELECT TOP (1000)
                event_time = evt.value(''(@timestamp)[1]'', ''datetime2(7)''),
                blocked_process_xml = evt.query(''.'')
            FROM
            (
                SELECT
                    rb.ring_buffer
                FROM @ring_buffer AS rb
            ) AS rb
            CROSS APPLY rb.ring_buffer.nodes(''RingBufferTarget/event[@name="blocked_process_report"]'') AS q(evt)
            WHERE evt.value(''(@timestamp)[1]'', ''datetime2(7)'') >= @cutoff_time
            AND NOT EXISTS
            (
                SELECT
                    1/0
                FROM collect.blocked_process_xml AS bx
                WHERE bx.event_time = evt.value(''(@timestamp)[1]'', ''datetime2(7)'')
            )
            ORDER BY
                evt.value(''(@timestamp)[1]'', ''datetime2(7)'') DESC
            OPTION(RECOMPILE);';
        END;
        ELSE
        BEGIN
            SET @sql = N'
            DECLARE
                @ring_buffer TABLE
            (
                ring_buffer xml NOT NULL
            );

            INSERT
                @ring_buffer
            (
                ring_buffer
            )
            SELECT
                ring_xml = TRY_CAST(xet.target_data AS xml)
            FROM sys.dm_xe_session_targets AS xet
            JOIN sys.dm_xe_sessions AS xes
              ON xes.address = xet.event_session_address
            WHERE xes.name = @session_name
            AND   xet.target_name = N''ring_buffer''
            OPTION(RECOMPILE);

            INSERT INTO
                collect.blocked_process_xml
            (
                event_time,
                blocked_process_xml
            )
            SELECT TOP (1000)
                event_time = evt.value(''(@timestamp)[1]'', ''datetime2(7)''),
                blocked_process_xml = evt.query(''.'')
            FROM
            (
                SELECT
                    rb.ring_buffer
                FROM @ring_buffer AS rb
            ) AS rb
            CROSS APPLY rb.ring_buffer.nodes(''RingBufferTarget/event[@name="blocked_process_report"]'') AS q(evt)
            WHERE evt.value(''(@timestamp)[1]'', ''datetime2(7)'') >= @cutoff_time
            AND NOT EXISTS
            (
                SELECT
                    1/0
                FROM collect.blocked_process_xml AS bx
                WHERE bx.event_time = evt.value(''(@timestamp)[1]'', ''datetime2(7)'')
            )
            ORDER BY
                evt.value(''(@timestamp)[1]'', ''datetime2(7)'') DESC
            OPTION(RECOMPILE);';
        END;

        BEGIN TRY
            IF @debug = 1
            BEGIN
                PRINT 'Session name: ' + @session_name;
                PRINT 'Current cutoff time: ' + RTRIM(@cutoff_time);
                PRINT 'Azure SQL DB: ' + CASE WHEN @is_azure_sql_db = 1 THEN 'Yes' ELSE 'No' END;
                PRINT @sql;
            END;

            EXECUTE sys.sp_executesql
                @sql,
                N'@session_name sysname, @cutoff_time datetime2(7)',
                @session_name,
                @cutoff_time;

            SET @rows_collected = ROWCOUNT_BIG();

        END TRY
        BEGIN CATCH
            /*
            Session doesn't exist or is not accessible
            This is expected if XE setup hasn't been run
            */
            IF @debug = 1
            BEGIN
                RAISERROR(N'Blocked process session not available: %s', 0, 1, @session_name) WITH NOWAIT;
            END;
        END CATCH;

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
            N'blocked_process_xml_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %d blocked process events', 0, 1, @rows_collected) WITH NOWAIT;
        END;

        COMMIT TRANSACTION;

        /*
        Chain-trigger: when new blocked process XML is found, immediately
        parse it and run the analyzer instead of waiting for their next
        scheduled runs. This eliminates up to 10 minutes of pipeline latency.
        Parser/analyzer errors are logged but do not fail this collector.
        */
        IF @rows_collected > 0
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Chain-triggering blocked process parser and analyzer', 0, 1) WITH NOWAIT;
            END;

            BEGIN TRY
                EXECUTE collect.process_blocked_process_xml
                    @debug = @debug;
            END TRY
            BEGIN CATCH
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
                    N'blocked_process_xml_collector',
                    N'CHAIN_ERROR',
                    DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
                    N'Chain-triggered parser failed: ' + ERROR_MESSAGE()
                );
            END CATCH;

            BEGIN TRY
                EXECUTE collect.blocking_deadlock_analyzer
                    @debug = @debug;
            END TRY
            BEGIN CATCH
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
                    N'blocked_process_xml_collector',
                    N'CHAIN_ERROR',
                    DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
                    N'Chain-triggered analyzer failed: ' + ERROR_MESSAGE()
                );
            END CATCH;
        END;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;

        DECLARE
            @error_message nvarchar(4000) = ERROR_MESSAGE();

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
            N'blocked_process_xml_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in blocked process XML collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Blocked process XML collector created successfully';
PRINT 'Reads from ring_buffer target of PerformanceMonitor_BlockedProcess session';
PRINT 'Stores raw blocked process XML for later analysis with sp_HumanEventsBlockViewer';
GO
