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
Memory pressure events collector
Collects memory resource monitor events from sys.dm_os_ring_buffers (RING_BUFFER_RESOURCE_MONITOR)
Tracks memory notifications and pressure indicators over time
*/

IF OBJECT_ID(N'collect.memory_pressure_events_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.memory_pressure_events_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.memory_pressure_events_collector
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
        @current_ms_ticks bigint =
        (
            SELECT
                osi.ms_ticks
            FROM sys.dm_os_sys_info AS osi
        ),
        @max_sample_time datetime2(7) =
        (
            SELECT
                MAX(mpe.sample_time)
            FROM collect.memory_pressure_events AS mpe
        ),
        @error_message nvarchar(4000);

    BEGIN TRY
        BEGIN TRANSACTION;

        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.memory_pressure_events', N'U') IS NULL
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
                N'memory_pressure_events_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.memory_pressure_events does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'memory_pressure_events',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.memory_pressure_events', N'U') IS NULL
            BEGIN
                ROLLBACK TRANSACTION;
                RAISERROR(N'Table collect.memory_pressure_events still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Collect memory pressure events from ring buffer
        Only collects events newer than most recent sample
        Calculates actual event time using ms_ticks offset
        */
        INSERT INTO
            collect.memory_pressure_events
        (
            sample_time,
            memory_notification,
            memory_indicators_process,
            memory_indicators_system
        )
        SELECT
            sample_time =
                DATEADD
                (
                    SECOND,
                    -((@current_ms_ticks - t.timestamp) / 1000),
                    @start_time
                ),
            memory_notification =
                t.record.value('(/Record/ResourceMonitor/Notification)[1]', 'nvarchar(100)'),
            memory_indicators_process =
                t.record.value('(/Record/ResourceMonitor/IndicatorsProcess)[1]', 'integer'),
            memory_indicators_system =
                t.record.value('(/Record/ResourceMonitor/IndicatorsSystem)[1]', 'integer')
        FROM
        (
            SELECT
                dorb.timestamp,
                record =
                    CONVERT(xml, dorb.record)
            FROM sys.dm_os_ring_buffers AS dorb
            WHERE dorb.ring_buffer_type = N'RING_BUFFER_RESOURCE_MONITOR'
        ) AS t
        WHERE DATEADD
        (
            SECOND,
            -((@current_ms_ticks - t.timestamp) / 1000),
            @start_time
        ) > ISNULL(@max_sample_time, CONVERT(datetime2(7), '19000101'))
        OPTION(RECOMPILE);

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
            N'memory_pressure_events_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %I64d memory pressure events', 0, 1, @rows_collected) WITH NOWAIT;
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
            N'memory_pressure_events_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in memory pressure events collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Memory pressure events collector created successfully';
PRINT 'Collects memory resource monitor events from sys.dm_os_ring_buffers (RING_BUFFER_RESOURCE_MONITOR)';
PRINT 'Tracks memory notifications and pressure indicators over time';
GO
