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
CPU utilization statistics collector
Collects CPU utilization events from sys.dm_os_ring_buffers (SCHEDULER_MONITOR)
Tracks SQL Server CPU vs other process CPU utilization over time
Point-in-time samples stored by ring buffer - not cumulative
*/

IF OBJECT_ID(N'collect.cpu_utilization_stats_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.cpu_utilization_stats_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.cpu_utilization_stats_collector
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
        @max_sample_time datetime2(7) = NULL,
        @error_message nvarchar(4000);

    BEGIN TRY
        BEGIN TRANSACTION;

        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.cpu_utilization_stats', N'U') IS NULL
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
                N'cpu_utilization_stats_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.cpu_utilization_stats does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'cpu_utilization_stats',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.cpu_utilization_stats', N'U') IS NULL
            BEGIN
                RAISERROR(N'Table collect.cpu_utilization_stats still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Get the most recent sample time to avoid duplicate collection
        */
        SELECT
            @max_sample_time = MAX(cus.sample_time)
        FROM collect.cpu_utilization_stats AS cus;

        /*
        Collect CPU utilization data from ring buffers
        Only collects samples newer than the most recent sample we have
        On first run (NULL max_sample_time), looks back 1 hour to populate initial data
        Avoids duplicate collection of same ring buffer events
        */
        INSERT INTO
            collect.cpu_utilization_stats
        (
            sample_time,
            sqlserver_cpu_utilization,
            other_process_cpu_utilization
        )
        SELECT
            sample_time =
                DATEADD
                (
                    SECOND,
                    -((@current_ms_ticks - t.timestamp) / 1000),
                    @start_time
                ),
            sqlserver_cpu_utilization =
                t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'integer'),
            other_process_cpu_utilization =
                CASE
                    WHEN (100 -
                          t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'integer') -
                          t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'integer')) < 0
                    THEN 0
                    ELSE 100 -
                         t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'integer') -
                         t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'integer')
                END
        FROM
        (
            SELECT
                dorb.timestamp,
                record =
                    CONVERT(xml, dorb.record)
            FROM sys.dm_os_ring_buffers AS dorb
            WHERE dorb.ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
        ) AS t
        WHERE DATEADD
        (
            SECOND,
            -((@current_ms_ticks - t.timestamp) / 1000),
            @start_time
        ) > ISNULL(@max_sample_time, DATEADD(HOUR, -1, @start_time))
        ORDER BY
            t.timestamp DESC
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
            N'cpu_utilization_stats_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %d CPU utilization stats rows', 0, 1, @rows_collected) WITH NOWAIT;
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
            N'cpu_utilization_stats_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in CPU utilization stats collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'CPU utilization stats collector created successfully';
PRINT 'Collects CPU utilization events from sys.dm_os_ring_buffers (SCHEDULER_MONITOR ring buffer)';
PRINT 'Tracks SQL Server CPU vs other process CPU utilization over time';
GO
