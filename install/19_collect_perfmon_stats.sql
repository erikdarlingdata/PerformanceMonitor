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
Performance counter statistics collector
Collects key SQL Server performance counters from sys.dm_os_performance_counters
Stores raw counter values with counter type for proper delta calculation
Filters to important counters only (not all ~1000+ counters)
*/

IF OBJECT_ID(N'collect.perfmon_stats_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.perfmon_stats_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.perfmon_stats_collector
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
        @server_start_time datetime2(7) =
        (
            SELECT
                sqlserver_start_time
            FROM sys.dm_os_sys_info
        ),
        @prefix sysname =
        (
            SELECT TOP (1)
                SUBSTRING
                (
                    dopc.object_name,
                    1,
                    CHARINDEX(N':', dopc.object_name)
                ) + N'%'
            FROM sys.dm_os_performance_counters AS dopc
        ),
        @error_message nvarchar(4000);

    BEGIN TRY
        BEGIN TRANSACTION;

        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.perfmon_stats', N'U') IS NULL
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
                N'perfmon_stats_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.perfmon_stats does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'perfmon_stats',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.perfmon_stats', N'U') IS NULL
            BEGIN
                RAISERROR(N'Table collect.perfmon_stats still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Collect key performance counters from sys.dm_os_performance_counters
        Filters to important counters only (not all 1000+ counters)
        Stores raw values with counter type for proper calculation
        */
        INSERT INTO
            collect.perfmon_stats
        (
            server_start_time,
            object_name,
            counter_name,
            instance_name,
            cntr_value,
            cntr_type
        )
        SELECT
            server_start_time = @server_start_time,
            object_name =
                RTRIM(LTRIM(dopc.object_name)),
            counter_name =
                RTRIM(LTRIM(dopc.counter_name)),
            instance_name =
                CASE
                    WHEN LEN(RTRIM(LTRIM(dopc.instance_name))) > 0
                    THEN RTRIM(LTRIM(dopc.instance_name))
                    ELSE N'_Total'
                END,
            cntr_value = dopc.cntr_value,
            cntr_type = dopc.cntr_type
        FROM sys.dm_os_performance_counters AS dopc
        WHERE dopc.object_name LIKE @prefix
        AND   dopc.instance_name NOT IN
              (
                  N'internal',
                  N'master',
                  N'model',
                  N'msdb',
                  N'model_msdb',
                  N'model_replicatedmaster',
                  N'mssqlsystemresource'
              )
        AND   dopc.counter_name IN
        (
            /*I/O counters*/
            N'Forwarded Records/sec',
            N'Full Scans/sec',
            N'Index Searches/sec',
            N'Page Splits/sec',
            N'Page reads/sec',
            N'Page writes/sec',
            N'Checkpoint pages/sec',
            N'Page lookups/sec',
            N'Readahead pages/sec',
            N'Background writer pages/sec',
            N'Lazy writes/sec',
            N'Non-Page latch waits',
            N'Page IO latch waits',
            N'Page latch waits',
            /*Transaction counters*/
            N'Transactions/sec',
            N'Longest Transaction Running Time',
            /*Locking counters*/
            N'Table Lock Escalations/sec',
            N'Lock Requests/sec',
            N'Lock Wait Time (ms)',
            N'Lock Waits/sec',
            N'Number of Deadlocks/sec',
            N'Lock waits',
            N'Lock Timeouts/sec',
            N'Processes blocked',
            /*Memory counters*/
            N'Granted Workspace Memory (KB)',
            N'Lock Memory (KB)',
            N'Memory Grants Pending',
            N'SQL Cache Memory (KB)',
            N'Stolen Server Memory (KB)',
            N'Target Server Memory (KB)',
            N'Total Server Memory (KB)',
            N'Memory grant queue waits',
            N'Thread-safe memory objects waits',
            N'Free list stalls/sec',
            /*Compilation counters*/
            N'SQL Compilations/sec',
            N'SQL Re-Compilations/sec',
            N'Query optimizations/sec',
            N'Reduced memory grants/sec',
            /*Batch and request counters*/
            N'Batch Requests/sec',
            N'Requests completed/sec',
            N'Active requests',
            N'Queued requests',
            N'Blocked tasks',
            N'Active parallel threads',
            /*Log counters*/
            N'Log Flushes/sec',
            N'Log Bytes Flushed/sec',
            N'Log Flush Write Time (ms)',
            N'Log buffer waits',
            N'Log write waits',
            /*TempDB counters*/
            N'Version Store Size (KB)',
            N'Free Space in tempdb (KB)',
            N'Active Temp Tables',
            N'Version Generation rate (KB/s)',
            N'Version Cleanup rate (KB/s)',
            N'Temp Tables Creation Rate',
            N'Workfiles Created/sec',
            N'Worktables Created/sec',
            /*Wait counters*/
            N'Network IO waits',
            N'Wait for the worker'
        )
        OPTION(RECOMPILE);

        SET @rows_collected = ROWCOUNT_BIG();

        /*
        Calculate deltas for the newly inserted data
        */
        EXECUTE collect.calculate_deltas
            @table_name = N'perfmon_stats',
            @debug = @debug;

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
            N'perfmon_stats_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %d perfmon stats rows', 0, 1, @rows_collected) WITH NOWAIT;
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
            N'perfmon_stats_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in perfmon stats collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Perfmon stats collector created successfully';
PRINT 'Collects key performance counters from sys.dm_os_performance_counters';
PRINT 'Stores raw counter values with counter type for delta calculation';
GO
