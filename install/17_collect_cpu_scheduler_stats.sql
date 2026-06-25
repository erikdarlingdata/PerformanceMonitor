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
CPU scheduler statistics collector
Collects scheduler and workload group data for CPU pressure monitoring
Stores raw counts and calculates pressure warnings
Point-in-time snapshot data for thread and CPU availability
*/

IF OBJECT_ID(N'collect.cpu_scheduler_stats_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.cpu_scheduler_stats_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.cpu_scheduler_stats_collector
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
        @error_message nvarchar(4000);

    BEGIN TRY
        BEGIN TRANSACTION;

        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.cpu_scheduler_stats', N'U') IS NULL
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
                N'cpu_scheduler_stats_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.cpu_scheduler_stats does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'cpu_scheduler_stats',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.cpu_scheduler_stats', N'U') IS NULL
            BEGIN
                RAISERROR(N'Table collect.cpu_scheduler_stats still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Collect CPU scheduler and workload group statistics
        Point-in-time snapshot showing thread availability and CPU pressure
        Stores raw numeric values and calculates pressure warnings
        */
        INSERT INTO
            collect.cpu_scheduler_stats
        (
            max_workers_count,
            scheduler_count,
            cpu_count,
            total_runnable_tasks_count,
            total_work_queue_count,
            total_current_workers_count,
            avg_runnable_tasks_count,
            total_active_request_count,
            total_queued_request_count,
            total_blocked_task_count,
            total_active_parallel_thread_count,
            runnable_request_count,
            total_request_count,
            runnable_percent,
            worker_thread_exhaustion_warning,
            runnable_tasks_warning,
            blocked_tasks_warning,
            queued_requests_warning,
            total_physical_memory_kb,
            available_physical_memory_kb,
            system_memory_state_desc,
            physical_memory_pressure_warning,
            total_node_count,
            nodes_online_count,
            offline_cpu_count,
            offline_cpu_warning
        )
        SELECT
            max_workers_count = osi.max_workers_count,
            scheduler_count = osi.scheduler_count,
            cpu_count = osi.cpu_count,
            total_runnable_tasks_count = SUM(dos.runnable_tasks_count),
            total_work_queue_count = SUM(dos.work_queue_count),
            total_current_workers_count = SUM(dos.current_workers_count),
            avg_runnable_tasks_count = AVG(CONVERT(decimal(38,2), dos.runnable_tasks_count)),
            total_active_request_count = MAX(wg.active_request_count),
            total_queued_request_count = MAX(wg.queued_request_count),
            total_blocked_task_count = MAX(wg.blocked_task_count),
            total_active_parallel_thread_count = MAX(wg.active_parallel_thread_count),
            runnable_request_count = r.runnable_count,
            total_request_count = r.total_count,
            runnable_percent = r.runnable_pct,
            worker_thread_exhaustion_warning =
                CASE
                    WHEN SUM(dos.current_workers_count) >= (osi.max_workers_count * 0.90)
                    THEN 1
                    ELSE 0
                END,
            runnable_tasks_warning =
                CASE
                    WHEN SUM(dos.runnable_tasks_count) >= osi.cpu_count
                    THEN 1
                    ELSE 0
                END,
            blocked_tasks_warning =
                CASE
                    WHEN MAX(wg.blocked_task_count) >= 10
                    THEN 1
                    ELSE 0
                END,
            queued_requests_warning =
                CASE
                    WHEN MAX(wg.queued_request_count) >= osi.cpu_count
                    THEN 1
                    ELSE 0
                END,
            total_physical_memory_kb = osm.total_physical_memory_kb,
            available_physical_memory_kb = osm.available_physical_memory_kb,
            system_memory_state_desc = osm.system_memory_state_desc,
            physical_memory_pressure_warning =
                CASE
                    WHEN osm.available_physical_memory_kb < (osm.total_physical_memory_kb * 0.10)
                    THEN 1
                    ELSE 0
                END,
            total_node_count = nodes.total_nodes,
            nodes_online_count = nodes.online_nodes,
            offline_cpu_count = nodes.offline_cpus,
            offline_cpu_warning =
                CASE
                    WHEN 
                    (
                        SELECT COUNT_BIG(*) 
                        FROM sys.dm_os_schedulers dos 
                        WHERE dos.is_online = 0
                    ) > 0
                    THEN 1
                    ELSE 0
                END
        FROM sys.dm_os_schedulers AS dos
        CROSS JOIN sys.dm_os_sys_info AS osi
        CROSS JOIN sys.dm_os_sys_memory AS osm
        CROSS JOIN
        (
            SELECT
                total_nodes = COUNT_BIG(*),
                online_nodes =
                    SUM
                    (
                        CASE
                            WHEN n.node_state_desc = N'ONLINE'
                            THEN 1
                            ELSE 0
                        END
                    ),
                offline_cpus =
                    SUM
                    (
                        CASE
                            WHEN n.node_state_desc <> N'ONLINE'
                            THEN 1
                            ELSE 0
                        END
                    )
            FROM sys.dm_os_nodes AS n
            WHERE n.node_id <> 32767 /*Exclude DAC node*/
        ) AS nodes
        CROSS JOIN
        (
            SELECT
                active_request_count =
                    SUM(wg.active_request_count),
                queued_request_count =
                    SUM(wg.queued_request_count),
                blocked_task_count =
                    SUM(wg.blocked_task_count),
                active_parallel_thread_count =
                    SUM(wg.active_parallel_thread_count)
            FROM sys.dm_resource_governor_workload_groups AS wg
        ) AS wg
        OUTER APPLY
        (
            SELECT
                total_count = COUNT_BIG(*),
                runnable_count =
                    SUM
                    (
                        CASE
                            WHEN der.status = N'runnable'
                            THEN 1
                            ELSE 0
                        END
                    ),
                runnable_pct =
                    CONVERT
                    (
                        decimal(38,2),
                        (
                            SUM
                            (
                                CASE
                                    WHEN der.status = N'runnable'
                                    THEN 1.0
                                    ELSE 0.0
                                END
                            ) /
                            NULLIF(CONVERT(decimal(38,2), COUNT_BIG(*)), 0)
                        ) * 100.0
                    )
            FROM sys.dm_exec_requests AS der
            WHERE der.session_id > 50
            AND   der.session_id <> @@SPID
            AND   der.status NOT IN (N'background', N'sleeping')
        ) AS r
        WHERE dos.status = N'VISIBLE ONLINE'
        GROUP BY
            osi.max_workers_count,
            osi.scheduler_count,
            osi.cpu_count,
            r.runnable_count,
            r.total_count,
            r.runnable_pct,
            osm.total_physical_memory_kb,
            osm.available_physical_memory_kb,
            osm.system_memory_state_desc,
            nodes.total_nodes,
            nodes.online_nodes,
            nodes.offline_cpus
        OPTION(RECOMPILE);

        SET @rows_collected = ROWCOUNT_BIG();

        /*
        Check for offline CPUs and log to critical_issues if found
        */
        IF EXISTS
        (
            SELECT
                1/0
            FROM collect.cpu_scheduler_stats AS css
            WHERE css.collection_id =
            (
                SELECT
                    MAX(css2.collection_id)
                FROM collect.cpu_scheduler_stats AS css2
            )
            AND   css.offline_cpu_warning = 1
        )
        BEGIN
            /*
            Log offline CPU detection
            */
            INSERT INTO
                config.collection_log
            (
                collector_name,
                collection_status,
                error_message
            )
            VALUES
            (
                N'cpu_scheduler_stats_collector',
                N'CRITICAL',
                N'CRITICAL: Offline CPUs detected in NUMA nodes'
            );
        END;

        /*
        Debug output for pressure warnings
        */
        IF @debug = 1
        BEGIN
            DECLARE
                @current_max_workers integer,
                @current_workers integer,
                @current_cpu_count integer,
                @current_runnable_tasks integer,
                @current_blocked_tasks integer,
                @current_queued_requests integer,
                @worker_warning bit,
                @runnable_warning bit,
                @blocked_warning bit,
                @queued_warning bit,
                @current_total_memory_kb bigint,
                @current_available_memory_kb bigint,
                @memory_pressure_warning bit,
                @current_offline_cpus integer,
                @offline_cpu_warning bit;

            SELECT
                @current_max_workers = css.max_workers_count,
                @current_workers = css.total_current_workers_count,
                @current_cpu_count = css.cpu_count,
                @current_runnable_tasks = css.total_runnable_tasks_count,
                @current_blocked_tasks = css.total_blocked_task_count,
                @current_queued_requests = css.total_queued_request_count,
                @worker_warning = css.worker_thread_exhaustion_warning,
                @runnable_warning = css.runnable_tasks_warning,
                @blocked_warning = css.blocked_tasks_warning,
                @queued_warning = css.queued_requests_warning,
                @current_total_memory_kb = css.total_physical_memory_kb,
                @current_available_memory_kb = css.available_physical_memory_kb,
                @memory_pressure_warning = css.physical_memory_pressure_warning,
                @current_offline_cpus = css.offline_cpu_count,
                @offline_cpu_warning = css.offline_cpu_warning
            FROM collect.cpu_scheduler_stats AS css
            WHERE css.collection_id =
            (
                SELECT
                    MAX(css2.collection_id)
                FROM collect.cpu_scheduler_stats AS css2
            );

            IF @worker_warning = 1
            BEGIN
                RAISERROR(N'WARNING: Worker threads at %d of %d maximum (>=90%%)', 0, 1, @current_workers, @current_max_workers) WITH NOWAIT;
            END;

            IF @runnable_warning = 1
            BEGIN
                RAISERROR(N'WARNING: Runnable tasks (%d) >= CPU count (%d)', 0, 1, @current_runnable_tasks, @current_cpu_count) WITH NOWAIT;
            END;

            IF @blocked_warning = 1
            BEGIN
                RAISERROR(N'WARNING: Blocked tasks detected: %d (>=10)', 0, 1, @current_blocked_tasks) WITH NOWAIT;
            END;

            IF @queued_warning = 1
            BEGIN
                RAISERROR(N'WARNING: Queued requests (%d) >= CPU count (%d)', 0, 1, @current_queued_requests, @current_cpu_count) WITH NOWAIT;
            END;

            IF @memory_pressure_warning = 1
            BEGIN
                RAISERROR(N'WARNING: Physical memory pressure - Available: %I64d KB of %I64d KB total (<10%%)', 0, 1, @current_available_memory_kb, @current_total_memory_kb) WITH NOWAIT;
            END;

            IF @offline_cpu_warning = 1
            BEGIN
                RAISERROR(N'CRITICAL: Offline CPUs detected: %d offline nodes', 0, 1, @current_offline_cpus) WITH NOWAIT;
            END;
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
            N'cpu_scheduler_stats_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %I64d CPU scheduler stats rows', 0, 1, @rows_collected) WITH NOWAIT;
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
            N'cpu_scheduler_stats_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in CPU scheduler stats collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'CPU scheduler stats collector created successfully';
PRINT 'Collects point-in-time CPU scheduler and workload group data';
PRINT 'Stores raw numeric values and calculates pressure warnings for thread availability and CPU monitoring';
GO
