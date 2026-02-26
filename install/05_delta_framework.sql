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
Delta calculation framework
This procedure handles delta calculations for cumulative DMV data
*/

IF OBJECT_ID(N'collect.calculate_deltas', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.calculate_deltas AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.calculate_deltas
(
    @table_name sysname, /*Table to calculate deltas for*/
    @debug bit = 0 /*Print debugging information*/
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    
    DECLARE
        @sql nvarchar(max) = N'',
        @rows_updated integer = 0,
        @start_time datetime2(7) = SYSDATETIME(),
        @trancount_at_entry integer = @@TRANCOUNT;

    BEGIN TRY
        /*
        Only start a transaction if we're not already in one
        This avoids nested transaction issues
        */
        IF @trancount_at_entry = 0
        BEGIN
            BEGIN TRANSACTION;
        END;

        /*
        Wait Stats Delta Calculation
        */
        IF @table_name = N'wait_stats'
        BEGIN
            SET @sql = N'
            WITH 
                current_collection AS
            (
                SELECT
                    ws.*,
                    row_number = 
                        ROW_NUMBER() OVER 
                        (
                            PARTITION BY 
                                ws.wait_type 
                            ORDER BY 
                                ws.collection_time DESC
                        )
                FROM collect.wait_stats AS ws
                WHERE ws.waiting_tasks_count_delta IS NULL
            ),
                previous_collection AS
            (
                SELECT
                    ws.collection_id,
                    ws.wait_type,
                    ws.waiting_tasks_count,
                    ws.wait_time_ms,
                    ws.signal_wait_time_ms,
                    ws.collection_time,
                    row_number = 
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                ws.wait_type
                            ORDER BY
                                ws.collection_time DESC
                        )
                FROM collect.wait_stats AS ws
                WHERE ws.waiting_tasks_count_delta IS NOT NULL
                OR    ws.collection_id IN
                (
                    SELECT 
                        MIN(ws2.collection_id)
                    FROM collect.wait_stats AS ws2
                    GROUP BY 
                        ws2.wait_type
                )
            )
            UPDATE
                cc
            SET
                waiting_tasks_count_delta = 
                    CASE 
                        WHEN cc.server_start_time >= pc.collection_time 
                        THEN cc.waiting_tasks_count /*Server restart*/
                        WHEN cc.waiting_tasks_count >= pc.waiting_tasks_count 
                        THEN cc.waiting_tasks_count - pc.waiting_tasks_count
                        ELSE cc.waiting_tasks_count /*Counter wrapped or restart*/
                    END,
                wait_time_ms_delta = 
                    CASE 
                        WHEN cc.server_start_time >= pc.collection_time 
                        THEN cc.wait_time_ms /*Server restart*/
                        WHEN cc.wait_time_ms >= pc.wait_time_ms 
                        THEN cc.wait_time_ms - pc.wait_time_ms
                        ELSE cc.wait_time_ms /*Counter wrapped or restart*/
                    END,
                signal_wait_time_ms_delta = 
                    CASE 
                        WHEN cc.server_start_time >= pc.collection_time 
                        THEN cc.signal_wait_time_ms /*Server restart*/
                        WHEN cc.signal_wait_time_ms >= pc.signal_wait_time_ms 
                        THEN cc.signal_wait_time_ms - pc.signal_wait_time_ms
                        ELSE cc.signal_wait_time_ms /*Counter wrapped or restart*/
                    END,
                sample_interval_seconds = 
                    DATEDIFF
                    (
                        SECOND, 
                        pc.collection_time, 
                        cc.collection_time
                    )
            FROM current_collection AS cc
            LEFT JOIN previous_collection AS pc
              ON  cc.wait_type = pc.wait_type
              AND pc.row_number = 1
            WHERE cc.row_number = 1
            AND   pc.collection_id IS NOT NULL /*Exclude first collection where no previous exists*/
            OPTION(RECOMPILE, HASH JOIN, HASH GROUP);';
        END;
        
        /*
        Query Stats Delta Calculation
        */
        ELSE IF @table_name = N'query_stats'
        BEGIN
            SET @sql = N'
            WITH 
                current_collection AS
            (
                SELECT
                    qs.*,
                    row_number = 
                        ROW_NUMBER() OVER 
                        (
                            PARTITION BY 
                                qs.sql_handle,
                                qs.statement_start_offset,
                                qs.statement_end_offset,
                                qs.plan_handle
                            ORDER BY 
                                qs.collection_time DESC
                        )
                FROM collect.query_stats AS qs
                WHERE qs.execution_count_delta IS NULL
            ),
                previous_collection AS
            (
                SELECT
                    qs.collection_id,
                    qs.sql_handle,
                    qs.statement_start_offset,
                    qs.statement_end_offset,
                    qs.plan_handle,
                    qs.execution_count,
                    qs.total_worker_time,
                    qs.total_elapsed_time,
                    qs.total_logical_reads,
                    qs.total_physical_reads,
                    qs.total_logical_writes,
                    qs.collection_time,
                    row_number = 
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                qs.sql_handle,
                                qs.statement_start_offset,
                                qs.statement_end_offset,
                                qs.plan_handle
                            ORDER BY
                                qs.collection_time DESC
                        )
                FROM collect.query_stats AS qs
                WHERE qs.execution_count_delta IS NOT NULL
                OR qs.collection_id IN
                (
                    SELECT 
                        MIN(qs2.collection_id)
                    FROM collect.query_stats AS qs2
                    GROUP BY
                        qs2.sql_handle,
                        qs2.statement_start_offset,
                        qs2.statement_end_offset,
                        qs2.plan_handle
                )
            )
            UPDATE
                cc
            SET
                execution_count_delta =
                    CASE
                        WHEN pc.collection_id IS NULL /*First collection - use raw total*/
                        THEN cc.execution_count
                        WHEN cc.server_start_time >= pc.collection_time /*Server restart*/
                        THEN cc.execution_count
                        WHEN cc.execution_count >= pc.execution_count
                        THEN cc.execution_count - pc.execution_count
                        ELSE cc.execution_count /*Plan recompiled or cache evicted*/
                    END,
                total_worker_time_delta =
                    CASE
                        WHEN pc.collection_id IS NULL /*First collection - use raw total*/
                        THEN cc.total_worker_time
                        WHEN cc.server_start_time >= pc.collection_time /*Server restart*/
                        THEN cc.total_worker_time
                        WHEN cc.total_worker_time >= pc.total_worker_time
                        THEN cc.total_worker_time - pc.total_worker_time
                        ELSE cc.total_worker_time
                    END,
                total_elapsed_time_delta =
                    CASE
                        WHEN pc.collection_id IS NULL /*First collection - use raw total*/
                        THEN cc.total_elapsed_time
                        WHEN cc.server_start_time >= pc.collection_time /*Server restart*/
                        THEN cc.total_elapsed_time
                        WHEN cc.total_elapsed_time >= pc.total_elapsed_time
                        THEN cc.total_elapsed_time - pc.total_elapsed_time
                        ELSE cc.total_elapsed_time
                    END,
                total_logical_reads_delta =
                    CASE
                        WHEN pc.collection_id IS NULL /*First collection - use raw total*/
                        THEN cc.total_logical_reads
                        WHEN cc.server_start_time >= pc.collection_time /*Server restart*/
                        THEN cc.total_logical_reads
                        WHEN cc.total_logical_reads >= pc.total_logical_reads
                        THEN cc.total_logical_reads - pc.total_logical_reads
                        ELSE cc.total_logical_reads
                    END,
                total_physical_reads_delta =
                    CASE
                        WHEN pc.collection_id IS NULL /*First collection - use raw total*/
                        THEN cc.total_physical_reads
                        WHEN cc.server_start_time >= pc.collection_time /*Server restart*/
                        THEN cc.total_physical_reads
                        WHEN cc.total_physical_reads >= pc.total_physical_reads
                        THEN cc.total_physical_reads - pc.total_physical_reads
                        ELSE cc.total_physical_reads
                    END,
                total_logical_writes_delta =
                    CASE
                        WHEN pc.collection_id IS NULL /*First collection - use raw total*/
                        THEN cc.total_logical_writes
                        WHEN cc.server_start_time >= pc.collection_time /*Server restart*/
                        THEN cc.total_logical_writes
                        WHEN cc.total_logical_writes >= pc.total_logical_writes
                        THEN cc.total_logical_writes - pc.total_logical_writes
                        ELSE cc.total_logical_writes
                    END,
                sample_interval_seconds = 
                    CASE 
                        WHEN pc.collection_id IS NULL /*First collection - use plan lifetime*/
                        THEN 
                            DATEDIFF
                            (
                                SECOND, 
                                cc.creation_time, 
                                cc.last_execution_time
                            )
                        ELSE
                            DATEDIFF
                            (
                                SECOND, 
                                pc.collection_time, 
                                cc.collection_time
                            )
                    END
            FROM current_collection AS cc
            LEFT JOIN previous_collection AS pc
              ON  cc.sql_handle = pc.sql_handle
              AND cc.statement_start_offset = pc.statement_start_offset
              AND cc.statement_end_offset = pc.statement_end_offset
              AND cc.plan_handle = pc.plan_handle
              AND pc.row_number = 1
            WHERE cc.row_number = 1
            OPTION(RECOMPILE, HASH JOIN, HASH GROUP);';
        END;

        /*
        Procedure Stats Delta Calculation
        */
        ELSE IF @table_name = N'procedure_stats'
        BEGIN
            SET @sql = N'
            WITH 
                current_collection AS
            (
                SELECT
                    ps.*,
                    row_number = 
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                ps.database_name,
                                ps.object_id,
                                ps.plan_handle
                            ORDER BY
                                ps.collection_time DESC
                        )
                FROM collect.procedure_stats AS ps
                WHERE ps.execution_count_delta IS NULL
            ),
                previous_collection AS
            (
                SELECT
                    ps.collection_id,
                    ps.database_name,
                    ps.object_id,
                    ps.plan_handle,
                    ps.execution_count,
                    ps.total_worker_time,
                    ps.total_elapsed_time,
                    ps.total_logical_reads,
                    ps.total_physical_reads,
                    ps.total_logical_writes,
                    ps.collection_time,
                    row_number = 
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                ps.database_name,
                                ps.object_id,
                                ps.plan_handle
                            ORDER BY
                                ps.collection_time DESC
                        )
                FROM collect.procedure_stats AS ps
                WHERE ps.execution_count_delta IS NOT NULL
                OR ps.collection_id IN
                (
                    SELECT 
                        MIN(ps2.collection_id)
                    FROM collect.procedure_stats AS ps2
                    GROUP BY
                        ps2.database_name,
                        ps2.object_id,
                        ps2.plan_handle
                )
            )
            UPDATE
                cc
            SET
                execution_count_delta =
                    CASE
                        WHEN cc.server_start_time >= pc.collection_time /*Server restart*/
                        THEN cc.execution_count
                        WHEN cc.execution_count >= pc.execution_count
                        THEN cc.execution_count - pc.execution_count
                        ELSE cc.execution_count /*Plan recompiled or cache evicted*/
                    END,
                total_worker_time_delta =
                    CASE
                        WHEN cc.server_start_time >= pc.collection_time /*Server restart*/
                        THEN cc.total_worker_time
                        WHEN cc.total_worker_time >= pc.total_worker_time
                        THEN cc.total_worker_time - pc.total_worker_time
                        ELSE cc.total_worker_time
                    END,
                total_elapsed_time_delta =
                    CASE
                        WHEN cc.server_start_time >= pc.collection_time /*Server restart*/
                        THEN cc.total_elapsed_time
                        WHEN cc.total_elapsed_time >= pc.total_elapsed_time
                        THEN cc.total_elapsed_time - pc.total_elapsed_time
                        ELSE cc.total_elapsed_time
                    END,
                total_logical_reads_delta =
                    CASE
                        WHEN cc.server_start_time >= pc.collection_time /*Server restart*/
                        THEN cc.total_logical_reads
                        WHEN cc.total_logical_reads >= pc.total_logical_reads
                        THEN cc.total_logical_reads - pc.total_logical_reads
                        ELSE cc.total_logical_reads
                    END,
                total_physical_reads_delta =
                    CASE
                        WHEN cc.server_start_time >= pc.collection_time /*Server restart*/
                        THEN cc.total_physical_reads
                        WHEN cc.total_physical_reads >= pc.total_physical_reads
                        THEN cc.total_physical_reads - pc.total_physical_reads
                        ELSE cc.total_physical_reads
                    END,
                total_logical_writes_delta =
                    CASE
                        WHEN cc.server_start_time >= pc.collection_time /*Server restart*/
                        THEN cc.total_logical_writes
                        WHEN cc.total_logical_writes >= pc.total_logical_writes
                        THEN cc.total_logical_writes - pc.total_logical_writes
                        ELSE cc.total_logical_writes
                    END,
                sample_interval_seconds =
                    DATEDIFF
                    (
                        SECOND,
                        pc.collection_time,
                        cc.collection_time
                    )
            FROM current_collection AS cc
            LEFT JOIN previous_collection AS pc
              ON  cc.database_name = pc.database_name
              AND cc.object_id = pc.object_id
              AND cc.plan_handle = pc.plan_handle
              AND pc.row_number = 1
            WHERE cc.row_number = 1
            AND   pc.collection_id IS NOT NULL /*Exclude first collection where no previous exists*/
            OPTION(RECOMPILE, HASH JOIN, HASH GROUP);'
        END;

        /*
        Performance Monitor Stats Delta Calculation
        */
        ELSE IF @table_name = N'perfmon_stats'
        BEGIN
            SET @sql = N'
            WITH 
                current_collection AS
            (
                SELECT
                    ps.*,
                    row_number = 
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                ps.object_name,
                                ps.counter_name,
                                ps.instance_name
                            ORDER BY
                                ps.collection_time DESC
                        )
                FROM collect.perfmon_stats AS ps
                WHERE ps.cntr_value_delta IS NULL
            ),
                previous_collection AS
            (
                SELECT
                    ps.collection_id,
                    ps.object_name,
                    ps.counter_name,
                    ps.instance_name,
                    ps.cntr_value,
                    ps.collection_time,
                    row_number = 
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                ps.object_name,
                                ps.counter_name,
                                ps.instance_name
                            ORDER BY
                                ps.collection_time DESC
                        )
                FROM collect.perfmon_stats AS ps
                WHERE ps.cntr_value_delta IS NOT NULL
                OR ps.collection_id IN
                (
                    SELECT 
                        MIN(ps2.collection_id)
                    FROM collect.perfmon_stats AS ps2
                    GROUP BY
                        ps2.object_name,
                        ps2.counter_name,
                        ps2.instance_name
                )
            )
            UPDATE
                cc
            SET
                cntr_value_delta =
                    CASE
                        WHEN cc.server_start_time >= pc.collection_time
                        THEN cc.cntr_value /*Server restart*/
                        WHEN cc.cntr_value >= pc.cntr_value
                        THEN cc.cntr_value - pc.cntr_value
                        ELSE cc.cntr_value /*Counter wrapped or restart*/
                    END,
                sample_interval_seconds =
                    DATEDIFF(SECOND, pc.collection_time, cc.collection_time)
            FROM current_collection AS cc
            LEFT JOIN previous_collection AS pc
              ON  cc.object_name = pc.object_name
              AND cc.counter_name = pc.counter_name
              AND cc.instance_name = pc.instance_name
              AND pc.row_number = 1
            WHERE cc.row_number = 1
            AND   pc.collection_id IS NOT NULL /*Exclude first collection where no previous exists*/
            OPTION(RECOMPILE, HASH JOIN, HASH GROUP);';
        END;


        /*
        File I/O Stats Delta Calculation
        */
        ELSE IF @table_name = N'file_io_stats'
        BEGIN
            SET @sql = N'
            WITH 
                current_collection AS
            (
                SELECT
                    fios.*,
                    row_number = 
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                fios.database_id,
                                fios.file_id
                            ORDER BY
                                fios.collection_time DESC
                        )
                FROM collect.file_io_stats AS fios
                WHERE fios.num_of_reads_delta IS NULL
            ),
                previous_collection AS
            (
                SELECT
                    fios.collection_id,
                    fios.database_id,
                    fios.file_id,
                    fios.num_of_reads,
                    fios.num_of_bytes_read,
                    fios.io_stall_read_ms,
                    fios.num_of_writes,
                    fios.num_of_bytes_written,
                    fios.io_stall_write_ms,
                    fios.io_stall_ms,
                    fios.io_stall_queued_read_ms,
                    fios.io_stall_queued_write_ms,
                    fios.sample_ms,
                    fios.collection_time,
                    row_number = 
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                fios.database_id,
                                fios.file_id
                            ORDER BY
                                fios.collection_time DESC
                        )
                FROM collect.file_io_stats AS fios
                WHERE fios.num_of_reads_delta IS NOT NULL
                OR fios.collection_id IN
                (
                    SELECT 
                        MIN(fios2.collection_id)
                    FROM collect.file_io_stats AS fios2
                    GROUP BY
                        fios2.database_id,
                        fios2.file_id
                )
            )
            UPDATE
                cc
            SET
                num_of_reads_delta = 
                    CASE 
                        WHEN cc.server_start_time >= pc.collection_time 
                        THEN cc.num_of_reads /*Server restart*/
                        WHEN cc.num_of_reads >= pc.num_of_reads 
                        THEN cc.num_of_reads - pc.num_of_reads
                        ELSE cc.num_of_reads /*Counter wrapped or restart*/
                    END,
                num_of_bytes_read_delta = 
                    CASE 
                        WHEN cc.server_start_time >= pc.collection_time 
                        THEN cc.num_of_bytes_read
                        WHEN cc.num_of_bytes_read >= pc.num_of_bytes_read 
                        THEN cc.num_of_bytes_read - pc.num_of_bytes_read
                        ELSE cc.num_of_bytes_read
                    END,
                io_stall_read_ms_delta = 
                    CASE 
                        WHEN cc.server_start_time >= pc.collection_time 
                        THEN cc.io_stall_read_ms
                        WHEN cc.io_stall_read_ms >= pc.io_stall_read_ms 
                        THEN cc.io_stall_read_ms - pc.io_stall_read_ms
                        ELSE cc.io_stall_read_ms
                    END,
                num_of_writes_delta = 
                    CASE 
                        WHEN cc.server_start_time >= pc.collection_time 
                        THEN cc.num_of_writes
                        WHEN cc.num_of_writes >= pc.num_of_writes 
                        THEN cc.num_of_writes - pc.num_of_writes
                        ELSE cc.num_of_writes
                    END,
                num_of_bytes_written_delta = 
                    CASE 
                        WHEN cc.server_start_time >= pc.collection_time 
                        THEN cc.num_of_bytes_written
                        WHEN cc.num_of_bytes_written >= pc.num_of_bytes_written 
                        THEN cc.num_of_bytes_written - pc.num_of_bytes_written
                        ELSE cc.num_of_bytes_written
                    END,
                io_stall_write_ms_delta = 
                    CASE 
                        WHEN cc.server_start_time >= pc.collection_time 
                        THEN cc.io_stall_write_ms
                        WHEN cc.io_stall_write_ms >= pc.io_stall_write_ms 
                        THEN cc.io_stall_write_ms - pc.io_stall_write_ms
                        ELSE cc.io_stall_write_ms
                    END,
                io_stall_ms_delta = 
                    CASE 
                        WHEN cc.server_start_time >= pc.collection_time 
                        THEN cc.io_stall_ms
                        WHEN cc.io_stall_ms >= pc.io_stall_ms 
                        THEN cc.io_stall_ms - pc.io_stall_ms
                        ELSE cc.io_stall_ms
                    END,
                io_stall_queued_read_ms_delta = 
                    CASE 
                        WHEN cc.server_start_time >= pc.collection_time 
                        THEN cc.io_stall_queued_read_ms
                        WHEN cc.io_stall_queued_read_ms >= pc.io_stall_queued_read_ms 
                        THEN cc.io_stall_queued_read_ms - pc.io_stall_queued_read_ms
                        ELSE cc.io_stall_queued_read_ms
                    END,
                io_stall_queued_write_ms_delta = 
                    CASE 
                        WHEN cc.server_start_time >= pc.collection_time 
                        THEN cc.io_stall_queued_write_ms
                        WHEN cc.io_stall_queued_write_ms >= pc.io_stall_queued_write_ms 
                        THEN cc.io_stall_queued_write_ms - pc.io_stall_queued_write_ms
                        ELSE cc.io_stall_queued_write_ms
                    END,
                sample_ms_delta = 
                    CASE 
                        WHEN cc.server_start_time >= pc.collection_time 
                        THEN cc.sample_ms
                        WHEN cc.sample_ms >= pc.sample_ms 
                        THEN cc.sample_ms - pc.sample_ms
                        ELSE cc.sample_ms
                    END
            FROM current_collection AS cc
            LEFT JOIN previous_collection AS pc
              ON  cc.database_id = pc.database_id
              AND cc.file_id = pc.file_id
              AND pc.row_number = 1
            WHERE cc.row_number = 1
            AND   pc.collection_id IS NOT NULL /*Exclude first collection where no previous exists*/
            OPTION(RECOMPILE, HASH JOIN, HASH GROUP);';
        END;

        /*
        Memory Clerks Stats Delta Calculation
        */
        ELSE IF @table_name = N'memory_clerks_stats'
        BEGIN
            SET @sql = N'
            WITH 
                current_collection AS
            (
                SELECT
                    mcs.*,
                    row_number = 
                        ROW_NUMBER() OVER 
                        (
                            PARTITION BY 
                                mcs.clerk_type, mcs.memory_node_id 
                            ORDER BY 
                                mcs.collection_time DESC
                        )
                FROM collect.memory_clerks_stats AS mcs
                WHERE mcs.pages_kb_delta IS NULL
            ),
                previous_collection AS
            (
                SELECT
                    mcs.collection_id,
                    mcs.clerk_type,
                    mcs.memory_node_id,
                    mcs.pages_kb,
                    mcs.virtual_memory_reserved_kb,
                    mcs.virtual_memory_committed_kb,
                    mcs.awe_allocated_kb,
                    mcs.shared_memory_reserved_kb,
                    mcs.shared_memory_committed_kb,
                    mcs.collection_time,
                    row_number = 
                        ROW_NUMBER() OVER 
                        (
                            PARTITION BY 
                                mcs.clerk_type, 
                                mcs.memory_node_id 
                            ORDER BY 
                                mcs.collection_time DESC
                        )
                FROM collect.memory_clerks_stats AS mcs
                WHERE mcs.pages_kb_delta IS NOT NULL
                OR mcs.collection_id IN
                (
                    SELECT 
                        MIN(mcs2.collection_id)
                    FROM collect.memory_clerks_stats AS mcs2
                    GROUP BY
                        mcs2.clerk_type,
                        mcs2.memory_node_id
                )
            )
            UPDATE cc
            SET
                pages_kb_delta = 
                    CASE 
                        WHEN cc.pages_kb >= pc.pages_kb 
                        THEN cc.pages_kb - pc.pages_kb 
                        ELSE cc.pages_kb 
                    END,
                virtual_memory_reserved_kb_delta = 
                    CASE 
                        WHEN cc.virtual_memory_reserved_kb >= pc.virtual_memory_reserved_kb 
                        THEN cc.virtual_memory_reserved_kb - pc.virtual_memory_reserved_kb 
                        ELSE cc.virtual_memory_reserved_kb 
                    END,
                virtual_memory_committed_kb_delta = 
                    CASE 
                        WHEN cc.virtual_memory_committed_kb >= pc.virtual_memory_committed_kb 
                        THEN cc.virtual_memory_committed_kb - pc.virtual_memory_committed_kb 
                        ELSE cc.virtual_memory_committed_kb 
                    END,
                awe_allocated_kb_delta = 
                    CASE 
                        WHEN cc.awe_allocated_kb >= pc.awe_allocated_kb 
                        THEN cc.awe_allocated_kb - pc.awe_allocated_kb 
                        ELSE cc.awe_allocated_kb 
                    END,
                shared_memory_reserved_kb_delta = 
                    CASE 
                        WHEN cc.shared_memory_reserved_kb >= pc.shared_memory_reserved_kb 
                        THEN cc.shared_memory_reserved_kb - pc.shared_memory_reserved_kb 
                        ELSE cc.shared_memory_reserved_kb 
                    END,
                shared_memory_committed_kb_delta = 
                    CASE 
                        WHEN cc.shared_memory_committed_kb >= pc.shared_memory_committed_kb 
                        THEN cc.shared_memory_committed_kb - pc.shared_memory_committed_kb 
                        ELSE cc.shared_memory_committed_kb 
                    END,
                sample_interval_seconds = 
                    DATEDIFF
                    (
                        SECOND, 
                        pc.collection_time, 
                        cc.collection_time
                    )
            FROM current_collection AS cc
            LEFT JOIN previous_collection AS pc
              ON  cc.clerk_type = pc.clerk_type
              AND cc.memory_node_id = pc.memory_node_id
              AND pc.row_number = 1
            WHERE cc.row_number = 1
            AND   pc.collection_id IS NOT NULL /*Exclude first collection where no previous exists*/
            OPTION(RECOMPILE, HASH JOIN, HASH GROUP);';
        END;

        /*
        Blocking/Deadlock Stats Delta Calculation
        */
        ELSE IF @table_name = N'blocking_deadlock_stats'
        BEGIN
            SET @sql = N'
            WITH 
                current_collection AS
            (
                SELECT
                    bds.*,
                    row_number = 
                        ROW_NUMBER() OVER 
                        (
                            PARTITION BY 
                                bds.database_name 
                            ORDER BY 
                                bds.collection_time DESC
                        )
                FROM collect.blocking_deadlock_stats AS bds
                WHERE bds.blocking_event_count_delta IS NULL
            ),
                previous_collection AS
            (
                SELECT
                    bds.collection_id,
                    bds.database_name,
                    bds.blocking_event_count,
                    bds.total_blocking_duration_ms,
                    bds.max_blocking_duration_ms,
                    bds.deadlock_count,
                    bds.total_deadlock_wait_time_ms,
                    bds.victim_count,
                    bds.collection_time,
                    row_number = 
                        ROW_NUMBER() OVER 
                        (
                            PARTITION BY 
                               bds.database_name
                            ORDER BY 
                                bds.collection_time DESC
                        )
                FROM collect.blocking_deadlock_stats AS bds
                WHERE bds.blocking_event_count_delta IS NOT NULL
                OR bds.collection_id IN
                (
                    SELECT 
                        MIN(bds2.collection_id)
                    FROM collect.blocking_deadlock_stats AS bds2
                    GROUP BY 
                        bds2.database_name
                )
            )
            UPDATE cc
            SET
                blocking_event_count_delta = 
                    CASE 
                        WHEN cc.blocking_event_count >= pc.blocking_event_count 
                        THEN cc.blocking_event_count - pc.blocking_event_count 
                        ELSE cc.blocking_event_count 
                    END,
                total_blocking_duration_ms_delta = 
                    CASE 
                        WHEN cc.total_blocking_duration_ms >= pc.total_blocking_duration_ms 
                        THEN cc.total_blocking_duration_ms - pc.total_blocking_duration_ms 
                        ELSE cc.total_blocking_duration_ms 
                    END,
                max_blocking_duration_ms_delta = 
                    CASE 
                        WHEN cc.max_blocking_duration_ms >= pc.max_blocking_duration_ms 
                        THEN cc.max_blocking_duration_ms - pc.max_blocking_duration_ms 
                        ELSE cc.max_blocking_duration_ms 
                    END,
                deadlock_count_delta = 
                    CASE 
                        WHEN cc.deadlock_count >= pc.deadlock_count 
                        THEN cc.deadlock_count - pc.deadlock_count 
                        ELSE cc.deadlock_count 
                     END,
                total_deadlock_wait_time_ms_delta = 
                    CASE 
                        WHEN cc.total_deadlock_wait_time_ms >= pc.total_deadlock_wait_time_ms 
                        THEN cc.total_deadlock_wait_time_ms - pc.total_deadlock_wait_time_ms 
                        ELSE cc.total_deadlock_wait_time_ms 
                    END,
                victim_count_delta = 
                    CASE 
                        WHEN cc.victim_count >= pc.victim_count 
                        THEN cc.victim_count - pc.victim_count 
                        ELSE cc.victim_count 
                    END,
                sample_interval_seconds = 
                    DATEDIFF(SECOND, pc.collection_time, cc.collection_time)
            FROM current_collection AS cc
            LEFT JOIN previous_collection AS pc
              ON  cc.database_name = pc.database_name
              AND pc.row_number = 1
            WHERE cc.row_number = 1
            AND   pc.collection_id IS NOT NULL /*Exclude first collection where no previous exists*/
            OPTION(RECOMPILE, HASH JOIN, HASH GROUP);';
        END;

        /*
        Latch Stats Delta Calculation
        */
        ELSE IF @table_name = N'latch_stats'
        BEGIN
            SET @sql = N'
            WITH
                current_collection AS
            (
                SELECT
                    ls.*,
                    row_number =
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                ls.server_start_time,
                                ls.latch_class
                            ORDER BY
                                ls.collection_time DESC
                        )
                FROM collect.latch_stats AS ls
                WHERE ls.waiting_requests_count_delta IS NULL
            ),
                previous_collection AS
            (
                SELECT
                    ls.collection_id,
                    ls.server_start_time,
                    ls.latch_class,
                    ls.waiting_requests_count,
                    ls.wait_time_ms,
                    ls.max_wait_time_ms,
                    ls.collection_time,
                    row_number =
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                ls.server_start_time,
                                ls.latch_class
                            ORDER BY
                                ls.collection_time DESC
                        )
                FROM collect.latch_stats AS ls
                WHERE ls.waiting_requests_count_delta IS NOT NULL
                OR ls.collection_id IN
                (
                    SELECT
                        MIN(ls2.collection_id)
                    FROM collect.latch_stats AS ls2
                    GROUP BY
                        ls2.server_start_time,
                        ls2.latch_class
                )
            )
            UPDATE cc
            SET
                waiting_requests_count_delta =
                    CASE
                        WHEN cc.waiting_requests_count >= pc.waiting_requests_count
                        THEN cc.waiting_requests_count - pc.waiting_requests_count
                        ELSE cc.waiting_requests_count
                    END,
                wait_time_ms_delta =
                    CASE
                        WHEN cc.wait_time_ms >= pc.wait_time_ms
                        THEN cc.wait_time_ms - pc.wait_time_ms
                        ELSE cc.wait_time_ms
                    END,
                max_wait_time_ms_delta =
                    CASE
                        WHEN cc.max_wait_time_ms >= pc.max_wait_time_ms
                        THEN cc.max_wait_time_ms - pc.max_wait_time_ms
                        ELSE cc.max_wait_time_ms
                    END,
                sample_interval_seconds =
                    DATEDIFF(SECOND, pc.collection_time, cc.collection_time)
            FROM current_collection AS cc
            LEFT JOIN previous_collection AS pc
              ON  cc.server_start_time = pc.server_start_time
              AND cc.latch_class = pc.latch_class
              AND pc.row_number = 1
            WHERE cc.row_number = 1
            AND   pc.collection_id IS NOT NULL /*Exclude first collection where no previous exists*/
            OPTION(RECOMPILE, HASH JOIN, HASH GROUP);';
        END;

        /*
        Spinlock Stats Delta Calculation
        */
        ELSE IF @table_name = N'spinlock_stats'
        BEGIN
            SET @sql = N'
            WITH
                current_collection AS
            (
                SELECT
                    ss.*,
                    row_number =
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                ss.server_start_time,
                                ss.spinlock_name
                            ORDER BY
                                ss.collection_time DESC
                        )
                FROM collect.spinlock_stats AS ss
                WHERE ss.collisions_delta IS NULL
            ),
                previous_collection AS
            (
                SELECT
                    ss.collection_id,
                    ss.server_start_time,
                    ss.spinlock_name,
                    ss.collisions,
                    ss.spins,
                    ss.spins_per_collision,
                    ss.sleep_time,
                    ss.backoffs,
                    ss.collection_time,
                    row_number =
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                ss.server_start_time,
                                ss.spinlock_name
                            ORDER BY
                                ss.collection_time DESC
                        )
                FROM collect.spinlock_stats AS ss
                WHERE ss.collisions_delta IS NOT NULL
                OR ss.collection_id IN
                (
                    SELECT
                        MIN(ss2.collection_id)
                    FROM collect.spinlock_stats AS ss2
                    GROUP BY
                        ss2.server_start_time,
                        ss2.spinlock_name
                )
            )
            UPDATE cc
            SET
                collisions_delta =
                    CASE
                        WHEN cc.collisions >= pc.collisions
                        THEN cc.collisions - pc.collisions
                        ELSE cc.collisions
                    END,
                spins_delta =
                    CASE
                        WHEN cc.spins >= pc.spins
                        THEN cc.spins - pc.spins
                        ELSE cc.spins
                    END,
                sleep_time_delta =
                    CASE
                        WHEN cc.sleep_time >= pc.sleep_time
                        THEN cc.sleep_time - pc.sleep_time
                        ELSE cc.sleep_time
                    END,
                backoffs_delta =
                    CASE
                        WHEN cc.backoffs >= pc.backoffs
                        THEN cc.backoffs - pc.backoffs
                        ELSE cc.backoffs
                    END,
                sample_interval_seconds =
                    DATEDIFF(SECOND, pc.collection_time, cc.collection_time)
            FROM current_collection AS cc
            LEFT JOIN previous_collection AS pc
              ON  cc.server_start_time = pc.server_start_time
              AND cc.spinlock_name = pc.spinlock_name
              AND pc.row_number = 1
            WHERE cc.row_number = 1
            AND   pc.collection_id IS NOT NULL /*Exclude first collection where no previous exists*/
            OPTION(RECOMPILE, HASH JOIN, HASH GROUP);';
        END;

        /*
        Memory Grant Stats Delta Calculation
        */
        ELSE IF @table_name = N'memory_grant_stats'
        BEGIN
            SET @sql = N'
            WITH
                current_collection AS
            (
                SELECT
                    mgs.*,
                    row_number =
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                mgs.resource_semaphore_id,
                                mgs.pool_id
                            ORDER BY
                                mgs.collection_time DESC
                        )
                FROM collect.memory_grant_stats AS mgs
                WHERE mgs.timeout_error_count_delta IS NULL
            ),
                previous_collection AS
            (
                SELECT
                    mgs.collection_id,
                    mgs.resource_semaphore_id,
                    mgs.pool_id,
                    mgs.timeout_error_count,
                    mgs.forced_grant_count,
                    mgs.collection_time,
                    row_number =
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                mgs.resource_semaphore_id,
                                mgs.pool_id
                            ORDER BY
                                mgs.collection_time DESC
                        )
                FROM collect.memory_grant_stats AS mgs
                WHERE mgs.timeout_error_count_delta IS NOT NULL
                OR    mgs.collection_id IN
                (
                    SELECT
                        MIN(mgs2.collection_id)
                    FROM collect.memory_grant_stats AS mgs2
                    GROUP BY
                        mgs2.resource_semaphore_id,
                        mgs2.pool_id
                )
            )
            UPDATE
                cc
            SET
                timeout_error_count_delta =
                    CASE
                        WHEN cc.server_start_time >= pc.collection_time
                        THEN cc.timeout_error_count /*Server restart*/
                        WHEN cc.timeout_error_count >= pc.timeout_error_count
                        THEN cc.timeout_error_count - pc.timeout_error_count
                        ELSE cc.timeout_error_count /*Counter wrapped or restart*/
                    END,
                forced_grant_count_delta =
                    CASE
                        WHEN cc.server_start_time >= pc.collection_time
                        THEN cc.forced_grant_count /*Server restart*/
                        WHEN cc.forced_grant_count >= pc.forced_grant_count
                        THEN cc.forced_grant_count - pc.forced_grant_count
                        ELSE cc.forced_grant_count /*Counter wrapped or restart*/
                    END,
                sample_interval_seconds =
                    DATEDIFF(SECOND, pc.collection_time, cc.collection_time)
            FROM current_collection AS cc
            LEFT JOIN previous_collection AS pc
              ON  cc.resource_semaphore_id = pc.resource_semaphore_id
              AND cc.pool_id = pc.pool_id
              AND pc.row_number = 1
            WHERE cc.row_number = 1
            AND   pc.collection_id IS NOT NULL /*Exclude first collection where no previous exists*/
            OPTION(RECOMPILE, HASH JOIN, HASH GROUP);';
        END;

        ELSE
        BEGIN
            RAISERROR(N'Unknown table name for delta calculation: %s', 16, 1, @table_name);
            RETURN;
        END;
        
        /*
        Execute the delta calculation
        */
        IF @debug = 1
        BEGIN
            PRINT @sql;
        END;
        
        EXECUTE sys.sp_executesql 
            @sql;
        
        SET @rows_updated = ROWCOUNT_BIG();
        
        /*
        Log the delta calculation
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
            N'calculate_deltas_' + @table_name,
            N'SUCCESS',
            @rows_updated,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );
        
        IF @debug = 1
        BEGIN
            RAISERROR(N'Updated %d rows with delta calculations for %s', 0, 1, @rows_updated, @table_name) WITH NOWAIT;
        END;

        /*
        Only commit if we started the transaction
        */
        IF @trancount_at_entry = 0
        BEGIN
            COMMIT TRANSACTION;
        END;

    END TRY
    BEGIN CATCH
        /*
        Only rollback if we started the transaction
        Otherwise let the caller handle it
        */
        IF  @trancount_at_entry = 0 
        AND @@TRANCOUNT > 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;

        DECLARE
            @error_message nvarchar(4000) = ERROR_MESSAGE();
        
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
            N'calculate_deltas_' + @table_name,
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );
        
        RAISERROR(N'Error calculating deltas for %s: %s', 16, 1, @table_name, @error_message);
    END CATCH;
END;
GO

PRINT 'Delta calculation framework created successfully';
GO
