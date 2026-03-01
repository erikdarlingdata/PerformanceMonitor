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
File I/O statistics collector
Collects raw cumulative I/O statistics from sys.dm_io_virtual_file_stats
Stores raw values for delta calculation - no pre-converted or pre-calculated metrics
*/

IF OBJECT_ID(N'collect.file_io_stats_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.file_io_stats_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.file_io_stats_collector
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
                osi.sqlserver_start_time
            FROM sys.dm_os_sys_info AS osi
        ),
        @error_message nvarchar(4000);

    BEGIN TRY
        BEGIN TRANSACTION;

        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.file_io_stats', N'U') IS NULL
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
                N'file_io_stats_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.file_io_stats does not exist, calling ensure procedure'
            );

            /*
            Call procedure to create table
            */
            EXECUTE config.ensure_collection_table
                @table_name = N'file_io_stats',
                @debug = @debug;

            /*
            Verify table now exists
            */
            IF OBJECT_ID(N'collect.file_io_stats', N'U') IS NULL
            BEGIN
                RAISERROR(N'Table collect.file_io_stats still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Collect raw cumulative I/O statistics for all database files
        Stores raw bytes and milliseconds - no pre-conversion to GB or averages
        Delta framework will calculate rates and averages
        */
        INSERT INTO
            collect.file_io_stats
        (
            server_start_time,
            database_id,
            database_name,
            file_id,
            file_name,
            file_type_desc,
            physical_name,
            size_on_disk_bytes,
            num_of_reads,
            num_of_bytes_read,
            io_stall_read_ms,
            num_of_writes,
            num_of_bytes_written,
            io_stall_write_ms,
            io_stall_ms,
            io_stall_queued_read_ms,
            io_stall_queued_write_ms,
            sample_ms
        )
        SELECT
            server_start_time = @server_start_time,
            database_id = vfs.database_id,
            database_name =
                ISNULL
                (
                    d.name,
                    N'UNKNOWN'
                ),
            file_id = vfs.file_id,
            file_name =
                ISNULL
                (
                    mf.name,
                    N'UNKNOWN'
                ),
            file_type_desc =
                ISNULL
                (
                    mf.type_desc,
                    N'UNKNOWN'
                ),
            physical_name = mf.physical_name,
            size_on_disk_bytes = vfs.size_on_disk_bytes,
            num_of_reads = vfs.num_of_reads,
            num_of_bytes_read = vfs.num_of_bytes_read,
            io_stall_read_ms = vfs.io_stall_read_ms,
            num_of_writes = vfs.num_of_writes,
            num_of_bytes_written = vfs.num_of_bytes_written,
            io_stall_write_ms = vfs.io_stall_write_ms,
            io_stall_ms = vfs.io_stall,
            io_stall_queued_read_ms = vfs.io_stall_queued_read_ms,
            io_stall_queued_write_ms = vfs.io_stall_queued_write_ms,
            sample_ms = vfs.sample_ms
        FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS vfs
        LEFT JOIN sys.databases AS d
          ON  d.database_id = vfs.database_id
        LEFT JOIN sys.master_files AS mf
          ON  mf.database_id = vfs.database_id
          AND mf.file_id = vfs.file_id
        WHERE (vfs.num_of_reads > 0 OR vfs.num_of_writes > 0)
        AND   vfs.database_id NOT IN
        (
            DB_ID(N'PerformanceMonitor'),
            DB_ID(N'master'),  /*1*/
            DB_ID(N'model'),   /*3*/
            DB_ID(N'msdb')     /*4*/
        )
        AND   vfs.database_id < 32761 /*exclude resource database and contained AG system databases*/
        OPTION(RECOMPILE);

        SET @rows_collected = ROWCOUNT_BIG();
        
        /*Calculate deltas for the newly inserted data*/        
        EXECUTE collect.calculate_deltas            
            @table_name = N'file_io_stats',            
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
            N'file_io_stats_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %d file I/O stats rows', 0, 1, @rows_collected) WITH NOWAIT;
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
            N'file_io_stats_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in file I/O stats collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'File I/O stats collector created successfully';
PRINT 'Collects raw cumulative I/O metrics from sys.dm_io_virtual_file_stats';
PRINT 'Delta framework will calculate rates and averages from raw values';
GO
