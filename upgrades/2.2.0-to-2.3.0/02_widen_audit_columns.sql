/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 2.2.0 to 2.3.0
Widens collector table columns to match DMV documentation:

cpu_scheduler_stats:
  - total_work_queue_count: integer -> bigint (dm_os_schedulers.work_queue_count is bigint)
  - total_active_parallel_thread_count: integer -> bigint (dm_resource_governor_workload_groups.active_parallel_thread_count is bigint)
  - system_memory_state_desc: nvarchar(120) -> nvarchar(256) (dm_os_sys_memory documents nvarchar(256))

waiting_tasks:
  - resource_description: nvarchar(1000) -> nvarchar(3072) (dm_os_waiting_tasks documents nvarchar(3072))

database_size_stats:
  - recovery_model_desc: nvarchar(12) -> nvarchar(60) (sys.databases documents nvarchar(60))
  - volume_mount_point: nvarchar(256) -> nvarchar(512) (dm_os_volume_stats documents nvarchar(512))
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
cpu_scheduler_stats: widen integer columns to bigint
*/
IF OBJECT_ID(N'collect.cpu_scheduler_stats', N'U') IS NOT NULL
BEGIN
    PRINT 'Checking collect.cpu_scheduler_stats columns...';

    IF EXISTS
    (
        SELECT
            1/0
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = N'collect'
        AND   TABLE_NAME = N'cpu_scheduler_stats'
        AND   COLUMN_NAME = N'total_work_queue_count'
        AND   DATA_TYPE = N'int'
    )
    BEGIN
        ALTER TABLE collect.cpu_scheduler_stats ALTER COLUMN total_work_queue_count bigint NULL;
        PRINT '  total_work_queue_count: int -> bigint';
    END;

    IF EXISTS
    (
        SELECT
            1/0
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = N'collect'
        AND   TABLE_NAME = N'cpu_scheduler_stats'
        AND   COLUMN_NAME = N'total_active_parallel_thread_count'
        AND   DATA_TYPE = N'int'
    )
    BEGIN
        ALTER TABLE collect.cpu_scheduler_stats ALTER COLUMN total_active_parallel_thread_count bigint NULL;
        PRINT '  total_active_parallel_thread_count: int -> bigint';
    END;

    IF EXISTS
    (
        SELECT
            1/0
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = N'collect'
        AND   TABLE_NAME = N'cpu_scheduler_stats'
        AND   COLUMN_NAME = N'system_memory_state_desc'
        AND   CHARACTER_MAXIMUM_LENGTH = 120
    )
    BEGIN
        ALTER TABLE collect.cpu_scheduler_stats ALTER COLUMN system_memory_state_desc nvarchar(256) NULL;
        PRINT '  system_memory_state_desc: nvarchar(120) -> nvarchar(256)';
    END;

    PRINT 'cpu_scheduler_stats complete.';
END;
GO

/*
waiting_tasks: widen resource_description
*/
IF OBJECT_ID(N'collect.waiting_tasks', N'U') IS NOT NULL
BEGIN
    PRINT 'Checking collect.waiting_tasks columns...';

    IF EXISTS
    (
        SELECT
            1/0
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = N'collect'
        AND   TABLE_NAME = N'waiting_tasks'
        AND   COLUMN_NAME = N'resource_description'
        AND   CHARACTER_MAXIMUM_LENGTH = 1000
    )
    BEGIN
        ALTER TABLE collect.waiting_tasks ALTER COLUMN resource_description nvarchar(3072) NULL;
        PRINT '  resource_description: nvarchar(1000) -> nvarchar(3072)';
    END;

    PRINT 'waiting_tasks complete.';
END;
GO

/*
database_size_stats: widen string columns
*/
IF OBJECT_ID(N'collect.database_size_stats', N'U') IS NOT NULL
BEGIN
    PRINT 'Checking collect.database_size_stats columns...';

    IF EXISTS
    (
        SELECT
            1/0
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = N'collect'
        AND   TABLE_NAME = N'database_size_stats'
        AND   COLUMN_NAME = N'recovery_model_desc'
        AND   CHARACTER_MAXIMUM_LENGTH = 12
    )
    BEGIN
        ALTER TABLE collect.database_size_stats ALTER COLUMN recovery_model_desc nvarchar(60) NULL;
        PRINT '  recovery_model_desc: nvarchar(12) -> nvarchar(60)';
    END;

    IF EXISTS
    (
        SELECT
            1/0
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = N'collect'
        AND   TABLE_NAME = N'database_size_stats'
        AND   COLUMN_NAME = N'volume_mount_point'
        AND   CHARACTER_MAXIMUM_LENGTH = 256
    )
    BEGIN
        ALTER TABLE collect.database_size_stats ALTER COLUMN volume_mount_point nvarchar(512) NULL;
        PRINT '  volume_mount_point: nvarchar(256) -> nvarchar(512)';
    END;

    PRINT 'database_size_stats complete.';
END;
GO
