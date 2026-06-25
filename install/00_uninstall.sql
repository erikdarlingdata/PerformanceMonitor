/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 *
 * Uninstall script - removes all Performance Monitor objects from SQL Server.
 *
 * Removes:
 *   - Server-side traces (must happen before database drop)
 *   - SQL Agent jobs (3 jobs in msdb)
 *   - Extended Events sessions (2 server-level sessions)
 *   - PerformanceMonitor database
 *
 * Does NOT reset:
 *   - blocked process threshold (s) sp_configure setting
 *     (other monitoring tools may depend on it)
 *
 * Safe to run multiple times (all operations are idempotent).
 */

USE master;
GO

SET NOCOUNT ON;
GO

PRINT '================================================================================';
PRINT 'Performance Monitor Uninstaller';
PRINT '================================================================================';
PRINT '';
GO

/*
Stop server-side traces before dropping database.
The trace_management_collector procedure lives in the PerformanceMonitor database,
so this must happen first.
*/
IF EXISTS
(
    SELECT
        1/0
    FROM sys.databases AS d
    WHERE d.name = N'PerformanceMonitor'
)
AND OBJECT_ID(N'PerformanceMonitor.collect.trace_management_collector', N'P') IS NOT NULL
BEGIN
    PRINT 'Stopping server-side traces...';

    BEGIN TRY
        EXECUTE PerformanceMonitor.collect.trace_management_collector
            @action = 'STOP';

        PRINT 'Server-side traces stopped';
    END TRY
    BEGIN CATCH
        PRINT 'Note: Could not stop traces (may not be running)';
    END CATCH;
END;
ELSE
BEGIN
    PRINT 'No traces to stop (database or procedure not found)';
END;
GO

PRINT '';
GO

/*
Delete SQL Agent jobs from msdb.
*/
IF EXISTS
(
    SELECT
        1/0
    FROM msdb.dbo.sysjobs AS sj
    WHERE sj.name = N'PerformanceMonitor - Collection'
)
BEGIN
    EXECUTE msdb.dbo.sp_delete_job
        @job_name = N'PerformanceMonitor - Collection',
        @delete_unused_schedule = 1;

    PRINT 'Deleted job: PerformanceMonitor - Collection';
END;
ELSE
BEGIN
    PRINT 'Job not found: PerformanceMonitor - Collection';
END;

IF EXISTS
(
    SELECT
        1/0
    FROM msdb.dbo.sysjobs AS sj
    WHERE sj.name = N'PerformanceMonitor - Data Retention'
)
BEGIN
    EXECUTE msdb.dbo.sp_delete_job
        @job_name = N'PerformanceMonitor - Data Retention',
        @delete_unused_schedule = 1;

    PRINT 'Deleted job: PerformanceMonitor - Data Retention';
END;
ELSE
BEGIN
    PRINT 'Job not found: PerformanceMonitor - Data Retention';
END;

IF EXISTS
(
    SELECT
        1/0
    FROM msdb.dbo.sysjobs AS sj
    WHERE sj.name = N'PerformanceMonitor - Hung Job Monitor'
)
BEGIN
    EXECUTE msdb.dbo.sp_delete_job
        @job_name = N'PerformanceMonitor - Hung Job Monitor',
        @delete_unused_schedule = 1;

    PRINT 'Deleted job: PerformanceMonitor - Hung Job Monitor';
END;
ELSE
BEGIN
    PRINT 'Job not found: PerformanceMonitor - Hung Job Monitor';
END;
GO

PRINT '';
GO

/*
Drop Extended Events sessions.
Stop running sessions before dropping.
*/
IF EXISTS
(
    SELECT
        1/0
    FROM sys.server_event_sessions AS ses
    WHERE ses.name = N'PerformanceMonitor_BlockedProcess'
)
BEGIN
    IF EXISTS
    (
        SELECT
            1/0
        FROM sys.dm_xe_sessions AS dxs
        WHERE dxs.name = N'PerformanceMonitor_BlockedProcess'
    )
    BEGIN
        ALTER EVENT SESSION
            [PerformanceMonitor_BlockedProcess]
        ON SERVER
            STATE = STOP;
    END;

    DROP EVENT SESSION
        [PerformanceMonitor_BlockedProcess]
    ON SERVER;

    PRINT 'Dropped Extended Events session: PerformanceMonitor_BlockedProcess';
END;
ELSE
BEGIN
    PRINT 'XE session not found: PerformanceMonitor_BlockedProcess';
END;

IF EXISTS
(
    SELECT
        1/0
    FROM sys.server_event_sessions AS ses
    WHERE ses.name = N'PerformanceMonitor_Deadlock'
)
BEGIN
    IF EXISTS
    (
        SELECT
            1/0
        FROM sys.dm_xe_sessions AS dxs
        WHERE dxs.name = N'PerformanceMonitor_Deadlock'
    )
    BEGIN
        ALTER EVENT SESSION
            [PerformanceMonitor_Deadlock]
        ON SERVER
            STATE = STOP;
    END;

    DROP EVENT SESSION
        [PerformanceMonitor_Deadlock]
    ON SERVER;

    PRINT 'Dropped Extended Events session: PerformanceMonitor_Deadlock';
END;
ELSE
BEGIN
    PRINT 'XE session not found: PerformanceMonitor_Deadlock';
END;
GO

PRINT '';
GO

/*
Drop the PerformanceMonitor database.
SET SINGLE_USER forces all connections closed.
*/
IF EXISTS
(
    SELECT
        1/0
    FROM sys.databases AS d
    WHERE d.name = N'PerformanceMonitor'
)
BEGIN
    PRINT 'Dropping PerformanceMonitor database...';

    ALTER DATABASE [PerformanceMonitor]
        SET SINGLE_USER
        WITH ROLLBACK IMMEDIATE;

    DROP DATABASE [PerformanceMonitor];

    PRINT 'PerformanceMonitor database dropped';
END;
ELSE
BEGIN
    PRINT 'PerformanceMonitor database not found';
END;
GO

PRINT '';
PRINT '================================================================================';
PRINT 'Uninstall complete';
PRINT '================================================================================';
PRINT '';
PRINT 'Note: blocked process threshold (s) was NOT reset.';
PRINT 'If no other tools use it, you can reset it manually:';
PRINT '  EXECUTE sp_configure ''show advanced options'', 1; RECONFIGURE;';
PRINT '  EXECUTE sp_configure ''blocked process threshold (s)'', 0; RECONFIGURE;';
PRINT '  EXECUTE sp_configure ''show advanced options'', 0; RECONFIGURE;';
GO
