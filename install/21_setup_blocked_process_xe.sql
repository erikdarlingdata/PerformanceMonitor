/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Extended Events Session Setup for Blocked Process Reports and Deadlocks
Creates ring_buffer sessions that work across:
  - On-premises SQL Server
  - Azure SQL Managed Instance
  - AWS RDS for SQL Server

Note: Azure SQL DB requires database-scoped sessions. The collection
procedures (scripts 22 and 24) create and start those at the top of every
run (#1086); they also re-create/re-start the server-scoped sessions below
if they are later dropped or stopped, so this script is the initial setup,
not the only creation path.
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

/*
=============================================================================
BLOCKED PROCESS THRESHOLD CONFIGURATION
=============================================================================
*/

BEGIN TRY
    DECLARE
        @blocked_threshold_current integer,
        @show_advanced_current integer,
        @show_advanced_reset integer;

    /* Check current values */
    SELECT
        @show_advanced_current = CONVERT(integer, c.value),
        @show_advanced_reset = CONVERT(integer, c.value)
    FROM sys.configurations AS c
    WHERE c.name = N'show advanced options';

    SELECT
        @blocked_threshold_current = CONVERT(integer, c.value)
    FROM sys.configurations AS c
    WHERE c.name = N'blocked process threshold (s)';

    /* Enable show advanced options if not already enabled */
    IF @show_advanced_current = 0
    BEGIN
        PRINT 'Enabling show advanced options...';
        EXECUTE sys.sp_configure N'show advanced options', 1;
        RECONFIGURE;
    END;

    /* Configure blocked process threshold if not already set */
    IF @blocked_threshold_current = 0
    BEGIN
        PRINT 'Configuring blocked process threshold to 5 seconds...';
        EXECUTE sys.sp_configure N'blocked process threshold (s)', 5;
        RECONFIGURE;
        PRINT 'Blocked process threshold configured successfully';
    END;
    ELSE
    BEGIN
        PRINT 'Blocked process threshold already configured: ' + CONVERT(varchar(10), @blocked_threshold_current) + ' seconds';
    END;

    /* Reset show advanced options if it was disabled */
    IF @show_advanced_reset = 0
    BEGIN
        PRINT 'Resetting show advanced options...';
        EXECUTE sys.sp_configure N'show advanced options', 0;
        RECONFIGURE;
    END;
END TRY
BEGIN CATCH
    PRINT 'Note: Blocked process threshold configuration skipped (insufficient permissions for sp_configure/RECONFIGURE).';
    PRINT 'On AWS RDS, configure this via the RDS parameter group instead.';
END CATCH;
GO

/*
=============================================================================
BLOCKED PROCESS REPORT EXTENDED EVENTS SESSION
Session Name: PerformanceMonitor_BlockedProcess
Target: ring_buffer (4MB) - works on all platforms
=============================================================================
*/

DECLARE
    @session_name sysname = N'PerformanceMonitor_BlockedProcess',
    @session_exists bit = 0,
    @session_running bit = 0;

/* Check if session exists and is running */
IF EXISTS
(
    SELECT
        1/0
    FROM sys.server_event_sessions AS ses
    WHERE ses.name = @session_name
)
BEGIN
    SET @session_exists = 1;

    IF EXISTS
    (
        SELECT
            1/0
        FROM sys.dm_xe_sessions AS dxs
        WHERE dxs.name = @session_name
    )
    BEGIN
        SET @session_running = 1;
    END;
END;

/* Handle existing session */
IF @session_exists = 1
BEGIN
    IF @session_running = 0
    BEGIN
        /* Session exists but stopped - start it */
        PRINT 'Starting existing ' + @session_name + ' session...';

        ALTER EVENT SESSION
            [PerformanceMonitor_BlockedProcess]
        ON SERVER
            STATE = START;

        PRINT 'Session started successfully';
    END;
    ELSE
    BEGIN
        PRINT @session_name + ' session is already running';
    END;
END;
ELSE
BEGIN
    /* Create new session with ring_buffer target */
    PRINT 'Creating ' + @session_name + ' Extended Events session with ring_buffer target...';

    CREATE EVENT SESSION
        [PerformanceMonitor_BlockedProcess]
    ON SERVER
        ADD EVENT
            sqlserver.blocked_process_report
        ADD TARGET
            package0.ring_buffer
        (
            SET max_memory = 4096
        )
    WITH
    (
        MAX_MEMORY = 4096KB,
        EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,
        MAX_DISPATCH_LATENCY = 5 SECONDS,
        MEMORY_PARTITION_MODE = NONE,
        STARTUP_STATE = ON
    );

    PRINT 'Session created successfully';

    /* Start the session */
    ALTER EVENT SESSION
        [PerformanceMonitor_BlockedProcess]
    ON SERVER
        STATE = START;

    PRINT 'Session started successfully';
END;
GO

/* Cleanup old session name if it exists */
IF EXISTS
(
    SELECT
        1/0
    FROM sys.server_event_sessions AS ses
    WHERE ses.name = N'blocked_process_report'
)
BEGIN
    PRINT '';
    PRINT 'Cleaning up old blocked_process_report session...';

    IF EXISTS
    (
        SELECT
            1/0
        FROM sys.dm_xe_sessions AS xs
        WHERE xs.name = N'blocked_process_report'
    )
    BEGIN
        ALTER EVENT SESSION
            blocked_process_report
        ON SERVER
            STATE = STOP;
    END;

    DROP EVENT SESSION
        blocked_process_report
    ON SERVER;

    PRINT 'Old session removed successfully';
END;
GO

/*
=============================================================================
DEADLOCK EXTENDED EVENTS SESSION
Session Name: PerformanceMonitor_Deadlock
Target: ring_buffer (4MB) - works on all platforms
=============================================================================
*/

DECLARE
    @session_name sysname = N'PerformanceMonitor_Deadlock',
    @session_exists bit = 0,
    @session_running bit = 0;

/* Check if session exists and is running */
IF EXISTS
(
    SELECT
        1/0
    FROM sys.server_event_sessions AS ses
    WHERE ses.name = @session_name
)
BEGIN
    SET @session_exists = 1;

    IF EXISTS
    (
        SELECT
            1/0
        FROM sys.dm_xe_sessions AS dxs
        WHERE dxs.name = @session_name
    )
    BEGIN
        SET @session_running = 1;
    END;
END;

/* Handle existing session */
IF @session_exists = 1
BEGIN
    IF @session_running = 0
    BEGIN
        /* Session exists but stopped - start it */
        PRINT '';
        PRINT 'Starting existing ' + @session_name + ' session...';

        ALTER EVENT SESSION
            [PerformanceMonitor_Deadlock]
        ON SERVER
            STATE = START;

        PRINT 'Session started successfully';
    END;
    ELSE
    BEGIN
        PRINT '';
        PRINT @session_name + ' session is already running';
    END;
END;
ELSE
BEGIN
    /* Create new session with ring_buffer target */
    PRINT '';
    PRINT 'Creating ' + @session_name + ' Extended Events session with ring_buffer target...';

    CREATE EVENT SESSION
        [PerformanceMonitor_Deadlock]
    ON SERVER
        ADD EVENT
            sqlserver.xml_deadlock_report
        ADD TARGET
            package0.ring_buffer
        (
            SET max_memory = 4096
        )
    WITH
    (
        MAX_MEMORY = 4096KB,
        EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,
        MAX_DISPATCH_LATENCY = 5 SECONDS,
        MEMORY_PARTITION_MODE = NONE,
        STARTUP_STATE = ON
    );

    PRINT 'Session created successfully';

    /* Start the session */
    ALTER EVENT SESSION
        [PerformanceMonitor_Deadlock]
    ON SERVER
        STATE = START;

    PRINT 'Session started successfully';
END;
GO

/* Cleanup old session name if it exists */
IF EXISTS
(
    SELECT
        1/0
    FROM sys.server_event_sessions AS ses
    WHERE ses.name = N'deadlock'
)
BEGIN
    PRINT '';
    PRINT 'Cleaning up old deadlock session...';

    IF EXISTS
    (
        SELECT
            1/0
        FROM sys.dm_xe_sessions AS xs
        WHERE xs.name = N'deadlock'
    )
    BEGIN
        ALTER EVENT SESSION
            deadlock
        ON SERVER
            STATE = STOP;
    END;

    DROP EVENT SESSION
        deadlock
    ON SERVER;

    PRINT 'Old session removed successfully';
END;
GO

/*
=============================================================================
VERIFY SESSIONS ARE RUNNING
=============================================================================
*/

PRINT '';
PRINT 'Extended Events Session Status:';
PRINT '===============================';

SELECT
    session_name = ses.name,
    is_running =
        CASE
            WHEN dxs.name IS NOT NULL
            THEN N'YES'
            ELSE N'NO'
        END,
    startup_state =
        CASE ses.startup_state
            WHEN 0 THEN N'OFF'
            WHEN 1 THEN N'ON'
        END,
    target_type = N'ring_buffer',
    max_memory_kb = ses.max_memory,
    event_retention_mode = ses.event_retention_mode_desc,
    max_dispatch_latency_seconds = ses.max_dispatch_latency / 1000
FROM sys.server_event_sessions AS ses
LEFT JOIN sys.dm_xe_sessions AS dxs
  ON dxs.name = ses.name
WHERE ses.name IN
(
    N'PerformanceMonitor_BlockedProcess',
    N'PerformanceMonitor_Deadlock'
)
ORDER BY
    ses.name;

PRINT '';
PRINT '=============================================================================';
PRINT 'Extended Events setup complete:';
PRINT '  - PerformanceMonitor_BlockedProcess: ring_buffer target (4MB)';
PRINT '  - PerformanceMonitor_Deadlock: ring_buffer target (4MB)';
PRINT '';
PRINT 'Both sessions use ring_buffer targets which work on:';
PRINT '  - On-premises SQL Server';
PRINT '  - Azure SQL Managed Instance';
PRINT '  - AWS RDS for SQL Server';
PRINT '';
PRINT 'Note: Azure SQL DB uses database-scoped sessions created by collection';
PRINT '      procedures (scripts 22 and 24).';
PRINT '=============================================================================';
GO
