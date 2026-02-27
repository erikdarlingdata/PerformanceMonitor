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

USE msdb;
GO

/*
Create SQL Server Agent Jobs for Performance Monitor
These jobs automate data collection and retention

When @preserve_jobs = 1, existing jobs are left untouched (owner,
schedule, notifications, etc.) and only missing jobs are created.
The installer sets this to 1 when --preserve-jobs is specified.
*/

DECLARE
    @preserve_jobs bit = 0;

PRINT 'Creating SQL Server Agent jobs for Performance Monitor';

IF @preserve_jobs = 1
BEGIN
    PRINT '(preserve mode — existing jobs will not be modified)';
END;
PRINT '';

/*
Job 1: PerformanceMonitor - Collection
Runs scheduled master collector every 1 minute
The collector checks config.collection_schedule to determine which collectors should run
*/
IF @preserve_jobs = 0
AND EXISTS
(
    SELECT
        1/0
    FROM msdb.dbo.sysjobs AS j
    WHERE j.name = N'PerformanceMonitor - Collection'
)
BEGIN
    /*Stop job if currently running (TRY/CATCH for RDS where syssessions is restricted)*/
    BEGIN TRY
        IF EXISTS
        (
            SELECT
                1/0
            FROM msdb.dbo.sysjobactivity AS ja
            JOIN msdb.dbo.sysjobs AS j
              ON j.job_id = ja.job_id
            WHERE j.name = N'PerformanceMonitor - Collection'
            AND   ja.start_execution_date IS NOT NULL
            AND   ja.stop_execution_date IS NULL
            AND   ja.session_id = (SELECT MAX(ss.session_id) FROM msdb.dbo.syssessions AS ss)
        )
        BEGIN
            EXECUTE msdb.dbo.sp_stop_job
                @job_name = N'PerformanceMonitor - Collection';
        END;
    END TRY
    BEGIN CATCH
        /*syssessions not accessible — skip stop check, proceed to delete*/
    END CATCH;

    EXECUTE msdb.dbo.sp_delete_job
        @job_name = N'PerformanceMonitor - Collection';

    PRINT 'Dropped existing PerformanceMonitor - Collection job';
END;

IF NOT EXISTS
(
    SELECT
        1/0
    FROM msdb.dbo.sysjobs AS j
    WHERE j.name = N'PerformanceMonitor - Collection'
)
BEGIN
    EXECUTE msdb.dbo.sp_add_job
        @job_name = N'PerformanceMonitor - Collection',
        @enabled = 1,
        @description = N'Runs scheduled master collector to execute collectors based on config.collection_schedule',
        @category_name = N'Data Collector';

    EXECUTE msdb.dbo.sp_add_jobstep
        @job_name = N'PerformanceMonitor - Collection',
        @step_name = N'Run Scheduled Master Collector',
        @subsystem = N'TSQL',
        @database_name = N'PerformanceMonitor',
        @command = N'EXECUTE collect.scheduled_master_collector @debug = 0;',
        @retry_attempts = 0,
        @on_success_action = 1; /*Quit with success*/

    EXECUTE msdb.dbo.sp_add_jobschedule
        @job_name = N'PerformanceMonitor - Collection',
        @name = N'Every 1 Minute',
        @freq_type = 4, /*Daily*/
        @freq_interval = 1,
        @freq_subday_type = 4, /*Minutes*/
        @freq_subday_interval = 1; /*Every 1 minute*/

    EXECUTE msdb.dbo.sp_add_jobserver
        @job_name = N'PerformanceMonitor - Collection',
        @server_name = N'(local)';

    PRINT 'Created PerformanceMonitor - Collection job (runs every 1 minute)';
END;
ELSE IF @preserve_jobs = 1
BEGIN
    PRINT 'PerformanceMonitor - Collection job already exists — preserving current settings';
END;
PRINT '';

/*
Job 2: PerformanceMonitor - Data Retention
Purges old performance monitoring data daily at 2am
*/
IF @preserve_jobs = 0
AND EXISTS
(
    SELECT
        1/0
    FROM msdb.dbo.sysjobs AS j
    WHERE j.name = N'PerformanceMonitor - Data Retention'
)
BEGIN
    /*Stop job if currently running (TRY/CATCH for RDS where syssessions is restricted)*/
    BEGIN TRY
        IF EXISTS
        (
            SELECT
                1/0
            FROM msdb.dbo.sysjobactivity AS ja
            JOIN msdb.dbo.sysjobs AS j
              ON j.job_id = ja.job_id
            WHERE j.name = N'PerformanceMonitor - Data Retention'
            AND   ja.start_execution_date IS NOT NULL
            AND   ja.stop_execution_date IS NULL
            AND   ja.session_id = (SELECT MAX(ss.session_id) FROM msdb.dbo.syssessions AS ss)
        )
        BEGIN
            EXECUTE msdb.dbo.sp_stop_job
                @job_name = N'PerformanceMonitor - Data Retention';
        END;
    END TRY
    BEGIN CATCH
        /*syssessions not accessible — skip stop check, proceed to delete*/
    END CATCH;

    EXECUTE msdb.dbo.sp_delete_job
        @job_name = N'PerformanceMonitor - Data Retention';

    PRINT 'Dropped existing PerformanceMonitor - Data Retention job';
END;

IF NOT EXISTS
(
    SELECT
        1/0
    FROM msdb.dbo.sysjobs AS j
    WHERE j.name = N'PerformanceMonitor - Data Retention'
)
BEGIN
    EXECUTE msdb.dbo.sp_add_job
        @job_name = N'PerformanceMonitor - Data Retention',
        @enabled = 1,
        @description = N'Purges old performance monitoring data',
        @category_name = N'Data Collector';

    EXECUTE msdb.dbo.sp_add_jobstep
        @job_name = N'PerformanceMonitor - Data Retention',
        @step_name = N'Run Data Retention',
        @subsystem = N'TSQL',
        @database_name = N'PerformanceMonitor',
        @command = N'EXECUTE config.data_retention @debug = 1;',
        @retry_attempts = 0,
        @on_success_action = 1; /*Quit with success*/

    EXECUTE msdb.dbo.sp_add_jobschedule
        @job_name = N'PerformanceMonitor - Data Retention',
        @name = N'Daily at 2am',
        @freq_type = 4, /*Daily*/
        @freq_interval = 1,
        @active_start_time = 20000; /*2:00 AM*/

    EXECUTE msdb.dbo.sp_add_jobserver
        @job_name = N'PerformanceMonitor - Data Retention',
        @server_name = N'(local)';

    PRINT 'Created PerformanceMonitor - Data Retention job (runs daily at 2:00 AM)';
END;
ELSE IF @preserve_jobs = 1
BEGIN
    PRINT 'PerformanceMonitor - Data Retention job already exists — preserving current settings';
END;
PRINT '';

/*
Job 3: PerformanceMonitor - Hung Job Monitor
Monitors the collection job for hung state every 5 minutes
*/
IF @preserve_jobs = 0
AND EXISTS
(
    SELECT
        1/0
    FROM msdb.dbo.sysjobs AS j
    WHERE j.name = N'PerformanceMonitor - Hung Job Monitor'
)
BEGIN
    /*Stop job if currently running (TRY/CATCH for RDS where syssessions is restricted)*/
    BEGIN TRY
        IF EXISTS
        (
            SELECT
                1/0
            FROM msdb.dbo.sysjobactivity AS ja
            JOIN msdb.dbo.sysjobs AS j
              ON j.job_id = ja.job_id
            WHERE j.name = N'PerformanceMonitor - Hung Job Monitor'
            AND   ja.start_execution_date IS NOT NULL
            AND   ja.stop_execution_date IS NULL
            AND   ja.session_id = (SELECT MAX(ss.session_id) FROM msdb.dbo.syssessions AS ss)
        )
        BEGIN
            EXECUTE msdb.dbo.sp_stop_job
                @job_name = N'PerformanceMonitor - Hung Job Monitor';
        END;
    END TRY
    BEGIN CATCH
        /*syssessions not accessible — skip stop check, proceed to delete*/
    END CATCH;

    EXECUTE msdb.dbo.sp_delete_job
        @job_name = N'PerformanceMonitor - Hung Job Monitor';

    PRINT 'Dropped existing PerformanceMonitor - Hung Job Monitor job';
END;

IF NOT EXISTS
(
    SELECT
        1/0
    FROM msdb.dbo.sysjobs AS j
    WHERE j.name = N'PerformanceMonitor - Hung Job Monitor'
)
BEGIN
    EXECUTE msdb.dbo.sp_add_job
        @job_name = N'PerformanceMonitor - Hung Job Monitor',
        @enabled = 1,
        @description = N'Monitors collection job for hung state and stops it if needed',
        @category_name = N'Data Collector';

    EXECUTE msdb.dbo.sp_add_jobstep
        @job_name = N'PerformanceMonitor - Hung Job Monitor',
        @step_name = N'Check for Hung Collection Job',
        @subsystem = N'TSQL',
        @database_name = N'PerformanceMonitor',
        @command = N'EXECUTE config.check_hung_collector_job
    @job_name = N''PerformanceMonitor - Collection'',
    @normal_max_duration_minutes = 5,
    @first_run_max_duration_minutes = 30,
    @stop_hung_job = 1,
    @debug = 0;',
        @retry_attempts = 0,
        @on_success_action = 1; /*Quit with success*/

    EXECUTE msdb.dbo.sp_add_jobschedule
        @job_name = N'PerformanceMonitor - Hung Job Monitor',
        @name = N'Every 5 Minutes',
        @freq_type = 4, /*Daily*/
        @freq_interval = 1,
        @freq_subday_type = 4, /*Minutes*/
        @freq_subday_interval = 5; /*Every 5 minutes*/

    EXECUTE msdb.dbo.sp_add_jobserver
        @job_name = N'PerformanceMonitor - Hung Job Monitor',
        @server_name = N'(local)';

    PRINT 'Created PerformanceMonitor - Hung Job Monitor job (runs every 5 minutes)';
END;
ELSE IF @preserve_jobs = 1
BEGIN
    PRINT 'PerformanceMonitor - Hung Job Monitor job already exists — preserving current settings';
END;
PRINT '';

/*
Verify jobs were created
Wrapped in TRY/CATCH for environments with limited msdb permissions (e.g., AWS RDS)
*/
BEGIN TRY
    SELECT
        job_name = j.name,
        enabled =
            CASE j.enabled
                WHEN 1 THEN N'YES'
                ELSE N'NO'
            END,
        schedule_name = s.name,
        schedule_description =
            CASE
                WHEN s.freq_subday_type = 4
                THEN N'Every ' + CONVERT(nvarchar(10), s.freq_subday_interval) + N' minutes'
                WHEN s.freq_type = 4 AND s.active_start_time > 0
                THEN N'Daily at ' +
                    STUFF(STUFF(RIGHT(N'000000' + CONVERT(nvarchar(6), s.active_start_time), 6), 5, 0, N':'), 3, 0, N':')
                ELSE N'See schedule details'
            END,
        owner = SUSER_SNAME(j.owner_sid),
        date_created = j.date_created
    FROM msdb.dbo.sysjobs AS j
    LEFT JOIN msdb.dbo.sysjobschedules AS js
      ON js.job_id = j.job_id
    LEFT JOIN msdb.dbo.sysschedules AS s
      ON s.schedule_id = js.schedule_id
    WHERE j.name LIKE N'PerformanceMonitor%'
    ORDER BY
        j.name;
END TRY
BEGIN CATCH
    PRINT 'Note: Job verification query skipped (insufficient permissions on msdb schedule tables).';
    PRINT 'Jobs were created successfully — verify via SQL Server Agent in SSMS.';
END CATCH;

PRINT '';
PRINT 'SQL Server Agent jobs created successfully';
PRINT '';
PRINT 'Job Details:';
PRINT '  1. PerformanceMonitor - Collection';
PRINT '     - Runs every 1 minute';
PRINT '     - Executes collect.scheduled_master_collector';
PRINT '     - Uses config.collection_schedule to determine which collectors to run';
PRINT '     - Enabled by default';
PRINT '';
PRINT '  2. PerformanceMonitor - Data Retention';
PRINT '     - Runs daily at 2:00 AM';
PRINT '     - Retains 30 days of data';
PRINT '     - Enabled by default';
PRINT '  3. PerformanceMonitor - Hung Job Monitor';
PRINT '     - Runs every 5 minutes';
PRINT '     - Monitors collection job for hung state';
PRINT '     - Stops hung jobs automatically (configurable)';
PRINT '     - Enabled by default';
PRINT '';
PRINT '';
PRINT 'IMPORTANT: Ensure SQL Server Agent service is running';
PRINT 'IMPORTANT: Configure individual collector schedules in config.collection_schedule';
GO
