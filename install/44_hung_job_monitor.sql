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
Hung collector job monitor
Detects when the collection job has been running longer than expected
Logs the issue and optionally stops the hung job to allow recovery
Runs independently from the collection job to provide external monitoring
*/

IF OBJECT_ID(N'config.check_hung_collector_job', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE config.check_hung_collector_job AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    config.check_hung_collector_job
(
    @job_name sysname = N'PerformanceMonitor - Collection', /*Name of the job to monitor*/
    @normal_max_duration_minutes integer = 5, /*Maximum expected duration for normal runs*/
    @first_run_max_duration_minutes integer = 30, /*Maximum expected duration for first run (collecting historical data)*/
    @stop_hung_job bit = 1, /*Whether to automatically stop hung jobs*/
    @debug bit = 0 /*Print debugging information*/
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE
        @job_id uniqueidentifier = NULL,
        @is_running bit = 0,
        @running_minutes integer = 0,
        @max_duration integer = 0,
        @is_first_run bit = 0,
        @message nvarchar(4000) = N'',
        @start_execution_date datetime = NULL;

    BEGIN TRY
        /*
        Get job ID
        */
        SELECT
            @job_id = j.job_id
        FROM msdb.dbo.sysjobs AS j
        WHERE j.name = @job_name;

        IF @job_id IS NULL
        BEGIN
            SET @message = N'Job "' + @job_name + N'" not found in msdb.dbo.sysjobs';

            RAISERROR(@message, 16, 1);
            RETURN;
        END;

        /*
        Check if any collector has not completed first run
        Uses collection_log to detect if any enabled collector never succeeded
        */
        IF EXISTS
        (
            SELECT
                1/0
            FROM config.collection_schedule AS cs
            WHERE cs.enabled = 1
            AND   NOT EXISTS
            (
                SELECT
                    1/0
                FROM config.collection_log AS cl
                WHERE cl.collector_name = cs.collector_name
            )
        )
        BEGIN
            SET @is_first_run = 1;
            SET @max_duration = @first_run_max_duration_minutes;

            IF @debug = 1
            BEGIN
                RAISERROR(N'First run detected - using %d minute timeout', 0, 1, @max_duration) WITH NOWAIT;
            END;
        END;
        ELSE
        BEGIN
            SET @max_duration = @normal_max_duration_minutes;
        END;

        /*
        Check if job is currently running and for how long
        Uses most recent session_id to handle job restarts
        */
        SELECT
            @is_running = 1,
            @running_minutes = DATEDIFF(MINUTE, ja.start_execution_date, SYSDATETIME()),
            @start_execution_date = ja.start_execution_date
        FROM msdb.dbo.sysjobactivity AS ja
        WHERE ja.job_id = @job_id
        AND   ja.start_execution_date IS NOT NULL
        AND   ja.stop_execution_date IS NULL
        AND   ja.session_id =
        (
            SELECT
                MAX(ja2.session_id)
            FROM msdb.dbo.sysjobactivity AS ja2
            WHERE ja2.job_id = @job_id
        );

        /*
        If not running, nothing to do
        */
        IF @is_running = 0
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Job "%s" is not currently running', 0, 1, @job_name) WITH NOWAIT;
            END;

            RETURN;
        END;

        /*
        If running but within threshold, nothing to do
        */
        IF @running_minutes <= @max_duration
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Job "%s" running for %d minutes (threshold: %d)', 0, 1, @job_name, @running_minutes, @max_duration) WITH NOWAIT;
            END;

            RETURN;
        END;

        /*
        Job is hung - log the issue
        */
        SET @message = N'Job "' + @job_name + N'" hung for ' +
                       CONVERT(nvarchar(10), @running_minutes) + N' minutes (threshold: ' +
                       CONVERT(nvarchar(10), @max_duration) + N' minutes)';

        IF @stop_hung_job = 1
        BEGIN
            SET @message = @message + N'. Stopping job.';
        END;

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
            N'hung_job_monitor',
            N'JOB_HUNG',
            @running_minutes * 60 * 1000,
            @message
        );

        /*
        Log to SQL Server error log for visibility
        */
        RAISERROR(@message, 10, 1) WITH LOG;

        /*
        Stop the hung job if configured
        */
        IF @stop_hung_job = 1
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Stopping job "%s"', 0, 1, @job_name) WITH NOWAIT;
            END;

            EXECUTE msdb.dbo.sp_stop_job
                @job_id = @job_id;

            /*
            Optional: Restart immediately
            Disabled by default - better to let next scheduled run handle it
            This allows time for blocking/issues to clear
            */
            /*
            WAITFOR DELAY '00:00:05';

            EXECUTE msdb.dbo.sp_start_job
                @job_id = @job_id;
            */
        END;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
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
            N'hung_job_monitor',
            N'ERROR',
            0,
            @error_message
        );

        RAISERROR(N'Error in hung job monitor: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Hung collector job monitor created successfully';
PRINT 'Schedule this to run every 5 minutes via separate SQL Agent job';
PRINT 'Example: EXECUTE config.check_hung_collector_job @job_name = N''PerformanceMonitor Collection'', @debug = 0;';
GO
