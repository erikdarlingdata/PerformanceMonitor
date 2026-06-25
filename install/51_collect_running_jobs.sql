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

/*******************************************************************************
Collector: running_jobs_collector
Purpose: Captures currently running SQL Agent jobs and compares their current
         duration to historical avg/p95 durations to flag jobs running longer
         than expected
Collection Type: Snapshot
Target Table: collect.running_jobs
Frequency: Every 5 minutes
Dependencies: msdb.dbo.sysjobs, msdb.dbo.sysjobactivity, msdb.dbo.sysjobhistory
Notes: Gracefully skips on Azure SQL DB or environments without SQL Agent
       Uses PERCENTILE_CONT for p95 calculation (available SQL Server 2012+)
*******************************************************************************/

IF OBJECT_ID(N'collect.running_jobs_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.running_jobs_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.running_jobs_collector
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
        /*
        Gate: check if msdb.dbo.sysjobs exists
        Azure SQL DB and some environments do not have SQL Agent
        */
        IF OBJECT_ID(N'msdb.dbo.sysjobs', N'U') IS NULL
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'msdb.dbo.sysjobs not available - SQL Agent not present, skipping', 0, 1) WITH NOWAIT;
            END;

            INSERT INTO
                config.collection_log
            (
                collector_name,
                collection_status,
                rows_collected,
                duration_ms,
                error_message
            )
            VALUES
            (
                N'running_jobs_collector',
                N'SKIPPED',
                0,
                DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
                N'SQL Agent not available (msdb.dbo.sysjobs does not exist)'
            );

            RETURN;
        END;

        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.running_jobs', N'U') IS NULL
        BEGIN
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
                N'running_jobs_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.running_jobs does not exist, calling ensure procedure'
            );

            EXECUTE config.ensure_collection_table
                @table_name = N'running_jobs',
                @debug = @debug;

            IF OBJECT_ID(N'collect.running_jobs', N'U') IS NULL
            BEGIN
                RAISERROR(N'Table collect.running_jobs still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        CTE: Find currently running jobs
        Joins sysjobactivity with syssessions to find the most recent session
        A job is "running" if it has a start_execution_date but no stop_execution_date
        */
        ;WITH
            running_jobs AS
        (
            SELECT
                job_name = j.name,
                job_id = j.job_id,
                job_enabled = j.enabled,
                start_time = ja.start_execution_date,
                current_duration_seconds =
                    DATEDIFF(SECOND, ja.start_execution_date, SYSDATETIME())
            FROM msdb.dbo.sysjobactivity AS ja
            JOIN msdb.dbo.syssessions AS ss
              ON ss.session_id = ja.session_id
            JOIN msdb.dbo.sysjobs AS j
              ON j.job_id = ja.job_id
            WHERE ja.start_execution_date IS NOT NULL
            AND   ja.stop_execution_date IS NULL
            AND   ss.session_id =
                  (
                      SELECT TOP (1)
                          ss2.session_id
                      FROM msdb.dbo.syssessions AS ss2
                      ORDER BY
                          ss2.agent_start_date DESC
                  )
        ),
        /*
        CTE: Decode run_duration and compute p95 as a window function over raw rows
        run_duration in sysjobhistory is stored as HHMMSS integer format
        Decode: (run_duration / 10000) * 3600 + ((run_duration / 100) % 100) * 60 + (run_duration % 100)
        PERCENTILE_CONT is a window function and must operate on raw rows (before GROUP BY)
        */
            job_history_raw AS
        (
            SELECT
                job_id = jh.job_id,
                duration_seconds =
                    (jh.run_duration / 10000) * 3600 +
                    ((jh.run_duration / 100) % 100) * 60 +
                    (jh.run_duration % 100),
                p95_duration_seconds =
                    CONVERT
                    (
                        bigint,
                        PERCENTILE_CONT(0.95) WITHIN GROUP
                        (
                            ORDER BY
                                (jh.run_duration / 10000) * 3600 +
                                ((jh.run_duration / 100) % 100) * 60 +
                                (jh.run_duration % 100)
                        ) OVER
                        (
                            PARTITION BY
                                jh.job_id
                        )
                    )
            FROM msdb.dbo.sysjobhistory AS jh
            WHERE jh.step_id = 0
            AND   jh.run_status = 1
            AND   jh.run_date >= CONVERT(integer, CONVERT(varchar(8), DATEADD(DAY, -30, SYSDATETIME()), 112))
        ),
        /*
        CTE: Aggregate the raw history into per-job stats
        */
            job_history_stats AS
        (
            SELECT
                job_id = jhr.job_id,
                avg_duration_seconds = AVG(jhr.duration_seconds),
                p95_duration_seconds = MAX(jhr.p95_duration_seconds),
                successful_run_count = COUNT_BIG(*)
            FROM job_history_raw AS jhr
            GROUP BY
                jhr.job_id
        )
        INSERT INTO
            collect.running_jobs
        (
            collection_time,
            server_start_time,
            job_name,
            job_id,
            job_enabled,
            start_time,
            current_duration_seconds,
            avg_duration_seconds,
            p95_duration_seconds,
            successful_run_count,
            is_running_long,
            percent_of_average
        )
        SELECT
            collection_time = @start_time,
            server_start_time =
            (
                SELECT
                    osi.sqlserver_start_time
                FROM sys.dm_os_sys_info AS osi
            ),
            job_name = rj.job_name,
            job_id = rj.job_id,
            job_enabled = rj.job_enabled,
            start_time = rj.start_time,
            current_duration_seconds = rj.current_duration_seconds,
            avg_duration_seconds = jhs.avg_duration_seconds,
            p95_duration_seconds = jhs.p95_duration_seconds,
            successful_run_count = jhs.successful_run_count,
            is_running_long =
                CASE
                    /*
                    Flag as running long if current duration exceeds p95
                    Fall back to 2x average if no p95 available
                    */
                    WHEN jhs.p95_duration_seconds IS NOT NULL
                    AND  rj.current_duration_seconds > jhs.p95_duration_seconds
                    THEN 1
                    WHEN jhs.p95_duration_seconds IS NULL
                    AND  jhs.avg_duration_seconds IS NOT NULL
                    AND  rj.current_duration_seconds > (jhs.avg_duration_seconds * 2)
                    THEN 1
                    ELSE 0
                END,
            percent_of_average =
                CASE
                    WHEN jhs.avg_duration_seconds IS NULL
                    OR   jhs.avg_duration_seconds = 0
                    THEN NULL
                    ELSE CONVERT
                         (
                             decimal(10,1),
                             (rj.current_duration_seconds * 100.0) / jhs.avg_duration_seconds
                         )
                END
        FROM running_jobs AS rj
        LEFT JOIN job_history_stats AS jhs
          ON jhs.job_id = rj.job_id
        OPTION(RECOMPILE);

        SET @rows_collected = ROWCOUNT_BIG();

        /*
        Debug output
        */
        IF @debug = 1
        BEGIN
            IF @rows_collected > 0
            BEGIN
                RAISERROR(N'Collected %I64d running jobs', 0, 1, @rows_collected) WITH NOWAIT;

                SELECT TOP (10)
                    rj.job_name,
                    rj.start_time,
                    rj.current_duration_seconds,
                    rj.avg_duration_seconds,
                    rj.p95_duration_seconds,
                    rj.percent_of_average,
                    rj.is_running_long
                FROM collect.running_jobs AS rj
                WHERE rj.collection_time = @start_time
                ORDER BY
                    rj.current_duration_seconds DESC;
            END;
            ELSE
            BEGIN
                RAISERROR(N'No running jobs found', 0, 1) WITH NOWAIT;
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
            N'running_jobs_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

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
            N'running_jobs_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in running jobs collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Running jobs collector created successfully';
PRINT 'Captures currently running SQL Agent jobs with historical duration comparison';
PRINT 'Use: EXECUTE collect.running_jobs_collector @debug = 1;';
GO
