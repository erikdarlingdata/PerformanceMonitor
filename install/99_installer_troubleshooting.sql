/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Lightweight troubleshooting script for the Performance Monitor Installer GUI.
This script runs quick diagnostic checks without executing collectors.
For full troubleshooting with collector execution, use 99_user_troubleshooting.sql
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
SET NOCOUNT ON;
GO

USE PerformanceMonitor;
GO

PRINT '================================================================================';
PRINT 'Performance Monitor - Installation Diagnostics';
PRINT '================================================================================';
PRINT '';
GO

/*
===============================================================================
1. Check for recent collection errors
===============================================================================
*/
PRINT 'Checking collection log for errors...';

IF EXISTS
(
    SELECT
        1/0
    FROM config.collection_log AS cl
    WHERE cl.collection_status = N'ERROR'
    AND   cl.collection_time >= DATEADD(HOUR, -24, SYSDATETIME())
)
BEGIN
    PRINT '  [WARN] Recent collection errors found:';

    SELECT TOP (10)
        collection_time = cl.collection_time,
        collector_name = cl.collector_name,
        error_message = LEFT(cl.error_message, 200)
    FROM config.collection_log AS cl
    WHERE cl.collection_status = N'ERROR'
    AND   cl.collection_time >= DATEADD(HOUR, -24, SYSDATETIME())
    ORDER BY
        cl.collection_time DESC;
END;
ELSE
BEGIN
    PRINT '  [OK] No collection errors in the last 24 hours';
END;

PRINT '';
GO

/*
===============================================================================
2. Check collection schedule status
===============================================================================
*/
PRINT 'Checking collector schedule...';

DECLARE
    @enabled_count integer,
    @disabled_count integer,
    @total_count bigint;

SELECT
    @enabled_count = SUM(CASE WHEN cs.enabled = 1 THEN 1 ELSE 0 END),
    @disabled_count = SUM(CASE WHEN cs.enabled = 0 THEN 1 ELSE 0 END),
    @total_count = COUNT_BIG(*)
FROM config.collection_schedule AS cs;

PRINT '  Enabled collectors: ' + CONVERT(varchar(10), @enabled_count);
PRINT '  Disabled collectors: ' + CONVERT(varchar(10), @disabled_count);
PRINT '  Total collectors: ' + CONVERT(varchar(10), @total_count);

IF @enabled_count = 0
BEGIN
    PRINT '  [WARN] No collectors are enabled!';
END;
ELSE
BEGIN
    PRINT '  [OK] Collectors are configured';
END;

PRINT '';
GO

/*
===============================================================================
3. Check SQL Agent job status
===============================================================================
*/
PRINT 'Checking SQL Agent job...';

IF EXISTS
(
    SELECT
        1/0
    FROM msdb.dbo.sysjobs AS j
    WHERE j.name LIKE N'%PerformanceMonitor%'
)
BEGIN
    SELECT
        job_name = j.name,
        is_enabled =
            CASE j.enabled
                WHEN 1 THEN 'Enabled'
                ELSE 'Disabled'
            END,
        last_run_outcome =
            CASE jh.run_status
                WHEN 0 THEN 'Failed'
                WHEN 1 THEN 'Succeeded'
                WHEN 2 THEN 'Retry'
                WHEN 3 THEN 'Canceled'
                ELSE 'Never run'
            END,
        last_run_date =
            CASE
                WHEN jh.run_date > 0
                THEN CONVERT
                     (
                         datetime,
                         CONVERT(varchar(8), jh.run_date) + ' ' +
                         STUFF
                         (
                             STUFF
                             (
                                 RIGHT('000000' + CONVERT(varchar(6), jh.run_time), 6),
                                 3,
                                 0,
                                 ':'
                             ),
                             6,
                             0,
                             ':'
                         )
                     )
                ELSE NULL
            END
    FROM msdb.dbo.sysjobs AS j
    LEFT JOIN
    (
        SELECT
            job_id,
            run_date,
            run_time,
            run_status,
            rn = ROW_NUMBER() OVER
                 (
                     PARTITION BY
                         job_id
                     ORDER BY
                         run_date DESC,
                         run_time DESC
                 )
        FROM msdb.dbo.sysjobhistory
        WHERE step_id = 0
    ) AS jh
      ON  jh.job_id = j.job_id
      AND jh.rn = 1
    WHERE j.name LIKE N'%PerformanceMonitor%';

    PRINT '  [OK] SQL Agent job found';
END;
ELSE
BEGIN
    PRINT '  [WARN] No PerformanceMonitor SQL Agent job found!';
END;

PRINT '';
GO

/*
===============================================================================
4. Check collection table row counts
===============================================================================
*/
PRINT 'Checking collection tables...';

SELECT
    table_name = t.name,
    row_count = SUM(p.rows)
FROM sys.tables AS t
JOIN sys.schemas AS s
  ON s.schema_id = t.schema_id
JOIN sys.partitions AS p
  ON p.object_id = t.object_id
WHERE s.name = N'collect'
AND   p.index_id IN (0, 1)
GROUP BY
    t.name
ORDER BY
    t.name;

DECLARE
    @tables_with_data integer,
    @total_tables integer,
    @total_rows bigint;

SELECT
    @tables_with_data = COUNT_BIG(CASE WHEN x.row_count > 0 THEN 1 END),
    @total_tables = COUNT_BIG(*),
    @total_rows = SUM(x.row_count)
FROM
(
    SELECT
        row_count = SUM(p.rows)
    FROM sys.tables AS t
    JOIN sys.schemas AS s
      ON s.schema_id = t.schema_id
    JOIN sys.partitions AS p
      ON p.object_id = t.object_id
    WHERE s.name = N'collect'
    AND   p.index_id IN (0, 1)
    GROUP BY
        t.name
) AS x;

PRINT '  Tables with data: ' + CONVERT(varchar(10), @tables_with_data) + '/' + CONVERT(varchar(10), @total_tables);
PRINT '  Total rows collected: ' + FORMAT(@total_rows, 'N0');

IF @tables_with_data = 0
BEGIN
    PRINT '  [WARN] No data has been collected yet!';
END;
ELSE
BEGIN
    PRINT '  [OK] Collection tables have data';
END;

PRINT '';
GO

/*
===============================================================================
5. Check most recent collection times
===============================================================================
*/
PRINT 'Checking recent collection activity...';

SELECT
    collector_name = cs.collector_name,
    last_run_time = cs.last_run_time,
    next_run_time = cs.next_run_time,
    minutes_since_last_run = DATEDIFF(MINUTE, cs.last_run_time, SYSDATETIME()),
    enabled = cs.enabled,
    frequency_minutes = cs.frequency_minutes
FROM config.collection_schedule AS cs
WHERE cs.enabled = 1
ORDER BY
    cs.last_run_time DESC;

DECLARE
    @stale_collectors integer;

SELECT
    @stale_collectors = COUNT_BIG(*)
FROM config.collection_schedule AS cs
WHERE cs.enabled = 1
AND   cs.last_run_time < DATEADD(HOUR, -1, SYSDATETIME());

IF @stale_collectors > 0
BEGIN
    PRINT '  [WARN] ' + CONVERT(varchar(10), @stale_collectors) + ' enabled collector(s) have not run in over an hour';
END;
ELSE
BEGIN
    PRINT '  [OK] Collectors are running on schedule';
END;

PRINT '';
GO

/*
===============================================================================
Summary
===============================================================================
*/
PRINT '================================================================================';
PRINT 'Diagnostics completed';
PRINT '================================================================================';
PRINT '';
PRINT 'For full troubleshooting with collector execution, run:';
PRINT '  99_user_troubleshooting.sql';
GO
