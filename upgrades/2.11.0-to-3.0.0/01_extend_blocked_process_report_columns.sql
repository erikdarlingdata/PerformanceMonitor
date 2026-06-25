/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 2.11.0 to 3.0.0
Adds typed blocker-side columns to collect.blocking_BlockedProcessReport so
the Dashboard analysis path (BLOCKING_CHAIN fact + drill-down) can read
structured columns instead of re-parsing blocked_process_report_xml on every
analysis cycle. Backfills existing activity='blocked' rows from their stored
XML in one pass.
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
Add columns idempotently. Each column gets its own guarded ALTER so a re-run
after a partial failure resumes cleanly. Separate GO batches are required
because the backfill below references the new columns by name and the parser
needs them to exist at compile time.
*/
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns AS c
    WHERE c.object_id = OBJECT_ID(N'collect.blocking_BlockedProcessReport')
    AND   c.name = N'blocking_spid'
)
BEGIN
    ALTER TABLE collect.blocking_BlockedProcessReport
        ADD blocking_spid integer NULL;

    PRINT 'Added blocking_spid to collect.blocking_BlockedProcessReport';
END;
GO

IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns AS c
    WHERE c.object_id = OBJECT_ID(N'collect.blocking_BlockedProcessReport')
    AND   c.name = N'blocking_last_tran_started'
)
BEGIN
    ALTER TABLE collect.blocking_BlockedProcessReport
        ADD blocking_last_tran_started datetime2(7) NULL;

    PRINT 'Added blocking_last_tran_started to collect.blocking_BlockedProcessReport';
END;
GO

IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns AS c
    WHERE c.object_id = OBJECT_ID(N'collect.blocking_BlockedProcessReport')
    AND   c.name = N'blocking_status'
)
BEGIN
    ALTER TABLE collect.blocking_BlockedProcessReport
        ADD blocking_status nvarchar(10) NULL;

    PRINT 'Added blocking_status to collect.blocking_BlockedProcessReport';
END;
GO

IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns AS c
    WHERE c.object_id = OBJECT_ID(N'collect.blocking_BlockedProcessReport')
    AND   c.name = N'blocked_sql_text'
)
BEGIN
    ALTER TABLE collect.blocking_BlockedProcessReport
        ADD blocked_sql_text nvarchar(max) NULL;

    PRINT 'Added blocked_sql_text to collect.blocking_BlockedProcessReport';
END;
GO

IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns AS c
    WHERE c.object_id = OBJECT_ID(N'collect.blocking_BlockedProcessReport')
    AND   c.name = N'blocking_sql_text'
)
BEGIN
    ALTER TABLE collect.blocking_BlockedProcessReport
        ADD blocking_sql_text nvarchar(max) NULL;

    PRINT 'Added blocking_sql_text to collect.blocking_BlockedProcessReport';
END;
GO

/*
One-time backfill of existing activity='blocked' rows. Idempotent: the WHERE
filter targets only rows where blocking_spid IS NULL (i.e., not yet
populated). Safe to re-run; once a row is populated the predicate excludes it.

XQuery uses the descendant axis (//blocked-process-report/...) because the
stored XML is <event>-rooted with the report nested two levels deep at
/event/data[@name="blocked_process"]/value/blocked-process-report - both
upstream writers preserve the outer <event> wrap. The descendant axis
sidesteps the wrap and is empirically validated against
sql2022.PerformanceMonitor; a leading-slash (/blocked-process-report/...)
returns NULL on every row.

LTRIM/RTRIM on the inputbuf text() matches the C# parser's .Trim() for space
characters only; T-SQL one-arg LTRIM/RTRIM does NOT strip CR/LF/TAB while C#
.Trim() does. Treated as close-enough - the reconstructor keys on session
pair (SPID + tran start), not SQL text, so whitespace divergence is cosmetic
and appears (if at all) in drill-down JSON only.
*/
UPDATE
    b
SET
    b.blocking_spid =
        b.blocked_process_report_xml.value
        (
            N'(//blocked-process-report/blocking-process/process/@spid)[1]',
            N'integer'
        ),
    b.blocking_last_tran_started =
        b.blocked_process_report_xml.value
        (
            N'(//blocked-process-report/blocking-process/process/@lasttranstarted)[1]',
            N'datetime2(7)'
        ),
    b.blocking_status =
        b.blocked_process_report_xml.value
        (
            N'(//blocked-process-report/blocking-process/process/@status)[1]',
            N'nvarchar(10)'
        ),
    b.blocked_sql_text =
        LTRIM(RTRIM(b.blocked_process_report_xml.value
        (
            N'(//blocked-process-report/blocked-process/process/inputbuf/text())[1]',
            N'nvarchar(max)'
        ))),
    b.blocking_sql_text =
        LTRIM(RTRIM(b.blocked_process_report_xml.value
        (
            N'(//blocked-process-report/blocking-process/process/inputbuf/text())[1]',
            N'nvarchar(max)'
        )))
FROM collect.blocking_BlockedProcessReport AS b
WHERE b.activity = 'blocked'
AND   b.blocking_spid IS NULL
AND   b.blocked_process_report_xml IS NOT NULL;

PRINT 'Backfilled blocker-side typed columns for existing activity=''blocked'' rows';
GO
