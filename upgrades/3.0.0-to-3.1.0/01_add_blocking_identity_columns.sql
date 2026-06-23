/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 3.0.0 to 3.1.0
Adds blocking-side identity columns (login / host / client app) to
collect.blocking_BlockedProcessReport so the Dashboard block-chain viewer can
show WHO the lead blocker (apex) is, not just a SPID — matching Lite, which
already denormalizes both sides. The 3.0.0 upgrade added the other blocker-side
typed columns (spid / tran / status / sql); this completes the set with the
identity attributes. Backfills existing activity='blocked' rows from their
stored XML in one pass.
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
needs them to exist at compile time. Types mirror the blocked-side identity
columns (login_name / host_name / client_app are nvarchar(256)).
*/
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns AS c
    WHERE c.object_id = OBJECT_ID(N'collect.blocking_BlockedProcessReport')
    AND   c.name = N'blocking_login_name'
)
BEGIN
    ALTER TABLE collect.blocking_BlockedProcessReport
        ADD blocking_login_name nvarchar(256) NULL;

    PRINT 'Added blocking_login_name to collect.blocking_BlockedProcessReport';
END;
GO

IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns AS c
    WHERE c.object_id = OBJECT_ID(N'collect.blocking_BlockedProcessReport')
    AND   c.name = N'blocking_host_name'
)
BEGIN
    ALTER TABLE collect.blocking_BlockedProcessReport
        ADD blocking_host_name nvarchar(256) NULL;

    PRINT 'Added blocking_host_name to collect.blocking_BlockedProcessReport';
END;
GO

IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns AS c
    WHERE c.object_id = OBJECT_ID(N'collect.blocking_BlockedProcessReport')
    AND   c.name = N'blocking_client_app'
)
BEGIN
    ALTER TABLE collect.blocking_BlockedProcessReport
        ADD blocking_client_app nvarchar(256) NULL;

    PRINT 'Added blocking_client_app to collect.blocking_BlockedProcessReport';
END;
GO

/*
One-time backfill of existing activity='blocked' rows. Idempotent: the WHERE
filter targets only rows where blocking_login_name IS NULL (i.e., not yet
populated). Safe to re-run; once a row is populated the predicate excludes it.

XQuery uses the descendant axis (//blocked-process-report/...) for the same
reason as the 3.0.0 backfill: the stored XML is <event>-rooted with the report
nested two levels deep, so a leading-slash path returns NULL on every row. The
<process> element carries loginname / hostname / clientapp (lowercase, no
underscores) — the same attribute schema as a deadlock-graph process node, and
unchanged across SQL Server 2016+, Azure SQL DB, and Managed Instance. On the
Azure blocked_process_report_filtered variant these come back as the literal
"filtered" (value masked, attribute present), which is the expected behavior.
*/
UPDATE
    b
SET
    b.blocking_login_name =
        b.blocked_process_report_xml.value
        (
            N'(//blocked-process-report/blocking-process/process/@loginname)[1]',
            N'nvarchar(256)'
        ),
    b.blocking_host_name =
        b.blocked_process_report_xml.value
        (
            N'(//blocked-process-report/blocking-process/process/@hostname)[1]',
            N'nvarchar(256)'
        ),
    b.blocking_client_app =
        b.blocked_process_report_xml.value
        (
            N'(//blocked-process-report/blocking-process/process/@clientapp)[1]',
            N'nvarchar(256)'
        )
FROM collect.blocking_BlockedProcessReport AS b
WHERE b.activity = 'blocked'
AND   b.blocking_login_name IS NULL
AND   b.blocked_process_report_xml IS NOT NULL;

PRINT 'Backfilled blocker-side identity columns for existing activity=''blocked'' rows';
GO
