/*
    Blocking-chain reconstruction now keys sessions by (monitor_loop, spid, ecid), mirroring
    sp_HumanEventsBlockViewer (spid:ecid within a monitor_loop episode). Add the two identity columns the
    Dashboard reconstruction reads; install/23 populates them from the stored blocked_process_report_xml via
    the existing descendant-axis XQuery (the blocked-side ecid is already the existing `ecid` column).

    Existing rows stay NULL until install/23 backfills them opportunistically; the reconstruction is
    null-tolerant (falls back to spid:ecid for the viewer; the collector ignores monitor_loop entirely), so
    historical blocking chains keep working in the meantime.

    Idempotent: only adds a column that isn't already present. install/02 carries these on fresh installs.
*/

IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns AS c
    WHERE c.object_id = OBJECT_ID(N'collect.blocking_BlockedProcessReport')
    AND   c.name = N'blocking_ecid'
)
BEGIN
    ALTER TABLE collect.blocking_BlockedProcessReport
        ADD blocking_ecid integer NULL;
END;

IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns AS c
    WHERE c.object_id = OBJECT_ID(N'collect.blocking_BlockedProcessReport')
    AND   c.name = N'monitor_loop'
)
BEGIN
    ALTER TABLE collect.blocking_BlockedProcessReport
        ADD monitor_loop integer NULL;
END;
