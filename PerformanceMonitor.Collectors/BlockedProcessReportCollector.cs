/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Blocked-process reports from the app-managed PerformanceMonitor_BlockedProcess XE ring-buffer
/// session. Extracted verbatim from Lite's RemoteCollectorService.BlockedProcessReport.cs: the
/// server- vs database-scoped ring-buffer read, the server-side wait_resource → contentious
/// object resolution (KEY via sys.partitions, PAGE/RID via sys.dm_db_page_info on 2019+/Azure,
/// mirroring sp_HumanEventsBlockViewer so the #1140 dedup fingerprint agrees across apps), the
/// event_time watermark that keeps ring-buffer lingerers from re-inserting (10-minute fallback),
/// and the C# blocked-process-report XML parse (read phase). Since #1865 a row the resolution could
/// not name says WHY in its own label — the permission posture screened before it is attempted, the
/// reallocated page reported once through the payload probe-failure channel. Session lifecycle — including the
/// blocked-process-threshold sp_configure bootstrap — stays host-side; the session name lives
/// here so the reader and the lifecycle can never disagree on it.
/// </summary>
public sealed class BlockedProcessReportCollector : CollectorDefinitionBase<BlockedProcessReportCollector.Row>
{
    public static BlockedProcessReportCollector Instance { get; } = new();

    private BlockedProcessReportCollector()
    {
    }

    /* We create and manage our own XE session to avoid conflicts with user's existing sessions */
    public const string XeSessionName = "PerformanceMonitor_BlockedProcess";

    public sealed class Row
    {
        public DateTime? EventTime { get; set; }
        public int? MonitorLoop { get; set; }
        public string? DatabaseName { get; set; }
        public int BlockedSpid { get; set; }
        public int BlockedEcid { get; set; }
        public int BlockingSpid { get; set; }
        public int BlockingEcid { get; set; }
        public long WaitTimeMs { get; set; }
        public string? WaitResource { get; set; }
        public string? LockMode { get; set; }
        public string? BlockedStatus { get; set; }
        public string? BlockedIsolationLevel { get; set; }
        public long BlockedLogUsed { get; set; }
        public int BlockedTransactionCount { get; set; }
        public string? BlockedClientApp { get; set; }
        public string? BlockedHostName { get; set; }
        public string? BlockedLoginName { get; set; }
        public string? BlockedSqlText { get; set; }
        public string? BlockingStatus { get; set; }
        public string? BlockingIsolationLevel { get; set; }
        public string? BlockingClientApp { get; set; }
        public string? BlockingHostName { get; set; }
        public string? BlockingLoginName { get; set; }
        public string? BlockingSqlText { get; set; }
        public string? BlockedTransactionName { get; set; }
        public string? BlockingTransactionName { get; set; }
        public DateTime? BlockedLastTranStarted { get; set; }
        public DateTime? BlockingLastTranStarted { get; set; }
        public DateTime? BlockedLastBatchStarted { get; set; }
        public DateTime? BlockingLastBatchStarted { get; set; }
        public DateTime? BlockedLastBatchCompleted { get; set; }
        public DateTime? BlockingLastBatchCompleted { get; set; }
        public int BlockedPriority { get; set; }
        public int BlockingPriority { get; set; }
        public string? ReportXml { get; set; }
        public int? ObjectId { get; set; }
        public int? DatabaseId { get; set; }
        public string? ContentiousObject { get; set; }
        /* Best-effort execution plans for the two contending statements, resolved from the plan cache
           at collection time (CapturePlanXml only). Null on Lite (flag off) and whenever the plan
           can't be recovered — see the resolution comment in BuildQuery. */
        public string? BlockedQueryPlanXml { get; set; }
        public string? BlockingQueryPlanXml { get; set; }
    }

    public override string Name => "blocked_process_report";

    public override string TargetTable => "blocked_process_reports";

    /// <summary>Lite schema names this table's prefix id "blocked_report_id"; Darling mirrors it.</summary>
    public override string PrefixIdColumnName => "blocked_report_id";

    /// <summary>
    /// Only events newer than the newest already-collected report are fetched, so a report
    /// lingering in the ring buffer across cycles is never inserted twice.
    /// </summary>
    public override string? WatermarkColumn => "event_time";

    /// <summary>
    /// Azure SQL DB capture is a database-scoped session per monitored database (#1535 — a single
    /// session only ever saw the connection's own database). The host ensures the session per
    /// database and this read then runs per database, exactly like the other Azure per-database
    /// collectors. The per-database contentious-object resolution degrades exactly as the
    /// single-database form did: cross-database lookups that can't run on Azure fall to the
    /// labeled-Unresolved sentinel via the existing TRY/CATCH guards, which since #1865 carry the
    /// reason for the fall in the label itself.
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target.IsAzureSqlDb;

    /// <summary>
    /// Per-database watermark for the per-database sessions (database_name is the blocked
    /// process's currentdbname, parsed since the column's introduction): each database's ring
    /// buffer dispatches independently, so one database's newer report must not watermark past
    /// another's older one.
    /// </summary>
    public override string? PerDatabaseWatermarkColumn => "database_name";

    /// <summary>
    /// This collector's object-resolution cursors return, after the payload, the per-database lookups
    /// they attempted and lost (#1865). Deliberately NOT every failure: the two causes behind an
    /// <c>Unresolved:</c> row want opposite responses, and only one of them is news.
    ///
    /// <para>A login with no metadata access to the contended database is a PERMISSION POSTURE — true
    /// again on the next cycle and the one after — and these cursors run per contended database on
    /// every blocked-process cycle, so routing it here would be a permanent note plus a warning burst
    /// every five minutes for something that is not changing. That is the burst #1854 had to add
    /// <c>HAS_DBACCESS</c> to <c>query_store</c>'s enumeration to stop. So the posture is screened
    /// before it is ever attempted and answers on the ROW instead, in the <c>Unresolved: … (no
    /// metadata access)</c> label an operator is already looking at; it reaches this channel zero
    /// times, not once per database per cycle. A page that was reallocated between the event firing
    /// and this read is the opposite — transient, unpredictable, and worth saying — so it rides the
    /// channel with its cause already named, and labels its row too.</para>
    ///
    /// <para>Not consulted on the Azure SQL DB path, which runs per database and reads through its own
    /// contract (see <see cref="ICollectorDefinition{TRow}.EmitsProbeFailures"/>): the trailing set is
    /// still emitted there and simply goes unread. The labels — the part of #1865 an operator reads —
    /// are produced by the same batch and are unaffected.</para>
    /// </summary>
    public override bool EmitsProbeFailures => true;

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        /* The ring-buffer source DMV is the only engine difference (database-scoped on Azure SQL DB,
           server-scoped elsewhere); event extraction and the wait_resource -> object decode are shared. */
        string ringBufferSource = context.Target.IsAzureSqlDb
            ? @"sys.dm_xe_database_session_targets AS xet
JOIN sys.dm_xe_database_sessions AS xes
  ON xes.address = xet.event_session_address"
            : @"sys.dm_xe_session_targets AS xet
JOIN sys.dm_xe_sessions AS xes
  ON xes.address = xet.event_session_address";

        /* Resolve contentious_object from wait_resource (KEY hobt -> sys.partitions, OBJECT objid,
           PAGE/RID -> sys.dm_db_page_info), mirroring sp_HumanEventsBlockViewer (DarlingData #812) so
           Dashboard and Lite -- and the #1140 dedup fingerprint -- agree. The event object_id/database_id
           is unreliable for lock waits, kept only as a COALESCE fallback. Cross-db lookups are per-database
           dynamic SQL guarded by DB-online + TRY/CATCH; the 2019+ DMV is version-gated (or @azure, which
           reports ProductVersion 12 on Azure SQL DB). #bpr is a #temp (not a table var) so the dynamic SQL
           can see it. The final SELECT keeps the original 5 columns (plus, gated on CapturePlanXml, the
           two best-effort plan columns) so the reader/appender is unchanged when the flag is off. */

        /* Best-effort plan capture (CapturePlanXml only — Darling). Resolve each contending statement's
           plan from the live plan cache by the executionStack frame's sql_handle + statement offsets,
           reusing the proven on-demand shape from Lite/Dashboard's #880 right-click View Plan
           (dm_exec_query_stats -> dm_exec_text_query_plan). This is honestly best-effort: the
           blocked_process_report XE fires asynchronously, so by collection the plan may have aged out of
           cache; dynamic-SQL / adhoc frames carry an all-zero sql_handle that matches no query_stats row;
           either way the column lands NULL rather than fabricated. Frames are walked top-of-stack first,
           first resolvable wins — the same order ExtractBlockedProcessFrames uses. Flag off: both
           fragments are empty strings, so the emitted SQL is byte-identical to the no-plan form. */
        var planSelect = context.CapturePlanXml
            ? @",
    blocked_query_plan_xml = bqp.query_plan,
    blocking_query_plan_xml = kqp.query_plan"
            : "";
        var planApply = context.CapturePlanXml
            ? BuildPlanResolutionApply("blocked-process", "bqp") + BuildPlanResolutionApply("blocking-process", "kqp")
            : "";

        string query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @product_version integer =
        CONVERT
        (
            integer,
            PARSENAME
            (
                CONVERT(sysname, SERVERPROPERTY('ProductVersion')),
                4
            )
        ),
    @azure bit =
        CASE
            WHEN CONVERT(integer, SERVERPROPERTY('EngineEdition')) = 5
            THEN 1
            ELSE 0
        END,
    @resolve_sql nvarchar(max),
    @resolve_database_id integer,
    @key_dbs CURSOR,
    @page_dbs CURSOR,
    /* #1865: why a row could not be named. The two reasons want opposite responses, so they are
       spelled once here and referenced everywhere below -- the screens stamp them without an
       attempt, the CATCHes stamp them after one, and the posture reason is also the one the
       probe-failure channel deliberately never reports. */
    @posture_reason nvarchar(64) = N'no metadata access',
    @unavailable_reason nvarchar(64) = N'database unavailable',
    @resolve_error integer,
    @resolve_message nvarchar(4000),
    @resolve_reason nvarchar(64);

DECLARE
    @PerformanceMonitor_BlockedProcess TABLE
(
    ring_buffer xml NOT NULL
);

/* #1865: the trailing (item_name, error_text) result set of the payload path's probe-failure
   contract (#1851, EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync), selected at the very
   end and normally empty. A table VARIABLE on purpose, mirroring database_size_stats: every write
   below happens in THIS batch, and a table variable is invisible to the nested @resolve_sql, so the
   handlers cannot quietly migrate back inside the dynamic SQL where they would stop catching its
   compile-time failures. */
DECLARE
    @probe_failures TABLE
(
    name sysname NOT NULL,
    error_text nvarchar(4000) NULL
);

INSERT
    @PerformanceMonitor_BlockedProcess
(
    ring_buffer
)
SELECT /* PerformanceMonitorLite */
    ring_xml = TRY_CAST(xet.target_data AS xml)
FROM {ringBufferSource}
WHERE xes.name = N'{XeSessionName}'
AND   xet.target_name = N'ring_buffer'
OPTION(RECOMPILE);

SELECT
    x.event_time,
    x.blocked_process_report_xml,
    x.object_id,
    x.database_id,
    x.wait_resource,
    lock_type = CONVERT(varchar(32), NULL),
    frag = CONVERT(nvarchar(1024), NULL),
    resource_database_id = CONVERT(integer, NULL),
    resource_hobt_id = CONVERT(bigint, NULL),
    resource_file_id = CONVERT(integer, NULL),
    resource_page_id = CONVERT(bigint, NULL),
    resource_object_id = CONVERT(integer, NULL),
    contentious_object = CONVERT(nvarchar(4000), NULL),
    unresolved_reason = CONVERT(nvarchar(64), NULL)
INTO #bpr
FROM
(
    SELECT
        event_time = evt.value('(@timestamp)[1]', 'datetime2'),
        blocked_process_report_xml = CONVERT(nvarchar(max), evt.query('data[@name=""blocked_process""]/value/blocked-process-report')),
        object_id = evt.value('(data[@name=""object_id""]/value/text())[1]', 'integer'),
        database_id = evt.value('(data[@name=""database_id""]/value/text())[1]', 'integer'),
        wait_resource = evt.value('(data[@name=""blocked_process""]/value/blocked-process-report/blocked-process/process/@waitresource)[1]', 'nvarchar(1024)')
    FROM @PerformanceMonitor_BlockedProcess AS rb
    CROSS APPLY rb.ring_buffer.nodes('RingBufferTarget/event[@name=""blocked_process_report""]') AS q(evt)
    WHERE evt.value('(@timestamp)[1]', 'datetime2') > @cutoff_time
) AS x
OPTION(RECOMPILE);

/* Classify the lock resource (prefers an embedded KEY/PAGE in a compound resource). */
UPDATE
    b
SET
    b.lock_type =
        CASE
            WHEN b.wait_resource LIKE N'%KEY: %'    THEN 'KEY'
            WHEN b.wait_resource LIKE N'%OBJECT: %' THEN 'OBJECT'
            WHEN b.wait_resource LIKE N'%RID: %'    THEN 'RID'
            WHEN b.wait_resource LIKE N'%PAGE: %'   THEN 'PAGE'
            ELSE LEFT(UPPER(LEFT(b.wait_resource, CHARINDEX(N':', b.wait_resource + N':') - 1)), 32)
        END
FROM #bpr AS b
WHERE b.wait_resource IS NOT NULL
AND   b.wait_resource <> N''
OPTION(RECOMPILE);

UPDATE
    b
SET
    b.frag =
        SUBSTRING
        (
            b.wait_resource,
            CHARINDEX(b.lock_type + N': ', b.wait_resource) + LEN(b.lock_type) + 2,
            1024
        )
FROM #bpr AS b
WHERE b.lock_type IN ('KEY', 'OBJECT', 'RID', 'PAGE')
OPTION(RECOMPILE);

UPDATE
    b
SET
    b.resource_database_id =
        TRY_CONVERT(integer, LEFT(b.frag, CHARINDEX(N':', b.frag + N':') - 1))
FROM #bpr AS b
WHERE b.frag IS NOT NULL
OPTION(RECOMPILE);

UPDATE
    b
SET
    b.resource_hobt_id =
        TRY_CONVERT(bigint, LTRIM(LEFT(r.rest, CHARINDEX(N' ', r.rest + N' ') - 1)))
FROM #bpr AS b
CROSS APPLY
(
    SELECT
        rest = SUBSTRING(b.frag, CHARINDEX(N':', b.frag) + 1, 1024)
) AS r
WHERE b.lock_type = 'KEY'
OPTION(RECOMPILE);

UPDATE
    b
SET
    b.resource_object_id =
        TRY_CONVERT(integer, LEFT(r.rest, CHARINDEX(N':', r.rest + N':') - 1))
FROM #bpr AS b
CROSS APPLY
(
    SELECT
        rest = SUBSTRING(b.frag, CHARINDEX(N':', b.frag) + 1, 1024)
) AS r
WHERE b.lock_type = 'OBJECT'
OPTION(RECOMPILE);

UPDATE
    b
SET
    b.resource_file_id =
        TRY_CONVERT(integer, LEFT(r.rest, CHARINDEX(N':', r.rest + N':') - 1)),
    b.resource_page_id =
        TRY_CONVERT(bigint, LEFT(p.rest2, CHARINDEX(N':', p.rest2 + N':') - 1))
FROM #bpr AS b
CROSS APPLY
(
    SELECT
        rest = SUBSTRING(b.frag, CHARINDEX(N':', b.frag) + 1, 1024)
) AS r
CROSS APPLY
(
    SELECT
        rest2 = SUBSTRING(r.rest, CHARINDEX(N':', r.rest) + 1, 1024)
) AS p
WHERE b.lock_type IN ('PAGE', 'RID')
OPTION(RECOMPILE);

/* KEY -> object_id via sys.partitions, per contended database (no cross-db form of the view). */
SET @key_dbs =
    CURSOR
    LOCAL FAST_FORWARD
    FOR
    SELECT DISTINCT
        b.resource_database_id
    FROM #bpr AS b
    WHERE b.lock_type = 'KEY'
    AND   b.resource_database_id IS NOT NULL
    AND   b.resource_hobt_id IS NOT NULL;

OPEN @key_dbs;
FETCH NEXT FROM @key_dbs INTO @resolve_database_id;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF DB_NAME(@resolve_database_id) IS NOT NULL
    AND DATABASEPROPERTYEX(DB_NAME(@resolve_database_id), 'Status') = N'ONLINE'
    BEGIN
        /* #1865: HAS_DBACCESS is the screen, and it is the ONLY one of these three that answers the
           permission question. Verified on SQL 2022 against a login with no user in the target
           database: DB_NAME() still returns the name and DATABASEPROPERTYEX still returns ONLINE, so
           the guard above was never a screen -- the cross-database reference was attempted and raised
           916 on EVERY cycle, forever, for a least-privilege login whose posture is not changing.
           Screening it here is the same move #1854 made on query_store's enumeration, for the same
           burst: the row still says why, in its own label, instead of in a note repeated every five
           minutes. */
        IF HAS_DBACCESS(DB_NAME(@resolve_database_id)) = 1
        BEGIN
            SET @resolve_sql = N'
            UPDATE b
            SET b.resource_object_id = p.object_id
            FROM #bpr AS b
            JOIN ' + QUOTENAME(DB_NAME(@resolve_database_id)) + N'.sys.partitions AS p
              ON p.hobt_id = b.resource_hobt_id
            WHERE b.lock_type = ''KEY''
            AND   b.resource_database_id = @resolve_database_id
            OPTION(RECOMPILE);';

            /* The handler moved OUT of @resolve_sql (#1865). A TRY/CATCH inside a dynamic batch
               cannot catch that batch's own compile-time failure, which is precisely how a
               cross-database reference fails; the caller's can, and does -- verified live, 916
               caught here with ERROR_NUMBER/ERROR_MESSAGE intact. */
            BEGIN TRY
                EXECUTE sys.sp_executesql
                    @resolve_sql,
                  N'@resolve_database_id integer',
                    @resolve_database_id;
            END TRY
            BEGIN CATCH
                SELECT
                    @resolve_error = ERROR_NUMBER(),
                    @resolve_message = ERROR_MESSAGE();

                SET @resolve_reason =
                    CASE
                        WHEN @resolve_error IN (229, 230, 262, 297, 300, 916)
                        THEN @posture_reason
                        ELSE N'lookup error ' + CONVERT(nvarchar(11), @resolve_error)
                    END;

                UPDATE
                    b
                SET
                    b.unresolved_reason = @resolve_reason
                FROM #bpr AS b
                WHERE b.lock_type = 'KEY'
                AND   b.resource_database_id = @resolve_database_id
                OPTION(RECOMPILE);

                /* The posture is reported on the ROW and nowhere else. Everything the screen did not
                   anticipate is news, so it rides the #1851 channel with its cause already named. */
                IF @resolve_reason <> @posture_reason
                BEGIN
                    INSERT
                        @probe_failures
                    (
                        name,
                        error_text
                    )
                    VALUES
                    (
                        DB_NAME(@resolve_database_id),
                        N'KEY lock resolution (' + @resolve_reason + N'): ' + @resolve_message
                    );
                END;
            END CATCH;
        END;
        ELSE
        BEGIN
            UPDATE
                b
            SET
                b.unresolved_reason = @posture_reason
            FROM #bpr AS b
            WHERE b.lock_type = 'KEY'
            AND   b.resource_database_id = @resolve_database_id
            OPTION(RECOMPILE);
        END;
    END;
    ELSE
    BEGIN
        /* Dropped between the event and the read, or offline/restoring: not a permission problem and
           not the operator's to fix, but it is the answer an operator needs for why this row is Unresolved. */
        UPDATE
            b
        SET
            b.unresolved_reason = @unavailable_reason
        FROM #bpr AS b
        WHERE b.lock_type = 'KEY'
        AND   b.resource_database_id = @resolve_database_id
        OPTION(RECOMPILE);
    END;
    FETCH NEXT FROM @key_dbs INTO @resolve_database_id;
END;

/* PAGE / RID -> object_id via sys.dm_db_page_info (2019+ or Azure SQL DB; VIEW DATABASE STATE -> TRY/CATCH). */
IF @product_version >= 15
OR @azure = 1
BEGIN
    /* #1865: deliberately NO server-level permission screen here, though sys.dm_db_page_info is
       gated by one (VIEW SERVER STATE; VIEW SERVER PERFORMANCE STATE on 2022+). So is the
       sys.dm_xe_session_targets read this batch opens with -- verified on SQL 2022, a login without
       it fails that first statement with 297 and never reaches this line -- so a screen for it could
       only ever answer yes, and would be machinery that never fires.
       What DOES vary per database, and is what actually fails here, is metadata access to the
       database the page belongs to: holding VIEW SERVER STATE but having no user in that database,
       sys.dm_db_page_info raises 916, the same cross-database access error the KEY lookup raises,
       NOT a permission-class denial. Verified. That is what the HAS_DBACCESS screen below catches,
       and it is why the page site needs the same screen as the key site rather than a different one. */
    SET @page_dbs =
        CURSOR
        LOCAL FAST_FORWARD
        FOR
        SELECT DISTINCT
            b.resource_database_id
        FROM #bpr AS b
        WHERE b.lock_type IN ('PAGE', 'RID')
        AND   b.resource_database_id IS NOT NULL
        AND   b.resource_page_id IS NOT NULL;

    OPEN @page_dbs;
    FETCH NEXT FROM @page_dbs INTO @resolve_database_id;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF DB_NAME(@resolve_database_id) IS NOT NULL
        AND DATABASEPROPERTYEX(DB_NAME(@resolve_database_id), 'Status') = N'ONLINE'
        BEGIN
            /* Same screen as the KEY site, for the same reason (#1865): the two guards above
               both say yes for a database the login cannot enter; HAS_DBACCESS says no. */
            IF HAS_DBACCESS(DB_NAME(@resolve_database_id)) = 1
            BEGIN
                /* Still dynamic SQL, and it must stay that way even though nothing is spliced
                   into it: sys.dm_db_page_info does not exist before SQL Server 2019, and an
                   outer-batch reference to it would fail to compile the WHOLE collection query
                   there -- the runtime version gate above would never get the chance to skip it.
                   Deferring it to a nested batch is what keeps this collector runnable on 2016
                   and 2017 at all. */
                SET @resolve_sql = N'
                UPDATE b
                SET b.resource_object_id = pi.object_id
                FROM #bpr AS b
                CROSS APPLY sys.dm_db_page_info(b.resource_database_id, b.resource_file_id, b.resource_page_id, ''LIMITED'') AS pi
                WHERE b.lock_type IN (''PAGE'', ''RID'')
                AND   b.resource_database_id = @resolve_database_id
                OPTION(RECOMPILE);';

                /* Handler outside the dynamic batch (#1865), which matters more here than at the
                   KEY site: an Invalid-object-name failure on sys.dm_db_page_info is a COMPILE
                   failure of the nested batch, and a TRY/CATCH written inside that batch cannot
                   catch its own compile failure. This one can. */
                BEGIN TRY
                    EXECUTE sys.sp_executesql
                        @resolve_sql,
                      N'@resolve_database_id integer',
                        @resolve_database_id;
                END TRY
                BEGIN CATCH
                    SELECT
                        @resolve_error = ERROR_NUMBER(),
                        @resolve_message = ERROR_MESSAGE();

                    /* 2561 is the engine's answer for a page that is not what the report said --
                       past the end of the file, unallocated, or handed back to another object
                       between the event firing and this read. Verified live: every one of those
                       shapes raises 2561, Parameter 3 is incorrect for this statement. It is
                       transient and nobody's to fix, which is exactly why it is worth saying
                       once rather than screening away. */
                    SET @resolve_reason =
                        CASE
                            WHEN @resolve_error IN (229, 230, 262, 297, 300, 916)
                            THEN @posture_reason
                            WHEN @resolve_error = 2561
                            THEN N'page reallocated'
                            ELSE N'lookup error ' + CONVERT(nvarchar(11), @resolve_error)
                        END;

                    UPDATE
                        b
                    SET
                        b.unresolved_reason = @resolve_reason
                    FROM #bpr AS b
                    WHERE b.lock_type IN ('PAGE', 'RID')
                    AND   b.resource_database_id = @resolve_database_id
                    OPTION(RECOMPILE);

                    IF @resolve_reason <> @posture_reason
                    BEGIN
                        INSERT
                            @probe_failures
                        (
                            name,
                            error_text
                        )
                        VALUES
                        (
                            DB_NAME(@resolve_database_id),
                            N'PAGE/RID lock resolution (' + @resolve_reason + N'): ' + @resolve_message
                        );
                    END;
                END CATCH;
            END;
            ELSE
            BEGIN
                UPDATE
                    b
                SET
                    b.unresolved_reason = @posture_reason
                FROM #bpr AS b
                WHERE b.lock_type IN ('PAGE', 'RID')
                AND   b.resource_database_id = @resolve_database_id
                OPTION(RECOMPILE);
            END;
        END;
        ELSE
        BEGIN
            UPDATE
                b
            SET
                b.unresolved_reason = @unavailable_reason
            FROM #bpr AS b
            WHERE b.lock_type IN ('PAGE', 'RID')
            AND   b.resource_database_id = @resolve_database_id
            OPTION(RECOMPILE);
        END;
        FETCH NEXT FROM @page_dbs INTO @resolve_database_id;
    END;
END;
ELSE
BEGIN
    /* Pre-2019 on-prem: there is no sys.dm_db_page_info to call, so every PAGE/RID row here is
       unresolvable for a reason that has nothing to do with permissions or with this server's
       health. Saying so is the whole point of #1865 -- an operator on 2016 or 2017 otherwise sees a
       bare Unresolved forever with no way to learn that the engine simply cannot answer. */
    UPDATE
        b
    SET
        b.unresolved_reason = N'page lookup needs sql server 2019'
    FROM #bpr AS b
    WHERE b.lock_type IN ('PAGE', 'RID')
    AND   b.resource_database_id IS NOT NULL
    AND   b.resource_page_id IS NOT NULL
    OPTION(RECOMPILE);
END;

/* Format (plain schema.object) + label; the event object_id is the fallback, then the Unresolved sentinel. */
UPDATE
    b
SET
    b.contentious_object =
        COALESCE
        (
            CASE
                WHEN b.resource_object_id > 0
                AND  OBJECT_NAME(b.resource_object_id, b.resource_database_id) IS NOT NULL
                THEN CONCAT
                     (
                         OBJECT_SCHEMA_NAME(b.resource_object_id, b.resource_database_id),
                         N'.',
                         OBJECT_NAME(b.resource_object_id, b.resource_database_id)
                     )
            END,
            CASE
                WHEN b.object_id > 0
                AND  OBJECT_NAME(b.object_id, b.database_id) IS NOT NULL
                THEN CONCAT
                     (
                         OBJECT_SCHEMA_NAME(b.object_id, b.database_id),
                         N'.',
                         OBJECT_NAME(b.object_id, b.database_id)
                     )
            END,
            N'Unresolved: ' +
            CASE
                WHEN b.lock_type IS NULL
                THEN N''
                ELSE LOWER(b.lock_type) + N' lock, '
            END +
            /* #1876: the LOCK RESOURCE's database, not the event's. These are the same database for
               almost every blocking wait, and different for exactly the case this label is least able
               to afford being wrong about: a cross-database lock, where the event's database_id names
               where the blocked session was RUNNING while the object nobody could name lives somewhere
               else entirely -- so the row named one database and the reason described another.
               resource_database_id is parsed straight out of the wait resource, so it is authoritative
               when present; it is NULL only for a resource shape with no database in it, where the
               event's database is the best remaining answer. */
            N'database: ' + COALESCE(DB_NAME(b.resource_database_id), DB_NAME(b.database_id), N'unknown') +
            /* #1865: the reason, where one is known. This is the fix the issue actually asked for --
               the operator reading the grid gets the answer on the row instead of a bare
               Unresolved whose two causes -- grant the login access, versus the page moved
               and there is nothing to fix -- could not be told apart. ISNULL over the fragment, so a row nobody
               attempted to resolve (an OBJECT lock, or a resource type with no lookup path) is
               labeled exactly as it always was. */
            ISNULL(N' (' + b.unresolved_reason + N')', N'')
        )
FROM #bpr AS b
OPTION(RECOMPILE);

SELECT
    b.event_time,
    b.blocked_process_report_xml,
    b.object_id,
    b.database_id,
    b.contentious_object{planSelect}
FROM #bpr AS b{planApply}
OPTION(RECOMPILE);

/* Trailing result set = the payload path's probe-failure contract (#1851/#1865,
   EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync). Always returned, and empty on both of
   the postures this collector can recognize -- a login without metadata access is screened before
   it is attempted and answers on the row instead, which is the whole reason #1865 needed a design
   rather than a mechanical adoption of the channel. What arrives here is what the screens did not
   anticipate. */
SELECT
    name,
    error_text
FROM @probe_failures
ORDER BY
    name;";

        /* Use the most recent timestamp from the host store as the cutoff, or fall back to a
           10-minute window on first run. */
        var cutoffTime = context.Watermark ?? context.CollectionTime.AddMinutes(-10);

        return new CollectorQuery(query, new List<CollectorParameter>
        {
            new("@cutoff_time", cutoffTime, CollectorParameterType.DateTime2),
        });
    }

    /// <summary>
    /// A best-effort execution-plan lookup for one side of a blocked-process report, spliced into the
    /// final projection only when CapturePlanXml is set. Shreds that side's executionStack frames from
    /// the stored report XML and, walking top-of-stack first, returns the first frame whose
    /// (sql_handle, statement offsets) still resolves to a cached statement plan via
    /// sys.dm_exec_query_stats → sys.dm_exec_text_query_plan — the exact pair the #880 live "View Plan"
    /// uses. OUTER APPLY returning one nullable value, so it never multiplies or drops #bpr rows.
    /// <paramref name="processNode"/> is "blocked-process" or "blocking-process"; <paramref name="alias"/>
    /// is the APPLY alias the SELECT reads (bqp / kqp).
    /// </summary>
    private static string BuildPlanResolutionApply(string processNode, string alias) => $@"
OUTER APPLY
(
    SELECT TOP (1)
        query_plan = tqp.query_plan
    FROM
    (
        SELECT
            frame_handle = TRY_CONVERT(varbinary(64), fr.n.value('@sqlhandle', 'varchar(130)'), 1),
            frame_start = fr.n.value('(@stmtstart)[1]', 'integer'),
            frame_end = fr.n.value('(@stmtend)[1]', 'integer'),
            frame_ordinal = fr.n.value('let $c := . return count(../frame[. << $c])', 'integer')
        FROM (SELECT report = TRY_CAST(b.blocked_process_report_xml AS xml)) AS x
        CROSS APPLY x.report.nodes('/blocked-process-report/{processNode}/process/executionStack/frame') AS fr(n)
    ) AS frames
    JOIN sys.dm_exec_query_stats AS qs
      ON  qs.sql_handle = frames.frame_handle
      AND qs.statement_start_offset = ISNULL(frames.frame_start, 0)
      AND qs.statement_end_offset = ISNULL(frames.frame_end, -1)
    OUTER APPLY
        sys.dm_exec_text_query_plan
        (
            qs.plan_handle,
            qs.statement_start_offset,
            qs.statement_end_offset
        ) AS tqp
    WHERE frames.frame_handle IS NOT NULL
    AND   tqp.query_plan IS NOT NULL
    ORDER BY
        frames.frame_ordinal
) AS {alias}";

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("event_time", CollectorColumnType.Timestamp),
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_spid", CollectorColumnType.Integer),
        new CollectorColumn("blocked_ecid", CollectorColumnType.Integer),
        new CollectorColumn("blocking_spid", CollectorColumnType.Integer),
        new CollectorColumn("blocking_ecid", CollectorColumnType.Integer),
        new CollectorColumn("wait_time_ms", CollectorColumnType.BigInt),
        new CollectorColumn("wait_resource", CollectorColumnType.Varchar),
        new CollectorColumn("lock_mode", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_status", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_isolation_level", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_log_used", CollectorColumnType.BigInt),
        new CollectorColumn("blocked_transaction_count", CollectorColumnType.Integer),
        new CollectorColumn("blocked_client_app", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_host_name", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_login_name", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_sql_text", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_status", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_isolation_level", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_client_app", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_host_name", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_login_name", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_sql_text", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_transaction_name", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_transaction_name", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_last_tran_started", CollectorColumnType.Timestamp),
        new CollectorColumn("blocking_last_tran_started", CollectorColumnType.Timestamp),
        new CollectorColumn("blocked_last_batch_started", CollectorColumnType.Timestamp),
        new CollectorColumn("blocking_last_batch_started", CollectorColumnType.Timestamp),
        new CollectorColumn("blocked_last_batch_completed", CollectorColumnType.Timestamp),
        new CollectorColumn("blocking_last_batch_completed", CollectorColumnType.Timestamp),
        new CollectorColumn("blocked_priority", CollectorColumnType.Integer),
        new CollectorColumn("blocking_priority", CollectorColumnType.Integer),
        new CollectorColumn("blocked_process_report_xml", CollectorColumnType.Varchar),
        new CollectorColumn("object_id", CollectorColumnType.Integer),
        new CollectorColumn("database_id", CollectorColumnType.Integer),
        new CollectorColumn("contentious_object", CollectorColumnType.Varchar),
        new CollectorColumn("monitor_loop", CollectorColumnType.Integer),
        /* Trailing best-effort plan columns (appended so one ADD COLUMN each brings a pre-plan store
           up to shape; NULL on Lite and whenever the statement's plan aged out of cache). */
        new CollectorColumn("blocked_query_plan_xml", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_query_plan_xml", CollectorColumnType.Varchar),
    };

    /* ── #3161: what this collector measured, for its own collection_log row ───────────────────────────
       A quiet server and a server whose every captured report we failed to parse both used to arrive as
       SUCCESS, zero rows, NULL note — and only the second is a fault. The ring buffer read is already
       filtered to the watermark SERVER-side, so a zero EventsRead is genuinely "nothing new was captured",
       and these four counts separate that from the two ways a captured event gets dropped in this method.

       COUNTS, not a verdict: the read derives which situation it is looking at, every time it looks. A
       stored conclusion would be a stale gate the moment somebody restarted the session or fixed the
       capture — see CollectorMeasurement and CollectorRuntimePrecondition (#2546). Nothing here is computed
       for the note: all four figures are the loop's own bookkeeping, so the cost is four increments and no
       extra round trip. */

    /// <summary>Events the ring-buffer read delivered — already watermark-filtered server-side.</summary>
    public const string EventsReadMeasurement = "events_read";

    /// <summary>Delivered events carrying no report XML at all.</summary>
    public const string EmptyReportMeasurement = "report_xml_empty";

    /// <summary>Delivered events whose report XML this collector could not parse.</summary>
    public const string UnparsedReportMeasurement = "report_xml_unparsed";

    /// <summary>Events that became stored rows.</summary>
    public const string EventsStoredMeasurement = "events_stored";

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();
        var eventsRead = 0;
        var emptyReports = 0;
        var unparsedReports = 0;

        while (await reader.ReadAsync(cancellationToken))
        {
            eventsRead++;
            var eventTime = reader.IsDBNull(0) ? (DateTime?)null : reader.GetDateTime(0);
            var reportXml = reader.IsDBNull(1) ? null : reader.GetString(1);
            /* #1140: object_id/database_id are the event's own data fields and contentious_object
               is resolved server-side in the collection query (cols 2-4), not parsed from the XML. */
            var objectId = reader.IsDBNull(2) ? (int?)null : reader.GetInt32(2);
            var databaseId = reader.IsDBNull(3) ? (int?)null : reader.GetInt32(3);
            var contentiousObject = reader.IsDBNull(4) ? null : reader.GetString(4);
            /* The two best-effort plan columns ride at ordinals 5/6 only when CapturePlanXml spliced
               them into the projection; the short-circuit skips them entirely when off. */
            var blockedQueryPlanXml = context.CapturePlanXml && !reader.IsDBNull(5) ? reader.GetString(5) : null;
            var blockingQueryPlanXml = context.CapturePlanXml && !reader.IsDBNull(6) ? reader.GetString(6) : null;

            if (string.IsNullOrEmpty(reportXml))
            {
                emptyReports++;
                continue;
            }

            /* Parse the blocked process report XML in C# (read phase, like the original) */
            var parsed = ParseReportXml(reportXml, eventTime);
            if (parsed == null)
            {
                unparsedReports++;
                continue;
            }

            parsed.ReportXml = reportXml;
            parsed.ObjectId = objectId;
            parsed.DatabaseId = databaseId;
            parsed.ContentiousObject = contentiousObject;
            parsed.BlockedQueryPlanXml = blockedQueryPlanXml;
            parsed.BlockingQueryPlanXml = blockingQueryPlanXml;
            /* Per-database path (#1535): the capture database is authoritative for the
               per-database watermark key — a database-scoped session only captures its own
               database, and a report whose XML carries no currentdbname would otherwise never
               advance that database's watermark, re-inserting every cycle. Server-scoped
               platforms keep the parsed currentdbname (CurrentDatabaseName is null there). */
            parsed.DatabaseName = context.CurrentDatabaseName ?? parsed.DatabaseName;
            rows.Add(parsed);
        }

        /* Measure() ACCUMULATES a repeated label, which is what makes this correct on the per-database
           shape: RunsPerDatabase is true on Azure SQL DB, so the host calls this method once per database
           against the one context, and the cycle reports the sum rather than the last database's slice. */
        context.Measure(EventsReadMeasurement, eventsRead);
        context.Measure(EmptyReportMeasurement, emptyReports);
        context.Measure(UnparsedReportMeasurement, unparsedReports);
        context.Measure(EventsStoredMeasurement, rows.Count);

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.EventTime)
            .Value(row.DatabaseName)
            .Value(row.BlockedSpid)
            .Value(row.BlockedEcid)
            .Value(row.BlockingSpid)
            .Value(row.BlockingEcid)
            .Value(row.WaitTimeMs)
            .Value(row.WaitResource)
            .Value(row.LockMode)
            .Value(row.BlockedStatus)
            .Value(row.BlockedIsolationLevel)
            .Value(row.BlockedLogUsed)
            .Value(row.BlockedTransactionCount)
            .Value(row.BlockedClientApp)
            .Value(row.BlockedHostName)
            .Value(row.BlockedLoginName)
            .Value(row.BlockedSqlText)
            .Value(row.BlockingStatus)
            .Value(row.BlockingIsolationLevel)
            .Value(row.BlockingClientApp)
            .Value(row.BlockingHostName)
            .Value(row.BlockingLoginName)
            .Value(row.BlockingSqlText)
            .Value(row.BlockedTransactionName)
            .Value(row.BlockingTransactionName)
            .Value(row.BlockedLastTranStarted)
            .Value(row.BlockingLastTranStarted)
            .Value(row.BlockedLastBatchStarted)
            .Value(row.BlockingLastBatchStarted)
            .Value(row.BlockedLastBatchCompleted)
            .Value(row.BlockingLastBatchCompleted)
            .Value(row.BlockedPriority)
            .Value(row.BlockingPriority)
            .Value(row.ReportXml)
            .Value(row.ObjectId)
            .Value(row.DatabaseId)
            .Value(row.ContentiousObject)
            .Value(row.MonitorLoop)
            .Value(row.BlockedQueryPlanXml)    /* null unless CapturePlanXml resolved it (Darling) */
            .Value(row.BlockingQueryPlanXml);
    }

    /// <summary>
    /// Parses a blocked-process-report XML fragment into a row (reader-side fields left unset).
    /// XML structure: &lt;blocked-process-report&gt;&lt;blocked-process&gt;&lt;process ...&gt;&lt;inputbuf&gt;...
    ///                 &lt;blocking-process&gt;&lt;process ...&gt;&lt;inputbuf&gt;...
    /// </summary>
    public static Row? ParseReportXml(string xml, DateTime? eventTime)
    {
        try
        {
            var doc = XElement.Parse(xml);

            var blockedProcess = doc.Element("blocked-process")?.Element("process");
            var blockingProcess = doc.Element("blocking-process")?.Element("process");

            if (blockedProcess == null)
            {
                return null;
            }

            return new Row
            {
                EventTime = eventTime,
                // Lite's stored XML is rooted at <blocked-process-report>, so monitorLoop is a root attribute.
                MonitorLoop = int.TryParse(doc.Attribute("monitorLoop")?.Value, out var ml) ? ml : (int?)null,
                DatabaseName = blockedProcess.Attribute("currentdbname")?.Value,
                BlockedSpid = int.TryParse(blockedProcess.Attribute("spid")?.Value, out var bs) ? bs : 0,
                BlockedEcid = int.TryParse(blockedProcess.Attribute("ecid")?.Value, out var be) ? be : 0,
                BlockingSpid = int.TryParse(blockingProcess?.Attribute("spid")?.Value, out var bks) ? bks : 0,
                BlockingEcid = int.TryParse(blockingProcess?.Attribute("ecid")?.Value, out var bke) ? bke : 0,
                WaitTimeMs = long.TryParse(blockedProcess.Attribute("waittime")?.Value, out var wt) ? wt : 0,
                WaitResource = blockedProcess.Attribute("waitresource")?.Value,
                LockMode = blockedProcess.Attribute("lockMode")?.Value,
                BlockedStatus = blockedProcess.Attribute("status")?.Value,
                BlockedIsolationLevel = blockedProcess.Attribute("isolationlevel")?.Value,
                BlockedLogUsed = long.TryParse(blockedProcess.Attribute("logused")?.Value, out var lu) ? lu : 0,
                BlockedTransactionCount = int.TryParse(blockedProcess.Attribute("trancount")?.Value, out var tc) ? tc : 0,
                BlockedClientApp = blockedProcess.Attribute("clientapp")?.Value,
                BlockedHostName = blockedProcess.Attribute("hostname")?.Value,
                BlockedLoginName = blockedProcess.Attribute("loginname")?.Value,
                BlockedSqlText = blockedProcess.Element("inputbuf")?.Value?.Trim(),
                BlockedTransactionName = blockedProcess.Attribute("transactionname")?.Value,
                BlockedLastTranStarted = DateTime.TryParse(blockedProcess.Attribute("lasttranstarted")?.Value, out var blts) ? blts : null,
                BlockedLastBatchStarted = DateTime.TryParse(blockedProcess.Attribute("lastbatchstarted")?.Value, out var blbs) ? blbs : null,
                BlockedLastBatchCompleted = DateTime.TryParse(blockedProcess.Attribute("lastbatchcompleted")?.Value, out var blbc) ? blbc : null,
                BlockedPriority = int.TryParse(blockedProcess.Attribute("priority")?.Value, out var bp) ? bp : 0,
                BlockingStatus = blockingProcess?.Attribute("status")?.Value,
                BlockingIsolationLevel = blockingProcess?.Attribute("isolationlevel")?.Value,
                BlockingClientApp = blockingProcess?.Attribute("clientapp")?.Value,
                BlockingHostName = blockingProcess?.Attribute("hostname")?.Value,
                BlockingLoginName = blockingProcess?.Attribute("loginname")?.Value,
                BlockingSqlText = blockingProcess?.Element("inputbuf")?.Value?.Trim(),
                BlockingTransactionName = blockingProcess?.Attribute("transactionname")?.Value,
                BlockingLastTranStarted = DateTime.TryParse(blockingProcess?.Attribute("lasttranstarted")?.Value, out var bklts) ? bklts : null,
                BlockingLastBatchStarted = DateTime.TryParse(blockingProcess?.Attribute("lastbatchstarted")?.Value, out var bklbs) ? bklbs : null,
                BlockingLastBatchCompleted = DateTime.TryParse(blockingProcess?.Attribute("lastbatchcompleted")?.Value, out var bklbc) ? bklbc : null,
                BlockingPriority = int.TryParse(blockingProcess?.Attribute("priority")?.Value, out var bkp) ? bkp : 0
            };
        }
        catch
        {
            return null;
        }
    }
}
