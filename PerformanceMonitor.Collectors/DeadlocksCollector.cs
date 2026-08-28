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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Deadlock events from the app-managed PerformanceMonitor_Deadlock XE ring-buffer session.
/// Extracted verbatim from Lite's RemoteCollectorService.Deadlocks.cs: the server-scoped read
/// (on-prem/MI/RDS, event xml_deadlock_report) vs the database-scoped read (Azure SQL DB, event
/// database_xml_deadlock_report — run once per monitored database since #1535, with a
/// per-database watermark), the deadlock_time watermark that keeps ring-buffer lingerers
/// from re-inserting (10-minute fallback window), and the victim-inputbuf extraction from the
/// graph XML (parsed in the SQL/read phase — XElement.Parse is expensive and was previously
/// misattributed as storage time). Session lifecycle (create/start/ensure) stays host-side; the
/// session name lives here so the reader and the lifecycle can never disagree on it.
/// </summary>
public sealed class DeadlocksCollector : CollectorDefinitionBase<DeadlocksCollector.Row>
{
    public static DeadlocksCollector Instance { get; } = new();

    private DeadlocksCollector()
    {
    }

    /* We create and manage our own XE session to avoid conflicts with user's existing sessions */
    public const string XeSessionName = "PerformanceMonitor_Deadlock";

    public sealed class Row
    {
        public DateTime? DeadlockTime { get; set; }
        public string? VictimProcessId { get; set; }
        public string? VictimSqlText { get; set; }
        public string? GraphXml { get; set; }
        /* Best-effort execution plan for the victim's deadlocked statement, resolved from the plan
           cache at collection time (CapturePlanXml only). Null on Lite (flag off) and whenever the
           plan can't be recovered — see the resolution comment on the query fragments. */
        public string? VictimQueryPlanXml { get; set; }
        /* Keys the per-database watermark for Azure SQL DB's per-database capture sessions. On the
           per-database read path this is the capture database itself (context.CurrentDatabaseName —
           a database-scoped session only captures its own database, and a null here could never
           advance that database's watermark, re-inserting the row every cycle); on server-scoped
           platforms it falls back to the victim process's currentdbname graph attribute (same
           source the grids' per-process parse and BlockedProcessReportCollector use), null when
           the engine doesn't emit it. */
        public string? DatabaseName { get; set; }
    }

    /* Azure SQL DB: read from ring_buffer (database-scoped session)
       Azure SQL DB uses database_xml_deadlock_report event instead of xml_deadlock_report.
       Use .query() to get XML with structure intact, then CONVERT to nvarchar(max) */
    private const string AzureQueryText = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @PerformanceMonitor_Deadlock TABLE
(
    ring_buffer xml NOT NULL
);

INSERT
    @PerformanceMonitor_Deadlock
(
    ring_buffer
)
SELECT /* PerformanceMonitorLite */
    ring_xml = TRY_CAST(xet.target_data AS xml)
FROM sys.dm_xe_database_session_targets AS xet
JOIN sys.dm_xe_database_sessions AS xes
  ON xes.address = xet.event_session_address
WHERE xes.name = N'{XeSessionName}'
AND   xet.target_name = N'ring_buffer'
OPTION(RECOMPILE);

SELECT
    deadlock_time = evt.value('(@timestamp)[1]', 'datetime2'),
    victim_process_id = evt.value('(data[@name=""xml_report""]/value/deadlock/victim-list/victimProcess/@id)[1]', 'varchar(50)'),
    deadlock_graph_xml = CONVERT(nvarchar(max), evt.query('data[@name=""xml_report""]/value/deadlock'))/*DL_PLAN_SELECT*/,
    /* NULL on this arm: a database-scoped session's events belong to the connected database, which the
       runner already stamps from CurrentDatabaseName. The telemetry arm below knows better and says so. */
    source_database_name = CONVERT(nvarchar(128), NULL)
FROM
(
    SELECT
        pmd.ring_buffer
    FROM @PerformanceMonitor_Deadlock AS pmd
) AS rb
CROSS APPLY rb.ring_buffer.nodes('RingBufferTarget/event[@name=""database_xml_deadlock_report""]') AS q(evt)/*DL_PLAN_APPLY*/
WHERE evt.value('(@timestamp)[1]', 'datetime2') > @cutoff_time

UNION ALL

/* #2641: the DURABLE half, and the one that made the field report. The session above is
   DATABASE-SCOPED, so a connection to master captures only master's deadlocks — a reporter with
   fifty user databases got essentially none of theirs — and its ring buffer is memory-resident, so an
   Azure failover empties it without notice.

   sys.fn_xe_telemetry_blob_target_read_file is Azure's own file-backed telemetry. Verified against a
   live Azure SQL Database rather than from documentation: it returns database_xml_deadlock_report with
   the event timestamp, the USER database's name, the victim process id and the full graph, backed by a
   .xel blob rather than memory.

   It is MASTER-SCOPED, which is the whole trick and the thing that cost two wrong conclusions before it
   was found: from a user database the identical call returns zero rows, silently. Guarded on DB_NAME()
   so a user-database connection does not pay for a blob read that cannot return anything — and so the
   zero it would get is never mistaken for an absence of deadlocks. */
SELECT
    deadlock_time = tel.evt.value('(/event/@timestamp)[1]', 'datetime2'),
    victim_process_id = tel.evt.value('(/event/data[@name=""xml_report""]/value/deadlock/victim-list/victimProcess/@id)[1]', 'varchar(50)'),
    deadlock_graph_xml = CONVERT(nvarchar(max), tel.evt.query('/event/data[@name=""xml_report""]/value/deadlock'))/*DL_PLAN_SELECT_NULL*/,
    source_database_name = tel.evt.value('(/event/data[@name=""database_name""]/value)[1]', 'nvarchar(128)')
FROM
(
    SELECT evt = TRY_CAST(t.event_data AS xml)
    FROM sys.fn_xe_telemetry_blob_target_read_file('dl', NULL, NULL, NULL) AS t
    WHERE DB_NAME() = N'master'
) AS tel
WHERE tel.evt IS NOT NULL
AND   tel.evt.value('(/event/@timestamp)[1]', 'datetime2') > @cutoff_time
OPTION(RECOMPILE);";

    /* On-prem / Azure MI / AWS RDS: read from ring_buffer (server-scoped session)
       Use .query() to get XML with structure intact, then CONVERT to nvarchar(max) */
    private const string ServerScopedQueryText = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @PerformanceMonitor_Deadlock TABLE
(
    ring_buffer xml NOT NULL
);

INSERT
    @PerformanceMonitor_Deadlock
(
    ring_buffer
)
SELECT /* PerformanceMonitorLite */
    ring_xml = TRY_CAST(xet.target_data AS xml)
FROM sys.dm_xe_session_targets AS xet
JOIN sys.dm_xe_sessions AS xes
  ON xes.address = xet.event_session_address
WHERE xes.name = N'{XeSessionName}'
AND   xet.target_name = N'ring_buffer'
OPTION(RECOMPILE);

SELECT
    deadlock_time = evt.value('(@timestamp)[1]', 'datetime2'),
    victim_process_id = evt.value('(data[@name=""xml_report""]/value/deadlock/victim-list/victimProcess/@id)[1]', 'varchar(50)'),
    deadlock_graph_xml = CONVERT(nvarchar(max), evt.query('data[@name=""xml_report""]/value/deadlock'))/*DL_PLAN_SELECT*/,
    /* Always NULL here: on a server-scoped session the graph's own currentdbname is the only database
       fact, and ReadAsync already falls back to it. Present so BOTH engines project the same ordinals
       and one reader serves them (#2641). */
    source_database_name = CONVERT(nvarchar(128), NULL)
FROM
(
    SELECT
        pmd.ring_buffer
    FROM @PerformanceMonitor_Deadlock AS pmd
) AS rb
CROSS APPLY rb.ring_buffer.nodes('RingBufferTarget/event[@name=""xml_deadlock_report""]') AS q(evt)/*DL_PLAN_APPLY*/
WHERE evt.value('(@timestamp)[1]', 'datetime2') > @cutoff_time
OPTION(RECOMPILE);";

    /* Best-effort victim plan capture (CapturePlanXml only — Darling). Resolve the victim's deadlocked
       statement plan from the live plan cache by its executionStack frame's sql_handle + statement
       offsets, reusing the proven on-demand shape from Lite/Dashboard's #880 right-click View Plan
       (dm_exec_query_stats → dm_exec_text_query_plan). Only the victim's plan is captured at collection
       time — the deadlocks row is victim-centric (one row per event: victim_process_id, victim_sql_text),
       and the other processes' plans stay reachable via the apps' live right-click path. Honestly
       best-effort: the deadlock XE fires asynchronously, so by collection the victim's plan may have aged
       out; adhoc/dynamic-SQL frames carry an all-zero sql_handle that matches no query_stats row; either
       way the column lands NULL rather than fabricated. Frames walked top-of-stack first (the innermost,
       actually-deadlocked statement), first resolvable wins — mirrors ExtractDeadlockProcessFrames. The
       victim <process> is matched by id against the graph's victim-list (forward navigation only — no
       reverse XPath axis). Flag off: both fragments empty, so the SQL is byte-identical to the no-plan
       form; the two query variants share these fragments (the victim shred navigates the shared
       data[@name="xml_report"]/value/deadlock payload, identical on-prem and Azure SQL DB). */
    private const string PlanSelectFragment = @",
    victim_query_plan_xml = vqp.query_plan";

    /* #2681: the telemetry-blob branch of AzureQueryText projects the plan column at the SAME ordinal as
       the ring-buffer branch (so the UNION ALL lines up under the one shared reader), but it can NEVER
       resolve a real plan and must not reference vqp. That branch is MASTER-scoped and captures USER
       databases' deadlocks; the victim's plan lives in the USER database's cache, which a master
       connection cannot read (dm_exec_query_stats is database-scoped on Azure SQL DB). It also carries no
       DL_PLAN_APPLY marker, so vqp is undefined there — sharing the DL_PLAN_SELECT marker spliced
       vqp.query_plan into a block with no vqp alias and failed at parse ("could not be bound"), breaking
       the whole deadlocks collector for every Azure SQL DB target whenever CapturePlanXml was on. Gated on
       the same flag so the column appears exactly when the ring-buffer branch's does; off = both empty and
       byte-identical to the no-plan form. */
    private const string PlanSelectNullFragment = @",
    victim_query_plan_xml = CONVERT(nvarchar(max), NULL)";

    private const string PlanApplyFragment = @"
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
        FROM evt.nodes('data[@name=""xml_report""]/value/deadlock') AS d(dl)
        CROSS APPLY d.dl.nodes('process-list/process') AS p(pr)
        CROSS APPLY p.pr.nodes('executionStack/frame') AS fr(n)
        WHERE p.pr.value('(@id)[1]', 'varchar(50)') = d.dl.value('(victim-list/victimProcess/@id)[1]', 'varchar(50)')
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
) AS vqp";

    public override string Name => "deadlocks";

    public override string TargetTable => "deadlocks";

    /// <summary>Lite schema names this table's prefix id "deadlock_id"; Darling mirrors it.</summary>
    public override string PrefixIdColumnName => "deadlock_id";

    /// <summary>
    /// Only events newer than the newest already-collected deadlock are fetched, so an event
    /// lingering in the ring buffer across cycles is never inserted twice.
    /// </summary>
    public override string? WatermarkColumn => "deadlock_time";

    /// <summary>
    /// Azure SQL DB capture is a database-scoped session per monitored database (#1535 — a single
    /// session only ever saw the connection's own database, so deadlocks in the other databases of
    /// the logical server never appeared). The host ensures the session per database and this read
    /// then runs per database, exactly like the other Azure per-database collectors.
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target.IsAzureSqlDb;

    /// <summary>
    /// Per-database watermark for the per-database sessions: each database's ring buffer dispatches
    /// independently, so one database's newer deadlock must not watermark past another's older one.
    /// </summary>
    public override string? PerDatabaseWatermarkColumn => "database_name";

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        var text = context.Target.IsAzureSqlDb ? AzureQueryText : ServerScopedQueryText;

        /* Splice (Darling) or erase (Lite, flag off — byte-identical) the victim plan capture. */
        text = text
            .Replace("/*DL_PLAN_SELECT*/", context.CapturePlanXml ? PlanSelectFragment : "", StringComparison.Ordinal)
            .Replace("/*DL_PLAN_SELECT_NULL*/", context.CapturePlanXml ? PlanSelectNullFragment : "", StringComparison.Ordinal)
            .Replace("/*DL_PLAN_APPLY*/", context.CapturePlanXml ? PlanApplyFragment : "", StringComparison.Ordinal);

        /* Use the most recent timestamp from the host store as the cutoff, or fall back to a
           10-minute window on first run. */
        var cutoffTime = context.Watermark ?? context.CollectionTime.AddMinutes(-10);

        return new CollectorQuery(text, new List<CollectorParameter>
        {
            new("@cutoff_time", cutoffTime, CollectorParameterType.DateTime2),
        });
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("deadlock_time", CollectorColumnType.Timestamp),
        new CollectorColumn("victim_process_id", CollectorColumnType.Varchar),
        new CollectorColumn("victim_sql_text", CollectorColumnType.Varchar),
        new CollectorColumn("deadlock_graph_xml", CollectorColumnType.Varchar),
        /* Trailing best-effort victim plan (appended so one ADD COLUMN brings a pre-plan store up to
           shape; NULL on Lite and whenever the victim's plan aged out of cache). */
        new CollectorColumn("victim_query_plan_xml", CollectorColumnType.Varchar),
        /* Trailing victim database (appended so one ADD COLUMN brings a pre-#1535 store up to shape;
           NULL when the graph carries no currentdbname). Keys the Azure per-database watermark. */
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        /* Parse the graph XML here in the read (SQL) phase — ExtractVictimSqlText does
           XElement.Parse, which is expensive and was previously misattributed as storage time. */
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var sourceDbOrdinal = SourceDatabaseNameOrdinal(context);
            var sourceDatabaseName = reader.FieldCount > sourceDbOrdinal && !reader.IsDBNull(sourceDbOrdinal)
                ? reader.GetString(sourceDbOrdinal)
                : null;

            var victimProcessId = reader.IsDBNull(1) ? null : reader.GetString(1);
            var graphXml = reader.IsDBNull(2) ? null : reader.GetString(2);
            var victim = ExtractVictimFields(graphXml, victimProcessId);

            rows.Add(new Row
            {
                DeadlockTime = reader.IsDBNull(0) ? null : reader.GetDateTime(0),
                VictimProcessId = victimProcessId,
                VictimSqlText = victim.SqlText,
                GraphXml = graphXml,
                /* victim_query_plan_xml rides at ordinal 3 only when CapturePlanXml spliced it into
                   the projection; the short-circuit skips it entirely when off. */
                VictimQueryPlanXml = context.CapturePlanXml && !reader.IsDBNull(3) ? reader.GetString(3) : null,
                /* #2641: a row that KNOWS its own database wins, and only the Azure telemetry arm
                   does. That arm is read from master while CurrentDatabaseName is "master", so taking
                   the connection's database would stamp every deadlock on the server as master's —
                   turning the one source that spans all fifty databases into fifty rows about the
                   wrong one.

                   Otherwise unchanged: per-database path takes the capture database (authoritative for
                   a database-scoped session), server-scoped falls back to the victim's currentdbname. */
                DatabaseName = sourceDatabaseName ?? context.CurrentDatabaseName ?? victim.DatabaseName,
            });
        }

        return rows;
    }

    /// <summary>
    /// The ordinal <c>source_database_name</c> sits at.
    ///
    /// <para>It is the LAST column of every arm, and the projection's width moves with
    /// <c>CapturePlanXml</c> — the victim plan is spliced in at ordinal 3 only when it is on. So the same
    /// conditional the plan column itself uses, one place along, rather than <c>FieldCount - 1</c>: an
    /// explicit ordinal goes wrong loudly when someone appends a column, and the relative one goes wrong
    /// silently by reading whatever they appended.</para>
    ///
    /// <para>By ordinal and not by name because the reader contract here is positional — the test fake
    /// throws from <c>GetName</c> deliberately, to stop a collector quietly depending on a capability the
    /// production readers have and the fixtures do not.</para>
    /// </summary>
    private static int SourceDatabaseNameOrdinal(CollectorContext context) => context.CapturePlanXml ? 4 : 3;

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.DeadlockTime)
            .Value(row.VictimProcessId)
            .Value(row.VictimSqlText)
            .Value(row.GraphXml)
            .Value(row.VictimQueryPlanXml)     /* null unless CapturePlanXml resolved it (Darling) */
            .Value(row.DatabaseName);          /* null when the graph carries no currentdbname */
    }

    /// <summary>
    /// Extracts victim SQL text from a deadlock graph XML fragment.
    /// </summary>
    public static string? ExtractVictimSqlText(string? graphXml, string? victimProcessId)
        => ExtractVictimFields(graphXml, victimProcessId).SqlText;

    /// <summary>
    /// Extracts the victim process's inputbuf and currentdbname from a deadlock graph XML fragment
    /// in one parse (XElement.Parse is the expensive part — see the ReadAsync comment). Victim
    /// matched by id, case-insensitively; falls back to the first process when the id is missing or
    /// unmatched (the pre-existing ExtractVictimSqlText contract). currentdbname is the same
    /// attribute the grids' per-process parse and the blocked-process collector read; engines that
    /// don't emit it yield a null DatabaseName rather than a fabricated one.
    /// </summary>
    public static (string? SqlText, string? DatabaseName) ExtractVictimFields(string? graphXml, string? victimProcessId)
    {
        if (string.IsNullOrEmpty(graphXml))
        {
            return (null, null);
        }

        try
        {
            var doc = XElement.Parse(graphXml);

            /* Find all process nodes in the deadlock graph */
            var processes = doc.Descendants("process").ToList();

            /* If we have a victim ID, find that specific process */
            XElement? victim = null;
            if (!string.IsNullOrEmpty(victimProcessId))
            {
                victim = processes.FirstOrDefault(p =>
                    string.Equals(p.Attribute("id")?.Value, victimProcessId, StringComparison.OrdinalIgnoreCase));
            }

            /* Fallback: the first process */
            victim ??= processes.FirstOrDefault();

            return (victim?.Element("inputbuf")?.Value?.Trim(), victim?.Attribute("currentdbname")?.Value);
        }
        catch
        {
            return (null, null);
        }
    }
}
