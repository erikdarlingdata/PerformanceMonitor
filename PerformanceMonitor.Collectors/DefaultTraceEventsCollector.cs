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
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Always-on server events read from the built-in Default Trace via <c>sys.traces</c> +
/// <c>sys.fn_trace_gettable</c> — the events no DMV captures: data/log file auto-grow/shrink stalls,
/// severe ErrorLog writes, schema DDL (Object:Created/Altered/Deleted), security audits (audit-change /
/// DBCC / alter-trace), and Server Memory Change. Ported from the full Dashboard's
/// <c>install/29_collect_default_trace.sql</c> curated event set into the shared collector library, so
/// both SKUs (portable Lite → DuckDB, Darling → Postgres) collect it from one definition.
///
/// <para><b>Agentless-safe (must-handle):</b> <c>fn_trace_gettable</c> runs entirely server-side against
/// the trace's own rollover files — no agent, no file share, no monitor-side artifact.</para>
///
/// <para><b>Reads the CURRENT file in steady state (#1962):</b> a rollover-set read has no predicate
/// pushdown — <c>fn_trace_gettable</c> materializes every file it is handed and the <c>StartTime</c>
/// watermark filters AFTERWARDS — so passing the BASE path with <c>max_files</c> re-read all 5 × 20 MB
/// every cycle to return a handful of new events. Measured on the reporting fleet's apex box: 897 ms for
/// the whole set against 178 ms for the current file alone, a 5.0x saving repeated by every server on
/// every cycle. Steady-state cycles therefore pass the CURRENT file with <c>1</c>, and fall back to the
/// base-path + <c>max_files</c> form (the proven Dashboard idiom, unchanged) whenever the trace file has
/// MOVED since the last cycle — because post-watermark events then span the previous file and the current
/// one. "Moved" is decided against <see cref="LastTraceFilePathStateKey"/>, the path this collector stored
/// at the end of its previous cycle; no stored path (first run, a restarted host, an upgraded store, a
/// cycle that failed) means the whole set is read, since a collector that cannot know what it missed must
/// not assume it missed nothing. Both the comparison and the read use ONE captured value of
/// <c>t.path</c>, so the path recorded for the next cycle is provably the file this cycle read.</para>
///
/// <para><b>Read-only (never writes the target):</b> unlike the Dashboard collector this definition does
/// NOT sp_configure/RECONFIGURE the trace on or attempt the disable/re-enable restart cycle — the shared
/// collectors are pure reads, and the default trace is enabled by default on every non-Azure-SQL-DB
/// engine. When it is off (or the configured-but-not-running SQL Server bug), the <c>status = 1</c> guard
/// simply yields zero rows for that cycle rather than mutating the monitored server.</para>
///
/// <para><b>STATEFUL dedup (must-handle):</b> mirrors the deadlock / blocked-process-report /
/// system_health collectors — an <c>event_time</c> watermark (the newest already-collected StartTime from
/// the host store) filters <c>ft.StartTime &gt; @cutoff_time</c>, so a rollover file re-read across cycles
/// never re-ingests an event. Unlike those XE ring-buffer collectors (whose 10-minute first-run fallback
/// fits their tiny recent-only buffers), the FIRST run here collects EVERYTHING still in the trace files
/// (a far-past cutoff), because the default trace persists hours-to-days of history on disk that would
/// otherwise be lost — the same first-run-collects-all choice the Dashboard collector makes.</para>
///
/// <para><b>Config-change de-duplication (must-handle):</b> the Dashboard collector's config-event slice
/// (Server / Database Configuration Change, Alter Database) is deliberately DROPPED here — Darling already
/// tracks that exact slice by diffing its <c>server_config</c> / <c>database_config</c> / <c>trace_flags</c>
/// snapshots (get_server_config_changes / get_database_config_changes / get_trace_flag_changes), which
/// carry the real old→new values, so re-collecting the trace's config events would double-count it. The
/// remaining categories have no such snapshot twin. (DBCC commands stay — <c>Audit DBCC Event</c> is a
/// distinct "someone ran DBCC" audit, not the resulting trace-flag STATE the snapshot diff tracks.)</para>
///
/// <para><b>Azure SQL Database (edition 5): does NOT apply</b> — there is no default trace on Azure SQL DB
/// (<c>sys.traces</c> exposes none), so the collector is gated off via <see cref="AppliesTo"/> =
/// <c>!IsAzureSqlDb</c> rather than logging a per-cycle empty/error. Azure SQL Managed Instance (edition 8)
/// and on-prem / AWS RDS all have the default trace and collect normally.</para>
/// </summary>
public sealed class DefaultTraceEventsCollector : CollectorDefinitionBase<DefaultTraceEventsCollector.Row>
{
    public static DefaultTraceEventsCollector Instance { get; } = new();

    private DefaultTraceEventsCollector()
    {
    }

    /// <summary>
    /// TRUE-first-run cutoff sentinel: on a genuine first run (no watermark AND no prior SUCCESS for this
    /// collector+server in the host's collection_log) collect everything still on disk in the trace's
    /// rollover files rather than only a recent window (the default trace persists real history; the XE
    /// ring-buffer collectors' 10-minute fallback would silently drop it). Mirrors the Dashboard collector's
    /// <c>CONVERT(datetime2(7), '19000101')</c> first-run branch. Bounded in practice by the trace's total
    /// on-disk size (default 5 files × 20 MB) and the curated low-volume event set. NOT used when the hot
    /// store was merely emptied by retention/archival — see <see cref="ArchivalEmptyFallbackHours"/>.
    /// </summary>
    private static readonly DateTime FirstRunCutoff = new(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// The BOUNDED fallback window (hours) used when the watermark is null but the collector HAS succeeded
    /// before (<see cref="CollectorContext.HasCollectedBefore"/>) — i.e. the hot store was emptied by
    /// retention/archival, not a true first run. Re-reading all on-disk .trc history here would re-insert
    /// events already aged into Lite's parquet archive, and <c>v_default_trace_events</c> UNIONs hot + parquet
    /// with no dedup, so those rows would DOUBLE-COUNT and recur every archival cycle. This window is far
    /// smaller than any retention horizon, so on an archival-emptied (necessarily quiet) server it re-reads
    /// only genuinely-recent events — which by definition are not yet archived — and never re-scans parquet.
    /// Ports the Dashboard's collection_log-SUCCESS guard (install/29_collect_default_trace.sql lines 116-117).
    ///
    /// <para><b>Computed server-side against <c>SYSDATETIME()</c>, not from the host's collection time.</b>
    /// <c>ft.StartTime</c> is the monitored server's LOCAL wall clock (the .trc files store local time), so a
    /// host-supplied UTC bound is a different clock and the window it delivers is skewed by the server's
    /// offset: at UTC-7 a 24-hour request reads only the last 17 hours, leaving a 7-hour hole in trace
    /// history that nothing later refills, and at UTC+10 it reads 34 hours, re-ingesting events already aged
    /// into parquet — the exact double-count this bound exists to prevent. Both are silent. The watermark
    /// branch needs no such correction: the watermark IS a previously-stored <c>event_time</c>, already in
    /// the server's clock. Same reasoning and same fix as <see cref="JobHistoryCollector"/>'s
    /// <c>GETDATE()</c>-relative fallback, whose <c>run_datetime</c> is server-local for the same reason.</para>
    /// </summary>
    private const int ArchivalEmptyFallbackHours = 24;

    /// <summary>
    /// The archival-empty floor, in the monitored server's own clock. Sits behind a <c>COALESCE</c> so the
    /// watermark and true-first-run branches keep binding <c>@cutoff_time</c> verbatim and only the archival
    /// branch — which binds NULL — reaches the server clock. Non-sargable by construction and free anyway:
    /// <c>fn_trace_gettable</c> materializes every file it is handed, so no predicate on
    /// <c>ft.StartTime</c> pushes down (see the remarks at the top of this class).
    /// </summary>
    private static readonly string CutoffExpression = string.Format(
        CultureInfo.InvariantCulture,
        "COALESCE(@cutoff_time, DATEADD(HOUR, -{0}, SYSDATETIME()))",
        ArchivalEmptyFallbackHours);

    /// <summary>
    /// The <see cref="CollectorContext.State"/> key holding the trace file path this collector read on
    /// its previous cycle for this server (#1962) — the sibling of the <c>event_time</c> watermark for a
    /// fact no MAX() over the collected rows can produce. It has to be host state and not a payload
    /// column precisely because the cycles that most need it collect NOTHING: a server whose default
    /// trace churns through 20 MB files while producing no CURATED events would, with a row-derived
    /// path, never record a rollover and so never leave the expensive fallback.
    /// </summary>
    public const string LastTraceFilePathStateKey = "last_trace_file_path";

    public sealed class Row
    {
        public DateTime? EventTime { get; set; }
        public string? EventName { get; set; }
        public int? EventClass { get; set; }
        public int? Spid { get; set; }
        public string? DatabaseName { get; set; }
        public int? DatabaseId { get; set; }
        public string? LoginName { get; set; }
        public string? HostName { get; set; }
        public string? ApplicationName { get; set; }
        public string? ObjectName { get; set; }
        public string? Filename { get; set; }
        public long? IntegerData { get; set; }
        public long? IntegerData2 { get; set; }
        public string? TextData { get; set; }
        public string? SessionLoginName { get; set; }
        public int? ErrorNumber { get; set; }
        public int? Severity { get; set; }
        public int? State { get; set; }
        public long? EventSequence { get; set; }
        public long? DurationUs { get; set; }
        public DateTime? EndTime { get; set; }
    }

    /* The curated, config-slice-free event set (see the class remarks). A {0} placeholder is spliced with
       the per-server excluded-database clause on ft.DatabaseName.

       The trace's live path is captured into @current_trace_path FIRST and everything downstream reads
       that ONE value — the arm decision, the file(s) handed to fn_trace_gettable, and the path handed
       back for the next cycle. That is what makes "the path we stored" and "the file we read" the same
       fact rather than two reads of sys.traces that a rollover can land between: whenever a rollover DOES
       fall inside the batch, the stored path is the file that was read, so the next cycle sees the
       inequality and takes the fallback. Deciding host-side instead would need a separate pre-read, and a
       rollover between that read and this one would store a path nothing had read yet — silently skipping
       the previous file's tail (#1962).

       Steady state (@current_trace_path = @last_trace_path — the path stored by the previous cycle) reads
       ONLY the current file. Any other case reads the whole rollover set via the base path + max_files:
       a moved path (rollover), and — because a NULL @last_trace_path makes the comparison UNKNOWN rather
       than true — no stored path at all (first run, restarted host, upgraded store, failed prior cycle).
       The base-path normalization (strip _NN + re-append the extension) is the proven Dashboard idiom,
       unchanged: the strip only fires when the LAST underscore sits in the FILENAME, i.e. after the last
       path separator (either family — the collector runs against Windows and Linux targets): SQL Server on
       Linux names the initial trace file without a rollover suffix (log.trc, not log_1.trc), so an
       unguarded strip mangles the path (log.trc.trc — #1633), and a trace DIRECTORY containing an
       underscore would mangle it a second way (/var/opt/my_sql/log/log.trc -> /var/opt/my.trc — #1636);
       fn_trace_gettable then errors on the nonexistent file (Msg 19049) and no events are collected.
       Falling back to the unmodified path in both cases keeps the Windows behavior (which always has the
       _N suffix) unchanged.

       The trailing SELECT returns the captured path (NULL when there is no running default trace, so
       nothing is recorded and the next cycle stays on the fallback). It is a SECOND RESULT SET read by
       ReadAsync — not a row on the payload, which a cycle collecting zero events would never carry.

       sys.traces stays in the FROM: its is_default/status predicate is what keeps fn_trace_gettable from
       being invoked at all on a server whose default trace is off, and max_files still comes from it.
       READ UNCOMMITTED like every collector; OPTION(RECOMPILE) because the cutoff selectivity varies
       wildly between the all-history first run and the tiny steady-state windows. */
    /* Parsed once — the template is re-formatted every collection cycle (CA1863). */
    private static readonly System.Text.CompositeFormat QueryTemplateFormat =
        System.Text.CompositeFormat.Parse(QueryTemplate);

    private const string QueryTemplate = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @current_trace_path nvarchar(260);

SELECT
    @current_trace_path = t.path
FROM sys.traces AS t
WHERE t.is_default = 1
AND   t.status = 1;

SELECT
    event_time = ft.StartTime,
    event_name = te.name,
    event_class = ft.EventClass,
    spid = ft.SPID,
    database_name = ft.DatabaseName,
    database_id = ft.DatabaseID,
    login_name = ft.LoginName,
    host_name = ft.HostName,
    application_name = ft.ApplicationName,
    object_name = ft.ObjectName,
    filename = ft.FileName,
    integer_data = ft.IntegerData,
    integer_data_2 = ft.IntegerData2,
    text_data = ft.TextData,
    session_login_name = ft.SessionLoginName,
    error_number = ft.Error,
    severity = ft.Severity,
    state = ft.State,
    event_sequence = ft.EventSequence,
    duration_us = ft.Duration,
    end_time = ft.EndTime
FROM sys.traces AS t
CROSS APPLY sys.fn_trace_gettable
(
    CASE
        /*Steady state: the trace has not rolled since the previous cycle, so every post-watermark
          event is in the file we are already pointed at*/
        WHEN @current_trace_path = @last_trace_path
        THEN @current_trace_path
        WHEN CHARINDEX(N'_', REVERSE(@current_trace_path)) > 0
        AND (CHARINDEX(N'\', REVERSE(@current_trace_path)) = 0 OR CHARINDEX(N'_', REVERSE(@current_trace_path)) < CHARINDEX(N'\', REVERSE(@current_trace_path)))
        AND (CHARINDEX(N'/', REVERSE(@current_trace_path)) = 0 OR CHARINDEX(N'_', REVERSE(@current_trace_path)) < CHARINDEX(N'/', REVERSE(@current_trace_path)))
        THEN LEFT(@current_trace_path, LEN(@current_trace_path) - CHARINDEX(N'_', REVERSE(@current_trace_path))) + RIGHT(@current_trace_path, 4)
        ELSE @current_trace_path
    END,
    CASE
        WHEN @current_trace_path = @last_trace_path
        THEN 1
        ELSE t.max_files
    END
) AS ft
JOIN sys.trace_events AS te
  ON ft.EventClass = te.trace_event_id
WHERE t.is_default = 1
AND   t.status = 1
AND   ft.StartTime > {1}
AND   ISNULL(ft.DatabaseID, 0) NOT IN (1, 3, 4) /*master, model, msdb*/
AND   ISNULL(ft.DatabaseID, 0) < 32761 /*exclude contained AG system databases*/{0}
AND
(
    /*Server Memory Change events*/
    (te.name LIKE N'%Server Memory Change%')
    OR
    /*Data/Log file auto grow/shrink STALLS (duration over one second only)*/
    (
        ISNULL(ft.Duration, 0) > 1000000
        AND
        (
            te.name LIKE N'%Data File Auto Grow%'
            OR te.name LIKE N'%Log File Auto Grow%'
            OR te.name LIKE N'%Data File Auto Shrink%'
            OR te.name LIKE N'%Log File Auto Shrink%'
        )
    )
    OR
    /*ErrorLog writes (severity gating happens in the significant-set layer, not here)*/
    (te.name = N'ErrorLog')
    OR
    /*Schema DDL (@include_object_ddl gates the whole slice; exclude tempdb and auto-created _WA_ statistics)*/
    (
        @include_object_ddl = 1
        AND ISNULL(ft.DatabaseID, 0) <> 2
        AND ISNULL(ft.ObjectName, N'') NOT LIKE N'[_]WA[_]%'
        AND te.name IN (N'Object:Created', N'Object:Altered', N'Object:Deleted')
    )
    OR
    /*Security audit events (audit-change, DBCC, alter-trace)*/
    (te.name IN (N'Audit Change Audit Event', N'Audit DBCC Event', N'Audit Server Alter Trace Event'))
)
ORDER BY
    ft.StartTime DESC
OPTION(RECOMPILE);

SELECT
    current_trace_path = @current_trace_path;";

    public override string Name => "default_trace_events";

    public override string TargetTable => "default_trace_events";

    /// <summary>Lite schema names this table's prefix id "default_trace_event_id"; Darling mirrors it.</summary>
    public override string PrefixIdColumnName => "default_trace_event_id";

    /// <summary>
    /// Only events newer than the newest already-collected StartTime are fetched, so re-reading the
    /// rolling trace files across cycles never re-ingests an event (mirrors the deadlock / system_health
    /// collectors' event_time watermark). First run has no watermark → collect all on-disk history.
    /// </summary>
    public override string? WatermarkColumn => "event_time";

    /// <summary>
    /// The last-seen trace file path (see <see cref="LastTraceFilePathStateKey"/>): the host loads it
    /// before the query is built and persists whatever this cycle observed once the cycle completes.
    /// The only collector that declares state — every other one's dedup is derivable from its rows.
    /// </summary>
    public override IReadOnlyList<string> StateKeys { get; } = new[] { LastTraceFilePathStateKey };

    /// <summary>
    /// Collects on SQL Server, Azure SQL Managed Instance, and AWS RDS — everywhere the built-in default
    /// trace exists. NOT Azure SQL Database (edition 5): there is no default trace there. A verified
    /// platform gap gated here, not a scope-down (see the class remarks).
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => !target.IsAzureSqlDb;

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        /* Per-server excluded databases apply to the trace's own NULLABLE DatabaseName — NULL-safe, so
           server-level curated events (Server Memory Change, server-scoped audits/ErrorLog) that carry a
           NULL DatabaseName are KEPT rather than silently dropped by a bare `NOT IN`. Spliced onto its own
           line (empty => nothing added) so the generated T-SQL stays readable. */
        var (exclusionClause, exclusionParameters) = BuildNullSafeDatabaseExclusion(context.ExcludedDatabases);
        var exclusionSplice = exclusionClause.Length == 0 ? string.Empty : "\r\n" + exclusionClause;

        var text = string.Format(CultureInfo.InvariantCulture, QueryTemplateFormat, exclusionSplice, CutoffExpression);

        /* Cutoff selection (the Dashboard's first-run guard, ported):
             - watermark present            -> steady state, collect newer than it. Already the server's own
                                               clock: the watermark IS a stored event_time (ft.StartTime).
             - watermark null, never succeeded -> TRUE first run, collect ALL on-disk .trc history. A far-past
                                               sentinel, so which clock reads it cannot matter.
             - watermark null, HAS succeeded   -> hot store emptied by retention/archival. NULL here, and the
                                               query's COALESCE supplies the bound from SYSDATETIME() — see
                                               ArchivalEmptyFallbackHours for why the host's UTC clock cannot
                                               express this window against a server-local ft.StartTime. */
        var cutoffTime = context.Watermark
            ?? (context.HasCollectedBefore
                ? (DateTime?)null
                : FirstRunCutoff);

        /* The path this collector read last cycle, or null when it has none — a first run, a host that
           restarted before it could store one, a store upgraded onto this build, or a previous cycle that
           failed. Null binds as DBNull, so the query's `@current_trace_path = @last_trace_path` is UNKNOWN
           and the read takes the whole rollover set: not knowing what you missed is not the same as
           knowing you missed nothing (#1962). */
        context.State.TryGetValue(LastTraceFilePathStateKey, out var lastTracePath);

        var parameters = new List<CollectorParameter>(exclusionParameters.Count + 3)
        {
            new("@cutoff_time", cutoffTime, CollectorParameterType.DateTime2),
            new("@last_trace_path", lastTracePath, CollectorParameterType.NVarChar260),
            /* Operator toggle mirroring the Dashboard proc's @include_object_events: 1 keeps the Object DDL
               slice (today's behavior), 0 drops it so a create/drop-happy workload can't flood the tab. */
            new("@include_object_ddl", context.CollectSchemaChangeEvents ? 1 : 0, CollectorParameterType.Int32),
        };
        parameters.AddRange(exclusionParameters);

        return new CollectorQuery(text, parameters);
    }

    /// <summary>
    /// The NULL-safe per-server excluded-database predicate for the trace's nullable <c>DatabaseName</c>.
    /// Server-level curated events carry a NULL DatabaseName; a bare <c>NOT IN (...)</c> evaluates NULL to
    /// UNKNOWN and would silently drop the WHOLE category whenever any database is excluded, so this keeps
    /// NULL rows (<c>ft.DatabaseName IS NULL OR ...</c>) — matching the Dashboard's id-based, NULL-safe
    /// exclusion. The shared <see cref="DatabaseExclusionFilter"/> is deliberately NOT reused here: its
    /// other callers filter a non-nullable name column and must keep emitting the bare form.
    /// </summary>
    private static (string Clause, List<CollectorParameter> Parameters) BuildNullSafeDatabaseExclusion(
        IReadOnlyList<string> excludedDatabaseNames)
    {
        if (excludedDatabaseNames.Count == 0)
        {
            return (string.Empty, new List<CollectorParameter>());
        }

        var paramNames = new List<string>(excludedDatabaseNames.Count);
        var parameters = new List<CollectorParameter>(excludedDatabaseNames.Count);
        for (int i = 0; i < excludedDatabaseNames.Count; i++)
        {
            var p = $"@excl_db_{i}";
            paramNames.Add(p);
            parameters.Add(new CollectorParameter(p, excludedDatabaseNames[i], CollectorParameterType.NVarChar128));
        }

        return ($"AND (ft.DatabaseName IS NULL OR ft.DatabaseName NOT IN ({string.Join(", ", paramNames)}))", parameters);
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("event_time", CollectorColumnType.Timestamp),
        new CollectorColumn("event_name", CollectorColumnType.Varchar),
        new CollectorColumn("event_class", CollectorColumnType.Integer),
        new CollectorColumn("spid", CollectorColumnType.Integer),
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("database_id", CollectorColumnType.Integer),
        new CollectorColumn("login_name", CollectorColumnType.Varchar),
        new CollectorColumn("host_name", CollectorColumnType.Varchar),
        new CollectorColumn("application_name", CollectorColumnType.Varchar),
        new CollectorColumn("object_name", CollectorColumnType.Varchar),
        new CollectorColumn("filename", CollectorColumnType.Varchar),
        new CollectorColumn("integer_data", CollectorColumnType.BigInt),
        new CollectorColumn("integer_data_2", CollectorColumnType.BigInt),
        new CollectorColumn("text_data", CollectorColumnType.Varchar),
        new CollectorColumn("session_login_name", CollectorColumnType.Varchar),
        new CollectorColumn("error_number", CollectorColumnType.Integer),
        new CollectorColumn("severity", CollectorColumnType.Integer),
        new CollectorColumn("state", CollectorColumnType.Integer),
        new CollectorColumn("event_sequence", CollectorColumnType.BigInt),
        new CollectorColumn("duration_us", CollectorColumnType.BigInt),
        new CollectorColumn("end_time", CollectorColumnType.Timestamp),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row
            {
                EventTime = reader.IsDBNull(0) ? null : reader.GetDateTime(0),
                EventName = reader.IsDBNull(1) ? null : reader.GetString(1),
                EventClass = reader.IsDBNull(2) ? null : Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture),
                Spid = reader.IsDBNull(3) ? null : Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture),
                DatabaseName = reader.IsDBNull(4) ? null : reader.GetString(4),
                DatabaseId = reader.IsDBNull(5) ? null : Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture),
                LoginName = reader.IsDBNull(6) ? null : reader.GetString(6),
                HostName = reader.IsDBNull(7) ? null : reader.GetString(7),
                ApplicationName = reader.IsDBNull(8) ? null : reader.GetString(8),
                ObjectName = reader.IsDBNull(9) ? null : reader.GetString(9),
                Filename = reader.IsDBNull(10) ? null : reader.GetString(10),
                IntegerData = reader.IsDBNull(11) ? null : Convert.ToInt64(reader.GetValue(11), CultureInfo.InvariantCulture),
                IntegerData2 = reader.IsDBNull(12) ? null : Convert.ToInt64(reader.GetValue(12), CultureInfo.InvariantCulture),
                TextData = reader.IsDBNull(13) ? null : reader.GetString(13),
                SessionLoginName = reader.IsDBNull(14) ? null : reader.GetString(14),
                ErrorNumber = reader.IsDBNull(15) ? null : Convert.ToInt32(reader.GetValue(15), CultureInfo.InvariantCulture),
                Severity = reader.IsDBNull(16) ? null : Convert.ToInt32(reader.GetValue(16), CultureInfo.InvariantCulture),
                State = reader.IsDBNull(17) ? null : Convert.ToInt32(reader.GetValue(17), CultureInfo.InvariantCulture),
                EventSequence = reader.IsDBNull(18) ? null : Convert.ToInt64(reader.GetValue(18), CultureInfo.InvariantCulture),
                DurationUs = reader.IsDBNull(19) ? null : Convert.ToInt64(reader.GetValue(19), CultureInfo.InvariantCulture),
                EndTime = reader.IsDBNull(20) ? null : reader.GetDateTime(20),
            });
        }

        /* The batch's SECOND result set: the trace file path this read actually used, recorded for the
           next cycle's rollover comparison (#1962). Read here, off the payload, because the cycles that
           depend on it hardest are the ones that returned no rows at all — a quiet server still has to
           notice its trace rolled. A missing/NULL path (no running default trace) records nothing, which
           leaves the next cycle on the safe fallback. */
        if (await reader.NextResultAsync(cancellationToken)
            && await reader.ReadAsync(cancellationToken)
            && !reader.IsDBNull(0))
        {
            context.PendingState[LastTraceFilePathStateKey] = reader.GetString(0);
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.EventTime)          /* event_time TIMESTAMP (watermark) */
            .Value(row.EventName)          /* event_name VARCHAR */
            .Value(row.EventClass)         /* event_class INTEGER */
            .Value(row.Spid)               /* spid INTEGER */
            .Value(row.DatabaseName)       /* database_name VARCHAR */
            .Value(row.DatabaseId)         /* database_id INTEGER */
            .Value(row.LoginName)          /* login_name VARCHAR */
            .Value(row.HostName)           /* host_name VARCHAR */
            .Value(row.ApplicationName)    /* application_name VARCHAR */
            .Value(row.ObjectName)         /* object_name VARCHAR */
            .Value(row.Filename)           /* filename VARCHAR */
            .Value(row.IntegerData)        /* integer_data BIGINT */
            .Value(row.IntegerData2)       /* integer_data_2 BIGINT */
            .Value(row.TextData)           /* text_data VARCHAR */
            .Value(row.SessionLoginName)   /* session_login_name VARCHAR */
            .Value(row.ErrorNumber)        /* error_number INTEGER */
            .Value(row.Severity)           /* severity INTEGER */
            .Value(row.State)              /* state INTEGER */
            .Value(row.EventSequence)      /* event_sequence BIGINT */
            .Value(row.DurationUs)         /* duration_us BIGINT */
            .Value(row.EndTime);           /* end_time TIMESTAMP */
    }
}
