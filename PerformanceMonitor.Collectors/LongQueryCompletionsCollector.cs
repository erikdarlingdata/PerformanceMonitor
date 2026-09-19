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

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Long-running query completions from the app-managed PerformanceMonitor_LongQueryCompletions XE
/// ring-buffer session (#1496) — the Extended Events equivalent of the classic long-query trace
/// (modeled on SSMS's QuickSessionStandard completion trio): <c>rpc_completed</c> +
/// <c>sql_batch_completed</c> filtered by <c>duration &gt;= @long_query_threshold</c> (microseconds),
/// plus <c>attention</c> captured UNFILTERED. Attention is the "long query that never finished —
/// cancelled/timed out" evidence and is deliberately not duration-filtered: the attention event's own
/// <c>duration</c> is the cancellation-HANDLING time (tens of milliseconds), not the query's runtime,
/// so a <c>duration &gt;=</c> predicate would discard exactly the cancelled-long-query rows the trace
/// exists to capture (MS Learn's timeout worked example: the batch's real duration ≈ 30 s while the
/// paired attention's duration = 40 ms). The completed events still carry the true runtime AND a
/// <c>result</c> = <c>Abort</c> when a long query is killed, so an aborted long query is captured on
/// BOTH sides — a completed row (result=Abort) and, for correlation, an attention row on the same
/// session_id.
///
/// <para><b>Opt-in, default OFF (Erik's dedicated switch).</b> A completion trace is not free on a
/// busy server — rpc_completed/sql_batch_completed fire per statement/batch even though the duration
/// predicate discards most of them — so the collector is seeded DISABLED
/// (<see cref="CollectorScheduleDefaults"/> DefaultEnabled = false) and the operator opts in per fleet.
/// The XE session lifecycle FOLLOWS the flag: enabling creates the session on the monitored servers,
/// disabling DROPS it there (the session itself is the cost, so merely skipping collection is not
/// enough). Both SKUs drive that reconcile from the shared <see cref="BuildCreateSessionSql"/> /
/// <see cref="BuildStartSessionSql"/> / <see cref="BuildDropSessionSql"/> here so the two hosts can
/// never drift on the (complex, 3-event / 9-action on-prem, 8-action Azure) DDL — the same "reader and
/// lifecycle never disagree" reason <see cref="XeSessionName"/> lives here.</para>
///
/// <para>Reads the ring buffer exactly like <see cref="BlockedProcessReportCollector"/>: server-scoped
/// on on-prem/MI/RDS, database-scoped per monitored database on Azure SQL DB (#1535 — a single session
/// only ever captured the connection's own database; rpc_completed/sql_batch_completed/attention are
/// all available in an Azure database-scoped session, verified against MS Learn), with an
/// <c>event_time</c> watermark (10-minute first-run fallback) that keeps ring-buffer lingerers from
/// re-inserting. The event payload + the QuickSessionStandard actions are shredded in the SQL/read
/// phase into typed columns. The ACTION surface is narrower on Azure SQL DB than the event surface
/// (#3753): two of the nine actions are rejected by a database-scoped session outright, so the Azure
/// DDL is built from a closed list of the rejects (<see cref="AzureSqlDbUnavailableActions"/>) and
/// substitutes <see cref="AzureSqlDbUsernameAction"/> for the principal name — see
/// <see cref="ActionList"/>.</para>
/// </summary>
public sealed class LongQueryCompletionsCollector : CollectorDefinitionBase<LongQueryCompletionsCollector.Row>
{
    public static LongQueryCompletionsCollector Instance { get; } = new();

    private LongQueryCompletionsCollector()
    {
    }

    /* We create and manage our own XE session to avoid conflicts with user's existing sessions. */
    public const string XeSessionName = "PerformanceMonitor_LongQueryCompletions";

    /// <summary>
    /// The default duration threshold (MICROSECONDS) for the completed-event predicate — 2 seconds,
    /// matching the value the full Dashboard's long-running-query SQL Trace shipped
    /// (install/30_collect_trace_management.sql: <c>@duration_threshold_ms bigint = 2000000</c>, which
    /// is microseconds despite the name, and which captured the same RPC:Completed / SQL:BatchCompleted
    /// / Attention trio). No per-collector numeric-knob surface exists in the schedule model today
    /// (schedules carry only frequency + retention), so this is a hardcoded default per the
    /// defaults-over-speculative-config rule — mirroring the blocked-process threshold, which is
    /// likewise a hardcoded 5-second sp_configure bootstrap.
    /// </summary>
    public const long DefaultDurationThresholdMicroseconds = 2_000_000;

    /// <summary>The XE event names this session captures (the QuickSessionStandard completion trio).</summary>
    public const string RpcCompletedEvent = "rpc_completed";
    public const string SqlBatchCompletedEvent = "sql_batch_completed";
    public const string AttentionEvent = "attention";

    public sealed class Row
    {
        public DateTime? EventTime { get; set; }
        public string? EventType { get; set; }
        public int? DatabaseId { get; set; }
        public string? DatabaseName { get; set; }
        public int? SessionId { get; set; }
        public string? ClientAppName { get; set; }
        public int? ClientPid { get; set; }
        public string? NtUserName { get; set; }
        /* server_principal_name on-prem; on Azure SQL DB the sqlserver.username action's value (the
           connecting login / Entra principal), because server_principal_name is not an action a
           database-scoped session may carry (#3753). NULL when neither action is in the payload. */
        public string? ServerPrincipalName { get; set; }
        public string? QueryHash { get; set; }
        public long? EventSequence { get; set; }
        /* Microseconds, from the completed event's payload. NULL for attention: the attention event's
           own duration is the cancellation-handling time, not the query runtime (see the class doc). */
        public long? DurationMicroseconds { get; set; }
        public long? CpuTimeMicroseconds { get; set; }
        public long? PhysicalReads { get; set; }
        public long? LogicalReads { get; set; }
        public long? Writes { get; set; }
        public long? RowCount { get; set; }
        /* OK / Error / Abort on the completed events (Abort = the long query was cancelled/timed out);
           NULL for attention. */
        public string? Result { get; set; }
        /* rpc_completed.statement or sql_batch_completed.batch_text; NULL for attention. */
        public string? StatementText { get; set; }
        /* rpc_completed.object_name (the module that ran); NULL for sql batches and attention. */
        public string? ObjectName { get; set; }
    }

    public override string Name => "long_query_completions";

    public override string TargetTable => "long_query_completions";

    /// <summary>Lite schema names this table's prefix id "long_query_completion_id"; Darling mirrors it.</summary>
    public override string PrefixIdColumnName => "long_query_completion_id";

    /// <summary>
    /// Only events newer than the newest already-collected completion are fetched, so an event
    /// lingering in the ring buffer across cycles is never inserted twice (mirrors the blocked-process
    /// and deadlock collectors' timestamp watermark).
    /// </summary>
    public override string? WatermarkColumn => "event_time";

    /// <summary>
    /// Azure SQL DB capture is a database-scoped session per monitored database (#1535). The host
    /// ensures the session per database and this read then runs per database, exactly like the
    /// blocked-process and deadlock collectors.
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target.IsAzureSqlDb;

    /// <summary>
    /// Per-database watermark for the per-database sessions: each database's ring buffer dispatches
    /// independently, so one database's newer completion must not watermark past another's older one.
    /// The <c>database_name</c> action carries the capture database.
    /// </summary>
    public override string? PerDatabaseWatermarkColumn => "database_name";

    /* Server- vs database-scoped ring-buffer source is the only engine difference; the event/action
       shred is shared. The session only captures the three long-completion events, so the read shreds
       everything in the ring buffer newer than the watermark — the duration predicate lives in the
       session DDL (BuildCreateSessionSql), not here. The customizable text columns (statement /
       batch_text) are turned on in the DDL's SET clause; object_name is rpc_completed's own default
       data field (#2129 — SETting a collect_object_name there fails the CREATE), so all three are
       present in the payload here.

       Every action read below is an XQuery over action[@name="..."] — an action the session does not
       carry has no node, the path yields NULL, and the column lands NULL. That is what lets ONE shred
       serve both action lists: nt_username is simply absent on Azure and reads NULL, and the
       server_principal_name column COALESCEs the on-prem server_principal_name action with the Azure
       stand-in sqlserver.username (#3753) — exactly one of the two is ever present in a given session's
       payload, so the COALESCE is a selector, not a merge. */
    private const string ShredSelect = @"
SELECT
    event_time = evt.value('(@timestamp)[1]', 'datetime2'),
    event_type = evt.value('(@name)[1]', 'sysname'),
    duration = evt.value('(data[@name=""duration""]/value/text())[1]', 'bigint'),
    cpu_time = evt.value('(data[@name=""cpu_time""]/value/text())[1]', 'bigint'),
    physical_reads = evt.value('(data[@name=""physical_reads""]/value/text())[1]', 'bigint'),
    logical_reads = evt.value('(data[@name=""logical_reads""]/value/text())[1]', 'bigint'),
    writes = evt.value('(data[@name=""writes""]/value/text())[1]', 'bigint'),
    row_count = evt.value('(data[@name=""row_count""]/value/text())[1]', 'bigint'),
    result = evt.value('(data[@name=""result""]/value/text())[1]', 'nvarchar(60)'),
    statement_text =
        COALESCE
        (
            evt.value('(data[@name=""statement""]/value/text())[1]', 'nvarchar(max)'),
            evt.value('(data[@name=""batch_text""]/value/text())[1]', 'nvarchar(max)')
        ),
    object_name = evt.value('(data[@name=""object_name""]/value/text())[1]', 'nvarchar(512)'),
    client_app_name = evt.value('(action[@name=""client_app_name""]/value/text())[1]', 'nvarchar(256)'),
    client_pid = evt.value('(action[@name=""client_pid""]/value/text())[1]', 'integer'),
    database_id = evt.value('(action[@name=""database_id""]/value/text())[1]', 'integer'),
    database_name = evt.value('(action[@name=""database_name""]/value/text())[1]', 'sysname'),
    event_sequence = evt.value('(action[@name=""event_sequence""]/value/text())[1]', 'bigint'),
    nt_username = evt.value('(action[@name=""nt_username""]/value/text())[1]', 'nvarchar(256)'),
    query_hash = evt.value('(action[@name=""query_hash""]/value/text())[1]', 'nvarchar(64)'),
    server_principal_name =
        COALESCE
        (
            evt.value('(action[@name=""server_principal_name""]/value/text())[1]', 'nvarchar(256)'),
            evt.value('(action[@name=""username""]/value/text())[1]', 'nvarchar(256)')
        ),
    session_id = evt.value('(action[@name=""session_id""]/value/text())[1]', 'integer')
FROM @PerformanceMonitor_LongQueryCompletions AS rb
CROSS APPLY rb.ring_buffer.nodes('RingBufferTarget/event[@name=""rpc_completed"" or @name=""sql_batch_completed"" or @name=""attention""]') AS q(evt)
WHERE evt.value('(@timestamp)[1]', 'datetime2') > @cutoff_time
OPTION(RECOMPILE);";

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        string ringBufferSource = context.Target.IsAzureSqlDb
            ? @"sys.dm_xe_database_session_targets AS xet
JOIN sys.dm_xe_database_sessions AS xes
  ON xes.address = xet.event_session_address"
            : @"sys.dm_xe_session_targets AS xet
JOIN sys.dm_xe_sessions AS xes
  ON xes.address = xet.event_session_address";

        string query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @PerformanceMonitor_LongQueryCompletions TABLE
(
    ring_buffer xml NOT NULL
);

INSERT
    @PerformanceMonitor_LongQueryCompletions
(
    ring_buffer
)
SELECT /* PerformanceMonitorLite */
    ring_xml = TRY_CAST(xet.target_data AS xml)
FROM {ringBufferSource}
WHERE xes.name = N'{XeSessionName}'
AND   xet.target_name = N'ring_buffer'
OPTION(RECOMPILE);
{ShredSelect}";

        /* Use the most recent event_time from the host store as the cutoff, or fall back to a
           10-minute window on first run (mirrors the blocked-process / deadlock collectors). */
        var cutoffTime = context.Watermark ?? context.CollectionTime.AddMinutes(-10);

        return new CollectorQuery(query, new List<CollectorParameter>
        {
            new("@cutoff_time", cutoffTime, CollectorParameterType.DateTime2),
        });
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("event_time", CollectorColumnType.Timestamp),
        new CollectorColumn("event_type", CollectorColumnType.Varchar),
        new CollectorColumn("database_id", CollectorColumnType.Integer),
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("session_id", CollectorColumnType.Integer),
        new CollectorColumn("client_app_name", CollectorColumnType.Varchar),
        new CollectorColumn("client_pid", CollectorColumnType.Integer),
        new CollectorColumn("nt_username", CollectorColumnType.Varchar),
        new CollectorColumn("server_principal_name", CollectorColumnType.Varchar),
        new CollectorColumn("query_hash", CollectorColumnType.Varchar),
        new CollectorColumn("event_sequence", CollectorColumnType.BigInt),
        new CollectorColumn("duration_microseconds", CollectorColumnType.BigInt),
        new CollectorColumn("cpu_time_microseconds", CollectorColumnType.BigInt),
        new CollectorColumn("physical_reads", CollectorColumnType.BigInt),
        new CollectorColumn("logical_reads", CollectorColumnType.BigInt),
        new CollectorColumn("writes", CollectorColumnType.BigInt),
        new CollectorColumn("row_count", CollectorColumnType.BigInt),
        new CollectorColumn("result", CollectorColumnType.Varchar),
        new CollectorColumn("statement_text", CollectorColumnType.Varchar),
        new CollectorColumn("object_name", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var eventType = reader.IsDBNull(1) ? null : reader.GetString(1);
            var isAttention = string.Equals(eventType, AttentionEvent, StringComparison.Ordinal);

            /* Reader ordinals follow the ShredSelect projection order exactly (0-19). */
            var parsedDatabaseName = reader.IsDBNull(14) ? null : reader.GetString(14);

            rows.Add(new Row
            {
                EventTime = reader.IsDBNull(0) ? null : reader.GetDateTime(0),
                EventType = eventType,
                /* duration is the runtime for completed events; for attention it is the
                   cancellation-handling time (not the query runtime), so it is dropped to NULL. */
                DurationMicroseconds = isAttention || reader.IsDBNull(2) ? null : reader.GetInt64(2),
                CpuTimeMicroseconds = reader.IsDBNull(3) ? null : reader.GetInt64(3),
                PhysicalReads = reader.IsDBNull(4) ? null : reader.GetInt64(4),
                LogicalReads = reader.IsDBNull(5) ? null : reader.GetInt64(5),
                Writes = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                RowCount = reader.IsDBNull(7) ? null : reader.GetInt64(7),
                Result = reader.IsDBNull(8) ? null : reader.GetString(8),
                StatementText = reader.IsDBNull(9) ? null : reader.GetString(9),
                ObjectName = reader.IsDBNull(10) ? null : reader.GetString(10),
                ClientAppName = reader.IsDBNull(11) ? null : reader.GetString(11),
                ClientPid = reader.IsDBNull(12) ? null : reader.GetInt32(12),
                DatabaseId = reader.IsDBNull(13) ? null : reader.GetInt32(13),
                /* Per-database path (#1535): the capture database is authoritative for the
                   per-database watermark key — a database-scoped session only captures its own
                   database. Server-scoped platforms keep the database_name action (CurrentDatabaseName
                   is null there). */
                DatabaseName = context.CurrentDatabaseName ?? parsedDatabaseName,
                EventSequence = reader.IsDBNull(15) ? null : reader.GetInt64(15),
                NtUserName = reader.IsDBNull(16) ? null : reader.GetString(16),
                QueryHash = reader.IsDBNull(17) ? null : reader.GetString(17),
                ServerPrincipalName = reader.IsDBNull(18) ? null : reader.GetString(18),
                SessionId = reader.IsDBNull(19) ? null : reader.GetInt32(19),
            });
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.EventTime)
            .Value(row.EventType)
            .Value(row.DatabaseId)
            .Value(row.DatabaseName)
            .Value(row.SessionId)
            .Value(row.ClientAppName)
            .Value(row.ClientPid)
            .Value(row.NtUserName)
            .Value(row.ServerPrincipalName)
            .Value(row.QueryHash)
            .Value(row.EventSequence)
            .Value(row.DurationMicroseconds)
            .Value(row.CpuTimeMicroseconds)
            .Value(row.PhysicalReads)
            .Value(row.LogicalReads)
            .Value(row.Writes)
            .Value(row.RowCount)
            .Value(row.Result)
            .Value(row.StatementText)
            .Value(row.ObjectName);
    }

    /* ---------------------------------------------------------------------------------------------
       Session DDL — shared by both hosts' lifecycle code (Lite's RemoteCollectorService and Darling's
       DarlingXeSessions) so the two can never drift on the 3-event / 9-action (8 on Azure, #3753) /
       SET-collect / duration-predicate CREATE. The lifecycle STRUCTURE (existence check, create, start,
       drop) still lives per-host, mirroring the blocked-process/deadlock pattern; only this DDL TEXT is
       centralized.
       --------------------------------------------------------------------------------------------- */

    /// <summary>
    /// The 9 QuickSessionStandard actions this collector adopted in #1496, in the order the
    /// server-scoped DDL emits them (client_app_name, client_pid, database_id, database_name,
    /// event_sequence, nt_username, query_hash, server_principal_name, session_id). On-prem/MI/RDS emit
    /// all nine, unchanged since #1496.
    /// </summary>
    private static readonly string[] QuickSessionStandardActions =
    {
        "sqlserver.client_app_name",
        "sqlserver.client_pid",
        "sqlserver.database_id",
        "sqlserver.database_name",
        "package0.event_sequence",
        "sqlserver.nt_username",
        "sqlserver.query_hash",
        "sqlserver.server_principal_name",
        "sqlserver.session_id",
    };

    /// <summary>
    /// The QuickSessionStandard actions Azure SQL DB REJECTS in a database-scoped session (#3753) — a
    /// CLOSED list, and the ONLY thing the Azure DDL strips. The engine refuses the whole CREATE with
    /// error 25744, <c>The action '%.*ls' is not available for Azure SQL Database</c> (MS Learn,
    /// Database Engine events and errors 23000–25999:
    /// https://learn.microsoft.com/sql/relational-databases/errors-events/database-engine-events-and-errors-23000-to-25999),
    /// so a single rejected action means the session never exists and the collector is dead on every
    /// Azure database, every sweep — which is exactly what #3753 reported for
    /// <c>server_principal_name</c> after #1496 had already stripped <c>nt_username</c> ad hoc. The
    /// first strip was a one-off string splice; the second showed the shape wants to be a list the
    /// DDL is generated FROM and a pin asserts AGAINST, so that a third member is a one-line edit here
    /// and the pin is what says the Azure DDL is clean.
    ///
    /// <para><b>Why a list here rather than a lookup on the server.</b> MS Learn publishes no static
    /// per-action availability table for Azure SQL DB; its Extended Events page
    /// (https://learn.microsoft.com/azure/azure-sql/database/xevent-db-diff-from-svr) directs you to
    /// query <c>sys.dm_xe_objects</c> on the database itself. The DDL is built before any connection is
    /// opened, and the reconcile that runs it is a create-or-start by session NAME (both hosts), not a
    /// compare of the live definition against the desired one — so a wrong member here cannot make the
    /// session flap; it can only fail the CREATE the way the reporter saw. Membership is verified
    /// against Microsoft's own Azure profiler template instead: Azure Data Studio ships
    /// <c>Standard_OnPrem</c> and <c>Standard_Azure</c> side by side on the same <c>attention</c> /
    /// <c>rpc_completed</c> / <c>sql_batch_completed</c> events, and the Azure one drops exactly these
    /// two actions and substitutes <see cref="AzureSqlDbUsernameAction"/>
    /// (https://github.com/microsoft/azuredatastudio/blob/main/src/sql/workbench/contrib/profiler/browser/profiler.contribution.ts).
    /// The seven actions that remain each appear in a Microsoft-authored <c>ON DATABASE</c> session on
    /// Azure SQL DB (that template; the Azure DB Support team's login-audit session for
    /// <c>database_name</c>, which the template happens not to carry). Consistent with that, the
    /// reporter's own engine run named <c>server_principal_name</c> as the reject — the seventh action
    /// in the Azure DDL as it then stood — and not any of the six emitted before it.</para>
    /// </summary>
    public static readonly IReadOnlyList<string> AzureSqlDbUnavailableActions = new[]
    {
        "sqlserver.nt_username",
        "sqlserver.server_principal_name",
    };

    /// <summary>
    /// The action that carries the connecting principal's name in an Azure SQL DB database-scoped
    /// session, emitted in place of the two rejected identity actions (#3753) so the
    /// <c>server_principal_name</c> column is populated on Azure rather than landing NULL forever. It is
    /// what Microsoft's own <c>Standard_Azure</c> profiler template substitutes for
    /// <c>nt_username</c> + <c>server_principal_name</c> (link on
    /// <see cref="AzureSqlDbUnavailableActions"/>), and it is the identity action in the Azure DB
    /// Support team's published <c>ON DATABASE</c> sessions that capture a login (their login-audit and
    /// change-tracking-audit examples). The value is the session's login name — a SQL login or an Entra principal on Azure —
    /// which is the same information <c>server_principal_name</c> carries on-prem. The shred reads
    /// either action into the one column (see <c>ShredSelect</c>); on-prem does NOT emit this action,
    /// so the on-prem DDL and payload are unchanged.
    /// </summary>
    public const string AzureSqlDbUsernameAction = "sqlserver.username";

    /// <summary>
    /// The action block shared by all three events. Server-scoped: the nine
    /// <see cref="QuickSessionStandardActions"/> verbatim. Database-scoped (Azure SQL DB): every member
    /// of <see cref="AzureSqlDbUnavailableActions"/> stripped and <see cref="AzureSqlDbUsernameAction"/>
    /// appended, for eight. Generated from the lists rather than spliced by hand so the Azure branch
    /// can never again carry an action the closed list says it must not (#3753).
    /// </summary>
    private static string ActionList(bool databaseScoped)
    {
        IEnumerable<string> actions = QuickSessionStandardActions;

        if (databaseScoped)
        {
            actions = actions
                .Where(action => !AzureSqlDbUnavailableActions.Contains(action, StringComparer.Ordinal))
                .Append(AzureSqlDbUsernameAction);
        }

        return string.Join(",", actions.Select(action => "\n        " + action));
    }

    /// <summary>
    /// The <c>CREATE EVENT SESSION</c> for the long-query completion trace. <paramref name="databaseScoped"/>
    /// selects the Azure SQL DB (ON DATABASE) form; otherwise the server-scoped form (with
    /// MEMORY_PARTITION_MODE = NONE for a single readable ring buffer / AWS RDS compatibility, mirroring
    /// the deadlock session). The <paramref name="thresholdMicroseconds"/> duration predicate is applied
    /// to the two COMPLETED events only; <c>attention</c> is captured unfiltered (see the class doc). The
    /// customizable TEXT columns (statement / batch_text) are turned on via SET so they are actually
    /// collected; object_name needs no SET — it is one of rpc_completed's default data fields (#2129).
    /// </summary>
    public static string BuildCreateSessionSql(bool databaseScoped, long thresholdMicroseconds)
    {
        var scope = databaseScoped ? "DATABASE" : "SERVER";
        var actions = ActionList(databaseScoped);
        /* Server-scoped: single, unpartitioned ring buffer (readable target_data on multi-socket boxes,
           AWS RDS compatible). Azure database-scoped sessions do not accept MEMORY_PARTITION_MODE. */
        var partitionMode = databaseScoped ? "" : "\n    MEMORY_PARTITION_MODE = NONE,";

        /* #2129: rpc_completed SETs only collect_statement. object_name is one of that event's
           DEFAULT data fields — the collect_object_name customizable attribute belongs to
           sp_statement_completed, and SETting it here made the CREATE fail on every server. The
           note lives in C# on purpose: the DDL string ships to every monitored server, and the
           test pin asserts the bogus attribute appears NOWHERE in it, comment included. */
        return $@"
CREATE EVENT SESSION [{XeSessionName}]
ON {scope}
ADD EVENT sqlserver.rpc_completed
(
    SET
        collect_statement = 1
    ACTION
    ({actions}
    )
    WHERE duration >= {thresholdMicroseconds}
),
ADD EVENT sqlserver.sql_batch_completed
(
    SET
        collect_batch_text = 1
    ACTION
    ({actions}
    )
    WHERE duration >= {thresholdMicroseconds}
),
ADD EVENT sqlserver.attention
(
    ACTION
    ({actions}
    )
)
ADD TARGET package0.ring_buffer
(
    SET max_memory = 4096
)
WITH
(
    MAX_DISPATCH_LATENCY = 5 SECONDS,
    EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,{partitionMode}
    STARTUP_STATE = ON
);";
    }

    /// <summary>Starts the session (CREATE does not auto-start it, even with STARTUP_STATE = ON).</summary>
    public static string BuildStartSessionSql(bool databaseScoped) =>
        $"ALTER EVENT SESSION [{XeSessionName}] ON {(databaseScoped ? "DATABASE" : "SERVER")} STATE = START;";

    /// <summary>
    /// Idempotently drops the session (the opt-out path — disabling the collector removes the
    /// server-side session, which is the actual busy-server cost). Guarded by an existence check so a
    /// drop on a server that never had the session is a clean no-op. Server- vs database-scoped catalog
    /// + scope selected by <paramref name="databaseScoped"/>.
    /// </summary>
    public static string BuildDropSessionSql(bool databaseScoped)
    {
        if (databaseScoped)
        {
            return $@"
IF EXISTS
(
    SELECT
        1/0
    FROM sys.database_event_sessions
    WHERE name = N'{XeSessionName}'
)
BEGIN
    DROP EVENT SESSION [{XeSessionName}] ON DATABASE;
END;";
        }

        return $@"
IF EXISTS
(
    SELECT
        1/0
    FROM sys.server_event_sessions
    WHERE name = N'{XeSessionName}'
)
BEGIN
    DROP EVENT SESSION [{XeSessionName}] ON SERVER;
END;";
    }
}
