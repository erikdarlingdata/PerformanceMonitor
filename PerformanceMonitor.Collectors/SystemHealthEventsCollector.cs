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

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Raw <c>system_health</c> Extended Events, captured from the built-in always-on session's
/// ring-buffer target and landed as one row of raw event XML each — the STAGE 1 capture half of the
/// system_health parity port (the Dashboard already shreds these via Erik's
/// <c>sp_HealthParser</c>). This collector ONLY captures and stores; the monitor-side shred into the
/// health-parser warning categories (memory broker / memory conditions / memory node-OOM / scheduler
/// issues / severe errors / significant waits) is STAGE 2 and owns all parsing — nothing here is
/// shredded into typed columns.
///
/// <para>Reads the <c>system_health</c> ring buffer the same way <see cref="DeadlocksCollector"/>
/// reads its own session: <c>TRY_CAST(target_data AS xml)</c> from sys.dm_xe_session_targets +
/// sys.dm_xe_sessions, <c>.nodes()</c> to split events, an <c>event_time</c> watermark so the rolling
/// ring buffer is never re-grabbed (10-minute fallback on first run), storing the raw
/// <c>evt.query('.')</c> event node verbatim. Unlike the deadlock collector there is NO session
/// lifecycle: <c>system_health</c> is created and started by the Database Engine and must not be
/// altered (MS Learn), so the host just reads it.</para>
///
/// <para><b>Events captured</b> (verified live on SQL 2016–2025 and against
/// <c>sp_HealthParser</c>'s event map): the clear feeders for the unique warning categories —
/// <c>sp_server_diagnostics_component_result</c> (memory conditions, IO, and system component health),
/// <c>error_reported</c> (severe errors), <c>memory_broker_ring_buffer_recorded</c> (memory broker),
/// <c>memory_node_oom_ring_buffer_recorded</c> (memory node OOM),
/// <c>scheduler_monitor_system_health_ring_buffer_recorded</c> (scheduler / non-yielding issues), and
/// <c>wait_info</c> (significant individual long waits — discrete threshold-crossing events, NOT the
/// cumulative sys.dm_os_wait_stats the wait_stats collector aggregates). <c>xml_deadlock_report</c> is
/// deliberately NOT captured here — <see cref="DeadlocksCollector"/> already owns deadlocks from its
/// own dedicated session. <c>security_error_ring_buffer_recorded</c> and
/// <c>connectivity_ring_buffer_recorded</c> are also skipped (outside the warning categories this
/// feeds, and excluded by sp_HealthParser too).</para>
///
/// <para><b>Azure SQL Database (edition 5): does NOT apply.</b> MS Learn is explicit — "There's no
/// built-in <c>system_health</c> Extended Event session in Azure SQL Database" (the session's "Applies
/// to" lists SQL Server and Azure SQL Managed Instance only), and the captured events
/// (sp_server_diagnostics, the memory-broker / node-OOM / scheduler ring buffers) are server-level and
/// have no database-scoped equivalent to recreate — so unlike the deadlock collector (which has the
/// database-scoped <c>database_xml_deadlock_report</c> to stand up its own session) there is nothing to
/// read or build on Azure SQL DB. Erik's own <c>sp_HealthParser</c> hard-refuses on edition 5 for the
/// same reason. A verified platform gap, gated via <see cref="AppliesTo"/> = <c>!IsAzureSqlDb</c>.
/// Azure SQL Managed Instance (edition 8) has the full server-scoped session and collects normally.</para>
/// </summary>
public sealed class SystemHealthEventsCollector : CollectorDefinitionBase<SystemHealthEventsCollector.Row>
{
    public static SystemHealthEventsCollector Instance { get; } = new();

    private SystemHealthEventsCollector()
    {
    }

    /// <summary>The built-in always-on session name — fixed by the Database Engine, never created by us.</summary>
    public const string XeSessionName = "system_health";

    public sealed class Row
    {
        public DateTime? EventTime { get; set; }
        public string? EventType { get; set; }
        public string? EventXml { get; set; }
    }

    /* On-prem / Azure MI / AWS RDS: read from the built-in system_health server-scoped ring buffer.
       The event-name filter keeps the capture to the six feeders for the unique warning categories
       (see the class doc); xml_deadlock_report / security / connectivity events are intentionally left
       behind. evt.query('.') stores each matching <event> node verbatim as text — no shredding here
       (Stage 2 owns that). Timestamp watermark (> @cutoff_time) stops the rolling ring buffer being
       re-collected. Azure SQL DB is gated out by AppliesTo, so only server-scoped reaches here. */
    private const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @system_health TABLE
(
    ring_buffer xml NOT NULL
);

INSERT
    @system_health
(
    ring_buffer
)
SELECT /* PerformanceMonitorLite */
    ring_xml = TRY_CAST(xet.target_data AS xml)
FROM sys.dm_xe_session_targets AS xet
JOIN sys.dm_xe_sessions AS xes
  ON xes.address = xet.event_session_address
WHERE xes.name = N'system_health'
AND   xet.target_name = N'ring_buffer'
OPTION(RECOMPILE);

SELECT
    event_time = evt.value('(@timestamp)[1]', 'datetime2'),
    event_type = evt.value('(@name)[1]', 'sysname'),
    event_xml = CONVERT(nvarchar(max), evt.query('.'))
FROM
(
    SELECT
        sh.ring_buffer
    FROM @system_health AS sh
) AS rb
CROSS APPLY rb.ring_buffer.nodes('RingBufferTarget/event') AS q(evt)
WHERE evt.value('(@timestamp)[1]', 'datetime2') > @cutoff_time
AND   evt.value('(@name)[1]', 'sysname') IN
      (
          N'sp_server_diagnostics_component_result',
          N'error_reported',
          N'memory_broker_ring_buffer_recorded',
          N'memory_node_oom_ring_buffer_recorded',
          N'scheduler_monitor_system_health_ring_buffer_recorded',
          N'wait_info'
      )
OPTION(RECOMPILE);";

    public override string Name => "system_health_events";

    public override string TargetTable => "system_health_events";

    /// <summary>Lite schema names this table's prefix id "system_health_event_id"; Darling mirrors it.</summary>
    public override string PrefixIdColumnName => "system_health_event_id";

    /// <summary>
    /// Only events newer than the newest already-collected event are fetched, so an event lingering
    /// in the rolling ring buffer across cycles is never inserted twice (mirrors the deadlock
    /// collector's deadlock_time watermark).
    /// </summary>
    public override string? WatermarkColumn => "event_time";

    /// <summary>#4197 part b: lets the host cache this collector's server-scoped watermark in memory.</summary>
    public override Func<Row, DateTime?>? WatermarkValueAccessor => static row => row.EventTime;

    /// <summary>
    /// Collects on SQL Server, Azure SQL Managed Instance, and AWS RDS — everywhere the built-in
    /// server-scoped system_health session exists. NOT Azure SQL Database (edition 5): there is no
    /// built-in system_health session there (MS Learn-verified) and the captured events are
    /// server-level with no database-scoped equivalent to stand up — a real platform gap, not a
    /// scope-down. See the class remarks.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => !target.IsAzureSqlDb;

    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        /* Use the most recent event_time from the host store as the cutoff, or fall back to a
           10-minute window on first run (mirrors DeadlocksCollector). */
        var cutoffTime = context.Watermark ?? context.CollectionTime.AddMinutes(-10);

        return new CollectorQuery(QueryText, new List<CollectorParameter>
        {
            new("@cutoff_time", cutoffTime, CollectorParameterType.DateTime2),
        });
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("event_time", CollectorColumnType.Timestamp),
        new CollectorColumn("event_type", CollectorColumnType.Varchar),
        new CollectorColumn("event_xml", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row
            {
                EventTime = reader.IsDBNull(0) ? null : reader.GetDateTime(0),
                EventType = reader.IsDBNull(1) ? null : reader.GetString(1),
                EventXml = reader.IsDBNull(2) ? null : reader.GetString(2),
            });
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.EventTime)   /* event_time TIMESTAMP (watermark) */
            .Value(row.EventType)   /* event_type VARCHAR (XE event name — Stage 2 dispatches on this) */
            .Value(row.EventXml);   /* event_xml VARCHAR (raw event node, unshredded) */
    }
}
