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
/// ring buffer is never re-grabbed (CollectorContext.EventFallbackWindow on first run), storing the raw
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
///
/// <para><b>Event file first, ring buffer as the fallback (#6441).</b> SQL Server cuts the ring buffer's XML at about
/// 4 MB and keeps the OLDEST events, so on a busy server a watermarked ring-buffer read stores nothing while new events
/// pile up unread. The session's <c>event_file</c> target has no such cap, so the read goes there first:
/// <c>sys.fn_xe_file_target_read_file</c> over the <c>system_health*.xel</c> files next to the file the target is writing
/// now, from a (file name, buffer offset) bookmark kept in collector state (<see cref="BookmarkStateKey"/>), filtered by
/// the same <c>event_time</c> watermark, so a pass reads only the buffers written since the last one and stores no
/// event twice. With no usable bookmark the read starts at the current file when the watermark falls inside it, or at
/// every file when the watermark is older than the current file. Anything that stops the file read (no event_file
/// target, a permission refusal, Managed Instance or RDS, an error) drops back to the ring buffer exactly as before,
/// truncation measurement included. The run records <c>event_file_events</c> for a file read and
/// <c>event_file_fallback</c> for the fallback.</para>
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

        /// <summary>#6441: the event_file the event was read from (the full path SQL Server returns). NULL for a ring-buffer row. Never stored.</summary>
        public string? SourceFile { get; set; }

        /// <summary>#6441: the byte offset of the buffer in <see cref="SourceFile"/> that held the event. NULL for a ring-buffer row. Never stored.</summary>
        public long? SourceOffset { get; set; }
    }

    /// <summary>The state key of the event_file bookmark: <c>&lt;event_time round-trip&gt;|&lt;buffer offset&gt;|&lt;file path&gt;</c>.</summary>
    public const string BookmarkStateKey = "sh_event_file_bookmark";

    /// <summary>Counts events a pass read from the event_file target (0 on a quiet pass). Written only on a file read.</summary>
    internal const string EventFileEventsMeasurement = "event_file_events";

    /// <summary>1 on a pass that read the ring buffer because the event_file target could not be read. Written only then.</summary>
    internal const string EventFileFallbackMeasurement = "event_file_fallback";

    public override IReadOnlyList<string> StateKeys { get; } = new[] { BookmarkStateKey };

    /// <summary>
    /// The bookmark <see cref="ReadAsync"/> left behind: where the newest stored event sits, plus its time. The time is what
    /// <see cref="TryUseBookmark"/> checks against the stored watermark, so a bookmark that got ahead of the store (a failed
    /// write, a purged store) is never trusted. Public for the host-side round-trip tests.
    /// </summary>
    public static string FormatBookmark(DateTime eventTime, long offset, string file) =>
        string.Concat(
            eventTime.ToString("o", CultureInfo.InvariantCulture),
            "|",
            offset.ToString(CultureInfo.InvariantCulture),
            "|",
            file);

    /// <summary>
    /// Reads <paramref name="state"/>'s bookmark and returns it only when it is safe to resume from: it parses, and its event
    /// time is not later than <paramref name="cutoffTime"/> (the stored watermark), compared at the microsecond the stores keep.
    /// A bookmark later than the watermark means the store lost events the bookmark had already skipped, and the read then
    /// starts without it.
    /// </summary>
    public static (string File, long Offset)? TryUseBookmark(
        IReadOnlyDictionary<string, string> state, DateTime cutoffTime)
    {
        if (!state.TryGetValue(BookmarkStateKey, out var raw) || string.IsNullOrEmpty(raw))
        {
            return null;
        }

        var parts = raw.Split('|', 3);
        if (parts.Length != 3
            || !DateTime.TryParse(parts[0], CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var eventTime)
            || !long.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var offset)
            || string.IsNullOrWhiteSpace(parts[2]))
        {
            return null;
        }

        var storedPrecision = eventTime.Ticks - (eventTime.Ticks % 10);
        return storedPrecision <= cutoffTime.Ticks ? (parts[2], offset) : null;
    }

    /* On-prem / Azure MI / AWS RDS: the built-in system_health session. #6441: the event_file target is read first,
       because the ring buffer's XML is cut at about 4 MB and keeps the OLDEST events (see the class doc); the ring
       buffer is the fallback when the file read cannot happen. The event-name filter keeps the capture to the six
       feeders for the unique warning categories (see the class doc); xml_deadlock_report / security / connectivity
       events are intentionally left behind. evt.query('.') stores each matching <event> node verbatim as text, the
       same normalised text on either source — no shredding here (Stage 2 owns that). Timestamp watermark
       (> @cutoff_time) stops events being collected twice. Azure SQL DB is gated out by AppliesTo, so only
       server-scoped reaches here.

       The file path: the target's current file name gives the directory and the session's file-name stem; the
       rollover timestamp in the name (a FILETIME, ticks since 1601) gives when that file was created. Attempt 1 reads
       from the bookmark (@bookmark_file, @bookmark_offset), which skips every buffer up to and including the one the
       last pass ended in; SQL Server refuses a bookmark whose file is gone or whose offset is not a buffer start
       (error 25722), and that moves to attempt 2: the current file alone when @cutoff_time falls inside it, every
       system_health file otherwise. A refusal of attempt 2 leaves @event_file_ok at 0 and the ring buffer is read.
       sys.fn_xe_file_target_read_file's timestamp_utc column is SQL Server 2017+, so the event time comes from the
       event XML, which also matches the ring-buffer path. */
    private const string QueryText = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @system_health TABLE
(
    ring_buffer xml NOT NULL
);

DECLARE
    @file_events TABLE
(
    event_time datetime2(7) NOT NULL,
    event_type sysname NOT NULL,
    event_xml nvarchar(max) NOT NULL,
    source_file nvarchar(260) NOT NULL,
    source_offset bigint NOT NULL
);

DECLARE
    @event_file_ok bit = 0,
    @event_file_name nvarchar(260),
    @event_file_stem nvarchar(260),
    @event_file_left nvarchar(260),
    @event_file_path nvarchar(260),
    @event_file_created datetime2(0),
    @event_file_ticks bigint,
    @event_file_last_underscore integer,
    @event_file_base_underscore integer,
    @read_path nvarchar(260),
    @read_file nvarchar(260),
    @read_offset bigint,
    @attempt integer;

{RingBufferXml.DeclareSql}

SELECT
    @event_file_name = TRY_CAST(xet.target_data AS xml).value('(EventFileTarget/File/@name)[1]', 'nvarchar(260)')
FROM sys.dm_xe_session_targets AS xet
JOIN sys.dm_xe_sessions AS xes
  ON xes.address = xet.event_session_address
WHERE xes.name = N'system_health'
AND   xet.target_name = N'event_file'
OPTION(RECOMPILE);

/* The target names the file it is writing, <directory>system_health_0_<FILETIME>.xel. Everything the read needs comes
   from that name; a name that does not have the shape leaves @event_file_path NULL and the ring buffer is read. */
IF  @event_file_name LIKE N'%.xel'
AND CHARINDEX(N'_', @event_file_name) > 0
BEGIN
    SET @event_file_stem = LEFT(@event_file_name, LEN(@event_file_name) - 4);

    SET @event_file_last_underscore =
        CASE
            WHEN CHARINDEX(N'_', @event_file_stem) > 0
            THEN LEN(@event_file_stem) - CHARINDEX(N'_', REVERSE(@event_file_stem)) + 1
        END;

    SET @event_file_left = LEFT(@event_file_stem, @event_file_last_underscore - 1);

    SET @event_file_base_underscore =
        CASE
            WHEN CHARINDEX(N'_', @event_file_left) > 0
            THEN LEN(@event_file_left) - CHARINDEX(N'_', REVERSE(@event_file_left)) + 1
        END;

    SET @event_file_ticks = TRY_CONVERT(bigint, SUBSTRING(@event_file_stem, @event_file_last_underscore + 1, 20));

    IF  @event_file_base_underscore > 1
    AND @event_file_ticks > 0
    BEGIN
        SET @event_file_path = LEFT(@event_file_stem, @event_file_base_underscore - 1) + N'*.xel';

        SET @event_file_created =
            DATEADD
            (
                SECOND,
                CONVERT(integer, (@event_file_ticks % 864000000000) / 10000000),
                DATEADD(DAY, CONVERT(integer, @event_file_ticks / 864000000000), CONVERT(datetime2(0), N'16010101'))
            );
    END;
END;

SET @attempt =
    CASE
        WHEN @bookmark_file IS NOT NULL
        AND  @bookmark_offset IS NOT NULL
        THEN 1
        ELSE 2
    END;

WHILE  @event_file_path IS NOT NULL
AND    @event_file_ok = 0
AND    @attempt <= 2
BEGIN
    SELECT
        @read_path =
            CASE
                WHEN @attempt = 2
                AND  @event_file_created <= @cutoff_time
                THEN @event_file_name
                ELSE @event_file_path
            END,
        @read_file = CASE WHEN @attempt = 1 THEN @bookmark_file END,
        @read_offset = CASE WHEN @attempt = 1 THEN @bookmark_offset END;

    BEGIN TRY
        DELETE @file_events;

        INSERT
            @file_events
        (
            event_time,
            event_type,
            event_xml,
            source_file,
            source_offset
        )
        SELECT
            event_time = evt.value('(@timestamp)[1]', 'datetime2(7)'),
            event_type = evt.value('(@name)[1]', 'sysname'),
            event_xml = CONVERT(nvarchar(max), evt.query('.')),
            source_file = x.file_name,
            source_offset = x.file_offset
        FROM sys.fn_xe_file_target_read_file(@read_path, NULL, @read_file, @read_offset) AS x
        CROSS APPLY (SELECT event_xml = CONVERT(xml, x.event_data)) AS ed
        CROSS APPLY ed.event_xml.nodes('event') AS q(evt)
        WHERE x.object_name IN
              (
                  N'sp_server_diagnostics_component_result',
                  N'error_reported',
                  N'memory_broker_ring_buffer_recorded',
                  N'memory_node_oom_ring_buffer_recorded',
                  N'scheduler_monitor_system_health_ring_buffer_recorded',
                  N'wait_info'
              )
        AND   evt.exist('@timestamp[. > sql:variable(""@cutoff_time"")]') = 1
        OPTION(RECOMPILE);

        SET @event_file_ok = 1;
    END TRY
    BEGIN CATCH
        /* A refused bookmark (attempt 1) goes on to attempt 2; a refused full read goes on to the ring buffer. */
        DELETE @file_events;
        SET @attempt += 1;
    END CATCH;
END;

SELECT /* PerformanceMonitorLite */
    @ring_text = CONVERT(nvarchar(max), xet.target_data),
    @ring_session = xes.name
FROM sys.dm_xe_session_targets AS xet
JOIN sys.dm_xe_sessions AS xes
  ON xes.address = xet.event_session_address
WHERE xes.name = N'system_health'
AND   xet.target_name = N'ring_buffer'
AND   @event_file_ok = 0
OPTION(RECOMPILE);

{RingBufferXml.ParseSql}

{RingBufferXml.TruncationSql}

INSERT
    @system_health
(
    ring_buffer
)
SELECT
    ring_xml = @ring_xml
WHERE @ring_xml IS NOT NULL;

IF @event_file_ok = 1
BEGIN
    SELECT
        fe.event_time,
        fe.event_type,
        fe.event_xml,
        fe.source_file,
        fe.source_offset
    FROM @file_events AS fe;
END;
ELSE
BEGIN
    SELECT
        event_time = evt.value('(@timestamp)[1]', 'datetime2'),
        event_type = evt.value('(@name)[1]', 'sysname'),
        event_xml = CONVERT(nvarchar(max), evt.query('.')),
        source_file = CONVERT(nvarchar(260), NULL),
        source_offset = CONVERT(bigint, NULL)
    FROM
    (
        SELECT
            sh.ring_buffer
        FROM @system_health AS sh
    ) AS rb
    CROSS APPLY rb.ring_buffer.nodes('RingBufferTarget/event') AS q(evt)
    WHERE evt.exist('@timestamp[. > sql:variable(""@cutoff_time"")]') = 1
    AND   evt.value('(@name)[1]', 'sysname') IN
          (
              N'sp_server_diagnostics_component_result',
              N'error_reported',
              N'memory_broker_ring_buffer_recorded',
              N'memory_node_oom_ring_buffer_recorded',
              N'scheduler_monitor_system_health_ring_buffer_recorded',
              N'wait_info'
          )
    OPTION(RECOMPILE);
END;

/* #6441: whether SQL Server truncated the buffer's XML (ring-buffer path only, all NULL on the file path), then which
   source answered. Read by ReadAsync after the event rows. */
SELECT
    {RingBufferXml.TruncationColumnsSql},
    event_file_read = @event_file_ok;";

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
        /* Use the most recent event_time from the host store as the cutoff, or fall back to
           CollectorContext.EventFallbackWindow on first run (mirrors DeadlocksCollector). */
        var cutoffTime = context.Watermark ?? context.CollectionTime - CollectorContext.EventFallbackWindow;

        /* #6441: the event_file bookmark, used only while it is not ahead of the stored watermark. */
        var bookmark = TryUseBookmark(context.State, cutoffTime);

        return new CollectorQuery(QueryText, new List<CollectorParameter>
        {
            new("@cutoff_time", cutoffTime, CollectorParameterType.DateTime2),
            new("@bookmark_file", bookmark?.File, CollectorParameterType.NVarChar260),
            new("@bookmark_offset", bookmark?.Offset, CollectorParameterType.BigInt),
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
        /* #4348: the event node goes through the statement filter's XML walk where it first enters a row. A named
           value (the sql_text action's statement, an error message that echoes one) becomes the placeholder in
           place and the rest of the event keeps its shape, so the stage-2 parse still reads the other fields. */
        var scrub = context.BeginStatementScrub();

        var hasSource = reader.FieldCount >= 5;
        Row? newest = null;

        while (await reader.ReadAsync(cancellationToken))
        {
            var row = new Row
            {
                EventTime = reader.IsDBNull(0) ? null : reader.GetDateTime(0),
                EventType = reader.IsDBNull(1) ? null : reader.GetString(1),
                EventXml = reader.IsDBNull(2) ? null : scrub.Xml(reader.GetString(2)),
                SourceFile = hasSource && !reader.IsDBNull(3) ? reader.GetString(3) : null,
                SourceOffset = hasSource && !reader.IsDBNull(4) ? reader.GetInt64(4) : null,
            };
            rows.Add(row);

            /* Ties keep the later row, which sits at or after the earlier one in the file. */
            if (row.SourceFile is not null && row.SourceOffset is not null && row.EventTime is { } time
                && (newest is null || time >= newest.EventTime))
            {
                newest = row;
            }
        }

        /* #6441: SQL Server cuts the ring buffer's XML at about 4 MB and keeps the OLDEST events, so on a busy server
           a ring-buffer read can store 0 rows while new events exist. The counts are what bands the collector WARNING;
           they stay NULL (not truncated) when the event_file target answered. The trailing row also says which source
           answered. */
        if (await reader.NextResultAsync(cancellationToken) && await reader.ReadAsync(cancellationToken))
        {
            RingBufferXml.MeasureTruncation(reader, 0, context);

            if (reader.FieldCount > 3 && !reader.IsDBNull(3))
            {
                if (reader.GetBoolean(3))
                {
                    context.Measure(EventFileEventsMeasurement, rows.Count);
                }
                else
                {
                    context.Measure(EventFileFallbackMeasurement, 1);
                }
            }
        }

        /* Resume the next pass at the buffer the newest stored event sat in. A pass that read nothing new keeps the
           bookmark it started from; the next pass starts past it all the same. */
        if (newest is { EventTime: { } newestTime, SourceFile: { } file, SourceOffset: { } offset })
        {
            context.PendingState[BookmarkStateKey] = FormatBookmark(newestTime, offset, file);
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
