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
/// <c>sys.fn_xe_file_target_read_file</c> over the <c>&lt;stem&gt;_0_*.xel</c> files next to the file the target is writing
/// now, from a (file name, buffer offset) bookmark kept in collector state (<see cref="BookmarkStateKey"/>), filtered by
/// the same <c>event_time</c> watermark, so a pass reads only the buffers written since the last one and stores no
/// event twice. With no usable bookmark the read is the current file alone. Every read is capped at
/// <see cref="MaxEventsPerPass"/> events, so one pass stays under the host's command timeout (a read the client cancels
/// saves no state and would repeat every interval); a pass that hits the cap keeps its rows and moves the bookmark to the
/// last buffer it scanned, even when every event there was older than the watermark, and the next one continues. Anything
/// that stops the file read (no event_file target, a permission refusal, RDS, an error) drops back to
/// the ring buffer exactly as before, truncation measurement included. The run records <c>event_file_events</c> for a
/// file read that reached the end of what the file holds (a pass the cap stopped does not, #6453), <c>event_file_fallback</c> for the fallback, <c>event_file_error</c> (the SQL Server error number) next to
/// it when an error caused it, and <c>event_file_buffer_cut</c> when one buffer held more than <see cref="BufferReReadFactor"/> times the cap, and <c>event_file_wildcard</c> when the bookmark's file had rolled over and the read opened every rollover file (#6544).</para>
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

    /// <summary>Counts events a pass read from the event_file target (0 on a quiet pass). Written only on a file read the cap did not stop: it says the session was read to the end.</summary>
    internal const string EventFileEventsMeasurement = "event_file_events";

    /// <summary>1 on a pass that read the ring buffer because the event_file target could not be read. Written only then.</summary>
    internal const string EventFileFallbackMeasurement = "event_file_fallback";

    /// <summary>The error number (<c>ERROR_NUMBER()</c>) that stopped the file read, recorded next to <see cref="EventFileFallbackMeasurement"/>. Written only when an error caused the fallback.</summary>
    internal const string EventFileErrorMeasurement = "event_file_error";

    /// <summary>1 on a pass where one buffer held more events than <see cref="BufferReReadFactor"/> times the cap: the pass kept the events it read and the bookmark skips the rest of that buffer. Written only then.</summary>
    internal const string EventFileBufferCutMeasurement = "event_file_buffer_cut";

    /// <summary>1 on a pass that read every <c>&lt;stem&gt;_0_*.xel</c> file because the bookmark's file is no longer the one the target is writing (#6544). A pass whose bookmark is in the current file, or that has no bookmark, reads that one file and writes nothing. Written only then.</summary>
    internal const string EventFileWildcardMeasurement = "event_file_wildcard";

    /// <summary>
    /// The multiple of the cap the single-buffer re-read reads (#6564): <c>TOP (cap x this)</c> raw events, then the cast of
    /// the kept buffer's events. Sized so the worst pass stays under half of Lite's 30 second command timeout (see
    /// <see cref="MaxEventsPerPass"/>): measured on the sql2019 test server, a first read at the cap costs about 3.3 s
    /// when it has to open every file and the re-read adds about 2.4 s to open them again, 0.055 ms a raw row and
    /// 0.3 ms an event cast, so 3.3 + 2.4 + (0.165 + 0.9) x factor seconds: 12.1 s at 6, 16.4 s at the old 10.
    /// </summary>
    internal const int BufferReReadFactor = 6;

    public override IReadOnlyList<string> StateKeys { get; } = new[] { BookmarkStateKey };

    /// <summary>
    /// The bookmark <see cref="ReadAsync"/> left behind: the last buffer it read, plus the newest event time it stored. The time is what
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

    /// <summary>
    /// The most events one pass reads from the event_file target. 3,000 is measured, not guessed. On the sql2019 test
    /// server a pass that starts at the first buffer of a 99 MB file (253,000 events, the largest backlog it can meet) took
    /// 13.6 s for 9,950 events and 9.1 s for 2,999: about 6 s to open the big file plus about 0.7 ms an event, file read and
    /// XML cast together. So 3,000 is the worst case at about 9 s, three times under Lite's 30 second command timeout and
    /// six under Darling's 60 (a file at the session's default 5 MB is about a second). A read that outruns the timeout
    /// is cancelled by the client, which skips the batch's CATCH and saves no state, so the same read would repeat every
    /// interval. A pass that reads this many keeps its bookmark and rows, and the next pass continues from there. A server
    /// writes a few hundred of these events an hour, so the cap only ever matters for a backlog.
    /// </summary>
    public const int MaxEventsPerPass = 3_000;

    /// <summary>The first SQL Server major version (2017) whose read function has the <c>timestamp_utc</c> column.</summary>
    internal const int TimestampUtcMajorVersion = 14;

    /* #6441: sys.fn_xe_file_target_read_file's timestamp_utc column exists on SQL Server 2017+ only, and a literal
       reference to a column the function lacks fails to compile, which takes the whole batch down on 2016. So the two
       versions are two batch texts, chosen in BuildQuery, that differ in exactly this predicate. It is written
       CONVERT(datetime2(7), x.timestamp_utc) on purpose: a plain x.timestamp_utc > @cutoff is pushed down into the
       function, where on sql2019 it returns NO rows at all (measured: 0 of 4,718 events against a cutoff years in the
       past), while the CONVERT form is evaluated on the rows the function returns and gives the right count. The
       predicate drops events older than the cutoff before the XML cast, which is the expensive step.

       Only the read from a bookmark (attempt 1) filters in the WHERE, where the TOP counts only newer events: the
       buffers after a bookmark are mostly newer than the watermark. The current-file read with no bookmark (attempt 2)
       can meet a whole file of older events (a first connect, a lost bookmark), and a filter inside the TOP made it read
       the whole file before it found @max_events newer ones (a 99 MB file took 23 s idle, about double under load). So
       there the time test moves into the CASE that keeps event_data: older events still count toward the cap but keep no
       text, and are deleted before the XML cast. timestamp_utc reads correctly inside a CASE (measured). */
    private const string TimestampUtcWhereSql =
        @"AND   (@attempt = 2 OR CONVERT(datetime2(7), x.timestamp_utc) > @read_cutoff)";

    private const string TimestampUtcCaseSql =
        @"AND CONVERT(datetime2(7), x.timestamp_utc) > @read_cutoff";

    /* One read of the raw events, capped at @read_top in file order BEFORE the XML cast. @keep_file / @keep_offset, when
       set, keep the text of that one buffer's events only (the re-read of a buffer the cap cut, below). */
    private static string RawReadSql(bool hasTimestampUtc) => $@"
        INSERT
            @raw_events
        (
            source_file,
            source_offset,
            event_data
        )
        SELECT TOP (@read_top)
            source_file = x.file_name,
            source_offset = x.file_offset,
            event_data =
                CASE
                    WHEN
                    (
                         @keep_file IS NULL
                      OR
                      (
                           x.file_name = @keep_file
                       AND x.file_offset = @keep_offset
                      )
                    )
                    {(hasTimestampUtc ? TimestampUtcCaseSql : string.Empty)}
                    THEN x.event_data
                END
        FROM sys.fn_xe_file_target_read_file(@read_path, NULL, @read_file, @read_offset) AS x
        WHERE x.object_name IN
              (
                  N'sp_server_diagnostics_component_result',
                  N'error_reported',
                  N'memory_broker_ring_buffer_recorded',
                  N'memory_node_oom_ring_buffer_recorded',
                  N'scheduler_monitor_system_health_ring_buffer_recorded',
                  N'wait_info'
              )
        {(hasTimestampUtc ? TimestampUtcWhereSql : string.Empty)}
        OPTION(RECOMPILE);

        SET @raw_rows = @@ROWCOUNT;";

    /* On-prem / Azure MI / AWS RDS: the built-in system_health session. #6441: the event_file target is read first,
       because the ring buffer's XML is cut at about 4 MB and keeps the OLDEST events (see the class doc); the ring
       buffer is the fallback when the file read cannot happen. The event-name filter keeps the capture to the six
       feeders for the unique warning categories (see the class doc); xml_deadlock_report / security / connectivity
       events are intentionally left behind. evt.query('.') stores each matching <event> node verbatim as text, the
       same normalised text on either source — no shredding here (Stage 2 owns that). Timestamp watermark
       (> @cutoff_time) stops events being collected twice. Azure SQL DB is gated out by AppliesTo, so only
       server-scoped reaches here.

       The file path: the target's current file name gives the directory and the session's file-name stem
       (<stem>_0_<FILETIME>.xel). Attempt 1 reads from the bookmark (@bookmark_file, @bookmark_offset), which skips every
       buffer up to and including the one the last pass ended in. When the bookmark is in the file the target is writing
       it reads that file by its exact path; otherwise (the file rolled over) it reads every <stem>_0_*.xel file after
       the bookmark (#6544, measured: the wildcard costs about 2 s a pass, the exact path about 0.1 s). SQL
       Server refuses a bookmark whose file is gone (error 25717, the operating system's file-not-found) or whose offset is
       not a buffer start (error 25722); SystemHealthEventFileLiveSqlTests proves both live. A refusal moves to
       attempt 2: the current file alone (never every file: a read of every file is what outran the command timeout). A
       refusal of attempt 2 leaves @event_file_ok at 0 and the ring buffer is read.

       Each read is capped at @max_events raw events, taken in file order BEFORE the XML cast (measured: the function
       streams, so a TOP stops it early, but a TOP over the cast rows does not: that plan read the whole 99 MB file in 50
       seconds whatever the TOP). That holds on every version and both attempts. When the cap is hit the last buffer read
       is dropped (the bookmark skips a whole buffer, so a buffer cut in the middle would lose its remaining events), the
       pass keeps what it has, and the next pass continues from the new bookmark. When every capped event is in one
       buffer there is no earlier buffer to keep, so that buffer is read again with BufferReReadFactor (6) times the cap and only its own
       events kept (the stream is in file order, so they come first); a buffer that fills even that is kept as far as it
       was read and recorded as event_file_buffer_cut. A capped read also returns its scan position, the last buffer it
       kept: when every event there is older than the cutoff no row carries it, and the host still moves the bookmark to
       it, or the next pass would read the same old events again and never get past them. A read from the bookmark uses
       the cutoff inclusively (the bookmark's own buffer is skipped by position, so an event that shares the watermark's
       time in a later buffer is not lost); the current-file read keeps it exclusive. */
    private static string BuildQueryText(bool hasTimestampUtc) => $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @system_health TABLE
(
    ring_buffer xml NOT NULL
);

DECLARE
    @raw_events TABLE
(
    source_file nvarchar(260) NOT NULL,
    source_offset bigint NOT NULL,
    event_data nvarchar(max) NULL
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
    @event_file_error integer,
    @bookmark_refused bit = 0,
    @event_file_name nvarchar(260),
    @event_file_stem nvarchar(260),
    @event_file_left nvarchar(260),
    @event_file_path nvarchar(260),
    @event_file_ticks bigint,
    @event_file_last_underscore integer,
    @event_file_base_underscore integer,
    @read_path nvarchar(260),
    @read_file nvarchar(260),
    @read_offset bigint,
    @read_cutoff datetime2(7),
    @read_cap bigint,
    @read_top bigint,
    @raw_rows bigint,
    @last_file nvarchar(260),
    @last_offset bigint,
    @keep_file nvarchar(260),
    @keep_offset bigint,
    @scan_file nvarchar(260),
    @scan_offset bigint,
    @buffer_cut bit = 0,
    @read_wildcard bit = 0,
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
   from that name; a name that does not have the shape leaves @event_file_path NULL and the ring buffer is read.
   The pattern is <stem>_0_*.xel, so another session's files (a user session named system_health_copy) are not read. */
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
        SET @event_file_path = @event_file_left + N'_*.xel';
    END;
END;

/* #6897: Managed Instance refuses every path with a directory in it and the `_0_` pattern (error 40538), so there the
   read always uses the bare pattern the host passes. The target must still name a file, or there is no event_file to read. */
IF  @read_path_override IS NOT NULL
AND @event_file_name IS NOT NULL
BEGIN
    SET @event_file_path = @read_path_override;
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
    /* #6544: the wildcard makes the function open every rollover file (about 2 s for 8 files, 330 MB, on sql2019, whatever
       the number of events returned), and it is only needed when the bookmark's file is not the one being written, so the
       read has to cross into a later file. A bookmark in the current file reads that file by its exact path (about 0.1 s). */
    SET @read_wildcard =
        CASE
            WHEN @attempt = 1
            AND  @bookmark_file COLLATE Latin1_General_100_CI_AS <> @event_file_name COLLATE Latin1_General_100_CI_AS
            THEN 1
            ELSE 0
        END;

    SELECT
        @read_path =
            CASE
                WHEN @read_path_override IS NOT NULL
                THEN @read_path_override
                WHEN @read_wildcard = 1
                THEN @event_file_path
                ELSE @event_file_name
            END,
        @read_file = CASE WHEN @attempt = 1 THEN @bookmark_file END,
        @read_offset = CASE WHEN @attempt = 1 THEN @bookmark_offset END,
        @read_cutoff =
            CASE
                WHEN @attempt = 1
                THEN DATEADD(NANOSECOND, -100, CONVERT(datetime2(7), @cutoff_time))
                ELSE CONVERT(datetime2(7), @cutoff_time)
            END,
        @read_cap = @max_events,
        @read_top = @max_events,
        @keep_file = NULL,
        @keep_offset = NULL,
        @scan_file = NULL,
        @scan_offset = NULL,
        @buffer_cut = 0;

    BEGIN TRY
        DELETE @raw_events;
        DELETE @file_events;

{RawReadSql(hasTimestampUtc)}

        /* The cap cut the read somewhere. Drop the last buffer read, so the bookmark the pass leaves is a buffer whose
           events were all read. */
        IF @raw_rows >= @read_top
        BEGIN
            SELECT TOP (1)
                @last_file = r.source_file,
                @last_offset = r.source_offset
            FROM @raw_events AS r
            ORDER BY
                r.source_file DESC,
                r.source_offset DESC;

            IF EXISTS
               (
                   SELECT
                       1/0
                   FROM @raw_events AS r
                   WHERE r.source_file <> @last_file
                   OR    r.source_offset <> @last_offset
               )
            BEGIN
                DELETE
                    r
                FROM @raw_events AS r
                WHERE r.source_file = @last_file
                AND   r.source_offset = @last_offset;
            END;
            ELSE
            BEGIN
                /* Every capped event is in one buffer, and the bookmark would skip the rest of it. Read it again with
                   {BufferReReadFactor} times the cap, keeping only its events; a buffer that fills even that is kept as far as it was
                   read and recorded. */
                SELECT
                    @keep_file = @last_file,
                    @keep_offset = @last_offset,
                    @read_top = @read_cap * {BufferReReadFactor};

                DELETE @raw_events;

{RawReadSql(hasTimestampUtc)}

                IF  @raw_rows >= @read_top
                AND NOT EXISTS
                    (
                        SELECT
                            1/0
                        FROM @raw_events AS r
                        WHERE r.source_file <> @keep_file
                        OR    r.source_offset <> @keep_offset
                    )
                BEGIN
                    SET @buffer_cut = 1;
                END;

                DELETE
                    r
                FROM @raw_events AS r
                WHERE r.source_file <> @keep_file
                OR    r.source_offset <> @keep_offset;
            END;

            /* The scan position: the last buffer kept, whether or not any of its events is newer than the cutoff. */
            SELECT TOP (1)
                @scan_file = r.source_file,
                @scan_offset = r.source_offset
            FROM @raw_events AS r
            ORDER BY
                r.source_file DESC,
                r.source_offset DESC;
        END;

        /* Events the CASE kept no text for: older than the cutoff, or outside the buffer a re-read keeps. */
        DELETE
            r
        FROM @raw_events AS r
        WHERE r.event_data IS NULL;

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
            source_file = r.source_file,
            source_offset = r.source_offset
        FROM @raw_events AS r
        CROSS APPLY (SELECT event_xml = CONVERT(xml, r.event_data)) AS ed
        CROSS APPLY ed.event_xml.nodes('event') AS q(evt)
        WHERE evt.exist('@timestamp[. > sql:variable(""@read_cutoff"")]') = 1
        OPTION(RECOMPILE);

        SET @event_file_ok = 1;
    END TRY
    BEGIN CATCH
        /* A refused bookmark (attempt 1) goes on to attempt 2; a refused current-file read goes on to the ring buffer.
           25717 and 25722 are the two refusals of a bookmark the server cannot use (its file is gone, its offset is not
           a buffer start); the host drops such a bookmark when nothing replaces it. */
        DELETE @raw_events;
        DELETE @file_events;
        SET @event_file_error = ERROR_NUMBER();

        IF  @attempt = 1
        AND @event_file_error IN (25717, 25722)
        BEGIN
            SET @bookmark_refused = 1;
        END;

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
    FROM @file_events AS fe
    ORDER BY
        fe.source_file,
        fe.source_offset;
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
   source answered, the error that stopped a file read (NULL when none did), whether the bookmark was refused as
   unusable, the scan position of a capped file read (NULL when the cap did not bind), and whether a buffer was cut.
   Read by ReadAsync after the event rows. */
SELECT
    {RingBufferXml.TruncationColumnsSql},
    event_file_read = @event_file_ok,
    event_file_error = CASE WHEN @event_file_ok = 0 THEN @event_file_error END,
    bookmark_refused = @bookmark_refused,
    scan_file = CASE WHEN @event_file_ok = 1 THEN @scan_file END,
    scan_offset = CASE WHEN @event_file_ok = 1 THEN @scan_offset END,
    event_file_buffer_cut = CASE WHEN @event_file_ok = 1 THEN @buffer_cut END,
    event_file_wildcard = CASE WHEN @event_file_ok = 1 THEN @read_wildcard END;";

    private static readonly string s_queryTextXmlTime = BuildQueryText(hasTimestampUtc: false);

    private static readonly string s_queryTextTimestampUtc = BuildQueryText(hasTimestampUtc: true);

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
        var cutoffTime = CutoffOf(context);

        /* #6441: the event_file bookmark, used only while it is not ahead of the stored watermark. */
        var bookmark = TryUseBookmark(context.State, cutoffTime);

        /* #6441: SQL Server 2017+ gets the batch that filters on timestamp_utc before the XML cast. An unknown version (0)
           takes the 2016 batch: naming a column the server lacks fails the whole batch, which is worse than a slower read.
           Managed Instance runs the current engine but reports a low ProductMajorVersion (see
           QueryStoreHealthCollector.HasWaitStatsCaptureMode), so it is matched by its edition. */
        var text = context.Target.SqlMajorVersion >= TimestampUtcMajorVersion || context.Target.IsAzureManagedInstance
            ? s_queryTextTimestampUtc
            : s_queryTextXmlTime;

        return new CollectorQuery(text, new List<CollectorParameter>
        {
            new("@cutoff_time", cutoffTime, CollectorParameterType.DateTime2),
            new("@bookmark_file", bookmark?.File, CollectorParameterType.NVarChar260),
            new("@bookmark_offset", bookmark?.Offset, CollectorParameterType.BigInt),
            new("@max_events", MaxEventsPerPass, CollectorParameterType.Int32),
            new("@read_path_override", EventFileReadPathOverride(context.Target), CollectorParameterType.NVarChar260),
        });
    }

    /// <summary>
    /// The bare file pattern Managed Instance reads the system_health event_file with (#6897).
    /// </summary>
    internal const string ManagedInstanceReadPath = "system_health*.xel";

    /// <summary>
    /// #6897: the read path the batch must use instead of the directory-qualified one, or NULL to keep the usual paths. Azure
    /// SQL Managed Instance refuses any path with a directory in it and the <c>system_health_0_*.xel</c> pattern with error
    /// 40538, and accepts the bare <c>system_health*.xel</c> for the fresh read and for the bookmark resume alike (the
    /// bookmark's <c>file_name</c> goes in unchanged, as the function returned it). The <c>_0_</c> pattern exists so a user
    /// session named like <c>system_health_copy</c> is not matched, and on Managed Instance a user session cannot write an
    /// event_file to a local folder (only to an https URL), so the bare pattern cannot match another session's files there.
    /// </summary>
    internal static string? EventFileReadPathOverride(CollectorTargetInfo target) =>
        target.IsAzureManagedInstance ? ManagedInstanceReadPath : null;

    /// <summary>
    /// The cutoff a pass reads past: the most recent event_time in the host store, or <see cref="CollectorContext.EventFallbackWindow"/>
    /// back on a first run (mirrors DeadlocksCollector). <see cref="ReadAsync"/> stamps it on a bookmark at a scan position
    /// no stored row carries.
    /// </summary>
    private static DateTime CutoffOf(CollectorContext context) =>
        context.Watermark ?? context.CollectionTime - CollectorContext.EventFallbackWindow;

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
        var bookmarkRefused = false;
        var fileRead = false;
        string? scanFile = null;
        long? scanOffset = null;
        string? lastFile = null;
        long? lastOffset = null;
        DateTime? newestTime = null;

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

            /* The bookmark is the LAST buffer the pass read (the greatest file, then offset), carrying the newest time
               the pass stored. The next pass skips every buffer up to it and keeps events at or after that time, so
               an event that shares the newest time in a later buffer is read once, and no stored buffer is read
               again. (The buffer of the newest-by-time row would re-read the buffers after it, and store their
               events that share the newest time a second time.) */
            if (row.SourceFile is not null && row.SourceOffset is not null && row.EventTime is { } time)
            {
                if (lastFile is null
                    || string.CompareOrdinal(row.SourceFile, lastFile) > 0
                    || (string.Equals(row.SourceFile, lastFile, StringComparison.Ordinal) && row.SourceOffset > lastOffset))
                {
                    lastFile = row.SourceFile;
                    lastOffset = row.SourceOffset;
                }

                if (newestTime is null || time > newestTime)
                {
                    newestTime = time;
                }
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
                    fileRead = true;
                }
                else
                {
                    context.Measure(EventFileFallbackMeasurement, 1);

                    /* The error that stopped the file read, next to the fallback it caused. Absent when the read never
                       started (no event_file target, a file name that does not have the shape). */
                    if (reader.FieldCount > 4 && !reader.IsDBNull(4))
                    {
                        context.Measure(EventFileErrorMeasurement, reader.GetInt32(4));
                    }
                }
            }

            bookmarkRefused = reader.FieldCount > 5 && !reader.IsDBNull(5) && reader.GetBoolean(5);

            /* The scan position of a capped file read: the last buffer it kept, NULL when the cap did not bind. */
            if (reader.FieldCount > 7 && !reader.IsDBNull(6) && !reader.IsDBNull(7))
            {
                scanFile = reader.GetString(6);
                scanOffset = reader.GetInt64(7);
            }

            /* #6453: event_file_events says the session was read to the end, so a pass that stopped at the cap does not write it: its
               stored rows are only the oldest events past the watermark (on a first connect, possibly none of them inside the
               cutoff), and a reader that took 0 of them for "nothing happened" would answer empty over events it never reached. The
               scan position is non-NULL only when the cap bound. */
            if (fileRead && scanFile is null)
            {
                context.Measure(EventFileEventsMeasurement, rows.Count);
            }

            if (reader.FieldCount > 8 && !reader.IsDBNull(8) && reader.GetBoolean(8))
            {
                context.Measure(EventFileBufferCutMeasurement, 1);
            }

            if (reader.FieldCount > 9 && !reader.IsDBNull(9) && reader.GetBoolean(9))
            {
                context.Measure(EventFileWildcardMeasurement, 1);
            }
        }

        /* Resume the next pass at the last buffer this pass read. A pass that read nothing new keeps the
           bookmark it started from; the next pass starts past it all the same. The exception is a bookmark the server
           refused as unusable (its file is gone, its offset is not a buffer start: 25717 / 25722, proved live) when
           nothing replaced it: it is dropped, so the next pass does not try a dead bookmark first for ever. An empty
           value reads back as no bookmark (TryUseBookmark).

           A capped read whose last buffers held only events older than the cutoff (or that returned no row at all) moves
           the bookmark to its scan position instead, or the next pass would read the same old events and stall there. The
           time it carries is the newest stored, or the cutoff when nothing was: every event it skips is at or before the
           cutoff, so TryUseBookmark accepts it next pass and nothing newer is skipped. */
        if (scanFile is not null
            && scanOffset is { } scanAt
            && (lastFile is null
                || string.CompareOrdinal(scanFile, lastFile) > 0
                || (string.Equals(scanFile, lastFile, StringComparison.Ordinal) && scanAt > lastOffset)))
        {
            context.PendingState[BookmarkStateKey] = FormatBookmark(newestTime ?? CutoffOf(context), scanAt, scanFile);
        }
        else if (newestTime is { } bookmarkTime && lastFile is not null && lastOffset is { } bookmarkOffset)
        {
            context.PendingState[BookmarkStateKey] = FormatBookmark(bookmarkTime, bookmarkOffset, lastFile);
        }
        else if (bookmarkRefused)
        {
            context.PendingState[BookmarkStateKey] = string.Empty;
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
