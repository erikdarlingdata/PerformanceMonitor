/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The one T-SQL fragment every ring-buffer collector reads its target through (#5948). SQL Server writes the
/// <c>objectname</c> and <c>indexname</c> attributes of a <c>keylock</c> in an <c>xml_deadlock_report</c> graph WITHOUT escaping
/// <c>&amp;</c>, so one deadlock on a table or index named <c>a&amp;x</c> makes the session's whole ring buffer unparseable. The
/// plain <c>TRY_CAST(target_data AS xml)</c> then gave NULL, the insert into a <c>ring_buffer xml NOT NULL</c> table variable
/// failed, the shred gate's event count was never saved, and every later run failed the same way until the session restarted.
///
/// <para>The rule, in <see cref="ParseSql"/>: cast plainly; when that gives NULL, protect the five named entities
/// (<c>&amp;amp; &amp;lt; &amp;gt; &amp;quot; &amp;apos;</c>) with placeholders, replace every other <c>&amp;</c> with
/// <c>&amp;amp;</c>, restore the five, and cast again. A numeric character reference (<c>&amp;#...;</c>) becomes literal text
/// under this pass: that changes the text but not the parse. A buffer that still will not cast raises an error carrying its
/// character count and SQL Server's own parse message, instead of inserting a NULL.</para>
///
/// <para>Usage in a collector batch: put <see cref="DeclareSql"/> where the read runs, read
/// <c>@ring_text = CONVERT(nvarchar(max), xet.target_data)</c> and <c>@ring_session = xes.name</c> from the target, emit
/// <see cref="ParseSql"/>, then insert <c>@ring_xml WHERE @ring_xml IS NOT NULL</c>. Neither constant holds a brace, so both
/// are safe in an interpolated string and in a <c>string.Format</c>-style template.</para>
/// </summary>
internal static class RingBufferXml
{
    /// <summary>The variables <see cref="ParseSql"/> reads (<c>@ring_text</c>, <c>@ring_session</c>) and the one it fills (<c>@ring_xml</c>).</summary>
    public const string DeclareSql = @"DECLARE
    @ring_text nvarchar(max),
    @ring_session sysname,
    @ring_xml xml;";

    /// <summary>
    /// Parses <c>@ring_text</c> into <c>@ring_xml</c>, escaping stray ampersands when the plain cast fails, and throws when
    /// the text still is not XML. <c>@ring_xml</c> stays NULL only when <c>@ring_text</c> is NULL (no such session target).
    /// </summary>
    public const string ParseSql = @"DECLARE
    @ring_characters bigint = LEN(@ring_text),
    @ring_parse_error nvarchar(1000),
    @ring_message nvarchar(2048);

SET @ring_xml = TRY_CAST(@ring_text AS xml);

/* #5948: SQL Server writes the objectname and indexname of a keylock in a deadlock graph without escaping
   an ampersand, so a table or index named a&x makes the whole buffer unparseable. Protect the five named entities,
   escape every other ampersand, restore the five, and cast again. A numeric character reference (&#...;) becomes
   literal text here, which changes the text but not the parse. */
IF  @ring_xml IS NULL
AND @ring_text IS NOT NULL
BEGIN
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'&amp;', N'@@pm5948amp@@');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'&lt;', N'@@pm5948lt@@');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'&gt;', N'@@pm5948gt@@');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'&quot;', N'@@pm5948quot@@');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'&apos;', N'@@pm5948apos@@');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'&', N'&amp;');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'@@pm5948amp@@', N'&amp;');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'@@pm5948lt@@', N'&lt;');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'@@pm5948gt@@', N'&gt;');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'@@pm5948quot@@', N'&quot;');
    SET @ring_text = REPLACE(@ring_text COLLATE Latin1_General_BIN2, N'@@pm5948apos@@', N'&apos;');

    SET @ring_xml = TRY_CAST(@ring_text AS xml);

    IF @ring_xml IS NULL
    BEGIN
        /* TRY_CAST hides the reason, so ask for it once more with a plain CONVERT inside a TRY block. */
        BEGIN TRY
            SET @ring_xml = CONVERT(xml, @ring_text);
        END TRY
        BEGIN CATCH
            SET @ring_parse_error = LEFT(ERROR_MESSAGE(), 1000);
        END CATCH;

        IF @ring_xml IS NULL
        BEGIN
            SET @ring_message =
                CONCAT
                (
                    N'The ',
                    @ring_session,
                    N' ring buffer (',
                    @ring_characters,
                    N' characters) is not XML SQL Server can parse: ',
                    COALESCE(@ring_parse_error, N'no parse error was returned'),
                    N' No event can be read from it until the buffer clears; restarting the session clears it.'
                );

            THROW 50000, @ring_message, 1;
        END;
    END;
END;";

    /// <summary>
    /// #6441. Reads the truncation SQL Server writes on the parsed buffer, right after <see cref="ParseSql"/>, in the same
    /// batch and from the same <c>@ring_xml</c>, so the target is read once. SQL Server cuts the target's XML at about 4 MB,
    /// marks <c>RingBufferTarget</c> <c>truncated="1"</c>, and keeps the OLDEST events: a watermarked read of a busy session
    /// then stores 0 rows and records SUCCESS while new events pile up out of sight. Only a truncated buffer pays for the
    /// extra pass over its events (a count and the newest timestamp); a whole buffer costs one <c>exist()</c>.
    /// Holds no brace, like the constants above.
    /// </summary>
    public const string TruncationSql = @"DECLARE
    @ring_truncated bit,
    @ring_event_count bigint,
    @ring_events_visible bigint,
    @ring_newest_visible datetime2(7);

IF @ring_xml.exist('RingBufferTarget[@truncated = 1]') = 1
BEGIN
    SELECT
        @ring_truncated = 1,
        @ring_event_count = @ring_xml.value('(RingBufferTarget/@eventCount)[1]', 'bigint');

    SELECT
        @ring_events_visible = COUNT_BIG(*),
        @ring_newest_visible = MAX(rbe.evt.value('(@timestamp)[1]', 'datetime2(7)'))
    FROM @ring_xml.nodes('RingBufferTarget/event') AS rbe(evt);
END;";

    /// <summary>
    /// The three columns <see cref="MeasureTruncation"/> reads, as a select-list fragment (no leading comma). Safe where
    /// <see cref="TruncationSql"/> never ran (a gated branch): every value is NULL then and reads as not truncated.
    /// </summary>
    public const string TruncationColumnsSql = @"ring_truncated = CONVERT(bit, ISNULL(@ring_truncated, 0)),
    ring_events_hidden =
        CASE
            WHEN @ring_event_count > ISNULL(@ring_events_visible, 0)
            THEN @ring_event_count - ISNULL(@ring_events_visible, 0)
            ELSE CONVERT(bigint, 0)
        END,
    newest_visible_event_age_s =
        CASE
            WHEN @ring_newest_visible IS NULL THEN CONVERT(bigint, NULL)
            ELSE CONVERT(bigint, DATEDIFF(SECOND, @ring_newest_visible, SYSUTCDATETIME()))
        END";

    /// <summary>A trailing one-row result carrying <see cref="TruncationColumnsSql"/>, for a collector with no gate row to add them to.</summary>
    public const string TruncationSelectSql = @"SELECT
    " + TruncationColumnsSql + ";";

    /// <summary>
    /// #8146. Declares what the ring-buffer position reads fill: the target header (<c>@ring_total</c>, <c>@ring_held</c>), the
    /// session row (<c>@ring_created</c>, <c>@ring_dropped</c>) and the session's <c>max_events_limit</c> (<c>@ring_limit</c>).
    /// The collector assigns <c>@ring_position_session</c> from its session-name literal before the session fragments run.
    /// Holds no brace.
    /// </summary>
    public const string PositionDeclareSql = @"DECLARE
    @ring_total bigint,
    @ring_held bigint,
    @ring_created datetime2(7),
    @ring_dropped bigint,
    @ring_limit bigint,
    @ring_position_session sysname;";

    /// <summary>
    /// #8146. Reads the target header off the parsed <c>@ring_xml</c>, right after <see cref="TruncationSql"/>: how many events the
    /// target has ever taken in (<c>totalEventsProcessed</c>) and how many it holds now (<c>eventCount</c>). A NULL
    /// <c>@ring_xml</c> (no such target) leaves both NULL. Needs <see cref="PositionDeclareSql"/>.
    /// </summary>
    public const string HeaderSql = @"SELECT
    @ring_total = @ring_xml.value('(RingBufferTarget/@totalEventsProcessed)[1]', 'bigint'),
    @ring_held = @ring_xml.value('(RingBufferTarget/@eventCount)[1]', 'bigint');";

    /// <summary>
    /// #8146. The session row for a server-scoped session named <c>@ring_position_session</c>: when it was created (a restart
    /// moves it), how many events it dropped before any target took them, and the ring_buffer target's
    /// <c>max_events_limit</c> (1000 when the session carries no such field). Runs on every run, gated or not. Needs
    /// <see cref="PositionDeclareSql"/>.
    /// </summary>
    public const string SessionPositionServerSql = @"SELECT
    @ring_created = xes.create_time,
    @ring_dropped = xes.dropped_event_count
FROM sys.dm_xe_sessions AS xes
WHERE xes.name = @ring_position_session
OPTION(RECOMPILE);

SET @ring_limit = 1000;

SELECT
    @ring_limit = ISNULL(TRY_CONVERT(bigint, sesf.value), 1000)
FROM sys.server_event_sessions AS ses
JOIN sys.server_event_session_targets AS sest
  ON sest.event_session_id = ses.event_session_id
JOIN sys.server_event_session_fields AS sesf
  ON  sesf.event_session_id = sest.event_session_id
  AND sesf.object_id = sest.target_id
WHERE ses.name = @ring_position_session
AND   sest.name = N'ring_buffer'
AND   sesf.name = N'max_events_limit'
OPTION(RECOMPILE);";

    /// <summary>
    /// #8146. <see cref="SessionPositionServerSql"/> for a database-scoped session (Azure SQL Database), from the
    /// <c>sys.dm_xe_database_sessions</c> and <c>sys.database_event_session*</c> views.
    /// </summary>
    public const string SessionPositionDatabaseSql = @"SELECT
    @ring_created = xes.create_time,
    @ring_dropped = xes.dropped_event_count
FROM sys.dm_xe_database_sessions AS xes
WHERE xes.name = @ring_position_session
OPTION(RECOMPILE);

SET @ring_limit = 1000;

SELECT
    @ring_limit = ISNULL(TRY_CONVERT(bigint, sesf.value), 1000)
FROM sys.database_event_sessions AS ses
JOIN sys.database_event_session_targets AS sest
  ON sest.event_session_id = ses.event_session_id
JOIN sys.database_event_session_fields AS sesf
  ON  sesf.event_session_id = sest.event_session_id
  AND sesf.object_id = sest.target_id
WHERE ses.name = @ring_position_session
AND   sest.name = N'ring_buffer'
AND   sesf.name = N'max_events_limit'
OPTION(RECOMPILE);";

    /// <summary>
    /// The six columns <see cref="RingBufferObservation.Read"/> reads, as a select-list fragment (no leading comma): total, held,
    /// created, dropped, limit and the read time. Safe where the header or session fragments never ran: every value but the
    /// read time is NULL then.
    /// </summary>
    public const string PositionColumnsSql = @"ring_total = @ring_total,
    ring_held = @ring_held,
    ring_created = @ring_created,
    ring_dropped = @ring_dropped,
    ring_events_limit = @ring_limit,
    ring_read_at = SYSUTCDATETIME()";

    /* The labels the health reads parse back (PerformanceMonitor.Common.RingBufferTruncationNote holds the same three). These stay
       string literals, not aliases of the Common constants: CollectorMeasurementSeamTests resolves each Measure( label to a
       `const string NAME = "..."` in this file and cannot follow a reference to another type. RingBufferTruncationSignalTests
       asserts each equals its Common constant, and renders a note from these and parses it with the Common reader. */
    internal const string TruncatedMeasurement = "ring_buffer_truncated";
    internal const string EventsHiddenMeasurement = "ring_buffer_events_hidden";
    internal const string NewestVisibleAgeMeasurement = "newest_visible_event_age_s";

    /// <summary>
    /// Records the truncation columns at <paramref name="firstOrdinal"/> (truncated, hidden, age) through
    /// <see cref="CollectorContext.Measure"/>. Writes nothing for a whole buffer, so a clean run's note stays as it was;
    /// the health band reads <see cref="PerformanceMonitor.Common.RingBufferTruncationNote"/>. A reader with fewer columns
    /// (a test fake, an older batch) is read as not truncated.
    /// </summary>
    public static void MeasureTruncation(System.Data.Common.DbDataReader reader, int firstOrdinal, CollectorContext context)
    {
        if (reader.FieldCount < firstOrdinal + 3
            || reader.IsDBNull(firstOrdinal)
            || !reader.GetBoolean(firstOrdinal))
        {
            return;
        }

        context.Measure(TruncatedMeasurement, 1);
        context.Measure(
            EventsHiddenMeasurement,
            reader.IsDBNull(firstOrdinal + 1) ? 0 : System.Math.Max(0, reader.GetInt64(firstOrdinal + 1)));
        context.Measure(
            NewestVisibleAgeMeasurement,
            reader.IsDBNull(firstOrdinal + 2) ? 0 : System.Math.Max(0, reader.GetInt64(firstOrdinal + 2)));
    }
}
