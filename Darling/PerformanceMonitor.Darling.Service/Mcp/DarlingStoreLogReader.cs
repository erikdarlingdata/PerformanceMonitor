/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The store-log census reads for the MCP/web surface (#3021) — over what the hourly
/// <see cref="StoreLogSweep"/> persisted from the store's OWN server log.
///
/// <para>Three reads, and the FIRST one is the point: the capture summary is the denominator. This source
/// writes no <c>collection_log</c> row (it is not a catalog collector), so it cannot borrow the
/// <c>get_pg_blocking</c> denominator from there and carries its own in
/// <c>collect.store_log_captures</c> — without it, "no events" and "the sweep never ran" are the same
/// absence of rows.</para>
///
/// <para>The class census then reports each class's window total beside its PER-HOUR MEDIAN and MAX over the
/// same window, and no band. That is deliberate and it is the whole answer to the volume problem: ~1,100
/// user-request cancels a day is the ordinary floor, so the useful question is never "are there cancels" but
/// "is this hour like the others", and a median beside a count answers that in a form the reader compares
/// rather than trusts. Banding it would put a colour on a healthy store and teach its owner to ignore the
/// colour — #1852's reasoning, restated by <c>deadlock_coverage</c> (#3017) and followed here.</para>
/// </summary>
internal static class DarlingStoreLogReader
{
    /// <summary>
    /// The denominator. One row for the window: how many captures landed, over how many distinct hours, how
    /// much they read, how much was still unread at the newest capture, how many discarded a resume marker
    /// because the weekday ring truncated, and how many distinct retained messages were folded away by the
    /// per-class budget. $1/$2 window (naive UTC).
    /// </summary>
    public const string CaptureSummarySql = @"
WITH windowed AS
(
    SELECT
        c.capture_time    AS capture_time,
        c.log_file        AS log_file,
        c.bytes_read      AS bytes_read,
        c.bytes_pending   AS bytes_pending,
        c.lines_read      AS lines_read,
        c.entries_read    AS entries_read,
        c.offset_reset    AS offset_reset,
        c.groups_dropped  AS groups_dropped
    FROM collect.store_log_captures AS c
    WHERE c.capture_time > $1
    AND   c.capture_time <= $2
),
newest AS
(
    /* Pending bytes are a LEVEL, not a total: summing them over the window would add the same backlog once
       per capture. Only the newest capture's figure means anything, so only it is read. */
    SELECT coalesce(sum(n.bytes_pending), 0)::bigint AS bytes_pending,
           coalesce(sum(n.bytes_read), 0)::bigint    AS bytes_read
    FROM windowed AS n
    WHERE n.capture_time = (SELECT max(m.capture_time) FROM windowed AS m)
)
SELECT
    /* DISTINCT capture_time, not count(*): one capture writes one row per FILE, so on the hour a rotation
       falls in it writes two. Counting rows would make the tick count exceed the tick count EXPECTED and
       the missing-interval comparison could then never fire, which is the one comparison this table exists
       to make possible. file_reads carries the row count separately, where it means what it says. */
    count(DISTINCT w.capture_time)::bigint                          AS captures,
    count(*)::bigint                                                AS file_reads,
    count(DISTINCT date_trunc('hour', w.capture_time))::integer     AS capture_hours,
    coalesce(sum(w.bytes_read), 0)::bigint                          AS bytes_read,
    coalesce(sum(w.lines_read), 0)::bigint                          AS lines_read,
    coalesce(sum(w.entries_read), 0)::bigint                        AS entries_read,
    count(*) FILTER (WHERE w.offset_reset)::integer                 AS offset_resets,
    coalesce(sum(w.groups_dropped), 0)::integer                     AS groups_dropped,
    min(w.capture_time)                                             AS first_capture_at,
    max(w.capture_time)                                             AS last_capture_at,
    (SELECT p.bytes_pending FROM newest AS p)                       AS bytes_pending,
    coalesce(array_agg(DISTINCT w.log_file ORDER BY w.log_file), '{}'::text[]) AS log_files,
    /* #9314: the newest capture's own bytes read, and when the marker last moved. Two captures in a row that read 0
       bytes with bytes pending are a marker that is not moving: one is a heartbeat over a short line still being
       written. The time is the newest capture that read bytes at or before the window's end, so a window that ends
       before a later reading capture does not report that later time. */
    (SELECT p.bytes_read FROM newest AS p)                          AS newest_bytes_read,
    (SELECT max(c.capture_time) FROM collect.store_log_captures AS c WHERE c.bytes_read > 0 AND c.capture_time <= $2) AS marker_last_moved_at,
    (SELECT sum(t.bytes_read)::bigint FROM
        (SELECT x.capture_time, sum(x.bytes_read) AS bytes_read FROM windowed AS x GROUP BY x.capture_time ORDER BY x.capture_time DESC LIMIT 1 OFFSET 1) AS t
    ) AS previous_bytes_read
FROM windowed AS w";

    /// <summary>
    /// One row per class: the window total, the severities it appeared at, and the per-hour distribution.
    ///
    /// <para>The distribution is computed over EVERY captured hour, not only the hours the class appeared
    /// in — that is what <c>buckets</c> and the CROSS JOIN are for. A median taken over appearances alone
    /// reads high by exactly the quiet hours it skipped, which for a class that fires in bursts (the convoy
    /// signature) is the difference between "unusual" and "normal". The zero-filled grid is the honest
    /// denominator, the same move the capture summary makes one level up.</para>
    ///
    /// <para><c>occurrences_last_hour</c> is the NEWEST CAPTURED hour rather than the last sixty minutes,
    /// and is named for what it is: the sweep is hourly, so there is no finer bucket to report and
    /// pretending otherwise would put a partial hour beside full ones. $1/$2 window (naive UTC).</para>
    /// </summary>
    public const string ClassCensusSql = @"
WITH buckets AS
(
    SELECT DISTINCT date_trunc('hour', c.capture_time) AS bucket
    FROM collect.store_log_captures AS c
    WHERE c.capture_time > $1
    AND   c.capture_time <= $2
),
hourly AS
(
    SELECT
        e.event_class                             AS event_class,
        date_trunc('hour', e.capture_time)        AS bucket,
        sum(e.occurrences)::bigint                AS occurrences
    FROM collect.store_log_events AS e
    WHERE e.capture_time > $1
    AND   e.capture_time <= $2
    GROUP BY e.event_class, date_trunc('hour', e.capture_time)
),
severities AS
(
    SELECT
        e.event_class                                         AS event_class,
        array_agg(DISTINCT e.severity ORDER BY e.severity)    AS severities
    FROM collect.store_log_events AS e
    WHERE e.capture_time > $1
    AND   e.capture_time <= $2
    GROUP BY e.event_class
),
grid AS
(
    SELECT
        s.event_class                       AS event_class,
        b.bucket                            AS bucket,
        coalesce(h.occurrences, 0)::bigint  AS occurrences
    FROM severities AS s
    CROSS JOIN buckets AS b
    LEFT JOIN hourly AS h
      ON  h.event_class = s.event_class
      AND h.bucket = b.bucket
)
SELECT
    g.event_class                                                            AS event_class,
    s.severities                                                             AS severities,
    sum(g.occurrences)::bigint                                               AS occurrences_window,
    coalesce(sum(g.occurrences) FILTER (
        WHERE g.bucket = (SELECT max(b2.bucket) FROM buckets AS b2)), 0)::bigint
                                                                             AS occurrences_last_hour,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY g.occurrences::double precision)
                                                                             AS per_hour_median,
    max(g.occurrences)::bigint                                               AS per_hour_max,
    count(*)::integer                                                        AS hours_in_window
FROM grid AS g
JOIN severities AS s
  ON s.event_class = g.event_class
GROUP BY g.event_class, s.severities
ORDER BY sum(g.occurrences) DESC, g.event_class";

    /// <summary>
    /// The retained entries — one row per distinct message, with the count of times it arrived and ONE
    /// entry as the evidence. <c>message_text</c> is the message's GROUPING key (#3944,
    /// <see cref="StoreLogClassifier.GroupingKeyOf"/>), so the same error for two values is one row; the tool
    /// shows the sample's own message. $1/$2 window (naive UTC), $3 row cap.
    ///
    /// <para>The order ends on the grouping key, so it is total: rows that tie on capture and count keep one order
    /// on every read, which is what lets an offset page through them (#8568).</para>
    ///
    /// <para>Inside one capture a WARNING or worse comes ahead of a LOG row, so a short page carries the errors and
    /// not only the most repeated slow plans (#8813).</para>
    ///
    /// <para><c>total_groups</c> is every row the window holds, counted over the grouped set before the LIMIT cuts it,
    /// so the page can say how much of the set it is (#8813).</para>
    ///
    /// <para>$4 is the <c>query_id</c> filter (#9435), or NULL for none. A slow plan's message ends in
    /// <c>queryid=&lt;id&gt;</c> and that text is the only place the id is kept (the row has no column for it), so the
    /// filter is one exact test of the message's tail on the plan class ($5, passed so this text names no class), in the WHERE ahead of the LIMIT: an id that
    /// is a prefix or suffix of another statement's does not match it.</para>
    ///
    /// <para><c>message_text IS NOT NULL</c> is the whole predicate, and it is a structural test rather than
    /// a class list: the sweep writes text only for the classes the classifier retains, so asking for rows
    /// that HAVE text asks exactly the right population and cannot drift from the classifier's own decision
    /// about which classes those are.</para>
    /// </summary>
    public const string RetainedEventsSql = @"
SELECT
    e.event_class                            AS event_class,
    e.severity                               AS severity,
    e.message_text                           AS message_text,
    sum(e.occurrences)::bigint               AS occurrences,
    min(e.capture_time)                      AS first_capture_at,
    max(e.capture_time)                      AS last_capture_at,
    (array_agg(e.sample_line ORDER BY e.capture_time DESC))[1] AS sample_line,
    count(*) OVER ()::bigint                 AS total_groups
FROM collect.store_log_events AS e
WHERE e.capture_time > $1
AND   e.capture_time <= $2
AND   e.message_text IS NOT NULL
AND   ($4::text IS NULL
       OR (e.event_class = $5 AND right(e.message_text, length($4::text)) = $4::text))
GROUP BY e.event_class, e.severity, e.message_text
ORDER BY max(e.capture_time) DESC,
         CASE WHEN e.severity IN ('WARNING', 'ERROR', 'FATAL', 'PANIC') THEN 0 ELSE 1 END,
         sum(e.occurrences) DESC, e.event_class, e.severity, e.message_text
LIMIT $3";

    /// <summary>The capture denominator for the window.</summary>
    public sealed class CaptureSummary
    {
        /// <summary>Sweep ticks that landed in the window — DISTINCT capture instants, not rows.</summary>
        [JsonPropertyName("captures")] public long Captures { get; init; }

        /// <summary>File reads across those ticks. Higher than <see cref="Captures"/> exactly when a tick
        /// spanned a log rotation and read the outgoing file as well as the incoming one.</summary>
        [JsonPropertyName("file_reads")] public long FileReads { get; init; }

        [JsonPropertyName("capture_hours")] public int CaptureHours { get; init; }

        /// <summary>How many hourly captures the window could have held. The comparison against
        /// <see cref="Captures"/> is what a missing interval looks like — and a missing interval is the ONLY
        /// tell for the one gap the resume marker cannot detect (see
        /// <see cref="StoreLogSlab.ResolveResume"/>).</summary>
        [JsonPropertyName("captures_expected")] public int CapturesExpected { get; init; }

        [JsonPropertyName("bytes_read")] public long BytesRead { get; init; }

        [JsonPropertyName("lines_read")] public long LinesRead { get; init; }

        [JsonPropertyName("entries_read")] public long EntriesRead { get; init; }

        /// <summary>Bytes still unread when the newest capture finished — non-zero means the per-capture
        /// read cap bit and the next capture picks it up, not that anything was lost.</summary>
        [JsonPropertyName("bytes_pending")] public long BytesPending { get; init; }

        /// <summary>Bytes the newest capture in the window read (#9314). Null when not known.</summary>
        [JsonPropertyName("newest_capture_bytes_read"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public long? NewestCaptureBytesRead { get; init; }

        /// <summary>When a read marker last moved, as UTC ISO text (#9314). Null when no marker is stored.</summary>
        [JsonPropertyName("marker_last_moved_at"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? MarkerLastMovedAt { get; init; }

        /// <summary>Bytes the capture before the newest in the window read (#9314). Null when the window holds one capture.</summary>
        [JsonIgnore] public long? PreviousCaptureBytesRead { get; init; }

        /// <summary>True when the newest two captures both read 0 bytes while bytes are pending (#9314): the marker has not moved, so
        /// the log is not being read past it. One such capture is not enough, since a heartbeat over a short line at the end of the file reads nothing too. That is not the backlog <see cref="BytesPending"/> alone describes, which later
        /// captures read down.</summary>
        [JsonIgnore] public bool MarkerStalled => BytesPending > 0 && NewestCaptureBytesRead == 0 && PreviousCaptureBytesRead == 0;

        /// <summary>Captures that discarded a resume marker because the weekday ring truncated the file
        /// underneath it.</summary>
        [JsonPropertyName("offset_resets")] public int OffsetResets { get; init; }

        /// <summary>Distinct retained messages folded into their class's count by the per-class budget.
        /// Non-zero means some class produced more distinct messages in one capture than
        /// <see cref="StoreLogClassifier.MaxRetainedGroupsPerClass"/> keeps — the occurrences are all still
        /// counted, the individual texts are not all kept.</summary>
        [JsonPropertyName("messages_folded")] public int MessagesFolded { get; init; }

        [JsonPropertyName("first_capture_at")] public string? FirstCaptureAt { get; init; }

        [JsonPropertyName("last_capture_at")] public string? LastCaptureAt { get; init; }

        [JsonPropertyName("log_files")] public IReadOnlyList<string> LogFiles { get; init; } = [];
    }

    /// <summary>One class's window total and its per-hour distribution. No band, by design — see the class
    /// remarks on <see cref="DarlingStoreLogReader"/>.</summary>
    public sealed class ClassCensus
    {
        [JsonPropertyName("event_class")] public string EventClass { get; init; } = string.Empty;

        [JsonPropertyName("severities")] public IReadOnlyList<string> Severities { get; init; } = [];

        /// <summary>Whether this class keeps its text. False is the record that the class is a counted
        /// FLOOR rather than a measurement that went missing.</summary>
        [JsonPropertyName("text_retained")] public bool TextRetained { get; init; }

        /// <summary>Why the class exists, from the classifier's own rule — never a second copy of it.</summary>
        [JsonPropertyName("why")] public string Why { get; init; } = string.Empty;

        [JsonPropertyName("occurrences_window")] public long OccurrencesWindow { get; init; }

        [JsonPropertyName("occurrences_last_hour")] public long OccurrencesLastHour { get; init; }

        /// <summary>The middle hour of the window, computed over EVERY captured hour including the ones this
        /// class did not appear in. The number to read <see cref="OccurrencesLastHour"/> against.</summary>
        [JsonPropertyName("per_hour_median")] public double PerHourMedian { get; init; }

        [JsonPropertyName("per_hour_max")] public long PerHourMax { get; init; }

        [JsonPropertyName("hours_in_window")] public int HoursInWindow { get; init; }
    }

    /// <summary>One retained message, with one entry as its evidence.</summary>
    public sealed class RetainedEvent
    {
        [JsonPropertyName("event_class")] public string EventClass { get; init; } = string.Empty;

        [JsonPropertyName("severity")] public string Severity { get; init; } = string.Empty;

        /// <summary>The sample's message as PostgreSQL wrote it (#3944): one instance of the row's message, which
        /// counts every entry that differs from it only in a quoted value or a number
        /// (<see cref="StoreLogClassifier.DisplayMessageOf"/>).</summary>
        [JsonPropertyName("message_text")] public string MessageText { get; init; } = string.Empty;

        [JsonPropertyName("occurrences")] public long Occurrences { get; init; }

        [JsonPropertyName("first_capture_at")] public string? FirstCaptureAt { get; init; }

        [JsonPropertyName("last_capture_at")] public string? LastCaptureAt { get; init; }

        /// <summary>The entry as the server wrote it, prefix and continuations included, with only its SQL
        /// normalized. The classified fields are an interpretation; this is the evidence, and it carries the
        /// server's own timestamp in the server's own <c>log_timezone</c> — uninterpreted, on purpose.</summary>
        [JsonPropertyName("sample_line")] public string? SampleLine { get; init; }

        /// <summary>True when <see cref="SampleLine"/> is a preview of the stored entry (#8568): a logged plan or
        /// statement runs to thousands of characters, and <c>full_text</c> returns it whole.</summary>
        [JsonPropertyName("sample_line_truncated")] public bool SampleLineTruncated { get; init; }

        /// <summary>The stored entry's whole length in characters, beside the preview.</summary>
        [JsonPropertyName("sample_line_length")] public int? SampleLineLength { get; init; }

        /// <summary>The TOAST relation an autovacuum cancel's CONTEXT names, as the log writes it with its
        /// database (<c>darling.pg_toast.pg_toast_17428</c>), when it names one (#8814). Null for every other row.</summary>
        [JsonPropertyName("toast_relation"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] public string? ToastRelation { get; init; }

        /// <summary>The table that owns <see cref="ToastRelation"/> (<c>schema.table</c>), read from the store's catalog
        /// when the answer is built (#8814), so the reader can act on the cancel without a catalog query. Null when
        /// the row names no TOAST relation, the relation no longer exists, or the log names it under another
        /// database than this store's (#8822).</summary>
        [JsonPropertyName("toast_owner"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] public string? ToastOwner { get; set; }
    }

    /// <summary>A class this build classifies into that had no rows in the window.</summary>
    public sealed class AbsentClass
    {
        [JsonPropertyName("event_class")] public string EventClass { get; init; } = string.Empty;

        [JsonPropertyName("text_retained")] public bool TextRetained { get; init; }

        [JsonPropertyName("why")] public string Why { get; init; } = string.Empty;
    }

    /// <summary>
    /// The classes this build has that the window did not see — computed from the classifier's own class
    /// list minus what came back, so it cannot describe a vocabulary the classifier no longer has.
    /// </summary>
    public static List<AbsentClass> ComputeAbsentClasses(IReadOnlyList<ClassCensus> seen)
    {
        var present = new HashSet<string>(StringComparer.Ordinal);
        if (seen is not null)
        {
            foreach (var row in seen)
            {
                present.Add(row.EventClass);
            }
        }

        var absent = new List<AbsentClass>();
        foreach (var name in StoreLogClassifier.ClassNames)
        {
            if (present.Contains(name))
            {
                continue;
            }

            absent.Add(new AbsentClass
            {
                EventClass = name,
                TextRetained = StoreLogClassifier.IsRetainedClass(name),
                Why = StoreLogClassifier.WhyFor(name),
            });
        }

        return absent;
    }

    /// <summary>The unread-log fields of one answer (#8904).</summary>
    public sealed class LogBacklogInfo
    {
        /// <summary>Bytes of log still unread when the newest capture finished.</summary>
        [JsonPropertyName("bytes_pending")] public long BytesPending { get; init; }

        /// <summary>The most log one capture reads.</summary>
        [JsonPropertyName("per_capture_bound_bytes")] public long PerCaptureBoundBytes { get; init; }

        /// <summary>The least captures the backlog needs if nothing more is written.</summary>
        [JsonPropertyName("captures_needed")] public long CapturesNeeded { get; init; }

        /// <summary>The log's own time stamp on the newest kept sample line, as written, in the store's log time zone.
        /// It is a floor, not a ceiling: the read reached at least this time, and how far past it is not known, because only
        /// a retained class keeps a sample line. Null when no kept sample line carries one.</summary>
        [JsonPropertyName("newest_sample_log_stamp")] public string? NewestSampleLogStamp { get; init; }

        /// <summary>True when the newest capture read nothing while bytes are pending (#9314).</summary>
        [JsonPropertyName("marker_stalled")] public bool MarkerStalled { get; init; }

        /// <summary>When a read marker last moved, as UTC ISO text, while <see cref="MarkerStalled"/>. Null when unknown.</summary>
        [JsonPropertyName("marker_last_moved_at"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string? MarkerLastMovedAt { get; init; }

        [JsonPropertyName("note")] public string Note => MarkerStalled
            ? "The read marker is not moving: the newest capture read 0 bytes while the log has more past the marker, so the newest hours of the log are not read, and 0 in occurrences_last_hour is not a quiet hour."
            : "The newest hours of the log are not read yet: occurrences_last_hour counts only what was read, so 0 there is not a quiet hour.";
    }

    /// <summary>What one <c>get_store_log</c> answer is.</summary>
    public sealed class StoreLogReport
    {
        [JsonPropertyName("window_hours")] public int WindowHours { get; init; }

        [JsonPropertyName("as_of")] public string AsOf { get; init; } = string.Empty;

        [JsonPropertyName("capture_summary")] public CaptureSummary Captures { get; init; } = new();

        /// <summary>The log's own stamp (as written, in the store's log time zone) on the newest kept sample line, set
        /// by the caller only while bytes are pending (#8904). Null when none was found.</summary>
        [JsonIgnore] public string? NewestSampleLogStamp { get; init; }

        /// <summary>Present only while bytes are pending (#8904): the newest hours of the log are not read yet, how much
        /// one capture reads, and the log time the read reached at least, so an <c>occurrences_last_hour</c> of 0 is not taken
        /// for a quiet hour. Absent when the reader is level.</summary>
        [JsonPropertyName("log_backlog"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public LogBacklogInfo? LogBacklog => Captures.BytesPending > 0
            ? new LogBacklogInfo
            {
                BytesPending = Captures.BytesPending,
                PerCaptureBoundBytes = StoreLogSweep.MaxBytesPerCapture,
                CapturesNeeded = StoreLogSweep.CapturesNeeded(Captures.BytesPending),
                MarkerStalled = Captures.MarkerStalled,
                MarkerLastMovedAt = Captures.MarkerStalled ? Captures.MarkerLastMovedAt : null,
                NewestSampleLogStamp = NewestSampleLogStamp,
            }
            : null;

        [JsonPropertyName("classes")] public IReadOnlyList<ClassCensus> Classes { get; init; } = [];

        /// <summary>
        /// Every class this build classifies into that did NOT occur in the window — so a class absent from
        /// <see cref="Classes"/> is legible as a zero rather than as a class this build might not have.
        ///
        /// <para>That distinction is <c>target_has_user_databases</c>'s (#1852) one level over: the reader
        /// needs to tell "nothing to report" from "not reported". It is also where the good news lives — zero
        /// crash recoveries, zero data-integrity complaints and zero panics are the resting state of a
        /// healthy store, and a payload that simply omitted them would make a reader go and check.</para>
        ///
        /// <para>DERIVED from the classifier's own class list rather than a second copy of it.</para>
        /// </summary>
        [JsonPropertyName("classes_not_seen")] public IReadOnlyList<AbsentClass> ClassesNotSeen { get; init; } = [];

        [JsonPropertyName("retained_events")] public IReadOnlyList<RetainedEvent> RetainedEvents { get; init; } = [];

        /// <summary>Retained rows skipped before <see cref="RetainedEvents"/>: the caller's offset (#8568).</summary>
        [JsonIgnore] public int RetainedEventsSkipped { get; init; }

        /// <summary>How many rows <see cref="RetainedEvents"/> holds.</summary>
        [JsonPropertyName("retained_events_returned")] public int RetainedEventsReturned => RetainedEvents.Count;

        private int _retainedEventsTotal;

        /// <summary>How many distinct retained messages the window holds, whatever the page: the rows this answer
        /// holds are <see cref="RetainedEventsReturned"/> of them, after the first <c>offset</c> (#8813). Never less
        /// than the rows the page already reaches.</summary>
        [JsonPropertyName("retained_events_total")]
        public int RetainedEventsTotal
        {
            get => Math.Max(_retainedEventsTotal, RetainedEventsSkipped + RetainedEvents.Count);
            init => _retainedEventsTotal = value;
        }

        /// <summary>True when retained messages follow the last row this answer holds, whether <c>limit</c> or the
        /// response budget left them off (#8813). The way on is <c>offset</c> at <c>skipped + returned</c>.</summary>
        [JsonPropertyName("truncated")] public bool Truncated => RetainedEventsSkipped + RetainedEvents.Count < RetainedEventsTotal;

        /// <summary>The oldest <c>last_capture_at</c> among <see cref="RetainedEvents"/>, which are newest capture
        /// first: where this answer's rows stop, and the time <c>as_of</c> reads on from (#8568). Null when the answer
        /// holds no row.</summary>
        [JsonPropertyName("oldest_returned_last_capture_at")]
        public string? OldestReturnedLastCaptureAt => RetainedEvents.Count == 0 ? null : RetainedEvents[^1].LastCaptureAt;

        /// <summary>True when retained rows were left off the end to fit the response budget (#8568).</summary>
        [JsonPropertyName("budget_truncated")] public bool BudgetTruncated { get; init; }

        /// <summary>How many rows were read and how many fit the budget; null when none were left out.</summary>
        [JsonPropertyName("budget_note")] public string? BudgetNote { get; init; }

        /// <summary>
        /// The sentence, DERIVED from the counts above rather than assigned — <c>deadlock_coverage</c>'s
        /// convention (#3017), for its reason: a settable note is a note that can be omitted, or can drift
        /// from the numbers it describes.
        /// </summary>
        [JsonPropertyName("note")]
        public string Note => BuildNote(this);
    }

    /// <summary>
    /// The window's whole story in one paragraph: what was read, what the floor is, what was excluded and
    /// under which name, and what is left to read. It names no verdict and applies no band — every number it
    /// cites is beside it in the payload.
    /// </summary>
    public static string BuildNote(StoreLogReport report)
    {
        if (report is null)
        {
            return string.Empty;
        }

        var summary = report.Captures;
        var note = new StringBuilder();

        note.Append(CultureInfo.InvariantCulture, $"{summary.EntriesRead:N0} log entr")
            .Append(summary.EntriesRead == 1 ? "y" : "ies")
            .Append(CultureInfo.InvariantCulture, $" classified from the store's own log over {summary.Captures:N0} capture")
            .Append(summary.Captures == 1 ? string.Empty : "s")
            .Append(CultureInfo.InvariantCulture, $" of an expected {summary.CapturesExpected:N0} in the last {report.WindowHours:N0} hour")
            .Append(report.WindowHours == 1 ? ". " : "s. ");

        if (summary.Captures < summary.CapturesExpected)
        {
            note.Append("Fewer captures than the hourly cadence would produce, so an interval of the log was "
                + "not read - the sweep skipped a tick, or the service was down for it. ");
        }

        var counted = new List<ClassCensus>();
        foreach (var row in report.Classes)
        {
            if (!row.TextRetained && row.OccurrencesWindow > 0)
            {
                counted.Add(row);
            }
        }

        if (counted.Count > 0)
        {
            note.Append("Counted and not retained, because these are the expected floor rather than faults: ");
            for (var i = 0; i < counted.Count; i++)
            {
                if (i > 0)
                {
                    note.Append(i == counted.Count - 1 ? " and " : ", ");
                }

                note.Append(CultureInfo.InvariantCulture,
                    $"{counted[i].EventClass} {counted[i].OccurrencesWindow:N0} ({counted[i].PerHourMedian:N0}/h median)");
            }

            note.Append(". ");
        }

        if (report.BudgetTruncated || report.Truncated || report.RetainedEventsSkipped > 0)
        {
            /* A page of the retained messages, whatever cut it (a limit, the budget, or an offset): the window's
               total beside the page's own count, and where the rest starts (#8813). */
            note.Append(CultureInfo.InvariantCulture,
                $"{report.RetainedEvents.Count:N0} of {report.RetainedEventsTotal:N0} distinct messages retained with their text {(report.RetainedEvents.Count == 1 ? "is" : "are")} listed here");
            if (report.Truncated)
            {
                note.Append(CultureInfo.InvariantCulture,
                    $"; offset {report.RetainedEventsSkipped + report.RetainedEvents.Count:N0} reads on, with as_of at this answer's as_of so the rows do not shift");
            }

            note.Append(". ");
        }
        else
        {
            note.Append(CultureInfo.InvariantCulture,
                $"{CountText.Plural(report.RetainedEvents.Count, "distinct message")} {(report.RetainedEvents.Count == 1 ? "is" : "are")} retained with their text. ");
        }

        if (summary.MessagesFolded > 0)
        {
            note.Append(string.Format(
                CultureInfo.InvariantCulture,
                "{0} {1} folded into their class's count by the per-class budget "
                + "of {2}; their occurrences are counted, their individual texts are not kept. ",
                CountText.Plural(summary.MessagesFolded, "further distinct message"),
                summary.MessagesFolded == 1 ? "was" : "were",
                StoreLogClassifier.MaxRetainedGroupsPerClass));
        }

        if (summary.OffsetResets > 0)
        {
            note.Append(string.Format(
                CultureInfo.InvariantCulture,
                "{0} discarded a resume marker because the weekday log ring truncated the file "
                + "underneath it, so those captures cover an interval the previous ones did not. ",
                CountText.Plural(summary.OffsetResets, "capture")));
        }

        if (summary.BytesPending > 0)
        {
            if (summary.MarkerStalled)
            {
                /* #9314: a capture that read nothing while bytes are pending is not a backlog the next capture reads down. */
                note.Append(string.Format(
                    CultureInfo.InvariantCulture,
                    "{0} {1} past the read marker, and the newest capture read none of them: the marker has {2}. "
                    + "The log is not being read past it, so what was written after that point is missing from every count here. ",
                    CountText.Plural(summary.BytesPending, "byte"),
                    summary.BytesPending == 1 ? "is" : "are",
                    summary.MarkerLastMovedAt is { } movedAt
                        ? $"not moved since {movedAt} UTC"
                        : "not moved in the latest capture"));
            }
            else if (summary.BytesPending > StoreLogSweep.MaxBytesPerCapture)
            {
                /* #8904: more than one capture can read, so "the next capture reads them" would be untrue. */
                var needed = StoreLogSweep.CapturesNeeded(summary.BytesPending);
                note.Append(string.Format(
                    CultureInfo.InvariantCulture,
                    "{0} {1} still unread when the newest capture finished. One capture reads at most {2} ({3} slabs of {4}), "
                    + "so this backlog needs at least {5} to read, at one capture an hour, and more if the log keeps growing. ",
                    CountText.Plural(summary.BytesPending, "byte"),
                    summary.BytesPending == 1 ? "was" : "were",
                    CountText.Plural(StoreLogSweep.MaxBytesPerCapture, "byte"),
                    StoreLogSweep.MaxSlabsPerCapture,
                    CountText.Plural(StoreLogSlab.MaxBytesPerRead, "byte"),
                    CountText.Plural(needed, "capture")));
            }
            else
            {
                note.Append(string.Format(
                    CultureInfo.InvariantCulture,
                    "{0} {1} still unread when the newest capture finished; the next capture reads them. ",
                    CountText.Plural(summary.BytesPending, "byte"),
                    summary.BytesPending == 1 ? "was" : "were"));
            }

            /* #8904: the newest hours are the unread ones, so a 0 in occurrences_last_hour is not a quiet hour. */
            if (!summary.MarkerStalled)
            {
                note.Append(report.NewestSampleLogStamp is { } stamp
                ? $"The read reached at least the log's own time {stamp} (the newest stamp on a kept sample line) and how far past it is not known, so occurrences_last_hour and the newest hours of every class count only what was read and can read low. "
                : "The newest hours of the log are not read yet (no kept sample line carries a log time to name), so occurrences_last_hour and the newest hours of every class count only what was read and can read low. ");
            }
        }

        note.Append("No band is applied. Whether a class moved is its count read against the per-hour median "
            + "beside it, which is a comparison rather than a verdict: a quiet store with a large cancel floor "
            + "is healthy and has to keep reading that way.");

        return note.ToString();
    }

    /// <summary>
    /// The first characters of every kept sample line in the newest capture that kept any (#8904). The sweep parses no
    /// timestamp, so the only log time the store holds is the one a kept entry carries at the front of its own line.
    /// $1/$2 window (naive UTC).
    /// </summary>
    public const string NewestSampleHeadsSql = @"
WITH newest AS
(
    SELECT max(m.capture_time) AS capture_time
    FROM collect.store_log_events AS m
    WHERE m.sample_line IS NOT NULL
    AND   m.capture_time > $1
    AND   m.capture_time <= $2
)
SELECT left(e.sample_line, 64)
FROM collect.store_log_events AS e
JOIN newest AS n
  ON n.capture_time = e.capture_time
WHERE e.sample_line IS NOT NULL";

    private static readonly System.Text.RegularExpressions.Regex s_logStamp = new(
        @"^\s*\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?(?: [A-Za-z+\-][A-Za-z0-9+:\-]*)?(?=\s|\[|$)",
        System.Text.RegularExpressions.RegexOptions.CultureInvariant | System.Text.RegularExpressions.RegexOptions.Compiled);

    /// <summary>The log stamp at the front of a sample line as the log wrote it, or null when it has none.</summary>
    public static string? LogStampOf(string? sampleHead)
    {
        if (string.IsNullOrEmpty(sampleHead))
        {
            return null;
        }

        var match = s_logStamp.Match(sampleHead);
        return match.Success ? match.Value.Trim() : null;
    }

    /// <summary>The newest log stamp among the kept samples (#8904), or null when none carries one.</summary>
    public static async Task<string?> GetNewestSampleLogStampAsync(
        NpgsqlDataSource postgres,
        DateTime windowStartUtc,
        DateTime windowEndUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(NewestSampleHeadsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(DateTime.SpecifyKind(windowStartUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(windowEndUtc, DateTimeKind.Unspecified));

        string? newest = null;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var stamp = LogStampOf(reader.IsDBNull(0) ? null : reader.GetString(0));
            if (stamp is not null && (newest is null || string.CompareOrdinal(stamp, newest) > 0))
            {
                newest = stamp;
            }
        }

        return newest;
    }

    /// <summary>Reads the denominator for the window.</summary>
    public static async Task<CaptureSummary> GetCaptureSummaryAsync(
        NpgsqlDataSource postgres,
        DateTime windowStartUtc,
        DateTime windowEndUtc,
        int windowHours,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(CaptureSummarySql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(DateTime.SpecifyKind(windowStartUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(windowEndUtc, DateTimeKind.Unspecified));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return new CaptureSummary { CapturesExpected = windowHours };
        }

        return new CaptureSummary
        {
            Captures = reader.GetInt64(0),
            FileReads = reader.GetInt64(1),
            CaptureHours = reader.GetInt32(2),
            CapturesExpected = windowHours,
            BytesRead = reader.GetInt64(3),
            LinesRead = reader.GetInt64(4),
            EntriesRead = reader.GetInt64(5),
            OffsetResets = reader.GetInt32(6),
            MessagesFolded = reader.GetInt32(7),
            FirstCaptureAt = reader.IsDBNull(8) ? null : Iso(reader.GetDateTime(8)),
            LastCaptureAt = reader.IsDBNull(9) ? null : Iso(reader.GetDateTime(9)),
            BytesPending = reader.GetInt64(10),
            LogFiles = reader.IsDBNull(11) ? [] : reader.GetFieldValue<string[]>(11),
            NewestCaptureBytesRead = reader.IsDBNull(12) ? null : reader.GetInt64(12),
            MarkerLastMovedAt = reader.IsDBNull(13) ? null : Iso(reader.GetDateTime(13)),
            PreviousCaptureBytesRead = reader.IsDBNull(14) ? null : reader.GetInt64(14),
        };
    }

    /// <summary>Reads the per-class census, filling each class's retention flag and reason from the
    /// classifier itself.</summary>
    public static async Task<List<ClassCensus>> GetClassCensusAsync(
        NpgsqlDataSource postgres,
        DateTime windowStartUtc,
        DateTime windowEndUtc,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<ClassCensus>();

        await using var command = postgres.CreateCommand(ClassCensusSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(DateTime.SpecifyKind(windowStartUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(windowEndUtc, DateTimeKind.Unspecified));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var eventClass = reader.GetString(0);
            rows.Add(new ClassCensus
            {
                EventClass = eventClass,
                Severities = reader.IsDBNull(1) ? [] : reader.GetFieldValue<string[]>(1),
                TextRetained = StoreLogClassifier.IsRetainedClass(eventClass),
                Why = StoreLogClassifier.WhyFor(eventClass),
                OccurrencesWindow = reader.GetInt64(2),
                OccurrencesLastHour = reader.GetInt64(3),
                PerHourMedian = reader.IsDBNull(4) ? 0 : Math.Round(reader.GetDouble(4), 1),
                PerHourMax = reader.GetInt64(5),
                HoursInWindow = reader.GetInt32(6),
            });
        }

        return rows;
    }

    /// <summary>Reads the retained messages for the window: up to <paramref name="limit"/> rows after the first
    /// <paramref name="offset"/> of the read's order. <c>Skipped</c> is how many rows the offset passed over, which is
    /// less than the offset when the window holds fewer. <c>Total</c> is how many rows the window holds, so a page can say
    /// it is a page (#8813); a row the read passes that has no text to show is left out of it, so it is exact whenever
    /// the page reaches the end.</summary>
    public static async Task<(List<RetainedEvent> Rows, int Skipped, int Total)> GetRetainedEventsAsync(
        NpgsqlDataSource postgres,
        DateTime windowStartUtc,
        DateTime windowEndUtc,
        int limit,
        int offset,
        int? sampleLinePreviewLength,
        long? queryId = null,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<RetainedEvent>();
        var skipped = 0;
        long total = 0;
        var emptied = 0;
        var toastRefs = new HashSet<StoreLogClassifier.ToastRelationRef>();

        await using var command = postgres.CreateCommand(RetainedEventsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(DateTime.SpecifyKind(windowStartUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(windowEndUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(offset + limit);
        command.Parameters.Add(new NpgsqlParameter
        {
            Value = queryId is long id ? " queryid=" + id.ToString(CultureInfo.InvariantCulture) : DBNull.Value,
            NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text,
        });
        command.Parameters.AddWithValue(StoreLogClassifier.SlowPlanClass);

        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        while (await reader.ReadAsync(cancellationToken))
        {
            total = reader.GetInt64(7);

            /* #3915, #3944: brought to this build's rules again on the way out, with the classifier's own
               function. Rows a newer capture wrote come back unchanged (it is idempotent), and rows an earlier
               build stored with a statement's literals are normalized in storage by the sweep's own pass, a
               bounded slice per hour; this covers the hours before that pass reaches them. The row SHOWS its
               sample's own message, because the stored one is the grouping key. */
            var eventClass = reader.GetString(0);
            var (key, sample) = StoreLogClassifier.MaskStoredEvent(
                eventClass, reader.GetString(2), reader.IsDBNull(6) ? null : reader.GetString(6));
            if (key is null)
            {
                /* A row the pass will empty (a fragment an older build filed as an entry): its occurrences are in
                   the class census, and it has no text to show. */
                emptied++;
                continue;
            }

            /* The offset counts the rows an answer lists, so a row with no text to show is not one of them. */
            if (skipped < offset)
            {
                skipped++;
                continue;
            }

            /* #8568: the evidence line is a preview when the caller gave a length; the message stays whole, because it is
               what tells one row from another. */
            var line = McpResponseBudget.Preview(sample, sampleLinePreviewLength ?? 0, whole: sampleLinePreviewLength is null);
            var toastRef = eventClass == StoreLogClassifier.AutovacuumCancelClass ? StoreLogClassifier.ToastRelationOf(sample) : null;
            if (toastRef is { Database: not null } named)
            {
                toastRefs.Add(named);
            }

            rows.Add(new RetainedEvent
            {
                ToastRelation = toastRef?.Qualified,
                EventClass = eventClass,
                Severity = reader.GetString(1),
                MessageText = StoreLogClassifier.DisplayMessageOf(eventClass, key, sample),
                Occurrences = reader.GetInt64(3),
                FirstCaptureAt = reader.IsDBNull(4) ? null : Iso(reader.GetDateTime(4)),
                LastCaptureAt = reader.IsDBNull(5) ? null : Iso(reader.GetDateTime(5)),
                SampleLine = line.Text,
                SampleLineTruncated = line.Truncated,
                SampleLineLength = line.Length,
            });
        }

        if (toastRefs.Count > 0)
        {
            var owners = await GetToastOwnersAsync(postgres, toastRefs, cancellationToken);
            foreach (var row in rows)
            {
                if (row.ToastRelation is { } name && owners.TryGetValue(name, out var owner))
                {
                    row.ToastOwner = owner;
                }
            }
        }

        return (rows, skipped, (int)Math.Max(0, Math.Min(int.MaxValue, total - emptied)));
    }

    /// <summary>The tables that own TOAST relations, as <c>schema.table</c> (#8814). An autovacuum cancel's CONTEXT
    /// names a TOAST relation by number only, with the database it lives in. This is the catalog read that tells a
    /// reader which table it belongs to, and it answers only for the database the answer is read from: a relation the
    /// log names under another database has no entry here, because this catalog cannot say what it is (#8822). A relation
    /// dropped since is not in the map either. Columns: the relation as the log writes it, then its owner.</summary>
    public const string ToastOwnerSql = @"
SELECT w.db || '.pg_toast.' || t.relname,
       format('%I.%I', n.nspname, o.relname)
FROM unnest($1::text[], $2::text[]) AS w(db, relname)
JOIN pg_catalog.pg_class AS t ON t.relname = w.relname
JOIN pg_catalog.pg_namespace AS tn ON tn.oid = t.relnamespace AND tn.nspname = 'pg_toast'
JOIN pg_catalog.pg_class AS o ON o.reltoastrelid = t.oid
JOIN pg_catalog.pg_namespace AS n ON n.oid = o.relnamespace
WHERE w.db = current_database()";

    private static async Task<Dictionary<string, string>> GetToastOwnersAsync(
        NpgsqlDataSource postgres,
        IReadOnlyCollection<StoreLogClassifier.ToastRelationRef> toastRelations,
        CancellationToken cancellationToken)
    {
        var owners = new Dictionary<string, string>(StringComparer.Ordinal);
        await using var command = postgres.CreateCommand(ToastOwnerSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(toastRelations.Select(r => r.Database!).ToArray());
        command.Parameters.AddWithValue(toastRelations.Select(r => r.Name).ToArray());

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            owners[reader.GetString(0)] = reader.GetString(1);
        }

        return owners;
    }

    /// <summary>
    /// The way on from a retained page cut for the budget (#8568). The rows keep one order for a window, so an offset
    /// past the rows returned reads on from the last of them, inside a capture as well as across captures: a smaller
    /// limit or a shorter hours_back returns the head of the page again. The window has to stay where it is for the
    /// offset to count the same rows, so the note holds as_of at the answer's own. The rows are newest capture first,
    /// so an earlier window end reaches the older captures. Every message of one capture carries that capture's time
    /// and the window includes its end, so when the rows kept are all from one capture a window ending there returns
    /// them again: the offset is then the way on, ahead of the previews, which fit more of that capture's messages,
    /// and a window that ends before it. With no row kept the offset has nothing to move past.
    /// </summary>
    public static string RetainedCutRemedy(IReadOnlyList<RetainedEvent> retained, int kept, bool fullText, int offset)
    {
        if (kept == 0)
        {
            return fullText
                ? "Omit full_text to read the messages as 400-character previews."
                : $"Pass an as_of earlier than {retained[0].LastCaptureAt} to read the messages captured before the newest one.";
        }

        var oldest = retained[kept - 1].LastCaptureAt;
        if (!string.Equals(retained[0].LastCaptureAt, oldest, StringComparison.Ordinal))
        {
            return string.Create(CultureInfo.InvariantCulture,
                $"Pass offset={offset + kept} with as_of at this answer's as_of to read on from the last row returned, or as_of at oldest_returned_last_capture_at with no offset to read on from that time.");
        }

        return string.Create(CultureInfo.InvariantCulture,
                $"Every message returned was last captured at oldest_returned_last_capture_at, and a window ending there returns them again. Pass offset={offset + kept} with as_of at this answer's as_of to read on from the last row returned. ")
            + (fullText ? "Omitting full_text fits more of that capture's messages as 400-character previews, and an" : "An")
            + " as_of earlier than it reads the captures before it.";
    }

    /// <summary>Naive-UTC store value to an unambiguous ISO string, the same shape every other read
    /// emits.</summary>
    private static string Iso(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);
}
