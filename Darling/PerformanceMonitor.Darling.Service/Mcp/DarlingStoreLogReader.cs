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
using System.Text;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

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
    WHERE c.capture_time >= $1
    AND   c.capture_time <= $2
),
newest AS
(
    /* Pending bytes are a LEVEL, not a total: summing them over the window would add the same backlog once
       per capture. Only the newest capture's figure means anything, so only it is read. */
    SELECT coalesce(sum(n.bytes_pending), 0)::bigint AS bytes_pending
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
    coalesce(array_agg(DISTINCT w.log_file ORDER BY w.log_file), '{}'::text[]) AS log_files
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
    WHERE c.capture_time >= $1
    AND   c.capture_time <= $2
),
hourly AS
(
    SELECT
        e.event_class                             AS event_class,
        date_trunc('hour', e.capture_time)        AS bucket,
        sum(e.occurrences)::bigint                AS occurrences
    FROM collect.store_log_events AS e
    WHERE e.capture_time >= $1
    AND   e.capture_time <= $2
    GROUP BY e.event_class, date_trunc('hour', e.capture_time)
),
severities AS
(
    SELECT
        e.event_class                                         AS event_class,
        array_agg(DISTINCT e.severity ORDER BY e.severity)    AS severities
    FROM collect.store_log_events AS e
    WHERE e.capture_time >= $1
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
    /// verbatim entry as the evidence. $1/$2 window (naive UTC), $3 row cap.
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
    (array_agg(e.sample_line ORDER BY e.capture_time DESC))[1] AS sample_line
FROM collect.store_log_events AS e
WHERE e.capture_time >= $1
AND   e.capture_time <= $2
AND   e.message_text IS NOT NULL
GROUP BY e.event_class, e.severity, e.message_text
ORDER BY max(e.capture_time) DESC, sum(e.occurrences) DESC
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

    /// <summary>One retained message, with one verbatim entry as its evidence.</summary>
    public sealed class RetainedEvent
    {
        [JsonPropertyName("event_class")] public string EventClass { get; init; } = string.Empty;

        [JsonPropertyName("severity")] public string Severity { get; init; } = string.Empty;

        [JsonPropertyName("message_text")] public string MessageText { get; init; } = string.Empty;

        [JsonPropertyName("occurrences")] public long Occurrences { get; init; }

        [JsonPropertyName("first_capture_at")] public string? FirstCaptureAt { get; init; }

        [JsonPropertyName("last_capture_at")] public string? LastCaptureAt { get; init; }

        /// <summary>The entry as the server wrote it, prefix and continuations included. The classified
        /// fields are an interpretation; this is the evidence, and it carries the server's own timestamp in
        /// the server's own <c>log_timezone</c> — uninterpreted, on purpose.</summary>
        [JsonPropertyName("sample_line")] public string? SampleLine { get; init; }
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

    /// <summary>What one <c>get_store_log</c> answer is.</summary>
    public sealed class StoreLogReport
    {
        [JsonPropertyName("window_hours")] public int WindowHours { get; init; }

        [JsonPropertyName("as_of")] public string AsOf { get; init; } = string.Empty;

        [JsonPropertyName("capture_summary")] public CaptureSummary Captures { get; init; } = new();

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

        note.Append(CultureInfo.InvariantCulture,
            $"{report.RetainedEvents.Count:N0} distinct message(s) are retained with their text. ");

        if (summary.MessagesFolded > 0)
        {
            note.Append(string.Format(
                CultureInfo.InvariantCulture,
                "{0:N0} further distinct message(s) were folded into their class's count by the per-class budget "
                + "of {1}; their occurrences are counted, their individual texts are not kept. ",
                summary.MessagesFolded,
                StoreLogClassifier.MaxRetainedGroupsPerClass));
        }

        if (summary.OffsetResets > 0)
        {
            note.Append(string.Format(
                CultureInfo.InvariantCulture,
                "{0:N0} capture(s) discarded a resume marker because the weekday log ring truncated the file "
                + "underneath it, so those captures cover an interval the previous ones did not. ",
                summary.OffsetResets));
        }

        if (summary.BytesPending > 0)
        {
            note.Append(string.Format(
                CultureInfo.InvariantCulture,
                "{0:N0} byte(s) were still unread when the newest capture finished; the next capture reads them. ",
                summary.BytesPending));
        }

        note.Append("No band is applied. Whether a class moved is its count read against the per-hour median "
            + "beside it, which is a comparison rather than a verdict: a quiet store with a large cancel floor "
            + "is healthy and has to keep reading that way.");

        return note.ToString();
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

    /// <summary>Reads the retained messages for the window.</summary>
    public static async Task<List<RetainedEvent>> GetRetainedEventsAsync(
        NpgsqlDataSource postgres,
        DateTime windowStartUtc,
        DateTime windowEndUtc,
        int limit,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<RetainedEvent>();

        await using var command = postgres.CreateCommand(RetainedEventsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(DateTime.SpecifyKind(windowStartUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(windowEndUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new RetainedEvent
            {
                EventClass = reader.GetString(0),
                Severity = reader.GetString(1),
                MessageText = reader.GetString(2),
                Occurrences = reader.GetInt64(3),
                FirstCaptureAt = reader.IsDBNull(4) ? null : Iso(reader.GetDateTime(4)),
                LastCaptureAt = reader.IsDBNull(5) ? null : Iso(reader.GetDateTime(5)),
                SampleLine = reader.IsDBNull(6) ? null : reader.GetString(6),
            });
        }

        return rows;
    }

    /// <summary>Naive-UTC store value to an unambiguous ISO string, the same shape every other read
    /// emits.</summary>
    private static string Iso(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);
}
