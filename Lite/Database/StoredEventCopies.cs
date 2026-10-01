/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;
using System.Linq;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitorLite.Database;

/// <summary>
/// Reads of the three event tables whose rows a later collection cycle can store again: blocked process reports,
/// long query completions and system_health events. Builds that read the watermark from the live table alone read
/// the collector's fallback window again after the 512 MB reset (ArchiveService.ArchiveAllAndResetAsync), and a
/// run whose watermark read fails still does, so the archive can hold an event's first copy and a later batch a
/// second one. Every read of these tables goes through here, except three a copy cannot change (a sweep test
/// pins the list), and each read drops those copies after its own filter.
///
/// <para>The rule: for each identity, every row of the first batch that stored it stays, and the copies a later
/// batch stored go. That is DENSE_RANK() OVER (PARTITION BY identity ORDER BY collection_time) = 1, computed as each
/// identity's MIN(collection_time), joined back to the rows on every part of the identity with IS NOT DISTINCT FROM,
/// so a NULL part matches a NULL part. A collector stamps one collection_time on a whole run, strictly later than
/// its previous run's (CollectionTimeClock.NextStrictlyAfter), so a copy from a later cycle always has a later
/// collection_time, while identical rows from one batch share theirs and all stay. The minimum is grouped, not a
/// window, because a window carries every row, its XML included, through the operator, while the grouped side holds
/// only the keys; that keeps a large read inside Lite's memory_limit.</para>
///
/// <para>The rule runs after the read's own filter, not in the archive view: collection_time is not part of an
/// event's identity, so in the view a read's collection_time filter could not run below the rule, and the rule would
/// cover the whole archive on every read. A read that filters on event_time needs nothing more: a copy keeps its
/// first copy's event_time, so both are inside the filter or both are outside it. A read that filters on
/// collection_time passes its lower bound as <c>collectedFrom</c>. The rule then also reads the
/// <see cref="CollectorContext.EventFallbackWindow"/> before that bound, so a copy stored inside the window whose
/// first copy was stored just before it goes too, and the first copy stays outside the window. A collector with no
/// watermark reads events back that far from its run's collection_time, so no first copy is further back, as long
/// as the monitored server's clock, which stamps event_time, is not ahead of the collector's. A server clock that is
/// ahead can put a first copy that much further back, and a copy stored just inside the window's start is then
/// read, though its first copy was stored before the window.</para>
/// </summary>
internal static class StoredEventCopies
{
    /* Each identity is the row's server, its event time and the event's own text. An event's XML is keyed by
       XmlKey, so the grouped side holds 8 bytes for it instead of the XML, which averages about 13,000 characters in
       a blocked process report. Every other part is compared exactly. */
    private static readonly string[] BlockedProcessReportIdentity = XmlEventIdentity("blocked_process_report_xml", "blocked_report_id");

    /* A long query completion stores no event XML: its statement text, session and XE event_sequence stand in for
       it, all compared exactly. */
    private static readonly string[] LongQueryCompletionIdentity =
        ["server_id", "database_name", "event_time", "statement_text", "session_id", "event_sequence",
         .. NeverCollapsed("statement_text", "long_query_completion_id")];

    private static readonly string[] SystemHealthEventIdentity = XmlEventIdentity("event_xml", "system_health_event_id");

    /* The one place an event's XML becomes its key: DuckDB's 64-bit hash() of the whole text. Two different events
       that share a server and event time merge only if their XML collides in 64 bits, about 2^-64 per pair. A merge
       hides one row from one read: nothing stores the key, and nothing is deleted. */
    private static string XmlKey(string xmlColumn) => $"hash({xmlColumn})";

    private static string[] XmlEventIdentity(string xmlColumn, string idColumn) =>
        ["server_id", "event_time", XmlKey(xmlColumn), .. NeverCollapsed(xmlColumn, idColumn)];

    /* A row with no usable identity (no text, or no event time) is never collapsed: these parts add the row's own
       id and collection_time to the key for it alone, and are NULL (one shared group) for every other row. They
       test the raw text, never its key, because hash(NULL) is not NULL. */
    private static string[] NeverCollapsed(string textColumn, string idColumn)
    {
        var unusable = $"{textColumn} IS NULL OR {textColumn} = '' OR event_time IS NULL";
        return [$"CASE WHEN {unusable} THEN {idColumn} END", $"CASE WHEN {unusable} THEN collection_time END"];
    }

    /// <summary>The blocked process reports that match <paramref name="where"/>, each stored event once.</summary>
    /// <param name="where">The read's own filter, without its collection_time lower bound when it has one.</param>
    /// <param name="collectedFrom">That lower bound (a parameter such as <c>$2</c>), or null for a read that does
    /// not filter on collection_time from below.</param>
    public static string BlockedProcessReports(string where, string? collectedFrom = null) =>
        Read("v_blocked_process_reports", BlockedProcessReportIdentity, where, collectedFrom);

    /// <summary>The long query completions that match <paramref name="where"/>, each stored event once.</summary>
    /// <param name="where">The read's own filter, without its collection_time lower bound when it has one.</param>
    /// <param name="collectedFrom">That lower bound (a parameter such as <c>$2</c>), or null for a read that does
    /// not filter on collection_time from below.</param>
    public static string LongQueryCompletions(string where, string? collectedFrom = null) =>
        Read("v_long_query_completions", LongQueryCompletionIdentity, where, collectedFrom);

    /// <summary>The system_health events that match <paramref name="where"/>, each stored event once.</summary>
    /// <param name="where">The read's own filter, without its collection_time lower bound when it has one.</param>
    /// <param name="collectedFrom">That lower bound (a parameter such as <c>$2</c>), or null for a read that does
    /// not filter on collection_time from below.</param>
    public static string SystemHealthEvents(string where, string? collectedFrom = null) =>
        Read("v_system_health_events", SystemHealthEventIdentity, where, collectedFrom);

    /* A parenthesized relation for a FROM clause, so a caller adds its own alias. The grouped side reads only the
       identity's keys and collection_time. The rows come from a second scan with the same filter, joined to the
       group on every key with IS NOT DISTINCT FROM and to its first batch on collection_time. */
    private static string Read(string view, string[] identity, string where, string? collectedFrom)
    {
        var keys = string.Join(", ", identity.Select((part, i) => $"{part} AS k{i}"));
        var names = string.Join(", ", identity.Select((_, i) => $"k{i}"));
        var on = string.Join(" AND ", identity.Select((part, i) => $"({part}) IS NOT DISTINCT FROM g.k{i}"));
        var lookBack = string.Empty;
        var from = string.Empty;
        if (collectedFrom is not null)
        {
            var seconds = ((long)CollectorContext.EventFallbackWindow.TotalSeconds).ToString(CultureInfo.InvariantCulture);
            lookBack = $" AND collection_time >= CAST({collectedFrom} AS TIMESTAMP) - INTERVAL {seconds} SECOND";
            from = $" AND collection_time >= {collectedFrom}";
        }

        return $"(SELECT v.* FROM {view} AS v JOIN (SELECT {names}, MIN(ct) AS first_ct FROM "
            + $"(SELECT {keys}, collection_time AS ct FROM {view} WHERE ({where}){lookBack}) GROUP BY {names}) AS g "
            + $"ON {on} AND v.collection_time = g.first_ct WHERE ({where}){from})";
    }
}
