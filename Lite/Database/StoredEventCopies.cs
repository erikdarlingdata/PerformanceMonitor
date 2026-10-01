/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;
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
/// <para>The rule: for each exact identity, every row of the first batch that stored it stays, and the copies a
/// later batch stored go. That is DENSE_RANK() OVER (PARTITION BY identity ORDER BY collection_time) = 1, written
/// as collection_time = MIN(collection_time) over the identity. A collector stamps one collection_time on a whole
/// run, strictly later than its previous run's (CollectionTimeClock.NextStrictlyAfter), so a copy from a later
/// cycle always has a later collection_time, while identical rows from one batch share theirs and all stay.</para>
///
/// <para>The rule runs after the read's own filter, not in the archive view: collection_time is not part of an
/// event's identity, so in the view a read's collection_time filter could not run below the window, and the window
/// would cover the whole archive on every read. A read that filters on event_time needs nothing more: a copy keeps
/// its first copy's event_time, so both are inside the filter or both are outside it. A read that filters on
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
    /* Each identity is the row's exact identity: its server, its event time and the event's own text, compared
       ordinally (no hash, so a collision can never hide a row). A row with no usable identity is never collapsed:
       its CASE parts add the row's own id and collection_time to the key for it alone, and are NULL (one shared
       group) for every other row. */
    private const string BlockedProcessReportIdentity = "server_id, event_time, blocked_process_report_xml, "
        + "CASE WHEN blocked_process_report_xml IS NULL OR blocked_process_report_xml = '' OR event_time IS NULL THEN blocked_report_id END, "
        + "CASE WHEN blocked_process_report_xml IS NULL OR blocked_process_report_xml = '' OR event_time IS NULL THEN collection_time END";

    /* A long query completion stores no event XML: its statement text, session and XE event_sequence stand in for it. */
    private const string LongQueryCompletionIdentity = "server_id, database_name, event_time, statement_text, session_id, event_sequence, "
        + "CASE WHEN statement_text IS NULL OR statement_text = '' OR event_time IS NULL THEN long_query_completion_id END, "
        + "CASE WHEN statement_text IS NULL OR statement_text = '' OR event_time IS NULL THEN collection_time END";

    private const string SystemHealthEventIdentity = "server_id, event_time, event_xml, "
        + "CASE WHEN event_xml IS NULL OR event_xml = '' OR event_time IS NULL THEN system_health_event_id END, "
        + "CASE WHEN event_xml IS NULL OR event_xml = '' OR event_time IS NULL THEN collection_time END";

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

    /* A parenthesized relation for a FROM clause, so a caller adds its own alias. */
    private static string Read(string view, string identity, string where, string? collectedFrom)
    {
        var rule = $"QUALIFY collection_time = MIN(collection_time) OVER (PARTITION BY {identity})";
        if (collectedFrom is null)
        {
            return $"(SELECT * FROM {view} WHERE ({where}) {rule})";
        }

        var lookBack = ((long)CollectorContext.EventFallbackWindow.TotalSeconds).ToString(CultureInfo.InvariantCulture);
        return $"(SELECT * FROM (SELECT * FROM {view} WHERE ({where}) "
            + $"AND collection_time >= CAST({collectedFrom} AS TIMESTAMP) - INTERVAL {lookBack} SECOND {rule}) "
            + $"WHERE collection_time >= {collectedFrom})";
    }
}
