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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Where the data starts, for the server page's grids (#4966). A grid or a list over a window hides a short history:
/// a server added two days ago shows two days of rows under "last 7 days" with nothing saying so. This asks
/// <see cref="DataWindowFloor"/> where the read's own table starts covering the server and, when that is after the
/// window's start, adds the same three fields the tools that already carry the window floor use
/// (<c>window_truncated</c>, <c>effective_start</c>, <c>truncation_note</c>) to the response the page reads.
///
/// <para><b>Why in the web layer.</b> The server page reads <c>/api/read/{tool}</c>, which returns the tool's own
/// payload. The tools are shared with the MCP host, which gets its own notices later; this wraps the web mirror only,
/// so no tool's payload changes. A read the tool already answers with <c>window_truncated</c> (the Queries tab's
/// grids) is left as the tool wrote it.</para>
///
/// <para><b>Which reads.</b> Only a grid over one collector table whose rows are stamped with the collection time, the
/// change histories over the config snapshot tables they diff, the memory grant and plan correction reads over their snapshot
/// tables, or the collection log: <see cref="TableByRead"/>. The memory grant reads answer a window aggregate and the newest
/// snapshot in one payload; the note is about the aggregate, and the page opts the newest-snapshot panels out. A chart whose time axis spans the asked range already shows the empty span, a read of
/// the newest snapshot has no window to cut, and an event surface (blocked process reports, deadlocks, system health
/// events, the default trace) filters on the event's own time, which can reach before the first collection, so a
/// coverage start could name a time later than the history it shows. Those are not in this list.</para>
///
/// <para><b>A capped list.</b> Four listed reads are LISTS over time, newest first, under a row cap:
/// <c>get_waiting_tasks</c>, <c>get_collection_log</c>, <c>get_pg_server_config_changes</c> and <c>get_plan_corrections</c>
/// (<see cref="NewestFirstCappedReads"/>). When one hits its cap the grid ends at the oldest row the read returned,
/// whatever the store covers, so the note names that row (<c>effective_start</c> is the answer's
/// <c>oldest_returned_collection_time</c>, or for the configuration changes the earliest <c>changed_at</c> on the page,
/// and the text says the grid shows the newest rows back to it). The answer already carries the time, so this asks the
/// store nothing. The note shows only when that oldest row is later than the window's start, by any amount: a page
/// whose oldest row is at the start, or before it, shows the whole range and says nothing. The collection log keeps
/// the coverage rule when a duration floor ranks its page slowest first, as it
/// is then a sample of the whole window. The other reads keep the coverage rule: aggregates over the whole window,
/// whose cap keeps the top rows and hides no time range; <c>get_pg_predicate_stats</c>, a list ranked by something
/// other than time; and the SQL Server change histories, which carry no cap.</para>
///
/// <para><b>The instants as fields.</b> The note's sentence names its times in UTC. The page prints every time in the
/// browser's zone, so the answer also carries the instants (<see cref="DataStartField"/> or
/// <see cref="OldestShownField"/>, <see cref="WindowStartField"/>, <see cref="WindowEndField"/>) and the page composes
/// the note from them in its own clock. The sentence stays for any other reader.</para>
///
/// <para><b>When it says nothing.</b> The window is covered (whether or not it holds rows: a quiet start is not a
/// cut) and the read did not hit a row cap that cuts by time (or hit it, and its oldest row is at or before the
/// window's start), nothing in scope holds a row or logged a run in it, the
/// answer is an envelope other than the read's own "looked and found nothing" word (<see cref="NothingFoundStatusByRead"/>:
/// <c>empty</c>, or <c>no_changes</c> for the PostgreSQL changes, which are probed like rows; unavailable (the memory grant reads' no-snapshot word), not_collected and
/// invalid are not) or an error rather than rows, the read cannot be resolved, the
/// window is no longer than the 90-minute slack (a window that short can never be cut by the store's coverage, so the
/// probe is not asked), or the probe fails. A failed probe costs the grid its notice, never its rows.</para>
/// </summary>
internal static class WebDataStartNote
{
    /// <summary>
    /// The grid reads that carry the notice, each over the one collector table it reads. A closed list: each name is
    /// a read <c>BuildReadDispatch</c> serves, each table one <see cref="DataWindowFloor.Source.TryForCollectorTable"/>
    /// can probe, and a test holds both.
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, string> TableByRead = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["get_waiting_tasks"] = "waiting_tasks",
        ["get_latch_stats"] = "latch_stats",
        ["get_spinlock_stats"] = "spinlock_stats",
        ["get_wait_stats"] = "wait_stats",
        ["get_pg_wait_stats"] = "pg_wait_stats",
        ["get_pg_wait_sampling"] = "pg_wait_sampling",
        ["get_pg_kernel_stats"] = "pg_kernel_stats",
        ["get_pg_lock_stats"] = "pg_lock_stats",
        ["get_pg_predicate_stats"] = "pg_predicate_stats",

        /* The change histories (#4966): each diffs the snapshots a config collector writes, so the rows' own times
           can never come before the table's coverage, and the notice says how far back the snapshots go. The
           trace-flag history is the third grid on the SQL Server Config Changes tab, and the PostgreSQL one is the
           Configuration tab's Changes grid. */
        ["get_server_config_changes"] = "server_config",
        ["get_database_config_changes"] = "database_config",
        ["get_trace_flag_changes"] = "trace_flags",
        ["get_pg_server_config_changes"] = "pg_server_config",

        /* The raw run log under Collection Health on a SQL Server page and on Overview for PostgreSQL (#4966): not a
           collector table, so its source is the collection log's own (TryGetSource). */
        ["get_collection_log"] = CollectionLogTable,

        /* The SQL Server snapshot reads (#4966). Memory grants and the resource semaphore read the one memory_grant_stats
           table; their page opts the newest-snapshot halves out (windowNote:false) and keeps the note on the window
           aggregates. Plan corrections list recommendations by capture time, newest first. */
        ["get_memory_grants"] = "memory_grant_stats",
        ["get_resource_semaphore"] = "memory_grant_stats",
        ["get_plan_corrections"] = "plan_correction",
    };

    /// <summary>
    /// The name <see cref="TableByRead"/> gives the collection log. It is not a collector table
    /// (<see cref="DataWindowFloor.Source.TryForCollectorTable"/> refuses it), so <see cref="TryGetSource"/> answers
    /// it with <see cref="DataWindowFloor.Source.ForCollectionLog"/>: its edge is the log's own fixed horizon.
    /// </summary>
    internal const string CollectionLogTable = "collection_log";

    /// <summary>
    /// The probe source for a table <see cref="TableByRead"/> names: the collection log's own source for
    /// <see cref="CollectionLogTable"/>, the collector table's for every other name, false for a table the probe
    /// cannot read by index.
    /// </summary>
    internal static bool TryGetSource(string table, out DataWindowFloor.Source source)
    {
        if (string.Equals(table, CollectionLogTable, StringComparison.Ordinal))
        {
            source = DataWindowFloor.Source.ForCollectionLog();
            return true;
        }

        return DataWindowFloor.Source.TryForCollectorTable(table, out source);
    }

    /// <summary>
    /// The <c>status</c> words a listed read answers its ROWS with. Any other <c>status</c> is an envelope
    /// (<c>empty</c>, <c>unavailable</c>, <c>invalid</c>) and is left as it is. <c>get_pg_server_config_changes</c>
    /// names its page <c>config_changes</c>, and the page reads an answer as an envelope only when it carries a
    /// <c>message</c> too.
    /// </summary>
    private static readonly HashSet<string> RowStatuses = new(StringComparer.Ordinal) { "config_changes" };

    /// <summary>
    /// The one <c>status</c> word each listed read answers with when it LOOKED and found nothing in the window (#4966): the
    /// word a reader takes for "nothing happened". Over a short history an empty span reads as quiet when it is only short, and
    /// empty is these grids' usual state, so an answer carrying the word goes through the coverage probe and gets the same
    /// note rows do. Every other word (<c>unavailable</c>, <c>not_collected</c>, <c>invalid</c>) keeps its own message and gets
    /// none: it says something other than "nothing happened".
    ///
    /// <para>The change histories and the collection log answer <c>empty</c> (the PostgreSQL changes read says
    /// <c>no_changes</c>), as do waiting tasks, plan corrections, wait sampling, kernel stats, lock stats and predicate stats: each ran its
    /// query and found no rows in the window. The collection log's <c>empty</c> covers a quiet window and a filter that matched
    /// nothing, and the coverage fact holds for both; its <c>unavailable</c> (never collected) stays as it is. Latch stats,
    /// spinlock stats, wait stats and PostgreSQL wait events are left out: their no-rows answer is <c>unavailable</c>. A test
    /// reads each tool's source and holds both halves.</para>
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, string> NothingFoundStatusByRead = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["get_waiting_tasks"] = "empty",
        ["get_pg_wait_sampling"] = "empty",
        ["get_pg_kernel_stats"] = "empty",
        ["get_pg_lock_stats"] = "empty",
        ["get_pg_predicate_stats"] = "empty",
        ["get_server_config_changes"] = "empty",
        ["get_database_config_changes"] = "empty",
        ["get_trace_flag_changes"] = "empty",
        ["get_pg_server_config_changes"] = "no_changes",
        ["get_collection_log"] = "empty",
        ["get_plan_corrections"] = "empty",
    };

    /// <summary>
    /// The listed reads whose tool takes any window length (<see cref="McpHelpers.ValidateUncappedWindow"/>), not the
    /// 168-hour ceiling: the collection log keeps 60 days, and its tool exists to look further back than the other
    /// reads allow, so a note that checked the shared ceiling would stay silent on exactly those windows.
    /// </summary>
    private static readonly HashSet<string> UncappedWindowReads = new(StringComparer.Ordinal) { "get_collection_log" };

    /// <summary>
    /// A window end as an <c>as_of</c> anchor the tools parse back to the same instant (to the millisecond): the web route
    /// takes the end of a newest-first capped read once and hands it to the read and to <see cref="AddAsync"/>.
    /// </summary>
    internal static string FormatWindowEnd(DateTime endUtc) =>
        endUtc.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture);

    private static string? ValidateWindowFor(string tool, int hours, string? asOf, out DateTime endUtc) =>
        UncappedWindowReads.Contains(tool)
            ? McpHelpers.ValidateUncappedWindow(hours, asOf, out endUtc)
            : McpHelpers.ValidateWindow(hours, asOf, out endUtc);

    /// <summary>
    /// The listed reads that LIST rows newest first under a row cap, so a capped answer ends at a time inside the
    /// window (<c>get_waiting_tasks</c>: <c>ORDER BY collection_time DESC</c>, the cap read as <c>truncated</c> and
    /// the end of the shown rows as <c>oldest_returned_collection_time</c>); <c>get_plan_corrections</c> pages its
    /// recommendations the same way (<c>ORDER BY collection_time DESC</c>, the cap read as <c>truncated</c>, the same
    /// oldest field and order word). Every other listed read is an
    /// aggregate over the whole window or a list ranked by something other than time, whose cap hides no time range;
    /// a <c>truncated</c> flag on those answers is about rows kept by rank and does not name a time. A test holds
    /// the list to these four reads, each name one <see cref="TableByRead"/> lists.
    /// </summary>
    internal static readonly IReadOnlySet<string> NewestFirstCappedReads = new HashSet<string>(StringComparer.Ordinal)
    {
        "get_waiting_tasks",
        "get_collection_log",
        "get_pg_server_config_changes",
        "get_plan_corrections",
    };

    /// <summary>
    /// <paramref name="result"/> with the notice fields added when <paramref name="tool"/> is a listed grid read whose
    /// window starts before its table's coverage, or is a newest-first list (<see cref="NewestFirstCappedReads"/>) that
    /// hit its row cap; otherwise <paramref name="result"/> itself, untouched.
    /// <paramref name="hoursBack"/> is the window the page asked for (null or below 1: none was asked), and
    /// <paramref name="asOf"/> the request's window anchor.
    /// </summary>
    internal static async Task<string> AddAsync(
        NpgsqlDataSource postgres, string tool, string? server, int? hoursBack, string? asOf, string result,
        ILogger? logger, CancellationToken cancellationToken)
    {
        if (!TableByRead.TryGetValue(tool, out var table)
            || string.IsNullOrWhiteSpace(server)
            || hoursBack is not int hours
            || hours < 1
            || !TryGetSource(table, out var source))
        {
            return result;
        }

        JsonObject? payload;
        try
        {
            payload = JsonNode.Parse(result) as JsonObject;
        }
        catch (System.Text.Json.JsonException)
        {
            return result;
        }

        /* Rows, or the answer that says the read looked and found nothing (#4966, NothingFoundStatusByRead): an empty span
           over a short history reads as "nothing happened" when it is only short, so that answer gets the note too. The
           probe counts a server only when it holds a row or logged a run in the window, so a server that did neither is
           left without one, as before. Any other envelope (unavailable, not_collected, invalid), an error, or a tool that
           already reports its own window floor is left as it is. */
        if (payload is null
            || (payload.ContainsKey("status") && !IsRowStatus(payload) && !IsNothingFoundStatus(tool, payload))
            || payload.ContainsKey("error")
            || payload.ContainsKey("window_truncated"))
        {
            return result;
        }

        /* A newest-first list that hit its row cap (#4966): the grid ends at the oldest row the read returned, which
           the store's coverage cannot move, so the note names that row. The answer carries it, so no probe: a store
           that covers the whole range still gets the note, and a store that does not names the same row, never an
           earlier one the grid does not show. */
        if (NewestFirstCappedReads.Contains(tool) && TryReadCappedStart(tool, payload, out var oldestShown, out var oldestText))
        {
            if (ValidateWindowFor(tool, hours, asOf, out var cappedEnd) is not null)
            {
                return result;
            }

            /* Only a page that stops short of the window's start is cut by its row cap. One whose oldest row is at the
               start, or before it, shows the whole range, so it says nothing: strictly later, with no slack (the
               90-minute slack belongs to the coverage rule below), as Lite's capped grids judge it
               (ServerTab.ApplyCappedWindowFloorToBanner). */
            var cappedStart = cappedEnd.AddHours(-hours);
            if (oldestShown <= cappedStart)
            {
                return result;
            }

            payload["window_truncated"] = true;
            payload["effective_start"] = oldestText;
            payload["truncation_note"] = ComposeStoreAvailability.BuildCappedListNotice(oldestShown, cappedStart, cappedEnd);
            AddInstants(payload, OldestShownField, oldestShown, cappedStart, cappedEnd);
            return payload.ToJsonString();
        }

        /* A window no longer than the slack can never be called cut by the store: the coverage rule needs a start more
           than RawWindowFloor's 90 minutes after the window's own, and the probe reports none at or after the window's
           end, so a window of an hour or less always comes back covered. Skip the registry read and the floor read that
           would say so (the capped list above does not use the probe and keeps its note at any length). */
        if (TimeSpan.FromHours(hours) <= DurationTrendRouting.TruncationSlack)
        {
            return result;
        }

        try
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server, cancellationToken);
            if (error is not null || ValidateWindowFor(tool, hours, asOf, out var windowEnd) is not null)
            {
                return result;
            }

            var windowStart = windowEnd.AddHours(-hours);
            var dataStart = await DataWindowFloor.GetAsync(
                postgres, [source], [resolved.ServerName], windowStart, windowEnd, StorageCommandDeadlines.McpReadSeconds, cancellationToken);
            if (!RawWindowFloor.IsTruncated(dataStart, windowStart))
            {
                return result;
            }

            payload["window_truncated"] = true;
            payload["effective_start"] = dataStart!.Value.ToString("o", CultureInfo.InvariantCulture);
            payload["truncation_note"] = ComposeStoreAvailability.BuildDataStartNotice(dataStart.Value, windowStart, windowEnd);
            AddInstants(payload, DataStartField, dataStart.Value, windowStart, windowEnd);
            return payload.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Debug, like the Custom Views runner's probe: the grid is still answered, and this runs once per grid
               refresh, so a standing fault at Warning would put a line in the log on every one. */
            logger?.LogDebug(ex, "The data-start probe for {Tool} failed; the grid is answered without a partial-window notice.", tool);
            return result;
        }
    }

    /// <summary>Where the data starts, as a UTC instant: the coverage note's first field (<see cref="AddAsync"/>).</summary>
    internal const string DataStartField = "data_start_utc";

    /// <summary>The oldest row a capped list shows, as a UTC instant: the capped note's first field.</summary>
    internal const string OldestShownField = "oldest_shown_utc";

    /// <summary>The window's start, as a UTC instant: on both notes.</summary>
    internal const string WindowStartField = "window_start_utc";

    /// <summary>The window's end, as a UTC instant: on both notes.</summary>
    internal const string WindowEndField = "window_end_utc";

    /// <summary>
    /// The data-start sentence a Custom Views panel's answer carries (#4966), the one <c>notice</c> holds, so the page
    /// can write that sentence again in the browser's zone and leave any row-cap sentence beside it as it is.
    /// </summary>
    internal const string DataStartNoteField = "data_start_note";

    /// <summary>
    /// The same three instants a grid's coverage note carries (<see cref="DataStartField"/>, <see cref="WindowStartField"/>,
    /// <see cref="WindowEndField"/>), added to a Custom Views panel's answer beside <c>notice</c> (#4966). The panel's
    /// <c>notice</c> names its times in UTC for any other reader; the page composes it again from these in the clock its
    /// own times are printed in. <paramref name="sentence"/> is the data-start sentence inside <c>notice</c>.
    /// </summary>
    internal static void AddComposedPanelInstants(
        JsonObject payload, string sentence, DateTime dataStartUtc, DateTime windowStartUtc, DateTime windowEndUtc)
    {
        payload[DataStartNoteField] = sentence;
        AddInstants(payload, DataStartField, dataStartUtc, windowStartUtc, windowEndUtc);
    }

    /// <summary>
    /// The note's instants as fields beside its sentence (#4966). The sentence names them in UTC, which is all an
    /// MCP client or any other reader of the tool's answer can use; the page prints every time in the browser's zone
    /// (<c>localTime</c>) and so composes the note again from these (<c>util.js</c>, the way <c>keptWindowStrip</c>
    /// composes its strip from <c>keptHours</c>), so a note above a grid reads in the grid's own clock. Each is an
    /// ISO instant with an explicit <c>Z</c>. A page that finds one missing draws the sentence as sent.
    /// </summary>
    private static void AddInstants(JsonObject payload, string startField, DateTime start, DateTime windowStart, DateTime windowEnd)
    {
        payload[startField] = Instant(start);
        payload[WindowStartField] = Instant(windowStart);
        payload[WindowEndField] = Instant(windowEnd);
    }

    /* Every instant here is UTC: the store's times carry no zone, and the window comes from DateTime.UtcNow or the
       request's as_of. Stamped Utc, so the "o" form ends in Z. */
    private static string Instant(DateTime utc) =>
        DateTime.SpecifyKind(utc, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);

    /// <summary>
    /// Whether a newest-first list's answer says its row cap cut it (<c>truncated: true</c>), and the time of the
    /// oldest row it returned (<c>oldest_returned_collection_time</c>, as the tool wrote it, and read as UTC: the
    /// store's times carry no zone). False for an answer that did not hit its cap, or that names no readable time:
    /// those take the coverage rule.
    /// </summary>
    private static bool TryReadCappedStart(string tool, JsonObject payload, out DateTime oldestShownUtc, out string oldestText)
    {
        oldestShownUtc = default;
        oldestText = string.Empty;

        if (payload["truncated"] is not JsonValue cap
            || !cap.TryGetValue<bool>(out var hitCap)
            || !hitCap)
        {
            return false;
        }

        /* A read that can rank its page by something other than time says which order it answered in: a page ranked
           slowest first (get_collection_log with a duration floor) is a sample of the whole window, so its oldest row
           names no reach and the coverage rule applies. A page with no order field is the time-ordered one. */
        if (payload["order"] is JsonValue order
            && order.TryGetValue<string>(out var orderText)
            && !string.Equals(orderText, McpHelpers.CollectionLogOrderNewestFirst, StringComparison.Ordinal))
        {
            return false;
        }

        string? text;
        if (string.Equals(tool, PgConfigChangesRead, StringComparison.Ordinal))
        {
            /* The changes page names no oldest-returned field: its rows are the changes, newest first, each stamped
               changed_at, so the oldest row it returned is the earliest of those. */
            text = OldestChangeTime(payload);
        }
        else
        {
            text = payload["oldest_returned_collection_time"] is JsonValue oldest && oldest.TryGetValue<string>(out var read) ? read : null;
        }

        if (text is null
            || !DateTime.TryParse(
                text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out oldestShownUtc))
        {
            return false;
        }

        oldestText = text;
        return true;
    }

    /// <summary>The PostgreSQL configuration changes read: a newest-first list of changes under <c>limit</c>, whose answer
    /// says it was cut (<c>truncated</c>) but carries its rows' times only on the rows (<c>changes[].changed_at</c>).</summary>
    internal const string PgConfigChangesRead = "get_pg_server_config_changes";

    /* Whether the answer's status word is one a listed read puts on its ROWS (RowStatuses) rather than an envelope. */
    private static bool IsRowStatus(JsonObject payload) =>
        payload["status"] is JsonValue word && word.TryGetValue<string>(out var status) && RowStatuses.Contains(status);

    /* Whether the answer's status word is this read's own "looked and found nothing" one (NothingFoundStatusByRead). */
    private static bool IsNothingFoundStatus(string tool, JsonObject payload) =>
        NothingFoundStatusByRead.TryGetValue(tool, out var expected)
        && payload["status"] is JsonValue word
        && word.TryGetValue<string>(out var status)
        && string.Equals(status, expected, StringComparison.Ordinal);

    /* The earliest changed_at on the page, as the tool wrote it; null when no row carries a readable one. Compared as
       instants (the store's times carry no zone, and are read as UTC), and returned as the text of the earliest. */
    private static string? OldestChangeTime(JsonObject payload)
    {
        if (payload["changes"] is not JsonArray changes)
        {
            return null;
        }

        string? oldestText = null;
        DateTime oldest = default;
        foreach (var change in changes)
        {
            if (change?["changed_at"] is JsonValue value
                && value.TryGetValue<string>(out var text)
                && DateTime.TryParse(
                    text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var at)
                && (oldestText is null || at < oldest))
            {
                oldestText = text;
                oldest = at;
            }
        }

        return oldestText;
    }
}
