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
/// <para><b>Which reads.</b> Only a grid over one collector table whose rows are stamped with the collection time:
/// <see cref="TableByRead"/>. A chart whose time axis spans the asked range already shows the empty span, a read of
/// the newest snapshot has no window to cut, and an event surface (blocked process reports, deadlocks, system health
/// events, the default trace) filters on the event's own time, which can reach before the first collection, so a
/// coverage start could name a time later than the history it shows. Those are not in this list.</para>
///
/// <para><b>A capped list.</b> One listed read is a LIST over time, newest first, under a row cap:
/// <c>get_waiting_tasks</c> (<see cref="NewestFirstCappedReads"/>). When it hits its cap the grid ends at the oldest
/// row the read returned, whatever the store covers, so the note names that row (<c>effective_start</c> is
/// <c>oldest_returned_collection_time</c>, and the text says the grid shows the newest rows back to it). The answer
/// already carries the time, so this asks the store nothing. The other eight reads keep the coverage rule: seven
/// are aggregates over the whole window, whose cap keeps the top rows and hides no time range, and
/// <c>get_pg_predicate_stats</c> is a list ranked by something other than time.</para>
///
/// <para><b>The instants as fields.</b> The note's sentence names its times in UTC. The page prints every time in the
/// browser's zone, so the answer also carries the instants (<see cref="DataStartField"/> or
/// <see cref="OldestShownField"/>, <see cref="WindowStartField"/>, <see cref="WindowEndField"/>) and the page composes
/// the note from them in its own clock. The sentence stays for any other reader.</para>
///
/// <para><b>When it says nothing.</b> The window is covered (whether or not it holds rows: a quiet start is not a
/// cut) and the read did not hit a row cap that cuts by time, nothing in scope holds a row or logged a run in it, the
/// answer is an envelope (empty, unavailable, invalid) or an error rather than rows, the read cannot be resolved, the
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
    };

    /// <summary>
    /// The listed reads that LIST rows newest first under a row cap, so a capped answer ends at a time inside the
    /// window (<c>get_waiting_tasks</c>: <c>ORDER BY collection_time DESC</c>, the cap read as <c>truncated</c> and
    /// the end of the shown rows as <c>oldest_returned_collection_time</c>). Every other listed read is an
    /// aggregate over the whole window or a list ranked by something other than time, whose cap hides no time range;
    /// a <c>truncated</c> flag on those answers is about rows kept by rank and does not name a time. A test holds
    /// the list to one read, each name one <see cref="TableByRead"/> lists.
    /// </summary>
    internal static readonly IReadOnlySet<string> NewestFirstCappedReads = new HashSet<string>(StringComparer.Ordinal)
    {
        "get_waiting_tasks",
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
            || !DataWindowFloor.Source.TryForCollectorTable(table, out var source))
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

        /* Rows only: an envelope (status), an error, or a tool that already reports its own window floor is left
           as it is. An empty answer says so itself, and the probe would count a server by its logged runs. */
        if (payload is null
            || payload.ContainsKey("status")
            || payload.ContainsKey("error")
            || payload.ContainsKey("window_truncated"))
        {
            return result;
        }

        /* A newest-first list that hit its row cap (#4966): the grid ends at the oldest row the read returned, which
           the store's coverage cannot move, so the note names that row. The answer carries it, so no probe: a store
           that covers the whole range still gets the note, and a store that does not names the same row, never an
           earlier one the grid does not show. */
        if (NewestFirstCappedReads.Contains(tool) && TryReadCappedStart(payload, out var oldestShown, out var oldestText))
        {
            if (McpHelpers.ValidateWindow(hours, asOf, out var cappedEnd) is not null)
            {
                return result;
            }

            payload["window_truncated"] = true;
            payload["effective_start"] = oldestText;
            payload["truncation_note"] = ComposeStoreAvailability.BuildCappedListNotice(oldestShown, cappedEnd.AddHours(-hours), cappedEnd);
            AddInstants(payload, OldestShownField, oldestShown, cappedEnd.AddHours(-hours), cappedEnd);
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
            if (error is not null || McpHelpers.ValidateWindow(hours, asOf, out var windowEnd) is not null)
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
    private static bool TryReadCappedStart(JsonObject payload, out DateTime oldestShownUtc, out string oldestText)
    {
        oldestShownUtc = default;
        oldestText = string.Empty;

        if (payload["truncated"] is not JsonValue cap
            || !cap.TryGetValue<bool>(out var hitCap)
            || !hitCap
            || payload["oldest_returned_collection_time"] is not JsonValue oldest
            || !oldest.TryGetValue<string>(out var text)
            || !DateTime.TryParse(
                text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out oldestShownUtc))
        {
            return false;
        }

        oldestText = text;
        return true;
    }
}
