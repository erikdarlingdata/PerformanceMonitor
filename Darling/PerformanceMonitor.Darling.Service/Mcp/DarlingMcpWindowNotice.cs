/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The three window-floor keys <see cref="DarlingMcpWindowNotice.Build"/> answers, as the values a tool writes into its
/// payload (#4966). <see cref="EffectiveStart"/> is null only for an empty answer over a window the store holds
/// nothing in: there is no start to name.
/// </summary>
internal readonly record struct McpWindowNotice(string? EffectiveStart, bool WindowTruncated, string? TruncationNote)
{
    /// <summary>
    /// The notice a failed coverage probe leaves (#4966): there is no verdict on the window, so a tool answers without the
    /// three keys (a data answer) or without hints (an empty one). A failed probe costs the notice, never the rows, the rule
    /// the web's own note follows. Not <see cref="WindowTruncated"/>, so a tool that writes its keys only for a cut window
    /// writes none.
    /// </summary>
    internal static McpWindowNotice Unavailable { get; } = new(null, false, null) { IsUnavailable = true };

    /// <summary>True for <see cref="Unavailable"/>: the probe failed, so no key and no hint may be written.</summary>
    internal bool IsUnavailable { get; init; }

    /// <summary>
    /// The same three keys, with the same values and wording, for an <c>empty</c> status, which carries them under
    /// <c>hints</c>: an empty answer over a window the store does not reach back to is not a true negative, and without
    /// them it read as one. A status that says the collector never ran (<c>not_collected</c>) does not carry them.
    /// A notice from a failed probe has no hints (null), which <see cref="McpHelpers.Status"/> leaves out.
    /// </summary>
    internal object? AsHints() => IsUnavailable ? null : new { effective_start = EffectiveStart, window_truncated = WindowTruncated, truncation_note = TruncationNote };
}

/// <summary>
/// The window-floor notice a time-ranged MCP tool writes beside <c>hours_back</c> (#4966): <c>effective_start</c>,
/// <c>window_truncated</c> and <c>truncation_note</c>, the shape and the wording Lite's tools carry
/// (<c>McpQueryTools.WindowNotice</c>), so a client reads one dialect from either app. The two sentences are copied
/// from Lite and held equal to it by a test.
///
/// <para>The coverage probe is <see cref="DataWindowFloor.GetAsync"/>, never <see cref="DataWindowFloor.GetForServerAsync"/>:
/// the latter answers null for any window of 90 minutes or less, and on an empty answer that null reads as "not
/// covered", so every default one-hour call that found nothing would say the store holds no collection.</para>
/// </summary>
internal static class DarlingMcpWindowNotice
{
    /// <summary>
    /// The notice for a probe's <paramref name="floor"/> (where coverage starts, null for none). On an empty answer
    /// (<paramref name="emptyAnswer"/>) a null floor means the store holds no collection for the server in the window,
    /// and the notice says NOT covered: <c>window_truncated</c> true, no <c>effective_start</c>, and a note that says
    /// so. On an answer WITH rows a null floor means covered, at the start that was asked for.
    /// <paramref name="tail"/> is one more sentence a truncated note carries.
    /// </summary>
    internal static McpWindowNotice Build(
        DateTime? floor, DateTime requestedStart, string table, string? tail = null, bool emptyAnswer = false, bool listOnly = false,
        bool storeSubject = false)
    {
        if (storeSubject)
        {
            return BuildForStore(floor, requestedStart, table, tail, emptyAnswer);
        }

        if (floor is null && listOnly)
        {
            /* The windowed list is empty but the answer carries other data (a latest-snapshot block), so it is not an empty answer. */
            return new McpWindowNotice(
                null,
                true,
                $"The store holds no collection of {table} for this server in this window, so no windowed rows were read, and a list with no rows is not a report that nothing happened. "
                    + "The window may reach further back than the store retains, this server may have been monitored for less time than that, or collection may have stopped; get_collection_health shows which.");
        }

        if (floor is null && emptyAnswer)
        {
            return new McpWindowNotice(
                null,
                true,
                $"The store holds no collection of {table} for this server in this window, so nothing was read, and this empty answer is not a report that nothing happened. "
                    + "The window may reach further back than the store retains, this server may have been monitored for less time than that, or collection may have stopped; get_collection_health shows which.");
        }

        var truncated = RawWindowFloor.IsTruncated(floor, requestedStart);
        return new McpWindowNotice(
            McpHelpers.FormatEffectiveStart(RawWindowFloor.EffectiveStart(floor, requestedStart)),
            truncated,
            truncated
                ? $"The window reaches further back than this server's raw {table} retains (or this server has been monitored for less time than that), so the older part of it was not read."
                    + (tail is null ? "" : " " + tail)
                : null);
    }

    /// <summary>
    /// <see cref="Build"/> for a tool whose subject is the STORE's own history (<c>storeSubject</c>), not a monitored server's
    /// collection: the same three keys, with wording that does not talk about "this server" or point at get_collection_health.
    /// </summary>
    private static McpWindowNotice BuildForStore(DateTime? floor, DateTime requestedStart, string table, string? tail, bool emptyAnswer)
    {
        if (floor is null && emptyAnswer)
        {
            return new McpWindowNotice(
                null,
                true,
                $"The store holds no {table} that read the extension in this window, so nothing was read, and this empty answer is not a report that nothing happened. "
                    + "The window may reach further back than the history exists, or snapshots may not have been taken yet.");
        }

        var truncated = RawWindowFloor.IsTruncated(floor, requestedStart);
        return new McpWindowNotice(
            McpHelpers.FormatEffectiveStart(RawWindowFloor.EffectiveStart(floor, requestedStart)),
            truncated,
            truncated
                ? $"The window reaches further back than the store's own {table} goes (its first snapshot is later than the start of the window), so the older part of it was not read."
                    + (tail is null ? "" : " " + tail)
                : null);
    }

    /// <summary>
    /// <see cref="Build"/> with its coverage probe, which a window no longer than
    /// <see cref="DurationTrendRouting.TruncationSlack"/> that answered rows never needs: the probe cannot find a floor
    /// later than the start by more than the slack, so the answer is covered whatever it would read. The probe is a
    /// delegate and is not started for such a window. <paramref name="listOnly"/> probes whatever the window, for an answer whose
    /// windowed list is empty but which carries other data (so it is not an <paramref name="emptyAnswer"/>). An EMPTY answer is always probed, whatever the window's length:
    /// nothing was read, so nothing else says the store held the window at all.
    ///
    /// <para>A probe that throws, other than because the CALLER cancelled, is logged at Warning and answers
    /// <see cref="McpWindowNotice.Unavailable"/>: the probe is a second read on a shared store, and it must not turn a page
    /// of rows that was already read into an error.</para>
    /// </summary>
    internal static async Task<McpWindowNotice> ReadAsync(
        Func<Task<DateTime?>> probe, DateTime requestedStart, DateTime windowEnd, string table, string? tail = null, bool emptyAnswer = false,
        bool listOnly = false, ILogger? logger = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(probe);
        try
        {
            var floor = emptyAnswer || listOnly || windowEnd - requestedStart > DurationTrendRouting.TruncationSlack ? await probe() : null;
            return Build(floor, requestedStart, table, tail, emptyAnswer, listOnly);
        }
        catch (Exception ex) when (!(ex is OperationCanceledException && cancellationToken.IsCancellationRequested))
        {
            logger?.LogWarning(ex, "The coverage probe of {Table} failed; the answer goes without its window-floor notice.", table);
            return McpWindowNotice.Unavailable;
        }
    }

    /// <summary>
    /// The earlier of two instants, a null (nothing to report) giving way to the other; null when both are. Lite's
    /// <c>LocalDataService.EarlierCoverageFloor</c>.
    /// </summary>
    internal static DateTime? Earlier(DateTime? first, DateTime? second) =>
        first is DateTime a && second is DateTime b ? (a <= b ? a : b) : first ?? second;

    /// <summary>
    /// The later of two instants, a null (nothing to report) giving way to the other; null when both are. For an answer that
    /// carries two separate series (get_blocking_stats): the one notice names the later of the two floors, since the answer is
    /// only as complete as its least-covered series. Lite's <c>LocalDataService.LaterCoverageFloor</c>.
    /// </summary>
    internal static DateTime? Later(DateTime? first, DateTime? second) =>
        first is DateTime a && second is DateTime b ? (a >= b ? a : b) : first ?? second;

    /// <summary>
    /// <see cref="ReadAsync"/> for an EVENT list (deadlocks, blocked process reports, long query completions), where a first run
    /// of the collector can store events from before itself (#4966): the coverage probe reads the collector's table on
    /// <c>collection_time</c>, but the rows are windowed on the event's own time, so a page can show an event older than the
    /// probe's floor, and a notice that names a start later than a row it shows is wrong on its face. On a data answer the floor is
    /// the EARLIER of the probe's and <paramref name="oldestEventShown"/> (the oldest event time on the page; null when the page
    /// holds none, as on an empty answer). The comparison runs inside the probe delegate, so a window the probe is skipped for
    /// (90 minutes or less, with rows) stays covered at the start that was asked for, as every other window-floor tool does, and
    /// a probe that throws still answers <see cref="McpWindowNotice.Unavailable"/> (the event time is not a verdict on its own).
    /// </summary>
    internal static Task<McpWindowNotice> ReadEventAsync(
        Func<Task<DateTime?>> probe, DateTime? oldestEventShown, DateTime requestedStart, DateTime windowEnd, string table,
        bool emptyAnswer = false, ILogger? logger = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(probe);
        return ReadAsync(
            async () => Earlier(await probe(), oldestEventShown),
            requestedStart, windowEnd, table, emptyAnswer: emptyAnswer, logger: logger, cancellationToken: cancellationToken);
    }

    /// <summary>
    /// A payload already serialized with the three window-floor keys, without them: what a tool answers when its probe failed
    /// (<see cref="McpWindowNotice.IsUnavailable"/>). Only top-level keys are touched, and the order of the rest is kept.
    /// </summary>
    internal static string WithoutKeys(string json)
    {
        var node = JsonNode.Parse(json)!.AsObject();
        node.Remove("effective_start");
        node.Remove("window_truncated");
        node.Remove("truncation_note");
        return node.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>The reads the web does not list, and the collector table each one windows on (both on <c>collection_time</c>).</summary>
    internal static readonly System.Collections.Generic.IReadOnlyDictionary<string, string> UnlistedTableByRead =
        new System.Collections.Generic.Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["get_pg_cpu_utilization"] = "pg_cpu_utilization",
        };

    /// <summary>The table <paramref name="read"/> is named after in a notice: the web's table when it lists the read, else its own.</summary>
    internal static string TableFor(string read) =>
        WebDataStartNote.TableByRead.TryGetValue(read, out var table) ? table : UnlistedTableByRead[read];

    /// <summary>
    /// The coverage probe source of <paramref name="read"/>: the one the WEB probes (<see cref="WebDataStartNote.TryGetReadSource"/>)
    /// when it lists the read, so a tool and the page it shares a read with never name different starts (#4966); else the
    /// read's collector table.
    /// </summary>
    internal static DataWindowFloor.Source SourceFor(string read) =>
        WebDataStartNote.TryGetReadSource(read, out var source) ? source : DataWindowFloor.Source.ForCollectorTable(UnlistedTableByRead[read]);

    /// <summary>
    /// <see cref="ReadAsync"/> for one tool over [<paramref name="requestedStart"/>, <paramref name="windowEnd"/>], on
    /// <see cref="SourceFor"/>, naming <see cref="TableFor"/> in the notice.
    /// </summary>
    internal static Task<McpWindowNotice> ReadForToolAsync(
        NpgsqlDataSource postgres, string tool, string serverName, DateTime requestedStart, DateTime windowEnd,
        bool emptyAnswer, ILogger? logger, CancellationToken cancellationToken)
    {
        /* TableFor throws on a tool in neither map; that must cost only the notice, never the answer. */
        string table;
        try
        {
            table = TableFor(tool);
        }
        catch (KeyNotFoundException ex)
        {
            logger?.LogWarning(ex, "No window-notice table is listed for {Tool}; the answer goes without its window-floor notice.", tool);
            return Task.FromResult(McpWindowNotice.Unavailable);
        }

        return ReadAsync(
            () => Probe(postgres, SourceFor(tool), serverName, requestedStart, windowEnd, cancellationToken),
            requestedStart, windowEnd, table, emptyAnswer: emptyAnswer, logger: logger, cancellationToken: cancellationToken);
    }

    /// <summary>A data answer already serialized: without the three window-floor keys when the probe failed, else as is.</summary>
    internal static string Finish(string json, McpWindowNotice notice) =>
        notice.IsUnavailable ? WithoutKeys(json) : json;

    /// <summary>
    /// An empty answer written with <c>hints = notice.AsHints()</c>, without the <c>hints</c> key when the probe failed (a null
    /// there would otherwise be written as <c>"hints":null</c>).
    /// </summary>
    internal static string FinishEmpty(string json, McpWindowNotice notice)
    {
        if (!notice.IsUnavailable)
        {
            return json;
        }

        var node = JsonNode.Parse(json)!.AsObject();
        node.Remove("hints");
        return node.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>A test's stand-in for the coverage probe, per async flow (null: the store is asked).</summary>
    private static readonly AsyncLocal<Func<Task<DateTime?>>?> s_testOnlyProbe = new();

    /// <summary>Test seam: while set, <see cref="Probe(NpgsqlDataSource, DataWindowFloor.Source, string, DateTime, DateTime, CancellationToken)"/> runs this instead of the store read.</summary>
    internal static Func<Task<DateTime?>>? TestOnlyProbe
    {
        get => s_testOnlyProbe.Value;
        set => s_testOnlyProbe.Value = value;
    }

    /// <summary>The coverage probe for one collector table and one server over [<paramref name="start"/>, <paramref name="end"/>].</summary>
    internal static Task<DateTime?> Probe(
        NpgsqlDataSource postgres, string table, string serverName, DateTime start, DateTime end, CancellationToken cancellationToken) =>
        Probe(postgres, DataWindowFloor.Source.ForCollectorTable(table), serverName, start, end, cancellationToken);

    /// <summary>The coverage probe over a source the caller built (the collection log, one collector's runs, a stitched read).</summary>
    internal static Task<DateTime?> Probe(
        NpgsqlDataSource postgres, DataWindowFloor.Source source, string serverName, DateTime start, DateTime end, CancellationToken cancellationToken) =>
        Probe(postgres, [source], serverName, start, end, cancellationToken);

    /// <summary>
    /// The coverage probe over several sources a page is fed by (get_blocking's XE reports and DMV snapshots, #4966):
    /// <see cref="DataWindowFloor.GetAsync"/> answers the EARLIEST of them in one read.
    /// </summary>
    internal static Task<DateTime?> Probe(
        NpgsqlDataSource postgres, IReadOnlyList<DataWindowFloor.Source> sources, string serverName, DateTime start, DateTime end, CancellationToken cancellationToken)
    {
        var stub = TestOnlyProbe;
        if (stub != null)
        {
            return stub();
        }

        return DataWindowFloor.GetAsync(postgres, sources, [serverName], start, end, StorageCommandDeadlines.McpReadSeconds, cancellationToken);
    }
}
