/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
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
    /// The same three keys, with the same values and wording, for an <c>empty</c> status, which carries them under
    /// <c>hints</c>: an empty answer over a window the store does not reach back to is not a true negative, and without
    /// them it read as one. A status that says the collector never ran (<c>not_collected</c>) does not carry them.
    /// </summary>
    internal object AsHints() => new { effective_start = EffectiveStart, window_truncated = WindowTruncated, truncation_note = TruncationNote };
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
        DateTime? floor, DateTime requestedStart, string table, string? tail = null, bool emptyAnswer = false)
    {
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
    /// <see cref="Build"/> with its coverage probe, which a window no longer than
    /// <see cref="DurationTrendRouting.TruncationSlack"/> that answered rows never needs: the probe cannot find a floor
    /// later than the start by more than the slack, so the answer is covered whatever it would read. The probe is a
    /// delegate and is not started for such a window. An EMPTY answer is always probed, whatever the window's length:
    /// nothing was read, so nothing else says the store held the window at all.
    /// </summary>
    internal static async Task<McpWindowNotice> ReadAsync(
        Func<Task<DateTime?>> probe, DateTime requestedStart, DateTime windowEnd, string table, string? tail = null, bool emptyAnswer = false)
    {
        ArgumentNullException.ThrowIfNull(probe);
        var floor = emptyAnswer || windowEnd - requestedStart > DurationTrendRouting.TruncationSlack ? await probe() : null;
        return Build(floor, requestedStart, table, tail, emptyAnswer);
    }

    /// <summary>The coverage probe for one collector table and one server over [<paramref name="start"/>, <paramref name="end"/>].</summary>
    internal static Task<DateTime?> Probe(
        NpgsqlDataSource postgres, string table, string serverName, DateTime start, DateTime end, CancellationToken cancellationToken) =>
        Probe(postgres, DataWindowFloor.Source.ForCollectorTable(table), serverName, start, end, cancellationToken);

    /// <summary>The coverage probe over a source the caller built (the collection log, one collector's runs, a stitched read).</summary>
    internal static Task<DateTime?> Probe(
        NpgsqlDataSource postgres, DataWindowFloor.Source source, string serverName, DateTime start, DateTime end, CancellationToken cancellationToken) =>
        DataWindowFloor.GetAsync(postgres, [source], [serverName], start, end, StorageCommandDeadlines.McpReadSeconds, cancellationToken);
}
