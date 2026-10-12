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

namespace PerformanceMonitor.Common;

/// <summary>
/// The Job History tab's Server and Category drop-down choices (#5703), shared by the Darling viewer and Lite so the two tabs offer
/// the same set. Pure — no WPF — so the merge is tested as behaviour, not pinned as source text.
/// <para>The drop-downs only grow. A read that Status or Category narrowed holds only the matching runs, and the row cap keeps the
/// newest runs of the whole fleet, so the rows alone would drop the quiet server (or a category) from the choices and the operator
/// could not pick it back. Each refresh therefore merges three sources: the choices already on offer, the choices known without
/// reading the runs (every monitored server; every category with a run in the window, from a read the row cap does not touch),
/// and the rows just read. A name in the rows wins over an older name, because the rows speak the names the grid shows (a renamed
/// server). The selected choice is always kept, so a refresh never drops what the operator picked.</para>
/// </summary>
public static class JobHistoryFilterChoices
{
    /// <summary>One Server choice: the server's id and the name the drop-down shows.</summary>
    public readonly record struct ServerChoice(int ServerId, string ServerName);

    /// <summary>
    /// The Server choices, ordered by name. <paramref name="offered"/> is what the drop-down holds now, <paramref name="known"/> the
    /// monitored servers, <paramref name="rows"/> the servers of the rows just read; a later source's name replaces an earlier one's
    /// for the same id, a server with no name is ignored, and <paramref name="selected"/> is kept even when no source names it.
    /// </summary>
    public static List<ServerChoice> MergeServers(
        IEnumerable<ServerChoice> offered, IEnumerable<ServerChoice> known, IEnumerable<ServerChoice> rows, ServerChoice? selected = null)
    {
        ArgumentNullException.ThrowIfNull(offered);
        ArgumentNullException.ThrowIfNull(known);
        ArgumentNullException.ThrowIfNull(rows);

        var byId = new Dictionary<int, string>();
        foreach (var choice in offered.Concat(known).Concat(rows))
        {
            if (!string.IsNullOrEmpty(choice.ServerName))
            {
                byId[choice.ServerId] = choice.ServerName;
            }
        }

        if (selected is { } keep && !byId.ContainsKey(keep.ServerId) && !string.IsNullOrEmpty(keep.ServerName))
        {
            byId[keep.ServerId] = keep.ServerName;
        }

        return byId
            .Select(p => new ServerChoice(p.Key, p.Value))
            .OrderBy(c => c.ServerName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(c => c.ServerId)
            .ToList();
    }

    /// <summary>
    /// The Category choices, ordered ignoring case and distinct ignoring case (the filter matches exactly, ignoring case, so two
    /// spellings are one choice). The first spelling seen wins, in the order offered, known, rows, so a choice already on offer keeps
    /// its text. Empty names are ignored; <paramref name="selected"/> is kept even when no source names it.
    /// </summary>
    public static List<string> MergeCategories(
        IEnumerable<string?> offered, IEnumerable<string?> known, IEnumerable<string?> rows, string? selected = null)
    {
        ArgumentNullException.ThrowIfNull(offered);
        ArgumentNullException.ThrowIfNull(known);
        ArgumentNullException.ThrowIfNull(rows);

        return offered.Concat(known).Concat(rows).Append(selected)
            .Where(c => !string.IsNullOrEmpty(c))
            .Select(c => c!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(c => c, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    /// <summary>The Server filter the read takes: null for the "All" entry (index 0, or no usable tag), else the server id in the tag.</summary>
    public static int? ServerFilter(int selectedIndex, string? tag) =>
        selectedIndex > 0 && int.TryParse(tag, NumberStyles.Integer, CultureInfo.InvariantCulture, out var id) ? id : null;

    /// <summary>The Status filter the read takes: null for the "All" entry, else the <c>sysjobhistory</c> code in the tag.</summary>
    public static int? StatusFilter(int selectedIndex, string? tag) =>
        selectedIndex > 0 && int.TryParse(tag, NumberStyles.Integer, CultureInfo.InvariantCulture, out var status) ? status : null;

    /// <summary>The Category filter the read takes: null for the "All" entry (or an empty tag), else the category name in the tag.</summary>
    public static string? CategoryFilter(int selectedIndex, string? tag) =>
        selectedIndex > 0 && !string.IsNullOrEmpty(tag) ? tag : null;
}
