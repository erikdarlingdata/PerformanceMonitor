/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #5988 part A: the viewer's side of the query-tag column and filter on the Queries grids. It adds no SQL of its own: the
/// filter is <see cref="QueryTagReadJoin.Apply"/> on each grid's statement (before the top-N cut, a parent matching its
/// descendants) and a row's tag is <see cref="QueryTagReadJoin.LookupAsync"/>, the same shared join the MCP reads use. The
/// viewer only reads; tags and rules are edited on the web page and over MCP.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>The filter choice that shows every row.</summary>
    public const string AllTagsChoice = "All tags";

    /// <summary>The filter choice that shows only rows with no tag.</summary>
    public const string UntaggedChoice = "Untagged";

    /// <summary>The tag argument a filter choice stands for: null for every row, <c>untagged</c>, or the tag's path.</summary>
    public static string? TagFilterValue(string? choice) =>
        string.IsNullOrWhiteSpace(choice) || choice == AllTagsChoice ? null
        : choice == UntaggedChoice ? QueryTagReadJoin.UntaggedWord
        : choice;

    /// <summary>
    /// Resolves a filter choice to the shared filter. A choice naming a tag nobody carries any more (deleted since the list
    /// was filled) reads as no filter, so a grid never goes blank on a stale choice.
    /// </summary>
    public async Task<QueryTagFilter> ResolveQueryTagFilterAsync(string? choice, CancellationToken cancellationToken = default)
    {
        var filter = await QueryTagReadJoin.ResolveAsync(_dataSource, TagFilterValue(choice), cancellationToken);
        return filter.Unknown ? QueryTagFilter.None : filter;
    }

    /// <summary>The filter's choices: all rows, untagged rows, then every tag by path.</summary>
    public async Task<List<string>> GetQueryTagFilterChoicesAsync(CancellationToken cancellationToken = default)
    {
        var choices = new List<string> { AllTagsChoice, UntaggedChoice };
        choices.AddRange((await QueryTagReadJoin.ListAsync(_dataSource, cancellationToken)).Select(t => t.Path));
        return choices;
    }

    /// <summary>Each tag path's stored colour, for the grid's pill (a tag with no colour maps to null).</summary>
    public async Task<Dictionary<string, string?>> GetQueryTagColoursAsync(CancellationToken cancellationToken = default) =>
        (await QueryTagReadJoin.ListAsync(_dataSource, cancellationToken))
            .GroupBy(t => t.Path, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(g => g.Key, g => g.First().Colour, StringComparer.OrdinalIgnoreCase);

    /// <summary>Puts each row's tag path on it (a key with no map row keeps null), by the shared lookup.</summary>
    internal async Task LabelTagsAsync<T>(
        int serverId, string keyKind, IReadOnlyList<T> rows, Func<T, string?> keyOf, CancellationToken cancellationToken)
        where T : QueryTagRowBase
    {
        if (rows.Count == 0)
        {
            return;
        }

        var tags = await QueryTagReadJoin.LookupAsync(_dataSource, serverId, keyKind, rows.Select(keyOf), cancellationToken);
        if (tags.Count == 0)
        {
            return;
        }

        var colours = await GetQueryTagColoursAsync(cancellationToken);
        foreach (var row in rows)
        {
            var key = keyOf(row);
            if (!string.IsNullOrEmpty(key) && tags.TryGetValue(key, out var path))
            {
                row.QueryTag = path;
                row.QueryTagColour = colours.GetValueOrDefault(path);
            }
        }
    }

    /// <summary>The same for a procedure page, whose key is the qualified object name.</summary>
    internal Task LabelObjectTagsAsync(int serverId, IReadOnlyList<ViewerProcedureStatsRow> rows, CancellationToken cancellationToken) =>
        LabelTagsAsync(serverId, QueryTagReadJoin.ObjectKind, rows,
            r => QueryTagReadJoin.ObjectKey(r.DatabaseName, r.SchemaName, r.ObjectName), cancellationToken);
}

/// <summary>
/// #5988: what a Queries grid row carries for its Tag column: the tag's path, its stored colour, and the pill brushes
/// (<see cref="TagColorBrushes"/>, so a tag with no colour is the neutral pill). The label is one light colour on every fill.
/// </summary>
public abstract class QueryTagRowBase
{
    /// <summary>The row's tag as its path ("Parent / Child"), or null when it has none.</summary>
    public string? QueryTag { get; set; }

    /// <summary>The tag's stored <c>#RRGGBB</c> colour, or null.</summary>
    public string? QueryTagColour { get; set; }

    /// <summary>True when the row has a tag, so a row without one shows no pill.</summary>
    public bool HasQueryTag => !string.IsNullOrEmpty(QueryTag);

    public System.Windows.Media.Brush QueryTagFill => TagColorBrushes.Fill(QueryTagColour);

    public System.Windows.Media.Brush QueryTagTextBrush => TagColorBrushes.PillText;
}
