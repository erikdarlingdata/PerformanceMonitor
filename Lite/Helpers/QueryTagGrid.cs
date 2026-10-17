/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Helpers;

/// <summary>What a grid row's "Assign query tag..." binds: a query hash, or a database, schema and object name (a procedure row).</summary>
public sealed record QueryTagAssignTarget(QueryTagKeyKind Kind, string? Hash, string? Database, string? Schema, string? Name, string Display);

/// <summary>
/// #5993 part B: the pure side of the Queries grids' tag column, tag filter and "Assign query tag..." menu item. The grids read
/// through the #5992 readers (<see cref="QueryTagReadScope"/>), which apply the filter before the top-N cut and name each row's tag;
/// nothing here resolves a rule or keeps a tag rule of its own.
/// </summary>
public static class QueryTagGrid
{
    public const string AllChoice = "All tags";
    public const string UntaggedChoice = "Untagged";

    /// <summary>The <c>tag</c> argument the readers take for a filter choice: null for all rows, <c>untagged</c>, or the tag's path.</summary>
    public static string? FilterValue(string? choice) =>
        string.IsNullOrWhiteSpace(choice) || choice == AllChoice ? null
        : choice == UntaggedChoice ? QueryTagReadScope.UntaggedWord
        : choice;

    /// <summary>Each tag's path (<c>Parent / Child</c>) by id.</summary>
    public static Dictionary<int, string> PathsOf(IReadOnlyList<QueryTagRow> tags)
    {
        var byId = tags.ToDictionary(t => t.Id);
        var paths = new Dictionary<int, string>();
        foreach (var tag in tags)
        {
            var parts = new List<string> { tag.Name };
            var parent = tag.ParentId;
            for (var steps = 0; parent is int pid && byId.TryGetValue(pid, out var up) && steps < 16; steps++)
            {
                parts.Add(up.Name);
                parent = up.ParentId;
            }

            parts.Reverse();
            paths[tag.Id] = string.Join(QueryTagReadScope.PathSeparator, parts);
        }

        return paths;
    }

    /// <summary>The filter's choices: all rows, untagged rows, then every tag by path.</summary>
    public static List<string> FilterChoices(IReadOnlyList<QueryTagRow> tags)
    {
        var choices = new List<string> { AllChoice, UntaggedChoice };
        choices.AddRange(PathsOf(tags).Values.OrderBy(p => p, StringComparer.OrdinalIgnoreCase));
        return choices;
    }

    /// <summary>Puts each row's tag path (or null) on the row from the scope's map.</summary>
    public static void Label(QueryTagReadScope scope, IEnumerable<object> rows)
    {
        foreach (var row in rows)
        {
            switch (row)
            {
                case QueryStatsRow r: r.QueryTag = scope.TagOfHash(r.QueryHash); break;
                case QueryStoreRow r: r.QueryTag = scope.TagOfHash(r.QueryHash); break;
                case LongQueryCompletionRow r: r.QueryTag = scope.TagOfHash(r.QueryHash); break;
                case ProcedureStatsRow r: r.QueryTag = scope.TagOfObject(r.DatabaseName, r.SchemaName, r.ObjectName); break;
            }
        }
    }

    /// <summary>
    /// Runs a grid read with the chosen tag filter and labels its rows. A filter naming a tag nobody carries any more (deleted since
    /// the list was built) reads as no filter, so the grid never goes blank on a stale choice.
    /// </summary>
    public static async Task<List<T>> ReadAsync<T>(LocalDataService dataService, int serverId, string? choice, Func<QueryTagReadScope, Task<List<T>>> read)
        where T : class
    {
        var scope = await dataService.GetQueryTagReadScopeAsync(serverId, FilterValue(choice));
        if (scope.Unknown)
        {
            scope = await dataService.GetQueryTagReadScopeAsync(serverId, null);
        }

        var rows = await read(scope);
        Label(scope, rows);
        return rows;
    }

    /// <summary>What a grid row can be assigned by, or null when it carries no key (a row with no query hash).</summary>
    public static QueryTagAssignTarget? TargetOf(object? row)
    {
        QueryTagAssignTarget? Hash(string? hash) =>
            QueryHashIdentity.KnownHash(hash) is { Length: > 0 } known ? new(QueryTagKeyKind.Hash, known, null, null, null, known) : null;

        switch (row)
        {
            case QueryStatsRow r: return Hash(r.QueryHash);
            case QueryStoreRow r: return Hash(r.QueryHash);
            case LongQueryCompletionRow r: return Hash(r.QueryHash);
            case ProcedureStatsRow r when !string.IsNullOrWhiteSpace(r.DatabaseName) && !string.IsNullOrWhiteSpace(r.SchemaName) && !string.IsNullOrWhiteSpace(r.ObjectName):
                return new(QueryTagKeyKind.Object, null, r.DatabaseName, r.SchemaName, r.ObjectName, $"{r.DatabaseName}.{r.SchemaName}.{r.ObjectName}");
            default: return null;
        }
    }

    /// <summary>Assigns the row's key to the tag on this server, through the store. A refusal throws <see cref="QueryTagRefusedException"/>.</summary>
    public static Task<int> AssignAsync(LocalDataService dataService, int serverId, int tagId, QueryTagAssignTarget target) =>
        dataService.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(
            tagId, target.Kind, serverId, target.Hash, target.Database, target.Schema, target.Name));

    /// <summary>The line after an assignment: a write shows on the grid only after the map's next pass.</summary>
    public static string AssignedMessage(QueryTagAssignTarget target, string tagPath) =>
        $"{target.Display} is assigned to '{tagPath}'. The row shows the tag after the tag map's next pass.";
}
