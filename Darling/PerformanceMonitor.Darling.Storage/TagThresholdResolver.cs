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
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One stored tag override: the table row's id, its tag, the wire key and the value.</summary>
public sealed record TagOverrideCandidate(long RowId, int TagId, string Key, double Value);

/// <summary>The tag override that applies to a server for one key: the tag it comes from and its value.</summary>
public sealed record TagOverrideWinner(int TagId, double Value);

/// <summary>
/// Which tag override applies to a server (#6926). It is the notification routes' rule, run by the routes' own code: each tag that
/// holds an override is expanded by <see cref="NotificationRouteScopeExpander"/> exactly as a tag-scoped route is (a tag covers the servers
/// mapped to it or to any tag below it, a tag that no longer exists is orphaned and covers nothing, and tag tables that could not be read
/// cover nothing and are not called orphaned). When a server is covered by several tags that hold the same key, the deepest tag wins
/// (<see cref="NotificationRoute.ScopeTagDepth"/>, the number of ancestors) and a tie goes to the lowest row id, the way the router's
/// ordering takes the lowest route id after depth. Pure: the service, the MCP view and the tests share this one resolution.
/// </summary>
public static class TagThresholdResolver
{
    /// <summary>The winning tag override per (server, key) for every server some tag covers.</summary>
    public static IReadOnlyDictionary<(int ServerId, string Key), TagOverrideWinner> Resolve(
        IReadOnlyCollection<TagOverrideCandidate> rows,
        IReadOnlyCollection<int> knownServerIds,
        IReadOnlyDictionary<int, int?> tagParents,
        IReadOnlyDictionary<int, IReadOnlyCollection<int>> serverTags,
        bool tagsReadable = true)
    {
        ArgumentNullException.ThrowIfNull(rows);
        if (rows.Count == 0)
            return new Dictionary<(int ServerId, string Key), TagOverrideWinner>();

        var tagIds = rows.Select(r => r.TagId).Distinct().ToList();
        var routes = tagIds
            .Select(t => new NotificationRoute(t, NotificationRoute.WildcardMatch, "", "", "", "", "", true) { ScopeTagId = t })
            .ToList();
        var expanded = NotificationRouteScopeExpander.Expand(routes, knownServerIds, tagParents, serverTags, tagsReadable);
        var byTag = new Dictionary<int, NotificationRoute>();
        for (var i = 0; i < tagIds.Count; i++)
            byTag[tagIds[i]] = expanded[i];

        var winners = new Dictionary<(int ServerId, string Key), (int Depth, long RowId, TagOverrideWinner Winner)>();
        foreach (var row in rows)
        {
            var route = byTag[row.TagId];
            if (route.Orphaned || route.ExpandedServerIds is null)
                continue;
            foreach (var serverId in route.ExpandedServerIds)
            {
                var slot = (serverId, row.Key);
                if (winners.TryGetValue(slot, out var current)
                    && (current.Depth > route.ScopeTagDepth || (current.Depth == route.ScopeTagDepth && current.RowId <= row.RowId)))
                    continue;
                winners[slot] = (route.ScopeTagDepth, row.RowId, new TagOverrideWinner(row.TagId, row.Value));
            }
        }

        return winners.ToDictionary(p => p.Key, p => p.Value.Winner);
    }
}
