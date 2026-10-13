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

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Turns each route's stored scope into the loader-set members the router reads (#5750): the expanded server-id
/// set, the scope tag's depth and the orphan flag. Pure — both products' loaders call it with the same four
/// inputs, so the same fleet yields the same sets in Lite and Darling (a parity test pins that).
///
/// <para><b>Rules.</b> A fleet-wide route (no scope) comes back untouched. A server scope whose id is not among
/// the known server ids is orphaned with an empty set. A tag scope whose tag is not in the parent map is orphaned
/// with an empty set. A tag scope covers every known server mapped to that tag OR ANY DESCENDANT. A route with
/// both scopes set is malformed and orphaned. The tag parent map can hold a cycle (nothing at the storage layer
/// forbids one); every walk stops on the first revisit, so a cycle caps the walk instead of looping.</para>
/// </summary>
public static class NotificationRouteScopeExpander
{
    /// <summary>
    /// Expands every route's scope.
    /// </summary>
    /// <param name="routes">The routes as stored (scope columns only).</param>
    /// <param name="knownServerIds">Every server id that exists in the product's registry.</param>
    /// <param name="tagParents">Tag id to parent tag id (null for a root). A tag absent from the keys does not exist.</param>
    /// <param name="serverTags">Server id to the tag ids it is mapped to. Servers not in
    /// <paramref name="knownServerIds"/> and tags not in <paramref name="tagParents"/> are ignored.</param>
    /// <returns>The routes in the same order, scoped ones carrying <see cref="NotificationRoute.ExpandedServerIds"/>,
    /// <see cref="NotificationRoute.ScopeTagDepth"/> and <see cref="NotificationRoute.Orphaned"/>.</returns>
    public static IReadOnlyList<NotificationRoute> Expand(
        IReadOnlyList<NotificationRoute> routes,
        IReadOnlyCollection<int> knownServerIds,
        IReadOnlyDictionary<int, int?> tagParents,
        IReadOnlyDictionary<int, IReadOnlyCollection<int>> serverTags)
    {
        ArgumentNullException.ThrowIfNull(routes);
        ArgumentNullException.ThrowIfNull(knownServerIds);
        ArgumentNullException.ThrowIfNull(tagParents);
        ArgumentNullException.ThrowIfNull(serverTags);

        var known = knownServerIds as ISet<int> ?? new HashSet<int>(knownServerIds);
        var coverCache = new Dictionary<int, HashSet<int>>();
        var result = new List<NotificationRoute>(routes.Count);

        foreach (var route in routes)
        {
            if (route is null || !route.IsScoped)
            {
                if (route is not null)
                {
                    result.Add(route);
                }

                continue;
            }

            if (route.ScopeServerId.HasValue && route.ScopeTagId.HasValue)
            {
                result.Add(route with { ExpandedServerIds = EmptySet, ScopeTagDepth = 0, Orphaned = true });
                continue;
            }

            if (route.ScopeServerId is { } serverId)
            {
                result.Add(known.Contains(serverId)
                    ? route with { ExpandedServerIds = new HashSet<int> { serverId }, ScopeTagDepth = 0, Orphaned = false }
                    : route with { ExpandedServerIds = EmptySet, ScopeTagDepth = 0, Orphaned = true });
                continue;
            }

            var tagId = route.ScopeTagId!.Value;
            if (!tagParents.ContainsKey(tagId))
            {
                result.Add(route with { ExpandedServerIds = EmptySet, ScopeTagDepth = 0, Orphaned = true });
                continue;
            }

            if (!coverCache.TryGetValue(tagId, out var covered))
            {
                covered = ServersUnder(tagId, known, tagParents, serverTags);
                coverCache[tagId] = covered;
            }

            result.Add(route with { ExpandedServerIds = covered, ScopeTagDepth = AncestorCount(tagId, tagParents), Orphaned = false });
        }

        return result;
    }

    /// <summary>How many ancestors <paramref name="tagId"/> has: its chain of parents that exist. Stops on a
    /// revisit (a cycle) and on a parent that is not in the map, so it always terminates.</summary>
    public static int AncestorCount(int tagId, IReadOnlyDictionary<int, int?> tagParents)
    {
        var seen = new HashSet<int> { tagId };
        var count = 0;
        var current = tagId;
        while (tagParents.TryGetValue(current, out var parent) && parent is { } parentId
               && tagParents.ContainsKey(parentId) && seen.Add(parentId))
        {
            count++;
            current = parentId;
        }

        return count;
    }

    /// <summary>The known servers mapped to <paramref name="rootTag"/> or any tag below it.</summary>
    private static HashSet<int> ServersUnder(
        int rootTag,
        ISet<int> known,
        IReadOnlyDictionary<int, int?> tagParents,
        IReadOnlyDictionary<int, IReadOnlyCollection<int>> serverTags)
    {
        /* A tag is in the subtree when walking UP from it reaches the root. Each walk is bounded by a visited
           set, so a cycle in the parent map ends the walk instead of looping. */
        var subtree = new HashSet<int>();
        foreach (var tag in tagParents.Keys)
        {
            var seen = new HashSet<int>();
            var current = tag;
            while (seen.Add(current))
            {
                if (current == rootTag)
                {
                    subtree.Add(tag);
                    break;
                }

                if (!tagParents.TryGetValue(current, out var parent) || parent is not { } up)
                {
                    break;
                }

                current = up;
            }
        }

        var servers = new HashSet<int>();
        foreach (var (serverId, tags) in serverTags)
        {
            if (known.Contains(serverId) && tags is not null && tags.Any(subtree.Contains))
            {
                servers.Add(serverId);
            }
        }

        return servers;
    }

    private static readonly IReadOnlySet<int> EmptySet = new HashSet<int>();
}
