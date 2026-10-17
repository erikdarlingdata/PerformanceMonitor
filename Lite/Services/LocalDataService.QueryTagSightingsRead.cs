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
using PerformanceMonitor.Common.QueryTags;

namespace PerformanceMonitorLite.Services;

/// <summary>One stored sighting of a key on a server, as the map builder reads it.</summary>
public sealed record QueryTagSightingEntry(int ServerId, QueryTagKeyKind Kind, string Key, QueryTagSighting Sighting);

public partial class LocalDataService
{
    /// <summary>
    /// The stored query-tag sightings (what the collectors saw, retention-bounded), for one server or every server, read
    /// the way the map builder reads them so the Test button on the Manage Query Tags window and the map agree.
    /// </summary>
    public async Task<List<QueryTagSightingEntry>> GetQueryTagSightingsAsync(int? serverId = null)
    {
        using var connection = await OpenConnectionAsync();
        var (rows, _) = await ReadQuerySightingsAsync(
            connection, null, int.MaxValue, true, DateTime.UtcNow, new HashSet<int>());
        return rows
            .Where(r => serverId is null || r.ServerId == serverId)
            .Select(r => new QueryTagSightingEntry(r.ServerId, r.Kind, r.Key, r.Sighting))
            .ToList();
    }

    /// <summary>The hand assignments, rules and tag names in the shapes the shared resolver takes, so a dry run resolves
    /// exactly as the map builder does.</summary>
    public async Task<(List<QueryTagHandAssignment> Assignments, List<QueryTagRule> Rules, IReadOnlyDictionary<string, long> TagNames)> GetQueryTagResolverInputsAsync()
    {
        var assignments = (await GetQueryTagAssignmentsAsync()).Select(ToHandAssignment).ToList();
        var rules = (await GetQueryTagRulesAsync()).Select(ToRule).ToList();
        var names = QueryTagRuleTester.TagNames(await GetQueryTagsAsync());
        return (assignments, rules, names);
    }
}
