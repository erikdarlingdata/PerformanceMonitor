/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Which servers of one map pass are rebuilt from one read of the sightings tables (#9705). Pure: no store, no I/O. A
/// rebuild reads both tables front to back, so a pass that rebuilds N servers one at a time reads every block of both
/// tables N times. A group is read once (<see cref="QueryTagMapStore.ReadRebuildRowsAsync"/>), each row going to its own
/// server's <see cref="QueryTagRebuildMerge"/>, and every merge of the group is held until its map is written. So a group
/// is bounded by the rows its servers are estimated to have in both tables together
/// (<see cref="QueryTagMapStore.RebuildGroupRows"/>), which bounds the tagged keys it can hold.
/// </summary>
internal static class QueryTagRebuildGroups
{
    /// <summary>
    /// What the store's statistics say of one table's <c>server_id</c> column. <paramref name="Rows"/> is
    /// <c>pg_class.reltuples</c>, which is -1 for a table never vacuumed or analyzed. <paramref name="Distinct"/> is
    /// <c>pg_stats.n_distinct</c>, none when the column has no statistics row: above zero a count of distinct servers, below
    /// zero minus the distinct servers as a fraction of the rows. <paramref name="CommonServers"/> and
    /// <paramref name="CommonShares"/> are <c>most_common_vals</c> and <c>most_common_freqs</c>, each share a fraction of the rows.
    /// </summary>
    internal sealed record ServerStatistics(double Rows, double? Distinct, IReadOnlyList<int> CommonServers, IReadOnlyList<double> CommonShares);

    /// <summary>
    /// The rows of one table that the statistics give one server, or none when they give no estimate: no row of the table in
    /// <c>pg_class</c>, a table never analyzed, or a table that has rows and no statistics for the column. A table analyzed
    /// with no rows gives every server zero. A server among the most common values gets its share of the rows. Any other
    /// server gets the share the common values leave, split evenly over the distinct servers that are not among them.
    /// </summary>
    internal static double? EstimateRows(ServerStatistics? statistics, int serverId)
    {
        if (statistics is null || statistics.Rows < 0)
        {
            return null;
        }

        if (statistics.Rows == 0)
        {
            return 0;
        }

        if (statistics.Distinct is not { } distinct)
        {
            return null;
        }

        double commonShare = 0;
        int common = Math.Min(statistics.CommonServers.Count, statistics.CommonShares.Count);
        for (int i = 0; i < common; i++)
        {
            if (statistics.CommonServers[i] == serverId)
            {
                return statistics.Rows * statistics.CommonShares[i];
            }

            commonShare += statistics.CommonShares[i];
        }

        double distinctServers = distinct < 0 ? -distinct * statistics.Rows : distinct;
        double otherServers = Math.Max(1, distinctServers - common);
        return statistics.Rows * Math.Max(0, 1 - commonShare) / otherServers;
    }

    /// <summary>
    /// Splits <paramref name="serverIds"/> into groups, in their order, each group's estimated rows together within
    /// <paramref name="budgetRows"/>. A server whose estimate alone is over the budget is a group of one, and so is a server
    /// with no estimate in <paramref name="estimatedRows"/>: with no statistics every server is read alone.
    /// </summary>
    internal static List<List<int>> Form(IReadOnlyList<int> serverIds, IReadOnlyDictionary<int, double>? estimatedRows, double budgetRows)
    {
        ArgumentNullException.ThrowIfNull(serverIds);

        var groups = new List<List<int>>();
        List<int>? open = null;
        double openRows = 0;
        foreach (int serverId in serverIds)
        {
            if (estimatedRows is null || !estimatedRows.TryGetValue(serverId, out double rows) || rows > budgetRows)
            {
                groups.Add(new List<int> { serverId });
                continue;
            }

            if (open is null || openRows + rows > budgetRows)
            {
                open = new List<int>();
                groups.Add(open);
                openRows = 0;
            }

            open.Add(serverId);
            openRows += rows;
        }

        return groups;
    }
}
