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
    /// What the store says of one table and its <c>server_id</c> column. <paramref name="Tuples"/> and
    /// <paramref name="Pages"/> are <c>pg_class.reltuples</c> and <c>relpages</c>, the rows and the heap blocks counted at the
    /// last vacuum or analyze: -1 rows for a table that has had neither. <paramref name="CurrentPages"/> is the heap blocks
    /// the table has now. <paramref name="Distinct"/> is <c>pg_stats.n_distinct</c>, none when the column has no statistics
    /// row: above zero a count of distinct servers, below zero minus the distinct servers as a fraction of the rows.
    /// <paramref name="CommonServers"/> and <paramref name="CommonShares"/> are <c>most_common_vals</c> and
    /// <c>most_common_freqs</c>, each share a fraction of the rows.
    /// </summary>
    internal sealed record ServerStatistics(
        double Tuples, double Pages, double CurrentPages, double? Distinct, IReadOnlyList<int> CommonServers, IReadOnlyList<double> CommonShares);

    /// <summary>
    /// The rows a table has now, estimated as the planner estimates them: the rows a block held at the last vacuum or
    /// analyze, times the blocks the table has now. The counted rows alone are the table as it was then, so a table that was
    /// analyzed empty and filled since would count no rows. A table with no blocks now has no rows, whatever was counted.
    /// None when the table has blocks now and the count gives no rows a block to scale: a table never vacuumed or analyzed,
    /// or one counted with no blocks or with no rows.
    /// </summary>
    internal static double? EstimateTableRows(ServerStatistics statistics)
    {
        ArgumentNullException.ThrowIfNull(statistics);

        if (statistics.CurrentPages <= 0)
        {
            return 0;
        }

        if (statistics.Pages <= 0 || statistics.Tuples <= 0)
        {
            return null;
        }

        return statistics.Tuples / statistics.Pages * statistics.CurrentPages;
    }

    /// <summary>
    /// The rows of one table that the statistics give one server, or none when they give no estimate: no row of the table in
    /// <c>pg_class</c>, no estimate of the table's rows (<see cref="EstimateTableRows"/>), or a table that has rows and no
    /// statistics for the column. A table with no blocks gives every server zero. A server among the most common values gets
    /// its share of the rows. Any other server gets the share the common values leave, split evenly over the distinct servers
    /// that are not among them.
    /// </summary>
    internal static double? EstimateRows(ServerStatistics? statistics, int serverId)
    {
        if (statistics is null || EstimateTableRows(statistics) is not { } rows)
        {
            return null;
        }

        if (rows == 0)
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
                return rows * statistics.CommonShares[i];
            }

            commonShare += statistics.CommonShares[i];
        }

        double distinctServers = distinct < 0 ? -distinct * rows : distinct;
        double otherServers = Math.Max(1, distinctServers - common);
        return rows * Math.Max(0, 1 - commonShare) / otherServers;
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
