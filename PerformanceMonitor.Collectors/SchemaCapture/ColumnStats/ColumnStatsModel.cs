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

namespace PerformanceMonitor.Collectors;

/// <summary>Header of a statistics object (DBCC SHOW_STATISTICS ... STAT_HEADER): when it was built and how many rows it saw.</summary>
public sealed record StatHeader(DateTime? LastUpdated, long Rows, long RowsSampled, float? PersistedSamplePercent);

/// <summary>
/// One statistics object on one table. <see cref="StatColumns"/> is the key column list as the catalog reports it, so a column name
/// containing a comma stays one name. <see cref="ClusteredKeyColumns"/> are the clustering key columns SQL Server appends to the
/// density vector of a nonclustered statistic.
/// </summary>
public sealed record StatCandidate(
    int ObjectId,
    string SchemaName,
    string ObjectName,
    int StatsId,
    string StatsName,
    string Origin,
    DateTime StatsDate,
    IReadOnlyList<string> StatColumns,
    IReadOnlyList<string> ClusteredKeyColumns,
    long TableRows);

/// <summary>One row of a density vector, tied to the leading column list it describes.</summary>
public sealed record StatPrefix(
    int ObjectId,
    int StatsId,
    int PrefixLength,
    IReadOnlyList<string> Columns,
    bool IncludesClusteringKey,
    double AllDensity);

/// <summary>Selectivity derived from an all-density value: the share of distinct values, the distinct estimate and the average rows per value.</summary>
public sealed record Selectivity(double Value, double DistinctEstimate, double RowsPerValue);

/// <summary>A prefix as it was stored on an earlier run, used to choose between statistics that describe the same column list.</summary>
public sealed record StoredPrefix(
    string StatsName,
    string Origin,
    IReadOnlyList<string> Columns,
    bool IncludesClusteringKey,
    double AllDensity,
    long Rows,
    long RowsSampled,
    DateTime LastUpdated,
    float? PersistedSamplePercent,
    DateTime ReadAt);

/// <summary>Whether a statistic needs to be read again compared with what is stored.</summary>
public enum StatChange { New, Changed, Unchanged }

/// <summary>
/// #8798: pure arithmetic and decisions for column selectivity taken from statistics density vectors. No I/O, so the rules are
/// testable without a server.
/// </summary>
public static class ColumnStatsModel
{
    /// <summary>Upper bound on statistics objects read per database, so one huge database cannot stall a collection run.</summary>
    public const int MaxStatisticsPerDatabase = 20_000;

    /// <summary>A single column averaging at least this many rows per value is flagged as low selectivity.</summary>
    public const long LowSelectivityRowsPerValue = 100_000;

    /// <summary>
    /// Density row i describes the first i+1 key columns. Rows past the key columns describe the key plus clustering key columns
    /// (SQL Server appends them to nonclustered statistics), skipping any already in the key. Names come from the catalog lists, never
    /// from splitting text, and a row with no name left is dropped.
    /// </summary>
    public static IReadOnlyList<StatPrefix> MapDensityRows(StatCandidate stat, IReadOnlyList<double> allDensityInOrder)
    {
        var names = new List<string>(stat.StatColumns);
        var keyCount = names.Count;
        foreach (var clustered in stat.ClusteredKeyColumns)
        {
            if (!names.Contains(clustered, StringComparer.OrdinalIgnoreCase))
            {
                names.Add(clustered);
            }
        }

        var result = new List<StatPrefix>();
        for (var i = 0; i < allDensityInOrder.Count && i < names.Count; i++)
        {
            result.Add(new StatPrefix(
                stat.ObjectId,
                stat.StatsId,
                i + 1,
                names.Take(i + 1).ToList(),
                i >= keyCount,
                allDensityInOrder[i]));
        }

        return result;
    }

    /// <summary>All-density is 1 / distinct values, so rows per value is density * rows. Null when either input makes that meaningless.</summary>
    public static Selectivity? Compute(double allDensity, long rows)
    {
        if (rows <= 0 || allDensity <= 0)
        {
            return null;
        }

        var distinct = 1.0 / allDensity;
        return new Selectivity(Math.Min(1.0, distinct / rows), distinct, allDensity * rows);
    }

    /// <summary>
    /// "unique" only when the schema guarantees it (a unique index or constraint covers the columns), never inferred from a density,
    /// which is an estimate. "low selectivity" only for a single column, where a high rows-per-value is a statement about that column.
    /// </summary>
    public static string? Label(Selectivity s, int columnCount, bool coveredByUniqueIndexOrConstraint)
    {
        if (coveredByUniqueIndexOrConstraint)
        {
            return "unique";
        }

        if (columnCount == 1 && s.RowsPerValue >= LowSelectivityRowsPerValue)
        {
            return "low selectivity";
        }

        return null;
    }

    /// <summary>Of several statistics describing the same column list, the one built from the most rows, then the most recently updated.</summary>
    public static StoredPrefix PickBest(IEnumerable<StoredPrefix> sameColumnList)
    {
        return sameColumnList
            .OrderByDescending(p => p.RowsSampled)
            .ThenByDescending(p => p.LastUpdated)
            .First();
    }

    /// <summary>A statistic is read again whenever its date differs from the stored one, older included (a rebuilt or restored statistic).</summary>
    public static StatChange Decide(DateTime current, DateTime? stored)
    {
        if (stored is null)
        {
            return StatChange.New;
        }

        return current == stored.Value ? StatChange.Unchanged : StatChange.Changed;
    }
}
