/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Text;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Builds the positional DuckDB predicate for the global database filter (issue #1319):
    /// <c>" AND {column} IN ($k, $k+1, ...)"</c>, mirroring the multi-value <c>IN</c> push-down the
    /// wait-type trends already use. Returns <c>""</c> (no clause, <paramref name="values"/> empty) when
    /// the selection is null or empty so callers short-circuit to today's unfiltered behavior — the
    /// caller MUST then add no database parameters. When non-empty, one object per database is appended
    /// to <paramref name="values"/> in <c>$k</c> order; the caller adds them as DuckDBParameters
    /// IMMEDIATELY AFTER its fixed parameters (so positional index <paramref name="firstParamIndex"/>
    /// lines up). The returned clause may be spliced into more than one WHERE (e.g. a comparison query's
    /// current + baseline CTEs) because a positional parameter can be referenced repeatedly — in that case
    /// still add <paramref name="values"/> only once.
    /// </summary>
    /// <param name="databases">Selected databases, or null/empty for "All" (no filter).</param>
    /// <param name="column">The unqualified/qualified database-name column to filter (e.g. "database_name").</param>
    /// <param name="firstParamIndex">The 1-based positional index of the FIRST database parameter (one past the caller's fixed params).</param>
    /// <param name="values">Receives the database values to bind, in order; empty when no clause is emitted.</param>
    internal static string BuildDbInClause(
        IReadOnlyList<string>? databases,
        string column,
        int firstParamIndex,
        out List<object> values)
    {
        values = new List<object>();
        if (databases == null || databases.Count == 0)
        {
            return "";
        }

        var sb = new StringBuilder();
        sb.Append(" AND ").Append(column).Append(" IN (");
        for (int i = 0; i < databases.Count; i++)
        {
            if (i > 0)
            {
                sb.Append(", ");
            }
            sb.Append('$').Append(firstParamIndex + i);
            values.Add(databases[i]);
        }
        sb.Append(')');
        return sb.ToString();
    }

    /// <summary>
    /// The aggregate-side form of a <see cref="BuildDbInClause"/> clause (#5414 M1): the same predicate without its
    /// leading <c>AND</c>, for a <c>FILTER (WHERE ...)</c> inside the per-collection aggregates of a trend read,
    /// or <c>""</c> when there is no filter. A trend's database filter belongs there and not in the WHERE: the
    /// collection's interval is the collection's whichever databases were asked about, so a collection where the
    /// chosen database had no rows must stay in the bucket as zero work over its real seconds, not vanish from the
    /// denominator and read the rate high.
    /// </summary>
    internal static string DbInPredicate(string dbClause) =>
        dbClause.Length == 0 ? "" : dbClause[" AND ".Length..];

    /// <summary>
    /// <c>SUM(expr)</c>, or with a database filter <c>COALESCE(SUM(expr) FILTER (WHERE predicate), 0)</c> — a
    /// collection the chosen databases had no rows in is zero work, a measurement (#5414 M1).
    /// </summary>
    internal static string FilteredSum(string expr, string dbClause) =>
        dbClause.Length == 0
            ? $"SUM({expr})"
            : $"COALESCE(SUM({expr}) FILTER (WHERE {DbInPredicate(dbClause)}), 0)";
}
