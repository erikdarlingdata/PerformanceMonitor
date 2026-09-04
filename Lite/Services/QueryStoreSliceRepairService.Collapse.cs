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

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The PURE half of the #1912 slice repair: which columns form the dedup key, and how each non-key column
/// combines when two slices of one interval collapse into one row.
///
/// <para><b>Split out so it can be LINKED, not copied (#2748).</b> <c>tools/SliceRepairRepro</c> reproduces
/// the repair against a real DuckDB, and a repro that reimplements the logic it is reproducing drifts from
/// production and then proves nothing — the same trap <c>tools/CompactionRepro</c> exists to avoid, which is
/// why it links <c>ParquetCompaction.cs</c> rather than restating it. The rest of the service cannot be
/// linked (it needs <c>DuckDbInitializer</c> and logging); this part needs nothing but the BCL, so it can be.
/// A partial class rather than a new type, so every existing caller and test keeps naming
/// <c>QueryStoreSliceRepairService</c>.</para>
/// </summary>
public sealed partial class QueryStoreSliceRepairService
{
    /// <summary>
    /// The dedup key at its FULLEST — the read side's partition plus the two columns every stored row carries.
    /// <c>collection_time</c> is what makes it the pre-fix signature rather than merely "the same interval":
    /// since #1907 the collector emits at most one row per interval per cycle, so no correctly-collected row
    /// can join such a group, which is why the repair is idempotent and safe to re-run.
    /// </summary>
    private static readonly string[] FullKeyColumns =
    [
        "server_id",
        "database_name",
        "query_id",
        "plan_id",
        "runtime_stats_interval_id",
        "first_execution_time",
        "execution_type_desc",
        "replica_role",
        "collection_time",
    ];

    /// <summary>
    /// The key columns that actually exist in <paramref name="available"/>.
    ///
    /// <para><b>This is why the archive needs per-file handling rather than one key.</b> Archive files are
    /// written per month and the SCHEMA CHANGED underneath them: <c>runtime_stats_interval_id</c> arrived with
    /// #1841 tier 2 and <c>replica_role</c> with #1844/#1872, so a file written before those has neither — a
    /// real one on this machine (June 2026) is missing both. It is also why the archive views read with
    /// <c>union_by_name</c>. Naming a column an old file does not have would fail the rewrite outright; worse,
    /// silently assuming the newest schema on an old file would group on a key that is not the era's dedup key
    /// at all, and quietly combine rows that are not slices of one interval.</para>
    /// </summary>
    internal static IReadOnlyList<string> KeyColumnsFor(IEnumerable<string> available)
    {
        ArgumentNullException.ThrowIfNull(available);

        var present = new HashSet<string>(available, StringComparer.OrdinalIgnoreCase);
        return FullKeyColumns.Where(present.Contains).ToList();
    }

    /// <summary>
    /// How one column combines, mirroring <c>QueryStoreCollector.BuildPayloadBody</c> and Darling's
    /// <c>QueryStoreSliceRepair</c>: the additive counter sums, every <c>avg_</c> takes the count-WEIGHTED
    /// mean, <c>min_</c>/<c>max_</c> take the extreme, <c>last_execution_time</c> is the interval's span end.
    ///
    /// <para>The weighted mean is the part that is easy to get wrong and impossible to see afterwards: Query
    /// Store stores an average and a count but never a total, so <c>avg * count</c> is what recovers a slice's
    /// total. A plain average of the slice averages weights a 25-execution sliver the same as a
    /// 100-execution flush.</para>
    /// </summary>
    internal static string CombineExpression(string column)
    {
        ArgumentNullException.ThrowIfNull(column);

        if (string.Equals(column, "execution_count", StringComparison.OrdinalIgnoreCase))
        {
            return "SUM(execution_count)";
        }

        if (string.Equals(column, "last_execution_time", StringComparison.OrdinalIgnoreCase))
        {
            return "MAX(last_execution_time)";
        }

        if (column.StartsWith("avg_", StringComparison.OrdinalIgnoreCase))
        {
            return $"CAST(SUM(CAST({column} AS DOUBLE) * execution_count) / NULLIF(SUM(execution_count), 0) AS BIGINT)";
        }

        if (column.StartsWith("min_", StringComparison.OrdinalIgnoreCase))
        {
            return $"MIN({column})";
        }

        if (column.StartsWith("max_", StringComparison.OrdinalIgnoreCase))
        {
            return $"MAX({column})";
        }

        /* Attributes of the interval rather than measurements of it — query text, hashes, the forced-plan
           flags. Every slice carries the same value, so ANY_VALUE is correct; DuckDB has it, which is why this
           does not need Postgres' bool_or special case. */
        return $"ANY_VALUE({column})";
    }

    /// <summary>
    /// Whether a column is a MEASUREMENT of the interval — something that has to be recombined when two
    /// slices merge — as opposed to an ATTRIBUTE of it, which every slice of one interval already carries
    /// identically.
    ///
    /// <para><b>Derived from <see cref="CombineExpression"/> rather than restated as a second list.</b> The
    /// rules for what sums, what takes a weighted mean and what takes an extreme live in exactly one place,
    /// and <c>ANY_VALUE</c> is precisely the fallback that method reaches when a column is none of those. Ask
    /// it, and a rule added there is picked up here for free; keep a parallel list and the two drift, which on
    /// this path would mean a measurement silently left uncombined — the original #1912 defect, reintroduced
    /// by the fix for it.</para>
    /// </summary>
    internal static bool IsMeasurement(string column)
    {
        ArgumentNullException.ThrowIfNull(column);
        return !CombineExpression(column).StartsWith(AnyValuePrefix, StringComparison.Ordinal);
    }

    private const string AnyValuePrefix = "ANY_VALUE(";

    /// <summary>
    /// The NARROW staging projection (#2771): the key, a surviving row's identity, and the recombined
    /// measurements — and deliberately not one attribute column.
    ///
    /// <para><b>Why the wide payload must not be here.</b> The full projection materializes every column of
    /// every collapsed group, and <c>query_plan_text</c> holds full showplan XML. On the reporting store —
    /// 31,426 split intervals — that aggregate does not fit in Lite's 1 GB <c>memory_limit</c>, and DuckDB
    /// raises <c>Out of Memory Error</c> (on Windows x64 the same pressure fast-fails the process natively,
    /// which is why the catch never ran). Attributes are identical across the slices of one interval by
    /// definition, so they never needed to travel through the aggregate at all: leave them on the row that
    /// survives and only the numbers move.</para>
    ///
    /// <para><paramref name="rowIdentity"/> is the expression that names the surviving row —
    /// <c>MIN(rowid)</c> against a table. Which row survives is arbitrary and must be, exactly as
    /// <c>ANY_VALUE</c> was arbitrary; what matters is that it is ONE row and that the delete and the update
    /// agree on it.</para>
    /// </summary>
    internal static string BuildMeasurementProjection(
        IReadOnlyList<string> columns, IReadOnlyList<string> key, string rowIdentity, string keepAlias)
    {
        ArgumentNullException.ThrowIfNull(columns);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentException.ThrowIfNullOrWhiteSpace(rowIdentity);
        ArgumentException.ThrowIfNullOrWhiteSpace(keepAlias);

        var keySet = new HashSet<string>(key, StringComparer.OrdinalIgnoreCase);
        var parts = new List<string>(key.Count + 1);
        parts.AddRange(columns.Where(keySet.Contains));
        parts.Add($"{rowIdentity} AS {keepAlias}");
        parts.AddRange(
            columns.Where(c => !keySet.Contains(c) && IsMeasurement(c))
                   .Select(c => $"{CombineExpression(c)} AS {c}"));
        return string.Join(", ", parts);
    }

    /// <summary>
    /// The <c>SET</c> list that copies the staged measurements back onto the surviving row. Mirrors
    /// <see cref="BuildMeasurementProjection"/>'s column set exactly, so a column that is staged is a column
    /// that is written back and vice versa.
    /// </summary>
    internal static string BuildMeasurementAssignments(
        IReadOnlyList<string> columns, IReadOnlyList<string> key, string stagingAlias)
    {
        ArgumentNullException.ThrowIfNull(columns);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentException.ThrowIfNullOrWhiteSpace(stagingAlias);

        var keySet = new HashSet<string>(key, StringComparer.OrdinalIgnoreCase);
        return string.Join(
            ", ",
            columns.Where(c => !keySet.Contains(c) && IsMeasurement(c))
                   .Select(c => $"{c} = {stagingAlias}.{c}"));
    }

    /// <summary>The full projection: key columns as-is, everything else combined, in the source's own order.</summary>
    private static string BuildProjection(IReadOnlyList<string> columns, IReadOnlyList<string> key)
    {
        var keySet = new HashSet<string>(key, StringComparer.OrdinalIgnoreCase);
        return string.Join(
            ", ",
            columns.Select(c => keySet.Contains(c) ? c : $"{CombineExpression(c)} AS {c}"));
    }
}
