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
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2771: the hot-table collapse must never push the wide payload through its aggregate.
///
/// <para>The original staging materialized the FULL projection - every column of every collapsed group,
/// including <c>query_plan_text</c>'s showplan XML - and on a store with a large pre-#1907 backlog that does
/// not fit in Lite's 1 GB <c>memory_limit</c>. Reproduced at the reporting store's own 31,426 split intervals
/// with <c>tools/SliceRepairRepro</c>: "Out of Memory Error: failed to pin block of size 256.0 KiB
/// (953.4 MiB/953.6 MiB used)". On Windows x64 the same pressure fast-fails the process natively, which is
/// why the catch in <c>RepairOnStartupAsync</c> never ran.</para>
///
/// <para><b>These assertions are non-vacuous in BOTH directions, which is what makes them a guard rather than
/// decoration.</b> Classify everything as a measurement (the old full-projection behavior) and the
/// "no attribute in staging" assertions go red; classify nothing as one and the "measurements are still
/// recombined" assertions go red. Both were run against a mutated build before this shipped.</para>
/// </summary>
public sealed class SliceRepairNarrowCollapseTests
{
    /// <summary>The production column shape, wide payload included.</summary>
    private static readonly string[] Columns =
    [
        "server_id", "database_name", "query_id", "plan_id", "runtime_stats_interval_id",
        "first_execution_time", "execution_type_desc", "replica_role", "collection_time",
        "last_execution_time", "interval_start_time_utc", "is_forced_plan", "force_failure_count",
        "compatibility_level", "execution_count",
        "avg_duration_us", "min_duration_us", "max_duration_us",
        "module_name", "query_text", "query_hash", "query_plan_text", "query_plan_hash", "plan_type",
    ];

    /// <summary>
    /// Columns that are ATTRIBUTES of the interval rather than measurements of it. Every slice of one
    /// interval carries these identically, which is why <c>ANY_VALUE</c> was correct - and why they never
    /// needed to travel through the aggregate at all.
    /// </summary>
    private static readonly string[] Attributes =
    [
        "query_plan_text", "query_text", "module_name", "query_hash", "query_plan_hash", "plan_type",
        "is_forced_plan", "force_failure_count", "compatibility_level", "interval_start_time_utc",
    ];

    private static IReadOnlyList<string> Key => QueryStoreSliceRepairService.KeyColumnsFor(Columns);

    [Fact]
    public void StagingProjectionCarriesNoAttributeColumn_SoTheWidePayloadNeverEntersTheAggregate()
    {
        var projection = QueryStoreSliceRepairService.BuildMeasurementProjection(
            Columns, Key, "MIN(rowid)", "pm_keep_rowid");

        /* The defect in one assertion: ANY_VALUE is exactly the fallback CombineExpression reaches for an
           attribute, so its presence here means the wide payload is back in the aggregate. */
        Assert.DoesNotContain("ANY_VALUE", projection, StringComparison.Ordinal);

        foreach (var attribute in Attributes)
        {
            Assert.DoesNotContain($" AS {attribute}", projection, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void StagingProjectionStillRecombinesEveryMeasurement()
    {
        var projection = QueryStoreSliceRepairService.BuildMeasurementProjection(
            Columns, Key, "MIN(rowid)", "pm_keep_rowid");

        /* The other direction. A narrow projection that dropped the measurements too would "fit in memory"
           and silently stop repairing anything - the #1912 defect reintroduced by its own fix. */
        Assert.Contains("SUM(execution_count) AS execution_count", projection, StringComparison.Ordinal);
        Assert.Contains("MAX(last_execution_time) AS last_execution_time", projection, StringComparison.Ordinal);
        Assert.Contains("MIN(min_duration_us) AS min_duration_us", projection, StringComparison.Ordinal);
        Assert.Contains("MAX(max_duration_us) AS max_duration_us", projection, StringComparison.Ordinal);

        /* The weighted mean specifically: a plain average of slice averages weights a 25-execution sliver
           the same as a 100-execution flush, and that error is invisible afterwards. */
        Assert.Contains("avg_duration_us AS DOUBLE) * execution_count", projection, StringComparison.Ordinal);

        Assert.Contains("MIN(rowid) AS pm_keep_rowid", projection, StringComparison.Ordinal);
    }

    [Fact]
    public void AssignmentsMirrorTheStagedColumnsExactly_SoTheTwoCannotDrift()
    {
        var projection = QueryStoreSliceRepairService.BuildMeasurementProjection(
            Columns, Key, "MIN(rowid)", "pm_keep_rowid");
        var assignments = QueryStoreSliceRepairService.BuildMeasurementAssignments(Columns, Key, "r");

        var keySet = new HashSet<string>(Key, StringComparer.OrdinalIgnoreCase);
        var expected = Columns
            .Where(c => !keySet.Contains(c) && QueryStoreSliceRepairService.IsMeasurement(c))
            .ToList();

        Assert.NotEmpty(expected);

        foreach (var column in expected)
        {
            /* Staged AND written back. A column present in one and absent from the other would either
               recombine into nothing or write an unstaged value onto the survivor. */
            Assert.Contains($" AS {column}", projection, StringComparison.Ordinal);
            Assert.Contains($"{column} = r.{column}", assignments, StringComparison.Ordinal);
        }

        foreach (var attribute in Attributes)
        {
            Assert.DoesNotContain($"{attribute} = r.", assignments, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void IsMeasurementAgreesWithCombineExpression_SoTheClassificationCannotDriftFromTheRules()
    {
        /* IsMeasurement is DERIVED from CombineExpression rather than restated, so this asserts the
           derivation still holds for every column rather than re-listing the rules a third time. */
        foreach (var column in Columns)
        {
            var combined = QueryStoreSliceRepairService.CombineExpression(column);
            var isAnyValue = combined.StartsWith("ANY_VALUE(", StringComparison.Ordinal);

            Assert.Equal(!isAnyValue, QueryStoreSliceRepairService.IsMeasurement(column));
        }

        Assert.True(QueryStoreSliceRepairService.IsMeasurement("execution_count"));
        Assert.True(QueryStoreSliceRepairService.IsMeasurement("avg_cpu_time_us"));
        Assert.False(QueryStoreSliceRepairService.IsMeasurement("query_plan_text"));
        Assert.False(QueryStoreSliceRepairService.IsMeasurement("query_text"));
    }
}
