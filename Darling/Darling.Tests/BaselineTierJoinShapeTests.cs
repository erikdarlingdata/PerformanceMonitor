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
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The robust scaffold's MAD pass must join its tiers by EQUALITY, never by an OR'd non-equi predicate.
///
/// <para><b>Observed on the dogfood box, 2026-08-27 through 2026-09-03.</b> <c>io_latency</c> logged
/// "Baseline query for io_latency did not finish within its command timeout" on eight days, always giving up
/// at ~30s. The scan was never the problem — chunk pruning worked and reading all 476,431 rows of the 30-day
/// window took 682 ms — but the MAD join was written
/// <c>ON (t.hour_of_day = -1 OR t.hour_of_day = k.hh) AND (t.day_of_week = -1 OR t.day_of_week = k.dw)</c>,
/// which Postgres cannot hash. The planner's own estimate for the resulting join reached cost 596,208,924
/// for a 193-row result, and it spilled to temp. Measured against the same store: 23.7s for the OR form
/// versus 4.2s once the tiers are expanded with UNION ALL and joined by equality — identical 193 rows,
/// identical checksum.</para>
///
/// <para>The pin is written over the METRIC FAMILY rather than over <c>io_latency</c>, because
/// <see cref="PgBaselineProvider.RobustTierScaffold"/> is shared by nine metrics and io_latency was simply
/// the one whose raw table grew large enough to trip the deadline first. A pin naming one metric would have
/// let the next one regress silently — the same way an enumerated pin let the Aurora arm of
/// <c>PgStatementText</c> stay broken under a green suite.</para>
///
/// <para>Lite needs no equivalent: DuckDB has a native <c>mad()</c> aggregate and computes all three tiers
/// single-pass with no join at all. This scaffold is the Postgres emulation of that, which is why its join
/// shape has to be asserted rather than assumed.</para>
/// </summary>
public sealed class BaselineTierJoinShapeTests
{
    /// <summary>
    /// Every metric name declared on <see cref="MetricNames"/>, read by reflection so a metric added later
    /// is covered without anyone remembering to add it here.
    /// </summary>
    public static TheoryData<string> AllMetricNames()
    {
        var data = new TheoryData<string>();
        foreach (var name in MetricNameValues())
        {
            data.Add(name);
        }

        return data;
    }

    private static IEnumerable<string> MetricNameValues() =>
        typeof(MetricNames)
            .GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy)
            .Where(f => f is { IsLiteral: true, IsInitOnly: false } && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!)
            .Where(v => !string.IsNullOrWhiteSpace(v));

    /// <summary>
    /// Matches the shape that caused the timeout: a tier predicate comparing a column to the -1 sentinel
    /// with OR. Deliberately structural rather than a literal-text match of the old join, so a
    /// reformatted or re-aliased reintroduction still fails.
    /// </summary>
    private static readonly Regex OrdTierPredicate =
        new(@"=\s*-1\s+OR\b", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    [Theory]
    [MemberData(nameof(AllMetricNames))]
    public void NoBaselineQueryJoinsItsTiersWithAnOrPredicate(string metricName)
    {
        var query = PgBaselineProvider.GetBaselineQuery(metricName);
        if (query is null)
        {
            return; // not every metric has a Postgres arm; that is the arm's own business.
        }

        Assert.DoesNotMatch(OrdTierPredicate, query);
    }

    /// <summary>
    /// The other half: a query that computes MAD must actually equi-join its expanded tiers. Without this a
    /// future edit could satisfy the negative assertion above by deleting the join and quietly changing what
    /// MAD means.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllMetricNames))]
    public void EveryMadQueryEquiJoinsItsExpandedTiers(string metricName)
    {
        var query = PgBaselineProvider.GetBaselineQuery(metricName);
        if (query is null || !query.Contains("tier_mads", StringComparison.Ordinal))
        {
            return;
        }

        Assert.Contains("keyed_tiers", query, StringComparison.Ordinal);
        Assert.Contains("t.hour_of_day = k.hour_of_day", query, StringComparison.Ordinal);
        Assert.Contains("t.day_of_week = k.day_of_week", query, StringComparison.Ordinal);
    }

    /// <summary>
    /// The scaffold is shared, so the fix must reach every metric that uses it — nine at the time of
    /// writing. If this count ever drops, a metric stopped routing through the robust path and its MAD
    /// silently became 0, which the reader cannot distinguish from a genuinely flat signal.
    /// </summary>
    [Fact]
    public void TheRobustScaffoldStillServesTheWholeRobustFamily()
    {
        var withMad = MetricNameValues()
            .Select(PgBaselineProvider.GetBaselineQuery)
            .Count(q => q is not null && q.Contains("tier_mads", StringComparison.Ordinal));

        Assert.Equal(9, withMad);
    }
}
