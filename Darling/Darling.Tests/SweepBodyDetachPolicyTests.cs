/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the policy behind #2700/#2717 — WHICH collectors are detached from the sequential per-server
/// collection body, and the invariant that makes detaching safe. Neither had any test before #2840.
///
/// <para><b>Why the set is what it is, measured.</b> The body runs every due collector for a server
/// together, and the outer launch loop will not relaunch it while it runs (INV-2, one body per server),
/// so one slow collector delays every other collector for that server. From
/// <c>collect.collection_log</c> on prod-pos-use1-monitor-01 over 24h (2026-09-03):</para>
///
/// <list type="table">
///   <item><description><c>query_store</c> — p50 3,350ms, <b>p90 65,053ms</b>, max 364,202ms</description></item>
///   <item><description><c>index_object_stats</c> — p90 15,640ms, but 1440-minute cadence (42 runs/day
///     fleet-wide, one per server per day), so its body impact is amortised to nil</description></item>
///   <item><description><c>procedure_stats</c> — p50 5,982ms, p90 11,964ms, but <b>1-minute tier</b></description></item>
///   <item><description><c>query_stats</c> — p50 2,070ms, p90 5,062ms, 1-minute tier</description></item>
///   <item><description><c>plan_correction</c> — p50 2,681ms, <b>max 133,934ms</b> — the same bimodal
///     shape as query_store with a smaller worst case, which is why #2717 followed #2700</description></item>
/// </list>
///
/// <para><b>The criterion is measured p90 against the fast tier's cadence, NOT collector shape.</b>
/// Enumerated-vs-scalar fanout was the intuitive rule and the data disproves it in both directions:
/// <c>database_scoped_config</c> fans out to 10 databases at p90 127ms, while <c>procedure_stats</c>
/// has zero fanout at p90 11,964ms. A fanout-derived split would detach the cheap collector and leave
/// the expensive one starving the tier. See #2840.</para>
///
/// <para><b>The 4.5x evidence.</b> use2 runs the same Balanced preset with Query Store dead since
/// 2026-08-17 17:36. Its <c>query_stats</c> delivered cadence stepped from 4.69-9.02 min (Query Store
/// live) to 1.44-1.57 min (dead) the following day, and held there for two weeks.</para>
/// </summary>
public sealed class SweepBodyDetachPolicyTests
{
    /// <summary>The collectors #2700/#2717 detach, by the criterion documented on this class.</summary>
    private static readonly string[] ExpectedDetached = { "query_store", "plan_correction" };

    private static bool IsDetached(string name) =>
        DarlingWorker.IsQueryStoreCollector(name) || DarlingWorker.IsPlanCorrectionCollector(name);

    /// <summary>
    /// The invariant that makes detaching safe, and the one that GENERALISES: a detached collector runs
    /// fire-and-forget behind its own per-(server, collector) gate, so a still-in-flight run SKIPS rather
    /// than overlapping. Skipping a 5-minute collector defers a tick; skipping a 1-minute collector is the
    /// starvation this policy exists to prevent. So detaching anything on the 1-minute tier trades one
    /// failure mode for another — it must not happen silently.
    ///
    /// <para>This is what would fire if someone "fixed" the residual ~1.5-minute floor (#2841) by
    /// detaching <c>procedure_stats</c> (p90 11,964ms, 1-minute tier) instead of addressing the body's
    /// sequential execution.</para>
    /// </summary>
    [Fact]
    public void NoDetachedCollectorSitsOnTheOneMinuteTier()
    {
        var offenders = CollectorScheduleDefaults.All
            .Where(kv => IsDetached(kv.Key) && kv.Value.FrequencyMinutes < 2)
            .Select(kv => $"{kv.Key} (every {kv.Value.FrequencyMinutes}min)")
            .OrderBy(s => s)
            .ToList();

        Assert.True(
            offenders.Count == 0,
            "A detached collector skips rather than queues when its previous run is still in flight, so "
            + "detaching a 1-minute-tier collector converts starvation into guaranteed misses. Offending: "
            + string.Join(", ", offenders));
    }

    /// <summary>
    /// The detach set is exactly the measured cost outliers — asserted over the WHOLE catalog rather than
    /// by naming the two, so a third collector added to the predicate fails here and sends its author to
    /// the criterion on this class instead of to a name list.
    /// </summary>
    [Fact]
    public void TheDetachSetIsExactlyTheMeasuredCostOutliers()
    {
        var actual = CollectorScheduleDefaults.All.Keys
            .Where(IsDetached)
            .OrderBy(n => n, System.StringComparer.OrdinalIgnoreCase)
            .ToList();

        Assert.Equal(ExpectedDetached.OrderBy(n => n, System.StringComparer.OrdinalIgnoreCase), actual);
    }

    /// <summary>
    /// Every name the detach predicate answers for must exist in the catalog. A predicate matching a name
    /// no collector declares is dead code that reads as active policy — and both predicates compare against
    /// the collector's OWN declared <c>Name</c> rather than a literal precisely so a rename cannot silently
    /// unhook the detach, which this asserts is still true.
    /// </summary>
    [Fact]
    public void EveryDetachedNameIsARealCatalogCollector()
    {
        foreach (var name in ExpectedDetached)
        {
            Assert.True(
                CollectorScheduleDefaults.All.ContainsKey(name),
                $"'{name}' is detached from the sweep body but is not in CollectorScheduleDefaults.");
            Assert.True(IsDetached(name), $"'{name}' is expected to be detached but the predicate says no.");
        }
    }
}
