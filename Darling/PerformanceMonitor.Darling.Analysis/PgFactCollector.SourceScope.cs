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
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgFactCollector
{
    /// <summary>
    /// #8570: the <see cref="Fact.Source"/> of every fact each family read writes, read from the family's code (its own
    /// <c>new Fact</c> sites and the shared builders it calls: <c>FactCollectorHelpers</c>, <c>TempDbFileSetup</c>,
    /// <c>BlockingChainFactBuilder</c>). A family that is not listed is never skipped.
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, string[]> FamilySources = new Dictionary<string, string[]>(StringComparer.Ordinal)
    {
        [nameof(WaitStatsAndGroupStepAsync)] = ["waits"],
        [nameof(CollectBlockingFactsAsync)] = ["blocking"],
        [nameof(CollectBlockingChainFactsAsync)] = ["blocking"],
        [nameof(CollectDeadlockFactsAsync)] = ["blocking"],
        [nameof(CollectServerConfigFactsAsync)] = ["config"],
        [nameof(CollectMemoryFactsAsync)] = ["memory"],
        [nameof(CollectDatabaseSizeFactAsync)] = ["config"],
        [nameof(CollectServerMetadataFactsAsync)] = ["config"],
        [nameof(CollectCpuUtilizationFactsAsync)] = ["cpu"],
        [nameof(CollectRunnableTaskFactsAsync)] = ["cpu"],
        [nameof(CollectIoLatencyFactsAsync)] = ["io"],
        [nameof(CollectTempDbFactsAsync)] = ["tempdb"],
        [nameof(CollectMemoryGrantFactsAsync)] = ["memory"],
        [nameof(CollectQueryStatsFactsAsync)] = ["queries"],
        [nameof(CollectParameterSensitivityFactsAsync)] = ["queries"],
        [nameof(CollectPlanRegressionFactsAsync)] = ["queries"],
        [nameof(CollectBadActorFactsAsync)] = ["bad_actor"],
        [nameof(CollectPerfmonFactsAsync)] = ["perfmon"],
        [nameof(CollectMemoryClerkFactsAsync)] = ["memory"],
        [nameof(CollectPlanCacheFactsAsync)] = ["memory"],
        [nameof(CollectMemoryPressureEventFactsAsync)] = ["memory"],
        [nameof(CollectDatabaseConfigFactsAsync)] = ["database_config"],
        [nameof(CollectFileAutogrowthFactsAsync)] = ["config"],
        [nameof(CollectTempDbFileSetupFactsAsync)] = ["config"],
        [nameof(CollectProcedureStatsFactsAsync)] = ["queries"],
        [nameof(CollectActiveQueryFactsAsync)] = ["queries"],
        [nameof(CollectRunningJobFactsAsync)] = ["jobs"],
        [nameof(CollectSessionFactsAsync)] = ["sessions"],
        [nameof(CollectTraceFlagFactsAsync)] = ["config"],
        [nameof(CollectServerPropertiesFactsAsync)] = ["config"],
        [nameof(CollectDiskSpaceFactsAsync)] = ["disk"],
        [nameof(CollectPlanAdvisoryFactsAsync)] = ["queries"],
    };

    /// <summary>
    /// #8570: the sources a read may narrow to, each with the families its facts need for scoring beyond the families that
    /// write it. <c>FactScorer.ScoreAll</c> scores a fact from its own value (layer 1), then from the facts its key's
    /// amplifiers read (layer 2), then caps the tuning-class keys unless a THREADPOOL, SOS_SCHEDULER_YIELD or
    /// RESOURCE_SEMAPHORE wait co-fired (layer 3). A source that is not listed reads every family, and so does any
    /// request that holds one.
    /// <list type="bullet">
    /// <item><description><c>queries</c>: QUERY_SPILLS reads MEMORY_GRANT_PENDING and the RESOURCE_SEMAPHORE wait,
    /// PARAMETER_SENSITIVITY reads MEMORY_GRANT_PENDING, PLAN_REGRESSION reads CPU_SPIKE and CPU_SQL_PERCENT, and
    /// QUERY_HIGH_DOP is a tuning-class key (the three waits). ACTIVE_QUERIES, PROCEDURE_STATS, MISSING_INDEX and
    /// PLAN_WARNING have no amplifier. None reads an ANOMALY_* fact.</description></item>
    /// <item><description><c>bad_actor</c> and <c>jobs</c>: BAD_ACTOR_&lt;hash&gt; and RUNNING_JOBS have no amplifier and
    /// are not tuning-class.</description></item>
    /// <item><description><c>sessions</c> and <c>coverage</c>: context sources, scored 0, so no amplifier runs. The coverage
    /// fact is the stamp that runs ahead of every family.</description></item>
    /// </list>
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, string[]> ScoringFamiliesBySource = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase)
    {
        ["queries"] = [nameof(WaitStatsAndGroupStepAsync), nameof(CollectMemoryGrantFactsAsync), nameof(CollectCpuUtilizationFactsAsync)],
        ["bad_actor"] = [],
        ["jobs"] = [],
        ["sessions"] = [],
        ["coverage"] = [],
    };

    /// <summary>
    /// #8570: the family reads a pass over <paramref name="requested"/> sources runs, in list order: every family when
    /// <paramref name="requested"/> is null or empty or holds a source <see cref="ScoringFamiliesBySource"/> does not list,
    /// else the families that write a requested source, the families their scoring reads, and every family
    /// <see cref="FamilySources"/> does not list. <paramref name="skipped"/> counts the rest.
    /// </summary>
    internal static List<Func<AnalysisContext, List<Fact>, Task>> FamiliesFor(
        IReadOnlySet<string>? requested, IReadOnlyList<Func<AnalysisContext, List<Fact>, Task>> families, out int skipped)
    {
        skipped = 0;
        if (requested is not { Count: > 0 } || requested.Any(source => !ScoringFamiliesBySource.ContainsKey(source)))
            return families.ToList();

        var needed = new HashSet<string>(requested.SelectMany(source => ScoringFamiliesBySource[source]), StringComparer.Ordinal);
        var kept = new List<Func<AnalysisContext, List<Fact>, Task>>();
        foreach (var family in families)
        {
            var name = family.Method.Name;
            if (!FamilySources.TryGetValue(name, out var sources)
                || needed.Contains(name)
                || sources.Any(source => requested.Contains(source)))
                kept.Add(family);
            else
                skipped++;
        }

        return kept;
    }

    /// <summary>
    /// #8570: whether a read over <paramref name="requested"/> sources needs the SQL Server anomaly detector's facts: always,
    /// unless every requested source is one <see cref="ScoringFamiliesBySource"/> lists and none is <c>anomaly</c>. The
    /// detector writes only <c>anomaly</c> facts, and no listed source's scoring reads one.
    /// </summary>
    public static bool NeedsAnomalyFacts(IReadOnlySet<string>? requested) =>
        requested is not { Count: > 0 }
        || requested.Any(source => !ScoringFamiliesBySource.ContainsKey(source));
}
