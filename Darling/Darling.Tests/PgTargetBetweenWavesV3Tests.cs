/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The third between-waves batch of #3691 (the #3688 / #3737 shape): what waves 4–5 reported back, decided and made
/// as one shared-file change. Numbered as the PR body numbers them. (1) <c>get_pg_database_stats</c> surfaces the
/// V133 <c>numbackends</c> level — pinned in <c>DarlingPgDatabaseReaderTests</c>. (2) <c>get_pg_cpu_utilization</c>
/// carries the V136 host-memory columns; the alert-gate reads do not. (3) <c>PG_TEMP_SPILL → CONFIG_PG_MEMORY_OVERCOMMIT</c>
/// from the query chain's file, base-gated. (4) Four stale docs rewritten. (5) The replication e2e anchored on
/// <c>as_of</c>. (6) <c>PgTargetV3PlumbingTests</c> renamed to what it still pins. (7) <c>ANOMALY_PG_CPU_BURN</c> joins
/// the load family as the stock second confirmer. (8) The autovacuum collector reads the reloption through the
/// boolean cast — pinned in <c>Lite.Tests</c> and the live spelling class. (9) The vacuum / bloat advice says
/// "samples", not "hourly samples": the cadence is the schedule's, not the prose's.
/// </summary>
public sealed class PgTargetBetweenWavesV3Tests
{
    /* ───────────────────────── item 2: memory on the served CPU read, not on the gate ───────────────────────── */

    /// <summary>
    /// The served read is the second reader of the six memory columns; the two alert reads stay byte-for-byte what
    /// they selected before V136 (the census in <c>PgDatabaseSizeStatsAndHostMemoryRungTests</c> asserts the roster;
    /// here the reader's own shape). <c>CpuSample.Memory</c> is NOT defaulted — a null must be stated at every
    /// construction site so "this read does not carry memory" is a decision, never an omission — and the tool's
    /// description names every key it now reports and says a null is "not measured".
    /// </summary>
    [Fact]
    public void TheServedCpuRead_CarriesTheSixMemoryColumns_TheGateReadsDoNot_AndMemoryIsNeverDefaulted()
    {
        var six = new[] { "memory_total_bytes", "memory_free_bytes", "memory_cached_bytes", "memory_buffers_bytes", "memory_active_bytes", "configured_memory_bytes" };
        foreach (var column in six)
        {
            Assert.Contains(column, DarlingPgCpuUtilizationReader.HistorySql, StringComparison.Ordinal);
            Assert.DoesNotContain(column, DarlingPgCpuUtilizationReader.LatestCpuSql, StringComparison.Ordinal);
            Assert.DoesNotContain(column, DarlingPgCpuUtilizationReader.SamplesSinceSql, StringComparison.Ordinal);
        }

        var ctor = Assert.Single(typeof(DarlingPgCpuUtilizationReader.CpuSample).GetConstructors());
        var memory = Assert.Single(ctor.GetParameters(), p => p.Name == "Memory");
        Assert.False(memory.HasDefaultValue, "CpuSample.Memory gained a default — a null must be stated, never omitted");
        Assert.Equal(typeof(DarlingPgCpuUtilizationReader.HostMemory), memory.ParameterType);
        Assert.Equal(6, typeof(DarlingPgCpuUtilizationReader.HostMemory).GetProperties(BindingFlags.Public | BindingFlags.Instance).Count(p => p.PropertyType == typeof(long?)));

        /* #4193 moved GetPgCpuUtilization behind an MCP-facing overload plus an internal budget-taking one, so
           GetMethod("GetPgCpuUtilization") is ambiguous now — resolved by the MCP registration instead, the
           idiom PgLogEventMetricsTests already uses for the same shape. */
        var description = typeof(DarlingMcpPgCpuUtilizationTools).GetMethods()
            .Single(m => m.GetCustomAttribute<ModelContextProtocol.Server.McpServerToolAttribute>()?.Name == "get_pg_cpu_utilization")
            .GetCustomAttributes<DescriptionAttribute>(inherit: false).Single().Description;
        foreach (var column in six)
            Assert.Contains(column, description, StringComparison.Ordinal);
        Assert.Contains("memory_samples_in_bucket", description, StringComparison.Ordinal);
        Assert.Contains("null means not measured, never zero memory", description, StringComparison.Ordinal);

        /* #4193: aggregation moved into the bucketed SQL (HistoryBucketedSql), so the tool's projection now
           reads each column off the reader's own CpuBucketPoint rather than folding raw rows with MeanBytes. */
        var reader = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "DarlingPgCpuUtilizationReader.cs");
        foreach (var column in six)
            Assert.Contains($"{column},", reader, StringComparison.Ordinal);
        var tool = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpPgCpuUtilizationTools.cs");
        foreach (var column in six)
            Assert.Contains($"{column} = p.Memory?.", tool, StringComparison.Ordinal);
        Assert.Contains("memory_samples_in_bucket = p.MemorySamples", tool, StringComparison.Ordinal);
    }

    /* ───────────────────────── item 3: the spill leads to the composition check ───────────────────────── */

    /// <summary>
    /// The edge lane 32 asked for, declared where the spill's edges live: the query chain's file, under the temp
    /// chain's category, base-gated on the SUM's verdict (never a ratio of its own). Open only when the sum's base is
    /// positive; inert when the sum is absent (a stock target) or reads 0 (the configured worst case fits the box).
    /// The <c>work_mem</c> edge beside it is unchanged.
    /// </summary>
    [Fact]
    public void TheSpill_LeadsToTheMemoryCompositionCheck_OnlyWhenTheSumsBaseIsPositive_FromTheQueryChainsFile()
    {
        var graph = new PgTargetRelationshipGraph();
        var edges = graph.GetAllEdges(PgTargetFactKeys.TempSpill).Where(e => !PgTargetRelationshipGraph.IsBadActorAlias(e.Destination)).ToList();
        Assert.Equal(2, edges.Count);
        Assert.Contains(edges, e => e.Destination == PgTargetFactKeys.ConfigWorkMem);
        var toSum = Assert.Single(edges, e => e.Destination == PgTargetFactKeys.ConfigMemoryOvercommit);
        Assert.Equal("temp_spill", toSum.Category);
        Assert.Contains("work_mem term is being spent", toSum.PredicateDescription, StringComparison.Ordinal);

        var fired = Sum(baseSeverity: 0.4);
        Assert.Contains(graph.GetActiveEdges(PgTargetFactKeys.TempSpill, Lookup(Spill(), fired)), e => e.Destination == PgTargetFactKeys.ConfigMemoryOvercommit);

        var fits = Sum(baseSeverity: 0.0);
        fits.Severity = 0.9;   /* an inflated Severity on a base-0 sum opens nothing: the predicate reads the base */
        Assert.DoesNotContain(graph.GetActiveEdges(PgTargetFactKeys.TempSpill, Lookup(Spill(), fits)), e => e.Destination == PgTargetFactKeys.ConfigMemoryOvercommit);
        Assert.DoesNotContain(graph.GetActiveEdges(PgTargetFactKeys.TempSpill, Lookup(Spill())), e => e.Destination == PgTargetFactKeys.ConfigMemoryOvercommit);

        /* Nothing leaves the sum: it stays the leaf. */
        Assert.Empty(graph.GetAllEdges(PgTargetFactKeys.ConfigMemoryOvercommit));

        var source = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetRelationshipGraph.Query.cs");
        Assert.Contains("AddEdge(PgTargetFactKeys.TempSpill, PgTargetFactKeys.ConfigMemoryOvercommit, TempCategory,", source, StringComparison.Ordinal);
        Assert.DoesNotContain("PgTargetFactKeys.TempSpill, PgTargetFactKeys.ConfigMemoryOvercommit", RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetRelationshipGraph.HostMemory.cs"), StringComparison.Ordinal);
    }

    /// <summary>
    /// The story shapes. A spill outranking a quiet <c>work_mem</c> beside a positive sum walks to the sum
    /// (<c>[PG_TEMP_SPILL, CONFIG_PG_MEMORY_OVERCOMMIT]</c>); when <c>PG_HOST_MEMORY_PRESSURE</c> outranks the spill it
    /// claims the sum first and the spill's story cannot reuse the consumed leaf — the outcome the live memory e2e
    /// pins on the planted Aurora host — so the sum sits in exactly one path either way.
    /// </summary>
    [Fact]
    public void TheSpillStory_ReachesTheSumWhereNothingHigherClaimedIt_AndNeverDuplicatesAConsumedLeaf()
    {
        var engine = new InferenceEngine(new PgTargetRelationshipGraph());

        var spill = Spill(); spill.BaseSeverity = 0.9; spill.Severity = 0.9;
        var sum = Sum(baseSeverity: 0.4); sum.Severity = 0.5;
        var stories = engine.BuildStories([spill, sum]);
        var spillStory = Assert.Single(stories, s => s.RootFactKey == PgTargetFactKeys.TempSpill);
        Assert.Equal([PgTargetFactKeys.TempSpill, PgTargetFactKeys.ConfigMemoryOvercommit], spillStory.Path);
        Assert.DoesNotContain(stories, s => s.RootFactKey == PgTargetFactKeys.ConfigMemoryOvercommit);

        var pressure = new Fact { Source = PgTargetSources.MemorySource, Key = PgTargetFactKeys.HostMemoryPressure, Value = 0.05, ServerId = 1, BaseSeverity = 0.95, Severity = 1.2 };
        var spill2 = Spill(); spill2.BaseSeverity = 0.9; spill2.Severity = 0.9;
        var sum2 = Sum(baseSeverity: 0.4); sum2.Severity = 0.6;
        var claimed = engine.BuildStories([spill2, sum2, pressure]);
        Assert.Equal([PgTargetFactKeys.HostMemoryPressure, PgTargetFactKeys.ConfigMemoryOvercommit], Assert.Single(claimed, s => s.RootFactKey == PgTargetFactKeys.HostMemoryPressure).Path);
        Assert.Equal([PgTargetFactKeys.TempSpill], Assert.Single(claimed, s => s.RootFactKey == PgTargetFactKeys.TempSpill).Path);
        Assert.Single(claimed, s => s.Path.Contains(PgTargetFactKeys.ConfigMemoryOvercommit));
    }

    /* ───────────────────────── item 4: the four docs ───────────────────────── */

    [Fact]
    public void TheFourStaleDocs_SayWhatIsTrueNow()
    {
        var keys = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetFactKeys.cs");
        /* The doc block ABOVE the const: from the end of the previous declaration to the const itself. */
        var declaration = keys.IndexOf("public const string SeqScanAdvisory", StringComparison.Ordinal);
        var previous = keys.LastIndexOf("public const string", declaration - 1, StringComparison.Ordinal);
        var seqScanDoc = keys[previous..declaration];
        Assert.Contains("EVIDENCE FIRST", seqScanDoc, StringComparison.Ordinal);
        Assert.Contains("CORROBORATION, never the driver", seqScanDoc, StringComparison.Ordinal);
        Assert.Contains("regressions elsewhere", seqScanDoc, StringComparison.Ordinal);
        Assert.Contains("pg_qualstats", seqScanDoc, StringComparison.Ordinal);
        Assert.DoesNotContain("no-missing-index-folklore", keys, StringComparison.Ordinal);

        var provider = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgBaselineProvider.cs");
        Assert.Contains("The first of the three seams a derived provider overrides", provider, StringComparison.Ordinal);
        Assert.DoesNotContain("The one seam a derived provider overrides", provider, StringComparison.Ordinal);

        /* Lane 37 wrote these two docs as "the keyed seam exists since #3810; switching the anomaly to per-queryid is
           lane 27's follow-up". Lane 39 DID the switch on calibration D's evidence, so the follow-up sentence is gone
           and the docs now say what the detector does: the keyed series is the instrument, the server-wide arm the
           cold fallback. The pin follows the truth: #3810 still named as the seam, the keyed reading named as live. */
        foreach (var file in new[] { "PgTargetAnomalyDetector.Plans.cs", "PgTargetBaselineProvider.Plans.cs" })
        {
            var text = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", file);
            Assert.Contains("#3810", text, StringComparison.Ordinal);
            Assert.Contains("keyed", text, StringComparison.Ordinal);
            Assert.DoesNotContain("lane 27's follow-up", text, StringComparison.Ordinal);
        }

        var detector = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.cs");
        Assert.Contains("gets no anomaly of ANY family", detector, StringComparison.Ordinal);
        Assert.Contains("HasBaselineDataSql", detector[..detector.IndexOf("public sealed partial class PgTargetAnomalyDetector", StringComparison.Ordinal)], StringComparison.Ordinal);
    }

    /* ───────────────────────── item 5: the replication e2e is anchored ───────────────────────── */

    /// <summary>The flake's mechanism was two clocks: the fixture's <c>windowEnd</c> and the tool's own wall clock. One
    /// clock now — every <c>analyze_server</c> / <c>get_analysis_facts</c> call in the e2e passes <c>as_of</c>.</summary>
    [Fact]
    public void TheReplicationE2e_PassesAsOf_ToEveryToolCall()
    {
        var text = RepoFile.ReadRepoFile("Darling", "Darling.Tests", "PgTargetReplicationTests.cs");
        var body = text[text.IndexOf("ADriftingStandbyAndAnInactiveGrowingSlot_ProduceBothStories", StringComparison.Ordinal)..];
        var calls = Regex.Matches(body, @"DarlingMcpTools\.(AnalyzeServer|GetAnalysisFacts)\([^;]*\);");
        Assert.Equal(2, calls.Count);
        Assert.All(calls, m => Assert.Contains("as_of: asOf", m.Value, StringComparison.Ordinal));
        Assert.Contains("var asOf = windowEnd.ToString(", body, StringComparison.Ordinal);
    }

    /* ───────────────────────── item 6: the plumbing test is named for what it pins ───────────────────────── */

    [Fact]
    public void TheV3PlumbingTest_NoLongerClaimsInertness()
    {
        var text = RepoFile.ReadRepoFile("Darling", "Darling.Tests", "PgTargetV3PlumbingTests.cs");
        Assert.DoesNotContain("public void TheV3Families_AreInertThroughEverySharedEntryPoint_UntilTheirLanesLand", text, StringComparison.Ordinal);
        Assert.Contains("public void TheV3Families_AreFilled_SelfGateOnBareFacts_AndRouteTheMemoryCheckByNameAheadOfThePrefixArms", text, StringComparison.Ordinal);
    }

    /* ───────────────────────── item 7: the stock second confirmer ───────────────────────── */

    /// <summary>
    /// Lane 36's measurement, made: on a stock target (no <c>PG_CPU_PERCENT</c>, no capacity anomaly) an extreme TPS
    /// anomaly beside a fired session spike AND a fired CPU-burn anomaly reads 1.0 × (1 + 0.3 + 0.3) = 1.6 and pages
    /// — two independent corroborators, #3584's bar — where it read 1.3 with the burn firing unread beside it. The
    /// same for the session spike as root. The burn anomaly takes the load arms in return, and its fold gains the
    /// decomposition beside the base-0 cores fact.
    /// </summary>
    [Fact]
    public void OnStock_AnExtremeLoadAnomaly_WithASiblingAndTheBurnAnomaly_Reads1Point6()
    {
        var tps = Anomaly(PgTargetFactKeys.AnomalyTps, ("deviation_sigma", 10.5), ("fire_threshold", 3.5), ("baseline_low_quality", 0), ("confidence", 1.0));
        var sessions = Anomaly(PgTargetFactKeys.AnomalySessionSpike, ("deviation_sigma", 4.0), ("fire_threshold", 3.5));
        var burn = Anomaly(PgTargetFactKeys.AnomalyCpuBurn, ("deviation_sigma", 25.0), ("fire_threshold", 3.5));
        var facts = new List<Fact> { tps, sessions, burn };
        new FactScorer().ScoreAll(facts);

        Assert.Equal(1.0, tps.BaseSeverity, precision: 9);
        Assert.Equal(1.6, tps.Severity, precision: 9);
        Assert.Equal(2, tps.AmplifierResults.Count(r => r.Matched));
        Assert.Contains(tps.AmplifierResults, r => r.Matched && r.Description.StartsWith("CPU-burn anomaly co-fired", StringComparison.Ordinal));
        Assert.Contains(tps.AmplifierResults, r => !r.Matched && r.Description.StartsWith("Instance CPU at or past the capacity warning bar", StringComparison.Ordinal));

        /* Without the burn: the 1.30 lane 36 measured — extreme plus one corroborator. */
        var tpsAlone = Anomaly(PgTargetFactKeys.AnomalyTps, ("deviation_sigma", 10.5), ("fire_threshold", 3.5), ("baseline_low_quality", 0), ("confidence", 1.0));
        new FactScorer().ScoreAll([tpsAlone, Anomaly(PgTargetFactKeys.AnomalySessionSpike, ("deviation_sigma", 4.0), ("fire_threshold", 3.5))]);
        Assert.Equal(1.3, tpsAlone.Severity, precision: 9);

        /* The session spike as root, same set. */
        var spike = Anomaly(PgTargetFactKeys.AnomalySessionSpike, ("deviation_sigma", 10.5), ("fire_threshold", 3.5), ("baseline_low_quality", 0), ("confidence", 1.0));
        new FactScorer().ScoreAll([spike, Anomaly(PgTargetFactKeys.AnomalyTps, ("deviation_sigma", 4.0), ("fire_threshold", 3.5)), Anomaly(PgTargetFactKeys.AnomalyCpuBurn, ("deviation_sigma", 25.0), ("fire_threshold", 3.5))]);
        Assert.Equal(1.6, spike.Severity, precision: 9);

        /* Symmetry: the burn anomaly takes the load arms — extreme burn beside two fired siblings pages too. */
        var burnRoot = Anomaly(PgTargetFactKeys.AnomalyCpuBurn, ("deviation_sigma", 25.0), ("fire_threshold", 3.5), ("baseline_low_quality", 0), ("confidence", 1.0));
        new FactScorer().ScoreAll([burnRoot, Anomaly(PgTargetFactKeys.AnomalyTps, ("deviation_sigma", 4.0), ("fire_threshold", 3.5)), Anomaly(PgTargetFactKeys.AnomalySessionSpike, ("deviation_sigma", 4.0), ("fire_threshold", 3.5))]);
        Assert.Equal(1.6, burnRoot.Severity, precision: 9);
        Assert.Equal(4, burnRoot.AmplifierResults.Count);   /* session, TPS, capacity anomaly, the capacity fact — never itself */
        Assert.DoesNotContain(burnRoot.AmplifierResults, r => r.Description.StartsWith("CPU-burn anomaly co-fired", StringComparison.Ordinal));

        /* A burn that did not fire confirms nothing: the predicate reads the base, not the presence. */
        var tpsWithQuietBurn = Anomaly(PgTargetFactKeys.AnomalyTps, ("deviation_sigma", 10.5), ("fire_threshold", 3.5), ("baseline_low_quality", 0), ("confidence", 1.0));
        new FactScorer().ScoreAll([tpsWithQuietBurn, Anomaly(PgTargetFactKeys.AnomalySessionSpike, ("deviation_sigma", 4.0), ("fire_threshold", 3.5)), Anomaly(PgTargetFactKeys.AnomalyCpuBurn, ("deviation_sigma", 1.0), ("fire_threshold", 3.5))]);
        Assert.Equal(1.3, tpsWithQuietBurn.Severity, precision: 9);

        Assert.Equal(new[] { PgTargetFactKeys.CpuBurnCores, PgTargetFactKeys.CpuDecomposition }, PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyCpuBurn]);
    }

    /* ───────────────────────── item 9: the cadence is the schedule's ───────────────────────── */

    [Fact]
    public void TheVacuumAndBloatAdvice_SaySamples_NotHourlySamples()
    {
        foreach (var file in new[] { "PgTargetAdvice.Vacuum.cs", "PgTargetAdvice.Bloat.cs" })
        {
            var text = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", file);
            Assert.DoesNotContain("hourly samples", text, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("samples", text, StringComparison.Ordinal);
        }
    }

    /* ───────────────────────── helpers ───────────────────────── */

    private static Fact Anomaly(string key, params (string Name, double Value)[] metadata)
    {
        var fact = new Fact { Source = PgTargetAnomalyDetector.AnomalySource, Key = key, Value = 1, ServerId = 1 };
        foreach (var (name, value) in metadata)
            fact.Metadata[name] = value;
        return fact;
    }

    private static Fact Spill() => new()
    {
        Source = PgTargetSources.TempSource, Key = PgTargetFactKeys.TempSpill, Value = 2_097_152, ServerId = 1, DatabaseName = "appdb",
        Metadata = { [PgTargetScorer.TempSpillBytesPerSecKey] = 2_097_152, [PgTargetScorer.CounterObservedMsKey] = 14_400_000 },
    };

    private static Fact Sum(double baseSeverity) => new()
    {
        Source = PgTargetSources.MemorySource, Key = PgTargetFactKeys.ConfigMemoryOvercommit, Value = 1.5, ServerId = 1,
        BaseSeverity = baseSeverity, Severity = baseSeverity,
    };

    private static Dictionary<string, Fact> Lookup(params Fact[] facts) =>
        facts.ToDictionary(f => f.Key, f => f, StringComparer.Ordinal);
}
