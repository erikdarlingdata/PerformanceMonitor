/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The v3 plumbing of #3691 (the #3715 / #3737 shape, third time): the plan, kernel and memory families exist as
/// vocabulary plus INERT stubs, every shared switch has its delegating arm, and nothing a PostgreSQL operator sees
/// has changed. Each pin here is what the content lanes (27 plans, 28 kernel, 32 memory; 29 and 30 on existing
/// files) build on and must not discover otherwise at CI: the names, the folds, the deviation-scored membership,
/// the config-advisory root, the tool rows, the marker in every stub file, and the two anomaly hooks declared in the
/// root so no content lane edits the anomaly partial. When a lane fills its family it moves ITS lines here from the
/// inert shape to the filled one (as <c>PgTargetBetweenWavesV2Tests</c> did for lane 17) and says so.
/// </summary>
public sealed class PgTargetV3PlumbingTests
{
    [Fact]
    public void TheV3Vocabulary_IsDeclaredOnce_FoldsItsAnomalies_AndRootsTheCompositionCheckAsAnAdvisory()
    {
        Assert.Equal("pg_plans", PgTargetSources.PlansSource);
        Assert.Equal("pg_kernel", PgTargetSources.KernelSource);
        Assert.Equal("pg_memory", PgTargetSources.MemorySource);
        foreach (var source in new[] { PgTargetSources.PlansSource, PgTargetSources.KernelSource, PgTargetSources.MemorySource })
        {
            Assert.Contains(source, PgTargetSources.All);
            Assert.Contains(source, FactScorer.KnownSources);
        }

        Assert.Equal("PG_PLAN_REGRESSION", PgTargetFactKeys.PlanRegression);
        Assert.Equal("PG_PARAMETER_SENSITIVITY", PgTargetFactKeys.ParameterSensitivity);
        Assert.Equal("PG_SEQ_SCAN_ADVISORY", PgTargetFactKeys.SeqScanAdvisory);
        Assert.Equal("ANOMALY_PG_PLAN_REGRESSION", PgTargetFactKeys.AnomalyPlanRegression);
        Assert.Equal("PG_CPU_BURN_CORES", PgTargetFactKeys.CpuBurnCores);
        Assert.Equal("PG_CPU_DECOMPOSITION", PgTargetFactKeys.CpuDecomposition);
        Assert.Equal("ANOMALY_PG_CPU_BURN", PgTargetFactKeys.AnomalyCpuBurn);
        Assert.Equal("CONFIG_PG_MEMORY_OVERCOMMIT", PgTargetFactKeys.ConfigMemoryOvercommit);
        Assert.Equal("PG_HOST_MEMORY_PRESSURE", PgTargetFactKeys.HostMemoryPressure);
        Assert.Equal("pg_statement_mean_ms", MetricNames.PgStatementMeanMs);
        Assert.Equal("pg_cpu_burn_cores", MetricNames.PgCpuBurnCores);

        /* The folds: each anomaly onto the regular fact that names what it deviates from. */
        Assert.Equal(new[] { PgTargetFactKeys.PlanRegression }, PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyPlanRegression]);
        Assert.Equal(new[] { PgTargetFactKeys.CpuBurnCores }, PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyCpuBurn]);
        foreach (var anomaly in new[] { PgTargetFactKeys.AnomalyPlanRegression, PgTargetFactKeys.AnomalyCpuBurn })
        {
            Assert.True(PgTargetScorer.IsDeviationScoredAnomalyKey(anomaly));
            Assert.False(PgTargetScorer.IsPgRatioAnomalyKey(anomaly));
        }

        /* D5: the §4b composition check is a convention reading and roots at the advisory base; the measured pressure
           fact beside it is not a config root. No max_wal_size-vs-disk key exists — disk free is not collected. */
        Assert.True(PgTargetFactKeys.IsConfigAdvisoryRoot(PgTargetFactKeys.ConfigMemoryOvercommit));
        Assert.False(PgTargetFactKeys.IsConfigAdvisoryRoot(PgTargetFactKeys.HostMemoryPressure));
        Assert.DoesNotContain(
            typeof(PgTargetFactKeys).GetFields().Where(f => f.IsLiteral).Select(f => (string)f.GetRawConstantValue()!),
            v => v.Contains("WAL_SIZE_VS_DISK", StringComparison.Ordinal) || v.Contains("DISK_FREE", StringComparison.Ordinal));
    }

    /// <summary>
    /// Inert through every shared entry point: a fact under each UNFILLED v3 source scores base 0 with no amplifier,
    /// composes no advice (static or through <c>FactAdvice</c>), opens no edge, has no baseline, and still has next
    /// reads (the McpSurface census requires a row the day a key is declared). The exit criterion —
    /// <c>analyze_server</c> on the gated e2e returns the SAME payload — follows: nothing here can emit, score or
    /// compose. Lane 27 filled the plan family's regression, sensitivity and anomaly arms: a BARE fact under those
    /// keys (no graded metadata) still scores 0 — the family self-gates on its own keys — and its advice, edges and
    /// baseline are pinned in <c>PgTargetPlanTests</c>; <c>PG_SEQ_SCAN_ADVISORY</c> stays lane 30's inert stub here.
    /// Lane 28 filled the kernel family: its two keys, its anomaly and its metric left the inert loop too (their live
    /// shapes are pinned in <c>PgTargetKernelTests</c>); the kernel proxy is base 0 by design and the split is 0.4, so the
    /// filled-family check below asserts the filled shape, not inertness.
    /// </summary>
    [Fact]
    public void TheV3Families_AreInertThroughEverySharedEntryPoint_UntilTheirLanesLand()
    {
        /* Lane 27's filled keys: a bare fact scores 0 (self-gated) and composes the family's static block, not null. */
        var filled = new[] { PgTargetFactKeys.PlanRegression, PgTargetFactKeys.ParameterSensitivity }
            .Select(k => new Fact { Source = PgTargetSources.PlansSource, Key = k, Value = 42, ServerId = 1 })
            .ToList();
        new FactScorer().ScoreAll(filled);
        foreach (var fact in filled)
        {
            Assert.Equal(0.0, fact.BaseSeverity);
            Assert.NotNull(PgTargetAdvice.Compose(fact.Key, filled.ToFactLookup()));
            Assert.NotNull(FactAdvice.GetForFactKey(fact.Key));
            Assert.NotEmpty(PgTargetToolRecommendations.GetForKey(fact.Key)!);
        }
        Assert.NotNull(FactAdvice.GetForFactKey(PgTargetFactKeys.AnomalyPlanRegression));
        Assert.Single(new PgTargetRelationshipGraph().GetAllEdges(PgTargetFactKeys.AnomalyPlanRegression));
        Assert.NotNull(PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgStatementMeanMs));
        Assert.Null(PgBaselineProvider.GetBaselineQuery(MetricNames.PgStatementMeanMs));

        var facts = new[]
            {
                (PgTargetSources.PlansSource, PgTargetFactKeys.SeqScanAdvisory),
                (PgTargetSources.MemorySource, PgTargetFactKeys.ConfigMemoryOvercommit),
                (PgTargetSources.MemorySource, PgTargetFactKeys.HostMemoryPressure),
            }
            .Select(t => new Fact { Source = t.Item1, Key = t.Item2, Value = 42, ServerId = 1 })
            .ToList();
        new FactScorer().ScoreAll(facts);
        var graph = new PgTargetRelationshipGraph();
        foreach (var fact in facts)
        {
            Assert.Equal(0.0, fact.BaseSeverity);
            Assert.Empty(fact.AmplifierResults);
            Assert.Null(PgTargetAdvice.Compose(fact.Key, facts.ToFactLookup()));
            Assert.Null(FactAdvice.GetForFactKey(fact.Key));
            Assert.Empty(graph.GetAllEdges(fact.Key));
            Assert.NotEmpty(PgTargetToolRecommendations.GetForKey(fact.Key)!);
        }
        /* Lane 28's kernel family is filled too: the anomaly composes its own block, the proxy's edges live on the
           decomposition (not on the anomaly — no anomaly has an edge), and the metric is served by the PostgreSQL provider
           only. No v3 anomaly or metric is a stub any longer. */
        Assert.NotNull(FactAdvice.GetForFactKey(PgTargetFactKeys.AnomalyCpuBurn));
        Assert.Empty(graph.GetAllEdges(PgTargetFactKeys.AnomalyCpuBurn));
        Assert.NotEmpty(graph.GetAllEdges(PgTargetFactKeys.CpuDecomposition));
        Assert.NotNull(PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgCpuBurnCores));
        Assert.Null(PgBaselineProvider.GetBaselineQuery(MetricNames.PgCpuBurnCores));

        /* The composition check is pg_memory-sourced and routed BY NAME ahead of the CONFIG_PG_ prefix arms, so its
           amplifiers and advice are the memory family's — the prefix arms would have handed it to the config family,
           which knows nothing of it and would compose null forever. Pinned as source text, the routing census's way. */
        var scorer = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetScorer.cs"));
        var advice = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetAdvice.cs"));
        Assert.Contains("PgTargetFactKeys.ConfigMemoryOvercommit or PgTargetFactKeys.HostMemoryPressure => MemoryAmplifiers(key),", scorer, StringComparison.Ordinal);
        Assert.True(scorer.IndexOf("=> MemoryAmplifiers(key),", StringComparison.Ordinal) < scorer.IndexOf("=> ConfigAmplifiers(key),", StringComparison.Ordinal));
        Assert.True(advice.IndexOf("=> ComposeMemory(rootFactKey, factsByKey),", StringComparison.Ordinal) < advice.IndexOf("=> ComposeConfig(rootFactKey, factsByKey),", StringComparison.Ordinal));

        /* The tool rows name only registered reads (PgTargetMcpSurfaceTests proves registration); the plan read is
           get_pg_plans, and the host-memory columns ride pg_cpu_utilization's row (V136, no pg_host_memory table), so
           the pressure fact's first read is the capacity read. */
        Assert.Equal(new[] { "get_pg_plans", "get_pg_top_queries", "get_pg_query_duration_trend" }, PgTargetToolRecommendations.GetForKey(PgTargetFactKeys.AnomalyPlanRegression)!.Select(r => r.Tool));
        Assert.Equal(new[] { "get_pg_kernel_stats", "get_pg_top_queries", "get_pg_extensions" }, PgTargetToolRecommendations.GetForKey(PgTargetFactKeys.AnomalyCpuBurn)!.Select(r => r.Tool));
        Assert.Equal("get_pg_server_config", PgTargetToolRecommendations.GetForKey(PgTargetFactKeys.ConfigMemoryOvercommit)![0].Tool);
        Assert.Equal("get_pg_cpu_utilization", PgTargetToolRecommendations.GetForKey(PgTargetFactKeys.HostMemoryPressure)![0].Tool);
    }

    /// <summary>
    /// The stubs: sixteen files, each carrying the exact marker its content brief quotes, none mentioning an
    /// Aurora-only function; the graph partial for the memory family is <c>HostMemory.cs</c> because <c>Memory.cs</c>
    /// is lane 2's v1 buffer chain; and both anomaly composers are root-declared with their <c>ComposeAnomaly</c>
    /// case, so lanes 27 and 28 never edit the anomaly partial (lanes 12 and 15 each had to; lane 17 did not).
    /// </summary>
    [Fact]
    public void TheV3Stubs_CarryTheirLaneMarkers_AndTheAnomalyHooksAreRootDeclared()
    {
        foreach (var (dir, file, lane) in new[]
        {
            ("PerformanceMonitor.Analysis", "PgTargetScorer.Plans.cs", 27), ("PerformanceMonitor.Analysis", "PgTargetAdvice.Plans.cs", 27), ("PerformanceMonitor.Analysis", "PgTargetRelationshipGraph.Plans.cs", 27),
            ("Darling/PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Plans.cs", 27), ("Darling/PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.Plans.cs", 27), ("Darling/PerformanceMonitor.Darling.Analysis", "PgTargetBaselineProvider.Plans.cs", 27),
            ("PerformanceMonitor.Analysis", "PgTargetScorer.Kernel.cs", 28), ("PerformanceMonitor.Analysis", "PgTargetAdvice.Kernel.cs", 28), ("PerformanceMonitor.Analysis", "PgTargetRelationshipGraph.Kernel.cs", 28),
            ("Darling/PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Kernel.cs", 28), ("Darling/PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.Kernel.cs", 28), ("Darling/PerformanceMonitor.Darling.Analysis", "PgTargetBaselineProvider.Kernel.cs", 28),
            ("PerformanceMonitor.Analysis", "PgTargetScorer.Memory.cs", 32), ("PerformanceMonitor.Analysis", "PgTargetAdvice.Memory.cs", 32), ("PerformanceMonitor.Analysis", "PgTargetRelationshipGraph.HostMemory.cs", 32),
            ("Darling/PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Memory.cs", 32),
        })
        {
            var text = RepoFile.ReadRepoFile(dir.Split('/').Append(file).ToArray());
            Assert.Contains($"/* filled by lane {lane}", text, StringComparison.Ordinal);
            Assert.DoesNotContain("aurora_stat_", text, StringComparison.Ordinal);
            /* D8 is about what the OPERATOR reads: no string literal in a v3 family file may carry index DDL. The doc
               comments name the prohibition; the literals must never satisfy it. */
            Assert.All(CSharpSourceWalker.StringLiteralBodies(text), literal => Assert.DoesNotContain("CREATE INDEX", literal.Text, StringComparison.OrdinalIgnoreCase));
        }
        /* Lane 30 shares the plan family's advice partial; its marker is there too. */
        Assert.Contains("lane 30", RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetAdvice.Plans.cs"), StringComparison.Ordinal);
        /* Memory.cs is the v1 buffer chain and must not have been touched into a v3 stub. */
        Assert.Contains("private partial void BuildMemoryEdges()", RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetRelationshipGraph.Memory.cs"), StringComparison.Ordinal);
        Assert.DoesNotContain("filled by lane 32", RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetRelationshipGraph.Memory.cs"), StringComparison.Ordinal);

        var anomalyAdvice = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetAdvice.Anomaly.cs"));
        Assert.Contains("case PgTargetFactKeys.AnomalyPlanRegression:", anomalyAdvice, StringComparison.Ordinal);
        Assert.Contains("return ComposePlanRegressionAnomaly(factsByKey);", anomalyAdvice, StringComparison.Ordinal);
        Assert.Contains("case PgTargetFactKeys.AnomalyCpuBurn:", anomalyAdvice, StringComparison.Ordinal);
        Assert.Contains("return ComposeCpuBurnAnomaly(factsByKey);", anomalyAdvice, StringComparison.Ordinal);
        var adviceRoot = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetAdvice.cs"));
        Assert.Contains("private static partial AdviceBlock? ComposePlanRegressionAnomaly(IReadOnlyDictionary<string, Fact> factsByKey);", adviceRoot, StringComparison.Ordinal);
        Assert.Contains("private static partial AdviceBlock? ComposeCpuBurnAnomaly(IReadOnlyDictionary<string, Fact> factsByKey);", adviceRoot, StringComparison.Ordinal);

        /* The collector emits the three families LAST, after the blocking family, so a v3 fact composed at collect time
           can read every earlier family's facts by key (the memory arithmetic reads the config knobs; the plan facts
           name lane 7's bad actors). */
        var collector = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.cs"));
        var blocking = collector.IndexOf("await CollectBlockingFactsAsync(context, facts);", StringComparison.Ordinal);
        Assert.True(blocking > 0);
        foreach (var call in new[] { "await CollectPlanFactsAsync(context, facts);", "await CollectKernelFactsAsync(context, facts);", "await CollectMemoryFactsAsync(context, facts);" })
            Assert.True(collector.IndexOf(call, StringComparison.Ordinal) > blocking, call + " must follow the blocking family");
    }
}
