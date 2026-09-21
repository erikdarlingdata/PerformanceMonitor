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
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Lane 28 of #3691 — the kernel family: <c>PG_CPU_BURN_CORES</c> (the self-hosted CPU proxy, cores busy from
/// <c>pg_stat_kcache</c>, graded only by <c>ANOMALY_PG_CPU_BURN</c>), <c>PG_CPU_DECOMPOSITION</c> (burning versus
/// waiting) and the anomaly behind them.
///
/// <para><b>The careful test is the honesty arm.</b> Every PostgreSQL target the fleet monitors today is Aurora, and
/// Aurora does not ship <c>pg_stat_kcache</c>: the measured population for this family is EMPTY. So the shape that
/// runs on every real pass is "the extension is absent", and what that must produce is exactly ONE <c>unavailable</c>
/// fact with the reason — no cores, no zeros, no decomposition, no anomaly — read from
/// <c>pg_extension_availability</c>, never inferred from the empty table. Executed here on arranged rows and, gated,
/// on a throwaway store against the REAL <c>analyze_server</c>.</para>
///
/// <para>Every bar is unmeasured (there was nothing to measure) and says so: the scorer's constants carry the marker
/// (<c>PgTargetThresholdLineageTests</c> scans them), the <c>AnomalyThresholds</c> pair carries it (the lane-9 pin over
/// every <c>Pg*</c> constant), and the decomposition and the anomaly stamp <c>threshold_lineage = 0</c>. The proxy
/// stamps 1 — the <c>PG_WAL_VOLUME_SHIFT</c> rule: no bar was chosen to have a lineage.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetKernelTests
{
    private const string AuroraServerName = "darling-pg-target-kernel-aurora";
    private static readonly int AuroraServerId = ServerIdHelper.GetDeterministicHashCode(AuroraServerName);
    private const string StockServerName = "darling-pg-target-kernel-stock";
    private static readonly int StockServerId = ServerIdHelper.GetDeterministicHashCode(StockServerName);

    /* ───────────────────────── the reads ───────────────────────── */

    [Fact]
    public void TheTwoReads_AreInAllSql_PerIdentityResetAware_AvailabilityByName_AndTheDetectorReadsTheCollectorsTextByAlias()
    {
        Assert.Contains(PgTargetFactCollector.PgTargetKernelCpuSql, PgTargetFactCollector.AllSql);
        Assert.Contains(PgTargetFactCollector.PgTargetKernelAvailabilitySql, PgTargetFactCollector.AllSql);
        Assert.Same(PgTargetFactCollector.PgTargetKernelCpuSql, PgTargetAnomalyDetector.CpuBurnWindowSql);

        var cpu = PgTargetFactCollector.PgTargetKernelCpuSql;
        Assert.Contains("FROM pg_kernel_stats", cpu, StringComparison.Ordinal);
        /* Per identity, the reset by stamp OR by the counter going backwards, the value taken WHOLE on a reset. */
        Assert.Contains("PARTITION BY database_name, query_id ORDER BY collection_time", cpu, StringComparison.Ordinal);
        Assert.Contains("stats_since IS DISTINCT FROM prev_since OR user_ms_total < prev_user", cpu, StringComparison.Ordinal);
        Assert.Contains("THEN user_ms_total   ELSE user_ms_total   - prev_user", cpu, StringComparison.Ordinal);
        Assert.DoesNotContain("GREATEST(user_ms_total", cpu, StringComparison.Ordinal);
        /* A delta counts only against the immediately preceding collection (the top-500 re-entry rule), by the window's
           DENSE_RANK ordinal — never a join back to a distinct-times CTE (the shape that ran past the baseline timeout). */
        Assert.Contains("DENSE_RANK() OVER (ORDER BY collection_time) AS k", cpu, StringComparison.Ordinal);
        Assert.Contains("WHERE prev_k = k - 1", cpu, StringComparison.Ordinal);
        Assert.DoesNotContain("JOIN", cpu, StringComparison.Ordinal);
        /* Rated per collection over its own gap: cores busy. */
        Assert.Contains("(SUM(user_ms) + SUM(system_ms) + SUM(plan_ms)) / (interval_sec * 1000.0) AS cores_busy", cpu, StringComparison.Ordinal);
        Assert.Contains("query_id IS NOT NULL", cpu, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", cpu, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", cpu, StringComparison.OrdinalIgnoreCase);

        var availability = PgTargetFactCollector.PgTargetKernelAvailabilitySql;
        Assert.Contains("FROM pg_extension_availability", availability, StringComparison.Ordinal);
        Assert.Contains("extension_name = 'pg_stat_kcache'", availability, StringComparison.Ordinal);
        Assert.Contains("a.state IN ('installed', 'outdated')", availability, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time)", availability, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", availability, StringComparison.Ordinal);
        Assert.Equal(3, PgTargetFactCollector.KernelAvailabilityLookbackDays);

        /* The collector reads availability BEFORE the rows, and the kernel family runs last in the root. */
        var source = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Kernel.cs"));
        Assert.True(source.IndexOf("PgTargetKernelAvailabilitySql, connection", StringComparison.Ordinal) > 0);
        Assert.True(source.IndexOf("ReadKernelAvailabilityAsync(context, connection)", StringComparison.Ordinal) < source.IndexOf("PgTargetKernelCpuSql, connection", StringComparison.Ordinal));
        Assert.Contains("catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))", source, StringComparison.Ordinal);
        Assert.Contains("ReportCollectionFailure(ex, context);", source, StringComparison.Ordinal);
        Assert.Contains("CommandTimeout = FactCommandTimeoutSeconds", source, StringComparison.Ordinal);
        Assert.DoesNotContain("engine_kind", source, StringComparison.Ordinal);
        Assert.DoesNotContain("DateTime.UtcNow", source, StringComparison.Ordinal);
    }

    [Fact]
    public void TheBaselineArm_IsServed_EndsInTheOneScaffold_HalfOpen_PerIdentity_AndFloorsRetention()
    {
        var sql = PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgCpuBurnCores);
        Assert.NotNull(sql);
        Assert.Null(PgBaselineProvider.GetBaselineQuery(MetricNames.PgCpuBurnCores));
        Assert.EndsWith(PgBaselineProvider.RobustTierScaffold, sql, StringComparison.Ordinal);
        Assert.Contains("clean AS (", sql, StringComparison.Ordinal);
        Assert.Contains("FROM pg_kernel_stats", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1 AND collection_time >= $2 AND collection_time < $3", sql, StringComparison.Ordinal);
        Assert.Contains("PARTITION BY database_name, query_id ORDER BY collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("::DOUBLE PRECISION AS v", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE prev_k = k - 1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("JOIN", sql![..sql.IndexOf(PgBaselineProvider.RobustTierScaffold, StringComparison.Ordinal)], StringComparison.Ordinal);
        /* Q6: the scaffold keys the hour-of-week through the root's clock params, never the arm's own EXTRACT. */
        Assert.DoesNotMatch(new Regex(@"EXTRACT\s*\(\s*(HOUR|DOW|ISODOW)", RegexOptions.IgnoreCase), sql![..sql.IndexOf(PgBaselineProvider.RobustTierScaffold, StringComparison.Ordinal)]);
        Assert.DoesNotContain("now(", sql, StringComparison.OrdinalIgnoreCase);

        Assert.Contains("pg_kernel_stats", DarlingRetentionHorizons.BaselineServingRawCollectors);
        Assert.True(PgTargetScorer.IsDeviationScoredAnomalyKey(PgTargetFactKeys.AnomalyCpuBurn));
        /* The fold gained the decomposition in the third between-waves batch of #3691 (lane 36's measurement): the cores
           fact is base 0 by design, so a fold onto it alone never landed. Pinned with the reason in PgTargetBetweenWavesV3Tests. */
        Assert.Equal(new[] { PgTargetFactKeys.CpuBurnCores, PgTargetFactKeys.CpuDecomposition }, PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyCpuBurn]);
    }

    [Fact]
    public void TheBars_AreUnmeasured_InCores_AndNoSqlServerCpuConstantReusedByValue()
    {
        Assert.Equal(0.5, AnomalyThresholds.PgCpuBurnCoresFloor);
        Assert.Equal(4.0, AnomalyThresholds.PgCpuBurnCoresFallback);
        Assert.True(AnomalyThresholds.PgCpuBurnCoresFallback > AnomalyThresholds.PgCpuBurnCoresFloor);
        /* The SQL Server CPU family is in PERCENT; the PostgreSQL proxy is in cores. Neither bar may be one of theirs by value. */
        foreach (var sqlServerBar in new[] { AnomalyThresholds.PgCpuFloorPct, AnomalyThresholds.PgCpuFallbackPct, PgTargetScorer.CpuCapacityWarningPercent, PgTargetScorer.CpuCapacityCriticalPercent })
        {
            Assert.NotEqual(AnomalyThresholds.PgCpuBurnCoresFloor, sqlServerBar);
            Assert.NotEqual(AnomalyThresholds.PgCpuBurnCoresFallback, sqlServerBar);
        }
        Assert.Equal(0.4, PgTargetScorer.CpuDecompositionInformational);
        Assert.Equal(0.8, PgTargetScorer.CpuDecompositionDominantShare);
        Assert.Equal(0.5, PgTargetScorer.CpuDecompositionCoFireBoost);
        Assert.Equal(0.6, PgTargetScorer.CpuDecompositionInformational * (1 + PgTargetScorer.CpuDecompositionCoFireBoost), precision: 9);

        var thresholds = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "Baselines", "AnomalyThresholds.cs");
        foreach (var name in new[] { "PgCpuBurnCoresFloor", "PgCpuBurnCoresFallback" })
        {
            var at = thresholds.IndexOf($"public const double {name}", StringComparison.Ordinal);
            Assert.True(at > 0, name);
            Assert.Contains("unmeasured: chosen, not measured", thresholds[Math.Max(0, at - 1400)..at], StringComparison.Ordinal);
        }
    }

    /* ───────────────────────── the arithmetic, executed on arranged rows ───────────────────────── */

    [Fact]
    public void CoresBusy_IsCpuOverTheSourcesOwnRatedTime_WithShares_TopStatement_AndTheProxyIsContext()
    {
        var context = Context(observedMs: 4 * 3_600_000);
        var facts = new List<Fact>();
        /* 240 rated minutes; 5 cores: 300,000 ms of CPU per 60 s. user 255k, system 45k, plan 3k per minute. */
        var row = new PgTargetFactCollector.KernelCpuRow(
            PeakCores: 5.2, MeanCores: 5.05, RatedSamples: 240,
            UserMs: 240 * 255_000.0, SystemMs: 240 * 45_000.0, PlanMs: 240 * 3_000.0, RatedSec: 240 * 60,
            ResetCount: 1, CollectionCount: 241,
            TopQueryId: 7001, TopQueryCpuMs: 240 * 230_000.0, TopQueryDatabase: "app");

        PgTargetFactCollector.EmitKernelFacts(context, facts, row);

        var burn = Assert.Single(facts);
        Assert.Equal(PgTargetSources.KernelSource, burn.Source);
        Assert.Equal(PgTargetFactKeys.CpuBurnCores, burn.Key);
        Assert.Equal(303_000.0 / 60_000.0, burn.Value, precision: 9);                       /* 5.05 cores */
        Assert.Equal(5.2, burn.Metadata[PgTargetScorer.KernelCoresBusyPeakKey]);
        Assert.Equal(5.05, burn.Metadata[PgTargetScorer.KernelCoresBusyMeanKey]);
        Assert.Equal(255_000.0 / 300_000.0, burn.Metadata[PgTargetScorer.KernelUserShareKey], precision: 9);
        Assert.Equal(3_000.0 / 303_000.0, burn.Metadata[PgTargetScorer.KernelPlanCpuShareKey], precision: 9);
        Assert.Equal(7001, burn.Metadata[PgTargetScorer.KernelTopQueryIdKey]);
        Assert.Equal(230_000.0 / 303_000.0, burn.Metadata[PgTargetScorer.KernelTopQueryShareKey], precision: 9);
        Assert.Equal("app", burn.ObjectName);
        Assert.Null(burn.DatabaseName);                                                     /* server-scoped: the fold key stays empty */
        Assert.Equal(240 * 60_000.0, burn.Metadata[PgTargetScorer.KernelObservedMsKey]);
        Assert.Equal(4 * 3_600_000, burn.Metadata["observed_ms"]);
        Assert.Equal(burn.Value, burn.Metadata["witness_cores_busy"], precision: 9);          /* equal cadences → equal figures */
        Assert.Equal(1, burn.Metadata["reset_count"]);
        Assert.Equal(1, burn.Metadata["threshold_lineage"]);

        /* Context: base 0 whatever the cores, no amplifier, no lineage stamp added by the scorer. */
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(burn));
        new FactScorer().ScoreAll(facts);
        Assert.Equal(0.0, burn.Severity);
        Assert.Empty(burn.AmplifierResults);

        /* No wait fact in the pass → no decomposition (one side unknown). */
        Assert.DoesNotContain(facts, f => f.Key == PgTargetFactKeys.CpuDecomposition);
    }

    [Fact]
    public void TheDecomposition_ReadsTheWaitFactsAlreadyEmitted_NormalisesBothSidesToRates_AndNamesTheGrade()
    {
        var context = Context(observedMs: 4 * 3_600_000);
        /* The wait partial emitted first: Lock:relation 0.1 of a backend (share 0.5 of all waiting) and the IO rollup
           0.1 (share 0.5), over the wait source's own 14,400 s — sampled. Total waiting = 0.2 backends. */
        var facts = new List<Fact>
        {
            WaitFact(PgTargetFactKeys.WaitKey("Lock", "relation"), waitMs: 1_440_000, shareOfWait: 0.5, observedMs: 14_400_000, sampled: true),
            WaitFact(PgTargetFactKeys.WaitKey("IO", null), waitMs: 1_440_000, shareOfWait: 0.5, observedMs: 14_400_000, sampled: true),
        };
        /* 4.8 cores over the kernel source's own 12,000 s (it observed less than the wait source did). */
        var row = new PgTargetFactCollector.KernelCpuRow(4.9, 4.8, 200, 4.8 * 12_000_000.0, 0, 0, 12_000, 0, 201, 7001, 3_000_000, null);

        PgTargetFactCollector.EmitKernelFacts(context, facts, row);

        var split = Assert.Single(facts, f => f.Key == PgTargetFactKeys.CpuDecomposition);
        Assert.Equal(PgTargetSources.KernelSource, split.Source);
        Assert.Equal(4.8 / 5.0, split.Value, precision: 9);
        Assert.Equal(4.8 / 5.0, split.Metadata[PgTargetScorer.KernelBurnShareKey], precision: 9);
        Assert.Equal(0.2 / 5.0, split.Metadata[PgTargetScorer.KernelWaitShareKey], precision: 9);
        Assert.Equal(0.1 / 5.0, split.Metadata[PgTargetScorer.KernelIoWaitShareKey], precision: 9);
        Assert.Equal(4.8, split.Metadata[PgTargetScorer.KernelCoresBusyKey], precision: 9);
        Assert.Equal(0.2, split.Metadata[PgTargetScorer.KernelBackendsWaitingKey], precision: 9);
        Assert.Equal(2_880_000, split.Metadata["wait_ms"], precision: 6);
        Assert.Equal(14_400_000, split.Metadata[PgTargetScorer.WaitSourceObservedMsKey]);
        Assert.Equal(12_000_000, split.Metadata[PgTargetScorer.KernelObservedMsKey]);
        Assert.Equal(1, split.Metadata[PgTargetScorer.KernelWaitIsSampledKey]);

        /* Informational base, lineage 0; without a co-fire it stays there and roots nothing. */
        Assert.Equal(0.4, PgTargetScorer.ScoreBase(split));
        Assert.Equal(0, split.Metadata["threshold_lineage"]);
        new FactScorer().ScoreAll(facts);
        Assert.Equal(0.4, split.Severity, precision: 9);
        Assert.All(split.AmplifierResults, r => Assert.False(r.Matched));
    }

    [Fact]
    public void ComputeBound_PlusTheBurnAnomaly_LiftsTheDecompositionTo0Point6_AndNothingElseDoes()
    {
        /* Compute-bound + anomaly fired → 0.6. */
        var split = Split(burnShare: 0.9);
        var anomaly = FiredBurnAnomaly(sigma: 8.0);
        new FactScorer().ScoreAll([split, anomaly]);
        Assert.True(anomaly.BaseSeverity > 0);
        Assert.Equal(0.6, split.Severity, precision: 9);
        Assert.Single(split.AmplifierResults, r => r.Matched);

        /* Compute-bound, anomaly absent → 0.4. */
        split = Split(burnShare: 0.9);
        new FactScorer().ScoreAll([split]);
        Assert.Equal(0.4, split.Severity, precision: 9);

        /* Anomaly fired, but the split is mixed (0.79) → 0.4: the line is the line. */
        split = Split(burnShare: 0.79);
        new FactScorer().ScoreAll([split, FiredBurnAnomaly(sigma: 8.0)]);
        Assert.Equal(0.4, split.Severity, precision: 9);

        /* Wait-bound + a fired wait → 0.6; wait-bound + a wait below its bar → 0.4. */
        split = Split(burnShare: 0.1);
        var firedWait = WaitFact(PgTargetFactKeys.WaitKey("Lock", "relation"), waitMs: 2_880_000, shareOfWait: 1.0, observedMs: 14_400_000, sampled: false);
        new FactScorer().ScoreAll([split, firedWait]);
        Assert.True(firedWait.BaseSeverity > 0, "a 0.2 relation-lock fraction is past the standout bar");
        Assert.Equal(0.6, split.Severity, precision: 9);

        split = Split(burnShare: 0.1);
        var quietWait = WaitFact(PgTargetFactKeys.WaitKey("Lock", "relation"), waitMs: 144_000, shareOfWait: 1.0, observedMs: 14_400_000, sampled: false);
        new FactScorer().ScoreAll([split, quietWait]);
        Assert.Equal(0.0, quietWait.BaseSeverity);
        Assert.Equal(0.4, split.Severity, precision: 9);

        /* The proxy is never lifted: base 0, amplifiers never run. */
        var burn = new Fact { Source = PgTargetSources.KernelSource, Key = PgTargetFactKeys.CpuBurnCores, Value = 9, ServerId = 1 };
        new FactScorer().ScoreAll([burn, FiredBurnAnomaly(sigma: 8.0)]);
        Assert.Equal(0.0, burn.Severity);
        Assert.Empty(burn.AmplifierResults);
    }

    [Fact]
    public void TheUnavailableShape_IsOneFact_WithOneReason_NoZeros_NoDecomposition_AndScoresNothing()
    {
        var absent = Unavailable(available: false);
        Assert.Equal(0, absent.Value);
        Assert.Equal(1, absent.Metadata["unavailable"]);
        Assert.Equal(1, absent.Metadata[PgTargetFactCollector.KernelReasonKcacheAbsentKey]);
        Assert.False(absent.Metadata.ContainsKey(PgTargetFactCollector.KernelReasonKcacheAvailableKey));
        Assert.False(absent.Metadata.ContainsKey(PgTargetScorer.KernelCoresBusyKey));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(absent));

        var offered = Unavailable(available: true);
        Assert.Equal(1, offered.Metadata[PgTargetFactCollector.KernelReasonKcacheAvailableKey]);
        Assert.False(offered.Metadata.ContainsKey(PgTargetFactCollector.KernelReasonKcacheAbsentKey));

        /* The advice says which, and never states a cores figure or a zero. */
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.CpuBurnCores, Lookup(absent))!;
        Assert.Contains("not measurable", block.Headline, StringComparison.Ordinal);
        Assert.Contains("does not offer pg_stat_kcache", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("absent instrument is not an idle server", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("cores busy", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("0 cores", block.Investigation, StringComparison.Ordinal);
        var offeredBlock = PgTargetAdvice.Compose(PgTargetFactKeys.CpuBurnCores, Lookup(offered))!;
        Assert.Contains("one CREATE EXTENSION pg_stat_kcache away", offeredBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("CREATE EXTENSION pg_stat_kcache", offeredBlock.Remediation, StringComparison.Ordinal);
    }

    /* ───────────────────────── edges ───────────────────────── */

    [Fact]
    public void TheEdges_LeaveTheDecomposition_ComputeBoundToTheBadActor_WaitBoundToTheFiredWait_AndNeverTheProxyOrTheAnomaly()
    {
        var graph = new PgTargetRelationshipGraph();
        Assert.Empty(graph.GetAllEdges(PgTargetFactKeys.CpuBurnCores));
        Assert.Empty(graph.GetAllEdges(PgTargetFactKeys.AnomalyCpuBurn));
        var declared = graph.GetAllEdges(PgTargetFactKeys.CpuDecomposition);
        Assert.Equal(9, declared.Count);
        Assert.Single(declared, e => e.Destination == PgTargetFactKeys.BadActorFamily);
        Assert.All(declared, e => Assert.Equal("cpu_burn", e.Category));

        /* Compute-bound with a fired bad actor → the alias resolves to the statement. */
        var split = Split(burnShare: 0.9);
        var actor = new Fact { Source = PgTargetSources.QueriesSource, Key = PgTargetFactKeys.BadActorKey(7001), Value = 1, ServerId = 1, BaseSeverity = 0.7, Severity = 0.7 };
        var active = graph.GetActiveEdges(PgTargetFactKeys.CpuDecomposition, Lookup(split, actor));
        var hop = Assert.Single(active);
        Assert.Equal(PgTargetFactKeys.BadActorKey(7001), hop.Destination);

        /* Compute-bound, no bad actor emitted → the alias edge is dropped, not thrown. */
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.CpuDecomposition, Lookup(split)));

        /* Wait-bound → the fired standout, not the quiet rollup and not the bad actor. */
        var waitBound = Split(burnShare: 0.1);
        var relation = new Fact { Source = PgTargetSources.WaitsSource, Key = PgTargetFactKeys.WaitKey("Lock", "relation"), Value = 0.3, ServerId = 1, BaseSeverity = 0.6, Severity = 0.6 };
        var lockRollup = new Fact { Source = PgTargetSources.WaitsSource, Key = PgTargetFactKeys.WaitKey("Lock", null), Value = 0.3, ServerId = 1, BaseSeverity = 0, Severity = 0 };
        active = graph.GetActiveEdges(PgTargetFactKeys.CpuDecomposition, Lookup(waitBound, relation, lockRollup, actor));
        hop = Assert.Single(active);
        Assert.Equal(PgTargetFactKeys.WaitKey("Lock", "relation"), hop.Destination);

        /* Mixed → nothing, whatever fired. */
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.CpuDecomposition, Lookup(Split(burnShare: 0.5), relation, actor)));
    }

    /* ───────────────────────── advice ───────────────────────── */

    [Fact]
    public void TheAdvice_IsValueStated_NamesTheStatementAndTheUnknownCoreCount_NeverIndexDdl_AndTheAnomalyComposesHere()
    {
        foreach (var key in new[] { PgTargetFactKeys.CpuBurnCores, PgTargetFactKeys.CpuDecomposition, PgTargetFactKeys.AnomalyCpuBurn })
        {
            var stat = PgTargetAdvice.Static(key);
            Assert.NotNull(stat);
            Assert.Equal(stat, FactAdvice.GetForFactKey(key));
            var text = stat!.Headline + stat.Investigation + stat.Remediation;
            Assert.DoesNotContain("CREATE INDEX", text, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Anomalous spike", text, StringComparison.Ordinal);
            Assert.DoesNotContain("fsync", text, StringComparison.Ordinal);
            Assert.DoesNotContain("synchronous_commit", text, StringComparison.Ordinal);
            Assert.Contains("pg_stat_kcache", text, StringComparison.Ordinal);
        }

        var context = Context(observedMs: 4 * 3_600_000);
        var facts = new List<Fact>();
        PgTargetFactCollector.EmitKernelFacts(context, facts, new PgTargetFactCollector.KernelCpuRow(
            5.2, 5.05, 240, 240 * 255_000.0, 240 * 45_000.0, 240 * 3_000.0, 240 * 60, 0, 241, 7001, 240 * 230_000.0, "app"));
        var burnBlock = PgTargetAdvice.Compose(PgTargetFactKeys.CpuBurnCores, Lookup(facts))!;
        Assert.Contains("5.05 cores busy across the window, 5.2 cores at peak", burnBlock.Headline, StringComparison.Ordinal);
        Assert.Contains("85 % of the execution CPU in user time", burnBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("Statement 7001 in app burned 75.9 % of it", burnBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("not the same ordering as the elapsed-time top statements", burnBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("not a percentage of anything", burnBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("never a host CPU percent", burnBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("max_parallel_workers_per_gather", burnBlock.Remediation, StringComparison.Ordinal);
        Assert.Contains("jit", burnBlock.Remediation, StringComparison.Ordinal);

        /* The decomposition, compute-bound, sampled wait side. */
        var split = Split(burnShare: 0.96, sampled: true);
        var splitBlock = PgTargetAdvice.Compose(PgTargetFactKeys.CpuDecomposition, Lookup(split))!;
        Assert.StartsWith("Compute-bound: 96 % of backend time was burning CPU", splitBlock.Headline, StringComparison.Ordinal);
        Assert.Contains("estimated from sampling", splitBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("the backends were running, not queued", splitBlock.Investigation, StringComparison.Ordinal);
        Assert.Equal(burnBlock.Remediation, splitBlock.Remediation);
        var waitBlock = PgTargetAdvice.Compose(PgTargetFactKeys.CpuDecomposition, Lookup(Split(burnShare: 0.1)))!;
        Assert.StartsWith("Wait-bound: 90 % of backend time was waiting", waitBlock.Headline, StringComparison.Ordinal);
        Assert.Contains("the engine's measured wait time", waitBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("get_pg_wait_stats", waitBlock.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("max_parallel_workers_per_gather", waitBlock.Remediation, StringComparison.Ordinal);

        /* The anomaly: the deviation composer's sentence, the ratio to the routine, the statement, the unknown core count. */
        var anomaly = FiredBurnAnomaly(sigma: 8.0);
        var anomalyBlock = FactAdvice.Compose(PgTargetFactKeys.AnomalyCpuBurn, Lookup(anomaly, facts[0]))!;
        Assert.Contains("CPU burn spiked to 5.2 cores busy", anomalyBlock.Headline, StringComparison.Ordinal);
        Assert.Contains("8σ above its", anomalyBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("this server routinely keeps busy at this hour of the week", anomalyBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("Statement 7001 in app burned 75.9 % of it", anomalyBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("How many cores this server has is not known from this reading", anomalyBlock.Investigation, StringComparison.Ordinal);
        Assert.NotEqual(PgTargetAdvice.Static(PgTargetFactKeys.AnomalyCpuBurn), anomalyBlock);

        /* A thin bucket prints no sigma. */
        var firstLook = FactAdvice.Compose(PgTargetFactKeys.AnomalyCpuBurn, Lookup(FiredBurnAnomaly(sigma: 0, lowQuality: true)))!;
        Assert.Contains("first occurrence, no baseline yet", firstLook.Headline, StringComparison.Ordinal);
        Assert.DoesNotContain("σ", firstLook.Investigation, StringComparison.Ordinal);

        /* Every next read is a PostgreSQL one. */
        foreach (var key in new[] { PgTargetFactKeys.CpuBurnCores, PgTargetFactKeys.CpuDecomposition, PgTargetFactKeys.AnomalyCpuBurn })
            Assert.All(PgTargetToolRecommendations.GetForKey(key)!, r => Assert.StartsWith("get_pg_", r.Tool, StringComparison.Ordinal));
    }

    /* ───────────────────────── live: the honesty arm and the exit criterion ───────────────────────── */

    /// <summary>
    /// The careful test. An Aurora-stamped server whose availability capture says <c>pg_stat_kcache</c> is <c>absent</c>
    /// and whose <c>pg_kernel_stats</c> is empty (the measured fleet's shape, every cluster) ⇒ the collector emits
    /// exactly ONE kernel fact — <c>PG_CPU_BURN_CORES</c>, <c>unavailable = 1</c>, <c>reason_pg_stat_kcache_absent</c>,
    /// value 0 — no decomposition, and the detector emits no anomaly; through <c>analyze_server</c> nothing kernel-
    /// shaped roots a card, and <c>get_analysis_facts source=pg_kernel</c> shows the one fact with its reason.
    /// </summary>
    [Fact]
    public async Task AnAuroraTarget_WithoutTheExtension_GetsOneUnavailableFact_NoZeros_NoAnomaly()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the kernel-family Aurora e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, AuroraServerId, AuroraServerName, MonitoredEngineKind.AuroraPostgres, 16, ct);
            var end = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = end.AddHours(-4);
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, AuroraServerId, AuroraServerName, end.AddHours(-25), ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, AuroraServerId, AuroraServerName, windowStart.AddMinutes(minute - 1), ct);
            /* Yesterday's daily capture, two databases, both absent — outside the four-hour window, inside the lookback. */
            await PlantAvailabilityAsync(connection, AuroraServerId, AuroraServerName, end.AddHours(-20), "postgres", "absent", ct);
            await PlantAvailabilityAsync(connection, AuroraServerId, AuroraServerName, end.AddHours(-20), "app", "absent", ct);

            var context = new AnalysisContext
            {
                ServerId = AuroraServerId, ServerName = AuroraServerName, TimeRangeStart = windowStart, TimeRangeEnd = end, ServerUtcOffset = TimeSpan.Zero,
                Coverage = new WindowCoverage { NominalMs = 4 * 3_600_000, ObservedMs = 4 * 3_600_000, SampleCount = 240 },
            };
            var collected = await new PgTargetFactCollector(postgres).CollectFactsAsync(context);
            var kernel = collected.Where(f => f.Source == PgTargetSources.KernelSource).ToList();
            var burn = Assert.Single(kernel);
            Assert.Equal(PgTargetFactKeys.CpuBurnCores, burn.Key);
            Assert.Equal(0, burn.Value);
            Assert.Equal(1, burn.Metadata["unavailable"]);
            Assert.Equal(1, burn.Metadata[PgTargetFactCollector.KernelReasonKcacheAbsentKey]);
            Assert.False(burn.Metadata.ContainsKey(PgTargetScorer.KernelCoresBusyKey));

            var baselines = new PgTargetBaselineProvider(postgres);
            var anomalies = await new PgTargetAnomalyDetector(postgres, baselines).DetectAnomaliesAsync(context);
            Assert.DoesNotContain(anomalies, a => a.Key == PgTargetFactKeys.AnomalyCpuBurn);

            var service = new DarlingAnalysisService(postgres);
            var asOf = end.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture);
            var json = await DarlingMcpTools.AnalyzeServer(service, postgres, AuroraServerName, 4, as_of: asOf);
            using (var doc = JsonDocument.Parse(json))
            {
                if (doc.RootElement.TryGetProperty("findings", out var findings) && findings.ValueKind == JsonValueKind.Array)
                {
                    foreach (var finding in findings.EnumerateArray())
                    {
                        var rootKey = finding.GetProperty("root_fact").GetProperty("key").GetString();
                        Assert.DoesNotContain(rootKey, new[] { PgTargetFactKeys.CpuBurnCores, PgTargetFactKeys.CpuDecomposition, PgTargetFactKeys.AnomalyCpuBurn });
                    }
                }
            }
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, AuroraServerName, 4, PgTargetSources.KernelSource, as_of: asOf);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var facts = doc.RootElement.GetProperty("facts").EnumerateArray().ToList();
                var fact = Assert.Single(facts);
                Assert.Equal(PgTargetFactKeys.CpuBurnCores, fact.GetProperty("key").GetString());
                Assert.Equal(0, fact.GetProperty("base_severity").GetDouble());
                Assert.Equal(1, fact.GetProperty("metadata").GetProperty(PgTargetFactCollector.KernelReasonKcacheAbsentKey).GetDouble());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The exit criterion. A stock-stamped server with <c>pg_stat_kcache</c> installed and 31 days of one-minute
    /// <c>pg_kernel_stats</c> for two statements at a routine of ~1 core busy (5 % jitter), then the last four hours at
    /// 5 cores with statement 7001 burning ~77 % of it, plus a light measured wait profile over the window. The bucket
    /// alone: trusted, median near 1. The detector alone: <c>ANOMALY_PG_CPU_BURN</c> at 5 cores, lineage 0, ratio ≈ 5, the
    /// pair gate passed. Through the REAL <c>analyze_server</c>: the anomaly roots with advice naming the cores, the
    /// routine and statement 7001; <c>PG_CPU_DECOMPOSITION</c> roots at 0.6 saying compute-bound (the anomaly lifted
    /// it); nothing roots on the proxy (context); every <c>next_tools</c> entry is a <c>get_pg_*</c> read; and
    /// <c>get_analysis_facts source=pg_kernel</c> shows the proxy at base 0 with its cores and the split with
    /// <c>burn_share ≥ 0.8</c> and <c>threshold_lineage = 0</c>.
    /// </summary>
    [Fact]
    public async Task AStockTarget_WithThirtyOneDaysAtOneCore_AndFourHoursAtFive_FiresTheBurnAnomaly_AndReadsComputeBound()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the kernel-family stock e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, StockServerId, StockServerName, MonitoredEngineKind.Postgres, 18, ct);

            var end = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            const int minutes = 31 * 24 * 60;
            var start = end.AddMinutes(-minutes);
            /* The surge's first INCREMENT lands on the row after the window's first row, so the half-open baseline
               (< window start) holds only the routine and every one of the window's 240 deltas is surge. */
            const int spikeFrom = minutes - 240 + 1;
            var windowStart = end.AddHours(-4);

            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, StockServerId, StockServerName, end.AddHours(-25), ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, StockServerId, StockServerName, windowStart.AddMinutes(minute - 1), ct);
            await PlantAvailabilityAsync(connection, StockServerId, StockServerName, end.AddHours(-20), "app", "installed", ct);
            await PlantKernelSeriesAsync(connection, StockServerId, StockServerName, start, minutes, spikeFrom, ct);
            /* A light measured wait profile over the window: Lock:relation 6 s per minute = 0.1 of a backend, below its bar. */
            for (var minute = 0; minute <= 240; minute++)
                await PlantWaitStatAsync(connection, StockServerId, StockServerName, windowStart.AddMinutes(minute), ct);

            /* ── The bucket alone: trusted, centred near one core. */
            var baselines = new PgTargetBaselineProvider(postgres);
            var bucket = await baselines.GetBaselineAsync(StockServerId, MetricNames.PgCpuBurnCores, windowStart, ct);
            Assert.True(bucket.IsTrustworthy, $"the 30-day cores-busy bucket is not trustworthy: samples={bucket.SampleCount} days={bucket.DistinctDays} mean={bucket.Mean} median={bucket.Median} mad={bucket.Mad} tier={bucket.Tier} sd={bucket.StdDev}");
            Assert.InRange(bucket.Median, 0.94, 1.06);
            Assert.True(bucket.EffectiveRobustSigma > 0);

            /* ── The collector alone. */
            var context = new AnalysisContext
            {
                ServerId = StockServerId, ServerName = StockServerName, TimeRangeStart = windowStart, TimeRangeEnd = end, ServerUtcOffset = TimeSpan.Zero,
                Coverage = new WindowCoverage { NominalMs = 4 * 3_600_000, ObservedMs = 4 * 3_600_000, SampleCount = 240 },
            };
            var collected = await new PgTargetFactCollector(postgres).CollectFactsAsync(context);
            var burn = Assert.Single(collected, f => f.Key == PgTargetFactKeys.CpuBurnCores);
            Assert.InRange(burn.Value, 4.9, 5.1);
            Assert.Equal(240, burn.Metadata["rated_samples"]);
            Assert.Equal(7001, burn.Metadata[PgTargetScorer.KernelTopQueryIdKey]);
            Assert.InRange(burn.Metadata[PgTargetScorer.KernelTopQueryShareKey], 0.74, 0.80);
            Assert.InRange(burn.Metadata[PgTargetScorer.KernelUserShareKey], 0.80, 0.90);
            Assert.Equal("app", burn.ObjectName);
            var split = Assert.Single(collected, f => f.Key == PgTargetFactKeys.CpuDecomposition);
            Assert.True(split.Metadata[PgTargetScorer.KernelBurnShareKey] >= 0.9, "5 cores against 0.1 backends waiting is compute-bound");
            Assert.Equal(0, split.Metadata[PgTargetScorer.KernelWaitIsSampledKey]);

            /* ── The detector alone, on the collector-shaped context: the pair gate passed (every window minute is surge). */
            var anomalies = await new PgTargetAnomalyDetector(postgres, baselines).DetectAnomaliesAsync(context);
            var anomaly = Assert.Single(anomalies, a => a.Key == PgTargetFactKeys.AnomalyCpuBurn);
            Assert.InRange(anomaly.Value, 4.9, 5.3);
            Assert.Equal(0, anomaly.Metadata["threshold_lineage"]);
            Assert.Equal(0, anomaly.Metadata["baseline_low_quality"]);
            Assert.InRange(anomaly.Metadata["baseline_ratio"], 4.6, 5.4);
            Assert.InRange(anomaly.Metadata["mean_cores_busy"], 4.9, 5.1);
            Assert.True(anomaly.Metadata["mean_sigma"] > 0);
            Assert.Equal(240, anomaly.Metadata["window_samples"]);
            Assert.Equal(7001, anomaly.Metadata[PgTargetScorer.KernelTopQueryIdKey]);

            /* ── THE EXIT CRITERION, through the real analyze_server, anchored at the planted window's end. */
            var service = new DarlingAnalysisService(postgres);
            var asOf = end.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture);
            var json = await DarlingMcpTools.AnalyzeServer(service, postgres, StockServerName, 4, as_of: asOf);
            using (var doc = JsonDocument.Parse(json))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                string RootKey(JsonElement f) => f.GetProperty("root_fact").GetProperty("key").GetString()!;

                var burnAnomaly = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.AnomalyCpuBurn);
                var advice = burnAnomaly.GetProperty("advice");
                var text = advice.GetProperty("headline").GetString() + advice.GetProperty("investigation").GetString();
                Assert.Contains("CPU burn spiked to", text, StringComparison.Ordinal);
                Assert.Contains("cores busy", text, StringComparison.Ordinal);
                Assert.Contains("σ above its", text, StringComparison.Ordinal);
                Assert.Contains("this server routinely keeps busy", text, StringComparison.Ordinal);
                Assert.Contains("Statement 7001 in app burned", text, StringComparison.Ordinal);
                Assert.Contains("How many cores this server has is not known", text, StringComparison.Ordinal);
                Assert.All(burnAnomaly.GetProperty("next_tools").EnumerateArray(), t => Assert.StartsWith("get_pg_", t.GetProperty("tool").GetString()!, StringComparison.Ordinal));

                var decomposition = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.CpuDecomposition);
                Assert.Equal(0.6, decomposition.GetProperty("severity").GetDouble(), precision: 6);
                Assert.StartsWith("Compute-bound:", decomposition.GetProperty("advice").GetProperty("headline").GetString()!, StringComparison.Ordinal);
                Assert.All(decomposition.GetProperty("next_tools").EnumerateArray(), t => Assert.StartsWith("get_pg_", t.GetProperty("tool").GetString()!, StringComparison.Ordinal));

                /* No card roots on the proxy: it is context. */
                Assert.DoesNotContain(findings, f => RootKey(f) == PgTargetFactKeys.CpuBurnCores);
            }

            /* ── The facts read. */
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, StockServerName, 4, PgTargetSources.KernelSource, as_of: asOf);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var facts = doc.RootElement.GetProperty("facts").EnumerateArray().ToList();
                Assert.Equal(2, facts.Count);
                var proxy = facts.Single(f => f.GetProperty("key").GetString() == PgTargetFactKeys.CpuBurnCores);
                Assert.Equal(0, proxy.GetProperty("base_severity").GetDouble());
                Assert.InRange(proxy.GetProperty("value").GetDouble(), 4.9, 5.1);
                Assert.Equal(1, proxy.GetProperty("metadata").GetProperty("threshold_lineage").GetDouble());
                var decomposition = facts.Single(f => f.GetProperty("key").GetString() == PgTargetFactKeys.CpuDecomposition);
                Assert.Equal(0.4, decomposition.GetProperty("base_severity").GetDouble(), precision: 6);
                Assert.True(decomposition.GetProperty("metadata").GetProperty(PgTargetScorer.KernelBurnShareKey).GetDouble() >= 0.8);
                Assert.Equal(0, decomposition.GetProperty("metadata").GetProperty("threshold_lineage").GetDouble());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ───────────────────────── fixtures ───────────────────────── */

    private static AnalysisContext Context(double observedMs) => new()
    {
        ServerId = 1, ServerName = "pg", TimeRangeStart = new DateTime(2026, 9, 20, 8, 0, 0), TimeRangeEnd = new DateTime(2026, 9, 20, 12, 0, 0),
        Coverage = new WindowCoverage { NominalMs = observedMs, ObservedMs = observedMs, SampleCount = (int)(observedMs / 60_000) },
    };

    private static Fact WaitFact(string key, double waitMs, double shareOfWait, double observedMs, bool sampled)
    {
        var fact = new Fact
        {
            Source = PgTargetSources.WaitsSource, Key = key, Value = waitMs / observedMs, ServerId = 1,
            Metadata =
            {
                [PgTargetScorer.WaitFractionKey] = waitMs / observedMs,
                [PgTargetScorer.WaitMsKey] = waitMs,
                [PgTargetScorer.WaitCountKey] = 1000,
                [PgTargetScorer.WaitSourceObservedMsKey] = observedMs,
                [PgTargetScorer.WaitSampleCountKey] = 240,
                [PgTargetScorer.WaitCollectionCountKey] = 241,
                [PgTargetScorer.WaitObservedMsKey] = observedMs,
                [PgTargetScorer.WaitShareOfWaitTimeKey] = shareOfWait,
                [PgTargetScorer.WaitIsStandoutKey] = key.Contains(':', StringComparison.Ordinal) ? 1 : 0,
            },
        };
        if (sampled)
        {
            fact.Metadata[PgTargetScorer.WaitIsSampledKey] = 1;
            fact.Metadata[PgTargetScorer.WaitEstimateResolutionMsKey] = 10;
            fact.Metadata[PgTargetScorer.WaitDeltaSamplesKey] = 1000;
        }
        return fact;
    }

    private static Fact Split(double burnShare, bool sampled = false) => new()
    {
        Source = PgTargetSources.KernelSource, Key = PgTargetFactKeys.CpuDecomposition, Value = burnShare, ServerId = 1,
        Metadata =
        {
            [PgTargetScorer.KernelBurnShareKey] = burnShare,
            [PgTargetScorer.KernelWaitShareKey] = 1 - burnShare,
            [PgTargetScorer.KernelIoWaitShareKey] = (1 - burnShare) / 2,
            [PgTargetScorer.KernelCoresBusyKey] = 5 * burnShare,
            [PgTargetScorer.KernelBackendsWaitingKey] = 5 * (1 - burnShare),
            [PgTargetScorer.KernelWaitIsSampledKey] = sampled ? 1 : 0,
        },
    };

    private static Fact Unavailable(bool available) => new()
    {
        Source = PgTargetSources.KernelSource, Key = PgTargetFactKeys.CpuBurnCores, Value = 0, ServerId = 1,
        Metadata =
        {
            ["unavailable"] = 1,
            [available ? PgTargetFactCollector.KernelReasonKcacheAvailableKey : PgTargetFactCollector.KernelReasonKcacheAbsentKey] = 1,
            ["observed_ms"] = 4 * 3_600_000,
            ["threshold_lineage"] = 1,
        },
    };

    private static Fact FiredBurnAnomaly(double sigma, double peak = 5.2, double mean = 5.05, bool lowQuality = false)
    {
        var fact = new Fact { Source = PgTargetAnomalyDetector.AnomalySource, Key = PgTargetFactKeys.AnomalyCpuBurn, Value = peak, ServerId = 1 };
        fact.Metadata["baseline_mean"] = 1.05;
        fact.Metadata["baseline_stddev"] = 0.08;
        fact.Metadata["deviation_sigma"] = sigma;
        fact.Metadata["fire_threshold"] = AnomalyThresholds.ModifiedZThreshold;
        fact.Metadata["baseline_low_quality"] = lowQuality ? 1 : 0;
        fact.Metadata["fallback_exceedance"] = lowQuality ? peak / AnomalyThresholds.PgCpuBurnCoresFallback : 0;
        fact.Metadata["baseline_samples"] = lowQuality ? 3 : 120;
        fact.Metadata["window_samples"] = 240;
        fact.Metadata["threshold_lineage"] = 0;
        fact.Metadata["baseline_hour"] = 3;
        fact.Metadata["baseline_dow"] = 2;
        fact.Metadata["baseline_tier"] = (double)BaselineTier.Full;
        fact.Metadata["baseline_median"] = lowQuality ? 0 : 1.0;
        fact.Metadata["baseline_mad"] = 0.05;
        fact.Metadata["confidence"] = lowQuality ? 0 : 1.0;
        fact.Metadata["peak_cores_busy"] = peak;
        fact.Metadata["mean_cores_busy"] = mean;
        fact.Metadata["mean_sigma"] = sigma;
        if (!lowQuality) fact.Metadata["baseline_ratio"] = peak / 1.0;
        return fact;
    }

    private static Dictionary<string, Fact> Lookup(params Fact[] facts) => Lookup((IEnumerable<Fact>)facts);

    private static Dictionary<string, Fact> Lookup(IEnumerable<Fact> facts)
    {
        var lookup = new Dictionary<string, Fact>(StringComparer.Ordinal);
        foreach (var fact in facts)
            lookup[fact.Key] = fact;
        return lookup;
    }

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private static async Task PlantAvailabilityAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, string database, string state, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_extension_availability
    (collection_id, collection_time, server_id, server_name, database_name, extension_name, state, installed_version, default_version, is_monitoring_relevant, comment)
VALUES ($1, $2, $3, $4, $5, 'pg_stat_kcache', $6, CASE WHEN $6 = 'installed' THEN '2.3.0' END, CASE WHEN $6 = 'absent' THEN NULL ELSE '2.3.0' END, true, 'Real OS CPU and disk per query')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(state);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Two statements in <c>app</c>, cumulative per minute as <c>PgKernelStatsCollector</c> writes them. Routine
    /// (per minute): 7001 user 30,000 ms + plan 300; 7002 user 25,000 + system 5,000 — one core busy, with a 5 % sine
    /// jitter so the bucket has a MAD. From <paramref name="spikeFromMinute"/>: 7001 +160,000 user +40,000 system, 7002
    /// +40,000 user — five cores, 7001 at 230,300 / 300,300 ≈ 77 %, user share (255,000 / 300,000) = 85 %.</summary>
    private static async Task PlantKernelSeriesAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime start, int minutes, int spikeFromMinute, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
WITH s AS (
    SELECT n,
           1.0 + 0.05 * sin(n) AS jitter,
           (n >= $5) AS spike
    FROM generate_series(0, $6) AS n
),
inc AS (
    SELECT n,
           7001::bigint AS query_id,
           30000 * jitter + CASE WHEN spike THEN 160000 ELSE 0 END AS user_inc,
           CASE WHEN spike THEN 40000 ELSE 0 END::double precision AS system_inc,
           300::double precision AS plan_inc
    FROM s
    UNION ALL
    SELECT n,
           7002::bigint,
           25000 * jitter + CASE WHEN spike THEN 40000 ELSE 0 END,
           5000 * jitter,
           0
    FROM s
)
INSERT INTO pg_kernel_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id,
     exec_user_time_ms, exec_system_time_ms, plan_cpu_time_ms, exec_read_bytes, exec_write_bytes, minor_faults, major_faults, stats_since)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, 'app', query_id,
       SUM(user_inc)   OVER (PARTITION BY query_id ORDER BY n),
       SUM(system_inc) OVER (PARTITION BY query_id ORDER BY n),
       SUM(plan_inc)   OVER (PARTITION BY query_id ORDER BY n),
       0, 0, 0, 0, $2 - interval '1 day'
FROM inc", connection) { CommandTimeout = 180 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 4_000_000L);
        command.Parameters.AddWithValue(start);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(spikeFromMinute);
        command.Parameters.AddWithValue(minutes);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One measured wait collection: <c>Lock:relation</c> 6 s of waiting over a stored 60 s interval — 0.1 of a
    /// backend, under the standout's bar, so the wait side is known and quiet.</summary>
    private static async Task PlantWaitStatAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type_id, wait_event_id, wait_type, wait_event,
     waits, wait_time_us, delta_waits, delta_wait_time_us, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 1, 1001, 'Lock', 'relation', 1000000, 1000000000000, 60, 6000000, 60)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 6_000_000L);
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id IN ({AuroraServerId}, {StockServerId}); " +
            $"DELETE FROM pg_kernel_stats WHERE server_id IN ({AuroraServerId}, {StockServerId}); " +
            $"DELETE FROM pg_extension_availability WHERE server_id IN ({AuroraServerId}, {StockServerId}); " +
            $"DELETE FROM pg_wait_stats WHERE server_id IN ({AuroraServerId}, {StockServerId}); " +
            $"DELETE FROM analysis_findings WHERE server_id IN ({AuroraServerId}, {StockServerId}); " +
            $"DELETE FROM analysis_muted WHERE server_id IN ({AuroraServerId}, {StockServerId}); " +
            $"DELETE FROM servers WHERE server_id IN ({AuroraServerId}, {StockServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
