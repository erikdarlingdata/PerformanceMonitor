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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The memory-composition family (lane 32 of #3691, design §4b): does the configuration fit THIS host?
/// <c>CONFIG_PG_MEMORY_OVERCOMMIT</c> is the §4b arithmetic over the latest <c>pg_server_config</c> snapshot against the
/// window's minimum <c>memory_total_bytes</c> on <c>pg_cpu_utilization</c>'s row (V136); <c>PG_HOST_MEMORY_PRESSURE</c> is the
/// OS's reclaimable share at its worst sustained point. Pinned here: the units go through <c>PgSettingValue</c>; a stock
/// target — no <c>pg_cpu_utilization</c> row — reads <c>unavailable</c> with its reason and gets NO composition fact
/// (the careful test); a pre-V136 NULL majority reads <c>unavailable</c> too; a Serverless window is measured against
/// its smallest total; the sum roots at exactly 0.4 on the arithmetic alone whatever the ratio and reaches ≥ 0.5 ONLY
/// on a workload co-fire (D5); the pressure bars need the sustain run; the edges read the destination's verdict; the
/// lineage stamps; and the routing pins moved from stub. The gated live classes drive the REAL <c>analyze_server</c>.
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetMemoryTests
{
    private const long KiB = 1024, MiB = KiB * 1024, GiB = MiB * 1024;
    private const int StockServerId = 93201, QuietServerId = 93202, ShortServerId = 93203, PeakServerId = 93204;
    private const string StockServerName = "pg-memory-stock", QuietServerName = "pg-memory-quiet", ShortServerName = "pg-memory-short", PeakServerName = "pg-memory-peak";

    /* ───────────────────────── the reads ───────────────────────── */

    [Fact]
    public void TheTwoReads_AreInAllSql_AnchoredOnTheSnapshot_AndReturnEveryWindowRow()
    {
        Assert.Contains(PgTargetFactCollector.PgTargetMemoryConfigSql, PgTargetFactCollector.AllSql);
        Assert.Contains(PgTargetFactCollector.PgTargetMemoryHostSql, PgTargetFactCollector.AllSql);

        var config = PgTargetFactCollector.PgTargetMemoryConfigSql;
        /* The config family's anchor and its three-value exclusion, copied not narrowed. */
        Assert.Contains("collection_time <= $2", config, StringComparison.Ordinal);
        Assert.Contains("NOT IN ('client', 'session', 'override')", config, StringComparison.Ordinal);
        foreach (var name in new[] { "shared_buffers", "work_mem", "max_connections", "max_parallel_workers_per_gather", "maintenance_work_mem", "autovacuum_work_mem", "autovacuum_max_workers", "wal_buffers", "effective_cache_size" })
            Assert.Contains($"'{name}'", config, StringComparison.Ordinal);

        var host = PgTargetFactCollector.PgTargetMemoryHostSql;
        /* Every row in the window, memory-carrying or not (the sparse rule is a ratio; the sustain gate needs the gaps),
           in time order; no aggregate, no filter on the memory columns. */
        Assert.Contains("FROM pg_cpu_utilization", host, StringComparison.Ordinal);
        Assert.Contains("ORDER BY w.collection_time", host, StringComparison.Ordinal);
        Assert.DoesNotContain("IS NOT NULL", host, StringComparison.Ordinal);
        Assert.DoesNotContain("GROUP BY", host, StringComparison.Ordinal);
        foreach (var column in new[] { "memory_total_bytes", "memory_free_bytes", "memory_cached_bytes", "memory_buffers_bytes", "memory_active_bytes", "configured_memory_bytes" })
            Assert.Contains(column, host, StringComparison.Ordinal);
    }

    /* ───────────────────────── the arithmetic ───────────────────────── */

    [Fact]
    public void TheTerms_NormaliseThroughPgSettingValue_AndTheSumIsTheEngineModel()
    {
        var terms = PgTargetFactCollector.ReadTerms(Snapshot()).GetValueOrDefault();
        Assert.Equal(4 * GiB, terms.SharedBuffersBytes);          // 524288 × 8kB
        Assert.Equal(64 * MiB, terms.WorkMemBytes);                // 65536 kB
        Assert.Equal(500, terms.MaxConnections);
        Assert.Equal(2, terms.ParallelWorkersPerGather);
        Assert.Equal(64 * MiB, terms.MaintWorkMemBytes);
        Assert.Null(terms.AutovacuumWorkMemBytes);                 // -1 = inherit → absent, maintenance_work_mem stands in
        Assert.Equal(3, terms.AutovacuumMaxWorkers);
        Assert.Equal(16 * MiB, terms.WalBuffersBytes);             // 2048 × 8kB
        Assert.Equal(12 * GiB, terms.EffectiveCacheSizeBytes);

        Assert.Equal(500.0 * 64 * MiB * 3, terms.BackendTermBytes);          // 93.75 GiB
        Assert.Equal(3.0 * 64 * MiB, terms.AutovacuumTermBytes);
        Assert.Equal(4.0 * GiB + 500.0 * 64 * MiB * 3 + 3.0 * 64 * MiB + 16.0 * MiB, terms.WorstCaseBytes);

        /* autovacuum_work_mem set replaces maintenance_work_mem for the workers — PostgreSQL's own rule. */
        var withAvwm = PgTargetFactCollector.ReadTerms(Snapshot(("autovacuum_work_mem", "1048576", "kB"))).GetValueOrDefault();
        Assert.Equal(1 * GiB, withAvwm.AutovacuumWorkMemBytes);
        Assert.Equal(3.0 * GiB, withAvwm.AutovacuumTermBytes);

        /* Optional terms absent multiply by 1 / contribute 0 and are ABSENT, not zero, on the terms. */
        var sparse = PgTargetFactCollector.ReadTerms(Snapshot(drop: new[] { "max_parallel_workers_per_gather", "autovacuum_max_workers", "wal_buffers", "effective_cache_size" })).GetValueOrDefault();
        Assert.Null(sparse.ParallelWorkersPerGather);
        Assert.Null(sparse.AutovacuumMaxWorkers);
        Assert.Null(sparse.WalBuffersBytes);
        Assert.Equal(500.0 * 64 * MiB, sparse.BackendTermBytes);
        Assert.Equal(0, sparse.AutovacuumTermBytes);
        Assert.Equal(4.0 * GiB + 500.0 * 64 * MiB, sparse.WorstCaseBytes);

        /* A mandatory term missing or un-normalisable → no terms at all (a partial sum stated as the worst case understates it). */
        Assert.Null(PgTargetFactCollector.ReadTerms(Snapshot(drop: new[] { "shared_buffers" })));
        Assert.Null(PgTargetFactCollector.ReadTerms(Snapshot(("work_mem", "lots", "kB"))));
        Assert.Null(PgTargetFactCollector.ReadTerms(Snapshot(drop: new[] { "max_connections" })));
    }

    [Fact]
    public void TheHostSummary_IsPerSample_MinOfTotal_SustainedOverRuns_AndGapsBreakRuns()
    {
        static PgTargetFactCollector.HostMemorySample S(long total, double reclaimable, long? configured = null, double active = 0.5) =>
            new(total, (long)(total * reclaimable / 2), (long)(total * reclaimable / 2), (long)(total * active), configured);
        var none = new PgTargetFactCollector.HostMemorySample(null, null, null, null, null);

        var summary = PgTargetFactCollector.SummariseHostMemory(
            [S(16 * GiB, 0.40), S(16 * GiB, 0.08), S(16 * GiB, 0.05), S(16 * GiB, 0.09, active: 0.9), S(8 * GiB, 0.50, configured: 8 * GiB), none, S(16 * GiB, 0.02), S(16 * GiB, 0.02)],
            sustain: 3);

        Assert.Equal(8, summary.Samples);
        Assert.Equal(7, summary.SamplesWithMemory);
        Assert.Equal(8 * GiB, summary.TotalMinBytes);
        Assert.Equal(16 * GiB, summary.TotalMaxBytes);
        Assert.Equal(8 * GiB, summary.ConfiguredMinBytes);
        Assert.Equal(0.02, summary.MinReclaimableShare!.Value, precision: 9);
        Assert.Equal((0.40 + 0.08 + 0.05 + 0.09 + 0.50 + 0.02 + 0.02) / 7, summary.MeanReclaimableShare!.Value, precision: 9);
        /* Runs of three: {.40,.08,.05}→.40, {.08,.05,.09}→.09, {.05,.09,.50}→.50; the NULL row breaks the run, and the
           two .02 samples after it never make three — so the worst share HELD for three samples is 0.09, not 0.02. */
        Assert.Equal(0.09, summary.SustainedMinReclaimableShare!.Value, precision: 9);
        Assert.Equal(0.9, summary.PeakActiveShare!.Value, precision: 9);
        Assert.Null(summary.PeakBuffersShare);   // no sample carried buffers
        var withBuffers = PgTargetFactCollector.SummariseHostMemory([new(16 * GiB, GiB, GiB, 8 * GiB, null, 512 * MiB), new(16 * GiB, GiB, GiB, 8 * GiB, null, GiB)], sustain: 3);
        Assert.Equal(1 / 16.0, withBuffers.PeakBuffersShare!.Value, precision: 9);
        Assert.Equal(0.125, withBuffers.MinReclaimableShare!.Value, precision: 9);   // buffers are stated, not folded into free + cached

        /* Fewer memory rows than the sustain count → nothing sustained; a zero total is not a measurement. */
        var thin = PgTargetFactCollector.SummariseHostMemory([S(16 * GiB, 0.01), S(0, 0.01), S(16 * GiB, 0.01)], sustain: 3);
        Assert.Equal(2, thin.SamplesWithMemory);
        Assert.Null(thin.SustainedMinReclaimableShare);
        Assert.Equal(0.01, thin.MinReclaimableShare!.Value, precision: 9);

        var empty = PgTargetFactCollector.SummariseHostMemory([], sustain: 3);
        Assert.Equal(0, empty.Samples);
        Assert.Null(empty.TotalMinBytes);
    }

    /* ───────────────────────── the three outcomes ───────────────────────── */

    [Fact]
    public void AStockTarget_NoRowAtAll_ReadsUnavailableWithItsReason_AndGetsNoCompositionFact()
    {
        var facts = new List<Fact>();
        PgTargetFactCollector.EmitMemoryFacts(Context(), facts, PgTargetFactCollector.SummariseHostMemory([], 3), PgTargetFactCollector.ReadTerms(Snapshot()), 120, isAurora: false);

        var pressure = Assert.Single(facts);
        Assert.Equal(PgTargetFactKeys.HostMemoryPressure, pressure.Key);
        Assert.Equal(PgTargetSources.MemorySource, pressure.Source);
        Assert.Equal(0, pressure.Value);
        Assert.Equal(1, pressure.Metadata["unavailable"]);
        Assert.Equal(1, pressure.Metadata[PgTargetScorer.HostMemoryReasonNoSourceKey]);
        Assert.False(pressure.Metadata.ContainsKey(PgTargetScorer.HostMemoryReasonSparseKey));
        Assert.False(pressure.Metadata.ContainsKey(PgTargetScorer.HostMemoryMinReclaimableShareKey));
        /* The configured sum rides the unavailable fact — true even without a denominator — and no ratio is fabricated. */
        Assert.True(pressure.Metadata[PgTargetScorer.MemoryWorstCaseBytesKey] > 90.0 * GiB);
        Assert.False(pressure.Metadata.ContainsKey(PgTargetScorer.MemoryOvercommitRatioKey));

        new FactScorer().ScoreAll(facts);
        Assert.Equal(0, pressure.BaseSeverity);
        Assert.Equal(0, pressure.Metadata["threshold_lineage"]);
        var advice = PgTargetAdvice.Compose(PgTargetFactKeys.HostMemoryPressure, facts.ToFactLookup())!;
        Assert.Contains("not collected for this target", advice.Headline, StringComparison.Ordinal);
        Assert.Contains("no row for this server", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("sums to", advice.Investigation, StringComparison.Ordinal);

        /* Zero rows on an AURORA target, or with no registry fact, is UNKNOWN (the collector has not run, or PI is off) —
           nothing is emitted; "unavailable" is said only where the absence is known by architecture (lane 28's shape). */
        var unknown = new List<Fact>();
        PgTargetFactCollector.EmitMemoryFacts(Context(), unknown, PgTargetFactCollector.SummariseHostMemory([], 3), PgTargetFactCollector.ReadTerms(Snapshot()), 120, isAurora: true);
        PgTargetFactCollector.EmitMemoryFacts(Context(), unknown, PgTargetFactCollector.SummariseHostMemory([], 3), PgTargetFactCollector.ReadTerms(Snapshot()), 120, isAurora: null);
        Assert.Empty(unknown);
    }

    [Fact]
    public void APreV136Window_WithAMinorityOfMemoryRows_ReadsUnavailableSparse_AndTheMajorityReadsMeasured()
    {
        var facts = new List<Fact>();
        var twoOfFive = Series(withMemory: 2, without: 3, reclaimable: 0.5);
        PgTargetFactCollector.EmitMemoryFacts(Context(), facts, PgTargetFactCollector.SummariseHostMemory(twoOfFive, 3), PgTargetFactCollector.ReadTerms(Snapshot()), null, isAurora: true);
        var sparse = Assert.Single(facts);
        Assert.Equal(1, sparse.Metadata["unavailable"]);
        Assert.Equal(1, sparse.Metadata[PgTargetScorer.HostMemoryReasonSparseKey]);
        Assert.Equal(5, sparse.Metadata[PgTargetScorer.MemorySamplesKey]);
        Assert.Equal(2, sparse.Metadata[PgTargetScorer.MemorySamplesWithMemoryKey]);
        var advice = PgTargetAdvice.Compose(PgTargetFactKeys.HostMemoryPressure, facts.ToFactLookup())!;
        Assert.Contains("sparse", advice.Headline, StringComparison.Ordinal);
        Assert.Contains("only 2 carry memory_total_bytes", advice.Investigation, StringComparison.Ordinal);

        /* Exactly half is NOT sparse (the rule is strict: 2 × with < all). */
        facts.Clear();
        PgTargetFactCollector.EmitMemoryFacts(Context(), facts, PgTargetFactCollector.SummariseHostMemory(Series(withMemory: 3, without: 3, reclaimable: 0.5), 3), PgTargetFactCollector.ReadTerms(Snapshot()), null, isAurora: true);
        Assert.Equal(2, facts.Count);
        Assert.False(facts[0].Metadata.ContainsKey("unavailable"));
    }

    [Fact]
    public void AMeasuredWindow_EmitsThePressureAndTheSum_RatioOverTheMinimumTotal_ServerlessStated()
    {
        var facts = new List<Fact>();
        var series = Series(withMemory: 48, without: 0, reclaimable: 0.45, total: 16 * GiB);
        /* A Serverless dip: four samples at 8 GiB (4 ACU) — the minimum is the denominator. */
        for (var i = 10; i < 14; i++) series[i] = new(8 * GiB, 3 * GiB, 1 * GiB, 3 * GiB, 8 * GiB);
        PgTargetFactCollector.EmitMemoryFacts(Context(), facts, PgTargetFactCollector.SummariseHostMemory(series, 3), PgTargetFactCollector.ReadTerms(Snapshot()), 300, isAurora: true);

        var pressure = Assert.Single(facts, f => f.Key == PgTargetFactKeys.HostMemoryPressure);
        Assert.False(pressure.Metadata.ContainsKey("unavailable"));
        Assert.Equal(0.45, pressure.Value, precision: 9);
        Assert.Equal(0.45, pressure.Metadata[PgTargetScorer.HostMemorySustainedMinReclaimableShareKey], precision: 9);
        Assert.Equal(3, pressure.Metadata[PgTargetScorer.HostMemorySustainSamplesKey]);
        Assert.Equal(8 * GiB, pressure.Metadata[PgTargetScorer.MemoryTotalMinBytesKey]);
        Assert.Equal(16 * GiB, pressure.Metadata[PgTargetScorer.MemoryTotalMaxBytesKey]);
        Assert.Equal(1, pressure.Metadata[PgTargetScorer.MemoryIsServerlessKey]);

        var sum = Assert.Single(facts, f => f.Key == PgTargetFactKeys.ConfigMemoryOvercommit);
        Assert.Equal(PgTargetSources.MemorySource, sum.Source);
        var worst = 4.0 * GiB + 500.0 * 64 * MiB * 3 + 3.0 * 64 * MiB + 16.0 * MiB;
        Assert.Equal(worst / (8.0 * GiB), sum.Value, precision: 9);
        Assert.Equal(sum.Value, sum.Metadata[PgTargetScorer.MemoryOvercommitRatioKey]);
        /* The natural basis-0 twin (#3691 lane 47): no backend count rode in, so the graded ratio IS the configured one,
           stamped under both names; the observed keys are absent, never 0. */
        Assert.Equal(0, sum.Metadata[PgTargetScorer.MemoryOvercommitBasisKey]);
        Assert.Equal(sum.Value, sum.Metadata[PgTargetScorer.MemoryConfiguredOvercommitRatioKey]);
        Assert.Equal(0, sum.Metadata[PgTargetScorer.MemoryPeakBackendsSamplesKey]);
        foreach (var absent in new[] { PgTargetScorer.MemoryPeakBackendsKey, PgTargetScorer.MemoryObservedBackendTermBytesKey, PgTargetScorer.MemoryObservedWorstCaseBytesKey })
            Assert.False(sum.Metadata.ContainsKey(absent), absent);
        Assert.Equal(worst, sum.Metadata[PgTargetScorer.MemoryWorstCaseBytesKey]);
        Assert.Equal(8 * GiB, sum.Metadata[PgTargetScorer.MemoryConfiguredMinBytesKey]);
        Assert.Equal(1, sum.Metadata[PgTargetScorer.MemoryIsServerlessKey]);
        Assert.Equal(300, sum.Metadata[PgTargetScorer.MemorySnapshotAgeSecondsKey]);
        /* effective_cache_size 12 GiB against the 8 GiB minimum: the plausibility line is stated, never a fact of its own. */
        Assert.Equal(1, sum.Metadata[PgTargetScorer.MemoryEffectiveCacheExceedsTotalKey]);
        Assert.Equal(12 * GiB, sum.Metadata[PgTargetScorer.MemoryEffectiveCacheSizeBytesKey]);
        Assert.DoesNotContain(facts, f => f.Key.Contains("EFFECTIVE_CACHE", StringComparison.Ordinal) && f.Source == PgTargetSources.MemorySource);

        /* Provisioned: no configured figure, min = max, is_serverless 0; and no snapshot → the pressure fact alone. */
        facts.Clear();
        PgTargetFactCollector.EmitMemoryFacts(Context(), facts, PgTargetFactCollector.SummariseHostMemory(Series(48, 0, 0.45, 16 * GiB), 3), null, null, isAurora: true);
        var alone = Assert.Single(facts);
        Assert.Equal(PgTargetFactKeys.HostMemoryPressure, alone.Key);
        Assert.Equal(0, alone.Metadata[PgTargetScorer.MemoryIsServerlessKey]);
        Assert.False(alone.Metadata.ContainsKey(PgTargetScorer.MemoryConfiguredMinBytesKey));
    }

    /* ───────────────────────── the graded basis (#3691 lane 47) ───────────────────────── */

    /// <summary>Erik's ruling on the D2 read (issue comment 5782361122): the card is graded on the backends that actually
    /// showed up. max_connections 5000 × work_mem 4 MB × 3 on a 16 GiB host is a 3.92× PERMISSION; at a peak of 50 backends
    /// the worst case is 0.30× — graded 0, no card, the permission stated as configured_overcommit_ratio. At a peak of
    /// 4,000 the same settings fire at exactly the advisory base. With no sampled level the permission is graded as before
    /// (basis 0), and a peak above max_connections clamps in the term while the raw peak rides as read.</summary>
    [Fact]
    public void TheGradedRatio_IsTheWindowsPeakBackends_ThePermissionIsStated_NoSampleGradesThePermission_AndAPeakAboveTheCeilingClamps()
    {
        var wide = Snapshot(new[] { ("max_connections", "5000", (string?)null), ("work_mem", "4096", (string?)"kB") });
        var host = PgTargetFactCollector.SummariseHostMemory(Series(48, 0, 0.45, 16 * GiB), 3);
        var configuredWorst = 4.0 * GiB + 5000.0 * 4 * MiB * 3 + 3.0 * 64 * MiB + 16.0 * MiB;
        var configuredRatio = configuredWorst / (16.0 * GiB);
        Assert.True(configuredRatio > 3.9 && configuredRatio < 4.0, "the permission is ~3.92× the box");

        Fact Emit(double? peak, long samples, out List<Fact> all)
        {
            all = new List<Fact>();
            var terms = PgTargetFactCollector.ReadTerms(wide)!.Value with { PeakBackends = peak, PeakBackendsSamples = samples };
            PgTargetFactCollector.EmitMemoryFacts(Context(), all, host, terms, 300, isAurora: true);
            new FactScorer().ScoreAll(all);
            return Assert.Single(all, f => f.Key == PgTargetFactKeys.ConfigMemoryOvercommit);
        }

        /* (i) basis 1, peak 50: graded under 1, the permission over it, base 0, no card. */
        var quiet = Emit(50, 1440, out var quietFacts);
        var observedWorst = 4.0 * GiB + 50.0 * 4 * MiB * 3 + 3.0 * 64 * MiB + 16.0 * MiB;
        Assert.Equal(1, quiet.Metadata[PgTargetScorer.MemoryOvercommitBasisKey]);
        Assert.Equal(observedWorst / (16.0 * GiB), quiet.Value, precision: 9);
        Assert.Equal(quiet.Value, quiet.Metadata[PgTargetScorer.MemoryOvercommitRatioKey]);
        Assert.True(quiet.Value < 1.0);
        Assert.Equal(configuredRatio, quiet.Metadata[PgTargetScorer.MemoryConfiguredOvercommitRatioKey], precision: 9);
        Assert.True(quiet.Metadata[PgTargetScorer.MemoryConfiguredOvercommitRatioKey] > 1.0);
        Assert.Equal(50, quiet.Metadata[PgTargetScorer.MemoryPeakBackendsKey]);
        Assert.Equal(1440, quiet.Metadata[PgTargetScorer.MemoryPeakBackendsSamplesKey]);
        Assert.Equal(50.0 * 4 * MiB * 3, quiet.Metadata[PgTargetScorer.MemoryObservedBackendTermBytesKey], precision: 3);
        Assert.Equal(observedWorst, quiet.Metadata[PgTargetScorer.MemoryObservedWorstCaseBytesKey], precision: 3);
        /* The configured terms still ride as read — the permission is stated in full, never dropped. */
        Assert.Equal(configuredWorst, quiet.Metadata[PgTargetScorer.MemoryWorstCaseBytesKey], precision: 3);
        Assert.Equal(5000.0 * 4 * MiB * 3, quiet.Metadata[PgTargetScorer.MemoryBackendTermBytesKey], precision: 3);
        Assert.Equal(0.0, quiet.BaseSeverity);
        Assert.Equal(0.0, quiet.Severity);
        Assert.DoesNotContain(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(quietFacts), s => s.RootFactKey == PgTargetFactKeys.ConfigMemoryOvercommit);

        /* (ii) basis 1, peak 4,000: the concurrency approaches the ceiling and the card fires at exactly the advisory base
           (the 2× band rides, and alone lifts nothing — D5). */
        var busy = Emit(4000, 1440, out var busyFacts);
        Assert.Equal(1, busy.Metadata[PgTargetScorer.MemoryOvercommitBasisKey]);
        Assert.Equal((4.0 * GiB + 4000.0 * 4 * MiB * 3 + 3.0 * 64 * MiB + 16.0 * MiB) / (16.0 * GiB), busy.Value, precision: 9);
        Assert.Equal(PgTargetScorer.ConfigAdvisoryBase, busy.BaseSeverity, precision: 9);
        Assert.Equal(0.4, busy.Severity, precision: 9);
        var card = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(busyFacts), s => s.RootFactKey == PgTargetFactKeys.ConfigMemoryOvercommit);
        Assert.Equal(0.4, card.Severity, precision: 9);

        /* (iii) basis 0: no sampled instant — today's behaviour exactly: the permission is the value, stamp 0, base 0.4.
           A peak with zero samples is "not observed", never "zero backends". */
        foreach (var (peak, samples) in new[] { ((double?)null, 0L), (50.0, 0L) })
        {
            var unsampled = Emit(peak, samples, out _);
            Assert.Equal(0, unsampled.Metadata[PgTargetScorer.MemoryOvercommitBasisKey]);
            Assert.Equal(configuredRatio, unsampled.Value, precision: 9);
            Assert.Equal(unsampled.Value, unsampled.Metadata[PgTargetScorer.MemoryConfiguredOvercommitRatioKey]);
            Assert.Equal(0, unsampled.Metadata[PgTargetScorer.MemoryPeakBackendsSamplesKey]);
            Assert.False(unsampled.Metadata.ContainsKey(PgTargetScorer.MemoryPeakBackendsKey));
            Assert.False(unsampled.Metadata.ContainsKey(PgTargetScorer.MemoryObservedBackendTermBytesKey));
            Assert.False(unsampled.Metadata.ContainsKey(PgTargetScorer.MemoryObservedWorstCaseBytesKey));
            Assert.Equal(PgTargetScorer.ConfigAdvisoryBase, unsampled.BaseSeverity, precision: 9);
        }
        Assert.Null((PgTargetFactCollector.ReadTerms(wide)!.Value with { PeakBackends = 50, PeakBackendsSamples = 0 }).ObservedBackendTermBytes);

        /* (iv) a peak above max_connections: the raw peak rides as read, the term uses max_connections, and the graded
           ratio equals the permission (never above it). */
        var over = Emit(6000, 1440, out _);
        Assert.Equal(1, over.Metadata[PgTargetScorer.MemoryOvercommitBasisKey]);
        Assert.Equal(6000, over.Metadata[PgTargetScorer.MemoryPeakBackendsKey]);
        Assert.Equal(over.Metadata[PgTargetScorer.MemoryBackendTermBytesKey], over.Metadata[PgTargetScorer.MemoryObservedBackendTermBytesKey], precision: 3);
        Assert.Equal(configuredRatio, over.Value, precision: 9);
        Assert.Equal(over.Value, over.Metadata[PgTargetScorer.MemoryConfiguredOvercommitRatioKey], precision: 9);
    }

    /* ───────────────────────── the bars and D5 ───────────────────────── */

    [Fact]
    public void TheSum_RootsAtExactlyTheAdvisoryBase_FromTheEngineLine_AndTheBandFlagCarriesTheLineage()
    {
        Assert.Equal(1.0, PgTargetScorer.OvercommitRatioLine);
        Assert.Equal(2.0, PgTargetScorer.OvercommitCriticalRatio);

        var under = Sum(0.99);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(under));
        Assert.Equal(1, under.Metadata["threshold_lineage"]);

        var at = Sum(1.0);
        Assert.Equal(PgTargetScorer.ConfigAdvisoryBase, PgTargetScorer.ScoreBase(at));
        Assert.Equal(0, at.Metadata[PgTargetScorer.MemoryOvercommitCriticalBandKey]);
        Assert.Equal(1, at.Metadata["threshold_lineage"]);       // the engine's line alone decided

        var doubled = Sum(2.0);
        Assert.Equal(PgTargetScorer.ConfigAdvisoryBase, PgTargetScorer.ScoreBase(doubled));  // still the advisory base — D5
        Assert.Equal(1, doubled.Metadata[PgTargetScorer.MemoryOvercommitCriticalBandKey]);
        Assert.Equal(0, doubled.Metadata["threshold_lineage"]);  // the chosen band participated

        Assert.Equal(0.0, PgTargetScorer.ScoreBase(new Fact { Source = PgTargetSources.MemorySource, Key = "PG_MEMORY_SOMETHING_ELSE", Value = 9 }));
    }

    [Fact]
    public void ThePressure_IsZeroBelowTheWarningLine_HalfAtIt_OneAtCritical_AndNeedsTheSustainRun()
    {
        Assert.Equal(0.10, PgTargetScorer.HostMemoryReclaimableWarningShare);
        Assert.Equal(0.03, PgTargetScorer.HostMemoryReclaimableCriticalShare);
        Assert.Equal(3, PgTargetScorer.HostMemoryPressureSustainSamples);

        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Pressure(sustained: 0.11)));
        Assert.Equal(0.5, PgTargetScorer.ScoreBase(Pressure(sustained: 0.10)), precision: 9);
        Assert.Equal(1.0, PgTargetScorer.ScoreBase(Pressure(sustained: 0.03)), precision: 9);
        Assert.Equal(1.0, PgTargetScorer.ScoreBase(Pressure(sustained: 0.0)), precision: 9);
        var mid = PgTargetScorer.ScoreBase(Pressure(sustained: 0.065));
        Assert.InRange(mid, 0.5, 1.0);

        /* A single dip to 1 % with no three-sample run: min 0.01, nothing sustained → 0. */
        var dip = Pressure(sustained: null, min: 0.01);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(dip));
        Assert.Equal(0, dip.Metadata["threshold_lineage"]);

        var unavailable = Pressure(sustained: 0.0);
        unavailable.Metadata["unavailable"] = 1;
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(unavailable));
    }

    [Fact]
    public void D5_TheSumAloneIsExactly0Point4_ACoFireLiftsItTo0Point5_BothTo0Point6_AndTheBandOnlyBesideACoFire()
    {
        /* Alone, at three times the box: exactly the advisory base — the arithmetic never lifts itself. */
        var alone = new List<Fact> { Sum(3.0) };
        new FactScorer().ScoreAll(alone);
        Assert.Equal(0.4, alone[0].Severity, precision: 9);
        Assert.Equal(3, alone[0].AmplifierResults.Count);
        Assert.All(alone[0].AmplifierResults, r => Assert.False(r.Matched));

        /* A quiet host (40 % reclaimable) beside it changes nothing: "fired" means at least the warning line. */
        var quietHost = new List<Fact> { Sum(1.2), Pressure(sustained: 0.40) };
        new FactScorer().ScoreAll(quietHost);
        Assert.Equal(0.4, quietHost[0].Severity, precision: 9);
        Assert.Equal(0.0, quietHost[1].Severity);

        /* Pressure fired → 0.5 exactly; the pressure is corroborated by the sum (0.5 × 1.3). */
        var pressured = new List<Fact> { Sum(1.2), Pressure(sustained: 0.10) };
        new FactScorer().ScoreAll(pressured);
        Assert.Equal(0.5, pressured[0].Severity, precision: 9);
        Assert.Equal(0.5 * (1 + PgTargetScorer.HostMemoryCauseBoost), pressured[1].Severity, precision: 9);

        /* Spill fired (lane 6's fact at its measured floor) → 0.5 exactly, through the amplifier — no edge needed. */
        var spilling = new List<Fact> { Sum(1.2), Spill(PgTargetScorer.TempSpillConcerningBytesPerSec) };
        new FactScorer().ScoreAll(spilling);
        Assert.True(spilling[1].BaseSeverity > 0);
        Assert.Equal(0.5, spilling[0].Severity, precision: 9);

        /* Both co-fires → 0.6; both plus the 2× band → 0.7. */
        var both = new List<Fact> { Sum(1.2), Pressure(sustained: 0.10), Spill(PgTargetScorer.TempSpillConcerningBytesPerSec) };
        new FactScorer().ScoreAll(both);
        Assert.Equal(0.6, both[0].Severity, precision: 9);
        var bothDoubled = new List<Fact> { Sum(2.5), Pressure(sustained: 0.10), Spill(PgTargetScorer.TempSpillConcerningBytesPerSec) };
        new FactScorer().ScoreAll(bothDoubled);
        Assert.Equal(0.7, bothDoubled[0].Severity, precision: 9);

        /* The band WITHOUT a co-fire lifts nothing: 2.5× alone is still 0.4 (D5). */
        var bandAlone = new List<Fact> { Sum(2.5), Pressure(sustained: 0.40) };
        new FactScorer().ScoreAll(bandAlone);
        Assert.Equal(0.4, bandAlone[0].Severity, precision: 9);
        Assert.False(bandAlone[0].AmplifierResults.Single(r => r.Description.Contains("twice", StringComparison.Ordinal)).Matched);

        /* A sum that fits the box is 0 whatever co-fires. */
        var fits = new List<Fact> { Sum(0.8), Pressure(sustained: 0.02), Spill(PgTargetScorer.TempSpillConcerningBytesPerSec) };
        new FactScorer().ScoreAll(fits);
        Assert.Equal(0.0, fits[0].Severity);
    }

    /* ───────────────────────── the edges and the stories ───────────────────────── */

    [Fact]
    public void TheEdges_LeadPressureAndTheCacheKnobToTheSum_ReadTheSumsVerdict_AndNoSpillEdgeLivesHere()
    {
        var graph = new PgTargetRelationshipGraph();

        var fromPressure = Assert.Single(graph.GetAllEdges(PgTargetFactKeys.HostMemoryPressure));
        Assert.Equal(PgTargetFactKeys.ConfigMemoryOvercommit, fromPressure.Destination);
        Assert.Equal("host_memory", fromPressure.Category);

        var fromKnob = graph.GetAllEdges(PgTargetFactKeys.ConfigSharedBuffers).Single(e => e.Destination == PgTargetFactKeys.ConfigMemoryOvercommit);
        Assert.Equal("host_memory", fromKnob.Category);

        /* The spill's edges are the query chain's; the spill → sum edge lane 32 asked for lives in
           PgTargetRelationshipGraph.Query.cs since the third between-waves batch (#3809), under the temp chain's
           category — pinned there (PgTargetBetweenWavesV3Tests); here only that it exists and is not this file's. */
        var fromSpill = Assert.Single(graph.GetAllEdges(PgTargetFactKeys.TempSpill), e => e.Destination == PgTargetFactKeys.ConfigMemoryOvercommit);
        Assert.Equal("temp_spill", fromSpill.Category);
        /* Nothing leaves the sum: it is the leaf. */
        Assert.Empty(graph.GetAllEdges(PgTargetFactKeys.ConfigMemoryOvercommit));

        /* Predicates read the destination's verdict, never a ratio of their own. */
        var fired = new List<Fact> { Sum(1.5) };
        new FactScorer().ScoreAll(fired);
        Assert.True(fromPressure.Predicate(fired.ToFactLookup()));
        Assert.True(fromKnob.Predicate(fired.ToFactLookup()));
        var fits = new List<Fact> { Sum(0.5) };
        new FactScorer().ScoreAll(fits);
        Assert.False(fromPressure.Predicate(fits.ToFactLookup()));
        Assert.False(fromPressure.Predicate(new Dictionary<string, Fact>()));
    }

    [Fact]
    public void TheStories_AQuietSumRootsOneAdvisoryCard_AShortHostTellsPressureToTheSum()
    {
        var quiet = new List<Fact> { Sum(1.3), Pressure(sustained: 0.40) };
        new FactScorer().ScoreAll(quiet);
        var quietStories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(quiet);
        var card = Assert.Single(quietStories);
        Assert.Equal(PgTargetFactKeys.ConfigMemoryOvercommit, card.RootFactKey);
        Assert.Equal(0.4, card.Severity, precision: 9);
        Assert.Single(card.Path);
        Assert.Equal(PgTargetSources.MemorySource, card.Category);

        var shortHost = new List<Fact> { Sum(1.3), Pressure(sustained: 0.05) };
        new FactScorer().ScoreAll(shortHost);
        var stories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(shortHost);
        var story = Assert.Single(stories, s => s.RootFactKey == PgTargetFactKeys.HostMemoryPressure);
        Assert.Equal($"{PgTargetFactKeys.HostMemoryPressure} → {PgTargetFactKeys.ConfigMemoryOvercommit}", story.StoryPath);
        Assert.Equal(PgTargetFactKeys.ConfigMemoryOvercommit, story.LeafFactKey);
        Assert.True(story.Severity >= 0.5);
        /* The sum is consumed by the story — no second card for it. */
        Assert.DoesNotContain(stories, s => s.RootFactKey == PgTargetFactKeys.ConfigMemoryOvercommit);
    }

    /* ───────────────────────── the advice ───────────────────────── */

    [Fact]
    public void TheAdvice_IsValueStated_NamesEveryTerm_TheCoFire_TheCounterObjectives_AndNeverPostureOrDdl()
    {
        var facts = new List<Fact> { Sum(1.5), Pressure(sustained: 0.40) };
        new FactScorer().ScoreAll(facts);
        var lookup = facts.ToFactLookup();

        var sum = PgTargetAdvice.Compose(PgTargetFactKeys.ConfigMemoryOvercommit, lookup)!;
        Assert.StartsWith("The configured memory worst case is 96 GB — 1.5× the host's 64 GB", sum.Headline, StringComparison.Ordinal);
        foreach (var expected in new[] { "shared_buffers 4 GB", "max_connections 500 × work_mem 64 MB × (1 + max_parallel_workers_per_gather 2) = 93.8 GB", "autovacuum_max_workers 3 × maintenance_work_mem 64 MB", "wal_buffers 16 MB", "1.5× of the host's memory_total_bytes, 64 GB", "CEILING", "advisory (0.4)", "engine-defined" })
            Assert.Contains(expected, sum.Investigation, StringComparison.Ordinal);
        Assert.Contains("effective_cache_size is 12 GB, within the host's 64 GB", sum.Investigation, StringComparison.Ordinal);
        Assert.Contains("Raising work_mem globally is the classic overcommit path", sum.Remediation, StringComparison.Ordinal);
        foreach (var counter in new[] { "spills more sorts", "queue in the application", "cache hits handed to the OS" })
            Assert.Contains(counter, sum.Remediation, StringComparison.Ordinal);

        /* With the pressure co-fire: the sentence names it and the pressure card points back. */
        var pressured = new List<Fact> { Sum(2.5), Pressure(sustained: 0.05) };
        new FactScorer().ScoreAll(pressured);
        var lifted = PgTargetAdvice.Compose(PgTargetFactKeys.ConfigMemoryOvercommit, pressured.ToFactLookup())!;
        Assert.EndsWith("— more than double", lifted.Headline, StringComparison.Ordinal);
        Assert.Contains("PG_HOST_MEMORY_PRESSURE co-fires", lifted.Investigation, StringComparison.Ordinal);
        Assert.Contains("2× band is a chosen line", lifted.Investigation, StringComparison.Ordinal);
        var pressure = PgTargetAdvice.Compose(PgTargetFactKeys.HostMemoryPressure, pressured.ToFactLookup())!;
        Assert.Contains("fell to 5% of total and stayed under 10% for 3 consecutive samples", pressure.Headline, StringComparison.Ordinal);
        Assert.Contains("threshold_lineage = 0", pressure.Investigation, StringComparison.Ordinal);
        Assert.Contains("CONFIG_PG_MEMORY_OVERCOMMIT fired in the same window", pressure.Investigation, StringComparison.Ordinal);

        /* Serverless framing on the sum. */
        var serverless = Sum(1.5);
        serverless.Metadata[PgTargetScorer.MemoryIsServerlessKey] = 1;
        serverless.Metadata[PgTargetScorer.MemoryTotalMaxBytesKey] = 128 * GiB;
        var sl = new List<Fact> { serverless };
        new FactScorer().ScoreAll(sl);
        var slAdvice = PgTargetAdvice.Compose(PgTargetFactKeys.ConfigMemoryOvercommit, sl.ToFactLookup())!;
        Assert.Contains("smallest memory this instance ran with (64 GB)", slAdvice.Headline, StringComparison.Ordinal);
        Assert.Contains("scaled between 64 GB and 128 GB", slAdvice.Investigation, StringComparison.Ordinal);
        Assert.Contains("MINIMUM capacity", slAdvice.Remediation, StringComparison.Ordinal);

        /* Static shapes claim no figure and are what FactAdvice hands back (the delegation census). */
        foreach (var key in new[] { PgTargetFactKeys.ConfigMemoryOvercommit, PgTargetFactKeys.HostMemoryPressure })
        {
            var block = PgTargetAdvice.Static(key)!;
            Assert.Equal(block, FactAdvice.GetForFactKey(key));
            Assert.DoesNotContain(" GB", block.Headline, StringComparison.Ordinal);
        }

        /* House rules over every block this family can compose: no posture knob, no DDL. */
        foreach (var block in new[] { sum, lifted, pressure, slAdvice, PgTargetAdvice.Static(PgTargetFactKeys.ConfigMemoryOvercommit)!, PgTargetAdvice.Static(PgTargetFactKeys.HostMemoryPressure)! })
        {
            var text = block.Headline + block.Investigation + block.Remediation;
            foreach (var forbidden in new[] { "fsync", "synchronous_commit", "full_page_writes", "CREATE INDEX" })
                Assert.DoesNotContain(forbidden, text, StringComparison.OrdinalIgnoreCase);
            Assert.Null(block.RemediationTsql);
        }
    }

    /// <summary>The overcommit card's three shapes (#3691 lane 47), composed from the stamps the REAL emit writes: basis 1
    /// headlines the worst case at the window's peak and states the permission as its own sentence — never approached,
    /// approached (≥ 50 %), or reached (clamped, with the reason); basis 0 says no backend count was sampled so the
    /// permission is what was graded; the fact-less static names the rule. Counter-objectives and the no-DDL rule hold on
    /// every shape.</summary>
    [Fact]
    public void TheAdvice_StatesWhichWorstCaseWasGraded_ThePeakWithThePermissionBeside_OrThePermissionAloneWhenNothingWasSampled()
    {
        var wide = Snapshot(new[] { ("max_connections", "5000", (string?)null), ("work_mem", "4096", (string?)"kB") });
        var host = PgTargetFactCollector.SummariseHostMemory(Series(48, 0, 0.45, 16 * GiB), 3);
        AdviceBlock Compose(double? peak, long samples)
        {
            var facts = new List<Fact>();
            var terms = PgTargetFactCollector.ReadTerms(wide)!.Value with { PeakBackends = peak, PeakBackendsSamples = samples };
            PgTargetFactCollector.EmitMemoryFacts(Context(), facts, host, terms, 300, isAurora: true);
            new FactScorer().ScoreAll(facts);
            return PgTargetAdvice.Compose(PgTargetFactKeys.ConfigMemoryOvercommit, facts.ToFactLookup())!;
        }

        /* Basis 1, never approached: the headline is the graded peak figure, within the box; the permission is stated. */
        var quiet = Compose(50, 1440);
        Assert.Equal("The worst-case memory at the window's peak of 50 backends is 4.8 GB — 0.3× the host's 16 GB, within the box", quiet.Headline);
        foreach (var expected in new[]
                 {
                     "At the window's peak of 50 backends", "over 1440 sampled instants", "the same peak PG_SESSION_SATURATION divides by",
                     "the backend term is 600 MB and the sum is 4.8 GB, 0.3× of", "that is the ratio graded.",
                     "The settings permit 3.92× if all max_connections sorted at once — a ceiling the window's concurrency never approached: peak 50 of 5000; stated, not graded.",
                     "max_connections 5000 × work_mem 4 MB", "Under 1.0× the model fits the box",
                 })
            Assert.Contains(expected, quiet.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("No backend count was sampled", quiet.Investigation, StringComparison.Ordinal);

        /* Basis 1, approached (4,000 of 5,000 = 80 %): fires, and the sentence says the ceiling was approached. */
        var busy = Compose(4000, 1440);
        Assert.StartsWith("The worst-case memory at the window's peak of 4000 backends is ", busy.Headline, StringComparison.Ordinal);
        Assert.EndsWith("— more than double", busy.Headline, StringComparison.Ordinal);
        Assert.Contains("a ceiling the window's concurrency approached: peak 4000 of 5000; stated, not graded.", busy.Investigation, StringComparison.Ordinal);
        Assert.Contains("At or past 1.0× the backends this window actually ran CAN exceed physical memory if each spills once", busy.Investigation, StringComparison.Ordinal);
        Assert.Contains("advisory (0.4)", busy.Investigation, StringComparison.Ordinal);
        /* The 50 % line is the boundary between the two words. */
        Assert.Contains("approached: peak 2500 of 5000", Compose(2500, 1440).Investigation, StringComparison.Ordinal);
        Assert.Contains("never approached: peak 2499 of 5000", Compose(2499, 1440).Investigation, StringComparison.Ordinal);

        /* Basis 1, reached: a peak above the ceiling is said to be clamped, and why. */
        var over = Compose(6000, 1440);
        Assert.Contains("a ceiling the window's concurrency reached: peak 6000 of 5000, counted as 5000 (numbackends also counts autovacuum and parallel workers, which hold no connection slot)", over.Investigation, StringComparison.Ordinal);

        /* Basis 0: the permission is graded, and the card says so. */
        var unsampled = Compose(null, 0);
        Assert.StartsWith("The configured memory worst case is ", unsampled.Headline, StringComparison.Ordinal);
        Assert.Contains("No backend count was sampled in the window (pg_database_stats.numbackends carried no value), so this is the configured permission", unsampled.Investigation, StringComparison.Ordinal);
        Assert.Contains("and that is what was graded", unsampled.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("stated, not graded", unsampled.Investigation, StringComparison.Ordinal);
        Assert.Contains("At or past 1.0× the configuration CAN exceed physical memory if every backend spills once", unsampled.Investigation, StringComparison.Ordinal);

        /* The static names the rule — the graded term is the observed peak, the all-slots figure stated beside it. */
        var fixedText = PgTargetAdvice.Static(PgTargetFactKeys.ConfigMemoryOvercommit)!;
        Assert.Contains("uses the window's peak pg_database_stats.numbackends (clamped to max_connections) whenever an instant sampled it", fixedText.Investigation, StringComparison.Ordinal);
        Assert.Contains("with no sampled backend count the configured permission is what is graded", fixedText.Investigation, StringComparison.Ordinal);

        foreach (var block in new[] { quiet, busy, over, unsampled, fixedText })
        {
            Assert.Contains("Raising work_mem globally is the classic overcommit path", block.Remediation, StringComparison.Ordinal);
            var text = block.Headline + block.Investigation + block.Remediation;
            foreach (var forbidden in new[] { "fsync", "synchronous_commit", "full_page_writes", "CREATE INDEX" })
                Assert.DoesNotContain(forbidden, text, StringComparison.OrdinalIgnoreCase);
            Assert.Null(block.RemediationTsql);
        }
    }

    /* ───────────────────────── live: the careful test ───────────────────────── */

    /// <summary>The careful test. A stock-stamped server with a day of <c>pg_database_stats</c>, a config snapshot, and NO
    /// <c>pg_cpu_utilization</c> row ⇒ ONE memory fact, <c>PG_HOST_MEMORY_PRESSURE</c> unavailable with
    /// <c>reason_no_host_memory_source</c>, no composition fact; through the REAL <c>analyze_server</c> nothing memory-shaped
    /// roots, and <c>get_analysis_facts source=pg_memory</c> shows the one fact with its reason and the configured sum.</summary>
    [Fact]
    public async Task AStockTarget_WithNoHostMemoryRow_ReadsUnavailable_AndGetsNoCompositionVerdict()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the memory-family stock e2e.");

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
            var windowStart = end.AddHours(-4);
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, StockServerId, StockServerName, end.AddHours(-25), ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, StockServerId, StockServerName, windowStart.AddMinutes(minute - 1), ct);
            await PlantConfigAsync(connection, StockServerId, StockServerName, end.AddMinutes(-30), ct);

            var context = new AnalysisContext
            {
                ServerId = StockServerId, ServerName = StockServerName, TimeRangeStart = windowStart, TimeRangeEnd = end, ServerUtcOffset = TimeSpan.Zero,
                Coverage = new WindowCoverage { NominalMs = 4 * 3_600_000, ObservedMs = 4 * 3_600_000, SampleCount = 240 },
            };
            var collected = await new PgTargetFactCollector(postgres).CollectFactsAsync(context);
            var memory = collected.Where(f => f.Source == PgTargetSources.MemorySource).ToList();
            var pressure = Assert.Single(memory);
            Assert.Equal(PgTargetFactKeys.HostMemoryPressure, pressure.Key);
            Assert.Equal(1, pressure.Metadata["unavailable"]);
            Assert.Equal(1, pressure.Metadata[PgTargetScorer.HostMemoryReasonNoSourceKey]);
            Assert.Equal(4 * GiB, pressure.Metadata[PgTargetScorer.MemorySharedBuffersBytesKey]);

            var service = new DarlingAnalysisService(postgres);
            var asOf = end.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture);
            var json = await DarlingMcpTools.AnalyzeServer(service, postgres, StockServerName, 4, as_of: asOf);
            using (var doc = JsonDocument.Parse(json))
            {
                if (doc.RootElement.TryGetProperty("findings", out var findings) && findings.ValueKind == JsonValueKind.Array)
                    foreach (var finding in findings.EnumerateArray())
                        Assert.DoesNotContain(finding.GetProperty("root_fact").GetProperty("key").GetString(), new[] { PgTargetFactKeys.HostMemoryPressure, PgTargetFactKeys.ConfigMemoryOvercommit });
            }
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, StockServerName, 4, PgTargetSources.MemorySource, as_of: asOf);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var fact = Assert.Single(doc.RootElement.GetProperty("facts").EnumerateArray().ToList());
                Assert.Equal(PgTargetFactKeys.HostMemoryPressure, fact.GetProperty("key").GetString());
                Assert.Equal(0, fact.GetProperty("base_severity").GetDouble());
                Assert.Equal(1, fact.GetProperty("metadata").GetProperty(PgTargetScorer.HostMemoryReasonNoSourceKey).GetDouble());
                Assert.True(fact.GetProperty("metadata").GetProperty(PgTargetScorer.MemoryWorstCaseBytesKey).GetDouble() > 90.0 * GiB);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The exit criterion. Two Aurora-stamped servers with the planted snapshot (shared_buffers 4 GB, work_mem 64 MB,
    /// max_connections 500, parallel 2) and five-minute <c>pg_cpu_utilization</c> rows at 16 GiB total: the QUIET one at 45 %
    /// reclaimable ⇒ ratio ≈ 6.1 ⇒ ONE advisory card at exactly 0.4 through the real <c>analyze_server</c>, the terms in its
    /// prose; the SHORT one at 5 % reclaimable for the last hour and spilling 2 MiB/s of temp ⇒ the story
    /// <c>PG_HOST_MEMORY_PRESSURE → CONFIG_PG_MEMORY_OVERCOMMIT</c> at ≥ 0.5 with both co-fires named, and
    /// <c>get_analysis_facts source=pg_memory</c> showing the sum at 0.4 base, band 1, lineage 0.</summary>
    [Fact]
    public async Task TwoAuroraTargets_TheQuietOneRootsTheAdvisoryAt0Point4_TheShortSpillingOneTellsPressureToTheSum()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the memory-family Aurora e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var end = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = end.AddHours(-4);
            foreach (var (id, name, spilling) in new[] { (QuietServerId, QuietServerName, false), (ShortServerId, ShortServerName, true) })
            {
                await PgTargetFactCollectorTests.RegisterServerAsync(connection, id, name, MonitoredEngineKind.AuroraPostgres, 16, ct);
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, id, name, end.AddHours(-25), ct);
                for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                    await PlantDatabaseStatsWithTempAsync(connection, id, name, windowStart.AddMinutes(minute - 1), spilling ? (minute + 1) * 120L * MiB : 0, ct);
                await PlantConfigAsync(connection, id, name, end.AddMinutes(-30), ct);
                for (var minute = 0; minute <= 240; minute += 5)
                {
                    var shortHour = spilling && minute >= 180;
                    await PlantHostMemoryAsync(connection, id, name, windowStart.AddMinutes(minute), 16 * GiB, reclaimable: shortHour ? 0.05 : 0.45, ct);
                }
            }

            var service = new DarlingAnalysisService(postgres);
            var asOf = end.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture);
            var worst = 4.0 * GiB + 500.0 * 64 * MiB * 3 + 3.0 * 64 * MiB + 16.0 * MiB;

            /* ── Quiet: exactly one advisory card at 0.4 rooted on the sum, the terms in its prose. */
            var quietJson = await DarlingMcpTools.AnalyzeServer(service, postgres, QuietServerName, 4, as_of: asOf);
            using (var doc = JsonDocument.Parse(quietJson))
            {
                var findings = doc.RootElement.GetProperty("findings").EnumerateArray().ToList();
                var card = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.ConfigMemoryOvercommit);
                Assert.Equal(0.4, card.GetProperty("severity").GetDouble(), precision: 6);
                var advice = card.GetProperty("advice");
                Assert.Contains("shared_buffers 4 GB", advice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                Assert.Contains("max_connections 500 × work_mem 64 MB", advice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                Assert.Contains("advisory (0.4)", advice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                Assert.All(card.GetProperty("next_tools").EnumerateArray(), t => Assert.StartsWith("get_pg_", t.GetProperty("tool").GetString()!, StringComparison.Ordinal));
                Assert.DoesNotContain(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.HostMemoryPressure);
            }

            /* ── Short and spilling: the story, both co-fires, ≥ 0.5. */
            var shortJson = await DarlingMcpTools.AnalyzeServer(service, postgres, ShortServerName, 4, as_of: asOf);
            using (var doc = JsonDocument.Parse(shortJson))
            {
                var findings = doc.RootElement.GetProperty("findings").EnumerateArray().ToList();
                var story = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.HostMemoryPressure);
                Assert.Equal($"{PgTargetFactKeys.HostMemoryPressure} → {PgTargetFactKeys.ConfigMemoryOvercommit}", story.GetProperty("story_path").GetString());
                Assert.True(story.GetProperty("severity").GetDouble() >= 0.5);
                Assert.Contains("stayed under 10% for 3 consecutive samples", story.GetProperty("advice").GetProperty("headline").GetString(), StringComparison.Ordinal);
                Assert.DoesNotContain(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.ConfigMemoryOvercommit);
                /* The spill → sum edge (#3809, the third between-waves batch) does not change this outcome: pressure
                   outranks the spill, claims the sum first, and the spill's story cannot re-use a consumed leaf — so
                   the sum sits in exactly ONE story path on this server. */
                Assert.Single(findings, f => (f.GetProperty("story_path").GetString() ?? string.Empty).Contains(PgTargetFactKeys.ConfigMemoryOvercommit, StringComparison.Ordinal));
            }
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ShortServerName, 4, PgTargetSources.MemorySource, as_of: asOf);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var facts = doc.RootElement.GetProperty("facts").EnumerateArray().ToList();
                Assert.Equal(2, facts.Count);
                var sum = facts.Single(f => f.GetProperty("key").GetString() == PgTargetFactKeys.ConfigMemoryOvercommit);
                Assert.Equal(0.4, sum.GetProperty("base_severity").GetDouble(), precision: 6);
                Assert.Equal(worst / (16.0 * GiB), sum.GetProperty("value").GetDouble(), precision: 6);
                Assert.Equal(1, sum.GetProperty("metadata").GetProperty(PgTargetScorer.MemoryOvercommitCriticalBandKey).GetDouble());
                Assert.Equal(0, sum.GetProperty("metadata").GetProperty("threshold_lineage").GetDouble());
                Assert.True(sum.GetProperty("severity").GetDouble() >= 0.6, "both co-fires plus the band");
                var pressure = facts.Single(f => f.GetProperty("key").GetString() == PgTargetFactKeys.HostMemoryPressure);
                Assert.Equal(0.05, pressure.GetProperty("metadata").GetProperty(PgTargetScorer.HostMemorySustainedMinReclaimableShareKey).GetDouble(), precision: 3);
                Assert.True(pressure.GetProperty("base_severity").GetDouble() >= 0.5);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The collector reads the window's peak backends end to end (#3691 lane 47). The quiet Aurora server's own
    /// settings (a 6.12× permission on a 16 GiB host — the quiet e2e's card) with <c>pg_database_stats.numbackends</c>
    /// planted at 20 + 12 per instant across two databases and ONE instant at 30 + 20: through the REAL collector the fact
    /// is basis 1 at the 50-backend peak — 0.85×, base 0, no card — the permission stated as
    /// <c>configured_overcommit_ratio</c>, the peak and its sample count equal to what <c>PgTargetNumbackendsPeakSql</c>
    /// returns on its own, and <c>analyze_server</c> roots nothing on the sum.</summary>
    [Fact]
    public async Task AnAuroraTarget_WithSampledBackends_IsGradedAtThePeak_AndThePermissionIsStatedNotGraded()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the memory-family peak-backends e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var end = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = end.AddHours(-4);
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, PeakServerId, PeakServerName, MonitoredEngineKind.AuroraPostgres, 16, ct);
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, PeakServerId, PeakServerName, end.AddHours(-25), ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
            {
                var peakInstant = minute == 121;
                await PlantDatabaseStatsWithBackendsAsync(connection, PeakServerId, PeakServerName, windowStart.AddMinutes(minute - 1), peakInstant ? 30 : 20, peakInstant ? 20 : 12, ct);
            }
            await PlantConfigAsync(connection, PeakServerId, PeakServerName, end.AddMinutes(-30), ct);
            for (var minute = 0; minute <= 240; minute += 5)
                await PlantHostMemoryAsync(connection, PeakServerId, PeakServerName, windowStart.AddMinutes(minute), 16 * GiB, reclaimable: 0.45, ct);

            var configuredWorst = 4.0 * GiB + 500.0 * 64 * MiB * 3 + 3.0 * 64 * MiB + 16.0 * MiB;
            var observedWorst = 4.0 * GiB + 50.0 * 64 * MiB * 3 + 3.0 * 64 * MiB + 16.0 * MiB;

            /* ── The collector alone: basis 1 at the 50-backend peak. */
            var collector = new PgTargetFactCollector(postgres);
            var context = new AnalysisContext { ServerId = PeakServerId, ServerName = PeakServerName, TimeRangeStart = windowStart, TimeRangeEnd = end, ServerUtcOffset = TimeSpan.Zero };
            var collected = await collector.CollectFactsAsync(context);
            var sum = Assert.Single(collected, f => f.Key == PgTargetFactKeys.ConfigMemoryOvercommit);
            Assert.Equal(1, sum.Metadata[PgTargetScorer.MemoryOvercommitBasisKey]);
            Assert.Equal(50, sum.Metadata[PgTargetScorer.MemoryPeakBackendsKey]);
            Assert.True(sum.Metadata[PgTargetScorer.MemoryPeakBackendsSamplesKey] >= 240, "every in-window instant carried the level");
            Assert.Equal(observedWorst / (16.0 * GiB), sum.Value, precision: 6);
            Assert.Equal(sum.Value, sum.Metadata[PgTargetScorer.MemoryOvercommitRatioKey]);
            Assert.Equal(configuredWorst / (16.0 * GiB), sum.Metadata[PgTargetScorer.MemoryConfiguredOvercommitRatioKey], precision: 6);
            Assert.Equal(50.0 * 64 * MiB * 3, sum.Metadata[PgTargetScorer.MemoryObservedBackendTermBytesKey], precision: 0);
            /* The same peak PG_SESSION_SATURATION reads — one statement, so the two facts cannot disagree. */
            using (var levelRead = new NpgsqlCommand(PgTargetFactCollector.PgTargetNumbackendsPeakSql, connection))
            {
                levelRead.Parameters.AddWithValue(PeakServerId);
                levelRead.Parameters.AddWithValue(windowStart);
                levelRead.Parameters.AddWithValue(end);
                using var reader = await levelRead.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(Convert.ToDouble(reader.GetValue(2), System.Globalization.CultureInfo.InvariantCulture), sum.Metadata[PgTargetScorer.MemoryPeakBackendsKey]);
                Assert.Equal(Convert.ToDouble(reader.GetValue(1), System.Globalization.CultureInfo.InvariantCulture), sum.Metadata[PgTargetScorer.MemoryPeakBackendsSamplesKey]);
            }

            /* ── Through the REAL analyze_server: the sum fits the box, so nothing roots on it; get_analysis_facts carries the stamps. */
            var service = new DarlingAnalysisService(postgres);
            var asOf = end.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture);
            using (var doc = JsonDocument.Parse(await DarlingMcpTools.AnalyzeServer(service, postgres, PeakServerName, 4, as_of: asOf)))
            {
                /* Nothing else is planted to fire, so the honest outcome is the all-clear ("empty"); a findings array, if
                   another family ever fires here, must still carry no story through the sum. */
                if (doc.RootElement.TryGetProperty("findings", out var findingsElement))
                    Assert.DoesNotContain(findingsElement.EnumerateArray(), f => (f.GetProperty("story_path").GetString() ?? string.Empty).Contains(PgTargetFactKeys.ConfigMemoryOvercommit, StringComparison.Ordinal));
                else
                    Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
            }
            using (var doc = JsonDocument.Parse(await DarlingMcpTools.GetAnalysisFacts(service, postgres, PeakServerName, 4, PgTargetSources.MemorySource, as_of: asOf)))
            {
                var fact = doc.RootElement.GetProperty("facts").EnumerateArray().Single(f => f.GetProperty("key").GetString() == PgTargetFactKeys.ConfigMemoryOvercommit);
                Assert.Equal(0.0, fact.GetProperty("base_severity").GetDouble());
                Assert.Equal(observedWorst / (16.0 * GiB), fact.GetProperty("value").GetDouble(), precision: 6);
                var metadata = fact.GetProperty("metadata");
                Assert.Equal(1, metadata.GetProperty(PgTargetScorer.MemoryOvercommitBasisKey).GetDouble());
                Assert.Equal(50, metadata.GetProperty(PgTargetScorer.MemoryPeakBackendsKey).GetDouble());
                Assert.True(metadata.GetProperty(PgTargetScorer.MemoryConfiguredOvercommitRatioKey).GetDouble() > 1.0);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ───────────────────────── fixtures ───────────────────────── */

    private static AnalysisContext Context() => new()
    {
        ServerId = 1, ServerName = "pg", TimeRangeStart = new DateTime(2026, 9, 20, 8, 0, 0), TimeRangeEnd = new DateTime(2026, 9, 20, 12, 0, 0),
        Coverage = new WindowCoverage { NominalMs = 4 * 3_600_000, ObservedMs = 4 * 3_600_000, SampleCount = 240 },
    };

    /// <summary>The planted snapshot: shared_buffers 4 GB (blocks), work_mem 64 MB (kB), max_connections 500, parallel 2,
    /// maintenance_work_mem 64 MB, autovacuum_work_mem -1, autovacuum_max_workers 3, wal_buffers 16 MB (blocks),
    /// effective_cache_size 12 GB — with optional overrides and drops.</summary>
    private static Dictionary<string, (string? Setting, string? Unit)> Snapshot(params (string Name, string Setting, string? Unit)[] overrides) => Snapshot(overrides, drop: null);

    private static Dictionary<string, (string? Setting, string? Unit)> Snapshot((string Name, string Setting, string? Unit)[]? overrides = null, string[]? drop = null)
    {
        var rows = new Dictionary<string, (string? Setting, string? Unit)>(StringComparer.Ordinal)
        {
            ["shared_buffers"] = ("524288", "8kB"),
            ["work_mem"] = ("65536", "kB"),
            ["max_connections"] = ("500", null),
            ["max_parallel_workers_per_gather"] = ("2", null),
            ["maintenance_work_mem"] = ("65536", "kB"),
            ["autovacuum_work_mem"] = ("-1", "kB"),
            ["autovacuum_max_workers"] = ("3", null),
            ["wal_buffers"] = ("2048", "8kB"),
            ["effective_cache_size"] = ("1572864", "8kB"),
        };
        foreach (var (name, setting, unit) in overrides ?? []) rows[name] = (setting, unit);
        foreach (var name in drop ?? []) rows.Remove(name);
        return rows;
    }

    private static List<PgTargetFactCollector.HostMemorySample> Series(int withMemory, int without, double reclaimable, long total = 16 * GiB)
    {
        var list = new List<PgTargetFactCollector.HostMemorySample>();
        for (var i = 0; i < withMemory; i++)
            list.Add(new(total, (long)(total * reclaimable / 2), (long)(total * reclaimable / 2), (long)(total * 0.5), null));
        for (var i = 0; i < without; i++)
            list.Add(new(null, null, null, null, null));
        return list;
    }

    /// <summary>A scored-shape sum fact on a 64 GiB host: worst case = ratio × 64 GiB, the planted terms beside it.</summary>
    private static Fact Sum(double ratio)
    {
        var total = 64.0 * GiB;
        return new Fact
        {
            Source = PgTargetSources.MemorySource, Key = PgTargetFactKeys.ConfigMemoryOvercommit, Value = ratio, ServerId = 1,
            Metadata =
            {
                [PgTargetScorer.MemoryOvercommitRatioKey] = ratio,
                [PgTargetScorer.MemoryWorstCaseBytesKey] = ratio * total,
                [PgTargetScorer.MemoryTotalMinBytesKey] = total,
                [PgTargetScorer.MemoryTotalMaxBytesKey] = total,
                [PgTargetScorer.MemoryIsServerlessKey] = 0,
                [PgTargetScorer.MemorySharedBuffersBytesKey] = 4.0 * GiB,
                [PgTargetScorer.MemoryWorkMemBytesKey] = 64.0 * MiB,
                [PgTargetScorer.MemoryMaxConnectionsKey] = 500,
                [PgTargetScorer.MemoryParallelWorkersPerGatherKey] = 2,
                [PgTargetScorer.MemoryMaintWorkMemBytesKey] = 64.0 * MiB,
                [PgTargetScorer.MemoryAutovacuumMaxWorkersKey] = 3,
                [PgTargetScorer.MemoryWalBuffersBytesKey] = 16.0 * MiB,
                [PgTargetScorer.MemoryBackendTermBytesKey] = 500.0 * 64 * MiB * 3,
                [PgTargetScorer.MemoryAutovacuumTermBytesKey] = 3.0 * 64 * MiB,
                [PgTargetScorer.MemoryEffectiveCacheSizeBytesKey] = 12.0 * GiB,
                [PgTargetScorer.MemoryEffectiveCacheExceedsTotalKey] = 0,
                [PgTargetScorer.MemorySamplesKey] = 48,
                [PgTargetScorer.MemorySamplesWithMemoryKey] = 48,
                [PgTargetScorer.MemorySnapshotAgeSecondsKey] = 600,
            },
        };
    }

    private static Fact Pressure(double? sustained, double? min = null)
    {
        var fact = new Fact
        {
            Source = PgTargetSources.MemorySource, Key = PgTargetFactKeys.HostMemoryPressure, Value = min ?? sustained ?? 0, ServerId = 1,
            Metadata =
            {
                [PgTargetScorer.HostMemoryMinReclaimableShareKey] = min ?? sustained ?? 0,
                [PgTargetScorer.HostMemoryMeanReclaimableShareKey] = 0.3,
                [PgTargetScorer.HostMemoryPeakActiveShareKey] = 0.7,
                [PgTargetScorer.HostMemorySustainSamplesKey] = 3,
                [PgTargetScorer.MemoryTotalMinBytesKey] = 64.0 * GiB,
                [PgTargetScorer.MemoryTotalMaxBytesKey] = 64.0 * GiB,
                [PgTargetScorer.MemoryIsServerlessKey] = 0,
                [PgTargetScorer.MemorySamplesKey] = 48,
                [PgTargetScorer.MemorySamplesWithMemoryKey] = 48,
            },
        };
        if (sustained is { } s) fact.Metadata[PgTargetScorer.HostMemorySustainedMinReclaimableShareKey] = s;
        return fact;
    }

    /// <summary>Lane 6's spill fact at <paramref name="bytesPerSec"/> — its measured floor is 1 MiB/s.</summary>
    private static Fact Spill(double bytesPerSec) => new()
    {
        Source = PgTargetSources.TempSource, Key = PgTargetFactKeys.TempSpill, Value = bytesPerSec, ServerId = 1, DatabaseName = "appdb",
        Metadata =
        {
            [PgTargetScorer.TempSpillBytesKey] = bytesPerSec * 14_400,
            [PgTargetScorer.TempSpillFilesKey] = 239,
            [PgTargetScorer.TempSpillBytesPerSecKey] = bytesPerSec,
            [PgTargetScorer.CounterObservedMsKey] = 14_400_000,
        },
    };

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private static async Task PlantConfigAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, CancellationToken ct)
    {
        var collectionId = CollectionIdGenerator.Next();
        foreach (var (name, (setting, unit)) in Snapshot())
        {
            using var command = new NpgsqlCommand(@"
INSERT INTO pg_server_config
    (collection_id, collection_time, server_id, server_name, name, setting, unit, category, context, vartype, source, boot_val, reset_val, sourcefile, sourceline, pending_restart, short_desc)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'Test', 'postmaster', 'string', 'configuration file', $6, $6, NULL, NULL, false, NULL)", connection);
            command.Parameters.AddWithValue(collectionId);
            command.Parameters.AddWithValue(at);
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(serverName);
            command.Parameters.AddWithValue(name);
            command.Parameters.AddWithValue(setting!);
            command.Parameters.Add(new NpgsqlParameter { Value = (object?)unit ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>One five-minute capture as V136 stores it: <paramref name="reclaimable"/> of the total split evenly between
    /// free and cached, active the rest, configured NULL (a provisioned class).</summary>
    private static async Task PlantHostMemoryAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, long total, double reclaimable, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_cpu_utilization
    (collection_id, collection_time, server_id, server_name, sample_time, cpu_percent, acu_utilization_percent, serverless_capacity_acu, max_configured_acu,
     memory_total_bytes, memory_free_bytes, memory_cached_bytes, memory_buffers_bytes, memory_active_bytes, configured_memory_bytes)
VALUES ($1, $2, $3, $4, $2, 40, NULL, NULL, NULL, $5, $6, $6, 0, $7, NULL)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 9_000_000L);
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(total);
        command.Parameters.AddWithValue((long)(total * reclaimable / 2));
        command.Parameters.AddWithValue((long)(total * (1 - reclaimable)));
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>A <c>pg_database_stats</c> row with a cumulative <c>temp_bytes</c> — 120 MiB per minute is 2 MiB/s, twice lane 6's
    /// measured floor — so the REAL temp collector emits a fired <c>PG_TEMP_SPILL</c> for the co-fire.</summary>
    private static async Task PlantDatabaseStatsWithTempAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, long tempBytes, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
VALUES ($1, $2, $3, $4, 'appdb', 1000, 10, 100, 9000, $5, $6, 0, NULL)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(tempBytes / (8 * MiB));
        command.Parameters.AddWithValue(tempBytes);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Two <c>pg_database_stats</c> rows at one instant carrying <c>numbackends</c> (V133) — the level the overcommit
    /// fact's peak read sums across databases (#3691 lane 47). A NEW planter on purpose: the quiet and stock e2e planters
    /// leave numbackends NULL so those servers stay basis 0 and keep their subject.</summary>
    private static async Task PlantDatabaseStatsWithBackendsAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, int appBackends, int reportBackends, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset, numbackends)
VALUES ($1, $2, $3, $4, 'appdb',   1000, 10, 100, 9000, 0, 0, 0, NULL, $5),
       ($1, $2, $3, $4, 'reports', 1000, 10, 100, 9000, 0, 0, 0, NULL, $6)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(appBackends);
        command.Parameters.AddWithValue(reportBackends);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = $"({StockServerId}, {QuietServerId}, {ShortServerId}, {PeakServerId})";
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id IN {ids}; " +
            $"DELETE FROM pg_server_config WHERE server_id IN {ids}; " +
            $"DELETE FROM pg_cpu_utilization WHERE server_id IN {ids}; " +
            $"DELETE FROM analysis_findings WHERE server_id IN {ids}; " +
            $"DELETE FROM analysis_muted WHERE server_id IN {ids}; " +
            $"DELETE FROM servers WHERE server_id IN {ids};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
