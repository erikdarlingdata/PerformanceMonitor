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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The wait-profile family of the PostgreSQL-target analysis pass (#3542 lane 5): the two reads' shape, the
/// grouping and estimate arithmetic (executed on arranged rows through the collector's internal seam, no store),
/// the fraction bars and their lineage, the sampled-resolution guard, the amplifiers, the chains into the
/// write / memory / saturation families, and the advice's two evidence grades. The live half — both tables
/// planted on two servers and the real <c>analyze_server</c> driven — is <c>PgTargetWaitLiveTests</c>.
///
/// <para><b>The honesty pins the design's adversarial pass names (§6 A).</b> (1) <c>is_sampled = 1</c> and
/// <c>estimate_resolution_ms</c> on EVERY fact the sampling read emits and on NONE from the Aurora read; (2) a
/// sampled fact's advice says "estimated from sampling", and no wait block of either grade says the server
/// "spent" time; (3) a sampled fact under three Δsamples scores 0; (4) the Aurora read's interval is the
/// three-state idiom — stored → <c>NULLIF(…, 0)</c>, NULL → <c>LAG(collection_time)</c>, never <c>ELSE 0</c> —
/// and a restart collection contributes no sample. The SQL Server lock remedy does not transfer: the
/// <c>Lock:relation</c> block names PostgreSQL's levers and never the isolation-level one.</para>
/// </summary>
public sealed class PgTargetWaitTests
{
    private static readonly string Lock = PgTargetFactKeys.WaitKey("Lock", null);
    private static readonly string LWLock = PgTargetFactKeys.WaitKey("LWLock", null);
    private static readonly string Io = PgTargetFactKeys.WaitKey("IO", null);
    private static readonly string Ipc = PgTargetFactKeys.WaitKey("IPC", null);
    private static readonly string LockRelation = PgTargetFactKeys.WaitKey("Lock", "relation");
    private static readonly string WalWrite = PgTargetFactKeys.WaitKey("LWLock", "WALWrite");
    private static readonly string DataFileRead = PgTargetFactKeys.WaitKey("IO", "DataFileRead");
    private static readonly string WalSync = PgTargetFactKeys.WaitKey("IO", "WALSync");

    private static readonly string[] s_allKeys = [Lock, LWLock, Io, Ipc, LockRelation, WalWrite, DataFileRead, WalSync];

    /* ── the reads ── */

    [Fact]
    public void TheAuroraRead_UsesTheThreeStateInterval_ExcludesRestartCollections_AndBothReadsAreServerScoped()
    {
        var aurora = PgTargetFactCollector.PgWaitStatsSql;
        Assert.Contains("FROM pg_wait_stats", aurora, StringComparison.Ordinal);
        /* Stored → NULLIF; NULL → LAG; never ELSE 0. */
        Assert.Contains("CASE WHEN MAX(sample_interval_seconds) IS NULL", aurora, StringComparison.Ordinal);
        Assert.Contains("LAG(collection_time) OVER (ORDER BY collection_time)", aurora, StringComparison.Ordinal);
        Assert.Contains("ELSE NULLIF(MAX(sample_interval_seconds), 0)", aurora, StringComparison.Ordinal);
        Assert.DoesNotContain("ELSE 0", aurora, StringComparison.Ordinal);
        /* A restart collection (every row's stored interval 0) is counted and excluded, and the deltas of a
           collection with no countable interval are excluded by the JOIN, not just their time. */
        Assert.Contains("FILTER (WHERE stored_interval = 0)", aurora, StringComparison.Ordinal);
        Assert.Contains("AND c.interval_sec > 0", aurora, StringComparison.Ordinal);
        Assert.Contains("GREATEST(w.delta_wait_time_us, 0)", aurora, StringComparison.Ordinal);
        /* The header always returns: LEFT JOIN, so an empty window and an uncountable one are distinguishable. */
        Assert.Contains("FROM observed AS o", aurora, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN by_event AS b", aurora, StringComparison.Ordinal);

        var sampling = PgTargetFactCollector.PgWaitSamplingSql;
        Assert.Contains("FROM pg_wait_sampling", sampling, StringComparison.Ordinal);
        /* Δ per series per consecutive collection, the reader's reset rule (newest whole when it went backwards),
           the estimate made beside the count. */
        Assert.Contains("LAG(sample_count) OVER (PARTITION BY event_type, event, query_id ORDER BY collection_time)", sampling, StringComparison.Ordinal);
        Assert.Contains("WHEN sample_count < prev_count THEN sample_count", sampling, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_samples * profile_period_ms)", sampling, StringComparison.Ordinal);
        Assert.DoesNotContain("GREATEST(", sampling, StringComparison.Ordinal);
        Assert.Contains("FROM observed AS o", sampling, StringComparison.Ordinal);

        foreach (var sql in new[] { aurora, sampling })
        {
            Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
            Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
            Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("now()", sql, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(sql, PgTargetFactCollector.AllSql);
        }

        /* The collector routes READ-then-fallback on the data, never on the registry kind. */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Waits.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        Assert.DoesNotContain("engine_kind", code, StringComparison.Ordinal);
        Assert.DoesNotContain("IsAurora", code, StringComparison.Ordinal);
        Assert.DoesNotContain("EngineKind", code, StringComparison.Ordinal);
        Assert.True(code.IndexOf("PgWaitStatsSql", StringComparison.Ordinal) < code.IndexOf("PgWaitSamplingSql", StringComparison.Ordinal),
            "pg_wait_stats is read first; pg_wait_sampling is the fallback");
    }

    /* ── grouping and the estimate, executed ── */

    [Fact]
    public void TheAuroraRows_GroupIntoRollupsAndStandouts_WithFractionsOverTheWaitSourcesOwnTime_AndNoSampledKeys()
    {
        /* 4 h window witnessed at 14,400 s; the wait source observed 13,800 s (one restart minute and nine
           first-sighting minutes dropped) — the fraction is over 13,800, the witness fraction over 14,400. */
        var context = Context(observedMs: 14_400_000);
        var rows = new List<PgTargetFactCollector.WaitProfileRow>
        {
            new("lock", "relation", 2_760_000, 1_200),      /* 2,760 s = 0.20 of 13,800 */
            new("lock", "transactionid", 690_000, 300),     /* 690 s   = 0.05 */
            new("lwlock", "walwrite", 1_380_000, 50_000),   /* 1,380 s = 0.10 */
            new("io", "datafileread", 6_900_000, 900_000),  /* 6,900 s = 0.50 */
            new("io", "walsync", 1_380_000, 40_000),        /* 1,380 s = 0.10 */
            new("ipc", "parallelfinish", 138_000, 10),      /* 138 s   = 0.01 */
            new("cpu", "cpu", 20_000_000, 0),               /* on-CPU: never a fact, never in the share */
            new("bufferpin", "bufferpin", 13_800, 5),       /* in the share, not a fact */
        };
        var facts = new List<Fact>();

        PgTargetFactCollector.EmitWaitFacts(context, facts, rows, observedSec: 13_800, sampleCount: 230, collectionCount: 240,
            sampled: null, extra: (PgTargetScorer.WaitRestartCollectionsKey, 1));

        Assert.Equal(8, facts.Count);
        Assert.All(facts, f => Assert.Equal(PgTargetSources.WaitsSource, f.Source));
        Assert.All(facts, f => Assert.StartsWith(PgTargetFactKeys.WaitKeyPrefix, f.Key, StringComparison.Ordinal));
        Assert.Equal(s_allKeys.Order(StringComparer.Ordinal), facts.Select(f => f.Key).Order(StringComparer.Ordinal));

        var lock_ = facts.Single(f => f.Key == Lock);
        Assert.Equal("Lock", lock_.ObjectName);
        Assert.Equal(0.25, lock_.Value, precision: 9);                                       /* relation + transactionid */
        Assert.Equal(3_450_000, lock_.Metadata[PgTargetScorer.WaitMsKey], precision: 6);
        Assert.Equal(1_500, lock_.Metadata[PgTargetScorer.WaitCountKey]);
        Assert.Equal(2, lock_.Metadata[PgTargetScorer.WaitEventsInTypeKey]);
        Assert.Equal(0, lock_.Metadata[PgTargetScorer.WaitIsStandoutKey]);
        Assert.False(lock_.Metadata.ContainsKey(PgTargetScorer.WaitShareOfTypeKey));
        /* The rollups carry the largest named-standout fraction of their type; a standout carries none. */
        Assert.Equal(0.20, lock_.Metadata[PgTargetScorer.WaitMaxStandoutFractionKey], precision: 9);
        Assert.Equal(0.50, facts.Single(f => f.Key == Io).Metadata[PgTargetScorer.WaitMaxStandoutFractionKey], precision: 9);
        Assert.Equal(0.10, facts.Single(f => f.Key == LWLock).Metadata[PgTargetScorer.WaitMaxStandoutFractionKey], precision: 9);
        Assert.Equal(0.0, facts.Single(f => f.Key == Ipc).Metadata[PgTargetScorer.WaitMaxStandoutFractionKey]);
        Assert.False(facts.Single(f => f.Key == LockRelation).Metadata.ContainsKey(PgTargetScorer.WaitMaxStandoutFractionKey));

        var relation = facts.Single(f => f.Key == LockRelation);
        Assert.Equal("Lock:relation", relation.ObjectName);
        Assert.Equal(0.20, relation.Value, precision: 9);
        Assert.Equal(1, relation.Metadata[PgTargetScorer.WaitIsStandoutKey]);
        Assert.Equal(0.8, relation.Metadata[PgTargetScorer.WaitShareOfTypeKey], precision: 9);   /* 2,760 of 3,450 */
        /* Share of all waiting: CPU excluded, BufferPin included. Total = 3,450 + 1,380 + 6,900 + 1,380 + 138 + 13.8 s. */
        var totalWaitMs = 3_450_000 + 1_380_000 + 6_900_000 + 1_380_000 + 138_000 + 13_800.0;
        Assert.Equal(2_760_000 / totalWaitMs, relation.Metadata[PgTargetScorer.WaitShareOfWaitTimeKey], precision: 9);

        /* Both denominators stated; the fraction is over the wait source's own time. */
        Assert.Equal(13_800_000, relation.Metadata[PgTargetScorer.WaitSourceObservedMsKey], precision: 6);
        Assert.Equal(14_400_000, relation.Metadata[PgTargetScorer.WaitObservedMsKey], precision: 6);
        Assert.Equal(2_760_000 / 14_400_000.0, relation.Metadata[PgTargetScorer.WaitWitnessFractionKey], precision: 9);
        Assert.Equal(230, relation.Metadata[PgTargetScorer.WaitSampleCountKey]);
        Assert.Equal(240, relation.Metadata[PgTargetScorer.WaitCollectionCountKey]);
        Assert.Equal(1, relation.Metadata[PgTargetScorer.WaitRestartCollectionsKey]);
        Assert.Equal(1.0, relation.Metadata["coverage_fraction"], precision: 9);

        /* The Aurora grade carries NONE of the sampled keys. */
        foreach (var fact in facts)
        {
            Assert.False(fact.Metadata.ContainsKey(PgTargetScorer.WaitIsSampledKey), fact.Key);
            Assert.False(fact.Metadata.ContainsKey(PgTargetScorer.WaitEstimateResolutionMsKey), fact.Key);
            Assert.False(fact.Metadata.ContainsKey(PgTargetScorer.WaitDeltaSamplesKey), fact.Key);
        }

        Assert.Equal(0.60, facts.Single(f => f.Key == Io).Value, precision: 9);
        Assert.Equal(0.10, facts.Single(f => f.Key == WalSync).Value, precision: 9);
        Assert.Equal(0.01, facts.Single(f => f.Key == Ipc).Value, precision: 9);
        Assert.DoesNotContain(facts, f => f.ObjectName == "CPU" || f.Key.Contains("CPU", StringComparison.Ordinal));
    }

    [Fact]
    public void TheSampledRows_CarryTheEstimateArithmetic_IsSampledAndResolution_OnEveryFact()
    {
        /* The collector's rows already carry Δsamples × period_ms; the seam stamps the provenance. 10 ms period,
           observed 14,400 s: Lock:relation 144,000 samples × 10 ms = 1,440 s = 0.10 of a backend. */
        var context = Context(observedMs: 14_400_000);
        var rows = new List<PgTargetFactCollector.WaitProfileRow>
        {
            new("lock", "relation", 144_000 * 10, 144_000, PeakBackends: 7),
            new("lock", "tuple", 1_000 * 10, 1_000, PeakBackends: 9),
            new("io", "datafileread", 288_000 * 10, 288_000, PeakBackends: 2),
            new("cpu", "running", 5_000_000, 500_000, PeakBackends: 40),
        };
        var facts = new List<Fact>();

        PgTargetFactCollector.EmitWaitFacts(context, facts, rows, observedSec: 14_400, sampleCount: 48, collectionCount: 49,
            sampled: (PeriodMs: 10, Resets: 1), extra: null);

        /* Lock, Lock:relation, IO, IO:DataFileRead — the two types present and their standouts. */
        Assert.Equal(4, facts.Count);
        foreach (var fact in facts)
        {
            Assert.Equal(1, fact.Metadata[PgTargetScorer.WaitIsSampledKey]);
            Assert.Equal(10, fact.Metadata[PgTargetScorer.WaitEstimateResolutionMsKey]);
            Assert.Equal(1, fact.Metadata[PgTargetScorer.WaitCounterResetsKey]);
            Assert.True(fact.Metadata.ContainsKey(PgTargetScorer.WaitDeltaSamplesKey), fact.Key);
            Assert.False(fact.Metadata.ContainsKey(PgTargetScorer.WaitRestartCollectionsKey), fact.Key);
        }

        var relation = facts.Single(f => f.Key == LockRelation);
        Assert.Equal(0.10, relation.Value, precision: 9);
        Assert.Equal(1_440_000, relation.Metadata[PgTargetScorer.WaitMsKey], precision: 6);
        Assert.Equal(144_000, relation.Metadata[PgTargetScorer.WaitDeltaSamplesKey]);
        Assert.Equal(144_000, relation.Metadata[PgTargetScorer.WaitCountKey]);
        Assert.Equal(144_000 / 145_000.0, relation.Metadata[PgTargetScorer.WaitShareOfTypeKey], precision: 9);
        /* CPU/Running samples are not waiting: share denominator = 1,440 + 10 + 2,880 s. */
        Assert.Equal(1_440_000 / 4_330_000.0, relation.Metadata[PgTargetScorer.WaitShareOfWaitTimeKey], precision: 9);
        /* peak_backends is PER KEY — the standout's own series, the rollup's most over its events — never the
           window's (the CPU row's 40 must not leak into a wait fact; the live run caught exactly that). */
        Assert.Equal(7, relation.Metadata[PgTargetScorer.WaitPeakBackendsKey]);
        Assert.Equal(9, facts.Single(f => f.Key == Lock).Metadata[PgTargetScorer.WaitPeakBackendsKey]);
        Assert.Equal(2, facts.Single(f => f.Key == DataFileRead).Metadata[PgTargetScorer.WaitPeakBackendsKey]);
        Assert.Equal(2, facts.Single(f => f.Key == Io).Metadata[PgTargetScorer.WaitPeakBackendsKey]);
    }

    [Fact]
    public void NoObservedTime_NoWait_AndAnUnknownTypeEmitNothing()
    {
        var context = Context(observedMs: 14_400_000);
        var rows = new List<PgTargetFactCollector.WaitProfileRow> { new("lock", "relation", 1_000, 10) };

        /* Every in-window collection was a restart: rows exist, the wait source observed nothing → no fact. */
        var none = new List<Fact>();
        PgTargetFactCollector.EmitWaitFacts(context, none, rows, observedSec: 0, sampleCount: 0, collectionCount: 5, sampled: null, extra: null);
        Assert.Empty(none);

        /* A key with zero wait in the window is a fact about nothing; a type outside the vocabulary is summed
           into the share but never keyed. */
        var some = new List<Fact>();
        PgTargetFactCollector.EmitWaitFacts(context, some,
            [new("lock", "relation", 0, 0), new("extension", "extension", 5_000, 3), new("io", "walsync", 500, 2)],
            observedSec: 600, sampleCount: 10, collectionCount: 10, sampled: null, extra: null);
        Assert.Equal(new[] { Io, WalSync }.Order(StringComparer.Ordinal), some.Select(f => f.Key).Order(StringComparer.Ordinal));
        Assert.Equal(500 / 5_500.0, some.Single(f => f.Key == WalSync).Metadata[PgTargetScorer.WaitShareOfWaitTimeKey], precision: 9);
    }

    /* ── the bars ── */

    [Fact]
    public void EveryV1WaitKey_HasFractionBars_AnUnknownKeyHasNone_AndNoBarIsASqlServerWaitConstant()
    {
        foreach (var key in s_allKeys)
        {
            var bars = PgTargetScorer.GetPgWaitThresholds(key);
            Assert.NotNull(bars);
            Assert.True(bars.Value.Concerning > 0 && bars.Value.Critical > bars.Value.Concerning, key);
        }
        Assert.Null(PgTargetScorer.GetPgWaitThresholds(PgTargetFactKeys.WaitKey("Client", "ClientRead")));
        Assert.Null(PgTargetScorer.GetPgWaitThresholds(PgTargetFactKeys.WaitKey("Lock", "transactionid")));
        Assert.Null(PgTargetScorer.GetPgWaitThresholds("LCK_M_S"));

        /* The key is the case-insensitivity: an Aurora major's re-cased spelling lands on the same entry. */
        Assert.Equal(PgTargetScorer.GetPgWaitThresholds(LockRelation), PgTargetScorer.GetPgWaitThresholds(PgTargetFactKeys.WaitKey("LOCK", "Relation")));

        /* No SQL Server wait bar reused by value (the SQL Server table's concerning / critical constants). */
        var sqlServerBars = new HashSet<double> { 0.01, 0.05, 0.10, 0.25, 0.30, 0.50, 0.75 };
        foreach (var key in s_allKeys)
        {
            var bars = PgTargetScorer.GetPgWaitThresholds(key)!.Value;
            Assert.DoesNotContain(bars.Concerning, sqlServerBars);
            Assert.DoesNotContain(bars.Critical, sqlServerBars);
        }
    }

    [Fact]
    public void TheFraction_ScoresZeroBelowConcerning_HalfAtIt_OneAtCritical_AndStampsTheLineage()
    {
        var bars = PgTargetScorer.GetPgWaitThresholds(LockRelation)!.Value;

        var below = Aurora(LockRelation, bars.Concerning * 0.9);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(below));
        Assert.Equal(0, below.Metadata["threshold_lineage"]);   /* stated even when not fired; a standout's per-event bars are unmeasured */

        Assert.Equal(0.5, PgTargetScorer.ScoreBase(Aurora(LockRelation, bars.Concerning)), precision: 9);
        var mid = (bars.Concerning + bars.Critical) / 2;
        Assert.Equal(0.75, PgTargetScorer.ScoreBase(Aurora(LockRelation, mid)), precision: 9);
        Assert.Equal(1.0, PgTargetScorer.ScoreBase(Aurora(LockRelation, bars.Critical)), precision: 9);
        Assert.Equal(1.0, PgTargetScorer.ScoreBase(Aurora(LockRelation, 3.0)), precision: 9);   /* >1.0: concurrent waiters */

        /* The IO rollup's bars sit at twice the others; the rollups and standouts each grade on their own row. */
        Assert.Equal(0.5, PgTargetScorer.ScoreBase(Rollup(Io, PgTargetScorer.WaitIoConcerning, maxStandout: 0.0)), precision: 9);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Rollup(Io, PgTargetScorer.WaitRollupConcerning, maxStandout: 0.0)));
        Assert.Equal(0.5, PgTargetScorer.ScoreBase(Rollup(Lock, PgTargetScorer.WaitRollupConcerning, maxStandout: 0.0)), precision: 9);

        /* An Aurora ROLLUP graded on its own fraction is graded on the fleet-measured type bars (#3691, 2026-09-19)
           and says so; a standout on the same fraction is graded on the unmeasured per-event bars and says 0. */
        var measuredRollup = Rollup(Lock, PgTargetScorer.WaitRollupConcerning, maxStandout: 0.0);
        PgTargetScorer.ScoreBase(measuredRollup);
        Assert.Equal(1, measuredRollup.Metadata["threshold_lineage"]);
        var quietRollup = Rollup(Io, PgTargetScorer.WaitIoConcerning * 0.5, maxStandout: 0.0);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(quietRollup));
        Assert.Equal(1, quietRollup.Metadata["threshold_lineage"]);   /* the verdict is on the bars, not the grade */
        var standout = Aurora(LockRelation, bars.Concerning);
        PgTargetScorer.ScoreBase(standout);
        Assert.Equal(0, standout.Metadata["threshold_lineage"]);

        /* One wait is graded once: a rollup whose named standout is itself a finding yields to it (scores 0,
           lineage still stated); a rollup whose standouts sit under their bar grades on its whole fraction. */
        var yielded = Rollup(Lock, 0.9, maxStandout: PgTargetScorer.WaitStandoutConcerning);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(yielded));
        Assert.Equal(0, yielded.Metadata["threshold_lineage"]);   /* the yield was decided on the unmeasured standout bar */
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Rollup(Io, 1.5, maxStandout: 0.6)));
        var unnamed = Rollup(Lock, 0.9, maxStandout: PgTargetScorer.WaitStandoutConcerning * 0.9);
        Assert.InRange(PgTargetScorer.ScoreBase(unnamed), 0.9, 1.0);
        /* A standout is never yielded: the key on the same fraction grades regardless of the rollup's state. */
        Assert.InRange(PgTargetScorer.ScoreBase(Aurora(LockRelation, 0.9)), 0.9, 1.0);

        /* No observed time → 0; a key outside the vocabulary → 0 without stamping. */
        var unobserved = Aurora(LockRelation, 0.9);
        unobserved.Metadata[PgTargetScorer.WaitSourceObservedMsKey] = 0;
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(unobserved));
        var unknown = Aurora(PgTargetFactKeys.WaitKey("Lock", "transactionid"), 0.9);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(unknown));
        Assert.False(unknown.Metadata.ContainsKey("threshold_lineage"));
    }

    [Fact]
    public void ASampledFact_GradesOnTheSameBars_AndScoresZeroUnderThreeSamples()
    {
        var bars = PgTargetScorer.GetPgWaitThresholds(LockRelation)!.Value;
        var mid = (bars.Concerning + bars.Critical) / 2;

        var measured = Aurora(LockRelation, mid);
        var estimated = Sampled(LockRelation, mid, deltaSamples: 3);
        Assert.Equal(PgTargetScorer.ScoreBase(measured), PgTargetScorer.ScoreBase(estimated), precision: 9);
        Assert.Equal(0.75, PgTargetScorer.ScoreBase(estimated), precision: 9);
        Assert.Equal(0, estimated.Metadata["threshold_lineage"]);

        /* The 2026-09-19 measurement is of Aurora's engine-measured waits; a stock sampled ESTIMATE is another
           instrument on another population, so even a sampled ROLLUP keeps 0 where the Aurora rollup says 1. */
        var sampledRollup = Sampled(Lock, PgTargetScorer.WaitRollupConcerning, deltaSamples: 3);
        sampledRollup.Metadata[PgTargetScorer.WaitIsStandoutKey] = 0;
        sampledRollup.Metadata[PgTargetScorer.WaitMaxStandoutFractionKey] = 0.0;
        Assert.Equal(0.5, PgTargetScorer.ScoreBase(sampledRollup), precision: 9);
        Assert.Equal(0, sampledRollup.Metadata["threshold_lineage"]);

        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Sampled(LockRelation, mid, deltaSamples: 2)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Sampled(LockRelation, 5.0, deltaSamples: 0)));
        Assert.Equal(3, PgTargetScorer.WaitSampledMinimumSamples);
    }

    /* ── amplifiers ── */

    [Fact]
    public void TheCoFires_LiftTheWaitByTheStatedBoost_OnlyWhenTheOtherFactFired()
    {
        var lock_ = Rollup(Lock, PgTargetScorer.WaitRollupConcerning, maxStandout: 0.0);
        var relation = Aurora(LockRelation, PgTargetScorer.WaitStandoutConcerning);
        var walSync = Aurora(WalSync, PgTargetScorer.WaitStandoutConcerning);
        var walWrite = Aurora(WalWrite, PgTargetScorer.WaitStandoutConcerning);
        var read = Aurora(DataFileRead, PgTargetScorer.WaitStandoutConcerning);
        var io = Rollup(Io, PgTargetScorer.WaitIoConcerning, maxStandout: 0.0);
        var saturation = Saturation(fired: true);
        var pressure = Pressure(0.9);
        var buffer = Buffer(0.30);
        var facts = new List<Fact> { lock_, relation, walSync, walWrite, read, io, saturation, pressure, buffer };
        new FactScorer().ScoreAll(facts);

        Assert.True(saturation.BaseSeverity > 0 && pressure.BaseSeverity > 0 && buffer.BaseSeverity > 0,
            $"{saturation.BaseSeverity} {pressure.BaseSeverity} {buffer.BaseSeverity}");
        foreach (var lifted in new[] { lock_, relation, walSync, walWrite, read })
        {
            Assert.Equal(0.5, lifted.BaseSeverity, precision: 9);
            Assert.Equal(0.5 * (1 + PgTargetScorer.WaitCoFireBoost), lifted.Severity, precision: 9);
            Assert.Single(lifted.AmplifierResults, a => a.Matched);
        }
        Assert.Contains(lock_.AmplifierResults, a => a.Matched && a.Description.Contains("PG_CONNECTION_SATURATION co-fired", StringComparison.Ordinal));
        Assert.Contains(lock_.AmplifierResults, a => !a.Matched && a.Description.Contains("PG_IDLE_IN_TRANSACTION", StringComparison.Ordinal));
        Assert.Contains(walSync.AmplifierResults, a => a.Matched && a.Description.Contains("PG_CHECKPOINT_PRESSURE co-fired", StringComparison.Ordinal));
        Assert.Contains(walWrite.AmplifierResults, a => a.Matched && a.Description.Contains("PG_CHECKPOINT_PRESSURE co-fired", StringComparison.Ordinal));
        Assert.Contains(read.AmplifierResults, a => a.Matched && a.Description.Contains("PG_BUFFER_CACHE_PRESSURE co-fired", StringComparison.Ordinal));
        /* The IO rollup has no amplifier in v1. */
        Assert.Empty(io.AmplifierResults);
        Assert.Equal(io.BaseSeverity, io.Severity);

        /* Present but not fired is not a co-fire. */
        var quiet = new List<Fact> { Rollup(Lock, PgTargetScorer.WaitRollupConcerning, maxStandout: 0.0), Saturation(fired: false) };
        new FactScorer().ScoreAll(quiet);
        Assert.Equal(0.5, quiet[0].Severity, precision: 9);
        Assert.DoesNotContain(quiet[0].AmplifierResults, a => a.Matched);
    }

    /* ── chains ── */

    [Fact]
    public void AFiredRelationLock_LeadsToSaturation_AndTheRollupYieldsToIt_OrLeadsThereItselfWhenUnnamed()
    {
        /* Lock 0.25 with Lock:relation 0.20 inside it (the live shape): the rollup scores 0, the standout roots,
           one story — Lock:relation → PG_CONNECTION_SATURATION. */
        var facts = new List<Fact>
        {
            Rollup(Lock, 0.25, maxStandout: 0.20), Aurora(LockRelation, 0.20), Saturation(fired: true),
        };
        new FactScorer().ScoreAll(facts);
        Assert.Equal(0.0, facts[0].Severity);
        Assert.True(facts[1].Severity > facts[2].Severity, $"{facts[1].Severity} {facts[2].Severity}");
        var story = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts));
        Assert.Equal(new[] { LockRelation, PgTargetFactKeys.ConnectionSaturation }, story.Path);
        Assert.Equal(PgTargetSources.WaitsSource, story.Category);

        /* The MESH (lane 4's lesson, safe here because saturation has no other leaf): when saturation OUTRANKS
           the wait (at the 0.9 critical band = 1.0), it roots and walks to the fired wait — still one story,
           the other way round — instead of leaving the wait to root a second card. */
        var causeLeads = new List<Fact> { Rollup(Lock, 0.25, maxStandout: 0.20), Aurora(LockRelation, 0.20), Saturation(fired: true, ratio: 0.9) };
        new FactScorer().ScoreAll(causeLeads);
        Assert.True(causeLeads[2].Severity > causeLeads[1].Severity, $"{causeLeads[2].Severity} {causeLeads[1].Severity}");
        var meshed = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(causeLeads));
        Assert.Equal(new[] { PgTargetFactKeys.ConnectionSaturation, LockRelation }, meshed.Path);
        Assert.Equal(PgTargetSources.SessionsSource, meshed.Category);

        /* The class's waiting is in events the vocabulary does not name (transactionid, tuple): the rollup is
           the most specific true thing and leads to saturation itself. */
        var unnamed = new List<Fact> { Rollup(Lock, 0.6, maxStandout: 0.05), Saturation(fired: true) };
        new FactScorer().ScoreAll(unnamed);
        var direct = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(unnamed));
        Assert.Equal(new[] { Lock, PgTargetFactKeys.ConnectionSaturation }, direct.Path);

        /* Saturation present but not fired: the wait roots alone; the edge is defined but inactive. */
        var graph = new PgTargetRelationshipGraph();
        var quiet = new List<Fact> { Aurora(LockRelation, 0.6), Saturation(fired: false) };
        new FactScorer().ScoreAll(quiet);
        Assert.Empty(graph.GetActiveEdges(LockRelation, quiet.ToFactLookup()));
        Assert.Single(graph.GetAllEdges(LockRelation));
        Assert.Single(graph.GetAllEdges(Lock));
        /* Both directions of the mesh are declared from saturation too. */
        Assert.Equal(
            new[] { Lock, LockRelation }.Order(StringComparer.Ordinal),
            graph.GetAllEdges(PgTargetFactKeys.ConnectionSaturation).Select(e => e.Destination).Order(StringComparer.Ordinal));
        Assert.Equal(new[] { LockRelation }, Assert.Single(new InferenceEngine(graph).BuildStories(quiet)).Path);
    }

    [Fact]
    public void TheWalWaits_LeadIntoTheWriteChain_AndDataFileRead_IntoTheMemoryChain_WhileTheRollupsStayOut()
    {
        /* IO:WALSync → PG_CHECKPOINT_PRESSURE → CONFIG_PG_MAX_WAL_SIZE, one story, when the wait outranks the
           pressure (0.6 on the standout ramp = 0.765, × 1.3 for the co-fire = 0.99; the pressure at its majority
           line = 0.5, × 1.3 for the knob cause = 0.65); the IO rollup (its standout fired) scores 0 and roots
           nothing. */
        var write = new List<Fact>
        {
            Rollup(Io, 1.2, maxStandout: 0.6), Aurora(WalSync, 0.6), Pressure(0.5), Knob(PgTargetFactKeys.ConfigMaxWalSize, 1024),
        };
        new FactScorer().ScoreAll(write);
        Assert.Equal(0.0, write[0].Severity);
        Assert.True(write[1].Severity > write[2].Severity, $"{write[1].Severity} {write[2].Severity}");
        var story = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(write));
        Assert.Equal(new[] { WalSync, PgTargetFactKeys.CheckpointPressure, PgTargetFactKeys.ConfigMaxWalSize }, story.Path);

        /* When the CAUSE outranks the symptom (near-total requested share: 1.0 × 1.3 = 1.3), the pressure roots
           first and walks to its knob, and the wait roots a second, one-fact story whose advice names the
           co-fire — pinned as the honest shape rather than papered over: a reverse edge from the pressure to
           the wait would make the walk take the higher-severity wait over the knob and leave the knob to root
           a third card. The engine's path is linear; the mesh lane 4 built has no knob leaf to protect. */
        var causeLeads = new List<Fact> { Aurora(WalSync, 0.6), Pressure(0.9), Knob(PgTargetFactKeys.ConfigMaxWalSize, 1024) };
        new FactScorer().ScoreAll(causeLeads);
        var split = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(causeLeads);
        Assert.Equal(2, split.Count);
        Assert.Equal(new[] { PgTargetFactKeys.CheckpointPressure, PgTargetFactKeys.ConfigMaxWalSize }, split[0].Path);
        Assert.Equal(new[] { WalSync }, split[1].Path);
        Assert.Contains("PG_CHECKPOINT_PRESSURE co-fired", PgTargetAdvice.Compose(WalSync, causeLeads.ToFactLookup())!.Investigation, StringComparison.Ordinal);

        /* LWLock:WALWrite alone → PG_CHECKPOINT_PRESSURE (the wait outranking the pressure, as above). */
        var lw = new List<Fact> { Aurora(WalWrite, 0.6), Pressure(0.5) };
        new FactScorer().ScoreAll(lw);
        Assert.Equal(new[] { WalWrite, PgTargetFactKeys.CheckpointPressure }, Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(lw)).Path);

        /* IO:DataFileRead → PG_BUFFER_CACHE_PRESSURE → CONFIG_PG_SHARED_BUFFERS. */
        var memory = new List<Fact> { Aurora(DataFileRead, 0.6), Buffer(0.30), Knob(PgTargetFactKeys.ConfigSharedBuffers, 128) };
        new FactScorer().ScoreAll(memory);
        var read = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(memory));
        Assert.Equal(new[] { DataFileRead, PgTargetFactKeys.BufferCachePressure, PgTargetFactKeys.ConfigSharedBuffers }, read.Path);

        /* The rollups have no edges of their own except Lock's; every wait edge's predicate reads the
           destination's verdict — nothing fired, no edge. */
        var graph = new PgTargetRelationshipGraph();
        Assert.Empty(graph.GetAllEdges(Io));
        Assert.Empty(graph.GetAllEdges(LWLock));
        Assert.Empty(graph.GetAllEdges(Ipc));
        var quiet = new List<Fact> { Aurora(WalSync, 0.6), Pressure(0.1) };
        new FactScorer().ScoreAll(quiet);
        Assert.Empty(graph.GetActiveEdges(WalSync, quiet.ToFactLookup()));
        Assert.NotEmpty(graph.GetAllEdges(WalSync));
    }

    /* ── advice ── */

    [Fact]
    public void ASampledFactsAdvice_SaysEstimatedFromSampling_AndNoWaitBlockSaysSpent()
    {
        foreach (var key in s_allKeys)
        {
            var measured = Aurora(key, 0.6);
            var estimated = Sampled(key, 0.6, deltaSamples: 6_000);
            var lookupMeasured = Lookup(measured);
            var lookupEstimated = Lookup(estimated);

            var blocks = new (string Grade, AdviceBlock Block)[]
            {
                ("aurora", PgTargetAdvice.Compose(key, lookupMeasured)!),
                ("sampled", PgTargetAdvice.Compose(key, lookupEstimated)!),
                ("static", PgTargetAdvice.Static(key)!),
            };
            foreach (var (grade, block) in blocks)
            {
                var text = block.Headline + "\n" + block.Investigation + "\n" + block.Remediation;
                Assert.DoesNotContain("spent", text, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("RCSI", text, StringComparison.Ordinal);
                Assert.DoesNotContain("READ_COMMITTED_SNAPSHOT", text, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("CREATE INDEX", text, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("synchronous_commit", text, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("fsync = off", text, StringComparison.OrdinalIgnoreCase);
                Assert.Null(block.RemediationTsql);
                Assert.False(string.IsNullOrWhiteSpace(block.Remediation), $"{key} {grade}");
            }

            var sampledText = blocks[1].Block.Headline + blocks[1].Block.Investigation + blocks[1].Block.Remediation;
            Assert.Contains("estimated from sampling", sampledText, StringComparison.Ordinal);
            Assert.Contains("10 ms sampling period", blocks[1].Block.Investigation, StringComparison.Ordinal);
            Assert.Contains("6,000 backend-samples", blocks[1].Block.Investigation, StringComparison.Ordinal);
            Assert.Contains("per BACKEND-sample", blocks[1].Block.Investigation, StringComparison.Ordinal);

            /* The measured grade says the engine measured it and does not call itself an estimate. */
            Assert.Contains("the engine measured", blocks[0].Block.Investigation, StringComparison.Ordinal);
            Assert.DoesNotContain("estimated from sampling", blocks[0].Block.Investigation, StringComparison.Ordinal);

            /* Value-stated: the wait seconds, the observed seconds and the share, from the fact. */
            Assert.Contains("0.6 of one backend", blocks[0].Block.Investigation, StringComparison.Ordinal);
            Assert.Contains("40 % of all waiting", blocks[0].Block.Investigation, StringComparison.Ordinal);
            Assert.Contains("4 h of observed", blocks[0].Block.Investigation, StringComparison.Ordinal);

            /* The static block is a PostgreSQL block for the key, pointing at the PostgreSQL reads. */
            Assert.Contains("get_pg_wait_stats", blocks[2].Block.Investigation, StringComparison.Ordinal);
            Assert.Contains("get_pg_wait_sampling", blocks[2].Block.Investigation, StringComparison.Ordinal);
        }

        /* Outside the vocabulary: no block (the shared caller then finds nothing SQL Server either). */
        Assert.Null(PgTargetAdvice.Static(PgTargetFactKeys.WaitKey("Lock", "transactionid")));
        Assert.Null(PgTargetAdvice.Static(PgTargetFactKeys.WaitKey("Client", "ClientRead")));
    }

    [Fact]
    public void TheRelationLockAdvice_NamesThePostgresLevers_TheHolder_AndMvcc_AndTheCoFireWhenSaturationFired()
    {
        var relation = Aurora(LockRelation, 0.6);
        var saturation = Saturation(fired: true);
        var facts = new List<Fact> { relation, saturation };
        new FactScorer().ScoreAll(facts);

        var block = PgTargetAdvice.Compose(LockRelation, facts.ToFactLookup())!;
        Assert.Contains("MVCC", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("PG_CONNECTION_SATURATION co-fired", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("lock_timeout", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("idle_in_transaction_session_timeout", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("holder", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_pg_blocking", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("CONCURRENTLY", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("counter-objective", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("Resolve PG_CONNECTION_SATURATION", block.Remediation, StringComparison.Ordinal);

        /* Without the co-fire the sentence is absent — the block states what fired, not what might. */
        var alone = PgTargetAdvice.Compose(LockRelation, Lookup(Aurora(LockRelation, 0.6)))!;
        Assert.DoesNotContain("co-fired", alone.Investigation, StringComparison.Ordinal);

        /* The WAL waits name the checkpoint chain and refuse the durability lever. */
        var wal = new List<Fact> { Aurora(WalSync, 0.6), Pressure(0.9) };
        new FactScorer().ScoreAll(wal);
        var walBlock = PgTargetAdvice.Compose(WalSync, wal.ToFactLookup())!;
        Assert.Contains("PG_CHECKPOINT_PRESSURE co-fired", walBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("Durability settings are not a lever here", walBlock.Remediation, StringComparison.Ordinal);
        Assert.Contains("max_wal_size", walBlock.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheWitnessDivergenceSentence_AppearsOnlyWhenTheTwoDenominatorsDiffer()
    {
        var same = Aurora(LockRelation, 0.6);
        Assert.DoesNotContain("coverage witness", PgTargetAdvice.Compose(LockRelation, Lookup(same))!.Investigation, StringComparison.Ordinal);

        var differs = Aurora(LockRelation, 0.6);
        differs.Metadata[PgTargetScorer.WaitSourceObservedMsKey] = 3_600_000;     /* hourly stock cadence: 1 h of 4 observed */
        Assert.Contains("coverage witness (pg_database_stats) observed 4 h", PgTargetAdvice.Compose(LockRelation, Lookup(differs))!.Investigation, StringComparison.Ordinal);
        Assert.Contains("own 1 h", PgTargetAdvice.Compose(LockRelation, Lookup(differs))!.Investigation, StringComparison.Ordinal);
    }

    /* ── the shared-switch delegation moved its wait line (PgTargetSharedSwitchRoutingTests) ── */

    [Fact]
    public void TheSharedEntryPoints_AnswerThisFamilysBlock_ForAWaitKey()
    {
        var lookup = Lookup(Sampled(LockRelation, 0.6, deltaSamples: 500));
        Assert.Equal(PgTargetAdvice.Compose(LockRelation, lookup), FactAdvice.Compose(LockRelation, lookup));
        Assert.Equal(PgTargetAdvice.Static(LockRelation), FactAdvice.GetForFactKey(LockRelation));
        Assert.NotNull(FactAdvice.GetForFactKey(LockRelation));
    }

    /* ── helpers ── */

    private static AnalysisContext Context(double observedMs) => new()
    {
        ServerId = 1,
        ServerName = "pg",
        TimeRangeStart = new DateTime(2026, 9, 18, 0, 0, 0, DateTimeKind.Unspecified),
        TimeRangeEnd = new DateTime(2026, 9, 18, 4, 0, 0, DateTimeKind.Unspecified),
        Coverage = new WindowCoverage { NominalMs = 14_400_000, ObservedMs = observedMs, SampleCount = 241, LargestGapMs = 60_000 },
    };

    /// <summary>An Aurora-grade wait fact at <paramref name="fraction"/> over a 4 h wait-source window (share of all waiting 40 %).</summary>
    private static Fact Aurora(string key, double fraction) => new()
    {
        Source = PgTargetSources.WaitsSource,
        Key = key,
        Value = fraction,
        ServerId = 1,
        ObjectName = key,
        Metadata =
        {
            [PgTargetScorer.WaitFractionKey] = fraction,
            [PgTargetScorer.WaitMsKey] = fraction * 14_400_000,
            [PgTargetScorer.WaitCountKey] = 12_345,
            [PgTargetScorer.WaitSourceObservedMsKey] = 14_400_000,
            [PgTargetScorer.WaitSampleCountKey] = 239,
            [PgTargetScorer.WaitCollectionCountKey] = 240,
            [PgTargetScorer.WaitObservedMsKey] = 14_400_000,
            [PgTargetScorer.WaitWitnessFractionKey] = fraction,
            [PgTargetScorer.WaitShareOfWaitTimeKey] = 0.4,
            [PgTargetScorer.WaitIsStandoutKey] = 1,
            [PgTargetScorer.WaitRestartCollectionsKey] = 1,
        },
    };

    /// <summary>A type-rollup fact: <c>is_standout = 0</c> with the largest named-standout fraction of its type.</summary>
    private static Fact Rollup(string key, double fraction, double maxStandout)
    {
        var fact = Aurora(key, fraction);
        fact.Metadata[PgTargetScorer.WaitIsStandoutKey] = 0;
        fact.Metadata[PgTargetScorer.WaitMaxStandoutFractionKey] = maxStandout;
        fact.Metadata[PgTargetScorer.WaitEventsInTypeKey] = 3;
        return fact;
    }

    private static Fact Sampled(string key, double fraction, double deltaSamples)
    {
        var fact = Aurora(key, fraction);
        fact.Metadata.Remove(PgTargetScorer.WaitRestartCollectionsKey);
        fact.Metadata[PgTargetScorer.WaitIsSampledKey] = 1;
        fact.Metadata[PgTargetScorer.WaitEstimateResolutionMsKey] = 10;
        fact.Metadata[PgTargetScorer.WaitDeltaSamplesKey] = deltaSamples;
        fact.Metadata[PgTargetScorer.WaitCountKey] = deltaSamples;
        fact.Metadata[PgTargetScorer.WaitPeakBackendsKey] = 4;
        fact.Metadata[PgTargetScorer.WaitCounterResetsKey] = 0;
        return fact;
    }

    /// <summary>Lane 3's <c>PG_CONNECTION_SATURATION</c> fact at its shape: <c>saturation_ratio</c> is what the
    /// sessions scorer grades (0 under the 0.8 warning band, 0.5 at it, 1.0 at 0.9). <paramref name="ratio"/>
    /// 0.8 fires at exactly 0.5; 0.9 at 1.0 — the two ends the mesh pins use.</summary>
    private static Fact Saturation(bool fired, double ratio = 0.8) => new()
    {
        Source = PgTargetSources.SessionsSource,
        Key = PgTargetFactKeys.ConnectionSaturation,
        Value = fired ? ratio : 0.2,
        ServerId = 1,
        Metadata = { ["saturation_ratio"] = fired ? ratio : 0.2 },
    };

    private static Fact Pressure(double share) => new()
    {
        Source = PgTargetSources.WriteSource,
        Key = PgTargetFactKeys.CheckpointPressure,
        Value = share,
        ServerId = 1,
        Metadata =
        {
            ["checkpoints_requested"] = Math.Round(share * 240),
            ["checkpoints_timed"] = 240 - Math.Round(share * 240),
            ["checkpoints_total"] = 240,
            ["requested_share"] = share,
        },
    };

    private static Fact Buffer(double missShare) => new()
    {
        Source = PgTargetSources.BufferSource,
        Key = PgTargetFactKeys.BufferCachePressure,
        Value = missShare,
        ServerId = 1,
        Metadata =
        {
            ["miss_share"] = missShare,
            ["hit_ratio"] = 1 - missShare,
            ["block_requests_per_sec"] = 2000,
            ["blocks_total"] = 2000 * 14_400,
            ["hit_ratio_suppressed"] = 0,
            ["evictions_tracked"] = 0,
            ["bgwriter_tracked"] = 0,
        },
    };

    private static Fact Knob(string key, double value) =>
        new() { Source = PgTargetSources.ConfigSource, Key = key, Value = value, ServerId = 1 };

    private static IReadOnlyDictionary<string, Fact> Lookup(params Fact[] facts) => facts.ToFactLookup();
}
