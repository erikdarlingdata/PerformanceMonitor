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
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The between-waves shared-file batch of #3691 (v2 → wave 3) — the items the content lanes (11, 12, 14, 15)
/// reported as outside their file-disjoint boundary, decided and made here as one PR: the two unfloored baseline
/// sources, the I/O baseline's quarter-hour grain, the WAL co-fire that read a base-0 context fact, the Lock →
/// idle-in-transaction blocking leaf, the idle fact's next reads, the wave-3 blocking family's stubs, the
/// "every declared key is routed" census, three stale docs and the <c>compare_analysis</c> description. Each pin
/// here is the property an item's decision rests on; the families' own tests keep their pins, and the ones this
/// pass moved (<c>PgTargetIoTests</c>' hour-of-day collapse, <c>PgTargetWaitTests</c>' Single Lock edges,
/// <c>PgTargetReplicationTests</c>' shift edge, the census counts) say so inline.
/// </summary>
public sealed class PgTargetBetweenWavesV2Tests
{
    private const long MiB = 1024 * 1024;

    /* ───────────────────────── item 1: the floors ───────────────────────── */

    /// <summary>The two tables lanes 11 and 15 reported unfloored are floored, and the floor is the detectors' own
    /// minimum-history gate — the same constant, so a retention shortened under it starves nothing silently.
    /// (<c>BaselineSupplyTests</c> derives the whole set from the provider's SQL; this pins the two by name.)</summary>
    [Fact]
    public void TheIoAndWalBaselineTables_AreFlooredAtTheBaselineWindow()
    {
        Assert.Contains("pg_io_stats", DarlingRetentionHorizons.BaselineServingRawCollectors);
        Assert.Contains("pg_write_stats", DarlingRetentionHorizons.BaselineServingRawCollectors);
        Assert.Contains("pg_replication_stats", DarlingRetentionHorizons.BaselineServingRawCollectors);
        /* Lane 17's pg_blocked_sessions arm reads pg_blocking_edges: its collector, pg_blocking, joined (9 → 10). */
        Assert.Contains("pg_blocking", DarlingRetentionHorizons.BaselineServingRawCollectors);
        /* Lane 24's pg_sampled_wait_ms_per_sec arm reads pg_wait_sampling directly: it joined (10 → 11). */
        Assert.Contains("pg_wait_sampling", DarlingRetentionHorizons.BaselineServingRawCollectors);
        Assert.Equal(11, DarlingRetentionHorizons.BaselineServingRawCollectors.Count);
        /* The doc the floor falsified is gone from the provider's root: it no longer says the PostgreSQL tables lack one. */
        var provider = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetBaselineProvider.cs");
        Assert.DoesNotContain("out of this lane's files", provider, StringComparison.Ordinal);
        Assert.DoesNotContain("need the same floor", provider, StringComparison.Ordinal);
        Assert.Contains("pg_write_stats</c>, lane 15", provider, StringComparison.Ordinal);
    }

    /* ───────────────────────── item 2: the quarter-hour grain ───────────────────────── */

    /// <summary>The grain and the floor are ONE decision, made in three places that must agree: the baseline arm and
    /// the detector read bucket on the same expression and rate under the same literal, and the literal is the scorer's
    /// constant — a quarter of the hourly floor the collector still applies to its window total.</summary>
    [Fact]
    public void TheIoBaselineAndDetector_ShareTheQuarterHourGrainAndTheReadsFloor_WhichIsAQuarterOfTheHourly()
    {
        var baseline = PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgIoReadLatency)!;
        var window = PgTargetAnomalyDetector.IoLatencyWindowSql;
        const string bucket = "date_bin('15 minutes', collection_time, TIMESTAMP '2000-01-01')";
        Assert.Equal(2, Regex.Matches(baseline, Regex.Escape(bucket)).Count);   /* SELECT and GROUP BY */
        Assert.Equal(2, Regex.Matches(window, Regex.Escape(bucket)).Count);
        Assert.DoesNotContain("date_trunc('hour'", baseline, StringComparison.Ordinal);
        Assert.DoesNotContain("date_trunc('hour'", window, StringComparison.Ordinal);
        Assert.Equal(250.0, PgTargetScorer.IoBaselineBucketMinimumReads);
        Assert.Equal(PgTargetScorer.IoMinimumOps / 4.0, PgTargetScorer.IoBaselineBucketMinimumReads);
        Assert.Single(Regex.Matches(baseline, @"reads >= 250 /\* PgTargetScorer\.IoBaselineBucketMinimumReads"));
        Assert.Single(Regex.Matches(window, @"reads >= 250 /\* PgTargetScorer\.IoBaselineBucketMinimumReads"));
        /* The collector's window read never carried a per-sample floor and still does not: the 1,000 is applied in C#. */
        Assert.DoesNotContain("reads >= ", PgTargetFactCollector.PgTargetIoLatencySql, StringComparison.Ordinal);
        /* Four samples an hour over the window's 30 days clears the restore threshold in every hour-of-week bucket. */
        Assert.True(4 * (BaselineMath.BaselineWindowDays / 7) >= BaselineMath.RestoreThreshold, "the quarter-hour grain would not restore hour-of-week");
    }

    /* ───────────────────────── item 3: the WAL co-fire reads the anomaly ───────────────────────── */

    [Fact]
    public void TheReplicationWalAmplifiers_ReadTheAnomaly_NotTheContextFact_AndTheDeadEdgeIsGone()
    {
        /* The context fact alone, with large numbers: neither WAL amplifier matches, on either replication fact. */
        var slotAlone = Slot(12L * 1024 * MiB, growth: 500 * MiB);
        var lagAlone = Lag(200 * MiB);
        var shift = new Fact { Source = PgTargetSources.WriteSource, Key = PgTargetFactKeys.WalVolumeShift, Value = 90 * MiB, ServerId = 1, Metadata = { ["mean_wal_bytes_per_sec"] = 50 * MiB, ["peak_wal_bytes_per_sec"] = 90 * MiB } };
        new FactScorer().ScoreAll([slotAlone, lagAlone, shift]);
        Assert.Equal(0.0, shift.BaseSeverity);
        var slotWal = Assert.Single(slotAlone.AmplifierResults, a => a.Description.StartsWith("ANOMALY_PG_WAL_VOLUME", StringComparison.Ordinal));
        Assert.False(slotWal.Matched);
        var lagWal = Assert.Single(lagAlone.AmplifierResults, a => a.Description.StartsWith("ANOMALY_PG_WAL_VOLUME", StringComparison.Ordinal));
        Assert.False(lagWal.Matched);
        Assert.DoesNotContain(slotAlone.AmplifierResults, a => a.Description.Contains("PG_WAL_VOLUME_SHIFT", StringComparison.Ordinal));
        Assert.DoesNotContain(lagAlone.AmplifierResults, a => a.Description.Contains("PG_WAL_VOLUME_SHIFT", StringComparison.Ordinal));

        /* The anomaly fired (7σ, trusted: base 1.0): both match at their 0.2 boost, and the slot's severity says so. */
        var slot = Slot(12L * 1024 * MiB, growth: 500 * MiB);
        var lag = Lag(200 * MiB);
        var anomaly = FiredWalAnomaly(7.0);
        new FactScorer().ScoreAll([slot, lag, anomaly]);
        Assert.Equal(1.0, anomaly.BaseSeverity, precision: 9);
        var slotLift = Assert.Single(slot.AmplifierResults, a => a.Description.StartsWith("ANOMALY_PG_WAL_VOLUME", StringComparison.Ordinal));
        Assert.True(slotLift.Matched);
        Assert.Equal(0.2, slotLift.Boost, precision: 9);
        Assert.True(Assert.Single(lag.AmplifierResults, a => a.Description.StartsWith("ANOMALY_PG_WAL_VOLUME", StringComparison.Ordinal)).Matched);
        Assert.True(slot.Severity > slotAlone.Severity, $"{slot.Severity} vs {slotAlone.Severity}");

        /* The advice states the co-fire from the same verdict — never from the context fact. */
        var lookup = new[] { slot, lag, anomaly }.ToFactLookup();
        Assert.Contains("ANOMALY_PG_WAL_VOLUME co-fired", PgTargetAdvice.Compose(PgTargetFactKeys.SlotRetention, lookup)!.Investigation, StringComparison.Ordinal);
        Assert.Contains("ANOMALY_PG_WAL_VOLUME co-fired", PgTargetAdvice.Compose(PgTargetFactKeys.ReplicationLag, lookup)!.Investigation, StringComparison.Ordinal);
        var withShiftOnly = new[] { slotAlone, lagAlone, shift }.ToFactLookup();
        Assert.DoesNotContain("co-fired: the primary is writing more WAL", PgTargetAdvice.Compose(PgTargetFactKeys.SlotRetention, withShiftOnly)!.Investigation, StringComparison.Ordinal);

        /* The graph: nothing leaves the shift, nothing leaves the anomaly into replication — the co-fire is the amplifier. */
        var graph = new PgTargetRelationshipGraph();
        Assert.Empty(graph.GetAllEdges(PgTargetFactKeys.WalVolumeShift));
        Assert.Empty(graph.GetAllEdges(PgTargetFactKeys.AnomalyWalVolume));
        var source = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetRelationshipGraph.Replication.cs"));
        Assert.DoesNotContain("PgTargetFactKeys.WalVolumeShift", source, StringComparison.Ordinal);
        var scorer = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetScorer.Replication.cs"));
        Assert.DoesNotContain("PgTargetFactKeys.WalVolumeShift", scorer, StringComparison.Ordinal);
        Assert.Equal(2, Count(scorer, "PgTargetFactKeys.AnomalyWalVolume"));
        /* The key's own doc no longer promises an edge. */
        Assert.DoesNotContain("declared for the write-chain edge", RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetFactKeys.cs"), StringComparison.Ordinal);
    }

    /* ───────────────────────── item 4: Lock → idle, the blocking leaf ───────────────────────── */

    [Fact]
    public void ALockWait_WalksToTheIdleHolder_WhenOneFired_AndKeepsItsSingleSaturationEdgeOtherwise()
    {
        var graph = new PgTargetRelationshipGraph();
        var lockRelation = PgTargetFactKeys.WaitKey("Lock", "relation");
        var lockRollup = PgTargetFactKeys.WaitKey("Lock", null);

        /* Both Lock keys carry the new edge, and it opens on the idle fact's BASE severity only. */
        foreach (var key in new[] { lockRelation, lockRollup })
        {
            var edge = Assert.Single(graph.GetAllEdges(key), e => e.Destination == PgTargetFactKeys.IdleInTransaction);
            Assert.Equal("connection_saturation", edge.Category);
            Assert.Contains("parked transaction", edge.PredicateDescription, StringComparison.Ordinal);
        }

        /* Lock wait outranks (0.6 on the standout ramp = 0.765, × 1.3 for the idle co-fire = 0.994) a two-minute
           holder (0.556 on the duration ramp, nothing lifting it): the wait roots and walks to the parked transaction
           — one story, the blocking leaf reached from the symptom side. */
        var waitLeads = new List<Fact> { LockWait(lockRelation, 0.6), Parked(120_000) };
        new FactScorer().ScoreAll(waitLeads);
        Assert.True(waitLeads[0].Severity > waitLeads[1].Severity, $"{waitLeads[0].Severity} {waitLeads[1].Severity}");
        var story = Assert.Single(new InferenceEngine(graph).BuildStories(waitLeads));
        Assert.Equal(new[] { lockRelation, PgTargetFactKeys.IdleInTransaction }, story.Path);
        Assert.Equal(PgTargetSources.WaitsSource, story.Category);

        /* The holder outranks (15 min, pinning the horizon, recurring: 1.0 × 1.2) a Lock wait at 0.2 (0.529 × 1.3 =
           0.688): it roots and walks to the wait — lane 14's edge, still one story the other way round. */
        var holderLeads = new List<Fact> { LockWait(lockRelation, 0.2), Parked(900_000, horizonAge: 5_000_000, recurring: 6) };
        new FactScorer().ScoreAll(holderLeads);
        Assert.True(holderLeads[1].Severity > holderLeads[0].Severity, $"{holderLeads[1].Severity} {holderLeads[0].Severity}");
        var reversed = Assert.Single(new InferenceEngine(graph).BuildStories(holderLeads));
        Assert.Equal(new[] { PgTargetFactKeys.IdleInTransaction, lockRelation }, reversed.Path);

        /* No idle fact: the wait's active edges are exactly lane 5's — saturation alone — and its story is byte-identical. */
        var saturationOnly = new List<Fact> { LockWait(lockRelation, 0.6), Saturation(fired: true) };
        new FactScorer().ScoreAll(saturationOnly);
        var active = Assert.Single(graph.GetActiveEdges(lockRelation, saturationOnly.ToFactLookup()));
        Assert.Equal(PgTargetFactKeys.ConnectionSaturation, active.Destination);
        Assert.Equal(new[] { lockRelation, PgTargetFactKeys.ConnectionSaturation }, Assert.Single(new InferenceEngine(graph).BuildStories(saturationOnly)).Path);

        /* An idle fact UNDER its floor (base 0) opens nothing, however large its Severity is made. */
        var underFloor = new List<Fact> { LockWait(lockRelation, 0.6), Parked(59_999) };
        new FactScorer().ScoreAll(underFloor);
        Assert.Equal(0.0, underFloor[1].BaseSeverity);
        underFloor[1].Severity = 0.9;
        Assert.Empty(graph.GetActiveEdges(lockRelation, underFloor.ToFactLookup()));

        /* Every Lock-wait edge lives in the Saturation partial (one source node's edges in one place); the blocking
           stub declares none from a Lock wait. */
        /* Raw source, not the stripped walk: the anchors are the AddEdge calls WITH their string arguments, which no comment spells. */
        var saturation = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetRelationshipGraph.Saturation.cs");
        /* Three edges INTO the idle fact in this category: saturation's (lane 14) and the two Lock waits' (this batch). */
        Assert.Equal(3, Count(saturation, "PgTargetFactKeys.IdleInTransaction, \"connection_saturation\""));
        /* Three per Lock key since lane 17: saturation, the idle leaf, and the sampled chain (moved from two, deliberately;
           the blocking file still declares none FROM a Lock wait — PgTargetBlockingTests pins that). */
        Assert.Equal(3, Count(saturation, "AddEdge(PgTargetFactKeys.WaitKey(\"Lock\", \"relation\")"));
        Assert.Equal(3, Count(saturation, "AddEdge(PgTargetFactKeys.WaitKey(\"Lock\", null)"));
    }

    /* ───────────────────────── item 5: the idle fact's next reads ───────────────────────── */

    [Fact]
    public void TheIdleInTransactionNextReads_LeadWithTheSessionRead_ThenTheHorizonAndTheChain()
    {
        var reads = PgTargetToolRecommendations.GetForKey(PgTargetFactKeys.IdleInTransaction)!;
        Assert.Equal(new[] { "get_pg_session_states", "get_pg_xmin_horizon", "get_pg_blocking" }, reads.Select(r => r.Tool));
        /* The two added reads are the two chains the idle advice names by tool. */
        var advice = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetAdvice.Sessions.cs");
        Assert.Contains("get_pg_xmin_horizon", advice, StringComparison.Ordinal);
        Assert.Contains("get_pg_blocking", advice, StringComparison.Ordinal);
    }

    /* ───────────────────────── item 6: the wave-3 blocking stubs ───────────────────────── */

    /// <summary>
    /// Item 6 as shipped pinned the wave-3 blocking family INERT through every shared entry point; lane 17 filled it,
    /// so this pin moved deliberately to the filled shape (the vocabulary, the routing, the marker in six files) and
    /// the family's own behaviour is <c>PgTargetBlockingTests</c>'. What stays pinned here is what the between-waves
    /// batch decided: the names, the fold, the deviation-scored membership, the tool rows, the marker, and that the
    /// anomaly's ComposeAnomaly arm delegates to the family file (never the SQL Server "Anomalous spike" composer).
    /// </summary>
    [Fact]
    public void TheBlockingFamily_IsRoutedThroughEverySharedEntryPoint_AndKeepsLane17sMarker()
    {
        Assert.Equal("pg_blocking", PgTargetSources.BlockingSource);
        Assert.Contains(PgTargetSources.BlockingSource, PgTargetSources.All);
        Assert.Contains(PgTargetSources.BlockingSource, FactScorer.KnownSources);
        Assert.Equal("PG_BLOCKING_CHAIN", PgTargetFactKeys.BlockingChain);
        Assert.Equal("PG_LOCK_WAIT_EVENTS", PgTargetFactKeys.LockWaitEvents);
        Assert.Equal("PG_LONG_RUNNING_QUERY", PgTargetFactKeys.LongRunningQuery);
        Assert.Equal("ANOMALY_PG_BLOCKING", PgTargetFactKeys.AnomalyBlocking);
        Assert.Equal("pg_blocked_sessions", MetricNames.PgBlockedSessions);
        Assert.Equal(new[] { PgTargetFactKeys.BlockingChain }, PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyBlocking]);
        Assert.True(PgTargetScorer.IsDeviationScoredAnomalyKey(PgTargetFactKeys.AnomalyBlocking));
        Assert.False(PgTargetScorer.IsPgRatioAnomalyKey(PgTargetFactKeys.AnomalyBlocking));

        /* Filled (lane 17): a bare fact with no graded metadata still scores 0 — the family self-gates on its own keys —
           but every shared entry point now answers the family's block, edges and baseline rather than null. */
        var facts = new[] { PgTargetFactKeys.BlockingChain, PgTargetFactKeys.LockWaitEvents, PgTargetFactKeys.LongRunningQuery }
            .Select(k => new Fact { Source = PgTargetSources.BlockingSource, Key = k, Value = 42, ServerId = 1 }).ToList();
        new FactScorer().ScoreAll(facts);
        var graph = new PgTargetRelationshipGraph();
        foreach (var fact in facts)
        {
            Assert.Equal(0.0, fact.BaseSeverity);
            Assert.NotNull(PgTargetAdvice.Compose(fact.Key, facts.ToFactLookup()));
            Assert.NotNull(FactAdvice.GetForFactKey(fact.Key));
            Assert.NotEmpty(PgTargetToolRecommendations.GetForKey(fact.Key)!);
        }
        Assert.NotEmpty(graph.GetAllEdges(PgTargetFactKeys.BlockingChain));
        Assert.NotNull(FactAdvice.GetForFactKey(PgTargetFactKeys.AnomalyBlocking));
        Assert.Single(graph.GetAllEdges(PgTargetFactKeys.AnomalyBlocking));
        Assert.NotNull(PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgBlockedSessions));
        Assert.Null(PgBaselineProvider.GetBaselineQuery(MetricNames.PgBlockedSessions));
        Assert.Equal(new[] { "get_pg_blocking", "get_pg_lock_stats", "get_pg_log_events" }, PgTargetToolRecommendations.GetForKey(PgTargetFactKeys.AnomalyBlocking)!.Select(r => r.Tool));

        /* Six family files, each still carrying the exact marker lane 17's brief quoted (a filled family keeps it, as v1's did). */
        foreach (var (dir, file) in new[]
        {
            ("PerformanceMonitor.Analysis", "PgTargetScorer.Blocking.cs"), ("PerformanceMonitor.Analysis", "PgTargetAdvice.Blocking.cs"), ("PerformanceMonitor.Analysis", "PgTargetRelationshipGraph.Blocking.cs"),
            ("Darling/PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Blocking.cs"), ("Darling/PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.Blocking.cs"), ("Darling/PerformanceMonitor.Darling.Analysis", "PgTargetBaselineProvider.Blocking.cs"),
        })
        {
            var text = RepoFile.ReadRepoFile(dir.Split('/').Append(file).ToArray());
            Assert.Contains("/* filled by lane 17", text, StringComparison.Ordinal);
            Assert.DoesNotContain("aurora_stat_", text, StringComparison.Ordinal);
        }

        /* The ComposeAnomaly hook is plumbing's, so lane 17 never edited the anomaly partial (lanes 12 and 15 each had to). */
        var anomalyAdvice = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetAdvice.Anomaly.cs"));
        Assert.Contains("case PgTargetFactKeys.AnomalyBlocking:", anomalyAdvice, StringComparison.Ordinal);
        Assert.Contains("return ComposeBlockingAnomaly(factsByKey);", anomalyAdvice, StringComparison.Ordinal);
    }

    /* ───────────────────────── item 9: compare_analysis says which keys are baselined, on both engines ───────────────────────── */

    [Fact]
    public void TheCompareAnalysisDescription_NamesThePostgresBaselinedKeys_OnBothSkus_AndTheBandRuleIsEngineNeutral()
    {
        static string Description(string text)
        {
            var m = Regex.Match(text, @"McpServerTool\(Name = ""compare_analysis""\), Description\(""((?:[^""\\]|\\.)*)""\)\]");
            Assert.True(m.Success, "the compare_analysis attribute has moved");
            return m.Groups[1].Value;
        }
        var darling = Description(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpTools.cs"));
        var lite = Description(RepoFile.ReadRepoFile("Lite", "Mcp", "McpAnalysisTools.cs"));
        Assert.Equal(darling, lite);
        Assert.Contains("(SQL Server: CPU %, read latency, connections; PostgreSQL: transactions/sec, session count, deadlocks/hour, CPU as % of capacity, read latency, replay lag bytes, WAL bytes/sec)", darling, StringComparison.Ordinal);
        /* The seven named are exactly the seven ComparisonBanding maps to a pg_ metric (#3713's seam, extended by the
           v2 exit-check residue for the I/O, replication and WAL buckets). */
        var mapped = new[] { PgTargetFactKeys.Tps, PgTargetFactKeys.ConnectionSaturation, PgTargetFactKeys.DeadlockRate, PgTargetFactKeys.CpuPercent, PgTargetFactKeys.IoReadLatencyMs, PgTargetFactKeys.ReplicationLag, PgTargetFactKeys.WalVolumeShift };
        foreach (var key in mapped)
            Assert.StartsWith("pg_", ComparisonBanding.BaselinedMetricFor(key)!, StringComparison.Ordinal);
        Assert.Equal(7, typeof(PgTargetFactKeys).GetFields().Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!).Count(k => ComparisonBanding.BaselinedMetricFor(k) is not null));

        using var doc = System.Text.Json.JsonDocument.Parse(System.Text.Json.JsonSerializer.Serialize(ComparisonBanding.BandRulesPayload));
        var rule = doc.RootElement.GetProperty("baseline").GetString()!;
        Assert.StartsWith("delta_sigma is the reading's delta in robust-sigma units", rule, StringComparison.Ordinal);
        Assert.DoesNotContain("the value delta", rule, StringComparison.Ordinal);
    }

    /* ───────────────────────── builders ───────────────────────── */

    private static Fact Slot(long retained, long growth) => new()
    {
        Source = PgTargetSources.ReplicationSource,
        Key = PgTargetFactKeys.SlotRetention,
        Value = retained,
        ServerId = 1,
        ObjectName = "cdc_slot",
        Metadata =
        {
            [PgTargetScorer.SlotRetainedBytesKey] = retained,
            [PgTargetScorer.SlotFirstRetainedBytesKey] = retained - growth,
            [PgTargetScorer.SlotGrowthBytesKey] = growth,
            [PgTargetScorer.SlotActiveKey] = 0,
            [PgTargetScorer.SlotWalStatusKey] = PgTargetScorer.WalStatusCode("reserved"),
            [PgTargetScorer.SlotKeepSizeSetKey] = 0,
            [PgTargetScorer.SlotLogicalKey] = 1,
            [PgTargetScorer.SlotInactiveSinceKnownKey] = 0,
            [PgTargetScorer.SlotsInWindowKey] = 2,
            [PgTargetScorer.SlotsGradedKey] = 1,
            [PgTargetScorer.SlotSamplesKey] = 49,
            [PgTargetScorer.SlotSpanHoursKey] = 4,
        },
    };

    /// <summary>Lane 12's drifting replay lag on a named async standby (<c>PgTargetReplicationTests.Lag</c> at its
    /// fired shape: 200 MiB peak, 51 MiB → 157 MiB across the halves).</summary>
    private static Fact Lag(double peak) => new()
    {
        Source = PgTargetSources.ReplicationSource,
        Key = PgTargetFactKeys.ReplicationLag,
        Value = peak,
        ServerId = 1,
        ObjectName = "replica-a",
        Metadata =
        {
            [PgTargetScorer.LagPeakBytesKey] = peak,
            [PgTargetScorer.LagLatestBytesKey] = peak,
            [PgTargetScorer.LagStageKey] = PgTargetScorer.LagStageReplay,
            [PgTargetScorer.LagStageBytesKey] = peak,
            [PgTargetScorer.LagSyncStateKey] = PgTargetScorer.SyncStateAsync,
            [PgTargetScorer.LagStandbyStreamingKey] = 1,
            [PgTargetScorer.LagMsReportedKey] = 0,
            [PgTargetScorer.LagSamplesKey] = 49,
            [PgTargetScorer.LagCollectionsKey] = 49,
            [PgTargetScorer.LagStandbysKey] = 1,
            [PgTargetScorer.LagSpanHoursKey] = 4,
            [PgTargetScorer.LagSlotsObservedKey] = 2,
            [PgTargetScorer.LagDriftComputableKey] = 1,
            [PgTargetScorer.LagFirstHalfMeanBytesKey] = peak * 0.25,
            [PgTargetScorer.LagSecondHalfMeanBytesKey] = peak * 0.75,
        },
    };

    private static Fact FiredWalAnomaly(double sigma)
    {
        var fact = new Fact { Source = PgTargetAnomalyDetector.AnomalySource, Key = PgTargetFactKeys.AnomalyWalVolume, Value = 12 * MiB, ServerId = 1 };
        fact.Metadata["baseline_mean"] = 1.1 * MiB;
        fact.Metadata["baseline_stddev"] = 0.1 * MiB;
        fact.Metadata["deviation_sigma"] = sigma;
        fact.Metadata["fire_threshold"] = AnomalyThresholds.ModifiedZThreshold;
        fact.Metadata["baseline_low_quality"] = 0;
        fact.Metadata["fallback_exceedance"] = 0;
        fact.Metadata["baseline_samples"] = 120;
        fact.Metadata["window_samples"] = 240;
        fact.Metadata["threshold_lineage"] = 0;
        fact.Metadata["baseline_tier"] = (double)BaselineTier.Full;
        fact.Metadata["baseline_median"] = 1.05 * MiB;
        fact.Metadata["baseline_mad"] = 0.1 * MiB;
        fact.Metadata["confidence"] = 1.0;
        fact.Metadata["peak_wal_bytes_per_sec"] = 12 * MiB;
        fact.Metadata["avg_wal_bytes_per_sec"] = 9.6 * MiB;
        return fact;
    }

    /// <summary>Lane 5's Aurora standout wait fact (<c>PgTargetWaitTests.Aurora</c>), at the given fraction of observed time.</summary>
    private static Fact LockWait(string key, double fraction) => new()
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

    private static Fact Saturation(bool fired, double ratio = 0.8) => new()
    {
        Source = PgTargetSources.SessionsSource,
        Key = PgTargetFactKeys.ConnectionSaturation,
        Value = fired ? ratio : 0.2,
        ServerId = 1,
        Metadata = { ["saturation_ratio"] = fired ? ratio : 0.2 },
    };

    /// <summary>Lane 14's idle-in-transaction fact (<c>PgTargetSessionsTests.Parked</c>): one holder, its longest
    /// transaction in ms, its horizon claim (-1 pins nothing), the recurrence.</summary>
    private static Fact Parked(double heldMs, double horizonAge = -1, double recurring = 1) => new()
    {
        Source = PgTargetSources.SessionsSource,
        Key = PgTargetFactKeys.IdleInTransaction,
        Value = heldMs / 1_000.0,
        ServerId = 1,
        DatabaseName = "appdb",
        ObjectName = "billing-worker as billing",
        Metadata =
        {
            [PgTargetScorer.IdleInTransactionDurationMsKey] = heldMs,
            [PgTargetScorer.IdleInTransactionHolderHorizonAgeKey] = horizonAge,
            ["holder_is_horizon_holder"] = horizonAge > 0 ? 1 : 0,
            ["holder_captures_seen"] = recurring,
            [PgTargetScorer.IdleInTransactionRecurringCapturesKey] = recurring,
            ["holder_identities"] = 1,
            ["holder_identities_pinning_horizon"] = horizonAge > 0 ? 1 : 0,
            ["captures_with_holders"] = recurring,
            ["captures_with_rows"] = 48,
            ["holder_rows"] = recurring,
            ["peak_concurrent_holders"] = 1,
            ["peak_age_s"] = 1_800,
            ["holder_last_seen_age_s"] = 300,
            ["floor_ms"] = PgTargetScorer.IdleInTransactionWarningMs,
            ["rows_redacted_share"] = 0,
        },
    };

    private static int Count(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0; i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
            count++;
        return count;
    }
}
