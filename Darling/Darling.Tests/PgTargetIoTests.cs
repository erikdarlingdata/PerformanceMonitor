/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
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
/// Lane 11 of #3691 — the I/O family (design §3.9 / §2a): <c>PG_IO_READ_LATENCY_MS</c>, <c>PG_IO_WRITE_LATENCY_MS</c>,
/// <c>ANOMALY_PG_IO_LATENCY</c>, their <c>pg_io_read_latency</c> baseline, the I/O chain and the advice — the
/// scorer and amplifiers through the shared <see cref="FactScorer.ScoreAll"/>, the graph through the real
/// <see cref="PgTargetRelationshipGraph"/>, the three reads by their own text, and — gated on <c>DARLING_TEST_PG</c>
/// — the exit criterion through the REAL <c>analyze_server</c> against 31 planted days of <c>pg_io_stats</c> at
/// ~1.5 ms per read with the last four hours at 25 ms.
///
/// <para><b>The pins that matter most.</b> The trackedness three-way: <c>track_io_timing</c> off is
/// <c>unavailable</c> with its reason and NEVER a 0.000 ms latency (the #3541 A8 lesson), a pre-16 major is
/// <c>unavailable</c> for a different reason, and a window under the operations floor is stated and not graded.
/// The bars are MEASURED (§B1) and the facts carry <c>threshold_lineage = 1</c> with the population named.
/// The anomaly's floor / fallback sit inside the regular fact's bars in the stated order. The literal operations
/// floor in the two hourly SQL texts equals the scorer's constant.</para>
///
/// <para><b>Every number asserted here was executed on this machine</b> through a net10.0 harness over the built
/// assemblies, and the SQL against a throwaway PostgreSQL 18 / TimescaleDB store, before the first CI run.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetIoTests
{
    private const string ServerName = "darling-pg-target-io-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string OldServerName = "darling-pg-target-io-e2e-pg15";
    private static readonly int OldServerId = ServerIdHelper.GetDeterministicHashCode(OldServerName);
    private const string UntimedServerName = "darling-pg-target-io-e2e-untimed";
    private static readonly int UntimedServerId = ServerIdHelper.GetDeterministicHashCode(UntimedServerName);

    /* ───────────────────────── scorer: the read fact ───────────────────────── */

    [Theory]
    [InlineData(9.99, 0.0)]                 // one hundredth under the warning bar: 0, never the formula's 0.4995
    [InlineData(10.0, 0.5)]                 // at the measured warning bar
    [InlineData(20.0, 0.75)]                // halfway
    [InlineData(25.0, 0.875)]               // the live test's planted value
    [InlineData(30.0, 1.0)]                 // at the measured critical bar
    [InlineData(45.0, 1.0)]
    public void ReadLatency_IsZeroBelowTheWarningBar_AndTheSharedFormulaFromIt_StampingAMeasuredLineage(double msPerRead, double expected)
    {
        var fact = ReadFact(msPerRead, ops: 26_400);
        new FactScorer().ScoreAll([fact]);
        Assert.Equal(expected, fact.BaseSeverity, precision: 9);
        Assert.Equal(1, fact.Metadata["threshold_lineage"]);
    }

    [Fact]
    public void ReadLatency_UnderTheOperationsFloor_IsStatedNotGraded_AndStillCarriesTheMeasuredLineage()
    {
        var fact = ReadFact(45.0, ops: 999);
        Assert.Equal(1, fact.Metadata[PgTargetScorer.IoInsufficientOpsKey]);
        new FactScorer().ScoreAll([fact]);
        Assert.Equal(0.0, fact.BaseSeverity);
        Assert.Equal(1, fact.Metadata["threshold_lineage"]);      // the floor is a measured bar and it decided
    }

    [Fact]
    public void ReadLatency_Unavailable_ForEitherReason_IsContextWithNoLineageStamp_AndNeverAValue()
    {
        var untimed = UnavailableReadFact(PgTargetScorer.IoReasonTrackIoTimingOffKey, ops: 50_000);
        var absent = UnavailableReadFact(PgTargetScorer.IoReasonPgStatIoAbsentKey, ops: 0);
        new FactScorer().ScoreAll([untimed, absent]);
        foreach (var fact in new[] { untimed, absent })
        {
            Assert.Equal(0.0, fact.BaseSeverity);
            Assert.Equal(0.0, fact.Value);
            Assert.Equal(0, fact.Metadata[PgTargetScorer.IoLatencyMeasuredKey]);
            Assert.False(fact.Metadata.ContainsKey("threshold_lineage"), "no bar was consulted, so no lineage is claimed");
        }
    }

    [Fact]
    public void WriteLatency_IsNeverGraded_WhateverItCarries_AndHasNoAmplifiers()
    {
        var write = WriteFact(45.0, ops: 100_000);
        var anomaly = IoAnomaly(sigma: 10.5);
        var pressure = BufferPressure(fired: true);
        var facts = new List<Fact> { write, anomaly, pressure };
        new FactScorer().ScoreAll(facts);
        Assert.Equal(0.0, write.BaseSeverity);
        Assert.Equal(0.0, write.Severity);
        Assert.False(write.Metadata.ContainsKey("threshold_lineage"));
        Assert.Empty(write.AmplifierResults);
    }

    /* ───────────────────────── amplifiers ───────────────────────── */

    /// <summary>The three corroborators, each 0.3, through the shipped arithmetic: a warning-line read (0.5) with the
    /// anomaly alone is 0.65; with all three 0.95; a critical read (1.0) with all three 1.9 — the parent is never
    /// capped. An IO wait counts whether it is the type rollup or a named standout; a Lock wait does not.</summary>
    [Fact]
    public void ReadLatency_IsLiftedByTheAnomaly_AnIoWait_AndCachePressure_EachByTheStatedBoost()
    {
        var alone = ReadFact(10.0, ops: 26_400);
        new FactScorer().ScoreAll([alone]);
        Assert.Equal(0.5, alone.Severity, precision: 9);
        Assert.Equal(3, alone.AmplifierResults.Count);
        Assert.Equal(0, alone.AmplifierResults.Count(r => r.Matched));

        var withAnomaly = ReadFact(10.0, ops: 26_400);
        new FactScorer().ScoreAll([withAnomaly, IoAnomaly(sigma: 10.5)]);
        Assert.Equal(0.65, withAnomaly.Severity, precision: 9);

        var withRollup = ReadFact(10.0, ops: 26_400);
        new FactScorer().ScoreAll([withRollup, Wait(PgTargetFactKeys.WaitKey("IO", null), fired: true)]);
        Assert.Equal(0.65, withRollup.Severity, precision: 9);

        var withStandout = ReadFact(10.0, ops: 26_400);
        new FactScorer().ScoreAll([withStandout, Wait(PgTargetFactKeys.WaitKey("IO", "DataFileRead"), fired: true)]);
        Assert.Equal(0.65, withStandout.Severity, precision: 9);

        var withLock = ReadFact(10.0, ops: 26_400);
        new FactScorer().ScoreAll([withLock, Wait(PgTargetFactKeys.WaitKey("Lock", "relation"), fired: true), Wait(PgTargetFactKeys.WaitKey("IO", "DataFileRead"), fired: false)]);
        Assert.Equal(0.5, withLock.Severity, precision: 9);

        var all = ReadFact(10.0, ops: 26_400);
        new FactScorer().ScoreAll([all, IoAnomaly(sigma: 10.5), Wait(PgTargetFactKeys.WaitKey("IO", "DataFileRead"), fired: true), BufferPressure(fired: true)]);
        Assert.Equal(0.95, all.Severity, precision: 9);

        var critical = ReadFact(30.0, ops: 26_400);
        new FactScorer().ScoreAll([critical, IoAnomaly(sigma: 10.5), Wait(PgTargetFactKeys.WaitKey("IO", null), fired: true), BufferPressure(fired: true)]);
        Assert.Equal(1.9, critical.Severity, precision: 9);
        Assert.True(critical.Severity > 1.5, "a corroborated critical read latency must reach the notify floor");
    }

    /// <summary>The anomaly itself: graded by the shared deviation ramp (registered deviation-scored by the plumbing),
    /// no amplifier arm of its own — it folds into the read fact, where the impact lives — and a low-quality bucket
    /// grades the exceedance of the measured fallback bar.</summary>
    [Fact]
    public void TheAnomaly_TakesTheDeviationRamp_HasNoAmplifiersOfItsOwn_AndFoldsOntoTheReadFact()
    {
        Assert.True(PgTargetScorer.IsDeviationScoredAnomalyKey(PgTargetFactKeys.AnomalyIoLatency));
        Assert.Equal(new[] { PgTargetFactKeys.IoReadLatencyMs }, PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyIoLatency]);

        var saturated = IoAnomaly(sigma: 10.5);
        new FactScorer().ScoreAll([saturated, ReadFact(30.0, ops: 26_400), Wait(PgTargetFactKeys.WaitKey("IO", null), fired: true), BufferPressure(fired: true)]);
        Assert.Equal(1.0, saturated.BaseSeverity, precision: 9);
        Assert.Equal(1.0, saturated.Severity, precision: 9);
        Assert.Empty(saturated.AmplifierResults);

        var atCutoff = IoAnomaly(sigma: 3.5);
        new FactScorer().ScoreAll([atCutoff]);
        Assert.Equal(0.5, atCutoff.BaseSeverity, precision: 9);

        var young = IoAnomaly(sigma: 0.2, lowQuality: true, fallbackExceedance: 1.5);
        new FactScorer().ScoreAll([young]);
        Assert.True(young.BaseSeverity > 0.5 && young.BaseSeverity < 1.0, $"the exceedance ramp should sit between the bar and saturation, was {young.BaseSeverity}");
    }

    /* ───────────────────────── the constants ───────────────────────── */

    [Fact]
    public void TheBars_AreOrderedFloorWarningFallbackCritical_TheMajorIsSixteen_AndTheOperationsFloorIsTheOneInBothHourlyReads()
    {
        Assert.Equal(2.0, AnomalyThresholds.PgIoLatencyFloorMs);
        Assert.Equal(10.0, PgTargetScorer.IoReadLatencyWarningMs);
        Assert.Equal(20.0, AnomalyThresholds.PgIoLatencyFallbackMs);
        Assert.Equal(30.0, PgTargetScorer.IoReadLatencyCriticalMs);
        Assert.True(AnomalyThresholds.PgIoLatencyFloorMs < PgTargetScorer.IoReadLatencyWarningMs);
        Assert.True(PgTargetScorer.IoReadLatencyWarningMs < AnomalyThresholds.PgIoLatencyFallbackMs);
        Assert.True(AnomalyThresholds.PgIoLatencyFallbackMs < PgTargetScorer.IoReadLatencyCriticalMs);
        Assert.Equal(16, PgTargetScorer.IoStatIoMinimumMajor);
        Assert.Equal(1000.0, PgTargetScorer.IoMinimumOps);

        /* The literal in the two hourly texts is the scorer's constant — a const string cannot splice a double. */
        var floor = $"reads >= {PgTargetScorer.IoMinimumOps.ToString("0", CultureInfo.InvariantCulture)} /* PgTargetScorer.IoMinimumOps";
        Assert.Contains(floor, PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgIoReadLatency)!, StringComparison.Ordinal);
        Assert.Contains(floor, PgTargetAnomalyDetector.IoLatencyWindowSql, StringComparison.Ordinal);

        /* Measured, and the scorer says so where the lineage census reads it; the population is named. */
        var scorer = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetScorer.Io.cs");
        Assert.Contains("measured: WARNING", scorer, StringComparison.Ordinal);
        Assert.Contains("50 Aurora PostgreSQL clusters", scorer, StringComparison.Ordinal);
        Assert.Contains("2026-09-19", scorer, StringComparison.Ordinal);
        Assert.Contains("Aurora-STORAGE shapes: stock local-disk PostgreSQL", scorer, StringComparison.Ordinal);
        Assert.DoesNotContain("aurora_stat_", scorer, StringComparison.Ordinal);
    }

    /* ───────────────────────── the three reads, by their text ───────────────────────── */

    [Fact]
    public void TheCollectorRead_DifferencesPerIdentity_ResetAware_RelationFilesOnly_AndKeepsNullAsNotReported()
    {
        var sql = PgTargetFactCollector.PgTargetIoLatencySql;
        Assert.Contains("FROM pg_io_stats", sql, StringComparison.Ordinal);
        Assert.Contains("PARTITION BY backend_type, object_type, context", sql, StringComparison.Ordinal);
        Assert.Contains("ROW_NUMBER() OVER series > 1", sql, StringComparison.Ordinal);
        Assert.Contains("stats_reset IS DISTINCT FROM LAG(stats_reset) OVER series", sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(raw_reads, 0)", sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(raw_read_ms, 0)", sql, StringComparison.Ordinal);
        Assert.Contains("object_type = 'relation'", sql, StringComparison.Ordinal);
        Assert.Contains("(reads IS NOT NULL)  AS reads_tracked", sql, StringComparison.Ordinal);
        Assert.Contains("(writes IS NOT NULL) AS writes_tracked", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CURRENT_TIMESTAMP", sql, StringComparison.OrdinalIgnoreCase);
        /* No per-op quotient in SQL: the collector divides in C# so "0 ÷ 0" and "0 ÷ N" are distinguishable shapes. */
        Assert.DoesNotContain("/ ", sql, StringComparison.Ordinal);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Io.cs");
        Assert.Contains("/* filled by lane 11", source, StringComparison.Ordinal);
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        Assert.Contains("CommandTimeout = FactCommandTimeoutSeconds", code, StringComparison.Ordinal);
        Assert.Contains("ExecuteReaderAsync(context.CancellationToken)", code, StringComparison.Ordinal);
        Assert.Contains("catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))", code, StringComparison.Ordinal);
        Assert.Contains("ReportCollectionFailure(ex, context)", code, StringComparison.Ordinal);
        Assert.Contains("context.ObservedDurationMs", code, StringComparison.Ordinal);
        Assert.DoesNotContain("TimeRangeEnd - context.TimeRangeStart", code, StringComparison.Ordinal);
        /* The absent-major shape reads the registry fact and never a 0 major as "old". */
        Assert.Contains("major > 0 && major < PgTargetScorer.IoStatIoMinimumMajor", code, StringComparison.Ordinal);
    }

    [Fact]
    public void TheBaselineArm_IsHourly_PerIdentity_UnderTheFloor_AndEndsInTheOneScaffold()
    {
        var sql = PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgIoReadLatency)!;
        Assert.Null(PgBaselineProvider.GetBaselineQuery(MetricNames.PgIoReadLatency));
        Assert.EndsWith(PgBaselineProvider.RobustTierScaffold, sql, StringComparison.Ordinal);
        Assert.Contains("FROM pg_io_stats", sql, StringComparison.Ordinal);
        Assert.Contains("date_trunc('hour', collection_time) AS hour_start", sql, StringComparison.Ordinal);
        Assert.Contains("PARTITION BY backend_type, context ORDER BY hour_start", sql, StringComparison.Ordinal);
        Assert.Contains("object_type = 'relation'", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(GREATEST(raw_reads, 0))::DOUBLE PRECISION", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT hour_start AS collection_time, read_ms / reads AS v", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1 AND collection_time >= $2 AND collection_time < $3", sql, StringComparison.Ordinal);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetBaselineProvider.Io.cs");
        Assert.Contains("/* filled by lane 11", source, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDetectorRead_TakesTheSameHourlyQuantity_AndTheDetector_IsFencedAndStampsAMeasuredLineage()
    {
        var sql = PgTargetAnomalyDetector.IoLatencyWindowSql;
        Assert.Contains("FROM pg_io_stats", sql, StringComparison.Ordinal);
        Assert.Contains("date_trunc('hour', collection_time) AS hour_start", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(ms_per_read) AS peak_ms_per_read", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1 AND collection_time >= $2 AND collection_time <= $3", sql, StringComparison.Ordinal);
        /* The baseline and the window differ only in the closed upper bound — same CTEs, same floor. */
        var baseline = PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgIoReadLatency)!;
        foreach (var shared in new[] { "hourly AS (", "deltas AS (", "per_hour AS (", "WHERE raw_reads IS NOT NULL", "read_ms / reads" })
        {
            Assert.Contains(shared, sql, StringComparison.Ordinal);
            Assert.Contains(shared, baseline, StringComparison.Ordinal);
        }

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.Io.cs");
        Assert.Contains("/* filled by lane 11", source, StringComparison.Ordinal);
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        Assert.Contains("catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))", code, StringComparison.Ordinal);
        Assert.Contains("_baselineProvider.GetBaselineAsync(", code, StringComparison.Ordinal);
        Assert.Contains("MetricNames.PgIoReadLatency", code, StringComparison.Ordinal);
        Assert.Contains("PgIoLatencyFloorMs, PgIoLatencyFallbackMs", code, StringComparison.Ordinal);
        Assert.Contains("metadata[\"threshold_lineage\"] = 1", source, StringComparison.Ordinal);
        Assert.DoesNotContain("DateTime.UtcNow", code, StringComparison.Ordinal);
    }

    /* ───────────────────────── the graph ───────────────────────── */

    [Fact]
    public void TheIoChain_LeadsAnIoWaitToTheLatency_AndTheLatencyToCachePressureAndThePlannerKnobs_OnTheirVerdictsOnly()
    {
        var graph = new PgTargetRelationshipGraph();
        var standout = PgTargetFactKeys.WaitKey("IO", "DataFileRead");
        var rollup = PgTargetFactKeys.WaitKey("IO", null);

        /* Nothing fired: no active edge anywhere in the chain. */
        var quiet = Lookup(ReadFact(3.0, ops: 26_400), Wait(standout, fired: true), BufferPressure(fired: false), Knob(PgTargetFactKeys.ConfigEffectiveCacheSize, fired: false), Knob(PgTargetFactKeys.ConfigRandomPageCost, fired: false));
        new FactScorer().ScoreAll(quiet.Values.ToList());
        Assert.DoesNotContain(graph.GetActiveEdges(standout, quiet), e => e.Destination == PgTargetFactKeys.IoReadLatencyMs);
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.IoReadLatencyMs, quiet));

        /* Latency fired: the standout's edge leads to it (the IO rollup has none — lane 5's rule, pinned in PgTargetWaitTests);
           it leads on to the cache composite and to each knob at its default. */
        var fired = Lookup(ReadFact(25.0, ops: 26_400), Wait(standout, fired: true), Wait(rollup, fired: true), BufferPressure(fired: true), Knob(PgTargetFactKeys.ConfigEffectiveCacheSize, fired: true), Knob(PgTargetFactKeys.ConfigRandomPageCost, fired: false));
        new FactScorer().ScoreAll(fired.Values.ToList());
        Assert.Contains(graph.GetActiveEdges(standout, fired), e => e.Destination == PgTargetFactKeys.IoReadLatencyMs && e.Category == "io_latency");
        Assert.Empty(graph.GetAllEdges(rollup));
        var onward = graph.GetActiveEdges(PgTargetFactKeys.IoReadLatencyMs, fired).Select(e => e.Destination).Order(StringComparer.Ordinal).ToArray();
        Assert.Equal(new[] { PgTargetFactKeys.ConfigEffectiveCacheSize, PgTargetFactKeys.BufferCachePressure }.Order(StringComparer.Ordinal).ToArray(), onward);

        /* The write fact has no edges either way; the anomaly has none (it folds). */
        Assert.Empty(graph.GetAllEdges(PgTargetFactKeys.IoWriteLatencyMs));
        Assert.Empty(graph.GetAllEdges(PgTargetFactKeys.AnomalyIoLatency));
        Assert.DoesNotContain(graph.GetAllEdges(PgTargetFactKeys.IoReadLatencyMs), e => e.Destination == PgTargetFactKeys.IoWriteLatencyMs);
        Assert.Equal(3, graph.GetAllEdges(PgTargetFactKeys.IoReadLatencyMs).Count);
    }

    /* ───────────────────────── the advice ───────────────────────── */

    [Fact]
    public void ComposeIo_StatesEachTrackednessShape_NeverRendersAnUntimedZero_AndNamesThePopulation()
    {
        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.IoReadLatencyMs));
        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.IoWriteLatencyMs));
        Assert.Contains("pg_stat_io", PgTargetAdvice.Static(PgTargetFactKeys.IoReadLatencyMs)!.Investigation, StringComparison.Ordinal);

        var absent = UnavailableReadFact(PgTargetScorer.IoReasonPgStatIoAbsentKey, ops: 0, major: 15);
        var absentBlock = FactAdvice.Compose(PgTargetFactKeys.IoReadLatencyMs, Lookup(absent))!;
        Assert.Contains("PostgreSQL 15 has no pg_stat_io", absentBlock.Headline, StringComparison.Ordinal);
        Assert.Contains("blk_read_time", absentBlock.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain(" ms", absentBlock.Headline, StringComparison.Ordinal);

        var untimed = UnavailableReadFact(PgTargetScorer.IoReasonTrackIoTimingOffKey, ops: 50_000, knob: 0);
        var untimedBlock = FactAdvice.Compose(PgTargetFactKeys.IoReadLatencyMs, Lookup(untimed))!;
        Assert.Contains("50,000 reads counted over the 4 observed hours, none timed (track_io_timing is off)", untimedBlock.Headline, StringComparison.Ordinal);
        Assert.Contains("CONFIG_PG_TRACK_IO_TIMING", untimedBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("pg_test_timing", untimedBlock.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("0 ms", untimedBlock.Headline + untimedBlock.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("0.0 ms", untimedBlock.Headline + untimedBlock.Investigation, StringComparison.Ordinal);

        var flipped = UnavailableReadFact(PgTargetScorer.IoReasonTrackIoTimingOffKey, ops: 50_000, knob: 1);
        Assert.Contains("the flip happened inside this window", FactAdvice.Compose(PgTargetFactKeys.IoReadLatencyMs, Lookup(flipped))!.Investigation, StringComparison.Ordinal);

        var thin = ReadFact(45.0, ops: 12);
        var thinBlock = FactAdvice.Compose(PgTargetFactKeys.IoReadLatencyMs, Lookup(thin))!;
        Assert.Contains("45.0 ms per operation over only 12 reads", thinBlock.Headline, StringComparison.Ordinal);
        Assert.Contains("under the floor, not graded", thinBlock.Headline, StringComparison.Ordinal);
        Assert.Contains("Under 1,000 operations", thinBlock.Investigation, StringComparison.Ordinal);

        var aurora = ReadFact(18.4, ops: 26_400, aurora: true);
        var anomaly = IoAnomaly(sigma: 12.0, median: 1.3);
        var facts = Lookup(aurora, anomaly);
        new FactScorer().ScoreAll(facts.Values.ToList());
        var auroraBlock = FactAdvice.Compose(PgTargetFactKeys.IoReadLatencyMs, facts)!;
        Assert.Equal("Data-file reads averaged 18.4 ms per operation over the 4 observed hours (Aurora storage tier)", auroraBlock.Headline);
        Assert.Contains("against a 1.3 ms routine for this hour of the week", auroraBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("26,400 reads at 1.83/sec of observed time", auroraBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("14 days × 50 clusters of the dogfood fleet, 2026-09-19", auroraBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("this storage tier's own shape", auroraBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("no disk to swap", auroraBlock.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_pg_top_queries", auroraBlock.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", auroraBlock.Remediation, StringComparison.OrdinalIgnoreCase);

        var stock = ReadFact(4.0, ops: 26_400, aurora: false);
        var stockBlock = FactAdvice.Compose(PgTargetFactKeys.IoReadLatencyMs, Lookup(stock))!;
        Assert.Contains("under the warning bar", stockBlock.Headline, StringComparison.Ordinal);
        Assert.Contains("local NVMe", stockBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("posture, not performance settings", stockBlock.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("routine for this hour", stockBlock.Investigation, StringComparison.Ordinal);

        var write = WriteFact(4.0, ops: 3_000);
        var writeBlock = FactAdvice.Compose(PgTargetFactKeys.IoWriteLatencyMs, Lookup(write))!;
        Assert.Contains("4.0 ms per operation over the 4 observed hours — stated, not graded", writeBlock.Headline, StringComparison.Ordinal);
        Assert.Contains("No bar grades it yet", writeBlock.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void ComposeAnomaly_IoLatency_StatesThePeakSigmaAndBaseline_FirstOccurrenceWithoutASigma_AndNeverTheSqlServerSpike()
    {
        var anomaly = IoAnomaly(sigma: 12.0, median: 1.3);
        anomaly.Metadata["peak_ms_per_read"] = 25.0;
        anomaly.Metadata["baseline_samples"] = 120;
        var block = FactAdvice.Compose(PgTargetFactKeys.AnomalyIoLatency, Lookup(anomaly))!;
        Assert.Equal("Data-file read latency spiked to 25.0 ms per read — 12σ above its baseline for this time of week", block.Headline);
        Assert.Contains("12σ above its 1.3 ms per read baseline median for this hour-of-week (over 120 baseline samples)", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("pg_stat_io", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("get_pg_io_trend", block.Remediation, StringComparison.Ordinal);
        Assert.Equal(block, PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyIoLatency, Lookup(anomaly)));

        var young = IoAnomaly(sigma: 0.3, lowQuality: true, fallbackExceedance: 1.4);
        young.Metadata["peak_ms_per_read"] = 28.0;
        var youngBlock = FactAdvice.Compose(PgTargetFactKeys.AnomalyIoLatency, Lookup(young))!;
        Assert.Contains("first occurrence, no baseline yet", youngBlock.Headline, StringComparison.Ordinal);
        Assert.DoesNotContain("σ", youngBlock.Headline, StringComparison.Ordinal);

        var statik = FactAdvice.GetForFactKey(PgTargetFactKeys.AnomalyIoLatency)!;
        Assert.DoesNotContain("Anomalous spike", statik.Headline, StringComparison.Ordinal);
        Assert.Contains("1,000-read floor", statik.Investigation, StringComparison.Ordinal);
    }

    /* ───────────────────────── THE EXIT CRITERION (gated) ───────────────────────── */

    /// <summary>
    /// 31 days of one-minute <c>pg_io_stats</c> for an Aurora 17 cluster — a client backend at 100 reads a minute
    /// and an autovacuum worker at 10, both at 1.3–1.7 ms per read cycling by the hour (so every hour-of-week bucket
    /// has a spread and a non-zero MAD), a checkpointer whose <c>reads</c> is NULL throughout, the write side NULL
    /// everywhere (Aurora), a <c>stats_reset</c> mid-history restarting the client backend's counters — then the
    /// last 250 minutes at 25 ms per read. <c>pg_database_stats</c> rides alongside for the span gate and the
    /// coverage witness. Two more servers with four hours each: a PostgreSQL 15 with no I/O rows, and a PostgreSQL
    /// 18 whose reads climb with <c>read_time_ms</c> pinned at 0.
    ///
    /// <para><b>Asserted:</b> the collector's read fact at exactly 25.0 ms over 26,400 reads (240 intervals × 110), two
    /// identities, no reset in the window, no write fact (NULL is not reported); the PG 15 server's
    /// <c>unavailable</c> / absent fact and the untimed server's <c>unavailable</c> / timing-off fact with its
    /// 24,000 reads counted and <c>Value</c> 0; the baseline bucket trustworthy with a median inside 1.3–1.7; the
    /// detector firing at the display cap with <c>threshold_lineage = 1</c>; and through the REAL
    /// <c>analyze_server</c>, anchored at the planted window's end: <c>PG_IO_READ_LATENCY_MS</c> rooting a finding
    /// above the warning line lifted by the anomaly co-fire, <c>ANOMALY_PG_IO_LATENCY</c> sharing its incident, both
    /// advices in PostgreSQL nouns, every <c>next_tools</c> entry a <c>get_pg_*</c> read.</para>
    /// </summary>
    [Fact]
    public async Task ThirtyOneDaysAtOnePointFiveMs_ThenFourHoursAtTwentyFive_RootsTheLatencyFinding_WithItsAnomalyInTheSameIncident()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the I/O-family e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, MonitoredEngineKind.AuroraPostgres, 17, ct);
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, OldServerId, OldServerName, MonitoredEngineKind.Postgres, 15, ct);
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, UntimedServerId, UntimedServerName, MonitoredEngineKind.Postgres, 18, ct);

            var end = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            const int minutes = 31 * 24 * 60;
            var start = end.AddMinutes(-minutes);
            const int spikeFrom = minutes - 250;
            const int resetAt = minutes - 20_000;
            var windowStart = end.AddHours(-4);

            /* The span gate and the coverage witness: one flat pg_database_stats row per minute. */
            await PlantAsync(connection, @"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, 'appdb', 1000 + n, 10, 100, 9000, 0, 0, 0, NULL
FROM generate_series(0, $5) AS n", start, minutes, ServerId, ServerName, ct);

            /* pg_io_stats: per-minute increments, cumulative via SUM() OVER; ms per read = 1.3 + 0.1 × (hour % 5), 25.0 in the spike. */
            await PlantAsync(connection, @"
WITH s AS (
    SELECT n,
           CASE WHEN n >= $6 THEN 25.0 ELSE 1.3 + 0.1 * ((n / 60) % 5) END AS ms_per_read,
           CASE WHEN n >= $7 THEN 1 ELSE 0 END AS epoch
    FROM generate_series(0, $5) AS n
),
ident AS (
    SELECT * FROM (VALUES ('client backend', 'normal', 100), ('autovacuum worker', 'vacuum', 10)) AS v(backend_type, context, reads_per_min)
),
c AS (
    SELECT s.n, s.epoch, i.backend_type, i.context,
           SUM(i.reads_per_min) OVER (PARTITION BY i.backend_type, CASE WHEN i.backend_type = 'client backend' THEN s.epoch ELSE 0 END ORDER BY s.n) AS reads,
           SUM(i.reads_per_min * s.ms_per_read) OVER (PARTITION BY i.backend_type, CASE WHEN i.backend_type = 'client backend' THEN s.epoch ELSE 0 END ORDER BY s.n) AS read_ms
    FROM s CROSS JOIN ident i
)
INSERT INTO pg_io_stats
    (collection_id, collection_time, server_id, server_name, backend_type, object_type, context,
     reads, read_time_ms, writes, write_time_ms, stats_reset)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, backend_type, 'relation', context,
       reads::bigint, read_ms::double precision, NULL::bigint, NULL::double precision,
       CASE WHEN backend_type = 'client backend' AND epoch = 1 THEN $2 + ($7 * interval '1 minute') END
FROM c
UNION ALL
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, 'checkpointer', 'relation', 'normal', NULL::bigint, NULL::double precision, NULL::bigint, NULL::double precision, NULL::timestamp
FROM generate_series(0, $5) AS n", start, minutes, ServerId, ServerName, ct, spikeFrom, resetAt);

            /* The PG 15 server: four hours of the witness, no I/O rows (the view does not exist there). */
            await PlantAsync(connection, @"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, 'appdb', 1000 + n, 10, 100, 9000, 0, 0, 0, NULL
FROM generate_series(0, $5) AS n", windowStart, 240, OldServerId, OldServerName, ct);

            /* The untimed server: four hours, 100 reads a minute, read_time_ms pinned at 0. */
            await PlantAsync(connection, @"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, 'appdb', 1000 + n, 10, 100, 9000, 0, 0, 0, NULL
FROM generate_series(0, $5) AS n", windowStart, 240, UntimedServerId, UntimedServerName, ct);
            await PlantAsync(connection, @"
INSERT INTO pg_io_stats
    (collection_id, collection_time, server_id, server_name, backend_type, object_type, context,
     reads, read_time_ms, writes, write_time_ms, stats_reset)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, 'client backend', 'relation', 'normal', 100 * n, 0, 10 * n, 0, NULL
FROM generate_series(0, $5) AS n", windowStart, 240, UntimedServerId, UntimedServerName, ct);

            /* ── The collector alone, on each server. */
            var collector = new PgTargetFactCollector(postgres);
            var facts = await collector.CollectFactsAsync(Context(ServerId, ServerName, windowStart, end));
            var read = Assert.Single(facts, f => f.Key == PgTargetFactKeys.IoReadLatencyMs);
            Assert.Equal(25.0, read.Value, precision: 9);
            Assert.Equal(26_400, read.Metadata[PgTargetScorer.IoOpsKey]);
            Assert.Equal(660_000, read.Metadata[PgTargetScorer.IoOpTimeMsKey], precision: 6);
            Assert.Equal(1, read.Metadata[PgTargetScorer.IoLatencyMeasuredKey]);
            Assert.Equal(0, read.Metadata[PgTargetScorer.IoUnavailableKey]);
            Assert.Equal(0, read.Metadata[PgTargetScorer.IoInsufficientOpsKey]);
            Assert.Equal(0, read.Metadata[PgTargetScorer.IoResetCountKey]);
            Assert.Equal(241, read.Metadata[PgTargetScorer.IoSampleCountKey]);
            Assert.Equal(2, read.Metadata[PgTargetScorer.IoIdentityCountKey]);
            Assert.Equal(1, read.Metadata[PgTargetScorer.IoIsAuroraKey]);
            Assert.Equal(17, read.Metadata[PgTargetScorer.IoServerMajorKey]);
            Assert.Equal(26_400 / (4 * 3600.0), read.Metadata[PgTargetScorer.IoOpsPerSecKey], precision: 9);
            Assert.DoesNotContain(facts, f => f.Key == PgTargetFactKeys.IoWriteLatencyMs);   /* Aurora: writes NULL → not reported */

            var oldFacts = await collector.CollectFactsAsync(Context(OldServerId, OldServerName, windowStart, end));
            var absent = Assert.Single(oldFacts, f => f.Key == PgTargetFactKeys.IoReadLatencyMs);
            Assert.Equal(1, absent.Metadata[PgTargetScorer.IoUnavailableKey]);
            Assert.Equal(1, absent.Metadata[PgTargetScorer.IoReasonPgStatIoAbsentKey]);
            Assert.Equal(15, absent.Metadata[PgTargetScorer.IoServerMajorKey]);
            Assert.Equal(0.0, absent.Value);

            var untimedFacts = await collector.CollectFactsAsync(Context(UntimedServerId, UntimedServerName, windowStart, end));
            var untimed = Assert.Single(untimedFacts, f => f.Key == PgTargetFactKeys.IoReadLatencyMs);
            Assert.Equal(1, untimed.Metadata[PgTargetScorer.IoUnavailableKey]);
            Assert.Equal(1, untimed.Metadata[PgTargetScorer.IoReasonTrackIoTimingOffKey]);
            Assert.Equal(24_000, untimed.Metadata[PgTargetScorer.IoOpsKey]);
            Assert.Equal(0.0, untimed.Value);
            var untimedWrite = Assert.Single(untimedFacts, f => f.Key == PgTargetFactKeys.IoWriteLatencyMs);
            Assert.Equal(1, untimedWrite.Metadata[PgTargetScorer.IoReasonTrackIoTimingOffKey]);
            Assert.Equal(2_400, untimedWrite.Metadata[PgTargetScorer.IoOpsKey]);

            /* ── The baseline and the detector alone. */
            var baselines = new PgTargetBaselineProvider(postgres);
            var bucket = await baselines.GetBaselineAsync(ServerId, MetricNames.PgIoReadLatency, windowStart, ct);
            Assert.True(bucket.IsTrustworthy, "the 30-day read-latency bucket is not trustworthy");
            Assert.InRange(bucket.Median, 1.3, 1.7);
            Assert.True(bucket.EffectiveRobustSigma > 0);
            /* One hourly row per hour-of-week bucket per week is under RestoreThreshold (15) in 30 days: the arm answers
               at the hour-of-DAY tier, as its comment states. Pinned so a grain change here is a deliberate one. */
            Assert.Equal(BaselineTier.HourOnly, bucket.Tier);
            Assert.Equal(0.85, bucket.Confidence, precision: 9);

            var anomalies = await new PgTargetAnomalyDetector(postgres, baselines).DetectAnomaliesAsync(Context(ServerId, ServerName, windowStart, end));
            var anomaly = Assert.Single(anomalies, a => a.Key == PgTargetFactKeys.AnomalyIoLatency);
            Assert.Equal(25.0, anomaly.Value, precision: 6);
            Assert.Equal(25.0, anomaly.Metadata["peak_ms_per_read"], precision: 6);
            Assert.Equal(AnomalyThresholds.SigmaDisplayCap, anomaly.Metadata["deviation_sigma"]);
            Assert.Equal(0, anomaly.Metadata["baseline_low_quality"]);
            Assert.Equal(1, anomaly.Metadata["threshold_lineage"]);
            Assert.True(anomaly.Metadata["peak_hour_reads"] >= PgTargetScorer.IoMinimumOps);

            /* ── THE EXIT CRITERION, through the real analyze_server, anchored at the planted window's end. */
            var service = new DarlingAnalysisService(postgres);
            var asOf = end.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
            var json = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 4, as_of: asOf);
            using (var doc = JsonDocument.Parse(json))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                string RootKey(JsonElement f) => f.GetProperty("root_fact").GetProperty("key").GetString()!;

                var latency = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.IoReadLatencyMs);
                var severity = latency.GetProperty("severity").GetDouble();
                Assert.True(severity >= 0.875 * 1.3 - 1e-9, $"25 ms (base 0.875) lifted by the anomaly co-fire should read ≥ 1.1375, was {severity}");
                Assert.Equal(PgTargetSources.IoSource, latency.GetProperty("category").GetString());
                var advice = latency.GetProperty("advice");
                Assert.Contains("25.0 ms per operation", advice.GetProperty("headline").GetString(), StringComparison.Ordinal);
                Assert.Contains("(Aurora storage tier)", advice.GetProperty("headline").GetString(), StringComparison.Ordinal);
                Assert.Contains("routine for this hour of the week", advice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                Assert.Contains("pg_stat_io", advice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                var tools = latency.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()!).ToList();
                Assert.NotEmpty(tools);
                Assert.All(tools, t => Assert.StartsWith("get_pg_", t, StringComparison.Ordinal));
                Assert.Contains("get_pg_io_stats", tools);

                var anomalyCard = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.AnomalyIoLatency);
                Assert.Equal(latency.GetProperty("incident_id").GetString(), anomalyCard.GetProperty("incident_id").GetString());
                var anomalyAdvice = anomalyCard.GetProperty("advice");
                Assert.Contains("ms per read", anomalyAdvice.GetProperty("headline").GetString(), StringComparison.Ordinal);
                Assert.Contains("σ above its", anomalyAdvice.GetProperty("headline").GetString(), StringComparison.Ordinal);
                Assert.DoesNotContain("Anomalous spike", anomalyAdvice.GetProperty("headline").GetString(), StringComparison.Ordinal);
                var anomalyTools = anomalyCard.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()!).ToList();
                Assert.All(anomalyTools, t => Assert.StartsWith("get_pg_", t, StringComparison.Ordinal));
            }

            /* get_analysis_facts shows the family's one fact with its measured lineage and no write fact. */
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, 4, PgTargetSources.IoSource, as_of: asOf);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var shown = doc.RootElement.GetProperty("facts").EnumerateArray().ToList();
                var one = Assert.Single(shown);
                Assert.Equal(PgTargetFactKeys.IoReadLatencyMs, one.GetProperty("key").GetString());
                Assert.Equal(1, one.GetProperty("metadata").GetProperty("threshold_lineage").GetDouble());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ───────────────────────── helpers ───────────────────────── */

    private static Fact ReadFact(double msPerRead, long ops, bool aurora = true, int major = 17) => LatencyFact(PgTargetFactKeys.IoReadLatencyMs, msPerRead, ops, aurora, major);

    private static Fact WriteFact(double msPerWrite, long ops) => LatencyFact(PgTargetFactKeys.IoWriteLatencyMs, msPerWrite, ops, aurora: false, major: 18);

    /// <summary>The collector's measured shape (timing on): value, ops, the floor flag off the ops, four observed hours.</summary>
    private static Fact LatencyFact(string key, double msPerOp, long ops, bool aurora, int major) => new()
    {
        Source = PgTargetSources.IoSource,
        Key = key,
        Value = msPerOp,
        ServerId = 1,
        Metadata =
        {
            [PgTargetScorer.IoLatencyMeasuredKey] = 1,
            [PgTargetScorer.IoUnavailableKey] = 0,
            [PgTargetScorer.IoInsufficientOpsKey] = ops < PgTargetScorer.IoMinimumOps ? 1 : 0,
            [PgTargetScorer.IoOpsKey] = ops,
            [PgTargetScorer.IoOpTimeMsKey] = msPerOp * ops,
            [PgTargetScorer.IoOpsPerSecKey] = ops / (4 * 3600.0),
            [PgTargetScorer.IoOpsTrackedKey] = 1,
            [PgTargetScorer.IoResetCountKey] = 0,
            [PgTargetScorer.IoSampleCountKey] = 241,
            [PgTargetScorer.IoIdentityCountKey] = 2,
            [PgTargetScorer.IoObservedMsKey] = 4 * 3_600_000,
            [PgTargetScorer.IoServerMajorKey] = major,
            [PgTargetScorer.IoIsAuroraKey] = aurora ? 1 : 0,
        },
    };

    private static Fact UnavailableReadFact(string reasonKey, long ops, int major = 17, double? knob = null)
    {
        var fact = new Fact
        {
            Source = PgTargetSources.IoSource,
            Key = PgTargetFactKeys.IoReadLatencyMs,
            Value = 0,
            ServerId = 1,
            Metadata =
            {
                [PgTargetScorer.IoLatencyMeasuredKey] = 0,
                [PgTargetScorer.IoUnavailableKey] = 1,
                [reasonKey] = 1,
                [PgTargetScorer.IoOpsKey] = ops,
                [PgTargetScorer.IoOpTimeMsKey] = 0,
                [PgTargetScorer.IoOpsPerSecKey] = ops / (4 * 3600.0),
                [PgTargetScorer.IoObservedMsKey] = 4 * 3_600_000,
                [PgTargetScorer.IoServerMajorKey] = major,
                [PgTargetScorer.IoIsAuroraKey] = 0,
            },
        };
        if (knob is { } k)
            fact.Metadata[PgTargetScorer.IoTrackIoTimingConfigKey] = k;
        return fact;
    }

    private static Fact IoAnomaly(double sigma, bool lowQuality = false, double fallbackExceedance = 0, double median = 1.3)
    {
        var fact = new Fact { Source = PgTargetAnomalyDetector.AnomalySource, Key = PgTargetFactKeys.AnomalyIoLatency, Value = 25, ServerId = 1 };
        fact.Metadata["deviation_sigma"] = sigma;
        fact.Metadata["fire_threshold"] = 3.5;
        fact.Metadata["baseline_low_quality"] = lowQuality ? 1 : 0;
        fact.Metadata["fallback_exceedance"] = fallbackExceedance;
        fact.Metadata["confidence"] = 1.0;
        fact.Metadata["baseline_median"] = median;
        fact.Metadata["baseline_mean"] = median;
        return fact;
    }

    /* Sibling fixtures are shaped so the SHARED scorer grades them (ScoreAll recomputes every base): a wait exactly
       at its own concerning bar, the cache composite's hit-ratio arm exactly at its bar, a knob exactly at the
       shipped default. "Not fired" is the same fact one notch under its bar. */

    private static Fact Wait(string key, bool fired)
    {
        var bars = PgTargetScorer.GetPgWaitThresholds(key) ?? throw new InvalidOperationException(key + " is not in the v1 wait vocabulary");
        var isStandout = key != PgTargetFactKeys.WaitKey("IO", null) && key != PgTargetFactKeys.WaitKey("Lock", null);
        return new Fact
        {
            Source = PgTargetSources.WaitsSource,
            Key = key,
            Value = fired ? bars.Concerning : bars.Concerning / 2,
            ServerId = 1,
            Metadata =
            {
                [PgTargetScorer.WaitSourceObservedMsKey] = 4 * 3_600_000,
                [PgTargetScorer.WaitIsStandoutKey] = isStandout ? 1 : 0,
                [PgTargetScorer.WaitMaxStandoutFractionKey] = 0,
                [PgTargetScorer.WaitIsSampledKey] = 0,
            },
        };
    }

    private static Fact BufferPressure(bool fired) => new()
    {
        Source = PgTargetSources.BufferSource,
        Key = PgTargetFactKeys.BufferCachePressure,
        Value = fired ? PgTargetScorer.BufferMissShareConcerning : PgTargetScorer.BufferMissShareConcerning / 2,
        ServerId = 1,
        Metadata =
        {
            ["hit_ratio_suppressed"] = 0,
            ["block_requests_per_sec"] = PgTargetScorer.BufferHitArmMinimumBlocksPerSec * 10,
            ["miss_share"] = fired ? PgTargetScorer.BufferMissShareConcerning : PgTargetScorer.BufferMissShareConcerning / 2,
            ["evictions_tracked"] = 0,
            ["bgwriter_tracked"] = 0,
        },
    };

    private static Fact Knob(string key, bool fired) => new()
    {
        Source = PgTargetSources.ConfigSource,
        Key = key,
        Value = key == PgTargetFactKeys.ConfigEffectiveCacheSize
            ? (fired ? PgTargetScorer.EffectiveCacheSizeDefaultMb : PgTargetScorer.EffectiveCacheSizeDefaultMb * 4)
            : (fired ? PgTargetScorer.RandomPageCostDefault : 1.1),
        ServerId = 1,
    };

    private static Dictionary<string, Fact> Lookup(params Fact[] facts)
    {
        var lookup = new Dictionary<string, Fact>(StringComparer.Ordinal);
        foreach (var fact in facts)
            lookup[fact.Key] = fact;
        return lookup;
    }

    private static AnalysisContext Context(int serverId, string serverName, DateTime start, DateTime end) => new()
    {
        ServerId = serverId,
        ServerName = serverName,
        TimeRangeStart = start,
        TimeRangeEnd = end,
        ServerUtcOffset = TimeSpan.Zero,
        Coverage = new WindowCoverage { NominalMs = 4 * 3_600_000, ObservedMs = 4 * 3_600_000, SampleCount = 240 },
    };

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private static async Task PlantAsync(NpgsqlConnection connection, string sql, DateTime start, int count, int serverId, string serverName, CancellationToken ct, int? spikeFrom = null, int? resetAt = null)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(start);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(count);
        if (spikeFrom is { } s) command.Parameters.AddWithValue(s);
        if (resetAt is { } r) command.Parameters.AddWithValue(r);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = $"{ServerId}, {OldServerId}, {UntimedServerId}";
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_io_stats WHERE server_id IN ({ids}); " +
            $"DELETE FROM pg_database_stats WHERE server_id IN ({ids}); " +
            $"DELETE FROM analysis_findings WHERE server_id IN ({ids}); " +
            $"DELETE FROM analysis_muted WHERE server_id IN ({ids}); " +
            $"DELETE FROM servers WHERE server_id IN ({ids});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
