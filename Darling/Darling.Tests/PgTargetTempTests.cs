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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Lane 6 of #3542 — <c>PG_TEMP_SPILL</c> and its <c>CONFIG_PG_WORK_MEM</c> co-fire (design §3.9), plus the one
/// <c>pg_database_stats</c> read this lane owns (<c>PG_TPS</c>, <c>PG_DEADLOCK_RATE</c>): the scorer, the
/// amplifiers, the graph edge, the advice, the collector and drill-down SQL by their own text, and — gated on
/// <c>DARLING_TEST_PG</c> — the exit criterion through the REAL <c>analyze_server</c> against a planted series
/// with a <c>pg_stat_reset()</c> mid-window.
///
/// <para><b>The pin that matters most (D5).</b> <c>work_mem</c> is evidence-gated: it scores 0 with no spill
/// fact, 0 with a spill below the floor, and reaches ≥ 0.5 ONLY when <c>PG_TEMP_SPILL</c> fires — through the
/// shared <see cref="FactScorer.ScoreAll"/>, because the arithmetic under test (a base of 0 is never amplified;
/// 0.4 × 1.25 = 0.5) is the one that ships. And the spill always LEADS the story: a spill exactly at the floor
/// (0.5) beside a co-fired knob (0.5) would otherwise tie, and emission order (config first) would root the knob.</para>
///
/// <para><b>Every number asserted here was executed on this machine</b> through a net10.0 harness over the built
/// assemblies, and the SQL against a throwaway PostgreSQL 18 store, before the first CI run.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetTempTests
{
    private const string ServerName = "darling-pg-target-temp-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const long MiB = 1024 * 1024;

    /* ───────────────────────── scorer: the spill fact ───────────────────────── */

    [Theory]
    [InlineData(0.0, 0.0)]
    [InlineData(1024 * 1024 - 1, 0.0)]          // one byte under the floor: 0, not the formula's 0.4999
    [InlineData(1024 * 1024, 0.5)]              // at the floor
    [InlineData(10 * 1024 * 1024, 1.0)]         // at the critical line
    [InlineData(20 * 1024 * 1024, 1.0)]
    public void TempSpill_IsZeroBelowTheFloor_AndTheSharedFormulaFromIt(double bytesPerSec, double expected)
    {
        var fact = Spill(bytesPerSec);
        Assert.Equal(expected, PgTargetScorer.ScoreBase(fact), precision: 6);
        /* Unmeasured bar: the fact says so, including when graded to 0. */
        Assert.Equal(0, fact.Metadata["threshold_lineage"]);
    }

    [Fact]
    public void TempSpill_BetweenTheBars_RampsThroughTheSharedFormula()
    {
        var rate = 2_088_413.8666666667;   // the e2e's planted rate: 239 × 120 MiB over 14,400 observed seconds
        /* The shared formula between the bars: 0.5 + 0.5 × (rate − floor) / (critical − floor). */
        var expected = 0.5 + 0.5 * (rate - PgTargetScorer.TempSpillConcerningBytesPerSec) / (PgTargetScorer.TempSpillCriticalBytesPerSec - PgTargetScorer.TempSpillConcerningBytesPerSec);
        Assert.Equal(0.5551, expected, precision: 4);
        Assert.Equal(expected, PgTargetScorer.ScoreBase(Spill(rate)), precision: 10);
    }

    [Fact]
    public void AnyOtherPgTempKey_IsZero()
    {
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(new Fact { Source = PgTargetSources.TempSource, Key = "PG_PROBE", Value = 99 }));
    }

    /* ───────────────────────── scorer: the knob's evidence gate ───────────────────────── */

    [Fact]
    public void WorkMem_IsZeroWithoutTheStamp_ZeroBelowTheFloor_AndTheAdvisoryBaseAtIt()
    {
        var unstamped = Config(PgTargetFactKeys.ConfigWorkMem, 4);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(unstamped));
        /* Un-stamped is context and makes no claim — no lineage flag either. */
        Assert.False(unstamped.Metadata.ContainsKey("threshold_lineage"));

        Assert.Equal(0.0, PgTargetScorer.ScoreBase(StampedWorkMem(4, 1000)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(StampedWorkMem(4, 1024 * 1024 - 1)));

        var atFloor = StampedWorkMem(4, 1024 * 1024);
        Assert.Equal(PgTargetScorer.ConfigAdvisoryBase, PgTargetScorer.ScoreBase(atFloor));
        Assert.Equal(0, atFloor.Metadata["threshold_lineage"]);

        /* The VALUE is not graded: a 64 MB work_mem beside a spill is the same 0.4 as a 4 MB one. */
        Assert.Equal(PgTargetScorer.ConfigAdvisoryBase, PgTargetScorer.ScoreBase(StampedWorkMem(64, 5e6)));
    }

    [Fact]
    public void WorkMem_IsNotAnAdvisoryRoot_AndIsRoutedToTheTempAmplifiers()
    {
        Assert.False(PgTargetFactKeys.IsConfigAdvisoryRoot(PgTargetFactKeys.ConfigWorkMem));
        Assert.DoesNotContain(PgTargetFactKeys.ConfigWorkMem, PgTargetFactKeys.ConfigAdvisoryRoots);

        /* The dispatcher's arm, by its own text: work_mem and the spill share TempAmplifiers, and the config
           base delegates work_mem to the temp partial. */
        var dispatcher = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetScorer.cs");
        Assert.Contains("PgTargetFactKeys.TempSpill or PgTargetFactKeys.ConfigWorkMem => TempAmplifiers(key)", dispatcher, StringComparison.Ordinal);
        var config = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetScorer.Config.cs");
        Assert.Contains("case PgTargetFactKeys.ConfigWorkMem:", config, StringComparison.Ordinal);
        Assert.Contains("return ScoreConfigWorkMem(fact);", config, StringComparison.Ordinal);
    }

    /* ───────────────────────── ScoreAll: the D5 co-fire, through the shared arithmetic ───────────────────────── */

    [Fact]
    public void WorkMem_ScoresZero_WithNoSpillFact()
    {
        var knob = Config(PgTargetFactKeys.ConfigWorkMem, 4);
        new FactScorer().ScoreAll([knob]);
        Assert.Equal(0.0, knob.BaseSeverity);
        Assert.Equal(0.0, knob.Severity);
        Assert.Empty(knob.AmplifierResults);
    }

    [Fact]
    public void WorkMem_ScoresZero_WhenTheSpillIsBelowTheFloor()
    {
        /* The collector stamps whatever it measured; the floor decides. Both facts below it: nothing fires. */
        var knob = StampedWorkMem(4, 1000);
        var spill = Spill(1000);
        new FactScorer().ScoreAll([knob, spill]);
        Assert.Equal(0.0, knob.Severity);
        Assert.Equal(0.0, spill.Severity);
    }

    [Fact]
    public void WorkMem_ReachesTheIncidentLine_OnlyOnTheSpillCoFire_AndTheSpillLeads()
    {
        var rate = 2_088_413.8666666667;
        var knob = StampedWorkMem(4, rate);
        var spill = Spill(rate);
        new FactScorer().ScoreAll([knob, spill]);

        /* 0.4 × (1 + 0.25) = 0.5 — the D5 line, exactly, through KnobCoFireBoost. */
        Assert.Equal(PgTargetScorer.ConfigAdvisoryBase, knob.BaseSeverity);
        Assert.Equal(0.5, knob.Severity, precision: 10);
        Assert.Contains(knob.AmplifierResults, a => a.Matched && a.Description.StartsWith("PG_TEMP_SPILL co-fires", StringComparison.Ordinal));

        /* The spill is corroborated by the knob having fired: 0.5551 × 1.3. */
        Assert.Equal(0.5551, spill.BaseSeverity, precision: 4);
        Assert.Equal(spill.BaseSeverity * (1 + PgTargetScorer.TempCauseBoost), spill.Severity, precision: 10);
        Assert.Contains(spill.AmplifierResults, a => a.Matched && a.Description.StartsWith("CONFIG_PG_WORK_MEM co-fired", StringComparison.Ordinal));
        Assert.Contains(spill.AmplifierResults, a => !a.Matched && a.Description.Contains("PG_BAD_ACTOR_*", StringComparison.Ordinal));
        Assert.True(spill.Severity > knob.Severity);
    }

    [Fact]
    public void ASpillExactlyAtTheFloor_StillLeadsTheKnob_SoOneConditionIsOneStory()
    {
        /* The tie the amplifier exists to break: a 64 MB work_mem (not at default) beside a spill at exactly the
           floor. Without the spill-side boost both sit at 0.5 and emission order (config first) roots the knob. */
        var knob = StampedWorkMem(64, 1024 * 1024);
        var spill = Spill(1024 * 1024, workMemBytes: 64 * MiB);
        var facts = new List<Fact> { knob, spill };
        new FactScorer().ScoreAll(facts);
        Assert.Equal(0.5, knob.Severity, precision: 10);
        Assert.Equal(0.65, spill.Severity, precision: 10);

        var stories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts);
        var story = Assert.Single(stories);
        Assert.Equal($"{PgTargetFactKeys.TempSpill} → {PgTargetFactKeys.ConfigWorkMem}", story.StoryPath);
        Assert.Equal(PgTargetSources.TempSource, story.Category);
    }

    [Fact]
    public void ATempWritingBadActor_CorroboratesTheSpill_AndTheStoryStillEndsAtTheKnob()
    {
        var rate = 2_088_413.8666666667;
        var knob = StampedWorkMem(4, rate);
        var spill = Spill(rate);
        var bad = new Fact { Source = PgTargetSources.QueriesSource, Key = PgTargetFactKeys.BadActorKey(777), Value = 0.01, ServerId = 1 };
        bad.Metadata["temp_blks_written"] = 500_000;
        bad.Metadata["share_of_window_time"] = 0.01;
        var facts = new List<Fact> { knob, spill, bad };
        new FactScorer().ScoreAll(facts);

        /* 0.5551 × (1 + 0.3 + 0.3) = 0.8881 */
        Assert.Equal(spill.BaseSeverity * (1 + 2 * PgTargetScorer.TempCauseBoost), spill.Severity, precision: 10);
        Assert.Equal(0.8881, spill.Severity, precision: 4);
        Assert.Contains(spill.AmplifierResults, a => a.Matched && a.Description.Contains("PG_BAD_ACTOR_*", StringComparison.Ordinal));

        var stories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts);
        var story = Assert.Single(stories);
        Assert.Equal($"{PgTargetFactKeys.TempSpill} → {PgTargetFactKeys.ConfigWorkMem}", story.StoryPath);
    }

    [Fact]
    public void TheEdge_FiresOnlyWhenTheKnobFired_AndNoEdgeLeavesTheKnob()
    {
        var graph = new PgTargetRelationshipGraph();
        var edge = Assert.Single(graph.GetAllEdges(PgTargetFactKeys.TempSpill));
        Assert.Equal(PgTargetFactKeys.ConfigWorkMem, edge.Destination);
        Assert.Empty(graph.GetAllEdges(PgTargetFactKeys.ConfigWorkMem));

        var quiet = new Dictionary<string, Fact>(StringComparer.Ordinal) { [PgTargetFactKeys.ConfigWorkMem] = Config(PgTargetFactKeys.ConfigWorkMem, 4) };
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.TempSpill, quiet));

        var fired = Config(PgTargetFactKeys.ConfigWorkMem, 4);
        fired.BaseSeverity = 0.4;
        fired.Severity = 0.5;
        var active = new Dictionary<string, Fact>(StringComparer.Ordinal) { [PgTargetFactKeys.ConfigWorkMem] = fired };
        Assert.Single(graph.GetActiveEdges(PgTargetFactKeys.TempSpill, active));
    }

    /* ───────────────────────── scorer: the database facts ───────────────────────── */

    [Fact]
    public void TheDeadlockTiers_AreTheAlertBands_ByValue_PinnedEqual()
    {
        /* PerformanceMonitor.Analysis references nothing, so the two values are repeated there and pinned here —
           the FactScorerTests idiom for the SQL Server DEADLOCKS arm. */
        Assert.Equal(ServerHealthThresholds.DeadlockWarnPerHourDefault, PgTargetScorer.DeadlockWarnPerHour);
        Assert.Equal(ServerHealthThresholds.DeadlockCriticalPerHourDefault, PgTargetScorer.DeadlockCriticalPerHour);
    }

    [Theory]
    [InlineData(0.0, 0.0)]
    [InlineData(2.5, 0.25)]
    [InlineData(5.0, 0.5)]      // the Warning tier
    [InlineData(12.5, 0.75)]
    [InlineData(20.0, 1.0)]     // the Critical tier
    [InlineData(25.0, 1.0)]     // the exit criterion: 25/hr grades Critical
    public void DeadlockRate_GradesOnTheSharedPerHourTiers(double perHour, double expected)
    {
        var fact = new Fact { Source = PgTargetSources.DatabaseSource, Key = PgTargetFactKeys.DeadlockRate, Value = perHour };
        Assert.Equal(expected, PgTargetScorer.ScoreBase(fact), precision: 10);
        /* Measured bars carry no unmeasured flag. */
        Assert.False(fact.Metadata.ContainsKey("threshold_lineage"));
    }

    [Fact]
    public void Tps_AndHitRatio_AreContext()
    {
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(new Fact { Source = PgTargetSources.DatabaseSource, Key = PgTargetFactKeys.Tps, Value = 5000 }));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(new Fact { Source = PgTargetSources.DatabaseSource, Key = PgTargetFactKeys.HitRatio, Value = 0.5 }));
    }

    /* ───────────────────────── advice ───────────────────────── */

    [Fact]
    public void TheSpillAdvice_StatesTheHostsNumbers_AndTheOvercommitArithmetic()
    {
        var rate = 2_088_413.8666666667;
        var knob = StampedWorkMem(4, rate);
        var spill = Spill(rate);
        var facts = new List<Fact> { knob, spill };
        new FactScorer().ScoreAll(facts);
        var lookup = facts.ToDictionary(f => f.Key, StringComparer.Ordinal);

        var block = PgTargetAdvice.Compose(PgTargetFactKeys.TempSpill, lookup)!;
        Assert.Equal("Temp-file spill: 2 MB/s of observed time — 28 GB across 239 temp files in the window, 100% of it in appdb", block.Headline);
        Assert.Contains("this server wrote 28 GB in 239 temp files — 2 MB/s, 0.017 files/s, an average of 120 MB per file", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("the counters were reset 1 time(s) in the window (1 rewind(s) seen), each clamped to zero", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("is a chosen bar, not a fleet measurement (threshold_lineage = 0)", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("work_mem on this host is 4 MB, the shipped default", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("pg_temp_spill_statements", block.Investigation, StringComparison.Ordinal);
        /* The counter-objective with the host's numbers: 4 MB × 200 = 800 MB. */
        Assert.Contains("Here: 4 MB × 200 connections = 800 MB at a full house of one-sort backends", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("SET LOCAL work_mem", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("Start from the statements, not the knob", block.Remediation, StringComparison.Ordinal);

        /* The knob as the leaf: the same evidence paragraph, headed by the knob. */
        var leaf = PgTargetAdvice.Compose(PgTargetFactKeys.ConfigWorkMem, lookup)!;
        Assert.Equal("work_mem is 4 MB on this host, and the window spilled 2 MB/s to temp files", leaf.Headline);
        Assert.Equal(block.Investigation, leaf.Investigation);
        Assert.Equal(block.Remediation, leaf.Remediation);
    }

    [Fact]
    public void TheWorkMemAdvice_WithoutASpill_DescribesTheGate_NotAVerdict()
    {
        var lookup = new Dictionary<string, Fact>(StringComparer.Ordinal) { [PgTargetFactKeys.ConfigWorkMem] = Config(PgTargetFactKeys.ConfigWorkMem, 4) };
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.ConfigWorkMem, lookup)!;
        Assert.Equal("work_mem is 4 MB — a value alone is not a finding", block.Headline);
        Assert.Contains("evidence-gated (D5)", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("On this host it is 4 MB, the shipped default.", block.Investigation, StringComparison.Ordinal);
        Assert.StartsWith("Nothing to change on this value alone.", block.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSpillAdvice_WithoutTheConfigSnapshot_SaysWorkMemIsNotKnown_NeverAssumesFourMb()
    {
        var spill = Spill(5e6, workMemBytes: null, maxConnections: null);
        spill.BaseSeverity = 0.7;
        spill.Severity = 0.7;
        var lookup = new Dictionary<string, Fact>(StringComparer.Ordinal) { [PgTargetFactKeys.TempSpill] = spill };
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.TempSpill, lookup)!;
        Assert.Contains("work_mem on this host is not known here — the config snapshot has not been collected", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("Here:", block.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("4 MB", block.Investigation + block.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDeadlockAdvice_SaysTheEngineCountedN_AndMWereCapturedFromTheLog()
    {
        var rate = DeadlockRate(24.75, counter: 99, exemplars: 3, observedHours: 4);
        var lookup = new Dictionary<string, Fact>(StringComparer.Ordinal) { [PgTargetFactKeys.DeadlockRate] = rate };
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.DeadlockRate, lookup)!;
        Assert.Equal("24.75 deadlocks per hour over 4 hours — the engine counted 99; 3 were captured from the log", block.Headline);
        Assert.Contains("the engine counted 99; 3 were captured from the log", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("99 of them in appdb", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("The two differ by design, not by defect", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("(5 and 20 per hour)", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("get_pg_deadlocks", block.Remediation, StringComparison.Ordinal);

        /* The exemplar read failed: the count is not fabricated as 0. */
        var noExemplars = DeadlockRate(3, counter: 12, exemplars: null, observedHours: 4);
        var block2 = PgTargetAdvice.Compose(PgTargetFactKeys.DeadlockRate, new Dictionary<string, Fact>(StringComparer.Ordinal) { [PgTargetFactKeys.DeadlockRate] = noExemplars })!;
        Assert.Equal("3 deadlocks per hour over 4 hours — the engine counted 12", block2.Headline);
        Assert.Contains("The log capture was not read this pass", block2.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("0 were captured", block2.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryBlockThisLaneComposes_HasAStaticShape_CarriesNoDdl_AndNeverNamesEffectiveCacheSize()
    {
        var rate = 2_088_413.8666666667;
        var knob = StampedWorkMem(4, rate);
        var spill = Spill(rate);
        var deadlock = DeadlockRate(24.75, counter: 99, exemplars: 3, observedHours: 4);
        var tps = new Fact { Source = PgTargetSources.DatabaseSource, Key = PgTargetFactKeys.Tps, Value = 1.676, ServerId = 1 };
        tps.Metadata[PgTargetScorer.TpsCommitsKey] = 23_900;
        tps.Metadata[PgTargetScorer.TpsRollbacksKey] = 239;
        tps.Metadata[PgTargetScorer.TpsRollbackShareKey] = 239 / 24_139.0;
        var facts = new List<Fact> { knob, spill, deadlock, tps };
        new FactScorer().ScoreAll(facts);
        var lookup = facts.ToDictionary(f => f.Key, StringComparer.Ordinal);

        foreach (var key in new[] { PgTargetFactKeys.TempSpill, PgTargetFactKeys.ConfigWorkMem, PgTargetFactKeys.DeadlockRate, PgTargetFactKeys.Tps, PgTargetFactKeys.HitRatio })
        {
            foreach (var block in new[] { PgTargetAdvice.Compose(key, lookup), PgTargetAdvice.Static(key) })
            {
                Assert.NotNull(block);
                var text = block!.Headline + "\n" + block.Investigation + "\n" + block.Remediation;
                Assert.DoesNotContain("CREATE INDEX", text, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("effective_cache_size", text, StringComparison.OrdinalIgnoreCase);
                Assert.Null(block.RemediationTsql);
            }
        }

        Assert.Equal("1.68 transactions per second of observed time (1% rollbacks)", PgTargetAdvice.Compose(PgTargetFactKeys.Tps, lookup)!.Headline);
        Assert.StartsWith("Buffer-cache hit ratio — read from PG_BUFFER_CACHE_PRESSURE", PgTargetAdvice.Static(PgTargetFactKeys.HitRatio)!.Headline, StringComparison.Ordinal);
    }

    /* ───────────────────────── the SQL, by its own text ───────────────────────── */

    [Fact]
    public void TheDatabaseRead_IsTheReaderDifferencing_TakesTheRateFromTheCounter_AndLeavesTheBlockCountersToBuffer()
    {
        var sql = PgTargetFactCollector.PgTargetDatabaseCountersSql;
        Assert.Contains("FROM pg_database_stats", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        /* The reset-aware differencing shape, verbatim from DarlingPgDatabaseReader.PgDatabaseSql. */
        Assert.Contains("(ROW_NUMBER() OVER series > 1", sql, StringComparison.Ordinal);
        Assert.Contains("AND stats_reset IS DISTINCT FROM LAG(stats_reset) OVER series) AS reset_here", sql, StringComparison.Ordinal);
        Assert.Contains("PARTITION BY database_name", sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(raw_deadlocks, 0)", sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(raw_temp_bytes, 0)", sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(raw_temp_files, 0)", sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(raw_xact_commit, 0)", sql, StringComparison.Ordinal);
        Assert.Contains("count(raw_deadlocks) AS integer)                            AS intervals", sql, StringComparison.Ordinal);
        /* The trend halves ride the same scan, split on the midpoint bound as $4. */
        Assert.Contains("FILTER (WHERE collection_time > $4), 0) AS bigint) AS xact_second_half", sql, StringComparison.Ordinal);
        Assert.Contains("count(raw_deadlocks) FILTER (WHERE collection_time > $4) AS integer) AS intervals_second_half", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LAG(stats_reset) OVER series IS NOT NULL", sql, StringComparison.Ordinal);
        /* The RATE never comes from the log capture (design §6 C). */
        Assert.DoesNotContain("pg_deadlocks", sql, StringComparison.Ordinal);
        /* The block counters are the buffer composite's; a second hit ratio would double-count (D2). */
        Assert.DoesNotContain("blks_hit", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("blks_read", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("HAVING", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT", sql, StringComparison.Ordinal);

        /* The exemplar read is the ONLY place this collector names pg_deadlocks, and it is a count, not a rate. */
        var exemplar = PgTargetFactCollector.PgTargetDeadlockExemplarCountSql;
        Assert.Contains("FROM pg_deadlocks", exemplar, StringComparison.Ordinal);
        Assert.Contains("count(*)", exemplar, StringComparison.Ordinal);
        Assert.DoesNotContain("LAG(", exemplar, StringComparison.Ordinal);
        Assert.Contains(sql, PgTargetFactCollector.AllSql);
        Assert.Contains(exemplar, PgTargetFactCollector.AllSql);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Database.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        /* PG_HIT_RATIO is never emitted here. */
        Assert.DoesNotContain("PgTargetFactKeys.HitRatio", code, StringComparison.Ordinal);
        /* Each fact is gated on its own counter having moved; the D5 stamp lands on the knob fact. */
        Assert.Contains("if (transactions > 0)", code, StringComparison.Ordinal);
        Assert.Contains("if (deadlocks > 0)", code, StringComparison.Ordinal);
        Assert.Contains("if (tempFiles > 0)", code, StringComparison.Ordinal);
        Assert.Contains("workMem.Metadata[PgTargetScorer.WorkMemSpillBytesPerSecKey] = bytesPerSec;", code, StringComparison.Ordinal);
        /* A trend needs both halves observed; one half is not a trend. */
        Assert.Contains("if (intervalsFirstHalf > 0 && intervalsSecondHalf > 0)", code, StringComparison.Ordinal);
        /* Rates over observed time, never the nominal window. */
        Assert.Contains("context.ObservedDurationMs / 1000.0", code, StringComparison.Ordinal);
        Assert.DoesNotContain("TimeRangeEnd - context.TimeRangeStart", code, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDrillDown_ListsTheTopTempWriters_DifferencedAndCapped_UnderItsOwnKey()
    {
        var sql = PgTargetDrillDownCollector.PgTargetTempSpillStatementsSql;
        Assert.Contains("FROM pg_statement_stats", sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(temp_blks_written - LAG(temp_blks_written) OVER identity, 0)", sql, StringComparison.Ordinal);
        Assert.Contains("PARTITION BY queryid, database_id, user_id, toplevel", sql, StringComparison.Ordinal);
        Assert.Contains("HAVING SUM(s.d_temp_blks_written) > 0", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY SUM(s.d_temp_blks_written) DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN pg_statement_text AS t", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(5, PgTargetDrillDownCollector.TempSpillStatementCap);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetDrillDownCollector.Queries.cs");
        Assert.Contains("finding.DrillDown![\"pg_temp_spill_statements\"]", source, StringComparison.Ordinal);
        Assert.Contains("if (pathKeys.Contains(PgTargetFactKeys.TempSpill))", source, StringComparison.Ordinal);
        Assert.Contains("queryid = queryId.ToString(CultureInfo.InvariantCulture)", source, StringComparison.Ordinal);
        /* Lane 7's pin still holds — the seam is named as filled, not silently gone. */
        Assert.Contains("filled by lane 6", source, StringComparison.Ordinal);
    }

    /* ───────────────────────── gated: the exit criterion ───────────────────────── */

    /// <summary>
    /// Four hours of <c>pg_database_stats</c> for <c>appdb</c> with every counter climbing and a
    /// <c>pg_stat_reset()</c> at T+2h (counters to zero, <c>stats_reset</c> NULL → timestamp — the first-reset
    /// trap), a flat sibling database, three captured deadlock graphs, a config snapshot with <c>work_mem</c> at
    /// its default and <c>max_connections</c> 200, and one statement writing 500,000 temp blocks. Expected:
    /// <c>PG_TEMP_SPILL → CONFIG_PG_WORK_MEM</c> with the statement in the drill-down; <c>PG_DEADLOCK_RATE</c> at
    /// 24.75/hr (99 counted — 49 before the reset and 50 after, the clamp dropping only the reset interval — with
    /// 3 exemplars) rooting its own Critical card; <c>PG_TPS</c> as context; the knob never rooting.
    /// </summary>
    [Fact]
    public async Task APlantedSpillWithAMidWindowReset_ProducesTheSpillToWorkMemStory_WithTheOffenderInTheDrillDown()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the temp-spill e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, MonitoredEngineKind.Postgres, 18, ct);

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-4);
            var resetAt = windowStart.AddMinutes(120);

            /* The span gate: one flat row 25 h back. */
            await PlantDatabaseStatsAsync(connection, windowEnd.AddHours(-25), "appdb", 1000, 10, 0, 0, 0, null, ct);

            /* Minute -1 … 240 (the -1 row is outside the read; 241 rows in, 240 intervals per series).
               appdb climbs — 100 commits, 1 rollback, 1 temp file of 120 MiB per minute, 100 deadlocks over the
               four hours — and is RESET at minute 120: every counter back to zero, stats_reset NULL → resetAt.
               other is flat: a second series that sums in as zeros. */
            for (var m = -1; m <= 240; m++)
            {
                var at = windowStart.AddMinutes(m);
                var mm = Math.Max(m, 0);
                if (mm < 120)
                    await PlantDatabaseStatsAsync(connection, at, "appdb", 1000 + 100L * mm, 10 + mm, mm, 120 * MiB * mm, mm * 100 / 240, null, ct);
                else
                    await PlantDatabaseStatsAsync(connection, at, "appdb", 100L * (mm - 120), mm - 120, mm - 120, 120 * MiB * (mm - 120), (mm - 120) * 100 / 240, resetAt, ct);
                await PlantDatabaseStatsAsync(connection, at, "other", 5000, 50, 0, 0, 0, null, ct);
            }

            for (var i = 0; i < 3; i++)
                await PlantDeadlockAsync(connection, windowStart.AddMinutes(30 + i * 60), $"hash-{i}", ct);

            await PlantConfigSnapshotAsync(connection, windowEnd.AddMinutes(-10), ct);

            /* 777 spills 500,000 blocks between its two snapshots; 888 runs more and never spills. */
            await PlantStatementAsync(connection, windowStart.AddHours(1), 777, tempWritten: 0, tempRead: 0, deltaCalls: 10, deltaMs: 5000, ct);
            await PlantStatementAsync(connection, windowStart.AddHours(2), 777, tempWritten: 500_000, tempRead: 500_000, deltaCalls: 10, deltaMs: 5000, ct);
            await PlantStatementAsync(connection, windowStart.AddHours(1), 888, tempWritten: 0, tempRead: 0, deltaCalls: 100, deltaMs: 8000, ct);
            await PlantStatementAsync(connection, windowStart.AddHours(2), 888, tempWritten: 0, tempRead: 0, deltaCalls: 100, deltaMs: 8000, ct);
            await PlantStatementTextAsync(connection, 777, "SELECT * FROM big ORDER BY payload", ct);

            /* ── the collector alone. */
            var collector = new PgTargetFactCollector(postgres);
            var context = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var facts = await collector.CollectFactsAsync(context);
            Assert.False(context.Coverage!.IsPartial);
            Assert.Equal(14_400_000, context.ObservedDurationMs, precision: 3);

            /* The reset, clamped and REPORTED: 119 climbing intervals before it, 120 after; the reset interval
               itself is the one clamped. A last-minus-first would say 14,400 MiB; the truth is 239 × 120 MiB. */
            var spill = Assert.Single(facts, f => f.Key == PgTargetFactKeys.TempSpill);
            Assert.Equal(PgTargetSources.TempSource, spill.Source);
            Assert.Equal("appdb", spill.DatabaseName);
            Assert.Equal(239 * 120 * MiB, spill.Metadata[PgTargetScorer.TempSpillBytesKey]);
            Assert.Equal(239, spill.Metadata[PgTargetScorer.TempSpillFilesKey]);
            Assert.Equal(239 * 120 * MiB / 14_400.0, spill.Value, precision: 6);
            Assert.Equal(1.0, spill.Metadata[PgTargetScorer.TempSpillTopDatabaseShareKey]);
            Assert.Equal(2, spill.Metadata[PgTargetScorer.CounterDatabasesKey]);
            Assert.Equal(482, spill.Metadata[PgTargetScorer.CounterSampleCountKey]);
            Assert.Equal(480, spill.Metadata[PgTargetScorer.CounterIntervalsKey]);
            Assert.Equal(1, spill.Metadata[PgTargetScorer.CounterStatsResetCountKey]);
            Assert.Equal(1, spill.Metadata[PgTargetScorer.CounterRewindCountKey]);
            Assert.Equal(4 * MiB, spill.Metadata[PgTargetScorer.TempSpillWorkMemBytesKey]);
            Assert.Equal(200, spill.Metadata[PgTargetScorer.TempSpillMaxConnectionsKey]);

            var knob = Assert.Single(facts, f => f.Key == PgTargetFactKeys.ConfigWorkMem);
            Assert.Equal(spill.Value, knob.Metadata[PgTargetScorer.WorkMemSpillBytesPerSecKey], precision: 6);

            var deadlock = Assert.Single(facts, f => f.Key == PgTargetFactKeys.DeadlockRate);
            Assert.Equal(PgTargetSources.DatabaseSource, deadlock.Source);
            Assert.Equal("appdb", deadlock.DatabaseName);
            Assert.Equal(99, deadlock.Metadata[PgTargetScorer.DeadlockCounterCountKey]);
            Assert.Equal(24.75, deadlock.Value, precision: 6);
            Assert.Equal(24.75, deadlock.Metadata[PgTargetScorer.DeadlocksPerHourKey], precision: 6);
            Assert.Equal(3, deadlock.Metadata[PgTargetScorer.DeadlockExemplarCountKey]);
            Assert.Equal(1, deadlock.Metadata[PgTargetScorer.CounterStatsResetCountKey]);

            var tps = Assert.Single(facts, f => f.Key == PgTargetFactKeys.Tps);
            Assert.Equal(23_900, tps.Metadata[PgTargetScorer.TpsCommitsKey]);
            Assert.Equal(239, tps.Metadata[PgTargetScorer.TpsRollbacksKey]);
            Assert.Equal(24_139 / 14_400.0, tps.Value, precision: 6);
            /* The trend for lane 3: the first half holds minutes 1–120 (the reset interval at 120 clamped to zero —
               11,900 commits + 119 rollbacks), the second half minutes 121–240 (12,000 + 120); each over its own
               240 intervals of the 480 (the flat sibling's count in), i.e. 7,200 observed seconds a half. */
            Assert.Equal(12_019 / 7_200.0, tps.Metadata[PgTargetScorer.TpsFirstHalfKey], precision: 6);
            Assert.Equal(12_120 / 7_200.0, tps.Metadata[PgTargetScorer.TpsSecondHalfKey], precision: 6);
            Assert.Equal(101 / 7_200.0, tps.Metadata[PgTargetScorer.TpsTrendKey], precision: 6);
            Assert.DoesNotContain(facts, f => f.Key == PgTargetFactKeys.HitRatio);

            /* ── scored through the shared arithmetic. */
            new FactScorer().ScoreAll(facts);
            Assert.Equal(0.5551, spill.BaseSeverity, precision: 4);
            Assert.Equal(0.8881, spill.Severity, precision: 4);          // × (1 + 0.3 knob co-fired + 0.3 temp-writing bad actor)
            Assert.Equal(0.4, knob.BaseSeverity, precision: 10);
            Assert.Equal(0.5, knob.Severity, precision: 10);
            Assert.Equal(1.0, deadlock.Severity, precision: 10);
            Assert.Equal(0.0, tps.Severity);
            Assert.Equal(0, spill.Metadata["threshold_lineage"]);
            Assert.False(deadlock.Metadata.ContainsKey("threshold_lineage"));

            /* ── THE EXIT CRITERION: the real analyze_server. */
            var service = new DarlingAnalysisService(postgres);
            var analysis = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 4);
            using (var doc = JsonDocument.Parse(analysis))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();

                var chain = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.TempSpill);
                Assert.Equal($"{PgTargetFactKeys.TempSpill} → {PgTargetFactKeys.ConfigWorkMem}", chain.GetProperty("story_path").GetString());
                Assert.Equal(PgTargetSources.TempSource, chain.GetProperty("category").GetString());
                Assert.Equal(PgTargetFactKeys.ConfigWorkMem, chain.GetProperty("leaf_fact").GetProperty("key").GetString());
                /* The knob never roots. */
                Assert.DoesNotContain(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.ConfigWorkMem);

                var advice = chain.GetProperty("advice");
                Assert.Contains("28 GB across 239 temp files in the window, 100% of it in appdb", advice.GetProperty("headline").GetString(), StringComparison.Ordinal);
                Assert.Contains("work_mem on this host is 4 MB, the shipped default", advice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                Assert.Contains("Here: 4 MB × 200 connections = 800 MB", advice.GetProperty("remediation").GetString(), StringComparison.Ordinal);

                /* The offender, in the drill-down: 777 and only 777 (888 wrote no temp blocks). */
                var offenders = chain.GetProperty("drill_down").GetProperty("pg_temp_spill_statements").EnumerateArray().ToList();
                var offender = Assert.Single(offenders);
                Assert.Equal("777", offender.GetProperty("queryid").GetString());
                Assert.Equal(500_000, offender.GetProperty("temp_blks_written").GetInt64());
                Assert.Equal(500_000L * 8192, offender.GetProperty("temp_bytes_written").GetInt64());
                Assert.Equal(20, offender.GetProperty("calls").GetInt64());
                Assert.Equal("SELECT * FROM big ORDER BY payload", offender.GetProperty("query_text").GetString());

                var tools = chain.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()!).ToList();
                Assert.Contains("get_pg_database_stats", tools);
                Assert.Contains("get_pg_top_queries", tools);

                /* The deadlock rate roots its own Critical card, and says both numbers. */
                var deadlockCard = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.DeadlockRate);
                Assert.Equal(1.0, deadlockCard.GetProperty("severity").GetDouble(), precision: 6);
                Assert.Contains("the engine counted 99; 3 were captured from the log", deadlockCard.GetProperty("advice").GetProperty("headline").GetString(), StringComparison.Ordinal);
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

    private static Fact Config(string key, double value) =>
        new() { Source = PgTargetSources.ConfigSource, Key = key, Value = value, ServerId = 1 };

    private static Fact StampedWorkMem(double mb, double spillBytesPerSec)
    {
        var fact = Config(PgTargetFactKeys.ConfigWorkMem, mb);
        fact.Metadata["bytes"] = mb * MiB;
        fact.Metadata[PgTargetScorer.WorkMemSpillBytesPerSecKey] = spillBytesPerSec;
        return fact;
    }

    /// <summary>A spill fact as the collector stamps it for the e2e's series (4 observed hours, one reset).</summary>
    private static Fact Spill(double bytesPerSec, double? workMemBytes = 4 * MiB, double? maxConnections = 200)
    {
        var fact = new Fact { Source = PgTargetSources.TempSource, Key = PgTargetFactKeys.TempSpill, Value = bytesPerSec, ServerId = 1, DatabaseName = "appdb" };
        fact.Metadata[PgTargetScorer.TempSpillBytesKey] = bytesPerSec * 14_400;
        fact.Metadata[PgTargetScorer.TempSpillFilesKey] = 239;
        fact.Metadata[PgTargetScorer.TempSpillBytesPerSecKey] = bytesPerSec;
        fact.Metadata[PgTargetScorer.TempSpillFilesPerSecKey] = 239 / 14_400.0;
        fact.Metadata[PgTargetScorer.TempSpillTopDatabaseShareKey] = 1.0;
        fact.Metadata[PgTargetScorer.CounterDatabasesKey] = 2;
        fact.Metadata[PgTargetScorer.CounterSampleCountKey] = 482;
        fact.Metadata[PgTargetScorer.CounterIntervalsKey] = 480;
        fact.Metadata[PgTargetScorer.CounterStatsResetCountKey] = 1;
        fact.Metadata[PgTargetScorer.CounterRewindCountKey] = 1;
        fact.Metadata[PgTargetScorer.CounterObservedMsKey] = 14_400_000;
        if (workMemBytes is { } wm) fact.Metadata[PgTargetScorer.TempSpillWorkMemBytesKey] = wm;
        if (maxConnections is { } mc) fact.Metadata[PgTargetScorer.TempSpillMaxConnectionsKey] = mc;
        return fact;
    }

    private static Fact DeadlockRate(double perHour, double counter, double? exemplars, double observedHours)
    {
        var fact = new Fact { Source = PgTargetSources.DatabaseSource, Key = PgTargetFactKeys.DeadlockRate, Value = perHour, ServerId = 1, DatabaseName = "appdb" };
        fact.Metadata[PgTargetScorer.DeadlockCounterCountKey] = counter;
        fact.Metadata[PgTargetScorer.DeadlocksPerHourKey] = perHour;
        fact.Metadata[PgTargetScorer.DeadlockObservedHoursKey] = observedHours;
        fact.Metadata[PgTargetScorer.DeadlockTopDatabaseCountKey] = counter;
        fact.Metadata[PgTargetScorer.CounterIntervalsKey] = 480;
        fact.Metadata[PgTargetScorer.CounterStatsResetCountKey] = 1;
        fact.Metadata[PgTargetScorer.CounterRewindCountKey] = 1;
        if (exemplars is { } e) fact.Metadata[PgTargetScorer.DeadlockExemplarCountKey] = e;
        return fact;
    }

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    /// <summary>One <c>pg_database_stats</c> row as the collector writes it: raw cumulative counters, the block
    /// counters flat (they are the buffer composite's), <c>stats_reset</c> NULL until the planted reset.</summary>
    private static async Task PlantDatabaseStatsAsync(
        NpgsqlConnection connection, DateTime at, string database, long xactCommit, long xactRollback, long tempFiles, long tempBytes, long deadlocks,
        DateTime? statsReset, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
VALUES ($1, $2, $3, $4, $5, $6, $7, 100, 9000, $8, $9, $10, $11)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(xactCommit);
        command.Parameters.AddWithValue(xactRollback);
        command.Parameters.AddWithValue(tempFiles);
        command.Parameters.AddWithValue(tempBytes);
        command.Parameters.AddWithValue(deadlocks);
        command.Parameters.Add(new NpgsqlParameter { Value = statsReset.HasValue ? statsReset.Value : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantDeadlockAsync(NpgsqlConnection connection, DateTime at, string hash, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_deadlocks
    (collection_id, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count, deadlock_hash, lock_modes, resources, victim_statement, graph_text)
VALUES ($1, $2, $3, $4, $2, 4242, 2, $5, 'ShareLock', 'relation', 'UPDATE t SET x = $1', 'graph')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(hash);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The config snapshot: the two knobs and the three convention checks TUNED (so nothing else roots),
    /// <c>work_mem</c> at its 4 MB default, <c>max_connections</c> 200 for the arithmetic.</summary>
    private static async Task PlantConfigSnapshotAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct)
    {
        var rows = new (string Name, string Setting, string? Unit, string BootVal, string Source)[]
        {
            ("shared_buffers", "262144", "8kB", "1024", "configuration file"),
            ("max_wal_size", "4096", "MB", "1024", "configuration file"),
            ("min_wal_size", "80", "MB", "80", "configuration file"),
            ("effective_cache_size", "1048576", "8kB", "524288", "configuration file"),
            ("random_page_cost", "1.1", null, "4", "configuration file"),
            ("track_io_timing", "on", null, "off", "configuration file"),
            ("checkpoint_timeout", "300", "s", "300", "default"),
            ("checkpoint_completion_target", "0.9", null, "0.9", "default"),
            ("wal_compression", "off", null, "off", "default"),
            ("bgwriter_delay", "200", "ms", "200", "default"),
            ("bgwriter_lru_maxpages", "100", null, "100", "default"),
            ("max_connections", "200", null, "100", "configuration file"),
            ("superuser_reserved_connections", "3", null, "3", "default"),
            ("work_mem", "4096", "kB", "4096", "default"),
            ("maintenance_work_mem", "65536", "kB", "65536", "default"),
            ("autovacuum", "on", null, "on", "default"),
        };

        var collectionId = CollectionIdGenerator.Next();
        foreach (var row in rows)
        {
            using var command = new NpgsqlCommand(@"
INSERT INTO pg_server_config
    (collection_id, collection_time, server_id, server_name, name, setting, unit, category, context, vartype, source, boot_val, reset_val, sourcefile, sourceline, pending_restart, short_desc)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'Test', 'postmaster', 'string', $8, $9, $9, NULL, NULL, false, NULL)", connection);
            command.Parameters.AddWithValue(collectionId);
            command.Parameters.AddWithValue(at);
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(ServerName);
            command.Parameters.AddWithValue(row.Name);
            command.Parameters.AddWithValue(row.Setting);
            command.Parameters.Add(new NpgsqlParameter { Value = (object?)row.Unit ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            command.Parameters.AddWithValue(row.Source);
            command.Parameters.AddWithValue(row.BootVal);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task PlantStatementAsync(
        NpgsqlConnection connection, DateTime at, long queryId, long tempWritten, long tempRead, long deltaCalls, double deltaMs, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_statement_stats
    (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel,
     calls, total_exec_time_ms, max_exec_time_ms, rows_returned, shared_blks_hit, shared_blks_read,
     temp_blks_read, temp_blks_written, wal_bytes, delta_calls, delta_total_exec_time_ms, delta_rows, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 16384, 10, TRUE, 1000, 50000, 900.5, 100, 10, 5, $6, $7, 0, $8, $9, 10, 3600)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(tempRead);
        command.Parameters.AddWithValue(tempWritten);
        command.Parameters.AddWithValue(deltaCalls);
        command.Parameters.AddWithValue(deltaMs);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantStatementTextAsync(NpgsqlConnection connection, long queryId, string text, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_statement_text (server_id, queryid, query_text, first_seen, last_seen)
VALUES ($1, $2, $3, $4, $4)
ON CONFLICT (server_id, queryid) DO UPDATE SET query_text = EXCLUDED.query_text", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(text);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_deadlocks WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_server_config WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_statement_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_statement_text WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
