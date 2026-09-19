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
/// Lane 2 of #3542 — the two knobs (<c>CONFIG_PG_SHARED_BUFFERS</c>, <c>CONFIG_PG_MAX_WAL_SIZE</c>) and their
/// workload co-fires (<c>PG_BUFFER_CACHE_PRESSURE</c>, <c>PG_CHECKPOINT_PRESSURE</c>), across the scorer,
/// the amplifiers, the graph, the advice and the collector SQL; and, gated on <c>DARLING_TEST_PG</c>, the
/// lane's exit criterion through the REAL <c>analyze_server</c>.
///
/// <para><b>The pin that matters most (D5).</b> A convention check — a knob at its shipped default — roots an
/// ADVISORY card at 0.4 and never an incident on its own; it reaches the 0.5 incident line ONLY when the
/// engine's own workload signal co-fires with <c>BaseSeverity &gt; 0</c>. Not presence: the collector emits
/// the pressure fact whenever checkpoints ran or blocks moved, and a quiet server's 0 %-requested fact
/// beside a default knob must leave the knob at 0.4. Pinned through the shared <see cref="FactScorer.ScoreAll"/>
/// so the boost arithmetic under test is the one that ships.</para>
///
/// <para><b>Every number asserted here was executed on this machine</b> through a net10.0 harness over the
/// built assemblies before the first CI run — the brief's rule for pure-logic pins.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetKnobsTests
{
    private const string QuietServerName = "darling-pg-target-knobs-quiet";
    private static readonly int QuietServerId = ServerIdHelper.GetDeterministicHashCode(QuietServerName);
    private const string ChurnServerName = "darling-pg-target-knobs-churn";
    private static readonly int ChurnServerId = ServerIdHelper.GetDeterministicHashCode(ChurnServerName);

    /* ───────────────────────── scorer: the two knobs and the convention checks ───────────────────────── */

    [Theory]
    [InlineData(128.0, 0.4)]     // the initdb default
    [InlineData(8.0, 0.4)]       // the boot_val fallback — at or below the line
    [InlineData(129.0, 0.0)]
    [InlineData(4096.0, 0.0)]
    public void SharedBuffers_ScoresTheAdvisoryBase_AtOrBelowTheInitdbDefault(double mb, double expected)
    {
        Assert.Equal(expected, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigSharedBuffers, mb)));
    }

    [Theory]
    [InlineData(1024.0, 0.4)]    // the shipped default, 1 GB
    [InlineData(512.0, 0.4)]
    [InlineData(1025.0, 0.0)]
    [InlineData(16384.0, 0.0)]
    public void MaxWalSize_ScoresTheAdvisoryBase_AtOrBelowTheShippedDefault(double mb, double expected)
    {
        Assert.Equal(expected, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigMaxWalSize, mb)));
    }

    [Fact]
    public void TheConventionChecks_ScoreExactlyAtTheirCompiledDefaults_AndTheContextKeysNever()
    {
        Assert.Equal(0.4, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigEffectiveCacheSize, 4096)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigEffectiveCacheSize, 2048)));   // set lower on purpose
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigEffectiveCacheSize, 49152)));
        Assert.Equal(0.4, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigRandomPageCost, 4.0)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigRandomPageCost, 1.1)));
        Assert.Equal(0.4, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigTrackIoTiming, 0)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigTrackIoTiming, 1)));

        /* Context: value-bearing, never a root. */
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigCheckpointTimeout, 300)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigWalCompression, 0)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigMaxConnections, 100)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigSuperuserReserved, 3)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ServerMajorVersion, 18)));
        /* The evidence-gated keys are 0 without their stamp (D5); the vacuum family's posture arm is graded in
           PgTargetBetweenWavesTests. */
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigWorkMem, 4)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigMaintWorkMem, 64)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigAutovacuumOff, 0)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigReservedConnections, 2)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Config("PG_PROBE", 99)));
    }

    [Fact]
    public void TheConfigBars_AreTheEnginesOwnShippedValues()
    {
        Assert.Equal(128, PgTargetScorer.SharedBuffersInitdbDefaultMb);
        Assert.Equal(1024, PgTargetScorer.MaxWalSizeDefaultMb);
        Assert.Equal(4096, PgTargetScorer.EffectiveCacheSizeDefaultMb);
        Assert.Equal(4.0, PgTargetScorer.RandomPageCostDefault);
        Assert.Equal(0.4, PgTargetScorer.ConfigAdvisoryBase);
        /* The pure convention keys have no amplifier arm at all (D5: never amplified) — through the shipped
           ScoreAll, beside every co-fire this lane knows, none of them gains an amplifier result or a boost. */
        var conventions = new[]
        {
            Config(PgTargetFactKeys.ConfigEffectiveCacheSize, 4096),
            Config(PgTargetFactKeys.ConfigRandomPageCost, 4.0),
            Config(PgTargetFactKeys.ConfigTrackIoTiming, 0),
        };
        var facts = conventions.Concat([Pressure(1.0, 240), Buffer(missShare: 0.5, blocksPerSec: 500)]).ToList();
        new FactScorer().ScoreAll(facts);
        Assert.All(conventions, f => Assert.Empty(f.AmplifierResults));
        Assert.All(conventions, f => Assert.Equal(0.4, f.Severity, precision: 9));
    }

    /* ───────────────────────── scorer: checkpoint pressure ───────────────────────── */

    [Theory]
    [InlineData(0.0, 48, 0.0)]
    [InlineData(0.07, 240, 0.0)]      // one nightly bulk load: below the majority line the fact has not FIRED (D5 — never a fraction)
    [InlineData(0.25, 48, 0.0)]
    [InlineData(0.49, 100, 0.0)]
    [InlineData(0.5, 48, 0.5)]        // the majority line
    [InlineData(0.7, 48, 0.75)]
    [InlineData(0.9, 48, 1.0)]        // near-total
    [InlineData(1.0, 240, 1.0)]
    [InlineData(1.0, 2, 0.0)]         // two checkpoints: no resolution — a single manual CHECKPOINT would read as total pressure
    [InlineData(1.0, 3, 1.0)]         // three is the floor
    public void CheckpointPressure_GradesTheRequestedShare_FromTheMajorityLine_AboveAMinimumCount(double share, double total, double expected)
    {
        var fact = Pressure(share, total);
        Assert.Equal(expected, PgTargetScorer.ScoreBase(fact), precision: 9);
    }

    [Fact]
    public void CheckpointPressure_StampsUnmeasuredLineage_WhenGraded_AndOtherWriteKeysScoreZero()
    {
        var graded = Pressure(0.6, 48);
        PgTargetScorer.ScoreBase(graded);
        Assert.Equal(0, graded.Metadata["threshold_lineage"]);

        var belowFloor = Pressure(1.0, 2);
        PgTargetScorer.ScoreBase(belowFloor);
        Assert.False(belowFloor.Metadata.ContainsKey("threshold_lineage"));

        Assert.Equal(0.0, PgTargetScorer.ScoreBase(new Fact { Source = PgTargetSources.WriteSource, Key = PgTargetFactKeys.WalVolumeShift, Value = 5 }));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(new Fact { Source = PgTargetSources.WriteSource, Key = "PG_PROBE", Value = 99 }));
    }

    /* ───────────────────────── scorer: the buffer composite ───────────────────────── */

    [Fact]
    public void BufferComposite_HitArm_GradesTheMissShare_AboveTheBlockRateFloor()
    {
        Assert.Equal(0.5, PgTargetScorer.ScoreBase(Buffer(missShare: 0.10, blocksPerSec: 500)), precision: 9);
        Assert.Equal(0.75, PgTargetScorer.ScoreBase(Buffer(missShare: 0.30, blocksPerSec: 500)), precision: 9);
        Assert.Equal(1.0, PgTargetScorer.ScoreBase(Buffer(missShare: 0.50, blocksPerSec: 500)), precision: 9);
        /* Below the concerning line the arm is 0, never the formula's fraction — a 5 % miss share is a healthy
           cache, and BaseSeverity > 0 is the co-fire predicate (D5). */
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Buffer(missShare: 0.05, blocksPerSec: 500)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Buffer(missShare: 0.01, blocksPerSec: 50_000)));
        /* A trickle of blocks: the hit-ratio arm is not graded, whatever the share. */
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Buffer(missShare: 0.90, blocksPerSec: 10)));
    }

    [Fact]
    public void BufferComposite_HitArm_IsSuppressedOnAurora_AndTheFactSaysSo()
    {
        var aurora = Buffer(missShare: 0.90, blocksPerSec: 5000, suppressed: true);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(aurora));
        Assert.Equal(0, aurora.Metadata["arms_graded"]);
        Assert.Equal(0, aurora.Metadata["threshold_lineage"]);
    }

    [Fact]
    public void BufferComposite_EvictionArm_GradesCacheTurnover_OnlyWhenTrackedAndTheKnobIsKnown()
    {
        /* PG 16+, shared_buffers known: turnover graded. */
        Assert.Equal(0.5, PgTargetScorer.ScoreBase(Buffer(missShare: 0, blocksPerSec: 0, evictionsTracked: true, turnoversPerHour: 1.0)), precision: 9);
        Assert.Equal(1.0, PgTargetScorer.ScoreBase(Buffer(missShare: 0, blocksPerSec: 0, evictionsTracked: true, turnoversPerHour: 10.0)), precision: 9);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Buffer(missShare: 0, blocksPerSec: 0, evictionsTracked: true, turnoversPerHour: 0.5)));
        /* Below PG 16 the arm is structurally absent: evictions_tracked = 0 and no turnover, however the
           collector might have stamped one — the flag wins. */
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Buffer(missShare: 0, blocksPerSec: 0, evictionsTracked: false, turnoversPerHour: 10.0)));
        /* PG 16+ but no config snapshot: no denominator, arm not graded. */
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Buffer(missShare: 0, blocksPerSec: 0, evictionsTracked: true, turnoversPerHour: null)));
    }

    [Fact]
    public void BufferComposite_BgwriterArm_GradesTheHaltShare_OnlyWhenTrackedAndTheDelayIsKnown()
    {
        Assert.Equal(0.5, PgTargetScorer.ScoreBase(Buffer(missShare: 0, blocksPerSec: 0, bgwriterTracked: true, haltShare: 0.10)), precision: 9);
        Assert.Equal(1.0, PgTargetScorer.ScoreBase(Buffer(missShare: 0, blocksPerSec: 0, bgwriterTracked: true, haltShare: 0.50)), precision: 9);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Buffer(missShare: 0, blocksPerSec: 0, bgwriterTracked: true, haltShare: 0.05)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Buffer(missShare: 0, blocksPerSec: 0, bgwriterTracked: false, haltShare: 0.50)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Buffer(missShare: 0, blocksPerSec: 0, bgwriterTracked: true, haltShare: null)));
    }

    [Fact]
    public void BufferComposite_IsTheMaxOfItsGradedArms_NotTheirSum_AndCountsThem()
    {
        /* Three arms each at their concerning line: 0.5, not 1.5 — the D2 collapse. */
        var all = Buffer(missShare: 0.10, blocksPerSec: 500, evictionsTracked: true, turnoversPerHour: 1.0, bgwriterTracked: true, haltShare: 0.10);
        Assert.Equal(0.5, PgTargetScorer.ScoreBase(all), precision: 9);
        Assert.Equal(3, all.Metadata["arms_graded"]);

        /* The strongest arm carries it; the two below their lines contribute 0, not a fraction. */
        var oneHot = Buffer(missShare: 0.02, blocksPerSec: 500, evictionsTracked: true, turnoversPerHour: 10.0, bgwriterTracked: true, haltShare: 0.01);
        Assert.Equal(1.0, PgTargetScorer.ScoreBase(oneHot), precision: 9);

        /* Every arm evaluated and every arm under its line: 0, three arms graded — the fact exists as context. */
        var healthy = Buffer(missShare: 0.02, blocksPerSec: 500, evictionsTracked: true, turnoversPerHour: 0.1, bgwriterTracked: true, haltShare: 0.01);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(healthy));
        Assert.Equal(3, healthy.Metadata["arms_graded"]);

        /* Below PG 16 on stock: two legs, and the fact says two. */
        var pg15 = Buffer(missShare: 0.10, blocksPerSec: 500, evictionsTracked: false, turnoversPerHour: null, bgwriterTracked: true, haltShare: 0.01);
        Assert.Equal(0.5, PgTargetScorer.ScoreBase(pg15), precision: 9);
        Assert.Equal(2, pg15.Metadata["arms_graded"]);

        Assert.Equal(0.0, PgTargetScorer.ScoreBase(new Fact { Source = PgTargetSources.BufferSource, Key = "PG_PROBE", Value = 99 }));
    }

    /* ───────────────────────── D5: the knob reaches 0.5 ONLY on a co-fire, through the shipped ScoreAll ───────────────────────── */

    [Fact]
    public void SharedBuffers_ReachesTheIncidentLine_OnlyWhenTheCompositeFires_NotWhenItMerelyExists()
    {
        var alone = Config(PgTargetFactKeys.ConfigSharedBuffers, 128);
        new FactScorer().ScoreAll([alone]);
        Assert.Equal(0.4, alone.Severity, precision: 9);
        Assert.True(alone.Severity < 0.5);

        /* The composite present but healthy (base 0): the knob stays an advisory. */
        var knob = Config(PgTargetFactKeys.ConfigSharedBuffers, 128);
        var healthy = Buffer(missShare: 0.01, blocksPerSec: 500);
        new FactScorer().ScoreAll([knob, healthy]);
        Assert.Equal(0.0, healthy.BaseSeverity);
        Assert.Equal(0.4, knob.Severity, precision: 9);
        Assert.Single(knob.AmplifierResults);
        Assert.False(knob.AmplifierResults[0].Matched);

        /* The composite FIRES: exactly the incident line. */
        var knob2 = Config(PgTargetFactKeys.ConfigSharedBuffers, 128);
        var short2 = Buffer(missShare: 0.10, blocksPerSec: 500);
        new FactScorer().ScoreAll([knob2, short2]);
        Assert.True(short2.BaseSeverity > 0);
        Assert.Equal(0.5, knob2.Severity, precision: 9);
        Assert.True(knob2.Severity >= 0.5);
        Assert.True(knob2.AmplifierResults.Single().Matched);
        /* And the composite is corroborated by the knob at its default. */
        Assert.Equal(0.5 * 1.3, short2.Severity, precision: 9);

        /* A knob NOT at the default is 0 whatever the composite does. */
        var sized = Config(PgTargetFactKeys.ConfigSharedBuffers, 8192);
        var short3 = Buffer(missShare: 0.50, blocksPerSec: 500);
        new FactScorer().ScoreAll([sized, short3]);
        Assert.Equal(0.0, sized.Severity);
        Assert.Equal(1.0, short3.Severity, precision: 9);   // no cause boost: the knob did not fire
    }

    [Fact]
    public void MaxWalSize_ReachesTheIncidentLine_OnlyWhenCheckpointPressureFires_NotWhenItMerelyExists()
    {
        var alone = Config(PgTargetFactKeys.ConfigMaxWalSize, 1024);
        new FactScorer().ScoreAll([alone]);
        Assert.Equal(0.4, alone.Severity, precision: 9);

        /* Checkpoints ran on the clock (0 % requested): the pressure fact exists at base 0; the knob stays 0.4. */
        var knob = Config(PgTargetFactKeys.ConfigMaxWalSize, 1024);
        var onTheClock = Pressure(0.0, 48);
        new FactScorer().ScoreAll([knob, onTheClock]);
        Assert.Equal(0.0, onTheClock.BaseSeverity);
        Assert.Equal(0.4, knob.Severity, precision: 9);

        /* One nightly bulk load (7 % requested): still on the clock overall; the knob stays 0.4. */
        var knob1 = Config(PgTargetFactKeys.ConfigMaxWalSize, 1024);
        var bulkLoad = Pressure(0.07, 288);
        new FactScorer().ScoreAll([knob1, bulkLoad]);
        Assert.Equal(0.0, bulkLoad.BaseSeverity);
        Assert.Equal(0.4, knob1.Severity, precision: 9);

        var knob2 = Config(PgTargetFactKeys.ConfigMaxWalSize, 1024);
        var churn = Pressure(1.0, 240);
        new FactScorer().ScoreAll([knob2, churn]);
        Assert.Equal(0.5, knob2.Severity, precision: 9);
        Assert.Equal(1.0 * 1.3, churn.Severity, precision: 9);   // cause boost matched, WAL-shift (v2) not
        Assert.Equal(2, churn.AmplifierResults.Count);
        Assert.True(churn.AmplifierResults[0].Matched);
        Assert.False(churn.AmplifierResults[1].Matched);
    }

    [Fact]
    public void TheCoFireBoost_IsExactlyTheOneThatLandsOnTheIncidentLine()
    {
        Assert.Equal(0.25, PgTargetScorer.KnobCoFireBoost);
        Assert.Equal(0.5, PgTargetScorer.ConfigAdvisoryBase * (1.0 + PgTargetScorer.KnobCoFireBoost), precision: 12);
        Assert.True(PgTargetScorer.ConfigAdvisoryBase * (1.0 + PgTargetScorer.KnobCoFireBoost) >= 0.5);
    }

    /* ───────────────────────── graph + inference: the stories ───────────────────────── */

    [Fact]
    public void AQuietTargetAtDefaults_RootsTwoAdvisoryCards_EachStandingAlone_NeverAnIncident()
    {
        var facts = new List<Fact>
        {
            Config(PgTargetFactKeys.ConfigSharedBuffers, 128),
            Config(PgTargetFactKeys.ConfigMaxWalSize, 1024),
            Config(PgTargetFactKeys.ConfigCheckpointTimeout, 300),
            Pressure(0.0, 48),                                  // checkpoints on the clock
            Buffer(missShare: 0.01, blocksPerSec: 500),         // a healthy cache
        };
        new FactScorer().ScoreAll(facts);
        var stories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts);

        Assert.Equal(2, stories.Count);
        Assert.All(stories, s => Assert.Equal(0.4, s.Severity, precision: 9));
        Assert.All(stories, s => Assert.Single(s.Path));
        Assert.All(stories, s => Assert.False(s.IsAbsolution));
        Assert.Equal(
            new[] { PgTargetFactKeys.ConfigMaxWalSize, PgTargetFactKeys.ConfigSharedBuffers },
            stories.Select(s => s.RootFactKey).OrderBy(k => k, StringComparer.Ordinal));
        Assert.All(stories, s => Assert.Equal(PgTargetSources.ConfigSource, s.Category));
    }

    [Fact]
    public void ATargetUnderCheckpointChurn_TellsTheStory_PressureToTheKnob()
    {
        var facts = new List<Fact>
        {
            Config(PgTargetFactKeys.ConfigSharedBuffers, 128),
            Config(PgTargetFactKeys.ConfigMaxWalSize, 1024),
            Pressure(0.95, 240),
        };
        new FactScorer().ScoreAll(facts);
        var stories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts);

        var churn = Assert.Single(stories, s => s.RootFactKey == PgTargetFactKeys.CheckpointPressure);
        Assert.Equal($"{PgTargetFactKeys.CheckpointPressure} → {PgTargetFactKeys.ConfigMaxWalSize}", churn.StoryPath);
        Assert.Equal(PgTargetFactKeys.ConfigMaxWalSize, churn.LeafFactKey);
        Assert.Equal(0.95, churn.RootFactValue, precision: 9);
        Assert.True(churn.Severity >= 0.5);
        Assert.Equal(PgTargetSources.WriteSource, churn.Category);

        /* The knob is consumed by the story — no second card for it — and shared_buffers still roots its own. */
        Assert.DoesNotContain(stories, s => s.RootFactKey == PgTargetFactKeys.ConfigMaxWalSize);
        Assert.Contains(stories, s => s.RootFactKey == PgTargetFactKeys.ConfigSharedBuffers && s.Path.Count == 1);
        Assert.Equal(2, stories.Count);
    }

    [Fact]
    public void ATargetWithAShortCache_TellsTheStory_CompositeToTheKnob()
    {
        var facts = new List<Fact>
        {
            Config(PgTargetFactKeys.ConfigSharedBuffers, 128),
            Buffer(missShare: 0.30, blocksPerSec: 2000, evictionsTracked: true, turnoversPerHour: 4.0),
        };
        new FactScorer().ScoreAll(facts);
        var stories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts);

        var story = Assert.Single(stories);
        Assert.Equal($"{PgTargetFactKeys.BufferCachePressure} → {PgTargetFactKeys.ConfigSharedBuffers}", story.StoryPath);
        Assert.True(story.Severity >= 0.5);
    }

    [Fact]
    public void TheEdges_ReadTheDestinationsVerdict_NeverASizeOfTheirOwn()
    {
        var graph = new PgTargetRelationshipGraph();

        /* A sized knob (base 0) is not a destination, however hard the pressure. */
        var sized = new Dictionary<string, Fact>(StringComparer.Ordinal)
        {
            [PgTargetFactKeys.CheckpointPressure] = Scored(Pressure(1.0, 240)),
            [PgTargetFactKeys.ConfigMaxWalSize] = Scored(Config(PgTargetFactKeys.ConfigMaxWalSize, 16384)),
            [PgTargetFactKeys.BufferCachePressure] = Scored(Buffer(missShare: 0.5, blocksPerSec: 500)),
            [PgTargetFactKeys.ConfigSharedBuffers] = Scored(Config(PgTargetFactKeys.ConfigSharedBuffers, 8192)),
        };
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.CheckpointPressure, sized));
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.BufferCachePressure, sized));

        var atDefault = new Dictionary<string, Fact>(StringComparer.Ordinal)
        {
            [PgTargetFactKeys.CheckpointPressure] = Scored(Pressure(1.0, 240)),
            [PgTargetFactKeys.ConfigMaxWalSize] = Scored(Config(PgTargetFactKeys.ConfigMaxWalSize, 1024)),
            [PgTargetFactKeys.BufferCachePressure] = Scored(Buffer(missShare: 0.5, blocksPerSec: 500)),
            [PgTargetFactKeys.ConfigSharedBuffers] = Scored(Config(PgTargetFactKeys.ConfigSharedBuffers, 128)),
        };
        Assert.Equal(PgTargetFactKeys.ConfigMaxWalSize, Assert.Single(graph.GetActiveEdges(PgTargetFactKeys.CheckpointPressure, atDefault)).Destination);
        Assert.Equal(PgTargetFactKeys.ConfigSharedBuffers, Assert.Single(graph.GetActiveEdges(PgTargetFactKeys.BufferCachePressure, atDefault)).Destination);

        /* The v2 leading edge is declared and inert until the shift fact exists. */
        var shiftEdges = graph.GetAllEdges(PgTargetFactKeys.WalVolumeShift);
        Assert.Equal(PgTargetFactKeys.CheckpointPressure, Assert.Single(shiftEdges).Destination);
        Assert.Equal(PgTargetFactKeys.ConfigMaxWalSize, Assert.Single(graph.GetActiveEdges(PgTargetFactKeys.CheckpointPressure, atDefault)).Destination);
    }

    /* ───────────────────────── advice: value-stated, counter-objective named, honest about gaps ───────────────────────── */

    [Fact]
    public void CheckpointPressureAdvice_StatesTheMeasuredShare_TheWalBytesPerInterval_AndTheTimersOwnCount()
    {
        var pressure = Pressure(0.9, 240, requested: 216, timed: 24);
        pressure.Metadata["wal_tracked"] = 1;
        pressure.Metadata["wal_bytes"] = 15.0 * 1024 * 1024 * 1024;
        pressure.Metadata["wal_bytes_per_sec"] = 1.0 * 1024 * 1024;
        pressure.Metadata["wal_bytes_per_checkpoint"] = 64.0 * 1024 * 1024;
        pressure.Metadata["checkpoint_timeout_s"] = 300;
        pressure.Metadata["expected_timed_checkpoints"] = 48;
        pressure.Metadata["checkpoint_write_ms"] = 120_000;
        pressure.Metadata["checkpoint_sync_ms"] = 6_000;
        var knob = Config(PgTargetFactKeys.ConfigMaxWalSize, 1024);
        knob.Metadata["checkpoint_timeout_s"] = 300;

        var advice = PgTargetAdvice.Compose(PgTargetFactKeys.CheckpointPressure, Lookup(pressure, knob));
        Assert.NotNull(advice);
        Assert.Contains("216 of 240 checkpoints (90 %)", advice!.Headline, StringComparison.Ordinal);
        Assert.Contains("216 requested", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("24 timed", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("15 GB of WAL", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("1 MB/s", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("64 MB per checkpoint interval", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("checkpoint_timeout is 5 min", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("about 48", advice.Investigation, StringComparison.Ordinal);
        /* Sizing is the server's arithmetic: 1 MB/s × 300 s = 300 MB per timeout, against the stated 1 GB. */
        Assert.Contains("300 MB of WAL; max_wal_size is 1 GB", advice.Remediation, StringComparison.Ordinal);
        /* Counter-objectives named. */
        Assert.Contains("crash recovery", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("pg_wal", advice.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void CheckpointPressureAdvice_OnAurora_SaysWalVolumeIsNotReported_RatherThanPrintingZero()
    {
        var pressure = Pressure(1.0, 240, requested: 240, timed: 0);
        pressure.Metadata["wal_tracked"] = 0;
        var advice = PgTargetAdvice.Compose(PgTargetFactKeys.CheckpointPressure, Lookup(pressure))!;
        Assert.Contains("pg_stat_wal is not implemented on Aurora", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("0 B of WAL", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("not reported on this flavour", advice.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void MaxWalSizeAdvice_StatesTheValueRead_AndWhetherTheExhaustionSignalCoFired()
    {
        var knob = Config(PgTargetFactKeys.ConfigMaxWalSize, 1024);
        var quiet = PgTargetAdvice.Compose(PgTargetFactKeys.ConfigMaxWalSize, Lookup(knob))!;
        Assert.Equal("max_wal_size is 1 GB — the shipped default", quiet.Headline);
        Assert.Contains("convention finding", quiet.Investigation, StringComparison.Ordinal);
        Assert.Contains("max_wal_size is 1 GB", quiet.Remediation, StringComparison.Ordinal);

        var sized = Config(PgTargetFactKeys.ConfigMaxWalSize, 8192);
        Assert.Equal("max_wal_size is 8 GB", PgTargetAdvice.Compose(PgTargetFactKeys.ConfigMaxWalSize, Lookup(sized))!.Headline);

        var pressure = Scored(Pressure(0.8, 100, requested: 80, timed: 20));
        pressure.Metadata["wal_tracked"] = 1;
        pressure.Metadata["wal_bytes"] = 2.0 * 1024 * 1024 * 1024;
        pressure.Metadata["wal_bytes_per_sec"] = 512.0 * 1024;
        var coFired = PgTargetAdvice.Compose(PgTargetFactKeys.ConfigMaxWalSize, Lookup(knob, pressure))!;
        Assert.Contains("80 of 100 checkpoints (80 %) were requested", coFired.Investigation, StringComparison.Ordinal);
        Assert.Contains("2 GB of WAL", coFired.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("convention finding", coFired.Investigation, StringComparison.Ordinal);

        /* The static shape (no facts): names the default without a fabricated number for the server. */
        var stat = PgTargetAdvice.Static(PgTargetFactKeys.ConfigMaxWalSize)!;
        Assert.Contains("shipped default of 1 GB", stat.Headline, StringComparison.Ordinal);
    }

    [Fact]
    public void SharedBuffersAdvice_StatesTheValueRead_TheComponentsThatFired_AndTheCounterObjective()
    {
        var knob = Config(PgTargetFactKeys.ConfigSharedBuffers, 128);
        var quiet = PgTargetAdvice.Compose(PgTargetFactKeys.ConfigSharedBuffers, Lookup(knob))!;
        Assert.Equal("shared_buffers is 128 MB — the initdb default", quiet.Headline);
        Assert.Contains("convention finding", quiet.Investigation, StringComparison.Ordinal);
        Assert.Contains("quarter of the host's RAM", quiet.Remediation, StringComparison.Ordinal);
        Assert.Contains("restart", quiet.Remediation, StringComparison.Ordinal);
        Assert.Contains("page cache", quiet.Remediation, StringComparison.Ordinal);   // the counter-objective

        var composite = Scored(Buffer(missShare: 0.30, blocksPerSec: 2000, evictionsTracked: true, turnoversPerHour: 4.0, bgwriterTracked: true, haltShare: 0.2));
        composite.Metadata["blocks_total"] = 28_800_000;
        composite.Metadata["evictions"] = 262_144;
        composite.Metadata["evictions_per_sec"] = 18.2;
        composite.Metadata["shared_buffers_bytes"] = 128.0 * 1024 * 1024;
        composite.Metadata["maxwritten_clean"] = 14_400;
        composite.Metadata["buffers_clean"] = 1_440_000;
        composite.Metadata["buffers_alloc"] = 2_000_000;
        var coFired = PgTargetAdvice.Compose(PgTargetFactKeys.ConfigSharedBuffers, Lookup(knob, composite))!;
        Assert.Contains("measured, not presumed", coFired.Investigation, StringComparison.Ordinal);
        Assert.Contains("30 % were served from outside shared_buffers", coFired.Investigation, StringComparison.Ordinal);
        Assert.Contains("128 MB cache replaced about 4 times per hour", coFired.Investigation, StringComparison.Ordinal);
        Assert.Contains("20 % of its rounds", coFired.Investigation, StringComparison.Ordinal);
        Assert.Contains("Graded on 3 arms", coFired.Investigation, StringComparison.Ordinal);

        Assert.Equal("shared_buffers is 8 GB", PgTargetAdvice.Compose(PgTargetFactKeys.ConfigSharedBuffers, Lookup(Config(PgTargetFactKeys.ConfigSharedBuffers, 8192)))!.Headline);
        Assert.Contains("initdb default of 128 MB", PgTargetAdvice.Static(PgTargetFactKeys.ConfigSharedBuffers)!.Headline, StringComparison.Ordinal);
    }

    [Fact]
    public void BufferCompositeAdvice_SaysWhichArmsWereStructurallyAbsent_OnThisFlavourAndMajor()
    {
        /* PG 15 stock: no pg_stat_io. */
        var pg15 = Scored(Buffer(missShare: 0.20, blocksPerSec: 800, evictionsTracked: false, turnoversPerHour: null, bgwriterTracked: true, haltShare: 0.05));
        var advice15 = PgTargetAdvice.Compose(PgTargetFactKeys.BufferCachePressure, Lookup(pg15))!;
        Assert.StartsWith("Buffer cache under pressure — 20 % of block requests left shared_buffers", advice15.Headline, StringComparison.Ordinal);
        Assert.Contains("pg_stat_io arrived in PostgreSQL 16", advice15.Investigation, StringComparison.Ordinal);
        Assert.Contains("rests on the hit ratio and the bgwriter only", advice15.Investigation, StringComparison.Ordinal);

        /* Aurora 16: hit ratio stated, not graded; evictions carry the headline. */
        var aurora = Scored(Buffer(missShare: 0.40, blocksPerSec: 800, suppressed: true, evictionsTracked: true, turnoversPerHour: 3.0));
        var adviceAurora = PgTargetAdvice.Compose(PgTargetFactKeys.BufferCachePressure, Lookup(aurora))!;
        Assert.StartsWith("Buffer cache under pressure — shared_buffers turned over 3 times per hour", adviceAurora.Headline, StringComparison.Ordinal);
        Assert.Contains("not graded on Aurora", adviceAurora.Investigation, StringComparison.Ordinal);
        Assert.Contains("40 % were served from outside shared_buffers", adviceAurora.Investigation, StringComparison.Ordinal);

        /* PG 13: neither pg_stat_io nor pg_write_stats. */
        var pg13 = Scored(Buffer(missShare: 0.20, blocksPerSec: 800, evictionsTracked: false, turnoversPerHour: null, bgwriterTracked: false, haltShare: null));
        var advice13 = PgTargetAdvice.Compose(PgTargetFactKeys.BufferCachePressure, Lookup(pg13))!;
        Assert.Contains("pg_write_stats needs PostgreSQL 14+", advice13.Investigation, StringComparison.Ordinal);
        Assert.Contains("Graded on 1 arm.", advice13.Investigation, StringComparison.Ordinal);

        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.BufferCachePressure));
    }

    [Fact]
    public void TheConventionCards_StateTheValueRead_AndTheStaticShapeNamesTheDefault()
    {
        Assert.Equal("effective_cache_size is 4 GB — the compiled default",
            PgTargetAdvice.Compose(PgTargetFactKeys.ConfigEffectiveCacheSize, Lookup(Config(PgTargetFactKeys.ConfigEffectiveCacheSize, 4096)))!.Headline);
        Assert.Equal("random_page_cost is 4.0 — the spinning-disk default",
            PgTargetAdvice.Compose(PgTargetFactKeys.ConfigRandomPageCost, Lookup(Config(PgTargetFactKeys.ConfigRandomPageCost, 4.0)))!.Headline);
        Assert.Equal("random_page_cost is 1.1",
            PgTargetAdvice.Compose(PgTargetFactKeys.ConfigRandomPageCost, Lookup(Config(PgTargetFactKeys.ConfigRandomPageCost, 1.1)))!.Headline);
        Assert.StartsWith("track_io_timing is off",
            PgTargetAdvice.Compose(PgTargetFactKeys.ConfigTrackIoTiming, Lookup(Config(PgTargetFactKeys.ConfigTrackIoTiming, 0)))!.Headline, StringComparison.Ordinal);
        Assert.Equal("checkpoint_timeout is 5 min",
            PgTargetAdvice.Compose(PgTargetFactKeys.ConfigCheckpointTimeout, Lookup(Config(PgTargetFactKeys.ConfigCheckpointTimeout, 300)))!.Headline);

        foreach (var key in new[]
                 {
                     PgTargetFactKeys.ConfigEffectiveCacheSize, PgTargetFactKeys.ConfigRandomPageCost, PgTargetFactKeys.ConfigTrackIoTiming,
                     PgTargetFactKeys.ConfigCheckpointTimeout, PgTargetFactKeys.ConfigWalCompression, PgTargetFactKeys.WalVolumeShift,
                 })
        {
            Assert.NotNull(PgTargetAdvice.Static(key));
        }

        /* The context keys other lanes own compose nothing here. */
        Assert.Null(PgTargetAdvice.Static(PgTargetFactKeys.ConfigMaxConnections));
        Assert.Null(PgTargetAdvice.Static(PgTargetFactKeys.ConfigSuperuserReserved));
    }

    /// <summary>
    /// Two house rules over EVERY block this lane composes, in every shape: no <c>CREATE INDEX</c> (D8), and
    /// <c>effective_cache_size</c> named only in its own card — the design's adversarial pass (§6 D) says
    /// treating the planner hint as a memory figure is the way v1 embarrasses itself.
    /// </summary>
    [Fact]
    public void NoKnobFamilyBlock_CarriesDdl_OrTreatsEffectiveCacheSizeAsMemory()
    {
        var pressure = Scored(Pressure(0.9, 240, requested: 216, timed: 24));
        pressure.Metadata["wal_tracked"] = 1;
        pressure.Metadata["wal_bytes"] = 1e9;
        pressure.Metadata["wal_bytes_per_sec"] = 1e6;
        var composite = Scored(Buffer(missShare: 0.30, blocksPerSec: 2000, evictionsTracked: true, turnoversPerHour: 4.0, bgwriterTracked: true, haltShare: 0.2));
        var facts = Lookup(
            Config(PgTargetFactKeys.ConfigSharedBuffers, 128), Config(PgTargetFactKeys.ConfigMaxWalSize, 1024),
            Config(PgTargetFactKeys.ConfigEffectiveCacheSize, 4096), Config(PgTargetFactKeys.ConfigRandomPageCost, 4.0),
            Config(PgTargetFactKeys.ConfigTrackIoTiming, 0), Config(PgTargetFactKeys.ConfigCheckpointTimeout, 300),
            Config(PgTargetFactKeys.ConfigWalCompression, 0), pressure, composite);

        var keys = new[]
        {
            PgTargetFactKeys.ConfigSharedBuffers, PgTargetFactKeys.ConfigMaxWalSize, PgTargetFactKeys.ConfigEffectiveCacheSize,
            PgTargetFactKeys.ConfigRandomPageCost, PgTargetFactKeys.ConfigTrackIoTiming, PgTargetFactKeys.ConfigCheckpointTimeout,
            PgTargetFactKeys.ConfigWalCompression, PgTargetFactKeys.CheckpointPressure, PgTargetFactKeys.BufferCachePressure,
            PgTargetFactKeys.WalVolumeShift,
        };

        foreach (var key in keys)
        {
            foreach (var block in new[] { PgTargetAdvice.Compose(key, facts)!, PgTargetAdvice.Static(key)! })
            {
                var text = block.Headline + "\n" + block.Investigation + "\n" + block.Remediation;
                Assert.DoesNotContain("CREATE INDEX", text, StringComparison.OrdinalIgnoreCase);
                Assert.Null(block.RemediationTsql);
                if (key != PgTargetFactKeys.ConfigEffectiveCacheSize)
                    Assert.DoesNotContain("effective_cache_size", text, StringComparison.OrdinalIgnoreCase);
            }
        }
    }

    /* ───────────────────────── the collector SQL, by its own text ───────────────────────── */

    [Fact]
    public void TheConfigRead_IsTheShippedReadersShape_AnchoredAtOrBeforeTheWindowEnd()
    {
        var sql = PgTargetFactCollector.PgTargetConfigSnapshotSql;
        Assert.Contains("FROM pg_server_config", sql, StringComparison.Ordinal);
        /* The reader's three-value exclusion, spelled inline exactly as the reader spells it. */
        Assert.Contains("NOT IN (" + DarlingPgServerConfigReader.SessionScopedSources + ")", sql, StringComparison.Ordinal);
        /* The latest snapshot AT OR BEFORE the window end — so a historical window states the setting that applied then. */
        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $2", sql, StringComparison.Ordinal);
        /* PostgreSQL's own verdict, never a text comparison against boot_val. */
        Assert.Contains("(coalesce(c.source, 'default') = 'default') AS is_default", sql, StringComparison.Ordinal);
        foreach (var name in new[]
                 {
                     "shared_buffers", "max_wal_size", "min_wal_size", "effective_cache_size", "random_page_cost", "track_io_timing",
                     "checkpoint_timeout", "checkpoint_completion_target", "wal_compression", "bgwriter_delay", "bgwriter_lru_maxpages",
                     "max_connections", "superuser_reserved_connections", "reserved_connections", "work_mem", "maintenance_work_mem", "autovacuum",
                 })
        {
            Assert.Contains($"'{name}'", sql, StringComparison.Ordinal);
        }

        Assert.Contains(sql, PgTargetFactCollector.AllSql);
    }

    [Fact]
    public void TheCounterReads_CarryTheResetAwareDifferencing_Verbatim()
    {
        foreach (var sql in new[] { PgTargetFactCollector.PgTargetCheckpointSql, PgTargetFactCollector.PgTargetBufferCompositeSql })
        {
            Assert.Contains("ROW_NUMBER() OVER series > 1", sql, StringComparison.Ordinal);
            Assert.Contains("IS DISTINCT FROM LAG(", sql, StringComparison.Ordinal);
            Assert.Contains("GREATEST(", sql, StringComparison.Ordinal);
            Assert.Contains("WINDOW series AS", sql, StringComparison.Ordinal);
            Assert.Contains(sql, PgTargetFactCollector.AllSql);
        }

        var write = PgTargetFactCollector.PgTargetCheckpointSql;
        Assert.Contains("FROM pg_write_stats", write, StringComparison.Ordinal);
        Assert.Contains("checkpointer_stats_reset IS DISTINCT FROM LAG(checkpointer_stats_reset)", write, StringComparison.Ordinal);
        Assert.Contains("wal_stats_reset IS DISTINCT FROM LAG(wal_stats_reset)", write, StringComparison.Ordinal);
        Assert.Contains("(wal_bytes IS NOT NULL) AS wal_tracked", write, StringComparison.Ordinal);

        var buffer = PgTargetFactCollector.PgTargetBufferCompositeSql;
        Assert.Contains("FROM pg_database_stats", buffer, StringComparison.Ordinal);
        Assert.Contains("FROM pg_io_stats", buffer, StringComparison.Ordinal);
        Assert.Contains("FROM pg_write_stats", buffer, StringComparison.Ordinal);
        Assert.Contains("PARTITION BY database_name", buffer, StringComparison.Ordinal);
        Assert.Contains("PARTITION BY backend_type, object_type, context", buffer, StringComparison.Ordinal);
        /* Only shared_buffers evictions: the ring-buffer contexts recycle by design. */
        Assert.Contains("object_type = 'relation'", buffer, StringComparison.Ordinal);
        Assert.Contains("context = 'normal'", buffer, StringComparison.Ordinal);
        Assert.Contains("(evictions IS NOT NULL) AS evictions_tracked", buffer, StringComparison.Ordinal);
        Assert.Contains("(maxwritten_clean IS NOT NULL) AS bgwriter_tracked", buffer, StringComparison.Ordinal);
    }

    /* ───────────────────────── gated: the exit criterion through the real tool ───────────────────────── */

    [Fact]
    public async Task AQuietTargetAtDefaults_ReturnsTwoAdvisories_AndAChurningOne_TellsThePressureStoryWithTheMeasuredNumbers()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the knobs e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, QuietServerId, QuietServerName, "postgres", 18, ct);
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, ChurnServerId, ChurnServerName, "postgres", 18, ct);

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-4);

            foreach (var (id, name) in new[] { (QuietServerId, QuietServerName), (ChurnServerId, ChurnServerName) })
            {
                /* The span (one row 25 h back) and the coverage series, flat counters — the plumbing shape. */
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, id, name, windowEnd.AddHours(-25), ct);
                for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                    await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, id, name, windowStart.AddMinutes(minute - 1), ct);

                /* The two knobs at their shipped defaults; the convention checks deliberately NOT at theirs, so
                   exactly two advisories root on the quiet server. */
                await PlantConfigSnapshotAsync(connection, id, name, windowEnd.AddMinutes(-30), ct);
            }

            /* Churn: one requested checkpoint and 64 MB of WAL every minute, the timer never fires. */
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
            {
                await PlantWriteStatsAsync(connection, ChurnServerId, ChurnServerName, windowStart.AddMinutes(minute - 1),
                    requested: 1000 + minute, timed: 500, walBytes: 1_000_000_000_000L + minute * 64L * 1024 * 1024, ct);
            }

            var service = new DarlingAnalysisService(postgres);
            /* Anchored at the planted window's end (as_of), so the window is exactly the 241 planted rows — 240
               deltas, 14 400 observed seconds — and every number below is arithmetic, not timing. */
            var asOf = windowEnd.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture);

            /* ── the quiet target: two CONFIG_PG_* advisories at 0.4, each standing alone. */
            var quiet = await DarlingMcpTools.AnalyzeServer(service, postgres, QuietServerName, 4, as_of: asOf);
            using (var doc = JsonDocument.Parse(quiet))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                Assert.Equal(2, findings.Count);
                var roots = findings.Select(f => f.GetProperty("root_fact").GetProperty("key").GetString()).OrderBy(k => k, StringComparer.Ordinal).ToArray();
                Assert.Equal(new[] { PgTargetFactKeys.ConfigMaxWalSize, PgTargetFactKeys.ConfigSharedBuffers }, roots);
                foreach (var finding in findings)
                {
                    Assert.Equal(0.4, finding.GetProperty("severity").GetDouble(), precision: 6);
                    Assert.Equal(1, finding.GetProperty("fact_count").GetInt32());
                    Assert.DoesNotContain("→", finding.GetProperty("story_path").GetString()!, StringComparison.Ordinal);
                }

                var sharedBuffers = findings.Single(f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.ConfigSharedBuffers);
                Assert.Equal(128, sharedBuffers.GetProperty("root_fact").GetProperty("value").GetDouble(), precision: 6);
                Assert.Equal("shared_buffers is 128 MB — the initdb default", sharedBuffers.GetProperty("advice").GetProperty("headline").GetString());
                var maxWal = findings.Single(f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.ConfigMaxWalSize);
                Assert.Equal(1024, maxWal.GetProperty("root_fact").GetProperty("value").GetDouble(), precision: 6);
                Assert.Equal("max_wal_size is 1 GB — the shipped default", maxWal.GetProperty("advice").GetProperty("headline").GetString());
            }

            /* ── the churning target: PG_CHECKPOINT_PRESSURE → CONFIG_PG_MAX_WAL_SIZE with the measured numbers. */
            var churn = await DarlingMcpTools.AnalyzeServer(service, postgres, ChurnServerName, 4, as_of: asOf);
            using (var doc = JsonDocument.Parse(churn))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                var story = findings.Single(f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.CheckpointPressure);
                Assert.Equal($"{PgTargetFactKeys.CheckpointPressure} → {PgTargetFactKeys.ConfigMaxWalSize}", story.GetProperty("story_path").GetString());
                Assert.Equal(1.0, story.GetProperty("root_fact").GetProperty("value").GetDouble(), precision: 6);
                Assert.True(story.GetProperty("severity").GetDouble() >= 0.5);
                Assert.Equal(PgTargetFactKeys.ConfigMaxWalSize, story.GetProperty("leaf_fact").GetProperty("key").GetString());

                var advice = story.GetProperty("advice");
                Assert.Equal("Checkpoints are being forced by WAL volume — 240 of 240 checkpoints (100 %) were requested, not timed", advice.GetProperty("headline").GetString());
                var investigation = advice.GetProperty("investigation").GetString()!;
                Assert.Contains("240 requested", investigation, StringComparison.Ordinal);
                Assert.Contains("0 timed", investigation, StringComparison.Ordinal);
                /* 240 × 64 MB = 15 GB over 4 h = 1.07 MB/s; 64 MB per checkpoint; the timer would have run 48. */
                Assert.Contains("15 GB of WAL", investigation, StringComparison.Ordinal);
                Assert.Contains("1.1 MB/s", investigation, StringComparison.Ordinal);
                Assert.Contains("64 MB per checkpoint interval", investigation, StringComparison.Ordinal);
                Assert.Contains("checkpoint_timeout is 5 min", investigation, StringComparison.Ordinal);
                Assert.Contains("about 48", investigation, StringComparison.Ordinal);

                /* The knob is consumed by the story; shared_buffers still roots its own advisory. */
                Assert.DoesNotContain(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.ConfigMaxWalSize);
                Assert.Contains(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.ConfigSharedBuffers);
                Assert.Equal(2, findings.Count);
                /* The next-tools for the story are PostgreSQL reads. */
                Assert.Contains(story.GetProperty("next_tools").EnumerateArray(), t => t.GetProperty("tool").GetString() == "get_pg_write_stats");
            }

            /* ── the facts read: the pressure fact carries its unmeasured-lineage stamp and both amplifiers. */
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ChurnServerName, 4, PgTargetSources.WriteSource, as_of: asOf);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var fact = Assert.Single(doc.RootElement.GetProperty("facts").EnumerateArray());
                Assert.Equal(PgTargetFactKeys.CheckpointPressure, fact.GetProperty("key").GetString());
                var metadata = fact.GetProperty("metadata");
                Assert.Equal(0, metadata.GetProperty("threshold_lineage").GetDouble());
                Assert.Equal(240, metadata.GetProperty("checkpoints_requested").GetDouble());
                Assert.Equal(1, metadata.GetProperty("wal_tracked").GetDouble());
                Assert.Equal(48, metadata.GetProperty("expected_timed_checkpoints").GetDouble(), precision: 2);
                Assert.Equal(1024.0 * 1024 * 1024, metadata.GetProperty("max_wal_size_bytes").GetDouble());
                var amplifiers = fact.GetProperty("amplifiers").EnumerateArray().ToList();
                Assert.Equal(2, amplifiers.Count);
                Assert.True(amplifiers[0].GetProperty("matched").GetBoolean());     // the knob at its default
                Assert.False(amplifiers[1].GetProperty("matched").GetBoolean());    // PG_WAL_VOLUME_SHIFT: v2, inert
            }

            /* ── and the buffer composite is SILENT on flat counters — the plumbing e2e's single-fact pin survives. */
            var bufferJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, QuietServerName, 4, PgTargetSources.BufferSource, as_of: asOf);
            using (var doc = JsonDocument.Parse(bufferJson))
            {
                Assert.Equal(0, doc.RootElement.GetProperty("shown").GetInt32());
                /* And the whole pass emitted exactly the registry fact plus the twelve config facts one snapshot
                   yields: nothing from pg_write (no rows planted) and nothing from pg_buffer (flat counters). */
                Assert.Equal(13, doc.RootElement.GetProperty("total_facts").GetInt32());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ───────────────────────── fixtures ───────────────────────── */

    private static Fact Config(string key, double value) =>
        new() { Source = PgTargetSources.ConfigSource, Key = key, Value = value, ServerId = 1 };

    private static Fact Pressure(double share, double total, double? requested = null, double? timed = null)
    {
        var req = requested ?? Math.Round(share * total);
        var tim = timed ?? (total - req);
        return new Fact
        {
            Source = PgTargetSources.WriteSource,
            Key = PgTargetFactKeys.CheckpointPressure,
            Value = share,
            ServerId = 1,
            Metadata =
            {
                ["checkpoints_requested"] = req,
                ["checkpoints_timed"] = tim,
                ["checkpoints_total"] = total,
                ["requested_share"] = share,
            },
        };
    }

    private static Fact Buffer(
        double missShare, double blocksPerSec, bool suppressed = false,
        bool evictionsTracked = false, double? turnoversPerHour = null,
        bool bgwriterTracked = false, double? haltShare = null)
    {
        var fact = new Fact
        {
            Source = PgTargetSources.BufferSource,
            Key = PgTargetFactKeys.BufferCachePressure,
            Value = missShare,
            ServerId = 1,
            Metadata =
            {
                ["miss_share"] = missShare,
                ["hit_ratio"] = 1 - missShare,
                ["block_requests_per_sec"] = blocksPerSec,
                ["blocks_total"] = blocksPerSec * 14_400,
                ["hit_ratio_suppressed"] = suppressed ? 1 : 0,
                ["evictions_tracked"] = evictionsTracked ? 1 : 0,
                ["bgwriter_tracked"] = bgwriterTracked ? 1 : 0,
            },
        };
        if (turnoversPerHour is not null) fact.Metadata["cache_turnovers_per_hour"] = turnoversPerHour.Value;
        if (haltShare is not null) fact.Metadata["bgwriter_halt_share"] = haltShare.Value;
        return fact;
    }

    /// <summary>Base-scored through the real scorer, so an edge or an advice block sees the verdict it would in a pass.</summary>
    private static Fact Scored(Fact fact)
    {
        fact.BaseSeverity = PgTargetScorer.ScoreBase(fact);
        fact.Severity = fact.BaseSeverity;
        return fact;
    }

    private static IReadOnlyDictionary<string, Fact> Lookup(params Fact[] facts) =>
        facts.ToDictionary(f => f.Key, f => f, StringComparer.Ordinal);

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    /// <summary>
    /// One <c>pg_server_config</c> snapshot as the collector writes it: the two knobs at their shipped defaults
    /// (<c>shared_buffers 16384 × 8kB</c>, <c>max_wal_size 1024 MB</c>), the checkpoint rhythm at its default, and
    /// the three convention checks deliberately tuned so they root nothing — the e2e asserts EXACTLY two advisories.
    /// A <c>source = 'session'</c> row for <c>work_mem</c> is planted too, to prove the exclusion filters it.
    /// </summary>
    private static async Task PlantConfigSnapshotAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, CancellationToken ct)
    {
        var rows = new (string Name, string Setting, string? Unit, string BootVal, string Source)[]
        {
            ("shared_buffers", "16384", "8kB", "1024", "configuration file"),
            ("max_wal_size", "1024", "MB", "1024", "configuration file"),
            ("min_wal_size", "80", "MB", "80", "configuration file"),
            ("effective_cache_size", "1048576", "8kB", "524288", "configuration file"),   // 8 GB: tuned
            ("random_page_cost", "1.1", null, "4", "configuration file"),                 // SSD: tuned
            ("track_io_timing", "on", null, "off", "configuration file"),                 // on: tuned
            ("checkpoint_timeout", "300", "s", "300", "default"),
            ("checkpoint_completion_target", "0.9", null, "0.9", "default"),
            ("wal_compression", "off", null, "off", "default"),
            ("bgwriter_delay", "200", "ms", "200", "default"),
            ("bgwriter_lru_maxpages", "100", null, "100", "default"),
            ("max_connections", "100", null, "100", "configuration file"),
            ("superuser_reserved_connections", "3", null, "3", "default"),
            ("work_mem", "4096", "kB", "4096", "default"),
            ("work_mem", "1048576", "kB", "4096", "session"),                             // the collector's own session: excluded
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
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(serverName);
            command.Parameters.AddWithValue(row.Name);
            command.Parameters.AddWithValue(row.Setting);
            command.Parameters.Add(new NpgsqlParameter { Value = (object?)row.Unit ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            command.Parameters.AddWithValue(row.Source);
            command.Parameters.AddWithValue(row.BootVal);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>One <c>pg_write_stats</c> row as the PG 18 collector writes it: cumulative counters, bgwriter flat,
    /// the three reset stamps NULL (never reset — the common state).</summary>
    private static async Task PlantWriteStatsAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime at, long requested, long timed, long walBytes, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_write_stats
    (collection_id, collection_time, server_id, server_name,
     num_timed, num_requested, num_done, checkpoint_write_time_ms, checkpoint_sync_time_ms, buffers_written_checkpoint, checkpointer_stats_reset,
     buffers_clean, maxwritten_clean, buffers_alloc, buffers_backend, buffers_backend_fsync, bgwriter_stats_reset,
     wal_records, wal_fpi, wal_bytes, wal_buffers_full, wal_write, wal_sync, wal_write_time_ms, wal_sync_time_ms, wal_stats_reset)
VALUES ($1, $2, $3, $4,
        $5, $6, $5 + $6, 0, 0, 0, NULL,
        0, 0, 0, NULL, NULL, NULL,
        0, 0, $7, 0, NULL, NULL, NULL, NULL, NULL)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(timed);
        command.Parameters.AddWithValue(requested);
        command.Parameters.Add(new NpgsqlParameter { Value = (decimal)walBytes, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Numeric });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id IN ({QuietServerId}, {ChurnServerId}); " +
            $"DELETE FROM pg_write_stats WHERE server_id IN ({QuietServerId}, {ChurnServerId}); " +
            $"DELETE FROM pg_server_config WHERE server_id IN ({QuietServerId}, {ChurnServerId}); " +
            $"DELETE FROM analysis_findings WHERE server_id IN ({QuietServerId}, {ChurnServerId}); " +
            $"DELETE FROM analysis_muted WHERE server_id IN ({QuietServerId}, {ChurnServerId}); " +
            $"DELETE FROM servers WHERE server_id IN ({QuietServerId}, {ChurnServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
