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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1553 (Tier 2 of the 24-server field-incident response): the bounded-parallel fire-and-track sweep's
/// pure, testable seams — the cadence-jitter phase offset and the hardcoded concurrency cap. The stateful
/// pieces (Retired containment, the outer-thread-owned skip-log) live in a private async body with heavy
/// dependencies and are asserted by full-diff review, not contorted into unit tests (the plan's simplicity
/// rule); these two seams are the ones with a clean, dependency-free surface.
/// </summary>
public sealed class DarlingSweepSchedulingTests
{
    /* The cadence periods the jitter is applied against (seconds): 30/60/300/1800 span the collector
       frequencies, 150 is the fixed post-connect analysis window (the plan's jitter site 3). Exactly the
       spread Section C item 1 pins. */
    private static readonly int[] Periods = { 30, 60, 150, 300, 1800 };

    /// <summary>
    /// A spread of realistic, FNV-shaped server ids — the SAME derivation the loop keys on
    /// (<see cref="ServerIdHelper.GetDeterministicHashCode"/>, a process-independent FNV-1a hash), over real
    /// server-name shapes (boxes, named instances, Azure SQL DB storage names). Full signed-int range: roughly
    /// half are negative, which is exactly why <see cref="DarlingWorker.CadencePhaseOffset"/> casts to uint.
    /// </summary>
    private static readonly int[] RealisticServerIds =
        new[]
        {
            "PROD-SQL-01",
            "PROD-SQL-02",
            "PROD-SQL-03",
            "SQL2022-CORE\\INSTANCE1",
            "SQL2019-RPT\\REPORTING",
            "azure-eastus.database.windows.net:SalesDb",
            "azure-eastus.database.windows.net:InventoryDb",
            "10.0.14.72",
            "10.0.14.73:RO",
            "hammerdb-bench-01",
            "dr-secondary-west",
            "analytics-warehouse-primary",
        }
        .Select(ServerIdHelper.GetDeterministicHashCode)
        .ToArray();

    /// <summary>
    /// Deterministic and restart-stable: the offset is a PURE function of (id, period) with no
    /// <see cref="Random"/> and no process-specific state, so a service restart re-derives the SAME phase for
    /// the same server and does not re-herd the fleet (the whole point of the jitter). Repeated calls must be
    /// byte-identical; the ids themselves come from the process-independent FNV helper.
    /// </summary>
    [Fact]
    public void CadencePhaseOffset_IsDeterministic_ForSameInputs()
    {
        foreach (var id in RealisticServerIds)
        {
            foreach (var period in Periods)
            {
                var first = DarlingWorker.CadencePhaseOffset(id, period);
                var second = DarlingWorker.CadencePhaseOffset(id, period);
                Assert.Equal(first, second);
            }
        }
    }

    /// <summary>
    /// Pins the exact formula <c>(uint)serverId % periodSeconds</c> seconds, INCLUDING the uint cast that keeps a
    /// NEGATIVE FNV id (GetDeterministicHashCode returns a signed int) mapped into [0, period). A signed modulo
    /// would produce a negative offset and pull the collector's first due time into the PAST — re-herding it
    /// exactly like no jitter at all. Boundary ids exercise the cast directly.
    /// </summary>
    [Fact]
    public void CadencePhaseOffset_IsUnsignedModulo_MappingNegativeIdsIntoRange()
    {
        int[] boundaryIds = { 0, 1, 61, -1, -61, -12345, int.MaxValue, int.MinValue };
        foreach (var id in boundaryIds)
        {
            foreach (var period in Periods)
            {
                var expected = TimeSpan.FromSeconds((uint)id % period);
                var actual = DarlingWorker.CadencePhaseOffset(id, period);
                Assert.Equal(expected, actual);
                /* A negative id must never yield a negative (past) offset. */
                Assert.True(actual >= TimeSpan.Zero, $"offset for id {id}, period {period} must be non-negative");
            }
        }
    }

    /// <summary>
    /// Bounded within [0, period) for every realistic FNV id and every period: the phase can never push a due
    /// time a full cadence out (that would skip a cycle) nor before now (that would re-herd). This is the only
    /// "spread" property the plan pins — deliberately NOT a distribution or collision pin (pigeonhole makes
    /// those unachievable and brittle; reviewed out twice).
    /// </summary>
    [Fact]
    public void CadencePhaseOffset_IsBounded_WithinZeroInclusiveToPeriodExclusive()
    {
        foreach (var id in RealisticServerIds)
        {
            foreach (var period in Periods)
            {
                var offset = DarlingWorker.CadencePhaseOffset(id, period);
                Assert.True(offset >= TimeSpan.Zero, $"offset for id {id}, period {period} must be >= 0");
                Assert.True(offset < TimeSpan.FromSeconds(period), $"offset for id {id}, period {period} must be < the period");
            }
        }
    }

    /// <summary>
    /// The herd is actually broken: across the ≥10 distinct realistic ids, the offsets for a given period are
    /// NOT all identical (at least two distinct phases). This is the minimal "de-herded" pin — again NOT a
    /// distribution claim, just that a single shared instant is not what the function produces.
    /// </summary>
    [Fact]
    public void CadencePhaseOffset_IsNotAllIdentical_AcrossDistinctIds_PerPeriod()
    {
        Assert.True(RealisticServerIds.Length >= 10, "need ≥10 distinct ids to make the not-all-identical pin meaningful");

        foreach (var period in Periods)
        {
            var distinctOffsets = RealisticServerIds
                .Select(id => DarlingWorker.CadencePhaseOffset(id, period))
                .Distinct()
                .Count();

            Assert.True(distinctOffsets > 1, $"period {period}: all {RealisticServerIds.Length} ids phased to the same instant — the fleet would still fire in lockstep");
        }
    }

    /// <summary>
    /// A non-positive period yields no offset (TimeSpan.Zero) rather than a divide-by-zero or an undefined
    /// modulo — guards the on-load / recompute call sites where a frequency could in principle be zero and keeps
    /// the function total for callers.
    /// </summary>
    [Fact]
    public void CadencePhaseOffset_NonPositivePeriod_IsZero()
    {
        foreach (var id in RealisticServerIds)
        {
            Assert.Equal(TimeSpan.Zero, DarlingWorker.CadencePhaseOffset(id, 0));
            Assert.Equal(TimeSpan.Zero, DarlingWorker.CadencePhaseOffset(id, -1));
            Assert.Equal(TimeSpan.Zero, DarlingWorker.CadencePhaseOffset(id, -3600));
        }
    }

    /// <summary>
    /// The bounded per-server sweep concurrency is pinned at 4 (a cheap drift tripwire). Hardcoded by design —
    /// defaults-over-config, no control-plane knob: 4 clears a 24-server worst case in ~6 waves while the 120s
    /// analysis budget stays de-clustered by the cadence jitter (the plan's N=4 rationale). A change here is a
    /// deliberate capacity decision, not an incidental edit, so it must break this test first.
    /// </summary>
    [Fact]
    public void MaxConcurrentServerSweeps_IsFour()
    {
        Assert.Equal(4, DarlingWorker.MaxConcurrentServerSweeps);
    }

    /// <summary>The watchdog threshold both channels are judged against (drift tripwire).</summary>
    [Fact]
    public void SweepWatchdogSeconds_Is60()
    {
        Assert.Equal(60, DarlingWorker.SweepWatchdogSeconds);
    }

    /// <summary>
    /// THE regression case. A body that has been QUEUED behind the N=4 gate for ten minutes has not started
    /// executing, so it is capacity pressure — never a hang. Attributing gate queue time to the hang channel is
    /// what fired ~82 warnings/hour at 24 servers on a demonstrably healthy fleet (0 collector errors, data
    /// landing), burying the one signal that warning exists to raise.
    /// </summary>
    [Fact]
    public void ClassifySweepEpisode_LongQueuedBody_IsCapacityNotAHang()
    {
        var signal = DarlingWorker.ClassifySweepEpisode(
            episodeSeconds: 600, running: false, runningSeconds: 0, alreadyWarned: false, alreadyQueuedInfo: false);

        Assert.Equal(DarlingWorker.SweepEpisodeSignal.Queued, signal);
    }

    /// <summary>
    /// The precise field shape: a body queued ~5 minutes behind the gate that then ran for 3 seconds. The
    /// EPISODE is long (303s) but execution is trivial — nothing is stalled. The old measurement (episode only)
    /// called this a hang; it must now be silent, because by the time the watchdog looks the body is running
    /// fine and its queue wait was already reported on the capacity channel.
    /// </summary>
    [Fact]
    public void ClassifySweepEpisode_LongQueueThenFastExecution_IsSilent()
    {
        var signal = DarlingWorker.ClassifySweepEpisode(
            episodeSeconds: 303, running: true, runningSeconds: 3, alreadyWarned: false, alreadyQueuedInfo: false);

        Assert.Equal(DarlingWorker.SweepEpisodeSignal.None, signal);
    }

    /// <summary>A body genuinely EXECUTING past the threshold is the real stall the watchdog exists for.</summary>
    [Fact]
    public void ClassifySweepEpisode_RunningPastThreshold_IsAHang()
    {
        var signal = DarlingWorker.ClassifySweepEpisode(
            episodeSeconds: 75, running: true, runningSeconds: 61, alreadyWarned: false, alreadyQueuedInfo: false);

        Assert.Equal(DarlingWorker.SweepEpisodeSignal.Hang, signal);
    }

    /// <summary>Neither channel fires under its threshold.</summary>
    [Theory]
    [InlineData(59, true, 59)]    // executing, just under
    [InlineData(59, false, 0)]    // queued, just under
    public void ClassifySweepEpisode_UnderThreshold_IsSilent(double episodeSeconds, bool running, double runningSeconds)
    {
        var signal = DarlingWorker.ClassifySweepEpisode(
            episodeSeconds, running, runningSeconds, alreadyWarned: false, alreadyQueuedInfo: false);

        Assert.Equal(DarlingWorker.SweepEpisodeSignal.None, signal);
    }

    /// <summary>
    /// Each channel latches independently, once per episode — a long stall is flagged once, not every 15s sweep,
    /// and the same for a long queue wait. The latches must not cross-suppress: a body warned for a hang has
    /// nothing to say on the capacity channel and vice versa.
    /// </summary>
    [Fact]
    public void ClassifySweepEpisode_EachChannelLatchesOncePerEpisode()
    {
        Assert.Equal(
            DarlingWorker.SweepEpisodeSignal.None,
            DarlingWorker.ClassifySweepEpisode(75, running: true, runningSeconds: 61, alreadyWarned: true, alreadyQueuedInfo: false));

        Assert.Equal(
            DarlingWorker.SweepEpisodeSignal.None,
            DarlingWorker.ClassifySweepEpisode(600, running: false, runningSeconds: 0, alreadyWarned: false, alreadyQueuedInfo: true));

        /* A queued-latched body that later starts running can still raise a genuine hang. */
        Assert.Equal(
            DarlingWorker.SweepEpisodeSignal.Hang,
            DarlingWorker.ClassifySweepEpisode(700, running: true, runningSeconds: 90, alreadyWarned: false, alreadyQueuedInfo: true));
    }

    /// <summary>
    /// #1556 working-set launch guard: below the threshold the fleet may launch new collection bodies;
    /// at or above it, launches are held so the process backs away from the commit-limit exhaustion the
    /// field incident hit. The 0.80 fraction is exercised directly at, just below, and just above the line.
    /// </summary>
    [Fact]
    public void ShouldLaunchSweeps_GatesOnEightyPercentOfAvailable()
    {
        const long available = 16L * 1024 * 1024 * 1024; /* 16 GB */
        var thresholdBytes = DarlingWorker.MemoryGuardFraction * available;
        var margin = (long)(available * 0.01); /* 1% (~160 MB) — clear of any float rounding at the line. */

        /* Comfortably under the line → launch. */
        Assert.True(DarlingWorker.ShouldLaunchSweeps(1L * 1024 * 1024 * 1024, available));
        Assert.True(DarlingWorker.ShouldLaunchSweeps((long)thresholdBytes - margin, available));

        /* Comfortably over the line → hold new launches so in-flight bodies drain. */
        Assert.False(DarlingWorker.ShouldLaunchSweeps((long)thresholdBytes + margin, available));
        Assert.False(DarlingWorker.ShouldLaunchSweeps(15L * 1024 * 1024 * 1024, available));
    }

    /// <summary>
    /// An unknown memory budget (a non-positive available figure — GC could not report it) must NEVER block
    /// collection: the guard is a backstop, not a hard gate, so it fails open.
    /// </summary>
    [Fact]
    public void ShouldLaunchSweeps_UnknownBudget_NeverBlocks()
    {
        Assert.True(DarlingWorker.ShouldLaunchSweeps(long.MaxValue, 0));
        Assert.True(DarlingWorker.ShouldLaunchSweeps(long.MaxValue, -1));
    }

    /// <summary>
    /// Drift tripwire: the launch-guard fraction is 0.80. It is the fleet-level backstop against the exact
    /// commit-limit blowout of the field incident, so a change here is a deliberate capacity decision that
    /// must break this test first (the MaxConcurrentServerSweeps rationale, applied to the memory guard).
    /// </summary>
    [Fact]
    public void MemoryGuardFraction_IsEightyPercent()
    {
        Assert.Equal(0.80, DarlingWorker.MemoryGuardFraction);
    }

    /* ---------------- #1575 watermark seed policy (ComputeSeededNextDue / SeedJitter) ---------------- */

    /* A fixed reference "now" so the pure decision-table is deterministic (no wall-clock). Kind=Utc mirrors the
       DateTime.UtcNow the connect/reload path passes; the policy compares by ticks so the label is not load-bearing. */
    private static readonly DateTime Now = new(2026, 07, 18, 10, 30, 0, DateTimeKind.Utc);

    /* index_object_stats' 1440-minute (daily) cadence — the collector #1575's starvation was observed on. */
    private const int DailyMinutes = 1440;

    /// <summary>
    /// A daily collector that ran an hour ago waits out the REMAINING interval — nextDue ≈ lastRun + 24h (≈ 23h
    /// out), NOT now. This is the core of the fix: a restart must not re-phase a recently-run daily collector up
    /// to a full interval forward; it resumes the real cadence from the watermark.
    /// </summary>
    [Fact]
    public void ComputeSeededNextDue_DailyRecentlyRun_WaitsRemainingInterval_NotNow()
    {
        var lastRun = Now.AddHours(-1);
        var jitter = TimeSpan.FromSeconds(50);

        var nextDue = DarlingWorker.ComputeSeededNextDue(lastRun, DailyMinutes, Now, jitter);

        Assert.Equal(lastRun.AddMinutes(DailyMinutes), nextDue);        /* exactly lastRun + 24h */
        Assert.Equal(Now.AddHours(23), nextDue);                        /* ≈ 23h from now */
        Assert.NotEqual(Now + jitter, nextDue);                         /* NOT the prompt-run stamp */
    }

    /// <summary>
    /// A daily collector overdue by 3h (last ran 27h ago) runs PROMPTLY — nextDue = now + a small jitter, NOT
    /// another full 24h out. Without the watermark seed this collector was skipped for up to a day per restart.
    /// </summary>
    [Fact]
    public void ComputeSeededNextDue_DailyOverdue_RunsPromptlyWithJitter()
    {
        var lastRun = Now.AddHours(-27);
        var jitter = TimeSpan.FromSeconds(50);

        var nextDue = DarlingWorker.ComputeSeededNextDue(lastRun, DailyMinutes, Now, jitter);

        Assert.Equal(Now + jitter, nextDue);                            /* prompt (jittered), not lastRun + 24h */
        Assert.True(nextDue <= Now.AddSeconds(150), "an overdue collector must run within the jitter window, not another interval out");
    }

    /// <summary>
    /// A 1-minute collector that ran 30s ago keeps the unchanged feel — nextDue ≈ lastRun + 1min (≈ 30s out).
    /// Minute collectors were never the bug (their old full-interval offset was &lt; 60s); this pins that the new
    /// policy leaves them behaving identically.
    /// </summary>
    [Fact]
    public void ComputeSeededNextDue_OneMinuteRecentlyRun_WaitsRemainder()
    {
        var lastRun = Now.AddSeconds(-30);
        var jitter = TimeSpan.FromSeconds(10);

        var nextDue = DarlingWorker.ComputeSeededNextDue(lastRun, 1, Now, jitter);

        Assert.Equal(lastRun.AddMinutes(1), nextDue);                   /* lastRun + 1min == now + 30s */
        Assert.Equal(Now.AddSeconds(30), nextDue);
    }

    /// <summary>
    /// A never-run collector (null watermark) seeds at now + jitter — and for a DAILY collector the jitter is
    /// bounded to ≤ 150s, so a fresh install runs it shortly after first connect (then daily), NOT up to 24h
    /// later. Composes SeedJitter (the real caller's cap) with the policy across every realistic FNV id — the
    /// end-to-end fresh-install fix.
    /// </summary>
    [Fact]
    public void ComputeSeededNextDue_DailyNeverRun_SeedsWithinJitterWindow_NotAFullInterval()
    {
        foreach (var id in RealisticServerIds)
        {
            var jitter = DarlingWorker.SeedJitter(id, DailyMinutes * 60);
            var nextDue = DarlingWorker.ComputeSeededNextDue(lastRunUtc: null, DailyMinutes, Now, jitter);

            Assert.Equal(Now + jitter, nextDue);
            Assert.True(nextDue >= Now, $"id {id}: a never-run seed must not land in the past");
            Assert.True(nextDue < Now.AddSeconds(150), $"id {id}: a daily never-run collector must seed within ~150s, not up to a full interval");
        }
    }

    /// <summary>
    /// The overdue boundary is inclusive: when lastRun + interval == now EXACTLY, the collector is treated as
    /// overdue and runs promptly (now + jitter), never scheduled for the instant that just passed.
    /// </summary>
    [Fact]
    public void ComputeSeededNextDue_ExactlyDue_RunsPromptly()
    {
        var lastRun = Now.AddMinutes(-5);
        var jitter = TimeSpan.FromSeconds(7);

        var nextDue = DarlingWorker.ComputeSeededNextDue(lastRun, 5, Now, jitter);

        Assert.Equal(Now + jitter, nextDue);
    }

    /// <summary>
    /// The seed jitter is the deterministic phase CAPPED at min(intervalSeconds, 150) — so it de-clusters the
    /// fleet's overdue / never-run seeds within a bounded window and can NEVER defer a run by up to a full
    /// interval (the coarse full-interval offset was the #1575 bug). Pins the exact cap against every realistic
    /// FNV id across minute, 5-minute, hourly, and daily cadences.
    /// </summary>
    [Fact]
    public void SeedJitter_IsCappedAtMinIntervalAnd150Seconds()
    {
        int[] frequencyMinutes = { 1, 5, 60, DailyMinutes };
        foreach (var id in RealisticServerIds)
        {
            foreach (var minutes in frequencyMinutes)
            {
                var frequencySeconds = minutes * 60;
                var cap = Math.Min(frequencySeconds, 150);

                var jitter = DarlingWorker.SeedJitter(id, frequencySeconds);

                Assert.Equal(DarlingWorker.CadencePhaseOffset(id, cap), jitter);   /* exact formula */
                Assert.True(jitter >= TimeSpan.Zero, $"id {id}, {minutes}m: jitter must be non-negative");
                Assert.True(jitter < TimeSpan.FromSeconds(cap), $"id {id}, {minutes}m: jitter must be < min(interval, 150s)");
            }
        }
    }

    /// <summary>
    /// Deterministic and restart-stable per server id (a pure function of the FNV id, no Random) — a restart
    /// re-derives the SAME seed jitter and does not re-herd the fleet.
    /// </summary>
    [Fact]
    public void SeedJitter_IsDeterministic_ForSameInputs()
    {
        foreach (var id in RealisticServerIds)
        {
            Assert.Equal(DarlingWorker.SeedJitter(id, 60), DarlingWorker.SeedJitter(id, 60));
            Assert.Equal(DarlingWorker.SeedJitter(id, DailyMinutes * 60), DarlingWorker.SeedJitter(id, DailyMinutes * 60));
        }
    }

    /* ---------------- #1581 cold-start launch stagger (ColdStartFirstSweepDue) ---------------- */

    /* A fixed cold-start instant so the launch-time assertions are deterministic (no wall-clock). */
    private static readonly DateTime ColdStart = new(2026, 07, 20, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// Drift tripwire: the cold-start launch-spread window is 150s — the fixed post-connect analysis-phase
    /// window, reused so no new tuning knob is introduced. A change is a deliberate capacity decision that must
    /// break this test first (the MaxConcurrentServerSweeps rationale, applied to the launch spread).
    /// </summary>
    [Fact]
    public void ColdStartSpreadSeconds_Is150()
    {
        Assert.Equal(150, DarlingWorker.ColdStartSpreadSeconds);
    }

    /// <summary>
    /// The first-sweep launch offset is bounded within [0, ColdStartSpreadSeconds) for every realistic FNV id: it
    /// can never defer a server's FIRST post-startup catch-up beyond the small window (so steady-state cadence is
    /// untouched — the stagger only shifts the ONE launch), nor pull it before the captured startup instant (a
    /// negative offset would re-herd it exactly like no stagger). Exercises the same uint-cast safety the
    /// underlying CadencePhaseOffset guarantees, through the composition the seeding actually uses.
    /// </summary>
    [Fact]
    public void ColdStartFirstSweepDue_IsBounded_WithinSpreadWindow()
    {
        foreach (var id in RealisticServerIds)
        {
            var offset = DarlingWorker.ColdStartFirstSweepDue(ColdStart, id) - ColdStart;
            Assert.True(offset >= TimeSpan.Zero, $"id {id}: first-sweep offset must be >= 0 (never before startup)");
            Assert.True(offset < TimeSpan.FromSeconds(DarlingWorker.ColdStartSpreadSeconds),
                $"id {id}: first-sweep offset must be < the {DarlingWorker.ColdStartSpreadSeconds}s spread window");
        }
    }

    /// <summary>
    /// The herd is actually broken on cold start: across the ≥10 distinct realistic ids the first-sweep launch
    /// offsets are NOT all zero and NOT all identical — a service restart no longer launches every server's first
    /// catch-up body in the same sweep tick. Minimal de-herd pin (not a distribution claim), mirroring the
    /// CadencePhaseOffset one.
    /// </summary>
    [Fact]
    public void ColdStartFirstSweepDue_IsSpread_AcrossDistinctIds()
    {
        Assert.True(RealisticServerIds.Length >= 10, "need ≥10 distinct ids to make the spread pin meaningful");

        var offsets = RealisticServerIds
            .Select(id => DarlingWorker.ColdStartFirstSweepDue(ColdStart, id) - ColdStart)
            .ToArray();

        Assert.Contains(offsets, o => o > TimeSpan.Zero);   /* not all zero (every server launching at the base instant is still a herd) */
        Assert.True(offsets.Distinct().Count() > 1,
            "all servers' first-sweep launches phased to the same instant — the fleet would still herd on restart");
    }

    /// <summary>
    /// Deterministic and restart-stable: the first-sweep launch time is a PURE function of (coldStartInstant, id)
    /// with no Random, so re-deriving it for the same inputs yields the same instant. (The base instant differs
    /// per restart, which is correct — the OFFSET from that base is what must be stable, and it is.)
    /// </summary>
    [Fact]
    public void ColdStartFirstSweepDue_IsDeterministic_ForSameInputs()
    {
        foreach (var id in RealisticServerIds)
        {
            Assert.Equal(
                DarlingWorker.ColdStartFirstSweepDue(ColdStart, id),
                DarlingWorker.ColdStartFirstSweepDue(ColdStart, id));
        }
    }

    private static readonly DateTime GridOrigin = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan OneMinute = TimeSpan.FromSeconds(60);

    [Fact]
    public void ComputeNextDue_OnTimeRun_AdvancesOneInterval()
    {
        Assert.Equal(GridOrigin + OneMinute, DarlingWorker.ComputeNextDue(GridOrigin, GridOrigin, OneMinute));
    }

    [Fact]
    public void ComputeNextDue_LateRun_DoesNotCarryTheLateness()
    {
        var now = GridOrigin.AddSeconds(14);
        Assert.Equal(GridOrigin + OneMinute, DarlingWorker.ComputeNextDue(GridOrigin, now, OneMinute));
    }

    [Fact]
    public void ComputeNextDue_AfterStall_SkipsMissedSlotsToTheNextGridSlot()
    {
        var now = GridOrigin.AddMinutes(5).AddSeconds(20);
        Assert.Equal(GridOrigin.AddMinutes(6), DarlingWorker.ComputeNextDue(GridOrigin, now, OneMinute));
    }

    [Fact]
    public void ComputeNextDue_ExactBoundary_MovesToTheFollowingSlot()
    {
        var now = GridOrigin.AddMinutes(3);
        Assert.Equal(GridOrigin.AddMinutes(4), DarlingWorker.ComputeNextDue(GridOrigin, now, OneMinute));
    }

    /// <summary>
    /// Fifteen-second sweep ticks whose body starts 0-12 s late (seeded) drive a 1-minute collector for 60
    /// simulated minutes. The grid keeps the 60-second cadence; scheduling from the body start (the old rule)
    /// loses samples.
    /// </summary>
    [Fact]
    public void ComputeNextDue_TickSimulation_KeepsOneMinuteCadence_WhereBodyStartSchedulingDrifts()
    {
        var gridRuns = SimulateRuns((due, now) => DarlingWorker.ComputeNextDue(due, now, OneMinute));
        var oldRuns = SimulateRuns((_, now) => now + OneMinute);

        Assert.InRange(gridRuns, 59, 61);
        Assert.True(oldRuns < gridRuns, $"old rule {oldRuns} runs should be below grid {gridRuns}");
    }

    private static int SimulateRuns(Func<DateTime, DateTime, DateTime> advance)
    {
        var rng = new Random(4636);
        var due = GridOrigin;
        var runs = 0;
        for (var tick = TimeSpan.Zero; tick < TimeSpan.FromMinutes(60); tick += TimeSpan.FromSeconds(15))
        {
            var now = GridOrigin + tick + TimeSpan.FromSeconds(rng.Next(0, 13));
            if (now < due)
            {
                continue;
            }

            runs++;
            due = advance(due, now);
        }

        return runs;
    }
}
