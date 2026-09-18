/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2235: telling a key that is new TO US apart from a counter that is new to the WORLD.
///
/// <para><b>The defect.</b> <c>query_stats</c> keys its deltas on the full row identity, which includes
/// <c>plan_handle</c> — and <c>plan_handle</c> changes on every recompile. So a plan-churning statement
/// presents a fresh key on nearly every sighting, and a first sighting reports 0. On a production
/// replica that discarded most of the instance's CPU: a query Datadog measured at ~43% of an 8-vCPU box
/// read through these collectors as 18 executions and 2,824 ms over 168 hours, and the top-25 procedures
/// accounted for ~49M ms of roughly 498M core-ms available.</para>
///
/// <para><b>Why it was invisible, which is the worse half.</b> The calculator already had an honest path
/// for cache churn — the counter-reset branch reports <c>interval = 0</c> precisely so a reader can tell
/// a fabricated zero from an idle one (#2234, the same invariant <see cref="DeltaGapPolicyTests"/>
/// pins). But that branch needs the SAME key to reappear with a lower value, and a recompile never
/// does: it arrives under a new key and takes the baseline path instead. Same class of harm as the
/// 300-second gap policy #2233 replaced — it did not merely lose data, it invented quiet.</para>
///
/// <para><b>The rule.</b> The caller passes how old the counter series is; the calculator combines that
/// with its own record of when it last looked. If the series began since the previous pass, the whole
/// counter accrued inside that window and its baseline was 0 — so the delta is the full value, reported
/// with a real interval because it IS knowable. Otherwise nothing changes.</para>
///
/// <para>The age is an age and not a timestamp on purpose: a DMV <c>creation_time</c> is in the
/// monitored server's local time while collection times are UTC, so comparing them client-side is a
/// timezone bug on every server that is not UTC.</para>
/// </summary>
public sealed class DeltaSeriesAgeTests
{
    private const int ServerId = 1;
    private const string Collector = "query_stats_worker";
    private const int Gap = CollectorDeltaCalculator.DefaultMaxGapSeconds;

    private static DateTime T0 => new(2026, 8, 15, 12, 0, 0, DateTimeKind.Unspecified);
    private static DateTime T1 => T0.AddSeconds(60);

    /// <summary>
    /// THE FIX: a recompiled plan's counter is credited instead of silently reporting nothing.
    /// </summary>
    [Fact]
    public void ASeriesThatBeganSinceTheLastPassIsCreditedInFull()
    {
        var deltas = new CollectorDeltaCalculator();

        /* Pass one establishes when we last looked. */
        deltas.CalculateDeltaWithSeriesAge(ServerId, Collector, "sql:0:99:planA", 500, 10, out _, T0, Gap);

        /* Pass two: the statement recompiled, so this is a DIFFERENT plan_handle and therefore a key we
           have never seen — carrying 900 us accrued by a plan compiled 20 s ago, i.e. inside the 60 s
           since our last look. */
        var delta = deltas.CalculateDeltaWithSeriesAge(
            ServerId, Collector, "sql:0:99:planB", 900, seriesAgeSeconds: 20, out var interval, T1, Gap);

        Assert.Equal(900, delta);
        /* A real interval, because this delta is knowable — (0, 0) stays reserved for the cases that
           genuinely are not, which is what makes a stored zero readable at all. */
        Assert.Equal(60, interval);
    }

    /// <summary>
    /// A series OLDER than the gap is still refused. Most of that counter accrued before we were
    /// looking, so crediting it would invent work in this interval rather than merely lose some.
    /// </summary>
    [Fact]
    public void ASeriesOlderThanTheGapStaysUnknown()
    {
        var deltas = new CollectorDeltaCalculator();
        deltas.CalculateDeltaWithSeriesAge(ServerId, Collector, "k", 500, 10, out _, T0, Gap);

        var delta = deltas.CalculateDeltaWithSeriesAge(
            ServerId, Collector, "old", 999_999, seriesAgeSeconds: 3_600, out var interval, T1, Gap);

        Assert.Equal(0, delta);
        Assert.Equal(0, interval);
    }

    /// <summary>
    /// The gap policy still bounds it. A plan compiled during a two-hour outage is not two hours of
    /// work in the next minute — this is the inflated-spike guard, and the new path must not route
    /// around it.
    /// </summary>
    [Fact]
    public void AGapBeyondThePolicyRefusesToCreditAWholeCounter()
    {
        var deltas = new CollectorDeltaCalculator();
        deltas.CalculateDeltaWithSeriesAge(ServerId, Collector, "k", 500, 10, out _, T0, Gap);

        var delta = deltas.CalculateDeltaWithSeriesAge(
            ServerId, Collector, "fresh", 777_777, seriesAgeSeconds: 30, out var interval, T0.AddHours(2), Gap);

        Assert.Equal(0, delta);
        Assert.Equal(0, interval);
    }

    /// <summary>
    /// The first pass ever credits nothing: with no previous look there is no window to attribute a
    /// counter to, and a cold start must not dump every cached plan's lifetime into one interval.
    /// </summary>
    [Fact]
    public void TheFirstPassEverBaselinesRatherThanCrediting()
    {
        var deltas = new CollectorDeltaCalculator();

        var delta = deltas.CalculateDeltaWithSeriesAge(
            ServerId, Collector, "k", 12_345, seriesAgeSeconds: 5, out var interval, T0, Gap);

        Assert.Equal(0, delta);
        Assert.Equal(0, interval);
    }

    /// <summary>
    /// EVERY row of one pass measures against the same previous pass.
    ///
    /// <para>The trap this pins: a collector calls in once per row, so if the first row rolled the pass
    /// window forward, every later row in that same pass would compare against its own pass, see a zero
    /// gap, and be credited nothing. The fix would then work for exactly one row per cycle and look like
    /// it worked.</para>
    /// </summary>
    [Fact]
    public void EveryRowOfOnePassSeesTheSamePreviousPass()
    {
        var deltas = new CollectorDeltaCalculator();
        deltas.CalculateDeltaWithSeriesAge(ServerId, Collector, "seed", 1, 1, out _, T0, Gap);

        var first = deltas.CalculateDeltaWithSeriesAge(ServerId, Collector, "r1", 100, 5, out _, T1, Gap);
        var second = deltas.CalculateDeltaWithSeriesAge(ServerId, Collector, "r2", 200, 5, out _, T1, Gap);
        var third = deltas.CalculateDeltaWithSeriesAge(ServerId, Collector, "r3", 300, 5, out _, T1, Gap);

        Assert.Equal(100, first);
        Assert.Equal(200, second);
        Assert.Equal(300, third);
    }

    /// <summary>
    /// Passing no age is exactly the old behaviour, which is what lets the other forty-odd delta call
    /// sites stay untouched.
    /// </summary>
    [Fact]
    public void WithoutAnAgeTheBehaviourIsUnchanged()
    {
        var deltas = new CollectorDeltaCalculator();
        deltas.CalculateDeltaWithSeriesAge(ServerId, "perfmon", "k", 500, null, out _, T0, Gap);

        var delta = deltas.CalculateDeltaWithSeriesAge(
            ServerId, "perfmon", "newkey", 900, seriesAgeSeconds: null, out var interval, T1, Gap);

        Assert.Equal(0, delta);
        Assert.Equal(0, interval);

        /* And through the original entry point, which must not have changed at all. */
        var legacy = new CollectorDeltaCalculator();
        legacy.CalculateDeltaWithInterval(ServerId, "perfmon", "k", 500, out _, T0, Gap);
        var legacyDelta = legacy.CalculateDeltaWithInterval(ServerId, "perfmon", "newkey", 900, out var legacyInterval, T1, Gap);

        Assert.Equal(0, legacyDelta);
        Assert.Equal(0, legacyInterval);
    }

    /// <summary>Ordinary same-key subtraction is untouched, age supplied or not.</summary>
    [Fact]
    public void AnExistingKeyStillSubtracts()
    {
        var deltas = new CollectorDeltaCalculator();
        deltas.CalculateDeltaWithSeriesAge(ServerId, Collector, "k", 1_000, 5, out _, T0, Gap);

        var delta = deltas.CalculateDeltaWithSeriesAge(ServerId, Collector, "k", 1_750, 65, out var interval, T1, Gap);

        Assert.Equal(750, delta);
        Assert.Equal(60, interval);
    }

    /// <summary>
    /// A counter reset on the SAME key is still the honest (0, 0) — the age must not be read as
    /// permission to treat a decrease as a fresh series and credit the post-reset value.
    /// </summary>
    [Fact]
    public void AResetOnTheSameKeyIsStillUnknowable()
    {
        var deltas = new CollectorDeltaCalculator();
        deltas.CalculateDeltaWithSeriesAge(ServerId, Collector, "k", 1_000, 100, out _, T0, Gap);

        var delta = deltas.CalculateDeltaWithSeriesAge(ServerId, Collector, "k", 5, seriesAgeSeconds: 5, out var interval, T1, Gap);

        Assert.Equal(0, delta);
        Assert.Equal(0, interval);
    }

    /// <summary>
    /// <c>ClearServer</c> drops the pass window along with the baselines. Left behind, a re-added
    /// server's first pass would measure a series age against a look from before it was removed and
    /// credit a full counter to an interval that never happened.
    /// </summary>
    [Fact]
    public void ClearServerAlsoForgetsWhenWeLastLooked()
    {
        var deltas = new CollectorDeltaCalculator();
        deltas.CalculateDeltaWithSeriesAge(ServerId, Collector, "k", 100, 5, out _, T0, Gap);

        deltas.ClearServer(ServerId);

        var delta = deltas.CalculateDeltaWithSeriesAge(ServerId, Collector, "k2", 999, 5, out var interval, T1, Gap);

        Assert.Equal(0, delta);
        Assert.Equal(0, interval);
    }

    /// <summary>
    /// The pass window is per server AND per collector, so one server's sweep cannot make another
    /// server's first pass look warm.
    /// </summary>
    [Fact]
    public void ThePassWindowIsPerServerAndPerCollector()
    {
        var deltas = new CollectorDeltaCalculator();
        deltas.CalculateDeltaWithSeriesAge(ServerId, Collector, "k", 1, 1, out _, T0, Gap);

        Assert.Equal(0, deltas.CalculateDeltaWithSeriesAge(2, Collector, "k", 500, 5, out _, T1, Gap));
        Assert.Equal(0, deltas.CalculateDeltaWithSeriesAge(ServerId, "other_collector", "k", 500, 5, out _, T1, Gap));
    }

    /* ---------------- #3540 A4: the pass window survives a restart ---------------- */

    /// <summary>
    /// THE restart pin. Before #3540 no host seeded the pass window, so the first pass after a restart
    /// always saw "no previous look" and baselined every new key — the rescue was inert on exactly the
    /// cycle it exists for. A seeded Current alone arms it: the first post-restart pass rolls it into
    /// Previous and measures the gap against it.
    /// </summary>
    [Fact]
    public void ASeededPassWindowArmsTheRescueOnTheFirstPostRestartPass()
    {
        var deltas = new SeedingCalculator();
        deltas.SeedPassWindow(ServerId, Collector, current: T0);

        /* The first pass of the new process: a plan compiled 20 s ago, inside the 60 s since the
           predecessor's last look, is credited in full with a real interval. */
        var delta = deltas.CalculateDeltaWithSeriesAge(
            ServerId, Collector, "sql:0:99:planB", 900, seriesAgeSeconds: 20, out var interval, T1, Gap);

        Assert.Equal(900, delta);
        Assert.Equal(60, interval);

        /* The control: the same first pass on an UNSEEDED calculator is the pre-#3540 behaviour. */
        var cold = new CollectorDeltaCalculator();
        Assert.Equal(0, cold.CalculateDeltaWithSeriesAge(ServerId, Collector, "sql:0:99:planB", 900, 20, out var coldInterval, T1, Gap));
        Assert.Equal(0, coldInterval);
    }

    /// <summary>
    /// The seeded window is still bounded by the gap policy and by the age: a series older than the gap
    /// since the seeded look, or a first pass past the policy, is refused exactly as an in-process window
    /// would refuse it. Seeding restores the window; it does not loosen the rule.
    /// </summary>
    [Fact]
    public void ASeededPassWindowKeepsTheGapAndAgeBounds()
    {
        var deltas = new SeedingCalculator();
        deltas.SeedPassWindow(ServerId, Collector, current: T0);

        Assert.Equal(0, deltas.CalculateDeltaWithSeriesAge(ServerId, Collector, "old", 999, seriesAgeSeconds: 3_600, out var i1, T1, Gap));
        Assert.Equal(0, i1);
        Assert.Equal(0, deltas.CalculateDeltaWithSeriesAge(ServerId, Collector, "fresh", 777, seriesAgeSeconds: 30, out var i2, T0.AddHours(2), Gap));
        Assert.Equal(0, i2);
    }

    /// <summary>
    /// A seeded Previous is never what the first post-restart pass reads: that pass carries a NEW
    /// collection time, so <c>PreviousPass</c> rolls the seeded Current into Previous before returning.
    /// Pinned so a seeder that returns one collection time per server (the wait_stats shape) is known
    /// to lose nothing by passing null — and so nobody widens a seed read to fetch a second timestamp
    /// on the belief that the rescue needs it.
    /// </summary>
    [Fact]
    public void TheSeededPreviousIsNotWhatTheFirstPostRestartPassReads()
    {
        var withPrevious = new SeedingCalculator();
        withPrevious.SeedPassWindow(ServerId, Collector, current: T0, previous: T0.AddMinutes(-5));

        var withoutPrevious = new SeedingCalculator();
        withoutPrevious.SeedPassWindow(ServerId, Collector, current: T0);

        var a = withPrevious.CalculateDeltaWithSeriesAge(ServerId, Collector, "k", 900, 20, out var ia, T1, Gap);
        var b = withoutPrevious.CalculateDeltaWithSeriesAge(ServerId, Collector, "k", 900, 20, out var ib, T1, Gap);

        Assert.Equal(900, a);
        Assert.Equal(a, b);
        Assert.Equal(60, ia);
        Assert.Equal(ia, ib);
    }

    /// <summary>The seeded window is per server and per group, like the live one.</summary>
    [Fact]
    public void ASeededPassWindowIsPerServerAndPerGroup()
    {
        var deltas = new SeedingCalculator();
        deltas.SeedPassWindow(ServerId, Collector, current: T0);

        Assert.Equal(0, deltas.CalculateDeltaWithSeriesAge(2, Collector, "k", 500, 5, out _, T1, Gap));
        Assert.Equal(0, deltas.CalculateDeltaWithSeriesAge(ServerId, "query_stats_exec", "k", 500, 5, out _, T1, Gap));
    }

    /// <summary><c>ClearServer</c> drops a SEEDED window too — the re-add path both hosts wire.</summary>
    [Fact]
    public void ClearServerDropsASeededPassWindow()
    {
        var deltas = new SeedingCalculator();
        deltas.SeedPassWindow(ServerId, Collector, current: T0);

        deltas.ClearServer(ServerId);

        Assert.Equal(0, deltas.CalculateDeltaWithSeriesAge(ServerId, Collector, "k", 900, 5, out var interval, T1, Gap));
        Assert.Equal(0, interval);
    }

    /// <summary>
    /// <c>SeedPasses</c> seeds EVERY group named, from the tracker's per-server window — a seeder that
    /// seeded the family name instead of its groups would arm nothing, because the live window is keyed
    /// by the group the collector passes as <c>collectorName</c>.
    /// </summary>
    [Fact]
    public void SeedPassesArmsEveryGroupItIsGiven()
    {
        var deltas = new SeedingCalculator();
        deltas.SeedFromTracker(new[] { (ServerId, T0) }, "query_stats_exec", "query_stats_worker", "query_stats_rows");

        foreach (var group in new[] { "query_stats_exec", "query_stats_worker", "query_stats_rows" })
        {
            Assert.Equal(900, deltas.CalculateDeltaWithSeriesAge(ServerId, group, "k", 900, 20, out _, T1, Gap));
        }

        /* The family name itself is not a group and must not have been armed. */
        Assert.Equal(0, deltas.CalculateDeltaWithSeriesAge(ServerId, "query_stats", "k", 900, 20, out _, T1, Gap));
    }

    /// <summary>
    /// The tracker keeps the two most recent DISTINCT times per server whatever order the rows stream in,
    /// and ignores a null time rather than guessing — the seed reads are unordered by time within a
    /// server, and a latest-collection read repeats one time across every row.
    /// </summary>
    [Fact]
    public void TheSeedPassTrackerKeepsTheLatestAndTheOneBefore_InAnyArrivalOrder()
    {
        var a = T0.AddMinutes(-4);
        var b = T0.AddMinutes(-2);
        var c = T0;

        var arrivals = new[]
        {
            new[] { a, b, c },
            new[] { c, b, a },
            new[] { b, c, a, c, b, b },
        };

        foreach (var order in arrivals)
        {
            var tracker = new SeedingCalculator.Tracker();
            foreach (var t in order)
            {
                tracker.Observe(ServerId, t);
            }
            tracker.Observe(ServerId, null);

            var window = Assert.Single(tracker.Servers);
            Assert.Equal(ServerId, window.ServerId);
            Assert.Equal(c, window.Latest);
            Assert.Equal(b, window.Before);
        }

        /* One time observed many times: Latest is it, Before is nothing — not a copy of Latest. */
        var single = new SeedingCalculator.Tracker();
        single.Observe(ServerId, c);
        single.Observe(ServerId, c);
        var only = Assert.Single(single.Servers);
        Assert.Equal(c, only.Latest);
        Assert.Null(only.Before);

        /* And nothing observed is nothing seeded: a null-only stream yields no server. */
        var empty = new SeedingCalculator.Tracker();
        empty.Observe(ServerId, null);
        Assert.Empty(empty.Servers);
        Assert.Equal(0, empty.Count);
    }

    /// <summary>
    /// A host stand-in exposing the protected seeding hooks. The real hosts (Lite's DeltaCalculator,
    /// DarlingDeltaCalculator) call the same members from their store reads; this one calls them from
    /// literals so the window's semantics are pinned without an engine.
    /// </summary>
    private sealed class SeedingCalculator : CollectorDeltaCalculator
    {
        public void SeedPassWindow(int serverId, string group, DateTime current, DateTime? previous = null)
            => SeedPass(serverId, group, current, previous);

        public void SeedFromTracker((int ServerId, DateTime Time)[] rows, params string[] groups)
        {
            var tracker = new SeedPassTracker();
            foreach (var (serverId, time) in rows)
            {
                tracker.Observe(serverId, time);
            }
            SeedPasses(tracker, groups);
        }

        public sealed class Tracker
        {
            private readonly SeedPassTracker _inner = new();
            public void Observe(int serverId, DateTime? t) => _inner.Observe(serverId, t);
            public System.Collections.Generic.IEnumerable<(int ServerId, DateTime Latest, DateTime? Before)> Servers => _inner.Servers;
            public int Count => _inner.Count;
        }
    }

    /// <summary>
    /// An implementer that never opted in keeps compiling and keeps its old behaviour, which is the
    /// reason the interface method is default-implemented rather than abstract.
    /// </summary>
    [Fact]
    public void ADefaultImplementerIgnoresTheAge()
    {
        ICollectorDeltaCalculator legacy = new PreSeriesAgeCalculator();

        var delta = legacy.CalculateDeltaWithSeriesAge(ServerId, Collector, "k", 900, 5, out var interval, T1, Gap);

        Assert.Equal(-1, delta);
        Assert.Equal(-1, interval);
    }

    /// <summary>A pre-#2235 implementer: only the two original methods exist on it.</summary>
    private sealed class PreSeriesAgeCalculator : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
            DateTime? collectionTime = null, int maxGapSeconds = 0) => -1;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = -1;
            return -1;
        }
    }
}
