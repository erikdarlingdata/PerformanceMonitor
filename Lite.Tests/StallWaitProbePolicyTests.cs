/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The out-of-band stall probe's firing condition and its bounds (#2880).
///
/// <para>Pure policy, so all of it is asserted without a target, a store or a host. Every figure quoted here
/// is from the measurement recorded on #2880 against the two collectors that actually abandon
/// (<c>procedure_stats</c> and <c>query_stats</c>) on the 2 of 43 servers where it happens; the numbers are
/// in the assertions rather than only in the prose, so a "tuning" change that moves a bound outside the band
/// it was derived from fails here instead of shipping.</para>
/// </summary>
public class StallWaitProbePolicyTests
{
    /* ---- the measured populations, as constants, so each assertion names its own evidence ---- */

    /// <summary>Median delivered throughput on an abandoned run: 0.21 MB/s (procedure_stats).</summary>
    private const double AbandonedSlowestMbPerSecond = 0.21;

    /// <summary>Median delivered throughput on an abandoned run: 0.24 MB/s (query_stats) — the FASTEST of the two.</summary>
    private const double AbandonedFastestMbPerSecond = 0.24;

    /// <summary>Median delivered throughput on a successful run: 11.26 MB/s (procedure_stats) — the SLOWEST of the two.</summary>
    private const double SuccessfulSlowestMedianMbPerSecond = 11.26;

    /// <summary>The wall-clock budget the two abandoning collectors declare.</summary>
    private static readonly TimeSpan AbandoningBudget = TimeSpan.FromSeconds(120);

    /// <summary>
    /// The abandoning collectors' budget, plus <c>query_store</c>'s 600 s, plus a 1 s degenerate — every
    /// budget the arithmetic invariants below have to hold for, rather than the one that motivated them.
    /// </summary>
    public static TheoryData<int> ShippedBudgetSeconds() => new() { 1, 120, 600 };

    private static long BytesFor(double mbPerSecond, long elapsedMs) =>
        (long)(mbPerSecond * 1024 * 1024 * elapsedMs / 1000.0);

    /* ---------------- the firing condition ---------------- */

    /// <summary>
    /// The measured signature fires: past the elapsed floor, delivering at the abandoned band's rate, and —
    /// the part that matters — with the terminal silence the real defect actually has.
    ///
    /// <para><b>This is the constraint-5 pin.</b> <c>drain_ms - last_read_ms</c> was 0-3 ms on 8 of 8
    /// abandoned runs: the last row lands at the instant the budget fires, so the stream is streaming SLOWLY
    /// and never goes quiet. A watchdog gated on the reader having gone silent would therefore never fire on
    /// the defect, only on a delivered-and-hung failure that has never once been observed. The observation
    /// here is built with <c>LastReadMs == ElapsedMs</c> — zero terminal silence, constructed from the
    /// measured shape rather than left at a default — so a regression that adds a terminal-silence gate to
    /// <see cref="StallWaitProbePolicy.Decide"/> fails this assertion.</para>
    /// </summary>
    [Fact]
    public void Decide_FiresOnTheMeasuredSignature_IncludingZeroTerminalSilence()
    {
        var elapsed = (long)(AbandoningBudget.TotalMilliseconds * StallWaitProbePolicy.TriggerFractionOfBudget);

        /* 149 rows and 25.7 MB are inside the measured abandoned ranges (64-200 rows, 20.1-34.9 MB) — but
           the delivered figure the rate is computed from is what this run had delivered SO FAR, at the
           abandoned band's rate. */
        var signature = new StallProbeObservation(
            ElapsedMs: elapsed,
            RowsRead: 149,
            BytesRead: BytesFor(AbandonedSlowestMbPerSecond, elapsed),
            LastReadMs: elapsed);

        Assert.Equal(0, signature.TerminalSilenceMs);

        var decision = StallWaitProbePolicy.Decide(
            CollectorTargetEngine.SqlServer, AbandoningBudget, signature);

        Assert.True(decision.Fire, decision.Reason);
    }

    /// <summary>
    /// Terminal silence cannot change the answer — asserted as an EQUALITY across the whole range of it,
    /// not just at the measured end.
    ///
    /// <para>The zero-silence case above proves the defect fires. This proves the stronger claim the design
    /// rests on: the decision is a function of elapsed and rate ALONE. A regression that made the probe fire
    /// only on a quiet reader, or only on a busy one, breaks this even if it happened to leave the case above
    /// passing.</para>
    /// </summary>
    [Fact]
    public void Decide_IgnoresTerminalSilence_AcrossItsWholeRange()
    {
        var elapsed = (long)(AbandoningBudget.TotalMilliseconds * StallWaitProbePolicy.TriggerFractionOfBudget);
        var bytes = BytesFor(AbandonedFastestMbPerSecond, elapsed);

        /* elapsed = 0 ms of silence (the measured defect); 0 = the whole read silent since row 1
           (delivered-and-hung, never observed); -1 = no row has arrived at all. */
        var decisions = new[] { elapsed, elapsed / 2, 0L, -1L }
            .Select(lastRead => StallWaitProbePolicy.Decide(
                CollectorTargetEngine.SqlServer,
                AbandoningBudget,
                new StallProbeObservation(elapsed, 149, bytes, lastRead)))
            .ToArray();

        Assert.All(decisions, d => Assert.True(d.Fire, d.Reason));
        Assert.Single(decisions.Select(d => d.Fire).Distinct());
    }

    /// <summary>
    /// <b>The negative control.</b> A healthy stream that is slow in WALL CLOCK but progressing at a healthy
    /// rate does not fire.
    ///
    /// <para>This is the population a duration-only watchdog would fire on every time and the reason
    /// duration alone was rejected: one measured healthy body carried 71,977 ms for 12,557 rows beside peers
    /// at 1 ms — a genuinely large query, not a stall. Here the read is well past the elapsed floor and
    /// delivering at the slowest SUCCESSFUL median, and it must be left alone.</para>
    /// </summary>
    [Fact]
    public void Decide_DoesNotFire_OnAHealthySlowButProgressingStream()
    {
        var elapsed = (long)(AbandoningBudget.TotalMilliseconds * StallWaitProbePolicy.TriggerFractionOfBudget) + 30_000;

        var healthy = new StallProbeObservation(
            ElapsedMs: elapsed,
            RowsRead: 12_557,
            BytesRead: BytesFor(SuccessfulSlowestMedianMbPerSecond, elapsed),
            LastReadMs: elapsed);

        var decision = StallWaitProbePolicy.Decide(
            CollectorTargetEngine.SqlServer, AbandoningBudget, healthy);

        Assert.False(decision.Fire, decision.Reason);
        Assert.Contains("floor", decision.Reason, StringComparison.Ordinal);
    }

    /// <summary>
    /// The second half of the negative control: a run that is slow in RATE but has not been running long
    /// does not fire either.
    ///
    /// <para>Both halves of the conjunction are load-bearing because the two populations OVERLAP on
    /// throughput — successful runs reach down to 0.15 MB/s, below the abandoned median. What separates them
    /// is that a slow successful run is a SHORT one. A pin on the rate alone would pass while the elapsed
    /// floor was deleted.</para>
    /// </summary>
    [Fact]
    public void Decide_DoesNotFire_BeforeTheElapsedFloor()
    {
        var trigger = StallWaitProbePolicy.TriggerElapsedFor(AbandoningBudget)!.Value;
        var justUnder = (long)trigger.TotalMilliseconds - 1;

        var decision = StallWaitProbePolicy.Decide(
            CollectorTargetEngine.SqlServer,
            AbandoningBudget,
            new StallProbeObservation(justUnder, 0, 0, -1));

        Assert.False(decision.Fire, decision.Reason);
        Assert.Contains("trigger", decision.Reason, StringComparison.Ordinal);

        /* And one millisecond later, on an otherwise identical observation, it does. Without this the
           assertion above would also pass against a Decide that never fires. */
        var atFloor = StallWaitProbePolicy.Decide(
            CollectorTargetEngine.SqlServer,
            AbandoningBudget,
            new StallProbeObservation(justUnder + 1, 0, 0, -1));

        Assert.True(atFloor.Fire, atFloor.Reason);
    }

    /// <summary>
    /// A read still inside <c>ExecuteReaderAsync</c> — no counting reader yet, so every delivered figure is
    /// the <c>-1</c> not-measured sentinel — fires.
    ///
    /// <para>Deliberate rather than incidental: the forensics found the four cheapest collectors'
    /// <c>open_ms</c> degraded 3x to 152x against their own baselines (2, 2, 3 and 21 ms) in the body before
    /// each abandoned run, 9 of 9 across both affected servers. That is the target executing before the
    /// client reads a byte, which is the phase most likely to name the cause — so a read still executing a
    /// quarter of the way into its budget is exactly what a sample is for, and <c>-1</c> must not be read as
    /// a healthy rate.</para>
    /// </summary>
    [Fact]
    public void Decide_FiresWhileStillExecuting_WithNothingDeliveredYet()
    {
        var elapsed = (long)(AbandoningBudget.TotalMilliseconds * StallWaitProbePolicy.TriggerFractionOfBudget);
        var executing = new StallProbeObservation(elapsed, -1, -1, -1);

        Assert.Equal(-1, executing.BytesPerSecond);

        var decision = StallWaitProbePolicy.Decide(
            CollectorTargetEngine.SqlServer, AbandoningBudget, executing);

        Assert.True(decision.Fire, decision.Reason);
    }

    /// <summary>
    /// No wall-clock budget, no probe — which is every collector but the four that declare one, and is why
    /// nothing had to be enumerated by name.
    ///
    /// <para>Keyed on the declared budget rather than on a set of collector names deliberately: a
    /// hand-written set is exactly the shape a new member bypasses silently, whereas a future budgeted
    /// server-scoped collector inherits the probe by declaring a budget, and an unbudgeted one cannot be
    /// armed at all because there is no budget to take a fraction of.</para>
    /// </summary>
    [Fact]
    public void Decide_DoesNotFire_ForACollectorWithNoBudget()
    {
        var decision = StallWaitProbePolicy.Decide(
            CollectorTargetEngine.SqlServer,
            wallClockBudget: null,
            new StallProbeObservation(600_000, 0, 0, -1));

        Assert.False(decision.Fire, decision.Reason);
        Assert.Null(StallWaitProbePolicy.TriggerElapsedFor(null));
        Assert.Null(StallWaitProbePolicy.TriggerElapsedFor(TimeSpan.Zero));

        /* The collectors that DO declare one, so this pin fails if the population ever empties — a
           condition that can only fire on a set nobody is in is not a condition. */
        Assert.NotNull(ProcedureStatsCollector.Instance.PerItemWallClockBudget);
        Assert.NotNull(QueryStatsCollector.Instance.PerItemWallClockBudget);
        Assert.Equal(AbandoningBudget, ProcedureStatsCollector.Instance.PerItemWallClockBudget);
        Assert.Equal(AbandoningBudget, QueryStatsCollector.Instance.PerItemWallClockBudget);
    }

    /// <summary>
    /// The engine seam. A PostgreSQL target never fires, because <see cref="StallWaitProbePolicy.QueryText"/>
    /// reads <c>sys.dm_os_*</c> and would fail in the parser.
    ///
    /// <para>Gated inside the policy rather than at the call site, and pinned here, for #2213's reason: every
    /// blocking defect in that review was a call site that never learned engines exist, while both engines'
    /// own code was individually correct and individually tested.</para>
    /// </summary>
    [Fact]
    public void Decide_DoesNotFire_OnAPostgresTarget()
    {
        var elapsed = (long)(AbandoningBudget.TotalMilliseconds * StallWaitProbePolicy.TriggerFractionOfBudget);
        var signature = new StallProbeObservation(elapsed, 149, BytesFor(AbandonedSlowestMbPerSecond, elapsed), elapsed);

        /* The SAME observation that fires on SQL Server above, so the engine is provably the only difference. */
        Assert.True(StallWaitProbePolicy.Decide(CollectorTargetEngine.SqlServer, AbandoningBudget, signature).Fire);

        var postgres = StallWaitProbePolicy.Decide(CollectorTargetEngine.PostgreSql, AbandoningBudget, signature);

        Assert.False(postgres.Fire, postgres.Reason);
        Assert.Contains("T-SQL", postgres.Reason, StringComparison.Ordinal);
    }

    /* ---------------- the bounds ---------------- */

    /// <summary>
    /// <b>The constraint-2 invariant, as arithmetic, and TOTAL.</b> For every budget: either a probe fired at
    /// the trigger and running its whole hard budget finishes inside that budget, or the probe refuses to
    /// fire at all. There is no budget for which a watchdog outlives the stall it exists to explain.
    ///
    /// <para>Stated as the disjunction rather than as "it always fits" because it does NOT always fit — a
    /// one-second budget cannot accommodate a ten-second probe, and the honest answer there is to not arm.
    /// The <see cref="StallWaitProbePolicy.Decide"/> arm that enforces that is what makes this total, and it
    /// is the assertion below rather than a comment.</para>
    ///
    /// <para>This is the pin that catches a regression to unbounded: raising
    /// <see cref="StallWaitProbePolicy.HardBudget"/> past its headroom, or pushing
    /// <see cref="StallWaitProbePolicy.TriggerFractionOfBudget"/> toward 1, both leave a shipped budget
    /// firing a probe that cannot finish under it, and both fail here rather than being argued about.</para>
    /// </summary>
    [Theory]
    [MemberData(nameof(ShippedBudgetSeconds))]
    public void TheProbeCannotOutliveTheBudgetItDiagnoses(int budgetSeconds)
    {
        var budget = TimeSpan.FromSeconds(budgetSeconds);
        var elapsed = (long)(budget.TotalMilliseconds * StallWaitProbePolicy.TriggerFractionOfBudget);

        /* The signature that fires whenever anything fires: past the trigger, delivering nothing. */
        var decision = StallWaitProbePolicy.Decide(
            CollectorTargetEngine.SqlServer, budget, new StallProbeObservation(elapsed, -1, -1, -1));

        Assert.True(
            StallWaitProbePolicy.FitsUnderBudget(budget) || !decision.Fire,
            $"a {budget} budget fires a probe (trigger {StallWaitProbePolicy.TriggerElapsedFor(budget)} + "
            + $"{StallWaitProbePolicy.HardBudget}) that cannot finish underneath it");
    }

    /// <summary>
    /// The two halves of the invariant above, each shown to be reachable — so the disjunction is not passing
    /// because one side is vacuously true.
    /// </summary>
    [Fact]
    public void TheBudgetFitCheck_SaysYesToTheShippedBudgets_AndNoWhenItShould()
    {
        /* The budgets that actually abandon, and query_store's. These must FIT, or the instrument does not
           exist on the collectors it was built for. */
        Assert.True(StallWaitProbePolicy.FitsUnderBudget(AbandoningBudget));
        Assert.True(StallWaitProbePolicy.FitsUnderBudget(TimeSpan.FromMinutes(10)));

        /* And it has to be able to say no, or it is not a check. A budget equal to the probe's own is the
           regression shape — a hard budget wide enough to swallow the whole remaining window — and it must
           both fail the arithmetic AND be refused by Decide on the signature that otherwise fires. */
        Assert.False(StallWaitProbePolicy.FitsUnderBudget(StallWaitProbePolicy.HardBudget));

        var tooShort = TimeSpan.FromSeconds(1);
        Assert.False(StallWaitProbePolicy.FitsUnderBudget(tooShort));

        var refused = StallWaitProbePolicy.Decide(
            CollectorTargetEngine.SqlServer, tooShort, new StallProbeObservation(250, -1, -1, -1));

        Assert.False(refused.Fire, refused.Reason);
        Assert.Contains("does not fit under", refused.Reason, StringComparison.Ordinal);
    }

    /// <summary>
    /// The throughput floor sits strictly between the two measured populations, with the measurements in the
    /// assertion.
    ///
    /// <para>Two-sided on purpose: a floor below the abandoned band never fires on the defect, and a floor
    /// above the successful medians fires on every ordinary run. Both are "tuning" changes that would look
    /// harmless in a diff.</para>
    /// </summary>
    [Fact]
    public void TheThroughputFloor_SitsBetweenTheMeasuredPopulations()
    {
        var floorMb = StallWaitProbePolicy.ThroughputFloorBytesPerSecond / 1024d / 1024d;

        Assert.True(floorMb > AbandonedFastestMbPerSecond,
            $"the {floorMb:F2} MB/s floor must be above the fastest measured abandoned rate ({AbandonedFastestMbPerSecond} MB/s)");

        Assert.True(floorMb < SuccessfulSlowestMedianMbPerSecond,
            $"the {floorMb:F2} MB/s floor must be below the slowest measured successful median ({SuccessfulSlowestMedianMbPerSecond} MB/s)");
    }

    /// <summary>
    /// The elapsed floor sits above every healthy run of the collectors that abandon and well below their
    /// budget.
    /// </summary>
    [Fact]
    public void TheElapsedFloor_SitsAboveHealthyAndWellBelowTheBudget()
    {
        var trigger = StallWaitProbePolicy.TriggerElapsedFor(AbandoningBudget)!.Value;

        /* A healthy procedure_stats run: 28.2 MB at the 11.26 MB/s median, about 2.5 s. An order of
           magnitude of headroom, so an ordinary run is over long before the timer. */
        var healthyRunMs = 28.2 / SuccessfulSlowestMedianMbPerSecond * 1000;
        Assert.True(trigger.TotalMilliseconds > healthyRunMs * 5,
            $"the {trigger.TotalSeconds:F0}s trigger must clear a ~{healthyRunMs:F0}ms healthy run by a wide margin");

        /* And it must land inside the stall rather than at its far edge: the observed window is a ~240s
           steady state, and firing at a quarter of a 120s budget leaves three quarters of the budget to
           sample in. */
        Assert.True(trigger < AbandoningBudget / 2);
    }

    /* ---------------- one shot ---------------- */

    /// <summary>
    /// <b>The constraint-1 pin: one timer, one evaluation, one probe — even when the read runs on and on.</b>
    ///
    /// <para>Counting the WAITS rather than the fires is what makes this catch the regression that matters. A
    /// pin that only asserted "one probe was dispatched" would pass against a polling loop that re-evaluated
    /// every second and fired once because the first evaluation happened to say yes — and that loop is the
    /// obvious "improvement" somebody reaches for when a probe misses a stall that started late. Here the
    /// injected delay counts its invocations, so a second wait fails the test whether or not it led to a
    /// second probe.</para>
    /// </summary>
    [Fact]
    public async Task TheArm_WaitsOnce_FiresOnce_AndNeverRetries()
    {
        var delays = 0;
        var fires = 0;
        var observations = 0;
        StallProbeObservation? probed = null;
        var fired = new TaskCompletionSource();
        var elapsed = (long)(AbandoningBudget.TotalMilliseconds * StallWaitProbePolicy.TriggerFractionOfBudget);

        /* Each reading is DISTINGUISHABLE — the row count rises on every call, all of them firing. A
           constant observation would make a second reading invisible, which is how the claim that the
           stored row carries the observation the DECISION was made on went unpinned: the arm read the
           counters twice at one point, the two readings disagreed by whatever arrived in between, and no
           test could see it. `probed` is what the probe would have stored. */
        StallProbeObservation Observe()
        {
            var n = Interlocked.Increment(ref observations);
            return new StallProbeObservation(elapsed, 149 + n, BytesFor(AbandonedSlowestMbPerSecond, elapsed), elapsed);
        }

        using (var arm = StallProbeArm.Start(
            CollectorTargetEngine.SqlServer,
            AbandoningBudget,
            observe: Observe,
            fire: observation =>
            {
                probed = observation;
                Interlocked.Increment(ref fires);
                fired.TrySetResult();
                return Task.CompletedTask;
            },
            delay: (_, _) =>
            {
                Interlocked.Increment(ref delays);
                return Task.CompletedTask;
            }))
        {
            await fired.Task.WaitAsync(TimeSpan.FromSeconds(10));

            /* The read is still going: give any retry loop room to take a second lap before the arm is
               disposed, so "no second lap" is an observation rather than an artefact of disposing early. */
            await Task.Delay(50);

            Assert.Equal(1, arm.Delays);
            Assert.Equal(1, arm.Fires);
            Assert.Equal(1, Volatile.Read(ref delays));
            Assert.Equal(1, Volatile.Read(ref fires));

            /* One reading, and the probe got THAT reading. Two calls would leave the stored row describing a
               moment the decision was never made on, which both the arm's doc comment and V112's rung
               comment claim cannot happen. */
            Assert.Equal(1, Volatile.Read(ref observations));
            Assert.Equal(150, probed!.Value.RowsRead);
        }

        /* Disposal does not fire a parting shot either. */
        await Task.Delay(50);
        Assert.Equal(1, Volatile.Read(ref fires));
        Assert.Equal(1, Volatile.Read(ref observations));
    }

    /// <summary>
    /// <b>The shape this design deliberately does NOT sample</b>: a read that looks healthy at the trigger
    /// and degrades afterwards. Pinned as an absence, so the bound is discoverable by someone reading the
    /// guard rather than only by someone reading a report.
    ///
    /// <para><c>observe</c> hands back a HEALTHY observation first and a catastrophically degraded one on
    /// every later call. The arm takes the first and is finished: no probe, and — the load-bearing half —
    /// <c>observe</c> is called exactly once, so the later degradation is not merely ignored, it is never
    /// LOOKED AT. That is what makes this a statement about the design instead of about a threshold.</para>
    ///
    /// <para>Accepted on evidence, not convenience. The measured failure is uniformly slow rather than
    /// fast-then-stalled — 0.21-0.24 MB/s against 11.3-14.0 MB/s on identical payload, with 0-3 ms of
    /// terminal silence on 8 of 8, so the run was still delivering when the budget fired — and the condition
    /// PRECEDES the run: <c>open_ms</c> on the four cheapest collectors was already degraded 3x to 152x in
    /// the sweep body BEFORE each abandoned run, 9 of 9 across both affected servers. Re-evaluating would
    /// cover a shape nothing has observed, at the price of the one thing #2880's first constraint forbids.
    /// If a late-onset stall is ever measured, THIS is the test that has to change, and it says so.</para>
    /// </summary>
    [Fact]
    public async Task TheArm_HealthyAtTheTrigger_IsNeverReconsidered_ByDesign()
    {
        var observations = 0;
        var fires = 0;
        var elapsed = (long)(AbandoningBudget.TotalMilliseconds * StallWaitProbePolicy.TriggerFractionOfBudget);

        var healthy = new StallProbeObservation(
            elapsed, 12_557, BytesFor(SuccessfulSlowestMedianMbPerSecond, elapsed), elapsed);

        /* Well under the floor, and past the trigger — the signature that fires whenever anything fires. So
           if the arm ever asked a second time, it would get an answer that DOES fire, and the fire count
           below would move. */
        var degraded = new StallProbeObservation(
            elapsed * 4, 149, BytesFor(AbandonedSlowestMbPerSecond, elapsed), elapsed * 4);

        using (var arm = StallProbeArm.Start(
            CollectorTargetEngine.SqlServer,
            AbandoningBudget,
            observe: () => Interlocked.Increment(ref observations) == 1 ? healthy : degraded,
            fire: _ =>
            {
                Interlocked.Increment(ref fires);
                return Task.CompletedTask;
            },
            delay: (_, _) => Task.CompletedTask))
        {
            /* Room for a second lap, so "never reconsidered" is an observation rather than an artefact of
               disposing before one could happen. */
            await Task.Delay(100);

            Assert.Equal(1, arm.Delays);
            Assert.Equal(1, Volatile.Read(ref observations));
            Assert.Equal(0, arm.Fires);
            Assert.Equal(0, Volatile.Read(ref fires));
        }

        await Task.Delay(50);
        Assert.Equal(1, Volatile.Read(ref observations));
        Assert.Equal(0, Volatile.Read(ref fires));

        /* The positive control the absence needs: the degraded observation the arm never asked for WOULD
           have fired. Without this, the zero above would pass just as well against an observation that was
           healthy all along, and the test would be pinning nothing. */
        Assert.True(StallWaitProbePolicy.Decide(CollectorTargetEngine.SqlServer, AbandoningBudget, degraded).Fire);
        Assert.False(StallWaitProbePolicy.Decide(CollectorTargetEngine.SqlServer, AbandoningBudget, healthy).Fire);
    }

    /// <summary>
    /// An arm disarmed before its trigger — the ~99% case, an ordinary run finishing in seconds — spends
    /// nothing: no probe, and no observation of the reader either.
    /// </summary>
    [Fact]
    public async Task TheArm_DisarmedBeforeItsTrigger_SpendsNothing()
    {
        var observations = 0;
        var fires = 0;

        using (StallProbeArm.Start(
            CollectorTargetEngine.SqlServer,
            AbandoningBudget,
            observe: () =>
            {
                Interlocked.Increment(ref observations);
                return default;
            },
            fire: _ =>
            {
                Interlocked.Increment(ref fires);
                return Task.CompletedTask;
            }))
        {
            /* Real Task.Delay this time, so the 30-second trigger is genuinely outstanding when the using
               block ends. */
        }

        await Task.Delay(100);

        Assert.Equal(0, Volatile.Read(ref observations));
        Assert.Equal(0, Volatile.Read(ref fires));
    }

    /// <summary>
    /// Disposing twice is the ORDINARY case, not a mistake: the call site disarms explicitly the moment the
    /// read is over and keeps a <c>using</c> for the paths that leave by exception. So the second disposal
    /// must be a no-op rather than an <see cref="ObjectDisposedException"/> thrown out of a <c>using</c>
    /// during a collector's storage phase — and it must not fire a parting probe either.
    /// </summary>
    [Fact]
    public async Task TheArm_IsSafeToDisposeTwice_AndFiresNothingOnDisposal()
    {
        var fires = 0;

        var arm = StallProbeArm.Start(
            CollectorTargetEngine.SqlServer,
            AbandoningBudget,
            observe: () => default,
            fire: _ =>
            {
                Interlocked.Increment(ref fires);
                return Task.CompletedTask;
            });

        arm.Dispose();
        arm.Dispose();

        await Task.Delay(100);

        Assert.Equal(0, arm.Fires);
        Assert.Equal(0, Volatile.Read(ref fires));
    }

    /// <summary>
    /// An unbudgeted collector and a PostgreSQL target arm nothing at all — no timer started, so the cost on
    /// the ~60 collectors this never applies to is one allocation and no scheduled work.
    /// </summary>
    [Theory]
    [InlineData(CollectorTargetEngine.SqlServer, false)]
    [InlineData(CollectorTargetEngine.PostgreSql, true)]
    public async Task TheArm_IsInert_WithoutABudgetOrOnPostgres(CollectorTargetEngine engine, bool withBudget)
    {
        var fires = 0;

        using var arm = StallProbeArm.Start(
            engine,
            withBudget ? AbandoningBudget : null,
            observe: () => default,
            fire: _ =>
            {
                Interlocked.Increment(ref fires);
                return Task.CompletedTask;
            },
            delay: (_, _) => Task.CompletedTask);

        await Task.Delay(100);

        Assert.Equal(0, arm.Delays);
        Assert.Equal(0, arm.Fires);
        Assert.Equal(0, Volatile.Read(ref fires));
    }

    /* ---------------- the query and the sample ---------------- */

    /// <summary>
    /// The shipped query text, asserted for the properties a review would otherwise have to re-derive by
    /// reading it.
    /// </summary>
    [Fact]
    public void TheQuery_IsServerWide_Cheap_AndUnfiltered()
    {
        var sql = StallWaitProbePolicy.QueryText;

        /* Server-wide, and the two DMVs that answer the live hypotheses: what everything is waiting on, and
           whether the schedulers are saturated. */
        Assert.Contains("sys.dm_os_waiting_tasks", sql, StringComparison.Ordinal);
        Assert.Contains("sys.dm_os_schedulers", sql, StringComparison.Ordinal);

        /* And the session catalog, which classifies each waiting task as user or background and finds this
           service's own session. */
        Assert.Contains("sys.dm_exec_sessions", sql, StringComparison.Ordinal);

        /* NOT the expensive shredders. A probe against an instance that has gone 50x slow at producing rows
           must not ask it for plan XML or SQL text. */
        Assert.DoesNotContain("dm_exec_query_plan", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("dm_exec_sql_text", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("dm_exec_text_query_plan", sql, StringComparison.Ordinal);

        /* No tempdb object: the instance may be stalled ON tempdb, and creating one there to serve a
           diagnostic is how a watchdog becomes the second problem. */
        Assert.DoesNotContain("#", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE TABLE", sql, StringComparison.Ordinal);

        /* Read-only and recompiled, the house conventions for a collector query. */
        Assert.DoesNotContain("INSERT", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("UPDATE", sql, StringComparison.Ordinal);
        Assert.Contains("READ UNCOMMITTED", sql, StringComparison.Ordinal);
        Assert.Contains("OPTION(RECOMPILE)", sql, StringComparison.Ordinal);

        /* The top-N cut is DERIVED from the constant, so bumping the constant without editing the SQL is a
           failing test rather than a query that silently keeps returning five. */
        Assert.Contains(
            string.Format(CultureInfo.InvariantCulture, "TOP ({0})", StallWaitProbePolicy.TopWaitTypeCount),
            sql,
            StringComparison.Ordinal);

        /* The idle exclusion is a list of literals rendered from IgnoredWaitDefaults.All, never a copy of it
           and never a name of the set in the text. */
        Assert.DoesNotContain("IgnoredWait", sql, StringComparison.Ordinal);
        Assert.Contains("wt.is_idle = 0", sql, StringComparison.Ordinal);

        /* CONTRIBUTING.md's function rule: COUNT_BIG, never COUNT. Asserted here because nothing in the
           repo enforces it — this query shipped two bare COUNTs through every guard and was caught by eye
           in review. The negative reads cleanly because "COUNT(" cannot match "COUNT_BIG(". The Contains
           beside it is a FLOOR, not a per-site check: it fails only when the query counts nothing at all,
           which is the one way the negative could pass vacuously — it is satisfied by any single count, so
           it cannot tell four counts from one and is not asked to. Same for @@ROWCOUNT, which this query
           has no use for but the rule also covers. */
        Assert.Contains("COUNT_BIG(", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("COUNT(", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@@ROWCOUNT", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// <b>Emptiness has a denominator.</b> Zero waiting tasks is a real and interesting answer, so it must be
    /// storable as a sample — but a result set that describes no instance must NOT be.
    ///
    /// <para>Every live SQL Server reports at least one <c>VISIBLE ONLINE</c> scheduler, so the scheduler
    /// count is what separates the two absences. Without it, "the instance was idle and still slow" and "the
    /// probe read nothing" would both be a row of zeros, and the first of those is a finding.</para>
    /// </summary>
    [Fact]
    public async Task ReadAsync_SeparatesAnIdleInstanceFromNoSampleAtAll()
    {
        /* The guaranteed OUTER APPLY row: nothing waiting, 8 live schedulers. A real all-clear. */
        var idle = await StallWaitProbePolicy.ReadAsync(
            new FakeSampleReader(new object?[][]
            {
                [null, null, null, null, 0L, 0, 8, 0L, 0L, 0L, 0, 0L, 0, 0L, null, null, null, 0L, 0],
            }),
            CancellationToken.None);

        Assert.NotNull(idle);
        Assert.Equal(0, idle!.WaitingTaskCount);
        Assert.Null(idle.TopWaitType);
        Assert.Equal(8, idle.SchedulerCount);
        Assert.Equal(
            "excluded from ranking: background 0 tasks/0 types; idle 0 tasks/0 types; ours: not visible",
            idle.WaitSummary);

        /* The same shape with zero schedulers is not an instance, and must not be credited as a sample. */
        var notAnInstance = await StallWaitProbePolicy.ReadAsync(
            new FakeSampleReader(new object?[][]
            {
                [null, null, null, null, 0L, 0, 0, 0L, 0L, 0L, 0, 0L, 0, 0L, null, null, null, 0L, 0],
            }),
            CancellationToken.None);

        Assert.Null(notAnInstance);

        /* And no row at all is not a sample either. */
        Assert.Null(await StallWaitProbePolicy.ReadAsync(
            new FakeSampleReader(Array.Empty<object?[]>()), CancellationToken.None));
    }

    /// <summary>
    /// A populated sample: the headline totals come from the pre-cut aggregate, not from the top five's
    /// subtotal, and the top wait is the first row.
    /// </summary>
    [Fact]
    public async Task ReadAsync_TakesTheHeadlineTotalsFromBeforeTheTopNCut()
    {
        var sample = await StallWaitProbePolicy.ReadAsync(
            new FakeSampleReader(new object?[][]
            {
                ["SOS_SCHEDULER_YIELD", 412L, 9_931L, 61L, 631L, 14, 8, 97L, 3L, 12L, 21, 0L, 0, 0L, null, null, null, 0L, 0],
                ["ASYNC_NETWORK_IO", 3L, 8_812L, 4_401L, 631L, 14, 8, 97L, 3L, 12L, 21, 0L, 0, 0L, null, null, null, 0L, 0],
            }),
            CancellationToken.None);

        Assert.NotNull(sample);

        /* 631 across 14 types, not 415 across 2 — the whole instance, which is what makes the sample
           server-wide rather than a top-five report. */
        Assert.Equal(631, sample!.WaitingTaskCount);
        Assert.Equal(14, sample.DistinctWaitTypes);

        Assert.Equal("SOS_SCHEDULER_YIELD", sample.TopWaitType);
        Assert.Equal(9_931, sample.TopWaitTotalMs);
        Assert.Equal(61, sample.TopWaitMaxMs);

        Assert.Equal(97, sample.RunnableTasks);
        Assert.Equal(12, sample.PendingDiskIo);
        Assert.Equal(21, sample.MaxRunnableTasks);

        /* The breadth column carries both types; nothing parses it, and the discriminators are their own
           columns beside it. */
        Assert.Equal("SOS_SCHEDULER_YIELD:412x/9931ms; ASYNC_NETWORK_IO:3x/8812ms; excluded from ranking: background 0 tasks/0 types; idle 0 tasks/0 types; ours: not visible", sample.WaitSummary);
    }

    /// <summary>
    /// A few wait names come back from the DMV with a trailing space, and a user-task wait can carry it too
    /// (the fixture uses a name that is NOT on the idle list, so it is one the real query can rank). The space must not land in the stored top wait type or in the summary
    /// line.
    /// </summary>
    [Fact]
    public async Task ReadAsync_TrimsTheTrailingSpaceAWaitNameCarries()
    {
        var sample = await StallWaitProbePolicy.ReadAsync(
            new FakeSampleReader(new object?[][]
            {
                ["LCK_M_S ", 1L, 240_000L, 240_000L, 631L, 14, 8, 97L, 3L, 12L, 21, 0L, 0, 0L, null, null, null, 0L, 0],
                ["SOS_SCHEDULER_YIELD", 412L, 9_931L, 61L, 631L, 14, 8, 97L, 3L, 12L, 21, 0L, 0, 0L, null, null, null, 0L, 0],
            }),
            CancellationToken.None);

        Assert.NotNull(sample);
        Assert.Equal("LCK_M_S", sample!.TopWaitType);
        Assert.Equal("LCK_M_S:1x/240000ms; SOS_SCHEDULER_YIELD:412x/9931ms; excluded from ranking: background 0 tasks/0 types; idle 0 tasks/0 types; ours: not visible", sample.WaitSummary);
    }

    /// <summary>
    /// The ranking excludes exactly the shared idle list and nothing else (#5267): the rendered list is the
    /// set at load, in ordinal order, as <c>N'…'</c> literals, and the waits this probe exists to find are
    /// not in it.
    /// </summary>
    [Fact]
    public void TheQuery_ExcludesExactlyTheSharedIdleList_FromTheRanking()
    {
        var sql = StallWaitProbePolicy.QueryText;

        /* Rendered ONCE: the list lives in the leading derived set as is_idle, and the totals and the ranking
           both read that flag. Every N'...' literal before the SELECT is therefore the list, once. */
        var list = sql.Substring(0, sql.IndexOf("SELECT /* PerformanceMonitorDarling", StringComparison.Ordinal));
        var literals = System.Text.RegularExpressions.Regex.Matches(list, "N'([^']*)'")
            .Select(m => m.Groups[1].Value)
            .ToList();

        Assert.Equal(IgnoredWaitDefaults.All.OrderBy(n => n, StringComparer.Ordinal).ToList(), literals);
        Assert.All(literals, n => Assert.Matches("^[A-Z0-9_]+$", n));

        /* The starvation, lock, latch and IO waits are the point of the probe and must still rank. */
        foreach (var name in new[] { "SOS_SCHEDULER_YIELD", "THREADPOOL", "ASYNC_NETWORK_IO", "PAGEIOLATCH_SH", "LCK_M_X", "RESOURCE_SEMAPHORE" })
        {
            Assert.DoesNotContain("N'" + name + "'", sql, StringComparison.Ordinal);
        }

        /* Wait type only: no session or batch text is read to decide idleness. */
        Assert.DoesNotContain("sql_text", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The idle counts use the same list as the ranking exclusion, in the totals; a name that is not a plain
    /// wait name is refused rather than rendered into the text.
    /// </summary>
    [Fact]
    public void TheQuery_CountsTheIdleWaitsInTheTotals_AndRefusesAnUnsafeName()
    {
        var sql = StallWaitProbePolicy.QueryText;
        var crossJoin = sql.IndexOf("CROSS JOIN", StringComparison.Ordinal);
        var firstApply = sql.IndexOf("OUTER APPLY", StringComparison.Ordinal);
        var totals = sql.Substring(crossJoin, firstApply - crossJoin);

        Assert.Contains("idle_waiting_tasks", totals, StringComparison.Ordinal);
        Assert.Contains("idle_wait_types", totals, StringComparison.Ordinal);
        Assert.Contains("N'SP_SERVER_DIAGNOSTICS_SLEEP'", sql, StringComparison.Ordinal);
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(sql, "N'SP_SERVER_DIAGNOSTICS_SLEEP'"));

        /* A task counted as background is not counted again as idle. */
        Assert.Contains("ISNULL(wt.is_user_process, 1) <> 0 AND wt.is_idle = 1", totals, StringComparison.Ordinal);

        Assert.Throws<InvalidOperationException>(() => StallWaitProbePolicy.BuildQueryText(new[] { "OK_WAIT", "BAD'; DROP" }));
        Assert.Throws<InvalidOperationException>(() => StallWaitProbePolicy.BuildQueryText(Array.Empty<string>()));

        /* A null entry is the documented refusal too, not an ArgumentNullException from the sort or the regex. */
        Assert.Throws<InvalidOperationException>(() => StallWaitProbePolicy.BuildQueryText(new string?[] { "OK_WAIT", null }!));
        Assert.DoesNotContain("<<", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The ranking drops background tasks and the totals do not. The ISNULL form is asserted literally: a bare
    /// <c>es.is_user_process = 1</c> would drop every session the DMV cannot show (Azure SQL Database).
    /// </summary>
    [Fact]
    public void TheRanking_DropsBackgroundTasks_ButTheTotalsDoNot()
    {
        var sql = StallWaitProbePolicy.QueryText;

        var firstApply = sql.IndexOf("OUTER APPLY", StringComparison.Ordinal);
        var crossJoin = sql.IndexOf("CROSS JOIN", StringComparison.Ordinal);
        Assert.True(crossJoin > 0 && firstApply > crossJoin);

        var totals = sql.Substring(crossJoin, firstApply - crossJoin);
        var ranking = sql.Substring(firstApply, sql.IndexOf("OUTER APPLY", firstApply + 1, StringComparison.Ordinal) - firstApply);

        /* Both read the one derived set, which keeps a session the DMV cannot show (a LEFT JOIN), and the
           ranking keeps its fail-safe. */
        var waiting = sql.Substring(0, crossJoin);
        Assert.Contains("LEFT JOIN sys.dm_exec_sessions AS es", waiting, StringComparison.Ordinal);
        Assert.Contains("FROM waiting AS wt", totals, StringComparison.Ordinal);
        Assert.Contains("FROM waiting AS wt", ranking, StringComparison.Ordinal);
        Assert.Contains("TOP (5)", ranking, StringComparison.Ordinal);
        Assert.Contains("ISNULL(wt.is_user_process, 1) = 1", ranking, StringComparison.Ordinal);
        Assert.Contains("wt.is_idle = 0", ranking, StringComparison.Ordinal);

        Assert.DoesNotContain("is_user_process, 1) = 1", totals, StringComparison.Ordinal);
        Assert.Contains("background_waiting_tasks", totals, StringComparison.Ordinal);

        /* Sessionless tasks are dropped by an explicit predicate, once, in the derived set both read. */
        Assert.Contains("owt.session_id IS NOT NULL", waiting, StringComparison.Ordinal);
        Assert.Contains("owt.wait_type IS NOT NULL", waiting, StringComparison.Ordinal);
    }

    /// <summary>
    /// This service's own session is found by application name and client host, never by session id, and the
    /// query binds no parameters.
    /// </summary>
    [Fact]
    public void TheQuery_FindsOurOwnSession_ByProgramAndHost_NotBySessionId()
    {
        var sql = StallWaitProbePolicy.QueryText;

        Assert.Contains("es.program_name = APP_NAME()", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("PROGRAM_NAME", sql, StringComparison.Ordinal);
        Assert.Contains("es.host_name = HOST_NAME()", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT_BIG(DISTINCT es.session_id)", sql, StringComparison.Ordinal);
        Assert.Contains("sys.dm_exec_requests", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("target_session_id", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@session_id", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The planted rowset: 631 waiting tasks, 600 of them background, two ranked user waits, and one session of
    /// ours waiting on ASYNC_NETWORK_IO. The unfiltered total is preserved, the top wait is the user wait, and
    /// the summary ends with the exclusion tail.
    /// </summary>
    [Fact]
    public async Task ReadAsync_ReadsTheExclusionCountsAndOurSessionWait_FromThePlantedRowset()
    {
        var sample = await StallWaitProbePolicy.ReadAsync(
            new FakeSampleReader(new object?[][]
            {
                ["SOS_SCHEDULER_YIELD", 25L, 9_931L, 61L, 631L, 14, 8, 97L, 3L, 12L, 21, 600L, 9, 1L, "ASYNC_NETWORK_IO", 41_000L, "ASYNC_NETWORK_IO", 0L, 0],
                ["ASYNC_NETWORK_IO", 3L, 8_812L, 4_401L, 631L, 14, 8, 97L, 3L, 12L, 21, 600L, 9, 1L, "ASYNC_NETWORK_IO", 41_000L, "ASYNC_NETWORK_IO", 0L, 0],
            }),
            CancellationToken.None);

        Assert.NotNull(sample);
        Assert.Equal(631, sample!.WaitingTaskCount);
        Assert.Equal("SOS_SCHEDULER_YIELD", sample.TopWaitType);
        Assert.Equal(600, sample.BackgroundWaitingTasks);
        Assert.Equal(9, sample.BackgroundWaitTypes);
        Assert.Equal(1, sample.CollectorSessions);
        Assert.Equal("ASYNC_NETWORK_IO", sample.CollectorWaitType);
        Assert.Equal(41_000, sample.CollectorWaitMs);
        Assert.Equal("ASYNC_NETWORK_IO", sample.CollectorLastWaitType);
        Assert.EndsWith(
            "excluded from ranking: background 600 tasks/9 types; idle 0 tasks/0 types; ours: ASYNC_NETWORK_IO 41000ms (last ASYNC_NETWORK_IO)",
            sample.WaitSummary,
            StringComparison.Ordinal);
    }

    /// <summary>No visible session of ours says so, rather than reading as a zero-millisecond wait.</summary>
    [Fact]
    public async Task ReadAsync_OurSessionNotVisible_SaysSo()
    {
        var sample = await StallWaitProbePolicy.ReadAsync(
            new FakeSampleReader(new object?[][]
            {
                ["SOS_SCHEDULER_YIELD", 25L, 9_931L, 61L, 631L, 14, 8, 97L, 3L, 12L, 21, 600L, 9, 0L, null, null, null, 0L, 0],
            }),
            CancellationToken.None);

        Assert.NotNull(sample);
        Assert.EndsWith("; ours: not visible", sample!.WaitSummary, StringComparison.Ordinal);
    }

    /// <summary>
    /// Several matching sessions are labelled as such, and a running request (no current wait) reads as
    /// running.
    /// </summary>
    [Fact]
    public void RenderWaitSummary_LabelsSeveralSessions_AndARunningRequest()
    {
        var waits = new[] { new StallWaitRow("SOS_SCHEDULER_YIELD", 25, 9_931, 61) };

        var summary = StallWaitProbePolicy.RenderWaitSummary(waits, 600, 9, 2, null, 0, "PAGEIOLATCH_SH");

        Assert.EndsWith(
            "ours (oldest of 2): running (last PAGEIOLATCH_SH)",
            summary,
            StringComparison.Ordinal);
    }

    /// <summary>Only background sessions wait: nothing ranks, and the summary is just the tail.</summary>
    [Fact]
    public async Task ReadAsync_WithOnlyBackgroundWaits_KeepsTheTailAsTheWholeSummary()
    {
        var sample = await StallWaitProbePolicy.ReadAsync(
            new FakeSampleReader(new object?[][]
            {
                [null, null, null, null, 600L, 9, 8, 97L, 3L, 12L, 21, 600L, 9, 1L, "ASYNC_NETWORK_IO", 41_000L, "ASYNC_NETWORK_IO", 0L, 0],
            }),
            CancellationToken.None);

        Assert.NotNull(sample);
        Assert.Null(sample!.TopWaitType);
        Assert.Equal(
            "excluded from ranking: background 600 tasks/9 types; idle 0 tasks/0 types; ours: ASYNC_NETWORK_IO 41000ms (last ASYNC_NETWORK_IO)",
            sample.WaitSummary);
    }

    /// <summary>The worst-case tail (maximum counts, 60-character names, 40 long entries) stays inside the cap.</summary>
    [Fact]
    public void RenderWaitSummary_TheWorstCaseTail_FitsTheCap()
    {
        var name = new string('W', 60);
        var many = Enumerable.Range(0, 40).Select(_ => new StallWaitRow(name, long.MaxValue, long.MaxValue, 1)).ToArray();

        var summary = StallWaitProbePolicy.RenderWaitSummary(many, long.MaxValue, int.MaxValue, long.MaxValue, name, long.MaxValue, name, long.MaxValue, int.MaxValue);
        var alone = StallWaitProbePolicy.RenderWaitSummary(
            Array.Empty<StallWaitRow>(), long.MaxValue, int.MaxValue, long.MaxValue, name, long.MaxValue, name, long.MaxValue, int.MaxValue);

        Assert.True(summary.Length <= 512);
        Assert.True(alone.Length <= 512);
        Assert.StartsWith("excluded from ranking:", alone, StringComparison.Ordinal);
    }

    /// <summary>The idle counts ride in the tail, between the background counts and our own session (#5267).</summary>
    [Fact]
    public void RenderWaitSummary_CarriesTheIdleCounts_InTheTail()
    {
        var waits = new[] { new StallWaitRow("LCK_M_S", 1, 4_200, 4_200) };

        var summary = StallWaitProbePolicy.RenderWaitSummary(waits, 58, 7, 1, "ASYNC_NETWORK_IO", 10, "ASYNC_NETWORK_IO", 3, 2);

        Assert.Equal(
            "LCK_M_S:1x/4200ms; excluded from ranking: background 58 tasks/7 types; idle 3 tasks/2 types; ours: ASYNC_NETWORK_IO 10ms (last ASYNC_NETWORK_IO)",
            summary);
    }

    /// <summary>
    /// Only idle and background waits exist: nothing ranks (a null top wait, zero top totals), the tail is the
    /// whole summary and carries both counts, and the waiting-task total still counts every task.
    /// </summary>
    [Fact]
    public async Task ReadAsync_WithOnlyIdleAndBackgroundWaits_HasNoTopWait_AndCountsTheIdleOnes()
    {
        var sample = await StallWaitProbePolicy.ReadAsync(
            new FakeSampleReader(new object?[][]
            {
                [null, null, null, null, 61L, 8, 8, 0L, 0L, 0L, 0, 58L, 6, 0L, null, null, null, 3L, 2],
            }),
            CancellationToken.None);

        Assert.NotNull(sample);
        Assert.Null(sample!.TopWaitType);
        Assert.Equal(0, sample.TopWaitTotalMs);
        Assert.Equal(0, sample.TopWaitMaxMs);
        Assert.Equal(61, sample.WaitingTaskCount);
        Assert.Equal(
            "excluded from ranking: background 58 tasks/6 types; idle 3 tasks/2 types; ours: not visible",
            sample.WaitSummary);
    }

    /// <summary>The tail is reserved first, so 40 long entries cannot push it off the 512-character cap.</summary>
    [Fact]
    public void RenderWaitSummary_KeepsTheTailWholeUnderTheCap()
    {
        var many = Enumerable.Range(0, 40)
            .Select(i => new StallWaitRow($"WAIT_TYPE_NUMBER_{i:D2}", 1_234, 5_678_901, 4_321))
            .ToArray();
        const string tail = "excluded from ranking: background 600 tasks/9 types; idle 12 tasks/3 types; ours: ASYNC_NETWORK_IO 41000ms (last ASYNC_NETWORK_IO)";

        var summary = StallWaitProbePolicy.RenderWaitSummary(many, 600, 9, 1, "ASYNC_NETWORK_IO", 41_000, "ASYNC_NETWORK_IO", 12, 3);

        Assert.True(summary.Length <= StallWaitProbePolicy.WaitSummaryMaxLength);
        Assert.EndsWith(tail, summary, StringComparison.Ordinal);
        Assert.Contains("WAIT_TYPE_NUMBER_00:1234x/5678901ms; ", summary, StringComparison.Ordinal);
        Assert.DoesNotContain(";;", summary, StringComparison.Ordinal);
    }

    /// <summary>
    /// The summary is capped by whole entries. A cut mid-figure would read as a real, smaller number.
    /// </summary>
    [Fact]
    public void RenderWaitSummary_TruncatesOnEntryBoundaries()
    {
        var many = Enumerable.Range(0, 40)
            .Select(i => new StallWaitRow($"WAIT_TYPE_NUMBER_{i:D2}", 1_234, 5_678_901, 4_321))
            .ToArray();

        var summary = StallWaitProbePolicy.RenderWaitSummary(many);

        Assert.True(summary.Length <= StallWaitProbePolicy.WaitSummaryMaxLength);
        Assert.EndsWith("ours: not visible", summary, StringComparison.Ordinal);
        Assert.Contains("ms; excluded from ranking:", summary, StringComparison.Ordinal);
        Assert.DoesNotContain(";;", summary, StringComparison.Ordinal);
        Assert.False(summary.EndsWith("; ", StringComparison.Ordinal));
    }

    /// <summary>
    /// A probe that could not connect is one of the recorded outcomes, and the outcome vocabulary keeps a
    /// connect failure distinct from a connect TIMEOUT and from anything that happened after the connection
    /// opened.
    ///
    /// <para>#2880 lists "whether a new connection succeeds mid-stall" as untested — the one open in evidence
    /// (<c>open:104ms</c>) is the stalled collector's OWN open, taken before the stall — so these are the
    /// values that answer an open question, not error handling.</para>
    /// </summary>
    [Fact]
    public void TheOutcomeVocabulary_MakesAFailedConnectItsOwnAnswer()
    {
        Assert.Contains(StallWaitProbePolicy.OutcomeConnectFailed, StallWaitProbePolicy.Outcomes);
        Assert.Contains(StallWaitProbePolicy.OutcomeConnectTimedOut, StallWaitProbePolicy.Outcomes);

        /* Distinct from each other and from the post-connect outcomes, so a read can tell "never got a
           connection" from "got one and the query died". */
        Assert.Equal(
            StallWaitProbePolicy.Outcomes.Count,
            StallWaitProbePolicy.Outcomes.Distinct(StringComparer.Ordinal).Count());

        /* And none of them collides with the collection_log status vocabulary, which reads on the other
           surface bucket by explicit list: a shared literal would let a probe outcome be counted as a
           collector run's status. */
        Assert.DoesNotContain(EnumeratedCollectorDriver.AbandonedStatus, StallWaitProbePolicy.Outcomes);
        Assert.DoesNotContain("SUCCESS", StallWaitProbePolicy.Outcomes);
        Assert.DoesNotContain("ERROR", StallWaitProbePolicy.Outcomes);
        Assert.DoesNotContain("YIELDED", StallWaitProbePolicy.Outcomes);
    }

    /// <summary>
    /// A minimal <c>DbDataReader</c> over fixed rows. Positional, like the shipped reader, so a column
    /// reordered in the query without reordering the read is caught by the value assertions rather than
    /// hidden behind names.
    /// </summary>
    private sealed class FakeSampleReader : System.Data.Common.DbDataReader
    {
        private readonly IReadOnlyList<object?[]> _rows;
        private int _index = -1;

        internal FakeSampleReader(IReadOnlyList<object?[]> rows) => _rows = rows;

        public override bool Read()
        {
            _index++;
            return _index < _rows.Count;
        }

        public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Task.FromResult(Read());

        public override object GetValue(int ordinal) => _rows[_index][ordinal]!;

        public override bool IsDBNull(int ordinal) => _rows[_index][ordinal] is null;

        public override long GetInt64(int ordinal) => Convert.ToInt64(_rows[_index][ordinal], CultureInfo.InvariantCulture);

        public override int GetInt32(int ordinal) => Convert.ToInt32(_rows[_index][ordinal], CultureInfo.InvariantCulture);

        public override string GetString(int ordinal) => (string)_rows[_index][ordinal]!;

        public override int FieldCount => 19;

        public override bool HasRows => _rows.Count > 0;

        public override int Depth => 0;

        public override bool IsClosed => false;

        public override int RecordsAffected => 0;

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => throw new NotSupportedException();

        public override bool GetBoolean(int ordinal) => throw new NotSupportedException();

        public override byte GetByte(int ordinal) => throw new NotSupportedException();

        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) =>
            throw new NotSupportedException();

        public override char GetChar(int ordinal) => throw new NotSupportedException();

        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) =>
            throw new NotSupportedException();

        public override string GetDataTypeName(int ordinal) => throw new NotSupportedException();

        public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();

        public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();

        public override double GetDouble(int ordinal) => throw new NotSupportedException();

        public override Type GetFieldType(int ordinal) => throw new NotSupportedException();

        public override float GetFloat(int ordinal) => throw new NotSupportedException();

        public override Guid GetGuid(int ordinal) => throw new NotSupportedException();

        public override short GetInt16(int ordinal) => throw new NotSupportedException();

        public override string GetName(int ordinal) => throw new NotSupportedException();

        public override int GetOrdinal(string name) => throw new NotSupportedException();

        public override int GetValues(object[] values) => throw new NotSupportedException();

        public override System.Collections.IEnumerator GetEnumerator() => throw new NotSupportedException();

        public override bool NextResult() => false;
    }
}
