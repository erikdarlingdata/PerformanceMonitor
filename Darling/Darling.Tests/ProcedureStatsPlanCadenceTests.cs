/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2862 — the <c>procedure_stats</c> plan-capture cadence.
///
/// <para><b>What it is for.</b> procedure_stats is the most expensive collector on the production us-east-1
/// store, and almost all of that is the read loop draining plan XML. A controlled decomposition (6 heaviest
/// servers, 3 reps x 3 variants, serial AND 6-way concurrent, 108 executions, 150 rows every time) split the
/// drain three ways: no plan apply at all costs <b>0.2%</b>; rendering every plan while shipping only
/// <c>DATALENGTH</c> bigints costs <b>73.8%</b>; shipping the plans too adds <b>26.0%</b>. The render happens
/// inside <c>sys.dm_exec_text_query_plan</c>, a SERVER-side TVF, so it is CPU burned on the MONITORED
/// production server. That is why the lever is cadence and not a dedup key: hashing at the source would
/// remove the 26% transfer and leave the 74% render burning on customer hardware.</para>
///
/// <para><b>Why not a dedup key, settled rather than untried.</b> A plan_handle-keyed probe is disproven on
/// production data: 16,745 distinct handles map to 142,955 distinct (handle, digest) pairs — 8.5 distinct plan
/// XMLs per handle per day, one handle producing 457 plans in 468 sightings — and adding cached_time to the key
/// changes the count by exactly zero. The module recompiles statements in place as temp-table statistics
/// change, so the handle is stable by design while the XML is not.</para>
///
/// <para><b>Why the schedule is untouched.</b> #2843 pinned that no DETACHED collector sits on the 1-minute
/// tier, because a detached run skips when its predecessor is still in flight — so moving procedure_stats off
/// that tier would convert starvation into guaranteed misses. Gating the plan render instead leaves the
/// collector on its 1-minute schedule, so runtime statistics stay at full resolution and only the expensive
/// half is amortised.</para>
/// </summary>
public class ProcedureStatsPlanCadenceTests
{
    /* The exact seam the saving rides on: CollectorContext.CapturePlanXml=false erases the plan placeholders,
       which is decomposition variant A. Built here rather than retyped so the pin tracks the shipped SQL. */
    private static string BuiltQuery(bool capturePlanXml) =>
        ProcedureStatsCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "example",
            CollectionTime = new DateTime(2026, 9, 4, 0, 0, 0, DateTimeKind.Utc),
            Deltas = new CollectorDeltaCalculator(),
            CapturePlanXml = capturePlanXml,
        }).Text;

    [Fact]
    public void AGatedCycle_OmitsThePlanApplyEntirely_RatherThanRenderingAndDiscarding()
    {
        /* THE load-bearing pin for the whole change. Cadence only saves anything if the skipped cycle does
           not render; a gated path that rendered and threw the bytes away would still pay the 73.8%, which
           is the entire cost this exists to remove. Asserted on the SQL the collector actually builds. */
        var gated = BuiltQuery(capturePlanXml: false);
        var capturing = BuiltQuery(capturePlanXml: true);

        Assert.DoesNotContain("dm_exec_text_query_plan", gated, StringComparison.Ordinal);
        Assert.DoesNotContain("OUTER APPLY", gated, StringComparison.Ordinal);
        Assert.DoesNotContain("query_plan", gated, StringComparison.Ordinal);

        /* And the capturing form still renders at whole-module grain, so this is a cadence change and not a
           silent narrowing of what gets captured when it IS captured. The module DMVs expose no statement
           offsets, so 0, -1 is the only available grain. */
        Assert.Contains("sys.dm_exec_text_query_plan(CONVERT(varbinary(64), ranked.plan_handle, 1), 0, -1)", capturing, StringComparison.Ordinal);

        /* #1959's placement survives: the apply is outside the ranked derived table, so it renders once
           against at most 150 survivors rather than once per pre-TOP candidate. */
        Assert.Contains(") AS ranked" + Environment.NewLine + "OUTER APPLY", capturing.Replace("\r\n", Environment.NewLine, StringComparison.Ordinal), StringComparison.Ordinal);
    }

    [Fact]
    public void TheGatedQuery_KeepsTheRowShapeTheWriterAndPayloadDimensionExpect()
    {
        /* The gated form must differ from the capturing form ONLY by the plan fragments — same payload
           columns, same branches, same exclusion handling — because ReadAsync reads ordinal 27 only when
           CapturePlanXml is set and PgCollectorRowWriter writes a NULL payload for the plan position.
           Pinned by removing the two known fragments from the capturing text and demanding equality, which
           fails if either form grows or loses anything else. */
        var gated = BuiltQuery(capturePlanXml: false);
        var capturing = BuiltQuery(capturePlanXml: true);

        var stripped = capturing
            .Replace(",\r\n    query_plan_xml = tqp.query_plan", "", StringComparison.Ordinal)
            .Replace(",\n    query_plan_xml = tqp.query_plan", "", StringComparison.Ordinal)
            .Replace("\r\nOUTER APPLY sys.dm_exec_text_query_plan(CONVERT(varbinary(64), ranked.plan_handle, 1), 0, -1) AS tqp", "", StringComparison.Ordinal)
            .Replace("\nOUTER APPLY sys.dm_exec_text_query_plan(CONVERT(varbinary(64), ranked.plan_handle, 1), 0, -1) AS tqp", "", StringComparison.Ordinal);

        Assert.Equal(gated, stripped);

        /* Both forms still carry the last non-plan payload column, so "no plans" has not eaten a real one. */
        Assert.Contains("plan_handle = CONVERT(varchar(130), s.plan_handle, 1)", gated, StringComparison.Ordinal);
        Assert.Contains("plan_handle = CONVERT(varchar(130), s.plan_handle, 1)", capturing, StringComparison.Ordinal);

        /* Both forms are still capped, which is why a skipped cycle cannot make the next one more expensive:
           the us-east-1 fleet reports rows_collected = 150 on every run, so the candidate set is already
           saturated and a gap cannot enlarge it. */
        Assert.Contains("TOP (150)", gated, StringComparison.Ordinal);
        Assert.Contains("TOP (150)", capturing, StringComparison.Ordinal);
    }

    [Fact]
    public void AnIntervalOfOneOrLess_CapturesEveryCycle_SoTheKnobIsReversible()
    {
        /* This is the whole safety story: setting the knob back to 1 must give EXACTLY the pre-#2862
           collector, not something close to it. */
        foreach (var interval in new[] { int.MinValue, -1, 0, 1 })
        {
            for (var ordinal = 0L; ordinal < 12; ordinal++)
            {
                Assert.True(DarlingCollectorRunner.ShouldCapturePlanThisCycle(ordinal, serverId: 7, interval));
            }
        }
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(60)]
    public void ExactlyOneCycleInN_Captures(int interval)
    {
        const int serverId = 13;
        var captured = Enumerable.Range(0, interval * 20)
            .Count(i => DarlingCollectorRunner.ShouldCapturePlanThisCycle(i, serverId, interval));

        Assert.Equal(20, captured);
    }

    [Fact]
    public void TheFleetIsStaggered_SoNoCycleCarriesEveryServersPlanRender()
    {
        /* THE point of deriving the phase from the server id. A bare (ordinal % interval) would put all 42
           servers on the SAME capture cycle: three cheap cycles, then every server paying full plan-render
           cost at once — a 4x spike against the monitored fleet, which is WORSE than collecting every time,
           because peak is what produces the 120 s wall-clock abandonments.

           Pinned as a ceiling on the worst single cycle rather than as an exact distribution, because the
           even split is a property of 42 servers over 4 phases and the guard should survive a fleet that
           does not divide evenly. */
        const int interval = 4;
        var fleet = Enumerable.Range(1, 42).ToArray();

        var perCycle = Enumerable.Range(0, interval)
            .Select(ordinal => fleet.Count(id => DarlingCollectorRunner.ShouldCapturePlanThisCycle(ordinal, id, interval)))
            .ToArray();

        Assert.Equal(fleet.Length, perCycle.Sum());

        /* Every cycle does real work and no cycle carries more than a fair share plus a remainder — with a
           bare modulo this is {42, 0, 0, 0} and the upper bound fails on cycle 0 and the lower on 1..3. */
        var fairShare = (fleet.Length / interval) + 1;
        Assert.All(perCycle, count => Assert.InRange(count, 1, fairShare));

        /* The counterfactual, asserted rather than described: the naive form really does bunch. If this ever
           goes green the pin above has stopped discriminating between the two designs. */
        var bareModulo = Enumerable.Range(0, interval)
            .Select(ordinal => fleet.Count(_ => ordinal % interval == 0))
            .ToArray();
        Assert.Equal(new[] { 42, 0, 0, 0 }, bareModulo);
    }

    [Fact]
    public void AfterARestart_TheFleetIsStillSpread_NotAllOnTheFirstCycle()
    {
        /* The cycle counters are in-memory and reset to 0 on a service restart, so the WHOLE fleet re-enters
           at cycle ordinal 0 simultaneously. That is only safe because the phase comes from the server id
           rather than from accumulated drift — and it is why this needs no persisted state and therefore no
           schema change.

           Asserted as the observable property — how much of the fleet captures on that first post-restart
           cycle — rather than by calling the pure function twice and comparing, which referential
           transparency guarantees and which would stay green against any regression. */
        const int interval = 4;
        var fleet = Enumerable.Range(1, 42).ToArray();

        var capturingOnFirstCycleBack = fleet.Count(
            id => DarlingCollectorRunner.ShouldCapturePlanThisCycle(0, id, interval));

        Assert.InRange(capturingOnFirstCycleBack, 1, (fleet.Length / interval) + 1);
    }

    [Fact]
    public void ANegativeServerId_DoesNotThrowAndStaysInPhaseRange()
    {
        /* No negative ids exist today, but server_id is a signed integer; an unguarded (serverId % interval)
           would return a NEGATIVE phase and skew the schedule rather than failing loudly. */
        const int interval = 4;
        var captured = Enumerable.Range(0, interval * 10)
            .Count(i => DarlingCollectorRunner.ShouldCapturePlanThisCycle(i, int.MinValue, interval));

        Assert.Equal(10, captured);
    }

    [Fact]
    public void TheKnobClampsToItsDocumentedRange_AndNonsenseDegradesToCaptureAlways()
    {
        /* 0 and negatives land on 1 (capture every cycle), NOT on the default: a nonsense stored value must
           never silently START skipping plans. */
        Assert.Equal(1, StoreConfigProvider.ClampProcedureStatsPlanCycleInterval(0));
        Assert.Equal(1, StoreConfigProvider.ClampProcedureStatsPlanCycleInterval(-7));
        Assert.Equal(1, StoreConfigProvider.ClampProcedureStatsPlanCycleInterval(int.MinValue));
        Assert.Equal(4, StoreConfigProvider.ClampProcedureStatsPlanCycleInterval(4));
        Assert.Equal(60, StoreConfigProvider.ClampProcedureStatsPlanCycleInterval(60));
        Assert.Equal(60, StoreConfigProvider.ClampProcedureStatsPlanCycleInterval(int.MaxValue));
    }

    [Fact]
    public void TheShippedDefault_IsFourCycles()
    {
        /* Read off a fresh config rather than a literal, so this pins what the service actually starts with.
           4 keeps worst-case plan age inside the collector's own ten-minute last_execution_time candidate
           window at the cadence measured on us-east-1, and captures 75% of the achievable saving; the
           marginal return past 4 collapses while staleness keeps doubling. */
        Assert.Equal(4, new DarlingConfig().ProcedureStatsPlanCycleInterval);
        Assert.Equal(4, StoreConfigProvider.ClampProcedureStatsPlanCycleInterval(new DarlingConfig().ProcedureStatsPlanCycleInterval));
    }

    [Fact]
    public void OnlyProcedureStatsIsGated()
    {
        /* A targeted response to one collector's measured cost, not a fleet-wide policy change. query_stats,
           query_store, deadlocks and blocked_process_report all capture plans too and are deliberately left
           alone, so this constant must not drift into naming one of them. */
        Assert.Equal("procedure_stats", DarlingCollectorRunner.PlanCadenceGatedCollector);
    }

    /* A runner built the way DarlingWorker builds it, so the pins below exercise the SHIPPED composition
       (SKU flag AND cadence gate AND collector-name filter AND the cycle counter) rather than the pure
       policy in isolation. The data source is never opened - no test here reaches the store. */
    private static DarlingCollectorRunner Runner(int interval, bool capturePlans = true) =>
        new(
            NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=example;Username=example;Password=example"),
            new CollectorDeltaCalculator(),
            logger: null,
            capturePlans: () => capturePlans,
            collectSchemaChanges: () => true,
            textBudgetMb: () => 0,
            compressPlanContent: () => true,
            procedureStatsPlanCycleInterval: () => interval);

    [Fact]
    public void TheInstanceGate_LetsProcedureStatsCaptureExactlyOneCycleInN()
    {
        /* The wiring pin. CollectorContext.CapturePlanXml is assigned from exactly this method, so a
           refactor that drops the cadence half of the decision fails here rather than shipping a collector
           that renders every cycle again while every pure-policy pin stays green. */
        var runner = Runner(interval: 4);

        var captured = Enumerable.Range(0, 40)
            .Count(_ => runner.ShouldCapturePlanXmlFor("procedure_stats", serverId: 8));

        Assert.Equal(10, captured);
    }

    [Fact]
    public void TheInstanceGate_NeverGatesAnyOtherCollector()
    {
        /* Only procedure_stats is gated. query_stats and query_store capture plans too, on the same runner
           and the same cycle counter dictionary, and must be untouched. */
        var runner = Runner(interval: 4);

        foreach (var collector in new[] { "query_stats", "query_store", "deadlocks", "blocked_process_report" })
        {
            var captured = Enumerable.Range(0, 40).Count(_ => runner.ShouldCapturePlanXmlFor(collector, serverId: 8));
            Assert.Equal(40, captured);
        }
    }

    [Fact]
    public void TheSkuFlagStillWins_SoCapturePlansFalseCapturesNothing()
    {
        /* The cadence gate is an ADDITIONAL condition, not a replacement: a store that turned capture_plans
           off must still get no plans on the cadence's capture cycle. */
        var runner = Runner(interval: 4, capturePlans: false);

        Assert.All(
            Enumerable.Range(0, 40),
            _ => Assert.False(runner.ShouldCapturePlanXmlFor("procedure_stats", serverId: 8)));
    }

    [Fact]
    public void TheInstanceGate_StaggersTwoServersOntoDifferentCycles()
    {
        /* Same runner, same cycle numbers, different server ids - the phase offset has to put them on
           different capture cycles or the fleet spikes together. Pinned through the instance so the counter
           being per-(server, collector) is part of what is asserted. */
        var a = Runner(interval: 4);
        var b = Runner(interval: 4);

        var forServer4 = Enumerable.Range(0, 4).Select(_ => a.ShouldCapturePlanXmlFor("procedure_stats", 4)).ToArray();
        var forServer5 = Enumerable.Range(0, 4).Select(_ => b.ShouldCapturePlanXmlFor("procedure_stats", 5)).ToArray();

        Assert.Single(forServer4, true);
        Assert.Single(forServer5, true);
        Assert.NotEqual(Array.IndexOf(forServer4, true), Array.IndexOf(forServer5, true));
    }
}
