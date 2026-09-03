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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The V108 rung — the <c>procedure_stats</c> plan-capture cadence knob, and the top of the ladder.
///
/// <para><b>What it is for.</b> procedure_stats is the most expensive collector on the production use1 store
/// (98.1M ms/day over 17,869 runs, avg 5,490 ms, having overtaken query_store at 1.57x), and 96% of that is
/// the read loop draining plan XML: <c>sql:4,902ms = open:149ms + drain:4,724ms + other:1ms</c>. A controlled
/// decomposition split that drain into RENDER 73.8% / TRANSFER 26.0%, and the render happens inside
/// <c>sys.dm_exec_text_query_plan</c>, a SERVER-side TVF — so it is CPU burned on the MONITORED production
/// server. That is why the lever here is cadence and not a dedup key: hashing at the source would remove the
/// 26% transfer and leave the 74% render burning on customer hardware.</para>
///
/// <para><b>Why not a dedup key, settled rather than untried.</b> A plan_handle-keyed probe is disproven on
/// production data: 16,745 distinct handles map to 142,955 distinct (handle, digest) pairs — 8.5 distinct plan
/// XMLs per handle per day, one handle producing 457 plans in 468 sightings — and adding cached_time to the key
/// changes the count by exactly zero. The module recompiles statements in place as temp-table statistics
/// change, so the handle is stable by design while the XML is not.</para>
/// </summary>
public class ProcedureStatsPlanCadenceTests
{
    [Fact]
    public void TheRung_IsTheTopOfTheLadder_AndAddsTheKnobColumn()
    {
        Assert.Equal("procedure-stats-plan-cadence", PgMigrations.Scripts.Single(s => s.Version == 108).Name);
        Assert.Equal(108, PgMigrations.Scripts[^1].Version);
        Assert.Equal(108, StorageVersion.SchemaVersion);

        var sql = PgMigrations.Scripts.Single(s => s.Version == 108).Sql;
        Assert.Contains("ALTER TABLE config.config_service", sql, StringComparison.Ordinal);
        Assert.Contains("procedure_stats_plan_cycle_interval", sql, StringComparison.Ordinal);

        /* Schema-qualified, and idempotent: the migrate session's search_path puts collect first, so a bare
           ALTER would find the wrong schema. */
        Assert.Contains("ADD COLUMN IF NOT EXISTS", sql, StringComparison.Ordinal);

        /* The connect-time gate compares the probe against RequiredStoreSchemaVersion, so a fully-migrated
           store has to map to exactly this rung or the viewer refuses a store that is current. */
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);
    }

    [Fact]
    public void AnIntervalOfOneOrLess_CapturesEveryCycle_SoTheKnobIsReversible()
    {
        /* This is the whole safety story for the rung: setting the knob back to 1 must give EXACTLY the
           pre-V108 collector, not something close to it. */
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
           cost at once — a 4x spike against the monitored fleet, which is worse than collecting every time,
           because peak is what produces the 120s wall-clock abandonments this rung exists to reduce.

           Pinned as a ceiling on the worst single cycle rather than as an exact distribution, because the
           even split is a property of 42 servers over 4 phases and the guard should survive a fleet that
           does not divide evenly. */
        const int interval = 4;
        var fleet = Enumerable.Range(1, 42).ToArray();

        var perCycle = Enumerable.Range(0, interval)
            .Select(ordinal => fleet.Count(id => DarlingCollectorRunner.ShouldCapturePlanThisCycle(ordinal, id, interval)))
            .ToArray();

        Assert.Equal(fleet.Length, perCycle.Sum());

        /* Every cycle does real work (nothing is idle) and no cycle carries more than a fair share plus a
           remainder — with a bare modulo this would be {42, 0, 0, 0} and the upper bound would fail. */
        var fairShare = (fleet.Length / interval) + 1;
        Assert.All(perCycle, count => Assert.InRange(count, 1, fairShare));
    }

    [Fact]
    public void AfterARestart_TheFleetIsStillSpread_NotAllOnTheFirstCycle()
    {
        /* The cycle counters are in-memory and reset to 0 on a service restart, so the WHOLE fleet re-enters
           at cycle ordinal 0 simultaneously. That is only safe because the phase comes from the server id
           rather than from accumulated drift.

           Asserted as the observable property - how much of the fleet captures on that first post-restart
           cycle - rather than by calling the pure function twice and comparing, which referential
           transparency guarantees and which would stay green against any regression. */
        const int interval = 4;
        var fleet = Enumerable.Range(1, 42).ToArray();

        var capturingOnFirstCycleBack = fleet.Count(
            id => DarlingCollectorRunner.ShouldCapturePlanThisCycle(0, id, interval));

        /* A bare (ordinal % interval) puts all 42 here, which is the restart thundering-herd this guards
           against; the server-derived phase holds it to roughly a quarter. */
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
    public void TheKnobClampsToItsDocumentedRange()
    {
        Assert.Equal(1, StoreConfigProvider.ClampProcedureStatsPlanCycleInterval(0));
        Assert.Equal(1, StoreConfigProvider.ClampProcedureStatsPlanCycleInterval(int.MinValue));
        Assert.Equal(4, StoreConfigProvider.ClampProcedureStatsPlanCycleInterval(4));
        Assert.Equal(60, StoreConfigProvider.ClampProcedureStatsPlanCycleInterval(60));
        Assert.Equal(60, StoreConfigProvider.ClampProcedureStatsPlanCycleInterval(int.MaxValue));
    }

    [Fact]
    public void OnlyProcedureStatsIsGated()
    {
        /* The rung is a targeted response to one collector's measured cost, not a fleet-wide policy change.
           query_stats, query_store, deadlocks and blocked_process_report all capture plans too and are
           deliberately left alone, so this constant must not drift into naming one of them. */
        Assert.Equal("procedure_stats", DarlingCollectorRunner.PlanCadenceGatedCollector);
    }
}
