using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pure unit tests for BlockingChainReconstructor — apex/depth/victim reconstruction, the (monitor_loop,
/// spid, ecid) session identity (mirroring sp_HumanEventsBlockViewer), cumulative-vs-per-scan scoping, cycle
/// handling, and the traversal caps. No database.
/// </summary>
public class BlockingChainReconstructorTests
{
    private const int MaxDepth = 50;
    private const int MaxPairs = 5000;
    private const int StepBudget = 100_000;

    private static DateTime TranFor(int spid) => new DateTime(2026, 5, 22, 9, 0, 0).AddSeconds(spid);

    private static BlockingPairRow Pair(
        int blockedSpid, int blockingSpid,
        int? monitorLoop = null, int blockedEcid = 0, int blockingEcid = 0,
        long waitMs = 1000, string blockingStatus = "running", DateTime? at = null)
    {
        return new BlockingPairRow
        {
            EventTime = at ?? new DateTime(2026, 5, 22, 10, 0, 0),
            DatabaseName = "TestDb",
            BlockedSpid = blockedSpid,
            BlockedTranStarted = TranFor(blockedSpid),
            BlockingSpid = blockingSpid,
            BlockingTranStarted = TranFor(blockingSpid),
            MonitorLoop = monitorLoop,
            BlockedEcid = blockedEcid,
            BlockingEcid = blockingEcid,
            WaitTimeMs = waitMs,
            LockMode = "X",
            BlockingStatus = blockingStatus,
            BlockedSqlText = "blocked sql",
            BlockingSqlText = "blocking sql"
        };
    }

    // Default scope is cumulative (monitor_loop ignored); the per-scan tests pass scopeByMonitorLoop: true.
    private static BlockingReconstruction Run(IEnumerable<BlockingPairRow> rows, bool scopeByMonitorLoop = false) =>
        BlockingChainReconstructor.Reconstruct(rows, MaxDepth, MaxPairs, StepBudget, scopeByMonitorLoop);

    [Fact]
    public void Empty_ProducesNoChains()
    {
        var result = Run(Array.Empty<BlockingPairRow>());
        Assert.Empty(result.Chains);
    }

    /// <summary>
    /// #6645: a parallel query's threads write reports with blocked spid = blocking spid (different ecid). That
    /// is the query waiting on itself: no chain, no cycle, and a real chain beside it is unchanged.
    /// </summary>
    [Fact]
    public void ParallelQuerySelfWaits_MakeNoChainAndNoCycle()
    {
        var self = new[]
        {
            Pair(59, 59, blockedEcid: 2, blockingEcid: 1), Pair(59, 59, blockedEcid: 5, blockingEcid: 1),
            Pair(59, 59, blockedEcid: 6, blockingEcid: 1), Pair(59, 59, blockedEcid: 1, blockingEcid: 2),
        };

        var onlySelf = Run(self);
        Assert.Empty(onlySelf.Chains);
        Assert.False(onlySelf.CycleDetected);

        var withReal = Run(self.Append(Pair(52, 51)));
        var chain = Assert.Single(withReal.Chains);
        Assert.Equal(51, chain.ApexSpid);
        Assert.Equal(1, chain.Depth);
        Assert.Equal(1, chain.VictimCount);
        Assert.False(withReal.CycleDetected);
    }

    [Fact]
    public void DepthFourLine_ReportsApexDepthAndVictims()
    {
        // 200 → 201 → 202 → 203 → 204
        var result = Run(new[]
        {
            Pair(201, 200), Pair(202, 201), Pair(203, 202), Pair(204, 203)
        });

        var chain = Assert.Single(result.Chains);
        Assert.Equal(200, chain.ApexSpid);
        Assert.Equal(4, chain.Depth);
        Assert.Equal(4, chain.VictimCount);
        Assert.False(result.CycleDetected);
    }

    [Fact]
    public void FanOut_IsDepthOneWithAllVictims()
    {
        // 300 blocks 301..305 directly
        var result = Run(Enumerable.Range(301, 5).Select(v => Pair(v, 300)));

        var chain = Assert.Single(result.Chains);
        Assert.Equal(300, chain.ApexSpid);
        Assert.Equal(1, chain.Depth);
        Assert.Equal(5, chain.VictimCount);
    }

    [Fact]
    public void DeepChain_WithNullBlockerTran_StillFormsOneChain()
    {
        // The bug this whole change fixes: blocking_last_tran_started is always NULL, so the old (spid, tran)
        // key split a mid-chain node. Keying by spid:ecid within monitor_loop, 116 → 111 → 80 is ONE depth-2
        // chain — 111 unifies as (loop, 111, 0) whether it is the blocker or the blocked party.
        var result = Run(new[]
        {
            Pair(111, 116, monitorLoop: 620365),
            Pair(80, 111, monitorLoop: 620365),
        }, scopeByMonitorLoop: true);

        var chain = Assert.Single(result.Chains);
        Assert.Equal(116, chain.ApexSpid);
        Assert.Equal(2, chain.Depth);
        Assert.Equal(2, chain.VictimCount);
    }

    [Fact]
    public void SeparateEpisodes_SameSpid_DoNotLeak()
    {
        // The same blocker→blocked pair in two different monitor_loops (episodes) must stay two chains, not
        // merge — the cross-episode leakage the monitor_loop scope prevents for the viewer.
        var result = Run(new[]
        {
            Pair(201, 200, monitorLoop: 1),
            Pair(201, 200, monitorLoop: 2),
        }, scopeByMonitorLoop: true);

        Assert.Equal(2, result.Chains.Count);
        Assert.All(result.Chains, c => Assert.Equal(200, c.ApexSpid));
        Assert.Contains(result.Chains, c => c.MonitorLoop == 1);
        Assert.Contains(result.Chains, c => c.MonitorLoop == 2);
    }

    [Fact]
    public void ParallelBlocker_DistinctEcid_AreDistinctNodes()
    {
        // Same SPID, different ecid (parallel workers) are distinct sessions: spid 200 ecid 0 and ecid 1 each
        // head their own chain.
        var result = Run(new[]
        {
            Pair(201, 200, monitorLoop: 1, blockingEcid: 0),
            Pair(202, 200, monitorLoop: 1, blockingEcid: 1),
        }, scopeByMonitorLoop: true);

        Assert.Equal(2, result.Chains.Count);
        Assert.Contains(result.Chains, c => c.ApexEcid == 0);
        Assert.Contains(result.Chains, c => c.ApexEcid == 1);
    }

    [Fact]
    public void Cumulative_MergesAcrossScans()
    {
        // scopeByMonitorLoop:false ignores monitor_loop, so 200 → 201 (scan 1) and 201 → 202 (scan 2) form one
        // depth-2 chain. No analysis caller uses it any more (#6931): it links reused spids across the window.
        var result = Run(new[]
        {
            Pair(201, 200, monitorLoop: 1),
            Pair(202, 201, monitorLoop: 2),
        }, scopeByMonitorLoop: false);

        var chain = Assert.Single(result.Chains);
        Assert.Equal(200, chain.ApexSpid);
        Assert.Equal(2, chain.Depth);
        Assert.Null(chain.MonitorLoop);   // cumulative chains carry no episode
    }

    [Fact]
    public void SpidReuse_DifferentMonitorLoop_DoesNotSplice()
    {
        // SPID 201 reused across two episodes is two distinct sessions, not one spliced chain.
        var result = Run(new[]
        {
            Pair(201, 200, monitorLoop: 1),
            Pair(202, 201, monitorLoop: 1),   // 200 → 201 → 202 in episode 1
            Pair(203, 201, monitorLoop: 2),   // reused 201 heads its own chain in episode 2
        }, scopeByMonitorLoop: true);

        Assert.Equal(2, result.Chains.Count);
        Assert.Contains(result.Chains, c => c.ApexSpid == 200 && c.Depth == 2);
        Assert.Contains(result.Chains, c => c.ApexSpid == 201 && c.Depth == 1);
    }

    [Fact]
    public void PureCycle_IsDetectedAndGetsFallbackRoot()
    {
        // A blocks B, B blocks A — every node is blocked, so there is no apex.
        var result = Run(new[]
        {
            Pair(blockedSpid: 401, blockingSpid: 400),
            Pair(blockedSpid: 400, blockingSpid: 401)
        });

        Assert.True(result.CycleDetected);
        Assert.NotEmpty(result.Chains); // fallback root — the cycle is not silently dropped
    }

    [Fact]
    public void DepthCap_IsFlaggedAndBounded()
    {
        // A 12-edge line, reconstructed with a maxDepth of 4.
        var rows = Enumerable.Range(0, 12).Select(i => Pair(501 + i, 500 + i)).ToList();
        var result = BlockingChainReconstructor.Reconstruct(rows, maxDepth: 4, MaxPairs, StepBudget, scopeByMonitorLoop: false);

        Assert.True(result.DepthCapped);
        var chain = Assert.Single(result.Chains);
        Assert.True(chain.Depth <= 4, $"depth {chain.Depth} should be capped at 4");
    }

    [Fact]
    public void EdgeDedup_KeepsTheLargestWaitTime()
    {
        // Same blocked/blocker pair re-fires with a growing wait time.
        var result = Run(new[]
        {
            Pair(601, 600, waitMs: 1_000),
            Pair(601, 600, waitMs: 9_000),
            Pair(601, 600, waitMs: 4_000)
        });

        var chain = Assert.Single(result.Chains);
        Assert.Equal(9_000, chain.MaxWaitMs);
    }

    [Fact]
    public void SleepingApex_IsReported()
    {
        var result = Run(new[]
        {
            Pair(701, 700, blockingStatus: "sleeping"),
            Pair(702, 701)
        });

        var chain = Assert.Single(result.Chains);
        Assert.Equal(700, chain.ApexSpid);
        Assert.True(chain.ApexSleeping);
    }

    [Fact]
    public void Ranking_PutsTheHigherMagnitudeChainFirst()
    {
        // Chain A: depth 1, 8 victims (wide). Chain B: depth 2, 2 victims (shallow).
        var rows = new List<BlockingPairRow>();
        rows.AddRange(Enumerable.Range(801, 8).Select(v => Pair(v, 800)));   // wide
        rows.Add(Pair(901, 900));                                            // narrow
        rows.Add(Pair(902, 901));

        var result = Run(rows);

        Assert.Equal(2, result.Chains.Count);
        // The 8-victim fan-out out-scores the depth-2 / 2-victim chain.
        Assert.Equal(800, result.Chains[0].ApexSpid);
    }

    [Fact]
    public void ReconstructedOutput_CarriesTranStartedAndDatabaseName()
    {
        // Transaction start is display-only now, sourced from the rows (apex from its first outgoing edge).
        var result = Run(new[] { Pair(201, 200), Pair(202, 201) });

        var chain = Assert.Single(result.Chains);
        Assert.Equal(200, chain.ApexSpid);
        Assert.Equal(TranFor(200), chain.ApexTranStarted);

        Assert.NotEmpty(chain.Levels);
        Assert.All(chain.Levels, l => Assert.Equal("TestDb", l.DatabaseName));

        var top = chain.Levels.Single(l => l.BlockingSpid == 200 && l.BlockedSpid == 201);
        Assert.Equal(TranFor(200), top.BlockingTranStarted);
        Assert.Equal(TranFor(201), top.BlockedTranStarted);
    }

    [Fact]
    public void FindChainForSession_AnyMember_ReturnsChainRootedAtApex()
    {
        // Two separate chains — A: 200 → 201 → 202 ; B: 300 → 301. The viewer scopes to the ONE chain a
        // clicked session belongs to, rooted at its apex regardless of where in the chain it sits.
        var result = Run(new[] { Pair(201, 200), Pair(202, 201), Pair(301, 300) });

        var chainA = BlockingChainReconstructor.FindChainForSession(result, null, 201, 0);
        Assert.NotNull(chainA);
        Assert.Equal(200, chainA!.ApexSpid);

        Assert.Equal(200, BlockingChainReconstructor.FindChainForSession(result, null, 200, 0)!.ApexSpid);
        Assert.Equal(200, BlockingChainReconstructor.FindChainForSession(result, null, 202, 0)!.ApexSpid);
        Assert.Equal(300, BlockingChainReconstructor.FindChainForSession(result, null, 301, 0)!.ApexSpid);
        Assert.Null(BlockingChainReconstructor.FindChainForSession(result, null, 999, 0));
    }

    [Fact]
    public void FindChainForSession_DisambiguatesByMonitorLoop()
    {
        // Victim 500 was blocked by apex 100 in episode 1 and apex 200 in episode 2 — two scoped chains, both
        // containing 500. The clicked row's monitor_loop selects the right one.
        var result = Run(new[]
        {
            Pair(blockedSpid: 500, blockingSpid: 100, monitorLoop: 1),
            Pair(blockedSpid: 500, blockingSpid: 200, monitorLoop: 2)
        }, scopeByMonitorLoop: true);

        Assert.Equal(100, BlockingChainReconstructor.FindChainForSession(result, 1, 500, 0)!.ApexSpid);
        Assert.Equal(200, BlockingChainReconstructor.FindChainForSession(result, 2, 500, 0)!.ApexSpid);
    }

    [Fact]
    public void FindChainForSession_NullMonitorLoop_FallsBackToSpidEcid()
    {
        // A lead-blocker grid row may not supply its monitor_loop; the click must still resolve via spid:ecid
        // rather than reporting "no reconstructable chain" (supersedes the #1222 SPID-only fallback).
        var result = Run(new[]
        {
            Pair(201, 200, monitorLoop: 7),
            Pair(202, 201, monitorLoop: 7),
        }, scopeByMonitorLoop: true);

        var chain = BlockingChainReconstructor.FindChainForSession(result, null, 200, 0);
        Assert.NotNull(chain);
        Assert.Equal(200, chain!.ApexSpid);
    }

    [Fact]
    public void SpidReuse_AcrossScansAnHourApart_IsNoCycle_WhenScopedByScan()
    {
        // #6931: 132 blocks 133 in one scan and 133 blocks 132 an hour later, two unrelated moments. Keyed on
        // spid alone that is a cycle, scoped by scan it is two depth-1 chains.
        var rows = new[]
        {
            Pair(133, 132, monitorLoop: 10, waitMs: 142_000, at: new DateTime(2026, 5, 22, 13, 23, 0)),
            Pair(132, 133, monitorLoop: 90, waitMs: 20_000, at: new DateTime(2026, 5, 22, 14, 23, 0)),
        };

        Assert.True(Run(rows, scopeByMonitorLoop: false).CycleDetected);

        var scoped = Run(rows, scopeByMonitorLoop: true);
        Assert.False(scoped.CycleDetected);
        Assert.Equal(2, scoped.Chains.Count);
        Assert.All(scoped.Chains, c => { Assert.Equal(1, c.Depth); Assert.Equal(1, c.VictimCount); });
    }

    [Fact]
    public void NoMonitorLoop_ScopedRows_AreScopedByTheirEventSecond()
    {
        var rows = new[]
        {
            Pair(133, 132, waitMs: 142_000, at: new DateTime(2026, 5, 22, 13, 23, 0)),
            Pair(132, 133, waitMs: 20_000, at: new DateTime(2026, 5, 22, 14, 23, 0)),
        };

        var scoped = Run(rows, scopeByMonitorLoop: true);
        Assert.False(scoped.CycleDetected);
        Assert.Equal(2, scoped.Chains.Count);

        // Rows of one second stay one scan: a real three-level chain keeps its depth.
        var oneScan = Run(new[] { Pair(11, 10), Pair(12, 11), Pair(13, 12) }, scopeByMonitorLoop: true);
        Assert.Equal(3, Assert.Single(oneScan.Chains).Depth);
    }

    [Fact]
    public void ARealThreeLevelChainInsideOneScan_ReportsDepthThree()
    {
        var result = Run(new[]
        {
            Pair(11, 10, monitorLoop: 4), Pair(12, 11, monitorLoop: 4), Pair(13, 12, monitorLoop: 4),
            Pair(21, 20, monitorLoop: 5)
        }, scopeByMonitorLoop: true);

        var chain = result.Chains.First(c => c.ApexSpid == 10);
        Assert.Equal(3, chain.Depth);
        Assert.Equal(3, chain.VictimCount);
        Assert.False(result.CycleDetected);
    }

    [Theory]
    [InlineData(6)]
    [InlineData(2)]
    public void StepBudget_IsPerRoot_SoALaterChainNeverReportsZeroVictims(int stepBudget)
    {
        // Four heads each block 50, and 50 and 51 block each other: a cyclic graph, so the depth search has no memo.
        // On one shared budget the first head used it all and the other three walked nothing (0 victims, 0 ms).
        var rows = new[]
        {
            Pair(50, 1, waitMs: 4_000), Pair(50, 2, waitMs: 3_000), Pair(50, 3, waitMs: 2_000), Pair(50, 4, waitMs: 1_000),
            Pair(51, 50), Pair(50, 51)
        };

        var result = BlockingChainReconstructor.Reconstruct(rows, MaxDepth, MaxPairs, stepBudget, scopeByMonitorLoop: true);

        Assert.Equal(4, result.Chains.Count);
        Assert.All(result.Chains, c => { Assert.True(c.VictimCount > 0); Assert.True(c.MaxWaitMs > 0); });
        Assert.Equal(stepBudget < 3, result.TraversalTruncated);
    }
}
