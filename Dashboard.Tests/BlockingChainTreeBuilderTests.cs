using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Pure unit tests for the shared <see cref="BlockingChainTreeBuilder"/> — the DAG -> tree logic that
/// turns the analysis engine's edge lists into renderable trees. Tested once in Common (not duplicated
/// per app). No database, no WPF.
/// </summary>
public class BlockingChainTreeBuilderTests
{
    private static DateTime Tran(int spid) => new DateTime(2026, 5, 22, 9, 0, 0).AddSeconds(spid);

    private static BlockingEdgeInput Edge(
        int level, int blocker, int blocked,
        long wait = 1000, string lockMode = "X", string db = "TestDb",
        DateTime? blockerTran = null, DateTime? blockedTran = null,
        int blockerEcid = 0, int blockedEcid = 0) => new()
        {
            Level = level,
            BlockingSpid = blocker,
            BlockingEcid = blockerEcid,
            BlockingTranStarted = blockerTran ?? Tran(blocker),
            BlockedSpid = blocked,
            BlockedEcid = blockedEcid,
            BlockedTranStarted = blockedTran ?? Tran(blocked),
            WaitTimeMs = wait,
            LockMode = lockMode,
            DatabaseName = db,
            BlockingSqlText = $"blocker {blocker}",
            BlockedSqlText = $"blocked {blocked}",
            // Identity keyed per spid so a node's sourced login/host/app is assertable.
            BlockedLoginName = $"login{blocked}",
            BlockedHostName = $"host{blocked}",
            BlockedClientApp = $"app{blocked}",
            BlockingLoginName = $"login{blocker}",
            BlockingHostName = $"host{blocker}",
            BlockingClientApp = $"app{blocker}"
        };

    private static BlockingChainInput Chain(
        int apex, IEnumerable<BlockingEdgeInput> edges,
        double magnitude = 1.0, bool sleeping = false, DateTime? apexTran = null) => new()
        {
            ApexSpid = apex,
            ApexTranStarted = apexTran ?? Tran(apex),
            ApexSleeping = sleeping,
            Magnitude = magnitude,
            Edges = edges.ToList()
        };

    private static BlockingChainModel Build(params BlockingChainInput[] chains) =>
        BlockingChainTreeBuilder.Build(chains, false, false, false);

    private static IEnumerable<BlockingChainNode> Flatten(BlockingChainNode n)
    {
        yield return n;
        foreach (var c in n.Children)
            foreach (var d in Flatten(c))
                yield return d;
    }

    [Fact]
    public void SingleChain_BuildsLinearTree_WithSourcedFields()
    {
        // 200 -> 201 -> 202 -> 203
        var model = Build(Chain(200, new[]
        {
            Edge(1, 200, 201), Edge(2, 201, 202), Edge(3, 202, 203)
        }));

        var root = Assert.Single(model.Roots);
        Assert.Equal(200, root.Spid);
        Assert.True(root.IsApex);
        Assert.Equal(0, root.WaitTimeMs);          // apex waits on no one
        Assert.Equal(string.Empty, root.LockMode);
        Assert.Equal("blocker 200", root.SqlText); // apex SQL sourced from its outgoing edge
        Assert.Equal("TestDb", root.DatabaseName);

        var n201 = Assert.Single(root.Children);
        Assert.Equal(201, n201.Spid);
        Assert.False(n201.IsApex);
        Assert.Equal(1000, n201.WaitTimeMs);
        Assert.Equal("X", n201.LockMode);
        Assert.Equal("blocked 201", n201.SqlText); // victim SQL sourced from its incoming edge

        var n202 = Assert.Single(n201.Children);
        var n203 = Assert.Single(n202.Children);
        Assert.Equal(203, n203.Spid);
        Assert.Empty(n203.Children);
        Assert.Equal(4, Flatten(root).Count());
    }

    [Fact]
    public void BranchingBlocker_AttachesAllVictimsToApex_OrderedBySpid()
    {
        var model = Build(Chain(300, new[]
        {
            Edge(1, 300, 303), Edge(1, 300, 301), Edge(1, 300, 302)
        }));

        var root = Assert.Single(model.Roots);
        Assert.Equal(3, root.Children.Count);
        Assert.Equal(new[] { 301, 302, 303 }, root.Children.Select(c => c.Spid).ToArray());
        Assert.All(root.Children, c => Assert.Empty(c.Children));
    }

    [Fact]
    public void DeepLinearChain_NestsRootToGreatGrandchild_WithIdentity()
    {
        // 1 -> 2 -> 3 -> 4 must render NESTED at every level (root -> child -> grandchild -> great-grandchild),
        // not flattened. Also proves identity is sourced from the right edge side.
        var model = Build(Chain(1, new[] { Edge(1, 1, 2), Edge(2, 2, 3), Edge(3, 3, 4) }));

        var root = Assert.Single(model.Roots);
        Assert.Equal(1, root.Spid);
        Assert.True(root.IsApex);
        // Apex identity comes from the blocking side of its outgoing edge.
        Assert.Equal("login1", root.LoginName);
        Assert.Equal("host1", root.HostName);
        Assert.Equal("app1", root.ClientApp);

        var c2 = Assert.Single(root.Children);
        Assert.Equal(2, c2.Spid);
        Assert.Equal("login2", c2.LoginName);   // victim identity from the blocked side of its incoming edge
        Assert.Equal("host2", c2.HostName);

        var c3 = Assert.Single(c2.Children);
        Assert.Equal(3, c3.Spid);

        var c4 = Assert.Single(c3.Children);
        Assert.Equal(4, c4.Spid);
        Assert.Empty(c4.Children);

        Assert.Equal(4, Flatten(root).Count());
    }

    [Fact]
    public void MultiLevelBranching_NestsEachBranchSeparately()
    {
        // 1 blocks 2 AND 3; 2 blocks 4. Expect 1 -> {2, 3} and 2 -> {4}; no flattening, no duplication.
        var model = Build(Chain(1, new[] { Edge(1, 1, 2), Edge(1, 1, 3), Edge(2, 2, 4) }));

        var root = Assert.Single(model.Roots);
        Assert.Equal(new[] { 2, 3 }, root.Children.Select(c => c.Spid).OrderBy(x => x).ToArray());

        var n2 = root.Children.Single(c => c.Spid == 2);
        var n3 = root.Children.Single(c => c.Spid == 3);

        var n4 = Assert.Single(n2.Children);
        Assert.Equal(4, n4.Spid);
        Assert.Empty(n3.Children);
        Assert.Empty(n4.Children);

        Assert.Equal(4, Flatten(root).Count());   // 1, 2, 3, 4 — each once
    }

    [Fact]
    public void SameSpidDifferentEcid_AreKeptAsSeparateNodes()
    {
        // Apex 200 blocks SPID 201 on two DIFFERENT execution contexts (ecid 0 and 1 — parallel workers).
        // The builder keys on spid:ecid, so it must NOT collapse them into one node.
        var model = Build(Chain(200, new[]
        {
            Edge(1, 200, 201, blockedEcid: 0),
            Edge(1, 200, 201, blockedEcid: 1)
        }));

        var root = Assert.Single(model.Roots);
        Assert.Equal(2, root.Children.Count);
        Assert.All(root.Children, c => Assert.Equal(201, c.Spid));
        Assert.Equal(new[] { 0, 1 }, root.Children.Select(c => c.Ecid).OrderBy(e => e));
    }

    [Fact]
    public void Diamond_AttachesVictimToLowestParentSpid_DroppingExtraInEdge()
    {
        // 400 -> 401, 400 -> 402, and BOTH 401 and 402 block 403 at the same level. Tie on level breaks
        // to the lowest parent SPID (401); the 402 -> 403 in-edge is dropped, and 403 appears once.
        var model = Build(Chain(400, new[]
        {
            Edge(1, 400, 401), Edge(1, 400, 402),
            Edge(2, 401, 403), Edge(2, 402, 403)
        }));

        var root = Assert.Single(model.Roots);
        var n401 = root.Children.Single(c => c.Spid == 401);
        var n402 = root.Children.Single(c => c.Spid == 402);

        Assert.Single(n401.Children);
        Assert.Equal(403, n401.Children[0].Spid);
        Assert.Empty(n402.Children);
        Assert.Equal(4, Flatten(root).Count());        // 403 is not duplicated
        Assert.Single(Flatten(root), n => n.Spid == 403);
    }

    [Fact]
    public void Flags_ArePropagatedToModel()
    {
        var model = BlockingChainTreeBuilder.Build(
            new[] { Chain(200, new[] { Edge(1, 200, 201) }) },
            cycleDetected: true, depthCapped: true, traversalTruncated: true);

        Assert.True(model.CycleDetected);
        Assert.True(model.DepthCapped);
        Assert.True(model.TraversalTruncated);
    }

    [Fact]
    public void SleepingApex_IsFlaggedOnRoot()
    {
        var model = Build(Chain(200, new[] { Edge(1, 200, 201) }, sleeping: true));
        var root = Assert.Single(model.Roots);
        Assert.True(root.IsApex);
        Assert.True(root.IsApexSleeping);
    }

    [Fact]
    public void Roots_AreRankedByMagnitudeDescending()
    {
        var weak = Chain(200, new[] { Edge(1, 200, 201) }, magnitude: 0.2);
        var strong = Chain(300, new[] { Edge(1, 300, 301) }, magnitude: 0.9);

        var model = BlockingChainTreeBuilder.Build(new[] { weak, strong }, false, false, false);

        Assert.Equal(2, model.Roots.Count);
        Assert.Equal(300, model.Roots[0].Spid);   // higher magnitude first
        Assert.Equal(0.9, model.Roots[0].Magnitude);
        Assert.Equal(200, model.Roots[1].Spid);
    }

    [Fact]
    public void Cycle_DoesNotInfiniteLoop_AndPlacesEachNodeOnce()
    {
        // 500 -> 501 -> 502, plus a back-edge 502 -> 500. The visited set must stop the cycle and the
        // apex must never be re-parented; each session appears once.
        var model = BlockingChainTreeBuilder.Build(
            new[]
            {
                Chain(500, new[]
                {
                    Edge(1, 500, 501), Edge(2, 501, 502), Edge(3, 502, 500)
                })
            },
            cycleDetected: true, depthCapped: false, traversalTruncated: false);

        var root = Assert.Single(model.Roots);
        Assert.Equal(500, root.Spid);
        Assert.Equal(3, Flatten(root).Count());                 // 500, 501, 502 — no duplicate 500
        Assert.Single(Flatten(root), n => n.Spid == 500);
    }
}
