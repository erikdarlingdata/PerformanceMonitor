using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Unit tests for BlockingPairRowMerge — merging always-on DMV-snapshot pair-rows into blocked-process-report
/// rows: BPR preferred, DMV-only filled in (the AWS RDS / threshold-unset case), overlapping edges kept from both sources.
/// No database.
/// </summary>
public class BlockingPairRowMergeTests
{
    private static readonly DateTime BaseTime = new(2026, 5, 22, 10, 0, 0);

    private static BlockingPairRow Pair(int blockedSpid, int blockingSpid, string source,
        DateTime? eventTime = null, int blockedEcid = 0, int blockingEcid = 0, int? monitorLoop = null) =>
        new()
        {
            MonitorLoop = monitorLoop,
            EventTime = eventTime ?? BaseTime,
            DatabaseName = "TestDb",
            BlockedSpid = blockedSpid,
            BlockingSpid = blockingSpid,
            BlockedEcid = blockedEcid,
            BlockingEcid = blockingEcid,
            LockMode = "X",
            BlockingStatus = "running",
            BlockedSqlText = source, // tag the origin so assertions can tell which row survived
            BlockingSqlText = source
        };

    [Fact]
    public void EmptySnapshots_LeavesPrimaryUnchanged()
    {
        var primary = new List<BlockingPairRow> { Pair(201, 200, "BPR") };
        BlockingPairRowMerge.MergeInto(primary, Array.Empty<BlockingPairRow>());
        Assert.Equal("BPR", Assert.Single(primary).BlockedSqlText);
    }

    [Fact]
    public void EmptyPrimary_AddsAllSnapshots()
    {
        // The RDS / threshold-unset case: no blocked-process reports, the DMV snapshot is the only source.
        var primary = new List<BlockingPairRow>();
        BlockingPairRowMerge.MergeInto(primary, new[] { Pair(201, 200, "DMV"), Pair(203, 202, "DMV") });
        Assert.Equal(2, primary.Count);
        Assert.All(primary, r => Assert.Equal("DMV", r.BlockedSqlText));
    }

    [Fact]
    public void OverlappingEdge_KeepsBothSources()
    {
        // Same edge (blocked/blocker spid:ecid, same minute) from both sources: both stay. Reconstruction is per scan
        // and a BPR row and a DMV row never share a scan, so neither is a double count (#6931).
        var primary = new List<BlockingPairRow> { Pair(201, 200, "BPR") };
        BlockingPairRowMerge.MergeInto(primary, new[] { Pair(201, 200, "DMV") });
        Assert.Equal(new[] { "BPR", "DMV" }, primary.Select(r => r.BlockedSqlText));
    }

    [Fact]
    public void BprSeesOnlyTheTail_DmvSeesTheWholeChain_TheWorstChainIsTheThreeSessionOne()
    {
        // BPR reports only B (spid 2) blocks C (spid 3): B's own wait on A (spid 1) was under the report threshold.
        // The DMV snapshot in the same minute sees A blocks B and B blocks C. Dropping the DMV B-to-C edge split the
        // chain into A-to-B and B-to-C, so A-to-B-to-C (depth 2, apex A) was never rebuilt (#6931).
        var primary = new List<BlockingPairRow> { Pair(3, 2, "BPR", BaseTime.AddSeconds(5), monitorLoop: 7) };
        var dmv = new[]
        {
            Pair(2, 1, "DMV", BaseTime, monitorLoop: -7),
            Pair(3, 2, "DMV", BaseTime, monitorLoop: -7)
        };

        BlockingPairRowMerge.MergeInto(primary, dmv);
        var reconstruction = BlockingChainReconstructor.Reconstruct(
            primary, maxDepth: 50, maxPairs: 5000, stepBudget: 100_000, scopeByMonitorLoop: true);

        var worst = BlockingChainFactBuilder.PickWorst(reconstruction.Chains);
        Assert.Equal(2, worst.Depth);
        Assert.Equal(1, worst.ApexSpid);
    }

    [Fact]
    public void DistinctEdge_AddsDmvRow()
    {
        var primary = new List<BlockingPairRow> { Pair(201, 200, "BPR") };
        BlockingPairRowMerge.MergeInto(primary, new[] { Pair(301, 300, "DMV") });
        Assert.Equal(2, primary.Count);
    }

    [Fact]
    public void SameSpidsDifferentMinute_AreNotDeduped()
    {
        var primary = new List<BlockingPairRow> { Pair(201, 200, "BPR", BaseTime) };
        BlockingPairRowMerge.MergeInto(primary, new[] { Pair(201, 200, "DMV", BaseTime.AddMinutes(5)) });
        Assert.Equal(2, primary.Count);
    }

    [Fact]
    public void SameSpidDifferentEcid_AreNotDeduped()
    {
        // Parallel workers (distinct ecid) are distinct edges, not duplicates.
        var primary = new List<BlockingPairRow> { Pair(201, 200, "BPR", blockingEcid: 0) };
        BlockingPairRowMerge.MergeInto(primary, new[] { Pair(201, 200, "DMV", blockingEcid: 1) });
        Assert.Equal(2, primary.Count);
    }
}
