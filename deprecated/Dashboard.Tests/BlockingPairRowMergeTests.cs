using System;
using System.Collections.Generic;
using PerformanceMonitor.Analysis;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Unit tests for BlockingPairRowMerge — merging always-on DMV-snapshot pair-rows into blocked-process-report
/// rows: BPR preferred, DMV-only filled in (the AWS RDS / threshold-unset case), overlapping edges deduped.
/// No database.
/// </summary>
public class BlockingPairRowMergeTests
{
    private static readonly DateTime BaseTime = new(2026, 5, 22, 10, 0, 0);

    private static BlockingPairRow Pair(int blockedSpid, int blockingSpid, string source,
        DateTime? eventTime = null, int blockedEcid = 0, int blockingEcid = 0) =>
        new()
        {
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
        Assert.Equal(3, primary.Count);
        Assert.All(primary, r => Assert.Equal("DMV", r.BlockedSqlText));
    }

    [Fact]
    public void OverlappingEdge_PrefersBprAndDropsDmv()
    {
        // Same edge (blocked/blocker spid:ecid, same minute) from both sources -> only the BPR row survives.
        var primary = new List<BlockingPairRow> { Pair(201, 200, "BPR") };
        BlockingPairRowMerge.MergeInto(primary, new[] { Pair(201, 200, "DMV") });
        Assert.Equal("BPR", Assert.Single(primary).BlockedSqlText);
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
