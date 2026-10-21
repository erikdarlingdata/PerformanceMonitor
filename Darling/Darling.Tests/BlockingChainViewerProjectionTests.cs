using System;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Viewer;

using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6292: the Block Chain window prints each session's transaction start, which the pair rows hold on the monitored
/// server's own clock. The projection hands each stamp to the app's display conversion (the Server / Local / UTC mode the
/// grids honour) instead of letting the window print the stored clock whatever the mode.
/// </summary>
public class BlockingChainViewerProjectionTests
{
    private static BlockingPairRow Pair(int blocked, int blocking, DateTime blockedTran, DateTime blockingTran) => new()
    {
        EventTime = new DateTime(2026, 5, 22, 10, 0, 0),
        DatabaseName = "TestDb",
        BlockedSpid = blocked,
        BlockedTranStarted = blockedTran,
        BlockingSpid = blocking,
        BlockingTranStarted = blockingTran,
        WaitTimeMs = 1000,
        LockMode = "X",
        BlockingStatus = "running",
        MonitorLoop = 1,
    };

    [Fact]
    public void TransactionStarts_GoThroughTheDisplayConversion_ForEveryNodeOfTheTree()
    {
        var apexTran = new DateTime(2026, 5, 22, 2, 30, 0);
        var victimTran = new DateTime(2026, 5, 22, 2, 45, 0);
        var rows = new[] { Pair(58, 55, victimTran, apexTran) };
        var reconstruction = BlockingChainReconstructor.Reconstruct(rows, 50, 5000, 100_000, scopeByMonitorLoop: true);

        var stored = BlockingChainViewerProjection.BuildModelForSession(reconstruction, 1, 58, 0)!;
        Assert.Equal(apexTran, stored.Roots[0].TranStarted);

        /* A server seven hours behind UTC, shown in UTC mode: the window must say 09:30, not the stored 02:30. */
        var shown = BlockingChainViewerProjection.BuildModelForSession(reconstruction, 1, 58, 0, t => t.AddHours(7))!;
        Assert.Equal(apexTran.AddHours(7), shown.Roots[0].TranStarted);
        Assert.Equal(victimTran.AddHours(7), Assert.Single(shown.Roots[0].Children).TranStarted);
    }
}
