using System.Collections.Generic;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The reconciler's dominant wait family skips a contributor the Azure young-baseline bar left out, unless
/// every contributor was left out.
/// </summary>
public sealed class ReconcilerDominantWaitFamilyTests
{
    private static Dictionary<string, double> Meta(bool markFirst, bool markSecond) => new()
    {
        ["contrib_REMOTE_BLOCK_IO"] = 900_000,
        ["contrib_LCK_M_S_XACT_MODIFY"] = 270_000,
        ["bar_excluded_REMOTE_BLOCK_IO"] = markFirst ? 1 : 0,
        ["bar_excluded_LCK_M_S_XACT_MODIFY"] = markSecond ? 1 : 0,
    };

    [Fact]
    public void Marked_largest_is_skipped_and_next_wins()
    {
        Assert.Equal("LCK", AnomalyIncidentReconciler.DominantWaitFamily(Meta(true, false)));
    }

    [Fact]
    public void No_marker_keeps_the_largest()
    {
        var m = new Dictionary<string, double>
        {
            ["contrib_REMOTE_BLOCK_IO"] = 900_000,
            ["contrib_LCK_M_S_XACT_MODIFY"] = 270_000,
        };
        Assert.Equal("REMOTE_BLOCK_IO", AnomalyIncidentReconciler.DominantWaitFamily(m));
    }

    [Fact]
    public void All_marked_falls_back_to_the_largest()
    {
        Assert.Equal("REMOTE_BLOCK_IO", AnomalyIncidentReconciler.DominantWaitFamily(Meta(true, true)));
    }
}
