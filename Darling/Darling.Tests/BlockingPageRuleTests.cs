// Copyright (c) 2026 Darling Data, LLC. All rights reserved.
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7083: the page rule both engines use, the shipped bars, and the Cleared text. The SQL Server path is
/// driven through the engine in AlertEngineTests. The PostgreSQL path calls the same
/// <see cref="BlockingPageRule.Evaluate"/> with distinct root blockers as its events and each root's worst
/// victim wait as the event wait, so these cases are the PostgreSQL rule too.
/// </summary>
public class BlockingPageRuleTests
{
    private static bool Pages(int count, long worstWaitMs, int countThreshold = 3, int waitBar = 60) =>
        BlockingPageRule.Evaluate(count, countThreshold, worstWaitMs, waitBar,
            watermark: 0, cooldownElapsed: true, suppressed: false, wasActive: false).Fire;

    [Fact]
    public void OneTwoSecondEvent_DoesNotPage() => Assert.False(Pages(1, 2_000));

    [Fact]
    public void OneEventAtTheWaitBar_Pages_AndJustUnderDoesNot()
    {
        Assert.True(Pages(1, 90_000));
        Assert.True(Pages(1, 60_000));
        Assert.False(Pages(1, 25_000));
    }

    [Fact]
    public void ThreeEventsWithA20SecondWorstWait_Page_AndWithA5SecondWorstWaitDoNot()
    {
        Assert.True(Pages(3, 20_000));
        Assert.True(Pages(3, BlockingPageRule.CountArmWorstWaitFloorMs));
        Assert.False(Pages(3, 5_000));
        Assert.False(Pages(2, 20_000));
    }

    [Fact]
    public void AWaitBarOfZero_TurnsTheSingleEventRouteOff()
    {
        Assert.False(Pages(1, 90_000, waitBar: 0));
        Assert.True(Pages(3, 20_000, waitBar: 0));
    }

    [Fact]
    public void AnOpenIncident_StaysOpenWhileTheCountHolds_SoClearTimingIsUnchanged()
    {
        var held = BlockingPageRule.Evaluate(4, 3, 5_000, 60, watermark: 4, cooldownElapsed: true, suppressed: false, wasActive: true);
        Assert.False(held.Fire);
        Assert.True(held.Active);

        var neverOpened = BlockingPageRule.Evaluate(4, 3, 5_000, 60, watermark: 0, cooldownElapsed: true, suppressed: false, wasActive: false);
        Assert.False(neverOpened.Fire);
        Assert.False(neverOpened.Active);

        var cleared = BlockingPageRule.Evaluate(2, 3, 5_000, 60, watermark: 4, cooldownElapsed: true, suppressed: false, wasActive: true);
        Assert.False(cleared.Active);
    }

    [Fact]
    public void TheShippedBars_AreTheSameInDarlingTheViewerAndTheSharedConstants()
    {
        var alerts = new DarlingConfig().Alerts;
        Assert.Equal(3, BlockingPageRule.CountThresholdDefault);
        Assert.Equal(60, BlockingPageRule.WaitSecondsThresholdDefault);
        Assert.Equal(3, PostgresAlertEvaluator.BlockingCountThresholdDefault);
        Assert.Equal(BlockingPageRule.CountThresholdDefault, alerts.BlockingCountThreshold);
        Assert.Equal(BlockingPageRule.WaitSecondsThresholdDefault, alerts.BlockingWaitSecondsThreshold);
        Assert.Equal(PostgresAlertEvaluator.BlockingCountThresholdDefault, alerts.PgBlockingCountThreshold);

        var row = new AlertSettingsRow();
        Assert.Equal(BlockingPageRule.CountThresholdDefault, row.BlockingCountThreshold);
        Assert.Equal(BlockingPageRule.WaitSecondsThresholdDefault, row.BlockingWaitSecondsThreshold);
        Assert.Equal(PostgresAlertEvaluator.BlockingCountThresholdDefault, row.PgBlockingCountThreshold);
    }

    [Fact]
    public void TheClearedText_NamesTheWindowAndWhatClearsIt()
    {
        var text = BlockingPageRule.ClearedMessage("SRV-A", 3, 60);
        Assert.Contains("last 60 minutes", text);
        Assert.Contains("fewer than 3 blocked events", text);
        Assert.Contains("60s", text);
        Assert.DoesNotContain("waited", BlockingPageRule.ClearedMessage("SRV-A", 3, 0));
    }
}
