// Copyright (c) 2026 Darling Data, LLC. All rights reserved.
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Storage;
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

    /* #7083 part 2: the held-fire gate both engines share. */

    [Fact]
    public void ACountAtTheBarWithEveryWaitUnder15Seconds_IsHeldBack_OnceUnderTheOneEventBar()
    {
        Assert.True(BlockingPageRule.IsHeldBack(14, 3, 9_000, 60, wasActive: false, suppressed: false));
        Assert.True(BlockingPageRule.IsHeldBack(3, 3, 14_999, 60, wasActive: false, suppressed: false));
        Assert.False(BlockingPageRule.IsHeldBack(2, 3, 9_000, 60, wasActive: false, suppressed: false), "under the count bar");
        Assert.False(BlockingPageRule.IsHeldBack(3, 3, 15_000, 60, wasActive: false, suppressed: false), "the count arm pages");
        Assert.False(BlockingPageRule.IsHeldBack(1, 1, 90_000, 60, wasActive: false, suppressed: false), "the one-event bar pages");
        Assert.False(BlockingPageRule.IsHeldBack(14, 0, 9_000, 60, wasActive: false, suppressed: false), "a count bar of 0 is off");
        Assert.False(BlockingPageRule.IsHeldBack(14, 3, 9_000, 60, wasActive: true, suppressed: false), "an incident is open");
        Assert.False(BlockingPageRule.IsHeldBack(14, 3, 9_000, 60, wasActive: false, suppressed: true), "the server is silenced");
    }

    [Fact]
    public void TryHold_WritesOneRowPerBurst_AtMostOnePerCooldown_AndNeverTouchesThePageCooldown()
    {
        var stamps = new ConcurrentDictionary<string, DateTime>();
        var seen = new ConcurrentDictionary<string, DateTime>();
        var t0 = new DateTime(2026, 8, 31, 12, 0, 0, DateTimeKind.Utc);
        var burst = t0.AddMinutes(-3);
        var cooldown = TimeSpan.FromMinutes(5);

        Assert.True(BlockingPageRule.TryHold(stamps, seen, "pg-a", t0, cooldown, burst, 14, 3, 9_000, 60, false, false));
        Assert.False(BlockingPageRule.TryHold(stamps, seen, "pg-a", t0.AddMinutes(4), cooldown, burst.AddMinutes(3), 14, 3, 9_000, 60, false, false), "inside the cooldown, even with a newer event");
        Assert.True(BlockingPageRule.TryHold(stamps, seen, "pg-b", t0.AddMinutes(4), cooldown, burst, 14, 3, 9_000, 60, false, false), "another server");
        Assert.False(BlockingPageRule.TryHold(stamps, seen, "pg-a", t0.AddMinutes(5), cooldown, burst, 14, 3, 9_000, 60, false, false), "cooldown over, same events");
        Assert.False(BlockingPageRule.TryHold(stamps, seen, "pg-a", t0.AddMinutes(6), cooldown, burst.AddMilliseconds(1500), 14, 3, 9_000, 60, false, false), "a wait within the jitter of the last one is the same event");
        Assert.True(BlockingPageRule.TryHold(stamps, seen, "pg-a", t0.AddMinutes(10), cooldown, t0.AddMinutes(6), 14, 3, 9_000, 60, false, false), "cooldown over and a newer event");

        /* A held row followed by one event over the bar: the page rule has no memory of the hold. */
        var page = BlockingPageRule.Evaluate(15, 3, 90_000, 60, watermark: 0, cooldownElapsed: true, suppressed: false, wasActive: false);
        Assert.True(page.Fire);
    }

    [Fact]
    public void TheHoldReason_SaysCountWorstWaitAndChain()
    {
        Assert.Equal("14 blocking events, worst wait 9 s, longest chain 2", BlockingPageRule.HoldReason(14, 9_000, 2));
        Assert.Equal("1 blocking event, worst wait 0.4 s, longest chain 1", BlockingPageRule.HoldReason(1, 400, 0));
        Assert.Equal("4 root blockers, worst wait 12.5 s, longest chain 3", BlockingPageRule.HoldReason(4, 12_500, 3, rootBlockers: true));
    }

    [Fact]
    public void TheLongestChain_CountsBlockedSessionsBehindARoot_AndSurvivesALoop()
    {
        Assert.Equal(0, BlockingPageRule.LongestChain(Array.Empty<(int, int)>()));
        Assert.Equal(1, BlockingPageRule.LongestChain(new[] { (10, 1), (11, 2) }));
        Assert.Equal(3, BlockingPageRule.LongestChain(new[] { (10, 1), (20, 10), (30, 20), (99, 98) }));
        Assert.Equal(0, BlockingPageRule.LongestChain(new[] { (5, 5) })); /* a self-wait is not a chain */
        Assert.Equal(1, BlockingPageRule.LongestChain(new[] { (1, 2), (2, 1) }));
    }

    private static DarlingPgBlockingReader.PgBlockingChainRow PgRoot(long backendId, long worstWaitMs, int depth) =>
        new(new DateTime(2026, 8, 31, 12, 0, 0, DateTimeKind.Utc), backendId, (int)backendId, new[] { "sales" }, "app", "webapi",
            "active", "SELECT 1", false, 5_000, 5_000, 3, 3, depth, worstWaitMs, "SELECT 2", 1, false, false);

    [Fact]
    public void PgHeldBlocking_CarriesTheRootCountWorstWaitAndDeepestChain_AsADigestHold()
    {
        var roots = Enumerable.Range(1, 5).Select(i => PgRoot(i, i == 3 ? 9_000 : 2_000, i == 2 ? 2 : 1)).ToList();

        var outcome = DarlingWorker.PgBlockingHoldOutcome("pg-a", "PG A", roots, 3, new DateTime(2026, 8, 31, 12, 1, 0, DateTimeKind.Utc));

        Assert.Equal("Blocking Detected", outcome.MetricName);
        Assert.Equal("5 root blockers, worst wait 9 s, longest chain 2", outcome.Hold!.Reason);
        Assert.False(outcome.Muted);
        Assert.Equal("5", outcome.CurrentValue);
        Assert.Equal("3", outcome.ThresholdValue);
        /* The rule that chose to hold it: 5 roots at the count bar of 3, worst wait 9 s, nothing at 15 s or 60 s. */
        Assert.True(BlockingPageRule.IsHeldBack(5, 3, 9_000, 60, wasActive: false, suppressed: false));
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
