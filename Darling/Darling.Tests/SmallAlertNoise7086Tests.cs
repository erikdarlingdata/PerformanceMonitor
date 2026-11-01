/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7086: the small alert types that paged with little or no content. Deadlocks (the monitor's own sessions leave the
/// count, and one page per episode) and the PostgreSQL Long-Running Query alert (no page from an old capture, and the
/// statement in the detail text).
/// </summary>
public sealed class SmallAlertNoise7086Tests
{
    private const string Deadlocks = "Deadlocks Detected";

    private static AlertEngineTests.Harness DeadlockHarness()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.DeadlockEnabled = true;
        h.Settings.DeadlockCountThreshold = 1;
        h.Settings.CooldownMinutes = 5;
        return h;
    }

    private static DeadlockAlertRow RowFor(string clientApp)
    {
        var row = AlertEngineTests.DeadlockRow();
        row.DeadlockGraphXml = row.DeadlockGraphXml.Replace(
            "<process id=\"process1\"", $"<process id=\"process1\" clientapp=\"{clientApp}\"", StringComparison.Ordinal);
        return row;
    }

    private static async Task SweepAsync(AlertEngineTests.Harness h, AlertEngine engine, int minutes = 6)
    {
        h.Now = h.Now.AddMinutes(minutes);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
    }

    [Theory]
    [InlineData("PerformanceMonitorDarling")]
    [InlineData("PerformanceMonitorLite")]
    [InlineData("performancemonitordarling-Remediation")]
    public async Task Deadlock_WithTheMonitorsOwnSession_DoesNotAlert(string clientApp)
    {
        var h = DeadlockHarness();
        h.Adapter.Deadlocks.Add(RowFor(clientApp));

        await SweepAsync(h, h.Build());

        Assert.DoesNotContain(h.Deliverer.Outcomes, o => o.MetricName == Deadlocks);
    }

    [Fact]
    public async Task Deadlock_OfAnApplicationThatIsNotTheMonitor_StillAlerts()
    {
        var h = DeadlockHarness();
        h.Adapter.Deadlocks.Add(RowFor("MyApp"));

        await SweepAsync(h, h.Build());

        Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == Deadlocks);
    }

    [Fact]
    public async Task Deadlock_TheMonitorsRowLeavesTheCount_NotTheRealOnesBesideIt()
    {
        var h = DeadlockHarness();
        h.Adapter.Deadlocks.Add(RowFor("PerformanceMonitorDarling"));
        h.Adapter.Deadlocks.Add(RowFor("MyApp"));

        await SweepAsync(h, h.Build());

        var outcome = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == Deadlocks);
        Assert.Equal("1 deadlock in the last hour", outcome.ShortMessage);
    }

    /* The victim (process1) is the user's session and the monitor's session (process2) survived. */
    private static DeadlockAlertRow RowWithMonitorSurvivor(string survivorApp = "PerformanceMonitorDarling")
    {
        var row = AlertEngineTests.DeadlockRow();
        row.DeadlockGraphXml = row.DeadlockGraphXml.Replace(
            "<process id=\"process1\"", "<process id=\"process1\" clientapp=\"MyApp\"", StringComparison.Ordinal)
            .Replace("<process id=\"process2\"", $"<process id=\"process2\" clientapp=\"{survivorApp}\"", StringComparison.Ordinal);
        return row;
    }

    [Theory]
    [InlineData("PerformanceMonitorDarling")]
    [InlineData("PerformanceMonitorLite")]
    public async Task Deadlock_WhereAUserSessionIsTheVictim_AndTheMonitorSurvived_StillAlerts(string survivor)
    {
        var h = DeadlockHarness();
        h.Adapter.Deadlocks.Add(RowWithMonitorSurvivor(survivor));

        await SweepAsync(h, h.Build());

        var outcome = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == Deadlocks);
        Assert.Equal("1 deadlock in the last hour", outcome.ShortMessage);
    }

    [Fact]
    public async Task Deadlock_WhereTheMonitorIsTheVictim_AndAUserSessionSurvived_StaysLeftOut()
    {
        var h = DeadlockHarness();
        var row = AlertEngineTests.DeadlockRow();
        row.DeadlockGraphXml = row.DeadlockGraphXml.Replace(
            "<process id=\"process1\"", "<process id=\"process1\" clientapp=\"PerformanceMonitorLite\"", StringComparison.Ordinal)
            .Replace("<process id=\"process2\"", "<process id=\"process2\" clientapp=\"MyApp\"", StringComparison.Ordinal);
        h.Adapter.Deadlocks.Add(row);

        await SweepAsync(h, h.Build());

        Assert.DoesNotContain(h.Deliverer.Outcomes, o => o.MetricName == Deadlocks);
    }

    [Fact]
    public void VictimIsMonitorSession_ReadsTheVictimList_WhenTheStoredVictimIdIsBlank()
    {
        var monitorVictim = RowFor("PerformanceMonitorDarling").DeadlockGraphXml;
        var userVictim = RowWithMonitorSurvivor().DeadlockGraphXml;

        Assert.True(MonitorApplicationNames.VictimIsMonitorSession(monitorVictim, ""));
        Assert.False(MonitorApplicationNames.VictimIsMonitorSession(userVictim, ""));
        Assert.False(MonitorApplicationNames.VictimIsMonitorSession(userVictim, "process1"));
        /* No victim can be found in the graph: it is never called the monitor's. */
        Assert.False(MonitorApplicationNames.VictimIsMonitorSession(monitorVictim.Replace("victimProcess id=\"process1\"", "victimProcess id=\"nope\"", StringComparison.Ordinal), ""));
    }

    [Fact]
    public void MonitorApplicationNames_AreTheOnesTheCollectorsConnectWith()
    {
        Assert.True(MonitorApplicationNames.IsMonitorApplication(MonitorApplicationNames.Darling));
        Assert.True(MonitorApplicationNames.IsMonitorApplication(MonitorApplicationNames.Lite));
        Assert.False(MonitorApplicationNames.IsMonitorApplication("Microsoft SQL Server Management Studio"));
        Assert.False(MonitorApplicationNames.IsMonitorApplication(null));
        Assert.False(MonitorApplicationNames.VictimIsMonitorSession("not xml", "process1"));
    }

    [Fact]
    public async Task Deadlock_SixCountIncreasesInsideOneEpisode_PageOnce_AndTheNextPageNeedsACleared()
    {
        var h = DeadlockHarness();
        var engine = h.Build();

        for (var i = 0; i < 7; i++)
        {
            h.Adapter.Deadlocks.Add(RowFor("MyApp"));
            await SweepAsync(h, engine);
        }

        Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == Deadlocks);
        Assert.DoesNotContain(h.Resolutions, r => r.MetricName == Deadlocks);

        /* The window empties: the episode clears. */
        h.Adapter.Deadlocks.Clear();
        await SweepAsync(h, engine);
        Assert.Contains(h.Resolutions, r => r.MetricName == Deadlocks);

        /* The next deadlock is a new episode and pages. */
        h.Adapter.Deadlocks.Add(RowFor("MyApp"));
        await SweepAsync(h, engine);
        Assert.Equal(2, h.Deliverer.Outcomes.Count(o => o.MetricName == Deadlocks));
    }

    [Fact]
    public async Task Deadlock_ARiseToCritical_InsideTheEpisode_StillPages()
    {
        var h = DeadlockHarness();
        var engine = h.Build();

        h.Adapter.Deadlocks.Add(RowFor("MyApp"));
        await SweepAsync(h, engine);
        for (var i = 0; i < 24; i++)
        {
            h.Adapter.Deadlocks.Add(RowFor("MyApp"));
        }

        await SweepAsync(h, engine);

        var fires = h.Deliverer.Outcomes.Where(o => o.MetricName == Deadlocks).ToList();
        Assert.Equal(2, fires.Count);
        Assert.Equal(AlertSeverityLevel.Critical, fires[1].Severity);
    }

    private static DarlingPgSessionStatesReader.LongRunningSessionRow Session(string? statement) =>
        new(1001, 100, "sales", "appuser", "myapp", "SELECT", 2_100_000, DateTime.UtcNow, statement);

    /* The bar is the larger of two cycles of the collector's cadence and 10 minutes: the field case (a 16-minute-old sample at
       the default 1-minute cadence) is stale, a sweep running a few minutes behind under load is not. */
    [Theory]
    [InlineData(16, 1, true)]
    [InlineData(11, 1, true)]
    [InlineData(10, 1, false)]
    [InlineData(5, 1, false)]
    [InlineData(3, 1, false)]
    [InlineData(16, 5, true)]
    [InlineData(10, 5, false)]
    [InlineData(20, 10, false)]
    [InlineData(21, 10, true)]
    [InlineData(30, 15, false)]
    [InlineData(31, 15, true)]
    public void PgLongRunning_ASampleOlderThanTheLargerOfTwoCyclesAndTenMinutes_IsStale(int ageMinutes, int cadenceMinutes, bool stale)
    {
        var now = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

        Assert.Equal(stale, DarlingWorker.IsPgLongRunningSampleStale(now.AddMinutes(-ageMinutes), now, cadenceMinutes));
    }

    [Fact]
    public void PgLongRunning_ASampleWithNoCaptureTime_IsNotStale()
    {
        Assert.False(DarlingWorker.IsPgLongRunningSampleStale(null, DateTime.UtcNow, 1));
    }

    [Fact]
    public void PgLongRunning_TheDetailTextNamesTheStatement_AtFull()
    {
        var item = DarlingWorker.BuildPgLongRunningStatementItem(Session("SELECT * FROM orders WHERE id = 42"), McpDataDetail.Full);
        var context = new AlertContext();
        context.Details.Add(item!);

        var text = AlertContextBuilders.ContextToDetailText(context);

        Assert.Contains("Statement: SELECT * FROM orders WHERE id = 42", text);
        Assert.Contains("pid 100 running 35m (SELECT)", text);
    }

    [Fact]
    public void PgLongRunning_TheStatementIsMaskedAtMaskLiterals_AndLeftOutAtMetadataOnly()
    {
        var masked = DarlingWorker.BuildPgLongRunningStatementItem(Session("SELECT * FROM orders WHERE id = 42"), McpDataDetail.MaskLiterals);
        var statement = masked!.Fields.Single(f => f.Label == "Statement").Value;

        Assert.DoesNotContain("42", statement);
        Assert.Contains("orders", statement);
        Assert.Null(DarlingWorker.BuildPgLongRunningStatementItem(Session("SELECT 1"), McpDataDetail.MetadataOnly));
    }

    [Fact]
    public void PgLongRunning_NoStoredText_GivesNoStatementItem()
    {
        Assert.Null(DarlingWorker.BuildPgLongRunningStatementItem(Session(null), McpDataDetail.Full));
        Assert.Null(DarlingWorker.BuildPgLongRunningStatementItem(Session("  "), McpDataDetail.Full));
    }

    /* ---------------- Runnable Queue: only Critical pages ---------------- */

    private static AlertEngineTests.Harness RunnableHarness(double average)
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.RunnableQueueEnabled = true;
        for (var i = 0; i < 5; i++)
        {
            h.Adapter.RunnableQueue.Add(new RunnableQueueSample(h.Now.AddSeconds(-10).AddMinutes(-i), average, (long)Math.Round(average * 4), 4));
        }

        return h;
    }

    [Fact]
    public async Task RunnableQueue_AWarning_IsHeld_NotPaged()
    {
        var h = RunnableHarness(10);

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var outcome = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(AlertSeverityLevel.Warning, outcome.Severity);
        Assert.Equal("Warning: pages at Critical only", outcome.Hold!.Reason);
    }

    [Fact]
    public async Task RunnableQueue_AWarning_InsideAHighCpuIncident_IsHeldWithThatReason()
    {
        var h = RunnableHarness(10);
        h.Settings.CpuEnabled = true;
        h.Settings.RunnableQueueEnabled = false;
        var engine = h.Build();

        /* The High CPU incident opens first (its gate needs several fresh samples at or over the bar). */
        for (var i = 0; i < 4; i++)
        {
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot(sqlCpu: 99, totalCpu: 99));
        }

        /* Then the Warning-level queue shows up inside it. */
        h.Settings.RunnableQueueEnabled = true;
        h.Deliverer.Outcomes.Clear();
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot(sqlCpu: 99, totalCpu: 99));

        var held = h.Deliverer.Outcomes.Single(o => o.MetricName == RunnableQueueEvaluator.MetricName);
        Assert.Equal("inside a High CPU incident", held.Hold!.Reason);
    }

    [Fact]
    public async Task RunnableQueue_ACritical_Pages_AndIsNotHeld()
    {
        var h = RunnableHarness(20);

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var outcome = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(AlertSeverityLevel.Critical, outcome.Severity);
        Assert.Null(outcome.Hold);
    }

    [Fact]
    public async Task RunnableQueue_ACriticalRightAfterAHeldWarning_PagesAtOnce()
    {
        var h = RunnableHarness(10);
        var engine = h.Build();
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        h.Now = h.Now.AddMinutes(1);
        h.Adapter.RunnableQueue.Clear();
        for (var i = 0; i < 5; i++)
        {
            h.Adapter.RunnableQueue.Add(new RunnableQueueSample(h.Now.AddSeconds(-10).AddMinutes(-i), 20, 80, 4));
        }

        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.NotNull(h.Deliverer.Outcomes[0].Hold);
        Assert.Null(h.Deliverer.Outcomes[1].Hold);
    }

    /* ---------------- Forced Plan Failing: pages when it fails again ---------------- */

    private static ForcePlanFailureInfo Failure(DateTime observed, long planId = 22) => new()
    {
        DatabaseName = "Sales", QueryId = 11, PlanId = planId, ForcingType = "AUTO", FailureReason = "NO_PLAN",
        FailureDelta = 1, TotalFailures = 1, ObservedAtUtc = observed,
    };

    [Fact]
    public async Task ForcePlan_AFailureThatHappensOnce_IsHeld_AndItsRecoveryIsNotAnnounced()
    {
        var h = new AlertEngineTests.Harness();
        var engine = h.Build();
        h.Adapter.ForcePlanFailures.Add(Failure(h.Now.AddMinutes(-1)));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        /* The same collection re-read: still one held row. Then the plan stops failing. */
        h.Now = h.Now.AddMinutes(1);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        h.Adapter.ForcePlanFailures.Clear();
        h.Now = h.Now.AddMinutes(10);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var outcome = Assert.Single(h.Deliverer.Outcomes);
        Assert.NotNull(outcome.Hold);
        Assert.DoesNotContain(h.Resolutions, r => r.MetricName == "Forced Plan Failing");
    }

    [Fact]
    public async Task ForcePlan_FailingTwiceInADay_PagesOnce()
    {
        var h = new AlertEngineTests.Harness();
        var engine = h.Build();
        h.Adapter.ForcePlanFailures.Add(Failure(h.Now.AddMinutes(-1)));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        /* It recovers, then fails again 3 hours later. */
        h.Adapter.ForcePlanFailures.Clear();
        h.Now = h.Now.AddMinutes(10);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        h.Now = h.Now.AddHours(3);
        h.Adapter.ForcePlanFailures.Add(Failure(h.Now.AddMinutes(-1)));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.NotNull(h.Deliverer.Outcomes[0].Hold);
        Assert.Null(h.Deliverer.Outcomes[1].Hold);
    }

    [Fact]
    public async Task ForcePlan_TwoRisesOnConsecutiveCollections_Page()
    {
        var h = new AlertEngineTests.Harness();
        var engine = h.Build();
        h.Adapter.ForcePlanFailures.Add(Failure(h.Now.AddMinutes(-1)));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        h.Now = h.Now.AddMinutes(6);
        h.Adapter.ForcePlanFailures.Clear();
        h.Adapter.ForcePlanFailures.Add(Failure(h.Now.AddMinutes(-1)));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Null(h.Deliverer.Outcomes[1].Hold);
    }

    [Fact]
    public async Task ForcePlan_ASecondFailureMoreThanADayAfterTheFirst_IsHeldAgain()
    {
        var h = new AlertEngineTests.Harness();
        var engine = h.Build();
        h.Adapter.ForcePlanFailures.Add(Failure(h.Now.AddMinutes(-1)));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        h.Adapter.ForcePlanFailures.Clear();
        h.Now = h.Now.AddMinutes(10);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        h.Now = h.Now.AddHours(25);
        h.Adapter.ForcePlanFailures.Add(Failure(h.Now.AddMinutes(-1)));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.All(h.Deliverer.Outcomes, o => Assert.NotNull(o.Hold));
    }
}
