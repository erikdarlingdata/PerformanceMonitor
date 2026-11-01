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

    [Fact]
    public void MonitorApplicationNames_AreTheOnesTheCollectorsConnectWith()
    {
        Assert.True(MonitorApplicationNames.IsMonitorApplication(MonitorApplicationNames.Darling));
        Assert.True(MonitorApplicationNames.IsMonitorApplication(MonitorApplicationNames.Lite));
        Assert.False(MonitorApplicationNames.IsMonitorApplication("Microsoft SQL Server Management Studio"));
        Assert.False(MonitorApplicationNames.IsMonitorApplication(null));
        Assert.False(MonitorApplicationNames.GraphHasMonitorSession("not xml"));
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

    [Theory]
    [InlineData(16, 5, true)]
    [InlineData(11, 5, true)]
    [InlineData(10, 5, false)]
    [InlineData(9, 5, false)]
    [InlineData(3, 1, true)]
    [InlineData(2, 1, false)]
    public void PgLongRunning_ASampleOlderThanTwoCycles_IsStale(int ageMinutes, int cadenceMinutes, bool stale)
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
}
