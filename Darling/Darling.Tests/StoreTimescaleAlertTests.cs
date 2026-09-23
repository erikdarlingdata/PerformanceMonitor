using System;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3908: the self-alert for a store whose TimescaleDB extension is not the runtime's version. The update runs
/// before the store opens and never reverts anything, so what an operator needs from this alert is which version
/// the store is actually running, which one the runtime ships, why it did not move, and that the store is up.
/// </summary>
public sealed class StoreTimescaleAlertTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task AFailedUpdate_FiresCritical_UnderTheUpgradeMetric_OnItsOwnKey()
    {
        var h = new DarlingSelfAlertTests.Harness();
        await h.Build().EvaluateStoreTimescaleAsync(
            new DarlingSelfAlertEvaluator.StoreTimescaleReport(
                Failed: true, FromVersion: "2.28.1", ToVersion: "2.30.1",
                FailureMessage: "no update path from version \"2.28.1\" to version \"2.30.1\" of extension \"timescaledb\""),
            Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(DarlingSelfAlertEvaluator.StoreUpgradeMetric, fired.MetricName);
        Assert.Equal("storetimescale", fired.ServerKey);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.Contains("FAILED", fired.ShortMessage, StringComparison.Ordinal);
        Assert.Contains("TimescaleDB 2.28.1", fired.ShortMessage, StringComparison.Ordinal);
        Assert.Equal("TimescaleDB 2.28.1", fired.CurrentValue);
        Assert.Equal("TimescaleDB 2.30.1", fired.ThresholdValue);

        /* The reason survives, and the operator learns the store is up on the carried library. */
        Assert.Contains("no update path", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("still carries", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("retried on the next service start", fired.DetailText, StringComparison.Ordinal);

        /* Nothing is reverted on this path, so the alert must never say anything was. */
        Assert.DoesNotContain("revert", fired.DetailText, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("revert", fired.ShortMessage, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AStoreBehindAfterStart_FiresCritical_AndSaysNothingRanThisStart()
    {
        var h = new DarlingSelfAlertTests.Harness();
        await h.Build().EvaluateStoreTimescaleAsync(
            new DarlingSelfAlertEvaluator.StoreTimescaleReport(false, "2.28.1", "2.30.1", null), Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("storetimescale", fired.ServerKey);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.Equal("store is on TimescaleDB 2.28.1, runtime ships TimescaleDB 2.30.1", fired.ShortMessage);
        Assert.Contains("has not moved", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("Nothing moved it this start", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("Reason:", fired.DetailText, StringComparison.Ordinal);
    }

    /// <summary>
    /// A start that upgrades the PostgreSQL major and then cannot move the extension has two things to say. The
    /// two alerts share a metric and must not share a key, or one would silence the other.
    /// </summary>
    [Fact]
    public async Task AMajorUpgradeAndAFailedExtensionUpdate_OnOneStart_RaiseTwoAlerts()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var evaluator = h.Build();

        await evaluator.EvaluateStoreUpgradeAsync(
            new DarlingSelfAlertEvaluator.StoreUpgradeReport(
                Succeeded: true, FromMajor: 17, ToMajor: 18, FromTimescale: "2.28.1", ToTimescale: null,
                FailedStep: null, FailureMessage: null, WithoutRollbackCopy: false),
            Ct);
        await evaluator.EvaluateStoreTimescaleAsync(
            new DarlingSelfAlertEvaluator.StoreTimescaleReport(true, "2.28.1", "2.30.1", "boom"), Ct);

        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Equal("storeupgrade", h.Deliverer.Outcomes[0].ServerKey);
        Assert.Equal("storetimescale", h.Deliverer.Outcomes[1].ServerKey);
        Assert.Equal(AlertSeverityLevel.Warning, h.Deliverer.Outcomes[0].Severity);
        Assert.Equal(AlertSeverityLevel.Critical, h.Deliverer.Outcomes[1].Severity);
    }

    /// <summary>Versions are identities, not quantities (#1881): the stored pair is the state-only sentinel.</summary>
    [Fact]
    public async Task StoresTheStateOnlySentinel_NotAVersionNumber()
    {
        var h = new DarlingSelfAlertTests.Harness();
        await h.Build().EvaluateStoreTimescaleAsync(
            new DarlingSelfAlertEvaluator.StoreTimescaleReport(true, "2.28.1", "2.30.1", "boom"), Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(0.0, AlertValueParser.ResolveStoredValue(fired.NumericCurrentValue, fired.CurrentValue));
        Assert.Equal(0.0, AlertValueParser.ResolveStoredValue(fired.NumericThresholdValue, fired.ThresholdValue));
        Assert.True(AlertMetricClassifier.IsStateOnly(fired.MetricName));
    }

    /// <summary>A store whose version could not be read still gets a sentence that reads, not "TimescaleDB (null)".</summary>
    [Fact]
    public async Task AnUnreadVersion_ReadsAsProse()
    {
        var h = new DarlingSelfAlertTests.Harness();
        await h.Build().EvaluateStoreTimescaleAsync(
            new DarlingSelfAlertEvaluator.StoreTimescaleReport(true, null, "2.30.1", "could not start the cluster."), Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains("its current TimescaleDB", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("Reason: could not start the cluster.", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("..", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("null", fired.DetailText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task NothingFires_WhenAlertsAreOff()
    {
        var h = new DarlingSelfAlertTests.Harness();
        h.Settings.AlertsEnabled = false;
        await h.Build().EvaluateStoreTimescaleAsync(
            new DarlingSelfAlertEvaluator.StoreTimescaleReport(true, "2.28.1", "2.30.1", "boom"), Ct);

        Assert.Empty(h.Deliverer.Outcomes);
    }
}
