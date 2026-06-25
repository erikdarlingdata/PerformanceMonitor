using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Guards <see cref="LowDiskAlertGate"/> (#754 follow-up): a standing full volume must not
/// re-alert every cooldown — only a fresh or meaningfully-worsening breach fires. Without this,
/// the low-disk alert re-recorded an identical alert-history row every cooldown, which made
/// Alert-History "Dismiss" appear broken (the dismissed row was replaced by a newer one). The
/// gate is shared by Lite and Dashboard, so this one suite covers both apps' behaviour.
/// </summary>
public class LowDiskAlertGateTests
{
    [Fact]
    public void FreshBreach_NoWatermark_Alerts()
    {
        Assert.True(LowDiskAlertGate.ShouldAlert(currentWorstFreePercent: 8.0, lastAlertedFreePercent: null));
    }

    [Fact]
    public void UnchangedLevel_DoesNotReAlert()
    {
        Assert.False(LowDiskAlertGate.ShouldAlert(8.0, 8.0));
    }

    [Fact]
    public void SlightlyImproved_DoesNotReAlert()
    {
        Assert.False(LowDiskAlertGate.ShouldAlert(8.4, 8.0));
    }

    [Fact]
    public void WorsenedWithinJitterMargin_DoesNotReAlert()
    {
        // Dropped 0.5pp (< 1.0 default margin) — jitter, not a genuine decline.
        Assert.False(LowDiskAlertGate.ShouldAlert(7.5, 8.0));
    }

    [Fact]
    public void WorsenedExactlyMargin_ReAlerts()
    {
        // Dropped exactly the 1.0pp default margin.
        Assert.True(LowDiskAlertGate.ShouldAlert(7.0, 8.0));
    }

    [Fact]
    public void WorsenedBeyondMargin_ReAlerts()
    {
        Assert.True(LowDiskAlertGate.ShouldAlert(5.5, 8.0));
    }

    [Theory]
    [InlineData(8.0, 8.0, 2.0, false)] // unchanged, custom larger margin
    [InlineData(6.0, 8.0, 2.0, true)]  // dropped exactly the custom margin
    [InlineData(6.5, 8.0, 2.0, false)] // dropped 1.5pp (< 2.0 custom margin)
    public void RespectsCustomMargin(double current, double last, double margin, bool expected)
    {
        Assert.Equal(expected, LowDiskAlertGate.ShouldAlert(current, last, margin));
    }

    /// <summary>
    /// #1136: the critical tier is graded on EITHER dimension (OR semantics, matching the breach
    /// test) — a low percentage on a huge volume OR a few GB free on any volume is CRITICAL.
    /// </summary>
    [Theory]
    [InlineData(2.0, 100.0, true)]  // 2% <= 3% floor (huge volume, low %) -> critical
    [InlineData(3.0, 100.0, true)]  // exactly at the percent floor
    [InlineData(4.0, 100.0, false)] // 4% above floor, plenty of GB -> warning tier
    [InlineData(8.0, 1.5, true)]    // healthy %, but 1.5 GB <= 2 GB floor -> critical
    [InlineData(8.0, 2.0, true)]    // exactly at the GB floor
    [InlineData(8.0, 5.0, false)]   // above both floors -> warning tier
    public void IsCriticallyLow_GradesEitherDimension(double freePercent, double freeGb, bool expected)
    {
        Assert.Equal(expected, LowDiskAlertGate.IsCriticallyLow(freePercent, freeGb));
    }
}
