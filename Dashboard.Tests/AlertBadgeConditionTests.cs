using System;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Guards #754/#749: the server-level (Overview) tab badge must light up for an active low-disk
/// breach or a recent failed Agent job — not only for blocking/deadlock/CPU/memory — and the
/// acknowledge baseline must keep it hidden until a NEW such condition appears (false -> true).
/// The Lite badge path shares the same condition shape (UpdateAlertCounts OR-ing the two flags),
/// so this covers both apps' intent; the Lite-specific UI plumbing is verified live.
/// </summary>
public class AlertBadgeConditionTests
{
    /// <summary>Minimal IUserPreferencesService over one mutable UserPreferences instance.</summary>
    private sealed class FakePreferencesService : IUserPreferencesService
    {
        public UserPreferences Preferences { get; } = new();
        public UserPreferences GetPreferences() => Preferences;
        public void SavePreferences(UserPreferences preferences) { }
        public void UpdateWaitStatsRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
        public void UpdateCpuRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
        public void UpdateMemoryRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
        public void UpdateFileIoRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
        public void UpdateExpensiveQueriesRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
        public void UpdateBlockingRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
        public void UpdateCollectionHealthRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
    }

    private static ServerHealthStatus HealthyStatus() =>
        new(new ServerConnection { Id = "srv-1", ServerName = "SQL1", DisplayName = "SQL1" }) { IsOnline = true };

    [Fact]
    public void HasAnyAlertCondition_LowDiskOnly_IsTrue()
    {
        var svc = new AlertStateService(new FakePreferencesService());
        var status = HealthyStatus();
        status.HasLowDiskAlert = true;
        Assert.True(svc.HasAnyAlertCondition(status));
    }

    [Fact]
    public void HasAnyAlertCondition_FailedJobOnly_IsTrue()
    {
        var svc = new AlertStateService(new FakePreferencesService());
        var status = HealthyStatus();
        status.HasFailedJobAlert = true;
        Assert.True(svc.HasAnyAlertCondition(status));
    }

    [Fact]
    public void HasAnyAlertCondition_NoConditions_IsFalse()
    {
        var svc = new AlertStateService(new FakePreferencesService());
        Assert.False(svc.HasAnyAlertCondition(HealthyStatus()));
    }

    [Fact]
    public void ShouldShowBadge_LowDisk_ShowsThenSuppressedByAcknowledge_ReturnsOnNewCondition()
    {
        var svc = new AlertStateService(new FakePreferencesService());
        var status = HealthyStatus();
        status.HasLowDiskAlert = true;

        // The standing low-disk breach lights the Overview badge.
        Assert.True(svc.ShouldShowBadge("srv-1", "Overview", status));

        // Acknowledging snapshots the baseline; the same unchanged breach is now suppressed.
        svc.AcknowledgeAlert("srv-1", "Overview", status);
        Assert.False(svc.ShouldShowBadge("srv-1", "Overview", status));

        // A newly-appearing failed-job condition is worse than the baseline -> badge returns.
        status.HasFailedJobAlert = true;
        Assert.True(svc.ShouldShowBadge("srv-1", "Overview", status));
    }

    [Fact]
    public void ShouldShowBadge_LowDisk_AutoClearsWhenResolved()
    {
        var svc = new AlertStateService(new FakePreferencesService());
        var status = HealthyStatus();
        status.HasLowDiskAlert = true;

        svc.AcknowledgeAlert("srv-1", "Overview", status);
        Assert.False(svc.ShouldShowBadge("srv-1", "Overview", status));

        // Disk recovers: condition gone, baseline auto-clears, badge stays hidden.
        status.HasLowDiskAlert = false;
        Assert.False(svc.ShouldShowBadge("srv-1", "Overview", status));

        // A fresh breach after the auto-clear shows the badge again (no stale ack).
        status.HasLowDiskAlert = true;
        Assert.True(svc.ShouldShowBadge("srv-1", "Overview", status));
    }
}
