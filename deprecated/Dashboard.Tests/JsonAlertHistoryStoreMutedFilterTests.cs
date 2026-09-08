using System;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// #1225: muted alerts and resolution notices ("... Cleared/Resolved/Restored") are written to
/// history for audit but must not count toward the sidebar Alert badge. Proves GetAlertHistory's
/// includeMuted/includeResolved filters drop those rows, that the defaults still return them for
/// the history grid / MCP tool, and — the regression that bit the reporter — that both filters run
/// BEFORE the row limit, so noise can't evict a real alert from the counted window. A unique
/// serverId keeps the test isolated from any on-disk alert_history.json the store loads in its
/// constructor.
/// </summary>
public class JsonAlertHistoryStoreMutedFilterTests
{
    /// <summary>Minimal IUserPreferencesService over one UserPreferences instance (mirrors the other Dashboard tests).</summary>
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

    private static Task RecordAsync(JsonAlertHistoryStore store, string serverId, string metric, bool muted)
        => store.RecordAlertAsync(new AlertHistoryRecord(
            serverId, "Srv", metric, "4", "1", null, null,
#pragma warning disable CS0618
            Delivery: AlertDelivery.FromLegacyStoredColumns(!muted, muted ? "muted" : "tray", null),
#pragma warning restore CS0618
            Muted: muted, DetailText: null, ContextJson: null));

    [Fact]
    public async Task GetAlertHistory_ExcludeMuted_DropsMutedRows_ButDefaultKeepsThem()
    {
        var store = new JsonAlertHistoryStore(new FakePreferencesService());
        var srv = "test-" + Guid.NewGuid().ToString("N"); // unique -> isolated from on-disk history

        await RecordAsync(store, srv, "Long Running Query", muted: false);
        await RecordAsync(store, srv, "Long Running Query", muted: true);
        await RecordAsync(store, srv, "Deadlocks Detected", muted: false);
        await RecordAsync(store, srv, "Long Running Query", muted: true);

        var all = store.GetAlertHistory(hoursBack: 24, limit: 100);                            // history grid / MCP
        var actionable = store.GetAlertHistory(hoursBack: 24, limit: 100, includeMuted: false); // sidebar badge

        Assert.Equal(4, all.Count(a => a.ServerId == srv));            // default: muted rows still shown for audit
        var mine = actionable.Where(a => a.ServerId == srv).ToList();
        Assert.Equal(2, mine.Count);                                   // only the two unmuted alerts count
        Assert.All(mine, a => Assert.False(a.Muted));
    }

    [Fact]
    public async Task GetAlertHistory_ExcludeMuted_FiltersBeforeLimit_SoMutedNoiseCannotEvictRealAlert()
    {
        var store = new JsonAlertHistoryStore(new FakePreferencesService());
        var srv = "test-" + Guid.NewGuid().ToString("N");

        // One real alert first (oldest), then a flood of newer muted noise (the reporter's scenario:
        // a muted trace-reader session re-firing every cooldown).
        await RecordAsync(store, srv, "Deadlocks Detected", muted: false);
        await Task.Delay(10, TestContext.Current.CancellationToken);
        for (int i = 0; i < 5; i++)
            await RecordAsync(store, srv, "Long Running Query", muted: true);

        // Newest-first ordering + a small limit: the muted flood crowds the real alert out of the
        // window when muted rows are included (this is what made the old badge under/over-count).
        var withMuted = store.GetAlertHistory(hoursBack: 24, limit: 3);
        Assert.DoesNotContain(withMuted, a => a.ServerId == srv && !a.Muted);

        // Excluding muted BEFORE the limit keeps the real alert inside the counted window (#1225).
        var actionable = store.GetAlertHistory(hoursBack: 24, limit: 3, includeMuted: false);
        var mine = actionable.Where(a => a.ServerId == srv).ToList();
        Assert.Single(mine);
        Assert.False(mine[0].Muted);
    }

    [Fact]
    public async Task GetAlertHistory_ExcludeResolved_DropsResolutionRows_FiltersBeforeLimit()
    {
        var store = new JsonAlertHistoryStore(new FakePreferencesService());
        var srv = "test-" + Guid.NewGuid().ToString("N");

        // One real alert first (oldest), then a flood of newer resolution notices. "Capture Restored"
        // and "Server Restored" are included deliberately: the pre-#1225 classifier matched only
        // Cleared/Resolved, so those "Restored" rows would have slipped into the badge count.
        await RecordAsync(store, srv, "Deadlocks Detected", muted: false);
        await Task.Delay(10, TestContext.Current.CancellationToken);
        foreach (var resolved in new[] { "Blocking Cleared", "CPU Resolved", "Capture Restored", "tempdb Space Resolved", "Server Restored" })
            await RecordAsync(store, srv, resolved, muted: false);

        // Default includes resolution rows for audit (history grid / MCP); a small limit fills with them.
        var all = store.GetAlertHistory(hoursBack: 24, limit: 3);
        Assert.DoesNotContain(all, a => a.ServerId == srv && !AlertMetricClassifier.IsResolution(a.MetricName));

        // Excluding resolution rows before the limit keeps the one real alert in the counted window.
        var actionable = store.GetAlertHistory(hoursBack: 24, limit: 3, includeResolved: false);
        var mine = actionable.Where(a => a.ServerId == srv).ToList();
        Assert.Single(mine);
        Assert.Equal("Deadlocks Detected", mine[0].MetricName);
    }
}
