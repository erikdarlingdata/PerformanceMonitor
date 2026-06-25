using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// #1154: the Dashboard JSON store's per-fingerprint cooldown seed. Proves the in-memory scan
/// restricts to rows whose ContextJson carries the #1140 dedup key, isolates per-fingerprint, and
/// — critically — does NOT NRE on the many null-ContextJson rows (tray/muted) the scan also visits
/// (the round-1 MAJOR). A unique serverId keeps the test isolated from any on-disk alert_history.json
/// the store loads in its constructor.
/// </summary>
public class JsonAlertHistoryStoreDedupKeyTests
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

    private static string JsonWith(string dedupKey)
    {
        var ctx = new AlertContext
        {
            Incidents = new List<AlertIncident> { new(dedupKey, new[] { "db.dbo.T" }) }
        };
        return AlertContextSerializer.Serialize(ctx);
    }

    private static Task RecordAsync(JsonAlertHistoryStore store, string serverId, string metric, string type, string? contextJson)
        => store.RecordAlertAsync(new AlertHistoryRecord(
            serverId, "Srv", metric, "4", "1", null, null,
            AlertSent: true, NotificationType: type, SendError: null,
            Muted: false, DetailText: null, ContextJson: contextJson));

    [Fact]
    public async Task GetLastSentUtc_WithDedupKey_FiltersToFingerprint_AndIgnoresNullContextRows()
    {
        var store = new JsonAlertHistoryStore(new FakePreferencesService());
        var srv = "test-" + Guid.NewGuid().ToString("N"); // unique -> isolated from on-disk history

        // Email rows: AAA earlier, BBB later, then a null-context "email" row (must not NRE, must be
        // excluded by a dedupKey filter, but still counts for the metric-level seed).
        await RecordAsync(store, srv, "Deadlocks Detected", "email", JsonWith("aaaa1111"));
        await Task.Delay(10, TestContext.Current.CancellationToken);
        await RecordAsync(store, srv, "Deadlocks Detected", "email", JsonWith("bbbb2222"));
        await Task.Delay(10, TestContext.Current.CancellationToken);
        await RecordAsync(store, srv, "Deadlocks Detected", "email", null);

        var lastAaa = await store.GetLastEmailSentUtcAsync(srv, "Deadlocks Detected", "aaaa1111");
        var lastBbb = await store.GetLastEmailSentUtcAsync(srv, "Deadlocks Detected", "bbbb2222");
        var lastCcc = await store.GetLastEmailSentUtcAsync(srv, "Deadlocks Detected", "cccc3333");
        var lastMetric = await store.GetLastEmailSentUtcAsync(srv, "Deadlocks Detected"); // metric-level (null key)

        Assert.NotNull(lastAaa);
        Assert.NotNull(lastBbb);
        Assert.Null(lastCcc);                            // no such fingerprint
        Assert.True(lastAaa!.Value < lastBbb!.Value);    // the filter isolates per-fingerprint
        Assert.NotNull(lastMetric);
        Assert.True(lastMetric!.Value >= lastBbb.Value); // metric-level still sees the later null-context row

        // Webhook channel parity.
        await RecordAsync(store, srv, "Blocking Detected", "webhook", JsonWith("dddd4444"));
        Assert.NotNull(await store.GetLastWebhookSentUtcAsync(srv, "Blocking Detected", "dddd4444"));
        Assert.Null(await store.GetLastWebhookSentUtcAsync(srv, "Blocking Detected", "eeee5555"));
    }
}
