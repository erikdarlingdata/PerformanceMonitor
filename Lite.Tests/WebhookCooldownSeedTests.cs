using System;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #1145: the shared <see cref="WebhookAlertService"/> must seed its per-(serverId, metricName)
/// cooldown from alert history on first use, so a Teams/Slack alert posted shortly before an app
/// restart is not re-posted on the first post-restart sweep — the guarantee #981 gave the email
/// channel. The cooldown is time-bounded (EmailCooldownMinutes), so it only covers a restart
/// inside the cooldown window; the time-independent edge-trigger watermark persistence (Lite)
/// covers the rest. These tests use a dead webhook URL: a SUPPRESSED post never touches the
/// network (the cooldown short-circuits first), while an ATTEMPTED post fails against the dead
/// URL and increments the Teams failure counter — the observable proxy for "did it try to post".
/// </summary>
public class WebhookCooldownSeedTests
{
    private static WebhookAlertService MakeService(IAlertHistoryStore? history, FakeWebhookSettings settings)
        => new(settings, EmailAlertService.Branding, new AppLoggerAdapter<WebhookAlertService>(), history);

    private static FakeWebhookSettings EnabledTeamsSettings() => new()
    {
        TeamsWebhookEnabled = true,
        TeamsWebhookUrl = "http://localhost:1/never", // closed port -> connection refused, fast deterministic failure
        EmailCooldownMinutes = 15
    };

    [Fact]
    public async Task SeedsCooldownFromHistory_WithinWindow_SuppressesRepostAfterRestart()
    {
        // A webhook delivered "just now", then a restart (fresh service = empty in-memory cooldown).
        var history = new FakeHistoryStore { LastWebhookSent = DateTime.UtcNow };
        var svc = MakeService(history, EnabledTeamsSettings());

        var sent = await svc.TrySendWebhookAlertsAsync("Deadlocks Detected", "Srv", "4", "1", "1");

        Assert.False(sent);                                        // suppressed
        Assert.Equal(1, history.GetLastWebhookSentCallCount);      // the seed was consulted
        Assert.Equal(0, svc.GetTeamsHealth().ConsecutiveFailures); // and NO post was attempted
    }

    [Fact]
    public async Task SeedFromHistory_OlderThanCooldown_DoesNotSuppress()
    {
        // gotqn's repro: the restart is 17 min after the send, beyond the 15-min cooldown. The
        // cooldown seed must NOT suppress here — that's exactly why the Lite watermark persistence
        // is also needed. The post is attempted (and fails against the dead URL).
        var history = new FakeHistoryStore { LastWebhookSent = DateTime.UtcNow.AddMinutes(-17) };
        var svc = MakeService(history, EnabledTeamsSettings());

        var sent = await svc.TrySendWebhookAlertsAsync("Deadlocks Detected", "Srv", "4", "1", "1");

        Assert.False(sent);                                        // dead URL -> post failed
        Assert.Equal(1, history.GetLastWebhookSentCallCount);      // seed consulted
        Assert.Equal(1, svc.GetTeamsHealth().ConsecutiveFailures); // but it WAS attempted (not suppressed)
    }

    [Fact]
    public async Task NullHistoryStore_NoSeed_AttemptsPost()
    {
        // The legacy/test path passes no history store: pre-#1145 in-memory-only cooldown, so a
        // fresh service attempts the post.
        var svc = MakeService(history: null, EnabledTeamsSettings());

        var sent = await svc.TrySendWebhookAlertsAsync("Deadlocks Detected", "Srv", "4", "1", "1");

        Assert.False(sent);
        Assert.Equal(1, svc.GetTeamsHealth().ConsecutiveFailures);
    }

    private sealed class FakeWebhookSettings : IAlertSettings
    {
        public bool SmtpEnabled => false;
        public string SmtpServer => "";
        public int SmtpPort => 25;
        public bool SmtpUseSsl => false;
        public string SmtpUsername => "";
        public string SmtpFromAddress => "";
        public string SmtpRecipients => "";
        public string? GetSmtpPassword() => null;
        public int EmailCooldownMinutes { get; set; } = 15;
        public bool TeamsWebhookEnabled { get; set; }
        public string TeamsWebhookUrl { get; set; } = "";
        public string TeamsProxyAddress => "";
        public bool SlackWebhookEnabled { get; set; }
        public string SlackWebhookUrl { get; set; } = "";
        public string SlackProxyAddress => "";
        public bool GenericWebhookEnabled { get; set; }
        public string GenericWebhookUrl { get; set; } = "";
        public string GenericWebhookHeadersJson { get; set; } = "";
        public string GenericWebhookBodyTemplate { get; set; } = "";
        public string GenericWebhookProxyAddress => "";
        public bool PagerDutyEnabled { get; set; }
        public string PagerDutyRoutingKey { get; set; } = "";
        public bool PagerDutyUseEuRegion { get; set; }
        public string PagerDutyProxyAddress => "";
        public double AnalysisNotifySeverity => 1.5;
        public int AnalysisNotifyCooldownMinutes => 360;
    }

    private sealed class FakeHistoryStore : IAlertHistoryStore
    {
        public DateTime? LastWebhookSent { get; set; }
        public int GetLastWebhookSentCallCount { get; private set; }

        /// <summary>When set (#1154), the seed applies ONLY to this dedup key; other keys seed null.
        /// Null (default) returns <see cref="LastWebhookSent"/> for any call — the pre-#1154 shape.</summary>
        public string? SeededDedupKey { get; set; }

        public Task RecordAlertAsync(AlertHistoryRecord record) => Task.CompletedTask;
        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null)
        {
            GetLastWebhookSentCallCount++;
            if (SeededDedupKey is not null && dedupKey != SeededDedupKey)
                return Task.FromResult<DateTime?>(null);
            return Task.FromResult(LastWebhookSent);
        }
        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
    }

    private static AlertContext ContextWith(string dedupKey) => new()
    {
        Incidents = new System.Collections.Generic.List<AlertIncident>
        {
            new(dedupKey, new[] { "db.dbo.T" })
        }
    };

    [Fact]
    public async Task DistinctFingerprint_NotSuppressedByAnotherIncidentsCooldown()
    {
        // #1154: incident X was delivered "just now"; a DISTINCT incident Y arrives within the window.
        // Y must be attempted (it fails against the dead URL) — not throttled by X's cooldown.
        var history = new FakeHistoryStore { LastWebhookSent = DateTime.UtcNow, SeededDedupKey = "X" };
        var svc = MakeService(history, EnabledTeamsSettings());

        var sent = await svc.TrySendWebhookAlertsAsync(
            "Deadlocks Detected", "Srv", "4", "1", "1", ContextWith("Y"));

        Assert.False(sent);                                        // dead URL -> attempted, failed
        Assert.Equal(1, svc.GetTeamsHealth().ConsecutiveFailures); // ATTEMPTED, not suppressed
    }

    [Fact]
    public async Task SameFingerprint_SuppressedByItsOwnSeededCooldown()
    {
        // The same incident X, seeded "just now" -> suppressed (no network touch).
        var history = new FakeHistoryStore { LastWebhookSent = DateTime.UtcNow, SeededDedupKey = "X" };
        var svc = MakeService(history, EnabledTeamsSettings());

        var sent = await svc.TrySendWebhookAlertsAsync(
            "Deadlocks Detected", "Srv", "4", "1", "1", ContextWith("X"));

        Assert.False(sent);                                        // suppressed
        Assert.Equal(0, svc.GetTeamsHealth().ConsecutiveFailures); // NOT attempted
    }
}
