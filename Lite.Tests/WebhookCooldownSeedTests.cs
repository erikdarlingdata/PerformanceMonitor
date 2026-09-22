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

        var result = await svc.TrySendWebhookAlertsAsync("Deadlocks Detected", "Srv", "4", "1", "1");

        Assert.False(result.Sent);                                 // suppressed
        Assert.Equal(1, history.GetLastWebhookSentCallCount);      // the seed was consulted
        Assert.Equal(0, svc.GetTeamsHealth().ConsecutiveFailures); // and NO post was attempted

        /* #3427: the suppression names its mechanism. This fixture and
           SeedFromHistory_OlderThanCooldown_DoesNotSuppress differ only in how old the seeded send is, and
           they come back Throttled and Failed — the discrimination the alert log could not make. */
        Assert.Equal(AlertChannelOutcome.Throttled, result.Outcome);
        Assert.Null(result.SendError);
    }

    [Fact]
    public async Task SeedFromHistory_OlderThanCooldown_DoesNotSuppress()
    {
        // gotqn's repro: the restart is 17 min after the send, beyond the 15-min cooldown. The
        // cooldown seed must NOT suppress here — that's exactly why the Lite watermark persistence
        // is also needed. The post is attempted (and fails against the dead URL).
        var history = new FakeHistoryStore { LastWebhookSent = DateTime.UtcNow.AddMinutes(-17) };
        var svc = MakeService(history, EnabledTeamsSettings());

        var result = await svc.TrySendWebhookAlertsAsync("Deadlocks Detected", "Srv", "4", "1", "1");

        Assert.False(result.Sent);                                 // dead URL -> post failed
        Assert.Equal(1, history.GetLastWebhookSentCallCount);      // seed consulted
        Assert.Equal(1, svc.GetTeamsHealth().ConsecutiveFailures); // but it WAS attempted (not suppressed)

        /* #3427: Failed, not Throttled — and the two fixtures differ only in the seeded send's age, so the
           answer can only have come from the mechanism. Both used to be one bool's false. */
        Assert.Equal(AlertChannelOutcome.Failed, result.Outcome);
        Assert.NotNull(result.SendError);
        Assert.StartsWith("Teams: ", result.SendError);
    }

    [Fact]
    public async Task NullHistoryStore_NoSeed_AttemptsPost()
    {
        // The legacy/test path passes no history store: pre-#1145 in-memory-only cooldown, so a
        // fresh service attempts the post.
        var svc = MakeService(history: null, EnabledTeamsSettings());

        var result = await svc.TrySendWebhookAlertsAsync("Deadlocks Detected", "Srv", "4", "1", "1");

        Assert.False(result.Sent);
        Assert.Equal(1, svc.GetTeamsHealth().ConsecutiveFailures);
        Assert.Equal(AlertChannelOutcome.Failed, result.Outcome);
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
        public int AnalysisPageCap => 10;
        public string TriageBaseUrl => "";
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
        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) => Task.FromResult<DateTime?>(null);
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

        var result = await svc.TrySendWebhookAlertsAsync(
            "Deadlocks Detected", "Srv", "4", "1", "1", ContextWith("Y"));

        Assert.False(result.Sent);                                 // dead URL -> attempted, failed
        Assert.Equal(1, svc.GetTeamsHealth().ConsecutiveFailures); // ATTEMPTED, not suppressed
        Assert.Equal(AlertChannelOutcome.Failed, result.Outcome);
    }

    [Fact]
    public async Task SameFingerprint_SuppressedByItsOwnSeededCooldown()
    {
        // The same incident X, seeded "just now" -> suppressed (no network touch).
        var history = new FakeHistoryStore { LastWebhookSent = DateTime.UtcNow, SeededDedupKey = "X" };
        var svc = MakeService(history, EnabledTeamsSettings());

        var result = await svc.TrySendWebhookAlertsAsync(
            "Deadlocks Detected", "Srv", "4", "1", "1", ContextWith("X"));

        Assert.False(result.Sent);                                 // suppressed
        Assert.Equal(0, svc.GetTeamsHealth().ConsecutiveFailures); // NOT attempted
        Assert.Equal(AlertChannelOutcome.Throttled, result.Outcome);
    }

    /// <summary>
    /// #3456: a first notice on an UNPARSEABLE server key (the self-alert family's
    /// <c>cost:&lt;id&gt;:&lt;collector&gt;</c> shape) must never be throttled by another key's send. The
    /// store double here models the real stores' integer <c>server_id</c> column through the shared
    /// <c>AlertHistoryServerIdentity</c> mapping: the other key's fresh row sits in the collapsed 0 bucket,
    /// which is exactly where the old parse-to-0 seed found it and answered this key's question with it —
    /// throttling an unannounced incident, the class #1154 exists to prevent. The seed now declines, the
    /// key reads as a first notice, and the post is attempted (failing against the dead URL). The real
    /// DuckDB store's own refusal is pinned in <c>StoreRoundTripTests</c>; this is the service-level half,
    /// where "declined seed" has to come out as "attempted post".
    /// </summary>
    [Fact]
    public async Task FirstNotice_OnAnUnparseableKey_IsNotThrottledByAnotherKeysSend()
    {
        var history = new CollapsedColumnHistoryStore();
        history.AddWebhookRow("cost:99:wait_stats", DateTime.UtcNow); // another server's send, just now
        var svc = MakeService(history, EnabledTeamsSettings());

        var result = await svc.TrySendWebhookAlertsAsync(
            "Collector Cost Regression", "Srv", "120 ms/run", "60 ms/run", "cost:7:wait_stats");

        Assert.False(result.Sent);                                 // dead URL -> post failed
        Assert.Equal(1, history.SeedReads);                        // the seed WAS consulted
        Assert.Equal(1, svc.GetTeamsHealth().ConsecutiveFailures); // but the post was ATTEMPTED
        Assert.Equal(AlertChannelOutcome.Failed, result.Outcome);
        Assert.NotEqual(AlertChannelOutcome.Throttled, result.Outcome);
    }

    /// <summary>
    /// The control for the pin above, and the #1145 guarantee restated against the same column-faithful
    /// double: an INTEGER key still seeds from its own row — the restart suppression the seed exists for —
    /// and a different integer key is untouched by it. One fixture, three keys, and the only difference
    /// between Throttled and Failed is whose row the store holds.
    /// </summary>
    [Fact]
    public async Task IntegerKeys_StillSeedPerServer_AndOnlyFromTheirOwnRows()
    {
        var history = new CollapsedColumnHistoryStore();
        history.AddWebhookRow("7", DateTime.UtcNow);
        var svc = MakeService(history, EnabledTeamsSettings());

        var suppressed = await svc.TrySendWebhookAlertsAsync("Deadlocks Detected", "Srv", "4", "1", "7");
        Assert.Equal(AlertChannelOutcome.Throttled, suppressed.Outcome);
        Assert.Equal(0, svc.GetTeamsHealth().ConsecutiveFailures);

        var attempted = await svc.TrySendWebhookAlertsAsync("Deadlocks Detected", "Srv2", "4", "1", "8");
        Assert.Equal(AlertChannelOutcome.Failed, attempted.Outcome);
        Assert.Equal(1, svc.GetTeamsHealth().ConsecutiveFailures);
    }

    /// <summary>
    /// Rows keyed by the integer <c>server_id</c> column the real stores persist, resolved through the
    /// shared <c>AlertHistoryServerIdentity</c> mapping on both sides — NOT by the caller's string, which
    /// is the distinction #3456 turns on: a string-keyed fake keeps every key's history separate and the
    /// defect cannot reproduce against it. A regression of <c>SeedScope</c> to the parse-to-0 fallback
    /// makes this store answer <c>cost:7:…</c>'s question with <c>cost:99:…</c>'s row and the first-notice
    /// pin above goes red.
    /// </summary>
    private sealed class CollapsedColumnHistoryStore : IAlertHistoryStore
    {
        private readonly System.Collections.Generic.List<(int ServerId, DateTime AtUtc)> _webhookRows = new();

        public int SeedReads { get; private set; }

        public void AddWebhookRow(string serverKey, DateTime atUtc) =>
            _webhookRows.Add((AlertHistoryServerIdentity.StorageId(serverKey), atUtc));

        public Task RecordAlertAsync(AlertHistoryRecord record) => Task.CompletedTask;

        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null)
        {
            SeedReads++;
            var sid = AlertHistoryServerIdentity.SeedScope(serverId);
            if (sid is null)
                return Task.FromResult<DateTime?>(null);

            DateTime? max = null;
            foreach (var row in _webhookRows)
            {
                if (row.ServerId == sid.Value && (max is null || row.AtUtc > max.Value))
                    max = row.AtUtc;
            }

            return Task.FromResult(max);
        }

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) => Task.FromResult<DateTime?>(null);
    }
}
