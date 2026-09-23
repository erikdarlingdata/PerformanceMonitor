/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3916: the analysis #2054 hold (a steady-severity story notifies once and is then held while it keeps
/// firing) must be EARNED by a delivery. The restart seed read the latest alert-log row regardless of
/// delivery — a month-old undelivered row held one production story silent for 35 days — and the live
/// stamp armed the hold whether or not the send reached anyone. Now the seed reads only delivered pages
/// (<see cref="IAlertHistoryStore.GetLastDeliveredPageUtcAsync"/>), every send still stamps the cooldown
/// (so a no-channel store re-attempts once per cooldown, not every cycle), and only a delivered send arms
/// the hold. The Tier-0 seed (<see cref="IAlertHistoryStore.GetLastAlertTimeAsync"/>) is deliberately
/// unchanged.
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so
   cross-test row churn (inserts/purges/deletes) cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class HoldEarnedByDeliveryTests
{
    private const int TestServerId = -391600;
    private static readonly string TestServerKey = TestServerId.ToString(CultureInfo.InvariantCulture);
    private const string TestServerName = "hold-earned-by-delivery";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /* ---------------- the live PG store ---------------- */

    private static AlertDelivery Undelivered() =>
        AlertDelivery.FromFanout(
            new EmailFanoutResult(AlertChannelOutcome.NotAttempted, null, AlertChannelOutcome.NotAttempted, null, AnyChannelConfigured: false),
            muted: false, trayChannelPresent: false);

    private static AlertDelivery WebhookDelivered() =>
        AlertDelivery.FromFanout(
            new EmailFanoutResult(AlertChannelOutcome.NotAttempted, null, AlertChannelOutcome.Delivered, null, AnyChannelConfigured: true),
            muted: false, trayChannelPresent: false);

    private static AlertDelivery Muted() =>
        AlertDelivery.FromFanout(
            new EmailFanoutResult(AlertChannelOutcome.NotAttempted, null, AlertChannelOutcome.NotAttempted, null, AnyChannelConfigured: true),
            muted: true, trayChannelPresent: false);

    private static Task RecordAsync(PgAlertHistoryStore store, string metric, AlertDelivery delivery, bool muted = false, string? contextJson = null) =>
        store.RecordAlertAsync(new AlertHistoryRecord(
            TestServerKey, TestServerName, metric, "1.5", "1.5", 1.5, 1.5,
            Delivery: delivery, Muted: muted, DetailText: null, ContextJson: contextJson));

    /// <summary>Ages every test row by <paramref name="days"/> — a row "older than the cooldown".</summary>
    private static async Task AgeRowsAsync(NpgsqlConnection connection, int days)
    {
        using var age = new NpgsqlCommand(
            $"UPDATE config_alert_log SET alert_time = alert_time - interval '{days} days' WHERE server_id = {TestServerId};", connection);
        await age.ExecuteNonQueryAsync(Ct);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM config_alert_log WHERE server_id = {TestServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task RunLiveAsync(Func<PgAlertHistoryStore, NpgsqlConnection, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live alert-history test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(Ct);
        await PgMigrations.MigrateAsync(connection, Ct);
        await DeleteTestRowsAsync(connection, Ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            await body(new PgAlertHistoryStore(postgres), connection);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// Pin 1, with its negative control: the production shape — an undelivered row older than the cooldown
    /// — seeds NOTHING on the delivered-page read, while the pre-#3916 read (still the Tier-0 seed) answers
    /// on the very same rows. The control is what makes this a pin of the change and not of an empty table.
    /// </summary>
    [Fact]
    public async Task AnUndeliveredRow_OlderThanTheCooldown_DoesNotSeedTheHold_ButDidOnTheOldRead()
    {
        await RunLiveAsync(async (store, connection) =>
        {
            const string metric = "Analysis: cpu_pressure [3916aaaa]";
            await RecordAsync(store, metric, Undelivered());
            await AgeRowsAsync(connection, days: 35);

            /* Negative control: dev's analysis seed read (GetLastAlertTimeAsync) seeds the hold from it. */
            var oldRead = await store.GetLastAlertTimeAsync(TestServerKey, metric);
            Assert.NotNull(oldRead);
            Assert.True(DateTime.UtcNow - oldRead!.Value > TimeSpan.FromDays(34));

            /* The fix: the delivered-page read sees nothing, so the story is heard on its next firing. */
            Assert.Null(await store.GetLastDeliveredPageUtcAsync(TestServerKey, metric));
        });
    }

    /// <summary>Pin 2: a delivered row seeds; a newer undelivered row does not move the seed.</summary>
    [Fact]
    public async Task ADeliveredRow_SeedsTheHold()
    {
        await RunLiveAsync(async (store, connection) =>
        {
            const string metric = "Analysis: cpu_pressure [3916bbbb]";
            await RecordAsync(store, metric, WebhookDelivered());
            var seed = await store.GetLastDeliveredPageUtcAsync(TestServerKey, metric);
            Assert.NotNull(seed);

            await Task.Delay(TimeSpan.FromMilliseconds(50), Ct);
            await RecordAsync(store, metric, Undelivered());
            Assert.Equal(seed, await store.GetLastDeliveredPageUtcAsync(TestServerKey, metric));
        });
    }

    /// <summary>Pin 3: the digest stays excluded — live, and as the one composed symbol.</summary>
    [Fact]
    public async Task TheDigest_StaysExcluded_FromTheDeliveredPageRead()
    {
        Assert.Equal("\nAND   alert_sent\nAND   notification_type <> 'digest'", PgAlertHistoryStore.DeliveredPageFilter);
        Assert.EndsWith(PgAlertHistoryStore.DigestExclusionFilter, PgAlertHistoryStore.DeliveredPageFilter, StringComparison.Ordinal);

        await RunLiveAsync(async (store, _) =>
        {
            const string metric = "Analysis: cpu_pressure [3916cccc]";
            await RecordAsync(store, metric, AlertDelivery.RoutedToDigest());
            Assert.Null(await store.GetLastDeliveredPageUtcAsync(TestServerKey, metric));
        });
    }

    /// <summary>
    /// Pin 6: the Tier-0 seed is unchanged. A muted, undelivered row still seeds
    /// <see cref="PgAlertHistoryStore.GetLastAlertTimeAsync"/> with a dedupKey — those cooldowns stamp
    /// unconditionally (a muted alert still consumes its cooldown), so "any result" is correct there. Guards
    /// a future "simplification" that folds alert_sent into the shared read.
    /// </summary>
    [Fact]
    public async Task TheTier0Seed_StillSeesAMutedUndeliveredRow_WithItsDedupKey()
    {
        await RunLiveAsync(async (store, _) =>
        {
            const string metric = "Wraparound Risk";
            var context = AlertContextSerializer.Serialize(new AlertContext
            {
                Incidents = new List<AlertIncident> { new("orders_db", new[] { "orders_db" }) },
            });
            await RecordAsync(store, metric, Muted(), muted: true, contextJson: context);

            Assert.NotNull(await store.GetLastAlertTimeAsync(TestServerKey, metric, dedupKey: "orders_db"));
            Assert.Null(await store.GetLastDeliveredPageUtcAsync(TestServerKey, metric));
        });
    }

    /* ---------------- the shared service ---------------- */

    private sealed class ScriptedSender : IFindingAlertSender
    {
        public List<FindingAlert> Sent { get; } = new();
        public DateTime? DeliveredSeed { get; set; }
        public AlertDelivery? Delivery { get; set; }

        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) =>
            Task.FromResult(DeliveredSeed);

        public Task<AlertDelivery?> SendFindingAlertAsync(FindingAlert alert)
        {
            Sent.Add(alert);
            return Task.FromResult(Delivery);
        }

        /* #3916 PR B: over the cap the flush sends ONE summary; these pins stay under it. */
        public Task<AlertDelivery?> SendFindingSummaryAsync(IReadOnlyList<FindingAlert> named)
        {
            Sent.AddRange(named);
            return Task.FromResult(Delivery);
        }
    }

    /// <summary>Cooldown 30 minutes — the floor — so "past the cooldown" is reachable by a seed.</summary>
    private sealed class Settings : IAlertSettings
    {
        public bool SmtpEnabled => false;
        public string SmtpServer => "";
        public int SmtpPort => 25;
        public bool SmtpUseSsl => false;
        public string SmtpUsername => "";
        public string SmtpPassword => "";
        public string? GetSmtpPassword() => null;
        public int EmailCooldownMinutes => 15;
        public string SmtpFromAddress => "";
        public string SmtpRecipients => "";
        public string SmtpProxyAddress => "";
        public bool TeamsWebhookEnabled => false;
        public string TeamsWebhookUrl => "";
        public string TeamsProxyAddress => "";
        public bool SlackWebhookEnabled => false;
        public string SlackWebhookUrl => "";
        public string SlackProxyAddress => "";
        public bool GenericWebhookEnabled => false;
        public string GenericWebhookUrl => "";
        public string GenericWebhookHeadersJson => "";
        public string GenericWebhookBodyTemplate => "";
        public string GenericWebhookProxyAddress => "";
        public bool PagerDutyEnabled => false;
        public string PagerDutyRoutingKey => "";
        public bool PagerDutyUseEuRegion => false;
        public string PagerDutyProxyAddress => "";
        public double AnalysisNotifySeverity => 1.5;
        public int AnalysisNotifyCooldownMinutes => 30;
        public string TriageBaseUrl => "";
        public FindingRoute UncorroboratedFindingRoute => FindingRoute.Page;
        public int AnalysisPageCap => 5;
    }

    private static AnalysisFinding Steady(string hash) => new()
    {
        ServerId = 1,
        ServerName = "PROD01",
        Category = "cpu_pressure",
        StoryPath = "CPU_SPIKE → PLAN_REGRESSION",
        StoryPathHash = hash,
        IncidentId = "inc-" + hash,
        Severity = 2.0,
        Confidence = 0.67,
        FactCount = 3,
        MatchedAmplifiers = 2,
        DefinedAmplifiers = 5,
        RootFactKey = "CPU_SPIKE",
        RootFactValue = 92.5,
        TimeRangeStart = new DateTime(2026, 9, 11, 1, 0, 0, DateTimeKind.Utc),
        TimeRangeEnd = new DateTime(2026, 9, 11, 2, 0, 0, DateTimeKind.Utc)
    };

    private static AnalysisNotificationService Service(ScriptedSender sender, Action<string, string>? sink = null) =>
        new(sender, new Settings(), f => f.ServerId.ToString(), NullLogger<AnalysisNotificationService>.Instance,
            showTrayNotification: sink);

    /// <summary>
    /// Moves every live bucket's LastNotified back by <paramref name="by"/> (LastSeen stays now, so the
    /// prune does not forget it). The service reads DateTime.UtcNow and has no clock seam; this is the
    /// test-only way to place a LIVE-stamped bucket past the cooldown without a production change.
    /// </summary>
    private static void AgeBuckets(AnalysisNotificationService service, TimeSpan by)
    {
        var field = typeof(AnalysisNotificationService).GetField("_cooldowns",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        var dict = (System.Collections.IDictionary)field.GetValue(service)!;
        foreach (var key in new List<object>(System.Linq.Enumerable.Cast<object>(dict.Keys)))
        {
            var state = dict[key]!;
            var t = state.GetType();
            var last = (DateTime)t.GetProperty("LastNotified")!.GetValue(state)!;
            var severity = (double)t.GetProperty("LastNotifiedSeverity")!.GetValue(state)!;
            var delivered = (bool)t.GetProperty("Delivered")!.GetValue(state)!;
            dict[key] = Activator.CreateInstance(t, last - by, severity, DateTime.UtcNow, delivered);
        }
    }

    /// <summary>
    /// Pin 4: an undelivered send does NOT arm the hold. Inside the cooldown it is not re-attempted (every
    /// send still stamps the cooldown, so a no-channel store writes one row per cooldown, not one per
    /// cycle); past the cooldown the steady story IS re-attempted, because the hold was never earned.
    /// A null result (the sender caught) counts as undelivered.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnUndeliveredSend_DoesNotArmTheHold_ReAttemptsAfterTheCooldown_NotInsideIt(bool caught)
    {
        var sender = new ScriptedSender { Delivery = caught ? null : Undelivered() };
        var service = Service(sender);
        var hash = caught ? "undeliv000000002" : "undeliv000000001";

        await service.NotifyAsync(new[] { Steady(hash) });
        await service.FlushPendingAsync();
        await service.NotifyAsync(new[] { Steady(hash) });
        await service.FlushPendingAsync();
        Assert.Single(sender.Sent);                    /* inside the cooldown: throttled */

        AgeBuckets(service, TimeSpan.FromMinutes(31));
        await service.NotifyAsync(new[] { Steady(hash) });
        await service.FlushPendingAsync();
        Assert.Equal(2, sender.Sent.Count);            /* past it, steady severity: re-attempted */
    }

    /// <summary>
    /// Pin 5 (live): a delivered send arms the hold — the same steady story, past the same cooldown, stays
    /// held. The discriminating twin of pin 4.
    /// </summary>
    [Fact]
    public async Task ADeliveredSend_ArmsTheHold_SteadySeverityHeldPastTheCooldown()
    {
        var sender = new ScriptedSender { Delivery = WebhookDelivered() };
        var service = Service(sender);

        await service.NotifyAsync(new[] { Steady("delivered0000002") });
        await service.FlushPendingAsync();
        AgeBuckets(service, TimeSpan.FromMinutes(31));
        await service.NotifyAsync(new[] { Steady("delivered0000002") });
        await service.FlushPendingAsync();

        Assert.Single(sender.Sent);
    }

    /// <summary>
    /// Pin 2 (service half): a delivered-page seed arms the hold — seeded from a delivered row far past the cooldown, a story
    /// at steady severity stays held. (The seed assumes threshold severity, so this story is steady at
    /// threshold.)
    /// </summary>
    [Fact]
    public async Task ADeliveredPage_ArmsTheHold_PastTheCooldown()
    {
        var sender = new ScriptedSender { DeliveredSeed = DateTime.UtcNow.AddDays(-35), Delivery = WebhookDelivered() };
        var service = Service(sender);

        var steady = Steady("delivered0000001");
        steady.Severity = 1.5;
        await service.NotifyAsync(new[] { steady });
        await service.FlushPendingAsync();

        Assert.Empty(sender.Sent);
    }

    /// <summary>
    /// Pin 7: on Dashboard a wired tray sink IS a delivery (the balloon is raised on the same call), so an
    /// otherwise-undelivered send arms the hold there — Dashboard's behaviour is unchanged. Without a sink
    /// the same undelivered result leaves the hold unarmed (pin 4).
    /// </summary>
    [Fact]
    public async Task AWiredTraySink_CountsAsDelivered()
    {
        var toasts = 0;
        var sender = new ScriptedSender { Delivery = Undelivered() };
        var service = Service(sender, sink: (_, _) => toasts++);

        await service.NotifyAsync(new[] { Steady("dashsink00000001") });
        await service.FlushPendingAsync();
        AgeBuckets(service, TimeSpan.FromMinutes(31));
        await service.NotifyAsync(new[] { Steady("dashsink00000001") });
        await service.FlushPendingAsync();

        Assert.Single(sender.Sent);   /* steady story held past the cooldown, as before #3916 */
        Assert.Equal(1, toasts);
    }
}
