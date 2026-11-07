/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pins how a webhook post meets an endpoint that paces the sender (#8244). An HTTP 429 is the endpoint saying "not
/// yet", not "no": the post waits what <c>Retry-After</c> asks for, bounded, and goes again, and only the retry's answer
/// counts toward the destination's failures in a row, so a burst that clears on the retry raises no "Notification
/// Channel Failing". Per-event posts to one destination are spaced a second apart, so a deadlock storm split into one
/// message per incident does not arrive as the burst the endpoint would 429. Every wait is read off a fake clock and a
/// recorded delay, and every post is answered by a scripted handler, so nothing here waits on the wall clock or the network.
/// </summary>
public sealed class WebhookRateLimitTests
{
    private const string SlackUrl = "https://hooks.example.invalid/services/T000/B000/XXXX";

    /// <summary>Slack-only settings. The URL is never connected to: the scripted handler answers every post.</summary>
    private sealed class SlackSettings : IAlertSettings
    {
        public bool SlackWebhookEnabled => true;
        public string SlackWebhookUrl => SlackUrl;
        public IReadOnlyList<NotificationRoute> NotificationRoutes => Array.Empty<NotificationRoute>();

        public bool SmtpEnabled => false;
        public string SmtpServer => "";
        public int SmtpPort => 25;
        public bool SmtpUseSsl => false;
        public string SmtpUsername => "";
        public string SmtpFromAddress => "";
        public string SmtpRecipients => "";
        public string? GetSmtpPassword() => null;
        public int EmailCooldownMinutes => 15;
        public bool TeamsWebhookEnabled => false;
        public string TeamsWebhookUrl => "";
        public string TeamsProxyAddress => "";
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
        public double AnalysisNotifySeverity => 0.5;
        public int AnalysisNotifyCooldownMinutes => 360;
        public int AnalysisPageCap => 10;
        public string TriageBaseUrl => "";
    }

    /// <summary>Answers each post from a script, the last answer repeating, and stamps each request with the fake clock.</summary>
    private sealed class ScriptedHandler : HttpMessageHandler
    {
        private readonly Queue<Func<HttpResponseMessage>> _answers;
        private readonly Func<HttpResponseMessage> _last;
        private readonly Func<DateTime> _clock;

        public ScriptedHandler(Func<DateTime> clock, params Func<HttpResponseMessage>[] answers)
        {
            _clock = clock;
            _answers = new Queue<Func<HttpResponseMessage>>(answers);
            _last = answers[^1];
        }

        public List<DateTime> RequestsAt { get; } = new();

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            RequestsAt.Add(_clock());
            var answer = _answers.Count > 0 ? _answers.Dequeue() : _last;
            return Task.FromResult(answer());
        }
    }

    private static Func<HttpResponseMessage> Ok() => () => new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("ok") };

    private static Func<HttpResponseMessage> TooManyRequests(RetryConditionHeaderValue? retryAfter = null) => () =>
    {
        var response = new HttpResponseMessage(HttpStatusCode.TooManyRequests) { Content = new StringContent("slow down") };
        response.Headers.RetryAfter = retryAfter;
        return response;
    };

    private static RetryConditionHeaderValue After(int seconds) => new(TimeSpan.FromSeconds(seconds));

    /// <summary>A service on a fake clock, a recorded delay and a scripted handler. The delay advances the clock by what
    /// was asked, so a wait is observable both as the delay requested and as the stamp on the next request.</summary>
    private sealed class Rig
    {
        public Rig(params Func<HttpResponseMessage>[] answers)
        {
            Handler = new ScriptedHandler(() => Now, answers);
            Webhooks = new WebhookAlertService(new SlackSettings(), EmailAlertService.Branding, new AppLoggerAdapter<WebhookAlertService>());
            Seams.Attach(Webhooks, Handler, () => Now, (wait, _) =>
            {
                Waits.Add(wait);
                Now += wait;
                return Task.CompletedTask;
            });
        }

        public DateTime Start => StartUtc;
        public DateTime Now { get; set; } = StartUtc;
        public List<TimeSpan> Waits { get; } = new();
        public ScriptedHandler Handler { get; }
        public WebhookAlertService Webhooks { get; }

        public int SlackFailures => Webhooks.GetChannelFailureCounts().Single(c => c.Channel == NotificationRouter.SlackChannel).ConsecutiveFailures;

        /// <summary>One firing from a distinct server, so no cooldown stands between two posts.</summary>
        public Task<WebhookFanoutResult> FireAsync(AlertNotificationMode? mode = null, string metric = "High CPU")
        {
            var server = "SQL" + Interlocked.Increment(ref s_serverSequence);
            return Webhooks.TrySendWebhookAlertsAsync(metric, server, "97%", "90%", serverId: server, deliveryMode: mode);
        }
    }

    private static int s_serverSequence;

    private static readonly DateTime StartUtc = new(2026, 11, 7, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>The service's pacing seams: the handler every post goes through, the clock the per-destination slots are
    /// read against, and the wait a post performs. Reached by name so this file compiles against a service without
    /// them; a service without them cannot be driven by a fake handler or clock, and says so.</summary>
    private static class Seams
    {
        public static void Attach(
            WebhookAlertService webhooks, HttpMessageHandler handler, Func<DateTime> clock, Func<TimeSpan, CancellationToken, Task> delay)
        {
            Set(webhooks, "HttpHandlerOverride", handler, "a fake handler cannot answer its posts");
            Set(webhooks, "PacingClock", clock, "its waits cannot be read off a fake clock");
            Set(webhooks, "Delay", delay, "its waits run on the wall clock");
        }

        private static void Set(WebhookAlertService webhooks, string name, object value, string consequence)
        {
            var property = typeof(WebhookAlertService).GetProperty(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            if (property is null || !property.CanWrite)
            {
                Assert.Fail($"WebhookAlertService has no settable {name}, so {consequence}: a 429 cannot be tested without waiting on it.");
            }

            property.SetValue(webhooks, value);
        }
    }

    [Fact]
    public async Task A429WithRetryAfter_IsPostedAgainAfterTheWait_AndCountsNoFailure()
    {
        var rig = new Rig(TooManyRequests(After(2)), Ok());

        var result = await rig.FireAsync();

        Assert.True(result.Sent);
        Assert.Equal(2, rig.Handler.RequestsAt.Count);
        Assert.Equal(new[] { TimeSpan.FromSeconds(2) }, rig.Waits);
        Assert.Equal(rig.Start, rig.Handler.RequestsAt[0]);
        Assert.Equal(rig.Start.AddSeconds(2), rig.Handler.RequestsAt[1]);
        Assert.Equal(0, rig.SlackFailures);
        Assert.Equal(WebhookChannelNotice.None, WebhookChannelFailurePolicy.Decide(false, rig.SlackFailures, configured: true));
    }

    [Fact]
    public async Task A429OnTheRetryToo_IsTheDestinationsFailure_CountedOncePerFiring()
    {
        var rig = new Rig(TooManyRequests(After(1)));

        var result = await rig.FireAsync();

        Assert.Equal(AlertChannelOutcome.Failed, result.Outcome);
        Assert.Contains("HTTP 429 Too Many Requests", result.SendError, StringComparison.Ordinal);
        Assert.Equal(2, rig.Handler.RequestsAt.Count);
        Assert.Equal(1, rig.SlackFailures);

        await rig.FireAsync();
        await rig.FireAsync();
        Assert.Equal(3, rig.SlackFailures);
        Assert.Equal(WebhookChannelNotice.Failing, WebhookChannelFailurePolicy.Decide(false, rig.SlackFailures, configured: true));
    }

    [Fact]
    public async Task ARetryAfterBeyondTheBound_WaitsTheBound()
    {
        var rig = new Rig(TooManyRequests(After(600)), Ok());

        var result = await rig.FireAsync();

        Assert.True(result.Sent);
        Assert.Equal(new[] { TimeSpan.FromSeconds(60) }, rig.Waits);
    }

    [Fact]
    public async Task ARetryAfterDate_IsReadAgainstTheClock_AndNoHeaderWaitsASecond()
    {
        var dated = new Rig(TooManyRequests(new RetryConditionHeaderValue(new DateTimeOffset(StartUtc.AddSeconds(5)))), Ok());
        Assert.True((await dated.FireAsync()).Sent);
        Assert.Equal(new[] { TimeSpan.FromSeconds(5) }, dated.Waits);

        var bare = new Rig(TooManyRequests(), Ok());
        Assert.True((await bare.FireAsync()).Sent);
        Assert.Equal(new[] { TimeSpan.FromSeconds(1) }, bare.Waits);
    }

    [Fact]
    public async Task PerEventPostsToOneDestination_AreSpacedASecondApart()
    {
        var rig = new Rig(Ok());

        for (var i = 0; i < 3; i++)
        {
            Assert.True((await rig.FireAsync(AlertNotificationMode.PerEvent)).Sent);
        }

        Assert.Equal(new[] { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1) }, rig.Waits);
        Assert.Equal(
            new[] { rig.Start, rig.Start.AddSeconds(1), rig.Start.AddSeconds(2) },
            rig.Handler.RequestsAt);
        Assert.Equal(0, rig.SlackFailures);
    }

    [Fact]
    public async Task SummaryCards_TakeNoSpacing()
    {
        var rig = new Rig(Ok());

        Assert.True((await rig.FireAsync(metric: "High CPU")).Sent);
        Assert.True((await rig.FireAsync(metric: "Low Disk Space")).Sent);

        Assert.Empty(rig.Waits);
        Assert.Equal(new[] { rig.Start, rig.Start }, rig.Handler.RequestsAt);
    }
}
