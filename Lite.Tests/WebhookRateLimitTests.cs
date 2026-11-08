/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pins how a webhook post meets an endpoint that paces the sender (#8244). An HTTP 429 is the endpoint saying "not
/// yet", not "no": a post waits what <c>Retry-After</c> asks for when that is at or under the bound and goes again, and
/// only the retry's answer counts toward the destination's failures in a row, so a burst that clears on the retry raises
/// no "Notification Channel Failing". A <c>Retry-After</c> over the bound is not waited out: the post fails as any other
/// failed post does, with the endpoint's body in the log, and the destination is held so posts that follow inside the hold
/// do not hammer it. The hold is the destination's own: it never holds another destination, and a post with no spacing
/// (a Summary card, a notice) waits for the hold alone, never behind the Per-event slots other posts have reserved. The
/// channels of one firing post concurrently, so a held channel does not delay the others. Every wait is read off a fake
/// clock and a recorded delay, and every post is answered by a scripted handler, so nothing here waits on the wall clock.
/// </summary>
public sealed class WebhookRateLimitTests
{
    private const string SlackUrl = "https://hooks.example.invalid/services/T000/B000/XXXX";
    private const string TeamsUrl = "https://teams.example.invalid/webhookb2/XXXX";

    /* Only ever the limit on something that never happens: no assertion reads how long a step took. */
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(30);

    /// <summary>Slack, and Teams when asked for. The URLs are never connected to: the scripted handler answers every post.</summary>
    private sealed class Settings : IAlertSettings
    {
        public bool SlackWebhookEnabled => true;
        public string SlackWebhookUrl => SlackUrl;
        public bool TeamsWebhookEnabled { get; init; }
        public string TeamsWebhookUrl => TeamsUrl;
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

    /// <summary>Answers each post from the script for its URL, the last answer repeating, and stamps each request with the fake clock.</summary>
    private sealed class ScriptedHandler : HttpMessageHandler
    {
        private readonly Dictionary<string, Queue<Func<HttpResponseMessage>>> _scripts = new(StringComparer.Ordinal);
        private readonly Dictionary<string, Func<HttpResponseMessage>> _last = new(StringComparer.Ordinal);
        private readonly Func<DateTime> _clock;

        public ScriptedHandler(Func<DateTime> clock)
        {
            _clock = clock;
        }

        public void Script(string url, params Func<HttpResponseMessage>[] answers)
        {
            _scripts[url] = new Queue<Func<HttpResponseMessage>>(answers);
            _last[url] = answers[^1];
        }

        public List<(string Url, DateTime At)> Requests { get; } = new();

        public IReadOnlyList<DateTime> RequestsAt(string url) => Requests.Where(r => r.Url == url).Select(r => r.At).ToList();

        /// <summary>Completes when the first post to <paramref name="url"/> arrives.</summary>
        public TaskCompletionSource Posted(string url) => _posted.TryGetValue(url, out var tcs) ? tcs : _posted[url] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly Dictionary<string, TaskCompletionSource> _posted = new(StringComparer.Ordinal);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var url = request.RequestUri!.ToString();
            lock (Requests)
            {
                Requests.Add((url, _clock()));
            }

            if (_posted.TryGetValue(url, out var tcs))
            {
                tcs.TrySetResult();
            }

            var answer = _scripts.TryGetValue(url, out var script) && script.Count > 0 ? script.Dequeue() : _last.GetValueOrDefault(url) ?? Ok();
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

    /// <summary>Keeps every line the service logs, so a test can read what a failed post said about the endpoint's answer.</summary>
    private sealed class CapturingLogger : ILogger<WebhookAlertService>
    {
        public List<string> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (Lines)
            {
                Lines.Add(formatter(state, exception));
            }
        }
    }

    /// <summary>A service on a fake clock, a recorded delay and a scripted handler. The delay advances the clock by what
    /// was asked, so a wait is observable both as the delay requested and as the stamp on the next request.</summary>
    private sealed class Bench
    {
        public Bench(bool teams = false)
        {
            Handler = new ScriptedHandler(() => Now);
            Webhooks = new WebhookAlertService(new Settings { TeamsWebhookEnabled = teams }, EmailAlertService.Branding, Log);
            Webhooks.HttpHandlerOverride = Handler;
            Webhooks.PacingClock = () => Now;
            Webhooks.Delay = (wait, _) =>
            {
                Waits.Add(wait);
                Now += wait;
                return Task.CompletedTask;
            };
        }

        public Bench Slack(params Func<HttpResponseMessage>[] answers)
        {
            Handler.Script(SlackUrl, answers);
            return this;
        }

        public Bench Teams(params Func<HttpResponseMessage>[] answers)
        {
            Handler.Script(TeamsUrl, answers);
            return this;
        }

        public DateTime Start => StartUtc;
        public DateTime Now { get; set; } = StartUtc;
        public List<TimeSpan> Waits { get; } = new();
        public ScriptedHandler Handler { get; }
        public WebhookAlertService Webhooks { get; }
        public CapturingLogger Log { get; } = new();

        public IReadOnlyList<DateTime> SlackRequestsAt => Handler.RequestsAt(SlackUrl);
        public IReadOnlyList<DateTime> TeamsRequestsAt => Handler.RequestsAt(TeamsUrl);

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

    private static void AssertBodyLogged(Bench bench)
    {
        Assert.Contains(bench.Log.Lines, line =>
            line.Contains("HTTP 429 Too Many Requests", StringComparison.Ordinal) && line.Contains("slow down", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A429WithRetryAfter_IsPostedAgainAfterTheWait_AndCountsNoFailure()
    {
        var bench = new Bench().Slack(TooManyRequests(After(2)), Ok());

        var result = await bench.FireAsync();

        Assert.True(result.Sent);
        Assert.Equal(2, bench.SlackRequestsAt.Count);
        Assert.Equal(new[] { TimeSpan.FromSeconds(2) }, bench.Waits);
        Assert.Equal(bench.Start, bench.SlackRequestsAt[0]);
        Assert.Equal(bench.Start.AddSeconds(2), bench.SlackRequestsAt[1]);
        Assert.Equal(0, bench.SlackFailures);
        Assert.Equal(WebhookChannelNotice.None, WebhookChannelFailurePolicy.Decide(false, bench.SlackFailures, configured: true));
    }

    [Fact]
    public async Task A429OnTheRetryToo_IsTheDestinationsFailure_CountedOncePerFiring_WithTheBodyLogged()
    {
        var bench = new Bench().Slack(TooManyRequests(After(1)));

        var result = await bench.FireAsync();

        Assert.Equal(AlertChannelOutcome.Failed, result.Outcome);
        Assert.Contains("HTTP 429 Too Many Requests", result.SendError, StringComparison.Ordinal);
        Assert.Equal(2, bench.SlackRequestsAt.Count);
        Assert.Equal(1, bench.SlackFailures);
        AssertBodyLogged(bench);

        await bench.FireAsync();
        await bench.FireAsync();
        Assert.Equal(3, bench.SlackFailures);
        Assert.Equal(WebhookChannelNotice.Failing, WebhookChannelFailurePolicy.Decide(false, bench.SlackFailures, configured: true));
    }

    [Fact]
    public async Task ARetryAfterAtTheBound_IsWaitedOut_AndOneOverIt_FailsWithoutWaiting_AndHoldsTheDestination()
    {
        var atBound = new Bench().Slack(TooManyRequests(After(10)), Ok());
        Assert.True((await atBound.FireAsync()).Sent);
        Assert.Equal(new[] { TimeSpan.FromSeconds(10) }, atBound.Waits);
        Assert.Equal(2, atBound.SlackRequestsAt.Count);

        var over = new Bench().Slack(TooManyRequests(After(11)), Ok());

        var result = await over.FireAsync();

        /* Not waited out: one request, no delay, the failure the endpoint's answer is, with its body in the log. */
        Assert.Equal(AlertChannelOutcome.Failed, result.Outcome);
        Assert.Contains("HTTP 429 Too Many Requests", result.SendError, StringComparison.Ordinal);
        Assert.Empty(over.Waits);
        Assert.Single(over.SlackRequestsAt);
        Assert.Equal(1, over.SlackFailures);
        AssertBodyLogged(over);

        /* Inside the hold, with more of it left than a post waits: the post fails without a request, so the endpoint is not hammered. */
        var held = await over.FireAsync();
        Assert.Equal(AlertChannelOutcome.Failed, held.Outcome);
        Assert.Contains("HTTP 429", held.SendError, StringComparison.Ordinal);
        Assert.Empty(over.Waits);
        Assert.Single(over.SlackRequestsAt);

        /* Once the hold has passed, posts go again. */
        over.Now = over.Start.AddSeconds(11);
        Assert.True((await over.FireAsync()).Sent);
        Assert.Empty(over.Waits);
        Assert.Equal(2, over.SlackRequestsAt.Count);
        Assert.Equal(0, over.SlackFailures);
    }

    [Fact]
    public async Task AHoldOnOneDestination_HoldsNoOther()
    {
        var bench = new Bench(teams: true).Slack(TooManyRequests(After(600))).Teams(Ok());

        var first = await bench.FireAsync();
        Assert.True(first.Sent);
        Assert.Equal(AlertChannelOutcome.Delivered, first.ChannelOutcomes![NotificationRouter.TeamsChannel]);
        Assert.Equal(AlertChannelOutcome.Failed, first.ChannelOutcomes[NotificationRouter.SlackChannel]);
        Assert.Empty(bench.Waits);

        var second = await bench.FireAsync();
        Assert.True(second.Sent);
        Assert.Equal(AlertChannelOutcome.Delivered, second.ChannelOutcomes![NotificationRouter.TeamsChannel]);
        Assert.Equal(AlertChannelOutcome.Failed, second.ChannelOutcomes[NotificationRouter.SlackChannel]);

        Assert.Equal(new[] { bench.Start, bench.Start }, bench.TeamsRequestsAt);
        Assert.Equal(new[] { bench.Start }, bench.SlackRequestsAt);
        Assert.Empty(bench.Waits);
    }

    [Fact]
    public async Task ARetryAfterDate_IsReadAgainstTheClock_AndNoHeaderWaitsASecond()
    {
        var dated = new Bench().Slack(TooManyRequests(new RetryConditionHeaderValue(new DateTimeOffset(StartUtc.AddSeconds(5)))), Ok());
        Assert.True((await dated.FireAsync()).Sent);
        Assert.Equal(new[] { TimeSpan.FromSeconds(5) }, dated.Waits);

        var bare = new Bench().Slack(TooManyRequests(), Ok());
        Assert.True((await bare.FireAsync()).Sent);
        Assert.Equal(new[] { TimeSpan.FromSeconds(1) }, bare.Waits);
    }

    [Fact]
    public async Task PerEventPostsToOneDestination_AreSpacedASecondApart()
    {
        var bench = new Bench().Slack(Ok());

        for (var i = 0; i < 3; i++)
        {
            Assert.True((await bench.FireAsync(AlertNotificationMode.PerEvent)).Sent);
        }

        Assert.Equal(new[] { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1) }, bench.Waits);
        Assert.Equal(
            new[] { bench.Start, bench.Start.AddSeconds(1), bench.Start.AddSeconds(2) },
            bench.SlackRequestsAt);
        Assert.Equal(0, bench.SlackFailures);
    }

    [Fact]
    public async Task APostWithNoSpacing_DoesNotWaitBehindPerEventSlots()
    {
        var bench = new Bench().Slack(Ok());

        for (var i = 0; i < 3; i++)
        {
            Assert.True((await bench.FireAsync(AlertNotificationMode.PerEvent)).Sent);
        }

        /* Three Per-event posts went at 0, 1 and 2 s and the destination's next slot is at 3 s. A Summary card is one post
           with no spacing: it goes now, at 2 s, and reserves nothing. */
        Assert.True((await bench.FireAsync(AlertNotificationMode.Summary)).Sent);

        Assert.Equal(new[] { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1) }, bench.Waits);
        Assert.Equal(bench.Start.AddSeconds(2), bench.SlackRequestsAt[^1]);
        Assert.Equal(4, bench.SlackRequestsAt.Count);
    }

    [Fact]
    public async Task PerEventPostsAfterAHold_ResumeFromTheHoldsEnd_AndAPostWithNoSpacingWaitsForTheHoldAlone()
    {
        var bench = new Bench().Slack(TooManyRequests(After(11)), Ok());

        /* The first post is told to wait 11 s, over the bound: it fails without waiting, and the destination is held until 11 s. */
        Assert.Equal(AlertChannelOutcome.Failed, (await bench.FireAsync(AlertNotificationMode.PerEvent)).Outcome);
        Assert.Empty(bench.Waits);

        /* Still at 0 s, with 11 s of hold left: no request. */
        Assert.Equal(AlertChannelOutcome.Failed, (await bench.FireAsync(AlertNotificationMode.PerEvent)).Outcome);
        Assert.Single(bench.SlackRequestsAt);

        /* At 2 s, 9 s of hold left, which a post waits out: the Per-event post goes at the hold's end and the spacing resumes from there. */
        bench.Now = bench.Start.AddSeconds(2);
        Assert.True((await bench.FireAsync(AlertNotificationMode.PerEvent)).Sent);
        Assert.True((await bench.FireAsync(AlertNotificationMode.PerEvent)).Sent);
        Assert.Equal(new[] { TimeSpan.FromSeconds(9), TimeSpan.FromSeconds(1) }, bench.Waits);
        Assert.Equal(new[] { bench.Start, bench.Start.AddSeconds(11), bench.Start.AddSeconds(12) }, bench.SlackRequestsAt);

        /* A post with no spacing after the hold has passed waits for nothing, the slot at 13 s included. */
        Assert.True((await bench.FireAsync(AlertNotificationMode.Summary)).Sent);
        Assert.Equal(2, bench.Waits.Count);
        Assert.Equal(bench.Start.AddSeconds(12), bench.SlackRequestsAt[^1]);
    }

    [Fact]
    public async Task TheChannelsOfOneFiring_PostConcurrently_SoAHeldChannelDoesNotDelayAnother()
    {
        var bench = new Bench(teams: true).Teams(TooManyRequests(After(5)), Ok()).Slack(Ok());

        /* The delay is held open by the test rather than advancing the clock, so Teams sits in its 5 s wait until released. */
        var teamsWait = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        bench.Webhooks.Delay = (wait, _) =>
        {
            bench.Waits.Add(wait);
            return teamsWait.Task;
        };
        var slackPosted = bench.Handler.Posted(SlackUrl);

        var firing = bench.FireAsync();

        /* Patience only bounds a post that never comes; nothing here is judged on how long it took. */
        await Task.WhenAny(slackPosted.Task, firing, Task.Delay(Patience, TestContext.Current.CancellationToken));
        Assert.True(slackPosted.Task.IsCompleted, "the Slack post did not go while the Teams post was waiting out its Retry-After");
        Assert.False(firing.IsCompleted);
        Assert.Equal(new[] { TimeSpan.FromSeconds(5) }, bench.Waits);

        teamsWait.SetResult();
        var result = await firing;

        Assert.True(result.Sent);
        Assert.Equal(AlertChannelOutcome.Delivered, result.ChannelOutcomes![NotificationRouter.TeamsChannel]);
        Assert.Equal(AlertChannelOutcome.Delivered, result.ChannelOutcomes[NotificationRouter.SlackChannel]);
        Assert.Equal(2, bench.TeamsRequestsAt.Count);
        Assert.Single(bench.SlackRequestsAt);
    }

    [Fact]
    public async Task SummaryCards_TakeNoSpacing()
    {
        var bench = new Bench().Slack(Ok());

        Assert.True((await bench.FireAsync(metric: "High CPU")).Sent);
        Assert.True((await bench.FireAsync(metric: "Low Disk Space")).Sent);

        Assert.Empty(bench.Waits);
        Assert.Equal(new[] { bench.Start, bench.Start }, bench.SlackRequestsAt);
    }

    [Fact]
    public async Task TheTestButton_ShowsA429sBody()
    {
        using var endpoint = new RateLimitingEndpoint();

        var error = await WebhookAlertService.SendTestSlackAsync(endpoint.Url, null, EmailAlertService.Branding);

        Assert.NotNull(error);
        Assert.Contains("HTTP 429 Too Many Requests", error, StringComparison.Ordinal);
        Assert.Contains("slow down", error, StringComparison.Ordinal);
    }

    /// <summary>A loopback endpoint that answers every post with HTTP 429 and a short body, the way a paced webhook does.
    /// A raw socket rather than an HTTP listener, so it binds any free port without a URL reservation.</summary>
    private sealed class RateLimitingEndpoint : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _stop = new();

        public RateLimitingEndpoint()
        {
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            Url = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/hook";
            _ = ServeAsync();
        }

        public string Url { get; }

        private async Task ServeAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    using var stream = client.GetStream();
                    await ReadRequestAsync(stream, _stop.Token);
                    const string body = "slow down";
                    var head = "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Type: text/plain\r\n"
                        + $"Content-Length: {body.Length}\r\nConnection: close\r\n\r\n";
                    var bytes = Encoding.ASCII.GetBytes(head + body);
                    await stream.WriteAsync(bytes, _stop.Token);
                    await stream.FlushAsync(_stop.Token);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (SocketException)
            {
            }
            catch (IOException)
            {
            }
        }

        /// <summary>Reads the request head and as much body as its Content-Length names, so the answer is not written into a half-sent request.</summary>
        private static async Task ReadRequestAsync(NetworkStream stream, CancellationToken ct)
        {
            var buffer = new byte[8192];
            var read = new MemoryStream();
            var headEnd = -1;
            while (headEnd < 0)
            {
                var n = await stream.ReadAsync(buffer, ct);
                if (n == 0)
                {
                    return;
                }

                read.Write(buffer, 0, n);
                headEnd = Encoding.ASCII.GetString(read.ToArray()).IndexOf("\r\n\r\n", StringComparison.Ordinal);
            }

            var head = Encoding.ASCII.GetString(read.ToArray(), 0, headEnd);
            var lengthLine = head.Split("\r\n").FirstOrDefault(l => l.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase));
            var length = lengthLine is null ? 0 : int.Parse(lengthLine["Content-Length:".Length..].Trim(), System.Globalization.CultureInfo.InvariantCulture);
            var have = (int)read.Length - (headEnd + 4);
            while (have < length)
            {
                var n = await stream.ReadAsync(buffer, ct);
                if (n == 0)
                {
                    return;
                }

                have += n;
            }
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();
            _stop.Dispose();
        }
    }
}
