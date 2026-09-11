/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A regression #3302 shipped: an analysis finding's Diagnosis facts reach every channel TWICE.
///
/// <para>#3297 threaded each alert's prose <c>DetailText</c> through to all five delivery channels, gated
/// on <c>AlertDetailText.ProseForDelivery</c> — which suppresses the prose when it is textually equal to
/// the flattening of the alert's own structured context. <c>FindingMessageFormatter</c> is a third
/// producer of <c>(Context, DetailText)</c> pairs and nothing in #3297 touched it: <c>BuildContext</c>
/// puts story / severity / notify threshold / confidence / fact count / database / window into a
/// <c>Diagnosis</c> item, and <c>DetailText</c> formats the SAME values with different labels
/// ("Facts in chain" vs "Facts", one combined severity line vs two fields) and a different window
/// separator. Different text, so the equality gate never fires, so both are delivered — on the largest
/// alert category there is (CPU spikes, plan regressions, index and RCSI findings), on both SKUs.</para>
///
/// <para>The fix suppresses the prose at DELIVERY and leaves the PERSISTED value alone, because
/// <c>FindingMessageFormatter.DetailText</c> is also <c>config_alert_log.detail_text</c> — read by the MCP
/// alert reader, the triage endpoint and the Viewer's detail pane, and parsed by
/// <c>AlertMuteContext.PopulateFromDetailText</c> for the mute pre-fill. Both halves are asserted here:
/// what the channels render, and what the row keeps.</para>
/// </summary>
public class AnalysisProseDeliveryTests
{
    /// <summary>
    /// The counting marker. It appears exactly ONCE per rendering of the Diagnosis facts — as the
    /// <c>Database</c> field of the structured item, and as the <c>Database:</c> line of the prose — and
    /// nowhere else in any payload: not in the metric name (category + hash), not in the current value
    /// (root fact key + value), not in the thresholds, not in the static advice prose, and not in the
    /// email subject. So an occurrence count over a delivered body IS the number of times those facts
    /// arrived, which is the quantity in question.
    /// </summary>
    private const string DatabaseMarker = "LedgerArchive_7731";

    private const double NotifyThreshold = 1.5;

    /// <summary>
    /// A realistic <c>cpu_pressure</c> finding with every Diagnosis field populated and a FIXED window, so
    /// the persisted detail text below can be a frozen literal rather than a re-derivation of the code
    /// under test.
    /// </summary>
    private static AnalysisFinding Finding() => new()
    {
        ServerId = 1,
        ServerName = "TestServer",
        Category = "cpu_pressure",
        StoryPath = "CPU_SPIKE → PLAN_REGRESSION",
        StoryPathHash = "diag000000000001",
        IncidentId = string.Empty,
        Severity = 1.8,
        Confidence = 0.67,
        FactCount = 2,
        RootFactKey = "CPU_SPIKE",
        RootFactValue = 92.5,
        DatabaseName = DatabaseMarker,
        TimeRangeStart = new DateTime(2026, 9, 11, 1, 0, 0, DateTimeKind.Utc),
        TimeRangeEnd = new DateTime(2026, 9, 11, 2, 0, 0, DateTimeKind.Utc)
    };

    /// <summary>
    /// What <c>config_alert_log.detail_text</c> holds for an analysis row — stated as a frozen list of
    /// lines rather than computed from <c>FindingMessageFormatter</c>, so it cannot follow the code it is
    /// pinning. Joined with <c>Environment.NewLine</c> because <c>StringBuilder.AppendLine</c>
    /// emits that, and this suite runs on Windows in CI.
    /// </summary>
    private static string PersistedDetailText => string.Join(Environment.NewLine, new[]
    {
        "  Story: CPU_SPIKE → PLAN_REGRESSION",
        "  Severity: 1.80 (notify threshold 1.5)",
        "  Confidence: 0.67",
        "  Facts in chain: 2",
        "  Database: " + DatabaseMarker,
        "  Window: 2026-09-11 01:00:00Z - 2026-09-11 02:00:00Z"
    });

    /// <summary>
    /// Runs the REAL producer and returns the composed alert it dispatched. Driving
    /// <c>AnalysisNotificationService</c> rather than hand-building a <c>FindingAlert</c> is the point:
    /// the declaration under test is the producer's, and a hand-built record would assert the
    /// arrangement instead of the behaviour.
    /// </summary>
    private static async Task<FindingAlert> ComposedAlertAsync()
    {
        var sender = new CapturingSender();
        var notifier = new AnalysisNotificationService(
            sender,
            new FixedThresholdSettings(),
            f => f.ServerId.ToString(),
            new AppLoggerAdapter<AnalysisNotificationService>());

        await notifier.NotifyAsync(new[] { Finding() });

        return Assert.Single(sender.Sent);
    }

    /* ─────────────── what the channels render ─────────────── */

    /// <summary>
    /// The regression, on every channel that renders the facts at all, measured as an occurrence count.
    ///
    /// <para>Each assertion is paired with the SAME builder fed <c>DetailText</c> — the argument #3297
    /// shipped — which must come back with the facts twice. Without that control the test would pass just
    /// as well if the structured context were dropped instead of the prose, which is the opposite defect
    /// and the more expensive one: the context carries the advice, the remediation T-SQL and the
    /// drill-down that the prose does not.</para>
    /// </summary>
    [Fact]
    public async Task AnAnalysisFinding_RendersItsDiagnosisFactsOnce_OnEveryChannel()
    {
        var alert = await ComposedAlertAsync();
        var branding = EmailAlertService.Branding;

        /* Email, both bodies. They are built by two different methods and a repair to one says nothing
           about the other, so each is counted separately. */
        var (html, plain) = EmailTemplateBuilder.BuildAlertEmail(
            alert.MetricName, alert.ServerName, alert.CurrentValue, alert.ThresholdValue, 15, branding,
            context: alert.Context, detailText: alert.DeliveredProse);
        Assert.Equal(1, Occurrences(html, DatabaseMarker));
        Assert.Equal(1, Occurrences(plain, DatabaseMarker));

        var (htmlBefore, plainBefore) = EmailTemplateBuilder.BuildAlertEmail(
            alert.MetricName, alert.ServerName, alert.CurrentValue, alert.ThresholdValue, 15, branding,
            context: alert.Context, detailText: alert.DetailText);
        Assert.Equal(2, Occurrences(htmlBefore, DatabaseMarker));
        Assert.Equal(2, Occurrences(plainBefore, DatabaseMarker));

        /* Teams, Slack and the generic channel. PagerDuty takes the same resolved prose from the same
           single resolution in the fan-out; its endpoint is the hardcoded Events v2 URL, and it is
           covered at its builder in PagerDutyWebhookTests. */
        var teams = WebhookAlertService.BuildTeamsPayload(
            alert.MetricName, alert.ServerName, alert.CurrentValue, alert.ThresholdValue, branding,
            context: alert.Context, detailText: alert.DeliveredProse);
        Assert.Equal(1, Occurrences(teams, DatabaseMarker));
        Assert.Equal(2, Occurrences(WebhookAlertService.BuildTeamsPayload(
            alert.MetricName, alert.ServerName, alert.CurrentValue, alert.ThresholdValue, branding,
            context: alert.Context, detailText: alert.DetailText), DatabaseMarker));

        var slack = WebhookAlertService.BuildSlackPayload(
            alert.MetricName, alert.ServerName, alert.CurrentValue, alert.ThresholdValue, branding,
            context: alert.Context, detailText: alert.DeliveredProse);
        Assert.Equal(1, Occurrences(slack, DatabaseMarker));
        Assert.Equal(2, Occurrences(WebhookAlertService.BuildSlackPayload(
            alert.MetricName, alert.ServerName, alert.CurrentValue, alert.ThresholdValue, branding,
            context: alert.Context, detailText: alert.DetailText), DatabaseMarker));

        var generic = WebhookAlertService.BuildGenericPayload(
            alert.MetricName, alert.ServerName, alert.CurrentValue, alert.ThresholdValue, branding,
            context: alert.Context, detailText: alert.DeliveredProse);
        Assert.Equal(1, Occurrences(generic, DatabaseMarker));
        Assert.Equal(2, Occurrences(WebhookAlertService.BuildGenericPayload(
            alert.MetricName, alert.ServerName, alert.CurrentValue, alert.ThresholdValue, branding,
            context: alert.Context, detailText: alert.DetailText), DatabaseMarker));
    }

    /// <summary>
    /// The facts that are delivered are the STRUCTURED ones, not the prose. Stated separately from the
    /// count above because "once" is satisfied by either survivor, and which one survives decides whether
    /// the advice, the remediation T-SQL and the drill-down come with it.
    /// </summary>
    [Fact]
    public async Task TheSurvivingRendering_IsTheStructuredContext_NotTheProse()
    {
        var alert = await ComposedAlertAsync();

        var (_, plain) = EmailTemplateBuilder.BuildAlertEmail(
            alert.MetricName, alert.ServerName, alert.CurrentValue, alert.ThresholdValue, 15,
            EmailAlertService.Branding, context: alert.Context, detailText: alert.DeliveredProse);

        /* The Diagnosis item's heading and its own field labels. */
        Assert.Contains("Diagnosis", plain, StringComparison.Ordinal);
        Assert.Contains("Notify threshold: 1.5", plain, StringComparison.Ordinal);

        /* And not the prose's labels for the same values. */
        Assert.DoesNotContain("Facts in chain", plain, StringComparison.Ordinal);
        Assert.DoesNotContain("(notify threshold 1.5)", plain, StringComparison.Ordinal);

        /* The context's other items are why it is the copy worth keeping. */
        Assert.Contains("Investigation:", plain, StringComparison.Ordinal);
    }

    /* ─────────────── what the row keeps ─────────────── */

    /// <summary>
    /// The constraint the whole approach rests on: suppressing at delivery must not change what is
    /// stored. Asserted on the record Lite's real <c>EmailAlertService</c> handed its history store,
    /// against a frozen literal — the same text, byte for byte, that the row held before this change.
    /// </summary>
    [Fact]
    public async Task ThePersistedDetailText_IsUnchanged_ByTheDeliverySuppression()
    {
        var alert = await ComposedAlertAsync();

        /* The producer's own output, and the value carried on the record. */
        Assert.Equal(PersistedDetailText, FindingMessageFormatter.DetailText(Finding(), NotifyThreshold));
        Assert.Equal(PersistedDetailText, alert.DetailText);

        /* And the value that reaches the store, through the real shell with the suppression active. */
        var settings = new FixedThresholdSettings();
        var store = new CapturingHistoryStore();
        var email = new EmailAlertService(
            settings, store,
            new WebhookAlertService(settings, EmailAlertService.Branding, new AppLoggerAdapter<WebhookAlertService>()),
            new AppLoggerAdapter<EmailAlertService>());

        await email.SendFindingAlertAsync(alert);

        var record = Assert.Single(store.Records);
        Assert.Equal(PersistedDetailText, record.DetailText);
        Assert.NotNull(record.ContextJson);
    }

    /* ─────────────── the other direction ─────────────── */

    /// <summary>
    /// A producer whose prose is NOT a restatement still has it delivered, and the suppression is read
    /// off the producer's declaration rather than hardcoded per SKU.
    ///
    /// <para>Without this the fix is indistinguishable from reverting #3297 for the finding path: "never
    /// deliver a finding's prose" passes every assertion above. The #2109 AG alerts are the standing
    /// example of prose that carries a remedy no structured field does, which is the case
    /// <c>AlertDetailText.ProseForDelivery</c> was deliberately built not to drop.</para>
    /// </summary>
    [Fact]
    public void AProseThatIsNotARestatement_IsStillDelivered()
    {
        const string Remedy = "Fix the underlying cause, then resume it with ALTER DATABASE [Sales] SET HADR RESUME.";

        var restating = new FindingAlert(
            "Analysis: cpu_pressure [diag0000]", "TestServer", "CPU_SPIKE (92.5)", "1.5", "1",
            new AlertContext(), 1.8, NotifyThreshold, Remedy, DeliverDetailText: false);
        Assert.Null(restating.DeliveredProse);

        var independent = restating with { DeliverDetailText = true };
        Assert.Equal(Remedy, independent.DeliveredProse);

        /* Both records carry the same DetailText: the declaration governs delivery only. */
        Assert.Equal(Remedy, restating.DetailText);
        Assert.Equal(Remedy, independent.DetailText);
    }

    /// <summary>
    /// The same declaration, read through Lite's real send shell and measured on the bytes that left the
    /// process. <c>EmailAlertService</c> constructs its own <c>EmailSendCore</c>, so there is no
    /// interception point short of a channel: a loopback generic webhook is the cheapest one.
    ///
    /// <para>Both directions on one endpoint, because the pair is the claim — a shell that always
    /// suppressed and a shell that never did would each satisfy half of it. Settings are a local fake
    /// rather than the <c>App</c> statics <c>AppAlertSettings</c> reads: those are process-global and two
    /// other test classes already write them, so a body count keyed on them would race.</para>
    /// </summary>
    [Fact]
    public async Task LitesSendShell_DeliversTheProseOnlyWhenTheProducerSaysTo()
    {
        const string Prose = "Signal wait time has exceeded its ceiling. Check for a runaway parallel query.";

        using var endpoint = new CapturingWebhookEndpoint();
        var settings = new FixedThresholdSettings(genericWebhookUrl: endpoint.Url);
        var store = new CapturingHistoryStore();
        var email = new EmailAlertService(
            settings, store,
            new WebhookAlertService(settings, EmailAlertService.Branding, new AppLoggerAdapter<WebhookAlertService>()),
            new AppLoggerAdapter<EmailAlertService>());

        var delivered = new FindingAlert(
            "Analysis: cpu_pressure [deliver0]", "TestServer", "CPU_SPIKE (92.5)", "1.5", "1",
            new AlertContext(), 1.8, NotifyThreshold, Prose, DeliverDetailText: true);

        await email.SendFindingAlertAsync(delivered);

        var withProse = Assert.Single(endpoint.Bodies);
        Assert.Contains("runaway parallel query", withProse, StringComparison.Ordinal);

        /* The same alert, declared a restatement: nothing on the wire, everything on the row. */
        await email.SendFindingAlertAsync(delivered with
        {
            MetricName = "Analysis: cpu_pressure [suppress]",
            DeliverDetailText = false
        });

        Assert.Equal(2, endpoint.Bodies.Count);
        Assert.DoesNotContain("runaway parallel query", endpoint.Bodies[1], StringComparison.Ordinal);
        Assert.Equal(2, store.Records.Count);
        Assert.All(store.Records, r => Assert.Equal(Prose, r.DetailText));
    }

    /* ─────────────── helpers ─────────────── */

    private static int Occurrences(string haystack, string needle)
    {
        int count = 0, index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }

    private sealed class CapturingSender : IFindingAlertSender
    {
        public List<FindingAlert> Sent { get; } = new();

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName) =>
            Task.FromResult<DateTime?>(null);

        public Task SendFindingAlertAsync(FindingAlert alert)
        {
            Sent.Add(alert);
            return Task.CompletedTask;
        }
    }

    private sealed class CapturingHistoryStore : IAlertHistoryStore
    {
        public List<AlertHistoryRecord> Records { get; } = new();

        public Task RecordAlertAsync(AlertHistoryRecord record)
        {
            Records.Add(record);
            return Task.CompletedTask;
        }

        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);
    }

    /// <summary>
    /// Settings with a fixed analysis threshold and, optionally, one generic webhook pointed wherever the
    /// caller says. Deliberately NOT <c>AppAlertSettings</c>: that reads process-global <c>App</c> statics.
    /// </summary>
    private sealed class FixedThresholdSettings : IAlertSettings
    {
        private readonly string _genericUrl;

        public FixedThresholdSettings(string genericWebhookUrl = "") => _genericUrl = genericWebhookUrl;

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

        public bool SlackWebhookEnabled => false;
        public string SlackWebhookUrl => "";
        public string SlackProxyAddress => "";

        public bool GenericWebhookEnabled => _genericUrl.Length > 0;
        public string GenericWebhookUrl => _genericUrl;
        public string GenericWebhookHeadersJson => "";
        public string GenericWebhookBodyTemplate => "";
        public string GenericWebhookProxyAddress => "";

        public bool PagerDutyEnabled => false;
        public string PagerDutyRoutingKey => "";
        public bool PagerDutyUseEuRegion => false;
        public string PagerDutyProxyAddress => "";

        public double AnalysisNotifySeverity => NotifyThreshold;
        public int AnalysisNotifyCooldownMinutes => 360;
        public string TriageBaseUrl => "";
    }

    /// <summary>
    /// A loopback endpoint that records the bodies posted to it. <c>TcpListener</c> rather than
    /// <c>HttpListener</c> for the reason <c>Darling.Tests.AlertDeliveryChannelTests</c> gives: the latter
    /// wants a URL ACL on Windows, and four lines of HTTP are cheaper than that dependency.
    /// </summary>
    private sealed class CapturingWebhookEndpoint : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly List<string> _bodies = new();
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _accepting;

        public CapturingWebhookEndpoint()
        {
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            Url = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/hook";
            _accepting = Task.Run(AcceptLoopAsync);
        }

        public string Url { get; }

        /// <summary>
        /// Safe to read without synchronization once the send has been awaited: each body is appended
        /// before its response is written, and the sender awaits every response in turn.
        /// </summary>
        public IReadOnlyList<string> Bodies => _bodies;

        private async Task AcceptLoopAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    using var stream = client.GetStream();
                    _bodies.Add(await ReadRequestBodyAsync(stream));

                    var response = Encoding.ASCII.GetBytes(
                        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    await stream.WriteAsync(response, _stop.Token);
                    await stream.FlushAsync(_stop.Token);
                }
            }
            catch (OperationCanceledException) { /* Dispose */ }
            catch (SocketException) { /* listener stopped */ }
            catch (ObjectDisposedException) { /* listener stopped */ }
        }

        /// <summary>Reads headers to the blank line, then exactly Content-Length bytes of body.</summary>
        private static async Task<string> ReadRequestBodyAsync(NetworkStream stream)
        {
            var buffer = new byte[16 * 1024];
            var received = new List<byte>(capacity: 16 * 1024);
            int headerEnd;

            while ((headerEnd = IndexOfHeaderEnd(received)) < 0)
            {
                var read = await stream.ReadAsync(buffer);
                if (read == 0)
                {
                    return Encoding.UTF8.GetString(received.ToArray());
                }

                received.AddRange(new ArraySegment<byte>(buffer, 0, read));
            }

            var headers = Encoding.ASCII.GetString(received.ToArray(), 0, headerEnd);
            var contentLength = ParseContentLength(headers);
            var bodyStart = headerEnd + 4;

            while (received.Count - bodyStart < contentLength)
            {
                var read = await stream.ReadAsync(buffer);
                if (read == 0)
                {
                    break;
                }

                received.AddRange(new ArraySegment<byte>(buffer, 0, read));
            }

            var body = received.ToArray();
            var available = Math.Min(contentLength, body.Length - bodyStart);
            return Encoding.UTF8.GetString(body, bodyStart, Math.Max(0, available));
        }

        private static int IndexOfHeaderEnd(List<byte> bytes)
        {
            for (int i = 0; i + 3 < bytes.Count; i++)
            {
                if (bytes[i] == (byte)'\r' && bytes[i + 1] == (byte)'\n' &&
                    bytes[i + 2] == (byte)'\r' && bytes[i + 3] == (byte)'\n')
                {
                    return i;
                }
            }

            return -1;
        }

        private static int ParseContentLength(string headers)
        {
            foreach (var line in headers.Split("\r\n", StringSplitOptions.RemoveEmptyEntries))
            {
                if (line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase) &&
                    int.TryParse(line.AsSpan("Content-Length:".Length).Trim(), out var length))
                {
                    return length;
                }
            }

            return 0;
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();
            try
            {
                _accepting.Wait(TimeSpan.FromSeconds(5));
            }
            catch (AggregateException) { /* the cancellation above */ }

            _stop.Dispose();
        }
    }
}
