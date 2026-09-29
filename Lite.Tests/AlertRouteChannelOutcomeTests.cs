/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4750, Lite's half: <c>EmailAlertService.TrySendAlertEmailAsync</c> writes Lite's one alert-history row,
/// and an alert that reached one webhook and failed on another used to be stored as a plain delivery with
/// no trace of the failing channel. Lite has no routes table, so the row's route record names the channels
/// the parent settings resolve to (source <c>Default</c>) and, since this change, what the send to each did.
///
/// <para>Driven through the real <c>EmailAlertService</c> and <c>WebhookAlertService</c> against loopback
/// endpoints that answer with a real status line, and read back from the JSON the store was handed. Only the
/// outcome word is stored: a webhook failure message can name the endpoint's URL, which is a secret.</para>
/// </summary>
public sealed class AlertRouteChannelOutcomeTests
{
    /// <summary>Generic delivers, Slack answers 500: the row is a delivery and its route record names both
    /// channels with their own outcome and the Default source, with no error text.</summary>
    [Fact]
    public async Task AWebhookThatFailsBesideOneThatDelivers_IsNamedOnTheStoredRow()
    {
        using var generic = new Endpoint();
        using var slack = new Endpoint(statusCode: 500);
        var (service, store) = Build(new WebhookSettings(genericUrl: generic.Url, slackUrl: slack.Url));

        var delivery = await service.TrySendAlertEmailAsync("High CPU", "SQL01", "97%", "90%", serverId: 1);

        Assert.Equal(1, generic.Requests);
        Assert.Equal(1, slack.Requests);
        Assert.NotNull(delivery);
        Assert.True(delivery!.Sent);

        var record = Assert.Single(store.Records);
        var destinations = Destinations(record.ContextJson);
        Assert.Equal(2, destinations.Count);
        Assert.Equal(("Default", "delivered"), destinations["Generic"]);
        Assert.Equal(("Default", "failed"), destinations["Slack"]);

        var json = record.ContextJson!;
        Assert.DoesNotContain("HTTP 500", json, StringComparison.Ordinal);
        Assert.DoesNotContain(slack.Url, json, StringComparison.Ordinal);
        Assert.DoesNotContain("127.0.0.1", json, StringComparison.Ordinal);
    }

    /// <summary>A muted alert attempts no channel, so its row carries no route record: there was no outcome to
    /// record, and the context stays null exactly as it did.</summary>
    [Fact]
    public async Task AMutedAlert_RecordsNoRouteOutcomes()
    {
        using var generic = new Endpoint();
        var (service, store) = Build(new WebhookSettings(genericUrl: generic.Url));

        await service.TrySendAlertEmailAsync("High CPU", "SQL01", "97%", "90%", serverId: 1, muted: true);

        Assert.Equal(0, generic.Requests);
        var record = Assert.Single(store.Records);
        Assert.Null(record.ContextJson);
    }

    private static (EmailAlertService Service, HistoryStore Store) Build(WebhookSettings settings)
    {
        var store = new HistoryStore();
        var service = new EmailAlertService(
            settings, store,
            new WebhookAlertService(settings, EmailAlertService.Branding, new AppLoggerAdapter<WebhookAlertService>()),
            new AppLoggerAdapter<EmailAlertService>());
        return (service, store);
    }

    /// <summary>Channel to (source, outcome) from the stored route record; the outcome is null where the member
    /// is absent.</summary>
    private static Dictionary<string, (string Source, string? Outcome)> Destinations(string? contextJson)
    {
        Assert.NotNull(contextJson);
        using var document = JsonDocument.Parse(contextJson);
        return document.RootElement.GetProperty("Route").GetProperty("Destinations").EnumerateArray().ToDictionary(
            d => d.GetProperty("Channel").GetString()!,
            d => (
                d.GetProperty("Source").GetString()!,
                d.TryGetProperty("Outcome", out var outcome) && outcome.ValueKind == JsonValueKind.String
                    ? outcome.GetString()
                    : null));
    }

    private sealed class HistoryStore : IAlertHistoryStore
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

        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) =>
            Task.FromResult<DateTime?>(null);
    }

    /// <summary>Settings with up to two webhooks pointed wherever the caller says. Deliberately NOT
    /// <c>AppAlertSettings</c>, which reads process-global <c>App</c> statics.</summary>
    private sealed class WebhookSettings : IAlertSettings
    {
        private readonly string _genericUrl;
        private readonly string _slackUrl;

        public WebhookSettings(string genericUrl = "", string slackUrl = "")
        {
            _genericUrl = genericUrl;
            _slackUrl = slackUrl;
        }

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

        public bool SlackWebhookEnabled => _slackUrl.Length > 0;
        public string SlackWebhookUrl => _slackUrl;
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

        public double AnalysisNotifySeverity => 0.5;
        public int AnalysisNotifyCooldownMinutes => 360;
        public int AnalysisPageCap => 10;
        public string TriageBaseUrl => "";
    }

    /// <summary>
    /// A loopback endpoint that answers every POST with the given status. <c>TcpListener</c> rather than
    /// <c>HttpListener</c> for the reason the analysis-prose suite gives: the latter wants a URL ACL on Windows,
    /// and a status line and two headers are cheaper than that dependency.
    /// </summary>
    private sealed class Endpoint : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _accepting;
        private readonly int _statusCode;
        private int _requests;

        public Endpoint(int statusCode = 200)
        {
            _statusCode = statusCode;
            _listener.Start();
            Url = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/hook";
            _accepting = Task.Run(AcceptLoopAsync);
        }

        public string Url { get; }

        /// <summary>Requests fully read and answered. Safe to read once the send has been awaited, because the
        /// count is bumped before the response is written and the sender awaits every response.</summary>
        public int Requests => Volatile.Read(ref _requests);

        private async Task AcceptLoopAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    using var stream = client.GetStream();
                    await ReadRequestAsync(stream, _stop.Token);
                    Interlocked.Increment(ref _requests);

                    var response = Encoding.ASCII.GetBytes(
                        $"HTTP/1.1 {_statusCode} Status\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    await stream.WriteAsync(response, _stop.Token);
                    await stream.FlushAsync(_stop.Token);
                }
            }
            catch (OperationCanceledException) { /* Dispose */ }
            catch (SocketException) { /* listener stopped */ }
            catch (ObjectDisposedException) { /* listener stopped */ }
        }

        /// <summary>Reads to the blank line after the headers, then exactly Content-Length bytes of body.</summary>
        private static async Task ReadRequestAsync(NetworkStream stream, CancellationToken token)
        {
            var received = new List<byte>();
            var buffer = new byte[16 * 1024];
            var bodyStart = -1;
            var contentLength = 0;

            while (true)
            {
                if (bodyStart < 0)
                {
                    var headerEnd = IndexOfHeaderEnd(received);
                    if (headerEnd >= 0)
                    {
                        bodyStart = headerEnd + 4;
                        contentLength = ParseContentLength(Encoding.ASCII.GetString(received.ToArray(), 0, headerEnd));
                    }
                }

                if (bodyStart >= 0 && received.Count - bodyStart >= contentLength)
                {
                    return;
                }

                var read = await stream.ReadAsync(buffer, token);
                if (read == 0)
                {
                    return;
                }

                received.AddRange(new ArraySegment<byte>(buffer, 0, read));
            }
        }

        private static int IndexOfHeaderEnd(List<byte> bytes)
        {
            for (var i = 0; i + 3 < bytes.Count; i++)
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
