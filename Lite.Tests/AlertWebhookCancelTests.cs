/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Threading;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4752, Lite's half: a caller's cancel ends a webhook post in flight. Before, <c>LiteAlertDeliverer</c> held
/// the token its caller gave it but never handed it to <c>EmailAlertService.TrySendAlertEmailAsync</c>, so one
/// endpoint that accepted the connection and never answered held a delivery for the whole post timeout no matter
/// what the caller did, and the engine's stop waited it out. Driven through the real deliverer, email service
/// and webhook sender against a loopback endpoint that accepts and never answers.
/// </summary>
public sealed class AlertWebhookCancelTests
{
    /* Well above anything the cancelled path needs (an immediate cancel) and well below the ten seconds the
       post would otherwise hang, so a pass cannot be the post's own timeout ending the call. */
    private static readonly TimeSpan PromptBound = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The deliverer's shutdown path, reached through a real post. A caller's cancel is a stop request, not a
    /// failed delivery: it comes out of <see cref="LiteAlertDeliverer"/> as the
    /// <see cref="OperationCanceledException"/> it is, no history row is written for a delivery the engine
    /// abandoned, and the channel's failure count does not move, because the endpoint was never at fault.
    /// </summary>
    [Fact]
    public async Task ACancelInTheMiddleOfAPost_LeavesTheDelivererWithTheCancellation_AndWritesNoHistoryRow()
    {
        var configDir = Path.Combine(Path.GetTempPath(), "pmlite-webhookcancel-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(configDir);

        try
        {
            using var endpoint = new HungWebhookEndpoint();
            var settings = new GenericWebhookSettings(endpoint.Url);
            var history = new CapturingHistoryStore();
            var webhooks = new WebhookAlertService(
                settings, EmailAlertService.Branding, new AppLoggerAdapter<WebhookAlertService>());
            var emailAlerts = new EmailAlertService(
                settings, history, webhooks, new AppLoggerAdapter<EmailAlertService>());
            var deliverer = new LiteAlertDeliverer(
                emailAlerts,
                new MuteRuleService(new NoMuteRules(), new AppLoggerAdapter<MuteRuleService>()),
                new ServerManager(configDir),
                () => null,
                Dispatcher.CurrentDispatcher);
            using var cts = new CancellationTokenSource();

            var delivery = deliverer.DeliverAndReportAsync(
                new AlertOutcome(
                    "1", "SQL01", "High CPU", "97%", "90%", null, "detail", 97, 90, false, null,
                    "Total CPU at 97% (threshold: 90%)"),
                cts.Token);

            await endpoint.Connected.WaitAsync(PromptBound);
            Assert.False(delivery.IsCompleted);
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => delivery.WaitAsync(PromptBound));

            Assert.Empty(history.Records);
            Assert.Equal((0, (string?)null), webhooks.GetGenericHealth());
        }
        finally
        {
            Directory.Delete(configDir, recursive: true);
        }
    }

    /* A loopback listener that accepts every connection and never reads or answers, which is what a
       firewalled or wedged endpoint looks like from the client's side. TcpListener rather than HttpListener
       because the latter wants a URL ACL on Windows. Connected completes on the first accept, so the test can
       cancel while the post is genuinely in flight and not before it has connected. */
    private sealed class HungWebhookEndpoint : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _stop = new();
        private readonly List<TcpClient> _held = new();
        private readonly TaskCompletionSource _connected = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Task _accepting;

        public HungWebhookEndpoint()
        {
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            Url = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/hook";
            _accepting = Task.Run(AcceptLoopAsync);
        }

        public string Url { get; }

        public Task Connected => _connected.Task;

        private async Task AcceptLoopAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    lock (_held)
                    {
                        _held.Add(client);
                    }

                    _connected.TrySetResult();
                }
            }
            catch (OperationCanceledException) { /* Dispose */ }
            catch (SocketException) { /* listener stopped */ }
            catch (ObjectDisposedException) { /* listener stopped */ }
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();

            try
            {
                _accepting.Wait(TimeSpan.FromSeconds(5));
            }
            catch (AggregateException) { /* the loop's own stop */ }

            lock (_held)
            {
                foreach (var client in _held)
                {
                    client.Dispose();
                }
            }

            _stop.Dispose();
        }
    }

    private sealed class CapturingHistoryStore : IAlertHistoryStore
    {
        public List<AlertHistoryRecord> Records { get; } = new();

        public Task RecordAlertAsync(AlertHistoryRecord record)
        {
            lock (Records)
            {
                Records.Add(record);
            }

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

    /// <summary>The deliverer's constructor wants a mute-rule service; this one holds no rules, and the tray is
    /// absent, so nothing here is ever consulted.</summary>
    private sealed class NoMuteRules : IMuteRuleStore
    {
        public Task<IReadOnlyList<MuteRule>> LoadAllAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyList<MuteRule>>(new List<MuteRule>());

        public Task InsertAsync(MuteRule rule) => Task.CompletedTask;
        public Task UpdateAsync(MuteRule rule) => Task.CompletedTask;
        public Task SetEnabledAsync(string ruleId, bool enabled) => Task.CompletedTask;
        public Task DeleteAsync(string ruleId) => Task.CompletedTask;
        public Task DeleteExpiredAsync(IReadOnlyList<string> expiredIds) => Task.CompletedTask;
    }

    /// <summary>Settings with only the generic webhook configured. Deliberately NOT <c>AppAlertSettings</c>,
    /// which reads process-global <c>App</c> statics.</summary>
    private sealed class GenericWebhookSettings : IAlertSettings
    {
        private readonly string _genericUrl;

        public GenericWebhookSettings(string genericUrl)
        {
            _genericUrl = genericUrl;
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

        public double AnalysisNotifySeverity => 0.5;
        public int AnalysisNotifyCooldownMinutes => 360;
        public int AnalysisPageCap => 10;
        public string TriageBaseUrl => "";
    }
}
