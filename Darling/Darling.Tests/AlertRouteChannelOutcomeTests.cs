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
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4750: with two or more channels, an alert that reached one and failed on another was recorded as
/// delivered with no <c>send_error</c>, and the row's route record listed the channels the route RESOLVED to,
/// not the ones that succeeded. So a rotated Slack URL or a 5xx from one endpoint was invisible in the
/// history for as long as a sibling channel kept delivering. The route record now says what each channel's
/// send did.
///
/// <para>Measured at the row the deliverer hands its history store, against loopback endpoints that answer
/// with a real status line, and read back as JSON rather than through the DTO: the stored text is the
/// contract, and the same assertions hold for any reader of it.</para>
///
/// <para>The row carries the outcome WORD only. A webhook failure message can name the endpoint's URL, which
/// is a secret, so the reason stays in <c>send_error</c> (unchanged: set only when nothing delivered) and
/// never enters <c>context_json</c>.</para>
/// </summary>
public sealed class AlertRouteChannelOutcomeTests
{
    /// <summary>The bug: Slack delivers, Generic answers 500. The row is a delivery, as it should be, and its
    /// route record now also names the channel that failed.</summary>
    [Fact]
    public async Task AChannelThatFailsBesideOneThatDelivers_IsNamedOnTheRow_WhileTheRowStillReadsDelivered()
    {
        using var slack = new CapturingWebhookEndpoint();
        using var generic = new CapturingWebhookEndpoint(statusCode: 500);

        var config = new DarlingConfig();
        config.Webhooks.SlackUrl = slack.Url;
        config.Webhooks.GenericUrl = generic.Url;
        var (deliverer, history) = Build(config);

        await deliverer.DeliverAsync(Outcome("Deadlocks Detected", "SQL01", "3", "1"), TestContext.Current.CancellationToken);

        /* Both endpoints were actually posted to; the failure is the 500, not an unreached channel. */
        Assert.Single(slack.Bodies);
        Assert.Single(generic.Bodies);

        var record = Assert.Single(history.Records);
        Assert.True(record.Delivery.Sent);
        Assert.Null(record.Delivery.SendError);

        var outcomes = RouteOutcomes(record.ContextJson);
        Assert.Equal(2, outcomes.Count);
        Assert.Equal("delivered", outcomes["Slack"]);
        Assert.Equal("failed", outcomes["Generic"]);
    }

    /// <summary>The reason is kept out of the context on purpose. When nothing delivers the row's
    /// <c>send_error</c> still carries the first failure, and the route record carries only the word: no
    /// status line, no body, and none of the endpoint's address.</summary>
    [Fact]
    public async Task WhenNoChannelDelivers_TheRowKeepsItsSendError_AndTheRouteRecordStoresNoErrorText()
    {
        using var generic = new CapturingWebhookEndpoint(statusCode: 500);

        var config = new DarlingConfig();
        config.Webhooks.GenericUrl = generic.Url;
        var (deliverer, history) = Build(config);

        await deliverer.DeliverAsync(Outcome("Deadlocks Detected", "SQL01", "3", "1"), TestContext.Current.CancellationToken);

        var record = Assert.Single(history.Records);
        Assert.False(record.Delivery.Sent);
        Assert.Contains("HTTP 500", record.Delivery.SendError, StringComparison.Ordinal);

        var outcomes = RouteOutcomes(record.ContextJson);
        Assert.Equal("failed", Assert.Single(outcomes).Value);

        var json = record.ContextJson!;
        Assert.DoesNotContain("HTTP 500", json, StringComparison.Ordinal);
        Assert.DoesNotContain(generic.Url, json, StringComparison.Ordinal);
        Assert.DoesNotContain("127.0.0.1", json, StringComparison.Ordinal);
    }

    /// <summary>Email is a channel like the rest: attempted and sent, it joins the record as delivered,
    /// beside the webhooks' own entries.</summary>
    [Fact]
    public async Task AnEmailThatWasSent_JoinsTheRecord_BesideTheWebhooksOutcomes()
    {
        using var slack = new CapturingWebhookEndpoint();
        using var generic = new CapturingWebhookEndpoint(statusCode: 500);
        using var smtp = new CapturingSmtpEndpoint();

        var config = new DarlingConfig();
        config.Webhooks.SlackUrl = slack.Url;
        config.Webhooks.GenericUrl = generic.Url;
        ConfigureSmtp(config, smtp);
        var (deliverer, history) = Build(config);

        await deliverer.DeliverAsync(Outcome("Deadlocks Detected", "SQL01", "3", "1"), TestContext.Current.CancellationToken);

        Assert.Single(smtp.Messages);

        var record = Assert.Single(history.Records);
        var outcomes = RouteOutcomes(record.ContextJson);
        Assert.Equal(3, outcomes.Count);
        Assert.Equal("delivered", outcomes["Slack"]);
        Assert.Equal("failed", outcomes["Generic"]);
        Assert.Equal("delivered", outcomes["Email"]);
    }

    /// <summary>A channel the route resolved to but this firing never sent to is listed, and says so: here
    /// the email cooldown is still open (the alert log says an email went out a moment ago) while the
    /// webhooks are owed the page, so the record must not read as if the email had gone or failed.</summary>
    [Fact]
    public async Task AChannelHeldBackByItsCooldown_IsListedAsNotAttempted()
    {
        using var slack = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();

        var config = new DarlingConfig();
        config.Webhooks.SlackUrl = slack.Url;
        ConfigureSmtp(config, smtp);
        var (deliverer, history) = Build(config, lastEmailSentUtc: DateTime.UtcNow);

        await deliverer.DeliverAsync(Outcome("Deadlocks Detected", "SQL01", "3", "1"), TestContext.Current.CancellationToken);

        Assert.Single(slack.Bodies);
        Assert.Empty(smtp.Messages);

        var record = Assert.Single(history.Records);
        var outcomes = RouteOutcomes(record.ContextJson);
        Assert.Equal(2, outcomes.Count);
        Assert.Equal("delivered", outcomes["Slack"]);
        Assert.Equal("not attempted", outcomes["Email"]);
    }

    /* ---- helpers ------------------------------------------------------------------------------------- */

    /// <summary>Channel to the outcome text the stored route record gives it, or null where the member is
    /// absent. Read from the JSON itself so a passing assertion means the bytes on the row say it.</summary>
    private static Dictionary<string, string?> RouteOutcomes(string? contextJson)
    {
        Assert.NotNull(contextJson);
        using var document = JsonDocument.Parse(contextJson);
        var destinations = document.RootElement.GetProperty("Route").GetProperty("Destinations");
        return destinations.EnumerateArray().ToDictionary(
            d => d.GetProperty("Channel").GetString()!,
            d => d.TryGetProperty("Outcome", out var outcome) && outcome.ValueKind == JsonValueKind.String
                ? outcome.GetString()
                : null);
    }

    private static void ConfigureSmtp(DarlingConfig config, CapturingSmtpEndpoint smtp)
    {
        config.Smtp.Host = "127.0.0.1";
        config.Smtp.Port = smtp.Port;
        config.Smtp.UseSsl = false;
        config.Smtp.From = "monitor@example.invalid";
        config.Smtp.To = "operator@example.invalid";
    }

    private static (DarlingAlertDeliverer Deliverer, RecordingHistoryStore History) Build(
        DarlingConfig config, DateTime? lastEmailSentUtc = null)
    {
        var settings = new DarlingAlertSettings(config);
        var history = new RecordingHistoryStore(lastEmailSentUtc);
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        return (new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance), history);
    }

    private static AlertOutcome Outcome(string metric, string server, string value, string threshold) =>
        new(server, server, metric, value, threshold, Context: null, DetailText: null,
            NumericCurrentValue: 0, NumericThresholdValue: 0, Muted: false, Severity: null);

    /// <summary>Records every row, and answers the email cooldown's seed query with the instant it was given
    /// (null = no email ever sent), which is how a test opens that one channel's window.</summary>
    private sealed class RecordingHistoryStore : IAlertHistoryStore
    {
        private readonly DateTime? _lastEmailSentUtc;

        public RecordingHistoryStore(DateTime? lastEmailSentUtc) => _lastEmailSentUtc = lastEmailSentUtc;

        public List<AlertHistoryRecord> Records { get; } = new();

        public Task RecordAlertAsync(AlertHistoryRecord record)
        {
            Records.Add(record);
            return Task.CompletedTask;
        }

        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult(_lastEmailSentUtc);

        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) =>
            Task.FromResult<DateTime?>(null);
    }
}
