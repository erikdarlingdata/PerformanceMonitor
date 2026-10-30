/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The PagerDuty half of #6718: a resolve closes the incident its trigger opened. The trigger keys on the #3313
/// delivery-filtered context, so the deliverer must record the key the trigger carried, not one derived again from the
/// alert's unfiltered context. Driven through <see cref="DarlingAlertDeliverer"/> with PagerDuty's capture seam.
/// </summary>
public sealed class PagerDutyResolveSentKeyTests
{
    private const string ServerKey = "7";
    private const string Server = "SQL01";
    private const string Metric = "High CPU";
    private const string Template = """{"status":"{{status}}","triage":"{{triage_url}}"}""";

    private static readonly DateTime FiredAt = new(2026, 10, 15, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// The alert carries incidents A and B, and A is still inside its own cooldown window, so the trigger renders only B and
    /// keys on B. The resolve must close B, from memory and after a restart, where the history row still holds both
    /// incidents. The triage link the resolution carries names B too, the incident PagerDuty opened.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ANarrowedFire_AndItsPagerDutyResolve_CloseTheKeyTheTriggerOpened(bool restart)
    {
        const string inWindow = "db42:deadlock-in-window";
        const string fresh = "db42:deadlock-fresh";
        var pagerDuty = new List<string>();
        using var endpoint = new CapturingWebhookEndpoint();
        var config = new DarlingConfig();
        config.Webhooks.PagerDutyRoutingKey = "rk-test";
        config.Webhooks.PagerDutyAutoResolve = true;
        config.Webhooks.GenericUrl = endpoint.Url;
        config.Webhooks.GenericBodyTemplate = Template;
        config.Webhooks.GenericSendResolutions = true;
        config.Web.Enabled = true;
        config.Web.PublicBaseUrl = "https://monitor.example.invalid";
        /* The cooldown's restart seed puts A inside its window. It runs on the real clock, so the seed is a minute before now. */
        var store = new GenericWebhookStatusDedupTests.HistoryBackedStore
        {
            SeededDedupKey = inWindow,
            SeededSentUtc = DateTime.UtcNow.AddMinutes(-1),
        };
        var context = new AlertContext();
        AlertIncidentRenderer.Apply(context, new[]
        {
            new AlertIncident(inWindow, new[] { "dbo.Orders" }),
            new AlertIncident(fresh, new[] { "dbo.Shipments" }),
        });

        var first = Build(config, store, pagerDuty);
        await first.DeliverAsync(
            new AlertOutcome(ServerKey, Server, Metric, "97%", "90%", Context: context, DetailText: null,
                NumericCurrentValue: 97, NumericThresholdValue: 90, Muted: false, Severity: null),
            TestContext.Current.CancellationToken);
        store.Now = FiredAt.AddMinutes(1);
        var closer = restart ? Build(config, store, pagerDuty) : first;
        await closer.DeliverResolutionAsync(
            new AlertResolution(ServerKey, Server, Metric, "CPU Resolved", $"{Server}: Total CPU back to 12%"),
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, pagerDuty.Count);
        var trigger = JsonDocument.Parse(pagerDuty[0]).RootElement;
        var resolve = JsonDocument.Parse(pagerDuty[1]).RootElement;
        Assert.Equal("trigger", trigger.GetProperty("event_action").GetString());
        Assert.Equal(fresh, trigger.GetProperty("dedup_key").GetString());
        Assert.Equal("resolve", resolve.GetProperty("event_action").GetString());
        Assert.Equal(trigger.GetProperty("dedup_key").GetString(), resolve.GetProperty("dedup_key").GetString());

        Assert.Equal(2, endpoint.Bodies.Count);
        var clear = JsonDocument.Parse(endpoint.Bodies[1]).RootElement;
        Assert.Equal("resolved", clear.GetProperty("status").GetString());
        Assert.Contains("&dedup=" + Uri.EscapeDataString(fresh), clear.GetProperty("triage").GetString(), StringComparison.Ordinal);
    }

    private static DarlingAlertDeliverer Build(
        DarlingConfig config, GenericWebhookStatusDedupTests.HistoryBackedStore store, List<string> pagerDuty)
    {
        var settings = new DarlingAlertSettings(config);
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, store);
        webhooks.PostPagerDutyAsyncOverride = (_, payload) =>
        {
            pagerDuty.Add(payload);
            return Task.FromResult<string?>(null);
        };
        return new DarlingAlertDeliverer(settings, store, webhooks, NullLogger.Instance, utcNow: () => store.Now);
    }
}
