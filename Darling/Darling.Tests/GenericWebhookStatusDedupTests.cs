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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6718: the generic webhook can open and close an alert in a receiver that matches on a key (incident.io is the one the
/// issue names). A fire and its resolution render the same <c>{{dedup_key}}</c>, and <c>{{status}}</c> says <c>firing</c> then
/// <c>resolved</c>. Driven through <see cref="DarlingAlertDeliverer"/> against a loopback endpoint, reading the bodies it received.
/// </summary>
public sealed class GenericWebhookStatusDedupTests
{
    private const string ServerKey = "7";
    private const string Server = "SQL01";
    private const string Metric = "High CPU";
    private const string Template =
        """{"status":"{{status}}","key":"{{dedup_key}}","metric":"{{metric}}","server":"{{server}}"}""";

    private static readonly DateTime FiredAt = new(2026, 10, 15, 12, 0, 0, DateTimeKind.Utc);

    private static AlertResolution CpuResolution() =>
        new(ServerKey, Server, Metric, "CPU Resolved", $"{Server}: Total CPU back to 12%");

    private static AlertOutcome Fire(AlertContext? context = null) =>
        new(ServerKey, Server, Metric, "97%", "90%", Context: context, DetailText: null,
            NumericCurrentValue: 97, NumericThresholdValue: 90, Muted: false, Severity: null);

    private static DarlingConfig ConfigFor(CapturingWebhookEndpoint endpoint)
    {
        var config = new DarlingConfig();
        config.Webhooks.GenericUrl = endpoint.Url;
        config.Webhooks.GenericBodyTemplate = Template;
        config.Webhooks.GenericSendResolutions = true;
        return config;
    }

    private static JsonElement Body(CapturingWebhookEndpoint endpoint, int index) =>
        JsonDocument.Parse(endpoint.Bodies[index]).RootElement;

    /// <summary>A fire with no incident keys on the serverId:metric fallback. Its resolution renders the same key, the status
    /// moves from firing to resolved, and the resolution keeps its own title as the metric.</summary>
    [Fact]
    public async Task AFireAndItsResolution_RenderTheSameDedupKey_AndStatusMovesFromFiringToResolved()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        var store = new HistoryBackedStore();
        var deliverer = Build(ConfigFor(endpoint), store);

        await deliverer.DeliverAsync(Fire(), TestContext.Current.CancellationToken);
        store.Now = FiredAt.AddMinutes(1);
        await deliverer.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, endpoint.Bodies.Count);
        var fire = Body(endpoint, 0);
        var clear = Body(endpoint, 1);
        Assert.Equal("firing", fire.GetProperty("status").GetString());
        Assert.Equal("resolved", clear.GetProperty("status").GetString());
        Assert.Equal($"{ServerKey}:{Metric}", fire.GetProperty("key").GetString());
        Assert.Equal(fire.GetProperty("key").GetString(), clear.GetProperty("key").GetString());
        Assert.Equal(Metric, fire.GetProperty("metric").GetString());
        Assert.Equal("CPU Resolved", clear.GetProperty("metric").GetString());
    }

    /// <summary>An alert whose context carries an incident keys on the incident's own dedup key, the same one PagerDuty uses.
    /// The resolution renders it too, whether the deliverer that sent the fire is still running or the service restarted
    /// between the fire and the clear and the key is read back from the fire's history row.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnIncidentKeyedFire_AndItsResolution_RenderTheIncidentsKey(bool restart)
    {
        using var endpoint = new CapturingWebhookEndpoint();
        var config = ConfigFor(endpoint);
        var store = new HistoryBackedStore();
        var context = new AlertContext
        {
            Incidents = new List<AlertIncident> { new("db42:blocking-chain", new[] { "dbo.Orders" }) },
        };

        var first = Build(config, store);
        await first.DeliverAsync(Fire(context), TestContext.Current.CancellationToken);
        store.Now = FiredAt.AddMinutes(1);
        var closer = restart ? Build(config, store) : first;
        await closer.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, endpoint.Bodies.Count);
        Assert.Equal("db42:blocking-chain", Body(endpoint, 0).GetProperty("key").GetString());
        Assert.Equal("db42:blocking-chain", Body(endpoint, 1).GetProperty("key").GetString());
        Assert.Equal("resolved", Body(endpoint, 1).GetProperty("status").GetString());
    }

    /// <summary>
    /// The #3313 filter narrows the post. The alert carries incidents A and B, and A is still inside its own cooldown window,
    /// so the post renders only B and keys on B (the key anchors on the first incident the post shows). The resolution must
    /// close B too, from memory and after a restart, where the history row still holds both incidents.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ANarrowedFire_AndItsResolution_RenderTheKeyThePostRendered(bool restart)
    {
        const string inWindow = "db42:deadlock-in-window";
        const string fresh = "db42:deadlock-fresh";
        using var endpoint = new CapturingWebhookEndpoint();
        var config = ConfigFor(endpoint);
        /* The cooldown's restart seed is how an incident gets into its window without waiting through one. It runs on the
           real clock, so the seed is a minute before now. */
        var store = new HistoryBackedStore { SeededDedupKey = inWindow, SeededSentUtc = DateTime.UtcNow.AddMinutes(-1) };
        var context = new AlertContext();
        AlertIncidentRenderer.Apply(context, new[]
        {
            new AlertIncident(inWindow, new[] { "dbo.Orders" }),
            new AlertIncident(fresh, new[] { "dbo.Shipments" }),
        });

        var first = Build(config, store);
        await first.DeliverAsync(Fire(context), TestContext.Current.CancellationToken);
        store.Now = FiredAt.AddMinutes(1);
        var closer = restart ? Build(config, store) : first;
        await closer.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, endpoint.Bodies.Count);
        var fire = Body(endpoint, 0);
        var clear = Body(endpoint, 1);
        Assert.Equal("firing", fire.GetProperty("status").GetString());
        Assert.Equal(fresh, fire.GetProperty("key").GetString());
        Assert.Equal("resolved", clear.GetProperty("status").GetString());
        Assert.Equal(fire.GetProperty("key").GetString(), clear.GetProperty("key").GetString());
    }

    /// <summary>With PagerDuty auto-resolve on, the generic <c>{{dedup_key}}</c> still ignores the flag (#5469), on the fire and on
    /// the resolution alike, so the two stay equal while PagerDuty's own key is free to differ.</summary>
    [Fact]
    public async Task WithPagerDutyAutoResolveOn_TheGenericKeysStillMatch()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        var config = ConfigFor(endpoint);
        config.Webhooks.PagerDutyRoutingKey = "rk-test";
        config.Webhooks.PagerDutyAutoResolve = true;
        var store = new HistoryBackedStore();
        var deliverer = Build(config, store);

        await deliverer.DeliverAsync(Fire(), TestContext.Current.CancellationToken);
        await deliverer.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, endpoint.Bodies.Count);
        Assert.Equal(Body(endpoint, 0).GetProperty("key").GetString(), Body(endpoint, 1).GetProperty("key").GetString());
        Assert.Equal("resolved", Body(endpoint, 1).GetProperty("status").GetString());
    }

    private static DarlingAlertDeliverer Build(DarlingConfig config, HistoryBackedStore store)
    {
        var settings = new DarlingAlertSettings(config);
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, store);
        webhooks.PostPagerDutyAsyncOverride = (_, _) => Task.FromResult<string?>(null);
        return new DarlingAlertDeliverer(settings, store, webhooks, NullLogger.Instance, utcNow: () => store.Now);
    }

    /// <summary>A history that outlives the deliverer, as the store does across a restart: it keeps every row and answers
    /// <see cref="IDeliveredFireReader"/> the way the store's query does.</summary>
    internal sealed class HistoryBackedStore : IAlertHistoryStore, IDeliveredFireReader
    {
        private readonly List<(DateTime At, AlertHistoryRecord Record)> _rows = new();

        public DateTime Now { get; set; } = FiredAt;

        /// <summary>One incident key the cooldown's restart seed reports as sent at <see cref="SeededSentUtc"/>.</summary>
        public string? SeededDedupKey { get; init; }

        public DateTime SeededSentUtc { get; init; }

        private Task<DateTime?> Seed(string? dedupKey) =>
            Task.FromResult<DateTime?>(
                SeededDedupKey is not null && string.Equals(dedupKey, SeededDedupKey, StringComparison.Ordinal) ? SeededSentUtc : null);

        public Task RecordAlertAsync(AlertHistoryRecord record)
        {
            _rows.Add((Now, record));
            return Task.CompletedTask;
        }

        public Task<DeliveredFireHistory?> GetOpenDeliveredFiresAsync(
            string serverKey, string metricName, string resolutionTitle, CancellationToken cancellationToken = default)
        {
            var lastClear = _rows.FindLastIndex(r => r.Record.ServerId == serverKey && r.Record.MetricName == resolutionTitle);
            var open = _rows.Skip(lastClear + 1)
                .Where(r => r.Record.ServerId == serverKey && r.Record.MetricName == metricName && r.Record.AlertSent && !r.Record.Muted)
                .Reverse()
                .ToList();
            return Task.FromResult(open.Count == 0
                ? null
                : new DeliveredFireHistory(
                    open[^1].At,
                    open.Select(r => new DeliveredFireRow(r.At, r.Record.NumericCurrentValue, r.Record.DetailText, r.Record.ContextJson)).ToList()));
        }

        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Seed(dedupKey);

        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Seed(dedupKey);

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
            Seed(dedupKey);

        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) => Task.FromResult<DateTime?>(null);
    }
}
