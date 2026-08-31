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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the per-server + per-event alert delivery mode (#1236 / #1141) the service now honors. The pure tests
/// need no Postgres: <see cref="DarlingAlertSettings"/> exposes the global mode + per-event cap through the
/// by-reference config seam (with the 1–100 clamp), and <see cref="DarlingAlertDeliverer"/> resolves a
/// per-server override against the global mode and — in Per-event mode only — fans an incident-carrying alert
/// out through the shared <see cref="PerEventNotification.Split"/> into one send+history-row per distinct
/// incident (capped, "+N more"), with SMTP/webhooks unconfigured so every send records a "tray" row. The
/// live test (gated on DARLING_TEST_PG) round-trips the two settings columns + the per-server override column
/// through a real seed/read against an isolated scratch store.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. It reaches DARLING_TEST_PG only to CREATE and
   DROP its own database through ScratchPostgres, then works entirely inside it — it never touches the shared
   database's tables, so it cannot race the live collection and serializing it would be pure slowdown. Leave it
   out; this comment is here so the next sweep does not "fix" it. */
public sealed class DarlingDeliveryModeTests
{
    /* ---------------- pure: the settings seam ---------------- */

    [Fact]
    public void DarlingAlertSettings_ReadsDeliveryModeAndPerEventMax_ThroughTheByReferenceSeam()
    {
        var config = new DarlingConfig();
        var settings = new DarlingAlertSettings(config);

        /* Defaults mirror the V18 DDL: Summary / 5. */
        Assert.Equal(AlertNotificationMode.Summary, settings.DeliveryMode);
        Assert.Equal(5, settings.PerEventMax);

        /* A store reload mutates the held config in place; the same adapter instance reflects it. */
        config.Alerts.DeliveryMode = AlertNotificationMode.PerEvent;
        config.Alerts.PerEventMax = 12;
        Assert.Equal(AlertNotificationMode.PerEvent, settings.DeliveryMode);
        Assert.Equal(12, settings.PerEventMax);
    }

    [Theory]
    [InlineData(0, 1)]      // below floor -> 1
    [InlineData(-4, 1)]     // negative -> 1
    [InlineData(250, 100)]  // above ceiling -> 100
    [InlineData(37, 37)]    // in range -> unchanged
    public void DarlingAlertSettings_ClampsPerEventMax_1To100(int configured, int expected)
    {
        var config = new DarlingConfig();
        config.Alerts.PerEventMax = configured;
        Assert.Equal(expected, new DarlingAlertSettings(config).PerEventMax);
    }

    /* ---------------- pure: ApplyToConfig carries the new fields through the seam ---------------- */

    [Fact]
    public void ApplyToConfig_SwapsDeliveryModeAndPerEventMax_ReflectedThroughTheSettingsSeam()
    {
        var config = new DarlingConfig();
        var settings = new DarlingAlertSettings(config);

        StoreConfigProvider.ApplyToConfig(config, new StoreConfigView
        {
            Alerts = new AlertsConfig { DeliveryMode = AlertNotificationMode.PerEvent, PerEventMax = 9 },
        });

        Assert.Equal(AlertNotificationMode.PerEvent, settings.DeliveryMode);
        Assert.Equal(9, settings.PerEventMax);
    }

    /* ---------------- pure: the deliverer Summary vs Per-event branch ---------------- */

    [Fact]
    public async Task Summary_WithIncidents_RecordsOneCombinedRow()
    {
        var config = new DarlingConfig(); // Summary default
        var (deliverer, history) = BuildDeliverer(config);

        await deliverer.DeliverAsync(OutcomeWithIncidents(3), TestContext.Current.CancellationToken);

        /* One combined summary row, carrying the server-level numerics. */
        var record = Assert.Single(history.Records);
        Assert.Equal("Deadlocks Detected", record.MetricName);
        Assert.Equal("tray", record.NotificationType); // no channel configured
        Assert.Equal(3, record.NumericCurrentValue);
    }

    [Fact]
    public async Task PerEvent_WithinCap_RecordsOneRowPerIncident_NoOverflow_WithPerIncidentNumerics()
    {
        var config = new DarlingConfig();
        config.Alerts.DeliveryMode = AlertNotificationMode.PerEvent;
        config.Alerts.PerEventMax = 5;
        var (deliverer, history) = BuildDeliverer(config);

        await deliverer.DeliverAsync(OutcomeWithIncidents(3), TestContext.Current.CancellationToken);

        Assert.Equal(3, history.Records.Count);
        /* #1830: each per-event row stores its OWN incident's occurrence count as the numeric
           (fixture counts are 1..n), with the outcome's threshold on every row — the old null
           numerics left the store text-parsing, which coined 0 for the overflow trailer. */
        Assert.Equal(new double?[] { 1, 2, 3 }, history.Records.Select(r => r.NumericCurrentValue).ToArray());
        Assert.All(history.Records, r => Assert.Equal(1d, r.NumericThresholdValue));
        Assert.DoesNotContain(history.Records, r => r.CurrentValueText.Contains("more", StringComparison.Ordinal));
    }

    [Fact]
    public async Task PerEvent_OverCap_CapsIndividualRows_AndBatchesOverflowWithPlusNMore()
    {
        var config = new DarlingConfig();
        config.Alerts.DeliveryMode = AlertNotificationMode.PerEvent;
        config.Alerts.PerEventMax = 2;
        var (deliverer, history) = BuildDeliverer(config);

        await deliverer.DeliverAsync(OutcomeWithIncidents(5), TestContext.Current.CancellationToken);

        /* 2 individual + 1 overflow batch = 3 rows; the overflow row carries "+3 more". */
        Assert.Equal(3, history.Records.Count);
        var overflowRow = Assert.Single(history.Records, r => r.CurrentValueText.Contains("+3 more", StringComparison.Ordinal));
        /* #1830: the overflow row's text is unparseable by design, so its numeric MUST be carried
           explicitly — this exact row is the one that silently stored 0. */
        Assert.Equal(3d, overflowRow.NumericCurrentValue);
    }

    [Fact]
    public async Task PerEvent_NoIncidents_FallsBackToTheSingleSummarySend()
    {
        var config = new DarlingConfig();
        config.Alerts.DeliveryMode = AlertNotificationMode.PerEvent;
        var (deliverer, history) = BuildDeliverer(config);

        /* A CPU-style alert carries no incidents — even in Per-event mode it takes the single combined path. */
        var outcome = new AlertOutcome("42", "srv", "High CPU", "92", "80",
            Context: null, DetailText: "detail", NumericCurrentValue: 92, NumericThresholdValue: 80,
            Muted: false, Severity: null);

        await deliverer.DeliverAsync(outcome, TestContext.Current.CancellationToken);

        var record = Assert.Single(history.Records);
        Assert.Equal(92, record.NumericCurrentValue);
    }

    [Fact]
    public async Task PerServerOverride_PerEvent_WinsOverGlobalSummary_AndSplits()
    {
        var config = new DarlingConfig(); // global Summary
        var (deliverer, history) = BuildDeliverer(config, resolveOverride: _ => AlertNotificationMode.PerEvent);

        await deliverer.DeliverAsync(OutcomeWithIncidents(3), TestContext.Current.CancellationToken);

        Assert.Equal(3, history.Records.Count); // the override forced Per-event for this server
    }

    [Fact]
    public async Task PerServerOverride_Summary_WinsOverGlobalPerEvent_AndDoesNotSplit()
    {
        var config = new DarlingConfig();
        config.Alerts.DeliveryMode = AlertNotificationMode.PerEvent; // global Per-event
        var (deliverer, history) = BuildDeliverer(config, resolveOverride: _ => AlertNotificationMode.Summary);

        await deliverer.DeliverAsync(OutcomeWithIncidents(3), TestContext.Current.CancellationToken);

        Assert.Single(history.Records); // the override forced Summary — one combined row
    }

    [Fact]
    public async Task NullOverride_InheritsTheGlobalMode()
    {
        var config = new DarlingConfig();
        config.Alerts.DeliveryMode = AlertNotificationMode.PerEvent;
        var (deliverer, history) = BuildDeliverer(config, resolveOverride: _ => null); // null = inherit

        await deliverer.DeliverAsync(OutcomeWithIncidents(4), TestContext.Current.CancellationToken);

        Assert.Equal(4, history.Records.Count); // inherited the global Per-event
    }

    /* ---------------- live (DARLING_TEST_PG): seed -> read round-trip of the V18 columns ---------------- */

    [Fact]
    public async Task SeedAndRead_RoundTripsDeliveryMode_AndPerServerOverride_AgainstScratchPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the seed/read round-trip (the test mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        /* SeedIfEmptyAsync no-ops on a store any earlier test already seeded, so this test needs
           a database of its own — see ScratchPostgres. */
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct); // brings the scratch store to V18
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var provider = new StoreConfigProvider(dataSource);

        var config = new DarlingConfig();
        config.Alerts.DeliveryMode = AlertNotificationMode.PerEvent;
        config.Alerts.PerEventMax = 7;
        config.Servers.Add(new MonitoredServer
        {
            Name = "v18-override",
            Host = "v18-scratch-host",
            Auth = "integrated",
            AlertDeliveryModeOverride = AlertNotificationMode.PerEvent,
        });
        config.Servers.Add(new MonitoredServer
        {
            Name = "v18-inherit",
            Host = "v18-scratch-inherit",
            Auth = "integrated",
            /* no override -> null -> inherit global */
        });

        await provider.SeedIfEmptyAsync(config, ct);

        var view = await provider.LoadViewAsync(new DarlingConfig(), ct);
        Assert.NotNull(view);

        /* The global settings columns round-tripped. */
        Assert.Equal(AlertNotificationMode.PerEvent, view!.Alerts.DeliveryMode);
        Assert.Equal(7, view.Alerts.PerEventMax);

        /* The per-server override column round-tripped (set on one server, null on the other). */
        var overridden = Assert.Single(view.EnabledServers, s => s.Host == "v18-scratch-host");
        Assert.Equal(AlertNotificationMode.PerEvent, overridden.AlertDeliveryModeOverride);
        var inherited = Assert.Single(view.EnabledServers, s => s.Host == "v18-scratch-inherit");
        Assert.Null(inherited.AlertDeliveryModeOverride);
    }

    /* ---------------- helpers ---------------- */

    private static (DarlingAlertDeliverer Deliverer, RecordingHistoryStore History) BuildDeliverer(
        DarlingConfig config, Func<string, AlertNotificationMode?>? resolveOverride = null)
    {
        var settings = new DarlingAlertSettings(config);
        var history = new RecordingHistoryStore();
        /* SMTP + webhooks unconfigured (DarlingConfig defaults): no channel attempts, so each send records a
           single "tray" row — isolating the Summary-vs-Per-event fan-out we assert on. */
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance, resolveOverride);
        return (deliverer, history);
    }

    private static AlertOutcome OutcomeWithIncidents(int n, string serverKey = "42")
    {
        var incidents = new List<AlertIncident>();
        for (int i = 0; i < n; i++)
        {
            incidents.Add(new AlertIncident($"key{i}", new[] { $"db.dbo.T{i}" }, OccurrenceCount: i + 1));
        }

        var context = new AlertContext();
        AlertIncidentRenderer.Apply(context, incidents);
        return new AlertOutcome(
            serverKey, "srv", "Deadlocks Detected", n.ToString(CultureInfo.InvariantCulture), "1",
            context, "detail", NumericCurrentValue: n, NumericThresholdValue: 1, Muted: false, Severity: null);
    }

    private sealed class RecordingHistoryStore : IAlertHistoryStore
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
}
