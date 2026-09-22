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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3916 PR B, live against PG: the summary over the cap is a real delivery for every story it names.
/// Driven through the REAL Darling path — <see cref="DarlingFindingAlertSender"/> over a real
/// <see cref="PgAlertHistoryStore"/>, a loopback webhook for "delivered" and no channel at all for
/// "undelivered" — so the rows are the ones production writes, not hand-built ones.
/// <list type="bullet">
/// <item>Pin 1 (rows half): six pages over the cap → ONE message on the wire, SIX rows, each carrying the
/// summary's delivery under its own metric name.</item>
/// <item>Pin 3: every named story is held on the next pass AND after a restart — a NEW service over the same
/// store seeds from the now-delivered rows (<see cref="IAlertHistoryStore.GetLastDeliveredPageUtcAsync"/>).</item>
/// <item>Pin 8: an undelivered summary (no channel) stamps its members Delivered=false — cooldown-only: not
/// re-attempted inside the cooldown, re-attempted past it, never held.</item>
/// </list>
/// </summary>
[Collection("live-postgres")]
public sealed class AnalysisPageCapLiveTests
{
    private const int ServerA = -391610;
    private const int ServerB = -391611;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static AnalysisFinding Page(int serverId, string hash) => new()
    {
        ServerId = serverId,
        ServerName = $"page-cap-{-serverId}",
        Category = "anomaly",
        StoryPath = "ANOMALY_CPU_SPIKE → PLAN_REGRESSION",
        StoryPathHash = hash,
        IncidentId = "inc-" + hash,
        Severity = 1.6,
        Confidence = StoryConfidence.Compute(1, 5, 2),
        FactCount = 2,
        MatchedAmplifiers = 1,
        DefinedAmplifiers = 5,
        RootFactKey = "ANOMALY_CPU_SPIKE",
        RootFactValue = 1.0,
        AnalysisTime = DateTime.UtcNow,
    };

    /// <summary>Six fresh pages across two servers — one over the Darling cap of 5.</summary>
    private static List<AnalysisFinding> SixPages(string salt) =>
        Enumerable.Range(0, 6).Select(i => Page(i % 2 == 0 ? ServerA : ServerB, $"{salt}{i:x2}cap3916b")).ToList();

    private static async Task PassAsync(AnalysisNotificationService service, IEnumerable<AnalysisFinding> pages)
    {
        foreach (var server in pages.GroupBy(p => p.ServerId))
            await service.NotifyAsync(server.ToList());
        await service.FlushPendingAsync();
    }

    private static DarlingFindingAlertSender Sender(DarlingAlertSettings settings, PgAlertHistoryStore store) =>
        new(settings, store,
            new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance),
            NullLogger.Instance);

    private static AnalysisNotificationService Service(DarlingAlertSettings settings, PgAlertHistoryStore store) =>
        new(Sender(settings, store), settings, f => f.ServerId.ToString(System.Globalization.CultureInfo.InvariantCulture),
            NullLogger<AnalysisNotificationService>.Instance);

    private static async Task<(long Rows, long Sent, long Distinct)> RowsAsync(NpgsqlConnection connection)
    {
        using var count = new NpgsqlCommand(
            $"SELECT count(*), count(*) FILTER (WHERE alert_sent), count(DISTINCT metric_name) FROM config_alert_log WHERE server_id IN ({ServerA}, {ServerB});",
            connection);
        await using var reader = await count.ExecuteReaderAsync(Ct);
        await reader.ReadAsync(Ct);
        return (reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2));
    }

    /// <summary>Same reflection seam as <c>HoldEarnedByDeliveryTests.AgeBuckets</c>: the service reads
    /// DateTime.UtcNow and has no clock seam, so a live-stamped bucket is moved past the cooldown by hand.</summary>
    private static List<bool> AgeBuckets(AnalysisNotificationService service, TimeSpan by)
    {
        var field = typeof(AnalysisNotificationService).GetField("_cooldowns",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        var dict = (System.Collections.IDictionary)field.GetValue(service)!;
        var delivered = new List<bool>();
        foreach (var key in new List<object>(dict.Keys.Cast<object>()))
        {
            var state = dict[key]!;
            var t = state.GetType();
            var last = (DateTime)t.GetProperty("LastNotified")!.GetValue(state)!;
            var severity = (double)t.GetProperty("LastNotifiedSeverity")!.GetValue(state)!;
            var wasDelivered = (bool)t.GetProperty("Delivered")!.GetValue(state)!;
            delivered.Add(wasDelivered);
            dict[key] = Activator.CreateInstance(t, last - by, severity, DateTime.UtcNow, wasDelivered);
        }
        return delivered;
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM config_alert_log WHERE server_id IN ({ServerA}, {ServerB}); DELETE FROM analysis_muted WHERE server_id IN ({ServerA}, {ServerB});",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task RunLiveAsync(Func<PgAlertHistoryStore, NpgsqlConnection, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live page-cap test.");

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
    /// Pins 1 (rows) and 3: one summary on the wire, six delivered rows under six metric names; the next pass
    /// on the same service sends nothing, and a NEW service over the same store (the restart) sends nothing —
    /// its seed read the delivered rows. Negative control: the seed is non-null for every named story, so
    /// the restart's silence is the seed's doing, not an empty pass.
    /// </summary>
    [Fact]
    public async Task ASummaryNamedStory_IsHeldOnTheNextPass_AndAfterARestart()
    {
        await RunLiveAsync(async (store, connection) =>
        {
            using var endpoint = new CapturingWebhookEndpoint();
            var config = new DarlingConfig();
            config.Webhooks.GenericUrl = endpoint.Url;
            var settings = new DarlingAlertSettings(config);
            Assert.Equal(5, settings.AnalysisPageCap);

            var pages = SixPages("a1");
            using (var first = Service(settings, store))
            {
                await PassAsync(first, pages);
                Assert.Single(endpoint.Bodies);                       /* ONE message for six pages */
                Assert.Equal((6L, 6L, 6L), await RowsAsync(connection)); /* six rows, all delivered, six names */

                await PassAsync(first, pages);                        /* the next pass: held */
                Assert.Single(endpoint.Bodies);
                Assert.Equal((6L, 6L, 6L), await RowsAsync(connection));
            }

            foreach (var page in pages)
                Assert.NotNull(await store.GetLastDeliveredPageUtcAsync(
                    page.ServerId.ToString(System.Globalization.CultureInfo.InvariantCulture), FindingMessageFormatter.MetricName(page)));

            using (var restarted = Service(settings, store))            /* the restart: a new service, same store */
            {
                await PassAsync(restarted, pages);
                Assert.Single(endpoint.Bodies);
                Assert.Equal((6L, 6L, 6L), await RowsAsync(connection));
            }
        });
    }

    /// <summary>
    /// Pin 8: with no channel the summary reaches no one — six rows, none sent, every member stamped
    /// Delivered=false, no delivered-page seed. Inside the cooldown the next pass is throttled (one row per
    /// cooldown, not per pass); past it every member re-attempts (another summary: six more rows), because
    /// the hold was never earned.
    /// </summary>
    [Fact]
    public async Task AnUndeliveredSummary_IsCooldownOnly_ReAttemptsPastTheCooldown()
    {
        await RunLiveAsync(async (store, connection) =>
        {
            var settings = new DarlingAlertSettings(new DarlingConfig());
            var pages = SixPages("b2");
            using var service = Service(settings, store);

            await PassAsync(service, pages);
            Assert.Equal((6L, 0L, 6L), await RowsAsync(connection));
            foreach (var page in pages)
                Assert.Null(await store.GetLastDeliveredPageUtcAsync(
                    page.ServerId.ToString(System.Globalization.CultureInfo.InvariantCulture), FindingMessageFormatter.MetricName(page)));

            await PassAsync(service, pages);                          /* inside the cooldown: throttled */
            Assert.Equal((6L, 0L, 6L), await RowsAsync(connection));

            var stamped = AgeBuckets(service, TimeSpan.FromMinutes(settings.AnalysisNotifyCooldownMinutes + 1));
            Assert.Equal(6, stamped.Count);
            Assert.All(stamped, d => Assert.False(d));                /* members stamped Delivered=false */

            await PassAsync(service, pages);                          /* past it: re-attempted, not held */
            Assert.Equal((12L, 0L, 6L), await RowsAsync(connection));
        });
    }

    /// <summary>
    /// The mute seam's Darling read, live: the flush's re-check (DarlingWorker's <c>isStoryMuted</c>) reads
    /// <see cref="PgFindingStore.GetMutedStoryHashesAsync"/>, which sees a mute written after the page was
    /// queued — driven through the service, the queued page is dropped unsent and unrecorded. The read
    /// fails OPEN on an unreachable store: an empty set, so the page is delivered, never suppressed.
    /// </summary>
    [Fact]
    public async Task TheFlushMuteRead_SeesAMuteWrittenInsideTheWindow_AndFailsOpen()
    {
        await RunLiveAsync(async (store, connection) =>
        {
            var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG")!;
            await using var postgres = NpgsqlDataSource.Create(connectionString);
            var findings = new PgFindingStore(postgres);
            var page = Page(ServerA, "c3d4e5f6mute3916");

            using var endpoint = new CapturingWebhookEndpoint();
            var config = new DarlingConfig();
            config.Webhooks.GenericUrl = endpoint.Url;
            var settings = new DarlingAlertSettings(config);
            using var service = new AnalysisNotificationService(
                Sender(settings, store), settings, f => f.ServerId.ToString(System.Globalization.CultureInfo.InvariantCulture),
                NullLogger<AnalysisNotificationService>.Instance,
                isStoryMuted: async (serverId, hash) => (await findings.GetMutedStoryHashesAsync(serverId)).Contains(hash));

            Assert.Empty(await findings.GetMutedStoryHashesAsync(ServerA, Ct));
            await service.NotifyAsync(new[] { page });                /* queued */
            await findings.MuteStoryAsync(ServerA, page.StoryPathHash!, page.StoryPath, "3916 pin");
            Assert.Contains(page.StoryPathHash!, await findings.GetMutedStoryHashesAsync(ServerA, Ct));

            await service.FlushPendingAsync();
            Assert.Empty(endpoint.Bodies);                             /* dropped at the flush */
            Assert.Equal((0L, 0L, 0L), await RowsAsync(connection));   /* and unrecorded */

            /* Fail-open: nothing listens on port 1. */
            await using var dead = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Password=x;Database=x;Timeout=2");
            Assert.Empty(await new PgFindingStore(dead).GetMutedStoryHashesAsync(ServerA, Ct));
        });
    }
}
