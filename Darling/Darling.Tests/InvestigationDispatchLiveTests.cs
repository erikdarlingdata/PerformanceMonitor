/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The investigation dispatch hook on a live store (#5632 Phase A, plan v2 section 4, rulings R1 and R4): the real deliverer and
/// finding sender write their rows through <see cref="PgAlertHistoryStore"/> and hand them to the real
/// <see cref="InvestigationRequestStore"/>. An engine alert with an agent route opens a request whose link rows name the exact
/// <c>config_alert_log</c> rows; a second alert of the family within 30 minutes joins it; a Per-event split links each incident;
/// a muted alert, a report and a recovery open none; a single finding page opens one and a digest finding none; and the sweep
/// journals a lapsed claim once and never reopens it.
/// </summary>
public sealed class InvestigationDispatchLiveTests
{
    private const int ServerId = 42;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private TimeSpan _shift = TimeSpan.Zero;

    private sealed record Harness(
        DarlingAlertDeliverer Deliverer, DarlingFindingAlertSender Sender, InvestigationRequestStore Store, NpgsqlConnection Connection);

    private static NotificationRoute AgentRoute(int routeId, string match) =>
        new(routeId, match, "", "", "", "", "", Enabled: true) { AgentLevel = NotificationRoute.AgentLevelInvestigate };

    private static AlertOutcome Outcome(string metric, bool muted = false, int incidents = 0)
    {
        AlertContext? context = null;
        if (incidents > 0)
        {
            context = new AlertContext();
            AlertIncidentRenderer.Apply(context, Enumerable.Range(0, incidents)
                .Select(i => new AlertIncident("key" + i, new[] { "db.dbo.T" + i }, OccurrenceCount: i + 1)).ToList());
        }

        return new AlertOutcome(ServerId.ToString(CultureInfo.InvariantCulture), "srv", metric, "3", "1", context, "detail",
            NumericCurrentValue: 3, NumericThresholdValue: 1, Muted: muted, Severity: null);
    }

    /* Every link row names one config_alert_log row exactly: same instant, server and metric. */
    private const string UnlinkedSql = @"
SELECT count(*) FROM config.investigation_request_alerts AS a
WHERE NOT EXISTS (SELECT 1 FROM config_alert_log AS l
                  WHERE l.alert_time = (a.alert_time AT TIME ZONE 'UTC')
                    AND l.server_id = a.server_id AND l.metric_name = a.metric_name)";

    [Fact]
    public async Task AnEngineAlert_OpensARequest_LinkedToItsRow_AndASecondOfTheFamily_JoinsIt()
    {
        await RunAsync(AlertNotificationMode.Summary, async (h, ct) =>
        {
            await h.Deliverer.DeliverAsync(Outcome("Deadlocks Detected", incidents: 2), ct);
            await h.Deliverer.DeliverAsync(Outcome("Blocking Detected"), ct);

            Assert.Equal("1|blocking|2|Deadlocks Detected|key0", await ScalarAsync(h.Connection, @"
SELECT count(*) OVER () || '|' || family || '|' || alert_count || '|' || first_metric || '|' || first_dedup_key
FROM config.investigation_requests", ct));
            /* Two keys for the first alert (one per incident), the server-and-metric key for the second. */
            Assert.Equal("Blocking Detected=42:Blocking Detected,Deadlocks Detected=key0,Deadlocks Detected=key1", await ScalarAsync(h.Connection, @"
SELECT string_agg(metric_name || '=' || dedup_key, ',' ORDER BY metric_name, dedup_key) FROM config.investigation_request_alerts", ct));
            Assert.Equal("0", await ScalarAsync(h.Connection, UnlinkedSql, ct));
            Assert.Equal("opened,joined", await ScalarAsync(h.Connection,
                "SELECT string_agg(event, ',' ORDER BY journal_id) FROM config.investigation_journal", ct));
        });
    }

    [Fact]
    public async Task APerEventSplit_LinksEachIncident_ToItsOwnRow()
    {
        await RunAsync(AlertNotificationMode.PerEvent, async (h, ct) =>
        {
            await h.Deliverer.DeliverAsync(Outcome("Deadlocks Detected", incidents: 3), ct);

            Assert.Equal("3", await ScalarAsync(h.Connection, "SELECT count(*) FROM config_alert_log", ct));
            Assert.Equal("1|3", await ScalarAsync(h.Connection,
                "SELECT count(*) || '|' || max(alert_count) FROM config.investigation_requests", ct));
            Assert.Equal("key0,key1,key2", await ScalarAsync(h.Connection,
                "SELECT string_agg(dedup_key, ',' ORDER BY dedup_key) FROM config.investigation_request_alerts", ct));
            Assert.Equal("0", await ScalarAsync(h.Connection, UnlinkedSql, ct));
        });
    }

    [Fact]
    public async Task AMutedAlert_AReport_AndARecovery_OpenNone()
    {
        await RunAsync(AlertNotificationMode.Summary, async (h, ct) =>
        {
            await h.Deliverer.DeliverAsync(Outcome("Deadlocks Detected", muted: true), ct);
            await h.Deliverer.DeliverAsync(Outcome("Collector Cost Digest"), ct);
            await h.Deliverer.DeliverAsync(Outcome("Server Restored"), ct);

            Assert.Equal("3|0|0", await ScalarAsync(h.Connection, @"
SELECT (SELECT count(*) FROM config_alert_log) || '|' || (SELECT count(*) FROM config.investigation_requests)
       || '|' || (SELECT count(*) FROM config.investigation_request_alerts)", ct));
        });
    }

    [Fact]
    public async Task ASingleFindingPage_OpensOne_AndADigestFinding_OpensNone()
    {
        await RunAsync(AlertNotificationMode.Summary, async (h, ct) =>
        {
            await h.Sender.SendFindingAlertAsync(InvestigationDispatcherTests.Finding("Analysis: blocking [aaaa1111]", FindingRoute.Digest, "42"));
            Assert.Equal("0", await ScalarAsync(h.Connection, "SELECT count(*) FROM config.investigation_requests", ct));

            await h.Sender.SendFindingAlertAsync(InvestigationDispatcherTests.Finding(
                "Analysis: blocking [bbbb2222]", FindingRoute.Page, "42", hash: "bbbb2222cccc3333dddd4444"));
            /* #5632 Phase A: the request records the finding's whole story path hash (a report copies it from there), not
               the eight characters its metric name holds. */
            Assert.Equal("1|blocking|Analysis: blocking [bbbb2222]|bbbb2222cccc3333dddd4444", await ScalarAsync(h.Connection,
                "SELECT count(*) OVER () || '|' || family || '|' || first_metric || '|' || story_path_hash FROM config.investigation_requests", ct));
            Assert.Equal("0", await ScalarAsync(h.Connection, UnlinkedSql, ct));
        });
    }

    [Fact]
    public async Task TheSweep_JournalsALapsedClaimOnce_AndNeverReopensIt()
    {
        await RunAsync(AlertNotificationMode.Summary, async (h, ct) =>
        {
            await h.Deliverer.DeliverAsync(Outcome("Deadlocks Detected"), ct);
            var id = Guid.Parse((await ScalarAsync(h.Connection, "SELECT request_id FROM config.investigation_requests", ct))!);
            var claim = await h.Store.ClaimAsync(id, "agent-a", ct);
            Assert.Equal(InvestigationClaimOutcome.Claimed, claim.Outcome);

            /* Past the 15-minute lease, well inside the 240-minute expiry. */
            _shift = TimeSpan.FromMinutes(20);
            Assert.Equal((1, 0), ToPair(await h.Store.SweepAsync(ct)));
            Assert.Equal((0, 0), ToPair(await h.Store.SweepAsync(ct)));

            /* Still the lapsed claim's row: the sweep reopened nothing. */
            Assert.Equal($"claimed|{claim.ClaimId}|1|1", await ScalarAsync(h.Connection, @"
SELECT r.state || '|' || r.claim_id || '|' || r.claim_count || '|'
       || (SELECT count(*) FROM config.investigation_journal WHERE event = 'lease_expired')
FROM config.investigation_requests AS r", ct));
        });
    }

    /// <summary>
    /// #5821 review round 1: the request id leaves in the generic webhook before the alert's history row is written, and a
    /// runner may claim while the POST is in flight. The brief composed then reads its alerts section through
    /// <see cref="PgInvestigationBriefSourceReader"/>; it must carry the alert's value, threshold, detail text and context from
    /// the link rows, though the history row does not exist yet. Before V188 the four were null there.
    /// </summary>
    [Fact]
    public async Task ARunnerThatReadsTheBriefDuringThePost_FindsTheAlertsValues_BeforeItsHistoryRowExists()
    {
        string? storeString = null;
        InvestigationBriefSources? during = null;
        long rowsDuring = -1;
        Exception? failure = null;
        using var generic = new CapturingWebhookEndpoint(onRequest: async body =>
        {
            try
            {
                var id = Guid.Parse(JsonDocument.Parse(body).RootElement
                    .GetProperty("investigation").GetProperty("request_id").GetString()!);
                await using var source = NpgsqlDataSource.Create(storeString!);
                during = await new PgInvestigationBriefSourceReader(source).ReadAsync(id);
                await using var command = source.CreateCommand("SELECT count(*) FROM config_alert_log");
                rowsDuring = (long)(await command.ExecuteScalarAsync())!;
            }
            catch (Exception ex)
            {
                failure = ex;
            }
        });

        await RunAsync(AlertNotificationMode.Summary, async (h, ct) =>
        {
            await h.Deliverer.DeliverAsync(Outcome("Deadlocks Detected", incidents: 1), ct);

            Assert.Null(failure);
            Assert.Single(generic.Bodies);
            /* The window is real: the post was answered before the history row was written. */
            Assert.Equal(0, rowsDuring);
            var alert = Assert.Single(during!.Alerts);
            Assert.Equal("Deadlocks Detected", alert.MetricName);
            Assert.Equal(3, alert.CurrentValue);
            Assert.Equal(1, alert.ThresholdValue);
            Assert.False(alert.Muted);
            Assert.Contains("detail", alert.DetailText, StringComparison.Ordinal);
            Assert.NotEmpty(alert.Context);
            Assert.Equal("1", await ScalarAsync(h.Connection, "SELECT count(*) FROM config_alert_log", ct));
            Assert.Equal("0", await ScalarAsync(h.Connection, UnlinkedSql, ct));
        }, (config, connectionString) =>
        {
            storeString = connectionString;
            config.Webhooks.GenericUrl = generic.Url;
            config.Webhooks.GenericBodyTemplate = "{\"investigation\": {{investigation_json}}}";
        });
    }

    private static (int, int) ToPair(InvestigationSweepResult result) => (result.LeasesLapsed, result.Expired);

    private async Task RunAsync(
        AlertNotificationMode mode, Func<Harness, CancellationToken, Task> body, Action<DarlingConfig, string>? configure = null)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the investigation dispatch live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var storeString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var connection = new NpgsqlConnection(storeString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var historySource = NpgsqlDataSource.Create(storeString);
        await using var investigationSource = NpgsqlDataSource.Create(InvestigationDispatcher.WithLockTimeout(storeString));
        var store = new InvestigationRequestStore(investigationSource, 30, () => DateTime.UtcNow + _shift);

        var config = new DarlingConfig();
        config.Alerts.DeliveryMode = mode;
        config.NotificationRoutes = new[] { AgentRoute(1, AlertFamily.Performance), AgentRoute(2, AlertFamily.Reports) };
        configure?.Invoke(config, storeString);
        var settings = new DarlingAlertSettings(config);
        var history = new PgAlertHistoryStore(historySource, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds);
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var dispatcher = new InvestigationDispatcher(store, settings, NullLogger.Instance);

        await body(new Harness(
            new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance, investigationDispatcher: dispatcher),
            new DarlingFindingAlertSender(settings, history, webhooks, NullLogger.Instance, dispatcher),
            store,
            connection), ct);
    }

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }
}
