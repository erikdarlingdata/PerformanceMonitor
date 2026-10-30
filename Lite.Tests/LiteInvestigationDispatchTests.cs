/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5820 (slice L3): Lite's alerts open investigation requests through the shared <see cref="InvestigationDispatchCore"/>,
/// the port of Darling's <c>InvestigationDispatcherTests</c> (same fact names) against <see cref="EmailAlertService"/>, plus
/// the store facts against a throwaway DuckDB (<see cref="DuckDbInvestigationStore"/>), the 60-second sweep
/// (<see cref="InvestigationSweepService"/>) and the generic webhook's <c>{{investigation_json}}</c>.
/// </summary>
public sealed class LiteInvestigationDispatchTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static NotificationRoute AgentRoute(int routeId, string match, int? level = NotificationRoute.AgentLevelInvestigate) =>
        new(routeId, match, "", "", "", "", "", Enabled: true) { AgentLevel = level, AgentMaxPerHour = 7, AgentMaxPerDay = 70 };

    private static AlertContext? Incidents(int count)
    {
        if (count == 0)
        {
            return null;
        }

        var context = new AlertContext();
        AlertIncidentRenderer.Apply(context, Enumerable.Range(0, count)
            .Select(i => new AlertIncident("key" + i, new[] { "db.dbo.T" + i }, OccurrenceCount: i + 1)).ToList());
        return context;
    }

    private static (EmailAlertService Email, AlertWebhookCancelTests.CapturingHistoryStore History, List<InvestigationAlert> Asked) Build(
        RouteSettings settings, Func<InvestigationAlert, CancellationToken, Task<InvestigationOpenResult>>? store = null,
        Action<AlertWebhookCancelTests.CapturingHistoryStore>? onHistory = null)
    {
        var history = new AlertWebhookCancelTests.CapturingHistoryStore();
        onHistory?.Invoke(history);
        var asked = new List<InvestigationAlert>();
        var core = new InvestigationDispatchCore(
            (alert, token) =>
            {
                asked.Add(alert);
                return store is null
                    ? Task.FromResult(new InvestigationOpenResult(InvestigationOpenOutcome.Opened, Guid.NewGuid()))
                    : store(alert, token);
            },
            settings, NullLogger.Instance);
        var webhooks = new WebhookAlertService(settings, EmailAlertService.Branding, NullLogger<WebhookAlertService>.Instance, history);
        return (new EmailAlertService(settings, history, webhooks, NullLogger<EmailAlertService>.Instance, core), history, asked);
    }

    private static Task<AlertDelivery?> SendAsync(EmailAlertService email, string metric, bool muted = false, int incidents = 0) =>
        email.TrySendAlertEmailAsync(metric, "srv", "3", "1", 42, Incidents(incidents),
            numericCurrentValue: 3, numericThresholdValue: 1, muted: muted, detailText: "detail", cancellationToken: Ct);

    [Fact]
    public async Task AnEngineAlert_WithAnAgentRoute_IsHandedToTheStore_WithItsRowsTimeFamilyRouteAndEveryIncidentKey()
    {
        var (email, history, asked) = Build(new RouteSettings(AgentRoute(5, "Deadlocks Detected")));

        Assert.NotNull(await SendAsync(email, "Deadlocks Detected", incidents: 3));

        var record = Assert.Single(history.Records);
        var alert = Assert.Single(asked);
        Assert.NotNull(record.AlertTimeUtc);
        Assert.Equal(record.AlertTimeUtc, alert.AlertTimeUtc);
        Assert.Equal(0, record.AlertTimeUtc!.Value.Ticks % 10);
        Assert.Equal((5, 42, "blocking", "Deadlocks Detected"), (alert.RouteId, alert.ServerId, alert.Family, alert.MetricName));
        Assert.Equal((7, 70), (alert.MaxPerHour, alert.MaxPerDay));
        /* The PagerDuty key is the first incident's, then every incident's own key: one link per incident. */
        Assert.Equal(new[] { "key0", "key1", "key2" }, alert.DedupKeys);
        Assert.Null(alert.StoryPathHash);
    }

    [Fact]
    public async Task APerEventSplit_HandsEachIncidentRowToTheStore()
    {
        var (email, history, asked) = Build(new RouteSettings(AgentRoute(5, "Blocking Detected")));
        /* The real per-event split (LiteAlertDeliverer), sending through the real email service. */
        var deliverer = new LiteAlertDeliverer(
            (_, _, _, _, _) => { },
            (metric, server, current, threshold, serverId, context, numericCurrent, numericThreshold, muted, detail, mode, token) =>
                email.TrySendAlertEmailAsync(metric, server, current, threshold, serverId, context,
                    numericCurrentValue: numericCurrent, numericThresholdValue: numericThreshold, muted: muted,
                    detailText: detail, deliveryMode: mode, cancellationToken: token),
            _ => AlertNotificationMode.PerEvent);

        await deliverer.DeliverAsync(
            new AlertOutcome("42", "srv", "Blocking Detected", "3", "1", Incidents(3), "detail", 3, 1, false, null), Ct);

        Assert.Equal(3, history.Records.Count);
        Assert.Equal(new[] { "key0", "key1", "key2" }, asked.Select(a => Assert.Single(a.DedupKeys)).ToArray());
        Assert.Equal(history.Records.Select(r => r.AlertTimeUtc), asked.Select(a => (DateTime?)a.AlertTimeUtc));
    }

    [Theory]
    [InlineData("Deadlocks Detected", true)] // muted
    [InlineData("Collector Cost Digest", false)] // a report
    [InlineData("Server Restored", false)] // a recovery
    [InlineData("High CPU", false)] // no route sends it to an agent
    public async Task AMutedRow_AReport_ARecovery_AndAnUnroutedAlert_NeverReachTheStore(string metric, bool muted)
    {
        /* The reports and performance families are routed to an agent, so a report and a recovery are refused by the family
           map, not by a missing route. */
        var (email, history, asked) = Build(new RouteSettings(
            AgentRoute(1, "Deadlocks Detected"), AgentRoute(2, AlertFamily.Reports), AgentRoute(3, "Server Restored")));

        await SendAsync(email, metric, muted);

        Assert.Single(history.Records);
        Assert.Empty(asked);
    }

    [Fact]
    public async Task AnAgentLevelOfZero_OnAMoreSpecificRoute_StopsTheLookup()
    {
        var (email, _, asked) = Build(new RouteSettings(
            AgentRoute(1, AlertFamily.Performance), AgentRoute(2, "Deadlocks Detected", NotificationRoute.AgentLevelOff)));

        await SendAsync(email, "Deadlocks Detected");

        Assert.Empty(asked);
    }

    [Fact]
    public async Task AStoreFailure_NeverFailsTheDelivery()
    {
        var (email, history, asked) = Build(new RouteSettings(AgentRoute(5, "Deadlocks Detected")),
            (_, _) => throw new InvalidOperationException("store down"));

        var delivery = await SendAsync(email, "Deadlocks Detected");

        /* The row was written and the delivery reported; a throw would have turned the answer null. */
        Assert.NotNull(delivery);
        Assert.Single(history.Records);
        Assert.Single(asked);
    }

    [Fact]
    public async Task ACancelledDispatch_ReturnsNull_AndDoesNotThrow()
    {
        var settings = new RouteSettings(AgentRoute(5, "Deadlocks Detected"));
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var core = new InvestigationDispatchCore(
            (_, token) => Task.FromException<InvestigationOpenResult>(new OperationCanceledException(token)),
            settings, NullLogger.Instance);
        var record = new AlertHistoryRecord("42", "srv", "Deadlocks Detected", "3", "1", 3, 1,
            AlertDelivery.RoutedToDigest(), false, null, null);

        Assert.Null(await core.DispatchAsync(record, null, cancellationToken: cts.Token));

        /* Lite's half: a cancel the store throws that the caller never asked for is not the send's failure either. The
           email service's own catch would have turned it into a null delivery. */
        var (email, history, _) = Build(settings, (_, _) => throw new OperationCanceledException());
        Assert.NotNull(await SendAsync(email, "Deadlocks Detected"));
        Assert.Single(history.Records);
    }

    [Fact]
    public async Task TheFindingSender_HandsASinglePageAndEachHeldPage_ButNeverADigestFinding()
    {
        var (email, history, asked) = Build(new RouteSettings(AgentRoute(5, AlertFamily.Performance)));

        await email.SendFindingAlertAsync(Finding("Analysis: blocking [aaaa1111]", FindingRoute.Digest));
        Assert.Empty(asked);

        await email.SendFindingAlertAsync(Finding("Analysis: blocking [bbbb2222]", FindingRoute.Page));
        Assert.Equal("blocking", Assert.Single(asked).Family);

        await email.SendFindingSummaryAsync(new[] { Finding("Analysis: blocking [cccc3333]"), Finding("Analysis: memory [dddd4444]") });
        Assert.Equal(3, asked.Count);
        Assert.Equal(history.Records.Skip(1).Select(r => r.AlertTimeUtc), asked.Select(a => (DateTime?)a.AlertTimeUtc));
    }

    /// <summary>The metric name holds eight characters of the story path hash; the request records the whole hash the
    /// finding alert carries, on a single page and on each held page a summary names.</summary>
    [Fact]
    public async Task AFindingPage_HandsTheStoreItsFullStoryPathHash_NotTheMetricNamesEightCharacters()
    {
        var (email, _, asked) = Build(new RouteSettings(AgentRoute(5, AlertFamily.Performance)));

        await email.SendFindingAlertAsync(Finding("Analysis: blocking [bbbb2222]", hash: "bbbb2222cccc3333dddd4444"));
        await email.SendFindingSummaryAsync(new[]
        {
            Finding("Analysis: memory [dddd4444]", hash: "dddd4444eeee5555ffff6666"),
            Finding("Analysis: blocking [aaaa1111]"),
        });

        Assert.Equal(new string?[] { "bbbb2222cccc3333dddd4444", "dddd4444eeee5555ffff6666", null }, asked.Select(a => a.StoryPathHash));
    }

    /// <summary>The 60-second sweep, through <see cref="InvestigationSweepService"/> over the real store: a claim past its
    /// lease is journaled once, the next pass journals nothing, the claim's row is left as it was (never reopened), and only
    /// the pass that swept something logs.</summary>
    [Fact]
    public async Task TheSweep_JournalsALapsedClaimOnce_AndNeverReopensIt()
    {
        using var duck = new ThrowawayDuckDb();
        var settings = new RouteSettings(AgentRoute(5, "Deadlocks Detected"));
        var email = duck.EmailService(settings);

        await SendAsync(email, "Deadlocks Detected");
        var id = Guid.Parse(duck.Scalar("SELECT request_id FROM investigation_requests")!);
        var claim = await duck.Store.ClaimAsync(id, "agent-a", Ct);
        Assert.Equal(InvestigationClaimOutcome.Claimed, claim.Outcome);

        /* Past the 15-minute lease, well inside the 240-minute expiry. */
        duck.Shift = TimeSpan.FromMinutes(20);
        var log = new CountingLogger();
        await RunOnePassAsync(duck.Store, log);
        Assert.Equal("1", duck.Scalar("SELECT count(*) FROM investigation_journal WHERE event = 'lease_expired'"));
        Assert.Equal(1, log.Informations);

        await RunOnePassAsync(duck.Store, log);
        Assert.Equal(1, log.Informations);

        /* Still the lapsed claim's row: the sweep reopened nothing. */
        Assert.Equal($"claimed|{claim.ClaimId}|1|1", duck.Scalar(@"
SELECT concat_ws('|', r.state, r.claim_id::VARCHAR, r.claim_count,
       (SELECT count(*) FROM investigation_journal WHERE event = 'lease_expired'))
FROM investigation_requests AS r"));
        Assert.Equal(0, log.Warnings);
    }

    [Fact]
    public async Task AMutedAlert_AndARecovery_OpenNothing()
    {
        using var duck = new ThrowawayDuckDb();
        var settings = new RouteSettings(AgentRoute(1, AlertFamily.Performance), AgentRoute(2, "Server Restored"));
        var email = duck.EmailService(settings);

        await SendAsync(email, "Blocking Detected", muted: true);
        await SendAsync(email, "Server Restored");

        Assert.Equal("2|0", duck.Scalar(
            "SELECT concat_ws('|', (SELECT count(*) FROM config_alert_log), (SELECT count(*) FROM investigation_requests))"));

        /* The same route opens a request for an alert that is neither, so the two above were refused for what they are. */
        await SendAsync(email, "Blocking Detected");
        Assert.Equal("3|1", duck.Scalar(
            "SELECT concat_ws('|', (SELECT count(*) FROM config_alert_log), (SELECT count(*) FROM investigation_requests))"));
    }

    /// <summary>#5821's token in Lite: a generic body template that places <c>{{investigation_json}}</c> posts the request the
    /// alert opened, read back from the store, and the history row and the request share one instant.</summary>
    [Fact]
    public async Task AGenericTemplateWithTheToken_PostsTheRequestTheAlertOpened()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        using var duck = new ThrowawayDuckDb();
        var settings = new RouteSettings(AgentRoute(5, "Deadlocks Detected"))
        {
            GenericUrl = endpoint.Url,
            Template = "{\"metric\": \"{{metric}}\", \"investigation\": {{investigation_json}}}",
        };
        var email = duck.EmailService(settings);

        Assert.NotNull(await SendAsync(email, "Deadlocks Detected"));

        var body = Assert.Single(endpoint.Bodies);
        using var json = JsonDocument.Parse(body);
        var investigation = json.RootElement.GetProperty("investigation");
        var stored = duck.Scalar("SELECT request_id::VARCHAR FROM investigation_requests");
        Assert.NotNull(stored);
        Assert.Equal(Guid.Parse(stored!), Guid.Parse(investigation.GetProperty("request_id").GetString()!));
        Assert.Equal("1", duck.Scalar(
            "SELECT count(*) FROM config_alert_log AS l JOIN investigation_request_alerts AS a ON a.alert_time = l.alert_time"));
    }

    /// <summary>The template without the token posts no investigation value at all, and the alert still opens its request,
    /// before the post, as it does with the token.</summary>
    [Fact]
    public async Task AGenericTemplateWithoutTheToken_PostsNoTokenValue_AndStillOpensTheRequest()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        using var duck = new ThrowawayDuckDb();
        var settings = new RouteSettings(AgentRoute(5, "Deadlocks Detected"))
        {
            GenericUrl = endpoint.Url,
            Template = "{\"metric\": \"{{metric}}\", \"detail\": \"{{detail}}\"}",
        };
        var email = duck.EmailService(settings);

        Assert.NotNull(await SendAsync(email, "Deadlocks Detected"));

        var body = Assert.Single(endpoint.Bodies);
        var stored = duck.Scalar("SELECT request_id::VARCHAR FROM investigation_requests");
        Assert.NotNull(stored);
        Assert.DoesNotContain("request_id", body, StringComparison.Ordinal);
        Assert.DoesNotContain(stored!, body, StringComparison.OrdinalIgnoreCase);
        using var json = JsonDocument.Parse(body);
        Assert.False(json.RootElement.TryGetProperty("investigation", out _));
    }

    /// <summary>One path since #5821: every alert opens its request ONCE, BEFORE the channels post and before its history row
    /// is written, whether or not the generic template names the token, and the row and the request share one instant.</summary>
    [Theory]
    [InlineData("{\"metric\": \"{{metric}}\", \"investigation\": {{investigation_json}}}")]
    [InlineData("{\"metric\": \"{{metric}}\"}")]
    public async Task EveryAlert_OpensItsRequestOnce_BeforeThePostAndBeforeItsRow(string template)
    {
        using var endpoint = new CapturingWebhookEndpoint();
        var settings = new RouteSettings(AgentRoute(5, "Deadlocks Detected")) { GenericUrl = endpoint.Url, Template = template };
        var seen = new List<(int Posted, int Rows)>();
        AlertWebhookCancelTests.CapturingHistoryStore? historyRef = null;
        var (email, history, asked) = Build(settings, (alert, _) =>
        {
            seen.Add((endpoint.Bodies.Count, historyRef!.Records.Count));
            return Task.FromResult(new InvestigationOpenResult(InvestigationOpenOutcome.Opened, Guid.NewGuid()));
        }, h => historyRef = h);

        Assert.NotNull(await SendAsync(email, "Deadlocks Detected"));

        Assert.Equal((0, 0), Assert.Single(seen));
        Assert.Single(endpoint.Bodies);
        Assert.Equal(Assert.Single(history.Records).AlertTimeUtc, Assert.Single(asked).AlertTimeUtc);
    }

    /// <summary>A post that fails leaves the request open: it was opened before the post, and nothing undoes it.</summary>
    [Fact]
    public async Task AFailedPost_StillLeavesTheRequestOpen()
    {
        var settings = new RouteSettings(AgentRoute(5, "Deadlocks Detected"))
        {
            GenericUrl = "http://127.0.0.1:1/",
            Template = "{\"investigation\": {{investigation_json}}}",
        };
        var (email, history, asked) = Build(settings);

        Assert.NotNull(await SendAsync(email, "Deadlocks Detected"));

        Assert.Single(asked);
        Assert.Single(history.Records);
        Assert.Equal(history.Records[0].AlertTimeUtc, asked[0].AlertTimeUtc);
    }

    /// <summary>The held-pages summary opens one request per page, each before the one message posts and each at the instant
    /// of the row it names.</summary>
    [Fact]
    public async Task TheFindingSummary_OpensOneRequestPerHeldPage_BeforeItsMessagePosts()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        var settings = new RouteSettings(AgentRoute(5, AlertFamily.Performance)) { GenericUrl = endpoint.Url, Template = "{\"m\": \"{{metric}}\"}" };
        var seen = new List<(int Posted, int Rows)>();
        AlertWebhookCancelTests.CapturingHistoryStore? historyRef = null;
        var (email, history, asked) = Build(settings, (alert, _) =>
        {
            seen.Add((endpoint.Bodies.Count, historyRef!.Records.Count));
            return Task.FromResult(new InvestigationOpenResult(InvestigationOpenOutcome.Opened, Guid.NewGuid()));
        }, h => historyRef = h);

        await email.SendFindingSummaryAsync(new[] { Finding("Analysis: blocking [cccc3333]"), Finding("Analysis: memory [dddd4444]") });

        Assert.Equal(new[] { (0, 0), (0, 0) }, seen);
        Assert.Single(endpoint.Bodies);
        Assert.Equal(history.Records.Select(r => r.AlertTimeUtc), asked.Select(a => (DateTime?)a.AlertTimeUtc));
    }

    private static async Task RunOnePassAsync(IInvestigationRequestStore store, ILogger log)
    {
        using var stop = new CancellationTokenSource();
        var sweep = new InvestigationSweepService(store, log, (_, _) =>
        {
            stop.Cancel();
            return Task.FromCanceled(stop.Token);
        });
        await sweep.RunAsync(stop.Token);
    }

    /// <summary>#6596, through the shared core and Lite's email service over a throwaway DuckDB: a finding page whose story is
    /// recurring at this hour opens no request, on a single page and on a held page a summary names. Its history row is still
    /// written, and the skip is journaled as <c>recurring_skipped</c> with no request id, once per route, server, metric and
    /// hour. The same page without the label opens a request as before.</summary>
    [Fact]
    public async Task ARecurringAtThisHourFindingPage_OpensNoRequest_ButItsRowIsWritten_AndTheSkipIsJournaled()
    {
        using var duck = new ThrowawayDuckDb();
        var email = duck.EmailService(new RouteSettings(AgentRoute(5, AlertFamily.Performance)));

        Assert.NotNull(await email.SendFindingAlertAsync(Finding("Analysis: anomaly [aaaa1111]", hash: "aaaa1111bbbb", recurring: true)));
        await email.SendFindingAlertAsync(Finding("Analysis: anomaly [aaaa1111]", hash: "aaaa1111bbbb", recurring: true));
        await email.SendFindingSummaryAsync(new[] { Finding("Analysis: anomaly [cccc3333]", hash: "cccc3333dddd", recurring: true) });

        Assert.Equal("0", duck.Scalar("SELECT count(*) FROM investigation_requests"));
        Assert.Equal("3", duck.Scalar("SELECT count(*) FROM config_alert_log"));
        Assert.Equal("2|0|5|43", duck.Scalar(
            "SELECT concat_ws('|', count(*), count(request_id), max(route_id), max(server_id)) FROM investigation_journal WHERE event = 'recurring_skipped'"));
        Assert.Equal("Analysis: anomaly [aaaa1111]|Analysis: anomaly [cccc3333]", duck.Scalar(
            "SELECT string_agg(reason, '|' ORDER BY reason) FROM investigation_journal WHERE event = 'recurring_skipped'"));
        Assert.Contains("aaaa1111bbbb", duck.Scalar(
            "SELECT detail FROM investigation_journal WHERE event = 'recurring_skipped' AND reason = 'Analysis: anomaly [aaaa1111]'"));

        await email.SendFindingAlertAsync(Finding("Analysis: anomaly [eeee5555]", hash: "eeee5555ffff"));
        Assert.Equal("1", duck.Scalar("SELECT count(*) FROM investigation_requests"));
    }

    /// <summary>#6596 at the shared core: the label holds back only the store's open-or-join and calls the skip journal with
    /// the alert it would have opened; without the label the store is asked and the journal is not called.</summary>
    [Fact]
    public async Task TheCore_HoldsBackARecurringAlertsRequest_AndJournalsTheSkip_AndAsksTheStoreWithoutTheLabel()
    {
        var settings = new RouteSettings(AgentRoute(5, AlertFamily.Performance));
        var asked = new List<InvestigationAlert>();
        var skipped = new List<InvestigationAlert>();
        var core = new InvestigationDispatchCore(
            (alert, _) =>
            {
                asked.Add(alert);
                return Task.FromResult(new InvestigationOpenResult(InvestigationOpenOutcome.Opened, Guid.NewGuid()));
            },
            settings, NullLogger.Instance,
            recurringSkipped: (alert, _) =>
            {
                skipped.Add(alert);
                return Task.CompletedTask;
            });
        var at = new DateTime(2026, 10, 23, 1, 5, 0, DateTimeKind.Utc);

        Assert.Null(await core.OpenOrJoinAsync("42", "srv", "Analysis: anomaly [aaaa1111]", false, at, null, "aaaa1111bbbb", null, Ct,
            recurringAtThisHour: true));
        Assert.Empty(asked);
        var skip = Assert.Single(skipped);
        Assert.Equal((5, 42, "aaaa1111bbbb", at), (skip.RouteId, skip.ServerId, skip.StoryPathHash, skip.AlertTimeUtc));

        Assert.NotNull(await core.OpenOrJoinAsync("42", "srv", "Analysis: anomaly [aaaa1111]", false, at, null, "aaaa1111bbbb", null, Ct));
        Assert.Single(asked);
        Assert.Single(skipped);
    }

    private static FindingAlert Finding(string metric, FindingRoute route = FindingRoute.Page, string serverId = "43", string? hash = null,
        bool recurring = false) =>
        new(metric, "srv", "0.9", "0.5", serverId, new AlertContext(), 0.9, 0.5, "finding detail", false, route, hash, recurring);

    /// <summary>A throwaway DuckDB with the real alert history store and the real investigation store, on a clock the test
    /// can move forward.</summary>
    private sealed class ThrowawayDuckDb : IDisposable
    {
        private readonly string _dir;
        private readonly DuckDbInitializer _db;

        public ThrowawayDuckDb()
        {
            _dir = Path.Combine(Path.GetTempPath(), "LiteInvestigationDispatchTests_" + Guid.NewGuid().ToString("N")[..8]);
            Directory.CreateDirectory(_dir);
            _db = new DuckDbInitializer(Path.Combine(_dir, "lite.duckdb"));
            _db.InitializeAsync().GetAwaiter().GetResult();
            Store = new DuckDbInvestigationStore(_db, () => DateTime.UtcNow + Shift, DuckDbInvestigationStoreTests.TestLockBudget);
        }

        public DuckDbInvestigationStore Store { get; }

        public TimeSpan Shift { get; set; }

        public EmailAlertService EmailService(RouteSettings settings)
        {
            var history = new DuckDbAlertHistoryStore(_db);
            var webhooks = new WebhookAlertService(settings, EmailAlertService.Branding, NullLogger<WebhookAlertService>.Instance, history);
            var core = new InvestigationDispatchCore(Store.OpenOrJoinAsync, settings, NullLogger.Instance,
                recurringSkipped: Store.RecordRecurringSkipAsync);
            return new EmailAlertService(settings, history, webhooks, NullLogger<EmailAlertService>.Instance, core,
                (requestId, token) => Store.ReadRequestAsync(requestId, token));
        }

        public string? Scalar(string sql)
        {
            using var connection = _db.CreateConnection();
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            var value = command.ExecuteScalar();
            return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        public void Dispose()
        {
            _db.Dispose();
            try { if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true); }
            catch { /* best effort */ }
        }
    }

    private sealed class CountingLogger : ILogger
    {
        public int Informations { get; private set; }

        public int Warnings { get; private set; }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Information) Informations++;
            if (logLevel >= LogLevel.Warning) Warnings++;
        }
    }

    /// <summary>Settings with agent routes, and optionally the generic webhook and its body template. Not
    /// <c>AppAlertSettings</c>, which reads process-global statics.</summary>
    private sealed class RouteSettings : IAlertSettings
    {
        private readonly NotificationRoute[] _routes;

        public RouteSettings(params NotificationRoute[] routes) => _routes = routes;

        public string GenericUrl { get; init; } = "";

        public string Template { get; init; } = "";

        public IReadOnlyList<NotificationRoute> NotificationRoutes => _routes;
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
        public bool GenericWebhookEnabled => GenericUrl.Length > 0;
        public string GenericWebhookUrl => GenericUrl;
        public string GenericWebhookHeadersJson => "";
        public string GenericWebhookBodyTemplate => Template;
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

    /// <summary>A loopback endpoint that records each posted body and answers 200. <c>TcpListener</c> rather than
    /// <c>HttpListener</c>, which wants a URL ACL on Windows.</summary>
    private sealed class CapturingWebhookEndpoint : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly List<string> _bodies = new();
        private readonly CancellationTokenSource _stop = new();

        public CapturingWebhookEndpoint()
        {
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            Url = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/hook";
            _ = Task.Run(AcceptLoopAsync);
        }

        public string Url { get; }

        /// <summary>Read once the send has been awaited: each body is added before its response is written.</summary>
        public IReadOnlyList<string> Bodies => _bodies;

        private async Task AcceptLoopAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    using var stream = client.GetStream();
                    _bodies.Add(await ReadBodyAsync(stream));
                    await stream.WriteAsync(Encoding.ASCII.GetBytes("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"), _stop.Token);
                    await stream.FlushAsync(_stop.Token);
                }
            }
            catch (OperationCanceledException) { /* Dispose */ }
            catch (SocketException) { /* listener stopped */ }
            catch (ObjectDisposedException) { /* listener stopped */ }
        }

        private static async Task<string> ReadBodyAsync(NetworkStream stream)
        {
            var buffer = new byte[16 * 1024];
            var received = new List<byte>();
            int headerEnd;
            while ((headerEnd = HeaderEnd(received)) < 0)
            {
                var read = await stream.ReadAsync(buffer);
                if (read == 0) return Encoding.UTF8.GetString(received.ToArray());
                received.AddRange(new ArraySegment<byte>(buffer, 0, read));
            }

            var headers = Encoding.ASCII.GetString(received.ToArray(), 0, headerEnd);
            var length = 0;
            foreach (var line in headers.Split("\r\n"))
            {
                if (line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase))
                {
                    length = int.Parse(line["Content-Length:".Length..].Trim(), CultureInfo.InvariantCulture);
                }
            }

            var bodyStart = headerEnd + 4;
            while (received.Count - bodyStart < length)
            {
                var read = await stream.ReadAsync(buffer);
                if (read == 0) break;
                received.AddRange(new ArraySegment<byte>(buffer, 0, read));
            }

            return Encoding.UTF8.GetString(received.ToArray(), bodyStart, Math.Max(0, Math.Min(length, received.Count - bodyStart)));
        }

        private static int HeaderEnd(List<byte> bytes)
        {
            for (int i = 0; i + 3 < bytes.Count; i++)
            {
                if (bytes[i] == '\r' && bytes[i + 1] == '\n' && bytes[i + 2] == '\r' && bytes[i + 3] == '\n') return i;
            }

            return -1;
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();
            _stop.Dispose();
        }
    }
}
