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
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5320 (part of #4348), the delivery choke point: <c>DarlingAlertDeliverer.DeliverAndReportAsync</c> runs the statement
/// filter, so the eight callers that hand an outcome straight to it (the PostgreSQL families in <c>DarlingWorker</c>, a
/// self alert, a custom alert rule) are filtered like an engine alert. These run the REAL deliverer over a recording
/// history store, so what is asserted is what an operator would be sent and what the history row stores.
/// </summary>
public sealed class StatementFilterDeliveryTests
{
    private const string Marker = SensitiveStatements.PlaceholderText;

    private const string DefinitionJson =
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1000}}";

    private static (DarlingAlertDeliverer Deliverer, RecordingHistoryStore History) Build(DarlingConfig? config = null)
    {
        var settings = new DarlingAlertSettings(config ?? new DarlingConfig());
        var history = new RecordingHistoryStore();
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        return (new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance), history);
    }

    private static string RowText(AlertHistoryRecord record) =>
        string.Join("\n", new[] { record.CurrentValueText, record.ThresholdValueText, record.DetailText, record.ContextJson }
            .Where(p => p is not null));

    private static void AssertNoSecret(string text)
    {
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.DoesNotContain(needle, text, StringComparison.Ordinal);
        }
    }

    /// <summary>The shape a PostgreSQL long-running-query family builds: one detail item with the query text.</summary>
    private static AlertOutcome PgFamilyOutcome(string queryText)
    {
        var context = new AlertContext();
        var item = new AlertDetailItem { Heading = "Long-running query" };
        item.Fields.Add(("Database", "stackoverflow"));
        item.Fields.Add(("Query", queryText));
        context.Details.Add(item);
        return new AlertOutcome(
            "pg1", "pg-server", "PG Long-Running Query", "45", "30", context,
            AlertContextBuilders.ContextToDetailText(context), 45, 30, false, null,
            ShortMessage: "Session 71 running 45m - " + queryText);
    }

    private static LongRunningQueryInfo LongRunning(string queryText) => new()
    {
        SessionId = 71,
        DatabaseName = "StackOverflow",
        QueryText = queryText,
        ElapsedSeconds = 45 * 60,
        CpuTimeMs = 1000,
        QueryHash = "0x9AAF0129E4E9AD07",
    };

    [Fact]
    public async Task APostgreSqlFamilyAlert_HandedStraightToTheDeliverer_IsDeliveredWithTheStatementWithheld()
    {
        var (deliverer, history) = Build();

        await deliverer.DeliverAndReportAsync(PgFamilyOutcome(StatementScrubCanary.CanaryStatement), TestContext.Current.CancellationToken);

        var row = Assert.Single(history.Records);
        var text = RowText(row);
        AssertNoSecret(text);
        Assert.Contains(Marker, text, StringComparison.Ordinal);
        Assert.Contains("stackoverflow", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASelfAlert_WhoseProseNamesAStatement_IsDeliveredWithTheMarker()
    {
        var (deliverer, history) = Build();
        var outcome = new AlertOutcome(
            "1", "srv", "Collection Failing", "3", "1", Context: null,
            DetailText: "Collector failed on: " + StatementScrubCanary.CanaryStatement,
            NumericCurrentValue: 3, NumericThresholdValue: 1, Muted: false, Severity: null,
            ShortMessage: "Collector failed on: " + StatementScrubCanary.CanaryStatement);

        await deliverer.DeliverAndReportAsync(outcome, TestContext.Current.CancellationToken);

        var text = RowText(Assert.Single(history.Records));
        AssertNoSecret(text);
        Assert.Contains(Marker, text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACustomRuleAlert_WhoseNameAndDescriptionNameAStatement_IsDeliveredWithTheMarker()
    {
        var (definition, error) = CustomAlertRuleDefinition.TryParse(DefinitionJson);
        Assert.Null(error);
        var rule = new CustomAlertRule(
            7, StatementScrubCanary.CanaryStatement, DefinitionJson, StatementScrubCanary.CanaryStatement,
            Enabled: true, Version: 1, CreatedAt: DateTime.UtcNow, UpdatedAt: DateTime.UtcNow, UpdatedBy: null);
        var outcome = CustomAlertEvaluator.BuildFireOutcome(
            rule, definition!, serverKey: "7", serverDisplayName: "srv", value: 1500,
            severity: AlertSeverityLevel.Warning, muted: false);
        Assert.Contains("S3cret-canary-ssf", outcome.DetailText!, StringComparison.Ordinal);
        var (deliverer, history) = Build();

        await deliverer.DeliverAndReportAsync(outcome, TestContext.Current.CancellationToken);

        var row = Assert.Single(history.Records);
        AssertNoSecret(RowText(row));
        Assert.Contains(Marker, row.DetailText!, StringComparison.Ordinal);
        var filtered = AlertStatementFilter.Apply(outcome);
        AssertNoSecret(filtered.DisplayName!);
    }

    [Fact]
    public async Task APerEventSplit_OfADirectlyDeliveredAlert_IsFilteredToo()
    {
        var config = new DarlingConfig();
        config.Alerts.DeliveryMode = AlertNotificationMode.PerEvent;
        var (deliverer, history) = Build(config);
        var context = new AlertContext();
        AlertIncidentRenderer.Apply(context, new List<AlertIncident>
        {
            new("k1", new[] { "dbo.t" }, DetailFields: new[] { new AlertIncidentField("Victim SQL", StatementScrubCanary.CanaryStatement) }),
            new("k2", new[] { "dbo.u" }, DetailFields: new[] { new AlertIncidentField("Victim SQL", StatementScrubCanary.CanaryStatement) }),
        });
        var outcome = new AlertOutcome(
            "pg1", "pg-server", "PG Blocking Detected", "2", "1", context, "detail", 2, 1, false, null);

        await deliverer.DeliverAndReportAsync(outcome, TestContext.Current.CancellationToken);

        Assert.Equal(2, history.Records.Count);
        Assert.All(history.Records, r => AssertNoSecret(RowText(r)));
    }

    [Fact]
    public async Task AnOutcomeTheEngineAlreadyFiltered_IsNotJudgedASecondTime()
    {
        /* The marker is the whole contract: a marked outcome is trusted as it is, so text planted in one passes
           through, and the same text in an unmarked outcome does not. That is what "judged once" means here. */
        var (deliverer, history) = Build();
        var planted = PgFamilyOutcome(StatementScrubCanary.CanaryStatement);

        await deliverer.DeliverAndReportAsync(AlertStatementFilter.MarkJudged(planted), TestContext.Current.CancellationToken);
        await deliverer.DeliverAndReportAsync(planted, TestContext.Current.CancellationToken);

        Assert.Contains("S3cret-canary-ssf", RowText(history.Records[0]), StringComparison.Ordinal);
        AssertNoSecret(RowText(history.Records[1]));
    }

    [Fact]
    public void Apply_SetsTheMarkerOnTheCopyItReturns_AndNeverJudgesAMarkedOutcomeAgain()
    {
        var named = PgFamilyOutcome(StatementScrubCanary.CanaryStatement);
        Assert.False(named.StatementFiltered);

        var filtered = AlertStatementFilter.Apply(named);

        Assert.NotSame(named, filtered);
        Assert.True(filtered.StatementFiltered);
        Assert.Same(filtered, AlertStatementFilter.Apply(filtered));
        Assert.Same(filtered, AlertStatementFilter.MarkJudged(filtered));
    }

    [Fact]
    public void APlainAlert_IsTheSameInstanceThroughTheFilter_AndMarkedOnce()
    {
        var plain = PgFamilyOutcome(StatementScrubCanary.PlainStatement);

        Assert.Same(plain, AlertStatementFilter.Apply(plain));
        var marked = AlertStatementFilter.MarkJudged(plain);
        Assert.True(marked.StatementFiltered);
        Assert.Same(marked, AlertStatementFilter.Apply(marked));
        Assert.Same(marked, AlertStatementFilter.MarkJudged(marked));
        Assert.Same(plain.Context, marked.Context);
    }

    [Fact]
    public async Task APlainAlertThroughTheEngine_ReachesTheDelivererMarked_WithItsContextUntouched()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.LongRunningQueryEnabled = true;
        h.Adapter.LongRunning.Add(LongRunning(StatementScrubCanary.PlainStatement));

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var outcome = Assert.Single(h.Deliverer.Outcomes);
        Assert.True(outcome.StatementFiltered);
        Assert.Contains("canary_plain_ssf", StatementFilterAlertTests.Everything(outcome), StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnEngineAlert_IsFilteredByTheEngineAndMarked_SoTheRealDelivererStoresItFiltered()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.LongRunningQueryEnabled = true;
        h.Adapter.LongRunning.Add(LongRunning(StatementScrubCanary.CanaryStatement));

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var outcome = Assert.Single(h.Deliverer.Outcomes);
        Assert.True(outcome.StatementFiltered);
        var (deliverer, history) = Build();
        await deliverer.DeliverAndReportAsync(outcome, TestContext.Current.CancellationToken);
        AssertNoSecret(RowText(Assert.Single(history.Records)));
    }

    private static DarlingPgDeadlockReader.PgDeadlockRow DeadlockRow(string? victimStatement) =>
        new(
            OccurredAtUtc: new DateTime(2026, 8, 31, 0, 0, 0, DateTimeKind.Utc),
            VictimPid: 111,
            ParticipantCount: 2,
            DeadlockHash: "hash-1",
            LockModes: null,
            Resources: null,
            VictimStatement: victimStatement,
            TimesSeen: 1);

    private static DarlingPgBlockingReader.PgBlockingChainRow BlockingRow(string? rootQuery) =>
        new(
            CapturedAt: new DateTime(2026, 8, 31, 0, 0, 0, DateTimeKind.Utc),
            RootBackendId: 424242,
            RootPid: 200,
            Databases: new[] { "sales" },
            RootUsername: "app",
            RootApplicationName: "webapi",
            RootState: "active",
            RootQuery: rootQuery,
            RootIsIdleInTransaction: false,
            RootXactDurationMs: 5000,
            RootQueryDurationMs: 5000,
            TotalVictims: 3,
            DirectVictims: 3,
            MaxDepth: 1,
            WorstVictimWaitMs: 4000,
            WorstVictimQuery: "SELECT 2",
            SamplesAsRoot: 1,
            QueryTextMayBeTruncated: false,
            ChainMayBeTruncated: false);

    /// <summary>The outcome the PostgreSQL deadlock and blocking families hand the deliverer: incidents only, no detail text.</summary>
    private static AlertOutcome PgIncidentOutcome(string metric, params AlertIncident[] incidents) =>
        new(
            "pg1", "pg-server", metric, "1", "1",
            Context: new AlertContext { Incidents = incidents.ToList() },
            DetailText: null, NumericCurrentValue: 1, NumericThresholdValue: 1, Muted: false, Severity: null,
            ShortMessage: "1 event in the last hour");

    [Theory]
    [InlineData(true, false)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public async Task APostgreSqlIncident_ThatNamesAStatementAmongItsObjects_IsDeliveredWithTheStatementWithheld(bool blocking, bool perEvent)
    {
        /* The REAL incident builders (the objects hold the root query or the victim statement), the real deliverer
           over a recording history store, a capturing webhook endpoint and a capturing mail endpoint. */
        using var endpoint = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();
        var config = new DarlingConfig();
        config.Webhooks.TeamsUrl = endpoint.Url;
        config.Webhooks.SlackUrl = endpoint.Url;
        config.Webhooks.GenericUrl = endpoint.Url;
        config.Smtp.Host = "127.0.0.1";
        config.Smtp.Port = smtp.Port;
        config.Smtp.UseSsl = false;
        config.Smtp.From = "monitor@example.invalid";
        config.Smtp.To = "operator@example.invalid";
        if (perEvent)
        {
            config.Alerts.DeliveryMode = AlertNotificationMode.PerEvent;
        }

        var settings = new DarlingAlertSettings(config);
        var history = new RecordingHistoryStore();
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance);
        var incident = blocking
            ? DarlingWorker.BuildPgBlockingIncident(BlockingRow(StatementScrubCanary.CanaryStatement))
            : DarlingWorker.BuildPgDeadlockIncident(DeadlockRow(StatementScrubCanary.CanaryStatement));
        Assert.Contains("S3cret-canary-ssf", string.Join("\n", incident.InvolvedObjects), StringComparison.Ordinal);

        await deliverer.DeliverAndReportAsync(
            PgIncidentOutcome(blocking ? "PG Blocking Detected" : "PG Deadlocks Detected", incident),
            TestContext.Current.CancellationToken);

        var row = Assert.Single(history.Records);
        Assert.NotEmpty(endpoint.Bodies);
        Assert.NotEmpty(smtp.Messages);
        AssertNoSecret(RowText(row));
        foreach (var body in endpoint.Bodies)
        {
            AssertNoSecret(body);
        }

        foreach (var message in smtp.Messages)
        {
            AssertNoSecret(message);
        }

        Assert.Contains(Marker, string.Join("\n", endpoint.Bodies), StringComparison.Ordinal);
        Assert.Contains(Marker, row.ContextJson!, StringComparison.Ordinal);
    }

    [Fact]
    public void AFailureWhileJudging_ForAnAlertWithNoDetailText_StillSaysWhyItHasNoDetail()
    {
        /* A null detail item cannot be read, which forces the failure path. The per-event PostgreSQL families pass
           no detail text, so the reason has to ride the context. */
        var context = new AlertContext
        {
            Incidents = new List<AlertIncident> { DarlingWorker.BuildPgBlockingIncident(BlockingRow(StatementScrubCanary.CanaryStatement)) },
        };
        context.Details.Add(null!);
        var outcome = new AlertOutcome(
            "pg1", "pg-server", "PG Blocking Detected", "1", "1", context,
            DetailText: null, NumericCurrentValue: 1, NumericThresholdValue: 1, Muted: false, Severity: null);

        var filtered = AlertStatementFilter.Apply(outcome);

        Assert.Contains(Marker, AlertContextBuilders.ContextToDetailText(filtered.Context)!, StringComparison.Ordinal);
        AssertNoSecret(StatementFilterAlertTests.Everything(filtered));
        var incident = Assert.Single(filtered.Context!.Incidents!);
        Assert.Empty(incident.InvolvedObjects);
        Assert.Equal("424242", incident.DedupKey);
    }

    [Fact]
    public void AFailureWhileJudging_ForAnAlertWithANullIncident_ReturnsTheClearedAlertInsteadOfThrowing()
    {
        var failing = PgIncidentOutcome(
            "PG Blocking Detected",
            DarlingWorker.BuildPgBlockingIncident(BlockingRow(StatementScrubCanary.CanaryStatement)),
            null!);
        failing.Context!.Details.Add(null!);

        var filtered = AlertStatementFilter.Apply(failing);

        Assert.True(filtered.StatementFiltered);
        Assert.Contains(Marker, AlertContextBuilders.ContextToDetailText(filtered.Context)!, StringComparison.Ordinal);
        Assert.Equal(2, filtered.Context!.Incidents!.Count);
        Assert.Empty(filtered.Context.Incidents[0].InvolvedObjects);
        Assert.Null(filtered.Context.Incidents[1]);
    }

    [Fact]
    public async Task AFailureWhileJudging_ForAnAlertWithNoIncident_StillDeliversTheClearedAlert()
    {
        var (deliverer, history) = Build();
        var context = new AlertContext();
        context.Details.Add(null!);
        var failing = new AlertOutcome(
            "1", "srv", "Collection Failing", "3", "1", context,
            DetailText: null, NumericCurrentValue: 3, NumericThresholdValue: 1, Muted: false, Severity: null);

        await deliverer.DeliverAndReportAsync(failing, TestContext.Current.CancellationToken);

        var row = Assert.Single(history.Records);
        Assert.Contains(Marker, RowText(row), StringComparison.Ordinal);
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

        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) =>
            Task.FromResult<DateTime?>(null);
    }
}
