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
        var unmarked = PgFamilyOutcome(StatementScrubCanary.CanaryStatement);

        await deliverer.DeliverAndReportAsync(AlertStatementFilter.MarkJudged(planted), TestContext.Current.CancellationToken);
        await deliverer.DeliverAndReportAsync(unmarked, TestContext.Current.CancellationToken);

        Assert.Contains("S3cret-canary-ssf", RowText(history.Records[0]), StringComparison.Ordinal);
        AssertNoSecret(RowText(history.Records[1]));
    }

    [Fact]
    public async Task AWithCopyThatAddsNewText_OfAJudgedOutcome_IsJudgedAgain()
    {
        /* M2 (#5360): the mark lives in the filter's own table, keyed by instance, so a `with` copy of a judged
           outcome is a new instance and its new text is judged. At the base the mark was a record parameter that a
           `with` copy kept, so this text reached the deliverer unjudged. */
        var judged = AlertStatementFilter.MarkJudged(AlertStatementFilter.Apply(PgFamilyOutcome(StatementScrubCanary.PlainStatement)));
        var edited = judged with { DetailText = "Query: " + StatementScrubCanary.CanaryStatement, ShortMessage = StatementScrubCanary.CanaryStatement };

        Assert.NotSame(edited, AlertStatementFilter.Apply(edited));
        Assert.DoesNotContain("S3cret-canary-ssf", StatementFilterAlertTests.Everything(AlertStatementFilter.Apply(edited)), StringComparison.Ordinal);

        var (deliverer, history) = Build();
        await deliverer.DeliverAndReportAsync(edited, TestContext.Current.CancellationToken);
        AssertNoSecret(RowText(Assert.Single(history.Records)));
    }

    [Fact]
    public void AnOutcomeACallerBuilt_IsJudged_AndTheFilterHasNoPublicWayToMarkOne()
    {
        var built = PgFamilyOutcome(StatementScrubCanary.CanaryStatement);

        Assert.NotSame(built, AlertStatementFilter.Apply(built));
        Assert.DoesNotContain("S3cret-canary-ssf", StatementFilterAlertTests.Everything(AlertStatementFilter.Apply(built)), StringComparison.Ordinal);

        /* No public parameter, property or method can say "already filtered" (M2, #5360). */
        Assert.Null(typeof(AlertOutcome).GetProperty("StatementFiltered"));
        Assert.Null(typeof(AlertStatementFilter).GetMethod("MarkJudged", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static));
    }

    [Fact]
    public void Apply_SetsTheMarkerOnTheCopyItReturns_AndNeverJudgesAMarkedOutcomeAgain()
    {
        var named = PgFamilyOutcome(StatementScrubCanary.CanaryStatement);

        var filtered = AlertStatementFilter.Apply(named);

        Assert.NotSame(named, filtered);
        Assert.Same(filtered, AlertStatementFilter.Apply(filtered));
        Assert.Same(filtered, AlertStatementFilter.MarkJudged(filtered));
    }

    [Fact]
    public void APlainAlert_IsTheSameInstanceThroughTheFilter_AndMarkedOnce()
    {
        var plain = PgFamilyOutcome(StatementScrubCanary.PlainStatement);

        Assert.Same(plain, AlertStatementFilter.Apply(plain));
        var marked = AlertStatementFilter.MarkJudged(plain);
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
        Assert.Same(outcome, AlertStatementFilter.Apply(outcome));
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
        Assert.Same(outcome, AlertStatementFilter.Apply(outcome));
        var (deliverer, history) = Build();
        await deliverer.DeliverAndReportAsync(outcome, TestContext.Current.CancellationToken);
        AssertNoSecret(RowText(Assert.Single(history.Records)));
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
