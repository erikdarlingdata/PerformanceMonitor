/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The measured-cost floors a finding must clear to page (#7084), pinned through <see cref="AnalysisNotificationService.NotifyAsync"/>
/// so the whole path is covered: a finding under its floor sends nothing and spends no bucket, one over it pages, and the
/// PostgreSQL wait-profile cap holds a second page of one story for 24 hours, across a restart too. Every finding here is
/// corroborated (one matched co-fire check), so the route is Page and the floor is the only thing that can hold it.
/// </summary>
public class AnalysisPageFloorTests
{
    private sealed class CapturingSender : IFindingAlertSender
    {
        public List<FindingAlert> Sent { get; } = new();
        public Dictionary<string, DateTime> Seeds { get; } = new(StringComparer.Ordinal);

        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) =>
            Task.FromResult(Seeds.TryGetValue($"{serverId}:{metricName}", out var t) ? (DateTime?)t : null);

        public Task<AlertDelivery?> SendFindingAlertAsync(FindingAlert alert)
        {
            Sent.Add(alert);
            return Task.FromResult<AlertDelivery?>(null);
        }

        public Task<AlertDelivery?> SendFindingSummaryAsync(IReadOnlyList<FindingAlert> named) => Task.FromResult<AlertDelivery?>(null);
    }

    private sealed class Settings : IAlertSettings
    {
        public bool SmtpEnabled => false;
        public string SmtpServer => "";
        public int SmtpPort => 587;
        public bool SmtpUseSsl => true;
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
        public bool GenericWebhookEnabled => false;
        public string GenericWebhookUrl => "";
        public string GenericWebhookHeadersJson => "";
        public string GenericWebhookBodyTemplate => "";
        public string GenericWebhookProxyAddress => "";
        public bool PagerDutyEnabled => false;
        public string PagerDutyRoutingKey => "";
        public bool PagerDutyUseEuRegion => false;
        public string PagerDutyProxyAddress => "";
        public double AnalysisNotifySeverity => 1.5;
        /* 0 so the story's own steady-severity bucket cannot be what holds a repeat: only the cap can. */
        public int AnalysisNotifyCooldownMinutes => 0;
        public int AnalysisPageCap => 10;
        public string TriageBaseUrl => "";
        public FindingRoute UncorroboratedFindingRoute => FindingRoute.Digest;
    }

    private static (AnalysisNotificationService Notifier, CapturingSender Sender) Build()
    {
        var sender = new CapturingSender();
        var notifier = new AnalysisNotificationService(
            sender, new Settings(), f => f.ServerId.ToString(), NullLogger<AnalysisNotificationService>.Instance);
        return (notifier, sender);
    }

    private static AnalysisFinding Finding(
        string hash, string rootKey, Dictionary<string, double>? rootMetadata = null, IEnumerable<string>? path = null,
        Dictionary<string, double>? evidence = null, double severity = 1.9)
    {
        var keys = (path ?? [rootKey]).ToList();
        return new AnalysisFinding
        {
            ServerId = 7,
            ServerName = "a-server",
            Category = "anomaly",
            StoryPath = string.Join(" → ", keys),
            StoryPathHash = hash,
            Severity = severity,
            FactCount = keys.Count,
            MatchedAmplifiers = 1,
            DefinedAmplifiers = 4,
            Confidence = StoryConfidence.Compute(1, 4, keys.Count),
            RootFactKey = rootKey,
            RootFactValue = 1.0,
            PathKeys = keys,
            RootFactMetadata = rootMetadata,
            PageEvidence = evidence,
            TimeRangeStart = DateTime.UtcNow.AddHours(-4),
            TimeRangeEnd = DateTime.UtcNow,
        };
    }

    private static async Task<List<FindingAlert>> PagesFor(params AnalysisFinding[] findings)
    {
        var (notifier, sender) = Build();
        await notifier.NotifyAsync(findings);
        await notifier.FlushPendingAsync();
        return sender.Sent;
    }

    /* ---------------- part 2: write latency ---------------- */

    [Fact]
    public void TheWriteLatencyFloor_IsTheEnginesExistingWriteBar_NotASecondNumber()
    {
        Assert.Equal(PerformanceMonitor.Analysis.Baselines.AnomalyThresholds.WriteLatencyFloorMs, FindingPageFloor.WriteLatencyAvgFloorMs);
        Assert.Equal(20.0, FindingPageFloor.WriteLatencyAvgFloorMs);
    }

    [Theory]
    [InlineData(12.0, false)]
    [InlineData(19.9, false)]
    [InlineData(20.0, true)]
    [InlineData(40.0, true)]
    public async Task AWriteLatencyFinding_PagesOnlyAtOrOverTwentyMsAverage(double avgMs, bool pages)
    {
        var sent = await PagesFor(Finding("w000000000000001", "ANOMALY_WRITE_LATENCY",
            new Dictionary<string, double> { ["current_latency_ms"] = 90, ["avg_latency_ms"] = avgMs }));

        Assert.Equal(pages ? 1 : 0, sent.Count);
        if (pages)
            Assert.Equal(FindingRoute.Page, sent[0].Route);
    }

    [Fact]
    public async Task AWriteLatencyFindingUnderTheFloor_SpendsNoBucket_SoTheSameStoryPagesWhenItClearsTheFloor()
    {
        var (notifier, sender) = Build();
        var meta = (double avg) => new Dictionary<string, double> { ["current_latency_ms"] = 90, ["avg_latency_ms"] = avg };

        await notifier.NotifyAsync([Finding("w000000000000002", "ANOMALY_WRITE_LATENCY", meta(12))]);
        await notifier.FlushPendingAsync();
        Assert.Empty(sender.Sent);

        await notifier.NotifyAsync([Finding("w000000000000002", "ANOMALY_WRITE_LATENCY", meta(40))]);
        await notifier.FlushPendingAsync();
        Assert.Single(sender.Sent);
    }

    /* ---------------- part 3: plan-regression chains ---------------- */

    private static readonly string[] PlanRegressionChain = ["CPU_SQL_PERCENT", "PLAN_REGRESSION"];

    private static List<Fact> Facts(double avgTotalCpu, double? timeoutOffenders, double cpuSeverity = 0)
    {
        var regression = new Fact
        {
            Key = "PLAN_REGRESSION",
            Value = 4,
            Severity = 1.2,
            Metadata = timeoutOffenders is { } t
                ? new Dictionary<string, double> { ["timeout_offender_count"] = t }
                : new Dictionary<string, double>(),
        };
        var cpu = new Fact
        {
            Key = "CPU_SQL_PERCENT",
            Value = avgTotalCpu * 0.8,
            Severity = cpuSeverity,
            Metadata = new Dictionary<string, double> { ["avg_sql_cpu"] = avgTotalCpu * 0.8, ["avg_total_cpu"] = avgTotalCpu },
        };
        return [regression, cpu];
    }

    private static AnalysisFinding PlanRegressionFinding(double avgTotalCpu, double? timeouts, string hash)
        => Finding(hash, "CPU_SQL_PERCENT", path: PlanRegressionChain,
            evidence: FindingPageFloor.Evidence(PlanRegressionChain, Facts(avgTotalCpu, timeouts)));

    [Fact]
    public async Task APlanRegressionChainAt15PercentCpu_WithNoTimeouts_DoesNotPage()
        => Assert.Empty(await PagesFor(PlanRegressionFinding(15, timeouts: null, "p000000000000001")));

    [Fact]
    public async Task APlanRegressionChainAt70PercentCpu_Pages()
        => Assert.Single(await PagesFor(PlanRegressionFinding(70, timeouts: null, "p000000000000002")));

    [Fact]
    public async Task APlanRegressionChainAt15PercentCpu_WithTimeouts_Pages()
        => Assert.Single(await PagesFor(PlanRegressionFinding(15, timeouts: 2, "p000000000000003")));

    [Fact]
    public async Task APlanRegressionChainAt15PercentCpu_WhoseTimeoutCountIsZero_DoesNotPage()
        => Assert.Empty(await PagesFor(PlanRegressionFinding(15, timeouts: 0, "p000000000000004")));

    [Fact]
    public void TheEvidence_IsReadFromEveryFact_IncludingACpuFactThatScoredZero_AndIsNullOffAPlanRegressionChain()
    {
        var evidence = FindingPageFloor.Evidence(PlanRegressionChain, Facts(18, 1, cpuSeverity: 0));
        Assert.NotNull(evidence);
        Assert.Equal(18, evidence![FindingPageFloor.EvidenceWindowAvgCpu]);
        Assert.Equal(1, evidence[FindingPageFloor.EvidenceTimeoutOffenders]);

        Assert.Null(FindingPageFloor.Evidence(["CPU_SQL_PERCENT", "SOS_SCHEDULER_YIELD"], Facts(18, 1)));
    }

    [Fact]
    public async Task AChainWithoutPlanRegression_IsNotHeldByThePlanRegressionFloor()
        => Assert.Single(await PagesFor(Finding("p000000000000005", "CPU_SQL_PERCENT", path: ["CPU_SQL_PERCENT", "SOS_SCHEDULER_YIELD"])));

    /* ---------------- part 4: PostgreSQL wait profile ---------------- */

    private static AnalysisFinding WaitProfile(double meanMsPerSec, string hash = "g000000000000001", double severity = 1.75, string root = "ANOMALY_PG_WAIT_PROFILE")
        => Finding(hash, root, new Dictionary<string, double> { ["mean_ms_per_sec"] = meanMsPerSec, ["current_ms_per_sec"] = meanMsPerSec * 3 }, severity: severity);

    [Fact]
    public void TheWaitProfileFloor_IsPoisonWaitsWarningBar()
        => Assert.Equal(PoisonWaitEvaluator.WarningAvgWaiters, FindingPageFloor.WaitProfileAvgWaitersFloor);

    [Theory]
    [InlineData("ANOMALY_PG_WAIT_PROFILE")]
    [InlineData("ANOMALY_PG_SAMPLED_WAIT_PROFILE")]
    public async Task AWaitProfileAt0Point3WaitingBackends_DoesNotPage(string root)
        => Assert.Empty(await PagesFor(WaitProfile(300, root: root)));

    [Fact]
    public async Task AWaitProfileAt2WaitingBackends_PagesOnce_AndTheSameStoryLaterDoesNotPageAgain()
    {
        var (notifier, sender) = Build();

        await notifier.NotifyAsync([WaitProfile(2000)]);
        await notifier.FlushPendingAsync();
        Assert.Single(sender.Sent);

        /* The story worsens by more than the steady-severity step with no cooldown, so only the 24-hour cap holds it. */
        await notifier.NotifyAsync([WaitProfile(2500, severity: 2.0)]);
        await notifier.FlushPendingAsync();
        Assert.Single(sender.Sent);
    }

    [Fact]
    public async Task TheCap_SurvivesARestart_BecauseItReadsTheAlertLogsLastDeliveredPage()
    {
        var (notifier, sender) = Build();
        var finding = WaitProfile(2000, "g000000000000002");
        sender.Seeds[$"7:{FindingMessageFormatter.MetricName(finding)}"] = DateTime.UtcNow.AddHours(-1);

        await notifier.NotifyAsync([finding]);
        await notifier.FlushPendingAsync();

        Assert.Empty(sender.Sent);
    }

    [Fact]
    public async Task TheCap_IsOpenAgainAfter24Hours()
    {
        var (notifier, sender) = Build();
        /* 1.8 clears the existing steady-severity hold (a seeded page assumes the 1.5 notify floor plus the 0.25 step),
           so what is under test is the cap alone. */
        var finding = WaitProfile(2000, "g000000000000003", severity: 1.8);
        sender.Seeds[$"7:{FindingMessageFormatter.MetricName(finding)}"] = DateTime.UtcNow.AddHours(-25);

        await notifier.NotifyAsync([finding]);
        await notifier.FlushPendingAsync();

        Assert.Single(sender.Sent);
    }

    [Fact]
    public async Task TheCap_IsPerStory_AnotherStoryOnTheSameServerStillPages()
    {
        var (notifier, sender) = Build();

        await notifier.NotifyAsync([WaitProfile(2000, "g000000000000004")]);
        await notifier.FlushPendingAsync();
        await notifier.NotifyAsync([WaitProfile(2000, "g000000000000005")]);
        await notifier.FlushPendingAsync();

        Assert.Equal(2, sender.Sent.Count);
    }
}
