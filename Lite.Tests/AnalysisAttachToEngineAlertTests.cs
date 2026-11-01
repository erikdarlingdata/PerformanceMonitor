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
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A finding raised while an engine alert is open on the same server attaches to that alert instead of paging (#7084 part 4),
/// pinned through <see cref="AnalysisNotificationService.NotifyAsync"/> with the seam stubbed. Every finding is corroborated
/// (one matched co-fire check), so the gate routes it to Page and the seam is the only thing that can hold it.
/// </summary>
public class AnalysisAttachToEngineAlertTests
{
    private sealed class CapturingSender : IFindingAlertSender
    {
        public List<FindingAlert> Sent { get; } = new();

        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) => Task.FromResult<DateTime?>(null);

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
        public int AnalysisNotifyCooldownMinutes => 30;
        public int AnalysisPageCap => 10;
        public string TriageBaseUrl => "";
        public FindingRoute UncorroboratedFindingRoute => FindingRoute.Digest;
    }

    private static AnalysisFinding Finding(string hash = "a000000000000001") => new()
    {
        ServerId = 7,
        ServerName = "a-server",
        Category = "anomaly",
        StoryPath = "ROOT_ONE",
        StoryPathHash = hash,
        Severity = 1.6,
        FactCount = 1,
        MatchedAmplifiers = 1,
        DefinedAmplifiers = 4,
        Confidence = StoryConfidence.Compute(1, 4, 1),
        RootFactKey = "ROOT_ONE",
        RootFactValue = 1.0,
        PathKeys = new List<string> { "ROOT_ONE" },
        TimeRangeStart = DateTime.UtcNow.AddHours(-4),
        TimeRangeEnd = DateTime.UtcNow,
    };

    private static (AnalysisNotificationService Notifier, CapturingSender Sender, List<(string Server, DateTime Now)> Asked) Build(
        Func<string, DateTime, Task<AlertAttachedToDto?>> seam)
    {
        var sender = new CapturingSender();
        var asked = new List<(string, DateTime)>();
        var notifier = new AnalysisNotificationService(
            sender, new Settings(), f => f.ServerId.ToString(), NullLogger<AnalysisNotificationService>.Instance,
            findOpenEngineAlert: (server, now) =>
            {
                asked.Add((server, now));
                return seam(server, now);
            });
        return (notifier, sender, asked);
    }

    [Fact]
    public async Task AFindingOnAServerWithAnOpenEngineAlert_IsHeldAsAttached_AndSendsNothing()
    {
        var alert = new AlertAttachedToDto("High CPU", DateTime.UtcNow.AddMinutes(-20));
        var (notifier, sender, asked) = Build((_, _) => Task.FromResult<AlertAttachedToDto?>(alert));

        await notifier.NotifyAsync(new[] { Finding() });
        await notifier.FlushPendingAsync();

        var row = Assert.Single(sender.Sent);
        Assert.Equal(FindingRoute.Digest, row.Route);
        Assert.Equal("digest", row.Context.Routing!.Route);
        Assert.Equal($"attached to High CPU fired {alert.AlertTimeUtc:HH:mm}", row.Context.Routing.Reason);
        Assert.Equal(alert, row.Context.AttachedTo);
        Assert.Equal("7", Assert.Single(asked).Server);
    }

    [Fact]
    public async Task TheSameFindingWithNoOpenEngineAlert_StillPages()
    {
        var (notifier, sender, _) = Build((_, _) => Task.FromResult<AlertAttachedToDto?>(null));

        await notifier.NotifyAsync(new[] { Finding() });
        await notifier.FlushPendingAsync();

        var row = Assert.Single(sender.Sent);
        Assert.Equal(FindingRoute.Page, row.Route);
        Assert.Null(row.Context.AttachedTo);
        Assert.Equal("page", row.Context.Routing!.Route);
    }

    [Fact]
    public async Task AFailedSeamRead_PagesRatherThanSwallowingTheFinding()
    {
        var (notifier, sender, _) = Build((_, _) => throw new InvalidOperationException("read failed"));

        await notifier.NotifyAsync(new[] { Finding() });
        await notifier.FlushPendingAsync();

        Assert.Equal(FindingRoute.Page, Assert.Single(sender.Sent).Route);
    }

    [Fact]
    public async Task AHeldFinding_SpendsNoPageBucket_SoItPagesAsNewOnceTheEngineAlertResolves()
    {
        AlertAttachedToDto? open = new("Deadlocks Detected", DateTime.UtcNow.AddMinutes(-5));
        var (notifier, sender, _) = Build((_, _) => Task.FromResult<AlertAttachedToDto?>(open));

        await notifier.NotifyAsync(new[] { Finding() });
        Assert.Equal(FindingRoute.Digest, Assert.Single(sender.Sent).Route);

        open = null;
        await notifier.NotifyAsync(new[] { Finding() });
        await notifier.FlushPendingAsync();

        Assert.Equal(new[] { FindingRoute.Digest, FindingRoute.Page }, sender.Sent.Select(s => s.Route));
    }

    [Fact]
    public async Task AStandingAttachedFinding_WritesOneHeldRowPerCooldown_NotOnePerCycle()
    {
        var alert = new AlertAttachedToDto("Blocking Detected", DateTime.UtcNow.AddMinutes(-10));
        var (notifier, sender, _) = Build((_, _) => Task.FromResult<AlertAttachedToDto?>(alert));

        await notifier.NotifyAsync(new[] { Finding() });
        await notifier.NotifyAsync(new[] { Finding() });

        Assert.Single(sender.Sent);
    }

    [Fact]
    public async Task ANoSeam_PagesAsBefore()
    {
        var sender = new CapturingSender();
        var notifier = new AnalysisNotificationService(
            sender, new Settings(), f => f.ServerId.ToString(), NullLogger<AnalysisNotificationService>.Instance);

        await notifier.NotifyAsync(new[] { Finding() });
        await notifier.FlushPendingAsync();

        Assert.Equal(FindingRoute.Page, Assert.Single(sender.Sent).Route);
    }

    [Fact]
    public void TheAttachedTo_RoundTripsThroughTheStoredContext()
    {
        var at = new DateTime(2026, 11, 1, 14, 2, 3, 456, DateTimeKind.Utc).AddTicks(780);
        var context = new AlertContext { AttachedTo = new AlertAttachedToDto("High CPU", at) };
        var json = AlertContextSerializer.Serialize(context);

        var read = AlertContextSerializer.TryReadAttachedTo(json);
        Assert.Equal("High CPU", read!.Metric);
        Assert.Equal(at, read.AlertTimeUtc);
        Assert.True(AlertContextSerializer.TryDeserialize(json, out var back));
        Assert.Equal(context.AttachedTo, back.AttachedTo);
        Assert.True(OpenEngineAlerts.Names(read, "High CPU", at));
        Assert.False(OpenEngineAlerts.Names(read, "Blocking Detected", at));
        Assert.False(OpenEngineAlerts.Names(read, "High CPU", at.AddMinutes(1)));
    }

    [Theory]
    [InlineData("""{"Details":[]}""")]
    [InlineData("""{"Details":[],"Routing":{"Route":"digest","Reason":"a lone fact"},"External":null}""")]
    [InlineData("""{"Details":[],"AttachedTo":null}""")]
    [InlineData("not json")]
    [InlineData(null)]
    public void ARowWrittenBeforeTheMemberExisted_ReadsNull(string? json)
    {
        Assert.Null(AlertContextSerializer.TryReadAttachedTo(json));
        if (json is not null && json.StartsWith('{'))
        {
            Assert.True(AlertContextSerializer.TryDeserialize(json, out var context));
            Assert.Null(context.AttachedTo);
        }
    }

    [Theory]
    [InlineData("High CPU", true)]
    [InlineData("Blocking Detected", true)]
    [InlineData("Blocking Wait Time", true)]
    [InlineData("Deadlocks Detected", true)]
    [InlineData("Failed Jobs", false)]
    [InlineData("Analysis: something", false)]
    public void OnlyTheFourEngineAlertsAttract(string metric, bool attachable) =>
        Assert.Equal(attachable, OpenEngineAlerts.IsAttachable(metric));
}

/// <summary>
/// Lite's reader for the seam (#7084 part 4): the newest delivered, un-muted, non-digest fire of the four engine alerts in the
/// last 60 minutes with no resolution row after it, and the list of findings attached to an alert.
/// </summary>
public sealed class DuckDbOpenEngineAlertTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 52;
    private static readonly DateTime Now = DateTime.UtcNow;
    private readonly DuckDbInitializer _duckDb;

    public DuckDbOpenEngineAlertTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private Task RecordAsync(string metric, DateTime at, AlertDelivery delivery, bool muted = false, int serverId = ServerId, string? contextJson = null) =>
        new DuckDbAlertHistoryStore(_duckDb).RecordAlertAsync(new AlertHistoryRecord(
            serverId.ToString(), "SQL01", metric, "97%", "90%", 97, 90, delivery, muted, "detail", contextJson) { AlertTimeUtc = at });

    private static AlertDelivery Fanout(AlertChannelOutcome email, string? error = null, bool muted = false) => AlertDelivery.FromFanout(
        new EmailFanoutResult(EmailOutcome: email, SendError: error, WebhookOutcome: AlertChannelOutcome.NotAttempted,
            WebhookSendError: null, AnyChannelConfigured: true),
        muted, trayChannelPresent: false);

    private static AlertDelivery Delivered => Fanout(AlertChannelOutcome.Delivered);

    private Task<AlertAttachedToDto?> FindAsync(int serverId = ServerId) =>
        new DuckDbAlertHistoryStore(_duckDb).FindOpenEngineAlertAsync(serverId.ToString(), Now);

    [Fact]
    public async Task ADeliveredHighCpu20MinutesAgoWithNoResolution_IsOpen()
    {
        var firedAt = Now.AddMinutes(-20);
        await RecordAsync("High CPU", firedAt, Delivered);

        var open = await FindAsync();

        Assert.Equal("High CPU", open!.Metric);
        Assert.Equal(AlertHistoryEscalationSeed.TruncateToMicroseconds(firedAt), open.AlertTimeUtc);
    }

    [Fact]
    public async Task AResolvedHighCpu_IsNotOpen()
    {
        await RecordAsync("High CPU", Now.AddMinutes(-20), Delivered);
        await RecordAsync("CPU Resolved", Now.AddMinutes(-5), AlertDelivery.NoChannelApplies());

        Assert.Null(await FindAsync());
    }

    [Fact]
    public async Task AFireAfterTheResolution_IsOpenAgain()
    {
        await RecordAsync("Blocking Detected", Now.AddMinutes(-40), Delivered);
        await RecordAsync("Blocking Cleared", Now.AddMinutes(-30), AlertDelivery.NoChannelApplies());
        await RecordAsync("Blocking Detected", Now.AddMinutes(-10), Delivered);

        Assert.Equal("Blocking Detected", (await FindAsync())!.Metric);
    }

    [Fact]
    public async Task AHighCpuDelivered90MinutesAgo_IsNotOpen()
    {
        await RecordAsync("High CPU", Now.AddMinutes(-90), Delivered);

        Assert.Null(await FindAsync());
    }

    [Fact]
    public async Task AMutedRowAndADigestRow_DoNotAttractAnAttachment()
    {
        await RecordAsync("High CPU", Now.AddMinutes(-20), Fanout(AlertChannelOutcome.NotAttempted, muted: true), muted: true);
        await RecordAsync("Deadlocks Detected", Now.AddMinutes(-15), AlertDelivery.RoutedToDigest());
        await RecordAsync("Blocking Wait Time", Now.AddMinutes(-10), Fanout(AlertChannelOutcome.Failed, "smtp down"));

        Assert.Null(await FindAsync());
    }

    [Fact]
    public async Task AnotherServersAlert_IsNotOpenHere()
    {
        await RecordAsync("High CPU", Now.AddMinutes(-20), Delivered, serverId: ServerId + 1);

        Assert.Null(await FindAsync());
    }

    [Fact]
    public async Task TheNewestOpenAlertWins_AndAFailedKeyReadsNull()
    {
        await RecordAsync("High CPU", Now.AddMinutes(-30), Delivered);
        await RecordAsync("Deadlocks Detected", Now.AddMinutes(-10), Delivered);

        Assert.Equal("Deadlocks Detected", (await FindAsync())!.Metric);
        Assert.Null(await new DuckDbAlertHistoryStore(_duckDb).FindOpenEngineAlertAsync("not-a-server", Now));
    }

    [Fact]
    public async Task TheAlertsDetail_ListsExactlyTheFindingsAttachedToIt()
    {
        var firedAt = AlertHistoryEscalationSeed.TruncateToMicroseconds(Now.AddMinutes(-30));
        await RecordAsync("High CPU", firedAt, Delivered);
        string Held(string metric, DateTime at) => AlertContextSerializer.Serialize(
            new AlertContext { AttachedTo = new AlertAttachedToDto(metric, at), Routing = new AlertRoutingDto("digest", "attached") });
        await RecordAsync("Analysis: held one", firedAt.AddMinutes(5), AlertDelivery.RoutedToDigest(), contextJson: Held("High CPU", firedAt));
        /* Attached to a different alert, and a plain digest row with no attachment: neither is listed. */
        await RecordAsync("Analysis: other alert", firedAt.AddMinutes(6), AlertDelivery.RoutedToDigest(), contextJson: Held("High CPU", firedAt.AddMinutes(-90)));
        await RecordAsync("Analysis: plain digest", firedAt.AddMinutes(7), AlertDelivery.RoutedToDigest(),
            contextJson: AlertContextSerializer.Serialize(new AlertContext()));

        var service = new LocalDataService(_duckDb);
        var rows = await service.GetAlertHistoryAsync(24, 50, ServerId);
        var alert = Assert.Single(rows, r => r.MetricName == "High CPU");

        var lines = await service.GetAttachedFindingsAsync(alert);

        var line = Assert.Single(lines);
        Assert.EndsWith("Analysis: held one", line);
        Assert.Empty(await service.GetAttachedFindingsAsync(new AlertHistoryRow { MetricName = "Failed Jobs", ServerId = ServerId, AlertTime = firedAt }));
    }
}
