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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3916 PR B: the analysis page road waits out a hold-back window and, over
/// <see cref="IAlertSettings.AnalysisPageCap"/>, collapses into ONE summary. An install or restart re-pages
/// every story the fleet holds at once; the window is what makes the cap fleet-wide on a SKU with no cycle.
/// </summary>
public sealed class AnalysisPageCapTests
{
    /// <summary>A corroborated (two-fact, so page-routed) critical finding with its own incident and hash.</summary>
    private static AnalysisFinding Page(int serverId, string hash, double severity = 1.6, int minutesAgo = 0) => new()
    {
        ServerId = serverId,
        ServerName = $"server-{serverId}",
        Category = "anomaly",
        StoryPath = "ANOMALY_CPU_SPIKE → PLAN_REGRESSION",
        StoryPathHash = hash,
        IncidentId = "inc-" + hash,
        Severity = severity,
        Confidence = StoryConfidence.Compute(1, 5, 2),
        FactCount = 2,
        MatchedAmplifiers = 1,
        DefinedAmplifiers = 5,
        RootFactKey = "ANOMALY_CPU_SPIKE",
        RootFactValue = 1.0,
        AnalysisTime = DateTime.UtcNow.AddMinutes(-minutesAgo),
    };

    /// <summary>A lone uncorroborated fact — the digest road under the shipped route.</summary>
    private static AnalysisFinding Single(int serverId, string hash) => new()
    {
        ServerId = serverId,
        ServerName = $"server-{serverId}",
        Category = "anomaly",
        StoryPath = "ANOMALY_CPU_SPIKE",
        StoryPathHash = hash,
        IncidentId = "inc-" + hash,
        Severity = 1.6,
        Confidence = StoryConfidence.Compute(0, 5, 1),
        FactCount = 1,
        MatchedAmplifiers = 0,
        DefinedAmplifiers = 5,
        RootFactKey = "ANOMALY_CPU_SPIKE",
        RootFactValue = 1.0,
        AnalysisTime = DateTime.UtcNow,
    };

    private static AlertDelivery Delivered() =>
        AlertDelivery.FromFanout(
            new EmailFanoutResult(AlertChannelOutcome.Delivered, null, AlertChannelOutcome.NotAttempted, null, AnyChannelConfigured: true),
            muted: false, trayChannelPresent: false);

    private sealed class Sender : IFindingAlertSender
    {
        public List<FindingAlert> Sent { get; } = new();
        public List<IReadOnlyList<FindingAlert>> Summaries { get; } = new();
        public AlertDelivery? Delivery { get; set; } = Delivered();
        public TaskCompletionSource Flushed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) =>
            Task.FromResult<DateTime?>(null);

        public Task<AlertDelivery?> SendFindingAlertAsync(FindingAlert alert)
        {
            Sent.Add(alert);
            Flushed.TrySetResult();
            return Task.FromResult(Delivery);
        }

        public Task<AlertDelivery?> SendFindingSummaryAsync(IReadOnlyList<FindingAlert> named)
        {
            Summaries.Add(named);
            Flushed.TrySetResult();
            return Task.FromResult(Delivery);
        }

        /// <summary>Messages that reached a channel: each individual page, each summary.</summary>
        public int Messages => Sent.Count(a => a.Route == FindingRoute.Page) + Summaries.Count;
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
        public int AnalysisNotifyCooldownMinutes => 360;
        public int AnalysisPageCap => 5;
        public string TriageBaseUrl => "";
    }

    private static AnalysisNotificationService Service(
        Sender sender, Func<string, bool>? silenced = null, Func<int, string, Task<bool>>? muted = null) =>
        new(sender, new Settings(), f => f.ServerId.ToString(), NullLogger<AnalysisNotificationService>.Instance,
            isServerSilenced: silenced, isStoryMuted: muted);

    /// <summary>The summary's rendering, shared by all three SKUs: the heading counts incidents and servers,
    /// and every incident is named with its server, story and severity — newest first, then by severity.</summary>
    [Fact]
    public async Task TheSummary_NamesEveryIncident_NewestFirstThenSeverity_UnderACountingHeading()
    {
        var sender = new Sender();
        using var service = Service(sender);
        /* One clock for every page: Page() reads UtcNow per call, and equal times must tie exactly so the
           severity tie-break is what orders them. */
        var now = DateTime.UtcNow;
        AnalysisFinding At(int serverId, string hash, double severity, int minutesAgo)
        {
            var f = Page(serverId, hash, severity);
            f.AnalysisTime = now.AddMinutes(-minutesAgo);
            return f;
        }
        var pages = new List<AnalysisFinding>
        {
            At(1, "0000000aoldest00", 1.9, 30),
            At(2, "0000000bnewlow00", 1.6, 0),
            At(1, "0000000cnewhigh0", 1.8, 0),
            At(2, "0000000dmiddle00", 1.7, 10),
            At(1, "0000000emiddle00", 1.6, 10),
            At(2, "0000000foldest00", 1.6, 30),
        };
        foreach (var server in pages.GroupBy(p => p.ServerId))
            await service.NotifyAsync(server.ToList());
        await service.FlushPendingAsync();

        var named = Assert.Single(sender.Summaries);
        var (serverName, headline, context) = FindingSummary.Compose(named);

        Assert.Equal("6 incidents across 2 servers", headline);
        Assert.Equal("2 servers", serverName);
        Assert.Equal("1 incident across 1 server", FindingSummary.Heading(1, 1));
        Assert.Equal(6, context.Details.Count);
        Assert.Equal(6, context.Incidents!.Count);

        /* newest first, then severity: 0 min (1.8, 1.6), 10 min (1.7, 1.6), 30 min (1.9, 1.6) */
        Assert.Equal(new[] { "1.80", "1.60", "1.70", "1.60", "1.90", "1.60" },
            context.Details.Select(d => d.Fields.Single(f => f.Label == "Severity").Value).ToArray());
        foreach (var item in context.Details)
        {
            var server = item.Fields.Single(f => f.Label == "Server").Value;
            var story = item.Fields.Single(f => f.Label == "Story").Value;
            Assert.StartsWith("server-", server, StringComparison.Ordinal);
            Assert.Equal("ANOMALY_CPU_SPIKE → PLAN_REGRESSION", story);
            Assert.Equal($"{server} — {story}", item.Heading);
        }
    }

    /// <summary>Pin 1: six fresh pages across two servers in one window → ONE message naming all six, and
    /// every named story is held on the next pass (its bucket was stamped Delivered by the summary).</summary>
    [Fact]
    public async Task SixPagesAcrossTwoServers_OneSummary_NamingAllSix_AndAllHeld()
    {
        var sender = new Sender();
        using var service = Service(sender);
        var pages = Enumerable.Range(0, 6).Select(i => Page(1 + i % 2, $"{i:x8}aaaaaaaa")).ToList();
        foreach (var server in pages.GroupBy(p => p.ServerId))
            await service.NotifyAsync(server.ToList());

        Assert.Equal(0, sender.Messages);
        await service.FlushPendingAsync();

        Assert.Empty(sender.Sent);
        var summary = Assert.Single(sender.Summaries);
        Assert.Equal(6, summary.Count);
        Assert.Equal(6, summary.Select(a => a.MetricName).Distinct(StringComparer.Ordinal).Count());
        Assert.Equal(2, summary.Select(a => a.ServerId).Distinct(StringComparer.Ordinal).Count());

        /* Held: the next pass re-reports all six, and nothing is queued to deliver. */
        foreach (var server in pages.GroupBy(p => p.ServerId))
            await service.NotifyAsync(server.ToList());
        await service.FlushPendingAsync();
        Assert.Single(sender.Summaries);
        Assert.Empty(sender.Sent);
    }

    /// <summary>Pin 2: at the cap, each page is its own message.</summary>
    [Fact]
    public async Task FivePages_FiveMessages()
    {
        var sender = new Sender();
        using var service = Service(sender);
        await service.NotifyAsync(Enumerable.Range(0, 5).Select(i => Page(1, $"{i:x8}bbbbbbbb")).ToList());
        await service.FlushPendingAsync();

        Assert.Equal(5, sender.Sent.Count);
        Assert.Empty(sender.Summaries);
    }

    /// <summary>Pin 4: the window covers the #1581 cold-start stagger with margin, read off both constants.</summary>
    [Fact]
    public void TheWindow_CoversTheColdStartStagger_WithMargin()
    {
        var stagger = TimeSpan.FromSeconds(DarlingWorker.ColdStartSpreadSeconds);
        Assert.True(AnalysisNotificationService.PageHoldBackWindow >= stagger + TimeSpan.FromSeconds(60),
            $"H {AnalysisNotificationService.PageHoldBackWindow} must be >= stagger {stagger} + 60 s margin");
    }

    /// <summary>Pin 5: disposing with pages queued sends nothing and stamps nothing — a later service (the
    /// restart) pages the story fresh; a flush after dispose is inert.</summary>
    [Fact]
    public async Task DisposeWhileQueued_NothingSent_NothingHeld()
    {
        var sender = new Sender();
        var service = Service(sender);
        await service.NotifyAsync(new[] { Page(1, "cccccccc11111111") });
        service.Dispose();
        await service.FlushPendingAsync();
        Assert.Equal(0, sender.Messages);

        /* Same service re-reporting after dispose: dropped (a disposed service queues nothing). */
        await service.NotifyAsync(new[] { Page(1, "cccccccc11111111") });
        await service.FlushPendingAsync();
        Assert.Equal(0, sender.Messages);

        /* The restart: a fresh service with the same (null) seed pages it — nothing held it. */
        using var restarted = Service(sender);
        await restarted.NotifyAsync(new[] { Page(1, "cccccccc11111111") });
        await restarted.FlushPendingAsync();
        Assert.Single(sender.Sent);
    }

    /// <summary>Pin 6: a mute or a silence applied inside the window drops the page at flush, UNSTAMPED — so
    /// lifting it pages the story on the next pass.</summary>
    [Fact]
    public async Task MuteOrSilenceInsideTheWindow_DropsUnstamped()
    {
        var sender = new Sender();
        var mutedHash = "dddddddd22222222";
        var muted = true;
        var silenced = true;
        using var service = Service(sender,
            silenced: id => silenced && id == "2",
            muted: (_, hash) => Task.FromResult(muted && hash == mutedHash));

        /* Queued while neither applies at NotifyAsync time for the muted story (the pipeline's filter is
           upstream); the silence predicate is read at NotifyAsync too, so the silenced server's page is
           queued only once silence is turned on AFTER the enqueue. */
        silenced = false;
        await service.NotifyAsync(new[] { Page(1, mutedHash) });
        await service.NotifyAsync(new[] { Page(2, "eeeeeeee33333333") });
        silenced = true;
        await service.FlushPendingAsync();
        Assert.Equal(0, sender.Messages);

        /* Lifted: both page on the next pass — nothing was stamped. */
        muted = false;
        silenced = false;
        await service.NotifyAsync(new[] { Page(1, mutedHash) });
        await service.NotifyAsync(new[] { Page(2, "eeeeeeee33333333") });
        await service.FlushPendingAsync();
        Assert.Equal(2, sender.Sent.Count);
    }

    /// <summary>Pin 7: the digest road does not wait — it records immediately, no flush involved.</summary>
    [Fact]
    public async Task TheDigestRoad_IsImmediate()
    {
        var sender = new Sender();
        using var service = Service(sender);
        await service.NotifyAsync(new[] { Single(1, "ffffffff44444444") });

        var row = Assert.Single(sender.Sent);
        Assert.Equal(FindingRoute.Digest, row.Route);
    }

    /// <summary>Pin 9: a queued incident re-reported inside the window is one page, not two.</summary>
    [Fact]
    public async Task AReReportInsideTheWindow_IsNotDuplicated()
    {
        var sender = new Sender();
        using var service = Service(sender);
        await service.NotifyAsync(new[] { Page(1, "0000000055555555") });
        await service.NotifyAsync(new[] { Page(1, "0000000055555555") });
        await service.FlushPendingAsync();

        Assert.Single(sender.Sent);
    }

    /// <summary>Pin 10: the timer flushes on its own — with a short window, no explicit flush.</summary>
    [Fact]
    public async Task TheTimer_FlushesOnItsOwn()
    {
        var sender = new Sender();
        using var service = new AnalysisNotificationService(
            sender, new Settings(), f => f.ServerId.ToString(), NullLogger<AnalysisNotificationService>.Instance,
            TimeSpan.FromMilliseconds(50));
        await service.NotifyAsync(new[] { Page(1, "0000000066666666") });

        var done = await Task.WhenAny(sender.Flushed.Task, Task.Delay(TimeSpan.FromSeconds(10)));
        Assert.Same(sender.Flushed.Task, done);
        Assert.Single(sender.Sent);
    }
}
