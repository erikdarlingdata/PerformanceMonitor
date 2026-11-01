// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7083 part 3: a mute rule's action. A digest rule never mutes. Inside its window it holds a matching fire for the daily digest, outside
/// the window the fire pages, and a mute rule that also matches wins.
/// </summary>
public sealed class MuteRuleDigestActionTests
{
    private sealed class MemoryStore : IMuteRuleStore
    {
        public System.Collections.Generic.List<MuteRule> Rules { get; } = new();
        public System.Threading.Tasks.Task<System.Collections.Generic.IReadOnlyList<MuteRule>> LoadAllAsync(System.Threading.CancellationToken cancellationToken = default) =>
            System.Threading.Tasks.Task.FromResult<System.Collections.Generic.IReadOnlyList<MuteRule>>(Rules);
        public System.Threading.Tasks.Task InsertAsync(MuteRule rule) { Rules.Add(rule); return System.Threading.Tasks.Task.CompletedTask; }
        public System.Threading.Tasks.Task UpdateAsync(MuteRule rule) => System.Threading.Tasks.Task.CompletedTask;
        public System.Threading.Tasks.Task DeleteAsync(string ruleId) => System.Threading.Tasks.Task.CompletedTask;
        public System.Threading.Tasks.Task SetEnabledAsync(string ruleId, bool enabled) => System.Threading.Tasks.Task.CompletedTask;
        public System.Threading.Tasks.Task DeleteExpiredAsync(System.Collections.Generic.IReadOnlyList<string> ids) => System.Threading.Tasks.Task.CompletedTask;
    }

    private static readonly AlertMuteContext Blocking = new() { ServerName = "sql-a", ServerId = 7, MetricName = "Blocking Detected" };

    private static MuteRule DigestRule(bool allDay = true) => new()
    {
        Action = MuteRuleActions.Digest,
        MetricName = "Blocking Detected",
        ServerId = 7,
        Reason = "nightly delete",
        RecurStartTime = allDay ? new TimeOnly(0, 0) : new TimeOnly(3, 0),
        RecurEndTime = allDay ? new TimeOnly(23, 59) : new TimeOnly(3, 1),
        RecurTimeZone = "UTC",
    };

    private static MuteRuleService ServiceWith(params MuteRule[] rules)
    {
        var store = new MemoryStore();
        store.Rules.AddRange(rules);
        var service = new MuteRuleService(store, NullLogger<MuteRuleService>.Instance);
        service.LoadAsync().GetAwaiter().GetResult();
        return service;
    }

    [Fact]
    public void ADigestRule_DoesNotMute_ButHoldsTheFireInsideItsWindow()
    {
        var service = ServiceWith(DigestRule());

        Assert.False(service.IsAlertMuted(Blocking));
        Assert.True(service.IsInDigestWindow(Blocking));
        var judgment = MuteJudgment.Judge(service.IsAlertMuted, service.DigestWindowReason, Blocking);
        Assert.False(judgment.Muted);
        Assert.NotNull(judgment.Hold);
        Assert.Contains("nightly delete", judgment.Hold!.Reason);
        Assert.True(judgment.Silenced);
    }

    [Fact]
    public void ADigestRule_ForAnotherMetric_HoldsNothing()
    {
        var service = ServiceWith(DigestRule());
        var other = new AlertMuteContext { ServerName = "sql-a", ServerId = 7, MetricName = "High CPU" };

        var judgment = MuteJudgment.Judge(service.IsAlertMuted, service.DigestWindowReason, other);

        Assert.False(judgment.Silenced);
    }

    [Fact]
    public void ADigestRule_OutsideItsWindow_LetsTheFirePage()
    {
        var now = DateTime.UtcNow;
        var rule = DigestRule(allDay: false);
        /* A one-minute window that opened a few hours ago and closed: the clock is outside it. */
        var start = now.AddHours(-5);
        rule.RecurStartTime = TimeOnly.FromDateTime(start);
        rule.RecurEndTime = TimeOnly.FromDateTime(start.AddMinutes(1));
        var service = ServiceWith(rule);

        var judgment = MuteJudgment.Judge(service.IsAlertMuted, service.DigestWindowReason, Blocking);

        Assert.False(judgment.Silenced);
    }

    [Fact]
    public void AMuteRuleAndADigestRuleBothMatching_TheFireIsMuted_NotHeld()
    {
        var mute = new MuteRule { MetricName = "Blocking Detected", ServerId = 7 };
        var service = ServiceWith(DigestRule(), mute);

        var judgment = MuteJudgment.Judge(service.IsAlertMuted, service.DigestWindowReason, Blocking);

        Assert.True(judgment.Muted);
        Assert.Null(judgment.Hold);
    }

    [Fact]
    public void WithNoDigestDelegate_ALiteHost_NeverHolds()
    {
        var judgment = MuteJudgment.Judge(_ => false, null, Blocking);

        Assert.False(judgment.Silenced);
    }

    [Theory]
    [InlineData(null, "mute")]
    [InlineData("", "mute")]
    [InlineData("Digest", "digest")]
    [InlineData(" digest ", "digest")]
    [InlineData("something else", "mute")]
    public void NormalizeReadsOnlyDigestAsDigest(string? text, string expected) =>
        Assert.Equal(expected, MuteRuleActions.Normalize(text));

    [Fact]
    public void CloneKeepsTheAction() =>
        Assert.True(DigestRule().Clone().IsDigestAction);

    /* ---- the engines: a digest rule's window holds the real fire, recorded through the real deliverer ---- */

    private static MuteRule CpuDigestRule(bool insideWindow)
    {
        var rule = new MuteRule { Action = MuteRuleActions.Digest, MetricName = "High CPU", Reason = "batch window", RecurTimeZone = "UTC" };
        if (insideWindow)
        {
            rule.RecurStartTime = new TimeOnly(0, 0);
            rule.RecurEndTime = new TimeOnly(23, 59);
        }
        else
        {
            /* A one-minute window that closed hours ago. */
            var start = DateTime.UtcNow.AddHours(-5);
            rule.RecurStartTime = TimeOnly.FromDateTime(start);
            rule.RecurEndTime = TimeOnly.FromDateTime(start.AddMinutes(1));
        }

        return rule;
    }

    private static (DarlingAlertDeliverer Deliverer, CapturingHistoryStore History) RealDeliverer(
        CapturingWebhookEndpoint endpoint, CapturingSmtpEndpoint smtp)
    {
        var config = new DarlingConfig();
        config.Webhooks.TeamsUrl = endpoint.Url;
        config.Smtp.Host = "127.0.0.1";
        config.Smtp.Port = smtp.Port;
        config.Smtp.UseSsl = false;
        config.Smtp.From = "monitor@example.invalid";
        config.Smtp.To = "operator@example.invalid";
        var settings = new DarlingAlertSettings(config);
        var history = new CapturingHistoryStore();
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        return (new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance), history);
    }

    private static async Task DriveCpuAsync(AlertEngine engine)
    {
        var at = AlertEngineTests.Harness.SampleBase;
        for (var i = 0; i < AlertEngine.CpuBreachSamples; i++)
        {
            at = at.AddMinutes(1);
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot(sqlCpu: 70, totalCpu: 90, cpuSampleTime: at));
        }
    }

    [Fact]
    public async Task AHighCpuFire_InsideADigestWindow_IsOneDigestRowNamingTheRule_AndSendsNothing()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();
        var (deliverer, history) = RealDeliverer(endpoint, smtp);
        var service = ServiceWith(CpuDigestRule(insideWindow: true));
        var h = new AlertEngineTests.Harness();
        h.Settings.CpuEnabled = true;
        var engine = new AlertEngine(
            h.Settings, h.Adapter, h.StateStore, deliverer, service.IsAlertMuted,
            utcNow: () => h.Now, digestWindowReason: service.DigestWindowReason);

        await DriveCpuAsync(engine);

        Assert.Empty(endpoint.Bodies);
        Assert.Empty(smtp.Messages);
        var row = Assert.Single(history.Records);
        Assert.False(row.AlertSent);
        Assert.False(row.Muted);
        Assert.Equal(AlertDelivery.ChannelDigest, row.NotificationType);
        var routing = AlertContextSerializer.TryReadRouting(row.ContextJson);
        Assert.NotNull(routing);
        Assert.Equal("digest", routing!.Route);
        Assert.Contains("batch window", routing.Reason);
    }

    [Fact]
    public async Task AHighCpuFire_OutsideTheDigestWindow_Pages()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();
        var (deliverer, history) = RealDeliverer(endpoint, smtp);
        var service = ServiceWith(CpuDigestRule(insideWindow: false));
        var h = new AlertEngineTests.Harness();
        h.Settings.CpuEnabled = true;
        var engine = new AlertEngine(
            h.Settings, h.Adapter, h.StateStore, deliverer, service.IsAlertMuted,
            utcNow: () => h.Now, digestWindowReason: service.DigestWindowReason);

        await DriveCpuAsync(engine);

        Assert.Single(endpoint.Bodies);
        Assert.Single(smtp.Messages);
        var row = Assert.Single(history.Records);
        Assert.True(row.AlertSent);
        Assert.NotEqual(AlertDelivery.ChannelDigest, row.NotificationType);
    }

    [Fact]
    public async Task AHeldFire_StampsTheCooldown_SoTheNextSampleAfterTheWindowClosesStaysQuiet()
    {
        var service = ServiceWith(CpuDigestRule(insideWindow: true));
        var h = new AlertEngineTests.Harness();
        h.Settings.CpuEnabled = true;
        var windowOpen = true;
        var engine = new AlertEngine(
            h.Settings, h.Adapter, h.StateStore, h.Deliverer, service.IsAlertMuted,
            utcNow: () => h.Now, digestWindowReason: ctx => windowOpen ? service.DigestWindowReason(ctx) : null);

        await DriveCpuAsync(engine);
        var held = Assert.Single(h.Deliverer.Outcomes);
        Assert.NotNull(held.Hold);
        Assert.False(held.Muted);
        Assert.Contains("batch window", held.Hold!.Reason);

        /* The window closes inside the cooldown: the held fire stamped it, exactly as a mute does, so the next sample stays quiet. */
        windowOpen = false;
        h.Now = h.Now.AddMinutes(1);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot(sqlCpu: 70, totalCpu: 90));
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task ASelfAlert_InsideADigestWindow_IsHeld_AndAMuteRuleStillWinsOverIt()
    {
        var digest = new MuteRule
        {
            Action = MuteRuleActions.Digest, MetricName = "Collection Stopped", Reason = "maintenance", RecurTimeZone = "UTC",
            RecurStartTime = new TimeOnly(0, 0), RecurEndTime = new TimeOnly(23, 59),
        };
        var service = ServiceWith(digest);
        var h = new DarlingSelfAlertTests.Harness();
        var evaluator = new DarlingSelfAlertEvaluator(
            h.Settings, h.Deliverer, h.History, service.IsAlertMuted, utcNow: () => h.Now, digestWindowReason: service.DigestWindowReason);

        await evaluator.ApplyCollectionStoppedAsync(424242, "SELF-ALERT-SRV", stopped: true, "no recent collection", TestContext.Current.CancellationToken);

        var held = Assert.Single(h.Deliverer.Outcomes);
        Assert.False(held.Muted);
        Assert.NotNull(held.Hold);
        Assert.Contains("maintenance", held.Hold!.Reason);

        var withMute = ServiceWith(digest, new MuteRule { MetricName = "Collection Stopped" });
        var h2 = new DarlingSelfAlertTests.Harness();
        var muted = new DarlingSelfAlertEvaluator(
            h2.Settings, h2.Deliverer, h2.History, withMute.IsAlertMuted, utcNow: () => h2.Now, digestWindowReason: withMute.DigestWindowReason);

        await muted.ApplyCollectionStoppedAsync(424242, "SELF-ALERT-SRV", stopped: true, "no recent collection", TestContext.Current.CancellationToken);

        var mutedOutcome = Assert.Single(h2.Deliverer.Outcomes);
        Assert.True(mutedOutcome.Muted);
        Assert.Null(mutedOutcome.Hold);
    }
}
