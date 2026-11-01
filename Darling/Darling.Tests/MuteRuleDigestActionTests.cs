// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using Microsoft.Extensions.Logging.Abstractions;
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
}
