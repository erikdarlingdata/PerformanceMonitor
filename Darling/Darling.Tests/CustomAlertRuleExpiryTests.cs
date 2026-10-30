/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6235: what a caller's expiry means, with no store. A new expiry must be in the future; the rule's own stored expiry sent back is not a
/// change and is never refused (so an already-expired rule can still be renamed); an empty value clears it; an omitted one keeps it. An
/// expired rule's open incident is torn down like a disabled rule's, because nothing evaluates it to resolve it.
/// </summary>
public sealed class CustomAlertRuleExpiryTests
{
    private static readonly DateTime Now = new(2026, 10, 20, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void Create_RefusesAPastExpiry_AcceptsAFutureOne_AndTreatsEmptyAsNone()
    {
        var past = DarlingMcpCustomAlertTools.ResolveCreateExpiry(DateTime.UtcNow.AddMinutes(-1).ToString("o"));
        Assert.Null(past.Expiry);
        Assert.Contains("in the past", past.Error);

        var future = DarlingMcpCustomAlertTools.ResolveCreateExpiry(DateTime.UtcNow.AddHours(2).ToString("o"));
        Assert.Null(future.Error);
        Assert.NotNull(future.Expiry);
        Assert.Equal(DateTimeKind.Utc, future.Expiry!.Value.Kind);

        Assert.Equal((null, null), DarlingMcpCustomAlertTools.ResolveCreateExpiry(null));
        Assert.Equal((null, null), DarlingMcpCustomAlertTools.ResolveCreateExpiry(""));
        Assert.NotNull(DarlingMcpCustomAlertTools.ResolveCreateExpiry("not a time").Error);
    }

    [Fact]
    public void Update_OmittedKeeps_EmptyClears_AndOnlyAChangeMustBeInTheFuture()
    {
        var stored = DateTime.UtcNow.AddHours(-3); // the rule has expired
        stored = new DateTime(stored.Ticks - (stored.Ticks % 10), DateTimeKind.Utc);

        Assert.Equal((stored, (string?)null), DarlingMcpCustomAlertTools.ResolveUpdateExpiry(null, stored));
        Assert.Equal(((DateTime?)null, (string?)null), DarlingMcpCustomAlertTools.ResolveUpdateExpiry("", stored));

        // Sending the stored (past) instant back is not a change.
        Assert.Null(DarlingMcpCustomAlertTools.ResolveUpdateExpiry(stored.ToString("o"), stored).Error);

        // Moving it to a different past instant is refused; moving it later is accepted.
        Assert.Contains("in the past", DarlingMcpCustomAlertTools.ResolveUpdateExpiry(stored.AddHours(-1).ToString("o"), stored).Error);
        Assert.Null(DarlingMcpCustomAlertTools.ResolveUpdateExpiry(DateTime.UtcNow.AddDays(1).ToString("o"), stored).Error);
    }

    [Fact]
    public void ARule_IsExpiredAtItsExpiryInstant_NotBefore_AndNeverWithoutOne()
    {
        var rule = new CustomAlertRule(1, "r", "{}", null, true, 1, Now, Now, null, Now.AddHours(1));
        Assert.False(rule.IsExpiredAt(Now));
        Assert.True(rule.IsExpiredAt(Now.AddHours(1)));
        Assert.True(rule.IsExpiredAt(Now.AddHours(2)));
        Assert.False((rule with { ExpiresAtUtc = null }).IsExpiredAt(Now.AddYears(50)));
    }

    [Fact]
    public void Teardown_AnExpiredRulesOpenIncidentIsResolved_AndDisabledStillWins()
    {
        Assert.Equal(CustomAlertEvaluator.TeardownReasonExpired,
            CustomAlertEvaluator.ClassifyStateTeardown(true, null, null, 1, "S1", ruleExpired: true));
        Assert.Equal(CustomAlertEvaluator.TeardownReasonDisabled,
            CustomAlertEvaluator.ClassifyStateTeardown(false, null, null, 1, "S1", ruleExpired: true));
        Assert.Null(CustomAlertEvaluator.ClassifyStateTeardown(true, null, null, 1, "S1"));
    }
}
