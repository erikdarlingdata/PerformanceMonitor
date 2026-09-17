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
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pure pins for the enabled-rule count cap (#3285, Round-2): the cap constant sits in the sanctioned range,
/// and the evaluator's hard ceiling (<see cref="CustomAlertEvaluator.ApplyEnabledCeiling"/>) trims an over-cap
/// enabled set down to exactly the cap (deterministic id order) and WARNs, while leaving an at-or-under-cap set
/// untouched and silent. The write-path cap's count/race behavior needs a real store, so it lives in the gated
/// <c>CustomAlertRuleCapLiveTests</c>.
/// </summary>
public sealed class CustomAlertRuleCapTests
{
    private const int Cap = CustomAlertRuleStore.EnabledRuleCap;

    private static CustomAlertRule EnabledRule(long id) => new(
        id, "rule-" + id, "{}", Description: null, Enabled: true, Version: 1,
        CreatedAt: DateTime.UtcNow, UpdatedAt: DateTime.UtcNow, UpdatedBy: "test");

    private static IReadOnlyList<CustomAlertRule> EnabledRules(int count) =>
        Enumerable.Range(1, count).Select(i => EnabledRule(i)).ToList();

    [Fact]
    public void EnabledRuleCap_IsInTheSanctionedRange()
    {
        // The plan requires "a concrete number"; the coordinator sanctioned 100-250 (100 is the plan's number).
        Assert.InRange(CustomAlertRuleStore.EnabledRuleCap, 100, 250);
    }

    [Fact]
    public void ApplyEnabledCeiling_ReturnsAllRows_WhenUnderTheCap()
    {
        var logger = new CapturingTestLogger();
        var rows = EnabledRules(Cap - 1);

        var result = CustomAlertEvaluator.ApplyEnabledCeiling(rows, logger);

        Assert.Equal(Cap - 1, result.Count);
        Assert.DoesNotContain("Warning", logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public void ApplyEnabledCeiling_AtExactlyTheCap_IsNotTrimmedOrWarned()
    {
        var logger = new CapturingTestLogger();
        var rows = EnabledRules(Cap);

        var result = CustomAlertEvaluator.ApplyEnabledCeiling(rows, logger);

        Assert.Equal(Cap, result.Count);
        Assert.DoesNotContain("Warning", logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public void ApplyEnabledCeiling_TrimsToTheCap_AndWarns_WhenMoreEnabledRowsExist()
    {
        var logger = new CapturingTestLogger();
        var rows = EnabledRules(Cap + 5); // ids 1..Cap+5, already in the id order the store reads them in

        var result = CustomAlertEvaluator.ApplyEnabledCeiling(rows, logger);

        // Evaluates at most the cap, and deterministically the first Cap by id.
        Assert.Equal(Cap, result.Count);
        Assert.Equal(Enumerable.Range(1, Cap).Select(i => (long)i), result.Select(r => r.Id));

        // The over-cap condition is surfaced (WARN), not silently swallowed.
        Assert.Contains("Warning", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("ceiling", logger.Joined, StringComparison.OrdinalIgnoreCase);
    }
}
