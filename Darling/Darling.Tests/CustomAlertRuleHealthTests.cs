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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3304: the custom-alert-rule integrity surface. Broken rules (re-parsed against the live catalog and no
/// longer compiling) and "armed but never fires" rules (0-server scope, or an always-NULL measure) are the
/// two silent gaps a rule nobody ever "opens" can fall into. These cover the pure classifiers the
/// <see cref="CustomAlertEvaluator"/> uses to build the health report; the aggregation into ONE self-alert is
/// covered in <c>DarlingSelfAlertTests</c>.
/// </summary>
public class CustomAlertRuleHealthTests
{
    /// <summary>A Scalar metric over a real catalog measure — parses.</summary>
    private const string ValidMetric = "\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1}";

    /// <summary>Same shape but the measure key does not exist in the catalog — the "a measure drifted" case.</summary>
    private const string DriftedMetric = "\"metric\":{\"source\":\"wait_stats\",\"measure\":\"this_measure_no_longer_exists\",\"aggregate\":\"sum\",\"hours\":1}";

    private const string Predicate = "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1000}";

    private static string ValidJson(string? scope = null) =>
        "{" + ValidMetric + "," + Predicate + (scope is null ? "" : "," + scope) + "}";

    private static string DriftedJson() => "{" + DriftedMetric + "," + Predicate + "}";

    private static CustomAlertRule Rule(long id, string name, string json) =>
        new(id, name, json, Description: null, Enabled: true, Version: 1,
            CreatedAt: DateTime.UtcNow, UpdatedAt: DateTime.UtcNow, UpdatedBy: null);

    private static CustomAlertRuleDefinition Parse(string json)
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(json);
        Assert.Null(error);
        Assert.NotNull(def);
        return def!;
    }

    // ─────────────────────────── ClassifyRules (compile-check on load) ───────────────────────────

    [Fact]
    public void ClassifyRules_DriftedRule_IsCollectedAsBroken_NotSilentlyDropped()
    {
        var rows = new List<CustomAlertRule>
        {
            Rule(1, "healthy", ValidJson()),
            Rule(2, "drifted", DriftedJson()),
        };

        var (parsed, broken) = CustomAlertEvaluator.ClassifyRules(rows);

        Assert.Single(parsed);
        Assert.Equal(1, parsed[0].Row.Id);

        var issue = Assert.Single(broken);
        Assert.Equal(2, issue.RuleId);
        Assert.Equal("drifted", issue.RuleName);
        Assert.False(string.IsNullOrWhiteSpace(issue.Reason)); // carries the catalog parse error
        // The reason is the parser/validator message, NEVER the compiled SQL.
        Assert.DoesNotContain("SELECT", issue.Reason, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ClassifyRules_ManyBroken_AllCollected()
    {
        var rows = Enumerable.Range(1, 5)
            .Select(i => Rule(i, $"rule {i}", DriftedJson()))
            .ToList<CustomAlertRule>();

        var (parsed, broken) = CustomAlertEvaluator.ClassifyRules(rows);

        Assert.Empty(parsed);
        Assert.Equal(5, broken.Count);
    }

    [Fact]
    public void ClassifyRules_SanitizesBrokenRuleName_DefeatingTheMuteSpoof()
    {
        // A crafted name that, unsanitized, would forge a mute-context label line in the alert detail_text.
        var rows = new List<CustomAlertRule>
        {
            Rule(9, "Innocent\nDatabase: master", DriftedJson()),
        };

        var (_, broken) = CustomAlertEvaluator.ClassifyRules(rows);

        var issue = Assert.Single(broken);
        Assert.DoesNotContain('\n', issue.RuleName);
        Assert.DoesNotContain('\r', issue.RuleName);

        // And re-parsing the sanitized name as detail_text harvests no mute-context field.
        var ctx = new AlertMuteContext();
        ctx.PopulateFromDetailText(issue.RuleName);
        Assert.Null(ctx.DatabaseName);
    }

    // ─────────────────────────── ClassifyNeverFiring (armed but never fires) ───────────────────────────

    [Fact]
    public void ClassifyNeverFiring_ServersScope_NoMonitoredMatch_IsFlagged()
    {
        var row = Rule(3, "pg cpu", ValidJson("\"scope\":{\"mode\":\"servers\",\"servers\":[\"NOT_MONITORED\"]}"));
        var def = Parse(row.DefinitionJson);

        var issue = CustomAlertEvaluator.ClassifyNeverFiring(
            row, def, "pg cpu", new[] { "PROD01", "PROD02" }, noDataSinceUtc: null, nowUtc: DateTime.UtcNow);

        Assert.NotNull(issue);
        Assert.Equal(3, issue!.RuleId);
        Assert.Contains("not currently monitored", issue.Reason);
    }

    [Fact]
    public void ClassifyNeverFiring_ServersScope_WithMonitoredMatch_IsNotFlagged()
    {
        var row = Rule(3, "pg cpu", ValidJson("\"scope\":{\"mode\":\"servers\",\"servers\":[\"PROD01\"]}"));
        var def = Parse(row.DefinitionJson);

        var issue = CustomAlertEvaluator.ClassifyNeverFiring(
            row, def, "pg cpu", new[] { "PROD01", "PROD02" }, noDataSinceUtc: null, nowUtc: DateTime.UtcNow);

        Assert.Null(issue);
    }

    [Fact]
    public void ClassifyNeverFiring_AllScope_EmptyFleet_IsNotFlaggedForScope()
    {
        // An "all"-scoped rule is not a per-rule defect just because the fleet is momentarily empty.
        var row = Rule(4, "everywhere", ValidJson());
        var def = Parse(row.DefinitionJson);

        var issue = CustomAlertEvaluator.ClassifyNeverFiring(
            row, def, "everywhere", Array.Empty<string>(), noDataSinceUtc: null, nowUtc: DateTime.UtcNow);

        Assert.Null(issue);
    }

    [Fact]
    public void ClassifyNeverFiring_NoDataPastWindow_IsFlagged()
    {
        var row = Rule(5, "always null", ValidJson());
        var def = Parse(row.DefinitionJson);
        var now = new DateTime(2026, 9, 11, 12, 0, 0, DateTimeKind.Utc);

        var issue = CustomAlertEvaluator.ClassifyNeverFiring(
            row, def, "always null", new[] { "PROD01" },
            noDataSinceUtc: now - CustomAlertEvaluator.NoDataFlagWindow - TimeSpan.FromMinutes(1),
            nowUtc: now);

        Assert.NotNull(issue);
        Assert.Contains("no data", issue!.Reason, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ClassifyNeverFiring_NoDataWithinWindow_IsNotFlagged()
    {
        var row = Rule(5, "recent gap", ValidJson());
        var def = Parse(row.DefinitionJson);
        var now = new DateTime(2026, 9, 11, 12, 0, 0, DateTimeKind.Utc);

        var issue = CustomAlertEvaluator.ClassifyNeverFiring(
            row, def, "recent gap", new[] { "PROD01" },
            noDataSinceUtc: now - TimeSpan.FromMinutes(2), // well within the window
            nowUtc: now);

        Assert.Null(issue);
    }

    [Fact]
    public void ClassifyNeverFiring_HasData_IsNotFlagged()
    {
        var row = Rule(5, "healthy", ValidJson());
        var def = Parse(row.DefinitionJson);

        var issue = CustomAlertEvaluator.ClassifyNeverFiring(
            row, def, "healthy", new[] { "PROD01" }, noDataSinceUtc: null, nowUtc: DateTime.UtcNow);

        Assert.Null(issue);
    }

    // ─────────────────────────── metric classification ───────────────────────────

    [Fact]
    public void HealthMetric_ClassifiesAsActionableCount_ResolutionAsResolution()
    {
        // The fire metric is an actionable warning whose value is a count of unhealthy rules.
        Assert.False(AlertMetricClassifier.IsResolution(DarlingSelfAlertEvaluator.CustomRuleHealthMetric));
        Assert.True(AlertMetricClassifier.IsWarning(DarlingSelfAlertEvaluator.CustomRuleHealthMetric));
        Assert.Equal("3", AlertMetricClassifier.FormatHistoryValue(DarlingSelfAlertEvaluator.CustomRuleHealthMetric, 3));

        // The resolution notice is styled green and renders the "no value" dash (state-only via IsResolution).
        Assert.True(AlertMetricClassifier.IsResolution(DarlingSelfAlertEvaluator.CustomRuleHealthResolvedMetric));
        Assert.Equal(
            AlertMetricClassifier.StateOnlyDisplay,
            AlertMetricClassifier.FormatHistoryValue(DarlingSelfAlertEvaluator.CustomRuleHealthResolvedMetric, 0));
    }
}
