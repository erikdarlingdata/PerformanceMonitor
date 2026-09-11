/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The custom-alert rule definition parser/validator (#3285). The metric is validated by the compose
/// <c>TryParsePanel</c> authority (so these use a real catalog measure — wait_stats/wait_time_ms) and pinned
/// to Scalar; the predicate/hysteresis/scope/window rules are validated here.
/// </summary>
public class CustomAlertRuleDefinitionTests
{
    /// <summary>A valid Scalar metric over a real catalog measure.</summary>
    private const string Metric = "\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1}";

    [Fact]
    public void MinimalRule_Parses_WithDefaults()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1000}}");

        Assert.Null(error);
        Assert.NotNull(def);
        Assert.Equal(CustomAlertOp.GreaterOrEqual, def!.Op);
        Assert.Equal(1000, def.WarnThreshold);
        Assert.Null(def.CriticalThreshold);
        Assert.Equal(1, def.BreachSamples);
        Assert.Equal(1, def.ClearSamples);
        Assert.Equal(CustomAlertScopeMode.All, def.ScopeMode);
        Assert.Equal(1.0, def.WindowHours);
    }

    [Fact]
    public void FullRule_Parses_AllFields()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":25,\"criticalThreshold\":40}," +
            "\"hysteresis\":{\"breachSamples\":3,\"clearSamples\":2}," +
            "\"scope\":{\"mode\":\"servers\",\"servers\":[\"PROD-A\",\"PROD-B\"]}," +
            "\"evaluationIntervalSeconds\":120}");

        Assert.Null(error);
        Assert.NotNull(def);
        Assert.Equal(40, def!.CriticalThreshold);
        Assert.Equal(3, def.BreachSamples);
        Assert.Equal(2, def.ClearSamples);
        Assert.Equal(CustomAlertScopeMode.Servers, def.ScopeMode);
        Assert.Equal(120, def.EvaluationIntervalSeconds);
        Assert.True(def.AppliesTo("prod-a"));   // case-insensitive
        Assert.False(def.AppliesTo("PROD-C"));
    }

    [Fact]
    public void ScopeAll_AppliesToEveryServer()
    {
        var (def, _) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"gt\",\"warnThreshold\":1}}");
        Assert.True(def!.AppliesTo("anything"));
    }

    [Theory]
    [InlineData(20, false, AlertSeverityLevel.Warning)] // below warn
    [InlineData(30, true, AlertSeverityLevel.Warning)]  // over warn, under critical
    [InlineData(45, true, AlertSeverityLevel.Critical)] // over critical
    public void PredicateAndSeverity_GreaterOrEqual(double value, bool breaching, AlertSeverityLevel severity)
    {
        var (def, _) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":25,\"criticalThreshold\":40}}");

        Assert.Equal(breaching, def!.IsBreaching(value));
        if (breaching)
        {
            Assert.Equal(severity, def.SeverityFor(value));
        }
    }

    [Fact]
    public void MissingMetric_IsError()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse("{\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Fact]
    public void MissingPredicate_IsError()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse("{" + Metric + "}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Fact]
    public void UnknownOp_IsError()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"between\",\"warnThreshold\":1}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Fact]
    public void NonScalarMetric_IsError()
    {
        // A timeBucket makes the compose panel a time series, not a single value.
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"hours\":1}," +
            "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Fact]
    public void CriticalNotMoreExtremeThanWarn_IsError()
    {
        // op ge means higher is worse, so critical must be >= warn.
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":40,\"criticalThreshold\":25}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Fact]
    public void WindowBeyondCeiling_IsError()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":100}," +
            "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Fact]
    public void TagScope_IsRejectedForNow()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}," +
            "\"scope\":{\"mode\":\"tag\",\"tag\":\"prod\"}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Fact]
    public void HysteresisBelowOne_IsError()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1},\"hysteresis\":{\"breachSamples\":0}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Fact]
    public void ServersScopeWithEmptyList_IsError()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1},\"scope\":{\"mode\":\"servers\",\"servers\":[]}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }
}
