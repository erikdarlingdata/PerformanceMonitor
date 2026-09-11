/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3299: the evaluate-now half of <c>test_custom_alert_rule</c>. The value computation itself is the shared
/// <see cref="CustomAlertEvaluator.EvaluateScalarNowAsync"/> seam (exercised live in
/// <c>CustomAlertEvaluateNowLiveTests</c>); these pin the pure verdict mapping
/// (<see cref="CustomAlertEvaluator.ClassifyTestValue"/>) and the tool's input handling — that a bad
/// definition returns the validator error rather than throwing, and that exactly one of rule_id/definition is
/// required. All are DB-free: the input-guard paths return before the tool ever touches the store.
/// </summary>
public class CustomAlertEvaluateNowTests
{
    /// <summary>op ge, warn 1000, critical 2000.</summary>
    private static CustomAlertRuleDefinition TieredRule() => Parse(
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1000,\"criticalThreshold\":2000}}");

    private static CustomAlertRuleDefinition WarnOnlyRule() => Parse(
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1000}}");

    private static CustomAlertRuleDefinition Parse(string json)
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(json);
        Assert.Null(error);
        Assert.NotNull(def);
        return def!;
    }

    // ─────────────────────────── ClassifyTestValue (would-fire verdict) ───────────────────────────

    [Fact]
    public void BelowThreshold_WouldNotFire()
    {
        var (breaching, severity) = CustomAlertEvaluator.ClassifyTestValue(TieredRule(), 500);
        Assert.False(breaching);
        Assert.Null(severity);
    }

    [Fact]
    public void AboveWarnBelowCritical_WouldFire_Warning()
    {
        var (breaching, severity) = CustomAlertEvaluator.ClassifyTestValue(TieredRule(), 1500);
        Assert.True(breaching);
        Assert.Equal(AlertSeverityLevel.Warning, severity);
    }

    [Fact]
    public void AboveCritical_WouldFire_Critical()
    {
        var (breaching, severity) = CustomAlertEvaluator.ClassifyTestValue(TieredRule(), 2500);
        Assert.True(breaching);
        Assert.Equal(AlertSeverityLevel.Critical, severity);
    }

    [Fact]
    public void WarnOnlyRule_AboveThreshold_WouldFire_Warning()
    {
        var (breaching, severity) = CustomAlertEvaluator.ClassifyTestValue(WarnOnlyRule(), 999999);
        Assert.True(breaching);
        Assert.Equal(AlertSeverityLevel.Warning, severity);
    }

    [Fact]
    public void NoData_IsNeverABreach()
    {
        var (breaching, severity) = CustomAlertEvaluator.ClassifyTestValue(TieredRule(), null);
        Assert.False(breaching);
        Assert.Null(severity);
    }

    // ─────────────────────────── tool input handling (DB-free early returns) ───────────────────────────

    /// <summary>A data source that is created but never opened — the input-guard paths return before any query,
    /// so no connection is ever made.</summary>
    private static NpgsqlDataSource UnusedDataSource() => NpgsqlDataSource.Create("Host=127.0.0.1;Username=none;Database=none");

    private static JsonObject ParseResponse(string json) => (JsonObject)JsonNode.Parse(json)!;

    [Fact]
    public async Task BadDefinition_ReturnsTheValidatorError_NotAThrow()
    {
        await using var ds = UnusedDataSource();

        // '{}' is well-formed JSON but not a valid rule (no metric) — TryParse returns the validator message.
        var response = ParseResponse(await DarlingMcpCustomAlertTools.TestCustomAlertRule(ds, rule_id: null, definition: "{}"));

        Assert.Equal("invalid", (string?)response["status"]);
        Assert.Contains("metric", (string?)response["message"]!, System.StringComparison.Ordinal);
    }

    [Fact]
    public async Task BothRuleIdAndDefinition_IsRejected()
    {
        await using var ds = UnusedDataSource();

        var response = ParseResponse(await DarlingMcpCustomAlertTools.TestCustomAlertRule(ds, rule_id: 1, definition: "{}"));

        Assert.Equal("invalid", (string?)response["status"]);
        Assert.Contains("exactly one", (string?)response["message"]!, System.StringComparison.Ordinal);
    }

    [Fact]
    public async Task NeitherRuleIdNorDefinition_IsRejected()
    {
        await using var ds = UnusedDataSource();

        var response = ParseResponse(await DarlingMcpCustomAlertTools.TestCustomAlertRule(ds, rule_id: null, definition: null));

        Assert.Equal("invalid", (string?)response["status"]);
        Assert.Contains("exactly one", (string?)response["message"]!, System.StringComparison.Ordinal);
    }
}
