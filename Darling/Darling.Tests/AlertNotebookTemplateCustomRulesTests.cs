/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for #4223's custom-rule authored template (<c>Custom:&lt;id&gt;</c> -> <c>authored/custom-rule</c>):
/// exact-vs-prefix routing, a scalar rule's panel matching the rule's own plan with time-bucket forced on,
/// a range rule's two-sided band, the deleted-rule and unparseable-definition degrade notes (#2710: never a
/// throw), and <see cref="DarlingWebEndpoints.ValidateNotebookDefinition"/> passing for every shape that
/// isn't already a note.
/// </summary>
public sealed class AlertNotebookTemplateCustomRulesTests
{
    private static readonly DateTime WindowEnd = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowStart = WindowEnd - TimeSpan.FromHours(24);
    private const string AsOf = "2026-01-01T12:00:00Z";

    private const string ScalarDefinitionJson =
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1}," +
        "\"predicate\":{\"op\":\"gt\",\"warnThreshold\":80,\"criticalThreshold\":95}}";

    private const string RangeDefinitionJson =
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1}," +
        "\"predicate\":{\"op\":\"between\",\"lowerBound\":10,\"upperBound\":90}}";

    private static CustomAlertRule RuleWithDefinition(string definitionJson) => new(
        Id: 42,
        Name: "Wait time out of band",
        DefinitionJson: definitionJson,
        Description: null,
        Enabled: true,
        Version: 1,
        CreatedAt: WindowStart,
        UpdatedAt: WindowStart,
        UpdatedBy: null);

    private static AlertNotebookEndpoint.AuthoredContext ContextFor(CustomAlertRule? rule, bool missing) =>
        new(rule, missing, null, false);

    private static JsonArray InvokeCustomRuleTemplate(AlertNotebookEndpoint.AuthoredContext context)
    {
        var resolved = AlertNotebookEndpoint.ResolveAuthored("Custom:42");
        Assert.NotNull(resolved);

        return resolved!.Value.Entry.Invoke(
            "Custom:42", "SRV1", AsOf, WindowStart, WindowEnd, incident: null, row: null,
            status: "Firing", context);
    }

    /* ═══════════════════════════ routing ═══════════════════════════ */

    [Fact]
    public void ResolveAuthored_CustomPrefix_RoutesToCustomRuleTemplate()
    {
        var resolved = AlertNotebookEndpoint.ResolveAuthored("Custom:42");

        Assert.NotNull(resolved);
        Assert.Equal("authored/custom-rule", resolved!.Value.Entry.Id);
        Assert.Equal(AlertNotebookEndpoint.AuthoredContextKind.CustomRule, resolved.Value.Kind);
    }

    [Fact]
    public void ResolveAuthored_ExactMetric_StillWinsOverCustomPrefix()
    {
        // "Blocking Detected" is an exact-name row; the prefix table must never intercept it.
        var resolved = AlertNotebookEndpoint.ResolveAuthored("Blocking Detected");

        Assert.NotNull(resolved);
        Assert.Equal("authored/blocking", resolved!.Value.Entry.Id);
        Assert.Equal(AlertNotebookEndpoint.AuthoredContextKind.None, resolved.Value.Kind);
    }

    [Fact]
    public void ResolveAuthored_CustomWithNoColon_StaysMechanical()
    {
        Assert.Null(AlertNotebookEndpoint.ResolveAuthored("Custom"));
    }

    /* ═══════════════════════════ a scalar rule ═══════════════════════════ */

    [Fact]
    public void ScalarRule_PanelCell_MatchesRulesOwnPlan_WithTimeBucketForced()
    {
        var rule = RuleWithDefinition(ScalarDefinitionJson);
        var context = ContextFor(rule, missing: false);

        var cells = InvokeCustomRuleTemplate(context);

        var panel = cells.OfType<JsonObject>().Single(c => (string?)c["type"] == "panel");
        Assert.Equal("wait_stats", (string?)panel["source"]);
        Assert.Equal("wait_time_ms", (string?)panel["measure"]);
        Assert.Equal("sum", (string?)panel["aggregate"]);
        Assert.Null(panel["ratio"]);
        Assert.Null(panel["filters"]); // the stored rule declares none

        // The stored rule's own plan is Scalar (no timeBucket); the template forces one on.
        Assert.Equal("hour", (string?)panel["timeBucket"]);

        var thresholds = panel["thresholds"]!.AsArray().Select(v => (double)v!).ToArray();
        Assert.Equal(new[] { 80d, 95d }, thresholds);
    }

    [Fact]
    public void ScalarRule_PassesValidateNotebookDefinition()
    {
        var rule = RuleWithDefinition(ScalarDefinitionJson);
        var cells = InvokeCustomRuleTemplate(ContextFor(rule, missing: false));

        var notebook = new JsonObject { ["cells"] = cells };
        var validation = DarlingWebEndpoints.ValidateNotebookDefinition(notebook);

        Assert.True(validation.IsValid, validation.Error);
    }

    /* ═══════════════════════════ a range rule ═══════════════════════════ */

    [Fact]
    public void RangeRule_PanelCell_CarriesBothBounds()
    {
        var rule = RuleWithDefinition(RangeDefinitionJson);
        var context = ContextFor(rule, missing: false);

        var cells = InvokeCustomRuleTemplate(context);

        var panel = cells.OfType<JsonObject>().Single(c => (string?)c["type"] == "panel");
        var thresholds = panel["thresholds"]!.AsArray().Select(v => (double)v!).ToArray();
        Assert.Equal(new[] { 10d, 90d }, thresholds);
    }

    [Fact]
    public void RangeRule_PassesValidateNotebookDefinition()
    {
        var rule = RuleWithDefinition(RangeDefinitionJson);
        var cells = InvokeCustomRuleTemplate(ContextFor(rule, missing: false));

        var notebook = new JsonObject { ["cells"] = cells };
        var validation = DarlingWebEndpoints.ValidateNotebookDefinition(notebook);

        Assert.True(validation.IsValid, validation.Error);
    }

    /* ═══════════════════════════ deleted rule ═══════════════════════════ */

    [Fact]
    public void DeletedRule_ProducesNoteCell_NeverAnError()
    {
        var cells = InvokeCustomRuleTemplate(ContextFor(null, missing: true));

        var note = cells.OfType<JsonObject>().Single(c => (string?)c["type"] == "markdown");
        Assert.Contains("no longer exists", (string?)note["text"], StringComparison.Ordinal);

        // header + status + the note, nothing else -- no panel was ever attempted.
        Assert.Equal(3, cells.Count);

        var notebook = new JsonObject { ["cells"] = cells };
        var validation = DarlingWebEndpoints.ValidateNotebookDefinition(notebook);
        Assert.True(validation.IsValid, validation.Error);
    }

    /* ═══════════════════════════ unparseable definition ═══════════════════════════ */

    [Fact]
    public void UnparseableDefinition_ProducesNoteCell_NeverThrows()
    {
        var rule = RuleWithDefinition("{ this is not valid json");
        var context = ContextFor(rule, missing: false);

        var cells = InvokeCustomRuleTemplate(context);

        var note = cells.OfType<JsonObject>().Single(c => (string?)c["type"] == "markdown");
        Assert.Contains("could not be read", (string?)note["text"], StringComparison.Ordinal);
    }

    [Fact]
    public void UnparseableDefinition_ReferencingAMeasureThatNoLongerExists_ProducesNoteCell()
    {
        var rule = RuleWithDefinition(
            "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"no_such_measure\",\"aggregate\":\"sum\",\"hours\":1}," +
            "\"predicate\":{\"op\":\"gt\",\"warnThreshold\":80}}");

        var cells = InvokeCustomRuleTemplate(ContextFor(rule, missing: false));

        var note = cells.OfType<JsonObject>().Single(c => (string?)c["type"] == "markdown");
        Assert.Contains("could not be read", (string?)note["text"], StringComparison.Ordinal);
    }

    /* ═══════════════════════════ shared theory coverage (#4223) ═══════════════════════════ */

    /// <summary>The shared <c>AllAuthoredMetrics</c> theories in <c>AlertNotebookAuthoredTemplateTests</c>
    /// walk only <c>s_authoredTemplates</c> (the exact-name table); a prefix-routed family like
    /// <c>Custom:</c> needs its own fabricated-context coverage here.</summary>
    [Fact]
    public void CustomRuleTemplate_BuiltWithAFabricatedScalarContext_PassesValidateNotebookDefinition()
    {
        var rule = RuleWithDefinition(ScalarDefinitionJson);
        var cells = InvokeCustomRuleTemplate(ContextFor(rule, missing: false));

        var notebook = new JsonObject { ["cells"] = cells };
        var validation = DarlingWebEndpoints.ValidateNotebookDefinition(notebook);

        Assert.True(validation.IsValid, validation.Error);
    }
}
