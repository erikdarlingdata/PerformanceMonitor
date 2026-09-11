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
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3285 (plan Component 7): the starter custom-alert templates' drift-guard, the alert twin of
/// <c>ViewTemplatesTests</c>. A stored rule is validated once at write time and never again, so a template
/// naming a measure that later drifts out of the catalog would ship a rule that can never fire and nothing
/// would grep it. The templates are CODE, so the SAME authority the evaluator and create/update run
/// (<see cref="CustomAlertRuleDefinition.TryParse"/>, over the live <c>MeasureCatalog</c>) is applied to every
/// one on every build — a drifted measure fails here rather than shipping dead.
/// </summary>
public sealed class CustomAlertTemplatesTests
{
    [Fact]
    public void EveryTemplate_ValidatesAgainstTheLiveRuleValidatorAndCatalog()
    {
        Assert.NotEmpty(CustomAlertTemplates.All);

        foreach (var template in CustomAlertTemplates.All)
        {
            var (def, error) = CustomAlertRuleDefinition.TryParse(template.DefinitionJson);
            Assert.True(
                error is null && def is not null,
                $"template '{template.Key}' no longer validates against the live rule-validator + catalog: {error}");
        }
    }

    [Fact]
    public void EveryTemplate_HasAUniqueKey_AndSaysWhatItIsFor()
    {
        var keys = CustomAlertTemplates.All.Select(t => t.Key).ToArray();

        Assert.True(keys.Length >= 8, "expected the full starter set; found " + keys.Length);
        Assert.Equal(keys.Length, keys.Distinct(StringComparer.Ordinal).Count());
        Assert.All(CustomAlertTemplates.All, t =>
        {
            Assert.False(string.IsNullOrWhiteSpace(t.Key));
            Assert.False(string.IsNullOrWhiteSpace(t.Name));
            Assert.False(string.IsNullOrWhiteSpace(t.Description));
        });
    }

    [Fact]
    public void EveryTemplate_IsAScalarRuleWithAPredicate_AndBothSeverityTiers()
    {
        foreach (var template in CustomAlertTemplates.All)
        {
            var (def, error) = CustomAlertRuleDefinition.TryParse(template.DefinitionJson);
            Assert.Null(error);
            Assert.NotNull(def);

            // A rule window is real and alert-appropriate; TryParse enforces the Scalar-only + predicate rules,
            // and every starter ships a critical tier above warn (a user can drop it when they create the rule).
            Assert.True(def!.WindowHours > 0);
            Assert.NotNull(def.CriticalThreshold);
        }
    }

    [Fact]
    public async Task ListCustomAlertTemplatesTool_ReturnsEveryTemplate_WithAParsedDefinitionObject()
    {
        var json = await DarlingMcpCustomAlertTools.ListCustomAlertTemplates();
        var root = (JsonObject)JsonNode.Parse(json)!;
        var templates = (JsonArray)root["templates"]!;

        Assert.Equal(CustomAlertTemplates.All.Count, templates.Count);

        var toolKeys = templates.Select(t => (string?)((JsonObject)t!)["key"]).ToHashSet(StringComparer.Ordinal);
        Assert.Equal(CustomAlertTemplates.All.Select(t => t.Key).ToHashSet(StringComparer.Ordinal), toolKeys);

        foreach (var entry in templates)
        {
            var obj = (JsonObject)entry!;
            Assert.False(string.IsNullOrWhiteSpace((string?)obj["name"]));
            Assert.False(string.IsNullOrWhiteSpace((string?)obj["description"]));

            // The definition is embedded as a JSON OBJECT (not an escaped string), ready to hand to
            // create_custom_alert_rule / test_custom_alert_rule, and it still validates.
            var definition = obj["definition"] as JsonObject;
            Assert.NotNull(definition);
            var (parsed, error) = CustomAlertRuleDefinition.TryParse(definition!.ToJsonString());
            Assert.True(error is null && parsed is not null, error);
        }
    }
}
