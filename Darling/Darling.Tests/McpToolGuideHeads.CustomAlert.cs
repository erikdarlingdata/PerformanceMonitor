/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3898 D3 head pins for the custom-alert-rule "test/validate/update/list-templates" slice of
/// <c>DarlingMcpCustomAlertTools</c> (Darling-only: no Lite twin — custom alert rules are a central-store
/// feature). Follows the pattern in <see cref="McpToolGuideHeadsHealthParserTests"/>. The other four
/// custom-alert-rule tools (list/get/create/delete) stayed under the ~500-char conversion threshold and were
/// left unconverted.
/// </summary>
public sealed class McpToolGuideHeadsCustomAlertTests
{
    private static readonly string[] ConvertedTools =
    [
        "validate_custom_alert_rule",
        "test_custom_alert_rule",
        "update_custom_alert_rule",
        "list_custom_alert_templates",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("validate_custom_alert_rule", "WITHOUT persisting anything"),
        ("validate_custom_alert_rule", "exact authority create_custom_alert_rule / update_custom_alert_rule run before saving"),
        ("validate_custom_alert_rule", "A count-aggregate predicate that can fire when the count is 0 is always rejected"),

        ("test_custom_alert_rule", "WITHOUT delivering a notification, writing history, or touching per-server streak state"),
        ("test_custom_alert_rule", "breaching:true means the predicate is true THIS INSTANT, not that the rule has fired"),
        ("test_custom_alert_rule", "A null current_value is no-data and never breaches"),
        ("test_custom_alert_rule", "Zero in-scope servers returns status:no_in_scope_servers with empty results, not an error"),

        ("update_custom_alert_rule", "an omitted field keeps its current value"),
        ("update_custom_alert_rule", "description ALSO accepts an empty string \"\" to clear it"),
        ("update_custom_alert_rule", "a mismatch returns {status:\"conflict\"} rather than overwriting"),
        ("update_custom_alert_rule", "Setting enabled:true is refused as {status:\"invalid\"} if it would push the fleet-wide enabled-rule count over its cap"),

        ("list_custom_alert_templates", "curated {key, name, description, definition} entries"),
        ("list_custom_alert_templates", "deliberately conservative, not tuned"),
        ("list_custom_alert_templates", "Read-only: lists code-defined templates and touches no store"),
    ];

    [Fact]
    public void EveryConvertedHead_CarriesItsGuardrailFact_AndThePointer()
    {
        foreach (var tool in ConvertedTools)
        {
            var served = McpToolGuideTests.Served(tool);
            Assert.NotNull(served.Tail);
            Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
            Assert.True(served.Served.Length <= 620, $"{tool}: served head {served.Served.Length} is over the 620 target");
            Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"{tool}.{p.Parameter}: {p.Length} > 200"));
        }

        foreach (var (tool, fact) in HeadFacts)
        {
            Assert.Contains(fact, McpToolGuideTests.Served(tool).Served, StringComparison.Ordinal);
        }
    }

    /// <summary>D9: a caller who sees an empty <c>results</c> array from test_custom_alert_rule must be able to
    /// tell "nothing is in scope" (a real, well-formed answer) apart from a run that silently skipped every
    /// server. The head names the one status that ever accompanies it; the tail carries the exact response
    /// shape difference (no top-level status field on an ordinary run) so a caller building a parser gets it
    /// right. This response-shape split (and the update tool's enabled-rule cap below) was previously
    /// undocumented on the wire; both are additions verified against <c>CustomAlertRuleStore.UpdateAsync</c> /
    /// <c>DarlingMcpCustomAlertTools.TestCustomAlertRule</c>, not moved prose.</summary>
    [Fact]
    public void TestCustomAlertRule_TailExplainsTheNoScopeResponseShape()
    {
        var tail = McpToolGuideTests.Served("test_custom_alert_rule").Tail!;
        Assert.Contains("{status:\"no_in_scope_servers\", rule_id, name, note, results:[]}", tail, StringComparison.Ordinal);
        Assert.Contains("a normal run's response carries no top-level status field at all", tail, StringComparison.Ordinal);
    }

    /// <summary>D9: enabling a rule (create-enabled or update-to-enabled) is refused once the fleet-wide
    /// enabled-rule count is at <c>CustomAlertRuleStore.EnabledRuleCap</c> (100) — a fact <c>UpdateAsync</c>
    /// enforces but no description stated before this PR. The head carries the refusal; the tail carries the
    /// exact number and message, and that disabling is never subject to it.</summary>
    [Fact]
    public void UpdateCustomAlertRule_TailExplainsTheEnabledRuleCap()
    {
        var tail = McpToolGuideTests.Served("update_custom_alert_rule").Tail!;
        Assert.Contains("100 cap", tail, StringComparison.Ordinal);
        Assert.Contains("never counts against that cap and is never refused for it", tail, StringComparison.Ordinal);
    }

    /// <summary>The shared update-tool vocabulary pin (<c>DarlingMcpCustomViewToolsTests
    /// .BothUpdateTools_DescribeTheSameDescriptionVocabulary</c>) reads the raw <c>[Description]</c> attribute
    /// text, not the served head, so it is unaffected by the head/tail split either way — this just pins that
    /// the cross-reference survives the conversion in the text a caller actually sees (the head).</summary>
    [Fact]
    public void UpdateCustomAlertRule_HeadNamesTheSharedViewToolByName()
    {
        Assert.Contains("update_custom_view", McpToolGuideTests.Served("update_custom_alert_rule").Served, StringComparison.Ordinal);
    }
}
