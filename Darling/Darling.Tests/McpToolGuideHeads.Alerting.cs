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
/// #3898 D3 head pins for the alerting family (<c>get_alert_settings</c>, <c>get_notification_routes</c>).
/// Follows the <c>get_health_parser_*</c> pilot's pattern (see <see cref="McpToolGuideHeadsHealthParserTests"/>):
/// the family's tool roster and guardrail facts, plus the tests that pin them, using
/// <see cref="McpToolGuideTests"/>'s shared <see cref="McpToolGuideTests.Served"/> helper.
/// <c>McpToolGuideTests</c> itself is never edited to add a family, so two families converting in parallel never
/// conflict there. Lite's twin is <c>Lite.Tests/McpToolGuideHeads.Alerting.cs</c>; the two tools' heads are
/// byte-identical across SKUs (D6, pinned generically by <see cref="McpToolGuideTests.EverySharedToolName_CarriesTheMarkerOnBothSkus_OrNeither_WithByteIdenticalHeads"/>),
/// so the head-fact assertions below are duplicated verbatim in both files on purpose.
/// </summary>
public sealed class McpToolGuideHeadsAlertingTests
{
    private static readonly string[] AlertingTools =
    [
        "get_alert_settings",
        "get_notification_routes",
        "update_alert_settings",
        "update_mute_rule",
    ];

    [Fact]
    public void EveryAlertingHead_StaysAtOrUnder620Characters_AndPointsToTheGuide()
    {
        foreach (var tool in AlertingTools)
        {
            var served = McpToolGuideTests.Served(tool);
            Assert.NotNull(served.Tail);
            Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
            Assert.True(served.Served.Length <= 620, $"{tool}: served head {served.Served.Length} is over the 620 target");
        }
    }

    /// <summary>get_alert_settings: what the answer is (the effective, shared configuration), the two-cooldown
    /// guardrail (which stage each one gates), and how to read an unset/default value on each SKU.</summary>
    [Fact]
    public void GetAlertSettingsHead_StatesTheAnswerAndTheDefaultSemantics()
    {
        var head = McpToolGuideTests.Served("get_alert_settings").Served;
        Assert.Contains("Gets the alert configuration currently in effect", head, StringComparison.Ordinal);
        Assert.Contains("cooldown_minutes gates whether an alert FIRES", head, StringComparison.Ordinal);
        Assert.Contains("delivery.cooldown_minutes separately bounds the resulting post", head, StringComparison.Ordinal);
        Assert.Contains("Darling: an unseeded store answers status unavailable", head, StringComparison.Ordinal);
        Assert.Contains("Lite always answers its live in-memory settings", head, StringComparison.Ordinal);
        Assert.Contains("An empty knob list", head, StringComparison.Ordinal);
        Assert.Contains("means cleared, not a default in force", head, StringComparison.Ordinal);
    }

    /// <summary>get_notification_routes: zero routes is the ordinary state (not a broken read), the
    /// inherit-not-silence rule, the Darling/Lite split (D6: the fact itself differs, so the head states both),
    /// and that routing decides WHERE, never WHETHER.</summary>
    [Fact]
    public void GetNotificationRoutesHead_StatesTheOrdinaryStateAndTheSkuSplit()
    {
        var head = McpToolGuideTests.Served("get_notification_routes").Served;
        Assert.Contains("Zero routes is the ordinary state", head, StringComparison.Ordinal);
        Assert.Contains("EMPTY channel on a route INHERITS the parent's", head, StringComparison.Ordinal);
        Assert.Contains("routes_supported is false", head, StringComparison.Ordinal);
        Assert.Contains("Routing decides WHERE a post lands, never WHETHER it is sent", head, StringComparison.Ordinal);
    }

    /// <summary>D9: load-bearing facts that moved off the head stay reachable in Darling's own tail (per-product
    /// prose per D6; Lite's own tail is pinned by Lite.Tests' copy of this file).</summary>
    [Fact]
    public void DarlingTails_StillCarryTheKnobsAndTheFamiliesTheHeadCouldNotFit()
    {
        var settingsTail = McpToolGuideTests.Served("get_alert_settings").Tail!;
        Assert.Contains("self_alerts", settingsTail, StringComparison.Ordinal);
        Assert.Contains("health_bands", settingsTail, StringComparison.Ordinal);
        Assert.Contains("NOT an alert", settingsTail, StringComparison.Ordinal);
        Assert.Contains("fleet_sweep", settingsTail, StringComparison.Ordinal);
        Assert.Contains("poison_wait.threshold_ms is RETIRED", settingsTail, StringComparison.Ordinal);
        Assert.Contains("analysis.uncorroborated_route is where a notify-worthy but UNCORROBORATED finding goes", settingsTail, StringComparison.Ordinal);
        Assert.Contains("7-day read of one large production store", settingsTail, StringComparison.Ordinal);

        var routesTail = McpToolGuideTests.Served("get_notification_routes").Tail!;
        Assert.Contains("matching the metric EXACTLY, then one matching its FAMILY, then the parent", routesTail, StringComparison.Ordinal);
        Assert.Contains("Routing sits AFTER the cooldown", routesTail, StringComparison.Ordinal);
        Assert.Contains("are NOT reported", routesTail, StringComparison.Ordinal);
    }

    /// <summary>#3898 lane L3b: update_alert_settings's head states the shared-row PARTIAL-update contract, the
    /// all-or-nothing invalid/unknown-field outcome, the poison_wait.threshold_ms retirement, and that
    /// health_bands/fleet_sweep are not alert families — the four facts a #3898 D9 pass found a model could get
    /// wrong from the old head-less description alone.</summary>
    [Fact]
    public void UpdateAlertSettingsHead_StatesThePartialUpdateAndThePoisonWaitAndHealthBandsFacts()
    {
        var head = McpToolGuideTests.Served("update_alert_settings").Served;
        Assert.Contains("PARTIAL update of the single global alert-settings row the service delivers on", head, StringComparison.Ordinal);
        Assert.Contains("Call get_alert_settings FIRST", head, StringComparison.Ordinal);
        Assert.Contains("omitted fields stay unchanged", head, StringComparison.Ordinal);
        Assert.Contains("An invalid value or unknown field writes NOTHING; returns status invalid", head, StringComparison.Ordinal);
        Assert.Contains("poison_wait.threshold_ms is RETIRED: accepted with a warnings entry", head, StringComparison.Ordinal);
        Assert.Contains("poison_wait.enabled governs it", head, StringComparison.Ordinal);
        Assert.Contains("health_bands and fleet_sweep are NOT alert families", head, StringComparison.Ordinal);
        Assert.Contains("alerts_enabled does not govern fleet_sweep", head, StringComparison.Ordinal);
    }

    /// <summary>#3898 lane L3b: update_mute_rule's head states it edits an existing rule in place (a PARTIAL
    /// update where an unsent field stays as stored and an explicit null clears it), that it changes shared
    /// alert configuration, that enabled cannot be changed here, and the widen-to-everything hazard of clearing
    /// every scope field.</summary>
    [Fact]
    public void UpdateMuteRuleHead_StatesThePartialUpdateAndTheClearingAndEnabledFacts()
    {
        var head = McpToolGuideTests.Served("update_mute_rule").Served;
        Assert.Contains("Edits an existing alert mute rule IN PLACE by its id", head, StringComparison.Ordinal);
        Assert.Contains("changes shared alert configuration", head, StringComparison.Ordinal);
        Assert.Contains("PARTIAL", head, StringComparison.Ordinal);
        Assert.Contains("a field you do NOT send stays exactly as stored", head, StringComparison.Ordinal);
        Assert.Contains("an EXPLICIT JSON null CLEARS that field", head, StringComparison.Ordinal);
        Assert.Contains("enabled is NOT editable here", head, StringComparison.Ordinal);
        Assert.Contains("created_at_utc never moves", head, StringComparison.Ordinal);
        Assert.Contains("Clearing scope fields WIDENS the rule", head, StringComparison.Ordinal);
    }

    /// <summary>D9: load-bearing facts that moved off the head stay reachable in Darling's own tail for the two
    /// write tools (no Lite twin exists for either — verified by <c>git grep -n 'Name = "update_alert_settings"'
    /// -- Lite/Mcp</c> finding nothing — so there is no Lite copy of this pin).</summary>
    [Fact]
    public void DarlingTails_StillCarryTheUpdateToolsKnobsAndCautions()
    {
        var settingsTail = McpToolGuideTests.Served("update_alert_settings").Tail!;
        Assert.Contains("self_alerts", settingsTail, StringComparison.Ordinal);
        Assert.Contains("health_bands.deadlock_warn_per_hour", settingsTail, StringComparison.Ordinal);
        Assert.Contains("fleet_sweep.enabled", settingsTail, StringComparison.Ordinal);
        Assert.Contains("poison_wait.threshold_ms is RETIRED", settingsTail, StringComparison.Ordinal);
        Assert.Contains("analysis.uncorroborated_route is WRITABLE since V137", settingsTail, StringComparison.Ordinal);
        Assert.Contains("DEFAULTS from a 7-day production read", settingsTail, StringComparison.Ordinal);
        Assert.Contains("Pass the FULL list each time", settingsTail, StringComparison.Ordinal);

        var muteRuleTail = McpToolGuideTests.Served("update_mute_rule").Tail!;
        Assert.Contains("USE THIS", muteRuleTail, StringComparison.Ordinal);
        Assert.Contains("Stale Mute Rules self-alert ages a rule from its creation date", muteRuleTail, StringComparison.Ordinal);
        Assert.Contains("a whole-fleet silence", muteRuleTail, StringComparison.Ordinal);
        Assert.Contains("must match the alert rows' spelling EXACTLY", muteRuleTail, StringComparison.Ordinal);
    }
}
