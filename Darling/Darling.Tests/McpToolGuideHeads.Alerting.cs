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
        "get_alert_history",
        "set_notification_route_enabled",
        "create_mute_rule",
        "set_mute_rule_enabled",
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

    /// <summary>#3898 lane L3c: get_alert_history's head states the fired/delivered/dismissed distinction (a row
    /// existing means it FIRED; notification_type says whether it was DELIVERED; dismissed is a third, separate,
    /// excluded-by-default axis), the page-bound-by-limit-not-hours_back trap, and the empty-vs-quiet hazard (an
    /// empty page can mean no alerts fired OR that every alert was dismissed). Byte-identical on Lite (D6),
    /// pinned again in <c>Lite.Tests/McpToolGuideHeads.Alerting.cs</c>.</summary>
    [Fact]
    public void GetAlertHistoryHead_StatesFiredVsDeliveredVsDismissedAndThePageBound()
    {
        var head = McpToolGuideTests.Served("get_alert_history").Served;
        Assert.Contains("each row FIRED", head, StringComparison.Ordinal);
        Assert.Contains("notification_type says whether it was DELIVERED", head, StringComparison.Ordinal);
        Assert.Contains("DISMISSED (UI-acknowledged), excluded by default", head, StringComparison.Ordinal);
        Assert.Contains("THE PAGE IS BOUNDED BY limit, NOT hours_back", head, StringComparison.Ordinal);
        Assert.Contains("An EMPTY page can mean no alerts fired, or that every alert here was dismissed", head, StringComparison.Ordinal);
        Assert.Contains("A null send_error proves nothing about delivery", head, StringComparison.Ordinal);
    }

    /// <summary>#3898 lane L3c: set_notification_route_enabled's head states what it changes (shared alert
    /// configuration), that it is reversible (unlike delete_notification_route), and the two safe-retry
    /// outcomes.</summary>
    [Fact]
    public void SetNotificationRouteEnabledHead_StatesTheChangeAndReversibility()
    {
        var head = McpToolGuideTests.Served("set_notification_route_enabled").Served;
        Assert.Contains("WITHOUT deleting it", head, StringComparison.Ordinal);
        Assert.Contains("Changes shared alert configuration the service delivers on", head, StringComparison.Ordinal);
        Assert.Contains("Reversible: re-enabling restores it exactly", head, StringComparison.Ordinal);
        Assert.Contains("Returns updated (route as stored), unchanged", head, StringComparison.Ordinal);
        Assert.Contains("Cannot author or re-point a route", head, StringComparison.Ordinal);
    }

    /// <summary>#3898 lane L3c: create_mute_rule's head states the whole-fleet-silence hazard of a rule with no
    /// scope fields, that there is no confirm step (it takes effect on the next collection sweep), and what it
    /// returns for the follow-up delete/enable calls.</summary>
    [Fact]
    public void CreateMuteRuleHead_StatesTheEmptyRuleMutesEverythingHazard()
    {
        var head = McpToolGuideTests.Served("create_mute_rule").Served;
        Assert.Contains("Changes shared alert configuration the service delivers on", head, StringComparison.Ordinal);
        Assert.Contains("a rule with NO fields set matches and mutes EVERY alert across the whole fleet", head, StringComparison.Ordinal);
        Assert.Contains("No confirm step", head, StringComparison.Ordinal);
        Assert.Contains("Returns the stored rule with its generated id, for delete_mute_rule or set_mute_rule_enabled", head, StringComparison.Ordinal);
    }

    /// <summary>#3898 lane L3c: set_mute_rule_enabled's head states it is the reversible alternative to
    /// delete+create (which loses the id and resets the Stale Mute Rules age clock), and the two safe-retry
    /// outcomes.</summary>
    [Fact]
    public void SetMuteRuleEnabledHead_StatesTheReversibleFlagAndTheStaleClock()
    {
        var head = McpToolGuideTests.Served("set_mute_rule_enabled").Served;
        Assert.Contains("WITHOUT deleting it", head, StringComparison.Ordinal);
        Assert.Contains("Changes shared alert configuration the service delivers on", head, StringComparison.Ordinal);
        Assert.Contains("keeps its id, scope, reason and creation date", head, StringComparison.Ordinal);
        Assert.Contains("unlike delete+create, which loses those and resets the Stale Mute Rules age clock", head, StringComparison.Ordinal);
        Assert.Contains("Returns updated (rule as stored), unchanged", head, StringComparison.Ordinal);
    }

    /// <summary>D9: load-bearing facts that moved off get_alert_history's head stay reachable in Darling's own
    /// tail: the full notification_type taxonomy, the severity/severity_source distinction, and the route vs
    /// routing distinction (#3598/#3712 refs stripped per D4, the facts kept).</summary>
    [Fact]
    public void DarlingGetAlertHistoryTail_StillCarriesTheNotificationTypeAndRoutingTaxonomy()
    {
        var tail = McpToolGuideTests.Served("get_alert_history").Tail!;
        Assert.Contains("notification_type is the delivery disposition and is the ONLY field that says why a row did not deliver", tail, StringComparison.Ordinal);
        Assert.Contains("severity_source says where it came from", tail, StringComparison.Ordinal);
        Assert.Contains("route says WHERE the posts went:", tail, StringComparison.Ordinal);
        Assert.Contains("routing is a DIFFERENT question from route:", tail, StringComparison.Ordinal);
        Assert.Contains("To see what the digest carried", tail, StringComparison.Ordinal);
        Assert.DoesNotContain("(#3598)", tail, StringComparison.Ordinal);
        Assert.DoesNotContain("(#3712)", tail, StringComparison.Ordinal);
    }

    /// <summary>D9/D2: the three write tools' tails still carry their full original reasoning, and
    /// get_alert_history's include_dismissed overflow (D2: the parameter's own text stayed under 200 chars, the
    /// rest moved to the tool's tail) is reachable there.</summary>
    [Fact]
    public void DarlingWriteToolTails_StillCarryTheOriginalReasoningAndTheMovedParameterOverflow()
    {
        var routeTail = McpToolGuideTests.Served("set_notification_route_enabled").Tail!;
        Assert.Contains("the reversible form of", routeTail, StringComparison.Ordinal);
        Assert.Contains("The running service picks the change up on its next collection sweep", routeTail, StringComparison.Ordinal);
        Assert.Contains("destinations are bearer secrets and are set only in the Viewer's Settings window", routeTail, StringComparison.Ordinal);

        var createTail = McpToolGuideTests.Served("create_mute_rule").Tail!;
        Assert.Contains("the same rules get_mute_rules lists and the Viewer's Manage Mute Rules surface writes", createTail, StringComparison.Ordinal);
        Assert.Contains("expires_at is an optional ISO-8601 UTC timestamp", createTail, StringComparison.Ordinal);
        Assert.Contains("a matching alert already mid-flight can still be delivered once", createTail, StringComparison.Ordinal);

        var enabledTail = McpToolGuideTests.Served("set_mute_rule_enabled").Tail!;
        Assert.Contains("USE THIS rather than delete_mute_rule followed by create_mute_rule", enabledTail, StringComparison.Ordinal);
        Assert.Contains("NEITHER direction changes created_at_utc", enabledTail, StringComparison.Ordinal);

        var historyTail = McpToolGuideTests.Served("get_alert_history").Tail!;
        Assert.Contains("Dismissal is an acknowledgement, not a verdict", historyTail, StringComparison.Ordinal);
        Assert.Contains("Each row then carries dismissed so the two populations stay distinguishable", historyTail, StringComparison.Ordinal);
    }
}
