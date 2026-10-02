/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3898 D3 head pins for the alerting family (<c>get_alert_settings</c>, <c>get_notification_routes</c>).
/// Follows the <c>get_health_parser_*</c> pilot's pattern. Darling's twin is
/// <c>Darling/Darling.Tests/McpToolGuideHeads.Alerting.cs</c>; the two tools' heads are byte-identical across
/// SKUs (D6, pinned generically by <c>McpToolGuideTests.EverySharedToolName_CarriesTheMarkerOnBothSkus_OrNeither_WithByteIdenticalHeads</c>
/// on the Darling side), so the head-fact assertions below are duplicated verbatim in both files on purpose.
/// </summary>
public sealed class McpToolGuideHeadsAlertingTests
{
    private static readonly string[] AlertingTools =
    [
        "get_alert_settings",
        "get_notification_routes",
        "get_alert_history",
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

    /// <summary>D9: load-bearing facts that moved off the head stay reachable in Lite's own tail (per-product
    /// prose per D6; Darling's own tail is pinned by Darling.Tests' copy of this file).</summary>
    [Fact]
    public void LiteTails_StillCarryTheKnobsAndTheEditionGapsTheHeadCouldNotFit()
    {
        var settingsTail = McpToolGuideTests.Served("get_alert_settings").Tail!;
        Assert.Contains("poison_wait.threshold_ms is RETIRED", settingsTail, StringComparison.Ordinal);
        Assert.Contains("analysis.uncorroborated_route is where a notify-worthy but UNCORROBORATED finding goes", settingsTail, StringComparison.Ordinal);
        Assert.Contains("7-day read of one large production store", settingsTail, StringComparison.Ordinal);
        Assert.Contains("minus its self_alerts group", settingsTail, StringComparison.Ordinal);
        Assert.Contains("smtp", settingsTail, StringComparison.Ordinal);

        var routesTail = McpToolGuideTests.Served("get_notification_routes").Tail!;
        Assert.Contains("routes_supported is false here", routesTail, StringComparison.Ordinal);
        Assert.Contains("The taxonomy is the shared", routesTail, StringComparison.Ordinal);
    }

    /// <summary>#3898 lane L3c: get_alert_history's head, byte-identical to Darling's (D6) — the
    /// fired/delivered/dismissed distinction, the page-bound-by-limit-not-hours_back trap, and the
    /// empty-vs-quiet hazard. See <c>Darling.Tests/McpToolGuideHeads.Alerting.cs</c> for the Darling-side copy of
    /// this same assertion set.</summary>
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

    /// <summary>D9: load-bearing facts that moved off get_alert_history's head stay reachable in Lite's own
    /// tail: the 'tray' disposition (every non-muted alert gets one, and it does NOT report the email/webhook
    /// outcome), the parquet-archive dismissal gap unique to this edition, and the severity/routing taxonomy
    /// (#3712 refs stripped per D4, the facts kept).</summary>
    [Fact]
    public void LiteGetAlertHistoryTail_StillCarriesTheTrayDispositionAndArchiveGap()
    {
        var tail = McpToolGuideTests.Served("get_alert_history").Tail!;
        Assert.Contains("'tray' is this instance's own balloon notification", tail, StringComparison.Ordinal);
        Assert.Contains("it does NOT report the email or webhook outcome", tail, StringComparison.Ordinal);
        Assert.Contains("On this edition an alert that was dismissed AFTER aging into the parquet archive is removed by the archive view itself", tail, StringComparison.Ordinal);
        Assert.Contains("severity_source says where it came from", tail, StringComparison.Ordinal);
        Assert.Contains("routing is the corroboration gate's decision for an 'Analysis", tail, StringComparison.Ordinal);
        Assert.Contains("Dismissal is an acknowledgement, not a verdict", tail, StringComparison.Ordinal);
        Assert.DoesNotContain("(#3712)", tail, StringComparison.Ordinal);
    }
}
