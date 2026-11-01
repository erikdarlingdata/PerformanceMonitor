/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7109: the tighten-only rule for action approval over MCP (<see cref="DarlingMcpAlertTools.ApprovalRouteRefusal"/>). MCP can turn approval on
/// but never off, and cannot disable, re-level or reshape a Level 3 route with approval required, nor enable, widen or raise a route with approval
/// off. Each refusal names the rule and says a person makes the change in the Viewer or on the web dashboard. The live class
/// <see cref="McpRouteAgentFieldsLiveTests"/> runs the same rule through the real tool against a store.
/// </summary>
public sealed class McpRouteApprovalRuleTests
{
    private const string Tool = "update_notification_route";

    private static PgNotificationRouteStore.RouteSummary Route(int? level, string approval, bool enabled = true, bool singlePerson = false) =>
        new(7, "Deadlocks", null, Array.Empty<string>(), "", enabled, DateTime.UtcNow)
        {
            AgentLevel = level,
            ActionApproval = approval,
            ApprovalSinglePerson = singlePerson,
        };

    private static DarlingMcpAlertTools.RouteAgentRequest Agent(int? level = null, bool clear = false, string? approval = null, bool? single = null) =>
        new(level, null, null, null, null, clear, approval, single);

    private static void AssertRefused(string? answer, string rule)
    {
        Assert.NotNull(answer);
        var root = JsonDocument.Parse(answer!).RootElement;
        Assert.Equal("refused", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains(rule, message, StringComparison.Ordinal);
        Assert.Contains("Viewer", message, StringComparison.Ordinal);
        Assert.Contains("web dashboard", message, StringComparison.Ordinal);
    }

    [Fact]
    public void ActionApproval_GoesOnlyToRequired()
    {
        AssertRefused(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(3, "required"), Agent(approval: "off"), [], true, Tool), "never off");
        AssertRefused(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(1, "required"), Agent(approval: "OFF"), [], true, Tool), "never off");
        Assert.Null(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(3, "off"), Agent(approval: "required"), [], true, Tool));
    }

    [Fact]
    public void SinglePerson_GoesOnlyToFalse()
    {
        AssertRefused(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(3, "required"), Agent(single: true), [], true, Tool), "never to true");
        Assert.Null(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(3, "required", singlePerson: true), Agent(single: false), [], true, Tool));
    }

    [Fact]
    public void ALevel3RequiredRoute_CannotBeDisabled_OrPutBackToInherit()
    {
        AssertRefused(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(3, "required"), Agent(), ["enabled"], false, Tool), "does not disable it");
        AssertRefused(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(3, "required"), Agent(clear: true), [], true, Tool), "back to Inherit");
    }

    [Theory]
    [InlineData("metric_match")]
    [InlineData("scope")]
    [InlineData("severities")]
    public void ALevel3RequiredRoute_KeepsItsMatchScopeAndSeverities(string column)
    {
        AssertRefused(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(3, "required"), Agent(), [column], true, Tool), "match, scope or severities");
        /* Lowering in the same call does not open the change: the rule reads the route as stored. */
        AssertRefused(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(3, "required"), Agent(level: 2), [column], true, Tool), "match, scope or severities");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void ALevel3RequiredRoute_CanBeLowered(int level)
    {
        Assert.Null(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(3, "required"), Agent(level: level), [], true, Tool));
    }

    [Fact]
    public void ARouteBelowLevel3_WithApprovalRequired_CanBeDisabledAndReshaped()
    {
        Assert.Null(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(2, "required"), Agent(), ["enabled", "metric_match", "scope", "severities"], false, Tool));
        Assert.Null(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(2, "required"), Agent(level: 3), [], true, Tool));
    }

    [Theory]
    [InlineData(null)]
    [InlineData(0)]
    [InlineData(2)]
    public void ARouteWithApprovalOff_CannotBeRaisedToLevel3_UnlessTheSameCallSetsRequired(int? level)
    {
        AssertRefused(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(level, "off"), Agent(level: 3), [], true, Tool), "raise it to Level 3");
        Assert.Null(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(level, "off"), Agent(level: 3, approval: "required"), [], true, Tool));
    }

    [Fact]
    public void ALevel3RouteWithApprovalOff_CannotBeEnabledOrWidened_ButCanBeDisabledAndLowered()
    {
        AssertRefused(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(3, "off", enabled: false), Agent(), ["enabled"], true, Tool), "does not enable it");
        foreach (var column in new[] { "metric_match", "scope", "severities" })
        {
            AssertRefused(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(3, "off"), Agent(), [column], true, Tool), "match, scope or severities");
        }

        Assert.Null(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(3, "off"), Agent(), ["enabled"], false, Tool));
        Assert.Null(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(3, "off"), Agent(level: 1), [], true, Tool));
        Assert.Null(DarlingMcpAlertTools.ApprovalRouteRefusal(Route(3, "off"), Agent(clear: true), [], true, Tool));
    }

    [Fact]
    public void AnUnknownStoredWord_ReadsAsRequired()
    {
        Assert.Equal(NotificationRouteKinds.ActionApprovalRequired, NotificationRouteKinds.NormalizeActionApproval("maybe"));
        Assert.Equal(NotificationRouteKinds.ActionApprovalRequired, NotificationRouteKinds.NormalizeActionApproval(null));
        Assert.Equal(NotificationRouteKinds.ActionApprovalOff, NotificationRouteKinds.NormalizeActionApproval(" Off "));
    }
}
