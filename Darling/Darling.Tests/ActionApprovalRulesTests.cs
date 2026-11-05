/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="ActionApprovalRules.ForRequest"/> (#7109), the one rule the gate reports and the write refuses: a request needs approval when any of
/// the routes its metrics resolve to is <c>required</c>, always when it is a chat request, and single-person approval holds only when every route has it.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class ActionApprovalRulesTests
{
    private const string Off = NotificationRouteKinds.ActionApprovalOff;
    private const string Required = NotificationRouteKinds.ActionApprovalRequired;

    private static NotificationRoute Route(int id, string approval, bool single = false, int? level = 3, int? server = null) =>
        new(id, "Blocking", "", "", "", "", "", Enabled: true)
        {
            AgentLevel = level, ActionApproval = approval, ApprovalSinglePerson = single, ScopeServerId = server,
            ExpandedServerIds = server is { } s ? new HashSet<int> { s } : null,
        };

    [Fact]
    public void SeveralMetrics_OneRouteRequired_NeedsApproval()
    {
        var state = ActionApprovalRules.ForRequest([Route(1, Off), Route(2, Required), Route(3, Off)], InvestigationTriggerKinds.Alert);
        Assert.True(state.Needed);
    }

    [Fact]
    public void SeveralMetrics_AllOff_NeedsNoApproval()
    {
        var state = ActionApprovalRules.ForRequest([Route(1, Off), Route(2, Off)], InvestigationTriggerKinds.Alert);
        Assert.False(state.Needed);
    }

    [Fact]
    public void ARouteWithAnUnknownWord_ReadsAsRequired()
    {
        Assert.True(ActionApprovalRules.ForRequest([Route(1, "sometimes")], InvestigationTriggerKinds.Alert).Needed);
    }

    [Fact]
    public void AMetricWhoseSpecificRouteHasNoLevel_PassesToTheLessSpecificRoute_ForTheApproval()
    {
        /* A route with no level is transparent to ResolveAgent: the server route says nothing about agents, so the fleet route (required) answers. */
        var routes = new List<NotificationRoute>
        {
            Route(1, Off, level: null, server: 7),
            Route(2, Required, level: 3),
        };
        var picked = NotificationRouter.ResolveAgent("Blocking", routes, 7);
        Assert.Equal(2, picked!.RouteId);
        Assert.True(ActionApprovalRules.ForRequest([picked], InvestigationTriggerKinds.Alert).Needed);

        /* With a level on the specific route, it answers and its off wins. */
        routes[0] = Route(1, Off, level: 3, server: 7);
        picked = NotificationRouter.ResolveAgent("Blocking", routes, 7);
        Assert.Equal(1, picked!.RouteId);
        Assert.False(ActionApprovalRules.ForRequest([picked], InvestigationTriggerKinds.Alert).Needed);
    }

    [Fact]
    public void AChatRequest_NeedsApproval_OnARouteThatIsOff_AndWithNoRoute()
    {
        Assert.True(ActionApprovalRules.ForRequest([Route(1, Off)], InvestigationTriggerKinds.Chat).Needed);
        Assert.True(ActionApprovalRules.ForRequest([], InvestigationTriggerKinds.Chat).Needed);
        Assert.False(ActionApprovalRules.ForRequest([Route(1, Off)], InvestigationTriggerKinds.Alert).Needed);
    }

    [Fact]
    public void SinglePerson_HoldsOnlyWhenEveryRouteHasItOn()
    {
        Assert.True(ActionApprovalRules.ForRequest([Route(1, Required, single: true), Route(2, Required, single: true)], InvestigationTriggerKinds.Alert).SinglePerson);
        Assert.False(ActionApprovalRules.ForRequest([Route(1, Required, single: true), Route(2, Required, single: false)], InvestigationTriggerKinds.Alert).SinglePerson);
        Assert.False(ActionApprovalRules.ForRequest([Route(1, Required, single: false)], InvestigationTriggerKinds.Alert).SinglePerson);
    }

    [Fact]
    public void NoRoutes_MeansNoSinglePerson_AndAnUnresolvedMetricIsSkipped()
    {
        Assert.False(ActionApprovalRules.ForRequest([], InvestigationTriggerKinds.Alert).SinglePerson);
        var state = ActionApprovalRules.ForRequest([null, Route(1, Required, single: true)], InvestigationTriggerKinds.Alert);
        Assert.True(state.Needed);
        Assert.True(state.SinglePerson);
    }
}
