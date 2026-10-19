/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5750 (review of #5769): Darling has ONE route payload, <c>DarlingMcpAlertTools.RoutePayload</c>. <c>get_notification_routes</c>
/// lists it and <c>set_notification_route_enabled</c> returns it, so a scoped route carries its scope on both, and a <c>*</c> route
/// reads <c>wildcard</c> with no family (it used to read an exact performance metric) the same way Lite's twin does.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class DarlingRoutePayloadTests
{
    private static JsonElement Payload(PgNotificationRouteStore.RouteSummary summary) =>
        JsonDocument.Parse(JsonSerializer.Serialize(DarlingMcpAlertTools.RoutePayload(summary))).RootElement;

    [Fact]
    public void AScopedRoute_CarriesItsScopeAndState_OnThePayloadSetNotificationRouteEnabledReturns()
    {
        var summary = new PgNotificationRouteStore.RouteSummary(7, "Performance", "performance", new[] { "slack" }, "", true, DateTime.UtcNow, null, 4)
        {
            ScopeTagName = "West",
            ExpandedServerCount = 3,
        };

        var json = Payload(summary);

        Assert.Equal(4, json.GetProperty("scope_tag_id").GetInt32());
        Assert.Equal("West", json.GetProperty("scope_tag_name").GetString());
        Assert.Equal(JsonValueKind.Null, json.GetProperty("scope_server_id").ValueKind);
        Assert.Equal(JsonValueKind.Null, json.GetProperty("scope_server_name").ValueKind);
        Assert.Equal(3, json.GetProperty("expanded_server_count").GetInt32());
        Assert.False(json.GetProperty("orphaned").GetBoolean());
        Assert.False(json.GetProperty("scope_unreadable").GetBoolean());
        Assert.Equal("family", json.GetProperty("match_kind").GetString());
        Assert.Equal("performance", json.GetProperty("family").GetString());
    }

    [Fact]
    public void AnOrphanedAndAnUnreadableRoute_ReportThemselves()
    {
        var orphan = Payload(new PgNotificationRouteStore.RouteSummary(8, "High CPU", null, new[] { "email" }, "", true, DateTime.UtcNow, 99, null)
        {
            ExpandedServerCount = 0,
            Orphaned = true,
        });
        Assert.True(orphan.GetProperty("orphaned").GetBoolean());
        Assert.Equal(99, orphan.GetProperty("scope_server_id").GetInt32());
        Assert.Equal("exact_metric", orphan.GetProperty("match_kind").GetString());

        var unreadable = Payload(new PgNotificationRouteStore.RouteSummary(9, "Performance", "performance", new[] { "email" }, "", true, DateTime.UtcNow, null, 5)
        {
            ScopeTagName = "East",
            ScopeUnreadable = true,
        });
        Assert.True(unreadable.GetProperty("scope_unreadable").GetBoolean());
        Assert.False(unreadable.GetProperty("orphaned").GetBoolean());
    }

    [Fact]
    public void AWildcardRoute_ReadsWildcardWithNoFamily_AndEveryOtherKindKeepsItsWords()
    {
        var wildcard = Payload(new PgNotificationRouteStore.RouteSummary(10, "*", null, new[] { "slack" }, "", true, DateTime.UtcNow, 1, null));
        Assert.Equal("wildcard", wildcard.GetProperty("match_kind").GetString());
        Assert.Equal(JsonValueKind.Null, wildcard.GetProperty("family").ValueKind);

        var exact = Payload(new PgNotificationRouteStore.RouteSummary(11, "High CPU", null, new[] { "slack" }, "", true, DateTime.UtcNow));
        Assert.Equal("exact_metric", exact.GetProperty("match_kind").GetString());
        Assert.Equal("performance", exact.GetProperty("family").GetString());

        // The same helper both products call.
        Assert.Equal("wildcard", NotificationRouteKinds.MatchKind(" * "));
        Assert.Null(NotificationRouteKinds.FamilyOf("*"));
        Assert.Equal("family", NotificationRouteKinds.MatchKind("Agent-Jobs"));
        Assert.Equal(AlertFamily.AgentJobs, NotificationRouteKinds.FamilyOf("Agent-Jobs"));
    }

    [Fact]
    public void BothDarlingRouteTools_ShareTheOneProjection()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs").ReplaceLineEndings("\n");

        /* #5632: the list passes each route's agent use, so the projection takes it as a second argument; the one-argument
           form the write tools call must still delegate to that same body, never build a second one. */
        Assert.Contains("routes = routes.Select(r => RoutePayload(r, agentUse))", source, StringComparison.Ordinal);
        Assert.Contains("internal static object RoutePayload(PgNotificationRouteStore.RouteSummary r) => RoutePayload(r, null);", source, StringComparison.Ordinal);
        Assert.Equal(2, source.Split("internal static object RoutePayload(", StringSplitOptions.None).Length - 1);
        Assert.Equal(1, source.Split("scope_unreadable = ", StringSplitOptions.None).Length - 1);
        Assert.DoesNotContain("exact_metric", source,StringComparison.Ordinal);
    }
}
