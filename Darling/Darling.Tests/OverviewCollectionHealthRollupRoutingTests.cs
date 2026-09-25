/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4226: the Overview loader's per-server cards, the status bar's collector-health text, and the per-server
/// tab's permission-denied badge must all read the fleet-wide rollup-backed
/// <see cref="ViewerDataService.GetFleetCollectionHealthByServerAsync"/>, not a raw <c>collection_log</c> scan
/// per server (<c>ViewerDataService.GetCollectionHealthAsync</c>), a second raw fleet scan
/// (<c>ViewerDataService.GetFleetCollectionHealthAsync</c>) every 30 s tick, or the badge's own raw
/// per-server-tab scan (<see cref="ViewerDataService.PermissionDeniedCollectorCountSql"/>) on every 1 min
/// auto-refresh plus every tab activation. Source-text pins, deliberately: the regression this guards against
/// COMPILES (the old reads still exist, for the Collection Health tab and for Lite's parity twin), so only
/// reading the call sites catches a caller quietly routed back to raw.
/// </summary>
public sealed class OverviewCollectionHealthRollupRoutingTests
{
    [Fact]
    public void TheOverviewCard_ReadsTheFleetByServerRollup_NotThePerServerRawScan()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.Overview.cs");

        var countsMethodStart = source.IndexOf("GetCollectorHealthCountsAsync(int serverId", StringComparison.Ordinal);
        Assert.True(countsMethodStart >= 0, "GetCollectorHealthCountsAsync has moved or been renamed — update this pin's anchor.");

        var methodEnd = source.IndexOf("\n    }", countsMethodStart, StringComparison.Ordinal);
        var methodBody = source.Substring(countsMethodStart, methodEnd - countsMethodStart);
        Assert.Contains("GetFleetCollectionHealthByServerAsync", methodBody, StringComparison.Ordinal);
        Assert.DoesNotContain("await GetCollectionHealthAsync(serverId", methodBody, StringComparison.Ordinal);
    }

    [Fact]
    public void TheStatusBar_ReadsTheFleetByServerRollup_NotTheOldRawPair()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.ServerManagement.cs");

        var methodStart = source.IndexOf("private async Task UpdateCollectorHealthTextAsync", StringComparison.Ordinal);
        Assert.True(methodStart >= 0, "UpdateCollectorHealthTextAsync has moved or been renamed — update this pin's anchor.");

        var methodEnd = source.IndexOf("\n    }", methodStart, StringComparison.Ordinal);
        var methodBody = source.Substring(methodStart, methodEnd - methodStart);

        Assert.Contains("GetFleetCollectionHealthByServerAsync", methodBody, StringComparison.Ordinal);
        Assert.DoesNotContain("GetCollectionHealthAsync(serverId.Value)", methodBody, StringComparison.Ordinal);
        Assert.DoesNotContain("GetFleetCollectionHealthAsync()", methodBody, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4226's second pass: the server-tab badge (<c>ViewerServerTab.UpdatePermissionDeniedBadgeAsync</c>) must
    /// not issue <see cref="ViewerDataService.PermissionDeniedCollectorCountSql"/> itself — that scan is kept
    /// only as the documented raw shape Lite's parity twin still runs and this pin still holds it to.
    /// </summary>
    [Fact]
    public void TheServerTabBadge_ReadsTheFleetByServerRollup_NotItsOwnRawScan()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.CollectionHealth.cs");

        var methodStart = source.IndexOf("public async Task<int> GetPermissionDeniedCollectorCountAsync", StringComparison.Ordinal);
        Assert.True(methodStart >= 0, "GetPermissionDeniedCollectorCountAsync has moved or been renamed — update this pin's anchor.");

        var methodEnd = source.IndexOf("\n    }", methodStart, StringComparison.Ordinal);
        var methodBody = source.Substring(methodStart, methodEnd - methodStart);

        Assert.Contains("GetFleetCollectionHealthByServerAsync", methodBody, StringComparison.Ordinal);
        Assert.DoesNotContain("CreateCommand(PermissionDeniedCollectorCountSql", methodBody, StringComparison.Ordinal);
    }

    /// <summary>The new fleet-by-server raw statement is shaped for
    /// <see cref="CollectionHealthRollupSupport.ComposeFleetSql"/>: thirteen columns, <c>server_id</c> first,
    /// grouped by <c>server_id, collector_name</c>, scoped to enabled servers (the viewer's existing
    /// <c>FleetCollectionHealthSql</c> scope) rather than the service's <c>server_id &lt;&gt; 0</c>.</summary>
    [Fact]
    public void FleetCollectionHealthByServerSql_IsShapedForTheComposer_AndScopedToEnabledServers()
    {
        var sql = ViewerDataService.FleetCollectionHealthByServerSql;

        Assert.Contains("server_id,\r\n    collector_name,", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY server_id, collector_name", sql, StringComparison.Ordinal);
        Assert.Contains("server_id IN (SELECT server_id FROM config_monitored_servers WHERE is_enabled)", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE collection_time >= $1", sql, StringComparison.Ordinal);

        /* Composing must not throw — the anchor ComposeFleetSql derives the head slice from is present exactly
           once, same as the service's own raw statement. */
        var composed = CollectionHealthRollupSupport.ComposeFleetSql(sql);
        Assert.Contains("collect." + TimescaleSupport.CollectionHealthHourlyView, composed, StringComparison.Ordinal);
    }
}
