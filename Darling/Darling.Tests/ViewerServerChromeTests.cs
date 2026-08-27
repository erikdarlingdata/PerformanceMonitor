/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the ported MainWindow sidebar chrome: the server-row status-dot derivation (from collection
/// freshness, the same signal the Overview cards use), the favorite flag, the <see cref="ViewerServerEntry"/>
/// display helpers the Manage Servers grid binds, and the load-bearing clauses of the new status reads.
/// </summary>
public sealed class ViewerServerChromeTests
{
    private static DarlingServer Server() => new(1, "SQL2022", "Prod", true, 16);

    [Fact]
    public void DotStatus_IsUnknown_UntilFreshnessIsApplied()
    {
        Assert.Equal("Unknown", Server().DotStatus);
    }

    [Fact]
    public void ApplyFreshness_FreshCollection_IsOnline()
    {
        var now = DateTime.UtcNow;
        var server = Server();

        server.ApplyFreshness(now.AddSeconds(-30), now);

        Assert.True(server.IsOnline);
        Assert.False(server.HasCollectorErrors);
        Assert.Equal("Online", server.DotStatus);
    }

    [Fact]
    public void ApplyFreshness_StaleCollection_IsWarning()
    {
        var now = DateTime.UtcNow;
        var server = Server();

        /* Older than 2x the 1-minute collector cadence but not yet offline (< 15 min). */
        server.ApplyFreshness(now.AddMinutes(-5), now);

        Assert.True(server.IsOnline);
        Assert.True(server.HasCollectorErrors);
        Assert.Equal("Warning", server.DotStatus);
    }

    [Fact]
    public void ApplyFreshness_OldCollection_IsOffline()
    {
        var now = DateTime.UtcNow;
        var server = Server();

        server.ApplyFreshness(now.AddMinutes(-30), now);

        Assert.False(server.IsOnline);
        Assert.Equal("Offline", server.DotStatus);
    }

    [Fact]
    public void ApplyFreshness_NeverCollected_IsAwaitingFirstCollection_NotOffline()
    {
        /* Never-collected = the service hasn't reached the server yet (bootstrap). Never the red Offline —
           a queued server is not a dead one (24-server field incident, 2026-07-17).

           This asserted the grey "Unknown" dot until #2473, and the assertion was the defect written down:
           the Overview card said amber "Awaiting first collection" for the same server, off the same
           freshness call, one panel over. The dot now says what the card says. */
        var server = Server();

        server.ApplyFreshness(null, DateTime.UtcNow);

        Assert.Null(server.IsOnline);
        Assert.True(server.AwaitingFirstCollection);
        Assert.Equal("Awaiting first collection", server.DotStatus);
    }

    [Fact]
    public void DotStatus_RaisesChangeNotification_ForLiveDotUpdates()
    {
        var server = Server();
        var changed = 0;
        server.PropertyChanged += (_, e) =>
        {
            if (e.PropertyName is nameof(DarlingServer.DotStatus)) changed++;
        };

        server.ApplyFreshness(DateTime.UtcNow, DateTime.UtcNow);

        Assert.True(changed > 0);
    }

    [Fact]
    public void IsFavorite_RaisesChangeNotification_ForLiveStarUpdates()
    {
        var server = Server();
        var raised = false;
        server.PropertyChanged += (_, e) =>
        {
            if (e.PropertyName == nameof(DarlingServer.IsFavorite)) raised = true;
        };

        server.IsFavorite = true;

        Assert.True(raised);
    }

    [Theory]
    [InlineData(false, "Prod", "SQL2022")]
    [InlineData(true, "Prod (Read-Only)", "SQL2022 (Read-Only)")]
    public void ViewerServerEntry_DisplayHelpers_ReflectReadOnlyIntent(bool readOnly, string expectedDisplay, string expectedServer)
    {
        var entry = new ViewerServerEntry
        {
            ServerName = "SQL2022",
            DisplayName = "Prod",
            ReadOnlyIntent = readOnly
        };

        Assert.Equal(expectedDisplay, entry.DisplayNameWithIntent);
        Assert.Equal(expectedServer, entry.ServerNameDisplay);
    }

    [Fact]
    public void ViewerServerEntry_AuthenticationDisplay_MapsEachMode()
    {
        Assert.Equal("Windows", new ViewerServerEntry { AuthenticationType = AuthenticationTypes.Windows }.AuthenticationDisplay);
        Assert.Equal("SQL Server", new ViewerServerEntry { AuthenticationType = AuthenticationTypes.SqlServer }.AuthenticationDisplay);
        Assert.Equal("Microsoft Entra MFA", new ViewerServerEntry { AuthenticationType = AuthenticationTypes.EntraMFA }.AuthenticationDisplay);
        Assert.Equal("Azure — Service Principal", new ViewerServerEntry { AuthenticationType = AuthenticationTypes.ServicePrincipal }.AuthenticationDisplay);
        Assert.Equal("Azure — Managed Identity", new ViewerServerEntry { AuthenticationType = AuthenticationTypes.ManagedIdentity }.AuthenticationDisplay);
    }

    [Fact]
    public void ServerFreshnessSql_ReadsNewestCollectionPerServer()
    {
        Assert.Contains("MAX(collection_time)", ViewerDataService.ServerFreshnessSql, StringComparison.Ordinal);
        Assert.Contains("FROM v_collection_log", ViewerDataService.ServerFreshnessSql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY server_id", ViewerDataService.ServerFreshnessSql, StringComparison.Ordinal);
        /* Excludes the fleet retention run-record sentinel (server_id 0) so it never appears as a phantom
           server in the freshness dictionary. */
        Assert.Contains("server_id <> 0", ViewerDataService.ServerFreshnessSql, StringComparison.Ordinal);
    }

    [Fact]
    public void StoreSizeSql_UsesPgDatabaseSize()
    {
        Assert.Contains("pg_database_size", ViewerDataService.StoreSizeSql, StringComparison.Ordinal);
    }
}
