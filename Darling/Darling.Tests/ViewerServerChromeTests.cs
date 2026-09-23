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
        Assert.False(server.CollectionStale);
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
        Assert.True(server.CollectionStale);
        Assert.Equal("Warning", server.DotStatus);
    }

    [Fact]
    public void ApplyFreshness_OldCollection_IsOffline()
    {
        var now = DateTime.UtcNow;
        var server = Server();

        /* -31: exactly 30 minutes is the shared collection-stopped boundary and bands Stale (strict >, #2794). */
        server.ApplyFreshness(now.AddMinutes(-31), now);

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

    /// <summary>
    /// #3895: one newest-row probe per REGISTERED server, not a <c>GROUP BY</c> over every retained row of
    /// <c>collection_log</c> — which is what the sidebar re-read on every refresh tick for a handful of
    /// timestamps. Every registry row, disabled included (Manage Servers shows their last collection too),
    /// and unbounded, because a weeks-old "last collected" is still the answer.
    /// </summary>
    [Fact]
    public void ServerFreshnessSql_ReadsNewestCollectionPerServer()
    {
        var sql = ViewerDataService.ServerFreshnessSql;
        Assert.Contains("FROM servers AS s", sql, StringComparison.Ordinal);
        Assert.Contains("CROSS JOIN LATERAL", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_collection_log", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = s.server_id", sql, StringComparison.Ordinal);
        Assert.Matches(@"ORDER BY collection_time DESC\s+LIMIT 1", sql);
        Assert.DoesNotContain("GROUP BY", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("is_enabled", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$1", sql, StringComparison.Ordinal);
        /* Excludes the fleet retention run-record sentinel (server_id 0) so it never appears as a phantom
           server in the freshness dictionary. */
        Assert.Contains("server_id <> 0", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void StoreSizeSql_UsesPgDatabaseSize()
    {
        Assert.Contains("pg_database_size", ViewerDataService.StoreSizeSql, StringComparison.Ordinal);
    }
}
