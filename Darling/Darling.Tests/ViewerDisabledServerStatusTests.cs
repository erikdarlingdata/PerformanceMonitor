/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A server the operator disabled collects nothing on purpose, so its silence is not an outage. The sidebar dot
/// must not read Offline for it, and the Overview must not build a card for it, which is what kept it at the top
/// of Needs Attention for as long as it stayed disabled. The MCP fleet reader already scopes its fleet to
/// enabled servers; these pin the viewer to the same answer.
/// </summary>
public sealed class ViewerDisabledServerStatusTests
{
    private static readonly DateTime Now = new(2026, 11, 5, 12, 0, 0, DateTimeKind.Utc);

    private static DarlingServer Server(int id, bool isEnabled) =>
        new(id, $"target-{id}", $"target-{id}", isEnabled, 16, registeredAt: Now.AddDays(-30));

    [Fact]
    public void DisabledServer_DotDoesNotReadOffline()
    {
        var disabled = Server(1, isEnabled: false);

        /* Three days without a collection: on an enabled server this is Offline. */
        disabled.ApplyFreshness(Now.AddDays(-3), Now);

        Assert.NotEqual("Offline", disabled.DotStatus);
        Assert.Equal("Unknown", disabled.DotStatus);
    }

    [Fact]
    public void EnabledServer_WithTheSameSilence_ReadsOffline()
    {
        var enabled = Server(2, isEnabled: true);

        enabled.ApplyFreshness(Now.AddDays(-3), Now);

        Assert.Equal("Offline", enabled.DotStatus);
    }

    [Fact]
    public void DisabledServer_TooltipSaysCollectionIsDisabled()
    {
        var disabled = Server(3, isEnabled: false);
        disabled.ConnectDownSince = Now.AddHours(-2);

        disabled.ApplyFreshness(Now.AddDays(-3), Now);

        Assert.Contains("disabled", disabled.DotTooltip, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Offline", disabled.DotTooltip, StringComparison.Ordinal);
    }

    [Fact]
    public void OverviewServers_LeavesDisabledServersOut()
    {
        var fleet = new List<DarlingServer>
        {
            Server(1, isEnabled: true),
            Server(2, isEnabled: false),
            Server(3, isEnabled: true),
        };

        var overview = MainWindow.OverviewServers(fleet);

        Assert.Equal(new[] { 1, 3 }, overview.Select(s => s.ServerId).ToArray());
    }

    [Fact]
    public void OverviewServers_KeepsAnAllEnabledFleetWhole()
    {
        var fleet = new List<DarlingServer> { Server(1, isEnabled: true), Server(2, isEnabled: true) };

        Assert.Equal(2, MainWindow.OverviewServers(fleet).Count);
    }
}
