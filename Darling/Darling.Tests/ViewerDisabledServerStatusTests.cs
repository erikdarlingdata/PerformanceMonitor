/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Common;
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

    /// <summary>A healthy card whose deadlock collector ran, so the coverage line reads the card as covered.</summary>
    private static ServerSummaryItem Card(DarlingServer server) => new()
    {
        ServerId = server.ServerId,
        DisplayName = server.DisplayName,
        ServerName = server.ServerName,
        IsOnline = true,
        DeadlockCollectorBand = CollectorHealthClassifier.Healthy,
    };

    /// <summary>
    /// The Overview's total is the enabled fleet, while the sidebar's "Servers: N" counts every registered server, so
    /// a fleet with a disabled server shows two different numbers. Every roll-up line that states the Overview's count
    /// says which one it is.
    /// </summary>
    [Fact]
    public void OverviewRollup_LabelsItsCountAsEnabledServers()
    {
        var fleet = new List<DarlingServer>
        {
            Server(1, isEnabled: true),
            Server(2, isEnabled: false),
            Server(3, isEnabled: true),
        };
        var enabled = MainWindow.OverviewServers(fleet);
        var cards = enabled.Select(Card).ToList();

        var whole = OverviewCardView.BuildRollup(cards, new FleetTotals(), registeredCount: enabled.Count, search: null);

        Assert.Equal(2, whole.TotalServers);
        Assert.Equal("Monitoring 2 enabled servers", whole.MonitoringText);
        Assert.Equal("All 2 enabled servers healthy", whole.AllHealthyText);
        Assert.Equal("Deadlock coverage: read all 2 enabled servers", whole.DeadlockCoverageText);

        /* The search narrows the roll-up to the matching cards; the denominator is still the enabled fleet. */
        var narrowed = OverviewCardView.BuildRollup(cards, new FleetTotals(), registeredCount: enabled.Count, search: "target-1");

        Assert.Equal("Monitoring 1 of 2 enabled servers", narrowed.MonitoringText);

        var one = OverviewCardView.BuildRollup(cards.Take(1).ToList(), new FleetTotals(), registeredCount: 1, search: null);

        Assert.Equal("Monitoring 1 enabled server", one.MonitoringText);
        Assert.Equal("All 1 enabled server healthy", one.AllHealthyText);
        Assert.Equal("Deadlock coverage: read all 1 enabled server", one.DeadlockCoverageText);
    }
}
