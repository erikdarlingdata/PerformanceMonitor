/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>#6157: the viewer's Availability Groups card says "As reported by" with the server's display name, not its connection key.</summary>
public sealed class ViewerAvailabilityGroupDisplayNameTests
{
    private static AgTopologyCard CardFor(int serverId, string key) =>
        AgTopology.BuildCards(
            [new AgTopologyReplicaRow { ServerId = serverId, ServerName = key, AgName = "AG1", ReplicaServerName = "N1", RoleDesc = "PRIMARY" }],
            Array.Empty<AgTopologyDatabaseRow>()).Single();

    [Fact]
    public void TheCardSubtitle_NamesTheReportingServerByItsDisplayName()
    {
        var card = CardFor(1, "host-a,1433");
        ViewerDataService.ApplyDisplayNames([card], [new DarlingServer(1, "host-a,1433", "Orders", true, 16)]);
        Assert.StartsWith("As reported by Orders · ", card.SubtitleDisplay);
    }

    [Fact]
    public void TheCardSubtitle_KeepsTheKey_WhenTheServerHasNoDisplayNameOrIsNotListed()
    {
        var blank = CardFor(1, "host-a,1433");
        var unlisted = CardFor(2, "host-b,1433");
        ViewerDataService.ApplyDisplayNames([blank, unlisted], [new DarlingServer(1, "host-a,1433", "", true, 16)]);
        Assert.StartsWith("As reported by host-a,1433 · ", blank.SubtitleDisplay);
        Assert.StartsWith("As reported by host-b,1433 · ", unlisted.SubtitleDisplay);
    }
}
