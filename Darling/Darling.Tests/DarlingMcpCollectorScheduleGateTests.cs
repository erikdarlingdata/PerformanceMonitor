/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>The write gate of the collector schedule tools (#6227): served only with <c>mcp.allowWrites</c>, never offered to the
/// investigation agent, while the read is served like every other read.</summary>
public sealed class DarlingMcpCollectorScheduleGateTests
{
    private static readonly string[] Writes =
    {
        "disable_collector", "enable_collector", "set_collector_frequency", "set_collector_run_at", "set_collector_retention",
    };

    [Fact]
    public void EachWrite_IsRefusedWithoutAllowWrites_AndServedWithIt()
    {
        foreach (var tool in Writes)
        {
            Assert.True(DarlingMcpWritePolicy.IsWrite(tool), tool);
            Assert.False(DarlingMcpWritePolicy.Effective.Off.Offers(tool), tool);
            Assert.True(new DarlingMcpWritePolicy.Effective(Writes: true, ServerAdmin: false).Offers(tool), tool);
        }
    }

    [Fact]
    public void EachWrite_IsNeverOfferedToTheInvestigationAgent_EvenIfAnActionSetNamesIt()
    {
        var everything = new System.Collections.Generic.HashSet<string>(Writes, StringComparer.Ordinal);
        var investigating = new DarlingMcpWritePolicy.Effective(Writes: false, ServerAdmin: false, Investigations: true);
        foreach (var tool in Writes)
        {
            Assert.Contains(tool, DarlingMcpWritePolicy.NeverAgentTools);
            Assert.False(investigating.Offers(tool), tool);
            Assert.False(investigating.Offers(tool, everything), tool);
        }
    }

    [Fact]
    public void TheRead_IsNotAWrite_SoItIsServedWithWritesOff_AndToTheInvestigationAgent()
    {
        Assert.False(DarlingMcpWritePolicy.IsWrite("get_collector_schedules"));
        Assert.True(DarlingMcpWritePolicy.Effective.Off.Offers("get_collector_schedules"));
        Assert.True(Writes.All(w => w != "get_collector_schedules"));
    }
}
