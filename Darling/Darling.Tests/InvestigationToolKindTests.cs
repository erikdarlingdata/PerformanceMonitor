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
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8461: Darling's investigation gate is built from the shared kind list (<see cref="InvestigationToolNames"/>), the one both apps
/// read, so a shared name is gated by the same rule in Lite. Every investigation name Darling classifies is placed on that list as the
/// kind Darling treats it as, so the next investigation tool added to either app fails here until it is placed. Lite's twin is
/// <c>Lite.Tests/InvestigationToolKindTests.cs</c>.
/// </summary>
public sealed class InvestigationToolKindTests
{
    [Fact]
    public void TheGatedSet_IsTheSharedStateChangingList()
    {
        Assert.Equal(InvestigationToolNames.StateChanging.Order(StringComparer.Ordinal), DarlingMcpWritePolicy.InvestigationTools.Order(StringComparer.Ordinal));
        Assert.All(InvestigationToolNames.StateChanging, name => Assert.True(DarlingMcpWritePolicy.IsInvestigation(name), name));
    }

    /// <summary>Every tool the investigation tool class declares, whatever its name, is placed on the shared list as a read or a state change,
    /// so a tool added here fails until it is placed, as the Lite twin's declared-names fact does.</summary>
    [Fact]
    public void EveryDeclaredInvestigationTool_IsPlacedOnTheSharedList()
    {
        var declared = typeof(DarlingMcpInvestigationTools).GetMethods()
            .Select(m => m.GetCustomAttributes(typeof(ModelContextProtocol.Server.McpServerToolAttribute), false)
                .Cast<ModelContextProtocol.Server.McpServerToolAttribute>().SingleOrDefault()?.Name)
            .OfType<string>()
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.NotEmpty(declared);
        Assert.All(declared, name => Assert.True(InvestigationToolNames.IsPlaced(name), $"{name} is neither a shared read nor a shared state-changing tool; place it in InvestigationToolNames"));
        Assert.Equal(DarlingMcpWritePolicy.InvestigationTools.Order(StringComparer.Ordinal), declared.Where(InvestigationToolNames.IsStateChanging));
    }

    [Fact]
    public void EveryInvestigationNameOnTheReadOnlyList_IsASharedRead_AndEverySharedRead_IsOnIt()
    {
        var readOnlyInvestigationNames = DarlingMcpToolClassification.ReadOnlyTools
            .Where(n => n.Contains("investigation", StringComparison.Ordinal))
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(InvestigationToolNames.Reads.Order(StringComparer.Ordinal), readOnlyInvestigationNames);
        Assert.Empty(InvestigationToolNames.Reads.Intersect(InvestigationToolNames.StateChanging, StringComparer.Ordinal));
        Assert.Empty(DarlingMcpToolClassification.ReadOnlyTools.Intersect(DarlingMcpWritePolicy.InvestigationTools, StringComparer.Ordinal));
    }

    /// <summary>The reads are served under every policy, the one with everything off included; the state-changing tools under none of those.</summary>
    [Fact]
    public void TheSharedReads_AreOfferedWithEverySettingOff_AndTheStateChangingTools_AreNot()
    {
        var off = DarlingMcpWritePolicy.Effective.Off;
        Assert.All(InvestigationToolNames.Reads, name => Assert.True(off.Offers(name), $"{name} is a read and is offered with every setting off"));
        Assert.All(InvestigationToolNames.StateChanging, name => Assert.False(off.Offers(name), $"{name} changes investigation state and needs the investigations setting"));
    }
}
