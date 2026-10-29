/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Service.FrontDoor;
using Xunit;

namespace Darling.Tests.FrontDoor;

/// <summary>
/// The front door sends a call by what the tool is (#6840 part P3): a tool that takes <c>server_name</c> goes where the directory puts that
/// server, and every other tool is in exactly one class (store-required, any one store, or every store). A tool added later is in none until
/// someone puts it in one, and this fails on it, so a new write cannot be fanned out and a new read cannot be sent to a guessed store.
/// </summary>
public sealed class DarlingFrontDoorCensusTests
{
    private static bool TakesServerName(ModelContextProtocol.Server.McpServerTool tool) =>
        tool.ProtocolTool.InputSchema.TryGetProperty("properties", out var properties)
        && properties.TryGetProperty(DarlingFrontDoorRouting.ServerArgument, out _);

    [Fact]
    public void EveryRegisteredTool_TakesServerName_OrIsInExactlyOneFrontDoorClass()
    {
        var (tools, _, _) = McpToolsListBudgetTests.BuildServedTools();
        Assert.True(tools.Count > 200, "the registered tool set should be the full Darling surface");

        var unclassified = tools
            .Where(t => !TakesServerName(t) && DarlingFrontDoorRouting.ClassesListedIn(t.ProtocolTool.Name) == 0)
            .Select(t => t.ProtocolTool.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();
        var inTwoClasses = tools
            .Where(t => DarlingFrontDoorRouting.ClassesListedIn(t.ProtocolTool.Name) > 1)
            .Select(t => t.ProtocolTool.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.True(
            unclassified.Count == 0,
            "These tools take no server_name and are in no front door class. Put each in DarlingFrontDoorRouting's FanOutTools, AnyOneStoreTools or "
            + $"ExtraStoreRequiredTools (a write belongs in the write policy's sets): {string.Join(", ", unclassified)}");
        Assert.True(inTwoClasses.Count == 0, $"These tools are in more than one front door class: {string.Join(", ", inTwoClasses)}");
    }

    [Fact]
    public void EveryNameInAFrontDoorList_IsARegisteredTool_SoATypoOrARemovedToolFails()
    {
        var (tools, _, _) = McpToolsListBudgetTests.BuildServedTools();
        var registered = tools.Select(t => t.ProtocolTool.Name).ToHashSet(StringComparer.Ordinal);

        var missing = DarlingFrontDoorRouting.FanOutTools
            .Concat(DarlingFrontDoorRouting.AnyOneStoreTools)
            .Concat(DarlingFrontDoorRouting.ExtraStoreRequiredTools)
            .Where(name => !registered.Contains(name))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.True(missing.Count == 0, $"These names are in a front door list and are no registered tool: {string.Join(", ", missing)}");
    }

    [Fact]
    public void ATool_InTheFanOutList_TakesNoServerName_SoTheListHoldsOnlyTheNoServerReads()
    {
        var (tools, _, _) = McpToolsListBudgetTests.BuildServedTools();
        var takesServer = tools.Where(TakesServerName).Select(t => t.ProtocolTool.Name).ToHashSet(StringComparer.Ordinal);

        var wrong = DarlingFrontDoorRouting.FanOutTools.Where(takesServer.Contains).OrderBy(n => n, StringComparer.Ordinal).ToList();

        Assert.True(wrong.Count == 0, $"These tools take server_name, so they need no entry in FanOutTools: {string.Join(", ", wrong)}");
    }
}
