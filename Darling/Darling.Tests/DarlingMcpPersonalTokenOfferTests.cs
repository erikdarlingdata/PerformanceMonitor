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
using ModelContextProtocol.Server;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9643: who is offered the personal token tools. A call made with a personal token is never offered them, whatever role the token holds and
/// whatever <c>mcp.allowServerAdmin</c> says, so one token cannot make, revoke or list another. The offer rule is the one the per-request tool
/// collection is built from, and a call for a tool outside that collection fails as an unknown tool.
/// </summary>
public sealed class DarlingMcpPersonalTokenOfferTests
{
    private static readonly string[] PersonalTokenTools = { "mint_personal_token", "revoke_personal_token", "get_personal_tokens" };

    private static DarlingMcpTokenSet SetWithPersonalTokens(bool serverAdmin) =>
        new("", DarlingMcpWritePolicy.Effective.Off,
            new List<DarlingMcpTokenSet.Entry>
            {
                new("ops", "ops-secret", McpTokenEntry.OperatorRole),
                new("viewer", "viewer-secret", McpTokenEntry.ReaderRole),
            },
            serverAdmin)
        {
            Personal = new PersonalTokenSettings(30, 5, McpTokenEntry.OperatorRole, null),
        };

    private static PersonalTokenRow Row(string role) =>
        new("dpt1_" + new string('a', 16), "owner-key", "Some Person", "laptop", role, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(7), null, null, null, null);

    [Theory]
    [InlineData(McpTokenEntry.OperatorRole)]
    [InlineData(McpTokenEntry.ReaderRole)]
    public void APersonalToken_IsNeverOfferedThePersonalTokenTools_WhateverItsRole(string role)
    {
        var caller = SetWithPersonalTokens(serverAdmin: true).PersonalCaller(Row(role));

        foreach (var tool in PersonalTokenTools)
        {
            Assert.False(caller.Policy.Offers(tool), tool);
        }
    }

    [Fact]
    public void APersonalOperatorToken_StillGetsTheOtherServerAdminTools_AndTheOrdinaryWrites()
    {
        var caller = SetWithPersonalTokens(serverAdmin: true).PersonalCaller(Row(McpTokenEntry.OperatorRole));

        Assert.True(caller.Policy.Offers("add_servers"));
        Assert.True(caller.Policy.Offers("create_mute_rule"));
    }

    [Fact]
    public void APersonalToken_CallByName_IsOutsideItsToolCollection()
    {
        var set = SetWithPersonalTokens(serverAdmin: true);
        var all = new McpServerPrimitiveCollection<McpServerTool>();
        foreach (var tool in McpToolsListBudgetTests.BuildServedTools().Tools)
        {
            all.Add(tool);
        }

        var personal = DarlingMcpWritePolicy.FilterToolCollection(all, set.PersonalCaller(Row(McpTokenEntry.OperatorRole)).Policy)
            .Select(t => t.ProtocolTool.Name).ToHashSet(StringComparer.Ordinal);
        var named = DarlingMcpWritePolicy.FilterToolCollection(all, set.PolicyFor(set.Named[0]))
            .Select(t => t.ProtocolTool.Name).ToHashSet(StringComparer.Ordinal);

        Assert.All(PersonalTokenTools, tool => Assert.DoesNotContain(tool, personal));
        Assert.All(PersonalTokenTools, tool => Assert.Contains(tool, named));
    }

    [Fact]
    public void ANamedOperatorToken_IsOfferedThePersonalTokenTools_OnlyWithServerAdminOn_AndAReaderNever()
    {
        var on = SetWithPersonalTokens(serverAdmin: true);
        var off = SetWithPersonalTokens(serverAdmin: false);

        foreach (var tool in PersonalTokenTools)
        {
            Assert.True(on.PolicyFor(on.Named[0]).Offers(tool), tool);
            Assert.False(off.PolicyFor(off.Named[0]).Offers(tool), tool);
            Assert.False(on.PolicyFor(on.Named[1]).Offers(tool), tool);
        }
    }
}
