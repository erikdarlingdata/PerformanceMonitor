/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>The fleet-tag write rules (#5085): name, duplicate, depth, cycle, parent and colour.</summary>
public sealed class ServerTagRulesTests
{
    /* A chain Prod(1) > East(2) > Web(3) > Edge(4), plus a root Test(10) with child Lab(11). */
    private static readonly List<ServerTagRow> Tree =
    [
        new(1, "Prod", null, 0, null),
        new(2, "East", 1, 0, null),
        new(3, "Web", 2, 0, null),
        new(4, "Edge", 3, 0, null),
        new(10, "Test", null, 0, null),
        new(11, "Lab", 10, 0, null),
    ];

    private static ServerTagEdit Move(int? parent) => new(false, null, false, null, true, parent);

    [Fact]
    public void Create_UnderADepthThreeParent_IsRefused_AndUnderDepthTwoIsAllowed()
    {
        var refused = Assert.IsType<ServerTagWriteResult.Refused>(ServerTagRules.CheckCreate(Tree, "Deep", 4, null, out _, out _));
        Assert.Equal("depth_limit", refused.Code);
        Assert.Equal("Tags can nest at most four levels deep.", refused.Message);

        Assert.Null(ServerTagRules.CheckCreate(Tree, "Ok", 3, null, out _, out _));
    }

    [Fact]
    public void Move_OfAHeightOneSubtreeUnderDepthTwo_IsRefused_AndMoveToRootIsAllowed()
    {
        /* Test(10) has height 1; under Web(3, depth 2) it would reach depth 4 (0-based). */
        var refused = Assert.IsType<ServerTagWriteResult.Refused>(ServerTagRules.CheckUpdate(Tree, 10, Move(3), out _, out _));
        Assert.Equal("depth_limit", refused.Code);

        Assert.Null(ServerTagRules.CheckUpdate(Tree, 4, Move(null), out _, out _));
    }

    [Fact]
    public void Move_UnderItselfOrAGrandchild_IsACycle()
    {
        var self = Assert.IsType<ServerTagWriteResult.Refused>(ServerTagRules.CheckUpdate(Tree, 1, Move(1), out _, out _));
        Assert.Equal("cycle", self.Code);
        var grand = Assert.IsType<ServerTagWriteResult.Refused>(ServerTagRules.CheckUpdate(Tree, 1, Move(3), out _, out _));
        Assert.Equal("cycle", grand.Code);
    }

    [Fact]
    public void Duplicates_AreRefusedIgnoringCase_OnCreateRenameAndMove()
    {
        Assert.IsType<ServerTagWriteResult.Conflict>(ServerTagRules.CheckCreate(Tree, "  prod ", null, null, out _, out _));

        var rename = new ServerTagEdit(true, "TEST", false, null, false, null);
        Assert.IsType<ServerTagWriteResult.Conflict>(ServerTagRules.CheckUpdate(Tree, 1, rename, out _, out _));

        /* Moving a tag named Lab into a parent that already has a Lab. */
        var withTwin = new List<ServerTagRow>(Tree) { new(12, "Lab", 1, 0, null) };
        Assert.IsType<ServerTagWriteResult.Conflict>(ServerTagRules.CheckUpdate(withTwin, 12, Move(10), out _, out _));
    }

    [Fact]
    public void ACaseOnlySelfRename_IsAllowed()
    {
        var rename = new ServerTagEdit(true, "PROD", false, null, false, null);
        Assert.Null(ServerTagRules.CheckUpdate(Tree, 1, rename, out var name, out _));
        Assert.Equal("PROD", name);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("a\tb")]
    public void BadNames_AreRefused(string name)
    {
        var refused = Assert.IsType<ServerTagWriteResult.Refused>(ServerTagRules.CheckName(name, out _));
        Assert.Equal("bad_name", refused.Code);
    }

    [Fact]
    public void Names_AreTrimmed_AndCappedAtOneHundred()
    {
        Assert.Null(ServerTagRules.CheckName("  Prod  ", out var clean));
        Assert.Equal("Prod", clean);
        Assert.Null(ServerTagRules.CheckName(new string('a', 100), out _));
        Assert.Equal("bad_name", ServerTagRules.CheckName(new string('a', 101), out _)!.Code);
    }

    [Fact]
    public void Colours_AreUpperCased_AndMalformedOnesRefused()
    {
        Assert.Null(ServerTagRules.CheckColour("#416fa6", out var colour));
        Assert.Equal("#416FA6", colour);
        Assert.Null(ServerTagRules.CheckColour(null, out var none));
        Assert.Null(none);

        foreach (var bad in new[] { "#12345", "red", string.Empty })
        {
            var refused = Assert.IsType<ServerTagWriteResult.Refused>(ServerTagRules.CheckColour(bad, out _));
            Assert.Equal("bad_colour", refused.Code);
            Assert.Equal("colour must be #RRGGBB, e.g. #416FA6, or null to clear it.", refused.Message);
        }
    }

    [Fact]
    public void AMissingParent_IsUnknownParent_AndAMissingTagIsNotFound()
    {
        var nf = Assert.IsType<ServerTagWriteResult.NotFound>(ServerTagRules.CheckCreate(Tree, "X", 99, null, out _, out _));
        Assert.Equal("unknown_parent", nf.Code);
        var missing = Assert.IsType<ServerTagWriteResult.NotFound>(ServerTagRules.CheckUpdate(Tree, 99, Move(null), out _, out _));
        Assert.Null(missing.Code);
        Assert.IsType<ServerTagWriteResult.NotFound>(ServerTagRules.CheckDelete(Tree, 99));
    }

    [Fact]
    public void AMalformedCycleInTheData_DoesNotHangTheWalks()
    {
        var cyclic = new List<ServerTagRow> { new(1, "A", 2, 0, null), new(2, "B", 1, 0, null) };
        Assert.True(ServerTagRules.Depth(cyclic, 1) <= ServerTagRules.MaxWalkSteps);
        Assert.True(ServerTagRules.Height(cyclic, 1) <= ServerTagRules.MaxWalkSteps);
        Assert.Contains(2, ServerTagRules.Descendants(cyclic, 1));
    }
}
