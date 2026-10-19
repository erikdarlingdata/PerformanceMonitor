/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Common.Tags;
using Xunit;

namespace Darling.Tests;

/// <summary>The shared sibling-reorder rule (#6094): display order, dense renumbering, edges and ties.</summary>
public sealed class TagSiblingOrderTests
{
    private static string Order(IEnumerable<TagSortAssignment> moves, IReadOnlyList<TagSibling> siblings) =>
        string.Join(",", moves.OrderBy(m => m.SortOrder).Select(m => siblings.First(s => s.Id == m.TagId).Name));

    [Fact]
    public void AllZeroStart_MovesByNameOrder_AndRenumbersEverySiblingDensely()
    {
        /* Every tag is created with sort order 0, so display order is by name: Apple, Banana, Cherry. */
        var siblings = new List<TagSibling> { new(3, "Cherry", 0), new(1, "Apple", 0), new(2, "Banana", 0) };

        var moves = TagSiblingOrder.Move(siblings, 3, TagMoveDirection.Up);

        Assert.Equal("Apple,Cherry,Banana", Order(moves, siblings));
        Assert.Equal(new[] { 0, 1, 2 }, moves.Select(m => m.SortOrder).OrderBy(i => i));
        Assert.Equal(3, moves.Count);
    }

    [Fact]
    public void Down_SwapsWithTheNextSibling()
    {
        var siblings = new List<TagSibling> { new(1, "A", 0), new(2, "B", 1), new(3, "C", 2) };

        var moves = TagSiblingOrder.Move(siblings, 1, TagMoveDirection.Down);

        Assert.Equal("B,A,C", Order(moves, siblings));
    }

    [Fact]
    public void SparseAndNegativeValues_AreRenumberedFromZero()
    {
        var siblings = new List<TagSibling> { new(1, "A", 40), new(2, "B", -5), new(3, "C", 1000) };

        var moves = TagSiblingOrder.Move(siblings, 3, TagMoveDirection.Up);

        Assert.Equal("B,C,A", Order(moves, siblings));
        Assert.Equal(new[] { 0, 1, 2 }, moves.Select(m => m.SortOrder).OrderBy(i => i));
    }

    [Fact]
    public void FirstUpAndLastDown_AreNoOps_AndAnUnknownTagMovesNothing()
    {
        var siblings = new List<TagSibling> { new(1, "A", 0), new(2, "B", 0) };

        Assert.Empty(TagSiblingOrder.Move(siblings, 1, TagMoveDirection.Up));
        Assert.Empty(TagSiblingOrder.Move(siblings, 2, TagMoveDirection.Down));
        Assert.Empty(TagSiblingOrder.Move(siblings, 99, TagMoveDirection.Up));
        Assert.False(TagSiblingOrder.CanMove(siblings, 1, TagMoveDirection.Up));
        Assert.False(TagSiblingOrder.CanMove(siblings, 2, TagMoveDirection.Down));
        Assert.False(TagSiblingOrder.CanMove(siblings, 99, TagMoveDirection.Down));
        Assert.True(TagSiblingOrder.CanMove(siblings, 1, TagMoveDirection.Down));
        Assert.True(TagSiblingOrder.CanMove(siblings, 2, TagMoveDirection.Up));
    }

    [Fact]
    public void ALoneSibling_CannotMove()
    {
        var siblings = new List<TagSibling> { new(1, "Only", 0) };

        Assert.Empty(TagSiblingOrder.Move(siblings, 1, TagMoveDirection.Up));
        Assert.Empty(TagSiblingOrder.Move(siblings, 1, TagMoveDirection.Down));
    }

    [Fact]
    public void TiesBreakByNameIgnoringCase_ThenById_LikeTheReaders()
    {
        var siblings = new List<TagSibling> { new(5, "beta", 1), new(2, "Alpha", 1), new(9, "ALPHA", 1) };

        var ordered = TagSiblingOrder.InDisplayOrder(siblings);

        /* Alpha and ALPHA fold equal, so the lower id comes first; beta follows. */
        Assert.Equal(new[] { 2, 9, 5 }, ordered.Select(s => s.Id));
    }

    [Fact]
    public void TwoMovesInARow_WalkATagAcrossTheList()
    {
        var siblings = new List<TagSibling> { new(1, "A", 0), new(2, "B", 0), new(3, "C", 0), new(4, "D", 0) };

        for (var step = 0; step < 3; step++)
        {
            var moves = TagSiblingOrder.Move(siblings, 1, TagMoveDirection.Down);
            siblings = moves.Select(m => siblings.First(s => s.Id == m.TagId) with { SortOrder = m.SortOrder }).ToList();
        }

        Assert.Equal(new[] { 2, 3, 4, 1 }, TagSiblingOrder.InDisplayOrder(siblings).Select(s => s.Id));
        Assert.False(TagSiblingOrder.CanMove(siblings, 1, TagMoveDirection.Down));
    }
}
