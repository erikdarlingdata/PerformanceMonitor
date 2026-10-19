/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Common.Tags;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Real-DuckDB round-trip pins for the Lite fleet-tag store (#2020 2b-i). Exercised against a real store on
/// purpose: the tree's mechanics diverge from the Darling twin precisely where DuckDB does — C#-assigned ids
/// (no IDENTITY), an in-code recursive-walk cascade (no self-FK), a code-enforced unique-name-per-parent (no
/// partial index), <c>ON CONFLICT DO NOTHING</c> bulk assign, and <c>now()::TIMESTAMP</c> — none of which a
/// compile or a mock would catch. Mirrors the semantics <c>DarlingServerTagsTests</c> pins for the viewer.
/// </summary>
public sealed class LiteServerTagsStoreTests : IClassFixture<SharedDuckDbFixture>
{
    private readonly LocalDataService _service;

    public LiteServerTagsStoreTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _service = new LocalDataService(fixture.DuckDb);
    }

    [Fact]
    public async Task Create_AssignsAscendingIds_AndAPaletteColourFromTheId()
    {
        var prodId = await _service.CreateServerTagAsync("Production", parentId: null);
        var devId = await _service.CreateServerTagAsync("Dev", parentId: null);

        Assert.Equal(1, prodId);
        Assert.Equal(2, devId);

        var tags = await _service.GetServerTagsAsync();
        var prod = tags.Single(t => t.Id == prodId);
        Assert.Null(prod.ParentId);
        /* Colour is auto-assigned from the palette, rotated by id — the shared rule, so it matches the viewer. */
        Assert.Equal(TagColorPalette.ForTagId(prodId), prod.Colour);
    }

    [Fact]
    public async Task Create_RejectsADuplicateNameUnderTheSameParent_ButAllowsItUnderAnother()
    {
        var root = await _service.CreateServerTagAsync("Environments", parentId: null);
        await _service.CreateServerTagAsync("Prod", parentId: root);

        /* Same name, same parent → rejected (the invariant Darling's unique index enforces). */
        await Assert.ThrowsAsync<System.InvalidOperationException>(
            () => _service.CreateServerTagAsync("prod", parentId: root));

        /* Same name under a DIFFERENT parent (here: a root) is fine. */
        var otherRoot = await _service.CreateServerTagAsync("Prod", parentId: null);
        Assert.Contains(await _service.GetServerTagsAsync(), t => t.Id == otherRoot);
    }

    [Fact]
    public async Task Rename_And_SetColour_RoundTrip_AndColourClearsToNull()
    {
        var id = await _service.CreateServerTagAsync("Staging", parentId: null);

        await _service.RenameServerTagAsync(id, "Pre-Prod");
        await _service.SetServerTagColorAsync(id, "#416FA6");
        var afterSet = (await _service.GetServerTagsAsync()).Single(t => t.Id == id);
        Assert.Equal("Pre-Prod", afterSet.Name);
        Assert.Equal("#416FA6", afterSet.Colour);

        await _service.SetServerTagColorAsync(id, null);   // back to neutral
        var afterClear = (await _service.GetServerTagsAsync()).Single(t => t.Id == id);
        Assert.Null(afterClear.Colour);
    }

    [Fact]
    public async Task Move_SwapsWithTheNeighbour_RenumbersTheSiblingsDensely_AndLeavesOtherParentsAlone()
    {
        var a = await _service.CreateServerTagAsync("A", parentId: null);
        var b = await _service.CreateServerTagAsync("B", parentId: null);
        var c = await _service.CreateServerTagAsync("C", parentId: null);
        var child = await _service.CreateServerTagAsync("Child", parentId: a);

        /* Every tag starts at sort order 0, so the move must renumber, not just swap two zeros. */
        Assert.True(await _service.MoveServerTagAsync(c, TagMoveDirection.Up));

        var tags = await _service.GetServerTagsAsync();
        var roots = tags.Where(t => t.ParentId is null).OrderBy(t => t.SortOrder).ToList();
        Assert.Equal(new[] { a, c, b }, roots.Select(t => t.Id));
        Assert.Equal(new[] { 0, 1, 2 }, roots.Select(t => t.SortOrder));
        Assert.Equal(0, tags.Single(t => t.Id == child).SortOrder);

        Assert.True(await _service.MoveServerTagAsync(a, TagMoveDirection.Down));
        var after = (await _service.GetServerTagsAsync()).Where(t => t.ParentId is null).OrderBy(t => t.SortOrder).Select(t => t.Id);
        Assert.Equal(new[] { c, a, b }, after);
    }

    [Fact]
    public async Task Move_PastAnEdge_OrOfAMissingTag_WritesNothing()
    {
        var a = await _service.CreateServerTagAsync("A", parentId: null);
        var b = await _service.CreateServerTagAsync("B", parentId: null);

        Assert.False(await _service.MoveServerTagAsync(a, TagMoveDirection.Up));
        Assert.False(await _service.MoveServerTagAsync(b, TagMoveDirection.Down));
        Assert.False(await _service.MoveServerTagAsync(999, TagMoveDirection.Up));
        Assert.All(await _service.GetServerTagsAsync(), t => Assert.Equal(0, t.SortOrder));
    }

    [Fact]
    public async Task Assign_IsIdempotent_AndUnassignAndClearRemoveRows()
    {
        var tag = await _service.CreateServerTagAsync("Prod", parentId: null);

        await _service.AssignServerTagAsync(new[] { 100, 200 }, tag);
        await _service.AssignServerTagAsync(new[] { 100 }, tag);   // re-assign is a no-op (ON CONFLICT)

        var assigned = await _service.GetServerTagAssignmentsAsync();
        Assert.Equal(2, assigned.Count(a => a.TagId == tag));

        await _service.UnassignServerTagAsync(new[] { 100 }, tag);
        Assert.Single(await _service.GetServerTagAssignmentsAsync(), a => a.TagId == tag);

        await _service.ClearServerTagsForServerAsync(200);
        Assert.DoesNotContain(await _service.GetServerTagAssignmentsAsync(), a => a.TagId == tag);
    }

    [Fact]
    public async Task Delete_CascadesTheSubtree_AndItsAssignments()
    {
        var parent = await _service.CreateServerTagAsync("Environments", parentId: null);
        var child = await _service.CreateServerTagAsync("Prod", parentId: parent);
        var grandchild = await _service.CreateServerTagAsync("EU", parentId: child);
        var unrelated = await _service.CreateServerTagAsync("Owner", parentId: null);

        await _service.AssignServerTagAsync(new[] { 1 }, child);
        await _service.AssignServerTagAsync(new[] { 2 }, grandchild);
        await _service.AssignServerTagAsync(new[] { 3 }, unrelated);

        Assert.True(await _service.ServerTagHasChildrenAsync(parent));

        await _service.DeleteServerTagAsync(parent);

        /* The whole subtree is gone; the unrelated root and its assignment survive. */
        var tags = await _service.GetServerTagsAsync();
        Assert.Equal(new[] { unrelated }, tags.Select(t => t.Id));

        var assignments = await _service.GetServerTagAssignmentsAsync();
        Assert.Equal(new[] { unrelated }, assignments.Select(a => a.TagId).Distinct());
    }
}
