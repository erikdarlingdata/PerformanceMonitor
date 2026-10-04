/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>Which custom alert rules a tag change affects (#5085), by diffing two snapshots.</summary>
public sealed class ServerTagCoverageTests
{
    private static ServerTagSnapshot Snap(
        IEnumerable<ServerTagRow> tags,
        IEnumerable<(int Server, int Tag)> assignments,
        IEnumerable<TagScopedRule> rules) =>
        new(tags.ToList(), assignments.Select(a => new ServerTagAssignmentRow(a.Server, a.Tag)).ToList(), rules.ToList());

    [Fact]
    public void Members_IsTheWholeSubtree()
    {
        var tags = new[] { new ServerTagRow(1, "Prod", null, 0, null), new ServerTagRow(2, "East", 1, 0, null) };
        var snap = Snap(tags, [(100, 1), (101, 2), (102, 99)], []);
        Assert.Equal([100, 101], ServerTagCoverage.Members(snap, 1).OrderBy(i => i));
        Assert.Equal([101], ServerTagCoverage.Members(snap, 2).OrderBy(i => i));
    }

    [Fact]
    public void Unassign_OfAServerStillReachableThroughASiblingUnderTheScope_ReportsNothing()
    {
        var tags = new[] { new ServerTagRow(1, "Prod", null, 0, null), new ServerTagRow(2, "East", 1, 0, null), new ServerTagRow(3, "West", 1, 0, null) };
        var rules = new[] { new TagScopedRule(7, "r", true, 1) };
        var before = Snap(tags, [(100, 2), (100, 3)], rules);
        var after = Snap(tags, [(100, 3)], rules);
        Assert.Empty(ServerTagCoverage.Diff(before, after));
    }

    [Fact]
    public void Move_ReportsOldAncestorsLosing_NewAncestorsGaining_AndNotTheCommonOne()
    {
        /* Root(1) > A(2) > M(5); Root(1) > B(3). Move M under B: A loses, B gains, Root unchanged. */
        var rules = new[] { new TagScopedRule(1, "root", true, 1), new TagScopedRule(2, "a", true, 2), new TagScopedRule(3, "b", false, 3), new TagScopedRule(4, "m", true, 5) };
        var beforeTags = new[] { new ServerTagRow(1, "Root", null, 0, null), new ServerTagRow(2, "A", 1, 0, null), new ServerTagRow(3, "B", 1, 0, null), new ServerTagRow(5, "M", 2, 0, null) };
        var afterTags = new[] { new ServerTagRow(1, "Root", null, 0, null), new ServerTagRow(2, "A", 1, 0, null), new ServerTagRow(3, "B", 1, 0, null), new ServerTagRow(5, "M", 3, 0, null) };
        var assignments = new[] { (100, 5) };

        var diff = ServerTagCoverage.Diff(Snap(beforeTags, assignments, rules), Snap(afterTags, assignments, rules));

        Assert.Equal(2, diff.Count);
        var a = diff.Single(d => d.ScopeTagId == 2);
        Assert.Equal("loses_servers", a.Effect);
        Assert.Equal([100], a.ServersLost);
        Assert.Equal(1, a.ServersLostCount);
        var b = diff.Single(d => d.ScopeTagId == 3);
        Assert.Equal("gains_servers", b.Effect);
        Assert.False(b.Enabled);
        Assert.DoesNotContain(diff, d => d.ScopeTagId is 1 or 5);
    }

    [Fact]
    public void ADeletedScopeTag_IsOrphaned_WithEveryOldServerLost()
    {
        var tags = new[] { new ServerTagRow(1, "Prod", null, 0, null), new ServerTagRow(2, "East", 1, 0, null) };
        var rules = new[] { new TagScopedRule(7, "r", true, 2), new TagScopedRule(8, "up", true, 1) };
        var before = Snap(tags, [(100, 2), (101, 1)], rules);

        var diff = ServerTagCoverage.SimulateDelete(before, 2);

        var orphan = diff.Single(d => d.RuleId == 7);
        Assert.Equal("orphaned", orphan.Effect);
        Assert.Equal([100], orphan.ServersLost);
        var up = diff.Single(d => d.RuleId == 8);
        Assert.Equal("loses_servers", up.Effect);
        Assert.Equal([100], up.ServersLost);
    }

    [Fact]
    public void RulesScopedInSubtree_IncludesDisabledRules_AndExcludesAncestors()
    {
        var tags = new[] { new ServerTagRow(1, "Prod", null, 0, null), new ServerTagRow(2, "East", 1, 0, null) };
        var rules = new[] { new TagScopedRule(7, "r", false, 2), new TagScopedRule(8, "up", true, 1) };
        var impact = ServerTagCoverage.RulesScopedInSubtree(Snap(tags, [], rules), 1);
        Assert.Equal(2, impact.Count);
        Assert.Equal([7L], ServerTagCoverage.RulesScopedInSubtree(Snap(tags, [], rules), 2).Select(r => r.RuleId));
    }
}
