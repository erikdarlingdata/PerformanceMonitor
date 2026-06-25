/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Pure unit tests for the shared <see cref="DeadlockGraphLayout"/> — determinism, no-overlap, and full
/// coverage of the cycle/ring placement at the real 5- and 8-process sizes. No WPF (cards are fixed-size).
/// </summary>
public class DeadlockGraphLayoutTests
{
    private static DeadlockGraphModel Parse(string fixture) =>
        DeadlockGraphParser.Parse(DeadlockGraphParserTests.LoadFixture(fixture));

    private static void AssertNoOverlap(DeadlockGraphModel m)
    {
        const double w = DeadlockGraphLayout.NodeWidth;
        const double h = DeadlockGraphLayout.NodeHeight;
        const double eps = 0.5;

        var nodes = m.Processes;
        for (int i = 0; i < nodes.Count; i++)
        {
            for (int j = i + 1; j < nodes.Count; j++)
            {
                var a = nodes[i];
                var b = nodes[j];
                bool overlapX = a.X < b.X + w - eps && b.X < a.X + w - eps;
                bool overlapY = a.Y < b.Y + h - eps && b.Y < a.Y + h - eps;
                Assert.False(overlapX && overlapY,
                    $"SPID {a.Spid} ({a.X},{a.Y}) overlaps SPID {b.Spid} ({b.X},{b.Y})");
            }
        }
    }

    [Theory]
    [InlineData("deadlock_2proc_real_sql2025.xml")]
    [InlineData("deadlock_5proc_real_sql2025.xml")]
    [InlineData("deadlock_8proc_multivictim_real_sql2025.xml")]
    [InlineData("deadlock_single5cycle_synthetic.xml")]
    [InlineData("deadlock_parallel_selfedge_synthetic.xml")]
    [InlineData("deadlock_crossproduct_synthetic.xml")]
    public void Layout_ProducesNoOverlappingNodes(string fixture)
    {
        var m = Parse(fixture);
        DeadlockGraphLayout.Layout(m);
        AssertNoOverlap(m);
    }

    [Theory]
    [InlineData("deadlock_5proc_real_sql2025.xml")]
    [InlineData("deadlock_8proc_multivictim_real_sql2025.xml")]
    public void Layout_IsDeterministic(string fixture)
    {
        var a = Parse(fixture);
        var b = Parse(fixture);

        DeadlockGraphLayout.Layout(a);
        DeadlockGraphLayout.Layout(b);

        var pa = a.Processes.OrderBy(p => p.Id, System.StringComparer.Ordinal).ToList();
        var pb = b.Processes.OrderBy(p => p.Id, System.StringComparer.Ordinal).ToList();
        Assert.Equal(pa.Count, pb.Count);
        for (int i = 0; i < pa.Count; i++)
        {
            Assert.Equal(pa[i].Id, pb[i].Id);
            Assert.Equal(pa[i].X, pb[i].X);
            Assert.Equal(pa[i].Y, pb[i].Y);
        }
    }

    [Theory]
    [InlineData("deadlock_2proc_real_sql2025.xml")]
    [InlineData("deadlock_5proc_real_sql2025.xml")]
    [InlineData("deadlock_8proc_multivictim_real_sql2025.xml")]
    public void Layout_ReturnsExtentCoveringAllNodes(string fixture)
    {
        var m = Parse(fixture);
        var (width, height) = DeadlockGraphLayout.Layout(m);

        Assert.True(width > 0);
        Assert.True(height > 0);
        Assert.True(width >= m.Processes.Max(p => p.X) + DeadlockGraphLayout.NodeWidth);
        Assert.True(height >= m.Processes.Max(p => p.Y) + DeadlockGraphLayout.NodeHeight);
    }

    [Fact]
    public void Layout_AssignsDistinctPositionsToEveryNode()
    {
        var m = Parse("deadlock_8proc_multivictim_real_sql2025.xml");
        DeadlockGraphLayout.Layout(m);

        var positions = m.Processes.Select(p => (p.X, p.Y)).ToHashSet();
        Assert.Equal(m.Processes.Count, positions.Count);   // no two cards share a position
    }

    [Fact]
    public void Layout_EmptyModel_ReturnsPositiveExtent()
    {
        var (width, height) = DeadlockGraphLayout.Layout(new DeadlockGraphModel());
        Assert.True(width > 0);
        Assert.True(height > 0);
    }

    private static bool Within(DeadlockProcessNode n, DeadlockComponentBox b, double eps = 0.5) =>
        n.X >= b.X - eps && n.X + DeadlockGraphLayout.NodeWidth <= b.X + b.Width + eps &&
        n.Y >= b.Y - eps && n.Y + DeadlockGraphLayout.NodeHeight <= b.Y + b.Height + eps;

    [Theory]
    [InlineData("deadlock_2proc_real_sql2025.xml", 1)]
    [InlineData("deadlock_5proc_real_sql2025.xml", 2)]            // 3-cycle + 2-cycle
    [InlineData("deadlock_8proc_multivictim_real_sql2025.xml", 4)] // four 2-cycles
    [InlineData("deadlock_single5cycle_synthetic.xml", 1)]
    [InlineData("deadlock_crossproduct_synthetic.xml", 1)]
    [InlineData("deadlock_parallel_selfedge_synthetic.xml", 1)]   // a<->b via exchange edges = one component
    public void Layout_PopulatesOneBoxPerComponent(string fixture, int expectedComponents)
    {
        var m = Parse(fixture);
        DeadlockGraphLayout.Layout(m);

        Assert.Equal(expectedComponents, m.ComponentBoxes.Count);
        Assert.Equal(m.Processes.Count, m.ComponentBoxes.Sum(b => b.NodeCount)); // every process is in one box
        Assert.Equal(Enumerable.Range(1, expectedComponents), m.ComponentBoxes.Select(b => b.Index));

        // Every card lands inside exactly one component box (so a drawn frame contains exactly its cycle).
        foreach (var n in m.Processes)
            Assert.Equal(1, m.ComponentBoxes.Count(b => Within(n, b)));
    }

    [Fact]
    public void ComponentBoxes_LoneNode_IsNotCountedAsACycle()
    {
        // The viewer frames/counts only components with >= 2 processes. A node connected solely by a self-edge
        // is its own 1-node component and must NOT read as an independent cycle. No real fixture produces one,
        // so build it: a<->b is a real 2-cycle and c carries only a self-edge.
        var model = new DeadlockGraphModel
        {
            Processes = new[]
            {
                new DeadlockProcessNode { Id = "a", Spid = 60 },
                new DeadlockProcessNode { Id = "b", Spid = 61 },
                new DeadlockProcessNode { Id = "c", Spid = 62 },
            },
            Edges = new[]
            {
                new DeadlockWaitEdge { WaiterProcessId = "a", OwnerProcessId = "b", ResourceKind = "pagelock", ResourceLabel = "PAGE db.dbo.t" },
                new DeadlockWaitEdge { WaiterProcessId = "b", OwnerProcessId = "a", ResourceKind = "pagelock", ResourceLabel = "PAGE db.dbo.t" },
                new DeadlockWaitEdge { WaiterProcessId = "c", OwnerProcessId = "c", ResourceKind = "exchangeEvent", ResourceLabel = "Parallelism" },
            }
        };

        DeadlockGraphLayout.Layout(model);

        Assert.Equal(2, model.ComponentBoxes.Count);                          // a<->b, and lone c
        Assert.Equal(1, model.ComponentBoxes.Count(b => b.NodeCount >= 2));   // only a<->b is a cycle

        var multi = Parse("deadlock_8proc_multivictim_real_sql2025.xml");
        DeadlockGraphLayout.Layout(multi);
        Assert.Equal(4, multi.ComponentBoxes.Count(b => b.NodeCount >= 2));   // four real cycles
    }
}
