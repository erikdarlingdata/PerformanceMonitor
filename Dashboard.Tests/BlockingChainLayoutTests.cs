using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Pure unit tests for the shared <see cref="BlockingChainLayout"/> — determinism and no-overlap of the
/// left-to-right tree/forest placement. No WPF (a constant measure callback supplies node heights).
/// </summary>
public class BlockingChainLayoutTests
{
    private const double H = 100.0;
    private static double Measure(BlockingChainNode _) => H;

    private static BlockingChainNode Node(int spid, params BlockingChainNode[] children) =>
        new() { Spid = spid, Children = children.ToList() };

    // A representative forest: a multi-level tree plus a second apex tree.
    private static IReadOnlyList<BlockingChainNode> SampleForest() => new[]
    {
        Node(1,
            Node(2, Node(4), Node(5)),
            Node(3)),
        Node(10,
            Node(11),
            Node(12, Node(13)))
    };

    private static IEnumerable<BlockingChainNode> Flatten(IReadOnlyList<BlockingChainNode> roots)
    {
        foreach (var r in roots)
            foreach (var n in FlattenOne(r))
                yield return n;
    }

    private static IEnumerable<BlockingChainNode> FlattenOne(BlockingChainNode n)
    {
        yield return n;
        foreach (var c in n.Children)
            foreach (var d in FlattenOne(c))
                yield return d;
    }

    [Fact]
    public void Layout_IsDeterministic_AcrossIdenticalForests()
    {
        var a = SampleForest();
        var b = SampleForest();

        BlockingChainLayout.Layout(a, Measure);
        BlockingChainLayout.Layout(b, Measure);

        var fa = Flatten(a).ToList();
        var fb = Flatten(b).ToList();
        Assert.Equal(fa.Count, fb.Count);
        for (int i = 0; i < fa.Count; i++)
        {
            Assert.Equal(fa[i].Spid, fb[i].Spid);
            Assert.Equal(fa[i].X, fb[i].X);
            Assert.Equal(fa[i].Y, fb[i].Y);
        }
    }

    [Fact]
    public void Layout_PlacesDepthByColumn()
    {
        var roots = SampleForest();
        BlockingChainLayout.Layout(roots, Measure);

        double depth0 = BlockingChainLayout.Padding;
        double depth1 = BlockingChainLayout.Padding + BlockingChainLayout.HorizontalSpacing;
        double depth2 = BlockingChainLayout.Padding + 2 * BlockingChainLayout.HorizontalSpacing;

        Assert.Equal(depth0, roots[0].X);                 // apex
        Assert.Equal(depth1, roots[0].Children[0].X);     // direct victim
        Assert.Equal(depth2, roots[0].Children[0].Children[0].X); // grand-victim
    }

    [Fact]
    public void Layout_ProducesNoOverlappingNodes()
    {
        var roots = SampleForest();
        BlockingChainLayout.Layout(roots, Measure);

        var nodes = Flatten(roots).ToList();
        const double w = BlockingChainLayout.NodeWidth;
        const double eps = 0.5;

        for (int i = 0; i < nodes.Count; i++)
        {
            for (int j = i + 1; j < nodes.Count; j++)
            {
                var a = nodes[i];
                var b = nodes[j];
                bool overlapX = a.X < b.X + w - eps && b.X < a.X + w - eps;
                bool overlapY = a.Y < b.Y + H - eps && b.Y < a.Y + H - eps;
                Assert.False(overlapX && overlapY,
                    $"SPID {a.Spid} ({a.X},{a.Y}) overlaps SPID {b.Spid} ({b.X},{b.Y})");
            }
        }
    }

    [Fact]
    public void Layout_ReturnsPositiveExtentCoveringAllNodes()
    {
        var roots = SampleForest();
        var (width, height) = BlockingChainLayout.Layout(roots, Measure);

        Assert.True(width > 0);
        Assert.True(height > 0);

        var nodes = Flatten(roots).ToList();
        Assert.True(width >= nodes.Max(n => n.X) + BlockingChainLayout.NodeWidth);
        Assert.True(height >= nodes.Max(n => n.Y) + H);
    }

    [Fact]
    public void Layout_EmptyForest_ReturnsPaddingExtent()
    {
        var (width, height) = BlockingChainLayout.Layout(Array.Empty<BlockingChainNode>(), Measure);
        Assert.True(width > 0);   // padding only
        Assert.True(height > 0);
    }
}
