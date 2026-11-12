/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The deadlock graph as it draws a two-session cycle, the shape a deadlock most often has. The two sessions' cards stack top to bottom, so
/// the two "waits for" edges bow left and right of one vertical chord: each edge's label sits on its own side of the chord instead of the two
/// drawing over each other at the chord. A card is as tall as its text, within the layout's box, rather than a fixed box with empty space
/// under a short card.
/// </summary>
public sealed class DeadlockGraphRenderTests
{
    private static DeadlockGraphModel TwoSessionCycle() => new()
    {
        Processes = new[]
        {
            new DeadlockProcessNode { Id = "process1", Spid = 61, IsVictim = true, LockMode = "X" },
            new DeadlockProcessNode { Id = "process2", Spid = 62, LockMode = "X" },
        },
        Edges = new[]
        {
            new DeadlockWaitEdge { WaiterProcessId = "process1", OwnerProcessId = "process2", ResourceKind = "KEY", ResourceLabel = "KEY walk.dbo.walk_rows", RequestMode = "X" },
            new DeadlockWaitEdge { WaiterProcessId = "process2", OwnerProcessId = "process1", ResourceKind = "KEY", ResourceLabel = "KEY walk.dbo.walk_rows", RequestMode = "X" },
        },
    };

    private static T Rendered<T>(System.Func<DeadlockGraphControl, T> read) => StaTestThread.Run(() =>
    {
        var control = new DeadlockGraphControl { Width = 800, Height = 600 };
        control.Measure(new Size(800, 600));
        control.Arrange(new Rect(0, 0, 800, 600));
        control.UpdateLayout();
        control.LoadModel(TwoSessionCycle());
        control.UpdateLayout();
        return read(control);
    });

    [Fact]
    public void TwoSessionCycle_DrawsItsTwoEdgeLabelsApart()
    {
        var labels = Rendered(control => control.GraphCanvas.Children.OfType<Border>()
            .Where(b => b.Child is TextBlock t && t.Text.Contains('·'))
            .Select(b => new Rect(Canvas.GetLeft(b), Canvas.GetTop(b), b.DesiredSize.Width, b.DesiredSize.Height))
            .ToList());

        Assert.Equal(2, labels.Count);
        Assert.False(labels[0].IntersectsWith(labels[1]), $"the edge labels at {labels[0]} and {labels[1]} draw over each other");
    }

    [Fact]
    public void SessionCards_AreAsTallAsTheirText()
    {
        var heights = Rendered(control => control.GraphCanvas.Children.OfType<Border>()
            .Where(b => b.Tag is DeadlockProcessNode)
            .Select(b => b.ActualHeight)
            .ToList());

        Assert.Equal(2, heights.Count);
        /* A SPID line and a lock line: well under the layout box's height, and never the box itself. */
        Assert.All(heights, h => Assert.InRange(h, 20, DeadlockGraphLayout.NodeHeight - 40));
    }
}
