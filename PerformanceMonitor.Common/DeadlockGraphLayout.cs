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

namespace PerformanceMonitor.Common
{
    /// <summary>
    /// Cycle/ring layout for a deadlock graph. Grounded in real sql2025 data: a single "N-process
    /// deadlock" is almost always a <b>bundle of independent small cycles</b> (a 5-proc deadlock is a
    /// 3-cycle + a 2-cycle; an 8-proc is four 2-cycles), NOT one big N-cycle. So this:
    /// <list type="number">
    /// <item>splits the processes into connected components (each component is one cycle);</item>
    /// <item>orders each component by following waiter -&gt; owner edges so the cycle reads around a ring;</item>
    /// <item>places each component's nodes on a circle sized so the fixed cards never overlap;</item>
    /// <item>tiles the component rings in a grid.</item>
    /// </list>
    /// Extra edges within a non-simple component (a resource with several owners/waiters) and parallel
    /// self-edges are kept on the ring and drawn by the control as chords / self-loops — they do not move
    /// nodes. Pure, WPF-free, deterministic, unit-testable. NOT a bipartite process|resource layout.
    /// </summary>
    public static class DeadlockGraphLayout
    {
        public const double NodeWidth = 240;
        public const double NodeHeight = 150;   // fits SPID + proc + contended object + lock/wait + SQL preview
        public const double Padding = 40;
        public const double NodeGap = 70;        // min clear space between two cards on a ring
        public const double ComponentGap = 70;   // gap between component rings when tiled

        // Two cards never overlap once their centers are at least the card diagonal + a gap apart
        // (d >= sqrt(W^2+H^2) => it is impossible for |dx|<W and |dy|<H both). Adjacent ring nodes are the
        // closest pair on an equal-spaced circle, so guaranteeing the adjacent chord >= this guarantees the
        // whole component is overlap-free.
        private static readonly double MinChord = Math.Sqrt(NodeWidth * NodeWidth + NodeHeight * NodeHeight) + NodeGap;

        /// <summary>
        /// Assigns <see cref="DeadlockProcessNode.X"/>/<see cref="DeadlockProcessNode.Y"/> to every process
        /// and returns the total canvas extent. Deterministic for a deterministic model.
        /// </summary>
        public static (double Width, double Height) Layout(DeadlockGraphModel model)
        {
            if (model == null) throw new ArgumentNullException(nameof(model));

            var nodes = model.Processes;
            if (nodes.Count == 0)
                return (Padding * 2, Padding * 2);

            var byId = nodes.ToDictionary(n => n.Id, StringComparer.Ordinal);

            // Undirected adjacency for component detection; directed for ring ordering. Self-edges are
            // ignored for both (they do not connect distinct nodes).
            var undirected = new Dictionary<string, SortedSet<string>>(StringComparer.Ordinal);
            var directed = new Dictionary<string, SortedSet<string>>(StringComparer.Ordinal);
            foreach (var n in nodes)
            {
                undirected[n.Id] = new SortedSet<string>(StringComparer.Ordinal);
                directed[n.Id] = new SortedSet<string>(StringComparer.Ordinal);
            }
            foreach (var e in model.Edges)
            {
                if (!byId.ContainsKey(e.WaiterProcessId) || !byId.ContainsKey(e.OwnerProcessId)) continue;
                if (string.Equals(e.WaiterProcessId, e.OwnerProcessId, StringComparison.Ordinal)) continue;
                undirected[e.WaiterProcessId].Add(e.OwnerProcessId);
                undirected[e.OwnerProcessId].Add(e.WaiterProcessId);
                directed[e.WaiterProcessId].Add(e.OwnerProcessId); // waiter -> owner
            }

            var components = FindComponents(nodes, undirected);

            // Biggest cycle first (top-left), then by lowest spid — deterministic tiling order.
            components.Sort((a, b) =>
            {
                var c = b.Count.CompareTo(a.Count);
                if (c != 0) return c;
                c = byId[a[0]].Spid.CompareTo(byId[b[0]].Spid);
                if (c != 0) return c;
                return string.CompareOrdinal(a[0], b[0]);
            });

            // Lay each component out at a local origin, capture its (width, height), then flow into a grid.
            var laidOut = components
                .Select(comp => PlaceComponentRing(comp, byId, directed))
                .ToList();

            double maxBoxW = laidOut.Max(l => l.Width);
            int targetCols = Math.Max(1, (int)Math.Ceiling(Math.Sqrt(laidOut.Count)));
            double rowWidthLimit = targetCols * (maxBoxW + ComponentGap);

            double x = Padding, rowTop = Padding, rowMaxH = 0, maxRight = 0;
            var boxes = new List<DeadlockComponentBox>(laidOut.Count);
            int cycleIndex = 0;
            foreach (var comp in laidOut)
            {
                if (x > Padding && x + comp.Width > Padding + rowWidthLimit)
                {
                    x = Padding;
                    rowTop += rowMaxH + ComponentGap;
                    rowMaxH = 0;
                }

                foreach (var (node, localX, localY) in comp.Nodes)
                {
                    node.X = x + localX;
                    node.Y = rowTop + localY;
                }

                // The component's cards span [x, x+Width] x [rowTop, rowTop+Height] — capture it so the viewer
                // can frame each independent cycle. Index is 1-based in the (biggest-first) tiling order.
                boxes.Add(new DeadlockComponentBox
                {
                    Index = ++cycleIndex,
                    NodeCount = comp.Nodes.Count,
                    X = x,
                    Y = rowTop,
                    Width = comp.Width,
                    Height = comp.Height
                });

                maxRight = Math.Max(maxRight, x + comp.Width);
                x += comp.Width + ComponentGap;
                rowMaxH = Math.Max(rowMaxH, comp.Height);
            }

            model.ComponentBoxes = boxes;
            return (maxRight + Padding, rowTop + rowMaxH + Padding);
        }

        private static List<List<string>> FindComponents(
            IReadOnlyList<DeadlockProcessNode> nodes,
            Dictionary<string, SortedSet<string>> undirected)
        {
            var components = new List<List<string>>();
            var visited = new HashSet<string>(StringComparer.Ordinal);

            foreach (var start in nodes)
            {
                if (!visited.Add(start.Id)) continue;

                var comp = new List<string>();
                var queue = new Queue<string>();
                queue.Enqueue(start.Id);
                while (queue.Count > 0)
                {
                    var cur = queue.Dequeue();
                    comp.Add(cur);
                    foreach (var nb in undirected[cur])
                        if (visited.Add(nb))
                            queue.Enqueue(nb);
                }
                components.Add(comp);
            }
            return components;
        }

        private sealed class PlacedComponent
        {
            public double Width;
            public double Height;
            public List<(DeadlockProcessNode Node, double X, double Y)> Nodes = new();
        }

        /// <summary>
        /// Orders a component into a ring by walking waiter -&gt; owner edges (so a simple cycle comes out in
        /// cycle order), places the nodes on a circle, and returns the component's local bounding box.
        /// </summary>
        private static PlacedComponent PlaceComponentRing(
            List<string> componentIds,
            Dictionary<string, DeadlockProcessNode> byId,
            Dictionary<string, SortedSet<string>> directed)
        {
            var order = OrderRing(componentIds, byId, directed);
            int k = order.Count;

            var result = new PlacedComponent();

            if (k == 1)
            {
                var only = byId[order[0]];
                result.Nodes.Add((only, 0, 0));
                result.Width = NodeWidth;
                result.Height = NodeHeight;
                return result;
            }

            // radius so the closest (adjacent) chord clears the no-overlap threshold.
            double radius = MinChord / (2.0 * Math.Sin(Math.PI / k));
            const double startAngle = -Math.PI / 2.0; // first node at the top

            // Local card-center positions on the circle.
            var centers = new (DeadlockProcessNode Node, double Cx, double Cy)[k];
            for (int i = 0; i < k; i++)
            {
                double angle = startAngle + i * (2.0 * Math.PI / k);
                centers[i] = (byId[order[i]], radius * Math.Cos(angle), radius * Math.Sin(angle));
            }

            // Shift so the component's bounding box starts at local (0,0).
            double minLeft = centers.Min(c => c.Cx - NodeWidth / 2.0);
            double minTop = centers.Min(c => c.Cy - NodeHeight / 2.0);
            double maxRight = centers.Max(c => c.Cx + NodeWidth / 2.0);
            double maxBottom = centers.Max(c => c.Cy + NodeHeight / 2.0);

            foreach (var (node, cx, cy) in centers)
                result.Nodes.Add((node, cx - NodeWidth / 2.0 - minLeft, cy - NodeHeight / 2.0 - minTop));

            result.Width = maxRight - minLeft;
            result.Height = maxBottom - minTop;
            return result;
        }

        private static List<string> OrderRing(
            List<string> componentIds,
            Dictionary<string, DeadlockProcessNode> byId,
            Dictionary<string, SortedSet<string>> directed)
        {
            var inComponent = new HashSet<string>(componentIds, StringComparer.Ordinal);
            var visited = new HashSet<string>(StringComparer.Ordinal);
            var order = new List<string>(componentIds.Count);

            // Deterministic start: lowest spid (then id) in the component.
            string current = componentIds
                .OrderBy(id => byId[id].Spid)
                .ThenBy(id => id, StringComparer.Ordinal)
                .First();

            while (order.Count < componentIds.Count)
            {
                if (visited.Add(current))
                    order.Add(current);

                // Prefer following an unvisited waiter->owner successor (keeps the cycle contiguous).
                string? next = directed.TryGetValue(current, out var succ)
                    ? succ.Where(s => inComponent.Contains(s) && !visited.Contains(s))
                          .OrderBy(s => byId[s].Spid).ThenBy(s => s, StringComparer.Ordinal)
                          .FirstOrDefault()
                    : null;

                // Dead end (non-simple SCC or branch exhausted): jump to the lowest unvisited member.
                next ??= componentIds.Where(id => !visited.Contains(id))
                                     .OrderBy(id => byId[id].Spid).ThenBy(id => id, StringComparer.Ordinal)
                                     .FirstOrDefault();

                if (next == null) break;
                current = next;
            }

            return order;
        }
    }
}
