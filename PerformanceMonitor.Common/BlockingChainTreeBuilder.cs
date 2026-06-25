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
    /// Turns the analysis engine's per-chain edge lists into renderable trees (one per apex). This is the
    /// single, shared home for the non-trivial DAG -> tree logic; both apps feed it the same public DTO so
    /// the rule lives in exactly one place. Pure and app-agnostic.
    /// </summary>
    public static class BlockingChainTreeBuilder
    {
        // (Spid, Ecid) is the session identity — see BlockingChainNode (mirrors sp_HumanEventsBlockViewer's
        // spid:ecid). Transaction start is display-only and no longer part of the key.
        private readonly struct NodeKey : IEquatable<NodeKey>
        {
            public readonly int Spid;
            public readonly int Ecid;
            public NodeKey(int spid, int ecid) { Spid = spid; Ecid = ecid; }
            public bool Equals(NodeKey other) => Spid == other.Spid && Ecid == other.Ecid;
            public override bool Equals(object? obj) => obj is NodeKey k && Equals(k);
            public override int GetHashCode() => HashCode.Combine(Spid, Ecid);
        }

        /// <summary>
        /// Builds the tree forest. DAG -> tree policy: a victim reachable from more than one blocker (a
        /// diamond) attaches to its lowest-<c>Level</c> parent; ties break to the lowest parent SPID then
        /// transaction start. Extra in-edges are dropped. Sessions that share a reused SPID but differ in
        /// transaction start are kept as separate nodes (never collapsed). Cycle-safe via a visited set.
        /// </summary>
        public static BlockingChainModel Build(
            IReadOnlyList<BlockingChainInput> chains,
            bool cycleDetected,
            bool depthCapped,
            bool traversalTruncated)
        {
            if (chains == null) throw new ArgumentNullException(nameof(chains));

            var roots = chains
                .Select(BuildOne)
                .Where(r => r != null)
                .Select(r => r!)
                // Rank worst-first; deterministic tiebreak so the layout is stable regardless of input order.
                .OrderByDescending(r => r.Magnitude)
                .ThenBy(r => r.Spid)
                .ThenBy(r => r.Ecid)
                .ToList();

            return new BlockingChainModel
            {
                Roots = roots,
                CycleDetected = cycleDetected,
                DepthCapped = depthCapped,
                TraversalTruncated = traversalTruncated
            };
        }

        private static BlockingChainNode? BuildOne(BlockingChainInput chain)
        {
            var apexKey = new NodeKey(chain.ApexSpid, chain.ApexEcid);

            // Group edges by parent (blocking side); within a parent, sort children deterministically.
            var childrenByParent = new Dictionary<NodeKey, List<BlockingEdgeInput>>();
            foreach (var edge in chain.Edges)
            {
                var parent = new NodeKey(edge.BlockingSpid, edge.BlockingEcid);
                if (!childrenByParent.TryGetValue(parent, out var list))
                    childrenByParent[parent] = list = new List<BlockingEdgeInput>();
                list.Add(edge);
            }
            foreach (var list in childrenByParent.Values)
                list.Sort(static (a, b) =>
                {
                    var c = a.BlockedSpid.CompareTo(b.BlockedSpid);
                    if (c != 0) return c;
                    return a.BlockedEcid.CompareTo(b.BlockedEcid);
                });

            // The apex sources its own SqlText / DatabaseName from its FIRST outgoing edge (it has no
            // incoming edge); its wait/lock are empty because it waits on no one. Note: a cross-database
            // lead blocker therefore shows only its first victim's database here.
            var apexEdge = childrenByParent.TryGetValue(apexKey, out var apexOut) && apexOut.Count > 0
                ? apexOut[0]
                : null;

            var root = new BlockingChainNode
            {
                Spid = chain.ApexSpid,
                Ecid = chain.ApexEcid,
                TranStarted = chain.ApexTranStarted,
                IsApex = true,
                IsApexSleeping = chain.ApexSleeping,
                Magnitude = chain.Magnitude,
                WaitTimeMs = 0,
                LockMode = string.Empty,
                SqlText = apexEdge?.BlockingSqlText ?? string.Empty,
                DatabaseName = apexEdge?.DatabaseName ?? string.Empty,
                // Apex identity comes from the blocking side of an outgoing edge (it's never blocked).
                LoginName = apexEdge?.BlockingLoginName ?? string.Empty,
                HostName = apexEdge?.BlockingHostName ?? string.Empty,
                ClientApp = apexEdge?.BlockingClientApp ?? string.Empty
            };

            // Level-synchronized BFS from the apex. Processing each level in (Spid, Tran) order means the
            // lowest-level / lowest-SPID parent claims a shared victim first; the visited set both
            // implements the diamond "drop extra in-edge" rule and guards against cycles.
            var visited = new HashSet<NodeKey> { apexKey };
            var current = new List<(NodeKey Key, BlockingChainNode Node)> { (apexKey, root) };

            while (current.Count > 0)
            {
                current.Sort(static (a, b) =>
                {
                    var c = a.Key.Spid.CompareTo(b.Key.Spid);
                    if (c != 0) return c;
                    return a.Key.Ecid.CompareTo(b.Key.Ecid);
                });

                var next = new List<(NodeKey, BlockingChainNode)>();
                foreach (var (key, node) in current)
                {
                    if (!childrenByParent.TryGetValue(key, out var edges)) continue;
                    foreach (var edge in edges)
                    {
                        var childKey = new NodeKey(edge.BlockedSpid, edge.BlockedEcid);
                        if (!visited.Add(childKey)) continue; // already placed (diamond back-edge) or a cycle

                        var child = new BlockingChainNode
                        {
                            Spid = edge.BlockedSpid,
                            Ecid = edge.BlockedEcid,
                            TranStarted = edge.BlockedTranStarted,
                            IsApex = false,
                            WaitTimeMs = edge.WaitTimeMs,
                            LockMode = edge.LockMode,
                            SqlText = edge.BlockedSqlText,
                            DatabaseName = edge.DatabaseName,
                            // A victim / mid-level blocker sources its identity from the blocked side of its
                            // incoming edge (where it is the blocked party).
                            LoginName = edge.BlockedLoginName,
                            HostName = edge.BlockedHostName,
                            ClientApp = edge.BlockedClientApp,
                            // The object this victim is waiting on — its incoming edge's contended resource.
                            ContentiousObject = edge.ContentiousObject
                        };
                        node.Children.Add(child);
                        next.Add((childKey, child));
                    }
                }
                current = next;
            }

            return root;
        }
    }
}
