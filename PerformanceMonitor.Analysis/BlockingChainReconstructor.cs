/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor analysis engine.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// One blocked/blocker pair from blocked_process_reports — the raw input to reconstruction.
/// </summary>
internal sealed class BlockingPairRow
{
    public DateTime EventTime { get; init; }
    public string DatabaseName { get; init; } = string.Empty;
    public int BlockedSpid { get; init; }
    public DateTime? BlockedTranStarted { get; init; }
    public int BlockingSpid { get; init; }
    public DateTime? BlockingTranStarted { get; init; }
    /// <summary>The blocked-process-report's monitorLoop — the episode (one monitor scan) the row belongs to.
    /// Nullable: only the viewer scopes by it; the collector passes scopeByMonitorLoop=false (treated as null).</summary>
    public int? MonitorLoop { get; init; }
    /// <summary>Execution-context id of each side; with spid it forms the session identity the reconstruction
    /// keys on (mirrors sp_HumanEventsBlockViewer's spid:ecid). 0 for the common non-parallel case.</summary>
    public int BlockedEcid { get; init; }
    public int BlockingEcid { get; init; }
    public long WaitTimeMs { get; init; }
    public string LockMode { get; init; } = string.Empty;
    public string BlockingStatus { get; init; } = string.Empty;
    public string BlockedSqlText { get; init; } = string.Empty;
    public string BlockingSqlText { get; init; } = string.Empty;
    // Session identity for each side (login / host / client app). Carried so the viewer can show WHO a
    // session is, not just a SPID. Threaded the same way as the SQL text.
    public string BlockedLoginName { get; init; } = string.Empty;
    public string BlockedHostName { get; init; } = string.Empty;
    public string BlockedClientApp { get; init; } = string.Empty;
    // Blocker-side identity is settable (not init) so the Dashboard viewer can enrich it after Read from
    // the correlated activity='blocking' row — its table denormalizes only the blocked side (see Dashboard
    // BlockingPairRowQuery.ReadWithBlockerIdentity). Lite sets all six in its Read object initializer.
    public string BlockingLoginName { get; set; } = string.Empty;
    public string BlockingHostName { get; set; } = string.Empty;
    public string BlockingClientApp { get; set; } = string.Empty;
    /// <summary>The contended object (schema.object where resolvable) — the blocked row's contentious_object.</summary>
    public string ContentiousObject { get; init; } = string.Empty;
}

/// <summary>
/// Stable session identity mirroring sp_HumanEventsBlockViewer: spid:ecid scoped to a monitor_loop (one
/// blocked-process-report scan = one episode). MonitorLoop is null when the caller reconstructs cumulatively
/// across the window (the collector) rather than per-scan (the viewer); ecid distinguishes parallel workers.
/// Transaction start is NOT part of the identity — the blocking-process node omits it, so it is null on every
/// blocker and cannot disambiguate.
/// </summary>
internal readonly record struct SessionKey(int? MonitorLoop, int Spid, int Ecid);

/// <summary>One level (one blocked/blocker edge) of a reconstructed chain, for drill-down.</summary>
internal sealed class ChainLevel
{
    public int Level { get; init; }
    public int BlockingSpid { get; init; }
    public int BlockingEcid { get; init; }
    /// <summary>Transaction start of the blocking side — display only (sentinel-normalized); not part of the
    /// session identity, which is spid:ecid within monitor_loop.</summary>
    public DateTime? BlockingTranStarted { get; init; }
    public int BlockedSpid { get; init; }
    public int BlockedEcid { get; init; }
    public DateTime? BlockedTranStarted { get; init; }
    public string LockMode { get; init; } = string.Empty;
    public long WaitTimeMs { get; init; }
    public string DatabaseName { get; init; } = string.Empty;
    public string BlockingSqlText { get; init; } = string.Empty;
    public string BlockedSqlText { get; init; } = string.Empty;
    public string BlockedLoginName { get; init; } = string.Empty;
    public string BlockedHostName { get; init; } = string.Empty;
    public string BlockedClientApp { get; init; } = string.Empty;
    public string BlockingLoginName { get; init; } = string.Empty;
    public string BlockingHostName { get; init; } = string.Empty;
    public string BlockingClientApp { get; init; } = string.Empty;
    /// <summary>The contended object for this edge — the blocked side's contentious_object.</summary>
    public string ContentiousObject { get; init; } = string.Empty;
}

/// <summary>A single reconstructed blocking chain, rooted at an apex head blocker.</summary>
internal sealed class ReconstructedChain
{
    public int ApexSpid { get; init; }
    public int ApexEcid { get; init; }
    /// <summary>The chain's episode (monitor_loop) when scoped per-scan; null for a cumulative reconstruction.
    /// All nodes in a scoped chain share it (one report's two sides carry the same monitor_loop).</summary>
    public int? MonitorLoop { get; init; }
    /// <summary>Transaction start of the apex — display only (null for a blocker, which has none).</summary>
    public DateTime? ApexTranStarted { get; init; }
    public bool ApexSleeping { get; init; }
    public int Depth { get; init; }
    public int VictimCount { get; init; }
    public long MaxWaitMs { get; init; }
    public double Magnitude { get; init; }
    public IReadOnlyList<ChainLevel> Levels { get; init; } = Array.Empty<ChainLevel>();
}

/// <summary>Result of a reconstruction pass — chains ranked worst-first, plus cap flags.</summary>
internal sealed class BlockingReconstruction
{
    public IReadOnlyList<ReconstructedChain> Chains { get; init; } = Array.Empty<ReconstructedChain>();
    public bool DepthCapped { get; init; }
    public bool TraversalTruncated { get; init; }
    public bool CycleDetected { get; init; }
}

/// <summary>
/// Reconstructs blocking chains (apex head blocker, depth, victim count) from the per-pair
/// blocked_process_reports rows. Pure — no DB dependency — so the collector and the
/// drill-down collector share one implementation and it is directly unit-testable.
/// </summary>
internal static class BlockingChainReconstructor
{
    /// <summary>
    /// SQL Server's blocked-process-report emits lasttranstarted="1900-01-01T00:00:00"
    /// (a real, parseable value — not NULL) for a session with no open transaction.
    /// A transaction start at or before this floor is treated as "no transaction".
    /// </summary>
    private static readonly DateTime SentinelFloor = new(1900, 1, 2);

    // Holds the deduped winning row for an edge, so every per-pair field (wait, lock, SQL, identity) is
    // available when building the ChainLevel without re-listing each one here.
    private sealed record EdgeInfo(BlockingPairRow Row);

    /// <summary>Builds a session key. MonitorLoop is null when reconstructing cumulatively (collector).</summary>
    public static SessionKey MakeKey(int? monitorLoop, int spid, int ecid) => new(monitorLoop, spid, ecid);

    /// <summary>Normalizes the 1900-01-01 sentinel to NULL for display tran values.</summary>
    private static DateTime? NormalizeTran(DateTime? tranStarted) =>
        tranStarted.HasValue && tranStarted.Value > SentinelFloor ? tranStarted : null;

    /// <summary>
    /// Finds the single reconstructed chain that contains the clicked session, matched by spid:ecid within
    /// its episode (monitor_loop). The session may be the apex, a mid-level blocker, or a leaf victim. When
    /// the clicked monitor_loop is unavailable (e.g. a lead-blocker grid row), it falls back to a
    /// monitor_loop-agnostic spid:ecid match — the reconstruction window is already tight, so this still lands
    /// on the right chain rather than reporting "no reconstructable chain." Returns null when no chain holds
    /// the session. Only the viewer calls this, on a per-scan (scoped) reconstruction; the collector takes the
    /// worst chain directly.
    /// </summary>
    public static ReconstructedChain? FindChainForSession(
        BlockingReconstruction reconstruction, int? monitorLoop, int spid, int ecid)
    {
        foreach (var chain in reconstruction.Chains)
            if ((!monitorLoop.HasValue || chain.MonitorLoop == monitorLoop) &&
                ChainContainsSpidEcid(chain, spid, ecid))
                return chain;

        if (monitorLoop.HasValue)
            foreach (var chain in reconstruction.Chains)
                if (ChainContainsSpidEcid(chain, spid, ecid))
                    return chain;

        return null;
    }

    /// <summary>True if the session (spid:ecid) appears anywhere in the chain — apex, a blocker, or a victim.</summary>
    private static bool ChainContainsSpidEcid(ReconstructedChain chain, int spid, int ecid)
    {
        if (chain.ApexSpid == spid && chain.ApexEcid == ecid)
            return true;

        foreach (var l in chain.Levels)
        {
            if (l.BlockingSpid == spid && l.BlockingEcid == ecid) return true;
            if (l.BlockedSpid == spid && l.BlockedEcid == ecid) return true;
        }

        return false;
    }

    public static BlockingReconstruction Reconstruct(
        IEnumerable<BlockingPairRow> rows, int maxDepth, int maxPairs, int stepBudget, bool scopeByMonitorLoop)
    {
        var pairs = rows.Take(maxPairs).ToList();
        if (pairs.Count == 0)
            return new BlockingReconstruction();

        // Directed graph: blocker -> blocked. Edges deduped by max wait time (a pair
        // re-fires every few seconds with a growing wait), keeping the worst row's detail.
        var adjacency = new Dictionary<SessionKey, Dictionary<SessionKey, EdgeInfo>>();
        var allNodes = new HashSet<SessionKey>();
        var blockedNodes = new HashSet<SessionKey>();
        var sleepingBlockers = new HashSet<SessionKey>();

        foreach (var row in pairs)
        {
            // The collector reconstructs cumulatively (loop = null) so an episode's per-scan re-fires merge into
            // one chain (preserves window-level depth/victims for the severity fact); the viewer scopes per scan.
            var loop = scopeByMonitorLoop ? row.MonitorLoop : null;
            var blocker = MakeKey(loop, row.BlockingSpid, row.BlockingEcid);
            var blocked = MakeKey(loop, row.BlockedSpid, row.BlockedEcid);

            allNodes.Add(blocker);
            allNodes.Add(blocked);
            blockedNodes.Add(blocked);

            if (string.Equals(row.BlockingStatus, "sleeping", StringComparison.OrdinalIgnoreCase))
                sleepingBlockers.Add(blocker);

            if (blocker.Equals(blocked))
                continue; // a session cannot block itself — guard against degenerate data

            if (!adjacency.TryGetValue(blocker, out var dests))
                adjacency[blocker] = dests = new Dictionary<SessionKey, EdgeInfo>();

            if (!dests.TryGetValue(blocked, out var existing) || row.WaitTimeMs > existing.Row.WaitTimeMs)
            {
                dests[blocked] = new EdgeInfo(row);
            }
        }

        var cycleDetected = HasCycle(allNodes, adjacency);

        // Roots: apexes (blockers that are never blocked). Subgraphs that are pure cycles
        // have no apex — give each a fallback root so the chain is not silently dropped.
        var roots = allNodes.Where(n => adjacency.ContainsKey(n) && !blockedNodes.Contains(n)).ToList();
        AddFallbackRoots(roots, allNodes, blockedNodes, adjacency);

        var steps = stepBudget;
        var depthCapped = false;
        var truncated = false;
        var depthMemo = new Dictionary<SessionKey, int>();

        var chains = new List<ReconstructedChain>(roots.Count);
        foreach (var root in roots)
        {
            var depth = LongestDepth(root, adjacency, maxDepth, !cycleDetected, depthMemo,
                new HashSet<SessionKey>(), ref steps, ref depthCapped, ref truncated);
            var (victimCount, maxWait, levels) = WalkChain(root, adjacency, ref steps, ref truncated);

            var magnitude = Math.Max(
                FactScorer.ApplyThresholdFormula(depth, 3, 8),
                FactScorer.ApplyThresholdFormula(victimCount, 5, 25));

            // Apex display tran comes from its first outgoing edge's blocking side (the apex never appears as a
            // blocked row); a blocker has no tran on Dashboard (null) but Lite carries it.
            var apexTran = adjacency.TryGetValue(root, out var apexDests) && apexDests.Count > 0
                ? NormalizeTran(apexDests.Values.First().Row.BlockingTranStarted)
                : null;

            chains.Add(new ReconstructedChain
            {
                ApexSpid = root.Spid,
                ApexEcid = root.Ecid,
                MonitorLoop = root.MonitorLoop,
                ApexTranStarted = apexTran,
                ApexSleeping = sleepingBlockers.Contains(root),
                Depth = depth,
                VictimCount = victimCount,
                MaxWaitMs = maxWait,
                Magnitude = magnitude,
                Levels = levels
            });
        }

        return new BlockingReconstruction
        {
            Chains = chains.OrderByDescending(c => c.Magnitude)
                           .ThenByDescending(c => c.Depth)
                           .ToList(),
            DepthCapped = depthCapped,
            TraversalTruncated = truncated,
            CycleDetected = cycleDetected
        };
    }

    /// <summary>Kahn's algorithm — true if the graph is not a DAG.</summary>
    private static bool HasCycle(
        HashSet<SessionKey> allNodes,
        Dictionary<SessionKey, Dictionary<SessionKey, EdgeInfo>> adjacency)
    {
        var inDegree = allNodes.ToDictionary(n => n, _ => 0);
        foreach (var dests in adjacency.Values)
            foreach (var dest in dests.Keys)
                inDegree[dest]++;

        var queue = new Queue<SessionKey>(inDegree.Where(kv => kv.Value == 0).Select(kv => kv.Key));
        var removed = 0;
        while (queue.Count > 0)
        {
            var node = queue.Dequeue();
            removed++;
            if (adjacency.TryGetValue(node, out var dests))
                foreach (var dest in dests.Keys)
                    if (--inDegree[dest] == 0)
                        queue.Enqueue(dest);
        }

        return removed != allNodes.Count;
    }

    /// <summary>
    /// For any subgraph with no apex (a pure cycle), adds the highest-wait node as a
    /// fallback root so the chain is reconstructed rather than silently dropped.
    /// </summary>
    private static void AddFallbackRoots(
        List<SessionKey> roots,
        HashSet<SessionKey> allNodes,
        HashSet<SessionKey> blockedNodes,
        Dictionary<SessionKey, Dictionary<SessionKey, EdgeInfo>> adjacency)
    {
        var reached = new HashSet<SessionKey>();
        foreach (var root in roots)
            MarkReachable(root, adjacency, reached);

        var orphans = allNodes.Where(n => adjacency.ContainsKey(n) && !reached.Contains(n)).ToList();
        while (orphans.Count > 0)
        {
            // Pick the orphan with the largest outgoing wait time as the fallback root.
            var fallback = orphans
                .OrderByDescending(n => adjacency[n].Values.Max(e => e.Row.WaitTimeMs))
                .First();
            roots.Add(fallback);
            MarkReachable(fallback, adjacency, reached);
            orphans = orphans.Where(n => !reached.Contains(n)).ToList();
        }
    }

    private static void MarkReachable(
        SessionKey start,
        Dictionary<SessionKey, Dictionary<SessionKey, EdgeInfo>> adjacency,
        HashSet<SessionKey> reached)
    {
        var stack = new Stack<SessionKey>();
        if (reached.Add(start))
            stack.Push(start);
        while (stack.Count > 0)
        {
            var node = stack.Pop();
            if (adjacency.TryGetValue(node, out var dests))
                foreach (var dest in dests.Keys)
                    if (reached.Add(dest))
                        stack.Push(dest);
        }
    }

    /// <summary>
    /// Longest downward path (in edges) from a node. Memoized when the graph is a DAG
    /// (memo is path-independent there); on a cyclic graph memo is disabled and the
    /// per-path visited set plus the global step budget bound the traversal.
    /// </summary>
    private static int LongestDepth(
        SessionKey node,
        Dictionary<SessionKey, Dictionary<SessionKey, EdgeInfo>> adjacency,
        int maxDepth,
        bool useMemo,
        Dictionary<SessionKey, int> memo,
        HashSet<SessionKey> path,
        ref int steps,
        ref bool depthCapped,
        ref bool truncated)
    {
        if (useMemo && memo.TryGetValue(node, out var cached))
            return cached;

        if (steps-- <= 0)
        {
            truncated = true;
            return 0;
        }

        if (path.Count >= maxDepth)
        {
            depthCapped = true;
            return 0;
        }

        var best = 0;
        if (adjacency.TryGetValue(node, out var dests))
        {
            path.Add(node);
            foreach (var child in dests.Keys)
            {
                if (path.Contains(child))
                    continue; // cycle guard

                var childDepth = LongestDepth(child, adjacency, maxDepth, useMemo, memo, path,
                    ref steps, ref depthCapped, ref truncated);
                if (1 + childDepth > best)
                    best = 1 + childDepth;
            }
            path.Remove(node);
        }

        if (useMemo)
            memo[node] = best;
        return best;
    }

    /// <summary>
    /// Walks the subtree under a root: distinct transitive victim count, the worst edge
    /// wait time, and a BFS-ordered level list for drill-down.
    /// </summary>
    private static (int VictimCount, long MaxWaitMs, List<ChainLevel> Levels) WalkChain(
        SessionKey root,
        Dictionary<SessionKey, Dictionary<SessionKey, EdgeInfo>> adjacency,
        ref int steps,
        ref bool truncated)
    {
        var victims = new HashSet<SessionKey>();
        var levels = new List<ChainLevel>();
        long maxWait = 0;

        var queue = new Queue<(SessionKey Node, int Level)>();
        var enqueued = new HashSet<SessionKey> { root };
        queue.Enqueue((root, 0));

        while (queue.Count > 0)
        {
            if (steps-- <= 0)
            {
                truncated = true;
                break;
            }

            var (node, level) = queue.Dequeue();
            if (!adjacency.TryGetValue(node, out var dests))
                continue;

            foreach (var (child, edge) in dests)
            {
                victims.Add(child);
                var row = edge.Row;
                if (row.WaitTimeMs > maxWait)
                    maxWait = row.WaitTimeMs;

                levels.Add(new ChainLevel
                {
                    Level = level + 1,
                    BlockingSpid = node.Spid,
                    BlockingEcid = node.Ecid,
                    BlockingTranStarted = NormalizeTran(row.BlockingTranStarted),
                    BlockedSpid = child.Spid,
                    BlockedEcid = child.Ecid,
                    BlockedTranStarted = NormalizeTran(row.BlockedTranStarted),
                    LockMode = row.LockMode ?? string.Empty,
                    WaitTimeMs = row.WaitTimeMs,
                    DatabaseName = row.DatabaseName ?? string.Empty,
                    BlockingSqlText = row.BlockingSqlText ?? string.Empty,
                    BlockedSqlText = row.BlockedSqlText ?? string.Empty,
                    BlockedLoginName = row.BlockedLoginName ?? string.Empty,
                    BlockedHostName = row.BlockedHostName ?? string.Empty,
                    BlockedClientApp = row.BlockedClientApp ?? string.Empty,
                    BlockingLoginName = row.BlockingLoginName ?? string.Empty,
                    BlockingHostName = row.BlockingHostName ?? string.Empty,
                    BlockingClientApp = row.BlockingClientApp ?? string.Empty,
                    ContentiousObject = row.ContentiousObject ?? string.Empty
                });

                if (enqueued.Add(child))
                    queue.Enqueue((child, level + 1));
            }
        }

        return (victims.Count, maxWait, levels);
    }
}
