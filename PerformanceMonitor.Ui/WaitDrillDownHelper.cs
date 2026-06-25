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

namespace PerformanceMonitor.Ui;

/// <summary>
/// Classifies wait types for drill-down behavior and walks blocking chains
/// to find head blockers. Used by WaitDrillDownWindow.
/// </summary>
public static class WaitDrillDownHelper
{
    public enum WaitCategory
    {
        /// <summary>Wait is too brief to appear in snapshots. Show all queries sorted by correlated metric.</summary>
        Correlated,
        /// <summary>Walk blocking chain to find head blockers (LCK_M_*).</summary>
        Chain,
        /// <summary>Sessions may lack worker threads, unlikely to appear in snapshots.</summary>
        Uncapturable,
        /// <summary>Attempt direct wait_type filter; may return empty for brief waits.</summary>
        Filtered
    }

    public sealed record WaitClassification(
        WaitCategory Category,
        string SortProperty,
        string Description
    );

    /// <summary>
    /// Lightweight result from the chain walker — just the head blocker identity and blocked count.
    /// Callers look up the original full row by (CollectionTime, SessionId).
    /// </summary>
    public sealed record HeadBlockerInfo(
        DateTime CollectionTime,
        int SessionId,
        int BlockedSessionCount,
        string BlockingPath
    );

    public sealed record SnapshotInfo
    {
        public int SessionId { get; init; }
        public int BlockingSessionId { get; init; }
        public DateTime CollectionTime { get; init; }
        public string DatabaseName { get; init; } = "";
        public string Status { get; init; } = "";
        public string QueryText { get; init; } = "";
        public string? WaitType { get; init; }
        public long WaitTimeMs { get; init; }
        public long CpuTimeMs { get; init; }
        public long Reads { get; init; }
        public long Writes { get; init; }
        public long LogicalReads { get; init; }
    }

    private const int MaxChainDepth = 20;

    public static WaitClassification Classify(string waitType)
    {
        if (string.IsNullOrEmpty(waitType))
            return new WaitClassification(WaitCategory.Filtered, "WaitTimeMs", "Unknown");

        return waitType switch
        {
            "SOS_SCHEDULER_YIELD" =>
                new(WaitCategory.Correlated, "CpuTimeMs", "CPU pressure — showing high-CPU queries active during this period"),
            "WRITELOG" =>
                new(WaitCategory.Correlated, "Writes", "Transaction log writes — showing high-write queries active during this period"),
            "CXPACKET" or "CXCONSUMER" =>
                new(WaitCategory.Correlated, "Dop", "Parallelism — showing parallel queries active during this period"),
            "RESOURCE_SEMAPHORE" or "RESOURCE_SEMAPHORE_QUERY_COMPILE" =>
                new(WaitCategory.Correlated, "GrantedQueryMemoryGb", "Memory grant pressure — showing high-memory queries active during this period"),
            "THREADPOOL" =>
                new(WaitCategory.Uncapturable, "CpuTimeMs", "Thread pool starvation — sessions may not appear in snapshots"),
            "LATCH_EX" or "LATCH_UP" =>
                new(WaitCategory.Correlated, "CpuTimeMs", "Latch contention — showing high-CPU queries active during this period"),
            _ when waitType.StartsWith("PAGEIOLATCH_", StringComparison.OrdinalIgnoreCase) =>
                new(WaitCategory.Correlated, "Reads", "Disk I/O — showing high-read queries active during this period"),
            _ when waitType.StartsWith("LCK_M_", StringComparison.OrdinalIgnoreCase) =>
                new(WaitCategory.Chain, "", "Lock contention — showing head blockers"),
            _ =>
                new(WaitCategory.Filtered, "WaitTimeMs", "Filtered by wait type")
        };
    }

    /// <summary>
    /// Walks blocking chains to find head blockers.
    /// Returns lightweight HeadBlockerInfo records — callers look up original full rows
    /// by (CollectionTime, SessionId) to preserve all columns.
    /// </summary>
    public static List<HeadBlockerInfo> WalkBlockingChains(
        IEnumerable<SnapshotInfo> waiters,
        IEnumerable<SnapshotInfo> allSnapshots)
    {
        var byTime = allSnapshots
            .GroupBy(s => s.CollectionTime)
            .ToDictionary(
                g => g.Key,
                g => g.ToDictionary(s => s.SessionId));

        var headBlockers = new Dictionary<(DateTime, int), (SnapshotInfo Info, HashSet<int> BlockedSessions)>();

        foreach (var waiter in waiters)
        {
            if (!byTime.TryGetValue(waiter.CollectionTime, out var sessionsAtTime))
                continue;

            var head = FindHeadBlocker(waiter, sessionsAtTime);
            // head == waiter means the waiter is its own head: it is not blocked by another
            // session (BlockingSessionId <= 0, blocked-by-self, or the blocker is absent from
            // this snapshot). Recording it would render "Head SPID X blocking 1 session(s)"
            // where that one session is X itself.
            if (head == null || head.SessionId == waiter.SessionId)
                continue;

            var key = (waiter.CollectionTime, head.SessionId);
            if (!headBlockers.TryGetValue(key, out var existing))
            {
                existing = (head, new HashSet<int>());
                headBlockers[key] = existing;
            }

            existing.BlockedSessions.Add(waiter.SessionId);
        }

        return headBlockers.Values
            .Select(hb => new HeadBlockerInfo(
                hb.Info.CollectionTime,
                hb.Info.SessionId,
                hb.BlockedSessions.Count,
                $"Head SPID {hb.Info.SessionId} blocking {hb.BlockedSessions.Count} session(s)"))
            .OrderByDescending(r => r.BlockedSessionCount)
            .ThenByDescending(r => r.CollectionTime)
            .ToList();
    }

    private static SnapshotInfo? FindHeadBlocker(
        SnapshotInfo waiter,
        Dictionary<int, SnapshotInfo> sessionsAtTime)
    {
        var visited = new HashSet<int>();
        var current = waiter;

        for (int depth = 0; depth < MaxChainDepth; depth++)
        {
            if (!visited.Add(current.SessionId))
                return current; // cycle detected — treat current as head

            var blockerId = current.BlockingSessionId;

            // Head blocker: not blocked by anyone, or blocked by self, or blocker not found
            if (blockerId <= 0 || blockerId == current.SessionId)
                return current;

            if (!sessionsAtTime.TryGetValue(blockerId, out var blocker))
                return current; // blocker not in snapshot — treat current as head

            current = blocker;
        }

        return current; // max depth — treat current as head
    }
}
