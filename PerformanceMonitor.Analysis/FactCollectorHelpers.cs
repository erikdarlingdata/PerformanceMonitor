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

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Pure, data-source-agnostic fact-shaping helpers shared by the per-app fact collectors
/// (Dashboard's SqlServerFactCollector and Lite's DuckDbFactCollector). These operate only on the
/// already-collected <see cref="Fact"/> list plus <see cref="AnalysisContext"/>, so both apps emit
/// and group facts identically regardless of where the underlying data was read from. Keeping one
/// copy here prevents the two collectors from drifting apart.
/// </summary>
public static class FactCollectorHelpers
{
    /// <summary>
    /// RAM floor below which LPIM-off is not worth flagging — on a small buffer pool the OS paging
    /// SQL out is not the practical risk it is on a large dedicated host.
    /// </summary>
    public const long LpimAdvisoryMinPhysicalMemoryMb = 32 * 1024;

    /// <summary>
    /// Emits the WS5 advise-only server-health facts (IFI off / LPIM off / memory dumps) from the
    /// latest server_properties values, applying the noise-control gating both apps share:
    ///   • IFI: emit whenever the value is known (Value = enabled bit) — universally good advice.
    ///   • LPIM: emit only on non-Express editions with meaningful RAM (Value = enabled bit) — so a
    ///     tiny instance never flags. When LPIM is ON the emitted Value scores 0 (harmless).
    ///   • Dumps: emit whenever the count is known (Value = count) — the scorer flags count > 0.
    /// </summary>
    public static void EmitServerHealthFacts(
        AnalysisContext context, List<Fact> facts, string edition, long physicalMemMb,
        bool? lockPagesInMemory, bool? instantFileInit, int? memoryDumpCount)
    {
        var isExpress = edition.Contains("Express", StringComparison.OrdinalIgnoreCase);

        if (instantFileInit.HasValue)
        {
            facts.Add(new Fact
            {
                Source = "config",
                Key = "CONFIG_IFI_DISABLED",
                Value = instantFileInit.Value ? 1 : 0,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["instant_file_initialization_enabled"] = instantFileInit.Value ? 1 : 0
                }
            });
        }

        if (lockPagesInMemory.HasValue && !isExpress && physicalMemMb >= LpimAdvisoryMinPhysicalMemoryMb)
        {
            facts.Add(new Fact
            {
                Source = "config",
                Key = "CONFIG_LPIM_DISABLED",
                Value = lockPagesInMemory.Value ? 1 : 0,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["lock_pages_in_memory"] = lockPagesInMemory.Value ? 1 : 0,
                    ["physical_memory_mb"] = physicalMemMb
                }
            });
        }

        if (memoryDumpCount.HasValue)
        {
            facts.Add(new Fact
            {
                Source = "config",
                Key = "SERVER_MEMORY_DUMPS",
                Value = memoryDumpCount.Value,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["memory_dump_count"] = memoryDumpCount.Value
                }
            });
        }
    }

    /// <summary>
    /// Groups general lock waits (X, U, IX, SIX, BU, IU, UIX, etc.) into a single "LCK" fact.
    /// Keeps individual facts for:
    ///   - LCK_M_S, LCK_M_IS (reader/writer blocking — RCSI signal)
    ///   - LCK_M_RS_*, LCK_M_RIn_*, LCK_M_RX_* (serializable/repeatable read signal)
    ///   - SCH_M, SCH_S (schema locks — DDL/index operations)
    /// Individual constituent wait times are preserved in metadata as "{type}_ms" keys.
    /// </summary>
    public static void GroupGeneralLockWaits(List<Fact> facts, AnalysisContext context)
    {
        var generalLocks = facts.Where(f => f.Source == "waits" && IsGeneralLockWait(f.Key)).ToList();
        if (generalLocks.Count == 0) return;

        var totalWaitTimeMs = generalLocks.Sum(f => f.Metadata.GetValueOrDefault("wait_time_ms"));
        var totalWaitingTasks = generalLocks.Sum(f => f.Metadata.GetValueOrDefault("waiting_tasks_count"));
        var totalSignalMs = generalLocks.Sum(f => f.Metadata.GetValueOrDefault("signal_wait_time_ms"));
        var avgMsPerWait = totalWaitingTasks > 0 ? totalWaitTimeMs / totalWaitingTasks : 0;
        var fractionOfPeriod = totalWaitTimeMs / context.PeriodDurationMs;

        var metadata = new Dictionary<string, double>
        {
            ["wait_time_ms"] = totalWaitTimeMs,
            ["waiting_tasks_count"] = totalWaitingTasks,
            ["signal_wait_time_ms"] = totalSignalMs,
            ["resource_wait_time_ms"] = totalWaitTimeMs - totalSignalMs,
            ["avg_ms_per_wait"] = avgMsPerWait,
            ["period_duration_ms"] = context.PeriodDurationMs,
            ["lock_type_count"] = generalLocks.Count
        };

        // Preserve individual constituent wait times for detailed analysis
        foreach (var lck in generalLocks)
            metadata[$"{lck.Key}_ms"] = lck.Metadata.GetValueOrDefault("wait_time_ms");

        // Remove individual facts, add grouped fact
        foreach (var lck in generalLocks)
            facts.Remove(lck);

        facts.Add(new Fact
        {
            Source = "waits",
            Key = "LCK",
            Value = fractionOfPeriod,
            ServerId = context.ServerId,
            Metadata = metadata
        });
    }

    /// <summary>
    /// Groups all CX* parallelism waits (CXPACKET, CXCONSUMER, CXSYNC_PORT, CXSYNC_CONSUMER, etc.)
    /// into a single "CXPACKET" fact. They all indicate the same thing: parallel queries are running.
    /// Individual wait times are preserved in metadata for detailed analysis.
    /// </summary>
    public static void GroupParallelismWaits(List<Fact> facts, AnalysisContext context)
    {
        var cxWaits = facts.Where(f => f.Source == "waits" && f.Key.StartsWith("CX", StringComparison.Ordinal)).ToList();
        if (cxWaits.Count <= 1) return;

        var totalWaitTimeMs = cxWaits.Sum(f => f.Metadata.GetValueOrDefault("wait_time_ms"));
        var totalWaitingTasks = cxWaits.Sum(f => f.Metadata.GetValueOrDefault("waiting_tasks_count"));
        var totalSignalMs = cxWaits.Sum(f => f.Metadata.GetValueOrDefault("signal_wait_time_ms"));
        var avgMsPerWait = totalWaitingTasks > 0 ? totalWaitTimeMs / totalWaitingTasks : 0;
        var fractionOfPeriod = totalWaitTimeMs / context.PeriodDurationMs;

        var metadata = new Dictionary<string, double>
        {
            ["wait_time_ms"] = totalWaitTimeMs,
            ["waiting_tasks_count"] = totalWaitingTasks,
            ["signal_wait_time_ms"] = totalSignalMs,
            ["resource_wait_time_ms"] = totalWaitTimeMs - totalSignalMs,
            ["avg_ms_per_wait"] = avgMsPerWait,
            ["period_duration_ms"] = context.PeriodDurationMs
        };

        // Preserve individual constituent wait times for detailed analysis
        foreach (var cx in cxWaits)
            metadata[$"{cx.Key}_ms"] = cx.Metadata.GetValueOrDefault("wait_time_ms");

        foreach (var cx in cxWaits)
            facts.Remove(cx);

        facts.Add(new Fact
        {
            Source = "waits",
            Key = "CXPACKET",
            Value = fractionOfPeriod,
            ServerId = cxWaits[0].ServerId,
            Metadata = metadata
        });
    }

    /// <summary>
    /// Returns true for general lock waits that should be grouped into "LCK".
    /// Excludes reader locks (S, IS), range locks (RS_*, RIn_*, RX_*), and schema locks.
    /// </summary>
    private static bool IsGeneralLockWait(string waitType)
    {
        if (!waitType.StartsWith("LCK_M_", StringComparison.OrdinalIgnoreCase)) return false;

        // Keep individual: reader/writer locks
        if (waitType is "LCK_M_S" or "LCK_M_IS") return false;

        // Keep individual: range locks (serializable/repeatable read)
        if (waitType.StartsWith("LCK_M_RS_", StringComparison.OrdinalIgnoreCase) ||
            waitType.StartsWith("LCK_M_RIn_", StringComparison.OrdinalIgnoreCase) ||
            waitType.StartsWith("LCK_M_RX_", StringComparison.OrdinalIgnoreCase)) return false;

        // Everything else (X, U, IX, SIX, BU, IU, UIX, etc.) -> group
        return true;
    }
}
