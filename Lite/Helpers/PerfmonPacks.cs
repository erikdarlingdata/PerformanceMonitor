/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitorLite.Helpers;

public static class PerfmonPacks
{
    public const string AllCounters = "All Counters";

    public static readonly string[] PackNames =
    [
        AllCounters,
        "General Throughput",
        "Memory Pressure",
        "CPU / Compilation",
        "I/O Pressure",
        "TempDB Pressure",
        "Lock / Blocking",
    ];

    public static readonly Dictionary<string, string[]> Packs = new()
    {
        ["General Throughput"] =
        [
            "Batch Requests/sec",
            "SQL Compilations/sec",
            "SQL Re-Compilations/sec",
            "Query optimizations/sec",
            "Network IO waits",
        ],
        ["Memory Pressure"] =
        [
            "Memory Grants Pending",
            "Granted Workspace Memory (KB)",
            "Target Server Memory (KB)",
            "Total Server Memory (KB)",
            "Stolen Server Memory (KB)",
            "Lock Memory (KB)",
            "SQL Cache Memory (KB)",
            "Lazy writes/sec",
            "Free list stalls/sec",
            "Reduced memory grants/sec",
            "Memory grant queue waits",
            "Thread-safe memory objects waits",
            "Page reads/sec",
            "Readahead pages/sec",
        ],
        ["CPU / Compilation"] =
        [
            "Batch Requests/sec",
            "SQL Compilations/sec",
            "SQL Re-Compilations/sec",
            "Query optimizations/sec",
            "Active parallel threads",
            "Active requests",
            "Queued requests",
            "Wait for the worker",
        ],
        ["I/O Pressure"] =
        [
            "Page reads/sec",
            "Page writes/sec",
            "Checkpoint pages/sec",
            "Page lookups/sec",
            "Readahead pages/sec",
            "Background writer pages/sec",
            "Log Flushes/sec",
            "Log Bytes Flushed/sec",
            "Log Flush Write Time (ms)",
            "Page IO latch waits",
            "Log buffer waits",
            "Log write waits",
            "Full Scans/sec",
            "Index Searches/sec",
            "Page Splits/sec",
        ],
        ["TempDB Pressure"] =
        [
            "Version Store Size (KB)",
            "Free Space in tempdb (KB)",
            "Active Temp Tables",
            "Version Generation rate (KB/s)",
            "Version Cleanup rate (KB/s)",
            "Temp Tables Creation Rate",
            "Workfiles Created/sec",
            "Worktables Created/sec",
        ],
        ["Lock / Blocking"] =
        [
            "Lock Requests/sec",
            "Lock Wait Time (ms)",
            "Lock Waits/sec",
            "Number of Deadlocks/sec",
            "Table Lock Escalations/sec",
            "Blocked tasks",
            "Lock waits",
            "Non-Page latch waits",
            "Page latch waits",
            "Processes blocked",
            "Lock Timeouts/sec",
        ],
    };
}
