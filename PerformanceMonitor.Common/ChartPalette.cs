/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Common
{
    /// <summary>
    /// The single, app-agnostic source of truth for chart COLOR IDENTITY — which color means what
    /// across Dashboard and Lite. Lives in Common (no ScottPlot / WPF dependency) and therefore
    /// returns plain hex strings; callers wrap with <c>ScottPlot.Color.FromHex(...)</c> or WPF
    /// <c>ColorConverter.ConvertFromString(...)</c> as needed (both already used throughout).
    ///
    /// Four resolvers, by the rule that keeps both apps consistent:
    ///   - <see cref="SeriesColor"/>  : a fixed, enumerable-meaning data series (Buffer Pool, SQL CPU, …).
    ///   - <see cref="AccentColor"/>  : thresholds / pressure-zones / anomaly accents.
    ///   - <see cref="WaitColor"/>    : a value in the wait taxonomy (use with <see cref="WaitCategory"/>).
    ///   - <see cref="CyclingColor"/> : an unbounded / data-driven member set (per-DB, per-plan, wait-type
    ///                                  lists) — assign by a STABLE index (e.g. sorted-key position).
    ///
    /// Series/category colors are intentionally CONSTANT across themes; the only per-theme variation is
    /// a light/CoolBreeze override for the few wait colors that are near-invisible on a light background
    /// (see <see cref="WaitColor"/>'s <c>lightBackground</c> parameter).
    /// </summary>
    public static class ChartPalette
    {
        // ── Cycling palette (Material Design 300/200) — for arbitrary, data-driven series ──
        // Ported from Dashboard's ChartColors so both apps share one ordering (Lite's old
        // SeriesColors was reordered + swapped #FFD54F<->#90A4AE, which broke cross-app parity).
        private static readonly string[] Cycling =
        {
            "#4FC3F7", "#81C784", "#FFB74D", "#E57373", "#BA68C8",
            "#4DD0E1", "#FFF176", "#F06292", "#AED581", "#90A4AE",
            "#A1887F", "#7986CB", "#FF7043", "#80DEEA", "#FFE082",
            "#CE93D8", "#EF9A9A", "#C5E1A5", "#FFCC80", "#B0BEC5",
        };

        /// <summary>Stable cycling color for index <paramref name="index"/> (wraps the palette).</summary>
        public static string CyclingColor(int index)
            => Cycling[((index % Cycling.Length) + Cycling.Length) % Cycling.Length];

        /// <summary>The raw cycling palette (hex), for callers that build their own color arrays.</summary>
        public static IReadOnlyList<string> CyclingPalette => Cycling;

        // ── Fixed-meaning data series ──────────────────────────────────────────────────────
        // Defines ONE color per named series, applied identically in both apps. This is the fix
        // for "Buffer Pool is three different colors today" (green in Lite, blue and purple in two
        // Dashboard charts). Unknown names fall back to stable cycling.
        private static readonly Dictionary<string, string> Series =
            new(StringComparer.OrdinalIgnoreCase)
            {
                ["SqlCpu"]        = "#4FC3F7", // SQL Server CPU %
                ["TotalCpu"]      = "#FF7043", // Total (non-idle) CPU %
                ["BufferPool"]    = "#81C784", // Buffer pool MB
                ["MemoryGrants"]  = "#FFB74D", // Granted memory MB
                ["OtherCpu"]          = "#E57373", // Other (non-SQL) process CPU %
                ["TotalServerMemory"] = "#4FC3F7", // Total server memory GB
                ["TargetMemory"]      = "#808080", // Target server memory (dashed grey reference line)
                ["Blocking"]          = "#FFB74D", // blocking count / incidents
                ["BlockingDuration"]  = "#FF7043", // total blocking duration
                ["Deadlocks"]         = "#E57373", // deadlock count
                ["DeadlockWaitTime"]  = "#BA68C8", // total deadlock wait time
                ["BlockedSessions"]   = "#EF9A9A", // blocked session count
                ["ReadLatency"]   = "#4DD0E1", // Avg read latency ms
                ["WriteLatency"]  = "#F06292", // Avg write latency ms
                ["Reads"]         = "#4DD0E1", // Logical/physical reads
                ["Writes"]        = "#F06292", // Logical/physical writes
                ["Duration"]      = "#4FC3F7", // Elapsed/duration
                ["Executions"]    = "#BA68C8", // Execution count
                // Session states (co-occur in the Sessions chart)
                ["SessionTotal"]      = "#4FC3F7",
                ["SessionRunning"]    = "#81C784",
                ["SessionSleeping"]   = "#FFB74D",
                ["SessionBackground"] = "#BA68C8",
                ["SessionDormant"]    = "#4DD0E1",
                ["SessionIdle"]       = "#90A4AE",
                ["SessionWaiting"]    = "#E57373",
                // Memory overview (total / plan cache / available)
                ["TotalMemory"]       = "#90A4AE",
                ["CacheMemory"]       = "#81C784",
                ["AvailableMemory"]   = "#FFB74D",
                // Memory pressure event severity (stacked event-count bars; SQL = orange family, OS = red family)
                ["SqlPressureMedium"] = "#FFB74D",
                ["SqlPressureSevere"] = "#E65100",
                ["OsPressureMedium"]  = "#E57373",
                ["OsPressureSevere"]  = "#B71C1C",
                // Plan cache size buckets
                ["SinglePagePlans"]   = "#E57373",
                ["MultiPagePlans"]    = "#81C784",
                // Tempdb object types
                ["UserObjects"]       = "#4FC3F7",
                ["VersionStore"]      = "#81C784",
                ["InternalObjects"]   = "#FFB74D",
                ["UnallocatedTempdb"] = "#90A4AE",
                ["TopTempdbTask"]     = "#E57373",
                // Duration trend charts (history windows / per-type)
                ["QueryDuration"]     = "#4FC3F7",
                ["ProcedureDuration"] = "#81C784",
                ["QueryStoreDuration"]= "#FFB74D",
                ["MetricTrend"]       = "#4FC3F7", // generic single-metric history trend
                ["LockWaits"]         = "#4FC3F7",
                ["CurrentWaits"]      = "#4FC3F7",
            };

        /// <summary>Color for a fixed-meaning series (e.g. "BufferPool"); falls back to cycling.</summary>
        public static string SeriesColor(string name)
            => Series.TryGetValue(name, out var hex) ? hex : CyclingColor(StableIndex(name));

        // ── Accents: thresholds, pressure zones, anomaly markers ───────────────────────────
        private static readonly Dictionary<string, string> Accents =
            new(StringComparer.OrdinalIgnoreCase)
            {
                ["Threshold"]      = "#FFD54F", // generic threshold / target line (yellow)
                ["PressureMedium"] = "#FFB74D", // medium pressure zone (orange)
                ["PressureSevere"] = "#E57373", // severe pressure zone (red)
                ["Anomaly"]        = "#FF5252", // anomaly dot
                ["Average"]        = "#FFD54F", // mean / average line
                ["BaselineCpu"]      = "#4FC3F7", // CPU-lane baseline band / mean tint
                ["BaselineBlocking"] = "#E57373", // blocking-lane baseline band / mean tint
                ["Crosshair"]        = "#FFFFFF", // correlated-charts crosshair vline
                ["GhostLine"]        = "#FFFFFF", // comparison-overlay ghost line (rendered semi-transparent)
                ["Placeholder"]      = "#888888", // no-data placeholder line
            };

        /// <summary>Color for a threshold / pressure-zone / anomaly accent.</summary>
        public static string AccentColor(string name)
            => Accents.TryGetValue(name, out var hex) ? hex : "#B0BEC5";

        // ── Wait categories ────────────────────────────────────────────────────────────────
        // PerformanceStudio's ~21 semantic wait-category colors (Themes/DarkTheme.axaml). Constant
        // across themes EXCEPT the handful flagged near-invisible on a light background, which get a
        // darker variant when lightBackground = true (D3).
        private static readonly Dictionary<string, string> WaitCategoryColors =
            new(StringComparer.OrdinalIgnoreCase)
            {
                ["CPU"]              = "#00BD23",
                ["Worker Thread"]    = "#52E3B5",
                ["Lock"]             = "#EE5A24",
                ["Latch"]            = "#F368E0",
                ["Buffer Latch"]     = "#F5069E",
                ["Buffer IO"]        = "#2D13F2",
                ["Compilation"]      = "#A29BFE",
                ["SQL CLR"]          = "#B8DCDC",
                ["Mirroring"]        = "#54625F",
                ["Transaction"]      = "#C77416",
                ["Preemptive"]       = "#FD79A8",
                ["Service Broker"]   = "#E17055",
                ["Tran Log IO"]      = "#0984E3",
                ["Network IO"]       = "#75BBF8",
                ["Parallelism"]      = "#7B4FFF",
                ["Batch Mode"]       = "#00CEC9",
                ["Memory"]           = "#FDCB6E",
                ["Tracing"]          = "#B2BEC3",
                ["Full Text Search"] = "#DFE6E9",
                ["Other Disk IO"]    = "#636E72",
                ["Replication"]      = "#81ECEC",
                ["Log Rate Governor"]= "#FAB1A0",
                ["Unknown"]          = "#8E8E93",
                ["Others"]           = "#555D66",
            };

        // Darker variants for the three categories that are near-invisible on light/CoolBreeze
        // backgrounds (D3). Only consulted when lightBackground = true.
        private static readonly Dictionary<string, string> WaitCategoryColorsLight =
            new(StringComparer.OrdinalIgnoreCase)
            {
                ["SQL CLR"]          = "#5BA3A3", // was #B8DCDC
                ["Full Text Search"] = "#8FA3AB", // was #DFE6E9
                ["Replication"]      = "#1FA8A8", // was #81ECEC
            };

        /// <summary>Color for a wait category. Pass lightBackground=true on Light/CoolBreeze themes.</summary>
        public static string WaitColor(string category, bool lightBackground = false)
        {
            if (lightBackground && WaitCategoryColorsLight.TryGetValue(category, out var light))
                return light;
            return WaitCategoryColors.TryGetValue(category, out var hex) ? hex : WaitCategoryColors["Others"];
        }

        /// <summary>
        /// Classifies a raw wait_type into one of the wait categories above. A CURATED mapping of
        /// the wait types that actually surface in query / DMV wait stats — NOT the full SQL Server
        /// internal table — following SQL Server's own categorization (so CXPACKET is Parallelism,
        /// not CPU, etc.). Anything unrecognized returns "Unknown".
        /// REVIEW NOTE: wait categorization is opinionated; tune to taste.
        /// </summary>
        public static string WaitCategory(string? waitType)
        {
            if (string.IsNullOrEmpty(waitType))
                return "Unknown";

            var w = waitType.ToUpperInvariant();

            bool Starts(string p) => w.StartsWith(p, StringComparison.Ordinal);

            // Parallelism — exchange-iterator / CX* family (genuine parallel-plan exchange waits).
            if (Starts("CXPACKET") || Starts("CXCONSUMER") || Starts("CXSYNC_") || Starts("EXCHANGE"))
                return "Parallelism";

            // Batch mode — hash-table / bitmap-filter / batch-sort operator waits (verified vs SQLskills
            // + sys.dm_os_wait_stats docs). BMPALLOCATION IS documented (MS docs: batch-mode bitmap
            // allocation) even though SQLskills' page is a stub, so it belongs here with its siblings.
            if (Starts("HTBUILD") || Starts("HTREPARTITION") || Starts("HTDELETE") || Starts("HTMEMO") ||
                Starts("BMPBUILD") || Starts("BMPREPARTITION") || Starts("BMPALLOCATION") || Starts("BPSORT"))
                return "Batch Mode";

            // SOS_WORK_DISPATCHER is deliberately NOT classified as CPU: it is an idle/benign
            // "waiting for something to do" wait (per SQLskills), not CPU pressure -> falls to Unknown.
            if (w == "SOS_SCHEDULER_YIELD")
                return "CPU";

            if (w == "THREADPOOL")
                return "Worker Thread";

            if (Starts("LCK_M_"))
                return "Lock";

            if (Starts("PAGEIOLATCH_"))
                return "Buffer IO";

            if (Starts("PAGELATCH_"))
                return "Buffer Latch";

            if (Starts("LATCH_"))
                return "Latch";

            if (w == "WRITELOG" || Starts("LOGBUFFER") || Starts("LOGMGR") || Starts("LOG_RATE"))
                return w.Contains("RATE", StringComparison.Ordinal) ? "Log Rate Governor" : "Tran Log IO";

            if (Starts("POOL_LOG_RATE_GOVERNOR") || Starts("INSTANCE_LOG_RATE_GOVERNOR") || Starts("LOG_RATE_GOVERNOR"))
                return "Log Rate Governor";

            if (w == "IO_COMPLETION" || w == "ASYNC_IO_COMPLETION" || Starts("BACKUPIO") ||
                w == "IO_QUEUE_LIMIT" || w == "IO_RETRY" || Starts("DISKIO"))
                return "Other Disk IO";

            if (w == "RESOURCE_SEMAPHORE" || w == "RESOURCE_SEMAPHORE_QUERY_COMPILE" ||
                w == "CMEMTHREAD" || Starts("MEMORY_ALLOCATION") || Starts("RESERVED_MEMORY") ||
                w == "RESOURCE_SEMAPHORE_SMALL_QUERY")
                return "Memory";

            if (w == "ASYNC_NETWORK_IO" || w == "NET_WAITFOR_PACKET" || w == "PROXY_NETWORK_IO" ||
                w == "EXTERNAL_SCRIPT_NETWORK_IO")
                return "Network IO";

            if (Starts("SQLCLR") || Starts("CLR_"))
                return "SQL CLR";

            if (Starts("DBMIRROR") || Starts("MIRROR_"))
                return "Mirroring";

            if (Starts("REPL_") || Starts("REPLICA_") || Starts("PUB_"))
                return "Replication";

            if (Starts("BROKER_") || Starts("SERVICE_BROKER") || Starts("SSB"))
                return "Service Broker";

            if (Starts("FT_") || Starts("FULLTEXT") || Starts("MSSEARCH"))
                return "Full Text Search";

            // MSQL_XP is NOT here: it's an extended-stored-procedure wait (per SQLskills), unrelated
            // to transactions -> falls through to Unknown.
            if (Starts("DTC") || Starts("XACT") || Starts("TRANSACTION_"))
                return "Transaction";

            if (Starts("TRACE") || Starts("SQLTRACE") || w == "QUERY_TRACEOUT")
                return "Tracing";

            if (Starts("PREEMPTIVE_"))
                return "Preemptive";

            return "Unknown";
        }

        private static int StableIndex(string key)
        {
            // Deterministic non-negative hash so an unmapped name keeps the same cycling color.
            unchecked
            {
                int h = 17;
                foreach (var ch in key) h = h * 31 + ch;
                return h & 0x7FFFFFFF;
            }
        }
    }
}
