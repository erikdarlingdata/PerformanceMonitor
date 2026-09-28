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
        // ── Cycling palette: validated colorblind-safe categorical ramp ──
        // Slots 1-8 are the primary ramp, chosen with the dataviz palette validator:
        // CVD-safe (worst adjacent deltaE ~24) and >= 3:1 contrast on all three chart
        // surfaces (dark #111217, light #F5F7FA, CoolBreeze #DAE6F0), with no gray-reading
        // (low-chroma) hues. This is ONE theme-independent list by design (see class
        // remarks), so the hues are mid-tone cross-theme compromises rather than the
        // brighter per-theme optima. Slots 9-20 are a chromatic fallback tier: color stops
        // reliably distinguishing past 8, so many-series charts lean on legends/labels.
        // Keep the slot 1-8 order; it is the CVD-safety mechanism.
        private static readonly string[] Cycling =
        {
            "#2A78D6", "#1BAF7A", "#EDA100", "#008300", "#6C5CE7",
            "#E34948", "#E87BA4", "#EB6834", "#17A2B8", "#B5179E",
            "#74B816", "#AD6B3C", "#3D5AFE", "#C2185B", "#2E7D32",
            "#8E44AD", "#0097A7", "#D81B60", "#7CB342", "#5C6BC0",
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
        //
        // Identity series draw from the validated Cycling ramp above (co-occurring series in a
        // given chart are kept on distinct ramp slots). Severity series use reserved status hues
        // (dataviz good/warning/serious/critical) so a "getting worse" signal never reuses an
        // identity hue.
        // A few series are intentional NEUTRALS (a dashed reference line, an inactive/idle state,
        // an unallocated remainder) and stay low-chroma grey on purpose.
        private static readonly Dictionary<string, string> Series =
            new(StringComparer.OrdinalIgnoreCase)
            {
                ["SqlCpu"]        = "#2A78D6", // SQL Server CPU %
                ["TotalCpu"]      = "#EB6834", // Total (non-idle) CPU %
                ["BufferPool"]    = "#008300", // Buffer pool MB
                ["MemoryGrants"]  = "#EDA100", // Granted memory MB
                ["OtherCpu"]          = "#E34948", // Other (non-SQL) process CPU %
                ["TotalServerMemory"] = "#2A78D6", // Total server memory GB
                ["TargetMemory"]      = "#808080", // Target server memory (dashed grey reference line)
                ["Blocking"]          = "#2A78D6", // blocking count / incidents
                ["BlockingDuration"]  = "#EDA100", // total blocking duration
                ["Deadlocks"]         = "#E34948", // deadlock count
                ["DeadlockWaitTime"]  = "#6C5CE7", // total deadlock wait time
                ["BlockedSessions"]   = "#17A2B8", // blocked session count (spread off the warm cluster; tritan-safe)
                ["ReadLatency"]   = "#17A2B8", // Avg read latency ms
                ["WriteLatency"]  = "#E87BA4", // Avg write latency ms
                ["Reads"]         = "#17A2B8", // Logical/physical reads
                ["Writes"]        = "#E87BA4", // Logical/physical writes
                ["Duration"]      = "#2A78D6", // Elapsed/duration
                ["Executions"]    = "#6C5CE7", // Execution count
                // Session states (co-occur in the Sessions chart)
                ["SessionTotal"]      = "#2A78D6",
                ["SessionRunning"]    = "#008300",
                ["SessionSleeping"]   = "#EDA100",
                ["SessionBackground"] = "#6C5CE7",
                ["SessionDormant"]    = "#17A2B8",
                ["SessionIdle"]       = "#90A4AE", // intentional neutral: inactive state
                ["SessionWaiting"]    = "#E34948",
                // Memory overview (total / plan cache / available)
                ["TotalMemory"]       = "#90A4AE", // intentional neutral: overall envelope
                ["CacheMemory"]       = "#008300",
                ["AvailableMemory"]   = "#EDA100",
                // Memory pressure event severity (status palette: SQL = amber->orange, OS = salmon->red)
                ["SqlPressureMedium"] = "#FAB219",
                ["SqlPressureSevere"] = "#E8720C",
                ["OsPressureMedium"]  = "#EC835A",
                ["OsPressureSevere"]  = "#D03B3B",
                // Plan cache size buckets
                ["SinglePagePlans"]   = "#E34948",
                ["MultiPagePlans"]    = "#008300",
                // Tempdb object types
                ["UserObjects"]       = "#2A78D6",
                ["VersionStore"]      = "#008300",
                ["InternalObjects"]   = "#EDA100",
                ["UnallocatedTempdb"] = "#90A4AE", // intentional neutral: unallocated remainder
                ["TopTempdbTask"]     = "#E34948",
                // Duration trend charts (history windows / per-type)
                ["QueryDuration"]     = "#2A78D6",
                ["ProcedureDuration"] = "#008300",
                ["QueryStoreDuration"]= "#EDA100",
                ["MetricTrend"]       = "#2A78D6", // generic single-metric history trend
                ["LockWaits"]         = "#2A78D6",
                ["CurrentWaits"]      = "#2A78D6",
            };

        /// <summary>Color for a fixed-meaning series (e.g. "BufferPool"); falls back to cycling.</summary>
        public static string SeriesColor(string name)
            => Series.TryGetValue(name, out var hex) ? hex : CyclingColor(StableIndex(name));

        // ── Accents: thresholds, pressure zones, anomaly markers ───────────────────────────
        private static readonly Dictionary<string, string> Accents =
            new(StringComparer.OrdinalIgnoreCase)
            {
                ["Threshold"]      = "#E8B21E", // generic threshold / target line (gold)
                ["PressureMedium"] = "#FAB219", // medium pressure zone (status warning)
                ["PressureSevere"] = "#D03B3B", // severe pressure zone (status critical)
                ["Anomaly"]        = "#E8352E", // anomaly dot (critical red)
                ["Average"]        = "#E8B21E", // mean / average line (gold)
                ["BaselineCpu"]      = "#2A78D6", // CPU-lane baseline band / mean tint (matches SqlCpu)
                ["BaselineBlocking"] = "#E34948", // blocking-lane baseline band / mean tint
                ["Crosshair"]        = "#FFFFFF", // correlated-charts crosshair vline
                ["GhostLine"]        = "#FFFFFF", // comparison-overlay ghost line (rendered semi-transparent)
                ["Placeholder"]      = "#888888", // no-data placeholder line
                ["Discontinuity"]    = "#E8B21E", // #3653 A5 baseline-discontinuity marker (dashed vline; gold, like a threshold: chart chrome, not a series)
            };

        /// <summary>Color for a threshold / pressure-zone / anomaly accent.</summary>
        public static string AccentColor(string name)
            => Accents.TryGetValue(name, out var hex) ? hex : "#B0BEC5";

        // ── Wait categories ────────────────────────────────────────────────────────────────
        // PerformanceStudio's wait-category colors (Themes/DarkTheme.axaml, laid out rather than
        // picked: erikdarlingdata/PerformanceStudio@161785f). Hue carries the family - greens for
        // compute, blues for storage and network, warm for contention, grey for "no information" -
        // and within a family the categories climb a lightness ladder, which is what keeps
        // neighbours apart under simulated deuteranopia. Every color clears 3:1 contrast against
        // the dark chart background. Constant across themes EXCEPT the handful flagged
        // near-invisible on a light background, which get a darker variant when
        // lightBackground = true (D3).
        private static readonly Dictionary<string, string> WaitCategoryColors =
            new(StringComparer.OrdinalIgnoreCase)
            {
                ["CPU"]              = "#3EBD50",
                ["Worker Thread"]    = "#72E1A9",
                ["Lock"]             = "#FB6640",
                ["Latch"]            = "#FEB5ED",
                ["Buffer Latch"]     = "#E8469D",
                ["Buffer IO"]        = "#238CFA",
                ["Compilation"]      = "#A1A3FB",
                ["SQL CLR"]          = "#63C5C6",
                ["Mirroring"]        = "#30907E",
                ["Transaction"]      = "#DB9027",
                ["Preemptive"]       = "#FFA8C0",
                ["Service Broker"]   = "#FF9289",
                ["Tran Log IO"]      = "#8AC6FF",
                ["Network IO"]       = "#BBE2FE",
                ["Parallelism"]      = "#9A84FE",
                ["Batch Mode"]       = "#00CEC9",
                ["Memory"]           = "#FFE6B1",
                ["Tracing"]          = "#BDC8CE",
                ["Full Text Search"] = "#D8F2FC",
                ["Other Disk IO"]    = "#0FB3E2",
                ["Replication"]      = "#5DF9ED",
                ["Log Rate Governor"]= "#FFCACA",
                ["Unknown"]          = "#9CA1A5",
                ["Others"]           = "#676E73",
            };

        // Darker variants for the categories that are near-invisible on light/CoolBreeze
        // backgrounds (D3). Only consulted when lightBackground = true. Re-checked against the
        // new hexes above (161785f): SQL CLR and Full Text Search still need a light-theme
        // override; Replication's new hue is dark enough to read on light backgrounds unmodified,
        // so its override is dropped (PM-ahead: kept the mechanism, retired the entry).
        private static readonly Dictionary<string, string> WaitCategoryColorsLight =
            new(StringComparer.OrdinalIgnoreCase)
            {
                ["SQL CLR"]          = "#3E8384", // was #5BA3A3, now darkened vs new #63C5C6
                ["Full Text Search"] = "#6E8890", // was #8FA3AB, now darkened vs new #D8F2FC
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
