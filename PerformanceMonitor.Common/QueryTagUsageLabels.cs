/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Common;

/// <summary>
/// #6878: the words the Resource use by query tag screens put on a section heading and a column header, in one place for the Darling
/// viewer and Lite. They are the labels the web Query Tag Usage page uses (<c>query-tag-usage.js</c>, <c>SECTIONS</c> and
/// <c>MEASURE_LABELS</c>), so the three surfaces read alike. The MCP field names (<c>plan_cache</c>, <c>cpu_ms</c>) stay the keys of
/// the answers; only what is drawn changes. A name that is not listed is returned as it came.
/// </summary>
public static class QueryTagUsageLabels
{
    private static readonly Dictionary<string, string> Sections = new()
    {
        ["plan_cache"] = "Plan cache",
        ["query_store"] = "Query Store",
        ["procedures"] = "Procedures",
        ["blocking_reports"] = "Blocking reports",
        ["blocking_snapshots"] = "Blocking snapshots",
        ["deadlocks"] = "Deadlocks",
    };

    private static readonly Dictionary<string, string> Measures = new()
    {
        ["cpu_ms"] = "CPU (ms)",
        ["duration_ms"] = "Duration (ms)",
        ["logical_reads"] = "Logical reads",
        ["logical_writes"] = "Logical writes",
        ["executions"] = "Executions",
        ["peak_memory_pages"] = "Peak memory (pages)",
        ["blocked_ms"] = "Blocked (ms)",
        ["caused_ms"] = "Caused (ms)",
        ["blocked_reports"] = "Blocked reports",
        ["caused_reports"] = "Caused reports",
        ["blocked_snapshots"] = "Blocked snapshots",
        ["caused_snapshots"] = "Caused snapshots",
        ["deadlocks"] = "Deadlocks",
        ["processes"] = "Processes",
    };

    /// <summary>The section heading for a section's field name ("plan_cache" reads "Plan cache").</summary>
    public static string SectionTitle(string name) => Sections.TryGetValue(name, out var title) ? title : name;

    /// <summary>The column header for a measure's field name ("cpu_ms" reads "CPU (ms)").</summary>
    public static string MeasureLabel(string name) => Measures.TryGetValue(name, out var label) ? label : name;
}
