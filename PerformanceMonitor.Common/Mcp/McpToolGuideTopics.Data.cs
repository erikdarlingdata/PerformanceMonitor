/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>The data family's topics (#3898).</summary>
public static partial class McpToolGuideTopics
{
    /// <summary>The <see cref="CpuTimeExtremesAndAttribution"/> topic's name.</summary>
    public const string CpuTimeExtremesAndAttributionName = "cpu_time_extremes_and_attribution";

    /// <summary>
    /// get_top_queries_by_cpu and get_top_procedures_by_cpu carried this identical sentence: min/max_cpu_ms
    /// and min/max_elapsed_ms are lifetime extremes rather than windowed, and cpu_attribution's ratio is
    /// omitted, never invented, when its inputs are missing.
    /// </summary>
    public const string CpuTimeExtremesAndAttribution =
        "min/max_cpu_ms and min/max_elapsed_ms are LIFETIME extremes for the plan's time in cache (same " +
        "semantics as max_dop), not windowed — totals and avgs are windowed deltas; rows where an extreme " +
        "provably predates the window carry extremes_note. Also returns cpu_attribution: the returned rows' " +
        "summed CPU-seconds against the SQL process's measured CPU-seconds for the window (avg cpu_utilization " +
        "% x core count x window) - attributed_cpu_ratio says how much of the box the ranking explains; when " +
        "the CPU series or core count is missing, or covers too little of the window, the ratio is omitted " +
        "rather than invented.";

    private static readonly McpToolGuideTopic[] s_data =
    [
        new(
            CpuTimeExtremesAndAttributionName,
            "How min/max CPU/elapsed ms and cpu_attribution's ratio read on the top_queries/top_procedures CPU rankings.",
            CpuTimeExtremesAndAttribution),
    ];
}
