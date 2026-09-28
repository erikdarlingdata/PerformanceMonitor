using System.Collections.Generic;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Pure formatting for the estimated-plan memory grant row(s) on the Runtime card (#4569).
/// Mirrors PerformanceStudio's Runtime-card estimated-memory display: a plan with no
/// <see cref="PlanStatement.QueryTimeStats"/> (an estimated plan) but a non-zero optimizer
/// "desired" grant shows "Memory (estimated): N desired", plus a "Serial required" row when
/// the serial-required grant differs from the desired grant.
/// </summary>
public static class EstimatedMemoryGrantDisplay
{
    /// <summary>
    /// Returns the estimated-plan memory grant label/value row(s) for <paramref name="statement"/>,
    /// in display order. Empty for actual plans (<see cref="PlanStatement.QueryTimeStats"/> set) and
    /// for estimated plans with no desired grant.
    /// </summary>
    public static List<(string Label, string Value)> EstimatedMemoryGrantRows(PlanStatement statement)
    {
        var rows = new List<(string Label, string Value)>();

        if (statement.QueryTimeStats != null)
            return rows;

        var mg = statement.MemoryGrant;
        if (mg == null || mg.DesiredMemoryKB <= 0)
            return rows;

        rows.Add(("Memory (estimated)", $"{FormatKB(mg.DesiredMemoryKB)} desired"));

        if (mg.SerialRequiredMemoryKB > 0 && mg.SerialRequiredMemoryKB != mg.DesiredMemoryKB)
            rows.Add(("Serial required", FormatKB(mg.SerialRequiredMemoryKB)));

        return rows;
    }

    /// <summary>
    /// Formats a memory value given in KB to a human-readable string.
    /// Under 1,024 KB: show KB. 1,024-1,048,576 KB: show MB (1 decimal). Over 1,048,576 KB: show GB (2 decimals).
    /// </summary>
    private static string FormatKB(long kb)
    {
        if (kb < 1024)
            return $"{kb:N0} KB";
        if (kb < 1024 * 1024)
            return $"{kb / 1024.0:N1} MB";
        return $"{kb / (1024.0 * 1024.0):N2} GB";
    }
}
