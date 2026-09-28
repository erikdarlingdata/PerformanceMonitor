using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// One row of the runtime summary card: a label, a display value, and an optional theme brush
/// resource key ("ErrorBrush"/"WarningBrush"), where null means the card's default value color.
/// </summary>
public readonly record struct RuntimeSummaryRow(string Label, string Value, string? ColorKey = null);

/// <summary>
/// Small, pure display-text helpers shared by every plan-analysis surface (viewer, MCP, drill-down),
/// kept here so a unit test can pin the exact wording without needing WPF (<c>PerformanceMonitor.Ui</c>
/// is net10.0-windows-only and cannot run on non-Windows test hosts).
/// </summary>
public static class PlanDisplayText
{
    /// <summary>
    /// The message a plan viewer shows in place of its empty state when <see cref="ParsedPlan.ParseError"/>
    /// is set. Returns null when the plan parsed without error, so a caller can treat null as "show the
    /// normal empty/rendered state instead".
    /// </summary>
    public static string? ParseErrorMessage(ParsedPlan plan) =>
        plan.ParseError == null ? null : $"This plan couldn't be parsed: {plan.ParseError}";

    /// <summary>
    /// The CPU:Elapsed ratio shown in the runtime summary, matching
    /// erikdarlingdata/PerformanceStudio@28d4c74's row: CPU time with external-wait time
    /// (<see cref="BenefitScorer.IsExternalWait"/>) subtracted, divided by elapsed time.
    /// Returns null when elapsed time is not positive, in which case the row isn't shown.
    /// </summary>
    public static double? CpuElapsedRatio(PlanStatement statement)
    {
        var stats = statement.QueryTimeStats;
        if (stats == null || stats.ElapsedTimeMs <= 0)
            return null;

        long externalWaitMs = 0;
        foreach (var w in statement.WaitStats)
            if (BenefitScorer.IsExternalWait(w.WaitType))
                externalWaitMs += w.WaitTimeMs;

        var effectiveCpu = Math.Max(0L, stats.CpuTimeMs - externalWaitMs);
        return (double)effectiveCpu / stats.ElapsedTimeMs;
    }

    /// <summary>
    /// The runtime summary card's title, matching erikdarlingdata/PerformanceStudio@40ade29 (E7):
    /// "Predicted Runtime" when the statement has no <see cref="PlanStatement.QueryTimeStats"/>
    /// (an estimated-plan-only statement), "Runtime Summary" otherwise.
    /// </summary>
    public static string RuntimeSummaryTitle(PlanStatement statement) =>
        statement.QueryTimeStats == null ? "Predicted Runtime" : "Runtime Summary";

    /// <summary>
    /// Walks a plan's node tree looking for any warning whose type ends in " Spill" (Sort Spill,
    /// Hash Spill, Exchange Spill — see <see cref="ShowPlanParser"/>'s warning builders), matching
    /// erikdarlingdata/PerformanceStudio@40ade29 (E9/E10)'s <c>HasSpillInPlanTree</c>. A plan-wide spill
    /// forces the memory grant row to at best its warning tier, regardless of utilization.
    /// </summary>
    public static bool HasSpillInPlanTree(PlanNode? node)
    {
        if (node == null)
            return false;

        foreach (var w in node.Warnings)
            if (w.WarningType.EndsWith(" Spill", StringComparison.Ordinal))
                return true;

        foreach (var child in node.Children)
            if (HasSpillInPlanTree(child))
                return true;

        return false;
    }

    /// <summary>
    /// The theme brush resource key for a utilization percentage (DOP efficiency, thread
    /// utilization), matching erikdarlingdata/PerformanceStudio@5731ae9 (C1)'s <c>EfficiencyColor</c>
    /// thresholds: &gt;= 40% is the card's default value color (returned as null), 20-39% is the
    /// warning tier, and below 20% is the error tier.
    /// </summary>
    public static string? EfficiencyColorKey(double pct) =>
        pct >= 40 ? null : pct >= 20 ? "WarningBrush" : "ErrorBrush";

    /// <summary>
    /// The theme brush resource key for the memory grant row, matching
    /// erikdarlingdata/PerformanceStudio@40ade29 (E8/E9)'s <c>MemoryGrantBrushKey</c>: an over-used
    /// grant (&gt; 100%) is always the error tier; any spill anywhere in the plan forces at best the
    /// warning tier regardless of utilization; otherwise the tier follows <see cref="EfficiencyColorKey"/>'s
    /// same 40%/20% thresholds.
    /// </summary>
    public static string? MemoryGrantColorKey(double pctUsed, bool hasSpill)
    {
        if (pctUsed > 100)
            return "ErrorBrush";
        if (hasSpill)
            return "WarningBrush";
        return EfficiencyColorKey(pctUsed);
    }

    /// <summary>
    /// Formats a memory value given in KB to a human-readable string. Under 1,024 KB: show KB.
    /// 1,024-1,048,576 KB: show MB (1 decimal). Over 1,048,576 KB: show GB (2 decimals).
    /// </summary>
    public static string FormatMemoryGrantKB(long kb)
    {
        if (kb < 1024)
            return $"{kb:N0} KB";
        if (kb < 1024 * 1024)
            return $"{kb / 1024.0:N1} MB";
        return $"{kb / (1024.0 * 1024.0):N2} GB";
    }

    /// <summary>
    /// Builds the runtime summary card's rows in erikdarlingdata/PerformanceStudio@40ade29 (E11)'s
    /// order: Elapsed, CPU:Elapsed, DOP (or Serial reason), CPU, UDF CPU, UDF elapsed, Compile,
    /// Cached plan size, Memory grant, Branches, Threads, Optimization, Early abort, CE model. A row
    /// is omitted entirely when its underlying value isn't present, matching the WPF card's own
    /// omission rules.
    /// </summary>
    public static IReadOnlyList<RuntimeSummaryRow> BuildRuntimeSummaryRows(
        PlanStatement statement)
    {
        var rows = new List<RuntimeSummaryRow>();
        var stats = statement.QueryTimeStats;

        if (stats != null)
        {
            rows.Add(new RuntimeSummaryRow("Elapsed", $"{stats.ElapsedTimeMs:N0}ms"));

            var cpuElapsedRatio = CpuElapsedRatio(statement);
            if (cpuElapsedRatio != null)
                rows.Add(new RuntimeSummaryRow("CPU:Elapsed", cpuElapsedRatio.Value.ToString("N2")));
        }

        if (statement.DegreeOfParallelism > 0)
        {
            var dopText = statement.DegreeOfParallelism.ToString();
            string? dopColorKey = null;
            if (stats != null && stats.ElapsedTimeMs > 0 && stats.CpuTimeMs > 0 && statement.DegreeOfParallelism > 1)
            {
                long externalWaitMs = 0;
                foreach (var w in statement.WaitStats)
                    if (BenefitScorer.IsExternalWait(w.WaitType))
                        externalWaitMs += w.WaitTimeMs;

                var effectiveCpu = Math.Max(0L, stats.CpuTimeMs - externalWaitMs);
                var speedup = (double)effectiveCpu / stats.ElapsedTimeMs;
                var efficiency = Math.Min(100.0, (speedup - 1.0) / (statement.DegreeOfParallelism - 1.0) * 100.0);
                efficiency = Math.Max(0.0, efficiency);
                dopText += $" ({efficiency:N0}% efficient)";
                dopColorKey = EfficiencyColorKey(efficiency);
            }
            rows.Add(new RuntimeSummaryRow("DOP", dopText, dopColorKey));
        }
        else if (statement.NonParallelPlanReason != null)
            rows.Add(new RuntimeSummaryRow("Serial", statement.NonParallelPlanReason));

        if (stats != null)
        {
            rows.Add(new RuntimeSummaryRow("CPU", $"{stats.CpuTimeMs:N0}ms"));
            if (statement.QueryUdfCpuTimeMs > 0)
                rows.Add(new RuntimeSummaryRow("UDF CPU", $"{statement.QueryUdfCpuTimeMs:N0}ms"));
            if (statement.QueryUdfElapsedTimeMs > 0)
                rows.Add(new RuntimeSummaryRow("UDF elapsed", $"{statement.QueryUdfElapsedTimeMs:N0}ms"));
        }

        if (statement.CompileTimeMs > 0)
            rows.Add(new RuntimeSummaryRow("Compile", $"{statement.CompileTimeMs:N0}ms"));
        if (statement.CachedPlanSizeKB > 0)
            rows.Add(new RuntimeSummaryRow("Cached plan size", $"{statement.CachedPlanSizeKB:N0} KB"));

        if (statement.MemoryGrant != null)
        {
            var mg = statement.MemoryGrant;
            var grantPct = mg.GrantedMemoryKB > 0 ? (double)mg.MaxUsedMemoryKB / mg.GrantedMemoryKB * 100 : 100;
            var hasSpillInTree = HasSpillInPlanTree(statement.RootNode);
            var grantColorKey = MemoryGrantColorKey(grantPct, hasSpillInTree);
            var spillTag = hasSpillInTree ? " \u26a0 spill" : "";
            rows.Add(new RuntimeSummaryRow(
                "Memory grant",
                $"{FormatMemoryGrantKB(mg.GrantedMemoryKB)} granted, {FormatMemoryGrantKB(mg.MaxUsedMemoryKB)} used ({grantPct:N0}%){spillTag}",
                grantColorKey));

            if (mg.GrantWaitTimeMs > 0)
                rows.Add(new RuntimeSummaryRow("Grant wait", $"{mg.GrantWaitTimeMs:N0}ms", "ErrorBrush"));
        }

        if (statement.ThreadStats != null)
        {
            var ts = statement.ThreadStats;
            rows.Add(new RuntimeSummaryRow("Branches", ts.Branches.ToString()));

            var totalReserved = ts.Reservations.Sum(r => r.ReservedThreads);
            if (totalReserved > 0)
            {
                var threadPct = (double)ts.UsedThreads / totalReserved * 100;
                var threadColorKey = EfficiencyColorKey(threadPct);
                var threadText = ts.UsedThreads == totalReserved
                    ? $"{ts.UsedThreads} used ({totalReserved} reserved)"
                    : $"{ts.UsedThreads} used of {totalReserved} reserved ({totalReserved - ts.UsedThreads} inactive)";
                rows.Add(new RuntimeSummaryRow("Threads", threadText, threadColorKey));
            }
            else
            {
                rows.Add(new RuntimeSummaryRow("Threads", $"{ts.UsedThreads} used"));
            }
        }

        if (!string.IsNullOrEmpty(statement.StatementOptmLevel))
            rows.Add(new RuntimeSummaryRow("Optimization", statement.StatementOptmLevel));
        if (!string.IsNullOrEmpty(statement.StatementOptmEarlyAbortReason))
            rows.Add(new RuntimeSummaryRow("Early abort", statement.StatementOptmEarlyAbortReason));
        if (statement.CardinalityEstimationModelVersion > 0)
            rows.Add(new RuntimeSummaryRow("CE model", statement.CardinalityEstimationModelVersion.ToString()));

        return rows;
    }
}
