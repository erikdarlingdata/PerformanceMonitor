using System;

namespace PerformanceMonitor.PlanAnalysis;

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
    /// The Wait Stats card's collapsible header text, matching erikdarlingdata/PerformanceStudio@9b8252e:
    /// "Wait Stats" alone when there's nothing to show, or "Wait Stats" plus the total wait time in
    /// milliseconds when there is. <paramref name="totalWaitMs"/> is the sum of every wait's WaitTimeMs.
    /// </summary>
    public static string WaitStatsHeader(int waitCount, long totalWaitMs) =>
        waitCount > 0 ? $"Wait Stats {totalWaitMs:N0} ms" : "Wait Stats";

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
}
