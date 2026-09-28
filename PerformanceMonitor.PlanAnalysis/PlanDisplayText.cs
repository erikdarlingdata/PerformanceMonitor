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
    /// The text "Copy Query Text" hands back for <paramref name="statement"/>: the plan's own copy
    /// normally, or the query the plan was captured from when the plan's copy hit SQL Server's
    /// 4,000-character showplan cap (<see cref="PlanStatement.IsTextTruncated"/>) -- matching
    /// erikdarlingdata/PerformanceStudio@35249e2, corrected by @fdd3d22.
    ///
    /// <para>Single-statement plans only, counted the same way the Statements grid counts them
    /// (<paramref name="statementCount"/> is the caller's already-flattened count, descending into
    /// stored procedure and UDF bodies -- NOT <c>Batches.Sum</c>, which stops at the outer batch and
    /// would call a plan captured around <c>EXEC dbo.SomeProc</c> single-statement). The captured text
    /// is the whole batch and showplan records no statement offsets, so for more than one statement
    /// there is no way to tell which slice belongs to the row the user picked -- handing back a
    /// confidently wrong statement is worse than handing back a short one.</para>
    /// </summary>
    public static string CopyQueryText(PlanStatement statement, int statementCount, string? capturedQueryText) =>
        statement.IsTextTruncated && statementCount == 1 && !string.IsNullOrEmpty(capturedQueryText)
            ? capturedQueryText
            : statement.StatementText;
}
