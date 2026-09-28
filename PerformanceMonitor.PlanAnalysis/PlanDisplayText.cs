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
}
