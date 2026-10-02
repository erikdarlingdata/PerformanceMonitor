/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// #4576: the Wait Stats card's per-row "up to N%" benefit text, pulled out of
/// <c>PlanViewerControl.Properties.cs</c>'s <c>ShowWaitStats</c> (WPF, Windows-only, can't run in a
/// unit test on macOS) into a plain function this project's test suite can call directly. Mirrors
/// PerformanceStudio's <c>PlanViewerControl.WaitStats.cs</c>, which joins each wait row to the
/// benefit the analyzer already scored for it (<see cref="PlanWarning.MaxBenefitPercent"/> on the
/// "Wait: {type}" finding <c>BenefitScorer.EmitWaitStatWarnings</c> emits) rather than recomputing
/// anything here — the row is display only.
/// </summary>
public static class WaitRowText
{
    /// <summary>
    /// The benefit text for one wait row: "up to N%" when a "Wait: {waitType}" finding among
    /// <paramref name="statementWarnings"/> carries a positive <see cref="PlanWarning.MaxBenefitPercent"/>,
    /// or null when there is no matching finding, no score, or the score is zero or negative.
    /// A wait type that never made it into <c>BenefitScorer.EmitWaitStatWarnings</c> (it skips
    /// zero-duration waits) has no finding to match and gets no benefit text, same as PS's
    /// lookup-miss case. The match is case-insensitive because the wait grid and the warning list
    /// both ultimately read the wait type off the same captured plan, but never assume identical
    /// casing between the two paths.
    /// </summary>
    public static string? Benefit(string waitType, IEnumerable<PlanWarning> statementWarnings)
    {
        var finding = statementWarnings.FirstOrDefault(w =>
            w.WarningType.Equals("Wait: " + waitType, System.StringComparison.OrdinalIgnoreCase));

        if (finding?.MaxBenefitPercent is not double pct || pct <= 0)
            return null;

        return $"up to {pct:N0}%";
    }
}
