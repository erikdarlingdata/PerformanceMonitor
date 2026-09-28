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
/// #4546: the viewer's warning header text and ordering, pulled out of
/// <c>PlanViewerControl.Properties.cs</c> (WPF, Windows-only, can't run in a unit test on macOS) into
/// plain functions this project's test suite can call directly. Mirrors PerformanceStudio's
/// <c>PlanViewerControl.Properties.cs</c> plan-warning and node-warning sections: same ordering
/// (benefit descending, nulls last), same header suffix.
/// </summary>
public static class PlanWarningDisplay
{
    /// <summary>
    /// The warning's display header: "⚠ {type}{source tag} — up to {N}% benefit" when
    /// <see cref="PlanWarning.MaxBenefitPercent"/> has a value, or just "⚠ {type}{source tag}"
    /// when it's null (not quantifiable). Matches PerformanceStudio's plan-warning and
    /// node-warning header text exactly, minus PerformanceStudio's legacy tag (PM has no
    /// equivalent notion of a legacy warning).
    /// </summary>
    public static string PlanWarningHeader(PlanWarning warning)
    {
        var sourceTag = WarningSourceTag(warning);
        return warning.MaxBenefitPercent.HasValue
            ? $"\u26A0 {warning.WarningType}{sourceTag} \u2014 up to {FormatBenefitPercent(warning.MaxBenefitPercent.Value)}% benefit"
            : $"\u26A0 {warning.WarningType}{sourceTag}";
    }

    /// <summary>
    /// Orders warnings the way PerformanceStudio's viewer does: highest <see cref="PlanWarning.MaxBenefitPercent"/>
    /// first, unscored (null) warnings last, ties broken by severity descending then warning type ascending.
    /// </summary>
    public static IEnumerable<PlanWarning> OrderByBenefit(IEnumerable<PlanWarning> warnings) =>
        warnings
            .OrderByDescending(w => w.MaxBenefitPercent ?? -1)
            .ThenByDescending(w => w.Severity)
            .ThenBy(w => w.WarningType);

    /// <summary>#4520: marks the warnings SQL Server itself wrote into the plan, so they are not read as one
    /// of the analyzer's inferences. Only the engine's are tagged — they are the minority, and a badge on
    /// every warning would carry no information.</summary>
    private static string WarningSourceTag(PlanWarning warning) =>
        warning.Source == PlanWarningSource.SqlServer ? " [SQL Server]" : "";

    private static string FormatBenefitPercent(double pct) =>
        pct >= 100 ? $"{pct:N0}" : $"{pct:N1}";
}
