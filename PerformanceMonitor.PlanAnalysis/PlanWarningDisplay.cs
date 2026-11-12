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
    /// The warning's display header: "⚠ {type}{source tag}{legacy tag}: up to {N}% benefit" when
    /// <see cref="PlanWarning.MaxBenefitPercent"/> has a value, or just "⚠ {type}{source tag}{legacy tag}"
    /// when it's null (not quantifiable). Matches PerformanceStudio's plan-warning and
    /// node-warning header text exactly, including the " [legacy]" tag (#4566), ported from
    /// erikdarlingdata/PerformanceStudio dev (85492a1) <c>src/PlanViewer.App/Controls/PlanViewerControl.Properties.cs:862,948,1024</c>:
    /// source tag, then legacy tag, then the benefit suffix.
    /// </summary>
    public static string PlanWarningHeader(PlanWarning warning)
    {
        var sourceTag = WarningSourceTag(warning);
        var legacyTag = warning.IsLegacy ? " [legacy]" : "";
        return warning.MaxBenefitPercent.HasValue
            ? $"\u26A0 {warning.WarningType}{sourceTag}{legacyTag}: up to {FormatBenefitPercent(warning.MaxBenefitPercent.Value)}% benefit"
            : $"\u26A0 {warning.WarningType}{sourceTag}{legacyTag}";
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

    /// <summary>
    /// Formats a benefit percentage the way this warning header does: a bare number, whole once it
    /// reaches 100 and one decimal below that. <see cref="WaitRowText"/>'s wait-row benefit text uses
    /// its own whole-number format instead, matching PerformanceStudio's wait-row display.
    /// </summary>
    private static string FormatBenefitPercent(double pct) =>
        pct >= 100 ? $"{pct:N0}" : $"{pct:N1}";

    /// <summary>
    /// #4572: a warning's severity as a fixed colour, tuned for a dark background. The plan viewer paints warning text with
    /// theme brushes per severity (<c>PlanViewerControl.WarningSeverityBrush</c>, as PerformanceStudio's helper of that name
    /// does) and falls back to these values only when a host's theme lacks the key.
    /// </summary>
    public static string WarningSeverityColorHex(PlanWarningSeverity severity) =>
        severity == PlanWarningSeverity.Critical ? "#E57373"
        : severity == PlanWarningSeverity.Warning ? "#FFB347" : "#6BB5FF";

    /// <summary>
    /// #8447: what a statement's warnings are, everywhere the viewer counts or lists them (the root
    /// operator's badge, the root tooltip and the Statements grid): the statement-level warnings, then
    /// every operator's warnings in tree order, at every severity. A statement whose findings are all
    /// statement-level, such as a memory grant warning, still counts them.
    /// </summary>
    public static List<PlanWarning> StatementWarnings(PlanStatement statement)
    {
        var all = new List<PlanWarning>(statement.PlanWarnings);
        if (statement.RootNode != null)
            AddOperatorWarnings(statement.RootNode, all);
        return all;
    }

    private static void AddOperatorWarnings(PlanNode node, List<PlanWarning> warnings)
    {
        warnings.AddRange(node.Warnings);
        foreach (var child in node.Children)
            AddOperatorWarnings(child, warnings);
    }

    /// <summary>
    /// #4534: the text the viewer needs to turn a warning header into a link to the operator it came
    /// from. Null when <paramref name="originNodeIds"/> is empty — a finding with no known operator
    /// origin (for example one that happened before any row was read) gets no affordance, rather than
    /// a link that would go somewhere arbitrary. Mirrors PerformanceStudio's
    /// <c>AttachOriginNavigation</c>: one origin names it directly, several name the first as the
    /// navigation target and list the rest in the tooltip so the count is visible.
    /// </summary>
    public static (string Suffix, string Tooltip)? OriginNavigationText(IReadOnlyList<int> originNodeIds)
    {
        if (originNodeIds.Count == 0)
            return null;

        var tooltip = originNodeIds.Count == 1
            ? $"Go to operator (Node {originNodeIds[0]})"
            : $"Go to Node {originNodeIds[0]}, also from {string.Join(", ", originNodeIds.Skip(1).Select(id => "Node " + id))}";

        return ("  \u2192", tooltip);
    }
}
