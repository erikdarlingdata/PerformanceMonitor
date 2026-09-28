/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// One row of the properties panel: what it says, and what the filter box and the copy menu work
/// from. The viewer's panel is raw WPF controls with no bindings behind them, so once a row is in
/// the visual tree its text is the only thing left to read back, which is why this is recorded
/// separately while the panel builds rather than reconstructed later. Kept here (not in
/// <c>PerformanceMonitor.Ui</c>, which is net10.0-windows-only) so a unit test can pin the filter
/// match and the copy text without needing WPF.
/// </summary>
public sealed class PropertyRow
{
    public string Label { get; init; } = "";
    public string Value { get; init; } = "";
    public bool IsCode { get; init; }

    /// <summary>
    /// Plain text for rows that are not a label/value pair (the per-thread breakdown, a warning
    /// entry). Null for ordinary rows, which the copy menu renders from <see cref="Label"/> and
    /// <see cref="Value"/>.
    /// </summary>
    public string? BlockText { get; init; }

    /// <summary>What the filter box matches against, case-insensitively.</summary>
    public string SearchText { get; init; } = "";

    /// <summary>What "Copy value" hands back.</summary>
    public string CopyValue => BlockText ?? Value;

    /// <summary>What "Copy name and value" hands back.</summary>
    public string CopyLabelAndValue => BlockText
        ?? (string.IsNullOrEmpty(Label) ? Value : $"{Label}: {Value}");
}

/// <summary>One section of the properties panel: a title and the rows filed under it.</summary>
public sealed class PropertySection
{
    public string Title { get; init; } = "";
    public List<PropertyRow> Rows { get; } = new();
}

/// <summary>
/// Pure filter-match and copy-text logic for the properties panel, shared by the Darling Viewer
/// and Lite (both host <c>PerformanceMonitor.Ui.PlanViewerControl</c>). Mirrors
/// PerformanceStudio's Avalonia properties panel: same filter behaviour (case-insensitive on
/// label+value, a section whose title matches keeps every row, an empty section always hides),
/// same "Copy all properties" text layout, same width default/clamp.
/// </summary>
public static class PropertyRows
{
    /// <summary>The width the panel opens to when nothing has been remembered yet.</summary>
    public const double DefaultPropertiesWidth = 380;

    /// <summary>The narrowest the panel can be dragged to while open.</summary>
    public const double MinPropertiesWidth = 280;

    /// <summary>The widest the panel can be dragged to while open.</summary>
    public const double MaxPropertiesWidth = 800;

    /// <summary>Clamps a remembered/dragged width to the panel's open-state bounds.</summary>
    public static double ClampWidth(double width) =>
        Math.Clamp(width, MinPropertiesWidth, MaxPropertiesWidth);

    /// <summary>
    /// True when a section's own title matches the filter (case-insensitive), which is what lets
    /// typing a section name jump to that section rather than empty it: every row under a
    /// title-matching section stays visible regardless of its own text.
    /// </summary>
    public static bool SectionTitleMatches(string sectionTitle, string filter) =>
        filter.Length == 0 || sectionTitle.Contains(filter, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// True when a row should stay visible: either its section's title already matched (see
    /// <see cref="SectionTitleMatches"/>), or the row's own label+value text does.
    /// </summary>
    public static bool RowMatches(PropertyRow row, string filter, bool sectionTitleMatches) =>
        sectionTitleMatches || row.SearchText.Contains(filter, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// True when a section with the given visible-row count should itself stay visible. An
    /// empty section (no rows at all, filtered or not) is always hidden — a few sections build
    /// with zero rows depending on the node, and an expander with nothing inside it is noise
    /// whether or not anyone is filtering.
    /// </summary>
    public static bool SectionVisible(int visibleRowCount) => visibleRowCount > 0;

    /// <summary>
    /// The whole panel as plain text, for "Copy all properties": the operator header, then a
    /// line per section title with "  Label: Value" beneath it. Code values and block rows
    /// (the per-thread breakdown, a warning entry) go out verbatim on their own lines, so a
    /// predicate or a CREATE INDEX statement comes back pasteable rather than needing to be
    /// re-indented first. Sections with no rows are skipped.
    /// </summary>
    public static string BuildPropertiesText(
        string header, string subHeader, IReadOnlyList<PropertySection> sections)
    {
        var text = new StringBuilder();
        text.AppendLine(header);
        if (!string.IsNullOrEmpty(subHeader))
            text.AppendLine(subHeader);

        foreach (var section in sections)
        {
            if (section.Rows.Count == 0) continue;

            text.AppendLine();
            text.AppendLine(section.Title);
            foreach (var row in section.Rows)
            {
                if (row.BlockText != null)
                {
                    text.AppendLine(row.BlockText);
                }
                else if (row.IsCode)
                {
                    if (!string.IsNullOrEmpty(row.Label))
                        text.Append("  ").Append(row.Label).AppendLine(":");
                    text.AppendLine(row.Value);
                }
                else
                {
                    text.Append("  ").Append(row.Label).Append(": ").AppendLine(row.Value);
                }
            }
        }

        return text.ToString().TrimEnd();
    }
}
