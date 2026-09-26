/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

internal static partial class AlertNotebookEndpoint
{
    /// <summary>Analysis-finding alert (<c>Analysis: &lt;category&gt; [&lt;hash8&gt;]</c>) template version
    /// (#4223). Bumped only if this template's SHAPE changes.</summary>
    internal const int AnalysisFindingTemplateVersion = 1;

    /// <summary>Analysis-finding alert: header, status, a summary of the pre-fetched
    /// <see cref="AnalysisFinding"/> (<see cref="PrefetchAnalysisFindingAsync"/> already matched the metric's
    /// hash suffix against the server's recent findings), plus a <c>get_analysis_findings</c> evidence read
    /// bound to the same server/window. A finding that has aged out of the window, been superseded, or whose
    /// hash could not be parsed (<see cref="AuthoredContext.FindingMissing"/>) degrades to a note, per #2710's
    /// degrade rule: never an exception, and never a silent empty notebook.</summary>
    private static JsonArray BuildAnalysisFindingCells(
        string? metric, string? serverName, string? asOf, DateTime windowStart, DateTime windowEnd,
        AlertIncident? incident, DarlingAlertReader.AlertHistoryReadRow? row, string status, AuthoredContext context)
    {
        var cells = new JsonArray
        {
            HeaderCell(metric, serverName, incident, row),
            StatusCell(status),
        };

        if (context.FindingMissing || context.Finding is null)
        {
            cells.Add(AnalysisFindingMissingNoteCell());
            return cells;
        }

        cells.Add(AnalysisFindingSummaryCell(context.Finding));
        cells.Add(AuthoredReadCell(
            "get_analysis_findings", "Finding evidence", serverName, asOf,
            ("hours", "24"), ("include_drilldown", "true"), ("full_text", "true")));
        return cells;
    }

    /// <summary>The finding's own fields (spec: "category, severity, story path, first/last seen,
    /// occurrences, whichever fields exist"), built entirely from the pre-fetched
    /// <see cref="AnalysisFinding"/> — no store read in the builder. <c>analysis_findings</c> carries the
    /// analysis-time timestamp and the finding's own time range, not a separately-tracked first/last-seen
    /// pair or an occurrence count (those are computed by <see cref="DarlingMcpTools.GetAnalysisFindings"/>'s
    /// grouping over the read window, which the evidence read cell below covers), so this cell states what
    /// the fetched row itself carries.</summary>
    private static JsonObject AnalysisFindingSummaryCell(AnalysisFinding finding) => new()
    {
        ["type"] = "markdown",
        ["text"] = AnalysisFindingSummaryText(finding),
    };

    private static string AnalysisFindingSummaryText(AnalysisFinding finding)
    {
        var lines = new System.Collections.Generic.List<string>
        {
            $"**Category:** {finding.Category}",
            $"**Severity:** {finding.Severity.ToString("F1", CultureInfo.InvariantCulture)}"
                + $" (confidence {finding.Confidence.ToString("F2", CultureInfo.InvariantCulture)})",
            $"**Story:** {finding.StoryPath}",
        };

        if (!string.IsNullOrEmpty(finding.DatabaseName))
        {
            lines.Add($"**Database:** {finding.DatabaseName}");
        }

        if (finding.TimeRangeStart is DateTime start && finding.TimeRangeEnd is DateTime end)
        {
            lines.Add(
                $"**Window:** {start.ToString("o", CultureInfo.InvariantCulture)} \u2013 "
                    + $"{end.ToString("o", CultureInfo.InvariantCulture)}");
        }

        lines.Add($"**Last analyzed:** {finding.AnalysisTime.ToString("o", CultureInfo.InvariantCulture)}");
        lines.Add($"**Facts in chain:** {finding.FactCount.ToString(CultureInfo.InvariantCulture)}");

        if (!string.IsNullOrEmpty(finding.StoryText))
        {
            lines.Add(string.Empty);
            lines.Add(finding.StoryText);
        }

        return string.Join("\n\n", lines);
    }

    /// <summary>The note a finding that could not be matched gets — never an error (#2710's degrade rule):
    /// the alert already fired and is real history; the finding it named may simply have aged out of
    /// retention, been muted, or been superseded by a later occurrence of the same chain since this alert
    /// fired.</summary>
    private static JsonObject AnalysisFindingMissingNoteCell() => new()
    {
        ["type"] = "markdown",
        ["text"] = "This analysis finding is no longer in the store; it may have aged out or been superseded "
            + "since this alert fired.",
    };
}
