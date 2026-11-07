/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// #8081: the one sentence both products' <c>analyze_server</c> print when the findings did not all fit
/// <see cref="McpResponseBudget.DefaultBytes"/>. The findings ride severity-first, so the cut keeps the most
/// severe and leaves the tail off; the sentence says how many fired, how many ride, and where the rest are
/// read. Shared so the two tools cannot drift on the wording.
/// </summary>
public static class AnalysisFindingsBudget
{
    /// <summary>Null when every finding rides (<paramref name="returned"/> equals <paramref name="findingCount"/>).</summary>
    public static string? TruncationNote(int findingCount, int returned) =>
        returned < findingCount
            ? string.Create(CultureInfo.InvariantCulture,
                $"TRUNCATED: {findingCount} findings fired; only the {returned} most severe fit the {McpResponseBudget.DefaultBytes:#,0}-byte response budget. finding_count counts every finding, and an unanchored run persisted them all. Read the rest with get_analysis_findings (limit pages the persisted findings, still-firing first then by severity; start at 5), or narrow hours_back.")
            : null;
}
