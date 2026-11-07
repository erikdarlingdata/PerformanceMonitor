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
/// read. Where depends on the run: an unanchored run persisted every finding first, so the store has the
/// rest and get_analysis_findings reads them; an as_of run persisted nothing (and get_analysis_findings
/// answers a different window), so the only way to the cut findings is a narrower hours_back on the same
/// anchor. Shared so the two tools cannot drift on the wording.
/// </summary>
public static class AnalysisFindingsBudget
{
    /// <summary>Null when every finding rides (<paramref name="returned"/> equals <paramref name="findingCount"/>).
    /// <paramref name="persisted"/> is the reply's own <c>persisted</c> flag: true on an unanchored run.</summary>
    public static string? TruncationNote(int findingCount, int returned, bool persisted)
    {
        if (returned >= findingCount)
        {
            return null;
        }

        var cut = string.Create(CultureInfo.InvariantCulture,
            $"TRUNCATED: {findingCount} findings fired; only the {returned} most severe fit the {McpResponseBudget.DefaultBytes:#,0}-byte response budget. finding_count counts every finding.");
        return persisted
            ? cut + " This run persisted them all: read the rest with get_analysis_findings (limit pages the persisted findings, still-firing first then by severity; start at 5), or narrow hours_back."
            : cut + " This run was anchored with as_of, so its findings were not persisted and the ones left off are not in the store: narrow hours_back (as_of still ends the window) to see them, or run without as_of to analyze and persist the present.";
    }
}
