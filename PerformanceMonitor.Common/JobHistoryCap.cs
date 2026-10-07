/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// The Job History tab's row-cap label (#4478). Pure — no WPF — so it, and the pin on it, run on macOS: a test
/// class that references the WPF <c>JobHistoryTab</c> UserControl fails there on <c>PresentationFramework</c>
/// even when the method under test itself touches no WPF type, because loading the class that hosts the method
/// still loads its base type. Moved here so this one rule keeps its coverage on every dev machine, not only in
/// CI.
/// </summary>
public static class JobHistoryCap
{
    /// <summary>The label's text: stated ONLY when the read actually reached the cap, so a reader who sees
    /// fewer rows than the cap never wonders whether more were silently dropped, and a reader who sees exactly
    /// the cap knows there could be more outside the window.</summary>
    public static string Label(int rowCount, int cap) =>
        rowCount >= cap ? $"showing the newest {cap:N0}" : "";

    /// <summary>
    /// The count text beside the grid (#4966): "N run(s)", with the cap label in brackets whenever the READ reached the cap,
    /// and empty when the grid shows nothing. <paramref name="shownCount"/> is what the grid shows now, after the Status,
    /// Category and column filters; <paramref name="readCount"/> is what the read returned before any of them. The cap
    /// belongs to the read, so a column filter that narrows the grid must not take the label away while the read is still
    /// cut at the cap: every writer of the count text, the load and the column-filter handler alike, builds it here.
    /// </summary>
    public static string CountText(int shownCount, int readCount, int cap)
    {
        if (shownCount <= 0)
        {
            return "";
        }

        var label = Label(readCount, cap);
        return label.Length > 0 ? $"{shownCount} run(s) ({label})" : $"{shownCount} run(s)";
    }
}
