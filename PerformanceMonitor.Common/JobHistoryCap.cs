/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

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
    /// The count text beside the grid (#4966): "12 runs", with the cap label in brackets whenever the READ reached the cap,
    /// and empty when the grid shows nothing. <paramref name="shownCount"/> is what the grid shows now, after the column
    /// filters; <paramref name="readCount"/> is what the read returned, after the Status and Category filters (the Job History read applies
    /// them ahead of its row cap, #5703) and before the column filters. The cap
    /// belongs to the read, so a column filter that narrows the grid must not take the label away while the read is still
    /// cut at the cap: every writer of the count text, the load and the column-filter handler alike, builds it here.
    /// </summary>
    public static string CountText(int shownCount, int readCount, int cap) =>
        CountText(shownCount, readCount, cap, "run");

    /// <summary>
    /// <see cref="CountText(int, int, int)"/> for any list whose read is cut at a row cap, with the noun it counts in the
    /// singular ("run", "alert"); the count reads as a person says it ("1 alert", "500 alerts"). The Alert History tab reads
    /// its newest 500, and a bare "500 alerts" reads as the whole answer, so the text says "500 alerts (showing the newest
    /// 500)" whenever the read reached the cap, however a column filter narrows the grid afterwards.
    /// </summary>
    public static string CountText(int shownCount, int readCount, int cap, string noun)
    {
        if (shownCount <= 0)
        {
            return "";
        }

        var label = Label(readCount, cap);
        var count = global::PerformanceMonitor.Common.CountText.Plural(shownCount, noun);
        return label.Length > 0 ? $"{count} ({label})" : count;
    }

    /// <summary>
    /// #8133: the share of the All Servers read's cap each server gets, so one server that runs a job every minute cannot fill
    /// the page with its own newest steps while the quieter servers' day goes unshown. The cap divided by the servers the read
    /// can see, rounded up and never below one; the whole cap when no server count is known.
    /// </summary>
    public static int PerServerShare(int cap, int serverCount) =>
        serverCount <= 0 ? cap : Math.Max(1, (cap + serverCount - 1) / serverCount);
}
