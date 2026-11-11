/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One database of the Database Protection tab: the three facts as text, in the web page's words.</summary>
public sealed record ViewerDatabaseProtectionRow(
    string Database, string State, string FullBackup, string LogBackup, string CheckDb, string Platform, string NotJudgedBecause, bool Overdue);

/// <summary>
/// The words of the Database Protection tab (#6873), as functions of <see cref="ViewerDatabaseProtectionRead"/>. They match the Health Checks
/// panels on the web: the same age text, the state words with their underscores read as spaces, the platform label as the reader gives it,
/// and the same order, overdue databases first. The judgement is <see cref="DatabaseProtectionBands"/>'s, never made here.
/// </summary>
public static class ViewerDatabaseProtectionText
{
    /// <summary>What the list says when the newest capture holds no database.</summary>
    public const string NoDatabases = "No databases in the newest capture.";

    /// <summary>What the tab says when the read holds no capture and gave no sentence of its own.</summary>
    public const string NoCapture = "No database protection capture yet.";

    /// <summary>#6907: the grid note when the read holds no capture. The status line already gives the reason, so the grid does not repeat it.</summary>
    public const string NoData = "No data for this server.";

    private const string Dash = "-";

    /// <summary>
    /// The status line: the summary sentence and when the newest capture was taken. The capture time is stored naive UTC and is stamped
    /// to the second with its zone's label on the same clock as the tab's "Last refreshed" stamp (#8576), through
    /// <see cref="ViewerTimeHelper.FormatFleetWide(DateTime, string, TimeDisplayMode)"/>: this machine's zone in Local mode, UTC in
    /// every other mode, so the Server Time setting reads UTC here as it does beside it. It used to read the server's own offset
    /// ("UTC-05:00") next to a refresh stamp in UTC, so one tab showed two clocks.
    /// </summary>
    public static string StatusLine(ViewerDatabaseProtectionRead read)
    {
        ArgumentNullException.ThrowIfNull(read);
        if (read.Summary is null || read.CapturedAt is not { } captured)
        {
            return read.Message ?? NoCapture;
        }

        var when = ViewerTimeHelper.FormatFleetWide(captured, "yyyy-MM-dd HH:mm:ss", ViewerTimeHelper.CurrentDisplayMode);
        return $"{read.Summary.Text}   Captured: {when}";
    }

    /// <summary>The sentence under the list when it has no row: the read's sentence, or the empty-capture sentence.</summary>
    public static string EmptyText(ViewerDatabaseProtectionRead read)
    {
        ArgumentNullException.ThrowIfNull(read);
        return read.Summary is null ? NoData : NoDatabases;
    }

    /// <summary>One fact as text: the age, the state word when it is more than ok or overdue, and "overdue" when the banding says so.</summary>
    public static string FactText(DatabaseProtectionFactBand fact)
    {
        ArgumentNullException.ThrowIfNull(fact);
        var state = fact.State is DatabaseProtectionBands.Ok or DatabaseProtectionBands.Overdue ? "" : fact.State.Replace('_', ' ');
        var overdue = fact.Overdue ? " - overdue" : "";
        return fact.Age is { } age
            ? DatabaseProtectionBands.Describe(age) + (state.Length > 0 ? " (" + state + ")" : "") + overdue
            : (state.Length > 0 ? state : Dash) + overdue;
    }

    /// <summary>The databases of the newest capture, overdue ones first (the most overdue facts, then by name).</summary>
    public static IReadOnlyList<ViewerDatabaseProtectionRow> Rows(ViewerDatabaseProtectionRead read)
    {
        ArgumentNullException.ThrowIfNull(read);
        static int Count(DatabaseProtectionBand b) => (b.CheckDb.Overdue ? 1 : 0) + (b.FullBackup.Overdue ? 1 : 0) + (b.LogBackup.Overdue ? 1 : 0);
        return read.Databases
            .OrderByDescending(d => Count(d.Band))
            .ThenBy(d => d.Fact.DatabaseName, StringComparer.OrdinalIgnoreCase)
            .Select(d => new ViewerDatabaseProtectionRow(
                d.Fact.DatabaseName,
                d.Fact.StateDesc ?? "",
                FactText(d.Band.FullBackup),
                FactText(d.Band.LogBackup),
                FactText(d.Band.CheckDb),
                PlatformLabel(d.Fact.Platform),
                d.Band.ExcludedReason ?? "",
                d.Band.AnyOverdue))
            .ToList();
    }

    /// <summary>
    /// Whether the Platform column has anything to show (#7416): a boxed server reads "native" on every row, which draws as nothing, so the grid drops the
    /// column when no row names a platform, as the web page does.
    /// </summary>
    public static bool ShowsPlatformColumn(IEnumerable<ViewerDatabaseProtectionRow> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);
        return rows.Any(r => !string.IsNullOrEmpty(r.Platform));
    }

    /// <summary>The platform as the reader gives it, read with spaces: a plain SQL Server (native) shows nothing.</summary>
    public static string PlatformLabel(string? platform) =>
        string.IsNullOrEmpty(platform) || platform == "native" ? "" : platform.Replace('_', ' ');
}
