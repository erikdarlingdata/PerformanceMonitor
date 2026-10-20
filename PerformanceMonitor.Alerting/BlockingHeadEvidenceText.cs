/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The text the Blocking grids show for the head blocker's open transaction count and transaction-log bytes (#5632 part 4), shared so
/// the Darling viewer and Lite spell them one way. A NULL (a row captured before the columns existed, or a session with no
/// transaction row) is an empty cell, never 0: 0 bytes is a real answer (a transaction that has written no log), and 0 transactions
/// is a real count. The grids sort on the number, not on this text.
/// </summary>
public static class BlockingHeadEvidenceText
{
    /// <summary>Human units for a byte total: B below 1 KB, then KB, MB and GB (1024-based) with one decimal. Empty for null.</summary>
    public static string FormatLogBytes(long? bytes)
    {
        if (bytes is not { } value)
        {
            return "";
        }

        const double Kb = 1024.0;
        const double Mb = Kb * 1024;
        const double Gb = Mb * 1024;
        var inv = CultureInfo.InvariantCulture;
        return value switch
        {
            < 1024 => value.ToString("N0", inv) + " B",
            < 1024L * 1024 => (value / Kb).ToString("F1", inv) + " KB",
            < 1024L * 1024 * 1024 => (value / Mb).ToString("F1", inv) + " MB",
            _ => (value / Gb).ToString("F1", inv) + " GB",
        };
    }

    /// <summary>The open transaction count as text, empty for null.</summary>
    public static string FormatCount(int? count) =>
        count is { } value ? value.ToString("N0", CultureInfo.InvariantCulture) : "";
}
