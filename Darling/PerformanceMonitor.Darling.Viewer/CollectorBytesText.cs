/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Windows;
using System.Windows.Controls;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #6629: the text for a collector's network bytes in the Collection Health grid and the collection history window. A byte count reads in
/// 1024-based units ("1.5 MB", as the web's Received and Sent columns do); a missing one reads "not measured", never 0 B, because 0 is a
/// real answer and a run with no SQL Server connection has none.
/// </summary>
public static class CollectorBytesText
{
    public const string NotMeasured = "not measured";

    /// <summary>Shows or hides a grid's byte columns (the ones whose sort path starts with "Bytes"). A store below V219 has no byte figures, so
    /// the columns are hidden there instead of reading "not measured" on every row.</summary>
    public static void ShowColumns(DataGrid grid, bool visible)
    {
        foreach (var column in grid.Columns)
        {
            if (column.SortMemberPath.StartsWith("Bytes", StringComparison.Ordinal))
            {
                column.Visibility = visible ? Visibility.Visible : Visibility.Collapsed;
            }
        }
    }

    public static string Format(long? bytes)
    {
        if (bytes is not long value)
        {
            return NotMeasured;
        }

        var abs = Math.Abs((double)value);
        if (abs < 1024)
        {
            return value.ToString("N0", CultureInfo.CurrentCulture) + " B";
        }

        if (abs < 1024 * 1024)
        {
            return (value / 1024.0).ToString("N1", CultureInfo.CurrentCulture) + " KB";
        }

        if (abs < 1024.0 * 1024 * 1024)
        {
            return (value / (1024.0 * 1024)).ToString("N1", CultureInfo.CurrentCulture) + " MB";
        }

        return (value / (1024.0 * 1024 * 1024)).ToString("N1", CultureInfo.CurrentCulture) + " GB";
    }
}
