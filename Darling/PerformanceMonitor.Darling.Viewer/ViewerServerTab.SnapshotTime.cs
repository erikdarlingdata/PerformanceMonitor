/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;

namespace PerformanceMonitor.Darling.Viewer;

public partial class ViewerServerTab
{
    /// <summary>
    /// Sets the muted "Snapshot at &lt;time&gt;" label above a newest-snapshot grid (#4966): the surface shows the collection it
    /// rendered instead of a data-start note. An empty snapshot (<paramref name="collectionUtc"/> null) hides the label and leaves the
    /// grid's own empty text to speak.
    /// </summary>
    internal static void ShowSnapshotTime(TextBlock label, DateTime? collectionUtc)
    {
        if (collectionUtc is not DateTime utc)
        {
            label.Text = "";
            label.Visibility = Visibility.Collapsed;
            return;
        }

        label.Text = ViewerTimeHelper.FormatSnapshotLabel(utc);
        label.Visibility = Visibility.Visible;
    }
}
