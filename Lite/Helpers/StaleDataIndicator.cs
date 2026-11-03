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
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// The "Refreshed Ns ago" label and the archival warning that the Alerts History and Job History tabs both
/// show under their grids (#7228: the two tabs carried identical copies of this body).
/// </summary>
internal static class StaleDataIndicator
{
    /// <summary>The label text for data refreshed <paramref name="elapsed"/> ago.</summary>
    internal static string RefreshedText(TimeSpan elapsed) =>
        elapsed.TotalSeconds < 5
            ? "Refreshed just now"
            : elapsed.TotalMinutes < 1
                ? $"Refreshed {(int)elapsed.TotalSeconds}s ago"
                : $"Refreshed {(int)elapsed.TotalMinutes}m ago";

    /// <summary>Updates the refreshed label from <paramref name="lastRefreshedUtc"/> (left alone when null) and shows the archival warning while an archive run is in progress.</summary>
    internal static void Update(TextBlock lastRefreshedIndicator, TextBlock archivalWarning, DateTime? lastRefreshedUtc)
    {
        if (lastRefreshedUtc.HasValue)
        {
            lastRefreshedIndicator.Text = RefreshedText(DateTime.UtcNow - lastRefreshedUtc.Value);
        }

        if (ArchiveService.IsArchiving)
        {
            archivalWarning.Text = "⚠ Archival in progress";
            archivalWarning.Visibility = Visibility.Visible;
        }
        else
        {
            archivalWarning.Visibility = Visibility.Collapsed;
        }
    }
}
