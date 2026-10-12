/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The one rule for "has this server been monitored long enough for recommendations to mean anything".
/// Lite's and the Darling service's analysis passes, and Lite's Recommendations tab, all ask it.
/// </summary>
public static class AnalysisHistoryGate
{
    /// <summary>Minimum hours of collected history before analysis runs.</summary>
    public const double MinimumDataHours = 24;

    /// <summary>True when <paramref name="dataSpanHours"/> clears <paramref name="minimumHours"/>.</summary>
    public static bool HasEnoughHistory(double dataSpanHours, double minimumHours = MinimumDataHours) =>
        dataSpanHours >= minimumHours;

    /// <summary>The user-facing sentence for a server that has not cleared the gate.</summary>
    public static string InsufficientDataMessage(double dataSpanHours, double minimumHours = MinimumDataHours)
    {
        var needed = minimumHours >= 24
            ? $"{minimumHours / 24:F1} days"
            : $"{minimumHours:F0} hours";
        var have = dataSpanHours >= 24
            ? $"{dataSpanHours / 24:F1} days"
            : $"{dataSpanHours:F1} hours";

        return $"Not enough data for reliable analysis. Need {needed} of collected data, " +
               $"have {have}. Keep the collector running and try again later.";
    }
}
