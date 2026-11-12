/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Ui;

/// <summary>
/// What a picker chart (Wait Stats, Perfmon, Memory Clerks) says when its batched read fails. The chart is cleared before the
/// read, so a failure that says nothing leaves a blank chart that reads as "nothing selected"; the failure goes to the tab's status
/// line instead, named for its picker, as a slicer's does. A run a newer selection has superseded says nothing: the current run
/// answers for the chart.
/// </summary>
public static class PickerChartReadFailure
{
    /// <summary>The status-line text for a failed picker read, or null when a newer run has superseded this one.</summary>
    public static string? StatusText(string picker, Exception error, int runGeneration, int currentGeneration)
    {
        ArgumentNullException.ThrowIfNull(picker);
        ArgumentNullException.ThrowIfNull(error);

        return runGeneration == currentGeneration
            ? $"{picker} picker failed: {error.Message}"
            : null;
    }
}
