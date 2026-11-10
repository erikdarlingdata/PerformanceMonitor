/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Ui;

/// <summary>The opening of a history window's summary line: how many samples the chart draws and the times they span.</summary>
public static class HistorySampleSummary
{
    /// <summary>
    /// "1 sample at {first}" for one sample, "{count} samples from {first} to {last}" otherwise. <paramref name="first"/> and
    /// <paramref name="last"/> are the first and last collection times, already formatted for the window's display zone.
    /// </summary>
    public static string Span(int count, string first, string last) =>
        count == 1 ? $"1 sample at {first}" : $"{count} samples from {first} to {last}";
}
