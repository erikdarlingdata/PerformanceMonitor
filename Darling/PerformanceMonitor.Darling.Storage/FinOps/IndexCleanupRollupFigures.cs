/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;

namespace PerformanceMonitor.Darling.Storage.FinOps;

/// <summary>The index-cleanup rollup's average-wait formulas, shared by the viewer grid and any other consumer.</summary>
public static class IndexCleanupRollupFigures
{
    /// <summary>Average wait in ms per wait: <paramref name="waitInMs"/> / <paramref name="waitCount"/>; 0 when there were no waits.</summary>
    public static decimal AverageWaitMs(long waitInMs, long waitCount) =>
        waitCount > 0 ? (decimal)waitInMs / waitCount : 0m;

    /// <summary>The display text of the average wait: two decimals when there were waits, otherwise "0".</summary>
    public static string AverageWaitMsText(long waitInMs, long waitCount) =>
        waitCount > 0 ? ((decimal)waitInMs / waitCount).ToString("N2", CultureInfo.CurrentCulture) : "0";
}
