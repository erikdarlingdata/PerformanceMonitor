/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// The elapsed time an Active Queries row shows. The collector stores it as <c>dd hh:mm:ss.fff</c>, a day count ahead of the clock, so a
/// request a few minutes old reads <c>00 00:05:12.345</c>. A grid shows the clock alone until the request is a day old, when the day count
/// carries meaning. The stored text, and what the MCP tools return, keep the day count.
/// </summary>
public static class ElapsedTimeText
{
    /// <summary>
    /// <paramref name="formatted"/> without a leading <c>00 </c> day count; any other text, a day count of one or more included, as it is.
    /// Null reads as empty.
    /// </summary>
    public static string WithoutZeroDays(string? formatted)
    {
        if (string.IsNullOrEmpty(formatted))
            return "";
        return formatted.Length > 3 && formatted.StartsWith("00 ", System.StringComparison.Ordinal) ? formatted[3..] : formatted;
    }
}
