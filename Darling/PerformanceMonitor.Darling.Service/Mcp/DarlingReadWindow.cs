/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The window validators every Darling read calls (#5814): each runs the shared <see cref="McpHelpers"/> check and, when it
/// passes, notes the window the read will run with on the <see cref="ReadScope"/>. The window is the one after the
/// arguments resolved, so a call that left <c>hours_back</c> out notes the default it reads, and the slow-read row records
/// that window rather than none. A refused window notes nothing. Outside a read scope the note is ignored. The checks and
/// their messages are the shared ones, word for word: these only add the note.
/// </summary>
internal static class DarlingReadWindow
{
    /// <summary><see cref="McpHelpers.ValidateWindow(int, string, out DateTime)"/>, then the note.</summary>
    internal static string? Validate(int hoursBack, string? asOf, out DateTime endUtc) =>
        Noted(McpHelpers.ValidateWindow(hoursBack, asOf, out endUtc), hoursBack, endUtc);

    /// <summary><see cref="McpHelpers.ValidateWindow(int, string, int, out DateTime)"/>, then the note.</summary>
    internal static string? Validate(int hoursBack, string? asOf, int maxHours, out DateTime endUtc) =>
        Noted(McpHelpers.ValidateWindow(hoursBack, asOf, maxHours, out endUtc), hoursBack, endUtc);

    /// <summary><see cref="McpHelpers.ValidateUncappedWindow"/>, then the note.</summary>
    internal static string? ValidateUncapped(int hoursBack, string? asOf, out DateTime endUtc) =>
        Noted(McpHelpers.ValidateUncappedWindow(hoursBack, asOf, out endUtc), hoursBack, endUtc);

    /// <summary><see cref="McpHelpers.ValidateHoursBack(int)"/> for a read with no <c>as_of</c>: the window ends now.</summary>
    internal static string? ValidateHoursBack(int hoursBack) =>
        Noted(McpHelpers.ValidateHoursBack(hoursBack), hoursBack, DateTime.UtcNow);

    /// <summary><see cref="McpHelpers.ValidateHoursBack(int, int)"/> for a read with no <c>as_of</c>: the window ends now.</summary>
    internal static string? ValidateHoursBack(int hoursBack, int maxHours) =>
        Noted(McpHelpers.ValidateHoursBack(hoursBack, maxHours), hoursBack, DateTime.UtcNow);

    /// <summary><see cref="McpHelpers.ValidateDaysBack"/> for a read whose days end now.</summary>
    internal static string? ValidateDaysBack(int daysBack, int maxDaysBack) =>
        Noted(McpHelpers.ValidateDaysBack(daysBack, maxDaysBack), daysBack * 24.0, DateTime.UtcNow);

    private static string? Noted(string? error, double hours, DateTime endUtc)
    {
        if (error is null)
        {
            ReadScope.NoteWindow(hours, endUtc);
        }

        return error;
    }
}
