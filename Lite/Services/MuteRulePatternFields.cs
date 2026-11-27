/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Notifications;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Which pattern fields a mute rule can use for a given metric (#9639). One list, read by the Mute Rule window (which shows only
/// those boxes) and by the MCP mute-rule tools (which refuse the others), so the two cannot disagree about what a rule keeps. A
/// pattern for a dimension the metric's alert never carries would match nothing, and the window drops it on Save.
/// A null or "(any)" metric allows every field.
/// </summary>
internal static class MuteRulePatternFields
{
    private static bool Any(string? metric) => metric is null or "(any)";

    /* #1839's "Blocking Wait Time" carries the same blocked-process context as "Blocking Detected",
       so it offers the same database/query-text pattern fields. Database State carries the database whose state changed. */
    internal static bool AllowsDatabase(string? metric) =>
        Any(metric) || metric is "Blocking Detected" or "Blocking Wait Time" or "Deadlocks Detected" or "Long-Running Query" or "Database State" or AlertFamily.ClientTimeoutsMetric;

    internal static bool AllowsWaitType(string? metric) => Any(metric) || metric is "Poison Wait" or "Long-Running Query";

    internal static bool AllowsQueryText(string? metric) => Any(metric) || metric is "Blocking Detected" or "Blocking Wait Time" or "Long-Running Query";

    internal static bool AllowsJobName(string? metric) => Any(metric) || metric is "Long-Running Job" or "Failed Agent Job";

    /* #5640: Client Timeouts offers Database AND Client Application, the two dimensions its alert carries. */
    internal static bool AllowsClientApp(string? metric) => Any(metric) || metric is AlertFamily.ClientTimeoutsMetric;
}
