/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// The one definition of "critically low" free space on a volume (#1136, #2107, #7583): at or below
/// <see cref="FreePercent"/> percent free, or at or below <see cref="FreeGb"/> GB free, on EITHER dimension.
/// The low disk alert (<c>LowDiskAlertGate</c>) grades its Critical tier with it, and Analysis
/// (<c>FactScorer</c>, the DISK_SPACE fact) lifts the finding to Critical with the same cut, so the alert and
/// the finding agree about one disk. It lives here because Analysis cannot reference Notifications (the
/// reference runs the other way) and a second copy of the numbers would drift.
/// </summary>
public static class DiskCriticalCut
{
    /// <summary>The shipped default for <c>critical_free_percent</c>.</summary>
    public const double FreePercent = 3.0;

    /// <summary>The shipped default for <c>critical_free_gb</c>.</summary>
    public const double FreeGb = 2.0;

    /// <summary>True when free space is at or below either floor (the alert's OR rule).</summary>
    public static bool IsCriticallyLow(double freePercent, double freeGb, double criticalFreePercent, double criticalFreeGb) =>
        freePercent <= criticalFreePercent || freeGb <= criticalFreeGb;
}
