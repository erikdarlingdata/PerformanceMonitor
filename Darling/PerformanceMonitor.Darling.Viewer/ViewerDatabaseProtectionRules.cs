/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The three Database Protection overdue ages (#7611) as the Settings window checks them. The bounds are the override registry's
/// (<see cref="DarlingThresholdKeys"/>), the same ones <c>update_alert_settings</c> and the Threshold Overrides window enforce, so
/// the window holds no second copy of them. A value outside its bounds is refused, never clamped.
/// </summary>
public static class ViewerDatabaseProtectionRules
{
    public const string BackupHoursKey = DatabaseProtectionThresholdSettings.BackupHoursKey;
    public const string LogBackupMinutesKey = DatabaseProtectionThresholdSettings.LogBackupMinutesKey;
    public const string CheckDbHoursKey = DatabaseProtectionThresholdSettings.CheckDbHoursKey;

    /// <summary>True for a whole number inside the key's bounds.</summary>
    public static bool TryParse(string? text, string wire, out int value)
    {
        value = 0;
        if (!DarlingThresholdKeys.TryGet(wire, out var entry)
            || !int.TryParse(text?.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            || parsed < entry.Min || parsed > entry.Max)
        {
            return false;
        }

        value = parsed;
        return true;
    }

    /// <summary>The sentence shown beside a refused value, with the unit written out.</summary>
    public static string Refusal(string label, string wire, string unit)
    {
        DarlingThresholdKeys.TryGet(wire, out var entry);
        return $"{label} must be a whole number of {unit} between {entry.Min} and {entry.Max}.";
    }
}
