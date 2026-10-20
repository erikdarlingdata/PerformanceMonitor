/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The bounds a collector's <c>frequency_minutes</c> and <c>retention_days</c> must meet before they are written to
/// <c>config.config_collector_schedules</c> (#6227): the V17 CHECK constraints (frequency at least 0, retention at least 1), the upper limits (a year of minutes, ten years of days) and the
/// delta-family cadence cap (#3532). One definition for every writer, so the Viewer's schedule editor and the service's command
/// plane (the MCP tools) refuse the same values in the same words. Pure. Each method returns the refusal text, or null when the
/// value is fine.
/// </summary>
public static class CollectorScheduleBounds
{
    /// <summary>The refusal for a frequency the store or the collection pipeline cannot honor, or null. 0 means "collect once when
    /// the server loads".</summary>
    public static string? FrequencyError(string collectorName, int frequencyMinutes)
    {
        if (frequencyMinutes < 0)
        {
            return $"'{collectorName}': frequency (minutes) can't be negative. Use 0 to collect once on server load.";
        }

        if (frequencyMinutes > CollectorScheduleDefaults.MaxFrequencyMinutes)
        {
            return $"'{collectorName}': frequency (minutes) can't be more than {CollectorScheduleDefaults.MaxFrequencyMinutes} (one year).";
        }

        return CollectorDeltaCalculator.DeltaFrequencyError(collectorName, frequencyMinutes);
    }

    /// <summary>The refusal for a retention the purge cannot honor (0 would invert the purge cutoff and wipe the table), or null.</summary>
    public static string? RetentionError(string collectorName, int retentionDays) =>
        retentionDays < 1
            ? $"'{collectorName}': retention (days) must be at least 1."
            : retentionDays > CollectorScheduleDefaults.MaxRetentionDays
                ? $"'{collectorName}': retention (days) can't be more than {CollectorScheduleDefaults.MaxRetentionDays} (ten years)."
                : null;
}
