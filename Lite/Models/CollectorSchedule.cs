/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Serialization;

namespace PerformanceMonitorLite.Models;

/// <summary>
/// Represents a collector's schedule configuration.
/// </summary>
public class CollectorSchedule
{
    /// <summary>
    /// The name of the collector (e.g., "wait_stats", "query_stats").
    /// </summary>
    [JsonPropertyName("name")]
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Whether this collector is enabled.
    /// </summary>
    [JsonPropertyName("enabled")]
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// How often this collector runs, in minutes.
    /// 0 means "on-load only" (not scheduled).
    /// </summary>
    [JsonPropertyName("frequency_minutes")]
    public int FrequencyMinutes { get; set; } = 15;

    /// <summary>
    /// How long to retain data for this collector, in days.
    /// </summary>
    [JsonPropertyName("retention_days")]
    public int RetentionDays { get; set; } = 30;

    /// <summary>
    /// Optional description of what this collector does.
    /// </summary>
    [JsonPropertyName("description")]
    public string? Description { get; set; }

    /// <summary>
    /// #4938: the time of day this collector should run, as 24-hour HH:MM on the monitored server's clock, such as
    /// "02:00". Missing means none. It applies only where the collector runs once a day or less often; on any other
    /// interval it is ignored, with a log warning. Written to the file only when set, and an older Lite skips it as a
    /// field it does not know.
    /// </summary>
    [JsonPropertyName("run_at")]
    public string? RunAt { get; set; }

    /// <summary>
    /// The last time this collector was run successfully.
    /// </summary>
    [JsonIgnore]
    public DateTime? LastRunTime { get; set; }

    /// <summary>
    /// The next scheduled run time for this collector.
    /// </summary>
    [JsonIgnore]
    public DateTime? NextRunTime { get; set; }

    /// <summary>
    /// Whether this collector is scheduled (vs on-load only).
    /// </summary>
    [JsonIgnore]
    public bool IsScheduled => FrequencyMinutes > 0;

    /// <summary>
    /// Whether this collector is due to run.
    /// </summary>
    [JsonIgnore]
    public bool IsDue
    {
        get
        {
            if (!Enabled || !IsScheduled)
            {
                return false;
            }

            // First run - never been executed
            if (!LastRunTime.HasValue)
            {
                return true;
            }

            // Check if enough time has elapsed
            var elapsed = DateTime.UtcNow - LastRunTime.Value;
            return elapsed.TotalMinutes >= FrequencyMinutes;
        }
    }

    /// <summary>
    /// Gets a display-friendly frequency string.
    /// </summary>
    [JsonIgnore]
    public string FrequencyDisplay
    {
        get
        {
            if (FrequencyMinutes == 0)
            {
                return "On-load only";
            }

            if (FrequencyMinutes == 1)
            {
                return "Every minute";
            }

            if (FrequencyMinutes < 60)
            {
                return $"Every {FrequencyMinutes} minutes";
            }

            if (FrequencyMinutes == 60)
            {
                return "Every hour";
            }

            var hours = FrequencyMinutes / 60;
            var mins = FrequencyMinutes % 60;

            if (mins == 0)
            {
                return hours == 1 ? "Every hour" : $"Every {hours} hours";
            }

            return $"Every {hours}h {mins}m";
        }
    }
}
