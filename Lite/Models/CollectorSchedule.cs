/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
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
    /// field it does not know. A value in the file that is not a JSON string, such as <c>120</c>, reads as its own text
    /// (<see cref="RunAtJsonConverter"/>), so the run-time check refuses it like "25:00" instead of the whole file failing to load.
    /// </summary>
    [JsonPropertyName("run_at")]
    [JsonConverter(typeof(RunAtJsonConverter))]
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

/// <summary>
/// #4938: reads a collector's <c>run_at</c> so that a value of the wrong JSON type costs only that collector's run
/// time. A string reads as it always did. Any other token (a number, true or false, an object, an array) reads as its
/// own JSON text, such as "120", which the run-time check refuses like "25:00": it logs a warning that names the
/// collector, and the collector runs on its interval. Without this, a hand edit such as <c>"run_at": 120</c> threw
/// while the file was read, so Lite restored the backup or the defaults and every other edit in the file was lost.
/// The value is written back as text, so the next load meets an ordinary string.
/// </summary>
internal sealed class RunAtJsonConverter : JsonConverter<string?>
{
    public override string? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.String)
        {
            return reader.GetString();
        }

        /* ParseValue takes the whole value, nested or not, and leaves the reader on its last token. */
        using var document = JsonDocument.ParseValue(ref reader);
        return document.RootElement.GetRawText();
    }

    public override void Write(Utf8JsonWriter writer, string? value, JsonSerializerOptions options)
    {
        if (value is null)
        {
            writer.WriteNullValue();
        }
        else
        {
            writer.WriteStringValue(value);
        }
    }
}
