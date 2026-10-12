/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Serialization;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The newest-sample field the web alert pages and the MCP <c>get_alert_history</c> read publish (#5642), one
/// shape for both SKUs' <c>get_alert_history</c> (Lite uses this type too): the sample's
/// source label, its UTC instant (round-trip text) and the age in seconds, truncated to the millisecond. Built from the carrier the
/// engine persisted when the alert fired, never from the clock at read time. A row with no carrier publishes null, not an empty object.
/// </summary>
public sealed record AlertNewestSampleWire(
    [property: JsonPropertyName("source")] string Source,
    [property: JsonPropertyName("instant_utc")] string InstantUtc,
    [property: JsonPropertyName("age_seconds")] double AgeSeconds)
{
    /// <summary>
    /// The age in seconds, TRUNCATED to the millisecond, never rounded (#5642 review): <see cref="AlertDetailText"/> prints the age by
    /// truncating (59.6 s is "59s old", 3599.6 s is "59m 59s old"), so a consumer that floors this field gets the same
    /// unit the notification said at every boundary, where a whole-second rounding put 59.6 s at 60 and the web page
    /// said "1m 0s old" for the alert the email called "59s old". Milliseconds are kept so the floor is exact.
    /// </summary>
    private static double TruncatedSeconds(TimeSpan age) => Math.Floor(Math.Max(0d, age.TotalSeconds) * 1000d) / 1000d;

    /// <summary>The wire field for a persisted <c>context_json</c>, or null when the row carries no sample age.</summary>
    public static AlertNewestSampleWire? FromContextJson(string? contextJson)
    {
        var sample = AlertContextSerializer.TryReadNewestSample(contextJson);
        if (sample is null || AlertDetailText.SampleAgeLine(sample) is null)
        {
            return null;
        }

        return new AlertNewestSampleWire(
            sample.Source,
            sample.InstantUtc.ToString("o", CultureInfo.InvariantCulture),
            TruncatedSeconds(sample.Age));
    }
}
