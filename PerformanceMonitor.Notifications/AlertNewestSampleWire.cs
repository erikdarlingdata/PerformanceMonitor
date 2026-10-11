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
/// shape for both, and the same names and rounding Lite's <c>get_alert_history</c> publishes: the sample's
/// source label, its UTC instant (round-trip text) and the age in whole seconds. Built from the carrier the
/// engine persisted when the alert fired, never from the clock at read time. A row with no carrier publishes null, not an empty object.
/// </summary>
public sealed record AlertNewestSampleWire(
    [property: JsonPropertyName("source")] string Source,
    [property: JsonPropertyName("instant_utc")] string InstantUtc,
    [property: JsonPropertyName("age_seconds")] double AgeSeconds)
{
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
            Math.Round(sample.Age.TotalSeconds, 0));
    }
}
