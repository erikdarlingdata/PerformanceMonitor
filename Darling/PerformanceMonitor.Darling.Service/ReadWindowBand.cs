/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The window band a read is recorded under in <c>collect.read_latency</c> (#5506). The window a read asks for is the main
/// driver of its cost, so a 1-hour read and a 7-day read of one route are kept apart. The band is the width of the window:
/// <c>1h</c> up to 1 hour, <c>24h</c> over 1 hour up to 24, <c>7d</c> over 24 hours up to 7 days, <c>7d+</c> longer, and
/// <c>none</c> for a read that asked for no window.
/// </summary>
internal static class ReadWindowBand
{
    internal const string None = "none";
    internal const string OneHour = "1h";
    internal const string OneDay = "24h";
    internal const string SevenDays = "7d";
    internal const string Longer = "7d+";

    /// <summary>The band of a window <paramref name="hours"/> wide. Null, zero, negative or not a number is <c>none</c>.
    /// Each band includes its upper boundary: exactly 1 hour is <c>1h</c>, exactly 24 is <c>24h</c>, exactly 168 is
    /// <c>7d</c>.</summary>
    internal static string Of(double? hours) => hours switch
    {
        null => None,
        double h when double.IsNaN(h) || h <= 0 => None,
        <= 1 => OneHour,
        <= 24 => OneDay,
        <= 24 * 7 => SevenDays,
        _ => Longer,
    };

    /// <summary>The band of a window from <paramref name="start"/> to <paramref name="end"/>; <c>none</c> when either is
    /// missing.</summary>
    internal static string Of(DateTime? start, DateTime? end) =>
        start is { } s && end is { } e ? Of((e - s).TotalHours) : None;

    /// <summary>The window in hours named by a read's arguments: <c>hours</c>, else <c>hours_back</c>, else
    /// <c>days_back</c> times 24. <paramref name="lookup"/> returns an argument's text, or null when it was not sent.</summary>
    internal static double? HoursOf(Func<string, string?> lookup)
    {
        if (Number(lookup("hours")) is { } hours) return hours;
        if (Number(lookup("hours_back")) is { } hoursBack) return hoursBack;
        return Number(lookup("days_back")) is { } days ? days * 24 : null;
    }

    /// <summary>The window of an MCP tool call: the arguments the caller sent, else the default the tool declares for that
    /// argument in its input schema (a call that leaves <c>hours_back</c> out reads the tool's default window, not none).</summary>
    internal static string OfToolCall(System.Collections.Generic.IDictionary<string, JsonElement>? arguments, JsonElement? inputSchema) =>
        Of(HoursOf(key =>
            arguments is not null && arguments.TryGetValue(key, out var sent) && sent.ValueKind is not (JsonValueKind.Null or JsonValueKind.Undefined)
                ? ElementText(sent)
                : SchemaDefault(inputSchema, key)));

    private static string? SchemaDefault(JsonElement? schema, string key) =>
        schema is { ValueKind: JsonValueKind.Object } s
        && s.TryGetProperty("properties", out var properties) && properties.ValueKind == JsonValueKind.Object
        && properties.TryGetProperty(key, out var property) && property.ValueKind == JsonValueKind.Object
        && property.TryGetProperty("default", out var value)
            ? ElementText(value)
            : null;

    private static string? ElementText(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.Number => value.GetRawText(),
        JsonValueKind.String => value.GetString(),
        _ => null,
    };

    private static double? Number(string? text) =>
        double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var value) ? value : null;
}
