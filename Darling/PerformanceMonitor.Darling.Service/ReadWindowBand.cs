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

    /// <summary>The window <c>get_collection_health</c> reads, fixed in code: the trailing 7 days (#5814).</summary>
    internal const int CollectionHealthHours = 24 * 7;

    /// <summary>
    /// The reads whose window is fixed in code rather than taken from an argument (#5814), by route: the MCP tool name and
    /// the <c>/api/read</c> name, which are the same. The band of such a read is known before it runs, so admission, the
    /// read cache and the read-latency row all use one key (today's only entry is a health read, which admission and the cache
    /// skip, so its band reaches only the read-latency row), and the tool notes the same window on its
    /// <see cref="ReadScope"/> for the slow-read row.
    /// </summary>
    internal static readonly System.Collections.Generic.IReadOnlyDictionary<string, int> FixedHours =
        new System.Collections.Generic.Dictionary<string, int>(StringComparer.Ordinal)
        {
            ["get_collection_health"] = CollectionHealthHours,
        };

    /// <summary><paramref name="band"/>, or the band of the window <paramref name="route"/> fixes in code when the
    /// arguments named none (#5814).</summary>
    internal static string OrFixed(string band, string? route) =>
        band == None && route is not null && FixedHours.TryGetValue(route, out var hours) ? Of(hours) : band;

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
        start.HasValue && end.HasValue ? Of((end.Value - start.Value).TotalHours) : None;

    /// <summary>The window in hours named by a read's arguments: <c>hours</c>, else <c>hours_back</c>, else
    /// <c>days_back</c> times 24. <paramref name="sent"/> returns the text of an argument the caller sent, or null when it
    /// was not sent. Only when the caller sent none of the three is <paramref name="declaredDefault"/> asked, in the same
    /// order, for the default the read itself declares: a call that leaves its window out reads that default window, not
    /// none, and a sent <c>hours_back</c> is never overridden by a declared default for <c>hours</c>.</summary>
    internal static double? HoursOf(Func<string, string?> sent, Func<string, string?>? declaredDefault = null)
    {
        var hours = WindowOf(sent);
        if (hours.HasValue || declaredDefault is null)
        {
            return hours;
        }

        return WindowOf(declaredDefault);
    }

    /// <summary>The window of an MCP tool call: the arguments the caller sent, else the default the tool method declares
    /// for that argument. <paramref name="declaredDefault"/> reads the method's parameter default, never the served input
    /// schema: the schema the host serves has every <c>default</c> keyword stripped for Gemini (#5506).</summary>
    internal static string OfToolCall(System.Collections.Generic.IDictionary<string, JsonElement>? arguments, Func<string, string?>? declaredDefault) =>
        Of(HoursOfToolCall(arguments, declaredDefault));

    /// <summary>The window in hours of an MCP tool call, before it is banded (<see cref="OfToolCall"/>).</summary>
    internal static double? HoursOfToolCall(System.Collections.Generic.IDictionary<string, JsonElement>? arguments, Func<string, string?>? declaredDefault)
    {
        return HoursOf(key =>
        {
            if (arguments is null || !arguments.TryGetValue(key, out var value))
            {
                return null;
            }

            return value.ValueKind == JsonValueKind.Null || value.ValueKind == JsonValueKind.Undefined ? null : ElementText(value);
        }, declaredDefault);
    }

    /// <summary>A declared default as argument text, read the way a sent number is: invariant culture. Null for no default
    /// or a null default.</summary>
    internal static string? DefaultText(object? value) =>
        value is IFormattable formattable ? formattable.ToString(null, CultureInfo.InvariantCulture) : value as string;

    private static double? WindowOf(Func<string, string?> lookup)
    {
        var hours = Number(lookup("hours"));
        if (hours.HasValue)
        {
            return hours;
        }

        var hoursBack = Number(lookup("hours_back"));
        if (hoursBack.HasValue)
        {
            return hoursBack;
        }

        var days = Number(lookup("days_back"));
        return days.HasValue ? days.Value * 24 : null;
    }

    private static string? ElementText(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.Number => value.GetRawText(),
        JsonValueKind.String => value.GetString(),
        _ => null,
    };

    private static double? Number(string? text) =>
        double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var value) ? value : null;
}
