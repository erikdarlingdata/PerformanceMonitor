/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common.KnownPatterns;

/// <summary>
/// #6334: what a writer sends to add or change a known-pattern entry, and the one place its rules are checked, so the web editor, the
/// Viewer and the store's functions refuse the same things in the same words. <see cref="Validate"/> returns the reason a write is
/// refused, or null; <see cref="Normalize"/> trims text, turns blank metric and zone into NULL (any / UTC) and fills the default window.
/// </summary>
public sealed record KnownPatternInput(
    int? ServerId,
    string? MetricName,
    TimeOnly? TimeOfDay,
    string? TimeZone,
    int? WindowMinutes,
    string? Note,
    DateTime? ExpiresAtUtc)
{
    /// <summary>The reason this input cannot be stored, or null when it can.</summary>
    public string? Validate()
    {
        var note = Note?.Trim();
        if (string.IsNullOrEmpty(note))
        {
            return "A known pattern needs a note: what the operator knows about it.";
        }

        if (note.Length > KnownPatternMatcher.MaxNoteLength)
        {
            return $"The note is {note.Length} characters; the most a known pattern holds is {KnownPatternMatcher.MaxNoteLength}.";
        }

        if (WindowMinutes is { } window && (window < 0 || window > KnownPatternMatcher.MaxWindowMinutes))
        {
            return $"window_minutes must be between 0 and {KnownPatternMatcher.MaxWindowMinutes}.";
        }

        var metric = MetricName?.Trim();
        if (metric is { Length: > KnownPatternMatcher.MaxMetricNameLength })
        {
            return $"The metric name is {metric.Length} characters; the most a known pattern holds is {KnownPatternMatcher.MaxMetricNameLength}. Use the alert metric name, or leave it empty for every metric.";
        }

        // Shape and length first, before any lookup: only an IANA-shaped id of at most 64 characters ever reaches the zone database
        // (security review of #6334). The store's CHECK uses the same limits.
        var zone = TimeZone?.Trim();
        if (!string.IsNullOrEmpty(zone) && !KnownPatternMatcher.IsZoneIdShape(zone))
        {
            return $"The time zone must be an IANA id of at most {KnownPatternMatcher.MaxTimeZoneLength} characters, such as America/New_York, or empty for UTC.";
        }

        if (!KnownPatternMatcher.IsKnownTimeZone(TimeZone))
        {
            return $"The time zone '{zone}' is not known on this machine. Use an IANA id such as America/New_York, or leave it empty for UTC.";
        }

        if (ServerId is < 1)
        {
            return "server_id must be a registered server's id, or empty for every server.";
        }

        return null;
    }

    /// <summary>The input with text trimmed, blank metric and zone as NULL and the default window filled. Call after <see cref="Validate"/>.</summary>
    public KnownPatternInput Normalize() => this with
    {
        MetricName = string.IsNullOrWhiteSpace(MetricName) ? null : MetricName.Trim(),
        TimeZone = string.IsNullOrWhiteSpace(TimeZone) ? null : TimeZone.Trim(),
        WindowMinutes = WindowMinutes ?? KnownPatternMatcher.DefaultWindowMinutes,
        Note = Note?.Trim(),
    };
}
