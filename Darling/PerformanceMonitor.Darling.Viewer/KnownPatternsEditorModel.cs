/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using PerformanceMonitor.Common.KnownPatterns;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The known-pattern editor's form logic (#6334), kept apart from the WPF dialog so a refused save is testable: the dialog's text
/// boxes in, a <see cref="KnownPatternInput"/> or the refusal text out. The refusal for note, window, zone and server is the shared
/// <see cref="KnownPatternInput.Validate"/> text, so the editor says what the store functions' other callers say.
/// </summary>
public static class KnownPatternsEditorModel
{
    /// <summary>The alert metrics the mute rule editor offers, in its order. The metric box also takes typed text, so a metric
    /// that is not listed (a custom alert rule's name) can still be given.</summary>
    public static readonly IReadOnlyList<string> MetricChoices = new[]
    {
        "Blocking Detected",
        "Blocking Wait Time",
        "Deadlocks Detected",
        "High CPU",
        "Poison Wait",
        "Runnable Queue",
        AlertFamily.ClientTimeoutsMetric,
        "Long-Running Query",
        "tempdb Space",
        "Volume Free Space",
        "Long-Running Job",
    };

    private static readonly string[] ExpiryFormats = { "yyyy-MM-dd", "yyyy-MM-dd HH:mm" };

    /// <summary>
    /// Builds the input a Save sends. Returns null and sets <paramref name="input"/> when the form is acceptable; otherwise returns the
    /// text to show the operator. Blank time of day, window and expiry mean "none", "the shared default" and "never".
    /// </summary>
    public static string? TryBuild(
        int? serverId, string? metricName, string? timeOfDayText, string? timeZone, string? windowText,
        string? note, string? expiresText, out KnownPatternInput? input)
    {
        input = null;

        TimeOnly? timeOfDay = null;
        if (!string.IsNullOrWhiteSpace(timeOfDayText))
        {
            if (!TimeOnly.TryParseExact(timeOfDayText.Trim(), new[] { "HH:mm", "H:mm", "HH:mm:ss" }, CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed))
            {
                return $"The time of day '{timeOfDayText.Trim()}' is not a time. Use 24-hour HH:mm such as 02:10, or leave it empty for any time.";
            }

            timeOfDay = parsed;
        }

        int? window = null;
        if (!string.IsNullOrWhiteSpace(windowText))
        {
            if (!int.TryParse(windowText.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var minutes))
            {
                return $"The window '{windowText.Trim()}' is not a whole number of minutes.";
            }

            window = minutes;
        }

        DateTime? expires = null;
        if (!string.IsNullOrWhiteSpace(expiresText))
        {
            if (!DateTime.TryParseExact(expiresText.Trim(), ExpiryFormats, CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed))
            {
                return $"The expiry '{expiresText.Trim()}' is not a date. Use UTC as yyyy-MM-dd or yyyy-MM-dd HH:mm, or leave it empty for no expiry.";
            }

            expires = DateTime.SpecifyKind(parsed, DateTimeKind.Utc);
        }

        var built = new KnownPatternInput(serverId, metricName, timeOfDay, timeZone, window, note, expires);
        var refusal = built.Validate();
        if (refusal is not null)
        {
            return refusal;
        }

        input = built;
        return null;
    }
}

/// <summary>One row of the known-pattern list: an entry with the strings the grid shows.</summary>
public sealed class KnownPatternRow
{
    public KnownPatternRow(KnownPattern entry, IReadOnlyDictionary<int, string> serverNames)
    {
        Entry = entry ?? throw new ArgumentNullException(nameof(entry));
        ServerDisplay = entry.ServerId is not { } id
            ? "(any)"
            : serverNames.TryGetValue(id, out var name) ? name : $"#{id.ToString(CultureInfo.InvariantCulture)} (not registered)";
    }

    public KnownPattern Entry { get; }

    public long Id => Entry.Id;

    public string ServerDisplay { get; }

    public string MetricDisplay => Entry.MetricName ?? "(any)";

    public string TimeDisplay => Entry.TimeOfDay is { } time ? time.ToString("HH:mm", CultureInfo.InvariantCulture) : "(any)";

    /// <summary>
    /// What the Time column sorts by: the entry's clock time as a DateTime on a fixed date, or null for "(any)" (which sorts first, as its text did).
    /// The date is meaningless; only the clock time is compared, in the entry's own zone, which is how the column reads.
    /// </summary>
    public DateTime? TimeOfDaySortKey => Entry.TimeOfDay is { } time ? DateTime.MinValue.Date + time.ToTimeSpan() : null;

    public string ZoneDisplay => Entry.TimeOfDay is null ? "" : Entry.TimeZone ?? "UTC";

    public int WindowMinutes => Entry.WindowMinutes;

    public string Note => Entry.Note;

    public string CreatedBy => Entry.CreatedBy ?? "";

    /// <summary>The last editor and when, or empty for an entry never edited. The time reads as every fleet-wide
    /// window words one (#6457): the viewer's Local or UTC reading with the zone's name after it.</summary>
    public string UpdatedDisplay => Entry.UpdatedBy is null && Entry.UpdatedAtUtc is null
        ? ""
        : (Entry.UpdatedBy ?? "") + (Entry.UpdatedAtUtc is { } edited ? ", " + ViewerTimeHelper.FormatFleetWide(edited, "yyyy-MM-dd HH:mm") : "");

    /// <summary>What the Expires column sorts by: the UTC expiry, with "Never" as the latest value (it sorted after every date as text).</summary>
    public DateTime ExpiresSortUtc => Entry.ExpiresAtUtc ?? DateTime.MaxValue;

    public string ExpiresDisplay => Entry.ExpiresAtUtc is { } at
        ? at.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC"
        : "Never";
}
