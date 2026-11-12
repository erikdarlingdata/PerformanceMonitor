/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Darling.Service.Reports;

/// <summary>
/// The four Daily Health Report settings (#7041) and the one place that validates and resolves them. The store
/// columns are <c>daily_report_enabled</c>, <c>daily_report_send_at</c>, <c>daily_report_time_zone</c> and
/// <c>daily_report_title</c> (V232); <c>update_alert_settings</c> validates through <see cref="TryParseSendAt"/>,
/// <see cref="TryResolveZone"/> and <see cref="TryValidateTitle"/> so a bad value is refused with a message rather
/// than stored and silently replaced at the first tick.
/// </summary>
public static class DailyHealthReportSettings
{
    /// <summary>The shipped send time, local time in the report's zone.</summary>
    public const string DefaultSendAt = "07:00";

    /// <summary>The shipped title, used when <c>daily_report_title</c> is NULL.</summary>
    public const string DefaultTitle = "Daily Health Report";

    /// <summary>The longest accepted title. A title lands in an email subject and a Slack heading.</summary>
    public const int MaxTitleLength = 100;

    private static readonly string[] SendAtFormats = { "HH\\:mm", "H\\:mm", "HH\\:mm\\:ss" };

    /// <summary>Parses a send time as <c>HH:mm</c> (or <c>HH:mm:ss</c>), a time of day with no zone. Returns false for anything else.</summary>
    public static bool TryParseSendAt(string? text, out TimeOnly sendAt)
    {
        sendAt = default;
        if (string.IsNullOrWhiteSpace(text))
        {
            return false;
        }

        if (!TimeOnly.TryParseExact(text.Trim(), SendAtFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed))
        {
            return false;
        }

        sendAt = new TimeOnly(parsed.Hour, parsed.Minute);
        return true;
    }

    /// <summary>The wire spelling of a send time: <c>HH:mm</c>.</summary>
    public static string FormatSendAt(TimeOnly sendAt) => sendAt.ToString("HH:mm", CultureInfo.InvariantCulture);

    /// <summary>The send time to use for a configured string: the parsed value, or <see cref="DefaultSendAt"/> when it does not parse.</summary>
    public static TimeOnly ResolveSendAt(string? text) =>
        TryParseSendAt(text, out var sendAt) ? sendAt : new TimeOnly(7, 0);

    /// <summary>
    /// Resolves a configured zone. NULL or blank means the service host's local zone. Otherwise the id is looked up the way
    /// the host resolves ids (an IANA id such as <c>America/New_York</c>, or the host's own spelling). Returns false for an
    /// id the host does not know.
    /// </summary>
    public static bool TryResolveZone(string? zoneId, out TimeZoneInfo zone)
    {
        if (string.IsNullOrWhiteSpace(zoneId))
        {
            zone = TimeZoneInfo.Local;
            return true;
        }

        try
        {
            zone = TimeZoneInfo.FindSystemTimeZoneById(zoneId.Trim());
            return true;
        }
        catch (TimeZoneNotFoundException)
        {
            zone = TimeZoneInfo.Local;
            return false;
        }
        catch (InvalidTimeZoneException)
        {
            zone = TimeZoneInfo.Local;
            return false;
        }
    }

    /// <summary>The zone to schedule in: the configured one, or the host's local zone when it is unset or unknown to the host.</summary>
    public static TimeZoneInfo ResolveZone(string? zoneId) => TryResolveZone(zoneId, out var zone) ? zone : TimeZoneInfo.Local;

    /// <summary>
    /// Checks a title. NULL (use the default) is the caller's case; this takes a non-null string and refuses blank text,
    /// text over <see cref="MaxTitleLength"/> and control characters (a newline in a title would break an email subject).
    /// </summary>
    public static bool TryValidateTitle(string title, out string? error)
    {
        if (string.IsNullOrWhiteSpace(title))
        {
            error = "must not be blank (send null to use the default title).";
            return false;
        }

        if (title.Trim().Length > MaxTitleLength)
        {
            error = $"must be at most {MaxTitleLength} characters.";
            return false;
        }

        foreach (var c in title)
        {
            if (char.IsControl(c))
            {
                error = "must not contain control characters.";
                return false;
            }
        }

        error = null;
        return true;
    }

    /// <summary>The title to print: the configured one trimmed, or <see cref="DefaultTitle"/> when it is unset or blank.</summary>
    public static string ResolveTitle(string? title) => string.IsNullOrWhiteSpace(title) ? DefaultTitle : title.Trim();
}
