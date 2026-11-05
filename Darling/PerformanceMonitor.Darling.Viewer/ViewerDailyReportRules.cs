/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Daily Health Report's setting rules (#7041) as the Viewer's Settings window applies them. The Viewer does not reference the
/// service project, so this mirrors the service's <c>DailyHealthReportSettings</c> (the same rules <c>update_alert_settings</c>
/// enforces): a send time written <c>HH:mm</c>, a time zone id the machine knows (blank means the service host's zone), and a title
/// of at most 100 characters with no control characters (blank means "Daily Health Report"). The zone is checked against THIS
/// machine's zone database, the service checks the host's; both resolve an IANA id such as America/New_York.
/// </summary>
public static class ViewerDailyReportRules
{
    public const string DefaultSendAt = "07:00";
    public const int MaxTitleLength = 100;

    private static readonly string[] SendAtFormats = { "HH\\:mm", "H\\:mm", "HH\\:mm\\:ss" };

    public static bool TryParseSendAt(string? text, out TimeOnly sendAt)
    {
        sendAt = default;
        if (string.IsNullOrWhiteSpace(text)
            || !TimeOnly.TryParseExact(text.Trim(), SendAtFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed))
        {
            return false;
        }

        sendAt = new TimeOnly(parsed.Hour, parsed.Minute);
        return true;
    }

    public static string FormatSendAt(TimeOnly sendAt) => sendAt.ToString("HH:mm", CultureInfo.InvariantCulture);

    /// <summary>The send time to store for a row's text: the parsed value, or 07:00 when it does not parse (what the service does on read).</summary>
    public static TimeOnly ResolveSendAt(string? text) => TryParseSendAt(text, out var sendAt) ? sendAt : new TimeOnly(7, 0);

    /// <summary>True for a blank zone (the service host's zone) or an id this machine knows.</summary>
    public static bool IsKnownZone(string? zoneId)
    {
        if (string.IsNullOrWhiteSpace(zoneId))
        {
            return true;
        }

        try
        {
            TimeZoneInfo.FindSystemTimeZoneById(zoneId.Trim());
            return true;
        }
        catch (TimeZoneNotFoundException)
        {
            return false;
        }
        catch (InvalidTimeZoneException)
        {
            return false;
        }
    }

    /// <summary>Checks a non-blank title; returns false with the reason for text over <see cref="MaxTitleLength"/> or with a control character.</summary>
    public static bool TryValidateTitle(string title, out string? error)
    {
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

    /// <summary>The trimmed text, or a database NULL for a blank (the zone and the title store NULL for "not set").</summary>
    public static object TextOrNull(string? text) => string.IsNullOrWhiteSpace(text) ? DBNull.Value : text.Trim();
}
