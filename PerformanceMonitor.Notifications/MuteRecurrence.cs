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

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The recurring schedule of a Darling mute rule (#7040): days of the week, a daily start and end time, and the IANA
/// time zone those times are read in. The one home of the day-mask encoding, the day-name parsing, the zone
/// normalisation, the in-window test and the next-window search, so the MCP tools, the web routes, the Viewer and the
/// SQL readers cannot disagree about what a schedule means. Pure: no clock, no I/O beyond the OS time-zone database.
///
/// <para>Days are a bit mask, bit n set for <see cref="DayOfWeek"/> value n (Sunday = bit 0, Saturday = bit 6); a null mask
/// is every day. A window that crosses midnight (start later than end) belongs to the day it STARTS on, so
/// "Friday 22:00 to 02:00" covers Friday night into Saturday morning.</para>
///
/// <para>Times are wall-clock times in the rule's zone. Across a daylight-saving change the window is the same wall-clock
/// span, so on the night clocks go back, a 01:30-02:30 window covers the repeated hour twice, and on the night they go
/// forward the skipped hour is simply absent.</para>
/// </summary>
public static class MuteRecurrence
{
    /// <summary>All seven days set.</summary>
    public const int AllDaysMask = 0x7F;

    private static readonly string[] ShortNames = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    private static readonly string[] FullNames = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

    /// <summary>The mask bit for one day.</summary>
    public static int Bit(DayOfWeek day) => 1 << (int)day;

    /// <summary>Whether <paramref name="mask"/> lists <paramref name="day"/>. A null mask lists every day.</summary>
    public static bool IncludesDay(int? mask, DayOfWeek day) => mask is not { } m || (m & Bit(day)) != 0;

    /// <summary>The short day names (<c>Mon</c>) a mask lists, Sunday first. A null mask yields all seven.</summary>
    public static IReadOnlyList<string> DayNames(int? mask)
    {
        var names = new List<string>(7);
        for (var i = 0; i < 7; i++)
            if (IncludesDay(mask, (DayOfWeek)i)) names.Add(ShortNames[i]);
        return names;
    }

    /// <summary>Parses day names, case-insensitive, short (<c>mon</c>) or full (<c>Monday</c>), into a mask. A null or empty
    /// list is "no days given" (<paramref name="mask"/> null, true). An unknown name fails, naming it.</summary>
    public static bool TryParseDays(IEnumerable<string?>? names, out int? mask, out string? error)
    {
        mask = null;
        error = null;
        if (names is null) return true;
        var m = 0;
        var any = false;
        foreach (var raw in names)
        {
            var name = raw?.Trim();
            if (string.IsNullOrEmpty(name)) continue;
            var found = -1;
            for (var i = 0; i < 7; i++)
            {
                if (string.Equals(name, ShortNames[i], StringComparison.OrdinalIgnoreCase)
                    || string.Equals(name, FullNames[i], StringComparison.OrdinalIgnoreCase))
                {
                    found = i;
                    break;
                }
            }
            if (found < 0)
            {
                error = $"'{name}' is not a day of the week. Use Sun, Mon, Tue, Wed, Thu, Fri, Sat (or the full names).";
                return false;
            }
            m |= 1 << found;
            any = true;
        }
        mask = any ? m : null;
        return true;
    }

    /// <summary>Parses an <c>HH:mm</c> (or <c>HH:mm:ss</c>) time of day.</summary>
    public static bool TryParseTime(string? text, out TimeOnly time) =>
        TimeOnly.TryParseExact(text?.Trim(), ["HH:mm", "H:mm", "HH:mm:ss"], CultureInfo.InvariantCulture, DateTimeStyles.None, out time);

    /// <summary>A time of day as <c>HH:mm</c> (seconds only when set).</summary>
    public static string FormatTime(TimeOnly time) =>
        time.ToString(time.Second == 0 ? "HH:mm" : "HH:mm:ss", CultureInfo.InvariantCulture);

    /// <summary>Turns a zone id the caller typed into the IANA id the store keeps. A Windows id
    /// (<c>Eastern Standard Time</c>) is converted; an IANA id is kept as given. False when the host does not know the zone, and
    /// false for a Windows id that has no IANA equivalent (<c>Mid-Atlantic Standard Time</c>): the store keeps IANA ids only, and
    /// PostgreSQL cannot resolve a Windows one.</summary>
    public static bool TryNormalizeZone(string? zone, out string iana)
    {
        iana = "";
        var z = zone?.Trim();
        if (string.IsNullOrEmpty(z)) return false;
        if (TimeZoneInfo.TryConvertWindowsIdToIanaId(z, out var converted) && !string.IsNullOrEmpty(converted))
        {
            iana = converted;
            return true;
        }
        try
        {
            /* A zone found under a Windows id the table above could not convert has no IANA id; storing it would give PostgreSQL a name it rejects. */
            if (!TimeZoneInfo.FindSystemTimeZoneById(z).HasIanaId) return false;
        }
        catch (Exception ex) when (ex is TimeZoneNotFoundException or InvalidTimeZoneException)
        {
            return false;
        }
        iana = z;
        return true;
    }

    /// <summary>The service host's local zone as an IANA id, the default for a schedule given no zone.</summary>
    public static string HostZoneIana()
    {
        var local = TimeZoneInfo.Local;
        if (TimeZoneInfo.TryConvertWindowsIdToIanaId(local.Id, out var iana) && !string.IsNullOrEmpty(iana)) return iana;
        return local.Id;
    }

    /// <summary>
    /// Checks a schedule and normalises the zone. Times are both set or both null and must differ; days or a zone without
    /// times are refused; a zone is stored as an IANA id (a Windows id is converted, an unknown one refused); times without a zone
    /// take the host's zone. With no times and no days and no zone the schedule is absent and passes unchanged.
    /// </summary>
    public static bool TryNormalize(int? days, TimeOnly? start, TimeOnly? end, ref string? zone, out string? error)
    {
        error = null;
        var hasZone = !string.IsNullOrWhiteSpace(zone);
        if (start.HasValue != end.HasValue)
        {
            error = "'recur_start' and 'recur_end' go together: set both or neither.";
            return false;
        }
        if (!start.HasValue)
        {
            if (days.HasValue || hasZone)
            {
                error = "'recur_days' and 'recur_time_zone' need 'recur_start' and 'recur_end'. Set the daily start and end time, or clear the schedule.";
                return false;
            }
            zone = null;
            return true;
        }
        if (start.Value == end!.Value)
        {
            error = "'recur_start' and 'recur_end' must differ. For every hour of a day, use a one-off mute instead.";
            return false;
        }
        if (days is { } m && (m < 1 || m > AllDaysMask))
        {
            error = "'recur_days' must list at least one day of the week.";
            return false;
        }
        if (hasZone)
        {
            if (!TryNormalizeZone(zone, out var iana))
            {
                error = $"'recur_time_zone' is not a time zone this host knows (got '{zone!.Trim()}'). Use an IANA id such as America/New_York or UTC.";
                return false;
            }
            zone = iana;
        }
        else
        {
            /* The host's own zone is stored the same way a typed one is: as an IANA id, or not at all. */
            if (!TryNormalizeZone(HostZoneIana(), out var hostIana))
            {
                error = "This host's time zone has no IANA id to store. Give 'recur_time_zone' an IANA id such as America/New_York or UTC.";
                return false;
            }
            zone = hostIana;
        }
        return true;
    }

    /// <summary>Whether the schedule is inside a window at <paramref name="nowUtc"/>. No times means no schedule, so always true.
    /// A zone this host cannot resolve, or one that is not an IANA id, reads as outside the window: the rule mutes nothing rather than
    /// throwing in the alert path. The SQL form (<c>MuteRuleRecurrenceSql.InWindow</c>) reads a zone its PostgreSQL does not know the same way.</summary>
    public static bool IsInWindowAt(int? days, TimeOnly? start, TimeOnly? end, string? zone, DateTime nowUtc)
    {
        if (!start.HasValue || !end.HasValue) return true;
        if (!TryGetZone(zone, out var tz)) return false;
        var local = TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(nowUtc, DateTimeKind.Utc), tz);
        var t = TimeOnly.FromDateTime(local);
        var today = local.DayOfWeek;
        var s = start.Value;
        var e = end.Value;
        if (s < e) return IncludesDay(days, today) && t >= s && t < e;
        var yesterday = (DayOfWeek)(((int)today + 6) % 7);
        return (IncludesDay(days, today) && t >= s) || (IncludesDay(days, yesterday) && t < e);
    }

    /// <summary>The UTC instant the next window starts after <paramref name="nowUtc"/>, or null when the schedule has none (no
    /// times, an unknown zone). The caller applies the rule's own start and expiry.</summary>
    public static DateTime? NextWindowStartUtc(int? days, TimeOnly? start, TimeOnly? end, string? zone, DateTime nowUtc)
    {
        if (!start.HasValue || !end.HasValue) return null;
        if (!TryGetZone(zone, out var tz)) return null;
        var now = DateTime.SpecifyKind(nowUtc, DateTimeKind.Utc);
        var localDate = TimeZoneInfo.ConvertTimeFromUtc(now, tz).Date;
        for (var i = 0; i <= 8; i++)
        {
            var date = localDate.AddDays(i);
            if (!IncludesDay(days, date.DayOfWeek)) continue;
            var local = DateTime.SpecifyKind(date + start.Value.ToTimeSpan(), DateTimeKind.Unspecified);
            /* A start inside the skipped spring-forward hour begins when the hour ends. */
            if (tz.IsInvalidTime(local)) local = local.AddHours(1);
            var utc = DateTime.SpecifyKind(local - tz.GetUtcOffset(local), DateTimeKind.Utc);
            if (utc > now) return utc;
        }
        return null;
    }

    /// <summary>A one-line schedule such as <c>Mon,Tue 22:00-02:00 America/New_York</c>, or null when there is none.</summary>
    public static string? Describe(int? days, TimeOnly? start, TimeOnly? end, string? zone)
    {
        if (!start.HasValue || !end.HasValue) return null;
        var dayText = days is null || days == AllDaysMask ? "daily" : string.Join(",", DayNames(days));
        return $"{dayText} {FormatTime(start.Value)}-{FormatTime(end.Value)} {zone ?? "(host zone)"}";
    }

    private static bool TryGetZone(string? zone, out TimeZoneInfo tz)
    {
        tz = TimeZoneInfo.Utc;
        if (string.IsNullOrWhiteSpace(zone)) return false;
        try
        {
            tz = TimeZoneInfo.FindSystemTimeZoneById(zone);
            /* A Windows id the host resolves but PostgreSQL cannot is outside the window here too, so both readers agree (#7040). */
            return tz.HasIanaId;
        }
        catch (Exception ex) when (ex is TimeZoneNotFoundException or InvalidTimeZoneException)
        {
            return false;
        }
    }
}
