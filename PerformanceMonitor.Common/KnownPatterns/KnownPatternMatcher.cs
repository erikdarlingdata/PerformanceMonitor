/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;

namespace PerformanceMonitor.Common.KnownPatterns;

/// <summary>
/// #6334: the one rule for "does this known-pattern entry describe this alert", shared by Darling and Lite so the two cannot drift.
///
/// <para>An entry matches an alert when ALL of these hold: the entry's server is NULL or the alert's; the entry's metric is NULL or the
/// alert's (ignoring case, the way alert history filters metric names); the entry's time of day is NULL, or the alert's
/// time converted to the entry's zone is within the entry's window either side of it (a window that crosses midnight wraps: 23:50 is
/// 20 minutes from 00:10); and the entry had not expired at the alert's time. The check is made at the ALERT's time, not at the
/// reader's clock, so an old alert still shows the entry that was in force when it fired.</para>
///
/// <para>The zone is applied to the alert's instant, so a daylight-saving change moves the matching UTC time with it: an entry at 02:00
/// <c>America/New_York</c> matches 07:00 UTC in winter and 06:00 UTC in summer. Zones resolve through
/// <see cref="TimeZoneInfo.FindSystemTimeZoneById"/>, which accepts IANA ids on every platform this ships on. An unknown zone is refused on
/// a write (<see cref="IsKnownTimeZone"/>) and read as UTC here, reported once through the caller's log.</para>
/// </summary>
public static class KnownPatternMatcher
{
    /// <summary>The longest note a write accepts.</summary>
    public const int MaxNoteLength = 500;

    /// <summary>The most entries the table holds (#6398). <c>config.insert_known_pattern</c> refuses a create past it (DL409), so the web editor, the
    /// Viewer and the MCP create tool share it; every read and every investigation brief loads the whole table. Edit and delete are not capped.
    /// The two SQL copies of the function spell the same number.</summary>
    public const int MaxEntries = 500;

    /// <summary>The window an entry gets when its writer names none: the same half-width the investigation brief's recurrence count uses
    /// (<c>InvestigationAlertRecurrence.WindowMinutes</c>), so "recurs at this time" means the same thing in both.</summary>
    public const int DefaultWindowMinutes = InvestigationAlertRecurrence.WindowMinutes;

    /// <summary>The widest window a write accepts: half a day either side already covers every time of day.</summary>
    public const int MaxWindowMinutes = 720;

    private const int MinutesPerDay = 24 * 60;

    private static readonly ConcurrentDictionary<string, TimeZoneInfo?> ZoneCache = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, bool> Warned = new(StringComparer.Ordinal);

    /// <summary>True when <paramref name="timeZone"/> is a zone this machine can resolve. Null or empty is true: it means UTC.</summary>
    public static bool IsKnownTimeZone(string? timeZone) => string.IsNullOrWhiteSpace(timeZone) || FindZone(timeZone.Trim()) is not null;

    /// <summary>
    /// The entries of <paramref name="entries"/> that describe the alert, in the order given. <paramref name="alertTimeUtc"/> is the
    /// alert's instant in UTC (a naive value, as the stores hold it, is read as UTC).
    /// <paramref name="log"/> receives one line the first time an unknown zone is met.
    /// </summary>
    public static IReadOnlyList<KnownPattern> Match(
        IReadOnlyList<KnownPattern> entries, int serverId, string? metricName, DateTime alertTimeUtc, Action<string>? log = null)
    {
        if (entries.Count == 0)
        {
            return Array.Empty<KnownPattern>();
        }

        var matched = new List<KnownPattern>();
        foreach (var entry in entries)
        {
            if (Matches(entry, serverId, metricName, alertTimeUtc, log))
            {
                matched.Add(entry);
            }
        }

        return matched;
    }

    /// <summary>Whether one entry describes the alert. See the class remarks for the rule.</summary>
    public static bool Matches(KnownPattern entry, int serverId, string? metricName, DateTime alertTimeUtc, Action<string>? log = null)
    {
        if (entry.ServerId is { } entryServer && entryServer != serverId)
        {
            return false;
        }

        if (!string.IsNullOrEmpty(entry.MetricName)
            && !string.Equals(entry.MetricName, metricName, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var alertUtc = DateTime.SpecifyKind(alertTimeUtc, DateTimeKind.Utc);
        if (entry.ExpiresAtUtc is { } expires && alertUtc >= DateTime.SpecifyKind(expires, DateTimeKind.Utc))
        {
            return false;
        }

        if (entry.TimeOfDay is not { } timeOfDay)
        {
            return true;
        }

        var local = ToZone(alertUtc, entry.TimeZone, log);
        var alertMinute = local.Hour * 60 + local.Minute;
        var entryMinute = timeOfDay.Hour * 60 + timeOfDay.Minute;
        var apart = Math.Abs(alertMinute - entryMinute);
        apart = Math.Min(apart, MinutesPerDay - apart);
        return apart <= entry.WindowMinutes;
    }

    private static DateTime ToZone(DateTime utc, string? timeZone, Action<string>? log)
    {
        if (string.IsNullOrWhiteSpace(timeZone))
        {
            return utc;
        }

        var id = timeZone.Trim();
        var zone = FindZone(id);
        if (zone is null)
        {
            if (Warned.Count < MaxCachedZones && Warned.TryAdd(id, true))
            {
                log?.Invoke(string.Create(CultureInfo.InvariantCulture,
                    $"Known pattern: time zone '{id}' is not known on this machine; its entries are matched in UTC."));
            }

            return utc;
        }

        return TimeZoneInfo.ConvertTimeFromUtc(utc, zone);
    }

    /// <summary>
    /// Resolves an IANA zone id, caching only the zones that were found: a miss is never stored, so ids a caller invents cannot grow the
    /// cache (security review of #6334). Every non-fatal failure counts as "not found": on Unix a folder-named id such as <c>America</c>
    /// throws a <see cref="System.Security.SecurityException"/> or an <see cref="System.IO.IOException"/> instead of
    /// <see cref="TimeZoneNotFoundException"/>, and a lookup must never throw out of a match or a write check.
    /// </summary>
    private static TimeZoneInfo? FindZone(string id)
    {
        if (ZoneCache.TryGetValue(id, out var cached))
        {
            return cached;
        }

        if (!IsZoneIdShape(id))
        {
            return null;
        }

        TimeZoneInfo? found;
        try
        {
            found = ZoneLookup(id);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return null;
        }

        if (found is not null && ZoneCache.Count < MaxCachedZones)
        {
            ZoneCache[id] = found;
        }

        return found;
    }

    /// <summary>The lookup behind <see cref="FindZone"/>; a test replaces it to fake a host whose lookup throws.</summary>
    internal static Func<string, TimeZoneInfo?> ZoneLookup { get; set; } = static id => TimeZoneInfo.FindSystemTimeZoneById(id);

    /// <summary>How many resolved zones the cache keeps. The machine has a few hundred zones; this is a backstop, not a working limit.</summary>
    private const int MaxCachedZones = 1024;

    /// <summary>The number of zones the cache holds (for tests).</summary>
    internal static int CachedZoneCount => ZoneCache.Count;

    /// <summary>True when <paramref name="id"/> is shaped like an IANA id: at most <see cref="MaxTimeZoneLength"/> characters of
    /// letters, digits, <c>_ + -</c>, with <c>/</c> between parts. Anything else is refused before any lookup.</summary>
    internal static bool IsZoneIdShape(string id) => id.Length <= MaxTimeZoneLength && ZoneIdShape.IsMatch(id);

    /// <summary>The longest time zone id a write accepts. The store's CHECK uses the same limit.</summary>
    public const int MaxTimeZoneLength = 64;

    /// <summary>The longest metric name a write accepts.</summary>
    public const int MaxMetricNameLength = 128;

    private static readonly System.Text.RegularExpressions.Regex ZoneIdShape = new(
        @"\A[A-Za-z0-9_+-]+(/[A-Za-z0-9_+-]+)*\z",
        System.Text.RegularExpressions.RegexOptions.CultureInvariant | System.Text.RegularExpressions.RegexOptions.NonBacktracking);
}
