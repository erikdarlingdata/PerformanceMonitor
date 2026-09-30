/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;

namespace PerformanceMonitor.Analysis.Baselines;

/// <summary>
/// A monitored server's wall clock, as the viewer needs it: naive UTC to the server's local time and back,
/// with the offset following the server's own time zone across a daylight-saving change (#4766).
///
/// <para>The viewer used to hold ONE <c>utc_offset_minutes</c> per tab and add it to every time. That is
/// right on the snapshot's own side of a daylight-saving change and an hour off on the other, so a chart or a
/// picker range that crossed the change showed the wrong wall clock. This type keeps the zone instead. It is
/// not <see cref="LocalClockWindow"/>: that holds one transition inside one baseline window, and a viewer
/// range (a year of job history, say) can cross two.</para>
///
/// <para><see cref="Resolve"/> picks the input in the same order <see cref="BaselineLocalClock"/> does, and
/// looks the zone up with the same <see cref="BaselineLocalClock.TryFindZone"/>: the zone id when it resolves
/// on this host, else the stored offset as a fixed shift, else UTC.</para>
///
/// <para>Every value in and out is naive: <see cref="DateTimeKind.Unspecified"/>, the frame the store keeps.
/// The instance is immutable, so a tab can hand it to the process-wide time helper and to a background read.</para>
/// </summary>
public sealed class ServerClock
{
    private readonly TimeZoneInfo? _zone;
    private readonly int _fixedOffsetMinutes;

    /* The fixed-offset zone AsTimeZone hands out, built on first use and kept: a chart asks for the zone on
       every render pass, and a custom zone is an allocation the first call pays for and the rest reuse. */
    private TimeZoneInfo? _fixedZone;

    private ServerClock(TimeZoneInfo? zone, int fixedOffsetMinutes)
    {
        _zone = zone;
        _fixedOffsetMinutes = fixedOffsetMinutes;
    }

    /// <summary>The UTC clock: no shift. What a server with neither a zone id nor an offset gets.</summary>
    public static ServerClock Utc { get; } = new(null, 0);

    /// <summary>A clock that is always <paramref name="offsetMinutes"/> ahead of UTC, whatever the date.</summary>
    public static ServerClock FixedOffset(int offsetMinutes) => offsetMinutes == 0 ? Utc : new(null, offsetMinutes);

    /// <summary>
    /// The zone id when it resolves on this host, else the fixed offset, else UTC.
    /// </summary>
    /// <param name="timeZoneId">
    /// <c>server_properties.time_zone_id</c>: a Windows zone id ("Eastern Standard Time") on a SQL Server 2022
    /// or later engine, NULL before. Resolved by <see cref="BaselineLocalClock.TryFindZone"/>.
    /// </param>
    /// <param name="utcOffsetMinutes"><c>server_properties.utc_offset_minutes</c>: the offset in force at the snapshot.</param>
    public static ServerClock Resolve(string? timeZoneId, int? utcOffsetMinutes)
    {
        if (!string.IsNullOrWhiteSpace(timeZoneId))
        {
            var zone = BaselineLocalClock.TryFindZone(timeZoneId);
            if (zone is not null)
            {
                return new ServerClock(zone, 0);
            }
        }

        return utcOffsetMinutes.HasValue ? FixedOffset(utcOffsetMinutes.Value) : Utc;
    }

    /// <summary>
    /// This clock as a <see cref="TimeZoneInfo"/>, for code that renders through a zone and cannot take a
    /// <see cref="ServerClock"/> (the shared chart project does not reference this one): the server's own zone
    /// when it resolved, else a custom zone at the fixed offset, else <see cref="TimeZoneInfo.Utc"/>. The
    /// fixed-offset zone is built once per instance. An offset past the 14 hours a zone can hold (a stored value
    /// no real server reports) is held at 14 hours rather than thrown.
    /// </summary>
    public TimeZoneInfo AsTimeZone()
    {
        if (_zone is not null)
        {
            return _zone;
        }

        if (_fixedOffsetMinutes == 0)
        {
            return TimeZoneInfo.Utc;
        }

        var cached = Volatile.Read(ref _fixedZone);
        if (cached is not null)
        {
            return cached;
        }

        const int maxMinutes = 14 * 60;
        var offset = TimeSpan.FromMinutes(Math.Clamp(_fixedOffsetMinutes, -maxMinutes, maxMinutes));
        var sign = offset < TimeSpan.Zero ? "-" : "+";
        var name = string.Create(CultureInfo.InvariantCulture, $"UTC{sign}{offset.Duration().Hours:00}:{offset.Duration().Minutes:00}");
        var created = TimeZoneInfo.CreateCustomTimeZone(name, offset, name, name);

        /* Two threads can race to the first call; the loser's zone is dropped so every caller sees one instance. */
        return Interlocked.CompareExchange(ref _fixedZone, created, null) ?? created;
    }

    /// <summary>Minutes the server's clock is ahead of UTC at the instant <paramref name="utc"/> (naive UTC).</summary>
    public int OffsetMinutesAt(DateTime utc)
    {
        if (_zone is null)
        {
            return _fixedOffsetMinutes;
        }

        return (int)_zone.GetUtcOffset(DateTime.SpecifyKind(utc, DateTimeKind.Utc)).TotalMinutes;
    }

    /// <summary>The server's wall-clock time at the instant <paramref name="utc"/> (naive UTC), Kind Unspecified.</summary>
    public DateTime ToServerLocal(DateTime utc)
        => Shift(DateTime.SpecifyKind(utc, DateTimeKind.Unspecified), OffsetMinutesAt(utc));

    /// <summary>
    /// The naive-UTC instant of a server wall-clock time, Kind Unspecified. Never throws, because a picker
    /// can hand it any wall-clock value and a stored server-local time can land on either odd hour:
    /// <list type="bullet">
    ///   <item>A SKIPPED local time (spring forward: 02:30 on the US change day never happened) moves forward
    ///   by the gap, so it reads as 03:30 daylight time.</item>
    ///   <item>A REPEATED local time (fall back: 01:30 happens twice) takes the FIRST occurrence, the daylight
    ///   offset. A stored server-local time cannot say which one it was.</item>
    /// </list>
    /// </summary>
    public DateTime ToUtc(DateTime serverLocal)
    {
        var local = DateTime.SpecifyKind(serverLocal, DateTimeKind.Unspecified);
        if (_zone is null)
        {
            return Shift(local, -_fixedOffsetMinutes);
        }

        TimeSpan offset;
        if (_zone.IsAmbiguousTime(local))
        {
            /* Both offsets are valid; the first occurrence is the one further ahead of UTC. */
            offset = TimeSpan.MinValue;
            foreach (var candidate in _zone.GetAmbiguousTimeOffsets(local))
            {
                if (candidate > offset)
                {
                    offset = candidate;
                }
            }
        }
        else
        {
            /* An invalid (skipped) local time answers with the standard offset: the one in force before the
               change, so subtracting it lands the time on the far side of the gap. */
            offset = _zone.GetUtcOffset(local);
        }

        return Shift(local, -(int)offset.TotalMinutes);
    }

    private static DateTime Shift(DateTime value, int minutes)
    {
        /* Clamp instead of throwing at the ends of the calendar: a sentinel MinValue/MaxValue bound must
           survive the conversion. */
        var ticks = value.Ticks + minutes * TimeSpan.TicksPerMinute;
        if (ticks < DateTime.MinValue.Ticks)
        {
            return DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Unspecified);
        }

        if (ticks > DateTime.MaxValue.Ticks)
        {
            return DateTime.SpecifyKind(DateTime.MaxValue, DateTimeKind.Unspecified);
        }

        return new DateTime(ticks, DateTimeKind.Unspecified);
    }
}
