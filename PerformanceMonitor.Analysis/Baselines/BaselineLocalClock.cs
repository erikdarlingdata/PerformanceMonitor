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

namespace PerformanceMonitor.Analysis.Baselines;

/// <summary>
/// The monitored target's clock over ONE baseline window, reduced to the three numbers the bucket SQL can
/// key on without a time-zone database of its own (#3653 item 12, Q6): the single offset transition inside
/// the window (or the window's end when there is none), and the UTC offset in force before and after it.
///
/// <para><b>The defect this exists for.</b> Both SKUs stamp <c>collection_time</c> with the service host's
/// <c>DateTime.UtcNow</c> and the hour-of-week baseline keyed on it — <c>EXTRACT(HOUR FROM collection_time)</c>,
/// <c>EXTRACT(DOW FROM collection_time)</c>. A store's "Tuesday 22h" bucket was therefore Tuesday 17:00 for a
/// target at UTC−5 in winter and Tuesday 18:00 for the SAME target in summer: the bucket pooled two different
/// local hours, and the operator reading the finding's <c>baseline_bucket</c> ("Tue 22:00") saw a time nobody on
/// that server keeps. Erik's ruling: hour-of-week buckets key on the target's LOCAL clock; the UTC smear IS the
/// DST defect. The clock lives in <c>server_properties.utc_offset_minutes</c> (the offset in force at the
/// snapshot) and <c>server_properties.time_zone_id</c> (V134 — <c>CURRENT_TIMEZONE_ID()</c>, a Windows zone id such
/// as "Eastern Standard Time", NULL on a pre-2022 engine).</para>
///
/// <para><b>Why three numbers and not <c>AT TIME ZONE</c>.</b> Every row's <c>collection_time</c> is naive UTC, so
/// the conversion is exact row by row — but doing it with a zone NAME in SQL would need the Windows-id → IANA
/// mapping bound as text, PostgreSQL's own tz database for the Darling store and DuckDB's ICU extension for Lite,
/// and a SECOND statement shape for the offset-only fallback. A real zone changes offset at most ONCE in any
/// 30-day window (DST pairs are six months apart; the closest pair in the current tz database is a Ramadan
/// suspension ~35 days wide), so the whole zone, over this window, IS a step function with one step:
/// <c>collection_time + (CASE WHEN collection_time &lt; transition THEN before ELSE after END) * INTERVAL '1' MINUTE</c>.
/// That is engine-agnostic arithmetic (<see cref="LocalCollectionTimeSql"/> runs byte-identical on PostgreSQL and
/// DuckDB — the same <c>integer * INTERVAL '1' MINUTE</c> idiom the finding stores' recurrence read already
/// shares), it is exact by construction, and the C# lookup (<see cref="LocalClockWindow.LocalKey"/>) uses the SAME
/// three numbers, so the key the SQL wrote and the key the provider looks up cannot disagree. The step function
/// also degrades honestly: no transition in the window, or a NULL zone id with only the offset, binds
/// <c>before == after</c>; no <c>server_properties</c> row at all (a PostgreSQL target today) binds 0/0, which is
/// exactly the UTC keying the store had before this file existed.</para>
///
/// <para><b>Nothing keyed is stored, so nothing re-buckets by migration.</b> Every baseline supply carries
/// per-collection rows with their own <c>collection_time</c>; the hour×dow key exists only in the query text and
/// in the providers' 1-hour cache. Existing baselines therefore "re-bucket once" automatically — the first compute
/// after the cache expires keys on local time. That is also why the aggregates did not need a rung.</para>
///
/// <para><b>Where it lives.</b> The shared assembly, beside <see cref="BaselineMath"/> and
/// <see cref="BaselineBucket"/>, for the same reason they do: both providers (Lite <c>BaselineProvider</c>,
/// Darling <c>PgBaselineProvider</c>) call this ONE resolver and embed this ONE SQL expression, so the SKUs cannot
/// drift on how a row's time becomes a bucket. Pure math with no store dependency: the providers read the two
/// columns and hand them in; this file never opens a connection.</para>
/// </summary>
public sealed class BaselineLocalClock
{
    /// <summary>
    /// The bucket key's time source, for BOTH SQL dialects: <c>collection_time</c> shifted to the target's local
    /// clock by the piecewise offset. Bound parameters, after the providers' <c>$1 server_id, $2 window start,
    /// $3 analysis time</c>: <c>$4</c> the transition instant (naive UTC; the window end when there is none),
    /// <c>$5</c> the offset in minutes before it, <c>$6</c> the offset after it. Parenthesized so it can be
    /// cast (<c>…::DATE</c>) and EXTRACTed from without further bracketing.
    ///
    /// <para>The shared <c>RobustTierScaffold</c> in each provider extracts hour, dow and date from this, so every
    /// arm that ends in <c>clean(collection_time, v)</c> keys locally with no edit of its own; an arm with its own
    /// <c>EXTRACT</c> (the two event-family arms) must extract from THIS and not from bare <c>collection_time</c>.
    /// Neither engine enforces that: measured on PostgreSQL 17 through Npgsql and on DuckDB 1.5.5, a statement that
    /// references only <c>$1..$3</c> while six parameters are bound executes without complaint — an arm that
    /// bypassed the scaffold would key on UTC SILENTLY. The census tests in both test projects are the
    /// enforcement.</para>
    /// </summary>
    public const string LocalCollectionTimeSql =
        "(collection_time + (CASE WHEN collection_time < $4 THEN $5 ELSE $6 END) * INTERVAL '1' MINUTE)";

    /// <summary>
    /// The transition search's step. A zone's offset is sampled at this grain across the window and every change
    /// is then bisected to the second, so a transition is found wherever it falls; only two transitions inside
    /// the SAME hour could hide from it, and no zone has ever had that.
    /// </summary>
    private static readonly TimeSpan ScanStep = TimeSpan.FromHours(1);

    private readonly Action<string>? _log;
    private readonly ConcurrentDictionary<string, bool> _noted = new(StringComparer.Ordinal);

    /// <param name="log">
    /// Where the ONCE-per-process notes go (Information — a configuration statement about this host, not an
    /// event): the zone id the store holds cannot be resolved here, or a window held more than one transition.
    /// Null discards them; the providers pass their own sink (Darling <c>ILogger</c>, Lite <c>AppLogger</c>).
    /// </param>
    public BaselineLocalClock(Action<string>? log = null)
    {
        _log = log;
    }

    /// <summary>
    /// The target's clock over <c>[windowStartUtc, windowEndUtc)</c>, from the two <c>server_properties</c>
    /// columns as the provider read them (newest row with an offset).
    ///
    /// <para>Preference order, and why: the zone id when present and resolvable on this host — it is the only
    /// input that knows WHEN the offset changed inside the window, which is the whole of the DST fix; else the
    /// stored offset as a fixed shift for the whole window (correct on every day of the window but the ones
    /// across a transition from the snapshot's side, and the note says so once); else UTC, which is what every
    /// bucket was before. A resolvable id whose offset at the window end disagrees with the stored offset is
    /// still the zone's answer — the snapshot could be from the other side of the transition.</para>
    /// </summary>
    /// <param name="timeZoneId">
    /// <c>server_properties.time_zone_id</c>: a Windows zone id ("Eastern Standard Time") on a 2022+ engine, NULL
    /// before. Resolved with <see cref="TimeZoneInfo.FindSystemTimeZoneById"/>, which accepts Windows ids on
    /// Linux and macOS as well as Windows when ICU is present — the Darling service image (Debian aspnet) has it;
    /// an invariant-globalization host does not, and lands on the offset fallback with the note.
    /// </param>
    /// <param name="utcOffsetMinutes"><c>server_properties.utc_offset_minutes</c>: the offset in force at the snapshot.</param>
    /// <param name="windowStartUtc">The baseline window's inclusive start (<c>$2</c>), naive UTC.</param>
    /// <param name="windowEndUtc">The baseline window's exclusive end (<c>$3</c>, the analysis time), naive UTC.</param>
    public LocalClockWindow Resolve(string? timeZoneId, int? utcOffsetMinutes, DateTime windowStartUtc, DateTime windowEndUtc)
    {
        if (windowEndUtc < windowStartUtc)
        {
            throw new ArgumentException("The window ends before it starts.", nameof(windowEndUtc));
        }

        if (!string.IsNullOrWhiteSpace(timeZoneId))
        {
            var zone = TryFindZone(timeZoneId);
            if (zone is not null)
            {
                return FromZone(zone, timeZoneId, windowStartUtc, windowEndUtc);
            }

            NoteOnce(
                "zone:" + timeZoneId,
                string.Create(CultureInfo.InvariantCulture,
                    $"[BaselineLocalClock] zone id '{timeZoneId}' is not resolvable on this host; using the offset in force at the snapshot ({FormatOffset(utcOffsetMinutes ?? 0)}), which is off by an hour across a DST transition. Hour-of-week baselines for this server key on that fixed offset until the host can resolve the zone."));
        }

        return LocalClockWindow.FixedOffset(windowEndUtc, utcOffsetMinutes ?? 0);
    }

    /// <summary>
    /// The zone's offset step inside the window. Samples the offset hourly from start to end, bisects every
    /// change to the second, and returns the ONE step the SQL can express. More than one step in a window is a
    /// zone the tz database says does not exist today; if it ever does, the NEWEST step is kept — the analysis
    /// time's own segment and the freshest history stay exact, the rows before the earlier step are off by that
    /// earlier shift — and the note says so once rather than pooling silently.
    /// </summary>
    private LocalClockWindow FromZone(TimeZoneInfo zone, string timeZoneId, DateTime windowStartUtc, DateTime windowEndUtc)
    {
        var start = DateTime.SpecifyKind(windowStartUtc, DateTimeKind.Utc);
        var end = DateTime.SpecifyKind(windowEndUtc, DateTimeKind.Utc);

        var transitions = new List<(DateTime AtUtc, int Before, int After)>();
        var previous = OffsetMinutes(zone, start);
        var cursor = start;
        while (cursor < end)
        {
            var next = cursor + ScanStep;
            if (next > end)
            {
                next = end;
            }

            var offset = OffsetMinutes(zone, next);
            if (offset != previous)
            {
                transitions.Add((FirstInstantWithOffset(zone, cursor, next, offset), previous, offset));
                previous = offset;
            }

            cursor = next;
        }

        if (transitions.Count == 0)
        {
            return LocalClockWindow.FixedOffset(windowEndUtc, previous);
        }

        if (transitions.Count > 1)
        {
            NoteOnce(
                "steps:" + timeZoneId,
                string.Create(CultureInfo.InvariantCulture,
                    $"[BaselineLocalClock] zone '{timeZoneId}' changed offset {transitions.Count} times inside one {BaselineMath.BaselineWindowDays}-day baseline window; keying on the newest change ({transitions[^1].AtUtc:yyyy-MM-dd HH:mm:ss}Z, {FormatOffset(transitions[^1].Before)} → {FormatOffset(transitions[^1].After)}) — rows before the earlier change are off by that earlier shift."));
        }

        var step = transitions[^1];
        return new LocalClockWindow(DateTime.SpecifyKind(step.AtUtc, DateTimeKind.Unspecified), step.Before, step.After);
    }

    /// <summary>
    /// Bisects <c>(before, after]</c> — <paramref name="before"/> still has the old offset, <paramref name="after"/>
    /// already has <paramref name="newOffset"/> — down to one second and returns the first instant with the new
    /// offset. Consistent with the SQL's <c>collection_time &lt; $4 THEN before</c>: a row stamped exactly at the
    /// transition is on the new side, as <see cref="TimeZoneInfo.GetUtcOffset(DateTime)"/> also says it is.
    /// </summary>
    private static DateTime FirstInstantWithOffset(TimeZoneInfo zone, DateTime before, DateTime after, int newOffset)
    {
        while (after - before > TimeSpan.FromSeconds(1))
        {
            var mid = before + TimeSpan.FromTicks((after - before).Ticks / 2);
            if (OffsetMinutes(zone, mid) == newOffset)
            {
                after = mid;
            }
            else
            {
                before = mid;
            }
        }

        return after;
    }

    private static int OffsetMinutes(TimeZoneInfo zone, DateTime utc) => (int)zone.GetUtcOffset(utc).TotalMinutes;

    private static TimeZoneInfo? TryFindZone(string id)
    {
        try
        {
            return TimeZoneInfo.FindSystemTimeZoneById(id);
        }
        catch (TimeZoneNotFoundException)
        {
            return null;
        }
        catch (InvalidTimeZoneException)
        {
            return null;
        }
    }

    private void NoteOnce(string key, string message)
    {
        if (_log is not null && _noted.TryAdd(key, true))
        {
            _log(message);
        }
    }

    private static string FormatOffset(int minutes)
    {
        var sign = minutes < 0 ? "-" : "+";
        var magnitude = Math.Abs(minutes);
        return string.Create(CultureInfo.InvariantCulture, $"UTC offset {sign}{magnitude / 60:00}:{magnitude % 60:00}");
    }
}

/// <summary>
/// One baseline window's clock as the SQL binds it and the lookup reads it: <c>$4</c>, <c>$5</c>, <c>$6</c> of
/// <see cref="BaselineLocalClock.LocalCollectionTimeSql"/>. <see cref="TransitionAtUtc"/> is naive UTC
/// (Kind Unspecified, so Npgsql maps it to <c>timestamp</c> like the window bounds). A window with no step
/// carries the window end as its transition and equal offsets — the CASE then never matters, which is the point.
/// </summary>
public sealed record LocalClockWindow(DateTime TransitionAtUtc, int OffsetBeforeMinutes, int OffsetAfterMinutes)
{
    /// <summary>No step in the window: the same offset everywhere. Offset 0 is UTC keying — the pre-Q6 behaviour.</summary>
    public static LocalClockWindow FixedOffset(DateTime windowEndUtc, int offsetMinutes)
        => new(DateTime.SpecifyKind(windowEndUtc, DateTimeKind.Unspecified), offsetMinutes, offsetMinutes);

    /// <summary>UTC keying, for a metric with no baseline query and for a provider that has no clock to read.</summary>
    public static LocalClockWindow Utc(DateTime windowEndUtc) => FixedOffset(windowEndUtc, 0);

    /// <summary>Whether the window straddles an offset change — the case the fixed offset got wrong.</summary>
    public bool HasTransition => OffsetBeforeMinutes != OffsetAfterMinutes;

    /// <summary>The offset the SQL applies to a row stamped at <paramref name="utc"/>: the CASE, in C#.</summary>
    public int OffsetMinutesAt(DateTime utc) => utc < TransitionAtUtc ? OffsetBeforeMinutes : OffsetAfterMinutes;

    /// <summary>The row's local wall-clock time, Kind Unspecified — exactly what the SQL expression yields.</summary>
    public DateTime ToLocal(DateTime utc)
        => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified).AddMinutes(OffsetMinutesAt(utc));

    /// <summary>
    /// The bucket key for an instant: local hour and local day-of-week (Sunday 0, the <c>EXTRACT(DOW)</c> and
    /// <see cref="DayOfWeek"/> convention both engines share). The day is the LOCAL date's — Wednesday 03:00 UTC
    /// at UTC−5 is Tuesday 22h — which is what makes this the same key the scaffold's <c>EXTRACT(DOW FROM …)</c>
    /// over the shifted time produces.
    /// </summary>
    public (int HourOfDay, int DayOfWeek) LocalKey(DateTime utc)
    {
        var local = ToLocal(utc);
        return (local.Hour, (int)local.DayOfWeek);
    }
}
