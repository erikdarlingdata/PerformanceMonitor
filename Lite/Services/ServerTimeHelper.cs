/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Holds the connected server's clock so model display properties
/// can convert UTC timestamps to server-local time without per-instance wiring.
/// Set by ServerTab on creation and on tab selection; defaults to the local offset for backwards compatibility.
///
/// <para>The clock is a <see cref="ServerClock"/> (#4766): the server's time zone where one was collected
/// (<c>server_properties.time_zone_id</c>, SQL Server 2022 and later), else its fixed UTC offset. This class
/// used to hold ONE offset and add it to every time, so a time on the far side of a daylight-saving change
/// showed an hour off the server's wall clock. Every conversion below routes through the clock, so the offset
/// follows the date; the overloads that take an <c>int</c> offset stay, delegating to a fixed-offset clock, for
/// a caller that names a server's offset itself.</para>
/// </summary>
public static class ServerTimeHelper
{
    private static volatile ServerClock _serverClock = MachineClock(TimeZoneInfo.Local, DateTime.UtcNow);

    /// <summary>
    /// The clock of <paramref name="machine"/> at <paramref name="utcNow"/>: a fixed offset, the machine's UTC
    /// offset at that instant. What the active clock starts as, and what a server with no clock of its own is shown
    /// in (<see cref="ClockForServer(ServerClock?, ServerClock?)"/>). The zone and the instant come in as arguments
    /// so a test names them instead of depending on the zone of the machine it runs on.
    /// </summary>
    private static ServerClock MachineClock(TimeZoneInfo machine, DateTime utcNow) =>
        ServerClock.FixedOffset((int)machine.GetUtcOffset(DateTime.SpecifyKind(utcNow, DateTimeKind.Utc)).TotalMinutes);

    /// <summary>The connected (selected) server's clock. Setting <c>null</c> installs UTC.</summary>
    public static ServerClock ActiveServerClock
    {
        get => _serverClock;
        set => _serverClock = value ?? ServerClock.Utc;
    }

    /// <summary>The active server's UTC offset in minutes right now (UTC + this = the server's local time).
    /// Setting it installs a fixed-offset clock, which is what a server with no time zone id gets.</summary>
    public static int UtcOffsetMinutes
    {
        get => _serverClock.OffsetMinutesAt(DateTime.UtcNow);
        set => _serverClock = ServerClock.FixedOffset(value);
    }

    /// <summary>Naive UTC to the active server's wall clock.</summary>
    public static DateTime ToServerTime(DateTime utcTime) => _serverClock.ToServerLocal(utcTime);

    /// <summary>Naive UTC to the wall clock of an explicit server clock.</summary>
    public static DateTime ToServerTime(DateTime utcTime, ServerClock clock) => clock.ToServerLocal(utcTime);

    /// <summary>
    /// Converts a server DateTime to local time.
    /// Use this when displaying server timestamps to the user in the UI.
    /// </summary>
    private static DateTime ToLocalTime(DateTime serverTime, ServerClock clock)
    {
        /* Convert server time to UTC, then to local */
        var utcTime = DateTime.SpecifyKind(clock.ToUtc(serverTime), DateTimeKind.Utc);
        return utcTime.ToLocalTime();
    }

    /// <summary>
    /// The current display mode preference. Read from App settings at startup.
    /// </summary>
    public static TimeDisplayMode CurrentDisplayMode { get; set; } = TimeDisplayMode.ServerTime;

    /// <summary>
    /// Converts a server DateTime for display based on the selected display mode.
    /// </summary>
    public static DateTime ConvertForDisplay(DateTime serverTime, TimeDisplayMode mode) =>
        ConvertForDisplay(serverTime, mode, _serverClock);

    /// <summary>
    /// <see cref="ConvertForDisplay(DateTime, TimeDisplayMode)"/> for an explicit server clock: the pure core,
    /// which a test drives without touching the process-wide clock. Server mode is the value itself (it is
    /// already in that frame); the other two go through <see cref="ServerClock.ToUtc"/>, which never throws on
    /// a skipped or repeated hour.
    /// </summary>
    public static DateTime ConvertForDisplay(DateTime serverTime, TimeDisplayMode mode, ServerClock clock) => mode switch
    {
        TimeDisplayMode.LocalTime => ToLocalTime(serverTime, clock),
        TimeDisplayMode.UTC => clock.ToUtc(serverTime),
        _ => serverTime
    };

    /// <summary>
    /// The zone a time is shown in for <paramref name="mode"/> (#4766): UTC, this machine's zone, or the server's own
    /// clock (<paramref name="clock"/>: its time zone where one was collected, else its fixed offset). Pure, so a
    /// caller that holds its own server's clock (a server tab that is not the selected one, an alert row) names it
    /// instead of reading the active one. <c>DisplayZone.ToDisplay</c> turns a naive-UTC instant into the wall
    /// clock of the zone this returns.
    /// </summary>
    internal static TimeZoneInfo DisplayZoneFor(TimeDisplayMode mode, ServerClock clock) => mode switch
    {
        TimeDisplayMode.UTC => TimeZoneInfo.Utc,
        TimeDisplayMode.LocalTime => TimeZoneInfo.Local,
        _ => clock.AsTimeZone()
    };

    /// <summary>
    /// The clock one server's rows are shown on (#4766), in the order the sources are trusted: the server's own
    /// collected clock (<paramref name="collected"/>, from <c>server_properties</c>), else the clock of the server's
    /// open tab (<paramref name="openTabClock"/>; a tab keeps the fixed offset its connect probe read until the first
    /// collected row arrives, so that is what the server's own tab shows for it), else this machine's own offset
    /// right now, which is what the active clock starts as before any server has been selected. The machine comes
    /// last, never UTC: a list that put a server with no clock of its own in UTC would disagree with every other
    /// list beside it in Server mode. The MCP tools keep their own stated UTC fallback
    /// (<c>McpServerLocalWindow.ClockForAsync</c>); this is the desktop's and is separate on purpose. Never the ACTIVE
    /// tab's clock in place of the row's own server's: another server's zone would put the row's time an hour or
    /// more off its own server's clock.
    /// </summary>
    internal static ServerClock ClockForServer(ServerClock? collected, ServerClock? openTabClock) =>
        ClockForServer(collected, openTabClock, TimeZoneInfo.Local, DateTime.UtcNow);

    /// <inheritdoc cref="ClockForServer(ServerClock?, ServerClock?)"/>
    /// <param name="machine">This machine's zone; a test names one instead of depending on the zone it runs in.</param>
    /// <param name="utcNow">The instant the machine's offset is read at.</param>
    internal static ServerClock ClockForServer(
        ServerClock? collected, ServerClock? openTabClock, TimeZoneInfo machine, DateTime utcNow) =>
        collected ?? openTabClock ?? MachineClock(machine, utcNow);

    /// <summary>
    /// Returns a short timezone label for the current display mode, on the active server's clock.
    /// </summary>
    public static string GetTimezoneLabel(TimeDisplayMode mode) => GetTimezoneLabel(mode, _serverClock);

    /// <summary>
    /// A short label for the zone <paramref name="mode"/> shows times in (#4766): "UTC", this machine's zone name,
    /// or the server's. The server's is its zone's name (for example "Eastern Standard Time") when the clock has a
    /// zone whose offset changes through the year, else its fixed offset (for example "UTC-5:00"). The offset in
    /// force NOW would name the wrong one for a time on the far side of a daylight-saving change, which is what
    /// the zone's name does not do; where the offset never changes (a fixed-offset clock, UTC, a zone with no
    /// daylight saving) it is exact and shorter than the name, which is what the header's "Last refresh" line needs.
    /// </summary>
    public static string GetTimezoneLabel(TimeDisplayMode mode, ServerClock clock) => mode switch
    {
        TimeDisplayMode.LocalTime => TimeZoneInfo.Local.StandardName,
        TimeDisplayMode.UTC => "UTC",
        _ => ServerZoneLabel(clock)
    };

    private static string ServerZoneLabel(ServerClock clock)
    {
        var zone = clock.AsTimeZone();
        return zone.SupportsDaylightSavingTime
            ? zone.StandardName
            : OffsetLabel(clock.OffsetMinutesAt(DateTime.UtcNow));
    }

    private static string OffsetLabel(int utcOffsetMinutes)
    {
        var magnitude = Math.Abs(utcOffsetMinutes);
        return $"UTC{(utcOffsetMinutes < 0 ? "-" : "+")}{magnitude / 60}:{magnitude % 60:D2}";
    }

    /// <summary>
    /// Formats a NAIVE-UTC timestamp for display (#4766): the instant, in the zone of the selected display mode on
    /// the active server's clock (<see cref="DisplayZoneFor"/>). One conversion from the instant to that zone's wall
    /// clock, so UTC mode shows the stored instant itself and a time in the hour that repeats after a fall-back reads
    /// the wall time of its own offset in every mode. The path used to go to the server's wall clock first and back
    /// out of it, and a wall clock cannot say which occurrence of the repeated hour it was: 06:30Z on 2026-11-01 on a
    /// US Eastern server read 05:30 in UTC mode. Use <see cref="FormatServerClock"/> instead for a value that is
    /// already the server's clock.
    /// </summary>
    public static string FormatServerTime(DateTime utcTime, string format = "yyyy-MM-dd HH:mm:ss")
        => DisplayZone.ToDisplay(utcTime, DisplayZoneFor(CurrentDisplayMode, _serverClock)).ToString(format);

    /// <inheritdoc cref="FormatServerTime(DateTime, string)"/>
    public static string FormatServerTime(DateTime? utcTime, string format = "yyyy-MM-dd HH:mm:ss")
        => utcTime.HasValue
            ? DisplayZone.ToDisplay(utcTime.Value, DisplayZoneFor(CurrentDisplayMode, _serverClock)).ToString(format)
            : "";

    /// <summary>
    /// Formats a timestamp that is ALREADY the monitored server's own wall clock: the
    /// <c>sys.dm_exec_*</c> family (<c>creation_time</c>, <c>cached_time</c>, <c>last_execution_time</c>),
    /// the blocked-process report's <c>*_last_tran_started</c> / <c>*_last_batch_*</c> attributes,
    /// <c>query_snapshots.tran_start_time</c>, the ADR cleaner times and msdb Agent's
    /// <c>start_execution_date</c>.
    ///
    /// <para><c>plan_correction</c>'s recommendation and action stamps are deliberately NOT in that
    /// list: <c>sys.dm_db_tuning_recommendations</c> reports them in UTC, so they take
    /// <see cref="FormatServerTime"/> instead. The frame is measured against the collector-written UTC
    /// <c>collection_time</c> rather than inferred from the collector shipping the DMV verbatim, and
    /// naming the exception here is the point — this list is what a reader consults to pick a
    /// renderer.</para>
    ///
    /// <para><see cref="ConvertForDisplay"/> already takes the server's clock and honours the selected
    /// display mode, so this is that conversion of the value as it stands, not a second one on top of the
    /// naive-UTC renderer's. <see cref="FormatServerTime"/> reads its input as an instant and shifts it into the
    /// display zone; that shift is correct for naive UTC and, applied to a value already in the server's frame,
    /// skews it by the collected offset a second time — the fleet's -240 renders four hours early, in every
    /// display mode. That is the defect this renderer exists to prevent.</para>
    ///
    /// <para>Darling's <c>ViewerDataService.FormatServerClock</c> is the mirror of this, under the same
    /// name and with the same input contract, reached the other way round: its
    /// <c>ViewerTimeHelper.ConvertToDisplay</c> takes naive UTC, so the server-clock conversion there
    /// goes to UTC through the server's clock first (<c>ServerClock.ToUtc</c>), where this one starts from the
    /// server's clock and the naive-UTC renderer converts the instant into the display zone. Either way there is
    /// exactly one conversion through the server's clock between the two frames, and both renderers honour the
    /// display preference.</para>
    /// </summary>
    public static string FormatServerClock(DateTime serverLocal, string format = "yyyy-MM-dd HH:mm:ss")
        => ConvertForDisplay(serverLocal, CurrentDisplayMode).ToString(format);

    /// <inheritdoc cref="FormatServerClock(DateTime, string)"/>
    public static string FormatServerClock(DateTime? serverLocal, string format = "yyyy-MM-dd HH:mm:ss")
        => serverLocal.HasValue ? ConvertForDisplay(serverLocal.Value, CurrentDisplayMode).ToString(format) : "";
}
