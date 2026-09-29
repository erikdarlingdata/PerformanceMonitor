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
    private static volatile ServerClock _serverClock =
        ServerClock.FixedOffset((int)TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow).TotalMinutes);

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

    /// <summary>Naive UTC to the wall clock of an explicit fixed offset.</summary>
    public static DateTime ToServerTime(DateTime utcTime, int utcOffsetMinutes) =>
        ToServerTime(utcTime, ServerClock.FixedOffset(utcOffsetMinutes));

    /// <summary>Naive UTC to the wall clock of an explicit server clock.</summary>
    public static DateTime ToServerTime(DateTime utcTime, ServerClock clock) => clock.ToServerLocal(utcTime);

    /// <summary>
    /// The naive-UTC instant of a wall-clock time on the active server's clock. Never throws: a skipped local
    /// time moves forward by the gap and a repeated one takes its first occurrence (<see cref="ServerClock.ToUtc"/>).
    /// </summary>
    public static DateTime ServerTimeToUtc(DateTime serverTime) => _serverClock.ToUtc(serverTime);

    /// <summary>
    /// Converts a local DateTime (from date picker) to server time.
    /// Use when the user picks dates in their local timezone but the database stores server time.
    /// </summary>
    private static DateTime LocalToServerTime(DateTime localTime, ServerClock clock)
    {
        var utcTime = localTime.ToUniversalTime();
        return clock.ToServerLocal(utcTime);
    }

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
    /// Converts a display-mode DateTime back to server time. Reverse of ConvertForDisplay.
    /// </summary>
    public static DateTime DisplayTimeToServerTime(DateTime displayTime, TimeDisplayMode mode) =>
        DisplayTimeToServerTime(displayTime, mode, _serverClock);

    /// <summary>
    /// Converts a display-mode DateTime back to the local time of a NAMED server, rather than of
    /// whichever server the desktop currently has selected.
    ///
    /// <para>For a caller that then hands the result to a read windowing on that same server: the read
    /// converts server time back out to UTC through the server's clock, so the clock behind this conversion
    /// has to be the read's. This overload is the fixed-offset clock (<see cref="ServerClock.FixedOffset"/>):
    /// right for a server that reported no time zone id, one shift for every date. A server that follows a
    /// time zone needs the <see cref="ServerClock"/> overload below, or a range across a daylight saving
    /// change comes back an hour off at one bound. Under <c>TimeDisplayMode.UTC</c> and <c>LocalTime</c> this
    /// conversion and the read's cancel each other and the window survives unchanged; under
    /// <c>ServerTime</c>, the default, this conversion is the identity and only the read's applies. Two
    /// different servers' clocks across the pair therefore skew the window in every mode, not just the
    /// default.</para>
    /// </summary>
    public static DateTime DisplayTimeToServerTime(DateTime displayTime, TimeDisplayMode mode, int utcOffsetMinutes) =>
        DisplayTimeToServerTime(displayTime, mode, ServerClock.FixedOffset(utcOffsetMinutes));

    /// <summary>
    /// <see cref="DisplayTimeToServerTime(DateTime, TimeDisplayMode, int)"/> for an explicit server clock: the
    /// pure core. A picker value is the user's wall clock in the display mode, so under UTC and Local the
    /// answer is the server's wall clock at that instant (<see cref="ServerClock.ToServerLocal"/>). Server mode
    /// is the identity here, and the read that windows on the result converts it to UTC with
    /// <see cref="ServerClock.ToUtc"/>, which resolves a skipped or repeated server hour without throwing.
    /// </summary>
    public static DateTime DisplayTimeToServerTime(DateTime displayTime, TimeDisplayMode mode, ServerClock clock) => mode switch
    {
        TimeDisplayMode.LocalTime => LocalToServerTime(displayTime, clock),
        TimeDisplayMode.UTC => clock.ToServerLocal(displayTime),
        _ => displayTime
    };

    /// <summary>
    /// Returns a short timezone label for the current display mode.
    /// </summary>
    public static string GetTimezoneLabel(TimeDisplayMode mode) => mode switch
    {
        TimeDisplayMode.LocalTime => TimeZoneInfo.Local.StandardName,
        TimeDisplayMode.UTC => "UTC",
        _ => OffsetLabel(UtcOffsetMinutes)
    };

    private static string OffsetLabel(int utcOffsetMinutes) =>
        $"UTC{(utcOffsetMinutes >= 0 ? "+" : "")}{utcOffsetMinutes / 60}:{Math.Abs(utcOffsetMinutes % 60):D2}";

    /// <summary>
    /// Formats a NAIVE-UTC timestamp for display. The offset add converts UTC to the server's own clock,
    /// which is <see cref="ConvertForDisplay"/>'s input; use <see cref="FormatServerClock"/> instead for a
    /// value that is already the server's clock.
    /// </summary>
    public static string FormatServerTime(DateTime utcTime, string format = "yyyy-MM-dd HH:mm:ss")
        => ConvertForDisplay(ToServerTime(utcTime), CurrentDisplayMode).ToString(format);

    /// <inheritdoc cref="FormatServerTime(DateTime, string)"/>
    public static string FormatServerTime(DateTime? utcTime, string format = "yyyy-MM-dd HH:mm:ss")
        => utcTime.HasValue ? ConvertForDisplay(ToServerTime(utcTime.Value), CurrentDisplayMode).ToString(format) : "";

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
    /// display mode, so this is <see cref="FormatServerTime"/> with the offset add removed rather than a
    /// second conversion. That add is the whole defect it exists to prevent: it is correct for naive UTC
    /// and, applied to a value already in the server's frame, skews it by the collected offset a second
    /// time — the fleet's -240 renders four hours early, in every display mode.</para>
    ///
    /// <para>Darling's <c>ViewerDataService.FormatServerClock</c> is the mirror of this, under the same
    /// name and with the same input contract, reached the other way round: its
    /// <c>ViewerTimeHelper.ConvertToDisplay</c> takes naive UTC, so the server-clock conversion there
    /// goes to UTC through the server's clock first (<c>ServerClock.ToUtc</c>), where this one starts from the
    /// server's clock and the naive-UTC renderer converts into it. Either way there is exactly one conversion
    /// through the server's clock between the two frames, and both renderers honour the display preference.</para>
    /// </summary>
    public static string FormatServerClock(DateTime serverLocal, string format = "yyyy-MM-dd HH:mm:ss")
        => ConvertForDisplay(serverLocal, CurrentDisplayMode).ToString(format);

    /// <inheritdoc cref="FormatServerClock(DateTime, string)"/>
    public static string FormatServerClock(DateTime? serverLocal, string format = "yyyy-MM-dd HH:mm:ss")
        => serverLocal.HasValue ? ConvertForDisplay(serverLocal.Value, CurrentDisplayMode).ToString(format) : "";
}
