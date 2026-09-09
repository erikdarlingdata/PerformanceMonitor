/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Holds the connected server's UTC offset so model display properties
/// can convert UTC timestamps to server-local time without per-instance wiring.
/// Set by ServerTab on creation; defaults to local offset for backwards compatibility.
/// </summary>
public static class ServerTimeHelper
{
    private static int _utcOffsetMinutes = (int)TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow).TotalMinutes;

    public static int UtcOffsetMinutes
    {
        get => _utcOffsetMinutes;
        set => _utcOffsetMinutes = value;
    }

    public static DateTime ToServerTime(DateTime utcTime) => utcTime.AddMinutes(_utcOffsetMinutes);

    /// <summary>
    /// Converts a local DateTime (from date picker) to server time.
    /// Use when the user picks dates in their local timezone but the database stores server time.
    /// </summary>
    private static DateTime LocalToServerTime(DateTime localTime) =>
        LocalToServerTime(localTime, _utcOffsetMinutes);

    private static DateTime LocalToServerTime(DateTime localTime, int utcOffsetMinutes)
    {
        var utcTime = localTime.ToUniversalTime();
        return utcTime.AddMinutes(utcOffsetMinutes);
    }

    /// <summary>
    /// Converts a server DateTime to local time.
    /// Use this when displaying server timestamps to the user in the UI.
    /// </summary>
    private static DateTime ToLocalTime(DateTime serverTime)
    {
        /* Convert server time to UTC, then to local */
        var utcTime = serverTime.AddMinutes(-_utcOffsetMinutes);
        return utcTime.ToLocalTime();
    }

    /// <summary>
    /// The current display mode preference. Read from App settings at startup.
    /// </summary>
    public static TimeDisplayMode CurrentDisplayMode { get; set; } = TimeDisplayMode.ServerTime;

    /// <summary>
    /// Converts a server DateTime for display based on the selected display mode.
    /// </summary>
    public static DateTime ConvertForDisplay(DateTime serverTime, TimeDisplayMode mode) => mode switch
    {
        TimeDisplayMode.LocalTime => ToLocalTime(serverTime),
        TimeDisplayMode.UTC => serverTime.AddMinutes(-_utcOffsetMinutes),
        _ => serverTime
    };

    /// <summary>
    /// Converts a display-mode DateTime back to server time. Reverse of ConvertForDisplay.
    /// </summary>
    public static DateTime DisplayTimeToServerTime(DateTime displayTime, TimeDisplayMode mode) =>
        DisplayTimeToServerTime(displayTime, mode, _utcOffsetMinutes);

    /// <summary>
    /// Converts a display-mode DateTime back to the local time of a NAMED server, rather than of
    /// whichever server the desktop currently has selected.
    ///
    /// <para>For a caller that then hands the result to a read windowing on that same server: the read
    /// converts server time back out to UTC with the server's offset, so the offset given here has to be
    /// the one the read will use. Under <c>TimeDisplayMode.UTC</c> and <c>LocalTime</c> this conversion
    /// and the read's cancel each other and the window survives unchanged; under <c>ServerTime</c>, the
    /// default, this conversion is the identity and only the read's applies. Two different servers'
    /// offsets across the pair therefore skews the window in every mode, not just the default.</para>
    /// </summary>
    public static DateTime DisplayTimeToServerTime(DateTime displayTime, TimeDisplayMode mode, int utcOffsetMinutes) => mode switch
    {
        TimeDisplayMode.LocalTime => LocalToServerTime(displayTime, utcOffsetMinutes),
        TimeDisplayMode.UTC => displayTime.AddMinutes(utcOffsetMinutes),
        _ => displayTime
    };

    /// <summary>
    /// Returns a short timezone label for the current display mode.
    /// </summary>
    public static string GetTimezoneLabel(TimeDisplayMode mode) => mode switch
    {
        TimeDisplayMode.LocalTime => TimeZoneInfo.Local.StandardName,
        TimeDisplayMode.UTC => "UTC",
        _ => $"UTC{(_utcOffsetMinutes >= 0 ? "+" : "")}{_utcOffsetMinutes / 60}:{Math.Abs(_utcOffsetMinutes % 60):D2}"
    };

    /// <summary>
    /// Formats a NAIVE-UTC timestamp for display. The offset add converts UTC to the server's own clock,
    /// which is <see cref="ConvertForDisplay"/>'s input; use <see cref="FormatServerClock"/> instead for a
    /// value that is already the server's clock.
    /// </summary>
    public static string FormatServerTime(DateTime utcTime, string format = "yyyy-MM-dd HH:mm:ss")
        => ConvertForDisplay(utcTime.AddMinutes(_utcOffsetMinutes), CurrentDisplayMode).ToString(format);

    /// <inheritdoc cref="FormatServerTime(DateTime, string)"/>
    public static string FormatServerTime(DateTime? utcTime, string format = "yyyy-MM-dd HH:mm:ss")
        => utcTime.HasValue ? ConvertForDisplay(utcTime.Value.AddMinutes(_utcOffsetMinutes), CurrentDisplayMode).ToString(format) : "";

    /// <summary>
    /// Formats a timestamp that is ALREADY the monitored server's own wall clock: the
    /// <c>sys.dm_exec_*</c> family (<c>creation_time</c>, <c>cached_time</c>, <c>last_execution_time</c>),
    /// <c>plan_correction</c>'s recommendation and action stamps, the blocked-process report's
    /// <c>*_last_tran_started</c> / <c>*_last_batch_*</c> attributes, <c>query_snapshots.tran_start_time</c>
    /// and msdb Agent's <c>start_execution_date</c>.
    ///
    /// <para><see cref="ConvertForDisplay"/> already takes the server's clock and honours the selected
    /// display mode, so this is <see cref="FormatServerTime"/> with the offset add removed rather than a
    /// second conversion. That add is the whole defect it exists to prevent: it is correct for naive UTC
    /// and, applied to a value already in the server's frame, skews it by the collected offset a second
    /// time — the fleet's -240 renders four hours early, in every display mode.</para>
    ///
    /// <para>Darling's <c>ViewerDataService.FormatServerClock</c> takes the same frame under the same name.
    /// The INPUT CONTRACT is the shared fact; the mode handling is not — that renderer emits the server's
    /// clock verbatim in all three modes, because <c>ViewerTimeHelper</c> has no server-local arm to route
    /// through. Do not read the shared name as shared behaviour.</para>
    /// </summary>
    public static string FormatServerClock(DateTime serverLocal, string format = "yyyy-MM-dd HH:mm:ss")
        => ConvertForDisplay(serverLocal, CurrentDisplayMode).ToString(format);

    /// <inheritdoc cref="FormatServerClock(DateTime, string)"/>
    public static string FormatServerClock(DateTime? serverLocal, string format = "yyyy-MM-dd HH:mm:ss")
        => serverLocal.HasValue ? ConvertForDisplay(serverLocal.Value, CurrentDisplayMode).ToString(format) : "";
}
