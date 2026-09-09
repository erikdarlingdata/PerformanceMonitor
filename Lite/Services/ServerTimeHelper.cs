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

    public static string FormatServerTime(DateTime utcTime, string format = "yyyy-MM-dd HH:mm:ss")
        => ConvertForDisplay(utcTime.AddMinutes(_utcOffsetMinutes), CurrentDisplayMode).ToString(format);

    public static string FormatServerTime(DateTime? utcTime, string format = "yyyy-MM-dd HH:mm:ss")
        => utcTime.HasValue ? ConvertForDisplay(utcTime.Value.AddMinutes(_utcOffsetMinutes), CurrentDisplayMode).ToString(format) : "";

    /// <summary>
    /// Renders a value that is ALREADY the monitored server's local wall clock — a DMV or XML timestamp the
    /// collector shipped verbatim — in the current display mode. The counterpart to
    /// <see cref="FormatServerTime(DateTime, string)"/>, which takes naive UTC and adds the offset to reach
    /// the server's clock; this one is already there, so it only converts OUT.
    ///
    /// <para><b>Why Lite needed a second renderer.</b> <c>FormatServerTime</c> names its parameter
    /// <c>utcTime</c> and adds the offset unconditionally, so handing it a server-local value adds the
    /// offset a second time and displays one whole offset EARLY — four hours on the production fleet's
    /// measured -240. Sixteen render sites did exactly that (#3207), because Lite had no way to say "this
    /// value is already on the server's clock". The Darling viewer has had that distinction since it split
    /// <c>ViewerTimeHelper.ForDisplay</c> from <c>ViewerDataService.FormatServerClock</c>; this closes the
    /// gap, and a doc comment in <c>ViewerDataService.QuerySnapshots</c> that claimed the two SKUs already
    /// agreed is corrected with it.</para>
    ///
    /// <para><b>It is mode-aware, unlike Darling's namesake.</b> Darling's <c>FormatServerClock</c> renders
    /// literally raw and so shows the server's clock even under UTC display mode. Routing through
    /// <see cref="ConvertForDisplay"/> instead honours the user's Server/Local/UTC choice for these columns
    /// the way every other Lite timestamp does, which is the behaviour the setting promises.</para>
    /// </summary>
    public static string FormatServerClock(DateTime serverLocalTime, string format = "yyyy-MM-dd HH:mm:ss")
        => ConvertForDisplay(serverLocalTime, CurrentDisplayMode).ToString(format);

    /// <summary>As <see cref="FormatServerClock(DateTime, string)"/>, empty for a value the collector did
    /// not capture.</summary>
    public static string FormatServerClock(DateTime? serverLocalTime, string format = "yyyy-MM-dd HH:mm:ss")
        => serverLocalTime.HasValue ? FormatServerClock(serverLocalTime.Value, format) : "";
}
