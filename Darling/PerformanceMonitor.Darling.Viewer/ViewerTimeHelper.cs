/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's port of Lite's <c>ServerTimeHelper</c>: the single chokepoint every rendered timestamp
/// routes through, converting a stored value to the user's chosen <see cref="TimeDisplayMode"/>. It
/// replaces the viewer's old fixed machine-local conversion (the former <c>ViewerDataService.ToLocalTime</c>,
/// which every call site now reaches as <see cref="ForDisplay"/>).
///
/// <para>
/// The Darling store is naive-UTC (every collected <c>timestamp</c> column is UTC with
/// <see cref="DateTimeKind.Unspecified"/>), so the three modes map on as:
/// <list type="bullet">
///   <item><b>UTC</b> — the raw stored value.</item>
///   <item><b>Local</b> — the viewer machine's local time (<c>SpecifyKind(Utc).ToLocalTime()</c>, the
///   viewer's historical one-and-only convention).</item>
///   <item><b>Server</b> — the monitored server's own local time = UTC + the server's UTC offset
///   (<c>server_properties.utc_offset_minutes</c>, collected because a headless viewer can't query the
///   target live the way Lite does at connect).</item>
/// </list>
/// </para>
///
/// <para>
/// Process-wide statics mirroring <c>ServerTimeHelper</c>: <see cref="CurrentDisplayMode"/> is the global
/// user preference (persisted in <see cref="ViewerAppSettings.TimeDisplayMode"/>), and
/// <see cref="UtcOffsetMinutes"/> is set by the active <see cref="ViewerServerTab"/> to ITS server's
/// offset before that tab renders — only the visible tab renders (the viewer's visible-only rule), so the
/// visible tab's offset always wins. Charts pre-convert their X values through <see cref="ForDisplay"/>,
/// so <see cref="UiTimeContext.ConvertForDisplay"/> is deliberately left at its identity default in the
/// viewer (wiring it would double-convert the already-display-time chart X on hover/crosshair).
/// </para>
/// </summary>
public static class ViewerTimeHelper
{
    /// <summary>Seeds to the viewer machine's offset so Server mode degrades gracefully to ~Local until a
    /// server's <c>utc_offset_minutes</c> has been collected.</summary>
    private static int _utcOffsetMinutes = (int)TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow).TotalMinutes;

    /// <summary>The active server's UTC offset in minutes (UTC + this = the server's local time).</summary>
    public static int UtcOffsetMinutes
    {
        get => _utcOffsetMinutes;
        set => _utcOffsetMinutes = value;
    }

    /// <summary>The global timestamp display mode; the default (Server-time) mirrors Lite's default.</summary>
    public static TimeDisplayMode CurrentDisplayMode { get; set; } = TimeDisplayMode.ServerTime;

    /// <summary>
    /// Converts a stored naive-UTC timestamp to the current display mode + active server offset — the one
    /// method every timestamp render routes through. Single overload on purpose so the many
    /// <c>&lt;see cref="ViewerTimeHelper.ForDisplay"/&gt;</c> doc references stay unambiguous.
    /// </summary>
    public static DateTime ForDisplay(DateTime naiveUtc) => ConvertToDisplay(naiveUtc, CurrentDisplayMode, _utcOffsetMinutes);

    /// <summary>
    /// Pure (static-free) naive-UTC → display conversion for an explicit mode + offset. The testable core
    /// of <see cref="ForDisplay"/>; unit tests exercise it without mutating the process-wide statics.
    /// </summary>
    public static DateTime ConvertToDisplay(DateTime naiveUtc, TimeDisplayMode mode, int utcOffsetMinutes) => mode switch
    {
        TimeDisplayMode.LocalTime => DateTime.SpecifyKind(naiveUtc, DateTimeKind.Utc).ToLocalTime(),
        TimeDisplayMode.ServerTime => naiveUtc.AddMinutes(utcOffsetMinutes),
        _ => naiveUtc, /* UTC — the store is already naive UTC */
    };

    /// <summary>
    /// Converts a stored SERVER-CLOCK timestamp — one already in the monitored server's own frame, which
    /// is what the <c>sys.dm_exec_*</c> family, <c>plan_correction</c>'s action stamps, msdb Agent's job
    /// start time and the blocked-process report's attributes hold — to the current display mode.
    ///
    /// <para>It composes out of <see cref="ConvertToDisplay"/>'s existing arms rather than adding new
    /// ones: the naive-UTC twin of a server-clock value is <c>serverLocal - offset</c>, so subtracting
    /// the offset first is the whole difference between the two conversions. Nothing is re-derived — the
    /// Local arm in particular is the same one <see cref="ForDisplay"/> uses, reached with a corrected
    /// input.</para>
    ///
    /// <para>This is the mirror of Lite's pair, which starts from the other frame:
    /// <c>ServerTimeHelper.ConvertForDisplay</c> takes the server's clock and its naive-UTC renderer
    /// ADDS the offset first. Either way there is exactly one offset step between the two frames, and
    /// applying it twice or not at all is the whole of #3207.</para>
    /// </summary>
    public static DateTime ForServerClockDisplay(DateTime serverLocal) =>
        ConvertServerClockToDisplay(serverLocal, CurrentDisplayMode, _utcOffsetMinutes);

    /// <summary>Pure (static-free) server-clock → display conversion for an explicit mode + offset. The
    /// testable core of <see cref="ForServerClockDisplay"/>.</summary>
    public static DateTime ConvertServerClockToDisplay(DateTime serverLocal, TimeDisplayMode mode, int utcOffsetMinutes) =>
        ConvertToDisplay(serverLocal.AddMinutes(-utcOffsetMinutes), mode, utcOffsetMinutes);

    /// <summary>
    /// Inverse of <see cref="ForDisplay"/> for the custom-range pickers: a wall-clock value the user typed
    /// IN THE CURRENT display mode, back to the store's naive-UTC window bound.
    /// </summary>
    public static DateTime DisplayToNaiveUtc(DateTime display) => ConvertFromDisplay(display, CurrentDisplayMode, _utcOffsetMinutes);

    /// <summary>As <see cref="DisplayToNaiveUtc(DateTime)"/> but for an explicit mode (the active offset) —
    /// used when re-reading the pickers in their OLD mode during a mode switch.</summary>
    public static DateTime DisplayToNaiveUtc(DateTime display, TimeDisplayMode mode) => ConvertFromDisplay(display, mode, _utcOffsetMinutes);

    /// <summary>Pure (static-free) display → naive-UTC inverse for an explicit mode + offset.</summary>
    public static DateTime ConvertFromDisplay(DateTime display, TimeDisplayMode mode, int utcOffsetMinutes) => mode switch
    {
        TimeDisplayMode.LocalTime =>
            DateTime.SpecifyKind(DateTime.SpecifyKind(display, DateTimeKind.Local).ToUniversalTime(), DateTimeKind.Unspecified),
        TimeDisplayMode.ServerTime =>
            DateTime.SpecifyKind(display.AddMinutes(-utcOffsetMinutes), DateTimeKind.Unspecified),
        _ => DateTime.SpecifyKind(display, DateTimeKind.Unspecified), /* UTC — the picker IS naive UTC */
    };
}
