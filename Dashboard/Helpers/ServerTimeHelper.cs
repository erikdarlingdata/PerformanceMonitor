/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitorDashboard.Helpers
{
    /// <summary>
    /// Holds the connected server's UTC offset so chart axis bounds
    /// can use server-local "now" instead of client-local "now".
    /// Set when a server tab is opened; defaults to local offset.
    /// </summary>
    public static class ServerTimeHelper
    {
        private static volatile int _utcOffsetMinutes = (int)TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow).TotalMinutes;

        public static int UtcOffsetMinutes
        {
            get => _utcOffsetMinutes;
            set => _utcOffsetMinutes = value;
        }

        /// <summary>
        /// Returns the current time in the server's timezone.
        /// Use this instead of DateTime.Now for chart axis range bounds.
        /// </summary>
        public static DateTime ServerNow => DateTime.UtcNow.AddMinutes(_utcOffsetMinutes);

        /// <summary>
        /// Converts a local DateTime to server time.
        /// Use this when the user picks dates in the UI (which are in local time)
        /// but the database stores timestamps in server time.
        /// </summary>
        public static DateTime ToServerTime(DateTime localTime)
        {
            /* Convert local to UTC, then apply server offset */
            var utcTime = localTime.ToUniversalTime();
            return utcTime.AddMinutes(_utcOffsetMinutes);
        }

        /// <summary>
        /// Converts a server DateTime to local time.
        /// Use this when displaying server timestamps to the user in the UI.
        /// </summary>
        public static DateTime ToLocalTime(DateTime serverTime)
        {
            /* Convert server time to UTC, then to local */
            var utcTime = serverTime.AddMinutes(-_utcOffsetMinutes);
            return utcTime.ToLocalTime();
        }

        /// <summary>
        /// The current display mode preference. Read from UserPreferences at startup,
        /// updated when the user changes the setting.
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
        /// Returns a short timezone label for the current display mode (e.g., "UTC", "PST", "UTC-8:00").
        /// </summary>
        public static string GetTimezoneLabel(TimeDisplayMode mode) => mode switch
        {
            TimeDisplayMode.LocalTime => TimeZoneInfo.Local.StandardName,
            TimeDisplayMode.UTC => "UTC",
            _ => $"UTC{(_utcOffsetMinutes >= 0 ? "+" : "")}{_utcOffsetMinutes / 60}:{Math.Abs(_utcOffsetMinutes % 60):D2}"
        };
    }
}
