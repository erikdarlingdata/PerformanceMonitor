/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common;

/// <summary>
/// Represents the runtime connection status of a server.
/// This is transient state that is not persisted to disk.
///
/// Shared by Lite and Dashboard. A few fields are only populated by one app (Dashboard sets
/// <see cref="InstalledMonitorVersion"/>; Lite sets <see cref="SqlServerVersion"/>,
/// <see cref="SqlMajorVersion"/>, and <see cref="HasMsdbAccess"/>) — the other app simply leaves
/// them at their defaults.
/// </summary>
public class ServerConnectionStatus
{
    /// <summary>
    /// The server ID this status belongs to.
    /// </summary>
    public string ServerId { get; set; } = string.Empty;

    /// <summary>
    /// Whether the server is currently reachable.
    /// Null means status has not been checked yet.
    /// </summary>
    public bool? IsOnline { get; set; }

    /// <summary>
    /// The last time connectivity was checked.
    /// Null means status has never been checked.
    /// </summary>
    public DateTime? LastChecked { get; set; }

    /// <summary>
    /// The time when the status last changed (online to offline or vice versa).
    /// Used to show "Online since X" or "Offline for X".
    /// </summary>
    public DateTime? StatusChangedAt { get; set; }

    /// <summary>
    /// The previous online status, used to detect status changes.
    /// </summary>
    public bool? PreviousIsOnline { get; set; }

    /// <summary>
    /// Error message if the connection failed.
    /// Null if online or not yet checked.
    /// </summary>
    public string? ErrorMessage { get; set; }

    /// <summary>
    /// The SQL Server start time, queried from sys.dm_os_sys_info.
    /// Only populated when server is online.
    /// </summary>
    public DateTime? ServerStartTime { get; set; }

    /// <summary>
    /// The SQL Server version string. (Lite) Only populated when server is online.
    /// </summary>
    public string? SqlServerVersion { get; set; }

    /// <summary>
    /// SQL Server major product version (e.g., 13 = 2016, 14 = 2017, 15 = 2019, 16 = 2022). (Lite)
    /// Used for version-gating collectors that require specific DMV columns.
    /// </summary>
    public int SqlMajorVersion { get; set; }

    /// <summary>
    /// SQL Server engine edition from SERVERPROPERTY('EngineEdition').
    /// 1=Personal, 2=Standard, 3=Enterprise, 4=Express, 5=Azure SQL DB, 8=Azure MI.
    /// Used for gating collectors that are incompatible with Azure platforms.
    /// </summary>
    public int SqlEngineEdition { get; set; }

    /// <summary>
    /// Whether this server is an AWS RDS instance (detected by presence of rdsadmin database).
    /// Used for gating features that require msdb permissions unavailable on RDS.
    /// </summary>
    public bool IsAwsRds { get; set; }

    /// <summary>
    /// Whether the connected login has access to msdb. (Lite)
    /// Used for gating collectors that query msdb system tables (e.g., running jobs).
    /// </summary>
    public bool HasMsdbAccess { get; set; } = true;

    /// <summary>
    /// The installed PerformanceMonitor version on the server (e.g., "2.5.0"). (Dashboard)
    /// Null if the PerformanceMonitor database is not installed.
    /// </summary>
    public string? InstalledMonitorVersion { get; set; }

    /// <summary>
    /// The server's UTC offset in minutes, queried via DATEDIFF(MINUTE, GETUTCDATE(), GETDATE()).
    /// Used to convert UTC-stored collection_time values to server-local time for display.
    /// </summary>
    public int? UtcOffsetMinutes { get; set; }

    /// <summary>
    /// Whether the user cancelled MFA authentication for this server.
    /// When true, background connectivity checks are skipped to avoid repeated authentication popups.
    /// </summary>
    public bool UserCancelledMfa { get; set; }

    /// <summary>
    /// Gets the status display text for the UI.
    /// </summary>
    public string StatusText
    {
        get
        {
            if (!LastChecked.HasValue)
                return "Not checked";

            if (IsOnline == true)
                return "Online";

            return "Offline";
        }
    }

    /// <summary>
    /// Gets the status icon for the UI (checkmark or X).
    /// </summary>
    public string StatusIcon
    {
        get
        {
            if (!LastChecked.HasValue)
                return "?";

            return IsOnline == true ? "✓" : "✗"; // checkmark or X
        }
    }

    /// <summary>
    /// Gets the formatted "last checked" time for display.
    /// </summary>
    public string LastCheckedDisplay
    {
        get
        {
            if (!LastChecked.HasValue)
                return "Never checked";

            var elapsed = DateTime.Now - LastChecked.Value;

            if (elapsed.TotalSeconds < 60)
                return "Checked just now";

            if (elapsed.TotalMinutes < 60)
                return $"Checked {(int)elapsed.TotalMinutes}m ago";

            if (elapsed.TotalHours < 24)
                return $"Checked {(int)elapsed.TotalHours}h ago";

            return $"Checked {LastChecked.Value.ToString("g")}";
        }
    }

    /// <summary>
    /// Gets the status duration display.
    /// For online: "Online since [server start time]"
    /// For offline: Just "Offline"
    /// </summary>
    public string StatusDurationDisplay
    {
        get
        {
            if (!IsOnline.HasValue)
                return string.Empty;

            if (IsOnline == true)
            {
                if (ServerStartTime.HasValue)
                {
                    return $"Online since {ServerStartTime.Value.ToString("g")}";
                }
                return "Online";
            }

            return "Offline";
        }
    }
}
