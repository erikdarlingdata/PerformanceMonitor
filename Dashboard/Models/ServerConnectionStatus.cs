/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitorDashboard.Models
{
    /// <summary>
    /// Represents the runtime connection status of a server.
    /// This is transient state that is not persisted to disk.
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
        /// SQL Server engine edition from SERVERPROPERTY('EngineEdition').
        /// 5=Azure SQL DB (unsupported), 8=Azure MI (supported).
        /// </summary>
        public int SqlEngineEdition { get; set; }

        /// <summary>
        /// Whether this server is an AWS RDS instance (detected by presence of rdsadmin database).
        /// Used for gating features that require msdb permissions unavailable on RDS.
        /// </summary>
        public bool IsAwsRds { get; set; }

        /// <summary>
        /// The server's UTC offset in minutes, queried via DATEDIFF(MINUTE, GETUTCDATE(), GETDATE()).
        /// Used to convert UTC-stored collection_time values to server-local time for display.
        /// </summary>
        public int? UtcOffsetMinutes { get; set; }

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

                return IsOnline == true ? "\u2713" : "\u2717"; // ✓ or ✗
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
}
