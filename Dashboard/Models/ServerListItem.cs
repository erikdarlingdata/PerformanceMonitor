/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace PerformanceMonitorDashboard.Models
{
    /// <summary>
    /// View model that combines a ServerConnection with its connection status for display in the server list.
    /// Implements INotifyPropertyChanged to support UI updates when status changes.
    /// </summary>
    public class ServerListItem : INotifyPropertyChanged
    {
        private ServerConnectionStatus _status;

        public ServerListItem(ServerConnection server, ServerConnectionStatus status)
        {
            Server = server;
            _status = status;
        }

        /// <summary>
        /// The underlying server connection configuration.
        /// </summary>
        public ServerConnection Server { get; }

        /// <summary>
        /// The current connection status.
        /// </summary>
        public ServerConnectionStatus Status
        {
            get => _status;
            set
            {
                if (_status != value)
                {
                    _status = value;
                    OnPropertyChanged();
                    OnPropertyChanged(nameof(StatusIcon));
                    OnPropertyChanged(nameof(StatusColor));
                    OnPropertyChanged(nameof(LastCheckedDisplay));
                    OnPropertyChanged(nameof(StatusDurationDisplay));
                    OnPropertyChanged(nameof(IsOnline));
                    OnPropertyChanged(nameof(HasBeenChecked));
                }
            }
        }

        // Convenience properties for binding
        public string Id => Server.Id;
        public string DisplayName => Server.DisplayName;
        public string ServerName => Server.ServerName;

        /// <summary>
        /// Whether to show the ServerName line (hide if same as DisplayName).
        /// </summary>
        public bool ShowServerName => !string.Equals(Server.ServerName, Server.DisplayName, System.StringComparison.OrdinalIgnoreCase);
        public string? Description => Server.Description;
        public bool IsFavorite => Server.IsFavorite;

        /// <summary>
        /// Status icon: ✓ for online, ✗ for offline, ? for unknown.
        /// </summary>
        public string StatusIcon => _status.StatusIcon;

        /// <summary>
        /// Color for the status icon: green for online, red for offline, gray for unknown.
        /// </summary>
        public string StatusColor
        {
            get
            {
                if (!_status.LastChecked.HasValue)
                    return "#888888"; // Gray for not checked

                return _status.IsOnline == true ? "#81C784" : "#E57373"; // Green or Red
            }
        }

        /// <summary>
        /// Formatted display of when the status was last checked.
        /// </summary>
        public string LastCheckedDisplay => _status.LastCheckedDisplay;

        /// <summary>
        /// Formatted display of how long the server has been online/offline.
        /// </summary>
        public string StatusDurationDisplay => _status.StatusDurationDisplay;

        /// <summary>
        /// Whether the server is currently online.
        /// </summary>
        public bool? IsOnline => _status.IsOnline;

        /// <summary>
        /// Whether the server has been checked at least once.
        /// </summary>
        public bool HasBeenChecked => _status.LastChecked.HasValue;

        public event PropertyChangedEventHandler? PropertyChanged;

        protected virtual void OnPropertyChanged([CallerMemberName] string? propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        /// <summary>
        /// Refreshes the status and notifies UI of changes.
        /// </summary>
        public void RefreshStatus(ServerConnectionStatus newStatus)
        {
            Status = newStatus;
        }

        /// <summary>
        /// Refreshes the timestamp-based display properties without re-checking connectivity.
        /// Call this periodically to update "Xm ago" style displays.
        /// </summary>
        public void RefreshTimestampDisplay()
        {
            OnPropertyChanged(nameof(LastCheckedDisplay));
            OnPropertyChanged(nameof(StatusDurationDisplay));
        }
    }
}
