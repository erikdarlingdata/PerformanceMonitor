/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace PerformanceMonitorLite.Services
{
    /// <summary>
    /// Manages alert state including suppression and acknowledgement for server tab badges.
    /// State is persisted to a JSON file so it survives app restarts.
    /// Thread-safe: All operations are protected by _lock.
    /// </summary>
    public class AlertStateService
    {
        private readonly object _lock = new();
        private readonly string _stateFilePath;

        private readonly HashSet<string> _silencedServers;

        /* Acknowledged alerts: serverId → UTC time of acknowledgement.
           Alerts older than the ack time stay suppressed; new events clear it. */
        private readonly Dictionary<string, DateTime> _acknowledgedAlerts;

        public event EventHandler? SuppressionStateChanged;

        public AlertStateService()
        {
            _stateFilePath = Path.Combine(AppContext.BaseDirectory, "alert_state.json");
            _silencedServers = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _acknowledgedAlerts = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
            Load();
        }

        /// <summary>
        /// Determines if alerts should be shown for a specific server.
        /// </summary>
        public bool ShouldShowAlerts(string serverId)
        {
            lock (_lock)
            {
                if (_silencedServers.Contains(serverId))
                    return false;

                if (_acknowledgedAlerts.ContainsKey(serverId))
                    return false;

                return true;
            }
        }

        /// <summary>
        /// Updates alert counts and returns whether the badge should be shown.
        /// Clears acknowledgement only if latestEventTime is newer than the ack timestamp,
        /// meaning genuinely new events arrived (not just a time-range change).
        /// </summary>
        public bool UpdateAlertCounts(string serverId, int blockingCount, int deadlockCount, DateTime? latestEventTimeUtc)
        {
            lock (_lock)
            {
                if (latestEventTimeUtc.HasValue
                    && _acknowledgedAlerts.TryGetValue(serverId, out var ackTime)
                    && latestEventTimeUtc.Value > ackTime)
                {
                    /* Event newer than acknowledgement — clear it */
                    _acknowledgedAlerts.Remove(serverId);
                    Save();
                }

                int totalAlerts = blockingCount + deadlockCount;
                return totalAlerts > 0 && ShouldShowAlertsInternal(serverId);
            }
        }

        /// <summary>
        /// Acknowledges alerts for a specific server. Persisted across restarts.
        /// Alerts stay suppressed until new events arrive (counts increase).
        /// </summary>
        public void AcknowledgeAlert(string serverId)
        {
            lock (_lock)
            {
                _acknowledgedAlerts[serverId] = DateTime.UtcNow;
                Save();
            }
            SuppressionStateChanged?.Invoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Silences a server entirely (no badges until unsilenced). Persisted across restarts.
        /// </summary>
        public void SilenceServer(string serverId)
        {
            lock (_lock)
            {
                _silencedServers.Add(serverId);
                Save();
            }
            SuppressionStateChanged?.Invoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Unsilences a server.
        /// </summary>
        public void UnsilenceServer(string serverId)
        {
            lock (_lock)
            {
                _silencedServers.Remove(serverId);
                Save();
            }
            SuppressionStateChanged?.Invoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Checks if a server is silenced.
        /// </summary>
        public bool IsServerSilenced(string serverId)
        {
            lock (_lock)
            {
                return _silencedServers.Contains(serverId);
            }
        }

        /// <summary>
        /// Removes all state for a server (call when server tab is closed).
        /// </summary>
        public void RemoveServerState(string serverId)
        {
            lock (_lock)
            {
                _silencedServers.Remove(serverId);
                _acknowledgedAlerts.Remove(serverId);
                Save();
            }
        }

        /* Internal check without re-acquiring lock */
        private bool ShouldShowAlertsInternal(string serverId)
        {
            if (_silencedServers.Contains(serverId))
                return false;
            if (_acknowledgedAlerts.ContainsKey(serverId))
                return false;
            return true;
        }

        private static readonly JsonSerializerOptions s_writeOptions = new()
        {
            WriteIndented = true
        };

        private void Save()
        {
            try
            {
                var state = new AlertStatePersisted
                {
                    SilencedServers = _silencedServers.ToList(),
                    AcknowledgedAlerts = _acknowledgedAlerts.ToDictionary(kv => kv.Key, kv => kv.Value)
                };
                var json = JsonSerializer.Serialize(state, s_writeOptions);
                File.WriteAllText(_stateFilePath, json);
            }
            catch
            {
                /* Best effort — don't crash if file write fails */
            }
        }

        private void Load()
        {
            try
            {
                if (!File.Exists(_stateFilePath)) return;

                var json = File.ReadAllText(_stateFilePath);
                var state = JsonSerializer.Deserialize<AlertStatePersisted>(json);
                if (state == null) return;

                if (state.SilencedServers != null)
                {
                    foreach (var s in state.SilencedServers)
                        _silencedServers.Add(s);
                }

                if (state.AcknowledgedAlerts != null)
                {
                    foreach (var kv in state.AcknowledgedAlerts)
                        _acknowledgedAlerts[kv.Key] = kv.Value;
                }
            }
            catch
            {
                /* Best effort — start fresh if file is corrupted */
            }
        }

        private class AlertStatePersisted
        {
            public List<string>? SilencedServers { get; set; }
            public Dictionary<string, DateTime>? AcknowledgedAlerts { get; set; }
        }
    }
}
