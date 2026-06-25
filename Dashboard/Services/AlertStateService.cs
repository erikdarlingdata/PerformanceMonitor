/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services
{
    /// <summary>
    /// Manages alert state including suppression and acknowledgement for tab badges.
    /// Thread-safe: All collection operations are protected by _lock.
    /// </summary>
    public class AlertStateService
    {
        private readonly IUserPreferencesService _preferencesService;
        private readonly object _lock = new object();

        // Suppression state (persisted via UserPreferencesService)
        private readonly HashSet<string> _silencedServers;
        private readonly HashSet<string> _silencedServerTabs;
        private readonly HashSet<string> _silencedSubTabs;

        // Acknowledged alert baselines (persisted via UserPreferencesService)
        // Key format: "{serverId}:{tabName}"
        // Badge stays hidden unless conditions worsen beyond the baseline.
        // Auto-cleared when the alert condition fully resolves.
        private readonly Dictionary<string, AlertBaseline> _acknowledgedBaselines;

        // Event for when suppression state changes
        public event EventHandler? SuppressionStateChanged;

        public AlertStateService(IUserPreferencesService? preferencesService = null)
        {
            _preferencesService = preferencesService ?? new UserPreferencesService();

            // Load persisted suppression state (use case-insensitive comparison for tab names)
            var prefs = _preferencesService.GetPreferences();
            _silencedServers = new HashSet<string>(prefs.SilencedServers ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
            _silencedServerTabs = new HashSet<string>(prefs.SilencedServerTabs ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
            _silencedSubTabs = new HashSet<string>(prefs.SilencedSubTabs ?? new List<string>(), StringComparer.OrdinalIgnoreCase);

            // Load persisted acknowledgement baselines
            var savedBaselines = prefs.AcknowledgedBaselines ?? new Dictionary<string, AlertBaseline>();
            _acknowledgedBaselines = new Dictionary<string, AlertBaseline>(savedBaselines, StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Determines if a badge should be shown for a specific tab.
        /// </summary>
        public bool ShouldShowBadge(string serverId, string tabName, ServerHealthStatus? status)
        {
            if (status == null || status.IsOnline != true)
                return false;

            lock (_lock)
            {
                // Check suppression hierarchy
                if (_silencedServers.Contains(serverId))
                    return false;

                if (_silencedServerTabs.Contains(serverId))
                    return false;

                var subTabKey = $"{serverId}:{tabName}";
                if (_silencedSubTabs.Contains(subTabKey))
                    return false;

                // Check acknowledgement baselines
                if (_acknowledgedBaselines.TryGetValue(subTabKey, out var baseline))
                {
                    // If condition has fully resolved, auto-clear the baseline
                    if (!HasAlertCondition(tabName, status))
                    {
                        _acknowledgedBaselines.Remove(subTabKey);
                        SaveAcknowledgementState();
                        return false;
                    }

                    // Condition still active — only show badge if WORSE than baseline
                    if (!IsWorseThanBaseline(tabName, status, baseline))
                        return false;
                }
            }

            // Check if there's actually an alert condition
            return HasAlertCondition(tabName, status);
        }

        /// <summary>
        /// Checks if a specific tab has an alert condition based on health status.
        /// </summary>
        public bool HasAlertCondition(string tabName, ServerHealthStatus status)
        {
            var prefs = _preferencesService.GetPreferences();

            return tabName.ToLowerInvariant() switch
            {
                "locking" => status.LongestBlockedSeconds >= prefs.BlockingThresholdSeconds
                          || status.DeadlocksSinceLastCheck >= prefs.DeadlockThreshold,
                "memory" => status.RequestsWaitingForMemory > 0,
                "resource metrics" => status.TotalCpuPercent.HasValue
                                   && status.TotalCpuPercent.Value >= prefs.CpuThresholdPercent,
                "overview" => HasAnyAlertCondition(status),
                _ => false
            };
        }

        /// <summary>
        /// Checks if any alert condition exists (for server-level badge).
        /// </summary>
        public bool HasAnyAlertCondition(ServerHealthStatus status)
        {
            var prefs = _preferencesService.GetPreferences();

            // Blocking
            if (status.LongestBlockedSeconds >= prefs.BlockingThresholdSeconds)
                return true;

            // Deadlocks
            if (status.DeadlocksSinceLastCheck >= prefs.DeadlockThreshold)
                return true;

            // Memory pressure
            if (status.RequestsWaitingForMemory > 0)
                return true;

            // CPU
            if (status.TotalCpuPercent.HasValue && status.TotalCpuPercent.Value >= prefs.CpuThresholdPercent)
                return true;

            // Low disk / failed Agent jobs (#754/#749) — set from the alert engine's per-server state.
            if (status.HasLowDiskAlert || status.HasFailedJobAlert)
                return true;

            return false;
        }

        /// <summary>
        /// Determines if current conditions are worse than the acknowledged baseline.
        /// </summary>
        private static bool IsWorseThanBaseline(string tabName, ServerHealthStatus status, AlertBaseline baseline)
        {
            return tabName.ToLowerInvariant() switch
            {
                "locking" =>
                    status.LongestBlockedSeconds > baseline.LongestBlockedSeconds
                    || status.DeadlocksSinceLastCheck > baseline.DeadlocksSinceLastCheck,

                "memory" =>
                    status.RequestsWaitingForMemory > baseline.RequestsWaitingForMemory,

                "resource metrics" =>
                    status.TotalCpuPercent.HasValue
                    && status.TotalCpuPercent.Value > (baseline.TotalCpuPercent ?? 0),

                "overview" =>
                    status.LongestBlockedSeconds > baseline.LongestBlockedSeconds
                    || status.DeadlocksSinceLastCheck > baseline.DeadlocksSinceLastCheck
                    || status.RequestsWaitingForMemory > baseline.RequestsWaitingForMemory
                    || (status.TotalCpuPercent.HasValue
                        && status.TotalCpuPercent.Value > (baseline.TotalCpuPercent ?? 0))
                    || (status.HasLowDiskAlert && !baseline.HasLowDiskAlert)
                    || (status.HasFailedJobAlert && !baseline.HasFailedJobAlert),

                _ => false
            };
        }

        /// <summary>
        /// Creates a baseline snapshot from the current health status.
        /// </summary>
        private static AlertBaseline CreateBaseline(ServerHealthStatus status)
        {
            return new AlertBaseline
            {
                LongestBlockedSeconds = status.LongestBlockedSeconds,
                DeadlocksSinceLastCheck = status.DeadlocksSinceLastCheck,
                RequestsWaitingForMemory = status.RequestsWaitingForMemory,
                TotalCpuPercent = status.TotalCpuPercent,
                HasLowDiskAlert = status.HasLowDiskAlert,
                HasFailedJobAlert = status.HasFailedJobAlert
            };
        }

        /// <summary>
        /// Acknowledges an alert for a specific tab. Captures a baseline snapshot
        /// so the badge stays hidden unless conditions worsen.
        /// </summary>
        public void AcknowledgeAlert(string serverId, string tabName, ServerHealthStatus? status)
        {
            var key = $"{serverId}:{tabName}";
            var baseline = status != null ? CreateBaseline(status) : new AlertBaseline();

            lock (_lock)
            {
                _acknowledgedBaselines[key] = baseline;
            }
            SaveAcknowledgementState();
            SuppressionStateChanged?.Invoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Acknowledges all alerts for a server. Captures baseline snapshots
        /// so badges stay hidden unless conditions worsen.
        /// </summary>
        public void AcknowledgeAllAlerts(string serverId, ServerHealthStatus? status)
        {
            var tabNames = new[] { "Overview", "Locking", "Memory", "Resource Metrics" };
            var baseline = status != null ? CreateBaseline(status) : new AlertBaseline();

            lock (_lock)
            {
                foreach (var tabName in tabNames)
                {
                    _acknowledgedBaselines[$"{serverId}:{tabName}"] = baseline;
                }
            }
            SaveAcknowledgementState();
            SuppressionStateChanged?.Invoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Silences a specific sub-tab (persisted).
        /// </summary>
        public void SilenceSubTab(string serverId, string tabName)
        {
            var key = $"{serverId}:{tabName}";
            lock (_lock)
            {
                _silencedSubTabs.Add(key);
            }
            SaveSuppressionState();
            SuppressionStateChanged?.Invoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Unsilences a specific sub-tab.
        /// </summary>
        public void UnsilenceSubTab(string serverId, string tabName)
        {
            var key = $"{serverId}:{tabName}";
            lock (_lock)
            {
                _silencedSubTabs.Remove(key);
            }
            SaveSuppressionState();
            SuppressionStateChanged?.Invoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Checks if a sub-tab is silenced.
        /// </summary>
        public bool IsSubTabSilenced(string serverId, string tabName)
        {
            var key = $"{serverId}:{tabName}";
            lock (_lock)
            {
                return _silencedSubTabs.Contains(key);
            }
        }

        /// <summary>
        /// Silences all tabs for a server (persisted).
        /// </summary>
        public void SilenceServerTab(string serverId)
        {
            lock (_lock)
            {
                _silencedServerTabs.Add(serverId);
            }
            SaveSuppressionState();
            SuppressionStateChanged?.Invoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Unsilences all tabs for a server.
        /// </summary>
        public void UnsilenceServerTab(string serverId)
        {
            lock (_lock)
            {
                _silencedServerTabs.Remove(serverId);
            }
            SaveSuppressionState();
            SuppressionStateChanged?.Invoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Checks if a server's tabs are silenced.
        /// </summary>
        public bool IsServerTabSilenced(string serverId)
        {
            lock (_lock)
            {
                return _silencedServerTabs.Contains(serverId);
            }
        }

        /// <summary>
        /// Silences a server entirely (no badges or notifications).
        /// </summary>
        public void SilenceServer(string serverId)
        {
            lock (_lock)
            {
                _silencedServers.Add(serverId);
            }
            SaveSuppressionState();
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
            }
            SaveSuppressionState();
            SuppressionStateChanged?.Invoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Checks if a server is completely silenced (for toast suppression).
        /// </summary>
        public bool IsServerSilenced(string serverId)
        {
            lock (_lock)
            {
                return _silencedServers.Contains(serverId);
            }
        }

        /// <summary>
        /// Checks if any form of silencing is active for a server.
        /// </summary>
        public bool IsAnySilencingActive(string serverId)
        {
            lock (_lock)
            {
                return _silencedServers.Contains(serverId) || _silencedServerTabs.Contains(serverId);
            }
        }

        /// <summary>
        /// Removes all state for a server (call when server is deleted).
        /// </summary>
        public void RemoveServerState(string serverId)
        {
            bool stateChanged = false;
            lock (_lock)
            {
                stateChanged |= _silencedServers.Remove(serverId);
                stateChanged |= _silencedServerTabs.Remove(serverId);

                // Remove all sub-tab silencing for this server
                var subTabsToRemove = _silencedSubTabs.Where(k => k.StartsWith(serverId + ":", StringComparison.OrdinalIgnoreCase)).ToList();
                foreach (var key in subTabsToRemove)
                {
                    stateChanged |= _silencedSubTabs.Remove(key);
                }

                // Remove all acknowledgement baselines for this server
                var baselinesToRemove = _acknowledgedBaselines.Keys
                    .Where(k => k.StartsWith(serverId + ":", StringComparison.OrdinalIgnoreCase))
                    .ToList();
                foreach (var key in baselinesToRemove)
                {
                    _acknowledgedBaselines.Remove(key);
                    stateChanged = true;
                }
            }

            if (stateChanged)
            {
                SaveSuppressionState();
                SaveAcknowledgementState();
            }
        }

        private void SaveSuppressionState()
        {
            var prefs = _preferencesService.GetPreferences();
            lock (_lock)
            {
                prefs.SilencedServers = new List<string>(_silencedServers);
                prefs.SilencedServerTabs = new List<string>(_silencedServerTabs);
                prefs.SilencedSubTabs = new List<string>(_silencedSubTabs);
            }
            _preferencesService.SavePreferences(prefs);
        }

        private void SaveAcknowledgementState()
        {
            var prefs = _preferencesService.GetPreferences();
            lock (_lock)
            {
                prefs.AcknowledgedBaselines = new Dictionary<string, AlertBaseline>(_acknowledgedBaselines);
            }
            _preferencesService.SavePreferences(prefs);
        }
    }
}
