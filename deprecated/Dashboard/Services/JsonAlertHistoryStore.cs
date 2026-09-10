/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Interfaces;

namespace PerformanceMonitorDashboard.Services
{
    /// <summary>
    /// JSON-backed <see cref="IAlertHistoryStore"/> over <c>alert_history.json</c>.
    /// Owns the in-memory <c>List&lt;AlertLogEntry&gt;</c> + lock + load/save that
    /// previously lived inside <see cref="EmailAlertService"/>, and also hosts the
    /// Dashboard-only history-management API (GetAlertHistory / HideAlerts /
    /// HideAllAlerts) — that surface is not on <see cref="IAlertHistoryStore"/>.
    /// The async store reads/writes wrap the in-memory scan in completed tasks.
    /// </summary>
    public sealed class JsonAlertHistoryStore : IAlertHistoryStore
    {
        private const int MaxAlertLogEntries = 1000;
        private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };

        /* Retained for the LogAlertDismissals toggle in HideAlerts/HideAllAlerts, which is
           a history-management concern (not an alert setting). */
        private readonly IUserPreferencesService _preferencesService;

        /* Alert log — loaded from JSON on startup, saved on exit, new alerts added in-memory */
        private readonly List<AlertLogEntry> _alertLog = new();
        private readonly object _alertLogLock = new();
        private readonly string _alertLogFilePath;

        /// <summary>
        /// The current instance, set when MainWindow creates the store. Used by the Alerts
        /// history UI and MCP tools to reach the history-management API (GetAlertHistory /
        /// Hide*) directly, instead of forwarding through <see cref="EmailAlertService"/>
        /// (Plan E E3c Phase 6).
        /// </summary>
        public static JsonAlertHistoryStore? Current { get; private set; }

        public JsonAlertHistoryStore(IUserPreferencesService preferencesService)
        {
            _preferencesService = preferencesService;
            Current = this;

            var appDataPath = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "PerformanceMonitorDashboard");
            Directory.CreateDirectory(appDataPath);
            _alertLogFilePath = Path.Combine(appDataPath, "alert_history.json");

            LoadAlertLog();
        }

        /// <summary>
        /// Records an alert (tray notification or email) to the in-memory log.
        /// </summary>
        public Task RecordAlertAsync(AlertHistoryRecord record)
        {
            var entry = new AlertLogEntry
            {
                AlertTime = DateTime.UtcNow,
                ServerId = record.ServerId,
                ServerName = record.ServerName,
                MetricName = record.MetricName,
                CurrentValue = record.CurrentValueText,
                ThresholdValue = record.ThresholdValueText,
                AlertSent = record.AlertSent,
                NotificationType = record.NotificationType,
                SendError = record.SendError,
                Muted = record.Muted,
                DetailText = record.DetailText,
                ContextJson = record.ContextJson
            };

            lock (_alertLogLock)
            {
                _alertLog.Add(entry);

                /* Trim if over max */
                if (_alertLog.Count > MaxAlertLogEntries)
                {
                    _alertLog.RemoveRange(0, _alertLog.Count - MaxAlertLogEntries);
                }
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Returns the UTC time the most recent alert email was successfully
        /// sent for this server/metric, scanned from the in-memory alert log
        /// (which itself is loaded from alert_history.json on startup) — or
        /// null if none. Used to seed the in-memory cooldown after an app
        /// restart (#981 parity for Dashboard).
        /// </summary>
        /// <remarks>
        /// Dashboard records email and webhook deliveries as separate alert-log
        /// rows, so the filter is just NotificationType == "email" — Lite's
        /// combined "email+webhook" notification_type never appears here.
        /// When <paramref name="dedupKey"/> is non-null (#1154), the scan is additionally
        /// restricted to rows whose ContextJson carries that #1140 fingerprint (the helper
        /// null-guards the many tray/muted rows whose ContextJson is null).
        /// </remarks>
        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null)
        {
            lock (_alertLogLock)
            {
                DateTime? max = null;
                foreach (var entry in _alertLog)
                {
                    if (entry.ServerId != serverId) continue;
                    if (entry.MetricName != metricName) continue;
                    if (entry.NotificationType != "email") continue;
                    if (!string.IsNullOrEmpty(entry.SendError)) continue;
                    if (dedupKey is not null && !AlertContextSerializer.ContextJsonContainsDedupKey(entry.ContextJson, dedupKey)) continue;
                    if (max == null || entry.AlertTime > max.Value) max = entry.AlertTime;
                }
                return Task.FromResult(max);
            }
        }

        /// <summary>
        /// Returns the UTC time the most recent alert webhook was successfully
        /// sent for this server/metric, scanned from the in-memory alert log
        /// (loaded from alert_history.json on startup) — or null if none. Seeds
        /// the webhook cooldown after restart so a Teams/Slack alert posted
        /// shortly before a restart is not re-posted afterward (#1145, mirroring
        /// the email seed #981).
        /// </summary>
        /// <remarks>
        /// Dashboard records webhook deliveries as their own alert-log rows with
        /// NotificationType == "webhook" (written only on a successful post), so
        /// the type alone implies success — no SendError filter is needed.
        /// When <paramref name="dedupKey"/> is non-null (#1154), the scan is additionally
        /// restricted to rows whose ContextJson carries that #1140 fingerprint.
        /// </remarks>
        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null)
        {
            lock (_alertLogLock)
            {
                DateTime? max = null;
                foreach (var entry in _alertLog)
                {
                    if (entry.ServerId != serverId) continue;
                    if (entry.MetricName != metricName) continue;
                    if (entry.NotificationType != "webhook") continue;
                    if (dedupKey is not null && !AlertContextSerializer.ContextJsonContainsDedupKey(entry.ContextJson, dedupKey)) continue;
                    if (max == null || entry.AlertTime > max.Value) max = entry.AlertTime;
                }
                return Task.FromResult(max);
            }
        }

        /// <summary>
        /// Returns the AlertTime of the most recent log entry for the given
        /// (serverId, metricName), regardless of notification channel or send
        /// result. Used by <see cref="AnalysisNotificationService"/> to seed
        /// its per-finding cooldown across restarts. The analysis cooldown is
        /// stamped unconditionally, so the persisted equivalent ignores
        /// NotificationType (which can be "email", "webhook", or "tray" on
        /// Dashboard) and SendError. Returns null if no matching entry.
        /// </summary>
        /// <remarks>
        /// When <paramref name="dedupKey"/> is non-null (#1154, reused by #2716), the scan is
        /// additionally restricted to rows whose ContextJson carries that #1140 fingerprint, same as
        /// this store's email/webhook siblings.
        /// </remarks>
        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null)
        {
            lock (_alertLogLock)
            {
                DateTime? max = null;
                foreach (var entry in _alertLog)
                {
                    if (entry.ServerId != serverId) continue;
                    if (entry.MetricName != metricName) continue;
                    if (dedupKey is not null && !AlertContextSerializer.ContextJsonContainsDedupKey(entry.ContextJson, dedupKey)) continue;
                    if (max == null || entry.AlertTime > max.Value) max = entry.AlertTime;
                }
                return Task.FromResult(max);
            }
        }

        /// <summary>
        /// Gets alert history from the log (excludes hidden alerts).
        /// </summary>
        /// <param name="includeMuted">
        /// When true (default), muted rows are returned for audit/history display. When false,
        /// muted rows are filtered out <em>before</em> the limit is applied — used by the sidebar
        /// Alert badge count so known recurring noise (a muted source firing every cooldown) can
        /// neither inflate the badge nor push real alerts out of the counted window (#1225).
        /// </param>
        /// <param name="includeResolved">
        /// When true (default), resolution / good-news rows ("&#8230; Cleared/Resolved/Restored")
        /// are returned for audit/history display. When false, they are filtered out before the
        /// limit — also used by the sidebar Alert badge so a resolved condition is not counted as
        /// an actionable alert (#1225). See <see cref="AlertMetricClassifier.IsResolution"/>.
        /// </param>
        public List<AlertLogEntry> GetAlertHistory(int hoursBack = 24, int limit = 50, bool includeMuted = true, bool includeResolved = true)
        {
            var cutoff = DateTime.UtcNow.AddHours(-hoursBack);

            lock (_alertLogLock)
            {
                return _alertLog
                    .Where(a => a.AlertTime >= cutoff
                        && !a.Hidden
                        && (includeMuted || !a.Muted)
                        && (includeResolved || !AlertMetricClassifier.IsResolution(a.MetricName)))
                    .OrderByDescending(a => a.AlertTime)
                    .Take(limit)
                    .ToList();
            }
        }

        /// <summary>
        /// Hides specific alerts matching the given keys.
        /// Each key is (AlertTime, ServerName, MetricName).
        /// </summary>
        public void HideAlerts(List<(DateTime AlertTime, string ServerName, string MetricName)> keys)
        {
            if (keys.Count == 0) return;

            var keySet = new HashSet<(DateTime, string, string)>(keys);
            int hidden = 0;

            lock (_alertLogLock)
            {
                foreach (var alert in _alertLog)
                {
                    if (keySet.Contains((alert.AlertTime, alert.ServerName, alert.MetricName)))
                    {
                        alert.Hidden = true;
                        hidden++;
                    }
                }
            }

            if (_preferencesService.GetPreferences().LogAlertDismissals)
                Logger.Info($"[AlertDismiss] Dismissed {hidden} of {keys.Count} selected alert(s)");
        }

        /// <summary>
        /// Hides all non-hidden alerts matching the time/server filter.
        /// </summary>
        public void HideAllAlerts(int hoursBack, string? serverName = null)
        {
            var cutoff = DateTime.UtcNow.AddHours(-hoursBack);
            int hidden = 0;

            lock (_alertLogLock)
            {
                foreach (var alert in _alertLog)
                {
                    if (!alert.Hidden &&
                        alert.AlertTime >= cutoff &&
                        (serverName == null || alert.ServerName == serverName))
                    {
                        alert.Hidden = true;
                        hidden++;
                    }
                }
            }

            if (_preferencesService.GetPreferences().LogAlertDismissals)
                Logger.Info($"[AlertDismiss] Dismissed all: {hidden} alert(s) hidden (hoursBack={hoursBack}, server={serverName ?? "all"})");
        }

        #region Alert Log Persistence

        /// <summary>
        /// Saves the alert log to a JSON file. Call on application exit.
        /// </summary>
        public void SaveAlertLog()
        {
            try
            {
                List<AlertLogEntry> snapshot;
                lock (_alertLogLock)
                {
                    snapshot = new List<AlertLogEntry>(_alertLog);
                }

                var json = JsonSerializer.Serialize(snapshot, s_jsonOptions);
                File.WriteAllText(_alertLogFilePath, json);
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to save alert log: {ex.Message}");
            }
        }

        private void LoadAlertLog()
        {
            try
            {
                if (!File.Exists(_alertLogFilePath)) return;

                var json = File.ReadAllText(_alertLogFilePath);
                var entries = JsonSerializer.Deserialize<List<AlertLogEntry>>(json);

                if (entries != null)
                {
                    lock (_alertLogLock)
                    {
                        _alertLog.AddRange(entries);
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to load alert log, starting fresh: {ex.Message}");
            }
        }

        #endregion
    }

    /// <summary>
    /// Represents a single alert event in the log.
    /// </summary>
    public class AlertLogEntry
    {
        public DateTime AlertTime { get; set; }
        public string ServerId { get; set; } = "";
        public string ServerName { get; set; } = "";
        public string MetricName { get; set; } = "";
        public string CurrentValue { get; set; } = "";
        public string ThresholdValue { get; set; } = "";
        public bool AlertSent { get; set; }
        public string NotificationType { get; set; } = "";
        public string? SendError { get; set; }
        public bool Hidden { get; set; }
        public bool Muted { get; set; }
        public string? DetailText { get; set; }
        public string? ContextJson { get; set; }
    }
}
