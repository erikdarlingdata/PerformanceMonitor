/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Interop;
using System.Windows.Media;
using System.Windows.Threading;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using PerformanceMonitorDashboard.Mcp;
using PerformanceMonitorDashboard.Models;
using System.Reflection;
using PerformanceMonitorDashboard.Controls;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Services;
using System.ComponentModel;
using System.Windows.Data;
using System.Xml.Linq;
using PerformanceMonitor.Ui;
using PerformanceMonitor.Common;
/* Type alias (not a namespace import) so PerformanceMonitor.Alerting's CpuAlertMode enum can never
   collide with Models.CpuAlertMode used below. */
using AlertContextBuilders = PerformanceMonitor.Alerting.AlertContextBuilders;

namespace PerformanceMonitorDashboard
{
    public partial class MainWindow : Window
    {
        #region Independent Alert Engine

        private void ConfigureAlertCheckTimer()
        {
            var prefs = _preferencesService.GetPreferences();

            if (prefs.NotificationsEnabled)
            {
                // Use auto-refresh interval if configured, otherwise default to 60 seconds
                var intervalSeconds = (prefs.AutoRefreshEnabled && prefs.AutoRefreshIntervalSeconds > 0)
                    ? prefs.AutoRefreshIntervalSeconds
                    : 60;
                _alertCheckTimer.Interval = TimeSpan.FromSeconds(intervalSeconds);
                _alertCheckTimer.Start();
            }
            else
            {
                _alertCheckTimer.Stop();
            }
        }

        private bool _isCheckingAlerts;

        private async void AlertCheckTimer_Tick(object? sender, EventArgs e)
        {
            /* Skip if the previous alert sweep is still running — otherwise slow ticks overlap and
               pile up concurrent per-server query batches on the shared connections. */
            if (_isCheckingAlerts) return;
            _isCheckingAlerts = true;
            try
            {
                await CheckAllServerAlertsAsync();

                /* Auto-refresh alert history if the tab is open */
                _alertsHistoryContent?.RefreshAlerts();

                UpdateAlertBadge();
            }
            finally
            {
                _isCheckingAlerts = false;
            }
        }

        private void UpdateAlertBadge()
        {
            /* The badge counts only actionable alerts. Muted alerts (known recurring noise) and
               resolution notices ("... Cleared/Resolved/Restored" — a condition that already
               recovered) both stay in Alert History for audit but must not inflate the badge, or the
               dashboard looks like it has unhandled alerts when it doesn't (#1225). Both are excluded
               inside the query, before the limit, so neither can push real alerts out of the count. */
            var alerts = _alertHistoryStore.GetAlertHistory(hoursBack: 24, limit: 100, includeMuted: false, includeResolved: false);
            var count = alerts.Count;

            if (count > 0)
            {
                AlertBadgeText.Text = count > 99 ? "99+" : count.ToString();
                AlertBadge.Visibility = Visibility.Visible;
            }
            else
            {
                AlertBadge.Visibility = Visibility.Collapsed;
            }
        }

        /// <summary>
        /// Checks all servers for alert conditions using lightweight queries.
        /// Runs independently of the LandingPage UI refresh.
        /// </summary>
        private async Task CheckAllServerAlertsAsync()
        {
            if (_notificationService == null) return;

            var prefs = _preferencesService.GetPreferences();
            if (!prefs.NotificationsEnabled) return;

            var servers = _serverManager.GetAllServers();
            var tasks = servers.Select(async server =>
            {
                try
                {
                    var connectionString = server.GetConnectionString(_credentialService);
                    var databaseService = new DatabaseService(connectionString);
                    var connStatus = _serverManager.GetConnectionStatus(server.Id);
                    var health = await databaseService.GetAlertHealthAsync(connStatus.SqlEngineEdition, prefs.LongRunningQueryThresholdMinutes, prefs.LongRunningJobMultiplier, prefs.LongRunningQueryMaxResults, prefs.LongRunningQueryExcludeSpServerDiagnostics, prefs.LongRunningQueryExcludeWaitFor, prefs.LongRunningQueryExcludeBackups, prefs.LongRunningQueryExcludeMiscWaits, prefs.LongRunningQueryExcludeCdc, prefs.FailedJobLookbackMinutes, prefs.AlertExcludedDatabases);

                    if (health.IsOnline)
                    {
                        /* Capture previous deadlock count BEFORE EvaluateAlertConditionsAsync updates it,
                           so the badge delta calculation sees the correct baseline. */
                        var prevDeadlockCount = _previousDeadlockCounts.TryGetValue(server.Id, out var pdc) ? pdc : 0;

                        await EvaluateAlertConditionsAsync(server.Id, server.DisplayNameWithIntent, health, databaseService);

                        /* Update tab badges from alert health data.
                           This ensures badges update even when the NOC view isn't active. */
                        await Dispatcher.InvokeAsync(() => UpdateTabBadgeFromAlertHealth(server.Id, health, prevDeadlockCount));
                    }
                }
                catch (Exception ex)
                {
                    Logger.Warning($"Alert check failed for {server.DisplayName}: {ex.Message}");
                }
            });

            await Task.WhenAll(tasks);
        }

        /// <summary>
        /// Evaluates alert conditions for a single server and fires notifications/emails.
        /// Uses cooldown tracking to prevent notification spam.
        /// </summary>
        private async Task EvaluateAlertConditionsAsync(
            string serverId, string serverName, AlertHealthResult health, DatabaseService databaseService)
        {
            var prefs = _preferencesService.GetPreferences();
            var alertCooldown = TimeSpan.FromMinutes(prefs.AlertCooldownMinutes);

            if (_alertStateService.IsAnySilencingActive(serverId))
            {
                return;
            }

            /* Cooldowns measure elapsed wall-clock time, so use a clock that does not shift when
               the user switches server tabs. ServerTimeHelper.ServerNow is derived from a process-wide
               UTC offset that is reassigned on every tab change; using it here makes (now - lastAlert)
               jump by the timezone delta whenever the selected tab changes between alert ticks, which
               either suppresses alerts (offset went back) or bypasses the cooldown (offset went
               forward) for every server. UTC is offset-independent. Display strings keep server time. */
            var now = DateTime.UtcNow;

            /* Blocking alerts */
            bool blockingExceeded = prefs.NotifyOnBlocking
                && health.LongestBlockedSeconds >= prefs.BlockingThresholdSeconds;

            if (blockingExceeded)
            {
                _activeBlockingAlert[serverId] = true;
                if (!_lastBlockingAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= alertCooldown)
                {
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Blocking Detected" };
                    bool isMuted = _muteRuleService.IsAlertMuted(muteCtx);
                    _lastBlockingAlert[serverId] = now;

                    var blockingContext = await BuildBlockingContextAsync(serverName, databaseService, prefs.AlertExcludedDatabases);
                    var detailText = AlertContextBuilders.ContextToDetailText(blockingContext)
                        ?? $"Blocked Sessions: {(int)health.TotalBlocked}\nLongest Wait: {(int)health.LongestBlockedSeconds}s";

                    if (!isMuted)
                    {
                        _notificationService?.ShowSnoozableNotification(
                            "Blocking Detected",
                            $"{serverName}: {(int)health.TotalBlocked} blocked session(s), longest {(int)health.LongestBlockedSeconds}s",
                            NotificationType.Warning,
                            serverName,
                            "Blocking Detected",
                            _muteRuleService);
                    }

                    _emailAlertService.RecordAlert(serverId, serverName, "Blocking Detected",
                        $"{(int)health.TotalBlocked} session(s), longest {(int)health.LongestBlockedSeconds}s",
                        $"{prefs.BlockingThresholdSeconds}s", !isMuted, isMuted ? "muted" : "tray", muted: isMuted, detailText: detailText);

                    if (!isMuted)
                    {
                        await SendDetectedAlertAsync(prefs,
                            "Blocking Detected",
                            serverName,
                            $"{(int)health.TotalBlocked} session(s), longest {(int)health.LongestBlockedSeconds}s",
                            $"{prefs.BlockingThresholdSeconds}s",
                            serverId,
                            blockingContext);
                    }
                }
            }
            else if (_activeBlockingAlert.TryRemove(serverId, out var wasBlocking) && wasBlocking)
            {
                _notificationService?.ShowStyledNotification("Blocking Cleared",
                    $"{serverName}: No active blocking", ToastSeverity.Success);
                _emailAlertService.RecordAlert(serverId, serverName, "Blocking Cleared",
                    "0", $"{prefs.BlockingThresholdSeconds}s", true, "tray");
            }

            /* Deadlock alerts — independent delta tracking */
            long deadlockDelta = 0;
            if (_previousDeadlockCounts.TryGetValue(serverId, out var prevCount))
            {
                deadlockDelta = health.DeadlockCount - prevCount;
                if (deadlockDelta < 0) deadlockDelta = 0; // handle counter reset
            }
            _previousDeadlockCounts[serverId] = health.DeadlockCount;

            /* Use the database-filtered count when excluded databases are configured,
               matching how blocking alerts filter before the threshold check.
               Falls back to the raw delta when no databases are excluded. */
            var effectiveDeadlockDelta = health.FilteredDeadlockCount ?? deadlockDelta;

            bool deadlocksExceeded = prefs.NotifyOnDeadlock
                && effectiveDeadlockDelta >= prefs.DeadlockThreshold;

            if (deadlocksExceeded)
            {
                _activeDeadlockAlert[serverId] = true;
                _lastDeadlockActivity[serverId] = now;
                if (!_lastDeadlockAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= alertCooldown)
                {
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Deadlocks Detected" };
                    bool isMuted = _muteRuleService.IsAlertMuted(muteCtx);
                    _lastDeadlockAlert[serverId] = now;

                    var deadlockContext = await BuildDeadlockContextAsync(serverName, databaseService, prefs.AlertExcludedDatabases);
                    var detailText = AlertContextBuilders.ContextToDetailText(deadlockContext)
                        ?? $"New Deadlocks: {effectiveDeadlockDelta}";

                    if (!isMuted)
                    {
                        var deadlockPlural = effectiveDeadlockDelta == 1 ? "" : "s";
                        _notificationService?.ShowSnoozableNotification(
                            "Deadlock Detected",
                            $"{serverName}: {(int)effectiveDeadlockDelta} deadlock{deadlockPlural} detected",
                            NotificationType.Error,
                            serverName,
                            "Deadlocks Detected",
                            _muteRuleService);
                    }

                    _emailAlertService.RecordAlert(serverId, serverName, "Deadlocks Detected",
                        effectiveDeadlockDelta.ToString(),
                        prefs.DeadlockThreshold.ToString(), !isMuted, isMuted ? "muted" : "tray", muted: isMuted, detailText: detailText);

                    if (!isMuted)
                    {
                        await SendDetectedAlertAsync(prefs,
                            "Deadlocks Detected",
                            serverName,
                            effectiveDeadlockDelta.ToString(),
                            prefs.DeadlockThreshold.ToString(),
                            serverId,
                            deadlockContext);
                    }
                }
            }
            else
            {
                /* Don't flap: deadlock detection is edge-triggered, so the check right after a
                   deadlock sees a zero delta. Only clear once the deadlock-quiet window has
                   elapsed since the last new deadlock, matching Lite's window semantics (#1091). */
                bool wasDeadlockActive = _activeDeadlockAlert.TryGetValue(serverId, out var wasDeadlock) && wasDeadlock;
                DateTime? lastDeadlockActivity = _lastDeadlockActivity.TryGetValue(serverId, out var lda) ? lda : null;
                if (DeadlockAlertClearPolicy.ShouldClear(wasDeadlockActive, lastDeadlockActivity, now, DeadlockQuietWindow))
                {
                    _activeDeadlockAlert.TryRemove(serverId, out _);
                    _lastDeadlockActivity.TryRemove(serverId, out _);
                    _notificationService?.ShowStyledNotification("Deadlocks Cleared",
                        $"{serverName}: No deadlocks in the last hour", ToastSeverity.Success);
                    _emailAlertService.RecordAlert(serverId, serverName, "Deadlocks Cleared",
                        "0", prefs.DeadlockThreshold.ToString(), true, "tray");
                }
            }

            /* Capture Down alerts — the blocking/deadlock XE session is missing and the
               collector couldn't create it, so capture is silently non-functional (#1086).
               Gated on the blocking/deadlock notification prefs: if the user wants those
               alerts, they need to know when the data feeding them stops existing. */
            bool captureDown = (prefs.NotifyOnBlocking || prefs.NotifyOnDeadlock)
                && health.MissingCaptureSessions.Count > 0;

            if (captureDown)
            {
                _activeCaptureDownAlert[serverId] = true;
                if (!_lastCaptureDownAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= alertCooldown)
                {
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Capture Down" };
                    bool isMuted = _muteRuleService.IsAlertMuted(muteCtx);
                    _lastCaptureDownAlert[serverId] = now;

                    var captureList = string.Join(" and ", health.MissingCaptureSessions);
                    var detailText = $"The {captureList} Extended Events session(s) are missing and could not be created. " +
                        "Blocking/deadlock data is NOT being captured. " +
                        "Check the collection log for the SESSION_MISSING error detail (usually a permissions problem: " +
                        "ALTER ANY EVENT SESSION on-prem, CREATE ANY DATABASE EVENT SESSION on Azure SQL DB).";

                    if (!isMuted)
                    {
                        _notificationService?.ShowSnoozableNotification(
                            "Capture Down",
                            $"{serverName}: {captureList} capture is not running — XE session missing",
                            NotificationType.Error,
                            serverName,
                            "Capture Down",
                            _muteRuleService);
                    }

                    _emailAlertService.RecordAlert(serverId, serverName, "Capture Down",
                        captureList, "session running", !isMuted, isMuted ? "muted" : "tray", muted: isMuted, detailText: detailText);

                    if (!isMuted)
                    {
                        await _emailAlertService.TrySendAlertEmailAsync(
                            "Capture Down",
                            serverName,
                            captureList,
                            "session running",
                            serverId);
                    }
                }
            }
            else if (_activeCaptureDownAlert.TryRemove(serverId, out var wasCaptureDown) && wasCaptureDown)
            {
                _notificationService?.ShowStyledNotification("Capture Restored",
                    $"{serverName}: Blocking/deadlock capture is running again", ToastSeverity.Success);
                _emailAlertService.RecordAlert(serverId, serverName, "Capture Restored",
                    "running", "session running", true, "tray");
            }

            /* Collection Stopped alerts — the collector Agent jobs are disabled, the Agent service is
               stopped, or the collectors are failing, so no new data is landing. This is the one check
               the app computes itself (live msdb job state + collection_log freshness), because every
               collected table the dead collector would have filled is unreliable here. Without it, a
               stopped collector reads as a calm, all-zero server instead of a warning. */
            if (prefs.NotifyOnCollectionStopped && health.CollectionStopped)
            {
                _activeCollectionStoppedAlert[serverId] = true;
                if (!_lastCollectionStoppedAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= alertCooldown)
                {
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Collection Stopped" };
                    bool isMuted = _muteRuleService.IsAlertMuted(muteCtx);
                    _lastCollectionStoppedAlert[serverId] = now;

                    var reason = health.CollectionStoppedReason ?? "Data collection has stopped.";
                    var detailText = reason +
                        " The Dashboard's history and live cards read from collected data, so a stopped collector makes " +
                        "the server look quiet (all zeros) rather than broken. Re-enable the PerformanceMonitor SQL Agent " +
                        "jobs (or restart the SQL Agent service) to resume collection.";

                    if (!isMuted)
                    {
                        _notificationService?.ShowSnoozableNotification(
                            "Collection Stopped",
                            $"{serverName}: {reason}",
                            NotificationType.Error,
                            serverName,
                            "Collection Stopped",
                            _muteRuleService);
                    }

                    /* #1913: the history row's Value carries the SAME cause the toast, the email and the
                       detail text carry, just compressed for the grid — and it is computed with the reason
                       (DecideCollectionStopped) rather than rebuilt here. The local rebuild disagreed with
                       the reason on both branches: it dropped the job TOTAL, which is what separates
                       "partially stopped" from "completely stopped", and on the staleness branch it wrote
                       the constant "no recent collection" — a restatement of the metric name that discarded
                       the minutes figure the alert exists to report. */
                    _emailAlertService.RecordAlert(serverId, serverName, "Collection Stopped",
                        health.CollectionStoppedShortValue ?? "no recent collection",
                        "collecting", !isMuted, isMuted ? "muted" : "tray", muted: isMuted, detailText: detailText);

                    if (!isMuted)
                    {
                        await _emailAlertService.TrySendAlertEmailAsync(
                            "Collection Stopped",
                            serverName,
                            reason,
                            "collecting",
                            serverId);
                    }
                }
            }
            else if (_activeCollectionStoppedAlert.TryRemove(serverId, out var wasCollectionStopped) && wasCollectionStopped)
            {
                _notificationService?.ShowStyledNotification("Collection Resumed",
                    $"{serverName}: Data collection is running again", ToastSeverity.Success);
                _emailAlertService.RecordAlert(serverId, serverName, "Collection Resumed",
                    "collecting", "collecting", true, "tray");
            }

            /* High CPU alerts — evaluator picks Total or SQL based on prefs.CpuAlertMode */
            int? alertCpuValue = prefs.CpuAlertMode == CpuAlertMode.Total
                ? health.TotalCpuPercent
                : health.CpuPercent;
            string cpuMetricLabel = prefs.CpuAlertMode == CpuAlertMode.Total ? "Total CPU" : "SQL CPU";

            bool cpuExceeded = prefs.NotifyOnHighCpu
                && alertCpuValue.HasValue
                && alertCpuValue.Value >= prefs.CpuThresholdPercent;

            if (cpuExceeded)
            {
                var cpuValue = alertCpuValue!.Value;
                _activeHighCpuAlert[serverId] = true;
                if (!_lastHighCpuAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= alertCooldown)
                {
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "High CPU" };
                    bool isMuted = _muteRuleService.IsAlertMuted(muteCtx);
                    _lastHighCpuAlert[serverId] = now;

                    if (!isMuted)
                    {
                        _notificationService?.ShowSnoozableNotification(
                            "High CPU",
                            $"{serverName}: {cpuMetricLabel} at {cpuValue}% (threshold: {prefs.CpuThresholdPercent}%)",
                            NotificationType.Warning,
                            serverName,
                            "High CPU",
                            _muteRuleService);
                    }

                    _emailAlertService.RecordAlert(serverId, serverName, "High CPU",
                        $"{cpuValue:F0}% ({cpuMetricLabel})",
                        $"{prefs.CpuThresholdPercent}%", !isMuted, isMuted ? "muted" : "tray", muted: isMuted,
                        detailText: $"  {cpuMetricLabel}: {cpuValue:F0}%\n  Threshold: {prefs.CpuThresholdPercent}%");

                    if (!isMuted)
                    {
                        await _emailAlertService.TrySendAlertEmailAsync(
                            "High CPU",
                            serverName,
                            $"{cpuValue:F0}% ({cpuMetricLabel})",
                            $"{prefs.CpuThresholdPercent}%",
                            serverId);
                    }
                }
            }
            else if (_activeHighCpuAlert.TryRemove(serverId, out var wasCpu) && wasCpu)
            {
                var cpuText = alertCpuValue.HasValue ? $"{alertCpuValue.Value:F0}%" : "N/A";
                _notificationService?.ShowStyledNotification("CPU Resolved",
                    $"{serverName}: {cpuMetricLabel} back to {cpuText}", ToastSeverity.Success);
                _emailAlertService.RecordAlert(serverId, serverName, "CPU Resolved",
                    cpuText, $"{prefs.CpuThresholdPercent}%", true, "tray");
            }

            /* Poison wait alerts */
            var triggeredWaits = prefs.NotifyOnPoisonWaits
                ? health.PoisonWaits.FindAll(w => w.AvgMsPerWait >= prefs.PoisonWaitThresholdMs)
                : new List<PoisonWaitDelta>();

            if (triggeredWaits.Count > 0)
            {
                _activePoisonWaitAlert[serverId] = true;
                if (!_lastPoisonWaitAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= alertCooldown)
                {
                    var worst = triggeredWaits[0];
                    var allWaitNames = string.Join(", ", triggeredWaits.ConvertAll(w => $"{w.WaitType} ({w.AvgMsPerWait:F0}ms)"));

                    /* Poison wait mute check uses the worst (highest avg ms/wait) triggered wait type.
                       Limitation: if a user mutes a specific wait type that isn't the worst, the alert
                       still fires. Conversely, muting the worst type suppresses the entire alert even
                       if other unmuted poison waits are present. */
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Poison Wait", WaitType = worst.WaitType };
                    bool isMuted = _muteRuleService.IsAlertMuted(muteCtx);
                    _lastPoisonWaitAlert[serverId] = now;
                    var poisonContext = AlertContextBuilders.BuildPoisonWaitContext(triggeredWaits);
                    var detailText = AlertContextBuilders.ContextToDetailText(poisonContext);

                    if (!isMuted)
                    {
                        _notificationService?.ShowSnoozableNotification(
                            "Poison Wait",
                            $"{serverName}: {worst.WaitType} avg {worst.AvgMsPerWait:F0}ms/wait",
                            NotificationType.Error,
                            serverName,
                            "Poison Wait",
                            _muteRuleService);
                    }

                    _emailAlertService.RecordAlert(serverId, serverName, "Poison Wait",
                        allWaitNames,
                        $"{prefs.PoisonWaitThresholdMs}ms avg", !isMuted, isMuted ? "muted" : "tray", muted: isMuted, detailText: detailText);

                    if (!isMuted)
                    {
                        await _emailAlertService.TrySendAlertEmailAsync(
                            "Poison Wait",
                            serverName,
                            allWaitNames,
                            $"{prefs.PoisonWaitThresholdMs}ms avg",
                            serverId,
                            poisonContext);
                    }
                }
            }
            else if (_activePoisonWaitAlert.TryRemove(serverId, out var wasPoisonWait) && wasPoisonWait)
            {
                _notificationService?.ShowStyledNotification("Poison Waits Cleared",
                    $"{serverName}: Poison wait avg below threshold", ToastSeverity.Success);
                _emailAlertService.RecordAlert(serverId, serverName, "Poison Waits Cleared",
                    "0", $"{prefs.PoisonWaitThresholdMs}ms avg", true, "tray");
            }

            /* Long-running query alerts */
            var lrqList = health.LongRunningQueries;
            if (prefs.AlertExcludedDatabases.Count > 0)
                lrqList = lrqList
                    .Where(q => string.IsNullOrEmpty(q.DatabaseName) ||
                        !prefs.AlertExcludedDatabases.Any(e =>
                            string.Equals(e, q.DatabaseName, StringComparison.OrdinalIgnoreCase)))
                    .ToList();

            bool longRunningTriggered = prefs.NotifyOnLongRunningQueries
                && lrqList.Count > 0;

            if (longRunningTriggered)
            {
                _activeLongRunningQueryAlert[serverId] = true;
                if (!_lastLongRunningQueryAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= alertCooldown)
                {
                    var worst = lrqList[0];
                    var elapsedMinutes = worst.ElapsedSeconds / 60;
                    var preview = AlertContextBuilders.TruncateText(worst.QueryText, 80);

                    var muteCtx = new AlertMuteContext
                    {
                        ServerName = serverName,
                        MetricName = "Long-Running Query",
                        DatabaseName = worst.DatabaseName,
                        QueryText = worst.QueryText
                    };
                    bool isMuted = _muteRuleService.IsAlertMuted(muteCtx);
                    _lastLongRunningQueryAlert[serverId] = now;
                    var lrqContext = AlertContextBuilders.BuildLongRunningQueryContext(serverName, lrqList);
                    var detailText = AlertContextBuilders.ContextToDetailText(lrqContext);

                    if (!isMuted)
                    {
                        var lrqPreview = string.IsNullOrEmpty(preview) ? "" : $" — {preview}";
                        _notificationService?.ShowSnoozableNotification(
                            "Long-Running Query",
                            $"{serverName}: Session #{worst.SessionId} running {elapsedMinutes}m{lrqPreview}",
                            NotificationType.Warning,
                            serverName,
                            "Long-Running Query",
                            _muteRuleService);
                    }

                    _emailAlertService.RecordAlert(serverId, serverName, "Long-Running Query",
                        $"Session #{worst.SessionId} running {elapsedMinutes}m",
                        $"{prefs.LongRunningQueryThresholdMinutes}m", !isMuted, isMuted ? "muted" : "tray", muted: isMuted, detailText: detailText);

                    if (!isMuted)
                    {
                        await _emailAlertService.TrySendAlertEmailAsync(
                            "Long-Running Query",
                            serverName,
                            $"{lrqList.Count} query(s), longest {elapsedMinutes}m",
                            $"{prefs.LongRunningQueryThresholdMinutes}m",
                            serverId,
                            lrqContext);
                    }
                }
            }
            else if (_activeLongRunningQueryAlert.TryRemove(serverId, out var wasLongRunning) && wasLongRunning)
            {
                _notificationService?.ShowStyledNotification("Long-Running Queries Cleared",
                    $"{serverName}: No queries over threshold", ToastSeverity.Success);
                _emailAlertService.RecordAlert(serverId, serverName, "Long-Running Queries Cleared",
                    "0", $"{prefs.LongRunningQueryThresholdMinutes}m", true, "tray");
            }

            /* TempDB space alerts */
            bool tempDbExceeded = prefs.NotifyOnTempDbSpace
                && health.TempDbSpace != null
                && health.TempDbSpace.ReservedPercent >= prefs.TempDbSpaceThresholdPercent;

            if (tempDbExceeded)
            {
                var tempDb = health.TempDbSpace!;
                _activeTempDbSpaceAlert[serverId] = true;
                if (!_lastTempDbSpaceAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= alertCooldown)
                {
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "tempdb Space" };
                    bool isMuted = _muteRuleService.IsAlertMuted(muteCtx);
                    _lastTempDbSpaceAlert[serverId] = now;
                    var tempDbContext = AlertContextBuilders.BuildTempDbSpaceContext(tempDb);
                    var detailText = AlertContextBuilders.ContextToDetailText(tempDbContext);

                    if (!isMuted)
                    {
                        _notificationService?.ShowSnoozableNotification(
                            "tempdb Space",
                            $"{serverName}: tempdb {tempDb.ReservedPercent:F0}% reserved",
                            NotificationType.Warning,
                            serverName,
                            "tempdb Space",
                            _muteRuleService);
                    }

                    _emailAlertService.RecordAlert(serverId, serverName, "tempdb Space",
                        $"{tempDb.ReservedPercent:F0}% reserved ({tempDb.TotalReservedMb:F0} MB)",
                        $"{prefs.TempDbSpaceThresholdPercent}%", !isMuted, isMuted ? "muted" : "tray", muted: isMuted, detailText: detailText);

                    if (!isMuted)
                    {
                        await _emailAlertService.TrySendAlertEmailAsync(
                            "tempdb Space",
                            serverName,
                            $"{tempDb.ReservedPercent:F0}% reserved ({tempDb.TotalReservedMb:F0} MB)",
                            $"{prefs.TempDbSpaceThresholdPercent}%",
                            serverId,
                            tempDbContext);
                    }
                }
            }
            else if (_activeTempDbSpaceAlert.TryRemove(serverId, out var wasTempDb) && wasTempDb)
            {
                var pct = health.TempDbSpace != null ? $"{health.TempDbSpace.ReservedPercent:F0}%" : "N/A";
                _notificationService?.ShowStyledNotification("tempdb Space Resolved",
                    $"{serverName}: tempdb reserved space back to {pct}", ToastSeverity.Success);
                _emailAlertService.RecordAlert(serverId, serverName, "tempdb Space Resolved",
                    pct, $"{prefs.TempDbSpaceThresholdPercent}%", true, "tray");
            }

            /* Low volume free space alerts — not applicable to Azure SQL DB (health.Volumes is empty there) */
            var breachedVolumes = prefs.NotifyOnLowDisk
                ? AlertContextBuilders.GetBreachedVolumes(health.Volumes, prefs.LowDiskThresholdPercent, prefs.LowDiskThresholdGb)
                : new List<VolumeFreeSpaceInfo>();

            if (breachedVolumes.Count > 0)
            {
                var worst = breachedVolumes[0];
                _activeLowDiskAlert[serverId] = true;
                double? lastLowDiskPercent =
                    _lastAlertedLowDiskPercent.TryGetValue(serverId, out var lowDiskPct) ? lowDiskPct : (double?)null;
                /* #754 follow-up: notify only on a fresh or worsening breach, not every cooldown for a
                   standing full volume (which also re-recorded a history row and made Dismiss feel broken). */
                if (LowDiskAlertGate.ShouldAlert(worst.FreePercent, lastLowDiskPercent)
                    && (!_lastLowDiskAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= alertCooldown))
                {
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Volume Free Space" };
                    bool isMuted = _muteRuleService.IsAlertMuted(muteCtx);
                    _lastLowDiskAlert[serverId] = now;
                    _lastAlertedLowDiskPercent[serverId] = worst.FreePercent;
                    var lowDiskContext = AlertContextBuilders.BuildVolumeFreeSpaceContext(serverName, breachedVolumes);
                    /* #1136: grade the alert — WARNING normally, CRITICAL when the worst volume is
                       critically low — so the email/webhook badge reflects how dire the breach is.
                       (lowDiskContext is non-null here — breachedVolumes.Count > 0 — but typed nullable.) */
                    if (lowDiskContext is not null && LowDiskAlertGate.IsCriticallyLow(worst.FreePercent, worst.FreeGb))
                    {
                        lowDiskContext.SeverityOverride = AlertSeverityLevel.Critical;
                    }
                    var detailText = AlertContextBuilders.ContextToDetailText(lowDiskContext);
                    var currentValue = $"{worst.MountPoint} {worst.FreePercent:F0}% free ({worst.FreeGb:F1} GB)";
                    var thresholdValue = AlertContextBuilders.FormatLowDiskThreshold(prefs.LowDiskThresholdPercent, prefs.LowDiskThresholdGb);

                    if (!isMuted)
                    {
                        _notificationService?.ShowSnoozableNotification(
                            "Volume Free Space",
                            $"{serverName}: {currentValue}",
                            NotificationType.Warning,
                            serverName,
                            "Volume Free Space",
                            _muteRuleService);
                    }

                    _emailAlertService.RecordAlert(serverId, serverName, "Volume Free Space",
                        currentValue, thresholdValue, !isMuted, isMuted ? "muted" : "tray", muted: isMuted, detailText: detailText);

                    if (!isMuted)
                    {
                        await _emailAlertService.TrySendAlertEmailAsync(
                            "Volume Free Space",
                            serverName,
                            currentValue,
                            thresholdValue,
                            serverId,
                            lowDiskContext);
                    }
                }
            }
            else if (_activeLowDiskAlert.TryRemove(serverId, out var wasLowDisk) && wasLowDisk)
            {
                _lastAlertedLowDiskPercent.TryRemove(serverId, out _);
                _notificationService?.ShowStyledNotification("Volume Free Space Resolved",
                    $"{serverName}: All volumes back above threshold", ToastSeverity.Success);
                _emailAlertService.RecordAlert(serverId, serverName, "Volume Free Space Resolved",
                    "OK", AlertContextBuilders.FormatLowDiskThreshold(prefs.LowDiskThresholdPercent, prefs.LowDiskThresholdGb), true, "tray");
            }

            /* Anomalous Agent job alerts */
            bool anomalousJobsTriggered = prefs.NotifyOnLongRunningJobs
                && health.AnomalousJobs.Count > 0;

            if (anomalousJobsTriggered)
            {
                _activeLongRunningJobAlert[serverId] = true;
                /* Prune aged-out per-run keys ({server}:{job}:{start}) — like Lite, this dict
                   otherwise grows one entry per anomalous job run for the whole session. */
                foreach (var staleJobKey in _lastLongRunningJobAlert
                             .Where(kv => now - kv.Value >= alertCooldown)
                             .Select(kv => kv.Key)
                             .ToList())
                {
                    _lastLongRunningJobAlert.TryRemove(staleJobKey, out _);
                }
                var worst = health.AnomalousJobs[0];
                var jobKey = $"{serverId}:{worst.JobId}:{worst.StartTime:O}";

                if (!_lastLongRunningJobAlert.TryGetValue(jobKey, out var lastAlert) || (now - lastAlert) >= alertCooldown)
                {
                    var currentMinutes = worst.CurrentDurationSeconds / 60;

                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Long-Running Job", JobName = worst.JobName };
                    bool isMuted = _muteRuleService.IsAlertMuted(muteCtx);
                    _lastLongRunningJobAlert[jobKey] = now;
                    var jobContext = AlertContextBuilders.BuildAnomalousJobContext(serverName, health.AnomalousJobs);
                    var detailText = AlertContextBuilders.ContextToDetailText(jobContext);

                    if (!isMuted)
                    {
                        _notificationService?.ShowSnoozableNotification(
                            "Long-Running Job",
                            $"{serverName}: {worst.JobName} at {(worst.PercentOfAverage ?? 0):F0}% of avg ({currentMinutes}m)",
                            NotificationType.Warning,
                            serverName,
                            "Long-Running Job",
                            _muteRuleService);
                    }

                    _emailAlertService.RecordAlert(serverId, serverName, "Long-Running Job",
                        $"{worst.JobName} at {worst.PercentOfAverage:F0}% of avg ({currentMinutes}m)",
                        $"{prefs.LongRunningJobMultiplier}x avg", !isMuted, isMuted ? "muted" : "tray", muted: isMuted, detailText: detailText);

                    if (!isMuted)
                    {
                        await _emailAlertService.TrySendAlertEmailAsync(
                            "Long-Running Job",
                            serverName,
                            $"{health.AnomalousJobs.Count} job(s) exceeding {prefs.LongRunningJobMultiplier}x average",
                            $"{prefs.LongRunningJobMultiplier}x historical avg",
                            serverId,
                            jobContext);
                    }
                }
            }
            else if (_activeLongRunningJobAlert.TryRemove(serverId, out var wasJob) && wasJob)
            {
                _notificationService?.ShowStyledNotification("Long-Running Jobs Cleared",
                    $"{serverName}: No jobs exceeding threshold", ToastSeverity.Success);
                _emailAlertService.RecordAlert(serverId, serverName, "Long-Running Jobs Cleared",
                    "0", $"{prefs.LongRunningJobMultiplier}x avg", true, "tray");
            }

            /* Failed Agent job alerts — live msdb query for runs that failed in the lookback window.
               Failures are point-in-time events (not a sustained state), so there is no "cleared"
               notification; the per-server watermark below dedups so the same failure never re-fires. */
            /* Track failed-job presence for the server tab badge (#749): active while a failure sits
               in the lookback window, cleared when it ages out, so the badge auto-resolves. */
            _activeFailedJobAlert[serverId] = prefs.NotifyOnFailedJobs && health.RecentlyFailedJobs.Count > 0;
            if (prefs.NotifyOnFailedJobs && health.RecentlyFailedJobs.Count > 0)
            {
                var newestFailure = health.RecentlyFailedJobs.Max(j => j.RunDateTime);
                /* Lazy restart-seed: the in-memory watermark is empty after a reopen, so failures
                   still in the lookback window would re-fire toasts the user already saw and
                   dismissed. Seed once per server from the persisted watermark (server-local basis,
                   stored as Ticks) — the failed-job equivalent of the #1145 blocking/deadlock seed. */
                if (!_lastAlertedFailedJobTime.ContainsKey(serverId)
                    && prefs.FailedJobAlertWatermarkTicks.TryGetValue(serverId, out var seededTicks))
                {
                    _lastAlertedFailedJobTime[serverId] = new DateTime(seededTicks);
                }
                bool hasWatermark = _lastAlertedFailedJobTime.TryGetValue(serverId, out var lastFailure);
                bool hasNewFailure = !hasWatermark || newestFailure > lastFailure;

                if (hasNewFailure
                    && (!_lastFailedJobAlert.TryGetValue(serverId, out var lastFailedAlert) || (now - lastFailedAlert) >= alertCooldown))
                {
                    var mostRecent = health.RecentlyFailedJobs[0]; // ORDER BY run_datetime DESC
                    var jobNames = string.Join(", ", health.RecentlyFailedJobs.Select(j => j.JobName).Distinct().Take(3));

                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Failed Agent Job", JobName = mostRecent.JobName };
                    bool isMuted = _muteRuleService.IsAlertMuted(muteCtx);
                    _lastFailedJobAlert[serverId] = now;
                    _lastAlertedFailedJobTime[serverId] = newestFailure;
                    /* Persist so the watermark survives a reopen (#1145 parity): a restart must not
                       re-fire toasts for these failures while they linger in the lookback window.
                       On-change only — only when a failed-job toast actually fires. */
                    prefs.FailedJobAlertWatermarkTicks[serverId] = newestFailure.Ticks;
                    _preferencesService.SavePreferences(prefs);
                    var jobContext = AlertContextBuilders.BuildFailedJobContext(serverName, health.RecentlyFailedJobs);
                    var detailText = AlertContextBuilders.ContextToDetailText(jobContext);

                    if (!isMuted)
                    {
                        _notificationService?.ShowSnoozableNotification(
                            "Failed Agent Job",
                            $"{serverName}: {health.RecentlyFailedJobs.Count} job failure(s) — {jobNames}",
                            NotificationType.Warning,
                            serverName,
                            "Failed Agent Job",
                            _muteRuleService);
                    }

                    _emailAlertService.RecordAlert(serverId, serverName, "Failed Agent Job",
                        $"{health.RecentlyFailedJobs.Count} failure(s) — {jobNames}",
                        $"last {prefs.FailedJobLookbackMinutes}m", !isMuted, isMuted ? "muted" : "tray", muted: isMuted, detailText: detailText);

                    if (!isMuted)
                    {
                        await _emailAlertService.TrySendAlertEmailAsync(
                            "Failed Agent Job",
                            serverName,
                            $"{health.RecentlyFailedJobs.Count} job failure(s) in last {prefs.FailedJobLookbackMinutes}m — {jobNames}",
                            $"last {prefs.FailedJobLookbackMinutes}m",
                            serverId,
                            jobContext);
                    }
                }
            }
        }

        /* #1141: in Per-event mode, deliver one notification per distinct incident (capped at
           AlertPerEventMaxPerCycle, with a trailing "+N more" carrying the remaining fingerprints)
           instead of one batched summary card. Falls back to the single summary send in Summary mode
           or when there are no incidents. Recording to alert history is left to the one RecordAlert
           call at the firing site; this only shapes the outbound send. */
        private async Task SendDetectedAlertAsync(
            UserPreferences prefs, string metricName, string serverName, string summaryCurrentValue,
            string thresholdValue, string serverId, AlertContext? context)
        {
            /* #1236: a per-server override (Manage Servers -> Edit) wins over the global delivery mode;
               null inherits prefs.AlertDeliveryMode. */
            var deliveryMode = AlertDeliveryModeResolver.Resolve(
                _serverManager.GetServerById(serverId)?.AlertDeliveryModeOverride, prefs.AlertDeliveryMode);
            if (deliveryMode == AlertNotificationMode.PerEvent && context?.Incidents is { Count: > 0 })
            {
                foreach (var msg in PerEventNotification.Split(context, prefs.AlertPerEventMaxPerCycle))
                {
                    await _emailAlertService.TrySendAlertEmailAsync(
                        metricName, serverName, msg.CurrentValue, thresholdValue, serverId, msg.Context);
                }
                return;
            }

            await _emailAlertService.TrySendAlertEmailAsync(
                metricName, serverName, summaryCurrentValue, thresholdValue, serverId, context);
        }

        private static async Task<AlertContext?> BuildBlockingContextAsync(string serverName, DatabaseService databaseService, List<string>? excludedDatabases = null)
        {
            try
            {
                var events = await databaseService.GetBlockingEventsAsync(hoursBack: 1);
                if (events == null || events.Count == 0) return null;

                if (excludedDatabases != null && excludedDatabases.Count > 0)
                {
                    events = events
                        .Where(e => string.IsNullOrEmpty(e.DatabaseName) ||
                            !excludedDatabases.Any(ex =>
                                string.Equals(ex, e.DatabaseName, StringComparison.OrdinalIgnoreCase)))
                        .ToList();
                    if (events.Count == 0) return null;
                }

                var context = new AlertContext();
                var firstXml = (string?)null;

                foreach (var e in events.GetRange(0, Math.Min(3, events.Count)))
                {
                    var item = new AlertDetailItem
                    {
                        Heading = $"Session #{e.Spid}",
                        Fields = new()
                    };

                    if (!string.IsNullOrEmpty(e.DatabaseName))
                        item.Fields.Add(("Database", e.DatabaseName));
                    if (!string.IsNullOrEmpty(e.QueryText))
                        item.Fields.Add(("Query", AlertContextBuilders.TruncateText(e.QueryText)));
                    if (e.WaitTimeMs.HasValue)
                        item.Fields.Add(("Wait Time", $"{e.WaitTimeMs:N0} ms"));
                    if (!string.IsNullOrEmpty(e.LockMode))
                        item.Fields.Add(("Lock Mode", e.LockMode));
                    if (!string.IsNullOrEmpty(e.ClientApp))
                        item.Fields.Add(("Client App", e.ClientApp));

                    context.Details.Add(item);
                    firstXml ??= e.BlockedProcessReportXml;
                }

                if (!string.IsNullOrEmpty(firstXml))
                {
                    context.AttachmentXml = firstXml;
                    context.AttachmentFileName = AlertIncidentAttachment.BlockedProcessReportFileName;
                }

                /* #1140: dedup by the resolved contentious object across the blocked-process rows
                   (already populated by sp_HumanEventsBlockViewer); falls back to db+query-pair only
                   when an object did not resolve. Computed over ALL events, not just the 3 displayed. */
                AlertIncidentRenderer.Apply(context, BlockingIncidentGrouper.Group(
                    serverName,
                    events.Select(e => new BlockingIncidentGrouper.BlockedEvent(
                        e.DatabaseName, e.ContentiousObject, e.QueryText, null, e.WaitTimeMs ?? 0, e.LockMode,
                        /* #3330: the row's own report, so a per-event card attaches its own incident's.
                           Wired here as well as in the shared builders because this app calls the same
                           PerEventNotification.Split, which no longer copies the alert-level attachment
                           onto every message — an unwired producer would deliver cards with no file at
                           all. The alert-level value above still drives the Summary card. */
                        string.IsNullOrEmpty(e.BlockedProcessReportXml)
                            ? null
                            : new AlertIncidentAttachment(
                                e.BlockedProcessReportXml,
                                AlertIncidentAttachment.BlockedProcessReportFileName))))
                    .Select(g => g.Incident).ToList());

                return context;
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to fetch blocking detail for email: {ex.Message}");
                return null;
            }
        }

        private static async Task<AlertContext?> BuildDeadlockContextAsync(string serverName, DatabaseService databaseService, List<string>? excludedDatabases = null)
        {
            try
            {
                var deadlocks = await databaseService.GetDeadlocksAsync(hoursBack: 1);
                if (deadlocks == null || deadlocks.Count == 0) return null;

                if (excludedDatabases != null && excludedDatabases.Count > 0)
                {
                    deadlocks = deadlocks
                        .Where(d => !IsDeadlockExcluded(d, excludedDatabases))
                        .ToList();
                    if (deadlocks.Count == 0) return null;
                }

                var context = new AlertContext();
                var firstGraph = (string?)null;

                // Group participants by deadlock event so victim + survivor are shown together
                var deadlockEvents = deadlocks
                    .GroupBy(d => d.EventDate)
                    .Take(3);

                foreach (var deadlockEvent in deadlockEvents)
                {
                    foreach (var d in deadlockEvent)
                    {
                        var role = string.Equals(d.DeadlockGroup, "victim", StringComparison.OrdinalIgnoreCase)
                            ? "victim" : "survivor";
                        var heading = $"Deadlock — Session #{d.Spid} ({role})";

                        var item = new AlertDetailItem
                        {
                            Heading = heading,
                            Fields = new()
                        };

                        if (!string.IsNullOrEmpty(d.DatabaseName))
                            item.Fields.Add(("Database", d.DatabaseName));
                        if (!string.IsNullOrEmpty(d.Query))
                            item.Fields.Add(("Query", AlertContextBuilders.TruncateText(d.Query)));
                        if (!string.IsNullOrEmpty(d.WaitResource))
                            item.Fields.Add(("Wait Resource", d.WaitResource));
                        if (!string.IsNullOrEmpty(d.LockMode))
                            item.Fields.Add(("Lock Mode", d.LockMode));
                        if (!string.IsNullOrEmpty(d.ClientApp))
                            item.Fields.Add(("Client App", d.ClientApp));

                        context.Details.Add(item);
                        firstGraph ??= d.DeadlockGraph;
                    }
                }

                if (!string.IsNullOrEmpty(firstGraph))
                {
                    context.AttachmentXml = firstGraph;
                    context.AttachmentFileName = AlertIncidentAttachment.DeadlockGraphFileName;
                }

                /* #1140: fingerprint each deadlock by its sorted involved-object set, parsed from the
                   deadlock graph (same DeadlockObjectExtractor Lite uses, for parity), grouped per
                   deadlock event across ALL events in the window. */
                AlertIncidentRenderer.Apply(context, DeadlockIncidentGrouper.Group(
                    serverName,
                    deadlocks.GroupBy(d => d.EventDate).Select(g =>
                    {
                        /* One graph per deadlock EVENT: the participants of one deadlock all carry the same
                           graph, so the first non-empty one is the event's. Bound once and used for both the
                           object parse and #3330's attachment, rather than walked twice. */
                        var graph = g.Select(x => x.DeadlockGraph).FirstOrDefault(x => !string.IsNullOrEmpty(x));
                        return new DeadlockIncidentGrouper.DeadlockEvent(
                            DeadlockObjectExtractor.FromGraphXml(graph),
                            DeadlockDetailFields(g),
                            /* #3330: see the blocking builder's note — this app shares
                               PerEventNotification.Split, which now attaches the card's OWN graph. */
                            string.IsNullOrEmpty(graph)
                                ? null
                                : new AlertIncidentAttachment(
                                    graph, AlertIncidentAttachment.DeadlockGraphFileName));
                    }))
                    .Select(g => g.Incident).ToList());

                return context;
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to fetch deadlock detail for email: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Returns true if a deadlock should be excluded based on the deadlock graph XML.
        /// A deadlock is only excluded when ALL process nodes have a currentdbname in the excluded list.
        /// Cross-database deadlocks involving any non-excluded database will still be reported.
        /// </summary>
        /* #1141: forensic detail carried on a deadlock incident so per-event cards keep the query +
           wait resource + lock mode (Summary mode shows them via the builder's own items). */
        private static List<AlertIncidentField>? DeadlockDetailFields(IEnumerable<DeadlockItem> participants)
        {
            var rep = participants.FirstOrDefault(x => !string.IsNullOrWhiteSpace(x.Query)) ?? participants.FirstOrDefault();
            if (rep is null) return null;
            var f = new List<AlertIncidentField>();
            if (!string.IsNullOrWhiteSpace(rep.DatabaseName)) f.Add(new AlertIncidentField("Database", rep.DatabaseName));
            if (!string.IsNullOrWhiteSpace(rep.Query)) f.Add(new AlertIncidentField("Query", AlertContextBuilders.TruncateText(rep.Query)));
            if (!string.IsNullOrWhiteSpace(rep.WaitResource)) f.Add(new AlertIncidentField("Wait Resource", rep.WaitResource));
            if (!string.IsNullOrWhiteSpace(rep.LockMode)) f.Add(new AlertIncidentField("Lock Mode", rep.LockMode));
            return f.Count > 0 ? f : null;
        }

        private static bool IsDeadlockExcluded(DeadlockItem deadlock, List<string> excludedDatabases)
        {
            if (string.IsNullOrEmpty(deadlock.DeadlockGraph)) return false;
            try
            {
                var doc = XElement.Parse(deadlock.DeadlockGraph);
                var dbNames = doc.Descendants("process")
                    .Select(p => p.Attribute("currentdbname")?.Value)
                    .Where(n => !string.IsNullOrEmpty(n))
                    .Cast<string>()
                    .ToList();
                if (dbNames.Count == 0) return false;
                return dbNames.All(db => excludedDatabases.Any(e =>
                    string.Equals(e, db, StringComparison.OrdinalIgnoreCase)));
            }
            catch { return false; }
        }

        /* The six pure alert-context builders (poison wait, long-running query, volume free space,
           tempdb space, anomalous job, failed job) and their pure helpers (ContextToDetailText,
           Truncate/TruncateText, GetBreachedVolumes, FormatLowDiskThreshold) moved to the shared
           PerformanceMonitor.Alerting.AlertContextBuilders (Phase-5 slice A). The async
           blocking/deadlock builders above stay app-side — they query the Dashboard's own store. */

        #endregion
    }
}
