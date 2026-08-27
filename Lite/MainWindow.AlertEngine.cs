/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
/* The whole per-check loop (CPU → blocking → deadlocks → poison waits → long-running queries →
   tempdb → volume free space → anomalous jobs → failed jobs) lives in the shared
   PerformanceMonitor.Alerting.AlertEngine (Phase-5); this file only builds the snapshot, forwards,
   and applies the sweep's badge outcomes. The app's own CpuAlertMode enum is never referenced
   here, so the namespace import cannot collide. */
using PerformanceMonitor.Alerting;

namespace PerformanceMonitorLite;

public partial class MainWindow : Window
{
    /// <summary>
    /// Phase-5 forwarding: one alert sweep for one overview summary. The evaluation —
    /// thresholds, edge triggers, cooldowns, mutes, watermark persistence, resolution
    /// transitions — runs in the shared <see cref="AlertEngine"/> (the same code the headless
    /// Darling service runs); Lite-specific delivery (tray toasts, the #1141/#1236 per-event
    /// split, the one-combined-history-row send) lives behind <see cref="LiteAlertDeliverer"/>,
    /// and this method keeps only what never left the app: the suppression input
    /// (<see cref="AlertStateService.ShouldShowAlerts"/>) and the #754/#749 server-tab badge
    /// writes fed by the engine's sweep result.
    /// </summary>
    private async void CheckPerformanceAlerts(ServerSummaryItem summary)
    {
        /* Same early return as the pre-forwarding loop: alerts off or no tray yet — leave badge
           state untouched. (_alertEngine is wired right after the tray in MainWindow_Loaded.) */
        if (!App.AlertsEnabled || _trayService == null || _alertEngine == null) return;

        var key = summary.ServerId.ToString();

        /* Resolve the ServerConnection (the GUID identity used by the tabs/badges) for this summary,
           which carries the int DuckDB server_id. Drives the server tab badge for the low-disk /
           failed-job conditions (#754/#749); null when the server isn't in the list. */
        var badgeServer = _serverManager.GetAllServers().FirstOrDefault(s =>
            RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(s)).ToString() == key);

        /* #1128 review fix: snapshot the prior badge flags, take this sweep's values from the
           engine's result, and write them ONCE at the end — so a disabled feature / offline server
           clears a stale badge (not just the active branch), and a false->true transition clears
           the ack. */
        bool prevBadgeLowDisk = badgeServer != null && _badgeLowDisk.TryGetValue(badgeServer.Id, out var _pBadgeLd) && _pBadgeLd;
        bool prevBadgeFailedJob = badgeServer != null && _badgeFailedJob.TryGetValue(badgeServer.Id, out var _pBadgeFj) && _pBadgeFj;

        /* Skip popup/email alerts if user has acknowledged or silenced this server — suppression
           is an INPUT to the shared engine (evaluate-but-don't-deliver, gates don't advance). */
        bool suppressPopups = !_alertStateService.ShouldShowAlerts(key);

        /* #1696 Availability Group health, evaluated alongside the engine sweep rather than inside it: AG
           conditions are judged off the COLLECTED ag_* snapshots, not the engine's live per-server snapshot
           (the same reason Darling keeps them in its self-alert evaluator). Fire-and-forget so a DuckDB read
           can never stall the UI-timer sweep, and gated on the master AG switch so a fleet with no AGs pays
           only a dictionary lookup. */
        if (App.NotifyAgHealth)
        {
            _ = EvaluateAvailabilityGroupAlertsAsync(summary.ServerId, summary.DisplayName, suppressPopups);
        }

        /* The failed-jobs check needs the live connection status (online + engine edition); the
           fetcher re-checks it fresh at fetch time, exactly where the old loop's gate sat. */
        var connStatus = badgeServer != null ? _serverManager.GetConnectionStatus(badgeServer.Id) : null;

        var snapshot = new AlertServerSnapshot(
            key,
            summary.DisplayName,
            IsOnline: connStatus?.IsOnline == true,
            SqlCpuPercent: summary.CpuPercent,
            TotalCpuPercent: summary.TotalCpuPercent,
            IsAzureSqlDb: connStatus?.SqlEngineEdition == 5,
            Suppressed: suppressPopups);

        AlertSweepResult sweep;
        try
        {
            sweep = await _alertEngine.EvaluateServerAsync(snapshot);
        }
        catch (Exception ex)
        {
            /* The engine absorbs per-check failures itself; this catches the truly unexpected so
               an async-void sweep can never take down the dispatcher. Badges stay untouched. */
            AppLogger.Error("Alerts", $"Alert sweep failed for {summary.DisplayName}: {ex.Message}");
            return;
        }

        /* Master switch flipped off between our gate and the engine's — no sweep ran, so leave
           badge state untouched (the old loop's early return). */
        if (!sweep.Evaluated) return;

        /* #1128 review fix: write the badge's low-disk / failed-job flags ONCE per sweep from the
           standing conditions the engine observed. Doing it here (not only inside the
           feature-enabled / online / msdb branches) means a disabled feature or an offline server
           clears a previously-lit badge instead of leaving it stale. A false->true transition is a
           genuinely new condition, so it clears any acknowledgement — matching the Dashboard, whose
           IsWorseThanBaseline re-shows on a new disk/job condition. RefreshServerBadgeExtras
           re-renders once (no-op without a tab). */
        bool curBadgeLowDisk = sweep.LowDiskConditionPresent;
        bool curBadgeFailedJob = sweep.FailedJobConditionPresent;
        if (badgeServer != null)
        {
            _badgeLowDisk[badgeServer.Id] = curBadgeLowDisk;
            _badgeFailedJob[badgeServer.Id] = curBadgeFailedJob;
            if ((curBadgeLowDisk && !prevBadgeLowDisk) || (curBadgeFailedJob && !prevBadgeFailedJob))
                _alertStateService.ClearAcknowledgementForNewCondition(badgeServer.Id);
            RefreshServerBadgeExtras(badgeServer.Id);
        }
    }

    /// <summary>
    /// Delivers a connection-edge alert — email + webhook + the <c>config_alert_log</c> row — for one server
    /// (#1506). Called from <see cref="CheckConnectionsAndNotify"/>'s existing edge-triggered transitions,
    /// which keep showing their tray balloon; this is purely ADDITIVE, and closes the gap where Lite was the
    /// only app of the three that could not tell you your server had gone down unless you were watching the
    /// tray (the Dashboard emails on the same edge, and Darling's <c>DarlingSelfAlertEvaluator</c> emails and
    /// webhooks it).
    ///
    /// <para>The metric names — "Server Unreachable" / "Server Restored" — are the ones the shared severity
    /// map and <see cref="PerformanceMonitor.Common.AlertMetricClassifier"/> already understand, so the alert
    /// renders CRITICAL-red / RESOLVED-green and the history grid classifies the restore as a resolution
    /// without any new naming.</para>
    ///
    /// <para><b>Fires once per edge.</b> The caller's <c>_previousConnectionStates</c> dictionary is the edge
    /// trigger: a server that stays offline is offline→offline and does not re-enter this method, so an
    /// 8-hour outage produces ONE "Server Unreachable", not one per poll. There is deliberately no cooldown
    /// re-fire here — an edge is a single event.</para>
    ///
    /// <para>Suppression matches Lite's other alerts: a mute rule flags the row muted and skips the channels
    /// (<see cref="EmailAlertService.TrySendAlertEmailAsync"/>'s own contract — the history row is still
    /// written), and a silenced server delivers nothing at all, the same "Silence All Alerts covers the
    /// connection edge too" rule the Dashboard applies. Never throws: the send is fire-and-forget (SMTP/HTTP
    /// off a UI-timer tick) and <c>TrySendAlertEmailAsync</c> absorbs its own failures.</para>
    /// </summary>
    private void SendConnectionAlert(ServerConnection server, string metricName, string currentValue, string detailText)
    {
        try
        {
            /* The name every other Lite alert keys on: the sweep's serverName is summary.DisplayName, which
               MainWindow builds from DisplayNameWithIntent — so mute rules and history rows written here
               match the ones the engine writes for the same server. */
            var serverName = server.DisplayNameWithIntent;
            var serverId = RemoteCollectorService.GetDeterministicHashCode(
                RemoteCollectorService.GetServerNameForStorage(server));

            /* A silenced/acknowledged server suppresses the new channels, exactly as it suppresses the sweep's
               (ShouldShowAlerts is the engine's Suppressed input). The tray balloon in the caller is left
               alone — it never consulted this, and this change must not regress it. */
            if (!_alertStateService.ShouldShowAlerts(serverId.ToString()))
            {
                return;
            }

            bool isMuted = _muteRuleService.IsAlertMuted(new AlertMuteContext
            {
                ServerName = serverName,
                MetricName = metricName
            });

            /* #1681: log the FIRING. Connection alerts bypass both the shared engine and its logging funnel
               (they fire from the connect edge, not the sweep), so without this they were invisible in the
               log while their resolution toast was not. */
            AppLogger.Warn("Alerts", AlertFiringLog.Fired(
                serverName, metricName, "Critical", currentValue, isMuted));

            _ = _emailAlertService.TrySendAlertEmailAsync(
                metricName,
                serverName,
                currentValue,
                "Online",
                serverId,
                context: null,
                muted: isMuted,
                detailText: detailText);
        }
        catch (Exception ex)
        {
            AppLogger.Error("ConnectionAlerts", $"Connection alert delivery failed for {metricName}: {ex.Message}");
        }
    }

    /// <summary>
    /// Resolves a server's EFFECTIVE cadence (minutes) for one collector, feeding the read adapter's
    /// snapshot-freshness bounds (#1812 running_jobs, #1839 dmv_blocking_snapshot). The adapter keys
    /// servers by the deterministic int hash while ScheduleManager keys by the connection GUID, so this
    /// does the same hash-match <see cref="FetchFailedJobsForAlertAsync"/> does. Returns 0 — "no answer,
    /// use the shipped default" — for an unknown server or an unscheduled collector.
    /// </summary>
    private int ResolveCollectorCadenceForAlerts(int serverId, string collectorName)
    {
        var server = _serverManager.GetAllServers().FirstOrDefault(s =>
            RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(s)) == serverId);
        return server is null
            ? 0
            : _scheduleManager.GetScheduleForServer(server.Id, collectorName)?.FrequencyMinutes ?? 0;
    }

    /// <summary>
    /// The engine's live-msdb failed-jobs fetcher. Failure outcomes aren't part of the collected
    /// running_jobs snapshot, so this queries the monitored server directly at alert-check time via
    /// the collector's connection path (async SqlClient, already off the UI thread; MFA
    /// serialization / throttle / retry handled inside). The pre-forwarding gate moved here intact:
    /// only online, non-Azure-SQL-DB servers whose login has msdb access are queried — Azure SQL DB
    /// has no SQL Agent; a login without msdb can't read sysjobhistory — every other case degrades
    /// to an empty list (per the engine's fetcher contract), which also keeps the #749 badge dark.
    /// </summary>
    private async Task<List<FailedJobInfo>> FetchFailedJobsForAlertAsync(
        string serverKey, int lookbackMinutes, CancellationToken cancellationToken)
    {
        var server = _serverManager.GetAllServers().FirstOrDefault(s =>
            RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(s)).ToString() == serverKey);
        var connStatus = server != null ? _serverManager.GetConnectionStatus(server.Id) : null;

        if (server == null
            || _collectorService == null
            || connStatus == null
            || connStatus.IsOnline != true
            || connStatus.SqlEngineEdition == 5
            || !connStatus.HasMsdbAccess)
        {
            return new List<FailedJobInfo>();
        }

        /* GetRecentlyFailedJobsAsync degrades every read failure (permissions, transient) to an
           empty list itself, so a broken msdb read can't fail the sweep. */
        return await _collectorService.GetRecentlyFailedJobsAsync(server, lookbackMinutes, cancellationToken);
    }

    /// <summary>
    /// The engine's resolution callback: Lite's tray-only "Resolved/Cleared" toasts. The engine
    /// supplies the pre-forwarding loop's exact title/message strings and already applied the
    /// old gates (only on an active→inactive transition, never suppressed/disabled); every
    /// resolution toast was Success severity. No history row is recorded — exactly the old loop.
    /// </summary>
    private Task ShowAlertResolutionToastAsync(AlertResolution resolution, CancellationToken cancellationToken)
    {
        /* #1681: Lite logged NEITHER half — the toast is transient and nothing reached the log file, so an
           operator reading logs after the fact saw no alert history at all. Now the engine's funnel logs the
           firing and this logs the clear, in the shared shape, so the pair greps together. */
        AppLogger.Info("Alerts", AlertFiringLog.Resolved(
            resolution.ServerName, resolution.MetricName, resolution.Message));

        var tray = _trayService;
        if (tray != null)
        {
            if (Dispatcher.CheckAccess())
            {
                tray.ShowStyledNotification(resolution.Title, resolution.Message, ToastSeverity.Success);
            }
            else
            {
                Dispatcher.Invoke(() => tray.ShowStyledNotification(resolution.Title, resolution.Message, ToastSeverity.Success));
            }
        }
        return Task.CompletedTask;
    }

    /// <summary>
    /// Evaluates the four Availability Group conditions for one server from its latest collected snapshots
    /// and delivers whatever fired (#1696) — Lite's half of AG alert parity with Darling.
    ///
    /// <para>Each grain is freshness-gated on its OWN snapshot time, because the two AG collectors carry
    /// independent schedule entries: a disabled or failing database-grain collector must not have the replica
    /// grain's healthy timestamp vouch for its stale rows and keep re-firing "AG Sync Fell Behind" off
    /// days-old lag readings. A missing or stale snapshot is NO SIGNAL — neither fire nor clear — which is
    /// also the normal state on a server that simply has no Availability Groups (zero rows collected).</para>
    ///
    /// <para>Never throws: this runs unawaited off a UI-timer tick, so a DuckDB read failure logs and returns
    /// rather than surfacing as an unobserved task exception.</para>
    /// </summary>
    private async Task EvaluateAvailabilityGroupAlertsAsync(int serverId, string serverName, bool suppressed)
    {
        /* Captured once and null-checked, the same shape as the tray capture above. _dataService is
           nullable and unset until the local store opens, and this method runs UNAWAITED off a UI-timer
           tick — so it can land before the service is wired, and an NRE on a fire-and-forget task is an
           unobserved exception nobody ever sees. The symptom would have been AG alerts silently not
           evaluating on some launches with nothing in the log. Capturing also removes the gap a bare
           field check leaves: the field could be reassigned across either await below. */
        var data = _dataService;
        if (data is null)
        {
            return;
        }

        try
        {
            var alerts = new List<AgAlert>();
            var now = DateTime.UtcNow;

            var (replicaTimeUtc, replicas) = await data.GetLatestAgReplicaStatesAsync(serverId);
            if (IsFresh(replicaTimeUtc))
            {
                alerts.AddRange(_agAlertEvaluator.EvaluateReplicas(
                    serverId,
                    replicas,
                    App.AgDisconnectRefireMinutes > 0
                        ? TimeSpan.FromMinutes(App.AgDisconnectRefireMinutes)
                        : null));
            }

            var (databaseTimeUtc, databases) = await data.GetLatestAgDatabaseReplicaStatesAsync(serverId);
            if (IsFresh(databaseTimeUtc))
            {
                alerts.AddRange(_agAlertEvaluator.EvaluateDatabases(
                    serverId,
                    databases,
                    App.AgLagAlertSeconds,
                    App.AgRedoQueueAlertKb,
                    TimeSpan.FromMinutes(App.AlertCooldownMinutes)));
            }

            if (alerts.Count == 0 || suppressed)
            {
                return;
            }

            foreach (var alert in alerts)
            {
                SendAgAlert(serverId, serverName, alert);

                /* #2426: the re-fire window opens on DELIVERY, which is what the suppressed return above
                   makes necessary — an acknowledged or silenced server still evaluates every sweep and
                   sends nothing, and windows consumed by alerts nobody received would turn the re-fire back
                   into the silence it exists to end. */
                _agAlertEvaluator.NoteDelivered(alert);
            }

            bool IsFresh(DateTime? snapshotUtc) =>
                snapshotUtc.HasValue && now - snapshotUtc.Value < TimeSpan.FromMinutes(30);
        }
        catch (Exception ex)
        {
            AppLogger.Error("AgAlerts", $"Availability Group alert evaluation failed for {serverName}: {ex.Message}");
        }
    }

    /// <summary>
    /// Delivers one AG alert through the SAME path every other Lite alert uses — mute check, then
    /// <see cref="EmailAlertService.TrySendAlertEmailAsync"/>, which writes the one combined
    /// <c>config_alert_log</c> row and fans out to email + webhook. Metric names come from the shared
    /// <see cref="PerformanceMonitor.Common.AgAlertPolicy"/> consts, so a webhook keyed on "AG Failover"
    /// matches whether the alert came from Lite or from Darling.
    /// </summary>
    private void SendAgAlert(int serverId, string serverName, AgAlert alert)
    {
        try
        {
            bool isMuted = _muteRuleService.IsAlertMuted(new AlertMuteContext
            {
                ServerName = serverName,
                MetricName = alert.MetricName
            });

            /* #1681: AG alerts also bypass the engine funnel, so they log here. A resolution notice logs as
               RESOLVED rather than TRIGGERED, which is what makes the pair readable — an "AG Replica
               Reconnected" with no preceding "AG Replica Disconnected" is the exact half-story this fixes. */
            if (alert.IsResolution)
            {
                AppLogger.Info("Alerts", AlertFiringLog.Resolved(serverName, alert.MetricName, alert.DetailText));
            }
            else
            {
                AppLogger.Warn("Alerts", AlertFiringLog.Fired(
                    serverName, alert.MetricName, "Warning", alert.CurrentValue, isMuted));
            }

            _ = _emailAlertService.TrySendAlertEmailAsync(
                alert.MetricName,
                serverName,
                alert.CurrentValue,
                alert.ThresholdValue,
                serverId,
                context: alert.Context,
                muted: isMuted,
                detailText: alert.DetailText);
        }
        catch (Exception ex)
        {
            AppLogger.Error("AgAlerts", $"AG alert delivery failed for {alert.MetricName}: {ex.Message}");
        }
    }

}
