/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Threading;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorDashboard.Analysis;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Interfaces;

namespace PerformanceMonitorDashboard.Services
{
    /// <summary>
    /// Runs the triage engine (<see cref="AnalysisService"/>) for each connected server on
    /// a configurable interval and routes high-severity findings to
    /// <see cref="AnalysisNotificationService"/>. Lives next to the threshold-alert timer
    /// in MainWindow's lifetime; entirely separate cadence and gating.
    ///
    /// <para>
    /// Production is gated by <see cref="UserPreferences.AnalysisEnabled"/> (default ON):
    /// when enabled the timer runs and <c>AnalyzeAsync</c> persists findings every cycle.
    /// Notification *delivery* is the inner gate — <c>NotifyAsync</c> is only called when
    /// <see cref="UserPreferences.AnalysisNotificationsEnabled"/> is also on. Per-server
    /// in-flight tracking prevents a hung connection from piling up overlapping analysis
    /// tasks for the same server.
    /// </para>
    /// </summary>
    public sealed class AnalysisScheduler
    {
        private readonly IServerManager _serverManager;
        private readonly ICredentialService _credentialService;
        private readonly IUserPreferencesService _preferencesService;
        private readonly AnalysisNotificationService _notificationService;
        private readonly DispatcherTimer _timer;

        /// <summary>
        /// Server-ids whose previous analysis is still running. A hung connection that
        /// outlived its timeout would otherwise pile up tasks. The marker is cleared on
        /// real task completion (in the ContinueWith), never on the timeout fast-path —
        /// so a permanently-hung server is parked, not relaunched. Accepted limitation:
        /// matches Lite Stage 2.
        /// </summary>
        private readonly ConcurrentDictionary<int, byte> _inFlight = new();

        private readonly CancellationTokenSource _cts = new();
        private bool _cycleRunning;

        public AnalysisScheduler(
            IServerManager serverManager,
            ICredentialService credentialService,
            IUserPreferencesService preferencesService,
            AnalysisNotificationService notificationService)
        {
            _serverManager = serverManager;
            _credentialService = credentialService;
            _preferencesService = preferencesService;
            _notificationService = notificationService;

            _timer = new DispatcherTimer();
            _timer.Tick += OnTick;
        }

        /// <summary>
        /// Applies the latest interval from preferences and starts or stops the timer.
        /// Safe to call repeatedly — call after any settings save.
        /// </summary>
        public void Configure()
        {
            var prefs = _preferencesService.GetPreferences();

            /* D0: analysis *production* is gated by AnalysisEnabled, NOT by the
               notification toggle. The timer runs (and AnalyzeAsync persists findings)
               whenever analysis is enabled; NotifyAsync is gated separately in the cycle. */
            if (!prefs.AnalysisEnabled)
            {
                _timer.Stop();
                return;
            }

            var intervalMinutes = Math.Clamp(prefs.AnalysisIntervalMinutes, 5, 360);
            _timer.Interval = TimeSpan.FromMinutes(intervalMinutes);

            if (!_timer.IsEnabled)
                _timer.Start();
        }

        /// <summary>
        /// Stops the timer and cancels any in-flight cycle. Called on app shutdown.
        /// </summary>
        public void Stop()
        {
            _timer.Stop();
            try
            {
                _cts.Cancel();
            }
            catch (ObjectDisposedException)
            {
                /* Already disposed — no-op. */
            }
        }

        private async void OnTick(object? sender, EventArgs e)
        {
            /* Re-entrancy guard: if the previous cycle still has not finished (e.g. a
               slow server), don't pile up another cycle on top of it. */
            if (_cycleRunning)
                return;

            _cycleRunning = true;
            try
            {
                await RunCycleAsync(_cts.Token);
            }
            catch (Exception ex)
            {
                Logger.Error($"AnalysisScheduler: cycle failed: {ex.GetType().Name}: {ex.Message}");
            }
            finally
            {
                _cycleRunning = false;
            }
        }

        private async Task RunCycleAsync(CancellationToken cancellationToken)
        {
            var prefs = _preferencesService.GetPreferences();
            if (!prefs.AnalysisEnabled)
                return;

            /* D0: deliver findings only when notifications are also enabled. Analysis
               runs+persists regardless; this inner gate controls delivery alone. */
            var notify = prefs.AnalysisNotificationsEnabled;

            var timeoutSeconds = Math.Clamp(prefs.AnalysisTimeoutSeconds, 30, 600);
            var timeout = TimeSpan.FromSeconds(timeoutSeconds);

            foreach (var server in _serverManager.GetAllServers())
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                /* Stable, process-independent server id — matches the MCP entry points
                   so persisted analysis_findings/analysis_muted rows align. */
                var serverId = ServerIdHelper.GetDeterministicHashCode(server.ServerName);
                var displayName = server.DisplayNameWithIntent;

                /* Skip a server whose previous analysis is still running. */
                if (!_inFlight.TryAdd(serverId, 0))
                    continue;

                try
                {
                    var connectionString = server.GetConnectionString(_credentialService);

                    /* Fresh AnalysisService per server: IsAnalyzing is a single instance
                       flag, so a shared instance whose task is abandoned on timeout would
                       block analysis for every other server. */
                    var planFetcher = new SqlServerPlanFetcher(connectionString);
                    var analysisService = new AnalysisService(connectionString, planFetcher);
                    var analyzeTask = analysisService.AnalyzeAsync(serverId, displayName, hoursBack: 4);

                    /* Clear the in-flight marker only when the task truly finishes — not
                       when the timeout below moves us on — so a hung server is not relaunched. */
                    _ = analyzeTask.ContinueWith(
                        completed => _inFlight.TryRemove(serverId, out _),
                        TaskScheduler.Default);

                    /* Linked CTS so the Task.Delay timer is cancelled the moment analyze
                       wins the race — otherwise the orphaned timer lingers for the full
                       timeout window after we have moved on. */
                    using var delayCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    var finished = await Task.WhenAny(analyzeTask, Task.Delay(timeout, delayCts.Token));
                    delayCts.Cancel();

                    if (cancellationToken.IsCancellationRequested)
                        break;

                    if (finished != analyzeTask)
                    {
                        Logger.Warning(
                            $"AnalysisScheduler: scheduled analysis for {displayName} exceeded {timeoutSeconds}s — skipped this cycle");
                        continue;
                    }

                    var findings = await analyzeTask;

                    /* Analysis already persisted via the batched insert inside AnalyzeAsync.
                       Only route findings to the notification channels when delivery is on. */
                    if (notify)
                        await _notificationService.NotifyAsync(findings);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Logger.Error(
                        $"AnalysisScheduler: scheduled analysis failed for {displayName}: {ex.GetType().Name}: {ex.Message}");
                    /* If analyzeTask was never created (e.g. ctor threw), the continuation
                       never ran — clear the marker defensively. */
                    _inFlight.TryRemove(serverId, out _);
                }
            }
        }
    }
}
