/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitorDashboard.Analysis;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitorDashboard.Services.Recommendations;
using PerformanceMonitorDashboard.Services.Remediation;

namespace PerformanceMonitorDashboard.Controls
{
    /// <summary>
    /// The unified, read-only Recommendations surface (recommendations rebuild WS1b-1). Renders
    /// the de-duped <see cref="RecommendationItem"/> list from
    /// <see cref="RecommendationsReader.GetRecommendationsAsync"/> as a card list grouped into
    /// collapsible Critical / Warning / Info sections.
    ///
    /// <para>
    /// Per-card affordances split on whether the finding is an <b>incident</b> (time-bound) or a
    /// standing <b>config-fix</b>. Incidents get "Open in Active Queries" (raises
    /// <see cref="OpenActiveQueriesRequested"/> so the host tab deep-links to the time window) and
    /// "Ask AI" (copies an MCP investigation prompt). Config-fixes get "Copy fix" (copies the
    /// ALTER). Both kinds get Apply when a built remediation exists — rendered DISABLED with an
    /// "Available in the next update" tooltip; the Apply action + informed-consent gate are WS1b-2.
    /// </para>
    /// </summary>
    public partial class RecommendationsContent : UserControl
    {
        /// <summary>
        /// Raised when the user clicks "Open in Active Queries" on an incident card. Carries the
        /// finding's RAW UTC time window; the host (<c>ServerTab</c>) applies the ±1h grace, the
        /// UTC→server-local conversion, selects the Performance/Queries tab + Active Queries
        /// sub-tab, and scopes it to the window. Mirrors
        /// <c>CriticalIssuesContent.InvestigateRequested</c>.
        /// </summary>
        public event Action<DateTime, DateTime>? OpenActiveQueriesRequested;

        private DatabaseService? _databaseService;
        private SqlServerFindingStore? _findingStore;
        private RecommendationsReader? _reader;
        private ServerConnection? _serverConnection;
        private ICredentialService? _credentialService;
        private RemediationApplyService? _remediationApplyService;

        /// <summary>
        /// The shared, gated remediation entry point (constructed once in <c>MainWindow</c> and
        /// threaded MainWindow -> ServerTab -> here, mirroring how <c>AlertsHistoryContent</c>
        /// receives it). Drives the Apply button: server resolution, the two-sided informed-
        /// consent gate for destructive fixes (RCSI / clear-plan), and the audit write. Null in
        /// contexts where no service was supplied, which leaves Apply inert.
        /// </summary>
        public RemediationApplyService? RemediationApplyService
        {
            get => _remediationApplyService;
            set => _remediationApplyService = value;
        }

        private int _hoursBack = 24;
        private int _utcOffsetMinutes;
        private bool _isBusy;

        public RecommendationsContent()
        {
            InitializeComponent();
        }

        /// <summary>
        /// Initializes the control with the dependencies it needs to read recommendations and to
        /// run an on-demand analysis. <paramref name="serverConnection"/> +
        /// <paramref name="credentialService"/> are used only by "Generate now" to build a
        /// connection string and derive the deterministic server id, exactly as the
        /// <see cref="AnalysisScheduler"/> does. <paramref name="utcOffsetMinutes"/> is the
        /// monitored server's UTC offset, used to normalize the legacy producer's server-local
        /// timestamps to UTC (reader) and to render the Ask-AI prompt window in server-local time.
        /// </summary>
        public void Initialize(
            DatabaseService databaseService,
            ServerConnection serverConnection,
            ICredentialService credentialService,
            int utcOffsetMinutes = 0)
        {
            _databaseService = databaseService ?? throw new ArgumentNullException(nameof(databaseService));
            _serverConnection = serverConnection ?? throw new ArgumentNullException(nameof(serverConnection));
            _credentialService = credentialService ?? throw new ArgumentNullException(nameof(credentialService));
            _utcOffsetMinutes = utcOffsetMinutes;
            _findingStore = new SqlServerFindingStore(databaseService.ConnectionString);
            _reader = new RecommendationsReader(databaseService, _findingStore);
        }

        /// <summary>
        /// Sets the look-back window used for both producer reads (mirrors
        /// <c>CriticalIssuesContent.SetTimeRange</c>'s hours-back contract; the Recommendations
        /// reader takes a single hours-back window).
        /// </summary>
        public void SetTimeRange(int hoursBack)
        {
            _hoursBack = hoursBack <= 0 ? 24 : hoursBack;
        }

        /// <summary>
        /// Re-reads recommendations for this server and re-renders. Safe to call from the parent
        /// tab's refresh. Read-only: surfaces Loaded or the all-clear Empty state. The
        /// insufficient-data state is surfaced by <see cref="GenerateNowButton_Click"/> (the
        /// engine owns that determination), not by this read-only path.
        /// </summary>
        public async Task RefreshDataAsync()
        {
            if (_reader is null || _serverConnection is null)
                return;

            if (_isBusy)
                return;

            _isBusy = true;
            try
            {
                ApplyViewModel(RecommendationsViewModel.Loading());

                var serverName = _serverConnection.ServerName;
                var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);

                var items = await _reader.GetRecommendationsAsync(
                    serverId, serverName, _hoursBack, utcOffsetMinutes: _utcOffsetMinutes);

                ApplyViewModel(RecommendationsViewModel.FromItems(items, serverName, _utcOffsetMinutes));
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading recommendations: {ex.Message}", ex);
                // Fall back to the empty state rather than leaving the spinner up.
                ApplyViewModel(RecommendationsViewModel.FromItems(Array.Empty<RecommendationItem>()));
            }
            finally
            {
                _isBusy = false;
            }
        }

        /// <summary>
        /// Runs an on-demand analysis for the current server (same construction path the
        /// <see cref="AnalysisScheduler"/> uses), then re-reads. If the engine reports insufficient
        /// collected history, surfaces the insufficient-data state with its message; otherwise the
        /// freshly-persisted findings are read back by <see cref="RefreshDataAsync"/>.
        /// </summary>
        private async void GenerateNowButton_Click(object sender, RoutedEventArgs e)
        {
            if (_serverConnection is null || _credentialService is null)
                return;

            if (_isBusy)
                return;

            _isBusy = true;
            try
            {
                ApplyViewModel(RecommendationsViewModel.Loading());
                StatusText.Text = "Generating…";

                var serverName = _serverConnection.ServerName;
                var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
                var displayName = _serverConnection.DisplayNameWithIntent;
                var connectionString = _serverConnection.GetConnectionString(_credentialService);

                // Fresh per-run AnalysisService (IsAnalyzing is per-instance), mirroring the
                // scheduler. AnalyzeAsync persists findings; we then read them back.
                var planFetcher = new SqlServerPlanFetcher(connectionString);
                var analysisService = new AnalysisService(connectionString, planFetcher);

                await analysisService.AnalyzeAsync(serverId, displayName, hoursBack: 4);

                if (analysisService.InsufficientDataMessage is { Length: > 0 } message)
                {
                    StatusText.Text = string.Empty;
                    ApplyViewModel(RecommendationsViewModel.InsufficientData(message));
                    return;
                }

                StatusText.Text = string.Empty;
                _isBusy = false; // release before re-entering RefreshDataAsync's own guard
                await RefreshDataAsync();
                return;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error generating recommendations: {ex.Message}", ex);
                StatusText.Text = "Generate failed — see log.";
                ApplyViewModel(RecommendationsViewModel.FromItems(Array.Empty<RecommendationItem>()));
            }
            finally
            {
                _isBusy = false;
            }
        }

        private async void RefreshButton_Click(object sender, RoutedEventArgs e)
        {
            StatusText.Text = string.Empty;
            await RefreshDataAsync();
        }

        /// <summary>
        /// Deep-links an incident card to the Active Queries view scoped to the finding's window.
        /// Raises <see cref="OpenActiveQueriesRequested"/> with the RAW UTC window; the host tab
        /// applies the grace + timezone conversion. Wired in WS1b-1.
        /// </summary>
        private void OpenInActiveQueries_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not FrameworkElement fe || fe.DataContext is not RecommendationCardViewModel card)
                return;

            // Engine findings always carry a window; legacy incidents carry log_date. Fall back to
            // a "now"-anchored band only if a producer somehow omitted it (handler widens anyway).
            var fromUtc = card.WindowStartUtc ?? DateTime.UtcNow.AddHours(-2);
            var toUtc = card.WindowEndUtc ?? DateTime.UtcNow;

            OpenActiveQueriesRequested?.Invoke(fromUtc, toUtc);
        }

        /// <summary>
        /// Copies the MCP investigation prompt for an incident card to the clipboard (no dialog;
        /// a brief status confirmation). Wired in WS1b-1.
        /// </summary>
        private void AskAi_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not FrameworkElement fe || fe.DataContext is not RecommendationCardViewModel card)
                return;

            // SetDataObject with copy=false avoids WPF's problematic Clipboard.Flush() (matches the
            // convention in CriticalIssuesContent / AlertDetailWindow).
            Clipboard.SetDataObject(card.AskAiPrompt, false);
            StatusText.Text = "AI prompt copied to clipboard.";
        }

        /// <summary>
        /// Copies a config-fix card's ALTER statement to the clipboard. Wired in WS1b-1.
        /// </summary>
        private void CopyFix_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not FrameworkElement fe || fe.DataContext is not RecommendationCardViewModel card)
                return;

            if (string.IsNullOrEmpty(card.CopyPasteSql))
                return;

            Clipboard.SetDataObject(card.CopyPasteSql, false);
            StatusText.Text = "Fix copied to clipboard.";
        }

        // ── Apply (gated remediation, mirrors AlertDetailWindow) ───────────────────

        /// <summary>
        /// Applies the card's built remediation through the gated <see cref="RemediationApplyService"/>.
        /// Mirrors <c>AlertDetailWindow</c> exactly: fail-closed server resolution, the handler-for-fact
        /// gate, then <c>ApplyAsync</c> with a confirm callback that news up the (two-sided when
        /// destructive) <see cref="RemediationConfirmWindow"/>. On a run, refreshes so the action-log
        /// outcome is reflected. <c>finding: null</c> is correct — the disclosure renders from the
        /// persisted action's carried figures, exactly as on the alert path.
        /// </summary>
        private async void ApplyButton_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not FrameworkElement fe || fe.DataContext is not RecommendationCardViewModel card)
                return;

            await RunApplyAsync(card);
        }

        private async Task RunApplyAsync(RecommendationCardViewModel card)
        {
            var item = card.Item;
            if (_remediationApplyService is null || item.Remediation is null || _serverConnection is null)
                return;

            if (_isBusy)
                return;

            // M3 fail-closed resolution: exact GUID match on this tab's connection Id, falling back to a
            // unique ServerName match. Ambiguous/unresolved disables Apply (the reason is surfaced).
            var resolution = _remediationApplyService.ResolveServer(_serverConnection.Id, _serverConnection.ServerName);
            if (!resolution.IsResolved || resolution.Server is null)
            {
                StatusText.Text = resolution.Reason ?? "Apply is unavailable — the source server could not be resolved.";
                return;
            }

            if (!_remediationApplyService.HasHandlerFor(item.Remediation.FactKey))
            {
                StatusText.Text = "No remediation handler is registered for this recommendation.";
                return;
            }

            _isBusy = true;
            try
            {
                StatusText.Text = "Applying…";

                var operatorIdentity = ResolveOperatorIdentity();
                // Audit provenance (RemediationIdentity doc: "the alert's metric name / story hash").
                var sourceRef = $"Recommendations: {item.StoryPathHash ?? item.Title}";

                // previewSql may be null for RCSI/clear-plan (no DB-config ALTER) — ApplyAsync then
                // renders the canonical EXEC/ALTER preview itself, exactly as on the alert path.
                var report = await _remediationApplyService.ApplyAsync(
                    item.Remediation, resolution.Server, item.CopyPasteSql, operatorIdentity, sourceRef,
                    request => ConfirmAsync(request, resolution), CancellationToken.None, finding: null);

                StatusText.Text = FormatApplyStatus(report);
                _isBusy = false; // release before re-entering RefreshDataAsync's own guard
                await RefreshDataAsync();
                return;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error applying recommendation: {ex.Message}", ex);
                StatusText.Text = $"Apply failed: {ex.Message}";
            }
            finally
            {
                _isBusy = false;
            }
        }

        /// <summary>
        /// The confirm gate. Invoked by the service from a background continuation, so it marshals to
        /// the UI thread to show the modal. Returning true is the only thing that lets the privileged
        /// handler run. For a destructive action the request carries the two-sided risks and the dialog
        /// renders the acknowledge-each-risk checkboxes automatically (no special-casing here).
        /// </summary>
        private async Task<bool> ConfirmAsync(RemediationConfirmRequest request, ServerResolution resolution)
        {
            return await Dispatcher.InvokeAsync(() =>
            {
                var dialog = new RemediationConfirmWindow(request, resolution.ResolvedByName, resolution.Reason)
                {
                    Owner = Window.GetWindow(this)
                };
                return dialog.ShowDialog() == true;
            });
        }

        /// <summary>
        /// A compact one-line outcome for the status strip (the alert path renders a per-target detail
        /// block; the Recommendations surface refreshes the cards, so a summary line suffices).
        /// </summary>
        private static string FormatApplyStatus(RemediationRunReport report)
        {
            switch (report.Status)
            {
                case RemediationRunStatus.NotConfirmed:
                    return "Cancelled — no change was made.";
                case RemediationRunStatus.NoHandler:
                    return "No remediation handler is registered for this recommendation.";
                case RemediationRunStatus.UnapplyNotSupported:
                    return "This fix type cannot be un-applied — no change was made.";
            }

            var succeeded = report.Targets.Count(t => t.Status == RemediationStatus.Success);
            var failed = report.Targets.Count(t =>
                t.Status is RemediationStatus.Error or RemediationStatus.Blocked or RemediationStatus.PermissionDenied);
            var skipped = report.Targets.Count(t => t.Status == RemediationStatus.Skipped);

            if (report.Targets.Any(t => t.AppliedButUnlogged && t.AuditFailureKind == AuditWriteFailureKind.Permanent))
                return $"Applied ({succeeded}), but the audit row could NOT be written — the monitoring login " +
                       "lacks INSERT on config.remediation_action_log. Fix this grant.";

            if (failed > 0)
                return $"Apply finished with issues — {succeeded} succeeded, {failed} failed" +
                       (skipped > 0 ? $", {skipped} skipped." : ".");

            if (succeeded > 0)
                return $"Applied — {succeeded} target(s) succeeded" + (skipped > 0 ? $", {skipped} skipped." : ".");

            if (skipped > 0)
                return $"Nothing to apply — {skipped} target(s) skipped.";

            return "Apply finished — no targets were processed.";
        }

        // ── Mute (engine rows only) ────────────────────────────────────────────────

        /// <summary>
        /// Mutes an engine recommendation's story pattern via the finding store, keyed on
        /// (serverId, <c>story_path_hash</c>) — the lower-level story mute, because the card carries a
        /// <see cref="RecommendationItem"/> rather than a full <c>AnalysisFinding</c>. After muting,
        /// refreshes so the row drops out (the next read filters it). Legacy rows have no mute concept
        /// and never reach here (the button is engine-only).
        /// </summary>
        private async void MuteButton_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not FrameworkElement fe || fe.DataContext is not RecommendationCardViewModel card)
                return;

            await RunMuteAsync(card);
        }

        private async Task RunMuteAsync(RecommendationCardViewModel card)
        {
            var item = card.Item;
            if (_findingStore is null || _serverConnection is null
                || item.Source != RecommendationSource.Engine
                || string.IsNullOrEmpty(item.StoryPathHash))
                return;

            if (_isBusy)
                return;

            _isBusy = true;
            try
            {
                StatusText.Text = "Muting…";

                // Same deterministic server id the reader/scheduler use.
                var serverId = ServerIdHelper.GetDeterministicHashCode(_serverConnection.ServerName);
                await _findingStore.MuteStoryAsync(
                    serverId, item.StoryPathHash, item.StoryPath ?? item.StoryPathHash,
                    reason: "Muted from Recommendations");

                StatusText.Text = "Recommendation muted.";
                _isBusy = false; // release before re-entering RefreshDataAsync's own guard
                await RefreshDataAsync();
                return;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error muting recommendation: {ex.Message}", ex);
                StatusText.Text = $"Mute failed: {ex.Message}";
            }
            finally
            {
                _isBusy = false;
            }
        }

        /// <summary>
        /// The OS / Windows user running the Dashboard, recorded as the audit <c>operator_identity</c>
        /// (mirrors <c>AlertDetailWindow.ResolveOperatorIdentity</c>; not a credential).
        /// </summary>
        private static string ResolveOperatorIdentity()
        {
            try
            {
                var name = System.Security.Principal.WindowsIdentity.GetCurrent()?.Name;
                if (!string.IsNullOrEmpty(name))
                    return name;
            }
            catch
            {
                /* WindowsIdentity can throw in constrained contexts — fall back. */
            }
            return Environment.UserName;
        }

        /// <summary>
        /// Swaps the visible region to match the view-model's state and binds the sections.
        /// </summary>
        private void ApplyViewModel(RecommendationsViewModel vm)
        {
            switch (vm.State)
            {
                case RecommendationsState.Loading:
                    LoadingOverlay.IsLoading = true;
                    SectionsScroll.Visibility = Visibility.Collapsed;
                    EmptyMessage.Visibility = Visibility.Collapsed;
                    InsufficientDataMessage.Visibility = Visibility.Collapsed;
                    break;

                case RecommendationsState.InsufficientData:
                    LoadingOverlay.IsLoading = false;
                    SectionsScroll.Visibility = Visibility.Collapsed;
                    EmptyMessage.Visibility = Visibility.Collapsed;
                    InsufficientDataMessage.Text = vm.InsufficientDataMessage;
                    InsufficientDataMessage.Visibility = Visibility.Visible;
                    break;

                case RecommendationsState.Empty:
                    LoadingOverlay.IsLoading = false;
                    SectionsList.ItemsSource = null;
                    SectionsScroll.Visibility = Visibility.Collapsed;
                    InsufficientDataMessage.Visibility = Visibility.Collapsed;
                    EmptyMessage.Visibility = Visibility.Visible;
                    break;

                case RecommendationsState.Loaded:
                default:
                    LoadingOverlay.IsLoading = false;
                    EmptyMessage.Visibility = Visibility.Collapsed;
                    InsufficientDataMessage.Visibility = Visibility.Collapsed;
                    SectionsList.ItemsSource = vm.Sections;
                    SectionsScroll.Visibility = Visibility.Visible;
                    break;
            }
        }
    }
}
