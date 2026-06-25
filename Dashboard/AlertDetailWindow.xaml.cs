/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Media;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Notifications;
using PerformanceMonitorDashboard.Controls;
using PerformanceMonitorDashboard.Services.Remediation;

namespace PerformanceMonitorDashboard
{
    public partial class AlertDetailWindow : Window
    {
        private readonly RemediationApplyService? _applyService;
        private readonly ServerResolution _resolution;
        private readonly string _operatorIdentity;
        private readonly string? _sourceAlertRef;

        public AlertDetailWindow(AlertHistoryDisplayItem item)
            : this(item, applyService: null)
        {
        }

        public AlertDetailWindow(AlertHistoryDisplayItem item, RemediationApplyService? applyService)
        {
            InitializeComponent();

            _applyService = applyService;
            _sourceAlertRef = item.MetricName;
            _operatorIdentity = ResolveOperatorIdentity();

            // M3 fail-closed server resolution. Resolve once; every remediation block
            // in this alert is for the same source server. Unresolved/ambiguous ->
            // Apply is hard-disabled (the resolution carries the tooltip reason).
            _resolution = _applyService is not null
                ? _applyService.ResolveServer(item.ServerId, item.ServerName)
                : new ServerResolution { Reason = "Apply Fix is unavailable in this context." };

            TimeText.Text = item.TimeLocal;
            ServerText.Text = item.ServerName;
            MetricText.Text = item.MetricName;
            CurrentValueText.Text = item.CurrentValue;
            ThresholdText.Text = item.ThresholdValue;
            NotificationText.Text = item.NotificationType;
            StatusText.Text = item.StatusDisplay;

            if (item.Muted)
                MutedBanner.Visibility = Visibility.Visible;

            /* Prefer the structured context (advice / remediation T-SQL / drill-down) persisted as
               ContextJson; fall back to the flat detail text for older rows that have no context. */
            if (TryBuildDetailViews(item.ContextJson, out var views))
            {
                DetailItems.ItemsSource = views;
                DetailItemsScroll.Visibility = Visibility.Visible;
                DetailPanel.Visibility = Visibility.Visible;
            }
            else if (!string.IsNullOrWhiteSpace(item.DetailText))
            {
                DetailTextBox.Text = item.DetailText;
                DetailTextBox.Visibility = Visibility.Visible;
                DetailPanel.Visibility = Visibility.Visible;
            }
        }

        /* WPF cannot data-bind to AlertDetailItem.Fields (a List<(string,string)> ValueTuple — its
           Item1/Item2 are fields, not properties), so the deserialized context is projected into
           bindable view-models the DataTemplate can render. */
        private bool TryBuildDetailViews(string? contextJson, out List<DetailItemView> views)
        {
            views = new List<DetailItemView>();
            if (!AlertContextSerializer.TryDeserialize(contextJson, out var context))
                return false;

            foreach (var detail in context.Details)
            {
                var view = new DetailItemView
                {
                    Heading = detail.Heading,
                    Body = detail.Body,
                    IsCodeBlock = detail.IsCodeBlock,
                    Remediation = detail.Remediation
                };
                foreach (var (label, value) in detail.Fields)
                    view.Fields.Add(new FieldView { Label = label, Value = value });

                ConfigureApplyAffordance(view);
                views.Add(view);
            }

            return views.Count > 0;
        }

        /// <summary>
        /// Decides whether this block shows an Apply affordance and whether it is
        /// enabled. Shown ONLY for a non-null RemediationAction whose fact key has a
        /// registered handler. Enabled ONLY when the source server resolved
        /// unambiguously (M3 fail-closed); otherwise hard-disabled with a tooltip.
        /// </summary>
        private void ConfigureApplyAffordance(DetailItemView view)
        {
            var known = _applyService is not null
                && view.Remediation is not null
                && _applyService.HasHandlerFor(view.Remediation.FactKey);

            view.ShowApply = known;
            view.SetServerResolved(known && _resolution.IsResolved);
            view.ApplyDisabledReason = _resolution.IsResolved
                ? null
                : _resolution.Reason;
        }

        private async void ApplyButton_Click(object sender, RoutedEventArgs e)
        {
            if (sender is FrameworkElement { DataContext: DetailItemView view })
                await RunRemediationAsync(view, isUnapply: false);
        }

        private async void UnapplyButton_Click(object sender, RoutedEventArgs e)
        {
            if (sender is FrameworkElement { DataContext: DetailItemView view })
                await RunRemediationAsync(view, isUnapply: true);
        }

        private async Task RunRemediationAsync(DetailItemView view, bool isUnapply)
        {
            if (_applyService is null || view.Remediation is null || _resolution.Server is null || view.IsBusy)
                return;

            view.BeginRun(isUnapply ? "Un-applying…" : "Applying…");
            try
            {
                var report = isUnapply
                    ? await _applyService.UnapplyAsync(
                        view.Remediation, _resolution.Server, _operatorIdentity, _sourceAlertRef, ConfirmAsync, CancellationToken.None)
                    : await _applyService.ApplyAsync(
                        view.Remediation, _resolution.Server, view.Body, _operatorIdentity, _sourceAlertRef, ConfirmAsync, CancellationToken.None);

                view.SetResult(FormatReport(report), SeverityOf(report));
            }
            catch (Exception ex)
            {
                view.SetResult($"{(isUnapply ? "Un-apply" : "Apply")} failed: {ex.Message}", ResultSeverity.Error);
            }
        }

        /// <summary>
        /// The confirm gate. Invoked by the service from a background continuation,
        /// so it marshals to the UI thread to show the modal. Returning true is the
        /// only thing that lets the privileged handler run.
        /// </summary>
        private async Task<bool> ConfirmAsync(RemediationConfirmRequest request)
        {
            return await Dispatcher.InvokeAsync(() =>
            {
                var dialog = new RemediationConfirmWindow(request, _resolution.ResolvedByName, _resolution.Reason)
                {
                    Owner = this
                };
                return dialog.ShowDialog() == true;
            });
        }

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

        private void CopyTsqlButton_Click(object sender, RoutedEventArgs e)
        {
            if (sender is FrameworkElement { DataContext: DetailItemView view } && !string.IsNullOrEmpty(view.Body))
            {
                try
                {
                    Clipboard.SetText(view.Body);
                }
                catch
                {
                    /* Clipboard can be locked by another process — swallow contention. */
                }
            }
        }

        // ── Result formatting ──────────────────────────────────────────────────

        private static string FormatReport(RemediationRunReport report)
        {
            switch (report.Status)
            {
                case RemediationRunStatus.NoHandler:
                    return "No remediation handler is registered for this finding.";
                case RemediationRunStatus.NotConfirmed:
                    return "Cancelled — no change was made.";
                case RemediationRunStatus.UnapplyNotSupported:
                    return "This fix type cannot be un-applied — no change was made.";
            }

            var verb = report.IsUnapply ? "unforced" : "forced";
            var sb = new StringBuilder();
            foreach (var t in report.Targets)
            {
                if (sb.Length > 0) sb.Append('\n');
                sb.Append(FormatTarget(t, verb));
            }
            return sb.Length > 0 ? sb.ToString() : "No targets were processed.";
        }

        private static string FormatTarget(RemediationTargetReport t, string verb)
        {
            // Force-plan rows carry query_id/plan_id; DB_CONFIG rows do not (both 0).
            var isForcePlan = t.QueryId != 0 || t.PlanId != 0;
            var where = isForcePlan
                ? $"[{t.Database}] query_id {t.QueryId}, plan_id {t.PlanId}"
                : $"[{t.Database}]";
            // For DB_CONFIG the handler's per-target message already names the setting
            // and outcome; "plan forced" wording is force-plan-specific.
            var successText = isForcePlan ? $"plan {verb}" : (t.Message ?? "applied");

            // Applied-but-unlogged (O3 + LOW-2): the mutation succeeded; distinguish a
            // permanent setup/grant failure (loud, "fix this") from a transient one.
            if (t.AppliedButUnlogged)
            {
                var reversal = isForcePlan
                    ? "The change is in place (reversible via Un-apply)."
                    : "The change is in place (the prior value was recorded for manual reversal).";
                return t.AuditFailureKind switch
                {
                    AuditWriteFailureKind.Permanent =>
                        $"⚠ {where}: {successText}, but the audit row could NOT be written — the monitoring "
                        + "login lacks INSERT on config.remediation_action_log. EVERY Apply on this server "
                        + "will go unlogged until this grant is fixed. " + reversal,
                    _ =>
                        $"⚠ {where}: {successText}, but writing the audit row failed (transient). The change is "
                        + "in place; re-check config.remediation_action_log."
                };
            }

            return t.Status switch
            {
                RemediationStatus.Success => $"✓ {where}: {successText}.",
                RemediationStatus.Skipped => $"• {where}: skipped — {t.Message}",
                RemediationStatus.PermissionDenied => $"⛔ {where}: {t.Message}",
                RemediationStatus.Blocked => $"⛔ {where}: {t.Message}",
                RemediationStatus.Error => $"✗ {where}: {t.Message}",
                _ => $"{where}: {t.Message}"
            };
        }

        private static ResultSeverity SeverityOf(RemediationRunReport report)
        {
            if (report.Status is RemediationRunStatus.NotConfirmed
                or RemediationRunStatus.NoHandler
                or RemediationRunStatus.UnapplyNotSupported)
                return ResultSeverity.Info;

            // A permanent unlogged failure, or any hard error/block/permission-denied,
            // is loud.
            if (report.Targets.Any(t =>
                    (t.AppliedButUnlogged && t.AuditFailureKind == AuditWriteFailureKind.Permanent)
                    || t.Status is RemediationStatus.Error or RemediationStatus.Blocked or RemediationStatus.PermissionDenied))
                return ResultSeverity.Error;

            if (report.Targets.Any(t => t.AppliedButUnlogged || t.Status == RemediationStatus.Skipped))
                return ResultSeverity.Warning;

            return report.AnySuccess ? ResultSeverity.Success : ResultSeverity.Info;
        }

        public enum ResultSeverity { Info, Success, Warning, Error }

        public sealed class DetailItemView : INotifyPropertyChanged
        {
            public string Heading { get; set; } = "";
            public string? Body { get; set; }
            public bool IsCodeBlock { get; set; }
            public List<FieldView> Fields { get; } = new();

            public bool IsProse => !IsCodeBlock && !string.IsNullOrEmpty(Body);
            public bool HasFields => Fields.Count > 0;

            /// <summary>Structured remediation payload (null when this block is not an applicable fix).</summary>
            public RemediationAction? Remediation { get; set; }

            /// <summary>Whether to render the Apply/Un-apply buttons at all (set before binding).</summary>
            public bool ShowApply { get; set; }

            /// <summary>Tooltip explaining why Apply is disabled (set before binding).</summary>
            public string? ApplyDisabledReason { get; set; }

            private bool _serverResolved;
            public void SetServerResolved(bool resolved)
            {
                _serverResolved = resolved;
                OnChanged(nameof(CanApply));
            }

            private bool _isBusy;
            public bool IsBusy
            {
                get => _isBusy;
                private set { _isBusy = value; OnChanged(nameof(IsBusy)); OnChanged(nameof(CanApply)); }
            }

            /// <summary>Enabled only for a known fix + resolved server + not mid-run.</summary>
            public bool CanApply => ShowApply && _serverResolved && !_isBusy;

            private string? _resultText;
            public string? ResultText
            {
                get => _resultText;
                private set { _resultText = value; OnChanged(nameof(ResultText)); OnChanged(nameof(HasResult)); }
            }
            public bool HasResult => !string.IsNullOrEmpty(_resultText);

            private Brush _resultBrush = SystemColors.ControlTextBrush;
            public Brush ResultBrush
            {
                get => _resultBrush;
                private set { _resultBrush = value; OnChanged(nameof(ResultBrush)); }
            }

            private FontWeight _resultBold = FontWeights.Normal;
            public FontWeight ResultBold
            {
                get => _resultBold;
                private set { _resultBold = value; OnChanged(nameof(ResultBold)); }
            }

            public void BeginRun(string message)
            {
                IsBusy = true;
                ResultBrush = BrushFor(ResultSeverity.Info);
                ResultBold = FontWeights.Normal;
                ResultText = message;
            }

            public void SetResult(string message, ResultSeverity severity)
            {
                IsBusy = false;
                ResultBrush = BrushFor(severity);
                ResultBold = severity is ResultSeverity.Error or ResultSeverity.Warning
                    ? FontWeights.SemiBold
                    : FontWeights.Normal;
                ResultText = message;
            }

            private static Brush BrushFor(ResultSeverity severity)
            {
                var key = severity switch
                {
                    ResultSeverity.Success => "SuccessBrush",
                    ResultSeverity.Warning => "WarningBrush",
                    ResultSeverity.Error => "ErrorBrush",
                    _ => "ForegroundBrush"
                };
                return Application.Current?.TryFindResource(key) as Brush ?? SystemColors.ControlTextBrush;
            }

            public event PropertyChangedEventHandler? PropertyChanged;
            private void OnChanged(string name) =>
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
        }

        public sealed class FieldView
        {
            public string Label { get; set; } = "";
            public string Value { get; set; } = "";
        }
    }
}
