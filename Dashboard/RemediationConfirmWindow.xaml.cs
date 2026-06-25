/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System.Collections.Generic;
using System.Globalization;
using System.Windows;
using PerformanceMonitorDashboard.Services.Remediation;

namespace PerformanceMonitorDashboard
{
    /// <summary>
    /// The five-W confirm gate for an Apply / Un-apply. Shows the exact SQL, which
    /// server + database(s), the executing identity (SUSER_SNAME) and the operator,
    /// the original regression factor, and the M2 "verify the forced plan is still
    /// better against current data" caveat. The dialog only returns true when the
    /// operator explicitly clicks the apply button — that return value is the sole
    /// thing that lets <see cref="RemediationApplyService"/> reach the privileged
    /// handler.
    /// </summary>
    public partial class RemediationConfirmWindow : Window
    {
        // ── B3 Phase 3: informed-consent gate state ──────────────────────────────
        // The dialog IS the trust boundary. For a destructive (RequiresInformedConsent)
        // request, ConfirmButton is enabled ONLY when every risk checkbox is ticked AND
        // the base apply-ability holds AND (if resolved-by-name) the by-name ack is ticked.
        private readonly bool _requiresConsent;
        private readonly bool _resolvedByName;
        private bool _baseActionable;                 // audit present + at least one actionable target
        private readonly List<RiskRow> _changingRisks = new();
        private readonly List<RiskRow> _notChangingRisks = new();

        public RemediationConfirmWindow(RemediationConfirmRequest request, bool resolvedByName, string? resolvedByNameReason)
        {
            InitializeComponent();

            _requiresConsent = request.RequiresInformedConsent;
            _resolvedByName = resolvedByName;

            var verb = request.IsUnapply ? "Un-apply Fix" : "Apply Fix";
            Title = $"Confirm {verb}";
            var isDbConfig = string.Equals(request.FactKey, "DB_CONFIG", System.StringComparison.Ordinal);
            var isRcsi = string.Equals(request.FactKey, "RCSI", System.StringComparison.Ordinal);
            var isClearPlan = string.Equals(request.FactKey, "CLEAR_PLAN", System.StringComparison.Ordinal);
            HeaderText.Text = isRcsi
                ? $"Enable READ_COMMITTED_SNAPSHOT (RCSI) — a database-wide concurrency change — on {request.ServerDisplayName}?"
                : isClearPlan
                    ? $"Clear the cached plan(s) for an abnormally-expensive query (DBCC FREEPROCCACHE) on {request.ServerDisplayName}?"
                    : isDbConfig
                        ? $"Apply the always-safe database setting change(s) on {request.ServerDisplayName}?"
                        : request.IsUnapply
                            ? $"Un-apply (unforce) the forced plan on {request.ServerDisplayName}?"
                            : $"Force the historical-better plan on {request.ServerDisplayName}?";

            ServerText.Text = request.ServerDisplayName;
            ExecutingText.Text = string.IsNullOrEmpty(request.ExecutingLogin)
                ? "(monitoring login — re-probed at execution time via SUSER_SNAME())"
                : request.ExecutingLogin;
            OperatorText.Text = request.OperatorIdentity;

            if (resolvedByName)
            {
                ByNameBanner.Visibility = Visibility.Visible;
                ByNameText.Text = resolvedByNameReason
                    ?? "The source server was resolved by name (the alert did not carry a stable server id). "
                       + "Confirm this is the intended server before applying.";
            }

            TargetsHeader.Text = request.Targets.Count == 1 ? "Target" : $"Targets ({request.Targets.Count})";
            var rows = new List<TargetRow>();
            foreach (var t in request.Targets)
                rows.Add(TargetRow.From(t, request.IsUnapply));
            TargetsList.ItemsSource = rows;

            // Fact-key-specific caveat. RCSI: the always-safe caveat does NOT apply (the
            // two-sided risk sections below carry the framing). DB_CONFIG: the always-safe
            // note (RCSI excluded). PLAN_REGRESSION: the M2 still-better caveat (apply-time
            // judgment; hidden on un-apply where there is no "still better" decision).
            if (isRcsi)
            {
                CaveatBanner.Visibility = Visibility.Collapsed;
            }
            else if (isClearPlan)
            {
                // One-line framing (§6); the two-sided risk sections below carry the full
                // disclosure + the acknowledge-each-risk gate.
                CaveatText.Text =
                    "Clearing a cached plan forces a recompile; it may not produce a better plan — if this "
                    + "query is parameter-sensitive or a known plan regression, prefer those fixes. Read both risk lists.";
            }
            else if (isDbConfig)
            {
                CaveatText.Text =
                    "These three settings are always-safe to change on a live database (online, no blocking). "
                    + "RCSI (READ_COMMITTED_SNAPSHOT) is intentionally excluded — test it on a copy first.";
            }
            else if (request.IsUnapply)
            {
                CaveatBanner.Visibility = Visibility.Collapsed;
            }
            else
            {
                CaveatText.Text = RemediationConfirmRequest.StillBetterCaveat;
            }

            // B3 Phase 3: render the two-sided acknowledge-each-risk sections for a
            // destructive request. ALWAYS render both sections (RCSI has >= 1 each).
            if (_requiresConsent && request.Risks is not null)
            {
                ConsentSection.Visibility = Visibility.Visible;
                ConsentBannerText.Text = isRcsi
                    ? "RCSI is a database-wide concurrency change. Read both risk lists; every box must be checked to enable Apply."
                    : isClearPlan
                        ? "Clearing a cached plan is possibly destructive and cannot be undone — it live-resolves and clears every currently-cached plan for this query hash. Read both risk lists; every box must be checked to enable Apply."
                        : "This is a possibly-destructive change. Read both risk lists; every box must be checked to enable Apply.";

                foreach (var r in request.Risks.RisksOfChanging)
                    _changingRisks.Add(new RiskRow(r.Text));
                foreach (var r in request.Risks.RisksOfNotChanging)
                    _notChangingRisks.Add(new RiskRow(r.Text));

                ChangingRisksList.ItemsSource = _changingRisks;
                NotChangingRisksList.ItemsSource = _notChangingRisks;
            }

            SqlPreview.Text = request.PreviewSql;

            // Audit-table-absent hard block: the privileged core would block every
            // target with no mutation, so disable the confirm button and say why.
            // _baseActionable is the audit-present + actionable predicate the consent /
            // by-name gates further restrict — never loosen.
            if (!request.AuditTableExists)
            {
                AuditAbsentBanner.Visibility = Visibility.Visible;
                AuditAbsentText.Text =
                    "This server is not on the 3.0.0 schema (config.remediation_action_log is absent). "
                    + "Apply Fix is hard-blocked here — no change will be made. Upgrade this server to "
                    + "3.0.0 to enable audited Apply Fix.";
                ConfirmButton.Content = request.IsUnapply ? "Un-apply" : "Apply";
                _baseActionable = false;
                ConfirmButton.ToolTip = AuditAbsentText.Text;
            }
            else if (!request.IsUnapply && !request.AnyActionable)
            {
                // Nothing applyable (already forced / stale / QS off / no ALTER / wrong DB).
                ConfirmButton.Content = $"Apply to {request.ServerDisplayName}";
                _baseActionable = false;
                ConfirmButton.ToolTip = "No target is in an applyable state — see the per-target notes above.";
            }
            else
            {
                ConfirmButton.Content = request.IsUnapply
                    ? $"Un-apply on {request.ServerDisplayName}"
                    : $"Apply to {request.ServerDisplayName}";
                _baseActionable = true;
            }

            // LOW-2 (wrong-server boundary): when the source server was resolved by
            // NAME (the alert lacked a stable id and a unique name matched), a server
            // renamed/replaced since the alert could be a *different* target. Remove
            // the Enter-key click-through, and — only if Apply would otherwise be
            // enabled — require an explicit acknowledgement checkbox before enabling it.
            // The destructive-consent risk boxes ALSO suppress the default-Enter and add
            // their own N-checkbox gate; both combine in RecomputeConfirmEnabled.
            if (resolvedByName || _requiresConsent)
                ConfirmButton.IsDefault = false;

            if (resolvedByName && _baseActionable)
            {
                ByNameAckCheck.Visibility = Visibility.Visible;
                ByNameAckCheck.IsChecked = false;
            }

            RecomputeConfirmEnabled();
        }

        /// <summary>
        /// The single enablement predicate (generalizes the LOW-2 by-name ack to N risk
        /// boxes). Apply is enabled ONLY when the base apply-ability holds AND, for a
        /// destructive request, EVERY risk checkbox is ticked, AND, for a by-name-resolved
        /// server, the by-name ack is ticked. A destructive RCSI on a by-name target
        /// therefore requires BOTH all risk boxes AND the by-name ack.
        /// </summary>
        private void RecomputeConfirmEnabled()
        {
            var allRiskBoxesChecked = _changingRisks.TrueForAll(r => r.IsChecked)
                && _notChangingRisks.TrueForAll(r => r.IsChecked);
            var riskBoxCount = _changingRisks.Count + _notChangingRisks.Count;
            var byNameAck = ByNameAckCheck.IsChecked == true;

            var enabled = ComputeConfirmEnabled(
                _baseActionable, _requiresConsent, allRiskBoxesChecked, _resolvedByName, byNameAck, riskBoxCount);
            ConfirmButton.IsEnabled = enabled;

            if (enabled)
            {
                ConfirmButton.ClearValue(ToolTipProperty);
            }
            else if (!_baseActionable)
            {
                // Keep the audit-absent / not-actionable tooltip already set above.
            }
            else if (_requiresConsent && !allRiskBoxesChecked)
            {
                ConfirmButton.ToolTip = "Acknowledge every risk (all checkboxes above) to enable.";
            }
            else if (_resolvedByName && !byNameAck)
            {
                ConfirmButton.ToolTip = "Confirm the target server (checkbox above) to enable.";
            }
        }

        /// <summary>
        /// PURE enablement predicate — the whole consent gate (B-1). Apply is enabled ONLY
        /// when the base apply-ability holds AND, for a destructive request, EVERY risk
        /// checkbox is ticked, AND, for a by-name-resolved server, the by-name ack is
        /// ticked. A destructive RCSI on a by-name target requires BOTH all risk boxes AND
        /// the by-name ack. Extracted as internal static so the HARD gate-enforcement test
        /// (Dashboard.Tests) verifies the exact predicate the dialog uses, without WPF.
        /// </summary>
        internal static bool ComputeConfirmEnabled(
            bool baseActionable,
            bool requiresConsent,
            bool allRiskBoxesChecked,
            bool resolvedByName,
            bool byNameAck,
            int riskBoxCount)
        {
            if (!baseActionable)
                return false;
            // FAIL CLOSED (LOW-1): a destructive request enables Apply ONLY when there is at
            // least one rendered risk box AND every box is checked. List.TrueForAll on an
            // EMPTY list returns true, so without the riskBoxCount > 0 guard a FUTURE
            // destructive handler whose disclosure is empty/null would enable Apply with zero
            // acknowledged checkboxes. The count guard removes that implicit coupling.
            if (requiresConsent && !(riskBoxCount > 0 && allRiskBoxesChecked))
                return false;
            if (resolvedByName && !byNameAck)
                return false;
            return true;
        }

        // Re-evaluates the full enablement predicate when any risk checkbox toggles.
        private void RiskBox_Changed(object sender, RoutedEventArgs e) => RecomputeConfirmEnabled();

        // Gates Apply on the explicit by-name acknowledgement (LOW-2), combined with the
        // destructive-consent risk boxes via the unified RecomputeConfirmEnabled.
        private void ByNameAck_Changed(object sender, RoutedEventArgs e) => RecomputeConfirmEnabled();

        private void ConfirmButton_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = true;
            Close();
        }

        private void CancelButton_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = false;
            Close();
        }

        /// <summary>
        /// One acknowledge-each-risk checkbox row (B3 Phase 3). <see cref="IsChecked"/>
        /// is two-way bound to the CheckBox; the unified RecomputeConfirmEnabled reads it.
        /// The CheckBox Checked/Unchecked events also fire RiskBox_Changed so the gate
        /// re-evaluates on every toggle. Text is read-only/wrap-enabled (display only).
        /// </summary>
        private sealed class RiskRow
        {
            public RiskRow(string text) => Text = text;
            public string Text { get; }
            public bool IsChecked { get; set; }
        }

        /// <summary>Bindable projection of one confirm target.</summary>
        private sealed class TargetRow
        {
            public string HeadLine { get; private set; } = "";
            public string StatusLine { get; private set; } = "";

            public static TargetRow From(RemediationConfirmTarget t, bool isUnapply)
            {
                // DB_CONFIG rows carry a fact-key-neutral StatusTitle (no query_id/plan_id);
                // force-plan rows render the query_id/plan_id head + the M2 regression Nx.
                string head;
                if (!string.IsNullOrEmpty(t.StatusTitle))
                {
                    head = t.StatusTitle!;
                }
                else
                {
                    head = $"[{t.Database}]  query_id {t.QueryId}, plan_id {t.PlanId}";
                    if (!isUnapply && t.RegressionFactor > 0)
                        head += $"  —  regression {t.RegressionFactor.ToString("0.#", CultureInfo.InvariantCulture)}x";
                }

                var status = DescribeDisposition(t, isUnapply);
                return new TargetRow { HeadLine = head, StatusLine = status };
            }

            private static string DescribeDisposition(RemediationConfirmTarget t, bool isUnapply)
            {
                if (isUnapply)
                    return "Will unforce if this plan was forced by Apply Fix; otherwise skipped.";

                return t.Disposition switch
                {
                    RemediationDisposition.Ok => "Ready to apply.",
                    RemediationDisposition.WarnFailing => "⚠ " + (t.DispositionMessage ?? "Has a prior force failure; re-forcing may not help."),
                    RemediationDisposition.AlreadyForced => "Already forced — will be skipped.",
                    RemediationDisposition.BlockStale => "Plan/query no longer present — will be skipped.",
                    RemediationDisposition.BlockQueryStoreOff => "Query Store is not READ_WRITE — cannot force.",
                    RemediationDisposition.BlockNoAlter => "Monitoring login lacks ALTER — will fail closed (no change).",
                    RemediationDisposition.BlockWrongDatabase => "Connected DB does not match the target — will not proceed.",
                    RemediationDisposition.BlockAuditTableAbsent => "Audit table absent (pre-3.0.0) — hard-blocked.",
                    RemediationDisposition.AlreadyInDesiredState => "Already in the desired state — will be skipped.",
                    RemediationDisposition.BlockDatabaseNotFound => "Database not found on the server — will not proceed.",
                    _ => t.DispositionMessage ?? "Unable to determine target state."
                };
            }
        }
    }
}
