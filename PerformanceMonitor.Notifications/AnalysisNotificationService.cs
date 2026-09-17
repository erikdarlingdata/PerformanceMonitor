/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Routes high-severity analysis findings into the notification channels.
/// Filters by severity, dedups per finding (so a recurring finding does not
/// re-notify every analysis cycle), composes a readable message, and hands off
/// to an <see cref="IFindingAlertSender"/> — each app's <c>EmailAlertService</c> —
/// which fans out to email + Slack + Teams and records the alert per that app's
/// history cadence.
///
/// <para>
/// Shared between Lite and Dashboard (Plan E E3c). The per-app divergences are absorbed
/// by two injected dependencies: a <c>serverId</c> resolver (Lite uses the finding's int
/// id as a string; Dashboard resolves the matching <c>ServerConnection</c> GUID and falls
/// back to the int id) and the <see cref="IFindingAlertSender"/> (which owns the per-app
/// record cadence — Approach B, plan §4.6).
/// </para>
/// </summary>
public sealed class AnalysisNotificationService
{
    private readonly IFindingAlertSender _sender;
    private readonly IAlertSettings _settings;
    private readonly Func<AnalysisFinding, string> _resolveServerId;
    private readonly Func<string, bool>? _isServerSilenced;
    private readonly ILogger<AnalysisNotificationService> _logger;
    private readonly Action<string, string>? _showTrayNotification;

    /// <summary>
    /// Per-symptom notification state (escalate-on-CRITICAL keying). A below-critical finding is
    /// keyed by its incident ("{serverId}:{IncidentId}", falling back to its StoryPathHash when it has
    /// no incident id — legacy rows / absolution findings — so those never collapse into one shared
    /// bucket), so co-fired non-critical symptoms stay one-e-mail-per-incident. A CRITICAL-band finding
    /// (severity &gt;= <see cref="CriticalSeverityCutoff"/>) is instead keyed by its OWN StoryPathHash, so
    /// a new critical escalates past that dedup and is never silently held inside an already-alerted
    /// incident. Seeded lazily from the alert log on first lookup per key so a symptom that just fired
    /// stays suppressed across an app restart.
    ///
    /// <para>#2054 fresh-or-worsening: each bucket remembers the severity it last NOTIFIED at and the
    /// last time its story was SEEN at all. A story that keeps firing at the same severity notifies
    /// once and then stays silent — the first weekday on a 52-server fleet showed why: ~46 of 50
    /// alerts were the IDENTICAL ambient chain sitting exactly at the notify threshold, once per
    /// server, drowning the one genuine outlier. Re-notification needs the story to WORSEN
    /// (<see cref="WorseningStep"/>) — the same standing-condition treatment the PVS-pressure alert
    /// shipped with — or to RESOLVE first: a bucket unseen for 2 × AnalysisNotifyCooldownMinutes is
    /// pruned, so a chain that stops firing and later returns is fresh and notifies again. Pruning is
    /// by last-SEEN, deliberately not last-notified: a standing ambient chain refreshes its bucket
    /// every cycle and never ages back into freshness while it persists.</para>
    /// </summary>
    private readonly ConcurrentDictionary<string, BucketState> _cooldowns = new();

    /// <summary>One bucket's memory: when it last notified, at what severity, and when its story was
    /// last SEEN (notified or held — the prune horizon runs on this, so a persisting story cannot age
    /// back into freshness while it keeps firing).</summary>
    private sealed record BucketState(DateTime LastNotified, double LastNotifiedSeverity, DateTime LastSeen);

    /// <summary>
    /// How much a story's severity must rise over its last-notified level to re-notify while it keeps
    /// firing (#2054) — the analysis twin of the PVS alert's worsening re-fire. 0.25 on the 0–2 band:
    /// the fleet's ambient chain sat at exactly 1.50 while the one server worth a second look reached
    /// 1.80, so a quarter-band step separates precisely those two on live evidence. A restart-seeded
    /// bucket has no persisted severity and conservatively assumes the notify threshold, so the first
    /// post-restart re-notify needs threshold + step.
    /// </summary>
    private const double WorseningStep = 0.25;

    /// <summary>
    /// Severity at/above which a finding is CRITICAL-band — the canonical <c>&gt;= 1.5</c> cutoff every
    /// recommendation reader bands CRITICAL by (Dashboard <c>RecommendationDeduper.FromEngineSeverity</c>,
    /// Lite <c>LiteRecommendationsReader.SeverityBand</c>, and the Darling viewer's
    /// <c>ViewerDataService.SeverityBand</c>). A finding at/above this escalates past the per-incident
    /// e-mail dedup: it gets its own per-finding re-notify bucket, so a new critical joining an
    /// already-alerted incident e-mails fresh and is never silently held. Below-critical co-fired
    /// symptoms stay deduped under the incident bucket. Not a fresh literal — it keys off the same
    /// value the shared severity bands use (there is no single named constant to import; the readers
    /// each spell the cutoff inline, and this service references only PerformanceMonitor.Analysis).
    /// </summary>
    private const double CriticalSeverityCutoff = 1.5;

    /// <param name="sender">The per-app alert dispatcher (its <c>EmailAlertService</c>).</param>
    /// <param name="settings">Alert settings (severity threshold + cooldown; clamped per app).</param>
    /// <param name="serverIdResolver">
    /// Resolves the persistence <c>serverId</c> for a finding. Lite: <c>f =&gt; f.ServerId.ToString()</c>;
    /// Dashboard: the <c>IServerManager</c> GUID lookup with the int-id fallback.
    /// </param>
    /// <param name="logger">Diagnostic logger.</param>
    /// <param name="isServerSilenced">
    /// Optional predicate (keyed by resolved <c>serverId</c>) that suppresses notifications
    /// for a silenced server. Dashboard passes its <c>AlertStateService.IsAnySilencingActive</c>
    /// so "Silence All Alerts" also stops analysis-finding emails; Lite has no silencing
    /// feature and leaves this null (never silenced).
    /// </param>
    /// <param name="showTrayNotification">
    /// Optional <c>(title, message)</c> sink raised for every finding that notifies — the same
    /// notify-worthy set that reaches email/webhook — so a local-only user with no channel
    /// configured still gets a visible signal. Dashboard wires this to its tray
    /// <c>NotificationService.ShowNotification</c> (which itself honors the global
    /// notifications-enabled pref and marshals to the UI thread); Lite leaves it null. Best-effort:
    /// invoked inside the per-finding try, so a sink fault is logged, not propagated.
    /// </param>
    public AnalysisNotificationService(
        IFindingAlertSender sender,
        IAlertSettings settings,
        Func<AnalysisFinding, string> serverIdResolver,
        ILogger<AnalysisNotificationService> logger,
        Func<string, bool>? isServerSilenced = null,
        Action<string, string>? showTrayNotification = null)
    {
        _sender = sender;
        _settings = settings;
        _resolveServerId = serverIdResolver;
        _logger = logger;
        _isServerSilenced = isServerSilenced;
        _showTrayNotification = showTrayNotification;
    }

    /// <summary>
    /// Notifies at or above the configured severity, collapsing co-fired symptoms to one e-mail per
    /// incident per cycle — EXCEPT a CRITICAL-band symptom (severity &gt;= <see cref="CriticalSeverityCutoff"/>),
    /// which holds its own re-notify bucket and so escalates past the dedup: a new critical joining an
    /// already-alerted incident sends a fresh e-mail naming that critical symptom, while below-critical
    /// co-fired symptoms stay held under the incident bucket. Each cycle sends one e-mail per incident,
    /// led by the highest-severity member whose bucket is not cooling and naming the rest as co-fired.
    /// Never throws.
    /// </summary>
    public async Task NotifyAsync(IReadOnlyList<AnalysisFinding> findings)
    {
        if (findings is null || findings.Count == 0)
            return;

        // Bounds are enforced in the settings adapter (Dashboard clamps AnalysisNotifySeverity
        // to [0, 2] and AnalysisNotifyCooldownMinutes to [30, 10080]; Lite passes through);
        // the service consumes the already-clamped values.
        var threshold = _settings.AnalysisNotifySeverity;
        var cooldown = TimeSpan.FromMinutes(_settings.AnalysisNotifyCooldownMinutes);
        var now = DateTime.UtcNow;

        /* Drop entries UNSEEN for 2× cooldown so the dict stays bounded AND a resolved story becomes
           fresh again (#2054): last-SEEN, not last-notified — a standing ambient chain refreshes its
           bucket every cycle it fires and so never ages back into freshness while it persists, while
           a chain that genuinely stopped firing is forgotten and re-alerts on recurrence. If a key
           here also matches an incident in this batch, the per-incident seed below re-adds it from
           history; that's a wash, not a bug. */
        var pruneBefore = now - TimeSpan.FromTicks(cooldown.Ticks * 2);
        foreach (var stale in _cooldowns)
        {
            if (stale.Value.LastSeen < pruneBefore)
                _cooldowns.TryRemove(stale.Key, out _);
        }

        /* Group the notify-worthy findings by incident so co-firing symptoms collapse into ONE e-mail
           per cycle. The group token is the finding's IncidentId, falling back to its StoryPathHash
           when empty — a legacy or absolution finding has no incident id, and without the fallback
           every empty-id finding would collapse into a single bucket. ServerId is part of the key so
           two servers never share a bucket. GroupBy preserves first-seen order, keeping alert ordering
           stable. */
        var incidents = findings
            .Where(f => f is not null && f.Severity >= threshold)
            .GroupBy(f => $"{f.ServerId}:{IncidentToken(f)}");

        foreach (var incident in incidents)
        {
            /* Members ordered so the highest-severity finding leads and the rest are named as co-fired.
               Highest severity wins, root-key- then hash-tiebroken for a deterministic order (mirrors
               IncidentId.Compute's severity-desc, ordinal-root-key tiebreak). */
            var members = incident
                .OrderByDescending(f => f.Severity)
                .ThenBy(f => f.RootFactKey, StringComparer.Ordinal)
                .ThenBy(f => f.StoryPathHash, StringComparer.Ordinal)
                .ToList();

            /* ServerId is identical for every member (it is part of the group key); resolve once. */
            var serverId = _resolveServerId(members[0]);

            /* Honor per-server silencing (Dashboard "Silence All Alerts"). Checked after
               resolving serverId but before the cooldown seed/stamp, so a silenced server
               neither notifies nor consumes its cooldown — unsilencing resumes immediately. */
            if (_isServerSilenced is not null && _isServerSilenced(serverId))
                continue;

            /* Per-member re-notification bucket (escalate-on-CRITICAL): a CRITICAL-band member
               (severity >= the shared >= 1.5 cutoff) gets its OWN per-finding bucket keyed on its
               StoryPathHash, so a NEW critical symptom escalates past the incident dedup and e-mails
               even when the incident already alerted on another member. A below-critical member stays
               on the shared per-incident bucket, so co-fired non-critical symptoms remain one-e-mail-
               per-incident. */
            string BucketKey(AnalysisFinding f) =>
                f.Severity >= CriticalSeverityCutoff
                    ? $"{serverId}:{f.StoryPathHash}"
                    : $"{serverId}:{IncidentToken(f)}";

            /* Seed each not-yet-known bucket from the alert log on first lookup so a symptom that
               fired shortly before an app restart is not re-fired afterward. The persisted equivalent
               is the latest row for that member's metric_name (which embeds the finding's hash).
               History carries no severity, so the seed conservatively assumes the notify threshold —
               the minimum a notified finding can have had — and the first post-restart re-notify needs
               threshold + WorseningStep (#2054). Below-critical members share one bucket, so only the
               first (highest-severity) below-critical member — the one that would lead a
               below-critical e-mail — is looked up. */
            foreach (var m in members)
            {
                var seedKey = BucketKey(m);
                if (_cooldowns.ContainsKey(seedKey))
                    continue;
                var lastPersisted = await _sender.GetLastAlertTimeAsync(serverId, FindingMessageFormatter.MetricName(m));
                if (lastPersisted.HasValue)
                    _cooldowns.TryAdd(seedKey, new BucketState(lastPersisted.Value, threshold, now));
            }

            /* Lead = the highest-severity member whose bucket allows a notification (#2054
               fresh-or-worsening): a member with NO bucket is fresh and notifies; a member with a
               bucket re-notifies ONLY when it worsened by WorseningStep over the severity it last
               notified at AND its last notification is outside the cooldown (the cooldown keeps a
               rapidly-climbing story from e-mailing every analysis cycle; genuine escalation is
               band-jumping, and a new critical already gets its own fresh hash bucket). Steady
               severity holds forever while the story keeps firing — the whole point: an ambient
               fleet truth notifies once per server, not once per cooldown expiry. The whole incident
               is held only when EVERY member holds (send-if-any-fresh, unchanged). */
            AnalysisFinding? lead = null;
            foreach (var m in members)
            {
                if (_cooldowns.TryGetValue(BucketKey(m), out var state)
                    && (now - state.LastNotified < cooldown || m.Severity < state.LastNotifiedSeverity + WorseningStep))
                    continue;
                lead = m;
                break;
            }

            if (lead is null)
            {
                /* Held entirely — still refresh every member's last-SEEN so a standing story keeps
                   its bucket alive (the prune horizon runs on LastSeen; see the field doc). */
                foreach (var m in members)
                {
                    if (_cooldowns.TryGetValue(BucketKey(m), out var held))
                        _cooldowns[BucketKey(m)] = held with { LastSeen = now };
                }

                continue;
            }

            try
            {
                var context = FindingMessageFormatter.BuildContext(lead, threshold);

                /* Name the OTHER findings that co-fired in THIS incident, in the one message, so the
                   single alert still accounts for everything the incident surfaced. Reuses the shared
                   CoFiredSummary the viewer/MCP surfaces use, but with the INCIDENT-scoped lead-in so the
                   body matches the "Co-fired in this incident" heading (the default window wording would
                   be inaccurate here — these siblings are incident-scoped, not the whole run). Null (and
                   so no item) for a lone finding, leaving the message byte-identical to the pre-dedup
                   single-finding path. */
                var coFired = CoFiredSummary.Line(
                    CoFiredSummary.OtherTitles(FindingTitle(lead),
                        members.Select(m => (FindingTitle(m), m.Severity))),
                    leadIn: CoFiredSummary.IncidentLeadIn);
                if (coFired is not null)
                    context.Details.Add(new AlertDetailItem { Heading = "Co-fired in this incident", Body = coFired });

                /* SendFindingAlertAsync fans out to email + Slack + Teams and records the
                   alert per this app's cadence. It returns no success/failure signal, so the
                   buckets are stamped regardless — a symptom whose delivery failed is
                   suppressed for the full cooldown (accepted best-effort behavior). */
                await _sender.SendFindingAlertAsync(new FindingAlert(
                    FindingMessageFormatter.MetricName(lead),
                    lead.ServerName,
                    FindingMessageFormatter.CurrentValue(lead),
                    threshold.ToString("F1"),
                    serverId,
                    context,
                    lead.Severity,
                    threshold,
                    FindingMessageFormatter.DetailText(lead, threshold),
                    /* The prose is not delivered, and the row still persists it. DetailText and
                       BuildContext's "Diagnosis" item are two renderings of the SAME values — story,
                       severity, notify threshold, confidence, fact count, database, window — so a channel
                       that renders the structured context and the prose prints every one of them twice.
                       They differ in labels ("Facts in chain" vs "Facts", one combined severity line vs
                       two fields) and in the window separator, so the textual equality in
                       AlertDetailText.ProseForDelivery cannot see that they are the same facts; the
                       producer has to say so. Delivery is the only half suppressed: DetailText remains
                       config_alert_log.detail_text, which is the sole copy of these facts on the surfaces
                       that render no structured context, and the input the mute pre-fill parses. */
                    DeliverDetailText: false));

                /* Always raise the tray balloon for a notify-worthy incident (user choice), the
                   same visible signal threshold alerts already pop — so a local-only user with no
                   email/webhook still sees it. No-op when the host wired no sink (Lite) or tray
                   notifications are disabled (the sink checks the pref). */
                if (_showTrayNotification is not null)
                {
                    var (title, message) = FindingMessageFormatter.BalloonText(lead);
                    _showTrayNotification(title, message);
                }

                /* Stamp EVERY member's bucket (send-if-any-fresh, stamp-all): the e-mail named every
                   member, so none should re-fire until it WORSENS past what was just notified — the
                   just-led critical on its hash bucket, each co-fired critical on its own, and every
                   below-critical member on the shared incident bucket. Members arrive severity-DESC,
                   so the FIRST write to a shared bucket carries its highest member's severity; later
                   (lower) members must not overwrite it downward, or a mid-severity member would
                   spuriously "worsen" past the lowest next cycle. */
                var stamped = new HashSet<string>(StringComparer.Ordinal);
                foreach (var m in members)
                {
                    var key = BucketKey(m);
                    if (stamped.Add(key))
                        _cooldowns[key] = new BucketState(now, m.Severity, now);
                }
            }
            catch (Exception ex)
            {
                /* SendFindingAlertAsync is documented never to throw; this guards a
                   formatter defect so one bad incident cannot abort the rest. */
                _logger.LogError(
                    $"AnalysisNotificationService: failed to notify on incident {incident.Key}: {ex.GetType().Name}: {ex.Message}");
            }
        }
    }

    /// <summary>
    /// The batch/cooldown grouping token for a finding: its incident id, or its StoryPathHash when the
    /// incident id is empty (legacy / absolution findings) so those stay one-bucket-per-finding rather
    /// than collapsing together.
    /// </summary>
    private static string IncidentToken(AnalysisFinding f) =>
        string.IsNullOrEmpty(f.IncidentId) ? f.StoryPathHash : f.IncidentId;

    /// <summary>
    /// A finding's human-readable title for the co-fired cross-reference — its advice headline when the
    /// shared library has one, else its root fact key (finally its category), matching the title the
    /// viewer/MCP co-fired surfaces use.
    /// </summary>
    private static string FindingTitle(AnalysisFinding f) =>
        FactAdvice.GetForFinding(f)?.Headline
            ?? (string.IsNullOrEmpty(f.RootFactKey) ? f.Category : f.RootFactKey);
}

/// <summary>
/// Composes the arguments for an analysis-finding notification. The engine never
/// populates <see cref="AnalysisFinding.StoryText"/>, so the readable message is
/// built here from the finding's structured fields and drill-down detail.
/// </summary>
internal static class FindingMessageFormatter
{
    private const int FieldValueLimit = 300;

    /// <summary>
    /// Alert metric name. The "Analysis: " prefix groups these in the Alerts tab; the
    /// short hash suffix makes each distinct finding unique, so the {serverId}:{metricName}
    /// cooldown cannot collapse two findings sharing a Category.
    /// </summary>
    public static string MetricName(AnalysisFinding finding)
    {
        var hash = finding.StoryPathHash ?? string.Empty;
        var shortHash = hash.Length >= 8 ? hash[..8] : hash;
        var category = string.IsNullOrEmpty(finding.Category) ? "finding" : finding.Category;
        return $"Analysis: {category} [{shortHash}]";
    }

    /// <summary>
    /// Headline value — the root fact and its value, plus baseline context for anomaly findings.
    /// </summary>
    public static string CurrentValue(AnalysisFinding finding)
    {
        var root = string.IsNullOrEmpty(finding.RootFactKey) ? finding.Category : finding.RootFactKey;
        var sb = new StringBuilder(root);

        if (finding.RootFactValue.HasValue)
            sb.Append($" ({finding.RootFactValue.Value:F1})");

        if (finding.RootFactMetadata is { Count: > 0 })
        {
            var baseline = BaselineContextFormatter.FormatBaselineContext(finding.RootFactMetadata);
            if (baseline is { Count: > 0 })
            {
                var parts = baseline.Select(kv => $"{Humanize(kv.Key)} {kv.Value}");
                sb.Append(" — ").Append(string.Join(", ", parts));
            }
        }

        return sb.ToString();
    }

    /// <summary>
    /// Tray-balloon title + message for a finding (WS2). Concise by design — balloons truncate.
    /// Title names the category + server; message is the headline value (root fact + baseline
    /// context), prefixed with the database when the finding is database-scoped.
    /// </summary>
    public static (string Title, string Message) BalloonText(AnalysisFinding finding)
    {
        var category = string.IsNullOrEmpty(finding.Category) ? "Performance finding" : finding.Category;
        var server = string.IsNullOrEmpty(finding.ServerName) ? "this server" : finding.ServerName;
        var title = $"Analysis: {category} on {server}";

        var message = CurrentValue(finding);
        if (!string.IsNullOrEmpty(finding.DatabaseName))
            message = $"{finding.DatabaseName}: {message}";

        return (title, message);
    }

    /// <summary>
    /// Plain-text detail block — the causal chain and supporting metadata. Used as the
    /// alert's <c>detailText</c> (Lite persists it on every analysis row; Dashboard uses it
    /// on the no-channel "tray" fallback row). Threshold is threaded through as a parameter.
    /// </summary>
    public static string DetailText(AnalysisFinding finding, double notifyThreshold)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"  Story: {finding.StoryPath}");
        sb.AppendLine($"  Severity: {finding.Severity:F2} (notify threshold {notifyThreshold:F1})");
        sb.AppendLine($"  Confidence: {finding.Confidence:F2}");
        sb.AppendLine($"  Facts in chain: {finding.FactCount}");

        if (!string.IsNullOrEmpty(finding.DatabaseName))
            sb.AppendLine($"  Database: {finding.DatabaseName}");

        if (finding.TimeRangeStart.HasValue && finding.TimeRangeEnd.HasValue)
            sb.AppendLine($"  Window: {finding.TimeRangeStart.Value:u} - {finding.TimeRangeEnd.Value:u}");

        return sb.ToString().TrimEnd();
    }

    /// <summary>
    /// Builds the structured <see cref="AlertContext"/> for the alert template.
    /// First detail item is the Diagnosis summary; then the Advice/Remediation block
    /// (when the shared analysis library has one for the finding's root fact-key); then
    /// the finding's drill-down detail flattened into label/value pairs.
    /// </summary>
    public static AlertContext BuildContext(AnalysisFinding finding, double notifyThreshold)
    {
        var context = new AlertContext();

        /* Diagnosis summary — fits inside the 600px email template (label column 120px, value column ~480px). */
        var diagnosis = new AlertDetailItem { Heading = "Diagnosis" };
        diagnosis.Fields.Add(("Story", finding.StoryPath ?? string.Empty));
        diagnosis.Fields.Add(("Severity", finding.Severity.ToString("F2")));
        diagnosis.Fields.Add(("Notify threshold", notifyThreshold.ToString("F1")));
        diagnosis.Fields.Add(("Confidence", finding.Confidence.ToString("F2")));
        diagnosis.Fields.Add(("Facts", finding.FactCount.ToString()));
        if (!string.IsNullOrEmpty(finding.DatabaseName))
            diagnosis.Fields.Add(("Database", finding.DatabaseName));
        if (finding.TimeRangeStart.HasValue && finding.TimeRangeEnd.HasValue)
            diagnosis.Fields.Add(("Window", $"{finding.TimeRangeStart.Value:u} → {finding.TimeRangeEnd.Value:u}"));
        context.Details.Add(diagnosis);

        /* Advice (Investigation/Remediation prose) + generated remediation T-SQL, when the
           shared analysis library has a block for this finding's root fact-key. Inserted
           after Diagnosis and before the drill-down so every surface renders the same order:
           Diagnosis → Advice → Remediation T-SQL → drill-down. */
        var advice = FactAdvice.GetForFinding(finding);
        if (advice is not null)
        {
            context.Details.Add(new AlertDetailItem
            {
                Heading = advice.Headline,
                Body = $"Investigation: {advice.Investigation}\n\nRemediation: {advice.Remediation}"
            });

            if (!string.IsNullOrEmpty(advice.RemediationTsql))
            {
                context.Details.Add(new AlertDetailItem
                {
                    Heading = "Remediation T-SQL",
                    Body = advice.RemediationTsql,
                    IsCodeBlock = true,
                    /* Structured, typed payload for an in-app Apply (PR-B). Rides in the
                       persisted contextJson; may be null (e.g. PARAMETER_SENSITIVITY has
                       advice + prose but no force action), in which case no Apply affordance
                       is offered. Built from the same drill-down the preview rendered. */
                    Remediation = FactRemediation.BuildAction(finding)
                });
            }
        }

        /* B3 Phase 3 (PR-B): the DESTRUCTIVE "Enable RCSI (advanced)" affordance is a
           SEPARATE detail item from the always-safe DB-config Apply — a distinct view
           with its own singular Remediation (FactKey "RCSI"), so the two Apply buttons
           live on two views and can never cross. Emitted on ANY config_issues-bearing
           finding where RCSI is OFF + the §3.3 enrichment is present (BuildRcsiAction
           returns non-null); it is NOT gated behind the always-safe block's
           advice.RemediationTsql condition (a finding can offer RCSI even when no
           always-safe setting is wrong). The risk-of-not-changing figures are captured
           HERE (the finding is in hand) onto the action so the in-app dialog renders the
           REAL numbers at apply time; the in-app consent gate is what makes it live. */
        var rcsiAction = FactRemediation.BuildRcsiAction(finding);
        if (rcsiAction is not null)
        {
            context.Details.Add(new AlertDetailItem
            {
                Heading = "Enable RCSI (advanced)",
                Body = FactRemediation.GenerateRcsiPreview(finding),
                IsCodeBlock = true,
                Remediation = rcsiAction
            });

            /* Cross-surface disclosure (§6): the two-sided risk renders as READ-ONLY
               prose on email (both bodies) / webhook (all flow from context.Details) —
               you cannot consent through an email, so there is NO checkbox gate off-app.
               The in-app dialog renders the SAME RiskDisclosure as acknowledge-each-risk
               checkboxes (the only surface that ENFORCES consent). Built from advice.Risks
               (FactAdvice.GetForFinding), which the MCP findings output also reads. */
            var risksBody = RenderRiskDisclosureBody(advice?.Risks);
            if (risksBody is not null)
            {
                context.Details.Add(new AlertDetailItem
                {
                    Heading = "RCSI — risks of changing / not changing",
                    Body = risksBody
                });
            }
        }

        /* Clear-cached-plan (§5/§6, PR-B): the DESTRUCTIVE "Clear cached plan (advanced)"
           affordance is a SEPARATE detail item from the CPU finding's always-safe advice
           — its own view with its own singular Remediation (FactKey "CLEAR_PLAN"), so it
           can never cross the force-plan / DB-config / RCSI affordances (each keys on a
           distinct FactKey). Emitted on a CPU finding (CPU_SQL_PERCENT / CPU_SPIKE) that
           carries an abnormal_cpu_plans drill-down with >= 1 qualifying row (BuildClearPlanAction
           returns non-null); returns null otherwise → NO item. Mirrors the RCSI second-item
           pattern exactly. The risk-of-not-changing figures (the anomaly ratio / per-exec
           CPU / window CPU%) are captured HERE onto the action so the in-app dialog renders
           the REAL numbers at apply time; the in-app consent gate is what makes it live. */
        var clearPlanAction = FactRemediation.BuildClearPlanAction(finding);
        if (clearPlanAction is not null)
        {
            context.Details.Add(new AlertDetailItem
            {
                Heading = "Clear cached plan (advanced)",
                Body = FactRemediation.GenerateClearPlanPreview(finding),
                IsCodeBlock = true,
                Remediation = clearPlanAction
            });

            /* Cross-surface disclosure (§5): the two-sided CLEAR_PLAN risk renders as
               READ-ONLY prose on email (both bodies) / webhook (all flow from
               context.Details). You cannot consent through an email, so there is NO
               checkbox gate off-app; the in-app dialog renders the SAME RiskDisclosure as
               acknowledge-each-risk checkboxes. Built from advice.Risks (FactAdvice.GetForFinding),
               which the MCP findings output also reads. */
            var clearRisksBody = RenderRiskDisclosureBody(advice?.Risks);
            if (clearRisksBody is not null)
            {
                context.Details.Add(new AlertDetailItem
                {
                    Heading = "Clear cached plan — risks of changing / not changing",
                    Body = clearRisksBody
                });
            }
        }

        /* Drill-down values are anonymous types behind object (a bare object, or a
           List<object> of them). Round-trip through System.Text.Json and walk as
           JsonElement — robust to any shape DrillDownCollector emits. */
        if (finding.DrillDown is { Count: > 0 })
        {
            foreach (var (key, value) in finding.DrillDown)
            {
                if (value is null)
                    continue;

                var item = new AlertDetailItem { Heading = Humanize(key) };
                try
                {
                    FlattenInto(item.Fields, JsonSerializer.SerializeToElement(value));
                }
                catch
                {
                    /* Unexpected value shape — skip this drill-down entry, keep the rest. */
                    continue;
                }

                if (item.Fields.Count > 0)
                    context.Details.Add(item);
            }
        }

        /* #1140: derive dedup incidents from the drill-down so this (secondary, anomaly) alert path
           carries the SAME fingerprints as the live "Detected" path. Deadlock -> involved-object set,
           blocking -> contentious object / query pair, query/CPU -> query_hash. Appended after the
           detail items, so the existing Diagnosis->Advice->drill-down order is preserved. */
        AlertIncidentRenderer.Apply(context, BuildIncidents(finding));

        return context;
    }

    /// <summary>
    /// #1140: derives the dedup incidents for the anomaly-finding alert path from the finding's
    /// drill-down, reusing the same shared groupers/fingerprint as the live builders so a deadlock,
    /// blocking chain, or long-running query produces an identical key on either path. Returns an
    /// empty list when the drill-down carries no fingerprintable identity.
    /// </summary>
    private static List<AlertIncident> BuildIncidents(AnalysisFinding finding)
    {
        var result = new List<AlertIncident>();
        if (finding.DrillDown is not { Count: > 0 })
            return result;

        var server = finding.ServerName ?? string.Empty;

        if (TryGetRows(finding.DrillDown, "top_deadlocks", out var deadlockRows))
        {
            var events = deadlockRows.Select(r =>
                new DeadlockIncidentGrouper.DeadlockEvent(SplitObjects(GetField(r, "objects"))));
            result.AddRange(DeadlockIncidentGrouper.Group(server, events).Select(g => g.Incident));
            return result;
        }

        if (TryGetRows(finding.DrillDown, "top_blocking_chains", out var blockingRows))
        {
            var events = blockingRows.Select(r => new BlockingIncidentGrouper.BlockedEvent(
                GetField(r, "database"), GetField(r, "contentious_object"),
                GetField(r, "blocked_sql"), GetField(r, "blocking_sql"), GetLongField(r, "wait_time_ms")));
            result.AddRange(BlockingIncidentGrouper.Group(server, events).Select(g => g.Incident));
            return result;
        }

        /* Query / CPU findings: one incident per distinct query_hash across any drill-down section. */
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (_, value) in finding.DrillDown)
        {
            if (value is null)
                continue;
            foreach (var row in AsRows(value))
            {
                var queryHash = GetField(row, "query_hash");
                if (string.IsNullOrEmpty(queryHash) || !seen.Add(queryHash))
                    continue;
                var db = GetField(row, "database");
                var incident = AlertFingerprint.ForKey(server, AlertFingerprint.Query, queryHash,
                    string.IsNullOrEmpty(db) ? System.Array.Empty<string>() : new[] { db });
                if (incident is not null)
                    result.Add(incident);
            }
        }
        return result;
    }

    private static bool TryGetRows(Dictionary<string, object> drillDown, string key, out List<JsonElement> rows)
    {
        rows = (drillDown.TryGetValue(key, out var value) && value is not null)
            ? AsRows(value)
            : new List<JsonElement>();
        return rows.Count > 0;
    }

    /// <summary>Round-trips a drill-down value (anonymous object or List&lt;object&gt;) through JSON and
    /// returns its object rows — the same robust shape-walk <see cref="FlattenInto"/> relies on.</summary>
    private static List<JsonElement> AsRows(object value)
    {
        var rows = new List<JsonElement>();
        try
        {
            var element = JsonSerializer.SerializeToElement(value);
            if (element.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in element.EnumerateArray())
                    if (item.ValueKind == JsonValueKind.Object)
                        rows.Add(item);
            }
            else if (element.ValueKind == JsonValueKind.Object)
            {
                rows.Add(element);
            }
        }
        catch { /* unexpected shape -> no rows */ }
        return rows;
    }

    private static string GetField(JsonElement row, string name) =>
        row.ValueKind == JsonValueKind.Object && row.TryGetProperty(name, out var p) && p.ValueKind == JsonValueKind.String
            ? p.GetString() ?? string.Empty
            : string.Empty;

    private static long GetLongField(JsonElement row, string name) =>
        row.ValueKind == JsonValueKind.Object && row.TryGetProperty(name, out var p)
            && p.ValueKind == JsonValueKind.Number && p.TryGetInt64(out var v)
            ? v : 0L;

    private static string[] SplitObjects(string joined) =>
        string.IsNullOrWhiteSpace(joined)
            ? System.Array.Empty<string>()
            : joined.Split(", ", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    /// <summary>
    /// Renders the two-sided <see cref="RiskDisclosure"/> as read-only prose for the
    /// cross-surface (email / webhook) disclosure item (B3 Phase 3, §6). Sections are
    /// separated by blank lines so the email/webhook prose renderer emits one paragraph
    /// per chunk. Returns null when there is nothing to disclose.
    /// </summary>
    private static string? RenderRiskDisclosureBody(RiskDisclosure? risks)
    {
        if (risks is null ||
            (risks.RisksOfChanging.Count == 0 && risks.RisksOfNotChanging.Count == 0))
            return null;

        var sb = new StringBuilder();
        sb.Append("Risks of CHANGING:");
        foreach (var r in risks.RisksOfChanging)
            sb.Append("\n\n• ").Append(r.Text);
        sb.Append("\n\nRisks of NOT changing:");
        foreach (var r in risks.RisksOfNotChanging)
            sb.Append("\n\n• ").Append(r.Text);
        return sb.ToString();
    }

    /// <summary>
    /// Flattens one drill-down value into label/value field pairs. Arrays are capped at
    /// the first 3 elements; nested objects/arrays are rendered as compact JSON.
    /// </summary>
    private static void FlattenInto(List<(string Label, string Value)> fields, JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Array:
                var index = 0;
                foreach (var child in element.EnumerateArray())
                {
                    if (index >= 3)
                        break;
                    index++;

                    if (child.ValueKind == JsonValueKind.Object)
                    {
                        foreach (var prop in child.EnumerateObject())
                            fields.Add(($"#{index} {Humanize(prop.Name)}", ScalarText(prop.Value)));
                    }
                    else
                    {
                        fields.Add(($"#{index}", ScalarText(child)));
                    }
                }
                break;

            case JsonValueKind.Object:
                foreach (var prop in element.EnumerateObject())
                    fields.Add((Humanize(prop.Name), ScalarText(prop.Value)));
                break;

            default:
                fields.Add(("value", ScalarText(element)));
                break;
        }
    }

    /// <summary>Renders a single JSON value as truncated display text.</summary>
    private static string ScalarText(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => Truncate(element.GetString() ?? string.Empty),
            JsonValueKind.Number => element.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            JsonValueKind.Null => string.Empty,
            // Nested object/array — show compact raw JSON rather than recursing further.
            _ => Truncate(element.GetRawText())
        };
    }

    /// <summary>Turns a snake_case key into spaced Title Case ("top_blocking_chains" -> "Top Blocking Chains").</summary>
    private static string Humanize(string key)
    {
        if (string.IsNullOrEmpty(key))
            return key;

        var words = key.Replace('_', ' ').Split(' ', StringSplitOptions.RemoveEmptyEntries);
        return string.Join(' ', words.Select(w => char.ToUpperInvariant(w[0]) + w[1..]));
    }

    private static string Truncate(string text)
    {
        return text.Length <= FieldValueLimit ? text : text[..FieldValueLimit] + "…";
    }
}
