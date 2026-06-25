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
    /// Per-finding re-notification cooldown, keyed "{serverId}:{StoryPathHash}".
    /// Seeded lazily from the alert log on first lookup per key so a finding
    /// that just fired and entered its cooldown stays suppressed across an
    /// app restart. Pruned on each notify cycle to entries within
    /// 2 × AnalysisNotifyCooldownMinutes.
    /// </summary>
    private readonly ConcurrentDictionary<string, DateTime> _cooldowns = new();

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
    /// Notifies on every finding at or above the configured severity that is not
    /// inside its re-notification cooldown. Never throws.
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

        /* Drop entries past 2× cooldown so the dict stays bounded — any entry
           past 1× is already re-fire-eligible, doubling gives clock-skew margin.
           If a key here also matches a finding in this batch, the per-finding
           seed below will re-add it from history; that's a wash, not a bug. */
        var pruneBefore = now - TimeSpan.FromTicks(cooldown.Ticks * 2);
        foreach (var stale in _cooldowns)
        {
            if (stale.Value < pruneBefore)
                _cooldowns.TryRemove(stale.Key, out _);
        }

        foreach (var finding in findings)
        {
            if (finding.Severity < threshold)
                continue;

            /* Cooldown key uses the finding's stable int id (matches both apps' prior
               behaviour); the resolved serverId below is only the persistence shape. */
            var key = $"{finding.ServerId}:{finding.StoryPathHash}";
            var serverId = _resolveServerId(finding);

            /* Honor per-server silencing (Dashboard "Silence All Alerts"). Checked after
               resolving serverId but before the cooldown seed/stamp, so a silenced server
               neither notifies nor consumes its cooldown — unsilencing resumes immediately. */
            if (_isServerSilenced is not null && _isServerSilenced(serverId))
                continue;

            var metricName = FindingMessageFormatter.MetricName(finding);

            /* Seed the in-memory cooldown from the alert log on first lookup per key so an
               analysis finding that fired shortly before an app restart is not re-fired
               afterward. No channel/error filter — the cooldown is stamped unconditionally
               below, so the persisted equivalent is the latest row for that metric_name. */
            if (!_cooldowns.ContainsKey(key))
            {
                var lastPersisted = await _sender.GetLastAlertTimeAsync(serverId, metricName);
                if (lastPersisted.HasValue)
                {
                    _cooldowns.TryAdd(key, lastPersisted.Value);
                }
            }

            if (_cooldowns.TryGetValue(key, out var last) && now - last < cooldown)
                continue;

            try
            {
                /* SendFindingAlertAsync fans out to email + Slack + Teams and records the
                   alert per this app's cadence. It returns no success/failure signal, so the
                   cooldown is stamped regardless — a finding whose delivery failed is
                   suppressed for the full cooldown (accepted best-effort behavior). */
                await _sender.SendFindingAlertAsync(new FindingAlert(
                    metricName,
                    finding.ServerName,
                    FindingMessageFormatter.CurrentValue(finding),
                    threshold.ToString("F1"),
                    serverId,
                    FindingMessageFormatter.BuildContext(finding, threshold),
                    finding.Severity,
                    threshold,
                    FindingMessageFormatter.DetailText(finding, threshold)));

                /* Always raise the tray balloon for a notify-worthy finding (user choice), the
                   same visible signal threshold alerts already pop — so a local-only user with no
                   email/webhook still sees it. No-op when the host wired no sink (Lite) or tray
                   notifications are disabled (the sink checks the pref). */
                if (_showTrayNotification is not null)
                {
                    var (title, message) = FindingMessageFormatter.BalloonText(finding);
                    _showTrayNotification(title, message);
                }

                _cooldowns[key] = now;
            }
            catch (Exception ex)
            {
                /* SendFindingAlertAsync is documented never to throw; this guards a
                   formatter defect so one bad finding cannot abort the rest. */
                _logger.LogError(
                    $"AnalysisNotificationService: failed to notify on finding {finding.StoryPathHash}: {ex.GetType().Name}: {ex.Message}");
            }
        }
    }
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
