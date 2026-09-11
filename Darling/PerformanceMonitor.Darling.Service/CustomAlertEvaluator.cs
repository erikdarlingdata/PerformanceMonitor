/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Evaluates user-authored custom alert rules (#3285) on the service sweep — one generic loop, not N
/// hardcoded checks. Called per server (like the self-alert pass) from ABOVE the connect gate, so a rule can
/// still evaluate the collected store for a server that is currently disconnected.
///
/// <para>For each enabled rule applicable to the server: compile the rule's Scalar compose metric scoped to
/// that server, run it on the VIEWER-role pool (which carries the <c>statement_timeout</c> cap and the
/// least-privilege ACL — never the worker's owner/superuser pool), apply the predicate + the shared
/// <see cref="AlertPersistenceGate"/>, and deliver a fire / resolve through the SAME
/// <see cref="IAlertDeliverer"/> the built-in alerts use, keyed on <c>metric_name = "Custom:&lt;rule_id&gt;"</c>
/// (rename-safe, collision-free with built-in metric names). Anti-flap is the persistence gate, not a cooldown:
/// firing is edge-triggered, so a still-true condition does not re-nag and needs no cooldown suppressant, and
/// the persisted state makes a restart not re-fire an already-open incident.</para>
///
/// <para>No-data (an empty window, or an always-NULL measure such as ACU headroom on a provisioned instance)
/// FREEZES the streak — it is never a breach and never a clear — so a data gap neither fires nor resolves.
/// A rule that no longer parses (a measure drifted out of the catalog) is logged and skipped rather than
/// silently treated as healthy.</para>
/// </summary>
public sealed class CustomAlertEvaluator
{
    private readonly CustomAlertRuleStore _ruleStore;
    private readonly CustomAlertStateStore _stateStore;
    private readonly NpgsqlDataSource _viewer;
    private readonly IAlertDeliverer _deliverer;
    private readonly Func<AlertMuteContext, bool>? _isAlertMuted;
    private readonly PgAlertHistoryStore _historyStore;
    private readonly int _defaultIntervalSeconds;
    private readonly ILogger _logger;

    private readonly TimeSpan _cacheTtl;
    private volatile IReadOnlyList<ParsedRule> _cache = Array.Empty<ParsedRule>();
    private DateTime _cacheRefreshedUtc = DateTime.MinValue;

    private sealed record ParsedRule(CustomAlertRule Row, CustomAlertRuleDefinition Definition);

    public CustomAlertEvaluator(
        CustomAlertRuleStore ruleStore,
        CustomAlertStateStore stateStore,
        NpgsqlDataSource viewerDataSource,
        IAlertDeliverer deliverer,
        Func<AlertMuteContext, bool>? isAlertMuted,
        PgAlertHistoryStore historyStore,
        int defaultIntervalSeconds,
        TimeSpan cacheTtl,
        ILogger logger)
    {
        _ruleStore = ruleStore ?? throw new ArgumentNullException(nameof(ruleStore));
        _stateStore = stateStore ?? throw new ArgumentNullException(nameof(stateStore));
        _viewer = viewerDataSource ?? throw new ArgumentNullException(nameof(viewerDataSource));
        _deliverer = deliverer ?? throw new ArgumentNullException(nameof(deliverer));
        _isAlertMuted = isAlertMuted;
        _historyStore = historyStore ?? throw new ArgumentNullException(nameof(historyStore));
        _defaultIntervalSeconds = Math.Max(CustomAlertRuleDefinition.MinEvaluationIntervalSeconds, defaultIntervalSeconds);
        _cacheTtl = cacheTtl;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>The <c>config_alert_log</c> / mute / cooldown key for a rule — the immutable id, never the
    /// name, so a rename does not orphan history or break mute rules.</summary>
    public static string MetricNameFor(long ruleId) =>
        "Custom:" + ruleId.ToString(CultureInfo.InvariantCulture);

    /// <summary>An abuse bound on the description portion of the rendered <c>detail_text</c> (the column is
    /// unbounded text; the name has its own <see cref="CustomAlertRuleStore.MaxNameLength"/> bound).</summary>
    private const int MaxDetailDescriptionLength = 500;

    /// <summary>
    /// Makes a user-authored rule name/description safe to render in a notification title and to write into
    /// the <c>detail_text</c> that the viewer's mute-from-history pre-fill
    /// (<see cref="AlertMuteContext.PopulateFromDetailText"/>) later parses. That parser splits
    /// <c>detail_text</c> on <c>'\n'</c> and reads any line beginning with a <c>"Database: "</c> /
    /// <c>"Query: "</c> / <c>"Wait Type: "</c> / <c>"Job Name: "</c> label into a mute pattern, so a crafted
    /// name carrying <c>"\nDatabase: master"</c> could forge a mute-context field and spoof the pre-fill.
    /// Collapsing every line break and control character (CR/LF/TAB/NEL and the Unicode line/paragraph
    /// separators) to a single space guarantees the value stays one line that cannot begin a forged label
    /// line, and the length cap bounds the other half of the problem. Runs of whitespace collapse and the
    /// result is trimmed; returns "" for null/blank input so a render site falls back to the metric name.
    /// </summary>
    public static string SanitizeDisplayText(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value) || maxLength <= 0)
        {
            return string.Empty;
        }

        var builder = new StringBuilder(Math.Min(value.Length, maxLength));
        var pendingSpace = false;
        foreach (var ch in value)
        {
            var isBreakOrControl =
                ch < ' ' || ch == (char)0x7F || ch == (char)0x85 || ch == '\u2028' || ch == '\u2029';

            if (isBreakOrControl || ch == ' ')
            {
                // Defer whitespace: emitted only before the next real char (drops leading + trailing runs).
                pendingSpace = builder.Length > 0;
                continue;
            }

            if (pendingSpace)
            {
                if (builder.Length >= maxLength)
                {
                    break;
                }

                builder.Append(' ');
                pendingSpace = false;
            }

            if (builder.Length >= maxLength)
            {
                break;
            }

            builder.Append(ch);
        }

        return builder.ToString();
    }

    /// <summary>Evaluates every enabled rule applicable to one server. Failure-isolated per rule.</summary>
    public async Task EvaluateServerAsync(int serverId, string storageName, string displayName, CancellationToken cancellationToken)
    {
        IReadOnlyList<ParsedRule> rules;
        try
        {
            rules = await GetEnabledRulesAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Custom alert rule load failed: {Message}", ex.Message);
            return;
        }

        var applicable = rules.Where(r => r.Definition.AppliesTo(storageName)).ToList();
        if (applicable.Count == 0)
        {
            return;
        }

        RollupAvailability rollups;
        RollupCoverage coverage;
        int composedSeconds;
        try
        {
            (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(_viewer, cancellationToken);
            composedSeconds = await McpCommandDeadlines.ResolveComposedQuerySecondsAsync(_viewer, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("[{Server}] custom-alert store-availability probe failed: {Message}", displayName, ex.Message);
            return;
        }

        var now = DateTime.UtcNow;
        foreach (var (row, def) in applicable)
        {
            try
            {
                await EvaluateRuleForServerAsync(row, def, serverId, storageName, displayName, now, rollups, coverage, composedSeconds, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("[{Server}] custom alert rule {Id} '{Name}' evaluation failed: {Message}",
                    displayName, row.Id, row.Name, ex.Message);
            }
        }
    }

    private async Task EvaluateRuleForServerAsync(
        CustomAlertRule row, CustomAlertRuleDefinition def, int serverId, string storageName, string displayName,
        DateTime now, RollupAvailability rollups, RollupCoverage coverage, int composedSeconds, CancellationToken cancellationToken)
    {
        var state = await _stateStore.LoadAsync(row.Id, serverId, row.Version, cancellationToken);

        // Per-rule cadence over a single sweep: skip until this subject is due.
        if (state.NextDueAt is DateTime due && now < due)
        {
            return;
        }

        var intervalSeconds = def.EvaluationIntervalSeconds ?? _defaultIntervalSeconds;
        var nextDue = now.AddSeconds(intervalSeconds);

        var value = await RunScalarAsync(def, storageName, now, rollups, coverage, composedSeconds, cancellationToken);

        if (value is null)
        {
            // No-data: freeze the streak, just reschedule. Never a breach, never a clear.
            await _stateStore.SaveAsync(row.Id, serverId, state with { LastEvaluatedAt = now, NextDueAt = nextDue }, cancellationToken);
            return;
        }

        var breaching = def.IsBreaching(value.Value);
        var evaluation = AlertPersistenceGate.Evaluate(state.Persistence, breaching, def.BreachSamples, def.ClearSamples);
        var newState = state with { Persistence = evaluation.State, LastEvaluatedAt = now, NextDueAt = nextDue };

        switch (evaluation.Outcome)
        {
            case PersistenceOutcome.Fire:
                var severity = def.SeverityFor(value.Value);
                await DeliverFireAsync(row, def, serverId, displayName, value.Value, severity, cancellationToken);
                newState = newState with { FiredSeverity = severity.ToString() };
                break;

            case PersistenceOutcome.Resolve:
                // Only deliver a resolve for an incident that was actually delivered.
                if (state.FiredSeverity is not null)
                {
                    await DeliverResolveAsync(row, serverId, displayName, value.Value);
                }

                newState = newState with { FiredSeverity = null };
                break;

            case PersistenceOutcome.None:
            default:
                break;
        }

        await _stateStore.SaveAsync(row.Id, serverId, newState, cancellationToken);
    }

    private async Task<double?> RunScalarAsync(
        CustomAlertRuleDefinition def, string storageName, DateTime now, RollupAvailability rollups, RollupCoverage coverage,
        int composedSeconds, CancellationToken cancellationToken)
    {
        var end = now;
        var start = now.AddHours(-def.WindowHours);
        var context = new ComposeRunContext(
            new[] { storageName }, start, end, ComposeRunContext.NoVariables, rollups, now, coverage);

        var (compiled, compileError) = ComposeCompiler.Compile(def.Plan, context);
        if (compileError is not null || compiled is null)
        {
            _logger.LogWarning("Custom alert metric failed to compile: {Error}", compileError);
            return null;
        }

        await using var command = _viewer.CreateCommand(compiled.Sql);
        command.CommandTimeout = composedSeconds;
        foreach (var parameter in compiled.Parameters)
        {
            command.Parameters.Add(parameter);
        }

        var scalar = await command.ExecuteScalarAsync(cancellationToken);
        return scalar is null or DBNull ? null : Convert.ToDouble(scalar, CultureInfo.InvariantCulture);
    }

    private async Task DeliverFireAsync(
        CustomAlertRule row, CustomAlertRuleDefinition def, int serverId, string displayName, double value,
        AlertSeverityLevel severity, CancellationToken cancellationToken)
    {
        var metricName = MetricNameFor(row.Id);
        var serverKey = serverId.ToString(CultureInfo.InvariantCulture);
        var muted = _isAlertMuted?.Invoke(new AlertMuteContext { ServerName = displayName, MetricName = metricName }) ?? false;

        await _deliverer.DeliverAsync(
            BuildFireOutcome(row, def, serverKey, displayName, value, severity, muted),
            cancellationToken);
    }

    /// <summary>
    /// Builds the <see cref="AlertOutcome"/> for one custom-rule fire — pure (no I/O), so the display-name
    /// wiring, the detail_text sanitization/separator, and the metric key are unit-testable without a store.
    /// The user-authored name/description are newline-stripped + length-capped (<see cref="SanitizeDisplayText"/>);
    /// the sanitized name rides <see cref="AlertOutcome.DisplayName"/> (rendered in titles/subjects) while
    /// <c>metric_name = "Custom:&lt;id&gt;"</c> stays the immutable history / mute / cooldown key.
    /// </summary>
    /// <param name="serverDisplayName">The monitored server's display name (<see cref="AlertOutcome.ServerName"/>).</param>
    public static AlertOutcome BuildFireOutcome(
        CustomAlertRule row, CustomAlertRuleDefinition def, string serverKey, string serverDisplayName,
        double value, AlertSeverityLevel severity, bool muted)
    {
        var metricName = MetricNameFor(row.Id);
        var currentText = value.ToString("0.###", CultureInfo.InvariantCulture);
        var thresholdValue = severity == AlertSeverityLevel.Critical && def.CriticalThreshold is double critical
            ? critical
            : def.WarnThreshold;
        var thresholdText = string.Create(CultureInfo.InvariantCulture, $"{def.OpSymbol} {thresholdValue:0.###}");

        /* Sanitize the user-authored name/description before they enter any rendered string or the persisted
           detail_text: strip every line break + control char and cap the length so a crafted name cannot
           forge a "Database:"/"Query:" label line that the mute-from-history pre-fill would parse
           (AlertMuteContext.PopulateFromDetailText). safeName also rides AlertOutcome.DisplayName so the
           delivery layer renders the human name instead of the "Custom:<id>" metric key. */
        var safeName = SanitizeDisplayText(row.Name, CustomAlertRuleStore.MaxNameLength);
        var safeDescription = SanitizeDisplayText(row.Description, MaxDetailDescriptionLength);
        var shortMessage = string.Create(CultureInfo.InvariantCulture,
            $"{safeName}: {def.MeasureDisplayName} {currentText} (threshold {thresholdText})");
        /* Plain " - " separator (never an em dash — a style tell to keep out of delivered text). */
        var detail = safeDescription.Length == 0 ? safeName : $"{safeName} - {safeDescription}";

        return new AlertOutcome(
            serverKey,
            serverDisplayName,
            metricName,
            currentText,
            thresholdText,
            Context: null,
            DetailText: detail,
            NumericCurrentValue: value,
            NumericThresholdValue: thresholdValue,
            Muted: muted,
            Severity: severity,
            ShortMessage: shortMessage,
            DisplayName: safeName);
    }

    private async Task DeliverResolveAsync(CustomAlertRule row, int serverId, string displayName, double value)
    {
        var metricName = MetricNameFor(row.Id);
        var serverKey = serverId.ToString(CultureInfo.InvariantCulture);
        /* Same sanitization as the fire path: the resolution's Title becomes the history row's metric_name
           and its Message becomes detail_text, so a crafted rule name must be newline-stripped + capped here
           too or it re-opens the mute-pre-fill spoof on the recovery row. */
        var safeName = SanitizeDisplayText(row.Name, CustomAlertRuleStore.MaxNameLength);
        var title = safeName + " Resolved";
        var message = string.Create(CultureInfo.InvariantCulture,
            $"{displayName}: {safeName} back within threshold (now {value:0.###})");

        _logger.LogInformation("{Line}", AlertFiringLog.Resolved(displayName, title, message));

        try
        {
            await _historyStore.RecordAlertAsync(DarlingSelfAlertEvaluator.BuildResolutionRecord(
                new AlertResolution(serverKey, displayName, metricName, title, message)));
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Could not record custom alert resolution for {Server}/{Metric}: {Message}",
                displayName, metricName, ex.Message);
        }
    }

    private async Task<IReadOnlyList<ParsedRule>> GetEnabledRulesAsync(CancellationToken cancellationToken)
    {
        // Lock-free by design: the refresh is an idempotent read of a tiny table, so a rare duplicate
        // refresh under concurrent server sweeps is harmless (last writer wins; both compute the same list).
        if (DateTime.UtcNow - _cacheRefreshedUtc < _cacheTtl)
        {
            return _cache;
        }

        var rows = await _ruleStore.ListEnabledAsync(cancellationToken);
        var parsed = new List<ParsedRule>(rows.Count);
        foreach (var rowItem in rows)
        {
            var (definition, error) = CustomAlertRuleDefinition.TryParse(rowItem.DefinitionJson);
            if (error is not null || definition is null)
            {
                // A rule nobody ever "opens" must not fail silently: log it. (The self-health surface
                // that raises this as an alert is a follow-up slice.)
                _logger.LogWarning("Custom alert rule {Id} '{Name}' will NOT fire — its definition no longer validates: {Error}",
                    rowItem.Id, rowItem.Name, error);
                continue;
            }

            parsed.Add(new ParsedRule(rowItem, definition));
        }

        _cache = parsed;
        _cacheRefreshedUtc = DateTime.UtcNow;
        return _cache;
    }
}
