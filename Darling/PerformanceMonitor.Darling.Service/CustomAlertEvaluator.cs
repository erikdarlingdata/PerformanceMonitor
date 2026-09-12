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

    /// <summary>Broken rules found at the last cache refresh — re-parsed against the live catalog and no longer
    /// compiling. Surfaced (aggregated) as a service self-health alert (#3304). Refreshed alongside
    /// <see cref="_cache"/> in <see cref="GetEnabledRulesAsync"/>.</summary>
    private volatile IReadOnlyList<CustomAlertRuleHealthIssue> _brokenRules = Array.Empty<CustomAlertRuleHealthIssue>();

    /// <summary>Per-rule UTC instant the current unbroken no-data streak began (#3304). An in-memory heuristic —
    /// set the first time a rule returns no data, cleared the moment ANY in-scope server returns a value, so a
    /// rule with data anywhere never accrues. A rule whose entry is older than <see cref="NoDataFlagWindow"/> is
    /// flagged "armed but never fires" (an always-NULL measure, or a dead collector). Deliberately NOT persisted:
    /// a restart simply restarts the window, which is the right bias for an integrity heuristic (no false alarm
    /// across a restart) and keeps the slice migration-free.</summary>
    private readonly ConcurrentDictionary<long, DateTime> _noDataSinceUtc = new();

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

    /// <summary>Length cap on a broken-rule catalog error / never-firing reason before it enters the aggregated
    /// self-health alert's detail_text — the same newline-strip + cap discipline as the rule name, since the
    /// error can echo a user-supplied measure key.</summary>
    private const int MaxHealthReasonLength = 300;

    /// <summary>
    /// How long a rule must produce NO data (across every in-scope server, continuously) before it is flagged
    /// "armed but never fires" (#3304). Chosen as a DURATION rather than a raw sweep count so it is independent
    /// of fleet size and per-rule cadence: a count would trip a large all-servers rule in a single sweep (many
    /// no-data evaluations at once) and would flag a just-created rule before its first window filled. At the
    /// ~60s custom-alert cadence this is ~20 minutes of continuous no-data — long enough to ride out a transient
    /// collector gap or a rule still awaiting its first sample, short enough to surface a permanently-NULL
    /// measure (ACU headroom on a provisioned instance) or a dead collector within the hour.
    /// </summary>
    internal static readonly TimeSpan NoDataFlagWindow = TimeSpan.FromMinutes(20);

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
            // #3304: remember when this rule's unbroken no-data run began (earliest instant kept) so the
            // fleet-global health check can flag a rule that is armed but has produced no data for too long.
            _noDataSinceUtc.GetOrAdd(row.Id, now);
            await _stateStore.SaveAsync(row.Id, serverId, state with { LastEvaluatedAt = now, NextDueAt = nextDue }, cancellationToken);
            return;
        }

        // #3304: any real value clears the no-data run; a rule with data on even one in-scope server is
        // firing-eligible and must never be flagged as never-firing.
        _noDataSinceUtc.TryRemove(row.Id, out _);

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

    private Task<double?> RunScalarAsync(
        CustomAlertRuleDefinition def, string storageName, DateTime now, RollupAvailability rollups, RollupCoverage coverage,
        int composedSeconds, CancellationToken cancellationToken) =>
        EvaluateScalarNowAsync(_viewer, def, storageName, now, rollups, coverage, composedSeconds, _logger, cancellationToken);

    /// <summary>
    /// Compiles the rule's Scalar metric scoped to ONE server and reads its current value on the given
    /// least-privilege pool — the SINGLE shared value computation the sweep and the <c>test_custom_alert_rule</c>
    /// evaluate-now tool (#3299) both call, so a "would fire" preview can never diverge from what the sweep
    /// actually computes. Returns null for no-data (an empty window, or an always-NULL measure) AND for a metric
    /// that no longer compiles (logged); the caller treats null as "no value", never a breach.
    /// <para><b>Pool discipline (Component 4 R2-SEC):</b> <paramref name="pool"/> MUST be a least-privilege role
    /// that carries the <c>statement_timeout</c> cap and the secret-column ACL — the worker's dedicated viewer
    /// pool on the sweep, or the MCP host's mcp-role pool from the tool — NEVER the owner/superuser pool.</para>
    /// </summary>
    public static async Task<double?> EvaluateScalarNowAsync(
        NpgsqlDataSource pool, CustomAlertRuleDefinition definition, string storageName, DateTime nowUtc,
        RollupAvailability rollups, RollupCoverage coverage, int composedQuerySeconds,
        ILogger? logger, CancellationToken cancellationToken)
    {
        var start = nowUtc.AddHours(-definition.WindowHours);
        var context = new ComposeRunContext(
            new[] { storageName }, start, nowUtc, ComposeRunContext.NoVariables, rollups, nowUtc, coverage);

        var (compiled, compileError) = ComposeCompiler.Compile(definition.Plan, context);
        if (compileError is not null || compiled is null)
        {
            logger?.LogWarning("Custom alert metric failed to compile: {Error}", compileError);
            return null;
        }

        await using var command = pool.CreateCommand(compiled.Sql);
        command.CommandTimeout = composedQuerySeconds;
        foreach (var parameter in compiled.Parameters)
        {
            command.Parameters.Add(parameter);
        }

        var scalar = await command.ExecuteScalarAsync(cancellationToken);
        return scalar is null or DBNull ? null : Convert.ToDouble(scalar, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Maps a one-shot scalar value to a would-fire verdict for the evaluate-now tool (#3299): no-data (null)
    /// and a non-breaching value both return <c>(false, null)</c>; a breaching value returns
    /// <c>(true, def.SeverityFor(value))</c> — the SAME predicate + severity tier the sweep applies. This is the
    /// INSTANTANEOUS half only: a real fire also requires the condition to hold across the rule's hysteresis
    /// (breach/clear samples) and per-server streak, which a one-shot cannot reproduce — the tool states so.
    /// </summary>
    public static (bool Breaching, AlertSeverityLevel? Severity) ClassifyTestValue(CustomAlertRuleDefinition definition, double? value)
    {
        if (value is not double v || !definition.IsBreaching(v))
        {
            return (false, null);
        }

        return (true, definition.SeverityFor(v));
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

    /// <summary>The reason a firing (rule, server) subject is force-resolved when its rule is disabled (#3305).</summary>
    internal const string TeardownReasonDisabled = "the rule was disabled";

    /// <summary>The reason a firing (rule, server) subject is force-resolved when the server has left the rule's
    /// scope, or has been removed from monitoring entirely (#3305).</summary>
    internal const string TeardownReasonOutOfScope = "the server is no longer in the rule's scope";

    /// <summary>The reason a firing (rule, server) subject is force-resolved when the rule is deleted (#3305).</summary>
    internal const string TeardownReasonDeleted = "the rule was deleted";

    /// <summary>
    /// Writes ONE recovery/resolution row for a teardown (#3305) — the SAME resolve idiom
    /// <see cref="DeliverResolveAsync"/> uses (<c>BuildResolutionRecord</c> + <c>AlertFiringLog.Resolved</c>),
    /// but keyed off an already-known (rule, server) rather than a live evaluation, and stating WHY (deleted /
    /// disabled / out of scope) rather than a threshold recovery. Static so both the delete path and the sweep
    /// reconcile can call it. The rule name is newline-stripped + length-capped before it reaches
    /// <c>detail_text</c>, so a crafted name cannot re-open the mute-from-history spoof on the recovery row.
    /// Failure-isolated: an audit-row write must never break a delete or a sweep.
    /// </summary>
    public static async Task WriteTeardownResolutionAsync(
        IAlertHistoryStore historyStore, ILogger? logger,
        long ruleId, string ruleName, int serverId, string serverName, string reason)
    {
        var safeName = SanitizeDisplayText(ruleName, CustomAlertRuleStore.MaxNameLength);
        var metricName = MetricNameFor(ruleId);
        var title = safeName + " Resolved";
        var message = string.Create(CultureInfo.InvariantCulture, $"{serverName}: {safeName} resolved because {reason}");

        logger?.LogInformation("{Line}", AlertFiringLog.Resolved(serverName, title, message));

        try
        {
            await historyStore.RecordAlertAsync(DarlingSelfAlertEvaluator.BuildResolutionRecord(
                new AlertResolution(serverId.ToString(CultureInfo.InvariantCulture), serverName, metricName, title, message)));
        }
        catch (Exception ex)
        {
            logger?.LogWarning("Could not record custom alert teardown resolution for rule {Rule} on {Server}: {Message}",
                ruleId, serverName, ex.Message);
        }
    }

    /// <summary>
    /// Force-resolves any OPEN incident of a rule and then deletes the rule (#3305). Reads the rule (for its
    /// name) and its firing subjects and writes a recovery row for each BEFORE calling
    /// <see cref="CustomAlertRuleStore.DeleteAsync"/> — because that delete's FK cascade drops the
    /// <c>custom_alert_state</c> rows that say which (rule, server) pairs were firing, so the resolve must read
    /// them first. Static (the MCP delete tool holds only the owner pool, not the singleton evaluator); the
    /// evaluator's in-memory no-data map is pruned lazily on its next cache refresh. The resolve is best-effort
    /// (its own failure isolation) so a resolve blip never blocks the delete the operator asked for.
    /// </summary>
    public static async Task<CustomAlertRuleResult> ResolveAndDeleteRuleAsync(
        NpgsqlDataSource postgres, long ruleId, ILogger? logger, CancellationToken cancellationToken)
    {
        var ruleStore = new CustomAlertRuleStore(postgres);

        var current = await ruleStore.GetAsync(ruleId, cancellationToken);
        if (current is CustomAlertRuleResult.Ok ok && ok.Rule is not null)
        {
            var stateStore = new CustomAlertStateStore(postgres);
            var firing = await stateStore.ListFiringWithNamesAsync(ruleId, cancellationToken);
            if (firing.Count > 0)
            {
                var historyStore = new PgAlertHistoryStore(postgres, logger);
                foreach (var (serverId, serverName) in firing)
                {
                    await WriteTeardownResolutionAsync(historyStore, logger, ruleId, ok.Rule.Name, serverId, serverName, TeardownReasonDeleted);
                }
            }
        }

        return await ruleStore.DeleteAsync(ruleId, cancellationToken);
    }

    private async Task<IReadOnlyList<ParsedRule>> GetEnabledRulesAsync(CancellationToken cancellationToken)
    {
        // Lock-free by design: the refresh is an idempotent read of a tiny table, so a rare duplicate
        // refresh under concurrent server sweeps is harmless (last writer wins; both compute the same list).
        if (DateTime.UtcNow - _cacheRefreshedUtc < _cacheTtl)
        {
            return _cache;
        }

        var rows = ApplyEnabledCeiling(await _ruleStore.ListEnabledAsync(cancellationToken), _logger);
        var (parsedPairs, broken) = ClassifyRules(rows);

        foreach (var b in broken)
        {
            // #3304: a rule nobody ever "opens" must not fail silently. It is both logged here AND surfaced as
            // an aggregated service self-health alert (BuildHealthReportAsync -> DarlingSelfAlertEvaluator).
            _logger.LogWarning("Custom alert rule {Id} '{Name}' will NOT fire: {Reason}", b.RuleId, b.RuleName, b.Reason);
        }

        var parsed = parsedPairs.Select(p => new ParsedRule(p.Row, p.Definition)).ToList();

        // Drop no-data bookkeeping for rules that are gone (disabled/deleted) or now broken, so the in-memory
        // map tracks only the currently-parsed, evaluable set and cannot grow without bound.
        var liveIds = new HashSet<long>(parsed.Select(p => p.Row.Id));
        foreach (var id in _noDataSinceUtc.Keys)
        {
            if (!liveIds.Contains(id))
            {
                _noDataSinceUtc.TryRemove(id, out _);
            }
        }

        _cache = parsed;
        _brokenRules = broken;
        _cacheRefreshedUtc = DateTime.UtcNow;
        return _cache;
    }

    /// <summary>
    /// The evaluator's HARD ceiling on how many enabled rules a sweep will ever evaluate (#3285, Round-2). The
    /// write path already caps enabled rules at <see cref="CustomAlertRuleStore.EnabledRuleCap"/>, so this is a
    /// defense-in-depth backstop: even if the table somehow holds more enabled rows than the cap (a direct SQL
    /// insert that bypassed the store, say), the sweep still evaluates at most the cap, in the deterministic
    /// id order <see cref="CustomAlertRuleStore.ListEnabledSql"/> reads them in. When the ceiling is actually hit
    /// it is logged (WARN) rather than silently trimmed, so an over-cap table is visible. The store reads one
    /// beyond the cap, so a hit here means "at least cap+1 enabled rows exist". Pure over its inputs so the trim
    /// is unit-testable without a store.
    /// </summary>
    internal static IReadOnlyList<CustomAlertRule> ApplyEnabledCeiling(IReadOnlyList<CustomAlertRule> enabledRows, ILogger? logger)
    {
        if (enabledRows.Count <= CustomAlertRuleStore.EnabledRuleCap)
        {
            return enabledRows;
        }

        logger?.LogWarning(
            "Custom alert rule enabled-count ceiling hit: at least {Count} enabled rules exist but only the first {Cap} (by id) will be evaluated. The write-path cap should prevent this — a rule was likely inserted bypassing the store; disable the excess.",
            enabledRows.Count, CustomAlertRuleStore.EnabledRuleCap);
        return enabledRows.Take(CustomAlertRuleStore.EnabledRuleCap).ToList();
    }

    /// <summary>
    /// Re-parses every enabled rule against the LIVE <see cref="MeasureCatalog"/> (via
    /// <see cref="CustomAlertRuleDefinition.TryParse"/>) and splits the rows into the ones that still compile
    /// and the ones that no longer do (#3304, Component 4 step 0 "compile-check on load"). A broken rule is not
    /// silently skipped: it is returned as a <see cref="CustomAlertRuleHealthIssue"/> carrying the sanitized
    /// name + the sanitized catalog error (NEVER any compiled SQL — the error is the parser/validator message).
    /// Pure and static so the drift-surfacing is unit-testable without a store.
    /// </summary>
    internal static (
        List<(CustomAlertRule Row, CustomAlertRuleDefinition Definition)> Parsed,
        List<CustomAlertRuleHealthIssue> Broken) ClassifyRules(IReadOnlyList<CustomAlertRule> rows)
    {
        var parsed = new List<(CustomAlertRule, CustomAlertRuleDefinition)>(rows.Count);
        var broken = new List<CustomAlertRuleHealthIssue>();
        foreach (var row in rows)
        {
            var (definition, error) = CustomAlertRuleDefinition.TryParse(row.DefinitionJson);
            if (error is not null || definition is null)
            {
                broken.Add(new CustomAlertRuleHealthIssue(
                    row.Id,
                    SanitizeDisplayText(row.Name, CustomAlertRuleStore.MaxNameLength),
                    SanitizeDisplayText(error ?? "its definition no longer validates", MaxHealthReasonLength)));
                continue;
            }

            parsed.Add((row, definition));
        }

        return (parsed, broken);
    }

    /// <summary>
    /// Classifies whether a rule that DOES compile is nonetheless "armed but never fires" (#3304) — the two
    /// cases that pass the compile-check yet can never deliver: a rule scoped to specific servers that are not
    /// currently monitored (so it never evaluates), and a rule that has produced no data for longer than
    /// <see cref="NoDataFlagWindow"/> (an always-NULL measure, or a dead collector). Returns the issue, or null
    /// when the rule is firing-eligible. Pure so both cases are unit-testable with controlled inputs.
    /// </summary>
    internal static CustomAlertRuleHealthIssue? ClassifyNeverFiring(
        CustomAlertRule row, CustomAlertRuleDefinition definition, string sanitizedName,
        IReadOnlyCollection<string> monitoredStorageNames, DateTime? noDataSinceUtc, DateTime nowUtc)
    {
        // A server-scoped rule whose named servers are not currently monitored never enters any server's
        // applicable set, so it is evaluated zero times, so it can never fire. (An "all"-scoped rule always
        // applies to whatever fleet exists, so an empty fleet is not a per-rule defect and is not flagged.)
        if (definition.ScopeMode == CustomAlertScopeMode.Servers && !monitoredStorageNames.Any(definition.AppliesTo))
        {
            return new CustomAlertRuleHealthIssue(
                row.Id, sanitizedName,
                "armed but scoped only to servers that are not currently monitored, so it never evaluates");
        }

        // Produced no data continuously for longer than the window: an always-NULL measure for every in-scope
        // server (e.g. ACU headroom on a provisioned instance) or a collector that is not producing rows.
        if (noDataSinceUtc is DateTime since && nowUtc - since >= NoDataFlagWindow)
        {
            return new CustomAlertRuleHealthIssue(
                row.Id, sanitizedName,
                $"armed but has produced no data for over {(int)NoDataFlagWindow.TotalMinutes} minutes; the measure may be NULL for every in-scope server, or their collector is not producing rows");
        }

        return null;
    }

    /// <summary>
    /// Builds the fleet-global custom-alert-rule health report (#3304): the broken rules found at the last
    /// cache refresh, plus every compiling rule that is "armed but never fires" (0-server scope, or no data for
    /// <see cref="NoDataFlagWindow"/>). Refreshes the rule cache first (respecting its TTL). The caller
    /// (<c>DarlingWorker</c>) hands the report to <see cref="DarlingSelfAlertEvaluator.EvaluateCustomRuleHealthAsync"/>,
    /// which raises ONE aggregated self-health alert. Returns NULL (not an empty report) when the rule load
    /// fails, so the caller HOLDS the standing alert rather than resolving it on a transient store blip — an
    /// empty report would read as "all healthy" and clear a real broken-rule alert.
    /// </summary>
    public async Task<CustomAlertHealthReport?> BuildHealthReportAsync(
        IReadOnlyCollection<string> monitoredStorageNames, CancellationToken cancellationToken)
    {
        IReadOnlyList<ParsedRule> parsed;
        try
        {
            parsed = await GetEnabledRulesAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Load failed: return null so the caller leaves the standing alert as-is (hold), never resolves
            // it on uncertainty. (This evaluator's store reads sit outside #3013's counted alert-pass census,
            // like its sibling EvaluateServerAsync/RunScalarAsync catches.)
            _logger.LogWarning("Custom alert rule-health check: rule load failed: {Message}", ex.Message);
            return null;
        }

        var now = DateTime.UtcNow;
        var neverFiring = new List<CustomAlertRuleHealthIssue>();
        foreach (var (row, def) in parsed)
        {
            var since = _noDataSinceUtc.TryGetValue(row.Id, out var s) ? s : (DateTime?)null;
            var issue = ClassifyNeverFiring(
                row, def, SanitizeDisplayText(row.Name, CustomAlertRuleStore.MaxNameLength),
                monitoredStorageNames, since, now);
            if (issue is not null)
            {
                neverFiring.Add(issue);
            }
        }

        return new CustomAlertHealthReport(_brokenRules, neverFiring);
    }

    /// <summary>
    /// Decides whether one persisted (rule, server) state row should be TORN DOWN (#3305): a disabled rule's
    /// state is orphaned (the evaluator's <c>ListEnabledAsync</c> skips it, so its incident never resolves on its
    /// own), and an enabled rule's state for a server that has left its scope — or left monitoring entirely
    /// (<paramref name="serverStorageName"/> null) — is stale. Returns the teardown reason, or null to KEEP the
    /// row. Conservative on uncertainty: an enabled rule whose definition is not currently in the cache
    /// (<paramref name="definition"/> null — just enabled, or a stale cache) is left alone rather than torn down.
    /// Pure so both teardown cases are unit-testable without a store.
    /// </summary>
    internal static string? ClassifyStateTeardown(
        bool ruleEnabled, CustomAlertRuleDefinition? definition, string? serverStorageName)
    {
        if (!ruleEnabled)
        {
            return TeardownReasonDisabled;
        }

        if (definition is null)
        {
            // Enabled but its definition is not in the cache this pass — don't tear down on uncertainty.
            return null;
        }

        if (serverStorageName is null || !definition.AppliesTo(serverStorageName))
        {
            return TeardownReasonOutOfScope;
        }

        return null;
    }

    /// <summary>
    /// Reconciles <c>custom_alert_state</c> against the live rules + monitored fleet (#3305): force-resolves and
    /// cleans the state of DISABLED rules and of servers that have left a rule's scope. Deleted rules need no
    /// handling here — their state cascaded away and their open incidents were resolved on the delete path
    /// (<see cref="ResolveAndDeleteRuleAsync"/>). Runs on the fleet-global health cadence. Failure-isolated per
    /// row and at each load, so a bad row or a store blip never stops the sweep (it only ever propagates
    /// cancellation).
    /// </summary>
    /// <param name="monitoredServers">The current fleet: server id → (storage name for the scope check, display
    /// name for the resolution row). A server absent from this map has left monitoring.</param>
    public async Task ReconcileStateAsync(
        IReadOnlyDictionary<int, (string StorageName, string DisplayName)> monitoredServers,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<ParsedRule> parsed;
        try
        {
            parsed = await GetEnabledRulesAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Custom alert reconcile: rule load failed: {Message}", ex.Message);
            return;
        }

        var definitionsById = parsed.ToDictionary(p => p.Row.Id, p => p.Definition);

        IReadOnlyList<CustomAlertStateRow> rows;
        try
        {
            rows = await _stateStore.ListAllWithRuleAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Custom alert reconcile: state load failed: {Message}", ex.Message);
            return;
        }

        foreach (var row in rows)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var storageName = monitoredServers.TryGetValue(row.ServerId, out var server) ? server.StorageName : null;
            var definition = definitionsById.TryGetValue(row.RuleId, out var def) ? def : null;
            var reason = ClassifyStateTeardown(row.RuleEnabled, definition, storageName);
            if (reason is null)
            {
                continue;
            }

            try
            {
                if (row.Firing)
                {
                    var serverName = monitoredServers.TryGetValue(row.ServerId, out var s)
                        ? s.DisplayName
                        : row.ServerId.ToString(CultureInfo.InvariantCulture);
                    await WriteTeardownResolutionAsync(_historyStore, _logger, row.RuleId, row.RuleName, row.ServerId, serverName, reason);
                }

                await _stateStore.DeleteAsync(row.RuleId, row.ServerId, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Custom alert reconcile: teardown of rule {Rule} on server {Server} failed: {Message}",
                    row.RuleId, row.ServerId, ex.Message);
            }
        }
    }
}

/// <summary>One unhealthy custom-alert rule for the #3304 self-health surface: its id, its sanitized name, and
/// a sanitized human reason (a catalog parse error, or why it never fires). All strings are newline-stripped +
/// length-capped so they are safe to render into the aggregated alert's <c>detail_text</c>.</summary>
public sealed record CustomAlertRuleHealthIssue(long RuleId, string RuleName, string Reason);

/// <summary>
/// The fleet-global custom-alert-rule health snapshot (#3304): rules that no longer compile
/// (<see cref="BrokenRules"/>) and compiling rules that can never fire (<see cref="NeverFiringRules"/>).
/// <see cref="DarlingSelfAlertEvaluator"/> raises ONE aggregated self-alert when this has any issues and
/// resolves it when it clears.
/// </summary>
public sealed record CustomAlertHealthReport(
    IReadOnlyList<CustomAlertRuleHealthIssue> BrokenRules,
    IReadOnlyList<CustomAlertRuleHealthIssue> NeverFiringRules)
{
    public static readonly CustomAlertHealthReport Empty =
        new(Array.Empty<CustomAlertRuleHealthIssue>(), Array.Empty<CustomAlertRuleHealthIssue>());

    /// <summary>Total unhealthy rules across both categories.</summary>
    public int TotalIssues => BrokenRules.Count + NeverFiringRules.Count;

    /// <summary>True when at least one rule is broken or never-firing.</summary>
    public bool HasIssues => TotalIssues > 0;
}
