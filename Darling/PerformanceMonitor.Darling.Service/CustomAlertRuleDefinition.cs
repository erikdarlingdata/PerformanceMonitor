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
using System.Text.Json.Nodes;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>The scalar comparison a rule's value is tested against.</summary>
public enum CustomAlertOp
{
    GreaterThan,
    GreaterOrEqual,
    LessThan,
    LessOrEqual,
}

/// <summary>Which servers a rule evaluates against. Tag scope is deferred to a later slice.</summary>
public enum CustomAlertScopeMode
{
    All,
    Servers,
}

/// <summary>
/// A parsed, validated custom-alert rule definition (#3285). This is also the thin rule VALIDATOR: the
/// metric is validated by the compose <see cref="ComposeSpec.TryParsePanel"/> authority (so a rule can name
/// nothing a chart cannot) and pinned to <see cref="PanelMode.Scalar"/>; the predicate/hysteresis/scope are
/// validated here. <see cref="TryParse"/> is pure (no DB), so the MCP create/validate tools and the editor
/// can call it before persisting, and the <see cref="CustomAlertEvaluator"/> calls it when it loads rules —
/// a rule that no longer parses (a measure drifted out of the catalog) is not evaluated.
///
/// <para>The parsed <see cref="Plan"/> is captured once so the evaluator only re-COMPILES per window (the
/// window changes each run; the metric structure does not). The window (<see cref="WindowHours"/>) is an
/// alert-appropriate ceiling (<see cref="MaxWindowHours"/>), NOT the 90-day compose chart ceiling.</para>
/// </summary>
public sealed record CustomAlertRuleDefinition(
    PanelPlan Plan,
    string MeasureDisplayName,
    double WindowHours,
    CustomAlertOp Op,
    double WarnThreshold,
    double? CriticalThreshold,
    int BreachSamples,
    int ClearSamples,
    CustomAlertScopeMode ScopeMode,
    IReadOnlyList<string> ScopeServers,
    int? EvaluationIntervalSeconds)
{
    /// <summary>Alert windows are recent (hours), never the 90-day compose chart ceiling — R2 hardening.</summary>
    public const double MaxWindowHours = 24.0;

    /// <summary>One minute floor — a shorter window has too few raw samples to be meaningful.</summary>
    public const double MinWindowHours = 1.0 / 60.0;

    /// <summary>Cadence floor so a rule cannot ask to be evaluated faster than the collectors move.</summary>
    public const int MinEvaluationIntervalSeconds = 30;

    /// <summary>Whether this rule evaluates against the given storage server name.</summary>
    public bool AppliesTo(string storageName) =>
        ScopeMode == CustomAlertScopeMode.All
        || ScopeServers.Any(s => string.Equals(s, storageName, StringComparison.OrdinalIgnoreCase));

    /// <summary>Whether a value breaches the warning bar (the predicate).</summary>
    public bool IsBreaching(double value) => Compare(value, WarnThreshold);

    /// <summary>The tier a breaching value fires at: Critical when it also crosses the critical bar, else Warning.</summary>
    public AlertSeverityLevel SeverityFor(double value) =>
        CriticalThreshold is double c && Compare(value, c) ? AlertSeverityLevel.Critical : AlertSeverityLevel.Warning;

    private bool Compare(double value, double threshold) => Op switch
    {
        CustomAlertOp.GreaterThan => value > threshold,
        CustomAlertOp.GreaterOrEqual => value >= threshold,
        CustomAlertOp.LessThan => value < threshold,
        CustomAlertOp.LessOrEqual => value <= threshold,
        _ => false,
    };

    /// <summary>The operator's human symbol, for rendering the threshold in an alert body.</summary>
    public string OpSymbol => Op switch
    {
        CustomAlertOp.GreaterThan => ">",
        CustomAlertOp.GreaterOrEqual => ">=",
        CustomAlertOp.LessThan => "<",
        CustomAlertOp.LessOrEqual => "<=",
        _ => "?",
    };

    /// <summary>
    /// Parses and validates a rule definition JSON. Returns the typed definition, or a human-readable error
    /// (which the create/validate tools surface to the author and the evaluator logs as a broken rule).
    /// </summary>
    public static (CustomAlertRuleDefinition? Definition, string? Error) TryParse(string definitionJson)
    {
        JsonObject? root;
        try
        {
            root = JsonNode.Parse(definitionJson) as JsonObject;
        }
        catch (Exception ex)
        {
            return (null, $"definition is not valid JSON: {ex.Message}");
        }

        if (root is null)
        {
            return (null, "definition must be a JSON object.");
        }

        // ---- metric (a compose Scalar panel + a window) ----
        if (root["metric"] is not JsonObject metricNode)
        {
            return (null, "'metric' (a compose panel spec) is required.");
        }

        // Deep-clone so removing the run-level 'hours' key does not mutate the caller's tree, then hand the
        // pure panel to the compose authority (which rejects unknown keys, so 'hours' must not remain).
        var panel = JsonNode.Parse(metricNode.ToJsonString()) as JsonObject;
        if (panel is null)
        {
            return (null, "'metric' must be a JSON object.");
        }

        var windowHours = 1.0;
        if (panel["hours"] is JsonNode hoursNode)
        {
            if (!TryDouble(hoursNode, out windowHours))
            {
                return (null, "'metric.hours' must be a number.");
            }

            panel.Remove("hours");
        }

        if (windowHours < MinWindowHours || windowHours > MaxWindowHours)
        {
            return (null, $"'metric.hours' must be between {MinWindowHours:0.###} and {MaxWindowHours:0} (an alert window is recent, not a 90-day chart span).");
        }

        // The metric is evaluated to a single value, never rendered, so supply the scalar 'stat' viz the
        // compose validator requires rather than making the author choose a meaningless chart type.
        if (panel["viz"] is null)
        {
            panel["viz"] = "stat";
        }

        var (plan, planError) = ComposeSpec.TryParsePanel(panel, Array.Empty<string>());
        if (planError is not null || plan is null)
        {
            return (null, $"invalid metric: {planError}");
        }

        if (plan.Mode != PanelMode.Scalar)
        {
            return (null, "an alert metric must be a single value — remove 'timeBucket' and 'topN' from the metric.");
        }

        // ---- predicate ----
        if (root["predicate"] is not JsonObject predicate)
        {
            return (null, "'predicate' is required.");
        }

        if (predicate["op"] is not JsonValue opValue || !TryParseOp(opValue.ToString(), out var op))
        {
            return (null, "'predicate.op' must be one of: gt, ge, lt, le.");
        }

        // A '<'/'<=' alert on a COUNT cannot tell zero matching events from a stalled collector reading zero
        // rows, so it would false-fire exactly when the true signal is "no data". Deferred; use a >= count.
        if (plan.Aggregate == ComposeAggregate.Count && op is CustomAlertOp.LessThan or CustomAlertOp.LessOrEqual)
        {
            return (null, "a '<'/'<=' alert on a count can't distinguish zero events from a stalled collector; use '>=' instead.");
        }

        if (predicate["warnThreshold"] is not JsonNode warnNode || !TryDouble(warnNode, out var warn))
        {
            return (null, "'predicate.warnThreshold' (a number) is required.");
        }

        double? critical = null;
        if (predicate["criticalThreshold"] is JsonNode critNode)
        {
            if (!TryDouble(critNode, out var c))
            {
                return (null, "'predicate.criticalThreshold' must be a number.");
            }

            // Critical must be MORE extreme than warning in the operator's direction.
            var ordered = op is CustomAlertOp.GreaterThan or CustomAlertOp.GreaterOrEqual ? c >= warn : c <= warn;
            if (!ordered)
            {
                return (null, "'predicate.criticalThreshold' must be more extreme than 'warnThreshold' in the operator's direction.");
            }

            critical = c;
        }

        // ---- hysteresis ----
        var breachSamples = 1;
        var clearSamples = 1;
        if (root["hysteresis"] is JsonObject hysteresis)
        {
            if (hysteresis["breachSamples"] is JsonNode bs && (!TryInt(bs, out breachSamples) || breachSamples < 1))
            {
                return (null, "'hysteresis.breachSamples' must be an integer >= 1.");
            }

            if (hysteresis["clearSamples"] is JsonNode cs && (!TryInt(cs, out clearSamples) || clearSamples < 1))
            {
                return (null, "'hysteresis.clearSamples' must be an integer >= 1.");
            }
        }

        // ---- scope ----
        var scopeMode = CustomAlertScopeMode.All;
        IReadOnlyList<string> scopeServers = Array.Empty<string>();
        if (root["scope"] is JsonObject scope)
        {
            var mode = (scope["mode"] as JsonValue)?.ToString() ?? "all";
            switch (mode.ToLowerInvariant())
            {
                case "all":
                    scopeMode = CustomAlertScopeMode.All;
                    break;
                case "servers":
                    scopeMode = CustomAlertScopeMode.Servers;
                    if (scope["servers"] is not JsonArray serverArray || serverArray.Count == 0)
                    {
                        return (null, "'scope.servers' must be a non-empty array when scope.mode is 'servers'.");
                    }

                    scopeServers = serverArray
                        .Select(n => (n as JsonValue)?.ToString())
                        .Where(n => !string.IsNullOrWhiteSpace(n))
                        .Select(n => n!)
                        .ToList();
                    if (scopeServers.Count == 0)
                    {
                        return (null, "'scope.servers' must contain at least one server name.");
                    }

                    break;
                case "tag":
                    return (null, "tag scope is not yet supported; use scope.mode 'all' or 'servers'.");
                default:
                    return (null, $"'scope.mode' must be 'all' or 'servers' (got '{mode}').");
            }
        }

        // ---- cadence ----
        int? intervalSeconds = null;
        if (root["evaluationIntervalSeconds"] is JsonNode intervalNode)
        {
            if (!TryInt(intervalNode, out var interval) || interval < MinEvaluationIntervalSeconds)
            {
                return (null, $"'evaluationIntervalSeconds' must be an integer >= {MinEvaluationIntervalSeconds}.");
            }

            intervalSeconds = interval;
        }

        var definition = new CustomAlertRuleDefinition(
            plan, plan.Measure.DisplayName, windowHours, op, warn, critical,
            breachSamples, clearSamples, scopeMode, scopeServers, intervalSeconds);
        return (definition, null);
    }

    private static bool TryParseOp(string? raw, out CustomAlertOp op)
    {
        op = CustomAlertOp.GreaterThan;
        switch (raw?.Trim().ToLowerInvariant())
        {
            case "gt":
            case ">":
                op = CustomAlertOp.GreaterThan;
                return true;
            case "ge":
            case ">=":
                op = CustomAlertOp.GreaterOrEqual;
                return true;
            case "lt":
            case "<":
                op = CustomAlertOp.LessThan;
                return true;
            case "le":
            case "<=":
                op = CustomAlertOp.LessOrEqual;
                return true;
            default:
                return false;
        }
    }

    private static bool TryDouble(JsonNode node, out double value)
    {
        value = 0;
        return node is JsonValue v && v.TryGetValue(out value);
    }

    private static bool TryInt(JsonNode node, out int value)
    {
        value = 0;
        return node is JsonValue v && v.TryGetValue(out value);
    }
}
