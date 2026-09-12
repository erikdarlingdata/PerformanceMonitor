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

/// <summary>The comparison a rule's value is tested against: a one-sided scalar bar
/// (<see cref="GreaterThan"/>..<see cref="LessOrEqual"/>) or a two-sided range band
/// (<see cref="Between"/>/<see cref="Outside"/>, #3351).</summary>
public enum CustomAlertOp
{
    GreaterThan,
    GreaterOrEqual,
    LessThan,
    LessOrEqual,

    /// <summary>Breaches when the value is INSIDE the inclusive band [lowerBound, upperBound] (#3351).</summary>
    Between,

    /// <summary>Breaches when the value is OUTSIDE the inclusive band [lowerBound, upperBound] (#3351).</summary>
    Outside,
}

/// <summary>Which servers a rule evaluates against: every monitored server (<see cref="All"/>), an explicit
/// set of storage names (<see cref="Servers"/>), or the members of a fleet tag (<see cref="Tag"/>, #3350).
/// Tag membership is store state that changes independently of the rule, so a tag-scoped rule stores only the
/// tag's stable id and the evaluator resolves it to a server-id set each sweep.</summary>
public enum CustomAlertScopeMode
{
    All,
    Servers,
    Tag,
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
    double? WarnThreshold,
    double? CriticalThreshold,
    double? LowerBound,
    double? UpperBound,
    int BreachSamples,
    int ClearSamples,
    CustomAlertScopeMode ScopeMode,
    IReadOnlyList<string> ScopeServers,
    int? ScopeTagId,
    int? EvaluationIntervalSeconds)
{
    /// <summary>Whether this rule's operator tests the value against a two-sided band
    /// (<see cref="LowerBound"/>/<see cref="UpperBound"/>) rather than a single scalar threshold (#3351). A
    /// range op carries both bounds and neither <see cref="WarnThreshold"/> nor <see cref="CriticalThreshold"/>;
    /// a scalar op is the mirror image — the validator enforces that mutual exclusivity.</summary>
    public bool IsRange => IsRangeOp(Op);

    /// <summary>Pure op-category test, usable by the validator before a definition is constructed.</summary>
    private static bool IsRangeOp(CustomAlertOp op) => op is CustomAlertOp.Between or CustomAlertOp.Outside;

    /// <summary>Alert windows are recent (hours), never the 90-day compose chart ceiling — R2 hardening.</summary>
    public const double MaxWindowHours = 24.0;

    /// <summary>One minute floor — a shorter window has too few raw samples to be meaningful.</summary>
    public const double MinWindowHours = 1.0 / 60.0;

    /// <summary>Cadence floor so a rule cannot ask to be evaluated faster than the collectors move.</summary>
    public const int MinEvaluationIntervalSeconds = 30;

    /// <summary>
    /// Whether this rule evaluates against the given storage server name — the PURE All/Servers decision
    /// (no store). <see cref="CustomAlertScopeMode.Tag"/> cannot be decided from a name alone (its membership
    /// is the <c>config.server_tag_map</c> set the evaluator resolves at sweep time), so this returns
    /// <c>false</c> for a tag-scoped rule; the tag-aware gate is
    /// <see cref="CustomAlertEvaluator.RuleAppliesToServer"/>, which delegates the non-tag modes back to this
    /// one authority so All/Servers is resolved in exactly one place.
    /// </summary>
    public bool AppliesTo(string storageName) =>
        ScopeMode == CustomAlertScopeMode.All
        || ScopeServers.Any(s => string.Equals(s, storageName, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// Whether a value breaches the predicate. A scalar op crosses its warning bar (the <see cref="Compare"/>
    /// authority); a range op falls IN (<see cref="CustomAlertOp.Between"/>) or OUT
    /// (<see cref="CustomAlertOp.Outside"/>) of the INCLUSIVE band [<see cref="LowerBound"/>,
    /// <see cref="UpperBound"/>] (#3351), through the shared <see cref="RangeBreaches"/> band authority. Bounds
    /// are guaranteed present for a range op by the validator.
    /// </summary>
    public bool IsBreaching(double value) => IsRange
        ? RangeBreaches(Op, LowerBound, UpperBound, value)
        : WarnThreshold is double warn && Compare(value, warn);

    /// <summary>The pure band-membership decision a range op breaches on (#3351): <see cref="CustomAlertOp.Between"/>
    /// breaches INSIDE the inclusive band [lower, upper], <see cref="CustomAlertOp.Outside"/> beyond it. Factored
    /// out of <see cref="IsBreaching"/> so the band math lives in ONE place, shared by the live evaluation, the
    /// <see cref="BreachesOnZero"/> no-data guard, and the parse-time count guard (#3373). Returns false for a
    /// scalar op (whose bar is <see cref="Compare"/>) and whenever a bound is absent.</summary>
    private static bool RangeBreaches(CustomAlertOp op, double? lowerBound, double? upperBound, double value) => op switch
    {
        CustomAlertOp.Between => lowerBound is double lo && upperBound is double hi && value >= lo && value <= hi,
        CustomAlertOp.Outside => lowerBound is double lo && upperBound is double hi && (value < lo || value > hi),
        _ => false,
    };

    /// <summary>
    /// Whether this range predicate's band treats a no-data 0 as a breach (#3373). A COUNT metric cannot tell
    /// "zero events happened" from "the collector stalled and wrote no rows" — both read as <c>COUNT(*)</c> 0, a
    /// real value (never the NULL the evaluator's no-data freeze catches, unlike SUM/AVG over an empty window) —
    /// so a band that fires on 0 would false-fire on a dead collector. This is the predicate the parse-time count
    /// guard uses to reject such a rule, mirroring the scalar '&lt;'/'&lt;=' count rejection. Expressed as the
    /// band's own <see cref="IsBreaching"/> verdict at 0 so the band logic stays the single authority; meaningful
    /// only for a range op (a scalar op returns false here — its count trap is the separate '&lt;'/'&lt;=' rejection).
    /// </summary>
    public bool BreachesOnZero() => IsRange && IsBreaching(0);

    /// <summary>The tier a breaching value fires at: Critical when it also crosses the critical bar, else Warning.
    /// A range op is Warning-only in v1 (a two-tier band would need a warn-band AND a crit-band = four bounds;
    /// that escalation is a tracked follow-up, not this slice), so it always fires Warning.</summary>
    public AlertSeverityLevel SeverityFor(double value) =>
        !IsRange && CriticalThreshold is double c && Compare(value, c) ? AlertSeverityLevel.Critical : AlertSeverityLevel.Warning;

    /// <summary>The scalar comparison — the single authority for a one-sided bar. Range ops go through the band
    /// logic in <see cref="IsBreaching"/> instead, never here.</summary>
    private bool Compare(double value, double threshold) => Op switch
    {
        CustomAlertOp.GreaterThan => value > threshold,
        CustomAlertOp.GreaterOrEqual => value >= threshold,
        CustomAlertOp.LessThan => value < threshold,
        CustomAlertOp.LessOrEqual => value <= threshold,
        _ => false,
    };

    /// <summary>The scalar operator's human symbol, for rendering a one-sided threshold in an alert body. A range
    /// op has no single symbol — its band renders through <see cref="FiredThreshold"/> — so this is "?" for one.</summary>
    public string OpSymbol => Op switch
    {
        CustomAlertOp.GreaterThan => ">",
        CustomAlertOp.GreaterOrEqual => ">=",
        CustomAlertOp.LessThan => "<",
        CustomAlertOp.LessOrEqual => "<=",
        _ => "?",
    };

    /// <summary>
    /// How the threshold reads in a fired alert body, plus the numeric twin persisted alongside it
    /// (<c>NumericThresholdValue</c>). A scalar op reports the crossed bar as "&lt;symbol&gt; &lt;value&gt;"
    /// (e.g. "&gt;= 25") with that bar as the numeric twin; a range op reports the band as "outside 10 - 100" /
    /// "between 10 - 100" with a NULL numeric twin (a band has no single threshold value). The plain " - " band
    /// separator keeps an em dash — a style tell — out of delivered text. Range ops are Warning-only, so the tier
    /// only selects warn-vs-critical for a scalar op.
    /// </summary>
    public (string Text, double? Numeric) FiredThreshold(AlertSeverityLevel severity)
    {
        if (IsRange)
        {
            var word = Op == CustomAlertOp.Between ? "between" : "outside";
            return (string.Create(CultureInfo.InvariantCulture, $"{word} {LowerBound ?? 0:0.###} - {UpperBound ?? 0:0.###}"), null);
        }

        var bar = severity == AlertSeverityLevel.Critical && CriticalThreshold is double c ? c : WarnThreshold ?? 0;
        return (string.Create(CultureInfo.InvariantCulture, $"{OpSymbol} {bar:0.###}"), bar);
    }

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
            return (null, "'predicate.op' must be one of: gt, ge, lt, le, between, outside.");
        }

        // Scalar ops carry a warning bar (+ optional more-extreme critical); range ops carry a two-sided band.
        // The two shapes are MUTUALLY EXCLUSIVE (#3351): a scalar predicate MUST NOT carry bounds, and a range
        // predicate MUST NOT carry warn/critical. Overloading warn/critical as the band would break the "crossed
        // the warning threshold" wording and the two-tier severity model, so the band lives in its own
        // lowerBound/upperBound and only one shape is populated on any given rule.
        double? warn = null;
        double? critical = null;
        double? lowerBound = null;
        double? upperBound = null;

        if (IsRangeOp(op))
        {
            if (predicate["warnThreshold"] is not null || predicate["criticalThreshold"] is not null)
            {
                return (null, "a range predicate ('between'/'outside') uses 'lowerBound'/'upperBound', not 'warnThreshold'/'criticalThreshold'.");
            }

            if (predicate["lowerBound"] is not JsonNode lowerNode || !TryDouble(lowerNode, out var lower))
            {
                return (null, "'predicate.lowerBound' (a number) is required for a range predicate ('between'/'outside').");
            }

            if (predicate["upperBound"] is not JsonNode upperNode || !TryDouble(upperNode, out var upper))
            {
                return (null, "'predicate.upperBound' (a number) is required for a range predicate ('between'/'outside').");
            }

            if (!(lower < upper))
            {
                return (null, "'predicate.lowerBound' must be less than 'predicate.upperBound'.");
            }

            // A range band that fires when the count is 0 has the SAME stalled-collector ambiguity as the scalar
            // '<'/'<=' count trap below: COUNT(*) over an empty window is 0 (never the NULL the evaluator's
            // no-data freeze catches), so a dead collector reads 0 and the band false-fires exactly when the true
            // signal is "no data". Guard it the same way, keyed on the same COUNT-aggregate archetype and routed
            // through the same band authority (RangeBreaches) the live evaluation uses. 'outside' fires on 0 when
            // 0 < lower; 'between' when lower <= 0 <= upper; nudging the bounds so 0 falls outside the firing
            // region (outside -> lower 0, between -> lower above 0) makes the band safe on an empty window.
            if (plan.Aggregate == ComposeAggregate.Count && RangeBreaches(op, lower, upper, 0))
            {
                return (null, "a 'between'/'outside' alert on a count whose band fires when the count is 0 can't distinguish zero events from a stalled collector; set the bounds so a count of 0 does not fire.");
            }

            lowerBound = lower;
            upperBound = upper;
        }
        else
        {
            if (predicate["lowerBound"] is not null || predicate["upperBound"] is not null)
            {
                return (null, "a scalar predicate ('gt'/'ge'/'lt'/'le') uses 'warnThreshold' (+ an optional 'criticalThreshold'), not 'lowerBound'/'upperBound'.");
            }

            // A '<'/'<=' alert on a COUNT cannot tell zero matching events from a stalled collector reading zero
            // rows, so it would false-fire exactly when the true signal is "no data". Deferred; use a >= count.
            if (plan.Aggregate == ComposeAggregate.Count && op is CustomAlertOp.LessThan or CustomAlertOp.LessOrEqual)
            {
                return (null, "a '<'/'<=' alert on a count can't distinguish zero events from a stalled collector; use '>=' instead.");
            }

            if (predicate["warnThreshold"] is not JsonNode warnNode || !TryDouble(warnNode, out var w))
            {
                return (null, "'predicate.warnThreshold' (a number) is required.");
            }

            warn = w;

            if (predicate["criticalThreshold"] is JsonNode critNode)
            {
                if (!TryDouble(critNode, out var c))
                {
                    return (null, "'predicate.criticalThreshold' must be a number.");
                }

                // Critical must be MORE extreme than warning in the operator's direction.
                var ordered = op is CustomAlertOp.GreaterThan or CustomAlertOp.GreaterOrEqual ? c >= w : c <= w;
                if (!ordered)
                {
                    return (null, "'predicate.criticalThreshold' must be more extreme than 'warnThreshold' in the operator's direction.");
                }

                critical = c;
            }
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
        int? scopeTagId = null;
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
                    // Tag scope (#3350): the rule evaluates the DIRECTLY-assigned members of one fleet tag,
                    // stored by the tag's STABLE id (not its name — a rename must not silently re-scope the rule),
                    // the same immutable-id discipline the metric key uses. The id -> current server-id set is
                    // resolved by the evaluator from config.server_tag_map each sweep; a tag that resolves to no
                    // servers (empty, deleted, or renamed away) matches nothing, so the rule never fires — and
                    // that 0-server case is surfaced by the #3304 never-firing self-health check.
                    scopeMode = CustomAlertScopeMode.Tag;
                    if (scope["tagId"] is not JsonNode tagIdNode || !TryInt(tagIdNode, out var tagId))
                    {
                        return (null, "'scope.tagId' (an integer fleet-tag id) is required when scope.mode is 'tag'.");
                    }

                    if (tagId <= 0)
                    {
                        return (null, "'scope.tagId' must be a positive integer fleet-tag id.");
                    }

                    scopeTagId = tagId;
                    break;
                default:
                    return (null, $"'scope.mode' must be 'all', 'servers', or 'tag' (got '{mode}').");
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
            plan, plan.Measure.DisplayName, windowHours, op, warn, critical, lowerBound, upperBound,
            breachSamples, clearSamples, scopeMode, scopeServers, scopeTagId, intervalSeconds);
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
            case "between":
                op = CustomAlertOp.Between;
                return true;
            case "outside":
                op = CustomAlertOp.Outside;
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
