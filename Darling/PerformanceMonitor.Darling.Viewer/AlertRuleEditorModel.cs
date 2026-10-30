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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>The scope the form offers: every server, a set of servers, or the members of a fleet tag.</summary>
public enum AlertRuleScopeKind
{
    All,
    Servers,
    Tag,
}

/// <summary>
/// What the edit form holds, as the text the user typed. Numbers stay text until <see cref="AlertRuleEditorModel.TryBuild"/>, so a
/// blank box means "not set" and a bad one is refused by name.
/// </summary>
public sealed record AlertRuleForm(
    string MeasureKey,
    string? Aggregate,
    string? Unit,
    string Hours,
    string Op,
    string Warn,
    string Critical,
    string Lower,
    string Upper,
    string CriticalLower,
    string CriticalUpper,
    string BreachSamples,
    string ClearSamples,
    AlertRuleScopeKind Scope,
    IReadOnlyList<string> Servers,
    int? TagId);

/// <summary>
/// The rule edit form's logic, kept out of the dialog so a test can drive it (the same split as <c>KnownPatternsEditorModel</c>).
/// It builds the definition JSON the web editor posts and hands it to <see cref="CustomAlertRuleDefinition.TryParse"/>, the one
/// authority <c>/api/alerts/validate</c> and the store's callers use, so Validate says exactly what the web says. A rule being
/// edited keeps every key this form does not own (metric filters, <c>evaluationIntervalSeconds</c>): only the keys below are rewritten.
/// </summary>
public static class AlertRuleEditorModel
{
    /// <summary>The operators the form offers, as the definition's wire names.</summary>
    public static readonly IReadOnlyList<string> Ops = new[] { "gt", "ge", "lt", "le", "between", "outside" };

    /// <summary>True for the two band operators, which take lower and upper bounds instead of a warning and critical bar.</summary>
    public static bool IsRangeOp(string op) => op is "between" or "outside";

    /// <summary>The measures a rule may name, from the compose catalog. Ordered by category then name; nothing is copied, so a measure the catalog adds appears here.</summary>
    public static IReadOnlyList<ComposeMeasure> Measures { get; } =
        MeasureCatalog.Measures.OrderBy(m => m.Category, StringComparer.Ordinal).ThenBy(m => m.DisplayName, StringComparer.Ordinal).ToList();

    public static string MeasureLabel(ComposeMeasure m) => $"{m.Category}: {m.DisplayName} ({m.SourceTable})";

    /// <summary>The aggregates the measure accepts, as wire names. A ratio has none (the catalog fixes it).</summary>
    public static IReadOnlyList<string> AggregatesFor(ComposeMeasure m) =>
        m.Kind == MeasureKind.Ratio ? Array.Empty<string>() : m.ValidAggs.Select(MeasureCatalog.WireName).ToList();

    /// <summary>The units the measure accepts for the aggregate: <c>count</c> alone for a count aggregate, else the measure's unit family.</summary>
    public static IReadOnlyList<string> UnitsFor(ComposeMeasure m, string? aggregate)
    {
        if (m.Kind != MeasureKind.Ratio && aggregate == MeasureCatalog.WireName(ComposeAggregate.Count))
        {
            return new[] { MeasureCatalog.FamilyCount };
        }

        return MeasureCatalog.Family(m.UnitFamily)?.Units.Select(u => u.Name).ToList() ?? (IReadOnlyList<string>)new[] { m.DefaultUnit };
    }

    public static string DefaultAggregateFor(ComposeMeasure m) =>
        m.Kind == MeasureKind.Ratio ? string.Empty : MeasureCatalog.WireName(m.DefaultTimeAgg);

    /// <summary>
    /// Builds the definition JSON from the form, on top of <paramref name="existingJson"/> when editing, and validates it through
    /// <see cref="CustomAlertRuleDefinition.TryParse"/>. Returns null when the definition is accepted, else the refusal text:
    /// the form's own message for a box that is not a number, or the validator's text verbatim.
    /// </summary>
    public static string? TryBuild(string? existingJson, AlertRuleForm form, out string definitionJson)
    {
        definitionJson = string.Empty;
        var measure = MeasureCatalog.Measure(form.MeasureKey);
        if (measure is null)
        {
            return "Pick a metric.";
        }

        JsonObject root;
        try
        {
            root = string.IsNullOrWhiteSpace(existingJson) ? new JsonObject() : (JsonNode.Parse(existingJson) as JsonObject) ?? new JsonObject();
        }
        catch (System.Text.Json.JsonException)
        {
            root = new JsonObject();
        }

        var metric = root["metric"] as JsonObject ?? new JsonObject();
        metric["source"] = measure.SourceTable;
        metric.Remove("measure");
        metric.Remove("ratio");
        if (measure.Kind == MeasureKind.Ratio)
        {
            metric["ratio"] = measure.Key;
            metric.Remove("aggregate");
        }
        else
        {
            metric["measure"] = measure.Key;
            metric["aggregate"] = string.IsNullOrWhiteSpace(form.Aggregate) ? DefaultAggregateFor(measure) : form.Aggregate;
        }

        if (string.IsNullOrWhiteSpace(form.Unit))
        {
            metric.Remove("unit");
        }
        else
        {
            metric["unit"] = form.Unit;
        }

        if (!TryNumber(form.Hours, "Window (hours)", required: true, out var hours, out var error))
        {
            return error;
        }

        metric["hours"] = hours;
        root["metric"] = metric;

        var predicate = new JsonObject { ["op"] = form.Op };
        if (IsRangeOp(form.Op))
        {
            if (!TryAdd(predicate, "lowerBound", form.Lower, "Lower bound", true, out error)
                || !TryAdd(predicate, "upperBound", form.Upper, "Upper bound", true, out error)
                || !TryAdd(predicate, "criticalLowerBound", form.CriticalLower, "Critical lower bound", false, out error)
                || !TryAdd(predicate, "criticalUpperBound", form.CriticalUpper, "Critical upper bound", false, out error))
            {
                return error;
            }
        }
        else if (!TryAdd(predicate, "warnThreshold", form.Warn, "Warning threshold", true, out error)
            || !TryAdd(predicate, "criticalThreshold", form.Critical, "Critical threshold", false, out error))
        {
            return error;
        }

        root["predicate"] = predicate;

        var hysteresis = new JsonObject();
        if (!TryAdd(hysteresis, "breachSamples", form.BreachSamples, "Breach samples", false, out error)
            || !TryAdd(hysteresis, "clearSamples", form.ClearSamples, "Clear samples", false, out error))
        {
            return error;
        }

        root["hysteresis"] = hysteresis;

        switch (form.Scope)
        {
            case AlertRuleScopeKind.Servers:
                var servers = new JsonArray();
                foreach (var server in form.Servers)
                {
                    servers.Add(server);
                }

                root["scope"] = new JsonObject { ["mode"] = "servers", ["servers"] = servers };
                break;
            case AlertRuleScopeKind.Tag:
                root["scope"] = new JsonObject { ["mode"] = "tag" };
                if (form.TagId is { } tagId)
                {
                    root["scope"]!["tagId"] = tagId;
                }

                break;
            default:
                root["scope"] = new JsonObject { ["mode"] = "all" };
                break;
        }

        definitionJson = root.ToJsonString();
        return Validate(definitionJson);
    }

    /// <summary>The validator's verdict: null when the definition parses, else its error text, the same string <c>/api/alerts/validate</c> returns.</summary>
    public static string? Validate(string definitionJson)
    {
        var (parsed, error) = CustomAlertRuleDefinition.TryParse(definitionJson);
        return parsed is not null && error is null ? null : error ?? "definition is invalid.";
    }

    /// <summary>
    /// The expiry the box holds: empty means none; otherwise an ISO-8601 UTC instant. A create refuses a past instant; an edit
    /// refuses one only when it is a change from <paramref name="current"/>, so re-saving an already-expired rule still works.
    /// Shared with the web routes through <see cref="CustomAlertExpiry"/>.
    /// </summary>
    public static (DateTime? Expiry, string? Error) ResolveExpiry(string? text, bool isEdit, DateTime? current) =>
        isEdit ? CustomAlertExpiry.ResolveUpdateExpiry(text ?? string.Empty, current) : CustomAlertExpiry.ResolveCreateExpiry(text);

    public static string ExpiryText(DateTime? expiresAtUtc) => CustomAlertExpiry.Format(expiresAtUtc) ?? string.Empty;

    /// <summary>Reads a saved definition back into form values; null when it does not parse as an object.</summary>
    public static AlertRuleForm? Load(string definitionJson)
    {
        JsonObject? root;
        try
        {
            root = JsonNode.Parse(definitionJson) as JsonObject;
        }
        catch (System.Text.Json.JsonException)
        {
            return null;
        }

        if (root is null)
        {
            return null;
        }

        var metric = root["metric"] as JsonObject;
        var predicate = root["predicate"] as JsonObject;
        var hysteresis = root["hysteresis"] as JsonObject;
        var scope = root["scope"] as JsonObject;
        var servers = (scope?["servers"] as JsonArray)?.Select(n => n?.ToString() ?? string.Empty).Where(s => s.Length > 0).ToList() ?? new List<string>();
        var mode = scope?["mode"]?.ToString()?.ToLowerInvariant();
        var kind = mode switch { "servers" => AlertRuleScopeKind.Servers, "tag" => AlertRuleScopeKind.Tag, _ => AlertRuleScopeKind.All };
        int? tagId = scope?["tagId"] is JsonValue tv && tv.TryGetValue<int>(out var t) ? t : null;

        return new AlertRuleForm(
            Text(metric?["measure"]) is { Length: > 0 } m ? m : Text(metric?["ratio"]),
            Text(metric?["aggregate"]),
            Text(metric?["unit"]),
            Text(metric?["hours"]),
            Text(predicate?["op"]),
            Text(predicate?["warnThreshold"]),
            Text(predicate?["criticalThreshold"]),
            Text(predicate?["lowerBound"]),
            Text(predicate?["upperBound"]),
            Text(predicate?["criticalLowerBound"]),
            Text(predicate?["criticalUpperBound"]),
            Text(hysteresis?["breachSamples"]),
            Text(hysteresis?["clearSamples"]),
            kind,
            servers,
            tagId);
    }

    private static string Text(JsonNode? node) => node is null ? string.Empty : Convert.ToString(node.ToString(), CultureInfo.InvariantCulture) ?? string.Empty;

    private static bool TryNumber(string? text, string label, bool required, out double value, out string? error)
    {
        value = 0;
        error = null;
        if (string.IsNullOrWhiteSpace(text))
        {
            if (required)
            {
                error = $"{label} is required.";
                return false;
            }

            return true;
        }

        if (!double.TryParse(text.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out value) || !double.IsFinite(value))
        {
            error = $"{label} must be a number.";
            return false;
        }

        return true;
    }

    /// <summary>Adds the box's number under <paramref name="key"/> unless it is blank and optional. Whole values are written as integers, since the validator reads the sample counts as ints.</summary>
    private static bool TryAdd(JsonObject target, string key, string? text, string label, bool required, out string? error)
    {
        if (!TryNumber(text, label, required, out var value, out error))
        {
            return false;
        }

        if (!string.IsNullOrWhiteSpace(text))
        {
            target[key] = value == Math.Floor(value) && Math.Abs(value) < 1e15 ? JsonValue.Create((long)value) : JsonValue.Create(value);
        }

        return true;
    }
}
