/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

internal static partial class AlertNotebookEndpoint
{
    /// <summary>Custom-rule alert (<c>Custom:&lt;id&gt;</c>) template version (#4223). Bumped only if this
    /// template's SHAPE changes.</summary>
    internal const int CustomRuleTemplateVersion = 1;

    /// <summary>Custom-rule alert: header, status, ONE composed panel built from the rule's OWN
    /// <see cref="PanelPlan"/> — the same source/measure/filters/aggregate the rule evaluates against,
    /// forced into time-bucket mode over the absolute window (never the scalar shape the rule itself
    /// stores) so the alert shows a trend, not a single point — plus the rule's own threshold or band. A
    /// deleted rule (<see cref="AuthoredContext.CustomRuleMissing"/>) or a definition that no longer parses
    /// degrades to a note, per #2710's degrade rule: never an exception, and never a silent empty
    /// notebook.</summary>
    private static JsonArray BuildCustomRuleCells(
        string? metric, string? serverName, string? asOf, DateTime windowStart, DateTime windowEnd,
        AlertIncident? incident, DarlingAlertReader.AlertHistoryReadRow? row, string status, AuthoredContext context)
    {
        var cells = new JsonArray
        {
            HeaderCell(metric, serverName, incident, row),
            StatusCell(status),
        };

        if (context.CustomRuleMissing || context.CustomRule is null)
        {
            cells.Add(CustomRuleMissingNoteCell());
            return cells;
        }

        var (definition, parseError) = CustomAlertRuleDefinition.TryParse(context.CustomRule.DefinitionJson);
        if (parseError is not null || definition is null)
        {
            cells.Add(CustomRuleUnparseableNoteCell());
            return cells;
        }

        cells.Add(CustomRulePanelCell(definition, context.CustomRule.Name, windowStart, windowEnd));
        return cells;
    }

    /// <summary>The custom rule's own measure, forced into time-bucket mode over the absolute alert window
    /// (spec: "the notebook could plot the exact measure the rule evaluates ... over the window"). The
    /// rule's stored <see cref="PanelPlan"/> is always <see cref="PanelMode.Scalar"/> (the alert-evaluation
    /// shape — <see cref="CustomAlertRuleDefinition.TryParse"/> pins it there); this rebuilds the SAME
    /// source/measure/aggregate/filters as an hourly line, so the panel cell compiles to the identical
    /// query the rule evaluated, just bucketed instead of collapsed to one value.</summary>
    private static JsonObject CustomRulePanelCell(
        CustomAlertRuleDefinition definition, string ruleName, DateTime windowStart, DateTime windowEnd)
    {
        var plan = definition.Plan;
        var measure = plan.Measure;

        var cell = new JsonObject
        {
            ["type"] = "panel",
            ["title"] = CustomRulePanelTitle(ruleName, definition),
            ["source"] = measure.SourceTable,
            ["aggregate"] = MeasureCatalog.WireName(plan.Aggregate),
            ["unit"] = plan.Unit,
            ["viz"] = "line",
            ["timeBucket"] = "hour",
            ["range"] = new JsonObject
            {
                ["windowStart"] = windowStart.ToString("o", CultureInfo.InvariantCulture),
                ["windowEnd"] = windowEnd.ToString("o", CultureInfo.InvariantCulture),
            },
        };

        if (measure.Kind == MeasureKind.Ratio)
        {
            cell["ratio"] = measure.Key;
        }
        else
        {
            cell["measure"] = measure.Key;
        }

        if (plan.Filters.Count > 0)
        {
            var filters = new JsonArray();
            foreach (var filter in plan.Filters)
            {
                filters.Add(CustomRuleFilterCell(filter));
            }

            cell["filters"] = filters;
        }

        var thresholds = CustomRuleThresholds(definition);
        if (thresholds.Count > 0)
        {
            var thresholdsArray = new JsonArray();
            foreach (var value in thresholds)
            {
                thresholdsArray.Add(value);
            }

            cell["thresholds"] = thresholdsArray;
        }

        return cell;
    }

    /// <summary>One of the rule's <see cref="PanelPlan.Filters"/>, rebuilt into the wire shape
    /// <see cref="ComposeSpec.TryParsePanel"/> reads back (<c>dimension</c>/<c>op</c>/<c>value</c>) — the
    /// exact inverse of <c>ParseFilters</c>, so a rebuilt panel filters the SAME rows the rule
    /// evaluated.</summary>
    private static JsonObject CustomRuleFilterCell(ComposeFilter filter)
    {
        var cell = new JsonObject
        {
            ["dimension"] = filter.Dimension.Name,
            ["op"] = MeasureCatalog.WireName(filter.Op),
        };

        if (filter.Value.Literals is { Count: 1 } single)
        {
            cell["value"] = single[0];
        }
        else if (filter.Value.Literals is { Count: > 1 } many)
        {
            var values = new JsonArray();
            foreach (var literal in many)
            {
                values.Add(literal);
            }

            cell["value"] = values;
        }
        else if (filter.Value.VariableRef is string variableRef)
        {
            /* A rule's own filter can never reference a view variable (the rule has none declared), but the
               shape is reproduced defensively rather than dropped silently. */
            cell["value"] = "$" + variableRef;
        }

        return cell;
    }

    /// <summary>The rule's own threshold(s) or band, in <see cref="PanelPlan.Unit"/> (design D3's render-only
    /// reference lines, the same <c>thresholds</c> key <see cref="ComposeSpec"/> accepts on a composed
    /// panel — there is no separate "band" key on that schema, so a range rule's two bounds, plus the
    /// optional critical band, are carried as up to four reference-line values, capped at
    /// <see cref="ComposeLimits.MaxThresholds"/>). A scalar rule carries its warn (+ optional critical); a
    /// range rule carries its lower/upper (+ optional critical lower/upper, in whichever of the four slots
    /// remain).</summary>
    private static System.Collections.Generic.List<double> CustomRuleThresholds(CustomAlertRuleDefinition definition)
    {
        var values = new System.Collections.Generic.List<double>(4);

        if (definition.IsRange)
        {
            if (definition.LowerBound is double lower)
            {
                values.Add(lower);
            }

            if (definition.UpperBound is double upper)
            {
                values.Add(upper);
            }

            if (definition.CriticalLowerBound is double criticalLower)
            {
                values.Add(criticalLower);
            }

            if (definition.CriticalUpperBound is double criticalUpper)
            {
                values.Add(criticalUpper);
            }
        }
        else
        {
            if (definition.WarnThreshold is double warn)
            {
                values.Add(warn);
            }

            if (definition.CriticalThreshold is double critical)
            {
                values.Add(critical);
            }
        }

        if (values.Count > ComposeLimits.MaxThresholds)
        {
            values.RemoveRange(ComposeLimits.MaxThresholds, values.Count - ComposeLimits.MaxThresholds);
        }

        return values;
    }

    /// <summary>The panel's title (spec: plot "the rule's own measure ... with its threshold or band") —
    /// the rule's name, plus its band in parentheses when the threshold values aren't self-explanatory as
    /// reference lines alone (a range rule's four possible bounds have no fixed meaning as plain numbers on
    /// a chart, so the title states the band in words).</summary>
    private static string CustomRulePanelTitle(string ruleName, CustomAlertRuleDefinition definition)
    {
        if (!definition.IsRange)
        {
            return ruleName;
        }

        return definition.Op == CustomAlertOp.Between
            ? $"{ruleName} (band {definition.LowerBound}\u2013{definition.UpperBound})"
            : $"{ruleName} (outside {definition.LowerBound}\u2013{definition.UpperBound})";
    }

    /// <summary>The note a deleted custom rule gets — never an error (#2710's degrade rule): the alert
    /// already fired and is real history; only the rule that named it is gone.</summary>
    private static JsonObject CustomRuleMissingNoteCell() => new()
    {
        ["type"] = "markdown",
        ["text"] = "This custom rule no longer exists; it may have been deleted after this alert fired.",
    };

    /// <summary>The note a custom rule whose stored definition no longer parses gets — a measure or
    /// dimension that drifted out of the catalog since the rule was saved, never a throw.</summary>
    private static JsonObject CustomRuleUnparseableNoteCell() => new()
    {
        ["type"] = "markdown",
        ["text"] = "This custom rule's definition could not be read; it may reference a measure that no "
            + "longer exists.",
    };
}
