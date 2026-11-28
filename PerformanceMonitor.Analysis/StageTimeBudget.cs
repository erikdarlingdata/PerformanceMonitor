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
using System.Text.Json;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// #9879: what one pass left out because a stage's time budget ended: the anomaly detectors that had not finished, and
/// the findings whose drill-down had not. Both lists are in the order the pass runs them. A drill-down entry is the root
/// fact key of its finding, once per finding.
/// </summary>
public sealed record StageTimeCuts(
    IReadOnlyList<string> Detectors, TimeSpan? DetectorBudget,
    IReadOnlyList<string> DrillDowns, TimeSpan? DrillDownBudget)
{
    /// <summary>A pass with no cut.</summary>
    public static readonly StageTimeCuts None = new([], null, [], null);
}

/// <summary>
/// #9879: the time an on-demand <c>analyze_server</c> pass gives the two stages the family budget
/// (<see cref="FamilyTimeBudget"/>) does not reach, and what the reply says when one is cut. Both are counted from the
/// start of the pass, so each is an instant and the reads ahead of a stage come out of it. The anomaly detectors run
/// beside the family reads and end with them. The drill-downs run after both and end 1.5 seconds later, so the stated
/// budgets of one pass add up to <see cref="OnDemandDrillDowns"/>. The scheduled pass has neither.
/// </summary>
public static class StageTimeBudget
{
    /// <summary>The time an on-demand pass gives its anomaly detectors, from the start of the pass.</summary>
    public static readonly TimeSpan OnDemandDetectors = TimeSpan.FromSeconds(3);

    /// <summary>The time an on-demand pass has, from its start, before it stops drilling into findings.</summary>
    public static readonly TimeSpan OnDemandDrillDowns = TimeSpan.FromSeconds(4.5);

    /// <summary>The reason word every entry carries: the same one a <c>families_left_out</c> entry does.</summary>
    public const string Reason = FamilyTimeBudget.Reason;

    /// <summary>The sentences for the reply's caveat, or null when the pass cut neither stage.</summary>
    public static string? Describe(StageTimeCuts cuts)
    {
        ArgumentNullException.ThrowIfNull(cuts);
        var detectors = cuts.Detectors.Count == 0
            ? null
            : $"{Count(cuts.Detectors.Count, "anomaly detector was", "anomaly detectors were")} left out for time ({string.Join(", ", cuts.Detectors)}): this call gives the anomaly detectors {Seconds(cuts.DetectorBudget, OnDemandDetectors)} s from its start and {(cuts.Detectors.Count == 1 ? "that one" : "those")} had not finished, so no anomaly finding from {(cuts.Detectors.Count == 1 ? "it" : "them")} is in this answer and {(cuts.Detectors.Count == 1 ? "its" : "their")} absence is not an all-clear. The same call again often finishes, because the baselines it waited for are kept, and the scheduled pass runs every detector without a budget (get_analysis_findings).";
        var drillDowns = cuts.DrillDowns.Count == 0
            ? null
            : $"{Count(cuts.DrillDowns.Count, "finding has", "findings have")} no drill-down detail ({string.Join(", ", Distinct(cuts.DrillDowns).Select(group => group.Count == 1 ? group.Key : $"{group.Key} x{group.Count}"))}): this call stops drilling into findings {Seconds(cuts.DrillDownBudget, OnDemandDrillDowns)} s from its start. Each finding itself is in this answer and is complete; only its supporting detail (top queries, plans, remediation) is missing. The scheduled pass drills into every finding (get_analysis_findings).";
        return detectors is null ? drillDowns : drillDowns is null ? detectors : $"{detectors} {drillDowns}";
    }

    /// <summary>
    /// <paramref name="payload"/> itself when the pass cut neither stage, so such a reply keeps its bytes. Otherwise the
    /// payload as a <see cref="JsonObject"/> with <c>detectors_left_out</c> and then <c>drill_downs_left_out</c> appended
    /// last, each only when it has an entry: one per detector in detector order, and one per root fact key in finding
    /// order with the number of findings it stands for.
    /// </summary>
    public static object Attach(object payload, StageTimeCuts cuts, JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(payload);
        ArgumentNullException.ThrowIfNull(cuts);
        if (cuts.Detectors.Count == 0 && cuts.DrillDowns.Count == 0)
            return payload;

        var reply = payload as JsonObject ?? JsonSerializer.SerializeToNode(payload, options)!.AsObject();
        if (cuts.Detectors.Count > 0)
        {
            var seconds = Math.Round((cuts.DetectorBudget ?? OnDemandDetectors).TotalSeconds, 1);
            reply["detectors_left_out"] = new JsonArray(cuts.Detectors.Select(detector => (JsonNode)new JsonObject
            {
                ["detector"] = detector,
                ["reason"] = Reason,
                ["budget_seconds"] = seconds,
                ["read_it_with"] = "the same call again, or get_analysis_findings for the scheduled pass"
            }).ToArray());
        }

        if (cuts.DrillDowns.Count > 0)
        {
            var seconds = Math.Round((cuts.DrillDownBudget ?? OnDemandDrillDowns).TotalSeconds, 1);
            reply["drill_downs_left_out"] = new JsonArray(Distinct(cuts.DrillDowns).Select(group => (JsonNode)new JsonObject
            {
                ["finding"] = group.Key,
                ["findings"] = group.Count,
                ["reason"] = Reason,
                ["budget_seconds"] = seconds,
                ["read_it_with"] = "a shorter hours_back, or get_analysis_findings for the scheduled pass"
            }).ToArray());
        }

        return reply;
    }

    /// <summary>Each root fact key once, in first-seen order, with how many findings carry it.</summary>
    private static List<(string Key, int Count)> Distinct(IReadOnlyList<string> keys)
    {
        var groups = new List<(string Key, int Count)>();
        foreach (var key in keys)
        {
            var at = groups.FindIndex(group => string.Equals(group.Key, key, StringComparison.Ordinal));
            if (at < 0)
                groups.Add((key, 1));
            else
                groups[at] = (key, groups[at].Count + 1);
        }

        return groups;
    }

    private static string Count(int count, string one, string many) =>
        count == 1 ? $"1 {one}" : $"{count.ToString(CultureInfo.InvariantCulture)} {many}";

    private static string Seconds(TimeSpan? budget, TimeSpan standard) =>
        (budget ?? standard).TotalSeconds.ToString("0.#", CultureInfo.InvariantCulture);
}
