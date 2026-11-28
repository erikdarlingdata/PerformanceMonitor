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

    /// <summary>
    /// The root fact key of each finding whose before and after compare the pass left out because the compare's time
    /// budget ended. The finding itself is in the reply and states the change. Empty when the compare finished and when
    /// the pass ran none.
    /// </summary>
    public IReadOnlyList<string> Compares { get; init; } = [];

    /// <summary>The length of the compare's budget, for the reply's note. Null with no budget.</summary>
    public TimeSpan? CompareBudget { get; init; }

    /// <summary>Whether the pass cut any of the three stages this record carries.</summary>
    public bool Any => Detectors.Count > 0 || DrillDowns.Count > 0 || Compares.Count > 0;
}

/// <summary>
/// #9879: the time an on-demand <c>analyze_server</c> pass gives the stages the family budget
/// (<see cref="FamilyTimeBudget"/>) does not reach, and what the reply says when one is cut. Each is counted from the
/// start of the pass, so each is an instant and the reads ahead of a stage come out of it. The anomaly detectors run
/// beside the family reads and end with them. The configuration change compare runs after both and ends 0.75 seconds
/// later. The drill-downs run after the compare and end 0.75 seconds after it, so the stated budgets of one pass add up
/// to <see cref="OnDemandDrillDowns"/>. The scheduled pass has none of them.
/// </summary>
public static class StageTimeBudget
{
    /// <summary>The time an on-demand pass gives its anomaly detectors, from the start of the pass.</summary>
    public static readonly TimeSpan OnDemandDetectors = TimeSpan.FromSeconds(3);

    /// <summary>The time an on-demand pass has, from its start, before it stops drilling into findings.</summary>
    public static readonly TimeSpan OnDemandDrillDowns = TimeSpan.FromSeconds(4.5);

    /// <summary>
    /// The time an on-demand pass has, from its start, before it stops the before and after compare of a configuration
    /// change. The compare is two whole fact collections and runs ahead of the drill-downs, so its instant is earlier
    /// than <see cref="OnDemandDrillDowns"/>: a compare that runs to its end of time still leaves every finding its
    /// drill-down time.
    /// </summary>
    public static readonly TimeSpan OnDemandConfigCompare = TimeSpan.FromSeconds(3.75);

    /// <summary>Where a left-out compare's <c>read_it_with</c> sends the reader for the verdict.</summary>
    public const string CompareReadItWith =
        "compare_analysis over the hours before and after the change, or the scheduled pass's finding in get_analysis_findings";

    /// <summary>The reason word every entry carries: the same one a <c>families_left_out</c> entry does.</summary>
    public const string Reason = FamilyTimeBudget.Reason;

    /// <summary>
    /// What a reply says about the store when the pass left anything out for time, in any of its budgeted stages: a
    /// family, an anomaly detector, a configuration change's compare or a drill-down
    /// (<see cref="AnalysisContext.LeftOutForTime"/>). Such a pass stores no
    /// findings, so the stored latest batch is the last pass that left nothing out. Said once per reply, after the
    /// sentences that name what was left out.
    /// </summary>
    public const string NotStored =
        "This pass's findings were not stored, because it left something out for time: get_analysis_findings shows the last whole pass, not this one.";

    /// <summary>Where a left-out entry's <c>read_it_with</c> sends the reader for the stored findings.</summary>
    public const string LastWholePass = "get_analysis_findings for the last whole pass (this pass stored no findings)";

    /// <summary>
    /// The sentences for the reply's caveat, or null when the pass cut none of the stages. They name what was left out.
    /// Where the stored findings are is <see cref="NotStored"/>, which the reply adds once.
    /// </summary>
    public static string? Describe(StageTimeCuts cuts)
    {
        ArgumentNullException.ThrowIfNull(cuts);
        var detectors = cuts.Detectors.Count == 0
            ? null
            : $"{Count(cuts.Detectors.Count, "anomaly detector was", "anomaly detectors were")} left out for time ({string.Join(", ", cuts.Detectors)}): this call gives the anomaly detectors {Seconds(cuts.DetectorBudget, OnDemandDetectors)} s from its start and {(cuts.Detectors.Count == 1 ? "that one" : "those")} had not finished, so no anomaly finding from {(cuts.Detectors.Count == 1 ? "it" : "them")} is in this answer and {(cuts.Detectors.Count == 1 ? "its" : "their")} absence is not an all-clear. The same call again often finishes, because the baselines it waited for are kept, and the scheduled pass runs every detector without a budget.";
        var drillDowns = cuts.DrillDowns.Count == 0
            ? null
            : $"{Count(cuts.DrillDowns.Count, "finding has", "findings have")} no drill-down detail ({string.Join(", ", Distinct(cuts.DrillDowns).Select(group => group.Count == 1 ? group.Key : $"{group.Key} x{group.Count}"))}): this call stops drilling into findings {Seconds(cuts.DrillDownBudget, OnDemandDrillDowns)} s from its start. Each finding itself is in this answer and is complete; only its supporting detail (top queries, plans, remediation) is missing. The scheduled pass drills into every finding.";
        var compares = cuts.Compares.Count == 0
            ? null
            : $"{Count(cuts.Compares.Count, "finding has", "findings have")} no before and after compare ({string.Join(", ", cuts.Compares)}): this call stops comparing the hours before a configuration change with the hours after it {CompareSeconds(cuts.CompareBudget)} s from its start. The finding is in this answer and states the change itself (the setting, its old and new value, and when), without the verdict on what moved. Read the verdict with {CompareReadItWith}: the scheduled pass runs the whole compare.";
        var sentences = new[] { detectors, compares, drillDowns }.Where(sentence => sentence is not null).ToList();
        return sentences.Count == 0 ? null : string.Join(" ", sentences);
    }

    /// <summary>
    /// <paramref name="payload"/> itself when the pass cut none of the stages, so such a reply keeps its bytes. Otherwise
    /// the payload as a <see cref="JsonObject"/> with <c>detectors_left_out</c>, <c>compares_left_out</c> and then
    /// <c>drill_downs_left_out</c> appended last, each only when it has an entry: one per detector in detector order, one
    /// per finding whose compare was left out, and one per root fact key in finding order with the number of findings it
    /// stands for.
    /// </summary>
    public static object Attach(object payload, StageTimeCuts cuts, JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(payload);
        ArgumentNullException.ThrowIfNull(cuts);
        if (!cuts.Any)
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
                ["read_it_with"] = $"the same call again, or {LastWholePass}"
            }).ToArray());
        }

        if (cuts.Compares.Count > 0)
        {
            var seconds = Math.Round((cuts.CompareBudget ?? OnDemandConfigCompare).TotalSeconds, 2);
            reply["compares_left_out"] = new JsonArray(cuts.Compares.Select(finding => (JsonNode)new JsonObject
            {
                ["finding"] = finding,
                ["compare"] = "before_and_after_the_change",
                ["reason"] = Reason,
                ["budget_seconds"] = seconds,
                ["read_it_with"] = CompareReadItWith
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
                ["read_it_with"] = $"a shorter hours_back, or {LastWholePass}"
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

    private static string CompareSeconds(TimeSpan? budget) =>
        (budget ?? OnDemandConfigCompare).TotalSeconds.ToString("0.##", CultureInfo.InvariantCulture);

    private static string Seconds(TimeSpan? budget, TimeSpan standard) =>
        (budget ?? standard).TotalSeconds.ToString("0.#", CultureInfo.InvariantCulture);
}
