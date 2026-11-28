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
/// #9879: what an on-demand <c>analyze_server</c> reply says about the family reads its time budget stopped. Over a long
/// window on a busy server the four query-history families (parameter sensitivity, plan advisory, bad actor, procedure
/// stats) each read every row of the window, and the call waited for the slowest of them. The pass now gives those four a
/// stated time. One that has not finished is stopped, and the reply names it in <c>families_left_out</c>, in the style of
/// <c>findings_left_out</c>: which family, why, and where its findings can still be read.
/// </summary>
public static class FamilyTimeBudget
{
    /// <summary>The time an on-demand <c>analyze_server</c> pass gives its heavy family reads, from the start of the pass.</summary>
    public static readonly TimeSpan OnDemand = TimeSpan.FromSeconds(3);

    /// <summary>The reason word every <c>families_left_out</c> entry carries.</summary>
    public const string Reason = "time_budget";

    /// <summary>The sentence for the reply's caveat, or null when no family was left out.</summary>
    public static string? Describe(IReadOnlyList<string> leftOut, TimeSpan? budget)
    {
        if (leftOut is null || leftOut.Count == 0)
            return null;

        var seconds = (budget ?? OnDemand).TotalSeconds.ToString("0.#", CultureInfo.InvariantCulture);
        var names = string.Join(", ", leftOut);
        return leftOut.Count == 1
            ? $"1 family was left out for time ({names}): this call gives the heavy query-history reads {seconds} s from its start and that read had not finished, so no finding from it is in this answer and its absence is not an all-clear. A shorter hours_back finishes inside the budget, and the scheduled pass reads every family without one (get_analysis_findings)."
            : $"{leftOut.Count} families were left out for time ({names}): this call gives the heavy query-history reads {seconds} s from its start and those reads had not finished, so no finding from them is in this answer and their absence is not an all-clear. A shorter hours_back finishes inside the budget, and the scheduled pass reads every family without one (get_analysis_findings).";
    }

    /// <summary>
    /// <paramref name="payload"/> itself when no family was left out, so such a reply keeps its bytes. Otherwise the payload
    /// as a <see cref="JsonObject"/> with <c>families_left_out</c> appended last: one entry per family, in family order.
    /// </summary>
    public static object Attach(object payload, IReadOnlyList<string> leftOut, TimeSpan? budget, JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(payload);
        if (leftOut is null || leftOut.Count == 0)
            return payload;

        var reply = payload as JsonObject ?? JsonSerializer.SerializeToNode(payload, options)!.AsObject();
        var seconds = Math.Round((budget ?? OnDemand).TotalSeconds, 1);
        reply["families_left_out"] = new JsonArray(leftOut.Select(family => (JsonNode)new JsonObject
        {
            ["family"] = family,
            ["reason"] = Reason,
            ["budget_seconds"] = seconds,
            ["read_it_with"] = "a shorter hours_back, or get_analysis_findings for the scheduled pass"
        }).ToArray());
        return reply;
    }
}
