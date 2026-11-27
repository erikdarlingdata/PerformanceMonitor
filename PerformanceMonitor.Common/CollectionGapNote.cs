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

namespace PerformanceMonitor.Common;

/// <summary>
/// #9339: what one snapshot collector's missing runs were in a read's window. <see cref="SkippedRecords"/> and
/// <see cref="SkippedSlots"/> come from the skipped-slot rows the collection loop writes (one row stands for several
/// scheduled slots of the server), and <see cref="FailedRuns"/> counts the collector's own runs that did not end SUCCESS.
/// </summary>
public sealed record CollectionGapFacts(
    long SkippedRecords,
    long SkippedSlots,
    DateTime? SkippedFirstUtc,
    DateTime? SkippedLastUtc,
    long FailedRuns,
    DateTime? FailedFirstUtc,
    DateTime? FailedLastUtc)
{
    /// <summary>True when the window overlaps at least one skipped slot record or one failed run.</summary>
    public bool Any => SkippedRecords > 0 || FailedRuns > 0;
}

/// <summary>
/// #9339: the note an empty snapshot answer carries when collection skipped slots or failed runs inside the window.
/// An empty answer from a snapshot tool reads as "nothing was happening", and that is false when the collector could not
/// connect (a THREADPOOL episode starves the collector's own connection). The sentence is shared by Lite and Darling, so
/// both say the same thing. It touches only an <c>empty</c> answer: an offline server's <c>unavailable</c> answer keeps its
/// own words.
/// </summary>
public static class CollectionGapNote
{
    /// <summary>The key of the structured copy in the answer's <c>hints</c>.</summary>
    public const string HintKey = "collection_gaps_in_window";

    private static string Time(DateTime? utc) =>
        DateTime.SpecifyKind(utc!.Value, DateTimeKind.Utc).ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

    private static string Span(DateTime? first, DateTime? last) =>
        first is null || last is null
            ? string.Empty
            : first.Value == last.Value ? " at " + Time(first) : " between " + Time(first) + " and " + Time(last);

    /// <summary>The sentence for <paramref name="facts"/>, or null when the window held no gap.</summary>
    public static string? Sentence(string collectorName, CollectionGapFacts facts)
    {
        if (!facts.Any)
        {
            return null;
        }

        var parts = new System.Collections.Generic.List<string>();
        if (facts.SkippedRecords > 0)
        {
            parts.Add(CountText.Plural(facts.SkippedSlots, "collection slot") + " " + (facts.SkippedSlots == 1 ? "was" : "were")
                + " skipped" + Span(facts.SkippedFirstUtc, facts.SkippedLastUtc));
        }

        if (facts.FailedRuns > 0)
        {
            parts.Add(CountText.Plural(facts.FailedRuns, "run") + " of the " + collectorName + " collector did not succeed"
                + Span(facts.FailedFirstUtc, facts.FailedLastUtc));
        }

        return "Collection did not run cleanly in this window: " + string.Join(", and ", parts)
            + ". A skipped or failed run stores no snapshot, so this empty answer does not show that nothing was happening. "
            + "Check get_collection_health for the cause.";
    }

    /// <summary>
    /// <paramref name="answerJson"/> with the gap sentence added to its <c>message</c> and the counts and times in
    /// <c>hints.collection_gaps_in_window</c>. An answer that is not <c>empty</c>, or a window with no gap, comes back unchanged.
    /// </summary>
    public static string Apply(string answerJson, string collectorName, CollectionGapFacts facts)
    {
        var sentence = Sentence(collectorName, facts);
        if (sentence is null
            || JsonNode.Parse(answerJson) is not JsonObject node
            || node["status"]?.GetValue<string>() != "empty")
        {
            return answerJson;
        }

        var message = node["message"]?.GetValue<string>();
        node["message"] = string.IsNullOrWhiteSpace(message) ? sentence : message.TrimEnd() + " " + sentence;

        var gap = new JsonObject
        {
            ["collector"] = collectorName,
            ["skipped_slots"] = facts.SkippedSlots,
            ["skipped_records"] = facts.SkippedRecords,
            ["skipped_first"] = McpHelpers.FormatEffectiveStart(facts.SkippedFirstUtc),
            ["skipped_last"] = McpHelpers.FormatEffectiveStart(facts.SkippedLastUtc),
            ["failed_runs"] = facts.FailedRuns,
            ["failed_first"] = McpHelpers.FormatEffectiveStart(facts.FailedFirstUtc),
            ["failed_last"] = McpHelpers.FormatEffectiveStart(facts.FailedLastUtc),
        };

        var hints = node["hints"] as JsonObject ?? new JsonObject();
        hints[HintKey] = gap;
        node["hints"] = hints;
        return node.ToJsonString(McpHelpers.JsonOptions);
    }
}
