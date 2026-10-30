/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Common;

/// <summary>
/// The fields that make an investigation report reusable (#6004, D17 of #5632 Phase C): <c>family_of</c> (the first report's
/// request id, checked to exist by the submit), a required <c>basis</c> on each evidence item, and <c>queries_run</c> (the
/// statements the agent ran, masked as statement text when served). The caps live here once: the validation, the store's own
/// check and the tool description name these constants, and V193's check constraint
/// (<c>ck_investigation_reports_queries_run</c>, 20 items and 2,000 characters) holds the same numbers.
/// </summary>
public static class InvestigationReportFields
{
    /// <summary>The most <c>queries_run</c> items (V193's check constraint).</summary>
    public const int MaxQueriesRun = 20;

    /// <summary>The longest <c>queries_run</c> item, in characters (V193's check constraint).</summary>
    public const int MaxQueryChars = 2000;

    /// <summary>The <c>queries_run</c> cap as a tool description states it. A const, because an attribute takes no other
    /// kind; an interpolated int is not one, so the numbers here are the two above written out and the tool test reads the
    /// published description against them.</summary>
    public const string QueriesRunRule = "at most 20 statement strings, each at most 2,000 characters";

    /// <summary>How far ahead a report may set <c>follow_up_at</c> (#6008, D16).</summary>
    public const int MaxFollowUpDays = 30;

    /// <summary>The most pending follow-ups one route may hold (#6008, D16): reported requests whose follow-up has not opened.</summary>
    public const int MaxPendingFollowUpsPerRoute = 20;

    /// <summary>The <c>follow_up_at</c> rule as a tool description states it.</summary>
    public const string FollowUpRule = "an ISO 8601 time after now and at most 30 days ahead; at most 20 pending follow-ups per route";

    /// <summary>
    /// Parses the <c>follow_up_at</c> argument (null or blank is none; a time with no offset is UTC). Gives one sentence
    /// (without the field name) when it is not a time, is not after <paramref name="nowUtc"/>, or is more than
    /// <see cref="MaxFollowUpDays"/> days past it; never a clamp (#6008).
    /// </summary>
    public static string? ValidateFollowUp(string? followUpAt, DateTime nowUtc, out DateTime? followUpAtUtc)
    {
        followUpAtUtc = null;
        if (string.IsNullOrWhiteSpace(followUpAt))
            return null;
        if (!DateTimeOffset.TryParse(followUpAt.Trim(), System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal, out var parsed))
            return "not a time; give an ISO 8601 time such as 2026-11-01T09:00:00Z.";
        var utc = parsed.UtcDateTime;
        return FollowUpProblem(utc, nowUtc) is { } problem ? problem : Accept(utc, out followUpAtUtc);
    }

    /// <summary>The <c>follow_up_condition</c> rule as a tool description states it (#6009).</summary>
    public const string FollowUpConditionRule = "module:<database>.<schema>.<object>, needs follow_up_at as its latest date";

    /// <summary>The longest one name of a module condition may be (a SQL Server identifier).</summary>
    public const int MaxModuleNameChars = 128;

    /// <summary>
    /// Parses the <c>follow_up_condition</c> argument (#6009, D16): null or blank is none; otherwise
    /// <c>module:&lt;database&gt;.&lt;schema&gt;.&lt;object&gt;</c>, three non-empty names with no brackets, and only with a
    /// <c>follow_up_at</c> (a deploy follow-up still needs an end). Gives one sentence (without the field name) when it is not.
    /// </summary>
    public static string? ValidateFollowUpCondition(string? condition, bool hasFollowUpAt, out string? normalized)
    {
        normalized = null;
        if (string.IsNullOrWhiteSpace(condition))
            return null;
        var text = condition.Trim();
        const string prefix = "module:";
        if (!text.StartsWith(prefix, StringComparison.Ordinal))
            return "not a condition; give module:<database>.<schema>.<object>.";
        var parts = text[prefix.Length..].Split('.');
        if (parts.Length != 3 || parts.Any(p => p.Length == 0 || p.Length > MaxModuleNameChars
                || p.IndexOfAny(['[', ']', '\'', '"', ';']) >= 0 || p.Any(char.IsControl)))
            return "not a module; give module:<database>.<schema>.<object> with three names and no brackets or quotes.";
        if (!hasFollowUpAt)
            return "needs follow_up_at too: a deploy follow-up still opens at that time if the module has not changed.";
        normalized = prefix + string.Join('.', parts);
        return null;
    }

    private static string? Accept(DateTime utc, out DateTime? followUpAtUtc)
    {
        followUpAtUtc = utc;
        return null;
    }

    /// <summary>The sentence a <c>follow_up_at</c> of <paramref name="utc"/> earns, or null when it is after <paramref name="nowUtc"/>
    /// and within <see cref="MaxFollowUpDays"/> days of it.</summary>
    public static string? FollowUpProblem(DateTime utc, DateTime nowUtc)
    {
        if (utc <= nowUtc)
            return "must be after now.";
        if (utc > nowUtc.AddDays(MaxFollowUpDays))
            return $"is more than {MaxFollowUpDays} days ahead.";
        return null;
    }

    /// <summary>The evidence item the agent checked against collected data.</summary>
    public const string BasisVerified = "verified";

    /// <summary>The evidence item the agent reasoned to without checking it.</summary>
    public const string BasisInferred = "inferred";

    /// <summary>The bases as a tool description writes them: <c>verified|inferred</c>. A const, built from the two above, so
    /// the tool description and the parameter description share one spelling (#3684).</summary>
    public const string BasesAlternation = BasisVerified + "|" + BasisInferred;

    /// <summary>The bases an evidence item may carry.</summary>
    public static IReadOnlyList<string> Bases { get; } = new[] { BasisVerified, BasisInferred };

    /// <summary>The bases as a refusal names them: <c>verified or inferred</c>.</summary>
    public static string BasesText { get; } = string.Join(" or ", Bases);

    /// <summary>
    /// Parses the <c>family_of</c> argument: null or blank is none, and a UUID gives <paramref name="familyOf"/>. Anything else
    /// gives one sentence (without the field name). Whether the id names a request on the store is the caller's check, after
    /// this one. Darling and Lite both call this, so the refusal is one sentence in one place.
    /// </summary>
    public static string? ValidateFamilyOf(string? familyOfText, out Guid? familyOf)
    {
        familyOf = null;
        if (string.IsNullOrWhiteSpace(familyOfText))
            return null;
        if (Guid.TryParse(familyOfText, out var parsed))
        {
            familyOf = parsed;
            return null;
        }

        /* An agent that passes the short form of a request id (its first 8 hex digits) is told the full one is needed,
           rather than only that the value is not an id. */
        return "not a request id. Give the full request_id, a UUID, not a short form such as its first 8 characters.";
    }

    /// <summary>
    /// Parses the <c>queries_run</c> argument: null, blank, <c>[]</c> or <c>null</c> is none. A JSON array of strings gives
    /// <paramref name="queriesRunJson"/> normalized; anything else gives one sentence (without the field name). The caps
    /// are <see cref="CapProblems"/>'s, never a clamp.
    /// </summary>
    public static string? Validate(string? queriesRun, out string? queriesRunJson)
    {
        queriesRunJson = null;
        if (string.IsNullOrWhiteSpace(queriesRun) || queriesRun.Trim() is "[]" or "null")
            return null;

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(queriesRun);
        }
        catch (JsonException ex)
        {
            return "not valid JSON: " + ex.Message;
        }

        if (root is not JsonArray items)
            return "must be a JSON array of statement strings.";
        for (var i = 0; i < items.Count; i++)
        {
            if (items[i] is not JsonValue v || v.GetValueKind() != JsonValueKind.String)
                return $"item {i} is not a string.";
        }

        queriesRunJson = items.ToJsonString();
        return null;
    }

    /// <summary>The caps a <c>queries_run</c> array is over, each as <c>queries_run: sentence</c>: more than
    /// <see cref="MaxQueriesRun"/> items, or an item over <see cref="MaxQueryChars"/> characters. Text that is not an array
    /// of strings is <see cref="Validate"/>'s to refuse. Characters are UTF-16 units, never fewer than PostgreSQL's code
    /// points, so an array these caps pass also passes V193's check.</summary>
    public static List<(int Cap, string Problem)> CapProblems(string? queriesRun)
    {
        var problems = new List<(int Cap, string Problem)>();
        if (Validate(queriesRun, out var json) is not null || json is null)
            return problems;
        var items = (JsonArray)JsonNode.Parse(json)!;
        if (items.Count > MaxQueriesRun)
            problems.Add((MaxQueriesRun, $"queries_run: {items.Count} items, over {MaxQueriesRun}."));
        for (var i = 0; i < items.Count; i++)
        {
            var length = items[i]!.GetValue<string>().Length;
            if (length > MaxQueryChars)
                problems.Add((MaxQueryChars, $"queries_run: item {i} is {length} characters, over {MaxQueryChars}."));
        }

        return problems;
    }

    /// <summary>A store's own check of a submission's <c>queries_run</c>, the tool layer's rule again: a caller that skipped
    /// the tool's validation gets an <see cref="ArgumentException"/> before anything is written.</summary>
    public static void ThrowIfInvalid(string? queriesRunJson, string paramName)
    {
        if (queriesRunJson is null)
            return;
        if (Validate(queriesRunJson, out _) is { } problem)
            throw new ArgumentException("queries_run: " + problem, paramName);
        if (CapProblems(queriesRunJson) is { Count: > 0 } caps)
            throw new ArgumentException(caps[0].Problem, paramName);
    }
}
