/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Turns <c>auto_explain</c>'s log output into stored plan rows: extract, redact, hash (#2538).
///
/// <para><b>Why this is its own type rather than living in the collector.</b> There are two ways a plan
/// reaches this product and there will not be a third: <c>pg_read_file</c> against a self-hosted server's
/// log, and the RDS <c>DownloadDBLogFilePortion</c> API for managed PostgreSQL, which has no filesystem to
/// read. Those are different transports for identical text.</para>
///
/// <para><b>The redaction MUST NOT be duplicated across them.</b> Every other divergence in this codebase
/// has cost a wrong number; this one would cost a customer's data. One implementation, two callers, and a
/// test suite that does not care which transport is asking.</para>
///
/// <para><b>What redaction does, and the asymmetry that matters.</b> <c>Query Text</c> is removed outright —
/// <c>auto_explain</c> emits the statement verbatim and <c>log_parameter_max_length = 0</c> does not
/// suppress it, because that setting covers bind parameters only. Quoted literals are then stripped from
/// EVERY remaining string, which is safe because relation and alias names are not quoted. Bare numbers are
/// stripped only inside condition fields: a blanket numeric strip would rewrite a relation genuinely named
/// <c>transactionitems1</c> into <c>transactionitems?</c>, destroying identity to hide a value that was
/// never there.</para>
/// </summary>
public static class PgPlanLogParser
{
    /// <param name="PlanHash">Of the REDACTED plan, so one shape recurs to one hash whatever values it ran
    /// with. Hashing the raw text would defeat dedup exactly where it matters most.</param>
    public readonly record struct ParsedPlan(
        long QueryId,
        double DurationMs,
        string PlanHash,
        int NodeCount,
        string? TopNodeType,
        string PlanJson);

    /* The log line prefix carries the query id via %Q, which is the ONLY place auto_explain exposes it -
       the plan JSON has no identifier of its own even with compute_query_id on. The JSON block follows,
       tab-indented, which is what makes it recognisable as a unit rather than needing a brace counter. */
    private static readonly Regex s_planBlock = new(
        @"\[\d+\] (-?\d+) LOG:  duration: ([0-9.]+) ms  plan:\s*\n((?:\t[^\n]*\n)+)",
        RegexOptions.Compiled);

    /* Condition fields, where a bare number is a VALUE rather than part of a name. Enumerated rather than
       inferred: wrong in the safe direction leaves a number in a filter, wrong the other way rewrites an
       object's name. */
    private static readonly HashSet<string> s_conditionFields = new(StringComparer.Ordinal)
    {
        "Filter", "Index Cond", "Recheck Cond", "Join Filter", "Hash Cond", "Merge Cond",
        "TID Cond", "One-Time Filter", "Cache Key", "Function Call", "Output", "Group Key",
        "Sort Key", "Presorted Key", "Hash Key", "Conflict Filter", "Repeatable Seed",
    };

    private static readonly Regex s_quotedLiteral = new("'(?:[^']|'')*'", RegexOptions.Compiled);

    /* Bare numbers NOT glued to an identifier character, so 'transactionitems1' survives and '(id > 100)'
       does not. */
    private static readonly Regex s_bareNumber = new(
        @"(?<![A-Za-z0-9_])\d+(?:\.\d+)?(?![A-Za-z0-9_])", RegexOptions.Compiled);

    /// <summary>
    /// Every plan in a raw slab of server log. Used by the transport that receives log TEXT — the RDS API —
    /// where no SQL ran to pick the blocks out first.
    ///
    /// <para>Blocks that will not parse are skipped rather than reported. Both transports read a bounded
    /// window, so a block cut in half at the edge is an ordinary consequence of not reading the whole file
    /// and not a fault worth surfacing every cycle.</para>
    /// </summary>
    public static List<ParsedPlan> Extract(string? logBody)
    {
        var plans = new List<ParsedPlan>();

        if (string.IsNullOrEmpty(logBody))
        {
            return plans;
        }

        foreach (Match match in s_planBlock.Matches(logBody))
        {
            if (!long.TryParse(match.Groups[1].Value, out var queryId)
                || !double.TryParse(match.Groups[2].Value,
                       System.Globalization.NumberStyles.Float,
                       System.Globalization.CultureInfo.InvariantCulture,
                       out var durationMs))
            {
                continue;
            }

            var parsed = FromBlock(queryId, durationMs, match.Groups[3].Value.Replace("\t", string.Empty));

            if (parsed is not null)
            {
                plans.Add(parsed.Value);
            }
        }

        return plans;
    }

    /// <summary>
    /// One already-isolated plan block. Used by the transport whose SQL already split the log — the
    /// <c>pg_read_file</c> path, where <c>regexp_matches</c> did the extraction server-side so the whole log
    /// never crosses the wire.
    /// </summary>
    public static ParsedPlan? FromBlock(long queryId, double durationMs, string? planJson)
    {
        if (string.IsNullOrWhiteSpace(planJson))
        {
            return null;
        }

        JsonNode? parsed;

        try
        {
            parsed = JsonNode.Parse(planJson);
        }
        catch (JsonException)
        {
            return null;
        }

        if (parsed is not JsonObject root || root["Plan"] is not JsonObject plan)
        {
            return null;
        }

        /* Removed BEFORE anything else touches the tree, so no later step can carry it by accident. */
        root.Remove("Query Text");

        Redact(plan);

        var json = root.ToJsonString();

        return new ParsedPlan(
            QueryId: queryId,
            DurationMs: durationMs,
            PlanHash: Hash(json),
            NodeCount: CountNodes(plan),
            TopNodeType: plan["Node Type"]?.GetValue<string>(),
            PlanJson: json);
    }

    /// <summary>Strips values from a plan tree in place. See the type header for the asymmetry.</summary>
    internal static void Redact(JsonObject node)
    {
        foreach (var property in node.ToList())
        {
            switch (property.Value)
            {
                case JsonObject child:
                    Redact(child);
                    break;

                case JsonArray array:
                    foreach (var element in array)
                    {
                        if (element is JsonObject arrayChild)
                        {
                            Redact(arrayChild);
                        }
                    }

                    /* Arrays of STRINGS carry values too — Output and the key lists are arrays. */
                    for (var i = 0; i < array.Count; i++)
                    {
                        if (array[i] is JsonValue value && value.TryGetValue<string>(out var text))
                        {
                            array[i] = JsonValue.Create(Scrub(text, property.Key));
                        }
                    }

                    break;

                case JsonValue value when value.TryGetValue<string>(out var text):
                    node[property.Key] = JsonValue.Create(Scrub(text, property.Key));
                    break;
            }
        }
    }

    private static string Scrub(string text, string fieldName)
    {
        var scrubbed = s_quotedLiteral.Replace(text, "'?'");

        if (s_conditionFields.Contains(fieldName))
        {
            scrubbed = s_bareNumber.Replace(scrubbed, "?");
        }

        return scrubbed;
    }

    private static int CountNodes(JsonObject plan)
    {
        var count = 1;

        if (plan["Plans"] is JsonArray children)
        {
            foreach (var child in children)
            {
                if (child is JsonObject childPlan)
                {
                    count += CountNodes(childPlan);
                }
            }
        }

        return count;
    }

    private static string Hash(string json)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(json));
        return Convert.ToHexString(bytes, 0, 16);
    }
}
