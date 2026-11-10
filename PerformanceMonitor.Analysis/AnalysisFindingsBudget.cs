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
using System.Text;
using System.Text.Json.Nodes;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Analysis;

/// <summary>One finding the reply left out, by the two things a reader needs to recognize it: its category and its headline.</summary>
public sealed record FindingLabel(string Category, string Headline);

/// <summary>
/// What a packed reply carries: the findings it holds (in severity order, trimmed ones flagged on the finding itself),
/// the ones it left out, and how many it trimmed. <see cref="FindingsArray"/> hands out a fresh copy each call because a
/// <see cref="JsonNode"/> can sit under one parent only and the packer measures many candidate replies.
/// </summary>
public sealed class PackedFindings
{
    private readonly IReadOnlyList<JsonNode> _kept;

    internal PackedFindings(IReadOnlyList<JsonNode> kept, IReadOnlyList<FindingLabel> leftOut, int trimmedCount)
    {
        _kept = kept;
        LeftOut = leftOut;
        TrimmedCount = trimmedCount;
    }

    public int KeptCount => _kept.Count;

    public IReadOnlyList<FindingLabel> LeftOut { get; }

    public int TrimmedCount { get; }

    public JsonArray FindingsArray()
    {
        var array = new JsonArray();
        foreach (var node in _kept)
        {
            array.Add(node.DeepClone());
        }

        return array;
    }

    /// <summary>Adds <c>findings_left_out</c> as the LAST property when anything was left out, so a reply with nothing left out keeps the bytes it always had.</summary>
    public JsonObject Annotate(JsonObject reply)
    {
        ArgumentNullException.ThrowIfNull(reply);
        if (LeftOut.Count > 0)
        {
            var entries = new JsonArray();
            foreach (var label in LeftOut)
            {
                entries.Add(new JsonObject { ["category"] = label.Category, ["headline"] = label.Headline });
            }

            reply["findings_left_out"] = entries;
        }

        return reply;
    }
}

/// <summary>
/// #8081, #8569: the one packer and the one sentence both products' <c>analyze_server</c> use when the findings did not
/// all fit <see cref="McpResponseBudget.DefaultBytes"/>. The findings ride severity-first. #8081 cut the reply at the first
/// finding that did not fit, so one large second finding left out every smaller one behind it, however much room was
/// left (one reply of 7,892 bytes said only 1 of 7 findings fit the 32,768-byte budget). The packer now keeps going past a
/// finding that does not fit, so every smaller finding that fits is returned, still in severity order. A finding that is
/// bigger than the whole budget on its own is trimmed rather than dropped: its largest parts (drill-down rows, remediation
/// SQL) are set to null, largest first, until it fits what is left, and the finding carries <c>trimmed</c> naming what was
/// cut. Only a finding that does not fit even trimmed is dropped. Every finding left out is listed, by category and
/// headline, in <c>findings_left_out</c>. Shared so the two tools cannot drift on the wording or the order.
/// </summary>
public static class AnalysisFindingsBudget
{
    /// <summary>The finding fields that carry bulk and can be cut without losing what the finding is: the drill-down rows, the
    /// remediation commands and their projections, the risk disclosure and the side-lever cards. Cut largest first.</summary>
    private static readonly string[] TrimmableFields =
    [
        "drill_down", "suggested_remediation_sql", "remediation_command", "structured_remediation",
        "destructive_risk_disclosure", "side_leaves", "next_tools",
    ];

    /// <summary>
    /// Packs <paramref name="findings"/> (serialized, severity-first, one label each) into the largest reply that fits
    /// <paramref name="budgetBytes"/>. <paramref name="build"/> writes the whole reply for a candidate set and measures
    /// nothing itself; the packer measures it. While measuring a candidate, every finding not in it counts as left out,
    /// so the final reply (which lists only the ones actually left out) is never larger than the last one measured.
    /// </summary>
    public static (PackedFindings Packed, string Json) Pack(
        IReadOnlyList<JsonNode> findings,
        IReadOnlyList<FindingLabel> labels,
        Func<PackedFindings, string> build,
        int budgetBytes = McpResponseBudget.DefaultBytes)
    {
        ArgumentNullException.ThrowIfNull(findings);
        ArgumentNullException.ThrowIfNull(labels);
        ArgumentNullException.ThrowIfNull(build);

        bool Fits(PackedFindings candidate) => Encoding.UTF8.GetByteCount(build(candidate)) <= budgetBytes;

        PackedFindings Candidate(IReadOnlyList<JsonNode> nodes, ISet<int> keptSet, int trimmed)
        {
            var left = new List<FindingLabel>();
            for (var j = 0; j < labels.Count; j++)
            {
                if (!keptSet.Contains(j))
                {
                    left.Add(labels[j]);
                }
            }

            return new PackedFindings(nodes, left, trimmed);
        }

        var whole = new PackedFindings(findings, [], 0);
        var wholeJson = build(whole);
        if (Encoding.UTF8.GetByteCount(wholeJson) <= budgetBytes)
        {
            return (whole, wholeJson);
        }

        var kept = new List<JsonNode>();
        var keptIndexes = new HashSet<int>();
        var trimmedCount = 0;

        for (var i = 0; i < findings.Count; i++)
        {
            var withThis = new List<JsonNode>(kept) { findings[i] };
            var withIndexes = new HashSet<int>(keptIndexes) { i };
            if (Fits(Candidate(withThis, withIndexes, trimmedCount)))
            {
                kept = withThis;
                keptIndexes = withIndexes;
                continue;
            }

            /* Does not fit beside what is already kept. One that would fit a reply of its own is left out (and listed): a
               smaller finding behind it may still fit. One that is over the whole budget even alone is trimmed to what is left. */
            if (Fits(Candidate([findings[i]], new HashSet<int> { i }, 0)))
            {
                continue;
            }

            var trimmed = TrimToFit(findings[i], node =>
            {
                var attempt = new List<JsonNode>(kept) { node };
                return Fits(Candidate(attempt, withIndexes, trimmedCount + 1));
            });
            if (trimmed is not null)
            {
                kept.Add(trimmed);
                keptIndexes = withIndexes;
                trimmedCount++;
            }
        }

        var packed = Candidate(kept, keptIndexes, trimmedCount);
        return (packed, build(packed));
    }

    /// <summary>Cuts the finding's bulky fields, largest first, until <paramref name="fits"/> says the reply holds; null when even
    /// every cut does not make it fit. Works on a copy and flags the copy with <c>trimmed</c>.</summary>
    private static JsonObject? TrimToFit(JsonNode finding, Func<JsonNode, bool> fits)
    {
        if (finding is not JsonObject original)
        {
            return null;
        }

        var copy = (JsonObject)original.DeepClone();
        var candidates = new List<(string Field, int Bytes)>();
        foreach (var field in TrimmableFields)
        {
            if (copy[field] is { } value)
            {
                candidates.Add((field, Encoding.UTF8.GetByteCount(value.ToJsonString())));
            }
        }

        var cut = new List<(string Field, int Bytes)>();
        foreach (var (field, bytes) in candidates.OrderByDescending(c => c.Bytes))
        {
            copy[field] = null;
            cut.Add((field, bytes));
            Flag(copy, cut);
            if (fits(copy))
            {
                return copy;
            }
        }

        return null;
    }

    private static void Flag(JsonObject finding, List<(string Field, int Bytes)> cut)
    {
        var fields = new JsonArray();
        foreach (var (field, bytes) in cut)
        {
            fields.Add(new JsonObject { ["field"] = field, ["bytes"] = bytes });
        }

        finding["trimmed"] = new JsonObject
        {
            ["fields"] = fields,
            ["note"] = "This finding alone was larger than the " + McpResponseBudget.DefaultBytes.ToString("#,0", CultureInfo.InvariantCulture)
                + "-byte response budget, so the listed fields (with the bytes each held) were set to null. Everything else on it is as analyzed.",
        };
    }

    /// <summary>Null when every finding rides whole (<paramref name="packed"/> left none out and trimmed none).
    /// <paramref name="persisted"/> is the reply's own <c>persisted</c> flag: true on an unanchored run.</summary>
    public static string? TruncationNote(int findingCount, PackedFindings packed, bool persisted)
    {
        ArgumentNullException.ThrowIfNull(packed);
        if (packed.LeftOut.Count == 0 && packed.TrimmedCount == 0)
        {
            return null;
        }

        var note = new StringBuilder("TRUNCATED: ");
        note.Append(CultureInfo.InvariantCulture, $"{findingCount} findings fired and {packed.KeptCount} ride in this reply, in severity order");
        if (packed.LeftOut.Count > 0)
        {
            note.Append(CultureInfo.InvariantCulture, $"; {packed.LeftOut.Count} did not fit in the room left under the {McpResponseBudget.DefaultBytes:#,0}-byte response budget and {(packed.LeftOut.Count == 1 ? "is" : "are")} named in findings_left_out");
        }

        if (packed.TrimmedCount > 0)
        {
            note.Append(CultureInfo.InvariantCulture, $"; {packed.TrimmedCount} {(packed.TrimmedCount == 1 ? "was" : "were")} larger than the whole budget and {(packed.TrimmedCount == 1 ? "was" : "were")} trimmed (the finding's trimmed field names what was cut)");
        }

        note.Append(". A smaller finding behind a larger one is still returned. finding_count counts every finding.");
        if (packed.LeftOut.Count == 0)
        {
            return note.ToString();
        }

        return persisted
            ? note + " This run persisted them all: read the rest with get_analysis_findings (limit pages the persisted findings, still-firing first then by severity; start at 5), or narrow hours_back."
            : note + " This run was anchored with as_of, so its findings were not persisted and the ones left off are not in the store: narrow hours_back (as_of still ends the window) to see them, or run without as_of to analyze and persist the present.";
    }
}
