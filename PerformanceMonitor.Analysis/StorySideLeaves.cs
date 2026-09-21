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

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The payload and prose shape of a story's SIDE LEAVES — the config-advisory facts hanging off a node on the
/// story's path by an ACTIVE edge the greedy walk did not follow (#3691, lane 42). The sweep that finds them is
/// <c>InferenceEngine.SweepSideLeaves</c>; this class is the two places they reach a reader, kept together and in
/// the shared assembly so both engines' <c>analyze_server</c> renderers and both SKUs' advice composition say the
/// same thing in the same words.
///
/// <para><b>The defect, for the reader of this file.</b> The traversal takes the single highest-severity active
/// edge per node, so a mid-path node with two active edges reaches only one. The vacuum backlog reaches the
/// wraparound trend (and the hold chain behind it) and skips <c>CONFIG_PG_MAINT_WORK_MEM</c> — the memory bound on
/// how much of the backlog one autovacuum pass can clear, which is the lever that would relieve the very thing the
/// story is about. Un-visited meant un-consumed, and a config-advisory key roots at any positive severity, so the
/// lever then rooted its own one-node card at 0.6 beside the incident it belongs to: two cards where the truth is
/// one story with a lever.</para>
///
/// <para><b>Emitted only when there is something to emit.</b> The tools' serializer options write nulls (the
/// payload's <c>leaf_fact</c> is proof — it renders as <c>null</c> on a one-node story), so a
/// <c>side_leaves</c> property set to null would be a byte change on EVERY finding of every clean pass, for a
/// thing that did not happen — and the SQL Server exit checks pin those bytes. <see cref="Attach"/> therefore
/// follows <see cref="CollectionCaveats.Attach"/> exactly: with no lever it returns the caller's payload object
/// untouched, through the serializer call the caller always made; with levers it appends the array as the last
/// property of a <see cref="JsonObject"/>. Likewise <see cref="Sentence"/> returns null when there is no lever,
/// so a story this does not concern keeps the advice it had, byte for byte.</para>
/// </summary>
public static class StorySideLeaves
{
    /// <summary>The phrase the composed sentence opens with — one spelling, so a test can find the sentence and a
    /// reader recognises the shape across engines (the <see cref="FactIdentity.SentenceMarker"/> pattern).</summary>
    public const string SentenceMarker = "A configuration lever hangs off this story:";

    /// <summary>
    /// The <c>side_leaves</c> array: one card per lever, the same <c>key</c> + <c>advice</c> shape the finding's
    /// own card carries, so a reader who is told a lever hangs off this story does not have to call a second tool
    /// to learn what the lever says. Null (not an empty array) when there is no lever, so a caller writes the
    /// property only when it is owed — see this class's summary for why that matters to the byte-identity pins.
    ///
    /// <para><b>Why the advice is the STATIC block and there is no <c>value</c>.</b> A finding does not carry the
    /// lever's fact: the scored fact set lives in the analysis pass, and <c>analyze_server</c> renders findings,
    /// which is why <see cref="AnalysisFinding.RootFactValue"/> exists as a copied scalar at all. Composing a
    /// value-stated block here would mean re-reading the facts on the render path, and inventing a value from the
    /// key would be worse than omitting one. So the lever's card states the shared advice for its key
    /// (<see cref="FactAdvice.GetForFactKey"/> — which routes the <c>CONFIG_PG_*</c> vocabulary to
    /// <c>PgTargetAdvice</c>, so both engines are served by this one call), and the VALUE-stated reading of the
    /// same knob stays where it always was: <c>get_analysis_facts</c>, which returns the fact with its metadata.
    /// The root card's sentence (<see cref="Sentence"/>) is what points the reader at both.</para>
    /// </summary>
    public static object? ToPayload(IReadOnlyList<string>? sideLeafKeys)
    {
        if (sideLeafKeys is null || sideLeafKeys.Count == 0)
            return null;

        var cards = new List<object>(sideLeafKeys.Count);
        foreach (var key in sideLeafKeys)
        {
            var advice = FactAdvice.GetForFactKey(key);
            cards.Add(new
            {
                key,
                advice = advice is null ? null : new
                {
                    headline = advice.Headline,
                    investigation = advice.Investigation,
                    remediation = advice.Remediation
                }
            });
        }

        return cards;
    }

    /// <summary>
    /// Adds <c>side_leaves</c> to one finding's payload object ONLY when a lever hangs off it. With none, the very
    /// same <paramref name="payload"/> reference comes back, so a clean pass's bytes cannot move (see this class's
    /// summary). With levers the payload is serialized to a <see cref="JsonObject"/> with the caller's options and
    /// the array is appended as the LAST property — the <see cref="CollectionCaveats.Attach"/> shape, deliberately,
    /// because a second way of doing the same thing is how two renderers drift apart.
    /// </summary>
    public static object Attach(object payload, IReadOnlyList<string>? sideLeafKeys, JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(payload);
        var block = ToPayload(sideLeafKeys);
        if (block is null) return payload;

        var node = JsonSerializer.SerializeToNode(payload, options)?.AsObject()
            ?? throw new InvalidOperationException("the finding payload did not serialize to a JSON object");
        node["side_leaves"] = JsonSerializer.SerializeToNode(block, options);
        return node;
    }

    /// <summary>
    /// The ONE sentence a story's investigation gains when a lever hangs off it — "A configuration lever hangs off
    /// this story: `CONFIG_PG_MAINT_WORK_MEM` — see its card." — with a leading space so it appends to the existing
    /// prose the way the named-hop, recurrence and fold sentences do. One sentence however many levers there are:
    /// the levers' own cards carry their values and their remediation, and this is the pointer to them, not a
    /// second copy. Returns null when there is no lever, which is the byte-identity arm.
    /// </summary>
    public static string? Sentence(IReadOnlyList<string>? sideLeafKeys)
    {
        if (sideLeafKeys is null || sideLeafKeys.Count == 0)
            return null;
        var keys = string.Join(", ", sideLeafKeys.Select(k => $"`{k}`"));
        var cards = sideLeafKeys.Count == 1 ? "see its card" : "see their cards";
        return $" {SentenceMarker} {keys} — {cards}.";
    }
}
