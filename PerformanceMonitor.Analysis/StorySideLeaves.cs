/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text;
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
    /// The root's frozen StoryText carries that value-stated reading too — <see cref="Sentence"/> and
    /// <see cref="RemediationSentence"/> compose the lever where the facts live (#4730) — for the surfaces that
    /// never render this array.</para>
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
    /// The sentences a story's investigation gains when a lever hangs off it, one per lever — "A configuration lever
    /// hangs off this story: `CONFIG_PG_MAINT_WORK_MEM` — maintenance_work_mem is being tested by public.hot's 5,250
    /// dead tuples." — with a leading space so they append to the existing prose the way the named-hop, recurrence and
    /// fold sentences do. The text after the dash is the lever's own composed headline, read from the FULL fact set in
    /// <paramref name="factsByKey"/> the way the root's values are (#4730). A lever that composes to no advice gets its
    /// key named and nothing after it; the sentence never points at a card.
    ///
    /// <para><b>Why the advice travels here and not on a card.</b> The walk consumed the lever, so it has no card of
    /// its own, and only <c>analyze_server</c> renders the <c>side_leaves</c> array (<see cref="ToPayload"/>).
    /// <c>get_analysis_findings</c>, the viewer and the e-mail render the frozen StoryText alone, so the lever's
    /// headline is here and its remediation is in <see cref="RemediationSentence"/>, both in the text that
    /// persists. The lever's fix is the lever's, in its own family's words: it is appended to the root's
    /// remediation under the lever's key rather than restated.</para>
    ///
    /// <para>Returns null when there is no lever, which is the byte-identity arm.</para>
    /// </summary>
    public static string? Sentence(IReadOnlyList<string>? sideLeafKeys, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        if (sideLeafKeys is null || sideLeafKeys.Count == 0)
            return null;
        var sentences = new StringBuilder();
        foreach (var key in sideLeafKeys)
        {
            var headline = FactAdvice.Compose(key, factsByKey)?.Headline?.Trim();
            var body = string.IsNullOrEmpty(headline) ? $"`{key}`" : $"`{key}` — {headline}";
            sentences.Append(' ').Append(SentenceMarker).Append(' ').Append(body);
            if (!EndsWithTerminator(body))
                sentences.Append('.');
        }
        return sentences.ToString();
    }

    /// <summary>
    /// The clauses a story's remediation gains when a lever hangs off it, one per lever that composes to a
    /// remediation — " For `CONFIG_PG_MAINT_WORK_MEM`: Raise autovacuum_work_mem (reload, not restart) …" — with a
    /// leading space so they append to the root's remediation. The text after the colon is the lever's own composed
    /// remediation, unchanged, introduced by its key so the reader can tell whose fix it is (#4730). Before this the
    /// remediation stayed on the lever's card, which only <c>analyze_server</c> renders; it now travels in StoryText
    /// with the headline in <see cref="Sentence"/>. Returns null when no lever composes to a remediation — no
    /// lever, or levers with no advice — so the root's remediation keeps its bytes.
    /// </summary>
    public static string? RemediationSentence(IReadOnlyList<string>? sideLeafKeys, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        if (sideLeafKeys is null || sideLeafKeys.Count == 0)
            return null;
        StringBuilder? clauses = null;
        foreach (var key in sideLeafKeys)
        {
            var remediation = FactAdvice.Compose(key, factsByKey)?.Remediation?.Trim();
            if (string.IsNullOrEmpty(remediation))
                continue;
            clauses ??= new StringBuilder();
            clauses.Append(" For `").Append(key).Append("`: ").Append(remediation);
            if (!EndsWithTerminator(remediation))
                clauses.Append('.');
        }
        return clauses?.ToString();
    }

    private static bool EndsWithTerminator(string text) =>
        text.Length > 0 && (text[^1] == '.' || text[^1] == '!' || text[^1] == '?');
}
