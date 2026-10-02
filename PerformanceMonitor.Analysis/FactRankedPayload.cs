/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// How a fact's <see cref="Fact.Ranked"/> list reaches an MCP caller (#3691 lane 43) — the payload half of the
/// seam whose model half is <see cref="RankedObject"/>. Split out of <c>AnalysisModels.cs</c> so the models file
/// stays models: this half knows about <see cref="JsonObject"/> and the tools' serializer options, which is the
/// same reason <see cref="StorySideLeaves"/> is its own file rather than a method on <c>AnalysisStory</c>.
///
/// <para><b>Two or more, or nothing at all.</b> A fact that ranks ONE object says everything it has to say in
/// <see cref="Fact.ObjectName"/> and <see cref="Fact.Value"/>, which every payload already carries — a
/// <c>ranked</c> array of one would be a second spelling of the subject, and every card in the product would
/// grow a property for it. So <see cref="ToPayload"/> returns null below two entries and
/// <see cref="Attach"/> then hands back the caller's own payload object, unserialized and untouched.</para>
///
/// <para><b>Why attach rather than a property.</b> The tools' options
/// (<c>McpHelpers.JsonOptions</c>) do NOT ignore nulls — the payload's <c>leaf_fact</c> renders as
/// <c>null</c> on a one-node story, which is the proof — so <c>ranked = ToPayload(...)</c> spelled as an
/// anonymous-object property would write <c>"ranked": null</c> onto every card of every pass on both engines,
/// for a thing that did not happen. The SQL Server exit checks pin those bytes. This follows
/// <see cref="StorySideLeaves.Attach"/> and <c>CollectionCaveats.Attach</c> exactly, deliberately: a third way
/// of doing the same thing is how three renderers drift apart.</para>
/// </summary>
public static partial class FactRanked
{
    /// <summary>
    /// The <c>ranked</c> array — <c>[{ object_name, database_name, value, figures }]</c>, worst first, the
    /// collector's own rank order — or null when there are fewer than two objects to rank (see this class's
    /// summary for why null and not an empty array).
    ///
    /// <para><b>Entry [0] is the subject, and it is emitted.</b> It repeats <c>ObjectName</c> / <c>value</c>
    /// that the enclosing payload already carries, on purpose: a client that reads <c>ranked</c> as "the objects
    /// this card is about, ranked" must not have to prepend the subject itself to get a list, and a list whose
    /// first element is silently elsewhere is the kind of shape that produces off-by-one client code. The
    /// invariant (<see cref="RankedObject"/>) is what makes the repetition safe — the two cannot disagree.</para>
    ///
    /// <para><see cref="RankedObject.Figures"/> passes through as the family's own un-prefixed metadata names
    /// with no rounding: these are the numbers the card's prose was composed from, and a caller comparing the
    /// two should not have to account for two different roundings. The enclosing payloads round their own
    /// scalars where they always did.</para>
    /// </summary>
    public static object? ToPayload(IReadOnlyList<RankedObject>? ranked)
    {
        if (ranked is null || ranked.Count < 2)
            return null;

        var entries = new List<object>(ranked.Count);
        foreach (var o in ranked)
        {
            entries.Add(new
            {
                object_name = o.ObjectName,
                database_name = o.DatabaseName,
                value = o.Value,
                figures = o.Figures,
            });
        }

        return entries;
    }

    /// <summary>
    /// Adds <c>ranked</c> to one card's payload object ONLY when the fact ranks two or more objects. With fewer
    /// the very same <paramref name="payload"/> reference comes back, so a card this does not concern is
    /// byte-for-byte what it was; with more, the payload is serialized to a <see cref="JsonObject"/> with the
    /// caller's options and the array is appended as the LAST property.
    /// </summary>
    public static object Attach(object payload, IReadOnlyList<RankedObject>? ranked, JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(payload);
        var block = ToPayload(ranked);
        if (block is null) return payload;

        var node = JsonSerializer.SerializeToNode(payload, options)?.AsObject()
            ?? throw new InvalidOperationException("the ranked payload's card did not serialize to a JSON object");
        node["ranked"] = JsonSerializer.SerializeToNode(block, options);
        return node;
    }
}
