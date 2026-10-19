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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3691 lane 43: the <see cref="Fact.Ranked"/> seam's own pins — the invariant every ranking collector owes,
/// the payload's two-or-more rule, and the parity between the two SKUs' tool files. The three migrated families
/// assert their own CONTENT in their own classes (<c>PgTargetVacuumTests</c>, <c>PgTargetGrowthTests</c>,
/// <c>PgTargetBloatTests</c>, and the live twins against a real PostgreSQL); what lives here is the part that is
/// true of every family and must not be re-argued per family.
///
/// <para><b>Why the invariant is a public helper and not only a test.</b> <see cref="AssertInvariant"/> is called
/// from those families' pins and from their live tests, over the fixtures they already plant — that is the census
/// the STEP asked for, and it runs where the facts are actually built by collector code reading a real read's
/// rows. A reflection walk over every collector would have to invent fixtures for families that never rank, and
/// would pass vacuously on the three that do.</para>
/// </summary>
public sealed class FactRankedTests
{
    /// <summary>
    /// The invariant, in one place (#3691 lane 43): at most <see cref="FactRanked.MaxObjects"/> entries; entry
    /// [0] IS the fact's own subject, by name and by value; every name distinct and non-empty. Called by every
    /// family's ranked pin, unit and live, so a collector that stamps a ranked list out of order, twice, or
    /// without its own subject in front fails in that family's own test rather than in a distant census.
    ///
    /// <para>The value comparison is 1e-9 rather than exact because the subject's value and the ranked entry's
    /// come from the same read column through two <c>Convert.ToDouble</c> paths; the names are compared
    /// ordinally, because a schema-qualified PostgreSQL name is bytes, not a locale.</para>
    /// </summary>
    internal static void AssertInvariant(Fact fact)
    {
        ArgumentNullException.ThrowIfNull(fact);
        if (fact.Ranked.Count == 0)
            return;

        Assert.InRange(fact.Ranked.Count, 1, FactRanked.MaxObjects);
        Assert.Equal(fact.ObjectName, fact.Ranked[0].ObjectName);
        Assert.Equal(fact.Value, fact.Ranked[0].Value, precision: 9);
        Assert.All(fact.Ranked, o => Assert.False(string.IsNullOrEmpty(o.ObjectName), $"{fact.Key}: a ranked entry with no name"));
        Assert.Equal(
            fact.Ranked.Count,
            fact.Ranked.Select(o => o.ObjectName).Distinct(StringComparer.Ordinal).Count());
    }

    /// <summary>The invariant helper itself catches each way a fixture or a collector can get it wrong — an
    /// over-long list, a subject that is not entry [0], a value that disagrees with its own subject's, a
    /// duplicate name, an unnamed entry. Without this, a family's pin could call
    /// <see cref="AssertInvariant"/> and be pinning nothing.</summary>
    [Fact]
    public void TheInvariant_RejectsEveryWayARankedListCanContradictItsOwnFact()
    {
        Assert.Null(Record.Exception(() => AssertInvariant(Ranked("public.hot", 5.0, ("public.hot", 5.0), ("public.orders", 3.1)))));
        /* An empty list is the overwhelming majority of facts and is not a violation: it is the byte-identity arm. */
        Assert.Null(Record.Exception(() => AssertInvariant(new Fact { Key = "X", ObjectName = "public.hot", Value = 5.0 })));

        Assert.NotNull(Record.Exception(() => AssertInvariant(Ranked("public.hot", 5.0,
            ("public.hot", 5.0), ("a", 4.0), ("b", 3.0), ("c", 2.0)))));
        Assert.NotNull(Record.Exception(() => AssertInvariant(Ranked("public.hot", 5.0, ("public.orders", 3.1)))));
        Assert.NotNull(Record.Exception(() => AssertInvariant(Ranked("public.hot", 5.0, ("public.hot", 4.0), ("public.orders", 3.1)))));
        Assert.NotNull(Record.Exception(() => AssertInvariant(Ranked("public.hot", 5.0, ("public.hot", 5.0), ("public.hot", 5.0)))));
        Assert.NotNull(Record.Exception(() => AssertInvariant(Ranked("public.hot", 5.0, ("public.hot", 5.0), ("", 3.1)))));
    }

    /// <summary>
    /// The payload's rule: nothing below two objects, and then the four-field entry shape in the collector's
    /// rank order with the figures passed through unrounded. One object is not a list — its name and value are
    /// already on the enclosing card — and emitting a one-entry array would have put a <c>ranked</c> property on
    /// every object-scoped card in the product.
    /// </summary>
    [Fact]
    public void ThePayload_IsNullUnderTwoObjects_AndOtherwiseCarriesEveryEntryInRankOrderWithItsOwnFigures()
    {
        Assert.Null(FactRanked.ToPayload(null));
        Assert.Null(FactRanked.ToPayload([]));
        Assert.Null(FactRanked.ToPayload([new RankedObject("public.hot", "appdb", 5.0)]));

        var fact = Ranked("public.hot", 5.0, ("public.hot", 5.0), ("public.orders", 3.1), ("sales.ledger", 1.4));
        var json = JsonSerializer.Serialize(FactRanked.ToPayload(fact.Ranked), McpHelpers.JsonOptions);
        Assert.Equal(
            "[{\"object_name\":\"public.hot\",\"database_name\":\"appdb\",\"value\":5,\"figures\":{\"ratio\":5}}," +
            "{\"object_name\":\"public.orders\",\"database_name\":\"appdb\",\"value\":3.1,\"figures\":{\"ratio\":3.1}}," +
            "{\"object_name\":\"sales.ledger\",\"database_name\":\"appdb\",\"value\":1.4,\"figures\":{\"ratio\":1.4}}]",
            json);
    }

    /// <summary>
    /// The byte-identity arm, which is the whole reason this seam attaches instead of declaring a property: a
    /// card whose fact ranks nothing — every SQL Server fact, and every PostgreSQL family other than the three
    /// migrated ones — gets back the very object it passed in, serializes to exactly the bytes it did before the
    /// seam existed, and carries no <c>ranked</c> key at all. The tools' options write nulls
    /// (<c>WriteIndented = false</c> and nothing else), which this also proves: a <c>ranked = null</c> property
    /// would have shown up in these bytes.
    /// </summary>
    [Fact]
    public void ANonRankingCard_GetsItsOwnObjectBack_WithNoRankedKeyAndNoNull()
    {
        var card = new { key = "CONFIG_PG_MAINT_WORK_MEM", value = 67_108_864.0 };
        var before = JsonSerializer.Serialize(card, McpHelpers.JsonOptions);

        foreach (var ranked in new List<RankedObject>?[] { null, [], [new RankedObject("public.hot", "appdb", 5.0)] })
        {
            var attached = FactRanked.Attach(card, ranked, McpHelpers.JsonOptions);
            Assert.Same(card, attached);
            var after = JsonSerializer.Serialize(attached, McpHelpers.JsonOptions);
            Assert.Equal(before, after);
            Assert.DoesNotContain("ranked", after, StringComparison.Ordinal);
        }

        /* And with two, the array is appended LAST, so nothing that was there moved. */
        var withRanked = JsonSerializer.Serialize(
            FactRanked.Attach(card, [new RankedObject("public.hot", "appdb", 5.0), new RankedObject("public.orders", "appdb", 3.1)], McpHelpers.JsonOptions),
            McpHelpers.JsonOptions);
        Assert.StartsWith(before[..^1], withRanked, StringComparison.Ordinal);
        Assert.Contains("\"ranked\":[{\"object_name\":\"public.hot\"", withRanked, StringComparison.Ordinal);
    }

    /// <summary>
    /// Parity between the SKUs, read from source in the <c>CollectionCaveatsParityTests</c> style because no
    /// CI-run test project references both SKUs' assemblies: each tool file attaches the ranked block exactly
    /// once per tool — <c>analyze_server</c> on the finding's root fact, <c>get_analysis_facts</c> on every fact
    /// entry — and NEITHER file ever spells <c>ranked =</c> as a property, which is how a null would reach the
    /// wire on every clean card. Both tools' descriptions state the field and its two-or-more rule, because a
    /// payload field an agent is not told about is a field it will not read.
    /// </summary>
    [Fact]
    public void BothToolFiles_AttachTheRankedBlockOncePerTool_AndNeverSpellItAsAProperty()
    {
        foreach (var (sku, source) in Tools())
        {
            var analyze = ToolBody(source, "analyze_server", "AnalyzeServer", sku);
            var facts = ToolBody(source, "get_analysis_facts", "GetAnalysisFacts", sku);

            /* analyze_server: the LIVE pass's root fact only. The read-back twin (get_analysis_findings) must
               NOT attach — Ranked is ephemeral, so a persisted finding has none and an attach there would be a
               property that is always absent, which is a lie about why it is absent. */
            Assert.Equal(1, CountOf(analyze, "FactRanked.Attach("));
            Assert.Contains("f.RootFactRanked,", analyze, StringComparison.Ordinal);
            Assert.Equal(1, CountOf(facts, "FactRanked.Attach(new"));
            Assert.Contains("}, f.Ranked, McpHelpers.JsonOptions))", facts, StringComparison.Ordinal);

            /* Never as a property, anywhere in the file: that is a serialized null on every card. */
            Assert.DoesNotContain("ranked =", CSharpSourceWalker.StripCommentsAndStrings(source), StringComparison.Ordinal);

            /* The read-back tool is untouched, which is the other half of "ephemeral". */
            var readBack = ToolBody(source, "get_analysis_findings", "GetAnalysisFindings", sku);
            Assert.Equal(0, CountOf(readBack, "FactRanked."));

            foreach (var (tool, method, sentence) in new[]
            {
                ("analyze_server", "AnalyzeServer", "carries root_fact.ranked"),
                ("get_analysis_facts", "GetAnalysisFacts", "carries ranked: the objects it ranks, worst first"),
            })
            {
                var at = source.IndexOf($"[McpServerTool(Name = \"{tool}\")", StringComparison.Ordinal);
                var attribute = source[at..source.IndexOf($"public static async Task<string> {method}(", at, StringComparison.Ordinal)];
                Assert.Contains(sentence, attribute, StringComparison.Ordinal);
                Assert.Contains("[{object_name, database_name, value, figures}]", attribute, StringComparison.Ordinal);
                Assert.Contains("capped at three", attribute, StringComparison.Ordinal);
            }
        }
    }

    /// <summary>The carry-through is in memory on BOTH SKUs' finding stores and on the engine, beside the root
    /// metadata it mirrors — and no store SQL mentions it, which is what "no migration rung" means here.</summary>
    [Fact]
    public void TheRootFactsRankedList_RidesInMemoryOnBothStores_AndNoStoreWritesIt()
    {
        var engine = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "InferenceEngine.cs");
        Assert.Contains("RootFactRanked = rootFact?.Ranked ?? []", engine, StringComparison.Ordinal);

        foreach (var (name, source) in new[]
        {
            ("PgFindingStore", RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgFindingStore.cs")),
            ("FindingStore", RepoFile.ReadRepoFile("Lite", "Analysis", "FindingStore.cs")),
        })
        {
            Assert.Contains("RootFactRanked = story.RootFactRanked", source, StringComparison.Ordinal);
            /* No column, no parameter, no INSERT list entry: the seam is ephemeral by construction. */
            var code = CSharpSourceWalker.StripCommentsAndStrings(source);
            Assert.DoesNotContain("root_fact_ranked", code, StringComparison.Ordinal);
            Assert.DoesNotContain($"ranked{name}", code, StringComparison.Ordinal);
        }
    }

    /* ── helpers ── */

    private static Fact Ranked(string objectName, double value, params (string Name, double Value)[] ranked)
    {
        var fact = new Fact
        {
            Key = PgTargetFactKeys.AutovacuumBacklog,
            Source = PgTargetSources.VacuumSource,
            ObjectName = objectName,
            DatabaseName = "appdb",
            Value = value,
        };
        foreach (var (name, v) in ranked)
            fact.Ranked.Add(new RankedObject(name, "appdb", v, new Dictionary<string, double>(StringComparer.Ordinal) { ["ratio"] = v }));
        return fact;
    }

    private static IEnumerable<(string Sku, string Source)> Tools()
    {
        yield return ("Darling", RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpTools.cs"));
        yield return ("Lite", RepoFile.ReadRepoFile("Lite", "Mcp", "McpAnalysisTools.cs"));
    }

    /// <summary>The tool method's body: from its attribute to the next <c>[McpServerTool(</c> (or end of file) —
    /// the <c>CollectionCaveatsParityTests</c> helper, same shape for the same reason.</summary>
    private static string ToolBody(string source, string tool, string method, string sku)
    {
        var at = source.IndexOf($"[McpServerTool(Name = \"{tool}\")", StringComparison.Ordinal);
        Assert.True(at > 0, $"{sku}: {tool} has moved");
        var bodyStart = source.IndexOf($"public static async Task<string> {method}(", at, StringComparison.Ordinal);
        Assert.True(bodyStart > at, $"{sku}: {method} has moved");
        var next = source.IndexOf("[McpServerTool(", bodyStart, StringComparison.Ordinal);
        return source[bodyStart..(next > 0 ? next : source.Length)];
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
            count++;
        return count;
    }
}
