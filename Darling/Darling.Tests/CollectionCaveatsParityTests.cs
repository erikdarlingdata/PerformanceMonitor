/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3691: the collection-caveat wiring is one design with two spellings, and this pins the parts that must not
/// drift between the SKUs — read from source because no CI-run test project references both SKUs' assemblies
/// (the <c>AnalysisWindowEmptyRuleParityTests</c> pattern). Three layers:
/// <list type="bullet">
/// <item><description><b>Every reporter records.</b> The three <c>ReportCollectionFailure</c> helpers (Lite's
/// DuckDB collector, Darling's SQL Server collector, Darling's PostgreSQL-target collector) call
/// <c>context.RecordCollectionFailure(</c> as their FIRST statement, ahead of the log arms, so no log level
/// can skip the record. Together with <c>FactCollectorFailureReportingTests</c> (every swallowing catch reaches
/// a reporter) that is the census the STEP asked for: every catch in a collect method reaches the recorder.
/// Each collector stamps the family total from its own type.</description></item>
/// <item><description><b>Both passes carry it out.</b> <c>LastCollectionFailures</c> is taken off the context
/// on the line after <c>LastWindowCoverage</c>, the fact counts are read immediately after <c>ScoreAll</c>,
/// and every return of <c>CollectAndScoreFactsAsync</c> — the catch included — hands back the state.</description></item>
/// <item><description><b>Both tool files emit it only when owed.</b> <c>analyze_server</c>'s three envelopes and
/// <c>get_analysis_facts</c>'s three go through <c>Attach(</c>, the caveat strings go through <c>Compose(</c>,
/// and no tool body spells <c>collection_caveats =</c> as a property — which is how a null would get
/// serialized on a clean pass and move its bytes. The descriptions name the field on both SKUs.</description></item>
/// </list>
/// </summary>
public sealed class CollectionCaveatsParityTests
{
    private const string Record = "context.RecordCollectionFailure(";

    private static IEnumerable<(string Name, string Source)> Reporters()
    {
        yield return ("DuckDbFactCollector", RepoFile.ReadRepoFile("Lite", "Analysis", "DuckDbFactCollector.cs"));
        yield return ("PgFactCollector", RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgFactCollector.cs"));
        yield return ("PgTargetFactCollector", RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.cs"));
    }

    private static IEnumerable<(string Sku, string Source)> Services()
    {
        yield return ("Darling", RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "DarlingAnalysisService.cs"));
        yield return ("Lite", RepoFile.ReadRepoFile("Lite", "Analysis", "AnalysisService.cs"));
    }

    private static IEnumerable<(string Sku, string Source)> Tools()
    {
        yield return ("Darling", RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpTools.cs"));
        yield return ("Lite", RepoFile.ReadRepoFile("Lite", "Mcp", "McpAnalysisTools.cs"));
    }

    [Fact]
    public void EveryReporter_RecordsOnTheContextFirst_AndEveryCollectorStampsItsFamilyTotal()
    {
        foreach (var (name, source) in Reporters())
        {
            var start = source.IndexOf("void ReportCollectionFailure(", StringComparison.Ordinal);
            Assert.True(start > 0, $"{name}: ReportCollectionFailure has moved");
            var open = source.IndexOf('{', start);
            var end = NextMemberAfter(source, open);
            var body = CSharpSourceWalker.StripCommentsAndStrings(source[open..end]);

            /* The record is the first statement of the body: the text between the opening brace and the
               first semicolon names it — ahead of every log arm, so no level can skip it. Once. */
            var firstStatement = body[1..(body.IndexOf(';') + 1)];
            Assert.Contains(Record, firstStatement, StringComparison.Ordinal);
            Assert.Equal(1, CountOf(body, Record));

            Assert.Contains($"CollectionCaveats.CountFamilies(typeof({name}))", source, StringComparison.Ordinal);
            Assert.Contains("context.CollectionFamilyCount = s_familyCount;", source, StringComparison.Ordinal);

            /* The stamp precedes the first family call, so a pass that fails at its first read has a total. */
            var stamp = source.IndexOf("context.CollectionFamilyCount = s_familyCount;", StringComparison.Ordinal);
            var firstFamily = source.IndexOf("CollectObservedCoverageAsync", stamp, StringComparison.Ordinal);
            Assert.True(firstFamily > stamp, $"{name}: the family total must be stamped before the coverage witness runs");
        }

        /* The PostgreSQL-target reporter labels by FILE (its families split reads across helpers); the two
           SQL Server reporters label by method. */
        var pgTarget = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.cs");
        Assert.Contains("[CallerFilePath] string collectFile", pgTarget, StringComparison.Ordinal);
        Assert.Contains("CollectionFailure.FamilyOfFile(collectFile)", pgTarget, StringComparison.Ordinal);
        Assert.Contains("CollectionFailure.FamilyOf(collectMethod)", RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgFactCollector.cs"), StringComparison.Ordinal);
        Assert.Contains("CollectionFailure.FamilyOf(collectMethod)", RepoFile.ReadRepoFile("Lite", "Analysis", "DuckDbFactCollector.cs"), StringComparison.Ordinal);
    }

    [Fact]
    public void BothPasses_CarryTheFailuresAndTheFactCountsOut_AndBothReadsReturnTheState()
    {
        foreach (var (sku, source) in Services())
        {
            var passStart = source.IndexOf("public async Task<List<AnalysisFinding>> AnalyzeAsync(AnalysisContext context)", StringComparison.Ordinal);
            const string readEntry = "public async Task<(List<Fact> Facts, WindowCoverage? Coverage, CollectionCaveatState Caveats)> CollectAndScoreFactsAsync(";
            var readStart = source.IndexOf(readEntry, passStart, StringComparison.Ordinal);
            var readEnd = source.IndexOf("ComparePeriodsAsync(", readStart, StringComparison.Ordinal);
            Assert.True(passStart > 0 && readStart > passStart && readEnd > readStart, $"{sku}: the pass / read layout has moved, or the read no longer returns the caveat state");

            var pass = source[passStart..readStart];
            var passCode = CSharpSourceWalker.StripCommentsAndStrings(pass);

            /* Off the context on the line after the coverage — the same place, the same reason. */
            var coverage = passCode.IndexOf("LastWindowCoverage = context.Coverage;", StringComparison.Ordinal);
            var failures = passCode.IndexOf("LastCollectionFailures = context.CollectionFailures;", StringComparison.Ordinal);
            var total = passCode.IndexOf("LastCollectionFamilyCount = context.CollectionFamilyCount;", StringComparison.Ordinal);
            var unobserved = passCode.IndexOf("if (context.ObservedDurationMs <= 0)", StringComparison.Ordinal);
            Assert.True(coverage > 0 && failures > coverage && total > failures && unobserved > total,
                $"{sku}: expected coverage ({coverage}) < failures ({failures}) < total ({total}) < unobserved branch ({unobserved})");

            /* The pass logs the summary once, at Warning, before the unobserved branch — a scheduled pass has
               no payload to carry the block. */
            var caveatLog = passCode.IndexOf("CollectionCaveats.Describe(context.CollectionFailures, context.CollectionFamilyCount)", StringComparison.Ordinal);
            Assert.True(caveatLog > total && caveatLog < unobserved, $"{sku}: the collection-caveat log line sits between the stamp and the unobserved branch");

            /* The counts are read right after scoring, before attribution appends its own cards. */
            var score = passCode.IndexOf("_scorer.ScoreAll(facts);", StringComparison.Ordinal);
            var factCount = passCode.IndexOf("LastFactCount = facts.Count;", StringComparison.Ordinal);
            var scored = passCode.IndexOf("LastFactsScored = facts.Count(f => f.Severity > 0);", StringComparison.Ordinal);
            var attribution = passCode.IndexOf("AttributeConfigChangesAsync(", StringComparison.Ordinal);
            Assert.True(score > 0 && factCount > score && scored > factCount && attribution > scored,
                $"{sku}: expected ScoreAll ({score}) < fact count ({factCount}) < facts scored ({scored}) < attribution ({attribution})");

            /* Every return of the read — the empty early return, the scored return, the catch — hands back the state. */
            var read = CSharpSourceWalker.StripCommentsAndStrings(source[readStart..readEnd]);
            Assert.Equal(3, CountOf(read, "CollectionCaveatState.From(context)"));
            Assert.Equal(3, CountOf(read, "return ("));
            Assert.DoesNotContain("return (facts, context.Coverage);", read, StringComparison.Ordinal);
            Assert.DoesNotContain("return ([], null);", read, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void BothToolFiles_AttachTheBlockOnlyWhenOwed_AndNeverSpellItAsAProperty()
    {
        foreach (var (sku, source) in Tools())
        {
            var analyze = ToolBody(source, "analyze_server", "AnalyzeServer", sku);
            var facts = ToolBody(source, "get_analysis_facts", "GetAnalysisFacts", sku);

            /* analyze_server: the unavailable envelope, the empty envelope and the findings envelope carry the caveat
               block; since #3691 lane 42 the findings envelope ALSO passes through StorySideLeaves.Attach (the config
               levers the walk swept onto each story), counted apart so the two attachments cannot be confused — and
               so both SKUs' files must carry the side-leaf pass exactly once. */
            Assert.Equal(1, CountOf(analyze, "StorySideLeaves.Attach(new"));
            Assert.Equal(3, CountOf(analyze, ".Attach(new") - CountOf(analyze, "StorySideLeaves.Attach(new"));
            Assert.Equal(1, CountOf(analyze, "CollectionCaveats.Compose(coverageCaveat, collectionCaveat)"));
            Assert.Contains("new CollectionCaveatState(analysisService.LastCollectionFailures, analysisService.LastCollectionFamilyCount)", analyze, StringComparison.Ordinal);
            Assert.Contains("fact_count = analysisService.LastFactCount,", analyze, StringComparison.Ordinal);
            Assert.Contains("facts_scored = analysisService.LastFactsScored", analyze, StringComparison.Ordinal);
            Assert.Contains("COLLECTION CAVEAT: ", analyze, StringComparison.Ordinal);

            /* get_analysis_facts: the zero-facts miss, the unobserved miss and the data result. */
            Assert.Contains("var (facts, coverage, collection) = await analysisService.CollectAndScoreFactsAsync(", facts, StringComparison.Ordinal);
            Assert.Equal(3, CountOf(facts, "collection.Attach(new"));
            Assert.Equal(1, CountOf(facts, "CollectionCaveats.Compose("));
            Assert.Contains("COLLECTION CAVEAT: ", facts, StringComparison.Ordinal);

            /* Never as a property: that is a serialized null on every clean pass. */
            Assert.DoesNotContain("collection_caveats =", CSharpSourceWalker.StripCommentsAndStrings(source), StringComparison.Ordinal);

            /* Both descriptions name the field and the rule. */
            foreach (var (tool, method) in new[] { ("analyze_server", "AnalyzeServer"), ("get_analysis_facts", "GetAnalysisFacts") })
            {
                var at = source.IndexOf($"[McpServerTool(Name = \"{tool}\")", StringComparison.Ordinal);
                var attribute = source[at..source.IndexOf($"public static async Task<string> {method}(", at, StringComparison.Ordinal)];
                Assert.Contains("the payload carries collection_caveats (families_failed, families_total, entries[{family, read, outcome, message}])", attribute, StringComparison.Ordinal);
            }
            var analyzeAttribute = source[source.IndexOf("[McpServerTool(Name = \"analyze_server\")", StringComparison.Ordinal)..source.IndexOf("public static async Task<string> AnalyzeServer(", StringComparison.Ordinal)];
            Assert.Contains("The empty envelope also states fact_count (facts the scorer saw) and facts_scored (those graded above zero)", analyzeAttribute, StringComparison.Ordinal);
        }
    }

    /// <summary>The offset of the next member declaration after <paramref name="from"/> — the method's end, without brace counting.</summary>
    private static int NextMemberAfter(string source, int from)
    {
        var candidates = new[] { "\n    private ", "\n    internal ", "\n    public ", "\n    /// <summary>" }
            .Select(marker => source.IndexOf(marker, from, StringComparison.Ordinal))
            .Where(i => i > from)
            .ToArray();
        Assert.NotEmpty(candidates);
        return candidates.Min();
    }

    /// <summary>The tool method's body: from its attribute to the next <c>[McpServerTool(</c> (or end of file).</summary>
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
