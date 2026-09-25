/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3691: <c>get_analysis_facts</c> reads through <c>CollectAndScoreFactsAsync</c>, and on both SKUs that
/// read was collector + scorer only — the anomaly detectors ran in the full pass and nowhere else. So the
/// tool whose description sells "every observation the engine sees" never held an <c>ANOMALY_*</c> /
/// <c>ANOMALY_PG_*</c> fact, and the gate metadata the detectors stamp was reachable only through a finding
/// that had already crossed the severity floor. This pins the repaired shape in both services from source,
/// the way <see cref="AnalysisWindowEmptyRuleParityTests"/> pins the pass: the twins are
/// <c>Lite/Analysis/AnalysisService.cs</c> and <c>Darling/PerformanceMonitor.Darling.Analysis/DarlingAnalysisService.cs</c>,
/// no CI-run test project references both assemblies, and the Darling filter reaches every Lite <c>.cs</c>
/// file while the <c>lite</c> filter does not reach this tree (<c>CrossAppGuardCiGateTests</c>, #2839).
///
/// <para>What is pinned. (1) Inside the read, the order is collector → detector → scorer, the pass's order,
/// so anomaly facts arrive SCORED with their metadata; the detector call is the same
/// <c>.DetectAnomaliesAsync(context)</c> spelling the pass uses over the same context. (2) The detector is
/// gated on the coverage witness exactly as the pass gates it — <c>context.ObservedDurationMs &gt; 0</c> —
/// so the tools' unobserved envelope keeps describing exactly the point-in-time facts it names. (3) Darling
/// runs the RESOLVED engine's detector (<c>engine.Detector</c>) off the one <c>ResolveEngineAsync</c> the
/// read already performs — never a detector constructed by hand, which is how a PostgreSQL target would get
/// the SQL Server detector's reads against tables it does not have. (4) The behavioural half lives where a
/// store exists: Lite's <c>AnomalyDetectorTests.CollectAndScoreFacts_RunsTheDetector_…</c> drives the real
/// service over the batch-request spike fixture; Darling's gated <c>PgTargetAnomalyTests</c> e2e asserts
/// the planted TPS spike through the real <c>get_analysis_facts</c>. This file is the shape both rest on.</para>
/// </summary>
public sealed class AnalysisFactsReadRunsDetectorParityTests
{
    private const string ReadEntry = "public async Task<(List<Fact> Facts, WindowCoverage? Coverage, CollectionCaveatState Caveats)> CollectAndScoreFactsAsync(";
    private const string ReadEnd = "ComparePeriodsAsync(";
    private const string Collect = ".CollectFactsAsync(context);";
    private const string Gate = "if (context.ObservedDurationMs > 0)";
    private const string Detector = ".DetectAnomaliesAsync(context);";
    private const string Append = "facts.AddRange(anomalies);";
    private const string Score = "_scorer.ScoreAll(facts);";

    private static IEnumerable<(string Sku, string Source)> Sources()
    {
        yield return ("Darling", RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "DarlingAnalysisService.cs"));
        yield return ("Lite", RepoFile.ReadRepoFile("Lite", "Analysis", "AnalysisService.cs"));
    }

    [Fact]
    public void TheFactsRead_RunsTheDetectorBetweenCollectorAndScorer_GatedOnTheCoverageWitness_InBothSkus()
    {
        foreach (var (sku, source) in Sources())
        {
            var read = ReadBody(source, sku);
            var code = CSharpSourceWalker.StripCommentsAndStrings(read);

            var collect = code.IndexOf(Collect, StringComparison.Ordinal);
            var gate = code.IndexOf(Gate, StringComparison.Ordinal);
            var detector = code.IndexOf(Detector, StringComparison.Ordinal);
            var append = code.IndexOf(Append, StringComparison.Ordinal);
            var score = code.IndexOf(Score, StringComparison.Ordinal);

            Assert.True(
                collect > 0 && gate > collect && detector > gate && append > detector && score > append,
                $"{sku}: expected collect ({collect}) < witness gate ({gate}) < detector ({detector}) < append ({append}) < score ({score})");

            /* Exactly one detector call and one scoring call: the read is the pass's first three stages
               once, not a detector that runs twice or a scorer that runs before the anomalies are in. */
            Assert.Equal(1, CountOf(code, Detector));
            Assert.Equal(1, CountOf(code, Score));

            /* The gate is the pass's, with the pass's polarity — the pass returns on `<= 0`, the read skips
               the detector on the same witness. A read that ran the detector over an unobserved window would
               hand the tools anomaly facts to render under an envelope that says nothing windowed exists. */
            Assert.DoesNotContain("ObservedDurationMs <= 0", code, StringComparison.Ordinal);

            /* The one early return sits AFTER the detector: a collector that observed the window and emitted
               nothing must not short-circuit past the detector's say (the #3653 rule the pass follows). */
            var earlyReturn = code.IndexOf("if (facts.Count == 0) return", StringComparison.Ordinal);
            Assert.True(earlyReturn > append && earlyReturn < score, $"{sku}: the empty-set return must follow the detector and precede scoring");

            /* No detector is constructed inside the read on either SKU. */
            Assert.DoesNotContain("new AnomalyDetector(", code, StringComparison.Ordinal);
            Assert.DoesNotContain("new PgAnomalyDetector(", code, StringComparison.Ordinal);
            Assert.DoesNotContain("new PgTargetAnomalyDetector(", code, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Darling_RunsTheResolvedEnginesDetector_AndLiteRunsItsOwn()
    {
        foreach (var (sku, source) in Sources())
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(ReadBody(source, sku));
            if (sku == "Darling")
            {
                /* The set the read already resolved, so a PostgreSQL target gets PgTargetAnomalyDetector's
                   ANOMALY_PG_* facts and a SQL Server target PgAnomalyDetector's — one resolution, both
                   components off it. */
                Assert.Contains("await ResolveEngineAsync(", code, StringComparison.Ordinal);
                Assert.Contains("engine.Collector.CollectFactsAsync(context);", code, StringComparison.Ordinal);
                Assert.Contains("engine.Detector.DetectAnomaliesAsync(context);", code, StringComparison.Ordinal);
                Assert.DoesNotContain("_sqlServerEngine.Detector", code, StringComparison.Ordinal);
                Assert.DoesNotContain("_pgTargetEngine.Detector", code, StringComparison.Ordinal);
            }
            else
            {
                Assert.Contains("_collector.CollectFactsAsync(context);", code, StringComparison.Ordinal);
                Assert.Contains("_anomalyDetector.DetectAnomaliesAsync(context);", code, StringComparison.Ordinal);
            }
        }
    }

    /// <summary>
    /// The read's description on both SKUs says the detector runs on it — the sentence that replaces the
    /// old one's silence, which a caller read as "facts, not anomalies". The byte-identical text across the
    /// SKUs is <c>McpMissMessageParityPinTests</c>' pin; this is the pin that the sentence is on the
    /// <c>get_analysis_facts</c> tool and not merely somewhere in each tree.
    /// </summary>
    [Fact]
    public void BothToolDescriptions_SayTheDetectorRunsOnThisRead()
    {
        foreach (var (sku, tools) in new[]
        {
            ("Darling", RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpTools.cs")),
            ("Lite", RepoFile.ReadRepoFile("Lite", "Mcp", "McpAnalysisTools.cs")),
        })
        {
            var at = tools.IndexOf("[McpServerTool(Name = \"get_analysis_facts\")", StringComparison.Ordinal);
            Assert.True(at > 0, $"{sku}: get_analysis_facts has moved");
            var attribute = tools[at..tools.IndexOf("public static async Task<string> GetAnalysisFacts(", at, StringComparison.Ordinal)];
            Assert.Contains("The anomaly detector runs on this read too, so the ANOMALY_* facts the full pass would score are here", attribute, StringComparison.Ordinal);
            Assert.Contains("including the ones that fired but stayed under the finding floor", attribute, StringComparison.Ordinal);
            Assert.Contains("the cost is the detector's baseline reads on top of the collector's", attribute, StringComparison.Ordinal);
        }

        /* #3898 Phase 2 (D5): the instructions' per-tool row that used to restate this is gone on both SKUs —
           the "Asking about a PAST window" section's as_of guardrail still says the anchor reaches the
           detector, which is the fact this half of the pin protects; the stale collect+score phrasing it
           guarded against cannot come back once there is no per-tool row left to carry it. */
        foreach (var (sku, instructions) in new[]
        {
            ("Darling", RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpInstructions.cs")),
            ("Lite", RepoFile.ReadRepoFile("Lite", "Mcp", "McpInstructions.cs")),
        })
        {
            Assert.Contains("re-run fact collection, anomaly detection and scoring over the anchored window", instructions, StringComparison.Ordinal);
            Assert.DoesNotContain("`analyze_server`'s anomaly detection moves with it", instructions, StringComparison.Ordinal);
        }
    }

    private static string ReadBody(string source, string sku)
    {
        var start = source.IndexOf(ReadEntry, StringComparison.Ordinal);
        Assert.True(start > 0, $"{sku}: the facts read's entry point has moved");
        /* Stop at CollectConfigAuditFactsAsync when present (Darling only; #4206 added it between the two
           reads), otherwise stop at ComparePeriodsAsync. Without this, the scorer added to the narrow pass
           is counted twice — once from CollectAndScoreFactsAsync, once from CollectConfigAuditFactsAsync. */
        var configAuditStart = source.IndexOf("CollectConfigAuditFactsAsync(", start, StringComparison.Ordinal);
        var end = configAuditStart > start
            ? configAuditStart
            : source.IndexOf(ReadEnd, start, StringComparison.Ordinal);
        Assert.True(end > start, $"{sku}: the end of the facts read no longer follows its entry point");
        return source[start..end];
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
            count++;
        return count;
    }
}
