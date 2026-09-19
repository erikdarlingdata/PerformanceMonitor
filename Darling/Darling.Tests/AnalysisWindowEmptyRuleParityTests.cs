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
/// #3653: the empty-window rule in the two analysis services is one rule with two spellings of a logger,
/// and this pins the parts that must not drift. The services are twins (<c>Lite/Analysis/AnalysisService.cs</c>
/// and <c>Darling/PerformanceMonitor.Darling.Analysis/DarlingAnalysisService.cs</c>) and both are read from
/// source here because no CI-run test project references both SKUs' assemblies; it lives in this project
/// rather than Lite.Tests because Darling's build filter reaches every Lite <c>.cs</c> file while the
/// <c>lite</c> filter does not reach this analysis tree (<c>CrossAppGuardCiGateTests</c>, #2839).
///
/// <para>What is pinned. (1) The dead-collector envelope is gated on the coverage witness ALONE —
/// <c>ObservedDurationMs &lt;= 0</c> — and the pre-#3653 <c>facts.Count == 0 ||</c> half is gone from both,
/// because that half is what made an observed window that produced no fact say "collection appears to have
/// stopped". (2) The observed-window-no-facts branch follows it, sets no message, does not return, and
/// precedes both the partial-coverage caveat and the anomaly detector — so an all-clear out of it is a real
/// pass at the stated coverage and the detector still gets its say. (3) The composed
/// <c>WindowEmptyMessage</c> statement — both arms, the point-in-time one and the bare one — is byte-identical
/// across the SKUs, and still carries the #3524 prose, because that prose is now true of exactly the case it
/// is emitted for.</para>
/// </summary>
public sealed class AnalysisWindowEmptyRuleParityTests
{
    private const string PassEntry = "public async Task<List<AnalysisFinding>> AnalyzeAsync(AnalysisContext context)";
    private const string UnobservedBranch = "if (context.ObservedDurationMs <= 0)";
    private const string ObservedZeroBranch = "if (facts.Count == 0)";
    private const string PartialCaveat = "if (context.Coverage is { IsPartial: true } partial)";
    private const string Detector = ".DetectAnomaliesAsync(context)";
    private const string MessageStart = "WindowEmptyMessage = hasPointInTimeFactsOnly";
    private const string MessageEnd = "— this is NOT an all-clear.\";";

    private static IEnumerable<(string Sku, string Source)> Sources()
    {
        yield return ("Darling", Lf(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "DarlingAnalysisService.cs")));
        yield return ("Lite", Lf(RepoFile.ReadRepoFile("Lite", "Analysis", "AnalysisService.cs")));
    }

    /// <summary>Line endings normalised here rather than through the LF reader: every anchor in this file
    /// sits on one line, and the block comparison below must not be decided by a checkout's autocrlf.</summary>
    private static string Lf(string source) => source.Replace("\r\n", "\n", StringComparison.Ordinal);

    [Fact]
    public void TheDeadCollectorEnvelope_IsGatedOnTheCoverageWitnessAlone_InBothSkus()
    {
        foreach (var (sku, source) in Sources())
        {
            var pass = PassBody(source, sku);

            Assert.Contains(UnobservedBranch, pass, StringComparison.Ordinal);

            /* The pre-#3653 spelling, in either operand order — checked on CODE, because the branch's own
               comment quotes the old rule to say why it went. */
            var code = CSharpSourceWalker.StripCommentsAndStrings(pass);
            Assert.DoesNotContain("facts.Count == 0 ||", code, StringComparison.Ordinal);
            Assert.DoesNotContain("|| context.ObservedDurationMs <= 0", code, StringComparison.Ordinal);
            Assert.DoesNotContain("|| facts.Count == 0", code, StringComparison.Ordinal);

            /* Order: unobserved envelope, then the observed-window-no-facts branch, then the partial caveat,
               then the detector. The observed-zero branch preceding the detector is the point — the pass
               continues into detection rather than returning an empty list on the collector's say-so. */
            var unobserved = pass.IndexOf(UnobservedBranch, StringComparison.Ordinal);
            var observedZero = pass.IndexOf(ObservedZeroBranch, StringComparison.Ordinal);
            var partial = pass.IndexOf(PartialCaveat, StringComparison.Ordinal);
            var detector = pass.IndexOf(Detector, StringComparison.Ordinal);
            Assert.True(
                unobserved > 0 && observedZero > unobserved && partial > observedZero && detector > partial,
                $"{sku}: expected unobserved ({unobserved}) < observed-zero ({observedZero}) < partial caveat ({partial}) < detector ({detector})");

            /* The unobserved branch is the one that sets the message and returns. */
            var unobservedBody = pass[unobserved..observedZero];
            Assert.Contains(MessageStart, unobservedBody, StringComparison.Ordinal);
            Assert.Contains("return [];", unobservedBody, StringComparison.Ordinal);

            /* The observed-zero branch sets NO message and does NOT return: WindowEmptyMessage stays null,
               so both analyze_server tools take their `empty` arm and the worker clears its marker. Its one
               line is Information, names the coverage, and says what the all-clear rests on. */
            var observedZeroBody = pass[observedZero..partial];
            Assert.DoesNotContain("WindowEmptyMessage =", observedZeroBody, StringComparison.Ordinal);
            Assert.DoesNotContain("return", CSharpSourceWalker.StripCommentsAndStrings(observedZeroBody), StringComparison.Ordinal);
            Assert.Contains("context.Coverage!.Describe()", observedZeroBody, StringComparison.Ordinal);
            Assert.Contains("nothing rose to a fact, and an all-clear from this pass rests on that coverage", observedZeroBody, StringComparison.Ordinal);
            Assert.DoesNotContain("collection may be down", observedZeroBody, StringComparison.Ordinal);
            Assert.DoesNotContain("NOT an all-clear", observedZeroBody, StringComparison.Ordinal);
            Assert.DoesNotContain("appears to have stopped", observedZeroBody, StringComparison.Ordinal);
            Assert.True(
                observedZeroBody.Contains("_logger?.LogInformation(", StringComparison.Ordinal)
                || observedZeroBody.Contains("AppLogger.Info(", StringComparison.Ordinal),
                $"{sku}: the observed-zero branch must log at Information — a Warning here is the old lie at a lower volume");
            Assert.DoesNotContain("LogWarning", observedZeroBody, StringComparison.Ordinal);
            Assert.DoesNotContain("AppLogger.Warn", observedZeroBody, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheComposedWindowEmptyMessage_IsByteIdenticalAcrossTheSkus_AndStillNamesTheDeadCollector()
    {
        string? darling = null, lite = null;
        foreach (var (sku, source) in Sources())
        {
            var statement = MessageStatement(source, sku);
            if (sku == "Darling") darling = statement; else lite = statement;
        }

        Assert.NotNull(darling);
        Assert.NotNull(lite);
        Assert.Equal(darling, lite);

        /* Both arms, and the prose that is now true of exactly the case it is emitted for. */
        Assert.Contains("The collector observed none of the analysis window", darling, StringComparison.Ordinal);
        Assert.Contains("No facts were collected in the analysis window", darling, StringComparison.Ordinal);
        Assert.Contains("point-in-time", darling, StringComparison.Ordinal);
        Assert.Equal(2, CountOf(darling!, "Collection appears to have stopped or "));
        Assert.Equal(2, CountOf(darling!, "this is NOT an all-clear."));
    }

    private static string PassBody(string source, string sku)
    {
        var start = source.IndexOf(PassEntry, StringComparison.Ordinal);
        Assert.True(start > 0, $"{sku}: the pass entry point has moved");
        var end = source.IndexOf("CollectAndScoreFactsAsync(", start, StringComparison.Ordinal);
        Assert.True(end > start, $"{sku}: CollectAndScoreFactsAsync no longer follows the pass");
        return source[start..end];
    }

    private static string MessageStatement(string source, string sku)
    {
        var pass = PassBody(source, sku);
        var start = pass.IndexOf(MessageStart, StringComparison.Ordinal);
        Assert.True(start > 0, $"{sku}: the WindowEmptyMessage assignment has moved");
        var end = pass.IndexOf(MessageEnd, start, StringComparison.Ordinal);
        Assert.True(end > start, $"{sku}: the WindowEmptyMessage assignment no longer ends on the #3524 sentence");
        return pass[start..(end + MessageEnd.Length)];
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
            count++;
        return count;
    }
}
