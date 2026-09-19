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
using System.Reflection;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// D6 made executable (#3542 step 8): durability is policy, never optimisation, and the product enforces that
/// STRUCTURALLY rather than by review — a posture fact lives in a source no amplifier and no graph edge can
/// reach, and its advice cannot be phrased as a win because the words are not in the file.
///
/// <para><b>The four pins.</b> (1) No <c>PgTargetScorer*.cs</c> file other than the posture partial names a
/// posture key — so no <c>Amplifiers(...)</c> arm can boost a posture fact or read one as a corroborator — and
/// the posture partial itself defines no amplifier. (2) No <c>PgTargetRelationshipGraph*.cs</c> file names a
/// posture key — no edge into or out of a posture fact, so no story can absorb one as a leaf or root a tuning
/// chain on one; asserted at runtime too, over the built graph. (3) <c>PgTargetAdvice.Posture.cs</c> contains
/// none of the five words, comments included — the review-rejection rule from the design (§4c: "review should
/// reject any PR that adds one") as a test rather than a reviewer's memory. (4) The posture partial reads no
/// other fact and divides by no window: it is a switch statement over the fact's own key and metadata.</para>
///
/// <para>The scan is over the file text with the shared licence header removed and the product's own identifier
/// (<c>PerformanceMonitor</c>) blanked, because those two carry one of the words in every file in the repo and
/// are not advice. Everything else in the file — code, strings, doc comments — is in scope, on purpose: the rule
/// is that nobody explains a durability setting in optimisation terms anywhere near the advice.</para>
/// </summary>
public sealed class PgTargetPostureIsolationTests
{
    private static readonly string[] s_postureKeyTokens =
    {
        "PostureFsync", "PostureFullPageWrites", "PostureSynchronousCommit", "PG_POSTURE_",
    };

    [Fact]
    public void NoScorerFileButThePosturePartial_NamesAPostureKey_AndThePosturePartialHasNoAmplifier()
    {
        var directory = RepoFile.PathTo("PerformanceMonitor.Analysis");
        var files = Directory.GetFiles(directory, "PgTargetScorer*.cs").OrderBy(f => f, StringComparer.Ordinal).ToList();
        Assert.True(files.Count >= 12, $"expected the PgTargetScorer family (dispatcher + eleven partials); found {files.Count}");

        var offenders = new List<string>();
        foreach (var file in files)
        {
            var name = Path.GetFileName(file);
            var text = File.ReadAllText(file);
            if (name == "PgTargetScorer.Posture.cs")
            {
                Assert.DoesNotContain("AmplifierDefinition", text, StringComparison.Ordinal);
                Assert.DoesNotContain("Boost", text, StringComparison.Ordinal);
                Assert.DoesNotContain("Predicate", text, StringComparison.Ordinal);
                /* Reads its own fact only: no lookup into the scored set, no observed-window divisor. */
                Assert.DoesNotContain("factsByKey", text, StringComparison.Ordinal);
                Assert.DoesNotContain("ObservedDurationMs", text, StringComparison.Ordinal);
                Assert.DoesNotContain("period_duration_ms", text, StringComparison.Ordinal);
                continue;
            }

            foreach (var token in s_postureKeyTokens)
            {
                if (text.Contains(token, StringComparison.Ordinal))
                    offenders.Add($"{name} names {token}");
            }
        }

        Assert.True(
            offenders.Count == 0,
            "A scorer file outside the posture partial names a posture key — an amplifier arm or corroborator read that "
            + "could turn a durability statement into a tuning argument (D6):" + Environment.NewLine + string.Join(Environment.NewLine, offenders));
    }

    [Fact]
    public void NoGraphFile_NamesAPostureKey_AndTheBuiltGraphHasNoEdgeTouchingOne()
    {
        var directory = RepoFile.PathTo("PerformanceMonitor.Analysis");
        var files = Directory.GetFiles(directory, "PgTargetRelationshipGraph*.cs").OrderBy(f => f, StringComparer.Ordinal).ToList();
        Assert.True(files.Count >= 6, $"expected the PgTargetRelationshipGraph family (root + five chains); found {files.Count}");

        foreach (var file in files)
        {
            var text = File.ReadAllText(file);
            foreach (var token in s_postureKeyTokens)
                Assert.DoesNotContain(token, text, StringComparison.Ordinal);
        }

        /* And the graph as built: no edge FROM a posture key, and no edge from ANY declared key TO one. */
        var graph = new PgTargetRelationshipGraph();
        var postureKeys = PgTargetPostureTests.PostureKeys().ToHashSet(StringComparer.Ordinal);
        foreach (var key in postureKeys)
            Assert.Empty(graph.GetAllEdges(key));

        var everyKey = typeof(PgTargetFactKeys).GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!)
            .Where(PgTargetFactKeys.IsPgKey)
            .Append(PgTargetFactKeys.WaitKey("Lock", "relation"))
            .Append(PgTargetFactKeys.BadActorKey(7));
        foreach (var key in everyKey)
        {
            foreach (var edge in graph.GetAllEdges(key))
                Assert.False(postureKeys.Contains(edge.Destination), $"edge {key} → {edge.Destination} reaches a posture fact");
        }
    }

    [Fact]
    public void ThePostureAdviceFile_ContainsNoneOfTheOptimisationVocabulary_CommentsIncluded()
    {
        var path = Path.Combine(RepoFile.PathTo("PerformanceMonitor.Analysis"), "PgTargetAdvice.Posture.cs");
        var text = File.ReadAllText(path);

        /* Past the licence header (the first `using` is the first line of the file proper), with the product's
           identifier blanked — those two carry the word in every file of the repo and are not advice. */
        var start = text.IndexOf("using ", StringComparison.Ordinal);
        Assert.True(start > 0, "the advice partial has no using directive; the header trim would scan nothing");
        var body = text[start..].Replace("PerformanceMonitor", string.Empty, StringComparison.Ordinal);

        foreach (var word in PgTargetPostureTests.OptimisationVocabulary)
            Assert.DoesNotContain(word, body, StringComparison.OrdinalIgnoreCase);

        /* And the file is not empty of advice — the scan means something only over a filled partial. */
        Assert.Contains("ALTER SYSTEM SET fsync = on", body, StringComparison.Ordinal);
        Assert.Contains("ALTER SYSTEM SET full_page_writes = on", body, StringComparison.Ordinal);
        Assert.Contains("ALTER SYSTEM SET synchronous_commit = on", body, StringComparison.Ordinal);
    }

    /// <summary>The posture keys are advisory ROOTS (so a 0.4 card can surface on a quiet server) and NOT
    /// tuning-class: <c>FactScorer.IsTuningClassKey</c> is private, so the cap is pinned by its effect in
    /// <c>PgTargetPostureTests</c>; here the membership half, against the shared keys file.</summary>
    [Fact]
    public void PostureKeys_AreAdvisoryRoots_AndCarryThePosturePrefixNoOtherClassClaims()
    {
        foreach (var key in PgTargetPostureTests.PostureKeys())
        {
            Assert.True(PgTargetFactKeys.IsConfigAdvisoryRoot(key), $"{key} must root its own card at any positive grade");
            Assert.StartsWith("PG_POSTURE_", key, StringComparison.Ordinal);
            Assert.False(key.StartsWith(PgTargetFactKeys.ConfigPrefix, StringComparison.Ordinal), $"{key} must not be routed as a CONFIG_PG_ convention check");
            Assert.False(PgTargetFactKeys.IsPgAnomalyKey(key), $"{key} must not be routed as an anomaly (the tuning-class cap catches ANOMALY_)");
        }
    }
}
