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
using System.Text.RegularExpressions;
using System.Threading;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Tier-0 baseline-model collapse (Darling half of the Lite&lt;-&gt;Darling lockstep):
/// Darling's baseline/anomaly brain consumes the SHARED model from the PerformanceMonitor.Analysis
/// assembly (namespace <c>PerformanceMonitor.Analysis.Baselines</c>) and keeps NO per-app copy of it,
/// so Darling and Lite can't silently re-fork the once-triplicated model + constants. The deprecated
/// Dashboard keeps its OWN copy on purpose (<c>PerformanceMonitorDashboard.Analysis</c>) — this pin is
/// Lite&lt;-&gt;Darling only. Lite.Tests carries the mirror-image pin for the Lite half.
/// </summary>
public sealed class SharedBaselineModelPinTests
{
    private const string SharedAssembly = "PerformanceMonitor.Analysis";
    private const string SharedNamespace = "PerformanceMonitor.Analysis.Baselines";

    [Fact]
    public void BaselineModelTypes_LiveInSharedAssemblyAndNamespace()
    {
        Assert.Equal(SharedAssembly, typeof(BaselineBucket).Assembly.GetName().Name);
        Assert.Equal(SharedAssembly, typeof(BaselineTier).Assembly.GetName().Name);
        Assert.Equal(SharedAssembly, typeof(MetricNames).Assembly.GetName().Name);
        Assert.Equal(SharedAssembly, typeof(BaselineMath).Assembly.GetName().Name);
        Assert.Equal(SharedAssembly, typeof(AnomalyThresholds).Assembly.GetName().Name);

        Assert.Equal(SharedNamespace, typeof(BaselineBucket).Namespace);
        Assert.Equal(SharedNamespace, typeof(BaselineTier).Namespace);
        Assert.Equal(SharedNamespace, typeof(MetricNames).Namespace);
        Assert.Equal(SharedNamespace, typeof(BaselineMath).Namespace);
        Assert.Equal(SharedNamespace, typeof(AnomalyThresholds).Namespace);
    }

    [Fact]
    public void Darling_KeepsNoPrivateCopyOfTheBaselineModel()
    {
        // If someone re-declares any of these in Darling's own Analysis namespace, this fails —
        // the whole point of the collapse is that the model has exactly ONE home.
        var darlingAssembly = typeof(PgBaselineProvider).Assembly;
        foreach (var forkedType in new[]
        {
            "PerformanceMonitor.Darling.Analysis.BaselineBucket",
            "PerformanceMonitor.Darling.Analysis.BaselineTier",
            "PerformanceMonitor.Darling.Analysis.MetricNames",
            "PerformanceMonitor.Darling.Analysis.BaselineMath",
            "PerformanceMonitor.Darling.Analysis.AnomalyThresholds",
        })
        {
            Assert.Null(darlingAssembly.GetType(forkedType));
        }
    }

    [Fact]
    public void PgBaselineProvider_BindsToTheSharedBaselineBucket()
    {
        // GetBaselineAsync returns Task<BaselineBucket>; pin the element type to the shared type —
        // the SAME PerformanceMonitor.Analysis.Baselines.BaselineBucket Lite binds to. BOTH overloads since #3691
        // lane 33 (the unkeyed four-parameter lookup and the keyed five-parameter one): a keyed series returns the
        // same shared bucket shape, never a keyed variant of it — the key belongs to the caller, not the bucket.
        foreach (var signature in new[]
        {
            new[] { typeof(int), typeof(string), typeof(DateTime), typeof(CancellationToken) },
            new[] { typeof(int), typeof(string), typeof(string), typeof(DateTime), typeof(CancellationToken) },
        })
        {
            var method = typeof(PgBaselineProvider).GetMethod(nameof(PgBaselineProvider.GetBaselineAsync), signature);
            Assert.NotNull(method);
            var bucketType = method!.ReturnType.GetGenericArguments()[0];
            Assert.Same(typeof(BaselineBucket), bucketType);
            Assert.Equal(SharedAssembly, bucketType.Assembly.GetName().Name);
        }
    }

    /* ── #3859 item 3: the three AddBaselineContext copies stamp ONE key set ── */

    /// <summary>The three LIVE hand-maintained copies of the baseline-context stamping helper, repo-root
    /// relative. The frozen fourth (<c>deprecated/Dashboard/Analysis/SqlServerAnomalyDetector.cs</c>) is
    /// deliberately absent: it keeps its own copy of the whole baseline model on purpose, the same boundary
    /// this class's other pins draw, and holding a frozen file to a live contract would red on the next stamp
    /// anyone adds rather than on a drift.</summary>
    private static readonly string[] s_stampingBodies =
    {
        "Lite/Analysis/AnomalyDetector.cs",
        "Darling/PerformanceMonitor.Darling.Analysis/PgAnomalyDetector.cs",
        "Darling/PerformanceMonitor.Darling.Analysis/PgTargetAnomalyDetector.cs",
    };

    /// <summary>
    /// The metadata keys every one of those bodies must stamp, and the whole set each may stamp.
    ///
    /// <para>Written out rather than derived from one of the three, because deriving it from a body makes the
    /// census a tautology on that body: whichever file the expectation is read from can drift freely and the
    /// other two are then measured against the drift. A literal set is the only spelling where adding a stamp
    /// is a decision recorded HERE as well as in three source files.</para>
    ///
    /// <para><c>baseline_distinct_days_is_proxy</c> is #3859 item 2's stamp and is the reason this census was
    /// written beside it: the Flat tier's distinct-day count is a ~5 ceiling proxy (see <c>CollapseToFlat</c>),
    /// the stamp is what tells a <c>get_analysis_facts</c> reader so, and a stamp that landed in two of three
    /// bodies would say it for one SKU and lie by omission in the other.</para>
    /// </summary>
    private static readonly string[] s_stampedKeys =
    {
        "baseline_distinct_days",
        "baseline_distinct_days_is_proxy",
        "baseline_dow",
        "baseline_hour",
        "baseline_mad",
        "baseline_median",
        "baseline_tier",
        "confidence",
    };

    /// <summary>A <c>metadata["some_key"]</c> write inside the helper's body. Matched against the body text
    /// with comments and literals INTACT for the key to be readable at all, so the doc comments in these
    /// bodies must not contain the code spelling — they discuss the keys by bare name, which this cannot
    /// match.</summary>
    private static readonly Regex StampedKey = new(
        @"metadata\[""(?<key>[A-Za-z_][A-Za-z0-9_]*)""\]\s*=", RegexOptions.Compiled);

    [Fact]
    public void TheThreeAddBaselineContextCopies_StampTheIdenticalKeySet()
    {
        /* #3859 item 3, the #3665 delegation-equality shape. AddBaselineContext is hand-maintained in three
           live copies — one per SKU pass — and nothing pinned that they agree. #3691 lane 41 edited all three
           identically to add two stamps and got away with it; the NEXT such edit is the one that lands in two
           of the three, and the failure is silent in the direction that costs: the fact payload keeps its
           shape for the SKU that got the stamp, and the reader of the other SKU's facts cannot tell a key
           that was never stamped from a condition that never held.

           The SET of keys, not the body text. The Darling copies carry extra comments and the PgTarget copy's
           summary is a cross-reference rather than a restatement, all of which is deliberate; text equality
           would red on prose and would have to be defeated by normalising the very thing that makes these
           bodies readable. The keys are the contract get_analysis_facts reads. */
        var found = new Dictionary<string, string[]>(StringComparer.Ordinal);

        foreach (var relativePath in s_stampingBodies)
        {
            var body = AddBaselineContextBody(relativePath);

            /* The floor that stops an empty scan passing. A body located but read as nothing satisfies a set
               comparison against nothing, and this whole census then reports clean on three empty strings —
               the same failure RepoFileAdoptionTests floors its own sweep against. */
            Assert.True(
                body.Length > 200,
                $"AddBaselineContext in {relativePath} read as {body.Length} characters. The declaration was "
              + "located but its body was not, so every key comparison below would pass on nothing");

            var keys = StampedKey.Matches(body)
                .Select(m => m.Groups["key"].Value)
                .Distinct(StringComparer.Ordinal)
                .OrderBy(k => k, StringComparer.Ordinal)
                .ToArray();

            Assert.True(
                keys.Length >= s_stampedKeys.Length,
                $"{relativePath} stamps {keys.Length} keys, under the {s_stampedKeys.Length} this contract "
              + "names. Either a stamp was dropped from this copy alone, or the scan cannot see the spelling "
              + "it uses — in which case the equality below is comparing populations it cannot read");

            found[relativePath] = keys;
        }

        var expected = s_stampedKeys.OrderBy(k => k, StringComparer.Ordinal).ToArray();

        foreach (var (relativePath, keys) in found)
        {
            Assert.Equal(expected, keys);
        }

        /* Asserted as an equality BETWEEN the three as well as against the expectation, because that is the
           claim in the issue: the copies agree with each OTHER. Against the literal alone, three bodies could
           satisfy it while the expectation itself had been edited to match a drift in one of them. */
        var distinct = found.Values
            .Select(keys => string.Join(",", keys))
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        Assert.Single(distinct);

        /* And the floor under the expectation itself — emptying s_stampedKeys would make every comparison
           above trivially true on three bodies that stamp nothing. */
        Assert.True(s_stampedKeys.Length >= 8, "the expected key set has been emptied, not narrowed");
        Assert.Equal(3, s_stampingBodies.Length);
    }

    /// <summary>
    /// One <c>AddBaselineContext</c> declaration's body, verbatim, located on a comment- and literal-stripped
    /// copy so a mention of the name in a doc comment cannot be mistaken for the declaration — then returned
    /// from the ORIGINAL text over the same offsets, because the keys this census reads ARE string literals
    /// and the stripped copy has blanked them. The walker replaces rather than removes, so the offsets agree
    /// (the shape <c>PgCappedReadSurfaceTests.MemberBody</c> uses, and for the same reason).
    /// </summary>
    private static string AddBaselineContextBody(string relativePath)
    {
        var source = RepoFile.ReadRepoFile(relativePath);
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(source);

        var declarations = Regex
            .Matches(stripped, @"private\s+static\s+void\s+AddBaselineContext\s*\(", RegexOptions.None)
            .Select(m => m.Index)
            .ToList();

        var at = Assert.Single(declarations);
        var open = stripped.IndexOf('{', at);

        Assert.True(open > at, relativePath + ": AddBaselineContext has no body");

        /* Brace-balanced by the WALKER's helper rather than a local counter: braces inside an interpolation
           hole are code, so a hand-rolled count unbalances on the first $"...{x}..." it meets. */
        var extent = CSharpSourceWalker.BraceBalanced(stripped, open).Length;

        return source[open..(open + extent)];
    }
}
