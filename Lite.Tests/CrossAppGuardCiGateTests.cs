/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Linq;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The DERIVED form of <see cref="WatermarkPolicyTests.TheLiteSuite_RunsOnEveryTreeTheHorizonPinScans"/>.
///
/// <para>That pin closed ONE instance: <c>Darling/PerformanceMonitor.Darling.Service</c> belongs only to the
/// <c>darling</c> path filter, so a Darling-only PR skipped the Lite suite that scans it. #2830 then hit the
/// same class twice more — <c>Lite.Tests/QueryStoreSliceTieBreakSourceTests.cs</c> reads DARLING'S counterpart
/// guard as source and asserts on the site total declared there, and raising that total 12 -> 18 fired
/// <c>darling</c> but not <c>lite</c>. Six checks green, merged, and the nightly on the merge commit failed
/// what the PR could not run. <c>dev</c> was red for ~20 minutes.</para>
///
/// <para><b>So this asserts the property rather than a path.</b> Every cross-app source reference in either
/// test project must be reachable by the filter that gates that project's suite. An enumerated list of trees
/// is what stopped covering the next file added — three times in one week — so the requirement is derived from
/// the source: add a read of the other app and the filter must grow, or this fails.</para>
///
/// <para>Only references that RESOLVE on disk are required, which drops message strings and the filter
/// patterns other guards assert on (those carry glob characters and are excluded outright). The glob matcher
/// is self-validated against known answers below, so a matcher bug fails loudly instead of green-washing.</para>
///
/// <para><b>Two populations, because a cross-app read is not always C#.</b> A linked compile —
/// <c>&lt;Compile Include="..\Darling\Darling.Tests\X.cs" /&gt;</c> — is a read of exactly the kind this
/// guard exists to require be filter-reachable, and #3063 was filed because a <c>*.cs</c>-only scan could not
/// see one: the file was never opened. So the second population is the project's ITEMS — and
/// <b>MSBuild evaluates them rather than this file parsing them.</b>
/// <c>dotnet msbuild &lt;project&gt; -getItem:… -getProperty:…</c> answers with properties expanded, imports
/// followed, <c>Condition</c>s applied, wildcards expanded, and the <c>Directory.Build.props</c> probe
/// already performed, in one offline invocation that builds nothing. Reading the XML by regex meant
/// re-implementing MSBuild's evaluator, and each correctness fix to it exposed the next construct it could
/// not evaluate; asking MSBuild retires the class rather than the instance.</para>
///
/// <para><b>Shelling out is a new failure mode, and it fails in this guard's dangerous direction.</b> A run
/// that reports no cross-app references because a process never started is worse than a regex that reported
/// too few, so the invocation is floored in three places that are asserted separately — it exited zero, its
/// output parsed, and it returned items — before anything asks WHAT those items are. The per-type floor is
/// not decoration either: an item type MSBuild does not recognise comes back as a present-but-EMPTY array
/// with exit 0, so a typo in <see cref="EvaluatedItemTypes"/> is invisible to any check on the real tree
/// (<c>Content</c>, <c>EmbeddedResource</c> and <c>Reference</c> are all legitimately empty here). Only
/// <see cref="TheEvaluatedRead_SurvivesEveryFormThatDefeatsAnXmlParse"/>'s synthetic two-SKU fixture, which
/// carries one item of every requested type, can tell a typo from an honest zero.</para>
///
/// <para><b>The one thing the evaluated read does not report is an <c>&lt;Import&gt;</c>'s own path.</b>
/// Items an imported file DEFINES arrive with the rest; the imported file's path is not an item, and
/// <c>-getProperty</c> answers only for the build files MSBuild locates itself. So a scanned project
/// carrying an <c>&lt;Import&gt;</c> element fails <c>Check</c> (removed with the old CI jobs, #6656) loudly rather than being quietly
/// half-scanned. There are none in this repository, which is what makes a gate cheaper than an
/// evaluator.</para>
///
/// <para><b>A SKU owns more than one tree, which is #3067.</b> Every anchor here was the APP directory,
/// and <c>Lite.Tests</c> is a SIBLING of <c>Lite</c> rather than a child — so four reads of Lite's test
/// project from <c>Darling.Tests</c> matched nothing at all, and an arm that opened 454 files and found
/// 15 references looked exactly as healthy as one that had found them. The anchor is now every root a
/// SKU owns, and each arm is FLOORED on finding at least one reference under the other SKU's test tree,
/// because that is the population whose disappearance a reference total cannot show.</para>
///
/// <para><b>A filter entry is not the only thing that can cover a read.</b> <c>darling-tree-guards</c>
/// runs the whole <c>Darling.Tests</c> suite exactly when the <c>darling</c> filter did not fire, so on
/// that arm a read is covered either by a filter entry or by that job. The reads resting on the second
/// are enumerated in <c>DarlingTestsBackstopped</c> (removed with the old CI jobs, #6656) with the bound each rests on, compared for
/// EQUALITY against what the filter could not reach, and honoured only while
/// <c>WholeTreeBackstopIsIntact</c> (removed with the old CI jobs, #6656) holds.</para>
/// </summary>
[Trait("Stage", "Guard")] // #5798: walks the repo tree, so the guard stage runs it
public class CrossAppGuardCiGateTests
{
    /* Which filter gates which suite, per build.yml's "Run Lite tests (shard)" / "Run Darling tests"
       steps. The Lite arm reads the SHARD job's step since #3887: the build job's own "Run Lite tests"
       is release-only now, and the step that decides whether the suite runs on a pull request is the
       one in the lite-tests matrix. Its filter block is a byte-for-byte copy of the build job's,
       renamed *_shard so each set is findable unambiguously and pinned equal by
       a test that went with the old CI jobs (#6656). */
    private const string LiteAppDir = "Lite";
    private const string LiteTestsDir = "Lite.Tests";
    private const string DarlingAppDir = "Darling";
    private const string DarlingTestsDir = "Darling/Darling.Tests";

    /* The two SKUs, each as the pair of trees it owns. Both arms are spelled the same way even though
       Darling's test project sits INSIDE Darling/ and its second root is therefore subsumed by its
       first: a redundant root costs nothing, and an arm written as a special case is how the next SKU
       inherits the sibling bug #3067 was filed for. */
    private static readonly SkuTrees LiteTrees = new(LiteAppDir, LiteTestsDir);
    private static readonly SkuTrees DarlingTrees = new(DarlingAppDir, DarlingTestsDir);

    /* Every item type that can carry a path to another app's file. Requested in one invocation, and
       floored PER TYPE against the synthetic fixture rather than against this repository: an item type
       MSBuild does not recognise returns an empty array and exit 0, so a typo here is indistinguishable
       on a tree where three of the six are legitimately empty. */
    private static readonly string[] EvaluatedItemTypes =
        { "Compile", "None", "Content", "EmbeddedResource", "Reference", "ProjectReference" };

    /* MSBuild's own answer to "which build files did you import without being named", which is the
       question a hand-rolled upward probe of Directory.Build.props / .targets was asking. It is also a
       wider answer than that probe: Directory.Packages.props is imported into every project here and is
       not one of the two names such a probe looks for. Empty string when there is none, which is how this
       repository reads for the first two. */
    private static readonly string[] EvaluatedProperties =
        { "MSBuildProjectFullPath", "DirectoryBuildPropsPath", "DirectoryBuildTargetsPath", "DirectoryPackagesPropsPath" };

    /* The build files among those properties - MSBuildProjectFullPath is the project itself and is the
       floor that a returned property set was actually populated. */
    private static readonly string[] ImportedBuildFileProperties =
        { "DirectoryBuildPropsPath", "DirectoryBuildTargetsPath", "DirectoryPackagesPropsPath" };

    /* A guard that hangs is a guard that never reports. Generous against a cold SDK on a CI runner and
       still far below any job timeout; measured at ~0.3s per project on a warm one. */
    private static readonly TimeSpan EvaluationTimeout = TimeSpan.FromMinutes(2);

    /// <summary>One SKU's two top-level trees.
    ///
    /// <para>A NAMED pair rather than two positions in a list, because two different things are taken
    /// off it and reading the wrong one is silent both ways. <see cref="Roots"/> is what a cross-app
    /// path is anchored on; <see cref="TestsDir"/> alone is what <c>Check</c> (removed with the old CI jobs, #6656)'s anti-vacuity
    /// floor is taken over — floored on the app tree instead, that floor would be satisfied by the
    /// references #3067 never lost.</para></summary>
    private sealed class SkuTrees
    {
        public SkuTrees(string appDir, string testsDir)
        {
            AppDir = appDir;
            TestsDir = testsDir;

            /* Longest root first. A required separator already stops "Lite" from claiming
               "Lite.Tests/X.cs" — the '.' is not a separator — and .NET's alternation backtracks into
               the longer arm regardless, so this ordering changes no answer today. It is here because a
               rewrite to a non-backtracking matcher, or to a per-root StartsWith over this list, would
               otherwise silently start letting the shorter root absorb the token under test, and the
               reference would vanish rather than be reported wrong. */
            Roots = new[] { appDir, testsDir }
                .OrderByDescending(r => r.Length)
                .ThenBy(r => r, StringComparer.Ordinal)
                .ToArray();
        }

        public string AppDir { get; }

        public string TestsDir { get; }

        public IReadOnlyList<string> Roots { get; }
    }

    [Fact]
    public void TheGlobMatcher_AgreesWithKnownAnswers()
    {
        /* Self-validation: these are the exact discriminations the assertions below depend on. If the
           matcher regressed, every coverage check would pass vacuously. */
        Assert.True(Matches("Lite/**/*.xaml", "Lite/Controls/ServerTab.xaml"));
        Assert.False(Matches("Lite/**/*.xaml", "Lite/Mcp/McpHealthTools.cs"));
        Assert.True(Matches("Lite/**/*.cs", "Lite/Mcp/McpHealthTools.cs"));
        Assert.True(Matches("Darling/Darling.Tests/**/!(*.md)", "Darling/Darling.Tests/Whatever.cs"));
        Assert.False(Matches("Darling/Darling.Tests/**/!(*.md)", "Darling/Darling.Tests/README.md"));
        Assert.True(Matches("Lite/**/!(*.md)", "Lite/Services/LocalDataService.cs"));

        /* The gap #2830 fell through: a Themes-only pattern does NOT reach the rest of the Viewer. */
        Assert.False(Matches(
            "Darling/PerformanceMonitor.Darling.Viewer/Themes/*.xaml",
            "Darling/PerformanceMonitor.Darling.Viewer/MainWindow.xaml"));
        Assert.True(Matches(
            "Darling/PerformanceMonitor.Darling.Viewer/**/!(*.md)",
            "Darling/PerformanceMonitor.Darling.Viewer/MainWindow.xaml"));

        Assert.True(Matches("README.md", "README.md"));
        Assert.False(Matches("README.md", "Lite/README.md"));
    }

    /// <summary>
    /// The C# half of the self-validation, and the discrimination #3067 turned on: a SIBLING test
    /// project is a tree of the other SKU, and a tree merely PREFIXED by a root's name is not.
    ///
    /// <para>Asserted as the whole ordered set rather than by containment, because both ways of getting
    /// this wrong are silent in a containment check. A matcher that lost the sibling root passes
    /// <c>Assert.Contains</c> on the three app-tree answers; a matcher widened into a prefix match
    /// passes it too, and starts requiring filter coverage for trees no SKU owns. Only the set says
    /// which of the two happened.</para>
    ///
    /// <para>Fed the shipped matcher, so this cannot agree with itself while <see cref="Scan"/> does
    /// something else.</para>
    /// </summary>
    [Fact]
    public void TheCSharpMatchers_SeeASiblingTestProject_AndNothingMerelyPrefixedByIt()
    {
        const string Text = """
            const string twin = "Lite.Tests/QueryHighDopStaleMaxDopParityTests.cs";
            var app = "Lite/Services/LocalDataService.cs";
            var win = @"Lite\Windows\WaitDrillDownWindow.xaml";
            var combined = Path.Combine("Lite.Tests", "Fixtures", "SystemHealth");
            var prefixedRoot = "LiteTests/NotOurs.cs";
            var longerRoot = "Lite.TestsExtra/NotOurs.cs";
            var sweep = new[] { "Lite", "Lite.Tests", "PerformanceMonitor.Common" }
                .Select(d => Path.Combine(root, d))
                .Where(Directory.Exists)
                .SelectMany(d => Directory.EnumerateFiles(d, "*.cs", SearchOption.AllDirectories));
            """;

        Assert.Equal(
            new[]
            {
                /* The sibling read #3067 was filed for, and it is FIRST in the text on purpose: the
                   app root is a PREFIX of the test root, so a matcher that let "Lite" claim these
                   characters would then require a separator where the '.' is and drop the reference
                   entirely rather than report it wrongly. */
                "Lite.Tests/QueryHighDopStaleMaxDopParityTests.cs",

                /* The app tree in both spellings, unchanged by the widening. */
                "Lite/Services/LocalDataService.cs",
                "Lite/Windows/WaitDrillDownWindow.xaml",

                /* Path.Combine's first argument is a root in its own right, and the sibling root has to
                   be one there too — the six-file XAML enumeration in the darling filter is exactly
                   this shape one directory over. Collected after the quoted paths, hence last. */
                "Lite.Tests/Fixtures/SystemHealth",

                /* #3076, and the reason the `sweep` line above is spelled exactly as
                   ControlPlaneReloadDurabilityTests spells it: both roots named bare, with no
                   separator on either, in a text that composes a whole path per collection element.
                   Both are found and they are found SEPARATELY — the test root is not folded into the
                   app root, which is what a shorter-root absorption would produce and what would then
                   drop Lite.Tests out of the set entirely. Longest root first, as Roots is ordered. */
                "Lite.Tests",
                "Lite",

                /* Absent, and each for its own reason: LiteTests/ shares no boundary with either
                   root, and Lite.TestsExtra/ is prefixed BY the longer root and is still not it. Both
                   would introduce a value nothing else in this set carries, so a widening into a
                   prefix match is visible here.

                   The spellings that remain UNFOUND after #3076 are NOT recorded here, deliberately.
                   This text carries a genuine sweep, so both roots are in the expected set already,
                   and arm three yields a root at most once per collection — an extra accepting site
                   could not change the set, and a case whose firing is invisible records nothing. They
                   are not asserted here: their firing is invisible in this set. */
            },
            CSharpPaths(Text, LiteTrees).ToArray());
    }

    /// <summary>
    /// #3076's arm, for BOTH SKUs and in one code path, with the fixture text DERIVED from each SKU's
    /// own tree names rather than written out per arm.
    ///
    /// <para>Two things need saying at once, and neither is provable from the live tree. The arm has to
    /// be live on the Lite arm as well as the Darling one — measured, <c>Lite.Tests</c> has no bare
    /// directory read of a Darling root today, so a floor taken over the live scan there would be
    /// satisfied by an arm that had stopped working entirely. And it has to be live on the DARLING SKU,
    /// whose test root contains a separator and is therefore already visible to the first arm, so an
    /// implementation that quietly special-cased "roots without a separator" would look identical here
    /// until the day Darling's test project moved out to a sibling.</para>
    ///
    /// <para>Both expectations are taken off <see cref="SkuTrees.AppDir"/> and
    /// <see cref="SkuTrees.TestsDir"/> — the NAMED members — while the matcher anchors on
    /// <see cref="SkuTrees.Roots"/>. A root dropped from <c>Roots</c> therefore reds here rather than
    /// disappearing from both sides of a comparison that derived them from the same list, which is how
    /// a fixture agrees with itself.</para>
    ///
    /// <para>The negative halves are the point as much as the positive one. Without the gate this arm
    /// reports every quoted mention of a root, and a root is quoted bare far more often than it names a
    /// tree: measured at #3076, 37 bare tokens in <c>Darling.Tests</c> of which 2 name a tree, and 21 in
    /// <c>Lite.Tests</c> of which none do. The Lite arm has no backstop to exempt the references all 21
    /// would produce.</para>
    /// </summary>
    [Fact]
    public void TheBareDirectoryArm_IsLiveForBothSkus()
    {
        var skus = new[] { LiteTrees, DarlingTrees };

        /* Every assertion below lives inside a loop, and a loop over nothing satisfies all of them.
           Floored at the number of SKUs this class gates a suite for, so a third one reds here and
           gets a case rather than inheriting whichever arm's behaviour it happens to land on — which
           is the shape of #3067, and the reason both arms are spelled identically to begin with. */
        Assert.Equal(2, skus.Length);

        foreach (var other in skus)
        {
            /* The live shape: a collection of bare roots, projected one element to one whole path. */
            var swept =
                $"var sweep = new[] {{ \"{other.AppDir}\", \"{other.TestsDir}\", \"PerformanceMonitor.Common\" }}\n"
                + "    .Select(d => Path.Combine(root, d))\n"
                + "    .SelectMany(d => Directory.EnumerateFiles(d, \"*.cs\", SearchOption.AllDirectories));\n";

            /* Both roots, once each. A root that already carries a separator is ALSO seen by the first
               arm, so it arrives twice; the duplicate is asserted rather than normalised away, and the
               claim being made is about the distinct set. */
            Assert.Equal(
                new[] { other.AppDir, other.TestsDir }.OrderBy(r => r, StringComparer.Ordinal),
                CSharpPaths(swept, other).Distinct().OrderBy(r => r, StringComparer.Ordinal));

            /* Anti-vacuity, stated separately because the equality above holds for an empty set the day
               someone writes an empty expectation next to an empty answer. */
            Assert.Contains(other.TestsDir, CSharpPaths(swept, other));
            Assert.Contains(other.AppDir, CSharpPaths(swept, other));

            /* The precision half: the SAME roots, bare, with no per-element composition anywhere in the
               text. This is the shape of 35 of the 37 bare Darling-arm tokens and of every one of the 21
               on the Lite arm, and the arm must see none of them. What those tokens ARE varies, and does
               not matter here: tuple labels and assertion keys, this class's own root constants, the
               FIRST argument of a Path.Combine the second arm resolves, and three that name a tree in a
               spelling no arm reaches. What
               this arm decides is only whether the token is projected one element to one whole path, and
               none of them is. Nothing else in the text carries a separator, so an empty answer here is
               the whole answer rather than the residue of one. */
            var prose =
                $"private const string AppDir = \"{other.AppDir}\";\n"
                + $"private const string TestsDir = \"{other.TestsDir}\";\n"
                + $"var scopes = new[] {{ (\"{other.AppDir}\", 1) }};\n"
                + $"Assert.Equal(0, perStore[\"{other.AppDir}\"]);\n";

            Assert.Equal(
                /* Except the one a root containing a separator hands to the FIRST arm, which is not
                   gated and never was. Derived, so the Lite SKU expects nothing and the Darling SKU
                   expects its test root, without either being written down as a special case. */
                new[] { other.AppDir, other.TestsDir }
                    .Where(r => r.Contains('/', StringComparison.Ordinal)),
                CSharpPaths(prose, other));

            /* The absorption red-proof, and the reason the gate insists the element be the LAST
               argument. This text carries a lambda, a Path.Combine and the element — everything the
               gate looks for except the element being the whole path — and the group before it must
               not absorb `root, d, "Fixtures"` into a match. If it did, every segment composition in
               the repository would turn its first segment into a whole-tree reference, which is the
               noisy widening #3076 exists to reject. */
            var segments =
                $"var files = new[] {{ \"{other.AppDir}\", \"{other.TestsDir}\" }}\n"
                + "    .Select(d => Path.Combine(root, d, \"Fixtures\"));\n";

            Assert.Equal(
                new[] { other.AppDir, other.TestsDir }
                    .Where(r => r.Contains('/', StringComparison.Ordinal)),
                CSharpPaths(segments, other));

            /* A collection element merely PREFIXED by a root is not that root, and it is a DIFFERENT
               tree. Elements are compared for equality rather than by prefix, so neither root can
               claim these. Written as its own case because equality and a prefix test agree on every
               element that IS a root — the two only diverge on an element like this one, so a fixture
               without it leaves the choice between them unpinned. The separator-carrying spelling is
               still reported by the FIRST arm, which anchors on a root followed by a separator and is
               right to: Darling/Darling.TestsExtra opens with the Darling root and a separator, so it
               IS a path under that root, unlike Lite.TestsExtra which is a sibling of one. */
            var prefixed =
                $"var files = new[] {{ \"{other.AppDir}Extra\", \"{other.TestsDir}Extra\" }}\n"
                + "    .Select(d => Path.Combine(root, d));\n";

            Assert.Equal(
                new[] { other.AppDir + "Extra", other.TestsDir + "Extra" }
                    .Where(r => r.Contains('/', StringComparison.Ordinal)),
                CSharpPaths(prefixed, other));
        }
    }

    /// <summary>
    /// The evaluated project read, driven against a synthetic two-SKU tree rather than this repository.
    ///
    /// <para><b>This is where <see cref="EvaluatedItemTypes"/> is floored, and it is the only place that
    /// can be.</b> An item type MSBuild does not recognise comes back as a present-but-empty array with
    /// exit 0 — pinned directly below — and <c>Content</c>, <c>EmbeddedResource</c> and <c>Reference</c>
    /// are all legitimately empty in this repository, so a typo in that list changes nothing any check on
    /// the real tree can see. The fixture carries one item of every requested type, so a type that stops
    /// being asked for, or is asked for by the wrong name, reds here.</para>
    ///
    /// <para><b>And it is the only place <c>HintPath</c> can be proven.</b> There is no <c>Reference</c>
    /// item anywhere in this repository, so the metadata route to a cross-app assembly has no live
    /// example; the fixture supplies one, written relatively, which is both the ordinary spelling and the
    /// one that needs resolving — MSBuild returns <c>HintPath</c> AS WRITTEN rather than rooted, unlike
    /// <c>FullPath</c>.</para>
    ///
    /// <para><b>The rest of the fixture is the reason this stage stopped parsing XML.</b> Each construct
    /// below is one an XML parse has to grow a rule for, and MSBuild already answers: a property, an item
    /// group behind a <c>Condition</c>, a <c>Choose</c>/<c>When</c>, an item DEFINED in an imported props
    /// file, an <c>@(Item-&gt;'…')</c> transform, and a wildcard. Each is asserted in both directions —
    /// the arm that holds is FOUND and the arm that does not is ABSENT — so a fixture that failed to build
    /// its tree cannot pass by finding nothing.</para>
    /// </summary>
    [Fact]
    public void TheEvaluatedRead_SurvivesEveryFormThatDefeatsAnXmlParse()
    {
        var fixture = Path.Combine(
            Path.GetTempPath(), "crossapp-getitem-" + Guid.NewGuid().ToString("n"));

        try
        {
            /* A miniature of this repository's shape: the consuming project under one SKU's test tree,
               the files it reaches under the other SKU's app tree AND its sibling test tree, so the
               #3067 anchor is exercised on the evaluated population too. */
            var project = WriteCrossAppFixture(fixture);

            var evaluation = Evaluate(project);
            AssertInvocationSucceeded(evaluation);

            /* The per-type floor. Every requested type present AND non-empty - key presence alone proves
               nothing, which the pin below measures. */
            var counts = string.Join(
                ", ", evaluation.ItemCounts.Select(kv => kv.Key + "=" + kv.Value));

            foreach (var type in EvaluatedItemTypes)
            {
                Assert.True(
                    evaluation.ItemCounts.TryGetValue(type, out var count) && count > 0,
                    $"the fixture declares one {type} item and MSBuild returned none of that type. An " +
                    "unrecognised item type answers with an EMPTY array and exit 0, so this is what a " +
                    $"typo in {nameof(EvaluatedItemTypes)} looks like. Counts: [{counts}]");
            }

            var found = CrossAppItems(fixture, evaluation, DarlingTrees)
                .Select(r => r.Rooted)
                .ToHashSet(StringComparer.Ordinal);

            /* HintPath, relative, reaching the other app - the route with no live example here. */
            Assert.Contains("Darling/PerformanceMonitor.Darling.Service/Hinted.dll", found);

            /* An Include= carrying a property, and one reaching the other SKU's SIBLING test tree. */
            Assert.Contains("Darling/Darling.Tests/FromProperty.cs", found);

            /* An item group behind a Condition that HOLDS, and the one behind a Condition that does not. */
            Assert.Contains("Darling/Darling.Tests/ConditionTrue.cs", found);
            Assert.DoesNotContain("Darling/Darling.Tests/ConditionFalse.cs", found);

            /* Choose/When, both arms. */
            Assert.Contains("Darling/Darling.Tests/ChosenWhen.cs", found);
            Assert.DoesNotContain("Darling/Darling.Tests/ChosenOtherwise.cs", found);

            /* An item DEFINED in an imported props file - invisible to any scan that opens only the
               project, and resolved by MSBuild against the CONSUMING project's directory. */
            Assert.Contains("Darling/Darling.Tests/FromImport.cs", found);

            /* An item transform. */
            Assert.Contains("Darling/Darling.Tests/Transformed.cs", found);

            /* A wildcard, which arrives as the concrete files it matched rather than as a directory to
               probe - so the sibling that does not match is absent. */
            Assert.Contains("Darling/Fixtures/Globbed.xml", found);
            Assert.DoesNotContain("Darling/Fixtures/NotGlobbed.txt", found);

            /* Origin attribution comes from MSBuild's own DefiningProjectFullPath, so the item defined in
               the imported file names THAT file rather than the project that consumed it. */
            var fromImport = CrossAppItems(fixture, evaluation, DarlingTrees)
                .Single(r => r.Rooted.Equals("Darling/Darling.Tests/FromImport.cs", StringComparison.Ordinal));
            Assert.Contains("Imported.props", fromImport.Origin, StringComparison.Ordinal);

            /* And the anchor still refuses a tree merely PREFIXED by a root, on this population too. */
            Assert.DoesNotContain("DarlingExtra/Nope.cs", found);
        }
        finally
        {
            if (Directory.Exists(fixture))
            {
                Directory.Delete(fixture, recursive: true);
            }
        }
    }

    /// <summary>
    /// The shell-out's failure modes, each pinned to fail LOUDLY rather than to return nothing.
    ///
    /// <para>This guard's success condition is an empty offender list, so every way of producing an empty
    /// answer without looking is a way of passing while broken. Three exist for a child process — it never
    /// started, it exited non-zero, or its output did not parse — and one exists for MSBuild itself: an
    /// item type it does not recognise answers with a present, EMPTY array and exit 0. That last one is
    /// why <see cref="EvaluatedItemTypes"/> cannot be floored against this repository, and it is measured
    /// here rather than assumed.</para>
    ///
    /// <para><see cref="Evaluation.Failure"/> carries the reason as data instead of throwing, so a caller
    /// asserts on it in the same shape as the rest of the population floors and the message arrives with
    /// the command line and the process output attached.</para>
    /// </summary>
    [Fact]
    public void TheEvaluatedRead_FailsLoudlyRatherThanReturningNothing()
    {
        var scratch = Path.Combine(
            Path.GetTempPath(), "crossapp-getitem-fail-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(scratch);

        try
        {
            /* A project that does not exist. */
            var missing = Evaluate(Path.Combine(scratch, "NoSuchProject.csproj"));
            Assert.NotEqual(0, missing.ExitCode);
            Assert.NotEqual(string.Empty, missing.Failure);
            Assert.Empty(missing.Items);

            /* A project whose XML does not load. Distinct from the case above: MSBuild reports this one
               on stderr and writes nothing at all to stdout, so a parse of an empty string is what a
               lenient reader would call "no items". */
            var malformed = Path.Combine(scratch, "Malformed.csproj");
            File.WriteAllText(
                malformed,
                "<Project Sdk=\"Microsoft.NET.Sdk\"><ItemGroup><Compile Include=</ItemGroup></Project>");
            var broken = Evaluate(malformed);
            Assert.NotEqual(0, broken.ExitCode);
            Assert.NotEqual(string.Empty, broken.Failure);
            Assert.Empty(broken.Items);

            /* Output that is not the expected JSON is a failure and not an empty answer. Driven through
               the shipped parser rather than through a process, because there is no invocation that
               produces this today - the point is that the reader does not treat it as zero items. */
            Assert.NotEqual(string.Empty, ParseEvaluation("Build succeeded.", out _, out _, out _));
            Assert.NotEqual(string.Empty, ParseEvaluation(string.Empty, out _, out _, out _));
            Assert.NotEqual(string.Empty, ParseEvaluation("{\"Properties\":{}}", out _, out _, out _));

            /* A property whose value is not a string. GetString() RAISES on one rather than returning
               null, and an exception out of this method would bypass the Failure floor its callers
               assert on - so the one shape that could still throw is the one pinned here. */
            Assert.NotEqual(
                string.Empty,
                ParseEvaluation(
                    "{\"Properties\":{\"MSBuildProjectFullPath\":123},\"Items\":{}}",
                    out _,
                    out _,
                    out _));

            /* And an answer missing a property that was asked for, which is a different failure from a
               malformed one: MSBuild returning fewer properties than requested would otherwise leave
               the build-file paths reading as "there are none". */
            Assert.NotEqual(
                string.Empty,
                ParseEvaluation("{\"Properties\":{},\"Items\":{}}", out _, out _, out _));

            /* And an item type MSBuild does not recognise: exit 0, the key present, the array empty. This
               is the shape of a typo in EvaluatedItemTypes, and the reason the fixture pin above floors
               every type by name. */
            var probe = Path.Combine(scratch, "Probe.csproj");
            File.WriteAllText(
                probe,
                "<Project Sdk=\"Microsoft.NET.Sdk\"><PropertyGroup>"
                + "<TargetFramework>net10.0</TargetFramework><EnableDefaultItems>false</EnableDefaultItems>"
                + "</PropertyGroup><ItemGroup><Compile Include=\"Real.cs\" /></ItemGroup></Project>");

            var (exitCode, stdout, stderr) = RunDotnet(
                scratch,
                new[] { "msbuild", probe, "-getItem:Compile;NoSuchItemTypeAtAll" });

            Assert.True(
                exitCode == 0,
                $"the probe invocation itself failed, so nothing below is measuring MSBuild's answer to an "
              + $"unknown item type. exit={exitCode}, stderr={Truncate(stderr)}, stdout={Truncate(stdout)}");

            using var doc = JsonDocument.Parse(stdout);
            var items = doc.RootElement.GetProperty("Items");
            Assert.Equal(1, items.GetProperty("Compile").GetArrayLength());
            Assert.Equal(0, items.GetProperty("NoSuchItemTypeAtAll").GetArrayLength());

            /* The fourth way a child process returns nothing: it never exits. Driven with a deadline no
               real invocation can beat rather than by waiting out the shipped one - the same probe
               project answers in about a third of a second above, so a millisecond cannot be a flake in
               the passing direction. A hung child has to FAIL rather than hang the suite, which for a
               guard is the same as being deleted. */
            var (hungExit, hungOut, hungError) = RunDotnet(
                scratch,
                new[] { "msbuild", probe, "-getItem:Compile" },
                TimeSpan.FromMilliseconds(1));

            Assert.NotEqual(0, hungExit);
            Assert.Contains("did not exit within", hungError, StringComparison.Ordinal);
            Assert.Equal(string.Empty, hungOut);
        }
        finally
        {
            TryDeleteTree(scratch);
        }
    }

    /// <summary>
    /// The evaluated set has to CONTAIN what a text scan of the same XML finds, not merely differ from it.
    ///
    /// <para>A replacement that found different things rather than more things would read as an
    /// improvement while having moved the blind spot, so <see cref="ParsedProjectXmlPaths"/> stays as a
    /// deliberately crude second opinion — attribute values, element text, and a comment's
    /// double-quoted values and <c>&gt;</c>-to-<c>&lt;</c> spans, resolved against the project directory
    /// and anchored the same way. It is not the live route and is not a good one: it over-reads (a path
    /// inside a comment counts, quoted or not) and under-reads (anything needing evaluation
    /// disappears). Both are the safe direction for a lower bound, and
    /// <b>both are held by <see cref="TheCrudeParse_SeesEveryLiteralValueHoweverItIsQuoted"/></b> rather
    /// than by this sentence — which is #3140: this paragraph and the one on
    /// <see cref="ParsedProjectXmlPaths"/> both asserted a direction of error that was false, an
    /// apostrophe in a comment having silently deleted whole ItemGroups from the crude answer, and
    /// nothing anywhere checked either claim.</para>
    ///
    /// <para><b>Superset of nothing is free</b>, which is the way this pin passes while measuring nothing
    /// at all. So the crude side is floored twice: run against the arm's OWN trees it must find something
    /// (proving the parse read the file and can resolve and anchor a path), and on the Lite arm the
    /// cross-app answer is floored BY NAME on the linked compile #3063 was filed about — the one live
    /// cross-app project item in this repository. <c>Darling.Tests</c>' project names no file under
    /// <c>Lite</c> or <c>Lite.Tests</c> at all, so its cross-app answer is legitimately empty on both
    /// sides and only the own-tree floor is available there.</para>
    /// </summary>
    [Fact]
    public void TheEvaluatedSet_ContainsEverythingAParseOfTheSameXmlFinds()
    {
        var repo = RepoRoot();

        var arms = new[]
        {
            (Project: LiteTestsDir, Own: LiteTrees, Other: DarlingTrees,
                Floor: $"{DarlingTestsDir}/CSharpSourceWalker.cs"),
            (Project: DarlingTestsDir, Own: DarlingTrees, Other: LiteTrees, Floor: (string?)null),
        };

        foreach (var (project, own, other, floor) in arms)
        {
            var projectRoot = Path.Combine(repo, project.Replace('/', Path.DirectorySeparatorChar));
            var projectFiles = TrackedFiles(projectRoot, "*.csproj").ToList();
            Assert.True(projectFiles.Count > 0, $"no project file under {project} to parse");

            var parsedOwn = new SortedSet<string>(StringComparer.Ordinal);
            var parsedCross = new SortedSet<string>(StringComparer.Ordinal);
            foreach (var file in projectFiles)
            {
                var xml = File.ReadAllText(file);
                var dir = Path.GetDirectoryName(file)!;
                parsedOwn.UnionWith(ParsedProjectXmlPaths(repo, dir, xml, own));
                parsedCross.UnionWith(ParsedProjectXmlPaths(repo, dir, xml, other));
            }

            /* The crude side works at all: it read the file, resolved a relative path and anchored it. */
            Assert.True(
                parsedOwn.Count > 0,
                $"the XML parse found no path under {project}'s OWN trees, so it is not reading the file " +
                "and every comparison below is superset-of-nothing");

            var evaluated = new HashSet<string>(StringComparer.Ordinal);
            foreach (var file in projectFiles)
            {
                var evaluation = Evaluate(file);
                AssertInvocationSucceeded(evaluation);
                evaluated.UnionWith(CrossAppItems(repo, evaluation, other).Select(r => r.Rooted));
            }

            if (floor is not null)
            {
                /* Which SIDE lost the floor is the whole diagnosis, and a bare Assert.Contains names
                   neither: these two sat adjacent and message-less, so #3138 read a crude-parse failure
                   here as an evaluated-side regression and went looking in build.yml, which was
                   byte-identical to dev. The crude side is asserted first because it is the PRECONDITION
                   — if the second opinion cannot see the floor, the comparison below is measuring
                   nothing — but it now says so. */
                Assert.True(
                    parsedCross.Contains(floor),
                    $"the CRUDE parse of {project} did not find {floor}, so the SECOND OPINION is broken " +
                    "rather than the evaluated read, and the superset comparison below is measuring " +
                    $"nothing. Nothing about {nameof(Evaluate)} is implicated. Look at " +
                    $"{nameof(ParsedProjectXmlPaths)}: #3140 was an apostrophe in a comment swallowing " +
                    "the markup after it. Parsed cross-app: " +
                    $"[{string.Join(", ", parsedCross)}]");

                Assert.True(
                    evaluated.Contains(floor),
                    $"the EVALUATED read of {project} does not report {floor}, which the crude parse of " +
                    "the same XML does see. This is the defect this guard exists to catch: the live " +
                    "population has lost a cross-app project item that is spelled literally in the " +
                    "project file. Evaluated: [" +
                    string.Join(", ", evaluated.OrderBy(path => path, StringComparer.Ordinal)) + "]");
            }

            var missed = parsedCross.Except(evaluated, StringComparer.Ordinal).ToList();
            Assert.True(
                missed.Count == 0,
                $"the evaluated read of {project} does not contain what a parse of the same XML sees: " +
                string.Join(", ", missed) + ". Either the evaluated read has moved the blind spot rather " +
                "than closed it, or MSBuild is correctly excluding an item the crude parse over-read — a " +
                "cross-app path behind a Condition that never holds, or inside a comment. Both want a " +
                "human: decide whether that path is a read worth covering" +
                $". Evaluated: [{string.Join(", ", evaluated.OrderBy(p => p, StringComparer.Ordinal))}]");
        }
    }

    /// <summary>
    /// The crude second opinion sees every literal the project XML spells, however that literal is
    /// quoted and whatever prose sits above it — which is what its own doc claimed, twice, while being
    /// false (#3140).
    ///
    /// <para><b>The property.</b> Every path the XML names LITERALLY — as an attribute value, as element
    /// text, or inside a comment — is in <see cref="ParsedProjectXmlPaths"/>' answer, and the only paths
    /// missing from it are ones MSBuild has to evaluate before they are a path at all. A floor whose gaps
    /// depend on where an apostrophe landed is not a floor, and
    /// <see cref="TheEvaluatedSet_ContainsEverythingAParseOfTheSameXmlFinds"/> compares the live
    /// population against it.</para>
    ///
    /// <para><b>Derived from how that property can BECOME false, rather than from reading the code.</b>
    /// A literal goes missing exactly one way: something in the text is taken for a delimiter and the
    /// span it opens swallows the literal. Enumerating what can be taken for one gives the arms below —
    /// each on its OWN document with its OWN file name, so a broken arm reds by name rather than hiding
    /// behind a sibling that still passes.</para>
    ///
    /// <para><b>Two arms are the live defect, measured rather than supposed.</b> An apostrophe in one
    /// comment paired with an apostrophe in the NEXT comment and deleted everything between them: on
    /// <c>dev</c> that hid the whole <c>None</c> item carrying <c>Fixtures\SystemHealth\*.xml</c> out of
    /// <c>Lite.Tests</c>' own project, and four <c>None</c> items — about two kilobytes of markup — out of
    /// <c>Darling.Tests</c>'. <b>Parity was never the invariant, position relative to the item was</b>,
    /// which is why an EVEN count and an ODD count are both here: the two-apostrophe arm loses its item,
    /// and the file that lost the most carries an even six. A guard that counted apostrophes would have
    /// called the worse of the two clean.</para>
    ///
    /// <para><b>Prose in element TEXT was not the vector, and pinning that is the point.</b> The
    /// <c>&gt;</c>-to-<c>&lt;</c> arm of that regex consumed <c>&gt;Erik's tool&lt;</c> before the
    /// apostrophe arm could open on it, so the vector was a comment — the one construct that arm cannot
    /// enter, since a comment's body carries no <c>&gt;</c> before its own terminator. The element-text
    /// arm below therefore passed before the fix as well as after it. It stays because it is the
    /// difference between this diagnosis and the nearest plausible wrong one, and because a fix that
    /// stripped apostrophes wholesale would break it.</para>
    ///
    /// <para><b>Three arms nothing here could ever see</b> are a single-quoted attribute value, an
    /// entity-encoded separator and a CDATA body. The first is why the apostrophe arm was not simply
    /// deleted: MSBuild accepts <c>Include='…'</c>, so deleting it trades this defect for a quieter one
    /// of the same shape. The other two are literal paths a text scan has no way to spell.</para>
    ///
    /// <para><b>Both documented directions are held here rather than described.</b> A path inside a
    /// comment still counts, in the quoted spelling and the element-text one; a value carrying
    /// <c>$(</c> still falls out. Each negative arm is paired with a literal in the SAME document
    /// asserted present, so an absence cannot be the absence of a read. The one residual miss — a
    /// SINGLE-quoted attribute inside a comment — is asserted that way beside a double-quoted sibling in
    /// the same comment, so the stated limitation is a measurement and not a hope.</para>
    /// </summary>
    [Fact]
    public void TheCrudeParse_SeesEveryLiteralValueHoweverItIsQuoted()
    {
        /* Nothing here touches the disk. The crude read resolves and anchors by path arithmetic alone, so
           a rooted repository that does not exist measures exactly what a real one does - and a fixture
           that cannot be half-written is one fewer way for this to pass while measuring nothing. */
        var repo = Path.Combine(Path.GetTempPath(), "crossapp-crude-" + Guid.NewGuid().ToString("n"));
        var projectDir = Path.Combine(repo, LiteTestsDir);

        SortedSet<string> Found(string body) => new(
            ParsedProjectXmlPaths(
                repo,
                projectDir,
                "<Project Sdk=\"Microsoft.NET.Sdk\">" + body + "</Project>",
                DarlingTrees),
            StringComparer.Ordinal);

        void Sees(string route, string leaf, string body)
        {
            var found = Found(body);
            Assert.True(
                found.Contains($"{DarlingTestsDir}/{leaf}"),
                $"the crude parse cannot see a literal it must ({route}): {leaf} is spelled in the XML " +
                $"and is not in the answer. Found: [{string.Join(", ", found)}]");
        }

        void DoesNotSee(string route, string leaf, string present, string body)
        {
            var found = Found(body);

            /* The positive half first, in the same document: an absence measured against an empty answer
               is a pin that cannot fail. */
            Assert.True(
                found.Contains($"{DarlingTestsDir}/{present}"),
                $"the document for '{route}' answered without {present}, which it spells literally, so " +
                $"the absence asserted next is the absence of a READ. Found: [{string.Join(", ", found)}]");

            Assert.False(
                found.Contains($"{DarlingTestsDir}/{leaf}"),
                $"the crude parse now sees {leaf} ({route}), which its doc says it does not. That is the " +
                "one direction a lower bound may not move: an over-read makes this a partial oracle for " +
                $"the evaluated set rather than a floor under it. Found: [{string.Join(", ", found)}]");
        }

        /* THE LIVE DEFECT. An apostrophe in one comment, another in the next, the item between them - and
           an EVEN count, which is the shape a parity check calls clean. */
        Sees(
            "an apostrophe in one comment paired with one in the next, EVEN count",
            "EvenApostropheCount.cs",
            @"<!-- Darling.Tests' fixtures -->
              <ItemGroup>
                <Compile Include=""..\Darling\Darling.Tests\EvenApostropheCount.cs"" />
              </ItemGroup>
              <!-- Lite's copy -->");

        /* The same defect at an ODD count, so neither parity is mistaken for the invariant. */
        Sees(
            "the same pairing at an ODD count",
            "OddApostropheCount.cs",
            @"<!-- Darling.Tests' fixtures -->
              <ItemGroup>
                <Compile Include=""..\Darling\Darling.Tests\OddApostropheCount.cs"" />
              </ItemGroup>
              <!-- Lite's copy, and Darling's -->");

        /* Prose in element text, which the old read survived - so a fix that went after apostrophes
           rather than after the grammar reds here. */
        Sees(
            "an apostrophe in element text",
            "AfterProse.cs",
            @"<PropertyGroup><Notice>Erik's tool</Notice></PropertyGroup>
              <ItemGroup>
                <Compile Include=""..\Darling\Darling.Tests\AfterProse.cs"" />
              </ItemGroup>
              <PropertyGroup><Notice>Lite's copy</Notice></PropertyGroup>");

        /* MSBuild accepts this spelling, so deleting the apostrophe arm was not the fix. */
        Sees(
            "a single-quoted attribute value",
            "SingleQuoted.cs",
            @"<ItemGroup><Compile Include='..\Darling\Darling.Tests\SingleQuoted.cs' /></ItemGroup>");

        /* Four apostrophes inside one double-quoted value: the ordinary spelling of a Condition, and the
           reason a check on balanced quoting would fire on files that are correct. */
        Sees(
            "apostrophes inside a double-quoted Condition",
            "AfterCondition.cs",
            @"<ItemGroup Condition=""'$(Configuration)' != ''"">
                <Compile Include=""..\Darling\Darling.Tests\AfterCondition.cs"" />
              </ItemGroup>");

        /* A literal path a text scan reads as nonsense and MSBuild reads as a path. */
        Sees(
            "an entity-encoded separator",
            "EntityEncoded.cs",
            @"<ItemGroup>
                <Compile Include=""..&#x5C;Darling&#x5C;Darling.Tests&#x5C;EntityEncoded.cs"" />
              </ItemGroup>");

        Sees(
            "a CDATA body",
            "Cdata.cs",
            @"<ItemGroup><None><![CDATA[..\Darling\Darling.Tests\Cdata.cs]]></None></ItemGroup>");

        /* The documented OVER-read, both spellings. A commented-out cross-app path counts, which is the
           position ImportElements takes for the same reason: a construct arriving unnoticed. */
        Sees(
            "a commented-out item, quoted",
            "CommentedOut.cs",
            @"<!-- <Compile Include=""..\Darling\Darling.Tests\CommentedOut.cs"" /> -->");

        Sees(
            "a commented-out item, element text",
            "CommentedText.cs",
            @"<!-- <None>..\Darling\Darling.Tests\CommentedText.cs</None> -->");

        /* The documented UNDER-read: a value carrying $( is not a path and falls out. */
        DoesNotSee(
            "a value needing evaluation",
            "NeedsEvaluation.cs",
            "Literal.cs",
            @"<PropertyGroup><Reach>..\Darling\Darling.Tests</Reach></PropertyGroup>
              <ItemGroup>
                <Compile Include=""$(Reach)\NeedsEvaluation.cs"" />
                <Compile Include=""..\Darling\Darling.Tests\Literal.cs"" />
              </ItemGroup>");

        /* The one residual miss, stated in the doc and measured here: inside a comment there is no markup
           to parse, and dropping the apostrophe arm is what makes prose harmless. */
        DoesNotSee(
            "a single-quoted attribute inside a comment",
            "CommentedSingle.cs",
            "CommentedDouble.cs",
            @"<!-- <Compile Include=""..\Darling\Darling.Tests\CommentedDouble.cs"" />
                   <Compile Include='..\Darling\Darling.Tests\CommentedSingle.cs' /> -->");

        /* And a project that does not parse fails LOUDLY. A catch here would answer SHORT, which is the
           defect this whole test is about one layer down: an under-read nobody is told about. The
           document below spells a cross-app path and must still return none of it. */
        Assert.Throws<XmlException>(() =>
        {
            _ = Found(@"<ItemGroup><Compile Include=""..\Darling\Darling.Tests\Unclosed.cs"" />");
        });
    }

    /// <summary>
    /// An <c>&lt;Import&gt;</c>'s own path is the one cross-app read the evaluated population cannot
    /// report, so it is gated rather than evaluated — and the gate is pinned in both directions here.
    ///
    /// <para>Items an imported file DEFINES arrive with everything else, which is the interesting half;
    /// the imported file's own path is not an item and no <c>-getItem</c> or <c>-getProperty</c> answer
    /// carries it. There is no <c>&lt;Import&gt;</c> element in any project file in this repository, so a
    /// gate that fails the moment one appears costs three lines where an evaluator costs a rule per
    /// construct.</para>
    ///
    /// <para>It flags a commented-out import too. That is the right direction for a gate whose whole
    /// purpose is to stop a construct arriving unnoticed, and the message says what to do about it.</para>
    /// </summary>
    [Fact]
    public void TheImportGate_SeesAnImportElementAndNothingMerelyNamedLikeOne()
    {
        Assert.Equal(
            new[] { "<Import Project=\"..\\Darling\\Shared.targets\" />" },
            ImportElements("<Project><Import Project=\"..\\Darling\\Shared.targets\" /></Project>"));

        /* The Sdk-attribute and Sdk-element spellings resolve through MSBuild's SDK resolver rather than a
           path, and an Import inside a comment is still an Import arriving unnoticed. */
        Assert.Equal(
            new[] { "<Import Project=\"$(P)\" />" },
            ImportElements("<Project><!-- <Import Project=\"$(P)\" /> --></Project>"));

        /* And nothing merely named like one: an item type, an attribute, or a property whose name opens
           with the same six characters. */
        Assert.Empty(ImportElements(
            "<Project><ItemGroup><ImportedThing Include=\"x\" /></ItemGroup>"
            + "<PropertyGroup><ImportDirectoryBuildProps>true</ImportDirectoryBuildProps></PropertyGroup>"
            + "</Project>"));

        /* The live tree, which is what makes the gate cheap. Read through the shipped scan so the two
           cannot disagree. */
        var repo = RepoRoot();
        foreach (var (project, other) in new[]
        {
            (LiteTestsDir, DarlingTrees),
            (DarlingTestsDir, LiteTrees),
        })
        {
            var scan = Scan(repo, project, other);
            Assert.Empty(scan.UnreportedImports);
        }
    }

    /// <summary>
    /// A reference counts only when the path it resolved to spells it back, on every host.
    ///
    /// <para><c>Directory.Exists</c> answers according to the HOST's path rules, and Windows trims
    /// trailing periods from a path component: a <c>Darling</c> segment followed by three dots resolves
    /// to <c>Darling</c> there and to nothing on macOS or Linux. This guard runs on Windows in CI and
    /// gets verified on macOS, and the divergence is not hypothetical — a three-dot path written
    /// illustratively inside a comment in THIS file was matched by the C# stage, resolved on Windows
    /// only, and failed the coverage check in a way no local run could reproduce. Every case below
    /// answers identically on all three platforms.</para>
    /// </summary>
    [Fact]
    public void TheOnDiskProbe_AnswersTheSameOnEveryHost()
    {
        var repo = RepoRoot();

        Assert.NotNull(OnDisk(repo, DarlingTestsDir));
        Assert.NotNull(OnDisk(repo, DarlingTestsDir + "/CSharpSourceWalker.cs"));

        /* Trailing periods: Windows resolves this to the Darling directory, other hosts to nothing.
           Neither is a read of anything, so both must say so. */
        Assert.Null(OnDisk(repo, "Darling/..."));

        /* A dot segment resolves everywhere, and still is not the path it was written as. */
        Assert.Null(OnDisk(repo, DarlingTestsDir + "/."));

        Assert.Null(OnDisk(repo, "Darling/NoSuchArea/Fixtures"));
    }

    /// <summary>
    /// The population is what failed in #3063, so it is pinned directly rather than left implied.
    ///
    /// <para>The collector enumerated <c>*.cs</c> only, so a cross-app read expressed as
    /// <c>&lt;Compile Include="..\Darling\Darling.Tests\X.cs" /&gt;</c> was invisible — the file holding it
    /// was never opened. A widened collector that reaches no project items passes exactly as the narrow one
    /// did, which would be the same defect committed by its own fix. So the linked compile that reference was
    /// filed about is required in the evaluated set BY NAME rather than merely counted, and the two item
    /// types a test project cannot honestly be empty of are floored by count.</para>
    ///
    /// <para><b>Which types can be floored on this tree is not a matter of taste.</b> <c>Content</c>,
    /// <c>EmbeddedResource</c> and <c>Reference</c> are all genuinely empty in both projects, and an item
    /// type MSBuild does not recognise is ALSO empty — so a floor over those three would either red on a
    /// clean tree or say nothing. <c>Compile</c> and <c>ProjectReference</c> are structural for a test
    /// project that compiles and references its own SKU, so those two carry the count floor and
    /// <see cref="TheEvaluatedRead_SurvivesEveryFormThatDefeatsAnXmlParse"/> carries the other four.</para>
    ///
    /// <para><see cref="ImportedBuildFileProperties"/> is asserted by VALUE on all three names.
    /// <c>Directory.Build.props</c> carries the repository's one product-version declaration (#3222) and
    /// <c>Directory.Packages.props</c> the central package versions, so both are imported into every
    /// project here; there is no <c>Directory.Build.targets</c>, and that name reading empty is itself the
    /// assertion. A two-name upward probe of <c>Directory.Build.*</c> is exactly the answer that misses the
    /// packages file. What ALSO stays floored is that the property set came back populated at all, through
    /// <c>MSBuildProjectFullPath</c> naming the project that was asked — so "MSBuild says there are
    /// none" is distinguishable from "no properties were returned", which three empty strings would
    /// read as either.</para>
    /// </summary>
    [Fact]
    public void ThePopulationReachesTheProjectItems_AndNotOnlyTheCSharp()
    {
        var repo = RepoRoot();

        var expectations = new[]
        {
            (Project: LiteTestsDir, Other: DarlingTrees, Manifest: $"{LiteTestsDir}/Lite.Tests.csproj",
                /* #3059's linked compile, and the reference #3063 exists for. */
                Named: (string?)$"{DarlingTestsDir}/CSharpSourceWalker.cs"),
            (Project: DarlingTestsDir, Other: LiteTrees, Manifest: $"{DarlingTestsDir}/Darling.Tests.csproj",
                /* #3938's linked compile, the mirror image of the one above: Darling.Tests compiles the
                   cross-SKU source-scan pins out of Lite.Tests so they run on a Mac. */
                Named: (string?)$"{LiteTestsDir}/CrossSkuSurfaceSourceTests.cs"),
        };

        foreach (var (project, other, manifest, named) in expectations)
        {
            var scan = Scan(repo, project, other);

            Assert.True(scan.CSharpFiles > 0, $"the C# stage opened no files under {project}");

            Assert.True(
                scan.ProjectFiles.Contains(manifest, StringComparer.Ordinal),
                $"the MSBuild stage did not evaluate {manifest}, so an Include= naming {other.AppDir} there " +
                "would " +
                $"be invisible. Opened: [{string.Join(", ", scan.ProjectFiles)}]");

            /* Build output holds a copy of the project file on any machine that has built the suite; a
               scan that read it would assert on an artifact, and on this repo's own CI that copy is the
               one a stale build left behind. */
            Assert.DoesNotContain(
                scan.ProjectFiles,
                p => p.Contains("/bin/", StringComparison.Ordinal) ||
                     p.Contains("/obj/", StringComparison.Ordinal));

            /* The manifest's OWN evaluation, located by name rather than by position: a floor read off
               whichever project sorted first would answer about the wrong one the moment a second
               project file appears under the same tree. */
            var evaluated = Assert.Single(
                scan.Evaluated,
                e => Rooted(repo, e.Project).Equals(manifest, StringComparison.Ordinal));

            AssertInvocationSucceeded(evaluated);

            /* The two types a test project cannot honestly be empty of. */
            foreach (var structural in new[] { "Compile", "ProjectReference" })
            {
                Assert.True(
                    evaluated.ItemCounts.TryGetValue(structural, out var count) && count > 0,
                    $"{project} evaluated to no {structural} items, which no test project does — the " +
                    "invocation answered about something other than this project. Counts: " +
                    $"[{string.Join(", ", evaluated.ItemCounts.Select(kv => kv.Key + "=" + kv.Value))}]");
            }

            /* The property set is populated, established through the one property whose value is known
               before the call: MSBuild answered about the project that was asked. Without this, three
               empty build-file paths read the same whether MSBuild found none or returned nothing. */
            Assert.Equal(
                Path.GetFullPath(Path.Combine(repo, manifest.Replace('/', Path.DirectorySeparatorChar))),
                evaluated.Properties.TryGetValue("MSBuildProjectFullPath", out var self) ? self : null);

            /* MSBuild's own answer for the build files it imports unnamed. Directory.Build.props carries
               the one product-version declaration (#3222) and Directory.Packages.props the central
               package versions; there is no Directory.Build.targets, which is why the middle name reads
               empty. Asserting all three by VALUE is what a two-name upward probe of the
               Directory.Build names cannot do — it never looks for the packages file at all, and it
               cannot tell "MSBuild imported none" from "MSBuild answered nothing". */
            Assert.Equal(
                new[]
                {
                    Path.Combine(repo, "Directory.Build.props"),
                    string.Empty,
                    Path.Combine(repo, "Directory.Packages.props"),
                },
                ImportedBuildFileProperties
                    .Select(p => evaluated.Properties.TryGetValue(p, out var v) ? v : "<absent>")
                    .ToArray());

            if (named is not null)
            {
                Assert.True(
                    scan.References.Any(r =>
                        r.Raw.Equals(named, StringComparison.Ordinal) &&
                        r.Origin.Contains(manifest, StringComparison.Ordinal)),
                    $"{manifest} declares a linked compile of {named} and the evaluated read did not " +
                    "attribute it there, so the project-item population is not reaching the file that " +
                    "carries it. Found: " +
                    $"[{string.Join(", ", scan.References.Select(r => r.Raw + " <- " + r.Origin))}]");
            }
        }
    }

    [Fact]
    public void TheLiteFilters_NameTheShardScriptAndItsPacker()
    {
        var yaml = ReadBuildYaml(RepoRoot());

        foreach (var filter in new[] { "lite", "lite_shard", "darling", "darling_full" })
        {
            var patterns = FilterPatterns(yaml, filter);
            Assert.True(patterns.Count > 0, $"build.yml's '{filter}' filter is gone");
            Assert.Contains(".github/scripts/run-lite-shard.ps1", patterns);
            if (filter.StartsWith("lite", StringComparison.Ordinal))
            {
                Assert.Contains(".github/scripts/lite-shard-pack.py", patterns);
            }
            else
            {
                Assert.Contains(".github/scripts/run-darling-pg-shard.ps1", patterns);
            }
        }
    }

    /// <summary>What one scan found, carried together with what it opened to find it. One value rather
    /// than a bare reference list, because a found set is not interpretable without the population that
    /// produced it — an empty population and an empty answer are the same green.</summary>
    private readonly record struct CrossAppScan(
        int CSharpFiles,
        IReadOnlyList<string> ProjectFiles,
        IReadOnlyList<Evaluation> Evaluated,
        IReadOnlyList<string> UnreportedImports,
        IReadOnlyList<(string Raw, string Probe, string Origin)> References);

    /// <summary>One <c>dotnet msbuild -getItem -getProperty</c> answer, carried with the evidence needed
    /// to tell a real empty answer from a failed one.
    ///
    /// <para><see cref="Failure"/> is the empty string when the process ran and its output parsed, and a
    /// reason otherwise. Carried as data rather than thrown so a caller floors it in the same shape as
    /// every other population floor here, and so the message can arrive with the command line and the
    /// process output attached.</para></summary>
    private readonly record struct Evaluation(
        string Project,
        string CommandLine,
        int ExitCode,
        string Failure,
        string Diagnostics,
        IReadOnlyDictionary<string, int> ItemCounts,
        IReadOnlyList<EvaluatedItem> Items,
        IReadOnlyDictionary<string, string> Properties);

    /// <summary>One evaluated item, reduced to what a cross-app decision needs.
    ///
    /// <para><see cref="Path"/> is absolute and already normalised by MSBuild — for most item types from
    /// <c>FullPath</c>, and for a <c>Reference</c> from its <c>HintPath</c> metadata resolved against the
    /// project directory, because MSBuild returns that one AS WRITTEN. <see cref="DefinedIn"/> is
    /// MSBuild's <c>DefiningProjectFullPath</c>, which attributes an item to the file that actually
    /// declared it rather than to the project that consumed it.</para></summary>
    private readonly record struct EvaluatedItem(
        string ItemType,
        string Via,
        string Identity,
        string Path,
        string DefinedIn);

    /// <summary>A collection literal whose elements are each projected to a WHOLE path below a base,
    /// rather than to one segment of one:
    /// <c>new[] { "Lite", "Lite.Tests" }.Select(d =&gt; Path.Combine(root, d))</c>.
    ///
    /// <para><b>This is what makes a bare directory name a path, and it is the whole discrimination
    /// #3076 turns on.</b> <c>"Lite"</c> and <c>"Darling"</c> are quoted bare all over both test
    /// projects — as tuple labels, as assertion keys, as this class's own root constants — and the
    /// majority of the ones that touch a path at all are the FIRST SEGMENT of a longer literal
    /// composition, <c>Path.Combine("Lite", "Services", "X.cs")</c>, which
    /// <see cref="CSharpPaths"/>' second arm already resolves to the specific file. Reporting
    /// <c>Lite</c> for one of those would replace a precise reference with a whole tree, which is a
    /// LOSS of information, not a gain. Here the element is the last argument, so there is no further
    /// literal segment and the element is the entire path — a bare root in that position names the
    /// tree.</para>
    ///
    /// <para><b>The collection is matched together with the projection, not looked for anywhere in the
    /// same file.</b> A file-wide gate reads well and self-reports immediately: this class declares
    /// <c>DarlingAppDir = "Darling"</c> and carries a fixture text containing the projection, so a
    /// gate satisfied file-wide turned THIS file into a bare-directory read of <c>Darling</c> that the
    /// <c>lite</c> filter cannot reach — on the arm that deliberately has no exemption list. The bare
    /// root has to be an ELEMENT of the collection being projected, which is a local fact and cannot
    /// be manufactured by a fixture one member over. The element is compared for EQUALITY against the
    /// root rather than searched for as a quoted substring, so the shorter root cannot claim the
    /// longer one's characters here by any amount of backtracking.</para>
    ///
    /// <para>Any method taking the lambda, not <c>.Select</c> specifically: <c>.SelectMany</c> carries
    /// the same projection, and an enumerated list of LINQ operators is how the next spelling gets
    /// missed. The base may itself be a call — <c>Path.Combine(RepoRoot(), d)</c> — so one level of
    /// nesting is allowed inside it, matched as balanced pairs rather than "anything up to the next
    /// <c>)</c>", which would end the argument list inside the nested call.</para>
    ///
    /// <para><b>The element must be the LAST argument.</b>
    /// <c>Path.Combine(root, d, "Fixtures")</c> is a segment composition wearing a lambda, and the
    /// group before the element cannot absorb its way into matching one: after consuming
    /// <c>root, d, "Fixtures"</c> there is no <c>, d)</c> left to match, and every shorter split fails
    /// the same way. Pinned in <see cref="TheBareDirectoryArm_IsLiveForBothSkus"/>, because a group
    /// that DID absorb the token under test is exactly how a regex pin passes while blind.</para></summary>
    private static readonly Regex WholePathPerElement = new(
        @"(?:new\[\]\s*\{|\[)(?<elements>[^{}\[\]]*)[\}\]]\s*\.\w+\(\s*(?<id>\w+)\s*=>\s*"
        + @"Path\.Combine\((?:[^()]|\([^()]*\))*,\s*\k<id>\s*\)",
        RegexOptions.Compiled);

    /// <summary>The paths one C# file's text names that belong to <paramref name="other"/>, in the
    /// three spellings this repository's pins use, repo-rooted and forward-slashed.
    ///
    /// <para><b>The anchor is every root the SKU owns, which is #3067.</b> Anchored on the app directory
    /// alone, <c>Lite.Tests/X.cs</c> matched neither <c>Lite/</c> nor anything else and left the found
    /// set silently — the failure direction this whole class exists to close. A root still has to be
    /// followed by a SEPARATOR, so a tree merely PREFIXED by a root's name (<c>Lite.TestsExtra/</c>) is
    /// no more a read of that SKU than it was before.</para>
    ///
    /// <para><b>A directory named without a separator is still a path, which is #3076.</b>
    /// <c>ControlPlaneReloadDurabilityTests</c> sweeps <c>new[] { "Lite", "Lite.Tests", … }</c> through
    /// <c>Path.Combine(root, d)</c> and <c>Directory.EnumerateFiles</c>, so it reads Lite's whole tree
    /// AND Lite's whole test tree, and neither string carries the separator the first two arms need.
    /// Accepting a bare root on the strength of the STRING alone is what would make that fix worse than
    /// the miss. An on-disk-existence filter discriminates NOTHING here whatever the token count is: a
    /// root is a real directory by construction, so every bare token survives it. Only the
    /// used-as-a-directory shape narrows the population, and measured at #3076 it took 37 bare tokens in
    /// <c>Darling.Tests</c> down to 2 and 21 in <c>Lite.Tests</c> down to none. Zero is the right answer
    /// on the Lite arm — no bare <c>Darling</c> token there is projected one element to one whole path —
    /// and a string-only anchor would instead have manufactured a <c>Darling</c> reference that the
    /// <c>lite</c> filter cannot reach, on the one arm that deliberately has no exemption list to record
    /// it in.</para>
    ///
    /// <para><b>Three syntactic shapes is the BOUNDARY, and #3082 settled it as a decision rather than
    /// a gap.</b> Each widening found the next spelling it could not see — a project item (#3063), a
    /// sibling test tree (#3067), a bare directory (#3076), a root literal that is not
    /// <c>Path.Combine</c>'s first argument (#3082) — so the question stopped being what the next
    /// widening fixes and became where the sequence ends. It does not end soon: the corpus this scans
    /// names a cross-app file in SEVEN shapes no arm here reaches, each one a live instance in the corpus.</para>
    ///
    /// <para><b>What a fifth widening would actually buy, measured rather than estimated.</b> Take the
    /// widest regex this arm could plausibly become — the root literal at ANY <c>Path.Combine</c>
    /// argument position including the last, with one level of call nesting allowed in the base — and run
    /// it over the census and over this repository. It reaches three of the seven shapes, and it is
    /// WRONG on one of them: <c>SegmentsSplitAcrossTheHelperBoundary</c> resolves to a bare <c>Lite</c>,
    /// reporting the whole tree for a read that names one directory, because the segment after the root
    /// is a variable. That is the same precision trade #3076 refused. And on the live tree it
    /// manufactures a reference nothing reads: <c>ParitySource</c> locates the repo root by probing
    /// <c>Directory.Exists</c> on a root name, which the widened arm reports as a whole-tree read of
    /// <c>Darling</c> — on the Lite arm, which has no exemption list and no backstop job, so
    /// <c>EveryCrossAppSourceRead_IsReachableByTheFilterThatGatesItsSuite</c> (removed with the old CI jobs, #6656) fails and the only
    /// ways to green it are to put <c>Darling/**</c> in the <c>lite</c> filter or to grow this arm one
    /// more carve-out. Precision is bought with structure, and the bill arrives again each time.</para>
    ///
    /// <para><b>The four shapes no regex reaches at all compose a path across MEMBERS</b>, which is the
    /// reason the sequence has no natural end: a segment array declared as a field and spread into
    /// <c>Path.Combine</c>'s params overload somewhere else, an inline collection expression whose
    /// recomposition happens two members away through a record property, a variadic helper with no file
    /// API in the expression at all. Joining those requires resolving what a user-defined method does
    /// with its arguments — constant-value and data-flow analysis over a compilation, which is a
    /// semantic pass and not a wider pattern. So this matcher stops at what one file's text says
    /// LOCALLY, and reports the rest instead of pretending to see it.</para>
    ///
    /// <para><b>Reporting is the whole remaining job, and it is not a consolation prize.</b> Every
    /// unseen instance in the corpus resolves to a tree the filter gating its suite already reaches: the
    /// Darling arm's land under the derived source entry for Lite's app tree, or under the
    /// <c>Lite.Tests</c> tree that <c>DarlingTestsBackstopped</c> (removed with the old CI jobs, #6656) already exempts, and the Lite
    /// arm's two land under the Viewer and Service trees the <c>lite</c> filter names outright. So as
    /// with #3063 and #3067 the gap is in what this guard can SAY, not in what CI runs — and what a
    /// fifth widening buys is a NAME for a coverage decision that is already correct.</para>
    ///
    /// <para>Shared by the walk and by the pin that reads it back, so a retyped copy in a test cannot
    /// agree with itself while the matcher that ships does something else.</para></summary>
    private static IEnumerable<string> CSharpPaths(string text, SkuTrees other)
    {
        var anyRoot = "(?:" + string.Join("|", other.Roots.Select(Regex.Escape)) + ")";

        /* "Darling/Some/Path.cs" and the backslash spelling some Windows-facing pins use. */
        foreach (Match m in Regex.Matches(text, "\"(" + anyRoot + "[/\\\\][^\"]+)\""))
        {
            yield return m.Groups[1].Value.Replace('\\', '/');
        }

        /* Path.Combine("Darling", "Darling.Tests", "X.cs"). Per root rather than through the alternation
           above: the first argument is a whole literal here, so two roots cannot both match one call. */
        foreach (var root in other.Roots)
        {
            foreach (Match m in Regex.Matches(
                text, @"Path\.Combine\(\s*""" + Regex.Escape(root) + @"""\s*,([^)]*)\)"))
            {
                var segments = Regex.Matches(m.Groups[1].Value, "\"([^\"]+)\"")
                    .Select(s => s.Groups[1].Value)
                    .ToArray();
                if (segments.Length > 0)
                {
                    yield return root + "/" + string.Join("/", segments);
                }
            }
        }

        /* #3076: "Lite" on its own, as an element of a collection projected one element to one whole
           path. Elements are compared for EQUALITY against each root, so the shorter root cannot claim
           the longer one's characters — "Lite" is simply not the string "Lite.Tests". A root that
           CONTAINS a separator (Darling/Darling.Tests) is found by the first arm as well and
           unconditionally; the duplicate is expected rather than normalised away, so a later change
           that starts collapsing yields reds and gets read. Note() keys by reference, so a duplicate
           costs the scan nothing. */
        foreach (Match m in WholePathPerElement.Matches(text))
        {
            var elements = Regex.Matches(m.Groups["elements"].Value, "\"([^\"]+)\"")
                .Select(s => s.Groups[1].Value)
                .ToArray();

            foreach (var root in other.Roots)
            {
                if (elements.Contains(root, StringComparer.Ordinal))
                {
                    yield return root;
                }
            }
        }
    }

    /// <summary>Repo-relative paths naming a tree of <paramref name="other"/> that a test in
    /// <paramref name="project"/> reads, paired with a concrete file path to test coverage against.</summary>
    private static CrossAppScan Scan(string repo, string project, SkuTrees other)
    {
        /* Keyed by the reference, valued by every file that named it, so a failure says where to go. */
        var seen = new SortedDictionary<string, SortedSet<string>>(StringComparer.Ordinal);
        var projectRoot = Path.Combine(repo, project.Replace('/', Path.DirectorySeparatorChar));
        if (!Directory.Exists(projectRoot))
        {
            throw new DirectoryNotFoundException($"test project not found: {project}");
        }

        var csharpFiles = 0;
        foreach (var file in TrackedFiles(projectRoot, "*.cs"))
        {
            csharpFiles++;
            var origin = Rooted(repo, file);

            foreach (var path in CSharpPaths(File.ReadAllText(file), other))
            {
                Note(seen, path, origin);
            }
        }

        /* The second population, and the one #3063 was filed for. No C# matcher can see a linked
           compile: the path is relative and backslashed, so the root anchor never fires — but that is
           downstream of the real reason, which is that project XML is not a *.cs file and was never
           opened. MSBuild evaluates it here rather than this file parsing it. */
        var projectFiles = TrackedFiles(projectRoot, "*.csproj")
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToList();

        if (projectFiles.Count == 0)
        {
            /* Loud rather than an empty Evaluation carried forward: every floor below reads a population,
               and a zero population that arrived by not looking is the failure this guard exists for. */
            throw new FileNotFoundException($"no project file under {project} to evaluate");
        }

        /* EVERY project file, not the first of them. There is one under each test project today, and
           taking only one would be a silent miss the moment a second arrives - the read would answer
           about whichever sorted first and say nothing about the other. */
        var evaluations = projectFiles.Select(f => Evaluate(f)).ToList();

        foreach (var evaluation in evaluations.Where(e => e.Failure.Length == 0))
        {
            foreach (var (rooted, origin) in CrossAppItems(repo, evaluation, other))
            {
                Note(seen, rooted, origin);
            }
        }

        /* The gate for the construct the evaluated answer does not carry. Read over the project's own
           files AND over whichever build files MSBuild says it imported unnamed, since an Import in one
           of those is imported into this project just the same. */
        var unreportedImports = new List<string>();
        var importCandidates = projectFiles.Concat(
            evaluations
                .SelectMany(e => ImportedBuildFileProperties.Select(
                    p => e.Properties.TryGetValue(p, out var v) ? v : string.Empty))
                .Where(v => v.Length > 0 && File.Exists(v))
                .Distinct(StringComparer.Ordinal));

        foreach (var file in importCandidates)
        {
            foreach (var element in ImportElements(File.ReadAllText(file)))
            {
                unreportedImports.Add($"{Rooted(repo, file)}: {element}");
            }
        }

        return new CrossAppScan(
            csharpFiles,
            projectFiles.Select(f => Rooted(repo, f)).ToList(),
            evaluations,
            unreportedImports,
            Resolve(repo, seen).ToList());
    }

    /// <summary>Every file that names a reference is recorded, not just the first. The two populations
    /// can name the same path — a linked compile and a pin that reads the linked file both do — and a
    /// failure that reported only one of them would say the reference came from the C# and leave the
    /// project entry looking unread.</summary>
    private static void Note(SortedDictionary<string, SortedSet<string>> seen, string raw, string origin)
    {
        if (!seen.TryGetValue(raw, out var origins))
        {
            origins = new SortedSet<string>(StringComparer.Ordinal);
            seen[raw] = origins;
        }

        origins.Add(origin);
    }

    /// <summary>Files of one pattern under <paramref name="root"/>, minus build output — that carries
    /// copies of product source, and scanning it would assert on artifacts.</summary>
    private static IEnumerable<string> TrackedFiles(string root, string pattern) =>
        Directory.EnumerateFiles(root, pattern, SearchOption.AllDirectories)
            .Where(f =>
                !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal) &&
                !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal));

    /// <summary>What <c>dotnet msbuild -getItem -getProperty</c> answers about one project.
    ///
    /// <para>The answer is FULLY EVALUATED: properties expanded, imports followed, <c>Condition</c>s
    /// applied, wildcards expanded, and MSBuild's own <c>Directory.Build.props</c> probe already
    /// performed. Nothing is built and no <c>obj/</c> is written — evaluation reads the project and its
    /// import closure and stops — so this is offline and costs about a third of a second per project.</para>
    ///
    /// <para><c>-p:EnableWindowsTargeting=true</c> is what lets a non-Windows host work with this
    /// project's <c>net10.0-windows</c> target framework at all. Evaluation alone measures fine without
    /// it, but it is a no-op on the Windows runner and removes a host-dependent difference from a guard
    /// that has to answer the same way in CI and on a macOS verification run.</para>
    ///
    /// <para><see cref="Evaluation.Failure"/> rather than an exception, because the caller is asserting on
    /// a population: the three ways this returns nothing without having looked are floored by
    /// <see cref="AssertInvocationSucceeded"/> in the same shape as every other floor here.</para></summary>
    private static Evaluation Evaluate(string projectPath)
    {
        var arguments = new[]
        {
            "msbuild",
            projectPath,
            "-getItem:" + string.Join(";", EvaluatedItemTypes),
            "-getProperty:" + string.Join(";", EvaluatedProperties),
            "-p:EnableWindowsTargeting=true",
        };

        var commandLine = "dotnet " + string.Join(" ", arguments);
        /* Not run from the repository: the working directory is where the dotnet host looks for global.json, and the CI
           legs run on the runner image's SDK, an older patch than the one global.json pins (#7098), so a host
           started in the repository refuses to run at all. The project path is absolute, and MSBuild finds its
           Directory.Build.props from the project's own folder, so the answer does not depend on the directory. */
        var (exitCode, stdout, stderr) = RunDotnet(Path.GetTempPath(), arguments);
        var diagnostics = $"exit={exitCode}, stderr={Truncate(stderr)}, stdout={Truncate(stdout)}";

        if (exitCode != 0)
        {
            return new Evaluation(
                projectPath, commandLine, exitCode, "the invocation exited non-zero", diagnostics,
                new Dictionary<string, int>(StringComparer.Ordinal),
                Array.Empty<EvaluatedItem>(),
                new Dictionary<string, string>(StringComparer.Ordinal));
        }

        var failure = ParseEvaluation(stdout, out var items, out var counts, out var properties);

        return new Evaluation(
            projectPath, commandLine, exitCode, failure, diagnostics, counts, items, properties);
    }

    /// <summary>The JSON <c>-getItem</c> / <c>-getProperty</c> answer, or a reason it is not one.
    ///
    /// <para>Strict on purpose. Output that is not the expected shape is a FAILURE and not an empty item
    /// set: a lenient reader that shrugged at a banner, an error summary or a truncated stream would
    /// report "no cross-app references" for a run that never answered the question, which is strictly
    /// worse than the parse this replaces.</para>
    ///
    /// <para>Separate from the process call so the malformed-output cases can be pinned without an
    /// invocation that produces them.</para></summary>
    private static string ParseEvaluation(
        string stdout,
        out IReadOnlyList<EvaluatedItem> items,
        out IReadOnlyDictionary<string, int> counts,
        out IReadOnlyDictionary<string, string> properties)
    {
        items = Array.Empty<EvaluatedItem>();
        counts = new Dictionary<string, int>(StringComparer.Ordinal);
        properties = new Dictionary<string, string>(StringComparer.Ordinal);

        if (stdout.Length == 0)
        {
            return "the invocation wrote nothing to stdout";
        }

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(stdout);
        }
        catch (JsonException error)
        {
            return "the invocation's output is not JSON: " + error.Message;
        }

        using (document)
        {
            if (!document.RootElement.TryGetProperty("Items", out var itemsElement) ||
                itemsElement.ValueKind != JsonValueKind.Object)
            {
                return "the invocation's output carries no Items object";
            }

            /* Properties first, because the item pass needs one of them. */
            var readProperties = new SortedDictionary<string, string>(StringComparer.Ordinal);
            if (document.RootElement.TryGetProperty("Properties", out var propertiesElement) &&
                propertiesElement.ValueKind == JsonValueKind.Object)
            {
                foreach (var property in propertiesElement.EnumerateObject())
                {
                    /* Reported rather than thrown, because everything else about a malformed answer
                       here is - GetString() on a non-string raises instead, and an exception out of
                       this method bypasses the Failure floor its callers assert on.

                       And reported rather than coerced to the empty string the way item METADATA is:
                       empty is MSBuild's own answer for "there is no Directory.Build.props", so
                       flattening an unexpected shape into one would manufacture that answer instead of
                       admitting the read did not happen. */
                    if (property.Value.ValueKind != JsonValueKind.String)
                    {
                        return $"the invocation's output carries a non-string {property.Name} property";
                    }

                    readProperties[property.Name] = property.Value.GetString() ?? string.Empty;
                }
            }

            foreach (var expected in EvaluatedProperties)
            {
                if (!readProperties.ContainsKey(expected))
                {
                    return $"the invocation's output carries no {expected} property";
                }
            }

            /* MEASURED, and the two candidates diverge: MSBuild resolves a relative HintPath against the
               CONSUMING PROJECT's directory, not against the directory of the file that declared the
               Reference. A Reference declared in a Directory.Build.props one level up, whose HintPath
               names a file beside THAT props file, does not resolve; the same HintPath naming a file
               beside the consuming project does. Getting this backwards is a silent miss - the path
               resolves somewhere real-looking, fails the cross-app anchor and vanishes. */
            var projectDirectory =
                Path.GetDirectoryName(readProperties["MSBuildProjectFullPath"]) ?? string.Empty;

            var collected = new List<EvaluatedItem>();

            /* Counted from MSBuild's own arrays rather than from the paths taken out of them, because
               the two answer different questions and only the first is what a per-type floor is for: an
               item type MSBuild does not recognise returns an EMPTY array, so the count is the evidence
               the NAME is real. The extraction is asserted separately, by what lands in the found set -
               and one item can yield two paths, a Reference with a HintPath being exactly that, so a
               count derived from the extraction would read as two items where the project declared one. */
            var readCounts = new SortedDictionary<string, int>(StringComparer.Ordinal);

            foreach (var type in itemsElement.EnumerateObject())
            {
                if (type.Value.ValueKind != JsonValueKind.Array)
                {
                    return $"the Items entry for {type.Name} is not an array";
                }

                readCounts[type.Name] = type.Value.GetArrayLength();

                foreach (var element in type.Value.EnumerateArray())
                {
                    var identity = Metadata(element, "Identity");
                    var declaredIn = Metadata(element, "DefiningProjectFullPath");

                    /* FullPath is MSBuild's own rooted, normalised answer for the item's identity. */
                    var fullPath = Metadata(element, "FullPath");
                    if (fullPath.Length > 0)
                    {
                        collected.Add(
                            new EvaluatedItem(type.Name, "FullPath", identity, fullPath, declaredIn));
                    }

                    /* HintPath is the path a Reference actually names, and MSBuild hands it back AS
                       WRITTEN rather than rooted - unlike FullPath, whose value for a Reference is the
                       assembly NAME resolved under the project directory and means nothing. */
                    var hintPath = Metadata(element, "HintPath");
                    if (hintPath.Length == 0)
                    {
                        continue;
                    }

                    var resolved = Absolute(projectDirectory, hintPath);
                    if (resolved is not null)
                    {
                        collected.Add(
                            new EvaluatedItem(type.Name, "HintPath", identity, resolved, declaredIn));
                    }
                }
            }

            /* A requested type absent from the answer entirely reads as zero rather than as a missing
               key: the two are the same defect and only one of them is legible in a message. */
            foreach (var type in EvaluatedItemTypes)
            {
                if (!readCounts.ContainsKey(type))
                {
                    readCounts[type] = 0;
                }
            }

            items = collected;
            counts = readCounts;
            properties = readProperties;
            return string.Empty;
        }
    }

    private static string Metadata(JsonElement element, string name) =>
        element.ValueKind == JsonValueKind.Object &&
        element.TryGetProperty(name, out var value) &&
        value.ValueKind == JsonValueKind.String
            ? value.GetString() ?? string.Empty
            : string.Empty;

    /// <summary>The three ways the evaluated read returns nothing without having looked, asserted
    /// separately from anything about WHAT it returned.
    ///
    /// <para>This guard passes on an empty offender list, so an invocation that never ran produces the
    /// same green as a clean tree. Read from <c>Check</c> (removed with the old CI jobs, #6656) as well as from the pins, so a floor
    /// cannot hold in one and be missing from the other.</para></summary>
    private static void AssertInvocationSucceeded(in Evaluation evaluation)
    {
        Assert.True(
            evaluation.ExitCode == 0,
            $"the evaluated project read exited non-zero, so it reports no items for a reason that has " +
            $"nothing to do with the tree. {evaluation.CommandLine}\n  {evaluation.Diagnostics}");

        Assert.True(
            evaluation.Failure.Length == 0,
            $"the evaluated project read did not answer: {evaluation.Failure}. " +
            $"{evaluation.CommandLine}\n  {evaluation.Diagnostics}");

        Assert.True(
            evaluation.Items.Count > 0,
            $"the evaluated project read returned no items at all, which no project does. " +
            $"{evaluation.CommandLine}\n  {evaluation.Diagnostics}");
    }

    /// <summary><c>dotnet</c> with both streams drained concurrently and a deadline.
    ///
    /// <para>Reading one stream to completion before the other deadlocks on an output this size — the
    /// item answer for the larger project runs past half a megabyte — and a child that never exits would
    /// hang the suite rather than fail it, which for a guard is the same as being deleted.</para>
    ///
    /// <para>Launched by name rather than through a resolved host path: <c>DOTNET_HOST_PATH</c> is not set
    /// for a process started by <c>dotnet run</c>, which is how this suite runs. A missing <c>dotnet</c>
    /// on PATH surfaces as a non-zero exit with the exception text, so it fails loudly rather than as an
    /// empty answer.</para></summary>
    private static (int ExitCode, string StdOut, string StdErr) RunDotnet(
        string workingDirectory, IEnumerable<string> arguments, TimeSpan? deadline = null)
    {
        /* A parameter with the shipped value as its default, so the timeout branch can be exercised at
           all: the real deadline cannot be waited out in a test, and what the pin is about is what
           happens WHEN it expires rather than how long it is. */
        var timeout = deadline ?? EvaluationTimeout;

        var startInfo = new ProcessStartInfo("dotnet")
        {
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        foreach (var argument in arguments)
        {
            startInfo.ArgumentList.Add(argument);
        }

        /* Anything the CLI would print ahead of the JSON turns a valid answer into a parse failure. The
           parse stays strict regardless - this only removes the two banners that are switchable. */
        startInfo.Environment["DOTNET_NOLOGO"] = "1";
        startInfo.Environment["DOTNET_CLI_TELEMETRY_OPTOUT"] = "1";

        try
        {
            using var process = Process.Start(startInfo);
            if (process is null)
            {
                return (-1, string.Empty, "dotnet did not start");
            }

            var stdout = process.StandardOutput.ReadToEndAsync();
            var stderr = process.StandardError.ReadToEndAsync();

            if (!process.WaitForExit((int)timeout.TotalMilliseconds))
            {
                try
                {
                    process.Kill(entireProcessTree: true);
                }
                catch (Exception error) when (error is InvalidOperationException
                    or NotSupportedException
                    or System.ComponentModel.Win32Exception
                    or AggregateException)
                {
                    /* The kill is best-effort and its failure is not the finding: the deadline already
                       is, and it is returned below either way. All four are races or refusals rather
                       than defects here - the process exited between the deadline and the kill, it is
                       remote, the OS refused to terminate it, or (the one Kill(entireProcessTree)
                       documents for itself) some descendant could not be killed. Letting any of them
                       out would replace "dotnet did not exit within 2 minutes" with a kill-failure
                       stack trace, on the path that is hardest to diagnose from a CI log. */
                }

                /* No output, and the message says so rather than leaving an empty stdout to be read as
                   an answer: the reader tasks are abandoned with the process. */
                return (-1, string.Empty, $"dotnet did not exit within {timeout}, so no output was collected");
            }

            return (process.ExitCode, stdout.GetAwaiter().GetResult(), stderr.GetAwaiter().GetResult());
        }
        catch (System.ComponentModel.Win32Exception error)
        {
            return (-1, string.Empty, "dotnet could not be started: " + error.Message);
        }
    }

    /// <summary>
    /// Deletes a scratch tree and treats a locked file as cleanup, not as a verdict.
    ///
    /// <para>The timeout case above starts <c>dotnet msbuild</c> with a one-millisecond budget precisely so
    /// the run does NOT finish, so the abandoned process and its MSBuild nodes can still hold handles inside
    /// the tree when this runs. On Windows the recursive delete then throws and fails a test whose assertions
    /// have all already passed — the outcome reports a locked temp file as a guard regression. Same shape and
    /// same reasoning as <c>DarlingDeployRollbackRetentionTests.TryDeleteTree</c>; separate assemblies, so the
    /// helper cannot be shared.</para>
    /// </summary>
    private static void TryDeleteTree(string path)
    {
        if (!Directory.Exists(path))
        {
            return;
        }

        try
        {
            Directory.Delete(path, recursive: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* A leftover temp tree is not a test failure. */
        }
    }

    /// <summary>The evaluated items and imported build files that name a tree of
    /// <paramref name="other"/>, repo-rooted, each paired with the file MSBuild says declared it.
    ///
    /// <para>The build files MSBuild locates itself are checked too: <c>$(DirectoryBuildPropsPath)</c> can
    /// point outside the project's own tree, and a <c>Directory.Build.props</c> under another SKU is a
    /// read of it whatever its item groups say.</para></summary>
    private static IEnumerable<(string Rooted, string Origin)> CrossAppItems(
        string repo, Evaluation evaluation, SkuTrees other)
    {
        foreach (var item in evaluation.Items)
        {
            var rooted = CrossAppPath(repo, item.Path, other);
            if (rooted is not null)
            {
                yield return (rooted, $"{Declarer(repo, item.DefinedIn)} ({item.ItemType} {item.Via})");
            }
        }

        foreach (var name in ImportedBuildFileProperties)
        {
            if (!evaluation.Properties.TryGetValue(name, out var path) || path.Length == 0)
            {
                continue;
            }

            var rooted = CrossAppPath(repo, path, other);
            if (rooted is not null)
            {
                yield return (rooted, $"{Rooted(repo, evaluation.Project)} (${name})");
            }
        }
    }

    /// <summary>The file that declared an item, for a message.
    ///
    /// <para>MSBuild attributes an item to the file that declared it, which for the SDK's default globs is
    /// a <c>.props</c> inside the installed SDK. Reduced to its file name when it is outside the
    /// repository — the absolute path is a machine path on a public repository's CI log, and the name is
    /// the whole of what a reader needs.</para></summary>
    private static string Declarer(string repo, string definedIn)
    {
        if (definedIn.Length == 0)
        {
            return "<unattributed>";
        }

        var rooted = Rooted(repo, definedIn);
        return Path.IsPathRooted(rooted) ? Path.GetFileName(rooted) : rooted;
    }

    /// <summary>The repo-rooted spelling of an absolute path that names a tree belonging to
    /// <paramref name="other"/>, or null when it does not.
    ///
    /// <para>The anchor is every root the SKU owns, which is #3067: a linked compile reaching a SIBLING
    /// test project resolves to <c>Lite.Tests/X.cs</c>, which an anchor on <c>Lite</c> alone rejected
    /// because it neither equals <c>Lite</c> nor opens with <c>Lite/</c>. A root still has to be followed
    /// by a SEPARATOR, so a tree merely PREFIXED by a root's name is no more a read of that SKU than
    /// before.</para></summary>
    private static string? CrossAppPath(string repo, string absolute, SkuTrees other)
    {
        if (absolute.Length == 0 || !Path.IsPathRooted(absolute))
        {
            return null;
        }

        var prefix = Path.GetFullPath(repo).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
        if (!absolute.StartsWith(prefix, StringComparison.Ordinal))
        {
            /* Outside the tree, so no path filter could name it. */
            return null;
        }

        var rooted = absolute[prefix.Length..].Replace(Path.DirectorySeparatorChar, '/');

        return other.Roots.Any(root =>
            rooted.Equals(root, StringComparison.Ordinal)
            || rooted.StartsWith(root + "/", StringComparison.Ordinal))
                ? rooted
                : null;
    }

    /// <summary><c>Path.GetFullPath</c> that answers null instead of throwing on a value that is not a
    /// path — a wildcard is not a legal path component on every host, and an item identity is not always
    /// a path at all.</summary>
    private static string? Absolute(string baseDirectory, string relative)
    {
        try
        {
            return Path.GetFullPath(Path.Combine(
                baseDirectory,
                relative.Replace('\\', Path.DirectorySeparatorChar).Replace('/', Path.DirectorySeparatorChar)));
        }
        catch (ArgumentException)
        {
            return null;
        }
    }

    /// <summary>Every <c>&lt;Import&gt;</c> element one MSBuild file's XML carries.
    ///
    /// <para>The one cross-app read the evaluated population does not report: items an imported file
    /// DEFINES arrive with the rest, but the imported file's own path is not an item and no
    /// <c>-getProperty</c> answer carries it. So this is a gate rather than a reader — it says that the
    /// construct is present, not where it points — and there are none in this repository, which is what
    /// makes a gate the cheap answer.</para>
    ///
    /// <para>A commented-out import counts. A gate exists to stop a construct arriving unnoticed, and a
    /// reader that skipped comments would be re-implementing the XML grammar to be MORE permissive, which
    /// is the wrong direction twice.</para></summary>
    private static IEnumerable<string> ImportElements(string xml) =>
        Regex.Matches(xml, "<Import\\s[^>]*/?>", RegexOptions.None)
            .Select(m => m.Value);

    /// <summary>The cross-app paths a crude read of one project file finds: every attribute value, every
    /// element's text, and — inside a comment — every double-quoted value and every
    /// <c>&gt;</c>-to-<c>&lt;</c> span, each resolved against the project directory and anchored the same
    /// way the evaluated read is.
    ///
    /// <para>Not the live route, and deliberately not a good one. It exists so the switch to MSBuild can
    /// be asserted rather than asserted-about: the evaluated set has to CONTAIN what this finds, or the
    /// replacement has moved the blind spot rather than closed it.</para>
    ///
    /// <para><b>Crude means UNEVALUATED, not mis-parsed</b>, which is #3140. This read was a regex
    /// alternation over the raw text, and its single-quote arm treated an apostrophe ANYWHERE — in a
    /// comment, in prose — as an attribute delimiter: it paired with the next apostrophe and every
    /// literal between them went missing. So it under-read arbitrary literals, which is exactly what the
    /// two paragraphs claiming it only under-reads what needs evaluation had ruled out, and it was doing
    /// so live — hiding one <c>None</c> item in this project and four in <c>Darling.Tests</c>' at the
    /// moment it was found. The quoting grammar is <c>XDocument</c>'s problem now, because a regex that
    /// gets XML quoting right is most of an XML parser and the rest of that parser is what
    /// <see cref="Evaluate"/> exists to avoid writing.</para>
    ///
    /// <para><b>Parity of apostrophes was never the invariant; position relative to the paths was.</b> A
    /// check on the count would have called the worse of those two files clean — <c>Darling.Tests</c>'
    /// project carries an EVEN six apostrophes and still lost two kilobytes of markup, four
    /// <c>None</c> items inside it, because what decides the damage is whether a real <c>Include</c> sits
    /// between a bogus pair rather than how many apostrophes the file has.</para>
    ///
    /// <para>Crude in both directions still, and both are the safe direction for a lower bound. It
    /// over-reads — a path inside a comment, or behind a <c>Condition</c> that never holds, counts — and
    /// it under-reads anything needing evaluation, since a value carrying <c>$(</c> resolves to no real
    /// path and falls out. A lower bound that is sometimes too low is a floor; one that is ever too high
    /// is an oracle. <b>Both directions are now HELD</b> by
    /// <see cref="TheCrudeParse_SeesEveryLiteralValueHoweverItIsQuoted"/> rather than asserted here:
    /// #3140 was an unenforced claim before it was a false one, and the claim being false for two years
    /// with every gate green is what an unenforced claim buys.</para></summary>
    private static IEnumerable<string> ParsedProjectXmlPaths(
        string repo, string projectDir, string xml, SkuTrees other)
    {
        foreach (var raw in LiteralXmlValues(xml))
        {
            if (raw.Length == 0)
            {
                continue;
            }

            var absolute = Absolute(projectDir, raw);
            if (absolute is null)
            {
                continue;
            }

            var rooted = CrossAppPath(repo, absolute, other);
            if (rooted is not null)
            {
                yield return rooted;
            }
        }
    }

    /// <summary>Every literal value one MSBuild file's XML carries, before any of it is treated as a
    /// path: each attribute's value, each element's text, and — inside each comment — every double-quoted
    /// value and every <c>&gt;</c>-to-<c>&lt;</c> span. Quoted is not the whole of what a comment
    /// contributes, which is what makes the commented-out ELEMENT-TEXT arm of
    /// <see cref="TheCrudeParse_SeesEveryLiteralValueHoweverItIsQuoted"/> pass.
    ///
    /// <para><b>Attribute values come from the XML parser rather than from a delimiter guess</b>, so
    /// <c>Include='..\Darling\X.cs'</c> is read as readily as the double-quoted spelling, an apostrophe
    /// in <c>Condition="'$(X)' == 'y'"</c> is part of a value rather than the start of one, and
    /// <c>&amp;#x5C;</c> arrives decoded — which is what MSBuild itself sees. A malformed project throws
    /// <see cref="XmlException"/> and is deliberately NOT caught: a fallback here would answer short
    /// rather than loudly, which is the defect #3140 was, one layer down.</para>
    ///
    /// <para><b>A comment is scanned as text, on purpose.</b> A path inside a comment counting is the
    /// over-read this read's whole doc rests on and the position
    /// <see cref="ImportElements"/> takes for the same reason — a commented-out construct is a construct
    /// arriving unnoticed — so a reader that skipped comments would be narrowing a documented guarantee
    /// while claiming to fix a bug. Inside a comment there is no markup to parse, only the text of the
    /// markup somebody removed, so a double-quoted value and a <c>&gt;</c>-to-<c>&lt;</c> span are the
    /// whole of what is available. The one thing that scan cannot see is a SINGLE-quoted attribute inside
    /// a comment: dropping the apostrophe arm is what makes prose harmless, and a commented-out
    /// single-quoted <c>Include</c> is the price. That is a miss confined to commented-out markup and
    /// stated rather than discovered.</para></summary>
    private static IEnumerable<string> LiteralXmlValues(string xml)
    {
        var document = XDocument.Parse(xml);

        foreach (var node in document.DescendantNodes())
        {
            switch (node)
            {
                case XElement element:
                    foreach (var attribute in element.Attributes())
                    {
                        yield return attribute.Value;
                    }

                    break;

                /* XCData derives from XText, so a path delivered as CDATA arrives here too. */
                case XText text:
                    yield return text.Value.Trim();
                    break;

                case XComment comment:
                    foreach (var value in CommentedOutValues(comment.Value))
                    {
                        yield return value;
                    }

                    break;

                default:
                    break;
            }
        }
    }

    /* The crude scan, kept for comment bodies alone and with the apostrophe arm that was #3140 removed:
       inside a comment an apostrophe is prose, and the two arms that remain cannot be opened by one. */
    private static readonly Regex CommentedOutValue =
        new("\"(?<quoted>[^\"]*)\"|>(?<text>[^<>]*)<", RegexOptions.None);

    /// <summary>The values a comment's text spells, for the over-read that makes a commented-out path
    /// count.</summary>
    private static IEnumerable<string> CommentedOutValues(string comment)
    {
        foreach (Match match in CommentedOutValue.Matches(comment))
        {
            yield return match.Groups["quoted"].Success
                ? match.Groups["quoted"].Value
                : match.Groups["text"].Value.Trim();
        }
    }

    /// <summary>A bounded slice of process output, so a failure message carries the reason without the
    /// half-megabyte of item JSON behind it.</summary>
    private static string Truncate(string text) =>
        text.Length <= 600 ? text : text[..600] + $"… (+{text.Length - 600} chars)";

    /// <summary>A synthetic two-SKU tree carrying one item of every requested type and one instance of
    /// every construct an XML parse needs a rule for. Returns the consuming project's path.
    ///
    /// <para><b>Synthetic because the repository cannot supply it.</b> There is no <c>Reference</c> item
    /// anywhere here, so the <c>HintPath</c> route to a cross-app assembly has no live example; nor is
    /// there a <c>Condition</c>, a <c>Choose</c>, an imported props file, a transform or a cross-app
    /// wildcard. Each construct is written with a HOLDING arm and a NOT-holding arm so the pin reading it
    /// asserts presence and absence over the same fixture, and a tree that failed to be written cannot
    /// pass by finding nothing.</para>
    ///
    /// <para>Shaped like this repository — the consuming project under one SKU's test tree, the files it
    /// reaches under the other SKU's app tree and its SIBLING test tree — so the #3067 anchor is exercised
    /// on the evaluated population and not only on the C# one.</para></summary>
    private static string WriteCrossAppFixture(string root)
    {
        var consumer = Path.Combine(root, LiteTestsDir);
        var otherTests = Path.Combine(root, DarlingTestsDir.Replace('/', Path.DirectorySeparatorChar));
        var otherApp = Path.Combine(root, DarlingAppDir);
        var globbed = Path.Combine(otherApp, "Fixtures");

        Directory.CreateDirectory(consumer);
        Directory.CreateDirectory(otherTests);
        Directory.CreateDirectory(Path.Combine(otherApp, "PerformanceMonitor.Darling.Service"));
        Directory.CreateDirectory(globbed);
        Directory.CreateDirectory(Path.Combine(root, DarlingAppDir + "Extra"));

        foreach (var name in new[]
        {
            "FromProperty.cs", "ConditionTrue.cs", "ConditionFalse.cs", "ChosenWhen.cs",
            "ChosenOtherwise.cs", "FromImport.cs", "Transformed.cs",
        })
        {
            File.WriteAllText(Path.Combine(otherTests, name), "// fixture\n");
        }

        File.WriteAllText(
            Path.Combine(otherApp, "PerformanceMonitor.Darling.Service", "Hinted.dll"), "fixture");
        File.WriteAllText(Path.Combine(globbed, "Globbed.xml"), "<x />");
        File.WriteAllText(Path.Combine(globbed, "NotGlobbed.txt"), "fixture");
        File.WriteAllText(Path.Combine(root, DarlingAppDir + "Extra", "Nope.cs"), "// fixture");
        File.WriteAllText(
            Path.Combine(otherTests, "Darling.Tests.csproj"),
            "<Project Sdk=\"Microsoft.NET.Sdk\"><PropertyGroup>"
            + "<TargetFramework>net10.0</TargetFramework></PropertyGroup></Project>\n");

        /* ABOVE the consuming project, which is what makes the resolution base load-bearing. Both the
           relative Include here and the Reference's relative HintPath are resolved by MSBuild against
           the CONSUMING project's directory rather than against this file's, and written from this
           directory they resolve OUT of the fixture tree under the other rule - so an implementation
           that took the declaring file's directory finds neither and the pin reds. Sitting beside the
           project, as a Directory.Build.props usually does not, the two bases coincide and the pin
           could not fail for this reason at all. */
        File.WriteAllText(
            Path.Combine(root, "Imported.props"),
            """
            <Project>
              <PropertyGroup>
                <OtherTests>..\Darling\Darling.Tests</OtherTests>
                <FixtureGate>yes</FixtureGate>
              </PropertyGroup>
              <ItemGroup>
                <Compile Include="$(OtherTests)\FromImport.cs" Link="FromImport.cs" />
                <TransformSeed Include="Transformed.cs" />
                <Reference Include="SomeCrossAppLib">
                  <HintPath>..\Darling\PerformanceMonitor.Darling.Service\Hinted.dll</HintPath>
                </Reference>
              </ItemGroup>
            </Project>

            """.Replace("\r\n", "\n", StringComparison.Ordinal));

        var project = Path.Combine(consumer, "Fixture.csproj");
        File.WriteAllText(
            project,
            """
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net10.0</TargetFramework>
                <EnableDefaultItems>false</EnableDefaultItems>
              </PropertyGroup>
              <Import Project="..\Imported.props" />
              <ItemGroup>
                <Compile Include="$(OtherTests)\FromProperty.cs" />
                <None Include="..\Darling\Fixtures\*.xml" />
                <Content Include="$(OtherTests)\ChosenWhen.cs" Link="Content.cs" />
                <EmbeddedResource Include="$(OtherTests)\ConditionTrue.cs" />
                <ProjectReference Include="$(OtherTests)\Darling.Tests.csproj" />
                <Compile Include="..\DarlingExtra\Nope.cs" />
              </ItemGroup>
              <ItemGroup Condition="'$(FixtureGate)' == 'yes'">
                <Compile Include="$(OtherTests)\ConditionTrue.cs" />
              </ItemGroup>
              <ItemGroup Condition="'$(FixtureGate)' == 'no'">
                <Compile Include="$(OtherTests)\ConditionFalse.cs" />
              </ItemGroup>
              <Choose>
                <When Condition="'$(FixtureGate)' == 'yes'">
                  <ItemGroup>
                    <Compile Include="$(OtherTests)\ChosenWhen.cs" />
                  </ItemGroup>
                </When>
                <Otherwise>
                  <ItemGroup>
                    <Compile Include="$(OtherTests)\ChosenOtherwise.cs" />
                  </ItemGroup>
                </Otherwise>
              </Choose>
              <ItemGroup>
                <None Include="@(TransformSeed->'$(OtherTests)\%(Identity)')" />
              </ItemGroup>
            </Project>

            """.Replace("\r\n", "\n", StringComparison.Ordinal));

        return project;
    }

    /// <summary>Each collected reference paired with a concrete file path to test coverage against.</summary>
    private static IEnumerable<(string Raw, string Probe, string Origin)> Resolve(
        string repo, SortedDictionary<string, SortedSet<string>> seen)
    {
        foreach (var (raw, named) in seen)
        {
            /* Every file that named it, so a failure points at all of them. */
            var origin = string.Join(", ", named);

            /* A reference carrying glob syntax is an assertion ABOUT the filter (WatermarkPolicyTests does
               this), not a file read. Excluded rather than matched, or the guard would assert on itself.
               A project item's wildcard never arrives here as a wildcard: MSBuild expands it, so the
               evaluated read contributes the concrete files it matched. */
            if (raw.IndexOfAny(new[] { '*', '!', '(', ')', '{', '}' }) >= 0)
            {
                continue;
            }

            /* Anything that resolves to nothing is a message string or a moved file — not this test's
               business. */
            var onDisk = OnDisk(repo, raw);
            if (onDisk is null)
            {
                continue;
            }

            if (File.Exists(onDisk))
            {
                yield return (raw, raw, origin);
            }
            else
            {
                /* Directory reads are EnumerateCsFiles-shaped, so coverage of *.cs inside it is the ask. */
                yield return (raw + " (directory)", raw.TrimEnd('/') + "/CoverageProbe.cs", origin);
            }
        }
    }

    /// <summary>The on-disk path a repo-rooted reference names, or null when it names nothing real.
    ///
    /// <para>The round-trip is the load-bearing part. <c>Directory.Exists</c> answers according to the
    /// HOST's path rules, and Windows trims trailing periods from a path component — so
    /// <c>Darling</c> followed by a three-dot segment resolves to <c>Darling</c> there and to nothing on
    /// macOS or Linux. This guard runs on Windows in CI and gets verified on macOS, so a reference counts
    /// only when the path it resolved to spells it back, and the answer is the same on every host.</para></summary>
    private static string? OnDisk(string repo, string raw)
    {
        var candidate = Path.Combine(repo, raw.Replace('/', Path.DirectorySeparatorChar));
        if (!File.Exists(candidate) && !Directory.Exists(candidate))
        {
            return null;
        }

        return string.Equals(
                Rooted(repo, Path.GetFullPath(candidate)),
                raw.TrimEnd('/'),
                StringComparison.Ordinal)
            ? candidate
            : null;
    }

    /// <summary>A repo-relative, forward-slash spelling of an absolute path, for messages.</summary>
    private static string Rooted(string repo, string absolute)
    {
        var prefix = Path.GetFullPath(repo).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
        return absolute.StartsWith(prefix, StringComparison.Ordinal)
            ? absolute[prefix.Length..].Replace(Path.DirectorySeparatorChar, '/')
            : absolute;
    }

    /// <summary>The quoted pattern entries of one dorny/paths-filter area block.</summary>
    private static List<string> FilterPatterns(string yaml, string filterName)
    {
        /* #5669: the lite-tests job's *_shard sets live in .github/lite-paths-filter.yml (top-level keys, two-space
           entries), the one file the shard legs, the lite-scope job and ci-select.py all read. */
        if (filterName.EndsWith("_shard", StringComparison.Ordinal))
        {
            return LiteFilterFilePatterns(filterName);
        }

        var at = yaml.IndexOf($"\n            {filterName}:\n", StringComparison.Ordinal);
        if (at < 0)
        {
            return new List<string>();
        }

        var rest = yaml[(at + 1)..];
        var next = Regex.Match(rest, "\n            [a-z_]+:\n");
        var block = next.Success ? rest[..next.Index] : rest;

        return Regex.Matches(block, @"^\s*-\s*'([^']+)'\s*$", RegexOptions.Multiline)
            .Select(m => m.Groups[1].Value)
            .ToList();
    }

    /// <summary>The Lite path gate, LF-normalised (#5669).</summary>
    internal static string ReadLiteFilterFile(string repo) =>
        File.ReadAllText(Path.Combine(repo, ".github", "lite-paths-filter.yml"))
            .Replace("\r\n", "\n", StringComparison.Ordinal);

    /// <summary>The quoted pattern entries of one top-level filter in <c>.github/lite-paths-filter.yml</c>.</summary>
    internal static List<string> LiteFilterFilePatterns(string filterName)
    {
        var text = "\n" + ReadLiteFilterFile(RepoRoot());
        var at = text.IndexOf($"\n{filterName}:\n", StringComparison.Ordinal);
        if (at < 0)
        {
            return new List<string>();
        }

        var rest = text[(at + 1)..];
        var next = Regex.Match(rest[1..], "\n[a-z_]+:\n");
        var block = next.Success ? rest[..(next.Index + 1)] : rest;

        return Regex.Matches(block, @"^\s*-\s*'([^']+)'\s*$", RegexOptions.Multiline)
            .Select(m => m.Groups[1].Value)
            .ToList();
    }

    /// <summary>Glob match for the dorny/paths-filter subset this repo uses: <c>**</c>, <c>*</c>, and a
    /// trailing <c>!(*.md)</c> markdown carve-out.</summary>
    private static bool Matches(string pattern, string path)
    {
        const string NotMarkdown = "/!(*.md)";
        var excludesMarkdown = pattern.EndsWith(NotMarkdown, StringComparison.Ordinal);
        if (excludesMarkdown)
        {
            if (path.EndsWith(".md", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            pattern = pattern[..^NotMarkdown.Length] + "/**";
        }

        var rx = new StringBuilder("^");
        for (var i = 0; i < pattern.Length; i++)
        {
            if (pattern[i] != '*')
            {
                rx.Append(Regex.Escape(pattern[i].ToString()));
                continue;
            }

            var isDouble = i + 1 < pattern.Length && pattern[i + 1] == '*';
            if (!isDouble)
            {
                rx.Append("[^/]*");
                continue;
            }

            if (i + 2 < pattern.Length && pattern[i + 2] == '/')
            {
                /* A double-star segment followed by a separator spans zero or more directories. */
                rx.Append("(?:.*/)?");
                i += 2;
            }
            else
            {
                rx.Append(".*");
                i += 1;
            }
        }

        rx.Append('$');
        return Regex.IsMatch(path, rx.ToString());
    }

    /// <summary>One step's text inside a job block: from its <c>- name:</c> line to the next step's.</summary>
    internal static string StepBlock(string job, string stepName)
    {
        var at = job.IndexOf("      - name: " + stepName + "\n", StringComparison.Ordinal);
        Assert.True(at >= 0, $"the '{stepName}' step is gone — find where it moved before editing this test");
        var rest = job[at..];
        var next = rest.IndexOf("\n      - ", 1, StringComparison.Ordinal);
        return next < 0 ? rest : rest[..next];
    }

    private static string ReadBuildYaml(string repo) =>
        File.ReadAllText(Path.Combine(repo, ".github", "workflows", "build.yml"))
            .Replace("\r\n", "\n", StringComparison.Ordinal);

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            if (Directory.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.Collectors")))
            {
                return dir.FullName;
            }
        }

        throw new DirectoryNotFoundException($"could not locate the repo root walking up from {thisFile}");
    }
}
