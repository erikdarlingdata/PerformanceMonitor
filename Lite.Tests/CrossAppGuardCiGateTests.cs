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
/// carrying an <c>&lt;Import&gt;</c> element fails <see cref="Check"/> loudly rather than being quietly
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
/// are enumerated in <see cref="DarlingTestsBackstopped"/> with the bound each rests on, compared for
/// EQUALITY against what the filter could not reach, and honoured only while
/// <see cref="WholeTreeBackstopIsIntact"/> holds.</para>
/// </summary>
public class CrossAppGuardCiGateTests
{
    /* Which filter gates which suite, per build.yml's "Run Lite tests" / "Run Darling tests" steps. */
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
    /// path is anchored on; <see cref="TestsDir"/> alone is what <see cref="Check"/>'s anti-vacuity
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

    /// <summary>
    /// Cross-app reads of <see cref="LiteTestsDir"/> from <see cref="DarlingTestsDir"/> that the
    /// <c>darling</c> filter does not reach, each with the bound its exemption rests on.
    ///
    /// <para><b>Why an exemption rather than a filter entry.</b> Every one of these is a read of Lite's
    /// TEST project, and the only entry that reaches a tree enumeration is <c>Lite.Tests/**</c> —
    /// nothing narrower covers a guard that opens every file under it. In the <c>darling</c> filter that
    /// entry would buy no guard execution at all: what a build-job filter decides is which products get
    /// COMPILED AND PUBLISHED, and the suite reading these files already runs on every such change — in
    /// the build job when another entry lit <c>darling</c>, and in <c>darling-tree-guards</c> when none
    /// did. Measured over the 100 pull requests merged to <c>dev</c> before this landed: 6 touched
    /// <c>Lite.Tests/**</c> without touching <c>Darling/**</c>, all 6 ran the Darling suite, and the
    /// entry would have changed only WHERE it ran, for 1 of the 6, while making that one build the
    /// Darling Viewer and publish both Darling artifacts.</para>
    ///
    /// <para><b>The exemption is not standing permission.</b> It is honoured only while
    /// <see cref="WholeTreeBackstopIsIntact"/> holds, so narrowing that job to a class filter or losing
    /// its gate turns all of these back into failures on the next run instead of leaving them resting on
    /// something that stopped being true. And the set is compared for EQUALITY against what the filter
    /// could not reach, so an entry whose read is deleted — or which a later filter entry makes
    /// reachable — fails here rather than lingering as a permission nobody needs.</para>
    /// </summary>
    private static readonly Dictionary<string, string> DarlingTestsBackstopped = new(StringComparer.Ordinal)
    {
        [$"{LiteTestsDir}/QueryHighDopStaleMaxDopParityTests.cs"] =
            "NAMED READ. The Darling half declares this path as its `twin`, probes it with File.Exists "
            + "and parses the ExpectedGuardCopies total declared there, so an edit to the Lite half has "
            + "to run the Darling guard. It does: darling-tree-guards runs the whole suite exactly when "
            + "the darling filter did not fire, which is the case a Lite.Tests-only change produces.",

        [$"{LiteTestsDir}/ParameterSensitivityFiringSignatureParityTests.cs"] =
            "NAMED READ. Same shape as the entry above, against ExpectedSignatureCopies.",

        [$"{LiteTestsDir}/AnalysisPassTokenThreadingTests.cs"] =
            "WHOLE-TREE READ. CommentFilterAdoptionTests enumerates every *.cs under BOTH test projects "
            + "and compares the prefix-filter sites it finds against its own bounded set; this path is "
            + "one of that set's keys and also one of the files the sweep opens. The string is a "
            + "<project>/<filename> label, which for a file sitting directly under Lite.Tests happens "
            + "to spell a real repo-rooted path - so requiring reachability of it is not a false "
            + "positive, and nothing narrower than Lite.Tests/** would reach the sweep anyway.",

        [$"{LiteTestsDir}/LiteSidebarDotRendersTheCardStatusTests.cs"] =
            "WHOLE-TREE READ. The second key of that same bounded set, same sweep, same reason.",
    };

    /// <summary>The exact command <c>darling-tree-guards</c> has to run for anything in
    /// <see cref="DarlingTestsBackstopped"/> to be covered: the WHOLE Darling suite.
    ///
    /// <para>Compared as a whole line rather than searched for as a substring, which is the difference
    /// between a bound and a hope. A <c>-class</c> or <c>-method</c> filter appended to this invocation
    /// would still CONTAIN it, so a containment check would keep reporting the backstop intact while it
    /// had stopped running the two twin guards the exemptions above name — and those exemptions would
    /// then be the only thing standing between an unreachable read and a green build.</para></summary>
    private const string BackstopRunLine =
        "run: dotnet run --project Darling/Darling.Tests/Darling.Tests.csproj -c Release --no-build";

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
            var bareDirectory = "Lite.Tests";
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

                /* Absent, and each for its own reason: LiteTests/ shares no boundary with either root;
                   Lite.TestsExtra/ is prefixed BY the longer root and is still not it; and a bare
                   directory name with no separator was never in this matcher's language, before or
                   after the widening (ControlPlaneReloadDurabilityTests names one, and this pin
                   records that the scan does not see it rather than implying it does). */
            },
            CSharpPaths(Text, LiteTrees).ToArray());
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

            var evaluation = Evaluate(fixture, project);
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
            var missing = Evaluate(scratch, Path.Combine(scratch, "NoSuchProject.csproj"));
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
            var broken = Evaluate(scratch, malformed);
            Assert.NotEqual(0, broken.ExitCode);
            Assert.NotEqual(string.Empty, broken.Failure);
            Assert.Empty(broken.Items);

            /* Output that is not the expected JSON is a failure and not an empty answer. Driven through
               the shipped parser rather than through a process, because there is no invocation that
               produces this today - the point is that the reader does not treat it as zero items. */
            Assert.NotEqual(string.Empty, ParseEvaluation("Build succeeded.", out _, out _, out _));
            Assert.NotEqual(string.Empty, ParseEvaluation(string.Empty, out _, out _, out _));
            Assert.NotEqual(string.Empty, ParseEvaluation("{\"Properties\":{}}", out _, out _, out _));

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
        }
        finally
        {
            if (Directory.Exists(scratch))
            {
                Directory.Delete(scratch, recursive: true);
            }
        }
    }

    /// <summary>
    /// The evaluated set has to CONTAIN what a text scan of the same XML finds, not merely differ from it.
    ///
    /// <para>A replacement that found different things rather than more things would read as an
    /// improvement while having moved the blind spot, so <see cref="ParsedProjectXmlPaths"/> stays as a
    /// deliberately crude second opinion — quoted attribute values and element text, resolved against the
    /// project directory and anchored the same way. It is not the live route and is not a good one: it
    /// over-reads (a path inside a comment counts) and under-reads (anything needing evaluation
    /// disappears). Both are the safe direction for a lower bound.</para>
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
                var evaluation = Evaluate(repo, file);
                AssertInvocationSucceeded(evaluation);
                evaluated.UnionWith(CrossAppItems(repo, evaluation, other).Select(r => r.Rooted));
            }

            if (floor is not null)
            {
                Assert.Contains(floor, parsedCross);
                Assert.Contains(floor, evaluated);
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
    /// <para><see cref="ImportedBuildFileProperties"/> is reported and not floored on the first two names:
    /// there is no <c>Directory.Build.props</c> or <c>Directory.Build.targets</c> in this repository and
    /// asserting one exists would be asserting a fiction. What IS floored is that the property set came back
    /// populated at all, through <c>MSBuildProjectFullPath</c> naming the project that was asked — so
    /// "MSBuild says there are none" is distinguishable from "no properties were returned".
    /// <c>Directory.Packages.props</c> is asserted because it is imported into every project here, and
    /// because a two-name upward probe of <c>Directory.Build.*</c> is exactly the answer that misses
    /// it.</para>
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
                /* Darling.Tests' project names no file under Lite or Lite.Tests, so there is nothing to
                   require by name here and a fabricated expectation would be worse than none. Its own arm
                   of the #3067 floor in Check is taken over the C# population instead. */
                Named: null),
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

            /* MSBuild's own answer for the build files it imports unnamed. The two Directory.Build names
               are absent from this tree, and Directory.Packages.props is not — which is the half a
               two-name upward probe of the Directory.Build names cannot see. */
            Assert.Equal(
                new[] { string.Empty, string.Empty, Path.Combine(repo, "Directory.Packages.props") },
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
    public void EveryCrossAppSourceRead_IsReachableByTheFilterThatGatesItsSuite()
    {
        var repo = RepoRoot();
        var yaml = ReadBuildYaml(repo);

        var failures = new List<string>();

        Check(repo, yaml, failures,
            scannedProject: LiteTestsDir,
            other: DarlingTrees,
            filterName: "lite",
            gatingStep: "Run Lite tests",
            backstopped: null);

        Check(repo, yaml, failures,
            scannedProject: DarlingTestsDir,
            other: LiteTrees,
            filterName: "darling",
            gatingStep: "Run Darling tests",
            /* This arm alone, because this arm alone has a backstop: darling-tree-guards runs the whole
               Darling suite exactly when the darling filter did not fire. There is no counterpart for
               Lite.Tests, so the Lite arm gets no exemptions at all rather than an empty list that would
               read as "none needed yet". */
            backstopped: DarlingTestsBackstopped);

        Assert.True(
            failures.Count == 0,
            "Cross-app guards that PR CI cannot run — add the path to the named filter in " +
            ".github/workflows/build.yml, or the guard only fires in the nightly, after the merge:\n  " +
            string.Join("\n  ", failures));
    }

    /// <summary>
    /// The suite that reads the whole tree has to run on the whole tree.
    ///
    /// <para><see cref="EveryCrossAppSourceRead_IsReachableByTheFilterThatGatesItsSuite"/> above covers reads
    /// of a NAMED path, which is what a cross-app guard does — there is a path, so a filter can name it. Some
    /// guards in <c>Darling.Tests</c> take the repository ITSELF as input instead: <c>FleetIdentifierScrubTests</c>
    /// enumerates every tracked file carrying one of nine extensions, and <c>MigrationUpgradeLadderLiveTests</c>
    /// derives the fixture it requires from <c>CHANGELOG.md</c>. Those have no path to add, and
    /// <c>darling</c> / <c>core</c> / <c>root</c> describe Darling product code rather than the tree.</para>
    ///
    /// <para>So <c>build.yml</c> answers it from the other side: the <c>build</c> job publishes whether it ran
    /// the suite, and a second job runs the suite when that reads <c>skipped</c>. Three pieces of wiring carry
    /// that, and losing any one of them leaves a job reporting success having run nothing — the step must
    /// carry the id, the job must publish its outcome, and the consuming job must actually invoke the suite.
    /// Pinned here rather than in <c>Darling.Tests</c> because the failure being pinned is a suite that does
    /// not run: a pin inside it would be gated by the thing it is checking.</para>
    /// </summary>
    [Fact]
    public void TheDarlingSuite_RunsWhereTheAreaFiltersDoNotReach()
    {
        var yaml = ReadBuildYaml(RepoRoot());

        var step = yaml.IndexOf("- name: Run Darling tests", StringComparison.Ordinal);
        Assert.True(step > 0, "the step that runs Darling.Tests was renamed — re-point this assertion before editing it");
        Assert.Contains(
            "id: darling-tests",
            yaml[step..Math.Min(step + 200, yaml.Length)],
            StringComparison.Ordinal);

        Assert.Contains("darling-tests: ${{ steps.darling-tests.outcome }}", yaml, StringComparison.Ordinal);

        var consumer = yaml.IndexOf("needs.build.outputs.darling-tests", StringComparison.Ordinal);
        Assert.True(
            consumer > 0,
            "nothing reads the build job's Darling.Tests outcome, so the suite runs only where the area "
          + "filters reach and a markdown-only or .github-only change runs none of it");

        /* From the consumer onward, so this cannot be satisfied by the darling-pg job's invocation earlier
           in the file — the job that reads the outcome is the one that has to run the suite. */
        var consumingJob = yaml[consumer..];
        Assert.Contains("= \"skipped\"", consumingJob, StringComparison.Ordinal);

        /* The WHOLE line, not the invocation as a substring. A -class or -method filter appended to it
           still contains the invocation, so a containment check would report the backstop intact while
           it had stopped running whichever guards were not in the filter — and the exemptions in
           DarlingTestsBackstopped rest on this job running all of them. Failing on any added argument is
           the right direction: a widened invocation reds and gets read, rather than quietly narrowing. */
        Assert.Contains(
            BackstopRunLine,
            consumingJob.Split('\n').Select(line => line.Trim()),
            StringComparer.Ordinal);

        /* And the same fact through the predicate the exemptions are gated on, so the two cannot drift:
           the pin above would keep passing on a yaml this returns false for if it ever stopped reading
           the same slice. */
        Assert.True(
            WholeTreeBackstopIsIntact(yaml),
            "the wiring above reads intact but the predicate that gates the DarlingTestsBackstopped "
          + "exemptions does not agree — they are asserting different things about the same job");
    }

    private static void Check(
        string repo,
        string yaml,
        List<string> failures,
        string scannedProject,
        SkuTrees other,
        string filterName,
        string gatingStep,
        IReadOnlyDictionary<string, string>? backstopped)
    {
        var patterns = FilterPatterns(yaml, filterName);
        Assert.True(
            patterns.Count > 0,
            $"build.yml's '{filterName}' path filter is gone — find where it moved before editing this test");

        /* The step must actually consume the filter, or the entries are decoration. */
        var step = yaml.IndexOf("name: " + gatingStep, StringComparison.Ordinal);
        Assert.True(step > 0, $"the '{gatingStep}' step is gone — find where it moved before editing this test");
        Assert.Contains(
            $"steps.filter.outputs.{filterName} == 'true'",
            yaml[step..Math.Min(step + 400, yaml.Length)],
            StringComparison.Ordinal);

        var scan = Scan(repo, scannedProject, other);

        /* #3063: a widened population that reaches no project files passes exactly as the *.cs-only one
           did, which is the defect committed by its own fix. Each stage is floored by name, because
           "found no references" and "opened no files" are otherwise the same green. */
        Assert.True(scan.CSharpFiles > 0, $"the C# stage opened no files under {scannedProject}");
        Assert.True(
            scan.ProjectFiles.Count > 0,
            $"the MSBuild stage found no project file under {scannedProject}, so a cross-app reference " +
            "written as an Include= attribute is invisible again");

        /* The shell-out, floored in the three places it can fail without saying so, for every project
           evaluated. Asserted here and not only in the pins, because this is where an empty answer
           becomes a pass. */
        Assert.NotEmpty(scan.Evaluated);
        foreach (var evaluation in scan.Evaluated)
        {
            AssertInvocationSucceeded(evaluation);
        }

        /* An <Import>'s own path is the one cross-app read the evaluated population does not carry, so a
           project that grows one is a failure rather than a silent partial scan. */
        Assert.True(
            scan.UnreportedImports.Count == 0,
            "project files under " + scannedProject + " carry an <Import> element, and the evaluated read " +
            "reports the ITEMS an imported file defines but not the imported file's own path — so whether " +
            "that path crosses apps is unanswered here. Resolve it by hand, or add coverage for the " +
            "construct:\n  " +
            string.Join("\n  ", scan.UnreportedImports));

        /* #3067's floor, and the one the stage floors above cannot provide. Both arms opened hundreds of
           files and found a healthy pile of references while the other SKU's TEST tree contributed NONE
           of them: the anchor was the app directory, and Lite.Tests is a sibling of Lite rather than a
           child. So the floor is taken over the tree whose invisibility was the defect rather than over
           the reference total — a total stays comfortably non-zero while a whole tree drops out of it,
           which is exactly how this went unnoticed. */
        Assert.True(
            scan.References.Any(r => r.Probe.StartsWith(other.TestsDir + "/", StringComparison.Ordinal)),
            $"{scannedProject} named no file under {other.TestsDir}, so the other SKU's test tree is out " +
            "of the found set entirely and every decision below it is vacuous for that tree. That is " +
            $"#3067: an anchor on {other.AppDir} alone cannot see a read of a test project that is its " +
            $"SIBLING. Found {scan.References.Count} reference(s): " +
            $"[{string.Join(", ", scan.References.Select(r => r.Raw))}]");

        var unreachable = scan.References
            .Where(r => !patterns.Any(p => Matches(p, r.Probe)))
            .ToList();

        foreach (var reference in unreachable)
        {
            if (backstopped is not null &&
                backstopped.ContainsKey(reference.Raw) &&
                WholeTreeBackstopIsIntact(yaml))
            {
                continue;
            }

            failures.Add(
                $"{scannedProject} reads {reference.Raw} (named in {reference.Origin}) but the " +
                $"'{filterName}' filter does not reach it (probe path: {reference.Probe})");
        }

        /* The other half of the set comparison, and the half an allow-list stops having once nobody is
           forced to look at it: an entry whose read was deleted, or which a later filter entry made
           reachable, is a standing permission for nothing. Reported as a failure rather than asserted
           separately so both directions arrive in one message. */
        foreach (var stale in (backstopped?.Keys ?? Enumerable.Empty<string>())
            .Where(k => !unreachable.Any(r => r.Raw.Equals(k, StringComparison.Ordinal)))
            .OrderBy(k => k, StringComparer.Ordinal))
        {
            failures.Add(
                $"{nameof(DarlingTestsBackstopped)} still exempts {stale}, but {scannedProject} no longer " +
                $"has a read of it that the '{filterName}' filter cannot reach — delete the entry rather " +
                "than leaving an exemption nothing needs");
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

    /// <summary>The paths one C# file's text names that belong to <paramref name="other"/>, in the two
    /// spellings this repository's pins use, repo-rooted and forward-slashed.
    ///
    /// <para><b>The anchor is every root the SKU owns, which is #3067.</b> Anchored on the app directory
    /// alone, <c>Lite.Tests/X.cs</c> matched neither <c>Lite/</c> nor anything else and left the found
    /// set silently — the failure direction this whole class exists to close. A root still has to be
    /// followed by a SEPARATOR, so a tree merely PREFIXED by a root's name (<c>Lite.TestsExtra/</c>) is
    /// no more a read of that SKU than it was before.</para>
    ///
    /// <para>Shared by the walk and by the pin that reads it back, for the same reason
    /// <see cref="MsBuildPaths"/> is: a retyped copy in a test is free to agree with itself while the
    /// matcher that ships does something else.</para></summary>
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
    }

    /// <summary>Whether <c>build.yml</c> still runs the WHOLE <c>Darling.Tests</c> suite where the area
    /// filters do not reach, which is the bound every entry in
    /// <see cref="DarlingTestsBackstopped"/> rests on.
    ///
    /// <para>Read where the exemptions are HONOURED and not only by the pin below, so an exemption
    /// cannot outlive the thing it rests on: narrow that job to a class filter, or lose its
    /// <c>skipped</c> gate, and all four entries turn back into failures on the next run.</para>
    ///
    /// <para>Sliced from the consumer onward so the build job's own identical invocation cannot satisfy
    /// it — the job that reads the outcome is the job that has to run the suite.</para></summary>
    private static bool WholeTreeBackstopIsIntact(string yaml)
    {
        var consumer = yaml.IndexOf("needs.build.outputs.darling-tests", StringComparison.Ordinal);
        if (consumer < 0)
        {
            return false;
        }

        var consumingJob = yaml[consumer..];

        return consumingJob.Contains("= \"skipped\"", StringComparison.Ordinal)
            && consumingJob.Split('\n')
                .Any(line => line.Trim().Equals(BackstopRunLine, StringComparison.Ordinal));
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
        var evaluations = projectFiles.Select(f => Evaluate(repo, f)).ToList();

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
    private static Evaluation Evaluate(string repo, string projectPath)
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
        var (exitCode, stdout, stderr) = RunDotnet(repo, arguments);
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
    /// same green as a clean tree. Read from <see cref="Check"/> as well as from the pins, so a floor
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
        string workingDirectory, IEnumerable<string> arguments)
    {
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

            if (!process.WaitForExit((int)EvaluationTimeout.TotalMilliseconds))
            {
                try
                {
                    process.Kill(entireProcessTree: true);
                }
                catch (InvalidOperationException)
                {
                    /* Exited between the deadline and the kill; the exit code below is still read. */
                }

                return (-1, string.Empty, $"dotnet did not exit within {EvaluationTimeout}");
            }

            return (process.ExitCode, stdout.GetAwaiter().GetResult(), stderr.GetAwaiter().GetResult());
        }
        catch (System.ComponentModel.Win32Exception error)
        {
            return (-1, string.Empty, "dotnet could not be started: " + error.Message);
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

    /// <summary>The cross-app paths a crude TEXT scan of one project file finds: every quoted attribute
    /// value and every element's text, resolved against the project directory and anchored the same way
    /// the evaluated read is.
    ///
    /// <para>Not the live route, and deliberately not a good one. It exists so the switch to MSBuild can
    /// be asserted rather than asserted-about: the evaluated set has to CONTAIN what this finds, or the
    /// replacement has moved the blind spot rather than closed it.</para>
    ///
    /// <para>Crude in both directions, and both are the safe direction for a lower bound. It over-reads —
    /// a path inside a comment, or behind a <c>Condition</c> that never holds, counts — and it under-reads
    /// anything needing evaluation, since a value carrying <c>$(</c> resolves to no real path and falls
    /// out. A lower bound that is sometimes too low is a floor; one that is ever too high is an
    /// oracle.</para></summary>
    private static IEnumerable<string> ParsedProjectXmlPaths(
        string repo, string projectDir, string xml, SkuTrees other)
    {
        foreach (Match match in Regex.Matches(
            xml, "\"(?<dq>[^\"]*)\"|'(?<sq>[^']*)'|>(?<text>[^<>]*)<"))
        {
            var raw =
                match.Groups["dq"].Success ? match.Groups["dq"].Value
                : match.Groups["sq"].Success ? match.Groups["sq"].Value
                : match.Groups["text"].Value.Trim();

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
               MSBuild wildcards never arrive here — RepoRooted reduces those to a directory, because in
               project XML a wildcard IS a read. */
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
