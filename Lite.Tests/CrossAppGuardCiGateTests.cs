/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
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
/// see one: the file was never opened. So the scan reads project XML as well, and the MSBuild path
/// normaliser is self-validated against known answers alongside the glob matcher, for the same reason —
/// a relative <c>Include=</c> resolving to the WRONG repo-rooted path finds nothing and passes.</para>
/// </summary>
public class CrossAppGuardCiGateTests
{
    /* Which filter gates which suite, per build.yml's "Run Lite tests" / "Run Darling tests" steps. */
    private const string LiteTestsDir = "Lite.Tests";
    private const string DarlingTestsDir = "Darling/Darling.Tests";

    /* The build files MSBuild imports into a project without being named, in the order it probes
       them. Shared by the walk and by the pin that floors where the walk looked. */
    private static readonly string[] BuildFileNames =
        { "Directory.Build.props", "Directory.Build.targets" };

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
    /// The MSBuild half of the self-validation above, and for the same reason: a relative
    /// <c>Include=</c> that normalised to the WRONG repo-rooted path would match no filter pattern and
    /// find no file on disk, so <see cref="Resolve"/> would drop it and every coverage check would pass
    /// having seen nothing. The first case is #3059's linked compile — the reference #3063 exists for.
    /// </summary>
    [Fact]
    public void TheMsBuildPathNormaliser_AgreesWithKnownAnswers()
    {
        var repo = RepoRoot();
        var liteTests = Path.Combine(repo, LiteTestsDir);
        var darlingTests = Path.Combine(repo, DarlingTestsDir.Replace('/', Path.DirectorySeparatorChar));

        Assert.Equal(
            "Darling/Darling.Tests/CSharpSourceWalker.cs",
            RepoRooted(repo, liteTests, @"..\Darling\Darling.Tests\CSharpSourceWalker.cs", "Darling"));

        /* Forward slashes are legal in MSBuild too, and so is a redundant segment. */
        Assert.Equal(
            "Darling/Darling.Tests/CSharpSourceWalker.cs",
            RepoRooted(repo, liteTests, "../Darling/./Darling.Tests/CSharpSourceWalker.cs", "Darling"));

        /* The other direction is two levels up, out of Darling/Darling.Tests. */
        Assert.Equal(
            "Lite/Services/DataImportService.cs",
            RepoRooted(repo, darlingTests, @"..\..\Lite\Services\DataImportService.cs", "Lite"));

        /* Same app, so not this guard's business — and the discrimination a relative path cannot make
           until it is resolved, since both spellings open with the same "..\". */
        Assert.Null(RepoRooted(repo, liteTests, @"..\Lite\Mcp\McpHostService.cs", "Darling"));
        Assert.Null(RepoRooted(repo, liteTests, @"Fixtures\SystemHealth\*.xml", "Darling"));

        /* A wildcard over the other app IS a read, so it becomes the directory it enumerates. Spelled
           against a directory that does not exist, deliberately: an expected value written as a real
           repo-rooted path is itself matched by the C# stage above, and would put a path nothing
           actually reads into the found set. The normaliser never touches the disk, so a fictional
           directory tests it exactly as well. */
        Assert.Equal(
            "Darling/NoSuchArea/Fixtures",
            RepoRooted(repo, liteTests, @"..\Darling\NoSuchArea\Fixtures\*.xml", "Darling"));

        /* Out of the tree entirely, and not a path at all. */
        Assert.Null(RepoRooted(repo, liteTests, @"..\..\Darling\X.cs", "Darling"));
        Assert.Null(RepoRooted(repo, liteTests, "xunit.v3", "Darling"));
    }

    /// <summary>
    /// Both MSBuild spellings, because MSBuild accepts both and the element one is the common form.
    ///
    /// <para><c>HintPath</c> is item METADATA, and metadata may be written as an attribute or as a child
    /// element — the element form being what Visual Studio emits for a legacy <c>&lt;Reference&gt;</c>.
    /// Reading only the attribute form would be #3063 recurring one attribute over: the scan would
    /// advertise a population wider than the one it has, which is the actual shape of that defect.
    /// <c>Include</c>, <c>Update</c> and <c>Import</c>'s <c>Project</c> are item operations rather than
    /// metadata and have no element spelling.</para>
    ///
    /// <para>Fed the shipped matchers rather than a retyped copy, so the pin cannot pass while the code
    /// drifts underneath it.</para>
    /// </summary>
    [Fact]
    public void TheMsBuildMatchers_ReadBothSpellings()
    {
        const string Xml = """
            <Project>
              <ItemGroup>
                <Compile Include="..\Other\A.cs" Link="A.cs" />
                <None Update='..\Other\B.xml' />
                <Reference Include="SomeLib">
                  <HintPath>..\Other\bin\SomeLib.dll</HintPath>
                </Reference>
                <Reference Include="Other" HintPath="..\Other\C.dll" />
                <Compile Remove="..\Other\Removed.cs" />
                <Reference Include="Gated">
                  <HintPath Condition="'$(Platform)'=='x64'">..\Other\x64\Gated.dll</HintPath>
                </Reference>
                <Reference Include="Angled">
                  <HintPath Condition="'$(V)'>'1'">..\Other\Angled.dll</HintPath>
                </Reference>
              </ItemGroup>
              <Import Project="..\Other\D.props" />
            </Project>
            """;

        Assert.Equal(
            new[]
            {
                @"..\Other\A.cs",
                @"..\Other\B.xml",
                "SomeLib",
                "Other",
                @"..\Other\C.dll",
                "Gated",
                "Angled",
                @"..\Other\D.props",

                /* The element spellings are collected after the attributes, and are the cases the
                   attribute matcher alone cannot see at all. The last two carry their own attribute on
                   the open tag - Condition, for a platform-specific hint path - and the last one's
                   Condition holds a raw '>', which is legal in an attribute value and would end the tag
                   early for a matcher that scanned to the next angle bracket. */
                @"..\Other\bin\SomeLib.dll",
                @"..\Other\x64\Gated.dll",
                @"..\Other\Angled.dll",
            },
            MsBuildPaths(Xml));

        /* Remove= is absent by design: dropping an item from a glob is not a read of it. */
        Assert.DoesNotContain(@"..\Other\Removed.cs", MsBuildPaths(Xml));
    }

    /// <summary>
    /// A value this scan cannot resolve has to be reported, not dropped.
    ///
    /// <para>An unrecognised MSBuild expression is the worst case for this guard: it falls through as a
    /// literal relative path, resolves to nothing, and vanishes from the found set — silently, which is
    /// the direction #3063 exists to close. So the classification covers every form MSBuild evaluates
    /// rather than the two that happen to be common. There are three, and no fourth: a property or
    /// property function, item metadata, and an item list or transform.</para>
    /// </summary>
    [Fact]
    public void TheEvaluabilityCheck_CoversEveryMsBuildExpressionForm()
    {
        Assert.True(NeedsMsBuildToEvaluate(@"$(RepoRoot)Darling\X.cs"));
        Assert.True(NeedsMsBuildToEvaluate(@"$([MSBuild]::GetPathOfFileAbove('Directory.Build.props'))"));
        Assert.True(NeedsMsBuildToEvaluate(@"%(RecursiveDir)X.cs"));

        /* An item list, and an item transform - neither carries $( or %( anywhere. */
        Assert.True(NeedsMsBuildToEvaluate("@(SharedSources)"));
        Assert.True(NeedsMsBuildToEvaluate(@"@(SharedSources->'..\Darling\%(Filename)%(Extension)')"));

        /* A literal path is readable, and so is one that merely contains the sigils unparenthesised. */
        Assert.False(NeedsMsBuildToEvaluate(@"..\Darling\Darling.Tests\CSharpSourceWalker.cs"));
        Assert.False(NeedsMsBuildToEvaluate("Fixtures/100%-coverage@home.xml"));
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
    /// was never opened. A widened collector that reaches no project files passes exactly as the narrow one
    /// did, which would be the same defect committed by its own fix. So the file that WOULD carry such a
    /// reference is required to be in the population, by name, rather than merely counted.</para>
    ///
    /// <para><see cref="ImportedBuildFiles"/> is reported and not floored: there is no
    /// <c>Directory.Build.props</c> or <c>Directory.Build.targets</c> in this repository today, and asserting
    /// one exists would be asserting a fiction. It is in the population so the first one is read, and it
    /// takes only the NEAREST of each name, as MSBuild does.</para>
    /// </summary>
    [Fact]
    public void ThePopulationReachesTheProjectFiles_AndNotOnlyTheCSharp()
    {
        var repo = RepoRoot();

        var expectations = new[]
        {
            (Project: LiteTestsDir, OtherApp: "Darling", Manifest: $"{LiteTestsDir}/Lite.Tests.csproj"),
            (Project: DarlingTestsDir, OtherApp: "Lite", Manifest: $"{DarlingTestsDir}/Darling.Tests.csproj"),
        };

        foreach (var (project, otherApp, manifest) in expectations)
        {
            var scan = Scan(repo, project, otherApp);

            Assert.True(scan.CSharpFiles > 0, $"the C# stage opened no files under {project}");

            Assert.True(
                scan.ProjectFiles.Contains(manifest, StringComparer.Ordinal),
                $"the MSBuild stage did not open {manifest}, so an Include= naming {otherApp} there would " +
                $"be invisible. Opened: [{string.Join(", ", scan.ProjectFiles)}]");

            /* Build output holds a copy of the project file on any machine that has built the suite; a
               scan that read it would assert on an artifact, and on this repo's own CI that copy is the
               one a stale build left behind. */
            Assert.DoesNotContain(
                scan.ProjectFiles,
                p => p.Contains("/bin/", StringComparison.Ordinal) ||
                     p.Contains("/obj/", StringComparison.Ordinal));

            /* The imported-build-file half needs the same protection as ProjectFiles above and cannot
               have it in the same shape: nothing in this repository imports a Directory.Build.props, so
               flooring the found COUNT would red on a clean tree. What is floored instead is that the
               collector reached the locations MSBuild itself would probe — the two names in every
               directory from the project up to and including the repo root — so "we looked and there
               were none" is distinguishable from "we never looked". The chain is re-derived here from
               the project path rather than read back off the walk. */
            var directories = new List<string>();
            for (var d = project; ; )
            {
                directories.Add(d);
                var cut = d.LastIndexOf('/');
                if (cut < 0)
                {
                    break;
                }

                d = d[..cut];
            }

            directories.Add(string.Empty);

            Assert.Equal(
                directories.SelectMany(d => BuildFileNames.Select(n => d.Length == 0 ? n : $"{d}/{n}")),
                scan.ImportedBuildProbes);

            /* And what it opened is exactly the NEAREST existing probe of each name, which is MSBuild's
               own rule. Holds at zero, so a clean tree passes honestly rather than by not being asked. */
            Assert.Equal(
                BuildFileNames
                    .Select(n => scan.ImportedBuildProbes.FirstOrDefault(
                        p => p.EndsWith(n, StringComparison.Ordinal) &&
                             File.Exists(Path.Combine(repo, p.Replace('/', Path.DirectorySeparatorChar)))))
                    .Where(p => p is not null)
                    .OrderBy(p => p, StringComparer.Ordinal),
                scan.ImportedBuildFiles.OrderBy(p => p, StringComparer.Ordinal));
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
            otherApp: "Darling",
            filterName: "lite",
            gatingStep: "Run Lite tests");

        Check(repo, yaml, failures,
            scannedProject: DarlingTestsDir,
            otherApp: "Lite",
            filterName: "darling",
            gatingStep: "Run Darling tests");

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
        Assert.Contains(
            "dotnet run --project Darling/Darling.Tests/Darling.Tests.csproj",
            consumingJob,
            StringComparison.Ordinal);
    }

    private static void Check(
        string repo,
        string yaml,
        List<string> failures,
        string scannedProject,
        string otherApp,
        string filterName,
        string gatingStep)
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

        var scan = Scan(repo, scannedProject, otherApp);

        /* #3063: a widened population that reaches no project files passes exactly as the *.cs-only one
           did, which is the defect committed by its own fix. Each stage is floored by name, because
           "found no references" and "opened no files" are otherwise the same green. */
        Assert.True(scan.CSharpFiles > 0, $"the C# stage opened no files under {scannedProject}");
        Assert.True(
            scan.ProjectFiles.Count > 0,
            $"the MSBuild stage opened no project files under {scannedProject}, so a cross-app reference " +
            "written as an Include= attribute is invisible again");

        /* The imported-build-file stage is floored on where it LOOKED, not on what it found: there is no
           Directory.Build.props anywhere in this repository, so a count floor would red on a clean tree
           while still not distinguishing "probed nowhere" from "probed and found none". */
        Assert.True(
            scan.ImportedBuildProbes.Count > 0,
            $"the imported-build-file stage probed nowhere for {scannedProject}, so an item group in a " +
            "Directory.Build.props or .targets is invisible. Probed: " +
            $"[{string.Join(", ", scan.ImportedBuildProbes)}], opened: " +
            $"[{string.Join(", ", scan.ImportedBuildFiles)}]");

        /* This guard's failure direction is not noticing, so a path it CANNOT read has to be loud rather
           than absent from the found set. */
        Assert.True(
            scan.UnevaluablePaths.Count == 0,
            "MSBuild path attributes this scan cannot evaluate, so it cannot say whether they cross apps — " +
            "spell the path literally, or teach RepoRooted the property:\n  " +
            string.Join("\n  ", scan.UnevaluablePaths));

        foreach (var reference in scan.References)
        {
            if (!patterns.Any(p => Matches(p, reference.Probe)))
            {
                failures.Add(
                    $"{scannedProject} reads {reference.Raw} (named in {reference.Origin}) but the " +
                    $"'{filterName}' filter does not reach it (probe path: {reference.Probe})");
            }
        }
    }

    /// <summary>What one scan found, carried together with what it opened to find it. One value rather
    /// than a bare reference list, because a found set is not interpretable without the population that
    /// produced it — an empty population and an empty answer are the same green.</summary>
    private readonly record struct CrossAppScan(
        int CSharpFiles,
        IReadOnlyList<string> ProjectFiles,
        IReadOnlyList<string> ImportedBuildProbes,
        IReadOnlyList<string> ImportedBuildFiles,
        IReadOnlyList<string> UnevaluablePaths,
        IReadOnlyList<(string Raw, string Probe, string Origin)> References);

    /* The MSBuild attributes that can carry a path to another app's file. Remove= is deliberately
       absent: dropping an item from a glob is not a read of it. */
    private static readonly Regex MsBuildPathAttribute = new(
        "\\b(?:Include|Update|Project|HintPath)\\s*=\\s*(?:\"(?<dq>[^\"]*)\"|'(?<sq>[^']*)')",
        RegexOptions.Compiled);

    /* HintPath is item METADATA, and MSBuild lets metadata be written as an attribute OR as a child
       element — the element form being what Visual Studio emits for a legacy <Reference>. Reading only
       the attribute spelling would be this issue recurring one attribute over: a cross-app assembly
       reference written the ordinary way, silently invisible. Include, Update and Import's Project are
       item operations rather than metadata and have no element spelling, so this one matcher covers the
       whole difference between the two grammars. */
    /* The open tag carries its own attributes - Condition is the usual one, for a platform-specific
       hint path - so they are skipped by matching quoted values rather than by "anything up to the next
       >". An attribute value may legally contain a raw > (only < and & must be escaped), and
       [^>]* would end the tag inside it. */
    private static readonly Regex MsBuildPathElement = new(
        "<HintPath(?:\\s+[\\w:.-]+\\s*=\\s*(?:\"[^\"]*\"|'[^']*'))*\\s*>([^<]*)</HintPath\\s*>",
        RegexOptions.Compiled);

    /// <summary>Repo-relative paths naming <paramref name="otherApp"/> that a test in
    /// <paramref name="project"/> reads, paired with a concrete file path to test coverage against.</summary>
    private static CrossAppScan Scan(string repo, string project, string otherApp)
    {
        /* Keyed by the reference, valued by every file that named it, so a failure says where to go. */
        var seen = new SortedDictionary<string, SortedSet<string>>(StringComparer.Ordinal);
        var unevaluable = new List<string>();
        var projectRoot = Path.Combine(repo, project.Replace('/', Path.DirectorySeparatorChar));
        if (!Directory.Exists(projectRoot))
        {
            throw new DirectoryNotFoundException($"test project not found: {project}");
        }

        var csharpFiles = 0;
        foreach (var file in TrackedFiles(projectRoot, "*.cs"))
        {
            csharpFiles++;
            var text = File.ReadAllText(file);
            var origin = Rooted(repo, file);

            /* "Darling/Some/Path.cs" and the backslash spelling some Windows-facing pins use. */
            foreach (Match m in Regex.Matches(text, "\"(" + Regex.Escape(otherApp) + "[/\\\\][^\"]+)\""))
            {
                Note(seen, m.Groups[1].Value.Replace('\\', '/'), origin);
            }

            /* Path.Combine("Darling", "Darling.Tests", "X.cs") */
            foreach (Match m in Regex.Matches(
                text, @"Path\.Combine\(\s*""" + Regex.Escape(otherApp) + @"""\s*,([^)]*)\)"))
            {
                var segments = Regex.Matches(m.Groups[1].Value, "\"([^\"]+)\"")
                    .Select(s => s.Groups[1].Value)
                    .ToArray();
                if (segments.Length > 0)
                {
                    Note(seen, otherApp + "/" + string.Join("/", segments), origin);
                }
            }
        }

        /* The second population, and the one #3063 was filed for. Neither matcher above can see a linked
           compile: the path is relative and backslashed, so their otherApp anchor never fires — but that
           is downstream of the real reason, which is that project XML is not a *.cs file and was never
           opened. The project's own files first, then the build files MSBuild imports into it unnamed. */
        var projectFiles = TrackedFiles(projectRoot, "*.csproj")
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToList();
        var (importedProbes, importedBuildFiles) = ImportedBuildFiles(repo, projectRoot);

        foreach (var file in projectFiles)
        {
            ReadMsBuildPaths(repo, file, Path.GetDirectoryName(file)!, otherApp, seen, unevaluable);
        }

        foreach (var file in importedBuildFiles)
        {
            /* MSBuild resolves a relative item path in an IMPORTED file against the consuming project's
               directory, not the imported file's own — which is why these resolve against projectRoot
               even for a Directory.Build.props sitting at the repo root. */
            ReadMsBuildPaths(repo, file, projectRoot, otherApp, seen, unevaluable);
        }

        return new CrossAppScan(
            csharpFiles,
            projectFiles.Select(f => Rooted(repo, f)).ToList(),
            importedProbes.Select(f => Rooted(repo, f)).ToList(),
            importedBuildFiles.Select(f => Rooted(repo, f)).ToList(),
            unevaluable,
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

    /// <summary>The build files MSBuild imports into a project without being named.
    ///
    /// <para>There are none in this repository today, and nothing here asserts there are. They are in the
    /// population because an <c>ItemGroup</c> in one can legally carry a cross-app <c>Compile Include</c> —
    /// for a file OUTSIDE the project cone there is no duplicate-item conflict with the SDK's later default
    /// glob, so it works — which is the same invisible shape as a <c>.csproj</c> entry in a file no
    /// <c>.csproj</c> scan would open. The point is that the first one is read rather than this test needing
    /// to be edited first.</para>
    ///
    /// <para><b>Nearest wins, per name, because that is what MSBuild does.</b>
    /// <c>Microsoft.Common.props</c> probes upward for ONE <c>Directory.Build.props</c> and stops; it does
    /// not chain through every one above the project, and it probes the two names independently. Collecting
    /// all of them would attribute an item group to a project that never imports it, and a guard whose
    /// findings are not trustworthy stops being read.</para>
    ///
    /// <para>A nearest file that deliberately chains to its parent does so through an <c>Import</c> whose
    /// path is a property function, and <see cref="ReadMsBuildPaths"/> reports an unevaluable path as a
    /// failure rather than ignoring it — so the chain is loud, not silently uncollected. That is the right
    /// direction for a guard whose whole defect class is not noticing.</para>
    ///
    /// <para><b>Every location looked at is returned alongside what was there</b>, which is why this
    /// hands back <c>Probed</c> as well as <c>Found</c>. A stage that probed nowhere and a stage that
    /// probed everywhere and found nothing both leave an empty found list, and on a tree with no such
    /// file anywhere — this one — a count floor cannot tell them apart without redding on a clean
    /// checkout. The probe list can, so it is what gets floored.</para></summary>
    private static (List<string> Probed, List<string> Found) ImportedBuildFiles(
        string repo, string projectRoot)
    {
        var root = Path.GetFullPath(repo).TrimEnd(Path.DirectorySeparatorChar);
        var probed = new List<string>();
        var found = new List<string>();
        var claimed = new HashSet<string>(StringComparer.Ordinal);

        for (var dir = new DirectoryInfo(Path.GetFullPath(projectRoot)); dir is not null; dir = dir.Parent)
        {
            foreach (var name in BuildFileNames)
            {
                var candidate = Path.Combine(dir.FullName, name);

                /* Probing continues to the root even once a name is claimed. The walk costs two
                   File.Exists calls per directory, and a probe list that stopped early could not be
                   floored against the locations MSBuild would consider. */
                probed.Add(candidate);

                if (!claimed.Contains(name) && File.Exists(candidate))
                {
                    claimed.Add(name);
                    found.Add(candidate);
                }
            }

            if (string.Equals(
                    dir.FullName.TrimEnd(Path.DirectorySeparatorChar), root, StringComparison.Ordinal))
            {
                break;
            }
        }

        return (probed, found);
    }

    /* Every expression form MSBuild evaluates, and there is no fourth: a property or property function,
       item metadata, and an item list or transform. Anything carrying one of these is a path this scan
       cannot read, and it has to say so rather than let the value fall through as a literal relative
       path that resolves to nothing and disappears. */
    private static readonly string[] MsBuildExpressionForms = { "$(", "%(", "@(" };

    /// <summary>Whether a path attribute's value can only be resolved by MSBuild itself.</summary>
    private static bool NeedsMsBuildToEvaluate(string path) =>
        MsBuildExpressionForms.Any(form => path.Contains(form, StringComparison.Ordinal));

    /// <summary>Every path-bearing value one MSBuild file's XML names, in both spellings MSBuild
    /// accepts. Shared by the walk and by the pin that reads it back, so the pin cannot drift from what
    /// ships.</summary>
    private static IEnumerable<string> MsBuildPaths(string xml) =>
        MsBuildPathAttribute.Matches(xml)
            .Select(m => m.Groups["dq"].Success ? m.Groups["dq"].Value : m.Groups["sq"].Value)
            .Concat(MsBuildPathElement.Matches(xml).Select(m => m.Groups[1].Value.Trim()));

    /// <summary>The <paramref name="otherApp"/> paths one MSBuild file names, repo-rooted into
    /// <paramref name="seen"/>.</summary>
    private static void ReadMsBuildPaths(
        string repo,
        string file,
        string projectDir,
        string otherApp,
        SortedDictionary<string, SortedSet<string>> seen,
        List<string> unevaluable)
    {
        var text = File.ReadAllText(file);
        var origin = Rooted(repo, file);
        var thisFileDir = Path.GetDirectoryName(file)! + Path.DirectorySeparatorChar;

        foreach (var raw in MsBuildPaths(text))
        {
            /* The two properties a hand-written cross-app include actually uses. */
            var expanded = raw
                .Replace("$(MSBuildThisFileDirectory)", thisFileDir, StringComparison.Ordinal)
                .Replace("$(MSBuildProjectDirectory)", projectDir, StringComparison.Ordinal);

            if (NeedsMsBuildToEvaluate(expanded))
            {
                /* Only MSBuild can evaluate what is left. Recorded rather than dropped, so the caller
                   can be loud about a path this guard cannot read. An unrecognised expression would
                   otherwise fall through as a literal relative path, resolve to nothing, and vanish —
                   which is this guard's own defect class. */
                unevaluable.Add($"{origin}: {raw}");
                continue;
            }

            var rooted = RepoRooted(repo, projectDir, expanded, otherApp);
            if (rooted is not null)
            {
                Note(seen, rooted, origin);
            }
        }
    }

    /// <summary>One MSBuild path attribute, expressed the way the rest of this class reasons about paths:
    /// repo-rooted, forward slashes.
    ///
    /// <para>This is the whole difficulty of #3063's fix. An <c>Include=</c> is RELATIVE to the project
    /// directory and spelled with backslashes (<c>..\Darling\Darling.Tests\X.cs</c>), so it names no app at
    /// all until it is resolved — the <paramref name="otherApp"/> anchor the C# matchers open with cannot
    /// fire on it, and applying that anchor AFTER normalisation is what leaves every coverage decision
    /// downstream of here unchanged.</para>
    ///
    /// <para>Null when the path names something other than <paramref name="otherApp"/>, resolves outside the
    /// repository, or is not a path at all (a <c>PackageReference</c>'s Include is a package id).</para></summary>
    private static string? RepoRooted(string repo, string projectDir, string raw, string otherApp)
    {
        if (raw.Length == 0)
        {
            return null;
        }

        var native = raw.Replace('\\', Path.DirectorySeparatorChar)
                        .Replace('/', Path.DirectorySeparatorChar);

        string full;
        try
        {
            full = Path.GetFullPath(Path.Combine(projectDir, native));
        }
        catch (ArgumentException)
        {
            return null;
        }

        var prefix = Path.GetFullPath(repo).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
        if (!full.StartsWith(prefix, StringComparison.Ordinal))
        {
            /* Outside the tree, so no path filter could name it. */
            return null;
        }

        var rooted = full[prefix.Length..].Replace(Path.DirectorySeparatorChar, '/');

        /* An MSBuild wildcard is a real file set. In C# source a glob is the opposite — there the pattern
           IS the assertion, which is why Resolve excludes those outright — so reduce it to the directory
           it enumerates and let the directory arm probe it, rather than dropping a genuine read. */
        var wildcard = rooted.IndexOfAny(new[] { '*', '?' });
        if (wildcard >= 0)
        {
            var cut = rooted.LastIndexOf('/', wildcard);
            if (cut <= 0)
            {
                return null;
            }

            rooted = rooted[..cut];
        }

        return rooted.Equals(otherApp, StringComparison.Ordinal)
            || rooted.StartsWith(otherApp + "/", StringComparison.Ordinal)
                ? rooted
                : null;
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
