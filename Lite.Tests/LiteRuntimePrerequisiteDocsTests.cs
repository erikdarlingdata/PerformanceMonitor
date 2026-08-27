/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2489/#2501: Lite ships TWO artifacts, and what a reader must install before either one starts is
/// decided nowhere near the sentence that states it. #2489 was the docs having the two backwards —
/// <c>PerformanceMonitorLite-win-Setup.exe</c> is packed from a <c>--self-contained</c> publish and
/// needs no runtime at all, yet it was the one artifact carrying a runtime requirement in the README,
/// while the portable ZIP was framework-dependent and had no prerequisites documented anywhere.
/// #2501 then made the ZIP self-contained too, so the correct answer for BOTH artifacts is now
/// "nothing" — and every runtime sentence #2499 added had to come back out.
///
/// The reason this is worth a guard rather than a one-time correction is that nothing in the product
/// forces the docs and the artifacts to agree. Both facts are decided in files nobody edits while
/// writing docs — the publish shape lives in the workflows, and the framework list is a build OUTPUT
/// that changes when a package reference changes (ASP.NET Core is on that list only because
/// <c>ModelContextProtocol.AspNetCore</c> drags the framework reference in transitively). A prose
/// sentence cannot notice either one moving.
///
/// So every assertion here is DERIVED from the shipped artifact, never from a list kept beside it:
/// the runtimes a framework-dependent build would demand come from the built
/// <c>PerformanceMonitorLite.runtimeconfig.json</c>, and which artifacts are self-contained comes from
/// parsing the <c>dotnet publish</c> lines in the two workflows that build them. Every claim is stated
/// BOTH ways round, because #2501 proved a one-way assertion is not enough: under #2499's version of
/// this file, flipping the ZIP to self-contained left two facts still green — the docs kept demanding
/// two runtimes and nothing noticed. Now, revert either publish line to framework-dependent and the
/// prose has to come back; leave them self-contained and the prose must not be there.
///
/// A framework this file has no mapping for is still a hard failure, because an undocumentable
/// prerequisite is exactly the state that produced #2489.
///
/// Note for whoever touches the CI path filters: <c>README.md</c> and <c>Lite/README.md</c> are named
/// explicitly in build.yml's <c>lite</c> filter. They have to be. Every area filter carves markdown
/// out (<c>dir/**/!(*.md)</c>), and a docs-only PR additionally engages the fast path that skips .NET
/// setup entirely — so without those entries this guard would be unrunnable on precisely the change it
/// exists to catch.
/// </summary>
public sealed class LiteRuntimePrerequisiteDocsTests
{
    private const string RootReadmePath = "README.md";
    private const string LiteReadmePath = "Lite/README.md";
    private const string ShippedNoticePath = "Lite/READ-ME-FIRST.txt";
    private const string LiteProjectPath = "Lite/PerformanceMonitorLite.csproj";
    private const string LiteLockFilePath = "Lite/packages.lock.json";
    private const string BuildWorkflowPath = ".github/workflows/build.yml";
    private const string NightlyWorkflowPath = ".github/workflows/nightly.yml";

    private const string PublishCommand = "dotnet publish Lite/PerformanceMonitorLite.csproj";
    private const string SetupExeName = "PerformanceMonitorLite-win-Setup.exe";
    private const string PortableZipName = "PerformanceMonitorLite-<version>.zip";

    /// <summary>
    /// The root README's Lite section heading. The runtime-download rule is scoped to this section
    /// rather than applied to the whole file on purpose: Darling IS framework-dependent (#2481), and
    /// the day its prerequisites get written down in the root README they will name the same two
    /// downloads. A whole-file rule would then go red for a reason that has nothing to do with Lite.
    /// </summary>
    private const string RootReadmeLiteHeading = "## Quick Start — Lite";

    /// <summary>
    /// Shared-framework name to the name of the runtime a human downloads to satisfy it, as a format
    /// string over the major version. This is a TRANSLATION table, not an inventory: the set of
    /// frameworks is read out of the built runtimeconfig, and a name missing from here fails the suite
    /// rather than being skipped.
    ///
    /// <c>Microsoft.NETCore.App</c> maps to nothing on purpose. Both installers a reader could be sent
    /// to contain it, so naming it in the docs would send an operator to a third download they do not
    /// need.
    /// </summary>
    private static readonly Dictionary<string, string> InstallerNameFormats = new(StringComparer.Ordinal)
    {
        ["Microsoft.NETCore.App"] = string.Empty,
        ["Microsoft.WindowsDesktop.App"] = ".NET Desktop Runtime {0}",
        ["Microsoft.AspNetCore.App"] = "ASP.NET Core Runtime {0}",
    };

    /// <summary>
    /// Phrases that can only be on a doc line to tell a reader they must go and install something
    /// before the artifact that line names will start. Checked against lines that NAME an artifact, so
    /// a sentence about the build system, or about Darling, is not caught by them.
    /// </summary>
    private static readonly string[] RuntimeDemandPhrases =
    [
        "requires",
        "Desktop Runtime",
        "ASP.NET Core Runtime",
        "framework-dependent",
    ];

    /// <summary>One shared framework a framework-dependent build names, and the major version it wants.</summary>
    private sealed record FrameworkRequirement(string Name, int Major, string SourcePath);

    /// <summary>
    /// The frameworks a FRAMEWORK-DEPENDENT Lite build asks the host for, read from its own
    /// <c>runtimeconfig.json</c> under <c>Lite/bin</c>.
    ///
    /// Reading the build output rather than the csproj is deliberate. The csproj never mentions
    /// <c>Microsoft.AspNetCore.App</c>; that framework arrives transitively and only the SDK's own
    /// output says so, which is the whole reason the requirement was a surprise. A self-contained
    /// runtimeconfig carries <c>includedFrameworks</c> instead of <c>frameworks</c> and is skipped —
    /// it states what is bundled, which by definition is not a prerequisite.
    ///
    /// This still resolves after #2501 because the ordinary RID-less <c>dotnet build</c> that CI runs
    /// to compile this suite is framework-dependent; only the PUBLISH is pinned to win-x64. That is
    /// what keeps the "if it were framework-dependent, here is what it would demand" half of this file
    /// derivable at all.
    /// </summary>
    private static IReadOnlyList<FrameworkRequirement> FrameworkDependentRequirements()
    {
        var binRoot = Path.Combine(ParitySource.RepoRoot(), "Lite", "bin");
        Assert.True(
            Directory.Exists(binRoot),
            $"{binRoot} does not exist, so this guard cannot read what Lite actually asks the .NET host for. " +
            "Lite.Tests references Lite, so building the suite builds it - if this fires, the build layout moved.");

        var configs = Directory.GetFiles(binRoot, "PerformanceMonitorLite.runtimeconfig.json", SearchOption.AllDirectories);
        var requirements = new List<FrameworkRequirement>();

        foreach (var configPath in configs)
        {
            using var document = JsonDocument.Parse(File.ReadAllText(configPath));

            if (!document.RootElement.TryGetProperty("runtimeOptions", out var runtimeOptions) ||
                !runtimeOptions.TryGetProperty("frameworks", out var frameworks))
            {
                /* Self-contained output (includedFrameworks), or a shape this does not understand. */
                continue;
            }

            foreach (var framework in frameworks.EnumerateArray())
            {
                var name = framework.GetProperty("name").GetString();
                var version = framework.GetProperty("version").GetString();
                Assert.False(string.IsNullOrWhiteSpace(name), $"Nameless framework entry in {configPath}");
                Assert.False(string.IsNullOrWhiteSpace(version), $"Versionless framework entry in {configPath}");

                var major = int.Parse(version!.Split('.')[0], CultureInfo.InvariantCulture);
                requirements.Add(new FrameworkRequirement(name!, major, configPath));
            }
        }

        Assert.True(
            requirements.Count > 0,
            $"No framework-dependent PerformanceMonitorLite.runtimeconfig.json found under {binRoot}. " +
            $"Searched {configs.Length} runtimeconfig file(s). Without one, nothing here is derived and the " +
            "docs would be guarded by a list instead of by the build.");

        return requirements;
    }

    /// <summary>The runtime names a reader would be told to download if a Lite artifact were framework-dependent.</summary>
    private static string[] DerivedInstallerNames() =>
        FrameworkDependentRequirements()
            .Where(r => InstallerNameFormats[r.Name].Length > 0)
            .Select(r => string.Format(CultureInfo.InvariantCulture, InstallerNameFormats[r.Name], r.Major))
            .Distinct(StringComparer.Ordinal)
            .ToArray();

    /// <summary>
    /// Every <c>dotnet publish</c> of Lite in one workflow, as output directory to "was it published
    /// <c>--self-contained</c>". This is what makes "neither artifact needs a runtime" a derived claim
    /// rather than a remembered one.
    /// </summary>
    private static Dictionary<string, bool> LitePublishShapes(string workflowPath)
    {
        var shapes = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);

        foreach (var rawLine in ParitySource.ReadFile(workflowPath).Split('\n'))
        {
            var line = rawLine.Trim();
            if (!line.Contains(PublishCommand, StringComparison.Ordinal))
            {
                continue;
            }

            var tokens = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            var outputFlag = Array.IndexOf(tokens, "-o");
            Assert.True(
                outputFlag >= 0 && outputFlag + 1 < tokens.Length,
                $"A Lite publish in {workflowPath} has no '-o <dir>' this guard can read: {line}");

            shapes[tokens[outputFlag + 1]] = line.Contains("--self-contained", StringComparison.Ordinal);
        }

        Assert.True(shapes.Count > 0, $"No '{PublishCommand}' line found in {workflowPath}.");
        return shapes;
    }

    /// <summary>Both workflows' Lite publish shapes in one dictionary, keyed by workflow path plus output dir.</summary>
    private static Dictionary<string, bool> AllLitePublishShapes()
    {
        var all = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);

        foreach (var workflow in new[] { BuildWorkflowPath, NightlyWorkflowPath })
        {
            foreach (var (dir, selfContained) in LitePublishShapes(workflow))
            {
                all[$"{workflow} -> {dir}"] = selfContained;
            }
        }

        return all;
    }

    /// <summary>
    /// Every runtime identifier a Lite publish pins with <c>-r</c>, across both workflows. Empty when
    /// every publish is RID-agnostic, which is the state the committed lock file was written for
    /// before #2501.
    /// </summary>
    private static string[] PinnedRuntimeIdentifiers() =>
        new[] { BuildWorkflowPath, NightlyWorkflowPath }
            .SelectMany(path => ParitySource.ReadFile(path).Split('\n'))
            .Select(l => l.Trim())
            .Where(l => l.Contains(PublishCommand, StringComparison.Ordinal))
            .Select(l => l.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            .Select(tokens => (Tokens: tokens, Flag: Array.IndexOf(tokens, "-r")))
            .Where(t => t.Flag >= 0 && t.Flag + 1 < t.Tokens.Length)
            .Select(t => t.Tokens[t.Flag + 1])
            .Distinct(StringComparer.Ordinal)
            .ToArray();

    /// <summary>
    /// The three places a reader can land on Lite's "what do I install first" answer, each already
    /// narrowed to the text that is about LITE. Lite's own README and the notice inside the ZIP are
    /// wholly Lite's; the root README is shared with Darling, so only its Lite section counts.
    /// </summary>
    private static IEnumerable<(string Doc, string Text)> LiteRuntimeDocScopes()
    {
        yield return (RootReadmePath, SectionOf(RootReadmePath, RootReadmeLiteHeading));
        yield return (LiteReadmePath, ParitySource.ReadFile(LiteReadmePath));
        yield return (ShippedNoticePath, ParitySource.ReadFile(ShippedNoticePath));
    }

    /// <summary>
    /// One markdown section: from <paramref name="heading"/> to the next same-level heading. Taken
    /// from the document's own structure rather than by line number, so re-ordering the README cannot
    /// silently move the window somewhere else.
    /// </summary>
    private static string SectionOf(string docPath, string heading)
    {
        var lines = ParitySource.ReadFile(docPath).Split('\n').Select(l => l.TrimEnd('\r')).ToArray();

        var start = Array.FindIndex(lines, l => l.Trim().Equals(heading, StringComparison.Ordinal));
        Assert.True(
            start >= 0,
            $"{docPath} has no '{heading}' heading. This guard scopes Lite's runtime claims to that " +
            "section; renaming it needs this constant updated in the same change, or the rule quietly " +
            "starts checking nothing.");

        var end = Array.FindIndex(lines, start + 1, l => l.StartsWith("## ", StringComparison.Ordinal));

        return string.Join('\n', lines[start..(end < 0 ? lines.Length : end)]);
    }

    /// <summary>The last path segment of a workflow path token, quotes and a trailing <c>/*</c> removed.</summary>
    private static string DirectoryLeaf(string token) =>
        Path.GetFileName(token.Trim('\'', '"').TrimEnd('*').TrimEnd('/', '\\'));

    /// <summary>Every line of a doc that mentions <paramref name="needle"/>.</summary>
    private static string[] LinesMentioning(string docPath, string needle) =>
        ParitySource.ReadFile(docPath)
            .Split('\n')
            .Select(l => l.TrimEnd('\r'))
            .Where(l => l.Contains(needle, StringComparison.OrdinalIgnoreCase))
            .ToArray();

    [Fact]
    public void TheBuiltRuntimeconfig_NamesOnlyFrameworksThisGuardCanTranslateToADownload()
    {
        /* The gate that keeps the rest of this file honest. If a new shared framework shows up in the
           runtimeconfig, somebody has to decide what an operator downloads for it and say so in the
           docs - silently skipping it is how the ASP.NET Core requirement went undocumented for a
           release in the first place. Still load-bearing after #2501: the day a publish goes back to
           framework-dependent, the sentences this table generates are the ones that have to be written. */
        var requirements = FrameworkDependentRequirements();

        var unmapped = requirements
            .Select(r => r.Name)
            .Distinct(StringComparer.Ordinal)
            .Where(name => !InstallerNameFormats.ContainsKey(name))
            .ToArray();

        Assert.True(
            unmapped.Length == 0,
            $"Lite's runtimeconfig now names framework(s) this guard has no download mapping for: " +
            $"{string.Join(", ", unmapped)}. Add the mapping to InstallerNameFormats, and decide whether " +
            $"{RootReadmePath}, {LiteReadmePath} and {ShippedNoticePath} have to name it - a prerequisite " +
            "nobody documents is #2489.");

        var majors = requirements.Select(r => r.Major).Distinct().ToArray();
        Assert.True(
            majors.Length == 1,
            $"Lite's runtimeconfig files disagree about the .NET major version ({string.Join(", ", majors)}), " +
            "so there is no single number the docs could be correct about.");
    }

    [Fact]
    public void EveryShippedLiteArtifact_IsPublishedSelfContained()
    {
        /* #2501. Both publishes are now -r win-x64 --self-contained, and that is what entitles every
           other assertion here to say the answer to "what do I install first" is "nothing". It is also
           the SMALLER artifact, counter-intuitively: the RID-agnostic publish carried 537 MB of
           runtimes\ for platforms Windows can never load, and dropping them beat the cost of bundling
           the runtime by roughly two to one - 565 MB tree / 212.7 MB zipped became 277 MB / 114.2 MB,
           measured on one commit and one SDK.

           If this goes red, the change that made a publish framework-dependent again also has to put
           the prerequisites back into all three docs; the other facts here will say so individually. */
        var frameworkDependent = AllLitePublishShapes()
            .Where(kv => !kv.Value)
            .Select(kv => kv.Key)
            .ToArray();

        Assert.True(
            frameworkDependent.Length == 0,
            $"These Lite publishes are framework-dependent: {string.Join(", ", frameworkDependent)}. " +
            "Every artifact Lite ships is supposed to carry its own runtime (#2501), and the docs say so " +
            "in three places. Either restore --self-contained, or restore the prerequisites sections in " +
            $"{RootReadmePath}, {LiteReadmePath} and {ShippedNoticePath} in the same change.");
    }

    [Fact]
    public void TheDocsNameARuntimeDownload_IfAndOnlyIfSomeArtifactIsFrameworkDependent()
    {
        /* The assertion #2499 got half right. It checked that the docs DID name the runtimes, so when
           #2501 flipped the publish it stayed green while the prose went stale - the exact "docs drift
           from the artifact" failure this file exists to stop, just in the other direction. Stated as
           an if-and-only-if, both directions are covered by one fact: today every publish is
           self-contained, so naming a runtime download in these three places is a demand for something
           nobody needs; revert a publish and the same fact demands the sentences come back, with the
           version derived from the build rather than remembered. */
        var anyFrameworkDependent = AllLitePublishShapes().Any(kv => !kv.Value);
        var expected = DerivedInstallerNames();

        Assert.True(
            expected.Length >= 2,
            $"Expected a framework-dependent Lite build to need at least the Desktop and ASP.NET Core " +
            $"runtimes; derived only: {string.Join(", ", expected)}");

        foreach (var (doc, text) in LiteRuntimeDocScopes())
        {
            foreach (var installer in expected)
            {
                var named = text.Contains(installer, StringComparison.OrdinalIgnoreCase);

                if (anyFrameworkDependent)
                {
                    Assert.True(
                        named,
                        $"{doc} never names '{installer}', which the built runtimeconfig says a " +
                        "framework-dependent Lite cannot start without. The .NET host reports only the " +
                        "FIRST missing framework, so a half-documented pair costs the reader a second " +
                        "identical failure.");
                }
                else
                {
                    Assert.False(
                        named,
                        $"{doc} tells a reader to install '{installer}', but every Lite publish is " +
                        "--self-contained (#2501), so both artifacts carry their own runtime and there is " +
                        "nothing to install. Delete the sentence rather than softening it - a download " +
                        "instruction nobody needs is the #2489 defect with the sign flipped.");
                }
            }
        }
    }

    [Fact]
    public void NoDocLineNamingAnArtifact_HangsARuntimeRequirementOnIt()
    {
        /* THE #2489 defect, generalised. It was README.md:95 reading "(requires .NET 10 Desktop
           Runtime)" on the Setup.exe download line - the one artifact that needed nothing - which both
           burdened the recommended path with a download and left the impression the requirement had
           been handled. After #2501 the same is true of the ZIP line, so the rule is per-ARTIFACT
           rather than per artifact-name: no line naming either download may carry a runtime demand, and
           every such line must say self-contained so a reader can see why there is not one. */
        Assert.False(
            AllLitePublishShapes().Any(kv => !kv.Value),
            "A Lite publish is framework-dependent, so this fact's premise no longer holds. " +
            "EveryShippedLiteArtifact_IsPublishedSelfContained names which one.");

        foreach (var artifact in new[] { SetupExeName, PortableZipName })
        {
            var mentions = LinesMentioning(RootReadmePath, artifact)
                .Concat(LinesMentioning(LiteReadmePath, artifact))
                .ToArray();

            Assert.True(mentions.Length > 0, $"No doc line mentions {artifact}; the download instructions moved.");

            foreach (var line in mentions)
            {
                Assert.True(
                    line.Contains("self-contained", StringComparison.OrdinalIgnoreCase),
                    $"A line naming {artifact} does not say it is self-contained, which is the one fact that " +
                    $"tells a reader they need install nothing: {line.Trim()}");

                foreach (var phrase in RuntimeDemandPhrases)
                {
                    Assert.False(
                        line.Contains(phrase, StringComparison.OrdinalIgnoreCase),
                        $"A line naming {artifact} says '{phrase}', but it is packed from a --self-contained " +
                        $"publish and has no runtime prerequisite: {line.Trim()}");
                }
            }
        }
    }

    [Fact]
    public void SetupExeIsPackedFromTheVelopackPublish_AndTheZipFromTheOtherOne()
    {
        /* Both doc claims rest on which publish feeds which artifact, and that wiring is three hops of
           YAML away from the sentence it justifies. Derived on both ends rather than pinned as
           literals: the Velopack pack source and the release ZIP source each have to keep matching a
           publish directory. Before #2501 the two publishes were told apart by their SHAPE; now that
           both are self-contained the discriminator is the vpk pack line itself - whatever directory it
           packs is the Velopack tree, and the ZIP has to come from the other one. Repoint either and
           the docs become wrong silently, which is the failure this whole file exists for. */
        var shapes = LitePublishShapes(BuildWorkflowPath);

        Assert.True(
            shapes.Count == 2,
            $"Expected exactly two Lite publishes in {BuildWorkflowPath} (the ZIP's and the Velopack one); " +
            $"found {shapes.Count}: {string.Join(", ", shapes.Keys)}.");

        var workflow = ParitySource.ReadFile(BuildWorkflowPath);

        var packLine = workflow.Split('\n')
            .Select(l => l.Trim())
            .Single(l => l.Contains("vpk pack -u PerformanceMonitorLite", StringComparison.Ordinal));

        var packTokens = packLine.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var packFlag = Array.IndexOf(packTokens, "-p");
        Assert.True(packFlag >= 0 && packFlag + 1 < packTokens.Length, $"No '-p <dir>' on the vpk pack line: {packLine}");

        var velopackLeaf = DirectoryLeaf(packTokens[packFlag + 1]);

        Assert.Single(shapes.Keys, d => DirectoryLeaf(d).Equals(velopackLeaf, StringComparison.OrdinalIgnoreCase));
        var zipSourceDir = Assert.Single(shapes.Keys, d => !DirectoryLeaf(d).Equals(velopackLeaf, StringComparison.OrdinalIgnoreCase));

        var zipLines = workflow.Split('\n')
            .Select(l => l.Trim())
            .Where(l => l.Contains("Compress-Archive", StringComparison.Ordinal) &&
                        l.Contains("PerformanceMonitorLite-", StringComparison.Ordinal))
            .ToArray();

        Assert.True(zipLines.Length > 0, $"Nothing in {BuildWorkflowPath} builds a PerformanceMonitorLite ZIP.");

        foreach (var zipLine in zipLines)
        {
            var zipTokens = zipLine.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            var pathFlag = Array.IndexOf(zipTokens, "-Path");
            Assert.True(pathFlag >= 0 && pathFlag + 1 < zipTokens.Length, $"No '-Path <dir>' on: {zipLine}");

            /* Release re-zips from signed/Lite, the SignPath round-trip of publish/Lite; same leaf, and
               that is the property worth asserting - the zip must never come from the velopack tree. */
            Assert.Equal(
                DirectoryLeaf(zipSourceDir),
                DirectoryLeaf(zipTokens[pathFlag + 1]),
                ignoreCase: true);
        }
    }

    [Fact]
    public void TheNightlyZipHasTheSameShapeAsTheReleaseZip_SoOneAnswerCoversBoth()
    {
        /* The nightly is the UAT download, and it publishes Lite itself rather than reusing build.yml's
           step. If the two workflows ever disagree about the publish shape, the one answer the docs give
           is wrong for one of them - and it is the nightly that would be wrong, because it is the
           artifact with no Setup.exe alternative to fall back on. */
        var releaseShapes = LitePublishShapes(BuildWorkflowPath);
        var nightlyShapes = LitePublishShapes(NightlyWorkflowPath);

        foreach (var (dir, nightlySelfContained) in nightlyShapes)
        {
            Assert.True(
                releaseShapes.TryGetValue(dir, out var releaseSelfContained),
                $"{NightlyWorkflowPath} publishes Lite to {dir}, which {BuildWorkflowPath} does not, so the " +
                "two artifacts no longer share one documented answer.");

            Assert.Equal(releaseSelfContained, nightlySelfContained);
        }

        var zipSourceDir = releaseShapes.Keys
            .Single(d => !DirectoryLeaf(d).Contains("velopack", StringComparison.OrdinalIgnoreCase));

        Assert.True(
            nightlyShapes.ContainsKey(zipSourceDir),
            $"{NightlyWorkflowPath} does not publish Lite to {zipSourceDir}, so the nightly ZIP is built from " +
            "a different tree than the release ZIP.");
    }

    [Fact]
    public void TheCommittedLockFile_CoversEveryRuntimeIdentifierTheLitePublishesPin()
    {
        /* The blocker #2501 turned up, and the reason that change is three files rather than one.

           A RID publish restores a RID graph, and restore then REWRITES packages.lock.json to add a
           "<tfm>/<rid>" target. Both workflows run `dotnet restore <this project> --locked-mode` BEFORE
           they publish (build.yml, nightly.yml), and locked mode compares the PROJECT's runtime
           identifiers against the LOCK FILE's: with the RID living only on the publish command line
           that comparison is "<empty>" against "win-x64", and the restore dies NU1004. Reproduced
           locally, and it would fire on every PR - not some future --no-restore trap.

           The fix is <RuntimeIdentifiers> in the csproj, so the project itself asks for the graph and
           one committed lock file satisfies both the RID-less locked-mode restore and the RID publish.
           This fact holds the three halves together: pin a new RID in a workflow and it stays red until
           the csproj and the lock file follow. */
        var rids = PinnedRuntimeIdentifiers();

        Assert.True(
            rids.Length > 0,
            $"No Lite publish in either workflow pins a RID with -r. If that is deliberate, the " +
            $"<RuntimeIdentifiers> line in {LiteProjectPath} and the RID targets in {LiteLockFilePath} are " +
            "now dead weight and should go in the same change.");

        var csproj = ParitySource.ReadFile(LiteProjectPath);
        var lockFile = ParitySource.ReadFile(LiteLockFilePath);

        foreach (var rid in rids)
        {
            Assert.True(
                csproj.Contains("<RuntimeIdentifiers>", StringComparison.Ordinal) &&
                csproj.Contains($"<RuntimeIdentifiers>{rid}</RuntimeIdentifiers>", StringComparison.Ordinal),
                $"A Lite publish pins -r {rid}, but {LiteProjectPath} does not declare exactly that in " +
                "<RuntimeIdentifiers>. Without it the project's RID set is empty while the committed lock " +
                $"file's is {rid}, and every --locked-mode restore in CI fails NU1004 before the publish " +
                "ever runs.");

            Assert.True(
                lockFile.Contains($"/{rid}\"", StringComparison.Ordinal),
                $"A Lite publish pins -r {rid}, but {LiteLockFilePath} has no \"<tfm>/{rid}\" target. " +
                "Regenerate it with `dotnet restore Lite/PerformanceMonitorLite.csproj --force-evaluate` " +
                "and commit the result; shipping a RID shape the committed lock file does not cover is a " +
                "locked-mode failure waiting for the next CI run.");
        }
    }

    [Fact]
    public void TheZipsSection_SaysItCarriesItsOwnRuntimeRatherThanListingDownloads()
    {
        /* Lite/README.md had no prerequisites section at all before #2499 - zero hits for "prerequisite",
           ".NET 10", "Desktop Runtime" or "ASP.NET". Someone reading the Lite folder had nowhere to learn
           any of this, which was half of #2489. #2501 did not delete the section, because the root README
           links its anchor and because "nothing to install" is itself the answer a reader came for; it
           changed what the section has to say. */
        var liteReadme = ParitySource.ReadFile(LiteReadmePath);
        var anyFrameworkDependent = AllLitePublishShapes().Any(kv => !kv.Value);

        Assert.True(
            liteReadme.Contains("## Prerequisites", StringComparison.Ordinal),
            $"{LiteReadmePath} has no '## Prerequisites' section. The root README links to its anchor.");

        if (anyFrameworkDependent)
        {
            Assert.True(
                liteReadme.Contains("framework-dependent", StringComparison.OrdinalIgnoreCase),
                $"{LiteReadmePath} does not say which artifact is framework-dependent, which would be the " +
                "reason it has prerequisites at all.");
        }
        else
        {
            Assert.True(
                liteReadme.Contains("self-contained", StringComparison.OrdinalIgnoreCase),
                $"{LiteReadmePath} does not say the artifacts are self-contained, which is the whole content " +
                "of its Prerequisites section now that neither needs a runtime.");
        }

        foreach (var doc in new[] { RootReadmePath, LiteReadmePath })
        {
            Assert.True(
                LinesMentioning(doc, PortableZipName).Length > 0,
                $"{doc} never names the portable ZIP, so what it does and does not need is attached to nothing.");
        }
    }

    [Fact]
    public void TheRuntimeNotice_ShipsInsideTheZipBesideTheExe()
    {
        /* Kept from #2499 with its subject changed rather than deleted. Lite cannot pre-check the way
           Darling's install-darling.ps1 does: there is no install script, and before #2501 the host error
           preceded our code. The unzipped folder is still the only surface a reader has once they are
           looking at the files rather than the repo, so the notice still has to be COPIED to the publish
           output - a file that exists in the repo and never ships is worse than none, because both
           READMEs promise it is there. What it says changed with the publish shape; that it ships did not. */
        var noticePath = Path.Combine(ParitySource.RepoRoot(), ShippedNoticePath.Replace('/', Path.DirectorySeparatorChar));
        Assert.True(File.Exists(noticePath), $"{ShippedNoticePath} is missing.");

        var csproj = ParitySource.ReadFile(LiteProjectPath);
        var noticeFileName = Path.GetFileName(ShippedNoticePath);

        var itemIndex = csproj.IndexOf($"<None Update=\"{noticeFileName}\">", StringComparison.Ordinal);
        Assert.True(
            itemIndex >= 0,
            $"PerformanceMonitorLite.csproj has no <None Update=\"{noticeFileName}\"> item, so it never lands " +
            "beside the exe and both READMEs promise a file that is not in the ZIP.");

        var itemEnd = csproj.IndexOf("</None>", itemIndex, StringComparison.Ordinal);
        Assert.True(itemEnd > itemIndex, $"Unterminated <None> item for {noticeFileName}.");

        Assert.True(
            csproj[itemIndex..itemEnd].Contains("<CopyToOutputDirectory>", StringComparison.Ordinal),
            $"{noticeFileName} is declared but not copied to the output directory.");

        /* And it has to name both artifacts, because it is the copy a reader reaches for when they went
           looking for a prerequisite and there is not one. */
        var notice = ParitySource.ReadFile(ShippedNoticePath);
        foreach (var artifact in new[] { SetupExeName, PortableZipName })
        {
            Assert.True(
                notice.Contains(artifact, StringComparison.Ordinal),
                $"{ShippedNoticePath} does not name {artifact}, so a reader cannot tell whether it is about " +
                "the build they have.");
        }
    }
}
