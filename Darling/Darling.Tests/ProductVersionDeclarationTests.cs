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
using System.Text.RegularExpressions;
using System.Xml.Linq;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// One place in this repository declares a product version, and every consumer of a version reads a value
/// that resolves to it (#3222).
///
/// <para><b>The defect was not that the numbers differed, it was that nothing noticed.</b> Six csproj files
/// each declared their own version property: four at 3.6.0 and the two deprecated Installer projects at
/// 3.3.0, three releases behind. <c>check-version-bump.yml</c> gated the release on
/// <c>deprecated/Dashboard/Dashboard.csproj</c>; <c>nightly.yml</c> named every artifact from
/// <c>Lite/PerformanceMonitorLite.csproj</c>; the two Darling projects were read by neither; and no test
/// compared any of them. Bumping the file the gate read let a release merge with every artifact still
/// carrying the old version, and bumping the file the nightly read failed the gate on a correctly-versioned
/// build.</para>
///
/// <para><b>So these pins hold the invariant that survives the fix, not the six literals the fix
/// deleted.</b> A pin asserting the six agreed would have been deleted along with them. The routes by which
/// "one declaration, and every reader resolves to it" can be violated are enumerated rather than sampled,
/// because reading the assertions finds a wrong one and only enumerating the routes finds a missing one:
/// a second version property appears (<see cref="ExactlyOneMsBuildProperty_DeclaresTheProductVersion"/>); a
/// project hand-sets a property that DERIVES from it and so wins the stamp
/// (<see cref="NoProjectHandSetsADerivedVersionProperty"/>); the declaration moves off the repository root,
/// or a second <c>Directory.Build.props</c> shadows it for a subtree, either of which leaves projects
/// inheriting nothing (both in the first pin); a consumer reads a path that is not the declaration
/// (<see cref="EveryVersionRead_NamesTheDeclarationFile"/>); the detector that judges those consumers stops
/// detecting (<see cref="TheReadDetector_SeesEachShippedShapeAndReportsAWrongPath"/>); and the declaration
/// file resolves for one reader shape but not another
/// (<see cref="EveryShippedTextReadPattern_AgreesWithTheXmlRead"/>).</para>
///
/// <para><b>The reader pins are the half that would have caught this.</b> The gate and the nightly
/// disagreeing was the actual bug and no test had ever looked at a workflow file. They matter more after the
/// fix than before it: an INHERITED property is not present in the inheriting project's XML, so a read left
/// pointing at a csproj returns EMPTY rather than failing. Every one of these nine reads only names an
/// artifact, so an empty result produces <c>PerformanceMonitorLite-.zip</c> and a green build. That is not a
/// hypothetical either — <c>nightly.yml</c>'s own header records the 2026-07-26 nightly dying on a version
/// read whose path had moved (#1550/#1551 before it), and that variant at least had the decency to be loud,
/// because <c>Get-Content</c> on a MISSING file throws while <c>Get-Content</c> on a present file with no
/// such element returns nothing.</para>
/// </summary>
public class ProductVersionDeclarationTests
{
    /// <summary>The one file that declares a product version, repo-relative with forward slashes.</summary>
    private const string DeclarationFile = "Directory.Build.props";

    /// <summary>
    /// The version-source paths a reader may name. Two entries are checkouts of <c>main</c> rather than
    /// paths in this tree: <c>check-version-bump.yml</c> compares the PR's version against main's, and
    /// main does not carry <see cref="DeclarationFile"/> until the release that introduces it lands there,
    /// so the gate falls back to the file it used to read. A path under <c>main-branch/</c> cannot be
    /// validated against this tree at all, which is why the allowance is three named literals and not a
    /// prefix: a fourth arriving fails, and the fallback is meant to be deleted once main has the file.
    /// </summary>
    private static readonly string[] s_permittedVersionSources =
    {
        DeclarationFile,
        "main-branch/" + DeclarationFile,
        "main-branch/deprecated/Dashboard/Dashboard.csproj",
    };

    /// <summary>The MSBuild properties the SDK DERIVES from the declared version.</summary>
    private static readonly string[] s_derivedVersionProperties =
    {
        "AssemblyVersion",
        "FileVersion",
        "InformationalVersion",
    };

    /// <summary>
    /// Exactly one MSBuild property element in the tree declares a product version, it lives in
    /// <see cref="DeclarationFile"/> at the repository root, and its value is a version.
    ///
    /// <para><b>Counted by parsing XML, not by matching text.</b> The issue this closes was itself
    /// mis-counted by a grep: searching for <c>3.6.0</c> found four of six declarations, because a search
    /// shaped like the expected answer returns a confident partial count and no error. Text matching fails
    /// the other way here too — the three shipped csprojs each carried a COMMENT quoting the property name,
    /// so a substring count reported three per file where there was one. An element walk cannot see a
    /// comment, and scoping to elements whose parent is a <c>PropertyGroup</c> keeps a
    /// <c>PackageReference</c>'s nested version spelling out of the count.</para>
    ///
    /// <para><b>Root placement and shadowing are part of the same property.</b> A declaration that moved to
    /// a subdirectory would still be the only one, and every project outside that subdirectory would stamp
    /// the SDK's 1.0.0 default. A second <c>Directory.Build.props</c> below the root is worse, because
    /// MSBuild imports only the NEAREST one: a shadowing file that declares nothing takes the version away
    /// from its whole subtree while adding no declaration for the count to catch.</para>
    /// </summary>
    [Fact]
    public void ExactlyOneMsBuildProperty_DeclaresTheProductVersion()
    {
        var scanned = 0;
        var declarations = new List<string>();

        foreach (var file in MsBuildFiles())
        {
            scanned++;

            foreach (var value in PropertyValues(file, "Version"))
            {
                declarations.Add($"{RepoRelative(file)} = {value}");
            }
        }

        /* Non-vacuity: the walk read the tree's MSBuild files. A glob matching nothing would report zero
           declarations, which fails the count below rather than passing it — but it would fail for the wrong
           reason and send the next reader to the wrong file, so the floor says which one it was. */
        Assert.True(scanned >= 20, $"only {scanned} MSBuild files were scanned; the walk is not reading the tree");

        Assert.True(
            declarations.Count == 1,
            $"Exactly one MSBuild property may declare a product version, and it belongs in {DeclarationFile}. "
            + $"Found {declarations.Count}:\n  " + string.Join("\n  ", declarations));

        Assert.StartsWith(DeclarationFile + " = ", declarations[0], StringComparison.Ordinal);

        var declared = declarations[0][(DeclarationFile.Length + 3)..];
        Assert.True(
            Version.TryParse(declared, out _),
            $"The declared product version does not parse as a version: '{declared}'");

        /* The shadowing route. Path.GetRelativePath is used rather than a name comparison so a file called
           Directory.Build.props in a subdirectory is reported by the path that makes the problem obvious. */
        var shadowing = Directory
            .GetFiles(Root, "Directory.Build.props", SearchOption.AllDirectories)
            .Where(path => !IsBuildOutput(path))
            .Select(RepoRelative)
            .Where(relative => !relative.Equals(DeclarationFile, StringComparison.Ordinal))
            .OrderBy(relative => relative, StringComparer.Ordinal)
            .ToList();

        Assert.True(
            shadowing.Count == 0,
            "MSBuild imports only the NEAREST Directory.Build.props, so one below the root takes the "
            + $"product version away from its whole subtree even when it declares nothing:\n  {string.Join("\n  ", shadowing)}");
    }

    /// <summary>
    /// No project hand-sets <c>AssemblyVersion</c>, <c>FileVersion</c> or <c>InformationalVersion</c>.
    ///
    /// <para>These three DERIVE from the declared version at build time, and a hand-set copy silently wins
    /// the stamp. #2113 established that and deleted them from the three shipped projects, having proved it
    /// from an artifact: the 3.4.0 release bumped the version and shipped binaries still stamped 3.3.0.0.
    /// The three deprecated projects were never included, so the trap stayed live in exactly the place it
    /// did the most damage — <c>deprecated/Dashboard/Dashboard.csproj</c> declared 3.6.0 while its binary
    /// stamped 3.3.0.0, and that file was what the release gate took its judgement from. Measured on the
    /// change that added this pin, deleting those pins moved the Dashboard binary 3.3.0.0 to 3.6.0.0 with
    /// no other edit to it.</para>
    ///
    /// <para>Held separately from the declaration count because it is a different violation route with the
    /// same outcome: the count can be exactly one and the artifact still stamp something else.</para>
    /// </summary>
    [Fact]
    public void NoProjectHandSetsADerivedVersionProperty()
    {
        var scanned = 0;
        var offending = new List<string>();

        foreach (var file in MsBuildFiles())
        {
            scanned++;

            foreach (var property in s_derivedVersionProperties)
            {
                foreach (var value in PropertyValues(file, property))
                {
                    offending.Add($"{RepoRelative(file)}: <{property}>{value}</{property}>");
                }
            }
        }

        Assert.True(scanned >= 20, $"only {scanned} MSBuild files were scanned; the walk is not reading the tree");

        Assert.True(
            offending.Count == 0,
            "AssemblyVersion / FileVersion / InformationalVersion derive from the declared version at build "
            + "time. A hand-set copy wins the stamp, which is how a release bump ships binaries carrying the "
            + $"previous version (#2113):\n  {string.Join("\n  ", offending)}");
    }

    /// <summary>
    /// Every version read in the CI workflows and the repository's build scripts names
    /// <see cref="DeclarationFile"/>.
    ///
    /// <para><b>This is the pin that would have caught the reported defect</b>, and nothing had ever looked
    /// at a workflow file. It is also the pin the fix makes load-bearing rather than tidy: with the version
    /// inherited, a read still pointing at a csproj finds no such element and yields an EMPTY string, and
    /// since every one of these reads only names an artifact, the run stays green and the artifact is named
    /// wrongly.</para>
    ///
    /// <para>Scanned across workflows AND the root <c>.cmd</c> scripts, because narrowing to the workflows
    /// would have left three reads of the same fact unexamined next door — and two of those three were
    /// pointing at a path that had not existed since #1612 moved it, which is the live proof that this class
    /// of read rots silently.</para>
    /// </summary>
    [Fact]
    public void EveryVersionRead_NamesTheDeclarationFile()
    {
        var scanned = 0;
        var reads = 0;
        var offending = new List<string>();

        foreach (var file in VersionReadingSources())
        {
            scanned++;
            var text = File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal);
            var name = RepoRelative(file);

            foreach (var read in VersionReads(text))
            {
                reads++;

                foreach (var source in read.Sources)
                {
                    if (!s_permittedVersionSources.Contains(source, StringComparer.Ordinal))
                    {
                        offending.Add($"{name}: reads a version from '{source}' — {read.Line.Trim()}");
                    }
                }
            }
        }

        /* Non-vacuity, and the reason both floors are here rather than one. A crippled GLOB scans no files
           and finds no reads; a crippled DETECTOR scans every file and finds no reads. Either leaves the
           emptiness assertion below satisfied by having nothing to judge, and the two floors fail
           differently, so the message names which happened. The read count is the number this tree holds,
           not a token positive number: six XML selects and three text scrapes. */
        Assert.True(scanned >= 5, $"only {scanned} candidate files were scanned; the glob is not reading the tree");
        Assert.True(reads >= 9, $"only {reads} version reads were detected across {scanned} files; the detector is not detecting");

        Assert.True(
            offending.Count == 0,
            "A version read naming anything but the one file that declares one. An inherited property is "
            + "not in the inheriting project's XML, so such a read returns EMPTY and names the artifact "
            + $"with nothing rather than going red:\n  {string.Join("\n  ", offending)}");
    }

    /// <summary>
    /// The detector sees each shape this repository actually uses, reports a read whose path is wrong, and
    /// does not score a line that merely names a project file.
    ///
    /// <para><b>Without this, the repository scan above is decoration.</b> A detector narrowed until it
    /// matches nothing passes the emptiness assertion, and one widened until it accepts every path passes it
    /// too; the floors catch the first and only a case with a KNOWN-BAD path catches the second. That exact
    /// vacuity has shipped in this repository more than once, so the wrong-path row is the point of this
    /// theory and the rest is coverage of the shapes it has to survive.</para>
    /// </summary>
    [Theory]
    // The pwsh XML select, as four workflow steps and two .cmd scripts spell it.
    [InlineData(
        "          $version = ([xml](Get-Content Directory.Build.props)).Project.PropertyGroup.Version | Where-Object { $_ }",
        "Directory.Build.props")]
    // The same shape pointing at a project file: the defect, which must be REPORTED and not excused.
    [InlineData(
        "          $base = ([xml](Get-Content Lite/PerformanceMonitorLite.csproj)).Project.PropertyGroup.Version | Where-Object { $_ }",
        "Lite/PerformanceMonitorLite.csproj")]
    [InlineData(
        "          $version = ([xml](Get-Content deprecated/Dashboard/Dashboard.csproj)).Project.PropertyGroup.Version | Where-Object { $_ }",
        "deprecated/Dashboard/Dashboard.csproj")]
    // The bash lookbehind scrape, both quotings build.yml and nightly.yml use.
    [InlineData(
        "          base=$(grep -oPm1 '(?<=<Version>)[^<]+' Directory.Build.props)",
        "Directory.Build.props")]
    [InlineData(
        "          version=\"$(grep -oPm1 '(?<=<Version>)[^<]+' Lite/PerformanceMonitorLite.csproj)\"",
        "Lite/PerformanceMonitorLite.csproj")]
    // The cmd findstr scrape, with its backslash separator normalised.
    [InlineData(
        "for /f \"tokens=2 delims=<>\" %%a in ('findstr \"<Version>\" Directory.Build.props') do set VERSION=%%a",
        "Directory.Build.props")]
    [InlineData(
        "for /f \"tokens=2 delims=<>\" %%a in ('findstr \"<Version>\" Dashboard\\Dashboard.csproj') do set VERSION=%%a",
        "Dashboard/Dashboard.csproj")]
    // The cmd wrapper around the pwsh select.
    [InlineData(
        "for /f %%a in ('powershell -Command \"([xml](Get-Content Directory.Build.props)).Project.PropertyGroup.Version | Where-Object { $_ }\"') do set VERSION=%%a",
        "Directory.Build.props")]
    public void TheReadDetector_SeesEachShippedShapeAndReportsAWrongPath(string line, string expected)
    {
        var reads = VersionReads(line);

        var read = Assert.Single(reads);
        Assert.Equal(new[] { expected }, read.Sources);
    }

    /// <summary>
    /// A line that names a project file without reading a version out of it is not a version read, and a
    /// variable-indirect read resolves to the literals assigned to that variable.
    ///
    /// <para>The negative cases are what stop the detector degenerating into "any line mentioning a
    /// csproj": <c>build.yml</c> alone names project files on dozens of restore and publish lines, and a
    /// detector that scored those would report the whole workflow as offending and be turned off.</para>
    /// </summary>
    [Theory]
    [InlineData("        run: dotnet publish Lite/PerformanceMonitorLite.csproj -c Release -o publish/Lite", 0)]
    [InlineData("          dotnet restore Lite.Tests/Lite.Tests.csproj --locked-mode", 0)]
    [InlineData("              - 'Darling/**/!(*.md)'", 0)]
    [InlineData("          $version = ([xml](Get-Content Directory.Build.props)).Project.PropertyGroup.Version", 1)]
    public void TheReadDetector_DoesNotScoreALineThatMerelyNamesAProject(string line, int expected) =>
        Assert.Equal(expected, VersionReads(line).Count);

    /// <summary>
    /// A read through a variable resolves to every literal that variable is assigned in the same file, so
    /// the gate's fallback chain is judged rather than skipped.
    /// </summary>
    [Fact]
    public void TheReadDetector_ResolvesAVariableToItsAssignedLiterals()
    {
        const string Snippet =
            "          $path = 'main-branch/Directory.Build.props'\n"
          + "          if (-not (Test-Path $path)) { $path = 'main-branch/deprecated/Dashboard/Dashboard.csproj' }\n"
          + "          $version = ([xml](Get-Content $path)).Project.PropertyGroup.Version | Where-Object { $_ }\n";

        var read = Assert.Single(VersionReads(Snippet));

        Assert.Equal(
            new[] { "main-branch/Directory.Build.props", "main-branch/deprecated/Dashboard/Dashboard.csproj" },
            read.Sources);
    }

    /// <summary>
    /// Every text-scraping read in the tree, run as its OWN shipped pattern against the declaration file,
    /// returns the same value the XML selects return.
    ///
    /// <para><b>This holds the ordering inside the declaration file, which nothing else can.</b> The XML
    /// readers select an element and are indifferent to comments; the text readers take the FIRST match in
    /// the file, so a comment spelling the property name ahead of the property itself is what they return.
    /// That is not a hypothesis: the declaration file was written comment-first, and all three text reads
    /// came back with the word <c>from</c> out of the prose while the six XML selects still returned the
    /// version. Nothing would have gone red — naming an artifact is all those reads do.</para>
    ///
    /// <para>The pattern is extracted from the workflow and script source rather than retyped here. A
    /// retyped copy proves the transcription works and keeps passing while the shipped one drifts.</para>
    /// </summary>
    [Fact]
    public void EveryShippedTextReadPattern_AgreesWithTheXmlRead()
    {
        var declaration = PathTo(DeclarationFile);
        var text = File.ReadAllText(declaration);

        var expected = PropertyValues(declaration, "Version").Single();
        Assert.False(string.IsNullOrWhiteSpace(expected));

        var patterns = new List<(string Source, string Pattern)>();

        foreach (var file in VersionReadingSources())
        {
            var name = RepoRelative(file);
            var lines = File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');

            for (var i = 0; i < lines.Length; i++)
            {
                /* The lookbehind scrape, lifted verbatim out of the single-quoted grep argument. */
                var lookbehind = Regex.Match(lines[i], @"grep\s+-oPm1\s+'([^']+)'");
                if (lookbehind.Success)
                {
                    patterns.Add(($"{name}:{i + 1}", lookbehind.Groups[1].Value));
                }

                /* findstr takes a literal, not a pattern: the value is the third <>-delimited token of the
                   first matching line, which is what `tokens=2 delims=<>` extracts. Modelled as the
                   equivalent pattern so both shapes are judged by one comparison. */
                var findstr = Regex.Match(lines[i], @"findstr\s+""(<[A-Za-z]+>)""");
                if (findstr.Success)
                {
                    patterns.Add(($"{name}:{i + 1}", $"(?<={Regex.Escape(findstr.Groups[1].Value)})[^<]+"));
                }
            }
        }

        /* Non-vacuity: three text reads exist in this tree — build.yml's and nightly.yml's linux jobs, and
           package-release.cmd. Extracting none would leave the loop below asserting nothing at all. */
        Assert.True(
            patterns.Count >= 3,
            $"only {patterns.Count} text-scraping version-read patterns were extracted; the extraction is not reading the source");

        foreach (var (source, pattern) in patterns)
        {
            var match = Regex.Match(text, pattern);

            Assert.True(
                match.Success,
                $"{source}'s own pattern /{pattern}/ matches nothing in {DeclarationFile}, so that read yields "
                + "an empty version and names its artifact with nothing.");

            Assert.True(
                match.Value.Trim().Equals(expected, StringComparison.Ordinal),
                $"{source}'s own pattern /{pattern}/ reads '{match.Value}' from {DeclarationFile} where the "
                + $"XML select reads '{expected}'. The text readers take the FIRST match in the file, so a "
                + "comment naming the property ahead of the property itself is what they return.");
        }
    }

    /// <summary>One detected version read: the line it sits on, and the version-source paths it resolves
    /// to. Plural because a read through a variable resolves to every literal assigned to it.</summary>
    private sealed record VersionRead(string Line, IReadOnlyList<string> Sources);

    /// <summary>
    /// The version reads in a block of text.
    ///
    /// <para>A line is a read when it selects the version property out of parsed MSBuild XML, or scrapes
    /// the property's angle-bracket spelling out of MSBuild source as text. Both spellings are load-bearing
    /// and neither implies the other: four workflow steps and two scripts use the first, and three use the
    /// second. Naming a project file is deliberately NOT sufficient — see
    /// <see cref="TheReadDetector_DoesNotScoreALineThatMerelyNamesAProject"/>.</para>
    /// </summary>
    private static List<VersionRead> VersionReads(string text)
    {
        var normalised = text.Replace("\r\n", "\n", StringComparison.Ordinal);
        var lines = normalised.Split('\n');
        var found = new List<VersionRead>();

        foreach (var line in lines)
        {
            var selectsProperty = line.Contains("PropertyGroup.Version", StringComparison.Ordinal);
            var scrapesSource = line.Contains("<Version>", StringComparison.Ordinal);

            if (!selectsProperty && !scrapesSource)
            {
                continue;
            }

            var sources = MsBuildPathsIn(line);

            if (sources.Count == 0)
            {
                /* A read through a variable. Resolve it to the literals that variable is assigned in this
                   same text, so the gate's transitional fallback is judged rather than waved through. */
                var variable = Regex.Match(line, @"Get-Content\s+\$(\w+)");
                if (variable.Success)
                {
                    sources = Regex
                        .Matches(normalised, @"\$" + Regex.Escape(variable.Groups[1].Value) + @"\s*=\s*['""]([^'""]+)['""]")
                        .Select(match => Normalise(match.Groups[1].Value))
                        .Where(candidate => candidate.Length > 0)
                        .Distinct(StringComparer.Ordinal)
                        .ToList();
                }
            }

            /* A line that reads a version and resolves to no path at all is reported as a read with no
               source, which fails the caller's membership check rather than vanishing from the count. */
            found.Add(new VersionRead(line, sources.Count == 0 ? new[] { "(unresolved)" } : sources));
        }

        return found;
    }

    /// <summary>The MSBuild file paths named on one line, separators normalised to forward slashes.</summary>
    private static List<string> MsBuildPathsIn(string line) =>
        Regex
            .Matches(line, @"[A-Za-z0-9_./\\-]+\.(?:csproj|props|targets)\b")
            .Select(match => Normalise(match.Value))
            .Distinct(StringComparer.Ordinal)
            .ToList();

    private static string Normalise(string path) => path.Replace('\\', '/').Trim();

    /// <summary>
    /// The files that may perform a version read: the CI workflows, and the build and packaging scripts at
    /// the repository root. Ordered so a failure message reads the same on every host.
    /// </summary>
    private static List<string> VersionReadingSources()
    {
        var found = Directory
            .GetFiles(Path.Combine(Root, ".github", "workflows"), "*.yml")
            .ToList();

        found.AddRange(Directory.GetFiles(Root, "*.cmd", SearchOption.TopDirectoryOnly));

        return found.OrderBy(path => path, StringComparer.Ordinal).ToList();
    }

    /// <summary>
    /// Every MSBuild file in the tree, build output excluded.
    ///
    /// <para>The exclusion changes no answer today — 32 generated props and targets sit under <c>obj</c>
    /// after a build and not one declares a version property — and that is measured rather than left
    /// looking like a measurement. It is here because the walk is a recursive glob: a generated copy
    /// declaring one would be reported as a second declaration, failing for whoever had built the tree and
    /// passing for whoever had not.</para>
    /// </summary>
    private static List<string> MsBuildFiles()
    {
        var found = new List<string>();

        foreach (var pattern in new[] { "*.csproj", "*.props", "*.targets" })
        {
            found.AddRange(Directory.GetFiles(Root, pattern, SearchOption.AllDirectories));
        }

        return found
            .Where(path => !IsBuildOutput(path))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(path => path, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>
    /// The values of one MSBuild PROPERTY in a file: elements of that name whose parent is a
    /// <c>PropertyGroup</c>.
    ///
    /// <para>Parsed, not matched. A comment quoting the property name is invisible to an element walk and
    /// indistinguishable from a declaration to a substring count, which is how the three shipped csprojs
    /// each looked like three declarations. Scoping to a <c>PropertyGroup</c> parent keeps
    /// <c>PackageReference</c>'s nested version spelling out of the answer, and keeps the count on the
    /// question actually being asked: which places declare a version PROPERTY.</para>
    /// </summary>
    private static List<string> PropertyValues(string file, string property) =>
        XDocument
            .Load(file)
            .Descendants()
            .Where(element => element.Name.LocalName.Equals(property, StringComparison.Ordinal)
                           && element.Parent is not null
                           && element.Parent.Name.LocalName.Equals("PropertyGroup", StringComparison.Ordinal))
            .Select(element => element.Value.Trim())
            .ToList();

    /// <summary>An absolute path as a repo-relative one with forward slashes, so failure messages spell a
    /// path the same way on every host.</summary>
    private static string RepoRelative(string absolute) =>
        Path.GetRelativePath(Root, absolute).Replace(Path.DirectorySeparatorChar, '/');

    /// <summary>Whether a path sits under a <c>bin</c> or <c>obj</c> directory below the repository
    /// root.</summary>
    private static bool IsBuildOutput(string absolute) =>
        Path.GetRelativePath(Root, absolute)
            .Split(Path.DirectorySeparatorChar, '/')
            .Any(segment => segment.Equals("bin", StringComparison.OrdinalIgnoreCase)
                         || segment.Equals("obj", StringComparison.OrdinalIgnoreCase));
}
