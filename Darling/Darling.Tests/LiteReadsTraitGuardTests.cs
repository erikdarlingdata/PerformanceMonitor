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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every Darling.Tests class that reads a file under <c>Lite/</c> or <c>Lite.Tests/</c> carries
/// <c>[Trait("Reads", "Lite")]</c> (#5459 cut 3a), the mirror of Lite.Tests' <c>DarlingReadsTraitGuardTests</c>.
///
/// <para><b>Why the trait exists.</b> A pull request whose diff touches only Lite cannot change anything the Darling
/// service or viewer does, but it can change a Lite file that a Darling.Tests class reads as source (a cross-app
/// parity pin, a column-order comparison, a census over every XAML file). build.yml's <c>build</c> job therefore runs
/// ONLY the classes tagged <c>Reads=Lite</c> plus the Guard stage for such a diff on a pull request (and
/// <c>darling-tree-guards</c> does the same when the diff is Lite-only and left the build job's step unrun), and
/// the whole suite for any diff that touches Darling, a shared library, a root build file or a release. The
/// selection is the runner's own <c>-trait Reads=Lite</c> filter, so there is no roster to keep: a class gets into
/// the narrow run by being tagged.</para>
///
/// <para><b>What this guard is for.</b> A class that reads a Lite file and is NOT tagged would silently stop running
/// on Lite-only pull requests, and the nightly would be the first place its failure showed. So the requirement is
/// derived from the source: add a read of a Lite path to a test class and the class must carry the trait, or this
/// fails and names the class. <c>.github/scripts/tag-reads-lite.py</c> applies the tags this message lists.</para>
///
/// <para><b>What counts as a read.</b> See <see cref="GuardStageScanner.ReadsALitePath(string)"/>. The scan is per
/// FILE, so a helper class that holds the path puts the trait on every test class beside it.</para>
/// </summary>
[Trait("Stage", "Guard")]
[Trait("Reads", "Lite")]
public sealed class LiteReadsTraitGuardTests
{
    /* Anti-vacuity floors, far below today's counts (see the PR that added the trait): a scan that stopped seeing
       reads would otherwise pass for the wrong reason. */
    private const int ReaderFloor = 100;
    private const int TaggedFloor = 100;

    [Fact]
    public void TheDetector_SeesALiteRead_AndIgnoresProseAndOtherTrees()
    {
        Assert.True(GuardStageScanner.ReadsALitePath("var p = \"Lite/Services/X.cs\";"));
        Assert.True(GuardStageScanner.ReadsALitePath("var p = @\"..\\Lite\\Services\\X.cs\";"));
        Assert.True(GuardStageScanner.ReadsALitePath("var p = Path.Combine(root, \"Lite\", \"Mcp\", \"X.cs\");"));
        Assert.True(GuardStageScanner.ReadsALitePath("var s = RepoFile.ReadRepoFile(\"Lite\", \"Analysis\", \"X.cs\");"));
        Assert.True(GuardStageScanner.ReadsALitePath("var p = \"Lite.Tests/DataStartBannerQueriesTabTests.cs\";"));
        Assert.True(GuardStageScanner.ReadsALitePath("var p = \"Lite/PerformanceMonitorLite.csproj\";"));
        Assert.True(GuardStageScanner.ReadsALitePath("var p = \"PerformanceMonitorLite.Tests\";"));

        Assert.False(GuardStageScanner.ReadsALitePath("// \"Lite/Services/X.cs\" in prose\nvar a = 1;"));
        Assert.False(GuardStageScanner.ReadsALitePath("/* \"Lite/Services/X.cs\" */ var a = 1;"));
        Assert.False(GuardStageScanner.ReadsALitePath("Assert.Contains(\"Lite: an unseeded store\", head);"));
        Assert.False(GuardStageScanner.ReadsALitePath("var p = \"Darling/Darling.Tests/X.cs\";"));
        Assert.False(GuardStageScanner.ReadsALitePath("var p = \"Delite/X.cs\";"));
    }

    [Fact]
    public void TheDetector_SeesARecursiveEnumerationFromTheRepoRoot_AndNothingNarrower()
    {
        Assert.True(GuardStageScanner.ReadsALitePath(
            "var root = RepoRoot();\nvar f = Directory.EnumerateFiles(root, \"*.cs\", SearchOption.AllDirectories);"));
        Assert.True(GuardStageScanner.ReadsALitePath(
            "var f = Directory.GetFiles(RepoFile.Root, \"*.csproj\", SearchOption.AllDirectories);"));

        Assert.False(GuardStageScanner.ReadsALitePath(
            "var root = RepoRoot();\nvar f = Directory.EnumerateFiles(root, \"*.cs\", SearchOption.TopDirectoryOnly);"));
        Assert.False(GuardStageScanner.ReadsALitePath(
            "var dir = Path.Combine(RepoRoot(), \"PerformanceMonitor.Common\");\n"
          + "var f = Directory.GetFiles(dir, \"*.cs\", SearchOption.AllDirectories);"));
    }

    [Fact]
    public void TheScan_FlagsATestClassThatReadsLiteAndCarriesNoTrait()
    {
        const string untagged = "namespace Darling.Tests;\n\n"
          + "public sealed class SyntheticLiteReaderTests\n{\n"
          + "    [Fact]\n"
          + "    public void Read()\n"
          + "    {\n"
          + "        var text = RepoFile.ReadRepoFile(\"Lite\", \"Services\", \"X.cs\");\n"
          + "    }\n"
          + "}\n";
        var tagged = untagged.Replace(
            "public sealed class", GuardStageScanner.LiteTraitText + "\npublic sealed class", StringComparison.Ordinal);

        var before = Assert.Single(GuardStageScanner.ScanText(untagged));
        Assert.True(before.ReadsLite);
        Assert.False(before.TaggedLite);

        var after = Assert.Single(GuardStageScanner.ScanText(tagged));
        Assert.True(after.ReadsLite);
        Assert.True(after.TaggedLite);
    }

    [Fact]
    public void EveryTestClassThatReadsALitePath_CarriesTheReadsLiteTrait()
    {
        var rows = GuardStageScanner.Scan(TestsRoot(), ProjectFile());

        var readers = rows.Where(r => r.ReadsLite).ToList();
        Assert.True(
            readers.Count >= ReaderFloor,
            $"only {readers.Count} Darling.Tests classes were found reading a Lite path; the scan has stopped seeing them");
        var missing = readers.Where(r => !r.TaggedLite).Select(r => $"{r.File}: {r.Name}").OrderBy(m => m, StringComparer.Ordinal).ToList();

        Assert.True(
            missing.Count == 0,
            $"These Darling.Tests classes read a Lite path but do not carry {GuardStageScanner.LiteTraitText}, so a "
          + "pull request that changes only Lite files would not run them. Add the attribute above each class, or run "
          + "python .github/scripts/tag-reads-lite.py with this list saved to a file:\n  " + string.Join("\n  ", missing));

        Assert.True(
            rows.Count(r => r.TaggedLite) >= TaggedFloor,
            "the Reads=Lite trait scan has stopped seeing the tagged classes");
    }

    [Fact]
    public void TheTraitXunitSelects_IsTheTraitTheSourceScanSees()
    {
        /* The runner's -trait Reads=Lite reads the loaded assembly, not the source. A trait written in a shape the scan
           reads but the runtime does not (or the reverse) would run fewer classes than the census believes. */
        var selected = typeof(LiteReadsTraitGuardTests).Assembly.GetTypes()
            .Where(t => t.IsClass && HasLiteTrait(t))
            .Select(t => t.Name)
            .ToHashSet(StringComparer.Ordinal);
        var tagged = GuardStageScanner.Scan(TestsRoot(), ProjectFile())
            .Where(r => r.TaggedLite)
            .Select(r => r.Name)
            .ToList();

        Assert.True(selected.Count >= TaggedFloor, $"xunit selects only {selected.Count} classes for Reads=Lite");
        var notSelected = tagged.Where(n => !selected.Contains(n)).OrderBy(n => n, StringComparer.Ordinal).ToList();
        Assert.True(
            notSelected.Count == 0,
            "The source says these classes carry Reads=Lite but the built assembly does not:\n  " + string.Join("\n  ", notSelected));
    }

    /// <summary>
    /// The build job's <c>darling</c> filter is the union of two things: the paths that run the WHOLE suite
    /// (<c>darling_full</c>) and the Lite paths that run only the Guard and Reads=Lite classes. A path added to
    /// <c>darling</c> and not to <c>darling_full</c> would silently narrow the run for an edit that is not a Lite file.
    /// </summary>
    [Fact]
    public void TheDarlingFilter_IsTheFullSuiteFilter_PlusLitePathsOnly()
    {
        var yaml = File.ReadAllText(Path.Combine(RepoFile.Root, ".github", "workflows", "build.yml"))
            .Replace("\r\n", "\n", StringComparison.Ordinal);

        var all = FilterEntries(yaml, "darling");
        var full = FilterEntries(yaml, "darling_full");

        Assert.NotEmpty(full);
        Assert.Contains("Darling/**/!(*.md)", full);
        Assert.Empty(full.Except(all));

        var narrowed = all.Except(full).ToList();
        Assert.NotEmpty(narrowed);
        var notLite = narrowed.Where(e => !e.StartsWith("Lite/", StringComparison.Ordinal) && !e.StartsWith("Lite.Tests/", StringComparison.Ordinal)).ToList();
        Assert.True(
            notLite.Count == 0,
            "These entries are in build.yml's 'darling' filter but not in 'darling_full', so a diff that reaches only "
          + "them would run just the Guard and Reads=Lite classes. Only Lite paths may be narrowed; add the others to "
          + "'darling_full':\n  " + string.Join("\n  ", notLite));
    }

    private static bool HasLiteTrait(Type type)
    {
        foreach (var attribute in type.GetCustomAttributesData())
        {
            if (attribute.AttributeType.Name != "TraitAttribute" || attribute.ConstructorArguments.Count != 2)
            {
                continue;
            }

            if (Equals(attribute.ConstructorArguments[0].Value, "Reads") && Equals(attribute.ConstructorArguments[1].Value, "Lite"))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>The quoted patterns of one filter in the build job's <c>filters: |</c> block (12-space keys).</summary>
    private static List<string> FilterEntries(string yaml, string name)
    {
        var at = yaml.IndexOf("\n            " + name + ":\n", StringComparison.Ordinal);
        Assert.True(at > 0, $"build.yml's '{name}' path filter is gone");
        var rest = yaml[(at + 1)..];
        var next = Regex.Match(rest[1..], "\n            [a-z_]+:\n|\n      - name: ");
        var block = next.Success ? rest[..(next.Index + 1)] : rest;

        return Regex.Matches(block, @"^\s*-\s*'(?<p>[^']+)'\s*$", RegexOptions.Multiline)
            .Select(m => m.Groups["p"].Value)
            .ToList();
    }

    private static string TestsRoot([CallerFilePath] string thisFile = "") =>
        Path.GetDirectoryName(thisFile)
        ?? throw new InvalidOperationException("The caller file path has no directory.");

    private static string ProjectFile() => Path.Combine(TestsRoot(), "Darling.Tests.csproj");
}
