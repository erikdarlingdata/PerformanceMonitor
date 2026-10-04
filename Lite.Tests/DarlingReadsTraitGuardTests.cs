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
using System.Text.RegularExpressions;
using Darling.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Every Lite.Tests class that reads a file under <c>Darling/</c> carries <c>[Trait("Reads", "Darling")]</c>.
///
/// <para><b>Why the trait exists.</b> A pull request whose diff touches only the Darling viewer, service or
/// test trees cannot change anything Lite's own code does, but it can change a Darling file that a Lite.Tests
/// class reads as source (a cross-app parity pin, a theme comparison, a docs guard). build.yml's Lite shard
/// job therefore runs ONLY the classes carrying this trait, in one shard, for such a diff, and the whole
/// suite for any diff that touches Lite, Lite.Tests, a shared library or a root build file. The selection is
/// the runner's own <c>-trait Reads=Darling</c> filter over the built assembly, so there is no roster to
/// keep: a class gets into the narrow run by being tagged.</para>
///
/// <para><b>What this guard is for.</b> A class that reads a Darling file and is NOT tagged would silently
/// stop running on Darling-only pull requests, and the nightly would then be the first place its failure
/// showed. So the requirement is derived from the source, the way
/// <see cref="CrossAppGuardCiGateTests"/> derives the filter requirement: add a read of a <c>Darling/</c>
/// path to a test class and the class must carry the trait, or this fails and names the class.</para>
///
/// <para><b>What counts as a read.</b> A string literal (found by <see cref="CSharpSourceWalker"/>, so
/// comments and prose do not count) that names a path under <c>Darling/</c> or one of its projects, or the
/// bare segment <c>"Darling"</c> handed to <c>Path.Combine</c> or a segment array. A path assembled from a
/// constant declared in ANOTHER file is not visible to this scan; the read is then found where the constant's
/// text sits only if that file is itself a test class, which is the limit of a source scan.</para>
/// </summary>
[Trait("Reads", "Darling")]
public sealed class DarlingReadsTraitGuardTests
{
    private const string TraitText = "[Trait(\"Reads\", \"Darling\")]";

    private static readonly Regex s_classDeclaration = new(
        @"^(?:public|internal)\s+(?:(?:sealed|static|abstract|partial)\s+)*class\s+(?<name>\w+)",
        RegexOptions.Multiline | RegexOptions.CultureInvariant);

    private static readonly Regex s_testAttribute = new(
        @"\[(?:Fact|Theory)\b", RegexOptions.CultureInvariant);

    [Fact]
    public void TheDetector_SeesADarlingRead_AndIgnoresProseAndOtherTrees()
    {
        Assert.True(ReadsDarling("var p = \"Darling/PerformanceMonitor.Darling.Service/X.cs\";"));
        Assert.True(ReadsDarling("var p = @\"..\\Darling\\Darling.Tests\\X.cs\";"));
        Assert.True(ReadsDarling("var p = \"PerformanceMonitor.Darling.Viewer\";"));
        Assert.True(ReadsDarling("var p = Path.Combine(root, \"Darling\", \"Darling.Tests\", \"X.cs\");"));
        Assert.True(ReadsDarling("var s = new[] { \"Darling\", \"PerformanceMonitor.Darling.Viewer\" };"));

        Assert.False(ReadsDarling("// \"Darling/PerformanceMonitor.Darling.Service/X.cs\" in prose\nvar a = 1;"));
        Assert.False(ReadsDarling("/* \"Darling/Darling.Tests/X.cs\" */ var a = 1;"));
        Assert.False(ReadsDarling("Assert.Contains(\"Darling: an unseeded store\", head);"));
        Assert.False(ReadsDarling("var p = \"Lite/Services/X.cs\";"));
        Assert.False(ReadsDarling("var p = \"Lite.Tests/Darling.txt\";"));
    }

    [Fact]
    public void EveryTestClassThatReadsADarlingPath_CarriesTheReadsDarlingTrait()
    {
        var root = ParitySource.RepoRoot();
        var testsRoot = Path.Combine(root, "Lite.Tests");

        var files = Directory.EnumerateFiles(testsRoot, "*.cs", SearchOption.AllDirectories)
            .Where(f => !IsBuildOutput(testsRoot, f))
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToList();
        Assert.NotEmpty(files);

        var traited = new HashSet<string>(StringComparer.Ordinal);
        var readers = new List<(string File, string Class)>();
        var readingFiles = 0;

        foreach (var file in files)
        {
            var text = File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal);
            var reads = ReadsDarling(text);
            if (reads)
            {
                readingFiles++;
            }

            foreach (var declared in TestClasses(text))
            {
                if (declared.HasTrait)
                {
                    traited.Add(declared.Name);
                }

                if (reads)
                {
                    readers.Add((Path.GetRelativePath(testsRoot, file), declared.Name));
                }
            }
        }

        /* Anti-vacuity: a detector that found nothing would pass for the wrong reason. The suite reads
           Darling in dozens of files, so a floor far below that still catches a scan that stopped seeing. */
        Assert.True(
            readingFiles >= 20,
            $"only {readingFiles} Lite.Tests files were found reading a Darling path; the scan has stopped seeing them");
        Assert.True(
            traited.Count >= 20,
            $"only {traited.Count} Lite.Tests classes carry {TraitText}; the trait scan has stopped seeing them");

        var missing = readers
            .Where(r => !traited.Contains(r.Class))
            .Select(r => $"{r.File}: {r.Class}")
            .ToList();

        Assert.True(
            missing.Count == 0,
            $"These Lite.Tests classes read a Darling/ path but do not carry {TraitText}, so a pull request that "
          + "changes only Darling files would not run them. Add the attribute above each class:\n  "
          + string.Join("\n  ", missing));
    }

    /// <summary>
    /// Every Darling.Tests file that Lite.Tests.csproj compiles into itself is named in build.yml's
    /// <c>lite_linked_shard</c> filter, which sends a diff that edits one to the WHOLE Lite suite. Such a file
    /// is code the Lite classes run, so the narrow Reads=Darling selection would miss what an edit breaks.
    /// </summary>
    [Fact]
    public void EveryDarlingFileLiteTestsCompiles_IsInTheFullSuiteFilter()
    {
        var root = ParitySource.RepoRoot();
        var project = File.ReadAllText(Path.Combine(root, "Lite.Tests", "Lite.Tests.csproj"));
        var yaml = File.ReadAllText(Path.Combine(root, ".github", "workflows", "build.yml"))
            .Replace("\r\n", "\n", StringComparison.Ordinal);

        var linked = Regex.Matches(project, @"<Compile\s+Include=""\.\.\\(?<path>Darling\\[^""]+)""")
            .Select(m => m.Groups["path"].Value.Replace('\\', '/'))
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToList();
        Assert.NotEmpty(linked);

        var at = yaml.IndexOf("\n            lite_linked_shard:\n", StringComparison.Ordinal);
        Assert.True(at > 0, "build.yml's 'lite_linked_shard' path filter is gone");
        var rest = yaml[(at + 1)..];
        var next = Regex.Match(rest, "\n            [a-z_]+:\n|\n      - name: ");
        var block = next.Success ? rest[..next.Index] : rest;

        var named = Regex.Matches(block, @"^\s*-\s*'(?<p>[^']+)'\s*$", RegexOptions.Multiline)
            .Select(m => m.Groups["p"].Value)
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(linked, named);
    }

    /// <summary>Whether the text holds a string literal that reads a Darling path.</summary>
    private static bool ReadsDarling(string text)
    {
        foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(text))
        {
            var normalized = body.Replace('\\', '/');
            var segments = normalized.Split('/', StringSplitOptions.RemoveEmptyEntries);

            if (segments.Any(s => s.StartsWith("PerformanceMonitor.Darling.", StringComparison.Ordinal)
                || s.Equals("Darling.Tests", StringComparison.Ordinal)))
            {
                return true;
            }

            if (normalized.Contains('/', StringComparison.Ordinal)
                && segments.Any(s => s.Equals("Darling", StringComparison.Ordinal)))
            {
                return true;
            }

            if (body.Equals("Darling", StringComparison.Ordinal) && IsPathSegmentContext(text, start))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Whether the bare literal at <paramref name="start"/> is an argument to a path join or an
    /// element of a segment array, judged from the statement text before it.</summary>
    private static bool IsPathSegmentContext(string text, int start)
    {
        var from = Math.Max(0, start - 150);
        var before = text[from..start];
        var statementStart = before.LastIndexOf(';');
        if (statementStart >= 0)
        {
            before = before[(statementStart + 1)..];
        }

        return before.Contains("Combine(", StringComparison.Ordinal)
            || before.Contains("new[]", StringComparison.Ordinal)
            || before.Contains("new string[]", StringComparison.Ordinal);
    }

    /// <summary>Every top-level class in the file that declares a test, with whether the attribute sits
    /// directly above it.</summary>
    private static IEnumerable<(string Name, bool HasTrait)> TestClasses(string text)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(text);
        var matches = s_classDeclaration.Matches(code);

        for (var i = 0; i < matches.Count; i++)
        {
            var end = i + 1 < matches.Count ? matches[i + 1].Index : code.Length;
            if (!s_testAttribute.IsMatch(code[matches[i].Index..end]))
            {
                continue;
            }

            yield return (matches[i].Groups["name"].Value, HasTraitAbove(text, matches[i].Index));
        }
    }

    /// <summary>The attribute lines directly above a declaration, up to the first line that is not one.</summary>
    private static bool HasTraitAbove(string text, int declarationIndex)
    {
        var lines = text[..declarationIndex].Split('\n');

        for (var i = lines.Length - 2; i >= 0; i--)
        {
            var line = lines[i].Trim();
            if (!line.StartsWith('['))
            {
                return false;
            }

            if (line.Replace(" ", string.Empty, StringComparison.Ordinal)
                .Contains(TraitText.Replace(" ", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsBuildOutput(string testsRoot, string file)
    {
        var relative = Path.GetRelativePath(testsRoot, file);
        return relative.StartsWith("bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
            || relative.StartsWith("obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal);
    }
}
