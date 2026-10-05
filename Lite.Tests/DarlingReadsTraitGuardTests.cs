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
using System.Text;
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
/// text sits only if that file is itself a test class, which is the limit of a source scan. A class that
/// enumerates files RECURSIVELY from the repo root (<c>RepoRoot()</c>, or a variable set from it, as the
/// first argument of <c>EnumerateFiles</c>/<c>GetFiles</c> with <c>AllDirectories</c> or a recursive
/// <c>EnumerationOptions</c>) reads every Darling file without naming one, so it counts as a Darling read
/// too.</para>
/// </summary>
[Trait("Reads", "Darling")]
public sealed class DarlingReadsTraitGuardTests
{
    private const string TraitText = "[Trait(\"Reads\", \"Darling\")]";

    /* Indented as well as column 0, so a class inside a block namespace or nested in another class is seen. A
       nested class's methods belong to the nested class here, and the trait attribute sits directly above
       that declaration, which is where HasTraitAbove looks. */
    private static readonly Regex s_classDeclaration = new(
        @"^[ \t]*(?:(?:public|internal|private|protected|sealed|static|abstract|partial)\s+)*class\s+(?<name>\w+)",
        RegexOptions.Multiline | RegexOptions.CultureInvariant);

    private static readonly Regex s_enumerationCall = new(
        @"\b(?:EnumerateFiles|EnumerateFileSystemEntries|EnumerateDirectories|GetFiles|GetFileSystemEntries|GetDirectories)\s*\(",
        RegexOptions.CultureInvariant);

    private static readonly Regex s_repoRootExpression = new(
        @"^(?:\w+\.)*(?:RepoRoot|FindRepoRoot)\(\s*\)$", RegexOptions.CultureInvariant);

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
    public void TheDetector_SeesARecursiveEnumerationFromTheRepoRoot_AndNothingNarrower()
    {
        Assert.True(ReadsDarling(
            "var root = RepoRoot();\nvar f = Directory.EnumerateFiles(root, \"*.cs\", SearchOption.AllDirectories);"));
        Assert.True(ReadsDarling(
            "var f = Directory.GetFiles(ParitySource.RepoRoot(), \"*.csproj\", SearchOption.AllDirectories);"));
        Assert.True(ReadsDarling(
            "string root = ParitySource.RepoRoot();\nvar o = new EnumerationOptions { RecurseSubdirectories = true };\n"
          + "var f = Directory.EnumerateFiles(root, \"*.props\", o);"));

        /* Not the root, not recursive, or no enumeration at all. */
        Assert.False(ReadsDarling(
            "var root = RepoRoot();\nvar f = Directory.EnumerateFiles(Path.Combine(root, \"Lite\"), \"*.cs\", SearchOption.AllDirectories);"));
        Assert.False(ReadsDarling(
            "var root = RepoRoot();\nvar f = Directory.EnumerateFiles(root, \"*.cs\", SearchOption.TopDirectoryOnly);"));
        Assert.False(ReadsDarling(
            "var root = RepoRoot();\nvar f = Directory.EnumerateFiles(root, \"*.cs\");"));
        Assert.False(ReadsDarling(
            "var dir = Path.Combine(RepoRoot(), \"Lite.Tests\");\nvar f = Directory.GetFiles(dir, \"*.cs\", SearchOption.AllDirectories);"));
        Assert.False(ReadsDarling(
            "var root = RepoRoot();\n// Directory.EnumerateFiles(root, \"*.cs\", SearchOption.AllDirectories)\nvar a = 1;"));
    }

    [Fact]
    public void TheGuard_FlagsAClassThatEnumeratesTheRepoRootAndCarriesNoTrait()
    {
        const string untagged = "namespace Lite.Tests;\n\n"
          + "public sealed class SyntheticWholeRepoScanTests\n{\n"
          + "    [Fact]\n"
          + "    public void Scan()\n"
          + "    {\n"
          + "        var root = ParitySource.RepoRoot();\n"
          + "        var all = Directory.EnumerateFiles(root, \"*.cs\", SearchOption.AllDirectories);\n"
          + "    }\n"
          + "}\n";
        var tagged = untagged.Replace(
            "public sealed class", TraitText + "\npublic sealed class", StringComparison.Ordinal);

        Assert.Equal(new[] { "SyntheticWholeRepoScanTests" }, UntaggedReaders(untagged));
        Assert.Empty(UntaggedReaders(tagged));
    }

    [Fact]
    public void TheClassScan_SeesIndentedAndNestedClasses()
    {
        const string text = "namespace Lite.Tests\n{\n"
          + "    [Trait(\"Reads\", \"Darling\")]\n"
          + "    public sealed class InBlockNamespaceTests\n    {\n        [Fact]\n        public void A() { }\n    }\n\n"
          + "    public sealed class Outer\n    {\n"
          + "        public sealed class NestedTests\n        {\n            [Fact]\n            public void B() { }\n        }\n"
          + "    }\n}\n";

        var seen = TestClasses(text).ToDictionary(c => c.Name, c => c.HasTrait);

        Assert.Equal(2, seen.Count);
        Assert.True(seen["InBlockNamespaceTests"]);
        Assert.False(seen["NestedTests"]); // a nested class is flagged even though no enclosing class carries the trait
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

    /// <summary>The test classes in <paramref name="text"/> that read Darling and lack the trait.</summary>
    private static List<string> UntaggedReaders(string text) =>
        ReadsDarling(text)
            ? TestClasses(text).Where(c => !c.HasTrait).Select(c => c.Name).ToList()
            : new List<string>();

    /// <summary>Whether the text reads a Darling path: a string literal that names one, or a recursive
    /// enumeration that starts at the repo root.</summary>
    private static bool ReadsDarling(string text) =>
        NamesADarlingPath(text) || EnumeratesTheRepoRootRecursively(text);

    /// <summary>Whether a call such as <c>Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories)</c>
    /// starts at the repo root. The first argument counts when it is a repo-root call (<c>RepoRoot()</c>) or a
    /// variable the file sets from one; the call is recursive when its arguments say
    /// <c>AllDirectories</c> or pass an options value and the file sets <c>RecurseSubdirectories = true</c>.</summary>
    private static bool EnumeratesTheRepoRootRecursively(string text)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(text);
        var rootVariables = new HashSet<string>(StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(
                     code, @"\b(?<name>\w+)\s*=\s*(?<rhs>[^;=]+);", RegexOptions.CultureInvariant))
        {
            if (s_repoRootExpression.IsMatch(m.Groups["rhs"].Value.Trim()))
            {
                rootVariables.Add(m.Groups["name"].Value);
            }
        }

        var recursiveOptions = Regex.IsMatch(code, @"RecurseSubdirectories\s*=\s*true", RegexOptions.CultureInvariant);

        foreach (Match call in s_enumerationCall.Matches(code))
        {
            var args = CallArguments(code, call.Index + call.Length);
            if (args.Count == 0)
            {
                continue;
            }

            var first = args[0].Trim();
            var fromRoot = s_repoRootExpression.IsMatch(first) || rootVariables.Contains(first);
            var recursive = args.Any(a => a.Contains("AllDirectories", StringComparison.Ordinal))
                || (recursiveOptions && args.Count >= 3 && !args[^1].Contains("TopDirectoryOnly", StringComparison.Ordinal));

            if (fromRoot && recursive)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>The top-level, comma-separated arguments of the call whose opening parenthesis sits just
    /// before <paramref name="start"/>; empty when the parentheses do not balance.</summary>
    private static List<string> CallArguments(string code, int start)
    {
        var args = new List<string>();
        var depth = 0;
        var from = start;

        for (var i = start; i < code.Length; i++)
        {
            var c = code[i];
            if (c is '(' or '[' or '{')
            {
                depth++;
            }
            else if (c is ')' or ']' or '}')
            {
                if (depth == 0)
                {
                    args.Add(code[from..i]);
                    return args;
                }

                depth--;
            }
            else if (c == ',' && depth == 0)
            {
                args.Add(code[from..i]);
                from = i + 1;
            }
        }

        return new List<string>();
    }

    /// <summary>Whether the text holds a string literal that reads a Darling path.</summary>
    private static bool NamesADarlingPath(string text)
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

    /// <summary>Every class in the file (top-level, in a block namespace, or nested) that declares a test of
    /// its own, with whether the trait is on that class itself. xunit's trait selection reads a class's own attributes,
    /// so a nested class does not take the trait of the class around it. A nested helper class that
    /// declares no test is not a test class, and the tests of an enclosing class are not the nested class's.</summary>
    private static IEnumerable<(string Name, bool HasTrait)> TestClasses(string text)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(text);
        var declarations = new List<(string Name, int Index, int Open, int End)>();

        foreach (Match m in s_classDeclaration.Matches(code))
        {
            var open = code.IndexOf('{', m.Index);
            if (open < 0)
            {
                continue;
            }

            declarations.Add((m.Groups["name"].Value, m.Index, open, open + CSharpSourceWalker.BraceBalanced(code, open).Length));
        }

        foreach (var d in declarations)
        {
            var own = new StringBuilder(code[d.Open..d.End]);
            foreach (var nested in declarations.Where(n => n.Index > d.Open && n.End <= d.End))
            {
                for (var i = nested.Index - d.Open; i < nested.End - d.Open; i++)
                {
                    own[i] = ' ';
                }
            }

            if (!s_testAttribute.IsMatch(own.ToString()))
            {
                continue;
            }

            yield return (d.Name, HasTraitAbove(text, d.Index));
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
