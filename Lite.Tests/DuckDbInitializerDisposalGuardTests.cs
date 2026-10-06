using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Darling.Tests;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5208: every <c>DuckDbInitializer</c> a Lite test constructs is disposed, or is on a <c>using</c>.
///
/// <para><b>Why a source guard.</b> The initializer holds a sentinel connection that keeps the database open
/// until <c>Dispose()</c>. A test that deletes its temp folder without disposing it still passes, because
/// Windows lets the delete succeed while the handles stay open, so nothing fails: the test process just ends
/// with about a thousand open DuckDB databases, each holding three threads, seven handles and ~15 MB of native
/// memory. Only the source shows the missing call.</para>
///
/// <para><b>What counts as disposed.</b> A construction on a <c>using</c> line, or one assigned to a name the
/// same file calls <c>.Dispose()</c> on, or adds to a list the file disposes. A construction handed straight to
/// another constructor or awaited with no name has nothing to dispose, so it is an offender.</para>
///
/// <para>Literal- and comment-aware through <see cref="CSharpSourceWalker"/>, so this text naming the type is not
/// a construction.</para>
/// </summary>
public sealed class DuckDbInitializerDisposalGuardTests
{
    /// <summary>A construction, plain or namespace-qualified.</summary>
    private static readonly Regex Construction = new(@"\bnew\s+(?:[A-Za-z_][\w]*\s*\.\s*)*DuckDbInitializer\s*\(", RegexOptions.Compiled);

    [Fact]
    public void EveryDuckDbInitializerALiteTestOpensIsDisposed()
    {
        var root = RepoRoot();
        var testsDir = Path.Combine(root, "Lite.Tests");
        var offenders = new List<string>();
        var constructions = 0;

        foreach (var file in TestSourceFiles(testsDir))
        {
            var text = File.ReadAllText(file);
            var relative = Path.GetRelativePath(root, file);
            var found = Offenders(text);
            constructions += found.Total;
            offenders.AddRange(found.Offenders.Select(line => $"{relative}:{line}"));
        }

        Assert.True(constructions >= 100, $"the guard saw {constructions} constructions; the walk is not reading the tests");
        Assert.True(
            offenders.Count == 0,
            "These Lite tests construct a DuckDbInitializer and never dispose it, so its database stays open for the whole "
            + "test run (#5208). Put it on a 'using', or call Dispose() on it in the class's Dispose/DisposeAsync:\n  "
            + string.Join("\n  ", offenders));
    }

    [Fact]
    public void TheGuardFlagsAnUndisposedInitializer_AndAcceptsTheDisposedShapes()
    {
        Assert.Equal([3], Offenders("class C\n{\n    void M() { var x = new DuckDbInitializer(p); x.Run(); }\n}\n").Offenders);
        Assert.Equal([3], Offenders("class C\n{\n    void M() => new Svc(new DuckDbInitializer(p));\n}\n").Offenders);
        Assert.Equal([3], Offenders("class C\n{\n    async Task M() => await new DuckDbInitializer(p).InitializeAsync();\n}\n").Offenders);
        Assert.Equal([3], Offenders("class C\n{\n    void M() { var x = new PerformanceMonitorLite.Database.DuckDbInitializer(p); x.Run(); }\n}\n").Offenders);

        Assert.Empty(Offenders("class C\n{\n    void M() { using var x = new DuckDbInitializer(p); }\n}\n").Offenders);
        Assert.Empty(Offenders("class C\n{\n    void M() { using (var x = new DuckDbInitializer(p)) { } }\n}\n").Offenders);
        Assert.Empty(Offenders("class C\n{\n    void Open() { _db = new DuckDbInitializer(p); }\n    void Dispose() { _db.Dispose(); }\n}\n").Offenders);
        Assert.Empty(Offenders("class C\n{\n    void Open() { _db = new DuckDbInitializer(p); }\n    void Dispose() { _db?.Dispose(); }\n}\n").Offenders);
        Assert.Empty(Offenders("class C\n{\n    void Open() { var d = new DuckDbInitializer(p); _all.Add(d); }\n    void Dispose() { foreach (var i in _all) i.Dispose(); }\n}\n").Offenders);

        /* A mention in a comment or a string is not a construction. */
        Assert.Empty(Offenders("class C\n{\n    // var x = new DuckDbInitializer(p);\n    string s = \"new DuckDbInitializer(p)\";\n}\n").Offenders);
    }

    private static (int Total, List<int> Offenders) Offenders(string text)
    {
        var code = CSharpSourceWalker.CodeMask(text);
        var offenders = new List<int>();
        var total = 0;

        foreach (var match in Construction.Matches(text).Cast<Match>())
        {
            var at = match.Index;
            if (!code[at])
            {
                continue;
            }

            total++;
            var lineStart = text.LastIndexOf('\n', at) + 1;
            var before = text[lineStart..at];

            if (Regex.IsMatch(before, @"^\s*(?:\{\s*)?using\s*(?:var\b|\()") || Regex.IsMatch(before, @"\busing\s+var\b|\busing\s*\(\s*var\b"))
            {
                continue;
            }

            var name = Regex.Match(before, @"(\w+)\s*=\s*$");
            if (name.Success && IsDisposed(text, code, name.Groups[1].Value))
            {
                continue;
            }

            offenders.Add(text.Take(at).Count(c => c == '\n') + 1);
        }

        return (total, offenders);
    }

    /// <summary>The file calls <c>.Dispose()</c> on the name, or hands the name to a list and disposes list entries.</summary>
    private static bool IsDisposed(string text, bool[] code, string name)
    {
        if (Regex.Matches(text, $@"\b{Regex.Escape(name)}\s*\??\.\s*Dispose\s*\(").Any(m => code[m.Index]))
        {
            return true;
        }

        var added = Regex.Matches(text, $@"\.\s*Add\s*\(\s*{Regex.Escape(name)}\s*\)").Any(m => code[m.Index]);
        return added && Regex.Matches(text, @"\.\s*Dispose\s*\(\s*\)").Any(m => code[m.Index]);
    }

    private static IEnumerable<string> TestSourceFiles(string testsDir) =>
        Directory.EnumerateFiles(testsDir, "*.cs", SearchOption.AllDirectories)
            .Where(path => !Path.GetRelativePath(testsDir, path)
                .Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                .Any(segment => segment is "bin" or "obj"))
            .Where(path => !string.Equals(Path.GetFileName(path), nameof(DuckDbInitializerDisposalGuardTests) + ".cs", StringComparison.Ordinal))
            .OrderBy(path => path, StringComparer.Ordinal);

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
