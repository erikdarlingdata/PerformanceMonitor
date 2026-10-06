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
/// <para><b>What counts as disposed.</b> The check is per scope, not per file. A construction on a <c>using</c>
/// line is disposed. A local must be disposed (<c>.Dispose()</c> or <c>?.Dispose()</c>) in its own method. A field
/// must be disposed in its own class. A list entry (<c>list.Add(x)</c>) needs a <c>foreach</c> over that same list,
/// in the same class, whose body disposes the loop variable (or the list's own <c>Dispose*</c> helper). A
/// construction handed to another constructor, awaited with no name, or assigned to something this check cannot
/// follow is an offender. Target-typed <c>new(p)</c> assigned to an initializer name or added to a list of them
/// counts as a construction, and a class that derives from the initializer is refused, because its constructions
/// cannot be followed.</para>
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
            + "test run (#5208). Put it on a 'using', call Dispose() on it in its own method (a local) or its own class's Dispose (a field), or loop over the list you add it to and dispose each entry:\n  "
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

        /* G1 (#5321): a name or list disposed somewhere ELSE in the file does not cover this construction. */
        Assert.Equal([4], Offenders("class C\n{\n    void A() { var x = new DuckDbInitializer(p); x.Dispose(); }\n    void B() { var x = new DuckDbInitializer(p); x.Run(); }\n}\n").Offenders);
        Assert.Equal([3], Offenders("class C\n{\n    void A() { var x = new DuckDbInitializer(p); x.Run(); }\n}\nclass D\n{\n    void B() { var x = 1; x.Dispose(); }\n}\n").Offenders);
        Assert.Equal([3], Offenders("class C\n{\n    void Open() { _db = new DuckDbInitializer(p); }\n}\nclass D\n{\n    void Dispose() { _db.Dispose(); }\n}\n").Offenders);
        Assert.Equal([3], Offenders("class C\n{\n    void Open() { var d = new DuckDbInitializer(p); _all.Add(d); }\n    void Dispose() { c.Dispose(); }\n}\n").Offenders);
        Assert.Equal([3], Offenders("class C\n{\n    void Open() { var d = new DuckDbInitializer(p); _all.Add(d); }\n    void Dispose() { foreach (var i in _other) i.Dispose(); }\n}\n").Offenders);
        Assert.Equal([3], Offenders("class C\n{\n    void Open() { var d = new DuckDbInitializer(p); _all.Add(d); }\n    void Dispose() { foreach (var i in _all) { i.Run(); } }\n}\n").Offenders);
        Assert.Empty(Offenders("class C\n{\n    void Open() { var d = new DuckDbInitializer(p); _all.Add(d); }\n    void Dispose()\n    {\n        foreach (var i in _all)\n        {\n            try { i.Dispose(); } catch { }\n        }\n    }\n}\n").Offenders);
        Assert.Empty(Offenders("class C\n{\n    void A() { var x = new DuckDbInitializer(p); x.Dispose(); }\n    void B() { var x = new DuckDbInitializer(p); try { } finally { x?.Dispose(); } }\n}\n").Offenders);

        /* G2: a construction handed to another constructor is not what the using disposes. */
        Assert.Equal([3], Offenders("class C\n{\n    void M() { using var svc = new Svc(new DuckDbInitializer(p)); }\n}\n").Offenders);
        Assert.Equal([3], Offenders("class C\n{\n    void M() { var svc = new Svc(new DuckDbInitializer(p)); svc.Dispose(); }\n}\n").Offenders);

        /* G3: the target-typed form is a construction, and a subclass is refused outright. */
        Assert.Equal([3], Offenders("class C\n{\n    void M() { DuckDbInitializer x = new(p); x.Run(); }\n}\n").Offenders);
        Assert.Equal([4], Offenders("class C\n{\n    DuckDbInitializer _db;\n    void Open() { _db = new(p); }\n}\n").Offenders);
        Assert.Equal([3], Offenders("class C\n{\n    void M() { var all = new List<DuckDbInitializer>(); all.Add(new(p)); }\n}\n").Offenders);
        Assert.Empty(Offenders("class C\n{\n    void M() { using DuckDbInitializer x = new(p); }\n}\n").Offenders);
        Assert.Empty(Offenders("class C\n{\n    DuckDbInitializer _db;\n    void Open() { _db = new(p); }\n    void Dispose() { _db.Dispose(); }\n}\n").Offenders);
        Assert.Equal([1], Offenders("class Fake : DuckDbInitializer\n{\n}\n").Offenders);
        Assert.Equal([3], Offenders("class C\n{\n    sealed class Fake(string p) : Base, PerformanceMonitorLite.Database.DuckDbInitializer\n    {\n    }\n}\n").Offenders);

        /* A mention in a comment or a string is not a construction. */
        Assert.Empty(Offenders("class C\n{\n    // var x = new DuckDbInitializer(p);\n    string s = \"new DuckDbInitializer(p)\";\n}\n").Offenders);
    }

    /// <summary>Target-typed <c>new(</c>: only a site when it is assigned to a name declared as an initializer or added to a list of them.</summary>
    private static readonly Regex TargetTyped = new(@"\bnew\s*\(", RegexOptions.Compiled);

    /// <summary>A class or record that derives from the initializer: its constructions are invisible to <see cref="Construction"/>.</summary>
    private static readonly Regex Subclass = new(
        @"\b(?:class|record)\s+\w+(?:<[^>]*>)?\s*(?:\([^)]*\))?\s*:\s*(?:[\w.<>]+\s*,\s*)*(?:[\w.]+\.)?DuckDbInitializer\b",
        RegexOptions.Compiled);

    private static readonly Regex UsingBefore = new(@"\busing\s*\(?\s*(?:var|[\w.]+\??)\s+\w+\s*=\s*$", RegexOptions.Compiled);
    private static readonly Regex DeclBefore = new(@"\b(?:var|(?:[\w.]+\.)?DuckDbInitializer\??)\s+(\w+)\s*=\s*$", RegexOptions.Compiled);
    private static readonly Regex NameBefore = new(@"(\w+)\s*=\s*$", RegexOptions.Compiled);
    private static readonly Regex AddBefore = new(@"(\w+)\s*\.\s*Add\s*\(\s*$", RegexOptions.Compiled);

    private static (int Total, List<int> Offenders) Offenders(string text)
    {
        var code = CSharpSourceWalker.CodeMask(text);
        var offenders = new List<int>();
        var total = 0;

        foreach (var at in Sites(text, code))
        {
            total++;
            var lineStart = text.LastIndexOf('\n', at) + 1;
            if (!IsDisposed(text, code, at, text[lineStart..at]))
            {
                offenders.Add(LineOf(text, at));
            }
        }

        /* A subclass is constructed under its own name, which no regex here can follow: no test has one, so none may start. */
        offenders.AddRange(Subclass.Matches(text).Cast<Match>().Where(m => code[m.Index]).Select(m => LineOf(text, m.Index)));
        offenders.Sort();
        return (total, offenders);
    }

    private static int LineOf(string text, int at) => text.Take(at).Count(c => c == '\n') + 1;

    /// <summary>Every construction: <c>new DuckDbInitializer(</c>, plus target-typed <c>new(</c> assigned to an initializer name or added to a list of them.</summary>
    private static List<int> Sites(string text, bool[] code)
    {
        var sites = Construction.Matches(text).Cast<Match>().Select(m => m.Index).Where(i => code[i]).ToList();
        var initNames = Regex.Matches(text, @"(?:[\w.]+\.)?DuckDbInitializer\??\s+(\w+)\b").Cast<Match>().Select(m => m.Groups[1].Value).ToHashSet();
        var listNames = Regex.Matches(text, @"<\s*(?:[\w.]+\.)?DuckDbInitializer\s*>\??\s+(\w+)\b|\b(\w+)\s*=\s*new\s+\w+\s*<\s*(?:[\w.]+\.)?DuckDbInitializer\s*>")
            .Cast<Match>().Select(m => m.Groups[1].Success ? m.Groups[1].Value : m.Groups[2].Value).ToHashSet();

        foreach (var match in TargetTyped.Matches(text).Cast<Match>().Where(m => code[m.Index]))
        {
            var before = text[(text.LastIndexOf('\n', match.Index) + 1)..match.Index];
            var declared = Regex.IsMatch(before, @"(?:[\w.]+\.)?DuckDbInitializer\??\s+\w+\s*=\s*$");
            var assigned = NameBefore.Match(before);
            var added = AddBefore.Match(before);

            if (declared
                || (assigned.Success && initNames.Contains(assigned.Groups[1].Value))
                || (added.Success && listNames.Contains(added.Groups[1].Value)))
            {
                sites.Add(match.Index);
            }
        }

        sites.Sort();
        return sites;
    }

    /// <summary>
    /// Disposed in the scope the name lives in: a local in its own method, a field in its own class, a list entry
    /// through a <c>foreach</c> over that list in the same class. A <c>using</c> counts only when it is on the
    /// construction itself, so <c>using var svc = new Svc(new DuckDbInitializer(p))</c> is not disposed.
    /// </summary>
    private static bool IsDisposed(string text, bool[] code, int at, string before)
    {
        if (UsingBefore.IsMatch(before))
        {
            return true;
        }

        var blocks = EnclosingBlocks(text, code, at);
        var classIndex = blocks.FindLastIndex(b => IsTypeBlock(text, code, b.Open));
        var cls = classIndex >= 0 ? blocks[classIndex] : (Open: 0, Close: text.Length);
        (int Open, int Close)? method = classIndex + 1 < blocks.Count ? blocks[classIndex + 1] : null;

        var list = AddBefore.Match(before);
        if (list.Success)
        {
            return ListDisposed(text, code, list.Groups[1].Value, cls);
        }

        var assigned = NameBefore.Match(before);
        if (!assigned.Success)
        {
            return false;
        }

        var name = assigned.Groups[1].Value;
        var declared = DeclBefore.IsMatch(before);
        var localInMethod = method is { } m
            && Regex.IsMatch(text[m.Open..at], $@"\b(?:var|DuckDbInitializer\??)\s+{Regex.Escape(name)}\s*[=;]");
        var scope = (declared || localInMethod) && method is { } own ? own : cls;

        if (DisposedIn(text, code, name, scope.Open, scope.Close))
        {
            return true;
        }

        return Regex.Matches(text, $@"\b(\w+)\s*\.\s*Add\s*\(\s*{Regex.Escape(name)}\s*\)").Cast<Match>()
            .Any(a => a.Index >= scope.Open && a.Index < scope.Close && code[a.Index]
                && ListDisposed(text, code, a.Groups[1].Value, cls));
    }

    private static bool DisposedIn(string text, bool[] code, string name, int start, int end) =>
        Regex.Matches(text, $@"\b{Regex.Escape(name)}\s*\??\.\s*Dispose\s*\(").Cast<Match>()
            .Any(m => m.Index >= start && m.Index < end && code[m.Index]);

    /// <summary>The class disposes every entry of the list: a <c>foreach</c> over it whose body disposes the loop variable, or the list's own dispose helper.</summary>
    private static bool ListDisposed(string text, bool[] code, string list, (int Open, int Close) cls)
    {
        var escaped = Regex.Escape(list);
        foreach (var loop in Regex.Matches(text, $@"\bforeach\s*\(\s*(?:var|[\w.<>?]+)\s+(\w+)\s+in\s+{escaped}\b").Cast<Match>())
        {
            if (loop.Index < cls.Open || loop.Index >= cls.Close || !code[loop.Index])
            {
                continue;
            }

            var bodyStart = MatchClose(text, code, text.IndexOf('(', loop.Index), '(', ')') + 1;
            var next = bodyStart;
            while (next < text.Length && char.IsWhiteSpace(text[next]))
            {
                next++;
            }

            var bodyEnd = next < text.Length && text[next] == '{' ? MatchClose(text, code, next, '{', '}') : text.IndexOf(';', next);
            if (bodyEnd > bodyStart && DisposedIn(text, code, loop.Groups[1].Value, bodyStart, bodyEnd))
            {
                return true;
            }
        }

        return Regex.Matches(text, $@"\b{escaped}\s*\??\.\s*(Dispose\w*|ForEach)\s*\(").Cast<Match>().Any(m =>
            m.Index >= cls.Open && m.Index < cls.Close && code[m.Index]
            && (m.Groups[1].Value != "ForEach" || text[m.Index..Math.Max(m.Index, text.IndexOf(';', m.Index))].Contains(".Dispose(", StringComparison.Ordinal)));
    }

    /// <summary>The brace blocks around <paramref name="at"/>, outermost first, found on the code mask so braces in strings and comments do not count.</summary>
    private static List<(int Open, int Close)> EnclosingBlocks(string text, bool[] code, int at)
    {
        var open = new List<int>();
        for (var i = 0; i < at; i++)
        {
            if (!code[i])
            {
                continue;
            }

            if (text[i] == '{')
            {
                open.Add(i);
            }
            else if (text[i] == '}' && open.Count > 0)
            {
                open.RemoveAt(open.Count - 1);
            }
        }

        return open.Select(o => (Open: o, Close: MatchClose(text, code, o, '{', '}'))).ToList();
    }

    /// <summary>Index of the bracket that closes the one at <paramref name="open"/>, or the end of the text.</summary>
    private static int MatchClose(string text, bool[] code, int open, char opener, char closer)
    {
        var depth = 0;
        for (var i = open; i < text.Length; i++)
        {
            if (!code[i])
            {
                continue;
            }

            if (text[i] == opener)
            {
                depth++;
            }
            else if (text[i] == closer && --depth == 0)
            {
                return i;
            }
        }

        return text.Length;
    }

    /// <summary>True when the block at <paramref name="open"/> is the body of a class, struct, record or interface.</summary>
    private static bool IsTypeBlock(string text, bool[] code, int open)
    {
        /* Comments and strings are blanked first: a doc comment that says "the record of" is not a record. */
        var headerStart = text.LastIndexOfAny([';', '{', '}'], Math.Max(open - 1, 0)) + 1;
        var header = new string(Enumerable.Range(headerStart, open - headerStart).Select(i => code[i] ? text[i] : ' ').ToArray());
        return Regex.IsMatch(header, @"\b(?:class|struct|record|interface)\b");
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
