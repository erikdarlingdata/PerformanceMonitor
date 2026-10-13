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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every place a Darling test mints a scratch database reaches it only after a skip on an unset <c>DARLING_TEST_PG</c> (#5759).
///
/// <para><b>The defect this closes.</b> #5726 added <c>ExternalAlertDrainLiveTests</c> and
/// <c>ExternalAlertDrainLifecycleLiveTests</c> without the skip. Their PR passed, because a pull request's shards have a store.
/// Dev's whole-tree guards job runs Darling.Tests with no store by design, and 34 tests failed there with
/// <c>ArgumentNullException (Parameter 'Host')</c> (#5755, fixed in #5756 by putting the skip at the top of each class's
/// scratch-database helper). <see cref="LivePostgresCollectionHygieneTests"/> covers collection membership, the other censuses
/// cover cluster objects, clocks, plan isolation, unpooled drops and disposal, and none of them covered the skip.</para>
///
/// <para><b>What this pins.</b> For every site that calls the scratch-database factory in a Darling.Tests source file, the code
/// path that reaches it skips first on a null or empty connection string. That holds when the enclosing member has the skip
/// textually before the site, when it calls a member (a property or a method, in its own class, a <c>using static</c> class or
/// a named class) that itself skips on a null or empty string (<c>RequireLivePostgres()</c>, a <c>BaseConnectionString</c>
/// getter), or, for a helper such as <c>WithDrainAsync</c>, when every caller of the helper skips before the call. A fixture's
/// <c>InitializeAsync</c> has no caller in the source, so it needs its own guard before the site: a skip, or an early
/// <c>return</c> on a null or empty string (the shape every <c>IClassFixture</c> store here uses, which is why such a class's
/// tests then skip themselves).</para>
///
/// <para><b>How the shared fixtures are covered.</b> <c>LivePostgresStoreFixture</c> mints nothing: it migrates the
/// <c>DARLING_TEST_PG</c> store itself, and its <c>InitializeAsync</c> returns before it opens a connection when the variable is
/// unset, which <see cref="TheSharedStoreFixture_ReturnsBeforeConnectingWhenNoStoreIsSet"/> pins with the same guard rule. The
/// per-class store fixtures (<c>StoreFixture</c>, <c>InstallIdScratchStore</c> and the rest) do mint a scratch database, so they
/// are sites like any other and need the guard. <c>WebFixture</c> in <c>ConnectionTestReplyTests</c> opens no store.</para>
///
/// <para><b>Limits, on purpose.</b> Source text, as the other censuses in this folder are: there is no Roslyn here. A guard is
/// any skip call or early return whose condition is a null or empty test, textually before the site in the same member; it is
/// not proof that the tested value is the connection string. Calls resolve by name within the class, the <c>using static</c>
/// classes of the file, and <c>Type.Member(</c>. The census looks at <c>ScratchPostgres.CreateAsync</c> only: a test that runs
/// <c>CREATE DATABASE</c> by hand through its own connection is outside it.</para>
/// </summary>
/* #1776 own-store: this class reads test source files and never connects to a database; the quoted store names in its
   samples are text it feeds the census. */
[Trait("Stage", "Guard")]
public sealed class LiveClassStoreSkipCensusTests
{
    /* Spelled in two parts so this file's own samples are not mistaken for a test that mints a database. */
    private const string CreateCall = "ScratchPostgres" + ".CreateAsync(";

    private const string NullTest = @"!?\s*(?:string\.IsNullOrEmpty|string\.IsNullOrWhiteSpace|[\w.!?]+\s+is\s+null|[\w.!?]+\s*==\s*null)";

    private static readonly Regex SkipGuard = new(@"Assert\.Skip(?:When|Unless)?\(\s*" + NullTest, RegexOptions.Compiled);

    private static readonly Regex ReturnGuard = new(
        @"if\s*\([^\n]*?(?:IsNullOrEmpty|IsNullOrWhiteSpace|\bis null|== null)[^\n]*\)\s*\{?\s*(?:return|throw)",
        RegexOptions.Compiled);

    private static readonly Regex MethodStart = new(
        @"^(\s*)(?:\[[^\]]*\]\s*)*(?:public|private|internal|protected|static|async|override|sealed|virtual)\b[^;=]*?\b(\w+)\s*(?:<[^>(]*>)?\(",
        RegexOptions.Compiled);

    private static readonly Regex PropertyStart = new(
        @"^(\s*)(?:public|private|internal|protected|static)\b[^;=(]*?\b(\w+)\s*(?:\{.*|=>.*)?$",
        RegexOptions.Compiled);

    private static readonly Regex TypeStart = new(
        @"^(\s*)(?:(?:public|private|internal|protected|sealed|static|abstract|partial|file|readonly)\s+)*(?:class|record\s+struct|record|struct|interface)\s+(\w+)",
        RegexOptions.Compiled);

    private static readonly Regex Call = new(@"(?:(?<![\w.])|\b(\w+)\.)(\w+)\s*\(", RegexOptions.Compiled);

    [Fact]
    public void EveryPathToAScratchDatabase_SkipsFirstOnAnUnsetConnectionString()
    {
        var sources = ReadTestSources();
        var scanned = sources.Count(s => s.Value.Contains(CreateCall, StringComparison.Ordinal));
        Assert.True(scanned > 100, $"the census read {scanned} files that mint a scratch database; the walk to the test sources is broken");

        var findings = FindUnguardedSites(sources, CreateCall);

        Assert.True(findings.Count == 0, Describe(findings));
    }

    [Fact]
    public void TheSharedStoreFixture_ReturnsBeforeConnectingWhenNoStoreIsSet()
    {
        var sources = ReadTestSources();
        Assert.True(sources.ContainsKey("LivePostgresStoreFixture.cs"), "LivePostgresStoreFixture.cs is not among the test sources");

        var findings = FindUnguardedSites(
            new Dictionary<string, string> { ["LivePostgresStoreFixture.cs"] = sources["LivePostgresStoreFixture.cs"] },
            "new NpgsqlConnection(");

        Assert.True(findings.Count == 0, Describe(findings));
    }

    [Fact]
    public void ThePreFix5756DrainHelpers_AreFlagged()
    {
        /* The text of both drain classes as #5726 merged them: the helper minted the database at once. */
        var preFix = $$"""
            public sealed class ExternalAlertDrainLiveTests
            {
                private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

                [Fact]
                public async Task ADrainOnce_MovesTheRows()
                {
                    await WithDrainAsync(async rig => { await Task.CompletedTask; });
                }

                private static async Task WithDrainAsync(Func<Rig, Task> body)
                {
                    var ct = TestContext.Current.CancellationToken;
                    await using var scratch = await {{CreateCall}}ConnectionString!, ct);
                    await using var seed = new NpgsqlConnection(scratch.ConnectionString);
                }
            }
            """;
        var findings = FindUnguardedSites(Single("ExternalAlertDrainLiveTests.cs", preFix), CreateCall);

        var finding = Assert.Single(findings);
        Assert.Equal("WithDrainAsync", finding.Member);
        Assert.Equal("ExternalAlertDrainLiveTests.cs", finding.File);
        var message = Describe(findings);
        Assert.Contains("ExternalAlertDrainLiveTests.cs", message, StringComparison.Ordinal);
        Assert.Contains("WithDrainAsync", message, StringComparison.Ordinal);
        Assert.Contains(CreateCall, message, StringComparison.Ordinal);
        Assert.Contains("Assert.SkipWhen(string.IsNullOrEmpty(", message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCurrentDrainHelper_AndEveryAcceptedShape_Pass()
    {
        var current = $$"""
            public sealed class ExternalAlertDrainLiveTests
            {
                private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

                [Fact]
                public async Task ADrainOnce_MovesTheRows()
                {
                    await WithDrainAsync(async rig => { await Task.CompletedTask; });
                }

                private static async Task WithDrainAsync(Func<Rig, Task> body)
                {
                    /* #5755: the skip comes before the create. */
                    Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the drain's live facts.");

                    var ct = TestContext.Current.CancellationToken;
                    await using var scratch = await {{CreateCall}}ConnectionString!, ct);
                }
            }
            """;
        AssertNone(FindUnguardedSites(Single("Current.cs", current), CreateCall));

        var shapes = $$"""
            public sealed class Shapes
            {
                private static string Cs()
                {
                    var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
                    Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG.");
                    return cs!;
                }

                private static string BaseConnectionString
                {
                    get
                    {
                        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
                        Assert.SkipWhen(
                            string.IsNullOrWhiteSpace(cs),
                            "Set DARLING_TEST_PG.");
                        return cs!;
                    }
                }

                [Fact]
                public async Task ThroughAnArgumentCall()
                {
                    await using var scratch = await {{CreateCall}}Cs(), default);
                }

                [Fact]
                public async Task ThroughAGuardedProperty()
                {
                    await using var scratch = await {{CreateCall}}BaseConnectionString, default);
                }

                [Fact]
                public async Task ThroughALocalSkip()
                {
                    var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
                    Assert.SkipUnless(!string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG.");
                    await using var scratch = await {{CreateCall}}cs!, default);
                }

                private static async Task<ScratchPostgres> OpenAsync() => await {{CreateCall}}Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, default);

                [Fact]
                public async Task ACallerThatSkipsFirst()
                {
                    var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
                    Assert.SkipWhen(cs is null, "Set DARLING_TEST_PG.");
                    await using var scratch = await OpenAsync();
                }

                [Fact]
                public async Task AnotherCallerThroughAGuardedHelper()
                {
                    Cs();
                    await using var scratch = await OpenAsync();
                }
            }

            public sealed class StoreFixtureShape : IAsyncLifetime
            {
                private ScratchPostgres? _scratch;

                public async ValueTask InitializeAsync()
                {
                    var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
                    if (string.IsNullOrEmpty(baseConnectionString))
                    {
                        return;
                    }

                    _scratch = await {{CreateCall}}baseConnectionString, CancellationToken.None);
                }
            }
            """;
        AssertNone(FindUnguardedSites(Single("Shapes.cs", shapes), CreateCall));

        var crossFile = new Dictionary<string, string>
        {
            ["Support.cs"] = """
                internal static class LiveSupport
                {
                    internal static string RequireLivePostgres()
                    {
                        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
                        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG.");
                        return cs!;
                    }
                }
                """,
            ["User.cs"] = $$"""
                using static Darling.Tests.LiveSupport;

                public sealed class User
                {
                    [Fact]
                    public async Task ThroughAUsingStaticHelper()
                    {
                        var cs = RequireLivePostgres();
                        await using var scratch = await {{CreateCall}}cs, default);
                    }

                    [Fact]
                    public async Task ThroughAQualifiedHelper()
                    {
                        var cs = LiveSupport.RequireLivePostgres();
                        await using var scratch = await {{CreateCall}}cs, default);
                    }
                }
                """,
        };
        AssertNone(FindUnguardedSites(crossFile, CreateCall));
    }

    [Fact]
    public void TheDetector_FlagsEveryShapeThatCanReachTheCreateUnskipped()
    {
        var source = $$"""
            public sealed class Gaps
            {
                [Fact]
                public async Task NoSkipAtAll()
                {
                    await using var scratch = await {{CreateCall}}Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, default);
                }

                [Fact]
                public async Task SkipComesAfterTheCreate()
                {
                    await using var scratch = await {{CreateCall}}Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, default);
                    Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), "too late");
                }

                [Fact]
                public async Task SkipsOnSomethingElse()
                {
                    var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
                    Assert.SkipWhen(!TimescaleAvailable, "needs TimescaleDB");
                    await using var scratch = await {{CreateCall}}cs!, default);
                }

                private static async Task<ScratchPostgres> HelperWithOneBadCaller()
                {
                    return await {{CreateCall}}Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, default);
                }

                [Fact]
                public async Task SkipsFirst()
                {
                    Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), "Set DARLING_TEST_PG.");
                    await using var scratch = await HelperWithOneBadCaller();
                }

                [Fact]
                public async Task DoesNot()
                {
                    await using var scratch = await HelperWithOneBadCaller();
                }
            }

            public sealed class FixtureShape : IAsyncLifetime
            {
                public async ValueTask InitializeAsync()
                {
                    var scratch = await {{CreateCall}}Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, CancellationToken.None);
                }
            }
            """;

        var findings = FindUnguardedSites(Single("Gaps.cs", source), CreateCall);

        Assert.Equal(
            new[] { "NoSkipAtAll", "SkipComesAfterTheCreate", "SkipsOnSomethingElse", "HelperWithOneBadCaller", "InitializeAsync" },
            findings.Select(f => f.Member).ToArray());
    }

    /// <summary>Every site in <paramref name="sources"/> (file name to text) that contains <paramref name="siteToken"/> on a path with no skip first.</summary>
    internal static IReadOnlyList<Site> FindUnguardedSites(IReadOnlyDictionary<string, string> sources, string siteToken)
    {
        var members = new List<Member>();
        foreach (var (file, text) in sources.OrderBy(s => s.Key, StringComparer.Ordinal))
        {
            members.AddRange(ParseMembers(file, text));
        }

        var byType = members.GroupBy(m => m.Type).ToDictionary(g => g.Key, g => g.ToList());
        var callIndex = new Dictionary<string, List<(Member Caller, int Line, string? Qualifier)>>(StringComparer.Ordinal);
        foreach (var member in members)
        {
            for (var k = member.Start; k < member.End; k++)
            {
                foreach (Match call in Call.Matches(member.Lines[k]))
                {
                    var name = call.Groups[2].Value;
                    if (!callIndex.TryGetValue(name, out var list))
                    {
                        callIndex[name] = list = [];
                    }

                    list.Add((member, k, call.Groups[1].Success ? call.Groups[1].Value : null));
                }
            }
        }

        List<Member> Resolve(string name, string? qualifier, Member origin)
        {
            var found = new List<Member>();
            if (qualifier is null)
            {
                found.AddRange(members.Where(m => m.File == origin.File && m.Type == origin.Type && m.Name == name));
                foreach (var used in origin.UsingStatics)
                {
                    if (byType.TryGetValue(used, out var inType))
                    {
                        found.AddRange(inType.Where(m => m.Name == name));
                    }
                }
            }
            else if (byType.TryGetValue(qualifier, out var inType))
            {
                found.AddRange(inType.Where(m => m.Name == name && (!m.Nested || m.File == origin.File)));
            }

            return found;
        }

        bool GuardedBefore(Member member, int line)
        {
            var before = string.Join("\n", member.Lines[member.Start..line]);
            if (SkipGuard.IsMatch(before) || ReturnGuard.IsMatch(before))
            {
                return true;
            }

            /* The site's own line counts for a call: `Create(Cs(), ct)` evaluates Cs() first. */
            var through = string.Join("\n", member.Lines[member.Start..(line + 1)]);
            foreach (Match call in Call.Matches(through))
            {
                var qualifier = call.Groups[1].Success ? call.Groups[1].Value : null;
                if (Resolve(call.Groups[2].Value, qualifier, member).Any(t => !ReferenceEquals(t, member) && !t.IsProperty && t.SkipsOnNull))
                {
                    return true;
                }
            }

            return members.Any(p => p.IsProperty && p.SkipsOnNull && p.File == member.File && p.Type == member.Type
                && Regex.IsMatch(through, @"(?<![\w.])" + Regex.Escape(p.Name) + @"\b(?!\s*\()"));
        }

        bool Satisfied(Member member, int line, int depth)
        {
            if (GuardedBefore(member, line))
            {
                return true;
            }

            if (depth > 5 || !callIndex.TryGetValue(member.Name, out var calls))
            {
                return false;
            }

            var callers = calls.Where(c => !ReferenceEquals(c.Caller, member) && Resolve(member.Name, c.Qualifier, c.Caller).Contains(member)).ToList();
            return callers.Count > 0 && callers.All(c => Satisfied(c.Caller, c.Line, depth + 1));
        }

        var findings = new List<Site>();
        foreach (var member in members)
        {
            for (var i = member.Start; i < member.End; i++)
            {
                if (IsSite(member.Lines[i], siteToken) && !Satisfied(member, i, 0))
                {
                    findings.Add(new Site(member.File, member.Name, i + 1, member.Lines[i].Trim()));
                }
            }
        }

        /* A site outside every member (a field initializer, say) has no path to check, so it is a finding too. */
        foreach (var (file, text) in sources.OrderBy(s => s.Key, StringComparer.Ordinal))
        {
            var lines = Clean(text).Split('\n');
            for (var i = 0; i < lines.Length; i++)
            {
                if (IsSite(lines[i], siteToken)
                    && !members.Any(m => m.File == file && m.Start <= i && i < m.End))
                {
                    findings.Add(new Site(file, "(outside any member)", i + 1, lines[i].Trim()));
                }
            }
        }

        return findings;
    }

    private static bool IsSite(string line, string siteToken)
    {
        var at = line.IndexOf(siteToken, StringComparison.Ordinal);
        return at >= 0 && !InsideString(line, at);
    }

    private static void AssertNone(IReadOnlyList<Site> findings) => Assert.True(findings.Count == 0, Describe(findings));

    /// <summary>True when the token at <paramref name="index"/> sits inside a quoted string on its line, as in a census that names the call it looks for.</summary>
    private static bool InsideString(string line, int index)
    {
        var quotes = 0;
        for (var i = 0; i < index; i++)
        {
            if (line[i] == '"' && (i == 0 || line[i - 1] != (char)92))
            {
                quotes++;
            }
        }

        return quotes % 2 == 1;
    }

    internal static string Describe(IReadOnlyList<Site> findings) =>
        "These test members reach a scratch database (or a live store) on a path that does not first skip when DARLING_TEST_PG is unset. "
        + "Dev's whole-tree guards job runs this project with no store, so each one fails there with ArgumentNullException (Parameter 'Host') "
        + "(#5755, #5759). Put `Assert.SkipWhen(string.IsNullOrEmpty(connectionString), \"Set DARLING_TEST_PG to run ...\");` before the "
        + "line shown (or at the top of the helper that holds it, or make every caller skip first; a fixture's InitializeAsync returns when "
        + "the variable is unset):" + Environment.NewLine
        + string.Join(Environment.NewLine, findings.Select(f => $"  {f.File}: {f.Member} (line {f.Line}): {f.Text}"));

    internal readonly record struct Site(string File, string Member, int Line, string Text);

    private sealed class Member
    {
        public required string File { get; init; }
        public required string Name { get; init; }
        public required string Type { get; init; }
        public required bool Nested { get; init; }
        public required bool IsProperty { get; init; }
        public required int Start { get; init; }
        public int End { get; set; }
        public required string[] Lines { get; init; }
        public required IReadOnlyList<string> UsingStatics { get; init; }
        public bool SkipsOnNull { get; set; }
    }

    private static Dictionary<string, string> Single(string file, string text) => new() { [file] = text };

    /// <summary>The text with comments blanked (line numbers kept) and CRLF folded to LF.</summary>
    private static string Clean(string text)
    {
        text = text.Replace("\r\n", "\n", StringComparison.Ordinal);
        text = Regex.Replace(text, @"/\*.*?\*/", m => new string('\n', m.Value.Count(c => c == '\n')), RegexOptions.Singleline);
        return Regex.Replace(text, @"(?<!:)//[^\n]*", string.Empty);
    }

    /// <summary>
    /// Cuts a file into its members: a line at the member indent of the innermost enclosing type (that type's indent plus four)
    /// that opens with a modifier and a name and a parenthesis (a method) or no parenthesis at all (a property).
    /// </summary>
    private static List<Member> ParseMembers(string file, string text)
    {
        var source = Clean(text);
        var lines = source.Split('\n');
        var usingStatics = Regex.Matches(source, @"using static ([\w.]+);").Select(m => m.Groups[1].Value.Split('.')[^1]).ToList();
        var stack = new List<(int Indent, string Name)>();
        var members = new List<Member>();
        Member? current = null;
        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i];
            var type = TypeStart.Match(line);
            if (type.Success && !line.TrimEnd().EndsWith(';'))
            {
                var indent = type.Groups[1].Length;
                while (stack.Count > 0 && stack[^1].Indent >= indent)
                {
                    stack.RemoveAt(stack.Count - 1);
                }

                stack.Add((indent, type.Groups[2].Value));
                if (current is not null)
                {
                    current.End = i;
                    current = null;
                }

                continue;
            }

            if (type.Success)
            {
                continue;
            }

            if (stack.Count > 0 && line.Trim() == "}" && line.Length - line.TrimStart().Length == stack[^1].Indent)
            {
                stack.RemoveAt(stack.Count - 1);
            }

            var match = MethodStart.Match(line);
            var isProperty = false;
            if (!match.Success)
            {
                match = PropertyStart.Match(line);
                isProperty = match.Success;
            }

            if (match.Success && stack.Count > 0 && match.Groups[1].Length == stack[^1].Indent + 4)
            {
                if (current is not null)
                {
                    current.End = i;
                }

                current = new Member
                {
                    File = file,
                    Name = match.Groups[2].Value,
                    Type = stack[^1].Name,
                    Nested = stack[^1].Indent > 0,
                    IsProperty = isProperty,
                    Start = i,
                    End = lines.Length,
                    Lines = lines,
                    UsingStatics = usingStatics,
                };
                members.Add(current);
            }
        }

        foreach (var member in members)
        {
            member.SkipsOnNull = SkipGuard.IsMatch(string.Join("\n", member.Lines[member.Start..member.End]));
        }

        return members;
    }

    private static Dictionary<string, string> ReadTestSources()
    {
        var directory = RepoFile.PathTo("Darling", "Darling.Tests");
        Assert.True(Directory.Exists(directory), $"{directory} is not a directory; the walk to the test sources is broken");

        var sources = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var path in Directory.EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories).OrderBy(p => p, StringComparer.Ordinal))
        {
            var relative = Path.GetRelativePath(directory, path);
            var segments = relative.Split(Path.DirectorySeparatorChar);
            var name = Path.GetFileName(path);

            /* Build output, the factory itself, and this file (its samples hold the call as text). */
            if (segments.Any(s => s is "bin" or "obj")
                || name is "ScratchPostgres.cs" or "LiveClassStoreSkipCensusTests.cs" or "ScratchDatabaseDisposalCensusTests.cs")
            {
                continue;
            }

            sources[relative.Replace(Path.DirectorySeparatorChar, (char)47)] = File.ReadAllText(path);
        }

        return sources;
    }
}
