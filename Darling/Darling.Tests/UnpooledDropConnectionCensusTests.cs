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
/// A test that runs <c>DROP DATABASE</c> does it on an unpooled connection, and quiesces the database's TimescaleDB
/// jobs first (#5549).
///
/// <para><b>The defect this closes.</b> A PostgreSQL client backend crashed with 0xC0000005 in a TimescaleDB
/// <c>run_job</c>, after the same backend had run a scratch <c>DROP DATABASE ... WITH (FORCE)</c>. The admin connection
/// string is the caller's <c>DARLING_TEST_PG</c>, so a pooled admin connection went back into the pool every live test
/// draws from, and the next test took the backend that had just run the drop. The ruling: a database drop and a job
/// run never share a backend. <c>ScratchPostgres</c> opens its own create, drop and sweep connections unpooled
/// (<see cref="ScratchPostgres.UnpooledAdminConnectionString"/>); this census holds every other test file to the same
/// rule.</para>
///
/// <para><b>What this pins.</b> For every PostgreSQL <c>DROP DATABASE</c> string literal in the test project (comments
/// are blanked first): if the enclosing method opens a connection itself (<c>new NpgsqlConnection(</c> or
/// <c>LiveStoreCleanup.RunAsync(</c>), that method must also hold the evidence of an unpooled connection
/// (<c>UnpooledAdminConnectionString(</c>, <c>ExitDrainConnectionString(</c> or <c>Pooling = false</c>). A method that
/// is handed its connection (a drop helper that takes the cleanup connection as a parameter) is held to the weaker
/// file-level rule: the file must hold that evidence somewhere. The file-level rule cannot see which connection the
/// caller passes, so a drop helper's callers are the thing a reviewer still checks; the method-level rule is what stops
/// the common shape, a new drop with its own pooled admin connection beside it. A file that is not about PostgreSQL
/// drops at all is named in <see cref="Exceptions"/> with its reason.</para>
///
/// <para><b>The second rule: quiesce before the drop.</b> The same ruling says TimescaleDB jobs are quiesced before every
/// in-test drop, so a job worker is never running in a database while it is dropped. Every PostgreSQL <c>DROP DATABASE</c>
/// site (the same sites, under the same <see cref="Exceptions"/>) must sit in a member that calls
/// <c>QuiesceTimescaleJobsAsync</c> BEFORE the drop, plain or <c>WITH (FORCE)</c> (the older census in
/// <see cref="ScratchPostgresQuiesceCensusTests"/> sees only the FORCE form). A method that cannot be dropping a database
/// with jobs, because the test never created the extension in it and never ran a migration there, is named in
/// <see cref="QuiesceExceptions"/> as <c>File.cs::Method</c> (or <c>File.cs</c> for the whole file) with its reason.</para>
///
/// <para><b>How a drop's member is found.</b> The census is a source scan with no parser package. A small brace walk over
/// the file (comments and string literals blanked, so a brace or a call inside text is never read) finds the innermost
/// method, constructor, local function, property or accessor body around the drop, at any indentation and with or without
/// an access modifier; for an expression-bodied member or a field it is the declaration up to its own <c>;</c>. A region
/// never spans two members, so a quiesce in a neighbour cannot excuse a drop. The quiesce counts only as a real call
/// (not inside a string or a comment) and only when it comes before the drop in that member.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class UnpooledDropConnectionCensusTests
{
    /// <summary>The files that contain the text <c>DROP DATABASE</c> in a string but never drop a PostgreSQL database on a pooled connection, each with why.</summary>
    internal static readonly IReadOnlyDictionary<string, string> Exceptions = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["DarlingCollectorRunnerTests.cs"] = "T-SQL against SQL Server (DROP DATABASE [name]); SQL Server has no backend pool shared with a TimescaleDB job.",
        ["ProcedureStatsDeferredPlanFetchLiveTests.cs"] = "T-SQL against SQL Server (SINGLE_USER, then DROP DATABASE [name]).",
        ["QueryStatsDeferredPlanFetchLiveTests.cs"] = "T-SQL against SQL Server (SINGLE_USER, then DROP DATABASE [name]).",
        ["DropXeSessionsLiveTests.cs"] = "A SQL-injection session-name fixture: the text is data a validator must refuse, never run as a statement.",
        ["DropXeSessionsPerInstallTests.cs"] = "A SQL-injection session-name fixture: the text is data a validator must refuse, never run as a statement.",
        ["DropXeSessionsReadOnlyIntentTests.cs"] = "A SQL-injection session-name fixture: the text is data a validator must refuse, never run as a statement.",
        ["DropXeSessionsVerbTests.cs"] = "A SQL-injection session-name fixture and a list of forbidden verbs: text a validator must refuse, never run as a statement.",
        ["UnpooledDropConnectionCensusTests.cs"] = "This census: its own samples hold the drop text on purpose.",
        ["ScratchPostgresQuiesceTests.cs"] = "The quiesce census: its samples hold the drop text on purpose, and the live test there drops through ScratchPostgres.",
    };

    /// <summary>
    /// The PostgreSQL drop sites that skip <c>QuiesceTimescaleJobsAsync</c>, each as <c>File.cs::Method</c> (or <c>File.cs</c>
    /// for the whole file) with why the dropped database cannot hold a TimescaleDB job.
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, string> QuiesceExceptions = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["ComposeStoreRolesLiveTests.cs::TheComposeStore_ProvisionsItsOwnRoles_AndTheRealHostsConnectAsViewerAndMcp_Gated"] =
            "operator_app_3914 is a bare CREATE DATABASE from template1, made to stand for an operator's own database; no extension or migration ever runs in it.",
        ["PgReadBinaryFileCapabilityLiveTests.cs::IsGrantedAsync_OnAWin1252Database_GrantsTheRoute_AndCachesItsEncoding"] =
            "pm_test_enc1252 is a bare WIN1252 database made from template0, so it never holds the TimescaleDB extension.",
    };

    /// <summary>Matches case-insensitively: T-SQL and PostgreSQL both accept <c>drop database</c>.</summary>
    private static readonly Regex DropStatement = new("DROP DATABASE", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>A call to the quiesce helper, matched in code only (comments and string literals are blanked first).</summary>
    private static readonly Regex QuiesceCall = new(@"\bQuiesceTimescaleJobsAsync\s*\(", RegexOptions.Compiled);

    /// <summary>
    /// The member's name: the identifier right before the first <c>(</c> of its declaration, after attribute lists are
    /// removed, with an optional generic argument list between (<c>Foo&lt;T&gt;(</c>).
    /// </summary>
    private static readonly Regex MethodName = new(@"(\w+)\s*(?:<[^()]*>)?\s*\(", RegexOptions.Compiled);

    private static readonly Regex AttributeList = new(@"\[[^\]]*\]", RegexOptions.Compiled);

    private static readonly Regex Evidence = new(
        @"UnpooledAdminConnectionString\(|ExitDrainConnectionString\(|Pooling\s*=\s*false", RegexOptions.Compiled);

    private static readonly Regex OwnConnection = new(@"new NpgsqlConnection\(|LiveStoreCleanup\.RunAsync\(", RegexOptions.Compiled);

    /// <summary>A type header: modifiers, then the keyword. A <c>where T : class</c> constraint on a method is not one.</summary>
    private static readonly Regex TypeWord = new(@"^(?:\w+\s+)*?(?:class|struct|interface|record|enum|namespace|union)\b", RegexOptions.Compiled);

    private static readonly Regex NewOrDelegateWord = new(@"\b(?:new|delegate)\b", RegexOptions.Compiled);

    /// <summary>First words that open a block which is not a member: control flow and the like.</summary>
    private static readonly HashSet<string> ControlWords = new(StringComparer.Ordinal)
    {
        "if", "else", "for", "foreach", "while", "do", "switch", "using", "lock", "try", "catch", "finally", "fixed",
        "unsafe", "checked", "unchecked", "case", "default",
    };

    /// <summary>First words that start a statement which a <c>{</c> continues (<c>return x switch { ... }</c>): not a member.</summary>
    private static readonly HashSet<string> StatementWords = new(StringComparer.Ordinal) { "return", "yield", "throw", "var" };

    /// <summary>What a <c>{ ... }</c> block is, which decides where a member (the region a drop is judged in) starts and ends.</summary>
    private enum FrameKind
    {
        /// <summary>The file itself.</summary>
        Root,

        /// <summary>A namespace, class, struct, interface, record or enum body.</summary>
        Type,

        /// <summary>A method, constructor, local function, property or accessor body, at any indentation.</summary>
        Member,

        /// <summary>A control-flow block (if, try, using, a bare block): the statement ends when it closes.</summary>
        Control,

        /// <summary>A lambda, an initializer, an interpolation hole: part of the statement around it.</summary>
        Other,
    }

    private sealed class Frame
    {
        public Frame(FrameKind kind, int headerStart, int open, Frame? parent)
        {
            Kind = kind;
            HeaderStart = headerStart;
            Open = open;
            StatementStart = open + 1;
            Parent = parent;
        }

        public FrameKind Kind { get; }

        public int HeaderStart { get; }

        public int Open { get; }

        public int Close { get; set; } = -1;

        /// <summary>Where the statement being read at this block's own level started: just after the last <c>;</c>, or the last closed member, type or control block.</summary>
        public int StatementStart { get; set; }

        /// <summary>Open parentheses not yet closed at this block's own level; a <c>{</c> inside them is a lambda or an initializer.</summary>
        public int Parens { get; set; }

        public Frame? Parent { get; }
    }

    /// <summary>
    /// One <c>DROP DATABASE</c> line: where it is, the member it sits in (<see cref="MemberText"/>, comments blanked,
    /// whole member), the member's name, and whether a real call to the quiesce helper comes before the drop in that member.
    /// </summary>
    internal sealed record DropSite(int Line, string MemberName, string MemberText, bool QuiescedBefore);

    /// <summary>
    /// Every <c>DROP DATABASE</c> line in <paramref name="source"/> (case-insensitive, comments blanked first) with its
    /// member. The member is the innermost method, constructor, local function, property or accessor body around the
    /// line, at any indentation and with or without an access modifier; for an expression-bodied member or a field it
    /// is the declaration up to its terminating <c>;</c>. It is never wider than that, so a quiesce in a neighbouring
    /// member can never excuse a drop. The quiesce call is looked for in code only (comments and string literals are
    /// blanked), and only before the drop.
    /// </summary>
    internal static List<DropSite> FindDropSites(string source)
    {
        var blanked = ScratchPostgresQuiesceCensusTests.BlankComments(source);
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);

        var drops = new List<(int Offset, int Line)>();
        var lineStart = 0;
        for (var line = 1; lineStart <= blanked.Length; line++)
        {
            var newline = blanked.IndexOf('\n', lineStart);
            var lineEnd = newline < 0 ? blanked.Length : newline;
            var found = DropStatement.Match(blanked, lineStart, lineEnd - lineStart);
            if (found.Success)
            {
                drops.Add((found.Index, line));
            }

            lineStart = lineEnd + 1;
        }

        var root = new Frame(FrameKind.Root, 0, -1, null);
        var top = root;
        var snapshots = new List<(Frame Inner, int ContainerStatementStart, int Offset, int Line)>();
        var next = 0;
        for (var i = 0; i < code.Length; i++)
        {
            if (next < drops.Count && drops[next].Offset == i)
            {
                var container = top;
                while (container.Kind is not (FrameKind.Type or FrameKind.Root))
                {
                    container = container.Parent!;
                }

                snapshots.Add((top, container.StatementStart, i, drops[next].Line));
                next++;
            }

            switch (code[i])
            {
                case '(':
                    top.Parens++;
                    break;
                case ')':
                    if (top.Parens > 0)
                    {
                        top.Parens--;
                    }

                    break;
                case ';':
                    if (top.Parens == 0)
                    {
                        top.StatementStart = i + 1;
                    }

                    break;
                case '{':
                    var header = code.Substring(top.StatementStart, i - top.StatementStart);
                    var kind = top.Parens > 0 ? FrameKind.Other : Classify(header);
                    var firstCode = top.StatementStart;
                    while (firstCode < i && char.IsWhiteSpace(code[firstCode]))
                    {
                        firstCode++;
                    }

                    top = new Frame(kind, firstCode, i, top);
                    break;
                case '}':
                    if (top.Parent is not null)
                    {
                        top.Close = i;
                        var closed = top;
                        top = top.Parent;
                        if (closed.Kind != FrameKind.Other)
                        {
                            top.StatementStart = i + 1;
                        }
                    }

                    break;
            }
        }

        var sites = new List<DropSite>();
        foreach (var (inner, containerStatementStart, offset, line) in snapshots)
        {
            var member = inner;
            while (member is not null && member.Kind != FrameKind.Member)
            {
                member = member.Parent;
            }

            int start;
            int end;
            if (member is not null)
            {
                start = member.HeaderStart;
                end = member.Close >= 0 ? member.Close : code.Length - 1;
            }
            else
            {
                /* An expression-bodied member or a field: the declaration, up to the first ; that is not inside a nested block. */
                start = containerStatementStart;
                end = code.Length - 1;
                var depth = 0;
                for (var j = offset; j < code.Length; j++)
                {
                    if (code[j] == '{')
                    {
                        depth++;
                    }
                    else if (code[j] == '}')
                    {
                        if (depth == 0)
                        {
                            end = j;
                            break;
                        }

                        depth--;
                    }
                    else if (code[j] == ';' && depth == 0)
                    {
                        end = j;
                        break;
                    }
                }
            }

            start = Math.Min(start, offset);
            end = Math.Max(end, offset);
            var region = code.Substring(start, end - start + 1);
            var declared = MethodName.Match(AttributeList.Replace(region, " "));
            sites.Add(new DropSite(
                line,
                declared.Success ? declared.Groups[1].Value : string.Empty,
                blanked.Substring(start, end - start + 1),
                QuiesceCall.IsMatch(code.Substring(start, offset - start))));
        }

        return sites;
    }

    /// <summary>What the block opened by the <c>{</c> that follows <paramref name="header"/> is.</summary>
    private static FrameKind Classify(string header)
    {
        var text = StripParentheses(AttributeList.Replace(header, " ")).Trim();
        if (text.StartsWith("await ", StringComparison.Ordinal))
        {
            text = text[6..].TrimStart();
        }

        var first = Regex.Match(text, @"^\w+");
        if (text.Length == 0 || (first.Success && ControlWords.Contains(first.Value)))
        {
            return FrameKind.Control;
        }

        if ((first.Success && StatementWords.Contains(first.Value))
            || text.Contains('=', StringComparison.Ordinal)
            || NewOrDelegateWord.IsMatch(text))
        {
            return FrameKind.Other;
        }

        return TypeWord.IsMatch(text) ? FrameKind.Type : FrameKind.Member;
    }

    /// <summary>The text with every parenthesised run (nested ones too) removed, parentheses included.</summary>
    private static string StripParentheses(string text)
    {
        var sb = new System.Text.StringBuilder(text.Length);
        var depth = 0;
        foreach (var c in text)
        {
            if (c == '(')
            {
                depth++;
            }
            else if (c == ')')
            {
                if (depth > 0)
                {
                    depth--;
                }
            }
            else if (depth == 0)
            {
                sb.Append(c);
            }
        }

        return sb.ToString();
    }

    /// <summary>
    /// Every drop site that breaks the rule, as <c>file:line</c>, and how many drop sites were looked at.
    /// </summary>
    internal static (int Sites, List<string> Pooled) Scan(IEnumerable<(string Name, string Source)> files)
    {
        var sites = 0;
        var pooled = new List<string>();
        foreach (var (name, source) in files)
        {
            if (Exceptions.ContainsKey(Path.GetFileName(name)))
            {
                continue;
            }

            var blanked = ScratchPostgresQuiesceCensusTests.BlankComments(source);
            foreach (var site in FindDropSites(source))
            {
                sites++;
                var ok = OwnConnection.IsMatch(site.MemberText) ? Evidence.IsMatch(site.MemberText) : Evidence.IsMatch(blanked);
                if (!ok)
                {
                    pooled.Add($"{name}:{site.Line}");
                }
            }
        }

        return (sites, pooled);
    }

    /// <summary>
    /// Every PostgreSQL drop site that has no call to <c>QuiesceTimescaleJobsAsync</c> before it in its own member and is
    /// not in <see cref="QuiesceExceptions"/>, as <c>file:line</c>, and how many drop sites were looked at.
    /// </summary>
    internal static (int Sites, List<string> Unquiesced) ScanForQuiesce(IEnumerable<(string Name, string Source)> files)
    {
        var sites = 0;
        var unquiesced = new List<string>();
        foreach (var (name, source) in files)
        {
            var file = Path.GetFileName(name);
            if (Exceptions.ContainsKey(file))
            {
                continue;
            }

            foreach (var site in FindDropSites(source))
            {
                sites++;
                if (site.QuiescedBefore
                    || QuiesceExceptions.ContainsKey(file)
                    || (site.MemberName.Length > 0 && QuiesceExceptions.ContainsKey($"{file}::{site.MemberName}")))
                {
                    continue;
                }

                unquiesced.Add($"{name}:{site.Line}");
            }
        }

        return (sites, unquiesced);
    }

    [Fact]
    public void EveryPostgresDropInTheTestProject_RunsOnAnUnpooledConnection()
    {
        var directory = FindTestProjectDirectory();
        Assert.True(directory is not null, "could not find Darling/Darling.Tests from the test binary.");

        var files = Directory.EnumerateFiles(directory!, "*.cs", SearchOption.AllDirectories)
            .Select(path => (Name: Path.GetRelativePath(directory!, path).Replace('\\', '/'), Path: path))
            .Where(found => !found.Name.Split('/').Any(segment => segment is "bin" or "obj"))
            .Select(found => (found.Name, File.ReadAllText(found.Path)))
            .ToList();

        var (sites, pooled) = Scan(files);

        Assert.True(sites >= 9, $"the scan found {sites} PostgreSQL DROP DATABASE site(s); the project has at least 9, so the match found nothing.");
        Assert.True(pooled.Count == 0,
            "These DROP DATABASE statements run where no unpooled connection is in sight (#5549). A pooled connection returns the "
            + "backend that ran the drop to the pool every live test draws from, and a TimescaleDB job run on it crashed. Open the "
            + "connection with ScratchPostgres.UnpooledAdminConnectionString(...), or add the file to Exceptions with its reason: "
            + string.Join(", ", pooled));
    }

    [Fact]
    public void EveryPostgresDropInTheTestProject_QuiescesTheTimescaleJobsFirst()
    {
        var directory = FindTestProjectDirectory();
        Assert.True(directory is not null, "could not find Darling/Darling.Tests from the test binary.");

        var files = Directory.EnumerateFiles(directory!, "*.cs", SearchOption.AllDirectories)
            .Select(path => (Name: Path.GetRelativePath(directory!, path).Replace('\\', '/'), Path: path))
            .Where(found => !found.Name.Split('/').Any(segment => segment is "bin" or "obj"))
            .Select(found => (found.Name, File.ReadAllText(found.Path)))
            .ToList();

        var (sites, unquiesced) = ScanForQuiesce(files);

        Assert.True(sites >= 9, $"the scan found {sites} PostgreSQL DROP DATABASE site(s); the project has at least 9, so the match found nothing.");
        Assert.True(unquiesced.Count == 0,
            "These DROP DATABASE statements are not in a method that calls ScratchPostgres.QuiesceTimescaleJobsAsync (#5549). A drop "
            + "while a TimescaleDB job worker runs in the database kills the worker, and a job run next to a drop crashed a backend. "
            + "Call the helper before the drop, or add File.cs::Method to QuiesceExceptions with the reason the database holds no job: "
            + string.Join(", ", unquiesced));
    }

    [Fact]
    public void TheQuiesceExceptions_NameRealFiles_AndEachOneStillHoldsTheDropText()
    {
        var directory = FindTestProjectDirectory();
        Assert.True(directory is not null, "could not find Darling/Darling.Tests from the test binary.");

        foreach (var (key, reason) in QuiesceExceptions)
        {
            Assert.False(string.IsNullOrWhiteSpace(reason), $"{key} is allowed without a reason.");
            var parts = key.Split("::");
            var path = Path.Combine(directory!, parts[0]);
            Assert.True(File.Exists(path), $"{parts[0]} is in QuiesceExceptions but is not in the test project; remove the entry.");
            Assert.False(Exceptions.ContainsKey(parts[0]), $"{parts[0]} is already skipped by Exceptions; remove the entry from QuiesceExceptions.");
            var text = File.ReadAllText(path);
            Assert.True(text.Contains("DROP DATABASE", StringComparison.OrdinalIgnoreCase),
                $"{parts[0]} no longer holds DROP DATABASE; remove {key} from QuiesceExceptions.");
            if (parts.Length > 1)
            {
                Assert.True(Regex.IsMatch(text, @"\b" + Regex.Escape(parts[1]) + @"\s*(?:<[^()]*>)?\s*\("),
                    $"{parts[0]} no longer declares {parts[1]}; remove {key} from QuiesceExceptions.");
                Assert.True(MethodStillSkipsQuiesce(parts[1], text),
                    $"{parts[1]} in {parts[0]} no longer holds a DROP DATABASE that skips the quiesce; remove {key} from QuiesceExceptions.");
            }
        }
    }

    /// <summary>True when <paramref name="method"/> in <paramref name="source"/> holds a drop with no quiesce call before it, so its allow-list entry is still needed.</summary>
    private static bool MethodStillSkipsQuiesce(string method, string source) =>
        FindDropSites(source).Any(site => site.MemberName == method && !site.QuiescedBefore);

    [Fact]
    public void ADropWithoutTheQuiesce_IsReported_AndOneWithItOrAnAllowListedOneIsNot()
    {
        const string quiesced =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        await ScratchPostgres.QuiesceTimescaleJobsAsync(cs, n);\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n}\n";
        const string plainBare =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n}\n";
        const string forceBare =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n} WITH (FORCE)\", admin);\n    }\n}\n";
        /* A quiesce in another method of the file does not excuse this one. */
        const string quiesceElsewhere =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n\n"
            + "    public async Task Other()\n    {\n        await ScratchPostgres.QuiesceTimescaleJobsAsync(cs, n);\n    }\n}\n";
        const string commentOnlyQuiesce =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        // QuiesceTimescaleJobsAsync(cs, n) would go here\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n}\n";
        const string commentOnly = "public class A\n{\n    // DROP DATABASE x\n    /// DROP DATABASE y\n}\n";
        var allowListed = QuiesceExceptions.Keys.First();
        var allowListedFile = allowListed.Split("::")[0];
        var allowListedMethod = allowListed.Split("::")[1];
        var allowListedSource =
            "public class A\n{\n    public async Task " + allowListedMethod + "()\n    {\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n}\n";

        Assert.Empty(ScanForQuiesce([("Q.cs", quiesced)]).Unquiesced);
        Assert.Equal(["P.cs:5"], ScanForQuiesce([("P.cs", plainBare)]).Unquiesced);
        Assert.Equal(["F.cs:5"], ScanForQuiesce([("F.cs", forceBare)]).Unquiesced);
        Assert.Equal(["E.cs:5"], ScanForQuiesce([("E.cs", quiesceElsewhere)]).Unquiesced);
        Assert.Equal(["M.cs:6"], ScanForQuiesce([("M.cs", commentOnlyQuiesce)]).Unquiesced);
        Assert.Equal(0, ScanForQuiesce([("C.cs", commentOnly)]).Sites);
        /* The allow-list names a file and a method: that method in that file is excused, the same method in another file is not. */
        Assert.Empty(ScanForQuiesce([(allowListedFile, allowListedSource)]).Unquiesced);
        Assert.Equal(["Other.cs:5"], ScanForQuiesce([("Other.cs", allowListedSource)]).Unquiesced);
        /* A SQL Server or fixture file is outside the rule altogether (Exceptions). */
        Assert.Equal(0, ScanForQuiesce([("DarlingCollectorRunnerTests.cs", plainBare)]).Sites);
    }

    /// <summary>
    /// The ways the quiesce rule once passed a drop it should have failed: a lowercase <c>drop database</c>, a quiesce call
    /// that is only text (a string literal), a quiesce that comes after the drop, a member with no access modifier, a
    /// local function at a deeper indent, and an expression-bodied neighbour that stretched a region over two members.
    /// Each sample is a whole source file; the line number in each expectation is the drop line.
    /// </summary>
    [Fact]
    public void TheQuiesceRule_CannotBePassedBy_ALowercaseDrop_AStringCall_ALateCall_OrANeighbouringMember()
    {
        const string lowercase =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        await using var drop = new NpgsqlCommand($\"drop database if exists {n}\", admin);\n    }\n}\n";
        /* The string also holds a } so a scan that reads literals as code would close the member early. */
        const string quiesceInString =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        var s = \"await ScratchPostgres.QuiesceTimescaleJobsAsync(cs, n); }\";\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n}\n";
        const string quiesceAfterTheDrop =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n"
            + "        await ScratchPostgres.QuiesceTimescaleJobsAsync(cs, n);\n    }\n}\n";
        /* The member above quiesces; the one with no access modifier does not, and one member must not excuse the other. */
        const string noModifier =
            "public class A\n{\n    public async Task Other()\n    {\n"
            + "        await ScratchPostgres.QuiesceTimescaleJobsAsync(cs, n);\n    }\n\n"
            + "    Task Go()\n    {\n        return Run($\"DROP DATABASE IF EXISTS {n}\");\n    }\n}\n";
        /* The enclosing method quiesces, but the local function that drops does not. */
        const string localFunctionWithout =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        await ScratchPostgres.QuiesceTimescaleJobsAsync(cs, n);\n"
            + "        async Task Drop()\n        {\n"
            + "            await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n        }\n"
            + "        await Drop();\n    }\n}\n";
        const string localFunctionWith =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        async Task Drop()\n        {\n"
            + "            await ScratchPostgres.QuiesceTimescaleJobsAsync(cs, n);\n"
            + "            await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n        }\n"
            + "        await Drop();\n    }\n}\n";
        /* The expression-bodied drop ends at its own semicolon, not at the next member's closing brace. */
        const string expressionBodiedNeighbour =
            "public class A\n{\n    public Task Drop() => admin.RunAsync(\"DROP DATABASE IF EXISTS x\");\n\n"
            + "    public async Task Other()\n    {\n        await ScratchPostgres.QuiesceTimescaleJobsAsync(cs, n);\n    }\n}\n";
        const string expressionBodiedQuiesced =
            "public class A\n{\n    public Task Drop() => ScratchPostgres.QuiesceTimescaleJobsAsync(cs, n)"
            + ".ContinueWith(_ => admin.RunAsync(\"DROP DATABASE IF EXISTS x\"));\n}\n";
        /* The shape every LiveStoreCleanup drop has: the quiesce and the drop sit in one lambda inside the member. */
        const string insideLambda =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        await LiveStoreCleanup.RunAsync(cs, ok, async (cleanup, ct) =>\n        {\n"
            + "            await ScratchPostgres.QuiesceTimescaleJobsAsync(cs, n);\n"
            + "            await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", cleanup);\n        });\n    }\n}\n";

        Assert.Equal(["L.cs:5"], ScanForQuiesce([("L.cs", lowercase)]).Unquiesced);
        Assert.Equal(["L.cs:5"], Scan([("L.cs", lowercase)]).Pooled);
        Assert.Equal(["S.cs:6"], ScanForQuiesce([("S.cs", quiesceInString)]).Unquiesced);
        Assert.Equal(["T.cs:5"], ScanForQuiesce([("T.cs", quiesceAfterTheDrop)]).Unquiesced);
        Assert.Equal(["N.cs:10"], ScanForQuiesce([("N.cs", noModifier)]).Unquiesced);
        Assert.Equal(["D.cs:8"], ScanForQuiesce([("D.cs", localFunctionWithout)]).Unquiesced);
        Assert.Empty(ScanForQuiesce([("W.cs", localFunctionWith)]).Unquiesced);
        Assert.Equal(["X.cs:3"], ScanForQuiesce([("X.cs", expressionBodiedNeighbour)]).Unquiesced);
        Assert.Empty(ScanForQuiesce([("Y.cs", expressionBodiedQuiesced)]).Unquiesced);
        Assert.Empty(ScanForQuiesce([("Z.cs", insideLambda)]).Unquiesced);
    }

    /// <summary>A generic method's name is found (<c>Name&lt;T&gt;(</c>), so an allow-list entry for it matches and its still-needed check works.</summary>
    [Fact]
    public void AGenericMethod_OnTheAllowList_IsExcused_AndFoundByTheStaleEntryCheck()
    {
        var allowListed = QuiesceExceptions.Keys.First();
        var allowListedFile = allowListed.Split("::")[0];
        var allowListedMethod = allowListed.Split("::")[1];
        var generic =
            "public class A\n{\n    public async Task " + allowListedMethod + "<T>() where T : class\n    {\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n}\n";

        Assert.Empty(ScanForQuiesce([(allowListedFile, generic)]).Unquiesced);
        Assert.Equal(["Other.cs:5"], ScanForQuiesce([("Other.cs", generic)]).Unquiesced);
        Assert.True(MethodStillSkipsQuiesce(allowListedMethod, generic));
        Assert.False(MethodStillSkipsQuiesce("SomeOtherMethod", generic));
    }

    [Fact]
    public void TheExceptions_NameRealFiles_AndEachOneStillHoldsTheDropText()
    {
        var directory = FindTestProjectDirectory();
        Assert.True(directory is not null, "could not find Darling/Darling.Tests from the test binary.");

        foreach (var (file, reason) in Exceptions)
        {
            Assert.False(string.IsNullOrWhiteSpace(reason), $"{file} is allowed without a reason.");
            var path = Path.Combine(directory!, file);
            Assert.True(File.Exists(path), $"{file} is in Exceptions but is not in the test project; remove the entry.");
            Assert.True(File.ReadAllText(path).Contains("DROP DATABASE", StringComparison.OrdinalIgnoreCase),
                $"{file} no longer holds DROP DATABASE; remove it from Exceptions.");
        }
    }

    [Fact]
    public void APooledDrop_IsReported_AndAnUnpooledOrHandedInOneIsNot()
    {
        const string pooledOwn =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        await using var admin = new NpgsqlConnection(cs);\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n}\n";
        const string unpooledOwn =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        await using var admin = new NpgsqlConnection(ScratchPostgres.UnpooledAdminConnectionString(cs));\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n}\n";
        const string explicitOff =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        var b = new NpgsqlConnectionStringBuilder(cs) { Pooling = false };\n"
            + "        await using var admin = new NpgsqlConnection(b.ConnectionString);\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n}\n";
        const string unpooledElsewhereInFile =
            "public class A\n{\n    public async Task Go()\n    {\n"
            + "        await using var admin = new NpgsqlConnection(cs);\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", admin);\n    }\n\n"
            + "    public string Other() => ScratchPostgres.UnpooledAdminConnectionString(cs);\n}\n";
        const string handedIn =
            "public class A\n{\n    private static async Task Drop(NpgsqlConnection c)\n    {\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", c);\n    }\n\n"
            + "    public string Other() => ScratchPostgres.UnpooledAdminConnectionString(cs);\n}\n";
        const string handedInNoEvidence =
            "public class A\n{\n    private static async Task Drop(NpgsqlConnection c)\n    {\n"
            + "        await using var drop = new NpgsqlCommand($\"DROP DATABASE IF EXISTS {n}\", c);\n    }\n}\n";
        const string commentOnly = "public class A\n{\n    // DROP DATABASE x\n    /// DROP DATABASE y\n}\n";

        Assert.Equal(["P.cs:6"], Scan([("P.cs", pooledOwn)]).Pooled);
        Assert.Empty(Scan([("U.cs", unpooledOwn)]).Pooled);
        Assert.Empty(Scan([("E.cs", explicitOff)]).Pooled);
        /* An unrelated unpooled helper elsewhere in the file does not excuse a drop that opens its own pooled connection. */
        Assert.Equal(["F.cs:6"], Scan([("F.cs", unpooledElsewhereInFile)]).Pooled);
        Assert.Empty(Scan([("H.cs", handedIn)]).Pooled);
        Assert.Equal(["N.cs:5"], Scan([("N.cs", handedInNoEvidence)]).Pooled);
        Assert.Equal(0, Scan([("C.cs", commentOnly)]).Sites);
        Assert.Equal(0, Scan([("DarlingCollectorRunnerTests.cs", pooledOwn)]).Sites);
    }

    private static string? FindTestProjectDirectory()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                var source = Path.Combine(directory.FullName, "Darling", "Darling.Tests");
                return Directory.Exists(source) ? source : null;
            }

            directory = directory.Parent;
        }

        return null;
    }
}
