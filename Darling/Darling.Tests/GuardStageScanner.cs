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
using System.Text;
using System.Text.RegularExpressions;

namespace Darling.Tests;

/// <summary>
/// What a test class IS, read off its source, for the guard-stage census of each test suite (#5459).
///
/// <para><b>Why a scanner.</b> build.yml's first job on a pull request is <c>guard-tests</c>: it runs only the
/// classes carrying <c>[Trait("Stage", "Guard")]</c>, so a source pin, a census or a hygiene scan fails in about
/// ten minutes instead of after every shard has run. A guard class that is not tagged loses that fast failure
/// and nothing else, and nobody would notice, so the tagging is derived from the source the way
/// <c>DarlingReadsTraitGuardTests</c> derives its trait: a class whose NAME says it is a guard, or that
/// enumerates the whole repository, must carry the trait or be on a census exemption list.</para>
///
/// <para><b>Linked, not copied.</b> Lite.Tests compiles this file into its own assembly (the same arrangement as
/// the source walker it builds on), so both suites ask the same questions of their own sources. Editing it
/// therefore sends a pull request to the whole Lite suite, which build.yml's <c>lite_linked_shard</c> filter
/// pins.</para>
///
/// <para><b>What is read.</b> Comments and string literals are ignored for every code question (the source
/// walker blanks them); a string literal is read only for the one question that is ABOUT a literal, the name of
/// an environment variable. A path assembled in a helper another class owns is not visible to a source scan, and
/// that is the limit of the whole approach.</para>
/// </summary>
internal static class GuardStageScanner
{
    /// <summary>The attribute a guard class carries, as it is written in source.</summary>
    internal const string GuardTraitText = "[Trait(\"Stage\", \"Guard\")]";

    /// <summary>The name shapes the census treats as guard-style (ruling 2, R1).</summary>
    internal static readonly Regex GuardStyleName = new(
        @"(?:Hygiene|Census|Convention|Adoption|Ratchet|Guard|SourcePin|Pins?|Drift|Inventory)Tests$",
        RegexOptions.CultureInvariant);

    /* Indented as well as column 0, so a class inside a block namespace or nested in another class is seen. */
    private static readonly Regex s_classDeclaration = new(
        @"^[ \t]*(?:(?:public|internal|private|protected|sealed|static|abstract|partial)\s+)*class\s+(?<name>\w+)",
        RegexOptions.Multiline | RegexOptions.CultureInvariant);

    private static readonly Regex s_enumerationCall = new(
        @"\b(?:EnumerateFiles|EnumerateFileSystemEntries|EnumerateDirectories|GetFiles|GetFileSystemEntries|GetDirectories)\s*\(",
        RegexOptions.CultureInvariant);

    /* A call that answers the repo root: RepoRoot(), FindRepoRoot(), RepoRootOrFail(), ParitySource.RepoRoot(),
       ResolveRoot(), FindRoot(), or the shared RepoFile.Root property (a class's own Root alias for it included). Every
       test class that walks up from the build output spells it one of these. */
    private static readonly Regex s_repoRootExpression = new(
        @"^(?:\w+\.)*\w*(?:RepoRoot|FindRoot|ResolveRoot)\w*\(\s*\)$|^(?:RepoFile\.)?Root$", RegexOptions.CultureInvariant);

    private static readonly Regex s_testAttribute = new(@"\[(?:Fact|Theory)\b", RegexOptions.CultureInvariant);

    private static readonly Regex s_testVariable = new(@"^DARLING_TEST_\w+$", RegexOptions.CultureInvariant);

    /// <summary>One test class, folded over every file its partial parts sit in.</summary>
    internal sealed record Row(
        string Name,
        string File,
        bool IsTest,
        bool Tagged,
        bool LiveCollection,
        bool EnumeratesRepoRoot,
        bool ReadsTestVariable,
        bool UsesStoreHelper)
    {
        /// <summary>Whether the class is guard-style by name (R1).</summary>
        internal bool GuardStyleByName => GuardStyleName.IsMatch(Name);

        /// <summary>Whether the class is a guard candidate by either rule (R1 or R2).</summary>
        internal bool Candidate => IsTest && (GuardStyleByName || EnumeratesRepoRoot);

        /// <summary>Whether tagging the class would break rule R3: the guard job has no store.</summary>
        internal bool NeedsAStore => LiveCollection || ReadsTestVariable || UsesStoreHelper;

        /// <summary>The R3 reasons, in words, for a failure message or an exemption line.</summary>
        internal string StoreReasons => string.Join(
            "+",
            new[]
            {
                LiveCollection ? "live-postgres-collection" : null,
                ReadsTestVariable ? "reads-DARLING_TEST-variable" : null,
                UsesStoreHelper ? "uses-a-store-helper" : null,
            }.Where(r => r is not null));
    }

    private sealed record Decl(
        string Name,
        string File,
        bool IsTest,
        bool Tagged,
        bool LiveCollection,
        bool EnumeratesRepoRoot,
        bool ReadsTestVariable,
        string OwnCode);

    /// <summary>Every test class under <paramref name="testsRoot"/>, one row per class name.</summary>
    internal static IReadOnlyList<Row> Scan(string testsRoot)
    {
        var files = Directory.EnumerateFiles(testsRoot, "*.cs", SearchOption.AllDirectories)
            .Where(f => !IsBuildOutput(testsRoot, f))
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToList();

        var decls = new List<Decl>();
        foreach (var file in files)
        {
            var text = File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal);
            decls.AddRange(Declarations(text, Path.GetRelativePath(testsRoot, file).Replace('\\', '/')));
        }

        /* A type that is not a test class but names a DARLING_TEST_ variable is a store helper, and so is a type
           that refers to one: a test reaching the store through a helper never writes the variable itself. */
        var helpers = new HashSet<string>(
            decls.Where(d => !d.IsTest && d.ReadsTestVariable).Select(d => d.Name), StringComparer.Ordinal);
        for (var grew = true; grew;)
        {
            grew = false;
            foreach (var d in decls.Where(d => !d.IsTest && !helpers.Contains(d.Name)))
            {
                if (helpers.Any(h => Regex.IsMatch(d.OwnCode, @"\b" + Regex.Escape(h) + @"\b")))
                {
                    helpers.Add(d.Name);
                    grew = true;
                }
            }
        }

        var rows = new List<Row>();
        foreach (var group in decls.Where(d => d.IsTest).GroupBy(d => d.Name, StringComparer.Ordinal))
        {
            var usesHelper = group.Any(
                d => helpers.Any(h => h != d.Name && Regex.IsMatch(d.OwnCode, @"\b" + Regex.Escape(h) + @"\b")));
            rows.Add(new Row(
                group.Key,
                group.First().File,
                IsTest: true,
                Tagged: group.Any(d => d.Tagged),
                LiveCollection: group.Any(d => d.LiveCollection),
                EnumeratesRepoRoot: group.Any(d => d.EnumeratesRepoRoot),
                ReadsTestVariable: group.Any(d => d.ReadsTestVariable),
                UsesStoreHelper: usesHelper));
        }

        return rows;
    }

    /// <summary>
    /// What is wrong with the suite's tagging, one line per fault; empty when it is right. Rules R1 and R2: a
    /// guard candidate carries the trait or is exempt. Rule R3: a tagged class needs no store. And an exemption
    /// must still be one: it names a candidate that is untagged, with a reason.
    /// </summary>
    internal static List<string> Problems(IReadOnlyList<Row> rows, IReadOnlyDictionary<string, string> exempt)
    {
        var problems = new List<string>();
        var byName = rows.ToDictionary(r => r.Name, StringComparer.Ordinal);

        foreach (var r in rows.Where(r => r.Candidate && !r.Tagged && !exempt.ContainsKey(r.Name)).OrderBy(r => r.Name, StringComparer.Ordinal))
        {
            var why = r.GuardStyleByName ? "guard-style name" : "enumerates the repo root recursively";
            problems.Add($"MISSING {r.File}: {r.Name} ({why}; store: {(r.NeedsAStore ? r.StoreReasons : "none")})");
        }

        foreach (var r in rows.Where(r => r.Tagged && r.NeedsAStore).OrderBy(r => r.Name, StringComparer.Ordinal))
        {
            problems.Add($"STORE {r.File}: {r.Name} is tagged Stage=Guard but {r.StoreReasons}; the guard job has no PostgreSQL");
        }

        foreach (var (name, reason) in exempt)
        {
            if (string.IsNullOrWhiteSpace(reason))
            {
                problems.Add($"REASON {name}: an exemption needs a reason");
            }
            else if (!byName.TryGetValue(name, out var row) || !row.Candidate)
            {
                problems.Add($"STALE {name}: is exempt but is not a guard candidate (renamed, deleted or no longer matches); drop the entry");
            }
            else if (row.Tagged)
            {
                problems.Add($"STALE {name}: is exempt but now carries the trait; drop the entry");
            }
        }

        return problems;
    }

    /// <summary>The class declarations of one file (top-level, in a block namespace, or nested), each judged on
    /// its OWN body: a nested class does not take its enclosing class's tests, trait or reads.</summary>
    private static IEnumerable<Decl> Declarations(string text, string relativeFile)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(text);
        var found = new List<(string Name, int Index, int Open, int End)>();

        foreach (Match m in s_classDeclaration.Matches(code))
        {
            var open = code.IndexOf('{', m.Index);
            if (open < 0)
            {
                continue;
            }

            found.Add((m.Groups["name"].Value, m.Index, open, open + CSharpSourceWalker.BraceBalanced(code, open).Length));
        }

        if (found.Count == 0)
        {
            yield break;
        }

        var rootVariables = RepoRootVariables(code);
        var recursiveOptions = Regex.IsMatch(code, @"RecurseSubdirectories\s*=\s*true", RegexOptions.CultureInvariant);
        var literals = CSharpSourceWalker.StringLiteralBodies(text).ToList();

        foreach (var d in found)
        {
            var own = new StringBuilder(code[d.Open..d.End]);
            foreach (var nested in found.Where(n => n.Index > d.Open && n.End <= d.End))
            {
                for (var i = nested.Index - d.Open; i < nested.End - d.Open; i++)
                {
                    own[i] = ' ';
                }
            }

            var ownCode = own.ToString();
            var readsVariable = literals.Any(l =>
                l.Start >= d.Open && l.Start < d.End
                && !found.Any(n => n.Index > d.Open && n.End <= d.End && l.Start >= n.Open && l.Start < n.End)
                && s_testVariable.IsMatch(l.Text.Trim()));

            var attributes = AttributeLinesAbove(text, d.Index);

            yield return new Decl(
                d.Name,
                relativeFile,
                s_testAttribute.IsMatch(ownCode),
                attributes.Any(a => Squash(a).Contains(Squash(GuardTraitText), StringComparison.Ordinal)),
                attributes.Any(a => Squash(a).Contains("Collection(\"live-postgres\")", StringComparison.Ordinal)),
                EnumeratesRepoRootRecursively(ownCode, rootVariables, recursiveOptions),
                readsVariable,
                ownCode);
        }
    }

    private static string Squash(string s) => s.Replace(" ", string.Empty, StringComparison.Ordinal);

    /// <summary>The attribute lines directly above a declaration, up to the first line that is not one.</summary>
    private static List<string> AttributeLinesAbove(string text, int declarationIndex)
    {
        var result = new List<string>();
        var lines = text[..declarationIndex].Split('\n');

        for (var i = lines.Length - 2; i >= 0; i--)
        {
            var line = lines[i].Trim();
            if (!line.StartsWith('['))
            {
                break;
            }

            result.Add(line);
        }

        return result;
    }

    /// <summary>The names a file assigns from a repo-root call (a local or a field).</summary>
    private static HashSet<string> RepoRootVariables(string code)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(code, @"\b(?<name>\w+)\s*(?:=>|=)\s*(?<rhs>[^;=]+);", RegexOptions.CultureInvariant))
        {
            if (s_repoRootExpression.IsMatch(m.Groups["rhs"].Value.Trim()))
            {
                names.Add(m.Groups["name"].Value);
            }
        }

        return names;
    }

    /// <summary>Whether a call such as <c>Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories)</c>
    /// starts at the repo root: the first argument is a repo-root call or a variable the file sets from one, and
    /// the arguments say <c>AllDirectories</c> or pass an options value while the file sets
    /// <c>RecurseSubdirectories = true</c>.</summary>
    internal static bool EnumeratesRepoRootRecursively(string code, HashSet<string> rootVariables, bool recursiveOptions)
    {
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

    /// <summary>The same question over one class's source text, for the census's own detector tests.</summary>
    internal static bool EnumeratesRepoRootRecursively(string text)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(text);
        return EnumeratesRepoRootRecursively(
            code,
            RepoRootVariables(code),
            Regex.IsMatch(code, @"RecurseSubdirectories\s*=\s*true", RegexOptions.CultureInvariant));
    }

    /// <summary>The scan of a single in-memory source text, for the census's own detector tests.</summary>
    internal static IReadOnlyList<Row> ScanText(string text)
    {
        var decls = Declarations(text, "Synthetic.cs").Where(d => d.IsTest).ToList();
        return decls
            .Select(d => new Row(
                d.Name, d.File, true, d.Tagged, d.LiveCollection, d.EnumeratesRepoRoot, d.ReadsTestVariable, false))
            .ToList();
    }

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

    private static bool IsBuildOutput(string testsRoot, string file)
    {
        var relative = Path.GetRelativePath(testsRoot, file);
        return relative.StartsWith("bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
            || relative.StartsWith("obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal);
    }
}
