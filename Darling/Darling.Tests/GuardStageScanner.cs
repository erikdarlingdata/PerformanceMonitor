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
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using Xunit;
using Xunit.v3;

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

    /// <summary>The attribute a class that reads a Lite file carries, as it is written in source (#5459 cut 3a).</summary>
    internal const string LiteTraitText = "[Trait(\"Reads\", \"Lite\")]";

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
        bool UsesStoreHelper,
        bool ReadsLite = false,
        bool TaggedLite = false)
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
        string OwnCode,
        bool ReadsLite,
        bool TaggedLite);

    /// <summary>
    /// Every test class under <paramref name="testsRoot"/>, one row per class name. When <paramref name="projectFile"/>
    /// is given, the files the project compiles in through a <c>Link</c> are read too: they sit outside the folder
    /// and xunit runs their classes all the same (M1 of #5471's review).
    /// </summary>
    internal static IReadOnlyList<Row> Scan(string testsRoot, string? projectFile = null)
    {
        var files = Directory.EnumerateFiles(testsRoot, "*.cs", SearchOption.AllDirectories)
            .Where(f => !IsBuildOutput(testsRoot, f))
            .OrderBy(f => f, StringComparer.Ordinal)
            .Select(f => (Path: f, Relative: Path.GetRelativePath(testsRoot, f).Replace('\\', '/')))
            .ToList();

        if (projectFile is not null)
        {
            var inFolder = new HashSet<string>(files.Select(f => Path.GetFullPath(f.Path)), StringComparer.OrdinalIgnoreCase);
            files.AddRange(LinkedCompileFiles(projectFile)
                .Where(l => inFolder.Add(Path.GetFullPath(l.Path)))
                .Select(l => (l.Path, l.Link)));
        }

        var decls = new List<Decl>();
        foreach (var file in files)
        {
            var text = File.ReadAllText(file.Path).Replace("\r\n", "\n", StringComparison.Ordinal);
            decls.AddRange(Declarations(text, file.Relative));
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
                UsesStoreHelper: usesHelper,
                ReadsLite: group.Any(d => d.ReadsLite),
                TaggedLite: group.Any(d => d.TaggedLite)));
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
        /* File-level, like Lite.Tests' Darling census: a helper class in the same file may hold the path a test
           class in it reads, so every test class in a file that reads Lite needs the trait. */
        var fileReadsLite = ReadsALitePath(text, code, rootVariables, recursiveOptions);

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
                ownCode,
                fileReadsLite,
                attributes.Any(a => Squash(a).Contains(Squash(LiteTraitText), StringComparison.Ordinal)));
        }
    }

    /// <summary>
    /// Whether a source file reads a file under <c>Lite/</c> or <c>Lite.Tests/</c>: a string literal (not a comment)
    /// naming a path segment <c>Lite</c> (alone, or beside a slash), <c>Lite.Tests</c> or a <c>PerformanceMonitorLite*</c>
    /// project, or a recursive enumeration from the repo root, which reads every Lite file without naming one. The bare
    /// literal <c>"Lite"</c> counts in any position: a label such as <c>("Lite", source)</c> also counts, which only ever
    /// over-tags (the safe direction). A path assembled from a constant another file declares is not visible to a
    /// source scan; the replay of the failure corpus and the nightly are the backstop for that.
    /// </summary>
    private static bool ReadsALitePath(string text, string code, HashSet<string> rootVariables, bool recursiveOptions)
    {
        foreach (var (_, body) in CSharpSourceWalker.StringLiteralBodies(text))
        {
            var normalized = body.Replace('\\', '/');
            var segments = normalized.Split('/', StringSplitOptions.RemoveEmptyEntries);

            if (segments.Any(seg => seg.StartsWith("PerformanceMonitorLite", StringComparison.Ordinal)
                || seg.Equals("Lite.Tests", StringComparison.Ordinal)))
            {
                return true;
            }

            if (body.Equals("Lite", StringComparison.Ordinal)
                || (normalized.Contains('/', StringComparison.Ordinal)
                    && segments.Any(seg => seg.Equals("Lite", StringComparison.Ordinal))))
            {
                return true;
            }
        }

        return EnumeratesRepoRootRecursively(code, rootVariables, recursiveOptions);
    }

    /// <summary>The same question over one source text, for the census's own detector tests.</summary>
    internal static bool ReadsALitePath(string text)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(text);
        return ReadsALitePath(
            text,
            code,
            RepoRootVariables(code),
            Regex.IsMatch(code, @"RecurseSubdirectories\s*=\s*true", RegexOptions.CultureInvariant));
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
                d.Name, d.File, true, d.Tagged, d.LiveCollection, d.EnumeratesRepoRoot, d.ReadsTestVariable, false,
                d.ReadsLite, d.TaggedLite))
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

    /// <summary>One class xunit would run in the guard job, as the loaded assembly says it is.</summary>
    internal sealed record ReflectedClass(string Name, bool LiveCollection);

    /// <summary>
    /// The classes xunit selects for <c>-trait Stage=Guard</c>, read off the test assembly itself (M1 of #5471's
    /// review): an exported, concrete (or static) class with at least one test method, static or instance, public or
    /// not, where the trait sits on the assembly, the class (or a base class, which xunit inherits) or any of its test
    /// methods. That is xunit v3's own discovery (L1 of the second review): <c>GetExportedTypes</c>, a class that is
    /// not abstract or is sealed (a static class is both), and methods bound as Instance, Static, Public, NonPublic.
    /// This is xunit's own idea of "tagged": it sees a method-level trait, a combined attribute list, a trait spelled with constants and a
    /// linked file, which a source scan can miss, so a store-needing class cannot reach the guard job unseen.
    /// </summary>
    internal static IReadOnlyList<ReflectedClass> ReflectGuardClasses(Assembly assembly) =>
        ReflectGuardClasses(
            assembly.GetExportedTypes(),
            HasGuardTrait(assembly.GetCustomAttributes()));

    /// <summary>The same read over the given types, so a test can point it at probe classes xunit does not run.</summary>
    internal static IReadOnlyList<ReflectedClass> ReflectGuardClasses(
        IEnumerable<Type> types, bool assemblyTagged, Func<MethodInfo, bool>? isTest = null)
    {
        isTest ??= m => m.GetCustomAttributes(inherit: true).OfType<IFactAttribute>().Any();
        var result = new List<ReflectedClass>();

        foreach (var type in types.Where(t => t.IsClass && (!t.IsAbstract || t.IsSealed)))
        {
            var tests = type.GetMethods(
                    BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.FlattenHierarchy)
                .Where(isTest)
                .ToList();
            if (tests.Count == 0)
            {
                continue;
            }

            var tagged = assemblyTagged
                || HasGuardTrait(type.GetCustomAttributes(inherit: true).Cast<Attribute>())
                || tests.Any(m => HasGuardTrait(m.GetCustomAttributes(inherit: true).Cast<Attribute>()));
            if (!tagged)
            {
                continue;
            }

            var live = type.GetCustomAttributes(inherit: true).OfType<CollectionAttribute>()
                .Any(c => string.Equals(c.Name, "live-postgres", StringComparison.Ordinal));
            result.Add(new ReflectedClass(type.Name, live));
        }

        return result;
    }

    private static bool HasGuardTrait(IEnumerable<Attribute> attributes) =>
        attributes.OfType<ITraitAttribute>().Any(a => a.GetTraits().Any(
            t => string.Equals(t.Key, "Stage", StringComparison.OrdinalIgnoreCase)
              && string.Equals(t.Value, "Guard", StringComparison.OrdinalIgnoreCase)));

    /// <summary>
    /// Where the source scan and xunit disagree, and rule R3 over the reflected set (M1 of #5471's review). Both
    /// directions fail: a class xunit tags that the scan does not (it could need a store unseen) and a class the
    /// scan reads as tagged that xunit does not run in the guard job (the tag is not doing what the census thinks).
    /// </summary>
    internal static List<string> ReflectionProblems(IReadOnlyList<Row> rows, IReadOnlyList<ReflectedClass> reflected)
    {
        var problems = new List<string>();
        var byName = rows.ToDictionary(r => r.Name, StringComparer.Ordinal);
        var reflectedNames = new HashSet<string>(reflected.Select(r => r.Name), StringComparer.Ordinal);

        foreach (var r in reflected.OrderBy(r => r.Name, StringComparer.Ordinal))
        {
            byName.TryGetValue(r.Name, out var row);

            if (row is null)
            {
                problems.Add($"UNSCANNED {r.Name}: xunit runs it in the guard job but the source scan found no test class of that name "
                  + "(a linked file the csproj does not list, or tests inherited from a base class)");
            }
            else if (!row.Tagged)
            {
                problems.Add($"UNTAGGED {row.File}: {r.Name} is selected by -trait Stage=Guard through a trait the source scan does not read "
                  + "(a method-level trait, a combined attribute list, a constant, a base class); write it as the one attribute line above the class");
            }

            if (r.LiveCollection || (row is not null && row.NeedsAStore && !row.Tagged))
            {
                problems.Add($"STORE {r.Name} is selected by -trait Stage=Guard but needs a store ("
                  + (r.LiveCollection ? "live-postgres collection" : row!.StoreReasons) + "); the guard job has no PostgreSQL");
            }
        }

        foreach (var row in rows.Where(r => r.Tagged && !reflectedNames.Contains(r.Name)).OrderBy(r => r.Name, StringComparer.Ordinal))
        {
            problems.Add($"PHANTOM {row.File}: {row.Name} reads as tagged in source but xunit does not select it for -trait Stage=Guard "
              + "(not public, abstract, or no test method)");
        }

        return problems;
    }

    /// <summary>
    /// The files a project compiles in through an explicit <c>&lt;Compile Include=... Link=...&gt;</c> item, which a
    /// scan of the project's own folder never sees (M1 of #5471's review). Wildcards are not links and are skipped.
    /// </summary>
    internal static IReadOnlyList<(string Path, string Link)> LinkedCompileFiles(string projectFile)
    {
        var projectDirectory = Path.GetDirectoryName(projectFile)
            ?? throw new InvalidOperationException("The project file has no directory.");
        var result = new List<(string, string)>();

        foreach (var item in XDocument.Load(projectFile).Descendants().Where(e => e.Name.LocalName == "Compile"))
        {
            var include = (string?)item.Attribute("Include");
            var link = (string?)item.Attribute("Link");
            if (string.IsNullOrEmpty(include) || string.IsNullOrEmpty(link) || include.Contains('*', StringComparison.Ordinal))
            {
                continue;
            }

            result.Add((
                System.IO.Path.GetFullPath(System.IO.Path.Combine(projectDirectory, include.Replace('\\', System.IO.Path.DirectorySeparatorChar))),
                link.Replace('\\', '/')));
        }

        return result;
    }

    private static bool IsBuildOutput(string testsRoot, string file)
    {
        var relative = Path.GetRelativePath(testsRoot, file);
        return relative.StartsWith("bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
            || relative.StartsWith("obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal);
    }
}
