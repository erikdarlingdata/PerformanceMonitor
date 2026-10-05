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
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// <para>Every web test harness (<c>Darling/Darling.Tests/*harness*.mjs</c>) that runs the shipped page scripts under Node
/// copies the WHOLE <c>wwwroot/js</c> tree into its scratch folder with one <c>fs.cpSync(jsDir, scratch, { recursive: true })</c>,
/// and none copies a module (or a list of modules, or one directory's worth) by name (#5279).</para>
///
/// <para><b>Why a hand-kept list cannot stay correct.</b> Adding one file under <c>wwwroot/js/pages/</c> meant editing every
/// harness's copy list, and a PR cannot edit a list that another PR in the same merge train adds. Two PRs that were each
/// green alone then went red when merged, in test classes that look unrelated to harnesses: <c>server-tabs.js</c> imported
/// a new page module and a harness that never copied it failed at import, so all of that harness's tests failed. It broke a
/// train's build, and the harness behind <c>ComposeDataFloorLiveTests.TryRender</c> was a list nobody counted. Copying the
/// whole tree is harmless because only imported modules load, and each harness writes its stand-in modules AFTER the copy,
/// so a stand-in still replaces the real file.</para>
///
/// <para><b>The rule, as text.</b> A copy call (<c>copyFileSync</c>, <c>copyFile</c>, <c>cpSync</c> or <c>fs.cp</c>) in a harness
/// is allowed only when it is the whole-tree copy of <c>jsDir</c> (recursive, with no <c>filter</c> that would smuggle a list
/// back in), or when its source is already inside the scratch tree (the perfmon, waits and server-trends harnesses copy
/// the real <c>charts.js</c> to <c>charts-real.js</c> before they write a wrapper over <c>charts.js</c>). A harness that makes a scratch folder
/// (<c>mkdtempSync</c>) must contain the whole-tree copy, and every stand-in module it writes into the scratch folder must come
/// after that copy, because the copy overwrites what is already there. The analysis is text only, so it does not run Node and
/// it does not need the harnesses to be runnable on the machine that builds the tests.</para>
/// </summary>
public sealed class WebHarnessJsTreeCopyTests
{
    /// <summary>The copy calls that put a js module into a scratch folder. <c>cp</c> is matched only as <c>fs.cp</c> or
    /// <c>fs.promises.cp</c>, so an unrelated function that happens to be called <c>cp</c> is not a copy call.</summary>
    private static readonly Regex CopyCall = new(
        @"\b(?<api>copyFileSync|copyFile|cpSync)\s*\(|\bf(?:s|sp)\.(?:promises\.)?(?<api>cp)\s*\(",
        RegexOptions.Compiled, TimeSpan.FromSeconds(5));

    private static readonly Regex RecursiveTrue = new(@"\brecursive\s*:\s*true\b", RegexOptions.Compiled, TimeSpan.FromSeconds(5));

    private static readonly Regex FilterOption = new(@"\bfilter\b", RegexOptions.Compiled, TimeSpan.FromSeconds(5));

    private static readonly Regex InsideScratch = new(@"\bscratch\b", RegexOptions.Compiled, TimeSpan.FromSeconds(5));

    private static readonly Regex WriteCall = new(@"\bwriteFileSync\s*\(", RegexOptions.Compiled, TimeSpan.FromSeconds(5));

    /// <summary>A path argument that names a js module, as in <c>path.join(scratch, "pages", "server.js")</c>. The
    /// <c>package.json</c> marker a harness writes does not end in <c>.js"</c>, so it is not one.</summary>
    private static readonly Regex ModuleTarget = new(@"\.js""\s*\)?\s*$", RegexOptions.Compiled, TimeSpan.FromSeconds(5));

    private static readonly string[] SkippedDirectories = { "bin", "obj", "node_modules" };

    /// <summary>
    /// Every harness copies the whole js tree, and none copies by name. On failure it names each file, the line and the call.
    /// </summary>
    [Fact]
    public void EveryWebHarness_CopiesTheWholeJsTree_AndNoneCopiesByName()
    {
        var harnesses = HarnessFiles().ToList();
        Assert.True(harnesses.Count > 0, "no *harness*.mjs found under Darling/Darling.Tests, so this pin would read nothing");

        var offenders = new List<string>();
        var copyingTheTree = 0;
        foreach (var file in harnesses)
        {
            var source = File.ReadAllText(file);
            var relative = Path.GetRelativePath(Root, file).Replace('\\', '/');
            offenders.AddRange(Violations(source).Select(v => relative + ": " + v));

            if (CopiesWholeTree(source))
            {
                copyingTheTree++;
            }
        }

        Assert.True(offenders.Count == 0,
            "These web test harnesses copy js modules piecemeal instead of copying the whole tree (#5279). A hand-kept list " +
            "needs an edit every time a PR adds a module under wwwroot/js/pages/, and a PR cannot edit a list another PR in the " +
            "same merge train adds, so two PRs that are each green go red when merged. Replace the list with one " +
            "fs.cpSync(jsDir, scratch, { recursive: true }) BEFORE the stand-in modules are written (so each stand-in still " +
            "replaces the real file) and delete the list.\n\n" + string.Join("\n", offenders));

        /* Not vacuous: the analysis has to recognise the allowed form in at least one real harness, or it would pass a tree
           whose every harness it simply failed to read. */
        Assert.True(copyingTheTree > 0, "no harness was recognised as copying the whole js tree, so this pin cannot be trusted");
    }

    /// <summary>
    /// The analysis flags each way a harness can copy by name, so a regression to a hand-kept list cannot read as clean.
    /// </summary>
    [Theory]
    [InlineData("fs.copyFileSync(path.join(jsDir, \"util.js\"), path.join(scratch, \"util.js\"));")]
    [InlineData("for (const f of [\"util.js\", \"panels.js\"]) fs.copyFileSync(path.join(jsDir, f), path.join(scratch, f));")]
    [InlineData("for (const f of fs.readdirSync(jsDir)) if (f.endsWith(\".js\")) fs.copyFileSync(path.join(jsDir, f), path.join(scratch, f));")]
    [InlineData("const from = path.join(jsDir, rel);\nfs.copyFileSync(from, path.join(scratch, rel));")]
    [InlineData("fs.cpSync(path.join(jsDir, \"pages\"), path.join(scratch, \"pages\"), { recursive: true });")]
    [InlineData("fs.cpSync(jsDir, scratch, { recursive: true, filter: (src) => src.endsWith(\"util.js\") });")]
    [InlineData("fs.cpSync(jsDir, scratch);")]
    [InlineData("await fs.promises.copyFile(path.join(jsDir, \"util.js\"), path.join(scratch, \"util.js\"));")]
    public void APiecemealCopy_IsFlagged(string snippet)
    {
        Assert.NotEmpty(Violations(snippet));
    }

    /// <summary>
    /// A harness that builds a scratch folder without ever making the whole-tree copy is flagged even when it calls no copy
    /// function at all (it might write each module with <c>writeFileSync</c>).
    /// </summary>
    [Fact]
    public void AScratchFolderWithoutTheWholeTreeCopy_IsFlagged()
    {
        const string source =
            "const scratch = fs.mkdtempSync(path.join(os.tmpdir(), \"x-\"));\n" +
            "fs.writeFileSync(path.join(scratch, \"util.js\"), fs.readFileSync(path.join(jsDir, \"util.js\")));\n";

        Assert.Single(Violations(source));
    }

    /// <summary>
    /// A stand-in module written before the whole-tree copy would be overwritten by the real file, so it is flagged. The
    /// <c>package.json</c> marker is not a module and may come first.
    /// </summary>
    [Fact]
    public void AStandInWrittenBeforeTheWholeTreeCopy_IsFlagged()
    {
        const string early =
            "const scratch = fs.mkdtempSync(path.join(os.tmpdir(), \"x-\"));\n" +
            "fs.writeFileSync(path.join(scratch, \"charts.js\"), \"export const stub = 1;\\n\");\n" +
            "fs.cpSync(jsDir, scratch, { recursive: true });\n";
        const string marker =
            "const scratch = fs.mkdtempSync(path.join(os.tmpdir(), \"x-\"));\n" +
            "fs.writeFileSync(path.join(scratch, \"package.json\"), \"{}\");\n" +
            "fs.cpSync(jsDir, scratch, { recursive: true });\n";

        Assert.Single(Violations(early));
        Assert.Empty(Violations(marker));
    }

    /// <summary>
    /// The whole-tree copy, and a copy whose source is already inside the scratch tree, are the two allowed forms.
    /// </summary>
    [Fact]
    public void TheWholeTreeCopy_AndAnInScratchCopy_AreNotFlagged()
    {
        const string source =
            "const scratch = fs.mkdtempSync(path.join(os.tmpdir(), \"x-\"));\n" +
            "fs.cpSync(jsDir, scratch, { recursive: true });\n" +
            "fs.copyFileSync(path.join(scratch, \"charts.js\"), path.join(scratch, \"charts-real.js\"));\n" +
            "fs.writeFileSync(path.join(scratch, \"charts.js\"), \"export const stub = 1;\\n\");\n";

        Assert.Empty(Violations(source));
        Assert.True(CopiesWholeTree(source));
    }

    /// <summary>
    /// A harness that imports the shipped scripts in place, with no scratch folder and no copy, has nothing to flag.
    /// </summary>
    [Fact]
    public void AHarnessThatCopiesNothing_IsNotFlagged()
    {
        const string source = "const root = process.argv[2];\nawait import(pathToFileURL(path.join(root, \"app.js\")).href);\n";

        Assert.Empty(Violations(source));
        Assert.False(CopiesWholeTree(source));
    }

    private static IEnumerable<string> HarnessFiles()
    {
        /* Relative to the scanned directory, so the checkout's own location (a worktree under .claude/worktrees included)
           never changes what is skipped. */
        var directory = PathTo("Darling", "Darling.Tests");
        return Directory.EnumerateFiles(directory, "*harness*.mjs", SearchOption.AllDirectories)
            .Where(f => !Path.GetRelativePath(directory, f)
                .Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                .Any(segment => SkippedDirectories.Contains(segment, StringComparer.OrdinalIgnoreCase)))
            .OrderBy(f => f, StringComparer.Ordinal);
    }

    /// <summary>What is wrong with how one harness copies js modules, one entry per problem, each led by its line.</summary>
    internal static List<string> Violations(string source)
    {
        var found = new List<string>();
        var firstTreeCopy = -1;

        foreach (Match call in CopyCall.Matches(source))
        {
            var api = call.Groups["api"].Value;
            var arguments = CallArguments(source, call.Index + call.Length - 1);
            var first = arguments is null ? string.Empty : FirstArgument(arguments);
            var line = LineOf(source, call.Index);

            if (arguments is not null && IsWholeTreeCopy(api, first, arguments))
            {
                if (firstTreeCopy < 0)
                {
                    firstTreeCopy = call.Index;
                }

                continue;
            }

            if (InsideScratch.IsMatch(first))
            {
                continue;
            }

            found.Add($"line {line}: {api}({first}, ...) copies from the js tree by name or by directory, not the whole tree");
        }

        if (firstTreeCopy < 0)
        {
            if (source.Contains("mkdtempSync(", StringComparison.Ordinal))
            {
                found.Add("makes a scratch folder (mkdtempSync) but never copies the whole tree with fs.cpSync(jsDir, ..., { recursive: true })");
            }

            return found;
        }

        /* A stand-in module only replaces the real file when it is written AFTER the copy: the copy overwrites what is there. */
        foreach (Match write in WriteCall.Matches(source))
        {
            if (write.Index >= firstTreeCopy)
            {
                break;
            }

            var arguments = CallArguments(source, write.Index + write.Length - 1);
            var first = arguments is null ? string.Empty : FirstArgument(arguments);
            if (InsideScratch.IsMatch(first) && ModuleTarget.IsMatch(first))
            {
                found.Add($"line {LineOf(source, write.Index)}: writes {first} before the whole-tree copy, which would overwrite it with the real module; write every stand-in module after the copy");
            }
        }

        return found;
    }

    private static bool CopiesWholeTree(string source)
    {
        foreach (Match call in CopyCall.Matches(source))
        {
            var arguments = CallArguments(source, call.Index + call.Length - 1);
            if (arguments is not null && IsWholeTreeCopy(call.Groups["api"].Value, FirstArgument(arguments), arguments))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsWholeTreeCopy(string api, string firstArgument, string arguments) =>
        api is "cpSync" or "cp"
        && firstArgument == "jsDir"
        && RecursiveTrue.IsMatch(arguments)
        && !FilterOption.IsMatch(arguments);

    /// <summary>The text between the parentheses of the call whose opening parenthesis is at <paramref name="open"/>,
    /// skipping over string literals, or null when the call never closes.</summary>
    private static string? CallArguments(string source, int open)
    {
        var depth = 0;
        var quote = '\0';
        for (var i = open; i < source.Length; i++)
        {
            var c = source[i];
            if (quote != '\0')
            {
                if (c == '\\')
                {
                    i++;
                }
                else if (c == quote)
                {
                    quote = '\0';
                }

                continue;
            }

            if (c is '"' or '\'' or '`')
            {
                quote = c;
            }
            else if (c == '(')
            {
                depth++;
            }
            else if (c == ')' && --depth == 0)
            {
                return source.Substring(open + 1, i - open - 1);
            }
        }

        return null;
    }

    /// <summary>The first top-level argument of a call's argument text, trimmed.</summary>
    private static string FirstArgument(string arguments)
    {
        var depth = 0;
        var quote = '\0';
        for (var i = 0; i < arguments.Length; i++)
        {
            var c = arguments[i];
            if (quote != '\0')
            {
                if (c == '\\')
                {
                    i++;
                }
                else if (c == quote)
                {
                    quote = '\0';
                }

                continue;
            }

            if (c is '"' or '\'' or '`')
            {
                quote = c;
            }
            else if (c is '(' or '[' or '{')
            {
                depth++;
            }
            else if (c is ')' or ']' or '}')
            {
                depth--;
            }
            else if (c == ',' && depth == 0)
            {
                return arguments[..i].Trim();
            }
        }

        return arguments.Trim();
    }

    private static int LineOf(string source, int index)
    {
        var line = 1;
        for (var i = 0; i < index && i < source.Length; i++)
        {
            if (source[i] == '\n')
            {
                line++;
            }
        }

        return line;
    }
}
