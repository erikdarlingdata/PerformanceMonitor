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
/// <para>Every web test harness that runs the shipped page scripts under Node copies the WHOLE <c>wwwroot/js</c> tree into its
/// scratch folder with one <c>fs.cpSync(jsDir, scratch, { recursive: true })</c>, and none copies a module (or a list of
/// modules, or one directory's worth) by name (#5279). The scan reads every <c>.mjs</c> under <c>Darling/Darling.Tests</c>
/// (<c>WebHarness/load-web-modules.mjs</c> included), not only the files with "harness" in their name, so a helper file
/// cannot dodge the rule by what it is called.</para>
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
/// is allowed only when it is the whole-tree copy of the js folder (recursive, with no <c>filter</c> that would smuggle a list
/// back in), or when its source is already inside the scratch tree (the memory-pressure, perfmon, perfmon-multi,
/// server-trends, waits and waits-activity-trends harnesses copy the real <c>charts.js</c> to <c>charts-real.js</c> before
/// they write a wrapper over <c>charts.js</c>). The js folder is whatever name the harness binds straight from
/// <c>process.argv</c>: the key is where the copy's source comes from, not what it is called. The scratch folder is still
/// recognised by its name, <c>scratch</c>. A harness that makes a scratch folder (<c>mkdtempSync</c>) must contain the
/// whole-tree copy, and every stand-in module it writes into the scratch folder (a <c>.js</c> or <c>.mjs</c> path) must come
/// after EVERY whole-tree copy, because a copy overwrites what is already there. The analysis is text only, so it does not
/// run Node and it does not need the harnesses to be runnable on the machine that builds the tests.</para>
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

    /// <summary>A path argument that names a js module of either extension, as in <c>path.join(scratch, "pages", "server.js")</c>
    /// or <c>path.join(scratch, "stub.mjs")</c>. The <c>package.json</c> marker a harness writes ends in neither, so it is not one.</summary>
    private static readonly Regex ModuleTarget = new(@"\.m?js""\s*\)?\s*$", RegexOptions.Compiled, TimeSpan.FromSeconds(5));

    /// <summary>A plain js name, which is the only kind of copy source that can be a folder the harness took from the command line.</summary>
    private static readonly Regex Identifier = new(@"^[A-Za-z_$][\w$]*$", RegexOptions.Compiled, TimeSpan.FromSeconds(5));

    private static readonly string[] SkippedDirectories = { "bin", "obj", "node_modules" };

    /// <summary>The line a harness starts from: the js folder the test passes in, bound from the command line.</summary>
    private const string JsDirFromArgv = "const jsDir = process.argv[2];\n";

    private const string ScratchFolder = "const scratch = fs.mkdtempSync(path.join(os.tmpdir(), \"x-\"));\n";

    /// <summary>
    /// Every harness copies the whole js tree, and none copies by name. On failure it names each file, the line and the call.
    /// </summary>
    [Fact]
    public void EveryWebHarness_CopiesTheWholeJsTree_AndNoneCopiesByName()
    {
        var harnesses = HarnessFiles(PathTo("Darling", "Darling.Tests")).ToList();
        Assert.True(harnesses.Count > 0, "no .mjs file found under Darling/Darling.Tests, so this pin would read nothing");

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
            "These web test harnesses copy js modules by name or by directory instead of copying the whole tree (#5279). " +
            "A copy list goes stale. A PR cannot edit a list that another PR in the same merge train adds, so two PRs that " +
            "pass alone fail when merged.\n\n" +
            "Copy the whole tree with one call: fs.cpSync(jsDir, scratch, { recursive: true }). " +
            "The first argument is the js folder the test passes in. Bind it from process.argv under any name. " +
            "For example, const [jsDir, scenario] = process.argv.slice(-2). " +
            "Put the call before you write the stand-in modules, so each stand-in still replaces the real file. " +
            "Delete the list.\n\n" + string.Join("\n", offenders));

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
    [InlineData(JsDirFromArgv + "fs.cpSync(jsDir, scratch, { recursive: true, filter: (src) => src.endsWith(\"util.js\") });")]
    [InlineData(JsDirFromArgv + "fs.cpSync(jsDir, scratch);")]
    [InlineData("await fs.promises.copyFile(path.join(jsDir, \"util.js\"), path.join(scratch, \"util.js\"));")]
    /* The key is where the copy's source comes from, not what it is called (#5279): a by-name module copy is flagged under any
       variable name, and a folder that does not come straight from process.argv is not the whole tree. */
    [InlineData("const root = process.argv[2];\nconst from = path.join(root, \"util.js\");\nfs.copyFileSync(from, path.join(scratch, \"util.js\"));")]
    [InlineData(JsDirFromArgv + "const pagesDir = path.join(jsDir, \"pages\");\nfs.cpSync(pagesDir, scratch, { recursive: true });")]
    [InlineData("fs.cpSync(jsDir, scratch, { recursive: true });")]
    [InlineData("const jsDir = path.join(process.argv[2], \"pages\");\nfs.cpSync(jsDir, scratch, { recursive: true });")]
    [InlineData("const [jsDirs, scenario] = process.argv.slice(-2);\nfs.cpSync(jsDir, scratch, { recursive: true });")]
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
            "import fs from \"node:fs\";\n" +
            ScratchFolder +
            "fs.writeFileSync(path.join(scratch, \"util.js\"), fs.readFileSync(path.join(jsDir, \"util.js\")));\n";

        /* Like the by-name copy, it names its line, so a reader can find the scratch folder in a long harness. */
        var violation = Assert.Single(Violations(source));
        Assert.StartsWith("line 2:", violation);
    }

    /// <summary>
    /// A stand-in module written before the whole-tree copy would be overwritten by the real file, so it is flagged. The
    /// <c>package.json</c> marker is not a module and may come first.
    /// </summary>
    [Fact]
    public void AStandInWrittenBeforeTheWholeTreeCopy_IsFlagged()
    {
        const string early =
            JsDirFromArgv + ScratchFolder +
            "fs.writeFileSync(path.join(scratch, \"charts.js\"), \"export const stub = 1;\\n\");\n" +
            "fs.cpSync(jsDir, scratch, { recursive: true });\n";
        const string marker =
            JsDirFromArgv + ScratchFolder +
            "fs.writeFileSync(path.join(scratch, \"package.json\"), \"{}\");\n" +
            "fs.cpSync(jsDir, scratch, { recursive: true });\n";

        Assert.Single(Violations(early));
        Assert.Empty(Violations(marker));
    }

    /// <summary>
    /// The order rule holds against EVERY whole-tree copy that follows a stand-in, not only the first copy in the file. A stand-in
    /// written between two copies is overwritten by the second one (#5279). The chart-at-time harness copies the tree twice, into
    /// <c>scratch/shipped</c> and then into <c>scratch</c>, and is correct because its stand-ins come after both.
    /// </summary>
    [Fact]
    public void AStandInWrittenBetweenTwoWholeTreeCopies_IsFlagged()
    {
        const string between =
            JsDirFromArgv + ScratchFolder +
            "fs.cpSync(jsDir, path.join(scratch, \"shipped\"), { recursive: true });\n" +
            "fs.writeFileSync(path.join(scratch, \"charts.js\"), \"export const stub = 1;\\n\");\n" +
            "fs.cpSync(jsDir, scratch, { recursive: true });\n";
        const string inOrder =
            JsDirFromArgv + ScratchFolder +
            "fs.cpSync(jsDir, path.join(scratch, \"shipped\"), { recursive: true });\n" +
            "fs.cpSync(jsDir, scratch, { recursive: true });\n" +
            "fs.writeFileSync(path.join(scratch, \"charts.js\"), \"export const stub = 1;\\n\");\n";

        Assert.Single(Violations(between));
        Assert.Empty(Violations(inOrder));
    }

    /// <summary>
    /// A stand-in is a module of either extension: an <c>.mjs</c> one is overwritten by a later copy just like a <c>.js</c> one
    /// (#5279). A path that is not a module (the <c>package.json</c> marker) is still free to come first.
    /// </summary>
    [Theory]
    [InlineData("charts.js", 1)]
    [InlineData("stub.mjs", 1)]
    [InlineData("package.json", 0)]
    public void AWriteBeforeTheWholeTreeCopy_IsFlaggedWhenItIsAJsOrMjsModule(string file, int expected)
    {
        var source =
            JsDirFromArgv + ScratchFolder +
            "fs.writeFileSync(path.join(scratch, \"" + file + "\"), \"export const stub = 1;\\n\");\n" +
            "fs.cpSync(jsDir, scratch, { recursive: true });\n";

        Assert.Equal(expected, Violations(source).Count);
    }

    /// <summary>
    /// The whole-tree copy, and a copy whose source is already inside the scratch tree, are the two allowed forms.
    /// </summary>
    [Fact]
    public void TheWholeTreeCopy_AndAnInScratchCopy_AreNotFlagged()
    {
        const string source =
            JsDirFromArgv + ScratchFolder +
            "fs.cpSync(jsDir, scratch, { recursive: true });\n" +
            "fs.copyFileSync(path.join(scratch, \"charts.js\"), path.join(scratch, \"charts-real.js\"));\n" +
            "fs.writeFileSync(path.join(scratch, \"charts.js\"), \"export const stub = 1;\\n\");\n";

        Assert.Empty(Violations(source));
        Assert.True(CopiesWholeTree(source));
    }

    /// <summary>
    /// The whole-tree copy is recognised by where its source comes from, not by what the harness calls it (#5279): a name bound
    /// from <c>process.argv</c> is the js folder the test passes in, whether the harness calls it <c>jsDir</c> or <c>root</c>, and
    /// whichever way it reads the command line.
    /// </summary>
    [Theory]
    [InlineData("const root = process.argv[2];\nfs.cpSync(root, scratch, { recursive: true });")]
    [InlineData("const [root, scenario] = process.argv.slice(-2);\nfs.cpSync(root, scratch, { recursive: true });")]
    [InlineData("let tree = process.argv[2];\nfs.cpSync(tree, scratch, { recursive: true });")]
    [InlineData("const jsDir = path.resolve(process.argv[2]);\nfs.cpSync(jsDir, scratch, { recursive: true });")]
    [InlineData("const jsDir = process.argv[process.argv.length - 1];\nfs.cpSync(jsDir, scratch, { recursive: true });")]
    public void AWholeTreeCopyOfTheFolderTakenFromArgv_IsNotFlagged_UnderAnyName(string copy)
    {
        var source = ScratchFolder + copy + "\n";

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

    /// <summary>
    /// The scan reads every <c>.mjs</c> under the test folder, not only the files with "harness" in their name, so a helper that
    /// copies js into a scratch folder cannot dodge the rule by what it is called (#5279). <c>WebHarness/load-web-modules.mjs</c>
    /// is the file the old name pattern missed. Build output and <c>node_modules</c> stay out of the scan.
    /// </summary>
    [Fact]
    public void TheScan_ReadsEveryMjsUnderTheTestFolder_WhateverItIsCalled()
    {
        var directory = Path.Combine(Path.GetTempPath(), "harness-scan-" + Guid.NewGuid().ToString("N"));
        try
        {
            foreach (var relative in new[]
            {
                "web-x-harness.mjs",
                "WebHarness/load-web-modules.mjs",
                "helpers/copy-tree.mjs",
                "bin/Debug/web-x-harness.mjs",
                "obj/web-y-harness.mjs",
                "node_modules/pkg/index.mjs",
                "notes.txt",
                "web-z-harness.js",
            })
            {
                var file = Path.Combine(directory, relative.Replace('/', Path.DirectorySeparatorChar));
                Directory.CreateDirectory(Path.GetDirectoryName(file)!);
                File.WriteAllText(file, string.Empty);
            }

            var scanned = HarnessFiles(directory)
                .Select(f => Path.GetRelativePath(directory, f).Replace('\\', '/'))
                .OrderBy(f => f, StringComparer.Ordinal)
                .ToArray();

            Assert.Equal(new[] { "WebHarness/load-web-modules.mjs", "helpers/copy-tree.mjs", "web-x-harness.mjs" }, scanned);
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    internal static IEnumerable<string> HarnessFiles(string directory)
    {
        /* Relative to the scanned directory, so the checkout's own location (a worktree under .claude/worktrees included)
           never changes what is skipped. */
        return Directory.EnumerateFiles(directory, "*.mjs", SearchOption.AllDirectories)
            .Where(f => !Path.GetRelativePath(directory, f)
                .Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                .Any(segment => SkippedDirectories.Contains(segment, StringComparer.OrdinalIgnoreCase)))
            .OrderBy(f => f, StringComparer.Ordinal);
    }

    /// <summary>What is wrong with how one harness copies js modules, one entry per problem, each led by its line.</summary>
    internal static List<string> Violations(string source)
    {
        var found = new List<string>();
        var lastTreeCopy = -1;

        foreach (Match call in CopyCall.Matches(source))
        {
            var api = call.Groups["api"].Value;
            var arguments = CallArguments(source, call.Index + call.Length - 1);
            var first = arguments is null ? string.Empty : FirstArgument(arguments);
            var line = LineOf(source, call.Index);

            if (arguments is not null && IsWholeTreeCopy(source, api, first, arguments))
            {
                /* The LAST whole-tree copy is the one every stand-in must follow: a harness that copies twice (into
                   scratch/shipped and into scratch) loses a stand-in written between the two. */
                lastTreeCopy = call.Index;
                continue;
            }

            if (InsideScratch.IsMatch(first))
            {
                continue;
            }

            found.Add($"line {line}: {api}({first}, ...) copies a js module by name or by directory, not the whole tree.");
        }

        if (lastTreeCopy < 0)
        {
            var scratchFolder = source.IndexOf("mkdtempSync(", StringComparison.Ordinal);
            if (scratchFolder >= 0)
            {
                found.Add($"line {LineOf(source, scratchFolder)}: This scratch folder is made with mkdtempSync, but the whole tree is never copied into it.");
            }

            return found;
        }

        /* A stand-in module only replaces the real file when it is written AFTER the copy: the copy overwrites what is there. */
        foreach (Match write in WriteCall.Matches(source))
        {
            if (write.Index >= lastTreeCopy)
            {
                break;
            }

            var arguments = CallArguments(source, write.Index + write.Length - 1);
            var first = arguments is null ? string.Empty : FirstArgument(arguments);
            if (InsideScratch.IsMatch(first) && ModuleTarget.IsMatch(first))
            {
                found.Add($"line {LineOf(source, write.Index)}: This line writes {first} before the whole-tree copy. The copy overwrites it with the real module. Write every stand-in module after the copy.");
            }
        }

        return found;
    }

    private static bool CopiesWholeTree(string source)
    {
        foreach (Match call in CopyCall.Matches(source))
        {
            var arguments = CallArguments(source, call.Index + call.Length - 1);
            if (arguments is not null && IsWholeTreeCopy(source, call.Groups["api"].Value, FirstArgument(arguments), arguments))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsWholeTreeCopy(string source, string api, string firstArgument, string arguments) =>
        api is "cpSync" or "cp"
        && TakenFromArgv(source, firstArgument)
        && RecursiveTrue.IsMatch(arguments)
        && !FilterOption.IsMatch(arguments);

    /// <summary>
    /// True when <paramref name="name"/> is a plain name that the harness binds straight from <c>process.argv</c>, which makes it
    /// the js folder the test hands the harness, whatever the harness calls it. <c>const jsDir = process.argv[2]</c>,
    /// <c>const [root, scenario] = process.argv.slice(-2)</c> and <c>const jsDir = path.resolve(process.argv[2])</c> all qualify.
    /// A folder built from it (<c>path.join(jsDir, "pages")</c>) does not, so a copy of one directory is still a copy by
    /// directory (#5279).
    /// </summary>
    private static bool TakenFromArgv(string source, string name)
    {
        if (!Identifier.IsMatch(name))
        {
            return false;
        }

        var escaped = Regex.Escape(name);
        return Regex.IsMatch(
            source,
            @"\b(?:const|let|var)\s+(?:\[[^\]]*(?<![\w$])" + escaped + @"(?![\w$])[^\]]*\]|" + escaped + @")\s*=\s*(?:path\.resolve\(\s*)?process\.argv\b",
            RegexOptions.None,
            TimeSpan.FromSeconds(5));
    }

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
