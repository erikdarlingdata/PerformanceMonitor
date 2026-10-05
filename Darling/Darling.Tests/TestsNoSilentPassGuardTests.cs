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
/// No <c>[Fact]</c> or <c>[Theory]</c> in either test project may return early from a <c>catch</c> block.
///
/// <para>A test that does <c>try { setup(); } catch { return; }</c> is reported as a PASS, not a skip. On any
/// machine where the setup cannot run it asserts nothing and still reads as coverage: a Windows run of a
/// symlink test reported "0 skipped" for exactly this reason, when creating a symbolic link needs a privilege
/// an ordinary session lacks. The honest forms are <c>Assert.Skip(reason)</c> (or <c>Assert.SkipUnless</c>)
/// when the setup can legitimately be impossible, and letting the exception fail the test when it should
/// always work.</para>
///
/// <para>The scan runs over <see cref="CSharpSourceWalker.StripCommentsAndStrings"/> output, so prose and
/// literals cannot match. A <c>return;</c> in a catch of a helper that is not itself a test (a listener
/// shutdown loop, say) is not flagged. There is no exemption list: none was needed.</para>
/// </summary>
public sealed class TestsNoSilentPassGuardTests
{
    private static readonly Regex TestAttribute = new(@"\[\s*(?:Fact|Theory)\b", RegexOptions.Compiled);
    private static readonly Regex CatchKeyword = new(@"\bcatch\b", RegexOptions.Compiled);
    private static readonly Regex BareReturn = new(@"\breturn\s*;", RegexOptions.Compiled);

    /// <summary>The line (1-based) of every bare <c>return;</c> inside a <c>catch</c> block of a test method.</summary>
    internal static List<int> SilentPassLines(string text)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(text);
        var lines = new List<int>();

        foreach (Match attribute in TestAttribute.Matches(code))
        {
            var brace = code.IndexOf('{', attribute.Index);
            var arrow = code.IndexOf("=>", attribute.Index, StringComparison.Ordinal);
            if (brace < 0 || (arrow >= 0 && arrow < brace))
            {
                continue;
            }

            var body = CSharpSourceWalker.BraceBalanced(code, brace);

            foreach (Match handler in CatchKeyword.Matches(body))
            {
                var open = body.IndexOf('{', handler.Index);
                if (open < 0)
                {
                    continue;
                }

                var block = CSharpSourceWalker.BraceBalanced(body, open);
                foreach (Match ret in BareReturn.Matches(block))
                {
                    var offset = brace + open + ret.Index;
                    lines.Add(code.Take(offset).Count(c => c == '\n') + 1);
                }
            }
        }

        return lines.Distinct().ToList();
    }

    [Fact]
    public void NoTestInEitherProject_ReturnsEarlyFromACatchBlock()
    {
        var offenders = new List<string>();
        var scanned = 0;

        foreach (var project in new[] { "Darling/Darling.Tests", "Lite.Tests" })
        {
            var root = RepoFile.PathTo(project);
            Assert.True(Directory.Exists(root), $"test project folder not found: {root}");

            foreach (var file in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
            {
                var relative = Path.GetRelativePath(RepoFile.Root, file);
                var parts = relative.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
                if (parts.Contains("bin") || parts.Contains("obj"))
                {
                    continue;
                }

                scanned++;
                foreach (var line in SilentPassLines(File.ReadAllText(file)))
                {
                    offenders.Add($"{relative.Replace('\\', '/')}:{line}");
                }
            }
        }

        Assert.True(scanned > 100, $"the scan found only {scanned} files, so it is reading the wrong place");
        Assert.True(
            offenders.Count == 0,
            "A test that returns from a catch block is reported as a PASS, not a skip, so on a machine where its "
            + "setup cannot run it asserts nothing and still reads as coverage. Replace the return with "
            + "Assert.Skip(reason) (or Assert.SkipUnless) when the setup can legitimately be impossible, or let "
            + "the exception fail the test. Offenders: " + string.Join(", ", offenders));
    }

    [Fact]
    public void TheScan_FlagsABareReturnInACatch_AndNothingElse()
    {
        const string flagged = "public class T\n{\n    [Fact]\n    public void A()\n    {\n        try { X(); }\n        catch (Exception)\n        {\n            return;\n        }\n    }\n}\n";
        Assert.Equal(new[] { 9 }, SilentPassLines(flagged));

        const string notATest = "public class T\n{\n    private static void Helper()\n    {\n        try { X(); }\n        catch (Exception) { return; }\n    }\n}\n";
        Assert.Empty(SilentPassLines(notATest));

        const string explicitSkip = "public class T\n{\n    [Fact]\n    public void A()\n    {\n        try { X(); }\n        catch (Exception ex) { Assert.Skip(ex.Message); }\n        return;\n    }\n}\n";
        Assert.Empty(SilentPassLines(explicitSkip));
    }
}
