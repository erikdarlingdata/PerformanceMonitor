/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// build.yml's <c>darling</c> path filter is the <c>darling_full</c> filter plus Lite paths only.
///
/// <para>#5961 removed this class's trait roster: the Darling.Tests classes that read a Lite path are no longer tagged
/// <c>[Trait("Reads", "Lite")]</c> and checked against a source scan. The runners run <c>-class</c> lists that
/// <c>.github/scripts/ci-select.py reads-classes</c> derives from the source, so there is nothing to keep in step. What
/// stays is this check of the filter relation, because a gap in it narrows the run for an edit that is not a Lite file.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class LiteReadsTraitGuardTests
{
    /// <summary>
    /// The build job's <c>darling</c> filter is the union of two things: the paths that run the WHOLE suite
    /// (<c>darling_full</c>) and the Lite paths that run only the Guard and Lite-reading classes. A path added to
    /// <c>darling</c> and not to <c>darling_full</c> would silently narrow the run for an edit that is not a Lite file.
    /// </summary>
    [Fact]
    public void TheDarlingFilter_IsTheFullSuiteFilter_PlusLitePathsOnly()
    {
        var yaml = File.ReadAllText(Path.Combine(RepoFile.Root, ".github", "workflows", "build.yml"))
            .Replace("\r\n", "\n", StringComparison.Ordinal);

        var all = FilterEntries(yaml, "darling");
        var full = FilterEntries(yaml, "darling_full");

        Assert.NotEmpty(full);
        Assert.Contains("Darling/**/!(*.md)", full);
        Assert.Empty(full.Except(all));

        var narrowed = all.Except(full).ToList();
        Assert.NotEmpty(narrowed);
        var notLite = narrowed.Where(e => !e.StartsWith("Lite/", StringComparison.Ordinal) && !e.StartsWith("Lite.Tests/", StringComparison.Ordinal)).ToList();
        Assert.True(
            notLite.Count == 0,
            "These entries are in build.yml's 'darling' filter but not in 'darling_full', so a diff that reaches only "
          + "them would run just the Guard and Lite-reading classes. Only Lite paths may be narrowed; add the others to "
          + "'darling_full':\n  " + string.Join("\n  ", notLite));
    }

    /// <summary>The quoted patterns of one filter in the build job's <c>filters: |</c> block (12-space keys).</summary>
    private static System.Collections.Generic.List<string> FilterEntries(string yaml, string name)
    {
        var at = yaml.IndexOf("\n            " + name + ":\n", StringComparison.Ordinal);
        Assert.True(at > 0, $"build.yml's '{name}' path filter is gone");
        var rest = yaml[(at + 1)..];
        var next = Regex.Match(rest[1..], "\n            [a-z_]+:\n|\n      - name: ");
        var block = next.Success ? rest[..(next.Index + 1)] : rest;

        return Regex.Matches(block, @"^\s*-\s*'(?<p>[^']+)'\s*$", RegexOptions.Multiline)
            .Select(m => m.Groups["p"].Value)
            .ToList();
    }
}
