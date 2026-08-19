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
/// Repo-wide documentation hygiene (#1745). One rule today: no member carries two stacked
/// <c>&lt;summary&gt;</c> blocks.
///
/// <para><b>Why this needs a test rather than a review.</b> XML documentation takes the LAST summary, so
/// IntelliSense, generated docs and every analyzer render the correct text. The build is clean, nothing
/// warns, and no behavioral test can see it. The only reader misled is a human reading top-down — who gets
/// the WRONG description first, attached to the wrong member. In #1739 the same region was read at least
/// three times by two people over several hours, one of whom quoted a paragraph out of it, and neither saw
/// the duplicate; a regex found it in one pass. That is a property of the defect, not of the readers.</para>
///
/// <para><b>It is not cosmetic.</b> Of the eight sites this rule found on dev, SEVEN were a real doc block
/// displaced off its member by an insertion — so the member the summary described was left with no
/// documentation at all, and a different member acquired a description of something else. Only one was a
/// superseded duplicate safe to simply delete. A blind "remove the extra summary" sweep would have destroyed
/// documentation at seven of the eight.</para>
///
/// <para><b>Counted by openings, not by closing tags (#2190).</b> The first version of this rule keyed off a
/// <c>&lt;/summary&gt;</c> immediately followed by a reopening, so it saw a stacked pair only when the FIRST
/// block was closed. Two instances sat on dev unseen: a duplicated opening tag, and a doc block that an
/// insertion had split, stranding its head on the following member with no closing tag near it. Both are
/// caught by counting <c>&lt;summary&gt;</c> OPENINGS inside each contiguous run of <c>///</c> lines — a run
/// documents exactly one member, so two openings in one run means two summaries, whether or not either is
/// closed and whether they are written single-line or spread over many. The mixed form is the one a
/// closing-tag matcher cannot see at all: a single-line summary followed by a multi-line one.</para>
///
/// <para><b>Coverage limit, stated rather than assumed.</b> CI path filters are per-project, so this runs on
/// any pull request that trips the <c>darling</c> or <c>core</c> filter, and on every nightly and release
/// build — but a change touching ONLY Lite or Installer will not run it, and would be caught on the next
/// nightly instead. It lives here because the repo's other source-parsing pins do
/// (<c>HostHeaderGuardTests</c>, <c>DarlingStoreUpgradeTests</c>), which is where someone looks for this
/// kind of guard.</para>
/// </summary>
public sealed class DocCommentHygieneTests
{
    /// <summary>
    /// One <c>&lt;summary&gt;</c> OPENING tag. Counting these per doc run, rather than pairing them against a
    /// closing tag, is what lets the rule see an unclosed first block. Still deliberately narrow: a summary
    /// followed by <c>&lt;param&gt;</c>, <c>&lt;returns&gt;</c>, <c>&lt;remarks&gt;</c> or any number of
    /// <c>&lt;para&gt;</c> blocks is one opening and never matches twice, and an escaped mention in prose
    /// (<c>&amp;lt;summary&amp;gt;</c>, as used throughout this very file) is not an opening at all.
    /// </summary>
    private static readonly Regex SummaryOpening = new(@"<summary\s*>", RegexOptions.Compiled);

    [Fact]
    public void NoMemberCarriesTwoStackedSummaryBlocks()
    {
        var root = FindRepoRoot();

        /* FAIL rather than skip when the tree cannot be found. A guard that silently skips is a guard that
           silently stops guarding, which is the failure this whole rule exists to prevent — if the output
           layout ever changes, this should go red and get fixed, not evaporate. */
        Assert.True(root is not null,
            "Could not locate the repository root (walked up from the test binary looking for " +
            "PerformanceMonitor.sln). This test scans the source tree, so it cannot run without it — fix the " +
            "walk-up rather than skipping, or the rule stops being enforced without anyone noticing.");

        var offenders = new List<string>();
        foreach (var file in Directory.EnumerateFiles(root!, "*.cs", SearchOption.AllDirectories))
        {
            if (file.Contains(@"\bin\", StringComparison.OrdinalIgnoreCase)
                || file.Contains(@"\obj\", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            foreach (var run in StackedSummaryRuns(File.ReadAllLines(file)))
            {
                /* Name the run's first line — somewhere you can actually open — and every opening in it, since
                   the second one is usually the insertion point that caused the stacking. */
                offenders.Add(
                    $"{Path.GetRelativePath(root!, file)}:{run.Start} " +
                    $"(<summary> openings at lines {string.Join(", ", run.Openings)})");
            }
        }

        Assert.True(offenders.Count == 0,
            "Stacked <summary> blocks found. Each is a member carrying two summaries; XML docs take the LAST " +
            "one, so tooling looks correct and only a human reading the file is misled.\n\n" +
            "DO NOT just delete the first summary — check first whether it belongs to a DIFFERENT member that " +
            "an insertion pushed it away from. Seven of the eight found in #1745 were displaced doc blocks " +
            "whose real member had been left undocumented, and deleting them would have lost the documentation " +
            "rather than deduplicating it.\n\n" +
            "Where an insertion split a block, also check whether the member it came from has since been " +
            "re-documented in place. If it has, the stray text is a stranded HEAD rather than the whole block, " +
            "and moving it back would create the very duplicate this rule forbids — confirm sentence by " +
            "sentence that nothing is lost, then delete it (#2190).\n\n" +
            string.Join("\n", offenders));
    }

    /// <summary>
    /// The rule's own self-test. #2190 was a blind spot in the DETECTOR rather than in anyone's reading of the
    /// tree, and it was a synthetic case that exposed it — so the shapes this must catch, and the ones it must
    /// leave alone, are pinned here instead of being left to whatever the tree happens to contain. Every
    /// <c>true</c> case below is a real shape that has appeared in this repo.
    /// </summary>
    [Theory]
    /* Closed and immediately reopened: the only shape the pre-#2190 rule could see. */
    [InlineData(true, "/// <summary>\n/// A.\n/// </summary>\n/// <summary>\n/// B.\n/// </summary>\nvoid M();")]
    /* A duplicated opening tag, first block never closed. */
    [InlineData(true, "/// <summary>\n/// <summary>\n/// A.\n/// </summary>\nvoid M();")]
    /* An insertion split a block, stranding its unclosed head above the next member's whole block. */
    [InlineData(true, "/// <summary>\n/// A.\n///\n/// <summary>\n/// B.\n/// </summary>\nvoid M();")]
    /* Single-line followed by multi-line — invisible to a closing-tag matcher, since the reopening does not
       follow a </summary> on its own line. */
    [InlineData(true, "/// <summary>A.</summary>\n/// <summary>\n/// B.\n/// </summary>\nvoid M();")]
    /* A stacked run reaching end of file with no member under it. Not valid C#, but it pins the scan's
       one-past-the-end step: a run that never meets a non-doc line has to be closed, not dropped. */
    [InlineData(true, "/// <summary>\n/// A.\n/// <summary>\n/// B.")]
    /* One summary plus the other doc tags that legitimately follow it. */
    [InlineData(false, "/// <summary>\n/// A.\n/// </summary>\n/// <param name=\"x\">X.</param>\n/// <returns>Y.</returns>\nvoid M(int x);")]
    /* One summary carrying several <para> blocks, as most of this repo's docs do. */
    [InlineData(false, "/// <summary>\n/// A.\n///\n/// <para>B.</para>\n///\n/// <para>C.</para>\n/// </summary>\nvoid M();")]
    /* Two members, one summary each: the declarations between them end each run. */
    [InlineData(false, "/// <summary>A.</summary>\nint A;\n/// <summary>B.</summary>\nint B;")]
    /* Escaped mentions in prose are not openings — this very file is full of them. */
    [InlineData(false, "/// <summary>\n/// Two &lt;summary&gt; mentions in one &lt;summary&gt; block.\n/// </summary>\nvoid M();")]
    public void DetectorCountsSummaryOpeningsPerDocRun(bool stacked, string source)
    {
        var runs = StackedSummaryRuns(source.Split('\n'));

        Assert.True(
            runs.Count == (stacked ? 1 : 0),
            $"Expected {(stacked ? "one stacked run" : "no stacked run")}, found {runs.Count}, in:\n{source}");
    }

    /// <summary>
    /// Every contiguous run of <c>///</c> lines carrying more than one <c>&lt;summary&gt;</c> opening, as the
    /// run's first line and the line of each opening. A run ends at the first line that is not a doc comment,
    /// which is what ties it to exactly one member: the declaration itself terminates it.
    /// </summary>
    private static List<(int Start, List<int> Openings)> StackedSummaryRuns(string[] lines)
    {
        var runs = new List<(int Start, List<int> Openings)>();

        /* 0 means "not currently inside a run"; line numbers reported to a human are 1-based. Iterating one
           past the end closes a run that reaches EOF rather than dropping it. */
        var start = 0;
        var openings = new List<int>();

        for (var i = 0; i <= lines.Length; i++)
        {
            if (i < lines.Length && lines[i].TrimStart().StartsWith("///", StringComparison.Ordinal))
            {
                if (start == 0)
                {
                    start = i + 1;
                    openings = new List<int>();
                }

                openings.AddRange(Enumerable.Repeat(i + 1, SummaryOpening.Matches(lines[i]).Count));
            }
            else if (start != 0)
            {
                if (openings.Count > 1)
                {
                    runs.Add((start, openings));
                }

                start = 0;
            }
        }

        return runs;
    }

    /// <summary>
    /// Walks up from the test output directory to the repo root — the directory holding
    /// <c>PerformanceMonitor.sln</c>. Same walk-up idiom as <c>ThemeParityTests.FindRepoRoot</c>.
    /// </summary>
    private static string? FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        return null;
    }
}
