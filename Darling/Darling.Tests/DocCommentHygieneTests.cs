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
/// <para><b>An ATTRIBUTE does not end a doc run (#2445).</b> The #2190 rule keyed off a contiguous run of
/// <c>///</c> lines, so a stacked pair separated by an attribute line was two runs of one opening each and
/// invisible — which is exactly how a displaced block sat on dev carrying
/// <c>[SupportedOSPlatform("windows")]</c> with it, silently platform-annotating the record struct it landed
/// on. Attributes belong to the member BELOW them, so a run continues across them: a <c>///</c> line, any
/// number of attribute lines, and more <c>///</c> lines all still document one member. An attribute never
/// STARTS a run — one above a doc block is that member's own, and the doc block below it is still its first
/// summary. Widening the rule this way found exactly one offender in the whole tree, the one it was written
/// for, which is the measurement that says it is a sharpened rule rather than a looser one.</para>
///
/// <para><b>Balance, not just count (#2940).</b> Counting openings is the right shape for a DISPLACED
/// block — two summaries where one belongs — but a run with ZERO openings and a stray
/// <c>&lt;/summary&gt;</c> satisfies that rule vacuously, and that is precisely the shape a merge
/// produces. Where several branches append to one file, each group's block continues a
/// <c>&lt;summary&gt;</c> its predecessor opened above the conflict hunk, so a keep-both-sides resolution
/// leaves the second block with a closing tag and no opening one. It reached dev in #2940 on
/// <c>ServiceCommandDeadlines.cs</c> — 8 openings against 9 closings — and was found by hand on a later
/// merge rather than by this suite. Nothing in the build could have caught it either: no project sets
/// <c>GenerateDocumentationFile</c>, so no documentation XML is emitted and CS1570 never fires.
/// Requiring openings to EQUAL closings per run catches both directions, and catches one shape a
/// per-FILE tag count cannot — an unclosed run and an unopened one in the same file, whose counts
/// cancel.</para>
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

    /// <summary>
    /// One <c>&lt;/summary&gt;</c> CLOSING tag, counted per run against <see cref="SummaryOpening"/>. Kept
    /// as its own pattern rather than reusing the opening one with an optional slash, because the two are
    /// asked different questions and a single pattern that matched both would make the imbalance it exists
    /// to find invisible. Escaped mentions in prose (<c>&amp;lt;/summary&amp;gt;</c>) are not closings,
    /// exactly as they are not openings.
    /// </summary>
    private static readonly Regex SummaryClosing = new(@"</summary\s*>", RegexOptions.Compiled);

    [Fact]
    public void NoMemberCarriesTwoStackedSummaryBlocks()
    {
        var root = RepoRootOrFail();

        var offenders = new List<string>();
        foreach (var file in SourceFiles(root))
        {
            foreach (var run in StackedSummaryRuns(File.ReadAllLines(file)))
            {
                /* Name the run's first line — somewhere you can actually open — and every opening in it, since
                   the second one is usually the insertion point that caused the stacking. */
                offenders.Add(
                    $"{Path.GetRelativePath(root, file)}:{run.Start} " +
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
            "Where the two summaries are separated by an ATTRIBUTE line, the attribute travelled with the " +
            "displaced block and is now annotating the wrong member — move or delete BOTH, not just the text " +
            "(#2445).\n\n" +
            "Where an insertion split a block, also check whether the member it came from has since been " +
            "re-documented in place. If it has, the stray text is a stranded HEAD rather than the whole block, " +
            "and moving it back would create the very duplicate this rule forbids — confirm sentence by " +
            "sentence that nothing is lost, then delete it (#2190).\n\n" +
            string.Join("\n", offenders));
    }

    /// <summary>
    /// Every doc run closes exactly the <c>&lt;summary&gt;</c> elements it opens (#2940).
    ///
    /// <para><b>Why this is a second rule and not a widening of the first.</b>
    /// <see cref="NoMemberCarriesTwoStackedSummaryBlocks"/> fails a run at two or more openings, so it is
    /// blind by construction to a run with NONE — the stray <c>&lt;/summary&gt;</c> a merge leaves when it
    /// resolves away an opening tag. The two rules read the same runs from <see cref="DocRuns"/> so they
    /// cannot disagree about which lines document which member; they disagree only about what is wrong with
    /// a run, and neither subsumes the other. A stacked-but-balanced pair is the first rule's alone, and an
    /// unopened block is this one's.</para>
    ///
    /// <para><b>Per RUN, not per file.</b> A file-level tag count is the obvious form and it has a blind
    /// spot the run-level one does not: an unclosed run and an unopened run in the same file balance each
    /// other, so the count comes back equal while two members are documented wrongly.</para>
    /// </summary>
    [Fact]
    public void EveryDocRunClosesTheSummariesItOpens()
    {
        var root = RepoRootOrFail();

        var offenders = new List<string>();
        foreach (var file in SourceFiles(root))
        {
            foreach (var run in UnbalancedSummaryRuns(File.ReadAllLines(file)))
            {
                /* Both tag lists, not just the counts: which tag is missing is the whole of what the reader
                   has to decide, and the run's first line is where they have to look to decide it. */
                offenders.Add(
                    $"{Path.GetRelativePath(root, file)}:{run.Start} " +
                    $"(opens={run.Openings.Count} closes={run.Closings.Count}; " +
                    $"<summary> at [{string.Join(", ", run.Openings)}], " +
                    $"</summary> at [{string.Join(", ", run.Closings)}])");
            }
        }

        Assert.True(offenders.Count == 0,
            "Unbalanced <summary> elements found. Each is one doc run that opens and closes a different " +
            "number of <summary> elements — the artifact a keep-both-sides merge produces on a file several " +
            "branches append to, where the second block keeps a closing tag whose opening tag was resolved " +
            "away.\n\n" +
            "MORE CLOSINGS than openings: the run lost its opening tag. Put the <summary> line back — do NOT " +
            "delete the closing tag to balance it, which would quietly demote a documented block to a loose " +
            "comment and lose nothing visibly. Read the run's first line before you do: if it starts " +
            "mid-sentence, the merge took the block's opening prose along with the tag and that sentence has " +
            "to be written back too (#2940).\n\n" +
            "MORE OPENINGS than closings: the run lost its closing tag, so every member below it in the run " +
            "is swallowed by the open element.\n\n" +
            "This is not caught by the stacked-summary rule above, which counts openings and fails at two or " +
            "more: a run with zero openings satisfies it vacuously.\n\n" +
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
    /* Split by an attribute, which is how the stranded ConfigureFirewallAsync block hid on dev: two runs of
       one opening each to the pre-#2445 rule, one member with two summaries in fact. */
    [InlineData(true, "/// <summary>\n/// A.\n/// </summary>\n[SupportedOSPlatform(\"windows\")]\n/// <summary>\n/// B.\n/// </summary>\nvoid M();")]
    /* One summary plus the other doc tags that legitimately follow it. */
    [InlineData(false, "/// <summary>\n/// A.\n/// </summary>\n/// <param name=\"x\">X.</param>\n/// <returns>Y.</returns>\nvoid M(int x);")]
    /* One summary carrying several <para> blocks, as most of this repo's docs do. */
    [InlineData(false, "/// <summary>\n/// A.\n///\n/// <para>B.</para>\n///\n/// <para>C.</para>\n/// </summary>\nvoid M();")]
    /* Two members, one summary each: the declarations between them end each run. */
    [InlineData(false, "/// <summary>A.</summary>\nint A;\n/// <summary>B.</summary>\nint B;")]
    /* A documented member that also carries attributes — the ordinary shape of most of this repo. Widening
       the run across attributes must not turn this into an offender. */
    [InlineData(false, "/// <summary>A.</summary>\n[Fact]\n[Trait(\"k\", \"v\")]\nvoid M();")]
    /* The negative control for that widening, and the one that would catch it going too far: two members that
       each have a summary and an attribute must stay two runs, not merge into one with two openings. */
    [InlineData(false, "/// <summary>A.</summary>\n[Fact]\nvoid A();\n/// <summary>B.</summary>\n[Fact]\nvoid B();")]
    /* An attribute ABOVE a doc block belongs to that same member and must not open a run of its own, or the
       block below it would be counted as a second summary. */
    [InlineData(false, "[Fact]\n/// <summary>A.</summary>\nvoid M();")]
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
    /// The balance rule's own self-test, for the same reason the stacked rule has one: #2190 was a blind
    /// spot in the DETECTOR rather than in anyone's reading of the tree, and the #2940 artifact was a
    /// second. The first case below is that artifact reduced to its essentials, and it is the case the
    /// stacked rule returns GREEN on — which is what makes these two rules independent, not redundant.
    /// </summary>
    [Theory]
    /* The #2940 shape: a block whose opening tag a merge resolved away, closing tag intact. ZERO openings,
       which is exactly why counting openings and failing at two cannot see it. */
    [InlineData(1, "/// the prose the merge left beginning mid-sentence.\n/// </summary>\npublic const int X = 1;")]
    /* The mirror — an opening with no closing, which swallows the member below it. */
    [InlineData(1, "/// <summary>\n/// A.\npublic const int X = 1;")]
    /* Both faults in one file, in DIFFERENT runs. A per-FILE tag count reports this as balanced (one of
       each); per run it is two offenders. This is the case that says the rule belongs at run scope. */
    [InlineData(2, "/// <summary>\n/// A.\nint A;\n/// B.\n/// </summary>\nint B;")]
    /* An attribute does not end a run (#2445), so a block that lost its opening tag BELOW an attribute is
       part of the run above it: one offender, reported once, rather than two runs of one tag each. The
       stacked rule is green here too — one opening, not two. */
    [InlineData(1, "/// <summary>\n/// A.\n/// </summary>\n[Fact]\n/// B.\n/// </summary>\nvoid M();")]
    /* One summary, closed. */
    [InlineData(0, "/// <summary>\n/// A.\n/// </summary>\nvoid M();")]
    /* Single-line summary: the opening and closing sit on one line and must both be counted. */
    [InlineData(0, "/// <summary>A.</summary>\nvoid M();")]
    /* A stacked pair that is nonetheless BALANCED. Pinned to keep the two rules disjoint: this one must not
       double-report what the stacked rule already catches. */
    [InlineData(0, "/// <summary>\n/// A.\n/// </summary>\n/// <summary>\n/// B.\n/// </summary>\nvoid M();")]
    /* A doc run with no summary at all — <inheritdoc/>, or a run of <param> tags — balances at 0 == 0. */
    [InlineData(0, "/// <inheritdoc/>\nvoid M();")]
    /* Escaped mentions in prose are neither openings nor closings, as throughout this very file. */
    [InlineData(0, "/// <summary>\n/// A &lt;summary&gt; and a &lt;/summary&gt; named in prose.\n/// </summary>\nvoid M();")]
    public void DetectorBalancesSummaryTagsPerDocRun(int expected, string source)
    {
        var runs = UnbalancedSummaryRuns(source.Split('\n'));

        Assert.True(
            runs.Count == expected,
            $"Expected {expected} unbalanced run(s), found {runs.Count}, in:\n{source}");
    }

    /// <summary>
    /// Every contiguous run of <c>///</c> lines, as the run's first line and the line of each
    /// <c>&lt;summary&gt;</c> opening and closing tag in it. A run ends at the first line that is neither a
    /// doc comment nor an attribute, which is what ties it to exactly one member: the declaration itself
    /// terminates it.
    /// <para>Attributes are inside the run rather than ending it (#2445) because they document the member
    /// BELOW them, so <c>/// … [Attr] /// …</c> is one member with two summaries. They cannot OPEN a run: an
    /// attribute reached while <c>start == 0</c> falls through to the terminator branch, which is a no-op
    /// there, so an attribute written above a doc block leaves that block as the run's first opening.</para>
    /// <para>Both rules in this class read their runs from here rather than scanning separately, so they
    /// cannot come to disagree about which lines document which member — the question #2190 and #2445 each
    /// re-answered, and the one a second scanner would eventually get wrong on its own.</para>
    /// </summary>
    private static List<(int Start, List<int> Openings, List<int> Closings)> DocRuns(string[] lines)
    {
        var runs = new List<(int Start, List<int> Openings, List<int> Closings)>();

        /* 0 means "not currently inside a run"; line numbers reported to a human are 1-based. Iterating one
           past the end closes a run that reaches EOF rather than dropping it. */
        var start = 0;
        var openings = new List<int>();
        var closings = new List<int>();

        for (var i = 0; i <= lines.Length; i++)
        {
            var trimmed = i < lines.Length ? lines[i].TrimStart() : string.Empty;
            if (i < lines.Length && trimmed.StartsWith("///", StringComparison.Ordinal))
            {
                if (start == 0)
                {
                    start = i + 1;
                    openings = new List<int>();
                    closings = new List<int>();
                }

                openings.AddRange(Enumerable.Repeat(i + 1, SummaryOpening.Matches(lines[i]).Count));
                closings.AddRange(Enumerable.Repeat(i + 1, SummaryClosing.Matches(lines[i]).Count));
            }
            else if (start != 0 && trimmed.StartsWith("[", StringComparison.Ordinal))
            {
                /* Inside a run: an attribute annotates the member below it, so it separates nothing. */
            }
            else if (start != 0)
            {
                runs.Add((start, openings, closings));
                start = 0;
            }
        }

        return runs;
    }

    /// <summary>
    /// The runs carrying more than one <c>&lt;summary&gt;</c> opening — a member with two summaries,
    /// whether or not either is closed (#2190).
    /// </summary>
    private static List<(int Start, List<int> Openings)> StackedSummaryRuns(string[] lines) =>
        DocRuns(lines)
            .Where(run => run.Openings.Count > 1)
            .Select(run => (run.Start, run.Openings))
            .ToList();

    /// <summary>
    /// The runs that do not close every <c>&lt;summary&gt;</c> they open, in either direction (#2940).
    /// </summary>
    private static List<(int Start, List<int> Openings, List<int> Closings)> UnbalancedSummaryRuns(
        string[] lines) =>
        DocRuns(lines)
            .Where(run => run.Openings.Count != run.Closings.Count)
            .ToList();

    /// <summary>
    /// The repo root, or a failed assertion naming the walk-up that could not find it.
    /// <para>FAIL rather than skip when the tree cannot be found. A guard that silently skips is a guard
    /// that silently stops guarding, which is the failure this whole class exists to prevent — if the
    /// output layout ever changes, this should go red and get fixed, not evaporate.</para>
    /// </summary>
    private static string RepoRootOrFail()
    {
        var root = FindRepoRoot();

        Assert.True(root is not null,
            "Could not locate the repository root (walked up from the test binary looking for " +
            "PerformanceMonitor.sln). This test scans the source tree, so it cannot run without it — fix the " +
            "walk-up rather than skipping, or the rule stops being enforced without anyone noticing.");

        return root!;
    }

    /// <summary>
    /// Every <c>.cs</c> file under <paramref name="root"/> that is a source file rather than a build
    /// output. Both rules scan the same set, so a path either rule should ignore is excluded in one place.
    /// </summary>
    private static IEnumerable<string> SourceFiles(string root)
    {
        foreach (var file in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
        {
            if (file.Contains(@"\bin\", StringComparison.OrdinalIgnoreCase)
                || file.Contains(@"\obj\", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            yield return file;
        }
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
