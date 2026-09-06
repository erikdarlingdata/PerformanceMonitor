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
/// Repo-wide documentation hygiene (#1745). The rules, each with its own section below: no member
/// carries two stacked <c>&lt;summary&gt;</c> blocks, every doc run closes the
/// <c>&lt;summary&gt;</c> elements it opens, and every cref names something this repository's code
/// spells. Listed rather than counted, so the sentence cannot go stale by one the way it had before a
/// third rule arrived (#3069's criterion, applied to this class's own prose).
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
/// <para><b>Coverage, stated rather than assumed.</b> CI path filters are per-project, so the build job
/// runs this suite on any pull request that trips the <c>darling</c> or <c>core</c> filter — and where
/// none of them fires, <c>build.yml</c>'s <c>darling-tree-guards</c> job runs the WHOLE suite instead,
/// gated on the build job's step reporting <c>skipped</c>. So a change touching only Lite or only
/// Installer does run these rules, in that job rather than in the build one; that backstop is the bound
/// the cref rule's repository-wide sweep rests on, and it is the same bound
/// <c>CrossAppGuardCiGateTests</c>' exemptions rest on. It lives here because the repo's other
/// source-parsing pins do
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
    /// <see cref="HasBuildOutputSegment"/> reaches the same verdict for a path written with either
    /// separator, so the set both rules read is a property of the tree and not of the machine that ran
    /// them.
    ///
    /// <para><b>Why every path here is written out by hand.</b> <see cref="Path.Combine"/> emits the
    /// HOST's separator, so a case built with it exercises whichever flavour the runner happens to use and
    /// says nothing about the other — a filter recognising only <c>\</c> passes such a case on Windows
    /// while scanning every generated file on a macOS or Linux run, which is the asymmetry this pin is for.
    /// Each case below carries its separators literally and appears in both flavours, so whichever
    /// platform runs this asserts both.</para>
    ///
    /// <para>The negatives are the other half of the claim: excluding by whole segment is what keeps a
    /// directory merely NAMED like build output inside the scanned set.</para>
    /// </summary>
    [Theory]
    /* Generated output, the reason the filter exists: .AssemblyInfo.cs and .g.cs land here during a build. */
    [InlineData(true, "Darling/Darling.Tests/bin/Debug/net10.0/Darling.Tests.AssemblyInfo.cs")]
    [InlineData(true, @"Darling\Darling.Tests\bin\Debug\net10.0\Darling.Tests.AssemblyInfo.cs")]
    [InlineData(true, "Darling/Darling.Tests/obj/Debug/net10.0/Darling.Tests.g.cs")]
    [InlineData(true, @"Darling\Darling.Tests\obj\Debug\net10.0\Darling.Tests.g.cs")]
    /* Windows paths are case-insensitive and so is this rule. */
    [InlineData(true, "Darling/Darling.Tests/BIN/Debug/Generated.cs")]
    [InlineData(true, @"Darling\Darling.Tests\OBJ\Debug\Generated.cs")]
    /* Ordinary source — the set the two rules exist to read. */
    [InlineData(false, "Darling/Darling.Tests/DocCommentHygieneTests.cs")]
    [InlineData(false, @"Darling\Darling.Tests\DocCommentHygieneTests.cs")]
    /* Segments, not substrings: every one of these CONTAINS "bin" or "obj" and is source. */
    [InlineData(false, "Darling/PerformanceMonitor.Darling.Viewer/mybin/Thing.cs")]
    [InlineData(false, @"Darling\PerformanceMonitor.Darling.Viewer\mybin\Thing.cs")]
    [InlineData(false, "Darling/PerformanceMonitor.Darling.Viewer/obj-cache/Thing.cs")]
    [InlineData(false, @"Darling\PerformanceMonitor.Darling.Viewer\obj-cache\Thing.cs")]
    [InlineData(false, "Darling/PerformanceMonitor.Darling.Viewer/Objects/ObjectBrowser.cs")]
    [InlineData(false, @"Darling\PerformanceMonitor.Darling.Viewer\Objects\ObjectBrowser.cs")]
    public void BuildOutputIsRecognisedThroughEitherSeparator(bool excluded, string path)
    {
        Assert.True(
            HasBuildOutputSegment(path) == excluded,
            $"'{path}' should{(excluded ? " " : " NOT ")}be treated as build output. The verdict cannot " +
            "depend on which separator the path is written with: both rules in this class read whatever " +
            "this admits, so a separator-specific filter hands them a different set of files on Windows " +
            "than on macOS or Linux, and the same commit is then guarded differently depending on who ran " +
            "it. Compare whole path SEGMENTS against both separator characters.");
    }

    /// <summary>
    /// A <c>bin</c> or <c>obj</c> directory ABOVE the scanned root leaves the tree scanned, because
    /// <see cref="IsBuildOutput"/> judges each path relative to the root it was handed.
    ///
    /// <para>The failure direction is the silent one. An absolute-path test would classify every file in a
    /// repository checked out under, say, <c>/opt/bin/</c> as build output, and both rules would report no
    /// offenders having read nothing at all — which is indistinguishable from a clean tree.</para>
    /// </summary>
    [Fact]
    public void ABuildDirectoryAboveTheRootDoesNotExcludeTheWholeTree()
    {
        Assert.False(
            IsBuildOutput("/opt/bin/repo", "/opt/bin/repo/Darling/Darling.Tests/DocCommentHygieneTests.cs"),
            "A source file was read as build output because a directory ABOVE the scanned root is called " +
            "'bin'. Every file in the tree is excluded on that reading and both rules pass having scanned " +
            "nothing, which looks exactly like a clean tree.");

        Assert.True(
            IsBuildOutput("/opt/bin/repo", "/opt/bin/repo/Darling/Darling.Tests/obj/Debug/Generated.cs"),
            "Build output UNDER the scanned root was admitted as source, so relativising the path has " +
            "gone too far and dropped the exclusion the walk needs.");
    }

    /// <summary>
    /// <see cref="SourceFiles"/> applies the filter, so a <c>bin</c> or <c>obj</c> directory under the
    /// scanned root is never read.
    ///
    /// <para>Separate from the two pins above, which judge the predicate on paths that need not exist.
    /// This one walks a REAL directory tree, so it is the one that fails if the exclusion is ever detached
    /// from the enumeration: a predicate nothing calls leaves both rules reading everything, and they pass
    /// either way on a tree whose generated files happen to carry no doc comments.</para>
    /// </summary>
    [Fact]
    public void SourceFilesDoesNotReadBuildOutputUnderTheRoot()
    {
        var root = Directory.CreateTempSubdirectory("darling-doc-hygiene-sourcefiles-");
        try
        {
            Directory.CreateDirectory(Path.Combine(root.FullName, "bin", "Debug"));
            Directory.CreateDirectory(Path.Combine(root.FullName, "obj", "Debug"));
            /* A directory merely NAMED like build output, which has to stay in the scanned set. */
            Directory.CreateDirectory(Path.Combine(root.FullName, "Objects"));

            File.WriteAllText(Path.Combine(root.FullName, "Source.cs"), "// source");
            File.WriteAllText(Path.Combine(root.FullName, "Objects", "ObjectBrowser.cs"), "// source");
            File.WriteAllText(
                Path.Combine(root.FullName, "bin", "Debug", "Thing.AssemblyInfo.cs"), "// generated");
            File.WriteAllText(Path.Combine(root.FullName, "obj", "Debug", "Thing.g.cs"), "// generated");

            /* Separators normalised for the comparison only: what this pin asserts is WHICH files came
               back, and the platform-independence of the filter itself is asserted above. */
            var scanned = SourceFiles(root.FullName)
                .Select(f => Path.GetRelativePath(root.FullName, f).Replace('\\', '/'))
                .OrderBy(f => f, StringComparer.Ordinal)
                .ToArray();

            Assert.Equal(new[] { "Objects/ObjectBrowser.cs", "Source.cs" }, scanned);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /* ───────────────── crefs: the mechanically checkable part of a doc comment (#3083) ───────────────── */

    /// <summary>
    /// One cref attribute value, verbatim, with the doc element that carried it and where it was read
    /// from. The target is stored UNMODIFIED — every reduction the resolver performs happens in
    /// <see cref="CrefSegments"/>, on a copy, so a malformed target is still reportable in the shape it was
    /// written in.
    /// </summary>
    private readonly record struct CrefSite(string Path, int Line, string Element, string Target);

    /// <summary>
    /// What one scan produced: the population it read (per solution project, so a scan that lost a project
    /// is distinguishable from one that read it and found nothing), every cref in it, and the identifier
    /// universe those crefs resolve against.
    ///
    /// <para>Carried as one value rather than returned piecemeal so a caller cannot check the offenders and
    /// skip the floors — the failure direction here is silent non-detection, and a scan that read nothing
    /// reports zero offenders.</para>
    /// </summary>
    private sealed record CrefCensus(
        IReadOnlyDictionary<string, (int Files, int DocBlocks, int Crefs)> ByProject,
        IReadOnlyList<CrefSite> Sites,
        IReadOnlySet<string> CodeIdentifiers);

    /// <summary>
    /// One cref, anchored on the ELEMENT that carries it rather than on the attribute alone.
    ///
    /// <para><b>Why the element, which cost a round to learn.</b> A cref is only a reference when it is an
    /// attribute of a doc element. The attribute spelling occurs in prose too — this class discusses it,
    /// and <c>ViewerTimeHelper</c> shows an ESCAPED example inside <c>&lt;c&gt;</c> — and an
    /// attribute-only pattern reads all of those as references. That direction is loud rather than silent,
    /// but it is loud in a file whose whole job is to talk about crefs, and the first version of this
    /// extractor duly reported two offenders it had written itself. Anchoring on <c>&lt;element …</c>
    /// excludes prose by construction, because <c>[^>]*?</c> cannot cross the <c>&gt;</c> that closes the
    /// <c>&lt;c&gt;</c> tag, and an escaped <c>&amp;lt;see</c> has no <c>&lt;</c> to anchor on at all.</para>
    ///
    /// <para><b>The element name is CAPTURED, not enumerated.</b> An alternation of the element names
    /// known today would silently miss a cref on <c>&lt;seealso&gt;</c> or <c>&lt;permission&gt;</c> the
    /// first time one is written. Capturing it instead means nothing is missed, and
    /// <see cref="TheElementsCarryingACrefAreTheOnesThisExtractorKnows"/> compares the set actually found
    /// against <see cref="CrefCarryingElements"/> so a new one is noticed rather than absorbed.</para>
    ///
    /// <para><b>Both groups are negated classes and not <c>.*</c>.</b> A greedy value group spans from the
    /// first quote to the LAST one on the joined block, swallowing every cref after the first into one
    /// target that is then reported as a single unresolvable string — the token under test absorbed by the
    /// group meant to isolate it. <see cref="TheCrefExtractorDoesNotAbsorbTheTokenUnderTest"/> drives both
    /// forms over the same input and pins the difference, because the greedy form still yields A match and
    /// still fails on garbage, so nothing else here would notice.</para>
    /// </summary>
    private static readonly Regex CrefReference = new(
        @"<(?<element>[A-Za-z]+)\b[^>]*?cref\s*=\s*""(?<target>[^""]*)""",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The doc elements found carrying a cref in this tree, with what each one is for. Compared for
    /// EQUALITY against what the sweep finds, so an element kind that starts being used reds here and gets
    /// read — the extractor does not filter on this list, so the failure costs a line of prose rather than
    /// a missed reference.
    /// </summary>
    private static readonly Dictionary<string, string> CrefCarryingElements = new(StringComparer.Ordinal)
    {
        ["see"] = "An inline reference, and all but a handful of the crefs in the tree.",
        ["inheritdoc"] = "Pulls documentation from the named member rather than the base one.",
        ["exception"] = "Names the exception type a member throws.",
    };

    /// <summary>A whole C# identifier and nothing else — anchored at both ends, so a segment carrying a
    /// stray <c>!</c>, <c>?</c>, brace, space or empty string fails rather than matching its legal
    /// prefix.</summary>
    private static readonly Regex WholeIdentifier = new(
        @"\A[A-Za-z_][A-Za-z0-9_]*\z",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>Every identifier-shaped token in a span of code, which is how the universe below is
    /// built.</summary>
    private static readonly Regex IdentifierToken = new(
        @"[A-Za-z_][A-Za-z0-9_]*",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A doc-XML generic argument list — <c>{TRow}</c>, <c>{T, TResult}</c>. Balanced-free
    /// (<c>[^{}]*</c>) on purpose: an UNCLOSED brace is then not removed, so the segment keeps it and fails
    /// <see cref="WholeIdentifier"/> instead of being quietly repaired into something that resolves.</summary>
    private static readonly Regex GenericArgumentList = new(
        @"\{[^{}]*\}",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The documentation-ID kind prefixes a source cref may legally carry, which the resolver strips before
    /// reading the name.
    ///
    /// <para><b><c>!</c> is deliberately absent, and that omission is the guard's whole second half.</b>
    /// <c>!:</c> is the ID string the compiler EMITS for a reference it could not resolve. A cref carrying
    /// any <c>&lt;letter&gt;:</c> prefix is treated by the compiler as an already-resolved ID and passed
    /// through without being resolved at all — so <c>cref="!:Whatever"</c> compiles silently, emits a dead
    /// reference into the documentation XML, and is invisible to <c>CS1574</c> whether or not
    /// <c>GenerateDocumentationFile</c> is ever turned on. Widening this list by one character would make
    /// every such site resolve here as well. <see cref="TheResolverCannotLaunderAMalformedTarget"/> pins
    /// that character.</para>
    /// </summary>
    private static readonly string[] DocIdKindPrefixes = { "T:", "M:", "P:", "F:", "E:", "N:" };

    /// <summary>
    /// The kind label every entry in <see cref="UnresolvedCrefTargets"/> must open with, and what that kind
    /// asserts about the entry.
    ///
    /// <para>An enumeration, and the direction matters: an entry whose label is not here fails
    /// <see cref="EveryBoundNamesARecognisedKind"/> loudly rather than being counted as whatever the
    /// nearest match was. Two of the three kinds are real defects and one is a limit of this resolver;
    /// collapsing them would let the count of defects fall as the resolver's blind spots grew, which is
    /// the wrong direction for every number this class reports.</para>
    /// </summary>
    private static readonly (string Label, string Meaning)[] CrefBoundKinds =
    {
        ("MARKER", "carries the compiler's own !: unresolvable marker, so no compilation can object"),
        ("DANGLING", "names a symbol that no code in this repository spells"),
        ("OUTSIDE", "names a symbol outside this repository that no C# here spells, which this resolver "
                    + "cannot see"),
    };

    /// <summary>
    /// Every cref target in the tree that this resolver cannot resolve, with the bound its entry rests on.
    /// Compared for EQUALITY, so a reference that gets FIXED forces its entry out rather than lingering as
    /// a permission nobody needs, and a new one fails here.
    ///
    /// <para><b>Keyed by TARGET TEXT, not by site, and that is the bound.</b> A line number churns on every
    /// edit above it and a path churns on every rename, so an allow-list keyed either way would be rewritten
    /// constantly and read never. The cost is stated rather than hidden: a NEW occurrence of an
    /// already-listed target — a second <c>!:</c> on a symbol already listed — does not fail here. It is a
    /// new instance of a defect the list already records, and the rename or deletion this guard exists for
    /// produces a target that is not on the list.</para>
    ///
    /// <para><b>What each kind means for the reader.</b> <c>MARKER</c> and <c>DANGLING</c> are defects, and
    /// the list is where they are inventoried rather than fixed — mixing the repair into the change that
    /// introduces the guard would make the guard's own red-proof unreadable (#3083). <c>OUTSIDE</c> is not a
    /// defect: it is this resolver's boundary, recorded here rather than in prose so that a target which
    /// starts being spelled in code forces its entry out. The three kinds are exercised separately in
    /// <see cref="TheRecordedLimitsAreExercised"/>, because a boundary nobody drives is a claim rather than
    /// a record (#3079).</para>
    /// </summary>
    private static readonly Dictionary<string, string> UnresolvedCrefTargets = new(StringComparer.Ordinal)
    {
        ["!:DarlingManagedRoles"] =
            "MARKER. Names DarlingManagedRoles, which this repository declares in the Darling service. "
            + "Whether deleting the two marker characters is the whole fix depends on the referencing "
            + "project's own references and usings, so the repair is a separate change from this guard.",

        ["!:DarlingManagedRoles.BuildProvisioningSql"] =
            "MARKER. Names DarlingManagedRoles.BuildProvisioningSql, both of which this repository declares.",

        ["!:DarlingManagedRoles.ReassertComposeStatementTimeoutAsync"] =
            "MARKER. Names DarlingManagedRoles.ReassertComposeStatementTimeoutAsync, both declared here.",

        ["!:DarlingRetention"] =
            "MARKER. Names DarlingRetention, which this repository declares.",

        ["!:DarlingSelfAlertEvaluator"] =
            "MARKER. Names DarlingSelfAlertEvaluator, which this repository declares. Two sites, and only "
            + "the target is keyed, so both are covered by this one entry.",

        ["!:DarlingServerConnector"] =
            "MARKER. Names DarlingServerConnector, which this repository declares.",

        ["!:StallWaitProbePolicy.HardBudget"] =
            "MARKER. Names StallWaitProbePolicy.HardBudget, both of which this repository declares.",

        ["!:StorageVersion.SchemaVersion"] =
            "MARKER. Names StorageVersion.SchemaVersion, both declared here — and it sits two lines below a "
            + "cref to DarlingManagedRoles that resolves perfectly well, in the same doc block, which is how "
            + "little there is to see when reading this class of defect by eye.",

        ["!:TimescaleSupport"] =
            "MARKER. Names TimescaleSupport, which this repository declares.",

        ["!:TimescaleSupport.ChunkIntervalDays"] =
            "MARKER. Names TimescaleSupport.ChunkIntervalDays, both of which this repository declares.",

        ["!:TimescaleSupport.HypertableTables"] =
            "MARKER. Names TimescaleSupport.HypertableTables, both of which this repository declares.",

        ["!:ViewerSettings"] =
            "MARKER. Names ViewerSettings, which this repository declares in the Darling viewer.",

        ["PerformanceMonitor.Common.ServerHealthBands"] =
            "DANGLING. Names the FILE PerformanceMonitor.Common/ServerHealthBands.cs; nothing in the "
            + "repository declares a type called ServerHealthBands, so the reference has never pointed at "
            + "anything. A <c> would say what was meant.",

        ["QueryStoreTextState"] =
            "DANGLING. The only other mention of this name in the tree is inside a block comment, so no "
            + "such type exists. Written as the sibling of two states that do.",

        ["TheMemberWalk_SeesAGenericMethodWithAConstraintClause"] =
            "DANGLING. A test method name that nothing declares — the shape a rename leaves behind, and the "
            + "exact failure direction this guard exists for.",

        ["TryGetValidateConfigVerb"] =
            "DANGLING. A member name spelled nowhere in any code in the repository.",

        ["ViewerDataService.MonitoredServers"] =
            "DANGLING. Names the partial-class FILE ViewerDataService.MonitoredServers.cs. The file is real; "
            + "the member is not, and a cref resolves symbols rather than paths.",

        ["ViewerServerTab.LivePlan.cs"] =
            "DANGLING. A file path, .cs extension and all, written into a cref.",

        ["ViewerServerTab.SystemHealthCharts"] =
            "DANGLING. Names the partial-class FILE ViewerServerTab.SystemHealthCharts.cs, same shape as "
            + "the entry above.",

        ["COMException"] =
            "OUTSIDE. System.Runtime.InteropServices.COMException, cref'd where the code catches its base "
            + "ExternalException, so the derived name appears in no C# in the tree.",

        ["CallerFilePathAttribute"] =
            "OUTSIDE. C# elides the Attribute suffix at a usage site, so a tree that uses "
            + "[CallerFilePath] everywhere spells the full type name nowhere. Five sites, all in the two "
            + "test projects.",

        ["CallerMemberNameAttribute"] =
            "OUTSIDE. Same elision as the entry above.",

        ["HostString.Host"] =
            "OUTSIDE. Microsoft.AspNetCore.Http.HostString, reached through a property chain rather than "
            + "named, so the type name appears in no code here.",

        ["IConvertible"] =
            "OUTSIDE. System.IConvertible, named only to explain what a conversion does NOT go through.",

        ["JsonEncodedText.Encode(string)"] =
            "OUTSIDE, and it is important that this entry says so rather than DANGLING: the compiler "
            + "rejects this target too, but for the unrelated reason that the one-argument overload does "
            + "not exist. This resolver cannot see that — it discards parameter lists entirely — and lists "
            + "the target only because System.Text.Json.JsonEncodedText is spelled in no C# here. Right "
            + "answer, different question; labelling it DANGLING would credit the resolver with a check it "
            + "does not perform.",

        ["ListBox"] =
            "OUTSIDE. System.Windows.Controls.ListBox, used from XAML rather than from C#, so the C# "
            + "identifier universe cannot contain it. Three sites, across both SKUs.",

        ["NpgsqlBatch"] =
            "OUTSIDE. Npgsql's batch type, named in prose about why the code does not use it.",

        ["System.Diagnostics.ConditionalAttribute"] =
            "OUTSIDE. Fully qualified, and elided at its usage site as [Conditional], so neither the "
            + "namespace-qualified name nor the suffixed one appears in code.",

        ["System.Windows.Controls.WrapPanel"] =
            "OUTSIDE. A WPF panel type named in prose about layout; the XAML uses it, the C# does not.",

        ["X509KeyStorageFlags.EphemeralKeySet"] =
            "OUTSIDE. The flag is passed through a variable rather than named at the call site.",

        ["X509KeyStorageFlags.PersistKeySet"] =
            "OUTSIDE. Same as the entry above, the other flag of the pair.",
    };

    /// <summary>
    /// The solution projects whose tree carries no cref at all, so the per-project cref floor cannot be
    /// required of them. Compared for EQUALITY like every other bounded set here: a project that acquires
    /// its first cref forces its entry out, and a project that loses its last one fails rather than
    /// silently dropping below a floor nobody re-derived.
    /// </summary>
    private static readonly Dictionary<string, string> SolutionProjectsWithoutCrefs =
        new(StringComparer.Ordinal)
        {
            ["deprecated/Installer"] =
                "One .cs file, two doc-comment blocks, no cref in either. A deprecated WPF installer shell.",

            ["deprecated/Installer.Tests"] =
                "Doc comments but no cref in any of them. A deprecated test project.",
        };

    /// <summary>
    /// Directories holding a <c>.csproj</c> that the solution does not build, so the per-project floor
    /// cannot reach them. At equality, so a project ADDED to the solution is covered by the floor from
    /// then on rather than sitting outside it unnoticed.
    /// </summary>
    private static readonly Dictionary<string, string> ProjectDirectoriesOutsideTheSolution =
        new(StringComparer.Ordinal)
        {
            ["tools/CompactionRepro"] =
                "A throwaway repro rig, deliberately outside the solution and pinning its own package "
                + "version. Nothing here should be held to a documentation floor.",

            ["tools/SliceRepairRepro"] =
                "The second repro rig, same reasoning.",

            ["Darling/tools/generate-ladder-fixture"] =
                "A one-shot generator for a test fixture, run by hand and outside the solution. Nested "
                + "inside a tree the floor DOES cover, which is why the comparison is over project "
                + "directories rather than top-level trees.",
        };

    /// <summary>
    /// Target SHAPES this resolver accepts that a compiler would not, each with the reduction that makes it
    /// accept them — the fidelity boundary the issue named, written as assertions rather than as a
    /// paragraph.
    ///
    /// <para>Every one is driven in <see cref="TheRecordedLimitsAreExercised"/> against the real identifier
    /// universe and asserted to RESOLVE. A limit recorded only in prose is a limit nobody can tell has
    /// stopped being true, and #3079 found three such cases were not even reachable where they were first
    /// written.</para>
    /// </summary>
    private static readonly (string Target, string Limit)[] CrefShapesTheResolverAccepts =
    {
        ("CollectorCatalog.AppliesTo(NoSuchTypeAnywhere)",
            "PARAMETER LISTS are discarded at the first '(', so overload identity is never checked and a "
            + "parameter type that does not exist cannot be seen."),

        ("ICollectorDefinition{NoSuchTypeAnywhere}",
            "GENERIC ARGUMENTS are discarded with the braces, so neither arity nor the arguments themselves "
            + "are checked."),

        ("CollectorCatalog.NoSuchMemberButAppliesTo",
            "NOTHING is checked about a name's position: this segment is not a member of CollectorCatalog "
            + "and does not resolve — included as the negative control for the entry below, so the two "
            + "cannot be read as the same claim."),
    };

    /// <summary>
    /// Real sites in the tree where a cref resolves HERE and does not resolve for the compiler, each with
    /// the reason the two answers differ. Asserted to be present in the census and asserted to resolve, so
    /// the boundary is exercised on the input it describes rather than asserted about a fixture.
    ///
    /// <para>Measured on this branch's base with <c>GenerateDocumentationFile</c> switched on from the
    /// command line — no project file was changed, and none is changed by this PR: the flag remains the
    /// parked decision from #3025. That run reports 71 <c>CS1574</c> occurrences on 70 lines. This resolver
    /// sees 7 of those lines. The three rows below are the three reasons for the other 63, and together they
    /// are why this guard is complementary to that flag rather than a substitute for it — while the 12
    /// <c>MARKER</c> targets above are the traffic in the other direction, which the flag would never
    /// report.</para>
    /// </summary>
    private static readonly (string Project, string Target, string Reason)[]
        LiveSitesOutsideTheResolversReach =
    {
        ("PerformanceMonitor.Collectors", "AppliesTo",
            "SCOPE. A member name is resolved repo-wide, not against the type whose doc comment names it. "
            + "CollectorTargetInfo's summary names AppliesTo, which is declared on the collector "
            + "definitions and on CollectorCatalog rather than on CollectorTargetInfo, so the compiler has "
            + "nothing in scope to bind it to. This is the largest of the three reasons by site count."),

        ("Darling/PerformanceMonitor.Darling.Storage", "PlanCorrectionCollector",
            "USINGS. Namespace imports are ignored here. PgMigrations.cs carries no using for the "
            + "collectors' namespace, so this name is out of scope for the compiler even though the "
            + "project references the assembly that declares it."),

        ("Darling/Darling.Tests", "Write(char)",
            "OVERLOADS. The parameter list is discarded, and the name alone is spelled all over the tree. "
            + "The compiler resolves the whole signature against what is in scope, and in this nested "
            + "private class nothing named Write is."),
    };

    /// <summary>
    /// Every <c>cref</c> in the tree names something this repository's code spells, or is one of the
    /// bounded exceptions above (#3083).
    ///
    /// <para><b>Why a cref and not the rest of a doc comment.</b> By the criterion #3069 settled — a comment
    /// is worth pinning when it restates something derivable, and worth nothing but review when it explains
    /// why — a cref is maximally derivable: it names a symbol that either exists or does not. It is also the
    /// one part of a doc comment nothing in this repository checks, because no project sets
    /// <c>GenerateDocumentationFile</c>, so the compiler never resolves a cref and a rename leaves the
    /// reference aimed at nothing with every gate green.</para>
    ///
    /// <para><b>What resolution means here, stated because it is weaker than a compiler's and the
    /// difference is the whole of what this guard can and cannot say.</b> A target resolves when every
    /// dot-separated segment of it, after the reductions in <see cref="CrefSegments"/>, is an identifier
    /// that appears in the repository's C# CODE — comments and string literals removed by
    /// <see cref="CSharpSourceWalker.StripCommentsAndStrings"/>. So this asks "does anything here spell that
    /// name", not "does that name bind in that context". It cannot see scope, usings, overloads or generic
    /// arguments; those are recorded and driven in
    /// <see cref="LiveSitesOutsideTheResolversReach"/> and <see cref="CrefShapesTheResolverAccepts"/>. What
    /// it does see exactly is the failure mode the issue describes: a rename or a deletion that leaves the
    /// old name spelled nowhere but in the doc comment pointing at it.</para>
    ///
    /// <para><b>The universe is code and not declarations, which is a choice and not a shortcut.</b> A
    /// declaration-position index is the obvious form and it is strictly worse here: a regex over
    /// declarations misses the ones this codebase actually writes — a tuple-returning static, a property
    /// whose type is a nested generic — and every miss is a spurious offender in a set compared for
    /// equality. Measured before choosing: a declaration index reported 385 unresolvable targets against
    /// this one's 31, and the extra 354 were dominated by real members it had failed to parse. An index that
    /// is too WIDE loses recall, which the recorded limits above state; one that is too NARROW manufactures
    /// offenders, and a guard that cries wolf gets its allow-list widened until it says nothing.</para>
    /// </summary>
    [Fact]
    public void EveryCrefTargetResolvesOrIsBounded()
    {
        var root = RepoRootOrFail();
        var projects = SolutionProjectDirectories(root);
        var census = BuildCrefCensus(RepoSources(root), projects);

        /* The floors first, and inside this test rather than only in the one below it: a floor that lives
           somewhere else does not protect THIS assertion, and this is the assertion that reports zero
           offenders when the scan read nothing. */
        AssertCrefPopulationFloors(census, projects, SolutionProjectsWithoutCrefs.Keys);

        var unresolved = UnresolvedTargets(census);

        /* The two directions are asserted separately first, because each needs a different instruction and
           a collection diff gives neither. The equality below then holds them together, so a later edit
           cannot leave one direction as the only gate. */
        var appeared = unresolved.Keys.Except(UnresolvedCrefTargets.Keys, StringComparer.Ordinal).ToArray();

        Assert.True(appeared.Length == 0,
            "cref target(s) that resolve to nothing this repository spells:\n\n"
            + string.Join("\n", appeared.Select(t =>
                $"  {t}\n" + string.Join("\n", unresolved[t].Select(s => $"      {s.Path}:{s.Line}"))))
            + "\n\nA cref names a symbol, so the usual cause is a rename or a deletion that left the "
            + "reference behind, and the fix is to point it at what the text now means — or to make it "
            + "<c>prose</c> if it never named a symbol at all, which is what a cref spelling a FILE name "
            + "(Type.Partial.cs) always is.\n\n"
            + "If the target is genuinely OUTSIDE this repository and no C# here spells it, add it to "
            + "UnresolvedCrefTargets with the OUTSIDE kind and say why the name appears in no code — that "
            + "is this resolver's stated boundary and not a defect. Do NOT add a real dangling reference "
            + "there to make the build green; the two kinds are what the list is for.");

        var gone = UnresolvedCrefTargets.Keys.Except(unresolved.Keys, StringComparer.Ordinal).ToArray();

        Assert.True(gone.Length == 0,
            "UnresolvedCrefTargets still lists cref target(s) that now resolve:\n\n  "
            + string.Join("\n  ", gone)
            + "\n\nDelete the entries. The list is the inventory of what is still broken, compared for "
            + "equality precisely so a fixed reference does not leave a permission behind (#3075). If a "
            + "target resolves because it stopped being written anywhere rather than because it was "
            + "fixed, the entry still goes.");

        Assert.Equal(
            UnresolvedCrefTargets.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray(),
            unresolved.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray());
    }

    /// <summary>
    /// The population floors on their own, named so a reader can see what stops this family reporting a
    /// clean tree it never read.
    ///
    /// <para><b>Per solution project, not per repository.</b> A single total is satisfied by one large
    /// tree: <c>Darling</c> alone carries most of the crefs here, so a walk that silently stopped reading
    /// <c>PerformanceMonitor.Collectors</c> or either SKU's app tree would still clear any global floor by a
    /// wide margin. That is the shape #3067 was filed for and the one this class's own sweep is most
    /// exposed to, since it walks the repository root rather than a named list of trees.</para>
    ///
    /// <para><b>The project list is DERIVED from <c>PerformanceMonitor.sln</c>, not written here.</b> A
    /// hand-written list of trees is the same restatement this guard exists to catch: it goes stale the
    /// moment a project is added, and it goes stale silently, because a floor over the trees you remembered
    /// passes. Deriving it also means this file names no other SKU's directory as a literal, so it adds no
    /// cross-app reference for <c>CrossAppGuardCiGateTests</c> to reach or exempt.</para>
    /// </summary>
    [Fact]
    public void ThePopulationIsFlooredPerSolutionProject()
    {
        var root = RepoRootOrFail();
        var projects = SolutionProjectDirectories(root);

        AssertCrefPopulationFloors(
            BuildCrefCensus(RepoSources(root), projects), projects, SolutionProjectsWithoutCrefs.Keys);
    }

    /// <summary>
    /// The derived project list is the whole solution and the solution is the whole repository bar a named
    /// pair — so the floor above is aimed at every tree rather than at whichever ones the parse happened to
    /// return.
    ///
    /// <para>A parse that quietly returned a subset is the "floor aimed at the wrong tree" failure with an
    /// extra step, and it would look exactly like health: fewer projects to floor, all of them passing.</para>
    /// </summary>
    [Fact]
    public void TheDerivedProjectListCoversEveryProjectInTheTree()
    {
        var root = RepoRootOrFail();
        var solution = SolutionProjectDirectories(root);

        Assert.All(solution, project => Assert.True(
            Directory.Exists(Path.Combine(root, project.Replace('/', Path.DirectorySeparatorChar))),
            $"the solution names project directory '{project}', which does not exist — the parse has "
            + "produced something that is not a path, and the floor is then aimed at nothing."));

        var onDisk = Directory.EnumerateFiles(root, "*.csproj", SearchOption.AllDirectories)
            .Select(p => Path.GetRelativePath(root, p).Replace('\\', '/'))
            .Where(p => !HasBuildOutputSegment(p))
            .Select(p => p[..p.LastIndexOf('/')])
            .Distinct(StringComparer.Ordinal)
            .ToHashSet(StringComparer.Ordinal);

        Assert.Equal(
            ProjectDirectoriesOutsideTheSolution.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray(),
            onDisk.Except(solution, StringComparer.Ordinal).OrderBy(k => k, StringComparer.Ordinal).ToArray());

        Assert.Empty(solution.Except(onDisk, StringComparer.Ordinal));
    }

    /// <summary>
    /// The floors really do fail on a starved population, and they fail PER PROJECT — each of the three
    /// counts, for each project in turn, is individually load-bearing.
    ///
    /// <para>Driven on a synthetic census rather than the tree, because the claim is that removing one
    /// project's contribution reds, and the tree cannot be asked to lose one. The real tree's own pass is
    /// asserted above; without this, that pass could not tell a working floor from a vacuous one.</para>
    /// </summary>
    [Fact]
    public void TheFloorsFailOnAStarvedPopulation()
    {
        var projects = new[] { "AppOne", "AppTwo/Nested" };

        (string Path, string Text) Source(string project, bool docBlock, bool cref) =>
            ($"{project}/File.cs",
                (docBlock ? "/// <summary>" + (cref ? " <see cref=\"Marker\"/>" : string.Empty) + "</summary>\n"
                          : string.Empty)
                + "class Marker { }");

        /* The control: all three counts present for both projects, and the floors pass. Without it every
           throw below could be the fixture rather than the floor. */
        var whole = projects.Select(p => Source(p, docBlock: true, cref: true)).ToArray();
        var none = Array.Empty<string>();

        AssertCrefPopulationFloors(BuildCrefCensus(whole, projects), projects, none);

        Assert.ThrowsAny<Exception>(() => AssertCrefPopulationFloors(
            BuildCrefCensus(Array.Empty<(string, string)>(), projects), projects, none));

        /* And the list of projects itself: an empty one makes every per-project floor below iterate over
           nothing, which is the shape a mis-parsed solution file produces. */
        Assert.ThrowsAny<Exception>(() => AssertCrefPopulationFloors(
            BuildCrefCensus(whole, projects), Array.Empty<string>(), none));

        var starved = 0;
        foreach (var project in projects)
        {
            /* (a) the project contributes no FILE — the sweep lost a tree. */
            Assert.ThrowsAny<Exception>(() => AssertCrefPopulationFloors(
                BuildCrefCensus(whole.Where(s => !s.Path.StartsWith(project, StringComparison.Ordinal)), projects),
                projects,
                none));

            /* (b) it contributes files but no DOC BLOCK — the doc-comment collector stopped collecting. */
            Assert.ThrowsAny<Exception>(() => AssertCrefPopulationFloors(
                BuildCrefCensus(
                    projects.Select(p => Source(p, docBlock: p != project, cref: true)),
                    projects),
                projects,
                none));

            /* (c) doc blocks but no CREF — the extractor stopped extracting. This is the one a global
                   count cannot see, because the other project still supplies thousands. */
            Assert.ThrowsAny<Exception>(() => AssertCrefPopulationFloors(
                BuildCrefCensus(
                    projects.Select(p => Source(p, docBlock: true, cref: p != project)),
                    projects),
                projects,
                none));

            starved++;
        }

        Assert.Equal(projects.Length, starved);
    }

    /// <summary>
    /// The extractor isolates one target instead of absorbing the ones after it, and it reads a
    /// REFERENCE rather than the attribute spelling wherever it appears.
    ///
    /// <para>Both rejected patterns are run over the same input and the difference asserted, because
    /// neither fails obviously. The greedy value group still returns a match, and its one enormous target
    /// still fails to resolve — so the tree would report ONE unresolvable entry per doc block instead of
    /// the real set, the allow-list would be rewritten to match, and the guard would go green having
    /// stopped resolving crefs altogether. The attribute-only anchor is worse in the other direction: it
    /// reads prose ABOUT a cref as a cref, which this very class writes.</para>
    /// </summary>
    [Fact]
    public void TheCrefExtractorDoesNotAbsorbTheTokenUnderTest()
    {
        const string two = "<see cref=\"AlphaTarget\"/> and <see cref=\"BetaTarget\"/>";

        Assert.Equal(
            new[] { "AlphaTarget", "BetaTarget" },
            CrefReference.Matches(two).Select(m => m.Groups["target"].Value).ToArray());

        var greedy = new Regex(@"<(?<element>[A-Za-z]+)\b[^>]*?cref\s*=\s*""(?<target>.*)""");

        Assert.Equal(
            new[] { "AlphaTarget\"/> and <see cref=\"BetaTarget" },
            greedy.Matches(two).Select(m => m.Groups["target"].Value).ToArray());

        /* The element anchor, in both prose shapes this repository writes: inside <c> and escaped. */
        const string prose =
            "an attribute named in prose, <c>cref=\"ProseNotAReference\"</c>, and an escaped example, "
            + "<c>&lt;see cref=\"EscapedNotAReference\"/&gt;</c>";

        Assert.Empty(CrefReference.Matches(prose));

        var attributeOnly = new Regex(@"cref\s*=\s*""(?<target>[^""]*)""");

        Assert.Equal(
            new[] { "ProseNotAReference", "EscapedNotAReference" },
            attributeOnly.Matches(prose).Select(m => m.Groups["target"].Value).ToArray());
    }

    /// <summary>
    /// Which doc elements are found carrying a cref, compared for equality against
    /// <see cref="CrefCarryingElements"/>.
    ///
    /// <para>The extractor captures the element name rather than matching a list of them, so a new element
    /// kind cannot be MISSED — this pin exists so it cannot be missed silently either. A red here is one
    /// line of prose and a check that the new element's crefs are being read the way its owner expects.</para>
    /// </summary>
    [Fact]
    public void TheElementsCarryingACrefAreTheOnesThisExtractorKnows()
    {
        var root = RepoRootOrFail();
        var census = BuildCrefCensus(RepoSources(root), SolutionProjectDirectories(root));

        Assert.Equal(
            CrefCarryingElements.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray(),
            census.Sites.Select(s => s.Element).Distinct(StringComparer.Ordinal)
                .OrderBy(k => k, StringComparer.Ordinal).ToArray());
    }

    /// <summary>
    /// The population is <c>///</c> doc comments and nothing else, so a cref written inside a
    /// <c>/* … */</c> block comment or a string literal is not a reference and is not resolved.
    ///
    /// <para><b>Exercised on the tree as well as on the fixture, which is the half that would rot.</b> This
    /// repository really does discuss crefs inside block comments — one such comment exists precisely
    /// because a scanner once matched a cref out of one and reported a phantom call site — and the text
    /// there carries an ellipsis where a parameter list belongs, so admitting block comments to the
    /// population would put a target in the offender set that is not a reference at all. The tree-wide
    /// assertion below compares the two populations and requires the filter to be doing work on real
    /// input.</para>
    /// </summary>
    [Fact]
    public void TheCrefPopulationIsDocCommentsOnly()
    {
        const string fixture = """
            /* A block comment discussing <see cref="Absent.FromTheDocPopulation(…)"/>, which is prose
               about a reference rather than a reference. */
            /// <summary>A doc comment carrying <see cref="PresentInTheDocPopulation"/>.</summary>
            void M()
            {
                Log("a literal mentioning <see cref=\"AlsoAbsent\"/>");
            }
            """;

        Assert.Equal(
            new[] { "PresentInTheDocPopulation" },
            DocCommentBlocks(fixture.Split('\n'))
                .SelectMany(b => CrefReference.Matches(string.Join("\n", b.Select(l => l.Text))))
                .Select(m => m.Groups["target"].Value)
                .ToArray());

        var root = RepoRootOrFail();
        var sources = RepoSources(root).ToArray();
        var inDocComments = BuildCrefCensus(sources, SolutionProjectDirectories(root)).Sites.Count;
        var anywhere = sources.Sum(s => CrefReference.Matches(s.Text).Count);

        Assert.True(anywhere > inDocComments,
            $"every cref-shaped reference in the tree is inside a /// doc comment ({anywhere} found either "
            + $"way, {inDocComments} in doc comments), so the doc-comment filter is not discriminating "
            + "anything on real input and only the fixture above says it works. If the last such site was "
            + "genuinely reformatted away, delete this assertion and say so — do not weaken it to >=, "
            + "which is satisfied by a filter that does nothing.");
    }

    /// <summary>
    /// A <c>///</c> line inside a string LITERAL is not a doc comment, and a doc comment carrying quotes
    /// in its prose still is one.
    ///
    /// <para>Both halves, because the fix for the first can break the second. Blanking too much takes the
    /// doc comments with the literals and the population empties — which the floors catch. Blanking too
    /// little reads a test fixture's crefs as references in the tree, which nothing else here catches: the
    /// targets are arranged names, they resolve against nothing, and they arrive in the offender set
    /// looking exactly like real dangling references.</para>
    /// </summary>
    [Fact]
    public void ADocCommentInsideAStringLiteralIsNotPartOfThePopulation()
    {
        const string fixture = """"
            /// <summary>A real doc comment whose prose quotes "a string" and names
            /// <see cref="RealReference"/>.</summary>
            void M()
            {
                const string arranged = """
                    /// <summary>Arranged test data naming <see cref="ArrangedReference"/>.</summary>
                    """;
            }
            """";

        var found = DocCommentBlocks(WithStringLiteralsBlanked(fixture).Split('\n'))
            .SelectMany(b => CrefReference.Matches(string.Join("\n", b.Select(l => l.Text))))
            .Select(m => m.Groups["target"].Value)
            .ToArray();

        Assert.Equal(new[] { "RealReference" }, found);

        /* Without the blanking, the arranged data is read as a reference — the defect, reproduced. */
        Assert.Equal(
            new[] { "RealReference", "ArrangedReference" },
            DocCommentBlocks(fixture.Split('\n'))
                .SelectMany(b => CrefReference.Matches(string.Join("\n", b.Select(l => l.Text))))
                .Select(m => m.Groups["target"].Value)
                .ToArray());

        /* And through the census, which is the part that ships. A blanking helper the sweep does not call
           is the shape DocCommentHygieneTests already warns about one rule up: a predicate nothing calls
           leaves the rule reading everything, and this pin would pass either way. */
        Assert.Equal(
            new[] { "RealReference" },
            BuildCrefCensus(new[] { ("AppOne/File.cs", fixture) }, new[] { "AppOne" })
                .Sites.Select(s => s.Target).ToArray());
    }

    /// <summary>
    /// A cref is read from the whole doc block, so one that WRAPS across two <c>///</c> lines is extracted
    /// rather than missed — in both places it can wrap.
    ///
    /// <para>The obvious form is a per-line regex, and its failure is silent in the worst direction: the
    /// reference simply leaves the population, never resolved and never reported. XML collapses the line
    /// break to whitespace and C# permits whitespace around the dot, so the compiler resolves such a cref;
    /// this resolver has to see the same target or it is answering about a different population.</para>
    ///
    /// <para>Both shapes exist in this repository today and both are asserted on the tree: one cref has its
    /// TARGET split across the break, and several have the break between <c>&lt;see</c> and its
    /// <c>cref</c> attribute.</para>
    /// </summary>
    [Fact]
    public void TheCrefExtractorReadsAcrossTheLineBreakInsideOneDocBlock()
    {
        static string[] TargetsIn(string block) => DocCommentBlocks(block.Split('\n'))
            .SelectMany(b => CrefReference.Matches(string.Join("\n", b.Select(l => l.Text))))
            .Select(m => m.Groups["target"].Value)
            .ToArray();

        /* (i) the target itself wraps. */
        const string splitTarget = """
            /// <summary>A target long enough to wrap: <see cref="WrappedTargetType.
            /// WrappedTargetMember"/>.</summary>
            """;

        var wrapped = TargetsIn(splitTarget);
        Assert.Single(wrapped);
        Assert.Equal(new[] { "WrappedTargetType", "WrappedTargetMember" }, CrefSegments(wrapped[0]));

        /* (ii) the break falls between the element and its attribute. */
        const string splitElement = """
            /// <summary>Prose long enough that the element wraps away from its attribute (<see
            /// cref="WrappedElementTarget"/>).</summary>
            """;

        Assert.Equal(new[] { "WrappedElementTarget" }, TargetsIn(splitElement));

        /* The per-line form, for the difference: it sees neither reference. */
        Assert.Empty(splitTarget.Split('\n').SelectMany(l => CrefReference.Matches(l)));
        Assert.Empty(splitElement.Split('\n').SelectMany(l => CrefReference.Matches(l)));

        var root = RepoRootOrFail();
        var sources = RepoSources(root).ToArray();
        var census = BuildCrefCensus(sources, SolutionProjectDirectories(root));

        Assert.Contains(census.Sites, site => site.Target.Contains('\n', StringComparison.Ordinal));

        var perDocLine = sources.Sum(s => s.Text.Split('\n')
            .Where(l => l.TrimStart().StartsWith("///", StringComparison.Ordinal))
            .Sum(l => CrefReference.Matches(l).Count));

        Assert.True(census.Sites.Count > perDocLine,
            $"reading whole doc blocks found no more crefs than reading single lines would "
            + $"({census.Sites.Count} against {perDocLine}), so no cref in the tree wraps and only the "
            + "fixtures above say the joining works.");
    }

    /// <summary>
    /// The reductions in <see cref="CrefSegments"/> cannot repair a malformed target into one that
    /// resolves — the way this guard is likeliest to pass while broken, since every reduction it performs
    /// is a chance to normalise away the shape it exists to catch.
    ///
    /// <para>Each case names a real repository symbol, so the only reason it can fail to resolve is the
    /// malformation. A reduction that swallowed the malformation would make the case pass, and the positive
    /// controls make sure a resolver that simply says NO to everything cannot pass instead.</para>
    /// </summary>
    [Theory]
    /* Positive controls: these must resolve, or every negative below is vacuous. */
    [InlineData(true, "CollectorCatalog")]
    [InlineData(true, "CollectorCatalog.AppliesTo")]
    [InlineData(true, "T:CollectorCatalog")]
    [InlineData(true, "M:CollectorCatalog.AppliesTo")]
    /* The one character that decides twelve of the entries above. !: is the compiler's own marker for a
       reference it could not resolve; accepting it as a kind prefix would resolve every one of them. */
    [InlineData(false, "!:CollectorCatalog")]
    [InlineData(false, "!:CollectorCatalog.AppliesTo")]
    /* Whitespace is trimmed at a segment's EDGES, because XML puts it there when a cref wraps. Inside a
       segment it is not a legal identifier and must stay illegal. */
    [InlineData(true, "CollectorCatalog. AppliesTo")]
    [InlineData(false, "Collector Catalog")]
    /* An empty segment: a doubled or trailing dot names nothing. */
    [InlineData(false, "CollectorCatalog..AppliesTo")]
    [InlineData(false, "CollectorCatalog.")]
    [InlineData(false, ".CollectorCatalog")]
    [InlineData(false, "")]
    [InlineData(false, ".")]
    /* An unclosed generic argument list is NOT stripped, so the brace stays and the segment stays
       illegal. A balanced-anything pattern would have deleted it and resolved this. */
    [InlineData(false, "CollectorCatalog{TRow")]
    [InlineData(true, "CollectorCatalog{TRow}")]
    /* Nullable annotations and stray punctuation are not identifiers and are not stripped. */
    [InlineData(false, "CollectorCatalog?")]
    [InlineData(false, "CollectorCatalog!")]
    /* A prefix letter that is not one of the documented kinds is part of the name, not a prefix. */
    [InlineData(false, "Z:CollectorCatalog")]
    public void TheResolverCannotLaunderAMalformedTarget(bool resolves, string target)
    {
        var root = RepoRootOrFail();
        var census = BuildCrefCensus(RepoSources(root), SolutionProjectDirectories(root));

        Assert.True(census.CodeIdentifiers.Contains("CollectorCatalog"),
            "the identifier universe does not contain a type this repository certainly declares, so every "
            + "case here would fail for the wrong reason.");

        Assert.True(
            CrefResolves(target, census.CodeIdentifiers) == resolves,
            $"'{target}' should{(resolves ? " " : " NOT ")}resolve. A reduction that repairs a malformed "
            + "target into a resolvable one is how this whole family passes while resolving nothing.");
    }

    /// <summary>
    /// The identifier universe is built from CODE and reports NO for a name that only prose spells — so the
    /// resolver is neither a stub that says yes to everything nor one that reads its own doc comments back.
    ///
    /// <para>Both halves matter. A universe built without stripping comments would contain every name ever
    /// mentioned, including the names in this class's own allow-list prose, and every dangling reference
    /// would resolve against the comment that names it. A universe that came back empty would fail
    /// everything, which the population floors catch — but a universe built over the WRONG text would not
    /// be empty and nothing else here would notice.</para>
    /// </summary>
    [Fact]
    public void TheIdentifierUniverseComesFromCodeAndNotFromProse()
    {
        var census = BuildCrefCensus(
            new[]
            {
                ("AppOne/File.cs", """
                    /// <summary>Prose naming OnlyInADocComment and <see cref="AlsoOnlyInACref"/>.</summary>
                    class OnlyInCode
                    {
                        /* OnlyInABlockComment */
                        const string S = "OnlyInALiteral";
                    }
                    """),
            },
            new[] { "AppOne" });

        Assert.Contains("OnlyInCode", census.CodeIdentifiers);
        Assert.DoesNotContain("OnlyInADocComment", census.CodeIdentifiers);
        Assert.DoesNotContain("AlsoOnlyInACref", census.CodeIdentifiers);
        Assert.DoesNotContain("OnlyInABlockComment", census.CodeIdentifiers);
        Assert.DoesNotContain("OnlyInALiteral", census.CodeIdentifiers);

        Assert.True(CrefResolves("OnlyInCode", census.CodeIdentifiers));
        Assert.False(CrefResolves("AlsoOnlyInACref", census.CodeIdentifiers));
    }

    /// <summary>
    /// The bounded set is compared for EQUALITY, in both directions — a target that stops being
    /// unresolvable forces its entry out, and one that starts being unresolvable fails.
    ///
    /// <para>The direction that needs the pin is the first. A containment check would leave a fixed
    /// reference's entry sitting in the list forever, and the list is the only inventory of what is still
    /// broken; an inventory that only ever grows stops being read. #3075 is the shape this follows.</para>
    /// </summary>
    [Fact]
    public void TheBoundedSetIsComparedForEquality()
    {
        var root = RepoRootOrFail();
        var projects = SolutionProjectDirectories(root);
        var census = BuildCrefCensus(RepoSources(root), projects);
        var real = UnresolvedTargets(census).Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray();

        void Compare(IEnumerable<string> expected) => Assert.Equal(
            expected.OrderBy(k => k, StringComparer.Ordinal).ToArray(),
            real);

        /* The control. */
        Compare(UnresolvedCrefTargets.Keys);

        /* (a) an entry whose reference was FIXED, still listed. Each key in turn, so no single entry is
               carrying the whole comparison. */
        var dropped = 0;
        foreach (var key in UnresolvedCrefTargets.Keys)
        {
            Assert.ThrowsAny<Exception>(() => Compare(UnresolvedCrefTargets.Keys.Where(
                k => !string.Equals(k, key, StringComparison.Ordinal))));
            dropped++;
        }

        Assert.Equal(UnresolvedCrefTargets.Count, dropped);

        /* (b) a new unresolvable target, unlisted. */
        Assert.ThrowsAny<Exception>(() => Compare(
            UnresolvedCrefTargets.Keys.Append("ANewlyRenamedAwayTargetName")));
    }

    /// <summary>
    /// Every bound in <see cref="UnresolvedCrefTargets"/> opens with a kind from
    /// <see cref="CrefBoundKinds"/>, and an unrecognised one FAILS rather than being counted as something
    /// else.
    ///
    /// <para>Also cross-checked against the one kind that is mechanically decidable: a target carrying the
    /// <c>!:</c> marker must be labelled <c>MARKER</c> and nothing else may be. A hand label that drifted
    /// from the mechanism would let a real defect be filed as this resolver's own blind spot, which is the
    /// direction that costs — it would move a fixable reference into the category the guard has decided not
    /// to look at.</para>
    /// </summary>
    [Fact]
    public void EveryBoundNamesARecognisedKind()
    {
        Assert.NotEmpty(UnresolvedCrefTargets);

        foreach (var (target, bound) in UnresolvedCrefTargets)
        {
            var label = CrefBoundKinds
                .Where(k => bound.StartsWith(k.Label, StringComparison.Ordinal))
                .Select(k => k.Label)
                .FirstOrDefault();

            Assert.True(label is not null,
                $"the bound for '{target}' opens with no recognised kind: "
                + $"\"{bound[..Math.Min(60, bound.Length)]}…\". Every entry has to name its kind, because "
                + "two of the three are defects awaiting repair and the third is this resolver's own "
                + "boundary, and nothing else distinguishes them. Add the kind to CrefBoundKinds if it is "
                + "genuinely new.");

            Assert.True(
                string.Equals(label, "MARKER", StringComparison.Ordinal)
                    == target.StartsWith("!:", StringComparison.Ordinal),
                $"'{target}' is labelled {label} but the !: marker "
                + $"{(target.StartsWith("!:", StringComparison.Ordinal) ? "is" : "is not")} present. That "
                + "label is decidable from the target, so a hand label that disagrees with it is wrong.");
        }

        Assert.All(SolutionProjectsWithoutCrefs.Values, bound => Assert.NotEmpty(bound));
        Assert.All(ProjectDirectoriesOutsideTheSolution.Values, bound => Assert.NotEmpty(bound));
    }

    /// <summary>
    /// Everything recorded as beyond this resolver's reach is DRIVEN — the accepted shapes against the real
    /// universe, and the live sites against the real census.
    ///
    /// <para>#3079's finding, applied here: three limitations recorded in prose turned out to be
    /// unobservable where they had been written, so the prose described a boundary nothing stood on. A
    /// limit that is asserted goes red when it stops being true, which is the only way anyone finds out
    /// that this resolver got sharper or blunter.</para>
    /// </summary>
    [Fact]
    public void TheRecordedLimitsAreExercised()
    {
        var root = RepoRootOrFail();
        var census = BuildCrefCensus(RepoSources(root), SolutionProjectDirectories(root));

        /* The accepted shapes. The third row is a negative control, so "everything resolves" cannot pass
           this. */
        foreach (var (target, limit) in CrefShapesTheResolverAccepts)
        {
            var expected = !limit.StartsWith("NOTHING", StringComparison.Ordinal);

            Assert.True(
                CrefResolves(target, census.CodeIdentifiers) == expected,
                $"'{target}' should{(expected ? " " : " NOT ")}resolve; its recorded limit says "
                + $"\"{limit[..Math.Min(40, limit.Length)]}…\". A recorded boundary that has moved has to "
                + "be re-stated, not left describing a resolver that no longer behaves that way.");
        }

        /* The live sites. Present in the census, and resolving — so both the site and the disagreement
           with the compiler are still real. */
        foreach (var (project, target, reason) in LiveSitesOutsideTheResolversReach)
        {
            var matches = census.Sites
                .Where(s => s.Path.StartsWith(project + "/", StringComparison.Ordinal))
                .Where(s => string.Equals(s.Target, target, StringComparison.Ordinal))
                .ToArray();

            Assert.True(matches.Length > 0,
                $"no cref to '{target}' is left under '{project}', so the boundary this row records "
                + $"({reason[..Math.Min(40, reason.Length)]}…) is no longer exercised anywhere. Find "
                + "another live site for it or delete the row.");

            Assert.All(matches, site => Assert.True(
                CrefResolves(site.Target, census.CodeIdentifiers),
                $"{site.Path}:{site.Line} no longer resolves here, so this row records a disagreement with "
                + "the compiler that has stopped existing."));
        }
    }

    /// <summary>
    /// Every <c>cref</c> target this resolver cannot resolve, keyed by the target text and carrying the
    /// sites it was read from — the value is what a failure message needs, and only the key is compared.
    /// </summary>
    private static SortedDictionary<string, List<CrefSite>> UnresolvedTargets(CrefCensus census)
    {
        var unresolved = new SortedDictionary<string, List<CrefSite>>(StringComparer.Ordinal);

        foreach (var site in census.Sites)
        {
            if (CrefResolves(site.Target, census.CodeIdentifiers))
            {
                continue;
            }

            if (!unresolved.TryGetValue(site.Target, out var sites))
            {
                sites = new List<CrefSite>();
                unresolved[site.Target] = sites;
            }

            sites.Add(site);
        }

        return unresolved;
    }

    /// <summary>
    /// The three population counts, globally and then per solution project. Separated from the rule so the
    /// starvation pin can drive it with a synthetic census, which reading the real tree could never
    /// show.
    /// </summary>
    private static void AssertCrefPopulationFloors(
        CrefCensus census, IReadOnlyList<string> projects, IEnumerable<string> withoutCrefs)
    {
        Assert.True(projects.Count > 0,
            "no solution project was derived, so the per-project floors below iterate over nothing and "
            + "every one of them passes vacuously.");

        Assert.True(census.ByProject.Values.Sum(c => c.Files) > 0,
            "the sweep scanned no files at all. Every rule reading this census reports a clean tree.");

        Assert.True(census.Sites.Count > 0,
            "the sweep extracted no cref from any file, so nothing is being resolved and the offender set "
            + "is empty for want of input rather than for want of offenders.");

        Assert.True(census.CodeIdentifiers.Count > 0,
            "the identifier universe is empty, so nothing could resolve and the offender set would be "
            + "every cref in the tree — loud, but for the wrong reason.");

        foreach (var project in projects)
        {
            census.ByProject.TryGetValue(project, out var counts);

            Assert.True(counts.Files > 0,
                $"the sweep read no .cs file under solution project '{project}'. A repository-wide total "
                + "is dominated by the largest tree, so a project dropped from the walk passes any global "
                + "floor while contributing nothing (#3067).");

            Assert.True(counts.DocBlocks > 0,
                $"the sweep found no /// doc-comment block under '{project}', so the doc-comment collector "
                + "read its files and produced nothing from them.");
        }

        Assert.Equal(
            withoutCrefs.OrderBy(k => k, StringComparer.Ordinal).ToArray(),
            projects
                .Where(p => !census.ByProject.TryGetValue(p, out var c) || c.Crefs == 0)
                .OrderBy(p => p, StringComparer.Ordinal)
                .ToArray());
    }

    /// <summary>
    /// One scan of <paramref name="sources"/>: the population per project, every cref, and the identifier
    /// universe. Takes its sources as text rather than reading the disk itself, so every pin above can
    /// drive it with an arranged population.
    /// </summary>
    private static CrefCensus BuildCrefCensus(
        IEnumerable<(string Path, string Text)> sources, IReadOnlyList<string> projects)
    {
        /* Longest project path first, so a project nested inside another claims its own files. Nothing in
           the solution is nested today; ordering it anyway means a later addition cannot silently be
           counted against its parent. */
        var byLength = projects.OrderByDescending(p => p.Length).ThenBy(p => p, StringComparer.Ordinal).ToArray();

        var counts = new Dictionary<string, (int Files, int DocBlocks, int Crefs)>(StringComparer.Ordinal);
        var sites = new List<CrefSite>();
        var identifiers = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (path, text) in sources)
        {
            var project = byLength.FirstOrDefault(
                p => path.StartsWith(p + "/", StringComparison.Ordinal));

            void Add(int files, int blocks, int crefs)
            {
                if (project is null)
                {
                    return;
                }

                counts.TryGetValue(project, out var current);
                counts[project] = (current.Files + files, current.DocBlocks + blocks, current.Crefs + crefs);
            }

            Add(1, 0, 0);

            /* String literals blanked FIRST. A /// line inside a raw-string fixture is indistinguishable
               from a real doc comment to a line-prefix collector. Blanking literal TEXT while leaving comments
               intact is the one transformation that separates them; StripCommentsAndStrings would take the
               doc comments too, and there would be nothing left to collect. */
            foreach (var block in DocCommentBlocks(WithStringLiteralsBlanked(text).Split('\n')))
            {
                Add(0, 1, 0);

                var joined = string.Join("\n", block.Select(l => l.Text));

                foreach (Match match in CrefReference.Matches(joined))
                {
                    /* The line the attribute OPENS on, which is where a reader has to go. Offsets are
                       cumulative over the joined block plus one character per newline, so the arithmetic
                       here and the join above cannot disagree. */
                    var offset = 0;
                    var line = block[0].Line;

                    foreach (var (candidate, content) in block)
                    {
                        if (match.Index <= offset + content.Length)
                        {
                            line = candidate;
                            break;
                        }

                        offset += content.Length + 1;
                    }

                    sites.Add(new CrefSite(
                        path, line, match.Groups["element"].Value, match.Groups["target"].Value));
                    Add(0, 0, 1);
                }
            }

            foreach (Match token in IdentifierToken.Matches(CSharpSourceWalker.StripCommentsAndStrings(text)))
            {
                identifiers.Add(token.Value);
            }
        }

        return new CrefCensus(counts, sites, identifiers);
    }

    /// <summary>
    /// <paramref name="text"/> with every string literal's BODY blanked to spaces and its line structure
    /// intact, so a <c>///</c>-prefixed line inside a literal is no longer <c>///</c>-prefixed.
    ///
    /// <para><b>Why this is not <see cref="CSharpSourceWalker.StripCommentsAndStrings"/>.</b> That one
    /// blanks comments as well, and the population this feeds IS the comments. The walk is the same walk,
    /// through <see cref="CSharpSourceWalker.StringLiteralBodies"/> — the mirror entry point — so a
    /// delimiter one of them understands cannot desynchronise the other.</para>
    ///
    /// <para><b>The defect it closes, found by running this guard rather than by reading it.</b> A
    /// line-prefix collector cannot tell a doc comment from a fixture that CONTAINS one, and the first
    /// version of the cref rule duly read the crefs out of its own test data and reported them as
    /// offenders in the tree.</para>
    ///
    /// <para><b>Where it is exercised, measured rather than assumed.</b> Nine <c>///</c> lines in the
    /// whole repository sit inside a string literal and every one of them is in THIS file — the fixtures
    /// below are the only live instance, and there were none before them. So the blanking is exercised
    /// where the guard can see it rather than somewhere it might be, which is the direction #3079 asked
    /// for; the arithmetic is a count the sweep produces, not a number written down here.</para>
    /// </summary>
    private static string WithStringLiteralsBlanked(string text)
    {
        var characters = text.ToCharArray();

        foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(text))
        {
            for (var i = 0; i < body.Length && start + i < characters.Length; i++)
            {
                if (characters[start + i] is not ('\n' or '\r'))
                {
                    characters[start + i] = ' ';
                }
            }
        }

        return new string(characters);
    }

    /// <summary>
    /// Every STRICTLY contiguous run of <c>///</c> lines, with each line's number and its text after the
    /// <c>///</c>.
    ///
    /// <para><b>Strictly contiguous, unlike <see cref="DocRuns"/>, and the difference is deliberate.</b>
    /// That one continues a run across attribute lines because it is answering "which lines document this
    /// member" (#2445). This one is answering "which lines are one XML fragment", which is what the
    /// compiler parses a cref out of — an attribute line between two doc blocks separates two fragments,
    /// and joining them could weld a target out of text that no parser ever sees together.</para>
    /// </summary>
    private static IEnumerable<IReadOnlyList<(int Line, string Text)>> DocCommentBlocks(string[] lines)
    {
        var block = new List<(int Line, string Text)>();

        for (var i = 0; i <= lines.Length; i++)
        {
            var trimmed = i < lines.Length ? lines[i].TrimStart() : string.Empty;

            if (i < lines.Length && trimmed.StartsWith("///", StringComparison.Ordinal))
            {
                block.Add((i + 1, trimmed[3..]));
                continue;
            }

            if (block.Count > 0)
            {
                yield return block;
                block = new List<(int Line, string Text)>();
            }
        }
    }

    /// <summary>
    /// A cref target reduced to the dot-separated names it asserts exist. Every reduction is here, in one
    /// place, so <see cref="TheResolverCannotLaunderAMalformedTarget"/> can drive all of them at once.
    ///
    /// <para>Reductions, in order: one documented kind prefix from <see cref="DocIdKindPrefixes"/>; the
    /// parameter list from the first <c>(</c> onward; each balanced generic argument list; and whitespace at
    /// each segment's edges, which is what XML leaves behind when a cref wraps across two lines. Nothing
    /// else is touched — no punctuation is deleted, no brace is repaired, no interior whitespace is
    /// collapsed — because each of those would turn a malformed target into a resolvable one, and this
    /// resolver exists to notice malformed targets.</para>
    /// </summary>
    private static string[] CrefSegments(string target)
    {
        var text = target;

        foreach (var prefix in DocIdKindPrefixes)
        {
            if (text.StartsWith(prefix, StringComparison.Ordinal))
            {
                text = text[prefix.Length..];
                break;
            }
        }

        var parameters = text.IndexOf('(');
        if (parameters >= 0)
        {
            text = text[..parameters];
        }

        text = GenericArgumentList.Replace(text, string.Empty);

        return text.Split('.').Select(s => s.Trim()).ToArray();
    }

    /// <summary>
    /// Whether every segment of <paramref name="target"/> is an identifier that
    /// <paramref name="identifiers"/> holds.
    /// </summary>
    private static bool CrefResolves(string target, IReadOnlySet<string> identifiers)
    {
        var segments = CrefSegments(target);

        return segments.Length > 0
               && segments.All(s => WholeIdentifier.IsMatch(s) && identifiers.Contains(s));
    }

    /// <summary>
    /// Every project directory the solution builds, repo-relative and forward-slashed, derived from
    /// <c>PerformanceMonitor.sln</c> rather than written down.
    /// </summary>
    private static IReadOnlyList<string> SolutionProjectDirectories(string root)
    {
        var solution = File.ReadAllText(Path.Combine(root, "PerformanceMonitor.sln"));

        var directories = Regex.Matches(solution, "\"([^\"]+\\.csproj)\"")
            .Select(m => m.Groups[1].Value.Replace('\\', '/'))
            .Where(p => p.Contains('/', StringComparison.Ordinal))
            .Select(p => p[..p.LastIndexOf('/')])
            .Distinct(StringComparer.Ordinal)
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToArray();

        Assert.NotEmpty(directories);

        return directories;
    }

    /// <summary>
    /// Every source file under <paramref name="root"/>, as a repo-relative forward-slashed path and its
    /// text. The same set <see cref="SourceFiles"/> hands the other two rules, so a path excluded for one
    /// is excluded for all three.
    /// </summary>
    private static IEnumerable<(string Path, string Text)> RepoSources(string root)
    {
        foreach (var file in SourceFiles(root))
        {
            yield return (Path.GetRelativePath(root, file).Replace('\\', '/'), File.ReadAllText(file));
        }
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
    /// output. Both rules scan the same set, so a path either rule should ignore is excluded in one
    /// place — and the same set wherever they run, which is <see cref="HasBuildOutputSegment"/>'s job.
    /// </summary>
    private static IEnumerable<string> SourceFiles(string root)
    {
        foreach (var file in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
        {
            if (IsBuildOutput(root, file))
            {
                continue;
            }

            yield return file;
        }
    }

    /// <summary>
    /// True when <paramref name="path"/> is generated build output rather than source.
    ///
    /// <para>Judged RELATIVE to <paramref name="root"/> rather than on the absolute path, because the scan
    /// walks a whole repository from wherever it is checked out: a directory ABOVE the root called
    /// <c>bin</c> would otherwise classify every file in the tree as build output, and both rules would
    /// then report no offenders because they had read nothing.</para>
    /// </summary>
    private static bool IsBuildOutput(string root, string path) =>
        HasBuildOutputSegment(Path.GetRelativePath(root, path));

    /// <summary>
    /// True when any whole SEGMENT of <paramref name="path"/> is <c>bin</c> or <c>obj</c>, reading both
    /// separator characters on every platform.
    ///
    /// <para><b>Both separators, not the host's.</b> A substring test for <c>\bin\</c> matches nothing
    /// where the separator is <c>/</c> — macOS and Linux — so the two rules above would read every
    /// generated <c>.AssemblyInfo.cs</c> and <c>.g.cs</c> there and skip them on Windows, and a guard whose
    /// scope depends on the host is two guards. Splitting on
    /// <see cref="Path.DirectorySeparatorChar"/> alone relocates that asymmetry rather than removing it: it
    /// answers NO to a Windows-shaped path off Windows, which leaves the platform-independence pin below
    /// unstatable in a form that means one thing everywhere. Splitting on both makes the verdict a property
    /// of the path.</para>
    ///
    /// <para><b>Whole segments, not a substring.</b> <c>Objects</c>, <c>obj-cache</c> and <c>mybin</c> are
    /// source directories, and a <c>Contains("obj")</c> would quietly stop scanning them.</para>
    /// </summary>
    private static bool HasBuildOutputSegment(string path) =>
        path.Split('/', '\\')
            .Any(s => string.Equals(s, "bin", StringComparison.OrdinalIgnoreCase)
                      || string.Equals(s, "obj", StringComparison.OrdinalIgnoreCase));

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
