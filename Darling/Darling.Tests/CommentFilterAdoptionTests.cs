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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Which source-scanning pins separate code from comment by LINE PREFIX, and what bound each one rests on
/// (#3052).
///
/// <para><b>Why the shape needs watching at all.</b> Dropping lines that start with <c>//</c>, <c>///</c>,
/// <c>/</c> or <c>*</c> works for line comments and doc comments, which are prefixed on every line. It does
/// not work for block comments, because this codebase does not put an asterisk on the continuation
/// lines:</para>
/// <code>
/// /* Released here rather than at the closing brace: the six sub-tab loads at the end of this method
///    run after these five have finished, so they do not contend with them. */
/// </code>
/// <para>Only the first line starts with <c>/</c>. Every line after it is indented prose that a prefix
/// filter hands to the scanner as code, so a comment mentioning the thing being banned becomes an
/// offender.</para>
///
/// <para><b>The shape is not banned, because legitimate uses of it exist.</b> Collecting a <c>///</c> run is
/// not the same act as excluding comments from a code scan — a doc comment IS prefixed on every line, so the
/// prefix is the definition of the thing being gathered rather than an approximation of it. Reading a bound
/// off a named file is legitimate too, when the bound is stated and measured. And a filter over SQL text is
/// not this problem at all, because the C# walk cannot read SQL. So each site is listed with its reason and
/// the build re-derives the set, the same agreement
/// <see cref="CommandDeadlineScannerAdoptionTests"/> holds over the command-timeout family.</para>
///
/// <para><b>The failure direction, since it decides how much this is worth.</b> Every instance #3052 found
/// UNDER-strips: prose reads as code, the offender list gains an entry that is not one, and an
/// <c>Assert.Empty</c> fails loudly. That is the safe direction — a spurious red, not a hidden offender. The
/// instance that actually cost something was not an assertion: during #3047's review a check of this shape
/// reported 45 changed lines in a comments-only diff, nothing went red because nothing was asserting, and
/// the number nearly produced a wrong verdict about whether that PR touched code. <b>The same filter is loud
/// when it drives an assertion and silent when it drives a number</b>, and the silent half lives in ad-hoc
/// review checks that no test can reach. This guard covers the half that is committed.</para>
///
/// <para><b>No <c>build.yml</c> filter entry, deliberately.</b> This reads BOTH test projects, so a
/// <c>Lite.Tests</c>-only change fires <c>lite</c> and not <c>darling</c>, and the "Run Darling tests" step
/// is skipped. Naming <c>Lite.Tests/**</c> in the build job's <c>darling</c> filter would fix that and buy
/// nothing: what that filter decides is which products get COMPILED AND PUBLISHED, so every file in Lite's
/// test project would gate a Darling Viewer build and both Darling publishes, while the suite that reads
/// them runs on such a change either way. It belongs instead in the category <c>build.yml</c>'s
/// <c>darling-tree-guards</c> job already exists for — guards whose input is the repository rather than a
/// path, like <c>FleetIdentifierScrubTests</c> — and that job runs this suite exactly when the build job's
/// step reports <c>skipped</c>. The <c>darling-pg</c> job is not part of that trade: it declares its OWN
/// <c>darling</c> filter, naming only <c>Darling/**</c> and the two workflow files, so no entry added to
/// the build job's filter can stand up its throwaway cluster. <c>CrossAppGuardCiGateTests</c> does see the
/// two <c>Lite.Tests</c> keys below — #3067 widened its anchor past the app directory, since
/// <c>Lite.Tests</c> is a sibling of <c>Lite</c> rather than a directory inside it — and exempts them on
/// this same reasoning, under the same bound.</para>
/// </summary>
public sealed class CommentFilterAdoptionTests
{
    /// <summary>
    /// The prefixes that mean "this line is a comment" in C#. <c>*</c> and <c>/</c> are here because the
    /// existing sites use them: <c>*</c> catches a banner-style continuation and <c>/</c> catches <c>//</c>,
    /// <c>///</c> and <c>/*</c> at once.
    /// </summary>
    private static readonly string[] s_commentPrefixes = { "//", "///", "/*", "*", "/" };

    /// <summary>
    /// Every pin allowed to split code from comment by line prefix, and the bound it rests on. An
    /// enumeration rather than a wildcard, for the reason the exemption list in
    /// <see cref="AnalysisPassTokenThreadingTests"/> gives: the way a wildcard grows is that nobody has to
    /// name a new entry.
    ///
    /// <para>Four kinds live here and they are not the same kind. Four <b>collect</b> a doc-comment run,
    /// where the prefix is what defines the run. One reads a <b>stated, measured</b> bound off a named file.
    /// One filters <b>SQL</b>, which the C# walk cannot help with. One <b>demonstrates</b> the shape on an
    /// arranged fixture, which is this file.</para>
    /// </summary>
    private static readonly Dictionary<string, string> s_bounded = new(StringComparer.Ordinal)
    {
        ["Darling.Tests/CommentFilterAdoptionTests.cs"] =
            "DEMONSTRATES the shape. The control below builds the prefix filter deliberately, over an "
            + "arranged fixture, to show that it reads a block comment's body as code and that the walk does "
            + "not. Written with the literal rather than with s_commentPrefixes on purpose: routing it "
            + "through the array would slip past this guard's own detector, and a guard that demonstrates "
            + "the loophole in its stated bound is worse than one that names itself. The cost of the entry "
            + "is that a REAL prefix filter added to this file would not be noticed.",

        ["Darling.Tests/AnalysisPassTokenThreadingTests.cs"] =
            "COLLECTS a doc run. DocBlockAbove gathers the contiguous /// block above a declaration to read the "
            + "#2443 exemption marker out of it. A doc comment is prefixed on every line by definition, and the "
            + "marker lives IN a comment, so stripping comments here is the one thing that would break it. The "
            + "regexes in this file read the walked code.",

        ["Lite.Tests/AnalysisPassTokenThreadingTests.cs"] =
            "COLLECTS a doc run. The Lite twin of the above, same helper, same reason.",

        ["Darling.Tests/DocCommentHygieneTests.cs"] =
            "COLLECTS a doc run. DocRuns groups contiguous /// lines into the run that documents one member, "
            + "and DocCommentBlocks groups the strictly contiguous ones into the one XML fragment a cref is "
            + "parsed out of (#3083). Both are the subject of that class rather than an approximation of "
            + "comment-stripping - and its cref rule DOES ask for the walker, for the identifier universe it "
            + "resolves targets against.",

        ["Darling.Tests/RefreshCeilingProvenancePinTests.cs"] =
            "COLLECTS a doc run. DocProseFor walks the contiguous /// run above a named declaration and "
            + "collapses it, so that a pattern can span the wrapping of the prose it reads figures out of. "
            + "The prefix defines the run rather than approximating comment-stripping, and asking for the "
            + "walker would leave nothing to read - the figures being pinned live only in comments. Stated "
            + "bound: the walk STOPS at the first line that is not /// -prefixed, so a block comment between "
            + "the doc run and its declaration would truncate it. That direction is loud rather than quiet, "
            + "because the collected prose is then asserted to open at <summary> and close at </summary> "
            + "before any figure is compared - a truncated walk fails there instead of silently pinning half "
            + "a comment.",

        ["Lite.Tests/LiteSidebarDotRendersTheCardStatusTests.cs"] =
            "STATED BOUND, and asking for the walker would BREAK it. Its doc comment records the measurement: "
            + "ServerConnection.cs carries exactly one block comment, the licence header, so dropping "
            + "//-prefixed lines cannot eat a literal. It then asserts that four status WORDS appear in no "
            + "string literal there - so blanking literal text would satisfy Assert.DoesNotContain vacuously, "
            + "which is a pass for the wrong reason. Measurement-dependent, not permanent: a second block "
            + "comment in that file makes the bound wrong.",

        ["Darling.Tests/Pg18IoBytesTests.cs"] =
            "NOT C#. The filter runs over DarlingPgIoReader.PgIoSql - SQL text already extracted from a raw "
            + "string literal - and drops SQL block-comment lines from the outer SELECT list before counting "
            + "its items. CSharpSourceWalker cannot read SQL, so adopting it here would be a category error. "
            + "The window it cuts (the outer SELECT through FROM differenced) holds no comment today, so the "
            + "/* and * arms are inert; a comment added there whose continuation line contains ' AS ' would "
            + "over-count the select list and fail the 19 pin loudly.",
    };

    /// <summary>
    /// A real call to <c>StartsWith</c>, excluding <c>Assert.StartsWith</c>. The exclusion is the distinction
    /// #3052's own census got wrong twice: <c>Assert.StartsWith("/*", sql)</c> asserts that GENERATED T-SQL
    /// opens with a risk-disclosure header, which is the opposite act from filtering comments out of source.
    /// Same substring, different direction, and two files were put on the list for having it.
    /// </summary>
    private static readonly Regex s_startsWithCall = new(
        @"(?<!Assert)\.StartsWith\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Fact]
    public void EveryLinePrefixCommentFilter_StatesTheBoundItRestsOn()
    {
        var files = TestSources();

        /* A floor rather than an equality on the file count, for the reason the rest of this family gives:
           it catches a sweep that read the wrong directory and reported clean on nothing. Set above the
           LARGER project's own size (445 to Lite.Tests' 298 when this landed) so that reading only one of
           the two fails here rather than passing on half the corpus. */
        Assert.True(
            files.Count >= 600,
            $"the sweep found only {files.Count} test source file(s) — it is not reading both test projects");

        var found = new List<string>();

        foreach (var (name, text) in files)
        {
            if (PrefixFilterSites(text).Count > 0)
            {
                found.Add(name);
            }
        }

        Assert.Equal(
            s_bounded.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray(),
            found.OrderBy(k => k, StringComparer.Ordinal).ToArray());
    }

    /// <summary>
    /// The control, and the only thing here that can fail on an arranged input rather than on the tree: the
    /// prefix form really does read a block comment's body as code, and the walk really does not. Without
    /// this, "the walker fixes it" would be a claim in a doc comment with nothing checking it.
    ///
    /// <para>The fixture is the shape the issue describes — a token inside <c>/* … */</c> whose continuation
    /// lines carry no asterisk — plus one real call, so a scan that finds NOTHING cannot pass either half.
    /// </para>
    /// </summary>
    [Fact]
    public void TheLinePrefixForm_ReadsABlockCommentsBodyAsCode_AndTheWalkDoesNot()
    {
        const string fixture = """
            public void Probe()
            {
                /* The deadline is set by the caller rather than here, because a nested
                   CreateCommand(sql) would take the outer command's budget and the two
                   are not the same measurement. */
                Run();
                CreateCommand(other);
            }
            """;

        var prefixFiltered = string.Join("\n", fixture.Split('\n')
            .Where(l => !l.TrimStart().StartsWith("/", StringComparison.Ordinal)
                     && !l.TrimStart().StartsWith("*", StringComparison.Ordinal)));

        /* The defect: the continuation line survives the filter, so the prose reads as a second call. */
        Assert.Equal(2, Occurrences(prefixFiltered, "CreateCommand("));

        /* And the fix: the walk leaves the real call and nothing else. */
        var walked = CSharpSourceWalker.StripCommentsAndStrings(fixture);
        Assert.Equal(1, Occurrences(walked, "CreateCommand("));

        /* Stated as a line number too, because the pins report offenders by line and a walk that dropped a
           newline would send the reader to innocent code. */
        Assert.Equal(
            fixture.Split('\n').Length,
            walked.Split('\n').Length);
    }

    /* ───────────────── the census's own summary, which nothing was holding (#3069) ───────────────── */

    /// <summary>
    /// The kind labels every entry in <see cref="s_bounded"/> must open with, mapped to the counted claim in
    /// this class's summary that describes them.
    ///
    /// <para>An enumeration for the same reason the map above is one — but note the direction: an entry
    /// carrying a kind that is NOT here fails <see cref="KindCounts"/> loudly rather than going uncounted,
    /// so a new kind cannot slip past by being unrecognised. That is the opposite of the way a wildcard
    /// grows.</para>
    /// </summary>
    private static readonly (string Label, string Claim)[] s_kinds =
    {
        ("COLLECTS", "collect a doc-comment run"),
        ("STATED BOUND", "reads a stated, measured bound"),
        ("NOT C#", "filters SQL"),
        ("DEMONSTRATES", "demonstrates the shape"),
    };

    /// <summary>
    /// The numeral words this class's summary is allowed to spell its counts with. Prose reads better with
    /// words than digits here, so the mapping is explicit rather than the digits-only convention
    /// <c>ReadmeDerivedCountPinTests</c> uses over Markdown.
    ///
    /// <para><b>The bound, stated because it is the load-bearing part.</b> A count that outgrew this
    /// vocabulary does not silently pass — the pattern stops matching and
    /// <see cref="TheCensusSummary_CountsTheMapItDescribes"/> fails with a parse miss. Wrong toward the
    /// worse label, which is the only direction worth being wrong in for a guard.</para>
    /// </summary>
    private static readonly string[] s_numeralWords =
        { "Zero", "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine", "Ten" };

    /// <summary>
    /// This class's summary describes the map below it by COUNT — "Four kinds live here", then one claim per
    /// kind — and until #3069 nothing held the two together. The prose read "Two collect a doc-comment run"
    /// while THREE entries already did, so it was stale by one before #3069 added a fourth, and would have
    /// been stale by two after.
    ///
    /// <para><b>Why a pin rather than the corrected word.</b> Fixing the word fixes this instance. The
    /// asymmetry is the defect: <see cref="EveryLinePrefixCommentFilter_StatesTheBoundItRestsOn"/> catches a
    /// new MEMBER of the census by name, and nothing caught the census's own SUMMARY drifting — in a file
    /// whose entire job is adoption tracking. A counted claim that has drifted once unattended will drift
    /// again.</para>
    ///
    /// <para><b>All five claims are pinned, not four.</b> The kind total and one claim per kind. A partial
    /// pin on a summary that reads as fully pinned is worse than none, so if a claim were genuinely
    /// underivable it would be said here rather than left out quietly. None is: the kinds come from the
    /// entry TEXT and the counts from the map.</para>
    /// </summary>
    [Fact]
    public void TheCensusSummary_CountsTheMapItDescribes()
    {
        VerifySummaryCounts(CensusSummaryProse(), KindCounts(s_bounded.Values), s_bounded.Count);
    }

    /// <summary>
    /// Non-vacuity, in both directions and for the mapping in between — because the cheapest way this pin
    /// passes while the summary is wrong is that the extraction never yields a numeral at all, and a
    /// word-versus-digit mismatch would do exactly that.
    /// </summary>
    [Fact]
    public void TheCensusSummaryPin_ReportsDriftFromEitherSide()
    {
        var prose = CensusSummaryProse();
        var real = KindCounts(s_bounded.Values);
        var injected = 0;

        /* (a) the PROSE moves. One claim at a time, so each of the five is individually load-bearing. */
        foreach (var (_, claim) in s_kinds.Select(k => (k.Label, k.Claim)).Append((string.Empty, "kinds live here")))
        {
            var pattern = new Regex($@"({string.Join("|", s_numeralWords)}) {Regex.Escape(claim)}");
            var match = pattern.Match(prose);
            Assert.True(match.Success, $"the summary no longer states a count for '{claim}', so its pin is vacuous.");

            var wrong = s_numeralWords[(Array.IndexOf(s_numeralWords, match.Groups[1].Value) + 1) % s_numeralWords.Length];
            var mutated = prose.Remove(match.Groups[1].Index, match.Groups[1].Length).Insert(match.Groups[1].Index, wrong);
            Assert.NotEqual(prose, mutated);
            Assert.ThrowsAny<Exception>(() => VerifySummaryCounts(mutated, real, s_bounded.Count));
            injected++;
        }

        Assert.Equal(s_kinds.Length + 1, injected);

        /* (b) the MAP moves and the prose does not — a fifth COLLECTS entry added without a copy-edit. */
        var withAnother = KindCounts(s_bounded.Values.Append("COLLECTS a doc run. A synthetic entry, for this test only."));
        Assert.ThrowsAny<Exception>(() => VerifySummaryCounts(prose, withAnother, s_bounded.Count + 1));

        /* (c) an entry whose kind is not recognised must FAIL rather than go uncounted, or the breakdown
               could silently stop summing to the map. */
        Assert.ThrowsAny<Exception>(() => KindCounts(s_bounded.Values.Append("SOMETHING ELSE. an unlabelled bound.")));

        /* (d) and the word mapping itself: a count spelled outside the vocabulary reds rather than passing,
               which is the failure mode a digits-only pin would not have had to think about. */
        Assert.ThrowsAny<Exception>(() => VerifySummaryCounts(
            prose.Replace("kinds live here", "kinds live in here", StringComparison.Ordinal),
            real,
            s_bounded.Count));
    }

    /// <summary>
    /// Each kind label paired with how many entries open with it. Throws when an entry matches none, so the
    /// breakdown can never quietly stop accounting for the whole map.
    /// </summary>
    private static Dictionary<string, int> KindCounts(IEnumerable<string> bounds)
    {
        var counts = s_kinds.ToDictionary(k => k.Label, _ => 0, StringComparer.Ordinal);

        foreach (var bound in bounds)
        {
            var label = s_kinds.FirstOrDefault(k => bound.StartsWith(k.Label, StringComparison.Ordinal)).Label;
            Assert.False(label is null,
                $"this entry opens with no recognised kind: \"{bound[..Math.Min(60, bound.Length)]}…\". Every "
                + "entry must name its kind, because the class summary states one count per kind and "
                + "TheCensusSummary_CountsTheMapItDescribes derives those counts from this text. Add the new "
                + "kind to s_kinds and give it a claim in the summary.");
            counts[label!]++;
        }

        return counts;
    }

    /// <summary>
    /// The summary's counted claims against <paramref name="counts"/>. Separated from the map so
    /// <see cref="TheCensusSummaryPin_ReportsDriftFromEitherSide"/> can drive it with a synthetic census and
    /// prove the map side reds too, which reading the real map could never show.
    /// </summary>
    private static void VerifySummaryCounts(string prose, Dictionary<string, int> counts, int total)
    {
        Assert.Equal(total, counts.Values.Sum());
        Assert.True(total > 0, "the census is empty, so every count below would be vacuously satisfiable.");

        var claims = s_kinds
            .Select(k => (k.Claim, Expected: counts[k.Label]))
            .Append(("kinds live here", counts.Count(c => c.Value > 0)))
            .ToArray();

        foreach (var (claim, expected) in claims)
        {
            var pattern = new Regex($@"({string.Join("|", s_numeralWords)}) {Regex.Escape(claim)}");
            var match = pattern.Match(prose);

            Assert.True(match.Success,
                $"this class's summary no longer states a count for \"{claim}\" as one of "
                + $"{string.Join("/", s_numeralWords)}. It describes s_bounded by count, so keep the claim "
                + "parseable, spell the numeral as a word from that list — or delete the count from the "
                + "prose and remove it from s_kinds' claim, which is equally acceptable (#3069).");

            var actual = Array.IndexOf(s_numeralWords, match.Groups[1].Value);
            Assert.True(actual == expected,
                $"the summary says {match.Groups[1].Value} ({actual}) {claim}, but the census holds "
                + $"{expected}. Fix the word in the summary rather than this pin.");
        }
    }

    /// <summary>
    /// <see cref="s_bounded"/>'s OWN doc comment — which is where the counted claims live, not the class
    /// summary. Normalised the way <c>RefreshCeilingProvenancePinTests.DocProseFor</c> normalises the ones it
    /// reads: emphasis tags and dash style removed, so a copy-edit that only reformats cannot break a count
    /// pin.
    ///
    /// <para>Anchored on the map's declaration rather than the class's, and the difference is not academic:
    /// aiming this at the class summary is a mistake this pin caught on its first run, because the wrong doc
    /// run yields prose with no counted claim in it at all and the extraction fails loudly instead of
    /// comparing nothing.</para>
    /// </summary>
    private static string CensusSummaryProse([CallerFilePath] string thisFile = "")
    {
        var source = File.ReadAllText(thisFile);
        var declaration = source.IndexOf(
            "private static readonly Dictionary<string, string> s_bounded", StringComparison.Ordinal);
        Assert.True(declaration > 0, "could not find s_bounded's declaration, so its summary could not be read.");

        var lines = source[..declaration].Split('\n');
        var run = new List<string>();

        for (var index = lines.Length - 2; index >= 0; index--)
        {
            var line = lines[index].Trim('\r', ' ', '\t');
            if (!line.StartsWith("///", StringComparison.Ordinal))
            {
                break;
            }

            run.Insert(0, line.Length > 3 ? line[3..].Trim() : string.Empty);
        }

        var prose = string.Join(" ", run)
            .Replace("<b>", string.Empty, StringComparison.Ordinal)
            .Replace("</b>", string.Empty, StringComparison.Ordinal)
            .Replace('\u2014', '-')
            .Replace('\u2013', '-');
        prose = Regex.Replace(prose, @"\s+", " ").Trim();

        Assert.StartsWith("<summary>", prose, StringComparison.Ordinal);
        Assert.EndsWith("</summary>", prose, StringComparison.Ordinal);
        return prose;
    }

    /// <summary>
    /// Every <c>StartsWith</c> call in <paramref name="text"/> whose first argument is a comment prefix, as
    /// the offset of the call.
    ///
    /// <para>Matched over <see cref="CSharpSourceWalker.StripCommentsAndStrings"/>'s output so that prose
    /// ABOUT the shape does not count as the shape — which this very file needs, since it quotes the filter
    /// it is guarding. The argument is then read out of the ORIGINAL text at the same offset, because the
    /// walk blanks literal TEXT and the argument is exactly that; the two are character-aligned, which is
    /// what makes reading one at the other's index sound.</para>
    ///
    /// <para>Only a plain <c>"…"</c> argument is recognised. A comment prefix spelled any other way — a
    /// verbatim literal, a <c>const</c>, a <c>char</c> — would slip past, and there is no such site in
    /// either project. That is a stated bound on this guard and not a claim about C#.</para>
    /// </summary>
    private static List<int> PrefixFilterSites(string text)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(text);
        var sites = new List<int>();

        foreach (Match call in s_startsWithCall.Matches(code))
        {
            var i = call.Index + call.Length;

            while (i < text.Length && char.IsWhiteSpace(text[i]))
            {
                i++;
            }

            if (i >= text.Length || text[i] != '"')
            {
                continue;
            }

            var close = text.IndexOf('"', i + 1);
            if (close < 0)
            {
                continue;
            }

            if (s_commentPrefixes.Contains(text[(i + 1)..close], StringComparer.Ordinal))
            {
                sites.Add(call.Index);
            }
        }

        return sites;
    }

    private static int Occurrences(string text, string needle)
    {
        var count = 0;
        for (var i = text.IndexOf(needle, StringComparison.Ordinal); i >= 0; i = text.IndexOf(needle, i + 1, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    /// <summary>
    /// Both test projects' sources, keyed by the project-relative name the allow-list uses — so an entry
    /// names one project's file and not "whichever twin the sweep reached first".
    /// </summary>
    private static List<(string Name, string Text)> TestSources()
    {
        var root = RepoRoot();
        var sources = new List<(string, string)>();

        foreach (var (project, directory) in new[]
                 {
                     ("Darling.Tests", Path.Combine(root, "Darling", "Darling.Tests")),
                     ("Lite.Tests", Path.Combine(root, "Lite.Tests")),
                 })
        {
            foreach (var path in Directory.EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories)
                .Where(p => !p.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                .Where(p => !p.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                .OrderBy(p => p, StringComparer.Ordinal))
            {
                sources.Add(($"{project}/{Path.GetFileName(path)}", File.ReadAllText(path)));
            }
        }

        return sources;
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
