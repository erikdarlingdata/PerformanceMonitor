/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Lite's half of the cross-SKU parity guard for the QUERY_HIGH_DOP staleness cross-check. The exact
/// counterpart of <c>Darling.Tests.QueryHighDopStaleMaxDopParityTests</c>, deliberately built to the
/// same shape — and to the shape <c>ParameterSensitivityFiringSignatureParityTests</c> established in
/// #2998 — so the two apps are guarded the same way rather than one being guarded and the other trusted.
///
/// <para><b>The cross-check.</b> <c>max_dop</c> on <c>v_query_stats</c> is
/// <c>sys.dm_exec_query_stats</c>' LIFETIME max for the plan's time in cache, so a plan compiled before
/// <c>max degree of parallelism</c> was lowered keeps reporting the old higher DOP until it is evicted or
/// recompiled. A reading that EXCEEDS what the server's current MAXDOP can produce is impossible under
/// today's configuration, so it provably predates the change that set it and must not be counted. Lite
/// writes the cross-check once, in <c>DuckDbFactCollector</c>'s query-stats read, and Darling writes the
/// same one against its own store.</para>
///
/// <para><b>Why this pair exists at all.</b> #2705 fixed Darling and closed without a record that a twin
/// existed; #2999 is what that cost — Lite kept raising the finding at full confidence from readings
/// Darling provably excludes, and lowering MAXDOP is the ordinary remediation FOR this finding, so it
/// survived its own fix. #2998's guard is deliberately scoped to the PARAMETER_SENSITIVITY firing
/// signature and does not reach this predicate; this pair is its sibling for that reason.</para>
///
/// <para><b>The carve-outs cut the other way.</b> A <c>current_maxdop</c> of 0 (unlimited) and an absent
/// config row both make no configuration claim, so both must leave the count ALONE. Getting either
/// backwards suppresses the finding entirely, which is worse than over-reporting because nothing then
/// surfaces it — hence <see cref="EveryHighDopCount_CarriesTheStalenessCrossCheck"/> and
/// <see cref="TheNullSafeJoin_IsPinned"/>.</para>
///
/// <para><b>Why the pair, and not one scan.</b> Darling's twin does the cross-STORE comparison, because
/// <c>build.yml</c>'s <c>darling</c> path filter covers every Lite <c>.cs</c> file as well as the whole
/// Darling tree, so it runs on an edit to either analysis tree. This file owns Lite's own census and
/// meta-pins Darling's declared numbers, and the <c>lite</c> filter covers Darling's test tree — so an
/// edit that weakens Darling's guard runs this one. Raising a count or retuning a literal in one guard
/// without the other fails:
/// <see cref="DarlingsTwinGuard_PinsTheSameCanonicalCrossCheck_SoNeitherSideCanFallBehind"/> reads
/// Darling's guard as source and compares the two declarations.</para>
///
/// <para><b>Read from source, not from a constant.</b> Lite's analysis SQL is inline
/// <c>cmd.CommandText</c> where Darling's is a named constant. The asymmetry does not matter here,
/// because no test project references both SKUs' assemblies — whichever suite hosts a cross-SKU claim has
/// to read at least one side out of the checked-out tree anyway (that is what <see cref="ParitySource"/>
/// exists for).</para>
/// </summary>
public sealed class QueryHighDopStaleMaxDopParityTests
{
    /// <summary>
    /// The cross-check, normalized: runs of whitespace collapsed to single spaces. Byte-identical to the
    /// literal Darling's twin declares, and the meta-pin below compares the two.
    /// </summary>
    internal const string CanonicalStaleMaxDopGuard =
        "COUNT(CASE WHEN v.max_dop > 8 " +
        "AND (m.value_in_use IS NULL OR m.value_in_use = 0 OR v.max_dop <= m.value_in_use) " +
        "THEN 1 END) AS high_dop_queries";

    /// <summary>
    /// Copies of the cross-check in Lite's analysis tree. Darling's twin declares this same number as its
    /// <c>LiteGuardCopies</c> and the meta-pin below reads it back, so neither side can be raised alone.
    /// </summary>
    internal const int ExpectedGuardCopies = 1;

    /// <summary>
    /// The config source Lite's read must use. Not a style choice: Lite's <c>v_</c> views union the live
    /// tables with archived Parquet, and
    /// <see cref="AnalysisDataSpanTests.AnalysisPipeline_NeverReadsAnArchivableTableRaw"/> positively
    /// REQUIRES that form under <c>Lite/Analysis</c> — a bare <c>server_config</c> fails it, and would
    /// also cross-check against whatever survived the last 512 MB reset.
    /// </summary>
    private const string LiteConfigSource = "v_server_config";

    /// <summary>Darling's guard, read as source because no test project references both SKUs.</summary>
    private const string DarlingGuard = "Darling/Darling.Tests/QueryHighDopStaleMaxDopParityTests.cs";

    /// <summary>This file, read as source so the meta-pin's parser can be self-validated.</summary>
    private const string ThisGuard = "Lite.Tests/QueryHighDopStaleMaxDopParityTests.cs";

    /// <summary>
    /// The cross-check, per Lite file — a floor and a ceiling. Written out rather than globbed so that
    /// deleting a read, or moving one to a new file, fails here and has to be re-declared instead of
    /// quietly reducing this guard's coverage to whatever is left.
    /// </summary>
    private static readonly (string RelativePath, int Copies)[] ExpectedCopies =
    [
        ("Lite/Analysis/DuckDbFactCollector.QueryPerf.cs", 1),
    ];

    /// <summary>
    /// Every read that COUNTS high-DOP queries at all, guarded or not — the census the cross-check has to
    /// cover. Deliberately looser than <see cref="StaleMaxDopGuard"/>: the whole point is that a count
    /// WITHOUT the cross-check still lands here.
    /// </summary>
    private static readonly Regex HighDopCount = new(
        @"COUNT\([ \t]*CASE[ \t]+WHEN[ \t]+(?:[A-Za-z_]\w*\.)?max_dop[ \t]*(?<floorOp>>=?)[ \t]*(?<floor>\d+)",
        RegexOptions.IgnoreCase);

    /// <summary>
    /// The cross-check as a shape: every token pinned literally EXCEPT the DOP floor and the unlimited
    /// sentinel, which are captured so a skewed copy is still found and reported as a wrong value rather
    /// than vanishing from the census. Adjacency is pinned with <c>[ \t]</c> rather than <c>\s</c> so a
    /// blank line or a reordering cannot slide past.
    /// </summary>
    private static readonly Regex StaleMaxDopGuard = new(
        @"COUNT\([ \t]*CASE[ \t]+WHEN[ \t]+v\.max_dop[ \t]*(?<floorOp>>=?)[ \t]*(?<floor>\d+)[ \t]*\r?\n"
        + @"[ \t]*AND[ \t]+\([ \t]*m\.value_in_use[ \t]+IS[ \t]+NULL[ \t]+OR[ \t]+"
        + @"m\.value_in_use[ \t]*=[ \t]*(?<unlimited>\d+)[ \t]+OR[ \t]+"
        + @"v\.max_dop[ \t]*<=[ \t]*m\.value_in_use[ \t]*\)[ \t]*\r?\n"
        + @"[ \t]*THEN[ \t]+1[ \t]+END\)[ \t]+AS[ \t]+high_dop_queries",
        RegexOptions.IgnoreCase);

    /// <summary>
    /// The CTE that resolves the server's CURRENT MAXDOP. The config source is captured so it can be
    /// pinned to Lite's <c>v_</c> form; everything that decides WHICH row wins is pinned, because a copy
    /// that reads the oldest capture, or an unrelated setting, would cross-check against a number that
    /// means nothing.
    /// </summary>
    private static readonly Regex CurrentMaxDopCte = new(
        @"WITH[ \t]+current_maxdop[ \t]+AS[ \t]*\r?\n"
        + @"[ \t]*\([ \t]*\r?\n"
        + @"[ \t]*SELECT[ \t]+value_in_use[ \t]*\r?\n"
        + @"[ \t]*FROM[ \t]+(?<source>v_server_config|server_config)[ \t]*\r?\n"
        + @"[ \t]*WHERE[ \t]+server_id[ \t]*=[ \t]*\$\d+[ \t]*\r?\n"
        + @"[ \t]*AND[ \t]+configuration_name[ \t]*=[ \t]*'max degree of parallelism'[ \t]*\r?\n"
        + @"[ \t]*ORDER[ \t]+BY[ \t]+capture_time[ \t]+DESC[ \t]*\r?\n"
        + @"[ \t]*LIMIT[ \t]+1[ \t]*\r?\n"
        + @"[ \t]*\)",
        RegexOptions.IgnoreCase);

    /// <summary>
    /// The NULL-safe join. <c>LEFT JOIN ... ON true</c> against a one-row CTE is what makes an absent
    /// config row arrive as NULL and take the leave-the-count-alone arm. An inner join would return NO
    /// ROWS on a server with no collected config, so QUERY_HIGH_DOP — and QUERY_SPILLS with it — would
    /// silently vanish rather than being over-reported.
    /// </summary>
    private static readonly Regex NullSafeJoin = new(
        @"LEFT[ \t]+JOIN[ \t]+current_maxdop[ \t]+AS[ \t]+m[ \t]+ON[ \t]+true",
        RegexOptions.IgnoreCase);

    [Fact]
    public void EveryCopyOfTheCrossCheck_IsAccountedFor_InLitesAnalysisSql()
    {
        var total = 0;

        foreach (var (relativePath, expected) in ExpectedCopies)
        {
            var actual = StaleMaxDopGuard.Matches(ReadNormalized(relativePath)).Count;
            total += actual;

            Assert.True(
                actual == expected,
                $"{relativePath} carries {actual} copy/copies of the QUERY_HIGH_DOP staleness cross-check, "
                + $"expected {expected}. #2705 landed on Darling only and #2999 is what that cost: if a read "
                + "moved or was added, update this guard AND Darling's in the same commit.");
        }

        Assert.Equal(ExpectedGuardCopies, total);
    }

    /// <summary>
    /// The assertion that fails if the cross-check is deleted while everything else stays. A high-DOP
    /// COUNT with no staleness cross-check is exactly the #2999 defect, and it satisfies every count and
    /// canonical assertion above by simply not being there to disagree with.
    /// </summary>
    [Fact]
    public void EveryHighDopCount_CarriesTheStalenessCrossCheck()
    {
        var unguarded = new List<string>();

        foreach (var (relativePath, _) in ExpectedCopies)
        {
            var source = ReadNormalized(relativePath);
            var counts = HighDopCount.Matches(source).Count;
            var guarded = StaleMaxDopGuard.Matches(source).Count;

            if (counts != guarded)
            {
                unguarded.Add($"{relativePath}: {counts} high-DOP count(s), {guarded} carrying the cross-check");
            }
        }

        Assert.True(
            unguarded.Count == 0,
            "a QUERY_HIGH_DOP count exists that does NOT cross-check max_dop against the server's current "
            + "MAXDOP:" + Environment.NewLine + string.Join(Environment.NewLine, unguarded) + Environment.NewLine
            + "max_dop is dm_exec_query_stats' LIFETIME max, so a raw count fires from readings the current "
            + "configuration makes impossible — and lowering MAXDOP is the remediation this very finding "
            + "recommends, so the finding would outlive its own fix. That was #2999.");
    }

    [Fact]
    public void NoUndeclaredLiteAnalysisRead_CountsHighDopQueries()
    {
        var declared = ExpectedCopies
            .Select(e => RepoPath(e.RelativePath))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var files = LiteAnalysisSourceFiles();

        /* A floor, so a broken enumeration cannot report a clean bill of health. 24 files under
           Lite/Analysis when this was written; set well below that so ordinary growth never trips it, but
           high enough that an empty enumeration fails instead of passing. */
        Assert.True(files.Length >= 12, $"enumerated only {files.Length} files under Lite/Analysis");

        /* And the declared files must be part of what was enumerated, or the two halves of this guard are
           looking at different trees. */
        Assert.Equal(ExpectedCopies.Length, declared.Intersect(files, StringComparer.OrdinalIgnoreCase).Count());

        var strays = new List<string>();

        foreach (var path in files)
        {
            if (declared.Contains(path))
            {
                continue;
            }

            var count = HighDopCount.Matches(
                File.ReadAllText(path).Replace("\r\n", "\n", StringComparison.Ordinal)).Count;

            if (count > 0)
            {
                strays.Add($"{Path.GetFileName(path)}: {count} high-DOP count(s)");
            }
        }

        Assert.True(
            strays.Count == 0,
            "a Lite analysis read counts high-DOP queries in a file this guard does not declare:"
            + Environment.NewLine + string.Join(Environment.NewLine, strays) + Environment.NewLine
            + "Add it to ExpectedCopies — with the staleness cross-check, and with the mirror copy in "
            + "Darling's store — in the same commit.");
    }

    [Fact]
    public void EveryCopyOfTheCrossCheck_ReducesToTheCanonicalOne()
    {
        var skewed = new List<string>();
        var seen = 0;

        foreach (var (relativePath, _) in ExpectedCopies)
        {
            foreach (Match m in StaleMaxDopGuard.Matches(ReadNormalized(relativePath)))
            {
                seen++;

                if (!string.Equals(Normalize(m.Value), CanonicalStaleMaxDopGuard, StringComparison.Ordinal))
                {
                    skewed.Add(
                        $"{relativePath} @ char {m.Index}: floor={m.Groups["floorOp"].Value} "
                        + $"{m.Groups["floor"].Value}, unlimited sentinel={m.Groups["unlimited"].Value}");
                }
            }
        }

        Assert.True(
            skewed.Count == 0,
            "QUERY_HIGH_DOP staleness cross-check copies disagree with the canonical one:"
            + Environment.NewLine + string.Join(Environment.NewLine, skewed) + Environment.NewLine
            + "canonical: " + CanonicalStaleMaxDopGuard + Environment.NewLine
            + "Lite AND Darling must apply the SAME floor and the SAME unlimited sentinel, or one SKU "
            + "reports a finding the other provably excludes.");

        /* The census must have had something to look at: nothing found would otherwise pass this test by
           having nothing left to disagree with. */
        Assert.Equal(ExpectedGuardCopies, seen);
    }

    /// <summary>
    /// The <c>current_maxdop</c> CTE, and the <c>v_</c> config source it must read. Both halves matter:
    /// without the CTE the cross-check compares against nothing, and with the bare table name it compares
    /// against whatever survived the last 512 MB archive reset.
    /// </summary>
    [Fact]
    public void TheCurrentMaxDopCte_ReadsTheArchiveView_NotServerConfigRaw()
    {
        foreach (var (relativePath, _) in ExpectedCopies)
        {
            var source = ReadNormalized(relativePath);

            var m = CurrentMaxDopCte.Match(source);
            Assert.True(
                m.Success,
                $"{relativePath} no longer resolves the server's current MAXDOP in a current_maxdop CTE "
                + "that takes the NEWEST 'max degree of parallelism' capture.");

            Assert.Equal(LiteConfigSource, m.Groups["source"].Value);

            Assert.False(
                Regex.IsMatch(source, @"\bFROM\s+server_config\b"),
                $"{relativePath} reads server_config raw — AnalysisPipeline_NeverReadsAnArchivableTableRaw "
                + "fails on that, and the read would lose everything archived to Parquet.");
        }
    }

    /// <summary>
    /// The NULL-safe join. This is the carve-out that keeps an absent config row from suppressing the
    /// finding — and, because the join feeds the whole read, from suppressing every other fact it emits.
    /// </summary>
    [Fact]
    public void TheNullSafeJoin_IsPinned()
    {
        foreach (var (relativePath, expected) in ExpectedCopies)
        {
            var actual = NullSafeJoin.Matches(ReadNormalized(relativePath)).Count;

            Assert.True(
                actual == expected,
                $"{relativePath} carries {actual} NULL-safe current_maxdop join(s), expected {expected}. "
                + "LEFT JOIN ... ON true against the one-row CTE is what delivers NULL — and therefore the "
                + "leave-the-count-alone arm — on a server with no collected config. An inner join would "
                + "return no rows at all there, silently dropping every fact this read emits.");
        }
    }

    /// <summary>
    /// The discriminators, pinned in BOTH directions against literals written for the purpose, and kept
    /// in this suite as well as Darling's so that each half can fail on its own rather than inheriting
    /// the other's confidence.
    /// </summary>
    [Fact]
    public void TheDiscriminators_MatchTheShippedForms_AndSkew_ButNotABareProjection()
    {
        /* The shipped layout, in both line endings: the committed blobs are pure LF while the working
           tree is CRLF, so both have to reduce to one answer. */
        string[] shipped =
        [
            "    COUNT(CASE WHEN v.max_dop > 8\r\n"
            + "                AND (m.value_in_use IS NULL OR m.value_in_use = 0 OR v.max_dop <= m.value_in_use)\r\n"
            + "               THEN 1 END) AS high_dop_queries,\r\n",

            "    COUNT(CASE WHEN v.max_dop > 8\n"
            + "                AND (m.value_in_use IS NULL OR m.value_in_use = 0 OR v.max_dop <= m.value_in_use)\n"
            + "               THEN 1 END) AS high_dop_queries,\n",
        ];

        foreach (var sql in shipped)
        {
            var m = StaleMaxDopGuard.Match(sql);
            Assert.True(m.Success, "the discriminator missed a shipped cross-check form");
            Assert.Equal(CanonicalStaleMaxDopGuard, Normalize(m.Value));
            Assert.True(HighDopCount.IsMatch(sql), "the census pattern missed a shipped high-DOP count");
        }

        /* THE DEFECT ITSELF — #2999's Lite read as it stood. It must land in the CENSUS and must NOT
           match the guard, or EveryHighDopCount_CarriesTheStalenessCrossCheck cannot see it. */
        const string unguarded = "    COUNT(CASE WHEN max_dop > 8 THEN 1 END) AS high_dop_queries,\r\n";
        Assert.True(
            HighDopCount.IsMatch(unguarded),
            "the census pattern does not see an UNGUARDED high-DOP count — the #2999 defect would pass");
        Assert.False(
            StaleMaxDopGuard.IsMatch(unguarded),
            "the guard pattern matched an unguarded high-DOP count");

        /* SKEW — each must still MATCH, so it lands in the census and is reported as a wrong value, and
           must NOT reduce to the canonical cross-check. */
        (string Sql, string What)[] skews =
        [
            (shipped[0].Replace("max_dop > 8", "max_dop > 16", StringComparison.Ordinal), "DOP floor raised"),
            (shipped[0].Replace("max_dop > 8", "max_dop >= 8", StringComparison.Ordinal), "DOP floor operator loosened"),
            (shipped[0].Replace("value_in_use = 0", "value_in_use = 1", StringComparison.Ordinal), "unlimited sentinel changed"),
        ];

        foreach (var (sql, what) in skews)
        {
            var m = StaleMaxDopGuard.Match(sql);
            Assert.True(
                m.Success,
                $"the discriminator stopped matching a skewed copy ({what}) — it would be reported as a "
                + "missing copy rather than a skewed literal");
            Assert.NotEqual(CanonicalStaleMaxDopGuard, Normalize(m.Value));
        }

        /* CARVE-OUTS DROPPED. Either one alone inverts the fix: without the NULL arm a server with no
           collected config stops reporting, without the zero arm an unlimited server does. */
        (string Sql, string What)[] mutilated =
        [
            (shipped[0].Replace("m.value_in_use IS NULL OR ", "", StringComparison.Ordinal), "NULL carve-out dropped"),
            (shipped[0].Replace("m.value_in_use = 0 OR ", "", StringComparison.Ordinal), "unlimited carve-out dropped"),
            (shipped[0].Replace(" OR v.max_dop <= m.value_in_use", "", StringComparison.Ordinal), "ceiling test dropped"),
        ];

        foreach (var (sql, what) in mutilated)
        {
            Assert.False(
                StaleMaxDopGuard.IsMatch(sql),
                $"the guard pattern matched a cross-check with the {what}");
            Assert.True(
                HighDopCount.IsMatch(sql),
                $"the census pattern lost sight of the read when the {what} — it would be reported as a "
                + "deleted read rather than an unguarded one");
        }

        /* BARE PROJECTIONS — real lines from this corpus. max_dop is projected by Lite's drill-down and
           activity reads; a pattern keyed on the column rather than on the COUNT would flag every one. */
        string[] benign =
        [
            "       MAX(max_dop) AS max_dop,",
            "    MAX(max_dop) AS max_dop",
            "    MAX(dop) AS max_dop,",
            "                max_dop = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),",
            "                        [\"max_dop\"] = maxDop",
            /* And a genuinely unrelated COUNT(CASE WHEN ...) from the same read. */
            "    COUNT(CASE WHEN v.delta_spills > 0 THEN 1 END) AS spilling_queries,",
        ];

        foreach (var sql in benign)
        {
            Assert.False(HighDopCount.IsMatch(sql), $"the census pattern dragged in a benign form: {sql}");
            Assert.False(StaleMaxDopGuard.IsMatch(sql), $"the guard pattern dragged in a benign form: {sql}");
        }

        /* The CTE and join discriminators need their own positive control, for the same reason. */
        const string shippedCte =
            "WITH current_maxdop AS\r\n"
            + "(\r\n"
            + "    SELECT value_in_use\r\n"
            + "    FROM v_server_config\r\n"
            + "    WHERE server_id = $1\r\n"
            + "    AND   configuration_name = 'max degree of parallelism'\r\n"
            + "    ORDER BY capture_time DESC\r\n"
            + "    LIMIT 1\r\n"
            + ")\r\n";

        var cte = CurrentMaxDopCte.Match(shippedCte);
        Assert.True(cte.Success, "the CTE discriminator missed a shipped current_maxdop CTE");
        Assert.Equal(LiteConfigSource, cte.Groups["source"].Value);

        Assert.False(
            CurrentMaxDopCte.IsMatch(
                shippedCte.Replace("capture_time DESC", "capture_time ASC", StringComparison.Ordinal)),
            "the CTE discriminator accepted the OLDEST config capture");
        Assert.False(
            CurrentMaxDopCte.IsMatch(
                shippedCte.Replace(
                    "'max degree of parallelism'",
                    "'cost threshold for parallelism'",
                    StringComparison.Ordinal)),
            "the CTE discriminator accepted an unrelated configuration setting");

        /* And the raw-table check must actually fire on the bare name, or the v_ assertion above is
           decoration. */
        Assert.True(
            Regex.IsMatch("    FROM server_config\r\n", @"\bFROM\s+server_config\b"),
            "the raw-table pattern does not see a bare server_config read");
        Assert.False(
            Regex.IsMatch("    FROM v_server_config\r\n", @"\bFROM\s+server_config\b"),
            "the raw-table pattern flagged the v_ form");

        Assert.Matches(NullSafeJoin, "LEFT JOIN current_maxdop AS m ON true\r\n");
        Assert.False(
            NullSafeJoin.IsMatch("INNER JOIN current_maxdop AS m ON true\r\n"),
            "the join discriminator accepted an inner join");
        Assert.False(
            NullSafeJoin.IsMatch("JOIN current_maxdop AS m ON true\r\n"),
            "the join discriminator accepted a bare (inner) join");
    }

    /// <summary>
    /// Darling's twin must exist, pin the SAME canonical cross-check, and declare the same Lite-side
    /// count this file declares for itself. Retuning a literal or moving a copy on one side of the SKU
    /// boundary without the other is what this fails on — and because the guards, not just the SQL, are
    /// compared, deleting or softening one while the other keeps claiming parity fails too.
    /// </summary>
    [Fact]
    public void DarlingsTwinGuard_PinsTheSameCanonicalCrossCheck_SoNeitherSideCanFallBehind()
    {
        /* Self-validate the parser against THIS file first: a parser that reads nothing would otherwise
           "agree" with Darling by returning the same nothing from both sides. */
        Assert.Equal(CanonicalStaleMaxDopGuard, ParseCanonicalCrossCheck(ReadNormalized(ThisGuard), ThisGuard));

        var darling = ReadNormalized(DarlingGuard);

        Assert.Equal(CanonicalStaleMaxDopGuard, ParseCanonicalCrossCheck(darling, DarlingGuard));

        var liteSide = Regex.Match(darling, @"LiteGuardCopies\s*=\s*(?<n>\d+)\s*;");
        Assert.True(liteSide.Success, $"{DarlingGuard} no longer declares LiteGuardCopies.");
        Assert.Equal(
            ExpectedGuardCopies,
            int.Parse(liteSide.Groups["n"].Value, System.Globalization.CultureInfo.InvariantCulture));

        /* And it must still declare its own side, and still enumerate its files rather than globbing, for
           the same reason this one does. */
        Assert.Matches(@"DarlingGuardCopies\s*=\s*\d+\s*;", darling);
        Assert.Contains("ExpectedCopies", darling, StringComparison.Ordinal);
    }

    /// <summary>
    /// Reconstructs a <c>CanonicalStaleMaxDopGuard</c> declaration from C# source, for the half of the
    /// pair that cannot reference the other SKU's assembly.
    /// </summary>
    private static string ParseCanonicalCrossCheck(string source, string label)
    {
        var start = source.IndexOf("CanonicalStaleMaxDopGuard =", StringComparison.Ordinal);
        Assert.True(start >= 0, $"{label} no longer declares CanonicalStaleMaxDopGuard.");

        var end = source.IndexOf(';', start);
        Assert.True(end > start, $"{label}'s CanonicalStaleMaxDopGuard declaration is unterminated.");

        var declaration = source[start..end];

        /* The literal is deliberately escape-free on both sides, so the segment pattern below is exact
           rather than approximate. A backslash appearing here means the declaration changed shape and the
           parser is no longer reading what it thinks it is. */
        Assert.DoesNotContain("\\", declaration, StringComparison.Ordinal);

        var segments = Regex.Matches(declaration, "\"(?<s>[^\"]*)\"")
            .Select(m => m.Groups["s"].Value)
            .ToList();

        Assert.True(segments.Count > 0, $"{label}'s CanonicalStaleMaxDopGuard parsed to nothing.");

        return string.Concat(segments);
    }

    /// <summary>Collapses runs of whitespace to single spaces.</summary>
    private static string Normalize(string block) => Regex.Replace(block, @"\s+", " ").Trim();

    /// <summary>
    /// Reads a repo-relative source file with line endings collapsed to LF. <c>.gitattributes</c> checks
    /// the working tree out CRLF while the committed blobs are LF, and the cross-check is the same either
    /// way, so normalizing here keeps the regexes from having to care which one they are looking at.
    /// </summary>
    private static string ReadNormalized(string relativePath) =>
        ParitySource.ReadFile(relativePath).Replace("\r\n", "\n", StringComparison.Ordinal);

    /// <summary>Resolves a repo-relative path the same way <see cref="ParitySource.ReadFile"/> does.</summary>
    private static string RepoPath(string relativePath) =>
        Path.Combine(ParitySource.RepoRoot(), relativePath.Replace('/', Path.DirectorySeparatorChar));

    /// <summary>
    /// Every <c>.cs</c> file under Lite's analysis tree, RECURSIVELY.
    /// <see cref="ParitySource.EnumerateCsFiles"/> is top-directory-only, which would leave
    /// <c>Lite/Analysis/Recommendations</c> unscanned — and an unscanned subtree is exactly how a copy of
    /// a read added elsewhere escapes the census above.
    /// </summary>
    private static string[] LiteAnalysisSourceFiles()
    {
        var root = RepoPath("Lite/Analysis");

        return Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories)
            .Where(p =>
                !p.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                && !p.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .ToArray();
    }
}
