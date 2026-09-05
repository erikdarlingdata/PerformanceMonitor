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
/// Cross-SKU parity guard for the QUERY_HIGH_DOP staleness cross-check: #2705's guard, written out twice
/// across two stores and otherwise held equal by nothing. Built to the shape
/// <c>ParameterSensitivityFiringSignatureParityTests</c> established in #2998, because this is the defect
/// that guard exists to catch and did not cover.
///
/// <para><b>What the cross-check is.</b> <c>max_dop</c> on <c>v_query_stats</c> is
/// <c>sys.dm_exec_query_stats</c>' LIFETIME max for the plan's time in cache, so a plan compiled before
/// <c>max degree of parallelism</c> was lowered keeps reporting the old higher DOP until it is evicted or
/// recompiled. A reading that EXCEEDS what the server's current MAXDOP can produce is impossible under
/// today's configuration, so it provably predates the change that set it and must not be counted. Each
/// store's fact collector therefore resolves the server's current MAXDOP in a <c>current_maxdop</c> CTE
/// and cross-checks the per-query reading against it. One copy per SKU, two in total.</para>
///
/// <para><b>Why it matters more than a noisy finding.</b> Lowering MAXDOP is the ordinary remediation FOR
/// this finding. Without the cross-check the finding keeps firing from plans cached before the change, so
/// its own remediation does not make it stop, and the advice it drives is to do what has already been
/// done.</para>
///
/// <para><b>The two carve-outs are load-bearing in the OTHER direction.</b> A <c>current_maxdop</c> of 0
/// (unlimited) and an absent config row both make no configuration claim, so both must leave the count
/// ALONE. Getting either backwards suppresses the finding entirely, which is worse than over-reporting
/// because nothing then surfaces it — <see cref="EveryHighDopCount_CarriesTheStalenessCrossCheck"/> and
/// <see cref="TheNullSafeJoin_IsPinnedInBothStores"/> are what hold that shape.</para>
///
/// <para><b>Scope: the predicate, not the text.</b> The two stores' SQL diverges legitimately and
/// permanently — Darling reads <c>server_config</c> directly because its store has no <c>v_</c> views,
/// where Lite must read <c>v_server_config</c> (its <c>v_</c> views union the live tables with archived
/// Parquet, and <c>Lite.Tests.AnalysisDataSpanTests.AnalysisPipeline_NeverReadsAnArchivableTableRaw</c>
/// positively REQUIRES that form under Lite's analysis tree). The config SOURCE is therefore declared per
/// file and pinned per store, while the cross-check predicate itself is dialect-free and pinned
/// identically.</para>
///
/// <para><b>Where the halves live.</b> This file does the cross-store work because <c>build.yml</c>'s
/// <c>darling</c> path filter covers the whole Darling tree AND every Lite <c>.cs</c> file, so it runs on
/// an edit to either analysis tree. Lite's twin,
/// <c>Lite.Tests.QueryHighDopStaleMaxDopParityTests</c>, declares Lite's own half and meta-pins
/// <see cref="LiteGuardCopies"/> and <see cref="CanonicalStaleMaxDopGuard"/> out of this file, so raising
/// a number or retuning a literal in one guard without the other fails. Its filter, <c>lite</c>, covers
/// Darling's test tree — exactly the edits that could weaken this file.</para>
/// </summary>
public sealed class QueryHighDopStaleMaxDopParityTests
{
    /// <summary>
    /// The cross-check, normalized: runs of whitespace collapsed to single spaces. Every copy in either
    /// store reduces to exactly this, which is the parity claim in one line. Lite's twin declares the
    /// identical literal and its meta-pin compares the two, so the floor and the unlimited sentinel
    /// cannot be retuned on one side alone.
    ///
    /// <para>Written as a concatenation of plain string segments, none containing a quote or a backslash,
    /// because Lite's meta-pin reconstructs it by reading THIS FILE as source — no test project
    /// references both SKUs' assemblies. That parser is self-validated against an in-memory copy of the
    /// same literal on both sides, so a parser bug fails loudly instead of green-washing drift.</para>
    /// </summary>
    internal const string CanonicalStaleMaxDopGuard =
        "COUNT(CASE WHEN v.max_dop > 8 " +
        "AND (m.value_in_use IS NULL OR m.value_in_use = 0 OR v.max_dop <= m.value_in_use) " +
        "THEN 1 END) AS high_dop_queries";

    /// <summary>Copies of the cross-check in Darling's analysis tree.</summary>
    internal const int DarlingGuardCopies = 1;

    /// <summary>
    /// Copies of the cross-check in LITE's analysis tree. Lite's twin declares the same number for its
    /// own half and reads this constant back out of this file, so neither side can be raised alone.
    /// </summary>
    internal const int LiteGuardCopies = 1;

    /// <summary>
    /// The cross-check, per file, across BOTH stores — a floor and a ceiling — with the config source
    /// each store legitimately reads. Naming the count per file is what makes a copy silently dropped
    /// from one SKU fail: a bare total would let a Darling copy move to Lite and still add up.
    /// </summary>
    private static readonly (string RelativePath, int Copies, string ConfigSource)[] ExpectedCopies =
    [
        ("Darling/PerformanceMonitor.Darling.Analysis/PgFactCollector.QueryPerf.cs", 1, "server_config"),
        ("Lite/Analysis/DuckDbFactCollector.QueryPerf.cs", 1, "v_server_config"),
    ];

    /// <summary>
    /// Every read that COUNTS high-DOP queries at all, guarded or not. This is the census the cross-check
    /// has to cover: the floor and its operator are captured so a retuned copy is still FOUND and then
    /// reported as a wrong value rather than vanishing. Deliberately looser than
    /// <see cref="StaleMaxDopGuard"/> — the whole point is that a count WITHOUT the cross-check still
    /// lands here.
    /// </summary>
    private static readonly Regex HighDopCount = new(
        @"COUNT\([ \t]*CASE[ \t]+WHEN[ \t]+(?:[A-Za-z_]\w*\.)?max_dop[ \t]*(?<floorOp>>=?)[ \t]*(?<floor>\d+)",
        RegexOptions.IgnoreCase);

    /// <summary>
    /// The cross-check as a shape: every token pinned literally EXCEPT the DOP floor and the unlimited
    /// sentinel, which are captured so a skewed copy is still found and reported as a wrong value.
    /// Adjacency is pinned with <c>[ \t]</c> rather than <c>\s</c> so a blank line or a reordering cannot
    /// slide past.
    /// </summary>
    private static readonly Regex StaleMaxDopGuard = new(
        @"COUNT\([ \t]*CASE[ \t]+WHEN[ \t]+v\.max_dop[ \t]*(?<floorOp>>=?)[ \t]*(?<floor>\d+)[ \t]*\r?\n"
        + @"[ \t]*AND[ \t]+\([ \t]*m\.value_in_use[ \t]+IS[ \t]+NULL[ \t]+OR[ \t]+"
        + @"m\.value_in_use[ \t]*=[ \t]*(?<unlimited>\d+)[ \t]+OR[ \t]+"
        + @"v\.max_dop[ \t]*<=[ \t]*m\.value_in_use[ \t]*\)[ \t]*\r?\n"
        + @"[ \t]*THEN[ \t]+1[ \t]+END\)[ \t]+AS[ \t]+high_dop_queries",
        RegexOptions.IgnoreCase);

    /// <summary>
    /// The CTE that resolves the server's CURRENT MAXDOP. The config source is captured because it is the
    /// one part that legitimately differs by store; everything that decides WHICH row wins is pinned,
    /// because a copy that reads the oldest capture, or an unrelated setting, would cross-check against a
    /// number that means nothing.
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
    /// config row arrive as NULL and take the leave-the-count-alone arm. An inner join instead would
    /// return NO ROWS at all on a server with no collected config, so QUERY_HIGH_DOP — and every other
    /// fact this read emits — would silently vanish rather than being over-reported.
    /// </summary>
    private static readonly Regex NullSafeJoin = new(
        @"LEFT[ \t]+JOIN[ \t]+current_maxdop[ \t]+AS[ \t]+m[ \t]+ON[ \t]+true",
        RegexOptions.IgnoreCase);

    [Fact]
    public void EveryCopyOfTheCrossCheck_IsAccountedFor_InBothStores()
    {
        foreach (var (relativePath, expected, _) in ExpectedCopies)
        {
            var path = RepoPath(relativePath);
            Assert.True(File.Exists(path), $"{relativePath} is gone — update this guard deliberately");

            var actual = StaleMaxDopGuard.Matches(ReadNormalizedSource(path)).Count;

            Assert.True(
                actual == expected,
                $"{relativePath} carries {actual} copy/copies of the QUERY_HIGH_DOP staleness cross-check, "
                + $"expected {expected}. #2705 landed on Darling only and #2999 is what that cost: the two "
                + "SKUs are ports of one another, so a one-sided change is otherwise silent. If a read moved "
                + "or was added, update this guard AND the other store in the same commit.");
        }
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

        foreach (var (relativePath, _, _) in ExpectedCopies)
        {
            var source = ReadNormalizedSource(RepoPath(relativePath));
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
            + "recommends, so the finding would outlive its own fix.");
    }

    [Fact]
    public void NoUndeclaredAnalysisRead_CountsHighDopQueries()
    {
        var declared = ExpectedCopies
            .Select(e => RepoPath(e.RelativePath))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var scanned = AnalysisSourceFiles().ToList();
        var strays = new List<string>();

        foreach (var path in scanned)
        {
            if (declared.Contains(path))
            {
                continue;
            }

            var count = HighDopCount.Matches(ReadNormalizedSource(path)).Count;
            if (count > 0)
            {
                strays.Add($"{Path.GetFileName(path)}: {count} high-DOP count(s)");
            }
        }

        /* A floor, so a broken glob cannot report a clean bill of health. 44 files across the two
           analysis projects when this was written; set well below that so ordinary growth never trips it,
           but high enough that an empty enumeration fails instead of passing. */
        Assert.True(
            scanned.Count >= 20,
            $"scanned only {scanned.Count} analysis source files — check the globs below");

        /* And every declared file must be one of the files scanned, or the census above and this sweep are
           reading different trees — which is how the sweep comes to skip exactly the files that matter. */
        var unreached = declared.Except(scanned, StringComparer.OrdinalIgnoreCase).ToList();
        Assert.True(
            unreached.Count == 0,
            "the globs below do not reach every declared file:" + Environment.NewLine
            + string.Join(Environment.NewLine, unreached));

        Assert.True(
            strays.Count == 0,
            "an analysis read counts high-DOP queries in a file this guard does not declare:"
            + Environment.NewLine + string.Join(Environment.NewLine, strays) + Environment.NewLine
            + "Add it to ExpectedCopies — with the staleness cross-check, and with the mirror copy in the "
            + "other store — in the same commit.");
    }

    [Fact]
    public void EveryCopyOfTheCrossCheck_ReducesToTheCanonicalOne_InBothStores()
    {
        var perStore = new Dictionary<string, int>(StringComparer.Ordinal) { ["Darling"] = 0, ["Lite"] = 0 };
        var skewed = new List<string>();

        foreach (var (relativePath, _, _) in ExpectedCopies)
        {
            var store = relativePath.StartsWith("Lite/", StringComparison.Ordinal) ? "Lite" : "Darling";

            foreach (Match m in StaleMaxDopGuard.Matches(ReadNormalizedSource(RepoPath(relativePath))))
            {
                perStore[store]++;

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
            + "Both stores must apply the SAME floor and the SAME unlimited sentinel, or one SKU reports a "
            + "finding the other provably excludes. Retuning them means editing both copies and both "
            + "guards' canonical literal in one commit.");

        /* Both stores must actually have contributed. A store whose copy disappeared would otherwise pass
           this test by having nothing left to disagree with. */
        Assert.Equal(DarlingGuardCopies, perStore["Darling"]);
        Assert.Equal(LiteGuardCopies, perStore["Lite"]);
    }

    /// <summary>
    /// The <c>current_maxdop</c> CTE, per store, including which config source each legitimately reads.
    /// Lite's <c>v_server_config</c> is not a style choice: its <c>v_</c> views union the live tables with
    /// archived Parquet, and <c>AnalysisPipeline_NeverReadsAnArchivableTableRaw</c> fails on the bare name.
    /// </summary>
    [Fact]
    public void TheCurrentMaxDopCte_ReadsTheRightConfigSource_InEachStore()
    {
        foreach (var (relativePath, _, expectedSource) in ExpectedCopies)
        {
            var source = ReadNormalizedSource(RepoPath(relativePath));

            var m = CurrentMaxDopCte.Match(source);
            Assert.True(
                m.Success,
                $"{relativePath} no longer resolves the server's current MAXDOP in a current_maxdop CTE "
                + "that takes the NEWEST 'max degree of parallelism' capture. Without it the cross-check "
                + "compares against nothing.");

            Assert.True(
                string.Equals(m.Groups["source"].Value, expectedSource, StringComparison.Ordinal),
                $"{relativePath}'s current_maxdop CTE reads {m.Groups["source"].Value}, expected "
                + $"{expectedSource}.");
        }
    }

    /// <summary>
    /// The NULL-safe join, in both stores. This is the carve-out that keeps an absent config row from
    /// suppressing the finding — and, because the join feeds the whole read, from suppressing every other
    /// fact it emits.
    /// </summary>
    [Fact]
    public void TheNullSafeJoin_IsPinnedInBothStores()
    {
        foreach (var (relativePath, expected, _) in ExpectedCopies)
        {
            var actual = NullSafeJoin.Matches(ReadNormalizedSource(RepoPath(relativePath))).Count;

            Assert.True(
                actual == expected,
                $"{relativePath} carries {actual} NULL-safe current_maxdop join(s), expected {expected}. "
                + "LEFT JOIN ... ON true against the one-row CTE is what delivers NULL — and therefore the "
                + "leave-the-count-alone arm — on a server with no collected config. An inner join would "
                + "return no rows at all there, silently dropping every fact this read emits.");
        }
    }

    /// <summary>
    /// The discriminators, pinned in BOTH directions against literals written for the purpose. A scan
    /// whose pattern has quietly stopped matching reports a clean bill of health, which is worse than no
    /// scan — so every negative claim above is backed by a positive control through the identical regex.
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
           collected config stops reporting, without the zero arm an unlimited server does. Both must
           leave the census — the copy is no longer the cross-check — while still being counted as a
           high-DOP read, so the pairing assertion is what reports them. */
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

        /* BARE PROJECTIONS — real lines from this corpus. max_dop is projected by both stores'
           drill-downs and activity reads; a pattern keyed on the column rather than on the COUNT would
           flag every one of them. */
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
        Assert.Equal("v_server_config", cte.Groups["source"].Value);

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

        Assert.Matches(NullSafeJoin, "LEFT JOIN current_maxdop AS m ON true\r\n");
        Assert.False(
            NullSafeJoin.IsMatch("INNER JOIN current_maxdop AS m ON true\r\n"),
            "the join discriminator accepted an inner join");
        Assert.False(
            NullSafeJoin.IsMatch("JOIN current_maxdop AS m ON true\r\n"),
            "the join discriminator accepted a bare (inner) join");
    }

    /// <summary>
    /// Lite's twin must exist and pin the SAME canonical cross-check and the same Lite-side count.
    /// Deleting or softening one guard while the other keeps claiming parity is the drift this pair
    /// exists to prevent, and <c>build.yml</c>'s <c>lite</c> filter covers Darling's test tree, so the
    /// mirror of this assertion runs on any edit to this file.
    /// </summary>
    [Fact]
    public void LitesTwinGuard_PinsTheSameCanonicalCrossCheck_AndTheSameLiteSideCount()
    {
        const string twin = "Lite.Tests/QueryHighDopStaleMaxDopParityTests.cs";
        var path = RepoPath(twin);
        Assert.True(File.Exists(path), $"Lite's counterpart guard is missing: {twin}");

        var source = ReadNormalizedSource(path);

        Assert.Equal(CanonicalStaleMaxDopGuard, ParseCanonicalCrossCheck(source, twin));

        var declared = Regex.Match(source, @"ExpectedGuardCopies\s*=\s*(?<n>\d+)\s*;");
        Assert.True(declared.Success, $"{twin} no longer declares ExpectedGuardCopies.");
        Assert.Equal(
            LiteGuardCopies,
            int.Parse(declared.Groups["n"].Value, System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// The source parser must read THIS file's own constant back correctly, or it proves nothing about
    /// Lite's. Lite's twin runs the same self-check against its own copy.
    /// </summary>
    [Fact]
    public void TheCanonicalCrossCheckParser_RoundTripsThisGuardsOwnDeclaration()
    {
        var thisFile = ThisFilePath();

        Assert.Equal(
            CanonicalStaleMaxDopGuard,
            ParseCanonicalCrossCheck(ReadNormalizedSource(thisFile), thisFile));
    }

    /// <summary>
    /// Reconstructs a <c>CanonicalStaleMaxDopGuard</c> declaration from C# source, for the half of the
    /// pair that cannot reference the other SKU's assembly.
    /// </summary>
    internal static string ParseCanonicalCrossCheck(string source, string label)
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
    /// Reads source with line endings collapsed to LF. <c>.gitattributes</c> checks the working tree out
    /// CRLF while the committed blobs are LF, and the cross-check is the same either way, so normalizing
    /// here keeps the regexes from having to care which one they are looking at.
    /// </summary>
    private static string ReadNormalizedSource(string path) =>
        File.ReadAllText(path).Replace("\r\n", "\n", StringComparison.Ordinal);

    /* -- Source location. Resolved by walking up from this file, the way the sibling scans do. -- */

    private static string ThisFilePath([CallerFilePath] string thisFile = "") => thisFile;

    private static IEnumerable<string> AnalysisSourceFiles([CallerFilePath] string thisFile = "")
    {
        foreach (var root in new[]
        {
            RepoPath("Darling/PerformanceMonitor.Darling.Analysis", thisFile),
            RepoPath("Lite/Analysis", thisFile),
        })
        {
            /* AllDirectories, not TopDirectoryOnly: Lite/Analysis has a Recommendations subdirectory, and
               an unscanned subtree is exactly how a ported copy escapes a census. */
            foreach (var path in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
            {
                if (path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                    || path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                {
                    continue;
                }

                yield return path;
            }
        }
    }

    private static string RepoPath(string relative, [CallerFilePath] string thisFile = "")
    {
        /* This file lives at <repo>/Darling/Darling.Tests/, so the repo root is two levels up. */
        var repoRoot = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
        return Path.GetFullPath(Path.Combine(repoRoot, relative.Replace('/', Path.DirectorySeparatorChar)));
    }
}
