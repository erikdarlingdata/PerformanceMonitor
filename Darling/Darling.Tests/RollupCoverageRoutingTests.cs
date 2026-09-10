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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1759 Phase 1: the router stops routing windows onto rollups that never materialized them.
///
/// <para>The defect these pin: a continuous aggregate created <c>WITH NO DATA</c> over pre-existing history
/// serves ONLY its materialized span, because the real-time watermark is a hard partition (materialized below
/// <c>UNION ALL</c> raw at-or-above) rather than a fallback. Age-only routing therefore sent old windows to a
/// rollup that answered with silence while raw still held every row — and raw kept every row precisely because
/// the #1680 arming gate held its purge closed for the same reason.</para>
///
/// <para>The hard half is NOT "fall back when the window predates the floor" — it is NOT falling back when raw
/// would return LESS. On a healthy store raw keeps ~4 days against the rollups' weeks, so a window older than
/// any floor is the NORMAL case there, and a naive floor check would send every long window to a 4-day table.
/// Half of the table below exists to hold that line.</para>
/// </summary>
public sealed class RollupCoverageRoutingTests
{
    private static readonly DateTime Now = new(2026, 7, 25, 12, 0, 0, DateTimeKind.Utc);

    private static DateTime DaysAgo(double days) => Now.AddDays(-days);

    /* ─────────────────────────── the routing truth table ─────────────────────────── */

    /// <summary>
    /// One row per store shape that actually occurs in the field. <c>windowDays</c> is the age of the window's
    /// oldest point; the three floors are ages in days (negative = the relation is EMPTY, i.e. no floor at all).
    /// </summary>
    [Theory]
    /* ── The #1759 store: rollups created over a year of pre-existing history, purges HELD, so raw is deep and
          the rollups only reach back to creation-minus-3-days. Every window past that must reach raw. ── */
    [InlineData(10, 3, 3, 365, RetentionTier.Raw)]      /* hourly-age window, hourly floor too shallow */
    [InlineData(120, 3, 3, 365, RetentionTier.Raw)]     /* daily-age window, daily floor too shallow */
    [InlineData(400, 3, 3, 365, RetentionTier.Raw)]     /* far past everything */
    [InlineData(2, 3, 3, 365, RetentionTier.Raw)]       /* recent — raw by AGE, unchanged */
    /* ── The same store AFTER --backfill-rollups: coverage reaches raw, so routing returns to the rollups.
          This is the pin that says Phase 2 actually restores acceleration rather than stranding reads on raw. ── */
    [InlineData(10, 365, 365, 365, RetentionTier.Hourly)]
    [InlineData(120, 365, 365, 365, RetentionTier.Daily)]
    /* ── A HEALTHY store with armed purges: raw keeps ~4 days, the rollups keep weeks. A window older than every
          floor is routine here, and dropping to raw would return LESS. The fallback must stay silent. ── */
    [InlineData(120, 10, 10, 4, RetentionTier.Daily)]   /* 10-day-old store: raw is shallower — stay put */
    [InlineData(10, 21, 60, 4, RetentionTier.Hourly)]   /* mature store, window inside hourly coverage */
    [InlineData(120, 60, 200, 4, RetentionTier.Daily)]  /* mature store, window inside daily coverage */
    [InlineData(400, 21, 60, 4, RetentionTier.Daily)]   /* past even the daily floor, but raw is shallower */
    /* ── #1937's REGROW PERIOD, decided by #1939: complete beats partial. A store upgraded onto the 90-day
          hourly horizon still holds only ~21 days of hourly MATERIALIZED, because the rest was purged under the
          old horizon and raw (4 days) cannot rebuild it. A 30-day window is hourly by AGE now and its floor does
          not reach — but the DAILY floor covers the whole window, so the router serves it COMPLETELY at daily
          grain rather than partially at hourly with the older weeks silently blank. A month view missing its
          older weeks is the #1937 complaint wearing routing clothes; for ~10 weeks after upgrade these stores
          render month-plus windows at daily grain, exactly as they did before the horizon moved, and gain
          hourly grain as the tier regrows. ── */
    [InlineData(30, 21, 60, 4, RetentionTier.Daily)]
    /* ...and the arm's SELF-LIMIT: when the daily floor cannot cover the window either (25d floor, 30d window),
          nothing complete exists, so the pre-#1939 behavior stands — partial at the finest tier that reaches. */
    [InlineData(30, 21, 25, 4, RetentionTier.Hourly)]
    /* ── The TRANSIENT tier-2 shape (#1759's withdrawn-overclaim comment): a daily rollup still empty while its
          hourly has materialized. A 30-day window must land on the hourly, NOT skip it to a 4-day raw. ── */
    [InlineData(30, 21, -1, 4, RetentionTier.Hourly)]
    /* ...and the same shape on a HELD store, where raw IS deeper than the hourly: raw wins. */
    [InlineData(30, 3, -1, 365, RetentionTier.Raw)]
    /* ── Everything empty (a fresh store): no evidence anywhere, so the age ladder decides. 30 days reaching
          HOURLY rather than daily is the #1937 change itself. ── */
    [InlineData(120, -1, -1, -1, RetentionTier.Daily)]
    [InlineData(30, -1, -1, -1, RetentionTier.Hourly)]
    [InlineData(10, -1, -1, -1, RetentionTier.Hourly)]
    public void Resolve_WithCoverage_PrefersTheTierThatMeasurablyReachesFurthestBack(
        double windowDays, double hourlyFloorDays, double dailyFloorDays, double rawOldestDays, RetentionTier expected)
    {
        var coverage = new TierCoverage(
            hourlyFloorDays < 0 ? null : DaysAgo(hourlyFloorDays),
            dailyFloorDays < 0 ? null : DaysAgo(dailyFloorDays),
            rawOldestDays < 0 ? null : DaysAgo(rawOldestDays));

        Assert.Equal(
            expected,
            RetentionTierRouter.Resolve(Now, DaysAgo(windowDays), hourlyAvailable: true, dailyAvailable: true, coverage));
    }

    /// <summary>
    /// WATCHED (mutation): drop the floor check — i.e. let the coverage overload behave like the age-only one —
    /// and a #1759 store routes its 30-day window at a rollup whose materialization starts 3 days ago. That is
    /// the shipped defect: an EMPTY panel over data raw still holds. The two overloads must DISAGREE here.
    /// </summary>
    [Fact]
    public void Resolve_OnAHeldStore_DivergesFromTheAgeOnlyLadder()
    {
        var held = new TierCoverage(DaysAgo(3), DaysAgo(3), DaysAgo(365));
        var window = DaysAgo(30);

        /* 30 days is HOURLY by age since #1937 (it was Daily under the 21-day horizon). The point of this
           test is unchanged: the two overloads must DISAGREE on a held store, whatever the age answer is. */
        Assert.Equal(RetentionTier.Hourly, RetentionTierRouter.Resolve(Now, window, true, true));
        Assert.Equal(RetentionTier.Raw, RetentionTierRouter.Resolve(Now, window, true, true, held));
    }

    /// <summary>
    /// Unknown coverage is INERT: a failed probe, a plain-PostgreSQL store and a partially-built one all produce
    /// nulls, and nulls must never move a window. Anything else would make a transient store hiccup silently
    /// re-route every panel — the availability guard's own #1664 lesson.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(30)]
    [InlineData(400)]
    public void Resolve_UnknownCoverage_MatchesTheAgeAndAvailabilityLadderExactly(int ageDays)
    {
        foreach (var (hourly, daily) in new[] { (true, true), (true, false), (false, true), (false, false) })
        {
            Assert.Equal(
                RetentionTierRouter.Resolve(Now, DaysAgo(ageDays), hourly, daily),
                RetentionTierRouter.Resolve(Now, DaysAgo(ageDays), hourly, daily, TierCoverage.Unknown));
        }
    }

    /// <summary>
    /// Coverage never OVERRIDES availability: a store missing the hourly view cannot be routed to it just
    /// because its floor looks deep (a floor can only be stale, never a licence to name a missing relation —
    /// the #1664 42P01 in front of the user).
    /// </summary>
    [Fact]
    public void Resolve_CoverageNeverRoutesToAnUnavailableTier()
    {
        /* Daily-age window, daily view ABSENT, hourly deep, raw deeper still. */
        var coverage = new TierCoverage(DaysAgo(200), DaysAgo(300), DaysAgo(365));

        Assert.Equal(
            RetentionTier.Hourly,
            RetentionTierRouter.Resolve(Now, DaysAgo(30), hourlyAvailable: true, dailyAvailable: false, coverage));

        /* Both views absent: raw, exactly as availability alone decides. */
        Assert.Equal(
            RetentionTier.Raw,
            RetentionTierRouter.Resolve(Now, DaysAgo(30), hourlyAvailable: false, dailyAvailable: false, coverage));
    }

    /// <summary>An exactly-on-the-floor window is COVERED — the floor is the oldest bucket the rollup holds, so a
    /// window starting there is fully answerable. Off by one in this direction would strand a whole bucket.</summary>
    [Fact]
    public void Covers_IsInclusiveOfTheFloorItself()
    {
        Assert.True(TierCoverage.Covers(DaysAgo(10), DaysAgo(10)));
        Assert.True(TierCoverage.Covers(DaysAgo(10), DaysAgo(9)));
        Assert.False(TierCoverage.Covers(DaysAgo(10), DaysAgo(10.001)));

        /* A null floor covers NOTHING — the #1759 defect in one line. */
        Assert.False(TierCoverage.Covers(null, DaysAgo(1)));
    }

    /// <summary>
    /// The comparison that keeps the fix from becoming its own regression. A null candidate never wins (no
    /// evidence beats no evidence), a null incumbent loses to any real measurement (a tier holding nothing loses
    /// to one holding something), and equal floors do NOT trigger a move — otherwise a store whose tiers are all
    /// the same age would churn down the ladder for no gain.
    /// </summary>
    [Fact]
    public void ReachesFurtherBack_RequiresPositiveEvidenceAndAStrictImprovement()
    {
        Assert.True(TierCoverage.ReachesFurtherBack(DaysAgo(365), DaysAgo(3)));
        Assert.False(TierCoverage.ReachesFurtherBack(DaysAgo(3), DaysAgo(365)));
        Assert.False(TierCoverage.ReachesFurtherBack(DaysAgo(10), DaysAgo(10)));
        Assert.True(TierCoverage.ReachesFurtherBack(DaysAgo(4), null));
        Assert.False(TierCoverage.ReachesFurtherBack(null, DaysAgo(10)));
        Assert.False(TierCoverage.ReachesFurtherBack(null, null));
    }

    /* ─────────────────────────── the coverage probe ─────────────────────────── */

    /// <summary>
    /// The probe must name every view the router can route to, read the SAME <c>min(bucket)</c> the arming gate
    /// reads, and carry the raw floor for each rolled table (without which the router could not tell a held
    /// store from a healthy one and would have no basis to fall back at all).
    /// </summary>
    [Fact]
    public void CoverageProbe_CoversEveryRoutedViewAndEveryRolledRawTable()
    {
        var sql = TimescaleSupport.RollupCoverageProbeSql(RollupAvailability.All);

        foreach (var view in new[]
        {
            TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView,
            TimescaleSupport.QueryStatsDbHourlyView, TimescaleSupport.QueryStatsDbDailyView,
            TimescaleSupport.ProcedureStatsHourlyView, TimescaleSupport.ProcedureStatsDailyView,
            TimescaleSupport.QueryStoreStatsHourlyView, TimescaleSupport.QueryStoreStatsDailyView,
        })
        {
            Assert.Contains($"(SELECT min(bucket) FROM collect.{view})", sql, StringComparison.Ordinal);

            /* And every routed view must resolve to a raw table, or its ladder would have no floor to fall to. */
            Assert.NotNull(RollupCoverage.RawTableFor(view));
        }

        foreach (var rawTable in TimescaleSupport.RolledRawTables)
        {
            Assert.Contains($"(SELECT min(collection_time) FROM collect.{rawTable})", sql, StringComparison.Ordinal);
        }

        /* The probe's view list IS the availability probe's view list — a rollup added to one and not the other
           would route on evidence the other never gathered. */
        Assert.Equal(
            TimescaleSupport.RollupViews.Select(r => r.View).OrderBy(v => v, StringComparer.Ordinal).ToArray(),
            TimescaleSupport.RollupViews.Select(r => r.View).Where(RollupAvailability.All.Has)
                .OrderBy(v => v, StringComparer.Ordinal).ToArray());

        Assert.Null(RollupCoverage.RawTableFor("some_future_rollup"));
    }

    /// <summary>
    /// WATCHED (mutation): name an absent view anyway and the probe raises 42P01, killing the whole cycle — so
    /// coverage would be permanently unknown on exactly the partially-built stores #1664 exists for. A relation
    /// is resolved at PARSE time, so no in-statement <c>to_regclass</c> guard can save it; the SQL must simply
    /// not mention it. The column COUNT stays fixed either way, or the reader's ordinals would shift with the
    /// store's shape.
    /// </summary>
    [Fact]
    public void CoverageProbe_NeverNamesAViewTheStoreDoesNotHave()
    {
        var partial = RollupAvailability.All with { QueryGrainDaily = false, QueryStoreGrainHourly = false };
        var sql = TimescaleSupport.RollupCoverageProbeSql(partial);

        Assert.DoesNotContain($"collect.{TimescaleSupport.QueryStatsDailyView}", sql, StringComparison.Ordinal);
        Assert.DoesNotContain($"collect.{TimescaleSupport.QueryStoreStatsHourlyView}", sql, StringComparison.Ordinal);
        Assert.Contains($"collect.{TimescaleSupport.QueryStatsHourlyView}", sql, StringComparison.Ordinal);

        /* Plain PostgreSQL: no rollup named at all, but the raw floors still read. */
        var none = TimescaleSupport.RollupCoverageProbeSql(RollupAvailability.None);
        foreach (var (view, _, _, _, _) in TimescaleSupport.RollupViews)
        {
            Assert.DoesNotContain($"collect.{view}", none, StringComparison.Ordinal);
        }

        Assert.Contains("min(collection_time) FROM collect.query_stats)", none, StringComparison.Ordinal);

        /* Fixed column count across every shape (every rollup + the 3 raw tables), derived from the source
           lists so adding a rollup cannot silently leave the reader indexing past the end. */
        foreach (var shape in new[] { RollupAvailability.All, partial, RollupAvailability.None })
        {
            var columns = TimescaleSupport.RollupCoverageProbeSql(shape)["SELECT ".Length..].Split(", ");
            Assert.Equal(TimescaleSupport.RollupViews.Length + TimescaleSupport.RolledRawTables.Length, columns.Length);
        }
    }

    /* ─────────────────────────── the per-ladder lookup ─────────────────────────── */

    /// <summary>
    /// <see cref="RollupCoverage.For"/> must hand each ladder ITS OWN floors and the raw table underneath it —
    /// mixing two tables' floors would let a well-covered query_stats rollup vouch for a starved
    /// procedure_stats one, which is exactly the per-table isolation #1665 established for availability.
    /// </summary>
    [Fact]
    public void For_ResolvesEachLaddersOwnFloorsAndItsOwnRawTable()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal)
            {
                [TimescaleSupport.QueryStatsHourlyView] = DaysAgo(20),
                [TimescaleSupport.QueryStatsDailyView] = DaysAgo(60),
                [TimescaleSupport.ProcedureStatsHourlyView] = DaysAgo(3),
            },
            new Dictionary<string, DateTime>(StringComparer.Ordinal)
            {
                ["query_stats"] = DaysAgo(4),
                ["procedure_stats"] = DaysAgo(365),
            });

        var query = coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView);
        Assert.Equal(DaysAgo(20), query.HourlyFloorUtc);
        Assert.Equal(DaysAgo(60), query.DailyFloorUtc);
        Assert.Equal(DaysAgo(4), query.RawOldestUtc);

        /* procedure_stats: same store, opposite shape — starved rollup over deep raw (the held-purge case). */
        var procedure = coverage.For(TimescaleSupport.ProcedureStatsHourlyView, TimescaleSupport.ProcedureStatsDailyView);
        Assert.Equal(DaysAgo(3), procedure.HourlyFloorUtc);
        Assert.Null(procedure.DailyFloorUtc);
        Assert.Equal(DaysAgo(365), procedure.RawOldestUtc);

        /* And they route differently on the same store, same window. */
        Assert.Equal(RetentionTier.Daily, RetentionTierRouter.Resolve(Now, DaysAgo(30), true, true, query));
        Assert.Equal(RetentionTier.Raw, RetentionTierRouter.Resolve(Now, DaysAgo(30), true, true, procedure));

        /* A pair with no daily view answers with a null daily floor rather than guessing. */
        Assert.Null(coverage.For(TimescaleSupport.QueryStatsHourlyView, null).DailyFloorUtc);

        /* Unknown answers null everywhere — the inert shape. */
        Assert.Null(RollupCoverage.Unknown.FloorOf(TimescaleSupport.QueryStatsHourlyView));
        Assert.Null(RollupCoverage.Unknown.RawOldestOf("query_stats"));
    }

    /* ─────────────────────────── the drift guard ─────────────────────────── */

    /// <summary>
    /// Every PRODUCTION caller of the tier router must pass coverage. The compiler cannot enforce this: the
    /// four-argument availability overload is legitimate (the coverage form is built on it, and the tests above
    /// compare the two), so a new reader added without coverage compiles perfectly and silently serves the
    /// #1759 defect.
    ///
    /// <para>This is not hypothetical. Routing readers arrive one at a time and each is a separate file:
    /// #1661 added the composer, #1664 the viewer's built-in tabs, #1665 the composer's per-table flags, and
    /// the MCP <c>get_daily_health</c> reader answers the SAME question as the viewer's calendar off the SAME
    /// SQL — so a reader left un-gated would not just be stale, it would disagree with its own twin about how
    /// many queries ran on a given day, on exactly the stores this issue is about.</para>
    ///
    /// <para>Matched by CONTAINMENT, not by argument position or formatting, so reformatting or renaming a
    /// local cannot break it. The predicate is deliberately TWO-SIDED — a positive match on a real coverage
    /// lookup (<c>.For(</c>) AND an explicit rejection of <c>TierCoverage.Unknown</c>.</para>
    ///
    /// <para><b>Why two-sided, learned the hard way.</b> The first cut of this guard only checked that the
    /// argument list contained the substring <c>"overage"</c> — which is a substring of the TYPE NAME
    /// <c>TierCoverage</c>, so a reader passing <c>TierCoverage.Unknown</c> SATISFIED it. That form is the
    /// canonical un-gated value, it COMPILES, and it reads as deliberate, which makes it the single most
    /// probable way #1759 comes back: not by someone dropping an argument (a compile-shaped review catches
    /// that, and it was the only form the guard had been verified against), but by someone reaching for the
    /// inert value because it was the easy way to satisfy a signature. The guard standing between this defect
    /// and its own return has to see the likely path back, not just the obvious one. Both forms are now
    /// pinned as mutations.</para>
    ///
    /// <para><c>TierCoverage.Unknown</c> is legitimate in the TESTS — they use it to prove the coverage
    /// overload matches the age-only ladder exactly — which is why the scan excludes the test project. In a
    /// production reader it means "route this window on no evidence", and if that is ever genuinely wanted it
    /// should have to break this guard and be argued for, not slip through.</para>
    /// </summary>
    [Fact]
    public void EveryProductionRoutingCaller_PassesCoverage()
    {
        var root = FindRepoRoot();

        /* FAIL rather than skip when the tree cannot be found — a guard that silently skips is a guard that
           silently stopped guarding, which is this whole file's failure mode. */
        Assert.True(root is not null,
            "Could not locate the repository root (walked up from the test binary looking for " +
            "PerformanceMonitor.sln). This test scans the source tree, so it cannot run without it — fix the " +
            "walk-up rather than skipping.");

        /* The call plus its argument list, up to the closing paren of the call itself. */
        var callPattern = new Regex(@"RetentionTierRouter\.Resolve\(([^;]*?)\)\s*;", RegexOptions.Singleline);

        var offenders = new List<string>();
        var scanned = 0;
        foreach (var file in Directory.EnumerateFiles(Path.Combine(root!, "Darling"), "*.cs", SearchOption.AllDirectories))
        {
            if (IsExcludedFromScan(Path.GetRelativePath(root!, file)))
            {
                continue;
            }

            /* COMMENTS STRIPPED FIRST. A <see cref="RetentionTierRouter.Resolve(…, TierCoverage)"/> in a doc
               comment is not a call site, and the pattern spans newlines looking for the closing ");" — so a
               cref matched forward into whatever real code followed it. The old one-sided predicate hid that:
               the cref text contains the type name, so "mentions coverage" was satisfied and the phantom
               match passed silently while also inflating the rot-detector's count. */
            var text = StripComments(File.ReadAllText(file));
            foreach (Match match in callPattern.Matches(text))
            {
                scanned++;
                var arguments = match.Groups[1].Value;

                /* TWO-SIDED. A positive match on a real coverage lookup, AND a rejection of the inert value.
                   Either half alone is holed: "mentions coverage" is satisfied by the TYPE NAME in
                   TierCoverage.Unknown, and "does not say Unknown" is satisfied by passing nothing at all. */
                var lookedUpCoverage = arguments.Contains(".For(", StringComparison.Ordinal);
                var routedOnNoEvidence = arguments.Contains("TierCoverage.Unknown", StringComparison.Ordinal);

                if (!lookedUpCoverage || routedOnNoEvidence)
                {
                    var line = text.AsSpan(0, match.Index).Count('\n') + 1;
                    offenders.Add(
                        $"{Path.GetRelativePath(root!, file)}:{line}" +
                        (routedOnNoEvidence ? "  (passes TierCoverage.Unknown — routes on NO evidence)" : "  (passes no coverage lookup)"));
                }
            }
        }

        /* Rot detector, not a census: if the scan stops matching, the guard passes vacuously and nobody
           notices. Six production call sites today (the composer, the MCP daily-health reader, the viewer's
           calendar, and three FinOps readers). A legitimate REMOVAL should lower this deliberately rather
           than be absorbed silently. */
        Assert.True(scanned >= 6, $"expected to find the production routing callers, but matched {scanned} call(s) — the scan has rotted.");

        Assert.True(offenders.Count == 0,
            "These readers reach the tier router without real coverage, so on a store whose rollups were " +
            "created over pre-existing history they will serve EMPTY results for windows raw still holds " +
            "(#1759). Pass the matching coverage.For(hourlyView, dailyView).\n\n" +
            "TierCoverage.Unknown does NOT count. It means \"route on no evidence\" and is the inert value the " +
            "tests use to prove the coverage overload matches the age-only ladder — reaching for it in a " +
            "production reader satisfies the signature and reinstates the defect. If a reader genuinely has no " +
            "coverage available, that is a design question, not a value to paste.\n\n" +
            "And if this reader has a twin answering the same question elsewhere, gate BOTH or the two will " +
            "disagree:\n\n" +
            string.Join("\n", offenders));
    }

    /// <summary>
    /// True when a path is generated build output, or the test project's own source. Compared as whole
    /// path SEGMENTS against BOTH separator characters, so the scanned set is the same wherever this runs
    /// and a directory merely named <c>Objects</c> stays in it.
    ///
    /// <para><b>Both separators.</b> A substring test for <c>\bin\</c> matches nothing where the separator
    /// is <c>/</c>, so off Windows this scan would take in the generated <c>.AssemblyInfo.cs</c> under
    /// <c>obj/</c> AND the test project — and then report the test project's deliberate un-gated calls as
    /// production offenders, which is a guard whose verdict depends on who ran it.</para>
    ///
    /// <para><b>The test project is excluded</b> because it calls the un-gated overload on purpose, to
    /// prove the coverage-aware ladder and the age-only one agree. The same call in production code routes
    /// a window on no evidence, which is what this file forbids.</para>
    /// </summary>
    private static bool IsExcludedFromScan(string relativePath) =>
        relativePath.Split('/', '\\')
            .Any(s => string.Equals(s, "bin", StringComparison.OrdinalIgnoreCase)
                      || string.Equals(s, "obj", StringComparison.OrdinalIgnoreCase)
                      || string.Equals(s, "Darling.Tests", StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// <see cref="IsExcludedFromScan"/> reaches the same verdict for a path written with either separator.
    /// The cases are written out rather than composed with <see cref="Path.Combine"/>, which emits only the
    /// HOST's separator and so would leave one flavour unasserted wherever this ran.
    /// </summary>
    [Theory]
    [InlineData(true, "Darling/Darling.Tests/RollupCoverageRoutingTests.cs")]
    [InlineData(true, @"Darling\Darling.Tests\RollupCoverageRoutingTests.cs")]
    [InlineData(true, "Darling/PerformanceMonitor.Darling.Storage/obj/Debug/net10.0/Generated.cs")]
    [InlineData(true, @"Darling\PerformanceMonitor.Darling.Storage\obj\Debug\net10.0\Generated.cs")]
    [InlineData(true, "Darling/PerformanceMonitor.Darling.Storage/bin/Debug/net10.0/Generated.cs")]
    [InlineData(true, @"Darling\PerformanceMonitor.Darling.Storage\bin\Debug\net10.0\Generated.cs")]
    [InlineData(false, "Darling/PerformanceMonitor.Darling.Storage/RetentionTierRouter.cs")]
    [InlineData(false, @"Darling\PerformanceMonitor.Darling.Storage\RetentionTierRouter.cs")]
    /* Segments, not substrings: source that merely CONTAINS an excluded name stays in the scan. */
    [InlineData(false, "Darling/PerformanceMonitor.Darling.Storage/Objects/ObjectStore.cs")]
    [InlineData(false, @"Darling\PerformanceMonitor.Darling.Storage\Objects\ObjectStore.cs")]
    public void TheScanExclusionReadsBothSeparatorsTheSameWay(bool excluded, string relativePath)
    {
        Assert.True(
            IsExcludedFromScan(relativePath) == excluded,
            $"'{relativePath}' should{(excluded ? " " : " NOT ")}be excluded from the production scan. " +
            "A separator-specific exclusion gives this guard a different set of files on Windows than on " +
            "macOS or Linux: off Windows it would read the test project, whose un-gated calls are " +
            "deliberate, and report them as production defects.");
    }

    /// <summary>
    /// Blanks out block and line comments so the scan sees only CODE, preserving line count (comment bodies
    /// become blank lines / spaces) so reported line numbers still point where a reader can open them.
    ///
    /// <para>Crude by design: a <c>//</c> inside a string literal is blanked too. That is harmless here —
    /// mangling a literal cannot manufacture a <c>RetentionTierRouter.Resolve(…);</c> call that was not
    /// already there — and the alternative is a C# lexer in a drift guard.</para>
    /// </summary>
    private static string StripComments(string source)
    {
        var withoutBlocks = Regex.Replace(
            source,
            @"/\*.*?\*/",
            m => Regex.Replace(m.Value, @"[^\r\n]", " "),
            RegexOptions.Singleline);

        return Regex.Replace(withoutBlocks, @"//[^\r\n]*", m => new string(' ', m.Value.Length));
    }

    /// <summary>Walks up from the test output directory to the repo root — the directory holding
    /// <c>PerformanceMonitor.sln</c>. Same walk-up idiom as <c>DocCommentHygieneTests.FindRepoRoot</c>.</summary>
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
