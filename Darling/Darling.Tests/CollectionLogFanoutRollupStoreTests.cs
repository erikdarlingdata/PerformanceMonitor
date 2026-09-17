/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The V80 rung (#2472) — the per-database fan-out rollup on <c>collection_log</c>. No longer the top of the
/// ladder: V81 (#2515) took that, and the assertions that belong to whichever rung is newest moved with it to
/// <see cref="TempDbCeilingStoreTests"/>. What stays here is everything that is true of THIS rung forever.
///
/// <para><b>What it is for.</b> Five collectors run once per DATABASE and the run writes ONE row whose
/// <c>duration_ms</c> is the sum across all of them. So 8 databases at 10.1s and one at 62s beside seven at
/// 2.7s are both 80,900 ms, and they want opposite fixes — bounded parallelism for the first, a per-database
/// override for the second. #2468 could not be decided because nothing recorded which it was.</para>
///
/// <para><b>Why not the tail statistics.</b> #2460 gave every collector <c>p95_duration_ms</c> and
/// <c>max_duration_ms</c>, which is a real improvement and does not help here:
/// <see cref="TheTailStatistics_CannotSeparateTheTwoShapes"/> is the arithmetic. Both aggregate over RUNS,
/// and each of those runs is one blended row.</para>
/// </summary>
public class CollectionLogFanoutRollupStoreTests
{
    [Fact]
    public void TheRungIsRegisteredInADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("collection-log-fanout-rollup", PgMigrations.Scripts.Single(s => s.Version == 80).Name);

        /* Demoted at V81 (#2515): "80 is the maximum" was true of the LADDER while this was its top rung,
           not of this rung, and leaving it here is how a demotion turns into a red build on the next one.
           The density and ordering checks below stay — those are properties of the whole ladder that every
           rung's test may assert. */
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// Three nullable columns and a view refresh. Nullable with no DEFAULT is the whole reason this rung is
    /// safe to run on the largest table in the store: it is a catalog-only change in PostgreSQL and stays
    /// instant on a compressed hypertable, where adding a column WITH a default is the shape TimescaleDB has
    /// historically refused.
    /// </summary>
    [Fact]
    public void TheRungAddsTheColumns_Idempotently_AndWithoutADefault()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == 80).Sql;

        Assert.Contains("ALTER TABLE collect.collection_log", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS fanout_item_count integer", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS slowest_item text", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS slowest_item_ms integer", sql, StringComparison.Ordinal);

        /* No DEFAULT on any of the three, and no backfill. A row written before this rung genuinely does not
           know its fan-out; NULL says so, where 0 would read as "fanned out over nothing". */
        Assert.DoesNotContain("DEFAULT", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("UPDATE ", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The view refresh, which is the half of this rung that is easy to forget and impossible to notice.
    /// Postgres FREEZES a view's <c>SELECT *</c> column list at CREATE, so without this the passthrough every
    /// read goes through would keep serving eleven columns forever and the new ones would be invisible to a
    /// store that was upgraded rather than created. V14 exists because that already happened once.
    /// </summary>
    [Fact]
    public void TheRungRefreshesThePassthroughView()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == 80).Sql;

        Assert.Contains(
            "CREATE OR REPLACE VIEW collect.v_collection_log AS SELECT * FROM collect.collection_log;",
            sql, StringComparison.Ordinal);

        /* And the reads really do go through the view rather than the table, which is what makes the refresh
           load-bearing rather than tidy. */
        Assert.Contains("FROM v_collection_log", DarlingDataReader.CollectionHealthSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeAsksForTheColumn_AndTheReaderStillFetchesItsOrdinal()
    {
        Assert.Contains(
            "table_name = 'collection_log' AND column_name = 'slowest_item_ms'",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        /* Demoted at V81 (#2515): this used to read `mapParameters - 1`, which is the NEWEST sentinel's
           ordinal, so once a rung was appended it silently started testing that rung's wiring instead of
           this one's. Pinned at 55 — this rung's own ordinal — which cannot slide. The "and no more than
           that" half is the top rung's to assert, and it moved to TempDbCeilingStoreTests with it. */
        Assert.Contains("reader.GetBoolean(55)", ReadViewerSource(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeMapsAStoreAtExactly80To80()
    {
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* 55 positional sentinels, then this rung's own, then FALSE for anything a later rung appends. The
           leading count is FIXED at this rung's ordinal deliberately — see the note on V79's twin: deriving
           it from arity reads identically while this is the top rung, then slides one place right per new
           rung, and the assertion keeps passing while quietly testing a newer arm. V81 has since been
           appended and this test needed no edit, which is the fixed count doing exactly what it is for. */
        var all = Enumerable.Repeat(true, 55).Cast<object>().ToArray();
        object[] Args(bool ownFlag) => all
            .Concat(new object[] { ownFlag })
            .Concat(Enumerable.Repeat((object)false, arity - 56))
            .ToArray();

        Assert.Equal(80, (int)method.Invoke(null, Args(true))!);
        Assert.Equal(79, (int)method.Invoke(null, Args(false))!);
    }

    /// <summary>
    /// THE ARITHMETIC — the reason this rung exists rather than a note saying the existing columns are
    /// enough. Two fan-outs that any run-level statistic reports identically, separated by the rollup.
    ///
    /// <para>Written as a test rather than as prose in the issue because "max and p95 cannot answer this" is
    /// a claim that would otherwise have to be re-derived by hand every time someone proposes deleting these
    /// three columns.</para>
    /// </summary>
    [Fact]
    public void TheTailStatistics_CannotSeparateTheTwoShapes()
    {
        /* #2472 writes these as 8 x 10.1s and 1 x 62s + 7 x 2.7s, which round to the same 80,900 ms but do
           not actually sum to it (8 x 10,100 is 80,800). The figures below are the issue's shapes made to
           balance EXACTLY, because "these two are indistinguishable" is the claim under test and it cannot
           be demonstrated with two numbers that differ by 100. */
        var even = new[] { 10_100, 10_100, 10_100, 10_100, 10_100, 10_100, 10_100, 10_100 };
        var dominated = new[] { 61_900, 2_700, 2_700, 2_700, 2_700, 2_700, 2_700, 2_700 };

        /* The same run, as collection_log records it: one row, one duration. Every run-level aggregate over
           a window of such runs — AVG, MAX, PERCENTILE_DISC — therefore sees the same number for both. */
        Assert.Equal(80_800, even.Sum());
        Assert.Equal(80_800, dominated.Sum());
        Assert.Equal(even.Sum(), dominated.Sum());

        var evenHealth = Health(even);
        var dominatedHealth = Health(dominated);

        /* And now they are told apart, by the numbers the rollup adds. */
        Assert.Equal(1.0, evenHealth.FanoutDominance!.Value, 2);
        Assert.Equal(6.13, dominatedHealth.FanoutDominance!.Value, 2);

        /* The verdict the remedies turn on rides the SHARE (#3502): the even fan-out's slowest item is an
           eighth of its pass, the dominated one's is over three-quarters. Dominance agrees with the share
           here only because both runs are 8 items wide — at equal width the two figures are one ordering
           scaled — and TheShare_IsTheVerdict pins the case where the widths differ and near-neighbour
           dominance scores carry shares 5.6x apart. */
        Assert.Equal(12.5, evenHealth.FanoutSlowestSharePercent!.Value, 2);
        Assert.Equal(76.61, dominatedHealth.FanoutSlowestSharePercent!.Value, 2);
    }

    /// <summary>
    /// #3502's counter-example, preserved because it is short, real, and permanently instructive. The
    /// dominance ratio is the slowest item against the MEAN item, so its ceiling is <c>items</c> and its
    /// reading depends on the width — which the tool description's guidance ignored ("around 2.0 or above
    /// one database dominates"), and at least one triage conversation followed it into the wrong remedy.
    /// The decision-relevant quantity is the slowest item's share of the PASS: slowest_ms / run_ms, which
    /// is dominance / items, published so no reader is left one division and one misreading away from it.
    /// </summary>
    [Fact]
    public void TheShare_IsTheVerdict_AndDominanceAloneMisreadsTheWideInstance()
    {
        /* The #3477 reporter's two instances, to the published precision: 72 databases with the slowest at
           4.97% of the pass, and 15 with the slowest at 27.98% of one. */
        var wide = new CollectorHealth
        {
            FanoutItems = 72,
            SlowestItem = "worst",
            SlowestItemMs = 4_970,
            SlowestRunDurationMs = 100_000,
        };
        var concentrated = new CollectorHealth
        {
            FanoutItems = 15,
            SlowestItem = "worst",
            SlowestItemMs = 27_980,
            SlowestRunDurationMs = 100_000,
        };

        /* Near-neighbour dominance scores, both far above the retired "around 2.0" bar — so a threshold on
           dominance calls these the SAME shape. */
        Assert.Equal(3.58, wide.FanoutDominance!.Value, 2);
        Assert.Equal(4.20, concentrated.FanoutDominance!.Value, 2);

        /* The shares are 5.6x apart and want opposite remedies: 4.97% is width (no single database worth
           chasing), 27.98% is one database owning over a quarter of the pass (a per-database override or a
           stagger reaches exactly it). */
        Assert.Equal(4.97, wide.FanoutSlowestSharePercent!.Value, 2);
        Assert.Equal(27.98, concentrated.FanoutSlowestSharePercent!.Value, 2);

        /* The issue's own limiting case: at 72 items, even a dominance of 10 is one database holding 14%
           of a pass — the ratio's ceiling is items, so at width it cannot mean what the guidance said. */
        var wideAtTen = new CollectorHealth
        {
            FanoutItems = 72,
            SlowestItem = "worst",
            SlowestItemMs = 10_000,
            SlowestRunDurationMs = 72_000,
        };
        Assert.Equal(10.0, wideAtTen.FanoutDominance!.Value, 2);
        Assert.Equal(13.89, wideAtTen.FanoutSlowestSharePercent!.Value, 2);

        /* One run whose slowest item IS the whole pass: the share saturates at 100 and dominance reads the
           width — the two figures' relationship (share = dominance / items) stated at its endpoint. */
        var whole = new CollectorHealth
        {
            FanoutItems = 4,
            SlowestItem = "worst",
            SlowestItemMs = 8_000,
            SlowestRunDurationMs = 8_000,
        };
        Assert.Equal(100.0, whole.FanoutSlowestSharePercent!.Value, 2);
        Assert.Equal(4.0, whole.FanoutDominance!.Value, 2);

        /* And the pin that the two figures can never drift apart or describe different runs: the share is
           DERIVED from dominance (share = dominance / items * 100), asserted here over every populated row
           this test built, so a future edit that recomputes one from different columns fails loudly. */
        foreach (var health in new[] { wide, concentrated, wideAtTen, whole })
        {
            Assert.Equal(
                health.FanoutDominance!.Value / health.FanoutItems!.Value * 100,
                health.FanoutSlowestSharePercent!.Value,
                10);
        }
    }

    /// <summary>
    /// A collector that does not fan out reports nothing rather than a zero. NULL is the honest answer for
    /// "this run had no fan-out", which is not the same claim as "its fan-out was free" — the sentinel
    /// discipline the PostgreSQL collectors already hold to.
    /// </summary>
    [Fact]
    public void ACollectorThatDoesNotFanOut_ReportsNothingRatherThanZero()
    {
        var plain = new CollectorHealth { AvgDurationMs = 42 };

        Assert.Null(plain.FanoutItems);
        Assert.Null(plain.SlowestItem);
        Assert.Null(plain.FanoutDominance);
        Assert.Null(plain.FanoutSlowestSharePercent);

        /* And a fan-out whose run somehow recorded no duration is null too: a ratio against nothing is a
           wrong answer, not a smaller one. */
        var zeroRun = new CollectorHealth
        {
            FanoutItems = 8,
            SlowestItem = "db",
            SlowestItemMs = 10,
            SlowestRunDurationMs = 0,
        };
        Assert.Null(zeroRun.FanoutDominance);

        /* The share holds the same line, by construction rather than by a twin guard: it is derived from
           dominance, so it is null exactly when dominance is — never a 0% that would read as "perfectly
           even" on a run that reported nothing. */
        Assert.Null(zeroRun.FanoutSlowestSharePercent);
    }

    /// <summary>
    /// The accumulator both SKUs and both fan-out shapes feed. Empty batches count: their read time is in the
    /// blended total, so leaving them out would inflate the dominance of whichever database had rows — which
    /// is precisely the wrong database to send an operator after.
    /// </summary>
    [Fact]
    public void TheAccumulator_CountsEveryItemAndKeepsTheDearest()
    {
        var acc = new FanoutCostAccumulator();
        Assert.Null(acc.Result);

        acc.Observe("alpha", 2_700);
        acc.Observe("bravo", 62_000);
        acc.Observe("charlie", 0);

        var result = acc.Result!.Value;
        Assert.Equal(3, result.ItemCount);
        Assert.Equal("bravo", result.SlowestItem);
        Assert.Equal(62_000, result.SlowestItemMs);

        /* A single free item still becomes the slowest one — the floor is -1, not 0, so a fan-out that
           really did run never reports a null item. */
        var free = new FanoutCostAccumulator();
        free.Observe("only", 0);
        Assert.Equal("only", free.Result!.Value.SlowestItem);
        Assert.Equal(1, free.Result!.Value.ItemCount);

        /* Ties keep the first item seen, so the answer does not wobble between equally-priced databases from
           one cycle to the next. An operator chasing a name needs it to be the same name tomorrow.
           Adversarially named so an alphabetical tie-break would pick the other one. */
        var tie = new FanoutCostAccumulator();
        tie.Observe("zulu", 500);
        tie.Observe("alpha", 500);
        Assert.Equal("zulu", tie.Result!.Value.SlowestItem);
    }

    /// <summary>
    /// The tool that serves this names the enumeration-driven collectors it applies to, and the list is
    /// DERIVED from the collector sources rather than typed out here — the first draft of this test hand-typed
    /// five names, which is the same failure mode as the prose it was meant to guard.
    ///
    /// <para>The issue said "query_store plus the two snapshot ones" and a comment in the runner said the
    /// same; two more had joined since (<c>query_store_health</c> #2319, <c>plan_correction</c> #1952). And
    /// the description's FIRST version then made the opposite mistake, claiming five collectors fan out full
    /// stop — there are two mechanisms, and <c>RunsPerDatabase</c> puts eight more on a per-database
    /// connection loop on Azure SQL DB plus <c>pg_autovacuum_stats</c> always. Both are asserted, because
    /// the accumulator feeds from both and a caller told only half would look past a collector that has a
    /// <c>fanout</c> block.</para>
    ///
    /// <para>Pinned on the DESCRIPTION because that is what a caller reads before deciding whether the block
    /// applies to the collector in front of them.</para>
    /// </summary>
    [Fact]
    public void TheToolDescription_NamesEveryCollectorThatFansOut()
    {
        var description = ReadRepoFile(System.IO.Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"));

        /* Derived, not remembered: every collector source that overrides BuildEnumerationQuery drives the
           enumeration fan-out, so a sixth one starting to enumerate fails here instead of quietly falling
           out of the description. */
        var collectorsDir = System.IO.Path.Combine(RepoFile.Root, "PerformanceMonitor.Collectors");
        var enumerating = System.IO.Directory
            .EnumerateFiles(collectorsDir, "*Collector.cs", System.IO.SearchOption.TopDirectoryOnly)
            .Where(f => System.IO.File.ReadAllText(f).Contains("override CollectorQuery? BuildEnumerationQuery", StringComparison.Ordinal))
            .Select(f => System.IO.Path.GetFileNameWithoutExtension(f))
            .ToList();

        Assert.Equal(5, enumerating.Count);

        foreach (var typeName in enumerating)
        {
            /* CollectorName is the snake_case name the description writes; derive it from the type name so
               this needs no second hand-typed mapping either. */
            var snake = string.Concat(typeName[..^"Collector".Length]
                .Select((c, i) => char.IsUpper(c) && i > 0 ? "_" + char.ToLowerInvariant(c) : char.ToLowerInvariant(c).ToString()));

            Assert.Contains(snake, description, StringComparison.Ordinal);
        }

        /* The OTHER mechanism, which the description's first version left out entirely. */
        Assert.Contains("per-database connection loop when the target is Azure SQL DB", description, StringComparison.Ordinal);
        Assert.Contains("pg_autovacuum_stats", description, StringComparison.Ordinal);

        /* And both SKUs say the same thing, because the payload is field-identical and a caller should not
           have to learn which product it is talking to. */
        var lite = ReadRepoFile(System.IO.Path.Combine("Lite", "Mcp", "McpHealthTools.cs"));

        /* The formula itself, with no surrounding punctuation: the first version of this assertion included
           a word the description writes in backticks and matched neither file. */
        Assert.Contains("slowest_ms * items / run_ms", lite, StringComparison.Ordinal);
        Assert.Contains("slowest_ms * items / run_ms", description, StringComparison.Ordinal);

        /* #3502: the share's formula beside it, and the guidance routed through the share — the description
           told readers to take the width-versus-concentration decision on dominance ("around 2.0 or above
           one database dominates"), which misreads exactly the widest fan-outs. Both SKUs, same reason as
           the dominance pin above. */
        foreach (var toolSource in new[] { description, lite })
        {
            Assert.Contains("slowest_ms / run_ms", toolSource, StringComparison.Ordinal);
            Assert.Contains("The remediation decision routes through the SHARE", toolSource, StringComparison.Ordinal);
            Assert.DoesNotContain("around 2.0 or above one database dominates", toolSource, StringComparison.Ordinal);

            /* The WIRING, because the fields are emitted by an anonymous object inside a tool method that
               needs a live store to invoke — the #3010 lesson, where a mutation removing a field left the
               whole suite green. Pinned with its rounding so the emitted figure is the counter-example's
               precision (4.97, 27.98), not a re-rounded neighbour. */
            Assert.Contains(
                "slowest_share_pct = Math.Round(r.FanoutSlowestSharePercent!.Value, 2),",
                toolSource,
                StringComparison.Ordinal);
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────────────────

    /// <summary>One collector-health row as the read would build it from a fan-out of the given per-database
    /// costs — the slowest item, the width, and the blended run duration collection_log actually stores.</summary>
    private static CollectorHealth Health(int[] perDatabaseMs) => new()
    {
        FanoutItems = perDatabaseMs.Length,
        SlowestItem = "worst",
        SlowestItemMs = perDatabaseMs.Max(),
        SlowestRunDurationMs = perDatabaseMs.Sum(),
    };

    private static string ReadViewerSource() =>
        ReadRepoFile(System.IO.Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs"));

}
