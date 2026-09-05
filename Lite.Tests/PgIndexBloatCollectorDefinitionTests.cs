/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2561: measured b-tree index bloat. The two assertions that matter are that non-btree indexes can never
/// reach <c>pgstatindex</c> (it RAISES on them, so one GIN index would take the whole collection down) and
/// that no derived bloat percentage is stored.
/// </summary>
public class PgIndexBloatCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static string Sql()
        => PgIndexBloatCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 17,
            },
            ExcludedDatabases = Array.Empty<string>(),
        }).Text;

    /// <summary>
    /// Only b-trees may reach the function. Verified against a live server: GIN, BRIN and hash each raise
    /// <c>relation "x" is not a btree index</c>, so a single one of them would fail the collection every
    /// cycle.
    /// </summary>
    [Fact]
    public void OnlyBtreeIndexes_AreCandidates()
        => Assert.Matches(new Regex(@"am\.amname\s*=\s*'btree'"), Sql());

    /// <summary>
    /// The btree filter sits behind an <c>OFFSET 0</c> fence so it is applied BEFORE the function call
    /// rather than alongside it. In testing the planner did filter first without the fence — but when the
    /// failure mode is the entire collection erroring, correctness should not depend on plan shape.
    /// </summary>
    [Fact]
    public void TheCandidateFilter_IsFencedFromTheFunctionCall()
    {
        var sql = Sql();

        Assert.Contains("OFFSET 0", sql, StringComparison.Ordinal);

        var fence = sql.IndexOf("OFFSET 0", StringComparison.Ordinal);
        var call = sql.IndexOf("pgstatindex", StringComparison.Ordinal);

        Assert.True(call > fence, "pgstatindex is called before the candidate fence, so the btree filter may not have been applied yet");
    }

    /// <summary>
    /// A LEFT join, so an index over the measurement ceiling still produces a row. A cross join would drop
    /// exactly the indexes most likely to be holding reclaimable space.
    /// </summary>
    [Fact]
    public void TheFunctionJoin_IsLeft_SoSkippedIndexesStillAppear()
        => Assert.Matches(new Regex(@"LEFT JOIN LATERAL\s+public\.pgstatindex"), Sql());

    /// <summary>
    /// Skipping is recorded, never silent. A size cap that made indexes disappear would read as "no bloat
    /// here" on precisely the biggest ones.
    /// </summary>
    [Fact]
    public void SkippedIndexes_CarryAReason()
    {
        Assert.Contains("skipped_reason", PgIndexBloatCollector.Instance.PayloadColumns.Select(c => c.Name));
        Assert.Contains("skipped_reason", Sql(), StringComparison.Ordinal);
    }

    /// <summary>
    /// The density is stored raw. A freshly built index measures near 90 — between 89.98 and 91.48 across
    /// the seven measured while designing this — so a stored "bloat percent" would bake in a false floor
    /// that also varies per index.
    /// </summary>
    [Fact]
    public void TheRawDensityIsStored_AndNoDerivedBloatPercent()
    {
        var names = PgIndexBloatCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Contains("avg_leaf_density", names);
        Assert.DoesNotContain(names, n => n.Contains("bloat_pct", StringComparison.Ordinal)
                                       || n.Contains("bloat_percent", StringComparison.Ordinal));
    }

    /// <summary>
    /// <c>pgstatindex</c> is an EXTENSION function and lives where pgstattuple was created, so it is
    /// qualified <c>public.</c> — qualifying it <c>pg_catalog.</c> does not resolve at all (verified), and
    /// leaving it unqualified would let an object earlier in <c>search_path</c> shadow it.
    /// </summary>
    [Fact]
    public void TheExtensionFunction_IsQualifiedPublic_NotPgCatalog()
    {
        var sql = Regex.Replace(Sql(), @"/\*.*?\*/", " ", RegexOptions.Singleline);

        Assert.Contains("public.pgstatindex", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_catalog.pgstatindex", sql, StringComparison.Ordinal);
    }

    /// <summary>Primaries only: a standby's index files are byte-identical by replication, so measuring
    /// both pays the full-index read twice for one answer.</summary>
    [Fact]
    public void AppliesTo_PrimariesOnly()
    {
        Assert.True(PgIndexBloatCollector.Instance.AppliesTo(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17 }));
        Assert.False(PgIndexBloatCollector.Instance.AppliesTo(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17, IsInRecovery = true }));
    }

    /// <summary>Only valid, ready indexes — an invalid one from a failed CREATE INDEX CONCURRENTLY has no
    /// meaningful bloat and is a different finding entirely.</summary>
    [Fact]
    public void InvalidIndexes_AreExcluded()
    {
        var sql = Sql();

        Assert.Contains("x.indisvalid", sql, StringComparison.Ordinal);
        Assert.Contains("x.indisready", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The CYCLE has a work budget, not just each index (#2617).
    ///
    /// <para><b>This is the assertion that would have caught a collector which never returned a row.</b>
    /// <c>pgstatindex</c> reads every page it is pointed at, and the 20 GB ceiling bounds one index while
    /// nothing bounded the statement. Measured on a live Aurora target: 1,517 indexes totalling 461 GB in a
    /// single statement, which never finished and dropped the connection mid-read — <c>rows_ever = 0</c> for
    /// the collector's entire life. The local rig had two indexes, so the question never arose there.</para>
    /// </summary>
    [Fact]
    public void TheCycleHasAWorkBudget_NotJustAPerIndexCeiling()
    {
        var sql = Sql();

        /* Ranked by size so the measured ones are where bloat is worth reclaiming. */
        Assert.Contains("row_number() OVER (ORDER BY k.index_bytes DESC", sql, StringComparison.Ordinal);

        /* And the budget gates the LATERAL, which is the thing that costs pages. Gating only the
           skipped_reason would label rows correctly while still reading every index. */
        Assert.Matches(new Regex(@"LEFT JOIN LATERAL[\s\S]*?size_rank\s*<=\s*\d+"), sql);
    }

    /// <summary>
    /// Over-budget indexes are RETURNED with a reason, never dropped. An index missing from the result
    /// reads as one that does not exist; an index present with a stated reason cannot be mistaken for
    /// healthy — the same argument that put <c>skipped_reason</c> on the size ceiling.
    /// </summary>
    [Fact]
    public void OverBudgetIndexesAreReturnedWithAReason()
    {
        var sql = Sql();

        Assert.Contains("not measured this cycle (work budget)", sql, StringComparison.Ordinal);

        /* No WHERE that would remove them from the result set. */
        Assert.DoesNotMatch(new Regex(@"WHERE[^;]*size_rank\s*<="), sql);
    }

    /// <summary>
    /// A command-timeout override, so a slow single index yields a CLASSIFIED timeout rather than the
    /// unclassified <c>Exception while reading from stream</c> that #2617 actually surfaced as.
    /// </summary>
    [Fact]
    public void ItOverridesTheCommandTimeout()
    {
        Assert.NotNull(PgIndexBloatCollector.Instance.CommandTimeoutSecondsOverride);
        Assert.True(PgIndexBloatCollector.Instance.CommandTimeoutSecondsOverride >= 120);
    }

    /// <summary>
    /// The cycle budget bounds BYTES, not just index count (#2997).
    ///
    /// <para><b>This is the assertion that would have caught eleven consecutive failures.</b> The count
    /// budget from #2617 and the 300-second override from #2618 were both in place and both pinned, and
    /// the collector still died every single run with <c>rows = 0</c> — because 200 sub-ceiling indexes
    /// on a real Aurora target admit 286 GB, and <c>pgstatindex</c> reads every page of every one of
    /// them. A count bounds pages only where count correlates with bytes, and the one target that had
    /// the extension installed was the counterexample.</para>
    ///
    /// <para>The gate has to sit on the LATERAL. Labelling rows over the budget while still passing
    /// every one of them to the function is precisely the shape that shipped: correct
    /// <c>skipped_reason</c> text on a statement that reads the whole instance anyway.</para>
    /// </summary>
    [Fact]
    public void TheCycleBudget_BoundsBytes_NotJustIndexCount()
    {
        var sql = Sql();

        Assert.Contains("measured_bytes_through_here", sql, StringComparison.Ordinal);
        Assert.Matches(
            new Regex(@"LEFT JOIN LATERAL[\s\S]*?measured_bytes_through_here\s*<=\s*\d+"), sql);
    }

    /// <summary>
    /// Only the indexes that will actually be READ may spend the budget.
    ///
    /// <para>An over-ceiling index is never handed to <c>pgstatindex</c>, so it costs no pages. Charging
    /// it to the byte budget anyway would spend the allowance on work nobody does — and not marginally:
    /// on the target this bounds, the three over-ceiling indexes total 71 GB against a 20 GB budget, so
    /// the unfiltered running total would exhaust the budget before the first measurable index and the
    /// collector would return zero measurements for a second, entirely new reason.</para>
    /// </summary>
    [Fact]
    public void TheByteBudget_ChargesOnlyTheIndexesItWillActuallyRead()
        => Assert.Matches(
            new Regex(@"sum\(k\.index_bytes\)\s*FILTER\s*\(WHERE\s+k\.index_bytes\s*<\s*\d+\)"), Sql());

    /// <summary>
    /// The running total is a running total — ordered largest-first and framed from the start of the
    /// partition. An unframed window sum would return the WHOLE total on every row, so no row would ever
    /// be under budget and nothing would be measured; a differently-ordered one would spend the budget
    /// on small indexes and pass over the big ones, inverting the argument the ORDER BY exists to make.
    /// </summary>
    [Fact]
    public void TheByteBudget_AccumulatesLargestFirst()
        => Assert.Matches(
            new Regex(@"OVER \(ORDER BY k\.index_bytes DESC, k\.index_name\s+"
                      + @"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\)"), Sql());

    /// <summary>
    /// Over-budget-by-bytes indexes are RETURNED with a reason, on the same argument as the count budget
    /// and the size ceiling: an index missing from the result reads as one that does not exist.
    /// </summary>
    [Fact]
    public void OverByteBudgetIndexesAreReturnedWithAReason()
        => Assert.Matches(
            new Regex(@"WHEN k\.measured_bytes_through_here\s*>\s*\d+[\s\S]*?"
                      + @"not measured this cycle \(work budget\)"), Sql());

    /// <summary>
    /// The SQL literals agree with the constants a reader will find in C#. Two representations of one
    /// number is the existing shape here (<c>CeilingLiteral</c> beside
    /// <see cref="PgIndexBloatCollector.MeasureCeilingBytes"/>), and it had no pin — so this reads the
    /// literal out of the gate it actually governs rather than merely looking for the digits somewhere
    /// in the query, which a stale second copy would also satisfy.
    /// </summary>
    [Fact]
    public void TheBudgetLiterals_AgreeWithTheirConstants()
    {
        var sql = Sql();

        var byteGate = Regex.Match(sql, @"measured_bytes_through_here\s*<=\s*(\d+)");
        Assert.True(byteGate.Success, "the cycle byte budget no longer gates the LATERAL");
        Assert.Equal(
            PgIndexBloatCollector.CycleMeasureBudgetBytes,
            long.Parse(byteGate.Groups[1].Value, CultureInfo.InvariantCulture));

        var ceilingFilter = Regex.Match(sql, @"FILTER \(WHERE k\.index_bytes < (\d+)\)");
        Assert.True(ceilingFilter.Success, "the byte budget no longer filters on the per-index ceiling");
        Assert.Equal(
            PgIndexBloatCollector.MeasureCeilingBytes,
            long.Parse(ceilingFilter.Groups[1].Value, CultureInfo.InvariantCulture));
    }
}
