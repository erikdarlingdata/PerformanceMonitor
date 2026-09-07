/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
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

    private static CollectorContext Context(
        IReadOnlyDictionary<string, string>? state = null, string? currentDatabase = null)
        => new()
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
            State = state ?? CollectorContext.NoState,
            CurrentDatabaseName = currentDatabase,
        };

    private static CollectorQuery Plan(IReadOnlyDictionary<string, string>? state = null)
        => PgIndexBloatCollector.Instance.BuildQuery(Context(state));

    private static string Sql() => Plan().Text;

    /// <summary>One stored cursor, in the shape the collector persists it in.</summary>
    private static Dictionary<string, string> Cursor(string database, long indexBytes, long indexOid)
        => new(StringComparer.Ordinal)
        {
            [PgIndexBloatCollector.RotationCursorKeyPrefix + database] =
                PgIndexBloatCollector.FormatCursor(indexBytes, indexOid),
        };

    /// <summary>
    /// One output row in the collector's own projection order, for driving <c>ReadAsync</c>. Only the
    /// columns the rotation cursor is derived from carry real values; the measurement columns are the
    /// nulls a skipped row really has.
    /// </summary>
    private static object[] OutputRow(string database, long indexBytes, long indexOid, string? skippedReason)
        => new object[]
        {
            database, "public", "t", "ix_" + indexOid.ToString(CultureInfo.InvariantCulture),
            indexBytes,
            DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
            skippedReason is null ? DBNull.Value : skippedReason,
            indexOid,
        };

    /// <summary>
    /// The body of the <c>in_budget</c> CTE — the one relation the work bounds are allowed to filter, and
    /// the relation <c>pgstatindex</c> is applied to. Read out of the shipped query rather than looked for
    /// anywhere in it, so a bound that drifted back out into a join qual fails rather than still matching.
    /// </summary>
    private static string InBudgetBody(string sql)
    {
        var body = Regex.Match(sql, @"in_budget AS \((?<body>[\s\S]*?)\n\),");

        Assert.True(body.Success, "the query no longer has an in_budget relation for the work bounds to filter");

        return body.Groups["body"].Value;
    }

    /// <summary>
    /// The body of the <c>candidates</c> CTE — the CENSUS, which no bound may filter. Extracted the same
    /// way as <see cref="InBudgetBody"/>, and for the same reason: a search of the whole query for the
    /// absence of a predicate finds it in whichever CTE legitimately carries it.
    /// </summary>
    private static string CandidatesBody(string sql)
    {
        var body = Regex.Match(sql, @"WITH candidates AS \((?<body>[\s\S]*?)\n\),");

        Assert.True(body.Success, "the query no longer has a candidates relation to read the census out of");

        return body.Groups["body"].Value;
    }

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
    /// <c>pgstatindex</c> is applied to the GATED relation, and reached by a CROSS join to it.
    ///
    /// <para><b>The distinction this pin exists for.</b> A gate written as a qual on a
    /// <c>LEFT JOIN LATERAL</c> to the function reads like a gate and bounds nothing. The planner has no
    /// way to skip an inner side it has not evaluated, so the function is invoked once per candidate row
    /// and the qual only decides whether its answer is kept. Measured on PostgreSQL 17.11 with the
    /// shipped query text against real b-tree indexes and five candidates of which one was in budget: the
    /// ON-clause form plans as <c>Nested Loop Left Join</c> with <c>Rows Removed by Join Filter: 4</c>
    /// over a <c>Function Scan on pgstatindex ... loops=5</c>. Feeding the function a filtered relation
    /// instead gives <c>loops=1</c> for byte-identical output.</para>
    ///
    /// <para>So the property is not "which join keyword" but "what relation the function is applied to",
    /// and a cross join to a relation that already contains only rows this statement intends to read
    /// drops nothing — the skipped ones rejoin through
    /// <see cref="EverySkippedIndexStillReturnsARow_ThroughTheOuterJoin"/>'s outer join.</para>
    /// </summary>
    [Fact]
    public void TheFunctionCall_IsAppliedToTheGatedRelation()
    {
        var sql = Sql();

        Assert.Matches(
            new Regex(@"FROM in_budget AS b\s+CROSS JOIN LATERAL\s+public\.pgstatindex"), sql);
    }

    /// <summary>
    /// No work bound may sit on the nullable side of an outer join. This is the CATEGORY that produced
    /// both #2617's count budget and #2997's byte budget as labels rather than bounds: each was placed as
    /// a further qual on the same <c>LEFT JOIN LATERAL</c>, each was pinned, and the collector went on
    /// reading every index on the instance.
    ///
    /// <para>Stated as an absence rather than as a shape, because the failure has already arrived twice by
    /// two different expressions and the next one will not look like either. As long as the function is
    /// never on the nullable side of an outer join, no qual on such a join can be paying for work — which
    /// is what makes this checkable without a planner.</para>
    /// </summary>
    [Fact]
    public void NoWorkBoundSitsOnTheNullableSideOfAnOuterJoin()
    {
        var sql = Sql();

        Assert.DoesNotContain("LEFT JOIN LATERAL", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("RIGHT JOIN LATERAL", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("FULL JOIN LATERAL", sql, StringComparison.Ordinal);

        /* The one outer join left joins an already-computed relation, so its ON clause cannot buy pages.
           It must therefore carry the identity of the join and nothing that looks like a budget. */
        var outerOn = Regex.Match(sql, @"LEFT JOIN measured AS m\s*\n\s*ON ([^\n]+)");
        Assert.True(outerOn.Success, "the measurements are no longer joined back by a plain LEFT JOIN");
        Assert.Equal("m.index_oid = k.index_oid", outerOn.Groups[1].Value.Trim());
    }

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
    /// <c>pgstatindex</c> reads every page it is pointed at, and the per-index ceiling bounds one index
    /// while nothing bounded the statement. Measured on a live Aurora target: 1,517 indexes totalling
    /// 461 GB in a single statement, which never finished and dropped the connection mid-read —
    /// <c>rows_ever = 0</c> for the collector's entire life. The local rig had two indexes, so the question
    /// never arose there.</para>
    /// </summary>
    [Fact]
    public void TheCycleHasAWorkBudget_NotJustAPerIndexCeiling()
    {
        var sql = Sql();

        /* Ranked by size so the measured ones are where bloat is worth reclaiming. */
        Assert.Contains("OVER (ORDER BY k.index_bytes DESC", sql, StringComparison.Ordinal);

        /* And the count bound selects the relation handed to the function, which is the thing that costs
           pages. Bounding only the skipped_reason would label rows correctly while still reading every
           index. */
        Assert.Matches(new Regex(@"measure_rank\s*<=\s*\d+"), InBudgetBody(sql));

        /* The rank counts only the rows this cycle can MEASURE — a rank over the whole census would count
           the rows the rotation cursor has passed and the over-ceiling ones the function never sees, so
           the count bound would stop admitting anything once the cursor moved past its Nth row (#3153).
           row_number() takes no FILTER, which is why this is a filtered count. */
        Assert.Matches(
            new Regex(@"count\(\*\) FILTER \(WHERE k\.index_bytes < \d+ AND k\.in_rotation_window\)\s+"
                      + @"OVER \(ORDER BY k\.index_bytes DESC, k\.index_oid"),
            sql);
    }

    /// <summary>
    /// The gate relation is fenced with <c>OFFSET 0</c>, on the same argument as the candidate set: when
    /// the failure mode is that the collector returns nothing at all, the bound should not rest on the
    /// planner choosing to push a filter down through a CTE.
    /// </summary>
    [Fact]
    public void TheGateRelation_IsFencedLikeTheCandidateSet()
        => Assert.Contains("OFFSET 0", InBudgetBody(Sql()), StringComparison.Ordinal);

    /// <summary>
    /// Every candidate still returns exactly one row, even though the bounds are now a <c>WHERE</c>.
    ///
    /// <para>The bounds moved into a relation of their own precisely so they could be a <c>WHERE</c> — but
    /// a <c>WHERE</c> that reached the OUTPUT would drop the skipped indexes, and an index missing from
    /// the result reads as one that does not exist rather than one that was not measured. So the final
    /// <c>FROM</c> has to be the UNGATED relation, with the measurements joined back onto it.</para>
    /// </summary>
    [Fact]
    public void EverySkippedIndexStillReturnsARow_ThroughTheOuterJoin()
    {
        var sql = Sql();

        /* The result is driven by ranked, which no bound has filtered. Only the cursor's own coordinate
           may sit between the reason and that FROM, which the bounded span is what enforces. */
        Assert.Matches(
            new Regex(@"END::text\s+AS skipped_reason,[\s\S]{0,400}?\r?\nFROM ranked AS k"), sql);

        /* And the bounds live only in the gate relation, never in the final SELECT's own filters. */
        var gateEnd = sql.IndexOf("measured AS (", StringComparison.Ordinal);

        Assert.True(gateEnd >= 0, "the measurements are no longer computed in a relation of their own");

        var afterGate = sql[gateEnd..];
        Assert.DoesNotMatch(new Regex(@"WHERE[\s\S]*?measure_rank\s*<="), afterGate);
        Assert.DoesNotMatch(new Regex(@"WHERE[\s\S]*?measured_bytes_through_here\s*<="), afterGate);

        /* Same rule for the rotation window, and it is the one this collector is most likely to get wrong:
           filtering the CENSUS by the cursor would make an index the cursor has already passed
           indistinguishable from one that does not exist, which is the exact confusion skipped_reason
           exists to prevent (#3153). */
        Assert.DoesNotMatch(new Regex(@"WHERE[\s\S]*?in_rotation_window"), afterGate);
        Assert.DoesNotContain("in_rotation_window", CandidatesBody(sql), StringComparison.Ordinal);
        Assert.DoesNotContain("cursor_bytes", CandidatesBody(sql), StringComparison.Ordinal);
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

        /* The bound is a WHERE, which is what makes it a bound — so what stops it removing rows from the
           RESULT is that it filters a relation of its own.
           EverySkippedIndexStillReturnsARow_ThroughTheOuterJoin is the pin on that. */
        Assert.Matches(new Regex(@"in_budget AS \([\s\S]*?WHERE[\s\S]*?measure_rank\s*<="), sql);
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
    /// the collector still died every single run with <c>rows = 0</c> — because on a real Aurora target,
    /// at a 20 GB per-index ceiling, the 200 largest sub-ceiling indexes admitted 286 GB, and
    /// <c>pgstatindex</c> reads every page of every one of them. A count bounds pages only where count
    /// correlates with bytes, and the one target that had the extension installed was the counterexample.
    /// The figure is quoted with the ceiling it was measured at because both are inputs to it: which
    /// indexes count as sub-ceiling is what the ceiling decides.</para>
    ///
    /// <para>The bound has to select what the function is applied TO. Labelling rows over the budget
    /// while still passing every one of them to the function is precisely the shape that shipped: correct
    /// <c>skipped_reason</c> text on a statement that reads the whole instance anyway — see
    /// <see cref="TheFunctionCall_IsAppliedToTheGatedRelation"/> for the measurement.</para>
    /// </summary>
    [Fact]
    public void TheCycleBudget_BoundsBytes_NotJustIndexCount()
    {
        var sql = Sql();

        Assert.Contains("measured_bytes_through_here", sql, StringComparison.Ordinal);
        Assert.Matches(new Regex(@"measured_bytes_through_here\s*<=\s*\d+"), InBudgetBody(sql));
    }

    /// <summary>
    /// Only the indexes that will actually be READ may spend the budget.
    ///
    /// <para>An over-ceiling index is never handed to <c>pgstatindex</c>, so it costs no pages. Charging
    /// it to the byte budget anyway would spend the allowance on work nobody does — and not marginally.
    /// The over-ceiling indexes sort FIRST under size DESC and can exceed the whole budget between them,
    /// so the unfiltered running total would be over budget before the first measurable index and the
    /// collector would return zero measurements for a second, entirely new reason. Measured on a real
    /// Aurora target at a 20 GB ceiling: three over-ceiling indexes totalling 71 GB, against a budget of
    /// 20 GB.</para>
    /// </summary>
    [Fact]
    public void TheByteBudget_ChargesOnlyTheIndexesItWillActuallyRead()
        => Assert.Matches(
            new Regex(@"sum\(k\.index_bytes\)\s*FILTER\s*\(WHERE\s+k\.index_bytes\s*<\s*\d+"
                      + @"\s+AND\s+k\.in_rotation_window\)"), Sql());

    /// <summary>
    /// The running total is a running total — ordered largest-first and framed by ROWS from the start of
    /// the partition.
    ///
    /// <para><b>ROWS, not the default frame.</b> A window with an ORDER BY and no explicit frame gets
    /// <c>RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW</c>, which is also a running total but groups
    /// PEERS: every row tying on the whole ORDER BY sees the tie's ENTIRE total, the first member
    /// included. A tied pair is then all-or-nothing against the budget — measured with a spare index'
    /// worth of headroom, or skipped together even though one of them would have fitted. ROWS charges
    /// each row for itself, which is what spending a budget largest-first means.
    ///
    /// Ties are reachable, not theoretical: <c>index_name</c> is the tiebreaker and is unique per SCHEMA
    /// rather than per database, so two schemas can hold equally-sized indexes of the same name. Measured
    /// on PostgreSQL 17.11 with exactly that pair at 1,138,688 bytes each — ROWS gives 1,138,688 then
    /// 2,277,376, while the default RANGE frame gives 2,277,376 on BOTH rows.</para>
    ///
    /// <para>Dropping the ORDER BY instead would make the sum the whole total on every row, so no row
    /// would ever be under budget and nothing would be measured; reversing it would spend the budget on
    /// small indexes and pass over the big ones, inverting the argument the ordering exists to make.</para>
    ///
    /// <para><b>The tiebreaker is <c>index_oid</c> since #3153, and that is not cosmetic.</b> The rotation
    /// cursor is a coordinate in exactly this order, so the order has to be TOTAL or the cursor is not a
    /// resume point: parked on a non-unique coordinate it excludes a tied sibling it never measured while
    /// labelling it as already passed. <c>index_name</c> cannot serve — <c>pg_class</c> is unique on
    /// (relname, relnamespace), so two schemas can hold equally-sized indexes of the same name, which is
    /// the pair measured above. One honest consequence: with a unique tiebreaker the frame's peer group is
    /// a singleton, so ROWS and RANGE now agree here. ROWS stays because it states what spending a budget
    /// largest-first means, and because it is what keeps this right if the tiebreaker is ever dropped.</para>
    /// </summary>
    [Fact]
    public void TheByteBudget_AccumulatesLargestFirst()
        => Assert.Matches(
            new Regex(@"OVER \(ORDER BY k\.index_bytes DESC, k\.index_oid\s+"
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

        var ceilingFilter = Regex.Match(
            sql, @"sum\(k\.index_bytes\) FILTER \(WHERE k\.index_bytes < (\d+) AND k\.in_rotation_window\)");
        Assert.True(ceilingFilter.Success, "the byte budget no longer filters on the per-index ceiling");
        Assert.Equal(
            PgIndexBloatCollector.MeasureCeilingBytes,
            long.Parse(ceilingFilter.Groups[1].Value, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// The cycle byte budget may never sit BELOW the per-index ceiling.
    ///
    /// <para>The two bounds are independent in what they measure — one index versus the whole statement —
    /// and their doc comments each argue their number on its own terms, which makes it look as though any
    /// pair of values would do. It would not. A cycle budget under the ceiling opens a band between the
    /// two in which an index can never be measured at all: it is under the ceiling, so it is a legitimate
    /// candidate and earns no "too large" reason, yet it alone exceeds the entire cycle, so it is over
    /// budget on its own first row on every run forever. It would carry
    /// <c>not measured this cycle (work budget)</c> in perpetuity — a reason whose whole claim is that the
    /// index is DEFERRED, attached to one that is permanently skipped. That is the exact confusion between
    /// "unmeasured" and "healthy" that <c>skipped_reason</c> exists to prevent, reintroduced one level up.</para>
    ///
    /// <para>Pinned as an inequality rather than as equality: raising the cycle budget above the ceiling is
    /// a legitimate tuning move once a SUCCESS row supplies a real duration, and this must not stand in the
    /// way of it. Only the floor is load-bearing.</para>
    /// </summary>
    [Fact]
    public void TheCycleBudget_IsNeverBelowThePerIndexCeiling()
        => Assert.True(
            PgIndexBloatCollector.CycleMeasureBudgetBytes >= PgIndexBloatCollector.MeasureCeilingBytes,
            $"the cycle budget ({PgIndexBloatCollector.CycleMeasureBudgetBytes}) is below the per-index "
            + $"ceiling ({PgIndexBloatCollector.MeasureCeilingBytes}), so any index between the two can "
            + "never be measured while still being reported as merely deferred");

    /// <summary>
    /// The budget, the deadline and the assumed block rate have to agree, and this is the assertion that
    /// makes any one of the three answerable to the other two.
    ///
    /// <para><b>Why the three were never related before.</b> Each had its own justification and no pin
    /// compared them, so a budget could be — and was — chosen from an estimate of bulk throughput while
    /// the deadline it had to fit inside was decided separately. <c>pgstatindex</c> walks the index one
    /// block at a time with no prefetch, so its cost is a count of potentially-synchronous single-block
    /// reads; on network-attached storage a sequential-throughput figure overstates the achievable rate by
    /// orders of magnitude, and that is the arithmetic error that a per-byte argument cannot see.</para>
    ///
    /// <para><b>Half the deadline, not all of it.</b> The remainder pays for the catalog scan, connection
    /// setup, and the tail index admitted while the running total was still just under budget — that index
    /// is charged for itself, so the last admission can be almost a whole index past the point where the
    /// budget was nearly spent.</para>
    ///
    /// <para>The rate is an assumption and is named as one. This pin does not make it true; it makes
    /// raising the budget state a rate, and makes raising the rate state why.</para>
    /// </summary>
    [Fact]
    public void TheCycleBudget_FitsTheDeadline_AtThePessimisticBlockRate()
    {
        var deadlineSeconds = PgIndexBloatCollector.Instance.CommandTimeoutSecondsOverride;

        Assert.NotNull(deadlineSeconds);

        var blocks = PgIndexBloatCollector.CycleMeasureBudgetBytes / PgIndexBloatCollector.BlockSizeBytes;
        var seconds = blocks / (double)PgIndexBloatCollector.PessimisticBlocksPerSecond;
        var allowed = deadlineSeconds.Value / 2.0;

        Assert.True(
            seconds <= allowed,
            $"a full cycle budget of {PgIndexBloatCollector.CycleMeasureBudgetBytes} bytes is {blocks} "
            + $"blocks, which at {PgIndexBloatCollector.PessimisticBlocksPerSecond} blocks/s takes "
            + $"{seconds:F0}s — past the {allowed:F0}s that leaves half of the {deadlineSeconds}s command "
            + "deadline for everything else. Lower the budget, or argue the rate up and say on what "
            + "measurement");
    }

    /* ---------------- rotation (#3153) ---------------- */

    /// <summary>
    /// The statement this collector issues is a function of its STORED STATE, not only of the target
    /// catalog. That single property is what separates a collector that rotates from one that re-measures
    /// its largest index forever.
    ///
    /// <para><b>Why this is the load-bearing pin and not a numeric one.</b> Before #3153 the collector was
    /// <c>BuildQuery(CollectorContext context) =&gt; new(QueryText)</c> with <c>context</c> unreferenced and
    /// no declared <c>StateKeys</c>, so no state query ran at all. The ordering was
    /// <c>index_bytes DESC</c> over the live catalog, and measuring an index does not change its size — so
    /// the selection could not rotate, and the 2,456 rows stamped
    /// <c>not measured this cycle (work budget)</c> asserted a DEFERRAL that no mechanism could honour.
    /// No geometry fixes that: it is a claim about state flow, not about a number, which is why this
    /// asserts on the state flow.</para>
    ///
    /// <para>Asserted over the whole <see cref="CollectorQuery"/> rather than over its text, because the
    /// cursor is BOUND: the shape changes when a cursor appears at all, and the VALUES change with the
    /// cursor. Checking text alone would pass while every cycle bound the same coordinate.</para>
    /// </summary>
    [Fact]
    public void TheStatement_CarriesTheStoredCursor_SoTheMeasuredSetRotates()
    {
        var first = Plan();
        var second = Plan(Cursor("appdb", 2_099_085_312, 16_384));
        var third = Plan(Cursor("appdb", 57_344, 91_022));

        /* A cursor changes the statement's SHAPE — no cursor binds nothing at all. */
        Assert.Empty(first.Parameters);
        Assert.NotEqual(first.Text, second.Text);

        /* And two different cursors bind different coordinates. */
        Assert.Equal(second.Text, third.Text);
        Assert.NotEqual(
            string.Join("|", second.Parameters.Select(p => $"{p.Name}={p.Value}")),
            string.Join("|", third.Parameters.Select(p => $"{p.Name}={p.Value}")));

        /* The stored coordinate really reaches the statement, in both of its components. */
        Assert.Contains(second.Parameters, p => Equals(p.Value, 2_099_085_312L));
        Assert.Contains(second.Parameters, p => Equals(p.Value, 16_384L));
        Assert.Contains(second.Parameters, p => Equals(p.Value, "appdb"));

        /* Names are BOUND, never interpolated: a database name is a catalog identifier and may contain
           anything a quoted identifier may contain. */
        Assert.DoesNotContain("appdb", second.Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// The cursor is scoped per DATABASE and selected inside the statement by
    /// <c>current_database()</c>, because the host builds this collector's query ONCE for the whole
    /// per-database sweep — it declares no watermark, so there is no per-database rebuild to hang a
    /// scalar cursor on.
    ///
    /// <para>A cursor shared across databases would advance one database past indexes another database
    /// never measured, which is the false "already passed" claim this change exists to remove,
    /// reintroduced one level up.</para>
    /// </summary>
    [Fact]
    public void TheCursorIsPerDatabase_AndTheStatementPicksItsOwn()
    {
        var state = Cursor("alpha", 100, 1);
        state[PgIndexBloatCollector.RotationCursorKeyPrefix + "zeta"] =
            PgIndexBloatCollector.FormatCursor(200, 2);

        var plan = Plan(state);

        Assert.Contains("current_database()", plan.Text, StringComparison.Ordinal);
        Assert.Matches(new Regex(@"WHERE c\.database_name = pg_catalog\.current_database\(\)"), plan.Text);

        /* Both databases' cursors travel, ordered by database name. */
        Assert.Equal(6, plan.Parameters.Count);
        Assert.Equal("alpha", plan.Parameters[0].Value);
        Assert.Equal("zeta", plan.Parameters[3].Value);

        /* And identical state binds identical VALUES whatever order the dictionary iterates, so one cycle's
           statement is reproducible from its state alone.

           Asserted over the bound values and NOT over the text, which is the correction that makes this
           pin able to fail at all: the cursors are parameters, so two cursors always emit the same
           @rot_db_0/@rot_db_1 placeholders and a text comparison passes however the values were ordered.
           The first version of this assertion compared texts and was therefore vacuous. */
        var reversed = Plan(new Dictionary<string, string>(
            state.Reverse().ToDictionary(kv => kv.Key, kv => kv.Value), StringComparer.Ordinal));

        Assert.Equal(
            plan.Parameters.Select(p => $"{p.Name}={p.Value}").ToArray(),
            reversed.Parameters.Select(p => $"{p.Name}={p.Value}").ToArray());
    }

    /// <summary>
    /// A completed pass, an absent cursor, and a value this build cannot read all take the SAME path:
    /// start at the largest index. Absent is what a first run, a restarted host and a broken store all
    /// look like, so absent must never mean "skip" — the rule
    /// <see cref="CollectorContext.State"/> states for every definition that carries state.
    /// </summary>
    [Fact]
    public void AnUnusableCursor_StartsThePassAtTheLargestIndex()
    {
        var wrapped = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [PgIndexBloatCollector.RotationCursorKeyPrefix + "appdb"] = "wrap",
            [PgIndexBloatCollector.RotationCursorKeyPrefix + "other"] = "not-a-cursor",
            [PgIndexBloatCollector.RotationCursorKeyPrefix] = "1|2",
            ["some_unrelated_key"] = "3|4",
        };

        var plan = Plan(wrapped);

        Assert.Empty(plan.Parameters);
        Assert.Equal(Plan().Text, plan.Text);

        /* And the marker really is the one the collector writes, not a string this test invented. */
        Assert.Equal("wrap", PgIndexBloatCollector.RotationPassCompleteMarker);
        Assert.False(PgIndexBloatCollector.TryParseCursor(
            PgIndexBloatCollector.RotationPassCompleteMarker, out _, out _));
    }

    /// <summary>
    /// A stored cursor round-trips, and everything that is not a canonical cursor is REJECTED rather than
    /// half-read. A partially-parsed coordinate would resume somewhere nobody chose.
    /// </summary>
    [Fact]
    public void TheCursorFormat_RoundTripsAndRejectsEverythingElse()
    {
        Assert.True(PgIndexBloatCollector.TryParseCursor(
            PgIndexBloatCollector.FormatCursor(2_099_085_312, 16_384), out var bytes, out var oid));
        Assert.Equal(2_099_085_312, bytes);
        Assert.Equal(16_384, oid);

        foreach (var bad in new[] { null, "", "wrap", "|5", "5|", "abc|1", "1|abc", "-1|2", "1|-2", "12345" })
        {
            Assert.False(
                PgIndexBloatCollector.TryParseCursor(bad, out var badBytes, out var badOid),
                $"'{bad}' must not parse as a cursor");
            Assert.Equal(0, badBytes);
            Assert.Equal(0, badOid);
        }
    }

    /// <summary>
    /// The cursor for the next cycle is the LAST index this one measured, in the collector's own
    /// measurement order — smallest bytes, and among ties the largest oid.
    ///
    /// <para><b>Scanned, not taken from the last row.</b> The output is ordered by size alone, with no
    /// tiebreaker, and a SKIPPED row can legitimately sort after a measured one — so "the last row" and
    /// "the last measured position" are different rows, and only one of them is a valid resume point.</para>
    /// </summary>
    [Fact]
    public async System.Threading.Tasks.Task TheCursorAdvances_ToTheLastPositionItMeasured()
    {
        var context = Context(currentDatabase: "appdb");

        using var reader = new FakeCollectorDataReader(
            OutputRow("appdb", 5000, 10, null),
            OutputRow("appdb", 3000, 12, null),
            OutputRow("appdb", 3000, 99, null),
            OutputRow("appdb", 2000, 13, "not measured this cycle (work budget): ..."));

        var rows = await PgIndexBloatCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(4, rows.Count);
        Assert.Equal(
            PgIndexBloatCollector.FormatCursor(3000, 99),
            Assert.Contains(PgIndexBloatCollector.RotationCursorKeyPrefix + "appdb", context.PendingState));
    }

    /// <summary>
    /// A cycle that measured NOTHING ends the pass, because — given
    /// <c>CycleMeasureBudgetBytes &gt;= MeasureCeilingBytes</c> — an empty measured set can only mean no
    /// sub-ceiling candidate remained below the cursor.
    ///
    /// <para>Written as a marker rather than a deletion because <see cref="CollectorContext.PendingState"/>
    /// only ever upserts: leaving the old value would park the cursor at the end of the pass forever, and
    /// the collector would go back to measuring nothing — the #3153 failure with extra steps.</para>
    /// </summary>
    [Fact]
    public async System.Threading.Tasks.Task ACycleThatMeasuredNothing_EndsThePass()
    {
        var context = Context(currentDatabase: "appdb");

        using var reader = new FakeCollectorDataReader(
            OutputRow("appdb", 5000, 10, "above the rotation cursor: ..."),
            OutputRow("appdb", 4000, 11, "above the rotation cursor: ..."));

        await PgIndexBloatCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(
            PgIndexBloatCollector.RotationPassCompleteMarker,
            Assert.Contains(PgIndexBloatCollector.RotationCursorKeyPrefix + "appdb", context.PendingState));
    }

    /// <summary>
    /// A database with no b-tree indexes at all still records where it is, from the name the host is
    /// iterating rather than from a row it never got. Without the fallback such a database would keep
    /// whatever cursor it last had, forever, and a database whose indexes were all dropped would never
    /// return to the top of a pass.
    /// </summary>
    [Fact]
    public async System.Threading.Tasks.Task ADatabaseWithNoCandidates_StillRecordsThePassAsComplete()
    {
        var context = Context(currentDatabase: "emptydb");

        using var reader = new FakeCollectorDataReader();

        var rows = await PgIndexBloatCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(rows);
        Assert.Equal(
            PgIndexBloatCollector.RotationPassCompleteMarker,
            Assert.Contains(PgIndexBloatCollector.RotationCursorKeyPrefix + "emptydb", context.PendingState));
    }

    /// <summary>
    /// Every conjunct of the gate has a <c>skipped_reason</c> arm, and this is checked as a PAIRING rather
    /// than as four present strings.
    ///
    /// <para><b>The failure it catches.</b> The rotation window was added to the gate first. Without a
    /// matching CASE arm, every index the cursor had already passed came back with all its measurements
    /// NULL and NO reason — the one output shape this collector must never produce, because a blank
    /// measurement with no reason is indistinguishable from a measured emptiness (an empty index really
    /// does report a null density). Four bounds, four arms, and a bound that grows a fifth has to grow a
    /// fifth arm with it.</para>
    /// </summary>
    [Fact]
    public void EveryGateConjunct_HasItsOwnReasonArm()
    {
        var sql = Sql();
        var gate = InBudgetBody(sql);
        var reasons = Regex.Match(sql, @"CASE(?<arms>[\s\S]*?)\r?\n    END::text");

        Assert.True(reasons.Success, "the skipped_reason CASE is no longer where this pin can read it");

        var arms = reasons.Groups["arms"].Value;

        /* The four gate conjuncts, each paired with the arm that explains its exclusion. */
        foreach (var (conjunct, arm) in new[]
                 {
                     ("k.index_bytes < ", "k.index_bytes >= "),
                     ("k.in_rotation_window", "NOT k.in_rotation_window"),
                     ("k.measured_bytes_through_here <= ", "k.measured_bytes_through_here > "),
                     ("k.measure_rank <= ", "k.measure_rank > "),
                 })
        {
            Assert.Contains(conjunct, gate, StringComparison.Ordinal);
            Assert.Contains(arm, arms, StringComparison.Ordinal);
        }

        /* And nothing else gates: a fifth conjunct with no arm is the defect above. */
        Assert.Equal(4, Regex.Matches(gate, @"\r?\n    (?:WHERE|AND) ").Count);
        Assert.Equal(4, Regex.Matches(arms, @"\r?\n        WHEN ").Count);
    }

    /// <summary>
    /// Exactly ONE reason claims a permanent outcome, and it is the ceiling's — the other three promise a
    /// later cycle, which rotation is what makes true.
    ///
    /// <para>This is the wording half of #3153, and #3158 argued it is the higher priority of the two:
    /// 983 sub-64 kB indexes on the first production target appear in NO other collector, so for those
    /// objects the row carrying this text is the product's only record of them. A deferral claim on a row
    /// nothing will ever return to is the whole defect.</para>
    /// </summary>
    [Fact]
    public void OnlyTheCeilingReasonClaimsPermanence_AndTheOthersPromiseALaterCycle()
    {
        var sql = Sql();

        /* The ceiling arm says NEVER, in as many words, and points at what does track those indexes —
           43 of them held 68% of the first production target's index bytes, and pg_index_usage_stats
           covers all 43 with a size trend. */
        Assert.Matches(
            new Regex(@"index_bytes >= \d+[\s\S]{0,400}?NEVER[\s\S]{0,200}?not a deferral"), sql);
        Assert.Matches(
            new Regex(@"index_bytes >= \d+[\s\S]{0,600}?pg_index_usage_stats"), sql);

        /* And it is the only arm that does. */
        Assert.Single(Regex.Matches(sql, @"NEVER"));

        /* Each deferral arm states the mechanism that honours it. Three arms, three promises. */
        Assert.Equal(
            3,
            Regex.Matches(sql, @"(?:comes back to it when the pass wraps"
                               + @"|a later run in this pass reaches this)").Count);

        /* The pre-#3153 wording promised a deferral with no mechanism behind it. Whatever the arms say
           now, none of them may claim a run merely "stops" once it has measured its budget without
           saying what happens next. */
        Assert.DoesNotContain("This one is recorded at its size so it is never", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The declared state key is the cursor's PREFIX, and the concrete keys are per database — which is
    /// only sound because both hosts load state by collector NAME rather than by declared key.
    /// <c>CollectorStateContractTests</c> pins that half in both hosts; this pins the declaration.
    /// </summary>
    [Fact]
    public void StateKeys_DeclaresTheRotationCursorPrefix()
    {
        Assert.Equal(
            new[] { PgIndexBloatCollector.RotationCursorKeyPrefix },
            PgIndexBloatCollector.Instance.StateKeys.ToArray());

        Assert.Equal("rotate:", PgIndexBloatCollector.RotationCursorKeyPrefix);

        /* Declaring nothing means no state query runs, which is exactly the pre-#3153 behaviour: the
           cursor would never load and the collector would re-measure its largest index forever. */
        Assert.NotEmpty(PgIndexBloatCollector.Instance.StateKeys);
    }

    /// <summary>
    /// The cursor's coordinate is projected but NOT stored. Adding <c>index_oid</c> to the payload would
    /// be a store rung for a value no read wants, and dropping it from the projection would leave the
    /// cursor with no unique coordinate.
    /// </summary>
    [Fact]
    public void TheCursorCoordinateIsProjected_ButNotAPayloadColumn()
    {
        Assert.DoesNotContain("index_oid", PgIndexBloatCollector.Instance.PayloadColumns.Select(c => c.Name));

        /* Projected LAST, so every stored column keeps the ordinal ReadAsync reads it at. */
        Assert.Matches(
            new Regex(@"AS skipped_reason,[\s\S]*?k\.index_oid::bigint\s+AS index_oid\r?\nFROM ranked"),
            Sql());

        var writer = new RecordingCollectorRowWriter();
        PgIndexBloatCollector.Instance.WritePayload(default, writer, Context());

        Assert.Equal(PgIndexBloatCollector.Instance.PayloadColumns.Count, writer.Values.Count);
    }
}
