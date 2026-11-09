/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Darling's half of the cross-SKU text guard for #3648: the <c>top_cpu_queries</c> and <c>bad_actor_query</c>
/// drill-down SQL is one text, <c>PerformanceMonitor.Common.DrillDownSql</c> (#8470), that Darling's
/// <see cref="PgDrillDownCollector.TopCpuQueriesSql"/> and <see cref="PgDrillDownCollector.BadActorDetailSql"/> are
/// (<c>DrillDownSqlTests</c> holds them to it) and that Lite's two drill-down methods hand DuckDB when they run (Lite's
/// <c>DrillDownDopProvenanceParityTests</c> watches them). This class pins what the text Darling runs must carry: the
/// per-plan provenance shape rather than the old hash-folded <c>MAX(max_dop)</c>, and no zero stand-in for an unknown
/// DOP. That the readers keep that null is observed on the running collectors, in <c>DrillDownDopProvenanceLiveTests</c>
/// here and in Lite's parity class.
///
/// <para><b>Why one text and not two independent pins.</b> #2705 fixed Darling's stale-max_dop cross-check and closed
/// without a record that Lite had a twin; #2999 is what that cost. The two drill-down reads were already
/// character-for-character the same before #3648 (the port kept Lite's text), and the provenance columns are
/// engine-neutral (<c>ROW_NUMBER() OVER</c>, <c>MAX() OVER</c>, <c>CASE</c> aggregates, explicit <c>NULLS LAST</c>), so
/// the strongest guard available is one constant each app is held to: a read that stops running it fails in its own
/// suite until it is brought back.</para>
/// </summary>
public sealed class DrillDownDopProvenanceParityTests
{
    private static string DarlingText(string name) => name switch
    {
        nameof(PgDrillDownCollector.TopCpuQueriesSql) => PgDrillDownCollector.TopCpuQueriesSql,
        nameof(PgDrillDownCollector.BadActorDetailSql) => PgDrillDownCollector.BadActorDetailSql,
        _ => throw new ArgumentOutOfRangeException(nameof(name), name, "not a PgDrillDownCollector drill-down text"),
    };

    [Theory]
    [InlineData(nameof(PgDrillDownCollector.TopCpuQueriesSql))]
    [InlineData(nameof(PgDrillDownCollector.BadActorDetailSql))]
    public void TheDrillDownSql_CarriesThePerPlanProvenanceShape_NotTheHashFoldedMaximum(string name)
    {
        var sql = Lf(DarlingText(name));

        /* The headline is the NEWEST plan's reading, picked by a newest-first ranking with the tie-breakers
           spelled out (compile time, then CPU spent) and their null placement made explicit — DuckDB and
           Postgres default DESC null placement differently, and an implicit default here would make the two
           SKUs pick different rows on the same data. */
        Assert.Matches(
            new Regex(@"ROW_NUMBER\(\)\s+OVER\s*\(\s*PARTITION BY database_name, query_hash\s+ORDER BY collection_time DESC, creation_time DESC NULLS LAST, delta_worker_time DESC NULLS LAST\s*\)\s+AS newest_rn", RegexOptions.Singleline),
            sql);
        Assert.Contains("MAX(CASE WHEN newest_rn = 1 THEN max_dop END) AS max_dop", sql, StringComparison.Ordinal);

        /* The history: how many plans the group spans, the cross-plan maximum, and when it was last seen. */
        Assert.Contains("COUNT(DISTINCT query_plan_hash) AS plan_count", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(max_dop) OVER (PARTITION BY database_name, query_hash) AS max_dop_any_plan", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(max_dop_any_plan) AS max_dop_any_plan", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(CASE WHEN max_dop = max_dop_any_plan THEN collection_time END) AS max_dop_any_plan_last_seen", sql, StringComparison.Ordinal);

        /* And the lie itself is gone: no bare hash-folded MAX(max_dop) projected as max_dop. */
        Assert.DoesNotContain("MAX(max_dop) AS max_dop", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(PgDrillDownCollector.TopCpuQueriesSql))]
    [InlineData(nameof(PgDrillDownCollector.BadActorDetailSql))]
    public void TheText_LeavesAnUnknownDopNull_ForTheReaderToKeepNull(string name)
    {
        /* The DMV never reports 0 — a serial plan is 1 — so a 0 on a DOP column is "no reading" rendered as a
           degree of parallelism. The text's part of that rule: the DOP projections are the bare per-plan readings
           and their maximum, with no zero stand-in folded in anywhere the text names max_dop. The readers' part,
           that the null arrives as null on every DOP field and the note is composed from the four provenance
           values, is observed on the running collectors: DrillDownDopProvenanceLiveTests for Darling and Lite's
           DrillDownDopProvenanceParityTests for Lite. */
        var sql = Lf(DarlingText(name));

        Assert.Contains("MAX(CASE WHEN newest_rn = 1 THEN max_dop END) AS max_dop", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(max_dop) OVER (PARTITION BY database_name, query_hash) AS max_dop_any_plan", sql, StringComparison.Ordinal);
        Assert.DoesNotMatch(new Regex(@"(COALESCE|IFNULL|NVL)\s*\([^)]*max_dop", RegexOptions.IgnoreCase), sql);
        Assert.DoesNotMatch(new Regex(@"max_dop[^,\n]*,\s*0\s*\)"), sql);
    }

    private static string Lf(string text) => text.Replace("\r\n", "\n", StringComparison.Ordinal);
}
