/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Lite's half of the cross-SKU text guard for #3648. The exact counterpart of
/// <c>Darling.Tests.DrillDownDopProvenanceParityTests</c>, built to the shape
/// <c>QueryHighDopStaleMaxDopParityTests</c> established: Darling's twin does the cross-STORE comparison
/// (its <c>build.yml</c> filter covers every Lite <c>.cs</c> file, so it runs on an edit to either analysis
/// tree); this file owns Lite's own census — that the <c>top_cpu_queries</c> and <c>bad_actor_query</c> reads
/// carry the per-plan provenance shape rather than the old hash-folded <c>MAX(max_dop)</c>, and that an
/// unknown DOP is projected as null rather than 0 — and meta-pins Darling's guard, so weakening the byte-
/// identity comparison over there fails here, under the <c>lite</c> filter that reaches Darling's test tree.
///
/// <para><b>Why not read Darling's SQL from here.</b> The <c>lite</c> filter does not reach
/// <c>Darling/PerformanceMonitor.Darling.Analysis</c>, and <c>CrossAppGuardCiGateTests</c> fails any Lite
/// guard whose source read PR CI cannot run (#2839): a guard that compares the two apps cannot live behind a
/// filter that fires for only one of them. Darling's constants are public, so its suite compares them against
/// Lite's inline text without parsing its own file.</para>
/// </summary>
public sealed class DrillDownDopProvenanceParityTests
{
    private const string LiteFile = "Lite/Analysis/DrillDownCollector.Queries.cs";
    private const string DarlingGuard = "Darling/Darling.Tests/DrillDownDopProvenanceParityTests.cs";

    [Theory]
    [InlineData("CollectTopCpuQueries")]
    [InlineData("CollectBadActorDetail")]
    public void TheDrillDownSql_CarriesThePerPlanProvenanceShape_NotTheHashFoldedMaximum(string liteMethod)
    {
        var sql = LiteInlineSql(liteMethod);

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

    [Fact]
    public void Lite_DoesNotCoerceAnUnknownDopToZero()
    {
        /* The DMV never reports 0 — a serial plan is 1 — so `IsDBNull ? 0` on a DOP ordinal was "no reading"
           rendered as a degree of parallelism. Scoped to this file's max_dop reads: the other drill-downs'
           zero-coercions (counts, sums) are legitimately 0-when-absent. */
        var source = ParitySource.ReadFile(LiteFile);
        Assert.DoesNotMatch(new Regex(@"max_dop\s*=\s*reader\.IsDBNull\(\d+\)\s*\?\s*0\b"), source);
        Assert.Matches(new Regex(@"var maxDop = reader\.IsDBNull\(4\) \? \(int\?\)null"), source);
        Assert.Matches(new Regex(@"var maxDop = reader\.IsDBNull\(10\) \? \(int\?\)null"), source);
        Assert.Contains("dop_note = QueryDopProvenance.Note(maxDop, maxDopAnyPlan, maxDopAnyPlanLastSeen, planCount)", source, StringComparison.Ordinal);
    }

    [Fact]
    public void DarlingsTwinGuard_StillComparesBothReadsByteForByte_SoNeitherSideCanFallBehind()
    {
        /* Meta-pin: Darling's guard must keep BOTH reads in its byte-identity theory and keep reading Lite's
           inline text. Dropping a row, or narrowing the comparison to a Contains, is how one SKU's read would
           start drifting under a green board. */
        var guard = ParitySource.ReadFile(DarlingGuard);
        Assert.Contains("public void TheDrillDownSql_IsByteIdenticalAcrossSkus(", guard, StringComparison.Ordinal);
        Assert.Contains("{ \"CollectTopCpuQueries\", PgDrillDownCollector.TopCpuQueriesSql }", guard, StringComparison.Ordinal);
        Assert.Contains("{ \"CollectBadActorDetail\", PgDrillDownCollector.BadActorDetailSql }", guard, StringComparison.Ordinal);
        Assert.Contains("Assert.Equal(Lf(darlingSql), Lf(LiteInlineSql(liteMethod)));", guard, StringComparison.Ordinal);
        Assert.Contains("RepoFile.ReadRepoFile(\"Lite\", \"Analysis\", \"DrillDownCollector.Queries.cs\")", guard, StringComparison.Ordinal);
    }

    /// <summary>
    /// Lite's inline SQL: the first <c>cmd.CommandText = @"..."</c> verbatim literal after the named method's
    /// declaration. The two reads under test contain no doubled quotes, so the literal ends at the first
    /// <c>";</c>.
    /// </summary>
    private static string LiteInlineSql(string methodName)
    {
        var source = ParitySource.ReadFile(LiteFile);
        var start = source.IndexOf($"Task {methodName}(", StringComparison.Ordinal);
        Assert.True(start >= 0, $"{methodName} not found in {LiteFile}");
        const string Marker = "cmd.CommandText = @\"";
        var literalStart = source.IndexOf(Marker, start, StringComparison.Ordinal) + Marker.Length;
        var literalEnd = source.IndexOf("\";", literalStart, StringComparison.Ordinal);
        return source[literalStart..literalEnd];
    }
}
